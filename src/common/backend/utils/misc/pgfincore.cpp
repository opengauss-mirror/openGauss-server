/* -------------------------------------------------------------------------
 *
 * pgfincore.cpp 
 *	  This file let you see and mainpulate objects in the FS page cache
 *
 * Portions Copyright (c) 2022, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2011, Cédric Villemain
 *
 * IDENTIFICATION
 *	  src/common/backend/utils/misc/pgfincore.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "postgres.h"
#include "access/heapam.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_class.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/varbit.h"
#include "utils/relcache.h" 
#include "funcapi.h"
#include "catalog/pg_type.h" 
#include "storage/smgr/fd.h"
#include "securec.h"
#include "nodes/pg_list.h"
#include "storage/lock/lock.h"
#include "utils/relcache.h"

#define PGSYSCONF_COLS 3
#define PGFADVISE_COLS 4
#define PGFADVISE_LOADER_COLS 5
#define PGFINCORE_COLS 10

#define PGF_WILLNEED 10
#define PGF_DONTNEED 20
#define PGF_NORMAL 30
#define PGF_SEQUENTIAL 40
#define PGF_RANDOM 50

#define FINCORE_PRESENT 0x1
#define FINCORE_DIRTY 0x2
#define FINCORE_BITS 1

/*
 * pgfadvise_fctx structure is needed
 * to keep track of relation path, segment number, ...
 */
typedef struct {
    int	advice;	/* the posix_fadvise advice */
    TupleDesc tupd;	/* the tuple descriptor */
    Relation rel; /* the relation */
    unsigned int segcount; /* the segment current number */
    char *relationpath;	/* the relation path */
    bool isPartitionTable;	/* partition table ?*/
    bool isSubPartitionTable; /*subPartition table ?*/
    ListCell *partitionCell;
    ListCell *subPartitionCell;
    List *partitionIdList;
    List *subPartitionIdList;
    text *forkName;
    List *indexoidlist;	
    ListCell *indexCell;
    bool isFirstIndexOid;
} pgfadvise_fctx;

/*
 * pgfadvise structure is needed
 * to return values
 */
typedef struct {
    size_t pageSize; /* os page size */
    size_t pagesFree; /* free page cache */
    size_t filesize; /* the filesize */
} pgfadviseStruct;

/*
 * pgfloader structure is needed
 * to return values
 */
typedef struct {
    size_t pageSize; /* os page size */
    size_t pagesFree; /* free page cache */
    size_t pagesLoaded; /* pages loaded */
    size_t pagesUnloaded; /* pages unloaded  */
} pgfloaderStruct;

/*
 * pgfincore_fctx structure is needed
 * to keep track of relation path, segment number, ...
 */
typedef struct {
    bool getvector;	/* output varbit data ? */
    TupleDesc tupd;	/* the tuple descriptor */
    Relation rel; /* the relation */
    unsigned int segcount; /* the segment current number */
    char *relationpath;	/* the relation path */
    bool isPartitionTable;	/* partition table ?*/
    bool isSubPartitionTable; /*subPartition table ?*/
    ListCell *partitionCell;
    ListCell *subPartitionCell;
    List *partitionIdList;
    List *subPartitionIdList;
    text *forkName;
    List *indexoidlist;	
    ListCell *indexCell;
    bool isFirstIndexOid;
} pgfincore_fctx;

/*
 * pgfadvise_loader_struct structure is needed
 * to keep track of relation path, segment number, ...
 */
typedef struct {
    size_t pageSize; /* os page size */
    size_t pagesFree; /* free page cache */
    size_t rel_os_pages;
    size_t pages_mem;
    size_t group_mem;
    size_t pages_dirty;
    size_t group_dirty;
    VarBit *databit;
} pgfincoreStruct;

Datum pgsysconf(PG_FUNCTION_ARGS);

extern Datum pgfadvise(PG_FUNCTION_ARGS);
static bool pgfadvise_file(char *filename, int advice, pgfadviseStruct *pgfdv);

extern Datum pgfadvise_loader(PG_FUNCTION_ARGS);
static bool pgfadvise_loader_file(char *filename, bool willneed, 
    bool dontneed, VarBit *databit, pgfloaderStruct *pgfloader);
static char *getRelpath(ListCell *partitionCell, Relation rel, bool isSubPartition, text *forkName);
extern Datum pgfincore(PG_FUNCTION_ARGS);
static bool pgfincore_file(char *filename, pgfincoreStruct *pgfncr);

extern Datum pgfincore_drawer(PG_FUNCTION_ARGS);

#define relpathpg(rel, forkName) \
    relpathbackend((rel)->rd_node, (rel)->rd_backend, (forkname_to_number(text_to_cstring(forkName))))

/*
 * pgsysconf
 * just output the actual system value for
 * _SC_PAGESIZE     --> Page Size
 * _SC_AVPHYS_PAGES --> Free page in memory
 * _SC_PHYS_PAGES   --> Total memory
 *
 */
Datum pgsysconf(PG_FUNCTION_ARGS)
{
    HeapTuple tuple;
    TupleDesc tupdesc;
    Datum values[PGSYSCONF_COLS];
    bool nulls[PGSYSCONF_COLS];

    /* initialize nulls array to build the tuple */
    int ret = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(ret, "\0", "\0");
    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errmsg("pgsysconf: return type must be a row type")));

    /* Page size */
    values[0] = Int64GetDatum(sysconf(_SC_PAGESIZE));

    /* free page in memory */
    values[1] = Int64GetDatum(sysconf(_SC_AVPHYS_PAGES));

    /* total memory */
    values[2] = Int64GetDatum(sysconf(_SC_PHYS_PAGES));

    /* Build and return the result tuple. */
    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM( HeapTupleGetDatum(tuple) );
}

#if defined(USE_POSIX_FADVISE)

static bool pgfadvise_file(char *filename, int advice, pgfadviseStruct *pgfdv)
{
    /*
     * We use the AllocateFile(2) provided by PostgreSQL.  We're going to
     * close it ourselves even if PostgreSQL close it anyway at transaction
     * end.
     */
    FILE *fp;
    int	fd;
    struct stat st;
    int adviceFlag;

    /*
     * OS Page size and Free pages
     */
    pgfdv->pageSize	= sysconf(_SC_PAGESIZE);

    /*
     * Fopen and fstat file 
     * fd will be provided to posix_fadvise
     * if there is no file, just return	1, it is expected to leave the SRF
     */
    fp = AllocateFile(filename, "rb");
    if (fp == NULL)
        return false;

    fd = fileno(fp);
    if (fstat(fd, &st) == -1) {
        FreeFile(fp);
        ereport(ERROR, (errmsg("pgfadvise: Can not stat object file : %s", filename)));
        return false;
    }

    /*
     * the file size is used in the SRF to output the number of pages used by
     * the segment
     */
    pgfdv->filesize = st.st_size;
    ereport(DEBUG1, 
        (errmsg("pgfadvise: working on %s of %lld bytes", filename,(long long int) pgfdv->filesize)));
    /* FADVISE_WILLNEED */
    if (advice == PGF_WILLNEED) {
        adviceFlag = POSIX_FADV_WILLNEED;
        ereport(DEBUG1, (errmsg("pgfadvise: setting advice POSIX_FADV_WILLNEED")));
    }
    /* FADVISE_DONTNEED */
    else if (advice == PGF_DONTNEED) {
        adviceFlag = POSIX_FADV_DONTNEED;
        ereport(DEBUG1, (errmsg("pgfadvise: setting advice POSIX_FADV_DONTNEED")));
    }
    /* POSIX_FADV_NORMAL */
    else if (advice == PGF_NORMAL) {
        adviceFlag = POSIX_FADV_NORMAL;
        ereport(DEBUG1, (errmsg("pgfadvise: setting advice POSIX_FADV_NORMAL")));
    }
    /* POSIX_FADV_SEQUENTIAL */
    else if (advice == PGF_SEQUENTIAL) {
        adviceFlag = POSIX_FADV_SEQUENTIAL;
        ereport(DEBUG1, (errmsg("pgfadvise: setting advice POSIX_FADV_SEQUENTIAL")));
    }
    /* POSIX_FADV_RANDOM */
    else if (advice == PGF_RANDOM) {
        adviceFlag = POSIX_FADV_RANDOM;
        ereport(DEBUG1, (errmsg("pgfadvise: setting advice POSIX_FADV_RANDOM")));
    } else {
        ereport(ERROR, (errmsg("pgfadvise: invalid advice: %d", advice)));
        return false;
    }

    /*
     * Call posix_fadvise with the relevant advice on the file descriptor
     */
    posix_fadvise(fd, 0, 0, adviceFlag);

    /* close the file */
    FreeFile(fp);

    /*
     * OS things : Pages free
     */
    pgfdv->pagesFree = sysconf(_SC_AVPHYS_PAGES);

    return true;
}
#else
static bool pgfadvise_file(char *filename, int advice, pgfadviseStruct	*pgfdv)
{
    ereport(ERROR, (errmsg("POSIX_FADVISE UNSUPPORTED on your platform")));
    return false;
}
#endif

/*
 * pgfadvise is a function that handle the process to have a sharelock
 * on the relation and to walk the segments.
 * for each segment it call the posix_fadvise with the required flag
 * parameter
 */
Datum pgfadvise(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    pgfadvise_fctx *fctx;

    /* our structure use to return values */
    pgfadviseStruct *pgfdv;

    /* our return value, true for success */
    bool result;

    /* The file we are working on */
    char filename[MAXPGPATH];

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;

        Oid relOid = PG_GETARG_OID(0);
        text *forkName = PG_GETARG_TEXT_P(1);
        int	advice = PG_GETARG_INT32(2);

        /*
        * Postgresql stuff to return a tuple
        */
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* allocate memory for user context */
        fctx = (pgfadvise_fctx*)palloc(sizeof(pgfadvise_fctx));

        fctx->forkName = (text*)palloc(VARSIZE(forkName));
        SET_VARSIZE(fctx->forkName, VARSIZE(forkName));
        errno_t ret = memcpy_s((void*)VARDATA(fctx->forkName), VARSIZE(forkName) - VARHDRSZ, (void*)VARDATA(forkName), VARSIZE(forkName) - VARHDRSZ);
        securec_check(ret, "\0", "\0");

        /* Build a tuple descriptor for our result type */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR, (errmsg("pgfadvise: return type must be a row type")));

        /* provide the tuple descriptor to the fonction structure */
        fctx->tupd = tupdesc;

        /* open the current relation, accessShareLock */
        // TODO use try_relation_open instead ?
        fctx->rel = relation_open(relOid, AccessShareLock);

        if (RelationIsColStore(fctx->rel)) {
            ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("column-store relation doesn't support pgfadvise yet")));
        }
        
        if (RelationIsSegmentTable(fctx->rel)) {
            ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("segment-page tables doesn't support pgfadvise yet")));
        }

        if (RelationIsSubPartitioned(fctx->rel)) {
            fctx->isSubPartitionTable = true;
            fctx->isPartitionTable = false;
        } else if (RELATION_IS_PARTITIONED(fctx->rel)) {
            fctx->isPartitionTable = true;
            fctx->isSubPartitionTable = false;
        } else {
            fctx->isPartitionTable = false;
            fctx->isSubPartitionTable = false;
        }
        fctx->partitionCell = NULL;
        fctx->subPartitionCell = NULL;
        fctx->indexCell = NULL;
        fctx->partitionIdList = NULL;
        fctx->subPartitionIdList = NULL;
        fctx->isFirstIndexOid = true;;
        if (!RelationIsIndex(fctx->rel)) {
            fctx->indexoidlist = RelationGetIndexList(fctx->rel);
        } else {
            fctx->indexoidlist = NULL;
        }
        

        /* Here we keep track of current action in all calls */
        fctx->advice = advice;

        if (!(fctx->isPartitionTable || fctx->isSubPartitionTable)) {
            /* we get the common part of the filename of each segment of a relation */
            fctx->relationpath = relpathpg(fctx->rel, forkName);

            /* segcount is used to get the next segment of the current relation */
            fctx->segcount = 0;
        } else if (fctx->isSubPartitionTable) {
            fctx->subPartitionIdList = RelationGetSubPartitionList(fctx->rel, AccessShareLock);
            fctx->subPartitionCell = list_head(fctx->subPartitionIdList);
            fctx->relationpath = getRelpath(fctx->subPartitionCell, fctx->rel, true, forkName);
            fctx->segcount = 0;
        } else if (fctx->isPartitionTable) {
            fctx->partitionIdList = relationGetPartitionList(fctx->rel, AccessShareLock);
            fctx->partitionCell = list_head(fctx->partitionIdList);
            fctx->relationpath = getRelpath(fctx->partitionCell, fctx->rel, false, forkName);
            fctx->segcount = 0;
        }


        /* And finally we keep track of our initialization */
        ereport(DEBUG1, (errmsg("pgfadvise: init done for %s, in fork %s",
                                fctx->relationpath, text_to_cstring(forkName))));				
        funcctx->user_fctx = fctx;
        MemoryContextSwitchTo(oldcontext);
    }

    /* After the first call, we recover our context */
    funcctx = SRF_PERCALL_SETUP();
    fctx = (pgfadvise_fctx *) funcctx->user_fctx;

    /*
     * If we are still looking the first segment
     * relationpath should not be suffixed
     */
    if (fctx->segcount == 0){
        errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s", fctx->relationpath);
        securec_check_ss(rc, "\0", "\0");
    } else {
        errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s.%u", fctx->relationpath, fctx->segcount);
        securec_check_ss(rc, "\0", "\0");
    }

    FILE *fp = AllocateFile(filename, "rb");
    if (fp == NULL) {
        if (fctx->isPartitionTable || fctx->isSubPartitionTable) {
            if (fctx->isSubPartitionTable && lnext(fctx->subPartitionCell)) {
                fctx->subPartitionCell = lnext(fctx->subPartitionCell);
                fctx->relationpath = getRelpath(fctx->subPartitionCell, fctx->rel, true, fctx->forkName);
                fctx->segcount = 0;
                errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s", fctx->relationpath);
                securec_check_ss(rc, "\0", "\0");
            } else if (fctx->isPartitionTable && lnext(fctx->partitionCell)) {
                fctx->partitionCell = lnext(fctx->partitionCell);
                fctx->relationpath = getRelpath(fctx->partitionCell, fctx->rel, false, fctx->forkName);
                fctx->segcount = 0;
                errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s", fctx->relationpath);
                securec_check_ss(rc, "\0", "\0");
            } else {
                if (fctx->indexoidlist != NULL) {
                    if (fctx->isFirstIndexOid) {
                        fctx->indexCell = list_head(fctx->indexoidlist);
                        fctx->isFirstIndexOid=false;
                        Oid indexId = lfirst_oid(fctx->indexCell);
                        Relation currentIndex = index_open(indexId, AccessShareLock);
                        fctx->relationpath = relpathpg(currentIndex, fctx->forkName);
                        fctx->segcount = 0;
                        index_close(currentIndex, NoLock);
                        errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s", fctx->relationpath);
                        securec_check_ss(rc, "\0", "\0");
                    } else {
                        if (lnext(fctx->indexCell)) {
                            fctx->indexCell = lnext(fctx->indexCell);
                            Oid indexId = lfirst_oid(fctx->indexCell);
                            Relation currentIndex = index_open(indexId, AccessShareLock);
                            fctx->relationpath = relpathpg(currentIndex, fctx->forkName);
                            fctx->segcount = 0;
                            index_close(currentIndex, NoLock);
                            errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s", fctx->relationpath);
                            securec_check_ss(rc, "\0", "\0");
                        }
                    }
                }
            }
        } else {
            //process index 
            if (fctx->indexoidlist != NULL) {
                if (fctx->isFirstIndexOid) {
                    fctx->indexCell = list_head(fctx->indexoidlist);
                    fctx->isFirstIndexOid=false;
                    Oid indexId = lfirst_oid(fctx->indexCell);
                    Relation currentIndex = index_open(indexId, AccessShareLock);
                    fctx->relationpath = relpathpg(currentIndex, fctx->forkName);
                    fctx->segcount = 0;
                    index_close(currentIndex, NoLock);
                    errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s", fctx->relationpath);
                    securec_check_ss(rc, "\0", "\0");
                } else {
                    if (lnext(fctx->indexCell)) {
                        fctx->indexCell = lnext(fctx->indexCell);
                        Oid indexId = lfirst_oid(fctx->indexCell);
                        Relation currentIndex = index_open(indexId, AccessShareLock);
                        fctx->relationpath = relpathpg(currentIndex, fctx->forkName);
                        fctx->segcount = 0;
                        index_close(currentIndex, NoLock);
                        errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s", fctx->relationpath);
                        securec_check_ss(rc, "\0", "\0");
                    }
                }
            }
        }
    } else {
        FreeFile(fp);
    }
    
    ereport(DEBUG1, (errmsg("pgfadvise: about to work with %s, current advice : %d",
         filename, fctx->advice)));
    /*
     * Call posix_fadvise with the advice, returning the structure
     */
    pgfdv = (pgfadviseStruct *) palloc(sizeof(pgfadviseStruct));
    result = pgfadvise_file(filename, fctx->advice, pgfdv);

    /*
    * When we have work with all segments of the current relation
    * We exit from the SRF
    * Else we build and return the tuple for this segment
    */
    if (!result) {
        ereport(DEBUG1, (errmsg("pgfadvise: closing %s", fctx->relationpath)));
        if (fctx->isPartitionTable) {
            releasePartitionList(fctx->rel, &(fctx->partitionIdList), AccessShareLock);
        } else if (fctx->isSubPartitionTable) {
            releasePartitionList(fctx->rel, &(fctx->subPartitionIdList), AccessShareLock);
        }
        relation_close(fctx->rel, AccessShareLock);
        list_free(fctx->indexoidlist);
        pfree(fctx);
        SRF_RETURN_DONE(funcctx);
    } else {
        /*
        * Postgresql stuff to return a tuple
        */
        HeapTuple tuple;
        Datum values[PGFADVISE_COLS];
        bool nulls[PGFADVISE_COLS];

        /* initialize nulls array to build the tuple */
        int ret = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(ret, "\0", "\0");
        /* prepare the number of the next segment */
        fctx->segcount++;

        /* Filename */
        values[0] = CStringGetTextDatum( filename );
        /* os page size */
        values[1] = Int64GetDatum( (int64) pgfdv->pageSize );
        /* number of pages used by segment */
        values[2] = Int64GetDatum( (int64) ((pgfdv->filesize+pgfdv->pageSize-1)/pgfdv->pageSize) );
        /* free page cache */
        values[3] = Int64GetDatum( (int64) pgfdv->pagesFree );
        /* Build the result tuple. */
        tuple = heap_form_tuple(fctx->tupd, values, nulls);

        /* Ok, return results, and go for next call */
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
}

#if defined(USE_POSIX_FADVISE)

static bool pgfadvise_loader_file(char *filename,
                      bool willneed, bool dontneed, VarBit *databit,
                      pgfloaderStruct *pgfloader)
{
    bits8 *sp;
    int bitlen;
    bits8 x;
    int	i, k;

    /*
     * We use the AllocateFile(2) provided by PostgreSQL.  We're going to
     * close it ourselves even if PostgreSQL close it anyway at transaction
     * end.
     */
    FILE *fp;
    int	fd;
    struct stat st;

    /*
     * OS things : Page size
     */
    pgfloader->pageSize = sysconf(_SC_PAGESIZE);

    /*
     * we count the action we perform
     * both are theorical : we don't know if the page was or not in memory
     * when we call posix_fadvise
     */
    pgfloader->pagesLoaded		= 0;
    pgfloader->pagesUnloaded	= 0;

    /*
     * Fopen and fstat file
     * fd will be provided to posix_fadvise
     * if there is no file, just return	1, it is expected to leave the SRF
     */
    fp = AllocateFile(filename, "rb");
    if (fp == NULL)
        return false;

    fd = fileno(fp);
    if (fstat(fd, &st) == -1) {
        FreeFile(fp);
        ereport(ERROR, (errmsg("pgfadvise_loader: Can not stat object file: %s", filename)));
        return false;
    }

    ereport(DEBUG1, (errmsg("pgfadvise_loader: working on %s", filename)));

    bitlen = VARBITLEN(databit);
    sp = VARBITS(databit);
    for (i = 0; i < bitlen - BITS_PER_BYTE; i += BITS_PER_BYTE, sp++) {
        x = *sp;
        /*  Is this bit set ? */
        for (k = 0; k < BITS_PER_BYTE; k++) {
            if (IS_HIGHBIT_SET(x)) {
                if (willneed) {
                    (void) posix_fadvise(fd,
                                         ((i+k) * pgfloader->pageSize),
                                         pgfloader->pageSize,
                                         POSIX_FADV_WILLNEED);
                    pgfloader->pagesLoaded++;
                }
            } else if (dontneed) {	
                (void) posix_fadvise(fd,
                                     ((i+k) * pgfloader->pageSize),
                                     pgfloader->pageSize,
                                     POSIX_FADV_DONTNEED);
                pgfloader->pagesUnloaded++;
            }

            x <<= 1;
        }
    }
    /*
     * XXX this copy/paste of code to finnish to walk the bits is not pretty
     */
    if (i < bitlen)
    {
        /* print the last partial byte */
        x = *sp;
        for (k = i; k < bitlen; k++) {
            if (IS_HIGHBIT_SET(x)) {
                if (willneed) {
                    (void) posix_fadvise(fd,
                                         (k * pgfloader->pageSize),
                                         pgfloader->pageSize,
                                         POSIX_FADV_WILLNEED);
                    pgfloader->pagesLoaded++;
                }
            } else if (dontneed) {
                (void) posix_fadvise(fd,
                                     (k * pgfloader->pageSize),
                                     pgfloader->pageSize,
                                     POSIX_FADV_DONTNEED);
                pgfloader->pagesUnloaded++;
            }
            x <<= 1;
        }
    }
    FreeFile(fp);

    /*
     * OS things : Pages free
     */
    pgfloader->pagesFree = sysconf(_SC_AVPHYS_PAGES);

    return true;
}
#else
static bool pgfadvise_loader_file(char *filename,
                      bool willneed, bool dontneed, VarBit *databit,
                      pgfloaderStruct *pgfloader)
{
    ereport(ERROR, (errmsg("POSIX_FADVISE UNSUPPORTED on your platform")));
    return false;
}
#endif

/*
 *
 * pgfadv_loader to handle work with varbit map of buffer cache.
 * it is actually used for loading/unloading block to/from buffer cache
 *
 */
Datum pgfadvise_loader(PG_FUNCTION_ARGS)
{
    Oid relOid = PG_GETARG_OID(0);
    text *forkName = PG_GETARG_TEXT_P(1);
    char relType = PG_GETARG_CHAR(2);
    text *partitionName = NULL; 
    if (PARTTYPE_SUBPARTITIONED_RELATION == relType || 
        PARTTYPE_PARTITIONED_RELATION == relType) { 
        if (PG_ARGISNULL(3)) {
            ereport(ERROR, (errmsg("pgfadvise_loader: partitionName argument shouldn't be NULL if the relation is partition or subpartition")));
        }
    }
    if (!PG_ARGISNULL(3)) {
        partitionName = PG_GETARG_TEXT_P(3);
    }
    int segmentNumber = PG_GETARG_INT32(4);
    /* if this variable is set to false, no page will be set to willneed */
    bool willneed = PG_GETARG_BOOL(5);
    /* if this variable is set to false, no page will be set to dontneed */
    bool dontneed = PG_GETARG_BOOL(6);
    /*
     * if the variable willneed and dontneed is set to true, the pages 
     * corresponding to 1 will be set WILLNEED, and the pages corresponding
     * to 0 will be set DONTNEED.
     */
    VarBit *databit;

    /* our structure use to return values */
    pgfloaderStruct	*pgfloader;

    Relation rel;
    char *relationpath = NULL;
    char filename[MAXPGPATH];

    /* our return value, true for success */
    bool result;

    /*
     * Postgresql stuff to return a tuple
     */
    HeapTuple tuple;
    TupleDesc tupdesc;
    Datum values[PGFADVISE_LOADER_COLS];
    bool nulls[PGFADVISE_LOADER_COLS];

    if (PG_ARGISNULL(7))
        ereport(ERROR, (errmsg("pgfadvise_loader: databit argument shouldn't be NULL")));

    databit	= PG_GETARG_VARBIT_P(7);

    /* initialize nulls array to build the tuple */
    int ret = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(ret, "\0", "\0");
    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errmsg("return type must be a row type")));

    /* open the current relation in accessShareLock */
    rel = relation_open(relOid, AccessShareLock);

    if (RelationIsColStore(rel)) {
        ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Un-support feature"),
            errdetail("column-store relation doesn't support pgfadvise_loader yet")));
    }
        
    if (RelationIsSegmentTable(rel)) {
        ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Un-support feature"),
            errdetail("segment-page tables doesn't support pgfadvise_loader yet")));
    }

    /* we get the relationpath */
    if (PARTTYPE_SUBPARTITIONED_RELATION == relType || 
        PARTTYPE_PARTITIONED_RELATION == relType ) {
        ListCell* cell = NULL;
        Partition partition = NULL;
        Relation partRel = NULL;
        List *partitionList = NIL;

        if (PARTTYPE_SUBPARTITIONED_RELATION == relType) {
            if (!RelationIsSubPartitioned(rel)) {
                ereport(ERROR, (errmsg("The %s isn't subpartition", text_to_cstring(partitionName))));
            }
            partitionList = RelationGetSubPartitionList(rel, AccessShareLock);
        } else if (PARTTYPE_PARTITIONED_RELATION == relType) {
            if (RelationIsSubPartitioned(rel)) {
                ereport(ERROR, (errmsg("The %s is subpartition, however the relType is 'p'", text_to_cstring(partitionName))));
            } else if(!RELATION_IS_PARTITIONED(rel)) {
                ereport(ERROR, (errmsg("The %s isn't partition", text_to_cstring(partitionName))));
            }
            partitionList = relationGetPartitionList(rel, AccessShareLock);
        }

        foreach (cell, partitionList) {
            partition = (Partition)lfirst(cell);
            char* partName = PartitionGetPartitionName(partition);
            if (strcmp(partName,text_to_cstring(partitionName)) != 0) {
                continue;
            } else {
                if (RelationIsSubPartitioned(rel)) {
                    partRel = SubPartitionGetRelation(rel, partition, AccessShareLock);
                } else if (RELATION_IS_PARTITIONED(rel)) {
                    partRel = partitionGetRelation(rel, partition);
                }
                relationpath = relpathpg(partRel, forkName);
                releaseDummyRelation(&partRel);
                break;
            }
        }
        releasePartitionList(rel, &partitionList, AccessShareLock);
        if (relationpath == NULL) {
            if (RelationIsSubPartitioned(rel)) {
                ereport(ERROR, (errmsg("The subpartition %s isn't exist", text_to_cstring(partitionName))));
            } else if (RELATION_IS_PARTITIONED(rel)) {
                ereport(ERROR, (errmsg("The partition %s isn't exist", text_to_cstring(partitionName))));    
            }
        }
    } else if (RELKIND_RELATION == relType) {
        if (RelationIsSubPartitioned(rel)) {
            ereport(ERROR, (errmsg("The %s is subpartition", text_to_cstring(partitionName))));
        } else if (RELATION_IS_PARTITIONED(rel)) {
            ereport(ERROR, (errmsg("The %s is partition", text_to_cstring(partitionName))));    
        }
        relationpath = relpathpg(rel, forkName);
    } else {
        ereport(ERROR, (errmsg("The relType must be 'r', 'p' or 's'")));
    }

    /*
     * If we are looking the first segment,
     * relationpath should not be suffixed
     */
    if (segmentNumber == 0){
        errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s", relationpath);
        securec_check_ss(rc, "\0", "\0");
    } else {
        errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s.%u", relationpath, (int) segmentNumber);
        securec_check_ss(rc, "\0", "\0");
    }

    /*
     * We don't need the relation anymore
     * the only purpose was to get a consistent filename
     * (if file disappear, an error is logged)
     */
    relation_close(rel, AccessShareLock);

    /*
     * Call pgfadvise_loader with the varbit
     */
    pgfloader = (pgfloaderStruct *) palloc(sizeof(pgfloaderStruct));
    result = pgfadvise_loader_file(filename,
                                   willneed, dontneed, databit,
                                   pgfloader);
    if (!result)
        ereport(ERROR, (errmsg("Can't read file %s, fork(%s)",
                    filename, text_to_cstring(forkName))));
    /* Filename */
    values[0] = CStringGetTextDatum( filename );
    /* os page size */
    values[1] = Int64GetDatum( pgfloader->pageSize );
    /* free page cache */
    values[2] = Int64GetDatum( pgfloader->pagesFree );
    /* pages loaded */
    values[3] = Int64GetDatum( pgfloader->pagesLoaded );
    /* pages unloaded  */
    values[4] = Int64GetDatum( pgfloader->pagesUnloaded );

    /* Build and return the result tuple. */
    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM( HeapTupleGetDatum(tuple) );
}

/*
 * pgfincore_file handle the mmaping, mincore process (and access file, etc.)
 */
static bool pgfincore_file(char *filename, pgfincoreStruct *pgfncr)
{
    int	flag=1;
    int flag_dirty=1;

    int	len, bitlen;
    bits8 *r;
    bits8 x = 0;
    register size_t pageIndex;

    /*
     * We use the AllocateFile(2) provided by PostgreSQL.  We're going to
     * close it ourselves even if PostgreSQL close it anyway at transaction
     * end.
     */
    FILE *fp;
    int	fd;
    struct stat st;

    void *pa = (char *) 0;

    unsigned char *vec = (unsigned char *) 0;

    /*
     * OS Page size
     */
    pgfncr->pageSize = sysconf(_SC_PAGESIZE);

    /*
     * Initialize counters
     */
    pgfncr->pages_mem = 0;
    pgfncr->group_mem = 0;
    pgfncr->pages_dirty	= 0;
    pgfncr->group_dirty	= 0;
    pgfncr->rel_os_pages = 0;

    /*
     * Fopen and fstat file
     * fd will be provided to posix_fadvise
     * if there is no file, just return	1, it is expected to leave the SRF
     */
    fp = AllocateFile(filename, "rb");
    if (fp == NULL)
        return false;

    fd = fileno(fp);

    if (fstat(fd, &st) == -1) {
        FreeFile(fp);
        ereport(ERROR, (errmsg("Can not stat object file : %s", filename)));
        return false;
    }

    /*
    * if file ok
    * then process
    */
    if (st.st_size != 0) {
        /* number of pages in the current file */
        pgfncr->rel_os_pages = (st.st_size+pgfncr->pageSize-1)/pgfncr->pageSize;

        pa = mmap(NULL, st.st_size, PROT_NONE, MAP_SHARED, fd, 0);
        if (pa == MAP_FAILED) {
            int	save_errno = errno;
            FreeFile(fp);
            ereport(ERROR, (errmsg("Can not mmap object file : %s, errno = %i,%s\nThis error can happen if there is not enought space in memory to do the projection.",
                 filename, save_errno, strerror(save_errno))));
            return false;
        }

        /* Prepare our vector containing all blocks information */
        vec = (unsigned char *) palloc0((st.st_size+pgfncr->pageSize-1)/pgfncr->pageSize);
        if ((void *)0 == vec) {
            munmap(pa, st.st_size);
            FreeFile(fp);
            ereport(ERROR, (errmsg("Can not palloc object file : %s", filename)));
            return false;
        }

        /* Affect vec with mincore */
        if (mincore(pa, st.st_size, vec) != 0) {
            int save_errno = errno;
            munmap(pa, st.st_size);
            ereport(ERROR, (errmsg("mincore(%p, %lld, %p): %s\n",
                 pa, (long long int)st.st_size, vec, strerror(save_errno))));
            if (vec != NULL) {
                pfree(vec);
            }
            FreeFile(fp);
            return false;
        }

        /*
         * prepare the bit string
         */
        bitlen = FINCORE_BITS * ((st.st_size+pgfncr->pageSize-1)/pgfncr->pageSize);
        len = VARBITTOTALLEN(bitlen);
        /*
         * set to 0 so that *r is always initialised and string is zero-padded
         * XXX: do we need to free that ?
         */
        pgfncr->databit = (VarBit *) palloc0(len);
        SET_VARSIZE(pgfncr->databit, len);
        VARBITLEN(pgfncr->databit) = bitlen;

        r = VARBITS(pgfncr->databit);
        x = HIGHBIT;

        /* handle the results */
        for (pageIndex = 0; pageIndex < pgfncr->rel_os_pages; pageIndex++) {
            // block in memory
            if (vec[pageIndex] & FINCORE_PRESENT) {
                pgfncr->pages_mem++;
                *r |= x;
                if (FINCORE_BITS > 1) {
                    if (vec[pageIndex] & FINCORE_DIRTY) {
                        pgfncr->pages_dirty++;
                        *r |= (x >> 1);
                        /* we flag to detect contigous blocks in the same state */
                        if (flag_dirty)
                            pgfncr->group_dirty++;
                        flag_dirty = 0;
                    } else {
                        flag_dirty = 1;
                    }
                }
                ereport(DEBUG5, (errmsg("in memory blocks : %lld / %lld",
                      (long long int) pageIndex, (long long int) pgfncr->rel_os_pages)));
                /* we flag to detect contigous blocks in the same state */
                if (flag)
                    pgfncr->group_mem++;
                flag = 0;
            } else {
                flag=1;
            }

            x >>= FINCORE_BITS;
            if (x == 0) {
                x = HIGHBIT;
                r++;
            }
        }
    }
    ereport(DEBUG1, (errmsg("pgfincore %s: %lld of %lld block in linux cache, %lld groups",
            filename, (long long int) pgfncr->pages_mem,  (long long int) pgfncr->rel_os_pages, (long long int) pgfncr->group_mem)));

    /*
     * free and close
     */
    if (vec != NULL) {
        pfree(vec);
    }
    munmap(pa, st.st_size);
    FreeFile(fp);

    /*
     * OS things : Pages free
     */
    pgfncr->pagesFree = sysconf(_SC_AVPHYS_PAGES);

    return true;
}

static char *getRelpath(ListCell *partitionCell, Relation rel, bool isSubPartition, text *forkName) {
    Partition partition = (Partition)lfirst(partitionCell);
    Relation partitionRel = NULL;
    if (isSubPartition) {
        partitionRel = SubPartitionGetRelation(rel, partition, AccessShareLock);
    } else {
        partitionRel = partitionGetRelation(rel, partition);
    }
    char *relationpath = relpathpg(partitionRel,forkName);
    releaseDummyRelation(&partitionRel);
    return relationpath;
}

/*
 * pgfincore is a function that handle the process to have a sharelock
 * on the relation and to walk the segments.
 * for each segment it call the appropriate function depending on 'action'
 * parameter
 */
Datum pgfincore(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    pgfincore_fctx *fctx;

    /* our structure use to return values */
    pgfincoreStruct	*pgfncr;

    /* our return value, true for success */
    bool result;

    /* The file we are working on */
    char filename[MAXPGPATH];

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;

        Oid	relOid = PG_GETARG_OID(0);
        text *forkName = PG_GETARG_TEXT_P(1);
        bool getvector = PG_GETARG_BOOL(2);

        /*
        * Postgresql stuff to return a tuple
        */
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* allocate memory for user context */
        fctx = (pgfincore_fctx *) palloc(sizeof(pgfincore_fctx));

        fctx->forkName = (text*) palloc(VARSIZE(forkName));
        SET_VARSIZE(fctx->forkName, VARSIZE(forkName));
        errno_t ret = memcpy_s((void*)VARDATA(fctx->forkName), VARSIZE(forkName) - VARHDRSZ, (void*)VARDATA(forkName), VARSIZE(forkName) - VARHDRSZ);
        securec_check(ret, "\0", "\0");


        /* Build a tuple descriptor for our result type */
        if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
            ereport(ERROR, (errmsg("pgfadvise: return type must be a row type")));

        /* provide the tuple descriptor to the fonction structure */
        fctx->tupd = tupdesc;

        /* are we going to grab and output the varbit data (can be large) */
        fctx->getvector = getvector;

        /* open the current relation, accessShareLock */
        // TODO use try_relation_open instead ?
        fctx->rel = relation_open(relOid, AccessShareLock);

        if (RelationIsColStore(fctx->rel)) {
            ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("column-store relation doesn't support pgfincore yet")));
        }
        
        if (RelationIsSegmentTable(fctx->rel)) {
            ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("segment-page tables doesn't support pgfincore yet")));
        }

        if (RelationIsSubPartitioned(fctx->rel)) {
            fctx->isSubPartitionTable = true;
            fctx->isPartitionTable = false;
        } else if (RELATION_IS_PARTITIONED(fctx->rel)) {
            fctx->isPartitionTable = true;
            fctx->isSubPartitionTable = false;
        } else {
            fctx->isPartitionTable = false;
            fctx->isSubPartitionTable = false;
        }
        fctx->partitionCell = NULL;
        fctx->subPartitionCell = NULL;
        fctx->partitionIdList = NULL;
        fctx->subPartitionIdList = NULL;
        fctx->indexCell = NULL;

        fctx->isFirstIndexOid = true;;
        if (!RelationIsIndex(fctx->rel)) {
            fctx->indexoidlist = RelationGetIndexList(fctx->rel);
        } else {
            fctx->indexoidlist = NULL;
        }
        
        if (!(fctx->isPartitionTable || fctx->isSubPartitionTable)) {
            /* we get the common part of the filename of each segment of a relation */
            fctx->relationpath = relpathpg(fctx->rel, forkName);

            /* segcount is used to get the next segment of the current relation */
            fctx->segcount = 0;
        } else if (fctx->isSubPartitionTable) {
            fctx->subPartitionIdList = RelationGetSubPartitionList(fctx->rel, AccessShareLock);
            fctx->subPartitionCell = list_head(fctx->subPartitionIdList);
            fctx->relationpath = getRelpath(fctx->subPartitionCell, fctx->rel, true, forkName);
            fctx->segcount = 0;
        } else if (fctx->isPartitionTable) {
            fctx->partitionIdList = relationGetPartitionList(fctx->rel, AccessShareLock);
            fctx->partitionCell = list_head(fctx->partitionIdList);
            fctx->relationpath = getRelpath(fctx->partitionCell, fctx->rel, false, forkName);
            fctx->segcount = 0;
        }

        /* And finally we keep track of our initialization */
        ereport(DEBUG1, (errmsg("pgfincore: init done for %s, in fork %s", 
                    fctx->relationpath, text_to_cstring(forkName))));
        funcctx->user_fctx = fctx;
        MemoryContextSwitchTo(oldcontext);
    }

    /* After the first call, we recover our context */
    funcctx = SRF_PERCALL_SETUP();
    fctx = (pgfincore_fctx*) funcctx->user_fctx;

    /*
     * If we are still looking the first segment
     * relationpath should not be suffixed
     */
    if (fctx->segcount == 0) {
        errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s", fctx->relationpath);
        securec_check_ss(rc, "\0", "\0");
    } else {
        errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s.%u", fctx->relationpath, fctx->segcount);
        securec_check_ss(rc, "\0", "\0");
    }

    FILE *fp = AllocateFile(filename, "rb");
    if (fp == NULL) {
        if (fctx->isPartitionTable || fctx->isSubPartitionTable) {
            if (fctx->isSubPartitionTable && lnext(fctx->subPartitionCell)) {
                fctx->subPartitionCell = lnext(fctx->subPartitionCell);
                fctx->relationpath = getRelpath(fctx->subPartitionCell, fctx->rel, true, fctx->forkName);
                fctx->segcount = 0;
                errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s", fctx->relationpath);
                securec_check_ss(rc, "\0", "\0");
            } else if (fctx->isPartitionTable && lnext(fctx->partitionCell)) {
                fctx->partitionCell = lnext(fctx->partitionCell);                
                fctx->relationpath = getRelpath(fctx->partitionCell, fctx->rel, false, fctx->forkName);
                fctx->segcount = 0;
                errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s", fctx->relationpath);
                securec_check_ss(rc, "\0", "\0");
            } else {
                if (fctx->indexoidlist != NULL) {
                    if (fctx->isFirstIndexOid) {
                        fctx->indexCell = list_head(fctx->indexoidlist);
                        fctx->isFirstIndexOid=false;
                        Oid indexId = lfirst_oid(fctx->indexCell);
                        Relation currentIndex = index_open(indexId, AccessShareLock);
                        fctx->relationpath = relpathpg(currentIndex, fctx->forkName);
                        fctx->segcount = 0;
                        index_close(currentIndex, NoLock);
                        errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s", fctx->relationpath);
                        securec_check_ss(rc, "\0", "\0");
                    } else {
                        if (lnext(fctx->indexCell)) {
                            fctx->indexCell = lnext(fctx->indexCell);
                            Oid indexId = lfirst_oid(fctx->indexCell);
                            Relation currentIndex = index_open(indexId, AccessShareLock);
                            fctx->relationpath = relpathpg(currentIndex, fctx->forkName);
                            fctx->segcount = 0;
                            index_close(currentIndex, NoLock);
                            errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s", fctx->relationpath);
                            securec_check_ss(rc, "\0", "\0");
                        }
                    }
                }
            }
        } else {
            //process index 
            if (fctx->indexoidlist != NULL) {
                if (fctx->isFirstIndexOid) {
                    fctx->indexCell = list_head(fctx->indexoidlist);
                    fctx->isFirstIndexOid=false;
                    Oid indexId = lfirst_oid(fctx->indexCell);
                    Relation currentIndex = index_open(indexId, AccessShareLock);
                    fctx->relationpath = relpathpg(currentIndex, fctx->forkName);
                    fctx->segcount = 0;
                    index_close(currentIndex, NoLock);
                    errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s", fctx->relationpath);
                    securec_check_ss(rc, "\0", "\0");
                } else {
                    if (lnext(fctx->indexCell)) {
                        fctx->indexCell = lnext(fctx->indexCell);
                        Oid indexId = lfirst_oid(fctx->indexCell);
                        Relation currentIndex = index_open(indexId, AccessShareLock);
                        fctx->relationpath = relpathpg(currentIndex, fctx->forkName);
                        fctx->segcount = 0;
                        index_close(currentIndex, NoLock);
                        errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH-1, "%s", fctx->relationpath);
                        securec_check_ss(rc, "\0", "\0");
                    }
                }
            }
        }
    } else {
        FreeFile(fp);
    }

    ereport(DEBUG1, (errmsg("pgfincore: about to work with %s", filename)));

    /*
     * Call pgfincore with the advice, returning the structure
     */
    pgfncr = (pgfincoreStruct *) palloc(sizeof(pgfincoreStruct));
    result = pgfincore_file(filename, pgfncr);

    /*
    * When we have work with all segment of the current relation, test success
    * We exit from the SRF
    */
    if (!result) {
        ereport(DEBUG1, (errmsg("pgfincore: closing %s", fctx->relationpath)));
        
        if (fctx->isPartitionTable) {
            releasePartitionList(fctx->rel, &(fctx->partitionIdList), AccessShareLock);
        } else if (fctx->isSubPartitionTable) {
            releasePartitionList(fctx->rel, &(fctx->subPartitionIdList), AccessShareLock);
        }
        relation_close(fctx->rel, AccessShareLock);
        list_free(fctx->indexoidlist);
        pfree(fctx);
        SRF_RETURN_DONE(funcctx);
    } else {
        /*
        * Postgresql stuff to return a tuple
        */
        HeapTuple tuple;
        Datum values[PGFINCORE_COLS];
        bool nulls[PGFINCORE_COLS];

        /* initialize nulls array to build the tuple */
        int ret = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(ret, "\0", "\0");

        /* Filename */
        values[0] = CStringGetTextDatum(filename);
        /* Segment Number */
        values[1] = Int32GetDatum(fctx->segcount);
        /* os page size */
        values[2] = Int64GetDatum(pgfncr->pageSize);
        /* number of pages used by segment */
        values[3] = Int64GetDatum(pgfncr->rel_os_pages);
        /* number of pages in OS cache */
        values[4] = Int64GetDatum(pgfncr->pages_mem);
        /* number of group of contigous page in os cache */
        values[5] = Int64GetDatum(pgfncr->group_mem);
        /* free page cache */
        values[6] = Int64GetDatum(pgfncr->pagesFree);
        /* the map of the file with bit set for in os cache page */
        if (fctx->getvector && pgfncr->rel_os_pages) {
            values[7] = VarBitPGetDatum(pgfncr->databit);
        } else {
            nulls[7]  = true;
            values[7] = (Datum) NULL;
        }
        /* number of pages dirty in OS cache */
        values[8] = Int64GetDatum(pgfncr->pages_dirty);
        /* number of group of contigous dirty pages in os cache */
        values[9] = Int64GetDatum(pgfncr->group_dirty);
        /* Build the result tuple. */
        tuple = heap_form_tuple(fctx->tupd, values, nulls);

        /* prepare the number of the next segment */
        fctx->segcount++;

        /* Ok, return results, and go for next call */
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
}

/*
 * pgfincore_drawer A very naive renderer. (for testing)
 */
Datum pgfincore_drawer(PG_FUNCTION_ARGS)
{
    char *result,*r;
    int  len,i,k;
    VarBit *databit;
    bits8 *sp;
    bits8 x;

    if (PG_ARGISNULL(0))
        ereport(ERROR, (errmsg("pgfincore_drawer: databit argument shouldn't be NULL")));

    databit	= PG_GETARG_VARBIT_P(0);

    len =  VARBITLEN(databit);
    result = (char *) palloc((len/FINCORE_BITS) + 1);
    sp = VARBITS(databit);
    r = result;

    for (i = 0; i <= len - BITS_PER_BYTE; i += BITS_PER_BYTE, sp++) {
        x = *sp;
        /*  Is this bit set ? */
        for (k = 0; k < (BITS_PER_BYTE/FINCORE_BITS); k++) {
            char out = ' ';
            if (IS_HIGHBIT_SET(x))
                out = '.' ;
            x <<= 1;
            if (FINCORE_BITS > 1) {
                if (IS_HIGHBIT_SET(x))
                    out = '*';
                x <<= 1;
            }
            *r++ = out;
        }
    }
    if (i < len) {
        /* print the last partial byte */
        x = *sp;
        for (k = i; k < (len/FINCORE_BITS); k++) {
            char out = ' ';
            if (IS_HIGHBIT_SET(x))
                out = '.' ;
            x <<= 1;
            if (FINCORE_BITS > 1) {
                if (IS_HIGHBIT_SET(x))
                    out = '*';
                x <<= 1;
            }
            *r++ = out;
        }
    }

    *r = '\0';
    PG_RETURN_CSTRING(result);
}
