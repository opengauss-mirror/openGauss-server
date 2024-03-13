/* -------------------------------------------------------------------------
 *
 * analyze.cpp
 *	  the openGauss statistics generator
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/analyze.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/nbtree.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>

#include "access/heapam.h"
#include "access/transam.h"
#include "access/tupconvert.h"
#include "access/tuptoaster.h"
#include "access/ustore/knl_upage.h"
#include "access/ustore/knl_uvisibility.h"
#include "access/visibilitymap.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_hashbucket_fn.h"
#include "catalog/namespace.h"
#include "catalog/storage_gtt.h"
#include "commands/dbcommands.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/vacuum.h"
#include "db4ai/db4ai_api.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "foreign/fdwapi.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "nodes/print.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "pgstat.h"
#include "pgxc/groupmgr.h"
#include "postmaster/autovacuum.h"
#include "storage/cucache_mgr.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/acl.h"
#include "utils/aiomem.h"
#include "utils/attoptcache.h"
#include "utils/batchstore.h"
#include "utils/datum.h"
#include "utils/extended_statistics.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/snapmgr.h"
#include "utils/sortsupport.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "tcop/utility.h"
#include "tcop/dest.h"
#include "access/multixact.h"
#include "optimizer/aioptimizer.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/utils/ts_relcache.h"
#endif   /* ENABLE_MULTIPLE_NODES */

#if defined(ENABLE_UT) || defined(ENABLE_QUNIT)
#define static
#endif

/* Per-index data for ANALYZE */
typedef struct AnlIndexData {
    IndexInfo* indexInfo;        /* BuildIndexInfo result */
    double tupleFract;           /* fraction of rows for partial index */
    VacAttrStats** vacattrstats; /* index attrs to analyze */
    int attr_cnt;
} AnlIndexData;

typedef struct AnlPrefetchList {
    BlockNumber* blocklist;
    uint32 anl_idx;
    uint32 load_count;
    uint32 size; /* list element count */
} AnlPrefetchList;

typedef struct AnlPrefetch {
    AnlPrefetchList* blocklist; /* point current used prefetch list */
    AnlPrefetchList fetchlist1; /* fetchlist1 and fetchlist2 is ping pang list to keep prefetch blocks/cus */
    AnlPrefetchList fetchlist2;
    bool init; /* whether the prefetch list inited done or not */
} AnlPrefetch;

/* Default statistics target (GUC parameter) */
THR_LOCAL int default_statistics_target = 100;

/*
 * Currently, sample average tuple number on each ORC file for DFS table analyze.
 * In order to use eliminate algorithm, the parameter ANALYZE_FACTOR is given.
 * When using the paramter, each ORC file will be scanned the twice average numbers on. In this
 * way, the eliminate algorithm will be used.
 * The value of parameter comes from tpcds500x and tpcds1000x test.
 * The test result is the following:
 * 1. 3 Physical machine(21dn)
 *
 *                              500x(fixed analyze)   500x(svn6120 analyze)
 * non-partition DFS table      4421.739s             4405.214s
 * partition DFS table          7197s                 10271s
 *
 * 2. 12 Physical machine(84dn)
 *                              1000x(fixed analyze)  1000x(svn6061 analyze)
 * non-partition DFS table      4680s                 6805s
 */
#define ESTIMATE_BLOCK_FACTOR 0.65
#define EPSILON 1.0E-06

/* maximum relative error fraction of histogram */
#define ANALYZE_RELATIVE_ERROR ((double)(0.5))

/* The bound of most common value and histgram in pg_statisic for each column. */
#define MAX_ATTR_MCV_STAT_TARGET ((int)(100))
#define MAX_ATTR_HIST_STAT_TARGET ((int)(301))

/* The minimum count of most common value. */
#define MIN_MCV_COUNT 2
#define DEFAULT_ATTR_TARGET 100
#define DEFAULT_COLUMN_NUM 2
#define DEFAULT_SAMPLERATE 0.02
#define DEFAULT_EST_TARGET_ROWS 300
const char* const ANALYZE_TEMP_TABLE_PREFIX = "pg_analyze";

/*
 * If get sample rows on datanode or not. there are two cases:
 * 1. sampleRate >= 0 for hash or roundrobin table;
 * 2. sampleRate == 0 for replication table.
 */
#define NEED_GET_SAMPLE_ROWS_DN(vacstmt)                                                          \
    (IS_PGXC_DATANODE && ((DISTTYPE_REPLICATION != (vacstmt)->disttype &&                         \
                              (vacstmt)->pstGlobalStatEx[(vacstmt)->tableidx].sampleRate >= 0) || \
                             (DISTTYPE_REPLICATION == (vacstmt)->disttype &&                      \
                                 (vacstmt)->pstGlobalStatEx[(vacstmt)->tableidx].sampleRate == 0)))
/* We need delete pg_statisitc when total rows is 0. */
#define NEED_DELETE_STATS(vacstmt, totalrows)                                                                      \
    ((IS_PGXC_DATANODE && ((totalrows) == 0)) || (IS_PGXC_COORDINATOR && !IsConnFromCoord() && 0 == (totalrows) && \
                                                     !(vacstmt)->pstGlobalStatEx[(vacstmt)->tableidx].isReplication))
/*
 * We need do analyze to compute statistic for sample rows only on datanode or
 * required sample rows on coordinator.
 */
#define NEED_ANALYZE_SAMPLEROWS(vacstmt) ((IS_PGXC_DATANODE && !IS_SINGLE_NODE) || !(vacstmt)->sampleTableRequired)
/*
 * We need do analyze to compute statistic for sample table only
 * required sample table on coordinator.
 */
#define NEED_ANALYZE_SAMPLETABLE(vacstmt) \
    (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !(vacstmt)->pstGlobalStatEx[(vacstmt)->tableidx].isReplication)
/* We need do the second sampling for sample rows when sample rate more than 0. */
#define NEED_DO_SECOND_ANALYZE(analyzemode, vacstmt)                                                           \
    (IS_PGXC_DATANODE && IsConnFromCoord() &&                                                                  \
        ((ANALYZENORMAL == (analyzemode) && (vacstmt)->pstGlobalStatEx[(vacstmt)->tableidx].sampleRate > 0)))
/* We should send real total row count to original coordinator if the count more than 0 on datanode. */
#define NEED_SEND_TOTALROWCNT_TO_CN(analyzemode, vacstmt, totalrows)                                      \
    (ANALYZENORMAL == (analyzemode) && (totalrows) > 0)

static void do_analyze_rel(Relation onerel, VacuumStmt* vacstmt, BlockNumber relpages, bool inh, int elevel,
    AnalyzeMode mode = ANALYZENORMAL);
int compare_rows(const void* a, const void* b);
static void compute_index_stats(Relation onerel, double totalrows, AnlIndexData* indexdata, int nindexes,
    HeapTuple* rows, int numrows, MemoryContext col_context);
static VacAttrStats* examine_attribute(Relation onerel, int attnum, Node* index_expr);
template <bool estimate_table_rownum>
static int64 acquire_sample_rows(
    Relation onerel, int elevel, HeapTuple* rows, int64 targrows, double* totalrows, double* totaldeadrows);
template <bool estimate_table_rownum>
static int64 acquire_inherited_sample_rows(
    Relation onerel, int elevel, HeapTuple* rows, int64 targrows, double* totalrows, double* totaldeadrows);
template <bool estimate_table_rownum>
static int64 acquirePartitionedSampleRows(Relation onerel, VacuumStmt* vacstmt, int elevel, HeapTuple* rows,
    int64 targrows, double* totalrows, double* totaldeadrows, VacAttrStats** vacattrstats, int attrAnalyzeNum);

static Datum std_fetch_func(VacAttrStatsP stats, int rownum, bool* isNull, Relation rel);
static Datum ind_fetch_func(VacAttrStatsP stats, int rownum, bool* isNull, Relation rel);

static int get_one_tuplesize(Relation onerel, int total_width);
static int64 get_workmem_allow_rows(Relation onerel, int total_width);

template <bool estimate_table_rownum>
static int64 AcquireSampleCStoreRows(Relation onerel, int elevel, HeapTuple* rows, int64 targrows, double* totalrows,
    double* totaldeadrows, VacAttrStats** vacattrstats, int attrAnalyzeNum);

static void send_totalrowcnt_to_cn(VacuumStmt* vacstmt, AnalyzeMode analyzemode, double totalrows);
static void do_sampling_normal_second(
    Relation onerel, VacuumStmt* vacstmt, int64 numrows, double totalrows, HeapTuple* rows);
static bool equalTupleDescAttrsTypeid(TupleDesc tupdesc1, TupleDesc tupdesc2);
void CstoreAnalyzePrefetch(
    CStoreScanDesc cstoreScan, BlockNumber* cuidList, int32 cuidListCount, int analyzeAttrNum, int32* attrSeq);

static void get_sample_rows_for_query(
    MemoryContext speccontext, Relation rel, VacuumStmt* vacstmt, int64* num_sample_rows, HeapTuple** samplerows);
static ArrayType* es_construct_extended_statistics(VacAttrStats* stats, int k);
template <bool isSingleColumn>
static void update_stats_catalog(
    Relation pgstat, MemoryContext oldcontext, Oid relid, char relkind, bool inh, VacAttrStats* stats, int natts,
    char relpersistence);

static BlockNumber estimate_psort_index_blocks(TupleDesc desc, double totalTuples);
static BlockNumber estimate_btree_index_blocks(TupleDesc desc, double totalTuples, double table_factor = 1.0);

/* The sample info of special attribute for compute statistic for index or type of tsvector. */
typedef struct {
    MemoryContext memorycontext; /* current memory context for sample or save stats. */
    int64 num_sample;            /* how many sample rows we have get for index table. */
    HeapTuple* samplerows;       /* all sample rows we have get for index table. */
    TupleDesc tupDesc;           /* tuple desc of current relation. */
} AnalyzeSampleRowsSpecInfo;

/* The special infomation of compute statistic for multi-columns results. */
typedef struct {
    MemoryContext memoryContext; /* current memory context for sample or save stats. */
    int num_cols;                /* how many columns in the result. */
    int64 num_rows;              /* how many rows in the result. */
    ArrayBuildState** output;    /* saved value of result array. */
    bool compute_histgram;       /* identify the results using to comput histgram. */
    int64 non_mcv_num;           /* num of non-mcv which decide the histgram value is the last or not. */
    HistgramInfo hist_list;      /* histgram list for compute stats. */
    TupleDesc spi_tupDesc;       /* tuple desc of current mcv or histgram values. */
} AnalyzeResultMultiColAsArraySpecInfo;

static bool do_analyze_samplerows(Relation onerel, VacuumStmt* vacstmt, int attr_cnt, VacAttrStats** vacattrstats,
    bool hasindex, int nindexes, AnlIndexData* indexdata, bool inh, Relation* Irel, double* totalrows, int64* numrows,
    HeapTuple* rows, AnalyzeMode analyzemode, MemoryContext caller_context,
    Oid save_userid, int save_sec_context, int save_nestlevel);

static void do_analyze_sampletable(Relation onerel, VacuumStmt* vacstmt, int attr_cnt, VacAttrStats** vacattrstats,
    bool hasindex, int nindexes, AnlIndexData* indexdata, bool inh, Relation* Irel, double totalrows, int64 numrows,
    AnalyzeMode analyzemode);

static void do_analyze_compute_attr_stats(bool inh, Relation onerel, VacuumStmt* vacstmt, VacAttrStats* stats,
    double numTotalRows, int64 numSampleRows, AnalyzeMode analyzemode);

static void set_stats_dndistinct(VacAttrStats* stats, VacuumStmt* vacstmt, int tableidx);
static void update_pages_and_tuples_pgclass(Relation onerel, VacuumStmt* vacstmt, int attr_cnt,
    VacAttrStats** vacattrstats, bool hasindex, int nindexes, AnlIndexData* indexdata, Relation* Irel,
    BlockNumber relpages, double totalrows, double totaldeadrows, int64 numrows, bool inh);
static bool analyze_index_sample_rows(MemoryContext speccontext, Relation rel, VacuumStmt* vacstmt, int nindexes,
    AnlIndexData* indexdata, int64* num_sample_rows, HeapTuple** samplerows);
static void compute_histgram_final(int* slot_idx, HistgramInfo* hist_list, VacAttrStats* stats);
static void compute_histgram_internal(AnalyzeResultMultiColAsArraySpecInfo* spec);
static void analyze_rel_internal(Relation onerel, VacuumStmt* vacstmt, BufferAccessStrategy bstrategy,
    AnalyzeMode analyzemode = ANALYZENORMAL);
static void analyze_tmptbl_debug_dn(AnalyzeTempTblDebugStage type, AnalyzeMode analyzemode, List* tmpSampleTblNameList,
    Oid* sampleTableOid, Relation* tmpRelation, HeapTuple tuple, TupleConversionMap* tupmap);
static TupleConversionMap* check_tmptbl_tupledesc(Relation onerel, TupleDesc temptbl_desc);
static double compute_com_size(VacuumStmt* vacstmt, Relation rel, int tableidx);

#ifndef ENABLE_MULTIPLE_NODES
static void update_numSamplerows(int attr_cnt, VacAttrStats** vacattrstats, HeapTuple* rows, int64 numrows);
#endif

/*
 *	analyze_get_relation() -- get one relation by relid before do analyze.
 *	@in relid - the relation id for analyze or vacuum
 *	@in vacstmt - the statment for analyze or vacuum command
 */
Relation analyze_get_relation(Oid relid, VacuumStmt* vacstmt)
{
    Relation onerel = NULL;
    bool GetLock = false;
    LOCKMODE lockmode = NEED_EST_TOTAL_ROWS_DN(vacstmt) ? AccessShareLock : ShareUpdateExclusiveLock;

    /*
     * Check for user-requested abort.
     */
    CHECK_FOR_INTERRUPTS();

    /*
     * Open the relation, getting ShareUpdateExclusiveLock to ensure that two
     * ANALYZEs don't run on it concurrently.  (This also locks out a
     * concurrent VACUUM, which doesn't matter much at the moment but might
     * matter if we ever try to accumulate stats on dead tuples.) If the rel
     * has been dropped since we last saw it, we don't need to process it.
     */
    if ((vacuumRelation(vacstmt->flags) || vacuumMainPartition(vacstmt->flags)) &&
        !(vacstmt->options & VACOPT_NOWAIT)) {
        onerel = try_relation_open(relid, lockmode);
        GetLock = true;
    } else if ((vacuumRelation(vacstmt->flags) || vacuumMainPartition(vacstmt->flags)) &&
               ConditionalLockRelationOid(relid, lockmode)) {
        onerel = try_relation_open(relid, NoLock);
        GetLock = true;
    }

    if (!GetLock) {
        onerel = NULL;
        if (IsAutoVacuumWorkerProcess() && u_sess->attr.attr_storage.Log_autovacuum_min_duration >= 0)
            ereport(LOG,
                (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                    errmsg("skipping analyze of \"%s\" --- lock not available", vacstmt->relation->relname)));
    }

    return onerel;
}

/*
 * Description: The entry for analyze relation.
 *
 * Parameters:
 *	@in relid: relation oid
 *	@in vacstmt: the statment for analyze or vacuum command
 *	@in bstrategy: buffer access strategy objects
 *
 * Returns: void
 */
void analyze_rel(Oid relid, VacuumStmt* vacstmt, BufferAccessStrategy bstrategy)
{
    /*
     * try open the relation, we will skip the relation if try open failed.
     * the relation will close in analyze_rel_internal().
     */
    Relation onerel = analyze_get_relation(relid, vacstmt);

    if (STMT_RETRY_ENABLED) {
        // do noting for now, if query retry is on, just to skip validateTempRelation here
    } else if (onerel != NULL && onerel->rd_rel != NULL && 
        onerel->rd_rel->relpersistence == RELPERSISTENCE_TEMP && !validateTempNamespace(onerel->rd_rel->relnamespace)) {
            relation_close(onerel, NEED_EST_TOTAL_ROWS_DN(vacstmt) ? AccessShareLock : ShareUpdateExclusiveLock);
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEMP_OBJECTS),
                    errmsg("Temp table's data is invalid because datanode %s restart. "
                       "Quit your session to clean invalid temp tables.", 
                       g_instance.attr.attr_common.PGXCNodeName)));
    }

    /*
     * do analyze If have onerel, otherwise,we should CommitTransaction
     * if it has StartTransaction already.
     */
    if (onerel) {
        /* do analyze for normal table. */
        analyze_rel_internal(onerel, vacstmt, bstrategy, ANALYZENORMAL);

        /* 
         * Reset attcacheoff values in the tupleDesc of the relation 
         * to prevent ustore tuples from using the heap offset
         * values calculated while computing stats.
         */
        if (RelationIsUstoreFormat(onerel)) {
            for (int i = 0; i < onerel->rd_att->natts; i++) {
                if (onerel->rd_att->attrs[i].attcacheoff >= 0) {
                    onerel->rd_att->attrs[i].attcacheoff = -1;
                }
            }
        }
    }
}

/*
 * Description: catch error for update or delete pg_statistic concurrency.
 *
 * Parameters:
 * 	@in relid:  the relation oid for analyze or vacuum
 *	@in attnum: the column no for analyze concurrency updating
 * 	@in oldcontext: old memory context saved before catch error data
 * 	@in funcname: function name of caller
 * Returns: void
 */
void analyze_concurrency_process(Oid relid, int16 attnum, MemoryContext oldcontext, const char* funcname)
{
    ErrorData* edata = NULL;

    /* Save error info */
    (void)MemoryContextSwitchTo(oldcontext);
    edata = CopyErrorData();
    FlushErrorState();

    if (edata->sqlerrcode == ERRCODE_T_R_SERIALIZATION_FAILURE) {
        ereport(NOTICE,
            (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                errmsg("skipping \"%s.%s\" --- encounter concurrency analyze in function %s on %s: %s.",
                    get_rel_name(relid),
                    (attnum > 0) ? get_attname(relid, attnum) : "extended_stats",
                    funcname,
                    g_instance.attr.attr_common.PGXCNodeName,
                    edata->message),
                handle_in_client(true)));

        /* release error state */
        FreeErrorData(edata);
    } else {
        ReThrowError(edata);
    }
}

BlockNumber GetSubPartitionNumberOfBlocks(Relation rel, Partition part, LOCKMODE lockmode)
{
    Relation partRel = partitionGetRelation(rel, part);
    List *subPartList = NIL;
    ListCell *partCell = NULL;
    BlockNumber relpages = 0;

    subPartList = relationGetPartitionList(partRel, lockmode);

    foreach (partCell, subPartList) {
        Partition subPart = (Partition)lfirst(partCell);
        relpages += PartitionGetNumberOfBlocks(partRel, subPart);
    }

    releasePartitionList(partRel, &subPartList, NoLock);
    releaseDummyRelation(&partRel);
    return relpages;
}

/*
 * Description: Analyze one relation
 *
 * Parameters:
 *	@in onerel: relation for analyze
 *	@in vacstmt: the statment for analyze or vacuum command
 *	@in bstrategy: buffer access strategy objects
 *	@in analyzemode - identify which type the table, dfs table or delta table
 */
static void analyze_rel_internal(Relation onerel, VacuumStmt* vacstmt, BufferAccessStrategy bstrategy,
    AnalyzeMode analyzemode)
{
    AcquireSampleRowsFunc acquirefunc = NULL;
    int elevel;
    int messageLevel;
    BlockNumber relpages = 0;
    List* partList = NIL;
    LOCKMODE lockmode = NEED_EST_TOTAL_ROWS_DN(vacstmt) ? AccessShareLock : ShareUpdateExclusiveLock;

    AssertEreport(onerel, MOD_OPT, "onrel can not be NULL when doing analyze");

    /* Set up static variables */
    u_sess->analyze_cxt.vac_strategy = bstrategy;

    messageLevel = WARNING;
    elevel = DEBUG2;

    if (vacstmt->options & VACOPT_VERBOSE) {
        messageLevel = VERBOSEMESSAGE;
        elevel = VERBOSEMESSAGE;
    }

    /*
     * Check permissions --- this should match vacuum's check!
     */
    AclResult aclresult = pg_class_aclcheck(RelationGetPgClassOid(onerel, false), GetUserId(), ACL_VACUUM);
    if (aclresult != ACLCHECK_OK && !(pg_class_ownercheck(RelationGetPgClassOid(onerel, false), GetUserId()) ||
            (pg_database_ownercheck(u_sess->proc_cxt.MyDatabaseId, GetUserId()) && !onerel->rd_rel->relisshared) ||
                (isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode))) {
        /* No need for a WARNING if we already complained during VACUUM */
        if (!(vacstmt->options & VACOPT_VACUUM)) {
            if (onerel->rd_rel->relisshared)
                ereport(messageLevel,
                    (errmsg("skipping \"%s\" --- only system admin can analyze it", RelationGetRelationName(onerel))));
            else if (onerel->rd_rel->relnamespace == PG_CATALOG_NAMESPACE)
                ereport(messageLevel,
                    (errmsg("skipping \"%s\" --- only system admin or database owner can analyze it",
                        RelationGetRelationName(onerel))));
            else
                ereport(messageLevel,
                    (errmsg("skipping \"%s\" --- only table or database owner can analyze it",
                        RelationGetRelationName(onerel))));
        }

        relation_close(onerel, lockmode);

        return;
    }

    /*
     * Silently ignore tables that are temp tables of other backends ---
     * trying to analyze these is rather pointless, since their contents are
     * probably not up-to-date on disk.  (We don't throw a warning here; it
     * would just lead to chatter during a database-wide ANALYZE.)
     */
    if (RELATION_IS_OTHER_TEMP(onerel)) {
        relation_close(onerel, lockmode);
        return;
    }

    if (RELATION_IS_GLOBAL_TEMP(onerel) && !gtt_storage_attached(RelationGetRelid(onerel))) {
        relation_close(onerel, ShareUpdateExclusiveLock);
        return;
    }

    /*
     * We can ANALYZE any table except pg_statistic. See update_attstats
     */
    if (RelationGetRelid(onerel) == StatisticRelationId) {
        AssertEreport(RelationIsNonpartitioned(onerel), MOD_OPT, "pg_statistic can not be a partitioned table.");

        if (!IsInitdb && !IS_SINGLE_NODE) {
            elog(WARNING, "System catalog pg_statistic can not be analyzed, skip it.");
        }

        relation_close(onerel, lockmode);
        return;
    }

    /*
     * Check that it's a plain table or foreign table; we used to do this in
     * get_rel_oids() but seems safer to check after we've locked the
     * relation.
     */
    if (onerel->rd_rel->relkind == RELKIND_RELATION ||
        onerel->rd_rel->relkind == RELKIND_MATVIEW) {
        /* Regular table, so we'll use the regular row acquisition function */
        /* Also get regular table's size */
        if (RelationIsPartitioned(onerel)) {
            Partition part = NULL;
            ListCell* partCell = NULL;

            partList = relationGetPartitionList(onerel, lockmode);

            foreach (partCell, partList) {
                part = (Partition)lfirst(partCell);

                if (RelationIsSubPartitioned(onerel)) {
                    relpages += GetSubPartitionNumberOfBlocks(onerel, part, lockmode);
                } else {
                    relpages += PartitionGetNumberOfBlocks(onerel, part);
                }
            }
        } else {
            relpages = RelationGetNumberOfBlocks(onerel);
        }
    } else if (onerel->rd_rel->relkind == RELKIND_FOREIGN_TABLE 
               || onerel->rd_rel->relkind == RELKIND_STREAM) {
        /*
         * @hdfs
         * For a foreign table, call FDW's hook function to see whether it
         * supports analysis or not.
         */
        bool retValue = false;
        FdwRoutine* fdwroutine = GetFdwRoutineForRelation(onerel, false);

        /* Support analyze or not */
        if (NULL != fdwroutine->AnalyzeForeignTable) {
            /* Implement GetFdwType interface or not and file type is HDFS_ORC*/
            if (isObsOrHdfsTableFormTblOid(RelationGetRelid(onerel)) ||
                (IS_OBS_CSV_TXT_FOREIGN_TABLE(RelationGetRelid(onerel)) && !isWriteOnlyFt(RelationGetRelid(onerel)))) {
                /* pass data used by AnalyzeForeignTable */
                retValue = fdwroutine->AnalyzeForeignTable(
                    onerel, &acquirefunc, &relpages, (void*)vacstmt->HDFSDnWorkFlow, false);
            } else {
                /* other types of foreign table */
                retValue = fdwroutine->AnalyzeForeignTable(onerel, &acquirefunc, &relpages, 0, false);
            }

            if (!retValue) {
                /* Supress warning info for mysql_fdw */
                messageLevel = isMysqlFDWFromTblOid(RelationGetRelid(onerel)) ? LOG : messageLevel;
                ereport(messageLevel,
                    (errmsg(
                        "Skipping \"%s\" --- cannot analyze this foreign table.", RelationGetRelationName(onerel))));
                relation_close(onerel, lockmode);
                return;
            }

        } else {
            ereport(messageLevel,
                (errmsg("Table %s doesn't support analysis operation.", RelationGetRelationName(onerel))));
            relation_close(onerel, lockmode);
            return;
        }
    } else {
        /* No need for a WARNING if we already complained during VACUUM */
        if (!(vacstmt->options & VACOPT_VACUUM))
            ereport(messageLevel,
                (errmsg("skipping \"%s\" --- cannot analyze non-tables or special system tables",
                    RelationGetRelationName(onerel))));

        if (RelationIsPartitioned(onerel)) {
            releasePartitionList(onerel, &partList, lockmode);
        }

        relation_close(onerel, lockmode);
        return;
    }

    /*
     * OK, let's do it.  First let other backends know I'm in ANALYZE.
     */
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    t_thrd.pgxact->vacuumFlags |= PROC_IN_ANALYZE;
    LWLockRelease(ProcArrayLock);

    /* Also get regular table's size */
    if (RelationIsPartitioned(onerel)) {
        vacstmt->partList = partList;
    }

    /*
     * Do the normal non-recursive ANALYZE.
     */
    do_analyze_rel(onerel, vacstmt, relpages, false, elevel, analyzemode);
	
    /*
     * If there are child tables, do recursive ANALYZE.
     */
    if (onerel->rd_rel->relhassubclass)
        do_analyze_rel(onerel, vacstmt, relpages, true, elevel, analyzemode);

    /*
     * Close source relation now, but keep lock so that no one deletes it
     * before we commit.  (If someone did, they'd fail to clean up the entries
     * we made in pg_statistic.  Also, releasing the lock before commit would
     * expose us to concurrent-update failures in update_attstats.)
     */
    if (RelationIsPartitioned(onerel)) {
        releasePartitionList(onerel, &partList, NoLock);
    }

    relation_close(onerel, NoLock);

    /*
     * Reset my PGXACT flag.  Note: we need this here, and not in vacuum_rel,
     * because the vacuum flag is cleared by the end-of-xact code.
     */
    LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
    t_thrd.pgxact->vacuumFlags &= ~PROC_IN_ANALYZE;
    LWLockRelease(ProcArrayLock);
}

// to decide whether use the percent to collect statistics or not
bool whether_use_percent(VacAttrStats** vacattrstats, int attr_cnt, int nindexes, AnlIndexData* indexdata)
{
    int i;
    Form_pg_attribute attr;
    if (default_statistics_target < 0)
        return true;
    for (i = 0; i < attr_cnt; i++) {
        AssertEreport(vacattrstats[i] != NULL, MOD_OPT, "vacattrstats[i] can not be NULL");
        attr = vacattrstats[i]->attrs[0];
        if (attr->attstattarget < -1) {
            return true;
        }
    }

    for (int ind = 0; ind < nindexes; ind++) {
        AssertEreport(indexdata != NULL, MOD_OPT, "indexdata[i] can not be NULL");
        AnlIndexData* thisdata = &indexdata[ind];

        for (i = 0; i < thisdata->attr_cnt; i++) {
            attr = thisdata->vacattrstats[i]->attrs[0];
            if (attr->attstattarget < -1) {
                return true;
            }
        }
    }

    return false;
}

// Get final sample rows by percent
static void caculate_target_rows(
    Form_pg_attribute attr, int64 totalrows, int64* target_rows, int64 workmem_allow_rows, bool use_percent)
{
    int64 temp_rows = 0;

    /* Don't usee percent, we should get the max value between workmem_allow_rows and target rows. */
    if (use_percent) {
        if (attr->attstattarget < -1) {
            attr->attstattarget = (attr->attstattarget + 1) * (-1);
            temp_rows = totalrows * attr->attstattarget / 100;
        } else if (attr->attstattarget < 0) {
            if (default_statistics_target < 0) {
                attr->attstattarget = default_statistics_target * (-1);
                temp_rows = totalrows * attr->attstattarget / 100;
            } else {
                attr->attstattarget = default_statistics_target;
                temp_rows = DEFAULT_EST_TARGET_ROWS * (int64)attr->attstattarget;
            }
        } else
            temp_rows = DEFAULT_EST_TARGET_ROWS * (int64)attr->attstattarget;

        *target_rows = rtl::max(*target_rows, temp_rows);  // sample rows must be greater than 30000
    }

    *target_rows = rtl::min(*target_rows, workmem_allow_rows);  // sample rows must be less than workmem_allow_rows

    /* attstattarget's value rely on final sample rows */
    attr->attstattarget = *target_rows / DEFAULT_EST_TARGET_ROWS;

    if (attr->attstattarget < 1)
        attr->attstattarget = 1;
    if (attr->attstattarget > 10000)
        attr->attstattarget = 10000;
}

static int get_total_width(
    VacAttrStats** vacattrstats, int attr_cnt, int nindexes, AnlIndexData* indexdata, Relation rel)
{
    int total_width1 = 0;
    int total_width = 0;
    int i = 0;
    int32 item_width = 0;
    bool isPartition = RelationIsPartition(rel);
    Form_pg_attribute attr;

    for (i = 0; i < attr_cnt; i++) {
        for (unsigned int j = 0; j < vacattrstats[i]->num_attrs; ++j) {
            attr = vacattrstats[i]->attrs[j];

            /* This should match set_rel_width() in costsize.c */
            item_width = get_attavgwidth(RelationGetRelid(rel), attr->attnum, isPartition);
            if (item_width <= 0) {
                item_width = get_typavgwidth(attr->atttypid, attr->atttypmod);
                Assert(item_width > 0);
            }

            total_width += item_width;
        }
    }

    for (int ind = 0; ind < nindexes; ind++) {
        AnlIndexData* thisdata = &indexdata[ind];
        total_width1 = 0;

        for (i = 0; i < thisdata->attr_cnt; i++) {
            for (unsigned int j = 0; j < thisdata->vacattrstats[i]->num_attrs; ++j) {
                attr = thisdata->vacattrstats[i]->attrs[j];

                /* This should match set_rel_width() in costsize.c */
                item_width = get_attavgwidth(RelationGetRelid(rel), attr->attnum, isPartition);
                if (item_width <= 0) {
                    item_width = get_typavgwidth(attr->atttypid, attr->atttypmod);
                    Assert(item_width > 0);
                }

                total_width1 += item_width;
            }
        }

        if (total_width < total_width1) {
            total_width = total_width1;
        }
    }

    return total_width;
}

// get target sample rows by estimate total rows
int64 get_target_rows(Relation onerel, VacAttrStats** vacattrstats, int attr_cnt, int nindexes, AnlIndexData* indexdata,
    int64 totalrows, int64 targrows, bool use_percent)
{
    int i = 0;
    Form_pg_attribute attr;
    int total_width = 0;
    int64 target_rows = DEFAULT_SAMPLE_ROWCNT;  // this is the minmal rownum for using percent
    int64 workmem_allow_rows = 0;

    total_width = get_total_width(vacattrstats, attr_cnt, nindexes, indexdata, onerel);

    /*
     * Notice : get target rows control by work_mem is the temp method,
     * we should resolve the problem in palloc each tuple dynamic later.
     */
    /* Get real tuple size using to caculate workmem_allow_rows. */
    workmem_allow_rows = get_workmem_allow_rows(onerel, total_width);

    if (target_rows > workmem_allow_rows)
        workmem_allow_rows = target_rows;  // when workmem_allow_rows less than 30000,  set workmem_allow_rows = 30000

    /* Don't use percent, we should use real target rows. */
    if (!use_percent)
        target_rows = targrows;

    for (i = 0; i < attr_cnt; i++) {
        attr = vacattrstats[i]->attrs[0];
        caculate_target_rows(attr, totalrows, &target_rows, workmem_allow_rows, use_percent);
    }
    for (int ind = 0; ind < nindexes; ind++) {
        AnlIndexData* thisdata = &indexdata[ind];

        for (i = 0; i < thisdata->attr_cnt; i++) {
            attr = thisdata->vacattrstats[i]->attrs[0];
            caculate_target_rows(attr, totalrows, &target_rows, workmem_allow_rows, use_percent);
        }
    }

    return target_rows;
}

/*
 * Get real tuple size using to caculate workmem_allow_rows.
 */
static int64 get_workmem_allow_rows(Relation onerel, int total_width)
{
    int64 workMem = u_sess->attr.attr_memory.work_mem * 1024L;
    int64 workmem_allow_rows = 0;
    int oneTupleSize = 0;

    /* Get real tuple size using to caculate workmem_allow_rows. */
    oneTupleSize = get_one_tuplesize(onerel, total_width);
    workmem_allow_rows = workMem / oneTupleSize;

    return workmem_allow_rows;
}

/* Get real tuple size for palloc sample rows. */
static int get_one_tuplesize(Relation onerel, int total_width)
{
    int tupleMemSize = 0;

    /* For row store relation and it has compressed. */
    if (!RelationIsColStore(onerel) && RowRelationIsCompressed(onerel))
        tupleMemSize = MaxHeapTupleSize + HEAPTUPLESIZE;
    else
        tupleMemSize = MAXALIGN(total_width) + HEAPTUPLESIZE;

    return tupleMemSize;
}

/*
 * get_total_rows
 *     To get the total rows
 *
 * @param (in) estimate_table_rownum:
 *     Flag to mark wheather estimate sample row number or get sample rows
 *
 * @return:
 *     (estimate_table_rownum = true ) : NULL, estimated sample row number stored in "int64 *numrows"
 *     (estimate_table_rownum = false) : sample data tuples
 */
template <bool estimate_table_rownum>
HeapTuple* get_total_rows(Relation onerel, VacuumStmt* vacstmt, BlockNumber relpages, bool inh, int elevel,
    VacAttrStats** vacattrstats, int attr_cnt, int64 targrows, double* totalrows, double* totaldeadrows,
    int64* numrows)
{
#define DEFAULT_PARTITION_EST_TARGET_ROWS 3
    HeapTuple* rows = NULL;
    int64 target_rows = 0;

    /*
     * IsForeignTable
     */
    Oid foreignTableId = 0;
    bool isForeignTable = false;
    DEBUG_MOD_START_TIMER(MOD_AUTOVAC);
    int64 bufferRead = 0;
    int64 bufferHit = 0;

    if (onerel->pgstat_info != NULL) {
        bufferRead = onerel->pgstat_info->t_counts.t_blocks_fetched;
        bufferHit = onerel->pgstat_info->t_counts.t_blocks_hit;
    }

    if (vacstmt->relation != NULL) {
        foreignTableId = RelationGetRelid(onerel);
        isForeignTable = IsHDFSTableAnalyze(foreignTableId);
    }

    /*
     * Acquire the sample rows
     */
    if (estimate_table_rownum) {
        /* set default target rows for non-partitioned table, except foreign table. */
        if (!(RELATION_IS_PARTITIONED(onerel) && !isForeignTable))
            target_rows = DEFAULT_EST_TARGET_ROWS;
        else {
            /*
             * set target rows as the num of partition in order to
             * be sure we can scan one block for every partition at least when the num of partition is more than 300.
             */
            target_rows =
                Max(DEFAULT_EST_TARGET_ROWS, list_length(vacstmt->partList) * DEFAULT_PARTITION_EST_TARGET_ROWS);
        }
    } else {
        target_rows = targrows;
    }

    rows = (HeapTuple*)palloc0(target_rows * sizeof(HeapTuple));

    if (inh) {
        if (onerel->rd_rel->relhassubclass) {
            *numrows = acquire_inherited_sample_rows<estimate_table_rownum>(
                onerel, elevel, rows, target_rows, totalrows, totaldeadrows);
        }
    } else if (RELATION_IS_PARTITIONED(onerel) && !isForeignTable) {
        /* Table is partitioned but not a foreign table */
        *numrows = acquirePartitionedSampleRows<estimate_table_rownum>(
            onerel, vacstmt, elevel, rows, target_rows, totalrows, totaldeadrows, vacattrstats, attr_cnt);
    } else if (isForeignTable || onerel->rd_rel->relkind == RELKIND_STREAM ||
            (onerel->rd_rel->relkind == RELKIND_FOREIGN_TABLE &&
            (isOracleFDWFromTblOid(RelationGetRelid(onerel)) ||
#ifdef ENABLE_MOT
             isMOTFromTblOid(RelationGetRelid(onerel)) ||
#endif
             isPostgresFDWFromTblOid(RelationGetRelid(onerel))))) {
        /*
         * @hdfs processing foreign table sampling operation 
         * get foreign table FDW routine 
         */
        FdwRoutine* tableFdwRoutine = GetFdwRoutineByRelId(foreignTableId);

        /*
         * A foreign table supports sampling operation or not.
         */
        if (NULL == tableFdwRoutine->AcquireSampleRows) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("Table %s does not support sampling operation for analyze.",
                        RelationGetRelationName(RelationIdGetRelation(foreignTableId)))));
        } else {
            if (isObsOrHdfsTableFormTblOid(foreignTableId) || IS_OBS_CSV_TXT_FOREIGN_TABLE(foreignTableId)) {
                *numrows = (tableFdwRoutine->AcquireSampleRows)(onerel,
                    elevel,
                    rows,
                    target_rows,
                    totalrows,
                    totaldeadrows,
                    (void*)vacstmt->HDFSDnWorkFlow,
                    estimate_table_rownum);
            } else {
                *numrows = (tableFdwRoutine->AcquireSampleRows)(
                    onerel, elevel, rows, target_rows, totalrows, totaldeadrows, 0, estimate_table_rownum);
            }

            *totaldeadrows = 0; /* set totaldeadrows to zero. It is no means to foreign table. */
        }

    } else {
        if (RelationIsColStore(onerel)) {
            if (RelationIsCUFormat(onerel)) {
                /* analyze cu format table */
                *numrows = AcquireSampleCStoreRows<estimate_table_rownum>(
                    onerel, elevel, rows, target_rows, totalrows, totaldeadrows, vacattrstats, attr_cnt);
            }
        } else {
            /*
             * for hash bucket table without range partitions
             * we pretend it as a parition table and re-use the
             * parition sampling function.
             */
            if (RELATION_OWN_BUCKET(onerel))
                *numrows = acquirePartitionedSampleRows<estimate_table_rownum>(
                    onerel, vacstmt, elevel, rows, target_rows, totalrows, totaldeadrows, vacattrstats, attr_cnt);
            else
                *numrows = acquire_sample_rows<estimate_table_rownum>(
                    onerel, elevel, rows, target_rows, totalrows, totaldeadrows);
        }
    }

    if (onerel->pgstat_info != NULL) {
        bufferRead = onerel->pgstat_info->t_counts.t_blocks_fetched - bufferRead;
        bufferHit = onerel->pgstat_info->t_counts.t_blocks_hit - bufferHit;
    }

    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "%s get sample rows for table \"%s\". "
                     "totalrows: %ld, target_rows: %ld, numrows: %ld. Buffer: %ld hit/%ld read.",
        (estimate_table_rownum ? "Estimate" : "Real"),
        NameStr(onerel->rd_rel->relname),
        targrows,
        target_rows,
        *numrows,
        bufferHit,
        bufferRead);

    if (estimate_table_rownum) {
        for (int i = 0; i < *numrows; i++) {
            if (rows[i]) {
                pfree_ext(rows[i]);
                rows[i] = NULL;
            }
        }
        pfree_ext(rows);
        return NULL;
    } else {
        return rows;
    }
}

static BlockNumber estimate_cstore_blocks(
    Relation rel, VacAttrStats** vacAttrStats, int attrCnt, double sampleTuples, double totalTuples, bool dfsStore)
{
    int tuple_width = 0;
    BlockNumber total_pages = 0;
    int i;
    bool isPartition = RelationIsPartition(rel);

    if (totalTuples <= 0) {
        return 0;
    }

    /* just as get_rel_data_width */
    for (i = 1; i <= RelationGetNumberOfAttributes(rel); i++) {
        Form_pg_attribute att = &rel->rd_att->attrs[i - 1];
        int32 item_width = -1;

        if (att->attisdropped)
            continue;

        item_width = get_typlen(att->atttypid);
        if (item_width > 0) {
            /* for fixed-width datatype */
            tuple_width += item_width;
            continue;
        }

        if (sampleTuples > 0) {
            /* get stawidth from current doing stats */
            bool found = false;

            for (int attIdx = 0; attIdx < attrCnt; ++attIdx) {
                VacAttrStats* stats = vacAttrStats[attIdx];

                if (att->attnum == stats->tupattnum) {
                    item_width = stats->stawidth;
                    found = true;
                    break;
                }
            }

            if (found) {
                tuple_width += item_width;
                continue;
            }
        }

        if (item_width <= 0) /* stawidth from existings stats */
            item_width = get_attavgwidth(RelationGetRelid(rel), i, isPartition);

        if (item_width <= 0) /* average width of values of the type */
            item_width = get_typavgwidth(att->atttypid, att->atttypmod);

        Assert(item_width > 0);

        tuple_width += item_width;
    }

    if (dfsStore)
        total_pages = ceil((totalTuples * tuple_width * ESTIMATE_BLOCK_FACTOR) / BLCKSZ);
    else
        total_pages = ceil(totalTuples / RelDefaultFullCuSize) * (RelDefaultFullCuSize * tuple_width / BLCKSZ);

    if (totalTuples > 0 && totalTuples < total_pages)
        total_pages = totalTuples;

    if (totalTuples > 0 && total_pages == 0)
        total_pages = 1;

    return total_pages;
}

#ifdef ENABLE_MULTIPLE_NODES
static BlockNumber estimate_tsstore_blocks(Relation rel, int attrCnt, double totalTuples)
{
    int tuple_width = 0;
    BlockNumber total_pages = 0;
    int i;
    const int text_width = 12;

    if (0 >= totalTuples) {
        return 0;
    }

    /* just as get_rel_data_width */
    for (i = 1; i <= attrCnt; i++) {
        Form_pg_attribute att = &rel->rd_att->attrs[i - 1];
        int32 item_width = -1;

        if (att->attisdropped)
            continue;

        item_width = get_typlen(att->atttypid);
        if (item_width > 0) {
            /* for fixed-width datatype */
            tuple_width += item_width;
            continue;
        }
        /* text should be the tag, it should be small*/
        if (att->atttypid == TEXTOID) {
            tuple_width += text_width;
            continue;
        }

        /* estimate stats by average default value*/
        item_width = get_typavgwidth(att->atttypid, att->atttypmod);
        Assert(item_width > 0);
        
        tuple_width += item_width;
    }


    total_pages = ceil(totalTuples / RelDefaultFullCuSize) * (RelDefaultFullCuSize * tuple_width / BLCKSZ);

    if (0 < totalTuples && totalTuples < total_pages)
        total_pages = totalTuples;

    if (0 < totalTuples && 0 >= total_pages)
        total_pages = 1;

    return total_pages;
}
#endif   /* ENABLE_MULTIPLE_NODES */

/*
 * get_non_leaf_pages
 * Given a perfect tree's leaf pages, estimate the number of non leaf pages.
 */
static double get_non_leaf_pages(double leafPages, int totalWidth, bool hasToast)
{
    double nonLeafPages = 0.0;

    /* Find number of branches */
    double numOfBranches = hasToast ? (double)TOAST_TUPLES_PER_PAGE : \
        (double)(BLCKSZ - SizeOfPageHeaderData) * 100.0 / (BTREE_NONLEAF_FILLFACTOR * (double)totalWidth);

    /* If we have more branches than leaf pages, return the meta page only */
    if (numOfBranches <= 1) {
        return 1.0;
    }

    /* Find number of non leaf pages by repeatly dividing the branch number */
    while (leafPages >= 1) {
        leafPages /= numOfBranches;
        nonLeafPages += ceil(leafPages);
    }

    return nonLeafPages + 1.0; /* return non leaf pages AND the meta page */
}

static BlockNumber estimate_psort_index_blocks(TupleDesc desc, double totalTuples)
{
    int totalWidth = 0;
    int attrCnt = desc->natts;
    FormData_pg_attribute* attrs = desc->attrs;

    for (int attIdx = 0; attIdx < attrCnt; ++attIdx) {
        Form_pg_attribute thisatt = &attrs[attIdx];
        totalWidth += get_typavgwidth(thisatt->atttypid, thisatt->atttypmod);
    }

    return (ceil(totalTuples / RelDefaultFullCuSize) * (RelDefaultFullCuSize * totalWidth / BLCKSZ));
}

/*
 * estimate_btree_index_blocks
 * Estimate number of blocks occupied by index.
 * @in desc: tuple discriptor,
 * @in totalTuples: total tuples, 
 * @in table_factor: the table expansion factor,
 * @out totalPages: return number of blocks.
 */
static BlockNumber estimate_btree_index_blocks(TupleDesc desc, double totalTuples, double table_factor)
{
    int totalWidth = 0;
    int attrCnt = desc->natts;
    FormData_pg_attribute* attrs = desc->attrs;
    BlockNumber totalPages = 0;   /* returning block number */
    bool hasToast = false;

    for (int attIdx = 0; attIdx < attrCnt; ++attIdx) {
        Form_pg_attribute thisatt = &attrs[attIdx];
        totalWidth += get_typavgwidth(thisatt->atttypid, thisatt->atttypmod);
        AssertEreport(totalWidth > 0,
            MOD_OPT,
            "The estimated average width of values of the type is not larger than 0"
            "when setting the estimated output width of a base relation.");
    }

    /* Use colstore estimation without SQL_BETA_FEATURE GUC */
    if (!ENABLE_SQL_BETA_FEATURE(PAGE_EST_OPT)) {
        return ceil(totalTuples / RelDefaultFullCuSize) * (RelDefaultFullCuSize * totalWidth / BLCKSZ);
    }

    /* For toasted relations, we need to estimate the total pages differently */
    if (totalWidth >= (int)TOAST_TUPLE_TARGET) {
        hasToast = true;
    }

    /*
     * Assume we have only one big balanced ternary tree for index from all datanodes. We estimate its leaf pages by:
     * Find a perfectly filled tree and divide it by its fill factor (default, 90).
     * If the tree contains toast tuples, we use TOAST_TUPLES_PER_PAGE instead of totalWidth for estimation.
     */
    double leafPages = hasToast ? ceil(totalTuples / (double)TOAST_TUPLES_PER_PAGE) : \
        ceil(ceil((totalWidth * totalTuples) / BLCKSZ) * 100.0 / (double)BTREE_DEFAULT_FILLFACTOR);

    /* After we have the 'flawed' tree and its leaf pages, we can estimate its non leaf pages */
    double nonLeafPages = get_non_leaf_pages(leafPages, totalWidth, hasToast);

    /* Sum up, apply the non leaf fill factor */
    totalPages = ceil((leafPages + nonLeafPages * 100.0 / (double)BTREE_NONLEAF_FILLFACTOR) * table_factor);

    /* Print both original and optimized pages */
    ereport(DEBUG2,
            (errmodule(MOD_OPT),
                (errmsg("Estimating index blocks with sql_beta_feature = PAGE_EST_OPT(original: %u, optimized: %u).",
                    (unsigned int)(ceil(totalTuples / RelDefaultFullCuSize) * \
                        (RelDefaultFullCuSize * totalWidth / BLCKSZ)),
                    (unsigned int)totalPages))));

    return totalPages;
}

/*
 *	get_vacattrstats_by_vacstmt() -- set up statistic structure for all attributes or index by the statment for analyze
 * 								using for comput and update pg_statistic
 *
 * Input Param:
 * onerel: the relation for analyze or vacuum
 * vacstmt: the statment for analyze or vacuum command
 *
 * Output Param:
 * attr_cnt: the count of attributes for analyze
 * nindexes: the num of index for analyze
 * indexdata: per-index data for analyze
 * hasindex: are there have index or not for analyze
 * inh: Is inherit table for analzye or not
 * Irel: malloc a new index relation for analyze
 */
static VacAttrStats** get_vacattrstats_by_vacstmt(Relation onerel, VacuumStmt* vacstmt, int* attr_cnt, int* nindexes,
    AnlIndexData** indexdata, bool* hasindex, bool inh, Relation** Irel, bool isLog = true)
{
    int tcnt, i, ind;
    VacAttrStats** vacattrstats = NULL;
    List* list_multi_column = NIL;

    /*
     * Determine which columns to analyze
     *
     * Note that system attributes are never analyzed, so we just reject them
     * at the lookup stage.  We also reject duplicate column mentions.  (We
     * could alternatively ignore duplicates, but analyzing a column twice
     * won't work; we'd end up making a conflicting update in pg_statistic.)
     */
    if (vacstmt->va_cols != NIL) {
        Bitmapset* bms_single_column = NULL;

        /* search vacstmt->va_cols to get target single column or multi-column statistics */
        es_get_attnum_of_statistic_items(onerel, vacstmt, &bms_single_column, &list_multi_column);

        unsigned int num_of_statistic_items = bms_num_members(bms_single_column) + list_length(list_multi_column);
        elog(ES_LOGLEVEL, "num_of_statistic_items = [%u]", num_of_statistic_items);

        vacattrstats = (VacAttrStats**)palloc0(num_of_statistic_items * sizeof(VacAttrStats*));
        tcnt = 0;

        /* set up statistic structure for single column */
        for (int attnum = -1; (attnum = bms_next_member(bms_single_column, attnum)) > 0;) {
            vacattrstats[tcnt] = examine_attribute(onerel, attnum, NULL);
            if (vacattrstats[tcnt] != NULL) {
                ++tcnt;
            }
        }
    } else {
        /*
         * If there are no stats explicitly assigned,
         * we need to search system table to get pre-declared extended statistics
         */
        *attr_cnt = onerel->rd_att->natts;
        list_multi_column = es_explore_declared_stats(onerel->rd_id, vacstmt, inh);
        vacattrstats = (VacAttrStats**)palloc((*attr_cnt + list_length(list_multi_column)) * sizeof(VacAttrStats*));
        tcnt = 0;

        /* set up statistic structure for single column */
        for (i = 1; i <= *attr_cnt; i++) {
            vacattrstats[tcnt] = examine_attribute(onerel, i, NULL);
            if (vacattrstats[tcnt] != NULL)
                tcnt++;
        }
    }
    elog(ES_LOGLEVEL, "num of valid single column statistic = [%d]", tcnt);

    /*
     * set up statistic structure for multiple column
     * we only handle extended statistics in percent-sample mode
     */
    if (default_statistics_target < 0 && list_multi_column != NIL) {
        ListCell* lc = NULL;
        foreach (lc, list_multi_column) {
            Bitmapset* bms_multi_column = (Bitmapset*)lfirst(lc);

            if (bms_num_members(bms_multi_column) < 2) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION),
                        errmsg("Multi-column statistic needs at least two columns.")));
            }

            /* Construct VacAttrStats for multi-column stats */
            vacattrstats[tcnt] = examine_attribute(onerel, bms_multi_column, isLog);
            if (vacattrstats[tcnt] != NULL) {
                ++tcnt;
            }
        }
        elog(ES_LOGLEVEL, "num of extended statistic = [%d]", list_length(list_multi_column));
    }

    /* Total number of instences */
    *attr_cnt = tcnt;

    /*
     * Open all indexes of the relation, and see if there are any analyzable
     * columns in the indexes.	We do not analyze index columns if there was
     * an explicit column list in the ANALYZE command, however.  If we are
     * doing a recursive scan, we don't want to touch the parent's indexes at
     * all.
     */
    if (!inh && !NEED_EST_TOTAL_ROWS_DN(vacstmt)) {
        vac_open_indexes(onerel, AccessShareLock, nindexes, Irel);
    } else {
        *Irel = NULL;
        *nindexes = 0;
    }

    *hasindex = (*nindexes > 0);
    *indexdata = NULL;
    if (*hasindex && *Irel != NULL) {
        Relation* IdxRel = *Irel;
        AnlIndexData* tmpindexdata = NULL;

        tmpindexdata = *indexdata = (AnlIndexData*)palloc0(*nindexes * sizeof(AnlIndexData));
        for (ind = 0; ind < *nindexes; ind++) {
            AnlIndexData* thisdata = &tmpindexdata[ind];
            IndexInfo* indexInfo = NULL;

            thisdata->indexInfo = indexInfo = BuildIndexInfo(IdxRel[ind]);
            thisdata->tupleFract = 1.0;
            if (indexInfo->ii_Expressions != NIL && vacstmt->va_cols == NIL) {
                ListCell* indexpr_item = list_head(indexInfo->ii_Expressions);

                thisdata->vacattrstats = (VacAttrStats**)palloc(indexInfo->ii_NumIndexAttrs * sizeof(VacAttrStats*));
                tcnt = 0;
                for (i = 0; i < indexInfo->ii_NumIndexAttrs; i++) {
                    int keycol = indexInfo->ii_KeyAttrNumbers[i];

                    if (keycol == 0) {
                        /* Found an index expression */
                        Node* indexkey = NULL;

                        if (indexpr_item == NULL) /* shouldn't happen */
                            ereport(ERROR,
                                (errcode(ERRCODE_INDEX_CORRUPTED),
                                    errmodule(MOD_OPT),
                                    errmsg("too few entries in indexprs list")));

                        indexkey = (Node*)lfirst(indexpr_item);
                        indexpr_item = lnext(indexpr_item);
                        thisdata->vacattrstats[tcnt] = examine_attribute(IdxRel[ind], i + 1, indexkey);
                        if (thisdata->vacattrstats[tcnt] != NULL)
                            tcnt++;
                    }
                }
                thisdata->attr_cnt = tcnt;
            }
        }
    }

    return vacattrstats;
}

/*
 * Set up a working context before analyze
 */
static MemoryContext do_analyze_preprocess(Oid relowner, PGRUsage* ru0, TimestampTz* starttime, Oid* save_userid,
    int* save_sec_context, int* save_nestlevel, AnalyzeMode analyzemode)
{
    MemoryContext caller_context = NULL;

    /*
     * Set up a working context so that we can easily free whatever junk gets
     * created.
     */
    if (analyzemode == ANALYZENORMAL) {
        u_sess->analyze_cxt.analyze_context = AllocSetContextCreate(CurrentMemoryContext,
            "Analyze",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    }

    caller_context = MemoryContextSwitchTo(u_sess->analyze_cxt.analyze_context);

    /*
     * Switch to the table owner's userid, so that any index functions are run
     * as that user.  Also lock down security-restricted operations and
     * arrange to make GUC variable changes local to this command.
     */
    GetUserIdAndSecContext(save_userid, save_sec_context);
    SetUserIdAndSecContext(relowner, (unsigned int)*save_sec_context | SECURITY_RESTRICTED_OPERATION);
    *save_nestlevel = NewGUCNestLevel();

    /* measure elapsed time iff autovacuum logging requires it */
    if (IsAutoVacuumWorkerProcess() && u_sess->attr.attr_storage.Log_autovacuum_min_duration >= 0) {
        pg_rusage_init(ru0);
        if (u_sess->attr.attr_storage.Log_autovacuum_min_duration > 0)
            *starttime = GetCurrentTimestamp();
    }

    return caller_context;
}

/*
 * Delete working context after analyze
 */
static void do_analyze_finalize(
    MemoryContext caller_context, Oid save_userid, int save_sec_context, int save_nestlevel, AnalyzeMode analyzemode)
{
    /* Roll back any GUC changes executed by index functions */
    AtEOXact_GUC(false, save_nestlevel);

    /* Restore userid and security context */
    SetUserIdAndSecContext(save_userid, save_sec_context);

    /* Restore current context and release memory */
    (void)MemoryContextSwitchTo(caller_context);
    if (analyzemode == ANALYZENORMAL) {
        MemoryContextDelete(u_sess->analyze_cxt.analyze_context);
        u_sess->analyze_cxt.analyze_context = NULL;
    }

    return;
}

static inline void cleanup_indexes(int nindexes, Relation* Irel, const Relation onerel, int elevel)
{
    for (int ind = 0; ind < nindexes; ind++) {
        IndexBulkDeleteResult* stats = NULL;
        IndexVacuumInfo ivinfo;

        ivinfo.index = Irel[ind];
        ivinfo.analyze_only = true;
        ivinfo.estimated_count = true;
        ivinfo.message_level = elevel;
        ivinfo.invisibleParts = NULL;

        /*
         * if not handle the return value of index_vacuum_cleanup(),
         * ivinfo.num_heap_tuples might be useless.
         */
        ivinfo.num_heap_tuples = onerel->rd_rel->reltuples;

        ivinfo.strategy = u_sess->analyze_cxt.vac_strategy;

        if (RelationIsPartitioned(Irel[ind])) {
            Partition indexPartition = NULL;
            Relation indexPartitionRel = NULL;
            ListCell* cell = NULL;
            List* indexPartitionList = indexGetPartitionList(Irel[ind], AccessShareLock);

            /*
             * It is import to understand the nindexes indexes might be of different type,
             * btree, hash ,gin, gist etc.
             * here, we decide to seperate partitioned index with a list of partition,
             * transfer partition into dummy relation, replace index relation(ivinfo.index)
             * then call of index_vacuum_cleanup.
             * another way is to seperate, in the underground implementation function,
             * for example hash index or gin index implementation,
             * and those are ususally system inbuilt functions.
             * obviousll, the latter one will spend much more effort
             * as it must deal with every index ethod.
             */
            foreach (cell, indexPartitionList) {
                indexPartition = (Partition)lfirst(cell);
                indexPartitionRel = partitionGetRelation(Irel[ind], indexPartition);

                // key step: replace with coressponding dummy relation for partition
                ivinfo.index = indexPartitionRel;

                // finnaly, do the real work
                if (!RelationIsColStore(onerel))
                    stats = index_vacuum_cleanup(&ivinfo, NULL);
                if (stats != NULL)
                    pfree_ext(stats);
                releaseDummyRelation(&indexPartitionRel);
            }
            releasePartitionList(Irel[ind], &indexPartitionList, AccessShareLock);
        } else {
            // do the real work
            if (!RelationIsColStore(onerel))
                stats = index_vacuum_cleanup(&ivinfo, NULL);
            if (stats != NULL)
                pfree_ext(stats);
        }
    }
}

#ifndef ENABLE_MULTIPLE_NODES
static void update_numSamplerows(int attr_cnt, VacAttrStats** vacattrstats, HeapTuple* rows, int64 numrows)
{
    if (u_sess->attr.attr_sql.enable_functional_dependency) {
        for (int i = 0; i < attr_cnt; i++) {
            vacattrstats[i]->rows = rows;
            vacattrstats[i]->numSamplerows = numrows;
        }
    }
    return;
}
#endif

static void do_analyze_rel_start_log(bool inh, int elevel, Relation onerel)
{
    char* namespace_name =  get_namespace_name(RelationGetNamespace(onerel));
    if (inh)
        ereport(elevel,
            (errmodule(MOD_AUTOVAC),
            errmsg("analyzing \"%s.%s\" inheritance tree",
                namespace_name,
                RelationGetRelationName(onerel))));
    else
        ereport(elevel,
            (errmodule(MOD_AUTOVAC),
            errmsg("analyzing \"%s.%s\"",
                namespace_name,
                RelationGetRelationName(onerel))));
    pfree_ext(namespace_name);
}

static int64 do_analyze_calculate_sample_target_rows(int attr_cnt, VacAttrStats** vacattrstats, int nindexes,
                                                     AnlIndexData* indexdata)
{
    int64 targrows = 100;
    for (int i = 0; i < attr_cnt; i++) {
        if (targrows < vacattrstats[i]->minrows)
            targrows = vacattrstats[i]->minrows;
    }

    for (int ind = 0; ind < nindexes; ind++) {
        AnlIndexData* thisdata = &indexdata[ind];

        for (int i = 0; i < thisdata->attr_cnt; i++) {
            if (targrows < thisdata->vacattrstats[i]->minrows)
                targrows = thisdata->vacattrstats[i]->minrows;
        }
    }
    return targrows;
}

static void do_analyze_rel_end_log(TimestampTz starttime, Relation onerel, PGRUsage* ru0)
{
    /* Log the action if appropriate */
    if (IsAutoVacuumWorkerProcess() && u_sess->attr.attr_storage.Log_autovacuum_min_duration >= 0) {
        if (u_sess->attr.attr_storage.Log_autovacuum_min_duration == 0 ||
            TimestampDifferenceExceeds(
                starttime, GetCurrentTimestamp(), u_sess->attr.attr_storage.Log_autovacuum_min_duration)) {
            char* dbname = get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId);
            char* namespace_name = get_namespace_name(RelationGetNamespace(onerel));
            ereport(LOG,
                (errmsg("automatic analyze of table \"%s.%s.%s\" system usage: %s",
                    dbname,
                    namespace_name,
                    RelationGetRelationName(onerel),
                    pg_rusage_show(ru0))));
            pfree_ext(dbname);
            pfree_ext(namespace_name);
        }
    }
}

/*
 *	do_analyze_rel() -- analyze one relation, recursively or not
 *
 * Note that "acquirefunc" is only relevant for the non-inherited case.
 * If we supported foreign tables in inheritance trees,
 * acquire_inherited_sample_rows would need to determine the appropriate
 * acquirefunc for each child table.
 */
static void do_analyze_rel(Relation onerel, VacuumStmt* vacstmt, BlockNumber relpages, bool inh, int elevel,
    AnalyzeMode analyzemode)
{
    int attr_cnt = 0;
    int i = 0;
    Relation* Irel = NULL;
    int nindexes = 0;
    bool hasindex = false;
    VacAttrStats** vacattrstats = NULL;
    AnlIndexData* indexdata = NULL;
    int64 numrows = 0;
    int64 targrows = 0;
    double totalrows = 0;
    double totaldeadrows = 0;
    HeapTuple* rows = NULL;
    PGRUsage ru0;
    TimestampTz starttime = 0;
    MemoryContext caller_context = NULL;
    Oid save_userid = 0;
    int save_sec_context = 0;
    int save_nestlevel = 0;
    int tableidx = (analyzemode == ANALYZENORMAL) ? analyzemode : (analyzemode - 1);
    vacstmt->tableidx = tableidx;

    /*
     * (1) For replicate table
     * We do not support percent-sample mode for replicate table,
     * and we need to use this mode in extended statistics,
     * so, we let datanode use percent-sample mode when extended statistic is detected,
     * and then, coordinate node could fetch statistic from datanode.
     *
     * (2) check availability for extended statistics
     */
    bool replicate_needs_extstats = false;
    es_check_availability_for_table(vacstmt, onerel, inh, &replicate_needs_extstats);

    do_analyze_rel_start_log(inh, elevel, onerel);

    caller_context = do_analyze_preprocess(onerel->rd_rel->relowner,
        &ru0,
        &starttime,
        &save_userid,
        &save_sec_context,
        &save_nestlevel,
        analyzemode);

    /* Ready and construct for all attributes info in order to compute statistic. */
    vacattrstats =
        get_vacattrstats_by_vacstmt(onerel, vacstmt, &attr_cnt, &nindexes, &indexdata, &hasindex, inh, &Irel);

    /*
     * Stop analyze progress if there are no VacAttrStats instance initialized.
     * This happened on collect extended statistic using 'analyze t ((a, b))'
     *     when set default_statistics_target to a positive number.
     */
    if (attr_cnt <= 0) {
        vac_close_indexes(nindexes, Irel, NoLock);
        do_analyze_finalize(caller_context, save_userid, save_sec_context, save_nestlevel, analyzemode);
        if (analyzemode <= ANALYZENORMAL) {
            if (default_statistics_target >= 0)
                elog(INFO, "Please set default_statistics_target to a negative value to collect extended statistics.");
            else
                elog(INFO, "No columns in %s can be used to collect statistics.", NameStr(onerel->rd_rel->relname));
        }
        return;
    }

    /*
     * Determine how many rows we need to sample, using the worst case from
     * all analyzable columns.	We use a lower bound of 100 rows to avoid
     * possible overflow in Vitter's algorithm.  (Note: that will also be the
     * target in the corner case where there are no analyzable columns.)
     */
    targrows = do_analyze_calculate_sample_target_rows(attr_cnt, vacattrstats, nindexes, indexdata);

    /*
     * If get sample rows on datanode or coordinator or not. there are two cases:
     * 1. sampleRate > 0 for hash or roundrobin table on datanode;
     * 2. sampleRate == 0 for replication table on datanode;
     * 3. for system table on all nodes.
     */
    if (onerel->rd_id < FirstNormalObjectId || NEED_GET_SAMPLE_ROWS_DN(vacstmt)) {
        /*
         * CN has finish caculate sampleRate and DN get total rows and sample.
         * if sampleRate more or equal 0, it means CN should get sample rows from DN.
         */
        if (NEED_GET_SAMPLE_ROWS_DN(vacstmt)) {
            bool use_percent = whether_use_percent(vacattrstats, attr_cnt, nindexes, indexdata);

            if (use_percent) {
                rows = get_total_rows<true>(onerel,
                    vacstmt,
                    relpages,
                    inh,
                    elevel,
                    vacattrstats,
                    attr_cnt,
                    targrows,
                    &totalrows,
                    &totaldeadrows,
                    &numrows);
                vacstmt->pstGlobalStatEx[vacstmt->tableidx].totalRowCnts = totalrows;
            }

            targrows =
                get_target_rows(onerel, vacattrstats, attr_cnt, nindexes, indexdata, totalrows, targrows, use_percent);
        }

        /*
         * Get target rows only for system table or
         * sample rate more than 1 on all datanodes.
         */
        rows = get_total_rows<false>(onerel,
            vacstmt,
            relpages,
            inh,
            elevel,
            vacattrstats,
            attr_cnt,
            targrows,
            &totalrows,
            &totaldeadrows,
            &numrows);
    }

    /*
     *  If sampleRate is -1, it means DN send estimate total row count to CN.
     */
    if (NEED_EST_TOTAL_ROWS_DN(vacstmt)) {
        /* get estimate total row count. */
        rows = get_total_rows<true>(onerel,
            vacstmt,
            relpages,
            inh,
            elevel,
            vacattrstats,
            attr_cnt,
            targrows,
            &totalrows,
            &totaldeadrows,
            &numrows);
        vacstmt->pstGlobalStatEx[vacstmt->tableidx].totalRowCnts = totalrows;
        /* compute memory size (KB) for dn to send to cn */
        vacstmt->pstGlobalStatEx[vacstmt->tableidx].topMemSize =
            compute_com_size(vacstmt, onerel, vacstmt->tableidx) * GetOneTupleSize(vacstmt, onerel) / 1024;
        if (analyzemode == ANALYZENORMAL) {
            /* send estimate total row count to cn. */
            send_totalrowcnt_to_cn(vacstmt, analyzemode, totalrows);
        }

        if (!inh) {
            vac_close_indexes(nindexes, Irel, NoLock);
        }

        do_analyze_finalize(caller_context, save_userid, save_sec_context, save_nestlevel, analyzemode);

        return;
    }

    /*
     * We need do analyze to compute statistic for sample rows only on datanode or
     * required sample rows on coordinator.
     *
     * collect extended statistic for replicate table will use 'sampletable' method in data-node
     */
    if (NEED_ANALYZE_SAMPLEROWS(vacstmt) && (!replicate_needs_extstats)) {
        /*
         * Compute the statistics.	Temporary results during the calculations for
         * each column are stored in a child context.  The calc routines are
         * responsible to make sure that whatever they store into the VacAttrStats
         * structure is allocated in u_sess->analyze_cxt.analyze_context.
         */
        bool ret = do_analyze_samplerows(onerel,
            vacstmt,
            attr_cnt,
            vacattrstats,
            hasindex,
            nindexes,
            indexdata,
            inh,
            Irel,
            &totalrows,
            &numrows,
            rows,
            analyzemode,
            caller_context,
            save_userid,
            save_sec_context,
            save_nestlevel);
        /*
         * If the relation's attribute have modified in local under analyzing,
         * we shoule return, because caller_context has finalize.
         */
        if (!ret) {
            return;
        }
    } else if (NEED_ANALYZE_SAMPLETABLE(vacstmt) || replicate_needs_extstats) {
        if (vacstmt->pstGlobalStatEx[vacstmt->tableidx].totalRowCnts > 0) {
            /*
             * There is a concurrency condition:
             * coordinator received estimate totalRowCnts from all datanodes then estimate sample rate
             * for the first step. And there is insert or delete rows concurrency before coordinator
             * received real totalRowCnts from all datanodes.
             * The sampleRate is not match with the final totalRowCnts, it will result to compute
             * error samplerows and error statistics. So we should compute right sampleRate again.
             */
#ifndef ENABLE_MULTIPLE_NODES
            /* Only available in single node mode */
            update_numSamplerows(attr_cnt, vacattrstats, rows, numrows);
#endif
            (void)compute_sample_size(vacstmt, 0, NULL, onerel->rd_id, vacstmt->tableidx);
            numrows = ceil(vacstmt->pstGlobalStatEx[vacstmt->tableidx].totalRowCnts *
                           vacstmt->pstGlobalStatEx[vacstmt->tableidx].sampleRate);
            totalrows = vacstmt->pstGlobalStatEx[vacstmt->tableidx].totalRowCnts;

            /*
             * We need do analyze to compute statistic for sample table only
             * required sample table on coordinator.
             *
             * We only collect extended statistics for replicate table when it is not empty
             */
            if (vacstmt->pstGlobalStatEx[vacstmt->tableidx].exec_query ||
                (replicate_needs_extstats && numrows > 0 && totalrows > 0)) {
                vacstmt->pstGlobalStatEx[vacstmt->tableidx].exec_query = true;
                do_analyze_sampletable(onerel,
                    vacstmt,
                    attr_cnt,
                    vacattrstats,
                    hasindex,
                    nindexes,
                    indexdata,
                    inh,
                    Irel,
                    totalrows,
                    numrows,
                    analyzemode);
            }
        } else {
            /*
             * Supported replicate table uses query to collect statistics,
             * set 'exec_query' to mark it's analyze mode even it is empty
             */
            if (replicate_needs_extstats) {
                vacstmt->pstGlobalStatEx[vacstmt->tableidx].exec_query = true;
            }

            /* We still insert a record to pg_statistic for extended stats even the table is empty */
            for (i = 0; i < attr_cnt; ++i) {
                VacAttrStats* stats = vacattrstats[i];
                if (stats->num_attrs > 1) {
                    stats->stats_valid = true;
                    update_attstats(RelationGetRelid(onerel), STARELKIND_CLASS, inh, 1, &stats,
                                    RelationGetRelPersistence(onerel));
                }
            }
        }
    }

    if (!inh) {
        /* Update the pg_class for relation and index */
        update_pages_and_tuples_pgclass(onerel,
            vacstmt,
            attr_cnt,
            vacattrstats,
            hasindex,
            nindexes,
            indexdata,
            Irel,
            relpages,
            totalrows,
            totaldeadrows,
            numrows,
            inh);

        /*
         * Report ANALYZE to the stats collector, too.	However, if doing
         * inherited stats we shouldn't report, because the stats collector only
         * tracks per-table stats.
         */
        pgstat_report_analyze(onerel, totalrows, totaldeadrows);
    }

    /* If this isn't part of VACUUM ANALYZE, let index AMs do cleanup */
    if (!(vacstmt->options & VACOPT_VACUUM)) {
        cleanup_indexes(nindexes, Irel, onerel, elevel);
    }

    /* Done with indexes */
    vac_close_indexes(nindexes, Irel, NoLock);

    do_analyze_rel_end_log(starttime, onerel, &ru0);

    do_analyze_finalize(caller_context, save_userid, save_sec_context, save_nestlevel, analyzemode);

    return;
}

/*
 * Compute statistics about indexes of a relation
 */
static void compute_index_stats(Relation onerel, double totalrows, AnlIndexData* indexdata, int nindexes,
    HeapTuple* rows, int numrows, MemoryContext col_context)
{
    MemoryContext ind_context, old_context;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    int ind, i;

    ind_context = AllocSetContextCreate(u_sess->analyze_cxt.analyze_context,
        "Analyze Index",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    old_context = MemoryContextSwitchTo(ind_context);

    for (ind = 0; ind < nindexes; ind++) {
        AnlIndexData* thisdata = &indexdata[ind];
        IndexInfo* indexInfo = thisdata->indexInfo;
        int attr_cnt = thisdata->attr_cnt;
        TupleTableSlot* slot = NULL;
        EState* estate = NULL;
        ExprContext* econtext = NULL;
        List* predicate = NIL;
        Datum* exprvals = NULL;
        bool* exprnulls = NULL;
        int numindexrows, tcnt, rowno;
        double totalindexrows;

        /* Ignore index if no columns to analyze and not partial */
        if (attr_cnt == 0 && indexInfo->ii_Predicate == NIL)
            continue;

        /*
         * Need an EState for evaluation of index expressions and
         * partial-index predicates.  Create it in the per-index context to be
         * sure it gets cleaned up at the bottom of the loop.
         */
        estate = CreateExecutorState();
        econtext = GetPerTupleExprContext(estate);
        /* Need a slot to hold the current heap tuple, too */
        slot = MakeSingleTupleTableSlot(RelationGetDescr(onerel));

        /* Arrange for econtext's scan tuple to be the tuple under test */
        econtext->ecxt_scantuple = slot;

        /* Set up execution state for predicate. */
        if (estate->es_is_flt_frame){
            predicate = (List*)ExecPrepareQualByFlatten(indexInfo->ii_Predicate, estate);
        } else {
            predicate = (List*)ExecPrepareExpr((Expr*)indexInfo->ii_Predicate, estate);
        }

        /* Compute and save index expression values */
        exprvals = (Datum*)palloc(numrows * attr_cnt * sizeof(Datum));
        exprnulls = (bool*)palloc(numrows * attr_cnt * sizeof(bool));
        numindexrows = 0;
        tcnt = 0;
        for (rowno = 0; rowno < numrows; rowno++) {
            HeapTuple heapTuple = rows[rowno];

            /*
             * Reset the per-tuple context each time, to reclaim any cruft
             * left behind by evaluating the predicate or index expressions.
             */
            ResetExprContext(econtext);

            /* Set up for predicate or expression evaluation */
            (void)ExecStoreTuple(heapTuple, slot, InvalidBuffer, false);

            /* If index is partial, check predicate */
            if (predicate != NIL) {
                if (!ExecQual(predicate, econtext))
                    continue;
            }
            numindexrows++;

            if (attr_cnt > 0) {
                /*
                 * Evaluate the index row to compute expression values. We
                 * could do this by hand, but FormIndexDatum is convenient.
                 */
                FormIndexDatum(indexInfo, slot, estate, values, isnull);

                /*
                 * Save just the columns we care about.  We copy the values
                 * into ind_context from the estate's per-tuple context.
                 */
                for (i = 0; i < attr_cnt; i++) {
                    VacAttrStats* stats = thisdata->vacattrstats[i];
                    int attnum = stats->attrs[0]->attnum;

                    if (isnull[attnum - 1]) {
                        exprvals[tcnt] = (Datum)0;
                        exprnulls[tcnt] = true;
                    } else {
                        exprvals[tcnt] =
                            datumCopy(values[attnum - 1], stats->attrtype[0]->typbyval, stats->attrtype[0]->typlen);
                        exprnulls[tcnt] = false;
                    }
                    tcnt++;
                }
            }
        }

        /*
         * Having counted the number of rows that pass the predicate in the
         * sample, we can estimate the total number of rows in the index.
         */
        thisdata->tupleFract = (double)numindexrows / (double)numrows;
        totalindexrows = ceil(thisdata->tupleFract * totalrows);

        /*
         * Now we can compute the statistics for the expression columns.
         */
        if (numindexrows > 0) {
            (void)MemoryContextSwitchTo(col_context);
            for (i = 0; i < attr_cnt; i++) {
                VacAttrStats* stats = thisdata->vacattrstats[i];
                AttributeOpts* aopt = get_attribute_options(stats->attrs[0]->attrelid, stats->attrs[0]->attnum);

                stats->exprvals = exprvals + i;
                stats->exprnulls = exprnulls + i;
                stats->rowstride = attr_cnt;
                (*stats->compute_stats)(stats, ind_fetch_func, numindexrows, totalindexrows, onerel);

                /*
                 * If the n_distinct option is specified, it overrides the
                 * above computation.  For indices, we always use just
                 * n_distinct, not n_distinct_inherited.
                 */
                if (aopt != NULL && fabs(aopt->n_distinct) > EPSILON)
                    stats->stadistinct = aopt->n_distinct;

                MemoryContextResetAndDeleteChildren(col_context);
            }
        }

        /* And clean up */
        (void)MemoryContextSwitchTo(ind_context);

        ExecDropSingleTupleTableSlot(slot);
        FreeExecutorState(estate);
        MemoryContextResetAndDeleteChildren(ind_context);
    }

    (void)MemoryContextSwitchTo(old_context);
    MemoryContextDelete(ind_context);
}

/*
 * examine_attribute
 *     construct VacAttrStats for multi-column stats
 */
VacAttrStats* examine_attribute(Relation onerel, Bitmapset* bms_attnums, bool isLog)
{
    /* only analyze multi-column when each single column is valid */
    for (int attnum = -1; (attnum = bms_next_member(bms_attnums, attnum)) > 0;) {
        Form_pg_attribute attr = &onerel->rd_att->attrs[attnum - 1];
        if (!es_is_valid_column_to_analyze(attr)) {
            return NULL;
        }

        /* Don't analyze column with unanalyzable type */
        if (!GetDefaultOpClass(attr->atttypid, BTREE_AM_OID) && !GetDefaultOpClass(attr->atttypid, HASH_AM_OID)) {
            if (isLog) {
                ereport(WARNING,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Column %s with type %s is unanalyzable.",
                            attr->attname.data,
                            format_type_be(attr->atttypid))));
            }
            return NULL;
        }
    }

    /* Make a VacAttrStats */
    unsigned int num_attrs = bms_num_members(bms_attnums);
    VacAttrStats* stats = es_make_vacattrstats(num_attrs);

    /* Set stats->attrs */
    int index = 0;
    for (int attnum = -1; (attnum = bms_next_member(bms_attnums, attnum)) > 0;) {
        Form_pg_attribute attr = &onerel->rd_att->attrs[attnum - 1];

        stats->attrs[index] = (Form_pg_attribute)palloc0(ATTRIBUTE_FIXED_PART_SIZE);

        errno_t reterrno = memcpy_s(stats->attrs[index], ATTRIBUTE_FIXED_PART_SIZE, attr, ATTRIBUTE_FIXED_PART_SIZE);
        securec_check(reterrno, "\0", "\0");

        stats->attrtypid[index] = attr->atttypid;
        stats->attrtypmod[index] = attr->atttypmod;
        stats->attrtype[index] = es_get_attrtype(attr->atttypid);

        ++index;
    }

    stats->anl_context = u_sess->analyze_cxt.analyze_context;

    /* we set 'tupattnum' to the first attribute in initial */
    stats->tupattnum = stats->attrs[0]->attnum;

    return stats;
}

/*
 * examine_attribute -- pre-analysis of a single column
 *
 * Determine whether the column is analyzable; if so, create and initialize
 * a VacAttrStats struct for it.  If not, return NULL.
 *
 * If index_expr isn't NULL, then we're trying to analyze an expression index,
 * and index_expr is the expression tree representing the column's data.
 */
static VacAttrStats* examine_attribute(Relation onerel, int attnum, Node* index_expr)
{
    HeapTuple typtuple;
    int i;
    bool ok = false;

    Form_pg_attribute attr = &onerel->rd_att->attrs[attnum - 1];

    /* Check wheather the column is valid to analyze */
    if (!es_is_valid_column_to_analyze(attr)) {
        return NULL;
    }

    /*
     * Create the VacAttrStats struct.	Note that we only have a copy of the
     * fixed fields of the pg_attribute tuple.
     */
    VacAttrStats* stats = es_make_vacattrstats(1);

    /* Set stats->attrs */
    stats->attrs[0] = (Form_pg_attribute)palloc0(ATTRIBUTE_FIXED_PART_SIZE);

    errno_t reterrno = memcpy_s(stats->attrs[0], ATTRIBUTE_FIXED_PART_SIZE, attr, ATTRIBUTE_FIXED_PART_SIZE);
    securec_check(reterrno, "\0", "\0");

    /*
     * When analyzing an expression index, believe the expression tree's type
     * not the column datatype --- the latter might be the opckeytype storage
     * type of the opclass, which is not interesting for our purposes.	(Note:
     * if we did anything with non-expression index columns, we'd need to
     * figure out where to get the correct type info from, but for now that's
     * not a problem.)	It's not clear whether anyone will care about the
     * typmod, but we store that too just in case.
     */
    if (index_expr != NULL) {
        stats->attrtypid[0] = exprType(index_expr);
        stats->attrtypmod[0] = exprTypmod(index_expr);
    } else {
        stats->attrtypid[0] = attr->atttypid;
        stats->attrtypmod[0] = attr->atttypmod;
    }

    /* Set attr type */
    typtuple = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(stats->attrtypid[0]));
    if (!HeapTupleIsValid(typtuple))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", stats->attrtypid[0])));
    stats->attrtype[0] = (Form_pg_type)GETSTRUCT(typtuple);

    /* Set memory context and attnum */
    stats->anl_context = u_sess->analyze_cxt.analyze_context;
    stats->tupattnum = attnum;

    /*
     * The fields describing the stats->stavalues[n] element types default to
     * the type of the data being analyzed, but the type-specific typanalyze
     * function can change them if it wants to store something else.
     */
    for (i = 0; i < STATISTIC_NUM_SLOTS; i++) {
        stats->statypid[i] = stats->attrtypid[0];
        stats->statyplen[i] = stats->attrtype[0]->typlen;
        stats->statypbyval[i] = stats->attrtype[0]->typbyval;
        stats->statypalign[i] = stats->attrtype[0]->typalign;
    }

    /*
     * Call the type-specific typanalyze function.	If none is specified, use
     * std_typanalyze().
     */
    if (OidIsValid(stats->attrtype[0]->typanalyze)) {
        ok = DatumGetBool(OidFunctionCall1(stats->attrtype[0]->typanalyze, PointerGetDatum(stats)));
        /*
         * we will modify minrows for type-specific typanalyze function as traditional
         * analyze mode, since we fallback to analyze this column with traditional analyze
         * just see comment in function do_analyze_sampletable
         *
         * 1. OK = 'true' means we can do specific typanalyze
         * 2. default_statistics_target < 0 means just for analyze with samptable mode
         * 3. stats->minrows < 0 means rows wanted for stats
         */
        if (ok && default_statistics_target < 0 && stats->minrows < 0)
            stats->minrows = DEFAULT_EST_TARGET_ROWS * compute_attr_target(stats->attrs[0]);
    } else
        ok = std_typanalyze(stats);

    if (!ok || stats->compute_stats == NULL || stats->minrows <= 0) {
        tableam_tops_free_tuple(typtuple);
        pfree_ext(stats->attrs[0]);
        pfree_ext(stats->attrs);
        pfree_ext(stats);
        return NULL;
    }

    return stats;
}

/*
 * BlockSampler_Init -- prepare for random sampling of blocknumbers
 *
 * BlockSampler is used for stage one of our new two-stage tuple
 * sampling mechanism as discussed on pgsql-hackers 2004-04-02 (subject
 * "Large DB").  It selects a random sample of samplesize blocks out of
 * the nblocks blocks in the table.  If the table has less than
 * samplesize blocks, all blocks are selected.
 *
 * Since we know the total number of blocks in advance, we can use the
 * straightforward Algorithm S from Knuth 3.4.2, rather than Vitter's
 * algorithm.
 */
void BlockSampler_Init(BlockSampler bs, BlockNumber nblocks, int samplesize)
{
    bs->N = nblocks; /* measured table size */

    /*
     * If we decide to reduce samplesize for tables that have less or not much
     * more than samplesize blocks, here is the place to do it.
     */
    bs->n = samplesize;
    bs->t = 0; /* blocks scanned so far */
    bs->m = 0; /* blocks selected so far */
}

bool BlockSampler_HasMore(BlockSampler bs)
{
    return (bs->t < bs->N) && (bs->m < bs->n);
}

BlockNumber BlockSampler_Next(BlockSampler bs)
{
    BlockNumber K = bs->N - bs->t; /* remaining blocks */
    int k = bs->n - bs->m;         /* blocks still to sample */
    double p;                      /* probability to skip block */
    double V;                      /* random */

    AssertEreport(BlockSampler_HasMore(bs), MOD_OPT, ""); /* hence K > 0 and k > 0 */

    if ((BlockNumber)k >= K) {
        /* need all the rest */
        bs->m++;
        return bs->t++;
    }

    /* ----------
     * It is not obvious that this code matches Knuth's Algorithm S.
     * Knuth says to skip the current block with probability 1 - k/K.
     * If we are to skip, we should advance t (hence decrease K), and
     * repeat the same probabilistic test for the next block.  The naive
     * implementation thus requires an anl_random_fract() call for each block
     * number.	But we can reduce this to one anl_random_fract() call per
     * selected block, by noting that each time the while-test succeeds,
     * we can reinterpret V as a uniform random number in the range 0 to p.
     * Therefore, instead of choosing a new V, we just adjust p to be
     * the appropriate fraction of its former value, and our next loop
     * makes the appropriate probabilistic test.
     *
     * We have initially K > k > 0.  If the loop reduces K to equal k,
     * the next while-test must fail since p will become exactly zero
     * (we assume there will not be roundoff error in the division).
     * (Note: Knuth suggests a "<=" loop condition, but we use "<" just
     * to be doubly sure about roundoff error.)  Therefore K cannot become
     * less than k, which means that we cannot fail to select enough blocks.
     * ----------
     */
    V = anl_random_fract();
    p = 1.0 - (double)k / (double)K;
    while (V < p) {
        /* skip */
        bs->t++;
        K--; /* keep K == N - t */

        /* adjust p to be new cutoff point in reduced range */
        p *= 1.0 - (double)k / (double)K;
    }

    /* select */
    bs->m++;
    return bs->t++;
}

/*
 * get list  block number for acquire sample rows when uses ADIO
 */
uint32 BlockSampler_NextList(BlockSampler bs, AnlPrefetchList* list)
{
    uint32 idx = 0;

    while (BlockSampler_HasMore(bs)) {
        if (idx >= list->size) {
            break;
        }
        list->blocklist[idx] = BlockSampler_Next(bs);
        idx++;
    }
    return idx;
}

template <bool isColumnStore>
BlockNumber BlockSampler_NextBlock(
    void* rel_or_cuDesc, BlockSampler bs, AnlPrefetch* anlprefetch, int analyzeAttrNum = 0, int32* attrSeq = NULL)
{
    uint32 blocks = 0;
    BlockNumber blo = 0;
    Relation onerel = (Relation)rel_or_cuDesc;
    CStoreScanDesc cstoreScan = (CStoreScanDesc)rel_or_cuDesc;

    if (!anlprefetch->init) {
        /* prefetch */
        blocks = BlockSampler_NextList(bs, &anlprefetch->fetchlist1);
        if (blocks != 0) {
            if (isColumnStore == false) {
                PageListPrefetch(onerel, MAIN_FORKNUM, anlprefetch->fetchlist1.blocklist, blocks, 0, 0);
                ereport(DEBUG1,
                    (errmodule(MOD_ADIO),
                        errmsg("analyze prefetch for %s, start(%u), count(%u)",
                            RelationGetRelationName(onerel),
                            anlprefetch->fetchlist1.blocklist[0],
                            blocks)));
            } else {
                CstoreAnalyzePrefetch(cstoreScan, anlprefetch->fetchlist1.blocklist, blocks, analyzeAttrNum, attrSeq);
                ereport(DEBUG1,
                    (errmodule(MOD_ADIO),
                        errmsg("analyze prefetch for %s, start(%u), count(%u)",
                            RelationGetRelationName(cstoreScan->m_CStore->m_relation),
                            anlprefetch->fetchlist1.blocklist[0],
                            blocks)));
            }
        }
        anlprefetch->fetchlist1.anl_idx = 0;
        anlprefetch->fetchlist1.load_count = blocks;

        /* prefetch */
        blocks = BlockSampler_NextList(bs, &anlprefetch->fetchlist2);
        if (blocks != 0) {
            if (isColumnStore == false) {
                PageListPrefetch(onerel, MAIN_FORKNUM, anlprefetch->fetchlist2.blocklist, blocks, 0, 0);
                ereport(DEBUG1,
                    (errmodule(MOD_ADIO),
                        errmsg("analyze prefetch for %s, start(%u), count(%u)",
                            RelationGetRelationName(onerel),
                            anlprefetch->fetchlist2.blocklist[0],
                            blocks)));
            } else {
                CstoreAnalyzePrefetch(cstoreScan, anlprefetch->fetchlist2.blocklist, blocks, analyzeAttrNum, attrSeq);
                ereport(DEBUG1,
                    (errmodule(MOD_ADIO),
                        errmsg("analyze prefetch for %s, start(%u), count(%u)",
                            RelationGetRelationName(cstoreScan->m_CStore->m_relation),
                            anlprefetch->fetchlist2.blocklist[0],
                            blocks)));
            }
        }
        anlprefetch->fetchlist2.anl_idx = 0;
        anlprefetch->fetchlist2.load_count = blocks;
        anlprefetch->blocklist = &anlprefetch->fetchlist1;
        anlprefetch->init = true;
    }

    if (anlprefetch->blocklist == NULL) {
        ereport(ERROR, (errmodule(MOD_ADIO), errmsg("analyze prefetch block list can not be NULL")));
    }

    if (anlprefetch->blocklist->load_count == 0) {
        return InvalidBlockNumber;
    }

    blo = anlprefetch->blocklist->blocklist[anlprefetch->blocklist->anl_idx];
    anlprefetch->blocklist->anl_idx++;
    /* exchange list and trigger prefetch */
    if (anlprefetch->blocklist->anl_idx >= anlprefetch->blocklist->load_count) {
        blocks = BlockSampler_NextList(bs, anlprefetch->blocklist);
        if (blocks != 0) {
            if (isColumnStore == false) {
                PageListPrefetch(onerel, MAIN_FORKNUM, anlprefetch->blocklist->blocklist, blocks, 0, 0);
                ereport(DEBUG1,
                    (errmodule(MOD_ADIO),
                        errmsg("analyze prefetch for %s, start(%u), count(%u)",
                            RelationGetRelationName(onerel),
                            anlprefetch->blocklist->blocklist[0],
                            blocks)));
            } else {
                CstoreAnalyzePrefetch(cstoreScan, anlprefetch->blocklist->blocklist, blocks, analyzeAttrNum, attrSeq);
                ereport(DEBUG1,
                    (errmodule(MOD_ADIO),
                        errmsg("analyze prefetch for %s, start(%u), count(%u)",
                            RelationGetRelationName(cstoreScan->m_CStore->m_relation),
                            anlprefetch->blocklist->blocklist[0],
                            blocks)));
            }
        }
        anlprefetch->blocklist->anl_idx = 0;
        anlprefetch->blocklist->load_count = blocks;
        anlprefetch->blocklist =
            (anlprefetch->blocklist == &anlprefetch->fetchlist1) ? &anlprefetch->fetchlist2 : &anlprefetch->fetchlist1;
    }

    return blo;
}

/*
 * @Description: get one block for analyze
 * @Param[IN/OUT] anlprefetch: anlprefetch context if used adio
 * @Param[IN/OUT] bs: BlockSampler
 * @Param[IN/OUT] estimate_table_rownum: estimate table row num
 * @Param[IN/OUT] onerel: relation
 * @Return: block id, InvalidBlockNumber means no block for analyze
 * @See also:
 */
template <bool isColumnStore>
BlockNumber BlockSampler_GetBlock(void* rel_or_cuDesc, BlockSampler bs, AnlPrefetch* anlprefetch, int analyzeAttrNum,
    int32* attrSeq, bool estimate_table_rownum)
{
    if (estimate_table_rownum) {
        if (BlockSampler_HasMore(bs)) {
            return BlockSampler_Next(bs);
        }
    }

    ADIO_RUN()
    {
        return BlockSampler_NextBlock<isColumnStore>(rel_or_cuDesc, bs, anlprefetch, analyzeAttrNum, attrSeq);
    }
    ADIO_ELSE()
    {
        if (BlockSampler_HasMore(bs)) {
            return BlockSampler_Next(bs);
        }
    }
    ADIO_END();
    return InvalidBlockNumber;
}

/*
 * acquire_sample_rows -- acquire a random sample of rows from the table
 *
 * Selected rows are returned in the caller-allocated array rows[], which
 * must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also estimate the total numbers of live and dead rows in the table,
 * and return them into *totalrows and *totaldeadrows, respectively.
 *
 * The returned list of tuples is in order by physical position in the table.
 * (We will rely on this later to derive correlation estimates.)
 *
 * As of May 2004 we use a new two-stage method:  Stage one selects up
 * to targrows random blocks (or all blocks, if there aren't so many).
 * Stage two scans these blocks and uses the Vitter algorithm to create
 * a random sample of targrows rows (or less, if there are less in the
 * sample of blocks).  The two stages are executed simultaneously: each
 * block is processed as soon as stage one returns its number and while
 * the rows are read stage two controls which ones are to be inserted
 * into the sample.
 *
 * Although every row has an equal chance of ending up in the final
 * sample, this sampling method is not perfect: not every possible
 * sample has an equal chance of being selected.  For large relations
 * the number of different blocks represented by the sample tends to be
 * too small.  We can live with that for now.  Improvements are welcome.
 *
 * An important property of this sampling method is that because we do
 * look at a statistically unbiased set of blocks, we should get
 * unbiased estimates of the average numbers of live and dead rows per
 * block.  The previous sampling method put too much credence in the row
 * density near the start of the table.
 */
#define MAX_ESTIMATE_SAMPLE_PAGES 5
#define MAX_ESTIMATE_PART_SAMPLE_PAGES 3

#define MAX_ESTIMATE_RETRY_TIMES 3

template <bool estimate_table_rownum>
static int64 acquire_sample_rows(
    Relation onerel, int elevel, HeapTuple* rows, int64 targrows, double* totalrows, double* totaldeadrows)
{
    int64 numrows = 0;      /* # rows now in reservoir */
    double samplerows = 0;  /* total # rows collected */
    double liverows = 0;    /* # live rows seen */
    double deadrows = 0;    /* # dead rows seen */
    double rowstoskip = -1; /* -1 means not set yet */
    BlockNumber totalblocks;
    TransactionId OldestXmin;
    BlockSamplerData bs;
    double rstate;
    BlockNumber targblock = 0;
    BlockNumber sampleblock = 0;
    BlockNumber retrycount = 1;
    AnlPrefetch anlprefetch;
    int64 ori_targrows = targrows;
    anlprefetch.blocklist = NULL;
    bool isAnalyzing = true;

    AssertEreport(targrows > 0, MOD_OPT, "Target row number must be greater than 0 when sampling.");

    totalblocks = RelationGetNumberOfBlocks(onerel);

    /*
     * in pretty analyze mode, we estimate total rows from MAX_ESTIMATE_SAMPLE_PAGES
     * pages to promise sampled data density should be as uniform as possible.
     */
    if (SUPPORT_PRETTY_ANALYZE && estimate_table_rownum) {
        /*
         * set targrows as sample page num, so we can use BlockSampler to sample
         * pages as random as possible
         */
        if (targrows > MAX_ESTIMATE_SAMPLE_PAGES)
            targrows = MAX_ESTIMATE_SAMPLE_PAGES;

        /*
         * we should sample all partitions , so we sample no more than
         * MAX_ESTIMATE_PART_SAMPLE_PAGES for each partition
         */
        if (RelationIsPartition(onerel))
            targrows = MAX_ESTIMATE_PART_SAMPLE_PAGES;
    }

    /* Need a cutoff xmin for HeapTupleSatisfiesVacuum */
    OldestXmin = GetOldestXmin(onerel);

retry:
    /* Prepare for sampling block numbers */
    BlockSampler_Init(&bs, totalblocks, targrows);
    /* Prepare for sampling rows */
    rstate = anl_init_selection_state(targrows);

    ADIO_RUN()
    {
        if (1 == retrycount) {
            uint32 quantity =
                Min(u_sess->attr.attr_storage.prefetch_quantity, (g_instance.attr.attr_storage.NBuffers / 4));
            anlprefetch.fetchlist1.size =
                (uint32)((quantity > (totalblocks / 2 + 1)) ? (totalblocks / 2 + 1) : quantity);
            anlprefetch.fetchlist1.blocklist = (BlockNumber*)palloc(sizeof(BlockNumber) * anlprefetch.fetchlist1.size);
            anlprefetch.fetchlist1.anl_idx = 0;
            anlprefetch.fetchlist1.load_count = 0;

            anlprefetch.fetchlist2.size = anlprefetch.fetchlist1.size;
            anlprefetch.fetchlist2.blocklist = (BlockNumber*)palloc(sizeof(BlockNumber) * anlprefetch.fetchlist2.size);
            anlprefetch.fetchlist2.anl_idx = 0;
            anlprefetch.fetchlist2.load_count = 0;
            anlprefetch.init = false;
        }

        ereport(DEBUG2,
            (errmodule(MOD_ADIO),
                errmsg("analyze prefetch for %s prefetch quantity(%u)",
                    RelationGetRelationName(onerel),
                    anlprefetch.fetchlist1.size)));
    }
    ADIO_END();

    while (InvalidBlockNumber !=
           (targblock = BlockSampler_GetBlock<false>(onerel, &bs, &anlprefetch, 0, NULL, estimate_table_rownum))) {
        Buffer targbuffer;
        Page targpage;
        OffsetNumber targoffset, maxoffset;

        vacuum_delay_point();
        sampleblock++;

        /*
         * We must maintain a pin on the target page's buffer to ensure that
         * the maxoffset value stays good (else concurrent VACUUM might delete
         * tuples out from under us).  Hence, pin the page until we are done
         * looking at it.  We also choose to hold sharelock on the buffer
         * throughout --- we could release and re-acquire sharelock for each
         * tuple, but since we aren't doing much work per tuple, the extra
         * lock traffic is probably better avoided.
         */
        targbuffer = ReadBufferExtended(onerel, MAIN_FORKNUM, targblock, RBM_NORMAL, u_sess->analyze_cxt.vac_strategy);
        LockBuffer(targbuffer, BUFFER_LOCK_SHARE);
        targpage = BufferGetPage(targbuffer);

        if (RelationIsUstoreFormat(onerel)) {
            /* All ustore tuples are converted to heap tuples and stored in rows array.
             * The attcacheoff of ustore tuples in tupleDesc of the relation can now be 
             * reset to avoid their usage while reading heap tuples from rows array.
             */
            for (int i = 0; i < onerel->rd_att->natts; i++) {
                if (onerel->rd_att->attrs[i].attcacheoff >= 0) {
                    onerel->rd_att->attrs[i].attcacheoff = -1;
                }
            }

            /* TO DO: Need to switch this to inplaceheapam_scan_analyze_next_block after we have tableam. */
            TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(onerel), false, onerel->rd_tam_ops);
            maxoffset = UHeapPageGetMaxOffsetNumber(targpage);

            /* Inner loop over all tuples on the selected page */
            for (targoffset = FirstOffsetNumber; targoffset <= maxoffset; targoffset++) {
                RowPtr *lp = UPageGetRowPtr(targpage, targoffset);
                bool sampleIt = false;
                TransactionId xid;
                UHeapTuple targTuple;

                /*
                 * For UHeap, we need to count delete committed rows towards dead rows
                 * which would have been same, if the tuple was present in heap.
                 */
                if (RowPtrIsDeleted(lp)) {
                    deadrows += 1;
                    continue;
                }

                /*
                 * We ignore unused and redirect line pointers.  DEAD line pointers
                 * should be counted as dead, because we need vacuum to run to get rid
                 * of them.  Note that this rule agrees with the way that
                 * heap_page_prune() counts things.
                 */
                if (!RowPtrIsNormal(lp)) {
                    if (RowPtrIsDeleted(lp)) {
                        deadrows += 1;
                    }
                    continue;
                }

                if (!RowPtrHasStorage(lp)) {
                    continue;
                }

                /* Allocate memory for target tuple. */
                targTuple = UHeapGetTuple(onerel, targbuffer, targoffset);

                switch (UHeapTupleSatisfiesOldestXmin(targTuple, OldestXmin,
                    targbuffer, true, &targTuple, &xid, NULL, onerel)) {
                    case UHEAPTUPLE_LIVE:
                        sampleIt = true;
                        liverows += 1;
                        break;

                    case UHEAPTUPLE_DEAD:
                    case UHEAPTUPLE_RECENTLY_DEAD:
                        /* Count dead and recently-dead rows */
                        deadrows += 1;
                        break;

                    case UHEAPTUPLE_INSERT_IN_PROGRESS:
                        /*
                         * Insert-in-progress rows are not counted.  We assume that
                         * when the inserting transaction commits or aborts, it will
                         * send a stats message to increment the proper count.  This
                         * works right only if that transaction ends after we finish
                         * analyzing the table; if things happen in the other order,
                         * its stats update will be overwritten by ours.  However, the
                         * error will be large only if the other transaction runs long
                         * enough to insert many tuples, so assuming it will finish
                         * after us is the safer option.
                         *
                         * A special case is that the inserting transaction might be
                         * our own.  In this case we should count and sample the row,
                         * to accommodate users who load a table and analyze it in one
                         * transaction.  (pgstat_report_analyze has to adjust the
                         * numbers we send to the stats collector to make this come
                         * out right.)
                         */
                        if (TransactionIdIsCurrentTransactionId(xid)) {
                            sampleIt = true;
                            liverows += 1;
                        }
                        break;

                    case UHEAPTUPLE_DELETE_IN_PROGRESS:
                        /*
                         * We count delete-in-progress rows as still live, using the
                         * same reasoning given above; but we don't bother to include
                         * them in the sample.
                         *
                         * If the delete was done by our own transaction, however, we
                         * must count the row as dead to make pgstat_report_analyze's
                         * stats adjustments come out right.  (Note: this works out
                         * properly when the row was both inserted and deleted in our
                         * xact.)
                         */
                        if (TransactionIdIsCurrentTransactionId(xid)) {
                            deadrows += 1;
                        } else {
                            liverows += 1;
                        }
                        break;

                    default:
                        elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
                        break;
                }

                if (sampleIt) {
                    ExecStoreTuple(targTuple, slot, InvalidBuffer, false);

                    /*
                     * The first targrows sample rows are simply copied into the
                     * reservoir. Then we start replacing tuples in the sample until
                     * we reach the end of the relation.  This algorithm is from Jeff
                     * Vitter's paper (see full citation below). It works by
                     * repeatedly computing the number of tuples to skip before
                     * selecting a tuple, which replaces a randomly chosen element of
                     * the reservoir (current set of tuples).  At all times the
                     * reservoir is a true random sample of the tuples we've passed
                     * over so far, so when we fall off the end of the relation we're
                     * done.
                     */
                    if (numrows < targrows) {
                        rows[numrows++] = ExecCopySlotTuple(slot);
                    } else {
                        if (rowstoskip < 0) {
                            rowstoskip = anl_get_next_S(samplerows, targrows, &rstate);
                        }
                        if (rowstoskip <= 0) {
                            /*
                             * Found a suitable tuple, so save it, replacing one
                             * old tuple at random
                             */
                            int64 k = (int64)(targrows * anl_random_fract());

                            AssertEreport(k >= 0 && k < targrows, MOD_OPT,
                                "Index number out of range when replacing tuples.");

                            if (!estimate_table_rownum) {
                                heap_freetuple(rows[k]);
                                rows[k] = ExecCopySlotTuple(slot);
                            }
                        }
                        rowstoskip -= 1;
                    }
                    samplerows += 1;
                }

                /* Free memory for target tuple. */
                if (targTuple) {
                    UHeapFreeTuple(targTuple);
                }
            }

            /* Now release the lock and pin on the page */
            ExecDropSingleTupleTableSlot(slot);

            /* All ustore tuples are converted to heap tuples and stored in rows array.
             * The attcacheoff of ustore tuples in tupleDesc of the relation can now be 
             * reset to avoid their usage while reading heap tuples from rows array.
             */
            for (int i = 0; i < onerel->rd_att->natts; i++) {
                if (onerel->rd_att->attrs[i].attcacheoff >= 0) {
                    onerel->rd_att->attrs[i].attcacheoff = -1;
                }
            }

            goto uheap_end;
        }

        maxoffset = PageGetMaxOffsetNumber(targpage);
        /* Inner loop over all tuples on the selected page */
        for (targoffset = FirstOffsetNumber; targoffset <= maxoffset; targoffset++) {
            ItemId itemid;
            HeapTupleData targtuple;
            bool sample_it = false;

            /* IO collector and IO scheduler for analyze statement */
            if (ENABLE_WORKLOAD_CONTROL)
                IOSchedulerAndUpdate(IO_TYPE_READ, 10, IO_TYPE_ROW);

            targtuple.t_tableOid = InvalidOid;
            targtuple.t_bucketId = InvalidBktId;
            HeapTupleCopyBaseFromPage(&targtuple, targpage);

#ifdef PGXC
            targtuple.t_xc_node_id = InvalidOid;
#endif
            itemid = PageGetItemId(targpage, targoffset);
            /*
             * We ignore unused and redirect line pointers.  DEAD line
             * pointers should be counted as dead, because we need vacuum to
             * run to get rid of them.	Note that this rule agrees with the
             * way that heap_page_prune() counts things.
             */
            if (!ItemIdIsNormal(itemid)) {
                if (ItemIdIsDead(itemid))
                    deadrows += 1;
                continue;
            }

            ItemPointerSet(&targtuple.t_self, targblock, targoffset);

            targtuple.t_tableOid = RelationGetRelid(onerel);
            targtuple.t_bucketId = RelationGetBktid(onerel);
            targtuple.t_data = (HeapTupleHeader)PageGetItem(targpage, itemid);
            targtuple.t_len = ItemIdGetLength(itemid);

            switch (HeapTupleSatisfiesVacuum(&targtuple, OldestXmin, targbuffer, isAnalyzing)) {
                case HEAPTUPLE_LIVE:
                    sample_it = true;
                    liverows += 1;
                    break;

                case HEAPTUPLE_DEAD:
                case HEAPTUPLE_RECENTLY_DEAD:
                    /* Count dead and recently-dead rows */
                    deadrows += 1;
                    break;

                case HEAPTUPLE_INSERT_IN_PROGRESS:

                    /*
                     * Insert-in-progress rows are not counted.  We assume
                     * that when the inserting transaction commits or aborts,
                     * it will send a stats message to increment the proper
                     * count.  This works right only if that transaction ends
                     * after we finish analyzing the table; if things happen
                     * in the other order, its stats update will be
                     * overwritten by ours.  However, the error will be large
                     * only if the other transaction runs long enough to
                     * insert many tuples, so assuming it will finish after us
                     * is the safer option.
                     *
                     * A special case is that the inserting transaction might
                     * be our own.	In this case we should count and sample
                     * the row, to accommodate users who load a table and
                     * analyze it in one transaction.  (pgstat_report_analyze
                     * has to adjust the numbers we send to the stats
                     * collector to make this come out right.)
                     */
                    if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(targpage, targtuple.t_data))) {
                        sample_it = true;
                        liverows += 1;
                    }
                    break;

                case HEAPTUPLE_DELETE_IN_PROGRESS:

                    /*
                     * We count delete-in-progress rows as still live, using
                     * the same reasoning given above; but we don't bother to
                     * include them in the sample.
                     *
                     * If the delete was done by our own transaction, however,
                     * we must count the row as dead to make
                     * pgstat_report_analyze's stats adjustments come out
                     * right.  (Note: this works out properly when the row was
                     * both inserted and deleted in our xact.)
                     *
                    .* The net effect of these choices is that we act as
                    .* though an IN_PROGRESS transaction hasn't happened yet,
                    .* except if it is our own transaction, which we assume
                    .* has happened.
                    .*
                    .* This approach ensures that we behave sanely if we see
                    .* both the pre-image and post-image rows for a row being
                    .* updated by a concurrent transaction: we will sample the
                    .* pre-image but not the post-image.  We also get sane
                    .* results if the concurrent transaction never commits.
                     */
                    if (TransactionIdIsCurrentTransactionId(HeapTupleGetUpdateXid(&targtuple)))
                        deadrows += 1;
                    else {
                        sample_it = true;
                        liverows += 1;
                    }
                    break;

                default:
                    ereport(
                        ERROR, (errcode(ERRCODE_CASE_NOT_FOUND), errmsg("unexpected HeapTupleSatisfiesVacuum result")));
                    break;
            }

            if (sample_it) {
                /*
                 * The first targrows sample rows are simply copied into the
                 * reservoir. Then we start replacing tuples in the sample
                 * until we reach the end of the relation.	This algorithm is
                 * from Jeff Vitter's paper (see full citation below). It
                 * works by repeatedly computing the number of tuples to skip
                 * before selecting a tuple, which replaces a randomly chosen
                 * element of the reservoir (current set of tuples).  At all
                 * times the reservoir is a true random sample of the tuples
                 * we've passed over so far, so when we fall off the end of
                 * the relation we're done.
                 */
                if (numrows < targrows) {
                    if (!estimate_table_rownum)
                        rows[numrows++] = heapCopyTuple(&targtuple, onerel->rd_att, targpage);
                    else
                        numrows++;
                } else {
                    /*
                     * t in Vitter's paper is the number of records already
                     * processed.  If we need to compute a new S value, we
                     * must use the not-yet-incremented value of samplerows as
                     * t.
                     */
                    if (rowstoskip < 0)
                        rowstoskip = anl_get_next_S(samplerows, targrows, &rstate);

                    if (rowstoskip <= 0) {
                        /*
                         * Found a suitable tuple, so save it, replacing one
                         * old tuple at random
                         */
                        int64 k = (int64)(targrows * anl_random_fract());

                        AssertEreport(
                            k >= 0 && k < targrows, MOD_OPT, "Index number out of range when replacing tuples.");

                        if (!estimate_table_rownum) {
                            tableam_tops_free_tuple(rows[k]);
                            rows[k] = heapCopyTuple(&targtuple, onerel->rd_att, targpage);
                        }
                    }

                    rowstoskip -= 1;
                }

                samplerows += 1;
            }
        }

uheap_end:
        /* Now release the lock and pin on the page */
        UnlockReleaseBuffer(targbuffer);

        if (estimate_table_rownum && !SUPPORT_PRETTY_ANALYZE) {
            if (liverows > 0)
                break;

            totalblocks--;
            ereport(LOG,
                (errmsg("ANALYZE INFO : estimate total rows of \"%s\" - no lived rows in blockno: %u",
                    RelationGetRelationName(onerel),
                    targblock)));
        }
    }

    if (estimate_table_rownum) {
        if (liverows > 0) {
            /* sampled lived rows, just estimate total lived tuple num */
            AssertEreport(0 < sampleblock && 0 < totalblocks, MOD_OPT, "This should not happend when liverows>0");
            *totalrows = totalblocks * (liverows / sampleblock);
        } else if (sampleblock >= totalblocks || retrycount > MAX_ESTIMATE_RETRY_TIMES) {
            /*
             * if all tuples is dead or we have tried too many times
             * (MAX_ESTIMATE_RETRY_TIMES + 1), just return 0
             */
            *totalrows = 0.0;
        } else if (SUPPORT_PRETTY_ANALYZE) {
            /*
             * if we have tried MAX_ESTIMATE_RETRY_TIMES, just sample
             * tuples as normal analyze sample
             */
            if (MAX_ESTIMATE_RETRY_TIMES == retrycount)
                targrows = ori_targrows;

            /*
             * retry to fetch lived rows in pretty analyze mode unless
             * 1. fetch lived tuple (liverows > 0)
             * 2. retry enough times  (MAX_ESTIMATE_RETRY_TIMES)
             */
            retrycount++;
            goto retry;
        }

        ADIO_RUN()
        {
            pfree_ext(anlprefetch.fetchlist1.blocklist);
            pfree_ext(anlprefetch.fetchlist2.blocklist);
        }
        ADIO_END();

        ereport(elevel,
            (errmsg("ANALYZE INFO : estimate total rows of \"%s\": scanned %u pages of "
                    "total %u pages with %u retry times, containing %.0f "
                    "live rows and %.0f dead rows,  estimated %.0f total rows",
                RelationGetRelationName(onerel),
                sampleblock,
                totalblocks,
                retrycount,
                liverows,
                deadrows,
                *totalrows)));

        *totaldeadrows = 0.0;

        /*
         * we just count lived rows and dead rows, and have never sample tuples to fill
         * rows[], so return 0 here to avoid undesirable memory free
         */
        return 0;
    }

    /*
     * If we didn't find as many tuples as we wanted then we're done. No sort
     * is needed, since they're already in order.
     *
     * Otherwise we need to sort the collected tuples by position
     * (itempointer). It's not worth worrying about corner cases where the
     * tuples are already sorted.
     */

    // XXXTAM:  
    // Even if in case of USTORE tables, the tuples will be heap tuples 
    // as a result of the ExecCopySlotTuple() call when we stored 
    // the tuple pointer in the rows array, so in both 
    // cases, the comparator function is compare_rows()
    // We may need to revisit this later to use a the 
    // Tuple generic type and use a specific comparator for 
    // utuples. 
    // ---------------------------------------------------------------
    if (numrows == targrows) {
        qsort((void*)rows, numrows, sizeof(HeapTuple), compare_rows);
    }
    /*
     * Estimate total numbers of live and dead rows in relation, extrapolating
     * on the assumption that the average tuple density in pages we didn't
     * scan is the same as in the pages we did scan.  Since what we scanned is
     * a random sample of the pages in the relation, this should be a good
     * assumption.
     */
    if (bs.m > 0) {
        *totalrows = floor((liverows / bs.m) * totalblocks + 0.5);
        *totaldeadrows = floor((deadrows / bs.m) * totalblocks + 0.5);
    } else {
        *totalrows = 0.0;
        *totaldeadrows = 0.0;
    }

    ADIO_RUN()
    {
        pfree_ext(anlprefetch.fetchlist1.blocklist);
        pfree_ext(anlprefetch.fetchlist2.blocklist);
    }
    ADIO_END();

    /*
     * Emit some interesting relation info, only in datanode
     */
    if (IS_PGXC_DATANODE) {
        ereport(elevel,
            (errmsg("ANALYZE INFO : \"%s\": scanned %d of %u pages, "
                    "containing %.0f live rows and %.0f dead rows; "
                    "%ld rows in sample, %.0f estimated total rows",
                RelationGetRelationName(onerel),
                bs.m,
                totalblocks,
                liverows,
                deadrows,
                numrows,
                *totalrows)));
    }

    return numrows;
}

template <int attlen, bool hasNull>
Datum GetValue(CU* cuPtr, int rowIdx)
{
    return (Datum)cuPtr->GetValue<attlen, hasNull>(rowIdx);
}

void InitGetValFunc(int attlen, GetValFunc* getValFuncPtr, int col)
{
    switch (attlen) {
        case sizeof(uint8): {
            getValFuncPtr[col][0] = &GetValue<1, false>;
            getValFuncPtr[col][1] = &GetValue<1, true>;
            break;
        }
        case sizeof(uint16): {
            getValFuncPtr[col][0] = &GetValue<2, false>;
            getValFuncPtr[col][1] = &GetValue<2, true>;
            break;
        }
        case sizeof(uint32): {
            getValFuncPtr[col][0] = &GetValue<4, false>;
            getValFuncPtr[col][1] = &GetValue<4, true>;
            break;
        }
        case sizeof(uint64): {
            getValFuncPtr[col][0] = &GetValue<8, false>;
            getValFuncPtr[col][1] = &GetValue<8, true>;
            break;
        }
        case 12: {
            getValFuncPtr[col][0] = &GetValue<12, false>;
            getValFuncPtr[col][1] = &GetValue<12, true>;
            break;
        }
        case 16: {
            getValFuncPtr[col][0] = &GetValue<16, false>;
            getValFuncPtr[col][1] = &GetValue<16, true>;
            break;
        }
        case -1: {
            getValFuncPtr[col][0] = &GetValue<-1, false>;
            getValFuncPtr[col][1] = &GetValue<-1, true>;
            break;
        }
        case -2: {
            getValFuncPtr[col][0] = &GetValue<-2, false>;
            getValFuncPtr[col][1] = &GetValue<-2, true>;
            break;
        }
        default:
            ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("unsupported datatype")));
    }
}

/*
 * @Description: analyze prefetch cu
 * @Param[IN] analyzeAttrNum: analyze column num
 * @Param[IN] attrSeq: cloumn order list
 * @Param[IN] cstoreScan: cstore scan desc
 * @Param[IN] cuidList: cu id list for prefetch
 * @Param[IN] cuidListCount: cu id list count
 * @See also:
 */
void CstoreAnalyzePrefetch(
    CStoreScanDesc cstoreScan, BlockNumber* cuidList, int32 cuidListCount, int analyzeAttrNum, int32* attrSeq)
{
    AioDispatchCUDesc_t** dList = NULL;

    t_thrd.cstore_cxt.InProgressAioCUDispatch =
        (AioDispatchCUDesc_t**)palloc(sizeof(AioDispatchCUDesc_t*) * MAX_CU_PREFETCH_REQSIZ);
    dList = t_thrd.cstore_cxt.InProgressAioCUDispatch;
    t_thrd.cstore_cxt.InProgressAioCUDispatchCount = 0;
    File* vfdList = (File*)palloc(sizeof(File) * MAX_CU_PREFETCH_REQSIZ);
    errno_t rc =
        memset_s((char*)vfdList, sizeof(File) * MAX_CU_PREFETCH_REQSIZ, 0xFF, sizeof(File) * MAX_CU_PREFETCH_REQSIZ);
    securec_check(rc, "\0", "\0");

    for (int col = 0; col < analyzeAttrNum; col++) {
        for (int i = 0; i < cuidListCount; i++) {
            CUDesc cuDesc;
            int colNum = attrSeq[col];
            uint32 cuid = cuidList[i] + FirstCUID + 1;
            /* We may failed while loading data,that may cause some cuid did not match a real cudesc */
            if (cstoreScan->m_CStore->GetCUDesc(colNum, cuid, &cuDesc, NULL) != true) {
                continue;
            }

            /* deleted cu no need prefetch */
            cstoreScan->m_CStore->GetCUDeleteMaskIfNeed(cuid, NULL);
            if (cstoreScan->m_CStore->IsTheWholeCuDeleted(cuDesc.row_count)) {
                continue;
            }

            cstoreScan->m_CStore->CUPrefetch(
                &cuDesc, colNum, dList, t_thrd.cstore_cxt.InProgressAioCUDispatchCount, vfdList);
        }
    }

    if (t_thrd.cstore_cxt.InProgressAioCUDispatchCount > 0) {
#ifndef ENABLE_LITE_MODE
        int tmp_count = t_thrd.cstore_cxt.InProgressAioCUDispatchCount;
        HOLD_INTERRUPTS();
        FileAsyncCURead(dList, t_thrd.cstore_cxt.InProgressAioCUDispatchCount);
        t_thrd.cstore_cxt.InProgressAioCUDispatchCount = 0;
        RESUME_INTERRUPTS();

        FileAsyncCUClose(vfdList, tmp_count);
#endif
    }

    pfree_ext(dList);
    pfree_ext(vfdList);
    t_thrd.cstore_cxt.InProgressAioCUDispatch = NULL;
    t_thrd.cstore_cxt.InProgressAioCUDispatchCount = 0;
    t_thrd.storage_cxt.InProgressAioType = AioUnkown;
}

#define CSTORE_ANALYZE_PREFETCH_QUANTITY 32

typedef struct sample_tuple_cell_struct {
    Datum* values;
    bool* nulls;
} sample_tuple_cell;

#define maxAnalyzeUseCstoreBufferSize (g_instance.attr.attr_storage.cstore_buffers * 1024LL / 8)

/*
 * AcquireSampleCStoreRows -- acquire a random sample of rows from the table
 *
 * Selected rows are returned in the caller-allocated array rows[], which
 * must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also estimate the total number of live and dead rows in the table,
 * and return them into *totalrows and *totaldeadrows, respectively.
 *
 * To improve the analyze efficiency in the condition of severl attrs are selected,
 * we add two parameters which record the information of seleced attrs.The perform
 * can only used for column storage table.
 *
 * The returned list of tuples is in order by physical position in the table.
 * (We will rely on this later to derive correlation estimates.)
 */
template <bool estimate_table_rownum>
static int64 AcquireSampleCStoreRows(Relation onerel, int elevel, HeapTuple* rows, int64 targrows, double* totalrows,
    double* totaldeadrows, VacAttrStats** vacattrstats, int analyzeAttrNum)
{
    int64 numrows = 0;       /* # rows now in reservoir */
    int64 delta_numrows = 0; /* # delta rows now in reservoir */
    int32 samplerows = 0;    /* total # rows collected */
    int64 liverows = 0;      /* # live rows seen */
    int64 deadrows = 0;      /* # dead rows seen */
    BlockNumber totalblocks = 0;
    BlockNumber estBlkNum = 0;
    BlockSamplerData bs;
    double rstate;
    AnlPrefetch anlprefetch;
    int cusize_threshold = maxAnalyzeUseCstoreBufferSize;
    anlprefetch.blocklist = NULL;

    AssertEreport(targrows > 0, MOD_OPT, "target rows must be greater than 0 when sampling");
    /*
     * all system table is row-orientation, column-orientation table must
     * be a user-defined table, so we just return without sample
     */
    if (IS_PGXC_COORDINATOR) {
        *totalrows = 0;
        *totaldeadrows = 0;
        return 0;
    }
    
    /* For externed statistics */
    Bitmapset* bms_attnums = es_get_attnums_to_analyze(vacattrstats, analyzeAttrNum);
    int num_attnums = bms_num_members(bms_attnums);
    int colTotalNum = onerel->rd_att->natts;
    int32* attrSeq = NULL;
    int16* colIdx = (int16*)palloc0(sizeof(int16) * num_attnums);
    int* slotIdList = NULL;
    CStoreScanDesc cstoreScanDesc = NULL;
    CU** cuPtr = NULL;
    Relation deltarel = NULL;
    double totalRowCnts = 0;
    double deltarows = 0;
    double curows = 0;
    double cudeadrows = 0;

    /* consider delta relation first */
    Assert(OidIsValid(onerel->rd_rel->reldeltarelid));
    /* Open the delta table.  We only need AccessShareLock on the it. */
    deltarel = relation_open((onerel->rd_rel->reldeltarelid), AccessShareLock);
    /* just get estimate totoal row from delta relation */
    delta_numrows = acquire_sample_rows<true>(deltarel, elevel, rows, targrows, &deltarows, totaldeadrows);

    /* Here set nulls to true,  and these attrs which need not to be analyzed will alaways be null */
    for (int i = 0; i < num_attnums; ++i) {
        colIdx[i] = bms_first_member(bms_attnums);
    }

    /* initialize the local snapshot for current analyze. */
    Snapshot tmpSnapshot = GetActiveSnapshot();
    Assert(tmpSnapshot != NULL);

    cstoreScanDesc = CStoreBeginScan(onerel, num_attnums, colIdx, tmpSnapshot, false);
    totalblocks = CStoreRelGetCUNumByNow(cstoreScanDesc);

    curows = (double)cstoreScanDesc->m_CStore->GetLivedRowNumbers(&deadrows); /* alive-row number in CU */
    if (curows <= 0.0) {
        relation_close(deltarel, AccessShareLock);
        CStoreEndScan(cstoreScanDesc);
        pfree_ext(colIdx);
        pfree_ext(bms_attnums);
        return 0;
    }
    *totalrows = totalRowCnts = curows + deltarows;                           /* alive-row number */
    cudeadrows = (double)deadrows;
    *totaldeadrows += cudeadrows;
    deadrows = 0; /* reuse it in non-pretty mode for dead tuples */

    /* for pretty mode we just return actual lived rows */
    if (estimate_table_rownum && SUPPORT_PRETTY_ANALYZE) {
        /* only get estimate rows ,free delta rows and close relation */
        for (int i = 0; i < delta_numrows; i++) {
            if (rows[i]) {
                pfree_ext(rows[i]);
                rows[i] = NULL;
            }
        }
        relation_close(deltarel, AccessShareLock);
        CStoreEndScan(cstoreScanDesc);
        pfree_ext(colIdx);
        pfree_ext(bms_attnums);
        return 0;
    }

    attrSeq = (int*)palloc0(sizeof(int32) * num_attnums);
    slotIdList = (int*)palloc0(sizeof(int) * num_attnums);
    cuPtr = (CU**)palloc0(sizeof(CU*) * colTotalNum);

    if (!estimate_table_rownum) {
        /* free rows */
        for (int i = 0; i < delta_numrows; i++) {
            if (rows[i]) {
                pfree_ext(rows[i]);
                rows[i] = NULL;
            }
        }
        int64 deltatargrows = 0;
        /* recalculate tartget num */
        if (deltarows > 0) {
            deltatargrows = (int)rint(targrows * deltarows / totalRowCnts);
            /* Make sure we don't overrun due to roundoff error */
            deltatargrows = Min(deltatargrows, targrows);
            if (deltatargrows > 0) {
                double trows = 0;
                double tdrows = 0;
                /* Fetch a random sample of the delta table's rows */
                delta_numrows = acquire_sample_rows<false>(deltarel, elevel, rows, deltatargrows, &trows, &tdrows);
                /* fix total rows */
                *totalrows = trows + curows;
                *totaldeadrows = tdrows + cudeadrows;
            }
        }

        /* change cstore target num */
        int64 temp_target = (int)rint(targrows * curows / totalRowCnts);
        /* Make sure we don't overrun due to roundoff error */
        temp_target = Min(temp_target, targrows - deltatargrows);
        targrows = temp_target;

        /*
         * calculate cu number by following formula
         * 1. get lived row number
         * 2. estimate row size by suppose the width of each column is 4
         * 3. estimate how many pages needed if it's row-stored
         * 4. evaluate how many CUs needed to sample
         */
        int totalwidth = 0;
        int relpages = 0;
        BlockNumber sampleCUs = 0;

        totalwidth = 4 * onerel->rd_att->natts;
        relpages = ceil(*totalrows * totalwidth / BLCKSZ);
        if (relpages == 0) {
            relpages = 1;
        }
        sampleCUs = ceil((double)targrows / relpages * totalblocks);
        sampleCUs = (sampleCUs > totalblocks) ? totalblocks : sampleCUs;

        elog(DEBUG2, "ANALYZE INFO : sample %u cu from totoal %u cu", sampleCUs, totalblocks);
        BlockSampler_Init(&bs, totalblocks, sampleCUs);
    } else {
        AssertEreport(!SUPPORT_PRETTY_ANALYZE, MOD_OPT, "");
        BlockSampler_Init(&bs, totalblocks, targrows);
        elog(DEBUG2, "ANALYZE INFO : sample %ld rows from totoal %u cu", targrows, totalblocks);
    }
    rstate = anl_init_selection_state(targrows);

    /* Prepare for sampling rows */
    CStore* cstore = cstoreScanDesc->m_CStore;
    FormData_pg_attribute* attrs = cstore->m_relation->rd_att->attrs;
    GetValFunc* getValFuncPtr = (GetValFunc*)palloc(sizeof(GetValFunc) * colTotalNum);

    /* change sample rows pointer */
    rows += delta_numrows;

    for (int col = 0; col < num_attnums; ++col) {
        int colSeq = attrSeq[col] = colIdx[col] - 1;
        InitGetValFunc(attrs[colSeq].attlen, getValFuncPtr, colSeq);
    }

    ADIO_RUN()
    {
        uint32 quantity = (uint32)CSTORE_ANALYZE_PREFETCH_QUANTITY;
        anlprefetch.fetchlist1.size = (uint32)((quantity > (totalblocks / 2 + 1)) ? (totalblocks / 2 + 1) : quantity);
        anlprefetch.fetchlist1.blocklist = (BlockNumber*)palloc(sizeof(BlockNumber) * anlprefetch.fetchlist1.size);
        anlprefetch.fetchlist1.anl_idx = 0;
        anlprefetch.fetchlist1.load_count = 0;

        anlprefetch.fetchlist2.size = anlprefetch.fetchlist1.size;
        anlprefetch.fetchlist2.blocklist = (BlockNumber*)palloc(sizeof(BlockNumber) * anlprefetch.fetchlist2.size);
        anlprefetch.fetchlist2.anl_idx = 0;
        anlprefetch.fetchlist2.load_count = 0;
        anlprefetch.init = false;
        ereport(DEBUG1,
            (errmodule(MOD_ADIO),
                errmsg("analyze prefetch for %s prefetch quantity(%u)",
                    RelationGetRelationName(onerel),
                    anlprefetch.fetchlist1.size)));
    }
    ADIO_END();

    MemoryContext sample_context;
    MemoryContext old_context;
    BlockNumber targblock;
    int fstColIdx = CStoreGetfstColIdx(onerel);
    Datum* constValues = (Datum*)palloc(sizeof(Datum) * colTotalNum);
    bool* nullValues = (bool*)palloc(sizeof(bool) * colTotalNum);
    Datum* values = (Datum*)palloc0(sizeof(Datum) * colTotalNum);
    bool* nulls = (bool*)palloc0(sizeof(bool) * colTotalNum);
    int* funcIdx = (int*)palloc0(sizeof(int) * colTotalNum);
    List* sampleTupleInfo = NIL;
    sample_tuple_cell* st_cell = NULL;
    ListCell* cell1 = NULL;
    ListCell* cell2 = NULL;
    ListCell* cell3 = NULL;

    sample_context = AllocSetContextCreate(CurrentMemoryContext,
        "sample cstore rows for analyze",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    while (
        InvalidBlockNumber != (targblock = BlockSampler_GetBlock<true>(
                                   cstoreScanDesc, &bs, &anlprefetch, num_attnums, attrSeq, estimate_table_rownum))) {
        List* valueLocation = NIL;
        List* targoffsetList = NIL;
        int location = 0;
        CUDesc cuDesc;
        uint16 targoffset = 0;
        uint16 maxoffset;
        int64 total_cusize = 0;
        int start_col = -1;
        int end_col = -1;
        bool first_batch = true;
        bool all_in_buffer = true;

        targblock = targblock + FirstCUID + 1;

        /* We may failed while loading data,that may cause some cuid did not match a real cudesc  */
        if (cstoreScanDesc->m_CStore->GetCUDesc(fstColIdx, targblock, &cuDesc, tmpSnapshot) != true)
            continue;

        /* IO collector and IO scheduler for analyze statement */
        if (ENABLE_WORKLOAD_CONTROL)
            IOSchedulerAndUpdate(IO_TYPE_READ, 1, IO_TYPE_COLUMN);

        cstore->GetCUDeleteMaskIfNeed(targblock, tmpSnapshot);

        /* quit this loop quickly if all the tuples are dead in this CU unit. */
        if (cstore->IsTheWholeCuDeleted(cuDesc.row_count))
            continue;

        maxoffset = cuDesc.row_count;

        if (estimate_table_rownum) {
            liverows = maxoffset;
            estBlkNum++;

            if (liverows > 0)
                break;

            ereport(DEBUG1,
                (errmsg("ANALYZE INFO : estimate total rows of \"%s\" - no lived rows in cuid: %u",
                    RelationGetRelationName(onerel),
                    targblock)));

            totalblocks--;
            continue;
        }

        /* reset null flag and const value flag for next block */
        errno_t rc;

        rc = memset_s(nulls, sizeof(bool) * colTotalNum, true, sizeof(bool) * colTotalNum);
        securec_check(rc, "\0", "\0");
        rc = memset_s(nullValues, sizeof(bool) * colTotalNum, false, sizeof(bool) * colTotalNum);
        securec_check(rc, "\0", "\0");
        rc = memset_s(constValues, sizeof(Datum) * colTotalNum, 0, sizeof(Datum) * colTotalNum);
        securec_check(rc, "\0", "\0");

/* copy data from cu buffer */
#define copy_sample_dataum(dest, col_num, offset)                                   \
    do {                                                                            \
        old_context = MemoryContextSwitchTo(sample_context);                        \
        Datum dm = getValFuncPtr[colNum][funcIdx[colNum]](cuPtr[colNum], (offset)); \
        int16 valueTyplen = attrs[(col_num)].attlen;                               \
        bool valueTypbyval = attrs[(col_num)].attlen == 0 ? false : true;          \
        if (valueTyplen < 0)                                                        \
            (dest) = PointerGetDatum(PG_DETOAST_DATUM_COPY(dm));                    \
        else                                                                        \
            (dest) = datumCopy(dm, valueTypbyval, valueTyplen);                     \
        (void)MemoryContextSwitchTo(old_context);                                         \
    } while (0)

/*
 * check if special row is selected for analyze
 * 1. skip dead rows (deleted rows)
 * 2. fetch all rows if fetched-rows <  targrows
 * 3. random to replace a tuple in random location
 */
#define check_match_tuple(copyTuple)                                              \
    do {                                                                          \
        (copyTuple) = false;                                                      \
        if (cstore->IsDeadRow((uint32)targblock, (uint32)targoffset)) {           \
            deadrows++;                                                           \
        } else if (numrows < targrows) {                                          \
            (copyTuple) = true;                                                   \
            location = numrows;                                                   \
            numrows++;                                                            \
            samplerows++;                                                         \
        } else {                                                                  \
            if (0 >= anl_get_next_S(samplerows, targrows, &rstate)) {             \
                location = (int64)(targrows * anl_random_fract());                \
                (copyTuple) = true;                                               \
                AssertEreport(location >= 0 && location < targrows, MOD_OPT, ""); \
            }                                                                     \
            samplerows++;                                                         \
        }                                                                         \
    } while (0)

#define load_cu_data(s_col, e_col, values, nulls, copy)                                                       \
    do {                                                                                                      \
        for (int col1 = (s_col); col1 <= (e_col); ++col1) {                                                   \
            int colNum = attrSeq[col1];                                                                       \
            if (cuPtr[colNum]) {                                                                              \
                if (cuPtr[colNum]->IsNull(targoffset)) {                                                      \
                    (nulls)[colNum] = true;                                                                   \
                    (values)[colNum] = 0;                                                                     \
                } else {                                                                                      \
                    (nulls)[colNum] = false;                                                                  \
                    if (copy)                                                                                 \
                        copy_sample_dataum((values)[colNum], colNum, targoffset);                             \
                    else                                                                                      \
                        (values)[colNum] = getValFuncPtr[colNum][funcIdx[colNum]](cuPtr[colNum], targoffset); \
                }                                                                                             \
            } else {                                                                                          \
                (nulls)[colNum] = nullValues[colNum];                                                         \
                (values)[colNum] = constValues[colNum];                                                       \
            }                                                                                                 \
        }                                                                                                     \
    } while (0)

        /*
         *  try to load all cu to cstore_buffers
         * 1. if total cu size < cstore_buffers/8, load all cu into cu buffer
         * 2. if total cu size >= cstore_buffers/8, copy sample data to temp
         *    memory to avoid pining too many cu
         */
        for (int col = 0; col < num_attnums; ++col) {
            int colNum = attrSeq[col];

            cuPtr[colNum] = NULL;
            slotIdList[col] = CACHE_BLOCK_INVALID_IDX;

            if (-1 == start_col)
                start_col = col;
            end_col = col;
            (void)cstore->GetCUDesc(colNum, targblock, &cuDesc, tmpSnapshot);

            if (cuDesc.IsNullCU()) {
                nullValues[colNum] = true;
                constValues[colNum] = 0;
                elog(DEBUG2,
                    "ANALYZE INFO - table \"%s\": attnum(%d), cuid(%u) is null",
                    RelationGetRelationName(onerel),
                    colNum,
                    targblock);
                continue;
            } else if (cuDesc.IsSameValCU()) {
                bool shoulFree = false;

                nullValues[colNum] = false;
                old_context = MemoryContextSwitchTo(sample_context);
                constValues[colNum] = CStore::CudescTupGetMinMaxDatum(&cuDesc, &attrs[colNum], true, &shoulFree);
                (void)MemoryContextSwitchTo(old_context);
                elog(DEBUG2,
                    "ANALYZE INFO - table \"%s\": attnum(%d), cuid(%u) is const value",
                    RelationGetRelationName(onerel),
                    colNum,
                    targblock);
                continue;
            } else {
                nullValues[colNum] = false;
                constValues[colNum] = 0;
                cuPtr[colNum] = cstore->GetCUData(&cuDesc, colNum, attrs[colNum].attlen, slotIdList[col]);
                funcIdx[colNum] = cuPtr[colNum]->HasNullValue() ? 1 : 0;
                /*  check vacuum delay each CU IO action. if it's enable to fetch cu data from cu cache, we should
                 * reduce the calling number AMAP. */
                vacuum_delay_point();

                AssertEreport(!cuPtr[colNum]->m_cache_compressed, MOD_OPT, "");
                total_cusize += cuPtr[colNum]->GetUncompressBufSize();

                /*
                 * copy sample cu data to temp memory if
                 * 1. total_cusize >= cusize_threshold
                 * 2. or it is the last column and some sample cu data have beed loaded to temp memory
                 */
                if (total_cusize < cusize_threshold && (first_batch || (num_attnums - 1) != col))
                    continue;

                if (NIL == sampleTupleInfo) {
                    ereport(LOG,
                        (errmsg(
                             "ANALYZE INFO - table \"%s\": copy data from CU buffer", RelationGetRelationName(onerel)),
                            errdetail("enlarge cstore_buffers to avoid copying data")));
                }
                elog(DEBUG2,
                    "ANALYZE INFO - table \"%s\": copy [%d, %d] column data from CU buffer",
                    RelationGetRelationName(onerel),
                    start_col,
                    end_col);
            }

            all_in_buffer = false;
            if (first_batch) {
                /* deal with first batch cu data, we should initialize sample_tuple_cell first */
                bool copyTuple = false;
                cell1 = list_head(sampleTupleInfo) ? list_head(sampleTupleInfo) : NULL;
                for (targoffset = FirstOffsetNumber - 1; targoffset < maxoffset; targoffset++) {
                    check_match_tuple(copyTuple);
                    if (!copyTuple)
                        continue;

                    if (cell1 != NULL) {
                        errno_t rc;

                        st_cell = (sample_tuple_cell*)lfirst(cell1);
                        rc = memset_s(st_cell->nulls, sizeof(bool) * colTotalNum, true, sizeof(bool) * colTotalNum);
                        securec_check(rc, "\0", "\0");
                        rc = memset_s(st_cell->values, sizeof(Datum) * colTotalNum, 0, sizeof(Datum) * colTotalNum);
                        securec_check(rc, "\0", "\0");
                    } else {
                        errno_t rc;

                        st_cell = (sample_tuple_cell*)palloc0(sizeof(sample_tuple_cell));
                        sampleTupleInfo = lappend(sampleTupleInfo, st_cell);

                        st_cell->nulls = (bool*)palloc(sizeof(bool) * colTotalNum);
                        rc = memset_s(st_cell->nulls, sizeof(bool) * colTotalNum, true, sizeof(bool) * colTotalNum);
                        securec_check(rc, "\0", "\0");
                        st_cell->values = (Datum*)palloc0(sizeof(Datum) * colTotalNum);
                    }

                    valueLocation = lappend_int(valueLocation, location);
                    targoffsetList = lappend_int(targoffsetList, targoffset);
                    load_cu_data(start_col, end_col, st_cell->values, st_cell->nulls, true);
                    cell1 = (cell1 && lnext(cell1)) ? lnext(cell1) : NULL;
                }

                for (int col1 = start_col; col1 <= end_col; ++col1) {
                    if (IsValidCacheSlotID(slotIdList[col1]))
                        CUCache->UnPinDataBlock(slotIdList[col1]);
                }

                first_batch = false;
                AssertEreport(sampleTupleInfo, MOD_OPT, "");
            } else {
                /* have build sample_tuple_cell, just copy data to temp memory */
                AssertEreport(sampleTupleInfo, MOD_OPT, "");
                forboth(cell1, sampleTupleInfo, cell3, targoffsetList)
                {
                    st_cell = (sample_tuple_cell*)lfirst(cell1);
                    targoffset = lfirst_int(cell3);
                    load_cu_data(start_col, end_col, st_cell->values, st_cell->nulls, true);
                }

                for (int col1 = start_col; col1 <= end_col; ++col1) {
                    if (IsValidCacheSlotID(slotIdList[col1]))
                        CUCache->UnPinDataBlock(slotIdList[col1]);
                }
            }

            start_col = -1;
        }

        if (all_in_buffer) {
            /* have laoded all cu to cu buffer, just form tuple with cu buffer data */
            bool copyTuple = false;

            AssertEreport(0 == start_col && end_col == num_attnums - 1, MOD_OPT, "");

            for (targoffset = FirstOffsetNumber - 1; targoffset < maxoffset; targoffset++) {
                check_match_tuple(copyTuple);
                if (!copyTuple)
                    continue;

                load_cu_data(0, num_attnums - 1, values, nulls, false);

                tableam_tops_free_tuple(rows[location]);
                rows[location] = (HeapTuple)tableam_tops_form_tuple(onerel->rd_att, values, nulls);

                ItemPointerSet(&(rows[location])->t_self, targblock, targoffset + 1);
            }

            for (int col1 = 0; col1 < num_attnums; ++col1) {
                if (IsValidCacheSlotID(slotIdList[col1]))
                    CUCache->UnPinDataBlock(slotIdList[col1]);
            }
        } else {
            /* form tuple with temp memory */
            AssertEreport(end_col == num_attnums - 1, MOD_OPT, "");
            forthree(cell1, sampleTupleInfo, cell2, valueLocation, cell3, targoffsetList)
            {
                st_cell = (sample_tuple_cell*)lfirst(cell1);
                location = lfirst_int(cell2);
                targoffset = lfirst_int(cell3);

                tableam_tops_free_tuple(rows[location]);
                rows[location] = (HeapTuple)tableam_tops_form_tuple(onerel->rd_att, st_cell->values, st_cell->nulls);

                ItemPointerSet(&(rows[location])->t_self, targblock, targoffset + 1);
            }

            list_free_ext(valueLocation);
            list_free_ext(targoffsetList);
        }

        MemoryContextReset(sample_context);
    }

    /* Step 4: Clear CU memory */
    pfree_ext(nullValues);
    pfree_ext(constValues);
    pfree_ext(nulls);
    pfree_ext(values);
    pfree_ext(attrSeq);
    pfree_ext(colIdx);
    pfree_ext(funcIdx);
    pfree_ext(cuPtr);
    pfree_ext(slotIdList);
    pfree_ext(getValFuncPtr);
    list_free_ext(sampleTupleInfo);
    pfree_ext(bms_attnums);

    MemoryContextDelete(sample_context);

    ADIO_RUN()
    {
        pfree_ext(anlprefetch.fetchlist1.blocklist);
        pfree_ext(anlprefetch.fetchlist2.blocklist);
    }
    ADIO_END();

    /* Step 5: close delta relation and end of cstore scan */
    relation_close(deltarel, AccessShareLock);
    CStoreEndScan(cstoreScanDesc);

    if (estimate_table_rownum) {
        /* estimate totalrow num in non-pretty mode */
        AssertEreport(!SUPPORT_PRETTY_ANALYZE, MOD_OPT, "");

        if (totalblocks == 0 || (totalblocks > 0 && liverows > 0)) {
            *totalrows = liverows * totalblocks + deltarows;
        } else {
            /* All of block for the target rows of 300 are dead rows. */
            *totalrows = INVALID_ESTTOTALROWS + targrows;
        }

        ereport(elevel,
            (errmsg("ANALYZE INFO : \"%s\": scanned %u of %u cus, target cuid: %u, "
                    "containing %ld live rows, %ld dead rows, %.0f estimated total rows",
                RelationGetRelationName(onerel),
                estBlkNum,
                totalblocks,
                targblock,
                liverows,
                deadrows,
                *totalrows)));

        return numrows + delta_numrows;
    }

    /*
     * Step 6: sort rows and estimate reltuples
     *
     * If we didn't find as many tuples as we wanted then we're done. No sort
     * is needed, since they're already in order.
     *
     * Otherwise we need to sort the collected tuples by position
     * (itempointer). It's not worth worrying about corner cases where the
     * tuples are already sorted.
     */
    if (numrows == targrows)
        qsort((void*)rows, numrows, sizeof(HeapTuple), compare_rows);

    /* Emit some interesting relation info, only in datanode */
    if (IS_PGXC_DATANODE) {
        ereport(elevel,
            (errmsg("ANALYZE INFO : \"%s\": scanned %d of %u cus, sample %ld rows, estimated total %.0f rows",
                RelationGetRelationName(onerel),
                bs.m,
                totalblocks,
                numrows,
                *totalrows)));
    }

    return numrows + delta_numrows;
}

/* Select a random value R uniformly distributed in (0 - 1) */
double anl_random_fract(void)
{
    return ((double)random() + 1) / ((double)MAX_RANDOM_VALUE + 2);
}

/*
 * These two routines embody Algorithm Z from "Random sampling with a
 * reservoir" by Jeffrey S. Vitter, in ACM Trans. Math. Softw. 11, 1
 * (Mar. 1985), Pages 37-57.  Vitter describes his algorithm in terms
 * of the count S of records to skip before processing another record.
 * It is computed primarily based on t, the number of records already read.
 * The only extra state needed between calls is W, a random state variable.
 *
 * anl_init_selection_state computes the initial W value.
 *
 * Given that we've already read t records (t >= n), anl_get_next_S
 * determines the number of records to skip before the next record is
 * processed.
 */
double anl_init_selection_state(int n)
{
    if (unlikely(n == 0)) {
        ereport(ERROR, 
            (errcode(ERRCODE_DIVISION_BY_ZERO), 
                errmsg("records n should not be zero")));
    }
    /* Initial value of W (for use when Algorithm Z is first applied) */
    return exp(-log(anl_random_fract()) / n);
}

double anl_get_next_S(double t, int n, double* stateptr)
{
    double S;

    /* The magic constant here is T from Vitter's paper */
    if (t <= (22.0 * n)) {
        /* Process records using Algorithm X until t is large enough */
        double V, quot;

        V = anl_random_fract(); /* Generate V */
        S = 0;
        t += 1;
        /* Note: "num" in Vitter's code is always equal to t - n */
        quot = (t - (double)n) / t;
        /* Find min S satisfying (4.1) */
        while (quot > V) {
            S += 1;
            t += 1;
            quot *= (t - (double)n) / t;
        }
    } else {
        /* Now apply Algorithm Z */
        double W = *stateptr;
        double term = t - (double)n + 1;

        for (;;) {
            double numer, numer_lim, denom;
            double U, X, lhs, rhs, y, tmp;

            /* Generate U and X */
            U = anl_random_fract();
            X = t * (W - 1.0);
            S = floor(X); /* S is tentatively set to floor(X) */
            /* Test if U <= h(S)/cg(X) in the manner of (6.3) */
            tmp = (t + 1) / term;
            lhs = exp(log(((U * tmp * tmp) * (term + S)) / (t + X)) / n);
            rhs = (((t + X) / (term + S)) * term) / t;
            if (lhs <= rhs) {
                W = rhs / lhs;
                break;
            }
            /* Test if U <= f(S)/cg(X) */
            y = (((U * (t + 1)) / term) * (t + S + 1)) / (t + X);
            if ((double)n < S) {
                denom = t;
                numer_lim = term + S;
            } else {
                denom = t - (double)n + S;
                numer_lim = t + 1;
            }
            for (numer = t + S; numer >= numer_lim; numer -= 1) {
                y *= numer / denom;
                denom -= 1;
            }
            W = exp(-log(anl_random_fract()) / n); /* Generate W in advance */
            if (exp(log(y) / n) <= (t + X) / t)
                break;
        }
        *stateptr = W;
    }
    return S;
}

/*
 * qsort comparator for sorting rows[] array
 */
int compare_rows(const void* a, const void* b)
{
    HeapTuple ha = *(const HeapTuple*)a;
    HeapTuple hb = *(const HeapTuple*)b;
    BlockNumber ba = ItemPointerGetBlockNumber(&ha->t_self);
    OffsetNumber oa = ItemPointerGetOffsetNumber(&ha->t_self);
    BlockNumber bb = ItemPointerGetBlockNumber(&hb->t_self);
    OffsetNumber ob = ItemPointerGetOffsetNumber(&hb->t_self);

    if (ba < bb)
        return -1;
    if (ba > bb)
        return 1;
    if (oa < ob)
        return -1;
    if (oa > ob)
        return 1;
    return 0;
}

/*
 * acquire_inherited_sample_rows -- acquire sample rows from inheritance tree
 *
 * This has the same API as acquire_sample_rows, except that rows are
 * collected from all inheritance children as well as the specified table.
 * We fail and return zero if there are no inheritance children.
 */
template <bool estimate_table_rownum>
static int64 acquire_inherited_sample_rows(
    Relation onerel, int elevel, HeapTuple* rows, int64 targrows, double* totalrows, double* totaldeadrows)
{
    List* tableOIDs = NIL;
    Relation* rels = NULL;
    double* relblocks = NULL;
    double totalblocks = 0;
    int64 numrows = 0;
    int64 nrels = 0;
    int64 i = 0;
    ListCell* lc = NULL;

    /*
     * Find all members of inheritance set.  We only need AccessShareLock on
     * the children.
     */
    tableOIDs = find_all_inheritors(RelationGetRelid(onerel), AccessShareLock, NULL);

    /*
     * Check that there's at least one descendant, else fail.  This could
     * happen despite analyze_rel's relhassubclass check, if table once had a
     * child but no longer does.  In that case, we can clear the
     * relhassubclass field so as not to make the same mistake again later.
     * (This is safe because we hold ShareUpdateExclusiveLock.)
     */
    if (list_length(tableOIDs) < 2) {
        /* CCI because we already updated the pg_class row in this command */
        CommandCounterIncrement();
        SetRelationHasSubclass(RelationGetRelid(onerel), false);
        return 0;
    }

    /*
     * Count the blocks in all the relations.  The result could overflow
     * BlockNumber, so we use double arithmetic.
     */
    rels = (Relation*)palloc(list_length(tableOIDs) * sizeof(Relation));
    relblocks = (double*)palloc(list_length(tableOIDs) * sizeof(double));
    totalblocks = 0;
    nrels = 0;
    foreach (lc, tableOIDs) {
        Oid childOID = lfirst_oid(lc);
        Relation childrel;

        /* We already got the needed lock */
        childrel = heap_open(childOID, NoLock);

        /* Ignore if temp table of another backend */
        if (RELATION_IS_OTHER_TEMP(childrel)) {
            /* ... but release the lock on it */
            AssertEreport(childrel != onerel, MOD_OPT, "");
            heap_close(childrel, AccessShareLock);
            continue;
        }

        rels[nrels] = childrel;
        relblocks[nrels] = (double)RelationGetNumberOfBlocks(childrel);
        totalblocks += relblocks[nrels];
        nrels++;
    }

    /*
     * Now sample rows from each relation, proportionally to its fraction of
     * the total block count.  (This might be less than desirable if the child
     * rels have radically different free-space percentages, but it's not
     * clear that it's worth working harder.)
     */
    numrows = 0;
    *totalrows = 0;
    *totaldeadrows = 0;
    for (i = 0; i < nrels; i++) {
        Relation childrel = rels[i];
        double childblocks = relblocks[i];

        if (childblocks > 0) {
            int64 childtargrows;

            childtargrows = (int)rint(targrows * childblocks / totalblocks);
            /* Make sure we don't overrun due to roundoff error */
            childtargrows = Min(childtargrows, targrows - numrows);
            if (childtargrows > 0) {
                int64 childrows;
                double trows, tdrows;

                /* Fetch a random sample of the child's rows */
                childrows = acquire_sample_rows<estimate_table_rownum>(
                    childrel, elevel, rows + numrows, childtargrows, &trows, &tdrows);

                /* We may need to convert from child's rowtype to parent's */
                if (childrows > 0 && !equalTupleDescs(RelationGetDescr(childrel), RelationGetDescr(onerel))) {
                    TupleConversionMap* map = NULL;
                    map = convert_tuples_by_name(RelationGetDescr(childrel),
                        RelationGetDescr(onerel),
                        gettext_noop("could not convert row type"));
                    if (map != NULL) {
                        int64 j;

                        for (j = 0; j < childrows; j++) {
                            HeapTuple newtup;

                            newtup = do_convert_tuple(rows[numrows + j], map);
                            tableam_tops_free_tuple(rows[numrows + j]);
                            rows[numrows + j] = newtup;
                        }
                        free_conversion_map(map);
                    }
                }

                /* And add to counts */
                numrows += childrows;
                *totalrows += trows;
                *totaldeadrows += tdrows;
            }
        }

        /*
         * Note: we cannot release the child-table locks, since we may have
         * pointers to their TOAST tables in the sampled rows.
         */
        heap_close(childrel, NoLock);
    }

    return numrows;
}

void PartitionGePartRelationList(Relation onerel, Relation partRel, List** partList)
{
    if (RelationIsSubPartitioned(onerel)) {
        List *subPartList = relationGetPartitionList(partRel, NoLock);
        ListCell *lc = NULL;
        foreach (lc, subPartList) {
            Partition subPart = (Partition)lfirst(lc);
            Relation subPartRel = partitionGetRelation(partRel, subPart);
            *partList = lappend(*partList, subPartRel);
        }

        releasePartitionList(partRel, &subPartList, NoLock);
        releaseDummyRelation(&partRel);
    } else {
        *partList = lappend(*partList, partRel);
    }
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: acquire sample rows from partitions for a special partitioned
 *			: table
 * Description	: This has the same API as acquire_sample_rows, except that
 *			: rows are collected from all partitions.
 * Notes		:
 */
template <bool estimate_table_rownum>
static int64 acquirePartitionedSampleRows(Relation onerel, VacuumStmt* vacstmt, int elevel, HeapTuple* rows,
    int64 targrows, double* totalrows, double* totaldeadrows, VacAttrStats** vacattrstats, int attrAnalyzeNum)
{
    List* partList = NIL;
    double* partBlocks = NULL;
    double totalBlocks = 0;
    int64 numRows = 0;
    int nParts = 0;
    ListCell* partCell = NULL;
    int counter = 0;
    Partition part = NULL;
    Relation partRel = NULL;
    MemoryContext old_context = NULL;
    MemoryContext col_partition_analyze_context = NULL;

#ifdef ENABLE_MULTIPLE_NODES
    if (RelationIsTsStore(onerel)) {
        *totalrows = get_total_row_count(onerel);
        return numRows;
    }
#endif   /* ENABLE_MULTIPLE_NODES */

    if (RELATION_OWN_BUCKET(onerel)) {
        /*
         * for table with hash buckets, we pretend each bucket
         * as a parition and flatten all buckets into partList
         */
        if (RelationIsPartitioned(onerel)) {
            ListCell* partCell = NULL;

            /*
             * for each range partion get the underlying bucket
             * and save it in the flattened rpatList
             */
            foreach (partCell, vacstmt->partList) {
                List* partbuckets = NIL;

                Partition temp_part = (Partition)lfirst(partCell);
                if (RelationIsSubPartitioned(onerel)) {
                    partRel = partitionGetRelation(onerel, temp_part);
                    List *subPartList = relationGetPartitionList(partRel, NoLock);
                    ListCell *lc = NULL;
                    foreach (lc, subPartList) {
                        Partition subPart = (Partition)lfirst(lc);
                        partbuckets = relationGetBucketRelList(partRel, subPart);
                        partList = list_concat(partList, partbuckets);
                    }

                    releasePartitionList(partRel, &subPartList, NoLock);
                    releaseDummyRelation(&partRel);
                } else {
                    partbuckets = relationGetBucketRelList(onerel, temp_part);
                    partList = list_concat(partList, partbuckets);
                }
            }
        } else {
            /*
             * for non-range parition table, only need to
             * get the underlying hash bucket of the main table
             */
            partList = relationGetBucketRelList(onerel, NULL);
        }
    } else {
        foreach (partCell, vacstmt->partList) {

            part = (Partition)lfirst(partCell);
            partRel = partitionGetRelation(onerel, part);
            PartitionGePartRelationList(onerel, partRel, &partList);
        }
    }
    /*
     * Count the blocks in all the partitions.  The result could overflow
     * BlockNumber, so we use double arithmetic.
     */
    partBlocks = (double*)palloc(list_length(partList) * sizeof(double));
    totalBlocks = 0;
    nParts = 0;

    if (RelationIsColStore(onerel)) {
        /*
         * If there are many partitions of column store table, cstorescan will palloc
         * memory for VectorBatch, so we should create new memcontext for cstorescan,
         * and reset for each partition.
         */
        col_partition_analyze_context = AllocSetContextCreate(u_sess->analyze_cxt.analyze_context,
            "ColPartitionAnalyze",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);

        old_context = MemoryContextSwitchTo(col_partition_analyze_context);
    }

    foreach (partCell, partList) {
        partRel = (Relation)lfirst(partCell);

        /*
         * We already got the needed lock
         */
        if (!RelationIsColStore(onerel))
            partBlocks[nParts] = (double)RelationGetNumberOfBlocks(partRel);
        else {
            Relation deltaRel = relation_open(partRel->rd_rel->reldeltarelid, AccessShareLock);
            int16* colIdx = (int16*)palloc(sizeof(int16) * attrAnalyzeNum);
            for (int i = 0; i < attrAnalyzeNum; ++i)
                colIdx[i] = onerel->rd_att->attrs[vacattrstats[i]->tupattnum - 1].attnum;

            CStoreScanDesc cstoreScanDesc = CStoreBeginScan(partRel, attrAnalyzeNum, colIdx, NULL, false);

            partBlocks[nParts] = CStoreRelGetCUNumByNow(cstoreScanDesc);
            CStoreEndScan(cstoreScanDesc);

            /* also consider delta block */
            partBlocks[nParts] += (double)RelationGetNumberOfBlocks(deltaRel);
            relation_close(deltaRel, AccessShareLock);

            MemoryContextReset(col_partition_analyze_context);
        }

        totalBlocks += partBlocks[nParts];
        nParts++;
    }

    if (RelationIsColStore(onerel)) {
        (void)MemoryContextSwitchTo(old_context);
        MemoryContextDelete(col_partition_analyze_context);
    }

    /*
     * Now sample rows from each partition, proportionally to its fraction of
     * the total block count.  (This might be less than desirable if the
     * partition have radically different free-space percentages, but it's not
     * clear that it's worth working harder.)
     */
    *totalrows = 0;
    *totaldeadrows = 0;

    partCell = list_head(partList);
    for (counter = 0; counter < nParts; counter++) {
        double partBlock = 0;

        partBlock = partBlocks[counter];
        partRel = (Relation)lfirst(partCell);

        if (partBlock > 0) {
            int64 parttargrows = (int64)ceil(targrows * partBlock / totalBlocks);

            /*
             * Make sure we don't overrun due to roundoff error
             */
            parttargrows = Min(parttargrows, targrows - numRows);
            AssertEreport(parttargrows >= 0, MOD_OPT, "");

            if (parttargrows > 0) {
                int64 partrows = 0;
                double trows = 0;
                double tdrows = 0;

                /* Fetch a random sample of the partition's rows */
                if (!RelationIsColStore(onerel))
                    partrows = acquire_sample_rows<estimate_table_rownum>(
                        partRel, elevel, rows + numRows, parttargrows, &trows, &tdrows);
                else
                    partrows = AcquireSampleCStoreRows<estimate_table_rownum>(
                        partRel, elevel, rows + numRows, parttargrows, &trows, &tdrows, vacattrstats, attrAnalyzeNum);

                /* And add to counts */
                numRows += partrows;
                *totalrows += trows;
                *totaldeadrows += tdrows;

                /*
                 * Remember to report sampled rows to stats collector. Otherwise, stats
                 * collector will not set new state for this partition and the partition
                 * ill always in a "need to be analyzed" state.
                 */
                if (!estimate_table_rownum)
                    pgstat_report_analyze(partRel, trows, tdrows);
            }
        }

        partCell = lnext(partCell);
    }

    /*
     * clean up
     */
    pfree_ext(partBlocks);

    foreach (partCell, partList) {
        partRel = (Relation)lfirst(partCell);
        releaseDummyRelation(&partRel);
    }

    list_free_ext(partList);

    return numRows;
}

void releaseSourceAfterDelteOrUpdateAttStats(
    ResourceOwner asOwner, ResourceOwner oldOwner1, Relation pgstat, Relation pgstat_ext)
{
    if (pgstat != NULL) {
        heap_close(pgstat, RowExclusiveLock);
    }

    if (pgstat_ext != NULL) {
        heap_close(pgstat_ext, RowExclusiveLock);
    }

    ResourceOwnerRelease(asOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
    ResourceOwnerRelease(asOwner, RESOURCE_RELEASE_LOCKS, false, false);
    ResourceOwnerRelease(asOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, false);
    t_thrd.utils_cxt.CurrentResourceOwner = oldOwner1;
    ResourceOwnerDelete(asOwner);
}

/*
 *	update_attstats() -- update attribute statistics for one relation
 *
 *		Statistics are stored in several places: the pg_class row for the
 *		relation has stats about the whole relation, and there is a
 *		pg_statistic row for each (non-system) attribute that has ever
 *		been analyzed.	The pg_class values are updated by VACUUM, not here.
 *
 *		pg_statistic rows are just added or updated normally.  This means
 *		that pg_statistic will probably contain some deleted rows at the
 *		completion of a vacuum cycle, unless it happens to get vacuumed last.
 *
 *		To keep things simple, we punt for pg_statistic, and don't try
 *		to compute or store rows for pg_statistic itself in pg_statistic.
 *		This could possibly be made to work, but it's not worth the trouble.
 *		Note analyze_rel() has seen to it that we won't come here when
 *		vacuuming pg_statistic itself.
 *
 *		Note: there would be a race condition here if two backends could
 *		ANALYZE the same table concurrently.  Presently, we lock that out
 *		by taking a self-exclusive lock on the relation in analyze_rel().
 *
 * Note: both single-column and extended statistics are process/inserted in this
 *       function
 */
// Added parameter - char relkind by data partition
void update_attstats(Oid relid, char relkind, bool inh, int natts, VacAttrStats** vacattrstats, char relpersistence)
{
    Relation pgstat = NULL;
    Relation pgstat_ext = NULL;
    int attno;
    ResourceOwner asOwner, oldOwner1;
    MemoryContext oldcontext = CurrentMemoryContext;

    if (natts <= 0) {
        return; /* nothing to do */
    }

    /*
     * Create a resource owner to keep track of resources
     * in order to release resources when catch the exception.
     */
    asOwner = ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner, "update_stats",
                                  THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    oldOwner1 = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = asOwner;

    for (attno = 0; attno < natts; attno++) {
        VacAttrStats* stats = vacattrstats[attno];

        /* Ignore attr if we weren't able to collect stats */
        if (!stats->stats_valid)
            continue;

        if (stats->num_attrs > 1) {
            /* Open rel handler for pg_statistic_ext to process multi-column statistic */
            if (!pgstat_ext) {
                pgstat_ext = heap_open(StatisticExtRelationId, RowExclusiveLock);
            }

            /* do multi column stats update */
            update_stats_catalog<false>(pgstat_ext, oldcontext, relid, relkind, inh, stats, natts, relpersistence);
        } else {
            /* Open rel handler for pg_statistic to process single-column statistic */
            if (!pgstat) {
                pgstat = heap_open(StatisticRelationId, RowExclusiveLock);
            }

            /* do signle column stats update */
            update_stats_catalog<true>(pgstat, oldcontext, relid, relkind, inh, stats, natts, relpersistence);
        }
    }

    releaseSourceAfterDelteOrUpdateAttStats(asOwner, oldOwner1, pgstat, pgstat_ext);
}

static ArrayType* es_construct_extended_statistics(VacAttrStats* stats, int k)
{
    ArrayType *array = NULL;
    if (STATISTIC_KIND_MCV == stats->stakind[k] || STATISTIC_KIND_NULL_MCV == stats->stakind[k]) {
        /* construct MCV/NULL-MCV for extended statistics */
        array = es_construct_mcv_value_array(stats, k);
#ifndef ENABLE_MULTIPLE_NODES
    } else if (STATISTIC_EXT_DEPENDENCIES == stats->stakind[k]) {
        /* construct functional dependency for extended statistics */
        array = es_construct_dependency_value_array(stats, k);
#endif
    } else {
        array = NULL;
    }
    return array;
}

/*
 * update_single_column_attrstats --- insert a single-column statistic entry into pg_statistic
 *
 * @param(in) pgstat
 *     relation handler for pg_statistic
 * @param(in) oldcontext
 *     memory context for caller function
 * @param(in) relid
 *     relation oid for being analyzed relation
 * @param(in) relkind
 *     relation kind for being analyzed relation
 * @param(in) inh
 *     relation's inh value for being anlyzed relation
 * @param(in) stats
 *     column-level statistic that will be insert into pg_statistic
 *
 * @return: void
 */
template <bool isSingleColumn>
static void update_stats_catalog(
    Relation pgstat, MemoryContext oldcontext, Oid relid, char relkind, bool inh, VacAttrStats* stats, int natts,
    char relpersistence)
{
    HeapTuple stup, oldtup;
    int i, k, n;

    Assert(Natts_pg_statistic >= Natts_pg_statistic_ext);

    Datum values[Natts_pg_statistic];
    bool nulls[Natts_pg_statistic];
    bool replaces[Natts_pg_statistic];

    /*
     * Construct a new pg_statistic(_ext) tuple
     */
    for (i = 0; i < Natts_pg_statistic; ++i) {
        nulls[i] = false;
        replaces[i] = true;
    }

    AttrNumber attnum = 0;

    if (isSingleColumn) {
        attnum = stats->attrs[0]->attnum;

        values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(relid);
        values[Anum_pg_statistic_starelkind - 1] = CharGetDatum(relkind);
        values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(attnum);
        values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(inh);
        values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(stats->stanullfrac);
        values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(stats->stawidth);
        values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(stats->stadistinct);
        values[Anum_pg_statistic_stadndistinct - 1] = Float4GetDatum(stats->stadndistinct);

        /* For single column statistics we keep staextinfo to NULL */
        values[Anum_pg_statistic_staextinfo - 1] = 0;
        nulls[Anum_pg_statistic_staextinfo - 1] = true;
    } else {
        values[Anum_pg_statistic_ext_starelid - 1] = ObjectIdGetDatum(relid);
        values[Anum_pg_statistic_ext_starelkind - 1] = CharGetDatum(relkind);
        values[Anum_pg_statistic_ext_stainherit - 1] = BoolGetDatum(inh);
        values[Anum_pg_statistic_ext_stanullfrac - 1] = Float4GetDatum(stats->stanullfrac);
        values[Anum_pg_statistic_ext_stawidth - 1] = Int32GetDatum(stats->stawidth);
        values[Anum_pg_statistic_ext_stadistinct - 1] = Float4GetDatum(stats->stadistinct);
        values[Anum_pg_statistic_ext_stadndistinct - 1] = Float4GetDatum(stats->stadndistinct);

        /* not used, just kept to further extension for expr-statistic */
        values[Anum_pg_statistic_ext_staexprs - 1] = (Datum)0;
        nulls[Anum_pg_statistic_ext_staexprs - 1] = true;
    }

    Assert(Anum_pg_statistic_stakind1 == Anum_pg_statistic_ext_stakind1);

    /* Process stakind1 -> stakind5 */
    i = Anum_pg_statistic_stakind1 - 1;
    for (k = 0; k < STATISTIC_NUM_SLOTS; k++) {
        values[i++] = Int16GetDatum(stats->stakind[k]); /* stakindN */
    }

    Assert(Anum_pg_statistic_staop1 == Anum_pg_statistic_ext_staop1);

    /* Process staop1 -> staop5 */
    i = Anum_pg_statistic_staop1 - 1;
    for (k = 0; k < STATISTIC_NUM_SLOTS; k++) {
        values[i++] = ObjectIdGetDatum(stats->staop[k]); /* staopN */
    }

    /* Process stanumbers1 -> stanumber5 */
    if (isSingleColumn) {
        i = Anum_pg_statistic_stanumbers1 - 1;
    } else {
        i = Anum_pg_statistic_ext_stanumbers1 - 1;
    }

    for (k = 0; k < STATISTIC_NUM_SLOTS; k++) {
        int nnum = stats->numnumbers[k];

        if (nnum > 0) {
            Datum* numdatums = (Datum*)palloc(nnum * sizeof(Datum));
            ArrayType* arry = NULL;

            for (n = 0; n < nnum; n++)
                numdatums[n] = Float4GetDatum(stats->stanumbers[k][n]);
            /* XXX knows more than it should about type float4: */
            arry = construct_array(numdatums, nnum, FLOAT4OID, sizeof(float4), FLOAT4PASSBYVAL, 'i');
            values[i++] = PointerGetDatum(arry); /* stanumbersN */
        } else {
            nulls[i] = true;
            values[i++] = (Datum)0;
        }
    }

    /* Process stavalues1 -> stavalues5 */
    if (isSingleColumn) {
        i = Anum_pg_statistic_stavalues1 - 1;
    } else {
        i = Anum_pg_statistic_ext_stavalues1 - 1;
    }
    for (k = 0; k < STATISTIC_NUM_SLOTS; k++) {
        if (stats->numvalues[k] > 0) {
            if (isSingleColumn) {
                ArrayType* array = construct_array(stats->stavalues[k],
                    stats->numvalues[k],
                    stats->statypid[k],
                    stats->statyplen[k],
                    stats->statypbyval[k],
                    stats->statypalign[k]);
                values[i++] = PointerGetDatum(array); /* stavaluesN */
            } else {
                ArrayType* array = es_construct_extended_statistics(stats, k);
                if (array) {
                    values[i++] = PointerGetDatum(array);
                } else {
                    nulls[i] = true;
                    values[i++] = (Datum)0;
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                            errmsg("Multi-columns statistic does not support hist/corr/mcelem/dechist")));
                }
            }

        } else {
            nulls[i] = true;
            values[i++] = (Datum)0;
        }
    }

    int2vector* stakey = NULL;

    if (isSingleColumn == false) {
        Assert(stats->num_attrs > 1);
        /* compute stakey value */
        stakey = buildint2vector(NULL, stats->num_attrs);

        for (unsigned i = 0; i < stats->num_attrs; i++) {
            stakey->values[i] = stats->attrs[i]->attnum;
        }

        values[Anum_pg_statistic_ext_stakey - 1] = PointerGetDatum(stakey);
    }

    /* Update column statistic to localhash, not catalog */
    if (relpersistence == RELPERSISTENCE_GLOBAL_TEMP) {
        up_gtt_att_statistic(relid,
                             attnum,
                             natts,
                             RelationGetDescr(pgstat),
                             values,
                             nulls);
        return;
    }

    /* store tuple to pg_statistic(_ext) */
    PG_TRY();
    {
        /* Is there already a pg_statistic(_ext) tuple for this attribute? */
        if (isSingleColumn) {
            oldtup = SearchSysCache4(STATRELKINDATTINH,
                ObjectIdGetDatum(relid),
                CharGetDatum(relkind),
                Int16GetDatum(attnum),
                BoolGetDatum(inh));
        } else {
            oldtup = SearchSysCache4(STATRELKINDKEYINH,
                ObjectIdGetDatum(relid),
                CharGetDatum(relkind),
                BoolGetDatum(inh),
                values[Anum_pg_statistic_ext_stakey - 1]);
        }

        if (HeapTupleIsValid(oldtup)) {
            /* Yes, replace it */
            stup = heap_modify_tuple(oldtup, RelationGetDescr(pgstat), values, nulls, replaces);
            ReleaseSysCache(oldtup);
            simple_heap_update(pgstat, &stup->t_self, stup);
        } else {
            /* No, insert new tuple */
            stup = heap_form_tuple(RelationGetDescr(pgstat), values, nulls);
            (void)simple_heap_insert(pgstat, stup);
        }

        /* update indexes too */
        CatalogUpdateIndexes(pgstat, stup);
    }
    PG_CATCH();
    {
        if (isSingleColumn) {
            analyze_concurrency_process(relid, attnum, oldcontext, PG_FUNCNAME_MACRO);
        } else {
            analyze_concurrency_process(relid, ES_MULTI_COLUMN_STATS_ATTNUM, oldcontext, PG_FUNCNAME_MACRO);
        }
    }
    PG_END_TRY();

    if (isSingleColumn == false) {
        pfree_ext(stakey);
    }

    tableam_tops_free_tuple(stup);
}

/*
 *	delete_attstats() -- delete attribute statistics for one relation if
 *					there has no reltuples now and there has reltuples before.
 * @in relid - the relation id which we will do analyze
 * @in relkind - 'c': starelid ref pg_class.oid; 'p': starelid ref pg_partition.oid
 * @in inh - true if inheritance children are included
 * @in natts - how many column the relation have
 * @in vacattrstats - the statistic info for all the attributes of the relation
 * @return: void
 */
void delete_attstats(
    Oid relid, char relkind, bool inh, int natts, VacAttrStats** vacattrstats, unsigned int delete_stats_option)
{
    int attno;
    ResourceOwner asOwner, oldOwner1;
    MemoryContext oldcontext = CurrentMemoryContext;
    Relation pgstat = NULL;
    Relation pgstat_ext = NULL;

    if (natts <= 0) {
        return; /* nothing to do */
    }

    /*
     * Create a resource owner to keep track of resources
     * in order to release resources when catch the exception.
     */
    asOwner = ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner, "delete_stats", 
                                  THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    oldOwner1 = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = asOwner;

    for (attno = 0; attno < natts; attno++) {
        VacAttrStats* stats = vacattrstats[attno];
        bool multicol_stats = false;

        /* Skip multi-column stats if not explicitly assigned */
        if ((!(delete_stats_option & DELETE_STATS_MULTI)) && (stats->num_attrs > 1)) {
            continue;
        }

        /* For extended statistics, we should set attnum to a special flag */
        int16 attnum = stats->attrs[0]->attnum;
        if (stats->num_attrs > 1) {
            attnum = ES_MULTI_COLUMN_STATS_ATTNUM;
            multicol_stats = true;
        }

        /* Is there already a pg_statistic tuple for this attribute? */
        HeapTuple oldtup = NULL;

        if (multicol_stats) {
            if (!pgstat_ext) {
                /* Open rel handler for pg_statistic_ext */
                pgstat_ext = heap_open(StatisticExtRelationId, RowExclusiveLock);
            }

            int2vector* stakey = NULL;
            stakey = buildint2vector(NULL, stats->num_attrs);

            for (unsigned i = 0; i < stats->num_attrs; i++) {
                stakey->values[i] = stats->attrs[i]->attnum;
            }

            oldtup = SearchSysCache4(STATRELKINDKEYINH,
                ObjectIdGetDatum(relid),
                CharGetDatum(relkind),
                BoolGetDatum(inh),
                PointerGetDatum(stakey)); /* multi column statistics */

            if (stats->num_attrs > 0) {
                Bitmapset* bms_attnums = bms_make_singleton(stats->attrs[0]->attnum);
                for (unsigned int i = 1; i < stats->num_attrs; i++) {
                    bms_attnums = bms_add_member(bms_attnums, stats->attrs[i]->attnum);
                }
                const char* model_name_base = "BAYESNET_MODEL_STATS";
                StringInfoData model_name;
                initStringInfo(&model_name);
                appendStringInfo(&model_name, "%s_%d_%d", model_name_base, relid, bms_hash_value(bms_attnums));
                remove_model_from_cache_and_disk(model_name.data);
                pfree_ext(model_name.data);
            }

            pfree_ext(stakey);
        } else {
            if (!pgstat) {
                /* Open rel handler for pg_statistic */
                pgstat = heap_open(StatisticRelationId, RowExclusiveLock);
            }

            oldtup = SearchSysCache4(STATRELKINDATTINH,
                ObjectIdGetDatum(relid),
                CharGetDatum(relkind),
                Int16GetDatum(attnum), /* single column statistics */
                BoolGetDatum(inh));
        }

        /* Remove the tuple if we found it */
        if (HeapTupleIsValid(oldtup)) {
            Relation sd = NULL;

            if (multicol_stats) {
                sd = pgstat_ext;
            } else {
                /* Open rel handler for pg_statistic */
                if (!pgstat) {
                    pgstat = heap_open(StatisticRelationId, RowExclusiveLock);
                }

                sd = pgstat;
            }

            PG_TRY();
            {
                simple_heap_delete(sd, &oldtup->t_self);
            }
            PG_CATCH();
            {
                analyze_concurrency_process(relid, attnum, oldcontext, PG_FUNCNAME_MACRO);
            }
            PG_END_TRY();

            ReleaseSysCache(oldtup);
        }
    }

    releaseSourceAfterDelteOrUpdateAttStats(asOwner, oldOwner1, pgstat, pgstat_ext);
}

/*
 *	delete_attstats_replication() -- delete attribute statistics for one replication if
 *					there has no reltuples now and there has reltuples before.
 * @in relid - the relation id which we will do analyze
 * @in stmt - the statment for analyze or vacuum command
 * @return: void
 */
void delete_attstats_replication(Oid relid, VacuumStmt* stmt)
{
    int attr_cnt = 0;
    int nindexes = 0;
    AnlIndexData* indexdata = NULL;
    bool hasindex = false;
    VacAttrStats** vacattrstats = NULL;
    Relation* Irel = NULL;
    Relation rel = NULL;

    /* Get real tuple size using to caculate workmem_allow_rows. */
    rel = relation_open(relid, AccessShareLock);
    vacattrstats = get_vacattrstats_by_vacstmt(rel, stmt, &attr_cnt, &nindexes, &indexdata, &hasindex, false, &Irel);

    /*
     * Emit the completed stats rows into pg_statistic, deleting any
     * previous statistics for the target columns.
     */
    delete_attstats(relid, STARELKIND_CLASS, false, attr_cnt, vacattrstats);

    /*
     * we should delete statistic of complex table
     * if statistic of hdfs has deleted and all the table's totalrows are 0.
     */
    if ((stmt->pstGlobalStatEx[ANALYZEDELTA - 1].totalRowCnts) == 0 &&
        (rel->rd_rel->relhassubclass || RelationIsPAXFormat(rel)))
        delete_attstats(relid, STARELKIND_CLASS, true, attr_cnt, vacattrstats);

    /* Relate to delete index statistic. */
    for (int i = 0; i < nindexes; i++) {
        AnlIndexData* thisdata = &indexdata[i];

        delete_attstats(RelationGetRelid(Irel[i]), STARELKIND_CLASS, false, thisdata->attr_cnt, thisdata->vacattrstats);
    }

    vac_close_indexes(nindexes, Irel, NoLock);

    relation_close(rel, AccessShareLock);
}

/*
 * Standard fetch function for use by compute_stats subroutines.
 *
 * This exists to provide some insulation between compute_stats routines
 * and the actual storage of the sample data.
 */
static Datum std_fetch_func(VacAttrStatsP stats, int rownum, bool* isNull, Relation rel)
{
    int attnum = stats->tupattnum;
    HeapTuple tuple = stats->rows[rownum];
    TupleDesc tupDesc = rel->rd_att;

    return tableam_tops_tuple_getattr(tuple, attnum, tupDesc, isNull);
}

/*
 * Fetch function for analyzing index expressions.
 *
 * We have not bothered to construct index tuples, instead the data is
 * just in Datum arrays.
 */
static Datum ind_fetch_func(VacAttrStatsP stats, int rownum, bool* isNull, Relation rel)
{
    int i;

    /* exprvals and exprnulls are already offset for proper column */
    i = rownum * stats->rowstride;
    *isNull = stats->exprnulls[i];
    return stats->exprvals[i];
}

/* ==========================================================================
 *
 * Code below this point represents the "standard" type-specific statistics
 * analysis algorithms.  This code can be replaced on a per-data-type basis
 * by setting a nonzero value in pg_type.typanalyze.
 *
 *==========================================================================
 */

/*
 * To avoid consuming too much memory during analysis and/or too much space
 * in the resulting pg_statistic rows, we ignore varlena datums that are wider
 * than WIDTH_THRESHOLD (after detoasting!).  This is legitimate for MCV
 * and distinct-value calculations since a wide value is unlikely to be
 * duplicated at all, much less be a most-common value.  For the same reason,
 * ignoring wide values will not affect our estimates of histogram bin
 * boundaries very much.
 */
#define WIDTH_THRESHOLD 1024

#define swapInt(a, b) \
    do {              \
        int _tmp;     \
        _tmp = a;     \
        a = b;        \
        b = _tmp;     \
    } while (0)
#define swapDatum(a, b) \
    do {                \
        Datum _tmp;     \
        _tmp = a;       \
        a = b;          \
        b = _tmp;       \
    } while (0)

/*
 * Extra information used by the default analysis routines
 */
typedef struct {
    Oid eqopr;  /* '=' operator for datatype, if any */
    Oid eqfunc; /* and associated function */
    Oid ltopr;  /* '<' operator for datatype, if any */
} StdAnalyzeData;

typedef struct {
    Datum value; /* a data value */
    int tupno;   /* position index for tuple it came from */
} ScalarItem;

typedef struct {
    int count; /* # of duplicates */
    int first; /* values[] index of first occurrence */
} ScalarMCVItem;

typedef struct {
    SortSupport ssup;
    int* tupnoLink;
} CompareScalarsContext;

static void compute_minimal_stats(
    VacAttrStatsP stats, AnalyzeAttrFetchFunc fetchfunc, int samplerows, double totalrows, Relation rel);
static void compute_scalar_stats(
    VacAttrStatsP stats, AnalyzeAttrFetchFunc fetchfunc, int samplerows, double totalrows, Relation rel);
static int compare_scalars(const void* a, const void* b, void* arg);
static int compare_mcvs(const void* a, const void* b);
static void analyze_compute_ndistinct(
    int64 nmultiple, double ndistinct, int64 toowide_cnt, int64 samplerows, double totalrows, VacAttrStatsP stats);
static void analyze_compute_correlation(
    int64 values_cnt, double corr_xysum, double corrs, VacAttrStatsP stats, int slot_idx);
static void analyze_compute_mcv(
    int* slot_idx, const char* tableName, AnalyzeSampleTableSpecInfo* spec, bool process_null = false);

static double compute_mcv_mincount(int64 samplerows, double ndistinct, double num_bins);

/**
 * @Description: Compute target number of statistics datapoints to collect
 *				during VACUUM ANALYZE of this column according to default_statistics_target.
 *
 * @in attr -  a tuple with the format of pg_attribute relation.
 * @return -  int (target number of statistics)
 */
int compute_attr_target(Form_pg_attribute attr)
{
    int att_target = attr->attstattarget;

    /*
     * If the attstattarget column is -1, use the default value;
     * If  the attstattarget column is less -1 or  default_statistics_target is negtive, use the percent value.
     * NB: it is okay to scribble on stats->attr since it's a copy.
     */
    if (IS_PGXC_DATANODE) {
        if (attr->attstattarget < 0 && default_statistics_target > 0) {
            if (attr->attstattarget == -1)
                attr->attstattarget = default_statistics_target;
            att_target = default_statistics_target;
        } else if (attr->attstattarget < 0 && default_statistics_target <= 0)
            att_target = DEFAULT_ATTR_TARGET;
        else
            att_target = attr->attstattarget;
    } else if (attr->attstattarget < 0) {
        if (default_statistics_target < 0)
            attr->attstattarget = -default_statistics_target;
        else
            attr->attstattarget = default_statistics_target;

        /*
         * default_statistics_target < 0: We have give a specific mcv attstattarget is 100,
         * and histgram attstattarget is 301 for normal table. For text search type or index data,
         * attstattarget is -default_statistics_target may be few, so we should adjust it.
         */
        attr->attstattarget = Max(attr->attstattarget, DEFAULT_ATTR_TARGET);
        att_target = attr->attstattarget;
    }

    return att_target;
}

/*
 * std_typanalyze -- the default type-specific typanalyze function
 */
bool std_typanalyze(VacAttrStats* stats)
{
    Form_pg_attribute attr = stats->attrs[0];
    Oid ltopr;
    Oid eqopr;
    StdAnalyzeData* mystats = NULL;
    int att_target;

    att_target = compute_attr_target(attr);

    /* Look for default "<" and "=" operators for column's type */
    get_sort_group_operators(stats->attrtypid[0], false, false, false, &ltopr, &eqopr, NULL, NULL);

    /* If column has no "=" operator, we can't do much of anything */
    if (!OidIsValid(eqopr))
        return false;

    /* Save the operator info for compute_stats routines */
    mystats = (StdAnalyzeData*)palloc(sizeof(StdAnalyzeData));
    mystats->eqopr = eqopr;
    mystats->eqfunc = get_opcode(eqopr);
    mystats->ltopr = ltopr;
    stats->extra_data = mystats;

    /*
     * Determine which standard statistics algorithm to use
     */
    if (OidIsValid(ltopr)) {
        /* Seems to be a scalar datatype */
        stats->compute_stats = compute_scalar_stats;
        /* --------------------
         * The following choice of minrows is based on the paper
         * "Random sampling for histogram construction: how much is enough?"
         * by Surajit Chaudhuri, Rajeev Motwani and Vivek Narasayya, in
         * Proceedings of ACM SIGMOD International Conference on Management
         * of Data, 1998, Pages 436-447.  Their Corollary 1 to Theorem 5
         * says that for table size n, histogram size k, maximum relative
         * error in bin size f, and error probability gamma, the minimum
         * random sample size is
         *		r = 4 * k * ln(2*n/gamma) / f^2
         * Taking f = 0.5, gamma = 0.01, n = 10^6 rows, we obtain
         *		r = 305.82 * k
         * Note that because of the log function, the dependence on n is
         * quite weak; even at n = 10^12, a 300*k sample gives <= 0.66
         * bin size error with probability 0.99.  So there's no real need to
         * scale for n, which is a good thing because we don't necessarily
         * know it at this point.
         * --------------------
         */
        stats->minrows = DEFAULT_EST_TARGET_ROWS * att_target;
    } else {
        /* Can't do much but the minimal stuff */
        stats->compute_stats = compute_minimal_stats;
        /* Might as well use the same minrows as above */
        stats->minrows = DEFAULT_EST_TARGET_ROWS * att_target;
    }

    return true;
}

/*
 *	compute_minimal_stats() -- compute minimal column statistics
 *
 *	We use this when we can find only an "=" operator for the datatype.
 *
 *	We determine the fraction of non-null rows, the average width, the
 *	most common values, and the (estimated) number of distinct values.
 *
 *	The most common values are determined by brute force: we keep a list
 *	of previously seen values, ordered by number of times seen, as we scan
 *	the samples.  A newly seen value is inserted just after the last
 *	multiply-seen value, causing the bottommost (oldest) singly-seen value
 *	to drop off the list.  The accuracy of this method, and also its cost,
 *	depend mainly on the length of the list we are willing to keep.
 */
static void compute_minimal_stats(
    VacAttrStatsP stats, AnalyzeAttrFetchFunc fetchfunc, int samplerows, double totalrows, Relation rel)
{
    int i;
    int null_cnt = 0;
    int nonnull_cnt = 0;
    int toowide_cnt = 0;
    double total_width = 0;
    bool is_varlena = (!stats->attrtype[0]->typbyval && stats->attrtype[0]->typlen == -1);
    bool is_varwidth = (!stats->attrtype[0]->typbyval && stats->attrtype[0]->typlen < 0);
    FmgrInfo f_cmpeq;
    typedef struct {
        Datum value;
        int count;
    } TrackItem;
    TrackItem* track = NULL;
    int track_cnt, track_max;
    int num_mcv = stats->attrs[0]->attstattarget;
    StdAnalyzeData* mystats = (StdAnalyzeData*)stats->extra_data;

    /*
     * We track up to 2*n values for an n-element MCV list; but at least 10
     */
    track_max = 2 * num_mcv;
    if (track_max < 10) {
        track_max = 10;
    }
    track = (TrackItem*)palloc(track_max * sizeof(TrackItem));
    track_cnt = 0;

    fmgr_info(mystats->eqfunc, &f_cmpeq);

    for (i = 0; i < samplerows; i++) {
        Datum value;
        bool isnull = false;
        bool match = false;
        int firstcount1, j;

        vacuum_delay_point();

        value = fetchfunc(stats, i, &isnull, rel);

        /* Check for null/nonnull */
        if (isnull) {
            null_cnt++;
            continue;
        }
        nonnull_cnt++;

        /*
         * If it's a variable-width field, add up widths for average width
         * calculation.  Note that if the value is toasted, we use the toasted
         * width.  We don't bother with this calculation if it's a fixed-width
         * type.
         */
        if (is_varlena) {
            total_width += VARSIZE_ANY(DatumGetPointer(value));

            /*
             * If the value is toasted, we want to detoast it just once to
             * avoid repeated detoastings and resultant excess memory usage
             * during the comparisons.	Also, check to see if the value is
             * excessively wide, and if so don't detoast at all --- just
             * ignore the value.
             */
            if (toast_raw_datum_size(value) > WIDTH_THRESHOLD) {
                toowide_cnt++;
                continue;
            }
            value = PointerGetDatum(PG_DETOAST_DATUM(value));
        } else if (is_varwidth) {
            /* must be cstring */
            total_width += strlen(DatumGetCString(value)) + 1;
        }

        /*
         * See if the value matches anything we're already tracking.
         */
        match = false;
        firstcount1 = track_cnt;
        for (j = 0; j < track_cnt; j++) {
            /* We always use the default collation for statistics */
            if (DatumGetBool(FunctionCall2Coll(&f_cmpeq, DEFAULT_COLLATION_OID, value, track[j].value))) {
                match = true;
                break;
            }
            if (j < firstcount1 && track[j].count == 1) {
                firstcount1 = j;
            }
        }

        if (match) {
            /* Found a match */
            track[j].count++;
            /* This value may now need to "bubble up" in the track list */
            while (j > 0 && track[j].count > track[j - 1].count) {
                swapDatum(track[j].value, track[j - 1].value);
                swapInt(track[j].count, track[j - 1].count);
                j--;
            }
        } else {
            /* No match.  Insert at head of count-1 list */
            if (track_cnt < track_max) {
                track_cnt++;
            }
            for (j = track_cnt - 1; j > firstcount1; j--) {
                track[j].value = track[j - 1].value;
                track[j].count = track[j - 1].count;
            }
            if (firstcount1 < track_cnt) {
                track[firstcount1].value = value;
                track[firstcount1].count = 1;
            }
        }
    }

    /* We can only compute real stats if we found some non-null values. */
    if (nonnull_cnt > 0) {
        int nmultiple, summultiple;

        stats->stats_valid = true;
        /* Do the simple null-frac and width stats */
        stats->stanullfrac = (double)null_cnt / (double)samplerows;
        if (is_varwidth) {
            stats->stawidth = total_width / (double)nonnull_cnt;
        } else {
            stats->stawidth = stats->attrtype[0]->typlen;
        }

        /* Count the number of values we found multiple times */
        summultiple = 0;
        for (nmultiple = 0; nmultiple < track_cnt; nmultiple++) {
            if (track[nmultiple].count == 1) {
                break;
            }
            summultiple += track[nmultiple].count;
        }

        if (nmultiple == 0) {
            /*
             * If we found no repeated non-null values, assume it's a unique
             * column; but be sure to discount for any nulls we found.
             */
            stats->stadistinct = -1.0 * (1 - stats->stanullfrac);
        } else if (track_cnt < track_max && toowide_cnt == 0 && nmultiple == track_cnt) {
            /*
             * Our track list includes every value in the sample, and every
             * value appeared more than once.  Assume the column has just
             * these values.
             */
            stats->stadistinct = track_cnt;
        } else {
            /* ----------
             * Estimate the number of distinct values using the estimator
             * proposed by Haas and Stokes in IBM Research Report RJ 10025:
             *		n*d / (n - f1 + f1*n/N)
             * where f1 is the number of distinct values that occurred
             * exactly once in our sample of n rows (from a total of N),
             * and d is the total number of distinct values in the sample.
             * This is their Duj1 estimator; the other estimators they
             * recommend are considerably more complex, and are numerically
             * very unstable when n is much smaller than N.
             *
             * We assume (not very reliably!) that all the multiply-occurring
             * values are reflected in the final track[] list, and the other
             * nonnull values all appeared but once.  (XXX this usually
             * results in a drastic overestimate of ndistinct.	Can we do
             * any better?)
             * ----------
             */
            int f1 = nonnull_cnt - summultiple;
            int d = f1 + nmultiple;
            double numer, denom, stadistinct;

            numer = (double)samplerows * (double)d;

            denom = (double)(samplerows - f1) + (double)f1 * (double)samplerows / totalrows;

            stadistinct = numer / denom;
            /* Clamp to sane range in case of roundoff error */
            if (stadistinct < (double)d) {
                stadistinct = (double)d;
            }
            if (stadistinct > totalrows) {
                stadistinct = totalrows;
            }
            stats->stadistinct = floor(stadistinct + 0.5);
        }

        /*
         * If we estimated the number of distinct values at more than 10% of
         * the total row count (a very arbitrary limit), then assume that
         * stadistinct should scale with the row count rather than be a fixed
         * value.
         */
        if (stats->stadistinct > 0.1 * totalrows)
            stats->stadistinct = -(stats->stadistinct / totalrows);

        /*
         * Decide how many values are worth storing as most-common values. If
         * we are able to generate a complete MCV list (all the values in the
         * sample will fit, and we think these are all the ones in the table),
         * then do so.	Otherwise, store only those values that are
         * significantly more common than the (estimated) average. We set the
         * threshold rather arbitrarily at 25% more than average, with at
         * least 2 instances in the sample.
         */
        if (track_cnt < track_max && toowide_cnt == 0 && stats->stadistinct > 0 && track_cnt <= num_mcv) {
            /* Track list includes all values seen, and all will fit */
            num_mcv = track_cnt;
        } else {
            double ndistinct = stats->stadistinct;
            double avgcount, mincount;

            if (ndistinct < 0) {
                ndistinct = -ndistinct * totalrows;
            }
            /* estimate # of occurrences in sample of a typical value */
            avgcount = (double)samplerows / ndistinct;
            /* set minimum threshold count to store a value */
            mincount = avgcount * 1.25;
            if (mincount < 2) {
                mincount = 2;
            }
            if (num_mcv > track_cnt) {
                num_mcv = track_cnt;
            }
            for (i = 0; i < num_mcv; i++) {
                if (track[i].count < mincount) {
                    num_mcv = i;
                    break;
                }
            }
        }

        /* Generate MCV slot entry */
        if (num_mcv > 0) {
            MemoryContext old_context;
            Datum* mcv_values = NULL;
            float4* mcv_freqs = NULL;

            /* Must copy the target values into u_sess->analyze_cxt.analyze_context */
            old_context = MemoryContextSwitchTo(stats->anl_context);
            mcv_values = (Datum*)palloc(num_mcv * sizeof(Datum));
            mcv_freqs = (float4*)palloc(num_mcv * sizeof(float4));
            for (i = 0; i < num_mcv; i++) {
                mcv_values[i] = datumCopy(track[i].value, stats->attrtype[0]->typbyval, stats->attrtype[0]->typlen);
                mcv_freqs[i] = (double)track[i].count / (double)samplerows;
            }
            (void)MemoryContextSwitchTo(old_context);

            stats->stakind[0] = STATISTIC_KIND_MCV;
            stats->staop[0] = mystats->eqopr;
            stats->stanumbers[0] = mcv_freqs;
            stats->numnumbers[0] = num_mcv;
            stats->stavalues[0] = mcv_values;
            stats->numvalues[0] = num_mcv;

            /*
             * Accept the defaults for stats->statypid and others. They have
             * been set before we were called (see vacuum.h)
             */
        }
    } else if (null_cnt > 0) {
        /* We found only nulls; assume the column is entirely null */
        stats->stats_valid = true;
        stats->stanullfrac = 1.0;
        if (is_varwidth)
            stats->stawidth = 0; /* "unknown" */
        else
            stats->stawidth = stats->attrtype[0]->typlen;
        stats->stadistinct = 0.0; /* "unknown" */
    }

    /* We don't need to bother cleaning up any of our temporary palloc's */
}

/*
 *	compute_scalar_stats() -- compute column statistics
 *
 *	We use this when we can find "=" and "<" operators for the datatype.
 *
 *	We determine the fraction of non-null rows, the average width, the
 *	most common values, the (estimated) number of distinct values, the
 *	distribution histogram, and the correlation of physical to logical order.
 *
 *	The desired stats can be determined fairly easily after sorting the
 *	data values into order.
 */
static void compute_scalar_stats(
    VacAttrStatsP stats, AnalyzeAttrFetchFunc fetchfunc, int samplerows, double totalrows, Relation rel)
{
    int i;
    int null_cnt = 0;
    int nonnull_cnt = 0;
    int toowide_cnt = 0;
    double total_width = 0;
    bool is_varlena = (!stats->attrtype[0]->typbyval && stats->attrtype[0]->typlen == -1);
    bool is_varwidth = (!stats->attrtype[0]->typbyval && stats->attrtype[0]->typlen < 0);
    double corr_xysum;
    SortSupportData ssup;
    ScalarItem* values = NULL;
    int values_cnt = 0;
    int* tupnoLink = NULL;
    ScalarMCVItem* track = NULL;
    int track_cnt = 0;
    int num_mcv = Min(stats->attrs[0]->attstattarget, MAX_ATTR_MCV_STAT_TARGET);
    int num_bins = Min(stats->attrs[0]->attstattarget, MAX_ATTR_HIST_STAT_TARGET);
    StdAnalyzeData* mystats = (StdAnalyzeData*)stats->extra_data;

    values = (ScalarItem*)palloc(samplerows * sizeof(ScalarItem));
    tupnoLink = (int*)palloc(samplerows * sizeof(int));
    track = (ScalarMCVItem*)palloc(num_mcv * sizeof(ScalarMCVItem));

    errno_t rc = memset_s(&ssup, sizeof(ssup), 0, sizeof(ssup));
    securec_check(rc, "", "");

    ssup.ssup_cxt = CurrentMemoryContext;
    /* We always use the default collation for statistics */
    ssup.ssup_collation = DEFAULT_COLLATION_OID;
    ssup.ssup_nulls_first = false;
    /*
     * For now, don't perform abbreviated key conversion, because full values
     * are required for MCV slot generation.  Supporting that optimization
     * would necessitate teaching compare_scalars() to call a tie-breaker.
     */
    ssup.abbreviate = false;

    PrepareSortSupportFromOrderingOp(mystats->ltopr, &ssup);

    /* Initial scan to find sortable values */
    for (i = 0; i < samplerows; i++) {
        Datum value;
        bool isnull = false;

        vacuum_delay_point();

        value = fetchfunc(stats, i, &isnull, rel);

        /* Check for null/nonnull */
        if (isnull) {
            null_cnt++;
            continue;
        }
        nonnull_cnt++;

        /*
         * If it's a variable-width field, add up widths for average width
         * calculation.  Note that if the value is toasted, we use the toasted
         * width.  We don't bother with this calculation if it's a fixed-width
         * type.
         */
        if (is_varlena) {
            total_width += VARSIZE_ANY(DatumGetPointer(value));

            /*
             * If the value is toasted, we want to detoast it just once to
             * avoid repeated detoastings and resultant excess memory usage
             * during the comparisons.	Also, check to see if the value is
             * excessively wide, and if so don't detoast at all --- just
             * ignore the value.
             */
            if (toast_raw_datum_size(value) > WIDTH_THRESHOLD) {
                toowide_cnt++;
                continue;
            }
            value = PointerGetDatum(PG_DETOAST_DATUM(value));
        } else if (is_varwidth) {
            /* must be cstring */
            total_width += strlen(DatumGetCString(value)) + 1;
        }

        /* Add it to the list to be sorted */
        values[values_cnt].value = value;
        values[values_cnt].tupno = values_cnt;
        tupnoLink[values_cnt] = values_cnt;
        values_cnt++;
    }

    /* We can only compute real stats if we found some sortable values. */
    if (values_cnt > 0) {
        int ndistinct, /* # distinct values in sample */
            nmultiple, /* # that appear multiple times */
            num_hist, dups_cnt;
        int slot_idx = 0;
        CompareScalarsContext cxt;

        /* Sort the collected values */
        cxt.ssup = &ssup;
        cxt.tupnoLink = tupnoLink;
        qsort_arg((void*)values, values_cnt, sizeof(ScalarItem), compare_scalars, (void*)&cxt);

        /*
         * Now scan the values in order, find the most common ones, and also
         * accumulate ordering-correlation statistics.
         *
         * To determine which are most common, we first have to count the
         * number of duplicates of each value.	The duplicates are adjacent in
         * the sorted list, so a brute-force approach is to compare successive
         * datum values until we find two that are not equal. However, that
         * requires N-1 invocations of the datum comparison routine, which are
         * completely redundant with work that was done during the sort.  (The
         * sort algorithm must at some point have compared each pair of items
         * that are adjacent in the sorted order; otherwise it could not know
         * that it's ordered the pair correctly.) We exploit this by having
         * compare_scalars remember the highest tupno index that each
         * ScalarItem has been found equal to.	At the end of the sort, a
         * ScalarItem's tupnoLink will still point to itself if and only if it
         * is the last item of its group of duplicates (since the group will
         * be ordered by tupno).
         */
        corr_xysum = 0;
        ndistinct = 0;
        nmultiple = 0;
        dups_cnt = 0;
        for (i = 0; i < values_cnt; i++) {
            int tupno = values[i].tupno;

            corr_xysum += ((double)i) * ((double)tupno);
            dups_cnt++;
            if (tupnoLink[tupno] == tupno) {
                /* Reached end of duplicates of this value */
                ndistinct++;
                if (dups_cnt > 1) {
                    nmultiple++;
                    if (track_cnt < num_mcv || dups_cnt > track[track_cnt - 1].count) {
                        /*
                         * Found a new item for the mcv list; find its
                         * position, bubbling down old items if needed. Loop
                         * invariant is that j points at an empty/ replaceable
                         * slot.
                         */
                        int j;

                        if (track_cnt < num_mcv) {
                            track_cnt++;
                        }
                        for (j = track_cnt - 1; j > 0; j--) {
                            if (dups_cnt <= track[j - 1].count) {
                                break;
                            }
                            track[j].count = track[j - 1].count;
                            track[j].first = track[j - 1].first;
                        }
                        track[j].count = dups_cnt;
                        track[j].first = i + 1 - dups_cnt;
                    }
                }
                dups_cnt = 0;
            }
        }

        stats->stats_valid = true;
        /* Do the simple null-frac and width stats */
        stats->stanullfrac = (double)null_cnt / (double)samplerows;
        if (is_varwidth) {
            stats->stawidth = total_width / (double)nonnull_cnt;
        } else {
            stats->stawidth = stats->attrtype[0]->typlen;
        }

        analyze_compute_ndistinct(nmultiple, ndistinct, toowide_cnt, samplerows, totalrows, stats);

        /*
         * Decide how many values are worth storing as most-common values. If
         * we are able to generate a complete MCV list (all the values in the
         * sample will fit, and we think these are all the ones in the table),
         * then do so.	Otherwise, store only those values that are
         * significantly more common than the (estimated) average. We set the
         * threshold rather arbitrarily at 25% more than average, with at
         * least 2 instances in the sample.  Also, we won't suppress values
         * that have a frequency of at least 1/K where K is the intended
         * number of histogram bins; such values might otherwise cause us to
         * emit duplicate histogram bin boundaries.  (We might end up with
         * duplicate histogram entries anyway, if the distribution is skewed;
         * but we prefer to treat such values as MCVs if at all possible.)
         */
        if (track_cnt == ndistinct && toowide_cnt == 0 && stats->stadistinct > 0 && track_cnt <= num_mcv) {
            /* Track list includes all values seen, and all will fit */
            num_mcv = track_cnt;
        } else {
            double stadistinct = stats->stadistinct;
            double mincount;

            if (stadistinct < 0) {
                stadistinct = -stadistinct * totalrows;
            }

            mincount = compute_mcv_mincount(samplerows, stadistinct, num_bins);
            if (num_mcv > track_cnt) {
                num_mcv = track_cnt;
            }
            for (i = 0; i < num_mcv; i++) {
                if (track[i].count < mincount) {
                    num_mcv = i;
                    break;
                }
            }
        }

        /* Generate MCV slot entry */
        if (num_mcv > 0) {
            MemoryContext old_context;
            Datum* mcv_values = NULL;
            float4* mcv_freqs = NULL;

            /* Must copy the target values into analyze_context */
            old_context = MemoryContextSwitchTo(stats->anl_context);
            mcv_values = (Datum*)palloc(num_mcv * sizeof(Datum));
            mcv_freqs = (float4*)palloc(num_mcv * sizeof(float4));
            for (i = 0; i < num_mcv; i++) {
                mcv_values[i] =
                    datumCopy(values[track[i].first].value, stats->attrtype[0]->typbyval, stats->attrtype[0]->typlen);
                mcv_freqs[i] = (double)track[i].count / (double)samplerows;
            }
            (void)MemoryContextSwitchTo(old_context);

            stats->stakind[slot_idx] = STATISTIC_KIND_MCV;
            stats->staop[slot_idx] = mystats->eqopr;
            stats->stanumbers[slot_idx] = mcv_freqs;
            stats->numnumbers[slot_idx] = num_mcv;
            stats->stavalues[slot_idx] = mcv_values;
            stats->numvalues[slot_idx] = num_mcv;

            /*
             * Accept the defaults for stats->statypid and others. They have
             * been set before we were called (see vacuum.h)
             */
            slot_idx++;
        }

        /*
         * Generate a histogram slot entry if there are at least two distinct
         * values not accounted for in the MCV list.  (This ensures the
         * histogram won't collapse to empty or a singleton.)
         */
        num_hist = ndistinct - num_mcv;
        if (num_hist > num_bins) {
            num_hist = num_bins + 1;
        }
        if (num_hist >= 2) {
            MemoryContext old_context;
            Datum* hist_values = NULL;
            int nvals;
            int pos, posfrac, delta, deltafrac;

            /* Sort the MCV items into position order to speed next loop */
            qsort((void*)track, num_mcv, sizeof(ScalarMCVItem), compare_mcvs);

            /*
             * Collapse out the MCV items from the values[] array.
             *
             * Note we destroy the values[] array here... but we don't need it
             * for anything more.  We do, however, still need values_cnt.
             * nvals will be the number of remaining entries in values[].
             */
            if (num_mcv > 0) {
                int src, dest;
                int j;

                src = dest = 0;
                j = 0; /* index of next interesting MCV item */
                while (src < values_cnt) {
                    int ncopy;

                    if (j < num_mcv) {
                        int first = track[j].first;

                        if (src >= first) {
                            /* advance past this MCV item */
                            src = first + track[j].count;
                            j++;
                            continue;
                        }
                        ncopy = first - src;
                    } else {
                        ncopy = values_cnt - src;
                    }
                    rc = memmove_s(&values[dest], ncopy * sizeof(ScalarItem), &values[src], ncopy * sizeof(ScalarItem));
                    securec_check(rc, "", "");
                    src += ncopy;
                    dest += ncopy;
                }
                nvals = dest;
            } else {
                nvals = values_cnt;
            }
            AssertEreport(nvals >= num_hist, MOD_OPT, "");

            /* Must copy the target values into u_sess->analyze_cxt.analyze_context */
            old_context = MemoryContextSwitchTo(stats->anl_context);
            hist_values = (Datum*)palloc(num_hist * sizeof(Datum));

            /*
             * The object of this loop is to copy the first and last values[]
             * entries along with evenly-spaced values in between.	So the
             * i'th value is values[(i * (nvals - 1)) / (num_hist - 1)].  But
             * computing that subscript directly risks integer overflow when
             * the stats target is more than a couple thousand.  Instead we
             * add (nvals - 1) / (num_hist - 1) to pos at each step, tracking
             * the integral and fractional parts of the sum separately.
             */
            delta = (nvals - 1) / (num_hist - 1);
            deltafrac = (nvals - 1) % (num_hist - 1);
            pos = posfrac = 0;

            for (i = 0; i < num_hist; i++) {
                hist_values[i] = datumCopy(values[pos].value, stats->attrtype[0]->typbyval, stats->attrtype[0]->typlen);
                pos += delta;
                posfrac += deltafrac;
                if (posfrac >= (num_hist - 1)) {
                    /* fractional part exceeds 1, carry to integer part */
                    pos++;
                    posfrac -= (num_hist - 1);
                }
            }

            (void)MemoryContextSwitchTo(old_context);

            stats->stakind[slot_idx] = STATISTIC_KIND_HISTOGRAM;
            stats->staop[slot_idx] = mystats->ltopr;
            stats->stavalues[slot_idx] = hist_values;
            stats->numvalues[slot_idx] = num_hist;

            /*
             * Accept the defaults for stats->statypid and others. They have
             * been set before we were called (see vacuum.h)
             */
            slot_idx++;
        }

        /* Generate a correlation entry if there are multiple values */
        if (values_cnt > 1) {
            analyze_compute_correlation(values_cnt, corr_xysum, 0, stats, slot_idx);
            slot_idx++;
        }
    } else if (nonnull_cnt > 0) {
        /* We found some non-null values, but they were all too wide */
        Assert(nonnull_cnt == toowide_cnt);
        stats->stats_valid = true;

        /* Do the simple null-frac and width stats */
        stats->stanullfrac = (double) null_cnt / (double) samplerows;
        if (is_varwidth)
            stats->stawidth = total_width / (double) nonnull_cnt;
        else
            stats->stawidth = stats->attrtype[0]->typlen;

        /* Assume all too-wide values are distinct, so it's a unique column */
        stats->stadistinct = -1.0 * (1.0 - stats->stanullfrac);
    } else if (null_cnt > 0) {
        /* We found only nulls; assume the column is entirely null */
        stats->stats_valid = true;
        stats->stanullfrac = 1.0;
        if (is_varwidth) {
            stats->stawidth = 0; /* "unknown" */
        } else {
            stats->stawidth = stats->attrtype[0]->typlen;
        }
        stats->stadistinct = 0.0; /* "unknown" */
    }

    /* We don't need to bother cleaning up any of our temporary palloc's */
}

/*
 * qsort_arg comparator for sorting ScalarItems
 *
 * Aside from sorting the items, we update the tupnoLink[] array
 * whenever two ScalarItems are found to contain equal datums.	The array
 * is indexed by tupno; for each ScalarItem, it contains the highest
 * tupno that that item's datum has been found to be equal to.  This allows
 * us to avoid additional comparisons in compute_scalar_stats().
 */
static int compare_scalars(const void* a, const void* b, void* arg)
{
    Datum da = ((const ScalarItem*)a)->value;
    int ta = ((const ScalarItem*)a)->tupno;
    Datum db = ((const ScalarItem*)b)->value;
    int tb = ((const ScalarItem*)b)->tupno;
    CompareScalarsContext* cxt = (CompareScalarsContext*)arg;
    int compare;

    compare = ApplySortComparator(da, false, db, false, cxt->ssup);
    if (compare != 0) {
        return compare;
    }

    /*
     * The two datums are equal, so update cxt->tupnoLink[].
     */
    if (cxt->tupnoLink[ta] < tb) {
        cxt->tupnoLink[ta] = tb;
    }
    if (cxt->tupnoLink[tb] < ta) {
        cxt->tupnoLink[tb] = ta;
    }

    /*
     * For equal datums, sort by tupno
     */
    return ta - tb;
}

/*
 * @global stats
 * @Description: get sample flag the second times from the sample rows come which the first sampling.
 * @out require_samp - decide which block we need to get sample rows
 * @in totalSamples - the total sample row num of the first sampling
 * @in secSampSize - the second sample size
 * @return: void
 */
void get_sampling_rows(bool** require_samp, int totalSamples, int secSampSize)
{
    BlockSamplerData bs;
    *require_samp = (bool*)palloc0(totalSamples * sizeof(bool));

    BlockSampler_Init(&bs, totalSamples, secSampSize);
    while (BlockSampler_HasMore(&bs)) {
        BlockNumber targblock = BlockSampler_Next(&bs);
        (*require_samp)[targblock] = true;
    }

    return;
}

/*
 * @global_stats
 * compute sample rate for CN and DN.
 *
 * Input Param:
 * relid: the relation oid for analyze
 * vacstmt: the statment for analyze command
 * num_samples: identify CN or DN compute sample size
 *
 * Output Param:
 * require_samp: decide which block we need to get sample rows
 */
int compute_sample_size(VacuumStmt* vacstmt, int num_samples, bool** require_samp, Oid relid, int tableidx)
{
    double com_size;

    /* --------------------
     * The following choice of minrows is based on the paper
     * "Random sampling for histogram construction: how much is enough?"
     * by Surajit Chaudhuri, Rajeev Motwani and Vivek Narasayya, in
     * Proceedings of ACM SIGMOD International Conference on Management
     * of Data, 1998, Pages 436-447.  Their Corollary 1 to Theorem 5
     * says that for table size n, histogram size k, maximum relative
     * error fraction, and error probability gamma, the minimum
     * random sample size is
     *		r = 4 * k * ln(2*n/gamma) / error_fraction^2
     * We use gamma=0.01 in our calculations.
     * --------------------
     */
    if (num_samples <= 0) {
        Relation rel = NULL;

        vacstmt->sampleTableRequired = (default_statistics_target >= 0) ? false : true;
        if (vacstmt->pstGlobalStatEx[tableidx].totalRowCnts <= 0) {
            if (vacstmt->pstGlobalStatEx[tableidx].totalRowCnts == 0) {
                vacstmt->pstGlobalStatEx[tableidx].sampleRate = 0;
            } else {
                /*
                 * If estimate total rows are dead rows, may be exist lived rows,
                 * now we cann't compute  sample rate, so we should set sampleRate as 2%.
                 */
                vacstmt->pstGlobalStatEx[tableidx].sampleRate = DEFAULT_SAMPLERATE;
                vacstmt->pstGlobalStatEx[tableidx].totalRowCnts = 0;
            }

            return 0;
        }

        rel = relation_open(relid, AccessShareLock);
        com_size = compute_com_size(vacstmt, rel, tableidx);
        relation_close(rel, AccessShareLock);

        vacstmt->pstGlobalStatEx[tableidx].sampleRate = com_size / vacstmt->pstGlobalStatEx[tableidx].totalRowCnts;
        return 0;
    } else {
        /*
         * Compute num of sample rows as roundoff in all datanodes,
         * because avoid of exceeding to the num which compute in coordinator.
         */
        int secSize =
            (int)floor(vacstmt->pstGlobalStatEx[tableidx].totalRowCnts * vacstmt->pstGlobalStatEx[tableidx].sampleRate);
        secSize = Min(secSize, num_samples);
        get_sampling_rows(require_samp, num_samples, secSize);
        return secSize;
    }
}

/*
 * qsort comparator for sorting ScalarMCVItems by position
 */
static int compare_mcvs(const void* a, const void* b)
{
    int da = ((const ScalarMCVItem*)a)->first;
    int db = ((const ScalarMCVItem*)b)->first;

    return da - db;
}

/*
 * @global stats
 * @Description: send estimate or real total row count to cn.
 * @in vacstmt - the statment for analyze or vacuum command
 * @in analyzemode - identify which type the table will send total rows to cn
 * @in totalrows - the total row num
 * @return: void
 */
static void send_totalrowcnt_to_cn(VacuumStmt* vacstmt, AnalyzeMode analyzemode, double totalrows)
{
    StringInfoData buf;
    pq_beginmessage(&buf, 'P');

    if (analyzemode == ANALYZENORMAL) {
        pq_sendint64(&buf, totalrows);
        pq_sendint64(&buf, vacstmt->pstGlobalStatEx[ANALYZENORMAL].topMemSize);
    }

    pq_endmessage(&buf);
}

bool do_analyze_sampling_begin(Relation onerel, VacuumStmt* vacstmt, AnalyzeMode analyzemode, Relation& tmpRelation,
    Oid& tmpRelid, TupleConversionMap*& tupmap, TupleDesc& tupdesc, TupOutputState*& tstate)
{
    analyze_tmptbl_debug_dn(
        DebugStage_Begin, analyzemode, vacstmt->tmpSampleTblNameList, &tmpRelid, &tmpRelation, NULL, NULL);

    if (tmpRelation) {
        tupmap = check_tmptbl_tupledesc(onerel, tmpRelation->rd_att);
        if (tupmap == NULL) {
            /*
             * Don't insert tuple into temp table if doing ddl confilict with doing analyze.
             */
            analyze_tmptbl_debug_dn(
                DebugStage_End, ANALYZENORMAL, vacstmt->tmpSampleTblNameList, &tmpRelid, &tmpRelation, NULL, NULL);
            if (default_statistics_target < 0)
                return false;
        }
    }

    /*
     * Don't send sample rows to coordinator and only saved sample rows into
     * temp table for coordinator to select if relative-estimation.
     */
    if (!vacstmt->sampleTableRequired) {
        tupdesc = CreateTupleDescCopy(onerel->rd_att);
        tstate = begin_tup_output_tupdesc(vacstmt->dest, tupdesc);
    }

    return true;
}

/*
 * @global stats
 * @Description: Send the sample rows to cn generate for the second sampling and third sampling.
 *			Save sample rows to temp table for coordinator scan sample and compute statistics later.
 * @in onerel - the relation for analyze
 * @in vacstmt - the statment for analyze or vacuum command
 * @in numrows - the first sample row num
 * @in require_samp - the second sample flag
 * @in rows - the first sample rows
 * @return: void
 */
static void send_sample_to_cn(
    Relation onerel, VacuumStmt* vacstmt, int64 numrows, const bool* require_samp, HeapTuple* rows)

{
    TupOutputState* tstate = NULL;
    TupleDesc tupdesc = NULL;
    Oid tmpRelid = InvalidOid;
    Relation tmpRelation = NULL;
    TupleConversionMap* tupmap = NULL;

    bool ret =
        do_analyze_sampling_begin(onerel, vacstmt, ANALYZENORMAL, tmpRelation, tmpRelid, tupmap, tupdesc, tstate);
    if (!ret) {
        return;
    }

    vacstmt->dest->forAnalyzeSampleTuple = true;
    for (int64 i = 0; i < numrows; i++) {
        if (require_samp[i]) {
            if (!vacstmt->sampleTableRequired) {
                /* convert heaptuple into table tuple slot, then send back */
                (void)ExecStoreTuple(rows[i], tstate->slot, InvalidBuffer, false);

                (vacstmt->dest->receiveSlot)(tstate->slot, vacstmt->dest);
                (void)ExecClearTuple(tstate->slot);
            }

            analyze_tmptbl_debug_dn(DebugStage_Execute,
                ANALYZENORMAL,
                vacstmt->tmpSampleTblNameList,
                &tmpRelid,
                &tmpRelation,
                rows[i],
                tupmap);
        }
    }
    vacstmt->dest->forAnalyzeSampleTuple = false;

    analyze_tmptbl_debug_dn(
        DebugStage_End, ANALYZENORMAL, vacstmt->tmpSampleTblNameList, &tmpRelid, &tmpRelation, NULL, NULL);

    if (!vacstmt->sampleTableRequired) {
        /* clear tuple desc. */
        end_tup_output(tstate);
    }
}

/*
 * @global stats
 * @Description: do sampling for normal table the second times.
 * @in onerel - the relation for analyze
 * @in vacstmt - the statment for analyze or vacuum command
 * @in numrows - the first sample row num
 * @in totalrows - the total row num which using for comput the second sample num
 * @in rows - the first sample rows
 * @return: void
 */
static void do_sampling_normal_second(
    Relation onerel, VacuumStmt* vacstmt, int64 numrows, double totalrows, HeapTuple* rows)
{
    bool* require_samp = NULL;
    /* Sample the sampled rows from DN before sending them back to CN */
    vacstmt->tableidx = ANALYZENORMAL;
    vacstmt->pstGlobalStatEx[vacstmt->tableidx].totalRowCnts = totalrows;
    /* compute the num of sample rows for the second sampling based on the first num of rows we get. */
    vacstmt->num_samples = compute_sample_size(vacstmt, numrows, &require_samp, onerel->rd_id, ANALYZENORMAL);

    if (vacstmt->num_samples > 0) {
        /* send samples that we have sampled the second times to CN. */
        send_sample_to_cn(onerel, vacstmt, numrows, require_samp, rows);
    }

    elog(DEBUG1,
        "%s insert %lf sample rows for normal table into temp table for queryid:%lu. "
        "detail: samplerows[%ld], sampleRate[%lf], secsamplerows[%d]",
        g_instance.attr.attr_common.PGXCNodeName,
        totalrows,
        u_sess->debug_query_id,
        numrows,
        vacstmt->pstGlobalStatEx[ANALYZENORMAL].sampleRate,
        vacstmt->num_samples);

    return;
}

/*
 * equalTupleDescAttrsTypeid: check two tuples descriptor's typeid are diffirent or not.
 *
 * Parameters:
 *	@in tupdesc1: one tuple descriptor
 *	@in	tupdesc2: another tuple descriptor
 *
 * Returns: bool
 */
static bool equalTupleDescAttrsTypeid(TupleDesc tupdesc1, TupleDesc tupdesc2)
{
    if ((NULL == tupdesc1) || (NULL == tupdesc2))
        return true;

    if (tupdesc1->natts != tupdesc2->natts)
        return false;

    for (int i = 0; i < tupdesc1->natts; i++) {
        Form_pg_attribute attr1 = &tupdesc1->attrs[i];
        Form_pg_attribute attr2 = &tupdesc2->attrs[i];

        if ((!attr1->attisdropped && !attr2->attisdropped) &&
            ((attr1->atttypid != attr2->atttypid) || (attr1->attlen != attr2->attlen)))
            return false;
    }

    return true;
}

static BlockNumber GetOneRelNBlocks(
    const Relation onerel, const Relation Irel, const VacuumStmt* vacstmt, double totalindexrows)
{
    BlockNumber nblocks = 0;
    if (RelationIsColStore(onerel)) {
        nblocks = estimate_psort_index_blocks(Irel->rd_att, totalindexrows);
    } else if (RelationIsPartitioned(onerel) && !RelationIsGlobalIndex(Irel)) {
        ListCell* partCell = NULL;
        Partition part = NULL;
        Oid indexOid = InvalidOid;
        Oid partOid = InvalidOid;
        Oid partIndexOid = InvalidOid;
        Partition indexPart = NULL;
        Relation partRel = NULL;

        indexOid = RelationGetRelid(Irel);

        foreach (partCell, vacstmt->partList) {
            part = (Partition)lfirst(partCell);
            partOid = PartitionGetPartid(part);
            if (RelationIsSubPartitioned(onerel)) {
                partRel = partitionGetRelation(onerel, part);
                List *subPartOidList = relationGetPartitionOidList(partRel);
                ListCell *lc = NULL;
                foreach (lc, subPartOidList) {
                    Oid subPartOid = (Oid)lfirst_oid(lc);
                    partIndexOid = getPartitionIndexOid(indexOid, subPartOid);
                }
                releasePartitionOidList(&subPartOidList);
                releaseDummyRelation(&partRel);
            } else {
                partIndexOid = getPartitionIndexOid(indexOid, partOid);
            }
            indexPart = partitionOpen(Irel, partIndexOid, AccessShareLock);
            nblocks += PartitionGetNumberOfBlocks(Irel, indexPart);
            partitionClose(Irel, indexPart, AccessShareLock);
        }
    } else {
        nblocks = RelationGetNumberOfBlocks(Irel);
    }

    return nblocks;
}

/*
 * estimate_index_blocks
 * This function estimates the number of blocks(pages) of the index.
 * We assume that all non column stored relations are being stored as B-trees.
 */
static BlockNumber estimate_index_blocks(Relation rel, double totalTuples, double table_factor)
{
    BlockNumber nblocks = 0;
    if (RelationIsColStore(rel)) {
        nblocks = estimate_psort_index_blocks(rel->rd_att, totalTuples);
    } else {
        nblocks = estimate_btree_index_blocks(rel->rd_att, totalTuples, table_factor);
    }
    return nblocks;
}

/*
 * update_pages_and_tuples_pgclass: Update pages and tuples of the relation for analyze to pg_class.
 *
 * Parameters:
 *	@in onerel: the relation for analyze or vacuum
 *	@in	vacstmt: the statment for analyze or vacuum command
 *	@in attr_cnt: the count of attributes for analyze
 *	@in vacattrstats: statistic structure for all attributes or index by the statment for analyze
 *					using for comput and update pg_statistic.
 *	@in hasindex: are there have index or not for analyze
 *	@in nindexes: the num of index for analyze
 *	@in indexdata: per-index data for analyze
 *	@in Irel: malloc a new index relation for analyze
 *	@in relpages: relation pages
 *	@in totalrows: total rows for the relation
 *	@in numrows: the num of sample rows
 *	@in inh: is inherit table for analzye or not
 *
 * Returns: void
 */
static void update_pages_and_tuples_pgclass(Relation onerel, VacuumStmt* vacstmt, int attr_cnt,
    VacAttrStats** vacattrstats, bool hasindex, int nindexes, AnlIndexData* indexdata, Relation* Irel,
    BlockNumber relpages, double totalrows, double totaldeadrows, int64 numrows, bool inh)
{
    BlockNumber updrelpages = relpages;
    /*
     * we have updated pg_class in function ReceivePageAndTuple when we receive pg_class from dn1,
     * so, we only update pg_class on datanodes this place.
     */
    if (IS_PGXC_DATANODE) {
        BlockNumber mapCont = 0;

        /* update relpages for column-oriented table */
        if (RelationIsColStore(onerel)) {
            /*
             * for CU format and PAX format, relpages is 0 since RelationGetNumberOfBlocks
             * return 0, so we have to generate relpages from totalrows
             *
             * The best way is use all rows(alive and dead rows), however to ensure forward
             * compatibility, we can only process scenes that all rows are dead
             */
            double allrows = totalrows > 0 ? totalrows : totaldeadrows;

            /* * If the relation is DFS table, get relpage from HDFS files. */
            if (RelationIsPAXFormat(onerel)) {
                updrelpages = estimate_cstore_blocks(onerel, vacattrstats, attr_cnt, numrows, allrows, true);
            } else {
                updrelpages = estimate_cstore_blocks(onerel, vacattrstats, attr_cnt, numrows, allrows, false);
            }
        }
#ifdef ENABLE_MULTIPLE_NODES
        else if (RelationIsTsStore(onerel)) {
            updrelpages = estimate_tsstore_blocks(onerel, attr_cnt, totalrows);
        }
#endif   /* ENABLE_MULTIPLE_NODES */

        if (RelationIsPartitioned(onerel)) {
            Relation partRel = NULL;
            ListCell* partCell = NULL;
            Partition part = NULL;

            foreach (partCell, vacstmt->partList) {
                part = (Partition)lfirst(partCell);
                partRel = partitionGetRelation(onerel, part);
                mapCont += visibilitymap_count(onerel, part);
                releaseDummyRelation(&partRel);
            }
        } else {
            mapCont = visibilitymap_count(onerel, NULL);
        }

        Relation classRel = heap_open(RelationRelationId, RowExclusiveLock);
        vac_update_relstats(onerel, classRel, updrelpages, totalrows, mapCont,
            hasindex, InvalidTransactionId, InvalidMultiXactId);
        heap_close(classRel, RowExclusiveLock);
    }

    /* Estimate table expansion factor */
    double table_factor = 1.0;
    /* Compute table factor with GUC PARAM_PATH_OPTIMIZATION on */
    bool isCompute = ENABLE_SQL_BETA_FEATURE(PARAM_PATH_OPT) && onerel && onerel->rd_rel;
    if (isCompute) {
        BlockNumber pages = onerel->rd_rel->relpages;
        /* Reuse index estimation here (it is better to get the factor base on same kind of estimation) */
        double estimated_relpages = (double)estimate_index_blocks(onerel, totalrows, table_factor);
        table_factor = (double)pages / estimated_relpages;
        /* Use 1.0 if table factor is too small */
        table_factor = table_factor > 1.0 ? table_factor : 1.0;
    }

    /*
     * Same for indexes. Vacuum always scans all indexes, so if we're part of
     * VACUUM ANALYZE, don't overwrite the accurate count already inserted by
     * VACUUM.
     */
    if (!((unsigned int)vacstmt->options & VACOPT_VACUUM) || ((unsigned int)vacstmt->options & VACOPT_ANALYZE)) {
        for (int ind = 0; ind < nindexes; ind++) {
            AnlIndexData* thisdata = &indexdata[ind];
            double totalindexrows;
            BlockNumber nblocks = 0;
            if (RelationIsUstoreFormat(onerel)) {
                totalindexrows = totalrows;
            } else {
                totalindexrows = ceil(thisdata->tupleFract * totalrows);
            }
            /*
             * @global stats
             * Update pg_class with global relpages and global reltuples for indexs the CN where analyze is issued.
             */
            if (IS_PGXC_COORDINATOR && ((unsigned int)vacstmt->options & VACOPT_ANALYZE) &&
                (0 != vacstmt->pstGlobalStatEx[vacstmt->tableidx].totalRowCnts)) {
                nblocks = estimate_index_blocks(Irel[ind], totalindexrows, table_factor);
                Relation classRel = heap_open(RelationRelationId, RowExclusiveLock);
                vac_update_relstats(Irel[ind], classRel, nblocks, totalindexrows, 0,
                    false, BootstrapTransactionId, InvalidMultiXactId);
                heap_close(classRel, RowExclusiveLock);
                continue;
            }

            /* Don't update pg_class for vacuum. */
            if ((unsigned int)vacstmt->options & VACOPT_VACUUM)
                break;

            nblocks = GetOneRelNBlocks(onerel, Irel[ind], vacstmt, totalindexrows);

            Relation classRel = heap_open(RelationRelationId, RowExclusiveLock);
            vac_update_relstats(Irel[ind], classRel, nblocks, totalindexrows, 0,
                false, InvalidTransactionId, InvalidMultiXactId);
            heap_close(classRel, RowExclusiveLock);
        }
    }
}

void switch_memory_context(MemoryContext& col_context, MemoryContext& old_context,
    AnalyzeMode analyzemode)
{
    if (ANALYZENORMAL == analyzemode) {
        col_context = AllocSetContextCreate(u_sess->analyze_cxt.analyze_context,
            "Analyze Column",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
        old_context = MemoryContextSwitchTo(col_context);
    }
}

/*
 * do_analyze_samplerows: Compute statistics and update to pg_statistic if using sample rows.
 *
 * Parameters:
 *	@in onerel: the relation for analyze or vacuum
 *	@in	vacstmt: the statment for analyze or vacuum command
 *	@in attr_cnt: the count of attributes for analyze
 *	@in vacattrstats: statistic structure for all attributes or index by the statment for analyze
 *					using for comput and update pg_statistic.
 *	@in hasindex: are there have index or not for analyze
 *	@in nindexes: the num of index for analyze
 *	@in indexdata: per-index data for analyze
 *	@in tableidx: identify the relation type for analyze
 *	@in inh: is inherit table for analzye or not
 *	@in Irel: malloc a new index relation for analyze
 *	@in totalrows: total rows for the relation
 *	@in numrows: the num of sample rows
 *	@in rows: sample tuples for compute statistics
 *	@in analyzemode: identify the relation type for analyze
 *	@in caller_context: analyze memory context
 *	@in save_userid: saved current userId using for finalize to restore
 *	@in save_sec_context: saved for SecurityRestrictionContext
 *	@in save_nestlevel: saved for GUCNestLevel
 *
 * Returns: void
 */
static bool do_analyze_samplerows(Relation onerel, VacuumStmt* vacstmt, int attr_cnt, VacAttrStats** vacattrstats,
    bool hasindex, int nindexes, AnlIndexData* indexdata, bool inh, Relation* Irel, double* totalrows, int64* numrows,
    HeapTuple* rows, AnalyzeMode analyzemode, MemoryContext caller_context,
    Oid save_userid, int save_sec_context, int save_nestlevel)
{
    int64 num_samplerows = *numrows;
    double num_total_rows = *totalrows;
    HeapTuple* samplerows = rows;
    int tableidx = vacstmt->tableidx;

    /*
     * If sampleRate more than 0, it means DN need send sample rows and total row count to CN.
     * We should send real total row count back to CN If sampleRate of either
     * dfs or delta more than 0, and current table is dfs.
     * because if dfs's rows is 0, we should send delta's rows to CN.
     */
    if (NEED_DO_SECOND_ANALYZE(analyzemode, vacstmt)) {
        switch (analyzemode) {
            case ANALYZENORMAL:
                do_sampling_normal_second(onerel, vacstmt, num_samplerows, num_total_rows, rows);
                break;
            default:
                break;
        }

        /* We should send real total row count to CN If the count more than 0 on datanode. */
        if (NEED_SEND_TOTALROWCNT_TO_CN(analyzemode, vacstmt, num_total_rows)) {
            send_totalrowcnt_to_cn(vacstmt, analyzemode, num_total_rows);
        }
    }

    /*
     * @global stats: totalRowCnts = 0 indicates it's another CN trying to sync global stats with
     * the CN where analyze issued,
     * and we don't want other CNs to get the sample rows and compute stats again.
     */
    if (IS_PGXC_COORDINATOR && (vacstmt->pstGlobalStatEx[tableidx].totalRowCnts != 0)) {
        /* get sample rows and num for diffirent table. */
        if (analyzemode == ANALYZENORMAL) {
            samplerows = vacstmt->sampleRows;
            num_samplerows = vacstmt->num_samples;
        } else {
            samplerows = vacstmt->pstGlobalStatEx[tableidx].sampleRows;
            num_samplerows = vacstmt->pstGlobalStatEx[tableidx].num_samples;
        }

        num_total_rows = vacstmt->pstGlobalStatEx[tableidx].totalRowCnts;
        *numrows = num_samplerows;
        *totalrows = num_total_rows;

        /*
         * There is a condition: coordinator has received sample rows from all datanodes,
         * then the relation's attribute type has modified by other client.
         * if we compute stats using relation's attribute in local, it different with the sample tuples,
         * so we should check the relation's attribute in local is different with attribute
         * received from datanodes or not.
         */
        if (!IsConnFromCoord() && !vacstmt->pstGlobalStatEx[tableidx].isReplication) {
            TupleDesc rd_att =
                (analyzemode == ANALYZENORMAL) ? vacstmt->tupleDesc : vacstmt->pstGlobalStatEx[tableidx].tupleDesc;

            /*
             * It identify the relation's attribute have modified in local under analyzing
             * if they are different, we don't compute stats for the more.
             */
            if (!equalTupleDescAttrsTypeid(rd_att, onerel->rd_att)) {
                if (!inh)
                    vac_close_indexes(nindexes, Irel, NoLock);

                do_analyze_finalize(caller_context, save_userid, save_sec_context, save_nestlevel, analyzemode);

                /*
                 * If vacstmt->relation is NULL, it means that the delta table is processed here.
                 * The table information in the warning will be given when processing the main table.
                 */
                if (vacstmt->relation)
                    ereport(WARNING,
                        (errmsg("The tupleDesc analyzed in coordinator is different from tupleDesc which received from "
                                "datanode."),
                            errhint("Do analyze again for the relation: %s.", vacstmt->relation->relname)));
                else
                    ereport(WARNING,
                        (errmsg("The tupleDesc analyzed in coordinator is different from tupleDesc which received from "
                                "datanode.")));

                return false;
            }
        }
    }

    if (num_samplerows <= 0) {
        return true;
    }

    int i;
    MemoryContext col_context, old_context;
    /* we use u_sess.analyze_cxt.analyze_context for normal table. */
    switch_memory_context(col_context, old_context, analyzemode);

    for (i = 0; i < attr_cnt; i++) {
        VacAttrStats* stats = vacattrstats[i];
        AttributeOpts* aopt = NULL;

        /* Focus on single column here, skip multi-column statistics */
        if (stats->num_attrs > 1) {
            continue;
        }

        stats->rows = samplerows;
        stats->tupDesc = onerel->rd_att;
        Assert(onerel->rd_att->natts == stats->tupDesc->natts);

        DEBUG_START_TIMER;
        (*stats->compute_stats)(stats, std_fetch_func, num_samplerows, num_total_rows, onerel);
        DEBUG_STOP_TIMER("Compute statistic for attr: %s", NameStr(stats->attrs[0]->attname));

        /*
         * If the appropriate flavor of the n_distinct option is
         * specified, override with the corresponding value.
         */
        aopt = get_attribute_options(onerel->rd_id, stats->attrs[0]->attnum);
        if (aopt != NULL) {
            float8 n_distinct;

            n_distinct = inh ? aopt->n_distinct_inherited : aopt->n_distinct;
            if (fabs(n_distinct) > EPSILON)
                stats->stadistinct = n_distinct;
        }

        set_stats_dndistinct(stats, vacstmt, tableidx);

        MemoryContextResetAndDeleteChildren(col_context);
    }

    if (hasindex)
        compute_index_stats(onerel, num_total_rows, indexdata, nindexes, samplerows, num_samplerows, col_context);

    (void)MemoryContextSwitchTo(old_context);
    MemoryContextDelete(col_context);

    /*
     * Emit the completed stats rows into pg_statistic, replacing any
     * previous statistics for the target columns.	(If there are stats in
     * pg_statistic for columns we didn't process, we leave them alone.)
     */
    update_attstats(RelationGetRelid(onerel), STARELKIND_CLASS, inh, attr_cnt, vacattrstats,
                    RelationGetRelPersistence(onerel));
    for (i = 0; i < nindexes; i++) {
        AnlIndexData* thisdata = &indexdata[i];

        update_attstats(RelationGetRelid(Irel[i]), STARELKIND_CLASS, false, thisdata->attr_cnt, thisdata->vacattrstats,
                        RelationGetRelPersistence(Irel[i]));
    }

    return true;
}

/*
 * Description: Initialize special information for analyze one attributes of sample table.
 *
 * Parameters:
 *	@in spec: the sample info of special attribute for compute statistic
 *	@in stats: statistic info of one attribute for analyze
 *	@in numTotalRows: total rows for the relation
 *	@in numSampleRows: the num of sample rows
 *
 * Returns: void
 */
static void init_sample_table_spec_info(
    AnalyzeSampleTableSpecInfo* spec, VacAttrStats* stats, double numTotalRows, int64 numSampleRows)
{
    errno_t rc;
    AssertEreport(stats != NULL, MOD_OPT, "");
    AssertEreport(numSampleRows > 0.0, MOD_OPT, "");

    /* Default values */
    stats->stadistinct = 0.0;
    stats->stanullfrac = 0.0;
    stats->stawidth = 0;
    spec->stats = stats;
    spec->is_varwidth = es_is_variable_width(stats);
    spec->nonnull_cnt = 0;
    spec->null_cnt = 0;
    spec->nmultiple = 0;
    spec->ndistinct = 0;
    spec->samplerows = numSampleRows;
    spec->totalrows = numTotalRows;

    spec->v_alias = (char**)palloc0(sizeof(char*) * stats->num_attrs);
    for (unsigned int i = 0; i < stats->num_attrs; ++i) {
        spec->v_alias[i] = (char*)palloc0(sizeof(char) * NAMEDATALEN);
    }

    rc = memset_s(&spec->mcv_list, sizeof(McvInfo), 0, sizeof(McvInfo));
    securec_check(rc, "\0", "\0");
    spec->mcv_list.stattarget = MAX_ATTR_MCV_STAT_TARGET;

    rc = memset_s(&spec->hist_list, sizeof(HistgramInfo), 0, sizeof(HistgramInfo));
    securec_check(rc, "\0", "\0");
    spec->hist_list.stattarget = MAX_ATTR_HIST_STAT_TARGET;
    spec->hist_list.histitem = (SampleItem*)palloc0(spec->hist_list.stattarget * sizeof(SampleItem));
}

/*
 * do_analyze_sampletable: analyze attributes with execute query, compute statistics
 *						and update to pg_statistic if using sample rows.
 *
 * Parameters:
 *	@in onerel: the relation for analyze or vacuum
 *	@in vacstmt: the statment for analyze or vacuum command
 *	@in attr_cnt: the count of attributes for analyze
 *	@in vacattrstats: statistic structure for all attributes or index by the statment for analyze
 *					using for comput and update pg_statistic.
 *	@in hasindex: are there have index or not for analyze
 *	@in nindexes: the num of index for analyze
 *	@in indexdata: per-index data for analyze
 *	@in inh: is inherit table for analzye or not
 *	@in Irel: malloc a new index relation for analyze
 *	@in totalrows: total rows for the relation
 *	@in numrows: the num of sample rows
 *	@in analyzemode: identify the relation type for analyze
 *
 * Returns: void
 */
static void do_analyze_sampletable(Relation onerel, VacuumStmt* vacstmt, int attr_cnt, VacAttrStats** vacattrstats,
    bool hasindex, int nindexes, AnlIndexData* indexdata, bool inh, Relation* Irel, double totalrows, int64 numrows,
    AnalyzeMode analyzemode)
{
    int i;
    MemoryContext col_context, old_context;
    int64 num_sample = 0;
    HeapTuple* samplerows = NULL;

    /* we use u_sess.analyze_cxt.analyze_context for normal table. */
    switch_memory_context(col_context, old_context, analyzemode);

    for (i = 0; i < attr_cnt; i++) {
        VacAttrStats* stats = vacattrstats[i];
        AttributeOpts* aopt = NULL;

        stats->tupDesc = onerel->rd_att;

        if (stats->num_attrs > 1 || (!OidIsValid(stats->attrtype[0]->typanalyze))) {
            /* Analyze one attribute with execute query. */
            do_analyze_compute_attr_stats(inh, onerel, vacstmt, stats, totalrows, numrows, analyzemode);
        } else {
            /*
             * If specific the typanalyze of function, we should get sample rows from all datanodes,
             * and if there are more than one specific typanalyze of function, we want to get sample only once.
             */
            if (0 == num_sample) {
                get_sample_rows_for_query(
                    u_sess->analyze_cxt.analyze_context, onerel, vacstmt, &num_sample, &samplerows);
            }

            /* If we have got sample rows, we should call the specific function of analyze only once. */
            if (0 != num_sample) {
                stats->rows = samplerows;
                (*stats->compute_stats)(stats, std_fetch_func, num_sample, totalrows, onerel);
            }
        }

        /*
         * If the appropriate flavor of the n_distinct option is
         * specified, override with the corresponding value.
         */
        aopt = get_attribute_options(onerel->rd_id, stats->attrs[0]->attnum);
        if (aopt != NULL) {
            float8 n_distinct;

            n_distinct = inh ? aopt->n_distinct_inherited : aopt->n_distinct;
            if (fabs(n_distinct) > EPSILON)
                stats->stadistinct = n_distinct;
        }

        set_stats_dndistinct(stats, vacstmt, vacstmt->tableidx);

        MemoryContextResetAndDeleteChildren(col_context);
    }

    /*
     * Emit the completed stats rows into pg_statistic, replacing any
     * previous statistics for the target columns.	(If there are stats in
     * pg_statistic for columns we didn't process, we leave them alone.)
     */
    update_attstats(RelationGetRelid(onerel), STARELKIND_CLASS, inh, attr_cnt, vacattrstats,
                    RelationGetRelPersistence(onerel));

    /*
     * If exist index and there is no sample rows, we don't need compute statistic for index any more.
     * Otherwise, we shoulde compute index statistics.
     */
    if (hasindex &&
        analyze_index_sample_rows(
            u_sess->analyze_cxt.analyze_context, onerel, vacstmt, nindexes, indexdata, &num_sample, &samplerows)) {
        compute_index_stats(onerel, totalrows, indexdata, nindexes, samplerows, num_sample, col_context);

        for (i = 0; i < nindexes; i++) {
            AnlIndexData* thisdata = &indexdata[i];

            update_attstats(
                RelationGetRelid(Irel[i]), STARELKIND_CLASS, false, thisdata->attr_cnt, thisdata->vacattrstats,
                RelationGetRelPersistence(Irel[i]));
        }
    }

    (void)MemoryContextSwitchTo(old_context);
    MemoryContextDelete(col_context);
}

/*
 * analyze_compute_ndistinct: estimate the number of distinct values using the estimator.
 *
 * Parameters:
 *	@in nmultiple: duplicate num of distinct values more than 1
 *	@in ndistinct: num of distinct for sample rows
 *	@in toowide_cnt: how many rows that value is toasted
 *	@in samplerows: num of sample rows
 *	@in totalrows: num of total rows
 *	@in stats: statistics for one attribute
 *
 * Returns: void
 */
static void analyze_compute_ndistinct(
    int64 nmultiple, double ndistinct, int64 toowide_cnt, int64 samplerows, double totalrows, VacAttrStatsP stats)
{
    if (nmultiple == 0) {
        /*
         * If we found no repeated non-null values, assume it's a unique
         * column; but be sure to discount for any nulls we found.
         */
        stats->stadistinct = -1.0 * (1 - stats->stanullfrac);
    } else if (toowide_cnt == 0 && (nmultiple - ndistinct == 0)) {
        /*
         * Every value in the sample appeared more than once.  Assume the
         * column has just these values.
         */
        stats->stadistinct = ndistinct;
    } else {
        /* ----------
         * Estimate the number of distinct values using the estimator
         * proposed by Haas and Stokes in IBM Research Report RJ 10025:
         *		n*d / (n - f1 + f1*n/N)
         * where f1 is the number of distinct values that occurred
         * exactly once in our sample of n rows (from a total of N),
         * and d is the total number of distinct values in the sample.
         * This is their Duj1 estimator; the other estimators they
         * recommend are considerably more complex, and are numerically
         * very unstable when n is much smaller than N.
         *
         * Overwidth values are assumed to have been distinct.
         * ----------
         */
        double f1 = ndistinct - nmultiple + toowide_cnt;
        double d = f1 + nmultiple;
        double numer = 0;
        double denom = 0;
        double stadistinct = 0;

        numer = (double)samplerows * d;
        denom = (double)(samplerows - f1) + (double)f1 * (double)samplerows / totalrows;
        stadistinct = numer / denom;

        /* Clamp to sane range in case of roundoff error */
        if (stadistinct < (double)d) {
            stadistinct = (double)d;
        }

        if (stadistinct > totalrows) {
            stadistinct = totalrows;
        }

        stats->stadistinct = floor(stadistinct + 0.5);
    }

    /*
     * If we estimated the number of distinct values at more than 10% of
     * the total row count (a very arbitrary limit), then assume that
     * stadistinct should scale with the row count rather than be a fixed
     * value.
     */
    if (stats->stadistinct > 0.1 * totalrows) {
        stats->stadistinct = -(stats->stadistinct / totalrows);
    }

    /* gloable distinct value >= datanode distinct value */
    if (stats->stadistinct >= 0 && stats->stadndistinct >= 0 && stats->stadndistinct > stats->stadistinct) {
        stats->stadistinct = stats->stadndistinct;
    }
}

/*
 * analyze_compute_correlation: estimate correlation.
 *
 * Parameters:
 *	@in values_cnt: count of non-null values for all samples
 *	@in corr_xysum: correlation of the sum
 *	@in correlations: the correlations of dn1 if non zero for sampletable, others compute by corr_xysum
 *	@in stats: statistics for one attribute
 *	@in slot_idx: indentify index of statistic kind for correlation
 *
 * Returns: void
 */
static void analyze_compute_correlation(
    int64 values_cnt, double corr_xysum, double correlations, VacAttrStatsP stats, int slot_idx)
{
    StdAnalyzeData* mystats = (StdAnalyzeData*)stats->extra_data;
    MemoryContext old_context;
    float4* corrs = NULL;
    double corr_xsum, corr_x2sum;

    /* Must copy the target values into analyze_context */
    old_context = MemoryContextSwitchTo(stats->anl_context);
    corrs = (float4*)palloc(sizeof(float4));
    (void)MemoryContextSwitchTo(old_context);

    /* ----------
     * Since we know the x and y value sets are both
     *		0, 1, ..., values_cnt-1
     * we have sum(x) = sum(y) =
     *		(values_cnt-1)*values_cnt / 2
     * and sum(x^2) = sum(y^2) =
     *		(values_cnt-1)*values_cnt*(2*values_cnt-1) / 6.
     * ----------
     */
    if (correlations == 0) {
        corr_xsum = ((double)(values_cnt - 1)) * ((double)values_cnt) / 2.0;
        corr_x2sum = ((double)(values_cnt - 1)) * ((double)values_cnt) * (double)(2 * values_cnt - 1) / 6.0;

        /* And the correlation coefficient reduces to */
        corrs[0] =
            (values_cnt * corr_xysum - corr_xsum * corr_xsum) / (values_cnt * corr_x2sum - corr_xsum * corr_xsum);
    } else {
        *corrs = correlations;
    }

    stats->stakind[slot_idx] = STATISTIC_KIND_CORRELATION;
    stats->staop[slot_idx] = mystats->ltopr;
    stats->stanumbers[slot_idx] = corrs;
    stats->numnumbers[slot_idx] = 1;
}

/*
 * compute_mcv_mincount: Compute the min count of mcv.
 *
 * Parameters:
 *	@in samplerows: num of all the samples
 *	@in ndistinct: how many distinct value of all the samples
 *	@in attstattarget: the bound for the final mcv list.
 *
 * Returns: double
 */
static double compute_mcv_mincount(int64 samplerows, double ndistinct, double attstattarget)
{
#define MIN_MCV_THRESHOLD 1.25 /* the minimum threshold count */
    double avgcount, mincount, maxmincount;

    /* estimate # of occurrences in sample of a typical value */
    avgcount = (double)samplerows / ndistinct;
    /* set minimum threshold count to store a value */
    mincount = avgcount * 1.25;
    if (mincount < 2) {
        mincount = 2;
    }

    /* don't let threshold exceed 1/K, however */
    maxmincount = (double)samplerows / attstattarget;

    if (mincount > maxmincount) {
        mincount = maxmincount;
    }

    return mincount;
}

/*
 * spi_callback_get_sample_rows: A callback function for use to	copy each tuple for index sample rows.
 *
 * Parameters:
 *	@in clientData: argument to call back function (usually pointer to data-structure
 *				   that the callback function populates).
 *
 * Returns: void
 */
static void spi_callback_get_sample_rows(void* clientData)
{
    MemoryContext oldctx;
    AnalyzeSampleRowsSpecInfo* spec = (AnalyzeSampleRowsSpecInfo*)clientData;
    bool* nulls = NULL;
    Datum* values = NULL;

    AssertEreport(SPI_tuptable != NULL, MOD_OPT, "");
    AssertEreport(SPI_tuptable->tupdesc, MOD_OPT, "");

    if (0 == SPI_processed)
        return;

    nulls = (bool*)palloc0(spec->tupDesc->natts * sizeof(bool));
    values = (Datum*)palloc0(spec->tupDesc->natts * sizeof(Datum));

    oldctx = MemoryContextSwitchTo(spec->memorycontext);
    spec->num_sample = SPI_processed;
    spec->samplerows = (HeapTuple*)palloc0(SPI_processed * sizeof(HeapTuple));

    if (spec->tupDesc->natts == SPI_tuptable->tupdesc->natts) {
        for (uint32 i = 0; i < SPI_processed; i++) {
            spec->samplerows[i] = (HeapTuple)tableam_tops_copy_tuple(SPI_tuptable->vals[i]);
        }
    } else {
        /*
         * Rebuild tuple value by using the tuple receive by spi interface and relation's tuple desc.
         * Because if there is dropped column, the tuple desc of statistics include it and
         * the tuple desc receive from datanode exclude it, so we should keep consistent
         * between tuple desc and tuple values.
         */
        for (uint32 i = 0; i < SPI_processed; i++) {
            int dropped_num = 0;

            for (int j = 0; j < spec->tupDesc->natts; j++) {
                Form_pg_attribute attr1 = &spec->tupDesc->attrs[j];

                if (attr1->attisdropped) {
                    nulls[j] = true;
                    dropped_num++;
                    continue;
                }
                values[j] = tableam_tops_tuple_getattr(SPI_tuptable->vals[i], j - dropped_num + 1, SPI_tuptable->tupdesc, &nulls[j]);
            }

            AssertEreport(spec->tupDesc->natts == (SPI_tuptable->tupdesc->natts + dropped_num), MOD_OPT, "");
            spec->samplerows[i] = (HeapTuple)tableam_tops_form_tuple(spec->tupDesc, values, nulls);
        }
    }

    (void)MemoryContextSwitchTo(oldctx);
}

static void get_sample_rows_utility(
    MemoryContext speccontext, Relation onerel, double sampleRate, int64* num_sample_rows, HeapTuple** samplerows)
{
    AnalyzeSampleRowsSpecInfo spec;
    const char* sampleSchemaName = NULL;
    const char* sampleTableName = NULL;
    StringInfoData str;
    initStringInfo(&str);

    sampleSchemaName = get_namespace_name(get_rel_namespace(onerel->rd_id));
    sampleTableName = get_rel_name(onerel->rd_id);

    /**
     * This query orders the table by rank on the value and chooses values that occur every
     * 'bucketSize' interval. It also chooses the maximum value from the relation. It then
     * removes duplicate values and orders the values in ascending order. This corresponds to
     * the histogram.
     */
    appendStringInfo(&str,
        "select * from %s.%s where pg_catalog.random() <= %.2f",
        quote_identifier(sampleSchemaName),
        quote_identifier(sampleTableName),
        sampleRate);

    spec.memorycontext = speccontext;
    spec.tupDesc = onerel->rd_att;
    spec.num_sample = 0;
    u_sess->analyze_cxt.is_under_analyze = true;
    spi_exec_with_callback(DestSPI, str.data, false, 0, true, (void (*)(void*))spi_callback_get_sample_rows, &spec);
    u_sess->analyze_cxt.is_under_analyze = false;

    *num_sample_rows = spec.num_sample;
    *samplerows = spec.samplerows;

    pfree_ext(str.data);
    pfree_ext(sampleTableName);
    pfree_ext(sampleSchemaName);
}

/**
 * @Description: Construct and execute query to get sample rows
 *				for index columns or tsvetor column.
 *
 * @in speccontext: memory context for get sample rows
 * @in tableidx: identify the relation type for analyze
 * @in onerel: the relation for current table
 * @in vacstmt: the statment for analyze or vacuum command.
 * @out num_sample_rows: the result num of sample rows
 * @out samplerows: all the result sample rows
 * @return -  void
 */
static void get_sample_rows_for_query(
    MemoryContext speccontext, Relation onerel, VacuumStmt* vacstmt, int64* num_sample_rows, HeapTuple** samplerows)
{
    int save_default_statistics_target;
    double save_sampleRate;

    /* Save original guc and samplerate for compute new sample rate. */
    save_default_statistics_target = default_statistics_target;
    save_sampleRate = vacstmt->pstGlobalStatEx[vacstmt->tableidx].sampleRate;
    default_statistics_target = 100;

    (void)compute_sample_size(vacstmt, 0, NULL, onerel->rd_id, vacstmt->tableidx);

    /* Reset original guc and samplerate */
    default_statistics_target = save_default_statistics_target;

    get_sample_rows_utility(
        speccontext, onerel, vacstmt->pstGlobalStatEx[vacstmt->tableidx].sampleRate, num_sample_rows, samplerows);

    vacstmt->pstGlobalStatEx[vacstmt->tableidx].sampleRate = save_sampleRate;
}

/*
 * analyze_index_sample_rows: decide if need sample rows for index or not. construct and
 *							execute query to get sample rows if there are index columns.
 *
 * Parameters:
 *	@in speccontext: memory context for each column
 *	@in rel: the relation for analyze
 *	@in vacstmt: the statment for analyze or vacuum command.
 *	@in nindexes: the num of index for analyze
 *	@in indexdata: per-index data for analyze
 *	@out num_sample_rows: the result num of sample rows
 *	@out samplerows: all the result sample rows
 *
 * Returns: bool(need sample row for index or not)
 */
static bool analyze_index_sample_rows(MemoryContext speccontext, Relation rel, VacuumStmt* vacstmt, int nindexes,
    AnlIndexData* indexdata, int64* num_sample_rows, HeapTuple** samplerows)
{
    bool need_sample = false;

    for (int ind = 0; ind < nindexes; ind++) {
        AnlIndexData* thisdata = &indexdata[ind];
        IndexInfo* indexInfo = thisdata->indexInfo;
        int attr_cnt = thisdata->attr_cnt;

        /* Ignore index if no columns to analyze and not partial */
        if (attr_cnt != 0 || indexInfo->ii_Predicate != NIL) {
            need_sample = true;
            break;
        }
    }

    /*
     * If there are specific attrs in current relation, it have got sample rows before,
     * so we should not get sample rows again.
     */
    if (need_sample && *num_sample_rows == 0) {
        get_sample_rows_for_query(speccontext, rel, vacstmt, num_sample_rows, samplerows);

        /* If there is no sample rows, we don't need compute statistic for index any more. */
        if (*num_sample_rows == 0)
            need_sample = false;
    }

    return need_sample;
}

/*
 * set_stats_dndistinct: compute dndistinct value and set to statistic.
 *
 * Parameters:
 *	@in stats: statistic info of one attribute for analyze
 *	@in vacstmt: the statment for analyze or vacuum command
 *	@in tableidx: identify the relation type for analyze
 *
 * Returns: bool(need sample row for index or not)
 */
static void set_stats_dndistinct(VacAttrStats* stats, VacuumStmt* vacstmt, int tableidx)
{
    double distinct, dndistinct, dntotalrows;

    /* Get real number of data nodes for local and global distinct conversion */
    Oid reloid = stats->attrs[0]->attrelid;
    char relkind = get_rel_relkind(reloid);
    unsigned int num_datanodes = ng_get_baserel_num_data_nodes(reloid, relkind);

    if (!(IS_PGXC_COORDINATOR && !IsConnFromCoord()) || (vacstmt->pstGlobalStatEx[tableidx].totalRowCnts <= 0)) {
        return;
    }

    /* Get absolute global distinct value. */
    distinct = (stats->stadistinct < 0.0) ? (-stats->stadistinct * vacstmt->pstGlobalStatEx[tableidx].totalRowCnts)
                                          : stats->stadistinct;

    /*
     * Estimate dndistinct with poisson for
     * (1) foreign table
     * (2) extended statistics (when distribute key is not contained in multi-column)
     */
    if (vacstmt->isForeignTables ||
        (stats->num_attrs > 1 && (!es_is_distributekey_contained_in_multi_column(reloid, stats)))) {
        vacstmt->pstGlobalStatEx[tableidx].dndistinct[stats->attrs[0]->attnum - 1] =
            Min(ceil(NUM_DISTINCT_GTL_FOR_POISSON(
                    distinct, vacstmt->pstGlobalStatEx[tableidx].totalRowCnts, num_datanodes, 1)),
                distinct);
    }
    if (vacstmt->pstGlobalStatEx[tableidx].attnum < stats->attrs[0]->attnum || stats->attrs[0]->attnum <= 0) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("The column info is changed, Do analyze again for the relation: %s.", 
                    vacstmt->relation->relname)));
    }
    if (vacstmt->pstGlobalStatEx[tableidx].dndistinct[stats->attrs[0]->attnum - 1]  == 0.0) {
        return;
    }

    dndistinct = vacstmt->pstGlobalStatEx[tableidx].dndistinct[stats->attrs[0]->attnum - 1];

    /* there is no tuples on datanode1, compute dndistinct as distinct / u_sess->pgxc_cxt.NumDataNodes. */
    if (vacstmt->pstGlobalStatEx[tableidx].dn1totalRowCnts == 0) {
        stats->stadndistinct = dndistinct;
        return;
    }

    /* Convert to absolute value. */
    if (dndistinct < 0.0) {
        dndistinct = -dndistinct * vacstmt->pstGlobalStatEx[tableidx].dn1totalRowCnts;
    }

    /*
     * The dndistinct may be inaccurately if default_statistics_target is negtive
     * and target rows limited by work_mem, so minimume of dndistinct
     * can't less than stadistinct / num_datanodes.
     */
    stats->stadndistinct = Max(dndistinct, ceil(distinct / num_datanodes));
    dntotalrows = (dndistinct > ceil(distinct / num_datanodes))
                      ? vacstmt->pstGlobalStatEx[tableidx].dn1totalRowCnts
                      : ceil(vacstmt->pstGlobalStatEx[tableidx].totalRowCnts / num_datanodes);

    /* Convert to relative value. */
    if (stats->stadndistinct > 0.1 * dntotalrows) {
        stats->stadndistinct = -(stats->stadndistinct / dntotalrows);

        if (stats->stadndistinct < -1.0) {
            stats->stadndistinct = -1.0;
        }
    }
}

/*
 * Description: Generates a table name for the auxiliary temp table that may be created during ANALYZE.
 *
 * Parameters:
 * 	@in relationOid: relation oid
 * 	@in attname: attribute name.
 *
 * Returns:
 * 	sample table name. This must be pfree'd by the caller.
 */
static char* temporarySampleTableName(Oid relationOid, AnalyzeSampleTableSpecInfo* spec)
{
    char tmpname[NAMEDATALEN];
    errno_t ret;

    /* In 'if', for temp table of each column or multi-column */
    if (spec != NULL) {
        for (unsigned int i = 0; i < spec->stats->num_attrs; ++i) {
            ret = snprintf_s(spec->v_alias[i],
                NAMEDATALEN,
                NAMEDATALEN - 1,
                "v_%u_%lu_%ld_%d",
                relationOid,
                u_sess->debug_query_id,
                GetCurrentTimestamp(),
                spec->stats->attrs[i]->attnum);
            securec_check_ss(ret, "\0", "\0");
        }

        /* For single column and multi-column respectively */
        if (spec->stats->num_attrs == 1) {
            ret = snprintf_s(tmpname,
                NAMEDATALEN,
                NAMEDATALEN - 1,
                "%s_%u_%lu_%ld_s%d",
                ANALYZE_TEMP_TABLE_PREFIX,
                relationOid,
                u_sess->debug_query_id,
                GetCurrentTimestamp(),
                spec->stats->attrs[0]->attnum);
        } else {
            ret = snprintf_s(tmpname,
                NAMEDATALEN,
                NAMEDATALEN - 1,
                "%s_%u_%lu_%ld_m%d_m%d",
                ANALYZE_TEMP_TABLE_PREFIX,
                relationOid,
                u_sess->debug_query_id,
                GetCurrentTimestamp(),
                spec->stats->attrs[0]->attnum,
                spec->stats->attrs[1]->attnum);
        }
    } else {
        /* In 'else', for temp table of sample table */
        ret = snprintf_s(tmpname,
            NAMEDATALEN,
            NAMEDATALEN - 1,
            "%s_%u_%lu_%ld",
            ANALYZE_TEMP_TABLE_PREFIX,
            relationOid,
            u_sess->debug_query_id,
            GetCurrentTimestamp());
    }
    securec_check_ss(ret, "\0", "\0");

    return pstrdup(tmpname);
}

/*
 * Description: This method builds a sampled version of the given relation. The sampled
 *      version is created in a temp namespace. Note that ANALYZE can be
 *      executed only by the database or table user. It is safe to assume that the database
 *      user has permissions to create temp tables. The sampling is done by
 *      using the random() built-in function.
 *
 * Parameters:
 *  @in relid: relation to be sampled
 *  @in spec: the sample info of special attribute for compute statistic
 *  @in indexes: indexes of columns
 *  @in twocol_tmptable_name: temp table name of P(x1, x2) table
 *  @in left_tmptable_name: temp table name of P(x1) table
 *  @in right_tmptable_name: temp table name of P(x2) table
 *
 * Returns: temp table name (char*)
 */
char* build_temptable_sample_agg_joined(Oid relid, AnalyzeSampleTableSpecInfo* spec,
    const uint32 *indexes, const char *twocol_tmptable_name, const char *leftcol_tmptable_name,
    const char *rightcol_tmptable_name, int num_index)
{
    StringInfoData str;
    initStringInfo(&str);
    char* sampleTableName = NULL;
    sampleTableName = temporarySampleTableName(relid, NULL);

#ifdef ENABLE_MULTIPLE_NODES
    appendStringInfo(&str,
        "set query_dop = 4; create temp table %s ",
        quote_identifier(sampleTableName));
#else
    appendStringInfo(&str,
        " create temp table %s ",
        quote_identifier(sampleTableName));
#endif /* ENABLE_MULTIPLE_NODES */
 
    if (es_is_type_supported_by_cstore(spec->stats)) {
        if (g_instance.attr.attr_storage.enable_mix_replication || (!IS_DN_MULTI_STANDYS_MODE())) {
            appendStringInfo(&str, "with(orientation=column) ");
        }
    }
    appendStringInfo(&str, "as (select ");
    appendStringInfo(&str,
        "%s.v_count x1x2, %s.v_count x1, %s.v_count x2 from %s, %s, %s where %s.%s=%s.%s and %s.%s=%s.%s);",
        quote_identifier(twocol_tmptable_name),
        quote_identifier(leftcol_tmptable_name),
        quote_identifier(rightcol_tmptable_name),
        quote_identifier(twocol_tmptable_name),
        quote_identifier(leftcol_tmptable_name),
        quote_identifier(rightcol_tmptable_name),
        quote_identifier(twocol_tmptable_name),
        get_column_name_alias(spec, ES_COLUMN_ALIAS, indexes, 1),
        quote_identifier(leftcol_tmptable_name),
        get_column_name_alias(spec, ES_COLUMN_ALIAS, indexes, 1),
        quote_identifier(twocol_tmptable_name),
        get_column_name_alias(spec, ES_COLUMN_ALIAS, indexes+1, 1),
        quote_identifier(rightcol_tmptable_name),
        get_column_name_alias(spec, ES_COLUMN_ALIAS, indexes+1, 1));
    
#ifdef ENABLE_MULTIPLE_NODES
    int saved_query_dop = u_sess->opt_cxt.query_dop;
    appendStringInfo(&str, "set query_dop = %d", saved_query_dop);
#endif /* ENABLE_MULTIPLE_NODES */
    elog(ES_LOGLEVEL, "[Sample Table Query] %s", str.data);
 
    spi_exec_with_callback(DestSPI, str.data, false, 0, false, (void (*)(void*))NULL, NULL);
    pfree_ext(str.data);
    return sampleTableName;
}
 
/*
 * Description: This method builds a sampled version of the given relation. The sampled
 *      version is created in a temp namespace. Note that ANALYZE can be
 *      executed only by the database or table user. It is safe to assume that the database
 *      user has permissions to create temp tables. The sampling is done by
 *      using the random() built-in function.
 *
 * Parameters:
 *  @in relid: relation to be sampled
 *  @in analyzemode: the table type which collect statistics information
 *  @in inh: is inherit table or not
 *  @in vacstmt: the statment for analyze or vacuum command
 *  @in spec: the sample info of special attribute for compute statistic
 *  @in indexes: indexes of columns
 *  @in num_index: number of columns
 *
 * Returns: temp table name (char*)
 */
char* build_temptable_sample_agg(Oid relid, AnalyzeMode analyzemode, bool inh,
    VacuumStmt* vacstmt, AnalyzeSampleTableSpecInfo* spec, const uint32 *indexes,
    int num_index)
{
    StringInfoData str;
    const char* schemaName = NULL;
    const char* tableName = NULL;
    char* sampleTableName = NULL;
    Relation onerel = NULL;
    Relation mianrel = NULL;
 
    DEBUG_MOD_START_TIMER(MOD_AUTOVAC);
    onerel = relation_open(relid, AccessShareLock);
 
    schemaName = get_namespace_name(get_rel_namespace(relid));
    tableName = get_rel_name(relid);
 
    initStringInfo(&str);
 
    /* Generate temporary sample table name. */
    /* Spec alias has been set before */
    sampleTableName = temporarySampleTableName(relid, NULL);
 
    /*
        * This query orders the table by rank on the value and chooses values that occur every
        * 'bucketSize' interval. It also chooses the maximum value from the relation. It then
        * removes duplicate values and orders the values in ascending order. This corresponds to
        * the histogram.
        * We should set hashagg_table_size before query because distinct value of current attribute
        * may be not accurate which result in the performance of query degread.
        */
#ifdef ENABLE_MULTIPLE_NODES
    appendStringInfo(&str,
        "set query_dop = 4; create temp table %s ",
        quote_identifier(sampleTableName));
#else
    appendStringInfo(&str,
        " create temp table %s ",
        quote_identifier(sampleTableName));
#endif   /* ENABLE_MULTIPLE_NODES */

    if (es_is_type_supported_by_cstore(spec->stats)) {
        if (g_instance.attr.attr_storage.enable_mix_replication || (!IS_DN_MULTI_STANDYS_MODE())) {
            appendStringInfo(&str, "with(orientation=column) ");
        }
    }
 
#ifdef ENABLE_MULTIPLE_NODES
    if (es_is_type_distributable(spec->stats)) {
        appendStringInfo(
            &str, "distribute by hash(%s) as (select ",
            get_column_name_alias(spec, ES_COLUMN_ALIAS, indexes, num_index));
    } else {
        /* We should add a new column for distribute if the attribute is not support hash. */
        appendStringInfo(&str, "distribute by hash(i) as (select (random()*32767)::int2 as i, ");
    }
#else
    appendStringInfo(&str, "as (select ");
#endif   /* ENABLE_MULTIPLE_NODES */
 
    if (vacstmt->tmpSampleTblNameList) {
        Relation temp_relation;
 
        const char* tmpSampleTableName = get_sample_tblname(analyzemode, vacstmt->tmpSampleTblNameList);
        temp_relation = relation_open(RelnameGetRelid(tmpSampleTableName), AccessShareLock);

        appendStringInfo(&str,
            "%s, count(*) as v_count "
            "from %s %s group by %s);",
            get_column_name_alias(spec, ES_COLUMN_NAME | ES_COLUMN_ALIAS, indexes, num_index),
            (!inh && RelationIsPAXFormat(onerel)) ? "only" : "",
            quote_identifier(tmpSampleTableName),
            get_column_name_alias(spec, ES_COLUMN_ALIAS, indexes, num_index));
 
        /* Switch to relowner of temp table. */
        SetUserIdAndSecContext(temp_relation->rd_rel->relowner, 0);
        relation_close(temp_relation, AccessShareLock);
    } else {
        appendStringInfo(&str,
            "%s, count(*) as v_count "
            "from %s %s.%s where random() <= %.2f group by %s);",
            get_column_name_alias(spec, ES_COLUMN_NAME | ES_COLUMN_ALIAS, indexes, num_index),
            (!inh && RelationIsPAXFormat(onerel)) ? "only" : "",
            quote_identifier(schemaName),
            quote_identifier(tableName),
            vacstmt->pstGlobalStatEx[vacstmt->tableidx].sampleRate,
            get_column_name_alias(spec, ES_COLUMN_ALIAS, indexes, num_index));
 
        SetUserIdAndSecContext(onerel->rd_rel->relowner, 0);
    }
 
#ifdef ENABLE_MULTIPLE_NODES
    int saved_query_dop = u_sess->opt_cxt.query_dop;
    appendStringInfo(&str, "set query_dop = %d", saved_query_dop);
#endif

    if (mianrel)
        relation_close(mianrel, AccessShareLock);
    relation_close(onerel, AccessShareLock);
 
    elog(ES_LOGLEVEL, "[Sample Table Query] %s", str.data);
 
    spi_exec_with_callback(DestSPI, str.data, false, 0, false, (void (*)(void*))NULL, NULL);
 
    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "Created sample table %s success. querystring: %s", sampleTableName, str.data);
 
    pfree_ext(str.data);
    pfree_ext(tableName);
    pfree_ext(schemaName);
 
    return sampleTableName;
}


/*
 * Description: This method builds a sampled version of the given relation. The sampled
 * 		version is created in a temp namespace. Note that ANALYZE can be
 * 		executed only by the database or table user. It is safe to assume that the database
 * 		user has permissions to create temp tables. The sampling is done by
 * 		using the random() built-in function.
 *
 * Parameters:
 *	@in relid: relation to be sampled
 *	@in type: identify create temp table for attribute or table
 *	@in analyzemode: the table type which collect statistics information
 *	@in inh: is inherit table or not
 *	@in vacstmt: the statment for analyze or vacuum command
 *	@in spec: the sample info of special attribute for compute statistic
 *
 * Returns: temp table name (char*)
 */
char* buildTempSampleTable(Oid relid, Oid mian_relid, TempSmpleTblType type, AnalyzeMode analyzemode, bool inh,
    VacuumStmt* vacstmt, AnalyzeSampleTableSpecInfo* spec)
{
    StringInfoData str;
    const char* schemaName = NULL;
    const char* tableName = NULL;
    char* sampleTableName = NULL;
    Relation onerel = NULL;
    Relation mianrel = NULL;

    DEBUG_MOD_START_TIMER(MOD_AUTOVAC);
    onerel = relation_open(relid, AccessShareLock);
    if (OidIsValid(mian_relid)) {
        AssertEreport(TempSmpleTblType_Table == type, MOD_OPT, "");
        mianrel = relation_open(mian_relid, AccessShareLock);
        schemaName = get_namespace_name(get_rel_namespace(mian_relid));
        tableName = get_rel_name(mian_relid);
    } else {
        schemaName = get_namespace_name(get_rel_namespace(relid));
        tableName = get_rel_name(relid);
    }
    initStringInfo(&str);

    if (TempSmpleTblType_Attrbute == type) {
        int saved_query_dop = u_sess->opt_cxt.query_dop;

        /* Generate temporary sample table name. */
        sampleTableName = temporarySampleTableName(relid, spec);

        /*
         * This query orders the table by rank on the value and chooses values that occur every
         * 'bucketSize' interval. It also chooses the maximum value from the relation. It then
         * removes duplicate values and orders the values in ascending order. This corresponds to
         * the histogram.
         * We should set hashagg_table_size before query because distinct value of current attribute
         * may be not accurate which result in the performance of query degread.
         */
        appendStringInfo(&str,
            "%s create temp table %s ",
            IS_SINGLE_NODE ? "" : "set query_dop = 4;",
            quote_identifier(sampleTableName));

        if (es_is_type_supported_by_cstore(spec->stats)) {
            if (g_instance.attr.attr_storage.enable_mix_replication || (!IS_DN_MULTI_STANDYS_MODE())) {
                appendStringInfo(&str, "with(orientation=column) ");
            }
        }

        if (IS_SINGLE_NODE) {
            appendStringInfo(&str, "as (select ");
        } else if (es_is_type_distributable(spec->stats)) {
            appendStringInfo(
                &str, "distribute by hash(%s) as (select ", es_get_column_name_alias(spec, ES_COLUMN_ALIAS));
        } else {
            /* We should add a new column for distribute if the attribute is not support hash. */
            appendStringInfo(&str, "distribute by hash(i) as (select (pg_catalog.random()*32767)::int2 as i, ");
        }

        if (vacstmt->tmpSampleTblNameList) {
            Relation temp_relation;
            const char* tmpSampleTableName = get_sample_tblname(analyzemode, vacstmt->tmpSampleTblNameList);
            temp_relation = relation_open(RelnameGetRelid(tmpSampleTableName), AccessShareLock);

            appendStringInfo(&str,
                "%s, count(*) as v_count "
                "from %s %s group by %s);",
                es_get_column_name_alias(spec, ES_COLUMN_NAME | ES_COLUMN_ALIAS),
                (!inh && RelationIsPAXFormat(onerel)) ? "only" : "",
                quote_identifier(tmpSampleTableName),
                es_get_column_name_alias(spec, ES_COLUMN_ALIAS));

            /* Switch to relowner of temp table. */
            SetUserIdAndSecContext(temp_relation->rd_rel->relowner, 0);
            relation_close(temp_relation, AccessShareLock);
        } else {
            appendStringInfo(&str,
                "%s, pg_catalog.count(*) as v_count "
                "from %s %s.%s where pg_catalog.random() <= %.2f group by %s);",
                es_get_column_name_alias(spec, ES_COLUMN_NAME | ES_COLUMN_ALIAS),
                (!inh && RelationIsPAXFormat(onerel)) ? "only" : "",
                quote_identifier(schemaName),
                quote_identifier(tableName),
                vacstmt->pstGlobalStatEx[vacstmt->tableidx].sampleRate,
                es_get_column_name_alias(spec, ES_COLUMN_ALIAS));

            SetUserIdAndSecContext(onerel->rd_rel->relowner, 0);
        }

        appendStringInfo(&str, "set query_dop = %d", saved_query_dop);
    } else {
        char* group_name = get_pgxc_groupname(get_pgxc_class_groupoid(relid));
        const char* redistribution_group_name = PgxcGroupGetInRedistributionGroup();

        /*
         * Unable to create temp table on old installation group while
         * in cluster-resizing under analyzing.
         */
        if (redistribution_group_name != NULL && group_name != NULL &&
            strcmp(group_name, redistribution_group_name) == 0) {
            DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, 
                             "Unable to create temp table on old installation group \"%s\" while in cluster-resizing "
                             "under analyzing for table \"%s\".",
                group_name,
                quote_identifier(tableName));
            relation_close(onerel, AccessShareLock);
            pfree_ext(tableName);
            pfree_ext(schemaName);
            return NULL;
        }

        /* Generate temporary sample table name. */
        sampleTableName = temporarySampleTableName(relid, NULL);
        if (NULL == group_name) {
            appendStringInfo(&str,
                "create temp table %s (like %s.%s INCLUDING DISTRIBUTION);",
                quote_identifier(sampleTableName),
                quote_identifier(schemaName),
                quote_identifier(tableName));
        } else {
            appendStringInfo(&str,
                "create temp table %s (like %s.%s INCLUDING DISTRIBUTION) to group %s;",
                quote_identifier(sampleTableName),
                quote_identifier(schemaName),
                quote_identifier(tableName),
                quote_identifier(group_name));

            pfree_ext(group_name);
        }
    }

    if (mianrel)
        relation_close(mianrel, AccessShareLock);
    relation_close(onerel, AccessShareLock);

    elog(ES_LOGLEVEL, "[Sample Table Query] %s", str.data);

    u_sess->analyze_cxt.is_under_analyze = true;
    spi_exec_with_callback(DestSPI, str.data, false, 0, false, (void (*)(void*))NULL, NULL);
    u_sess->analyze_cxt.is_under_analyze = false;

    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "Created sample table %s success. querystring: %s", sampleTableName, str.data);

    pfree_ext(str.data);
    pfree_ext(tableName);
    pfree_ext(schemaName);

    return sampleTableName;
}

/*
 * Description: Drop the temp table created during ANALYZE.
 *
 * Parameters:
 *	@in tableName: temp table name
 *
 * Returns: void
 */
void dropSampleTable(const char* tableName)
{
    StringInfoData str;

    initStringInfo(&str);
    appendStringInfo(&str, "drop table %s", quote_identifier(tableName));

    spi_exec_with_callback(DestSPI, str.data, false, 0, false, (void (*)(void*))NULL, NULL);
    pfree_ext(str.data);

    elog(DEBUG1, "ANALYZE dropping sample table: %s", tableName);
}

/*
 * Description: This routine creates an array from one of the result attributes after an SPI call.
 * 			It allocates this array in the specified context. Note that, in general, the allocation
 * 			context must not the SPI context because that is likely to get cleaned out soon.
 *
 * Parameters:
 *	@in attrno: attribute to flatten into an array (1 based)
 *	@in memory_context: memory context for statistic of column
 *
 * Returns: array of attribute type
 */
static ArrayBuildState* spi_get_result_array(int attrno, MemoryContext memory_context)
{
    ArrayBuildState* result = NULL;
    Form_pg_attribute attribute = &SPI_tuptable->tupdesc->attrs[attrno];

    AssertEreport(attribute, MOD_OPT, "");

    for (uint32 i = 0; i < SPI_processed; i++) {
        Datum dValue = 0;
        bool isnull = false;
        dValue = tableam_tops_tuple_getattr(SPI_tuptable->vals[i], attrno + 1, SPI_tuptable->tupdesc, &isnull);

        /**
         * Add this value to the result array.
         */
        result = accumArrayResult(result, dValue, isnull, attribute->atttypid, memory_context);
    }

    return result;
}

/*
 * Description: A callback function for use with spiExecuteWithCallback.
 *			Reads each column of output into an array The number of arrays,
 *			and the output location are determined by treating *clientData.
 *
 * Parameters:
 *	@in clientData: as a EachResultColumnAsArraySpec
 *
 * Returns: void
 */
static void spi_callback_get_multicolarray(void* clientData)
{
    AnalyzeResultMultiColAsArraySpecInfo* spec = (AnalyzeResultMultiColAsArraySpecInfo*)clientData;
    ArrayBuildState** out = spec->output;

    AssertEreport(SPI_tuptable != NULL, MOD_OPT, "");
    AssertEreport(SPI_tuptable->tupdesc, MOD_OPT, "");

    /*
     * Save the copy of tuple desc for check whether the attribute of received from datanode
     * is the same with local attribute for analyzed.
     */
    if (spec->num_rows == 0) {
        MemoryContext oldcontext = MemoryContextSwitchTo(spec->memoryContext);
        spec->spi_tupDesc = CreateTupleDescCopy(SPI_tuptable->tupdesc);
        (void)MemoryContextSwitchTo(oldcontext);
    }

    if (SPI_processed == 0)
        return;

    if (spec->compute_histgram) {
        compute_histgram_internal(spec);
        spec->num_rows += SPI_processed;

        return;
    }

    for (int i = 0; i < spec->num_cols; i++) {
        *out = spi_get_result_array(i, spec->memoryContext);
        AssertEreport(*out, MOD_OPT, "");
        out++;
    }

    spec->num_rows += SPI_processed;
}

/*
 * Description: A callback function for use with spiExecuteWithCallback.
 *			Asserts that exactly one row was returned.
 *			Gets the row's first column as a float, using 0.0 if the value is null.
 *
 * Parameters:
 *	@in clientData: as a EachResultColumnAsArraySpec
 *
 * Returns: void
 */
static void spi_callback_get_singlerow(void* clientData)
{
    Datum datum_f;
    bool isnull = false;
    int64* out = (int64*)clientData;

    /* There is no null value. */
    if (0 == SPI_processed) {
        *out = 0;
    } else {
        AssertEreport(SPI_tuptable != NULL, MOD_OPT, "must have result");       /* must have result */
        AssertEreport(SPI_processed == 1, MOD_OPT, "we expect only one tuple"); /* we expect only one tuple. */
        AssertEreport(SPI_tuptable->tupdesc->attrs[0].atttypid == INT8OID, MOD_OPT, "");

        datum_f = tableam_tops_tuple_getattr(SPI_tuptable->vals[0], 1, SPI_tuptable->tupdesc, &isnull);

        if (isnull) {
            *out = 0;
        } else {
            *out = DatumGetInt64(datum_f);
        }
    }
}

/*
 * Description: Computes the total rows in sample table.
 *
 * Parameters:
 *	@in tableName: temp table name
 *	@in spec: the sample info of special attribute for compute statistic
 *
 * Returns: void
 */
static void analyze_compute_samplerows(const char* tableName, AnalyzeSampleTableSpecInfo* spec)
{
    int64 samplerows = 0;
    StringInfoData str;
    initStringInfo(&str);
    appendStringInfo(&str, "select pg_catalog.sum(v_count)::int8 as samplerows from %s;", quote_identifier(tableName));

    elog(ES_LOGLEVEL, "[Query to compute samplerows] : %s", str.data);

    DEBUG_MOD_START_TIMER(MOD_AUTOVAC);
    spi_exec_with_callback(DestSPI, str.data, false, 0, true, (void (*)(void*))spi_callback_get_singlerow, &samplerows);
    pfree_ext(str.data);

    spec->samplerows = samplerows;

    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "Compute samplerows = %ld for table %s success.", spec->samplerows, tableName);
}

/*
 * Description: Computes the absolute number of NULLs in the sample table.
 *
 * Parameters:
 *	@in tableName: temp table name
 *	@in spec: the sample info of special attribute for compute statistic
 *
 * Returns: void
 */
static void analyze_compute_nullcount(const char* tableName, AnalyzeSampleTableSpecInfo* spec)
{
    int64 null_count = 0;

    DEBUG_MOD_START_TIMER(MOD_AUTOVAC);
    if (es_is_not_null(spec)) {
        spec->stats->stanullfrac = 0.0;
    } else {
        StringInfoData str;
        initStringInfo(&str);

        /*
         * For multi-column statistics, the definition of composit NULL values are all
         * elements are NULL, and also use this result to compute nullfrac
         */
        appendStringInfo(&str,
            "select pg_catalog.sum(v_count)::int8 from %s where %s limit 1;",
            quote_identifier(tableName),
            es_get_column_name_alias(spec, ES_COLUMN_ALIAS, " and ", "", " is null "));

        elog(ES_LOGLEVEL, "[Query to compute null count] : %s", str.data);

        spi_exec_with_callback(
            DestSPI, str.data, false, 0, true, (void (*)(void*))spi_callback_get_singlerow, &null_count);
        pfree_ext(str.data);

        spec->stats->stanullfrac = Min((null_count * 1.0) / spec->samplerows, 1.0);
    }

    spec->null_cnt = null_count;
    spec->nonnull_cnt = spec->samplerows - spec->null_cnt;

    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "Compute nullfrac = %.6f for table %s success"
                     "null_count = %ld, samplerows = %ld.",
        spec->stats->stanullfrac,
        tableName,
        null_count,
        spec->samplerows);
}

/*
 * Description: Computes average width of an attribute. This uses the built-in pg_column_size.
 *
 * Parameters:
 *	@in tableName: temp table name
 *	@in spec: the sample info of special attribute for compute statistic
 *
 * Returns: void
 */
static void analyze_compute_avgwidth(const char* tableName, AnalyzeSampleTableSpecInfo* spec)
{
    DEBUG_MOD_START_TIMER(MOD_AUTOVAC);
    if (spec->is_varwidth) {
        if (spec->nonnull_cnt == 0 && spec->null_cnt > 0) {
            spec->stats->stawidth = 0;
        } else {
            int64 avgwidth = 0;
            StringInfoData str;
            initStringInfo(&str);
            appendStringInfo(&str,
                "select (pg_catalog.sum((%s) * v_count)::int8 / pg_catalog.sum(v_count)::int8)::int8 "
                "from %s where %s;",
                es_get_column_name_alias(spec, ES_COLUMN_ALIAS, " + ", "pg_catalog.pg_column_size(", ")"),
                quote_identifier(tableName),
                es_get_column_name_alias(spec, ES_COLUMN_ALIAS, " and ", "", " is not null "));

            elog(ES_LOGLEVEL, "[Query to compute avgwidth] : %s", str.data);

            spi_exec_with_callback(
                DestSPI, str.data, false, 0, true, (void (*)(void*))spi_callback_get_singlerow, &avgwidth);
            pfree_ext(str.data);

            spec->stats->stawidth = avgwidth;
        }
    } else {
        spec->stats->stawidth = 0;
        for (unsigned int i = 0; i < spec->stats->num_attrs; ++i) {
            spec->stats->stawidth += spec->stats->attrs[i]->attlen;
        }
    }

    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "Compute avgwidth = %d for table %s success.", spec->stats->stawidth, tableName);
}

/*
 * Description: Compute NDistinct of a column using sample table.
 *
 * Parameters:
 *	@in tableName: temp table name
 *	@in spec: the sample info of special attribute for compute statistic
 *
 * Returns: void
 */
static void analyze_distinct(const char* tableName, AnalyzeSampleTableSpecInfo* spec)
{
    StringInfoData str;
    ArrayBuildState* spiResult[DEFAULT_COLUMN_NUM];
    AnalyzeResultMultiColAsArraySpecInfo result;

    DEBUG_MOD_START_TIMER(MOD_AUTOVAC);
    initStringInfo(&str);
    appendStringInfo(&str,
        "select pg_catalog.count(*) as ndistinct, "
        "(select pg_catalog.count(*) from %s where v_count > 1) as nmultiple "
        "from %s where NOT (%s);",
        quote_identifier(tableName),
        quote_identifier(tableName),
        es_get_column_name_alias(spec, ES_COLUMN_ALIAS, " and ", "", " is null "));

    elog(ES_LOGLEVEL, "[Query to compute distinct] : %s", str.data);

    result.num_cols = DEFAULT_COLUMN_NUM;
    result.output = spiResult;
    result.memoryContext = CurrentMemoryContext;
    result.compute_histgram = false;
    result.num_rows = 0;
    spi_exec_with_callback(DestSPI, str.data, false, 0, true, (void (*)(void*))spi_callback_get_multicolarray, &result);
    pfree_ext(str.data);

    /* We won't come here if the num of non-null value is zero. */
    if (result.num_rows > 0) {
        spec->ndistinct = spiResult[0]->dvalues[0];
        spec->nmultiple = spiResult[1]->dvalues[0];

        if (spec->nonnull_cnt > 0) {
            analyze_compute_ndistinct(
                spec->nmultiple, spec->ndistinct, 0, spec->samplerows, spec->totalrows, spec->stats);
        } else {
            spec->stats->stadistinct = 0.0; /* "unknown" */
        }
    }

    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, 
                         "Compute global_ndistinct = %.0f for table %s success, "
                         "sample_ndistinct = %.0f, nmultiple = %ld, nonnull_cnt = %ld, "
                         "samplerows = %ld, totalrows = %.0f.",
                         spec->stats->stadistinct,
                         tableName,
                         spec->ndistinct,
                         spec->nmultiple,
                         spec->nonnull_cnt,
                         spec->samplerows,
                         spec->totalrows);
}

/*
 * Description: Computes the most common values and their frequencies for relation.
 *
 * Parameters:
 *	@in slot_idx: index of statistic struct
 *	@in tableName: temp table name
 *	@in spec: the sample info of special attribute for compute statistic
 *
 * Returns: void
 */
static void analyze_compute_mcv(
    int* slot_idx, const char* tableName, AnalyzeSampleTableSpecInfo* spec, bool process_null)
{
    if (process_null) {
        /* Check if there is no remaining slot to hold null-mcv */
        if (*slot_idx >= STATISTIC_NUM_SLOTS) {
            elog(WARNING, "statistics read max stavalue slot");
            return;
        }

        /*
         * Check if the to-be analyzed multi-column is defined with NOT-NULL constraint so
         * that we can optimize out the null-mcv steps
         */
        bool mcv_not_null = true;
        for (int i = 0; i < (int)spec->stats->num_attrs; i++) {
            if (!spec->stats->attrs[i]->attnotnull) {
                mcv_not_null = false;
                break;
            }
        }

        if (mcv_not_null) {
            elog(DEBUG3, "not need to collect null-mcv as each attributes is NOT NULL");
            return;
        }
    }

    double mincount; /* the min count of mcv */
    ArrayBuildState** spiResult = (ArrayBuildState**)palloc0(sizeof(ArrayBuildState*) * (spec->stats->num_attrs + 1));
    StringInfoData str;
    AnalyzeResultMultiColAsArraySpecInfo result;

    DEBUG_MOD_START_TIMER(MOD_AUTOVAC);
    /* Compute real mincount for all sample rows of 2%. */
    mincount = compute_mcv_mincount(spec->samplerows, spec->ndistinct, spec->mcv_list.stattarget);
    /* We should restrict the minimum of mincount as MIN_MCV_COUNT. */
    mincount = Max(mincount, MIN_MCV_COUNT);

    initStringInfo(&str);

    if (process_null) {
        appendStringInfo(&str,
            "select %s, v_count from %s where not (%s) and not (%s)"
            "and v_count >= %.0f order by v_count desc, %s limit 100;",
            es_get_column_name_alias(spec, ES_COLUMN_ALIAS),
            quote_identifier(tableName),
            /* exclude all NULL */
            es_get_column_name_alias(spec, ES_COLUMN_ALIAS, " and ", "", " is null "),
            /* exclude all NOT NULL */
            es_get_column_name_alias(spec, ES_COLUMN_ALIAS, " and ", "", " is not null "),
            mincount,
            es_get_column_name_alias(spec, ES_COLUMN_ALIAS));
    } else {
        appendStringInfo(&str,
            "select %s, v_count from %s where %s"
            "and v_count >= %.0f order by v_count desc, %s limit 100;",
            es_get_column_name_alias(spec, ES_COLUMN_ALIAS),
            quote_identifier(tableName),
            es_get_column_name_alias(spec, ES_COLUMN_ALIAS, " and ", "", " is not null "),
            mincount,
            es_get_column_name_alias(spec, ES_COLUMN_ALIAS));
    }

    elog(ES_LOGLEVEL, "[Query to compute MCV] : %s", str.data);

    result.num_cols = spec->stats->num_attrs + 1;
    result.output = spiResult;
    result.memoryContext = CurrentMemoryContext;
    result.compute_histgram = false;
    result.num_rows = 0;
    result.spi_tupDesc = NULL;
    spi_exec_with_callback(DestSPI, str.data, false, 0, true, (void (*)(void*))spi_callback_get_multicolarray, &result);
    pfree_ext(str.data);

    spec->mcv_list.num_mcv = result.num_rows;

    /*
     * Check whether the attribute of received from datanode
     * is the same with local attribute for analyzed or not.
     */
    if (spec->stats->num_attrs == 1 && result.spi_tupDesc &&
        (result.spi_tupDesc->attrs[0].atttypid != spec->stats->attrs[0]->atttypid ||
            result.spi_tupDesc->attrs[0].atttypmod != spec->stats->attrs[0]->atttypmod ||
            result.spi_tupDesc->attrs[0].attlen != spec->stats->attrs[0]->attlen)) {
        ereport(WARNING,
            (errmsg("The tupleDesc analyzed on %s is different from tupleDesc which received from datanode "
                    "when computing mcv.",
                 g_instance.attr.attr_common.PGXCNodeName),
                errdetail("Attribute \"%s\" of type %s does not match corresponding attribute of type %s.",
                    NameStr(spec->stats->attrs[0]->attname),
                    format_type_be(spec->stats->attrs[0]->atttypid),
                    format_type_be(result.spi_tupDesc->attrs[0].atttypid))));
        FreeTupleDesc(result.spi_tupDesc);

        DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, "Compute MCV for table %s failed.", tableName);

        return;
    }

    /* Generate MCV slot entry */
    if (result.num_rows > 0) {
        Datum* mcv_values = NULL;
        bool* mcv_nulls = NULL;
        float4* mcv_freqs = NULL;
        MemoryContext old_context;

        /* Must copy the target values into analyze_context */
        old_context = MemoryContextSwitchTo(spec->stats->anl_context);
        mcv_values = (Datum*)palloc((result.num_rows * spec->stats->num_attrs) * sizeof(Datum));
        mcv_nulls = (bool*)palloc((result.num_rows * spec->stats->num_attrs) * sizeof(bool));
        mcv_freqs = (float4*)palloc(result.num_rows * sizeof(float4));
        for (int64 i = 0; i < result.num_rows; i++) {
            for (unsigned int j = 0; j < spec->stats->num_attrs; ++j) {
                int index = i + j * result.num_rows;

                mcv_values[index] = datumCopy(
                    spiResult[j]->dvalues[i], spec->stats->attrtype[j]->typbyval, spec->stats->attrtype[j]->typlen);
                mcv_nulls[index] = spiResult[j]->dnulls[i];
            }
            mcv_freqs[i] =
                (double)DatumGetInt64(spiResult[spec->stats->num_attrs]->dvalues[i]) / (double)spec->samplerows;
            spec->mcv_list.rows_mcv += DatumGetInt64(spiResult[spec->stats->num_attrs]->dvalues[i]);
        }
        (void)MemoryContextSwitchTo(old_context);

        if (process_null) {
            spec->stats->stakind[*slot_idx] = STATISTIC_KIND_NULL_MCV;
        } else {
            spec->stats->stakind[*slot_idx] = STATISTIC_KIND_MCV;
        }
        spec->stats->staop[*slot_idx] =
            (1 == spec->stats->num_attrs) ? ((StdAnalyzeData*)spec->stats->extra_data)->eqopr : 0;
        spec->stats->stanumbers[*slot_idx] = mcv_freqs;
        spec->stats->numnumbers[*slot_idx] = result.num_rows;
        spec->stats->stavalues[*slot_idx] = mcv_values;
        spec->stats->stanulls[*slot_idx] = mcv_nulls;
        spec->stats->numvalues[*slot_idx] = result.num_rows * spec->stats->num_attrs;

        /*
         * Accept the defaults for stats->statypid and others. They have
         * been set before we were called (see vacuum.h)
         */
        (*slot_idx)++;

        /*
         * Save the last mcv as the start of the histgram value.
         * We only collect histgram in sigle column scenario
         */
        spec->hist_list.start_value = datumCopy(spiResult[0]->dvalues[result.num_rows - 1],
            spec->stats->attrtype[0]->typbyval,
            spec->stats->attrtype[0]->typlen);
        spec->hist_list.start_value_count = spiResult[1]->dvalues[result.num_rows - 1];
    }

    pfree_ext(spiResult);

    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC, 
                        "Compute %d rows of MCV for table %s success.",
                        spec->mcv_list.num_mcv, tableName);
}

/*
 * Description: Get each tuples received from datanodes,
 *			saved in histogram list if the sum of count reach bucketsize.
 *
 * Parameters:
 *	@in spec: the sample info of special attribute for compute histogram
 *
 * Returns: void
 */
static void compute_histgram_internal(AnalyzeResultMultiColAsArraySpecInfo* spec)
{

#define VALUE_COLUMN 1
#define ROWCOUNT_COLUMN 2

    MemoryContext oldContext;
    int64 vcount;
    Datum histgramValue;
    TupleDesc tupDesc = spec->spi_tupDesc;
    bool isnull = false;
    int16 valueTyplen = 0;
    bool valueTypbyval = false;
    char valueTypalign = 0;

#define GET_DETOAST_HISTGRAM_VALUE(val)                                              \
    do {                                                                             \
        (val) = tableam_tops_tuple_getattr(SPI_tuptable->vals[i], VALUE_COLUMN, tupDesc, &isnull); \
        if (!isnull && !valueTypbyval) {                                             \
            oldContext = MemoryContextSwitchTo(u_sess->analyze_cxt.analyze_context); \
            if (valueTyplen < 0)                                                     \
                (val) = PointerGetDatum(PG_DETOAST_DATUM_COPY((val)));               \
            else                                                                     \
                (val) = datumCopy((val), valueTypbyval, valueTyplen);                \
            (void)MemoryContextSwitchTo(oldContext);                                       \
        }                                                                            \
    } while (0)

    get_typlenbyvalalign(tupDesc->attrs[VALUE_COLUMN - 1].atttypid, &valueTyplen, &valueTypbyval, &valueTypalign);

    for (uint32 i = 0; i < SPI_processed; i++) {
        vcount = tableam_tops_tuple_getattr(SPI_tuptable->vals[i], ROWCOUNT_COLUMN, tupDesc, &isnull);

        spec->hist_list.sum_count += vcount;
        spec->hist_list.cur_mcv_idx++;

        /*
         * Save the value to histgram list either the sum of count exceed
         * the bucketSize or the current value is the first or the last value.
         */
        if (spec->hist_list.num_hist < spec->hist_list.stattarget &&
            (spec->hist_list.num_hist == 0 || spec->hist_list.cur_mcv_idx == spec->non_mcv_num ||
                spec->hist_list.sum_count >= spec->hist_list.bucketSize)) {
            GET_DETOAST_HISTGRAM_VALUE(histgramValue);
            spec->hist_list.histitem[spec->hist_list.num_hist].value = histgramValue;
            spec->hist_list.histitem[spec->hist_list.num_hist].count = vcount;
            spec->hist_list.rows_hist += vcount;
            spec->hist_list.num_hist++;
            /*
             * compute remainder sum of count current value in order to
             * compute next histgram value.
             */
            spec->hist_list.sum_count = 0;
        }

        /*
         * The histgram value is the last value(as the max value),
         * save it to the last location of hist list.
         */
        if (spec->hist_list.num_hist >= spec->hist_list.stattarget &&
            spec->hist_list.cur_mcv_idx == spec->non_mcv_num) {
            GET_DETOAST_HISTGRAM_VALUE(histgramValue);
            spec->hist_list.histitem[spec->hist_list.num_hist - 1].value = histgramValue;
            spec->hist_list.histitem[spec->hist_list.num_hist - 1].count = vcount;
            spec->hist_list.rows_hist += vcount;
        }
    }
}

/*
 * Description: Save all the values in histogram list to statistic struct.
 *
 * Parameters:
 *	@in slot_idx: index of statistic struct
 *	@in hist_list: histogram list
 *	@in stats: statistic info of one attribute for analyze
 *
 * Returns: void
 */
static void compute_histgram_final(int* slot_idx, HistgramInfo* hist_list, VacAttrStats* stats)
{
    if (hist_list->num_hist < 2) {
        return;
    }

    Datum* hist_values = NULL;
    MemoryContext old_context;

    /* Must copy the target values into analyze_context */
    old_context = MemoryContextSwitchTo(stats->anl_context);
    hist_values = (Datum*)palloc(MAX_ATTR_HIST_STAT_TARGET * sizeof(Datum));

    /*
     * We should restrict and adjust the num of histgram
     * if there are more than 100 histgram values in hist_list.
     */
    for (int i = 0; i < hist_list->num_hist; i++) {
        hist_values[i] =
            datumCopy(hist_list->histitem[i].value, stats->attrtype[0]->typbyval, stats->attrtype[0]->typlen);
    }

    (void)MemoryContextSwitchTo(old_context);
    stats->stakind[*slot_idx] = STATISTIC_KIND_HISTOGRAM;
    stats->staop[*slot_idx] = ((StdAnalyzeData*)stats->extra_data)->ltopr;
    stats->stavalues[*slot_idx] = hist_values;
    stats->numvalues[*slot_idx] = hist_list->num_hist;

    /*
     * Accept the defaults for stats->statypid and others. They have
     * been set before we were called (see vacuum.h)
     */
    (*slot_idx)++;
}

/*
 * Description: Compute histogram entries for relation.
 *
 * Parameters:
 *	@in slot_idx: index of statistic struct
 *	@in tableName: temp table name
 *	@in spec: the sample info of special attribute for compute statistic
 *
 * Returns: void
 */
static void analyze_compute_histgram(int* slot_idx, const char* tableName, AnalyzeSampleTableSpecInfo* spec)
{
#define ANAYLZE_MAX_STAWIDTH 20
/*
 * Only save the length of 20 for histgram value if the type of attribute is
 * varchar/char/bpchar and the max width more than 20.
 */
#define CHAR_WIDTH_EXCEED_20(spec)                                                                           \
    (((VARCHAROID == (spec)->stats->attrs[0]->atttypid) || (CHAROID == (spec)->stats->attrs[0]->atttypid) || \
         (BPCHAROID == (spec)->stats->attrs[0]->atttypid)) &&                                                \
        (spec)->stats->stawidth >= ANAYLZE_MAX_STAWIDTH)

    int64 non_mcv_num = 0;
    int64 non_mcv_rows = 0; /* sum of rows of all the  most common values */
    StringInfoData str;
    ArrayBuildState* spiResult[2];
    AnalyzeResultMultiColAsArraySpecInfo result;
    bool is_attr_diff = false;

    DEBUG_MOD_START_TIMER(MOD_AUTOVAC);
    non_mcv_rows = spec->nonnull_cnt - spec->mcv_list.rows_mcv;
    non_mcv_num = spec->ndistinct - spec->mcv_list.num_mcv;

    if (non_mcv_num > 2) {
        if (non_mcv_num > spec->hist_list.stattarget) {
            spec->hist_list.bucketSize = rint(non_mcv_rows / spec->hist_list.stattarget);
        } else {
            spec->hist_list.bucketSize = 1;
        }

        initStringInfo(&str);

        if (!CHAR_WIDTH_EXCEED_20(spec)) {
            appendStringInfo(&str,
                "select %s, v_count from %s ",
                es_get_column_name_alias(spec, ES_COLUMN_ALIAS),
                quote_identifier(tableName));
        } else {
            appendStringInfo(&str,
                "select pg_catalog.substr(%s,1,20) as sub_v, v_count from %s ",
                es_get_column_name_alias(spec, ES_COLUMN_ALIAS),
                quote_identifier(tableName));
        }

        if (spec->mcv_list.num_mcv > 0) {
            bool typisvarlena = false;
            Oid foutoid;
            char* start_value = NULL;

            /* Convert the last mcv to character. */
            getTypeOutputInfo(spec->stats->attrtypid[0], &foutoid, &typisvarlena);
            start_value = OidOutputFunctionCall(foutoid, spec->hist_list.start_value);
            appendStringInfo(&str,
                "where NOT(%s) and "
                "(v_count < %ld or (v_count = %ld and %s > %s)) %s order by %s;",
                es_get_column_name_alias(spec, ES_COLUMN_ALIAS, " AND ", " ", " is null "),
                spec->hist_list.start_value_count,
                spec->hist_list.start_value_count,
                es_get_column_name_alias(spec, ES_COLUMN_ALIAS),
                quote_literal_cstr(start_value),
                !CHAR_WIDTH_EXCEED_20(spec) ? "" : "and sub_v is not null",
                es_get_column_name_alias(spec, ES_COLUMN_ALIAS));
        } else {
            /* All of the value are histgram value. */
            appendStringInfo(&str,
                "where NOT (%s) %s order by %s;",
                es_get_column_name_alias(spec, ES_COLUMN_ALIAS, " and ", "", " is null "),
                !CHAR_WIDTH_EXCEED_20(spec) ? "" : "and sub_v is not null",
                es_get_column_name_alias(spec, ES_COLUMN_ALIAS));
        }

        elog(ES_LOGLEVEL, "[Query to compute histgram] %s", str.data);

        result.num_cols = 2;
        result.output = spiResult;
        result.memoryContext = CurrentMemoryContext;
        result.compute_histgram = true;
        result.non_mcv_num = non_mcv_num;
        result.hist_list = spec->hist_list;
        result.num_rows = 0;
        result.spi_tupDesc = NULL;
        spi_exec_with_callback(
            DestSPITupleAnalyze, str.data, false, 0, false, (void (*)(void*))spi_callback_get_multicolarray, &result);
        pfree_ext(str.data);

        if (!CHAR_WIDTH_EXCEED_20(spec)) {
            is_attr_diff =
                result.spi_tupDesc && (result.spi_tupDesc->attrs[0].atttypid != spec->stats->attrs[0]->atttypid ||
                                          result.spi_tupDesc->attrs[0].atttypmod != spec->stats->attrs[0]->atttypmod ||
                                          result.spi_tupDesc->attrs[0].attlen != spec->stats->attrs[0]->attlen);
        } else {
            is_attr_diff = result.spi_tupDesc && (result.spi_tupDesc->attrs[0].atttypid != TEXTOID ||
                                                     result.spi_tupDesc->attrs[0].atttypmod != -1 ||
                                                     result.spi_tupDesc->attrs[0].attlen != -1);
        }

        /*
         * Check whether the attribute of received from datanode
         * is the same with local attribute for analyzed or not.
         */
        if (is_attr_diff) {
            ereport(WARNING,
                (errmsg("The tupleDesc analyzed on %s is different from tupleDesc which received from datanode "
                        "when computing histgram.",
                     g_instance.attr.attr_common.PGXCNodeName),
                    errdetail("Attribute \"%s\" of type %s does not match corresponding attribute of type %s.",
                        NameStr(spec->stats->attrs[0]->attname),
                        format_type_be(spec->stats->attrs[0]->atttypid),
                        format_type_be(result.spi_tupDesc->attrs[0].atttypid))));
            FreeTupleDesc(result.spi_tupDesc);

            return;
        }

        if (result.num_rows > 0) {
            compute_histgram_final(slot_idx, &result.hist_list, spec->stats);
        }
    }

    DEBUG_MOD_STOP_TIMER(MOD_AUTOVAC,
                         "Compute %d rows of Histgram for table %s success.",
                         result.hist_list.num_hist, tableName);
}

/*
 * Description: Analyze one attribute with execute query,
 *			and compute distinct/mcv/histgram with sample tuple.
 *
 * Parameters:
 *	@in inh: is inherit table for analzye or not
 *	@in onerel: the relation for analyze or vacuum
 *	@in vacstmt: the statment for analyze or vacuum command
 *	@in stats: statistics for one attribute
 *	@in numTotalRows: total rows for the relation
 *	@in numSampleRows: the num of sample rows
 *	@in analyzemode: identify the relation type for analyze
 *
 * Returns: void
 */
static void do_analyze_compute_attr_stats(bool inh, Relation onerel, VacuumStmt* vacstmt, VacAttrStats* stats,
    double numTotalRows, int64 numSampleRows, AnalyzeMode analyzemode)
{
    int slot_idx = 0;
    bool computeDistinct = true;
    bool computeMCV = true;
    bool computeHist = true;
    Oid ltopr;
    Oid save_userid;
    int save_sec_context;
    AnalyzeSampleTableSpecInfo spec;
    char* tableName = NULL;

    init_sample_table_spec_info(&spec, stats, numTotalRows, numSampleRows);

    /* Be sure 'create temp table' command is security-restricted. */
    GetUserIdAndSecContext(&save_userid, &save_sec_context);
    tableName =
        buildTempSampleTable(onerel->rd_id, InvalidOid, TempSmpleTblType_Attrbute, analyzemode, inh, vacstmt, &spec);

    u_sess->analyze_cxt.is_under_analyze = true;
    analyze_compute_samplerows(tableName, &spec);
    analyze_compute_nullcount(tableName, &spec);
    if (ceil(spec.null_cnt) >= ceil(spec.samplerows)) {
        /*
         * All values are null. no point computing other statistics.
         */
        computeDistinct = false;
        computeMCV = false;
        computeHist = false;
    }

    if (spec.stats->attrs[0]->atttypid == BOOLOID) {
        spec.stats->stadistinct = 2;
        computeDistinct = false;
        computeHist = false;
    }

    if (computeDistinct) {
        analyze_distinct(tableName, &spec);

        /* If all of sample rows are distinct value, then we cann't comput mcv any more. */
        if (fabs(spec.stats->stadistinct + 1.0) < EPSILON) {
            computeMCV = false;
        }
    }

    analyze_compute_avgwidth(tableName, &spec);
    if (spec.stats->stawidth > WIDTH_THRESHOLD) {
        /*
         * Extremely wide columns are considered to be fully distinct. See comments
         * against WIDTH_THRESHOLD
         */
        computeMCV = false;
        computeHist = false;
    }

    if (computeMCV && !(u_sess->attr.attr_sql.enable_ai_stats &&
                        u_sess->attr.attr_sql.multi_stats_type == BAYESNET_OPTION)) {
        analyze_compute_mcv(&slot_idx, tableName, &spec);
    }

    if (computeHist) {
        /* Do not collect histgram information for multi-column condition */
        if (spec.stats->num_attrs > 1) {
            computeHist = false;
        }

        /* Look for default "<" operators for column's type */
        get_sort_group_operators(stats->attrtypid[0], false, false, false, &ltopr, NULL, NULL, NULL);
        /* If column has no "<" operator, we can't do much of anything */
        if (!OidIsValid(ltopr)) {
            computeHist = false;
        }
    }

    if (computeHist) {
        analyze_compute_histgram(&slot_idx, tableName, &spec);
    }

    /* Do not collect correlation statistics for multi-column statistics */
    if (spec.stats->num_attrs == 1 && IS_PGXC_COORDINATOR &&
        vacstmt->pstGlobalStatEx[vacstmt->tableidx].correlations[stats->attrs[0]->attnum - 1] != 0) {
        analyze_compute_correlation(spec.nonnull_cnt,
            0,
            vacstmt->pstGlobalStatEx[vacstmt->tableidx].correlations[stats->attrs[0]->attnum - 1],
            stats,
            slot_idx);
    }

    /*
     * In multi-column statistics case, the first MCV is created with no-null values,
     * in order to support better selectivity estimation of NULL-filtering with other
     * predicates, we need collect another type of MCV that created with null values.
     */
    if (computeMCV && spec.stats->num_attrs > 1 && !(u_sess->attr.attr_sql.enable_ai_stats &&
                                                     u_sess->attr.attr_sql.multi_stats_type == BAYESNET_OPTION)) {
        analyze_compute_mcv(&slot_idx, tableName, &spec, true);
    }

    if (spec.stats->num_attrs > 1 && u_sess->attr.attr_sql.enable_ai_stats &&
        u_sess->attr.attr_sql.multi_stats_type != MCV_OPTION) {
        bool bayesnet_success = analyze_compute_bayesnet(&slot_idx, onerel, analyzemode, inh,
                                                         vacstmt, &spec, tableName);
        if (!bayesnet_success && computeMCV &&
            u_sess->attr.attr_sql.multi_stats_type != (int)ALL_OPTION) {
            ereport(WARNING, (errmodule(MOD_AUTOVAC),
                    errmsg("Only NDV of multi-column statistics is created.")));
        }
    }
    
#ifndef ENABLE_MULTIPLE_NODES
    /* Functional dependency statistics is only available in single node */
    if (u_sess->attr.attr_sql.enable_functional_dependency && spec.stats->num_attrs > 1) {
        analyze_compute_dependencies(onerel, &slot_idx, tableName, &spec, stats);
    }
#endif

    if (log_min_messages > DEBUG1) {
        dropSampleTable(tableName);
    }

    u_sess->analyze_cxt.is_under_analyze = false;
    stats = spec.stats;
    stats->stats_valid = true;
    pfree_ext(tableName);

    /* Restore UID and security context */
    SetUserIdAndSecContext(save_userid, save_sec_context);

    ereport(DEBUG1,(errmodule(MOD_AUTOVAC),
        errmsg("ANALYZE computing statistics on attribute %s, total column number: %u.",
        NameStr(stats->attrs[0]->attname),
        stats->num_attrs)));
}

/*
 * Description: Get temp sample table name for normal or hdfs table.
 *
 * Parameters:
 * 	@in analyzemode: the table type which collect statistics information
 * 	@in tmpSampleTblNameList: list of temp table name
 *
 * Returns: const char *
 */
const char* get_sample_tblname(AnalyzeMode analyzemode, List* tmpSampleTblNameList)
{
    const char* tmpSampleTableName = NULL;

    if (analyzemode == ANALYZENORMAL) {
        tmpSampleTableName = ((Value*)linitial(tmpSampleTblNameList))->val.str;
    } else {
        tmpSampleTableName = ((Value*)lsecond(tmpSampleTblNameList))->val.str;
    }

    return tmpSampleTableName;
}

/*
 * Description: Insert sample tuple into temp table on datanode under debugging for analyze.
 *
 * Parameters:
 * 	@in type: the stage for datanode send sample to coordinator under debugging
 * 	@in analyzemode: the table type which collect statistics information
 * 	@in tmpSampleTblNameList: list of temp table name
 * 	@out sampleTableOid: temp table oid
 * 	@in tmpRelation: temp table relation
 * 	@in tuple: sample tuple which datanode send to coordinator
 *
 * Returns: void
 */
static void analyze_tmptbl_debug_dn(AnalyzeTempTblDebugStage type, AnalyzeMode analyzemode, List* tmpSampleTblNameList,
    Oid* sampleTableOid, Relation* tmpRelation, HeapTuple tuple, TupleConversionMap* tupmap)
{
    if ((sampleTableOid == NULL) || (tmpRelation == NULL) || (tmpSampleTblNameList == NIL) ||
        (default_statistics_target >= 0 && log_min_messages > DEBUG1)) {
        return;
    }

    /* Get relation according to temp table name at begin stage. */
    if (DebugStage_Begin == type) {
        const char* tmpSampleTableName = get_sample_tblname(analyzemode, tmpSampleTblNameList);
        *sampleTableOid = RelnameGetRelid(tmpSampleTableName);

        if (OidIsValid(*sampleTableOid)) {
            *tmpRelation = heap_open(*sampleTableOid, RowExclusiveLock);
        }
    } else if (DebugStage_Execute == type) {
        /* Insert sample into the relation at execute stage. */
        if (OidIsValid(*sampleTableOid)) {
            HeapTuple convert_tuple = tuple;

            if (tupmap != NULL) {
                convert_tuple = do_convert_tuple(tuple, tupmap);
            }

            (void)simple_heap_insert(*tmpRelation, convert_tuple);
            /* Update indexes */
            CatalogUpdateIndexes(*tmpRelation, convert_tuple);

            if (tupmap != NULL) {
                tableam_tops_free_tuple(convert_tuple);
            }
        }
    } else {
        /* Close temp relation at end stage. */
        if (OidIsValid(*sampleTableOid)) {
            heap_close(*tmpRelation, RowExclusiveLock);
            *sampleTableOid = InvalidOid;
        }
    }
}

/*
 * Description: Check tupdesc of analyze's relation match with tupdesc of temp table or not.
 *			If encounter with ddl for [ALTER TABLE] after create temp table under analyzing,
 *			it identify doing ddl confilict with doing analyze,
 *			so we don't insert sample tuple into temp table any more.
 * Parameters:
 * 	@in onerel: the relation for analyze or vacuum
 * 	@in temptbl_desc: the tuple desc for temp table
 *
 * Returns: TupleConversionMap*
 */
static TupleConversionMap* check_tmptbl_tupledesc(Relation onerel, TupleDesc temptbl_desc)
{
    TupleConversionMap* tupmap = NULL;
    MemoryContext oldcontext = CurrentMemoryContext;

    PG_TRY();
    {
        tupmap = convert_tuples_by_name(CreateTupleDescCopy(onerel->rd_att),
            temptbl_desc,
            gettext_noop("attributes of relation for analyze does not match with temp table."));
    }
    PG_CATCH();
    {
        ErrorData* edata = NULL;

        /* Save error info */
        (void)MemoryContextSwitchTo(oldcontext);
        edata = CopyErrorData();
        FlushErrorState();

        if (edata->sqlerrcode == ERRCODE_DATATYPE_MISMATCH) {
            ereport(LOG,
                (errmsg("The tupleDesc analyzed on %s is different from tupleDesc which temp table created.",
                     g_instance.attr.attr_common.PGXCNodeName),
                    errdetail("%s", edata->detail),
                    errhint("Do analyze again for the relation: %s.", NameStr(onerel->rd_rel->relname))));

            /* release error state */
            FreeErrorData(edata);
        } else {
            ReThrowError(edata);
        }
    }
    PG_END_TRY();

    return tupmap;
}

/*
 * Return estimated tupe size (Byte) of the relation.
 */
int GetOneTupleSize(VacuumStmt* stmt, Relation rel)
{
    int attr_cnt = 0;
    int nindexes = 0;
    AnlIndexData* indexdata = NULL;
    bool hasindex = false;
    Relation* Irel = NULL;

    VacAttrStats** vacattrstats =
        get_vacattrstats_by_vacstmt(rel, stmt, &attr_cnt, &nindexes, &indexdata, &hasindex, true, &Irel, false);
    int total_width = get_total_width(vacattrstats, attr_cnt, nindexes, indexdata, rel);
    int oneTupleSize = get_one_tuplesize(rel, total_width);

    return oneTupleSize;
}

/*
 * Return the tuple count to be processed
 */
static double compute_com_size(VacuumStmt* vacstmt, Relation rel, int tableidx)
{
    double com_size;
    int64 workmem_allow_rows = 0;
    int attr_cnt = 0;
    int nindexes = 0;
    int total_width = 0;
    AnlIndexData* indexdata = NULL;
    bool hasindex = false;
    VacAttrStats** vacattrstats = NULL;
    Relation* Irel = NULL;

    /*
     * If it is absolute estimate, we use theorem based on the paper
     * "Random sampling for histogram construction: how much is enough?"
     */
    if (default_statistics_target > 0) {
        int targetrows = 0;

        com_size = (double)4 * default_statistics_target *
                   log(2 * vacstmt->pstGlobalStatEx[tableidx].totalRowCnts / 0.01) /
                   (ANALYZE_RELATIVE_ERROR * ANALYZE_RELATIVE_ERROR);

        /* Get real tuple size using to caculate workmem_allow_rows. */
        vacattrstats =
            get_vacattrstats_by_vacstmt(rel, vacstmt, &attr_cnt, &nindexes, &indexdata, &hasindex, true, &Irel);

        /*
         * Determine how many rows we need to sample according to user-defined target
         * which is stored in pg_attribute.
         * if we have never modified the default target, targetrows shold be 30000
         */
        for (int i = 0; i < attr_cnt; i++) {
            if (targetrows < vacattrstats[i]->minrows)
                targetrows = vacattrstats[i]->minrows;
        }

        for (int j = 0; j < nindexes; j++) {
            AnlIndexData* thisdata = &indexdata[j];

            for (int i = 0; i < thisdata->attr_cnt; i++) {
                if (targetrows < thisdata->vacattrstats[i]->minrows)
                    targetrows = thisdata->vacattrstats[i]->minrows;
            }
        }

        /*
         * 1. if user-defined target rows is too low, we choose a better target rows accoding
         * the paper "Random sampling for histogram construction: how much is enough?" by
         * Surajit Chaudhuri, Rajeev Motwani and Vivek Narasayya
         */
        com_size = Max(com_size, targetrows);

        /* sample no less than DEFAULT_SAMPLE_ROWCNT rows for more accurate */
        com_size = Max(com_size, DEFAULT_SAMPLE_ROWCNT);

        /* sample no less than table size */
        com_size = Min(com_size, vacstmt->pstGlobalStatEx[tableidx].totalRowCnts);

        total_width = get_total_width(vacattrstats, attr_cnt, nindexes, indexdata, rel);
        workmem_allow_rows = get_workmem_allow_rows(rel, total_width);

        /*
         * 1. sample rows size should not be more than work_mem
         * 2. sample rows size should not be less than DEFAULT_SAMPLE_ROWCNT
         */
        if (com_size > DEFAULT_SAMPLE_ROWCNT && com_size > workmem_allow_rows) {
            com_size = Min(com_size, workmem_allow_rows);
            com_size = Max(com_size, DEFAULT_SAMPLE_ROWCNT);
        }
    } else if (default_statistics_target < 0) {
        /* relative estimate */
        com_size = ceil(vacstmt->pstGlobalStatEx[tableidx].totalRowCnts * (default_statistics_target * (-1) / 100.0));

        /* Get all tuples as the sample if com_size less than 30000. */
        com_size = com_size > DEFAULT_SAMPLE_ROWCNT ? com_size : vacstmt->pstGlobalStatEx[tableidx].totalRowCnts;
    } else {
        /* less than 30000 */
        com_size = Min(vacstmt->pstGlobalStatEx[tableidx].totalRowCnts, DEFAULT_SAMPLE_ROWCNT);
    }

    /* totalRowCnts may < 0 ? */
    com_size = Max(0, com_size);
    return com_size;
}
