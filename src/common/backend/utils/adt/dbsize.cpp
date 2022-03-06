/*
 * dbsize.c
 *		Database object size functions, and related inquiries
 *
 * Copyright (c) 2002-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/dbsize.c
 *
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/stat.h>

#include "access/cstore_am.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/htup.h"
#include "access/reloptions.h"
#include "catalog/catalog.h"
#include "catalog/dfsstore_ctlg.h"
#include "catalog/gs_matview.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_hashbucket_fn.h"
#include "commands/defrem.h"
#include "commands/dbcommands.h"
#include "commands/matview.h"
#include "commands/tablespace.h"
#include "executor/spi.h"
#include "miscadmin.h"
#ifdef ENABLE_MOT
#include "foreign/fdwapi.h"
#endif
#ifdef PGXC
#include "catalog/pgxc_node.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/execRemote.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/cache/partid_cachemgr.h"
#include "tsdb/utils/ts_relcache.h"
#include "tsdb/common/ts_tablecmds.h"
#endif
#endif
#include "storage/smgr/fd.h"
#include "threadpool/threadpool.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/relfilenodemap.h"
#include "utils/relmapper.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "catalog/pg_partition_fn.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "storage/cu.h"
#include "storage/custorage.h"
#include "storage/smgr/segment.h"
#include "storage/cstore/cstore_compress.h"
#include "storage/page_compression.h"
#include "vecexecutor/vecnodes.h"

#ifdef PGXC
static Datum pgxc_database_size(Oid dbOid);
static Datum pgxc_source_database_size();
static Datum pgxc_tablespace_size(Oid tbOid);
static int64 pgxc_exec_sizefunc(Oid relOid, char* funcname, char* extra_arg);
static int64 pgxc_exec_partition_sizefunc(Oid partTableOid, Oid partOid, char* funcName);
static int64 calculate_partition_size(Oid partTableOid, Oid partOid);
int64 CalculateCStoreRelationSize(Relation rel, ForkNumber forknum);
int64 CalculateCStorePartitionedRelationSize(Relation rel, ForkNumber forknum);
#ifdef ENABLE_MULTIPLE_NODES
int64 CalculateTStorePartitionedRelationSize(Relation rel, ForkNumber forknum);
#endif
static int64 calculate_partition_indexes_size(Oid partTableOid, Oid partOid);
Oid pg_toast_get_baseid(Oid relOid, bool* isPartToast);
bool IsToastRelationbyOid(Oid relid);
static bool IsIndexRelationbyOid(Oid relid);
static int64 calculate_toast_table_size(Oid toastrelid);
static int64 calculate_dir_size_on_dfs(Oid tblSpcOid, StringInfo dirPath);
static int64 calculate_dbsize_on_dfs(Oid dbOid);
static int64 calculate_tablespace_size_on_dfs(Oid tblSpcOid);
static int64 calculate_table_file_size(Relation rel, bool isCStore, int forkNumOption);
static void AddRemoteToastBuf(bool isPartToast, Relation rel, char* funcname, char* extra_arg,
                              Relation partTableRel, Partition part, StringInfo buf);
static void AddRemoteMatviewBuf(Relation rel, char* funcname, char* extra_arg,
                                MvRelationType matviewRelationType, StringInfo buf);
#define DEFAULT_FORKNUM -1

/*
 * Below macro is important when the object size functions are called
 * for system catalog tables. For pg_catalog tables and other Coordinator-only
 * tables, we should return the data from Coordinator. If we don't find
 * locator info, that means it is a Coordinator-only table.
 */
#define COLLECT_FROM_DATANODES(relid)              \
    ((IS_PGXC_COORDINATOR && !IsConnFromCoord() && !is_sys_table(relid) && \
        ((GetRelationLocInfo((relid)) != NULL) || IsToastRelationbyOid(relid) || IsIndexRelationbyOid(relid))))
#endif

/* Return physical size of directory contents, or 0 if dir doesn't exist */
static int64 db_dir_size(const char* path)
{
    int64 dirsize = 0;
    struct dirent* direntry = NULL;
    DIR* dirdesc = NULL;
    char filename[MAXPGPATH] = {'\0'};

    dirdesc = AllocateDir(path);

    if (unlikely(NULL == dirdesc))
        return 0;

    while ((direntry = ReadDir(dirdesc, path)) != NULL) {
        struct stat fst;

        CHECK_FOR_INTERRUPTS();

        if (unlikely(strcmp(direntry->d_name, ".") == 0 || strcmp(direntry->d_name, "..") == 0))
            continue;

        errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH - 1, "%s/%s", path, direntry->d_name);
        securec_check_ss(rc, "\0", "\0");

        if (unlikely(stat(filename, &fst) < 0)) {
            if (errno == ENOENT)
                continue;
            else
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", filename)));
        }
        if (S_ISDIR(fst.st_mode)) {
            dirsize += db_dir_size(filename);
        } else {
            dirsize += fst.st_size;
        }
    }

    FreeDir(dirdesc);
    return dirsize;
}

/**
 * @Description: calculate tablespace size on DFS.
 * @in tblSpcOid, the tablespace oid.
 * @out none.
 * @return If the type of specified tablespace is HDFS, return
 * real size, otherwise return 0.
 */
static int64 calculate_tablespace_size_on_dfs(Oid tblSpcOid)
{
    int64 dirSize = 0;
    StringInfo StorePath;

    if (!IsSpecifiedTblspc(tblSpcOid, FILESYSTEM_HDFS)) {
        return dirSize;
    }
    StorePath = getHDFSTblSpcStorePath(tblSpcOid);

    dirSize = calculate_dir_size_on_dfs(tblSpcOid, StorePath);

    pfree_ext(StorePath->data);
    pfree_ext(StorePath);
    return dirSize;
}

/**
 * @Description: calculate database size on DFS.
 * @in dbOid, the database oid.
 * @out none.
 * @return If the database exists on DFS, return real size,
 * otherwise return 0.
 */
static int64 calculate_dbsize_on_dfs(Oid dbOid)
{
    List* tblSpcList = HDFSTablespaceDirExistDatabase(dbOid);
    ListCell* lc = NULL;
    int64 dirSize = 0;

    if (NIL == tblSpcList) {
        return dirSize;
    }

    foreach (lc, tblSpcList) {
        StringInfo tblspcInfo = (StringInfo)lfirst(lc);
        Oid tblSpcOid = get_tablespace_oid(tblspcInfo->data, false);

        StringInfo StorePath = getHDFSTblSpcStorePath(tblSpcOid);

        appendStringInfo(StorePath, "/%s", get_and_check_db_name(dbOid, true));

        dirSize += calculate_dir_size_on_dfs(tblSpcOid, StorePath);

        pfree_ext(StorePath->data);
        pfree_ext(StorePath);
        pfree_ext(tblspcInfo);
    }
    list_free_ext(tblSpcList);
    return dirSize;
}

/**
 * @Description: calculate DFS directory size.
 * @in tblSpcOid, the specified tablespace oid.
 * @out dirPath, the directory to be calculated.
 * @return return the directory size.
 */
static int64 calculate_dir_size_on_dfs(Oid tblSpcOid, StringInfo dirPath)
{
    FEATURE_NOT_PUBLIC_ERROR("HDFS is not yet supported.");
    return 0;
}

/*
 * calculate size of database in all tablespaces
 */
static int64 calculate_database_size(Oid dbOid)
{
    int64 totalsize;
    DIR* dirdesc = NULL;
    struct dirent* direntry = NULL;
    char dirpath[MAXPGPATH] = {'\0'};
    char pathname[MAXPGPATH] = {'\0'};
    AclResult aclresult;
    List* existTblSpcList = NIL;
    errno_t rc = EOK;

    /* User must have connect privilege for target database */
    aclresult = pg_database_aclcheck(dbOid, GetUserId(), ACL_CONNECT);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_DATABASE, get_and_check_db_name(dbOid));
    existTblSpcList = HDFSTablespaceDirExistDatabase(dbOid);
    if (IS_PGXC_DATANODE && IsConnFromApp() && NIL != existTblSpcList) {
        StringInfo existTblspc = (StringInfo)linitial(existTblSpcList);
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("It is unsupported to calculate size of database \"%s\""
                       " under the DFS tablespace \"%s\" on data node.",
                    get_and_check_db_name(dbOid),
                    existTblspc->data),
                errdetail("Please calculate size of database \"%s\" on coordinator node.", get_and_check_db_name(dbOid))));
    }

    /* Shared storage in pg_global is not counted */

    /* Include pg_default storage */
    rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "base/%u", dbOid);
    securec_check_ss(rc, "\0", "\0");
    totalsize = db_dir_size(pathname);

    /* Scan the non-default tablespaces */
    rc = snprintf_s(dirpath, MAXPGPATH, MAXPGPATH - 1, "pg_tblspc");
    securec_check_ss(rc, "\0", "\0");
    dirdesc = AllocateDir(dirpath);
    if (NULL == dirdesc)
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open tablespace directory \"%s\": %m", dirpath)));

    while ((direntry = ReadDir(dirdesc, dirpath)) != NULL) {
        CHECK_FOR_INTERRUPTS();

        if (strcmp(direntry->d_name, ".") == 0 || strcmp(direntry->d_name, "..") == 0)
            continue;

#ifdef PGXC
        /* openGauss tablespaces include node name in path */
        rc = snprintf_s(pathname,
            MAXPGPATH,
            MAXPGPATH - 1,
            "pg_tblspc/%s/%s_%s/%u",
            direntry->d_name,
            TABLESPACE_VERSION_DIRECTORY,
            g_instance.attr.attr_common.PGXCNodeName,
            dbOid);
        securec_check_ss(rc, "\0", "\0");

#else
        rc = snprintf_s(pathname,
            MAXPGPATH,
            MAXPGPATH - 1,
            "pg_tblspc/%s/%s/%u",
            direntry->d_name,
            TABLESPACE_VERSION_DIRECTORY,
            dbOid);
        securec_check_ss(rc, "\0", "\0");
#endif
        totalsize += db_dir_size(pathname);
    }

    FreeDir(dirdesc);

    return totalsize;
}

/*
 * @Description: calculate the compress ratio of the column-partition relation.
 * @in onerel: the relation.
 * @return: the source size of this relation.
 */
static double calculate_coltable_compress_ratio(Relation onerel)
{
    CStoreScanDesc cstoreScanDesc = NULL;
    TupleDesc tupdesc = onerel->rd_att;
    int attrNum = tupdesc->natts;
    Form_pg_attribute* attrs = tupdesc->attrs;
    CUDesc cuDesc;
    CU* cuPtr = NULL;
    double total_source_size = 0;
    double total_cu_size = 0;
    BlockNumber targetblock = FirstCUID + 1;

    AttrNumber* colIdx = (AttrNumber*)palloc0(sizeof(AttrNumber) * attrNum);
    int* slotIdList = (int*)palloc0(sizeof(int) * attrNum);

    double numericExpandRatio = 0;
    double numericSrcDataSize = 0;
    double numericDataSize = 0;

    for (int i = 0; i < attrNum; i++) {
        colIdx[i] = attrs[i]->attnum;
        slotIdList[i] = CACHE_BLOCK_INVALID_IDX;
    }

    cstoreScanDesc = CStoreBeginScan(onerel, attrNum, colIdx, NULL, false);
    CStore* cstore = cstoreScanDesc->m_CStore;
    uint32 maxCuId = cstore->GetMaxCUID(onerel->rd_rel->relcudescrelid, onerel->rd_att, SnapshotNow);

    /*do not fetch the last CuID.*/
    if (maxCuId > FirstCUID + 1) {
        targetblock = FirstCUID + 1 + (gs_random() % (maxCuId - FirstCUID - 1));
    } else {
        targetblock = FirstCUID + 1;
    }

    /*sample the first CU of each column, and calculate the compression ratio of this table.*/
    for (int col = 0; col < attrNum; col++) {
        // skip dropped column
        if (attrs[col]->attisdropped) {
            continue;
        }

        bool found = cstore->GetCUDesc(col, targetblock, &cuDesc, SnapshotNow);
        if (found && cuDesc.cu_size != 0) {
            cuPtr = cstore->GetCUData(&cuDesc, col, attrs[col]->attlen, slotIdList[col]);
            if ((cuPtr->m_infoMode & CU_IntLikeCompressed) && ATT_IS_NUMERIC_TYPE(cuPtr->m_atttypid)) {
                numericExpandRatio = 1.5; /* default expand ratio */
                numericDataSize = 0;
                numericSrcDataSize = 0;
                for (int rowId = 0; rowId < cuDesc.row_count; rowId++) {
                    if (cuPtr->IsNull(rowId)) {
                        continue;
                    }
                    char* srcData = cuPtr->m_srcData + cuPtr->m_offset[rowId];
                    Numeric nx = DatumGetNumeric(CStringGetDatum(srcData));
                    if (NUMERIC_IS_BI(nx)) {
                        Numeric srcNx = makeNumericNormal(nx);
                        numericSrcDataSize += VARSIZE_ANY(srcNx);
                        numericDataSize += cuPtr->m_offset[rowId + 1] - cuPtr->m_offset[rowId];
                        pfree_ext(srcNx);
                    }
                }
                if (numericSrcDataSize != 0 && numericDataSize != 0) {
                    numericExpandRatio = numericDataSize / numericSrcDataSize;
                }

                total_source_size += (double)(cuPtr->m_srcBufSize / numericExpandRatio);
                total_cu_size += (double)cuPtr->m_cuSize;
            } else {
                total_source_size += (double)cuPtr->m_srcDataSize;
                total_cu_size += (double)(cuPtr->m_cuSize - cuPtr->GetCUHeaderSize());
            }

            if (IsValidCacheSlotID(slotIdList[col])) {
                CUCache->UnPinDataBlock(slotIdList[col]);
            }
        }
    }

    pfree_ext(colIdx);
    pfree_ext(slotIdList);
    CStoreEndScan(cstoreScanDesc);

    return (total_cu_size > 0) ? (total_source_size / total_cu_size) : 0;
}

/*
 * @Description: calculate the compress ratio of the column-partition relation.
 * @in onerel: the relation.
 * @return: the source size of this relation.
 */
static double calculate_parttable_compress_ratio(Relation onerel)
{
    double compress_ratio = 0;
    double sample_ratio = 0;
    int partition_num = 0;

    List* partitions = NIL;
    ListCell* cell = NULL;
    Partition partition = NULL;
    Relation partRel = NULL;

    /*fetch the compressed ratio of each partition, and get the average value.*/
    partitions = relationGetPartitionList(onerel, AccessShareLock);
    foreach (cell, partitions) {
        partition = (Partition)lfirst(cell);
        partRel = partitionGetRelation(onerel, partition);
        sample_ratio = calculate_coltable_compress_ratio(partRel);
        if (0 != sample_ratio) {
            compress_ratio += sample_ratio;
            partition_num++;
        }

        releaseDummyRelation(&partRel);
    }

    releasePartitionList(onerel, &partitions, AccessShareLock);

    return (partition_num > 0) ? (compress_ratio / partition_num) : 0;
}

Datum pg_database_size_oid(PG_FUNCTION_ARGS)
{
    Oid dbOid = PG_GETARG_OID(0);
    int64 size;

#ifdef PGXC
    if ((IS_PGXC_COORDINATOR && !IsConnFromCoord()))
        PG_RETURN_DATUM(pgxc_database_size(dbOid));
#endif

    /* Normally, check the validation of dbOid */
    (void)get_and_check_db_name(dbOid, true);
    size = calculate_database_size(dbOid);

    if (size == 0)
        PG_RETURN_NULL();

    PG_RETURN_INT64(size);
}

Datum pg_database_size_name(PG_FUNCTION_ARGS)
{
    Name dbName = PG_GETARG_NAME(0);
    Oid dbOid = get_database_oid(NameStr(*dbName), false);
    int64 size;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        PG_RETURN_DATUM(pgxc_database_size(dbOid));
#endif

    size = calculate_database_size(dbOid);

    if (size == 0)
        PG_RETURN_NULL();

    PG_RETURN_INT64(size);
}

typedef struct SampleTuplesInfo {
    Oid relid;
    double tuples;
} SampleTuplesInfo;

static int compareRelationTuples(const void* a, const void* b, void* arg)
{
    const SampleTuplesInfo* aa = (const SampleTuplesInfo*)a;
    const SampleTuplesInfo* bb = (const SampleTuplesInfo*)b;
    if (aa->tuples > bb->tuples)
        return -1;
    else if (aa->tuples < bb->tuples)
        return 1;
    else
        return 0;
}

/*
 * @Description: calculate the source size of database.
 * @return: the size of database on one DN.
 */
static double calculate_source_database_size()
{
    Relation class_rel;
    Relation sample_rel;
    SysScanDesc scan = NULL;
    HeapTuple tuple;
    Form_pg_class tuple_class;
    TupleDesc pg_class_tuple_desc;

    double row_tuples = 0;
    double col_tuples = 0;
    double sample_ratio = 0;
    int64 sample_relation_size = 0;
    double sample_relations_total_size = 0;
    /*we think the compress ratio less than 0.5 is abnormal*/
    const double abnormal_compress_ratio = 0.5;
    double compress_ratio = 0;
    double compress_ratio_weight = 0;

    int64 size = 0;
    int64 dbsize = 0;
    int idx = 0;

    /*fetch the most 128 relations to calculate compressed ratio.*/
    int sample_relation_num = 128;
    const int tmp_num = 1024;

    SampleTuplesInfo* tupleInfo = (SampleTuplesInfo*)palloc0(tmp_num * sizeof(SampleTuplesInfo));

    /*scan the pg_class to fetch the reltuples of every relation.*/
    class_rel = heap_open(RelationRelationId, AccessShareLock);
    pg_class_tuple_desc = class_rel->rd_att;
    scan = systable_beginscan(class_rel, InvalidOid, false, NULL, 0, NULL);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        tuple_class = (Form_pg_class)GETSTRUCT(tuple);
        Oid relid = HeapTupleHeaderGetOid(tuple->t_data);

        /* skip the table with zero retuples.*/
        if (0 == tuple_class->reltuples) {
            continue;
        }

        /*process the column tables.*/
        if ((tuple_class->relkind != RELKIND_INDEX && tuple_class->relkind != RELKIND_GLOBAL_INDEX) &&
            CheckRelOrientationByPgClassTuple(tuple, pg_class_tuple_desc, ORIENTATION_COLUMN)) {
            col_tuples += tuple_class->reltuples;
            if (idx < tmp_num) {
                tupleInfo[idx].relid = relid;
                tupleInfo[idx].tuples = tuple_class->reltuples;
                idx++;
            } else {
                /*sort the tables with reltuples.*/
                qsort_arg(tupleInfo, tmp_num, sizeof(SampleTuplesInfo), compareRelationTuples, NULL);
                idx = sample_relation_num;
            }
        } else if (tuple_class->relkind == RELKIND_RELATION &&
            CheckRelOrientationByPgClassTuple(tuple, pg_class_tuple_desc, ORIENTATION_ORC)) {
            continue;
        }
        /*skip the foreign tables.*/
        else if (tuple_class->relkind == RELKIND_FOREIGN_TABLE || tuple_class->relkind == RELKIND_STREAM) {
            continue;
        }
        /*other tables are processed as row tables.*/
        else {
            row_tuples += tuple_class->reltuples;
        }
    }
    systable_endscan(scan);
    heap_close(class_rel, AccessShareLock);

    if (idx != sample_relation_num) {
        qsort_arg(tupleInfo, idx, sizeof(SampleTuplesInfo), compareRelationTuples, NULL);
    }
    sample_relation_num = (idx > sample_relation_num) ? sample_relation_num : idx;

    /*calculate the compress ratio of each sample column table.*/
    for (int i = 0; i < sample_relation_num; i++) {
        sample_rel = try_relation_open(tupleInfo[i].relid, AccessShareLock);
        if (NULL == sample_rel) {
            continue;
        }

        /*non-partition column tables*/
        if (!RelationIsPartitioned(sample_rel)) {
            sample_ratio = calculate_coltable_compress_ratio(sample_rel);
            if (sample_ratio < abnormal_compress_ratio) {
                relation_close(sample_rel, AccessShareLock);
                continue;
            }
            /*calculate the weight average ratio.*/
            sample_relation_size = CalculateCStoreRelationSize(sample_rel, 0);
            compress_ratio_weight += sample_ratio * sample_relation_size;
            sample_relations_total_size += sample_relation_size;
        }
        /*partition column tables*/
        else {
            sample_ratio = calculate_parttable_compress_ratio(sample_rel);
            if (sample_ratio < abnormal_compress_ratio) {
                relation_close(sample_rel, AccessShareLock);
                continue;
            }
            sample_relation_size = CalculateCStorePartitionedRelationSize(sample_rel, 0);
            compress_ratio_weight += sample_ratio * sample_relation_size;
            sample_relations_total_size += sample_relation_size;
        }

        relation_close(sample_rel, AccessShareLock);
    }

    compress_ratio = (compress_ratio_weight / sample_relations_total_size) > 1
                         ? (compress_ratio_weight / sample_relations_total_size)
                         : 1;

    /*calculate the size of current database.*/
    dbsize = calculate_database_size(u_sess->proc_cxt.MyDatabaseId);

    if (0 == (col_tuples + row_tuples)) {
        size = 0;
        ereport(WARNING,
            (errmsg("The reltuples in pg_class is equal to zero."),
                errhint("Do analyze for the current database first.")));

    } else {
        size = dbsize + dbsize * (col_tuples / (col_tuples + row_tuples)) * (compress_ratio - 1);
    }

    pfree_ext(tupleInfo);
    return size;
}

/*
 * @Description: the entry of this function.
 * @return: the size of database on one DN or all DNs.
 */
Datum get_db_source_datasize(PG_FUNCTION_ARGS)
{
    double size = 0;

#ifdef PGXC
    /* the size of database on all DNs*/
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        PG_RETURN_DATUM(pgxc_source_database_size());
#endif

    size = calculate_source_database_size();

    if (size == 0)
        PG_RETURN_NULL();
    /* the size of database on one DN*/
    PG_RETURN_INT64(size);
}

/*
 * Calculate total size of tablespace. Returns -1 if the tablespace directory
 * cannot be found.
 */
static int64 calculate_tablespace_size(Oid tblspcOid)
{
    char tblspcPath[MAXPGPATH] = {'\0'};
    char pathname[MAXPGPATH] = {'\0'};
    int64 totalsize = 0;
    DIR* dirdesc = NULL;
    struct dirent* direntry = NULL;
    AclResult aclresult;
    errno_t rc = EOK;

    /*
     * User must have CREATE privilege for target tablespace, either
     * explicitly granted or implicitly because it is default for current
     * database.
     */
    if (tblspcOid != u_sess->proc_cxt.MyDatabaseTableSpace) {
        aclresult = pg_tablespace_aclcheck(tblspcOid, GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_TABLESPACE, get_tablespace_name(tblspcOid));
    }

    if (IS_PGXC_DATANODE && IsConnFromApp() && IsSpecifiedTblspc(tblspcOid, FILESYSTEM_HDFS)) {
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("It is unsupported to calculate size of DFS tablespace \"%s\" on data node.",
                    get_tablespace_name(tblspcOid)),
                errdetail("Please calculate size of DFS tablespace \"%s\" on coordinator node.",
                    get_tablespace_name(tblspcOid))));
    }
    if (tblspcOid == DEFAULTTABLESPACE_OID)
        rc = snprintf_s(tblspcPath, MAXPGPATH, MAXPGPATH - 1, "base");

    else if (tblspcOid == GLOBALTABLESPACE_OID)
        rc = snprintf_s(tblspcPath, MAXPGPATH, MAXPGPATH - 1, "global");
    else
#ifdef PGXC
        /* openGauss tablespaces include node name in path */
        rc = snprintf_s(tblspcPath,
            MAXPGPATH,
            MAXPGPATH - 1,
            "pg_tblspc/%u/%s_%s",
            tblspcOid,
            TABLESPACE_VERSION_DIRECTORY,
            g_instance.attr.attr_common.PGXCNodeName);
#else
        rc = snprintf_s(
            tblspcPath, MAXPGPATH, MAXPGPATH - 1, "pg_tblspc/%u/%s", tblspcOid, TABLESPACE_VERSION_DIRECTORY);
#endif
    securec_check_ss(rc, "\0", "\0");
    dirdesc = AllocateDir(tblspcPath);

    if (NULL == dirdesc)
        return -1;

    while ((direntry = ReadDir(dirdesc, tblspcPath)) != NULL) {
        struct stat fst;

        CHECK_FOR_INTERRUPTS();

        if (unlikely(strcmp(direntry->d_name, ".") == 0 || strcmp(direntry->d_name, "..") == 0))
            continue;

        rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s/%s", tblspcPath, direntry->d_name);
        securec_check_ss(rc, "\0", "\0");

        if (unlikely(stat(pathname, &fst) < 0)) {
            if (errno == ENOENT)
                continue;
            else
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", pathname)));
        }

        if (S_ISDIR(fst.st_mode))
            totalsize += db_dir_size(pathname);

        totalsize += fst.st_size;
    }

    FreeDir(dirdesc);

    return totalsize;
}

uint64 pg_cal_tablespace_size_oid(Oid tblspcOid)
{
    return (uint64)(calculate_tablespace_size(tblspcOid));
}

Datum pg_tablespace_size_oid(PG_FUNCTION_ARGS)
{
    Oid tblspcOid = PG_GETARG_OID(0);
    int64 size;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        PG_RETURN_DATUM(pgxc_tablespace_size(tblspcOid));
#endif

    size = calculate_tablespace_size(tblspcOid);

    if (size < 0)
        PG_RETURN_NULL();

    PG_RETURN_INT64(size);
}

Datum pg_tablespace_size_name(PG_FUNCTION_ARGS)
{
    Name tblspcName = PG_GETARG_NAME(0);
    Oid tblspcOid = get_tablespace_oid(NameStr(*tblspcName), false);
    int64 size;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        PG_RETURN_DATUM(pgxc_tablespace_size(tblspcOid));
#endif

    size = calculate_tablespace_size(tblspcOid);

    if (size < 0)
        PG_RETURN_NULL();

    PG_RETURN_INT64(size);
}

/*
 * calculate size of (one fork of) a relation
 *
 * Note: we can safely apply this to temp tables of other sessions, so there
 * is no check here or at the call sites for that.
 */
int64 calculate_relation_size(RelFileNode* rfn, BackendId backend, ForkNumber forknum)
{
    if (IsSegmentFileNode(*rfn)) {
        SMgrRelation reln = smgropen(*rfn, backend);
        int64 blcksz = BLCKSZ;
        return blcksz * smgrtotalblocks(reln, forknum);
    }

    int64 totalsize = 0;
    char* relationpath = NULL;
    char pathname[MAXPGPATH] = {'\0'};
    unsigned int segcount = 0;
    errno_t rc = EOK;

    relationpath = relpathbackend(*rfn, backend, forknum);

    bool rowCompress = IS_COMPRESSED_RNODE((*rfn), forknum);
    for (segcount = 0;; segcount++) {
        struct stat fst;

        CHECK_FOR_INTERRUPTS();

        if (segcount == 0)
            rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s", relationpath);
        else
            rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s.%u", relationpath, segcount);
        securec_check_ss(rc, "\0", "\0");
        if (stat(pathname, &fst) < 0) {
            if (errno == ENOENT)
                break;
            else
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", pathname)));
        }
        totalsize += rowCompress ? CalculateMainForkSize((char*)pathname, rfn, forknum) : fst.st_size;
    }

    pfree_ext(relationpath);

    return totalsize;
}

#ifdef ENABLE_MOT
uint64 CalculateMotRelationSize(Relation rel, Oid idxOid)
{
    uint64  totalSize = 0;
    FdwRoutine *fdwroutine = GetFdwRoutineForRelation(rel, false);
    if (fdwroutine != NULL) {
        totalSize = fdwroutine->GetForeignRelationMemSize(RelationGetRelid(rel), idxOid);
    }

    return totalSize;
}
#endif

int64 CalculateCStoreRelationSize(Relation rel, ForkNumber forknum)
{
    int64 totalsize = 0;
    int64 size = 0;
    uint64 segcount = 0;
    char pathname[MAXPGPATH] = {'\0'};

    if (forknum == MAIN_FORKNUM) {
        /*
         * Calcuate date files.
         */
        if (RelationIsDfsStore(rel)) {
            if (IS_PGXC_DATANODE && IsConnFromApp()) {
                /*
                 * Calcuate data files size for orc on hdfs.
                 */
                size = getDFSRelSize(rel);
                totalsize += size;
            }
        } else {
            for (int i = 0; i < RelationGetDescr(rel)->natts; i++) {
                totalsize += calculate_relation_size(
                    &rel->rd_node, rel->rd_backend, ColumnId2ColForkNum(rel->rd_att->attrs[i]->attnum));
                CFileNode tmpNode(rel->rd_node, rel->rd_att->attrs[i]->attnum, MAIN_FORKNUM);
                CUStorage custore(tmpNode);
                for (segcount = 0;; segcount++) {
                    struct stat fst;

                    CHECK_FOR_INTERRUPTS();

                    custore.GetFileName(pathname, MAXPGPATH, segcount);

                    if (stat(pathname, &fst) < 0) {
                        if (errno == ENOENT)
                            break;
                        else
                            ereport(
                                ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", pathname)));
                    }
                    totalsize += fst.st_size;
                }
                custore.Destroy();
            }
        }
    }

    // Add delta table's size
    Relation deltaRel = try_relation_open(rel->rd_rel->reldeltarelid, AccessShareLock);
    if (deltaRel != NULL) {
        totalsize += calculate_relation_size(&(deltaRel->rd_node), deltaRel->rd_backend, forknum);

        // Add CUDesc Toast table's size only when forkno is MAIN forkno.
        if (MAIN_FORKNUM == forknum && OidIsValid(deltaRel->rd_rel->reltoastrelid)) {
            totalsize += calculate_toast_table_size(deltaRel->rd_rel->reltoastrelid);
        }

        relation_close(deltaRel, AccessShareLock);
    }

    // Add CUDesc table's size
    Relation cudescRel = try_relation_open(rel->rd_rel->relcudescrelid, AccessShareLock);
    if (cudescRel != NULL) {
        totalsize += calculate_relation_size(&(cudescRel->rd_node), cudescRel->rd_backend, forknum);

        // Add CUDesc Toast table's size only when forkno is MAIN forkno.
        if (MAIN_FORKNUM == forknum && OidIsValid(cudescRel->rd_rel->reltoastrelid)) {
            totalsize += calculate_toast_table_size(cudescRel->rd_rel->reltoastrelid);
        }

        // Add CUDesc Index's size
        Relation cudescIdx = try_relation_open(cudescRel->rd_rel->relcudescidx, AccessShareLock);
        if (cudescIdx != NULL) {
            totalsize += calculate_relation_size(&(cudescIdx->rd_node), cudescIdx->rd_backend, forknum);
            // ignore toast relation size because CUDesc Index
            // doesn't have any toast sub-relation.
            relation_close(cudescIdx, AccessShareLock);
        }

        relation_close(cudescRel, AccessShareLock);
    }

    return totalsize;
}

int64 CalculateCStorePartitionedRelationSize(Relation rel, ForkNumber forknum)
{
    int64 size = 0;

    List* partitions = NIL;
    ListCell* cell = NULL;
    Partition partition = NULL;
    Relation partRel = NULL;

    partitions = relationGetPartitionList(rel, AccessShareLock);

    foreach (cell, partitions) {
        partition = (Partition)lfirst(cell);
        partRel = partitionGetRelation(rel, partition);
        size += CalculateCStoreRelationSize(partRel, forknum);

        releaseDummyRelation(&partRel);
    }

    releasePartitionList(rel, &partitions, AccessShareLock);

    return size;
}

#ifdef ENABLE_MULTIPLE_NODES 
/* 
 * @description: Calcute timeseries cudesc relation size. 
 * @param {rel}: partition relation
 * @param {forknum} : forknum of file name
 * @return {int64} : relation size
 */
int64 CalculateTsCudescRelationSize(const Relation rel, ForkNumber forknum)
{
    int64 totalsize = 0;
    // Add CUDesc table's size
    List* cudesc_oids = search_cudesc(rel->rd_id, false);
    if (list_length(cudesc_oids) == 0) {
        list_free_ext(cudesc_oids);
        return totalsize;
    }
    ListCell *cell = NULL;
    foreach(cell, cudesc_oids) {
        Oid cudesc_oid = lfirst_oid(cell);
        Relation cudescRel = try_relation_open(cudesc_oid, AccessShareLock);
        if (cudescRel != NULL) {
            totalsize += calculate_relation_size(&(cudescRel->rd_node), cudescRel->rd_backend, forknum);
        } else {
            continue;
        }

        // Add TsCudesc Toast table's size only when forkno is MAIN forkno
        if (MAIN_FORKNUM == forknum && OidIsValid(cudescRel->rd_rel->reltoastrelid)) {
            totalsize += calculate_toast_table_size(cudescRel->rd_rel->reltoastrelid);
        }

        // Add TsCUDesc Index's size
        Relation cudescIdx = try_relation_open(cudescRel->rd_rel->relcudescidx, AccessShareLock);
        if (cudescIdx != NULL) {
            totalsize += calculate_relation_size(&(cudescIdx->rd_node), cudescIdx->rd_backend, forknum);
            // ignore toast relation size because CUDesc Index
            // doesn't have any toast sub-relation.
            relation_close(cudescIdx, AccessShareLock);
        }
        relation_close(cudescRel, AccessShareLock);
    }
    list_free_ext(cudesc_oids);
    return totalsize;
}

/* 
 * @description: Calcute timeseries tag relation size. 
 * @param {rel}: partition relation
 * @param {forknum} : forknum of file name
 * @return {int64} : relation size
 */
int64 CalculateTsTagRelationSize(const Relation rel, ForkNumber forknum)
{
    int64 totalsize = 0;

    Oid tstag_oid = get_tag_relid(RelationGetRelationName(rel), rel->rd_rel->relnamespace);
    Relation tstag_rel = try_relation_open(tstag_oid, AccessShareLock);
    if (tstag_rel != NULL) {
        totalsize += calculate_relation_size(&(tstag_rel->rd_node), tstag_rel->rd_backend, forknum);
        // Add TsTag Toast table's size only when forkno is MAIN forkno
        if (MAIN_FORKNUM == forknum && OidIsValid(tstag_rel->rd_rel->reltoastrelid)) {
            totalsize += calculate_toast_table_size(tstag_rel->rd_rel->reltoastrelid);
        }
        relation_close(tstag_rel, AccessShareLock);

        // Add TsTag Index's size(todo, add more index) 
        Oid tstag_idx_oid = get_tag_index_relid(RelationGetRelationName(tstag_rel));
        Relation tstag_idx_rel = try_relation_open(tstag_idx_oid, AccessShareLock);
        if (tstag_idx_rel != NULL) {
            totalsize += calculate_relation_size(&(tstag_idx_rel->rd_node), tstag_idx_rel->rd_backend, forknum);
            relation_close(tstag_idx_rel, AccessShareLock);
        }
    }
    return totalsize;
}

/* 
 * @description: Calcute timeseries column file size. Relation size includs:
 *               1.pg_tags_*** relation size
 *               2.pg_cudesc_*** relation size
 *               4.column file size: 16395(partition_oid)_c2002(partid).0
 * @param {rel}: partition relation
 * @param {forknum} : forknum of file name
 * @return {int64} : relation size
 */
int64 CalculateTStoreRelationSize(Relation rel, ForkNumber forknum)
{
    int64 totalsize = 0;

    // Add delta table's size
    
    int64 file_size = CalculateTsCudescRelationSize(rel, forknum);
    if (file_size == 0) {
        // No part exist in this partition
        return totalsize;
    }
    totalsize += file_size;

    uint64 segcount = 0;
    char pathname[MAXPGPATH];
    errno_t rc = memset_s(pathname, MAXPGPATH, 0, MAXPGPATH);
    securec_check(rc, "", "");
    if (forknum == MAIN_FORKNUM) {
        // Calcuate date files.
        for (uint16 partid = TsConf::FIRST_PARTID; partid < TsConf::MAX_PART_NUM; partid++) {
            /*
             * In timeseries relation, froknum is partid when calculate ts column file size because:
             * Timeseries column name is like : 16391_C2002.0, 16391 is partition oid, 2002 is forknum.
             */
            int64 col_file_size = calculate_relation_size(&rel->rd_node, rel->rd_backend, ColumnId2ColForkNum(partid));
            totalsize += col_file_size;
            CFileNode tmpNode(rel->rd_node, partid, MAIN_FORKNUM);
            CUStorage custore(tmpNode);
            for (segcount = 0;; segcount++) {
                struct stat fst;

                CHECK_FOR_INTERRUPTS();

                custore.GetFileName(pathname, MAXPGPATH, segcount);

                if (stat(pathname, &fst) < 0) {
                    if (errno == ENOENT)
                        break;
                    else
                        ereport(
                            LOG, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", pathname)));
                }
                totalsize += fst.st_size;
            }
            custore.Destroy();
        }
    }
    
    return totalsize;
}

/* 
 * @description: Calcute timeseries column file size by partition. 
 * @param {rel}: relation
 * @param {forknum}
 * @return {int64} : relation size
 */
int64 CalculateTStorePartitionedRelationSize(Relation rel, ForkNumber forknum)
{
    int64 size = 0;

    List* partitions = NIL;
    ListCell* cell = NULL;
    Partition partition = NULL;
    Relation partRel = NULL;

    size += CalculateTsTagRelationSize(rel, forknum);

    partitions = relationGetPartitionList(rel, AccessShareLock);

    foreach (cell, partitions) {
        partition = (Partition)lfirst(cell);
        partRel = partitionGetRelation(rel, partition);
        size += CalculateTStoreRelationSize(partRel, forknum);

        releaseDummyRelation(&partRel);
    }

    releasePartitionList(rel, &partitions, AccessShareLock);

    return size;
}
#endif

/*
 * Calculate total on-disk size of a TOAST relation, including its index.
 * Must not be applied to non-TOAST relations.
 */
static int64 calculate_toast_table_size(Oid toastrelid)
{
    int64 size = 0;
    Relation toastRel;
    Relation toastIdxRel;
    ForkNumber forkNum;

    toastRel = relation_open(toastrelid, AccessShareLock);

    /* toast heap size, including FSM and VM size */
    for (int ifork = 0; ifork <= MAX_FORKNUM; ifork++) {
        forkNum = (ForkNumber)ifork;
        size += calculate_table_file_size(toastRel, false, forkNum);
    }

    /* toast index size, including FSM and VM size */
    toastIdxRel = relation_open(toastRel->rd_rel->reltoastidxid, AccessShareLock);
    for (int ifork = 0; ifork <= MAX_FORKNUM; ifork++) {
        forkNum = (ForkNumber)ifork;
        size += calculate_table_file_size(toastIdxRel, false, forkNum);
    }

    relation_close(toastIdxRel, AccessShareLock);
    relation_close(toastRel, AccessShareLock);

    return size;
}

static int64 calculate_table_file_size(Relation rel, bool isCStore, int forkNumOption)
{
    int64 size = 0;
    ForkNumber forkNum = (ForkNumber)0;

    if (RELATION_CREATE_BUCKET(rel)) {
        Relation heapBucketRel = NULL;
        oidvector* bucketlist = searchHashBucketByOid(rel->rd_bucketoid);

        for (int i = 0; i < bucketlist->dim1; i++) {
            heapBucketRel = bucketGetRelation(rel, NULL, bucketlist->values[i]);
            if (forkNumOption == DEFAULT_FORKNUM) {
                /*
                 * heap size, including FSM and VM
                 */
                for (int ifork = 0; ifork <= MAX_FORKNUM; ifork++) {
                    forkNum = (ForkNumber)ifork;

                    size += calculate_relation_size(&(heapBucketRel->rd_node), InvalidBackendId, forkNum);
                }
            } else {
                size += calculate_relation_size(&(heapBucketRel->rd_node), InvalidBackendId, forkNumOption);
            }
            bucketCloseRelation(heapBucketRel);
        }
        /*
         * Size of toast relation
         */
        if (OidIsValid(rel->rd_rel->reltoastrelid)) {
            size += calculate_toast_table_size(rel->rd_rel->reltoastrelid);
        }
    } else {
        if (forkNumOption == DEFAULT_FORKNUM) {
            /*
             * heap size, including FSM and VM
             */
            for (int ifork = 0; ifork <= MAX_FORKNUM; ifork++) {
                forkNum = (ForkNumber)ifork;

                if (isCStore) {
                    size += CalculateCStoreRelationSize(rel, forkNum);
                }
#ifdef ENABLE_MULTIPLE_NODES
                else if (RelationIsTsStore(rel)) {
                    size += CalculateTStoreRelationSize(rel, forkNum);
                }
#endif
                else {
                    size += calculate_relation_size(&(rel->rd_node), rel->rd_backend, forkNum);
                }
            }
            /*
             * Size of toast relation
             */
            if (OidIsValid(rel->rd_rel->reltoastrelid)) {
                size += calculate_toast_table_size(rel->rd_rel->reltoastrelid);
            }
        } else {
            if (isCStore) {
                size += CalculateCStoreRelationSize(rel, forkNumOption);
            } else if (RelationIsTsStore(rel)) {
#ifdef ENABLE_MULTIPLE_NODES
                size += CalculateTStoreRelationSize(rel, forkNumOption);
#endif
            } else {
                size += calculate_relation_size(&(rel->rd_node), rel->rd_backend, forkNumOption);
            }
        }
    }
    return size;
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * Calculate timeseries table size
 */
static int64 CalculateTsTableSize(Relation rel, int forkNumOption)
{
    int64 size = 0;
    if (forkNumOption == DEFAULT_FORKNUM) {
        /*
         * heap size, including FSM and VM
         */
        for (int ifork = 0; ifork <= MAX_FORKNUM; ifork++) {
            int forkNum = (ForkNumber)ifork;
            size += CalculateTStorePartitionedRelationSize(rel, forkNum);
        }
    } else {
        size = CalculateTStorePartitionedRelationSize(rel, forkNumOption);
    }
    return size;
}
#endif

/*
 * Calculate table size
 */
static int64 CalculateRelSize(Relation rel, int forkNumOption)
{
    int64 size = 0;
#ifdef ENABLE_MOT
    if (RelationIsForeignTable(rel) && RelationIsMOTTableByOid(RelationGetRelid(rel))) {
        size = CalculateMotRelationSize(rel, InvalidOid);
    } else if (!RelationIsPartitioned(rel)) {
#else
    if (!RelationIsPartitioned(rel)) {
#endif
        size = calculate_table_file_size(rel, RelationIsColStore(rel), forkNumOption);
#ifdef ENABLE_MULTIPLE_NODES
    } else if (RelationIsTsStore(rel)) {
        size = CalculateTsTableSize(rel, forkNumOption);
#endif
    } else {
        List* partitions = NIL;
        ListCell* cell = NULL;
        Partition partition = NULL;
        Relation partRel = NULL;
        partitions = relationGetPartitionList(rel, AccessShareLock);

        foreach (cell, partitions) {
            partition = (Partition)lfirst(cell);
            partRel = partitionGetRelation(rel, partition);
            if (RelationIsSubPartitioned(rel)) {
                ListCell* subcell = NULL;
                Partition subpartition = NULL;
                Relation subpartRel = NULL;
                List* subpartitions = relationGetPartitionList(partRel, AccessShareLock);
                foreach (subcell, subpartitions) {
                    subpartition = (Partition)lfirst(subcell);
                    subpartRel = partitionGetRelation(partRel, subpartition);
                    size += calculate_table_file_size(subpartRel, RelationIsColStore(rel), forkNumOption);
                    releaseDummyRelation(&subpartRel);
                }
                releasePartitionList(partRel, &subpartitions, AccessShareLock);
            } else {
                size += calculate_table_file_size(partRel, RelationIsColStore(rel), forkNumOption);
            }

            releaseDummyRelation(&partRel);
        }

        releasePartitionList(rel, &partitions, AccessShareLock);
#ifdef ENABLE_MULTIPLE_NODES
        if (RelationIsTsStore(rel)) {
            size += CalculateTsTagRelationSize(rel, forkNumOption);
        }
#endif
    }
    return size;
}

/*
 * Calculate Index size
 */
static int64 CalculateIndexSize(Relation rel, int forkNumOption)
{
    int64 size = 0;
    Relation baseRel = relation_open(rel->rd_index->indrelid, AccessShareLock);
    bool bCstore = RelationIsColStore(baseRel) && (rel->rd_rel->relam == PSORT_AM_OID);

#ifdef ENABLE_MOT
    if (RelationIsForeignTable(baseRel) && RelationIsMOTTableByOid(RelationGetRelid(baseRel))) {
        size = CalculateMotRelationSize(baseRel, RelationGetRelid(rel));
    } else if (!RelationIsPartitioned(rel)) {
#else
    if (!RelationIsPartitioned(rel)) {
#endif
        if (!bCstore) {
            size = calculate_table_file_size(rel, false, forkNumOption);
        }
        else {
            Relation cstoreIdxRel = relation_open(rel->rd_rel->relcudescrelid, AccessShareLock);
            if (cstoreIdxRel != NULL) {
                size = calculate_table_file_size(cstoreIdxRel, true, forkNumOption);
                relation_close(cstoreIdxRel, AccessShareLock);
            }
        }
    } else {
        List* partOids = NIL;
        ListCell* cell = NULL;
        Oid partOid = InvalidOid;
        Oid partIndexOid = InvalidOid;
        Partition partIndex = NULL;
        Relation partIndexRel = NULL;
        Relation cstorePartIndexRel = NULL;

        partOids = RelationIsSubPartitioned(baseRel) ? RelationGetSubPartitionOidList(baseRel) :
                                                       relationGetPartitionOidList(baseRel);

        foreach (cell, partOids) {
            partOid = lfirst_oid(cell);
            partIndexOid = getPartitionIndexOid(RelationGetRelid(rel), partOid);
            if (!OidIsValid(partIndexOid)) {
                continue;
            }
            partIndex = partitionOpen(rel, partIndexOid, AccessShareLock);
            partIndexRel = partitionGetRelation(rel, partIndex);

            if (bCstore) {
                cstorePartIndexRel = relation_open(partIndexRel->rd_rel->relcudescrelid, AccessShareLock);
                size += calculate_table_file_size(cstorePartIndexRel, true, forkNumOption);
                relation_close(cstorePartIndexRel, AccessShareLock);
            } else
                size += calculate_table_file_size(partIndexRel, false, forkNumOption);

            partitionClose(rel, partIndex, AccessShareLock);
            releaseDummyRelation(&partIndexRel);
        }

        releasePartitionOidList(&partOids);
#ifdef ENABLE_MULTIPLE_NODES
        if (RelationIsTsStore(rel)) {
            size += CalculateTsTagRelationSize(rel, forkNumOption);
        }
#endif
    }
    relation_close(baseRel, AccessShareLock);
    return size;
}

/*
 * Calculate total on-disk size of a given table,
 * including FSM and VM, plus TOAST table if any.
 * Indexes other than the TOAST table's index are not included.
 *
 * Note that this also behaves sanely if applied to an index or toast table;
 * those won't have attached toast tables, but they can have multiple forks.
 */
static int64 calculate_table_size(Relation rel, int forkNumOption)
{
    int64 size = 0;

    if (!RelationIsIndex(rel)) {
        size = CalculateRelSize(rel, forkNumOption);
    } else {
        size = CalculateIndexSize(rel, forkNumOption);
    }
    return size;
}

/*
 * Calculate total on-disk size of all indexes attached to the given table.
 *
 * Can be applied safely to an index, but you'll just get zero.
 */
static int64 calculate_indexes_size(Relation rel)
{
    int64 size = 0;

    /*
     * Aggregate all indexes on the given relation
     */
    if (rel->rd_rel->relhasindex) {
        List* index_oids = RelationGetIndexList(rel);
        ListCell* cell = NULL;

        foreach (cell, index_oids) {
            Oid idxOid = lfirst_oid(cell);
            Relation idxRel = NULL;

            idxRel = relation_open(idxOid, AccessShareLock);

            size += calculate_table_size(idxRel, DEFAULT_FORKNUM);

            relation_close(idxRel, AccessShareLock);
        }

        list_free_ext(index_oids);
    }

    return size;
}

Datum pg_relation_size(PG_FUNCTION_ARGS)
{
    Oid relOid = PG_GETARG_OID(0);
    text* forkName = PG_GETARG_TEXT_P(1);
    Relation rel;
    int64 size = 0;

#ifdef PGXC
    if (COLLECT_FROM_DATANODES(relOid)) {
        size = pgxc_exec_sizefunc(relOid, "pg_relation_size", text_to_cstring(forkName));
        PG_RETURN_INT64(size);
    }
#endif /* PGXC */

    rel = try_relation_open(relOid, AccessShareLock);

    /*
     * Before 9.2, we used to throw an error if the relation didn't exist, but
     * that makes queries like "SELECT pg_relation_size(oid) FROM pg_class"
     * less robust, because while we scan pg_class with an MVCC snapshot,
     * someone else might drop the table. It's better to return NULL for
     * already-dropped tables than throw an error and abort the whole query.
     */
    if (rel == NULL)
        PG_RETURN_NULL();

    ForkNumber forknumber = forkname_to_number(text_to_cstring(forkName));

    size = calculate_table_size(rel, forknumber);

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64(size);
}

Datum pg_table_size(PG_FUNCTION_ARGS)
{
    Oid relOid = PG_GETARG_OID(0);
    Relation rel = NULL;
    int64 size;

    /* acquire lock to prevent relation drop in other parallel */
    rel = try_relation_open(relOid, AccessShareLock);

    if (!RelationIsValid(rel)) {
        PG_RETURN_NULL();
    }

#ifdef PGXC
    if (COLLECT_FROM_DATANODES(relOid)) {
        size = pgxc_exec_sizefunc(relOid, "pg_table_size", NULL);
    } else {
#else
    if (true) {
#endif
        size = calculate_table_size(rel, DEFAULT_FORKNUM);
    }

    relation_close(rel, AccessShareLock);
    rel = NULL;

    PG_RETURN_INT64(size);
}

Datum pg_partition_size_oid(PG_FUNCTION_ARGS)
{
    Oid partTableOid = PG_GETARG_OID(0);
    Oid partOid = PG_GETARG_OID(1);
    int64 size = 0;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && (GetRelationLocInfo((partTableOid)) != NULL)) {
        PG_RETURN_INT64(pgxc_exec_partition_sizefunc(partTableOid, partOid, "pg_partition_size"));
    }
#endif

    size = calculate_partition_size(partTableOid, partOid);

    PG_RETURN_INT64(size);
}

Datum pg_partition_size_name(PG_FUNCTION_ARGS)
{
    char* partTableName = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* partName = text_to_cstring(PG_GETARG_TEXT_PP(1));
    Oid partTableOid = InvalidOid;
    Oid partOid = InvalidOid;
    List* names = NIL;
    int64 size = 0;

    names = stringToQualifiedNameList(partTableName);
    partTableOid = RangeVarGetRelid(makeRangeVarFromNameList(names), NoLock, false);

    partOid = partitionNameGetPartitionOid(
        partTableOid, partName, PART_OBJ_TYPE_TABLE_PARTITION, NoLock, false, false, NULL, NULL, NoLock);

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && (GetRelationLocInfo((partTableOid)) != NULL)) {
        PG_RETURN_INT64(pgxc_exec_partition_sizefunc(partTableOid, partOid, "pg_partition_size"));
    }
#endif

    size = calculate_partition_size(partTableOid, partOid);

    PG_RETURN_INT64(size);
}

static int64 calculate_partition_size(Oid partTableOid, Oid partOid)
{
    Relation partTableRel = NULL;
    Partition partition = NULL;
    Relation partRel = NULL;
    int64 size = 0;

    partTableRel = try_relation_open(partTableOid, AccessShareLock);

    if (partTableRel == NULL) {
        return 0;
    }

    partition = partitionOpen(partTableRel, partOid, AccessShareLock);
    partRel = partitionGetRelation(partTableRel, partition);

    size = calculate_table_file_size(partRel, RelationIsColStore(partRel), DEFAULT_FORKNUM);

    partitionClose(partTableRel, partition, AccessShareLock);
    releaseDummyRelation(&partRel);
    relation_close(partTableRel, AccessShareLock);

    return size;
}

Datum pg_partition_indexes_size_oid(PG_FUNCTION_ARGS)
{
    Oid partTableOid = PG_GETARG_OID(0);
    Oid partOid = PG_GETARG_OID(1);
    int64 size = 0;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && (GetRelationLocInfo((partTableOid)) != NULL)) {
        PG_RETURN_INT64(pgxc_exec_partition_sizefunc(partTableOid, partOid, "pg_partition_indexes_size"));
    }
#endif

    size = calculate_partition_indexes_size(partTableOid, partOid);

    PG_RETURN_INT64(size);
}

Datum pg_partition_indexes_size_name(PG_FUNCTION_ARGS)
{
    char* partTableName = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* partName = text_to_cstring(PG_GETARG_TEXT_PP(1));
    Oid partTableOid = InvalidOid;
    Oid partOid = InvalidOid;
    List* names = NIL;
    int64 size = 0;

    names = stringToQualifiedNameList(partTableName);
    partTableOid = RangeVarGetRelid(makeRangeVarFromNameList(names), NoLock, false);

    partOid = partitionNameGetPartitionOid(
        partTableOid, partName, PART_OBJ_TYPE_TABLE_PARTITION, NoLock, false, false, NULL, NULL, NoLock);

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && (GetRelationLocInfo((partTableOid)) != NULL)) {
        PG_RETURN_INT64(pgxc_exec_partition_sizefunc(partTableOid, partOid, "pg_partition_indexes_size"));
    }
#endif

    size = calculate_partition_indexes_size(partTableOid, partOid);

    PG_RETURN_INT64(size);
}

static int64 calculate_partition_indexes_size(Oid partTableOid, Oid partOid)
{
    Relation partTableRel = NULL;
    List* indexOids = NIL;
    ListCell* cell = NULL;
    int64 size = 0;

    partTableRel = try_relation_open(partTableOid, AccessShareLock);

    if (partTableRel == NULL) {
        return 0;
    }

    indexOids = RelationGetSpecificKindIndexList(partTableRel, false);

    foreach (cell, indexOids) {
        Oid indexOid = lfirst_oid(cell);
        Relation indexRel = NULL;
        Oid partIndexOid = InvalidOid;
        Partition partIndex = NULL;
        Relation partIndexRel = NULL;

        indexRel = relation_open(indexOid, AccessShareLock);
        partIndexOid = getPartitionIndexOid(indexOid, partOid);
        partIndex = partitionOpen(indexRel, partIndexOid, AccessShareLock);
        partIndexRel = partitionGetRelation(indexRel, partIndex);

        size += calculate_table_size(partIndexRel, DEFAULT_FORKNUM);

        partitionClose(indexRel, partIndex, AccessShareLock);
        releaseDummyRelation(&partIndexRel);
        relation_close(indexRel, AccessShareLock);
    }

    list_free_ext(indexOids);
    relation_close(partTableRel, AccessShareLock);

    return size;
}

Datum pg_indexes_size(PG_FUNCTION_ARGS)
{
    Oid relOid = PG_GETARG_OID(0);
    Relation rel;
    int64 size;

#ifdef PGXC
    if (COLLECT_FROM_DATANODES(relOid))
        PG_RETURN_INT64(pgxc_exec_sizefunc(relOid, "pg_indexes_size", NULL));
#endif /* PGXC */

    rel = try_relation_open(relOid, AccessShareLock);

    if (rel == NULL)
        PG_RETURN_NULL();

    size = calculate_indexes_size(rel);

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64(size);
}

/*
 *	Compute the on-disk size of all files for the relation,
 *	including heap data, index data, toast data, FSM, VM.
 */
static int64 calculate_total_relation_size(Relation rel)
{
    int64 size;

    /*
     * Aggregate the table size, this includes size of the heap, toast and
     * toast index with free space and visibility map
     */
    size = calculate_table_size(rel, DEFAULT_FORKNUM);

    /*
     * Add size of all attached indexes as well
     */
    size += calculate_indexes_size(rel);

    return size;
}

Datum pg_total_relation_size(PG_FUNCTION_ARGS)
{
    Oid relOid = PG_GETARG_OID(0);
    Relation rel;
    int64 size;

#ifdef PGXC
    if (COLLECT_FROM_DATANODES(relOid))
        PG_RETURN_INT64(pgxc_exec_sizefunc(relOid, "pg_total_relation_size", NULL));
#endif /* PGXC */

    rel = try_relation_open(relOid, AccessShareLock);

    if (rel == NULL)
        PG_RETURN_NULL();

    size = calculate_total_relation_size(rel);

    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64(size);
}

/*
 * formatting with size units
 */
Datum pg_size_pretty(PG_FUNCTION_ARGS)
{
    int64 size = PG_GETARG_INT64(0);
    char buf[64];
    const int64 limit = 10 * 1024;
    int64 limit2 = limit * 2 - 1;
    errno_t rc = EOK;

    if (size < 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Parameter value should not be negative.")));
        return (Datum)0;
    }
    if (size < limit)
        rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, INT64_FORMAT " bytes", size);
    else {
        size >>= 9; /* keep one extra bit for rounding */
        if (size < limit2)
            rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, INT64_FORMAT " kB", (size + 1) / 2);

        else {
            size >>= 10;
            if (size < limit2)
                rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, INT64_FORMAT " MB", (size + 1) / 2);
            else {
                size >>= 10;
                if (size < limit2)
                    rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, INT64_FORMAT " GB", (size + 1) / 2);

                else {
                    size >>= 10;
                    rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, INT64_FORMAT " TB", (size + 1) / 2);
                }
            }
        }
    }
    securec_check_ss(rc, "\0", "\0");
    PG_RETURN_TEXT_P(cstring_to_text(buf));
}

static char* numeric_to_cstring(Numeric n)
{
    Datum d = NumericGetDatum(n);

    return DatumGetCString(DirectFunctionCall1(numeric_out, d));
}

Numeric int64_to_numeric(int64 v)
{
    Datum d = Int64GetDatum(v);

    return DatumGetNumeric(DirectFunctionCall1(int8_numeric, d));
}

static bool numeric_is_less(Numeric a, Numeric b)
{
    Datum da = NumericGetDatum(a);
    Datum db = NumericGetDatum(b);

    return DatumGetBool(DirectFunctionCall2(numeric_lt, da, db));
}

static Numeric numeric_plus_one_over_two(Numeric n)
{
    Datum d = NumericGetDatum(n);
    Datum one;
    Datum two;
    Datum result;

    one = DirectFunctionCall1(int8_numeric, Int64GetDatum(1));
    two = DirectFunctionCall1(int8_numeric, Int64GetDatum(2));
    result = DirectFunctionCall2(numeric_add, d, one);
    result = DirectFunctionCall2(numeric_div_trunc, result, two);
    return DatumGetNumeric(result);
}

static Numeric numeric_shift_right(Numeric n, unsigned count)
{
    Datum d = NumericGetDatum(n);
    Datum divisor_int64;
    Datum divisor_numeric;
    Datum result;

    divisor_int64 = Int64GetDatum((int64)((uint64)1 << count));
    divisor_numeric = DirectFunctionCall1(int8_numeric, divisor_int64);
    result = DirectFunctionCall2(numeric_div_trunc, d, divisor_numeric);
    return DatumGetNumeric(result);
}

Datum pg_size_pretty_numeric(PG_FUNCTION_ARGS)
{
    Numeric size = PG_GETARG_NUMERIC(0);
    Numeric limit, limit2;
    char *buf = NULL, *result = NULL;
    errno_t rc = EOK;
    int result_size;

    if (numeric_is_less(size, int64_to_numeric(0))) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Parameter value should not be negative.")));
        return (Datum)0;
    }
    limit = int64_to_numeric(10 * 1024);
    limit2 = int64_to_numeric(10 * 1024 * 2 - 1);

    if (numeric_is_less(size, limit)) {
        buf = numeric_to_cstring(size);
        result_size = strlen(buf) + 7;
        result = (char*)palloc(result_size);
        rc = strcpy_s(result, result_size, buf);
        securec_check(rc, "\0", "\0");
        rc = strcat_s(result, result_size, " bytes");
        securec_check(rc, "\0", "\0");
    } else {
        /* keep one extra bit for rounding */
        /* size >>= 9 */
        size = numeric_shift_right(size, 9);

        if (numeric_is_less(size, limit2)) {
            /* size = (size + 1) / 2 */
            size = numeric_plus_one_over_two(size);
            buf = numeric_to_cstring(size);
            result_size = strlen(buf) + 4;
            result = (char*)palloc(result_size);
            rc = strcpy_s(result, result_size, buf);
            securec_check(rc, "\0", "\0");
            rc = strcat_s(result, result_size, " kB");
            securec_check(rc, "\0", "\0");
        } else {
            /* size >>= 10 */
            size = numeric_shift_right(size, 10);
            if (numeric_is_less(size, limit2)) {
                /* size = (size + 1) / 2 */
                size = numeric_plus_one_over_two(size);
                buf = numeric_to_cstring(size);
                result_size = strlen(buf) + 4;
                result = (char*)palloc(result_size);
                rc = strcpy_s(result, result_size, buf);
                securec_check(rc, "\0", "\0");
                rc = strcat_s(result, result_size, " MB");
                securec_check(rc, "\0", "\0");
            } else {
                /* size >>= 10 */
                size = numeric_shift_right(size, 10);

                if (numeric_is_less(size, limit2)) {
                    /* size = (size + 1) / 2 */
                    size = numeric_plus_one_over_two(size);
                    buf = numeric_to_cstring(size);
                    result_size = strlen(buf) + 4;
                    result = (char*)palloc(result_size);
                    rc = strcpy_s(result, result_size, buf);
                    securec_check(rc, "\0", "\0");
                    rc = strcat_s(result, result_size, " GB");
                    securec_check(rc, "\0", "\0");
                } else {
                    /* size >>= 10 */
                    size = numeric_shift_right(size, 10);
                    /* size = (size + 1) / 2 */
                    size = numeric_plus_one_over_two(size);
                    buf = numeric_to_cstring(size);
                    result_size = strlen(buf) + 4;
                    result = (char*)palloc(result_size);
                    rc = strcpy_s(result, result_size, buf);
                    securec_check(rc, "\0", "\0");
                    rc = strcat_s(result, result_size, " TB");
                    securec_check(rc, "\0", "\0");
                }
            }
        }
    }

    PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * Get the filenode of a relation
 *
 * This is expected to be used in queries like
 *		SELECT pg_relation_filenode(oid) FROM pg_class;
 * That leads to a couple of choices.  We work from the pg_class row alone
 * rather than actually opening each relation, for efficiency.	We don't
 * fail if we can't find the relation --- some rows might be visible in
 * the query's MVCC snapshot but already dead according to SnapshotNow.
 * (Note: we could avoid using the catcache, but there's little point
 * because the relation mapper also works "in the now".)  We also don't
 * fail if the relation doesn't have storage.  In all these cases it
 * seems better to quietly return NULL.
 */
Datum pg_relation_filenode(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    Oid result;
    HeapTuple tuple;
    Form_pg_class relform;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        PG_RETURN_NULL();
    relform = (Form_pg_class)GETSTRUCT(tuple);

    switch (relform->relkind) {
        case RELKIND_RELATION:
        case RELKIND_MATVIEW:
        case RELKIND_INDEX:
        case RELKIND_GLOBAL_INDEX:
        case RELKIND_SEQUENCE:
        case RELKIND_LARGE_SEQUENCE:
        case RELKIND_TOASTVALUE:
            /* okay, these have storage */
            if (relform->relfilenode)
                result = relform->relfilenode;
            else /* Consult the relation mapper */
                result = RelationMapOidToFilenode(relid, relform->relisshared);
            break;

        default:
            /* no storage, return NULL */
            result = InvalidOid;
            break;
    }

    ReleaseSysCache(tuple);

    if (!OidIsValid(result))
        PG_RETURN_NULL();

    PG_RETURN_OID(result);
}

/*
 * Get the relation via (reltablespace, relfilenode)
 *
 * This is expected to be used when somebody wants to match an individual file
 * on the filesystem back to its table. Thats not trivially possible via
 * pg_class because that doesn't contain the relfilenodes of shared and nailed
 * tables.
 *
 * We don't fail but return NULL if we cannot find a mapping.
 *
 * Instead of knowing DEFAULTTABLESPACE_OID you can pass 0.
 */
Datum pg_filenode_relation(PG_FUNCTION_ARGS)
{
    Oid reltablespace = PG_GETARG_OID(0);
    Oid relfilenode = PG_GETARG_OID(1);
    Oid heaprel = InvalidOid;

    heaprel = RelidByRelfilenode(reltablespace, relfilenode, false);

    if (!OidIsValid(heaprel))
        PG_RETURN_NULL();
    else
        PG_RETURN_OID(heaprel);
}

/*
 * Get the filenode of a partition
 * This is expected to be used in queries like
 *     SELECT pg_partition_filenode(oid);
 */
Datum pg_partition_filenode(PG_FUNCTION_ARGS)
{
    Oid partRelId = PG_GETARG_OID(0);
    Oid result;
    HeapTuple tuple;
    Form_pg_partition partRelForm;

    tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partRelId));
    if (!HeapTupleIsValid(tuple))
        PG_RETURN_NULL();

    partRelForm = (Form_pg_partition)GETSTRUCT(tuple);
    switch (partRelForm->parttype) {
        case PART_OBJ_TYPE_PARTED_TABLE:
        case PART_OBJ_TYPE_TABLE_PARTITION:
        case PART_OBJ_TYPE_INDEX_PARTITION:
        case PART_OBJ_TYPE_TOAST_TABLE:
            // okay, these have storage
            if (partRelForm->relfilenode)
                result = partRelForm->relfilenode;
            else
                result = RelationMapOidToFilenode(partRelId, false);
            break;

        default:
            // no storage, return NULL
            result = InvalidOid;
            break;
    }

    ReleaseSysCache(tuple);

    if (!OidIsValid(result))
        PG_RETURN_NULL();

    PG_RETURN_OID(result);
}

/*
 *  * Get the pathname (relative to $PGDATA) of a partition
 *   *
 *    */
Datum pg_partition_filepath(PG_FUNCTION_ARGS)
{
    Oid partRelId = PG_GETARG_OID(0);
    HeapTuple tuple;
    Form_pg_partition partRelForm;
    RelFileNode rnode;
    BackendId backend = InvalidBackendId;
    char* path = NULL;

    tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partRelId));
    if (!HeapTupleIsValid(tuple))
        PG_RETURN_NULL();
    partRelForm = (Form_pg_partition)GETSTRUCT(tuple);

    switch (partRelForm->parttype) {
        case PARTTYPE_PARTITIONED_RELATION:
        case PARTTYPE_VALUE_PARTITIONED_RELATION:
            rnode.spcNode = ConvertToRelfilenodeTblspcOid(partRelForm->reltablespace);
            if (rnode.spcNode == GLOBALTABLESPACE_OID) {
                rnode.dbNode = InvalidOid;
            } else {
                rnode.dbNode = u_sess->proc_cxt.MyDatabaseId;
            }
            if (partRelForm->relfilenode) {
                rnode.relNode = partRelForm->relfilenode;
            } else {
                /* Consult the relation mapper */
                rnode.relNode = RelationMapOidToFilenode(partRelId, false);
            }
            rnode.bucketNode = InvalidBktId;
            break;

        default:
            /* no storage, return NULL */
            rnode.relNode = InvalidOid;
            /* some compilers generate warnings without these next two lines */
            rnode.dbNode = InvalidOid;
            rnode.spcNode = InvalidOid;
            rnode.bucketNode = InvalidBktId;
            break;
    }

    if (!OidIsValid(rnode.relNode)) {
        ReleaseSysCache(tuple);
        PG_RETURN_NULL();
    }

    ReleaseSysCache(tuple);
    path = relpathbackend(rnode, backend, MAIN_FORKNUM);
    PG_RETURN_TEXT_P(cstring_to_text(path));
}

/*
 * Get the pathname (relative to $PGDATA) of a relation
 *
 * See comments for pg_relation_filenode.
 */
Datum pg_relation_filepath(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    HeapTuple tuple;
    Form_pg_class relform;
    RelFileNode rnode;
    BackendId backend;
    char* path = NULL;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        PG_RETURN_NULL();
    relform = (Form_pg_class)GETSTRUCT(tuple);

    switch (relform->relkind) {
        case RELKIND_RELATION:
        case RELKIND_MATVIEW:
        case RELKIND_INDEX:
        case RELKIND_GLOBAL_INDEX:
        case RELKIND_SEQUENCE:
        case RELKIND_LARGE_SEQUENCE:
        case RELKIND_TOASTVALUE:
            /* okay, these have storage */

            /* This logic should match RelationInitPhysicalAddr */
            rnode.spcNode = ConvertToRelfilenodeTblspcOid(relform->reltablespace);
            if (rnode.spcNode == GLOBALTABLESPACE_OID)
                rnode.dbNode = InvalidOid;
            else
                rnode.dbNode = u_sess->proc_cxt.MyDatabaseId;
            if (relform->relfilenode)
                rnode.relNode = relform->relfilenode;
            else /* Consult the relation mapper */
                rnode.relNode = RelationMapOidToFilenode(relid, relform->relisshared);
            rnode.bucketNode = InvalidBktId;
            break;

        default:
            /* no storage, return NULL */
            rnode.relNode = InvalidOid;
            /* some compilers generate warnings without these next two lines */
            rnode.dbNode = InvalidOid;
            rnode.spcNode = InvalidOid;
            rnode.bucketNode = InvalidBktId;
            break;
    }

    if (!OidIsValid(rnode.relNode)) {
        ReleaseSysCache(tuple);
        PG_RETURN_NULL();
    }

    /* Determine owning backend. */
    switch (relform->relpersistence) {
        case RELPERSISTENCE_UNLOGGED:
        case RELPERSISTENCE_PERMANENT:
        case RELPERSISTENCE_TEMP:
            backend = InvalidBackendId;
            break;

        case RELPERSISTENCE_GLOBAL_TEMP:
            backend = BackendIdForTempRelations;
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("invalid relpersistence: %c", relform->relpersistence)));
            backend = InvalidBackendId; /* placate compiler */
            break;
    }

    ReleaseSysCache(tuple);

    path = relpathbackend(rnode, backend, MAIN_FORKNUM);

    PG_RETURN_TEXT_P(cstring_to_text(path));
}

#ifdef PGXC

/*
 * pgxc_tablespace_size
 * Given a tablespace oid, return sum of pg_tablespace_size() executed on all the Datanodes
 */
static Datum pgxc_tablespace_size(Oid tsOid)
{
    StringInfoData buf;
    char* tsname = get_tablespace_name(tsOid);
    ParallelFunctionState* state = NULL;
    int64 size = 0;

    if (NULL == tsname)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("tablespace with OID %u does not exist", tsOid)));

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.pg_tablespace_size('%s')", tsname);

    state = RemoteFunctionResultHandler(buf.data, NULL, StrategyFuncSum);
    size = state->result;
    FreeParallelFunctionState(state);

    /*
     * If the type of specific tablespace is HDFS , calculate size of database on HDFS.
     */
    size += calculate_tablespace_size_on_dfs(tsOid);

    PG_RETURN_INT64(size);
}

/*
 * @Description: get the source database size from CN.
 * @return: the size of database on all DNs.
 */
static Datum pgxc_source_database_size()
{
    StringInfoData buf;
    ParallelFunctionState* state = NULL;
    int64 size = 0;
    /*the default compressed ratio of hdfs tabls is 3 now.*/
    const double hdfs_ratio = 3;

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.get_db_source_datasize()");

    state = RemoteFunctionResultHandler(buf.data, NULL, StrategyFuncSum);
    size = state->result;

    FreeParallelFunctionState(state);

    /*
     * If the specified database exists on HDFS, calculate size of database on HDFS.
     */
    size += calculate_dbsize_on_dfs(u_sess->proc_cxt.MyDatabaseId) * hdfs_ratio;

    pfree_ext(buf.data);

    PG_RETURN_INT64(size);
}

/*
 * pgxc_database_size
 * Given a dboid, return sum of pg_database_size() executed on all the Datanodes
 */
static Datum pgxc_database_size(Oid dbOid)
{
    StringInfoData buf;
    char* dbname = get_and_check_db_name(dbOid, true);
    ParallelFunctionState* state = NULL;
    int64 size = 0;

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.pg_database_size('%s')", dbname);

    state = RemoteFunctionResultHandler(buf.data, NULL, StrategyFuncSum);
    size = state->result;

    FreeParallelFunctionState(state);

    /*
     * If the specified database exists on HDFS, calculate size of database on HDFS.
     */
    size += calculate_dbsize_on_dfs(dbOid);

    PG_RETURN_INT64(size);
}

/*
 * pgxc_execute_on_nodes
 * Execute 'query' on all the nodes in 'nodelist', and returns int64 datum
 * which has the sum of all the results. If multiples nodes are involved, it
 * assumes that the query returns exactly one row with one attribute of type
 * int64. If there is a single node, it just returns the datum as-is without
 * checking the type of the returned value.
 */
Datum pgxc_execute_on_nodes(int numnodes, Oid* nodelist, char* query)
{
    StringInfoData buf;
    int ret;
    TupleDesc spi_tupdesc;
    int i;
    int64 total_size = 0;
    int64 size = 0;
    bool isnull = false;
    char* nodename = NULL;
    Datum datum = (Datum)0;

    /*
     * Connect to SPI manager
     */
    SPI_STACK_LOG("connect", NULL, NULL);
    if ((ret = SPI_connect()) < 0)
        /* internal error */
        ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE), errmsg("SPI connect failure - returned %d", ret)));

    initStringInfo(&buf);

    /* Get pg_***_size function results from all Datanodes */
    for (i = 0; i < numnodes; i++) {
        nodename = get_pgxc_nodename((Oid)nodelist[i]);

        ret = SPI_execute_direct(query, nodename);
        spi_tupdesc = SPI_tuptable->tupdesc;

        if (ret != SPI_OK_SELECT) {
            ereport(ERROR,
                (errcode(ERRCODE_SPI_EXECUTE_FAILURE),
                    errmsg("failed to execute query '%s' on node '%s'", query, nodename)));
        }

        if (SPI_processed != 1) {
            // will retry
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_RECEIVE_WRONG), errmsg("expected 1 row, actual %u row", SPI_processed)));
        }

        /*
         * The query must always return one row having one column:
         */
        Assert(SPI_processed == 1 && spi_tupdesc->natts == 1);

        datum = SPI_getbinval(SPI_tuptable->vals[0], spi_tupdesc, 1, &isnull);

        /* For single node, don't assume the type of datum. It can be bool also. */
        if (numnodes == 1)
            break;

        size = DatumGetInt64(datum);
        total_size += size;
    }

    SPI_STACK_LOG("finish", NULL, NULL);
    SPI_finish();

    if (numnodes == 1)
        PG_RETURN_DATUM(datum);
    else
        PG_RETURN_INT64(total_size);
}

/*
 * @@GaussDB@@
 * Target		: get  table oid from toast table  name
 * Brief		: toast table name  always like pg_toast_table_oid, get toast table name by its oid,
 *			: then get base table oid by spliting toast table name using function  text_substr_no_len_orclcompat.
 * Description	:
 * Input		:
 * Output	:
 * Notes		:
 */
Oid pg_toast_get_baseid(Oid relOid, bool* isPartToast)
{
    Oid base_table_oid;
    const char* table_oid_str = NULL;
    Datum toast_len = Int32GetDatum(10);
    Relation rel;

    rel = relation_open(relOid, AccessShareLock);

    if (strstr(RelationGetRelationName(rel), "part") != NULL) {
        *isPartToast = true;

        toast_len = Int32GetDatum(15);
    }

    table_oid_str = TextDatumGetCString(DirectFunctionCall2(
        text_substr_no_len_orclcompat, CStringGetTextDatum(RelationGetRelationName(rel)), toast_len));
    base_table_oid = DatumGetInt32(DirectFunctionCall1(int4in, CStringGetDatum(table_oid_str)));

    relation_close(rel, AccessShareLock);
    return base_table_oid;
}

bool IsToastRelationbyOid(Oid tbl_oid)
{
    Relation rel;
    bool result = false;
    rel = relation_open(tbl_oid, AccessShareLock);
    result = IsToastNamespace(RelationGetNamespace(rel));
    relation_close(rel, AccessShareLock);
    return result;
}

static bool IsIndexRelationbyOid(Oid tbl_oid)
{
    Relation rel;
    bool result = false;
    rel = relation_open(tbl_oid, AccessShareLock);
    result = RelationIsIndex(rel);
    relation_close(rel, AccessShareLock);
    return result;
}

static void AddRemoteToastBuf(bool isPartToast, Relation rel, char* funcname, char* extra_arg,
                              Relation partTableRel, Partition part, StringInfo buf)
{

    // toast table is belog to a ordinary table
    if (!isPartToast) {
        char *relname = RelationGetRelationName(rel);
        relname = repairObjectName(relname);
        if (NULL == extra_arg) {
            appendStringInfo(buf,
                "SELECT pg_catalog.%s(a.oid)  from  pg_class a,pg_class b where a.oid = b.reltoastrelid and "
                "b.relname='%s'",
                funcname,
                relname);
        } else {
            appendStringInfo(buf,
                "SELECT pg_catalog.%s(a.oid, '%s') from  pg_class a,pg_class b where a.oid = b.reltoastrelid and "
                "b.relname='%s'",
                funcname,
                extra_arg,
                relname);
        }
    }
    // toast table is belog to a partitoin
    else {
        char* partTableName = RelationGetRelationName(partTableRel);
        char* partName = PartitionGetPartitionName(part);

        partTableName = repairObjectName(partTableName);
        partName = repairObjectName(partName);

        if (NULL == extra_arg) {
            appendStringInfo(buf,
                "SELECT pg_catalog.%s(toast_table.oid) from pg_class toast_table, pg_class part_table, "
                "pg_partition part where toast_table.oid = part.reltoastrelid and part_table.oid=part.parentid and "
                "part_table.relname='%s' and part.relname='%s'",
                funcname,
                partTableName,
                partName);
        } else {
            appendStringInfo(buf,
                "SELECT pg_catalog.%s(toast_table.oid, '%s') from pg_class toast_table, pg_class part_table, "
                "pg_partition part where toast_table.oid = part.reltoastrelid and part_table.oid=part.parentid and "
                "part_table.relname='%s' and part.relname='%s'",
                funcname,
                extra_arg,
                partTableName,
                partName);
        }
    }
}

static void AddRemoteMatviewBuf(Relation rel, char* funcname, char* extra_arg,
                                MvRelationType matviewRelationType, StringInfo buf)
{
    char* relname = RelationGetRelationName(rel);
    relname = repairObjectName(relname);
    if (NULL == extra_arg) {
        appendStringInfo(buf,
            "SELECT pg_catalog.%s(a.oid) from pg_class a, "
            "(select oid from pg_class b where b.relname='%s') c "
            "where a.relname = (select concat('%s', CAST(c.oid as varchar)))",
            funcname,
            relname,
            matviewRelationType == MATVIEW_MAP ? MATMAPNAME : MLOGNAME);
    } else {
        appendStringInfo(buf,
            "SELECT pg_catalog.%s(a.oid, '%s') from pg_class a, "
            "(select oid from pg_class b where b.relname='%s') c "
            "where a.relname = (select concat('%s', CAST(c.oid as varchar)))",
            funcname,
            extra_arg,
            relname,
            matviewRelationType == MATVIEW_MAP ? MATMAPNAME : MLOGNAME);
    }
}

/*
 * pgxc_exec_sizefunc
 * Execute the given object size system function on all the Datanodes associated
 * with relOid, and return the sum of all.
 *
 * Args:
 *
 * relOid: Oid of the table for which the object size function is to be executed.
 *
 * funcname: Name of the system function.
 *
 * extra_arg: The first argument to such sys functions is always table name.
 * Some functions can have a second argument. To pass this argument, extra_arg
 * is used. Currently only pg_relation_size() is the only one that requires
 * a 2nd argument: fork text.
 */
static int64 pgxc_exec_sizefunc(Oid relOid, char* funcname, char* extra_arg)
{
    int numnodes;
    Oid* nodelist = NULL;
    Oid tbl_oid;
    char* relname = NULL;
    StringInfoData buf;
    Relation rel = NULL;
    bool isPartToast = false;
    Oid partTableOid = InvalidOid;
    Relation partTableRel = NULL;
    Partition part = NULL;
    ExecNodes* exec_nodes = makeNode(ExecNodes);
    int i = 0;
    ParallelFunctionState* state = NULL;
    int64 size = 0;
    MvRelationType matviewRelationType = MATVIEW_NOT;

    if (IsToastRelationbyOid(relOid)) {
        tbl_oid = pg_toast_get_baseid(relOid, &isPartToast);
    } else if (IsMatviewRelationbyOid(relOid, &matviewRelationType)) {
        tbl_oid = MatviewRelationGetBaseid(relOid, matviewRelationType);
    } else {
        tbl_oid = relOid;
    }

    if (!isPartToast) {
        rel = relation_open(tbl_oid, AccessShareLock);
    } else {
        partTableOid = partid_get_parentid(tbl_oid);
        partTableRel = relation_open(partTableOid, AccessShareLock);
        part = partitionOpen(partTableRel, tbl_oid, AccessShareLock);
    }

    initStringInfo(&buf);

    if (IsToastRelationbyOid(relOid)) {
        AddRemoteToastBuf(isPartToast, rel, funcname, extra_arg, partTableRel, part, &buf);
    } else if (IsMatviewRelationbyOid(relOid, &matviewRelationType)) {
        AddRemoteMatviewBuf(rel, funcname, extra_arg, matviewRelationType, &buf);
    } else {
        /* get relation name including any needed schema prefix and quoting */
        relname =
            quote_qualified_identifier(get_namespace_name(rel->rd_rel->relnamespace), RelationGetRelationName(rel));
        relname = repairObjectName(relname);
        NULL == extra_arg ? appendStringInfo(&buf, "SELECT pg_catalog.%s('%s')", funcname, relname) :
            appendStringInfo(&buf, "SELECT pg_catalog.%s('%s', '%s')", funcname, relname, extra_arg);
    }

    if (!isPartToast) {
        Oid tabRelOid = InvalidOid;
        RelationIsIndex(rel) ? tabRelOid = rel->rd_index->indrelid : tabRelOid = RelationGetRelid(rel);

        numnodes = get_pgxc_classnodes(tabRelOid, &nodelist);
        relation_close(rel, AccessShareLock);
    } else {
        numnodes = get_pgxc_classnodes(RelationGetRelid(partTableRel), &nodelist);
        partitionClose(partTableRel, part, AccessShareLock);
        relation_close(partTableRel, AccessShareLock);
    }

    // Build nodes to execute on
    //
    for (i = 0; i < numnodes; i++)
        exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, PGXCNodeGetNodeId(nodelist[i], PGXC_NODE_DATANODE));

    state = RemoteFunctionResultHandler(buf.data, exec_nodes, StrategyFuncSum);
    size = state->result;
    FreeParallelFunctionState(state);

    /*
     * If the relation is DFS table, must calculate data size on hdfs.
     */
    if (RelationIsPaxFormatByOid(relOid)) {
        Relation rel = relation_open(relOid, AccessShareLock);
        StringInfo dfsStorePath = getDfsStorePath(rel);
        Oid tblSpcOid = rel->rd_rel->reltablespace;
        size += calculate_dir_size_on_dfs(tblSpcOid, dfsStorePath);
        relation_close(rel, NoLock);
    }
    return size;
}

static int64 pgxc_exec_partition_sizefunc(Oid partTableOid, Oid partOid, char* funcName)
{
    int numnodes = 0;
    Oid* nodelist = NULL;
    char* partTableName = NULL;
    char* partName = NULL;
    Relation partTableRel = NULL;
    Partition partition = NULL;
    StringInfoData buf;
    ExecNodes* exec_nodes = makeNode(ExecNodes);
    int i = 0;
    ParallelFunctionState* state = NULL;
    int64 size = 0;

    partTableRel = relation_open(partTableOid, AccessShareLock);
    partition = partitionOpen(partTableRel, partOid, AccessShareLock);

    initStringInfo(&buf);

    if (partTableRel->rd_locator_info) {
        // get relation name including any needed schema prefix and quoting
        partTableName = quote_qualified_identifier(
            get_namespace_name(partTableRel->rd_rel->relnamespace), RelationGetRelationName(partTableRel));
        partTableName = repairObjectName(partTableName);
    }
    partName = partition->pd_part->relname.data;
    partName = repairObjectName(partName);

    appendStringInfo(&buf, "SELECT pg_catalog.%s('%s', '%s')", funcName, partTableName, partName);

    numnodes = get_pgxc_classnodes(RelationGetRelid(partTableRel), &nodelist);

    partitionClose(partTableRel, partition, AccessShareLock);
    relation_close(partTableRel, AccessShareLock);

    // Build nodes to execute on
    //
    for (i = 0; i < numnodes; i++)
        exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, PGXCNodeGetNodeId(nodelist[i], PGXC_NODE_DATANODE));

    state = RemoteFunctionResultHandler(buf.data, exec_nodes, StrategyFuncSum);
    size = state->result;
    FreeParallelFunctionState(state);

    return size;
}

// query the relation's compression attrubute
//
Datum pg_relation_with_compression(PG_FUNCTION_ARGS)
{
    text* relname = PG_GETARG_TEXT_P(0);
    List* name = textToQualifiedNameList(relname);
    RangeVar* relrv = makeRangeVarFromNameList(name);
    Relation rel = relation_openrv(relrv, AccessShareLock);

    if (rel == NULL)
        PG_RETURN_NULL();

    bool compressed = false;
    if (RelationIsColStore(rel))
        compressed = (0 != pg_strcasecmp(COMPRESSION_NO, RelationGetCompression(rel)));
    else
        compressed = RowRelationIsCompressed(rel);
    relation_close(rel, AccessShareLock);
    list_free_ext(name);
    PG_RETURN_BOOL(compressed);
}

// query the relation's compression ratio
//
Datum pg_relation_compression_ratio(PG_FUNCTION_ARGS)
{
    // Need complete it
    //
    PG_RETURN_FLOAT4(1.0);
}

/*
 * @Description: the table size, include index
 * @IN rel: relation of a table
 * @Return: table size
 * @See also:
 */
uint64 pg_relation_perm_table_size(Relation rel)
{
    /* get table size, include index */
    return (uint64)calculate_total_relation_size(rel);
}

/*
 * @Description: the table size
 * @IN rel: relation of a table
 * @Return: table size
 * @See also:
 */
uint64 pg_relation_table_size(Relation rel)
{
    /* get table size */
    return (uint64)calculate_table_size(rel, DEFAULT_FORKNUM);
}

/*
 * @Description : Parallel get max database size in all datanodes.
 * @in         	: group_name, nodegroup name
 * @out         : None
 * @return      : max database size in all datanodes of the nodegroup.
 */
Datum pgxc_max_datanode_size_name(PG_FUNCTION_ARGS)
{
    const char* group_name = NULL;
    int64 size;
    Oid group_oid;
    ExecNodes* exec_nodes = NULL;
    Oid* members = NULL;
    int nmembers;
    int64 max_size = 0;
    TupleTableSlot* slot = NULL;
    Name str = PG_GETARG_NAME(0);

    group_name = str->data;

    /* Only a DB administrator can remove cluster node groups */
    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to call gs_get_max_dbsize_name.")));

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        StringInfoData buf;
        ParallelFunctionState* state = NULL;

        group_oid = get_pgxc_groupoid(group_name);

        if (!OidIsValid(group_oid))
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("PGXC Group %s: group not defined", group_name)));

        nmembers = get_pgxc_groupmembers(group_oid, &members);
        if (nmembers == 0)
            PG_RETURN_INT64(0);

        exec_nodes = makeNode(ExecNodes);

        for (int i = 0; i < nmembers; i++) {
            int nodeId = PGXCNodeGetNodeId(members[i], PGXC_NODE_DATANODE);
            exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, nodeId);
        }

        initStringInfo(&buf);
        appendStringInfo(&buf, "SELECT SUM(pg_database_size(oid))::bigint FROM pg_database;");

        state = RemoteFunctionResultHandler(buf.data, exec_nodes, NULL, false);

        Assert(state && state->tupstore && state->tupdesc);
        slot = MakeSingleTupleTableSlot(state->tupdesc);

        for (;;) {
            bool isnull = false;

            if (!tuplestore_gettupleslot(state->tupstore, true, false, slot))
                break;

            size = DatumGetInt64(tableam_tslot_getattr(slot, 1, &isnull));
            if (!isnull && max_size < size)
                max_size = size;

            ExecClearTuple(slot);
        }

        ExecDropSingleTupleTableSlot(slot);

        FreeParallelFunctionState(state);
    }

    PG_RETURN_INT64(max_size);
}

/*
 * Indicate whether a relation is scannable.
 *
 * Currently, this is always true except for a materialized view which has not
 * been populated.
 */
Datum
pg_relation_is_scannable(PG_FUNCTION_ARGS)
{
   Oid         relid;
   Relation    relation;
   bool        result;

   relid = PG_GETARG_OID(0);
   relation = RelationIdGetRelation(relid);
   result = relation->rd_isscannable;
   RelationClose(relation);

   PG_RETURN_BOOL(result);
}

#endif /* PGXC */
