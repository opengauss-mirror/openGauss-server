/*
 * dbsize.c
 *		Database object size functions, and related inquiries
 *
 * Copyright (c) 2002-2012, PostgreSQL Global Development Group
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
#include "access/htup.h"
#include "access/reloptions.h"
#include "catalog/catalog.h"
#include "catalog/dfsstore_ctlg.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_hashbucket_fn.h"
#include "commands/defrem.h"
#include "commands/dbcommands.h"
#include "commands/tablespace.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "foreign/fdwapi.h"
#ifdef PGXC
#include "catalog/pgxc_node.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/execRemote.h"
#endif
#include "storage/fd.h"
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
#include "utils/tqual.h"
#include "catalog/pg_partition_fn.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "storage/cu.h"
#include "storage/custorage.h"
#include "storage/cstore_compress.h"
#include "vecexecutor/vecnodes.h"

#ifdef PGXC
static Datum pgxc_database_size(Oid db_oid);
static Datum pgxc_source_database_size();
static Datum pgxc_tablespace_size(Oid ts_oid);
static int64 pgxc_exec_sizefunc(Oid rel_oid, char* funcname, char* extra_arg);
static int64 pgxc_exec_partition_sizefunc(Oid part_table_oid, Oid part_oid, char* func_name);
static int64 calculate_partition_size(Oid part_table_oid, Oid part_oid);
int64 calculate_CStore_relation_size(Relation rel, ForkNumber fork_num);
int64 calculate_CStore_partitioned_relation_size(Relation rel, ForkNumber fork_num);
static int64 calculate_partition_indexes_size(Oid part_table_oid, Oid part_oid);
Oid pg_toast_get_baseid(Oid rel_oid, bool* is_part_toast);
bool IsToastRelationbyOid(Oid rel_oid);
static bool IsIndexRelationbyOid(Oid rel_oid);
static int64 calculate_toast_table_size(Oid toast_rel_oid);
static int64 calculate_dir_size_on_dfs(Oid tbl_spc_oid, StringInfo dir_path);
static int64 calculate_dbsize_on_dfs(Oid db_oid);
static int64 calculate_tablespace_size_on_dfs(Oid tbl_spc_oid);

#define DEFAULT_FORKNUM -1

/*
 * Below macro is important when the object size functions are called
 * for system catalog tables. For pg_catalog tables and other Coordinator-only
 * tables, we should return the data from Coordinator. If we don't find
 * locator info, that means it is a Coordinator-only table.
 */
#define COLLECT_FROM_DATANODES(relid)              \
    ((IS_PGXC_COORDINATOR && !IsConnFromCoord() && \
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

    if (unlikely(NULL == dirdesc)) {
        return 0;
    }
    while ((direntry = ReadDir(dirdesc, path)) != NULL) {
        struct stat fst;

        CHECK_FOR_INTERRUPTS();

        if (unlikely(strcmp(direntry->d_name, ".") == 0 || strcmp(direntry->d_name, "..") == 0)) {
            continue;
        }
        errno_t rc = snprintf_s(filename, MAXPGPATH, MAXPGPATH - 1, "%s/%s", path, direntry->d_name);
        securec_check_ss(rc, "\0", "\0");

        if (unlikely(stat(filename, &fst) < 0)) {
            if (errno == ENOENT) {
                continue;
            } else {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", filename)));
            }
        }
        dirsize += fst.st_size;
    }

    FreeDir(dirdesc);
    return dirsize;
}

/**
 * @Description: calculate tablespace size on DFS.
 * @in tbl_spc_oid, the tablespace oid.
 * @out none.
 * @return If the type of specified tablespace is HDFS, return
 * real size, otherwise return 0.
 */
static int64 calculate_tablespace_size_on_dfs(Oid tbl_spc_oid)
{
    int64 dir_size = 0;
    StringInfo store_path;

    if (!IsSpecifiedTblspc(tbl_spc_oid, FILESYSTEM_HDFS)) {
        return dir_size;
    }
    store_path = getHDFSTblSpcStorePath(tbl_spc_oid);

    dir_size = calculate_dir_size_on_dfs(tbl_spc_oid, store_path);

    pfree_ext(store_path->data);
    pfree_ext(store_path);
    return dir_size;
}

/**
 * @Description: calculate database size on DFS.
 * @in db_oid, the database oid.
 * @out none.
 * @return If the database exists on DFS, return real size,
 * otherwise return 0.
 */
static int64 calculate_dbsize_on_dfs(Oid db_oid)
{
    List* tbl_spc_list = HDFSTablespaceDirExistDatabase(db_oid);
    ListCell* lc = NULL;
    int64 dir_size = 0;

    if (NIL == tbl_spc_list) {
        return dir_size;
    }

    char* dbname = NULL;
    foreach (lc, tbl_spc_list) {
        StringInfo tbl_spc_info = (StringInfo)lfirst(lc);
        Oid tbl_spc_oid = get_tablespace_oid(tbl_spc_info->data, false);
        StringInfo store_path = getHDFSTblSpcStorePath(tbl_spc_oid);

        dbname = get_database_name(db_oid);
        if (dbname == NULL) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE),
                        errmsg("database with OID %u does not exist", db_oid)));
        }
        appendStringInfo(store_path, "/%s", dbname);
        dir_size += calculate_dir_size_on_dfs(tbl_spc_oid, store_path);

        pfree_ext(store_path->data);
        pfree_ext(store_path);
        pfree_ext(tbl_spc_info);
    }
    list_free_ext(tbl_spc_list);
    return dir_size;
}

/**
 * @Description: calculate DFS directory size.
 * @in tbl_spc_oid, the specified tablespace oid.
 * @out dir_path, the directory to be calculated.
 * @return return the directory size.
 */
static int64 calculate_dir_size_on_dfs(Oid tbl_spc_oid, StringInfo dir_path)
{
    FEATURE_NOT_PUBLIC_ERROR("HDFS is not yet supported.");
    return 0;
}

/*
 * calculate size of database in all tablespaces
 */
static int64 calculate_database_size(Oid db_oid)
{
    int64 totalsize;
    DIR* dirdesc = NULL;
    struct dirent* direntry = NULL;
    char dirpath[MAXPGPATH] = {'\0'};
    char pathname[MAXPGPATH] = {'\0'};
    char* dbname;
    AclResult acl_result;
    List* exist_tblspc_list = NIL;
    errno_t rc = EOK;

    dbname = get_database_name(db_oid);
    if (dbname == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE),
                    errmsg("database with OID %u does not exist", db_oid)));
    }

    /* User must have connect privilege for target database */
    acl_result = pg_database_aclcheck(db_oid, GetUserId(), ACL_CONNECT);
    if (acl_result != ACLCHECK_OK) {
        aclcheck_error(acl_result, ACL_KIND_DATABASE, dbname);
    }
    exist_tblspc_list = HDFSTablespaceDirExistDatabase(db_oid);
    if (IS_PGXC_DATANODE && IsConnFromApp() && exist_tblspc_list != NIL) {
        StringInfo exist_tbl_spc = (StringInfo)linitial(exist_tblspc_list);
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("It is unsupported to calculate size of database \"%s\""
                       " under the DFS tablespace \"%s\" on data node.",
                    dbname,
                    exist_tbl_spc->data),
                errdetail("Please calculate size of database \"%s\" on coordinator node.", dbname)));
    }

    /* Shared storage in pg_global is not counted */

    /* Include pg_default storage */
    rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "base/%u", db_oid);
    securec_check_ss(rc, "\0", "\0");
    totalsize = db_dir_size(pathname);

    /* Scan the non-default tablespaces */
    rc = snprintf_s(dirpath, MAXPGPATH, MAXPGPATH - 1, "pg_tblspc");
    securec_check_ss(rc, "\0", "\0");
    dirdesc = AllocateDir(dirpath);
    if (dirdesc == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open tablespace directory \"%s\": %m", dirpath)));
    }
    while ((direntry = ReadDir(dirdesc, dirpath)) != NULL) {
        CHECK_FOR_INTERRUPTS();

        if (strcmp(direntry->d_name, ".") == 0 || strcmp(direntry->d_name, "..") == 0) {
            continue;
        }
#ifdef PGXC
        /* Postgres-XC tablespaces include node name in path */
        rc = snprintf_s(pathname,
            MAXPGPATH,
            MAXPGPATH - 1,
            "pg_tblspc/%s/%s_%s/%u",
            direntry->d_name,
            TABLESPACE_VERSION_DIRECTORY,
            g_instance.attr.attr_common.PGXCNodeName,
            db_oid);
        securec_check_ss(rc, "\0", "\0");

#else
        rc = snprintf_s(pathname,
            MAXPGPATH,
            MAXPGPATH - 1,
            "pg_tblspc/%s/%s/%u",
            direntry->d_name,
            TABLESPACE_VERSION_DIRECTORY,
            db_oid);
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
    CStoreScanDesc cstore_scan_desc = NULL;
    TupleDesc tupdesc = onerel->rd_att;
    int att_num = tupdesc->natts;
    Form_pg_attribute* attrs = tupdesc->attrs;
    CUDesc cu_desc;
    CU* cu_ptr = NULL;
    double total_source_size = 0;
    double total_cu_size = 0;
    BlockNumber targetblock = FirstCUID + 1;

    AttrNumber* col_idx = (AttrNumber*)palloc0(sizeof(AttrNumber) * att_num);
    int* slot_id_list = (int*)palloc0(sizeof(int) * att_num);

    double numeric_expand_ratio = 0;
    double numeric_src_data_size = 0;
    double numeric_data_size = 0;

    for (int i = 0; i < att_num; i++) {
        col_idx[i] = attrs[i]->attnum;
        slot_id_list[i] = CACHE_BLOCK_INVALID_IDX;
    }

    cstore_scan_desc = CStoreBeginScan(onerel, att_num, col_idx, NULL, false);
    CStore* cstore = cstore_scan_desc->m_CStore;
    uint32 max_cu_id = cstore->GetMaxCUID(onerel->rd_rel->relcudescrelid, onerel->rd_att, SnapshotNow);

    /* do not fetch the last CuID. */
    if (max_cu_id > FirstCUID + 1) {
        targetblock = FirstCUID + 1 + (gs_random() % (max_cu_id - FirstCUID - 1));
    } else {
        targetblock = FirstCUID + 1;
    }

    /* sample the first CU of each column, and calculate the compression ratio of this table. */
    for (int col = 0; col < att_num; col++) {
        // skip dropped column
        if (attrs[col]->attisdropped) {
            continue;
        }

        bool found = cstore->GetCUDesc(col, targetblock, &cu_desc, SnapshotNow);
        if (found && cu_desc.cu_size != 0) {
            cu_ptr = cstore->GetCUData(&cu_desc, col, attrs[col]->attlen, slot_id_list[col]);
            if ((cu_ptr->m_infoMode & CU_IntLikeCompressed) && ATT_IS_NUMERIC_TYPE(cu_ptr->m_atttypid)) {
                numeric_expand_ratio = 1.5; /* default expand ratio */
                numeric_data_size = 0;
                numeric_src_data_size = 0;
                for (uint32 row_id = 0; row_id < (uint32)cu_desc.row_count; row_id++) {
                    if (cu_ptr->IsNull(row_id)) {
                        continue;
                    }
                    char* srcData = cu_ptr->m_srcData + cu_ptr->m_offset[row_id];
                    Numeric nx = DatumGetNumeric(CStringGetDatum(srcData));
                    if (NUMERIC_IS_BI(nx)) {
                        Numeric srcNx = makeNumericNormal(nx);
                        numeric_src_data_size += VARSIZE_ANY(srcNx);
                        numeric_data_size += cu_ptr->m_offset[row_id + 1] - cu_ptr->m_offset[row_id];
                        pfree_ext(srcNx);
                    }
                }
                if (numeric_src_data_size != 0 && numeric_data_size != 0) {
                    numeric_expand_ratio = numeric_data_size / numeric_src_data_size;
                }

                total_source_size += (double)(cu_ptr->m_srcBufSize / numeric_expand_ratio);
                total_cu_size += (double)cu_ptr->m_cuSize;
            } else {
                total_source_size += (double)cu_ptr->m_srcDataSize;
                total_cu_size += (double)(cu_ptr->m_cuSize - cu_ptr->GetCUHeaderSize());
            }

            if (IsValidCacheSlotID(slot_id_list[col])) {
                CUCache->UnPinDataBlock(slot_id_list[col]);
            }
        }
    }

    pfree_ext(col_idx);
    pfree_ext(slot_id_list);
    CStoreEndScan(cstore_scan_desc);

    return total_cu_size > 0 ? total_source_size / total_cu_size : 0;
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
    Relation part_rel = NULL;

    /* fetch the compressed ratio of each partition, and get the average value. */
    partitions = relationGetPartitionList(onerel, AccessShareLock);
    foreach (cell, partitions) {
        partition = (Partition)lfirst(cell);
        part_rel = partitionGetRelation(onerel, partition);
        sample_ratio = calculate_coltable_compress_ratio(part_rel);
        if (sample_ratio != 0) {
            compress_ratio += sample_ratio;
            partition_num++;
        }
        releaseDummyRelation(&part_rel);
    }

    releasePartitionList(onerel, &partitions, AccessShareLock);

    return partition_num > 0 ? compress_ratio / partition_num : 0;
}

Datum pg_database_size_oid(PG_FUNCTION_ARGS)
{
    Oid db_oid = PG_GETARG_OID(0);
    int64 size;

#ifdef PGXC
    if ((IS_PGXC_COORDINATOR && !IsConnFromCoord())) {
        PG_RETURN_DATUM(pgxc_database_size(db_oid));
    }
#endif

    /* Normally, check the validation of db_oid */
    char* dbname = get_database_name(db_oid);
    if (dbname == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE),
                    errmsg("database with OID %u does not exist", db_oid)));
    }
    size = calculate_database_size(db_oid);

    if (size == 0) {
        PG_RETURN_NULL();
    }
    PG_RETURN_INT64(size);
}

Datum pg_database_size_name(PG_FUNCTION_ARGS)
{
    Name dbName = PG_GETARG_NAME(0);
    Oid db_oid = get_database_oid(NameStr(*dbName), false);
    int64 size;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        PG_RETURN_DATUM(pgxc_database_size(db_oid));
    }
#endif

    size = calculate_database_size(db_oid);
    if (size == 0) {
        PG_RETURN_NULL();
    }
    PG_RETURN_INT64(size);
}

typedef struct SampleTuplesInfo {
    Oid relid;
    double tuples;
} SampleTuplesInfo;

static int compare_relation_tuples(const void* a, const void* b, void* arg)
{
    const SampleTuplesInfo* aa = (const SampleTuplesInfo*)a;
    const SampleTuplesInfo* bb = (const SampleTuplesInfo*)b;
    if (aa->tuples > bb->tuples) {
        return -1;
    } else if (aa->tuples < bb->tuples) {
        return 1;
    } else {
        return 0;
    }
}

bool rel_is_CU_format(HeapTuple tuple, TupleDesc tupdesc)
{
    bool ret = false;
    bytea* options = NULL;

    options = extractRelOptions(tuple, tupdesc, InvalidOid);
    if (options != NULL) {
        const char* format = ((options) && (((StdRdOptions*)(options))->orientation))
            ? ((char*)(options) + *(int*)&(((StdRdOptions*)(options))->orientation))
            : ORIENTATION_ROW;
        if (pg_strcasecmp(format, ORIENTATION_COLUMN) == 0) {
            ret = true;
        }
        pfree_ext(options);
    }

    return ret;
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
    /* we think the compress ratio less than 0.5 is abnormal */
    const double abnormal_compress_ratio = 0.5;
    double compress_ratio = 0;
    double compress_ratio_weight = 0;

    int64 size = 0;
    int64 dbsize = 0;
    uint idx = 0;

    /* fetch the most 128 relations to calculate compressed ratio. */
    uint sample_relation_num = 128;
    const int tmp_num = 1024;

    SampleTuplesInfo* tuple_info = (SampleTuplesInfo*)palloc0(tmp_num * sizeof(SampleTuplesInfo));

    /* scan the pg_class to fetch the reltuples of every relation. */
    class_rel = heap_open(RelationRelationId, AccessShareLock);
    pg_class_tuple_desc = class_rel->rd_att;
    scan = systable_beginscan(class_rel, InvalidOid, false, SnapshotNow, 0, NULL);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        tuple_class = (Form_pg_class)GETSTRUCT(tuple);
        Oid relid = HeapTupleHeaderGetOid(tuple->t_data);

        /* skip the table with zero retuples. */
        if (tuple_class->reltuples == 0) {
            continue;
        }

        /* process the column tables. */
        if (rel_is_CU_format(tuple, pg_class_tuple_desc)) {
            col_tuples += tuple_class->reltuples;
            if (idx < tmp_num) {
                tuple_info[idx].relid = relid;
                tuple_info[idx].tuples = tuple_class->reltuples;
                idx++;
            } else {
                /* sort the tables with reltuples. */
                qsort_arg(tuple_info, tmp_num, sizeof(SampleTuplesInfo), compare_relation_tuples, NULL);
                idx = sample_relation_num;
            }
        } else if (rel_is_pax_format(tuple, pg_class_tuple_desc) && tuple_class->relkind == RELKIND_RELATION) {
            continue;
        } else if (tuple_class->relkind == RELKIND_FOREIGN_TABLE) {
            /* skip the foreign tables. */
            continue;
        } else {
            /* other tables are processed as row tables. */
            row_tuples += tuple_class->reltuples;
        }
    }
    systable_endscan(scan);
    heap_close(class_rel, AccessShareLock);

    if (idx != sample_relation_num) {
        qsort_arg(tuple_info, idx, sizeof(SampleTuplesInfo), compare_relation_tuples, NULL);
    }
    sample_relation_num = idx > sample_relation_num ? sample_relation_num : idx;

    /* calculate the compress ratio of each sample column table. */
    for (uint i = 0; i < sample_relation_num; i++) {
        sample_rel = try_relation_open(tuple_info[i].relid, AccessShareLock);
        if (sample_rel == NULL) {
            continue;
        }

        /* non-partition column tables */
        if (!RelationIsPartitioned(sample_rel)) {
            sample_ratio = calculate_coltable_compress_ratio(sample_rel);
            if (sample_ratio < abnormal_compress_ratio) {
                relation_close(sample_rel, AccessShareLock);
                continue;
            }
            /* calculate the weight average ratio. */
            sample_relation_size = calculate_CStore_relation_size(sample_rel, 0);
            compress_ratio_weight += sample_ratio * sample_relation_size;
            sample_relations_total_size += sample_relation_size;
        } else {
            /* partition column tables */
            sample_ratio = calculate_parttable_compress_ratio(sample_rel);
            if (sample_ratio < abnormal_compress_ratio) {
                relation_close(sample_rel, AccessShareLock);
                continue;
            }
            sample_relation_size = calculate_CStore_partitioned_relation_size(sample_rel, 0);
            compress_ratio_weight += sample_ratio * sample_relation_size;
            sample_relations_total_size += sample_relation_size;
        }

        relation_close(sample_rel, AccessShareLock);
    }

    compress_ratio = (compress_ratio_weight / sample_relations_total_size) > 1
        ? (compress_ratio_weight / sample_relations_total_size)
        : 1;

    /* calculate the size of current database. */
    dbsize = calculate_database_size(u_sess->proc_cxt.MyDatabaseId);

    if ((col_tuples + row_tuples) == 0) {
        size = 0;
        ereport(WARNING,
            (errmsg("The reltuples in pg_class is equal to zero."),
                errhint("Do analyze for the current database first.")));
    } else {
        size = dbsize + dbsize * (col_tuples / (col_tuples + row_tuples)) * (compress_ratio - 1);
    }

    pfree_ext(tuple_info);
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
    /* the size of database on all DNs */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        PG_RETURN_DATUM(pgxc_source_database_size());
    }
#endif

    size = calculate_source_database_size();

    if (size == 0)
        PG_RETURN_NULL();
    /* the size of database on one DN */
    PG_RETURN_INT64(size);
}

/*
 * Calculate total size of tablespace. Returns -1 if the tablespace directory
 * cannot be found.
 */
static int64 calculate_tablespace_size(Oid tbl_spc_oid)
{
    char tbl_spc_path[MAXPGPATH] = {'\0'};
    char pathname[MAXPGPATH] = {'\0'};
    int64 totalsize = 0;
    DIR* dirdesc = NULL;
    struct dirent* direntry = NULL;
    AclResult acl_result;
    errno_t rc = EOK;

    /*
     * User must have CREATE privilege for target tablespace, either
     * explicitly granted or implicitly because it is default for current
     * database.
     */
    if (tbl_spc_oid != u_sess->proc_cxt.MyDatabaseTableSpace) {
        acl_result = pg_tablespace_aclcheck(tbl_spc_oid, GetUserId(), ACL_CREATE);
        if (acl_result != ACLCHECK_OK)
            aclcheck_error(acl_result, ACL_KIND_TABLESPACE, get_tablespace_name(tbl_spc_oid));
    }

    if (IS_PGXC_DATANODE && IsConnFromApp() && IsSpecifiedTblspc(tbl_spc_oid, FILESYSTEM_HDFS)) {
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_IN_USE),
                errmsg("It is unsupported to calculate size of DFS tablespace \"%s\" on data node.",
                    get_tablespace_name(tbl_spc_oid)),
                errdetail("Please calculate size of DFS tablespace \"%s\" on coordinator node.",
                    get_tablespace_name(tbl_spc_oid))));
    }
    if (tbl_spc_oid == DEFAULTTABLESPACE_OID) {
        rc = snprintf_s(tbl_spc_path, MAXPGPATH, MAXPGPATH - 1, "base");
    } else if (tbl_spc_oid == GLOBALTABLESPACE_OID) {
        rc = snprintf_s(tbl_spc_path, MAXPGPATH, MAXPGPATH - 1, "global");
    } else {
#ifdef PGXC
        /* Postgres-XC tablespaces include node name in path */
        rc = snprintf_s(tbl_spc_path,
            MAXPGPATH,
            MAXPGPATH - 1,
            "pg_tblspc/%u/%s_%s",
            tbl_spc_oid,
            TABLESPACE_VERSION_DIRECTORY,
            g_instance.attr.attr_common.PGXCNodeName);
#else
        rc = snprintf_s(
            tbl_spc_path, MAXPGPATH, MAXPGPATH - 1, "pg_tblspc/%u/%s", tbl_spc_oid, TABLESPACE_VERSION_DIRECTORY);
#endif
    }
    securec_check_ss(rc, "\0", "\0");
    dirdesc = AllocateDir(tbl_spc_path);

    if (dirdesc == NULL) {
        return -1;
    }
    while ((direntry = ReadDir(dirdesc, tbl_spc_path)) != NULL) {
        struct stat fst;
        CHECK_FOR_INTERRUPTS();

        if (unlikely(strcmp(direntry->d_name, ".") == 0 || strcmp(direntry->d_name, "..") == 0)) {
            continue;
        }
        rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s/%s", tbl_spc_path, direntry->d_name);
        securec_check_ss(rc, "\0", "\0");

        if (unlikely(stat(pathname, &fst) < 0)) {
            if (errno == ENOENT) {
                continue;
            } else {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", pathname)));
            }
        }

        if (S_ISDIR(fst.st_mode)) {
            totalsize += db_dir_size(pathname);
        }
        totalsize += fst.st_size;
    }

    FreeDir(dirdesc);

    return totalsize;
}

uint64 pg_cal_tablespace_size_oid(Oid tbl_spc_oid)
{
    return (uint64)(calculate_tablespace_size(tbl_spc_oid));
}

Datum pg_tablespace_size_oid(PG_FUNCTION_ARGS)
{
    Oid tbl_spc_oid = PG_GETARG_OID(0);
    int64 size;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        PG_RETURN_DATUM(pgxc_tablespace_size(tbl_spc_oid));
    }
#endif

    size = calculate_tablespace_size(tbl_spc_oid);

    if (size < 0) {
        PG_RETURN_NULL();
    }
    PG_RETURN_INT64(size);
}

Datum pg_tablespace_size_name(PG_FUNCTION_ARGS)
{
    Name tbl_spc_name = PG_GETARG_NAME(0);
    Oid tbl_spc_oid = get_tablespace_oid(NameStr(*tbl_spc_name), false);
    int64 size;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        PG_RETURN_DATUM(pgxc_tablespace_size(tbl_spc_oid));
    }
#endif

    size = calculate_tablespace_size(tbl_spc_oid);

    if (size < 0) {
        PG_RETURN_NULL();
    }
    PG_RETURN_INT64(size);
}

/*
 * calculate size of (one fork of) a relation
 *
 * Note: we can safely apply this to temp tables of other sessions, so there
 * is no check here or at the call sites for that.
 */
int64 calculate_relation_size(RelFileNode* rfn, BackendId backend, ForkNumber fork_num)
{
    int64 totalsize = 0;
    char* relationpath = NULL;
    char pathname[MAXPGPATH] = {'\0'};
    unsigned int segcount = 0;
    errno_t rc = EOK;

    relationpath = relpathbackend(*rfn, backend, fork_num);

    for (segcount = 0;; segcount++) {
        struct stat fst;
        CHECK_FOR_INTERRUPTS();

        if (segcount == 0) {
            rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s", relationpath);
        } else {
            rc = snprintf_s(pathname, MAXPGPATH, MAXPGPATH - 1, "%s.%u", relationpath, segcount);
        }
        securec_check_ss(rc, "\0", "\0");
        if (stat(pathname, &fst) < 0) {
            if (errno == ENOENT) {
                break;
            } else {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", pathname)));
            }
        }
        totalsize += fst.st_size;
    }

    pfree_ext(relationpath);

    return totalsize;
}

uint64 CalculateMotRelationSize(Relation rel, Oid idxOid)
{
    uint64  totalSize = 0;
    FdwRoutine *fdwroutine = GetFdwRoutineForRelation(rel, false);
    if (fdwroutine != NULL) {
        totalSize = fdwroutine->GetForeignRelationMemSize(RelationGetRelid(rel), idxOid);
    }

    return totalSize;
}

int64 calculate_CStore_relation_size(Relation rel, ForkNumber fork_num)
{
    int64 totalsize = 0;
    int64 size = 0;
    uint64 segcount = 0;
    char pathname[MAXPGPATH] = {'\0'};

    if (fork_num == MAIN_FORKNUM) {
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
                        if (errno == ENOENT) {
                            break;
                        } else {
                            ereport(
                                ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", pathname)));
                        }
                    }
                    totalsize += fst.st_size;
                }
                custore.Destroy();
            }
        }
    }

    // Add delta table's size
    Relation delta_rel = try_relation_open(rel->rd_rel->reldeltarelid, AccessShareLock);
    if (delta_rel != NULL) {
        totalsize += calculate_relation_size(&(delta_rel->rd_node), delta_rel->rd_backend, fork_num);

        // Add CUDesc Toast table's size only when forkno is MAIN forkno.
        if (fork_num == MAIN_FORKNUM && OidIsValid(delta_rel->rd_rel->reltoastrelid)) {
            totalsize += calculate_toast_table_size(delta_rel->rd_rel->reltoastrelid);
        }

        relation_close(delta_rel, AccessShareLock);
    }

    // Add CUDesc table's size
    Relation cudesc_rel = try_relation_open(rel->rd_rel->relcudescrelid, AccessShareLock);
    if (cudesc_rel != NULL) {
        totalsize += calculate_relation_size(&(cudesc_rel->rd_node), cudesc_rel->rd_backend, fork_num);

        // Add CUDesc Toast table's size only when forkno is MAIN forkno.
        if (fork_num == MAIN_FORKNUM && OidIsValid(cudesc_rel->rd_rel->reltoastrelid)) {
            totalsize += calculate_toast_table_size(cudesc_rel->rd_rel->reltoastrelid);
        }

        // Add CUDesc Index's size
        Relation cudescIdx = try_relation_open(cudesc_rel->rd_rel->relcudescidx, AccessShareLock);
        if (cudescIdx != NULL) {
            totalsize += calculate_relation_size(&(cudescIdx->rd_node), cudescIdx->rd_backend, fork_num);
            // ignore toast relation size because CUDesc Index
            // doesn't have any toast sub-relation.
            relation_close(cudescIdx, AccessShareLock);
        }

        relation_close(cudesc_rel, AccessShareLock);
    }

    return totalsize;
}

int64 calculate_CStore_partitioned_relation_size(Relation rel, ForkNumber fork_num)
{
    int64 size = 0;

    List* partitions = NIL;
    ListCell* cell = NULL;
    Partition partition = NULL;
    Relation part_rel = NULL;

    partitions = relationGetPartitionList(rel, AccessShareLock);

    foreach (cell, partitions) {
        partition = (Partition)lfirst(cell);
        part_rel = partitionGetRelation(rel, partition);
        size += calculate_CStore_relation_size(part_rel, fork_num);
        releaseDummyRelation(&part_rel);
    }

    releasePartitionList(rel, &partitions, AccessShareLock);

    return size;
}

/*
 * Calculate total on-disk size of a TOAST relation, including its index.
 * Must not be applied to non-TOAST relations.
 */
static int64 calculate_toast_table_size(Oid toast_rel_oid)
{
    int64 size = 0;
    Relation toast_rel;
    Relation toast_idx_rel;
    ForkNumber fork_num;

    toast_rel = relation_open(toast_rel_oid, AccessShareLock);

    /* toast heap size, including FSM and VM size */
    for (int ifork = 0; ifork <= MAX_FORKNUM; ifork++) {
        fork_num = (ForkNumber)ifork;
        size += calculate_relation_size(&(toast_rel->rd_node), toast_rel->rd_backend, fork_num);
    }

    /* toast index size, including FSM and VM size */
    toast_idx_rel = relation_open(toast_rel->rd_rel->reltoastidxid, AccessShareLock);
    for (int ifork = 0; ifork <= MAX_FORKNUM; ifork++) {
        fork_num = (ForkNumber)ifork;
        size += calculate_relation_size(&(toast_idx_rel->rd_node), toast_idx_rel->rd_backend, fork_num);
    }

    relation_close(toast_idx_rel, AccessShareLock);
    relation_close(toast_rel, AccessShareLock);

    return size;
}

static int64 calculate_table_file_size(Relation rel, bool isCStore, int fork_num_option)
{
    int64 size = 0;
    ForkNumber fork_num = (ForkNumber)0;

    if (RELATION_CREATE_BUCKET(rel)) {
        Relation heap_bucket_rel = NULL;
        oidvector* bucketlist = searchHashBucketByOid(rel->rd_bucketoid);

        for (int i = 0; i < bucketlist->dim1; i++) {
            heap_bucket_rel = bucketGetRelation(rel, NULL, bucketlist->values[i]);
            if (fork_num_option == DEFAULT_FORKNUM) {
                /*
                 * heap size, including FSM and VM
                 */
                for (int ifork = 0; ifork <= MAX_FORKNUM; ifork++) {
                    fork_num = (ForkNumber)ifork;

                    size += calculate_relation_size(&(heap_bucket_rel->rd_node), InvalidBackendId, fork_num);
                }
                /*
                 * Size of toast relation
                 */
                if (OidIsValid(heap_bucket_rel->rd_rel->reltoastrelid)) {
                    size += calculate_toast_table_size(heap_bucket_rel->rd_rel->reltoastrelid);
                }
            } else {
                size += calculate_relation_size(&(heap_bucket_rel->rd_node), InvalidBackendId, fork_num_option);
            }
            bucketCloseRelation(heap_bucket_rel);
        }
    } else {
        if (fork_num_option == DEFAULT_FORKNUM) {
            /*
             * heap size, including FSM and VM
             */
            for (int ifork = 0; ifork <= MAX_FORKNUM; ifork++) {
                fork_num = (ForkNumber)ifork;

                if (isCStore)
                    size += calculate_CStore_relation_size(rel, fork_num);
                else
                    size += calculate_relation_size(&(rel->rd_node), rel->rd_backend, fork_num);
            }
            /*
             * Size of toast relation
             */
            if (OidIsValid(rel->rd_rel->reltoastrelid)) {
                size += calculate_toast_table_size(rel->rd_rel->reltoastrelid);
            }
        } else {
            if (isCStore) {
                size += calculate_CStore_relation_size(rel, fork_num_option);
            } else {
                size += calculate_relation_size(&(rel->rd_node), rel->rd_backend, fork_num_option);
            }
        }
    }
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
static int64 calculate_table_size(Relation rel, int fork_num_option)
{
    int64 size = 0;

    if (!RelationIsIndex(rel)) {
        if (RelationIsForeignTable(rel) && RelationIsMOTTableByOid(RelationGetRelid(rel))) {
            size = CalculateMotRelationSize(rel, InvalidOid);
        } else if (!RelationIsPartitioned(rel)) {
            size = calculate_table_file_size(rel, RelationIsColStore(rel), fork_num_option);
        } else {
            List* partitions = NIL;
            ListCell* cell = NULL;
            Partition partition = NULL;
            Relation part_rel = NULL;
            partitions = relationGetPartitionList(rel, AccessShareLock);

            foreach (cell, partitions) {
                partition = (Partition)lfirst(cell);
                part_rel = partitionGetRelation(rel, partition);

                size += calculate_table_file_size(part_rel, RelationIsColStore(rel), fork_num_option);

                releaseDummyRelation(&part_rel);
            }

            releasePartitionList(rel, &partitions, AccessShareLock);
        }
    } else {
        Relation base_rel = relation_open(rel->rd_index->indrelid, AccessShareLock);
        bool b_cstore = RelationIsColStore(base_rel) && (rel->rd_rel->relam == PSORT_AM_OID);

        if (RelationIsForeignTable(base_rel) && RelationIsMOTTableByOid(RelationGetRelid(base_rel))) {
            size = CalculateMotRelationSize(base_rel, RelationGetRelid(rel));
        } else if (!RelationIsPartitioned(rel)) {
            if (!b_cstore)
                size = calculate_table_file_size(rel, false, fork_num_option);
            else {
                Relation cstoreIdxRel = relation_open(rel->rd_rel->relcudescrelid, AccessShareLock);
                if (cstoreIdxRel != NULL) {
                    size = calculate_table_file_size(cstoreIdxRel, true, fork_num_option);
                    relation_close(cstoreIdxRel, AccessShareLock);
                }
            }
        } else {
            List* part_oid_list = NIL;
            ListCell* cell = NULL;
            Oid part_oid = InvalidOid;
            Oid part_index_oid = InvalidOid;
            Partition part_index = NULL;
            Relation part_index_rel = NULL;
            Relation cstore_part_index_rel = NULL;

            part_oid_list = relationGetPartitionOidList(base_rel);

            foreach (cell, part_oid_list) {
                part_oid = lfirst_oid(cell);
                part_index_oid = getPartitionIndexOid(RelationGetRelid(rel), part_oid);
                part_index = partitionOpen(rel, part_index_oid, AccessShareLock);
                part_index_rel = partitionGetRelation(rel, part_index);

                if (b_cstore) {
                    cstore_part_index_rel = relation_open(part_index_rel->rd_rel->relcudescrelid, AccessShareLock);
                    size += calculate_table_file_size(cstore_part_index_rel, true, fork_num_option);
                    relation_close(cstore_part_index_rel, AccessShareLock);
                } else {
                    size += calculate_table_file_size(part_index_rel, false, fork_num_option);
                }
                partitionClose(rel, part_index, AccessShareLock);
                releaseDummyRelation(&part_index_rel);
            }
            releasePartitionOidList(&part_oid_list);
        }
        relation_close(base_rel, AccessShareLock);
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
            Oid idx_oid = lfirst_oid(cell);
            Relation idx_rel = NULL;

            idx_rel = relation_open(idx_oid, AccessShareLock);
            size += calculate_table_size(idx_rel, DEFAULT_FORKNUM);
            relation_close(idx_rel, AccessShareLock);
        }
        list_free_ext(index_oids);
    }

    return size;
}

Datum pg_relation_size(PG_FUNCTION_ARGS)
{
    Oid rel_oid = PG_GETARG_OID(0);
    text* fork_name = PG_GETARG_TEXT_P(1);
    Relation rel;
    int64 size = 0;

#ifdef PGXC
    if (COLLECT_FROM_DATANODES(rel_oid)) {
        size = pgxc_exec_sizefunc(rel_oid, "pg_relation_size", text_to_cstring(fork_name));
        PG_RETURN_INT64(size);
    }
#endif /* PGXC */

    rel = try_relation_open(rel_oid, AccessShareLock);

    /*
     * Before 9.2, we used to throw an error if the relation didn't exist, but
     * that makes queries like "SELECT pg_relation_size(oid) FROM pg_class"
     * less robust, because while we scan pg_class with an MVCC snapshot,
     * someone else might drop the table. It's better to return NULL for
     * already-dropped tables than throw an error and abort the whole query.
     */
    if (rel == NULL) {
        PG_RETURN_NULL();
    }
    ForkNumber forknumber = forkname_to_number(text_to_cstring(fork_name));
    size = calculate_table_size(rel, forknumber);
    relation_close(rel, AccessShareLock);

    PG_RETURN_INT64(size);
}

Datum pg_table_size(PG_FUNCTION_ARGS)
{
    Oid rel_oid = PG_GETARG_OID(0);
    Relation rel = NULL;
    bool isnull = false;
    int64 size;

    MemoryContext oldcxt = CurrentMemoryContext;
    ErrorData* edata = NULL;

    PG_TRY();
    {
        if (COLLECT_FROM_DATANODES(rel_oid)) {
            size = pgxc_exec_sizefunc(rel_oid, "pg_table_size", NULL);
        } else {
            rel = try_relation_open(rel_oid, AccessShareLock);
            if (likely(RelationIsValid(rel))) {
                size = calculate_table_size(rel, DEFAULT_FORKNUM);
            } else {
                isnull = true;
            }
        }
    }
    PG_CATCH();
    {
        isnull = true;

        MemoryContextSwitchTo(oldcxt);
        edata = CopyErrorData();
        FlushErrorState();
    }
    PG_END_TRY();

    if (edata != NULL) {
        ereport(LOG, (errmsg("Caught error and ignored it in pg_table_size: %s", edata->message)));
        FreeErrorData(edata);
        edata = NULL;
    }

    if (likely(RelationIsValid(rel))) {
        relation_close(rel, AccessShareLock);
        rel = NULL;
    }

    if (isnull) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_INT64(size);
    }
}

Datum pg_partition_size_oid(PG_FUNCTION_ARGS)
{
    Oid part_table_oid = PG_GETARG_OID(0);
    Oid part_oid = PG_GETARG_OID(1);
    int64 size = 0;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && (GetRelationLocInfo((part_table_oid)) != NULL)) {
        PG_RETURN_INT64(pgxc_exec_partition_sizefunc(part_table_oid, part_oid, "pg_partition_size"));
    }
#endif

    size = calculate_partition_size(part_table_oid, part_oid);

    PG_RETURN_INT64(size);
}

Datum pg_partition_size_name(PG_FUNCTION_ARGS)
{
    char* part_table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* part_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    Oid part_table_oid = InvalidOid;
    Oid part_oid = InvalidOid;
    List* names = NIL;
    int64 size = 0;

    names = stringToQualifiedNameList(part_table_name);
    part_table_oid = RangeVarGetRelid(makeRangeVarFromNameList(names), NoLock, false);

    part_oid = partitionNameGetPartitionOid(
        part_table_oid, part_name, PART_OBJ_TYPE_TABLE_PARTITION, NoLock, false, false, NULL, NULL, NoLock);

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && (GetRelationLocInfo((part_table_oid)) != NULL)) {
        PG_RETURN_INT64(pgxc_exec_partition_sizefunc(part_table_oid, part_oid, "pg_partition_size"));
    }
#endif

    size = calculate_partition_size(part_table_oid, part_oid);

    PG_RETURN_INT64(size);
}

static int64 calculate_partition_size(Oid part_table_oid, Oid part_oid)
{
    Relation part_table_rel = NULL;
    Partition partition = NULL;
    Relation part_rel = NULL;
    int64 size = 0;

    part_table_rel = try_relation_open(part_table_oid, AccessShareLock);

    if (part_table_rel == NULL) {
        return 0;
    }
    partition = partitionOpen(part_table_rel, part_oid, AccessShareLock);
    part_rel = partitionGetRelation(part_table_rel, partition);
    size = calculate_table_file_size(part_rel, RelationIsColStore(part_rel), DEFAULT_FORKNUM);

    partitionClose(part_table_rel, partition, AccessShareLock);
    releaseDummyRelation(&part_rel);
    relation_close(part_table_rel, AccessShareLock);

    return size;
}

Datum pg_partition_indexes_size_oid(PG_FUNCTION_ARGS)
{
    Oid part_table_oid = PG_GETARG_OID(0);
    Oid part_oid = PG_GETARG_OID(1);
    int64 size = 0;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && (GetRelationLocInfo((part_table_oid)) != NULL)) {
        PG_RETURN_INT64(pgxc_exec_partition_sizefunc(part_table_oid, part_oid, "pg_partition_indexes_size"));
    }
#endif

    size = calculate_partition_indexes_size(part_table_oid, part_oid);

    PG_RETURN_INT64(size);
}

Datum pg_partition_indexes_size_name(PG_FUNCTION_ARGS)
{
    char* part_table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* part_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    Oid part_table_oid = InvalidOid;
    Oid part_oid = InvalidOid;
    List* names = NIL;
    int64 size = 0;

    names = stringToQualifiedNameList(part_table_name);
    part_table_oid = RangeVarGetRelid(makeRangeVarFromNameList(names), NoLock, false);

    part_oid = partitionNameGetPartitionOid(
        part_table_oid, part_name, PART_OBJ_TYPE_TABLE_PARTITION, NoLock, false, false, NULL, NULL, NoLock);

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && (GetRelationLocInfo((part_table_oid)) != NULL)) {
        PG_RETURN_INT64(pgxc_exec_partition_sizefunc(part_table_oid, part_oid, "pg_partition_indexes_size"));
    }
#endif

    size = calculate_partition_indexes_size(part_table_oid, part_oid);

    PG_RETURN_INT64(size);
}

static int64 calculate_partition_indexes_size(Oid part_table_oid, Oid part_oid)
{
    Relation part_table_rel = NULL;
    List* indexOids = NIL;
    ListCell* cell = NULL;
    int64 size = 0;

    part_table_rel = try_relation_open(part_table_oid, AccessShareLock);

    if (part_table_rel == NULL) {
        return 0;
    }

    indexOids = RelationGetIndexList(part_table_rel);

    foreach (cell, indexOids) {
        Oid indexOid = lfirst_oid(cell);
        Relation index_rel = NULL;
        Oid part_index_oid = InvalidOid;
        Partition part_index = NULL;
        Relation part_index_rel = NULL;

        index_rel = relation_open(indexOid, AccessShareLock);
        part_index_oid = getPartitionIndexOid(indexOid, part_oid);
        part_index = partitionOpen(index_rel, part_index_oid, AccessShareLock);
        part_index_rel = partitionGetRelation(index_rel, part_index);
        size += calculate_table_size(part_index_rel, DEFAULT_FORKNUM);

        partitionClose(index_rel, part_index, AccessShareLock);
        releaseDummyRelation(&part_index_rel);
        relation_close(index_rel, AccessShareLock);
    }

    list_free_ext(indexOids);
    relation_close(part_table_rel, AccessShareLock);

    return size;
}

Datum pg_indexes_size(PG_FUNCTION_ARGS)
{
    Oid rel_oid = PG_GETARG_OID(0);
    Relation rel;
    int64 size;

#ifdef PGXC
    if (COLLECT_FROM_DATANODES(rel_oid)) {
        PG_RETURN_INT64(pgxc_exec_sizefunc(rel_oid, "pg_indexes_size", NULL));
    }
#endif /* PGXC */

    rel = try_relation_open(rel_oid, AccessShareLock);
    if (rel == NULL) {
        PG_RETURN_NULL();
    }
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
    Oid rel_oid = PG_GETARG_OID(0);
    Relation rel;
    int64 size;

#ifdef PGXC
    if (COLLECT_FROM_DATANODES(rel_oid)) {
        PG_RETURN_INT64(pgxc_exec_sizefunc(rel_oid, "pg_total_relation_size", NULL));
    }
#endif /* PGXC */

    rel = try_relation_open(rel_oid, AccessShareLock);
    if (rel == NULL) {
        PG_RETURN_NULL();
    }
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
    if (size < limit) {
        rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, INT64_FORMAT " bytes", size);
    } else {
        size >>= 9; /* keep one extra bit for rounding */
        if (size < limit2) {
            rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, INT64_FORMAT " kB", (size + 1) / 2);
        } else {
            size >>= 10;
            if (size < limit2) {
                rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, INT64_FORMAT " MB", (size + 1) / 2);
            } else {
                size >>= 10;
                if (size < limit2) {
                    rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, INT64_FORMAT " GB", (size + 1) / 2);
                } else {
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

static Numeric numeric_plus_one_over_two(const Numeric n)
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

    divisor_int64 = Int64GetDatum((int64)(1 << count));
    divisor_numeric = DirectFunctionCall1(int8_numeric, divisor_int64);
    result = DirectFunctionCall2(numeric_div_trunc, d, divisor_numeric);
    return DatumGetNumeric(result);
}

Datum pg_size_pretty_numeric(PG_FUNCTION_ARGS)
{
    Numeric size = PG_GETARG_NUMERIC(0);
    Numeric limit, limit2;
    char *buf = NULL;
    char *result = NULL;
    errno_t rc = EOK;
    uint32 result_size;

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
    if (!HeapTupleIsValid(tuple)) {
        PG_RETURN_NULL();
    }
    relform = (Form_pg_class)GETSTRUCT(tuple);

    switch (relform->relkind) {
        case RELKIND_RELATION:
        case RELKIND_MATVIEW:
        case RELKIND_INDEX:
        case RELKIND_GLOBAL_INDEX:
        case RELKIND_SEQUENCE:
        case RELKIND_TOASTVALUE:
            /* okay, these have storage */
            if (relform->relfilenode) {
                result = relform->relfilenode;
            } else { 
                /* Consult the relation mapper */
                result = RelationMapOidToFilenode(relid, relform->relisshared);
            }
            break;

        default:
            /* no storage, return NULL */
            result = InvalidOid;
            break;
    }

    ReleaseSysCache(tuple);

    if (!OidIsValid(result)) {
        PG_RETURN_NULL();
    }
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

    heaprel = RelidByRelfilenode(reltablespace, relfilenode);

    if (!OidIsValid(heaprel)) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_OID(heaprel);
    }
}

/*
 * Get the filenode of a partition
 * This is expected to be used in queries like
 *     SELECT pg_partition_filenode(oid);
 */
Datum pg_partition_filenode(PG_FUNCTION_ARGS)
{
    Oid part_rel_id = PG_GETARG_OID(0);
    Oid result;
    HeapTuple tuple;
    Form_pg_partition part_rel_form;

    tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(part_rel_id));
    if (!HeapTupleIsValid(tuple)) {
        PG_RETURN_NULL();
    }
    part_rel_form = (Form_pg_partition)GETSTRUCT(tuple);
    switch (part_rel_form->parttype) {
        case PART_OBJ_TYPE_PARTED_TABLE:
        case PART_OBJ_TYPE_TABLE_PARTITION:
        case PART_OBJ_TYPE_INDEX_PARTITION:
        case PART_OBJ_TYPE_TOAST_TABLE:
            // okay, these have storage
            if (part_rel_form->relfilenode) {
                result = part_rel_form->relfilenode;
            } else {
                result = RelationMapOidToFilenode(part_rel_id, false);
            }
            break;

        default:
            // no storage, return NULL
            result = InvalidOid;
            break;
    }

    ReleaseSysCache(tuple);

    if (!OidIsValid(result)) {
        PG_RETURN_NULL();
    }
    PG_RETURN_OID(result);
}

/*
 * Get the pathname (relative to $PGDATA) of a partition
 *
 */
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
        case RELKIND_TOASTVALUE:
            /* okay, these have storage */

            /* This logic should match RelationInitPhysicalAddr */
            rnode.spcNode = ConvertToRelfilenodeTblspcOid(relform->reltablespace);
            if (rnode.spcNode == GLOBALTABLESPACE_OID) {
                rnode.dbNode = InvalidOid;
            } else {
                rnode.dbNode = u_sess->proc_cxt.MyDatabaseId;
            }
            if (relform->relfilenode) {
                rnode.relNode = relform->relfilenode;
            } else {
                /* Consult the relation mapper */
                rnode.relNode = RelationMapOidToFilenode(relid, relform->relisshared);
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
static Datum pgxc_tablespace_size(Oid ts_oid)
{
    StringInfoData buf;
    char* tsname = get_tablespace_name(ts_oid);
    ParallelFunctionState* state = NULL;
    int64 size = 0;

    if (NULL == tsname) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("tablespace with OID %u does not exist", ts_oid)));
    }
    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.pg_tablespace_size('%s')", tsname);

    state = RemoteFunctionResultHandler(buf.data, NULL, StrategyFuncSum);
    size = state->result;
    FreeParallelFunctionState(state);

    /*
     * If the type of specific tablespace is HDFS , calculate size of database on HDFS.
     */
    size += calculate_tablespace_size_on_dfs(ts_oid);

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
    /* the default compressed ratio of hdfs tabls is 3 now. */
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
 * Given a db_oid, return sum of pg_database_size() executed on all the Datanodes
 */
static Datum pgxc_database_size(Oid db_oid)
{
    StringInfoData buf;
    char* dbname = get_database_name(db_oid);
    ParallelFunctionState* state = NULL;
    int64 size = 0;

    if (dbname == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE),
                    errmsg("database with OID %u does not exist", db_oid)));
    }
    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_catalog.pg_database_size('%s')", dbname);

    state = RemoteFunctionResultHandler(buf.data, NULL, StrategyFuncSum);
    size = state->result;

    FreeParallelFunctionState(state);

    /*
     * If the specified database exists on HDFS, calculate size of database on HDFS.
     */
    size += calculate_dbsize_on_dfs(db_oid);

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
    if ((ret = SPI_connect()) < 0) {
        /* internal error */
        ereport(ERROR, (errcode(ERRCODE_SPI_CONNECTION_FAILURE), errmsg("SPI connect failure - returned %d", ret)));
    }
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
        if (numnodes == 1) {
            break;
        }
        size = DatumGetInt64(datum);
        total_size += size;
    }

    SPI_finish();

    if (numnodes == 1) {
        PG_RETURN_DATUM(datum);
    } else {
        PG_RETURN_INT64(total_size);
    }
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
Oid pg_toast_get_baseid(Oid rel_oid, bool* is_part_toast)
{
    Oid base_table_oid;
    const char* table_oid_str = NULL;
    Datum toast_len = Int32GetDatum(10);
    Relation rel;

    rel = relation_open(rel_oid, AccessShareLock);
    if (strstr(RelationGetRelationName(rel), "part") != NULL) {
        *is_part_toast = true;
        toast_len = Int32GetDatum(15);
    }

    table_oid_str = TextDatumGetCString(DirectFunctionCall2(
        text_substr_no_len_orclcompat, CStringGetTextDatum(RelationGetRelationName(rel)), toast_len));
    base_table_oid = DatumGetInt32(DirectFunctionCall1(int4in, CStringGetDatum(table_oid_str)));

    relation_close(rel, AccessShareLock);
    return base_table_oid;
}

bool IsToastRelationbyOid(Oid rel_oid)
{
    Relation rel;
    bool result = false;
    rel = relation_open(rel_oid, AccessShareLock);
    result = IsToastNamespace(RelationGetNamespace(rel));
    relation_close(rel, AccessShareLock);
    return result;
}

static bool IsIndexRelationbyOid(Oid rel_oid)
{
    Relation rel;
    bool result = false;
    rel = relation_open(rel_oid, AccessShareLock);
    result = RelationIsIndex(rel);
    relation_close(rel, AccessShareLock);
    return result;
}

/*
 * pgxc_exec_sizefunc
 * Execute the given object size system function on all the Datanodes associated
 * with rel_oid, and return the sum of all.
 *
 * Args:
 *
 * rel_oid: Oid of the table for which the object size function is to be executed.
 *
 * funcname: Name of the system function.
 *
 * extra_arg: The first argument to such sys functions is always table name.
 * Some functions can have a second argument. To pass this argument, extra_arg
 * is used. Currently only pg_relation_size() is the only one that requires
 * a 2nd argument: fork text.
 */
static int64 pgxc_exec_sizefunc(Oid rel_oid, char* funcname, char* extra_arg)
{
    int numnodes;
    Oid* nodelist = NULL;
    Oid tbl_oid;
    char* relname = NULL;
    StringInfoData buf;
    Relation rel = NULL;
    bool is_part_toast = false;
    Oid part_table_oid = InvalidOid;
    Relation part_table_rel = NULL;
    Partition part = NULL;
    ExecNodes* exec_nodes = makeNode(ExecNodes);
    int i = 0;
    ParallelFunctionState* state = NULL;
    int64 size = 0;

    if (IsToastRelationbyOid(rel_oid)) {
        tbl_oid = pg_toast_get_baseid(rel_oid, &is_part_toast);
    } else {
        tbl_oid = rel_oid;
    }
    if (!is_part_toast) {
        rel = relation_open(tbl_oid, AccessShareLock);
    } else {
        part_table_oid = partid_get_parentid(tbl_oid);
        part_table_rel = relation_open(part_table_oid, AccessShareLock);
        part = partitionOpen(part_table_rel, tbl_oid, AccessShareLock);
    }

    initStringInfo(&buf);

    if (!IsToastRelationbyOid(rel_oid)) {
        /* get relation name including any needed schema prefix and quoting */
        relname =
            quote_qualified_identifier(get_namespace_name(rel->rd_rel->relnamespace), RelationGetRelationName(rel));
        relname = repairObjectName(relname);
        if (extra_arg == NULL) {
            appendStringInfo(&buf, "SELECT pg_catalog.%s('%s')", funcname, relname);
        } else {
            appendStringInfo(&buf, "SELECT pg_catalog.%s('%s', '%s')", funcname, relname, extra_arg);
        }
    } else {
        // toast table is belog to a ordinary table
        if (!is_part_toast) {
            relname = RelationGetRelationName(rel);
            relname = repairObjectName(relname);
            if (extra_arg == NULL) {
                appendStringInfo(&buf,
                    "SELECT pg_catalog.%s(a.oid)  from  pg_class a,pg_class b where a.oid = b.reltoastrelid and "
                    "b.relname='%s'",
                    funcname,
                    relname);
            } else {
                appendStringInfo(&buf,
                    "SELECT pg_catalog.%s(a.oid, '%s') from  pg_class a,pg_class b where a.oid = b.reltoastrelid and "
                    "b.relname='%s'",
                    funcname,
                    extra_arg,
                    relname);
            }
        } else {
            // toast table is belog to a partitoin
            char* part_table_name = RelationGetRelationName(part_table_rel);
            char* part_name = PartitionGetPartitionName(part);

            part_table_name = repairObjectName(part_table_name);
            part_name = repairObjectName(part_name);

            if (extra_arg == NULL) {
                appendStringInfo(&buf,
                    "SELECT pg_catalog.%s(toast_table.oid) from pg_class toast_table, pg_class part_table, "
                    "pg_partition part where toast_table.oid = part.reltoastrelid and part_table.oid=part.parentid and "
                    "part_table.relname='%s' and part.relname='%s'",
                    funcname,
                    part_table_name,
                    part_name);
            } else {
                appendStringInfo(&buf,
                    "SELECT pg_catalog.%s(toast_table.oid, '%s') from pg_class toast_table, pg_class part_table, "
                    "pg_partition part where toast_table.oid = part.reltoastrelid and part_table.oid=part.parentid and "
                    "part_table.relname='%s' and part.relname='%s'",
                    funcname,
                    extra_arg,
                    part_table_name,
                    part_name);
            }
        }
    }

    if (!is_part_toast) {
        Oid tab_rel_oid = InvalidOid;

        if (RelationIsIndex(rel)) {
            tab_rel_oid = rel->rd_index->indrelid;
        } else {
            tab_rel_oid = RelationGetRelid(rel);
        }

        numnodes = get_pgxc_classnodes(tab_rel_oid, &nodelist);
        relation_close(rel, AccessShareLock);
    } else {
        numnodes = get_pgxc_classnodes(RelationGetRelid(part_table_rel), &nodelist);
        partitionClose(part_table_rel, part, AccessShareLock);
        relation_close(part_table_rel, AccessShareLock);
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
    if (RelationIsPaxFormatByOid(rel_oid)) {
        Relation rel = relation_open(rel_oid, AccessShareLock);
        StringInfo dfs_store_path = getDfsStorePath(rel);
        Oid tbl_spc_oid = rel->rd_rel->reltablespace;
        size += calculate_dir_size_on_dfs(tbl_spc_oid, dfs_store_path);
        relation_close(rel, NoLock);
    }
    return size;
}

static int64 pgxc_exec_partition_sizefunc(Oid part_table_oid, Oid part_oid, char* func_name)
{
    int numnodes = 0;
    Oid* nodelist = NULL;
    char* part_table_name = NULL;
    char* part_name = NULL;
    Relation part_table_rel = NULL;
    Partition partition = NULL;
    StringInfoData buf;
    ExecNodes* exec_nodes = makeNode(ExecNodes);
    int i = 0;
    ParallelFunctionState* state = NULL;
    int64 size = 0;

    part_table_rel = relation_open(part_table_oid, AccessShareLock);
    partition = partitionOpen(part_table_rel, part_oid, AccessShareLock);

    initStringInfo(&buf);

    if (part_table_rel->rd_locator_info) {
        // get relation name including any needed schema prefix and quoting
        part_table_name = quote_qualified_identifier(
            get_namespace_name(part_table_rel->rd_rel->relnamespace), RelationGetRelationName(part_table_rel));
        part_table_name = repairObjectName(part_table_name);
    }
    part_name = partition->pd_part->relname.data;
    part_name = repairObjectName(part_name);

    appendStringInfo(&buf, "SELECT pg_catalog.%s('%s', '%s')", func_name, part_table_name, part_name);

    numnodes = get_pgxc_classnodes(RelationGetRelid(part_table_rel), &nodelist);

    partitionClose(part_table_rel, partition, AccessShareLock);
    relation_close(part_table_rel, AccessShareLock);

    // Build nodes to execute on
    //
    for (i = 0; i < numnodes; i++) {
        exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, PGXCNodeGetNodeId(nodelist[i], PGXC_NODE_DATANODE));
    }
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
    RangeVar* relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
    Relation rel = relation_openrv(relrv, AccessShareLock);

    if (rel == NULL) {
        PG_RETURN_NULL();
    }
    bool compressed = false;
    if (RelationIsColStore(rel)) {
        compressed = (0 != pg_strcasecmp(COMPRESSION_NO, RelationGetCompression(rel)));
    } else {
        compressed = RowRelationIsCompressed(rel);
    }
    relation_close(rel, AccessShareLock);
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
    if (!superuser()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to call gs_get_max_dbsize_name.")));
    }
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        StringInfoData buf;
        ParallelFunctionState* state = NULL;

        group_oid = get_pgxc_groupoid(group_name);

        if (!OidIsValid(group_oid)) {
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("PGXC Group %s: group not defined", group_name)));
        }
        nmembers = get_pgxc_groupmembers(group_oid, &members);
        if (nmembers == 0) {
            PG_RETURN_INT64(0);
        }
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
            if (!tuplestore_gettupleslot(state->tupstore, true, false, slot)) {
                break;
            }
            size = DatumGetInt64(slot_getattr(slot, 1, &isnull));
            if (!isnull && max_size < size) {
                max_size = size;
            }
            (void)ExecClearTuple(slot);
        }

        ExecDropSingleTupleTableSlot(slot);

        FreeParallelFunctionState(state);
    }

    PG_RETURN_INT64(max_size);
}

#endif /* PGXC */
