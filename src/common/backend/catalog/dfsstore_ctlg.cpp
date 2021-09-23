/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * dfsstore_ctlg.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/catalog/dfsstore_ctlg.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/cstore_ctlg.h"
#include "catalog/dependency.h"
#include "catalog/dfsstore_ctlg.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "catalog/storage.h"
#include "commands/tablespace.h"
#include "commands/vacuum.h"
#include "commands/dbcommands.h"
#include "dfsdesc.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "pgxc/pgxc.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "access/reloptions.h"
#include "catalog/toasting.h"
#include "storage/dfs/dfs_connector.h"
#include "dfs_adaptor.h"

extern Oid bupgrade_get_next_cudesc_toast_index_oid();
extern Oid bupgrade_get_next_cudesc_toast_pg_type_oid();
extern Oid bupgrade_get_next_cudesc_toast_pg_class_oid();
extern Oid bupgrade_get_next_cudesc_index_oid();
extern Oid bupgrade_get_next_cudesc_array_pg_type_oid();
extern Oid bupgrade_get_next_cudesc_pg_type_oid();
extern Oid bupgrade_get_next_cudesc_pg_class_oid();

extern Oid bupgrade_get_next_cudesc_pg_class_rfoid();
extern Oid bupgrade_get_next_cudesc_index_rfoid();
extern Oid bupgrade_get_next_cudesc_toast_pg_class_rfoid();
extern Oid bupgrade_get_next_cudesc_toast_index_rfoid();

/*
 * Brief        : Create DfsDesc table for Dfs table. the DfsDesc table is invisible table
 *                for user. It manages the Dfs table data that is stored in dfs system.
 * Input        : rel, the table relation.
 *                relOptions, the table options.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
void createDfsDescTable(Relation rel, Datum relOptions)
{
    Oid relOid = RelationGetRelid(rel);
    Oid namespaceid = CSTORE_NAMESPACE;
    /*
     * In order to void delta data and desc date of DFS table are stored in one folder.
     * The delta data and desc data will be put in default tablespace on DFS table.
     */
    Oid tablespaceid = InvalidOid;
    Oid duDescRelId = InvalidOid;
    char duDescRelName[NAMEDATALEN];
    char indexRelName[NAMEDATALEN];
    Oid collationObjectId[DfsDescIndexMaxAttrNum];
    Oid classObjectId[DfsDescIndexMaxAttrNum];
    int16 colOptions[DfsDescIndexMaxAttrNum];
    int indexOid = InvalidOid;
    bool sharedRelation = rel->rd_rel->relisshared;
    int catalogRelId = 0;
    enum SysCacheIdentifier catalogIndex;
    ObjectAddress baseobject;
    ObjectAddress cudescobject;
    errno_t errorno = EOK;

    /*
     * bulid the dfsdesc table name.
     */
    errorno = snprintf_s(duDescRelName, sizeof(duDescRelName), sizeof(duDescRelName) - 1, "pg_dfsdesc_%u", relOid);
    securec_check_ss(errorno, "\0", "\0");
    errorno = snprintf_s(indexRelName, sizeof(indexRelName), sizeof(indexRelName) - 1, "pg_dfsdesc_%u_index", relOid);
    securec_check_ss(errorno, "\0", "\0");

    /*
     * Define DfsDesc table attributes. If we add one column, we should change const variable.
     */
    TupleDesc tupdesc = CreateTemplateTupleDesc(DfsDescMaxAttrNum, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)Anum_pg_dfsdesc_duid, "duid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)Anum_pg_dfsdesc_partid, "partid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)Anum_pg_dfsdesc_rowcount, "rowcount", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)Anum_pg_dfsdesc_magic, "magic", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)Anum_pg_dfsdesc_min, "min", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)Anum_pg_dfsdesc_max, "max", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)Anum_pg_dfsdesc_deletemap, "deletemap", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)Anum_pg_dfsdesc_relativelyfilename, "relativefilename", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)Anum_pg_dfsdesc_extra, "extra", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)Anum_pg_dfsdesc_columnmap, "colmap", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)Anum_pg_dfsdesc_filesize, "filesize", INT8OID, -1, 0);

    /*
     * The DfsDesc table and DFS table have the same relkind.
     */
    char relKind = rel->rd_rel->relkind;

    relOptions = AddInternalOption(relOptions, INTERNAL_MASK_ENABLE);
    if (u_sess->proc_cxt.IsBinaryUpgrade) {
        u_sess->upg_cxt.binary_upgrade_next_pg_type_oid = bupgrade_get_next_cudesc_pg_type_oid();
        u_sess->upg_cxt.binary_upgrade_next_array_pg_type_oid = bupgrade_get_next_cudesc_array_pg_type_oid();
        u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_oid = bupgrade_get_next_cudesc_pg_class_oid();
        u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_rfoid = bupgrade_get_next_cudesc_pg_class_rfoid();
    }

    duDescRelId = heap_create_with_catalog(duDescRelName,
        namespaceid,
        tablespaceid,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        rel->rd_rel->relowner,
        tupdesc,
        NIL,
        relKind,
        (rel->rd_rel->relpersistence == 't' && u_sess->attr.attr_common.max_query_retry_times == 0)
            ? 'u'
            : rel->rd_rel->relpersistence,
        sharedRelation,
        false,
        true,
        0,
        ONCOMMIT_NOOP,
        relOptions,
        false,
        true,
        NULL,
        REL_CMPRS_NOT_SUPPORT,
        NULL);

    Assert(duDescRelId != InvalidOid);

    /*
     * Make the DfsDesc relation visible, otherwise heap_open will fail.
     */
    CommandCounterIncrement();

    /*
     * ShareLock is not really needed here, but take it anyway.
     */
    Relation duDescRel = heap_open(duDescRelId, ShareLock);

    /*
     * Build DfsDesc index.
     */
    IndexInfo* indexInfo = makeNode(IndexInfo);
    indexInfo->ii_NumIndexAttrs = DfsDescIndexMaxAttrNum;
    indexInfo->ii_NumIndexKeyAttrs = DfsDescIndexMaxAttrNum;
    indexInfo->ii_KeyAttrNumbers[0] = Anum_pg_dfsdesc_duid;
    indexInfo->ii_Expressions = NIL;
    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_Predicate = NIL;
    indexInfo->ii_PredicateState = NIL;
    indexInfo->ii_ExclusionOps = NULL;
    indexInfo->ii_ExclusionProcs = NULL;
    indexInfo->ii_ExclusionStrats = NULL;
    indexInfo->ii_Unique = true;
    indexInfo->ii_ReadyForInserts = true;
    indexInfo->ii_Concurrent = false;
    indexInfo->ii_BrokenHotChain = false;
    indexInfo->ii_ParallelWorkers = 0;
    indexInfo->ii_PgClassAttrId = Anum_pg_class_relcudescidx;

    collationObjectId[0] = InvalidOid;
    classObjectId[0] = INT4_BTREE_OPS_OID;
    colOptions[0] = 0;

    IndexCreateExtraArgs extra;
    SetIndexCreateExtraArgs(&extra, InvalidOid, false, false);

    if (u_sess->proc_cxt.IsBinaryUpgrade) {
        u_sess->upg_cxt.binary_upgrade_next_index_pg_class_oid = bupgrade_get_next_cudesc_index_oid();
        u_sess->upg_cxt.binary_upgrade_next_index_pg_class_rfoid = bupgrade_get_next_cudesc_index_rfoid();
    }

    indexOid = index_create(duDescRel,
        indexRelName,
        InvalidOid,
        InvalidOid,
        indexInfo,
        list_make1((void*)"duid"),
        BTREE_AM_OID,
        tablespaceid,
        collationObjectId,
        classObjectId,
        colOptions,
        (Datum)0,
        true,
        false,
        false,
        false,
        true,
        false,
        false,
        &extra,
        false);
    Assert(OidIsValid(indexOid));

    heap_close(duDescRel, NoLock);

    catalogRelId = RelationRelationId;
    catalogIndex = RELOID;

    Relation classRel = heap_open(catalogRelId, RowExclusiveLock);

    HeapTuple relTup = SearchSysCacheCopy1(catalogIndex, ObjectIdGetDatum(relOid));
    if (!HeapTupleIsValid(relTup)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("Cache lookup failed for relation %u.", relOid)));
    }

    ((Form_pg_class)GETSTRUCT(relTup))->relcudescrelid = duDescRelId;
    ((Form_pg_class)GETSTRUCT(relTup))->relcudescidx = indexOid;

    simple_heap_update(classRel, &relTup->t_self, relTup);

    /*
     * Keep catalog indexes current.
     */
    CatalogUpdateIndexes(classRel, relTup);

    heap_freetuple(relTup);
    relTup = NULL;

    heap_close(classRel, RowExclusiveLock);

    /*
     * Register the DfsDesc table in pg_depend, so that the
     * DfsDesc table will be deleted when delete the DFS table.
     */
    if (!IsBootstrapProcessingMode()) {
        baseobject.classId = RelationRelationId;
        baseobject.objectId = relOid;
        baseobject.objectSubId = 0;
        cudescobject.classId = RelationRelationId;
        cudescobject.objectId = duDescRelId;
        cudescobject.objectSubId = 0;

        recordDependencyOn(&cudescobject, &baseobject, DEPENDENCY_INTERNAL);
    }

    /*
     * Make changes visible.
     */
    CommandCounterIncrement();

    if (u_sess->proc_cxt.IsBinaryUpgrade) {
        u_sess->upg_cxt.binary_upgrade_next_toast_pg_type_oid = bupgrade_get_next_cudesc_toast_pg_type_oid();
        u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_oid = bupgrade_get_next_cudesc_toast_pg_class_oid();
        u_sess->upg_cxt.binary_upgrade_next_index_pg_class_oid = bupgrade_get_next_cudesc_toast_index_oid();
        u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_rfoid = bupgrade_get_next_cudesc_toast_pg_class_rfoid();
        u_sess->upg_cxt.binary_upgrade_next_index_pg_class_rfoid = bupgrade_get_next_cudesc_toast_index_rfoid();
    }
    AlterTableCreateToastTable(duDescRelId, relOptions);
}

/**
 * @Description: Get the tablespace store path.
 * @in tblSpcOid, the tablespace oid.
 * @return return store path StringInfo.
 */
StringInfo getHDFSTblSpcStorePath(Oid tblSpcOid)
{
    StringInfo storePath = makeStringInfo();
    char* tblspcPath = NULL;
    Assert(OidIsValid(tblSpcOid));
    tblspcPath = GetTablespaceOptionValue(tblSpcOid, TABLESPACE_OPTION_STOREPATH);
    if (tblspcPath != NULL) {
        tblspcPath = pstrdup(tblspcPath);
        canonicalize_path(tblspcPath);
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("get tablespace %u store path failed", tblSpcOid)));
    }
    Assert(NULL != tblspcPath);

    /* trim trailing '/' */
    Assert(strlen(tblspcPath) != 0);
    char* end = tblspcPath + strlen(tblspcPath) - 1;
    while (end > tblspcPath && *end == '/') {
        *end = '\0';
        end--;
    }

    /*
     * The tablespace store path.
     * "tblespace_dir/tablespace_secondary".
     */
    appendStringInfo(storePath, "%s/%s", tblspcPath, DFS_TABLESPACE_SUBDIR);
    return storePath;
}

/*
 * Brief        : Get the external store path of DFS table without CU format.
 * Input        : rel, Relation structure.
 * Output       : None.
 * Return Value : Return store path.
 * Notes        : None.
 */
StringInfo getDfsStorePath(Relation rel)
{
    Oid tablespaceOid = rel->rd_rel->reltablespace;
    StringInfo storePath = getHDFSTblSpcStorePath(tablespaceOid);

    /*
     * Build Dfsstore path. The deirectory structure is
     * "tblespace_dir/tablespace_secondary/db_dir/table_dir/table_partition_dir".
     * The table dir is named "schema.tablename"
     */
    appendStringInfo(storePath,
        "/%s/%s",
        get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId),
        get_namespace_name(RelationGetNamespace(rel)));
    appendStringInfo(storePath, ".");
    appendStringInfo(storePath, "%s", RelationGetRelationName(rel));

    return storePath;
}

/*
 * Brief        : Get the DfsDesc table oid using main table oid.
 * Input        : mainTblOid, the main table oid.
 * Output       : None.
 * Return Value : None.
 * Notes        : 1. The dfsDescTblOid validity must be checked by caller.
 */
Oid getDfsDescTblOid(Oid mainTblOid)
{
    HeapTuple tuple = NULL;
    Datum datum;
    bool isnull = false;
    Oid dfsDescTblOid = InvalidOid;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(mainTblOid));

    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("Cache lookup failed for relation %u.", mainTblOid)));
    }

    /*
     * Extract desc table oid.
     */
    datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relcudescrelid, &isnull);
    if (!isnull) {
        dfsDescTblOid = DatumGetObjectId(datum);
    }

    ReleaseSysCache(tuple);

    return dfsDescTblOid;
}

/*
 * Brief        : Get the DfsDesc index table oid using main table oid.
 * Input        : mainTblOid, the main table oid.
 * Output       : None.
 * Return Value : None.
 * Notes        : 1. The dfsDescTblIndexOid validity must be checked by caller.
 */
Oid getDfsDescIndexOid(Oid mainTblOid)
{
    Relation descRel = NULL;
    Oid dfsDescTblIndexOid;
    Oid dfsDescTblOid;

    dfsDescTblOid = getDfsDescTblOid(mainTblOid);
    descRel = heap_open(dfsDescTblOid, AccessShareLock);

    dfsDescTblIndexOid = descRel->rd_rel->relcudescidx;
    heap_close(descRel, AccessShareLock);

    Assert(InvalidOid != dfsDescTblIndexOid);
    return dfsDescTblIndexOid;
}

/*
 * brief: Get relation size from the DFS data files.
 * input param @relation: relation information struct.
 * @return the relation size.
 * nodes: the returned size dose not include size of delta table.
 */
int64 getDFSRelSize(Relation rel)
{
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("Un-support feature: HDFS")));
    return 0;
}

List* getFiles(List* files, char* path, dfs::DFSConnector* conn)
{
    List* rs_paths = conn->listDirectory(path, false);

    ListCell* lc = NULL;
    foreach (lc, rs_paths) {
        char* itemPath = (char*)lfirst(lc);
        if (conn->isDfsFile(itemPath, false))
            files = lappend(files, itemPath);
        else
            files = getFiles(files, itemPath, conn);
    }

    list_free(rs_paths);
    rs_paths = NIL;
    return files;
}

/*
 * "PG_9.2_201605161_datanode1.1" is valid example,
 * in "datanode1", node_info is "_datanode1."
 */
bool isNodeFile(const char* path, StringInfo node_info)
{
    const char* filename = basename(path);

    /* serach  the node postfix (case sensitive) */
    const char* postfix = strstr(filename, node_info->data);
    if (NULL == postfix)
        return false;

    return true;
}

/*
 * Brief        : scan directory to get all data files
 * Input        : dfsTblPathList, the path to be dropped.
 * Output       : None
 * Return Value : list of data files.
 * Notes        : None
 */
List* getDataFiles(Oid tbloid)
{
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("Un-support feature: HDFS")));
    return NIL;
}

/* data redistribution for DFS table.
 * Brief: Prepare the old files and new files, one of which will be deleted.
 * In the process of data redistribution, when execute the following command
 * "insert into temp_table select * form redistribution_table", there are two
 * file lists in HDFS directory of redistribution table. The new files that
 * are produced by the above command will be existed in HDFS directory of
 * redistribution table. The old files represent data of redistribution table.
 * The two file lists must keep it until finish commit or abort transaction.
 * If commit, delete the old file(file of redistribution table),
 * otherwise delete new file. In this function, the two file lists keep in
 * pendingDfsDeletes struct in order to delete it when commit or abort.
 * @_in_param dataDestRelation: the table to be redistributed.
 * @return None.
 */
void InsertDeletedFilesForTransaction(Relation dataDestRelation)
{
    Assert(RelationIsValid(dataDestRelation));
    /*
     * Save path for relation, and this path will be used in doPendingDfsDelete().
     */
    StringInfo store_path = getDfsStorePath(dataDestRelation);
    MemoryContext oldcontext = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    DFSDescHandler handler(MAX_LOADED_DFSDESC, dataDestRelation->rd_att->natts, dataDestRelation);

    u_sess->catalog_cxt.vf_store_root = makeStringInfo();
    appendStringInfo(u_sess->catalog_cxt.vf_store_root, "%s", store_path->data);
    (void)MemoryContextSwitchTo(oldcontext);

    /*
     * Get connection object and will be used in doPendingDfsDelete().
     */
    DfsSrvOptions* options = GetDfsSrvOptions(dataDestRelation->rd_rel->reltablespace);
    dfs::DFSConnector* conn = dfs::createConnector(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), options, dataDestRelation->rd_rel->reltablespace);
    if (NULL == conn) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_INVALID_SERVER_TYPE),
                errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("Failed to connect hdfs.")));
    }
    u_sess->catalog_cxt.delete_conn = conn;

    /*
     * Get old files in dataDestRelation dir.
     * Insert old files into pendingDfsDelete.
     */
    List* descs = handler.GetAllDescs(NULL);
    ListCell* lc = NULL;
    foreach (lc, descs) {
        DFSDesc* desc = (DFSDesc*)lfirst(lc);
        StringInfo oldFile = makeStringInfo();
        appendStringInfo(oldFile, "%s", desc->GetFileName());
        /*
         * Must delete old files When commit.
         */
        InsertIntoPendingDfsDelete(
            oldFile->data, true, dataDestRelation->rd_rel->relowner, (uint64)desc->GetFileSize());
        pfree_ext(oldFile->data);
        pfree_ext(oldFile);
    }

    pfree_ext(store_path->data);
    pfree_ext(store_path);
}
