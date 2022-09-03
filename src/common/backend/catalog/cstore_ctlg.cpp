/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * cstore_ctlg.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/catalog/cstore_ctlg.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/cstore_am.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_type.h"
#include "catalog/cstore_ctlg.h"
#include "catalog/toasting.h"
#include "cstore.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/uuid.h"
#include "securec_check.h"
#include "access/tupdesc.h"

extern char* get_namespace_name(Oid nspid);

static bool createCUDescTableForPartitionedTable(Relation rel, Datum reloptions);
static bool createDeltaTableForPartitionedTable(Relation rel, Datum reloptions, CreateStmt* mainTblStmt);
static void CreateStorageForPartition(Relation rel);

extern Oid bupgrade_get_next_cudesc_pg_class_oid();
static bool bupgrade_is_next_cudesc_pg_class_oid_valid();
extern Oid bupgrade_get_next_cudesc_pg_type_oid();
static bool bupgrade_is_next_cudesc_pg_type_oid_valid();
extern Oid bupgrade_get_next_cudesc_array_pg_type_oid();
static bool bupgrade_is_next_cudesc_array_pg_type_oid_valid();
extern Oid bupgrade_get_next_cudesc_index_oid();
static bool bupgrade_is_next_cudesc_index_oid_valid();

extern Oid bupgrade_get_next_cudesc_toast_pg_class_oid();
static bool bupgrade_is_next_cudesc_toast_pg_class_oid_valid();
extern Oid bupgrade_get_next_cudesc_toast_pg_type_oid();
static bool bupgrade_is_next_cudesc_toast_pg_type_oid_valid();

extern Oid bupgrade_get_next_cudesc_toast_index_oid();
static bool bupgrade_is_next_cudesc_toast_index_oid_valid();

static Oid bupgrade_get_next_delta_pg_class_oid();
static bool bupgrade_is_next_delta_pg_class_oid_valid();
static Oid bupgrade_get_next_delta_pg_type_oid();
static bool bupgrade_is_next_delta_pg_type_oid_valid();
static Oid bupgrade_get_next_delta_array_pg_type_oid();
static bool bupgrade_is_next_delta_array_pg_type_oid_valid();

static Oid bupgrade_get_next_delta_toast_pg_class_oid();
static bool bupgrade_is_next_delta_toast_pg_class_oid_valid();
static Oid bupgrade_get_next_delta_toast_pg_type_oid();
static bool bupgrade_is_next_delta_toast_pg_type_oid_valid();
static Oid bupgrade_get_next_delta_toast_index_oid();
static bool bupgrade_is_next_delta_toast_index_oid_valid();

extern Oid bupgrade_get_next_cudesc_pg_class_rfoid();
extern Oid bupgrade_get_next_cudesc_index_rfoid();
extern Oid bupgrade_get_next_cudesc_toast_pg_class_rfoid();
extern Oid bupgrade_get_next_cudesc_toast_index_rfoid();

static Oid bupgrade_get_next_delta_pg_class_rfoid();
static Oid bupgrade_get_next_delta_toast_pg_class_rfoid();
static Oid bupgrade_get_next_delta_toast_index_rfoid();

Datum AddInternalOption(Datum reloptions, int mask)
{
    List* opts = NIL;
    DefElem* def = NULL;

    opts = untransformRelOptions(reloptions);
    def = makeDefElem(pstrdup("internal_mask"), (Node*)makeInteger(mask));
    opts = lappend(opts, def);
    return transformRelOptions((Datum)0, opts, NULL, NULL, false, false);
}

Datum AddOrientationOption(Datum relOptions, bool isColStore)
{
    DefElem* def = makeDefElem(
        pstrdup("orientation"), (Node*)makeString((char*)(isColStore ? ORIENTATION_COLUMN : ORIENTATION_ROW)));

    List* optsList = untransformRelOptions(relOptions);
    optsList = lappend(optsList, def);

    return transformRelOptions((Datum)0, optsList, NULL, NULL, false, false);
}

/*
 * AlterTableCreateDeltaTable
 * 		 If it is a ColStore table, that should invoking this function.
 * 		 then create a delta table.
 */
void AlterCStoreCreateTables(Oid relOid, Datum reloptions, CreateStmt* mainTblStmt)
{
    Relation rel;

    /*
     * Grab an exclusive lock on the target table, since we'll update its
     * pg_class tuple. This is redundant for all present uses, since caller
     * will have such a lock already.  But the lock is needed to ensure that
     * concurrent readers of the pg_class tuple won't have visibility issues,
     * so let's be safe.
     */
    rel = heap_open(relOid, AccessExclusiveLock);
    /*
     * Dfs table will use AlterDfsCreateTables instead.
     */
    if (!RelationIsCUFormat(rel)) {
        heap_close(rel, NoLock);
        return;
    }

    if (!RELATION_IS_PARTITIONED(rel)) {
        /* create_delta_table does all the work */
        (void)CreateDeltaTable(rel, reloptions, false, mainTblStmt);
        (void)CreateCUDescTable(rel, reloptions, false);
        CStore::CreateStorage(rel, InvalidOid);
    } else {
        createCUDescTableForPartitionedTable(rel, reloptions);
        createDeltaTableForPartitionedTable(rel, reloptions, mainTblStmt);
        CreateStorageForPartition(rel);
    }

    heap_close(rel, NoLock);
}

/**
 * @Description: bulid the delta name for the given rel.
 * @in rel, the given relation.
 * @in ispartition, whether or not the rel is partition table.
 * @return return the delta name.
 */
char* makeDeltaNameFormRel(Relation rel, bool isPartition)
{
    char* deltaName = (char*)palloc0(sizeof(char) * NAMEDATALEN);

    Oid relOid = RelationGetRelid(rel);
    errno_t rc = EOK;

    if (!isPartition) {
        rc = snprintf_s(deltaName, NAMEDATALEN, NAMEDATALEN - 1, "pg_delta_%u", relOid);
        securec_check_ss(rc, "", "");
    } else {
        rc = snprintf_s(deltaName, NAMEDATALEN, NAMEDATALEN - 1, "pg_delta_part_%u", relOid);
        securec_check_ss(rc, "", "");
    }

    return deltaName;
}

static inline void GetCatalogRelIdAndIndex(bool isPartition, int* catalogRelId, int* catalogIndex)
{
    if (!isPartition) {
        *catalogRelId = RelationRelationId;
        *catalogIndex = RELOID;
    } else {
        *catalogRelId = PartitionRelationId;
        *catalogIndex = PARTRELID;
    }
}
/*
 * It will auto-create auxiliary delta table for ColStore.
 * For single insert, it will be insert into this table based on rowstore.
 * FUTURE CASE: how to fix max column number issue?
 */
bool CreateDeltaTable(Relation rel, Datum reloptions, bool isPartition, CreateStmt* mainTblStmt)
{
    Oid relOid = RelationGetRelid(rel);
    Oid namespaceid = CSTORE_NAMESPACE, delta_relid;
    /*
     * In order to void delta data and desc date of DFS table are stored in one folder.
     * The delta data and desc data will be put in default tablespace on DFS table.
     */
    Oid tablespaceid = InvalidOid;
    char* deltaRelName = NULL;
    int catalogRelId = 0;
    bool shared_relation = rel->rd_rel->relisshared;
    ObjectAddress baseobject, deltaobject;
    int catalogIndex = 0;

    deltaRelName = makeDeltaNameFormRel(rel, isPartition);

    tablespaceid = rel->rd_rel->reltablespace;

    TupleDesc mainTableTupDesc = rel->rd_att;

    TupleDesc tupdesc = CreateTupleDescCopyConstr(mainTableTupDesc);

    // if psort index change relkind to relation
    char relkind = rel->rd_rel->relkind == RELKIND_INDEX ? RELKIND_RELATION : rel->rd_rel->relkind;

    reloptions = AddInternalOption(reloptions, INTERNAL_MASK_ENABLE);

    if (u_sess->proc_cxt.IsBinaryUpgrade) {
        u_sess->upg_cxt.binary_upgrade_next_pg_type_oid = bupgrade_get_next_delta_pg_type_oid();
        u_sess->upg_cxt.binary_upgrade_next_array_pg_type_oid = bupgrade_get_next_delta_array_pg_type_oid();
        u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_oid = bupgrade_get_next_delta_pg_class_oid();

        u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_rfoid = bupgrade_get_next_delta_pg_class_rfoid();
    }

    delta_relid = heap_create_with_catalog(deltaRelName,
        namespaceid,
        tablespaceid,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        rel->rd_rel->relowner,
        tupdesc,
        NIL,
        relkind,
        // @Temp Table. for cstore temp table, we set the delta table unlogged, just like unlogged cstore table.
        (rel->rd_rel->relpersistence == 't' && u_sess->attr.attr_common.max_query_retry_times == 0)
            ? 'u'
            : rel->rd_rel->relpersistence,
        shared_relation,
        false,
        true,
        0,
        ONCOMMIT_NOOP,
        reloptions,
        false,
        true,
        NULL,
        REL_CMPRS_NOT_SUPPORT,
        NULL,
        false);
    Assert(delta_relid != InvalidOid);

    /* make the delta relation visible, else heap_open will fail */
    CommandCounterIncrement();

    /*
     * Store the delta table's OID in the parent relation's pg_class row
     */
    GetCatalogRelIdAndIndex(isPartition, &catalogRelId, &catalogIndex);

    Relation class_rel = heap_open(catalogRelId, RowExclusiveLock);

    HeapTuple reltup = SearchSysCacheCopy1(catalogIndex, ObjectIdGetDatum(relOid));
    if (!HeapTupleIsValid(reltup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), (errmsg("cache lookup failed for relation %u", relOid))));

    if (!isPartition) {
        ((Form_pg_class)GETSTRUCT(reltup))->reldeltarelid = delta_relid;
    } else {
        ((Form_pg_partition)GETSTRUCT(reltup))->reldeltarelid = delta_relid;
    }

    if (!IsBootstrapProcessingMode()) {
        /* normal case, use a transactional update */
        simple_heap_update(class_rel, &reltup->t_self, reltup);

        /* Keep catalog indexes current */
        CatalogUpdateIndexes(class_rel, reltup);
    } else {
        /* While bootstrapping, we cannot UPDATE, so overwrite in-place */
        heap_inplace_update(class_rel, reltup);
    }

    heap_freetuple(reltup);

    heap_close(class_rel, RowExclusiveLock);

    if (!IsBootstrapProcessingMode() && !isPartition) {
        baseobject.classId = RelationRelationId;
        baseobject.objectId = relOid;
        baseobject.objectSubId = 0;
        deltaobject.classId = RelationRelationId;
        deltaobject.objectId = delta_relid;
        deltaobject.objectSubId = 0;

        recordDependencyOn(&deltaobject, &baseobject, DEPENDENCY_INTERNAL);
    }

    /*
     * Make changes visible
     */
    CommandCounterIncrement();

    if (u_sess->proc_cxt.IsBinaryUpgrade) {
        u_sess->upg_cxt.binary_upgrade_next_toast_pg_type_oid = bupgrade_get_next_delta_toast_pg_type_oid();
        u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_oid = bupgrade_get_next_delta_toast_pg_class_oid();
        u_sess->upg_cxt.binary_upgrade_next_index_pg_class_oid = bupgrade_get_next_delta_toast_index_oid();
        u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_rfoid = bupgrade_get_next_delta_toast_pg_class_rfoid();
        u_sess->upg_cxt.binary_upgrade_next_index_pg_class_rfoid = bupgrade_get_next_delta_toast_index_rfoid();
    }

    AlterTableCreateToastTable(delta_relid, reloptions);
    return true;
}

/*
 * It will auto-create auxiliary CUDesc table for ColStore.
 * Each CU will has one tuple(CUDesc tuple) in this auxiliary table.
 */
bool CreateCUDescTable(Relation rel, Datum reloptions, bool isPartition)
{
    Oid relOid = RelationGetRelid(rel);
    Oid namespaceid = CSTORE_NAMESPACE;
    Oid cudesc_relid;
    char cudesc_relname[NAMEDATALEN];
    char index_relname[NAMEDATALEN];
    bool shared_relation = rel->rd_rel->relisshared;
    Oid collationObjectId[2];
    Oid classObjectId[2];
    int16 coloptions[2];
    int catalogRelId = 0;
    int indexOid = InvalidOid;
    int catalogIndex = 0;
    ObjectAddress baseobject, cudescobject;
    errno_t ret = 0;

    /*
     * Create the toast table and its index
     */
    if (!isPartition) {
        ret = snprintf_s(cudesc_relname, sizeof(cudesc_relname), sizeof(cudesc_relname) - 1, "pg_cudesc_%u", relOid);
        securec_check_ss_c(ret, "\0", "\0");

        ret = snprintf_s(index_relname, sizeof(index_relname), sizeof(index_relname) - 1, "pg_cudesc_%u_index", relOid);
        securec_check_ss_c(ret, "\0", "\0");
    } else {
        ret =
            snprintf_s(cudesc_relname, sizeof(cudesc_relname), sizeof(cudesc_relname) - 1, "pg_cudesc_part_%u", relOid);
        securec_check_ss_c(ret, "\0", "\0");

        ret = snprintf_s(
            index_relname, sizeof(index_relname), sizeof(index_relname) - 1, "pg_cudesc_part_%u_index", relOid);
        securec_check_ss_c(ret, "\0", "\0");
    }

    // If we add one column, we should change const variable
    // CUDescMaxAttrNum in gscstore_am.cpp
    //
    TupleDesc tupdesc = CreateTemplateTupleDesc(CUDescMaxAttrNum, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)CUDescColIDAttr, "col_id", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)CUDescCUIDAttr, "cu_id", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)CUDescMinAttr, "min", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)CUDescMaxAttr, "max", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)CUDescRowCountAttr, "row_count", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)CUDescCUModeAttr, "cu_mode", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)CUDescSizeAttr, "size", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)CUDescCUPointerAttr, "cu_pointer", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)CUDescCUMagicAttr, "magic", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)CUDescCUExtraAttr, "extra", TEXTOID, -1, 0);

    // if psort index change relkind to relation
    char relkind = rel->rd_rel->relkind == RELKIND_INDEX ? RELKIND_RELATION : rel->rd_rel->relkind;

    reloptions = AddInternalOption(reloptions,
        INTERNAL_MASK_DALTER | INTERNAL_MASK_DDELETE | INTERNAL_MASK_DINSERT | INTERNAL_MASK_DUPDATE |
            INTERNAL_MASK_ENABLE);
    if (u_sess->proc_cxt.IsBinaryUpgrade) {
        u_sess->upg_cxt.binary_upgrade_next_pg_type_oid = bupgrade_get_next_cudesc_pg_type_oid();
        u_sess->upg_cxt.binary_upgrade_next_array_pg_type_oid = bupgrade_get_next_cudesc_array_pg_type_oid();
        u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_oid = bupgrade_get_next_cudesc_pg_class_oid();
        u_sess->upg_cxt.binary_upgrade_next_heap_pg_class_rfoid = bupgrade_get_next_cudesc_pg_class_rfoid();
    }

    cudesc_relid = heap_create_with_catalog(cudesc_relname,
        namespaceid,
        rel->rd_rel->reltablespace,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        rel->rd_rel->relowner,
        tupdesc,
        NIL,
        relkind,
        // @Temp Table. for cstore temp table, we set the delta table unlogged, just like unlogged cstore table.
        (rel->rd_rel->relpersistence == 't' && u_sess->attr.attr_common.max_query_retry_times == 0)
            ? 'u'
            : rel->rd_rel->relpersistence,
        shared_relation,
        false,
        true,
        0,
        ONCOMMIT_NOOP,
        reloptions,
        false,
        true,
        NULL,
        REL_CMPRS_NOT_SUPPORT,
        NULL,
        false);

    Assert(cudesc_relid != InvalidOid);

    /* make the CUDesc relation visible, else heap_open will fail */
    CommandCounterIncrement();

    /* ShareLock is not really needed here, but take it anyway */
    Relation cudesc_rel = heap_open(cudesc_relid, ShareLock);

    IndexInfo* indexInfo = makeNode(IndexInfo);
    indexInfo->ii_NumIndexAttrs = 2;
    indexInfo->ii_NumIndexKeyAttrs = 2;
    indexInfo->ii_KeyAttrNumbers[0] = 1;
    indexInfo->ii_KeyAttrNumbers[1] = 2;
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
    collationObjectId[1] = InvalidOid;

    classObjectId[0] = INT4_BTREE_OPS_OID;
    classObjectId[1] = INT4_BTREE_OPS_OID;

    coloptions[0] = 0;
    coloptions[1] = 0;

    IndexCreateExtraArgs extra;
    SetIndexCreateExtraArgs(&extra, InvalidOid, false, false);

    if (u_sess->proc_cxt.IsBinaryUpgrade) {
        u_sess->upg_cxt.binary_upgrade_next_index_pg_class_oid = bupgrade_get_next_cudesc_index_oid();
        u_sess->upg_cxt.binary_upgrade_next_index_pg_class_rfoid = bupgrade_get_next_cudesc_index_rfoid();
    }

    indexOid = index_create(cudesc_rel,
        index_relname,
        InvalidOid,
        InvalidOid,
        indexInfo,
        list_make2((void*)"col_id", (void*)"cu_id"),
        BTREE_AM_OID,
        rel->rd_rel->reltablespace,
        collationObjectId,
        classObjectId,
        coloptions,
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

    heap_close(cudesc_rel, NoLock);

    GetCatalogRelIdAndIndex(isPartition, &catalogRelId, &catalogIndex);

    Relation class_rel = heap_open(catalogRelId, RowExclusiveLock);

    HeapTuple reltup = SearchSysCacheCopy1(catalogIndex, ObjectIdGetDatum(relOid));
    if (!HeapTupleIsValid(reltup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), (errmsg("cache lookup failed for relation %u", relOid))));

    if (!isPartition) {
        ((Form_pg_class)GETSTRUCT(reltup))->relcudescrelid = cudesc_relid;
        ((Form_pg_class)GETSTRUCT(reltup))->relcudescidx = indexOid;
    } else {
        ((Form_pg_partition)GETSTRUCT(reltup))->relcudescrelid = cudesc_relid;
        ((Form_pg_partition)GETSTRUCT(reltup))->relcudescidx = indexOid;
    }

    simple_heap_update(class_rel, &reltup->t_self, reltup);

    /* Keep catalog indexes current */
    CatalogUpdateIndexes(class_rel, reltup);

    heap_freetuple(reltup);

    heap_close(class_rel, RowExclusiveLock);

    /*
     * Register dependency from the CUDesc table to the master, so that the
     * toast table will be deleted if the master is.  Skip this in bootstrap
     * mode.
     */
    if (!IsBootstrapProcessingMode() && !isPartition) {
        baseobject.classId = RelationRelationId;
        baseobject.objectId = relOid;
        baseobject.objectSubId = 0;
        cudescobject.classId = RelationRelationId;
        cudescobject.objectId = cudesc_relid;
        cudescobject.objectSubId = 0;

        recordDependencyOn(&cudescobject, &baseobject, DEPENDENCY_INTERNAL);
    }

    /*
     * Make changes visible
     */
    CommandCounterIncrement();

    if (u_sess->proc_cxt.IsBinaryUpgrade) {
        u_sess->upg_cxt.binary_upgrade_next_toast_pg_type_oid = bupgrade_get_next_cudesc_toast_pg_type_oid();
        u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_oid = bupgrade_get_next_cudesc_toast_pg_class_oid();
        u_sess->upg_cxt.binary_upgrade_next_index_pg_class_oid = bupgrade_get_next_cudesc_toast_index_oid();
        u_sess->upg_cxt.binary_upgrade_next_toast_pg_class_rfoid = bupgrade_get_next_cudesc_toast_pg_class_rfoid();
        u_sess->upg_cxt.binary_upgrade_next_index_pg_class_rfoid = bupgrade_get_next_cudesc_toast_index_rfoid();
    }

    AlterTableCreateToastTable(cudesc_relid, reloptions);

    return true;
}

static bool createCUDescTableForPartitionedTable(Relation rel, Datum reloptions)
{
    Oid partition = InvalidOid;
    List* partitionList = NIL;
    ListCell* cell = NULL;
    bool result = false;

    Assert(RELATION_IS_PARTITIONED(rel));

    partitionList = relationGetPartitionOidList(rel);

    foreach (cell, partitionList) {
        partition = DatumGetObjectId(lfirst(cell));
        result = createCUDescTableForPartition(rel->rd_id, partition, reloptions);
    }

    if (partitionList != NULL)
        releasePartitionOidList(&partitionList);

    return result;
}

bool createCUDescTableForPartition(Oid relOid, Oid partOid, Datum reloptions)
{
    Relation partRel = NULL;
    Relation rel = NULL;
    Partition partition = NULL;
    bool result = false;

    rel = relation_open(relOid, NoLock);
    partition = partitionOpen(rel, partOid, AccessExclusiveLock);
    /* already has cu_desc? */
    if (partition->pd_part->relcudescrelid != InvalidOid) {
        relation_close(rel, NoLock);
        partitionClose(rel, partition, NoLock);
        return false;
    }

    /*
     * create cu_desc table for the special table partition
     * fake a relation and then invoke CreateCUDescTable to
     * create the cu_desc table
     */
    partRel = partitionGetRelation(rel, partition);

    Assert(PointerIsValid(partRel));
    result = CreateCUDescTable(partRel, reloptions, true);

    releaseDummyRelation(&partRel);

    partitionClose(rel, partition, NoLock);
    relation_close(rel, NoLock);

    return result;
}

static bool createDeltaTableForPartitionedTable(Relation rel, Datum reloptions, CreateStmt* mainTblStmt)
{
    Oid partition = InvalidOid;
    List* partitionList = NIL;
    ListCell* cell = NULL;
    bool result = false;

    Assert(RELATION_IS_PARTITIONED(rel));

    partitionList = relationGetPartitionOidList(rel);

    foreach (cell, partitionList) {
        partition = DatumGetObjectId(lfirst(cell));
        result = createDeltaTableForPartition(rel->rd_id, partition, reloptions, mainTblStmt);
    }

    if (partitionList != NULL)
        releasePartitionOidList(&partitionList);

    return result;
}

bool createDeltaTableForPartition(Oid relOid, Oid partOid, Datum reloptions, CreateStmt* mainTblStmt)
{
    Relation partRel = NULL;
    Relation rel = NULL;
    Partition partition = NULL;
    bool result = false;

    rel = relation_open(relOid, NoLock);
    partition = partitionOpen(rel, partOid, AccessExclusiveLock);
    if (partition->pd_part->reldeltarelid != InvalidOid) {
        relation_close(rel, NoLock);
        partitionClose(rel, partition, NoLock);
        return false;
    }

    partRel = partitionGetRelation(rel, partition);

    Assert(PointerIsValid(partRel));
    result = CreateDeltaTable(partRel, reloptions, true, mainTblStmt);

    releaseDummyRelation(&partRel);

    partitionClose(rel, partition, NoLock);
    relation_close(rel, NoLock);

    return result;
}

void CreateStorageForPartition(Relation rel)
{
    Oid partOid = InvalidOid;
    List* partitionList = NIL;
    ListCell* cell = NULL;
    Relation partRel = NULL;
    Partition part = NULL;

    Assert(RELATION_IS_PARTITIONED(rel));

    partitionList = relationGetPartitionOidList(rel);

    foreach (cell, partitionList) {
        partOid = DatumGetObjectId(lfirst(cell));
        part = partitionOpen(rel, partOid, AccessExclusiveLock);
        partRel = partitionGetRelation(rel, part);

        Assert(PointerIsValid(partRel));
        CStore::CreateStorage(partRel, InvalidOid);

        releaseDummyRelation(&partRel);
        partitionClose(rel, part, NoLock);
    }

    if (partitionList != NULL)
        releasePartitionOidList(&partitionList);
}

/*
 * check if the input tuple is column store
 *
 * @in tuple: the pg_class tuple to be check
 * @in tupdesc: the tuple desc of pg_class
 * @return true if it is column store
 */
static bool rel_is_column_format(HeapTuple tuple, TupleDesc tupdesc)
{
    bool ret = false;
    bytea* options = NULL;

    options = extractRelOptions(tuple, tupdesc, InvalidOid);
    if (options != NULL) {
        const char* format = ((options) && (((StdRdOptions*)(options))->orientation))
                                 ? ((char*)(options) + *(int*)&(((StdRdOptions*)(options))->orientation))
                                 : ORIENTATION_ROW;
        if (pg_strcasecmp(format, ORIENTATION_COLUMN) == 0)
            ret = true;

        pfree(options);
    }

    return ret;
}

/*
 * collect the result of function from remote nodes, set 0 if all success; set -1 if any one fail.
 *
 * @in state: the function state which includes all the info.
 * @return no return.
 */
static void StrategyFuncAnd(ParallelFunctionState* state)
{
    TupleTableSlot* slot = NULL;
    int64 result = 0;

    Assert(state);
    Assert(state->tupstore);
    Assert(state->tupdesc);
    slot = MakeSingleTupleTableSlot(state->tupdesc);

    for (;;) {
        bool isnull = false;

        if (!tuplestore_gettupleslot(state->tupstore, true, false, slot))
            break;

        if (DatumGetInt64(tableam_tslot_getattr(slot, 1, &isnull)) < 0) {
            result = -1;
            break;
        }
        (void)ExecClearTuple(slot);
    }

    state->result = result;
}

/*
 * check if the delta table tuple desc matches its main table tuple desc.
 * The main column store table is set either by relation or by tuple
 *
 * @in rel: the main column store relation.
 * @in tuple: the pg_class tuple of main column store relation.
 * @return true if match.
 */
static bool check_tupledesc_match(Relation rel, HeapTuple tuple)
{
    bool ret = true;
    Oid mainRelOid = 0;
    Relation mainRel = NULL;

    if (rel == NULL) {
        Assert(tuple != NULL);
        mainRelOid = HeapTupleGetOid(tuple);
        mainRel = relation_open(mainRelOid, AccessShareLock);
    } else {
        mainRel = rel;
        mainRelOid = RelationGetRelid(mainRel);
    }

    /* for partition column relation */
    if (RelationIsPartitioned(mainRel)) {
        /* It is a partitioned table, so find all the partitions in pg_partition */
        Relation pgpartition;
        TableScanDesc partScan;
        HeapTuple partTuple;
        ScanKeyData keys[2];

        /* Process all plain partitions listed in pg_partition */
        ScanKeyInit(&keys[0],
            Anum_pg_partition_parttype,
            BTEqualStrategyNumber,
            F_CHAREQ,
            CharGetDatum(PART_OBJ_TYPE_TABLE_PARTITION));

        ScanKeyInit(&keys[1], Anum_pg_partition_parentid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(mainRelOid));
        pgpartition = heap_open(PartitionRelationId, AccessShareLock);
        partScan = heap_beginscan(pgpartition, SnapshotNow, 2, keys);
        /* compare the first partition'delta with main table */
        if ((partTuple = heap_getnext(partScan, ForwardScanDirection)) != NULL) {
            Form_pg_partition partitionForm = (Form_pg_partition)GETSTRUCT(partTuple);
            Oid deltaRelOid = partitionForm->reldeltarelid;
            Relation deltaRel = relation_open(deltaRelOid, AccessShareLock);
            if (!equalDeltaTupleDescs(RelationGetDescr(mainRel), RelationGetDescr(deltaRel))) {
                ret = false;
            }
            relation_close(deltaRel, AccessShareLock);
        }

        heap_endscan(partScan);
        heap_close(pgpartition, AccessShareLock);
    } else {
        /* for non-partition table */
        Oid deltaRelOid = mainRel->rd_rel->reldeltarelid;
        Relation deltaRel = relation_open(deltaRelOid, AccessShareLock);
        if (!equalDeltaTupleDescs(RelationGetDescr(mainRel), RelationGetDescr(deltaRel))) {
            ret = false;
        }
        relation_close(deltaRel, AccessShareLock);
    }

    if (rel == NULL)
        relation_close(mainRel, AccessShareLock);

    return ret;
}

/*
 * send the sync function to remote nodes.
 *
 * @in schema: the name of the table schema.
 * @in rel: the name of the table.
 * @return 0 if all success, -1 if any fail.
 */
static int64 send_sync_to_remote(const char* schema, const char* rel)
{
    char* relname = NULL;
    int64 ret = 0;
    ParallelFunctionState* state = NULL;
    StringInfoData buf;
    initStringInfo(&buf);

    /* get relation name including any needed schema prefix and quoting */
    relname = quote_qualified_identifier(schema, rel);
    relname = repairObjectName(relname);

    /* construct the sql and send to dn */
    appendStringInfo(&buf, "SELECT pg_catalog.pg_sync_cstore_delta('%s')", relname);
    /* sync delta table both on datanode and coordinate */
    state = RemoteFunctionResultHandler(buf.data, NULL, StrategyFuncAnd, false, EXEC_ON_ALL_NODES);

    if (state != NULL) {
        ret = state->result;
        FreeParallelFunctionState(state);
    }
    return ret;
}

/*
 * Clean the old delta table
 *
 * @in relId: the rel id of delta table
 * @no return
 */
static void drop_delta_rel(Oid deltataRelId)
{
    /* clean dependency before drop the delta table */
    ScanKeyData key[2];
    SysScanDesc scan;
    HeapTuple tup;

    Relation depRel = relation_open(DependRelationId, RowExclusiveLock);
    ScanKeyInit(&key[0], Anum_pg_depend_classid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(RelationRelationId));
    ScanKeyInit(&key[1], Anum_pg_depend_objid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(deltataRelId));
    scan = systable_beginscan(depRel, DependDependerIndexId, true, NULL, 2, key);
    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        simple_heap_delete(depRel, &tup->t_self);
    }
    systable_endscan(scan);
    relation_close(depRel, RowExclusiveLock);

    CommandCounterIncrement();

    /* do the drop */
    ObjectAddresses* objects = NULL;
    ObjectAddress obj;
    objects = new_object_addresses();
    obj.classId = RelationRelationId;
    obj.objectId = deltataRelId;
    obj.objectSubId = 0;
    add_exact_object_address(&obj, objects);

    performMultipleDeletions(objects, DROP_CASCADE, 0);

    /* cleanup */
    free_object_addresses(objects);

    CommandCounterIncrement();
}

/*
 * do the sync work locally.
 *
 * @in mainRel: the column store main relation.
 * @in reloptions: the reloptions of the main relation.
 * @no return.
 */
static void sync_cstore_delta_Internal(Relation mainRel, Datum reloptions)
{
    if (RelationIsPartitioned(mainRel)) {
        /* for partition table, create new delta table and update pg_partition */
        Oid partOid = InvalidOid;
        Partition partition = NULL;
        Relation partRel = NULL;
        List* partitionList = NIL;
        ListCell* cell = NULL;

        /* get the partition list */
        partitionList = relationGetPartitionOidList(mainRel);

        /* handle each partition */
        foreach (cell, partitionList) {
            bool hasData = false;

            /* open the partition */
            partOid = DatumGetObjectId(lfirst(cell));
            partition = partitionOpen(mainRel, partOid, AccessExclusiveLock);

            /* check the delta table of the current partition that if it has data */
            Oid deltaRelid = partition->pd_part->reldeltarelid;
            Relation deltaRel = relation_open(deltaRelid, AccessShareLock);
            TableScanDesc deltaScan = heap_beginscan(deltaRel, SnapshotNow, 0, NULL);
            if (heap_getnext(deltaScan, ForwardScanDirection) != NULL) {
                hasData = true;
            }

            heap_endscan(deltaScan);
            relation_close(deltaRel, AccessShareLock);

            /* create a new delta table for the current partition */
            if (!hasData) {
                partRel = partitionGetRelation(mainRel, partition);
                Assert(PointerIsValid(partRel));

                /* drop the old delta table */
                drop_delta_rel(deltaRelid);

                /* create new delta table and associate it with main rel */
                (void)CreateDeltaTable(partRel, reloptions, true, NULL);
                releaseDummyRelation(&partRel);
            } else {
                partitionClose(mainRel, partition, NoLock);
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Sync failed because there is data inside the delta table %u.", deltaRelid)));
            }
            partitionClose(mainRel, partition, NoLock);
        }

        /* clean */
        if (partitionList != NIL)
            releasePartitionOidList(&partitionList);
    } else {
        /* for non-partition table, create new delta table and update pg_class */
        /* first check if delta table has data, skip it if so */
        bool hasData = false;
        Oid deltaRelid = mainRel->rd_rel->reldeltarelid;
        Relation deltaRel = relation_open(deltaRelid, AccessShareLock);
        TableScanDesc deltaScan = heap_beginscan(deltaRel, SnapshotNow, 0, NULL);
        if (heap_getnext(deltaScan, ForwardScanDirection) != NULL) {
            hasData = true;
        }

        heap_endscan(deltaScan);
        relation_close(deltaRel, AccessShareLock);

        if (!hasData) {
            /* drop the old delta table and create a new one */
            drop_delta_rel(deltaRelid);
            (void)CreateDeltaTable(mainRel, reloptions, false, NULL);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Sync failed because there is data inside the delta table %u.", deltaRelid)));
        }
    }
}

/*
 * do the sync work locally and send it to remote.
 *
 * @in mainRel: the column store main relation.
 * @in schema: the name of the table schema.
 * @in rel: the name of the table.
 * @out warnings: the failed tables' list
 * @out syncInfos: the successful tables' list
 * @return 0 if success, -1 if any fail.
 */
static int64 sync_local_and_remote(
    Relation mainRel, const char* schema, const char* relname, StringInfo warnings, StringInfo syncInfos)
{
    int64 ret = 0;
    MemoryContext oldcontext = CurrentMemoryContext;
    ResourceOwner oldowner = t_thrd.utils_cxt.CurrentResourceOwner;
    pgxc_node_remote_savepoint("Savepoint s1", EXEC_ON_ALL_NODES, true, true);
    BeginInternalSubTransaction(NULL);

    /* Want to run statements inside function's memory context */
    MemoryContextSwitchTo(oldcontext);
    PG_TRY();
    {
        /* for non-match table(including partition table here), send to remote nodes to do sync */
        ret = send_sync_to_remote(schema, relname);

        /* sync local node  */
        sync_cstore_delta_Internal(mainRel, (Datum)0);

        /* Commit the inner transaction, return to outer xact context */
        ReleaseCurrentSubTransaction();

        pgxc_node_remote_savepoint("release s1", EXEC_ON_ALL_NODES, false, false);

        MemoryContextSwitchTo(oldcontext);

        t_thrd.utils_cxt.CurrentResourceOwner = oldowner;
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcontext);
        FlushErrorState();

        RollbackAndReleaseCurrentSubTransaction();

        /* CN send rollback savepoint to remote nodes to abort sub transaction remotely */
        pgxc_node_remote_savepoint("rollback to s1", EXEC_ON_ALL_NODES, false, false);

        /* CN should send release savepoint command to remote nodes for savepoint name reuse */
        pgxc_node_remote_savepoint("release s1", EXEC_ON_ALL_NODES, false, false);

        MemoryContextSwitchTo(oldcontext);

        t_thrd.utils_cxt.CurrentResourceOwner = oldowner;
        ret = -1;

        /* fill the failed tables' list */
        if (warnings->len == 0)
            appendStringInfo(warnings, "%s", relname);
        else
            appendStringInfo(warnings, ", %s", relname);
    }
    PG_END_TRY();

    /* fill the successful tables' list */
    if (ret == 0) {
        if (syncInfos->len == 0)
            appendStringInfo(syncInfos, "%s", relname);
        else
            appendStringInfo(syncInfos, ", %s", relname);
    }

    return ret;
}

Datum pg_sync_cstore_delta(PG_FUNCTION_ARGS)
{
    text* syncRel = PG_GETARG_TEXT_P(0);

    StringInfoData warning_tables;
    StringInfoData sync_tables;
    initStringInfo(&warning_tables);
    initStringInfo(&sync_tables);
    int64 finalRet = 0;

    /* for CN */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        /* sync a single table */
        RangeVar* relrv = makeRangeVarFromNameList(textToQualifiedNameList(syncRel));

        /* check if it is a column store table */
        Relation mainRel = relation_openrv(relrv, AccessShareLock);
        if (!RelationIsCUFormat(mainRel))
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("the current function does not support non-column-store table")));

        /* check if the delta is matched */
        if (check_tupledesc_match(mainRel, NULL)) {
            relation_close(mainRel, AccessShareLock);
            PG_RETURN_INT64(0);
        }
        relation_close(mainRel, AccessShareLock);

        mainRel = relation_openrv(relrv, AccessExclusiveLock);
        finalRet = sync_local_and_remote(mainRel, relrv->schemaname, relrv->relname, &warning_tables, &sync_tables);
        relation_close(mainRel, NoLock);

        if (warning_tables.len > 0) {
            ereport(WARNING,
                (errmsg("The following tables do not sync correctly, please try again. The error "
                        "detail can be found in log.\n%s",
                    warning_tables.data)));
        }
        if (sync_tables.len > 0) {
            ereport(NOTICE, (errmsg("The following tables have sync successfully:\n%s", sync_tables.data)));
        }
        PG_RETURN_INT64(finalRet);
    } else { /* for other nodes(only accept one relation on time) */
        /*
         * since we have check the tupledesc on cn, so don't check in other nodes,
         * for any erros on other nodes, return -1.
         */
        RangeVar* relrv = makeRangeVarFromNameList(textToQualifiedNameList(syncRel));
        Relation mainRel = relation_openrv(relrv, AccessExclusiveLock);

        /* sync a single table */
        sync_cstore_delta_Internal(mainRel, (Datum)0);
        relation_close(mainRel, NoLock);
    }

    PG_RETURN_INT64(finalRet);
}

Datum pg_sync_all_cstore_delta(PG_FUNCTION_ARGS)
{
    StringInfoData warning_tables;
    StringInfoData sync_tables;
    initStringInfo(&warning_tables);
    initStringInfo(&sync_tables);
    int64 finalRet = 0;

    /* for CN */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        /*
         * when null is transfered, then we search all the current database and try
         * to sync all the column store table from pg_class and pg_partition if need.
         */
        Relation pgclass = NULL;
        TableScanDesc scan = NULL;
        HeapTuple tuple = NULL;
        ScanKeyData key;
        TupleDesc pgclassdesc = GetDefaultPgClassDesc();

        /* Process all column relations listed in pg_class */
        ScanKeyInit(&key, Anum_pg_class_relkind, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(RELKIND_RELATION));

        pgclass = heap_open(RelationRelationId, AccessShareLock);
        scan = heap_beginscan(pgclass, SnapshotNow, 1, &key);

        while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
            /* work only for column store table */
            if (rel_is_column_format(tuple, pgclassdesc)) {
                /* check if the main table is ever modified */
                if (!check_tupledesc_match(NULL, tuple)) {
                    Relation mainRel = relation_open(HeapTupleGetOid(tuple), AccessExclusiveLock);
                    Form_pg_class classForm = (Form_pg_class)GETSTRUCT(tuple);

                    finalRet += sync_local_and_remote(mainRel,
                        get_namespace_name(classForm->relnamespace),
                        classForm->relname.data,
                        &warning_tables,
                        &sync_tables);

                    relation_close(mainRel, NoLock);
                }
            }
        }

        /* clean */
        heap_endscan(scan);
        heap_close(pgclass, AccessShareLock);

        if (warning_tables.len > 0)
            ereport(WARNING,
                (errmsg("The following tables do not sync successfully, please try again. The error "
                        "detail can be found in log.\n%s",
                    warning_tables.data)));

        if (sync_tables.len > 0)
            ereport(NOTICE, (errmsg("The following tables have sync successfully:\n%s", sync_tables.data)));
        PG_RETURN_INT64(finalRet);
    } else { /* for other nodes(only accept one relation on time) */
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Function pg_sync_all_cstore_delta doest not support to run on DN.")));
    }

    PG_RETURN_INT64(finalRet);
}

static bool bupgrade_is_next_cudesc_pg_class_oid_valid()
{
    if (u_sess->upg_cxt.bupgrade_next_cudesc_pg_class_oid == NULL ||
        u_sess->upg_cxt.bupgrade_cur_cudesc_pg_class_oid >= u_sess->upg_cxt.bupgrade_max_cudesc_pg_class_oid) {
        return false;
    }

    if (!OidIsValid(
            u_sess->upg_cxt.bupgrade_next_cudesc_pg_class_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_pg_class_oid])) {
        return false;
    }

    return true;
}

Oid bupgrade_get_next_cudesc_pg_class_oid()
{
    Oid old_cudesc_pg_class_oid = InvalidOid;
    if (bupgrade_is_next_cudesc_pg_class_oid_valid() == false) {
        return InvalidOid;
    }

    old_cudesc_pg_class_oid =
        u_sess->upg_cxt.bupgrade_next_cudesc_pg_class_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_pg_class_oid];

    u_sess->upg_cxt.bupgrade_next_cudesc_pg_class_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_pg_class_oid] = InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_cudesc_pg_class_oid++;

    return old_cudesc_pg_class_oid;
}

Oid bupgrade_get_next_cudesc_pg_class_rfoid()
{
    Oid old_cudesc_pg_class_rfoid = InvalidOid;

    if (u_sess->upg_cxt.bupgrade_next_cudesc_pg_class_rfoid == NULL) {
        return InvalidOid;
    }

    if (u_sess->upg_cxt.bupgrade_cur_cudesc_pg_class_rfoid >= u_sess->upg_cxt.bupgrade_max_cudesc_pg_class_rfoid) {
        return InvalidOid;
    }

    old_cudesc_pg_class_rfoid =
        u_sess->upg_cxt.bupgrade_next_cudesc_pg_class_rfoid[u_sess->upg_cxt.bupgrade_cur_cudesc_pg_class_rfoid];

    u_sess->upg_cxt.bupgrade_next_cudesc_pg_class_rfoid[u_sess->upg_cxt.bupgrade_cur_cudesc_pg_class_rfoid] =
        InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_cudesc_pg_class_rfoid++;

    return old_cudesc_pg_class_rfoid;
}

static bool bupgrade_is_next_cudesc_pg_type_oid_valid()
{
    if (u_sess->upg_cxt.bupgrade_next_cudesc_pg_type_oid == NULL ||
        u_sess->upg_cxt.bupgrade_cur_cudesc_pg_type_oid >= u_sess->upg_cxt.bupgrade_max_cudesc_pg_type_oid) {
        return false;
    }

    if (!OidIsValid(
            u_sess->upg_cxt.bupgrade_next_cudesc_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_pg_type_oid])) {
        return false;
    }

    return true;
}

Oid bupgrade_get_next_cudesc_pg_type_oid()
{
    Oid old_cudesc_pg_type_oid = InvalidOid;
    if (bupgrade_is_next_cudesc_pg_type_oid_valid() == false) {
        return InvalidOid;
    }

    old_cudesc_pg_type_oid =
        u_sess->upg_cxt.bupgrade_next_cudesc_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_pg_type_oid];

    u_sess->upg_cxt.bupgrade_next_cudesc_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_pg_type_oid] = InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_cudesc_pg_type_oid++;

    return old_cudesc_pg_type_oid;
}

static bool bupgrade_is_next_cudesc_array_pg_type_oid_valid()
{
    if (u_sess->upg_cxt.bupgrade_next_cudesc_array_pg_type_oid == NULL ||
        u_sess->upg_cxt.bupgrade_cur_cudesc_array_pg_type_oid >=
            u_sess->upg_cxt.bupgrade_max_cudesc_array_pg_type_oid) {
        return false;
    }

    if (!OidIsValid(
            u_sess->upg_cxt
                .bupgrade_next_cudesc_array_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_array_pg_type_oid])) {
        return false;
    }

    return true;
}

Oid bupgrade_get_next_cudesc_array_pg_type_oid()
{
    Oid old_cudesc_array_pg_type_oid = InvalidOid;
    if (bupgrade_is_next_cudesc_array_pg_type_oid_valid() == false) {
        return InvalidOid;
    }

    old_cudesc_array_pg_type_oid =
        u_sess->upg_cxt.bupgrade_next_cudesc_array_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_array_pg_type_oid];

    u_sess->upg_cxt.bupgrade_next_cudesc_array_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_array_pg_type_oid] =
        InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_cudesc_array_pg_type_oid++;

    return old_cudesc_array_pg_type_oid;
}

static bool bupgrade_is_next_cudesc_index_oid_valid()
{
    if (u_sess->upg_cxt.bupgrade_next_cudesc_index_oid == NULL ||
        u_sess->upg_cxt.bupgrade_cur_cudesc_index_oid >= u_sess->upg_cxt.bupgrade_max_cudesc_index_oid) {
        return false;
    }

    if (!OidIsValid(u_sess->upg_cxt.bupgrade_next_cudesc_index_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_index_oid])) {
        return false;
    }

    return true;
}

Oid bupgrade_get_next_cudesc_index_oid()
{
    Oid old_cudesc_index_oid = InvalidOid;
    if (bupgrade_is_next_cudesc_index_oid_valid() == false) {
        return InvalidOid;
    }

    old_cudesc_index_oid =
        u_sess->upg_cxt.bupgrade_next_cudesc_index_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_index_oid];

    u_sess->upg_cxt.bupgrade_next_cudesc_index_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_index_oid] = InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_cudesc_index_oid++;

    return old_cudesc_index_oid;
}

Oid bupgrade_get_next_cudesc_index_rfoid()
{
    Oid old_cudesc_index_rfoid = InvalidOid;

    if (u_sess->upg_cxt.bupgrade_next_cudesc_index_rfoid == NULL) {
        return InvalidOid;
    }

    if (u_sess->upg_cxt.bupgrade_cur_cudesc_index_rfoid >= u_sess->upg_cxt.bupgrade_max_cudesc_index_rfoid) {
        return InvalidOid;
    }

    old_cudesc_index_rfoid =
        u_sess->upg_cxt.bupgrade_next_cudesc_index_rfoid[u_sess->upg_cxt.bupgrade_cur_cudesc_index_rfoid];

    u_sess->upg_cxt.bupgrade_next_cudesc_index_rfoid[u_sess->upg_cxt.bupgrade_cur_cudesc_index_rfoid] = InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_cudesc_index_rfoid++;

    return old_cudesc_index_rfoid;
}

static bool bupgrade_is_next_delta_pg_class_oid_valid()
{
    if (u_sess->upg_cxt.bupgrade_next_delta_pg_class_oid == NULL ||
        u_sess->upg_cxt.bupgrade_cur_delta_pg_class_oid >= u_sess->upg_cxt.bupgrade_max_delta_pg_class_oid) {
        return false;
    }

    if (!OidIsValid(
            u_sess->upg_cxt.bupgrade_next_delta_pg_class_oid[u_sess->upg_cxt.bupgrade_cur_delta_pg_class_oid])) {
        return false;
    }

    return true;
}

static bool bupgrade_is_next_cudesc_toast_pg_class_oid_valid()
{
    if (u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_class_oid == NULL ||
        u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_class_oid >=
            u_sess->upg_cxt.bupgrade_max_cudesc_toast_pg_class_oid) {
        return false;
    }

    if (!OidIsValid(
            u_sess->upg_cxt
                .bupgrade_next_cudesc_toast_pg_class_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_class_oid])) {
        return false;
    }

    return true;
}

Oid bupgrade_get_next_cudesc_toast_pg_class_oid()
{
    Oid old_cudesc_toast_pg_class_oid = InvalidOid;
    if (bupgrade_is_next_cudesc_toast_pg_class_oid_valid() == false) {
        return InvalidOid;
    }

    old_cudesc_toast_pg_class_oid =
        u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_class_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_class_oid];

    u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_class_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_class_oid] =
        InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_class_oid++;

    return old_cudesc_toast_pg_class_oid;
}

Oid bupgrade_get_next_cudesc_toast_pg_class_rfoid()
{
    Oid old_cudesc_toast_pg_class_rfoid = InvalidOid;

    if (u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_class_rfoid == NULL) {
        return InvalidOid;
    }

    if (u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_class_rfoid >=
        u_sess->upg_cxt.bupgrade_max_cudesc_toast_pg_class_rfoid) {
        return InvalidOid;
    }

    old_cudesc_toast_pg_class_rfoid =
        u_sess->upg_cxt
            .bupgrade_next_cudesc_toast_pg_class_rfoid[u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_class_rfoid];

    u_sess->upg_cxt
        .bupgrade_next_cudesc_toast_pg_class_rfoid[u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_class_rfoid] =
        InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_class_rfoid++;

    return old_cudesc_toast_pg_class_rfoid;
}

static bool bupgrade_is_next_cudesc_toast_pg_type_oid_valid()
{
    if (u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_type_oid == NULL ||
        u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_type_oid >=
            u_sess->upg_cxt.bupgrade_max_cudesc_toast_pg_type_oid) {
        return false;
    }

    if (!OidIsValid(
            u_sess->upg_cxt
                .bupgrade_next_cudesc_toast_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_type_oid])) {
        return false;
    }

    return true;
}

Oid bupgrade_get_next_cudesc_toast_pg_type_oid()
{
    Oid old_cudesc_toast_pg_type_oid = InvalidOid;
    if (bupgrade_is_next_cudesc_toast_pg_type_oid_valid() == false) {
        return InvalidOid;
    }

    old_cudesc_toast_pg_type_oid =
        u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_type_oid];

    u_sess->upg_cxt.bupgrade_next_cudesc_toast_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_type_oid] =
        InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_cudesc_toast_pg_type_oid++;

    return old_cudesc_toast_pg_type_oid;
}

static bool bupgrade_is_next_cudesc_toast_index_oid_valid()
{
    if (u_sess->upg_cxt.bupgrade_next_cudesc_toast_index_oid == NULL ||
        u_sess->upg_cxt.bupgrade_cur_cudesc_toast_index_oid >= u_sess->upg_cxt.bupgrade_max_cudesc_toast_index_oid) {
        return false;
    }

    if (!OidIsValid(u_sess->upg_cxt
                        .bupgrade_next_cudesc_toast_index_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_toast_index_oid])) {
        return false;
    }

    return true;
}

Oid bupgrade_get_next_cudesc_toast_index_oid()
{
    Oid old_cudesc_toast_index_oid = InvalidOid;
    if (bupgrade_is_next_cudesc_toast_index_oid_valid() == false) {
        return InvalidOid;
    }

    old_cudesc_toast_index_oid =
        u_sess->upg_cxt.bupgrade_next_cudesc_toast_index_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_toast_index_oid];

    u_sess->upg_cxt.bupgrade_next_cudesc_toast_index_oid[u_sess->upg_cxt.bupgrade_cur_cudesc_toast_index_oid] =
        InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_cudesc_toast_index_oid++;

    return old_cudesc_toast_index_oid;
}

Oid bupgrade_get_next_cudesc_toast_index_rfoid()
{
    Oid old_cudesc_toast_index_rfoid = InvalidOid;

    if (u_sess->upg_cxt.bupgrade_next_cudesc_toast_index_rfoid == NULL) {
        return InvalidOid;
    }

    if (u_sess->upg_cxt.bupgrade_cur_cudesc_toast_index_rfoid >=
        u_sess->upg_cxt.bupgrade_max_cudesc_toast_index_rfoid) {
        return InvalidOid;
    }

    old_cudesc_toast_index_rfoid =
        u_sess->upg_cxt.bupgrade_next_cudesc_toast_index_rfoid[u_sess->upg_cxt.bupgrade_cur_cudesc_toast_index_rfoid];

    u_sess->upg_cxt.bupgrade_next_cudesc_toast_index_rfoid[u_sess->upg_cxt.bupgrade_cur_cudesc_toast_index_rfoid] =
        InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_cudesc_toast_index_rfoid++;

    return old_cudesc_toast_index_rfoid;
}

static Oid bupgrade_get_next_delta_pg_class_oid()
{
    Oid old_delta_pg_class_oid = InvalidOid;
    if (bupgrade_is_next_delta_pg_class_oid_valid() == false) {
        return InvalidOid;
    }

    old_delta_pg_class_oid =
        u_sess->upg_cxt.bupgrade_next_delta_pg_class_oid[u_sess->upg_cxt.bupgrade_cur_delta_pg_class_oid];

    u_sess->upg_cxt.bupgrade_next_delta_pg_class_oid[u_sess->upg_cxt.bupgrade_cur_delta_pg_class_oid] = InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_delta_pg_class_oid++;

    return old_delta_pg_class_oid;
}

static Oid bupgrade_get_next_delta_pg_class_rfoid()
{
    Oid old_delta_pg_class_rfoid = InvalidOid;

    if (u_sess->upg_cxt.bupgrade_next_delta_pg_class_rfoid == NULL) {
        return InvalidOid;
    }

    if (u_sess->upg_cxt.bupgrade_cur_delta_pg_class_rfoid >= u_sess->upg_cxt.bupgrade_max_delta_pg_class_rfoid) {
        return InvalidOid;
    }

    old_delta_pg_class_rfoid =
        u_sess->upg_cxt.bupgrade_next_delta_pg_class_rfoid[u_sess->upg_cxt.bupgrade_cur_delta_pg_class_rfoid];

    u_sess->upg_cxt.bupgrade_next_delta_pg_class_rfoid[u_sess->upg_cxt.bupgrade_cur_delta_pg_class_rfoid] = InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_delta_pg_class_rfoid++;

    return old_delta_pg_class_rfoid;
}

static bool bupgrade_is_next_delta_pg_type_oid_valid()
{
    if (u_sess->upg_cxt.bupgrade_next_delta_pg_type_oid == NULL ||
        u_sess->upg_cxt.bupgrade_cur_delta_pg_type_oid >= u_sess->upg_cxt.bupgrade_max_delta_pg_type_oid) {
        return false;
    }

    if (!OidIsValid(u_sess->upg_cxt.bupgrade_next_delta_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_delta_pg_type_oid])) {
        return false;
    }

    return true;
}

static Oid bupgrade_get_next_delta_pg_type_oid()
{
    Oid old_delta_pg_type_oid = InvalidOid;
    if (bupgrade_is_next_delta_pg_type_oid_valid() == false) {
        return InvalidOid;
    }

    old_delta_pg_type_oid =
        u_sess->upg_cxt.bupgrade_next_delta_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_delta_pg_type_oid];

    u_sess->upg_cxt.bupgrade_next_delta_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_delta_pg_type_oid] = InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_delta_pg_type_oid++;

    return old_delta_pg_type_oid;
}

static bool bupgrade_is_next_delta_array_pg_type_oid_valid()
{
    if (u_sess->upg_cxt.bupgrade_next_delta_array_pg_type_oid == NULL ||
        u_sess->upg_cxt.bupgrade_cur_delta_array_pg_type_oid >= u_sess->upg_cxt.bupgrade_max_delta_array_pg_type_oid) {
        return false;
    }

    if (!OidIsValid(u_sess->upg_cxt
                        .bupgrade_next_delta_array_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_delta_array_pg_type_oid])) {
        return false;
    }

    return true;
}

static Oid bupgrade_get_next_delta_array_pg_type_oid()
{
    Oid old_delta_array_pg_type_oid = InvalidOid;
    if (bupgrade_is_next_delta_array_pg_type_oid_valid() == false) {
        return InvalidOid;
    }

    old_delta_array_pg_type_oid =
        u_sess->upg_cxt.bupgrade_next_delta_array_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_delta_array_pg_type_oid];

    u_sess->upg_cxt.bupgrade_next_delta_array_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_delta_array_pg_type_oid] =
        InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_delta_array_pg_type_oid++;

    return old_delta_array_pg_type_oid;
}

static bool bupgrade_is_next_delta_toast_pg_class_oid_valid()
{
    if (u_sess->upg_cxt.bupgrade_next_delta_toast_pg_class_oid == NULL ||
        u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_class_oid >=
            u_sess->upg_cxt.bupgrade_max_delta_toast_pg_class_oid) {
        return false;
    }

    if (!OidIsValid(
            u_sess->upg_cxt
                .bupgrade_next_delta_toast_pg_class_oid[u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_class_oid])) {
        return false;
    }

    return true;
}

static Oid bupgrade_get_next_delta_toast_pg_class_oid()
{
    Oid old_delta_toast_pg_class_oid = InvalidOid;
    if (bupgrade_is_next_delta_toast_pg_class_oid_valid() == false) {
        return InvalidOid;
    }

    old_delta_toast_pg_class_oid =
        u_sess->upg_cxt.bupgrade_next_delta_toast_pg_class_oid[u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_class_oid];

    u_sess->upg_cxt.bupgrade_next_delta_toast_pg_class_oid[u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_class_oid] =
        InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_class_oid++;

    return old_delta_toast_pg_class_oid;
}

static Oid bupgrade_get_next_delta_toast_pg_class_rfoid()
{
    Oid old_delta_toast_pg_class_rfoid = InvalidOid;

    if (u_sess->upg_cxt.bupgrade_next_delta_toast_pg_class_rfoid == NULL) {
        return InvalidOid;
    }

    if (u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_class_rfoid >=
        u_sess->upg_cxt.bupgrade_max_delta_toast_pg_class_rfoid) {
        return InvalidOid;
    }

    old_delta_toast_pg_class_rfoid =
        u_sess->upg_cxt
            .bupgrade_next_delta_toast_pg_class_rfoid[u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_class_rfoid];

    u_sess->upg_cxt.bupgrade_next_delta_toast_pg_class_rfoid[u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_class_rfoid] =
        InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_class_rfoid++;

    return old_delta_toast_pg_class_rfoid;
}

static bool bupgrade_is_next_delta_toast_pg_type_oid_valid()
{
    if (u_sess->upg_cxt.bupgrade_next_delta_toast_pg_type_oid == NULL ||
        u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_type_oid >= u_sess->upg_cxt.bupgrade_max_delta_toast_pg_type_oid) {
        return false;
    }

    if (!OidIsValid(u_sess->upg_cxt
                        .bupgrade_next_delta_toast_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_type_oid])) {
        return false;
    }

    return true;
}

static Oid bupgrade_get_next_delta_toast_pg_type_oid()
{
    Oid old_delta_toast_pg_type_oid = InvalidOid;
    if (bupgrade_is_next_delta_toast_pg_type_oid_valid() == false) {
        return InvalidOid;
    }

    old_delta_toast_pg_type_oid =
        u_sess->upg_cxt.bupgrade_next_delta_toast_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_type_oid];

    u_sess->upg_cxt.bupgrade_next_delta_toast_pg_type_oid[u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_type_oid] =
        InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_delta_toast_pg_type_oid++;

    return old_delta_toast_pg_type_oid;
}

static bool bupgrade_is_next_delta_toast_index_oid_valid()
{
    if (u_sess->upg_cxt.bupgrade_next_delta_toast_index_oid == NULL ||
        u_sess->upg_cxt.bupgrade_cur_delta_toast_index_oid >= u_sess->upg_cxt.bupgrade_max_delta_toast_index_oid) {
        return false;
    }

    if (!OidIsValid(
            u_sess->upg_cxt.bupgrade_next_delta_toast_index_oid[u_sess->upg_cxt.bupgrade_cur_delta_toast_index_oid])) {
        return false;
    }

    return true;
}

static Oid bupgrade_get_next_delta_toast_index_oid()
{
    Oid old_delta_toast_index_oid = InvalidOid;
    if (bupgrade_is_next_delta_toast_index_oid_valid() == false) {
        return InvalidOid;
    }

    old_delta_toast_index_oid =
        u_sess->upg_cxt.bupgrade_next_delta_toast_index_oid[u_sess->upg_cxt.bupgrade_cur_delta_toast_index_oid];

    u_sess->upg_cxt.bupgrade_next_delta_toast_index_oid[u_sess->upg_cxt.bupgrade_cur_delta_toast_index_oid] =
        InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_delta_toast_index_oid++;

    return old_delta_toast_index_oid;
}

static Oid bupgrade_get_next_delta_toast_index_rfoid()
{
    Oid old_delta_toast_index_rfoid = InvalidOid;

    if (u_sess->upg_cxt.bupgrade_next_delta_toast_index_rfoid == NULL) {
        return InvalidOid;
    }

    if (u_sess->upg_cxt.bupgrade_cur_delta_toast_index_rfoid >= u_sess->upg_cxt.bupgrade_max_delta_toast_index_rfoid) {
        return InvalidOid;
    }

    old_delta_toast_index_rfoid =
        u_sess->upg_cxt.bupgrade_next_delta_toast_index_rfoid[u_sess->upg_cxt.bupgrade_cur_delta_toast_index_rfoid];

    u_sess->upg_cxt.bupgrade_next_delta_toast_index_rfoid[u_sess->upg_cxt.bupgrade_cur_delta_toast_index_rfoid] =
        InvalidOid;

    u_sess->upg_cxt.bupgrade_cur_delta_toast_index_rfoid++;

    return old_delta_toast_index_rfoid;
}
