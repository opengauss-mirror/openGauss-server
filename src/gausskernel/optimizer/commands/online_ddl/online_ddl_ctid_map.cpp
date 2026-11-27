/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * ---------------------------------------------------------------------------------------
 *
 * online_ddl_ctid_map.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/optimizer/commands/online_ddl/online_ddl_ctid_map.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "access/tableam.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"

#include "commands/online_ddl_util.h"
#include "commands/online_ddl_ctid_map.h"

#define DatumGetItemPointer(X) ((ItemPointer)DatumGetPointer(X))

const char* ONLINE_DDL_CTID_MAP_RELNAME = "ctid_map";
const char* ONLINE_DDL_CTID_MAP_INDEXNAME = "ctid_map_index";
const uint32 CTID_MAP_ATTR_NUM = 2; /* old_tup_ctid, new_tup_ctid */
ItemPointerData CTID_MAP_DEFAULT_CTID = (ItemPointerData){(BlockIdData){(uint16)0, (uint16)0}, (OffsetNumber)1};
ItemPointerData CTID_MAP_INVALID_CTID = (ItemPointerData){(BlockIdData){(uint16)0, (uint16)0}, (OffsetNumber)0};

static Oid CreateCtidMap(char* schemaName)
{
    Oid ctidMapOid = InvalidOid;
    Oid relid = InvalidOid;
    StringInfo query = makeStringInfo();
    appendStringInfo(query, "CREATE UNLOGGED TABLE %s.%s", schemaName, ONLINE_DDL_CTID_MAP_RELNAME);
    appendStringInfo(query, "("
                            "old_tup_ctid TID NOT NULL, "
                            "new_tup_ctid TID NOT NULL"
                            ")");
    OnlineDDLExecuteCommand(query->data);
    DestroyStringInfo(query);
    relid = get_relname_relid(ONLINE_DDL_CTID_MAP_RELNAME, get_namespace_oid(schemaName, false));
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
                        errmsg("failed to get ctid map relation oid, during online ddl, relation: %s.%s.", schemaName,
                               ONLINE_DDL_CTID_MAP_RELNAME)));
    }
    query = makeStringInfo();
    appendStringInfo(query, "CREATE INDEX %s.%s on %s.%s using btree (old_tup_ctid, new_tup_ctid);", schemaName,
                     ONLINE_DDL_CTID_MAP_INDEXNAME, schemaName, ONLINE_DDL_CTID_MAP_RELNAME);
    OnlineDDLExecuteCommand(query->data);
    DestroyStringInfo(query);
    return relid;
}

static Oid CreateCtidMapForPartitionedTable(char* schemaName)
{
    Oid ctidMapOid = InvalidOid;
    Oid relid = InvalidOid;
    StringInfo query = makeStringInfo();
    appendStringInfo(query, "CREATE UNLOGGED TABLE %s.%s", schemaName, ONLINE_DDL_CTID_MAP_RELNAME);
    appendStringInfo(query, "("
                            "old_tup_ctid TID NOT NULL, "
                            "partition_no OID NOT NULL,"
                            "new_tup_ctid TID NOT NULL"
                            ")");
    OnlineDDLExecuteCommand(query->data);
    DestroyStringInfo(query);
    relid = get_relname_relid(ONLINE_DDL_CTID_MAP_RELNAME, get_namespace_oid(schemaName, false));
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
                        errmsg("failed to get ctid map relation oid, during online ddl, relation: %s.%s.", schemaName,
                               ONLINE_DDL_CTID_MAP_RELNAME)));
    }
    query = makeStringInfo();
    appendStringInfo(query, "CREATE INDEX %s.%s on %s.%s using btree (old_tup_ctid, partition_no, new_tup_ctid);",
                     schemaName, ONLINE_DDL_CTID_MAP_INDEXNAME, schemaName, ONLINE_DDL_CTID_MAP_RELNAME);
    OnlineDDLExecuteCommand(query->data);
    DestroyStringInfo(query);
    return relid;
}

/**
 * @brief Create a ctid map table for the given relation.
 *
 * @param rel
 * @return Oid the oid of the created ctid map table
 */
Oid OnlineDDLCreateCtidMap(StringInfo tempSchemaName)
{
    Assert(tempSchemaName != NULL && tempSchemaName->len > 0);
    OnlineDDLRelOperators* operators = ((OnlineDDLRelOperators*)u_sess->online_ddl_operators);
    bool isForPartition = operators->getIsForPartition();
    /*
     * Create ctid map table for partitioned table.
     * For partitioned table, we need to record the partition number for each delta record.
     * For non-partitioned table, we only record the operation type and old tuple ctid.
     * If vacuuming partitioned table, we rewrite one partition each time, no need to record partition number.
     * If clustering partitioned table, we cluster all partition at once, no need to record partition number.
     */
    if (isForPartition && (!OnlineDDLIsVacuumOrClusterMode(operators) || OnlineDDLIsClusterAllPartition(operators))) {
        return CreateCtidMapForPartitionedTable(tempSchemaName->data);
    } else {
        return CreateCtidMap(tempSchemaName->data);
    }
}

/*
 * Insert a ctid map entry into the ctid map table.
 * Ctid map only has index tuple, no heap data tuple, to speed up.
 */
bool OnlineDDLInsertCtidMap(ItemPointer oldTid, ItemPointer newTid, Relation relation)
{
    List* indexOidList = NIL;
    ListCell* l = NULL;
    if (relation == NULL || oldTid == NULL || newTid == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("relation or old/new ctid is null, during online ddl.")));
    }
    indexOidList = RelationGetIndexList(relation);
    Assert(list_length(indexOidList) == 1);
    foreach (l, indexOidList) {
        Oid indexOid = lfirst_oid(l);
        Relation indexDesc;
        Datum values[CTID_MAP_ATTR_NUM];
        bool isnull[CTID_MAP_ATTR_NUM];
        indexDesc = index_open(indexOid, AccessShareLock);
        values[0] = PointerGetDatum(oldTid);
        values[1] = PointerGetDatum(newTid);
        errno_t rc = memset_s(isnull, sizeof(isnull), 0, sizeof(isnull));
        securec_check_c(rc, "\0", "\0");
#ifdef USE_ASSERT_CHECKING
        index_insert(indexDesc, values, isnull, &CTID_MAP_DEFAULT_CTID, relation, UNIQUE_CHECK_PARTIAL);
#else
        index_insert(indexDesc, values, isnull, &CTID_MAP_DEFAULT_CTID, relation, UNIQUE_CHECK_NO);
#endif
        CommandCounterIncrement();
        index_close(indexDesc, AccessShareLock);
    }
    list_free_ext(indexOidList);
    return true;
}

/*
 * Insert a ctid map entry into the ctid map table.
 * Ctid map only used for insert/select by index scan,
 * so it only has index tuple, no heap data tuple, to speed up.
 */
bool OnlineDDLInsertCtidMap(ItemPointer oldTid, Oid relId, ItemPointer newTid, Relation relation)
{
    List* indexOidList = NIL;
    ListCell* l = NULL;
    if (relation == NULL || oldTid == NULL || newTid == NULL || relId == InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("relation or old/new ctid is null, during online ddl.")));
    }
    indexOidList = RelationGetIndexList(relation);
    Assert(list_length(indexOidList) == 1);
    foreach (l, indexOidList) {
        Oid indexOid = lfirst_oid(l);
        Relation indexDesc;
        Datum values[3];
        bool isnull[3];
        indexDesc = index_open(indexOid, AccessShareLock);
        values[0] = PointerGetDatum(oldTid);
        values[1] = PointerGetDatum(relId);
        values[CTID_MAP_ATTR_NUM] = PointerGetDatum(newTid);
        errno_t rc = memset_s(isnull, sizeof(isnull), 0, sizeof(isnull));
        securec_check_c(rc, "\0", "\0");
#ifdef USE_ASSERT_CHECKING
        index_insert(indexDesc, values, isnull, &CTID_MAP_DEFAULT_CTID, relation, UNIQUE_CHECK_PARTIAL);
#else
        index_insert(indexDesc, values, isnull, &CTID_MAP_DEFAULT_CTID, relation, UNIQUE_CHECK_NO);
#endif
        CommandCounterIncrement();
        index_close(indexDesc, AccessShareLock);
    }
    list_free_ext(indexOidList);
    return true;
}

ItemPointerData OnlineDDLGetTargetCtid(ItemPointer oldTid, Relation relation, Relation indexRelation)
{
    if (oldTid == NULL || !ItemPointerIsValid(oldTid)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("OnlineDDLGetTargetCtid error: old ctid is null or invalid.")));
    }
    if (relation == NULL || !RelationIsValid(relation)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("OnlineDDLGetTargetCtid error: relation is null or invalid.")));
    }
    if (indexRelation == NULL || !RelationIsValid(indexRelation)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("OnlineDDLGetTargetCtid error: index relation is null or invalid.")));
    }

    Datum values[CTID_MAP_ATTR_NUM];
    bool isnull[CTID_MAP_ATTR_NUM];
    ItemPointerData result = CTID_MAP_INVALID_CTID;

    IndexScanDesc indexScanDesc;
    ScanState* scanState = makeNode(ScanState);
    bool bucketChanged = false;

    ScanKeyData scanKey[CTID_MAP_ATTR_NUM];
    const AttrNumber keyNum = 1;
    const int oldCtidAttrId = 1;
    /* function oid of tideq */
    const RegProcedure TIDEQ_OID = 1292;
    ItemPointer tid = NULL;
    ScanKeyEntryInitialize(&scanKey[0], 0, oldCtidAttrId, BTEqualStrategyNumber, TIDOID, 0, TIDEQ_OID,
                           PointerGetDatum(oldTid));
    ScanDirection scandirection = ForwardScanDirection;
    indexScanDesc = scan_handler_idx_beginscan(relation, indexRelation, SnapshotNow, keyNum, 0, scanState);
    indexScanDesc->xs_want_itup = true;
    scan_handler_idx_rescan_local(indexScanDesc, scanKey, keyNum, NULL, 0);
    if ((tid = scan_handler_idx_getnext_tid(indexScanDesc, scandirection, &bucketChanged)) != NULL) {
        IndexScanDesc indexdesc = GetIndexScanDesc(indexScanDesc);
        index_deform_tuple(indexdesc->xs_itup, RelationGetDescr(indexRelation), values, isnull);
#ifdef USE_ASSERT_CHECKING
        ItemPointer resultOldTid = DatumGetItemPointer(values[0]);
        Assert(ItemPointerEquals(resultOldTid, oldTid));
#endif
        ItemPointerSetBlockNumber(&result, ItemPointerGetBlockNumber(DatumGetItemPointer(values[1])));
        ItemPointerSetOffsetNumber(&result, ItemPointerGetOffsetNumber(DatumGetItemPointer(values[1])));
    }
    pfree(scanState);
    scan_handler_idx_endscan(indexScanDesc);

    return result;
}

ItemPointerData OnlineDDLGetTargetCtid(ItemPointer oldTid, Oid* partOid, Relation relation, Relation indexRelation)
{
    if (oldTid == NULL || !ItemPointerIsValid(oldTid)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("OnlineDDLGetTargetCtid error: old ctid is null or invalid.")));
    }
    if (partOid == NULL || *partOid == InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("OnlineDDLGetTargetCtid error: part oid is null or invalid.")));
    }
    if (relation == NULL || !RelationIsValid(relation)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("OnlineDDLGetTargetCtid error: relation is null or invalid.")));
    }
    if (indexRelation == NULL || !RelationIsValid(indexRelation)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("OnlineDDLGetTargetCtid error: index relation is null or invalid.")));
    }

    Datum values[3];
    bool isnull[3];
    ItemPointerData result = CTID_MAP_INVALID_CTID;

    IndexScanDesc indexScanDesc;
    ScanState* scanState = makeNode(ScanState);
    bool bucketChanged = false;

    ScanKeyData scanKey[3];
    const AttrNumber keyNum = 2;
    const int oldCtidAttrId = 1;
    const int partOidAttrId = 2;
    const RegProcedure tideqOid = 1292;
    const RegProcedure oideqOid = 184;

    ItemPointer tid = NULL;
    ScanKeyEntryInitialize(&scanKey[0], 0, oldCtidAttrId, BTEqualStrategyNumber, TIDOID, 0, tideqOid,
                           PointerGetDatum(oldTid));
    ScanKeyEntryInitialize(&scanKey[1], 0, partOidAttrId, BTEqualStrategyNumber, OIDOID, 0, oideqOid,
                           ObjectIdGetDatum(*partOid));

    ScanDirection scandirection = ForwardScanDirection;
    indexScanDesc = scan_handler_idx_beginscan(relation, indexRelation, SnapshotNow, keyNum, 0, scanState);
    indexScanDesc->xs_want_itup = true;
    scan_handler_idx_rescan_local(indexScanDesc, scanKey, keyNum, NULL, 0);

    if ((tid = scan_handler_idx_getnext_tid(indexScanDesc, scandirection, &bucketChanged)) != NULL) {
        IndexScanDesc indexdesc = GetIndexScanDesc(indexScanDesc);
        index_deform_tuple(indexdesc->xs_itup, RelationGetDescr(indexRelation), values, isnull);
#ifdef USE_ASSERT_CHECKING
        ItemPointer resultOldTid = DatumGetItemPointer(values[0]);
        Assert(ItemPointerEquals(resultOldTid, oldTid));
#endif
        ItemPointerSetBlockNumber(&result, ItemPointerGetBlockNumber(DatumGetItemPointer(values[2])));
        ItemPointerSetOffsetNumber(&result, ItemPointerGetOffsetNumber(DatumGetItemPointer(values[2])));
    }
    pfree(scanState);
    scan_handler_idx_endscan(indexScanDesc);
    return result;
}

void OldCtidGetNewPartitionAndCtid(ItemPointer oldTid, Oid* partOid, ItemPointer newTid, Relation relation,
                                   Relation indexRelation)
{
    if (oldTid == NULL || !ItemPointerIsValid(oldTid)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("OnlineDDLGetTargetCtid error: old ctid is null or invalid.")));
    }
    if (relation == NULL || !RelationIsValid(relation)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("OnlineDDLGetTargetCtid error: relation is null or invalid.")));
    }
    if (indexRelation == NULL || !RelationIsValid(indexRelation)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("OnlineDDLGetTargetCtid error: index relation is null or invalid.")));
    }

    Datum values[3];
    bool isnull[3];

    IndexScanDesc indexScanDesc;
    ScanState* scanState = makeNode(ScanState);
    bool bucketChanged = false;

    ScanKeyData scanKey[3];
    const AttrNumber keyNum = 1;
    const int oldCtidAttrId = 1;
    const RegProcedure tideqOid = 1292;

    ItemPointer tid = NULL;
    ScanKeyEntryInitialize(&scanKey[0], 0, oldCtidAttrId, BTEqualStrategyNumber, TIDOID, 0, tideqOid,
                           PointerGetDatum(oldTid));

    ScanDirection scandirection = ForwardScanDirection;
    indexScanDesc = scan_handler_idx_beginscan(relation, indexRelation, SnapshotNow, keyNum, 0, scanState);
    indexScanDesc->xs_want_itup = true;
    scan_handler_idx_rescan_local(indexScanDesc, scanKey, keyNum, NULL, 0);

    if ((tid = scan_handler_idx_getnext_tid(indexScanDesc, scandirection, &bucketChanged)) != NULL) {
        IndexScanDesc indexdesc = GetIndexScanDesc(indexScanDesc);
        index_deform_tuple(indexdesc->xs_itup, RelationGetDescr(indexRelation), values, isnull);
#ifdef USE_ASSERT_CHECKING
        ItemPointer resultOldTid = DatumGetItemPointer(values[0]);
        Assert(ItemPointerEquals(resultOldTid, oldTid));
#endif

        *partOid = DatumGetObjectId(values[1]);
        ItemPointerSetBlockNumber(newTid, ItemPointerGetBlockNumber(DatumGetItemPointer(values[2])));
        ItemPointerSetOffsetNumber(newTid, ItemPointerGetOffsetNumber(DatumGetItemPointer(values[2])));
    } else {
        *partOid = InvalidOid;
        ItemPointerSetInvalid(newTid);
    }

    pfree(scanState);
    scan_handler_idx_endscan(indexScanDesc);
}

bool OldCtidAndPartitionGetNewCtid(ItemPointer oldTid, Oid* partOid, ItemPointer newTid, Relation relation,
                                   Relation indexRelation)
{
    if (oldTid == NULL || !ItemPointerIsValid(oldTid)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("OnlineDDLGetTargetCtid error: old ctid is null or invalid.")));
    }
    if (partOid == NULL || *partOid == InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("OnlineDDLGetTargetCtid error: part oid is null or invalid.")));
    }
    if (relation == NULL || !RelationIsValid(relation)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("OnlineDDLGetTargetCtid error: relation is null or invalid.")));
    }
    if (indexRelation == NULL || !RelationIsValid(indexRelation)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("OnlineDDLGetTargetCtid error: index relation is null or invalid.")));
    }

    Datum values[3];
    bool isnull[3];

    IndexScanDesc indexScanDesc;
    ScanState* scanState = makeNode(ScanState);
    bool bucketChanged = false;

    ScanKeyData scanKey[3];
    const AttrNumber keyNum = 2;
    const int oldCtidAttrId = 1;
    const int partOidAttrId = 2;
    const RegProcedure tideqOid = 1292;
    const RegProcedure oideqOid = 184;

    ItemPointer tid = NULL;
    ScanKeyEntryInitialize(&scanKey[0], 0, oldCtidAttrId, BTEqualStrategyNumber, TIDOID, 0, tideqOid,
                           PointerGetDatum(oldTid));
    ScanKeyEntryInitialize(&scanKey[1], 0, partOidAttrId, BTEqualStrategyNumber, OIDOID, 0, oideqOid,
                           ObjectIdGetDatum(*partOid));

    ScanDirection scandirection = ForwardScanDirection;
    indexScanDesc = scan_handler_idx_beginscan(relation, indexRelation, SnapshotNow, keyNum, 0, scanState);
    indexScanDesc->xs_want_itup = true;
    scan_handler_idx_rescan_local(indexScanDesc, scanKey, keyNum, NULL, 0);

    bool found = false;
    if ((tid = scan_handler_idx_getnext_tid(indexScanDesc, scandirection, &bucketChanged)) != NULL) {
        IndexScanDesc indexdesc = GetIndexScanDesc(indexScanDesc);
        index_deform_tuple(indexdesc->xs_itup, RelationGetDescr(indexRelation), values, isnull);
#ifdef USE_ASSERT_CHECKING
        ItemPointer resultOldTid = DatumGetItemPointer(values[0]);
        Assert(ItemPointerEquals(resultOldTid, oldTid));
#endif
        ItemPointerSetBlockNumber(newTid, ItemPointerGetBlockNumber(DatumGetItemPointer(values[2])));
        ItemPointerSetOffsetNumber(newTid, ItemPointerGetOffsetNumber(DatumGetItemPointer(values[2])));
        found = true;
    }
    pfree(scanState);
    scan_handler_idx_endscan(indexScanDesc);

    return found;
}