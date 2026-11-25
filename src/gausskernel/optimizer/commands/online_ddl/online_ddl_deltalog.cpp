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
 * online_ddl_deltalog.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/optimizer/commands/online_ddl/online_ddl_deltalog.cpp
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

#include "commands/online_ddl.h"
#include "commands/online_ddl_append.h"
#include "commands/online_ddl_ctid_map.h"
#include "commands/online_ddl_globalhash.h"
#include "commands/online_ddl_util.h"
#include "commands/online_ddl_deltalog.h"

const int DELTALOG_OPERATION_TYPE_IDX = 0;
const int DELTALOG_TUP_CTDI_IDX = 1;
const int ONLINE_DDL_DELTALOG_ATTR_NUM = 2; /* operation_type, tup_ctid */

/**
 * @brief Check if the online DDL temporary schema name conforms to the specified format
 *
 *
 * The expected format is:
 * online_ddl_temp_schema_<transaction_id>_<tablespace_oid>_<database_oid>_<relation_oid>
 *
 * @param npName The schema name to check
 * @param xid Output parameter to store  transaction ID
 * @param hashkey   Output parameter to store hask entry key
 * @return bool True if the name conforms to the format, false otherwise
 */
bool OnlineDDLParseTempSchma(char* npName, TransactionId* xid, DDLGlobalHashKey* hashKey)
{
    const static char* prefix = "online_ddl_temp_schema_";
    size_t prefixLen = strlen(prefix);
    if (npName && pg_strncasecmp(npName, prefix, prefixLen) != 0) {
        return false;
    }

    char* endptr;
    errno = 0;
    const char* remaining = npName + prefixLen;
    uint64 transactionId = strtoul(remaining, &endptr, 10);
    if (*endptr != '_' || transactionId == UINT64_MAX || errno == ERANGE) {
        return false;
    }
    remaining = endptr + 1;

    uint64 spcn = strtoul(remaining, &endptr, 10);
    if (*endptr != '_' || spcn == UINT64_MAX || errno == ERANGE) {
        return false;
    }
    remaining = endptr + 1;

    uint64 dbn = strtoul(remaining, &endptr, 10);
    if (*endptr != '_' || dbn == UINT64_MAX || errno == ERANGE) {
        return false;
    }
    remaining = endptr + 1;

    uint64 rn = strtoul(remaining, &endptr, 10);
    if (*endptr != '\0' || rn == UINT64_MAX || errno == ERANGE) {
        return false;
    }

    if (xid != NULL) {
        *xid = transactionId;
    }
    if (hashKey != NULL) {
        hashKey->spcNode = spcn;
        hashKey->dbNode = dbn;
        hashKey->relId = rn;
    }

    return true;
}

/**
 * @brief Check if updating online ddl deltalog table,
 * deltalog table can't be modified by user, can be dropped
 * when online ddl finish.
 *
 * @param rel relation
 * @param checkInProgress
 *        true: forbid to execute such operation for online ddl deltalog if online ddl is in progress.
 *        false: forbid to execute such operation for online ddl deltalog
 */
void ErrorIfOnlineDDLDeltaLog(Relation rel, bool checkInProgress)
{
    char* relName = RelationGetRelationName(rel);
    char* npName = get_namespace_name(rel->rd_rel->relnamespace);

    TransactionId xid;
    DDLGlobalHashKey hashKey = {0, 0, 0};
    bool ret = OnlineDDLParseTempSchma(npName, &xid, &hashKey);
    if (!ret) {
        return;
    }

    if (!checkInProgress) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("[Online-DDL] Relation %s belong to online ddl temp schema %s, "
                               "do not change it.",
                               relName, npName)));
    }
    if (CheckOnlineDDLStatusRunning(hashKey, xid)) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("[Online-DDL] Relation %s belong to online ddl temp schema schema %s, and the online "
                               "DDL operation is already in progress, "
                               "do not drop it until online DDL finished.",
                               relName, npName)));
    }
}

/**
 * @brief Insert an empty operation record into the online DDL delta log
 * to avoid scan empty delta log table.
 *
 * @param deltaRelation The delta log table
 * @param tid The tuple identifier (TID) to be recorded
 *
 * @throws ERROR If the deltaRelation is NULL or invalid
 */
void OnlineDDLEmptyDeltaLog(Relation deltaRelation, ItemPointer tid)
{
    if (deltaRelation == NULL || !RelationIsValid(deltaRelation)) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("Delta relation is not valid.")));
    }
    HeapTuple tuple;
    TupleDesc tupleDesc;
    Datum values[2];
    bool isnulls[2];
    errno_t rc;
    rc = memset_s(isnulls, sizeof(isnulls), 0, sizeof(isnulls));
    securec_check(rc, "\0", "\0");
    values[0] = Int8GetDatum(ONLINE_DDL_OPERATION_EMPTY);
    values[1] = PointerGetDatum(tid);
    tupleDesc = RelationGetDescr(deltaRelation);
    tuple = heap_form_tuple(tupleDesc, values, isnulls);
    (void)tableam_tuple_insert(deltaRelation, tuple, GetCurrentCommandId(true), 0, NULL);
    ereport(ONLINE_DDL_LOG_LEVEL, (errmsg("Insert empty delta log (%u, %u)", ItemPointerGetBlockNumber(&tuple->t_self),
                                          ItemPointerGetOffsetNumber(&tuple->t_self))));
    CommandCounterIncrement();
}

void OnlineDDLInsertDeltaLog(Relation deltaRelation, ItemPointer tid)
{
    if (deltaRelation == NULL || !RelationIsValid(deltaRelation)) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("Delta relation is not valid.")));
    }
    HeapTuple tuple;
    TupleDesc tupleDesc;
    Datum values[2];
    bool isnulls[2];
    errno_t rc;
    rc = memset_s(isnulls, sizeof(isnulls), 0, sizeof(isnulls));
    securec_check(rc, "\0", "\0");
    values[0] = Int8GetDatum(ONLINE_DDL_OPERATION_INSERT);
    values[1] = PointerGetDatum(tid);
    tupleDesc = RelationGetDescr(deltaRelation);
    tuple = heap_form_tuple(tupleDesc, values, isnulls);
    (void)tableam_tuple_insert(deltaRelation, tuple, GetCurrentCommandId(true), 0, NULL);
    ereport(NOTICE, (errmsg("Insert delta log (%u, %u)", ItemPointerGetBlockNumber(&tuple->t_self),
                            ItemPointerGetOffsetNumber(&tuple->t_self))));
    CommandCounterIncrement();
}

void OnlineDDLDeleteDeltaLog(Relation deltaRelation, ItemPointer tid, Oid partOid)
{
    if (deltaRelation == NULL || !RelationIsValid(deltaRelation)) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("Delta relation is not valid.")));
    }
    ereport(ONLINE_DDL_LOG_LEVEL, (errmsg("Delete delta log (%u, %u, %u)", ItemPointerGetBlockNumber(tid),
                            ItemPointerGetOffsetNumber(tid), partOid)));
    HeapTuple tuple;
    TupleDesc tupleDesc;
    Datum values[3];
    bool isnulls[3];
    errno_t rc;
    rc = memset_s(isnulls, sizeof(isnulls), 0, sizeof(isnulls));
    values[0] = Int8GetDatum(ONLINE_DDL_OPERATION_DELETE);
    values[1] = PointerGetDatum(tid);
    values[ONLINE_DDL_DELTALOG_ATTR_NUM] = ObjectIdGetDatum(partOid);
    tupleDesc = RelationGetDescr(deltaRelation);
    tuple = heap_form_tuple(tupleDesc, values, isnulls);
    (void)tableam_tuple_insert(deltaRelation, tuple, GetCurrentCommandId(true), 0, NULL);
    CommandCounterIncrement();
}

void OnlineDDLDeleteDeltaLog(Relation deltaRelation, ItemPointer tid)
{
    if (deltaRelation == NULL || !RelationIsValid(deltaRelation)) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("Delta relation is not valid.")));
    }
    ereport(ONLINE_DDL_LOG_LEVEL,
            (errmsg("Delete delta log (%u, %u)", ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid))));
    HeapTuple tuple;
    TupleDesc tupleDesc;
    Datum values[2];
    bool isnulls[2];
    errno_t rc;
    rc = memset_s(isnulls, sizeof(isnulls), 0, sizeof(isnulls));
    securec_check(rc, "\0", "\0");
    values[0] = Int8GetDatum(ONLINE_DDL_OPERATION_DELETE);
    values[1] = PointerGetDatum(tid);
    tupleDesc = RelationGetDescr(deltaRelation);
    tuple = heap_form_tuple(tupleDesc, values, isnulls);
    (void)tableam_tuple_insert(deltaRelation, tuple, GetCurrentCommandId(true), 0, NULL);
    CommandCounterIncrement();
}

void OnlineDDLUpdateDeltaLog(Relation deltaRelation, ItemPointer oldTid, ItemPointer newTid)
{
    if (deltaRelation == NULL || !RelationIsValid(deltaRelation)) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("Delta relation is not valid.")));
    }
    OnlineDDLDeleteDeltaLog(deltaRelation, oldTid);
}
