/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * sql_limit_process.cpp
 *
 * The file is used to provide the process of sql limit.
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/workload/sql_limit_process.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "workload/sql_limit_process.h"
#include "postgres.h"
#include "catalog/gs_sql_limit.h"
#include "catalog/indexing.h"
#include "utils/atomic.h"
#include "utils/hsearch.h"
#include "utils/palloc.h"
#include "utils/snapmgr.h"
#include "utils/array.h"
#include "utils/mem_snapshot.h"
#include "utils/fmgroids.h"
#include "knl/knl_variable.h"
#include "utils/builtins.h"
#include "nodes/pg_list.h"
#include "access/xact.h"
#include "utils/int8.h"

bool UpdateSqlLimitValidity(SqlLimit* limit)
{
    if (limit == NULL) {
        return false;
    }

    if (!limit->isValid) {
        return true;
    }

    if (RecoveryInProgress() && limit->xmin < g_instance.sqlLimit_cxt.processedXmin) {
        limit->isValid = false;
        return true;
    }

    if ((limit->timeWindow.endTime < GetCurrentTimestamp() && limit->timeWindow.endTime > 0) ||
        (limit->timeWindow.startTime != 0 && limit->timeWindow.endTime != 0 &&
            limit->timeWindow.startTime > limit->timeWindow.endTime)) {
        limit->isValid = false;
        return true;
    }

    return false;
}

bool IsKeywordsLimit(SqlType sqlType)
{
    return sqlType == SQL_TYPE_SELECT || sqlType == SQL_TYPE_INSERT ||
           sqlType == SQL_TYPE_UPDATE || sqlType == SQL_TYPE_DELETE;
}

void InvalidateSqlLimit(int64 limitId)
{
    if (RecoveryInProgress()) {
        return;
    }

    Datum values[Natts_gs_sql_limit] = {};
    bool nulls[Natts_gs_sql_limit] = {};
    bool replaces[Natts_gs_sql_limit] = {};

    values[Anum_gs_sql_limit_is_valid - 1] = BoolGetDatum(false);
    nulls[Anum_gs_sql_limit_is_valid - 1] = false;
    replaces[Anum_gs_sql_limit_is_valid - 1] = true;

    ScanKeyData key;
    ScanKeyInit(&key, (AttrNumber)Anum_gs_sql_limit_limit_id,
        BTEqualStrategyNumber, F_INT8EQ, UInt64GetDatum(limitId));

    Relation rel = heap_open(GsSqlLimitRelationId, RowExclusiveLock);
    SysScanDesc scan = systable_beginscan(rel, GsSqlLimitIdIndex, true, NULL, 1, &key);
    HeapTuple oldTuple = systable_getnext(scan);
    if (HeapTupleIsValid(oldTuple)) {
        HeapTuple newTuple = heap_modify_tuple(oldTuple, RelationGetDescr(rel), values, nulls, replaces);
        simple_heap_update(rel, &newTuple->t_self, newTuple);
        CatalogUpdateIndexes(rel, newTuple);
        heap_freetuple(newTuple);
    }

    systable_endscan(scan);
    heap_close(rel, RowExclusiveLock);
}

void RemoveInvalidSqlLimitCache()
{
    List* invalidLimits = NIL;
    LWLockAcquire(SqlLimitLock, LW_SHARED);
    HASH_SEQ_STATUS scan;
    SqlLimitHashEntry *entry = NULL;

    hash_seq_init(&scan, g_instance.sqlLimit_cxt.limitRegistry);
    while ((entry = (SqlLimitHashEntry *)hash_seq_search(&scan))) {
        if (entry == NULL || entry->sqlType == SQL_TYPE_OTHER) {
            ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("invalid limit type")));
            continue;
        }

        SqlLimit* limit = entry->limit;
        if (UpdateSqlLimitValidity(limit)) {
            invalidLimits = lappend(invalidLimits, (void*)limit->limitId);
            InvalidateSqlLimit(limit->limitId);
        }
    }

    LWLockRelease(SqlLimitLock);

    foreach_cell(cell, invalidLimits) {
        uint64 limitId = (uint64)lfirst(cell);
        DeleteSqlLimitCache(limitId);
    }

    list_free(invalidLimits);
}

void CleanSqlLimitCache()
{
    LWLockAcquire(SqlLimitLock, LW_EXCLUSIVE);

    if (!g_instance.sqlLimit_cxt.cacheInited) {
        LWLockRelease(SqlLimitLock);
        return;
    }

    for (int i = 0; i < MAX_SQL_LIMIT_TYPE; i++) {
        while (!dlist_is_empty(&g_instance.sqlLimit_cxt.keywordsLimits[i])) {
            dlist_node* node = dlist_pop_head_node(&g_instance.sqlLimit_cxt.keywordsLimits[i]);
            KeywordsLimitNode* limitNode = (KeywordsLimitNode*)dlist_container(KeywordsLimitNode, node, node);
            if (limitNode != NULL && limitNode->limit != NULL) {
                SqlLimitDestroy(limitNode->limit);
            }
            pfree_ext(limitNode);
        }
        dlist_init(&g_instance.sqlLimit_cxt.keywordsLimits[i]);
    }

    hash_destroy(g_instance.sqlLimit_cxt.uniqueSqlIdLimits);
    hash_destroy(g_instance.sqlLimit_cxt.limitRegistry);
    g_instance.sqlLimit_cxt.cacheInited = false;
    g_instance.sqlLimit_cxt.entryCount = 0;
    LWLockRelease(SqlLimitLock);
}

List* ParseNameArrayToOidList(Datum arrayDatum, Oid (*getOidFunc)(const char*, bool), bool missingOk)
{
    List* oidList = NIL;
    ArrayType* inputArray = DatumGetArrayTypeP(arrayDatum);
    ArrayIterator iterator = array_create_iterator(inputArray, 0);

    Datum elementValue;
    bool isNull;
    while (array_iterate(iterator, &elementValue, &isNull)) {
        if (isNull) {
            continue;
        }

        char* name = NameStr(*(DatumGetName(elementValue)));
        Oid oid = getOidFunc(name, missingOk);
        oidList = lappend_oid(oidList, oid);
    }

    array_free_iterator(iterator);
    return oidList;
}

static List* ParseOptionValues(Datum values)
{
    List* valuesList = NIL;
    bool isnull;
    Datum option;
    ArrayType *arrOption = DatumGetArrayTypeP(values);
    ArrayIterator it = array_create_iterator(arrOption, 0);
    while (array_iterate(it, &option, &isnull)) {
        if (isnull) {
            continue;
        }
        valuesList = lappend(valuesList, TextDatumGetCString(option));
    }

    array_free_iterator(it);
    return valuesList;
}

static void SetScopeFields(SqlLimit* limit, Datum* values, bool* nulls, bool missingOk)
{
    if (!nulls[Anum_gs_sql_limit_databases - 1]) {
        List* databases = ParseNameArrayToOidList(
            values[Anum_gs_sql_limit_databases - 1],
            get_database_oid, missingOk);
        SqlLimitSetDatabases(limit, databases);
    }

    if (!nulls[Anum_gs_sql_limit_users - 1]) {
        List* users = ParseNameArrayToOidList(
            values[Anum_gs_sql_limit_users - 1],
            get_role_oid, missingOk);
        SqlLimitSetUsers(limit, users);
    }
}

static void SetControlFields(SqlLimit* limit, Datum* values, bool* nulls)
{
    if (!nulls[Anum_gs_sql_limit_max_concurrency - 1]) {
        limit->maxConcurrency = DatumGetUInt64(values[Anum_gs_sql_limit_max_concurrency - 1]);
    }

    if (!nulls[Anum_gs_sql_limit_work_node - 1]) {
        limit->workNode = DatumGetUInt8(values[Anum_gs_sql_limit_work_node - 1]);
    }

    if (!nulls[Anum_gs_sql_limit_is_valid - 1]) {
        limit->isValid = DatumGetBool(values[Anum_gs_sql_limit_is_valid - 1]);
    }
}

static void SetTimeWindow(SqlLimit* limit, Datum* values, bool* nulls)
{
    TimestampTz startTime = 0;
    TimestampTz endTime = 0;

    if (!nulls[Anum_gs_sql_limit_start_time - 1]) {
        startTime = DatumGetTimestampTz(values[Anum_gs_sql_limit_start_time - 1]);
    }

    if (!nulls[Anum_gs_sql_limit_end_time - 1]) {
        endTime = DatumGetTimestampTz(values[Anum_gs_sql_limit_end_time - 1]);
    }

    TimeWindowSet(&limit->timeWindow, startTime, endTime);
}

static void PopulateBaseSqlLimitFields(SqlLimit* limit, Datum* values, bool* nulls, bool missingOk)
{
    limit->limitId = DatumGetUInt64(values[Anum_gs_sql_limit_limit_id - 1]);

    SetScopeFields(limit, values, nulls, missingOk);

    SetControlFields(limit, values, nulls);

    SetTimeWindow(limit, values, nulls);
    return;
}

SqlType GetSqlLimitType(const char* sqlType)
{
    if (strcasecmp(sqlType, SQLID_TYPE) == 0) {
        return SQL_TYPE_UNIQUE_SQLID;
    } else if (strcasecmp(sqlType, SELECT_TYPE) == 0) {
        return SQL_TYPE_SELECT;
    } else if (strcasecmp(sqlType, INSERT_TYPE) == 0) {
        return SQL_TYPE_INSERT;
    } else if (strcasecmp(sqlType, UPDATE_TYPE) == 0) {
        return SQL_TYPE_UPDATE;
    } else if (strcasecmp(sqlType, DELETE_TYPE) == 0) {
        return SQL_TYPE_DELETE;
    }
    return SQL_TYPE_OTHER;
}

static void PopulateKeywordLimitFields(SqlLimit* limit, Datum* values, bool* nulls)
{
    if (nulls[Anum_gs_sql_limit_limit_opt - 1]) {
        return;
    }

    char* sqlType = text_to_cstring(DatumGetTextP(values[Anum_gs_sql_limit_limit_type - 1]));
    SqlType limitTypeEnum = GetSqlLimitType(sqlType);
    if (limitTypeEnum == SQL_TYPE_OTHER) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid limit type: %s", sqlType)));
        pfree_ext(sqlType);
        return;
    }
    limit->sqlType = limitTypeEnum;
    List* keywords = ParseOptionValues(values[Anum_gs_sql_limit_limit_opt - 1]);
    limit->typeData.keyword.keywords = keywords;
    pfree_ext(sqlType);
}

static void PopulateUniqueSqlIdLimitFields(SqlLimit* limit, uint64* uniqueSqlid)
{
    limit->sqlType = SQL_TYPE_UNIQUE_SQLID;
    limit->typeData.uniqueSql.uniqueSqlId = *uniqueSqlid;
}

static void ApplySqlLimitChanges(SqlLimit* limit, Datum* values, bool* nulls, uint64* uniqueSqlid, bool missingOk)
{
    if (limit == NULL || limit->sqlType == SQL_TYPE_OTHER) {
        return;
    }

    SqlType sqlType = limit->sqlType;
    SqlLimitClear(limit);
    PopulateBaseSqlLimitFields(limit, values, nulls, missingOk);

    switch (sqlType) {
        case SQL_TYPE_UNIQUE_SQLID:
            PopulateUniqueSqlIdLimitFields(limit, uniqueSqlid);
            break;
        case SQL_TYPE_SELECT:
        case SQL_TYPE_INSERT:
        case SQL_TYPE_UPDATE:
        case SQL_TYPE_DELETE:
            PopulateKeywordLimitFields(limit, values, nulls);
            break;
        case SQL_TYPE_OTHER:
            return;
        default:
            break;
    }
}

SqlLimit* SearchSqlLimitCache(uint64 limitId)
{
    if (g_instance.sqlLimit_cxt.limitRegistry == NULL) {
        ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("sql limit cache is not initialized.")));
        return NULL;
    }

    bool found = false;
    SqlLimitHashEntry* registryEntry = (SqlLimitHashEntry*)hash_search(g_instance.sqlLimit_cxt.limitRegistry,
        (void*)&limitId, HASH_FIND, &found);
    if (!found) {
        ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("limit_id %lu not found in sql limit cache.", limitId)));
        return NULL;
    }

    return registryEntry->limit;
}

static SqlLimit* CreateKeywordsLimitCache(Datum* values, bool* nulls, bool missingOk)
{
    int errLevel = missingOk ? WARNING : ERROR;
    uint64 limitId = DatumGetUInt64(values[Anum_gs_sql_limit_limit_id - 1]);
    bool found = false;
    SqlLimitHashEntry* entry = (SqlLimitHashEntry*)hash_search(
        g_instance.sqlLimit_cxt.limitRegistry,  &limitId, HASH_ENTER, &found);

    if (found) {
        ereport(errLevel, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Cannot create keywords limit: limit_id %lu already exists.", limitId)));
        return NULL;
    }

    SqlLimit* limit = SqlLimitCreate(limitId, SQL_TYPE_OTHER);
    PopulateBaseSqlLimitFields(limit, values, nulls, missingOk);
    PopulateKeywordLimitFields(limit, values, nulls);

    SqlType sqlType = limit->sqlType;
    KeywordsLimitNode *newNode = (KeywordsLimitNode *)palloc0(sizeof(KeywordsLimitNode));
    newNode->limit = limit;

    uint64 newMaxConcurrency = limit->maxConcurrency;
    // insert at head, we assume the new limit has the highest priority
    dlist_push_head(&g_instance.sqlLimit_cxt.keywordsLimits[sqlType], &newNode->node);

    entry->sqlType = sqlType;
    entry->limit = limit;
    entry->keywordsNode = &newNode->node;
    ereport(DEBUG1, (errmsg("create keywords limit cache: id=%lu, type=%d, maxConcurrency=%lu",
        limitId, sqlType, newMaxConcurrency)));

    return limit;
}

static SqlLimit* CreateUniqueSqlidLimitCache(Datum* values, bool* nulls, uint64* uniqueSqlid, bool missingOk)
{
    int errLevel = missingOk ? WARNING : ERROR;
    bool found = false;
    UniqueSqlIdHashEntry* entry = (UniqueSqlIdHashEntry*)hash_search(g_instance.sqlLimit_cxt.uniqueSqlIdLimits,
        (void*)uniqueSqlid, HASH_ENTER, &found);
    if (found) {
        ereport(errLevel, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Cannot create limit: unique_sql_id %lu already exists.", *uniqueSqlid)));
        return NULL;
    }
    uint64 limitId = DatumGetUInt64(values[Anum_gs_sql_limit_limit_id - 1]);
    SqlLimitHashEntry* registryEntry = (SqlLimitHashEntry*)hash_search(g_instance.sqlLimit_cxt.limitRegistry,
        (void*)&limitId, HASH_ENTER, &found);
    if (found) {
        ereport(errLevel, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Cannot create limit: limit_id %lu already exists.", limitId)));
        return NULL;
    }

    SqlLimit* limit = SqlLimitCreate(limitId, SQL_TYPE_UNIQUE_SQLID);
    entry->limit = limit;
    PopulateBaseSqlLimitFields(limit, values, nulls, missingOk);
    PopulateUniqueSqlIdLimitFields(limit, uniqueSqlid);
    registryEntry->limit = limit;
    registryEntry->sqlType = SQL_TYPE_UNIQUE_SQLID;

    return limit;
}

bool ValidateAndExtractOption(SqlType sqlType, Datum* values, uint64* uniqueSqlid, bool missingOk)
{
    if (sqlType != SQL_TYPE_UNIQUE_SQLID) {
        return true;
    }

    int errLevel = missingOk ? WARNING : ERROR;
    List* valuesList = ParseOptionValues(*values);
    if (valuesList == NIL) {
        ereport(errLevel, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("unique SQL ID parameter is empty")));
        return false;
    }

    // Extract the first value from the list as unique SQL ID
    char* optionValue = (char*)linitial(valuesList);
    int64 sqlIdValue = 0;
    (void)scanint8(optionValue, true, &sqlIdValue);

    list_free_deep(valuesList);

    if (sqlIdValue <= 0) {
        ereport(errLevel, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("unique SQL ID must be a positive integer, received: %ld", sqlIdValue)));
        return false;
    }

    *uniqueSqlid = (uint64)sqlIdValue;
    return true;
}

static bool NeedUpdateSqlLimitCache(HeapTuple tuple)
{
    TransactionId tupleXmin = HeapTupleGetRawXmin(tuple);
    /*
     * check if need update:
     * 1. if tuple_xmin is less than or equal to last processed xmin, it means it has been processed
     * 2. if tuple_xmin is greater than current_xmin, it means the transaction is not committed
     */
    if (TransactionIdPrecedesOrEquals(tupleXmin, g_instance.sqlLimit_cxt.processedXmin)) {
        return false;
    }
    return true;
}

static bool ProcessLimitOptions(Datum* values, bool* nulls, SqlType limitTypeEnum, uint64* uniqueSqlid)
{
    Datum* datum = nulls[Anum_gs_sql_limit_limit_opt - 1] ?
        NULL : &values[Anum_gs_sql_limit_limit_opt - 1];

    bool res = ValidateAndExtractOption(limitTypeEnum, datum, uniqueSqlid, true);
    if (!res) {
        return false;
    }
    return true;
}

static void ApplyLimitChanges(SqlLimit* limit, Datum* values, bool* nulls, uint64* uniqueSqlid, bool missingOk)
{
    if (limit->sqlType == SQL_TYPE_UNIQUE_SQLID) {
        ApplySqlLimitChanges(limit, values, nulls, uniqueSqlid, missingOk);
    } else if (IsKeywordsLimit(limit->sqlType)) {
        ApplySqlLimitChanges(limit, values, nulls, NULL, missingOk);
    } else {
        ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid limit type, please check the input")));
    }
}

void DoUpdateSqlLimitCacheStandby(HeapTuple tuple, TupleDesc tupleDesc, TransactionId currentXmin)
{
    Datum values[Natts_gs_sql_limit] = {};
    bool nulls[Natts_gs_sql_limit] = {};
    heap_deform_tuple(tuple, tupleDesc, values, nulls);

    char* sqlType = text_to_cstring(DatumGetTextP(values[Anum_gs_sql_limit_limit_type - 1]));
    SqlType limitTypeEnum = GetSqlLimitType(sqlType);
    if (limitTypeEnum == SQL_TYPE_OTHER) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid limit type: %s", sqlType)));
        pfree_ext(sqlType);
        return;
    }
    pfree_ext(sqlType);

    uint64 uniqueSqlid = 0;
    if (!ProcessLimitOptions(values, nulls, limitTypeEnum, &uniqueSqlid)) {
        return;
    }

    uint64 limitId = DatumGetUInt64(values[Anum_gs_sql_limit_limit_id - 1]);

    LWLockAcquire(SqlLimitLock, LW_EXCLUSIVE);
    SqlLimit* limit = SearchSqlLimitCache(limitId);
    if (limit != NULL) {
        limit->xmin = currentXmin;
    }

    if (!NeedUpdateSqlLimitCache(tuple)) {
        LWLockRelease(SqlLimitLock);
        return;
    }

    MemoryContext oldCxt = MemoryContextSwitchTo(g_instance.sqlLimit_cxt.gSqlLimitCxt);
    if (limit == NULL) {
        if (limitTypeEnum == SQL_TYPE_UNIQUE_SQLID) {
            limit = CreateUniqueSqlidLimitCache(values, nulls, &uniqueSqlid, true);
        } else if (IsKeywordsLimit(limitTypeEnum)) {
            limit = CreateKeywordsLimitCache(values, nulls, true);
        } else {
            ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid limit type, please check the input")));
        }

        if (limit != NULL) {
            limit->xmin = currentXmin;
            g_instance.sqlLimit_cxt.entryCount++;
        }
    } else {
        ApplyLimitChanges(limit, values, nulls, &uniqueSqlid, true);
    }
    MemoryContextSwitchTo(oldCxt);
    LWLockRelease(SqlLimitLock);
}

void UpdateSqlLimitCache()
{
    if (!RecoveryInProgress()) {
        return;
    }

    TransactionId currentXmin = GetLatestSnapshot()->xmin;
    Relation rel = heap_open(GsSqlLimitRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
    TupleDesc tupleDesc = RelationGetDescr(rel);
    HeapTuple tuple;
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        DoUpdateSqlLimitCacheStandby(tuple, tupleDesc, currentXmin);
    }

    g_instance.sqlLimit_cxt.processedXmin = currentXmin;

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);
}

static SqlLimit* CreateSqlLimitCache(Datum* values, bool* nulls, uint64* uniqueSqlid, bool missingOk)
{
    int errLevel = missingOk ? WARNING : ERROR;
    if (nulls[Anum_gs_sql_limit_limit_type - 1]) {
        ereport(errLevel, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("limit type is empty.")));
        return NULL;
    }

    char* sqlType = text_to_cstring(DatumGetTextP(values[Anum_gs_sql_limit_limit_type - 1]));
    SqlLimit* limit = NULL;
    SqlType limitTypeEnum = GetSqlLimitType(sqlType);
    if (limitTypeEnum == SQL_TYPE_OTHER) {
        ereport(errLevel, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid limit type: %s", sqlType)));
        pfree_ext(sqlType);
        return NULL;
    }

    if (limitTypeEnum == SQL_TYPE_UNIQUE_SQLID) {
        limit = CreateUniqueSqlidLimitCache(values, nulls, uniqueSqlid, missingOk);
    } else if (IsKeywordsLimit(limitTypeEnum)) {
        limit = CreateKeywordsLimitCache(values, nulls, missingOk);
    } else {
        ereport(errLevel, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("invalid limit type: %s", sqlType)));
        pfree_ext(sqlType);
        return NULL;
    }

    pfree_ext(sqlType);
    if (limit != NULL) {
        g_instance.sqlLimit_cxt.entryCount++;
    }
    return limit;
}


bool UpdateSqlLimit(Datum* values, bool* nulls, uint64* uniqueSqlid)
{
    LWLockAcquire(SqlLimitLock, LW_EXCLUSIVE);
    if (!g_instance.sqlLimit_cxt.cacheInited) {
        LWLockRelease(SqlLimitLock);
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("sql limit cache is not initialized")));
        return false;
    }

    MemoryContext oldCxt = MemoryContextSwitchTo(g_instance.sqlLimit_cxt.gSqlLimitCxt);

    uint64 limitId = DatumGetUInt64(values[Anum_gs_sql_limit_limit_id - 1]);
    SqlLimit* limit = SearchSqlLimitCache(limitId);
    if (limit == NULL) {
        (void)CreateSqlLimitCache(values, nulls, uniqueSqlid, false);
        MemoryContextSwitchTo(oldCxt);
        LWLockRelease(SqlLimitLock);
        return true;
    }

    ApplySqlLimitChanges(limit, values, nulls, uniqueSqlid, false);

    MemoryContextSwitchTo(oldCxt);
    LWLockRelease(SqlLimitLock);
    return true;
}

void CreateSqlLimit(Datum* values, bool* nulls, uint64* uniqueSqlid)
{
    LWLockAcquire(SqlLimitLock, LW_EXCLUSIVE);
    if (!g_instance.sqlLimit_cxt.cacheInited) {
        LWLockRelease(SqlLimitLock);
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("sql limit cache is not initialized")));
    }

    MemoryContext oldCxt = MemoryContextSwitchTo(g_instance.sqlLimit_cxt.gSqlLimitCxt);
    (void)CreateSqlLimitCache(values, nulls, uniqueSqlid, false);
    MemoryContextSwitchTo(oldCxt);
    LWLockRelease(SqlLimitLock);
}

static void ConstructSqlLimits()
{
    Datum* values = (Datum*)palloc0((Natts_gs_sql_limit) * sizeof(Datum));
    bool* nulls = (bool*)palloc0((Natts_gs_sql_limit) * sizeof(bool));
    uint64 maxLimitId = 0;
    Relation rel = heap_open(GsSqlLimitRelationId, AccessShareLock);
    /* sequence scan */
    SysScanDesc scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
    TupleDesc tupleDesc = RelationGetDescr(rel);
    HeapTuple tuple;
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        heap_deform_tuple(tuple, tupleDesc, values, nulls);
        if (nulls[Anum_gs_sql_limit_limit_type - 1]) {
            continue;
        }
        char* sqlType = text_to_cstring(DatumGetTextP(values[Anum_gs_sql_limit_limit_type - 1]));
        SqlType limitTypeEnum = GetSqlLimitType(sqlType);
        if (limitTypeEnum == SQL_TYPE_OTHER) {
            ereport(WARNING, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid limit type: %s", sqlType)));
            pfree_ext(sqlType);
            continue;
        }
        pfree_ext(sqlType);
        Datum* datum = nulls[Anum_gs_sql_limit_limit_opt - 1] ?
            NULL : &values[Anum_gs_sql_limit_limit_opt - 1];

        uint64 uniqueSqlid = 0;
        bool res = ValidateAndExtractOption(limitTypeEnum, datum, &uniqueSqlid, true);
        if (!res) {
            continue;
        }

        SqlLimit* limit = CreateSqlLimitCache(values, nulls, &uniqueSqlid, true);
        if (limit == NULL) {
            continue;
        }

        maxLimitId = Max(maxLimitId, (uint64)limit->limitId);
    }

    g_instance.sqlLimit_cxt.entryIdSequence = maxLimitId + 1;

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);
    pfree_ext(values);
    pfree_ext(nulls);
}

SqlLimit* MatchSqlidLimit()
{
    bool found = false;
    uint64 uniqueSqlId = u_sess->unique_sql_cxt.unique_sql_id;
    UniqueSqlIdHashEntry* entry = (UniqueSqlIdHashEntry*)hash_search(g_instance.sqlLimit_cxt.uniqueSqlIdLimits,
        (void*)&uniqueSqlId, HASH_FIND, &found);
    if (!found) {
        return NULL;
    }

    SqlLimit* limit = entry->limit;
    if (UpdateSqlLimitValidity(limit)) {
        return NULL;
    }

    if (SqlLimitIsHit(limit, NULL, uniqueSqlId)) {
        return limit;
    }

    return NULL;
}

SqlLimit* MatchKeywordsLimit(const char* commandTag, const char* queryString)
{
    if (commandTag == NULL || strlen(commandTag) == 0 || queryString == NULL || strlen(queryString) == 0) {
        return NULL;
    }

    SqlType sqlType = GetSqlLimitType(commandTag);
    if (sqlType == SQL_TYPE_OTHER) {
        return NULL;
    }
    dlist_iter iter;
    dlist_foreach(iter, &g_instance.sqlLimit_cxt.keywordsLimits[sqlType]) {
        KeywordsLimitNode* keywordsNode = (KeywordsLimitNode*)dlist_container(KeywordsLimitNode, node, iter.cur);
        SqlLimit* limit = (SqlLimit*)(keywordsNode->limit);
        if (UpdateSqlLimitValidity(limit)) {
            continue;
        }
        if (SqlLimitIsHit(limit, queryString, 0)) {
            return limit;
        }
    }
    return NULL;
}

static void RemoveUniqueSqlIdLimit(SqlLimit* limit)
{
    uint64 uniqueSqlId = limit->typeData.uniqueSql.uniqueSqlId;
    (void)hash_search(g_instance.sqlLimit_cxt.uniqueSqlIdLimits, (void*)&uniqueSqlId, HASH_REMOVE, NULL);
}

static void RemoveKeywordsLimit(SqlLimit* limit)
{
    dlist_iter iter;
    dlist_foreach(iter, &g_instance.sqlLimit_cxt.keywordsLimits[limit->sqlType]) {
        KeywordsLimitNode* keywordsNode = (KeywordsLimitNode*)dlist_container(KeywordsLimitNode, node, iter.cur);
        SqlLimit* sqlLimit = (SqlLimit*)(keywordsNode->limit);
        if (sqlLimit != NULL && limit->limitId == sqlLimit->limitId) {
            dlist_delete(iter.cur);
            pfree_ext(keywordsNode);
            break;
        }
    }
}

bool DeleteSqlLimitCache(uint64 limitId)
{
    LWLockAcquire(SqlLimitLock, LW_EXCLUSIVE);
    if (!g_instance.sqlLimit_cxt.cacheInited) {
        LWLockRelease(SqlLimitLock);
        ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("sql limit cache is not initialized")));
        return false;
    }

    MemoryContext oldCxt = MemoryContextSwitchTo(g_instance.sqlLimit_cxt.gSqlLimitCxt);
    SqlLimit* limit = SearchSqlLimitCache(limitId);
    if (limit != NULL) {
        if (limit->sqlType == SQL_TYPE_UNIQUE_SQLID) {
            RemoveUniqueSqlIdLimit(limit);
        } else if (IsKeywordsLimit(limit->sqlType)) {
            RemoveKeywordsLimit(limit);
        }
        (void)hash_search(g_instance.sqlLimit_cxt.limitRegistry, (void*)&limitId, HASH_REMOVE, NULL);
        SqlLimitDestroy(limit);
        g_instance.sqlLimit_cxt.entryCount--;
    }
    MemoryContextSwitchTo(oldCxt);
    LWLockRelease(SqlLimitLock);
    return true;
}

static void RecordMatchedLimits(SqlLimit* sqlidLimit, SqlLimit* keywordsLimit)
{
    MemoryContext oldCxt = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));

    List* limits = NIL;
    if (sqlidLimit != NULL) {
        limits = lappend(limits, (void*)(sqlidLimit->limitId));
    }

    if (keywordsLimit != NULL) {
        limits = lappend(limits, (void*)(keywordsLimit->limitId));
    }

    u_sess->sqlLimit_ctx.limitSqls = limits;
    MemoryContextSwitchTo(oldCxt);
}

static bool ShouldRejectQuery(SqlLimit* limit)
{
    if (limit == NULL) {
        return false;
    }

    LimitStatsUpdateHit(&limit->stats);
    volatile uint64 currConcurrency = limit->stats.currConcurrency;
    volatile uint64 maxConcurrency = limit->maxConcurrency;

    return (currConcurrency >= maxConcurrency);
}

static void RejectQuery(SqlLimit* limit)
{
    LimitStatsUpdateReject(&limit->stats);
    LWLockRelease(SqlLimitLock);
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("The request is over max concurrency of sql limit, "
                "the request will be rejected. limitId: %ld", limit->limitId),
            errdetail("current concurrency: %lu, max concurrency: %lu",
                limit->stats.currConcurrency + 1, limit->maxConcurrency)));
}

static void ProcessAcceptedLimit(SqlLimit* limit)
{
    if (limit != NULL) {
        LimitStatsUpdateConcurrency(&limit->stats, true);
    }
}

void LimitCurrentQuery(const char* commandTag, const char* queryString)
{
    if (!u_sess->attr.attr_common.enable_sql_limit || IsAbortedTransactionBlockState() || superuser()) {
        return;
    }

    if (commandTag == NULL || strlen(commandTag) == 0 || queryString == NULL || strlen(queryString) == 0) {
        return;
    }

    LWLockAcquire(SqlLimitLock, LW_SHARED);

    if (!g_instance.sqlLimit_cxt.cacheInited) {
        LWLockRelease(SqlLimitLock);
        return;
    }

    SqlLimit* sqlidLimit = MatchSqlidLimit();
    SqlLimit* keywordsLimit = MatchKeywordsLimit(commandTag, queryString);

    if (sqlidLimit != NULL && ShouldRejectQuery(sqlidLimit)) {
        RejectQuery(sqlidLimit);
    }

    // keywords limit may be also matched, so we need to check it after sqlid limit
    if (keywordsLimit != NULL && ShouldRejectQuery(keywordsLimit)) {
        RejectQuery(keywordsLimit);
    }

    ProcessAcceptedLimit(sqlidLimit);
    ProcessAcceptedLimit(keywordsLimit);

    RecordMatchedLimits(sqlidLimit, keywordsLimit);
    LWLockRelease(SqlLimitLock);
}

void UnlimitCurrentQuery()
{
    if (!g_instance.sqlLimit_cxt.cacheInited) {
        if (u_sess->sqlLimit_ctx.limitSqls != NIL) {
            list_free_ext(u_sess->sqlLimit_ctx.limitSqls);
        }
        u_sess->sqlLimit_ctx.limitSqls = NIL;
        return;
    }

    LWLockAcquire(SqlLimitLock, LW_SHARED);
    if (u_sess->sqlLimit_ctx.limitSqls != NIL) {
        foreach_cell(cell, u_sess->sqlLimit_ctx.limitSqls) {
            uint64 limitId = (uint64)lfirst(cell);
            SqlLimit* limit = SearchSqlLimitCache(limitId);
            if (limit != NULL) {
                LimitStatsUpdateConcurrency(&limit->stats, false);
            }
        }
    }
    list_free_ext(u_sess->sqlLimit_ctx.limitSqls);
    u_sess->sqlLimit_ctx.limitSqls = NIL;
    LWLockRelease(SqlLimitLock);
}

static void CreateSqlLimitMemoryContext()
{
    g_instance.sqlLimit_cxt.gSqlLimitCxt = AllocSetContextCreate(g_instance.instance_context,
        "sql limit cache context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);
}

static void InitializeUniqueSqlIdLimitsHash()
{
    HASHCTL hashCtl;
    errno_t rc = memset_s(&hashCtl, sizeof(hashCtl), 0, sizeof(hashCtl));
    securec_check(rc, "\0", "\0");

    hashCtl.keysize = sizeof(uint64);
    hashCtl.entrysize = sizeof(UniqueSqlIdHashEntry);
    hashCtl.hash = tag_hash;
    hashCtl.hcxt = g_instance.sqlLimit_cxt.gSqlLimitCxt;

    g_instance.sqlLimit_cxt.uniqueSqlIdLimits = hash_create(
        "unique sql id limit hash",
        SQL_LIMIT_INIT_HASH_SIZE,
        &hashCtl,
        HASH_ELEM | HASH_FUNCTION | HASH_SHRCTX);
}

static void InitializeLimitRegistryHash()
{
    HASHCTL hashCtl;
    errno_t rc = memset_s(&hashCtl, sizeof(hashCtl), 0, sizeof(hashCtl));
    securec_check(rc, "\0", "\0");

    hashCtl.keysize = sizeof(uint64);
    hashCtl.entrysize = sizeof(SqlLimitHashEntry);
    hashCtl.hash = tag_hash;
    hashCtl.hcxt = g_instance.sqlLimit_cxt.gSqlLimitCxt;

    g_instance.sqlLimit_cxt.limitRegistry = hash_create(
        "total sql limit hash",
        SQL_LIMIT_INIT_HASH_SIZE,
        &hashCtl,
        HASH_ELEM | HASH_FUNCTION | HASH_SHRCTX);
}

static void InitializeLimitDataStructures()
{
    // Initialize unique SQL ID limits hash table
    InitializeUniqueSqlIdLimitsHash();

    // Initialize SQL limit registry hash table
    InitializeLimitRegistryHash();

    // Initialize keywords limit lists
    for (int i = 0; i < MAX_SQL_LIMIT_TYPE; i++) {
        dlist_init(&g_instance.sqlLimit_cxt.keywordsLimits[i]);
    }
}

void InitSqlLimitCache()
{
    if (g_instance.sqlLimit_cxt.cacheInited) {
        return;
    }

    if (g_instance.sqlLimit_cxt.gSqlLimitCxt != NULL) {
        MemoryContextDelete(g_instance.sqlLimit_cxt.gSqlLimitCxt);
    }

    CreateSqlLimitMemoryContext();
    MemoryContext oldCxt = MemoryContextSwitchTo(g_instance.sqlLimit_cxt.gSqlLimitCxt);
    InitializeLimitDataStructures();
    ConstructSqlLimits();
    MemoryContextSwitchTo(oldCxt);
    g_instance.sqlLimit_cxt.cacheInited = true;
}