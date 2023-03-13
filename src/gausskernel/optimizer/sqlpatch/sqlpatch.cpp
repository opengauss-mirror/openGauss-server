/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 *
 * sqlpatch.cpp
 *		sqlpatch support.
 *
 *
 * IDENTIFICATION
 *      src/gausskernel/optimizer/sqlpatch/sqlpatch.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "access/genam.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "auditfuncs.h"
#include "catalog/indexing.h"
#include "catalog/pg_authid.h"
#include "catalog/gs_sql_patch.h"
#include "instruments/unique_query.h"
#include "optimizer/sqlpatch.h"
#include "miscadmin.h"
#include "funcapi.h"
#include "parser/parse_hint.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"

typedef enum {
    USE_NAME,
    USE_UNIQUE_SQL_ID,
    USE_INVALID
} PatchScanKey;

const int MAX_TEXT_LENGTH = 1024;

static HeapTuple GetPatchTuple(Datum dat, Relation rel, PatchScanKey key);
static HintState* GetEnabledPatchHintBySqlId(uint64 sql_id);
static void ModifySQLPatchWorker(PG_FUNCTION_ARGS, bool enable);
static void PrivCheck();
static char* ProcessHintString(const char* hintStr);
static bool AddPatchToDml(Query* query);
static void AddPatchToExplainQuery(Query* query);
static void CheckAbortByPatch(uint64 queryId);
static void RemovePatchUnsupportHint(HintState* hs);
static bool QueryIsPatched(const Query* query);

Datum create_sql_patch_by_id_hint(PG_FUNCTION_ARGS)
{
    /* args: [patch_name, unique_sql_id, hint_string, description, enable] */
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(4)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Cannot create sql patch with NULL inputs")));
    }

    PreventCommandIfReadOnly("Create SQL Patch");

    PrivCheck();

    /* check parameter length */
    char* hintStr = text_to_cstring(PG_GETARG_TEXT_PP(2));

    int hintLen = (int)strlen(hintStr);
    int descLen = 0;

    if (!PG_ARGISNULL(3)) {
        char* descStr = text_to_cstring(PG_GETARG_TEXT_PP(3));
        descLen = strlen(descStr);
        pfree(descStr);
    }

    if (hintLen > MAX_TEXT_LENGTH || descLen > MAX_TEXT_LENGTH) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("hint or description too wide for a sql patch")));
    }

    Name patchName = PG_GETARG_NAME(0);
    int64 uniqueSqlId = DatumGetInt64(PG_GETARG_DATUM(1));
    if (uniqueSqlId == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Cannot accept sql patches with 0 unique sql id")));
    }

    char* hintNodeTree = ProcessHintString(hintStr);

    if (hintNodeTree == NULL) {
        PG_RETURN_BOOL(false);
    }

    /* open gs_sql_patch to insert a row */
    Relation rel_gs_sql_patch = heap_open(GsSqlPatchRelationId, RowExclusiveLock);
    /* need to check the uniqueness of unique sql id for now */
    if (HeapTupleIsValid(GetPatchTuple(PG_GETARG_DATUM(1), rel_gs_sql_patch, USE_UNIQUE_SQL_ID))) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Cannot accept sql patches with duplicate unique sql id")));
    }

    bool nulls[Natts_gs_sql_patch];
    Datum values[Natts_gs_sql_patch];
    errno_t rc = EOK;
    rc = memset_s(nulls, sizeof(bool) * Natts_gs_sql_patch, 0, sizeof(bool) * Natts_gs_sql_patch);
    securec_check(rc, "", "");
    rc = memset_s(values, sizeof(Datum) * Natts_gs_sql_patch, 0, sizeof(Datum) * Natts_gs_sql_patch);
    securec_check(rc, "", "");
    HeapTuple tup = NULL;

    /* fill in a data row of sql patch */
    values[Anum_gs_sql_patch_patch_name - 1] = PG_GETARG_DATUM(0);
    values[Anum_gs_sql_patch_unique_sql_id - 1] = PG_GETARG_DATUM(1);
    values[Anum_gs_sql_patch_owner - 1] = ObjectIdGetDatum(GetUserId());
    values[Anum_gs_sql_patch_enable - 1] = PG_GETARG_DATUM(4);
    values[Anum_gs_sql_patch_status - 1] = CharGetDatum(STATUS_DEFAULT);
    values[Anum_gs_sql_patch_abort - 1] = BoolGetDatum(false);
    values[Anum_gs_sql_patch_hint_string - 1] = PG_GETARG_DATUM(2);
    values[Anum_gs_sql_patch_hint_node - 1] = CStringGetTextDatum(hintNodeTree);
    nulls[Anum_gs_sql_patch_original_query - 1] = true;
    nulls[Anum_gs_sql_patch_original_query_tree - 1] = true;
    nulls[Anum_gs_sql_patch_patched_query - 1] = true;
    nulls[Anum_gs_sql_patch_patched_query_tree - 1] = true;
    values[Anum_gs_sql_patch_description - 1] = PG_GETARG_DATUM(3);
    nulls[Anum_gs_sql_patch_description - 1] = PG_ARGISNULL(3);

    tup = heap_form_tuple(RelationGetDescr(rel_gs_sql_patch), values, nulls);

    (void)simple_heap_insert(rel_gs_sql_patch, tup);

    /* Update indexes */
    CatalogUpdateIndexes(rel_gs_sql_patch, tup);

    heap_close(rel_gs_sql_patch, RowExclusiveLock);

    heap_freetuple(tup);

    /* audit */
    if (t_thrd.postgres_cxt.debug_query_string == NULL) {
        /* construct command */
        StringInfoData str;
        initStringInfo(&str);
        uint64 sqlId = DatumGetUInt64(PG_GETARG_DATUM(1));
        char* hintstr = text_to_cstring(PG_GETARG_TEXT_PP(2));
        const char* enable = PG_GETARG_BOOL(4) ? "true" : "false";
        appendStringInfo(&str, "SELECT * FROM dbe_sql_util.create_sql_patch('%s', '%lu', '%s', '****', '%s')",
            patchName->data, sqlId, hintstr, enable);
        pgaudit_ddl_sql_patch(patchName->data, str.data);
        pfree_ext(str.data);
    } else {
        pgaudit_ddl_sql_patch(patchName->data, t_thrd.postgres_cxt.debug_query_string);
    }

    u_sess->opt_cxt.xact_modify_sql_patch = true;

    PG_RETURN_BOOL(true);
}

Datum create_abort_patch_by_id(PG_FUNCTION_ARGS)
{
    /* args: [patch_name, unique_sql_id, description, enable] */
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(3)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Cannot create sql patch with NULL inputs")));
    }

    PreventCommandIfReadOnly("Create abort SQL Patch");

    PrivCheck();

    /* check parameter length */
    int descLen = 0;
    if (!PG_ARGISNULL(2)) {
        char* descStr = text_to_cstring(PG_GETARG_TEXT_PP(2));
        descLen = strlen(descStr);
        pfree(descStr);
    }

    if (descLen > MAX_TEXT_LENGTH) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("description too wide for a sql patch")));
    }

    Name patchName = PG_GETARG_NAME(0);
    int64 uniqueSqlId = DatumGetInt64(PG_GETARG_DATUM(1));
    if (uniqueSqlId == 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Cannot accept sql patches with 0 unique sql id")));
    }

    /* open gs_sql_patch to insert a row */
    Relation rel_gs_sql_patch = heap_open(GsSqlPatchRelationId, RowExclusiveLock);
    /* need to check the uniqueness of unique sql id for now */
    if (HeapTupleIsValid(GetPatchTuple(PG_GETARG_DATUM(1), rel_gs_sql_patch, USE_UNIQUE_SQL_ID))) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Cannot accept sql patches with duplicate unique sql id")));
    }

    Datum values[Natts_gs_sql_patch];
    bool nulls[Natts_gs_sql_patch];
    errno_t rc = EOK;
    rc = memset_s(values, sizeof(Datum) * Natts_gs_sql_patch, 0, sizeof(Datum) * Natts_gs_sql_patch);
    securec_check(rc, "", "");
    rc = memset_s(nulls, sizeof(bool) * Natts_gs_sql_patch, 0, sizeof(bool) * Natts_gs_sql_patch);
    securec_check(rc, "", "");
    HeapTuple tup = NULL;

    /* fill in a data row of sql patch */
    values[Anum_gs_sql_patch_patch_name - 1] = PG_GETARG_DATUM(0);
    values[Anum_gs_sql_patch_unique_sql_id - 1] = PG_GETARG_DATUM(1);
    values[Anum_gs_sql_patch_owner - 1] = ObjectIdGetDatum(GetUserId());
    values[Anum_gs_sql_patch_enable - 1] = PG_GETARG_DATUM(3);
    values[Anum_gs_sql_patch_status - 1] = CharGetDatum(STATUS_DEFAULT);
    values[Anum_gs_sql_patch_abort - 1] = BoolGetDatum(true);
    nulls[Anum_gs_sql_patch_hint_string - 1] = true;
    nulls[Anum_gs_sql_patch_hint_node - 1] = true;
    nulls[Anum_gs_sql_patch_original_query - 1] = true;
    nulls[Anum_gs_sql_patch_original_query_tree - 1] = true;
    nulls[Anum_gs_sql_patch_patched_query - 1] = true;
    nulls[Anum_gs_sql_patch_patched_query_tree - 1] = true;
    values[Anum_gs_sql_patch_description - 1] = PG_GETARG_DATUM(2);
    nulls[Anum_gs_sql_patch_description - 1] = PG_ARGISNULL(2);

    tup = heap_form_tuple(RelationGetDescr(rel_gs_sql_patch), values, nulls);

    (void)simple_heap_insert(rel_gs_sql_patch, tup);

    /* Update indexes */
    CatalogUpdateIndexes(rel_gs_sql_patch, tup);

    heap_close(rel_gs_sql_patch, RowExclusiveLock);

    heap_freetuple(tup);

    /* audit */
    if (t_thrd.postgres_cxt.debug_query_string != NULL) {
        pgaudit_ddl_sql_patch(patchName->data, t_thrd.postgres_cxt.debug_query_string);
    } else {
        /* construct command */
        StringInfoData str;
        initStringInfo(&str);
        uint64 sqlId = DatumGetUInt64(PG_GETARG_DATUM(1));
        const char* enable = PG_GETARG_BOOL(3) ? "true" : "false";
        appendStringInfo(&str, "SELECT * FROM dbe_sql_util.create_abort_patch('%s', '%lu', '****', '%s')",
            patchName->data, sqlId, enable);
        pgaudit_ddl_sql_patch(patchName->data, str.data);
        pfree_ext(str.data);
    }

    u_sess->opt_cxt.xact_modify_sql_patch = true;
    PG_RETURN_BOOL(true);
}

Datum enable_sql_patch(PG_FUNCTION_ARGS)
{
    PreventCommandIfReadOnly("Enable SQL Patch");

    PrivCheck();

    Name patchName = PG_GETARG_NAME(0);

    ModifySQLPatchWorker(fcinfo, true);

    /* audit */
    if (t_thrd.postgres_cxt.debug_query_string != NULL) {
        pgaudit_ddl_sql_patch(patchName->data, t_thrd.postgres_cxt.debug_query_string);
    } else {
        /* construct command */
        StringInfoData str;
        initStringInfo(&str);
        appendStringInfo(&str, "SELECT * FROM dbe_sql_util.enable_sql_patch('%s')", patchName->data);
        pgaudit_ddl_sql_patch(patchName->data, str.data);
        pfree_ext(str.data);
    }

    u_sess->opt_cxt.xact_modify_sql_patch = true;
    PG_RETURN_BOOL(true);
}

Datum disable_sql_patch(PG_FUNCTION_ARGS)
{
    PreventCommandIfReadOnly("Disable SQL Patch");

    PrivCheck();

    Name patchName = PG_GETARG_NAME(0);

    ModifySQLPatchWorker(fcinfo, false);

    /* audit */
    if (t_thrd.postgres_cxt.debug_query_string != NULL) {
        pgaudit_ddl_sql_patch(patchName->data, t_thrd.postgres_cxt.debug_query_string);
    } else {
        /* construct command */
        StringInfoData str;
        initStringInfo(&str);
        appendStringInfo(&str, "SELECT * FROM dbe_sql_util.disable_sql_patch('%s')", patchName->data);
        pgaudit_ddl_sql_patch(patchName->data, str.data);
        pfree_ext(str.data);
    }

    u_sess->opt_cxt.xact_modify_sql_patch = true;
    PG_RETURN_BOOL(true);
}

Datum drop_sql_patch(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Cannot alter sql patch with NULL inputs")));
    }

    PreventCommandIfReadOnly("Drop SQL Patch");
    PrivCheck();

    Name patchName = PG_GETARG_NAME(0);
    /* open gs_sql_patch to remove a row */
    Relation rel_gs_sql_patch = heap_open(GsSqlPatchRelationId, RowExclusiveLock);
    HeapTuple tup = GetPatchTuple(PG_GETARG_DATUM(0), rel_gs_sql_patch, USE_NAME);
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("No such SQL patch")));
    }

    simple_heap_delete(rel_gs_sql_patch, &tup->t_self);

    heap_freetuple(tup);

    heap_close(rel_gs_sql_patch, RowExclusiveLock);

    /* audit */
    if (t_thrd.postgres_cxt.debug_query_string != NULL) {
        pgaudit_ddl_sql_patch(patchName->data, t_thrd.postgres_cxt.debug_query_string);
    } else {
        /* construct command */
        StringInfoData str;
        initStringInfo(&str);
        appendStringInfo(&str, "SELECT * FROM dbe_sql_util.drop_sql_patch('%s')", patchName->data);
        pgaudit_ddl_sql_patch(patchName->data, str.data);
        pfree_ext(str.data);
    }

    u_sess->opt_cxt.xact_modify_sql_patch = true;
    PG_RETURN_BOOL(true);
}


Datum show_sql_patch(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Cannot alter sql patch with NULL inputs")));
    }

    PrivCheck();

    /* open gs_sql_patch to remove a row */
    Relation rel_gs_sql_patch = heap_open(GsSqlPatchRelationId, AccessShareLock);

    HeapTuple tup = GetPatchTuple(PG_GETARG_DATUM(0), rel_gs_sql_patch, USE_NAME);
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("No such SQL patch")));
    }

    Form_gs_sql_patch gs_sql_patch = (Form_gs_sql_patch)GETSTRUCT(tup);
    bool hintIsNull = false;
    Datum dat = fastgetattr(tup, Anum_gs_sql_patch_hint_string, rel_gs_sql_patch->rd_att, &hintIsNull);

    int i = 0;
    HeapTuple resTup = NULL;
    TupleDesc tupdesc = NULL;
    int SQL_PATCH_INFO_ATTR_NUM = 4;
    tupdesc = CreateTemplateTupleDesc(SQL_PATCH_INFO_ATTR_NUM, false, TableAmHeap);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "unique_sql_id", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "enable", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "abort", BOOLOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "hint_string", TEXTOID, -1, 0);
    TupleDesc tuple_desc = BlessTupleDesc(tupdesc);

    errno_t rc = 0;
    Datum values[SQL_PATCH_INFO_ATTR_NUM];
    bool nulls[SQL_PATCH_INFO_ATTR_NUM];
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    i = 0;
    values[i++] = Int64GetDatum(gs_sql_patch->unique_sql_id);
    values[i++] = BoolGetDatum(gs_sql_patch->enable);
    values[i++] = BoolGetDatum(gs_sql_patch->abort);
    if (hintIsNull) {
        nulls[i++] = true;
    } else {
        values[i++] = dat;
    }
    resTup = heap_form_tuple(tuple_desc, values, nulls);
    heap_close(rel_gs_sql_patch, AccessShareLock);
    PG_RETURN_DATUM(HeapTupleGetDatum(resTup));
}

static void ModifySQLPatchWorker(PG_FUNCTION_ARGS, bool enable)
{
    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Cannot alter sql patch with NULL inputs")));
    }

    bool nulls[Natts_gs_sql_patch] = {0};
    bool replaces[Natts_gs_sql_patch] = {0};
    Datum values[Natts_gs_sql_patch];
    Relation rel_gs_sql_patch = NULL;
    HeapTuple tup = NULL;
    HeapTuple newTup = NULL;

    /* open gs_sql_patch to update a row */
    rel_gs_sql_patch = heap_open(GsSqlPatchRelationId, RowExclusiveLock);
    
    tup = GetPatchTuple(PG_GETARG_DATUM(0), rel_gs_sql_patch, USE_NAME);
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("No such SQL patch")));
    }

    values[Anum_gs_sql_patch_enable - 1] = BoolGetDatum(enable);
    nulls[Anum_gs_sql_patch_enable - 1] = false;
    replaces[Anum_gs_sql_patch_enable - 1] = true;

    newTup = heap_modify_tuple(tup, RelationGetDescr(rel_gs_sql_patch), values, nulls, replaces);

    simple_heap_update(rel_gs_sql_patch, &newTup->t_self, newTup);

    /* Update indexes */
    CatalogUpdateIndexes(rel_gs_sql_patch, newTup);

    heap_freetuple(tup);
    heap_freetuple(newTup);

    heap_close(rel_gs_sql_patch, RowExclusiveLock);
}

static HeapTuple GetPatchTuple(Datum dat, Relation rel, PatchScanKey key)
{
    HeapTuple tup = NULL;
    ScanKeyData entry;
    SysScanDesc scanDesc = NULL;
    switch (key) {
        case USE_NAME:
            ScanKeyInit(&entry, Anum_gs_sql_patch_patch_name, BTEqualStrategyNumber, F_NAMEEQ, dat);
            scanDesc = systable_beginscan(rel, GsSqlPatchPatchNameIndex, true, NULL, 1, &entry);
            break;
        case USE_UNIQUE_SQL_ID:
            ScanKeyInit(&entry, Anum_gs_sql_patch_unique_sql_id, BTEqualStrategyNumber, F_INT8EQ, dat);
            scanDesc = systable_beginscan(rel, GsSqlPatchUniqueSqlIdIndex, true, NULL, 1, &entry);
            break;
        default:
            /* Syntactic sugar */
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                errmsg("PatchScanKey %d is invalid.", (int)key)));
            break;
    }
    tup = systable_getnext(scanDesc);
    HeapTuple rettup = heap_copytuple(tup);
    /* check if there's more than one hint for USE_UNIQUE_SQL_ID */
    if (key == USE_UNIQUE_SQL_ID && HeapTupleIsValid(tup)) {
        HeapTuple tmp = systable_getnext(scanDesc);
        if (HeapTupleIsValid(tmp)) {
            Form_gs_sql_patch gs_sql_patch = (Form_gs_sql_patch)GETSTRUCT(rettup);
            ereport(WARNING, (errcode(ERRCODE_WARNING),
                errmsg("There is more than one sql patch for the given unique SQL id: %ld. Use the first one named %s.",
                    DatumGetInt64(dat), NameStr(gs_sql_patch->patch_name))));
        }
    }
    systable_endscan(scanDesc);
    return rettup;
}

/*
 * unique_sql_post_parse_analyze - generate sql id
 */
void sql_patch_post_parse_analyze(ParseState* pstate, Query* query)
{
    if (t_thrd.proc->workingVersionNum >= SQL_PATCH_VERSION_NUM) {
        Assert(IS_SINGLE_NODE);
        Assert(query);
        
        switch (query->commandType) {
            case CMD_UTILITY: {
                CheckAbortByPatch(query->uniqueSQLId);
                AddPatchToExplainQuery(query);
                break;
            }
            case CMD_SELECT:
            case CMD_UPDATE:
            case CMD_INSERT:
            case CMD_DELETE:
            case CMD_MERGE: {
                CheckAbortByPatch(query->uniqueSQLId);
                (void)AddPatchToDml(query);
                break;
            }
            default: {
                break;
            }
        }
    }
    if (t_thrd.sql_patch_cxt.sql_patch_prev_post_parse_analyze_hook != NULL) {
        ((post_parse_analyze_hook_type)(t_thrd.sql_patch_cxt.sql_patch_prev_post_parse_analyze_hook))(pstate, query);
    }
}

static bool AddPatchToDml(Query* query)
{
    /* unique sql id is required for sql patch */
    if (query->uniqueSQLId == 0) {
        return false;
    }
    HintState* hs = GetEnabledPatchHintBySqlId(query->uniqueSQLId);
    /* substitute if patch exists */
    if (hs != NULL) {
        pfree_ext(query->hintState);
        query->hintState = hs;
        return true;
    }
    return false;
}

static void AddPatchToExplainQuery(Query* exp)
{
    if (!IsA(exp->utilityStmt, ExplainStmt)) {
        return;
    }
    ExplainStmt* stmt = (ExplainStmt*)exp->utilityStmt;
    if (stmt->query == NULL || !IsA(stmt->query, Query)) {
        return;
    }
    Query* query = (Query*)stmt->query;
    if (query->utilityStmt != NULL) {
        /* could be execute or create-table-as statement, do not support for now */
        return;
    }
    query->uniqueSQLId = (uint64)generate_unique_queryid(query, NULL);
    if (AddPatchToDml(query)) {
        ereport(NOTICE, (errmsg("Plan influenced by SQL hint patch")));
    }
}

static void CheckAbortByPatch(uint64 queryId)
{
    /* unique sql id is required for sql patch */
    if (queryId == 0) {
        return;
    }
    Relation rel_gs_sql_patch = heap_open(GsSqlPatchRelationId, AccessShareLock);
    HeapTuple tup = GetPatchTuple(UInt64GetDatum(queryId), rel_gs_sql_patch, USE_UNIQUE_SQL_ID);
    if (!HeapTupleIsValid(tup)) {
        heap_close(rel_gs_sql_patch, AccessShareLock);
        return;
    }

    /* check if enabled */
    bool isNull = false;
    Datum dat = fastgetattr(tup, Anum_gs_sql_patch_enable, rel_gs_sql_patch->rd_att, &isNull);
    if (isNull || !DatumGetBool(dat)) {
        heap_freetuple(tup);
        heap_close(rel_gs_sql_patch, AccessShareLock);
        return;
    }

    /* check if abort */
    dat = fastgetattr(tup, Anum_gs_sql_patch_abort, rel_gs_sql_patch->rd_att, &isNull);
    if (!isNull && DatumGetBool(dat)) {
        dat = fastgetattr(tup, Anum_gs_sql_patch_patch_name, rel_gs_sql_patch->rd_att, &isNull);
        char* patchName = NameStr(*DatumGetName(dat));
        ereport(ERROR, (errcode(ERRCODE_QUERY_INTERNAL_CANCEL),
            errmsg("Statement %lu canceled by abort patch %s", queryId, patchName)));
    }
    heap_freetuple(tup);
    heap_close(rel_gs_sql_patch, AccessShareLock);
}

void sql_patch_sql_register_hook()
{
    /* only register on single node */
    if (!IS_SINGLE_NODE) {
        return;
    }

    /* do the registration */
    t_thrd.sql_patch_cxt.sql_patch_prev_post_parse_analyze_hook = (void *)post_parse_analyze_hook;
    post_parse_analyze_hook = sql_patch_post_parse_analyze;
}

static HintState* GetEnabledPatchHintBySqlId(uint64 sql_id)
{
    Relation rel_gs_sql_patch = heap_open(GsSqlPatchRelationId, AccessShareLock);
    HeapTuple tup = GetPatchTuple(UInt64GetDatum(sql_id), rel_gs_sql_patch, USE_UNIQUE_SQL_ID);
    if (!HeapTupleIsValid(tup)) {
        heap_close(rel_gs_sql_patch, AccessShareLock);
        return NULL;
    }

    /* check if enabled */
    bool isNull = false;
    Datum dat = fastgetattr(tup, Anum_gs_sql_patch_enable, rel_gs_sql_patch->rd_att, &isNull);
    if (isNull || !DatumGetBool(dat)) {
        heap_freetuple(tup);
        heap_close(rel_gs_sql_patch, AccessShareLock);
        return NULL;
    }

    dat = fastgetattr(tup, Anum_gs_sql_patch_hint_node, rel_gs_sql_patch->rd_att, &isNull);
    if (isNull) {
        heap_freetuple(tup);
        heap_close(rel_gs_sql_patch, AccessShareLock);
        return NULL;
    }
    /* start deserializing hint node */
    char* hintNodeTree = TextDatumGetCString(dat);
    HintState* ret = (HintState*)stringToNode(hintNodeTree);
    heap_freetuple(tup);
    heap_close(rel_gs_sql_patch, AccessShareLock);
    return ret;
}

static void PrivCheck()
{
#ifdef ENABLE_MULTIPLE_NODES
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("SQL Patch is not supported for distribute mode.")));
#endif
    Oid roleId = GetCurrentUserId();
    Form_pg_authid auth = NULL;
    bool authorized = false;
    bool isNull = false;
    Relation relation = heap_open(AuthIdRelationId, AccessShareLock);
    HeapTuple tup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleId));
    if (HeapTupleIsValid(tup)) {
        auth = (Form_pg_authid)GETSTRUCT(tup);
        bool isSuperUser = auth->rolsuper;
        bool isSysAdmin = auth->rolsystemadmin;
        Datum dat = heap_getattr(tup, Anum_pg_authid_roloperatoradmin, RelationGetDescr(relation), &isNull);
        bool isOprAdmin = isNull ? false : DatumGetBool(dat);
        dat = heap_getattr(tup, Anum_pg_authid_rolmonitoradmin, RelationGetDescr(relation), &isNull);
        bool isMonAdmin = isNull ? false : DatumGetBool(dat);
        authorized = isSuperUser || isSysAdmin || isOprAdmin || isMonAdmin;
        ReleaseSysCache(tup);
    } else {
        ereport(ERROR, (errmodule(MOD_SEC), errcode(ERRCODE_UNDEFINED_OBJECT),
            errmsg("role with OID %u does not exist", roleId), errdetail("N/A"),
                errcause("System error."), erraction("Contact engineer to support.")));
    }
    heap_close(relation, AccessShareLock);
    if (!authorized) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Must be superuser or system/operator/monitor administrator to use SQL patch")));
    }
}

static char* ProcessHintString(const char* hintStr)
{
    HintState* hs = create_hintstate_worker(hintStr);

    if (hs == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Cannot get a valid hint from input")));
    }

    if (hs->hint_warning != NIL) {
        output_hint_warning(hs->hint_warning, WARNING);
    }

    RemovePatchUnsupportHint(hs);

    if (hs->nall_hints == 0) {
        pfree(hs);
        return NULL;
    }

    hs->from_sql_patch =  true;

    char* hintNodeTree = nodeToString(hs);

    pfree(hs);
    return hintNodeTree;
}

static void RemovePatchUnsupportHint(HintState* hs)
{
    bool remove = false;
    StringInfoData str;
    initStringInfo(&str);
    appendStringInfo(&str, "The followed hints are removed since they are not supported by SQL Patch:\n");
    /*
     * Hints like stream/skew/multi-node are banned by parser for
     * centralized mode, so no need to handle here.
     */
    /* only support outermost hint for now */
    if (hs->block_name_hint != NIL) {
        remove = true;
        appendStringInfo(&str, "%s ", HINT_BLOCKNAME);
        list_free_ext(hs->block_name_hint);
    }

    if (hs->no_expand_hint != NIL) {
        remove = true;
        appendStringInfo(&str, "%s ", HINT_NO_EXPAND);
        list_free_ext(hs->no_expand_hint);
    }

    /* no_gpc hint is not supported for now */
    if (hs->no_gpc_hint != NIL) {
        remove = true;
        appendStringInfo(&str, "%s ", HINT_NO_GPC);
        list_free_ext(hs->no_gpc_hint);
    }

    /* rewrite rule hint is not supported for now */
    if (hs->rewrite_hint != NIL) {
        remove = true;
        appendStringInfo(&str, "%s ", HINT_REWRITE);
        list_free_ext(hs->rewrite_hint);
    }

    if (remove) {
        hs->nall_hints =
            list_length(hs->join_hint) +
            list_length(hs->leading_hint) +
            list_length(hs->row_hint) +
            list_length(hs->scan_hint) +
            list_length(hs->predpush_hint) +
            list_length(hs->predpush_same_level_hint) +
            list_length(hs->set_hint) +
            list_length(hs->cache_plan_hint);
        ereport(WARNING, (errcode(ERRCODE_WARNING), errmsg("%s", str.data)));
    }
    pfree_ext(str.data);
}

bool CheckRecreateCachePlanBySqlPatch(const CachedPlanSource *plansource)
{
    if (t_thrd.proc->workingVersionNum < SQL_PATCH_VERSION_NUM) {
        return false;
    }
    if (u_sess->attr.attr_common.XactReadOnly) {
        return false;
    }
    return plansource->sql_patch_sequence <
        pg_atomic_read_u64(&g_instance.cost_cxt.sql_patch_sequence_id);
}

void RevalidateGplanBySqlPatch(CachedPlanSource *plansource)
{
    if (t_thrd.proc->workingVersionNum < SQL_PATCH_VERSION_NUM) {
        return;
    }
    bool needRevalidate = false;
    /* skip if no SQL patch is modified since gplan is built */
    if (!u_sess->attr.attr_common.XactReadOnly &&
        (plansource->sql_patch_sequence >=
            pg_atomic_read_u64(&g_instance.cost_cxt.sql_patch_sequence_id))) {
        return;
    }

    Relation rel_gs_sql_patch = heap_open(GsSqlPatchRelationId, AccessShareLock);
    uint64 uniqueSQLId = 0;

    ListCell* lc = NULL;
    foreach (lc, plansource->query_list) {
        Query* query = (Query*)lfirst(lc);
        if (query->uniqueSQLId == 0) {
            continue;
        }

        /* Patched query needs to be revalidate anyway in case of drop/disable patch */
        if (QueryIsPatched(query)) {
            if (plansource->gplan != NULL) {
                plansource->gplan->is_valid = false;
            }
            plansource->is_valid = false;
            needRevalidate = true;
            ereport(DEBUG2,
                (errmodule(MOD_SQLPATCH), errmsg("[SQLPatch] %lu is already patched, need revalidate",
                query->uniqueSQLId)));
            continue;
        }

        uniqueSQLId = query->uniqueSQLId;
        HeapTuple tup = GetPatchTuple(UInt64GetDatum(query->uniqueSQLId), rel_gs_sql_patch, USE_UNIQUE_SQL_ID);
        if (HeapTupleIsValid(tup)) {
            heap_freetuple(tup);
            if (plansource->gplan != NULL) {
                plansource->gplan->is_valid = false;
            }
            plansource->is_valid = false;
            needRevalidate = true;
            ereport(DEBUG2,
                (errmodule(MOD_SQLPATCH), errmsg("[SQLPatch] %lu found matching patch, need revalidate",
                query->uniqueSQLId)));
            break;
        }
    }

    if (!needRevalidate) {
        ereport(DEBUG2,
            (errmodule(MOD_SQLPATCH), errmsg("[SQLPatch] %lu no matching patch, update patch sequence id "
            "from %lu to %lu", uniqueSQLId, plansource->sql_patch_sequence,
            pg_atomic_read_u64(&g_instance.cost_cxt.sql_patch_sequence_id))));
        plansource->sql_patch_sequence = pg_atomic_read_u64(&g_instance.cost_cxt.sql_patch_sequence_id);
    }

    heap_close(rel_gs_sql_patch, AccessShareLock);
    /* plansource->sql_patch_sequence will be set to global id after parsed again */
}

static bool QueryIsPatched(const Query* query)
{
    if (query->hintState == NULL) {
        return false;
    }

    return query->hintState->from_sql_patch;
}
