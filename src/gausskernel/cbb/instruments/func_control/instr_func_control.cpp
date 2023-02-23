/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * instr_func_control.cpp
 *   functions for dynamic function control manager
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/instruments/func_control/instr_func_control.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "instruments/instr_func_control.h"
#include "instruments/instr_statement.h"
#include "utils/builtins.h"
#include "access/hash.h"
#include "funcapi.h"

static const int INSTRUMENTS_DYNAMIC_FUNC_CTL_ATTRNUM = 2;

static void stmt_track_hash_search(bool remove_entry, StringInfo buffer, bool log_entry);
typedef void (*instr_dynamic_func_handle)(StringInfo result, const char *action, const Datum *param_datums,
    int no_params);
static void instr_stmt_dynamic_handler(StringInfo result, const char *action, const Datum *param_datums, int no_params);

typedef struct {
    const char *func_name;
    instr_dynamic_func_handle func_handle;
} InstrFuncHandle;

typedef struct {
    uint64 unique_sql_id;
    StatLevel stmt_track_level;
} DynamicStmtTrackEntry;

typedef struct {
    ParallelFunctionState* state;
    TupleTableSlot* slot;
} FuncCtlInfo;

static InstrFuncHandle func_handle_mapper[] = {
    {"STMT", &instr_stmt_dynamic_handler}
};

static instr_dynamic_func_handle get_dynamic_func_handler(const char *func_name)
{
    for (uint32 i = 0; i < (sizeof(func_handle_mapper) / sizeof(func_handle_mapper[0])); i++) {
        if (pg_strcasecmp(func_name, func_handle_mapper[i].func_name) == 0) {
            return func_handle_mapper[i].func_handle;
        }
    }
    return NULL;
}

static void check_func_control_scope(const char *scope)
{
    if (pg_strcasecmp(scope, "LOCAL") == 0) {
        return;
    }
    if (pg_strcasecmp(scope, "GLOBAL") == 0) {
#ifdef ENABLE_MULTIPLE_NODES
        return;
#endif
    }
    ereport(ERROR, (errmodule(MOD_INSTR), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("[FuncCtl] scope options: 'LOCAL' when single node mode, or 'LOCAL/GLOBAL' when distributed mode"),
        errdetail("invalid scope options:%s", scope),
        errcause("invalid scope options"),
        erraction("scope should be 'LOCAL' or 'GLOBAL'")));
}

static void get_local_func_ctl_info(PG_FUNCTION_ARGS, Tuplestorestate *tupStore, TupleDesc tupDesc,
    const Datum *param_datums, int no_params)
{
    char *func_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char *action = text_to_cstring(PG_GETARG_TEXT_PP(2));

    Datum values[INSTRUMENTS_DYNAMIC_FUNC_CTL_ATTRNUM];
    bool nulls[INSTRUMENTS_DYNAMIC_FUNC_CTL_ATTRNUM] = {false};
    int rc = 0;
    StringInfoData result = {0};
    initStringInfo(&result);

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    instr_dynamic_func_handle func_handle = get_dynamic_func_handler(func_name);
    if (func_handle == NULL) {
        appendStringInfo(&result, "invalid func name, options:[STMT]");
    } else {
        appendStringInfo(&result, "ACTION:\t[%s]->[%s]\n", func_name, action);
        appendStringInfo(&result, "RESULT:\n");
        func_handle(&result, action, param_datums, no_params);
    }

    values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
    values[1] = CStringGetTextDatum(result.data);
    tuplestore_putvalues(tupStore, tupDesc, values, nulls);

    pfree_ext(result.data);
}

static FuncCtlInfo *get_remote_func_ctl_info(PG_FUNCTION_ARGS, TupleDesc tupleDesc,
    const Datum *param_datums, int no_params)
{
    char *func_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char *action = text_to_cstring(PG_GETARG_TEXT_PP(2));

    StringInfoData params_str;
    initStringInfo(&params_str);

    if (no_params == 0 || param_datums == NULL) {
        appendStringInfo(&params_str, "null");
    } else {
        appendStringInfo(&params_str, "'{");
        for (int i = 0; i < no_params; i++) {
            char *tmp = TextDatumGetCString(param_datums[i]);
            appendStringInfo(&params_str, "\"%s\"", tmp);

            if (i != no_params - 1) {
                appendStringInfo(&params_str, ",");
            }
            pfree_ext(tmp);
        }
        appendStringInfo(&params_str, "}'");
    }

    StringInfoData buf;
    FuncCtlInfo *func_ctl_info = (FuncCtlInfo*)palloc0(sizeof(FuncCtlInfo));

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT * from pg_catalog.dynamic_func_control('LOCAL', '%s', '%s', %s);",
        func_name, action, params_str.data);

    func_ctl_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    func_ctl_info->slot = MakeSingleTupleTableSlot(tupleDesc);

    pfree_ext(buf.data);
    pfree_ext(params_str.data);
    return func_ctl_info;
}

Datum dynamic_func_control(PG_FUNCTION_ARGS)
{
    if (!superuser() && !isMonitoradmin(GetUserId())) {
        ereport(ERROR, (errmodule(MOD_INSTR), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("[FuncCtl] only system/monitor admin can use dynamic func control"),
            errdetail("user should be 'Sysadmin' or 'Monitoradmin'"),
            errcause("user without enough permission"),
            erraction("check user permission to run the proc")));
    }
    char *scope = text_to_cstring(PG_GETARG_TEXT_PP(0));
    check_func_control_scope(scope);

    Datum params = PG_GETARG_DATUM(3);
    ArrayType *param_arr = NULL;
    Datum *param_datums = NULL;
    FuncCtlInfo *func_ctl_info = NULL;
    int no_params = 0;

    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

    if (PointerIsValid(DatumGetPointer(params))) {
        param_arr = DatumGetArrayTypeP(params);
        deconstruct_array(param_arr, TEXTOID, -1, false, 'i', &param_datums, NULL, &no_params);
    }

    TupleDesc tupdesc = CreateTemplateTupleDesc(INSTRUMENTS_DYNAMIC_FUNC_CTL_ATTRNUM, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "result", TEXTOID, -1, 0);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->setDesc = BlessTupleDesc(tupdesc);

    get_local_func_ctl_info(fcinfo, rsinfo->setResult, rsinfo->setDesc, param_datums, no_params);
    if (pg_strcasecmp(scope, "GLOBAL") == 0) {
        func_ctl_info = get_remote_func_ctl_info(fcinfo, rsinfo->setDesc, param_datums, no_params);
    }

    while (func_ctl_info != NULL) {
        Tuplestorestate* tupstore = func_ctl_info->state->tupstore;
        TupleTableSlot* slot = func_ctl_info->slot;

        if (!tuplestore_gettupleslot(tupstore, true, false, slot)) {
            FreeParallelFunctionState(func_ctl_info->state);
            ExecDropSingleTupleTableSlot(slot);
            pfree_ext(func_ctl_info);
            break;
        }

        tuplestore_puttupleslot(rsinfo->setResult, slot);
        ExecClearTuple(slot);
    }
    tuplestore_donestoring(rsinfo->setResult);
    MemoryContextSwitchTo(oldcontext);

    if (DatumGetPointer(params) != DatumGetPointer(param_arr)) {
        pfree(param_arr);
    }
    pfree_ext(param_datums);
    return (Datum) 0;
}

static int stmt_track_match_func(const void* k1, const void* k2, Size size)
{
    ((void)size);   /* not used argument */
    if (k1 != NULL && k2 != NULL && *((uint64*)k1) == *((uint64*)k2)) {
        return 0;
    } else {
        return 1;
    }
}

void InitTrackStmtControl()
{
    MemoryContext mem_cxt = AllocSetContextCreate(g_instance.instance_context,
        "DynamicFuncCtrolMgr",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    HASHCTL ctl;
    const long stmt_track_hash_count = 100;
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check_c(rc, "\0", "\0");

    ctl.hcxt = mem_cxt;
    ctl.keysize = sizeof(uint64);
    ctl.entrysize = sizeof(DynamicStmtTrackEntry);
    ctl.match = stmt_track_match_func;

    g_instance.stat_cxt.stmt_track_control_hashtbl = hash_create("dynamic stmt track control hash table",
        stmt_track_hash_count, &ctl, HASH_ELEM | HASH_SHRCTX | HASH_BLOBS | HASH_COMPARE | HASH_NOEXCEPT);
}

/*
 * return true if successful
 */
static bool extrack_unique_sql_id(StringInfo buffer, const char *unique_sql_id_str, uint64 *unique_sql_id)
{
    /* unique sql id should >= 0 */
    if (unique_sql_id_str == NULL) {
        appendStringInfo(buffer, "FAILED, reason: unique sql id should not be NULL!");
        return false;
    }
    for (size_t i = 0; i < strlen(unique_sql_id_str); i++) {
        if (unique_sql_id_str[i] == '-') {
            appendStringInfo(buffer, "FAILED, reason: unique sql id should not contain '-'!");
            return false;
        } else if (unique_sql_id_str[i] != ' ') {
            break;
        }
    }

    char *endptr = NULL;
    errno = 0;
    *unique_sql_id = pg_strtouint64(unique_sql_id_str, &endptr, 0);
    if (errno != 0) {
        appendStringInfo(buffer, "FAILED, reason: %s", gs_strerror(errno));
        errno = 0;
        return false;
    }
    if (endptr != (unique_sql_id_str + strlen(unique_sql_id_str))) {
        appendStringInfo(buffer, "FAILED, reason: invalid unique sql id (%s), should be uint64", unique_sql_id_str);
        return false;
    }
    return true;
}

static void instr_stmt_func_action_track(StringInfo buffer, const Datum *param_datums, int no_params)
{
    const int track_action_param_count = 2;
    if (no_params != track_action_param_count) {
        appendStringInfo(buffer, "FAILED, params should contain two elements, e.g. '{\"1234\", \"L1\"}'");
        return;
    }

    /* get unique sql id */
    char *unique_sql_id_str = TextDatumGetCString(param_datums[0]);
    uint64 unique_sql_id = 0;
    if (!extrack_unique_sql_id(buffer, unique_sql_id_str, &unique_sql_id)) {
        pfree_ext(unique_sql_id_str);
        return;
    }
    pfree_ext(unique_sql_id_str);

    /* get track level against the unique sql */
    char *track_level = TextDatumGetCString(param_datums[1]);
    StatLevel stmt_level = LEVEL_INVALID;
    if (pg_strcasecmp("L0", track_level) == 0) {
        stmt_level = STMT_TRACK_L0;
    } else if (pg_strcasecmp("L1", track_level) == 0) {
        stmt_level = STMT_TRACK_L1;
    } else if (pg_strcasecmp("L2", track_level) == 0) {
        stmt_level = STMT_TRACK_L2;
    } else {
        appendStringInfo(buffer, "FAILED, invalid levels, options:[L0/L1/L2]");
        return;
    }

    DynamicStmtTrackEntry *entry = NULL;
    LWLockAcquire(InstrStmtTrackCtlLock, LW_SHARED);
    entry = (DynamicStmtTrackEntry*)hash_search(g_instance.stat_cxt.stmt_track_control_hashtbl, &unique_sql_id,
        HASH_FIND, NULL);
    if (entry == NULL) {
        LWLockRelease(InstrStmtTrackCtlLock);

        LWLockAcquire(InstrStmtTrackCtlLock, LW_EXCLUSIVE);
        entry = (DynamicStmtTrackEntry*)hash_search(g_instance.stat_cxt.stmt_track_control_hashtbl,
            &unique_sql_id, HASH_ENTER, NULL);
        if (entry == NULL) {
            appendStringInfo(buffer, "FAILED, out of memory!");
            LWLockRelease(InstrStmtTrackCtlLock);
            return;
        }
    }
    entry->stmt_track_level = stmt_level;
    LWLockRelease(InstrStmtTrackCtlLock);
    appendStringInfo(buffer, "SUCCESSFUL - stmt (%lu) tracked, level (L%d)", unique_sql_id, (stmt_level - 1));
}

static void instr_stmt_func_action_untrack(StringInfo buffer, const Datum *param_datums, int no_params)
{
    if (no_params != 1) {
        appendStringInfo(buffer, "\tFAILED, UNTRACK need 1 param(unique id)\n");
        return;
    }

    /* get unique sql id */
    char *unique_sql_id_str = TextDatumGetCString(param_datums[0]);
    uint64 unique_sql_id = 0;
    if (!extrack_unique_sql_id(buffer, unique_sql_id_str, &unique_sql_id)) {
        pfree_ext(unique_sql_id_str);
        return;
    }

    DynamicStmtTrackEntry *entry = NULL;
    LWLockAcquire(InstrStmtTrackCtlLock, LW_SHARED);
    entry = (DynamicStmtTrackEntry*)hash_search(g_instance.stat_cxt.stmt_track_control_hashtbl, &unique_sql_id,
        HASH_FIND, NULL);
    if (entry == NULL) {
        appendStringInfo(buffer, "\tFAILED, stmt(%s) not tracked!", unique_sql_id_str);
    } else {
        LWLockRelease(InstrStmtTrackCtlLock);
        LWLockAcquire(InstrStmtTrackCtlLock, LW_EXCLUSIVE);
        (void)hash_search(g_instance.stat_cxt.stmt_track_control_hashtbl, &unique_sql_id, HASH_REMOVE, NULL);
        appendStringInfo(buffer, "\tSUCCESSFUL, stmt(%s) is removed from tracking list!", unique_sql_id_str);
    }
    LWLockRelease(InstrStmtTrackCtlLock);
    pfree_ext(unique_sql_id_str);
}

static void instr_stmt_func_action_clean(StringInfo buffer)
{
    appendStringInfo(buffer, "\tCLEANED INFO");
    stmt_track_hash_search(true, buffer, true);
}

static void stmt_track_hash_search(bool remove_entry, StringInfo buffer, bool log_entry)
{
    HASH_SEQ_STATUS hash_seq;
    DynamicStmtTrackEntry *entry = NULL;

    LWLockAcquire(InstrStmtTrackCtlLock, LW_SHARED);
    hash_seq_init(&hash_seq, g_instance.stat_cxt.stmt_track_control_hashtbl);
    while ((entry = (DynamicStmtTrackEntry*)hash_seq_search(&hash_seq)) != NULL) {
        if (buffer != NULL && log_entry) {
            appendStringInfo(buffer, "\nLEVEL: L%d, UNIQUE ID: %11lu", entry->stmt_track_level - 1,
                entry->unique_sql_id);
        }
        if (remove_entry) {
            hash_search(g_instance.stat_cxt.stmt_track_control_hashtbl, &entry->unique_sql_id, HASH_REMOVE, NULL);
        }
    }
    LWLockRelease(InstrStmtTrackCtlLock);
}

static void instr_stmt_func_action_list(StringInfo buffer)
{
    appendStringInfo(buffer, "\tLIST TRACKING STMT INFO");
    stmt_track_hash_search(false, buffer, true);
}

static void instr_stmt_dynamic_handler(StringInfo result, const char *action, const Datum *param_datums, int no_params)
{
    if (pg_strcasecmp("TRACK", action) == 0) {
        instr_stmt_func_action_track(result, param_datums, no_params);
    } else if (pg_strcasecmp("UNTRACK", action) == 0) {
        instr_stmt_func_action_untrack(result, param_datums, no_params);
    } else if (pg_strcasecmp("CLEAN", action) == 0) {
        instr_stmt_func_action_clean(result);
    } else if (pg_strcasecmp("LIST", action) == 0) {
        instr_stmt_func_action_list(result);
    } else {
        appendStringInfo(result, "Func(STMT) - invalid action name, options:[TRACK|UNTRACK|CLEAN|LIST]");
    }
}

StatLevel instr_track_stmt_find_level()
{
    DynamicStmtTrackEntry *entry = NULL;
    uint64 key = u_sess->unique_sql_cxt.unique_sql_id;
    StatLevel specified_level = STMT_TRACK_OFF;

    if (hash_get_num_entries(g_instance.stat_cxt.stmt_track_control_hashtbl) == 0) {
        return specified_level;
    }

    LWLockAcquire(InstrStmtTrackCtlLock, LW_SHARED);
    entry = (DynamicStmtTrackEntry*)hash_search(g_instance.stat_cxt.stmt_track_control_hashtbl, &key, HASH_FIND, NULL);
    if (entry != NULL) {
        specified_level = entry->stmt_track_level;
    }
    LWLockRelease(InstrStmtTrackCtlLock);
    return specified_level;
}
