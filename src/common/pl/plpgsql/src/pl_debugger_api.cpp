/* -------------------------------------------------------------------------
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * pl_debugger_api.cpp      - debug functions for the PL/pgSQL
 *            procedural language
 *
 * IDENTIFICATION
 *   src/common/pl/plpgsql/src/pl_debugger_api.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "commands/copy.h"
#include "catalog/pg_authid.h"
#include "utils/builtins.h"
#include "funcapi.h"
#include "utils/acl.h"
#include "utils/plpgsql.h"
#include <sys/socket.h>

/*
    supported functions:
    dbe_pldebugger.attatch
    dbe_pldebugger.info_locals
    dbe_pldebugger.next
    dbe_pldebugger.continue
    dbe_pldebugger.abort
    dbe_pldebugger.turn_on
    dbe_pldebugger.turn_off
    dbe_pldebugger.add_breakpoint
    dbe_pldebugger.delete_breakpoint
    dbe_pldebugger.info_breakpoints
    dbe_pldebugger.backtrace
    dbe_pldebugger.info_code
    dbe_pldebugger.step
*/

typedef struct {
    char* nodename;
    int port;
    Oid funcoid;
} DebuggerServerInfo;


/* send/rec msg for client */
static void debug_client_rec_msg(DebugClientInfo* client);
static void debug_client_send_msg(DebugClientInfo* client, char first_char, char* msg, int msg_len);

static Datum get_tuple_lineno_and_query(DebugClientInfo* client);
static void InterfaceCheck(const char* funcname, bool needAttach = true);
static PlDebugEntry* add_debug_func(Oid key);
static void* get_debug_entries(uint32* num);
static DebugClientInfo* InitDebugClient(int comm_idx);
static CodeLine* debug_show_code_worker(Oid funcid, uint32* num, int* headerlines);
static void* debug_client_split_breakpoints_msg(uint32* num);
static void* debug_client_split_localvariables_msg(uint32 *num);
static void* debug_client_split_backtrace_msg(uint32* num);
static List* collect_breakable_line_oid(Oid funcOid);
static void init_pldebug_htcl();
static bool CheckPlpgsqlFunc(Oid funcoid, bool report_error = true);
static List* collect_breakable_line(PLpgSQL_function* func);

static Datum get_tuple_lineno_and_query(DebugClientInfo* client)
{
    int i = 0;

    TupleDesc tupdesc;
    MemoryContext oldcontext = MemoryContextSwitchTo(client->context);

    int DEBUG_NEXT_ATTR_NUM = 4;
    tupdesc = CreateTemplateTupleDesc(DEBUG_NEXT_ATTR_NUM, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "func_oid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "funcname", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "lineno", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "query", TEXTOID, -1, 0);
    TupleDesc tuple_desc = BlessTupleDesc(tupdesc);

    /* Received buffer will be in the form of <func_oid:funcname:lineno:query> */
    char* psave = NULL;
    char* fir = strtok_r(client->rec_buffer, ":", &psave);
    const int int64Size = 10;
    Oid func_oid;
    char* funcname = NULL;
    int line_no;
    char* query = NULL;
    Datum values[DEBUG_NEXT_ATTR_NUM];
    bool nulls[DEBUG_NEXT_ATTR_NUM];
    HeapTuple tuple;
    errno_t rc = 0;

    char* new_fir = TrimStr(fir);
    
    if (new_fir == NULL) {
        ReportInvalidMsg(client->rec_buffer);
        PG_RETURN_DATUM(0);
    }
    func_oid = (Oid)pg_strtouint64(new_fir, NULL, int64Size);
    fir = strtok_r(NULL, ":", &psave);
    funcname = AssignStr(fir, false);
    fir = strtok_r(NULL, ":", &psave);
    new_fir = TrimStr(fir);
    if (new_fir == NULL) {
        ReportInvalidMsg(client->rec_buffer);
        PG_RETURN_DATUM(0);
    }
    line_no = pg_strtoint32(new_fir);
    query = AssignStr(psave, false);
    (void)MemoryContextSwitchTo(oldcontext);

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    i = 0;
    values[i++] = ObjectIdGetDatum(func_oid);
    values[i++] = CStringGetTextDatum(funcname);
    values[i++] = Int32GetDatum(line_no);
    values[i++] = CStringGetTextDatum(query);
    tuple = heap_form_tuple(tuple_desc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

static void check_debugger_valid(int commidx)
{
    PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[commidx];
    AutoMutexLock debuglock(&debug_comm->mutex);
    debuglock.lock();
    if (debug_comm->Used()) {
        if (debug_comm->hasClient()) {
            debuglock.unLock();
            ereport(ERROR, (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_TARGET_SERVER_ALREADY_ATTACHED),
                (errmsg("procedure has already attached on other client."))));
        }
        if (!(debug_comm->isRunning() && debug_comm->IsServerWaited)) {
            debuglock.unLock();
            ereport(ERROR, (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_INVALID_OPERATION),
                (errmsg("procedure is not running in expected way."))));
        }
        if (debug_comm->hasClientErrorOccured || debug_comm->hasServerErrorOccured) {
            debuglock.unLock();
            ereport(ERROR, (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_INVALID_OPERATION),
                (errmsg("procedure is not running in expected way."))));
        }
    } else {
        debuglock.unLock();
        ereport(ERROR, (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_INVALID_OPERATION),
            (errmsg("Corresponding procedure not turn on yet."))));
    }
    debuglock.unLock();
}

/*
 *   dbe_pldebugger.attach
 *   attach debug client to debug server
 */
Datum debug_client_attatch(PG_FUNCTION_ARGS)
{
    InterfaceCheck("attach", false);
    /* get ip from nodename, no need in single node */
    char* nodename = text_to_cstring(PG_GETARG_TEXT_PP(0));
    int commidx = PG_GETARG_INT32(1);
    /* if is attach to some other function, just clean up it */
    clean_up_debug_client(true);
    /* this nodename check is only for single node */
    nodename = TrimStr(nodename);
    if (nodename == NULL || strcasecmp(nodename, g_instance.attr.attr_common.PGXCNodeName) != 0) {
        ereport(ERROR, (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_AMBIGUOUS_PARAMETER),
            (errmsg("wrong debug nodename, should be %s.", g_instance.attr.attr_common.PGXCNodeName))));
    }
    if (commidx < 0 || commidx >= PG_MAX_DEBUG_CONN) {
        ereport(ERROR, (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_AMBIGUOUS_PARAMETER),
            (errmsg("invalid debug port id %d.", commidx))));
    }
    u_sess->plsql_cxt.debug_client = InitDebugClient(commidx);
    DebugClientInfo* client = u_sess->plsql_cxt.debug_client;
    MemoryContext old_cxt = MemoryContextSwitchTo(client->context);
    /* only can attach when comm satisfy contidion */
    check_debugger_valid(commidx);

    char buf[MAXINT8LEN + 1] = {'\0'};
    pg_lltoa(commidx, buf);
    StringInfoData str;
    initStringInfo(&str);
    appendStringInfo(&str, "%d:%lu", commidx, u_sess->session_id);
    /* send msg */
    debug_client_send_msg(client, DEBUG_ATTACH_HEADER, str.data, str.len);
    /* wait for server msg */
    debug_client_rec_msg(client);
    MemoryContextSwitchTo(old_cxt);
    /* get lineno tuple */
    return get_tuple_lineno_and_query(client);
}

/*
 *   dbe_pldebugger.print_var
 *   print the type and value of the given variable
 */
Datum debug_client_print_variables(PG_FUNCTION_ARGS)
{
    InterfaceCheck("print_var");
    char* var_name = text_to_cstring(PG_GETARG_TEXT_PP(0));

    const int DEBUG_LOCAL_VAR_TUPLE_ATTR_NUM = 4;

    FuncCallContext *funcctx = NULL;
    MemoryContext oldcontext;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 4 columns */
        tupdesc = CreateTemplateTupleDesc(DEBUG_LOCAL_VAR_TUPLE_ATTR_NUM, false, TAM_HEAP);
        int i = 0;
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "varname", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "vartype", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "value", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "package_name", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /* send msg & receive local variables from debug server */
        debug_client_send_msg(u_sess->plsql_cxt.debug_client, DEBUG_PRINT_HEADER, var_name, strlen(var_name));
        debug_client_rec_msg(u_sess->plsql_cxt.debug_client);

        /* total number of tuples to be returned */
        funcctx->user_fctx = debug_client_split_localvariables_msg(&(funcctx->max_calls));
        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) { /* do when there is more left to send */
        Datum values[DEBUG_LOCAL_VAR_TUPLE_ATTR_NUM];
        bool nulls[DEBUG_LOCAL_VAR_TUPLE_ATTR_NUM];
        HeapTuple tuple;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        PLDebug_variable* entry = (PLDebug_variable*)funcctx->user_fctx + funcctx->call_cntr;

        int i = 0;
        values[i++] = CStringGetTextDatum(entry->name);
        values[i++] = CStringGetTextDatum(entry->var_type);
        values[i++] = CStringGetTextDatum(entry->value);
        values[i++] = CStringGetTextDatum("");
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}

/*
 *   dbe_pldebugger.info_locals
 *   print the type and value of the all variables at current stack depth
 */
Datum debug_client_local_variables(PG_FUNCTION_ARGS)
{
    InterfaceCheck("info_locals");
    const int DEBUG_LOCAL_VAR_TUPLE_ATTR_NUM = 4;

    FuncCallContext *funcctx = NULL;
    MemoryContext oldcontext;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 4 columns */
        tupdesc = CreateTemplateTupleDesc(DEBUG_LOCAL_VAR_TUPLE_ATTR_NUM, false, TAM_HEAP);
        int i = 0;
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "varname", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "vartype", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "value", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "package_name", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /* send msg & receive local variables from debug server */
        debug_client_send_msg(u_sess->plsql_cxt.debug_client, DEBUG_LOCALS_HEADER, NULL, 0);
        debug_client_rec_msg(u_sess->plsql_cxt.debug_client);

        /* total number of tuples to be returned */
        funcctx->user_fctx = debug_client_split_localvariables_msg(&(funcctx->max_calls));
        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls)     { /* do when there is more left to send */
        Datum values[DEBUG_LOCAL_VAR_TUPLE_ATTR_NUM];
        bool nulls[DEBUG_LOCAL_VAR_TUPLE_ATTR_NUM];
        HeapTuple tuple;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        PLDebug_variable* entry = (PLDebug_variable*)funcctx->user_fctx + funcctx->call_cntr;

        int i = 0;
        values[i++] = CStringGetTextDatum(entry->name);
        values[i++] = CStringGetTextDatum(entry->var_type);
        values[i++] = CStringGetTextDatum(entry->value);
        values[i++] = CStringGetTextDatum("");
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}

/*
 *   dbe_pldebugger.info_code
 *   print the code of given function
 */
Datum debug_client_info_code(PG_FUNCTION_ARGS)
{
    Oid funcid = PG_GETARG_OID(0);

    const int DEBUG_LOCAL_VAR_TUPLE_ATTR_NUM = 3;

    FuncCallContext *funcctx = NULL;
    MemoryContext oldcontext;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 4 columns */
        tupdesc = CreateTemplateTupleDesc(DEBUG_LOCAL_VAR_TUPLE_ATTR_NUM, false, TAM_HEAP);
        int i = 0;
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "lineno", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "code", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "canbreak", BOOLOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        int headerlines = 0;
        /* total number of tuples to be returned */
        funcctx->user_fctx = debug_show_code_worker(funcid, &(funcctx->max_calls), &headerlines);
        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls)     { /* do when there is more left to send */
        Datum values[DEBUG_LOCAL_VAR_TUPLE_ATTR_NUM];
        bool nulls[DEBUG_LOCAL_VAR_TUPLE_ATTR_NUM];
        HeapTuple tuple;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        CodeLine* entry = (CodeLine*)funcctx->user_fctx + funcctx->call_cntr;

        int i = 0;
        if (entry->lineno > 0) {
            values[i++] = Int32GetDatum(entry->lineno);
        } else {
            nulls[i++] = true;
        }
        values[i++] = CStringGetTextDatum(entry->code);
        values[i++] = BoolGetDatum(entry->canBreak);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}

/*
 *   dbe_pldebugger.abort
 *   abort current procedure, throw abort error
 */
Datum debug_client_abort(PG_FUNCTION_ARGS)
{
    InterfaceCheck("abort");
    DebugClientInfo* client = u_sess->plsql_cxt.debug_client;
    debug_client_send_msg(client, DEBUG_ABORT_HEADER, NULL, 0);
    debug_client_rec_msg(client);
    bool ans = (u_sess->plsql_cxt.debug_client->rec_buffer[0] == 't');
    clean_up_debug_client();
    PG_RETURN_BOOL(ans);
}

/*
 *   dbe_pldebugger.next
 *   execute one query in current procedure if already attach to one.
 */
Datum debug_client_next(PG_FUNCTION_ARGS)
{
    InterfaceCheck("next");
    DebugClientInfo* client = u_sess->plsql_cxt.debug_client;
    /* send msg & receive local variables from debug server */
    debug_client_send_msg(client, DEBUG_NEXT_HEADER, NULL, 0);
    debug_client_rec_msg(client);
    return get_tuple_lineno_and_query(client);
}

/*
 *  dbe_pldebugger.continue
 *  execute until next breakpoint
 */
Datum debug_client_continue(PG_FUNCTION_ARGS)
{
    InterfaceCheck("continue");
    DebugClientInfo* client = u_sess->plsql_cxt.debug_client;
    debug_client_send_msg(u_sess->plsql_cxt.debug_client, DEBUG_CONTINUE_HEADER, NULL, 0);
    debug_client_rec_msg(u_sess->plsql_cxt.debug_client);
    return get_tuple_lineno_and_query(client);
}

/*
 *  dbe_pldebugger.add_breakpoint
 *  add a new breakpoint
 */
Datum debug_client_add_breakpoint(PG_FUNCTION_ARGS)
{
    InterfaceCheck("add_breakpoint");

    Oid funcOid = PG_GETARG_OID(0);
    int32 lineno = PG_GETARG_INT32(1);
    (void)CheckPlpgsqlFunc(funcOid);
    uint32 nLine = 0;
    int headerlines = 0;
    CodeLine* lines = debug_show_code_worker(funcOid, &nLine, &headerlines);

    if (lineno < 1 || (uint32)lineno > nLine - headerlines) {
        ereport(WARNING, (errcode(ERRCODE_WARNING),
            errmsg("lineno must be within the range of [1, MaxLineNumber]"
            " Please use dbe_pldebugger.info_code for valid breakpoint candidates")));
        PG_RETURN_INT32(-1);
    }

    CodeLine cl = lines[(uint32)headerlines + lineno - 1];
    if (!cl.canBreak) {
        ereport(WARNING, (errcode(ERRCODE_WARNING),
            errmsg("the given line number does not name a valid breakpoint."
            " Please use dbe_pldebugger.info_code for valid breakpoint candidates")));
        PG_RETURN_INT32(-1);
    }

    DebugClientInfo* client = u_sess->plsql_cxt.debug_client;
    StringInfoData str;
    initStringInfo(&str);
    appendStringInfo(&str, "%u:%d:%s", funcOid, lineno, cl.code);
    debug_client_send_msg(client, DEBUG_ADDBREAKPOINT_HEADER, str.data, str.len);
    debug_client_rec_msg(client);
    int32 ans = pg_strtoint32(client->rec_buffer);
    pfree(lines);
    PG_RETURN_INT32(ans);
}

/*
 *  dbe_pldebugger.delete_breakpoint
 *  delete a breakpoint
 */
Datum debug_client_delete_breakpoint(PG_FUNCTION_ARGS)
{
    InterfaceCheck("delete_breakpoint");
    int32 bpIndex = PG_GETARG_INT32(0);
    int32 ans = 0;
    DebugClientInfo* client = u_sess->plsql_cxt.debug_client;
    if (bpIndex < 0) {
        goto error;
    }

    StringInfoData str;
    initStringInfo(&str);
    appendStringInfo(&str, "%d", bpIndex);
    debug_client_send_msg(client, DEBUG_DELETEBREAKPOINT_HEADER, str.data, str.len);
    debug_client_rec_msg(client);
    ans = pg_strtoint32(client->rec_buffer);
    if (ans != 0) {
        goto error;
    }
    pfree(str.data);
    PG_RETURN_BOOL(true);

error:
    ereport(ERROR,
        (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_AMBIGUOUS_PARAMETER),
            errmsg("invalid break point index"),
            errdetail("the given index is either outside the range or already deleted"),
            errcause("try to delete a breakpoint that's never added"),
            erraction("use dbe_pldebugger.info_breakpoints() to show all valid breakpoints")));
    PG_RETURN_NULL();
}

/*
 *  dbe_pldebugger.info_breakpoints
 *  show all active breakpoints
 */
Datum debug_client_info_breakpoints(PG_FUNCTION_ARGS)
{
    InterfaceCheck("info_breakpoints");
    const int DEBUG_INFO_BP_TUPLE_ATTR_NUM = 4;

    FuncCallContext *funcctx = NULL;
    MemoryContext oldcontext;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 4 columns */
        tupdesc = CreateTemplateTupleDesc(DEBUG_INFO_BP_TUPLE_ATTR_NUM, false, TAM_HEAP);
        int i = 0;
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "breakpointno", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "funcoid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "lineno", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "query", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /* send msg & receive local breakpoints from debug server */
        debug_client_send_msg(u_sess->plsql_cxt.debug_client, DEBUG_BREAKPOINT_HEADER, NULL, 0);
        debug_client_rec_msg(u_sess->plsql_cxt.debug_client);

        /* total number of tuples to be returned */
        funcctx->user_fctx = debug_client_split_breakpoints_msg(&(funcctx->max_calls));
        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) { /* do when there is more left to send */
        Datum values[DEBUG_INFO_BP_TUPLE_ATTR_NUM];
        bool nulls[DEBUG_INFO_BP_TUPLE_ATTR_NUM];
        HeapTuple tuple;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        PLDebug_breakPoint* entry = (PLDebug_breakPoint*)funcctx->user_fctx + funcctx->call_cntr;

        int i = 0;
        values[i++] = Int32GetDatum(entry->bpIndex);
        values[i++] = ObjectIdGetDatum(entry->funcoid);
        values[i++] = Int32GetDatum(entry->lineno);
        if (entry->query == NULL) {
            nulls[i++] = true;
        } else {
            values[i++] = CStringGetTextDatum(entry->query);
        }
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}

static void* debug_client_split_localvariables_msg(uint32* num)
{
    char* msg = u_sess->plsql_cxt.debug_client->rec_buffer;
    Node* ret = (Node*)stringToNode(msg);
    if (ret == NULL || !(IsA(ret, List))) {
        *num = 0;
        return NULL;
    }
    List* list = (List*)ret;
    ListCell* lc = NULL;
    Size length = list_length(list);
    Size array_size = mul_size(sizeof(PLDebug_variable), length);
    PLDebug_variable* variables = (PLDebug_variable*)palloc0(array_size);
    PLDebug_variable* variable = NULL;
    int index = 0;
    foreach(lc, list) {
        Node* n = (Node*)lfirst(lc);
        if (!IsA(n, PLDebug_variable)) {
            goto error;
        }
        variable = variables + index;
        PLDebug_variable* var = (PLDebug_variable*)n;
        variable->type = var->type;
        variable->name = AssignStr(var->name);
        variable->var_type = AssignStr(var->var_type);
        variable->value = AssignStr(var->value);
        index++;
    }
    *num = length;
    return variables;

error:
    ereport(DEBUG1, (errmodule(MOD_PLDEBUGGER), errmsg("False output for variables type:\n%s", msg)));
    ereport(ERROR, (errmodule(MOD_PLDEBUGGER), errmsg("Get unexpected output for variables type.")));
    return NULL;
}

/*
 *  dbe_pldebugger.backtrace
 *  show backtrace of debug stacks
 */
Datum debug_client_backtrace(PG_FUNCTION_ARGS)
{
    InterfaceCheck("backtrace");
    const int DEBUG_BACKTRACE_ATTR_NUM = 4;

    FuncCallContext *funcctx = NULL;
    MemoryContext oldcontext;

    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 4 columns */
        tupdesc = CreateTemplateTupleDesc(DEBUG_BACKTRACE_ATTR_NUM, false, TAM_HEAP);
        int i = 0;
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "frameno", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "funcname", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "lineno", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "query", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        /* send msg & receive backtrace from debug server */
        debug_client_send_msg(u_sess->plsql_cxt.debug_client, DEBUG_BACKTRACE_HEADER, NULL, 0);
        debug_client_rec_msg(u_sess->plsql_cxt.debug_client);
        /* total number of tuples to be returned */
        funcctx->user_fctx = debug_client_split_backtrace_msg(&(funcctx->max_calls));
        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) { /* do when there is more left to send */
        Datum values[DEBUG_BACKTRACE_ATTR_NUM];
        bool nulls[DEBUG_BACKTRACE_ATTR_NUM];
        HeapTuple tuple;

        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        PLDebug_frame* entry = (PLDebug_frame*)funcctx->user_fctx + funcctx->call_cntr;

        int i = 0;
        /* to be consistent with gdb's back trace, flip the order of frameno */
        int frameno = funcctx->max_calls - entry->frameno - 1;
        values[i++] = Int32GetDatum(frameno);
        values[i++] = CStringGetTextDatum(entry->funcname);
        values[i++] = Int32GetDatum(entry->lineno);
        values[i++] = CStringGetTextDatum(entry->query);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}

/*
 *  dbe_pldebugger.step
 *  execute one statement. If the next statement is plpgsql function, start debugging inside.
 */
Datum debug_client_info_step(PG_FUNCTION_ARGS)
{
    InterfaceCheck("step");
    DebugClientInfo* client = u_sess->plsql_cxt.debug_client;
    debug_client_send_msg(u_sess->plsql_cxt.debug_client, DEBUG_STEP_INTO_HEADER, NULL, 0);
    debug_client_rec_msg(u_sess->plsql_cxt.debug_client);
    return get_tuple_lineno_and_query(client);
}

/*
 *  dbe_pldebugger.local_debug_server_info
 *  show all turn on'ed functions
 */
Datum local_debug_server_info(PG_FUNCTION_ARGS)
{
    InterfaceCheck("local_debug_server_info", false);
    FuncCallContext *funcctx = NULL;
    MemoryContext oldcontext;
    const int DEBUG_SERVER_INFO_TUPLE_ATTR_NUM = 3;
    /* stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function
         * calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* need a tuple descriptor representing 3 columns */
        int i = 0;
        tupdesc = CreateTemplateTupleDesc(DEBUG_SERVER_INFO_TUPLE_ATTR_NUM, false, TAM_HEAP);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "nodename", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "port", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "funcoid", OIDOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);
        /* total number of tuples to be returned */
        funcctx->user_fctx = get_debug_entries(&(funcctx->max_calls));
        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->user_fctx && funcctx->call_cntr < funcctx->max_calls) {
        Datum values[DEBUG_SERVER_INFO_TUPLE_ATTR_NUM];
        bool nulls[DEBUG_SERVER_INFO_TUPLE_ATTR_NUM];
        HeapTuple tuple;
        errno_t rc = 0;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");
        int i = 0;
        DebuggerServerInfo* entry = (DebuggerServerInfo*)funcctx->user_fctx + funcctx->call_cntr;
        values[i++] = CStringGetTextDatum(entry->nodename);
        values[i++] = Int8GetDatum(entry->port);
        values[i++] = Int16GetDatum(entry->funcoid);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}

/* turn_on */
Datum debug_server_turn_on(PG_FUNCTION_ARGS)
{
    InterfaceCheck("turn_on", false);
    /* return nodename & socket idx as port */
    int funcOid = PG_GETARG_OID(0);
    PlDebugEntry* entry = add_debug_func(funcOid);
    TupleDesc tupdesc;
    const int DEBUG_TURN_ON_ATTR_NUM = 2;
    int i = 0;
    tupdesc = CreateTemplateTupleDesc(DEBUG_TURN_ON_ATTR_NUM, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "nodename", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "port", INT4OID, -1, 0);
    TupleDesc tuple_desc = BlessTupleDesc(tupdesc);

    Datum values[DEBUG_TURN_ON_ATTR_NUM];
    bool nulls[DEBUG_TURN_ON_ATTR_NUM];
    HeapTuple tuple;
    errno_t rc = 0;
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    values[0] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
    values[1] = Int32GetDatum(entry->commIdx);
    tuple = heap_form_tuple(tuple_desc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum debug_server_turn_off(PG_FUNCTION_ARGS)
{
    InterfaceCheck("turn_off", false);
    int funcOid = PG_GETARG_OID(0);
    bool found = false;
    PlDebugEntry* entry = has_debug_func(funcOid, &found);
    if (!found) {
        ereport(WARNING, (errmodule(MOD_PLDEBUGGER),
                errmsg("function %d has not be turned on", funcOid)));
    } else {
        if (entry->func && entry->func->debug) {
            clean_up_debug_server(entry->func->debug, false, false);
        }
    }
    PG_RETURN_BOOL(delete_debug_func(funcOid));
}

static void InterfaceCheck(const char* funcname, bool needAttach)
{
#ifdef ENABLE_MULTIPLE_NODES
    PLDEBUG_FEATURE_NOT_SUPPORT_IN_DISTRIBUTED();
#endif
    if (!superuser() && !is_member_of_role(GetUserId(), DEFAULT_ROLE_PLDEBUGGER))
        ereport(ERROR, (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("must be system admin to execute dbe_pldebugger.%s", funcname))));
    if (u_sess->plsql_cxt.debug_client != NULL && needAttach){
        int commIdx = u_sess->plsql_cxt.debug_client->comm_idx;
        CHECK_DEBUG_COMM_VALID(commIdx);
        /* if current debug index is not myself during debug, clean up my self */
        PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[commIdx];
        if (debug_comm->clientId != u_sess->session_id) {
            clean_up_debug_client();
        }
    }
    if (needAttach && u_sess->plsql_cxt.debug_client == NULL) {
        ereport(ERROR,
            (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_TARGET_SERVER_NOT_ATTACHED),
                errmsg("must attach a execute func before execute dbe_pldebugger.%s", funcname),
                errdetail("execute func not attached before execute dbe_pldebugger.%s", funcname),
                errcause("target server not attached"),
                erraction("attach a execute func and retry")));
    }
}

static void debug_client_rec_msg(DebugClientInfo* client)
{
    CHECK_DEBUG_COMM_VALID(client->comm_idx);
    PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[client->comm_idx];

    MemoryContext old_context = MemoryContextSwitchTo(client->context);
    int len = 0;
    client->rec_ptr = 0;
    /* wait server's msg */
    WaitSendMsg(client->comm_idx, false);
    /* lock */
    AutoMutexLock debuglock(&debug_comm->mutex);
    debuglock.lock();
    RecvUnixMsg(client->comm_idx, (char*)&len, sizeof(int));
    client->rec_buffer = ResizeDebugBufferIfNecessary(client->rec_buffer, &client->rec_buf_len, len + 1);
    RecvUnixMsg(client->comm_idx, client->rec_buffer, len);
    client->rec_ptr = len;
    client->rec_buffer[len] = '\0';
    /* flushed invalid */
    debug_comm->hasServerFlushed = false;
    debug_comm->bufLen = 0;
    Assert(debug_comm->startPos == 0);
    /* unlock */
    debuglock.unLock();
    (void)MemoryContextSwitchTo(old_context);
}

static PlDebugEntry* add_debug_func(Oid key)
{
    if (unlikely(u_sess->plsql_cxt.debug_proc_htbl == NULL)) {
        init_pldebug_htcl();
    }

    (void)CheckPlpgsqlFunc(key);

    bool found = false;
    int commIdx = GetValidDebugCommIdx();
    if (commIdx == -1) {
        ereport(ERROR, (errmodule(MOD_PLDEBUGGER),
                (errmsg("max debug function is %d, turn_on function is out of range", PG_MAX_DEBUG_CONN))));
    }
    PlDebugEntry* entry = (PlDebugEntry*)hash_search(u_sess->plsql_cxt.debug_proc_htbl,
                                                     (void*)(&key), HASH_ENTER, &found);
    entry->key = key;
    if (!found) {
        entry->commIdx = commIdx;
        entry->func = NULL;
    } else {
        ReleaseDebugCommIdx(commIdx);
        ereport(ERROR, (errmodule(MOD_PLDEBUGGER),
                errmsg("function %d has already be turned on", key)));
    }
    return entry;
}

static void* get_debug_entries(uint32* num)
{
    if ((u_sess->plsql_cxt.debug_proc_htbl == NULL)) {
        *num = 0;
        return NULL;
    }
    /* no records, nothing to do. */
    if ((*num = hash_get_num_entries(u_sess->plsql_cxt.debug_proc_htbl)) <= 0) {
        return NULL;
    }

    PlDebugEntry* elem = NULL;
    DebuggerServerInfo* entry = NULL;
    Size array_size = mul_size(sizeof(DebuggerServerInfo), (Size)(*num));
    DebuggerServerInfo* entry_array = (DebuggerServerInfo*)palloc0(array_size);
    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, u_sess->plsql_cxt.debug_proc_htbl);
    int index = 0;

    /* Fetch all debugger entry info from the hash table */
    while ((elem = (PlDebugEntry*)hash_seq_search(&hash_seq)) != NULL) {
        entry = entry_array + index;
        entry->nodename = pstrdup(g_instance.attr.attr_common.PGXCNodeName);
        entry->port = elem->commIdx;
        entry->funcoid = elem->key;
        index++;
    }

    return entry_array;
}

static DebugClientInfo* InitDebugClient(int comm_idx)
{
    MemoryContext debug_context = AllocSetContextCreate(u_sess->cache_mem_cxt, "ClientDebugContext",
        ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext old_context = MemoryContextSwitchTo(debug_context);
    DebugClientInfo* debug_client = (DebugClientInfo*)palloc(sizeof(DebugClientInfo));
    debug_client->context = debug_context;
    debug_client->send_buf_len = DEFAULT_DEBUG_BUF_SIZE;
    debug_client->send_buffer = (char*)palloc0(sizeof(char) * debug_client->send_buf_len);
    debug_client->rec_buf_len = DEFAULT_DEBUG_BUF_SIZE;
    debug_client->rec_buffer = (char*)palloc0(sizeof(char) * debug_client->rec_buf_len);
    debug_client->send_ptr = 0;
    debug_client->rec_ptr = 0;
    debug_client->comm_idx = comm_idx;
    (void)MemoryContextSwitchTo(old_context);
    return debug_client;
}

static void* debug_client_split_breakpoints_msg(uint32* num)
{
    char* msg = u_sess->plsql_cxt.debug_client->rec_buffer;
    Node* ret = (Node*)stringToNode(msg);
    List* list = (List*)ret;
    ListCell* lc = NULL;
    Size length = list_length(list);
    Size array_size = mul_size(sizeof(PLDebug_breakPoint), length);
    PLDebug_breakPoint* bps = (PLDebug_breakPoint*)palloc0(array_size);
    PLDebug_breakPoint* bp = NULL;
    int index = 0;
    if (ret == NULL || !(IsA(ret, List))) {
        *num = 0;
        return bps;
    }

    foreach(lc, list) {
        Node* n = (Node*)lfirst(lc);
        if (!IsA(n, PLDebug_breakPoint)) {
            goto error;
        }
        bp = bps + index;
        PLDebug_breakPoint* b = (PLDebug_breakPoint*)n;
        bp->bpIndex = b->bpIndex;
        bp->funcoid = b->funcoid;
        bp->lineno = b->lineno;
        bp->query = pstrdup(b->query);
        index++;
    }
    *num = length;
    return bps;

error:
    ereport(DEBUG1, (errmodule(MOD_PLDEBUGGER), errmsg("False output for break point type:\n%s", msg)));
    ereport(ERROR, (errmodule(MOD_PLDEBUGGER), errmsg("Get unexpected output for breakpoints type.")));
    return NULL;
}


static void* debug_client_split_backtrace_msg(uint32* num)
{
    char* msg = u_sess->plsql_cxt.debug_client->rec_buffer;
    Node* ret = (Node*)stringToNode(msg);
    if (ret == NULL || !(IsA(ret, List))) {
        *num = 0;
        return NULL;
    }
    List* list = (List*)ret;
    ListCell* lc = NULL;
    Size length = list_length(list);
    Size array_size = mul_size(sizeof(PLDebug_frame), length);
    PLDebug_frame* frames = (PLDebug_frame*)palloc0(array_size);
    PLDebug_frame* frame = NULL;
    int index = 0;
    foreach(lc, list) {
        Node* n = (Node*)lfirst(lc);
        if (!IsA(n, PLDebug_frame)) {
            goto error;
        }
        frame = frames + index;
        PLDebug_frame* var = (PLDebug_frame*)n;
        frame->type = var->type;
        frame->frameno = var->frameno;
        frame->funcname = AssignStr(var->funcname);
        frame->lineno = var->lineno;
        frame->query = AssignStr(var->query);
        index++;
    }
    *num = length;
    return frames;

error:
    ereport(DEBUG1, (errmodule(MOD_PLDEBUGGER), errmsg("False output for backtrace frame type:\n%s", msg)));
    ereport(ERROR, (errmodule(MOD_PLDEBUGGER), errmsg("Get unexpected output for backtrace frame type.")));
    return NULL;
}

static List* collect_breakable_line_oid(Oid funcOid)
{
    /* only handle plpgsql function */
    if (!CheckPlpgsqlFunc(funcOid)) {
        return NIL;
    }
    /* trigger function is not supported */
    PLpgSQL_function* func = NULL;
    Oid rettype = get_func_rettype(funcOid);
    if (rettype == TRIGGEROID) {
        return NIL;
    }
    /* do the compilation */
    FunctionCallInfoData fake_fcinfo;
    FmgrInfo flinfo;
    errno_t rc = memset_s(&fake_fcinfo, sizeof(fake_fcinfo), 0, sizeof(fake_fcinfo));
    securec_check(rc, "", "");
    rc = memset_s(&flinfo, sizeof(flinfo), 0, sizeof(flinfo));
    securec_check(rc, "", "");
    fake_fcinfo.flinfo = &flinfo;
    fake_fcinfo.arg = (Datum*)palloc0(sizeof(Datum));
    fake_fcinfo.arg[0] = ObjectIdGetDatum(funcOid);
    flinfo.fn_oid = funcOid;
    flinfo.fn_mcxt = CurrentMemoryContext;
    _PG_init();
    PG_TRY();
    {
        func = plpgsql_compile(&fake_fcinfo, true);
    }
    PG_CATCH();
    {
        u_sess->plsql_cxt.plpgsql_curr_compile = NULL;
        u_sess->plsql_cxt.plpgsql_curr_compile_package = NULL;
        PG_RE_THROW();
    }
    PG_END_TRY();
    
    return collect_breakable_line(func);
}

CodeLine* debug_show_code_worker(Oid funcid, uint32* num, int* headerlines)
{
    /* Get the raw results */
    *headerlines = 0;
    char* funcdef = pg_get_functiondef_worker(funcid, headerlines);
    if (funcdef == NULL) {
        ereport(ERROR,
            (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_TARGET_SERVER_NOT_ATTACHED),
                errmsg("Unexpected NULL value for function definition"),
                errdetail("N/A"),
                errcause("Function definition is NULL"),
                erraction("Re-create the function and retry")));
    }

    /* Initialize returned list */
    int nLine = 0;
    for (unsigned int i = 0; i < strlen(funcdef); i++) {
        if (funcdef[i] == '\n') {
            nLine++;
        }
    }
    CodeLine* ret = (CodeLine*)palloc0(sizeof(CodeLine) * nLine);

    /* Process line number */
    int index = 0;
    int lineno = 1 - *headerlines;
    while (*funcdef != '\0') {
        char* eol = strchr(funcdef, '\n');
        if (eol != NULL) {
            *eol = '\0';
        }
        CodeLine* elem = ret + index;
        elem->lineno = lineno;
        elem->code = pstrdup(funcdef);
        elem->canBreak = false;
        index++;
        lineno++;
        funcdef = eol + 1; /* Move to next line */
    }

    *num = nLine;

    /* assign breakable attributes */
    List* breakables = collect_breakable_line_oid(funcid);
    ListCell* lc = NULL;
    foreach(lc, breakables) {
        int lineno = lfirst_int(lc);
        CodeLine* elem = ret + *headerlines + lineno - 1;
        elem->canBreak = true;
    }
    return ret;
}

static void debug_client_send_msg(DebugClientInfo* client, char first_char, char* msg, int msg_len)
{
    MemoryContext old_context = MemoryContextSwitchTo(client->context);
    const int EXTRA_LEN = 4;
    int buf_len = EXTRA_LEN + EXTRA_LEN + msg_len;
    client->send_buffer = ResizeDebugBufferIfNecessary(client->send_buffer, &(client->send_buf_len), buf_len + 1);

    client->send_ptr = 0;
    client->send_buffer[client->send_ptr++] = first_char;

    int rc = 0;
    rc = memcpy_s(client->send_buffer + client->send_ptr, client->send_buf_len - client->send_ptr, &msg_len, EXTRA_LEN);
    securec_check(rc, "\0", "\0");
    client->send_ptr += EXTRA_LEN;
    if (msg_len > 0) {
        rc = memcpy_s(client->send_buffer + client->send_ptr, client->send_buf_len - client->send_ptr, msg, msg_len);
        securec_check(rc, "\0", "\0");
        client->send_ptr += msg_len;
    }
    (void)MemoryContextSwitchTo(old_context);

    CHECK_DEBUG_COMM_VALID(client->comm_idx);
    PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[client->comm_idx];
    /* lock */
    (void)MemoryContextSwitchTo(g_instance.pldebug_cxt.PldebuggerCxt);
    AutoMutexLock debuglock(&debug_comm->mutex);
    debuglock.lock();
    Assert(debug_comm->hasClientFlushed == false);
    debug_comm->hasClientFlushed = false;
    SendUnixMsg(client->comm_idx, client->send_buffer, client->send_ptr);
    /* unlock */
    debuglock.unLock();
    /* wake up server */
    if (!WakeUpReceiver(client->comm_idx, true)) {
        ereport(ERROR, (errmodule(MOD_PLDEBUGGER),
                errmsg("fail to send msg from debug client to debug server.")));
    }
    (void)MemoryContextSwitchTo(old_context);
}

/* debug_proc_htbl function */
static void init_pldebug_htcl()
{
    if (u_sess->plsql_cxt.debug_proc_htbl)
        return;

    HASHCTL ctl;
    errno_t rc = EOK;
    MemoryContext context = AllocSetContextCreate(u_sess->cache_mem_cxt,
                                                  "DebugHashtblContext",
                                                  ALLOCSET_SMALL_MINSIZE,
                                                  ALLOCSET_SMALL_INITSIZE,
                                                  ALLOCSET_DEFAULT_MAXSIZE);

    const int debugSize = 64;
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(PlDebugEntry);
    ctl.hash = uint32_hash;
    ctl.hcxt = context;
    u_sess->plsql_cxt.debug_proc_htbl =
        hash_create("Debug Func Table", debugSize, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

static bool CheckPlpgsqlFunc(Oid funcoid, bool report_error)
{
    char* langname = get_func_langname(funcoid);
    if (strcmp(langname, "plpgsql") != 0) {
        if (report_error) {
            ereport(ERROR,
                (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("pl debugger only support function with language plpgsql"),
                    errdetail("the given language name of function is %s", langname),
                    errcause("pl debugger do not support the given function"),
                    erraction("use pl debugger with only plpgsql function")));
        }
        pfree(langname);
        return false;
    }
    pfree(langname);
    return true;
}

static void collect_breakable_line_walker(const List* stmts, List** lines)
{
    if (stmts == NIL) {
        return;
    }

    ListCell* lc = NULL;
    foreach (lc, stmts) {
        PLpgSQL_stmt* stmt = (PLpgSQL_stmt*)lfirst(lc);
        int lineno = stmt->lineno;
        if (lineno != 0) {
            *lines = lappend_int(*lines, lineno);
        }
        switch ((enum PLpgSQL_stmt_types)stmt->cmd_type) {
            case PLPGSQL_STMT_BLOCK:
                collect_breakable_line_walker(((PLpgSQL_stmt_block*)stmt)->body, lines);
                break;

            case PLPGSQL_STMT_IF: {
                PLpgSQL_stmt_if* if_stmt = (PLpgSQL_stmt_if*)stmt;
                ListCell* lc2 = NULL;
                collect_breakable_line_walker(if_stmt->then_body, lines);
                foreach (lc2, if_stmt->elsif_list) {
                    PLpgSQL_if_elsif* elif = (PLpgSQL_if_elsif*)lfirst(lc2);
                    collect_breakable_line_walker(elif->stmts, lines);
                }
                collect_breakable_line_walker(if_stmt->else_body, lines);
                break;
            }

            case PLPGSQL_STMT_CASE: {
                PLpgSQL_stmt_case* case_stmt = (PLpgSQL_stmt_case*)stmt;
                ListCell* lc2 = NULL;
                foreach (lc2, case_stmt->case_when_list) {
                    PLpgSQL_case_when* cwt = (PLpgSQL_case_when*)lfirst(lc2);
                    collect_breakable_line_walker(cwt->stmts, lines);
                }
                collect_breakable_line_walker(case_stmt->else_stmts, lines);
                break;
            }

            case PLPGSQL_STMT_LOOP: 
                collect_breakable_line_walker(((PLpgSQL_stmt_loop*)stmt)->body, lines);
                break;

            case PLPGSQL_STMT_WHILE:
                collect_breakable_line_walker(((PLpgSQL_stmt_while*)stmt)->body, lines);
                break;

            case PLPGSQL_STMT_FORI:
                collect_breakable_line_walker(((PLpgSQL_stmt_fori*)stmt)->body, lines);
                break;

            case PLPGSQL_STMT_FORS:
                collect_breakable_line_walker(((PLpgSQL_stmt_forq*)stmt)->body, lines);
                break;

            case PLPGSQL_STMT_FORC:
                collect_breakable_line_walker(((PLpgSQL_stmt_forq*)stmt)->body, lines);
                break;

            case PLPGSQL_STMT_FOREACH_A:
                collect_breakable_line_walker(((PLpgSQL_stmt_foreach_a*)stmt)->body, lines);
                break;

            case PLPGSQL_STMT_GOTO:
            case PLPGSQL_STMT_ASSIGN:
            case PLPGSQL_STMT_PERFORM:
            case PLPGSQL_STMT_GETDIAG:
            case PLPGSQL_STMT_EXIT:
            case PLPGSQL_STMT_RETURN:
            case PLPGSQL_STMT_RETURN_NEXT:
            case PLPGSQL_STMT_RETURN_QUERY:
            case PLPGSQL_STMT_RAISE:
            case PLPGSQL_STMT_EXECSQL:
            case PLPGSQL_STMT_DYNEXECUTE:
            case PLPGSQL_STMT_DYNFORS:
            case PLPGSQL_STMT_OPEN: 
            case PLPGSQL_STMT_FETCH:
            case PLPGSQL_STMT_CLOSE:
            case PLPGSQL_STMT_COMMIT:
            case PLPGSQL_STMT_ROLLBACK:
            case PLPGSQL_STMT_NULL:
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_PLSQL),
                        errmsg("unrecognized statement type: %d for PLSQL function.", stmt->cmd_type)));
                break;
        }
    }
}

static List* collect_breakable_line(PLpgSQL_function* func)
{
    if (func == NULL) {
        return NIL;
    }
    List* lines = NIL;
    PLpgSQL_stmt_block* block = func->action;
    collect_breakable_line_walker(block->body, &lines);
    return lines;
}
