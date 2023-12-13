/* -------------------------------------------------------------------------
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * pl_debugger.cpp      - debug functions for the PL/pgSQL
 *            procedural language
 *
 * IDENTIFICATION
 *   src/common/pl/plpgsql/src/pl_debugger.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "utils/plpgsql_domain.h"
#include "catalog/namespace.h"
#include "commands/copy.h"
#include "funcapi.h"
#include "optimizer/pgxcship.h"
#include "nodes/readfuncs.h"
#include "utils/plpgsql.h"
#include "utils/memutils.h" 
#include "utils/syscache.h"
#include "utils/builtins.h"
#include <sys/time.h>
#include "catalog/gs_package.h"

const char* PG_DEBUG_FILES_DIR = "base/plpgsql_debug";

const char* DEFAULT_UNKNOWN_VALUE = "<UNKNOWN>";

#define IS_AFTER_OPT(opt) (((opt) <= 'Z') && ((opt) >= 'A'))

static void server_debug_main(PLpgSQL_function* func, PLpgSQL_execstate* estate);
static void init_debug_server(PLpgSQL_function* func, int socketId, int debugStackIdx, PLpgSQL_execstate* estate);

static bool handle_debug_msg(DebugInfo* debug, char* firstChar, PLpgSQL_execstate* estate);

static char* get_stmt_query(PLpgSQL_stmt* stmt);
static bool get_cur_info(StringInfo str, PLpgSQL_execstate* estate, DebugInfo* debug);
static bool send_cur_info(DebugInfo* debug, PLpgSQL_execstate* estate, bool stop_next);
static bool ActiveBPInFunction(DebugInfo* debug, Oid funcOid);
static bool IsBreakPointExisted(DebugInfo* debug, Oid funcOid, int lineno, bool ignoreDisabled);
static char* ResizeDebugCommBufferIfNecessary(char* buffer, int* oldSize, int needSize);
static void set_debugger_procedure_state(int commIdx, bool state);

/* send/rec msg for server */
static void debug_server_rec_msg(DebugInfo* debug, char* firstChar);
static void debug_server_send_msg(DebugInfo* debug, const char* msg, int msg_len);
/* server manage client msg */
static void debug_server_attach(DebugInfo* debug, PLpgSQL_execstate* estate);
static void debug_server_local_variables(DebugInfo* debug);
static void debug_server_abort(DebugInfo* debug);
static void debug_server_add_breakpoint(DebugInfo* debug);
static void debug_server_delete_breakpoint(DebugInfo* debug);
static void debug_server_enable_breakpoint(DebugInfo* debug);
static void debug_server_disable_breakpoint(DebugInfo* debug);
static void debug_server_info_breakpoint(DebugInfo* debug);
static void debug_server_backtrace();
static void debug_server_set_variable(DebugInfo* debug, PLpgSQL_execstate* estate);
static void debug_server_info_code(DebugInfo* debug);
static char* debug_server_add_breakpoint_invalid(DebugInfo* debug, bool* valid, int lineno, int* newIndex, char* query, char* maskquery);

/* close each debug function's resource if necessary */
void PlDebugerCleanUp(int code, Datum arg)
{
    if (u_sess->plsql_cxt.debug_proc_htbl != NULL) {
        HASH_SEQ_STATUS seq;
        PlDebugEntry *entry = NULL;
        hash_seq_init(&seq, u_sess->plsql_cxt.debug_proc_htbl);
        while ((entry = (PlDebugEntry *)hash_seq_search(&seq)) != NULL) {
            if (entry->func && entry->func->debug) {
                clean_up_debug_server(entry->func->debug, true, true);
            } else {
                ReleaseDebugCommIdx(entry->commIdx);
            }
        }
        hash_destroy(u_sess->plsql_cxt.debug_proc_htbl);
    }
    clean_up_debug_client(true);
}

static void init_debug_server(PLpgSQL_function* func, int socketId, int debugStackIdx, PLpgSQL_execstate* estate)
{
    Assert(func->debug == NULL);
    MemoryContext debug_context = NULL;
    /* Initialize context only for outermost debug stack */
    if (debugStackIdx == 0) {
        debug_context = AllocSetContextCreate(func->fn_cxt,
                                              "ClientDebugContext",
                                              ALLOCSET_SMALL_MINSIZE,
                                              ALLOCSET_SMALL_INITSIZE,
                                              ALLOCSET_DEFAULT_MAXSIZE);
    } else {
        Assert(u_sess->plsql_cxt.cur_debug_server != NULL);
        debug_context = u_sess->plsql_cxt.cur_debug_server->debug_cxt;
    }
    MemoryContext old_context = MemoryContextSwitchTo(debug_context);
    DebugInfo* debug = (DebugInfo*)palloc0(sizeof(DebugInfo));
    debug->debug_cxt = debug_context;
    debug->stop_next_stmt = true;
    debug->cur_opt = DEBUG_NOTHING_HEADER;
    debug->debugStackIdx = debugStackIdx;
    debug->debugCallback = server_debug_main;
    debug->func = func;
    debug->bp_list = NULL;
    debug->comm = NULL;
    debug->inner_called_debugger = NULL;
    debug->cur_stmt = NULL;
    debug->estate = estate;
    /* create connection if is base function */
    if (socketId >= 0) {
        /*
          DebugInfoComm's memory context is always on the first DebugInfo's context.
          Other DebugInfoComm on the list are reference of the first one.
        */
        DebugInfoComm* sock = (DebugInfoComm*)palloc0(sizeof(DebugInfoComm));
        sock->comm_idx = socketId;
        sock->send_buffer = (char*)palloc0(DEFAULT_DEBUG_BUF_SIZE * sizeof(char));
        sock->send_buf_len = DEFAULT_DEBUG_BUF_SIZE;
        sock->send_buffer = (char*)palloc0(DEFAULT_DEBUG_BUF_SIZE * sizeof(char));
        sock->send_buf_len = DEFAULT_DEBUG_BUF_SIZE;
        sock->send_ptr = 0;
        sock->rec_buffer = (char*)palloc0(DEFAULT_DEBUG_BUF_SIZE * sizeof(char));
        sock->rec_buf_len = DEFAULT_DEBUG_BUF_SIZE;
        sock->rec_ptr = 0;
        debug->comm = sock;
    }
    func->debug = debug;
    (void)MemoryContextSwitchTo(old_context);
}

void check_debug(PLpgSQL_function* func, PLpgSQL_execstate* estate)
{
    bool found = false;
    bool need_continue_into = u_sess->plsql_cxt.cur_debug_server != NULL &&
        ActiveBPInFunction(u_sess->plsql_cxt.cur_debug_server, func->fn_oid);
    bool is_stepinto = u_sess->plsql_cxt.has_step_into;
    PlDebugEntry* entry = has_debug_func(func->fn_oid, &found);
    if ((found && u_sess->plsql_cxt.cur_debug_server == NULL) || is_stepinto || need_continue_into) {
#ifndef ENABLE_MULTIPLE_NODES
        if (func->action->isAutonomous) {
            ereport(ERROR, (errmodule(MOD_PLDEBUGGER),
                            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Un-support feature"),
                            errdetail("Pldebug is not supported for AutonomousTransaction currently.")));
        }
#endif
        int stackIdx = 0;
        /* get last stack debug func */
        DebugInfo* debug = u_sess->plsql_cxt.cur_debug_server;
        DebugInfo* outer_debug = debug;
        while (debug != NULL) {
            outer_debug = debug;
            debug = debug->inner_called_debugger;
        }
        if (outer_debug) {
            stackIdx = outer_debug->debugStackIdx + 1;
            Assert(stackIdx > 0);
            /* reset outer func debug state */
            outer_debug->cur_opt = DEBUG_NOTHING_HEADER;
        }
        int socketIdx = found ? entry->commIdx : -1;
        if (func->debug == NULL) {
            init_debug_server(func, socketIdx, stackIdx, estate);
        }
        
        if (stackIdx > 0) {
            func->debug->comm = outer_debug->comm;
            func->debug->bp_list = outer_debug->bp_list;
            outer_debug->inner_called_debugger = func->debug;
            /* set inner debug's state */
            if (is_stepinto) {
                func->debug->cur_opt = DEBUG_STEP_INTO_HEADER_AFTER;
            } else if (need_continue_into) {
                func->debug->cur_opt = DEBUG_CONTINUE_HEADER_AFTER;
            }
        } else {
            if (entry != NULL) {
                entry->func = func;
            }
            /* maintain session's debug server is on base turn on function */
            u_sess->plsql_cxt.cur_debug_server = func->debug;
        }
        func->debug->stop_next_stmt = (need_continue_into && !is_stepinto) ? false : true;
    }
}

static void set_debugger_procedure_state(int commIdx, bool state)
{
    PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[commIdx];
    AutoMutexLock debuglock(&debug_comm->mutex);
    debuglock.lock();
    bool start = ((debug_comm->isProcdeureRunning == false) && (state == true));
    debug_comm->isProcdeureRunning = state;
    /* reset flags for start execute procedure */
    if (start) {
        debug_comm->hasClientFlushed = false;
        debug_comm->hasServerFlushed = false;
        debug_comm->IsServerWaited = false;
        debug_comm->IsClientWaited = false;
        debug_comm->hasClientFlushed = false;
        debug_comm->hasServerFlushed = false;
        debug_comm->hasServerErrorOccured = false;
        debug_comm->hasClientErrorOccured = false;
        debug_comm->clientId = 0;
        debug_comm->bufLen = 0;
    }
    debuglock.unLock();
}

/* main callback hook */
void server_debug_main(PLpgSQL_function* func, PLpgSQL_execstate* estate)
{
    DebugInfo* debug_ptr = func->debug;
    debug_ptr->cur_stmt = estate->err_stmt;
    PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[debug_ptr->comm->comm_idx];
    Assert(debug_ptr != NULL);
    /* stop to wait client conn if need */
    debug_ptr->stop_next_stmt = debug_ptr->stop_next_stmt ||
        IsBreakPointExisted(debug_ptr, func->fn_oid, estate->err_stmt->lineno, true);
    if (debug_ptr->stop_next_stmt) {
        MemoryContext old_cxt = MemoryContextSwitchTo(debug_ptr->debug_cxt);
        PG_TRY();
        {
            bool need_wait = true;
            /* make sure comm state */
            if (!debug_comm->isRunning())
                set_debugger_procedure_state(debug_ptr->comm->comm_idx, true);
            while (need_wait) {
                if (!IS_AFTER_OPT(debug_ptr->cur_opt)) {
                    char firstChar;
                    /* wait for msg */
                    debug_server_rec_msg(func->debug, &firstChar);
                    debug_ptr->cur_opt = firstChar;
                }
                need_wait = handle_debug_msg(func->debug, &debug_ptr->cur_opt, estate);
            }
            MemoryContextSwitchTo(old_cxt);
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(old_cxt);
            PG_RE_THROW();
        }
        PG_END_TRY();
    }
}

bool handle_debug_msg(DebugInfo* debug, char* firstChar, PLpgSQL_execstate* estate)
{
    bool need_wait = true;
    switch (*firstChar) {
        case DEBUG_ATTACH_HEADER:
            debug_server_attach(debug, estate);
            /* if already get to function's end when attach, no need to wait for next command. */
            need_wait = debug->stop_next_stmt ? true : false;
            break;
        case DEBUG_LOCALS_HEADER:
            debug_server_local_variables(debug);
            need_wait = true;
            break;
        case DEBUG_NEXT_HEADER:
            /* wait after execute this sql */
            debug->stop_next_stmt = true;
            need_wait = false;
            *firstChar = DEBUG_NEXT_HEADER_AFTER;
            break;
        case DEBUG_NEXT_HEADER_AFTER:
            *firstChar = DEBUG_NOTHING_HEADER;
            need_wait = !send_cur_info(debug, estate, true);
            break;
        case DEBUG_STEP_INTO_HEADER:
            /* calling inner func if has one */
            debug->stop_next_stmt = true;
            u_sess->plsql_cxt.has_step_into = true;
            need_wait = false;
            *firstChar = DEBUG_STEP_INTO_HEADER_AFTER;
            break;
        case DEBUG_STEP_INTO_HEADER_AFTER:
            /* if don't have inner func, same as next; if is inner func, return first line like attach */
            *firstChar = DEBUG_NOTHING_HEADER;
            u_sess->plsql_cxt.has_step_into = false;
            need_wait = !send_cur_info(debug, estate, true);
            break;
        case DEBUG_FINISH_HEADER:
            debug->stop_next_stmt = false;
            need_wait = false;
            *firstChar = DEBUG_FINISH_HEADER_AFTER;
            break;
        case DEBUG_ABORT_HEADER:
            /* set abort flag incase has outter function */
            *firstChar = DEBUG_ABORT_HEADER_AFTER;
            debug_server_abort(debug);
            need_wait = false;
            break;
        case DEBUG_ABORT_HEADER_AFTER:
            *firstChar = DEBUG_ABORT_HEADER_AFTER;
            /* already send close to client, no need to send again, just throw error is enough */
            ereport(ERROR, (errmodule(MOD_PLDEBUGGER), (errmsg("receive abort message"))));
            need_wait = false;
            break;
        case DEBUG_CONTINUE_HEADER:
            debug->stop_next_stmt = false;
            need_wait = false;
            *firstChar = DEBUG_CONTINUE_HEADER_AFTER;
            break;
        case DEBUG_FINISH_HEADER_AFTER:
        case DEBUG_CONTINUE_HEADER_AFTER:
            *firstChar = DEBUG_NOTHING_HEADER;
            need_wait = !send_cur_info(debug, estate, false);
            break;
        case DEBUG_ADDBREAKPOINT_HEADER:
            debug_server_add_breakpoint(debug);
            break;
        case DEBUG_DELETEBREAKPOINT_HEADER:
            debug_server_delete_breakpoint(debug);
            break;
        case DEBUG_ENABLEBREAKPOINT_HEADER:
            debug_server_enable_breakpoint(debug);
            break;
        case DEBUG_DISABLEBREAKPOINT_HEADER:
            debug_server_disable_breakpoint(debug);
            break;
        case DEBUG_BREAKPOINT_HEADER:
            debug_server_info_breakpoint(debug);
            break;
        case DEBUG_BACKTRACE_HEADER:
            debug_server_backtrace();
            break;
        case DEBUG_SET_VARIABLE_HEADER:
            debug_server_set_variable(debug, estate);
            break;
        case DEBUG_INFOCODE_HEADER:
            debug_server_info_code(debug);
            break;
        default:
            ereport(ERROR, (errmodule(MOD_PLDEBUGGER),
                            (errmsg("received unknown plsql debug client msg: %c", *firstChar))));
            break;
    }
    return need_wait;
}

void server_pass_upper_debug_opt(DebugInfo* debug)
{
    if (IS_AFTER_OPT(debug->cur_opt) && debug != u_sess->plsql_cxt.cur_debug_server) {
        Assert(u_sess->plsql_cxt.cur_debug_server != NULL);
        /* get last stack debug func */
        DebugInfo* outer_debug = u_sess->plsql_cxt.cur_debug_server;
        while (outer_debug != NULL && outer_debug->inner_called_debugger != debug) {
            outer_debug = outer_debug->inner_called_debugger;
        }
        if (outer_debug == NULL) {
            ereport(ERROR,
                (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("Get null pointer for outer debugger when passing debug option"),
                    errdetail("N/A"),
                    errcause("Debugger info stack contains error."),
                    erraction("Contact Huawei Engineer.")));
        }
        outer_debug->cur_opt = debug->cur_opt;
        if (debug->cur_opt == DEBUG_FINISH_HEADER_AFTER) {
            outer_debug->stop_next_stmt = true;
        }
    }
}

void server_send_end_msg(DebugInfo* debug)
{
    if (debug == u_sess->plsql_cxt.cur_debug_server && IS_AFTER_OPT(debug->cur_opt)) {
        MemoryContext old_cxt = MemoryContextSwitchTo(debug->debug_cxt);
        StringInfoData str;
        initStringInfo(&str);
        Oid funcoid = debug->func->fn_oid;
        Oid pkgoid = debug->func->pkg_oid;
        char* funcname = NULL;
        if (OidIsValid(funcoid)) {
            funcname = get_func_name(funcoid);
        } else {
            funcname = pstrdup("anonymous block");
        }
        char* pkgname = NULL;
        Assert(funcname != NULL);
        if (pkgoid != InvalidOid) {
            pkgname = GetPackageName(pkgoid);
        }
        char* pkgfuncname = quote_qualified_identifier(pkgname, funcname);
        appendStringInfo(&str, "%u:%s:%d:%s", funcoid, pkgfuncname, 0, "[EXECUTION FINISHED]");
        /* set procedure end */
        set_debugger_procedure_state(debug->comm->comm_idx, false);
        debug_server_send_msg(debug, str.data, str.len);
        MemoryContextSwitchTo(old_cxt);
        debug->cur_opt = DEBUG_NOTHING_HEADER;
        pfree_ext(funcname);
        pfree_ext(pkgfuncname);
    }
}

void debug_server_attach(DebugInfo* debug, PLpgSQL_execstate* estate)
{
    MemoryContext old_context = MemoryContextSwitchTo(debug->debug_cxt);
    /* Received buffer will be in the form of <commidx:client session id> */
    char* psave = NULL;
    char* fir = strtok_r(debug->comm->rec_buffer, ":", &psave);
    const int int64Size = 10;
    char* new_fir = TrimStr(fir);
    if (new_fir == NULL) {
        ReportInvalidMsg(debug->comm->rec_buffer);
        return;
    }
    int sockId = pg_strtoint32(new_fir);
    bool valid = (sockId == debug->comm->comm_idx);
    if (!valid) {
        /* wrong comm index, should not be here */
        ereport(ERROR, (errmodule(MOD_PLDEBUGGER),
                    (errmsg("received wrong comm index: %d", sockId))));
    } else {
        debug->stop_next_stmt = true;
    }
    fir = strtok_r(NULL, ":", &psave);
    new_fir = TrimStr(fir);
    if (new_fir == NULL) {
        ReportInvalidMsg(debug->comm->rec_buffer);
        return;
    }
    MemoryContextSwitchTo(old_context);
    uint64 clientSessionId = (uint64)pg_strtouint64(new_fir, NULL, int64Size);
    /* set comm's session id */
    PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[debug->comm->comm_idx];
    AutoMutexLock debuglock(&debug_comm->mutex);
    debuglock.lock();
    debug_comm->clientId = clientSessionId;
    debuglock.unLock();

    StringInfoData str;
    initStringInfo(&str);
    get_cur_info(&str, estate, debug);
    debug_server_send_msg(debug, str.data, str.len);
    pfree_ext(new_fir);
}

static bool is_target_variable(const char* target, const char* this_var)
{
    /* [no target] OR [the two are the same] */
    return (target == NULL) || (this_var != NULL && !strcmp(target, this_var));
}

PLDebug_variable* get_debug_variable_var(PLpgSQL_var* node, const char* target)
{
    char* varname = node->varname == NULL || node->varname[0] == '\0' ? node->refname : node->varname;
    if (!is_target_variable(target, varname)) {
        return NULL;
    }

    if (node->datatype == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmodule(MOD_PLDEBUGGER),
                errmsg("plpgsql variable has NULL type.")));
    }

    PLDebug_variable* var = NULL;
    HeapTuple tuple;
    Form_pg_type form;

    /* Find the type of simple variable */
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(node->datatype->typoid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", node->datatype->typoid)));
    }
    form = (Form_pg_type)GETSTRUCT(tuple);
    var = (PLDebug_variable*)makeNode(PLDebug_variable);
    var->name = AssignStr(varname);
    var->var_type = AssignStr(NameStr(form->typname));
    if (node->value == (uintptr_t)NULL && form->typbyval == false) { /* type treated as pointers */
        var->value = pstrdup("");
    } else {
        var->value = OidOutputFunctionCall(form->typoutput, node->value);
    }
    if (node->ispkg && node->pkg != NULL) {
        char* pkgName = GetPackageName(node->pkg->pkg_oid);
        var->pkgname = AssignStr(pkgName);
    } else {
        var->pkgname = pstrdup("");
    }
    
    var->isconst = node->isconst;
    ReleaseSysCache(tuple);
    return var;
}

PLDebug_variable* get_debug_variable_row(PLpgSQL_row* node, PLpgSQL_execstate* estate, const char* target)
{
    char* varname = node->varname == NULL ? node->refname : node->varname;
    if (!is_target_variable(target, varname)) {
        return NULL;
    }

    /* no need to show internal variable */
    const char* internal = "*internal*";
    if (node->refname != NULL && strcmp(varname, internal) == 0) {
        return NULL;
    }

    PLDebug_variable* var = (PLDebug_variable*)makeNode(PLDebug_variable);
    var->isconst = false;
    var->name = AssignStr(varname);
    switch (node->dtype) {
        case PLPGSQL_DTYPE_ROW:
            var->var_type = pstrdup("Row");
            break;
        case PLPGSQL_DTYPE_RECORD:
            var->var_type = pstrdup("Record");
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_PLDEBUGGER),
                    errmsg("unrecognized data type: %d when gathering variable info for debugger.", node->dtype)));
    }
    if (node->rowtupdesc == NULL) {
        var->value = pstrdup(DEFAULT_UNKNOWN_VALUE);
    } else {
        StringInfo buf = makeStringInfo();
        HeapTuple tuple = make_tuple_from_row(estate, node, node->rowtupdesc);
        if (!HeapTupleIsValid(tuple)) {
            ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmodule(MOD_PLDEBUGGER),
                    errmsg("row not compatible with its own tupdesc in RETURN statement.")));
        }
        appendStringInfoChar(buf, '[');
        for (int i = 0; i < node->rowtupdesc->natts; i++) {
            char* val = SPI_getvalue(tuple, node->rowtupdesc, i + 1);
            appendStringInfo(buf, " %s,", val);
        }
        appendStringInfoChar(buf, ']');
        var->value = pstrdup(buf->data);
        heap_freetuple_ext(tuple);
        pfree(buf);
    }
    if (node->ispkg && node->pkg != NULL) {
        char* pkgName = GetPackageName(node->pkg->pkg_oid);
        var->pkgname = AssignStr(pkgName);
    } else {
        var->pkgname = pstrdup("");
    }
    return var;
}

PLDebug_variable* get_debug_variable_rec(PLpgSQL_rec* node, const char* target)
{
    char* varname = node->varname == NULL ? node->refname : node->varname;
    if (!is_target_variable(target, varname)) {
        return NULL;
    }

    PLDebug_variable* var = (PLDebug_variable*)makeNode(PLDebug_variable);
    var->isconst = false;
    var->name = AssignStr(varname);
    var->var_type = pstrdup("Rec");
    if (node->tupdesc == NULL || node->tup == NULL) {
        var->value = pstrdup(DEFAULT_UNKNOWN_VALUE);
    } else {
        StringInfo buf = makeStringInfo();
        HeapTuple tuple = node->tup;
        appendStringInfoChar(buf, '[');
        for (int i = 0; i < node->tupdesc->natts; i++) {
            char* val = SPI_getvalue(tuple, node->tupdesc, i + 1);
            appendStringInfo(buf, " %s,", val);
        }
        appendStringInfoChar(buf, ']');
        var->value = pstrdup(buf->data);
        pfree(buf);
    }
    if (node->ispkg && node->pkg != NULL) {
        char* pkgName = GetPackageName(node->pkg->pkg_oid);
        var->pkgname = AssignStr(pkgName);
    } else {
        var->pkgname = pstrdup("");
    }
    return var;
}

PLDebug_variable* get_debug_variable_refcursor(PLpgSQL_datum** node, int index, const char* target)
{
    char* varname = ((PLpgSQL_var*)node[index])->varname == NULL ?
        ((PLpgSQL_var*)node[index])->refname : ((PLpgSQL_var*)node[index])->varname;
    if (!is_target_variable(target, varname)) {
        return NULL;
    }
    PLDebug_variable* var = (PLDebug_variable*)makeNode(PLDebug_variable);
    var->isconst = false;
    PLDebug_variable* refcursor = get_debug_variable_var((PLpgSQL_var*)node[index++], NULL);
    PLDebug_variable* is_open = get_debug_variable_var((PLpgSQL_var*)node[index++], NULL);
    PLDebug_variable* found = get_debug_variable_var((PLpgSQL_var*)node[index++], NULL);
    PLDebug_variable* not_found = get_debug_variable_var((PLpgSQL_var*)node[index++], NULL);
    PLDebug_variable* row_count = get_debug_variable_var((PLpgSQL_var*)node[index], NULL);
    var->name = pstrdup(varname);
    var->var_type = pstrdup(refcursor->var_type);
    StringInfo buf = makeStringInfo();
    appendStringInfo(buf, "{name:\t%s\nis_open:\t%s\nfound:\t%s\nnot_found:\t%s\nrow_count:\t%s}",
        refcursor->value, is_open->value, found->value, not_found->value, row_count->value);
    var->value = buf->data;
    var->pkgname = pstrdup(refcursor->pkgname);

    pfree(refcursor);
    pfree(is_open);
    pfree(found);
    pfree(not_found);
    pfree(row_count);
    return var;
}

static PLpgSQL_execstate* GetEstateAtDepth(int n)
{
    DebugInfo* target = u_sess->plsql_cxt.cur_debug_server;
    DebugInfo* top = target;
    /* get n'th stack from lowest stack */
    for (int i = 0; i < n; i++) {
        top = top->inner_called_debugger;
        if (top == NULL) {
            return NULL;
        }
    }

    /* proceed both frame till top reaches outer most */
    while (top->inner_called_debugger != NULL) {
        top = top->inner_called_debugger;
        target = target->inner_called_debugger;
    }
    return target->estate;
}

static void report_msg_error(const char* msg)
{
    ereport(ERROR,
        (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
            errmsg("Unexpeted message for debug server info print_var/info_locals"),
            errdetail("error message: %s", msg),
            errcause("The message is not complete"),
            erraction("Contact Huawei Engineer.")));
}

void debug_server_local_variables(DebugInfo* debug)
{
    /* Received buffer will be in the form of <varname:framno> */
    PLpgSQL_execstate* estate = NULL;
    char* var_name = NULL;
    int frameno = -1;
    char* psave = NULL;
    char* fir = strtok_r(debug->comm->rec_buffer, ":", &psave);
    bool show_all = false;
    var_name = pstrdup(TrimStr(fir));
    if (psave == NULL) {
        report_msg_error(debug->comm->rec_buffer);
    }
    fir = strtok_r(NULL, ":", &psave);
    if (fir == NULL) {
        report_msg_error(debug->comm->rec_buffer);
    }
    frameno = pg_strtoint32(fir);
    if (frameno == 0) {
        estate = debug->estate;
    } else {
        estate = GetEstateAtDepth(frameno);
    }
    if (psave == NULL) {
        report_msg_error(debug->comm->rec_buffer);
    }
    show_all = (pg_strtoint32(psave) == 1);
    if (show_all) {
        /* empty var_name will show all variables */
        pfree_ext(var_name);
    }

    if (estate == NULL) {
        char* errmsg = (char*)palloc(MAX_INT_SIZE);
        pg_ltoa(DEBUG_SERVER_PRINT_VAR_FRAMENO_EXCEED, errmsg);
        debug_server_send_msg(debug, errmsg, strlen(errmsg));
        pfree(errmsg);
        return;
    }

    List* local_variables = NIL;
    for (int i = 0; i < estate->ndatums; i++) {
        PLpgSQL_variable* node = (PLpgSQL_variable*)estate->datums[i];
        PLDebug_variable* var = NULL;
        if (node->isImplicit) {
            continue;
        }
        switch (node->dtype) {
            case PLPGSQL_DTYPE_VAR: {
                PLpgSQL_var* var_node = (PLpgSQL_var*)node;
                if (var_node->datatype != NULL && var_node->datatype->typoid == REFCURSOROID) {
                    /* Cursor variables are followed by 4 attribute of its own. */
                    Assert(i < estate->ndatums - 4);
                    var = get_debug_variable_refcursor(estate->datums, i, var_name);
                    i += 4;
                } else {
                    var = get_debug_variable_var((PLpgSQL_var*)node, var_name);
                }
                break;
            }
            case PLPGSQL_DTYPE_ROW:
            case PLPGSQL_DTYPE_RECORD:
                var = get_debug_variable_row((PLpgSQL_row*)node, estate, var_name);
                break;
            case PLPGSQL_DTYPE_REC:
                var = get_debug_variable_rec((PLpgSQL_rec*)node, var_name);
                break;
            case PLPGSQL_DTYPE_EXPR: /* these node types should not be listed as local variables */
            case PLPGSQL_DTYPE_ARRAYELEM:
            case PLPGSQL_DTYPE_RECFIELD:
            default:
                break;
        }
        if (var != NULL && strcmp(var->name, DEFAULT_UNKNOWN_VALUE)) { /* no need to show internal variables */
            local_variables = lappend(local_variables, var);
        }
    }
    char* vars = nodeToString(local_variables);
    int len = strlen(vars);
    debug_server_send_msg(debug, vars, len);
    list_free_deep(local_variables);
    pfree(vars);
}

void clean_up_debug_client(bool hasError)
{
    if (u_sess->plsql_cxt.debug_client) {
        DebugClientInfo* client = u_sess->plsql_cxt.debug_client;
        /* clean comm idx*/
        if (client->comm_idx < PG_MAX_DEBUG_CONN && client->comm_idx >= 0) {
            uint64 clientSessionId = ENABLE_THREAD_POOL ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid;
            PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[client->comm_idx];
            AutoMutexLock debuglock(&debug_comm->mutex);
            debuglock.lock();
            if (debug_comm->hasClient() && debug_comm->clientId == clientSessionId) {
                /* only wake up server for error when it's not recevied server error */
                if (hasError && debug_comm->IsServerWaited && !debug_comm->hasServerErrorOccured) {
                    debug_comm->hasClientErrorOccured = true;
                    debug_comm->hasClientFlushed = false;
                    debug_comm->bufLen = 0;
                    debug_comm->clientId = 0;
                    debuglock.unLock();
                    WakeUpReceiver(client->comm_idx, true);
                } else {
                    Assert (debug_comm->IsServerWaited == false);
                    /* client has already handled server error, reset flag */
                    debug_comm->hasServerErrorOccured = false;
                    debug_comm->hasClientFlushed = false;
                    debug_comm->clientId = 0;
                    debug_comm->bufLen = 0;
                    debuglock.unLock();
                }
            } else {
                debuglock.unLock();
            }
        }
        client->comm_idx = -1;
        MemoryContextDelete(client->context);
        u_sess->plsql_cxt.debug_client = NULL;
    }
}

static void wait_for_client_handle_msg(int comm_idx)
{
    PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[comm_idx];
    PG_TRY();
    {
        pthread_mutex_lock(&debug_comm->mutex);
        while (true) {
            /* sleep until client get msg */
            if (debug_comm->hasClient() && debug_comm->hasServerFlushed && debug_comm->IsClientWaited) {
                pthread_mutex_unlock(&debug_comm->mutex);
                CHECK_FOR_INTERRUPTS();
                pg_usleep(100);
                pthread_mutex_lock(&debug_comm->mutex);
            } else {
                break;
            }
        }
        pthread_mutex_unlock(&debug_comm->mutex);
    }
    PG_CATCH();
    {
        /* invalid flushed msg */
        debug_comm->hasServerFlushed = false;
        PG_RE_THROW();
    }
    PG_END_TRY();
}

void clean_up_debug_server(DebugInfo* debug, bool sessClose, bool hasError)
{
    if (debug == NULL) {
        return;
    }
    if (sessClose) {
        Assert(debug->func);
        /* only remove from hash table when session close */
        delete_debug_func(debug->func->fn_oid);
    }
    if (debug->inner_called_debugger) {
        clean_up_debug_server(debug->inner_called_debugger, sessClose, hasError);
    }
    if (debug->debugStackIdx == 0) {
        wait_for_client_handle_msg(debug->comm->comm_idx);
        pfree_ext(debug->comm->send_buffer);
        pfree_ext(debug->comm->rec_buffer);
        PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[debug->comm->comm_idx];
        AutoMutexLock debuglock(&debug_comm->mutex);
        debuglock.lock();
        if (debug_comm->hasClient()) {
            debug_comm->isProcdeureRunning = false;
            /* only wake up client for error when it's not recevied client error */
            if (debug_comm->IsClientWaited && hasError && !debug_comm->hasClientErrorOccured) {
                /* still has client wait server msg, error occurs */
                debug_comm->hasServerErrorOccured = true;
                debug_comm->bufLen = 0;
                debug_comm->hasServerFlushed = false;
                debuglock.unLock();
                WakeUpReceiver(debug->comm->comm_idx, false);
            } else {
                Assert(debug_comm->IsClientWaited == false);
                /* server has already handled client error, reset flag */
                debug_comm->hasClientErrorOccured = false;
                debug_comm->clientId = 0;
                debug_comm->bufLen = 0;
                debug_comm->hasServerFlushed = false;
                debuglock.unLock();
            }
        } else {
            debug_comm->isProcdeureRunning = false;
            debuglock.unLock();
        }
        debug->func->debug = NULL;
        MemoryContextDelete(debug->debug_cxt);
        u_sess->plsql_cxt.cur_debug_server = NULL;
    } else {
        debug->comm = NULL;
        if (debug->func != NULL) {
            debug->func->debug = NULL;
        }
    }
    DebugInfo* outer_debug = u_sess->plsql_cxt.cur_debug_server;
    while (outer_debug != NULL && outer_debug->inner_called_debugger != debug) {
        outer_debug = outer_debug->inner_called_debugger;
    }
    if (outer_debug != NULL) {
        outer_debug->inner_called_debugger = NULL;
    }
}

void debug_server_abort(DebugInfo* debug)
{
    const char* ans = "t";
    debug_server_send_msg(debug, ans, strlen(ans));
    ereport(ERROR, (errmodule(MOD_PLDEBUGGER), (errmsg("receive abort message"))));
}

static bool send_cur_info(DebugInfo* debug, PLpgSQL_execstate* estate, bool stop_next)
{
    bool isend = false;
    debug->stop_next_stmt = stop_next;
    StringInfoData str;
    initStringInfo(&str);
    isend = get_cur_info(&str, estate, debug);
    debug_server_send_msg(debug, str.data, str.len);
    return isend;
}

static bool get_cur_info(StringInfo str, PLpgSQL_execstate* estate, DebugInfo* debug)
{
    Oid funcoid = debug->func->fn_oid;
    char* funcname = NULL;
    if (OidIsValid(funcoid)) {
        funcname = get_func_name(funcoid);
    } else {
        funcname = pstrdup("anonymous block");
    }
    Oid pkgoid = debug->func->pkg_oid;
    Assert(funcname != NULL);
    int lineno = estate->err_stmt->lineno;
    char* query = get_stmt_query(estate->err_stmt);
    bool isend = false;
    char* pkgname = NULL;

    if (pkgoid != InvalidOid) {
        pkgname = GetPackageName(pkgoid);
    }

    char* pkgfuncname = quote_qualified_identifier(pkgname, funcname);

    /* turn to show code's lineno */
    if (query) {
        char* maskquery = maskPassword(query);
        query = (maskquery == NULL) ? query : maskquery;
        appendStringInfo(str, "%u:%s:%d:%s", funcoid, pkgfuncname, lineno, query);
        if (maskquery != query) {
            pfree_ext(maskquery);
        }
    } else {
        if (debug->debugStackIdx == 0) {
            set_debugger_procedure_state(debug->comm->comm_idx, false);
            debug->stop_next_stmt = false;
            isend = true;
        }
        appendStringInfo(str, "%u:%s:%d:%s", funcoid, pkgfuncname, lineno, "[EXECUTION FINISHED]");
    }
    pfree(pkgfuncname);
    pfree(funcname);

    return isend;
}

template<typename T>
char* GetSqlString(PLpgSQL_stmt* stmt)
{
    return ((T*)stmt)->sqlString;
}

const int STMT_NUM = 28;
typedef char* (*GetSQLStringFunc)(PLpgSQL_stmt*);

/* KEEP IN THE SAME ORDER AS PLpgSQL_stmt_types */
const GetSQLStringFunc G_GET_SQL_STRING[STMT_NUM] = {
    GetSqlString<PLpgSQL_stmt_block>,
    GetSqlString<PLpgSQL_stmt_assign>,
    GetSqlString<PLpgSQL_stmt_if>,
    GetSqlString<PLpgSQL_stmt_goto>,
    GetSqlString<PLpgSQL_stmt_case>,
    GetSqlString<PLpgSQL_stmt_loop>,
    GetSqlString<PLpgSQL_stmt_while>,
    GetSqlString<PLpgSQL_stmt_fori>,
    GetSqlString<PLpgSQL_stmt_fors>,
    GetSqlString<PLpgSQL_stmt_forc>,
    GetSqlString<PLpgSQL_stmt_foreach_a>,
    GetSqlString<PLpgSQL_stmt_exit>,
    GetSqlString<PLpgSQL_stmt_return>,
    GetSqlString<PLpgSQL_stmt_return_next>,
    GetSqlString<PLpgSQL_stmt_return_query>,
    GetSqlString<PLpgSQL_stmt_raise>,
    GetSqlString<PLpgSQL_stmt_execsql>,
    GetSqlString<PLpgSQL_stmt_dynexecute>,
    GetSqlString<PLpgSQL_stmt_dynfors>,
    GetSqlString<PLpgSQL_stmt_getdiag>,
    GetSqlString<PLpgSQL_stmt_open>,
    GetSqlString<PLpgSQL_stmt_fetch>,
    GetSqlString<PLpgSQL_stmt_close>,
    GetSqlString<PLpgSQL_stmt_perform>,
    GetSqlString<PLpgSQL_stmt_commit>,
    GetSqlString<PLpgSQL_stmt_rollback>,
    GetSqlString<PLpgSQL_stmt_null>,
    GetSqlString<PLpgSQL_stmt_savepoint>
};

static char* get_stmt_query(PLpgSQL_stmt* stmt)
{
    char* query = NULL;
    if (stmt->cmd_type >= STMT_NUM || stmt->cmd_type < 0) {
        ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_PLDEBUGGER),
                    errmsg("unrecognized statement type: %d for PLSQL function.", stmt->cmd_type)));
    } else {
        query = G_GET_SQL_STRING[stmt->cmd_type](stmt);
    }
    return query;
}

static bool ActiveBPInFunction(DebugInfo* debug, Oid funcOid)
{
    ListCell* lc = NULL;
    foreach(lc, debug->bp_list) {
        PLDebug_breakPoint* bp = (PLDebug_breakPoint*)lfirst(lc);
        if (bp->funcoid == funcOid && !bp->deleted && bp->active) {
            return true;
        }
    }
    return false;
}

static bool IsBreakPointExisted(DebugInfo* debug, Oid funcOid, int lineno, bool ignoreDisabled)
{
    ListCell* lc = NULL;
    foreach(lc, debug->bp_list) {
        PLDebug_breakPoint* bp = (PLDebug_breakPoint*)lfirst(lc);
        if (bp->funcoid == funcOid && bp->lineno == lineno && !bp->deleted && (bp->active || !ignoreDisabled)) {
            return true;
        }
    }
    return false;
}

static void debug_server_add_breakpoint(DebugInfo* debug)
{
    /* Validity of the parameter are checked on client side */
    MemoryContext old_context = MemoryContextSwitchTo(debug->debug_cxt);
    
    /* Received buffer will be in the form of <func_oid:lineno> */
    char* psave = NULL;
    char* fir = strtok_r(debug->comm->rec_buffer, ":", &psave);
    const int int64Size = 10;
    char* new_fir = TrimStr(fir);
    if (new_fir == NULL) {
        ReportInvalidMsg(debug->comm->rec_buffer);
        return;
    }
    Oid funcOid = (Oid)pg_strtouint64(new_fir, NULL, int64Size);
    fir = strtok_r(NULL, ":", &psave);
    new_fir = TrimStr(fir);
    if (new_fir == NULL) {
        ReportInvalidMsg(debug->comm->rec_buffer);
        return;
    }
    int lineno = (int)pg_strtouint64(new_fir, NULL, int64Size);
    char* query = pstrdup(psave);
    char* maskquery = (query == NULL) ? NULL : maskPassword(query);
    if (maskquery != NULL && maskquery != query) {
        pfree_ext(query);
        query = maskquery;
    }

    int newIndex = -1;
    bool valid = true;
    if (funcOid == InvalidOid) {
        query = debug_server_add_breakpoint_invalid(debug, &valid, lineno, &newIndex, query, maskquery);
    }

    if (valid && !IsBreakPointExisted(debug, funcOid, lineno, false)) {
        PLDebug_breakPoint* bp = (PLDebug_breakPoint*)makeNode(PLDebug_breakPoint);
        bp->bpIndex = list_length(debug->bp_list);
        bp->funcoid = funcOid;
        bp->lineno = lineno;
        bp->active = true;
        bp->deleted = false;
        bp->query = query;
        debug->bp_list = lappend(debug->bp_list, bp);
        newIndex = bp->bpIndex;
    }

    (void)MemoryContextSwitchTo(old_context);

    StringInfoData str;
    initStringInfo(&str);
    appendStringInfo(&str, "%d", newIndex);
    debug_server_send_msg(debug, str.data, str.len);
}

static char* debug_server_add_breakpoint_invalid(DebugInfo* debug, bool* valid, int lineno, int* newIndex, char* query, char* maskquery)
{
    /* step 1: check whether exceed the max line number, return -2 if exceed */
    List* debug_body = debug->func->action->body;
    uint32 maxline = ((PLpgSQL_stmt*)lfirst((list_head(debug_body))))->lineno + debug_body->length - 2;
    *valid = (lineno < 1 || (uint32)lineno > maxline) ? false : true;
    *newIndex = *valid ? -1 : ADD_BP_ERR_OUT_OF_RANGE;
    /* step 2: valid breakpoint or not, return -3 if not valid */
    if (!*valid) {
        return "NULL";
    }
    List* breakables = collect_breakable_line(debug->func);
    *valid = list_member_int(breakables, lineno);
    *newIndex = *valid ? -1 : ADD_BP_ERR_INVALID_BP_POS;
    list_free(breakables);
    /*step 3: get query*/
    if (!*valid) {
        return "NULL";
    }
    ListCell *cell = NULL;
    foreach(cell, debug_body) {
        PLpgSQL_stmt* stmt = (PLpgSQL_stmt*)lfirst(cell);
        if (stmt->lineno == lineno) {
            query = pstrdup(get_stmt_query(stmt));
            maskquery = (query == NULL) ? NULL : maskPassword(query);
            if (maskquery != NULL && maskquery != query) {
                pfree_ext(query);
                query = maskquery;
            }
            break;
        }
    }
    return query;
    /* not found? should not happen*/
}

static void debug_server_delete_breakpoint(DebugInfo* debug)
{
    /* Client guarantees the index to be positive */
    MemoryContext old_context = MemoryContextSwitchTo(debug->debug_cxt);

    /* Received buffer will be in the form of <bpIndex> */
    int bpIndex = pg_strtoint32(debug->comm->rec_buffer);
    char* buf = NULL;

    if (bpIndex >= list_length(debug->bp_list)) {
        buf = pstrdup("1");
    } else {
        PLDebug_breakPoint* bp = (PLDebug_breakPoint*)list_nth(debug->bp_list, bpIndex);
        bp->deleted = true;
        buf = pstrdup("0");
    }
    debug_server_send_msg(debug, buf, strlen(buf));
    pfree(buf);
    (void)MemoryContextSwitchTo(old_context);
}

static void debug_server_enable_breakpoint(DebugInfo* debug)
{
    /* Client guarantees the index to be positive */
    MemoryContext old_context = MemoryContextSwitchTo(debug->debug_cxt);

    /* Received buffer will be in the form of <bpIndex> */
    int bpIndex = pg_strtoint32(debug->comm->rec_buffer);
    char* buf = NULL;

    if (bpIndex >= list_length(debug->bp_list)) {
        buf = pstrdup("1");
    } else {
        PLDebug_breakPoint* bp = (PLDebug_breakPoint*)list_nth(debug->bp_list, bpIndex);
        if (bp->deleted) {
            buf = pstrdup("1");
        }
        if (!bp->active) {
            bp->active = true;
            buf = pstrdup("0");
        } else {
            buf = pstrdup("1");
        }
    }
    debug_server_send_msg(debug, buf, strlen(buf));
    pfree(buf);
    (void)MemoryContextSwitchTo(old_context);
}

static void debug_server_disable_breakpoint(DebugInfo* debug)
{
    /* Client guarantees the index to be positive */
    MemoryContext old_context = MemoryContextSwitchTo(debug->debug_cxt);

    /* Received buffer will be in the form of <bpIndex> */
    int bpIndex = pg_strtoint32(debug->comm->rec_buffer);
    char* buf = NULL;

    if (bpIndex >= list_length(debug->bp_list)) {
        buf = pstrdup("1");
    } else {
        PLDebug_breakPoint* bp = (PLDebug_breakPoint*)list_nth(debug->bp_list, bpIndex);
        if (bp->deleted) {
            buf = pstrdup("1");
        }
        if (bp->active) {
            bp->active = false;
            buf = pstrdup("0");
        } else {
            buf = pstrdup("1");
        }
    }
    debug_server_send_msg(debug, buf, strlen(buf));
    pfree(buf);
    (void)MemoryContextSwitchTo(old_context);
}

static void debug_server_info_breakpoint(DebugInfo* debug)
{
    List* activeBreakPoints = NIL;
    ListCell* lc = NULL;
    foreach(lc, debug->bp_list) {
        PLDebug_breakPoint* bp = (PLDebug_breakPoint*)lfirst(lc);
        if (!bp->deleted) {
            activeBreakPoints = lappend(activeBreakPoints, bp);
        }
    }
    char* buf = nodeToString(activeBreakPoints);
    int len = strlen(buf);
    debug_server_send_msg(debug, buf, len);
    list_free(activeBreakPoints);
    pfree(buf);
}

PLDebug_frame* get_frame(DebugInfo* debug)
{
    if (debug == NULL || debug->func == NULL || debug->cur_stmt == NULL) {
        ereport(ERROR,
            (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("Server debug info contains unexpected NULL value"),
                errdetail("N/A"),
                errcause("Debugger info on current stack is not properly initialized."),
                erraction("Contact Huawei Engineer.")));
    }
    PLDebug_frame* frame = (PLDebug_frame*)makeNode(PLDebug_frame);
    char* funcname = NULL;
    char* pkgname = NULL;
    Oid pkgoid = debug->func->pkg_oid;
    if (OidIsValid(debug->func->fn_oid)) {
        funcname = get_func_name(debug->func->fn_oid);
    } else {
        funcname = pstrdup("anonymous block");
    }
    if (pkgoid != InvalidOid) {
        pkgname = GetPackageName(pkgoid);
    }
    frame->frameno = debug->debugStackIdx;
    frame->funcname = quote_qualified_identifier(pkgname, funcname);
    frame->lineno = debug->cur_stmt->lineno;
    char* query = get_stmt_query(debug->cur_stmt);
    char* maskquery = (query == NULL) ? NULL : maskPassword(query);
    query = (maskquery == NULL) ? query : maskquery;
    frame->query = query;
    frame->funcoid = debug->func->fn_oid;
    pfree(funcname);
    return frame;
}

static void debug_server_backtrace()
{
    List* stackInfo = NIL;
    DebugInfo* debug = u_sess->plsql_cxt.cur_debug_server;
    DebugInfo* outer_debug = NULL;
    /* recursively extracts inner stack information */
    while (debug != NULL) {
        stackInfo = lcons(get_frame(debug), stackInfo);
        outer_debug = debug;
        debug = debug->inner_called_debugger;
    }
    char* buf = nodeToString(stackInfo);
    int len = strlen(buf);
    debug_server_send_msg(u_sess->plsql_cxt.cur_debug_server, buf, len);
    list_free_deep(stackInfo);
    pfree(buf);
}

static PLpgSQL_expr* ConstructAssignExpr(char* value, PLpgSQL_nsitem* ns_top)
{
    StringInfoData str;
    initStringInfo(&str);
    appendStringInfo(&str, "SELECT %s", value);
    PLpgSQL_expr* expr = (PLpgSQL_expr*)palloc0(sizeof(PLpgSQL_expr));
    expr->dtype = PLPGSQL_DTYPE_EXPR;
    expr->query = pstrdup(str.data);
    expr->ns = ns_top;
    expr->dno = -1;
    expr->idx = -1;
    expr->out_param_dno = -1;
    pfree(str.data);
    return expr;
}

PLpgSQL_datum* GetDatumByVarname(PLpgSQL_execstate* estate, const char* varName, bool* foundCursor, bool* foundConst)
{
    for (int i = 0; i < estate->ndatums; ++i) {
        bool isCursor = false;
        bool isConst = false;
        char* datumName = NULL;
        PLpgSQL_variable* node = (PLpgSQL_variable*)estate->datums[i];
        switch(node->dtype) {
            case PLPGSQL_DTYPE_VAR: {
                PLpgSQL_var* var = (PLpgSQL_var*)node;
                datumName = var->varname == NULL ? var->refname : var->varname;
                if (var->datatype != NULL && var->datatype->typoid == REFCURSOROID) {
                    isCursor = true;
                } else if (var->isconst) {
                    isConst = true;
                }
                break;
            }
            case PLPGSQL_DTYPE_ROW:
            case PLPGSQL_DTYPE_RECORD: {
                PLpgSQL_row* row = (PLpgSQL_row*)node;
                datumName = row->varname == NULL ? row->refname : row->varname;
                break;
            }
            case PLPGSQL_DTYPE_REC: {
                PLpgSQL_rec* rec = (PLpgSQL_rec*)node;
                datumName = rec->varname == NULL ? rec->refname : rec->varname;
                break;
            }
            default:
                break;
        }
        if (datumName == NULL) {
            continue;
        }
        if (strcmp(varName, datumName) == 0) {
            if (isCursor) {
                *foundCursor = true;
                return NULL;
            } else if (isConst) {
                *foundConst = true;
                return NULL;
            }
            return (PLpgSQL_datum*)node;
        }
    }
    return NULL;
}

static void debug_server_set_variable(DebugInfo* debug, PLpgSQL_execstate* estate)
{
    MemoryContext func_context = MemoryContextSwitchTo(debug->debug_cxt);

    /* Received buffer will be in the form of <variable_name:value> */
    char* psave = NULL;
    char* fir = strtok_r(debug->comm->rec_buffer, ":", &psave);
    char* varname = TrimStr(fir);
    if (varname == NULL) {
        ereport(ERROR,
            (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("Get null pointer for variable name"),
                errdetail("message received: %s", debug->comm->rec_buffer),
                errcause("internal error, invalid debugger message."),
                erraction("Contact Huawei Engineer.")));
    }
    fir = psave;
    char* value = TrimStr(fir);
    StringInfoData str;
    initStringInfo(&str);

    bool foundCursor = false;
    bool foundConst = false;
    PLpgSQL_datum* targetDatum = GetDatumByVarname(estate, varname, &foundCursor, &foundConst);

    if (targetDatum == NULL) {
        if (foundCursor) {
            appendStringInfo(&str, "%d:", DEBUG_SERVER_SET_CURSOR);
        } else if (foundConst) {
            appendStringInfo(&str, "%d:", DEBUG_SERVER_SET_CONST);
        } else {
            appendStringInfo(&str, "%d:", DEBUG_SERVER_SET_NO_VAR);
        }

        debug_server_send_msg(u_sess->plsql_cxt.cur_debug_server, str.data, strlen(str.data));
        (void)MemoryContextSwitchTo(func_context);
        pfree(str.data);
        return;
    }

    PLpgSQL_expr* expr = ConstructAssignExpr(value, debug->func->ns_top);

    ResourceOwner oldowner = t_thrd.utils_cxt.CurrentResourceOwner;
    /* Try to execute the assignment */
    BeginInternalSubTransaction(NULL);
    (void)MemoryContextSwitchTo(func_context);
    PG_TRY();
    {
        /* Expr for set_var should only contain one query statement */
        List* parsetrees = pg_parse_query(expr->query);
        if (list_length(parsetrees) != 1) {
            appendStringInfo(&str, "%d:%s", DEBUG_SERVER_SET_EXEC_FAILURE, "invalid value for set_var");
            debug_server_send_msg(u_sess->plsql_cxt.cur_debug_server, str.data, strlen(str.data));
            pfree(str.data);
            list_free_deep(parsetrees);
            ReleaseCurrentSubTransaction();
            func_context = MemoryContextSwitchTo(debug->debug_cxt);
            t_thrd.utils_cxt.CurrentResourceOwner = oldowner;
            PG_TRY_RETURN();
        }
        exec_assign_expr(estate, targetDatum, expr);
        /* Commit the inner transaction and return to the outer context */
        ReleaseCurrentSubTransaction();
        func_context = MemoryContextSwitchTo(debug->debug_cxt);
        t_thrd.utils_cxt.CurrentResourceOwner = oldowner;
        SPI_restore_connection();
        appendStringInfo(&str, "%d:", DEBUG_SERVER_SUCCESS);
    }
    PG_CATCH();
    {
        /* Abort the inner transaction, flush error and return to the outer context. */
        func_context = MemoryContextSwitchTo(debug->debug_cxt);
        ErrorData* edata = &t_thrd.log_cxt.errordata[t_thrd.log_cxt.errordata_stack_depth];
        appendStringInfo(&str, "%d:%s", DEBUG_SERVER_SET_EXEC_FAILURE, edata->message);
        FlushErrorState();
        RollbackAndReleaseCurrentSubTransaction();
        (void)MemoryContextSwitchTo(debug->debug_cxt);
        t_thrd.utils_cxt.CurrentResourceOwner = oldowner;
        SPI_restore_connection();
    }
    PG_END_TRY();

    debug_server_send_msg(u_sess->plsql_cxt.cur_debug_server, str.data, strlen(str.data));
    pfree(str.data);
}

PlDebugEntry* has_debug_func(Oid key, bool* found)
{
    if ((u_sess->plsql_cxt.debug_proc_htbl == NULL)) {
        *found = false;
        return NULL;
    }
    PlDebugEntry* entry = (PlDebugEntry*)hash_search(u_sess->plsql_cxt.debug_proc_htbl,
                                                     (void*)(&key), HASH_FIND, found);
    return entry;
}

bool delete_debug_func(Oid key)
{
    if (unlikely(u_sess->plsql_cxt.debug_proc_htbl == NULL)) {
        return false;
    }
    bool found = false;
    PlDebugEntry* entry = has_debug_func(key, &found);
    if (found) {
        ReleaseDebugCommIdx(entry->commIdx);
    }
    (void)hash_search(u_sess->plsql_cxt.debug_proc_htbl, (void*)(&key), HASH_REMOVE, &found);
    return found;
}

void ReleaseDebugCommIdx(int idx)
{
    if (idx < 0 || idx >= PG_MAX_DEBUG_CONN)
        return;
    Assert(g_instance.pldebug_cxt.debug_comm[idx].Used());
    PlDebuggerComm* comm = &g_instance.pldebug_cxt.debug_comm[idx];
    AutoMutexLock debuglock(&comm->mutex);
    debuglock.lock();
    /* clean used msg */
    pfree_ext(comm->buffer);
    comm->hasClientFlushed = false;
    comm->hasServerFlushed = false;
    comm->IsServerWaited = false;
    comm->IsClientWaited = false;
    comm->hasClientFlushed = false;
    comm->hasServerFlushed = false;
    comm->hasServerErrorOccured = false;
    comm->hasClientErrorOccured = false;
    comm->isProcdeureRunning = false;
    comm->serverId = 0;
    comm->clientId = 0;
    comm->bufLen = 0;
    comm->bufSize = 0;
    debuglock.unLock();
    /* release debug comm */
    (void)LWLockAcquire(PldebugLock, LW_EXCLUSIVE);
    comm->hasUsed = false;
    LWLockRelease(PldebugLock);
}

int GetValidDebugCommIdx()
{
    (void)LWLockAcquire(PldebugLock, LW_EXCLUSIVE);
    int idx = -1;
    for (int i = 0; i < PG_MAX_DEBUG_CONN; i++) {
        if (!g_instance.pldebug_cxt.debug_comm[i].Used()) {
            PlDebuggerComm* comm = &g_instance.pldebug_cxt.debug_comm[i];
            comm->hasUsed = true;
            comm->hasClientFlushed = false;
            comm->hasServerFlushed = false;
            comm->IsServerWaited = false;
            comm->IsClientWaited = false;
            comm->hasClientFlushed = false;
            comm->hasServerFlushed = false;
            comm->hasServerErrorOccured = false;
            comm->hasClientErrorOccured = false;
            comm->isProcdeureRunning = false;
            comm->serverId = ENABLE_THREAD_POOL ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid;
            comm->clientId = 0;
            comm->bufLen = 0;
            comm->bufSize = 0;
            comm->buffer = NULL; /* init buffer when need use it */
            idx = i;
            break;
        }
    }
    LWLockRelease(PldebugLock);
    return idx;
}

bool WakeUpReceiver(int commIdx, bool isClient)
{
    PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[commIdx];
    bool succ = true;
    pthread_mutex_lock(&debug_comm->mutex);
    if (isClient) {
        /* client flushed, wake up server */
        if (likely(debug_comm->hasClientFlushed == false)) {
            debug_comm->hasClientFlushed = true;
            pthread_cond_signal(&debug_comm->cond);
        } else {
            succ = false;
        }
    } else {
        /* server flushed, wake up client */
        if (likely(debug_comm->hasServerFlushed == false)) {
            debug_comm->hasServerFlushed = true;
            pthread_cond_signal(&debug_comm->cond);
        } else {
            succ = false;
        }
    }
    pthread_mutex_unlock(&debug_comm->mutex);
    return succ;
}
/* get msg from global comm buffer, copy into local buffer */
void WaitSendMsg(int commIdx, bool isClient, char** destBuffer, int* destLen)
{
    int rc = 0;
    PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[commIdx];
    bool mutexLocked = true;
    pthread_mutex_lock(&debug_comm->mutex);
    PG_TRY();
    {
        struct timespec time_to_wait;
        long cur_time = 1;
        if (isClient) {
            /* server wait for client msg */
            debug_comm->IsServerWaited = true;
            while (debug_comm->hasClientFlushed == false) {
                /*
                 * The PlDebugger Server has acquired the lock while waiting. 
                 * When it receives the database stop signal, it exits the thread in the processing interrupt. 
                 * This will call PlDebuggerCleanUp() to clean up, and it will acquire mutex again, 
                 * which will lead to dead waiting.
                 * So when need to handle interrupt, need to release the lock before calling CHECK_FOR_INTERRUPTS().
                 */
                if (InterruptPending) {
                    pthread_mutex_unlock(&debug_comm->mutex);
                    mutexLocked = false;
                    CHECK_FOR_INTERRUPTS();
                    pthread_mutex_lock(&debug_comm->mutex);
                    mutexLocked = true;
                    continue;
                }
                if (cur_time >= u_sess->attr.attr_sql.pldebugger_timeout) {
                    ereport(ERROR,
                        (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_PLDEBUGGER_TIMEOUT),
                            errmsg("Client accept message wait timeout, max wait time %d seconds.",
                                   u_sess->attr.attr_sql.pldebugger_timeout),
                            errdetail("N/A"),
                            errcause("Debug client wait for server msg timeout."),
                            erraction("Debug server should send to client msg in time.")));
                }
                clock_gettime(CLOCK_REALTIME, &time_to_wait);
                time_to_wait.tv_sec += 1;
                pthread_cond_timedwait(&debug_comm->cond, &debug_comm->mutex, &time_to_wait);
                cur_time += 1;
            }
            debug_comm->IsServerWaited = false;
            /* save msg */
            *destBuffer  = ResizeDebugBufferIfNecessary(*destBuffer, destLen, debug_comm->bufSize);
            rc = memcpy_s(*destBuffer, *destLen, debug_comm->buffer, debug_comm->bufLen);
            securec_check(rc, "\0", "\0");
            /* reset buf msg */
            rc = memset_s(debug_comm->buffer, debug_comm->bufLen, 0, debug_comm->bufLen);
            securec_check(rc, "\0", "\0");
            debug_comm->bufLen = 0;
            debug_comm->hasClientFlushed = false;
            if (debug_comm->hasClientErrorOccured) {
                ereport(ERROR, (errmodule(MOD_PLDEBUGGER),
                        errmsg("Debug client has some error occured.")));
            }
        } else {
            /* client wait for server msg */
            debug_comm->IsClientWaited = true;
            while (debug_comm->hasServerFlushed == false) {
                if (InterruptPending) {
                    pthread_mutex_unlock(&debug_comm->mutex);
                    mutexLocked = false;
                    CHECK_FOR_INTERRUPTS();
                    pthread_mutex_lock(&debug_comm->mutex);
                    mutexLocked = true;
                    continue;
                }
                if (cur_time >= u_sess->attr.attr_sql.pldebugger_timeout) {
                    ereport(ERROR,
                        (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_PLDEBUGGER_TIMEOUT),
                            errmsg("Server accept message wait timeout, max wait time %d seconds.",
                                   u_sess->attr.attr_sql.pldebugger_timeout),
                            errdetail("N/A"),
                            errcause("Debug server wait for client msg timeout."),
                            erraction("Debug client should send to server msg in time.")));
                }
                clock_gettime(CLOCK_REALTIME, &time_to_wait);
                time_to_wait.tv_sec += 1;
                pthread_cond_timedwait(&debug_comm->cond, &debug_comm->mutex, &time_to_wait);
                cur_time += 1;
            }
            debug_comm->IsClientWaited = false;
            /* save msg */
            *destBuffer  = ResizeDebugBufferIfNecessary(*destBuffer, destLen, debug_comm->bufSize);
            rc = memcpy_s(*destBuffer, *destLen, debug_comm->buffer, debug_comm->bufLen);
            securec_check(rc, "\0", "\0");
            /* reset buf msg */
            rc = memset_s(debug_comm->buffer, debug_comm->bufLen, 0, debug_comm->bufLen);
            securec_check(rc, "\0", "\0");
            debug_comm->bufLen = 0;
            debug_comm->hasServerFlushed = false;
            if (debug_comm->hasServerErrorOccured) {
                ereport(ERROR, (errmodule(MOD_PLDEBUGGER),
                        errmsg("Debug server has some error occured.")));
            }
        }
        pthread_mutex_unlock(&debug_comm->mutex);
    }
    PG_CATCH();
    {
        if (isClient) {
            debug_comm->IsServerWaited = false;
            debug_comm->hasClientFlushed = false;
        } else {
            debug_comm->IsClientWaited = false;
            debug_comm->hasServerFlushed = false;
        }
        if (mutexLocked)
            pthread_mutex_unlock(&debug_comm->mutex);

        PG_RE_THROW();
    }
    PG_END_TRY();
}

/* msg send & receive for server thread */
static void PrintDebugBuffer(const char *buffer, int len)
{
    if (buffer == NULL || len <= 0) {
        return;
    }

    /* Line-by-line printing with a width limit of 1024 */
    const int nBytePrintLine = 1024;
    const int pqBufferSize = 8192;
    const char defaultNullChar = ' ';
    char tmp[nBytePrintLine + 1] = {0};
    int i, idx = 0;
    len = (len > pqBufferSize) ? pqBufferSize : len;

    ereport(DEBUG3, (errmodule(MOD_PLDEBUGGER), (errmsg("--------buffer begin--------"))));
    for (i = 0; i < len; i++) {
        idx = i % nBytePrintLine;

        if (buffer[i] != '\0') {
            tmp[idx] = buffer[i];
        } else {
            tmp[idx] = defaultNullChar;
        }
        if ((i + 1) % nBytePrintLine == 0) {
            ereport(DEBUG3, (errmodule(MOD_PLDEBUGGER), (errmsg("%s", tmp))));
        }
    }

    /* Output buffer with the remaining length. */
    if (len % nBytePrintLine != 0) {
        /* reset dirty data */
        if (idx <= nBytePrintLine) {
            errno_t rc = memset_s(tmp + idx + 1, nBytePrintLine - idx, 0, nBytePrintLine - idx);
            securec_check(rc, "\0", "\0");
        }
        ereport(DEBUG3, (errmodule(MOD_PLDEBUGGER), errmsg("%s", tmp)));
    }
    ereport(DEBUG3, (errmodule(MOD_PLDEBUGGER), errmsg("--------buffer end--------")));
    return;
}

/* Send msg into global comm buffer */
void SendUnixMsg(int comm_idx, const char* val, int len, bool is_client)
{
    ereport(DEBUG3, (errmodule(MOD_PLDEBUGGER), errmsg("send len %d", len)));
    if (len <= 0)
        return;
    PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[comm_idx];
    /* lock */
    (void)MemoryContextSwitchTo(g_instance.pldebug_cxt.PldebuggerCxt);
    AutoMutexLock debuglock(&debug_comm->mutex);
    debuglock.lock();
    Assert(debug_comm->Used());
    if (is_client) {
        Assert(debug_comm->hasClientFlushed == false);
        debug_comm->hasClientFlushed = false;
    } else {
        Assert(debug_comm->hasServerFlushed == false);
        debug_comm->hasServerFlushed = false;
    }
    Assert(debug_comm->bufLen == 0);
    /* resize */
    debug_comm->buffer = ResizeDebugCommBufferIfNecessary(debug_comm->buffer, &debug_comm->bufSize, len);
    /* copy buffer */
    int rc = 0;
    rc = memcpy_s(debug_comm->buffer, debug_comm->bufSize, val, len);
    securec_check(rc, "\0", "\0");
    debug_comm->bufLen += len;
    /* unlock */
    debuglock.unLock();
    PrintDebugBuffer(val, len);
}

/* Get msg from copyed buffer. No need to get lock */
void RecvUnixMsg(const char* buf, int bufLen, char* destBuf, int destLen)
{
    ereport(DEBUG3, (errmodule(MOD_PLDEBUGGER), errmsg("recv len: %d", destLen)));
    if (destLen <= 0)
        return;
    if (bufLen < destLen) {
        ereport(ERROR, (errmodule(MOD_PLDEBUGGER),
                        errmsg("need len: %d, but only get buffer len:%d", destLen, bufLen)));
    }
    /* copy buffer */
    int rc = 0;
    rc = memcpy_s(destBuf, destLen, buf, destLen);
    securec_check(rc, "\0", "\0");
    PrintDebugBuffer(destBuf, destLen);
}

/* buffer contains : buffer size + buffer msg */
static void debug_server_send_msg(DebugInfo* debug, const char* msg, int msg_len)
{
    MemoryContext old_context = MemoryContextSwitchTo(debug->debug_cxt);

    DebugInfoComm* sock = debug->comm;
    sock->send_ptr = 0;
    const int EXTRA_LEN = 4;
    int buf_len = EXTRA_LEN + msg_len;
    sock->send_buffer = ResizeDebugBufferIfNecessary(sock->send_buffer, &(sock->send_buf_len), buf_len + 1);

    int rc = 0;
    rc = memcpy_s(sock->send_buffer + sock->send_ptr, sock->send_buf_len - sock->send_ptr, &msg_len, EXTRA_LEN);
    securec_check(rc, "\0", "\0");
    sock->send_ptr += EXTRA_LEN;
    if (msg_len > 0) {
        rc = memcpy_s(sock->send_buffer + sock->send_ptr, sock->send_buf_len - sock->send_ptr, msg, msg_len);
        securec_check(rc, "\0", "\0");
        sock->send_ptr += msg_len;
    }
    (void)MemoryContextSwitchTo(old_context);

    CHECK_DEBUG_COMM_VALID(sock->comm_idx);
    SendUnixMsg(sock->comm_idx, sock->send_buffer, sock->send_ptr, false);
    /* wake up client */
    if (!WakeUpReceiver(debug->comm->comm_idx, false)) {
        ereport(ERROR, (errmodule(MOD_PLDEBUGGER),
                errmsg("fail to send msg from debug server to debug client.")));
    }
    (void)MemoryContextSwitchTo(old_context);
}

static void debug_server_rec_msg(DebugInfo* debug, char* firstChar)
{
    MemoryContext old_context = MemoryContextSwitchTo(debug->debug_cxt);
    char* copyBuf = NULL;
    int copyLen = 0;
    int copyPtr = 0;
    int msgLen = 0;
    DebugInfoComm* comm = debug->comm;
    comm->rec_ptr = 0;
    CHECK_DEBUG_COMM_VALID(comm->comm_idx);
    /* wait for client's msg and copy into local buf */
    WaitSendMsg(comm->comm_idx, true, &copyBuf, &copyLen);
    /* first char */
    RecvUnixMsg(copyBuf + copyPtr, copyLen - copyPtr, firstChar, sizeof(char));
    copyPtr += sizeof(char);
    /* msg length */
    RecvUnixMsg(copyBuf + copyPtr, copyLen - copyPtr, (char*)&msgLen, sizeof(int));
    copyPtr += sizeof(int);
    /* msg */
    comm->rec_buffer = ResizeDebugBufferIfNecessary(comm->rec_buffer, &(comm->rec_buf_len), msgLen + 1);
    RecvUnixMsg(copyBuf + copyPtr, copyLen - copyPtr, comm->rec_buffer, msgLen);
    comm->rec_ptr = msgLen;
    comm->rec_buffer[msgLen] = '\0';
    pfree_ext(copyBuf);
    (void)MemoryContextSwitchTo(old_context);
}

static int GetNewSize(int needSize)
{
    int newSize = ceil((double)needSize / (double)DEFAULT_DEBUG_BUF_SIZE) * DEFAULT_DEBUG_BUF_SIZE;
    if (newSize / DEFAULT_DEBUG_BUF_SIZE != ceil((double)needSize / (double)DEFAULT_DEBUG_BUF_SIZE)) {
        ereport(
            ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("PLdebugger buffer requested memory size overflows int")));
    }
    return newSize;
}


/* resize buffer and clear buffer's msg */
static char* ResizeDebugCommBufferIfNecessary(char* buffer, int* oldSize, int needSize)
{
    if (*oldSize > 0) {
        int rc = memset_s(buffer, *oldSize, 0, *oldSize);
        securec_check(rc, "\0", "\0");
    }
    if (needSize <= *oldSize) {
        return buffer;
    }
    int newSize = GetNewSize(needSize);
    MemoryContext oldcxt = MemoryContextSwitchTo(g_instance.pldebug_cxt.PldebuggerCxt);
    char* newBuffer = (char*)palloc0(sizeof(char) * newSize);
    pfree_ext(buffer);
    *oldSize = newSize;
    MemoryContextSwitchTo(oldcxt);
    return newBuffer;
}

/* resize buffer and clear buffer's msg */
char* ResizeDebugBufferIfNecessary(char* buffer, int* oldSize, int needSize)
{
    if (*oldSize > 0) {
        int rc = memset_s(buffer, *oldSize, 0, *oldSize);
        securec_check(rc, "\0", "\0");
    }
    if (needSize <= *oldSize) {
        return buffer;
    }
    int newSize = GetNewSize(needSize);
    char* newBuffer = (char*)palloc0(sizeof(char) * newSize);
    pfree_ext(buffer);
    *oldSize = newSize;
    return newBuffer;
}

char* AssignStr(char* src, bool copy)
{
    if (copy)
        return (src == NULL) ? (pstrdup(DEFAULT_UNKNOWN_VALUE)) : (pstrdup(src));
    else
        return (src == NULL) ? (pstrdup(DEFAULT_UNKNOWN_VALUE)) : (src);
}

void ReportInvalidMsg(const char* buf)
{
    ereport(ERROR,
        (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
            errmsg("Get NULL value when deparsing pldebugger message"),
            errdetail("Received: %s", buf),
            errcause("Debugger send false messages."),
            erraction("Contact Huawei Engineer.")));
}

static void debug_server_info_code(DebugInfo* debug)
{
    List* infoCode = NIL;
    DebugInfo* top = u_sess->plsql_cxt.cur_debug_server;
    while (top != NULL)
    {
        if (!OidIsValid(top->func->fn_oid))
        {
            break;
        }
        top = top->inner_called_debugger;
    }
    if (top == NULL) {
        ereport(ERROR, (errmodule(MOD_PLDEBUGGER), errmsg("There is no anonymous block in debugging")));
    }
    
    List* debug_body = top->func->action->body;
    List* breakables = collect_breakable_line(debug->func);
    ListCell *cell = NULL;

    foreach(cell, debug_body) {
        PLpgSQL_stmt* stmt = (PLpgSQL_stmt*)lfirst(cell);
        PLDebug_codeline* code = (PLDebug_codeline*)makeNode(PLDebug_codeline);;
        code->lineno = stmt->lineno;
        code->code = pstrdup(get_stmt_query(stmt));
        code->canBreak = list_member_int(breakables, stmt->lineno);
        infoCode = lappend(infoCode, code);
    }
    char* buf = nodeToString(infoCode);
    int len = strlen(buf);
    debug_server_send_msg(debug, buf, len);
    list_free(breakables);

    cell = list_head(infoCode);
    while (cell != NULL) {
        ListCell* tmp = cell;
        char* tmp_code = ((PLDebug_codeline*)lfirst(tmp))->code;
        cell = lnext(cell);
        if (tmp_code != NULL) {
            pfree(tmp_code);
        }
        pfree(lfirst(tmp));
        pfree(tmp);
    }
    if (infoCode != NULL) {
        pfree(infoCode);
    }

    pfree(buf);
}
