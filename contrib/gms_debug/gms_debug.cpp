/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * --------------------------------------------------------------------------------------
 *
 * gms_debug.cpp
 *  gms_debug can effectively estimate statistical data.
 *
 *
 * IDENTIFICATION
 *        contrib/gms_debug/gms_debug.cpp
 *
 * --------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "utils/plpgsql_domain.h"
#include "commands/copy.h"
#include "funcapi.h"
#include "utils/plpgsql.h"
#include <sys/socket.h>
#include "utils/pl_debug.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "catalog/pg_authid.h"
#include "miscadmin.h"
#include "gms_debug.h"

#include <bitset>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gms_debug_attach_session);
PG_FUNCTION_INFO_V1(gms_debug_detach_session);
PG_FUNCTION_INFO_V1(gms_debug_get_runtime_info);
PG_FUNCTION_INFO_V1(gms_debug_initialize);
PG_FUNCTION_INFO_V1(gms_debug_off);
PG_FUNCTION_INFO_V1(gms_debug_continue);
PG_FUNCTION_INFO_V1(gms_debug_set_breakpoint);


static void gms_attach_session(int commidx, uint64 sid)
{
    PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[commidx];
    AutoMutexLock debuglock(&debug_comm->mutex);
    debuglock.lock();
    if (debug_comm->Used()) {
        if (debug_comm->hasClient()) {
            debuglock.unLock();
            ereport(ERROR, (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_TARGET_SERVER_ALREADY_ATTACHED),
                (errmsg("target session already attached on other client."))));
        }
        if (debug_comm->hasClientErrorOccured || debug_comm->hasServerErrorOccured) {
            debuglock.unLock();
            ereport(ERROR, (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_INVALID_OPERATION),
                (errmsg("target function  is not running in expected way."))));
        }
        debug_comm->clientId =  sid;
    } else {
        debuglock.unLock();
        ereport(ERROR, (errmodule(MOD_PLDEBUGGER), errcode(ERRCODE_INVALID_OPERATION),
            (errmsg("target session should be init first."))));
    }
    debuglock.unLock();
}

static bool GMSInterfaceCheck(const char* funcname, bool needAttach)
{
#ifdef ENABLE_MULTIPLE_NODES
    PLDEBUG_FEATURE_NOT_SUPPORT_IN_DISTRIBUTED();
#endif
    if (!superuser() && !is_member_of_role(GetUserId(), DEFAULT_ROLE_PLDEBUGGER)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            (errmsg("must be system admin to execute %s", funcname))));
        return false;
    }
    if (u_sess->plsql_cxt.debug_client != NULL && needAttach){
        int commIdx = u_sess->plsql_cxt.debug_client->comm_idx;
        CHECK_DEBUG_COMM_VALID(commIdx);
        /* if current debug index is not myself during debug, clean up my self */
        PlDebuggerComm* debug_comm = &g_instance.pldebug_cxt.debug_comm[commIdx];
        DebugClientInfo* client = u_sess->plsql_cxt.debug_client;
        AutoMutexLock debuglock(&debug_comm->mutex);
        debuglock.lock();
        if (debug_comm == nullptr) {
            client->comm_idx = -1;
            MemoryContextDelete(client->context);
            u_sess->plsql_cxt.debug_client = NULL;
            debuglock.unLock();
            ereport(ERROR,
            (errcode(ERRCODE_TARGET_SERVER_NOT_ATTACHED),
                errmsg("must attach a execute func before execute %s", funcname),
                errhint("attach a execute func and retry")));
            return false;
        }
        if (!debug_comm->isRunning()) {
            debuglock.unLock();
            return false;
        }
        debuglock.unLock();
    }
    return true;
}

/*
*  This function initializes the target session for debugging.
*/
Datum gms_debug_initialize(PG_FUNCTION_ARGS)
{
    StringInfoData buf;
    // TupleDesc tupdesc;
    int commIdx = -1;
    GMSInterfaceCheck("gms_debug.initialize", false);
    if (unlikely(u_sess->plsql_cxt.debug_proc_htbl == NULL)) {
        init_pldebug_htcl();
    }
    /* return nodename & socket idx as port */
    if (!PG_ARGISNULL(0)) {
        // check if the debug_session_id valid
        char *debug_session_id = text_to_cstring(PG_GETARG_VARCHAR_PP(0));
        char *psave = NULL;
        char *nodename = strtok_r(debug_session_id, "-", &psave);
        if (nodename == NULL) {
            ereport(ERROR, (errmsg("invalid debug_session_id  %s", debug_session_id)));
        }
        char *fir = AssignStr(psave, false);
        char *new_fir = TrimStr(fir);
        if (new_fir == NULL) {
            ereport(ERROR, (errmsg("invalid debug_session_id  %s", debug_session_id)));
        }
        commIdx = pg_strtoint32(new_fir);
        if (commIdx < 0 || commIdx >= PG_MAX_DEBUG_CONN) {
            ereport(ERROR, (errmsg("invalid debug_session_id  %s", debug_session_id)));
        }
        if (!AcquireDebugCommIdx(commIdx)) {
            ereport(ERROR, (errmsg("debug_session_id %s has already been used", debug_session_id)));
        }
    } else {
        commIdx = GetValidDebugCommIdx();
        if (commIdx == -1) {
            ereport(ERROR, (errmsg("max debug function is %d, turn_on function is out of range", PG_MAX_DEBUG_CONN)));
        }
    }

    SetDebugCommGmsUsed(commIdx, true);
    // dms_debug indicates that session debugging functionality should be enabled.
    u_sess->plsql_cxt.gms_debug_idx = commIdx;
    initStringInfo(&buf);
    // simple concatenate node_name and port with an underscore
    appendStringInfo(&buf, "%s-%d", g_instance.attr.attr_common.PGXCNodeName, commIdx);
    PG_RETURN_VARCHAR_P(cstring_to_text(buf.data));
}

/*
* This procedure notifies the debug session about the target program.
*/
Datum gms_debug_attach_session(PG_FUNCTION_ARGS)
{
    GMSInterfaceCheck("gms_debug.attach_session", false);
    char *debug_session_id = text_to_cstring(PG_GETARG_VARCHAR_PP(0));
    char *psave = NULL;
    int commidx = -1;
    char *nodename = strtok_r(debug_session_id, "-", &psave);
    char *fir = AssignStr(psave, false);
    char *new_fir = TrimStr(fir);
    if (new_fir == NULL) {
        ereport(ERROR, (  (errmsg("invalid debug_session_id  %s", debug_session_id))));
    }
    commidx = pg_strtoint32(new_fir);

    /* if is attach to some other function, just clean up it */
    clean_up_debug_client(true);
    /* this nodename check is only for single node */
    nodename = TrimStr(nodename);
    if (nodename == NULL || strcasecmp(nodename, g_instance.attr.attr_common.PGXCNodeName) != 0) {
        ereport(ERROR, (  errcode(ERRCODE_AMBIGUOUS_PARAMETER),
                        (errmsg("wrong debug nodename, should be %s.", g_instance.attr.attr_common.PGXCNodeName))));
    }
    if (commidx < 0 || commidx >= PG_MAX_DEBUG_CONN) {
        ereport(ERROR, (  errcode(ERRCODE_AMBIGUOUS_PARAMETER),
                        (errmsg("invalid debug port id %d.", commidx))));
    }
    /* only can attach when comm satisfy contidion */
    gms_attach_session(commidx, u_sess->session_id);
    u_sess->plsql_cxt.debug_client = InitDebugClient(commidx);
    PG_RETURN_VOID();
}

/**
 * This function sets a breakpoint in a program unit, which persists for the current session.
 */
Datum gms_debug_set_breakpoint(PG_FUNCTION_ARGS)
{
    const int DEBUG_BREAK_TUPLE_ATTR_NUM = 2;
    Oid funcOid = PG_GETARG_OID(0);
    int32 lineno = PG_GETARG_INT32(1);
    CodeLine *lines = NULL;
    CodeLine cl;
    cl.code = NULL;
    bool found = false;
    DebugClientInfo *client = u_sess->plsql_cxt.debug_client;
    StringInfoData str;

    if(client == nullptr) {
        ereport(ERROR,
            (errcode(ERRCODE_TARGET_SERVER_NOT_ATTACHED),
                errmsg("error happened in debug session, please reattach target session and try")));
    }

    TupleDesc tupdesc;
    MemoryContext oldcontext = MemoryContextSwitchTo(client->context);

    tupdesc = CreateTemplateTupleDesc(DEBUG_BREAK_TUPLE_ATTR_NUM, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "status", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "breakpoint", INT4OID, -1, 0);
    TupleDesc tuple_desc = BlessTupleDesc(tupdesc);

    Datum values[DEBUG_BREAK_TUPLE_ATTR_NUM];
    bool nulls[DEBUG_BREAK_TUPLE_ATTR_NUM];
    HeapTuple tuple;
    errno_t rc = 0;

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    (void)MemoryContextSwitchTo(oldcontext);

    values[0] = Int32GetDatum(0);
    values[1] = Int32GetDatum(-1);
    if (OidIsValid(funcOid)) {
        bool checked = GMSInterfaceCheck("gms_debug.add_breakpoint", true);
        if(!checked) {
            ereport(WARNING,
            (errcode(ERRCODE_TARGET_SERVER_NOT_ATTACHED),
                errmsg("target func not attached")));
            values[0] = Int32GetDatum(ERROR_BAD_HANDLE);
            tuple = heap_form_tuple(tuple_desc, values, nulls);
            PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
        }
    } else {
        ereport(WARNING, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                          errmsg("pl debugger only support function with language plpgsql"),
                          errdetail("the given function is %u", funcOid),
                          errcause("pl debugger do not support the given function"),
                          erraction("use pl debugger with only plpgsql function")));
        values[0] = Int32GetDatum(ERROR_BAD_HANDLE);
        tuple = heap_form_tuple(tuple_desc, values, nulls);
        PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
    }

    if (unlikely(u_sess->plsql_cxt.debug_proc_htbl == NULL)) {
        init_pldebug_htcl();
    }

    PlDebugEntry *entry =
        (PlDebugEntry *)hash_search(u_sess->plsql_cxt.debug_proc_htbl, (void *)(&funcOid), HASH_ENTER, &found);
    entry->key = funcOid;
    if (!found) {
        entry->commIdx = client->comm_idx;
        entry->func = NULL;
    }
    initStringInfo(&str);
    uint64 sid = ENABLE_THREAD_POOL ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid;
    appendStringInfo(&str, "%lu:%u:%d:%s", sid, funcOid, lineno, cl.code == NULL ? "NULL" : cl.code);
    debug_client_send_msg(client, GMS_DEBUG_ADDBREAKPOINT_HEADER, str.data, str.len);
    debug_client_rec_msg(client);
    int32 ans = pg_strtoint32(client->rec_buffer);
    pfree_ext(lines);
    pfree_ext(str.data);

    if (ans == ADD_BP_ERR_ALREADY_EXISTS) {
        ereport(WARNING, (errcode(ERRCODE_WARNING), 
                          errmsg("the given line number already contains a valid breakpoint.")));
        values[0] = Int32GetDatum(ERROR_ALREADY_EXISTS);
        tuple = heap_form_tuple(tuple_desc, values, nulls);
        PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
    } else if (ans == ADD_BP_ERR_OUT_OF_RANGE) {
        ereport(WARNING, (errcode(ERRCODE_WARNING),
                          errmsg("lineno must be within the range of [1, MaxLineNumber]." )));
        values[0] = Int32GetDatum(ERROR_ILLEGAL_LINE);
        tuple = heap_form_tuple(tuple_desc, values, nulls);
        PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
    } else if (ans == ADD_BP_ERR_INVALID_BP_POS) {
        ereport(WARNING, (errcode(ERRCODE_WARNING),
                          errmsg("the given line number does not name a valid breakpoint.")));
        values[0] = Int32GetDatum(ERROR_ILLEGAL_LINE);
        tuple = heap_form_tuple(tuple_desc, values, nulls);
        PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
    }
    values[0] = Int32GetDatum(0);
    values[1] = Int32GetDatum(ans);
    tuple = heap_form_tuple(tuple_desc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

static char* parse_and_valid(char **psave, const char* rec_buf)
{
    char *fir = strtok_r(NULL, ":", psave);
    char *new_fir = TrimStr(fir);
    if (new_fir == NULL) {
        ReportInvalidMsg(rec_buf);
        return NULL;
    }
    return new_fir;
}

static Datum build_runtime_info(DebugClientInfo *client, int err_code)
{
    const int DEBUG_RUNTIME_TUPLE_ATTR_NUM = 8;
    int i = 0;
    TupleDesc tupdesc;
    MemoryContext oldcontext = MemoryContextSwitchTo(client->context);

    tupdesc = CreateTemplateTupleDesc(DEBUG_RUNTIME_TUPLE_ATTR_NUM, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "err_code", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "run_line", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "run_breakpoint", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "run_stackdepth", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "run_reason", INT4OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "pro_namespace", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "pro_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)++i, "pro_owner", OIDOID, -1, 0);
    TupleDesc tuple_desc = BlessTupleDesc(tupdesc);

    /* Received buffer will be in the form of <namespaceoid, owneroid, lineno, breakpoint, stackdepth, reason,
     * pkgfuncname> */
    char *psave = NULL;
    char *fir = strtok_r(client->rec_buffer, ":", &psave);
    const int int64Size = 10;
    Oid namespaceoid;
    Oid funcoid;
    Oid owneroid;
    int run_line = -1;
    int run_breakpoint = -1;
    int run_stackdepth = -1;
    int run_reason = 0;
    char *pro_name = NULL;
    Datum values[DEBUG_RUNTIME_TUPLE_ATTR_NUM];
    bool nulls[DEBUG_RUNTIME_TUPLE_ATTR_NUM];
    HeapTuple tuple;
    errno_t rc = 0;

    if(err_code == 0) {
        char *new_fir = TrimStr(fir);
        if (new_fir == NULL) {
            ReportInvalidMsg(client->rec_buffer);
            PG_RETURN_DATUM(0);
        }
        funcoid = (Oid)pg_strtouint64(new_fir, NULL, int64Size);
        new_fir = parse_and_valid(&psave, client->rec_buffer);
        CHECK_RETURN_DATUM(new_fir);
        namespaceoid = (Oid)pg_strtouint64(new_fir, NULL, int64Size);
        new_fir = parse_and_valid(&psave, client->rec_buffer);
        CHECK_RETURN_DATUM(new_fir);
        owneroid = (Oid)pg_strtouint64(new_fir, NULL, int64Size);
        new_fir = parse_and_valid(&psave, client->rec_buffer);
        CHECK_RETURN_DATUM(new_fir);
        run_line = pg_strtoint32(new_fir);
        new_fir = parse_and_valid(&psave, client->rec_buffer);
        CHECK_RETURN_DATUM(new_fir);
        run_breakpoint = pg_strtoint32(new_fir);
        new_fir = parse_and_valid(&psave, client->rec_buffer);
        CHECK_RETURN_DATUM(new_fir);
        run_stackdepth = pg_strtoint32(new_fir);
        new_fir = parse_and_valid(&psave, client->rec_buffer);
        CHECK_RETURN_DATUM(new_fir);
        run_reason = pg_strtoint32(new_fir);
        pro_name = AssignStr(psave, false);
    }

    (void)MemoryContextSwitchTo(oldcontext);

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    i = 0;
    values[i++] = Int32GetDatum(err_code);
    values[i++] = Int32GetDatum(run_line);
    values[i++] = Int32GetDatum(run_breakpoint);
    values[i++] = Int32GetDatum(run_stackdepth);
    values[i++] = Int32GetDatum(run_reason);
    values[i++] = ObjectIdGetDatum(namespaceoid);
    values[i++] = CStringGetTextDatum(pro_name);
    values[i++] = ObjectIdGetDatum(owneroid);
    tuple = heap_form_tuple(tuple_desc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/**
 * This function passes the given breakflags 
 * (a mask of the events that are of interest) to Probe in the target process.
 * It tells Probe to continue execution of the target process, 
 * and it waits until the target process runs to completion or signals an event.
 */
Datum gms_debug_continue(PG_FUNCTION_ARGS)
{
    DebugClientInfo *client = u_sess->plsql_cxt.debug_client;
    if(client == nullptr) {
        ereport(ERROR,
            (errcode(ERRCODE_TARGET_SERVER_NOT_ATTACHED),
                errmsg("error happened in debug session, please reattach target session and try")));
    }
    const int DEBUG_ACTION_ATTR_NUM = 5;
    const char actions[DEBUG_ACTION_ATTR_NUM] = {
        GMS_DEBUG_CONTINUE_HEADER, 
        GMS_DEBUG_NEXT_HEADER,
        GMS_DEBUG_STEP_INTO_HEADER,
        GMS_DEBUG_FINISH_HEADER,
        GMS_DEBUG_ABORT_HEADER 
    };
    int err_code = 0;
    bool checked = GMSInterfaceCheck("gms_debug.continue", true);
    if(!checked) {
        ereport(WARNING,
        (errcode(ERRCODE_TARGET_SERVER_NOT_ATTACHED),
            errmsg("target func not attached")));
        err_code = ERROR_FUNC_NOT_ATTACHED;
        return build_runtime_info(client,err_code);
    }
    int32 breakflags = PG_GETARG_INT32(0);
    char action;
    std::bitset<DEBUG_ACTION_ATTR_NUM> breakflags_bitset(breakflags);
     for (std::size_t i = 0; i < breakflags_bitset.size(); ++i) {
        if (breakflags_bitset.test(i)) {
            action = actions[i]; 
        }
    }
    debug_client_send_msg(client, action, NULL, 0);
    debug_client_rec_msg(client);
    return build_runtime_info(client,err_code);
}

/***
 * This function returns information about the current program. 
 */
Datum gms_debug_get_runtime_info(PG_FUNCTION_ARGS)
{
    int err_code = 0;
    DebugClientInfo *client = u_sess->plsql_cxt.debug_client;
    if(client == nullptr) {
        ereport(ERROR,
            (errcode(ERRCODE_TARGET_SERVER_NOT_ATTACHED),
                errmsg("error happened in debug session, please reattach target session and try")));
    }
    bool checked = GMSInterfaceCheck("gms_debug.runtime_info",true);
    if(!checked) {
        ereport(WARNING,
        (errcode(ERRCODE_TARGET_SERVER_NOT_ATTACHED),
            errmsg("target func not attached")));
        err_code = ERROR_FUNC_NOT_ATTACHED;
        return build_runtime_info(client, err_code);
    }
    debug_client_send_msg(client, GMS_DEBUG_RUNTIMEINFO_HEADER, NULL, 0);
    debug_client_rec_msg(client);
   return build_runtime_info(client, err_code);
}

/***
 * This procedure notifies the target session that 
 * debugging should no longer take place in that session. 
 * It is not necessary to call this function before ending the session.
 */
Datum gms_debug_off(PG_FUNCTION_ARGS)
{
    GMSInterfaceCheck("gms_debug.debug_off",false);
    // dms_debug indicates that session debugging functionality should be enabled.
    if ((u_sess->plsql_cxt.gms_debug_idx == -1)) {
       PG_RETURN_VOID();
    }
    SetDebugCommGmsUsed(u_sess->plsql_cxt.gms_debug_idx, false);
    u_sess->plsql_cxt.gms_debug_idx = -1;

    if ((u_sess->plsql_cxt.debug_proc_htbl == NULL)) {
       PG_RETURN_VOID();
    }

    PlDebugEntry* elem = NULL;
    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, u_sess->plsql_cxt.debug_proc_htbl);

    /* Fetch all debugger entry info from the hash table */
    while ((elem = (PlDebugEntry*)hash_seq_search(&hash_seq)) != NULL) {
         if (elem->func && elem->func->debug) {
             clean_up_debug_server(elem->func->debug, true, false);
          }
    }

    PG_RETURN_VOID();
}

/***
 * This procedure stops debugging the target program.
 * This procedure may be called at any time, 
 * but it does not notify the target session that the debug session is detaching itself, 
 * and it does not terminate execution of the target session. 
 * Therefore, care should be taken to ensure that the target session does not hang itself.
 */
Datum gms_debug_detach_session(PG_FUNCTION_ARGS)
{
    clean_up_debug_client();
    PG_RETURN_VOID();
}
