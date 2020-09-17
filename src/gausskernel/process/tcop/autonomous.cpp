/*--------------------------------------------------------------------------
 *
 * autonomous.cpp
 *        Run SQL commands using a background worker.
 *
 * Copyright (C) 2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        src/gausskernel/process/tcop/autonomous.cpp
 *
 *
 * This implements a C API to open an autonomous session and run SQL queries
 * in it.  The session looks much like a normal database connection, but it is
 * always to the same database, and there is no authentication needed.  The
 * "backend" for that connection is a background worker.  The normal backend
 * and the autonomous session worker communicate over the normal FE/BE
 * protocol.
 *
 * Types:
 *
 * AutonomousSession -- opaque connection handle
 * AutonomousPreparedStatement -- opaque prepared statement handle
 * AutonomousResult -- query result
 *
 * Functions:
 *
 * AutonomousSessionStart() -- start a session (launches background worker)
 * and return a handle
 *
 * AutonomousSessionEnd() -- close session and free resources
 *
 * AutonomousSessionExecute() -- run SQL string and return result (rows or
 * status)
 *
 * AutonomousSessionPrepare() -- prepare an SQL string for subsequent
 * execution
 *
 * AutonomousSessionExecutePrepared() -- run prepared statement
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "gs_thread.h"

#include "access/htup.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "commands/async.h"
#include "commands/variable.h"
#include "lib/stringinfo.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqmq.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "tcop/autonomous.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/ps_status.h"

/* Table-of-contents constants for our dynamic shared memory segment. */
#define AUTONOMOUS_MAGIC                0x50674267

#define AUTONOMOUS_KEY_FIXED_DATA        0
#define AUTONOMOUS_KEY_GUC                1
#define AUTONOMOUS_KEY_COMMAND_QUEUE    2
#define AUTONOMOUS_KEY_RESPONSE_QUEUE    3
#define AUTONOMOUS_NKEYS                4

#define AUTONOMOUS_QUEUE_SIZE            16384

/* Fixed-size data passed via our dynamic shared memory segment. */
struct autonomous_session_fixed_data {
    Oid database_id;
    Oid authenticated_user_id;
    Oid current_user_id;
    int sec_context;
};

struct AutonomousSession {
    char *seg;
    BackgroundWorkerHandle *worker_handle;
    shm_mq_handle *command_qh;
    shm_mq_handle *response_qh;
    int transaction_status;
};

struct AutonomousPreparedStatement {
    AutonomousSession *session;
    Oid *argtypes;
    TupleDesc tupdesc;
};

static void shm_mq_receive_stringinfo(shm_mq_handle *qh, StringInfoData *msg);
static void autonomous_check_client_encoding_hook(void);
static TupleDesc TupleDesc_from_RowDescription(StringInfo msg);
static HeapTuple HeapTuple_from_DataRow(TupleDesc tupdesc, StringInfo msg);
static void forward_NotifyResponse(StringInfo msg);
static void rethrow_errornotice(StringInfo msg);
static void invalid_protocol_message(char msgtype);

AutonomousSession *AutonomousSessionStart(void)
{
    BackgroundWorker worker = {0};
    ThreadId pid;
    AutonomousSession *session = NULL;
    shm_toc_estimator e;
    Size segsize;
    Size guc_len;
    char *gucstate = NULL;
    char *seg = NULL;
    shm_toc *toc = NULL;
    autonomous_session_fixed_data *fdata = NULL;
    shm_mq *command_mq = NULL;
    shm_mq *response_mq = NULL;
    BgwHandleStatus bgwstatus;
    StringInfoData msg;
    char msgtype;
    errno_t rc;

    session = (AutonomousSession *)palloc(sizeof(*session));

    shm_toc_initialize_estimator(&e);
    shm_toc_estimate_chunk(&e, sizeof(autonomous_session_fixed_data));
    shm_toc_estimate_chunk(&e, AUTONOMOUS_QUEUE_SIZE);
    shm_toc_estimate_chunk(&e, AUTONOMOUS_QUEUE_SIZE);
    guc_len = EstimateGUCStateSpace();
    shm_toc_estimate_chunk(&e, guc_len);
    shm_toc_estimate_keys(&e, AUTONOMOUS_NKEYS);
    segsize = shm_toc_estimate(&e);
    seg = (char *)palloc(sizeof(char) * segsize);

    session->seg = seg;

    toc = shm_toc_create(AUTONOMOUS_MAGIC, seg, segsize);

    /* Store fixed-size data in dynamic shared memory. */
    fdata = (autonomous_session_fixed_data *)shm_toc_allocate(toc, sizeof(*fdata));
    fdata->database_id = u_sess->proc_cxt.MyDatabaseId;
    fdata->authenticated_user_id = GetAuthenticatedUserId();
    GetUserIdAndSecContext(&fdata->current_user_id, &fdata->sec_context);
    shm_toc_insert(toc, AUTONOMOUS_KEY_FIXED_DATA, fdata);

    /* Store GUC state in dynamic shared memory. */
    gucstate = (char *)shm_toc_allocate(toc, guc_len);
    SerializeGUCState(guc_len, gucstate);
    shm_toc_insert(toc, AUTONOMOUS_KEY_GUC, gucstate);

    command_mq = shm_mq_create(shm_toc_allocate(toc, AUTONOMOUS_QUEUE_SIZE),
                               AUTONOMOUS_QUEUE_SIZE);
    shm_toc_insert(toc, AUTONOMOUS_KEY_COMMAND_QUEUE, command_mq);
    shm_mq_set_sender(command_mq, t_thrd.proc);

    response_mq = shm_mq_create(shm_toc_allocate(toc, AUTONOMOUS_QUEUE_SIZE),
                                AUTONOMOUS_QUEUE_SIZE);
    shm_toc_insert(toc, AUTONOMOUS_KEY_RESPONSE_QUEUE, response_mq);
    shm_mq_set_receiver(response_mq, t_thrd.proc);

    session->command_qh = shm_mq_attach(command_mq, seg, NULL);
    session->response_qh = shm_mq_attach(response_mq, seg, NULL);

    worker.bgw_flags =
        BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    rc = snprintf_s(worker.bgw_library_name, BGW_MAXLEN, BGW_MAXLEN, "postgres");
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(worker.bgw_function_name, BGW_MAXLEN, BGW_MAXLEN, "autonomous_worker_main");
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(worker.bgw_name, BGW_MAXLEN, BGW_MAXLEN, "autonomous session by PID %lu", 
                    t_thrd.proc_cxt.MyProcPid);
    securec_check_ss(rc, "\0", "\0");
    worker.bgw_main_arg = PointerGetDatum(seg);
    worker.bgw_notify_pid = t_thrd.proc_cxt.MyProcPid;

    if (!RegisterDynamicBackgroundWorker(&worker, &session->worker_handle))
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("could not register background process"),
                 errhint("You might need to increase max_background_workers.")));

    shm_mq_set_handle(session->command_qh, session->worker_handle);
    shm_mq_set_handle(session->response_qh, session->worker_handle);

    t_thrd.autonomous_cxt.handle = session->worker_handle;

    bgwstatus = WaitForBackgroundWorkerStartup(session->worker_handle, &pid);
    if (bgwstatus != BGWH_STARTED)
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("could not start background worker")));

    do {
        ereport(LOG, (errmsg("front begin receive msg")));
        shm_mq_receive_stringinfo(session->response_qh, &msg);
        ereport(LOG, (errmsg("front end receive msg")));
        ereport(LOG, (errmsg("front function AutonomousSessionStart receive msg %s", msg.data)));
        msgtype = pq_getmsgbyte(&msg);

        switch (msgtype) {
            case 'E':
            case 'N':
                rethrow_errornotice(&msg);
                break;
            case 'Z':
                session->transaction_status = pq_getmsgbyte(&msg);
                pq_getmsgend(&msg);
                break;
            default:
                invalid_protocol_message(msgtype);
                break;
        }
    }
    while (msgtype != 'Z');

    return session;
}

void AutonomousSessionEnd(AutonomousSession *session)
{
    StringInfoData msg;
    BgwHandleStatus bgwstatus;
    if (session->transaction_status == 'T')
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("autonomous session ended with transaction block open")));

    pq_redirect_to_shm_mq(session->command_qh);
    pq_beginmessage(&msg, 'X');
    pq_endmessage(&msg);
    pq_stop_redirect_to_shm_mq();
    bgwstatus = WaitForBackgroundWorkerShutdown(session->worker_handle);
    if (bgwstatus != BGWH_STOPPED)
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("could not stop background worker")));
    pfree(session->worker_handle);
    pfree(session->seg);
    pfree(session);
    t_thrd.autonomous_cxt.handle = NULL;
}

AutonomousResult *AutonomousSessionExecute(AutonomousSession *session, const char *sql)
{
    StringInfoData msg;
    char        msgtype;
    AutonomousResult *result = NULL;

    pq_redirect_to_shm_mq(session->command_qh);
    pq_beginmessage(&msg, 'Q');
    pq_sendstring(&msg, sql);
    pq_endmessage(&msg);
    pq_stop_redirect_to_shm_mq();

    result = (AutonomousResult *)palloc0(sizeof(*result));

    do {
        shm_mq_receive_stringinfo(session->response_qh, &msg);
        ereport(LOG, (errmsg("front function AutonomousSessionExecute receive msg %s", msg.data)));
        msgtype = pq_getmsgbyte(&msg);

        switch (msgtype) {
            case 'A':
                forward_NotifyResponse(&msg);
                break;
            case 'C':
                {
                    const char *tag = pq_getmsgstring(&msg);
                    result->command = pstrdup(tag);
                    pq_getmsgend(&msg);
                    break;
                }
            case 'D':
                if (!result->tupdesc)
                    elog(ERROR, "no T before D");
                result->tuples = lappend(result->tuples, HeapTuple_from_DataRow(result->tupdesc, &msg));
                pq_getmsgend(&msg);
                break;
            case 'E':
            case 'N':
                rethrow_errornotice(&msg);
                break;
            case 'T':
                if (result->tupdesc)
                    elog(ERROR, "already received a T message");
                result->tupdesc = TupleDesc_from_RowDescription(&msg);
                pq_getmsgend(&msg);
                break;
            case 'Z':
                session->transaction_status = pq_getmsgbyte(&msg);
                pq_getmsgend(&msg);
                break;
            default:
                invalid_protocol_message(msgtype);
                break;
        }
    }
    while (msgtype != 'Z');
    return result;
}

AutonomousPreparedStatement *AutonomousSessionPrepare(AutonomousSession *session, const char *sql, int16 nargs,
                                                      Oid argtypes[], const char *argnames[])
{
    AutonomousPreparedStatement *result = NULL;
    StringInfoData msg;
    int16        i;
    char        msgtype;

    pq_redirect_to_shm_mq(session->command_qh);
    pq_beginmessage(&msg, 'P');
    pq_sendstring(&msg, "");
    pq_sendstring(&msg, sql);
    pq_sendint16(&msg, (uint16)nargs);
    for (i = 0; i < nargs; i++)
        pq_sendint32(&msg, (uint32)argtypes[i]);
    if (argnames)
        for (i = 0; i < nargs; i++)
            pq_sendstring(&msg, argnames[i]);
    pq_endmessage(&msg);
    pq_stop_redirect_to_shm_mq();

    result = (AutonomousPreparedStatement *)palloc0(sizeof(*result));
    result->session = session;
    result->argtypes = (Oid *)palloc(nargs * sizeof(*result->argtypes));
    errno_t rc;
    rc = memcpy_s(result->argtypes, nargs * sizeof(*result->argtypes), argtypes, nargs * sizeof(*result->argtypes));
    securec_check(rc, "\0", "\0");

    shm_mq_receive_stringinfo(session->response_qh, &msg);
    ereport(LOG, (errmsg("front function AutonomousSessionPrepare receive msg %s", msg.data)));
    msgtype = pq_getmsgbyte(&msg);

    switch (msgtype) {
        case '1':
            break;
        case 'E':
            rethrow_errornotice(&msg);
            break;
        default:
            invalid_protocol_message(msgtype);
            break;
    }
    pq_redirect_to_shm_mq(session->command_qh);
    pq_beginmessage(&msg, 'D');
    pq_sendbyte(&msg, 'S');
    pq_sendstring(&msg, "");
    pq_endmessage(&msg);
    pq_stop_redirect_to_shm_mq();

    do {
        shm_mq_receive_stringinfo(session->response_qh, &msg);
        ereport(LOG, (errmsg("front function AutonomousSessionPrepare receive msg %s", msg.data)));
        msgtype = pq_getmsgbyte(&msg);

        switch (msgtype) {
            case 'A':
                forward_NotifyResponse(&msg);
                break;
            case 'E':
                rethrow_errornotice(&msg);
                break;
            case 'n':
                break;
            case 't':
                /* ignore for now */
                break;
            case 'T':
                if (result->tupdesc)
                    elog(ERROR, "already received a T message");
                result->tupdesc = TupleDesc_from_RowDescription(&msg);
                pq_getmsgend(&msg);
                break;
            default:
                invalid_protocol_message(msgtype);
                break;
        }
    }
    while (msgtype != 'n' && msgtype != 'T');

    return result;
}

AutonomousResult *AutonomousSessionExecutePrepared(AutonomousPreparedStatement *stmt, int16 nargs, 
                                                   Datum *values, bool *nulls)
{
    AutonomousSession *session = NULL;
    StringInfoData msg;
    AutonomousResult *result = NULL;
    char msgtype;
    int16 i;

    session = stmt->session;

    pq_redirect_to_shm_mq(session->command_qh);
    pq_beginmessage(&msg, 'B');
    pq_sendstring(&msg, "");
    pq_sendstring(&msg, "");
    pq_sendint16(&msg, 1);  /* number of parameter format codes */
    pq_sendint16(&msg, 1);
    pq_sendint16(&msg, (uint16)nargs);  /* number of parameter values */
    for (i = 0; i < nargs; i++) {
        if (nulls[i])
            pq_sendint32(&msg, -1);
        else {
            Oid typsend;
            bool typisvarlena;
            bytea *outputbytes = NULL;

            getTypeBinaryOutputInfo(stmt->argtypes[i], &typsend, &typisvarlena);
            outputbytes = OidSendFunctionCall(typsend, values[i]);
            pq_sendint32(&msg, VARSIZE(outputbytes) - VARHDRSZ);
            pq_sendbytes(&msg, VARDATA(outputbytes), VARSIZE(outputbytes) - VARHDRSZ);
            pfree(outputbytes);
        }
    }
    pq_sendint16(&msg, 1);  /* number of result column format codes */
    pq_sendint16(&msg, 1);
    pq_endmessage(&msg);
    pq_stop_redirect_to_shm_mq();

    shm_mq_receive_stringinfo(session->response_qh, &msg);
    ereport(LOG, (errmsg("front function AutonomousSessionExecutePrepared receive msg %s", msg.data)));
    msgtype = pq_getmsgbyte(&msg);

    switch (msgtype) {
        case '2':
            break;
        case 'E':
            rethrow_errornotice(&msg);
            break;
        default:
            invalid_protocol_message(msgtype);
            break;
    }

    pq_redirect_to_shm_mq(session->command_qh);
    pq_beginmessage(&msg, 'E');
    pq_sendstring(&msg, "");
    pq_sendint32(&msg, 0);
    pq_endmessage(&msg);
    pq_stop_redirect_to_shm_mq();

    result = (AutonomousResult *)palloc0(sizeof(*result));
    result->tupdesc = stmt->tupdesc;

    do {
        shm_mq_receive_stringinfo(session->response_qh, &msg);
        ereport(LOG, (errmsg("front function AutonomousSessionExecutePrepared receive msg %s", msg.data)));
        msgtype = pq_getmsgbyte(&msg);

        switch (msgtype) {
            case 'A':
                forward_NotifyResponse(&msg);
                break;
            case 'C':
                {
                    const char *tag = pq_getmsgstring(&msg);
                    result->command = pstrdup(tag);
                    pq_getmsgend(&msg);
                    break;
                }
            case 'D':
                if (!stmt->tupdesc)
                    elog(ERROR, "did not expect any rows");
                result->tuples = lappend(result->tuples, HeapTuple_from_DataRow(stmt->tupdesc, &msg));
                pq_getmsgend(&msg);
                break;
            case 'E':
            case 'N':
                rethrow_errornotice(&msg);
                break;
            default:
                invalid_protocol_message(msgtype);
                break;
        }
    }
    while (msgtype != 'C');

    pq_redirect_to_shm_mq(session->command_qh);
    pq_putemptymessage('S');
    pq_stop_redirect_to_shm_mq();

    shm_mq_receive_stringinfo(session->response_qh, &msg);
    ereport(LOG, (errmsg("front function AutonomousSessionExecutePrepared receive msg %s", msg.data)));
    msgtype = pq_getmsgbyte(&msg);

    switch (msgtype) {
        case 'A':
            forward_NotifyResponse(&msg);
            break;
        case 'Z':
            session->transaction_status = pq_getmsgbyte(&msg);
            pq_getmsgend(&msg);
            break;
        default:
            invalid_protocol_message(msgtype);
            break;
    }

    return result;
}

void autonomous_worker_main(Datum main_arg)
{
    char *seg = NULL;
    shm_toc *toc = NULL;
    autonomous_session_fixed_data *fdata = NULL;
    char *gucstate = NULL;
    shm_mq *command_mq = NULL;
    shm_mq *response_mq = NULL;
    shm_mq_handle *command_qh = NULL;
    shm_mq_handle *response_qh = NULL;
    StringInfoData msg;

    char msgtype;

    BackgroundWorkerUnblockSignals();

    t_thrd.autonomous_cxt.isnested = true;

    /* Set up a memory context and resource owner. */
    Assert(t_thrd.utils_cxt.CurrentResourceOwner == NULL);
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "autonomous");
    CurrentMemoryContext = AllocSetContextCreate(t_thrd.top_mem_cxt,
                                                 "autonomous session",
                                                 ALLOCSET_DEFAULT_MINSIZE,
                                                 ALLOCSET_DEFAULT_INITSIZE,
                                                 ALLOCSET_DEFAULT_MAXSIZE);

    seg = (char *)DatumGetPointer(main_arg);
    if (seg == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("could not map dynamic shared memory segment")));

    toc = shm_toc_attach(AUTONOMOUS_MAGIC, seg);
    if (toc == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("bad magic number in dynamic shared memory segment")));

    /* Find data structures in dynamic shared memory. */
    fdata = (autonomous_session_fixed_data *)shm_toc_lookup(toc, AUTONOMOUS_KEY_FIXED_DATA);

    gucstate = (char *)shm_toc_lookup(toc, AUTONOMOUS_KEY_GUC);

    command_mq = (shm_mq *)shm_toc_lookup(toc, AUTONOMOUS_KEY_COMMAND_QUEUE);
    shm_mq_set_receiver(command_mq, t_thrd.proc);
    command_qh = shm_mq_attach(command_mq, seg, NULL);

    response_mq = (shm_mq *)shm_toc_lookup(toc, AUTONOMOUS_KEY_RESPONSE_QUEUE);
    shm_mq_set_sender(response_mq, t_thrd.proc);
    response_qh = shm_mq_attach(response_mq, seg, NULL);

    if (!t_thrd.msqueue_cxt.is_changed) {
        pq_redirect_to_shm_mq(response_qh);
    }
    BackgroundWorkerInitializeConnectionByOid(fdata->database_id,
                                              fdata->authenticated_user_id);

    (void)SetClientEncoding(GetDatabaseEncoding());

    StartTransactionCommand();
    RestoreGUCState(gucstate);
    CommitTransactionCommand();

    process_local_preload_libraries();

    SetUserIdAndSecContext(fdata->current_user_id, fdata->sec_context);

    t_thrd.postgres_cxt.whereToSendOutput = DestRemote;
    ReadyForQuery((CommandDest)t_thrd.postgres_cxt.whereToSendOutput);

    t_thrd.mem_cxt.msg_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
                                           "MessageContext",
                                           ALLOCSET_DEFAULT_MINSIZE,
                                           ALLOCSET_DEFAULT_INITSIZE,
                                           ALLOCSET_DEFAULT_MAXSIZE);

    do {
        (void)MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
        MemoryContextResetAndDeleteChildren(t_thrd.mem_cxt.msg_mem_cxt);

        if (IsAbortedTransactionBlockState()) {
            set_ps_display("idle in transaction (aborted)", false);
            pgstat_report_activity(STATE_IDLEINTRANSACTION_ABORTED, NULL);
        } else if (IsTransactionOrTransactionBlock()) {
            set_ps_display("idle in transaction", false);
            pgstat_report_activity(STATE_IDLEINTRANSACTION, NULL);
        } else {
            ProcessCompletedNotifies();
            pgstat_report_stat(false);

            set_ps_display("idle", false);
            pgstat_report_activity(STATE_IDLE, NULL);
        }

        shm_mq_receive_stringinfo(command_qh, &msg);
        ereport(LOG, (errmsg("bgworker receive msg %s", msg.data)));
        msgtype = pq_getmsgbyte(&msg);

        switch (msgtype) {
            case 'B':
                {
                    SetCurrentStatementStartTimestamp();
                    exec_bind_message(&msg);
                    break;
                }
            case 'D':
                {
                    int describe_type;
                    const char *describe_target;

                    SetCurrentStatementStartTimestamp();

                    describe_type = pq_getmsgbyte(&msg);
                    describe_target = pq_getmsgstring(&msg);
                    pq_getmsgend(&msg);

                    switch (describe_type) {
                        case 'S':
                            exec_describe_statement_message(describe_target);
                            break;
#ifdef TODO
                        case 'P':
                            exec_describe_portal_message(describe_target);
                            break;
#endif
                        default:
                            ereport(ERROR,
                                    (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                     errmsg("invalid DESCRIBE message subtype %d",
                                            describe_type)));
                            break;
                    }
                }
                break;
            case 'E':
                {
                    const char *portal_name;
                    int max_rows;

                    SetCurrentStatementStartTimestamp();

                    portal_name = pq_getmsgstring(&msg);
                    max_rows = (int)pq_getmsgint(&msg, 4);
                    pq_getmsgend(&msg);

                    exec_execute_message(portal_name, max_rows);
                }
                break;

            case 'P':
                {
                    const char *stmt_name;
                    const char *query_string;
                    uint16 numParams;
                    Oid *paramTypes = NULL;
                    char **paramTypeNames = NULL;

                    SetCurrentStatementStartTimestamp();

                    stmt_name = pq_getmsgstring(&msg);
                    query_string = pq_getmsgstring(&msg);
                    numParams = pq_getmsgint(&msg, 2);
                    if (numParams > 0) {
                        int i;

                        paramTypes = (Oid *)palloc(numParams * sizeof(Oid));
                        for (i = 0; i < numParams; i++)
                            paramTypes[i] = pq_getmsgint(&msg, 4);
                    }
                    /* If data left in message, read parameter names. */
                    if (msg.cursor != msg.len) {
                        int i;

                        paramTypeNames = (char **)palloc(numParams * sizeof(char *));
                        for (i = 0; i < numParams; i++)
                            paramTypeNames[i] = (char *)pq_getmsgstring(&msg);
                    }
                    pq_getmsgend(&msg);

                    exec_parse_message(query_string, stmt_name, paramTypes, paramTypeNames, (int)numParams);
                    break;
                }
            case 'Q':
                {
                    const char *sql;
                    int save_log_statement;
                    bool save_log_duration;
                    int save_log_min_duration_statement;

                    sql = pq_getmsgstring(&msg);
                    pq_getmsgend(&msg);

                    /* XXX room for improvement */
                    save_log_statement = u_sess->attr.attr_common.log_statement;
                    save_log_duration = u_sess->attr.attr_sql.log_duration;
                    save_log_min_duration_statement = u_sess->attr.attr_storage.log_min_duration_statement;

                    t_thrd.autonomous_cxt.check_client_encoding_hook = autonomous_check_client_encoding_hook;
                    u_sess->attr.attr_common.log_statement = LOGSTMT_NONE;
                    u_sess->attr.attr_sql.log_duration = false;
                    u_sess->attr.attr_storage.log_min_duration_statement = -1;

                    SetCurrentStatementStartTimestamp();
                    exec_simple_query(sql, QUERY_MESSAGE);

                    u_sess->attr.attr_common.log_statement = save_log_statement;
                    u_sess->attr.attr_sql.log_duration = save_log_duration;
                    u_sess->attr.attr_storage.log_min_duration_statement = save_log_min_duration_statement;
                    t_thrd.autonomous_cxt.check_client_encoding_hook = NULL;

                    ReadyForQuery((CommandDest)t_thrd.postgres_cxt.whereToSendOutput);
                    break;
                }
            case 'S':
                {
                    pq_getmsgend(&msg);
                    finish_xact_command();
                    ReadyForQuery((CommandDest)t_thrd.postgres_cxt.whereToSendOutput);
                    break;
                }
            case 'X':
            case 'N':
                break;
            default:
                ereport(ERROR,
                        (errcode(ERRCODE_PROTOCOL_VIOLATION),
                         errmsg("invalid protocol message type from autonomous session leader: %c",
                                msgtype)));
                break;
        }
    }
    while ((msgtype != 'X') && !(t_thrd.bgworker_cxt.worker_shutdown_requested));
}

static void shm_mq_receive_stringinfo(shm_mq_handle *qh, StringInfoData *msg)
{
    shm_mq_result res;
    Size nbytes = 0;
    void *data = NULL;

    res = shm_mq_receive(qh, &nbytes, &data, false);
    if (res != SHM_MQ_SUCCESS)
        elog(ERROR, "shm_mq_receive failed: %d", res);
    initStringInfo(msg);
    appendBinaryStringInfo(msg, (const char*)data, (int)nbytes);
}

static void autonomous_check_client_encoding_hook(void)
{
    elog(ERROR, "cannot set client encoding in autonomous session");
}

static TupleDesc TupleDesc_from_RowDescription(StringInfo msg)
{
    TupleDesc tupdesc;
    int16 natts = pq_getmsgint(msg, 2);
    int16 i;

    tupdesc = CreateTemplateTupleDesc(natts, false);
    for (i = 0; i < natts; i++) {
        const char *colname;
        Oid type_oid;
        uint32 typmod;
        uint16 format;

        colname = pq_getmsgstring(msg);
        (void) pq_getmsgint(msg, 4);   /* table OID */
        (void) pq_getmsgint(msg, 2);   /* table attnum */
        type_oid = pq_getmsgint(msg, 4);
        (void) pq_getmsgint(msg, 2);   /* type length */
        typmod = pq_getmsgint(msg, 4);
        format = pq_getmsgint(msg, 2);
        (void) format;
#ifdef TODO
        /* XXX The protocol sometimes sends 0 (text) if the format is not
         * determined yet.  We always use binary, so this check is probably
         * not useful. */
        if (format != 1)
            elog(ERROR, "format must be binary");
#endif

        TupleDescInitEntry(tupdesc, i + 1, colname, type_oid, typmod, 0);
    }
    return tupdesc;
}

static HeapTuple HeapTuple_from_DataRow(TupleDesc tupdesc, StringInfo msg)
{
    int16 natts = pq_getmsgint(msg, 2);
    int16 i;
    Datum *values;
    bool *nulls;
    StringInfoData buf;

    Assert(tupdesc);

    if (natts != tupdesc->natts)
        elog(ERROR, "malformed DataRow");

    values = (Datum *)palloc(natts * sizeof(*values));
    nulls = (bool *)palloc(natts * sizeof(*nulls));
    initStringInfo(&buf);

    for (i = 0; i < natts; i++) {
        int32 len = pq_getmsgint(msg, 4);
        if (len < 0)
            nulls[i] = true;
        else {
            Oid recvid;
            Oid typioparams;
            nulls[i] = false;
            getTypeBinaryInputInfo(tupdesc->attrs[i]->atttypid,
                                   &recvid,
                                   &typioparams);
            resetStringInfo(&buf);
            appendBinaryStringInfo(&buf, pq_getmsgbytes(msg, len), len);
            values[i] = OidReceiveFunctionCall(recvid, &buf, typioparams,
                                               tupdesc->attrs[i]->atttypmod);
        }
    }

    return heap_form_tuple(tupdesc, values, nulls);
}

static void forward_NotifyResponse(StringInfo msg)
{
    int32 pid;
    const char *channel;
    const char *payload;

    pid = (int32)pq_getmsgint(msg, 4);
    channel = pq_getmsgrawstring(msg);
    payload = pq_getmsgrawstring(msg);
    pq_endmessage(msg);

    NotifyMyFrontEnd(channel, payload, pid);
}

static void rethrow_errornotice(StringInfo msg)
{
    ErrorData edata;
    pq_parse_errornotice(msg, &edata);
    edata.elevel = Min(edata.elevel, ERROR);
    ThrowErrorData(&edata);
}

static void invalid_protocol_message(char msgtype)
{
    ereport(ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
             errmsg("invalid protocol message type from autonomous session: %c",
                    msgtype)));
}

