/*
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
 *
 * AutonomousSession.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/process/tcop/autonomoustransaction.cpp
 *
 * -------------------------------------------------------------------------
 */


#include "tcop/autonomoustransaction.h"
#include "postgres.h"
#include "commands/dbcommands.h"
#include "knl/knl_variable.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "utils/lsyscache.h"
#include "utils/plpgsql.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/typcache.h"
#include "storage/lmgr.h"
#include "commands/user.h"
#define MAX_LINESIZE 32767

extern bool PQsendQueryStart(PGconn *conn);
extern Datum RecordCstringGetDatum(TupleDesc tupdesc, char* string);
extern void send_only_message_to_frontend(const char* message, bool is_putline);

static bool PQexecStart(PGconn* conn);
static int PQsendQueryAutonm(PGconn* conn, const char* query);
static PGresult* PQexecAutonm(PGconn* conn, const char* query, int64 currentXid, bool isLockWait = false);
static PGresult* PQexecAutonmFinish(PGconn* conn);

static void ProcessorNotice(void* arg, const char* message);

const int MAX_CONNINFO_SIZE = 512;
pg_atomic_uint32 AutonomousSession::m_sessioncnt = 0;

/*
 * AddSessionCount()
 * m_sessioncnt Number of sessions started by 
 * autonomous transactions on the current node
 */
void AutonomousSession::AddSessionCount(void)
{
    if (GetSessionCount() < (uint32)g_instance.attr.attr_storage.max_concurrent_autonomous_transactions) {
        AddRefcount();
        ereport(DEBUG2, (errmodule(MOD_PLSQL), errmsg("autonomous transaction inc session : %d", m_sessioncnt)));
    } else {
        ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR),
                errmsg("concurrent autonomous transactions reach its maximun : %d",
                    g_instance.attr.attr_storage.max_concurrent_autonomous_transactions)));
    }
}

void AutonomousSession::ReduceSessionCount()
{
    if (GetSessionCount() > 0) {
        SubRefCount();
        ereport(DEBUG2, (errmodule(MOD_PLSQL), errmsg("autonomous transaction dec session : %d", m_sessioncnt)));
    } else {
        if (!t_thrd.proc_cxt.proc_exit_inprogress){
            ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR),
                errmsg("The number of concurrent autonomous transactions is too small."),
                errdetail("Number of current connections: %d", m_sessioncnt),
                errcause("System error."),
                erraction("Contact Huawei Engineer.")));
        }
    }
}

/*
 * AttachSession()
 * Connecting to the server through libpq
 */
void AutonomousSession::AttachSession(void)
{
    if (m_conn != NULL) {  // create session alike singleton
        return;
    }

    /* create a connection info with current database info */
    char connInfo[MAX_CONNINFO_SIZE];
    char userName[NAMEDATALEN];
    const char* dbName = get_database_name(u_sess->proc_cxt.MyDatabaseId);
    if (dbName == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE), errmsg("database with OID %u does not exist",
                                                                    u_sess->proc_cxt.MyDatabaseId)));
    }
    /*
     * It shares a connection timeout interval with gsql CONNECT_TIMEOUT.
     * You can also define a connection timeout interval,
     * but this timeout interval is meaningless.
     */
    errno_t ret = snprintf_s(connInfo, sizeof(connInfo), sizeof(connInfo) - 1,
                             "dbname=%s port=%d host='localhost' application_name='autonomoustransaction' user=%s "
                             "connect_timeout=600",
                             dbName, g_instance.attr.attr_network.PostPortNumber,
                             (char*)GetSuperUserName((char*)userName));
    securec_check_ss_c(ret, "\0", "\0");

    /* do the actual create session */
    bool old = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = true;
    /* Allow cancel/die interrupts */
    CHECK_FOR_INTERRUPTS();
    AddSessionCount();
    m_conn = PQconnectdb(connInfo);
    PQsetNoticeProcessor(m_conn, ProcessorNotice, NULL);
    t_thrd.int_cxt.ImmediateInterruptOK = old;

    if (PQstatus(m_conn) != CONNECTION_OK) {
        ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR),
                        errmsg("autonomous transaction failed to create autonomous session"),
                        errdetail("%s", PQerrorMessage(m_conn))));
    }
}

/*
 * DetachSession()
 * Disconnects from the server and decreases the global link count by one.
 */
void AutonomousSession::DetachSession(void)
{
    if (m_conn == NULL) {
        return;
    }

    PQfinish(m_conn);
    m_conn = NULL;

    if (m_res != NULL) {
        PQclear(m_res);
    }

    ReduceSessionCount();
}

/*
 * SetDeadLockTimeOut()
 * Set current session DeadlockTimeout to INT_MAX.
 * So the deadlock can be reported by autonomous session.
 */
void AutonomousSession::SetDeadLockTimeOut(void)
{
    saved_deadlock_timeout = u_sess->attr.attr_storage.DeadlockTimeout;
    u_sess->attr.attr_storage.DeadlockTimeout = INT_MAX;
}

/*
 * ReSetDeadLockTimeOut()
 * Restore current session DeadlockTimeout.
 */
void AutonomousSession::ReSetDeadLockTimeOut(void)
{
    if (saved_deadlock_timeout != 0) {
        u_sess->attr.attr_storage.DeadlockTimeout = saved_deadlock_timeout;
        saved_deadlock_timeout = 0;
    }
}

/*
 * ExecSimpleQuery
 * Entry for executing concatenation statements. 
 * If the execution is successful, the execution result is returned. 
 * An error message is displayed.
 */
ATResult AutonomousSession::ExecSimpleQuery(const char* query, TupleDesc resultTupleDesc, 
                                            int64 currentXid, bool isLockWait, bool is_plpgsql_func_with_outparam)
{
    if (unlikely(query == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR),
                        errmsg("try execute null query string in autonomous transactions")));
    }

    bool old = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = true;
    CHECK_FOR_INTERRUPTS();  // Allow cancel/die interrupts
    m_res = PQexecAutonm(m_conn, query, currentXid, isLockWait);
    t_thrd.int_cxt.ImmediateInterruptOK = old;

    ATResult result = HandlePGResult(m_conn, m_res, resultTupleDesc, is_plpgsql_func_with_outparam);
    PQclear(m_res);
    m_res = NULL;

    return result;
}

/*
 * GetConnStatus()
 * Obtain the link status. 
 * If the link status is normal, the value 1 is returned. 
 * If the link status is faulty, the value 0 is returned.
 */
bool AutonomousSession::GetConnStatus(void)
{
    return PQstatus(m_conn) != CONNECTION_BAD;
}

bool AutonomousSession::ReConnSession(void)
{
    for (int i = 0; i < 3; i++) {
        PQreset(m_conn);
        if (u_sess->SPI_cxt.autonomous_session->GetConnStatus()) {
            return true;
        }
    }
    return false;
}

/*
 * CreateAutonomousSession()
 * Creates a new autonomous transaction session. 
 * If the session exists, the session is not processed.
 */
void CreateAutonomousSession(void)
{
    if (u_sess->SPI_cxt.autonomous_session == NULL) {
        MemoryContext oldCxt = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
        u_sess->SPI_cxt.autonomous_session = (AutonomousSession*)palloc(sizeof(AutonomousSession));
        (void)MemoryContextSwitchTo(oldCxt);
        u_sess->SPI_cxt.autonomous_session->Init();
        u_sess->SPI_cxt.autonomous_session->AttachSession();
        u_sess->SPI_cxt.autonomous_session->ExecSimpleQuery("set session_timeout = 0;", NULL, 0);
        ATResult res = u_sess->SPI_cxt.autonomous_session->ExecSimpleQuery("select \
            pg_catalog.pg_current_sessid();", NULL, 0);
        u_sess->SPI_cxt.autonomous_session->current_attach_sessionid = res.ResTup;
    } else {
        if (!u_sess->SPI_cxt.autonomous_session->GetConnStatus() 
            && !u_sess->SPI_cxt.autonomous_session->ReConnSession()) {
            ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR), errmsg("connection to server was lost !")));
        }
    }
}

/*
 * DestoryAutonomousSession
 * You are the top-level procedure 
 * and have completed the execution.
 */
void DestoryAutonomousSession(bool force)
{
    if (u_sess->SPI_cxt.autonomous_session && (force || u_sess->SPI_cxt._connected == 0)) {
        u_sess->SPI_cxt.autonomous_session->ReSetDeadLockTimeOut();
        u_sess->SPI_cxt.autonomous_session->DetachSession();
        AutonomousSession* autonomousSession = u_sess->SPI_cxt.autonomous_session;
        pfree(autonomousSession);
        u_sess->SPI_cxt.autonomous_session = NULL;
    }
}


/*
 * HandleResInfo
 * Process the result returned by the 'select function'.
 */
Datum HandleResInfo(PGconn *conn, const PGresult *result, TupleDesc resultTupleDesc, bool is_plpgsql_func_with_outparam,
                    bool *resisnull)
{
    Oid typeInput;
    Oid typeParam;
    int nColumns = PQnfields(result);
    int nRows = PQntuples(result);
    Datum res;
    *resisnull = false;
    if ((nColumns > 1 || nRows > 1) && !is_plpgsql_func_with_outparam) {
        ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR), 
            errmsg("Invalid autonomous transaction return datatypes"),
            errdetail("nColumns = %d, nRows = %d", nColumns, nRows),
            errcause("PL/SQL uses unsupported feature."),
            erraction("Contact Huawei Engineer.")));
    }

    if (!is_plpgsql_func_with_outparam) {
        Oid ftype = PQftype(result, 0);
        char* valueStr = PQgetvalue(result, 0, 0);

        if (resultTupleDesc != NULL) {
            res = RecordCstringGetDatum(resultTupleDesc, valueStr);
        } else {
            /* translate data string to Datum */
            getTypeInputInfo(ftype, &typeInput, &typeParam);
            if (PQgetisnull(result,0,0)) {
                res = (Datum)0;
                *resisnull = true;
            } else {
                res = OidInputFunctionCall(typeInput, valueStr, typeParam, -1);
            }
        }
    } else {
        Datum *values = (Datum*)palloc(sizeof(Datum) * nColumns);
        bool *nulls = (bool*)palloc(sizeof(bool) * nColumns);

        for (int i = 0; i < nColumns; i++) {
            Oid ftype = PQftype(result, i);
            char* valueStr = PQgetvalue(result, 0, i);
            getTypeInputInfo(ftype, &typeInput, &typeParam);
            nulls[i] = PQgetisnull(result,0,i);
            if (nulls[i]) {
                values[i] = (Datum)0;
            } else {
                values[i] = OidInputFunctionCall(typeInput, valueStr, typeParam, -1);
            }
        }
        HeapTuple rettup = heap_form_tuple(resultTupleDesc, values, nulls);
        res = PointerGetDatum(SPI_returntuple(rettup, resultTupleDesc));
        pfree(values);
        pfree(nulls);
    }

    return res;
}


ATResult HandlePGResult(PGconn* conn, PGresult* pgresult, TupleDesc resultTupleDesc, bool is_plpgsql_func_with_outparam)
{
    if (unlikely(conn == NULL || pgresult == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR), errmsg("invalid data \" %s \" in autonomous transactions",
                                                               (conn == NULL) ? "connection" : "result")));
    }
    ATResult res;
    ExecStatusType status = PQresultStatus(pgresult);
    int errorCode = 0;
    
    switch (status) {
        /* successful completion of a command returning no data */
        case PGRES_COMMAND_OK:
            res.result = RES_COMMAND_OK;
            res.withtuple = true;
            res.ResTup = (Datum)0;
            res.resisnull = true;
            break;
        /*
         * contains a single result tuple from the current command. 
         * this status occurs only when single-row mode has been selected for the query
         */
         /* We do not handle this type of return. */
        case PGRES_SINGLE_TUPLE:
            res.result = RES_SINGLE_TUPLE;
            res.withtuple = true;
            res.ResTup = (Datum)0;
            res.resisnull = true;
            break;
        case PGRES_TUPLES_OK:
            res.result = RES_TUPLES_OK;
            res.withtuple = true;

            res.ResTup = HandleResInfo(conn, pgresult, resultTupleDesc, is_plpgsql_func_with_outparam, &res.resisnull);
            break;

        /* the string sent to the server was empty */
        case PGRES_EMPTY_QUERY:    // fallthrough
        /* the server's response was not understood */
        case PGRES_BAD_RESPONSE:   // fallthrough
        /* a nonfatal error (a notice or warning) occurred */
        case PGRES_NONFATAL_ERROR: // fallthrough
        /* a fatal error occurred */
        case PGRES_FATAL_ERROR: {  // fallthrough
            char* hint = PQresultErrorMessage(pgresult);
            if (hint == NULL) {  // this a error associated with connection in stead of query command
                hint = PQerrorMessage(conn);
            }
            errorCode = MAKE_SQLSTATE((unsigned char)conn->last_sqlstate[0], (unsigned char)conn->last_sqlstate[1], 
                                      (unsigned char)conn->last_sqlstate[2], (unsigned char)conn->last_sqlstate[3], 
                                      (unsigned char)conn->last_sqlstate[4]);

            ereport(ERROR, (errcode(errorCode), errmsg("%s", hint)));
        }

        /* following cases should not be happened */
        /* copy Out (from server) data transfer started */
        case PGRES_COPY_OUT:  // fallthrough
        /* copy In (to server) data transfer started */
        case PGRES_COPY_IN:  // fallthrough
        /*
         * Copy In/Out (to and from server) data transfer started.
         * used only for streaming replication, so this status should not occur in ordinary applications.
         */
        case PGRES_COPY_BOTH:  // fallthrough
            ereport(ERROR, (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                            errmsg("autonomous transaction failed in query execution."),
                            errhint("copy command is not supported")));
        default:
            ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR), errmsg("unrecognized result type.")));
    }

    return res;
}


/* ----------
 * IsAutonomousTransaction
 *
 * Check whether a new session needs to be started 
 *    for the current stored procedure.
 * ----------
 */
bool IsAutonomousTransaction(bool isAutonomous)
{
    /* If the current stored procedure is not an autonomous transaction, false is returned. */
    if (!isAutonomous) {
        return false;
    }
    /* The current stored procedure is transferred from gsql and the PQexec interface is invoked. */
    if (u_sess->is_autonomous_session == false) {
        return true;
    }
    /* The current stored procedure is transferred 
     * through the Autonm interface of libpq and is a called function. 
     */
    if (u_sess->is_autonomous_session == true && u_sess->SPI_cxt._connected > 0) {
        return true;
    }

    return false;
}


/*
 * get output buffer size
 */
inline int32 GetSendBuffSize(void)
{
    StringInfoData* buffer = t_thrd.log_cxt.msgbuf;
    if (buffer == NULL) {
        ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR), errmsg("msgbuf is invalud")));
    }
    return buffer->len;
}

/*
 * for backend Notice messages (INFO, WARNING, etc)
 */
static void ProcessorNotice(void* arg, const char* message)
{
    (void)arg; /* not used */
    StringInfoData ds;
    bool isPutline = true;
    
    uint32 lineSize = strlen(message);
    if (lineSize > MAX_LINESIZE)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("The length of incoming msg exceeds the max line size %d", MAX_LINESIZE)));

    initStringInfo(&ds);
    appendStringInfoString(&ds, message);

    /* add a arg to see it's a put or put_line */
    send_only_message_to_frontend(ds.data, isPutline);
    pfree(ds.data);
}

int PQsendQueryAutonm(PGconn* conn, const char* query)
{
    if (!PQsendQueryStart(conn))
        return 0;

    if (query == NULL) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("command string is a null pointer\n"));
        return 0;
    }

#ifdef HAVE_CE
    StatementData statementData (conn, query);
    if (conn->client_logic->enable_client_encryption) {
        if (!conn->client_logic->disable_once) {
            bool clientLogicRet = Processor::run_pre_query(&statementData);
            if (!clientLogicRet)
                return 0;
            query = statementData.params.adjusted_query;
        } else {
            conn->client_logic->disable_once  = false;
        }
    } else { 
        char *temp_query = del_blanks(const_cast<char*>(query), strlen(query));
        const char *global_setting_str = "createclientmasterkey";
        const char *column_setting_str = "createcolumnencryptionkey";
        const char *client_logic_str = "encryptedwith";
        if (temp_query != NULL && ((strcasestr(temp_query, global_setting_str) != NULL) ||
            (strcasestr(temp_query, column_setting_str) != NULL) ||
            (strcasestr(temp_query, client_logic_str) != NULL))) {
            free(temp_query);
            temp_query = NULL;
            printfPQExpBuffer(&conn->errorMessage,
                libpq_gettext("ERROR(CLIENT): disable client-encryption feature, please use -C to enable it.\n"));
            return 0;
        }
        if (temp_query != NULL) {
            free(temp_query);
            temp_query = NULL;
        }
    } 
#endif

    Oid currentId = GetCurrentUserId();
    uint64 sessionId = IS_THREAD_POOL_WORKER ? u_sess->session_id : t_thrd.proc_cxt.MyProcPid;
    /* put 64bit sessionId in 32bit once time */
    uint32 sessionId_l32 = sessionId;
    uint32 sessionId_h32 = sessionId >> 32;
    /* construct the outgoing Query message */
    bool execFlag = pqPutMsgStart('u', false, conn) < 0 || pqPutInt((int)currentId, 4, conn)
        || pqPutInt(sessionId_h32, 4, conn) || pqPutInt(sessionId_l32, 4, conn)
        || pqPuts(query, conn) < 0 || pqPutMsgEnd(conn) < 0;
    if (execFlag) {
        pqHandleSendFailure(conn);
        return 0;
    }

    /* remember we are using simple query protocol */
    conn->queryclass = PGQUERY_SIMPLE;

    /* and remember the query text too, if possible */
    /* if insufficient memory, last_query just winds up NULL */
    if (conn->last_query != NULL)
        free(conn->last_query);
    conn->last_query = strdup(query);

    /*
     * Give the data a push.  In nonblock mode, don't complain if we're unable
     * to send it all; PQgetResult() will do any additional flushing needed.
     */
    if (pqFlush(conn) < 0) {
        pqHandleSendFailure(conn);
        return 0;
    }

    /* OK, it's launched! */
    conn->asyncStatus = PGASYNC_BUSY;
    return 1;
}


PGresult* PQexecAutonm(PGconn* conn, const char* query, int64 automnXid, bool isLockWait)
{
    if (!PQexecStart(conn)) {
        return NULL;
    }

    if (!PQsendQueryAutonm(conn, query)) {
        return NULL;
    }
    if (isLockWait) {
        u_sess->SPI_cxt.autonomous_session->SetDeadLockTimeOut();
        XactLockTableWait(automnXid);
        u_sess->SPI_cxt.autonomous_session->ReSetDeadLockTimeOut();
        CHECK_FOR_INTERRUPTS();
    }
    return PQexecAutonmFinish(conn);
}


/*
 * Common code for PQexec and sibling routines: prepare to send command
 */
static bool PQexecStart(PGconn* conn)
{
    PGresult* result = NULL;

    if (conn == NULL)
        return false;

    /*
     * Silently discard any prior query result that application didn't eat.
     * This is probably poor design, but it's here for backward compatibility.
     */
    while ((result = PQgetResult(conn)) != NULL) {
        ExecStatusType resultStatus = result->resultStatus;

        PQclear(result); /* only need its status */
        if (resultStatus == PGRES_COPY_IN) {
            if (PG_PROTOCOL_MAJOR(conn->pversion) >= 3) {
                /* In protocol 3, we can get out of a COPY IN state */
                if (PQputCopyEnd(conn, libpq_gettext("COPY terminated by new PQexec")) < 0)
                    return false;
                /* keep waiting to swallow the copy's failure message */
            } else {
                /* In older protocols we have to punt */
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("COPY IN state must be terminated first, remote datanode %s, err: %s\n"),
                    conn->remote_nodename, strerror(errno));
                return false;
            }
        } else if (resultStatus == PGRES_COPY_OUT) {
            if (PG_PROTOCOL_MAJOR(conn->pversion) >= 3) {
                /*
                 * In protocol 3, we can get out of a COPY OUT state: we just
                 * switch back to BUSY and allow the remaining COPY data to be
                 * dropped on the floor.
                 */
                conn->asyncStatus = PGASYNC_BUSY;
                /* keep waiting to swallow the copy's completion message */
            } else {
                /* In older protocols we have to punt */
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("COPY OUT state must be terminated first, remote datanode %s, err: %s\n"), 
                    conn->remote_nodename, strerror(errno));
                return false;
            }
        } else if (resultStatus == PGRES_COPY_BOTH) {
            /* We don't allow PQexec during COPY BOTH */
            printfPQExpBuffer(&conn->errorMessage,
                libpq_gettext("PQexec not allowed during COPY BOTH, remote datanode %s, errno: %s\n"),
                conn->remote_nodename, strerror(errno));
            return false;
        }
        /* check for loss of connection, too */
        if (conn->status == CONNECTION_BAD)
            return false;
    }

    /* OK to send a command */
    return true;
}


/*
 * Common code for PQexec and sibling routines: wait for command result
 */
static PGresult* PQexecAutonmFinish(PGconn* conn)
{
    PGresult* result = NULL;
    PGresult* lastResult = NULL;

    /*
     * For backwards compatibility, return the last result if there are more
     * than one --- but merge error messages if we get more than one error
     * result.
     *
     * We have to stop if we see copy in/out/both, however. We will resume
     * parsing after application performs the data transfer.
     *
     * Also stop if the connection is lost (else we'll loop infinitely).
     */
    lastResult = NULL;
    while ((result = PQgetResult(conn)) != NULL) {
        if (lastResult != NULL) {
            if (lastResult->resultStatus == PGRES_FATAL_ERROR && result->resultStatus == PGRES_FATAL_ERROR) {
                pqCatenateResultError(lastResult, result->errMsg);
                PQclear(result);
                result = lastResult;

                /*
                 * Make sure PQerrorMessage agrees with concatenated result
                 */
                resetPQExpBuffer(&conn->errorMessage);
                appendPQExpBufferStr(&conn->errorMessage, result->errMsg);
            } else {
                if (strncmp(result->cmdStatus, "COMMIT", 7) != 0) {
                    PQclear(lastResult);
                    lastResult = NULL;
                } else {
                    PQclear(result);
                    result = NULL;
                }
            }
        }
        if (lastResult == NULL) {
            lastResult = result;
        } 
        if (lastResult->resultStatus == PGRES_COPY_IN || lastResult->resultStatus == PGRES_COPY_OUT ||
            lastResult->resultStatus == PGRES_COPY_BOTH || conn->status == CONNECTION_BAD)
            break;
    }

    return lastResult;
}

/* end of file */
