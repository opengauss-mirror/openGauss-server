/*
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
#include "utils/lsyscache.h"
#include "utils/plpgsql.h"
#include "storage/spin.h"

ATManager g_atManager;

const int MAX_CONNINFO_SIZE = 512;

bool ATManager::AddSession()
{
    bool bok = false;

    SpinLockAcquire(&m_lock);
    if (m_sessioncnt < (uint32)g_instance.attr.attr_storage.max_concurrent_autonomous_transactions) {
        ++m_sessioncnt;
        ereport(DEBUG2, (errmodule(MOD_PLSQL), errmsg("autonomous transaction inc session : %d", m_sessioncnt)));
        bok = true;
    }
    SpinLockRelease(&m_lock);

    return bok;
}

void ATManager::RemoveSession()
{
    SpinLockAcquire(&m_lock);
    if (m_sessioncnt > 0) {
        --m_sessioncnt;
        ereport(DEBUG2, (errmodule(MOD_PLSQL), errmsg("autonomous transaction dec session : %d", m_sessioncnt)));
    }
    SpinLockRelease(&m_lock);
}


ATResult AutonomousSession::ExecSimpleQuery(const char* query)
{
    if (unlikely(query == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR),
                        errmsg("try execute null query string in autonomous transactions")));
    }

    CreateSession();

    bool old = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = true;
    CHECK_FOR_INTERRUPTS();  // Allow cancel/die interrupts
    m_res = PQexec(m_conn, query);
    t_thrd.int_cxt.ImmediateInterruptOK = old;

    ATResult result = HandlePGResult(m_conn, m_res);
    PQclear(m_res);
    m_res = NULL;

    return result;
}

ATResult AutonomousSession::ExecQueryWithParams(const char* query, PQ_ParamInfo* pinfo)
{
    if (unlikely(query == NULL || pinfo == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR), errmsg("invalid data \" %s \" in autonomous transactions",
                                                               (query == NULL) ? "query" : "param")));
    }

    CreateSession();

    bool old = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = true;
    CHECK_FOR_INTERRUPTS();  // Allow cancel/die interrupts
    m_res = PQexecParams(m_conn, query, pinfo->nparams, pinfo->paramtypes, pinfo->paramvalues, pinfo->paramlengths,
                         pinfo->paramformats, PQ_FORMAT_BINARY);
    t_thrd.int_cxt.ImmediateInterruptOK = old;

    FreePQParamInfo(pinfo);  // free pinfo as soon as it's useless for us

    ATResult result = HandlePGResult(m_conn, m_res);
    PQclear(m_res);
    m_res = NULL;

    return result;
}

void AutonomousSession::CloseSession(void)
{
    if (m_conn == NULL) {
        return;
    }

    m_manager->RemoveSession();
    PGconn* conn = m_conn;
    m_conn = NULL;
    PQfinish(conn);

    if (m_res != NULL) {
        PQclear(m_res);
    }
}

void AutonomousSession::CreateSession(void)
{
    if (m_conn != NULL) {  // create session alike singleton
        return;
    }

    if (!m_manager->AddSession()) {
        ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR),
                        errmsg("concurrent autonomous transactions reach its maximun : %d",
                               g_instance.attr.attr_storage.max_concurrent_autonomous_transactions)));
    }

    /* create a connection info with current database info */
    char conninfo[MAX_CONNINFO_SIZE];
    const char* dbname = get_database_name(u_sess->proc_cxt.MyDatabaseId);
    if (dbname == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE), errmsg("database with OID %u does not exist",
                                                                    u_sess->proc_cxt.MyDatabaseId)));
    }

    errno_t ret = snprintf_s(conninfo, sizeof(conninfo), sizeof(conninfo) - 1,
                             "dbname=%s port=%d application_name='autonomous_transaction'",
                             dbname, g_instance.attr.attr_network.PostPortNumber);
    securec_check_ss_c(ret, "\0", "\0");

    /* do the actual create session */
    CreateSession(conninfo);
}

void AutonomousSession::CreateSession(const char* conninfo)
{
    if (unlikely(conninfo == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR), errmsg("null connection info data")));
    }

    bool old = t_thrd.int_cxt.ImmediateInterruptOK;
    t_thrd.int_cxt.ImmediateInterruptOK = true;
    CHECK_FOR_INTERRUPTS();  // Allow cancel/die interrupts
    m_conn = PQconnectdb(conninfo);
    t_thrd.int_cxt.ImmediateInterruptOK = old;

    if (PQstatus(m_conn) != CONNECTION_OK) {
        ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR),
                        errmsg("autonomous transaction failed to create autonomous session"),
                        errdetail("%s", PQerrorMessage(m_conn))));
    }

    ereport(DEBUG2, (errmodule(MOD_PLSQL), errmsg("autonomous transaction create session : %s", conninfo)));
}

void AutonomousSession::Attach(void)
{
    ++m_refcount;
    ereport(DEBUG2, (errmodule(MOD_PLSQL), errmsg("autonomous transaction inc ref : %d", m_refcount)));
}

void AutonomousSession::Detach(void)
{
    if (m_refcount > 0) {
        --m_refcount;
        ereport(DEBUG2, (errmodule(MOD_PLSQL), errmsg("autonomous transaction dec ref : %d", m_refcount)));
    } else {
        ereport(DEBUG2, (errmodule(MOD_PLSQL), errmsg("autonomous transaction invalid dec ref")));
    }

    if (m_refcount == 0) {
        CloseSession();
    }
}


ATResult HandlePGResult(PGconn* conn, PGresult* pgresult)
{
    if (unlikely(conn == NULL || pgresult == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR), errmsg("invalid data \" %s \" in autonomous transactions",
                                                               (conn == NULL) ? "connection" : "result")));
    }
    ATResult res;
    ExecStatusType status = PQresultStatus(pgresult);
    switch (status) {
        /* successful completion of a command returning no data */
        case PGRES_COMMAND_OK:
            res.result = RES_COMMAND_OK;
            res.withtuple = true;
            break;
        /*
         * contains a single result tuple from the current command. 
         * this status occurs only when single-row mode has been selected for the query
         */
        case PGRES_SINGLE_TUPLE:
            res.result = RES_SINGLE_TUPLE;
            res.withtuple = true;
            break;
        /* successful completion of a command returning data (such as a `SELECT` or `SHOW`) */
        case PGRES_TUPLES_OK:
            res.result = RES_TUPLES_OK;
            res.withtuple = true;
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
            ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR), errmsg("autonomous transaction failed in query execution."),
                            errhint("%s", hint)));
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

void InitPQParamInfo(PQ_ParamInfo* pinfo, int n)
{
    if (pinfo == NULL) {
        return;
    }

    pinfo->nparams = n;
    if (n > 0) {
        pinfo->paramtypes   = (Oid*)palloc(n * sizeof(Oid));
        pinfo->paramvalues  = (char**)palloc(n * sizeof(char*));
        pinfo->paramlengths = (int*)palloc(n * sizeof(int));
        pinfo->paramformats = (int*)palloc(n * sizeof(int));
    } else {
        pinfo->paramtypes   = NULL;
        pinfo->paramvalues  = NULL;
        pinfo->paramlengths = NULL;
        pinfo->paramformats = NULL;
    }
}

void FreePQParamInfo(PQ_ParamInfo* pinfo)
{
    if (pinfo == NULL) {
        return;
    }
    if (pinfo->paramtypes) {
        pfree(pinfo->paramtypes);
    }
    if (pinfo->paramvalues) {
        pfree(pinfo->paramvalues);
    }
    if (pinfo->paramlengths) {
        pfree(pinfo->paramlengths);
    }
    if (pinfo->paramformats) {
        pfree(pinfo->paramformats);
    }

    pfree(pinfo);
}

bool IsValidAutonomousTransaction(const PLpgSQL_execstate* estate, const PLpgSQL_stmt_block* block)
{
    if (unlikely(estate == NULL || block == NULL)) {
        return false;
    }
    if (block->exceptions != NULL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Un-support feature : Autonomous transaction don't support exception")));
    }
    if (estate->func->fn_is_trigger) { 
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Un-support feature : Trigger don't support autonomous transaction")));
    } 

    return true;
}

bool IsAutonomousTransaction(const PLpgSQL_execstate* estate, const PLpgSQL_stmt_block* block)
{
    /* 
     * in outer block of plpgsql using block->autonomous to identify autonomus,
     * in subblock of plpgsql using estate->autonomous_session.
     * ----------------------------------------------------
      CREATE FUNCTION ff_subblock() RETURNS void AS $$
      DECLARE
      PRAGMA AUTONOMOUS_TRANSACTION;
      BEGIN <<outer block>>
           BEGIN <<subblock>>
               insert into tt01 values(1);
           END;
           BEGIN <<subblock>>
                insert into tt01 values(2);
           END;
      END;
      $$ LANGUAGE plpgsql;
     * ----------------------------------------------------
     */
    return (block->autonomous || estate->autonomous_session);
}

bool IsValidAutonomousTransactionQuery(PLpgSQL_exectype exectype, const PLpgSQL_expr* stmtexpr, bool isinto)
{
    if (exectype == STMT_UNKNOW) {
        return false;
    }
    if (unlikely(stmtexpr == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR), errmsg("null stmt expr in autonomous transactions")));
    }
    if (isinto) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Un-support feature : Autonomous transaction don't support 'into' clause"),
                        errdetail("%s", stmtexpr->query)));
    }
    if (stmtexpr->paramnos != NULL && exectype != STMT_DYNAMIC) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Un-support feature : Autonomous transaction don't support sql command with parameter"),
                        errdetail("%s", stmtexpr->query)));
    }

    return true;
}

void AttachToAutonomousSession(PLpgSQL_execstate* estate, const PLpgSQL_stmt_block* block)
{
    if (IsValidAutonomousTransaction(estate, block)) {
        if (estate->autonomous_session == NULL) {
            estate->autonomous_session = (AutonomousSession*)palloc(sizeof(AutonomousSession));
            estate->autonomous_session->Init(&g_atManager);
        }
        estate->autonomous_session->Attach();
    }
}

void DetachToAutonomousSession(const PLpgSQL_execstate* estate)
{
    if (estate->autonomous_session) {
        estate->autonomous_session->Detach();
    }
}

/* end of file */
