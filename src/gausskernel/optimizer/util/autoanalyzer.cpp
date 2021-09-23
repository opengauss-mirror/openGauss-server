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
 * autoanalyzer.cpp
 *	   routines for processing auto-analyze in optimizer
 * IDENTIFICATION
 *        src/gausskernel/optimizer/util/autoanalyzer.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "commands/dbcommands.h"
#include "executor/node/nodeModifyTable.h"
#include "miscadmin.h"
#include "optimizer/autoanalyzer.h"
#include "postmaster/postmaster.h"
#include "storage/buf/bufmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"

/* AutoAnalyze */
#define CHAR_BUF_SIZE 512
#define AUTOANALYZE_LOCKWAIT_TIMEOUT 10000 /* 10s */
static int autoAnalyzeFreeProcess = 10;

/* Constructor */
AutoAnaProcess::AutoAnaProcess(Relation rel) : m_pgconn(NULL), m_res(NULL)
{
    m_query = NIL;

    StringInfoData str_lockwait_timeout;
    initStringInfo(&str_lockwait_timeout);
    appendStringInfo(&str_lockwait_timeout, "set lockwait_timeout=%d ;", AUTOANALYZE_LOCKWAIT_TIMEOUT);
    m_query = lappend(m_query, str_lockwait_timeout.data);

    StringInfoData str_allow_concurrent_tuple_update;
    initStringInfo(&str_allow_concurrent_tuple_update);
    appendStringInfo(&str_allow_concurrent_tuple_update, "set allow_concurrent_tuple_update='off';");
    m_query = lappend(m_query, str_allow_concurrent_tuple_update.data);

    StringInfoData str_max_query_retry_times;
    initStringInfo(&str_max_query_retry_times);
    appendStringInfo(&str_max_query_retry_times, "set max_query_retry_times=0;");
    m_query = lappend(m_query, str_max_query_retry_times.data);

    StringInfoData str_analyze_command;
    initStringInfo(&str_analyze_command);
    appendStringInfo(&str_analyze_command,
        "analyze %s.%s ;",
        quote_identifier(get_namespace_name(rel->rd_rel->relnamespace)),
        quote_identifier(NameStr(rel->rd_rel->relname)));
    m_query = lappend(m_query, str_analyze_command.data);
}

AutoAnaProcess::~AutoAnaProcess()
{
    if (t_thrd.utils_cxt.CurrentResourceOwner == NULL) {
        m_res = NULL;
    }
    PQclear(m_res);
    PQfinish(m_pgconn);
    if (m_query != NIL) {
        list_free_deep(m_query);
    }
    m_pgconn = NULL;
    m_res = NULL;
    m_query = NIL;
}

bool AutoAnaProcess::executeSQLCommand(char* queryString)
{
    bool result = false;
    bool ImmediateInterruptOK_Old = t_thrd.int_cxt.ImmediateInterruptOK;
    /* Allow cancel/die interrupts to be processed while waiting */
    t_thrd.int_cxt.ImmediateInterruptOK = true;
    CHECK_FOR_INTERRUPTS();
    m_res = PQexec(m_pgconn, queryString);
    t_thrd.int_cxt.ImmediateInterruptOK = ImmediateInterruptOK_Old;

    if (PQresultStatus(m_res) != PGRES_COMMAND_OK) {
        elog(DEBUG2, "[AUTO-ANALYZE] autoanalyze failed: %s, error: %s", queryString, PQerrorMessage(m_pgconn));
    } else {
        result = true;
        elog(DEBUG2, "[AUTO-ANALYZE] autoanalyze sucess: %s", queryString);
    }
    PQclear(m_res);
    m_res = NULL;
    return result;
}

bool AutoAnaProcess::run()
{
    char conninfo[CHAR_BUF_SIZE];
    int ret;
    bool result = false;
    ListCell* lc = NULL;

    foreach (lc, m_query) {
        char* cmd = (char*)lfirst(lc);
        elog(DEBUG2, "[AUTO-ANALYZE] autoanalyze start: %s", cmd);
    }

    ret = snprintf_s(conninfo,
        sizeof(conninfo),
        sizeof(conninfo) - 1,
        "dbname=%s port=%d application_name='auto_analyze' enable_ce=1 ",
        get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true),
        g_instance.attr.attr_network.PostPortNumber);
    securec_check_ss_c(ret, "\0", "\0");

    m_pgconn = PQconnectdb(conninfo);

    /* check to see that the backend connection was successfully made */
    if (PQstatus(m_pgconn) == CONNECTION_OK) {
        foreach (lc, m_query) {
            char* cmd = (char*)lfirst(lc);
            result = executeSQLCommand(cmd);
            if (!result) {
                break;
            }
        }
    } else {
        elog(DEBUG2, "[AUTO-ANALYZE] connection to database failed: %s", PQerrorMessage(m_pgconn));
    }

    PQclear(m_res);
    PQfinish(m_pgconn);
    m_res = NULL;
    m_pgconn = NULL;
    return result;
}

void AutoAnaProcess::tear_down()
{
    delete u_sess->analyze_cxt.autoanalyze_process;
    u_sess->analyze_cxt.autoanalyze_process = NULL;
}

/*
 * check_conditions
 *		check the user privilege for autoanalyze, or if temp table
 */
bool AutoAnaProcess::check_conditions(Relation rel)
{
    /* if the rel is invalid, just return false */
    if (!rel)
        return false;

    /* if the rel is temp, just return false */
    if (RelationIsLocalTemp(rel))
        return false;

    /*
     * If rel is in read only mode(none redistribution scenario), we skip analyze
     * the relation.
     */
    if (!u_sess->attr.attr_sql.enable_cluster_resize && RelationInClusterResizingReadOnly(rel))
        return false;

    AclResult aclresult = pg_class_aclcheck(RelationGetRelid(rel), GetUserId(), ACL_VACUUM);
    if (aclresult != ACLCHECK_OK && !(pg_class_ownercheck(RelationGetRelid(rel), GetUserId()) ||
            (pg_database_ownercheck(u_sess->proc_cxt.MyDatabaseId, GetUserId()) && !rel->rd_rel->relisshared))) {
        return false;
    }

    return true;
}

bool AutoAnaProcess::runAutoAnalyze(Relation rel)
{
    bool getProcess = false;
    bool result = false;
    TimestampTz start_time = 0;

    /* need to show auto-analyze time in explain analyze */
    if (u_sess->analyze_cxt.autoanalyze_timeinfo) {
        start_time = GetCurrentTimestamp();
    }

    /* 1 check conditions */
    if (!check_conditions(rel)) {
        elog(DEBUG2, "[AUTO-ANALYZE] check analyze conditions failed: table \"%s\"", NameStr(rel->rd_rel->relname));
        return result;
    }

    /* 2 get free aa process */
    (void)LWLockAcquire(AutoanalyzeLock, LW_EXCLUSIVE);
    if (autoAnalyzeFreeProcess > 0) {
        getProcess = true;
        autoAnalyzeFreeProcess--;
    }
    (void)LWLockRelease(AutoanalyzeLock);

    if (!getProcess) {
        /* no free process */
        elog(DEBUG2, "[AUTO-ANALYZE] no free autoanalyze process");
        return result;
    }

    /* 3 run analyze */
    Assert(u_sess->analyze_cxt.autoanalyze_process == NULL);
    u_sess->analyze_cxt.autoanalyze_process = New(CurrentMemoryContext) AutoAnaProcess(rel);
    result = u_sess->analyze_cxt.autoanalyze_process->run();
    tear_down();

    /* 4 release free aa process */
    LWLockAcquire(AutoanalyzeLock, LW_EXCLUSIVE);
    autoAnalyzeFreeProcess++;
    LWLockRelease(AutoanalyzeLock);

    /* 5 show auto-analyze time in explain analyze when success */
    if (u_sess->analyze_cxt.autoanalyze_timeinfo && result) {
        long secs;
        long msecs;
        int usecs;

        TimestampDifference(start_time, GetCurrentTimestamp(), &secs, &usecs);
        msecs = usecs / 1000L;
        msecs = secs * 1000 + msecs;
        usecs = usecs % 1000;

        MemoryContext oldcontext = MemoryContextSwitchTo(u_sess->temp_mem_cxt);
        appendStringInfo(u_sess->analyze_cxt.autoanalyze_timeinfo,
            "\"%s.%s\" %ld.%03dms ",
            get_namespace_name(rel->rd_rel->relnamespace),
            NameStr(rel->rd_rel->relname),
            msecs,
            usecs);
        (void)MemoryContextSwitchTo(oldcontext);
    }

    return result;
}

void AutoAnaProcess::cancelAutoAnalyze()
{
    /* auto-analyzing now */
    if (u_sess->analyze_cxt.autoanalyze_process != NULL) {
        tear_down();

        /* release free aa process */
        (void)LWLockAcquire(AutoanalyzeLock, LW_EXCLUSIVE);
        autoAnalyzeFreeProcess++;
        LWLockRelease(AutoanalyzeLock);
    }

    /* clean explain info if exists */
    if (u_sess->analyze_cxt.autoanalyze_timeinfo != NULL) {
        pfree_ext(u_sess->analyze_cxt.autoanalyze_timeinfo->data);
        pfree_ext(u_sess->analyze_cxt.autoanalyze_timeinfo);
        u_sess->analyze_cxt.autoanalyze_timeinfo = NULL;
    }
}

/* Just for query cancel */
void CancelAutoAnalyze()
{
    AutoAnaProcess::cancelAutoAnalyze();
}
