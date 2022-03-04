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
* threadpool_stream.cpp
*    stream thread pool Interface
*
* IDENTIFICATION
*    src/distribute/kernel/stream/threadpool_stream.cpp
*
* NOTES
*    Aplly and return stream thread to pool.
*
* -------------------------------------------------------------------------
*/

#include "postgres.h"

#include "distributelayer/streamMain.h"
#include "distributelayer/streamProducer.h"
#include "executor/executor.h"
#include "gs_thread.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "threadpool/threadpool.h"
#include "utils/guc.h"
#include "utils/postinit.h"

static void ResetStreamStatus();

ThreadPoolStream::ThreadPoolStream()
{
    m_tid = InvalidTid;
    m_producer = NULL;
    m_group = NULL;
    DLInitElem(&m_elem, this);

    m_mutex = NULL;
    m_cond = NULL;
    m_threadStatus = THREAD_UNINIT;
}

ThreadPoolStream::~ThreadPoolStream()
{
}

int ThreadPoolStream::StartUp(int idx, StreamProducer* producer, ThreadPoolGroup* group,
                                pthread_mutex_t* mutex, pthread_cond_t* cond)
{
    m_idx = idx;
    m_producer = producer;
    m_group = group;
    m_mutex = mutex;
    m_cond = cond;
    
    m_tid = initialize_util_thread(THREADPOOL_STREAM, this);
    if (m_tid == 0) {
        m_threadStatus = THREAD_EXIT;
    } else {
        m_threadStatus = THREAD_RUN;
    }
    return m_tid;
}

void ThreadPoolStream::WaitMission()
{
    PreventSignal();
    pthread_mutex_lock(m_mutex);
    while (m_producer == NULL) {
        if (m_threadStatus == THREAD_EXIT) {
            break;
        }
        pthread_cond_wait(m_cond, m_mutex);
    }
    pthread_mutex_unlock(m_mutex);
    Assert(t_thrd.proc->pid == t_thrd.proc_cxt.MyProcPid);

    if (m_threadStatus == THREAD_EXIT) {
        StreamExit();
        proc_exit(0);
    } else {
        InitStream();
    }
    AllowSignal();
}

void ThreadPoolStream::WakeUpToWork(StreamProducer* producer)
{
    pthread_mutex_lock(m_mutex);
    m_producer = producer;
    pthread_cond_signal(m_cond);
    pthread_mutex_unlock(m_mutex);
}

void ThreadPoolStream::WakeUpToUpdate(ThreadStatus status)
{
    pthread_mutex_lock(m_mutex);
    m_threadStatus = status;
    pthread_cond_signal(m_cond);
    pthread_mutex_unlock(m_mutex);
}

void ThreadPoolStream::InitStream()
{
    knl_session_context* sc =
        create_session_context(g_threadPoolControler->GetMemCxt(), m_producer->getParentSessionid());
    sc->stat_cxt.trackedBytes += u_sess->stat_cxt.trackedBytes;
    sc->stat_cxt.trackedMemChunks += u_sess->stat_cxt.trackedMemChunks;
    u_sess->stat_cxt.trackedBytes = 0;
    u_sess->stat_cxt.trackedMemChunks = 0;
    u_sess = sc;
    SelfMemoryContext = u_sess->self_mem_cxt;

    /* Switch context to Session context. */
    AutoContextSwitch memSwitch(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));

    SetStreamWorkerInfo(m_producer);

    u_sess->proc_cxt.MyProcPort = (Port*)palloc0(sizeof(Port));
    SetStreamWorkerInfo(m_producer);
    ExtractProduerInfo();

    SetProcessingMode(InitProcessing);

    /* Init GUC option for this session. */
    InitializeGUCOptions();
    /* Read in remaining GUC variables */
    read_nondefault_variables();

    /* Do local initialization of file, storage and buffer managers */
    ReBuildLSC();
    InitFileAccess();
    smgrinit();

    /* Init Stream thread user and database */
    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(
        u_sess->stream_cxt.producer_obj->getDbName(), InvalidOid, u_sess->stream_cxt.producer_obj->getUserName());
    t_thrd.proc_cxt.PostInit->InitStreamSession();

    SetProcessingMode(NormalProcessing);

    repair_guc_variables();
    RestoreStreamSyncParam(&m_producer->m_syncParam);

    u_sess->exec_cxt.under_stream_runtime = true;
    u_sess->attr.attr_common.remoteConnType = REMOTE_CONN_DATANODE;

    STREAM_LOG(DEBUG2, "[StreamPool] Receive Query query_id %lu, tlevel %u, smpid %u",
                                            m_producer->getKey().queryId,
                                            m_producer->getKey().planNodeId,
                                            m_producer->getKey().smpIdentifier);
}

void ThreadPoolStream::CleanUp()
{
    m_producer = NULL;
    StreamExit();
    ResetStreamStatus();
    u_sess = t_thrd.fake_session;
    m_group->ReturnStreamToPool(&m_elem);
}

void ThreadPoolStream::ShutDown()
{
    m_producer = NULL;
    m_group->RemoveStreamFromPool(&m_elem, m_idx);
}

static void ResetStreamStatus()
{
    /* Add the pg_delete_audit operation to audit log */
    t_thrd.audit.Audit_delete = false;
    t_thrd.postgres_cxt.debug_query_string = NULL;
    t_thrd.postgres_cxt.g_NoAnalyzeRelNameList = NIL;
    t_thrd.postgres_cxt.mark_explain_analyze = false;
    t_thrd.postgres_cxt.mark_explain_only = false;
    t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.TopResourceOwner;
    t_thrd.log_cxt.msgbuf->cursor = 0;
    t_thrd.log_cxt.msgbuf->len = 0;
    if (unlikely(t_thrd.log_cxt.msgbuf->data != NULL)) {
        pfree_ext(t_thrd.log_cxt.msgbuf->data);
    }

    /*
     * Reset extended-query-message flag, so that any errors
     * encountered in "idle" state don't provoke skip.
     */
    u_sess->postgres_cxt.doing_extended_query_message = false;
    u_sess->debug_query_id = 0;
    u_sess->syscache_cxt.CacheInitialized = false;
    u_sess->misc_cxt.AuthenticatedUserId = InvalidOid;
    u_sess->analyze_cxt.is_under_analyze = false;

    /* We don't have a transaction command open anymore */
    t_thrd.postgres_cxt.xact_started = false;
    /*
     * Reset top transaction in case we have received parent transaction
     * from main worker thread, which has already been release.
     */
    InitTopTransactionState();
    InitCurrentTransactionState();

    if (!IS_PGSTATE_TRACK_UNDEFINE) {
        volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
        beentry->st_queryid = 0;
        pgstat_report_unique_sql_id(true);
        beentry->st_sessionid = 0;
        beentry->st_parent_sessionid = 0;
        beentry->st_thread_level = 0;
        beentry->st_smpid = 0;
    }
}

