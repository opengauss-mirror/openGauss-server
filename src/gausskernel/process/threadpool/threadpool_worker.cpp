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
 * threadpool_worker.cpp
 *
 *        ThreadPoolWorker will get an active session from ThreadPoolListener,
 *        read command from the session and execute the command. This class is
 *        also response to init and free session.
 *        The worker thread in thread pool is almost the original PostgresMain
 *        thread. However there is an important difference between them, that is
 *        the worker thread is stateless, which means it can serve any session
 *        related to any user and database.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/process/threadpool/threadpool_worker.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "threadpool/threadpool.h"

#include "access/xact.h"
#include "commands/prepare.h"
#include "commands/tablespace.h"
#include "commands/vacuum.h"
#include "gssignal/gs_signal.h"
#include "lib/dllist.h"
#include "lib/stringinfo.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "storage/ipc.h"
#include "storage/smgr/fd.h"
#include "storage/pmsignal.h"
#include "storage/sinvaladt.h"
#include "storage/smgr/smgr.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/plpgsql.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/syscache.h"
#include "utils/xml.h"
#include "executor/executor.h"
#include "storage/procarray.h"
#include "communication/commproxy_interface.h"

/* ===================== Static functions to init session ===================== */
static bool InitSession(knl_session_context* sscxt);
static bool InitPort(Port* port);
static void SendSessionIdxToClient();
static void ResetSignalHandle();
static void SessionSetBackendOptions();

ThreadPoolWorker::ThreadPoolWorker(uint idx, ThreadPoolGroup* group, pthread_mutex_t* mutex, pthread_cond_t* cond)
{
    m_idx = idx;
    m_group = group;
    m_tid = InvalidTid;
    m_threadStatus = THREAD_UNINIT;
    m_currentSession = NULL;
    m_mutex = mutex;
    m_cond = cond;
    m_waitState = STATE_WAIT_UNDEFINED;
    DLInitElem(&m_elem, this);
    m_thrd = &t_thrd;
}

ThreadPoolWorker::~ThreadPoolWorker()
{
    m_currentSession = NULL;
    m_group = NULL;
    m_mutex = NULL;
    m_cond = NULL;
}

void ThreadPoolWorker::ShutDown()
{
    pthread_mutex_lock(m_mutex);
    m_threadStatus = THREAD_EXIT;
    CleanUpSession(true);
    /* Remove the worker if it is in the free worker list. */
    m_group->GetListener()->RemoveWorkerFromList(this);
    pthread_mutex_unlock(m_mutex);
    m_group->ReleaseWorkerSlot(m_idx);
}

void ThreadPoolWorker::NotifyReady()
{
    pthread_mutex_lock(m_mutex);
    m_threadStatus = (m_threadStatus == THREAD_EXIT) ? THREAD_EXIT : THREAD_RUN;
    pthread_mutex_unlock(m_mutex);
}

int ThreadPoolWorker::StartUp()
{
    Port port;
    int ss_rc = memset_s(&port, sizeof(port), 0, sizeof(port));
    securec_check(ss_rc, "\0", "\0");

    port.canAcceptConnections = CAC_OK;
    port.sock = PGINVALID_SOCKET;
    port.gs_sock = GS_INVALID_GSOCK;
    /* Calculate cancel key which will be assigned to backend. */
    GenerateCancelKey(false);
    t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
    if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
        return STATUS_ERROR;
    }
    Backend* bn = CreateBackend();
    m_tid = initialize_worker_thread(THREADPOOL_WORKER, &port, (void*)this);
    t_thrd.proc_cxt.MyPMChildSlot = 0;
    if (m_tid == InvalidTid) {
        ReleasePostmasterChildSlot(bn->child_slot);
        bn->pid = 0;
        bn->role = (knl_thread_role)0;
        return STATUS_ERROR;
    }

    bn->pid = m_tid;
    bn->role = THREADPOOL_WORKER;
    Assert(bn->child_slot != 0);
    AddBackend(bn);

    return STATUS_OK;
}

void PreventSignal()
{
    HOLD_INTERRUPTS();
    t_thrd.int_cxt.QueryCancelPending = false;
    t_thrd.int_cxt.ignoreBackendSignal = true;
    disable_sig_alarm(true);
}

void AllowSignal()
{
    t_thrd.int_cxt.ignoreBackendSignal = false;
    /* now we can accept signal. out of this, we rely on signal handle. */
    RESUME_INTERRUPTS();
}

void ThreadPoolWorker::WaitMission()
{
    /* Return if we still in a transaction block. */
    if (!WorkerThreadCanSeekAnotherMission(&m_reason)) {
        return;
    }

    (void)enable_session_sig_alarm(u_sess->attr.attr_common.SessionTimeout * 1000);
    bool isRawSession = false;

    Assert(t_thrd.int_cxt.InterruptHoldoffCount == 0);
    if (unlikely(t_thrd.int_cxt.InterruptHoldoffCount > 0)) {
        ereport(PANIC,
                (errmodule(MOD_THREAD_POOL),
                 errmsg("InterruptHoldoffCount should be zero when get next session.")));
    }
    /*
     * prevent any signal execep siguit.
     * reset any pending signal and timer.
     * before we serve next session we must keep us clean.
     */
    PreventSignal();

    MemoryContext old = CurrentMemoryContext;
    while (true) {
        /* we should keep the thread clean for next Session. */
        CleanThread();
        /* Get next session. */
        WaitNextSession();
        Assert(m_currentSession != NULL);
        isRawSession = (m_currentSession->status == KNL_SESS_UNINIT);
        /* do the binding process ,binding the connection and thread */
        /* return to worker pool if binding fail. */
        if (AttachSessionToThread()) {
            if (isRawSession) {
                if (t_thrd.libpq_cxt.PqRecvPointer == t_thrd.libpq_cxt.PqRecvLength) {
                    continue;
                } else {
                    ereport(ERROR, 
                        (errcode(ERRCODE_PROTOCOL_VIOLATION),
                            errmsg("receive more connection message %d than expect %d",
                                t_thrd.libpq_cxt.PqRecvLength, t_thrd.libpq_cxt.PqRecvPointer)));
                }
            }
            Assert(m_currentSession != NULL);
            Assert(u_sess != NULL);
			
            /*
             * CommProxy Support
             *
             * session attach thread success, we record relation of sock with worker
             */
            if (AmIProxyModeSockfd(m_currentSession->proc_cxt.MyProcPort->sock)) {
                g_comm_controller->SetCommSockActive(m_currentSession->proc_cxt.MyProcPort->sock, m_idx);
            }

            Assert(CheckMyDatabaseMatch());
			
            break;
        }
    }
    MemoryContextSwitchTo(old);
    (void)disable_session_sig_alarm();
    /* now we can accept signal. out of this, we rely on signal handle. */
    AllowSignal();
    ShutDownIfNecessary();
}

bool ThreadPoolWorker::WakeUpToWork(knl_session_context* session)
{
    bool succ = true;
    pthread_mutex_lock(m_mutex);
    if (likely(m_threadStatus != THREAD_EXIT && m_threadStatus != THREAD_PENDING)) {
        m_currentSession = session;
        pthread_cond_signal(m_cond);
    } else {
        succ = false;
    }
    pthread_mutex_unlock(m_mutex);
    return succ;
}

void ThreadPoolWorker::WakeUpToUpdate(ThreadStatus status)
{
    pthread_mutex_lock(m_mutex);
    if (m_threadStatus != THREAD_EXIT) {
        m_threadStatus = status;
        pthread_cond_signal(m_cond);
    }
    pthread_mutex_unlock(m_mutex);
}

bool ThreadPoolWorker::WakeUpToPendingIfFree()
{
    bool ans = false;
    pthread_mutex_lock(m_mutex);
    if (m_threadStatus != THREAD_EXIT && m_threadStatus != THREAD_PENDING && m_currentSession == NULL) {
        m_threadStatus = THREAD_PENDING;
        pthread_cond_signal(m_cond);
        ans = true;
    } else {
        ans = false;
    }
    pthread_mutex_unlock(m_mutex);
    return ans;
}

/*
 * Some variable are session level, however they are used by some opensource
 * component like postgis, we can not move them to knl_session_context directly.
 * To solve this problem, providing two interface: RestoreThreadVariable and
 * SaveThreadVariable.
 */
void ThreadPoolWorker::RestoreThreadVariable()
{
    Assert(m_currentSession != NULL);

    /* use values in session to set local thread GUC */
    SetThreadLocalGUC(m_currentSession);

    /* use values in session to set other thread local variables */
    pg_reset_srand48(m_currentSession->rand_cxt.rand48_seed);
}

void ThreadPoolWorker::RestoreLocaleInfo()
{
    if (strcmp(NameStr(m_currentSession->mb_cxt.datcollate), NameStr(t_thrd.port_cxt.cur_datcollate)) == 0 &&
        strcmp(NameStr(m_currentSession->mb_cxt.datctype), NameStr(t_thrd.port_cxt.cur_datctype)) == 0) {
        /* no need set again. */
        return;
    }

    if (pg_perm_setlocale(LC_COLLATE, NameStr(m_currentSession->mb_cxt.datcollate)) == NULL) {
        ereport(FATAL,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("database locale is incompatible with operating system"),
                errdetail("The database was initialized with LC_COLLATE \"%s\", "
                          " which is not recognized by setlocale().",
                    NameStr(u_sess->mb_cxt.datcollate)),
                errhint("Recreate the database with another locale or install the missing locale.")));
    }

    if (pg_perm_setlocale(LC_CTYPE, NameStr(m_currentSession->mb_cxt.datctype)) == NULL) {
        ereport(FATAL,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("database locale is incompatible with operating system"),
                errdetail("The database was initialized with LC_CTYPE \"%s\", "
                          " which is not recognized by setlocale().",
                    NameStr(u_sess->mb_cxt.datctype)),
                errhint("Recreate the database with another locale or install the missing locale.")));
    }

    errno_t rc;
    rc = strncpy_s(
        NameStr(t_thrd.port_cxt.cur_datctype), NAMEDATALEN, NameStr(m_currentSession->mb_cxt.datctype), NAMEDATALEN);
    securec_check(rc, "\0", "\0");
    rc = strncpy_s(NameStr(t_thrd.port_cxt.cur_datcollate),
        NAMEDATALEN,
        NameStr(m_currentSession->mb_cxt.datcollate),
        NAMEDATALEN);
    securec_check(rc, "\0", "\0");

    /* Use the right encoding in translated messages */
#ifdef ENABLE_NLS
    pg_bind_textdomain_codeset(textdomain(NULL));
#endif
}

void ThreadPoolWorker::RestoreSessionVariable()
{
    m_currentSession->attr.attr_sql.default_statistics_target = default_statistics_target;
    m_currentSession->attr.attr_common.session_timezone = session_timezone;
    m_currentSession->attr.attr_common.log_timezone = log_timezone;
    m_currentSession->attr.attr_common.client_min_messages = client_min_messages;
    m_currentSession->attr.attr_common.log_min_messages = log_min_messages;
    m_currentSession->attr.attr_common.assert_enabled = assert_enabled;
    m_currentSession->attr.attr_common.AlarmReportInterval = AlarmReportInterval;
    m_currentSession->attr.attr_common.xmloption = xmloption;
    m_currentSession->attr.attr_network.comm_client_bind = comm_client_bind;
    m_currentSession->attr.attr_network.comm_ackchk_time = comm_ackchk_time;

    unsigned short* rand48 = pg_get_srand48();
    m_currentSession->rand_cxt.rand48_seed[0] = rand48[0];
    m_currentSession->rand_cxt.rand48_seed[1] = rand48[1];
    m_currentSession->rand_cxt.rand48_seed[2] = rand48[2];
}

void ThreadPoolWorker::SetSessionInfo()
{
    /*
     * The proc and pgxact are more likely thread level variable, maybe we need to
     * reconsider if it's better to put it in knl_thread_context.
     */
    struct PGPROC* thread_proc = t_thrd.proc;
    thread_proc->databaseId = m_currentSession->proc_cxt.MyDatabaseId;
    thread_proc->roleId = m_currentSession->proc_cxt.MyRoleId;
    Assert(thread_proc->pid == t_thrd.proc_cxt.MyProcPid);
    thread_proc->sessionid = m_currentSession->session_id;
    thread_proc->globalSessionId = m_currentSession->globalSessionId;
    thread_proc->workingVersionNum = m_currentSession->proc_cxt.MyProcPort->SessionVersionNum;
    m_currentSession->attachPid = thread_proc->pid;

    if (t_thrd.pgxact != NULL && m_currentSession->proc_cxt.Isredisworker) {
        LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
        t_thrd.pgxact->vacuumFlags |= PROC_IS_REDIST;
        LWLockRelease(ProcArrayLock);
    }
}

void ThreadPoolWorker::WaitNextSession()
{
    if (EnableLocalSysCache()) {
        g_instance.global_sysdbcache.GSCMemThresholdCheck();
    }
    /* Return worker to pool unless we can get a task right now. */
    ThreadPoolListener* lsn = m_group->GetListener();
    Assert(lsn != NULL);

    while (true) {
        /* Wait if the thread was turned into pending mode. */
        if (unlikely(m_threadStatus == THREAD_PENDING)) {
            Pending();
            /* pending thread must don't have session on it */
            if (m_currentSession != NULL) {
                u_sess = m_currentSession;
                ereport(FATAL,
                        (errmodule(MOD_THREAD_POOL),
                         errmsg("pending thread should not have session %lu on it",
                                 m_currentSession->session_id)));
            }
        } else if (unlikely(m_threadStatus == THREAD_EXIT)) {
            ShutDownIfNecessary();
        } else if (m_currentSession != NULL) {
            break;
        }
    
        /* Wait for listener dispatch. */
        if (!lsn->TryFeedWorker(this)) {
            /* report thread status. */
            u_sess = t_thrd.fake_session;
            WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_COMM);

            pthread_mutex_lock(m_mutex);
            while (!m_currentSession) {
                if (unlikely(m_threadStatus == THREAD_PENDING || m_threadStatus == THREAD_EXIT)) {
                    break;
                }
                pthread_cond_wait(m_cond, m_mutex);
            }
            pthread_mutex_unlock(m_mutex);
            m_group->GetListener()->RemoveWorkerFromList(this);
            pg_atomic_fetch_sub_u32((volatile uint32*)&m_group->m_idleWorkerNum, 1);
            pgstat_report_waitstatus(oldStatus);
        } else {
            u_sess = t_thrd.fake_session;
            ereport(DEBUG2,
                    (errmodule(MOD_THREAD_POOL),
                     errmsg("TryFeedWorker remove a session:%lu from m_readySessionList into worker, status %d",
                            m_currentSession->session_id, m_threadStatus)));
        }
    }
}

void ThreadPoolWorker::Pending()
{
    pg_atomic_fetch_sub_u32((volatile uint32*)&m_group->m_workerNum, 1);
    pthread_mutex_lock(m_mutex);
    while (m_threadStatus == THREAD_PENDING) {
        pthread_cond_wait(m_cond, m_mutex);
    }
    pthread_mutex_unlock(m_mutex);
    pg_atomic_fetch_add_u32((volatile uint32*)&m_group->m_workerNum, 1);

    if (m_threadStatus == THREAD_EXIT) {
        ShutDownIfNecessary();
    } 
}

void ThreadPoolWorker::ShutDownIfNecessary()
{
    if (unlikely(m_threadStatus == THREAD_EXIT)) {
        if (!m_currentSession) {
            use_fake_session();
            m_currentSession = t_thrd.fake_session;
        } else {
            u_sess = m_currentSession;
        }

        RestoreThreadVariable();
        proc_exit(0);
    }
    /* there is time window which the cancle signal has arrived but ignored by prevent signal called before,
     * so we rebuild the signal status here in case that happens. */
    if (unlikely(m_currentSession != NULL && m_currentSession->status == KNL_SESS_CLOSE)) {
        t_thrd.int_cxt.ClientConnectionLost = true;
        ereport(ERROR, (errmodule(MOD_THREAD_POOL),
                        errmsg("Interrupt signal has been received, but ignored by preventSignal, rebuild it.")));
    }
}

void ThreadPoolWorker::CleanThread()
{
    /*
     * In thread pool mode, ensure that packet transmission must be completed before thread switchover.
     * Otherwise, packet format disorder may occurs.
     */
    if (m_currentSession != NULL && t_thrd.libpq_cxt.PqSendPointer > 0) {
        int res = pq_flush();
        if (res != 0) {
            ereport(WARNING, (errmsg("[cleanup thread] failed to flush the remaining content. detail: %d", res)));
        }
    }
    
    /*
     * Clean up Allocated descs incase long jump happend
     * and they are not cleaned up in AtEOXact_Files.
     */
    FreeAllAllocatedDescs();

    /* we should abandon this session. */
    if (t_thrd.int_cxt.ClientConnectionLost || t_thrd.threadpool_cxt.reaper_dead_session) {
        t_thrd.int_cxt.ClientConnectionLost = false;
        t_thrd.threadpool_cxt.reaper_dead_session = false;
        CleanUpSession(false);
    }

    InterruptPending = false;
    t_thrd.int_cxt.QueryCancelPending = false;
    t_thrd.int_cxt.PoolValidateCancelPending = false;
    t_thrd.libpq_cxt.PqSendStart = 0;
    t_thrd.libpq_cxt.PqSendPointer = 0;
    t_thrd.libpq_cxt.PqRecvLength = 0;
    t_thrd.libpq_cxt.PqRecvPointer = 0;
    t_thrd.xact_cxt.currentGxid = InvalidGlobalTransactionId;

    struct PGPROC* thread_proc = t_thrd.proc;
    thread_proc->databaseId = InvalidOid;
    thread_proc->roleId = InvalidOid;
    thread_proc->sessionid = t_thrd.fake_session->session_id;
    thread_proc->globalSessionId = t_thrd.fake_session->globalSessionId;
    thread_proc->workingVersionNum = pg_atomic_read_u32(&WorkingGrandVersionNum);

    if (m_currentSession != NULL) {
        DetachSessionFromThread();
    }
}

void ThreadPoolWorker::DetachSessionFromThread()
{
    /* session attach thread success, we record relation of sock with worker */
    if (AmIProxyModeSockfd(m_currentSession->proc_cxt.MyProcPort->sock)) {
        g_comm_controller->SetCommSockIdle(m_currentSession->proc_cxt.MyProcPort->sock);
    }

    /* If some error occur at session initialization, we need to close it. */
    if (m_currentSession->status == KNL_SESS_UNINIT) {
        m_currentSession->status = KNL_SESS_CLOSERAW;
        /* cache may be in wrong stat, rebuild is ok */
        ReBuildLSC();
        CleanUpSession(false);
        m_currentSession = NULL;
        u_sess = NULL;
        return;
    }
    m_currentSession->status = KNL_SESS_DETACH;
    if (t_thrd.pgxact != NULL && m_currentSession->proc_cxt.Isredisworker) {
        LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
        t_thrd.pgxact->vacuumFlags &= ~PROC_IS_REDIST;
        LWLockRelease(ProcArrayLock);
    }
    if (ENABLE_GPC) {
#ifdef ENABLE_MULTIPLE_NODES
        CleanSessionGPCDetach(m_currentSession);
#endif
        m_currentSession->pcache_cxt.gpc_in_ddl = false;
    }
    RestoreSessionVariable();
    pgstat_couple_decouple_session(false);
    pgstat_deinitialize_session();
    m_currentSession->attachPid = (ThreadId)-1;

    /* should restore the data before return to listener. */
    m_group->GetListener()->AddEpoll(m_currentSession);
    m_currentSession = NULL;
    u_sess = NULL;
}

bool ThreadPoolWorker::AttachSessionToThread()
{
    Assert(m_currentSession != NULL);
    Assert(t_thrd.utils_cxt.TopTransactionResourceOwner == NULL);

    SetSessionInfo();
    RestoreThreadVariable();
    if (m_currentSession->status == KNL_SESS_DETACH) {
        RestoreLocaleInfo();
    }

    u_sess = m_currentSession;
    t_thrd.postgres_cxt.whereToSendOutput = DestRemote;
    SelfMemoryContext = u_sess->self_mem_cxt;
    /*
     * Since thread pool worker may start earlier than startup finishing recovery,
     * init xlog access if necessary.
     */
    PG_TRY();
    {
        (void)RecoveryInProgress();
    }
    PG_CATCH();
    {
        /* if init xlog has error, should throw fatal this thread */
        ereport(FATAL, (errmsg("init xlog failed, throw fatal for this thread")));
    }
    PG_END_TRY();
#ifdef ENABLE_QUNIT
    set_qunit_case_number_hook(u_sess->utils_cxt.qunit_case_number, NULL);
#endif

    switch (m_currentSession->status) {
        case KNL_SESS_UNINIT: {
            if (InitSession(m_currentSession)) {
                /* Registering backend_version */
                if (t_thrd.proc && contain_backend_version(t_thrd.proc->workingVersionNum)) {
                    register_backend_version(t_thrd.proc->workingVersionNum);
                }
                m_currentSession->status = KNL_SESS_ATTACH;
                Assert(CheckMyDatabaseMatch());
            } else {
                m_currentSession->status = KNL_SESS_CLOSE;
                /* clean up mess. */
                CleanUpSession(false);
                m_currentSession = NULL;
                u_sess = NULL;
            }
            /* init port will change the signal handle */
            ResetSignalHandle();
        } break;

        case KNL_SESS_DETACH: {
#ifdef ENABLE_LITE_MODE
            char thr_name[16];
            int rcs = 0;
            Port *port = m_currentSession->proc_cxt.MyProcPort;

            if (t_thrd.role == WORKER) {
                rcs = snprintf_truncated_s(thr_name, sizeof(thr_name), "w:%s", port->user_name);
                securec_check_ss(rcs, "\0", "\0");
                (void)pthread_setname_np(gs_thread_self(), thr_name);
            } else if (t_thrd.role == THREADPOOL_WORKER) {
                rcs = snprintf_truncated_s(thr_name, sizeof(thr_name), "tw:%s", port->user_name);
                securec_check_ss(rcs, "\0", "\0");
                (void)pthread_setname_np(gs_thread_self(), thr_name);
            }
#endif
            pgstat_initialize_session();
            pgstat_couple_decouple_session(true);
             /* Postgres init thread syscache. */
            t_thrd.proc_cxt.PostInit->InitLoadLocalSysCache(u_sess->proc_cxt.MyDatabaseId, 
                u_sess->proc_cxt.MyProcPort->database_name);
            Assert(CheckMyDatabaseMatch());
            m_currentSession->status = KNL_SESS_ATTACH;
        } break;

        case KNL_SESS_CLOSERAW:
        case KNL_SESS_CLOSE: {
            /* unified auditing logout */
            audit_processlogout_unified();

            /* clean up tmp schema */
            RemoveTempNamespace();

            /* clean up mess. */
            CleanUpSession(false);
            m_currentSession = NULL;
            u_sess = NULL;
#ifdef ENABLE_LITE_MODE
            if (EnableLocalSysCache()) {
                t_thrd.lsc_cxt.lsc->LocalSysDBCacheReSet();
            }
#endif
        } break;

        default:
            Assert(false);
            ereport(PANIC,
                (errcode(ERRCODE_INVALID_ATTRIBUTE),
                    errmsg("undefined state %d for session attach", m_currentSession->status)));
    }

    if (m_currentSession && m_currentSession->status == KNL_SESS_ATTACH) {
        return true;
    } else {
        use_fake_session();
        Assert(m_currentSession == NULL);
        return false;
    }
}

void ThreadPoolWorker::CleanUpSessionWithLock()
{
    if (m_currentSession == NULL) {
        return;
    }

    if (t_thrd.pgxact != NULL && m_currentSession->proc_cxt.Isredisworker) {
        LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
        t_thrd.pgxact->vacuumFlags &= ~PROC_IS_REDIST;
        LWLockRelease(ProcArrayLock);
    }
}

void ThreadPoolWorker::CleanUpSession(bool threadexit)
{
    if (m_currentSession == NULL) {
        return;
    }

    if (m_currentSession->status == KNL_SESS_FAKE) {
        Assert(m_threadStatus == THREAD_EXIT);
        return;
    }

    if (m_currentSession->status != KNL_SESS_END_PHASE1) {
        InitThreadLocalWhenSessionExit();

        if (!threadexit) {
            CleanUpSessionWithLock();
            DecreaseUserCount(m_currentSession->proc_cxt.MyRoleId);
        }

        /* Close Session. */
        m_group->GetListener()->DelSessionFromEpoll(m_currentSession);

        if (m_currentSession->proc_cxt.PassConnLimit) {
            SpinLockAcquire(&g_instance.conn_cxt.ConnCountLock);
            g_instance.conn_cxt.CurConnCount--;
            Assert(g_instance.conn_cxt.ConnCountLock >= 0);
            SpinLockRelease(&g_instance.conn_cxt.ConnCountLock);
        }

        /*
         * Record this state in case we reenter this function because
         * ERROR/FATAL occurs in sess_exit().
         */
        m_currentSession->status = KNL_SESS_END_PHASE1;
    }

    /*
     * If clean up work already be done at proc_exit(), then we don't need to
     * call sess_exit() anymore, otherwise, there will be double free.
     */
    if (!t_thrd.proc_cxt.proc_exit_inprogress) {
        sess_exit(0);
    }

    /* clear pgstat slot */
    pgstat_release_session_memory_entry();
    pgstat_deinitialize_session();
    pgstat_beshutdown_session(m_currentSession->session_ctr_index);
    localeconv_deinitialize_session();

    /* clean gpc refcount and plancache in shared memory */
    if (ENABLE_DN_GPC)
        CleanSessGPCPtr(m_currentSession);

    /*
     * clear invalid msg slot
     * If called during pool worker thread exit, session's invalid msg slot has already
     * been cleared along with that of pool worker in shmem_exit.
     */
    if (!t_thrd.proc_cxt.proc_exit_inprogress) {
        CleanupWorkSessionInvalidation();
    }

    g_threadPoolControler->GetSessionCtrl()->FreeSlot(m_currentSession->session_ctr_index);
    m_currentSession->session_ctr_index = -1;

    free_session_context(m_currentSession);
    m_currentSession = NULL;
}

Backend* ThreadPoolWorker::CreateBackend()
{
    Backend* bn = AssignFreeBackEnd(t_thrd.proc_cxt.MyPMChildSlot);
    bn->cancel_key = t_thrd.proc_cxt.MyCancelKey;
    bn->child_slot = t_thrd.proc_cxt.MyPMChildSlot;

    return bn;
}

void ThreadPoolWorker::AddBackend(Backend* bn)
{
    bn->is_autovacuum = false;
    DLInitElem(&bn->elem, bn);
    DLAddHead(g_instance.backend_list, &bn->elem);
}

static void init_session_share_memory()
{
    TableSpaceUsageManager::Init();
#ifndef ENABLE_MULTIPLE_NODES
    ReplicationOriginShmemInit();
#endif
}

#ifndef ENABLE_MULTIPLE_NODES
extern void InitBSqlPluginHookIfNeeded();
#endif

static bool InitSession(knl_session_context* session)
{
    /* non't send ereport to client now */
    t_thrd.postgres_cxt.whereToSendOutput = DestNone;
    /* Switch context to Session context. */
    AutoContextSwitch memSwitch(session->mcxt_group->GetMemCxtGroup(MEMORY_CONTEXT_DEFAULT));

    /*
     * Set thread version to the latest working version number for
     * InitializeGUCOptions.
     * This is ugly and can not avoid all race conditions during online upgrade.
     */
    t_thrd.proc->workingVersionNum = pg_atomic_read_u32(&WorkingGrandVersionNum);

    if(unlikely(u_sess->proc_cxt.clientIsGsrewind == true 
        && u_sess->proc_cxt.gsRewindAddCount == false)) {
        u_sess->proc_cxt.gsRewindAddCount = true;
        (void)pg_atomic_add_fetch_u32(&g_instance.comm_cxt.current_gsrewind_count, 1);
    }

    /* Init GUC option for this session. */
    InitializeGUCOptions();

    /* Read in remaining GUC variables */
    read_nondefault_variables();

    /* now safe to ereport to client */
    t_thrd.postgres_cxt.whereToSendOutput = DestRemote;

    /* Init port and connection. */
    if (!InitPort(session->proc_cxt.MyProcPort)) {
        /* reset some status below */
        if (!disable_sig_alarm(false)) {
            ereport(FATAL, (errmsg("could not disable timer for startup packet timeout")));
        }
        return false;
    }

    /* switch version number to that gotten from port */
    t_thrd.proc->workingVersionNum = session->proc_cxt.MyProcPort->SessionVersionNum;

    /* add process definer mode */
    Reset_Pseudo_CurrentUserId();

    SetProcessingMode(InitProcessing);

    SessionSetBackendOptions();

    /* initialize guc variables which need to be sended to stream threads */
#ifdef PGXC
    if (IS_PGXC_DATANODE && IsUnderPostmaster) {
        init_sync_guc_variables();
    }
#endif

    /* We need to allow SIGINT, etc during the initial transaction */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);

    /* init invalid msg slot */
    SharedInvalBackendInit(false, true);

    /* init pgstat slot */
    pgstat_initialize_session();

    /* Do local initialization of file, storage and buffer managers */
    InitFileAccess();
    smgrinit();

    /* openGauss init. */
    char* dbname = session->proc_cxt.MyProcPort->database_name;
    char* username = session->proc_cxt.MyProcPort->user_name;
    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(dbname, InvalidOid, username);
    t_thrd.proc_cxt.PostInit->InitSession();

#ifndef ENABLE_MULTIPLE_NODES
    if (u_sess->proc_cxt.MyDatabaseId != InvalidOid && DB_IS_CMPT(B_FORMAT) && u_sess->attr.attr_sql.dolphin) {
        InitBSqlPluginHookIfNeeded();
    }
#endif

    Assert(CheckMyDatabaseMatch());

    SetProcessingMode(NormalProcessing);

    init_session_share_memory();

    BeginReportingGUCOptions();

    SendSessionIdxToClient();

    /* init param hash table for sending set message */
    if (IS_PGXC_COORDINATOR) {
        init_set_params_htab();
    }

    /* check if memory already reach the max_dynamic_memory */
    if (t_thrd.utils_cxt.gs_mp_inited && processMemInChunks > maxChunksPerProcess) {
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                errmsg("memory usage reach the max_dynamic_memory"),
                errdetail("current memory usage is: %u MB, max_dynamic_memory is: %u MB",
                    (unsigned int)processMemInChunks << (chunkSizeInBits - BITS_IN_MB),
                    (unsigned int)maxChunksPerProcess << (chunkSizeInBits - BITS_IN_MB))));
    }

    ReadyForQuery((CommandDest)t_thrd.postgres_cxt.whereToSendOutput);

    return true;
}

static bool InitPort(Port* port)
{
    /* session version number is initialized to process version number */
    port->SessionVersionNum = pg_atomic_read_u32(&WorkingGrandVersionNum);

    PortInitialize(port, NULL);

    CheckClientIp(port);

    PreClientAuthorize();

    int status = ClientConnInitilize(port);

    if (status != STATUS_OK) {
        return false;
    }

    return true;
}

static void SendSessionIdxToClient()
{
    GenerateCancelKey(true);

    if (t_thrd.postgres_cxt.whereToSendOutput == DestRemote && PG_PROTOCOL_MAJOR(FrontendProtocol) >= 2) {
        StringInfoData buf;

        pq_beginmessage(&buf, 'K');
        pq_sendint32(&buf, (uint32)u_sess->session_ctr_index);
        pq_sendint32(&buf, (uint32)u_sess->cancel_key);
        pq_endmessage(&buf);
    }
}

static void ResetSignalHandle()
{
    // may change during thread init port(accept new connection)
    (void)gspqsignal(SIGALRM, handle_sig_alarm);
    (void)gspqsignal(SIGQUIT, quickdie); /* hard crash time */
    (void)gspqsignal(SIGTERM, die);      /* cancel current query and exit */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    /* not necessary, unblock for sure */
    gs_signal_unblock_sigusr2();
}

static void SessionSetBackendOptions()
{
    char** av = NULL;
    int maxac = 0;
    int ac = 0;

    /*
     * Now, build the argv vector that will be given to PostgresMain.
     *
     * The maximum possible number of commandline arguments that could come
     * from ExtraOptions is (strlen(ExtraOptions) + 1) / 2; see
     * pg_split_opts().
     */
    maxac = (strlen(g_instance.ExtraOptions) + 1) / 2 + 2;

    av = (char**)MemoryContextAlloc(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), maxac * sizeof(char*));
    av[ac++] = "gaussdb";
    pg_split_opts(av, &ac, g_instance.ExtraOptions);
    av[ac] = NULL;

    /* Parse command-line options. */
    process_postgres_switches(ac, av, PGC_POSTMASTER, NULL);
}
