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
 * threadpool_listener.cpp
 *
 *    There are multiple tasks for listener thread:
 *    1. Listen to all connections from client or other componets of this cluster
 *       (like connections from other cn).
 *    2. Dispatch session to available woker thread.
 *
 * IDENTIFICATION
 *    src/gausskernel/process/threadpool/threadpool_listener.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "threadpool/threadpool.h"

#include "access/xact.h"
#include "gssignal/gs_signal.h"
#include "libpq/libpq.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/atomic.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/guc.h"

#include <poll.h>
#include <sys/epoll.h>
#include <sys/socket.h>

#include "communication/commproxy_interface.h"

#define INVALID_FD (-1)

static void TpoolListenerLoop(ThreadPoolListener* listener);

static void ListenerSIGUSR1Handler(SIGNAL_ARGS)
{
    t_thrd.threadpool_cxt.listener->m_reaperAllSession = true;
}

static void ListenerSIGKILLHandler(SIGNAL_ARGS)
{
    proc_exit(0);
}

void TpoolListenerMain(ThreadPoolListener* listener)
{
    t_thrd.proc_cxt.MyProgName = "ThreadPoolListener";
    pgstat_report_appname("ThreadPoolListener");

    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGINT, SIG_IGN);
    // die with pm
    (void)gspqsignal(SIGTERM, SIG_IGN);
    (void)gspqsignal(SIGQUIT, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, ListenerSIGUSR1Handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGFPE, FloatExceptionHandler);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGKILL, ListenerSIGKILLHandler);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    listener->CreateEpoll();
    listener->NotifyReady();

    TpoolListenerLoop(listener);
    proc_exit(0);
}

static void TpoolListenerLoop(ThreadPoolListener* listener)
{
    listener->WaitTask();
}

void ThreadPoolListenerIAm()
{
    t_thrd.role = THREADPOOL_LISTENER;
}

ThreadPoolListener::ThreadPoolListener(ThreadPoolGroup* group)
{
    m_group = group;
    m_tid = InvalidTid;
    m_epollFd = INVALID_FD;
    m_epollEvents = NULL;
    m_reaperAllSession = false;
    m_freeWorkerList = New(CurrentMemoryContext) DllistWithLock();
    m_readySessionList = New(CurrentMemoryContext) DllistWithLock();
    m_idleSessionList = New(CurrentMemoryContext) DllistWithLock();
}

ThreadPoolListener::~ThreadPoolListener()
{
    if (ENABLE_THREAD_POOL_DN_LOGICCONN) {
        CommEpollClose(m_epollFd);
    } else {
        comm_close(m_epollFd); /* CommProxy support */
    }
    m_group = NULL;
    m_epollEvents = NULL;
    m_freeWorkerList = NULL;
    m_readySessionList = NULL;
    m_idleSessionList = NULL;
}

int ThreadPoolListener::StartUp()
{
    m_tid = initialize_util_thread(THREADPOOL_LISTENER, (void*)this);
    return ((m_tid == 0) ? STATUS_ERROR : STATUS_OK);
}

void ThreadPoolListener::NotifyReady()
{
    m_group->m_listenerNum = 1;
}

void ThreadPoolListener::CreateEpoll()
{
    /* MAX_LISTEN_SESSIONS for epool_create is ignored Since Linux 2.6.8 */
    if (ENABLE_THREAD_POOL_DN_LOGICCONN) {
        m_epollFd = CommEpollCreate(GLOBAL_MAX_SESSION_NUM);
    } else {
        CommSetEpollOption(CommEpollThreadPoolListener);
        m_epollFd = comm_epoll_create(GLOBAL_MAX_SESSION_NUM);
    }

    if (m_epollFd == INVALID_FD) {
        ereport(LOG,
            (errmsg("Fail to create epoll for thread pool listener, "
                    "check if the system is out of memory or "
                    "limit on the total number of open files has been reached.")));
        proc_exit(0);
    }

    m_epollEvents = (struct epoll_event*)palloc0_noexcept(sizeof(struct epoll_event) * GLOBAL_MAX_SESSION_NUM);

    if (m_epollEvents == NULL) {
        elog(LOG, "Not enough memory for listener epoll");
        proc_exit(0);
    }
}

void ThreadPoolListener::AddEpoll(knl_session_context* session)
{
    struct epoll_event ev = {0};
    int res = -1;

    m_idleSessionList->AddTail(&session->elem);
    ereport(DEBUG2,
            (errmodule(MOD_THREAD_POOL),
             errmsg("Add a session:%lu to idleSessionList ", session->session_id)));
    /*
     * Because we will dispatch the socket to worker thread once
     * we find an input event of the socket, so we use one_shot mode.
     */
    ev.events = EPOLLRDHUP | EPOLLIN | EPOLLET | EPOLLONESHOT;
    ev.data.ptr = (void*)session;
    if (session->status != KNL_SESS_UNINIT) {
        /* CommProxy Support */
        if (ENABLE_THREAD_POOL_DN_LOGICCONN) {
            res = CommEpollCtl(m_epollFd, EPOLL_CTL_MOD, session->proc_cxt.MyProcPort->sock, &ev);
        } else {
            res = comm_epoll_ctl(m_epollFd, EPOLL_CTL_MOD, session->proc_cxt.MyProcPort->sock, &ev);
        }
    } else {
        /* CommProxy Support */
        if (ENABLE_THREAD_POOL_DN_LOGICCONN) {
            res = CommEpollCtl(m_epollFd, EPOLL_CTL_ADD, session->proc_cxt.MyProcPort->sock, &ev);
        } else {
            res = comm_epoll_ctl(m_epollFd, EPOLL_CTL_ADD, session->proc_cxt.MyProcPort->sock, &ev);
        }
    }
    if (unlikely(res != 0)) {
#ifdef USE_ASSERT_CHECKING
        ereport(PANIC,
#else
        ereport(WARNING,
#endif
                (errmodule(MOD_THREAD_POOL),
                    errmsg("epoll_ctl fail %m, sess status:%d, sock:%d, host:%s, port:%s",
                           session->status, session->proc_cxt.MyProcPort->sock,
                           session->proc_cxt.MyProcPort->remote_host,
                           session->proc_cxt.MyProcPort->remote_port)));
    }
}

bool ThreadPoolListener::TryFeedWorker(ThreadPoolWorker* worker)
{
    Dlelem* sc = m_readySessionList->RemoveHead();
    if (sc != NULL) {
        worker->SetSession((knl_session_context*)sc->dle_val);
        pg_atomic_fetch_sub_u32((volatile uint32*)&m_group->m_waitServeSessionCount, 1);
        pg_atomic_fetch_add_u32((volatile uint32*)&m_group->m_processTaskCount, 1);
        return true;
    } else {
        m_freeWorkerList->AddTail(&worker->m_elem);
        pg_atomic_fetch_add_u32((volatile uint32*)&m_group->m_idleWorkerNum, 1);
        return false;
    }
}

void ThreadPoolListener::AddNewSession(knl_session_context* session)
{
    AddEpoll(session);
    (void)pg_atomic_fetch_add_u32((volatile uint32*)&m_group->m_sessionCount, 1);
    ereport(DEBUG2, 
        (errmodule(MOD_THREAD_POOL), 
            errmsg("This group add a session, and now sessionCount is %d, ",
                m_group->m_sessionCount)));
}

void ThreadPoolListener::SendShutDown()
{
    m_reaperAllSession = true;
    gs_signal_send(m_tid, SIGUSR1);
}

void ThreadPoolListener::ShutDown() const
{
    if (m_tid != 0)
        gs_signal_send(m_tid, SIGKILL);
}

void ThreadPoolListener::ReaperAllSession()
{
    Dlelem* elem = NULL;
    knl_session_context* sess = NULL;

    while (m_group->m_sessionCount > 0) {
        /*
         * There is a very rare case that all thread pool workers happen to
         * encounter FATAL and exit before close session.
         * Under such scenarios, we choose to exit directly.
         */
        if (m_group->m_workerNum <= 0 && m_group->m_sessionCount > 0) {
            ereport(WARNING,
                (errmsg("No thread pool worker left while waiting for session close. "
                        "This is a very rare case when all thread pool workers happen to"
                        " encounter FATAL problems before session close.")));
            abort();
        }
        /* m_sessionCount should be sum of the list length of m_idleSessionList and m_readySessionList
           and worker's attached session */
        pg_memory_barrier();
        if (m_idleSessionList->IsEmpty() && m_readySessionList->IsEmpty() &&
            m_group->m_workerNum - m_group->m_idleWorkerNum == 0) {
            ereport(WARNING, (errmsg("SessionCount should be zero when no session in this group.")));
            m_group->m_sessionCount = 0;
        }

        elem = m_idleSessionList->RemoveHead();
        while (elem != NULL) {
            sess = (knl_session_context*)DLE_VAL(elem);
            ereport(DEBUG2,
                (errmodule(MOD_THREAD_POOL),
                    errmsg("ReaperAllSession remove a session:%lu from idleSessionList", sess->session_id)));
            if (ENABLE_THREAD_POOL_DN_LOGICCONN) {
                struct epoll_event ev = {0};
                ev.events = EPOLLRDHUP | EPOLLIN | EPOLLET | EPOLLONESHOT;
                ev.data.ptr = (void*)sess;
                CommEpollCtl(m_epollFd, EPOLL_CTL_DEL, sess->proc_cxt.MyProcPort->sock, &ev);
            } else {    /* CommProxy Support */
                comm_epoll_ctl(m_epollFd, EPOLL_CTL_DEL, sess->proc_cxt.MyProcPort->sock, NULL);
            }
            sess->status = KNL_SESS_CLOSE;
            DispatchSession(sess);
            elem = m_idleSessionList->RemoveHead();
        }
        pg_usleep(100);
    }
    m_reaperAllSession = false;
}

void ThreadPoolListener::WaitTask()
{
    int nevents = 0;

    while (true) {
        if (m_reaperAllSession) {
            ReaperAllSession();
        }

        /* as we specify timeout -1, so 0 will not be return, either > 0 or < 0 */
        if (ENABLE_THREAD_POOL_DN_LOGICCONN) {
            nevents = CommEpollWait(m_epollFd, m_epollEvents, GLOBAL_MAX_SESSION_NUM, -1);
        } else {
            nevents = comm_epoll_wait(m_epollFd, m_epollEvents, GLOBAL_MAX_SESSION_NUM, -1); /* CommProxy Support */
        }
        if (nevents > 0 && nevents <= GLOBAL_MAX_SESSION_NUM) {
            HandleConnEvent(nevents);
            continue;
        } else if (nevents > GLOBAL_MAX_SESSION_NUM) {
            ereport(PANIC,
                (errmsg("epoll receive %d events which exceed the limitation %d", nevents, GLOBAL_MAX_SESSION_NUM)));
        } else if (nevents == -1 && errno == EINTR) {
            continue;
        } else {
            ereport(LOG, (errmsg("listener wait event encounter some error :%d", errno)));
        }
    }
}

void ThreadPoolListener::HandleConnEvent(int nevets)
{
    knl_session_context* session = NULL;
    struct epoll_event* tmp_event = NULL;

    for (int i = 0; i < nevets; i++) {
        tmp_event = &m_epollEvents[i];
        session = GetSessionBaseOnEvent(tmp_event);

        if (session == NULL) {
            continue;
        }

        DispatchSession(session);
    }
}

knl_session_context* ThreadPoolListener::GetSessionBaseOnEvent(struct epoll_event* ev)
{
    knl_session_context* session = (knl_session_context*)ev->data.ptr;

    if (ev->events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
        /* The worker thread will do the left clean up work. */
        if (session->status == KNL_SESS_UNINIT) {
            session->status = KNL_SESS_CLOSERAW;
        } else {
            session->status = KNL_SESS_CLOSE;
        }
        return session;
    } else if (ev->events & EPOLLIN) {
        return session;
    }
    return NULL;
}

void ThreadPoolListener::DispatchSession(knl_session_context* session)
{
    m_idleSessionList->Remove(&session->elem);
    /*
     * If the sock, idx, and streamid parameters of the current session
     * do not meet the requirements for logical connection parameters,
     * skip this dispatch operation.
     */
    if (session->proc_cxt.MyProcPort->sock == NO_SOCKET &&
        session->proc_cxt.MyProcPort->gs_sock.idx == 0 &&
        session->proc_cxt.MyProcPort->gs_sock.sid == 0) {
        ereport(WARNING, (errmsg("The DispatchSession is not executed, sock:%d idx:%d sid:%d.",
            session->proc_cxt.MyProcPort->sock,
            session->proc_cxt.MyProcPort->gs_sock.idx,
            session->proc_cxt.MyProcPort->gs_sock.sid)));
        return;
    }
    while (true) {
        Dlelem* sc = m_freeWorkerList->RemoveHead();
        if (sc != NULL) {
            ereport(DEBUG2,
                    (errmodule(MOD_THREAD_POOL),
                     errmsg("%s remove session:%lu from idleSessionList to worker", __func__, session->session_id)));
            if (((ThreadPoolWorker*)DLE_VAL(sc))->WakeUpToWork(session)) {
                pg_atomic_fetch_add_u32((volatile uint32*)&m_group->m_processTaskCount, 1);
                break;
           }
        } else {
            ereport(DEBUG2,
                    (errmodule(MOD_THREAD_POOL),
                        errmsg("%s remove session:%lu from idleSessionList to readySessionList",
                               __func__, session->session_id)));
            INSTR_TIME_SET_CURRENT(session->last_access_time);
            
            /* Add new session to the head so the connection request can be quickly processed. */
            if (session->status == KNL_SESS_UNINIT) {
                m_readySessionList->AddHead(&session->elem);
            } else {
                m_readySessionList->AddTail(&session->elem);
            }

            pg_atomic_fetch_add_u32((volatile uint32*)&m_group->m_waitServeSessionCount, 1);
            break;
        }
    }
}

void ThreadPoolListener::DelSessionFromEpoll(knl_session_context* session)
{
    if (ENABLE_THREAD_POOL_DN_LOGICCONN) {
        struct epoll_event ev = {0};
        ev.events = EPOLLRDHUP | EPOLLIN | EPOLLET | EPOLLONESHOT;
        ev.data.ptr = (void*)session;
        CommEpollCtl(m_epollFd, EPOLL_CTL_DEL, session->proc_cxt.MyProcPort->sock, &ev);
#ifdef ENABLE_MULTIPLE_NODES
    } else {
#else
    } else if (session->proc_cxt.MyProcPort->sock > 0) {
#endif
        comm_epoll_ctl(m_epollFd, EPOLL_CTL_DEL, session->proc_cxt.MyProcPort->sock, NULL);
    }
    (void)pg_atomic_fetch_sub_u32((volatile uint32*)&m_group->m_sessionCount, 1);
}

void ThreadPoolListener::RemoveWorkerFromList(ThreadPoolWorker* worker)
{
    m_freeWorkerList->Remove(&worker->m_elem);
}

bool ThreadPoolListener::GetSessIshang(instr_time* current_time, uint64* sessionId)
{
    bool ishang = true;
    m_readySessionList->GetLock();

    Dlelem* elem = m_readySessionList->GetHead();
    if (elem == NULL) {
        m_readySessionList->ReleaseLock();
        return false;
    }
    knl_session_context* head_sess = (knl_session_context *)(elem->dle_val);
    if (INSTR_TIME_GET_MICROSEC(head_sess->last_access_time) == INSTR_TIME_GET_MICROSEC(*current_time) &&
        head_sess->session_id == *sessionId) {
        ishang = true;
    } else {
        *current_time = head_sess->last_access_time;
        *sessionId = head_sess->session_id;
        ishang = false;
    }
    m_readySessionList->ReleaseLock();
    return ishang;
}

