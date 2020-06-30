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
 * threadpool_sessctl.cpp
 *        Class ThreadPoolSessControl is used to control all session info in thread pool.
 *
 * IDENTIFICATION
 *        src/gausskernel/process/threadpool/threadpool_sessctl.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "threadpool/threadpool.h"

#include "access/xact.h"
#include "catalog/pg_collation.h"
#include "gssignal/gs_signal.h"
#include "lib/dllist.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "pgxc/pgxc.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "tcop/dest.h"
#include "utils/atomic.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "executor/executor.h"

ThreadPoolSessControl::ThreadPoolSessControl(MemoryContext context)
{
    AutoContextSwitch acontext(context);
    m_context = context;
    pthread_mutex_init(&m_sessCtrlock, NULL);
    m_sessionId = 0;
    m_activeSessionCount = 0;
    m_maxActiveSessionCount = GLOBAL_MAX_SESSION_NUM;
    m_maxReserveSessionCount = GLOBAL_RESERVE_SESSION_NUM;
    m_activelist = NULL;
    m_freelist = NULL;
    m_base = (knl_sess_control*)palloc0(sizeof(knl_sess_control) * m_maxActiveSessionCount);
    for (int i = 0; i < m_maxActiveSessionCount; i++) {
        m_base[i].idx = (m_maxReserveSessionCount + i);
        m_base[i].lock = 0;
        m_base[i].next = m_freelist;
        m_freelist = &m_base[i];
    }
}

ThreadPoolSessControl::~ThreadPoolSessControl()
{
    pfree_ext(m_base);
    m_freelist = NULL;
    m_activelist = NULL;
    m_context = NULL;
}

knl_session_context* ThreadPoolSessControl::CreateSession(Port* port)
{
    knl_session_context* sc = NULL;

    /* We use u_sess->session_id to mark memory context. */
    m_sessionId++;
    sc = create_session_context(m_context, m_sessionId);
    if (sc == NULL) {
        ereport(WARNING, (errmsg("can't allocate memory for session")));
        return NULL;
    }

    MemoryContext old_cxt = MemoryContextSwitchTo(sc->top_mem_cxt);
    sc->proc_cxt.MyProcPort = (Port*)palloc0(sizeof(Port));
    (void)MemoryContextSwitchTo(old_cxt);
    int rc = memcpy_s(sc->proc_cxt.MyProcPort, sizeof(Port), port, sizeof(Port));
    securec_check(rc, "\0", "\0");

    if (AllocateSlot(sc)) {
        sc->stat_cxt.trackedBytes = u_sess->stat_cxt.trackedBytes;
        sc->stat_cxt.trackedMemChunks = u_sess->stat_cxt.trackedMemChunks;
        u_sess->stat_cxt.trackedBytes = 0;
        u_sess->stat_cxt.trackedMemChunks = 0;
        return sc;
    } else {
        MemoryContextDelete(sc->top_mem_cxt);
        pfree(sc);
        return NULL;
    }
}

knl_sess_control* ThreadPoolSessControl::AllocateSlot(knl_session_context* sc)
{
    AutoMutexLock alock(&m_sessCtrlock);
    alock.lock();
    if (canAcceptConnections(true) != CAC_OK) {
        alock.unLock();
        /* CN is in the process of starting recovery, redo is not completed, so the connection error is reasonable */
        ereport(WARNING,
            (errmsg("ThreadPool cannot start new session due to PM state, PM state is %s", GetPMState(pmState))));
        return NULL;
    }

    if (m_activeSessionCount == m_maxActiveSessionCount) {
        alock.unLock();
        ereport(WARNING,
            (errmsg("ThreadPool cannot start new session due to to many sessions, current upper bound is %d",
                m_maxActiveSessionCount)));
        return NULL;
    }
    Assert(m_freelist != NULL);

    /* remove from free list */
    knl_sess_control* ctrl = m_freelist;
    m_freelist = ctrl->next;
    /* add to active list */
    ctrl->prev = NULL;
    ctrl->next = m_activelist;
    if (m_activelist != NULL) {
        m_activelist->prev = ctrl;
    }
    m_activelist = ctrl;
    ctrl->sess = sc;
    sc->session_ctr_index = ctrl->idx;
    m_activeSessionCount++;
    alock.unLock();

    return ctrl;
}

void ThreadPoolSessControl::FreeSlot(int ctrl_index)
{
    if (!IsValidCtrlIndex(ctrl_index)) {
        return;
    }

    AutoMutexLock alock(&m_sessCtrlock);
    alock.lock();
    /* remove from active list. */
    knl_sess_control *ctrl, *prev, *next;
    ctrl = &m_base[ctrl_index - m_maxReserveSessionCount];
    prev = ctrl->prev;
    next = ctrl->next;
    if (prev == NULL) {
        m_activelist = next;
    } else {
        prev->next = next;
    }
    if (next != NULL) {
        next->prev = prev;
    }

    /* return to free list. */
    ctrl->next = m_freelist;
    m_freelist = ctrl;

    m_activeSessionCount--;
    volatile sig_atomic_t* plock = &ctrl->lock;
    sig_atomic_t val;
    do {
        if (*plock == 0) {
            /*  perform an atomic compare and swap. */
            val = __sync_val_compare_and_swap(plock, 0, 1);
            if (val == 0) {
                ctrl->sess = NULL;
                /*  restore the value. */
                ctrl->lock = 0;
                break;
            }
        }
        pg_usleep(100);
    } while (true);
    alock.unLock();
}

void ThreadPoolSessControl::MarkAllSessionClose()
{
    /* Mark all session to be closed. */
    AutoMutexLock alock(&m_sessCtrlock);
    alock.lock();
    knl_sess_control* ctrl = m_activelist;
    while (ctrl != NULL) {
        ctrl->sess->status = KNL_SESS_CLOSE;
        CloseClientSocket(ctrl->sess, false);
        ctrl = ctrl->next;
    }
    alock.unLock();
}

int ThreadPoolSessControl::SendSignal(int ctrl_index, int signal)
{
    Assert(signal != SIGHUP);
    int status = ESRCH;

    if (!IsValidCtrlIndex(ctrl_index)) {
        return ESRCH;
    }

    knl_sess_control* ctrl = &m_base[ctrl_index - m_maxReserveSessionCount];
    volatile sig_atomic_t* plock = &ctrl->lock;
    sig_atomic_t val;
    do {
        if (*plock == 0) {
            /*  perform an atomic compare and swap. */
            val = __sync_val_compare_and_swap(plock, 0, 1);
            if (val == 0) {
                if (ctrl->sess != NULL) {
                    if (ctrl->sess->status == KNL_SESS_ATTACH) {
                        status = gs_signal_send(ctrl->sess->attachPid, signal);
                    } else if (ctrl->sess->status == KNL_SESS_DETACH) {
                        switch (signal) {
                            case SIGTERM:
                                ctrl->sess->status = KNL_SESS_CLOSE;
                                status = 0;
                                break;
                            default:
                                break;
                        }
                    } else {
                        status = ESRCH;
                    }
                }
                /*  restore the value. */
                ctrl->lock = 0;
                break;
            }
        }
        pg_usleep(100);
    } while (true);

    return status;
}

void ThreadPoolSessControl::SendProcSignal(int ctrl_index, ProcSignalReason reason, uint64 query_id)
{
    if (!IsValidCtrlIndex(ctrl_index)) {
        return;
    }

    knl_sess_control* ctrl = &m_base[ctrl_index - m_maxReserveSessionCount];

    volatile sig_atomic_t* plock = &ctrl->lock;
    sig_atomic_t val;
    do {
        if (*plock == 0) {
            /*  perform an atomic compare and swap. */
            val = __sync_val_compare_and_swap(plock, 0, 1);
            if (val == 0) {
                if (ctrl->sess != NULL) {
                    switch (reason) {
                        case PROCSIG_EXECUTOR_FLAG: {
                            if (IS_PGXC_DATANODE && ctrl->sess->debug_query_id == query_id) {
                                ctrl->sess->exec_cxt.executor_stop_flag = true;
                            }
                            break;
                        }
                        default: {
                            Assert(0);
                            ctrl->lock = 0;
                            ereport(ERROR,
                                (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Unexpected receive proc signal.")));
                        }
                    }
                }
                /*  restore the value. */
                ctrl->lock = 0;
                break;
            }
        }
        pg_usleep(100);
    } while (true);
}

int ThreadPoolSessControl::CountDBSessions(Oid dbId)
{
    AutoMutexLock alock(&m_sessCtrlock);
    alock.lock();

    knl_sess_control* ctrl = m_activelist;
    int count = 0;

    while (ctrl != NULL) {
        if (ctrl->sess->proc_cxt.MyDatabaseId == dbId) {
            count++;
            if (strcmp(ctrl->sess->attr.attr_common.application_name, "WDRXdb") == 0) {
                ThreadPoolSessControl::SendSignal(ctrl->idx, SIGTERM);
            }
        }

        ctrl = ctrl->next;
    }

    alock.unLock();

    return count;
}

void ThreadPoolSessControl::SigHupHandler()
{
    AutoMutexLock alock(&m_sessCtrlock);
    alock.lock();

    knl_sess_control* ctrl = m_activelist;

    while (ctrl != NULL) {
        ctrl->sess->sig_cxt.got_SIGHUP = true;
        ctrl = ctrl->next;
    }
    alock.unLock();
}

void ThreadPoolSessControl::HandlePoolerReload()
{
    if (IS_PGXC_DATANODE) {
        return;
    }

    AutoMutexLock alock(&m_sessCtrlock);
    alock.lock();

    knl_sess_control* ctrl = m_activelist;

    while (ctrl != NULL) {
        ctrl->sess->sig_cxt.got_PoolReload = true;
        ctrl->sess->sig_cxt.cp_PoolReload = true;
        ctrl = ctrl->next;
    }
    alock.unLock();
}

void ThreadPoolSessControl::createSessTempSmallCxtGroup(knl_session_context* sess, SessionMemoryDetailPad* data) const
{
    /* create temp context for grouping all small size memory context */
    errno_t rc;
    SessionMemoryDetail* sessionMemoryDetail = (SessionMemoryDetail*)palloc0(sizeof(SessionMemoryDetail));
    if (data->sessionMemoryDetail == NULL) {
        data->sessionMemoryDetail = sessionMemoryDetail;
    } else {
        sessionMemoryDetail->next = data->sessionMemoryDetail->next;
        data->sessionMemoryDetail->next = sessionMemoryDetail;
    }

    rc = strncpy_s(sessionMemoryDetail->contextName,
        MEMORY_CONTEXT_NAME_LEN,
        "TempSmallContextGroup",
        MEMORY_CONTEXT_NAME_LEN - 1);
    securec_check(rc, "\0", "\0");
    sessionMemoryDetail->contextName[MEMORY_CONTEXT_NAME_LEN - 1] = '\0';

    sessionMemoryDetail->sessId = sess->session_id;
    sessionMemoryDetail->threadId = sess->attachPid;
    sessionMemoryDetail->sessStartTime = timestamptz_to_time_t(sess->proc_cxt.MyProcPort->SessionStartTime);
    sessionMemoryDetail->level = 0;
    sessionMemoryDetail->totalSize = 0;
    sessionMemoryDetail->freeSize = 0;
    sessionMemoryDetail->usedSize = 0;

    data->nelements++;
}

void ThreadPoolSessControl::calculateSessMemCxtStats(
    knl_session_context* sess, const MemoryContext context, SessionMemoryDetailPad* data, int groupcnt)
{
    AllocSetContext* set = (AllocSetContext*)context;
    SessionMemoryDetail* sessionMemoryDetail = NULL;
    errno_t rc = 0;

    /* Add it into small contxt group */
    if (sess != NULL && groupcnt >= 0 && set->totalSpace <= ALLOCSET_DEFAULT_INITSIZE) {
        sessionMemoryDetail = data->sessionMemoryDetail;

        sessionMemoryDetail->totalSize += set->totalSpace;
        sessionMemoryDetail->freeSize += set->freeSpace;

        if (sessionMemoryDetail->totalSize < sessionMemoryDetail->freeSize) {
            sessionMemoryDetail->totalSize = sessionMemoryDetail->freeSize;
        }

        /* obviously, total space must larger than free space */
        Assert(sessionMemoryDetail->totalSize >= sessionMemoryDetail->freeSize);

        /* memory context number is recorded in usedSize */
        sessionMemoryDetail->usedSize++;

        return;
    }

    sessionMemoryDetail = (SessionMemoryDetail*)palloc0(sizeof(SessionMemoryDetail));
    sessionMemoryDetail->next = data->sessionMemoryDetail->next;
    data->sessionMemoryDetail->next = sessionMemoryDetail;
    rc = strncpy_s(
            sessionMemoryDetail->contextName, MEMORY_CONTEXT_NAME_LEN, context->name, MEMORY_CONTEXT_NAME_LEN - 1);
    securec_check(rc, "\0", "\0");
    sessionMemoryDetail->contextName[MEMORY_CONTEXT_NAME_LEN - 1] = '\0';

    sessionMemoryDetail->level = context->level;
    if (context->level > 0 && context->parent != NULL) {
        rc = strncpy_s(sessionMemoryDetail->parent,
                       MEMORY_CONTEXT_NAME_LEN,
                       context->parent->name,
                       MEMORY_CONTEXT_NAME_LEN - 1);
        securec_check(rc, "\0", "\0");
        sessionMemoryDetail->parent[MEMORY_CONTEXT_NAME_LEN - 1] = '\0';
    }
    sessionMemoryDetail->totalSize = set->totalSpace;
    sessionMemoryDetail->freeSize = set->freeSpace;
    if (sessionMemoryDetail->totalSize < sessionMemoryDetail->freeSize) {
        sessionMemoryDetail->totalSize = sessionMemoryDetail->freeSize;
    }

    /* obviously, total space must larger than free space */
    Assert(sessionMemoryDetail->totalSize >= sessionMemoryDetail->freeSize);
    sessionMemoryDetail->usedSize = sessionMemoryDetail->totalSize - sessionMemoryDetail->freeSize;

    if (sess != NULL) {
        sessionMemoryDetail->sessId = sess->session_id;
        sessionMemoryDetail->threadId = sess->attachPid;
        sessionMemoryDetail->sessStartTime = timestamptz_to_time_t(sess->proc_cxt.MyProcPort->SessionStartTime);
    }
    data->nelements++;
}

void ThreadPoolSessControl::recursiveSessMemCxt(
    knl_session_context* sess, const MemoryContext context, SessionMemoryDetailPad* data, int groupcnt)
{
    MemoryContext child;

    /* calculate MemoryContext Stats */
    calculateSessMemCxtStats(sess, context, data, groupcnt);

    /* recursive MemoryContext's child */
    for (child = context->firstchild; child != NULL; child = child->nextchild) {
        recursiveSessMemCxt(sess, child, data, groupcnt);
    }
}

void ThreadPoolSessControl::getSessMemCxts(SessionMemoryDetailPad* data)
{
    HOLD_INTERRUPTS();

    AutoMutexLock alock(&m_sessCtrlock);
    alock.lock();
    knl_sess_control* sess_ctrl = m_activelist;
    while (sess_ctrl != NULL) {
        /* memory already reach the max size, just return */
        if (data->nelements == (uint32)TOTAL_MEMORY_CONTEXT_CHILD_NUM) {
            break;
        }

        if (sess_ctrl->sess) {
            createSessTempSmallCxtGroup(sess_ctrl->sess, data);
            recursiveSessMemCxt(sess_ctrl->sess, sess_ctrl->sess->top_mem_cxt, data, data->nelements - 1);
        }
        sess_ctrl = sess_ctrl->next;
    }
    alock.unLock();

    RESUME_INTERRUPTS();
}

SessionMemoryDetail* ThreadPoolSessControl::getSessionMemoryDetail(uint32* num)
{
    SessionMemoryDetailPad* data = NULL;
    SessionMemoryDetail* return_detail_array = NULL;

    data = (SessionMemoryDetailPad*)palloc0(sizeof(SessionMemoryDetailPad));
    *num = 0;

    /* collect all the Memory Context status,put in data */
    getSessMemCxts(data);

    if (data->nelements > 0) {
        *num = data->nelements;
        return_detail_array = data->sessionMemoryDetail;
    }

    return return_detail_array;
}

knl_session_context* ThreadPoolSessControl::GetSessionByIdx(int idx)
{
    if (IsValidCtrlIndex(idx)) {
        return m_base[idx - m_maxReserveSessionCount].sess;
    } else {
        return NULL;
    }
}
