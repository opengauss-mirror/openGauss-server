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
#include "catalog/pg_authid.h"
#include "catalog/pg_collation.h"
#include "commands/prepare.h"
#include "commands/user.h"
#include "gssignal/gs_signal.h"
#include "lib/dllist.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "pgstat.h"
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
#include "utils/acl.h"
#include "executor/executor.h"

#include "communication/commproxy_interface.h"
#ifdef MEMORY_CONTEXT_TRACK
#include "memory_func.h"
#endif
#include "utils/mem_snapshot.h"

ThreadPoolSessControl::ThreadPoolSessControl(MemoryContext context)
{
    AutoContextSwitch acontext(context);
    m_context = context;
    pthread_mutex_init(&m_sessCtrlock, NULL);
    m_sessionId = 0;
    m_activeSessionCount = 0;
    m_maxActiveSessionCount = GLOBAL_MAX_SESSION_NUM;
    m_maxReserveSessionCount = GLOBAL_RESERVE_SESSION_NUM;
    DLInitList(&m_activelist);
    DLInitList(&m_freelist);
    m_base = (knl_sess_control*)palloc0(sizeof(knl_sess_control) * m_maxActiveSessionCount);
    for (int i = 0; i < m_maxActiveSessionCount; i++) {
        m_base[i].idx = (m_maxReserveSessionCount + i);
        m_base[i].lock = 0;
        DLInitElem(&m_base[i].elem, &m_base[i]);
        DLAddHead(&m_freelist, &m_base[i].elem);
    }
}

ThreadPoolSessControl::~ThreadPoolSessControl()
{
    pfree_ext(m_base);
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

    MemoryContext old_cxt = MemoryContextSwitchTo(sc->mcxt_group->GetMemCxtGroup(MEMORY_CONTEXT_DEFAULT));
    sc->proc_cxt.MyProcPort = (Port*)palloc0(sizeof(Port));
    MemoryContextSwitchTo(old_cxt);
    int rc = memcpy_s(sc->proc_cxt.MyProcPort, sizeof(Port), port, sizeof(Port));
    securec_check(rc, "\0", "\0");

    ereport(DEBUG4, (errmsg("CreateSession fd:[%d] to dispatch.", port->sock)));
    if (AllocateSlot(sc)) {
        sc->stat_cxt.trackedBytes += u_sess->stat_cxt.trackedBytes;
        sc->stat_cxt.trackedMemChunks += u_sess->stat_cxt.trackedMemChunks;
        u_sess->stat_cxt.trackedBytes = 0;
        u_sess->stat_cxt.trackedMemChunks = 0;
        return sc;
    } else {
        u_sess->stat_cxt.trackedBytes += sc->stat_cxt.trackedBytes;
        u_sess->stat_cxt.trackedMemChunks += sc->stat_cxt.trackedMemChunks;
        // sc->top_mem_cxt has been sealed in create_session_context
        MemoryContextUnSeal(sc->top_mem_cxt);
        MemoryContextDeleteChildren(sc->top_mem_cxt, NULL);
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
    Assert(DLListLength(&m_freelist) != 0);

    /* remove from free list */
    knl_sess_control* ctrl = (knl_sess_control*)DLE_VAL(DLRemHead(&m_freelist));
    ctrl->sess = sc;
    sc->session_ctr_index = ctrl->idx;
    DLAddHead(&m_activelist, &ctrl->elem);
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

    knl_sess_control *ctrl = &m_base[ctrl_index - m_maxReserveSessionCount];
    Assert(ctrl->elem.dle_list == &m_activelist);

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
                DLRemove(&ctrl->elem);
                DLAddHead(&m_freelist, &ctrl->elem);
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
    knl_sess_control* ctrl = NULL;
    Dlelem* elem = DLGetHead(&m_activelist);
    while (elem != NULL) {
        ctrl= (knl_sess_control*)DLE_VAL(elem);
        ctrl->sess->status = KNL_SESS_CLOSE;
        CloseClientSocket(ctrl->sess, false);
        elem = DLGetSucc(elem);
    }
    alock.unLock();
    ereport(LOG, (errmodule(MOD_THREAD_POOL),
                    errmsg("pmState:%d, mark all threadpool sessions closed.", pmState)));
}

void ThreadPoolSessControl::CheckPermissionForSendSignal(knl_session_context* sess, sig_atomic_t* lock)
{
    /* User id is invalid only when sometimes dealing with cancel signal. Because that permission is ensured
      by random cancel key, so we don't have to check the permission again. */
    if (!OidIsValid(u_sess->misc_cxt.CurrentUserId)) {
        return;
    }

    /* Only users with sysadmin privilege or the member of gs_role_signal_backend role
     * or the owner of the database or user himself have the permission to send singal. */
    bool role_signal_backend_permission = is_member_of_role(GetUserId(), DEFAULT_ROLE_SIGNAL_BACKENDID) &&
        (sess->proc_cxt.MyRoleId != BOOTSTRAP_SUPERUSERID && !is_role_persistence(sess->proc_cxt.MyRoleId));
    if (!superuser() && !pg_database_ownercheck(sess->proc_cxt.MyDatabaseId, u_sess->misc_cxt.CurrentUserId) &&
        !role_signal_backend_permission) {
        if (sess->proc_cxt.MyRoleId != GetUserId()) {
            *lock = 0;
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    (errmsg("must have sysadmin privilege or a member of the gs_role_signal_backend role or the "
                    "owner of the database or the same user to terminate other backend"))));
        }
    }
}

void ThreadPoolSessControl::releaseLockIfNecessary()
{
    if (unlikely(t_thrd.sig_cxt.cur_ctrl_index != 0)) {
        knl_sess_control* ctrl = &m_base[t_thrd.sig_cxt.cur_ctrl_index - m_maxReserveSessionCount];
        volatile sig_atomic_t plock = ctrl->lock;
        if (plock != 0) {
            plock = 0;
        }
        t_thrd.sig_cxt.cur_ctrl_index = 0;
    }
}

int ThreadPoolSessControl::SendSignal(int ctrl_index, int signal)
{
    Assert(signal != SIGHUP);
    int status = ESRCH;

    if (!IsValidCtrlIndex(ctrl_index)) {
        return ESRCH;
    }

    knl_sess_control* ctrl = &m_base[ctrl_index - m_maxReserveSessionCount];
    t_thrd.sig_cxt.cur_ctrl_index = ctrl_index;
    volatile sig_atomic_t* plock = &ctrl->lock;
    sig_atomic_t val;
    do {
        if (*plock == 0) {
            /*  perform an atomic compare and swap. */
            val = __sync_val_compare_and_swap(plock, 0, 1);
            if (val == 0) {
                knl_session_context* sess = ctrl->sess;
                /* Session may be NULL when the session exits during the clean connection process.
                   We do nothing if the session is NULL */
                if (sess == NULL) {
                    /* restore the value. */
                    ctrl->lock = 0;
                    status = ESRCH;
                    break;
                }
                /* Check user permission, and we dont have user id for cancel request. */
                CheckPermissionForSendSignal(sess, (sig_atomic_t*)plock);
                if (sess->status == KNL_SESS_ATTACH) {
                    t_thrd.sig_cxt.gs_sigale_check_type = SIGNAL_CHECK_SESS_KEY;
                    t_thrd.sig_cxt.session_id = sess->session_id;
                    status = gs_signal_send(sess->attachPid, signal);
                    t_thrd.sig_cxt.gs_sigale_check_type = SIGNAL_CHECK_NONE;
                    t_thrd.sig_cxt.session_id = 0;
                } else if (sess->status == KNL_SESS_DETACH) {
                    switch (signal) {
                        case SIGTERM:
                            sess->status = KNL_SESS_CLOSE;
                            CloseClientSocket(sess, false);
                            status = 0;
                            break;
                        default:
                            break;
                    }
                } else {
                    status = ESRCH;
                }
                /*  restore the value. */
                ctrl->lock = 0;
                break;
            }
        }
        pg_usleep(100);
    } while (true);
    t_thrd.sig_cxt.cur_ctrl_index = 0;

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
                                ctrl->sess->exec_cxt.executorStopFlag = true;
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

    knl_sess_control* ctrl = NULL;
    Dlelem* elem = DLGetHead(&m_activelist);
    int count = 0;

    while (elem != NULL) {
        ctrl= (knl_sess_control*)DLE_VAL(elem);
        if (ctrl->sess->proc_cxt.MyDatabaseId == dbId) {
            count++;
            if (strcmp(ctrl->sess->attr.attr_common.application_name, "WDRXdb") == 0) {
                ThreadPoolSessControl::SendSignal(ctrl->idx, SIGTERM);
                ThreadPoolSessControl::SendSignal(ctrl->idx, SIGUSR2);
            }
        }

        elem = DLGetSucc(elem);
    }

    alock.unLock();

    return count;
}

bool ThreadPoolSessControl::ValidDBoidAndUseroid(Oid dbOid, Oid userOid, knl_sess_control* ctrl)
{
    /*
     * Thread are 3 situation in CLEAN CONNECTION:
     * 1. Only database, for example: CLEAN CONNECTION TO ALL FORCE FOR DATABASE xxx;
     * 2. Only user, for example: CLEAN CONNECTION TO ALL FORCE TO USER xxx;
     * 3. Both database and user, for example: CLEAN CONNECTION TO ALL FORCE FOR DATABASE xxx TO USER xxx;
     */
    if (((dbOid != InvalidOid) && (userOid == InvalidOid) && (ctrl->sess->proc_cxt.MyDatabaseId == dbOid))
        || ((dbOid == InvalidOid) && (userOid != InvalidOid) && (ctrl->sess->proc_cxt.MyRoleId == userOid))
        || ((ctrl->sess->proc_cxt.MyDatabaseId == dbOid) && (ctrl->sess->proc_cxt.MyRoleId == userOid))) {
        return true;
    }
    return false;
}

int ThreadPoolSessControl::CountDBSessionsNotCleaned(Oid dbOid, Oid userOid)
{
    if ((dbOid == InvalidOid) && (userOid == InvalidOid)) {
        ereport(WARNING,
            (errmsg("DB oid and user oid are all Invalid (may be NULL). Shut down clean activite sessions.")));
        return 0;
    }
    AutoMutexLock alock(&m_sessCtrlock);
    alock.lock();

    int count = 0;
    knl_sess_control* ctrl = NULL;
    Dlelem* elem = DLGetHead(&m_activelist);

    while (elem != NULL) {
        ctrl= (knl_sess_control*)DLE_VAL(elem);
        if (ValidDBoidAndUseroid(dbOid, userOid, ctrl)) {
            int status = ThreadPoolSessControl::SendSignal(ctrl->idx, 0);
            if (status == 0) {
                /* Termination not done yet */
                count++;
            }
        }
        elem = DLGetSucc(elem);
    }

    alock.unLock();
    return count;
}

int ThreadPoolSessControl::CleanDBSessions(Oid dbOid, Oid userOid)
{
    if ((dbOid == InvalidOid) && (userOid == InvalidOid)) {
        ereport(WARNING,
            (errmsg("DB oid and user oid are all Invalid (may be NULL). Shut down clean activite sessions.")));
        return 0;
    }
    AutoMutexLock alock(&m_sessCtrlock);
    alock.lock();

    int count = 0;
    knl_sess_control* ctrl = NULL;
    Dlelem* elem = DLGetHead(&m_activelist);

    while (elem != NULL) {
        ctrl= (knl_sess_control*)DLE_VAL(elem);
        if (ValidDBoidAndUseroid(dbOid, userOid, ctrl)) {
            count++;
            ThreadPoolSessControl::SendSignal(ctrl->idx, SIGTERM);
            ThreadPoolSessControl::SendSignal(ctrl->idx, SIGUSR2);
        }
        elem = DLGetSucc(elem);
    }

    alock.unLock();
    return count;
}

void ThreadPoolSessControl::SigHupHandler()
{
    AutoMutexLock alock(&m_sessCtrlock);
    alock.lock();

    knl_sess_control* ctrl = NULL;
    Dlelem* elem = DLGetHead(&m_activelist);
    while (elem != NULL) {
        ctrl= (knl_sess_control*)DLE_VAL(elem);
        ctrl->sess->sig_cxt.got_SIGHUP = true;
        elem = DLGetSucc(elem);
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

    knl_sess_control* ctrl = NULL;
    Dlelem* elem = DLGetHead(&m_activelist);
    while (elem != NULL) {
        ctrl= (knl_sess_control*)DLE_VAL(elem);
        /* we have already send got_pool_reload to threads */
        ctrl->sess->sig_cxt.got_pool_reload = true;
        ctrl->sess->sig_cxt.cp_PoolReload = true;
        elem = DLGetSucc(elem);
    }
    alock.unLock();
}

void ThreadPoolSessControl::calculateSessMemCxtStats(
    knl_session_context* sess, const MemoryContext context, Tuplestorestate* tupStore, TupleDesc tupDesc)
{
    AllocSetContext* set = (AllocSetContext*)context;

    char sessId[SESSION_ID_LEN] = {0};
    ThreadId threadId = 0;
    pg_time_t sessStartTime = 0;
    uint64 sessionId = 0;

    if (sess != NULL) {
        sessionId = sess->session_id;
        threadId = sess->attachPid;
        sessStartTime = timestamptz_to_time_t(sess->proc_cxt.MyProcPort->SessionStartTime);
    }
    getSessionID(sessId, sessStartTime, sessionId);

    /* build one tuple and save it in tuplestore. */
    Datum values[NUM_SESSION_MEMORY_DETAIL_ELEM] = {0};
    bool nulls[NUM_SESSION_MEMORY_DETAIL_ELEM] = {false};

    values[0] = CStringGetTextDatum(sessId);
    values[1] = Int64GetDatum(threadId);

    values[2] = CStringGetTextDatum(pstrdup(context->name));
    values[3] = Int16GetDatum(context->level);
    if (context->level > 0 && context->parent != NULL)
        values[4] = CStringGetTextDatum(pstrdup(context->parent->name));
    else
        nulls[4] = true;
    values[5] = Int64GetDatum(set->totalSpace);
    values[6] = Int64GetDatum(set->freeSpace);
    values[7] = Int64GetDatum(set->totalSpace - set->freeSpace);

    tuplestore_putvalues(tupStore, tupDesc, values, nulls);
}

void ThreadPoolSessControl::recursiveSessMemCxt(
    knl_session_context* sess, const MemoryContext context, Tuplestorestate* tupStore, TupleDesc tupDesc)
{
    /* calculate MemoryContext Stats */
    calculateSessMemCxtStats(sess, context, tupStore, tupDesc);

    /* recursive MemoryContext's child */
    for (MemoryContext child = context->firstchild; child != NULL; child = child->nextchild) {
        recursiveSessMemCxt(sess, child, tupStore, tupDesc);
    }
}

void ThreadPoolSessControl::getSessionMemoryDetail(Tuplestorestate* tupStore,
    TupleDesc tupDesc, knl_sess_control** sess)
{
    AutoMutexLock alock(&m_sessCtrlock);
    knl_sess_control* ctrl = NULL;
    Dlelem* elem = NULL;

    PG_TRY();
    {
        HOLD_INTERRUPTS();
        alock.lock();

        /* collect all the Memory Context status, put in data */
        elem = DLGetHead(&m_activelist);

        while (elem != NULL) {
            ctrl = (knl_sess_control*)DLE_VAL(elem);
            *sess = ctrl;
            if (ctrl->sess) {
                (void)syscalllockAcquire(&ctrl->sess->utils_cxt.deleMemContextMutex);
                recursiveSessMemCxt(ctrl->sess, ctrl->sess->top_mem_cxt, tupStore, tupDesc);
                (void)syscalllockRelease(&ctrl->sess->utils_cxt.deleMemContextMutex);
            }
            elem = DLGetSucc(elem);
        }
        alock.unLock();

        RESUME_INTERRUPTS();
    }
    PG_CATCH();
    {
        if (*sess != NULL) {
            ctrl = *sess;
            (void)syscalllockRelease(&ctrl->sess->utils_cxt.deleMemContextMutex);
        }
        alock.unLock();
        PG_RE_THROW();
    }
    PG_END_TRY();
}

void ThreadPoolSessControl::calculateClientInfo(
    knl_session_context* sess, Tuplestorestate* tupStore, TupleDesc tupDesc)
{
    /* build one tuple and save it in tuplestore. */
    const int COLUMN_NUM = 2;
    Datum values[COLUMN_NUM] = {0};
    bool nulls[COLUMN_NUM] = {false};
 
    values[0] = Int64GetDatum(sess->session_id);
    if (sess->plsql_cxt.client_info != NULL) {
        values[1] = CStringGetTextDatum(sess->plsql_cxt.client_info);
    } else {
        nulls[1] = true;
    }
    tuplestore_putvalues(tupStore, tupDesc, values, nulls);
}
 
void ThreadPoolSessControl::getSessionClientInfo(Tuplestorestate* tupStore, TupleDesc tupDesc)
{
    AutoMutexLock alock(&m_sessCtrlock);
    knl_sess_control* ctrl = NULL;
    Dlelem* elem = NULL;
    knl_sess_control* sess = NULL;
 
    PG_TRY();
    {
        HOLD_INTERRUPTS();
        alock.lock();
 
        /* collect all the Memory Context status, put in data */
        elem = DLGetHead(&m_activelist);
 
        while (elem != NULL) {
            ctrl = (knl_sess_control*)DLE_VAL(elem);
            sess = ctrl;
            if (ctrl->sess) {
                (void)syscalllockAcquire(&ctrl->sess->plsql_cxt.client_info_lock);
                calculateClientInfo(ctrl->sess, tupStore, tupDesc);
                (void)syscalllockRelease(&ctrl->sess->plsql_cxt.client_info_lock);
            }
            elem = DLGetSucc(elem);
        }
        alock.unLock();
        sess = NULL;
 
        RESUME_INTERRUPTS();
    }
    PG_CATCH();
    {
        if (sess != NULL) {
            ctrl = sess;
            (void)syscalllockRelease(&ctrl->sess->plsql_cxt.client_info_lock);
        }
        alock.unLock();
        PG_RE_THROW();
    }
    PG_END_TRY();
}

void ThreadPoolSessControl::getSessionMemoryContextInfo(const char* ctx_name,
    StringInfoDataHuge* buf, knl_sess_control** sess)
{
#ifdef MEMORY_CONTEXT_TRACK
    AutoMutexLock alock(&m_sessCtrlock);
    knl_sess_control* ctrl = NULL;
    Dlelem* elem = NULL;

    PG_TRY();
    {
        HOLD_INTERRUPTS();
        alock.lock();

        /* collect all the Memory Context status, put in data */
        elem = DLGetHead(&m_activelist);

        while (elem != NULL) {
            ctrl = (knl_sess_control*)DLE_VAL(elem);
            *sess = ctrl;
            if (ctrl->sess) {
                (void)syscalllockAcquire(&ctrl->sess->utils_cxt.deleMemContextMutex);
                gs_recursive_unshared_memory_context(ctrl->sess->top_mem_cxt, ctx_name, buf);
                (void)syscalllockRelease(&ctrl->sess->utils_cxt.deleMemContextMutex);
            }
            elem = DLGetSucc(elem);
        }
        alock.unLock();

        RESUME_INTERRUPTS();
    }
    PG_CATCH();
    {
        if (*sess != NULL) {
            ctrl = *sess;
            (void)syscalllockRelease(&ctrl->sess->utils_cxt.deleMemContextMutex);
        }
        alock.unLock();
        PG_RE_THROW();
    }
    PG_END_TRY();
#endif
}

void ThreadPoolSessControl::getSessionMemoryContextSpace(StringInfoDataHuge* buf, knl_sess_control** sess)
{
    AutoMutexLock alock(&m_sessCtrlock);
    knl_sess_control* ctrl = NULL;
    Dlelem* elem = NULL;

    PG_TRY();
    {
        HOLD_INTERRUPTS();
        alock.lock();

        /* collect all the Memory Context status, put in data */
        elem = DLGetHead(&m_activelist);

        while (elem != NULL) {
            ctrl = (knl_sess_control*)DLE_VAL(elem);
            *sess = ctrl;
            if (ctrl->sess) {
                (void)syscalllockAcquire(&ctrl->sess->utils_cxt.deleMemContextMutex);
                RecursiveUnSharedMemoryContext(ctrl->sess->top_mem_cxt, buf);
                (void)syscalllockRelease(&ctrl->sess->utils_cxt.deleMemContextMutex);
            }
            elem = DLGetSucc(elem);
        }
        alock.unLock();

        RESUME_INTERRUPTS();
    }
    PG_CATCH();
    {
        if (*sess != NULL) {
            ctrl = *sess;
            (void)syscalllockRelease(&ctrl->sess->utils_cxt.deleMemContextMutex);
        }
        alock.unLock();
        PG_RE_THROW();
    }
    PG_END_TRY();
}

bool ThreadPoolSessControl::CheckSessionCanTerminate(const Oid roleId)
{
    if (superuser_arg(roleId) || systemDBA_arg(roleId)) {
        return false;
    }

    if (isMonitoradmin(roleId)) {
        return false;
    }

    return true;
}
SessMemoryUsage* ThreadPoolSessControl::getSessionMemoryUsage(int* num)
{
    AutoMutexLock alock(&m_sessCtrlock);
    knl_sess_control* ctrl = NULL;
    Dlelem* elem = NULL;

    HOLD_INTERRUPTS();
    alock.lock();

    SessMemoryUsage* result = (SessMemoryUsage*)palloc(m_activeSessionCount * sizeof(SessMemoryUsage));
    int index = 0;

    elem = DLGetHead(&m_activelist);
    while (elem != NULL) {
        ctrl = (knl_sess_control*)DLE_VAL(elem);
        knl_session_context* sess = ctrl->sess;
        if (sess && CheckSessionCanTerminate(sess->proc_cxt.MyRoleId)) {
            result[index].sessid = sess->session_id;
            result[index].usedSize = sess->stat_cxt.trackedBytes;
            /* BackendStatusArray may not assigned while new connection init */
            if (t_thrd.shemem_ptr_cxt.BackendStatusArray != NULL) {
                result[index].state = (int)t_thrd.shemem_ptr_cxt.BackendStatusArray[sess->session_ctr_index].st_state;
            } else {
                result[index].state = STATE_IDLE;
            }
            index++;
        }
        elem = DLGetSucc(elem);
    }

    alock.unLock();
    RESUME_INTERRUPTS();

    *num = index;
    return result;
}

knl_session_context* ThreadPoolSessControl::GetSessionByIdx(int idx)
{
    if (IsValidCtrlIndex(idx)) {
        return m_base[idx - m_maxReserveSessionCount].sess;
    } else {
        return NULL;
    }
}

int ThreadPoolSessControl::FindCtrlIdxBySessId(uint64 id)
{
    int cidx = 0;
    for (cidx = 0; cidx < m_maxActiveSessionCount; cidx++) {
        if (m_base[cidx].sess != NULL && m_base[cidx].sess->session_id == id) {
            return cidx + m_maxReserveSessionCount;
        }
    }

    ereport(LOG, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid session id")));

    return -1;
}
void ThreadPoolSessControl::CheckSessionTimeout()
{
    AutoMutexLock alock(&m_sessCtrlock);
    alock.lock();
    int cidx;
    TimestampTz now = GetCurrentTimestamp();
    for (cidx = 0; cidx < m_maxActiveSessionCount; cidx++) {
        knl_sess_control* ctrl = &m_base[cidx];
        knl_session_context* sess = ctrl->sess;
        if (sess != NULL && sess->attr.attr_common.SessionTimeout != 0 &&
            sess->storage_cxt.session_timeout_active) {
            if (now >= sess->storage_cxt.session_fin_time && sess->attr.attr_common.SessionTimeoutCount < 10) {
#ifdef HAVE_INT64_TIMESTAMP
                elog(LOG, "close session : %lu for %d times due to session timeout : %d, max finish time is %ld. But now is:%ld",
                        sess->session_id, sess->attr.attr_common.SessionTimeoutCount + 1,
                        sess->attr.attr_common.SessionTimeout, sess->storage_cxt.session_fin_time, now);
#else
                elog(LOG, "close session : %lu for %d times due to session timeout : %d, max finish time is %lf. But now is:%lf",
                        sess->session_id, sess->attr.attr_common.SessionTimeoutCount + 1,
                        sess->attr.attr_common.SessionTimeout, sess->storage_cxt.session_fin_time, now);
#endif
                sess->attr.attr_common.SessionTimeoutCount++;
                CloseClientSocket(sess, false);
            }
        }
    }
    alock.unLock();
}

#ifndef ENABLE_MULTIPLE_NODES
void ThreadPoolSessControl::CheckIdleInTransactionSessionTimeout()
{
    AutoMutexLock alock(&m_sessCtrlock);
    alock.lock();
    int cidx;
    TimestampTz now = GetCurrentTimestamp();
    for (cidx = 0; cidx < m_maxActiveSessionCount; cidx++) {
        knl_sess_control* ctrl = &m_base[cidx];
        knl_session_context* sess = ctrl->sess;
        if (sess != NULL && sess->attr.attr_common.IdleInTransactionSessionTimeout > 0 &&
            sess->storage_cxt.idle_in_transaction_session_timeout_active) {
            if (now >= sess->storage_cxt.idle_in_transaction_session_fin_time &&
                sess->attr.attr_common.IdleInTransactionSessionTimeoutCount < 10) {
#ifdef HAVE_INT64_TIMESTAMP
                elog(LOG, "close session : %lu for %d times due to idle in transaction timeout : %d, max finish time is %ld. But now is:%ld",
                        sess->session_id, sess->attr.attr_common.IdleInTransactionSessionTimeoutCount + 1,
                        sess->attr.attr_common.IdleInTransactionSessionTimeout, sess->storage_cxt.idle_in_transaction_session_fin_time, now);
#else
                elog(LOG, "close session : %lu for %d times due to idle in transaction session timeout : %d, max finish time is %lf. But now is:%lf",
                        sess->session_id, sess->attr.attr_common.IdleInTransactionSessionTimeoutCount + 1,
                        sess->attr.attr_common.IdleInTransactionSessionTimeout, sess->storage_cxt.idle_in_transaction_session_fin_time, now);
#endif
                sess->attr.attr_common.IdleInTransactionSessionTimeout++;
                CloseClientSocket(sess, false);
            }
        }
    }
    alock.unLock();
}
#endif

TransactionId ThreadPoolSessControl::ListAllSessionGttFrozenxids(int maxSize,
    ThreadId *pids, TransactionId *xids, int *n)
{
    TransactionId result = InvalidTransactionId;
    int           i = 0;

    if (g_instance.attr.attr_storage.max_active_gtt <= 0) {
        return 0;
    }

    if (maxSize > 0) {
        Assert(pids);
        Assert(xids);
        Assert(n);
        *n = 0;
    }

    if (RecoveryInProgress() || SSIsServerModeReadOnly()) {
        return InvalidTransactionId;
    }

    AutoMutexLock alock(&m_sessCtrlock);
    alock.lock();
    knl_sess_control *ctl = nullptr;
    knl_session_context *session = nullptr;
    Dlelem* elem = DLGetHead(&m_activelist);
    while (elem != nullptr) {
        ctl = (knl_sess_control*)DLE_VAL(elem);
        session = ctl->sess;
        if (session->proc_cxt.MyDatabaseId == u_sess->proc_cxt.MyDatabaseId &&
            TransactionIdIsNormal(session->gtt_ctx.gtt_session_frozenxid)) {
            if (result == InvalidTransactionId) {
                result = session->gtt_ctx.gtt_session_frozenxid;
            } else if (TransactionIdPrecedes(session->gtt_ctx.gtt_session_frozenxid, result)) {
                result = session->gtt_ctx.gtt_session_frozenxid;
            }

            if (maxSize > 0) {
                pids[i] = session->attachPid;
                xids[i] = session->gtt_ctx.gtt_session_frozenxid;
                i++;
            }
        }
        elem = DLGetSucc(elem);
    }
    alock.unLock();

    if (maxSize > 0) {
        *n = i;
    }
    return result;
}

bool ThreadPoolSessControl::IsActiveListEmpty()
{
    AutoMutexLock alock(&m_sessCtrlock);
    alock.lock();
    bool res = (m_activelist.dll_len == 0);
    alock.unLock();
    return res;
}

void ThreadPoolSessControl::GetSessionPreparedStatements(Tuplestorestate* tupStore, TupleDesc tupDesc, uint64 sessionId)
{
    AutoMutexLock alock(&m_sessCtrlock);
    knl_sess_control* ctrl = NULL;
    knl_session_context* session = NULL;

    PG_TRY();
    {
        HOLD_INTERRUPTS();
        alock.lock();

        Dlelem* elem = DLGetHead(&m_activelist);
        while (elem != NULL) {
            ctrl = (knl_sess_control*)DLE_VAL(elem);
            session = ctrl->sess;
                       
            if ((session->session_id == sessionId || sessionId == 0) &&
                (session->misc_cxt.CurrentUserName != NULL)) {
                char userName[NAMEDATALEN];
                errno_t rc = memset_s(userName, NAMEDATALEN, '\0', sizeof(userName));
                securec_check(rc, "\0", "\0");
                rc = strcpy_s(userName, NAMEDATALEN, session->misc_cxt.CurrentUserName);
                securec_check(rc, "\0", "\0");
                HTAB* htbl = session->pcache_cxt.prepared_queries;
                if (htbl) {
                    (void)syscalllockAcquire(&session->pcache_cxt.pstmt_htbl_lock);
                    GetPreparedStatements(htbl, tupStore, tupDesc, session->session_id, userName);
                    (void)syscalllockRelease(&session->pcache_cxt.pstmt_htbl_lock);
                }
            }
            
            elem = DLGetSucc(elem);
        }
        alock.unLock();
        RESUME_INTERRUPTS();
    }
    PG_CATCH();
    {
        (void)syscalllockRelease(&session->pcache_cxt.pstmt_htbl_lock);
        alock.unLock();
        PG_RE_THROW();
    }
    PG_END_TRY();
}
