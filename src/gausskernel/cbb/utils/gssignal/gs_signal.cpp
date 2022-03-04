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
 * File Name	: gs_signal.cpp
 * Brief		: signal management implement
 * Description	: implement the analog signal, include signal init, send, kill, handle, etc.
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/gssignal/gs_signal.cpp
 *
 * -------------------------------------------------------------------------
 */
#ifdef WIN32
#define _WIN32_WINNT 0x0500 /* CreateTimerQueue */
#endif

#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/time.h>
#include <sys/types.h>
#ifndef WIN32
#include <sys/syscall.h>
#endif
#include "utils/elog.h"
#include "gssignal/gs_signal.h"
#include "storage/lock/s_lock.h"
#include "storage/spin.h"
#include "threadpool/threadpool.h"
#include "gs_thread.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "postmaster/postmaster.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "utils/guc.h"

#define SUB_HODLER_SIZE 100
#define RES_SIGNAL SIGUSR2
#define FUNC_NAME_LEN 32

#ifdef ENABLE_UT
#define STATIC
#else
#define STATIC static
#endif
extern volatile ThreadId PostmasterPid;
extern bool IsPostmasterEnvironment;

typedef struct base_signal_lock_info {
    ThreadId tid;
    int just_init;
    char lock_infunc[FUNC_NAME_LEN];
} base_signal_lock_info;

static base_signal_lock_info location_lock_base_signal;

static void* gs_signal_receiver_thread(void* args);
static void gs_signal_reset_sigpool(GsSignal* gs_signal);
static gs_sigfunc gs_signal_register_handler(GsSignal* gs_signal, int signo, gs_sigfunc fun);
static int gs_signal_thread_kill(ThreadId tid, int signo);
static GsSignalSlot* gs_signal_alloc_slot_for_new_thread(char* name, ThreadId thread_id);
static void gs_res_signal_handler(int signo, siginfo_t* siginfo, void* context);
STATIC gs_sigaction_func gs_signal_install_handler(void);
static GsSignalSlot* gs_signal_find_slot(ThreadId thread_id);
static void gs_signal_reset_signal(GsSignal* gssignal);

static void gs_signal_location_base_signal_lock_info(const char* funname, int just_init);
static void gs_signal_unlocation_base_signal_lock_info(void);
extern bool CheckProcSignal(ProcSignalReason reason);

/*
 * @@GaussDB@@
 * Brief		: set lock function info
 * Description	:
 * Notes		:
 */
static void gs_signal_location_base_signal_lock_info(const char* funname, int just_init)
{
    errno_t rc = 0;
    rc = strncpy_s(location_lock_base_signal.lock_infunc, FUNC_NAME_LEN, funname, FUNC_NAME_LEN - 1);
    securec_check(rc, "\0", "\0");
    location_lock_base_signal.lock_infunc[FUNC_NAME_LEN - 1] = '\0';
    location_lock_base_signal.tid = gs_thread_self();
    location_lock_base_signal.just_init = just_init;
}

/*
 * @@GaussDB@@
 * Brief		: remove lock function info
 * Description	:
 * Notes		:
 */
static void gs_signal_unlocation_base_signal_lock_info(void)
{
    location_lock_base_signal.lock_infunc[0] = 0;
    location_lock_base_signal.tid = 0;
}

/*
 * @@GaussDB@@
 * Brief		: lock the g_instance.signal_base->slots_lock to ensure the the thread not exit when send the signal
 * Description	:
 * Notes		:
 */
static int gs_signal_thread_kill(ThreadId tid, int signo)
{
    unsigned int loop = 0;
    GsSignalSlot* slots_index = g_instance.signal_base->slots;
    int ret = 0;

    Assert(NULL != g_instance.signal_base->slots);
    Assert(g_instance.signal_base->slots_size > 0);

    (void)pthread_mutex_lock(&(g_instance.signal_base->slots_lock));
    gs_signal_location_base_signal_lock_info(__func__, 0);

    for (loop = 0; loop < g_instance.signal_base->slots_size; loop++) {
        if (slots_index->thread_id == tid) {
            ret = gs_thread_kill(tid, signo);

            gs_signal_unlocation_base_signal_lock_info();
            (void)pthread_mutex_unlock(&(g_instance.signal_base->slots_lock));

            return ret;
        }
        slots_index++;
    }

    gs_signal_unlocation_base_signal_lock_info();
    (void)pthread_mutex_unlock(&(g_instance.signal_base->slots_lock));

    return ESRCH;
}

/*
 * @@GaussDB@@
 * Brief		: init gs signal pool
 * Description	:
 * Notes		:
 */
static void gs_signal_sigpool_init(GsSignal* gs_signal, int cnt_nodes)
{
    unsigned long loop = 0;
    SignalPool* sigpool = NULL;

    Assert(cnt_nodes > 0);
    sigpool = &gs_signal->sig_pool;

    sigpool->free_head =
        (GsNode*)MemoryContextAlloc(t_thrd.mem_cxt.gs_signal_mem_cxt, (unsigned int)(cnt_nodes) * sizeof(GsNode));

    for (loop = 0; loop < (unsigned int)cnt_nodes - 1; loop++) {
        sigpool->free_head[loop].next = &(sigpool->free_head[loop + 1]);
    }
    sigpool->free_head[cnt_nodes - 1].next = NULL;

    sigpool->free_head = &sigpool->free_head[0];
    sigpool->free_tail = &sigpool->free_head[cnt_nodes - 1];
    sigpool->used_head = NULL;
    sigpool->used_tail = NULL;
    sigpool->pool_size = cnt_nodes;
    if (0 != pthread_mutex_init(&(gs_signal->sig_pool.sigpool_lock), NULL)) {
        ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to init sigpool mutex: %m.")));
    }

    return;
}

/*
 * @@GaussDB@@
 * Brief		: init GsSignal struct for each thread
 * Description	:
 * Notes		:
 */
static GsSignal* gs_signal_init(int cnt_nodes)
{
    GsSignal* gs_signal = (GsSignal*)MemoryContextAlloc(t_thrd.mem_cxt.gs_signal_mem_cxt, sizeof(struct GsSignal));

    int ss_rc = memset_s(gs_signal, sizeof(struct GsSignal), 0, sizeof(struct GsSignal));
    securec_check(ss_rc, "\0", "\0");
    gs_signal_sigpool_init(gs_signal, cnt_nodes);

    return gs_signal;
}

/*
 * @@GaussDB@@
 * Brief		: mark signal used by signal handle
 * Description	:
 * Notes		:
 */
static void gs_signal_send_mark(GsNode* local_node, GsSignalCheckType check_type)
{
    local_node->sig_data.check.check_type = check_type;
    switch (check_type) {
        case SIGNAL_CHECK_EXECUTOR_STOP: {
            if (u_sess != NULL) {
                local_node->sig_data.check.debug_query_id = u_sess->stream_cxt.stop_query_id;
            }
            break;
        }
        case SIGNAL_CHECK_STREAM_STOP: {
            if (u_sess != NULL && u_sess->debug_query_id != 0) {
                local_node->sig_data.check.debug_query_id = u_sess->debug_query_id;
            } else {
                local_node->sig_data.check.check_type = SIGNAL_CHECK_NONE;
            }
            break;
        }
        case SIGNAL_CHECK_SESS_KEY: {
            local_node->sig_data.check.session_id = t_thrd.sig_cxt.session_id;
            break;
        }
        default: {
            break;
        }
    }
}

/*
 * @@GaussDB@@
 * Brief		: set analog signal
 * Description	:
 * Notes		:
 */
static int gs_signal_set_signal_by_threadid(ThreadId thread_id, int signo)
{
    GsSignalSlot* signal_slot = gs_signal_find_slot(thread_id);
    GsSignal* pGsSignal = NULL;
    GsNode* local_node = NULL;
    bool found = false;
    gs_thread_t curThread = gs_thread_get_cur_thread();

    Assert(signo >= 0);
    Assert(signo < GS_SIGNAL_COUNT);

    if (NULL == signal_slot) {
        write_stderr(
            "no signal slot for thread %lu, signo %d, debug_query_id %lu\n", thread_id, signo, u_sess->debug_query_id);
        return -1;
    }

    pGsSignal = (GsSignal*)signal_slot->gssignal;
    Assert(pGsSignal != NULL);

    // Check whether this signal has been sent
    if (0 != pthread_mutex_lock(&(pGsSignal->sig_pool.sigpool_lock))) {
        ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to lock sigpool mutex: %m.")));
    }
    local_node = pGsSignal->sig_pool.used_head;
    for (; local_node != NULL; local_node = local_node->next) {
        bool isFound = (local_node->sig_data.signo == (unsigned int)signo) &&
            (signo != SIGCHLD || local_node->sig_data.thread.thid == curThread.thid);
        if (isFound) {
            found = true;
            break;
        }
    }
    if (0 != pthread_mutex_unlock(&(pGsSignal->sig_pool.sigpool_lock))) {
        ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to unlock sigpool mutex: %m.")));
    }

    if (found) {
        return 0;
    }

    for (;;) {
        if (0 != pthread_mutex_lock(&(pGsSignal->sig_pool.sigpool_lock))) {
            ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to lock sigpool mutex: %m.")));
        }

        if (NULL != pGsSignal->sig_pool.free_head) {
            local_node = pGsSignal->sig_pool.free_head;
            pGsSignal->sig_pool.free_head = local_node->next;
            local_node->next = NULL;
            /* last free node */
            if (local_node == pGsSignal->sig_pool.free_tail) {
                pGsSignal->sig_pool.free_tail = NULL;
            }
            if (0 != pthread_mutex_unlock(&(pGsSignal->sig_pool.sigpool_lock))) {
                ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to unlock sigpool mutex: %m.")));
            }

            break;
        }
        if (0 != pthread_mutex_unlock(&(pGsSignal->sig_pool.sigpool_lock))) {
            ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to unlock sigpool mutex: %m.")));
        }

        /*
         * The thread has exited, slot is clean with gs_signal_slot_release.
         * If new thread use the same slot, new thread maybe receive this signal!
         */
        if (signal_slot->thread_id != thread_id) {
            break;
        }

        Assert(thread_id == PostmasterPid || thread_id == g_instance.pid_cxt.ReaperBackendPID);
    }

    Assert(NULL != local_node);
    if (NULL == local_node) {
        write_stderr(
            "no local_node for thread %lu, signo %d, debug_query_id %lu\n", thread_id, signo, u_sess->debug_query_id);
        return -1;
    }

    local_node->sig_data.signo = signo;
    local_node->sig_data.thread = curThread;
    gs_signal_send_mark(local_node, t_thrd.sig_cxt.gs_sigale_check_type);

    if (0 != pthread_mutex_lock(&(pGsSignal->sig_pool.sigpool_lock))) {
        ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to lock sigpool mutex: %m.")));
    }
    if (NULL == pGsSignal->sig_pool.used_tail) {
        pGsSignal->sig_pool.used_head = local_node;
        pGsSignal->sig_pool.used_tail = local_node;
    } else {
        pGsSignal->sig_pool.used_tail->next = local_node;
        pGsSignal->sig_pool.used_tail = local_node;
    }
    if (0 != pthread_mutex_unlock(&(pGsSignal->sig_pool.sigpool_lock))) {
        ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to unlock sigpool mutex: %m.")));
    }

    return 0;
}

/*
 * @@GaussDB@@
 * Brief		: init the slots in g_instance.signal_base
 * Description	:
 * Notes		:
 */
void gs_signal_slots_init(unsigned long int size)
{
    unsigned int loop = 0;
    GsSignalSlot* tmp_sig_slot = NULL;

    Assert(size > 0);

    (void)pthread_mutex_init(&(g_instance.signal_base->slots_lock), NULL);
    gs_signal_location_base_signal_lock_info(__func__, 1);
    g_instance.signal_base->slots =
        (GsSignalSlot*)MemoryContextAlloc(t_thrd.mem_cxt.gs_signal_mem_cxt, (sizeof(GsSignalSlot) * size));

    /* create GsSignal for ever slot */
    g_instance.signal_base->slots_size = size;
    for (loop = 0; loop < g_instance.signal_base->slots_size; loop++) {
        int cnt_nodes = ((loop > 0) ? SUB_HODLER_SIZE : size);

        tmp_sig_slot = &(g_instance.signal_base->slots[loop]);
        tmp_sig_slot->gssignal = gs_signal_init(cnt_nodes);
        tmp_sig_slot->thread_id = 0;
        tmp_sig_slot->thread_name = NULL;
    }

    return;
}

/*
 * @@GaussDB@@
 * Brief		: take a slot for new thread
 * Description	:
 * Notes		:
 */
static GsSignalSlot* gs_signal_alloc_slot_for_new_thread(char* name, ThreadId thread_id)
{
    unsigned int loop = 0;

    GsSignalSlot* slots_index = g_instance.signal_base->slots;

    (void)pthread_mutex_lock(&(g_instance.signal_base->slots_lock));
    gs_signal_location_base_signal_lock_info(__func__, 0);

    for (loop = 0; loop < g_instance.signal_base->slots_size; loop++) {
        if (slots_index->thread_id == 0) {
            slots_index->thread_id = thread_id;
            slots_index->thread_name = name;
            break;
        }
        slots_index++;
    }

    gs_signal_unlocation_base_signal_lock_info();
    (void)pthread_mutex_unlock(&(g_instance.signal_base->slots_lock));
    if (loop >= g_instance.signal_base->slots_size) {
        ereport(
            FATAL, (errcode(ERRCODE_TOO_MANY_CONNECTIONS), errmsg("no signal slot available for new thread creation")));
    }

    /* init the gssignal of the slot */
    gs_signal_reset_signal(slots_index->gssignal);
    t_thrd.signal_slot = slots_index;
    Assert(NULL != t_thrd.signal_slot);
    return slots_index;
}

/*
 * @@GaussDB@@
 * Brief		: reset the sigpool
 * Description	:
 * Notes		:
 */
static void gs_signal_reset_sigpool(GsSignal* gs_signal)
{
    Assert(NULL != gs_signal);

    if (0 != pthread_mutex_lock(&(gs_signal->sig_pool.sigpool_lock))) {
        ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to lock sigpool mutex: %m.")));
    }

    if (NULL != gs_signal->sig_pool.used_head) {
        if (NULL == gs_signal->sig_pool.free_tail) {
            gs_signal->sig_pool.free_tail = gs_signal->sig_pool.used_tail;
            gs_signal->sig_pool.free_head = gs_signal->sig_pool.used_head;
        } else {
            gs_signal->sig_pool.free_tail->next = gs_signal->sig_pool.used_head;

            Assert(NULL != gs_signal->sig_pool.used_tail);
            gs_signal->sig_pool.free_tail = gs_signal->sig_pool.used_tail;
        }

        gs_signal->sig_pool.used_head = NULL;
        gs_signal->sig_pool.used_tail = NULL;
    }

    if (0 != pthread_mutex_unlock(&(gs_signal->sig_pool.sigpool_lock))) {
        ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to unlock sigpool mutex: %m.")));
    }

    return;
}

/*
 * @@GaussDB@@
 * Brief		: reset the analog signal
 * Description	:
 * Notes		:
 */
static void gs_signal_reset_signal(GsSignal* gs_signal)
{
    gs_sigfunc* handlerList = gs_signal->handlerList;

    // Reset all signal handlers to ingore. If current thread is interested
    // in any of them, they will override it with gspqsignal().
    //
    for (int i = 0; i < GS_SIGNAL_COUNT; i++)
        handlerList[i] = SIG_IGN;

    gs_signal->bitmapSigProtectFun = 0;
    gs_signal_reset_sigpool(gs_signal);
    return;
}

/*
 * @@GaussDB@@
 * Brief		: kill() emulation for threads
 * Description	:
 * Notes		: make sure it is async-signal-safe
 */
int gs_thread_kill(ThreadId tid, int sig)
{
    return pthread_kill(tid, sig);
}

/*
 * @@GaussDB@@
 * Brief		: the thread must set signal structure
 * Description	:
 * Notes		:
 */
GsSignalSlot* gs_signal_find_slot(ThreadId thread_id)
{
    unsigned int loop = 0;
    GsSignalSlot* slots_index = g_instance.signal_base->slots;

    Assert(NULL != g_instance.signal_base->slots);
    Assert(g_instance.signal_base->slots_size > 0);

    (void)pthread_mutex_lock(&(g_instance.signal_base->slots_lock));
    gs_signal_location_base_signal_lock_info(__func__, 0);

    for (loop = 0; loop < g_instance.signal_base->slots_size; loop++) {
        if (slots_index->thread_id == thread_id) {
            gs_signal_unlocation_base_signal_lock_info();
            (void)pthread_mutex_unlock(&(g_instance.signal_base->slots_lock));
            return slots_index;
        }
        slots_index++;
    }

    gs_signal_unlocation_base_signal_lock_info();
    (void)pthread_mutex_unlock(&(g_instance.signal_base->slots_lock));

    return NULL;
}

/*
 * @@GaussDB@@
 * Brief		: release the stot of a thread
 * Description	:
 * Notes		:
 */
GsSignalSlot* gs_signal_slot_release(ThreadId thread_id)
{
    unsigned int loop = 0;
    GsSignalSlot* slots_index = g_instance.signal_base->slots;

    (void)pthread_mutex_lock(&(g_instance.signal_base->slots_lock));
    gs_signal_location_base_signal_lock_info(__func__, 0);

    for (loop = 0; loop < g_instance.signal_base->slots_size; loop++) {
        if (slots_index->thread_id == thread_id) {
            slots_index->thread_id = 0;
            slots_index->thread_name = NULL;
            break;
        }

        slots_index++;
    }

    gs_signal_unlocation_base_signal_lock_info();
    (void)pthread_mutex_unlock(&(g_instance.signal_base->slots_lock));

    return slots_index;
}

/*
 * @@GaussDB@@
 * Brief		: create the signal receiver thread
 * Description	:
 * Notes		:
 */
void gs_signal_monitor_startup(void)
{
    gs_thread_t thread;
    int errCode = 0;

    (void)gs_signal_block_sigusr2();
    (void)gs_signal_install_handler();
    errCode = gs_thread_create(&thread, gs_signal_receiver_thread, 0, NULL);
    if (0 != errCode) {
        ereport(LOG, (errmsg("could not start a new thread, because of no  enough system resource. ")));
        proc_exit(1);
    }

    return;
}

/*
 * @@GaussDB@@
 * Brief		: gs send signal to thread
 * Description	:
 * Notes		:
 */
int gs_signal_send(ThreadId thread_id, int signo, int nowait)
{
    sigset_t old_sigset;
    int code = 0;
    int count = 0;

    if (unlikely(signo < 0 || signo >= GS_SIGNAL_COUNT)) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_ATTRIBUTE), errmsg("Invalid signal number for signal send: %d.", signo)));
    }

    if (unlikely(thread_id == 0)) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_ATTRIBUTE), errmsg("Invalid thread id for signal send: %lu.", thread_id)));
    }

    old_sigset = gs_signal_block_sigusr2();

retry:
    code = gs_signal_set_signal_by_threadid(thread_id, signo);
    if (code != 0) {
        count++;
        if (!nowait && count <= 3) {
            /*
             * sleep a few ms time, which cannot be too long or
             * too short.
             *
             * caller maybe have a big loop. so if this sleep time
             * is too long, the whole process may be present as hanging.
             *
             * for backend thread, main thread add its thread id into backendlist
             * in BackendStartup(), and send signal to it according to backendlist.
             * but signal slot is set by backend thread during starting up. here
             * there is a time window that this thread id appears in backendlist
             * but not in signal slots. so the signal may not reach the backend
             * and this backend cannot be nofified to exit.
             */
            pg_usleep(10000L);
            goto retry;
        } else {
            gs_signal_recover_mask(old_sigset);
            t_thrd.sig_cxt.gs_sigale_check_type = SIGNAL_CHECK_NONE;
            return EBADSLT;
        }
    }

    t_thrd.sig_cxt.gs_sigale_check_type = SIGNAL_CHECK_NONE;

    code = gs_signal_thread_kill(thread_id, RES_SIGNAL);

    gs_signal_recover_mask(old_sigset);

    return code;
}

/*
 * @@GaussDB@@
 * Brief		: gs send signal to thread
 * Description	:
 * Notes		:
 */
int gs_signal_send(ThreadId thread_id, int signo, bool is_thread_pool)
{
    if (is_thread_pool == true) {
        Assert(ENABLE_THREAD_POOL);
        return g_threadPoolControler->GetSessionCtrl()->SendSignal((int)thread_id, signo);
    } else {
        return gs_signal_send(thread_id, signo);
    }
}

/*
 * @@GaussDB@@
 * Brief		: set signal handle of signo
 * Description	: this function is called in thread self
 * Notes		:
 */
static gs_sigfunc gs_signal_register_handler(GsSignal* gs_signal, int signo, gs_sigfunc fun)
{
    gs_sigfunc prefun;

    Assert(gs_signal != NULL);
    Assert(signo >= 0);
    Assert(signo < GS_SIGNAL_COUNT);

    prefun = gs_signal->handlerList[signo];
    gs_signal->handlerList[signo] = fun;

    return prefun;
}

/*
 * @@GaussDB@@
 * Brief		: check signal is valid or not
 * Description	: check by debug_query_id and so on.
 * Notes		:
 */
static bool gs_signal_handle_check(const GsSndSignal* local_node)
{
    switch (local_node->check.check_type) {
        case SIGNAL_CHECK_EXECUTOR_STOP:
        case SIGNAL_CHECK_STREAM_STOP: {
            if (u_sess != NULL && local_node->check.debug_query_id == u_sess->debug_query_id) {
                return true;
            }

            ereport(DEBUG2,
                (errmodule(MOD_STREAM), errcode(ERRCODE_LOG),
                    errmsg("receive stop signal from queryid %lu, but current queryid is %lu.",
                        local_node->check.debug_query_id,
                        (u_sess != NULL) ? u_sess->debug_query_id : 0)));
            return false;
        }
        case SIGNAL_CHECK_SESS_KEY: {
            return (u_sess != NULL && local_node->check.session_id == u_sess->session_id);
        }
        default: {
            return true;
        }
    }
}

/*
 * @@GaussDB@@
 * Brief		: the signal handle function of SIGUSR2
 * Description	: handle the received analog signal
 * Notes		:
 */
void gs_signal_handle(void)
{
    GsSignalSlot* signal_slot = t_thrd.signal_slot;
    GsSndSignal* psnd_signal = NULL;
    unsigned int sndsigno = 0;
    GsSignal* pGsSignal = NULL;
    GsNode* local_node = NULL;
    GsNode* mask_list_head = NULL;
    GsNode* mask_list_tail = NULL;
    bool is_signal_valid = false;

    Assert(signal_slot != NULL);

    if (NULL == (pGsSignal = (GsSignal*)signal_slot->gssignal)) {
        return;
    }

    sigset_t old_sigset = gs_signal_block_sigusr2();
    for (;;) {
        if (0 != pthread_mutex_lock(&(pGsSignal->sig_pool.sigpool_lock))) {
            ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to lock sigpool mutex: %m.")));
        }
        local_node = pGsSignal->sig_pool.used_head;

        if (NULL == local_node) {
            if (0 != pthread_mutex_unlock(&(pGsSignal->sig_pool.sigpool_lock))) {
                ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to unlock sigpool mutex: %m.")));
            }
            break;
        }

        if (NULL == local_node->next) {
            pGsSignal->sig_pool.used_head = NULL;
            pGsSignal->sig_pool.used_tail = NULL;
        } else {
            pGsSignal->sig_pool.used_head = local_node->next;
        }

        if (0 != pthread_mutex_unlock(&(pGsSignal->sig_pool.sigpool_lock))) {
            ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to unlock sigpool mutex: %m.")));
        }

        local_node->next = NULL;
        psnd_signal = (GsSndSignal*)(&(local_node->sig_data));

        sndsigno = psnd_signal->signo;

        if (FALSE == sigismember(&pGsSignal->masksignal, sndsigno)) {
            gs_thread_t tmpThread;
            gs_sigfunc sigHandler = pGsSignal->handlerList[sndsigno];
            tmpThread = psnd_signal->thread;
            if (0 != pthread_mutex_lock(&(pGsSignal->sig_pool.sigpool_lock))) {
                ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to lock sigpool mutex: %m.")));
            }

            if (NULL == pGsSignal->sig_pool.free_tail) {
                pGsSignal->sig_pool.free_tail = local_node;
                pGsSignal->sig_pool.free_head = local_node;
            } else {
                pGsSignal->sig_pool.free_tail->next = local_node;
                pGsSignal->sig_pool.free_tail = local_node;
            }

            is_signal_valid = gs_signal_handle_check(psnd_signal);

            if (0 != pthread_mutex_unlock(&(pGsSignal->sig_pool.sigpool_lock))) {
                ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to unlock sigpool mutex: %m.")));
            }

            if (sigHandler == SIG_IGN || sigHandler == SIG_DFL) {
                continue;
            }

            // Hornor the user signal handler
            //
            if (NULL != sigHandler) {
                t_thrd.postmaster_cxt.CurExitThread = tmpThread;
                if (is_signal_valid == true) {
                    (*sigHandler)(sndsigno);
                }
            }
        } else {
            if (NULL == mask_list_tail) {
                mask_list_tail = local_node;
                mask_list_head = local_node;
            } else {
                mask_list_tail->next = local_node;
                mask_list_tail = local_node;
            }
        }
    }

    /* mask list add used list, because mask signal need be */
    if (0 != pthread_mutex_lock(&(pGsSignal->sig_pool.sigpool_lock))) {
        ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to lock sigpool mutex: %m.")));
    }

    if (NULL == pGsSignal->sig_pool.used_tail) {
        pGsSignal->sig_pool.used_head = mask_list_head;
        pGsSignal->sig_pool.used_tail = mask_list_tail;
    } else {
        pGsSignal->sig_pool.used_tail->next = mask_list_head;
        if (NULL != mask_list_tail) {
            pGsSignal->sig_pool.used_tail = mask_list_tail;
        }
    }

    if (0 != pthread_mutex_unlock(&(pGsSignal->sig_pool.sigpool_lock))) {
        ereport(PANIC, (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("failed to unlock sigpool mutex: %m.")));
    }

    gs_signal_recover_mask(old_sigset);

    return;
}

/*
 * @@GaussDB@@
 * Brief		: mask signal
 * Description	:
 * Notes		:
 */
static void gs_signal_record_mask(sigset_t* mask, sigset_t* old_mask)
{
    GsSignalSlot* signal_slot = t_thrd.signal_slot;
    GsSignal* pGsSignal = NULL;

    if (NULL == signal_slot) {
        return;
    }

    pGsSignal = (GsSignal*)signal_slot->gssignal;
    if (NULL != old_mask) {
        int ss_rc = memcpy_s(old_mask, sizeof(sigset_t), &pGsSignal->masksignal, sizeof(sigset_t));
        securec_check(ss_rc, "\0", "\0");
    }
    int ss_rc = memcpy_s(&pGsSignal->masksignal, sizeof(pGsSignal->masksignal), mask, sizeof(sigset_t));
    securec_check(ss_rc, "\0", "\0");

    if (t_thrd.sig_cxt.signal_handle_cnt == 0) {
        t_thrd.sig_cxt.signal_handle_cnt++;
        gs_signal_handle();
        t_thrd.sig_cxt.signal_handle_cnt--;
    }

    return;
}

/*
 * @@GaussDB@@
 * Brief		: gaussdb signal setmask
 * Description	:
 * Notes		: todo: change name
 */
void gs_signal_setmask(sigset_t* mask, sigset_t* old_mask)
{
    sigset_t old_sigset;
#ifndef WIN32
    if (!IsPostmasterEnvironment) {
        sigprocmask(SIG_SETMASK, mask, NULL);
        return;
    }
#endif
    old_sigset = gs_signal_block_sigusr2();
    gs_signal_record_mask(mask, old_mask);
    gs_signal_recover_mask(old_sigset);

    return;
}

/*
 * @@GaussDB@@
 * Brief		: the implement of the signal receiver thread
 * Description	:
 * Notes		:
 */
void* gs_signal_receiver_thread(void* args)
{
    /*
     * Listen on all signals. We explicitely include the list of signals that we
     * are interested in to reduce exposure.
     */
    sigset_t waitMask;

    /* wait below signals: SIGINT, SIGTERM, SIGQUIT, SIGHUP */
    sigemptyset(&waitMask);
    sigaddset(&waitMask, SIGINT);
    sigaddset(&waitMask, SIGTERM);
    sigaddset(&waitMask, SIGQUIT);
    sigaddset(&waitMask, SIGHUP);
    sigaddset(&waitMask, SIGUSR1);

    gs_signal_block_sigusr2();

    /* add just for memcheck */
    gs_thread_args_free();

    for (;;) {
        int signo;

        /* Wait for signals arrival. */
        sigwait(&waitMask, &signo);

        /* send signal to thread */
        (void)gs_signal_send(PostmasterPid, signo);
    }
    return NULL;
}

#ifndef WIN32
/*
 * @@GaussDB@@
 * Brief		: Signal handler for the reserved communication signal.
 * Description	: RES_SIGNAL is enabled by all threads to enable inter-signal notification.
 * Notes		:
 */
static void gs_res_signal_handler(int signo, siginfo_t* siginfo, void* context)
{
    void* signature = NULL;
    MemoryContext oldContext = CurrentMemoryContext;

    /*
     * Calculate the timer signature and mark the SIGALRM by myself. This is
     * because SIGALRM is dilivered by current thread's timer.
     */
    ThreadId thread_id = gs_thread_self();
    signature = (void*)(SIGALRM + thread_id);

    if (siginfo->si_value.sival_ptr == signature) {
        if (SIG_IGN != t_thrd.signal_slot->gssignal->handlerList[SIGALRM] &&
            SIG_DFL != t_thrd.signal_slot->gssignal->handlerList[SIGALRM]) {
            (t_thrd.signal_slot->gssignal->handlerList[SIGALRM])(SIGALRM);
        }

        CurrentMemoryContext = oldContext;
        return;
    }

    /* Hornour the signal */
    gs_signal_handle();

    CurrentMemoryContext = oldContext;
    return;
}
#endif

/*
 * @@GaussDB@@
 * Brief		: init signal info fo ever thread.
 * Description	:
 * Notes		:
 */
void gs_signal_startup_siginfo(char* thread_name)
{
    pqinitmask();
    (void)gs_signal_alloc_slot_for_new_thread(thread_name, gs_thread_self());
}

/*
 * @@GaussDB@@
 * Brief		: Install a process wise SYSV style signal handler.
 * Description	:
 * Notes		:
 */
STATIC gs_sigaction_func gs_signal_install_handler(void)
{
    struct sigaction act, oact;

    /*
     * It is important to set SA_NODEFER flag, so this signal is not added
     * to the signal mask of the calling process on entry to the signal handler.
     */
    sigemptyset(&act.sa_mask);
    act.sa_sigaction = gs_res_signal_handler;
    act.sa_flags = 0;
    act.sa_flags |= SA_SIGINFO;
    act.sa_flags |= SA_RESTART;

    if (sigaction(RES_SIGNAL, &act, &oact) < 0) {
        ereport(PANIC, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("not able to set up signal action handler")));
    }

    /* Return previously installed handler */
    return oact.sa_sigaction;
}

/*
 * @@GaussDB@@
 * Brief		: Create timer support for current thread
 * Description	:
 * Notes		:
 */
int gs_signal_createtimer(void)
{
    struct sigevent sev;

    /*
     * Set up per thread timer.
     * Upon timeout, a SIGUSR2 will be dilivered to current thread itself.
     * The signal handler can check sival_ptr for the reason. We don't want
     * to use SIGEV_THREAD callback method, because it needs certain environment
     * set up to work correctly. Meanwhile, it creates a new thread for the
     * callback, which is inefficiently.
     */
    sev.sigev_notify = SIGEV_SIGNAL | SIGEV_THREAD_ID;
    sev.sigev_signo = RES_SIGNAL;
    sev.sigev_value.sival_ptr = (void*)(SIGALRM + gs_thread_self());
    sev._sigev_un._tid = syscall(SYS_gettid);

    if (timer_create(CLOCK_REALTIME, &sev, &t_thrd.utils_cxt.sigTimerId) == -1) {
        ereport(FATAL, (errmsg("failed to create timer for thread")));
        return -1;
    }

    return 0;
}

/*
 * @@GaussDB@@
 * Brief		: A thread-safe version replacement of setitimer () call.
 * Description	: 0  is returned if successed.
 * Notes		:
 */
int gs_signal_settimer(struct itimerval* interval)
{
    struct itimerspec itspec;

    /* Translate itimerval to itimerspec */
    itspec.it_value.tv_sec = interval->it_value.tv_sec;
    itspec.it_value.tv_nsec = interval->it_value.tv_usec * 1000ULL;
    itspec.it_interval.tv_sec = interval->it_interval.tv_sec;
    itspec.it_interval.tv_nsec = interval->it_interval.tv_usec * 1000ULL;

    /*
     * Return directly without no ereport as it is a replacement for
     * setitimer() library call
     * is returned if successed
     */
    return timer_settime(t_thrd.utils_cxt.sigTimerId, /* the created timer */
        0,                                            /* timer flag */
        &itspec,                                      /* wake up interval */
        NULL);                                        /* previous timer setting */
}

/*
 * @@GaussDB@@
 * Brief		: A thread-safe version replacement of canceltimer () call.
 * Description	: 0  is returned if successed.
 * Notes		:
 */
int gs_signal_canceltimer(void)
{
    struct itimerspec itspec;

    /* Set interval to zero to cancel the timer */
    int ss_rc = memset_s(&itspec, sizeof(itspec), 0, sizeof(itspec));
    securec_check(ss_rc, "\0", "\0");

    /*
     * Return directly without no ereport as it is a replacement for
     * setitimer() library call.
     */
    return timer_settime(t_thrd.utils_cxt.sigTimerId, /* the created timer */
        0,                                            /* timer flag */
        &itspec,                                      /* wake up interval */
        NULL);                                        /* previous timer setting */
}

/*
 * @@GaussDB@@
 * Brief		: A thread-safe version replacement of delteitimer (0) call.
 * Description	: 0  is returned if successed.
 * Notes		:
 */
int gs_signal_deletetimer(void)
{
    int code = 0;

    if (t_thrd.utils_cxt.sigTimerId != NULL) {
        code = timer_delete(t_thrd.utils_cxt.sigTimerId);
        t_thrd.utils_cxt.sigTimerId = NULL;
    }
    return code;
}

/*
 *  block all signal except some thread including sigusr2
 */
sigset_t gs_signal_unblock_sigusr2(void)
{
    sigset_t intMask;
    sigset_t oldMask;

    (void)sigfillset(&intMask);
    (void)sigdelset(&intMask, SIGUSR2);
    (void)sigdelset(&intMask, SIGPROF);
    (void)sigdelset(&intMask, SIGSEGV);
    (void)sigdelset(&intMask, SIGBUS);
    (void)sigdelset(&intMask, SIGFPE);
    (void)sigdelset(&intMask, SIGILL);
    (void)sigdelset(&intMask, SIGSYS);
    (void)pthread_sigmask(SIG_SETMASK, &intMask, &oldMask);

    return oldMask;
}
/*
 * block some signal including sigusr2
 * return the old mask
 */
sigset_t gs_signal_block_sigusr2(void)
{
    sigset_t intMask;
    sigset_t oldMask;

    sigfillset(&intMask);
    sigdelset(&intMask, SIGPROF);
    (void)sigdelset(&intMask, SIGSEGV);
    (void)sigdelset(&intMask, SIGBUS);
    (void)sigdelset(&intMask, SIGFPE);
    (void)sigdelset(&intMask, SIGILL);
    (void)sigdelset(&intMask, SIGSYS);

    pthread_sigmask(SIG_SETMASK, &intMask, &oldMask);

    return oldMask;
}

void gs_signal_recover_mask(const sigset_t mask)
{
    pthread_sigmask(SIG_SETMASK, &mask, NULL);
}

/*
 * @@GaussDB@@
 * Brief		: Set up a signal handler  gspqsignall
 * Description	:
 * Notes		:
 */
gs_sigfunc gspqsignal(int signo, gs_sigfunc func)
{
    return gs_signal_register_handler(t_thrd.signal_slot->gssignal, signo, func);
}
