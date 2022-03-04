/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 *
 *
 *  heartbeat.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/heartbeat.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <sys/epoll.h>
#include <new>
#include "postgres.h"
#include "knl/knl_variable.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "gssignal/gs_signal.h"
#include "replication/heartbeat.h"
#include "replication/heartbeat/heartbeat_server.h"
#include "replication/heartbeat/heartbeat_client.h"
#include "replication/heartbeat/heartbeat_conn.h"

const int EXIT_MODE_TWO = 2;
const int SLEEP_MILLISECONDS = 20;

static HeartbeatServer *g_heartbeat_server = NULL;
static HeartbeatClient *g_heartbeat_client = NULL;

/* Signal handlers */
static void heartbeat_sighup_handler(SIGNAL_ARGS);
static void heartbeat_quick_die(SIGNAL_ARGS);
static void heartbeat_shutdown_handler(SIGNAL_ARGS);
static void heartbeat_sigusr1_handler(SIGNAL_ARGS);
static void heartbeat_kill(int code, Datum arg);
static void heartbeat_init(void);
static void set_block_sigmask(sigset_t *block_signal);
static void destroy_client_and_server();
static int create_client_and_server(int epollfd);
static void delay_control(TimestampTz last_send_time);
static int deal_with_events(int epollfd, struct epoll_event *events, const sigset_t *block_sig_set);
static int deal_with_sigup();
static void unset_events_conn(struct epoll_event *events, int begin, int size, HeartbeatConnection *con);

static int server_loop(void)
{
    TimestampTz last_send_time = 0;
    sigset_t block_sig_set;
    struct epoll_event events[MAX_EVENTS];
    set_block_sigmask(&block_sig_set);
    int epollfd = epoll_create(MAX_EVENTS);
    if (epollfd < 0) {
        ereport(ERROR, (errmsg("create epoll failed %d.", epollfd)));
        return 1;
    }

    if (create_client_and_server(epollfd)) {
        goto OUT;
    }

    for (;;) {
        if (deal_with_sigup()) {
            break;
        }

        if (t_thrd.heartbeat_cxt.shutdown_requested) {
            /*
             * From here on, elog(ERROR) should end with exit(1), not send
             * control back to the sigsetjmp block above.
             */
            u_sess->attr.attr_common.ExitOnAnyError = true;
            g_instance.heartbeat_cxt.heartbeat_running = false;
            destroy_client_and_server();
            (void)close(epollfd);
            proc_exit(0);
        }

        ereport(DEBUG2, (errmsg("heartbeat ...")));
        if (g_heartbeat_client && !g_heartbeat_client->IsConnect() && g_heartbeat_client->Connect()) {
            /*
             * The client has sent a startup packet in the Connect method,
             * and the server will reply a heartbeat packet.
             */
            last_send_time = GetCurrentTimestamp();
        }

        if (deal_with_events(epollfd, events, &block_sig_set)) {
            break;
        }

        if (g_heartbeat_client) {
            /* Limit the heartbeat frequency */
            delay_control(last_send_time);
            if (g_heartbeat_client->IsConnect() && g_heartbeat_client->SendBeatHeartPacket()) {
                last_send_time = GetCurrentTimestamp();
            }
        }
    }

OUT:
    g_instance.heartbeat_cxt.heartbeat_running = false;
    destroy_client_and_server();
    (void)close(epollfd);
    return 1;
}

static int deal_with_sigup()
{
    int j;
    if (t_thrd.heartbeat_cxt.got_SIGHUP) {
        t_thrd.heartbeat_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
        /*
         * when Ha replconninfo have changed and current_mode is not NORMAL,
         * dynamically modify the ha socket.
         */
        for (j = 1; j < MAX_REPLNODE_NUM; j++) {
            if (t_thrd.postmaster_cxt.ReplConnChangeType[j] == OLD_REPL_CHANGE_IP_OR_PORT) {
                break;
            }
        }
        if (j < MAX_REPLNODE_NUM) {
            if (g_heartbeat_server != NULL) {
                if (!g_heartbeat_server->Restart()) {
                    return 1;
                }
            }
            for (int i = 1; i < MAX_REPLNODE_NUM; i++) {
                t_thrd.postmaster_cxt.ReplConnChangeType[i] = NO_CHANGE;
            }
            if (g_heartbeat_client != NULL) {
                /* The client will auto connect later. */
                g_heartbeat_client->DisConnect();
            }
        }
    }
    return 0;
}

static int deal_with_events(int epollfd, struct epoll_event *events, const sigset_t *block_sig_set)
{
    /* Wait for events to happen in send_interval */
    int fds = epoll_pwait(epollfd, events, MAX_EVENTS, u_sess->attr.attr_common.dn_heartbeat_interval, block_sig_set);
    if (fds < 0) {
        if (errno != EINTR && errno != EWOULDBLOCK && errno != ETIMEDOUT) {
            ereport(ERROR, (errmsg("epoll_wait fd %d error :%m, agent thread exit.", epollfd)));
            return 1;
        }
    }

    for (int i = 0; i < fds; i++) {
        HeartbeatConnection *con = (HeartbeatConnection *)events[i].data.ptr;
        HeartbeatConnection *releasedConn = NULL;

        if (events[i].events & EPOLLIN) {
            if (con != NULL) {
                con->callback(epollfd, events[i].events, con, (void **)&releasedConn);
                if (releasedConn) {
                    /* skip remaining events of the release connection */
                    unset_events_conn(events, i, fds, releasedConn);
                }
            }
        }
    }
    /* add flow control to avoid network attack */
    pg_usleep(SLEEP_MILLISECONDS * USECS_PER_MSEC);
    return 0;
}

static void unset_events_conn(struct epoll_event *events, int begin, int size, HeartbeatConnection *con)
{
    for (int i = begin; i < size; i++) {
        HeartbeatConnection *curCon = (HeartbeatConnection *)events[i].data.ptr;

        if (curCon == con) {
            events[i].data.ptr = NULL;
        }
    }
}

static void delay_control(TimestampTz last_send_time)
{
    long secs = 0;
    int microsecs = 0;
    TimestampTz now = GetCurrentTimestamp();
    TimestampTz timeout = TimestampTzPlusMilliseconds(last_send_time, u_sess->attr.attr_common.dn_heartbeat_interval);
    TimestampDifference(now, timeout, &secs, &microsecs);

    /* If has exceeded send_interval, don't delay. */
    if (secs == 0 && microsecs == 0) {
        return;
    } else if (secs > u_sess->attr.attr_common.dn_heartbeat_interval) {
        secs = u_sess->attr.attr_common.dn_heartbeat_interval;
    }

    pg_usleep(secs * USECS_PER_SEC + microsecs);
}

static void set_block_sigmask(sigset_t *block_signal)
{
    (void)sigfillset(block_signal);
#ifdef SIGTRAP
    (void)sigdelset(block_signal, SIGTRAP);
#endif
#ifdef SIGABRT
    (void)sigdelset(block_signal, SIGABRT);
#endif
#ifdef SIGILL
    (void)sigdelset(block_signal, SIGILL);
#endif
#ifdef SIGFPE
    (void)sigdelset(block_signal, SIGFPE);
#endif
#ifdef SIGSEGV
    (void)sigdelset(block_signal, SIGSEGV);
#endif
#ifdef SIGBUS
    (void)sigdelset(block_signal, SIGBUS);
#endif
#ifdef SIGSYS
    (void)sigdelset(block_signal, SIGSYS);
#endif
}

static void heartbeat_handle_exception(MemoryContext heartbeat_context)
{
    /* Since not using PG_TRY, must reset error stack by hand */
    t_thrd.log_cxt.error_context_stack = NULL;

    /* Prevent interrupts while cleaning up */
    HOLD_INTERRUPTS();

    /* Report the error to the server log */
    EmitErrorReport();

    /* release resource held by lsc */
    AtEOXact_SysDBCache(false);

    /* Buffer pins are released here: */
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);

    /*
     * Free client and server before free memory context
     */
    g_instance.heartbeat_cxt.heartbeat_running = false;
    destroy_client_and_server();

    /*
     * Now return to normal top-level context and clear ErrorContext for
     * next time.
     */
    (void)MemoryContextSwitchTo(heartbeat_context);
    FlushErrorState();

    /* Flush any leaked data in the top-level context */
    MemoryContextResetAndDeleteChildren(heartbeat_context);

    /* Now we can allow interrupts again */
    RESUME_INTERRUPTS();

    /*
     * Sleep at least 1 second after any error.  A write error is likely
     * to be repeated, and we don't want to be filling the error logs as
     * fast as we can.
     */
    pg_usleep(1000000L);

    return;
}
void heartbeat_main(void)
{
    sigjmp_buf localSigjmpBuf;
    MemoryContext heartbeat_context;

    t_thrd.role = HEARTBEAT;
    t_thrd.proc_cxt.MyProgName = "Heartbeat";

    heartbeat_init();

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGHUP, heartbeat_sighup_handler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, heartbeat_shutdown_handler);
    (void)gspqsignal(SIGQUIT, heartbeat_quick_die); /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, heartbeat_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);

    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    g_instance.heartbeat_cxt.heartbeat_running = true;

    /*
     * Create a resource owner to keep track of our resources.
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Heartbeat",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    /*
     * Create a memory context that we will do all our work in.  We do this so
     * that we can reset the context during error recovery and thereby avoid
     * possible memory leaks.  Formerly this code just ran in
     * TopMemoryContext, but resetting that would be a really bad idea.
     */
    heartbeat_context = AllocSetContextCreate(TopMemoryContext, "Heartbeat", ALLOCSET_DEFAULT_MINSIZE,
                                              ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(heartbeat_context);

    /*
     * If an exception is encountered, processing resumes here.
     *
     * See notes in postgres.c about the design of this coding.
     *
     */
    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        heartbeat_handle_exception(heartbeat_context);
    }

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    ereport(LOG, (errmsg("heartbeat thread started")));
    proc_exit(server_loop());
}

static void heartbeat_sighup_handler(SIGNAL_ARGS)
{
    t_thrd.heartbeat_cxt.got_SIGHUP = true;
}

static void heartbeat_quick_die(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    /*
     * We DO NOT want to run proc_exit() callbacks -- we're here because
     * shared memory may be corrupted, so we don't want to try to clean up our
     * transaction.  Just nail the windows shut and get out of town.  Now that
     * there's an atexit callback to prevent third-party code from breaking
     * things by calling exit() directly, we have to reset the callbacks
     * explicitly to make this work as intended.
     */
    on_exit_reset();

    g_instance.heartbeat_cxt.heartbeat_running = false;

    /*
     * Note we do exit(2) not exit(0).    This is to force the postmaster into a
     * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
     * backend.  This is necessary precisely because we don't clean up our
     * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
     * should ensure the postmaster sees this as a crash, too, but no harm in
     * being doubly sure.)
     */
    exit(EXIT_MODE_TWO);
}

static void heartbeat_shutdown_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.heartbeat_cxt.shutdown_requested = true;

    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    errno = save_errno;
}

/* SIGUSR1: used for latch wakeups */
static void heartbeat_sigusr1_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    latch_sigusr1_handler();

    errno = save_errno;
}

/*
 * Return 0 if the heartbeat thread is not running.
 */
TimestampTz get_last_reply_timestamp(int replindex)
{
    /* Initialize the last reply timestamp */
    volatile heartbeat_state *stat = t_thrd.heartbeat_cxt.state;
    TimestampTz last_reply_time = 0;
    if (stat == NULL || stat->pid == 0) {
        return last_reply_time;
    }

    if (replindex < START_REPLNODE_NUM || replindex >= DOUBLE_MAX_REPLNODE_NUM) {
        ereport(COMMERROR, (errmsg("Invalid channel id: %d.", replindex)));
        return last_reply_time;
    }

    ReplConnInfo* replconninfo = NULL;
    if (replindex >= MAX_REPLNODE_NUM)
            replconninfo = t_thrd.postmaster_cxt.CrossClusterReplConnArray[replindex - MAX_REPLNODE_NUM];
        else
            replconninfo = t_thrd.postmaster_cxt.ReplConnArray[replindex]; 

    if (replconninfo == NULL) {
        ereport(COMMERROR, (errmsg("The reliconninfo is not find.")));
        return last_reply_time;
    }

    SpinLockAcquire(&stat->mutex);
    last_reply_time = stat->channel_array[replindex].last_reply_timestamp;
    SpinLockRelease(&stat->mutex);

    ereport(DEBUG2, (errmsg("Get last reply timestap of replindex:%d, Time:%ld", replindex, last_reply_time)));
    return last_reply_time;
}

/* Report shared-memory space needed by heartbeat */
Size heartbeat_shmem_size(void)
{
    Size size = 0;
    size = add_size(size, sizeof(heartbeat_state));
    return size;
}

/*
 * Reset heartbeat timestamp.
 */ 
void InitHeartbeatTimestamp()
{
    volatile heartbeat_state *stat = t_thrd.heartbeat_cxt.state;

    ReplConnInfo* replconninfo = NULL;
    SpinLockAcquire(&stat->mutex);
    for (int i = 1; i < DOUBLE_MAX_REPLNODE_NUM; i++) {
        if (i >= MAX_REPLNODE_NUM)
            replconninfo = t_thrd.postmaster_cxt.CrossClusterReplConnArray[i - MAX_REPLNODE_NUM];
        else
            replconninfo = t_thrd.postmaster_cxt.ReplConnArray[i];        
        if (replconninfo == NULL) {
            continue;
        }
        stat->channel_array[i].last_reply_timestamp = 0;
    }
    SpinLockRelease(&stat->mutex);
}

/* Allocate and initialize heartbeat shared memory */
void heartbeat_shmem_init(void)
{
    bool found = false;
    t_thrd.heartbeat_cxt.state = (heartbeat_state *)ShmemInitStruct("heatbeat Shmem Data", heartbeat_shmem_size(),
                                                                    &found);
    if (!found) {
        errno_t rc = memset_s(t_thrd.heartbeat_cxt.state, heartbeat_shmem_size(), 0, heartbeat_shmem_size());
        securec_check(rc, "", "");
        SpinLockInit(&t_thrd.heartbeat_cxt.state->mutex);
    }
}

/* Initialize heartbeat state structure */
static void heartbeat_init(void)
{
    /*
     * heartbeat state should be set up already (we inherit this by fork() or
     * EXEC_BACKEND mechanism from the postmaster).
     */
    Assert(t_thrd.heartbeat_cxt.state != NULL);
    volatile heartbeat_state *stat = t_thrd.heartbeat_cxt.state;
    stat->pid = t_thrd.proc_cxt.MyProcPid;

#ifndef WIN32
    stat->lwpId = syscall(SYS_gettid);
#else
    stat->lwpId = (int)t_thrd.proc_cxt.MyProcPid;
#endif

    /* Arrange to clean up at walsender exit */
    on_shmem_exit(heartbeat_kill, 0);
}

/* Destroy the per-walsender data structure for this walsender process */
static void heartbeat_kill(int code, Datum arg)
{
    heartbeat_state *stat = t_thrd.heartbeat_cxt.state;
    errno_t rc = 0;

    Assert(stat != NULL);

    t_thrd.heartbeat_cxt.state = NULL;

    /* Mark WalSnd struct no longer in use. */
    SpinLockAcquire(&stat->mutex);
    stat->pid = 0;
    stat->lwpId = 0;

    rc = memset_s(stat->channel_array, sizeof(channel_info) * DOUBLE_MAX_REPLNODE_NUM, 0,
                  sizeof(channel_info) * DOUBLE_MAX_REPLNODE_NUM);
    securec_check_c(rc, "", "");
    SpinLockRelease(&stat->mutex);

    ereport(LOG, (errmsg("heartbeat thread shut down")));
}

static int create_client_and_server(int epollfd)
{
    /*
     * The heartbeat server and client can appear simultaneously.
     * To support dummy standby, only need add current_mode conditions.
     */
    if (t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE ||
        /* To support cascade standby, a standby instance will also start heartbeat server */
        (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE &&
         !t_thrd.postmaster_cxt.HaShmData->is_cascade_standby))
    {
        g_heartbeat_server = new (std::nothrow) HeartbeatServer(epollfd);
        if (g_heartbeat_server == NULL) {
            ereport(COMMERROR, (errmsg("Failed to cerate heartbeat server.")));
            return 1;
        }

        if (!g_heartbeat_server->Start()) {
            return 1;
        }
    }

    if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
        g_heartbeat_client = new (std::nothrow) HeartbeatClient(epollfd);
        if (g_heartbeat_client == NULL) {
            ereport(COMMERROR, (errmsg("Failed to create heartbeat client.")));
            return 1;
        }
    }

    if (g_heartbeat_server == NULL && g_heartbeat_client == NULL) {
        ereport(COMMERROR, (errmsg("There is no need to create heartbeat server and client in mode %d.",
                                   t_thrd.postmaster_cxt.HaShmData->current_mode)));

        /* Wait to change to the primary mode or standby mode. */
        pg_usleep(1000000L);
        return 1;
    }
    return 0;
}

static void destroy_client_and_server()
{
    if (g_heartbeat_server != NULL) {
        delete g_heartbeat_server;
        g_heartbeat_server = NULL;
    }

    if (g_heartbeat_client != NULL) {
        delete g_heartbeat_client;
        g_heartbeat_client = NULL;
    }
}
