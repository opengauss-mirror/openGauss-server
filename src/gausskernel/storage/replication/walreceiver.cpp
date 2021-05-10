/* -------------------------------------------------------------------------
 *
 * walreceiver.cpp
 *
 * The WAL receiver process (walreceiver) is new as of Postgres 9.0. It
 * is the process in the standby server that takes charge of receiving
 * XLOG records from a primary server during streaming replication.
 *
 * When the startup process determines that it's time to start streaming,
 * it instructs postmaster to start walreceiver. Walreceiver first connects
 * to the primary server (it will be served by a walsender process
 * in the primary server), and then keeps receiving XLOG records and
 * writing them to the disk as long as the connection is alive. As XLOG
 * records are received and flushed to disk, it updates the
 * WalRcv->receivedUpto variable in shared memory, to inform the startup
 * process of how far it can proceed with XLOG replay.
 *
 * Normal termination is by SIGTERM, which instructs the walreceiver to
 * exit(0). Emergency termination is by SIGQUIT; like any postmaster child
 * process, the walreceiver will simply abort and exit on SIGQUIT. A close
 * of the connection and a FATAL error are treated not as a crash but as
 * normal operation.
 *
 * This file contains the server-facing parts of walreceiver. The libpq-
 * specific parts are in the libpqwalreceiver module. It's loaded
 * dynamically to avoid linking the server with libpq.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/replication/walreceiver.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#ifndef WIN32
#include <syscall.h>
#endif
#include <sys/stat.h>

#include "access/xlog_internal.h"
#include "access/xlog.h"
#include "access/multi_redo_api.h"

#include "funcapi.h"
#include "nodes/execnodes.h"
#include "libpq/libpq-fe.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "replication/replicainternal.h"
#include "replication/dataqueue.h"
#include "replication/walprotocol.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"
#include "storage/copydir.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/copydir.h"
#include "storage/procarray.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "gssignal/gs_signal.h"
#include "gs_bbox.h"

#include "flock.h"
#include "postmaster/postmaster.h"
#include "hotpatch/hotpatch.h"
#include "utils/distribute_test.h"

bool wal_catchup = false;

#define NAPTIME_PER_CYCLE 1 /* max sleep time between cycles (1ms) */

#define CONFIG_BAK_FILENAME "postgresql.conf.bak"

#define WAL_DATA_LEN ((sizeof(uint32) + 1 + sizeof(XLogRecPtr)))

#define TEMP_CONF_FILE "postgresql.conf.bak"

#define MAX_PATH 256

const char *g_reserve_param[RESERVE_SIZE] = {
    "application_name",
    "archive_command",
    "audit_directory",
    "available_zone",
    "comm_control_port",
    "comm_sctp_port",
    "listen_addresses",
    "log_directory",
    "port",
    "replconninfo1",
    "replconninfo2",
    "replconninfo3",
    "replconninfo4",
    "replconninfo5",
    "replconninfo6",
    "replconninfo7",
    "replconninfo8",
    "ssl",
    "ssl_ca_file",
    "ssl_cert_file",
    "ssl_ciphers",
    "ssl_crl_file",
    "ssl_key_file",
    "ssl_renegotiation_limit",
    "ssl_cert_notify_time",
    "synchronous_standby_names",
    "local_bind_address",
    "perf_directory",
    "query_log_directory",
    "asp_log_directory",
    "streaming_router_port",
    "enable_upsert_to_merge",
    "archive_dest",
#ifndef ENABLE_MULTIPLE_NODES
    "recovery_min_apply_delay",
    "archive_mode",
    "sync_config_strategy"
#else
    NULL,
    NULL,
    NULL
#endif
};

const WalReceiverFunc WalReceiverFuncTable[] = {
    { libpqrcv_connect, libpqrcv_receive, libpqrcv_send, libpqrcv_disconnect },
    { obs_connect, obs_receive, obs_send, obs_disconnect },
};

const int FUNC_LIBPQ_IDX = 0;
const int FUNC_OBS_IDX = 1;
#define GET_FUNC_IDX \
    (t_thrd.walreceiverfuncs_cxt.WalRcv->conn_target == REPCONNTARGET_OBS ? FUNC_OBS_IDX : FUNC_LIBPQ_IDX)

/* Prototypes for private functions */
static void EnableWalRcvImmediateExit(void);
static void DisableWalRcvImmediateExit(void);
static void WalRcvDie(int code, Datum arg);
static void XLogWalRcvDataPageReplication(char *buf, Size len);
static void XLogWalRcvProcessMsg(unsigned char type, char *buf, Size len);
static void XLogWalRcvReceive(char *buf, Size nbytes, XLogRecPtr recptr);
static void XLogWalRcvReceiveInBuf(char *buf, Size nbytes, XLogRecPtr recptr);
static void XLogWalRcvSendHSFeedback(void);
static void XLogWalRcvSendSwitchRequest(void);
static void WalDataRcvReceive(char *buf, Size nbytes, XLogRecPtr recptr);
static void ProcessSwitchResponse(int code);
static void ProcessWalSndrMessage(XLogRecPtr *walEnd, TimestampTz sendTime);

static void ProcessKeepaliveMessage(PrimaryKeepaliveMessage *keepalive);
static void ProcessRmXLogMessage(RmXLogMessage *rmXLogMessage);
static void ProcessEndXLogMessage(EndXLogMessage *endXLogMessage);
static void ProcessWalHeaderMessage(WalDataMessageHeader *msghdr);
static void ProcessWalDataHeaderMessage(WalDataPageMessageHeader *msghdr);
const char *wal_get_rebuild_reason_string(HaRebuildReason reason);
static void wal_get_ha_rebuild_reason(char *buildReason, ServerMode local_role, bool isRunning);
Datum pg_stat_get_wal_receiver(PG_FUNCTION_ARGS);
/* Signal handlers */
static void WalRcvSigHupHandler(SIGNAL_ARGS);
static void WalRcvShutdownHandler(SIGNAL_ARGS);
static void WalRcvQuickDieHandler(SIGNAL_ARGS);
static void sigusr1_handler(SIGNAL_ARGS);
static void sigusr2_handler(SIGNAL_ARGS);
static void ConfigFileTimer(void);
static bool ProcessConfigFileMessage(char *buf, Size len);
static void firstSynchStandbyFile(void);

static TimestampTz GetHeartbeatLastReplyTimestamp();
static bool WalRecCheckTimeOut(TimestampTz nowtime, TimestampTz last_recv_timestamp, bool ping_sent);
static void WalRcvRefreshPercentCountStartLsn(XLogRecPtr currentMaxLsn, XLogRecPtr currentDoneLsn);
static void ProcessArchiveXlogMessage(const ArchiveXlogMessage* archive_xlog_message);
static void ProcessStandbyArchiveXlogMessage(const ArchiveXlogMessage* archive_xlog_message);
static void WalRecvSendArchiveXlogResponse();
static void WalRecvSendArchiveXlogResult2Standby();
static void SendArchiveStatus(bool status);
static void ProcessArchiveStatusResponse(ArchiveStatusResponseMessage* response);
static void InitArchiveStartPoint();
static bool CheckXlogNameValid(char* xlog);
int XlogNameCmp(const void* a, const void* b);
void ProcessWalRcvInterrupts(void)
{
    /*
     * Although walreceiver interrupt handling doesn't use the same scheme as
     * regular backends, call CHECK_FOR_INTERRUPTS() to make sure we receive
     * any incoming signals on Win32.
     */
    CHECK_FOR_INTERRUPTS();

    if (t_thrd.walreceiver_cxt.got_SIGTERM) {
        t_thrd.walreceiver_cxt.WalRcvImmediateInterruptOK = false;
        ereport(FATAL, (errcode(ERRCODE_ADMIN_SHUTDOWN),
                        errmsg("terminating walreceiver process due to administrator command")));
    }
}

static void EnableWalRcvImmediateExit(void)
{
    t_thrd.walreceiver_cxt.WalRcvImmediateInterruptOK = true;
    ProcessWalRcvInterrupts();
}

static void DisableWalRcvImmediateExit(void)
{
    t_thrd.walreceiver_cxt.WalRcvImmediateInterruptOK = false;
    ProcessWalRcvInterrupts();
}

void wakeupWalRcvWriter()
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    if (walrcv->walrcvWriterLatch != NULL)
        SetLatch(walrcv->walrcvWriterLatch);
    SpinLockRelease(&walrcv->mutex);
}

static void walRcvCtlBlockInit()
{
    char *buf = NULL;
    int64 recBufferSize = g_instance.attr.attr_storage.WalReceiverBufSize * 1024;
    size_t len = offsetof(WalRcvCtlBlock, walReceiverBuffer) + recBufferSize;
    errno_t rc = 0;

    Assert(t_thrd.walreceiver_cxt.walRcvCtlBlock == NULL);
    buf = (char *)MemoryContextAlloc(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), len);

    if (buf == NULL) {
        ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }

    rc = memset_s(buf, sizeof(WalRcvCtlBlock), 0, sizeof(WalRcvCtlBlock));
    securec_check_c(rc, "\0", "\0");

    t_thrd.walreceiver_cxt.walRcvCtlBlock = (WalRcvCtlBlock *)buf;
    if (BBOX_BLACKLIST_WALREC_CTL_BLOCK) {
        bbox_blacklist_add(WALRECIVER_CTL_BLOCK, t_thrd.walreceiver_cxt.walRcvCtlBlock, len);
    }

    SpinLockInit(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
}

static void walRcvCtlBlockFini()
{
    if (BBOX_BLACKLIST_WALREC_CTL_BLOCK) {
        bbox_blacklist_remove(WALRECIVER_CTL_BLOCK, t_thrd.walreceiver_cxt.walRcvCtlBlock);
    }

    pfree(t_thrd.walreceiver_cxt.walRcvCtlBlock);
    t_thrd.walreceiver_cxt.walRcvCtlBlock = NULL;
}

/*
 * Clean up data in receive buffer.
 * This function should be called on thread exit.
 */
void walRcvDataCleanup()
{
    while (WalDataRcvWrite() > 0) {
    };
}

bool walRcvCtlBlockIsEmpty(void)
{
    volatile WalRcvCtlBlock *walrcb = NULL;

    LWLockAcquire(WALWriteLock, LW_EXCLUSIVE);
    walrcb = getCurrentWalRcvCtlBlock();

    if (walrcb == NULL) {
        LWLockRelease(WALWriteLock);
        return true;
    }

    bool retState = false;

    SpinLockAcquire(&walrcb->mutex);
    if (IsExtremeRedo()) {
        if (walrcb->walFreeOffset == walrcb->walReadOffset) {
            retState = true;
        }
    } else {
        if (walrcb->walFreeOffset == walrcb->walWriteOffset) {
            retState = true;
        }
    }

    SpinLockRelease(&walrcb->mutex);
    LWLockRelease(WALWriteLock);
    return retState;
}

void setObsArchLatch(const Latch* latch)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    walrcv->obsArchLatch = (Latch *)latch;
    SpinLockRelease(&walrcv->mutex);
}

void SetStandbyArchLatch(const Latch* latch)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    walrcv->arch_latch = (Latch *)latch;
    SpinLockRelease(&walrcv->mutex);
}

static void wakeupObsArchLatch()
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    if (walrcv->obsArchLatch != NULL) {
        SetLatch(walrcv->obsArchLatch);
    }
    SpinLockRelease(&walrcv->mutex);
}

static void wakeupArchLatch()
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    if (walrcv->obsArchLatch != NULL) {
        SetLatch(walrcv->arch_latch);
    }
    SpinLockRelease(&walrcv->mutex);
}

void RefuseConnect()
{
    WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    knl_g_disconn_node_context_data disconn_node =
        g_instance.comm_cxt.localinfo_cxt.disable_conn_node.disable_conn_node_data;

    if (disconn_node.conn_mode == POLLING_CONNECTION) {
        return;
    }

    if (disconn_node.conn_mode == SPECIFY_CONNECTION &&
        strcmp(disconn_node.disable_conn_node_host, (char *)walrcv->conn_channel.remotehost) == 0 &&
        disconn_node.disable_conn_node_port == walrcv->conn_channel.remoteport) {
        return;
    }
    ereport(FATAL,
            (errmsg("Refuse WAL streaming, connection mode is %d, connertion IP is %s:%d\n", disconn_node.conn_mode,
                    disconn_node.disable_conn_node_host, disconn_node.disable_conn_node_port)));
}

void WalRcvrProcessData(TimestampTz *last_recv_timestamp, bool *ping_sent)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    unsigned char type;
    char *buf = NULL;
    int len;

#ifdef ENABLE_DISTRIBUTE_TEST
    if (TEST_STUB(DN_WALRECEIVE_MAINLOOP, stub_sleep_emit)) {
        ereport(get_distribute_test_param()->elevel,
                (errmsg("sleep_emit happen during WalReceiverMain  time:%ds, stub_name:%s",
                        get_distribute_test_param()->sleep_time, get_distribute_test_param()->test_stub_name)));
    }
#endif

    /*
     * Emergency bailout if postmaster has died.  This is to avoid the
     * necessity for manual cleanup of all postmaster children.
     */
    if (!PostmasterIsAlive())
        gs_thread_exit(1);
    if(walrcv->conn_target != REPCONNTARGET_OBS)
        RefuseConnect();

    /*
     * Exit walreceiver if we're not in recovery. This should not happen,
     * but cross-check the status here.
     */
    if (!RecoveryInProgress())
        ereport(FATAL, (errmsg("cannot continue WAL streaming, recovery has already ended")));

    /* Process any requests or signals received recently */
    ProcessWalRcvInterrupts();

    if (t_thrd.walreceiver_cxt.got_SIGHUP) {
        t_thrd.walreceiver_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }

    volatile unsigned int *pitr_task_status = &g_instance.archive_obs_cxt.pitr_task_status;
    if (unlikely(pg_atomic_read_u32(pitr_task_status) == PITR_TASK_DONE)) {
        WalRecvSendArchiveXlogResponse();
        pg_memory_barrier();
        pg_atomic_write_u32(pitr_task_status, PITR_TASK_NONE);
    }

    /* send archive status to primary */
    if (g_instance.archive_standby_cxt.need_to_send_archive_status) {
        g_instance.archive_standby_cxt.need_to_send_archive_status = false;
        SendArchiveStatus(g_instance.archive_standby_cxt.archive_enabled);
    }

    /* response the result of archive to primary */
    volatile unsigned int* arch_task_status = &g_instance.archive_standby_cxt.arch_task_status;
    if (unlikely(pg_atomic_read_u32(arch_task_status) == ARCH_TASK_DONE)) {
        WalRecvSendArchiveXlogResult2Standby();
        pg_memory_barrier();
        pg_atomic_write_u32(arch_task_status, ARCH_TASK_NONE);
    }

    if (!WalRcvWriterInProgress())
        ereport(FATAL, (errmsg("terminating walreceiver process due to the death of walrcvwriter")));

    if (t_thrd.walreceiver_cxt.start_switchover && walrcv->conn_target != REPCONNTARGET_OBS) {
        t_thrd.walreceiver_cxt.start_switchover = false;
        XLogWalRcvSendSwitchRequest();
    }

    /* Wait a while for data to arrive */
    if ((WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_receive(NAPTIME_PER_CYCLE, &type, &buf, &len)) {
        *last_recv_timestamp = GetCurrentTimestamp();
        *ping_sent = false;
        /* Accept the received data, and process it */
        XLogWalRcvProcessMsg(type, buf, len);

        /* Receive any more data we can without sleeping */
        while ((t_thrd.walreceiver_cxt.start_switchover == false) &&
            (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_receive(0, &type, &buf, &len) ) {
            *last_recv_timestamp = GetCurrentTimestamp();
            *ping_sent = false;
            XLogWalRcvProcessMsg(type, buf, len);
        }

        /* Let the master know that we received some data. */
        if(walrcv->conn_target != REPCONNTARGET_OBS)
            XLogWalRcvSendReply(false, false);
    } else if(walrcv->conn_target != REPCONNTARGET_OBS) {
        /*
         * We didn't receive anything new. If we haven't heard anything
         * from the server for more than u_sess->attr.attr_storage.wal_receiver_timeout / 2,
         * ping the server. Also, if it's been longer than
         * u_sess->attr.attr_storage.wal_receiver_status_interval since the last update we sent,
         * send a status update to the master anyway, to report any
         * progress in applying WAL.
         */
        TimestampTz nowtime = GetCurrentTimestamp();
        bool requestReply = WalRecCheckTimeOut(nowtime, *last_recv_timestamp, *ping_sent);
        if (requestReply) {
            *ping_sent = true;
            *last_recv_timestamp = nowtime;
        }

        XLogWalRcvSendReply(requestReply, requestReply);
        XLogWalRcvSendHSFeedback();
    }
    ConfigFileTimer();
}

/* Main entry point for walreceiver process */
void WalReceiverMain(void)
{
    char conninfo[MAXCONNINFO];
    char slotname[NAMEDATALEN];
    XLogRecPtr startpoint;
    TimestampTz last_recv_timestamp;
    bool ping_sent = false;
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    int channel_identifier = 0;
    int nRet = 0;
    errno_t rc = 0;
    uint32 isRedoFinish;

    t_thrd.walreceiver_cxt.last_sendfilereply_timestamp = GetCurrentTimestamp();
    t_thrd.walreceiver_cxt.standby_config_modify_time = time(NULL);
    isRedoFinish = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.isRedoFinish));
    knl_g_set_redo_finish_status(isRedoFinish | REDO_FINISH_STATUS_CM);
    ereport(LOG, (errmsg("set knl_g_set_redo_finish_status_CM to true when connecting to the primary")));

    /*
     * WalRcv should be set up already (if we are a backend, we inherit this
     * by fork() or EXEC_BACKEND mechanism from the postmaster).
     */
    Assert(walrcv != NULL);

    ereport(LOG, (errmsg("walreceiver thread started")));

    /* Initialize walrcv buffer for walreceive optimization */
    walRcvCtlBlockInit();

    load_server_mode();

    /*
     * Mark walreceiver as running in shared memory.
     *
     * Do this as early as possible, so that if we fail later on, we'll set
     * state to STOPPED. If we die before this, the startup process will keep
     * waiting for us to start up, until it times out.
     */
    SpinLockAcquire(&walrcv->mutex);
    Assert(walrcv->pid == 0);
    switch (walrcv->walRcvState) {
        case WALRCV_STOPPING:
            /* If we've already been requested to stop, don't start up. */
            walrcv->walRcvState = WALRCV_STOPPED;
            // fall through
        case WALRCV_STOPPED:
            SpinLockRelease(&walrcv->mutex);
            ereport(WARNING, (errmsg("walreceiver requested to stop when starting up.")));
            KillWalRcvWriter();
            proc_exit(1);
            break;

        case WALRCV_STARTING:
            /* The usual case */
            break;

        case WALRCV_RUNNING:
            /* Shouldn't happen */
            ereport(PANIC, (errmsg("walreceiver still running according to shared memory state")));
    }
    /* Advertise our PID so that the startup process can kill us */
    if (walrcv->conn_target == REPCONNTARGET_PRIMARY || walrcv->conn_target == REPCONNTARGET_OBS)
        walrcv->node_state = NODESTATE_NORMAL;
    walrcv->pid = t_thrd.proc_cxt.MyProcPid;
    walrcv->obsArchLatch = NULL;
#ifndef WIN32
    walrcv->lwpId = syscall(SYS_gettid);
#else
    walrcv->lwpId = (int)t_thrd.proc_cxt.MyProcPid;
#endif
    walrcv->isRuning = false;
    walrcv->walRcvState = WALRCV_RUNNING;

    rc = memset_s(slotname, NAMEDATALEN, 0, NAMEDATALEN);
    securec_check(rc, "\0", "\0");

    rc = memset_s(conninfo, MAXCONNINFO, 0, MAXCONNINFO);
    securec_check(rc, "\0", "\0");

    /* Fetch information required to start streaming */
    rc = strncpy_s(conninfo, MAXCONNINFO, (char *)walrcv->conninfo, MAXCONNINFO - 1);
    securec_check(rc, "\0", "\0");
    rc = strncpy_s(slotname, NAMEDATALEN, (char *)walrcv->slotname, NAMEDATALEN - 1);
    securec_check(rc, "\0", "\0");

    startpoint = walrcv->receiveStart;

    /* Initialise to a sanish value */
    walrcv->lastMsgSendTime = walrcv->lastMsgReceiptTime = walrcv->latestWalEndTime = GetCurrentTimestamp();

    WalRcvCtlAcquireExitLock();
    walrcv->walRcvCtlBlock = t_thrd.walreceiver_cxt.walRcvCtlBlock;
    WalRcvCtlReleaseExitLock();
    if(walrcv->conn_target != REPCONNTARGET_OBS) {
        t_thrd.walreceiver_cxt.AmWalReceiverForFailover =
            (walrcv->conn_target == REPCONNTARGET_DUMMYSTANDBY || walrcv->conn_target == REPCONNTARGET_STANDBY) ? true
                                                                                                                : false;
        t_thrd.walreceiver_cxt.AmWalReceiverForStandby = (walrcv->conn_target == REPCONNTARGET_STANDBY) ? true : false;
        SpinLockRelease(&walrcv->mutex);
        /* using localport for channel identifier */
        if (!t_thrd.walreceiver_cxt.AmWalReceiverForStandby) {
            volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
            SpinLockAcquire(&hashmdata->mutex);
            int walreplindex = hashmdata->current_repl;
            SpinLockRelease(&hashmdata->mutex);

            if (t_thrd.postmaster_cxt.ReplConnArray[walreplindex])
                channel_identifier = t_thrd.postmaster_cxt.ReplConnArray[walreplindex]->localport;
        }
    }
    else {
        SpinLockRelease(&walrcv->mutex);
    }
    /* Arrange to clean up at walreceiver exit */
    on_shmem_exit(WalRcvDie, 0);

    /* Reset some signals that are accepted by postmaster but not here */
    (void)gspqsignal(SIGHUP, WalRcvSigHupHandler); /* set flag to read config file */
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, WalRcvShutdownHandler); /* request shutdown */
    (void)gspqsignal(SIGQUIT, WalRcvQuickDieHandler); /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, sigusr1_handler);
    (void)gspqsignal(SIGUSR2, sigusr2_handler);

    /* Reset some signals that are accepted by postmaster but not here */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    /*
     * Create a resource owner to keep track of our resources (not clear that
     * we need this, but may as well have one).
     */
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Wal Receiver", MEMORY_CONTEXT_STORAGE);

    /* Unblock signals (they were blocked when the postmaster forked us) */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
    if(walrcv->conn_target != REPCONNTARGET_OBS)
        SetWalRcvDummyStandbySyncPercent(0);
    
    t_thrd.xlog_cxt.ThisTimeLineID = GetRecoveryTargetTLI();
    /* Establish the connection to the primary for XLOG streaming */
    EnableWalRcvImmediateExit();

    WalReceiverFuncTable[GET_FUNC_IDX].walrcv_connect(conninfo, &startpoint, slotname[0] != '\0' ? slotname : NULL,
                                                      channel_identifier);
    DisableWalRcvImmediateExit();

    if (GetWalRcvDummyStandbySyncPercent() == SYNC_DUMMY_STANDBY_END && walrcv->conn_target != REPCONNTARGET_OBS) {
        Assert(t_thrd.walreceiver_cxt.AmWalReceiverForFailover == true);
        ereport(LOG, (errmsg("Secondary Standby has no xlog")));
    }

    rc = memset_s(t_thrd.walreceiver_cxt.reply_message, sizeof(StandbyReplyMessage), 0, sizeof(StandbyReplyMessage));
    securec_check(rc, "\0", "\0");
    rc = memset_s(t_thrd.walreceiver_cxt.feedback_message, sizeof(StandbyHSFeedbackMessage), 0,
                  sizeof(StandbyHSFeedbackMessage));
    securec_check(rc, "\0", "\0");
    ereport(LOG, (errmsg("start replication at start point %X/%X", (uint32)(startpoint >> 32), (uint32)startpoint)));

    last_recv_timestamp = GetCurrentTimestamp();

    if (t_thrd.proc_cxt.DataDir) {
        nRet = snprintf_s(t_thrd.walreceiver_cxt.gucconf_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf",
                          t_thrd.proc_cxt.DataDir);
        securec_check_ss(nRet, "\0", "\0");

        nRet = snprintf_s(t_thrd.walreceiver_cxt.temp_guc_conf_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s",
                          t_thrd.proc_cxt.DataDir, TEMP_CONF_FILE);
        securec_check_ss(nRet, "\0", "\0");

        nRet = snprintf_s(t_thrd.walreceiver_cxt.gucconf_lock_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf.lock",
                          t_thrd.proc_cxt.DataDir);
        securec_check_ss(nRet, "\0", "\0");
    }

    SpinLockAcquire(&walrcv->mutex);
    walrcv->isRuning = true;
    walrcv->local_write_pos.queueid = 0;
    walrcv->local_write_pos.queueoff = 0;
    SpinLockRelease(&walrcv->mutex);

    if (!dummyStandbyMode) {
        SpinLockAcquire(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
        t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->writePtr =
            t_thrd.walreceiver_cxt.walRcvCtlBlock->flushPtr = GetXLogReplayRecPtr(NULL);
        SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
    } else {
        SpinLockAcquire(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
        t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->writePtr =
            t_thrd.walreceiver_cxt.walRcvCtlBlock->flushPtr = startpoint;
        SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
    }

    /*
     * Synchronize standby's configure file once the HA build successfully.
     *
     * Note: If switchover in one hour, and there is no parameter is reloaded,
     * the parameters set by client will be disabled. So we should do this.
     */
    if(walrcv->conn_target != REPCONNTARGET_OBS) {
        firstSynchStandbyFile();
        set_disable_conn_mode();
    }

    knl_g_set_redo_finish_status(REDO_FINISH_STATUS_LOCAL);
    ereport(LOG, (errmsg("set knl_g_set_redo_finish_status to false when connecting to the primary")));
    /*
     * Prevent the effect of the last wallreceiver connection.
     */
    InitHeartbeatTimestamp();

    /* Loop until end-of-streaming or error */
    for (;;) {
        WalRcvrProcessData(&last_recv_timestamp, &ping_sent);
    }
}

static TimestampTz GetHeartbeatLastReplyTimestamp()
{
    int replindex;
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    SpinLockAcquire(&hashmdata->mutex);
    replindex = hashmdata->current_repl;
    SpinLockRelease(&hashmdata->mutex);

    return get_last_reply_timestamp(replindex);
}

/* return timeout time */
static inline TimestampTz CalculateTimeout(TimestampTz last_reply_time)
{
    return TimestampTzPlusMilliseconds(last_reply_time, u_sess->attr.attr_storage.wal_receiver_timeout / 2);
}

/*
 * Check if time since last receive from primary has reached the
 * configured limit. If we didn't receive anything new for half of receiver
 * replication timeout, need ping the server.
 *
 * NB: the timeout stategy is different from the sender due to ping_sent,
 * if pint_sent is set true,  abnormal heartbeat for (wal_receiver_timeout / 2) will cause timeout.
 */
static bool WalRecCheckTimeOut(TimestampTz nowtime, TimestampTz last_recv_timestamp, bool ping_sent)
{
    bool requestReply = false;
    TimestampTz heartbeat = GetHeartbeatLastReplyTimestamp();
    TimestampTz calculateTime = CalculateTimeout(heartbeat);
    TimestampTz hbValid = TimestampTzPlusMilliseconds(heartbeat, u_sess->attr.attr_storage.wal_receiver_timeout * 2);
    /*
     * The host locally records the last communication time of the standby,
     * when the time exceeds heartbeat_ Timeout: if the heartbeat message is not received,
     * the heartbeat timeout will be triggered and walsender will exit.
     * In switchover scenario, if the host exits the heartbeat thread,
     * the standby will exit the walkreceiver thread. This causes switchover to fail,
     * so heartbeat timeout is not judged during switchover.
     */
    if (timestamptz_cmp_internal(nowtime, calculateTime) >= 0 && timestamptz_cmp_internal(hbValid, nowtime) >= 0 &&
        (t_thrd.walreceiverfuncs_cxt.WalRcv != NULL &&
        t_thrd.walreceiverfuncs_cxt.WalRcv->node_state != NODESTATE_STANDBY_WAITING)) {
        ereport(ERROR, (errmsg(
            "terminating walreceiver due to heartbeat timeout,now time(%s) last heartbeat time(%s) calculateTime(%s)",
            timestamptz_to_str(nowtime), timestamptz_to_str(heartbeat), timestamptz_to_str(calculateTime))));
    }

    /* don't bail out if we're doing something that doesn't require timeouts */
    if (u_sess->attr.attr_storage.wal_receiver_timeout <= 0) {
        return requestReply;
    }

    /*
     * Use static last_reply_time to avoid call GetHeartbeatLastReplyTimestamp frequently
     * when last_recv_timestamp has meet the timeout condition
     * but last heartbeat time doesn't.
     */
    static TimestampTz last_reply_time = last_recv_timestamp;
    if (timestamptz_cmp_internal(last_recv_timestamp, last_reply_time) > 0) {
        last_reply_time = last_recv_timestamp;
    }

    TimestampTz timeout = CalculateTimeout(last_reply_time);
    if (nowtime < timeout) {
        return requestReply;
    }

    /* If heartbeat newer, use heartbeat to recalculate timeout. */
    if (timestamptz_cmp_internal(heartbeat, last_reply_time) > 0) {
        last_reply_time = heartbeat;
        timeout = CalculateTimeout(last_reply_time);
    }

    /*
     * We didn't receive anything new, for half of receiver
     * replication timeout. Ping the server.
     */
    if (nowtime >= timeout) {
        WalReplicationTimestampInfo tpInfo;
        if (log_min_messages <= DEBUG2 || client_min_messages <= DEBUG2) {
            WalReplicationTimestampToString(&tpInfo, nowtime, timeout, last_recv_timestamp, heartbeat);
            ereport(DEBUG2,
                (errmsg("now time(%s) timeout time(%s) last recv time(%s), heartbeat time(%s), ping_sent(%d)",
                tpInfo.nowTimeStamp, tpInfo.timeoutStamp, tpInfo.lastRecStamp, tpInfo.heartbeatStamp, ping_sent)));
        }
        if (!ping_sent) {
            requestReply = true;
        } else {
            knl_g_set_redo_finish_status(0);
            ereport(LOG, (errmsg("set knl_g_set_redo_finish_status to false in WalRecCheckTimeOut")));
            if (log_min_messages <= ERROR || client_min_messages <= ERROR) {
                WalReplicationTimestampToString(&tpInfo, nowtime, timeout, last_recv_timestamp, heartbeat);
                ereport(ERROR, (errcode(ERRCODE_CONNECTION_TIMED_OUT), errmsg("terminating walreceiver due to timeout "
                    "now time(%s) timeout time(%s) last recv time(%s) heartbeat time(%s)",
                    tpInfo.nowTimeStamp, tpInfo.timeoutStamp, tpInfo.lastRecStamp, tpInfo.heartbeatStamp)));
            }
        }
    }
    return requestReply;
}

/*
 * Mark us as STOPPED in proc at exit.
 */
static void WalRcvDie(int code, Datum arg)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    /*
     * Shutdown WalRcvWriter thread, clear the data receive buffer.
     * Ensure that all WAL records received are flushed to disk.
     */
    KillWalRcvWriter();
    
    /* we have to set REDO_FINISH_STATUS_LOCAL to false here, or there will be problems in this case:
       extremRTO is on, and DN received force finish signal, if cleanup is blocked, the force finish 
       signal will be ignored!
    */
    knl_g_clear_local_redo_finish_status();
    ereport(LOG, (errmsg("set local_redo_finish_status to false in WalRcvDie")));

    walRcvDataCleanup();

    LWLockAcquire(WALWriteLock, LW_EXCLUSIVE);
    SpinLockAcquire(&walrcv->mutex);
    Assert(walrcv->walRcvState == WALRCV_RUNNING || walrcv->walRcvState == WALRCV_STOPPING);
    walrcv->walRcvState = WALRCV_STOPPED;
    walrcv->pid = 0;
    walrcv->lwpId = 0;
    walrcv->isRuning = false;
    if (walrcv->walRcvCtlBlock != NULL)
        walrcv->walRcvCtlBlock = NULL;
    SpinLockRelease(&walrcv->mutex);

    WalRcvCtlAcquireExitLock();
    walRcvCtlBlockFini();
    WalRcvCtlReleaseExitLock();
    LWLockRelease(WALWriteLock);

    /* Terminate the connection gracefully. */
    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();

    /* Wake up the startup process to notice promptly that we're gone */
    WakeupRecovery();

    if (t_thrd.libwalreceiver_cxt.recvBuf != NULL) {
        PQfreemem(t_thrd.libwalreceiver_cxt.recvBuf);
        t_thrd.libwalreceiver_cxt.recvBuf = NULL;
    }

    /* reset conn_channel */
    errno_t rc = memset_s((void*)&walrcv->conn_channel,
        sizeof(walrcv->conn_channel), 0, sizeof(walrcv->conn_channel));
    securec_check_c(rc, "\0", "\0");

    ereport(LOG, (errmsg("walreceiver thread shut down")));
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void WalRcvSigHupHandler(SIGNAL_ARGS)
{
    t_thrd.walreceiver_cxt.got_SIGHUP = true;
}

/* SIGTERM: set flag for main loop, or shutdown immediately if safe */
static void WalRcvShutdownHandler(SIGNAL_ARGS)
{
    t_thrd.walreceiver_cxt.got_SIGTERM = true;
}

/*
 * WalRcvQuickDieHandler() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm, so we need to stop what we're doing and
 * exit.
 */
static void WalRcvQuickDieHandler(SIGNAL_ARGS)
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

    /*
     * Note we do exit(2) not exit(0).	This is to force the postmaster into a
     * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
     * backend.  This is necessary precisely because we don't clean up our
     * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
     * should ensure the postmaster sees this as a crash, too, but no harm in
     * being doubly sure.)
     */
    exit(2);
}

/*
 * handle signal conditions from other processes
 */
static void sigusr1_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    if (t_thrd.walreceiverfuncs_cxt.WalRcv &&
        t_thrd.walreceiverfuncs_cxt.WalRcv->node_state >= NODESTATE_SMART_DEMOTE_REQUEST &&
        t_thrd.walreceiverfuncs_cxt.WalRcv->node_state <= NODESTATE_FAST_DEMOTE_REQUEST) {
        /* Tell walreceiver process to start switchover */
        t_thrd.walreceiver_cxt.start_switchover = true;
    }

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);

    errno = save_errno;
}

static void sigusr2_handler(SIGNAL_ARGS) {
    /* get sigusr2 */
    int save_errno = errno;
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
    /*
     * sending archive status to the primary in this step
     */
    g_instance.archive_standby_cxt.need_to_send_archive_status = true;
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    errno = save_errno;
}

/* Wal receiver is shut down? */
bool WalRcvIsShutdown(void)
{
    return t_thrd.walreceiver_cxt.got_SIGTERM;
}

static void XLogWalRcvDataPageReplication(char *buf, Size len)
{
    WalDataPageMessageHeader msghdr;
    Assert(true == g_instance.attr.attr_storage.enable_mix_replication);

    if (!g_instance.attr.attr_storage.enable_mix_replication) {
        ereport(PANIC, (errmsg("WAL streaming isn't employed to sync all the replication data log.")));
    }
    if (len < sizeof(WalDataPageMessageHeader)) {
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                        errmsg_internal("invalid wal data page message received from primary")));
    }

    /* memcpy is required here for alignment reasons */
    error_t rc = memcpy_s(&msghdr, sizeof(WalDataPageMessageHeader), buf, sizeof(WalDataPageMessageHeader));
    securec_check(rc, "\0", "\0");

    ProcessWalDataHeaderMessage(&msghdr);

    buf += sizeof(WalDataPageMessageHeader);
    len -= sizeof(WalDataPageMessageHeader);

    if (len > WS_MAX_DATA_QUEUE_SIZE) {
        Assert(false);
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                        errmsg_internal(
                            "unexpected wal data size %lu bytes exceeds the max receiving data queue size %u bytes",
                            len, WS_MAX_DATA_QUEUE_SIZE)));
    }
    if (u_sess->attr.attr_storage.HaModuleDebug) {
        WSDataRcvCheck(buf, len);
    }
    WalDataRcvReceive(buf, len, 0);
}

/*
 * Accept the message from XLOG stream, and process it.
 */
static void XLogWalRcvProcessMsg(unsigned char type, char *buf, Size len)
{
    errno_t errorno = EOK;

    ereport(DEBUG5, (errmsg("received wal message type: %c", type)));

    switch (type) {
        case 'e': /* dummy standby sendxlog end. */
        {
            EndXLogMessage endXLogMessage;
            CHECK_MSG_SIZE(len, EndXLogMessage, "invalid EndXLogMessage message received from Secondary Standby");
            /* memcpy is required here for alignment reasons */
            errorno = memcpy_s(&endXLogMessage, sizeof(EndXLogMessage), buf, sizeof(EndXLogMessage));
            securec_check(errorno, "\0", "\0");
            ProcessEndXLogMessage(&endXLogMessage);
            break;
        }
        case 'w': /* WAL records */
        {
            WalDataMessageHeader msghdr;
            if (len < sizeof(WalDataMessageHeader))
                ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                    errmsg_internal("invalid WAL message received from primary")));
            /* memcpy is required here for alignment reasons */
            errorno = memcpy_s(&msghdr, sizeof(WalDataMessageHeader), buf, sizeof(WalDataMessageHeader));
            securec_check(errorno, "", "");

            ProcessWalHeaderMessage(&msghdr);

            buf += sizeof(WalDataMessageHeader);
            len -= sizeof(WalDataMessageHeader);
            if (IsExtremeRedo()) {
                XLogWalRcvReceiveInBuf(buf, len, msghdr.dataStart);
            } else {
                XLogWalRcvReceive(buf, len, msghdr.dataStart);
            }
            break;
        }
        case 'd': /* Data page replication for the logical xlog */
        {
            XLogWalRcvDataPageReplication(buf, len);
            break;
        }
        case 'k': /* Keepalive */
        {
            CHECK_MSG_SIZE(len, PrimaryKeepaliveMessage, "invalid keepalive message received from primary");
            PrimaryKeepaliveMessage keepalive;
            /* memcpy is required here for alignment reasons */
            errorno = memcpy_s(&keepalive, sizeof(PrimaryKeepaliveMessage), buf, sizeof(PrimaryKeepaliveMessage));
            securec_check(errorno, "\0", "\0");

            ProcessKeepaliveMessage(&keepalive);

            /* If the primary requested a reply, send one immediately */
            if (keepalive.replyRequested)
                XLogWalRcvSendReply(true, false);
            break;
        }
        case 'p': /* Promote standby */
        {
            PrimarySwitchResponseMessage response;
            CHECK_MSG_SIZE(len, PrimarySwitchResponseMessage, "invalid switchover response message received from primary")
           /* memcpy is required here for alignment reasons */
            errorno = memcpy_s(&response, sizeof(PrimarySwitchResponseMessage), buf,
                               sizeof(PrimarySwitchResponseMessage));
            securec_check(errorno, "\0", "\0");
            ProcessWalSndrMessage(&response.walEnd, response.sendTime);

            ereport(LOG, (errmsg("received switchover response message from primary")));
            ProcessSwitchResponse(response.switchResponse);
            break;
        }
        case 'm': /* config file */
        {
            if (len < sizeof(ConfigModifyTimeMessage)) {
                ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg_internal("invalid config file message")));
            }
            ConfigModifyTimeMessage primary_config_file;
            /* memcpy is required here for alignment reasons */
            errorno = memcpy_s(&primary_config_file, sizeof(ConfigModifyTimeMessage), buf,
                               sizeof(ConfigModifyTimeMessage));
            securec_check(errorno, "\0", "\0");
            t_thrd.walreceiver_cxt.Primary_config_modify_time = primary_config_file.config_modify_time;
            buf += sizeof(ConfigModifyTimeMessage);
            len -= sizeof(ConfigModifyTimeMessage);
            ereport(LOG, (errmsg("walreceiver received gaussdb config file size: %lu", len)));
            if (true != ProcessConfigFileMessage(buf, len)) {
                ereport(LOG, (errmsg("walreceiver update config file failed")));
            }
            break;
        }
        case 'x': /* rm xlog */
        {
            RmXLogMessage rmXLogMessage;
            CHECK_MSG_SIZE(len, RmXLogMessage, "invalid RmXLog message received from primary");
            /* memcpy is required here for alignment reasons */
            errorno = memcpy_s(&rmXLogMessage, sizeof(RmXLogMessage), buf, sizeof(RmXLogMessage));
            securec_check(errorno, "\0", "\0");
            ProcessRmXLogMessage(&rmXLogMessage);
            break;
        }
        case 'a': /* pitr archive xlog */
        {
            ArchiveXlogMessage archiveXLogMessage;
            CHECK_MSG_SIZE(len, ArchiveXlogMessage, "invalid ArchiveXlogMessage message received from primary");
            /* memcpy is required here for alignment reasons */
            errorno = memcpy_s(&archiveXLogMessage, sizeof(ArchiveXlogMessage), buf, sizeof(ArchiveXlogMessage));
            securec_check(errorno, "\0", "\0");
            ProcessArchiveXlogMessage(&archiveXLogMessage);
            break;
        }
        case 'n' : /* process the archive task message sent by primary */
        {
            ArchiveXlogMessage archiveXLogMessage;
            CHECK_MSG_SIZE(len, ArchiveXlogMessage, "invalid ArchiveXlogMessage message received from primary");
            /* memcpy is required here for alignment reasons */
            errorno = memcpy_s(&archiveXLogMessage, sizeof(ArchiveXlogMessage), buf, sizeof(ArchiveXlogMessage));
            securec_check(errorno, "\0", "\0");
            ProcessStandbyArchiveXlogMessage(&archiveXLogMessage);
            break;
        }
        case 'S': /* send the status of the archive thread on standby */
        {
            ArchiveStatusResponseMessage response;
            CHECK_MSG_SIZE(len, ArchiveStatusResponseMessage, "invalid ArchiveStatusResponseMessage message received from primary");
            errorno = memcpy_s(&response, sizeof(ArchiveStatusResponseMessage), buf, sizeof(ArchiveStatusResponseMessage));
            securec_check(errorno, "\0", "\0");
            ProcessArchiveStatusResponse(&response);
            break;
        }
        default:
            ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                            errmsg_internal("invalid replication message type %c", type)));
    }
}

void WSDataRcvCheck(char *data_buf, Size nbytes)
{
    errno_t errorno = EOK;
    char *cur_buf = NULL;
    uint32 total_len = 0;
    XLogRecPtr ref_xlog_ptr = InvalidXLogRecPtr;

    cur_buf = data_buf;

    errorno = memcpy_s(&total_len, sizeof(uint32), cur_buf, sizeof(uint32));
    securec_check(errorno, "\0", "\0");
    cur_buf += sizeof(uint32);

    if (total_len != nbytes) {
        Assert(false);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("the corrupt data total len is %u bytes, the expected len is %lu bytes.", total_len, nbytes)));
    }

    if (cur_buf[0] != 'd') {
        Assert(false);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("the unexpected data flag is %c, the expected data flag is 'd'.", cur_buf[0])));
    }
    cur_buf += 1;

    errorno = memcpy_s(&ref_xlog_ptr, sizeof(XLogRecPtr), cur_buf, sizeof(XLogRecPtr));
    securec_check(errorno, "\0", "\0");
    if (XLogRecPtrIsInvalid(ref_xlog_ptr)) {
        Assert(false);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("the start xlog employed for the wal data is invalid.")));
    }
    cur_buf += sizeof(XLogRecPtr);

    errorno = memcpy_s(&ref_xlog_ptr, sizeof(XLogRecPtr), cur_buf, sizeof(XLogRecPtr));
    securec_check(errorno, "\0", "\0");
    if (XLogRecPtrIsInvalid(ref_xlog_ptr)) {
        Assert(false);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("the end xlog employed for the wal data is invalid.")));
    }

    return;
}

/*
 * Receive all the required replication data page.
 */
static void WalDataRcvReceive(char *buf, Size nbytes, XLogRecPtr recptr)
{
    /* buf unit */
    uint32 expected_len = 0;
#ifdef DATA_DEBUG
    pg_crc32 crc;
#endif
    Size left_len = nbytes;
    char *cur_buf = buf;
    errno_t errorno = EOK;

    /* 'd' means the replication data, 'w' means the xlog. */
    char data_flag = 0;
    XLogRecPtr received_ptr = InvalidXLogRecPtr;
    bool empty_streaming_body = false;

    while (left_len > 0) {
        errorno = memcpy_s(&expected_len, sizeof(uint32), cur_buf, sizeof(uint32));
        securec_check(errorno, "\0", "\0");
        cur_buf += sizeof(uint32);

        /* skip the 'd' flag */
        data_flag = cur_buf[0];
        Assert(data_flag == 'd' || data_flag == 'w');
        cur_buf += 1;

        if (data_flag == 'd') {
            if (expected_len <= (sizeof(uint32) + 1 + sizeof(XLogRecPtr) * 2)) {
                Assert(false);
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("the received wal data is unexpected %u bytes at least more than %lu bytes",
                                       expected_len, (sizeof(uint32) + 1 + sizeof(XLogRecPtr) * 2))));
            }
        } else if (data_flag == 'w') {
            if (expected_len < (sizeof(uint32) + 1 + sizeof(XLogRecPtr))) {
                Assert(false);
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("the received xlog is unexpected %u bytes at least more than %lu bytes.",
                                       expected_len, (sizeof(uint32) + 1 + sizeof(XLogRecPtr)))));
            }

            errorno = memcpy_s(&received_ptr, sizeof(XLogRecPtr), cur_buf, sizeof(XLogRecPtr));
            securec_check(errorno, "\0", "\0");

            if (expected_len == (sizeof(uint32) + 1 + sizeof(XLogRecPtr))) {
                ereport(DEBUG2, (errmsg("received empty streaming body at %X/%X.", (uint32)(received_ptr >> 32),
                                        (uint32)received_ptr)));

                empty_streaming_body = true;
            }

            if (!empty_streaming_body) {
                XLByteAdvance(recptr, (uint32)(expected_len - WAL_DATA_LEN));

                SpinLockAcquire(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
                t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr = recptr;
                SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
            }
        } else {
            Assert(false);
            ereport(
                ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("fail to push some wal data to the wal streaming writer queue: unexpected wal data flag %c.",
                        data_flag)));
        }

        if (!empty_streaming_body) {
            /* skip the message header */
            (void)PushToWriterQueue(cur_buf - sizeof(uint32) - 1, expected_len);
            ereport(DEBUG5, (errmsg("push some wal data to the wal streaming writer queue: data flag %c, %u bytes.",
                                    data_flag, expected_len)));
        } else
            empty_streaming_body = false;

        cur_buf += (expected_len - (sizeof(uint32) + 1));
        left_len -= expected_len;

        wakeupWalRcvWriter();
    }
    Assert(left_len == 0);

    wakeupWalRcvWriter();
}

void UpdateWalRcvCtl(struct WalRcvCtlBlock* walRcvCtlBlock, const XLogRecPtr recptr, const int segbytes)
{
    const int64 recBufferSize = g_instance.attr.attr_storage.WalReceiverBufSize * 1024;
    SpinLockAcquire(&walRcvCtlBlock->mutex);
    walRcvCtlBlock->walFreeOffset += segbytes;
    if (walRcvCtlBlock->walFreeOffset == recBufferSize && walRcvCtlBlock->walWriteOffset > 0 &&
        walRcvCtlBlock->walReadOffset > 0) {
        walRcvCtlBlock->walFreeOffset = 0;
    }
    walRcvCtlBlock->receivePtr = recptr;
    SpinLockRelease(&walRcvCtlBlock->mutex);
}

inline void WalReceiverWaitCopyXLogCount(XLogRecPtr recptr, XLogRecPtr startptr, int64 walfreeoffset,
    int64 walwriteoffset, int64 walreadoffset)
{
    static uint64 waitCount = 0;
    ++waitCount;
    const uint64 printInterval = 0xFFFF;
    if ((waitCount & printInterval) == 0) {
        const uint32 rightShiftSize = 32;
        ereport(WARNING, (errmsg("WalReceiverWaitCopyXLogCount: recptr(%X:%X),walfreeoffset(%ld),"
                                  "walwriteoffset(%ld),walreadoffset(%ld),startptr(%X:%X)",
                                  (uint32)(recptr >> rightShiftSize), (uint32)recptr,  walfreeoffset, walwriteoffset,
                                  walreadoffset, (uint32)(startptr >> rightShiftSize), (uint32)startptr)));
    }
}
/*
 * Receive XLOG data into receiver buffer.
 */
static void XLogWalRcvReceiveInBuf(char *buf, Size nbytes, XLogRecPtr recptr)
{
    int64 walfreeoffset;
    int64 walwriteoffset;
    int64 walreadoffset;
    char *walrecvbuf = NULL;
    XLogRecPtr startptr;
    int64 recBufferSize = g_instance.attr.attr_storage.WalReceiverBufSize * 1024;

    while (nbytes > 0) {
        int segbytes;
        int endPoint = recBufferSize;

        SpinLockAcquire(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
        if (t_thrd.walreceiver_cxt.walRcvCtlBlock->walFreeOffset ==
            t_thrd.walreceiver_cxt.walRcvCtlBlock->walWriteOffset) {
            // no data to be flushed
            t_thrd.walreceiver_cxt.walRcvCtlBlock->walStart = recptr;
        } else if (t_thrd.walreceiver_cxt.walRcvCtlBlock->walFreeOffset == recBufferSize &&
                   t_thrd.walreceiver_cxt.walRcvCtlBlock->walWriteOffset > 0 && 
                   t_thrd.walreceiver_cxt.walRcvCtlBlock->walReadOffset > 0) {
            t_thrd.walreceiver_cxt.walRcvCtlBlock->walFreeOffset = 0;
        }
        walfreeoffset = t_thrd.walreceiver_cxt.walRcvCtlBlock->walFreeOffset;
        walwriteoffset = t_thrd.walreceiver_cxt.walRcvCtlBlock->walWriteOffset;
        walreadoffset = t_thrd.walreceiver_cxt.walRcvCtlBlock->walReadOffset;
        walrecvbuf = t_thrd.walreceiver_cxt.walRcvCtlBlock->walReceiverBuffer;
        startptr = t_thrd.walreceiver_cxt.walRcvCtlBlock->walStart;
        SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);

        ereport(DEBUG5, (errmsg("XLogWalRcvReceive: recptr(%X:%X),nbytes(%d),"
                                "walfreeoffset(%ld),walwriteoffset(%ld),startptr(%X:%X)",
                                (uint32)(recptr >> 32), (uint32)recptr, (int)nbytes, walfreeoffset, walwriteoffset,
                                (uint32)(startptr >> 32), (uint32)startptr)));

        XLogWalRcvSendReply(false, false);

        Assert(walrecvbuf != NULL);
        Assert(walfreeoffset <= recBufferSize);
        Assert(walwriteoffset <= recBufferSize);
        Assert(walreadoffset <= recBufferSize);

        if (walfreeoffset < walreadoffset) {
            endPoint = walreadoffset - 1;
        }

        if (endPoint == walfreeoffset) {
            if (WalRcvWriterInProgress()) {
                wakeupWalRcvWriter();
                WakeupRecovery();
                /* Process any requests or signals received recently */
                ProcessWalRcvInterrupts();
                /* Keepalived with primary when waiting flush wal data */
                XLogWalRcvSendReply(false, false);
                pg_usleep(1000);
                WalReceiverWaitCopyXLogCount(recptr, startptr, walfreeoffset, walwriteoffset, walreadoffset);
            } else {
                walRcvDataCleanup();
                WakeupRecovery();
                ProcessWalRcvInterrupts();
            }
            continue;
        }

        segbytes = ((walfreeoffset + (int)nbytes > endPoint) ? (endPoint - walfreeoffset) : (int)nbytes);

        /* Need to seek in the buffer? */
        if (walfreeoffset != walwriteoffset) {
            if (walfreeoffset > walwriteoffset) {
                XLByteAdvance(startptr, (uint32)(walfreeoffset - walwriteoffset));
            } else {
                XLByteAdvance(startptr, (uint32)(recBufferSize - walwriteoffset + walfreeoffset));
            }
            if (!XLByteEQ(startptr, recptr)) {
                /* wait for finishing flushing all wal data */
                while (true) {
                    SpinLockAcquire(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
                    if (t_thrd.walreceiver_cxt.walRcvCtlBlock->walFreeOffset == 
                        t_thrd.walreceiver_cxt.walRcvCtlBlock->walWriteOffset) {
                        t_thrd.walreceiver_cxt.walRcvCtlBlock->walStart = recptr;
                        SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
                        break;
                    }
                    SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);

                    if (WalRcvWriterInProgress()) {
                        wakeupWalRcvWriter();
                        WakeupRecovery();
                        /* Process any requests or signals received recently */
                        ProcessWalRcvInterrupts();
                        /* Keepalived with primary when waiting flush wal data */
                        XLogWalRcvSendReply(false, false);
                        pg_usleep(1000); /* 1ms */
                    } else {
                        walRcvDataCleanup();
                        WakeupRecovery();
                        ProcessWalRcvInterrupts();
                    }
                }

                ereport(FATAL,
                        (errmsg("Unexpected seek in the walreceiver buffer. "
                                "xlogrecptr is (%X:%X) but local xlogptr is (%X:%X)."
                                "nbyte is %lu, walfreeoffset is %ld walwriteoffset is %ld walreadoffset is %ld",
                                (uint32)(recptr >> 32), (uint32)recptr, (uint32)(startptr >> 32), (uint32)startptr,
                                nbytes, walfreeoffset, walwriteoffset, walreadoffset)));
            }
        }

        /* OK to receive the logs */
        Assert(walfreeoffset + segbytes <= recBufferSize);
        errno_t errorno = memcpy_s(walrecvbuf + walfreeoffset, recBufferSize - walfreeoffset, buf, segbytes);
        securec_check(errorno, "\0", "\0");

        XLByteAdvance(recptr, (uint32)segbytes);

        nbytes -= segbytes;
        buf += segbytes;

        // update shared memory
        UpdateWalRcvCtl(t_thrd.walreceiver_cxt.walRcvCtlBlock, recptr, segbytes);
    }

    wakeupWalRcvWriter();
}

/*
 * Receive XLOG data into receiver buffer.
 */
static void XLogWalRcvReceive(char *buf, Size nbytes, XLogRecPtr recptr)
{
    int walfreeoffset;
    int walwriteoffset;
    char *walrecvbuf = NULL;
    XLogRecPtr startptr;
    int recBufferSize = g_instance.attr.attr_storage.WalReceiverBufSize * 1024;

    while (nbytes > 0) {
        int segbytes;
        int endPoint = recBufferSize;
        errno_t errorno = EOK;

        SpinLockAcquire(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
        if (t_thrd.walreceiver_cxt.walRcvCtlBlock->walFreeOffset ==
            t_thrd.walreceiver_cxt.walRcvCtlBlock->walWriteOffset) {
            // no data to be flushed
            t_thrd.walreceiver_cxt.walRcvCtlBlock->walStart = recptr;
        } else if (t_thrd.walreceiver_cxt.walRcvCtlBlock->walFreeOffset == recBufferSize &&
                   t_thrd.walreceiver_cxt.walRcvCtlBlock->walWriteOffset > 0) {
            t_thrd.walreceiver_cxt.walRcvCtlBlock->walFreeOffset = 0;
        }
        walfreeoffset = t_thrd.walreceiver_cxt.walRcvCtlBlock->walFreeOffset;
        walwriteoffset = t_thrd.walreceiver_cxt.walRcvCtlBlock->walWriteOffset;
        walrecvbuf = t_thrd.walreceiver_cxt.walRcvCtlBlock->walReceiverBuffer;
        startptr = t_thrd.walreceiver_cxt.walRcvCtlBlock->walStart;
        SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);

        ereport(DEBUG5, (errmsg("XLogWalRcvReceive: recptr(%u:%X),nbytes(%d),"
                                "walfreeoffset(%d),walwriteoffset(%d),startptr(%u:%X)",
                                (uint32)(recptr >> 32), (uint32)recptr, (int)nbytes, walfreeoffset, walwriteoffset,
                                (uint32)(startptr >> 32), (uint32)startptr)));

        XLogWalRcvSendReply(false, false);

        Assert(walrecvbuf != NULL);
        Assert(walfreeoffset <= recBufferSize);
        Assert(walwriteoffset <= recBufferSize);

        if (walfreeoffset < walwriteoffset) {
            endPoint = walwriteoffset - 1;
        }

        if (endPoint == walfreeoffset) {
            if (WalRcvWriterInProgress()) {
                wakeupWalRcvWriter();
                /* Process any requests or signals received recently */
                ProcessWalRcvInterrupts();
                /* Keepalived with primary when waiting flush wal data */
                XLogWalRcvSendReply(false, false);
                pg_usleep(1000);
            } else
                walRcvDataCleanup();
            continue;
        }

        segbytes = (walfreeoffset + (int)nbytes > endPoint) ? endPoint - walfreeoffset : nbytes;

        /* Need to seek in the buffer? */
        if (walfreeoffset != walwriteoffset) {
            uint32 waladvancelen = (walfreeoffset > walwriteoffset) ?
                                   (uint32)(walfreeoffset - walwriteoffset) :
                                   (uint32)(recBufferSize - walwriteoffset + walfreeoffset);
            XLByteAdvance(startptr, waladvancelen);
            if (!XLByteEQ(startptr, recptr)) {
                /* wait for finishing flushing all wal data */
                while (true) {
                    SpinLockAcquire(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
                    if (t_thrd.walreceiver_cxt.walRcvCtlBlock->walFreeOffset ==
                        t_thrd.walreceiver_cxt.walRcvCtlBlock->walWriteOffset) {
                        t_thrd.walreceiver_cxt.walRcvCtlBlock->walStart = recptr;
                        SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
                        break;
                    }
                    SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);

                    if (WalRcvWriterInProgress()) {
                        wakeupWalRcvWriter();
                        /* Process any requests or signals received recently */
                        ProcessWalRcvInterrupts();
                        /* Keepalived with primary when waiting flush wal data */
                        XLogWalRcvSendReply(false, false);
                        pg_usleep(1000);
                    } else
                        walRcvDataCleanup();
                }

                ereport(FATAL,
                        (errmsg("Unexpected seek in the walreceiver buffer. "
                                "xlogrecptr is (%X:%X) but local xlogptr is (%X:%X).",
                                (uint32)(recptr >> 32), (uint32)recptr, (uint32)(startptr >> 32), (uint32)startptr)));
            }
        }

        /* OK to receive the logs */
        Assert(walfreeoffset + segbytes <= recBufferSize);
        errorno = memcpy_s(walrecvbuf + walfreeoffset, recBufferSize, buf, segbytes);
        securec_check(errorno, "\0", "\0");

        XLByteAdvance(recptr, (uint32)segbytes);

        nbytes -= segbytes;
        buf += segbytes;

        // update shared memory
        SpinLockAcquire(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
        t_thrd.walreceiver_cxt.walRcvCtlBlock->walFreeOffset += segbytes;
        if (t_thrd.walreceiver_cxt.walRcvCtlBlock->walFreeOffset == recBufferSize &&
            t_thrd.walreceiver_cxt.walRcvCtlBlock->walWriteOffset > 0) {
            t_thrd.walreceiver_cxt.walRcvCtlBlock->walFreeOffset = 0;
        }
        t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr = recptr;
        SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
    }

    wakeupWalRcvWriter();
}

/*
 * Send reply message to primary, indicating our current XLOG positions, oldest
 * xmin and the current time.
 *
 * If 'force' is not true, the message is not sent unless enough time has
 * passed since last status update to reach wal_receiver_status_internal (or
 * if wal_receiver_status_interval is disabled altogether).
 *
 * If 'requestReply' is true, requests the server to reply immediately upon receiving
 * this message. This is used for heartbearts, when approaching wal_receiver_timeout.
 */
void XLogWalRcvSendReply(bool force, bool requestReply)
{
    char buf[sizeof(StandbyReplyMessage) + 1] = {0};
    TimestampTz now;
    XLogRecPtr receivePtr = InvalidXLogRecPtr;
    XLogRecPtr writePtr = InvalidXLogRecPtr;
    XLogRecPtr flushPtr = InvalidXLogRecPtr;
    XLogRecPtr ReplayReadPtr = InvalidXLogRecPtr;
    int rc = 0;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    XLogRecPtr sndFlushPtr;

    /*
     * If the user doesn't want status to be reported to the master, be sure
     * to exit before doing anything at all.
     */
    if (!force && u_sess->attr.attr_storage.wal_receiver_status_interval <= 0)
        return;

    SpinLockAcquire(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
    receivePtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr;
    writePtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->writePtr;
    flushPtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->flushPtr;
    SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);

    /* Get current timestamp. */
    now = GetCurrentTimestamp();
    /*
     * We can compare the write and flush positions to the last message we
     * sent without taking any lock, but the apply position requires a spin
     * lock, so we don't check that unless something else has changed or 10
     * seconds have passed.  This means that the apply log position will
     * appear, from the master's point of view, to lag slightly, but since
     * this is only for reporting purposes and only on idle systems, that's
     * probably OK.
     */
    if (!force && XLByteEQ(t_thrd.walreceiver_cxt.reply_message->receive, receivePtr) &&
        XLByteEQ(t_thrd.walreceiver_cxt.reply_message->write, writePtr) &&
        XLByteEQ(t_thrd.walreceiver_cxt.reply_message->flush, flushPtr) &&
        !(TimestampDifferenceExceeds(t_thrd.walreceiver_cxt.reply_message->sendTime, now,
                                     u_sess->attr.attr_storage.wal_receiver_status_interval * 1000) ||
          TimestampDifferenceExceeds(now, t_thrd.walreceiver_cxt.reply_message->sendTime,
                                     u_sess->attr.attr_storage.wal_receiver_status_interval * 1000))) {
        return;
    }

    /* Construct a new message */
    t_thrd.walreceiver_cxt.reply_message->receive = receivePtr;
    t_thrd.walreceiver_cxt.reply_message->write = writePtr;
    t_thrd.walreceiver_cxt.reply_message->flush = flushPtr;
    if (!dummyStandbyMode) {
        t_thrd.walreceiver_cxt.reply_message->apply = GetXLogReplayRecPtr(NULL, &ReplayReadPtr);
        t_thrd.walreceiver_cxt.reply_message->applyRead = ReplayReadPtr;
    } else {
        t_thrd.walreceiver_cxt.reply_message->apply = flushPtr;
        t_thrd.walreceiver_cxt.reply_message->applyRead = flushPtr;
    }
    t_thrd.walreceiver_cxt.reply_message->sendTime = now;
    t_thrd.walreceiver_cxt.reply_message->replyRequested = requestReply;

    SpinLockAcquire(&hashmdata->mutex);
    t_thrd.walreceiver_cxt.reply_message->peer_role = hashmdata->current_mode;
    SpinLockRelease(&hashmdata->mutex);
    t_thrd.walreceiver_cxt.reply_message->peer_state = get_local_dbstate();
    SpinLockAcquire(&walrcv->mutex);
    walrcv->receiver_received_location = receivePtr;
    walrcv->receiver_write_location = writePtr;
    walrcv->receiver_flush_location = flushPtr;
    walrcv->receiver_replay_location = t_thrd.walreceiver_cxt.reply_message->apply;
    sndFlushPtr = walrcv->sender_flush_location;
    SpinLockRelease(&walrcv->mutex);

    if (u_sess->attr.attr_storage.HaModuleDebug) {
        ereport(LOG, (errmsg("HA-XLogWalRcvSendReply: sending receive %X/%X write %X/%X flush %X/%X apply %X/%X",
                             (uint32)(t_thrd.walreceiver_cxt.reply_message->receive >> 32),
                             (uint32)t_thrd.walreceiver_cxt.reply_message->receive,
                             (uint32)(t_thrd.walreceiver_cxt.reply_message->write >> 32),
                             (uint32)t_thrd.walreceiver_cxt.reply_message->write,
                             (uint32)(t_thrd.walreceiver_cxt.reply_message->flush >> 32),
                             (uint32)t_thrd.walreceiver_cxt.reply_message->flush,
                             (uint32)(t_thrd.walreceiver_cxt.reply_message->apply >> 32),
                             (uint32)t_thrd.walreceiver_cxt.reply_message->apply)));
    }

    /* Prepend with the message type and send it. */
    buf[0] = 'r';
    rc = memcpy_s(&buf[1], sizeof(StandbyReplyMessage), t_thrd.walreceiver_cxt.reply_message,
                  sizeof(StandbyReplyMessage));
    securec_check(rc, "\0", "\0");
    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_send(buf, sizeof(StandbyReplyMessage) + 1);
    WalRcvRefreshPercentCountStartLsn(sndFlushPtr, flushPtr);
}

/*
 * Send hot standby feedback message to primary, plus the current time,
 * in case they don't have a watch.
 */
static void XLogWalRcvSendHSFeedback(void)
{
    char buf[sizeof(StandbyHSFeedbackMessage) + 1];
    TimestampTz now;
    TransactionId xmin;
    errno_t rc = 0;
    /*
     * If the user doesn't want status to be reported to the master, be sure
     * to exit before doing anything at all.
     */
    if (u_sess->attr.attr_storage.wal_receiver_status_interval <= 0 || !u_sess->attr.attr_storage.hot_standby_feedback)
        return;

    /* Get current timestamp. */
    now = GetCurrentTimestamp();
    /*
     * Send feedback at most once per wal_receiver_status_interval.
     */
    if (!TimestampDifferenceExceeds(t_thrd.walreceiver_cxt.feedback_message->sendTime, now,
                                    u_sess->attr.attr_storage.wal_receiver_status_interval * 1000)) {
        return;
    }

    /*
     * If Hot Standby is not yet active there is nothing to send. Check this
     * after the interval has expired to reduce number of calls.
     */
    if (!HotStandbyActive())
        return;

    /*
     * Make the expensive call to get the oldest xmin once we are certain
     * everything else has been checked.
     */
#ifndef ENABLE_MULTIPLE_NODES
    /* Get updated RecentGlobalXmin */
    GetSnapshotData(u_sess->utils_cxt.CurrentSnapshotData, true, true);
#endif
    xmin = GetOldestXmin(NULL);

    /*
     * Always send feedback message.
     */
    t_thrd.walreceiver_cxt.feedback_message->sendTime = now;
    t_thrd.walreceiver_cxt.feedback_message->xmin = xmin;

    ereport(DEBUG2,
            (errmsg("sending hot standby feedback xmin " XID_FMT, t_thrd.walreceiver_cxt.feedback_message->xmin)));

    /* Prepend with the message type and send it. */
    buf[0] = 'h';
    rc = memcpy_s(&buf[1], sizeof(StandbyHSFeedbackMessage), t_thrd.walreceiver_cxt.feedback_message,
                  sizeof(StandbyHSFeedbackMessage));
    securec_check(rc, "\0", "\0");
    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_send(buf, sizeof(StandbyHSFeedbackMessage) + 1);
}

/*
 * Process WaldataHeaderMessage received from sender message type is 'd'.
 */
static void ProcessWalDataHeaderMessage(WalDataPageMessageHeader *msghdr)
{
    /* Use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    TimestampTz lastMsgReceiptTime = GetCurrentTimestamp();

    Assert(msghdr);

    /* Update shared-memory status */
    SpinLockAcquire(&walrcv->mutex);
    walrcv->lastMsgSendTime = msghdr->sendTime;
    walrcv->lastMsgReceiptTime = lastMsgReceiptTime;
    SpinLockRelease(&walrcv->mutex);

    if (log_min_messages <= DEBUG2) {
        MakeDebugLog(msghdr->sendTime, lastMsgReceiptTime,
                     "wal receive waldata header data sendtime %s receipttime %s");
    }
}

/*
 * Process walHeaderMessage received from sender, message type is 'w'.
 */
static void ProcessWalHeaderMessage(WalDataMessageHeader *msghdr)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    TimestampTz lastMsgReceiptTime = GetCurrentTimestamp();

    /* Update shared-memory status */
    SpinLockAcquire(&walrcv->mutex);
    walrcv->lastMsgSendTime = msghdr->sendTime;
    walrcv->lastMsgReceiptTime = lastMsgReceiptTime;

    walrcv->sender_sent_location = msghdr->sender_sent_location;

    walrcv->sender_flush_location = msghdr->sender_flush_location;

    walrcv->sender_replay_location = msghdr->sender_replay_location;

    walrcv->sender_write_location = msghdr->sender_write_location;

    SpinLockRelease(&walrcv->mutex);

    /* Update the catchup flag */
    wal_catchup = msghdr->catchup;

    ereport(DEBUG2, (errmsg("wal receiver data message: start %X/%X end %X/%X "
                            "sender_write %X/%X sender_flush %X/%X sender_replay %X/%X",
                            (uint32)(msghdr->dataStart >> 32), (uint32)msghdr->dataStart,
                            (uint32)(msghdr->sender_sent_location >> 32), (uint32)msghdr->sender_sent_location,
                            (uint32)(msghdr->sender_write_location >> 32), (uint32)msghdr->sender_write_location,
                            (uint32)(msghdr->sender_flush_location >> 32), (uint32)msghdr->sender_flush_location,
                            (uint32)(msghdr->sender_replay_location >> 32), (uint32)msghdr->sender_replay_location)));

    if (log_min_messages <= DEBUG2) {
        MakeDebugLog(msghdr->sendTime, lastMsgReceiptTime, "wal receive wal header data sendtime %s receipttime %s");
        ereport(DEBUG2, (errmsg("replication apply delay %d ms transfer latency %d ms", GetReplicationApplyDelay(),
                                GetReplicationTransferLatency())));
    }

    return;
}

/*
 * Process ProcessKeepaliveMessage received from sender, message type is 'k'.
 */
static void ProcessKeepaliveMessage(PrimaryKeepaliveMessage *keepalive)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    TimestampTz lastMsgReceiptTime = GetCurrentTimestamp();

    /* Update shared-memory status */
    SpinLockAcquire(&walrcv->mutex);
    walrcv->peer_role = keepalive->peer_role;
    walrcv->peer_state = keepalive->peer_state;
    walrcv->sender_sent_location = keepalive->walEnd;
    walrcv->lastMsgSendTime = keepalive->sendTime;
    walrcv->lastMsgReceiptTime = lastMsgReceiptTime;
    SpinLockRelease(&walrcv->mutex);
    wal_catchup = keepalive->catchup;

    if (log_min_messages <= DEBUG2) {
        MakeDebugLog(keepalive->sendTime, lastMsgReceiptTime, "wal receive keep alive data sendtime %s receipttime %s");
        ereport(DEBUG2, (errmsg("replication apply delay %d ms transfer latency %d ms", GetReplicationApplyDelay(),
                                GetReplicationTransferLatency())));
    }
}

/*
 * update pg_control file.
 * only wal receiver set system_identifier.
 */
void SyncSystemIdentifier(void)
{
    if (t_thrd.walreceiver_cxt.control_file_writed == 0) {
        ereport(LOG, (errmsg("update secondary system identifier")));

        SetSystemIdentifier(sync_system_identifier);

        t_thrd.walreceiver_cxt.control_file_writed++;
        UpdateControlFile();
    }
}

void ProcessWSRmXLog(void)
{
    char xlog_path[MAXPGPATH] = {0};
    int nRet = 0;

    nRet = snprintf_s(xlog_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", t_thrd.proc_cxt.DataDir, XLOGDIR);
    securec_check_ss(nRet, "\0", "\0");

    DIR *dir = NULL;
    struct dirent *de;

    dir = AllocateDir(xlog_path);
    while ((de = ReadDir(dir, xlog_path)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        char path[MAXPGPATH] = {0};
        nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", xlog_path, de->d_name);
        securec_check_ss(nRet, "\0", "\0");

        (void)unlink(path);
    }
    FreeDir(dir);
}

void ProcessWSRmData(void)
{
    DIR *dir = NULL;
    struct dirent *de = NULL;
    char data_path[MAXPGPATH] = {0};
    int nRet = 0;

    nRet = snprintf_s(data_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", t_thrd.proc_cxt.DataDir, DUMMY_STANDBY_DATADIR);
    securec_check_ss(nRet, "\0", "\0");

    dir = AllocateDir(data_path);
    while ((de = ReadDir(dir, data_path)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        char path[MAXPGPATH] = {0};
        nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", data_path, de->d_name);
        securec_check_ss(nRet, "\0", "\0");

        ereport(LOG, (errmsg("delete data path %s on the dummy standby.", path)));

        (void)unlink(path);
    }
    FreeDir(dir);
}

/*
 * Process RmXLogMessage received from primary sender, message type is 'x'.
 * Refence searchBCMFiles
 */
static void ProcessRmXLogMessage(RmXLogMessage *rmXLogMessage)
{
    XLogRecPtr lastFlushPtr = InvalidXLogRecPtr;

    // check command source
    if (rmXLogMessage->peer_role != PRIMARY_MODE) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION),
                        errmsg("rm xlog comand is not from primary,peer_role=%d", rmXLogMessage->peer_role)));
    }

    ereport(DEBUG2, (errmsg("received rm xlog message")));

    walRcvDataCleanup();
    WalRcvXLogClose();

    SpinLockAcquire(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
    lastFlushPtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->flushPtr;
    t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->writePtr =
        t_thrd.walreceiver_cxt.walRcvCtlBlock->flushPtr = InvalidXLogRecPtr;
    t_thrd.walreceiver_cxt.walRcvCtlBlock->walStart = InvalidXLogRecPtr;
    t_thrd.walreceiver_cxt.walRcvCtlBlock->walWriteOffset = t_thrd.walreceiver_cxt.walRcvCtlBlock->walFreeOffset = 0;
    t_thrd.walreceiver_cxt.walRcvCtlBlock->walReadOffset = 0;
    SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);

    /* Now rm the WAL files. */
    ProcessWSRmXLog();
    if (!XLByteEQ(lastFlushPtr, InvalidXLogRecPtr)) {
        ereport(LOG, (errmsg("rm xlog command done, lastFlushPtr=%X/%X", (uint32)(lastFlushPtr >> 32),
                             (uint32)(lastFlushPtr))));
    }
    SyncSystemIdentifier();

    /* Now rm the data file the same operation copyed from ProcessRmDataMessage() */
    if (g_instance.attr.attr_storage.enable_mix_replication) {
        while (true) {
            if (!ws_dummy_data_writer_use_file) {
                CloseWSDataFileOnDummyStandby();
                break;
            } else
                pg_usleep(100000); /* sleep 0.1 s */
        }

        ProcessWSRmData();
    }

    return;
}

/*
 * Process RmXLogMessage received from primary sender, message type is 'e'.
 * Refence searchBCMFiles
 */
static void ProcessEndXLogMessage(EndXLogMessage *endXLogMessage)
{
    ereport(dummyStandbyMode ? DEBUG2 : LOG, (errmsg("sync Secondary Standby xlog done")));

    if (endXLogMessage->percent == SYNC_DUMMY_STANDBY_END) {
        SetWalRcvDummyStandbySyncPercent(SYNC_DUMMY_STANDBY_END);

        if (dummyStandbyMode)
            SyncSystemIdentifier();
    }
}

/*
 * Process ProcessArchiveXlogMessage received from primary sender, message type is 'a'.
 */
const static int GET_ARCHIVE_XLOG_RETRY_MAX = 50;
const static int ARCHIVE_XLOG_DELAY = 10000;

static void ProcessArchiveXlogMessage(const ArchiveXlogMessage* archive_xlog_message)
{
    ereport(LOG, (errmsg("get archive xlog message :%X/%X", (uint32)(archive_xlog_message->targetLsn >> 32), 
        (uint32)(archive_xlog_message->targetLsn))));
    errno_t errorno = EOK;
    volatile unsigned int *pitr_task_status = &g_instance.archive_obs_cxt.pitr_task_status;
    unsigned int expected = PITR_TASK_NONE;
    int failed_times = 0;
    while (pg_atomic_compare_exchange_u32(pitr_task_status, &expected, PITR_TASK_GET) == false) {
        /* some task arrived before last task done if expected not equal to NONE */
        expected = PITR_TASK_NONE;
        pg_usleep(ARCHIVE_XLOG_DELAY);  // sleep 0.01s
        if (failed_times++ >= GET_ARCHIVE_XLOG_RETRY_MAX) {
            ereport(WARNING, (errmsg("get archive xlog message :%X/%X, but not finished",
                                     (uint32)(archive_xlog_message->targetLsn >> 32),
                                     (uint32)(archive_xlog_message->targetLsn))));
            return;
        }
    }
    errorno = memcpy_s(&g_instance.archive_obs_cxt.archive_task,
        sizeof(ArchiveXlogMessage) + 1,
        archive_xlog_message,
        sizeof(ArchiveXlogMessage));
    securec_check(errorno, "\0", "\0");
    wakeupObsArchLatch();
}

/*
 * Process ProcessStandbyArchiveXlogMessage received from primary sender, message type is 'n'.
 */
static void ProcessStandbyArchiveXlogMessage(const ArchiveXlogMessage* archive_xlog_message)
{
    ereport(LOG, (errmsg("ProcessStandbyArchiveXlogMessage: get archive xlog message :%X/%X",
                        (uint32)(archive_xlog_message->targetLsn >> 32), (uint32)(archive_xlog_message->targetLsn))));
    errno_t errorno = EOK;
    volatile unsigned int* arch_task_status = &g_instance.archive_standby_cxt.arch_task_status;
    unsigned int expected = ARCH_TASK_NONE;
    int failed_times = 0;
    while (pg_atomic_compare_exchange_u32(arch_task_status, &expected, ARCH_TASK_GET) == false) {
        /* some task arrived before last task done if expected not equal to NONE */
        expected = ARCH_TASK_NONE;
        pg_usleep(ARCHIVE_XLOG_DELAY);  // sleep 0.01s
        if (failed_times++ >= GET_ARCHIVE_XLOG_RETRY_MAX) {
            ereport(WARNING, (errmsg("get archive xlog message :%X/%X, but not finished",
                                     (uint32)(archive_xlog_message->targetLsn >> 32),
                                     (uint32)(archive_xlog_message->targetLsn))));
            return;
        }
    }
    errorno = memcpy_s(&g_instance.archive_standby_cxt.archive_task,
        sizeof(ArchiveXlogMessage) + 1,
        archive_xlog_message,
        sizeof(ArchiveXlogMessage));
    securec_check(errorno, "\0", "\0");
    wakeupArchLatch();
}

/*
 * Send switchover request message to primary, indicating the current time.
 */
static void XLogWalRcvSendSwitchRequest(void)
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    char buf[sizeof(StandbySwitchRequestMessage) + 1];
    TimestampTz local_now;
    errno_t errorno = EOK;

    /* Get current timestamp. */
    local_now = GetCurrentTimestamp();
    t_thrd.walreceiver_cxt.request_message->sendTime = local_now;

    SpinLockAcquire(&walrcv->mutex);
    t_thrd.walreceiver_cxt.request_message->demoteMode = walrcv->node_state;
    walrcv->node_state = NODESTATE_STANDBY_WAITING;
    SpinLockRelease(&walrcv->mutex);

    /* Prepend with the message type and send it. */
    buf[0] = 's';
    errorno = memcpy_s(&buf[1], sizeof(StandbySwitchRequestMessage), t_thrd.walreceiver_cxt.request_message,
        sizeof(StandbySwitchRequestMessage));
    securec_check(errorno, "\0", "\0");
    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_send(buf, sizeof(StandbySwitchRequestMessage) + 1);

    SendPostmasterSignal(PMSIGNAL_UPDATE_WAITING);
    ereport(LOG, (errmsg("send %s switchover request to primary",
        DemoteModeDesc(t_thrd.walreceiver_cxt.request_message->demoteMode))));
}

/*
 * Send archive xlog response message to primary.
 */
static void WalRecvSendArchiveXlogResponse()
{
    char buf[sizeof(ArchiveXlogResponseMeeeage) + 1];
    ArchiveXlogResponseMeeeage reply;
    errno_t errorno = EOK;
    reply.pitr_result = g_instance.archive_obs_cxt.pitr_finish_result;
    reply.targetLsn = g_instance.archive_obs_cxt.archive_task.targetLsn;
    buf[0] = 'a';
    errorno = memcpy_s(&buf[1],
        sizeof(ArchiveXlogResponseMeeeage),
        &reply,
        sizeof(ArchiveXlogResponseMeeeage));
    securec_check(errorno, "\0", "\0");
    libpqrcv_send(buf, sizeof(ArchiveXlogResponseMeeeage) + 1);

    ereport(LOG,
        (errmsg("WalRecvSendArchiveXlogResponse %d %X/%X", reply.pitr_result, 
            (uint32)(reply.targetLsn >> 32), (uint32)(reply.targetLsn))));

}

/*
 * Send archive xlog result message to primary.
 */
static void WalRecvSendArchiveXlogResult2Standby()
{
    char buf[sizeof(ArchiveXlogResponseMeeeage) + 1];
    ArchiveXlogResponseMeeeage reply;
    errno_t errorno = EOK;
    reply.pitr_result = g_instance.archive_standby_cxt.arch_finish_result;
    reply.targetLsn = g_instance.archive_standby_cxt.archive_task.targetLsn;
    buf[0] = 'n';
    errorno = memcpy_s(&buf[1],
        sizeof(ArchiveXlogResponseMeeeage),
        &reply,
        sizeof(ArchiveXlogResponseMeeeage));
    securec_check(errorno, "\0", "\0");
    libpqrcv_send(buf, sizeof(ArchiveXlogResponseMeeeage) + 1);
    const char* archive_result_string = ((reply.pitr_result) ? "success" : "fail");
    ereport(LOG,
        (errmsg("WalRecvSendArchiveXlogResult2Standby archive_result:%s archive_lsn:%X/%X", archive_result_string, 
            (uint32)(reply.targetLsn >> 32), (uint32)(reply.targetLsn))));

}

/*
 * process switchover response message from primary.
 */
static void ProcessSwitchResponse(int code)
{
    switch (code) {
        case SWITCHOVER_PROMOTE_REQUEST: /* promote standby */
            t_thrd.walreceiverfuncs_cxt.WalRcv->node_state = NODESTATE_STANDBY_PROMOTING;
            SendPostmasterSignal(PMSIGNAL_PROMOTE_STANDBY);
            break;

        case SWITCHOVER_DEMOTE_FAILED: /* demote failed */
            ereport(WARNING, (errmsg("primary demote failed")));
            break;

        case SWITCHOVER_DEMOTE_CATCHUP_EXIST: /* demote failed */
            t_thrd.walreceiverfuncs_cxt.WalRcv->node_state = NODESTATE_NORMAL;
            SendPostmasterSignal(PMSIGNAL_ROLLBACK_STANDBY_PROMOTE);
            ereport(LOG, (errmsg("catchup is still alive, switchover failed")));
            break;

        default:
            ereport(WARNING, (errmsg("unknown switchover response message received from primary")));
            break;
    }
}

/*
 * Keep track of important messages from primary.
 */
static void ProcessWalSndrMessage(XLogRecPtr *walEnd, TimestampTz sendTime)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    TimestampTz lastMsgReceiptTime = GetCurrentTimestamp();

    /* Update shared-memory status */
    SpinLockAcquire(&walrcv->mutex);
    if (XLByteLT(walrcv->latestWalEnd, *walEnd))
        walrcv->latestWalEndTime = sendTime;
    walrcv->latestWalEnd = *walEnd;
    walrcv->sender_sent_location = *walEnd;
    walrcv->lastMsgSendTime = sendTime;
    walrcv->lastMsgReceiptTime = lastMsgReceiptTime;
    SpinLockRelease(&walrcv->mutex);

    if (log_min_messages <= DEBUG2) {
        int applyDelay;
        applyDelay = GetReplicationApplyDelay();
        MakeDebugLog(sendTime, lastMsgReceiptTime, "wal receive walSndMsg sendtime %s receipttime %s");
        /* apply delay is not available */
        if (applyDelay == -1) {
            ereport(DEBUG2,
                    (errmsg("replication apply delay (N/A) transfer latency %d ms", GetReplicationTransferLatency())));
        } else {
            ereport(DEBUG2, (errmsg("replication apply delay %d ms transfer latency %d ms", applyDelay,
                                    GetReplicationTransferLatency())));
        }
    }
}

/*
 * get xlog sync percent between walsender and walreceiver.
 */
int GetSyncPercent(XLogRecPtr startLsn, XLogRecPtr totalLsn, XLogRecPtr hasCompleteLsn)
{
    int64 needSyncLogSum = 0;
    int64 haveSyncLog = 0;
    int haveCompletePer = 0;
    int basePercent = 0;
    XLogSegNo segno;

    if (XLByteLE(totalLsn, hasCompleteLsn)) {
        return HIGHEST_PERCENT;
    }

    /*
     * When startLsn is invalid, standby is under streaming, so count percent base on
     * maxlsn - wal_keep_segments*XLOG_SEG_SIZE, and percent is 90% ~ 100%
     */
    if (XLogRecPtrIsInvalid(startLsn)) {
        XLByteToSeg(totalLsn, segno);
        if (segno < WalGetSyncCountWindow()) {
            startLsn = InvalidXLogRecPtr;
        } else {
            startLsn = totalLsn - (WalGetSyncCountWindow() * XLOG_SEG_SIZE);
            basePercent = STREAMING_START_PERCENT;
        }
    }

    needSyncLogSum = XLogDiff(totalLsn, startLsn);
    haveSyncLog = XLogDiff(hasCompleteLsn, startLsn) + SizeOfXLogRecord - 1;

    if (needSyncLogSum == 0) {
        return HIGHEST_PERCENT;
    } else {
        haveCompletePer = (int)((HIGHEST_PERCENT - basePercent) * (haveSyncLog * 1.0 / needSyncLogSum)) + basePercent;
    }
    if (haveCompletePer > HIGHEST_PERCENT) {
        haveCompletePer = HIGHEST_PERCENT;
    } else if (haveCompletePer < 0) {
        haveCompletePer = 0;
    }
    return haveCompletePer;
}

static bool am_cascade_standby(void)
{
    if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE &&
        t_thrd.postmaster_cxt.HaShmData->is_cascade_standby) {
        return true;
    }

    return false;
}

/*
 * transfer the server mode to string.
 */
const char* wal_get_role_string(ServerMode mode, bool getPeerRole)
{
    switch (mode) {
        case NORMAL_MODE:
            return "Normal";
        case PRIMARY_MODE:
            return "Primary";
        case STANDBY_MODE:
        {
            if (am_cascade_standby() && !getPeerRole) {
                return "Cascade Standby";
            } else {
                return "Standby";
            }
        }
        case CASCADE_STANDBY_MODE:
            return "Cascade Standby";
        case PENDING_MODE:
            return "Pending";
        case UNKNOWN_MODE:
            return "Unknown";
        default:
            ereport(WARNING, (errmsg("invalid server mode:%d", (int)mode)));
            break;
    }
    return "Unknown";
}

const char *wal_get_rebuild_reason_string(HaRebuildReason reason)
{
    switch (reason) {
        case NONE_REBUILD:
            return "Normal";
        case WALSEGMENT_REBUILD:
            return "WAL segment removed";
        case CONNECT_REBUILD:
            return "Disconnected";
        case VERSION_REBUILD:
            return "Version not matched";
        case MODE_REBUILD:
            return "Mode not matched";
        case SYSTEMID_REBUILD:
            return "System id not matched";
        case TIMELINE_REBUILD:
            return "Timeline not matched";
        default:
            break;
    }
    return "Unknown";
}

static void wal_get_ha_rebuild_reason_with_dummy(char *buildReason, ServerMode local_role, bool isRunning)
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    int nRet = 0;
    load_server_mode();
    if (local_role == NORMAL_MODE || local_role == PRIMARY_MODE || IS_DISASTER_RECOVER_MODE) {
        nRet = snprintf_s(buildReason, MAXFNAMELEN, MAXFNAMELEN - 1, "%s", "Normal");
        securec_check_ss(nRet, "\0", "\0");
        return;
    }

    if (t_thrd.postmaster_cxt.ReplConnArray[1] != NULL && walrcv->conn_target == REPCONNTARGET_PRIMARY) {
        if (hashmdata->repl_reason[1] == NONE_REBUILD && isRunning) {
            nRet = snprintf_s(buildReason, MAXFNAMELEN, MAXFNAMELEN - 1, "%s", "Normal");
            securec_check_ss(nRet, "\0", "\0");
        } else if (hashmdata->repl_reason[1] == NONE_REBUILD && !isRunning) {
            nRet = snprintf_s(buildReason, MAXFNAMELEN, MAXFNAMELEN - 1, "%s", "Connecting...");
            securec_check_ss(nRet, "\0", "\0");
        } else {
            nRet = snprintf_s(buildReason, MAXFNAMELEN, MAXFNAMELEN - 1, "%s",
                              wal_get_rebuild_reason_string(hashmdata->repl_reason[1]));
            securec_check_ss(nRet, "\0", "\0");
        }
    } else if (t_thrd.postmaster_cxt.ReplConnArray[2] != NULL && walrcv->conn_target == REPCONNTARGET_DUMMYSTANDBY) {
        if (hashmdata->repl_reason[2] == NONE_REBUILD && isRunning) {
            nRet = snprintf_s(buildReason, MAXFNAMELEN, MAXFNAMELEN - 1, "%s", "Normal");
            securec_check_ss(nRet, "\0", "\0");
        } else if (hashmdata->repl_reason[2] == NONE_REBUILD && !isRunning) {
            nRet = snprintf_s(buildReason, MAXFNAMELEN, MAXFNAMELEN - 1, "%s", "Connecting...");
            securec_check_ss(nRet, "\0", "\0");
        } else {
            nRet = snprintf_s(buildReason, MAXFNAMELEN, MAXFNAMELEN - 1, "%s",
                              wal_get_rebuild_reason_string(hashmdata->repl_reason[2]));
            securec_check_ss(nRet, "\0", "\0");
        }
    } else {
        nRet = snprintf_s(buildReason, MAXFNAMELEN, MAXFNAMELEN - 1, "%s", "Disconnected");
        securec_check_ss(nRet, "\0", "\0");
    }
}

static void wal_get_ha_rebuild_reason_with_multi(char *buildReason, ServerMode local_role, bool isRunning)
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    int rcs = 0;
    load_server_mode();
    if (local_role == NORMAL_MODE || local_role == PRIMARY_MODE) {
        rcs = snprintf_s(buildReason, MAXFNAMELEN, MAXFNAMELEN - 1, "%s", "Normal");
        securec_check_ss(rcs, "\0", "\0");
        return;
    }

    if (t_thrd.postmaster_cxt.ReplConnArray[hashmdata->current_repl] != NULL &&
        (walrcv->conn_target == REPCONNTARGET_PRIMARY || am_cascade_standby()
        || IS_DISASTER_RECOVER_MODE)) {
        if (hashmdata->repl_reason[hashmdata->current_repl] == NONE_REBUILD && isRunning) {
            rcs = snprintf_s(buildReason, MAXFNAMELEN, MAXFNAMELEN - 1, "%s", "Normal");
            securec_check_ss(rcs, "\0", "\0");
        } else if (hashmdata->repl_reason[hashmdata->current_repl] == NONE_REBUILD && !isRunning) {
            rcs = snprintf_s(buildReason, MAXFNAMELEN, MAXFNAMELEN - 1, "%s", "Connecting...");
            securec_check_ss(rcs, "\0", "\0");
        } else {
            rcs = snprintf_s(buildReason, MAXFNAMELEN, MAXFNAMELEN - 1, "%s",
                             wal_get_rebuild_reason_string(hashmdata->repl_reason[hashmdata->current_repl]));
            securec_check_ss(rcs, "\0", "\0");
        }
    } else {
        rcs = snprintf_s(buildReason, MAXFNAMELEN, MAXFNAMELEN - 1, "%s", "Disconnected");
        securec_check_ss(rcs, "\0", "\0");
    }
}

static void wal_get_ha_rebuild_reason(char *buildReason, ServerMode local_role, bool isRunning)
{
    if (IS_DN_DUMMY_STANDYS_MODE())
        wal_get_ha_rebuild_reason_with_dummy(buildReason, local_role, isRunning);
    else
        wal_get_ha_rebuild_reason_with_multi(buildReason, local_role, isRunning);
}

/*
 * Descriptions: Returns activity of walreveiver, including pids and xlog
 * locations received from primary o  cascading server.
 */
Datum pg_stat_get_wal_receiver(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_WAL_RECEIVER_COLS 15
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupdesc = NULL;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;

    char location[MAXFNAMELEN] = {0};

    XLogRecPtr rcvRedo;
    XLogRecPtr rcvWrite;
    XLogRecPtr rcvFlush;
    bool isRuning = false;

    XLogRecPtr sndSent;
    XLogRecPtr sndWrite;
    XLogRecPtr sndFlush;
    XLogRecPtr sndReplay;
    XLogRecPtr rcvReceived;
    XLogRecPtr syncStart;

    int sync_percent = 0;
    ServerMode peer_role;
    DbState peer_state;
    DbState local_state;
    ServerMode local_role;
    char localip[IP_LEN] = {0};
    char remoteip[IP_LEN] = {0};
    int localport = 0;
    int remoteport = 0;
    Datum values[PG_STAT_GET_WAL_RECEIVER_COLS];
    bool nulls[PG_STAT_GET_WAL_RECEIVER_COLS];
    errno_t rc = EOK;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
        return (Datum)0;
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("materialize mode required, but it is not allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    (void)MemoryContextSwitchTo(oldcontext);

    SpinLockAcquire(&walrcv->mutex);
    isRuning = walrcv->isRuning;
    SpinLockRelease(&walrcv->mutex);

    if (walrcv->pid == 0 || !isRuning)
        return (Datum)0;

    SpinLockAcquire(&hashmdata->mutex);
    local_role = hashmdata->current_mode;
    SpinLockRelease(&hashmdata->mutex);

    SpinLockAcquire(&walrcv->mutex);
    localport = walrcv->conn_channel.localport;
    remoteport = walrcv->conn_channel.remoteport;
    rc = strncpy_s(localip, IP_LEN, (char *)walrcv->conn_channel.localhost, IP_LEN - 1);
    securec_check(rc, "\0", "\0");
    rc = strncpy_s(remoteip, IP_LEN, (char *)walrcv->conn_channel.remotehost, IP_LEN - 1);
    securec_check(rc, "\0", "\0");
    localip[IP_LEN - 1] = '\0';
    remoteip[IP_LEN - 1] = '\0';
    peer_role = walrcv->peer_role;
    peer_state = walrcv->peer_state;
    load_server_mode();
    local_state = get_local_dbstate();

    sndSent = walrcv->sender_sent_location;
    sndWrite = walrcv->sender_write_location;
    sndFlush = walrcv->sender_flush_location;
    sndReplay = walrcv->sender_replay_location;

    rcvReceived = walrcv->receiver_received_location;
    rcvRedo = walrcv->receiver_replay_location;
    rcvWrite = walrcv->receiver_write_location;
    rcvFlush = walrcv->receiver_flush_location;
    syncStart = walrcv->syncPercentCountStart;

    SpinLockRelease(&walrcv->mutex);

    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");
    values[0] = Int32GetDatum(walrcv->lwpId);

    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) {
        /*
         * Only superusers can see details. Other users only get the pid
         * value to know it's a receiver, but no details.
         */
        rc = memset_s(&nulls[1], PG_STAT_GET_WAL_RECEIVER_COLS - 1, true, PG_STAT_GET_WAL_RECEIVER_COLS - 1);
        securec_check(rc, "\0", "\0");
    } else {
        /* local_role */
        values[1] = CStringGetTextDatum(wal_get_role_string(local_role));
        /* peer_role */
        values[2] = CStringGetTextDatum(wal_get_role_string(peer_role, true));
        /* peer_state */
        values[3] = CStringGetTextDatum(wal_get_db_state_string(peer_state));
        /* state */
        values[4] = CStringGetTextDatum(wal_get_db_state_string(local_state));

        /* sender_sent_location */
        rc = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(sndSent >> 32),
                        (uint32)sndSent);
        securec_check_ss(rc, "\0", "\0");
        values[5] = CStringGetTextDatum(location);

        /* sender_write_location */
        if (sndWrite == 0)
            SETXLOGLOCATION(sndWrite, sndSent)
        rc = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(sndWrite >> 32),
                        (uint32)sndWrite);
        securec_check_ss(rc, "\0", "\0");
        values[6] = CStringGetTextDatum(location);

        /* sender_flush_location */
        if (sndFlush == 0)
            SETXLOGLOCATION(sndFlush, sndSent)
        rc = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(sndFlush >> 32),
                        (uint32)sndFlush);
        securec_check_ss(rc, "\0", "\0");
        values[7] = CStringGetTextDatum(location);

        /* sender_replay_location */
        if (sndReplay == 0)
            SETXLOGLOCATION(sndReplay, sndSent)
        rc = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(sndReplay >> 32),
                        (uint32)sndReplay);
        securec_check_ss(rc, "\0", "\0");
        values[8] = CStringGetTextDatum(location);

        /* receiver_received_location */
        if (rcvReceived == 0)
            SETXLOGLOCATION(rcvReceived, sndSent)
        rc = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(rcvReceived >> 32),
                        (uint32)rcvReceived);
        securec_check_ss(rc, "\0", "\0");
        values[9] = CStringGetTextDatum(location);

        /* receiver_write_location */
        if (rcvWrite == 0)
            SETXLOGLOCATION(rcvWrite, sndSent)
        rc = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(rcvWrite >> 32),
                        (uint32)rcvWrite);
        securec_check_ss(rc, "\0", "\0");
        values[10] = CStringGetTextDatum(location);

        /* receiver_flush_location */
        if (rcvFlush == 0)
            SETXLOGLOCATION(rcvFlush, sndSent)
        rc = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(rcvFlush >> 32),
                        (uint32)rcvFlush);
        securec_check_ss(rc, "\0", "\0");
        values[11] = CStringGetTextDatum(location);

        /* receiver_replay_location */
        if (rcvRedo == 0)
            SETXLOGLOCATION(rcvRedo, sndSent)
        rc = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%X/%X", (uint32)(rcvRedo >> 32),
                        (uint32)rcvRedo);
        securec_check_ss(rc, "\0", "\0");
        values[12] = CStringGetTextDatum(location);

        /* sync_percent */
        sync_percent = GetSyncPercent(syncStart, sndFlush, rcvFlush);
        rc = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%d%%", sync_percent);
        securec_check_ss(rc, "\0", "\0");
        values[13] = CStringGetTextDatum(location);

        /* channel */
        rc = snprintf_s(location, sizeof(location), sizeof(location) - 1, "%s:%d<--%s:%d", localip, localport, remoteip,
                        remoteport);
        securec_check_ss(rc, "\0", "\0");
        values[14] = CStringGetTextDatum(location);
    }
    tuplestore_putvalues(tupstore, tupdesc, values, nulls);

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tupstore);

    return (Datum)0;
}

/*
 * Returns activity of ha state, including static connections,local role,
 * database state and rebuild reason if database state is unnormal.
 */
Datum pg_stat_get_stream_replications(PG_FUNCTION_ARGS)
{
#define PG_STAT_GET_STREAM_REPLICATIONS_COLS 4
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    TupleDesc tupdesc = NULL;
    Tuplestorestate *tupstore = NULL;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    Datum values[PG_STAT_GET_STREAM_REPLICATIONS_COLS];
    bool nulls[PG_STAT_GET_STREAM_REPLICATIONS_COLS];

    ServerMode local_role;
    int static_connnections = 0;
    char buildReason[MAXFNAMELEN] = {0};

    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    bool isRunning = false;
    DbState db_state = UNKNOWN_STATE;
    errno_t rc = 0;
    load_server_mode();

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
        return (Datum)0;
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("materialize mode required, but it is not "
                                                                       "allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    (void)MemoryContextSwitchTo(oldcontext);
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check_c(rc, "\0", "\0");

    SpinLockAcquire(&walrcv->mutex);
    isRunning = walrcv->isRuning;
    SpinLockRelease(&walrcv->mutex);

    SpinLockAcquire(&hashmdata->mutex);
    local_role = hashmdata->current_mode;
    static_connnections = hashmdata->repl_list_num;
    SpinLockRelease(&hashmdata->mutex);

    wal_get_ha_rebuild_reason(buildReason, local_role, isRunning);
    /* If walreceiver has detected a system id not match reason before, then use it. */
    for (int i = 1; i <= hashmdata->repl_list_num; i++) {
        if ((hashmdata->repl_reason[i] == SYSTEMID_REBUILD || hashmdata->repl_reason[i] == WALSEGMENT_REBUILD)
            && strcmp(buildReason, "Connecting...") == 0) {
            rc = snprintf_s(buildReason, MAXFNAMELEN, MAXFNAMELEN - 1, "%s",
                wal_get_rebuild_reason_string(hashmdata->repl_reason[i]));
            securec_check_ss(rc, "\0", "\0");
            break;
        }
    }

    if (local_role == UNKNOWN_MODE)
        ereport(WARNING, (errmsg("server mode is unknown.")));

    /* local role */
    values[0] = CStringGetTextDatum(wal_get_role_string(local_role));
    /* static connections */
    values[1] = Int32GetDatum(static_connnections);
    /* db state */
    db_state = get_local_dbstate();
    values[2] = CStringGetTextDatum(wal_get_db_state_string(db_state));
    /* build_reason */
    values[3] = CStringGetTextDatum(buildReason);

    tuplestore_putvalues(tupstore, tupdesc, values, nulls);

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tupstore);

    return (Datum)0;
}

/*
 * we check the configure file every check_file_timeout, if
 * the configure has been modified, send the modify time to standy.
 */
static void ConfigFileTimer(void)
{
#ifndef ENABLE_MULTIPLE_NODES
    if (g_instance.attr.attr_common.sync_config_strategy == NONE_NODE) {
        return;
    }
#endif
    struct stat statbuf;
    char bufTime[sizeof(ConfigModifyTimeMessage) + 1];
    TimestampTz nowTime;

    if (t_thrd.walreceiver_cxt.check_file_timeout > 0) {
        nowTime = GetCurrentTimestamp();
        if (TimestampDifferenceExceeds(t_thrd.walreceiver_cxt.last_sendfilereply_timestamp, nowTime,
                                       t_thrd.walreceiver_cxt.check_file_timeout) ||
            TimestampDifferenceExceeds(nowTime, t_thrd.walreceiver_cxt.last_sendfilereply_timestamp,
                                       t_thrd.walreceiver_cxt.check_file_timeout)) {
            errno_t errorno = EOK;
            ereport(LOG, (errmsg("time is up to send file")));
            if (lstat(t_thrd.walreceiver_cxt.gucconf_file, &statbuf) != 0) {
                if (errno != ENOENT) {
                    ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file or directory \"%s\": %m",
                                                                      t_thrd.walreceiver_cxt.gucconf_file)));
                }
            }
            /* the configure file in standby has been change yet. */
            if (t_thrd.walreceiver_cxt.standby_config_modify_time != statbuf.st_mtime) {
                ereport(LOG,
                        (errmsg("statbuf.st_mtime:%d is not equal to config_modify_time:%d", (int)(statbuf.st_mtime),
                                (int)(t_thrd.walreceiver_cxt.standby_config_modify_time))));
                t_thrd.walreceiver_cxt.reply_modify_message->config_modify_time = 0;
            } else {
                ereport(LOG, (errmsg("the config file of standby has no change:%d", (int)(statbuf.st_mtime))));
                t_thrd.walreceiver_cxt.reply_modify_message->config_modify_time =
                    t_thrd.walreceiver_cxt.Primary_config_modify_time;
            }
            bufTime[0] = 'A';
            errorno = memcpy_s(&bufTime[1], sizeof(ConfigModifyTimeMessage),
                               t_thrd.walreceiver_cxt.reply_modify_message, sizeof(ConfigModifyTimeMessage));
            securec_check(errorno, "\0", "\0");
            (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_send(bufTime, sizeof(ConfigModifyTimeMessage) + 1);
            /* save the current timestamp */
            t_thrd.walreceiver_cxt.last_sendfilereply_timestamp = GetCurrentTimestamp();
        }
    }
}

static bool ProcessConfigFileMessage(char *buf, Size len)
{
    struct stat statbuf;
    ErrCode retcode = CODE_OK;
    ConfFileLock filelock = { NULL, 0 };
    char conf_bak[MAXPGPATH];
    int ret = 0;
    char **reserve_item = NULL;

    ret = snprintf_s(conf_bak, MAXPGPATH, MAXPGPATH - 1, "%s/%s", t_thrd.proc_cxt.DataDir, CONFIG_BAK_FILENAME);
    securec_check_ss(ret, "\0", "\0");

    if (lstat(t_thrd.walreceiver_cxt.gucconf_file, &statbuf) != 0) {
        if (errno != ENOENT)
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file or directory \"%s\": %m",
                                                              t_thrd.walreceiver_cxt.gucconf_file)));
        return false;
    }

    reserve_item = alloc_opt_lines(RESERVE_SIZE);
    if (reserve_item == NULL) {
        ereport(LOG, (errmsg("Alloc mem for reserved parameters failed")));
        return false;
    }

    /* 1. lock postgresql.conf */
    if (get_file_lock(t_thrd.walreceiver_cxt.gucconf_lock_file, &filelock) != CODE_OK) {
        release_opt_lines(reserve_item);
        ereport(LOG, (errmsg("Modify the postgresql.conf failed : can not get the file lock ")));
        return false;
    }

    /* 2. load reserved parameters to reserve_item(array in memeory) */
    retcode = copy_asyn_lines(t_thrd.walreceiver_cxt.gucconf_file, reserve_item, g_reserve_param);
    if (retcode != CODE_OK) {
        release_opt_lines(reserve_item);
        release_file_lock(&filelock);
        ereport(LOG, (errmsg("copy asynchronization items failed: %s\n", gs_strerror(retcode))));
        return false;
    }

    /* 3. genreate temp files and fill it with content from primary. */
    retcode = generate_temp_file(buf, conf_bak, len);
    if (retcode != CODE_OK) {
        release_opt_lines(reserve_item);
        release_file_lock(&filelock);
        ereport(LOG, (errmsg("create %s failed: %s\n", conf_bak, gs_strerror(retcode))));
        return false;
    }

    /* 4. adjust the info with reserved parameters, and sync to temp file. */
    retcode = update_temp_file(conf_bak, reserve_item, g_reserve_param);
    if (retcode != CODE_OK) {
        release_file_lock(&filelock);
        release_opt_lines(reserve_item);
        ereport(LOG, (errmsg("update gaussdb config file failed: %s\n", gs_strerror(retcode))));
        return false;
    } else {
        ereport(LOG, (errmsg("update gaussdb config file success")));
        if (rename(conf_bak, t_thrd.walreceiver_cxt.gucconf_file) != 0) {
            release_file_lock(&filelock);
            release_opt_lines(reserve_item);
            ereport(LOG, (errcode_for_file_access(), errmsg("could not rename \"%s\" to \"%s\": %m", conf_bak,
                                                            t_thrd.walreceiver_cxt.gucconf_file)));
            return false;
        }
    }

    /* save the modify time of standby config file */
    if (lstat(t_thrd.walreceiver_cxt.gucconf_file, &statbuf) != 0) {
        if (errno != ENOENT) {
            release_file_lock(&filelock);
            release_opt_lines(reserve_item);
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file or directory \"%s\": %m",
                                                              t_thrd.walreceiver_cxt.gucconf_file)));
            return false;
        }
    }
    t_thrd.walreceiver_cxt.standby_config_modify_time = statbuf.st_mtime;

    if (statbuf.st_size > 0) {
        copy_file_internal(t_thrd.walreceiver_cxt.gucconf_file, t_thrd.walreceiver_cxt.temp_guc_conf_file, true);
        ereport(DEBUG1, (errmsg("copy %s to %s success", t_thrd.walreceiver_cxt.gucconf_file,
                                t_thrd.walreceiver_cxt.temp_guc_conf_file)));
    }

    release_file_lock(&filelock);
    release_opt_lines(reserve_item);

    /* notify postmaster the config file has changed */
    if (gs_signal_send(PostmasterPid, SIGHUP) != 0) {
        ereport(WARNING, (errmsg("send SIGHUP to PM failed")));
        return false;
    }
    return true;
}

/*
 * firstSynchStandbyFile - Synchronise standby's configure file once the HA
 * build successfully.
 */
static void firstSynchStandbyFile(void)
{
#ifndef ENABLE_MULTIPLE_NODES
    if (g_instance.attr.attr_common.sync_config_strategy == NONE_NODE) {
        return;
    }
#endif
    char bufTime[sizeof(ConfigModifyTimeMessage) + 1];
    errno_t errorno = EOK;

    bufTime[0] = 'A';
    t_thrd.walreceiver_cxt.reply_modify_message->config_modify_time = 0;
    errorno = memcpy_s(&bufTime[1], sizeof(ConfigModifyTimeMessage), t_thrd.walreceiver_cxt.reply_modify_message,
                       sizeof(ConfigModifyTimeMessage));
    securec_check(errorno, "\0", "\0");
    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_send(bufTime, sizeof(ConfigModifyTimeMessage) + 1);
}

void GetPrimaryServiceAddress(char *address, size_t address_len)
{
    if (address == NULL || address_len == 0 || t_thrd.walreceiverfuncs_cxt.WalRcv == NULL)
        return;

    bool is_running = false;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    int rc = 0;

    SpinLockAcquire(&walrcv->mutex);
    is_running = walrcv->isRuning;
    SpinLockRelease(&walrcv->mutex);

    if (walrcv->pid == 0 || !is_running)
        return;

    SpinLockAcquire(&walrcv->mutex);
    rc = snprintf_s(address, address_len, (address_len - 1), "%s:%d", walrcv->conn_channel.remotehost,
                    walrcv->conn_channel.remoteservice);
    securec_check_ss(rc, "\0", "\0");
    SpinLockRelease(&walrcv->mutex);
}

void MakeDebugLog(TimestampTz sendTime, TimestampTz lastMsgReceiptTime, const char *msgFmt)
{
    char *sendtimeStr = NULL;
    char *receipttimeStr = NULL;
    /* Copy because timestamptz_to_str returns a static buffer */
    sendtimeStr = pstrdup(timestamptz_to_str(sendTime));
    receipttimeStr = pstrdup(timestamptz_to_str(lastMsgReceiptTime));
    ereport(DEBUG2, (errmsg(msgFmt, sendtimeStr, receipttimeStr)));
    pfree(sendtimeStr);
    sendtimeStr = NULL;
    pfree(receipttimeStr);
    receipttimeStr = NULL;
    return;
}

/* Set start send lsn for current walsender (only called in walsender) */
void WalRcvSetPercentCountStartLsn(XLogRecPtr startLsn)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    SpinLockAcquire(&walrcv->mutex);
    walrcv->syncPercentCountStart = startLsn;
    SpinLockRelease(&walrcv->mutex);
}

/* Set start send lsn for current walsender (only called in walsender) */
static void WalRcvRefreshPercentCountStartLsn(XLogRecPtr currentMaxLsn, XLogRecPtr currentDoneLsn)
{
    uint64 coundWindow = ((uint64)WalGetSyncCountWindow() * XLOG_SEG_SIZE);
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    XLogRecPtr baseStartLsn = InvalidXLogRecPtr;

    if (!walrcv) {
        return;
    }

    /* clear syncPercentCountStart when recevier's redo equal to sender's flush */
    if (XLByteEQ(currentMaxLsn, currentDoneLsn)) {
        WalRcvSetPercentCountStartLsn(InvalidXLogRecPtr);
        return;
    }

    /* if syncPercentCountStart is valid, it means last counting cycle has not been done. */
    SpinLockAcquire(&walrcv->mutex);
    baseStartLsn = walrcv->syncPercentCountStart;
    SpinLockRelease(&walrcv->mutex);
    if (!XLByteEQ(baseStartLsn, InvalidXLogRecPtr)) {
        return;
    }

    /* starting a new counting cycle. */
    if (XLogDiff(currentMaxLsn, currentDoneLsn) < coundWindow) {
        WalRcvSetPercentCountStartLsn(InvalidXLogRecPtr);
    } else {
        WalRcvSetPercentCountStartLsn(currentDoneLsn);
    }
}

/* 
 * SendArchiveStatus
 * This function is used to notify the primary of
 * whether archiving is enabled for standby.
 * 
 * If the value of status is true, archiving is enabled for the standby node.
 * Otherwise, archiving is disabled.
 * 
 * The targetLsn is the minimum lsn which is not archived.
 */
static void SendArchiveStatus(bool status)
{
    char buf[sizeof(ArchiveStatusMessage) + 1];
    ArchiveStatusMessage message;
    errno_t errorno = EOK;
    message.is_archive_activied = status;
    InitArchiveStartPoint();
    const char* status_string = status ? "on" : "off";
    ereport(LOG, (errmsg("the archive thread status is %s, and the start point of lsn is %ld",
                        status_string, g_instance.archive_standby_cxt.standby_archive_start_point)));
    message.startLsn = g_instance.archive_standby_cxt.standby_archive_start_point;
    buf[0] = 'S';
    errorno = memcpy_s(&buf[1],
        sizeof(ArchiveStatusMessage),
        &message,
        sizeof(ArchiveStatusMessage));
    securec_check(errorno, "\0", "\0");
    libpqrcv_send(buf, sizeof(ArchiveStatusMessage) + 1);
}

static void ProcessArchiveStatusResponse(ArchiveStatusResponseMessage* response)
{
    ereport(LOG, (errmsg("get updating archive status response")));
    bool is_success = response->is_set_status_success;
    if (is_success) {
        SetLatch(g_instance.archive_standby_cxt.arch_latch);
    } else {
        ereport(WARNING,
                (errcode(ERRCODE_WARNING),
                    errmsg("it is failed to notify the primary that updating the archive status of standby")));
    }
}

/*
 * InitArchiveStartPoint is used to find a start point which is the standby should check
 */
static void InitArchiveStartPoint()
{
    ReplicationSlot* obs_archive_slot = getObsReplicationSlot();
    if (!IsServerModeStandby() || obs_archive_slot != NULL) {
        return;
    }
    struct stat stat_buf;
    XLogRecPtr targetLsn = 0;
    XLogRecPtr flushPtr = t_thrd.walreceiverfuncs_cxt.WalRcv->walRcvCtlBlock->flushPtr;
    XLogRecPtr* standby_archive_start_point = &g_instance.archive_standby_cxt.standby_archive_start_point;
    XLogRecPtr backup = g_instance.archive_standby_cxt.standby_archive_start_point;
    DIR *xldir = NULL;
    struct dirent *xlde = NULL;

    errno_t errorno = EOK;
    xldir = AllocateDir(XLOGDIR);

    if (xldir == NULL) {
        ereport(ERROR,
                (errcode_for_file_access(),
                errmsg("InitArchiveStartPoint: could not open transaction log directory \"%s\": %m", XLOGDIR)));
    }

    /* find all xlog in the XLOGDIR and push it into a list */
    List* xlog_names = NIL;
    ListCell* xlog = NULL;
    while ((xlde = ReadDir(xldir, XLOGDIR)) != NULL){
        if (!CheckXlogNameValid(xlde->d_name)) {
            continue;
        }
        char* xlog_name = (char*)palloc(sizeof(char) * MAX_PATH);
        errorno = snprintf_s(xlog_name, MAX_PATH, MAX_PATH - 1, xlde->d_name);
        xlog_names = lappend(xlog_names, xlog_name);
    }
    if (xlog_names->length == 0) {
        *standby_archive_start_point = flushPtr;
        return;
    }

    /* cast the type List to array of string, and sort the array */
    char** xlog_array = (char**)palloc((sizeof(char*) * xlog_names->length));
    int count = 0;
    foreach(xlog, xlog_names) {
        xlog_array[count++] = (char*)lfirst(xlog);
    }
    qsort(xlog_array, xlog_names->length, sizeof(char*), XlogNameCmp);

    /* check the lsn which is the existsed xlog but not archived */
    for (;;) {
        targetLsn += XLogSegSize;
        char lastoff[MAXFNAMELEN];
        XLogSegNo segno = 0;
        XLByteToSeg(targetLsn, segno);
        errorno = snprintf_s(lastoff, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", t_thrd.xlog_cxt.ThisTimeLineID,
                            (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
        
        if (!CheckXlogNameValid(lastoff)) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FILE), errmsg("InitArchiveStartPoint:the lsn is error")));
            break;
        }

        bool is_xlog = false;
        for (int i = 0; i < xlog_names->length; i++) {
            char* xlog_name = xlog_array[i];
            int cmp_result = strcmp(lastoff, xlog_name);
            if (cmp_result < 0) {
                is_xlog = true;
                break;
            } else if (cmp_result > 0) {
                continue;
            } else {
                is_xlog = true;
                char statusPath[MAXPGPATH];
                errorno = snprintf_s(statusPath, MAXPGPATH, MAXPGPATH - 1, XLOGDIR "/archive_status/%s%s", lastoff,
                                    ".done");
                securec_check_ss(errorno, "", "");
                bool is_done = (stat(statusPath, &stat_buf) == 0);
        
                if (!is_done) {
                    *standby_archive_start_point = targetLsn;
                    ereport(LOG,
                            (errmsg("the start point is %X/%X",
                                    (uint32)(*standby_archive_start_point >> 32), (uint32)(*standby_archive_start_point))));
                    return;
                }
                break;
            }
        }
        if (!is_xlog) {
            ereport(LOG, (errmsg("can not found this xlog in the XLOGDIR")));
            break;
        }
    }

    /* we use the smaller lsn as the archive start point */
    if (backup <= *standby_archive_start_point) {
        *standby_archive_start_point = backup;
    }

    /* we use the flushPtr as the archive start point if we can't find a start point according to the xlog */
    if (*standby_archive_start_point == 0) {
        *standby_archive_start_point = flushPtr;
    }

    /* release all memory which is used to store the xlog name */
    foreach(xlog, xlog_names) {
        char* name = (char*)lfirst(xlog);
        pfree(name);
        name = NULL;
    }
    if (xlog_array != NULL) {
        pfree(xlog_array);
        xlog_array = NULL;
    }
}

static bool CheckXlogNameValid(char* xlog)
{
    if (strlen(xlog) != 24 || strspn(xlog, "0123456789ABCDEF") != 24) {
        return false;
    }
    return true;
}

int XlogNameCmp(const void* a, const void* b)
{
    return strcmp(*(char* const*)a, *(char* const*)b);
}