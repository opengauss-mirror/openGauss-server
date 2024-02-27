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
#include <string>

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
#include "replication/dcf_replication.h"
#include "replication/ss_disaster_cluster.h"
#include "storage/copydir.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/copydir.h"
#include "storage/procarray.h"
#include "storage/xlog_share_storage/xlog_share_storage.h"
#include "storage/gs_uwal/gs_uwal.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "gssignal/gs_signal.h"
#ifdef ENABLE_BBOX
#include "gs_bbox.h"
#endif

#include "flock.h"
#include "postmaster/postmaster.h"
#include "hotpatch/hotpatch.h"
#include "utils/distribute_test.h"

#include "lz4.h"

bool wal_catchup = false;

#define NAPTIME_PER_CYCLE 1 /* max sleep time between cycles (1ms) */

#define WAL_DATA_LEN ((sizeof(uint32) + 1 + sizeof(XLogRecPtr)))

#define TEMP_WAL_CONF_FILE "postgresql.conf.wal.bak"

const char *g_reserve_param[] = {
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
    "cross_cluster_replconninfo1",
    "cross_cluster_replconninfo2",
    "cross_cluster_replconninfo3",
    "cross_cluster_replconninfo4",
    "cross_cluster_replconninfo5",
    "cross_cluster_replconninfo6",
    "cross_cluster_replconninfo7",
    "cross_cluster_replconninfo8",
    "repl_uuid",
    "repl_auth_mode",
    "ssl",
    "ssl_ca_file",
    "ssl_cert_file",
    "ssl_ciphers",
    "ssl_crl_file",
    "ssl_key_file",
 #ifdef USE_TASSL
    "ssl_enc_cert_file",
    "ssl_enc_key_file",
 #endif
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
    "cluster_run_mode",
    "stream_cluster_run_mode",
    "xlog_file_size",
    "xlog_file_path",
    "xlog_lock_file_path",
    "auto_csn_barrier",
#ifndef ENABLE_MULTIPLE_NODES
    "recovery_min_apply_delay",
    "sync_config_strategy",
#else
    NULL,
    NULL,
#endif
#ifndef ENABLE_MULTIPLE_NODES
    "dcf_node_id",
    "dcf_data_path",
    "dcf_log_path",
#else
    NULL,
    NULL,
    NULL,
#endif
    "enable_huge_pages",
    "huge_page_size",
    "exrto_standby_read_opt",
    "huge_page_size",
    "enable_uwal",
    "uwal_config",
    "uwal_disk_size",
    "uwal_devices_path",
    "uwal_log_path",
    "uwal_rpc_compression_switch",
    "uwal_rpc_flowcontrol_switch",
    "uwal_rpc_flowcontrol_value",
    "uwal_async_append_switch"
};

const int g_reserve_param_num = lengthof(g_reserve_param);

const WalReceiverFunc WalReceiverFuncTable[] = {
    { libpqrcv_connect, libpqrcv_receive, libpqrcv_send, libpqrcv_disconnect, NULL, NULL, NULL, NULL },
    { archive_connect, archive_receive, archive_send, archive_disconnect, NULL, NULL, NULL, NULL },
    { shared_storage_connect, shared_storage_receive, shared_storage_send, shared_storage_disconnect,
        NULL, NULL, NULL, NULL},
    { sub_connect, libpqrcv_receive, libpqrcv_send, libpqrcv_disconnect, libpqrcv_exec, sub_identify_system,
        sub_startstreaming, sub_create_slot}
};

/* Prototypes for private functions */
static void EnableWalRcvImmediateExit(void);
static void DisableWalRcvImmediateExit(void);
static void WalRcvDie(int code, Datum arg);
static void XLogWalRcvDataPageReplication(char *buf, Size len);
static void XLogWalRcvProcessMsg(unsigned char type, char *buf, Size len);
static void XLogWalRcvSendHSFeedback(void);
static void XLogWalRcvSendSwitchRequest(void);
static void XLogWalRcvSendSwitchTimeoutRequest(void);
static void WalDataRcvReceive(char *buf, Size nbytes, XLogRecPtr recptr);
static void ProcessSwitchResponse(int code);
static void ProcessWalSndrMessage(XLogRecPtr *walEnd, TimestampTz sendTime);

static void ProcessKeepaliveMessage(PrimaryKeepaliveMessage *keepalive);
static void ProcessRmXLogMessage(RmXLogMessage *rmXLogMessage);
static void ProcessEndXLogMessage(EndXLogMessage *endXLogMessage);
static void ProcessWalHeaderMessage(WalDataMessageHeader *msghdr);
static void ProcessWalDataHeaderMessage(WalDataPageMessageHeader *msghdr);
Datum pg_stat_get_wal_receiver(PG_FUNCTION_ARGS);
/* Signal handlers */
static void WalRcvSigHupHandler(SIGNAL_ARGS);
static void WalRcvShutdownHandler(SIGNAL_ARGS);
static void WalRcvQuickDieHandler(SIGNAL_ARGS);
static void sigusr1_handler(SIGNAL_ARGS);
static void ConfigFileTimer(void);
static bool ProcessConfigFileMessage(char *buf, Size len);
static void firstSynchStandbyFile(void);

static TimestampTz GetHeartbeatLastReplyTimestamp();
static bool WalRecCheckTimeOut(TimestampTz nowtime, TimestampTz last_recv_timestamp, bool ping_sent);
static void WalRcvRefreshPercentCountStartLsn(XLogRecPtr currentMaxLsn, XLogRecPtr currentDoneLsn);
static void ProcessArchiveXlogMessage(const ArchiveXlogMessage* archive_xlog_message);
static void WalRecvSendArchiveXlogResponse(ArchiveTaskStatus *archive_status);
static void ProcessHadrSwitchoverRequest(HadrSwitchoverMessage *hadrSwitchoverMessage);
static void WalRecvHadrSwitchoverResponse();
#ifndef ENABLE_MULTIPLE_NODES
static void ResetConfirmedLSNOnDisk();
#endif
#ifdef ENABLE_MULTIPLE_NODES
static void WalRecvHadrSendReply();
#endif


void ProcessWalRcvInterrupts(void)
{
    /*
     * Although walreceiver interrupt handling doesn't use the same scheme as
     * regular backends, call CHECK_FOR_INTERRUPTS() to make sure we receive
     * any incoming signals on Win32.
     */
    CHECK_FOR_INTERRUPTS();

    if (t_thrd.walreceiver_cxt.got_SIGTERM) {
        t_thrd.walreceiver_cxt.got_SIGTERM = false;
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
    if (g_instance.wal_cxt.walReceiverStats->isEnableStat) {
        SpinLockAcquire(&g_instance.wal_cxt.walReceiverStats->mutex);
        volatile bool recheck = g_instance.wal_cxt.walReceiverStats->isEnableStat;
        if (recheck) {
            g_instance.wal_cxt.walReceiverStats->wakeWriterTimes++;
            TimestampTz time = GetCurrentTimestamp();
            if (g_instance.wal_cxt.walReceiverStats->firstWakeTime == 0) {
                g_instance.wal_cxt.walReceiverStats->firstWakeTime = time;
            }
            g_instance.wal_cxt.walReceiverStats->lastWakeTime = time;
        }
        SpinLockRelease(&g_instance.wal_cxt.walReceiverStats->mutex);
    }

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
    g_instance.wal_cxt.walReceiverStats->walRcvCtlBlock = t_thrd.walreceiver_cxt.walRcvCtlBlock;
#ifdef ENABLE_BBOX
    if (BBOX_BLACKLIST_WALREC_CTL_BLOCK) {
        bbox_blacklist_add(WALRECIVER_CTL_BLOCK, t_thrd.walreceiver_cxt.walRcvCtlBlock, len);
    }
#endif

    SpinLockInit(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
}

static void walRcvCtlBlockFini()
{
#ifdef ENABLE_BBOX
    if (BBOX_BLACKLIST_WALREC_CTL_BLOCK) {
        bbox_blacklist_remove(WALRECIVER_CTL_BLOCK, t_thrd.walreceiver_cxt.walRcvCtlBlock);
    }
#endif

    g_instance.wal_cxt.walReceiverStats->walRcvCtlBlock = nullptr;
    pfree(t_thrd.walreceiver_cxt.walRcvCtlBlock);
    t_thrd.walreceiver_cxt.walRcvCtlBlock = NULL;
}

/*
 * Clean up data in receive buffer.
 * This function should be called on thread exit.
 */
void walRcvDataCleanup()
{
    if (!g_instance.attr.attr_storage.enable_uwal) {
        while (WalDataRcvWrite() > 0) {
        };
    }
    WalRcvXLogClose();
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
    if (walrcv->conn_target != REPCONNTARGET_OBS) {
        RefuseConnect();
    }

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

    ArchiveTaskStatus *archive_task = walreceiver_find_archive_task_status(PITR_TASK_DONE);
    if (unlikely(archive_task != NULL)) {
        if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
#ifndef ENABLE_MULTIPLE_NODES
            DcfSendArchiveXlogResponse(archive_task);
#endif
        } else {
            WalRecvSendArchiveXlogResponse(archive_task);
        }
    }

    /* walrec in dorado replication mode not need walrecwrite */
    if (!SS_DORADO_MAIN_STANDBY_NODE && !WalRcvWriterInProgress())
        ereport(FATAL, (errmsg("terminating walreceiver process due to the death of walrcvwriter")));
#ifndef ENABLE_MULTIPLE_NODES
    /* For Paxos, receive wal should be done by send log callback function */
    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
        DCFSendXLogLocation();
        /* avoid frequent lock acquire */
        pg_usleep(10000); /* 10ms */
        return;
    }
#endif
    if (t_thrd.walreceiver_cxt.start_switchover && (walrcv->conn_target != REPCONNTARGET_OBS)) {
        t_thrd.walreceiver_cxt.start_switchover = false;
        XLogWalRcvSendSwitchRequest();
    }

    if (g_instance.stat_cxt.switchover_timeout) {
        g_instance.stat_cxt.switchover_timeout = false;
        XLogWalRcvSendSwitchTimeoutRequest();
        SendPostmasterSignal(PMSIGNAL_SWITCHOVER_TIMEOUT);
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
            ProcessWalRcvInterrupts();
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
        if (requestReply || get_walrcv_reply_dueto_commit()) {
            *ping_sent = true;
            *last_recv_timestamp = nowtime;
            requestReply = true;
            set_walrcv_reply_dueto_commit(false);
        }

        XLogWalRcvSendReply(requestReply, requestReply);
        XLogWalRcvSendHSFeedback();
    }
#ifdef ENABLE_MULTIPLE_NODES
    WalRecvHadrSendReply();
#endif
    ConfigFileTimer();
}

/* Main entry point for walreceiver process */
void WalReceiverMain(void)
{
    char conninfo[MAXCONNINFO];
    char slotname[NAMEDATALEN];
    XLogRecPtr startpoint;
    TimestampTz last_recv_timestamp = 0;
    bool ping_sent = false;
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    int channel_identifier = 0;
    int nRet = 0;
    errno_t rc = 0;

    if (SS_DISASTER_MAIN_STANDBY_NODE) {
        ereport(LOG, (errmsg("walreceiver thread started for main standby")));
    } else {
        Assert(ENABLE_DSS == false);    
    }

    t_thrd.walreceiver_cxt.last_sendfilereply_timestamp = GetCurrentTimestamp();
    t_thrd.walreceiver_cxt.standby_config_modify_time = time(NULL);

    knl_g_disconn_node_context_data disconn_node =
        g_instance.comm_cxt.localinfo_cxt.disable_conn_node.disable_conn_node_data;

    if (disconn_node.conn_mode == PROHIBIT_CONNECTION) {
        ereport(ERROR, (errmodule(MOD_WALRECEIVER), (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),
            (errmsg("Refuse WAL streaming when starting, ")), (errdetail("connection mode %d, connertion IP is %s:%d",
                disconn_node.conn_mode, disconn_node.disable_conn_node_host, disconn_node.disable_conn_node_port)))));
    }

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
    if (walrcv->conn_target == REPCONNTARGET_PRIMARY || walrcv->conn_target == REPCONNTARGET_OBS
        || walrcv->conn_target == REPCONNTARGET_SHARED_STORAGE) {
        walrcv->node_state = NODESTATE_NORMAL;
    }
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

    walrcv->walRcvCtlBlock = t_thrd.walreceiver_cxt.walRcvCtlBlock;
    if(walrcv->conn_target != REPCONNTARGET_OBS) {
        t_thrd.walreceiver_cxt.AmWalReceiverForFailover =
            (walrcv->conn_target == REPCONNTARGET_DUMMYSTANDBY || walrcv->conn_target == REPCONNTARGET_STANDBY) ? true
                                                                                                                : false;
        t_thrd.walreceiver_cxt.AmWalReceiverForStandby = (walrcv->conn_target == REPCONNTARGET_STANDBY) ? true : false;
        SpinLockRelease(&walrcv->mutex);
        /* using localport for channel identifier */
        if (!t_thrd.walreceiver_cxt.AmWalReceiverForStandby) {
            ReplConnInfo *replConnInfo = NULL;
            volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
            SpinLockAcquire(&hashmdata->mutex);
            int walreplindex = hashmdata->current_repl;
            SpinLockRelease(&hashmdata->mutex);

            if (!IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE && !SS_DISASTER_MAIN_STANDBY_NODE) {
                replConnInfo = t_thrd.postmaster_cxt.ReplConnArray[walreplindex];
            } else if (walreplindex >= MAX_REPLNODE_NUM) {
                replConnInfo = t_thrd.postmaster_cxt.CrossClusterReplConnArray[walreplindex - MAX_REPLNODE_NUM];
            }
            if (replConnInfo)
                channel_identifier = replConnInfo->localport;
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
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGURG, print_stack);
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
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Wal Receiver",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    /* Unblock signals (they were blocked when the postmaster forked us) */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
    if(walrcv->conn_target != REPCONNTARGET_OBS)
        SetWalRcvDummyStandbySyncPercent(0);
    
    t_thrd.xlog_cxt.ThisTimeLineID = GetRecoveryTargetTLI();
    if (!g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
        /* Establish the connection to the primary for XLOG streaming */
        EnableWalRcvImmediateExit();
        WalReceiverFuncTable[GET_FUNC_IDX].walrcv_connect(conninfo, &startpoint, slotname[0] != '\0' ? slotname : NULL,
                                                        channel_identifier);
        DisableWalRcvImmediateExit();

        if (GetWalRcvDummyStandbySyncPercent() == SYNC_DUMMY_STANDBY_END && !((walrcv->conn_target == REPCONNTARGET_OBS)
            || (walrcv->conn_target == REPCONNTARGET_SHARED_STORAGE))) {
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
    }

    if (t_thrd.proc_cxt.DataDir) {
        nRet = snprintf_s(t_thrd.walreceiver_cxt.gucconf_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf",
                          t_thrd.proc_cxt.DataDir);
        securec_check_ss(nRet, "\0", "\0");

        nRet = snprintf_s(t_thrd.walreceiver_cxt.temp_guc_conf_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s",
                          t_thrd.proc_cxt.DataDir, TEMP_WAL_CONF_FILE);
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

#ifndef ENABLE_MULTIPLE_NODES
    /* receivePtr should be set before it */
    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
        LaunchPaxos();
    }
#endif

    /*
     * For primary-standby, synchronize standby's configure file once the HA build successfully.
     *
     * Note: If switchover in one hour, and there is no parameter is reloaded,
     * the parameters set by client will be disabled. So we should do this.
     */
    if(walrcv->conn_target != REPCONNTARGET_OBS && !g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
        firstSynchStandbyFile();
        set_disable_conn_mode();
#ifndef ENABLE_MULTIPLE_NODES
        ResetConfirmedLSNOnDisk();
#endif
    }

    knl_g_set_redo_finish_status(REDO_FINISH_STATUS_LOCAL);
    ereport(LOG, (errmsg("set knl_g_set_redo_finish_status to false when connecting to the primary")));
    pgstat_report_appname("Wal Receiver");
    pgstat_report_activity(STATE_IDLE, NULL);
    /*
     * Prevent the effect of the last wallreceiver connection.
     */
    InitHeartbeatTimestamp();
    t_thrd.walreceiver_cxt.hasReceiveNewData = true;
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
    if (IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE || (IS_SHARED_STORAGE_PRIMARY_CLUSTER_STANDBY_MODE
        && !t_thrd.libwalreceiver_cxt.streamConn))
        return false;
    bool requestReply = false;
    WalReplicationTimestampInfo tpInfo;
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
        WalReplicationTimestampToString(&tpInfo, nowtime, calculateTime, last_recv_timestamp, heartbeat);
        ereport(ERROR, (errmsg(
            "terminating walreceiver due to heartbeat timeout,now time(%s) last heartbeat time(%s) calculateTime(%s)",
            tpInfo.nowTimeStamp, tpInfo.heartbeatStamp, tpInfo.timeoutStamp)));
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

bool HasBuildReason()
{
    /*
     * Consider a scenario, we have a primary node, a standby node and two cascade standby in az1.
     * The standby node stops running due to faults. We failover cascade standby A to standby.
     * During this period, services are still running on the primary, for example, inserting data.
     * Then we fix the original standby, we stop the cascade standby A and start the original standby.
     * In this scenario, cascade standby B has more xlog than those on the standby node, so cascade
     * standby B can not connect to the standby node. It is normal.
     * We don't need to rebuild cascade standby B, instead we establish a connection between cascade
     * standby B and standby until there is enough xlog on the standby node.
     */
    HaRebuildReason reason =
        t_thrd.postmaster_cxt.HaShmData->repl_reason[t_thrd.postmaster_cxt.HaShmData->current_repl];

    if ((t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE &&
        t_thrd.postmaster_cxt.HaShmData->is_cascade_standby) || SS_STREAM_CLUSTER) {
        return !(reason == NONE_REBUILD || reason == CONNECT_REBUILD || reason == WALSEGMENT_REBUILD);
    } else {
        return !(reason == NONE_REBUILD || reason == CONNECT_REBUILD);
    }
}

static void rcvAllXlog()
{
    if (SS_DORADO_CLUSTER) {
        return;
    }

    if (HasBuildReason() || t_thrd.walreceiver_cxt.checkConsistencyOK == false) {
        ereport(LOG, (errmsg("rcvAllXlog no need copy xlog buildreason:%d, check flag:%d",
            (int)t_thrd.postmaster_cxt.HaShmData->repl_reason[t_thrd.postmaster_cxt.HaShmData->current_repl],
            (int)t_thrd.walreceiver_cxt.checkConsistencyOK)));
        return;
    }
    unsigned char type;
    char *buf = NULL;
    int len;
    ereport(LOG, (errmsg("rcvAllXlog before walreceiver quit")));
    while(WalReceiverFuncTable[GET_FUNC_IDX].walrcv_receive(0, &type, &buf, &len)) {
        XLogWalRcvProcessMsg(type, buf, len);
        t_thrd.walreceiver_cxt.hasReceiveNewData = true;
    }

    const uint32 shiftSize = 32;
    ereport(LOG, (errmsg("rcvAllXlog dorado position:%x/%x, flush position: %x/%x",
        (uint32)(g_instance.xlog_cxt.shareStorageXLogCtl->insertHead >> shiftSize),
        (uint32)(g_instance.xlog_cxt.shareStorageXLogCtl->insertHead),
        (uint32)(t_thrd.walreceiver_cxt.walRcvCtlBlock->flushPtr >> shiftSize),
        (uint32)(t_thrd.walreceiver_cxt.walRcvCtlBlock->flushPtr))));
}

static void WalRcvClearSignalFlag()
{
    t_thrd.walreceiver_cxt.got_SIGHUP = false;
    t_thrd.walreceiver_cxt.got_SIGTERM = false;
    t_thrd.walreceiver_cxt.start_switchover = false;
}

/*
 * Mark us as STOPPED in proc exit.
 */
static void WalRcvDie(int code, Datum arg)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    if ((IS_SHARED_STORAGE_MODE || SS_DISASTER_CLUSTER) && !t_thrd.walreceiver_cxt.termChanged) {
        SpinLockAcquire(&walrcv->mutex);
        walrcv->walRcvState = WALRCV_STOPPING;
        SpinLockRelease(&walrcv->mutex);
        WalRcvClearSignalFlag();
        rcvAllXlog();
        uint32 disableConnectionNode = 
            pg_atomic_read_u32(&g_instance.comm_cxt.localinfo_cxt.need_disable_connection_node);
        if (disableConnectionNode && !t_thrd.walreceiver_cxt.termChanged) {
            pg_atomic_write_u32(&t_thrd.walreceiverfuncs_cxt.WalRcv->rcvDoneFromShareStorage, true);
        }
    }
    /*
     * Shutdown WalRcvWriter thread, clear the data receive buffer.
     * Ensure that all WAL records received are flushed to disk.
     */
    KillWalRcvWriter();

    /* we have to set REDO_FINISH_STATUS_LOCAL to false here, or there will be problems in this case:
       extremRTO is on, and DN received force finish signal, if cleanup is blocked, the force finish 
       signal will be ignored!
    */
    if (t_thrd.walreceiver_cxt.hasReceiveNewData) {
        knl_g_clear_local_redo_finish_status();
        ereport(LOG, (errmsg("set local_redo_finish_status to false in WalRcvDie")));
    }
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

    if (t_thrd.libwalreceiver_cxt.decompressBuf != NULL) {
        pfree(t_thrd.libwalreceiver_cxt.decompressBuf);
        t_thrd.libwalreceiver_cxt.decompressBuf = NULL;
    }

    /* reset conn_channel */
    errno_t rc = memset_s((void*)&walrcv->conn_channel, sizeof(walrcv->conn_channel),
                         0, sizeof(walrcv->conn_channel));
    securec_check_c(rc, "\0", "\0");

    if (g_instance.attr.attr_storage.enable_uwal) {
        ereport(LOG, (errmsg("walreceiver has been shut down, start notify changed")));
        if (GsUwalWalReceiverNotify(false) != 0) {
            ereport(FATAL, (errmsg("uwal standby notify for WalRcvDie() failed.")));
        }
        SpinLockAcquire(&walrcv->mutex);
        walrcv->flagAlreadyNotifyCatchup = false;
        SpinLockRelease(&walrcv->mutex);
    }
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
        t_thrd.walreceiverfuncs_cxt.WalRcv->node_state <= NODESTATE_EXTRM_FAST_DEMOTE_REQUEST) {
        /* Tell walreceiver process to start switchover */
        t_thrd.walreceiver_cxt.start_switchover = true;
    }

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
            errmsg_internal("unexpected wal data size %lu bytes exceeds the max receiving data queue size %u bytes",
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
            XLogWalRecordsPreProcess(&buf, &len, &msghdr);
            XLogWalRcvReceive(buf, len, msghdr.dataStart);
            break;
        }
        case 'C': /* Compressed WAL records */
        {
            WalDataMessageHeader msghdr;
            XLogWalRecordsPreProcess(&buf, &len, &msghdr);
            Size decompressedSize = (Size)XLogDecompression(buf, len, msghdr.dataStart);
            XLogWalRcvReceive(t_thrd.libwalreceiver_cxt.decompressBuf, decompressedSize, msghdr.dataStart);
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
#ifndef ENABLE_MULTIPLE_NODES
            if (IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE || AM_HADR_WAL_RECEIVER
                || SS_DISASTER_CLUSTER) {
#else
            if (IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE || AM_HADR_WAL_RECEIVER 
                || SS_DISASTER_CLUSTER || AM_HADR_CN_WAL_RECEIVER) {
#endif
                break;
            }
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
        case 'S':
        {   
            HadrSwitchoverMessage hadrSwithoverMessage;
            CHECK_MSG_SIZE(len, HadrSwitchoverMessage, "invalid HadrSwitchoverRequest message received from primary");
            /* memcpy is required here for alignment reasons */
            errorno = memcpy_s(&hadrSwithoverMessage, sizeof(HadrSwitchoverMessage), buf, sizeof(HadrSwitchoverMessage));
            securec_check(errorno, "\0", "\0");
            ProcessHadrSwitchoverRequest(&hadrSwithoverMessage);
            break;    
        }
        default:
            ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                            errmsg_internal("invalid replication message type %c", type)));
    }
}

void XLogWalRecordsPreProcess(char **buf, Size *len, WalDataMessageHeader *msghdr)
{
    errno_t errorno = EOK;
    if (*len < sizeof(WalDataMessageHeader))
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
            errmsg_internal("invalid WAL message received from primary")));
    /* memcpy is required here for alignment reasons */
    errorno = memcpy_s(msghdr, sizeof(WalDataMessageHeader), *buf, sizeof(WalDataMessageHeader));
    securec_check(errorno, "", "");
    
    ProcessWalHeaderMessage(msghdr);
    
    *buf += sizeof(WalDataMessageHeader);
    *len -= sizeof(WalDataMessageHeader);
}

int XLogDecompression(const char *buf, Size len, XLogRecPtr dataStart)
{
    char *decompressBuff = t_thrd.libwalreceiver_cxt.decompressBuf;
    const int maxBlockSize = g_instance.attr.attr_storage.WalReceiverBufSize * 1024;
    if (decompressBuff == NULL) {
        t_thrd.libwalreceiver_cxt.decompressBuf = (char *)palloc0(maxBlockSize);
        decompressBuff = t_thrd.libwalreceiver_cxt.decompressBuf;
    }
    int decompressedSize = LZ4_decompress_safe(buf, decompressBuff, len, maxBlockSize);
    if (decompressedSize <= 0) {
        ereport(ERROR, (errmsg("[DecompressFailed] startPtr %X/%X, compressedSize: %ld, decompressSize: %d",
                               (uint32)(dataStart >> 32), (uint32)dataStart, len, decompressedSize)));
    }
    ereport(DEBUG4, ((errmodule(MOD_REDO), errcode(ERRCODE_LOG),
        errmsg("[XLOG_COMPRESS] xlog decompression working! startPtr %X/%X, compressedSize %ld, decompressSize %d",
              (uint32)(dataStart >> 32), (uint32)dataStart, len, decompressedSize))));
    return decompressedSize;
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
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("the corrupt data total len is %u bytes, the expected len is %lu bytes.", total_len, nbytes)));
    }

    if (cur_buf[0] != 'd') {
        Assert(false);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
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
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
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

static void ProcessReplyFlags(void)
{
#ifdef ENABLE_MULTIPLE_NODES
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    if (IS_MULTI_DISASTER_RECOVER_MODE) {
        SpinLockAcquire(&walrcv->mutex);
        if (walrcv->isPauseByTargetBarrier) {
            t_thrd.walreceiver_cxt.reply_message->replyFlags |= IS_PAUSE_BY_TARGET_BARRIER;
        } else {
            t_thrd.walreceiver_cxt.reply_message->replyFlags &= ~IS_PAUSE_BY_TARGET_BARRIER;
        }
        SpinLockRelease(&walrcv->mutex);
        ereport(DEBUG4, ((errmodule(MOD_REDO), errcode(ERRCODE_LOG), errmsg(
                "[RcvSendReply] replyFlags=%d, receive=%lu, flush=%lu, apply=%lu",
                t_thrd.walreceiver_cxt.reply_message->replyFlags, t_thrd.walreceiver_cxt.reply_message->receive,
                t_thrd.walreceiver_cxt.reply_message->flush, t_thrd.walreceiver_cxt.reply_message->apply))));
    } else {
        t_thrd.walreceiver_cxt.reply_message->replyFlags &= ~IS_PAUSE_BY_TARGET_BARRIER;
    }
#else
    t_thrd.walreceiver_cxt.reply_message->replyFlags &= ~IS_PAUSE_BY_TARGET_BARRIER;
#endif

    /* if the standby has bad file, need cancel log ctl */
    if (RecoveryIsSuspend()) {
        t_thrd.walreceiver_cxt.reply_message->replyFlags |= IS_CANCEL_LOG_CTRL;
    } else {
        t_thrd.walreceiver_cxt.reply_message->replyFlags &= ~IS_CANCEL_LOG_CTRL;
    }
}

/*
 * Receive XLOG data into receiver buffer.
 */
void XLogWalRcvReceive(char *buf, Size nbytes, XLogRecPtr recptr)
{
    int walfreeoffset;
    int walwriteoffset;
    char *walrecvbuf = NULL;
    XLogRecPtr startptr;
    int recBufferSize = g_instance.attr.attr_storage.WalReceiverBufSize * 1024;
    bool isSameFull = false;

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
            if (g_instance.wal_cxt.walReceiverStats->isEnableStat && !isSameFull) {
                g_instance.wal_cxt.walReceiverStats->bufferFullTimes++;
                isSameFull = true;
            }
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

        isSameFull = false;
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

    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf)
        return;

    SpinLockAcquire(&walrcv->mutex);
    bool isStopping = (walrcv->walRcvState == WALRCV_STOPPING);
    SpinLockRelease(&walrcv->mutex);
    if (isStopping) {
        return;
    }

    /*
     * If the user doesn't want status to be reported to the master, be sure
     * to exit before doing anything at all.
     */
    if (!force && u_sess->attr.attr_storage.wal_receiver_status_interval <= 0)
        return;

    if (g_instance.attr.attr_storage.enable_uwal) {
        SpinLockAcquire(&walrcv->mutex);
        receivePtr = walrcv->receiver_received_location;
        writePtr = walrcv->receiver_write_location;
        flushPtr = walrcv->receiver_flush_location;
        SpinLockRelease(&walrcv->mutex);
    } else {
        SpinLockAcquire(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
        receivePtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr;
        writePtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->writePtr;
        flushPtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->flushPtr;
        SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
    }

    /* In shared storage, standby receivePtr equal dorado insertHead */
    if (IS_SHARED_STORAGE_STANBY_MODE) {
        ShareStorageXLogCtl *ctlInfo = AlignAllocShareStorageCtl();
        ReadShareStorageCtlInfo(ctlInfo);
        receivePtr = ctlInfo->insertHead;
        AlignFreeShareStorageCtl(ctlInfo);
    } else if (SS_DORADO_CLUSTER) {
        ReadSSDoradoCtlInfoFile();
        receivePtr = g_instance.xlog_cxt.ssReplicationXLogCtl->insertHead;
        writePtr = g_instance.xlog_cxt.ssReplicationXLogCtl->insertHead;
        flushPtr = g_instance.xlog_cxt.ssReplicationXLogCtl->insertHead;
    }
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
    ProcessReplyFlags();
    if (!dummyStandbyMode) {
        if (AM_HADR_WAL_RECEIVER) {
            /* for streaming disaster cluster, the main standby should collect all cascade standby info
             * then send lsn which satisfied Quorum to main cluster.
             */
            GetMinLsnRecordsFromHadrCascadeStandby();
        } else {
            t_thrd.walreceiver_cxt.reply_message->apply = GetXLogReplayRecPtr(NULL, &ReplayReadPtr);
            t_thrd.walreceiver_cxt.reply_message->applyRead = ReplayReadPtr;
        }
    } else {
        t_thrd.walreceiver_cxt.reply_message->apply = flushPtr;
        t_thrd.walreceiver_cxt.reply_message->applyRead = flushPtr;
    }
    t_thrd.walreceiver_cxt.reply_message->sendTime = now;
    t_thrd.walreceiver_cxt.reply_message->replyRequested = requestReply;

    SpinLockAcquire(&hashmdata->mutex);
    if (!IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE && !SS_DORADO_MAIN_STANDBY_NODE)
        t_thrd.walreceiver_cxt.reply_message->peer_role = hashmdata->current_mode;
    else
        t_thrd.walreceiver_cxt.reply_message->peer_role = STANDBY_CLUSTER_MODE;
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

void UWalCatchupEndRcvSendReply() {
    char buf[sizeof(UwalCatchEndMessage) + 1] = {0};
    UwalCatchEndMessage uwalCatchEndMessage;

    /* Prepend with the message type and send it. */
    buf[0] = 'w';
    int rc = memcpy_s(&buf[1], sizeof(UwalCatchEndMessage), &uwalCatchEndMessage,
                  sizeof(uwalCatchEndMessage));
    securec_check(rc, "\0", "\0");
    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_send(buf, sizeof(UwalCatchEndMessage) + 1);
    ereport(LOG, (errmsg("send uwal catchup end message.")));
}

/*
 * Send hot standby feedback message to primary, plus the current time,
 * in case they don't have a watch.
 */
static void XLogWalRcvSendHSFeedback(void)
{
#ifdef ENABLE_MULTIPLE_NODES
    return;
#endif
    char buf[sizeof(StandbyHSFeedbackMessage) + 1];
    TimestampTz now;
    TransactionId xmin;
    errno_t rc = 0;
    /*
     * If the user doesn't want status to be reported to the master, be sure
     * to exit before doing anything at all.
     */
    if (u_sess->attr.attr_storage.wal_receiver_status_interval <= 0)
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

    if (u_sess->attr.attr_storage.hot_standby_feedback) {
#ifndef ENABLE_MULTIPLE_NODES
        /* Get updated RecentGlobalXmin */
        GetSnapshotData(u_sess->utils_cxt.CurrentSnapshotData, true, true);
#endif

        /*
         * Make the expensive call to get the oldest xmin once we are certain
         * everything else has been checked.
         */
        xmin = GetOldestXmin(NULL);
    } else {
        xmin = InvalidTransactionId;
    }
    t_thrd.pgxact->xmin = InvalidTransactionId;

    t_thrd.proc->exrto_read_lsn = 0;
    t_thrd.proc->exrto_min = 0;
    t_thrd.proc->exrto_gen_snap_time = 0;
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

    if (!IS_SHARED_STORAGE_STANBY_MODE) {
        walrcv->sender_sent_location = msghdr->sender_sent_location;

        walrcv->sender_flush_location = msghdr->sender_flush_location;

        walrcv->sender_replay_location = msghdr->sender_replay_location;

        walrcv->sender_write_location = msghdr->sender_write_location;
    }
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
    XLogRecPtr lastReceived = InvalidXLogRecPtr;

    /* Update shared-memory status */
    SpinLockAcquire(&walrcv->mutex);
    walrcv->peer_role = keepalive->peer_role;
    walrcv->peer_state = keepalive->peer_state;
    
    bool uwal_update_lsn = (g_instance.attr.attr_storage.enable_uwal &&
                            walrcv->sender_sent_location < keepalive->walEnd && walrcv->flagAlreadyNotifyCatchup);
    if (uwal_update_lsn) {
        lastReceived = walrcv->receivedUpto;        
        walrcv->sender_write_location = keepalive->walEnd;
        walrcv->sender_flush_location = keepalive->walEnd;
        walrcv->sender_replay_location = keepalive->walEnd;
        walrcv->receiver_received_location = keepalive->walEnd;
        walrcv->receiver_write_location = keepalive->walEnd;
        walrcv->receiver_flush_location = keepalive->walEnd;
        walrcv->receivedUpto = keepalive->walEnd;
    }
    walrcv->sender_sent_location = keepalive->walEnd;
    walrcv->lastMsgSendTime = keepalive->sendTime;
    walrcv->lastMsgReceiptTime = lastMsgReceiptTime;
    SpinLockRelease(&walrcv->mutex);

    if (g_instance.attr.attr_storage.enable_uwal) {
        wal_catchup = keepalive->catchup || keepalive->uwal_catchup;
    } else {
        wal_catchup = keepalive->catchup;
    }

    if (g_instance.attr.attr_storage.enable_uwal && keepalive->uwal_catchup && !walrcv->flagAlreadyNotifyCatchup) {
        walrcv->flagAlreadyNotifyCatchup = true;
        if (GsUwalWalReceiverNotify() != 0) {
            ereport(FATAL, (errmsg("uwal standby notify failed.")));
            proc_exit(1);
        }
        UWalCatchupEndRcvSendReply();
    }
    if (walrcv->flagAlreadyNotifyCatchup) {
        UwalrcvWriterState *uwalrcv = GsGetCurrentUwalRcvState();
        SpinLockAcquire(&uwalrcv->mutex);
        uwalrcv->fullSync = keepalive->fullSync;
        SpinLockRelease(&uwalrcv->mutex);
    }
    // wake up the startup thread to redo
    if (uwal_update_lsn) {
        WakeupRecovery();
        if (lastReceived < walrcv->receivedUpto) {
            GsUwalRcvStateUpdate(lastReceived);
        }
    }

    if (log_min_messages <= DEBUG2) {
        MakeDebugLog(keepalive->sendTime, lastMsgReceiptTime, "wal receive keep alive data sendtime %s receipttime %s");
        ereport(DEBUG2, (errmodule(MOD_WALRECEIVER), errmsg("replication apply delay %d ms transfer latency %d ms",
                                                            GetReplicationApplyDelay(),
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

    SpinLockAcquire(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
    lastFlushPtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->flushPtr;
    t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr = t_thrd.walreceiver_cxt.walRcvCtlBlock->writePtr =
        t_thrd.walreceiver_cxt.walRcvCtlBlock->flushPtr = InvalidXLogRecPtr;
    t_thrd.walreceiver_cxt.walRcvCtlBlock->walStart = InvalidXLogRecPtr;
    t_thrd.walreceiver_cxt.walRcvCtlBlock->walWriteOffset = t_thrd.walreceiver_cxt.walRcvCtlBlock->walFreeOffset = 0;
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
    ereport(LOG, (errmsg("get archive xlog message %s :%X/%X", archive_xlog_message->slot_name, 
        (uint32)(archive_xlog_message->targetLsn >> 32), (uint32)(archive_xlog_message->targetLsn))));
    errno_t errorno = EOK;
    ArchiveTaskStatus *archive_task = find_archive_task_status(archive_xlog_message->slot_name);
    if (archive_task == NULL) {
        ereport(WARNING, (errmsg("get archive xlog message %s :%X/%X, but task slot not find",
            archive_xlog_message->slot_name, 
            (uint32)(archive_xlog_message->targetLsn >> 32), 
            (uint32)(archive_xlog_message->targetLsn))));
        return;
    }
    volatile unsigned int *pitr_task_status = &archive_task->pitr_task_status;
    if (archive_xlog_message->targetLsn == InvalidXLogRecPtr) {
        pg_atomic_write_u32(pitr_task_status, PITR_TASK_NONE);
    }
    unsigned int expected = PITR_TASK_NONE;
    int failed_times = 0;
    /* lock for archiver get PITR_TASK_GET flag, but works on old task . 
    * if archiver works between set flag and set task details.
    */
    while (pg_atomic_compare_exchange_u32(pitr_task_status, &expected, PITR_TASK_GET) == false) {
        /* some task arrived before last task done if expected not equal to NONE */
        expected = PITR_TASK_NONE;
        pg_usleep(ARCHIVE_XLOG_DELAY);  // sleep 0.01s
        if (failed_times++ >= GET_ARCHIVE_XLOG_RETRY_MAX) {
            ereport(WARNING, (errmsg("get archive xlog message %s :%X/%X, but not finished",
                                     archive_xlog_message->slot_name,
                                     (uint32)(archive_xlog_message->targetLsn >> 32),
                                     (uint32)(archive_xlog_message->targetLsn))));
            return;
        }
    }
    SpinLockAcquire(&archive_task->mutex);
    errorno = memcpy_s(&archive_task->archive_task,
        sizeof(ArchiveXlogMessage) + 1,
        archive_xlog_message,
        sizeof(ArchiveXlogMessage));
    securec_check(errorno, "\0", "\0");
    SpinLockRelease(&archive_task->mutex);
    wakeupObsArchLatch();
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
 * Send switchover timeout request message to primary.
 */
static void XLogWalRcvSendSwitchTimeoutRequest(void)
{
    char buf = 'b';
    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_send(&buf, 1);
    ereport(LOG, (errmsg("send switchover timeout request to primary")));
}

/*
 * Send archive xlog response message to primary.
 */
static void WalRecvSendArchiveXlogResponse(ArchiveTaskStatus *archive_task)
{
    if (archive_task == NULL) {
        return;
    }
    char buf[sizeof(ArchiveXlogResponseMessage) + 1];
    ArchiveXlogResponseMessage reply;
    errno_t errorno = EOK;
    SpinLockAcquire(&archive_task->mutex);
    reply.pitr_result = archive_task->pitr_finish_result;
    reply.targetLsn = archive_task->archive_task.targetLsn;
    SpinLockRelease(&archive_task->mutex);
    errorno = memcpy_s(&reply.slot_name, NAMEDATALEN, archive_task->slotname, NAMEDATALEN);
    securec_check(errorno, "\0", "\0");

    buf[0] = 'a';
    errorno = memcpy_s(&buf[1],
        sizeof(ArchiveXlogResponseMessage),
        &reply,
        sizeof(ArchiveXlogResponseMessage));
    securec_check(errorno, "\0", "\0");
    libpqrcv_send(buf, sizeof(ArchiveXlogResponseMessage) + 1);

    ereport(LOG,
        (errmsg("WalRecvSendArchiveXlogResponse %s:%d %X/%X", reply.slot_name, reply.pitr_result, 
            (uint32)(reply.targetLsn >> 32), (uint32)(reply.targetLsn))));
    volatile unsigned int *pitr_task_status = &archive_task->pitr_task_status;
    pg_memory_barrier();
    pg_atomic_write_u32(pitr_task_status, PITR_TASK_NONE);

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
            startLsn = totalLsn - (WalGetSyncCountWindow() * XLogSegSize);
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

    char location[MAXFNAMELEN * 3] = {0};

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

    if (IS_SHARED_STORAGE_MODE) {
        XLogRecPtr sendLocFix = rcvReceived;
        ShareStorageXLogCtl *ctlInfo = AlignAllocShareStorageCtl();
        ReadShareStorageCtlInfo(ctlInfo);
        if (XLByteLT(sendLocFix, ctlInfo->insertHead)) {
            sendLocFix = ctlInfo->insertHead;
        }
        AlignFreeShareStorageCtl(ctlInfo);
        sndSent = sendLocFix;
        sndWrite = sendLocFix;
        sndFlush = sendLocFix;
        sndReplay = sendLocFix;
    }

    if (SS_DORADO_CLUSTER) {
        ReadSSDoradoCtlInfoFile();
        XLogRecPtr sendLocFix = rcvReceived;
        ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.ssReplicationXLogCtl;
        if (XLByteLT(sendLocFix, ctlInfo->insertHead)) {
            sendLocFix = ctlInfo->insertHead;
        }
        sndSent = sendLocFix;
        sndWrite = sendLocFix;
        sndFlush = sendLocFix;
        sndReplay = sendLocFix;
    }

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
    if (g_instance.attr.attr_storage.dms_attr.enable_dms) {
        values[0] = CStringGetTextDatum(GetSSServerMode(local_role));
    } else {
        values[0] = CStringGetTextDatum(wal_get_role_string(local_role));
    }
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

    ret = snprintf_s(conf_bak, MAXPGPATH, MAXPGPATH - 1, "%s/%s", t_thrd.proc_cxt.DataDir, CONFIG_BAK_FILENAME_WAL);
    securec_check_ss(ret, "\0", "\0");

    if (lstat(t_thrd.walreceiver_cxt.gucconf_file, &statbuf) != 0) {
        if (errno != ENOENT)
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file or directory \"%s\": %m",
                                                              t_thrd.walreceiver_cxt.gucconf_file)));
        return false;
    }

    reserve_item = alloc_opt_lines(g_reserve_param_num);
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
    LWLockAcquire(ConfigFileLock, LW_EXCLUSIVE);


    /* 2. load reserved parameters to reserve_item(array in memeory) */
    retcode = copy_asyn_lines(t_thrd.walreceiver_cxt.gucconf_file, reserve_item, g_reserve_param);
    if (retcode != CODE_OK) {
        release_opt_lines(reserve_item);
        release_file_lock(&filelock);
        LWLockRelease(ConfigFileLock);
        ereport(LOG, (errmsg("copy asynchronization items failed: %s\n", gs_strerror(retcode))));
        return false;
    }

    /* 3. genreate temp files and fill it with content from primary. */
    retcode = generate_temp_file(buf, conf_bak, len);
    if (retcode != CODE_OK) {
        release_opt_lines(reserve_item);
        release_file_lock(&filelock);
        LWLockRelease(ConfigFileLock);
        ereport(LOG, (errmsg("create %s failed: %s\n", conf_bak, gs_strerror(retcode))));
        return false;
    }

    /* 4. adjust the info with reserved parameters, and sync to temp file. */
    retcode = update_temp_file(conf_bak, reserve_item, g_reserve_param);
    if (retcode != CODE_OK) {
        release_file_lock(&filelock);
        LWLockRelease(ConfigFileLock);
        release_opt_lines(reserve_item);
        ereport(LOG, (errmsg("update gaussdb config file failed: %s\n", gs_strerror(retcode))));
        return false;
    } else {
        ereport(LOG, (errmsg("update gaussdb config file success")));
        if (rename(conf_bak, t_thrd.walreceiver_cxt.gucconf_file) != 0) {
            release_file_lock(&filelock);
            LWLockRelease(ConfigFileLock);
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
            LWLockRelease(ConfigFileLock);
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
    LWLockRelease(ConfigFileLock);
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
    uint64 coundWindow = ((uint64)WalGetSyncCountWindow() * XLogSegSize);
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
 * Process ProcessKeepaliveMessage received from sender, message type is 'S'.
 */
static void ProcessHadrSwitchoverRequest(HadrSwitchoverMessage *hadrSwitchoverMessage)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    XLogRecPtr last_flush_location = walrcv->receiver_flush_location;
    XLogRecPtr last_replay_location = GetXLogReplayRecPtr(NULL);
    SpinLockAcquire(&walrcv->mutex);
    walrcv->targetSwitchoverBarrierLSN = hadrSwitchoverMessage->switchoverBarrierLsn;
    walrcv->isMasterInstanceReady = hadrSwitchoverMessage->isMasterInstanceReady;
    SpinLockRelease(&walrcv->mutex);

    if (g_instance.streaming_dr_cxt.isInSwitchover && 
        XLByteEQ(walrcv->targetSwitchoverBarrierLSN, walrcv->lastSwitchoverBarrierLSN) &&
        XLByteEQ(last_flush_location, last_replay_location) &&
        XLByteEQ(walrcv->targetSwitchoverBarrierLSN, last_replay_location)) {
        WalRecvHadrSwitchoverResponse();
        if (walrcv->isMasterInstanceReady) {
            g_instance.streaming_dr_cxt.isInteractionCompleted = true;
        }
    }

    ereport(LOG,(errmsg("ProcessHadrSwitchoverRequest: "
            "is_hadr_main_standby: %d, is_cn: %d, target switchover barrier lsn  %X/%X, "
            "receive switchover barrier lsn %X/%X, last_replay_location %X/%X, "
            "last_flush_location %X/%X, isInteractionCompleted %d",
            t_thrd.xlog_cxt.is_hadr_main_standby, IS_PGXC_COORDINATOR,
            (uint32)(walrcv->targetSwitchoverBarrierLSN >> 32),
            (uint32)(walrcv->targetSwitchoverBarrierLSN),
            (uint32)(walrcv->lastSwitchoverBarrierLSN >> 32),
            (uint32)(walrcv->lastSwitchoverBarrierLSN),
            (uint32)(last_replay_location >> 32),
            (uint32)(last_replay_location),
            (uint32)(last_flush_location >> 32),
            (uint32)(last_flush_location),
            g_instance.streaming_dr_cxt.isInteractionCompleted)));
}

/*
 * Send streaming dr switchover response message to primary.
 */
static void WalRecvHadrSwitchoverResponse()
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    HadrSwitchoverMessage hadrSwithoverMessage;
    char buf[sizeof(HadrSwitchoverMessage) + 1];
    errno_t errorno = EOK;

    SpinLockAcquire(&walrcv->mutex);
    hadrSwithoverMessage.switchoverBarrierLsn = walrcv->targetSwitchoverBarrierLSN;
    SpinLockRelease(&walrcv->mutex);

    /* Prepend with the message type and send it. */
    buf[0] = 'S';
    errorno = memcpy_s(&buf[1], sizeof(HadrSwitchoverMessage), &hadrSwithoverMessage,
        sizeof(HadrSwitchoverMessage));
    securec_check(errorno, "\0", "\0");
    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_send(buf, sizeof(HadrSwitchoverMessage) + 1);

    ereport(LOG, (errmsg("send streaming dr switchover response to primary")));
}

/* remove any '%zone' part from an IPv6 address string */
char* remove_ipv6_zone(char* addr_src, char* addr_dest, int len)
{
    /* check if has ipv6 and ipv6 addr include '%zone' */
    if (strchr(addr_src, '%') == NULL) {
        return addr_src;
    } else {
        char* pct = NULL;
        errno_t rc = 0;
        rc = strncpy_s(addr_dest, len, addr_src, len - 1);
        securec_check(rc, "", "");
        addr_dest[len - 1] = '\0';
        pct = strchr(addr_dest, '%');
        if (pct != NULL) {
            *pct = '\0';
        }
        return addr_dest;
    }

    /* for compile no error */
    return addr_src;
}

#ifdef ENABLE_MULTIPLE_NODES
static void WalRecvHadrSendReply()
{
    if (!(AM_HADR_WAL_RECEIVER || AM_HADR_CN_WAL_RECEIVER)) {
        return;
    }
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    HadrReplyMessage hadrReply;
    char buf[sizeof(HadrReplyMessage) + 1];
    errno_t errorno = EOK;

    SpinLockAcquire(&walrcv->mutex);
    if (!IS_CSN_BARRIER((char *)walrcv->recoveryTargetBarrierId)) {
        SpinLockRelease(&walrcv->mutex);
        return;
    }
    errorno = strncpy_s((char *)hadrReply.targetBarrierId, MAX_BARRIER_ID_LENGTH,
        (char *)walrcv->recoveryTargetBarrierId, MAX_BARRIER_ID_LENGTH);
    SpinLockRelease(&walrcv->mutex);
    securec_check(errorno, "\0", "\0");
    hadrReply.sendTime = GetCurrentTimestamp();

    /* Prepend with the message type and send it. */
    buf[0] = 'R';
    errorno = memcpy_s(&buf[1], sizeof(HadrReplyMessage), &hadrReply, sizeof(HadrReplyMessage));
    securec_check(errorno, "\0", "\0");
    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_send(buf, sizeof(HadrReplyMessage) + 1);

    ereport(DEBUG5, (errmsg("send streaming dr reply to primary, barrier id %s", hadrReply.targetBarrierId)));
}
#endif

#ifndef ENABLE_MULTIPLE_NODES
static void ResetConfirmedLSNOnDisk()
{
    if (!g_instance.attr.attr_storage.enable_save_confirmed_lsn ||
        !IsServerModeStandby()) {
        return;
    }

    int i;
    bool modified = false;

    ereport(DEBUG1, (errmsg("Reset the confirmed LSN info of replication slot.")));
    for (i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *s = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        if (!s->in_use || GET_SLOT_PERSISTENCY(s->data) != RS_PERSISTENT || XLogRecPtrIsInvalid(s->data.confirmed_flush))
            continue;

        SpinLockAcquire(&s->mutex);
        s->data.confirmed_flush = InvalidXLogRecPtr;
        s->just_dirtied = true;
        s->dirty = true;
        modified = true;
        SpinLockRelease(&s->mutex);
    }

    if (modified) {
        CheckPointReplicationSlots();
    }
}
#endif
