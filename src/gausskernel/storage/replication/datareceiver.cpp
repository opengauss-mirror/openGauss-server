/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 *
 *
 * datareceiver.cpp
 *
 * The data receiver process (datareceiver) is new as of MppDB. It
 * is the process in the standby server that takes charge of receiving
 * data pages from a primary server during streaming replication.
 *
 * When the startup process determines that it's time to start streaming,
 * it instructs postmaster to start datareceiver. Datareceiver first connects
 * to the primary server (it will be served by a datasender process
 * in the primary server), and then keeps receiving data pages and
 * writing them to the disk as long as the connection is alive.
 *
 * Normal termination is by SIGTERM, which instructs the datareceiver to
 * exit(0). Emergency termination is by SIGQUIT; like any postmaster child
 * process, the datareceiver will simply abort and exit on SIGQUIT. A close
 * of the connection and a FATAL error are treated not as a crash but as
 * normal operation.
 *
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/replication/datareceiver.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif
#ifndef WIN32
#include <syscall.h>
#endif
#include <sys/stat.h>

#include "libpq/libpq-fe.h"
#include "flock.h"
#include "gssignal/gs_signal.h"
#include "nodes/execnodes.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "postmaster/startup.h"
#include "replication/dataprotocol.h"
#include "replication/dataqueue.h"
#include "replication/datareceiver.h"
#include "replication/datasender.h"
#include "replication/walreceiver.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/procarray.h"
#include "storage/smgr/smgr.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#ifdef ENABLE_BBOX
#include "gs_bbox.h"
#endif


#ifdef ENABLE_UT
#define static
#endif

/* max sleep time between cycles (1ms) */
#define NAPTIME_PER_CYCLE 1

/*
 * How long to wait for datareceiver to start up after requesting
 * postmaster to launch it. In seconds.
 */
#define DATARCV_STARTUP_TIMEOUT 10

extern bool dummy_data_writer_use_file;
extern bool dummyStandbyMode;
bool data_catchup = false;

/*
 * About SIGTERM handling:
 *
 * We can't just exit(1) within SIGTERM signal handler, because the signal
 * might arrive in the middle of some critical operation, like while we're
 * holding a spinlock. We also can't just set a flag in signal handler and
 * check it in the main loop, because we perform some blocking operations
 * like libpqrcv_PQexec(), which can take a long time to finish.
 *
 * We use a combined approach: When DataRcvImmediateInterruptOK is true, it's
 * safe for the signal handler to elog(FATAL) immediately. Otherwise it just
 * sets got_SIGTERM flag, which is checked in the main loop when convenient.
 *
 * This is very much like what regular backends do with t_thrd.int_cxt.ImmediateInterruptOK,
 * ProcessInterrupts() etc.
 */
static THR_LOCAL volatile bool DataRcvImmediateInterruptOK = false;

/* Prototypes for private functions */
static void EnableDataRcvImmediateExit(void);
static void DisableDataRcvImmediateExit(void);
static void DataRcvDie(int code, Datum arg);
static void SetDataRcvConninfo(ReplConnTarget conn_target);
static void ShutDownDataRcvWriter(void);

/* Message */
static void DataRcvProcessMsg(unsigned char type, char *buf, Size len);
static void DataRcvReceive(char *buf, Size nbytes);

static void ProcessKeepaliveMessage(DataSndKeepaliveMessage *keepalive);
static void ProcessDataHeaderMessage(DataPageMessageHeader *msghdr);
static void ProcessEndDataMessage(EndDataMessage *endDataMessage);
static void ProcessRmDataMessage(RmDataMessage *rmDataMessage);
static void ProcessRmData(void);

/* Signal handlers */
static void DataRcvSigHupHandler(SIGNAL_ARGS);
static void DataRcvShutdownHandler(SIGNAL_ARGS);
static void DataRcvQuickDieHandler(SIGNAL_ARGS);

/* Stream connection */
static void DataRcvStreamConnect(char *conninfo);
static void DataRcvStreamSend(const char *buffer, int nbytes);
static void DataRcvStreamSendFiles(const char *buffer, int nbytes);

static void DataRcvStreamDisconnect(void);
static bool DataRcvStreamReceive(int timeout, unsigned char *type, char **buffer, int *len);
static bool DataRcvStreamSelect(int timeout_ms);

/* Data process handlers */
static void InitDummyFileNum(uint *readFileNum, uint *writerFileNum);
static void ParseDummyFile();
static void ReadDummyFile(HTAB *relFileNodeTab, uint32 readFileNum, int *bufferSize);
static bool ReadDummySlice(FILE *readFileFd, char *buf, uint32 length, char *path);
static int ParseDataHeader(HTAB *relFileNodeTab, char *bufPtr, uint32 nbytes);

void DataReceiverPing(bool *ping_ptr, TimestampTz *last_recv_timestamp_ptr)
{
    /*
     * We didn't receive anything new. If we haven't heard anything
     * from the server for more than wal_receiver_timeout / 2,
     * ping the server. Also, if it's been longer than
     * wal_receiver_status_interval since the last update we sent,
     * send a status update to the master anyway, to report any
     * progress in applying WAL.
     */
    bool requestReply = false;

    /*
     * Check if time since last receive from master has reached the
     * configured limit.
     */
    if (u_sess->attr.attr_storage.wal_receiver_timeout > 0) {
        TimestampTz nowtime = GetCurrentTimestamp();
        TimestampTz timeout = 0;

        timeout = TimestampTzPlusMilliseconds(*last_recv_timestamp_ptr,
                                              u_sess->attr.attr_storage.wal_receiver_timeout / 2);

        /*
         * We didn't receive anything new, for half of receiver
         * replication timeout. Ping the server.
         */
        if (nowtime >= timeout) {
            if (!(*ping_ptr)) {
                requestReply = true;
                *ping_ptr = true;
                *last_recv_timestamp_ptr = nowtime;
            } else {
                ereport(ERROR, (errcode(ERRCODE_ADMIN_SHUTDOWN), errmsg("terminating datareceiver due to timeout")));
            }
        }
    }
    DataRcvSendReply(requestReply, requestReply);
}

/* Main entry point for datareceiver process */
void DataReceiverMain(void)
{
    char conninfo[MAXCONNINFO];
    TimestampTz last_recv_timestamp;
    bool ping_sent = false;
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
    errno_t rc = 0;

    t_thrd.xlog_cxt.InRecovery = true;

    if (datarcv == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("datarcv should not be null")));
    }

    ereport(LOG, (errmsg("datareceiver thread started")));
    /*
     * Mark datareceiver as running in shared memory.
     *
     * Do this as early as possible, so that if we fail later on, we'll set
     * state to STOPPED. If we die before this, the startup process will keep
     * waiting for us to start up, until it times out.
     */
    SpinLockAcquire(&datarcv->mutex);
    if (datarcv->pid != 0) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("datarcv already in use")));
    }
    switch (datarcv->dataRcvState) {
        case DATARCV_STOPPING:
            /* If we've already been requested to stop, don't start up. */
            datarcv->dataRcvState = DATARCV_STOPPED;
            /* fall through */
        case DATARCV_STOPPED:
            SpinLockRelease(&datarcv->mutex);
            ereport(WARNING, (errmsg("datareceiver requested to stop when starting up.")));
            proc_exit(1);
            break;

        case DATARCV_STARTING:
            /* The usual case */
            break;

        case DATARCV_RUNNING:
            /* Shouldn't happen */
            ereport(PANIC, (errmsg("datareceiver still running according to shared memory state")));
    }
    /* Advertise our PID so that the startup process can kill us */
    datarcv->pid = t_thrd.proc_cxt.MyProcPid;
#ifndef WIN32
    datarcv->lwpId = syscall(SYS_gettid);
#else
    datarcv->lwpId = (int)t_thrd.proc_cxt.MyProcPid;
#endif
    datarcv->isRuning = false;
    datarcv->dataRcvState = DATARCV_RUNNING;

    /* Fetch information required to start streaming */
    rc = strncpy_s(conninfo, MAXCONNINFO, (char *)datarcv->conninfo, MAXCONNINFO - 1);
    securec_check(rc, "", "");

    /* Initialise to a sanish value */
    datarcv->lastMsgSendTime = datarcv->lastMsgReceiptTime = GetCurrentTimestamp();
    t_thrd.datareceiver_cxt.AmDataReceiverForDummyStandby = (datarcv->conn_target == REPCONNTARGET_DUMMYSTANDBY)
                                                                ? true
                                                                : false;
    SpinLockRelease(&datarcv->mutex);

    /* Arrange to clean up at datareceiver exit */
    on_shmem_exit(DataRcvDie, 0);

    /* Reset some signals that are accepted by postmaster but not here */
    (void)gspqsignal(SIGHUP, DataRcvSigHupHandler); /* set flag to read config file */
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, DataRcvShutdownHandler); /* request shutdown */
    (void)gspqsignal(SIGQUIT, DataRcvQuickDieHandler); /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
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
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "Data Receiver",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    /* Unblock signals (they were blocked when the postmaster forked us) */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /* Establish the connection to the primary for the replication data streaming */
    EnableDataRcvImmediateExit();
    DataRcvStreamConnect(conninfo);
    DisableDataRcvImmediateExit();

    ereport(LOG, (errmsg("DataReceiverMain streaming begin")));

    if (GetDataRcvDummyStandbySyncPercent() == SYNC_DUMMY_STANDBY_END) {
        Assert(t_thrd.datareceiver_cxt.AmDataReceiverForDummyStandby == true);
        /* thread exit */
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("terminating datareceiver due to Secondary Standby has no data")));
    }

    rc = memset_s(t_thrd.datareceiver_cxt.reply_message, sizeof(StandbyDataReplyMessage), 0,
                  sizeof(StandbyDataReplyMessage));
    securec_check(rc, "", "");

    last_recv_timestamp = GetCurrentTimestamp();
    ping_sent = false;

    SpinLockAcquire(&datarcv->mutex);
    datarcv->isRuning = true;
    datarcv->sendPosition.queueid = datarcv->receivePosition.queueid = datarcv->localWritePosition.queueid = 0;
    datarcv->sendPosition.queueoff = datarcv->receivePosition.queueoff = datarcv->localWritePosition.queueoff = 0;
    SpinLockRelease(&datarcv->mutex);
    pgstat_report_appname("Data Receiver");
    pgstat_report_activity(STATE_IDLE, NULL);

    /* Loop until end-of-streaming or error */
    for (;;) {
        unsigned char type;
        char *buf = NULL;
        int len = 0;

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if (!PostmasterIsAlive())
            gs_thread_exit(1);

        /*
         * Exit datareceiver if we're not in recovery. This should not happen,
         * but cross-check the status here.
         */
        if (!RecoveryInProgress())
            ereport(FATAL, (errmsg("cannot continue DATA streaming, recovery has already ended")));

        /* Process any requests or signals received recently */
        ProcessDataRcvInterrupts();

        if (t_thrd.datareceiver_cxt.got_SIGHUP) {
            t_thrd.datareceiver_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (!DataRcvWriterInProgress())
            ereport(FATAL, (errmsg("terminating datareceiver process due to the death of datarcvwriter")));

        /* Wait a while for data to arrive */
        if (DataRcvStreamReceive(NAPTIME_PER_CYCLE, &type, &buf, &len)) {
            last_recv_timestamp = GetCurrentTimestamp();
            ping_sent = false;
            /* Accept the received data, and process it */
            DataRcvProcessMsg(type, buf, len);

            /* Receive any more data we can without sleeping */
            while (DataRcvStreamReceive(0, &type, &buf, &len)) {
                last_recv_timestamp = GetCurrentTimestamp();
                ping_sent = false;
                DataRcvProcessMsg(type, buf, len);
            }

            /* Let the master know that we received some data. */
            DataRcvSendReply(false, false);
        } else {
            DataReceiverPing(&ping_sent, &last_recv_timestamp);
        }
    }
}

/*
 * Mark us as STOPPED in shared memory at exit.
 */
static void DataRcvDie(int code, Datum arg)
{
    /* Use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;

    /*
     * Shutdown DataRcvWriter thread, clear the data receive buffer.
     * Ensure that all pages received are flushed to disk.
     */
    ShutDownDataRcvWriter();

    /*
     * Clean up the data writer queue, then reset the writer queue. In most
     * scenarios, we should not force to reset it especially when there are remained
     * data in data queue which were left by datarcv writer when database directory
     * was not ready.
     * We use the catchup process in primary to avoid data loss in forcing reset.
     * When the data receiver starts up again, the primary will catchup the data which
     * wasn't received by standby include the data received but not feeded back.
     * If the current data receiver was connected to secondary during failover, the data
     * forced reset would be cleaned up in secondary until caughtup between primary
     * and standby.
     */
    DataRcvDataCleanup();
    ResetDataQueue(t_thrd.dataqueue_cxt.DataWriterQueue);

    SpinLockAcquire(&datarcv->mutex);
    Assert(datarcv->dataRcvState == DATARCV_RUNNING || datarcv->dataRcvState == DATARCV_STOPPING);
    Assert(datarcv->pid == t_thrd.proc_cxt.MyProcPid);
    datarcv->dataRcvState = DATARCV_STOPPED;
    datarcv->pid = 0;
    datarcv->isRuning = false;
    datarcv->receivePosition.queueid = 0;
    datarcv->receivePosition.queueoff = 0;
    SpinLockRelease(&datarcv->mutex);

    /* Terminate the connection gracefully. */
    DataRcvStreamDisconnect();

    if (t_thrd.datareceiver_cxt.recvBuf != NULL) {
        PQfreemem(t_thrd.datareceiver_cxt.recvBuf);
        t_thrd.datareceiver_cxt.recvBuf = NULL;
    }

    ereport(LOG, (errmsg("datareceiver thread shut down")));
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void DataRcvSigHupHandler(SIGNAL_ARGS)
{
    t_thrd.datareceiver_cxt.got_SIGHUP = true;
}

/* SIGTERM: set flag for main loop, or shutdown immediately if safe */
static void DataRcvShutdownHandler(SIGNAL_ARGS)
{
    t_thrd.datareceiver_cxt.got_SIGTERM = true;

    /* cancel the wait for database directory */
    t_thrd.int_cxt.ProcDiePending = true;
}

/*
 * DataRcvQuickDieHandler() occurs when signalled SIGQUIT by the postmaster.
 *
 * Some backend has bought the farm, so we need to stop what we're doing and
 * exit.
 */
static void DataRcvQuickDieHandler(SIGNAL_ARGS)
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

void ProcessDataRcvInterrupts(void)
{
    /*
     * Although datareceiver interrupt handling doesn't use the same scheme as
     * regular backends, call CHECK_FOR_INTERRUPTS() to make sure we receive
     * any incoming signals on Win32.
     */
    CHECK_FOR_INTERRUPTS();

    if (t_thrd.datareceiver_cxt.got_SIGTERM) {
        t_thrd.datareceiver_cxt.DataRcvImmediateInterruptOK = false;
        ereport(FATAL, (errcode(ERRCODE_ADMIN_SHUTDOWN),
                        errmsg("terminating datareceiver process due to administrator command")));
    }
}

static void EnableDataRcvImmediateExit(void)
{
    t_thrd.datareceiver_cxt.DataRcvImmediateInterruptOK = true;
    ProcessDataRcvInterrupts();
}

static void DisableDataRcvImmediateExit(void)
{
    t_thrd.datareceiver_cxt.DataRcvImmediateInterruptOK = false;
    ProcessDataRcvInterrupts();
}

/* Report shared memory space needed by DataRcvShmemInit */
Size DataRcvShmemSize(void)
{
    Size size = 0;

    size = add_size(size, sizeof(DataRcvData));

    return size;
}

/* Allocate and initialize datareceiver-related shared memory */
void DataRcvShmemInit(void)
{
    bool found = false;
    errno_t rc = 0;

    t_thrd.datareceiver_cxt.DataRcv = (DataRcvData *)ShmemInitStruct("Data Receiver Ctl", DataRcvShmemSize(), &found);

    if (!found) {
        /* First time through, so initialize */
        rc = memset_s(t_thrd.datareceiver_cxt.DataRcv, DataRcvShmemSize(), 0, DataRcvShmemSize());
        securec_check(rc, "", "");
        SpinLockInit(&t_thrd.datareceiver_cxt.DataRcv->mutex);
    }
}

int GetDataRcvDummyStandbySyncPercent(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
    int percent = 0;

    SpinLockAcquire(&datarcv->mutex);
    percent = datarcv->dummyStandbySyncPercent;
    SpinLockRelease(&datarcv->mutex);

    return percent;
}

void SetDataRcvDummyStandbySyncPercent(int percent)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;

    SpinLockAcquire(&datarcv->mutex);
    datarcv->dummyStandbySyncPercent = percent;
    SpinLockRelease(&datarcv->mutex);
}

void WakeupDataRcvWriter(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
    SpinLockAcquire(&datarcv->mutex);
    if (datarcv->datarcvWriterLatch != NULL)
        SetLatch(datarcv->datarcvWriterLatch);
    SpinLockRelease(&datarcv->mutex);

    ProcessDataRcvInterrupts();
}

/* Is datareceiver in progress (or starting up)? */
bool DataRcvInProgress(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
    DataRcvState state;
    pg_time_t startTime;

    SpinLockAcquire(&datarcv->mutex);

    state = datarcv->dataRcvState;
    startTime = datarcv->startTime;

    SpinLockRelease(&datarcv->mutex);

    /*
     * If it has taken too long for walreceiver to start up, give up. Setting
     * the state to STOPPED ensures that if walreceiver later does start up
     * after all, it will see that it's not supposed to be running and die
     * without doing anything.
     */
    if (state == DATARCV_STARTING) {
        pg_time_t now = (pg_time_t)time(NULL);
        if ((now - startTime) > DATARCV_STARTUP_TIMEOUT) {
            SpinLockAcquire(&datarcv->mutex);

            if (datarcv->dataRcvState == DATARCV_STARTING)
                state = datarcv->dataRcvState = DATARCV_STOPPED;

            SpinLockRelease(&datarcv->mutex);
            ereport(WARNING, (errmsg("shut down datareceiver due to start up timeout.")));
        }
    }

    if (state != DATARCV_STOPPED)
        return true;
    else
        return false;
}

/*
 * Stop datareceiver (if running) and wait for it to die.
 * Executed by the Startup process.
 */
void ShutdownDataRcv(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
    ThreadId datarcvpid = 0;
    int i = 1;

    /*
     * Request datareceiver to stop. Walreceiver will switch to WALRCV_STOPPED
     * mode once it's finished, and will also request postmaster to not
     * restart itself.
     */
    SpinLockAcquire(&datarcv->mutex);
    switch (datarcv->dataRcvState) {
        case DATARCV_STOPPED:
            break;
        case DATARCV_STARTING:
            datarcv->dataRcvState = DATARCV_STOPPED;
            break;
        case DATARCV_RUNNING:
            datarcv->dataRcvState = DATARCV_STOPPING;
            /* fall through */
        case DATARCV_STOPPING:
            datarcvpid = datarcv->pid;
            break;
    }
    SpinLockRelease(&datarcv->mutex);

    ereport(LOG, (errmsg("startup shut down datareceiver.")));
    /*
     * Signal datareceiver process if it was still running.
     */
    if (datarcvpid != 0)
        (void)gs_signal_send(datarcvpid, SIGTERM);

    /*
     * Wait for datareceiver to acknowledge its death by setting state to
     * DATARCV_STOPPED.
     */
    while (DataRcvInProgress()) {
        /*
         * This possibly-long loop needs to handle interrupts of startup
         * process.
         */
        RedoInterruptCallBack();
        pg_usleep(100000); /* 100ms */

        SpinLockAcquire(&datarcv->mutex);
        datarcvpid = datarcv->pid;
        SpinLockRelease(&datarcv->mutex);

        if ((datarcvpid != 0) && (i % 2000 == 0)) {
            (void)gs_signal_send(datarcvpid, SIGTERM);
            i = 1;
        }
        i++;
    }
}

/*
 * Request postmaster to start datareceiver.
 */
void RequestDataStreaming(const char *conninfo, ReplConnTarget conn_target)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
    pg_time_t now = (pg_time_t)time(NULL);
    errno_t retcode = EOK;

    /* (Single node && multi standby mode) doesn't need datareceiver */
    if (IS_SINGLE_NODE && !IS_DN_DUMMY_STANDYS_MODE())
        return;

    SpinLockAcquire(&datarcv->mutex);

    /* It better be stopped before we try to restart it */
    Assert(datarcv->dataRcvState == DATARCV_STOPPED);

    if (conninfo != NULL) {
        retcode = strncpy_s((char *)datarcv->conninfo, MAXCONNINFO, conninfo, MAXCONNINFO - 1);
        securec_check(retcode, "", "");
    } else {
        SpinLockRelease(&datarcv->mutex);
        SetDataRcvConninfo(conn_target);
        SpinLockAcquire(&datarcv->mutex);
    }

    datarcv->dataRcvState = DATARCV_STARTING;
    datarcv->startTime = now;

    SpinLockRelease(&datarcv->mutex);

    SendPostmasterSignal(PMSIGNAL_START_DATARECEIVER);
}

static void SetDataRcvConninfo(ReplConnTarget conn_target)
{
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
    ReplConnInfo *replConnArray = NULL;

    /* 1. if we are in primary, standby, dummystandby mode. */
    if ((t_thrd.postmaster_cxt.ReplConnArray[1] == NULL && conn_target == REPCONNTARGET_PRIMARY) ||
        (t_thrd.postmaster_cxt.ReplConnArray[2] == NULL && conn_target == REPCONNTARGET_DUMMYSTANDBY)) {
        ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR), errmsg("no replication connection config information."),
                        errhint("please check your configuration in postgresql.conf.")));
    }

    /* 2. get conninfo array */
    if (IS_DN_DUMMY_STANDYS_MODE()) {
        if (conn_target == REPCONNTARGET_DUMMYSTANDBY) {
            ereport(LOG, (errmsg("data receiver get replconninfo[2] to connect dummystandby.")));
            t_thrd.datareceiver_cxt.DataReplFlag = 2;
        }

        if (conn_target == REPCONNTARGET_PRIMARY) {
            ereport(LOG, (errmsg("data receiver get replconninfo[1] to connect primary.")));
            t_thrd.datareceiver_cxt.DataReplFlag = 1;
        }
    }

    replConnArray = GetRepConnArray(&t_thrd.datareceiver_cxt.DataReplFlag);
    if (replConnArray != NULL) {
        int rc = EOK;

        SpinLockAcquire(&datarcv->mutex);
        rc = snprintf_s((char *)datarcv->conninfo, MAXCONNINFO, MAXCONNINFO - 1,
                        "host=%s port=%d localhost=%s localport=%d", replConnArray->remotehost,
                        replConnArray->remoteport, replConnArray->localhost, replConnArray->localport);

        securec_check_ss(rc, "", "");
        datarcv->conninfo[MAXCONNINFO - 1] = '\0';
        datarcv->conn_target = conn_target;
        SpinLockRelease(&datarcv->mutex);

        ereport(DEBUG5, (errmsg("get datarcv conninfo%d=%s", t_thrd.datareceiver_cxt.DataReplFlag, datarcv->conninfo)));
        t_thrd.datareceiver_cxt.DataReplFlag++;
    }
}

/*
 * Send a message to DATA stream.
 * ereports on error.
 */
static void DataRcvStreamSendFiles(const char *buffer, int nbytes)
{
    if (PQputCopyData(t_thrd.datareceiver_cxt.dataStreamingConn, buffer, nbytes) <= 0 ||
        PQflush(t_thrd.datareceiver_cxt.dataStreamingConn))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_STATUS), errmsg("could not send data to DATA stream: %s",
                                                         PQerrorMessage(t_thrd.datareceiver_cxt.dataStreamingConn))));
}

/*
 * Get dummy file numbers
 * including the total file numbers. and start serial number
 * writer_file_num : total file number
 * read_file_num   : start serial number
 */
static void InitDummyFileNum(uint32 *readFileNum, uint32 *writerFileNum)
{
    /* Get the read_file_num and  writer_file_num stored in dummy */
    DIR *dir = NULL;
    struct dirent *de = NULL;
    int maxNumFile = 0;
    int minNumFile = 0;
    char *dirPath = DUMMY_STANDBY_DATADIR;

    /* open the dir of base/dummy_standby */
    errno = 0;
    dir = AllocateDir(dirPath);
    if (dir == NULL && errno == ENOENT) {
        if (mkdir(dirPath, S_IRWXU) < 0) {
            /* Failure other than not exists */
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", dirPath)));
        }
        dir = AllocateDir(dirPath);
    }
    if (dir == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", dirPath)));
        return;
    }

    /* loop for reading the file name of base/dummy_standby */
    while ((de = ReadDir(dir, dirPath)) != NULL) {
        /* Skip special stuff */
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;
        minNumFile = atoi(de->d_name);
        break;
    }

    FreeDir(dir);
    dir = AllocateDir(dirPath);

    /* loop aims to read the file name of base/dummy_standby and get the nums */
    while ((de = ReadDir(dir, dirPath)) != NULL) {
        /* Skip special stuff */
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        /* search the max num file */
        maxNumFile = Max(atoi(de->d_name), maxNumFile);
        minNumFile = Min(atoi(de->d_name), minNumFile);
        ereport(DEBUG5, (errmsg("InitDummyDataNum de->d_name=%s;   max_num_path=%d; min_num_file=%d.", de->d_name,
                                maxNumFile, minNumFile)));
    }

    *writerFileNum = maxNumFile;
    *readFileNum = minNumFile;
    FreeDir(dir);
    ereport(LOG, (errmsg("read files starting serial number:%u", *readFileNum)));
    ereport(LOG, (errmsg("read files ending serial number:%u", *writerFileNum)));
}

static void ParseDummyFile()
{
    /* Hash indicates */
    HTAB *relFileNodeTab = NULL;
    errno_t rc;
    /* Total size we need to palloc the buffer */
    int bufferSize = 0;
    char *replyBcmFileListInfo = NULL;
    /* Page info init */
    uint32 readFileNum = 1;
    uint32 writerFileNum = 0;

    Assert(dummyStandbyMode);

    HASHCTL ctl;
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "", "");
    ctl.keysize = sizeof(RelFileNodeKey);
    ctl.entrysize = sizeof(RelFileNodeKeyEntry);
    ctl.hash = tag_hash;
    relFileNodeTab = hash_create("relfilenode table", 1024, &ctl, HASH_ELEM | HASH_FUNCTION);

    /* Init dummy file data */
    InitDummyFileNum(&readFileNum, &writerFileNum);
    /* Parse each dummy file and get the bcm file list info, then put them into hashtable */
    while (readFileNum <= writerFileNum) {
        ReadDummyFile(relFileNodeTab, readFileNum, &bufferSize);
        /* add heartbeat after every file is scanned */
        DataRcvSendReply(false, false);
        readFileNum++;
    }

    /* Send all data at once */
    if (bufferSize == INT_MAX) {
        ereport(ERROR, (errmsg("bufferSize reaches the limit of INT_MAX")));
    }
    replyBcmFileListInfo = (char *)palloc(bufferSize + 1);
    replyBcmFileListInfo[0] = 'x';
    char *tempPtr = replyBcmFileListInfo + 1;

    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, relFileNodeTab);
    RelFileNodeKeyEntry *entry = NULL;

    while ((entry = (RelFileNodeKeyEntry *)hash_seq_search(&hash_seq)) != NULL) {
        rc = memcpy_s(tempPtr, sizeof(RelFileNodeKey), &(entry->key), sizeof(RelFileNodeKey));
        securec_check(rc, "", "");
        tempPtr += sizeof(RelFileNodeKey);
    }
    ereport(LOG, (errmsg("total bcm file list info buffer size we send is : %d", bufferSize)));
    DataRcvStreamSendFiles(replyBcmFileListInfo, bufferSize + 1);

    tempPtr = NULL;
    hash_destroy(relFileNodeTab);
    pfree(replyBcmFileListInfo);
    ereport(LOG, (errmsg("read files serial number now is : %u", readFileNum)));
}

static void ReadDummyFile(HTAB *relFileNodeTab, uint32 readFileNum, int *bufferSize)
{
    uint32 nbytes = 0;
    int nRet = 0;
    char path[MAXPGPATH] = {0};
    FILE *readFileFd = NULL;
    TimestampTz lastDummyPingTime;
    /* record ping time when we are searching */
    lastDummyPingTime = GetCurrentTimestamp();
    /* get the file path */
    nRet = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "base/dummy_standby/%u", readFileNum);
    securec_check_ss(nRet, "", "");

    ereport(DEBUG5, (errmsg("DataSendReadData path=%s", path)));

    /* open the file */
    readFileFd = fopen(path, "rb");
    if (readFileFd == NULL) {
        ereport(WARNING, (errcode_for_file_access(), errmsg("could not open data file \"%s\": %m", path)));
        return;
    }

    while (true) {
        /*
         * OK to read the data:
         * 1. first, read nbytes of the FD;
         */
        if (!ReadDummySlice(readFileFd, (char *)&nbytes, (uint32)sizeof(nbytes), path)) {
            /*
             * if we receive the eof when reading nbytes, the file maybe
             * interrupted when writting. So we break and return to read the next file.
             */
            ereport(LOG, (errmsg("step1: data file num %u, read file fd %d", readFileNum, readFileFd->_fileno)));
            break;
        }
        char *elementBuf = NULL;
        elementBuf = (char *)palloc(nbytes);
        /*
         * 2. then, read the data which according to nbytes;
         */
        if (!ReadDummySlice(readFileFd, elementBuf, nbytes, path)) {
            ereport(LOG, (errmsg("step2: data file num %u, read file fd %d", readFileNum, readFileFd->_fileno)));
            pfree(elementBuf);
            break;
        }
        *bufferSize += ParseDataHeader(relFileNodeTab, elementBuf, nbytes);
        TimestampTz now;
        now = GetCurrentTimestamp();
        if (TimestampDifferenceExceeds(lastDummyPingTime, now, 1000)) {
            /* add heartbeat per 1 second */
            DataRcvSendReply(false, false);
            lastDummyPingTime = now;
        }
        pfree(elementBuf);
    }
    fclose(readFileFd);
    return;
}

static bool ReadDummySlice(FILE *readFileFd, char *buf, uint32 length, char *path)
{
    uint32 readBytes = 0;
    readBytes = fread(buf, 1, length, readFileFd);
    if (readBytes != length) {
        if (ferror(readFileFd)) {
            fclose(readFileFd);
            readFileFd = NULL;
            ereport(FATAL, (errcode_for_file_access(), errmsg("could not read to data file slice %s "
                                                              "length %u: %m",
                                                              path, length)));
        }
        if (feof(readFileFd)) {
            return false;
        }
    }
    return true;
}

static int ParseDataHeader(HTAB *relFileNodeTab, char *bufPtr, uint32 nbytes)
{
    uint32 currentLen = 0;
    int bufferSize = 0;
    size_t headerLen = sizeof(DataElementHeaderData);
    DataElementHeaderData dataInfo;
    RelFileNodeKey *elementKey = NULL;
    RelFileNodeKeyEntry *rel = NULL;
    bool found = false;
    errno_t errorno = EOK;
    while (nbytes > 0) {
        /* parse total_len for this slice of data */
        if ((size_t)nbytes < sizeof(uint32) + headerLen) {
            ereport(ERROR, (errmsg("ParseDataHeader failed due to insufficient buffer.")));
        }
        errorno = memcpy_s(&currentLen, sizeof(uint32), bufPtr, sizeof(uint32));
        securec_check(errorno, "", "");
        bufPtr += sizeof(uint32);
        /* parse data element header, and skip the payload then parse next one */
        errorno = memcpy_s((void *)&dataInfo, headerLen, bufPtr, headerLen);
        securec_check(errorno, "", "");
        bufPtr += headerLen;

        elementKey = (RelFileNodeKey *)palloc(sizeof(RelFileNodeKey));
        /* Fill the key during this read */
        RelFileNodeCopy(elementKey->relfilenode, dataInfo.rnode, GETBUCKETID(dataInfo.attid));
        elementKey->columnid = (int)GETATTID((uint)dataInfo.attid);

        if (u_sess->attr.attr_storage.HaModuleDebug) {
            ereport(LOG, (errmsg("Parse each relfilenode or CU info: dbNode %u, spcNode %u, relNode %u, columnid %d  ",
                                 elementKey->relfilenode.dbNode, elementKey->relfilenode.spcNode,
                                 elementKey->relfilenode.relNode, elementKey->columnid)));
        }
        /* Hash table used for removing duplicated files */
        rel = (RelFileNodeKeyEntry *)hash_search(relFileNodeTab, elementKey, HASH_ENTER, &found);

        if (!found) {
            rel->number = 1;
            bufferSize += sizeof(RelFileNodeKey);
            pfree(elementKey);
        } else {
            found = false;
            pfree(elementKey);
        }
        if ((size_t)currentLen < sizeof(uint32) + headerLen || currentLen > nbytes) {
            ereport(ERROR, (errmsg("ParseDataHeader failed due to illegal currentLen %u.", currentLen)));
        }
        nbytes -= currentLen;
        bufPtr += dataInfo.data_size;
    }
    return bufferSize;
}

/*
 * Accept the message from replication stream, and process it.
 */
static void DataRcvProcessMsg(unsigned char type, char *buf, Size len)
{
    errno_t errorno = EOK;
    switch (type) {
        case 'b': { /* search files for bcm */
            if (dummyStandbyMode) {
                ereport(LOG, (errmsg("Received incremental searching bcm message")));
                ParseDummyFile();
                break;
            }
        }
        /* fall through */
        case 'd': /* Data page */
        {
            DataPageMessageHeader msghdr;

            if (len < sizeof(DataPageMessageHeader))
                ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                errmsg_internal("invalid data page message received from primary")));
            /* memcpy is required here for alignment reasons */
            errorno = memcpy_s(&msghdr, sizeof(DataPageMessageHeader), buf, sizeof(DataPageMessageHeader));
            securec_check(errorno, "", "");
            ProcessDataHeaderMessage(&msghdr);

            buf += sizeof(DataPageMessageHeader);
            len -= sizeof(DataPageMessageHeader);

            volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;

            if (datarcv->conn_target != REPCONNTARGET_DUMMYSTANDBY) {
                DQByteAdvance(msghdr.dataStart, len);
                if (!DQByteEQ(msghdr.dataStart, msghdr.dataEnd))
                    ereport(PANIC, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                    errmsg("invalid message header, maybe the parameter of"
                                           " \"data_replicate_buffer_size\" on the master has been changed"),
                                    errhint("You might need to restart the instance.")));
            } else if (unlikely(!DataQueuePtrIsInvalid(msghdr.dataEnd))) {
                ereport(PANIC, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid message end")));
            }
            DataRcvReceive(buf, len);
            break;
        }

        case 'k': /* Keepalive */
        {
            DataSndKeepaliveMessage keepalive;

            if (len != sizeof(DataSndKeepaliveMessage))
                ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                errmsg_internal("invalid keepalive message received from primary")));
            /* memcpy is required here for alignment reasons */
            errorno = memcpy_s(&keepalive, sizeof(DataSndKeepaliveMessage), buf, sizeof(DataSndKeepaliveMessage));
            securec_check(errorno, "", "");
            ProcessKeepaliveMessage(&keepalive);

            /* If the primary requested a reply, send one immediately */
            if (keepalive.replyRequested)
                DataRcvSendReply(true, false);
            break;
        }
        case 'e': /* end data */
        {
            EndDataMessage endDataMessage;

            if (len != sizeof(EndDataMessage))
                ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                errmsg_internal("invalid EndDataMessage message received from Secondary Standby")));
            /* memcpy is required here for alignment reasons */
            errorno = memcpy_s(&endDataMessage, sizeof(EndDataMessage), buf, sizeof(EndDataMessage));
            securec_check(errorno, "", "");
            ProcessEndDataMessage(&endDataMessage);
            break;
        }
        case 'x': /* rm data */
        {
            RmDataMessage rmDataMessage;

            if (len != sizeof(RmDataMessage))
                ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                                errmsg_internal("invalid RmData message received from primary")));
            /* memcpy is required here for alignment reasons */
            errorno = memcpy_s(&rmDataMessage, sizeof(RmDataMessage), buf, sizeof(RmDataMessage));
            securec_check(errorno, "", "");
            ProcessRmDataMessage(&rmDataMessage);
            break;
        }
        default:
            ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                            errmsg_internal("invalid data replication message type %d", type)));
    }
}

/*
 * Process dataHeaderMessage received from sender
 * message type is 'd'.
 */
static void ProcessDataHeaderMessage(DataPageMessageHeader *msghdr)
{
    /* Use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
    TimestampTz lastMsgReceiptTime = GetCurrentTimestamp();

    /* Update shared-memory status */
    SpinLockAcquire(&datarcv->mutex);
    datarcv->sendPosition.queueid = msghdr->dataEnd.queueid;
    datarcv->sendPosition.queueoff = msghdr->dataEnd.queueoff;
    datarcv->lastMsgSendTime = msghdr->sendTime;
    datarcv->lastMsgReceiptTime = lastMsgReceiptTime;
    SpinLockRelease(&datarcv->mutex);

    /* Update the catchup flag */
    data_catchup = msghdr->catchup;

    if (log_min_messages <= DEBUG2) {
        MakeDebugLog(msghdr->sendTime, lastMsgReceiptTime, "data receive data header data sendtime %s receipttime %s");
    }
}

/*
 * Process ProcessKeepaliveMessage received from datasender,
 * message type is 'k'.
 */
static void ProcessKeepaliveMessage(DataSndKeepaliveMessage *keepalive)
{
    /* Use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
    TimestampTz lastMsgReceiptTime = GetCurrentTimestamp();

    /* Update shared-memory status */
    SpinLockAcquire(&datarcv->mutex);
    datarcv->sendPosition.queueid = keepalive->sendPosition.queueid;
    datarcv->sendPosition.queueoff = keepalive->sendPosition.queueoff;
    datarcv->lastMsgSendTime = keepalive->sendTime;
    datarcv->lastMsgReceiptTime = lastMsgReceiptTime;
    SpinLockRelease(&datarcv->mutex);

    data_catchup = keepalive->catchup;

    if (log_min_messages <= DEBUG2) {
        MakeDebugLog(keepalive->sendTime, lastMsgReceiptTime, "data receive keepalive data sendtime %s receipttime %s");
    }
}

/*
 * Process RmDataMessage received from (primary? and dummystandby?) sender,
 * message type is 'e'. Refence searchBCMFiles
 */
static void ProcessEndDataMessage(EndDataMessage *endDataMessage)
{
    ereport(dummyStandbyMode ? DEBUG2 : LOG, (errmsg("sync Secondary Standby data done")));

    if (endDataMessage->percent == SYNC_DUMMY_STANDBY_END) {
        /*
         * We have received all the data in secondary and pushed them to the
         * data writer queue. If the data receiver writer thread is online, shut
         * down it even if there are remained data whose database directory is
         * not ready.
         * If the data receiver writer is offline, the data remained in data writer
         * queue would be handled during datarcvdie.
         */
        ShutDownDataRcvWriter();

        /*
         * Maybe, some xlog records were not transferred to secondary when standby
         * is offline but the corresponding data have transferred to secondary already
         * especially tablespace data. After we received the end message from
         * secondary, the wal receiver is probably still working and startup is still
         * replaying.
         *
         * We set the sync percent to end directly here, the startup thread would shut
         * down the data receiver and wal receiver after received all the xlog records.
         * And during the shutting down, we do a last cleanup to write the remained
         * data in data writer queue.
         */
        SetDataRcvDummyStandbySyncPercent(SYNC_DUMMY_STANDBY_END);
    }
}

static void ProcessRmData(void)
{
    DIR *dir = NULL;
    struct dirent *de = NULL;
    char data_path[MAXPGPATH] = {0};
    int nRet = 0;

    nRet = snprintf_s(data_path, sizeof(data_path), MAXPGPATH - 1, "%s/%s", t_thrd.proc_cxt.DataDir,
                      DUMMY_STANDBY_DATADIR);
    securec_check_ss(nRet, "", "");

    dir = AllocateDir(data_path);
    while ((de = ReadDir(dir, data_path)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        char path[MAXPGPATH] = {0};
        nRet = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "%s/%s", data_path, de->d_name);
        securec_check_ss(nRet, "", "");

        unlink(path);
    }
    FreeDir(dir);
}

/*
 * Process RmDataMessage received from primary sender, message type is 'x'.
 * Refence searchBCMFiles
 */
static void ProcessRmDataMessage(RmDataMessage *rmDataMessage)
{
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;

    /* check command source */
    if (rmDataMessage->peer_role != PRIMARY_MODE) {
        ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND),
                        errmsg("rm data comand is not from primary, peer_role=%d", rmDataMessage->peer_role)));
    }

    ereport(DEBUG2, (errmsg("received rm data message")));

    SpinLockAcquire(&datarcv->mutex);
    datarcv->sendPosition.queueid = 0;
    datarcv->sendPosition.queueoff = 0;
    datarcv->receivePosition.queueid = 0;
    datarcv->receivePosition.queueoff = 0;
    SpinLockRelease(&datarcv->mutex);

    while (true) {
        if (!dummy_data_writer_use_file) {
            CloseDataFile();
            break;
        } else {
            pg_usleep(100000); /* sleep 0.1 s */
        }
    }

    ProcessRmData();
}

/*
 * Receive data from remote server, then push to writer queue.
 */
static void DataRcvReceive(char *buf, Size nbytes)
{
    BlockNumber cursegno = InvalidBlockNumber;
    DataElementHeaderData dataelemheader;
    /* buf unit */
    uint32 currentlen = 0;
    int headerlen = sizeof(DataElementHeaderData);
    errno_t rc = 0;
#ifdef DATA_DEBUG
    pg_crc32 crc;
#endif

    while (nbytes >= sizeof(uint32) + sizeof(DataElementHeaderData)) {
        rc = memcpy_s((void *)&currentlen, sizeof(uint32), buf, sizeof(uint32));
        securec_check(rc, "", "");
        buf += sizeof(uint32);

        rc = memcpy_s((void *)&dataelemheader, headerlen, buf, headerlen);
        securec_check(rc, "", "");
        buf += headerlen;
        if (u_sess->attr.attr_storage.HaModuleDebug) {
            ereport(LOG,
                (errmsg("DataRcvReceive element info: %u, %u, %u, %d, %u  ",
                    dataelemheader.rnode.dbNode,
                    dataelemheader.rnode.spcNode,
                    dataelemheader.rnode.relNode,
                    GETBUCKETID(dataelemheader.attid),
                    GETATTID((uint)dataelemheader.attid))));
        }

        cursegno = dataelemheader.blocknum / ((BlockNumber)RELSEG_SIZE);

        if (currentlen != (sizeof(uint32) + (uint32)headerlen + (uint32)dataelemheader.data_size)) {
            ereport(ERROR,
                (errmsg("Current length is illegal, the dataRcvReceiveelement info is : %u, %u, %u, %d, %u  ",
                    dataelemheader.rnode.dbNode,
                    dataelemheader.rnode.spcNode,
                    dataelemheader.rnode.relNode,
                    GETBUCKETID(dataelemheader.attid),
                    GETATTID((uint)dataelemheader.attid))));
        }

        if (u_sess->attr.attr_storage.HaModuleDebug) {
            /* now BLCKSZ is equal to ALIGNOF_CUSIZE, so either one is used */
            ereport(LOG,
                (errmsg("HA-DataRcvReceive: rnode %u/%u/%u, blockno %u, segno %u, "
                        "pageoffset2blockno %lu, size %u, queueoffset %u/%u",
                    dataelemheader.rnode.spcNode,
                    dataelemheader.rnode.dbNode,
                    dataelemheader.rnode.relNode,
                    dataelemheader.blocknum,
                    cursegno,
                    dataelemheader.offset / BLCKSZ,
                    dataelemheader.data_size,
                    dataelemheader.queue_offset.queueid,
                    dataelemheader.queue_offset.queueoff)));
        }

        /* Add hearbeat */
        DataRcvSendReply(false, false);

#ifdef DATA_DEBUG
        INIT_CRC32(crc);
        COMP_CRC32(crc, buf, dataelemheader.data_size);
        FIN_CRC32(crc);

        if (!EQ_CRC32(dataelemheader.data_crc, crc)) {
            ereport(PANIC,
                (errmsg("received incorrect data page checksum at: "
                        "rnode[%u,%u,%u], blockno[%u], segno[%u], "
                        "pageoffset[%u], size[%u], queueoffset[%u/%u]",
                    dataelemheader.rnode.spcNode,
                    dataelemheader.rnode.dbNode,
                    dataelemheader.rnode.relNode,
                    dataelemheader.blocknum,
                    cursegno,
                    dataelemheader.offset,
                    dataelemheader.data_size,
                    dataelemheader.queue_offset.queueid,
                    dataelemheader.queue_offset.queueoff)));
        }
#endif

        /* Push the holl data element with head into writer queue. */
        (void)PushToWriterQueue(buf - sizeof(uint32) - headerlen, currentlen);

        buf += dataelemheader.data_size;
        nbytes -= currentlen;

        WakeupDataRcvWriter();
    }
    Assert(nbytes == 0);

    WakeupDataRcvWriter();
}

/*
 * Clean up data in receive queue.
 * This function should be called on thread exit.
 */
void DataRcvDataCleanup(void)
{
    /*
     * Cleanup the data writer queue rather than do a last write because the data
     * may distribute on the both sides of data write queue. In this scenario, an
     * attempt to write the data write queue would leave the left data queue data
     * which may cause data loss especially during standby failover process.
     */
    if (!dummyStandbyMode)
        while (DataRcvWrite() > 0)
            ;
}

/*
 * Send reply message to primary, indicating our current received positions.
 *
 * If 'force' is not true, the message is not sent unless enough time has
 * passed since last status update to reach wal_receiver_status_internal (or
 * if u_sess->attr.attr_storage.wal_receiver_status_interval is disabled altogether).
 *
 * If 'requestReply' is true, requests the server to reply immediately upon receiving
 * this message. This is used for heartbearts, when approaching u_sess->attr.attr_storage.wal_receiver_timeout.
 */
void DataRcvSendReply(bool force, bool requestReply)
{
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
    char buf[sizeof(StandbyDataReplyMessage) + 1] = {0};
    TimestampTz now;
    DataQueuePtr receivePosition;
    errno_t errorno = EOK;

    /*
     * If the user doesn't want status to be reported to the master, be sure
     * to exit before doing anything at all.
     */
    if (!force && u_sess->attr.attr_storage.wal_receiver_status_interval <= 0)
        return;

    /* Get current timestamp. */
    now = GetCurrentTimestamp();

    SpinLockAcquire(&datarcv->mutex);
    receivePosition.queueid = datarcv->receivePosition.queueid;
    receivePosition.queueoff = datarcv->receivePosition.queueoff;
    SpinLockRelease(&datarcv->mutex);

    if (!force && (DQByteEQ(t_thrd.datareceiver_cxt.reply_message->receivePosition, receivePosition)) &&
        !(TimestampDifferenceExceeds(t_thrd.datareceiver_cxt.reply_message->sendTime, now,
                                     u_sess->attr.attr_storage.wal_receiver_status_interval * 1000) ||
          TimestampDifferenceExceeds(now, t_thrd.datareceiver_cxt.reply_message->sendTime,
                                     u_sess->attr.attr_storage.wal_receiver_status_interval * 1000))) {
        return;
    }

    /* Construct a new message */
    t_thrd.datareceiver_cxt.reply_message->receivePosition = receivePosition;
    t_thrd.datareceiver_cxt.reply_message->sendTime = now;
    t_thrd.datareceiver_cxt.reply_message->replyRequested = requestReply;

    ereport(DEBUG2, (errmsg("sending data receive queue position %u/%u",
                            t_thrd.datareceiver_cxt.reply_message->receivePosition.queueid,
                            t_thrd.datareceiver_cxt.reply_message->receivePosition.queueoff)));

    /* Prepend with the message type and send it. */
    buf[0] = 'r';
    errorno = memcpy_s(&buf[1], sizeof(StandbyDataReplyMessage), t_thrd.datareceiver_cxt.reply_message,
                       sizeof(StandbyDataReplyMessage));
    securec_check(errorno, "", "");
    DataRcvStreamSend(buf, sizeof(StandbyDataReplyMessage) + 1);
}

static void DataRcvStreamConnect(char *conninfo)
{
    char conninfo_repl[MAXCONNINFO + 75] = {0};

    char *primary_sysid = NULL;
    char standby_sysid[32];
    TimeLineID primary_tli;
    TimeLineID standby_tli;
    PGresult *res = NULL;
    ServerMode primary_mode;
    int rc = EOK;

    /*
     * Connect the primary server in data replication.
     */
    if (dummyStandbyMode) {
        rc = snprintf_s(conninfo_repl, sizeof(conninfo_repl), sizeof(conninfo_repl) - 1,
                        "%s dbname=replication replication=data "
                        "fallback_application_name=dummystandby "
                        "connect_timeout=%d",
                        conninfo, u_sess->attr.attr_storage.wal_receiver_connect_timeout);
    } else {
        rc = snprintf_s(conninfo_repl, sizeof(conninfo_repl), sizeof(conninfo_repl) - 1,
                        "%s dbname=replication replication=data "
                        "fallback_application_name=%s "
                        "connect_timeout=%d enable_ce=1",
                        conninfo,
                        (u_sess->attr.attr_common.application_name &&
                         strlen(u_sess->attr.attr_common.application_name) > 0)
                            ? u_sess->attr.attr_common.application_name
                            : "datareceiver",
                        u_sess->attr.attr_storage.wal_receiver_connect_timeout);
    }
    securec_check_ss(rc, "", "");

    ereport(LOG, (errmsg("data streaming replication connecting to primary :%s", conninfo_repl)));

    t_thrd.datareceiver_cxt.dataStreamingConn = PQconnectdb(conninfo_repl);
    if (PQstatus(t_thrd.datareceiver_cxt.dataStreamingConn) != CONNECTION_OK)
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_TIMED_OUT),
                        errmsg("data receiver could not connect to the primary server: %s",
                               PQerrorMessage(t_thrd.datareceiver_cxt.dataStreamingConn))));
    ereport(LOG, (errmsg("data streaming replication connected to primary :%s success.", conninfo)));

    if (!dummyStandbyMode) {
        /*  FUTURE CASE:: need some consistence check */
        if (!t_thrd.datareceiver_cxt.AmDataReceiverForDummyStandby) {
            res = PQexec(t_thrd.datareceiver_cxt.dataStreamingConn, "IDENTIFY_MODE");
            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                PQclear(res);
                ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                                errmsg("could not receive the ongoing mode infomation from "
                                       "the primary server: %s",
                                       PQerrorMessage(t_thrd.datareceiver_cxt.dataStreamingConn))));
            }
            if (PQnfields(res) != 1 || PQntuples(res) != 1) {
                int ntuples = PQntuples(res);
                int nfields = PQnfields(res);

                PQclear(res);
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_STATUS), errmsg("invalid response from primary server"),
                         errdetail("Expected 1 tuple with 1 fields, got %d tuples with %d fields.", ntuples, nfields)));
            }
            primary_mode = (ServerMode)pg_strtoint32(PQgetvalue(res, 0, 0));
            if (primary_mode != PRIMARY_MODE) {
                PQclear(res);
                ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                                errmsg("the mode of the remote server must be primary, current is %s",
                                       wal_get_role_string(primary_mode))));
            }
            PQclear(res);
        }

        /*
         * Identify system
         */
        res = PQexec(t_thrd.datareceiver_cxt.dataStreamingConn, "IDENTIFY_SYSTEM");
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            PQclear(res);
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("could not receive database system identifier and timeline ID from "
                                   "the primary server: %s",
                                   PQerrorMessage(t_thrd.datareceiver_cxt.dataStreamingConn))));
        }
        if (PQnfields(res) != 2 || PQntuples(res) != 1) {
            int ntuples = PQntuples(res);
            int nfields = PQnfields(res);

            PQclear(res);
            ereport(
                ERROR,
                (errcode(ERRCODE_INVALID_STATUS), errmsg("invalid response from primary server"),
                 errdetail(
                     "Could not identify system: got %d rows and %d fields, expected %d rows and %d or more fields.",
                     ntuples, nfields, 2, 1)));
        }
        primary_sysid = PQgetvalue(res, 0, 0);
        primary_tli = pg_strtoint32(PQgetvalue(res, 0, 1));

        /*
         * Confirm that the system identifier of the primary is the same as ours.
         */
        rc = snprintf_s(standby_sysid, sizeof(standby_sysid), sizeof(standby_sysid) - 1, UINT64_FORMAT,
                        GetSystemIdentifier());
        securec_check_ss(rc, "", "");

        if (strcmp(primary_sysid, standby_sysid) != 0) {
            primary_sysid = pstrdup(primary_sysid);
            PQclear(res);
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("database system identifier differs between the primary and standby"),
                            errdetail("The primary's identifier is %s, the standby's identifier is %s.", primary_sysid,
                                      standby_sysid)));
        }
        /*
         * Confirm that the current timeline of the primary is the same as the
         * recovery target timeline.
         */
        standby_tli = GetRecoveryTargetTLI();
        PQclear(res);

        if (primary_tli != standby_tli) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("timeline %u of the primary does not match recovery target timeline %u", primary_tli,
                                   standby_tli)));
        }
        t_thrd.xlog_cxt.ThisTimeLineID = primary_tli;
    }
    /*
     * Start data replication.
     */
    res = PQexec(t_thrd.datareceiver_cxt.dataStreamingConn, "START_REPLICATION DATA");
    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        PQclear(res);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_STATUS), errmsg("could not start DATA streaming: %s",
                                                         PQerrorMessage(t_thrd.datareceiver_cxt.dataStreamingConn))));
    }
    PQclear(res);

    ereport(LOG, (errmsg("data streaming replication successfully connected to primary.")));
}

static bool DataRcvStreamReceive(int timeout, unsigned char *type, char **buffer, int *len)
{
    int rawlen;

    if (t_thrd.datareceiver_cxt.recvBuf != NULL) {
        PQfreemem(t_thrd.datareceiver_cxt.recvBuf);
    }
    t_thrd.datareceiver_cxt.recvBuf = NULL;

    /* Try to receive a CopyData message */
    rawlen = PQgetCopyData(t_thrd.datareceiver_cxt.dataStreamingConn, &t_thrd.datareceiver_cxt.recvBuf, 1);
    if (rawlen == 0) {
        /*
         * No data available yet. If the caller requested to block, wait for
         * more data to arrive.
         */
        if (timeout > 0) {
            if (!DataRcvStreamSelect(timeout)) {
                return false;
            }
        }

        if (PQconsumeInput(t_thrd.datareceiver_cxt.dataStreamingConn) == 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("could not receive data from DATA stream: %s",
                                   PQerrorMessage(t_thrd.datareceiver_cxt.dataStreamingConn))));
        }

        /* Now that we've consumed some input, try again */
        rawlen = PQgetCopyData(t_thrd.datareceiver_cxt.dataStreamingConn, &t_thrd.datareceiver_cxt.recvBuf, 1);
        if (rawlen == 0) {
            return false;
        }
    }
    if (rawlen == -1) { /* end-of-streaming or error */
        PGresult *res = NULL;

        res = PQgetResult(t_thrd.datareceiver_cxt.dataStreamingConn);
        if (PQresultStatus(res) == PGRES_COMMAND_OK || PQresultStatus(res) == PGRES_COPY_IN) {
            PQclear(res);
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("data replication terminated by primary server :%s",
                                   PQerrorMessage(t_thrd.datareceiver_cxt.dataStreamingConn))));
        } else {
            PQclear(res);
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("could not receive data from DATA stream: %s",
                                   PQerrorMessage(t_thrd.datareceiver_cxt.dataStreamingConn))));
        }
    }
    if (rawlen < -1) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_STATUS), errmsg("could not receive data from DATA stream: %s",
                                                         PQerrorMessage(t_thrd.datareceiver_cxt.dataStreamingConn))));
    }

    /* Return received messages to caller */
    *type = *((unsigned char *)t_thrd.datareceiver_cxt.recvBuf);
    *buffer = t_thrd.datareceiver_cxt.recvBuf + sizeof(*type);
    *len = rawlen - sizeof(*type);
    return true;
}

/*
 * Send a message to DATA stream.
 * ereports on error.
 */
static void DataRcvStreamSend(const char *buffer, int nbytes)
{
    if (PQputCopyData(t_thrd.datareceiver_cxt.dataStreamingConn, buffer, nbytes) <= 0 ||
        PQflush(t_thrd.datareceiver_cxt.dataStreamingConn)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_STATUS), errmsg("could not send data to DATA stream: %s",
                                                         PQerrorMessage(t_thrd.datareceiver_cxt.dataStreamingConn))));
    }
}

/*
 * Disconnect connection to primary, if any.
 */
static void DataRcvStreamDisconnect(void)
{
    PQfinish(t_thrd.datareceiver_cxt.dataStreamingConn);
    t_thrd.datareceiver_cxt.dataStreamingConn = NULL;
}

/*
 * Wait until we can read DATA stream, or timeout.
 *
 * Returns true if data has become available for reading, false if timed out
 * or interrupted by signal.
 *
 * This is based on pqSocketCheck.
 */
static bool DataRcvStreamSelect(int timeout_ms)
{
    int ret;

    Assert(t_thrd.datareceiver_cxt.dataStreamingConn != NULL);
    if (PQsocket(t_thrd.datareceiver_cxt.dataStreamingConn) < 0) {
        ereport(ERROR, (errcode_for_socket_access(), errmsg("socket not open")));
    }

    /* We use poll(2) if available, otherwise select(2) */
    {
#ifdef HAVE_POLL
        struct pollfd input_fd;

        input_fd.fd = PQsocket(t_thrd.datareceiver_cxt.dataStreamingConn);
        input_fd.events = POLLIN | POLLERR;
        input_fd.revents = 0;

        ret = poll(&input_fd, 1, timeout_ms);
#else  /* !HAVE_POLL */

        fd_set input_mask;
        struct timeval timeout;
        struct timeval *ptr_timeout = NULL;

        FD_ZERO(&input_mask);
        FD_SET(PQsocket(t_thrd.datareceiver_cxt.dataStreamingConn), &input_mask);

        if (timeout_ms < 0) {
            ptr_timeout = NULL;
        } else {
            timeout.tv_sec = timeout_ms / 1000;
            timeout.tv_usec = (timeout_ms % 1000) * 1000;
            ptr_timeout = &timeout;
        }

        ret = select(PQsocket(t_thrd.datareceiver_cxt.dataStreamingConn) + 1, &input_mask, NULL, NULL, ptr_timeout);
#endif /* HAVE_POLL */
    }

    if (ret == 0 || (ret < 0 && errno == EINTR)) {
        return false;
    }
    if (ret < 0) {
        ereport(ERROR, (errcode_for_socket_access(), errmsg("select() failed: %m")));
    }
    return true;
}

/*
 * Wait data receiver writer shut down.
 */
static void ShutDownDataRcvWriter(void)
{
    /* Use volatile pointer to prevent code rearrangement */
    volatile DataRcvData *datarcv = t_thrd.datareceiver_cxt.DataRcv;
    ThreadId writerPid;
    int i = 1;

    SpinLockAcquire(&datarcv->mutex);
    writerPid = datarcv->writerPid;
    SpinLockRelease(&datarcv->mutex);

    if (writerPid != 0)
        (void)gs_signal_send(writerPid, SIGTERM);

    ereport(LOG, (errmsg("waiting datarcvwriter: %lu terminate", writerPid)));

    while (writerPid) {
        pg_usleep(10000L);  // sleep 0.01s

        SpinLockAcquire(&datarcv->mutex);
        writerPid = datarcv->writerPid;
        SpinLockRelease(&datarcv->mutex);

        if ((writerPid != 0) && (i % 2000 == 0)) {
            if (gs_signal_send(writerPid, SIGTERM) != 0) {
                ereport(WARNING, (errmsg("datarcvwriter:%lu may be terminated", writerPid)));
                break;
            }
            i = 1;
        }
        i++;
    }
}
