/* -------------------------------------------------------------------------
 *
 * walreceiverfuncs.cpp
 *
 * This file contains functions used by the startup process to communicate
 * with the walreceiver process. Functions implementing walreceiver itself
 * are in walreceiver.c.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/replication/walreceiverfuncs.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>

#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "access/xlog_internal.h"
#include "postmaster/startup.h"
#include "postmaster/postmaster.h"
#include "replication/dataqueue.h"
#include "replication/replicainternal.h"
#include "replication/walreceiver.h"
#include "replication/slot.h"
#include "storage/pmsignal.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "gssignal/gs_signal.h"

/*
 * There are several concepts in stream replication connection:
 *
 * Flag means which entryway will be used ReplConnArray1 or ReplConnArray2.
 * Index means that which channel will be used for connection in a given entryway.
 * ReplArrayLength means the numbers of channel in a configed entryway.
 *
 * example:
 * entryway ReplConnArray1 = (channel1, channel2, channel3)
 * entryway ReplConnArray2 = (channel4, channel5, channel6)
 */
extern bool dummyStandbyMode;

static void SetWalRcvConninfo(ReplConnTarget conn_target);
static void SetFailoverFailedState(void);
extern void SetDataRcvDummyStandbySyncPercent(int percent);

/*
 * How long to wait for walreceiver to start up after requesting
 * postmaster to launch it. In seconds.
 */
#define WALRCV_STARTUP_TIMEOUT 3
#define FAILOVER_HOST_FOR_DUMMY "failover_host_for_dummy"

/*
 * reference SetWalRcvConninfo
 */
void connect_dn_str(char *conninfo, int replIndex)
{
    ReplConnInfo *replConnArray = NULL;
    int rc = 0;

    if (t_thrd.postmaster_cxt.ReplConnArray[1] == NULL || t_thrd.postmaster_cxt.ReplConnArray[2] == NULL) {
        ereport(FATAL, (errmsg("replconninfo1 or replconninfo2 not configured."),
                        errhint("please check your configuration in postgresql.conf.")));
    }

    replConnArray = t_thrd.postmaster_cxt.ReplConnArray[replIndex];
    rc = snprintf_s((char *)conninfo, MAXCONNINFO, MAXCONNINFO - 1, "host=%s port=%d localhost=%s localport=%d",
                    replConnArray->remotehost, replConnArray->remoteport, replConnArray->localhost,
                    replConnArray->localport);
    securec_check_ss(rc, "\0", "\0");
}

/*
 * Reset the triggered flag, so CheckForFailoverTriggered() will return false.
 * And also set db_state to promoting failed.
 */
static void SetFailoverFailedState(void)
{
    if (!t_thrd.xlog_cxt.failover_triggered)
        return;

    /*
     * reset sync flag before switching to next channel/replication,
     * in order to avoid data/xlog loss.
     */
    SetWalRcvDummyStandbySyncPercent(0);
    SetDataRcvDummyStandbySyncPercent(0);

    /* reset the dummy standby connection failed flag */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    walrcv->dummyStandbyConnectFailed = false;
    SpinLockRelease(&walrcv->mutex);

    t_thrd.xlog_cxt.failover_triggered = false;

    ereport(LOG, (errmsg("set failover failed state.")));
    return;
}

/*
 * Find next connect channel , and try to connect. According to the ReplFlag,
 * ReplIndex , connect error in the walrcv, find next channel, save it in
 * walrcv. Latter the walreceiver will use it, try to connect the primary.
 */
static void SetWalRcvConninfo(ReplConnTarget conn_target)
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    ReplConnInfo *conninfo = NULL;
    int checknum = MAX_REPLNODE_NUM;

    if (IS_DN_DUMMY_STANDYS_MODE()) {
        if (conn_target == REPCONNTARGET_PRIMARY) {
            ereport(LOG, (errmsg("wal receiver get replconninfo[1] to connect primary.")));
            t_thrd.walreceiverfuncs_cxt.WalReplIndex = REPL_IDX_PRIMARY;
        } else if (walrcv->dummyStandbyConnectFailed) {
            /* if have tried to connect dummy and standby, set failover failed */
            if (t_thrd.walreceiverfuncs_cxt.WalReplIndex == REPL_IDX_STANDBY) {
                SetFailoverFailedState();
                /* else, try to connect to another standby */
            } else {
                conn_target = REPCONNTARGET_STANDBY;
            }
            ereport(LOG, (errmsg("wal receiver get replconninfo[%d] to failover to another instance.",
                                 t_thrd.walreceiverfuncs_cxt.WalReplIndex)));
        } else {
            /* always connect to dummy standby */
            ereport(LOG, (errmsg("wal receiver get conninfo[2] for dummystandby.")));
            t_thrd.walreceiverfuncs_cxt.WalReplIndex = REPL_IDX_STANDBY;
        }
    }
    SpinLockAcquire(&g_instance.comm_cxt.localinfo_cxt.disable_conn_node.info_lck);
    knl_g_disconn_node_context_data connNode =
        g_instance.comm_cxt.localinfo_cxt.disable_conn_node.disable_conn_node_data;
    SpinLockRelease(&g_instance.comm_cxt.localinfo_cxt.disable_conn_node.info_lck);

    /*
     * Skip other connections if you specify a connection host.
     */
    while (checknum--) {
        conninfo = GetRepConnArray(&t_thrd.walreceiverfuncs_cxt.WalReplIndex);
        if (connNode.conn_mode == SPECIFY_CONNECTION &&
            (conninfo != NULL &&
            strcmp(connNode.disable_conn_node_host, (char *)conninfo->remotehost) == 0) &&
            connNode.disable_conn_node_port == conninfo->remoteport) {
            break;
        }

        if (connNode.conn_mode != SPECIFY_CONNECTION) {
            break;
        }

        t_thrd.walreceiverfuncs_cxt.WalReplIndex++;
    }

    if (conninfo != NULL) {
        int rcs = 0;

        SpinLockAcquire(&walrcv->mutex);
        rcs = snprintf_s((char *)walrcv->conninfo, MAXCONNINFO, MAXCONNINFO - 1,
                         "host=%s port=%d localhost=%s localport=%d", conninfo->remotehost, conninfo->remoteport,
                         conninfo->localhost, conninfo->localport);
        securec_check_ss(rcs, "\0", "\0");
        walrcv->conninfo[MAXCONNINFO - 1] = '\0';
        walrcv->conn_errno = NONE_ERROR;
        walrcv->conn_target = conn_target;
        walrcv->conn_channel.localservice = conninfo->localservice;
        walrcv->conn_channel.remoteservice = conninfo->remoteservice;
        SpinLockRelease(&walrcv->mutex);

        SpinLockAcquire(&hashmdata->mutex);
        hashmdata->current_repl = t_thrd.walreceiverfuncs_cxt.WalReplIndex;
        hashmdata->disconnect_count[t_thrd.walreceiverfuncs_cxt.WalReplIndex] = 0;
        SpinLockRelease(&hashmdata->mutex);
        t_thrd.walreceiverfuncs_cxt.WalReplIndex++;
    }
}

/* Report shared memory space needed by WalRcvShmemInit */
Size WalRcvShmemSize(void)
{
    Size size = 0;

    size = add_size(size, sizeof(WalRcvData));

    return size;
}

/* Allocate and initialize walreceiver-related shared memory */
void WalRcvShmemInit(void)
{
    bool found = false;
    errno_t rc = 0;

    t_thrd.walreceiverfuncs_cxt.WalRcv = (WalRcvData *)ShmemInitStruct("Wal Receiver Ctl", WalRcvShmemSize(), &found);

    if (!found) {
        /* First time through, so initialize */
        rc = memset_s(t_thrd.walreceiverfuncs_cxt.WalRcv, WalRcvShmemSize(), 0, WalRcvShmemSize());
        securec_check(rc, "", "");
        t_thrd.walreceiverfuncs_cxt.WalRcv->walRcvState = WALRCV_STOPPED;
        t_thrd.walreceiverfuncs_cxt.WalRcv->node_state = NODESTATE_NORMAL;
        t_thrd.walreceiverfuncs_cxt.WalRcv->conn_errno = NONE_ERROR;
        t_thrd.walreceiverfuncs_cxt.WalRcv->ntries = 0;
        t_thrd.walreceiverfuncs_cxt.WalRcv->dummyStandbySyncPercent = 0;
        t_thrd.walreceiverfuncs_cxt.WalRcv->dummyStandbyConnectFailed = false;
        SpinLockInit(&t_thrd.walreceiverfuncs_cxt.WalRcv->mutex);
        SpinLockInit(&t_thrd.walreceiverfuncs_cxt.WalRcv->exitLock);
    }
}

/* Is walreceiver in progress (or starting up)? */
bool WalRcvInProgress(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    WalRcvState state;
    pg_time_t startTime;

    SpinLockAcquire(&walrcv->mutex);

    state = walrcv->walRcvState;
    startTime = walrcv->startTime;

    SpinLockRelease(&walrcv->mutex);

    /*
     * If it has taken too long for walreceiver to start up, give up. Setting
     * the state to STOPPED ensures that if walreceiver later does start up
     * after all, it will see that it's not supposed to be running and die
     * without doing anything.
     */
    if (state == WALRCV_STARTING) {
        pg_time_t now = (pg_time_t)time(NULL);
        if ((now - startTime) > WALRCV_STARTUP_TIMEOUT) {
            SpinLockAcquire(&walrcv->mutex);
            if (walrcv->walRcvState == WALRCV_STARTING)
                state = walrcv->walRcvState = WALRCV_STOPPED;
            SpinLockRelease(&walrcv->mutex);
            ereport(WARNING, (errmsg("shut down walreceiver due to start up timeout,"
                                     "timeout=%d,now=%ld,starttime=%ld",
                                     WALRCV_STARTUP_TIMEOUT, now, startTime)));
        }
    }

    if (state != WALRCV_STOPPED)
        return true;
    else
        return false;
}

/*
 * return rcv slotname
 *		slot_type: 0 for local 1 for remote
 */
StringInfo get_rcv_slot_name(void)
{
    StringInfo slotname = makeStringInfo();

    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    /* only for dummy standby mode */
    if (IS_DN_DUMMY_STANDYS_MODE()) {
        SpinLockAcquire(&walrcv->mutex);
        appendStringInfo(slotname, "%s", g_instance.attr.attr_common.PGXCNodeName);
        SpinLockRelease(&walrcv->mutex);
    }

    return slotname;
}

/*
 * Set current walrcv's slotname.
 *  depend on have setting the hashmdata->current_repl
 */
static void set_rcv_slot_name(const char *slotname)
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    ReplConnInfo *conninfo = NULL;
    int replIdx = 0;
    errno_t retcode = EOK;

    /* get current repl conninfo, */
    SpinLockAcquire(&hashmdata->mutex);
    replIdx = hashmdata->current_repl;
    SpinLockRelease(&hashmdata->mutex);

    conninfo = GetRepConnArray(&replIdx);

    SpinLockAcquire(&walrcv->mutex);
    if (slotname != NULL) {
        retcode = strncpy_s((char *)walrcv->slotname, NAMEDATALEN, slotname, NAMEDATALEN - 1);
        securec_check(retcode, "\0", "\0");
    } else if (u_sess->attr.attr_common.application_name && strlen(u_sess->attr.attr_common.application_name) > 0) {
        int rc = 0;
        rc = snprintf_s((char *)walrcv->slotname, NAMEDATALEN, NAMEDATALEN - 1, "%s",
                        u_sess->attr.attr_common.application_name);
        securec_check_ss(rc, "\0", "\0");
    } else if (g_instance.attr.attr_common.PGXCNodeName != NULL) {
        int rc = 0;

        if (IS_DN_DUMMY_STANDYS_MODE()) {
            rc = snprintf_s((char *)walrcv->slotname, NAMEDATALEN, NAMEDATALEN - 1, "%s",
                            g_instance.attr.attr_common.PGXCNodeName);
        } else if (conninfo != NULL) {
            rc = snprintf_s((char *)walrcv->slotname, NAMEDATALEN, NAMEDATALEN - 1, "%s_%s_%d",
                            g_instance.attr.attr_common.PGXCNodeName, conninfo->localhost, conninfo->localport);
        }
        securec_check_ss(rc, "\0", "\0");
    } else
        walrcv->slotname[0] = '\0';

    SpinLockRelease(&walrcv->mutex);
    return;
}

void KillWalRcvWriter(void)
{
    volatile WalRcvData *walRcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    ThreadId writerPid;
    int i = 1;
    /*
     * Shutdown WalRcvWriter thread.
     */
    SpinLockAcquire(&walRcv->mutex);
    writerPid = walRcv->writerPid;
    SpinLockRelease(&walRcv->mutex);
    if (writerPid != 0) {
        (void)gs_signal_send(writerPid, SIGTERM);
    }
    ereport(LOG, (errmsg("waiting walrcvwriter: %lu terminate", writerPid)));
    while (writerPid) {
        pg_usleep(10000L);    /* sleep 0.01s */
        SpinLockAcquire(&walRcv->mutex);
        writerPid = walRcv->writerPid;
        SpinLockRelease(&walRcv->mutex);
        if ((writerPid != 0) && (i % 2000 == 0)) {
            if (gs_signal_send(writerPid, SIGTERM) != 0) {
                ereport(WARNING, (errmsg("walrcvwriter:%lu may be terminated", writerPid)));
                break;
            }
            i = 1;
        }
        i++;
    }
}

/*
 * Stop walreceiver (if running) and wait for it to die.
 * Executed by the Startup process.
 */
void ShutdownWalRcv(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    ThreadId walrcvpid = 0;
    int i = 1;

    /*
     * Request walreceiver to stop. Walreceiver will switch to WALRCV_STOPPED
     * mode once it's finished, and will also request postmaster to not
     * restart itself.
     */
    SpinLockAcquire(&walrcv->mutex);
    switch (walrcv->walRcvState) {
        case WALRCV_STOPPED:
            break;
        case WALRCV_STARTING:
            walrcv->walRcvState = WALRCV_STOPPED;
            break;

        case WALRCV_RUNNING:
            walrcv->walRcvState = WALRCV_STOPPING;
            // fall through
        case WALRCV_STOPPING:
            walrcvpid = walrcv->pid;
            break;
    }
    SpinLockRelease(&walrcv->mutex);

    ereport(LOG, (errmsg("startup shut down walreceiver.")));
    /*
     * Signal walreceiver process if it was still running.
     */
    if (walrcvpid != 0)
        (void)gs_signal_send(walrcvpid, SIGTERM);

    /*
     * Wait for walreceiver to acknowledge its death by setting state to
     * WALRCV_STOPPED.
     */
    while (WalRcvInProgress()) {
        /*
         * This possibly-long loop needs to handle interrupts of startup
         * process.
         */
        RedoInterruptCallBack();
        pg_usleep(100000); /* 100ms */

        SpinLockAcquire(&walrcv->mutex);
        walrcvpid = walrcv->pid;
        SpinLockRelease(&walrcv->mutex);

        if ((walrcvpid != 0) && (i % 2000 == 0)) {
            (void)gs_signal_send(walrcvpid, SIGTERM);
            i = 1;
        }
        i++;
    }
}

/*
 * Request postmaster to start walreceiver.
 *
 * recptr indicates the position where streaming should begin, conninfo
 * is a libpq connection string to use, and slotname is, optionally, the name
 * of a replication slot to acquire.
 */
void RequestXLogStreaming(XLogRecPtr *recptr, const char *conninfo, ReplConnTarget conn_target, const char *slotname)
{
    knl_g_disconn_node_context_data disconn_node =
        g_instance.comm_cxt.localinfo_cxt.disable_conn_node.disable_conn_node_data;
    if (disconn_node.conn_mode == PROHIBIT_CONNECTION) {
        ereport(LOG, (errmsg("Stop to start walreceiver in disable connect mode")));
        return;
    }

    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    pg_time_t now = (pg_time_t)time(NULL);
    XLogRecPtr Lcrecptr;
    errno_t retcode = EOK;

    Lcrecptr = *recptr;

    /*
     * We always start at the beginning of the segment. That prevents a broken
     * segment (i.e., with no records in the first half of a segment) from
     * being created by XLOG streaming, which might cause trouble later on if
     * the segment is e.g archived.
     * Prev the requested segment if request xlog from the beginning of a segment.
     */
    if (Lcrecptr % XLogSegSize != 0) {
        Lcrecptr -= Lcrecptr % XLogSegSize;
    } else if (!dummyStandbyMode) {
        XLogSegNo _logSeg;
        XLByteToSeg(Lcrecptr, _logSeg);
        _logSeg--;
        Lcrecptr = _logSeg * XLogSegSize;
    }

    SpinLockAcquire(&walrcv->mutex);
    if (walrcv->ntries > 2 && !dummyStandbyMode) {
        walrcv->isRuning = false;
        SpinLockRelease(&walrcv->mutex);
        return;
    }
    walrcv->conn_target = conn_target;
    /* It better be stopped before we try to restart it */
    Assert(walrcv->walRcvState == WALRCV_STOPPED);
    if(conn_target != REPCONNTARGET_OBS) {
        if (conninfo != NULL) {
            retcode = strncpy_s((char *)walrcv->conninfo, MAXCONNINFO, conninfo, MAXCONNINFO - 1);
            securec_check(retcode, "\0", "\0");
            walrcv->conn_errno = NONE_ERROR;
            walrcv->conn_target = conn_target;
        } else {
            SpinLockRelease(&walrcv->mutex);
            SetWalRcvConninfo(conn_target);
            SpinLockAcquire(&walrcv->mutex);
        }
        SpinLockRelease(&walrcv->mutex);
        set_rcv_slot_name(slotname);
        SpinLockAcquire(&walrcv->mutex);
    }

    walrcv->walRcvState = WALRCV_STARTING;
    walrcv->startTime = now;

    /*
     * If this is the first startup of walreceiver, we initialize receivedUpto
     * and latestChunkStart to receiveStart.
     */
    if (walrcv->receiveStart == 0) {
        walrcv->receivedUpto = Lcrecptr;
        walrcv->latestChunkStart = Lcrecptr;
    }

    walrcv->receiveStart = Lcrecptr;

    walrcv->latestValidRecord = latestValidRecord;
    walrcv->latestRecordCrc = latestRecordCrc;
    SpinLockRelease(&walrcv->mutex);
    WalRcvSetPercentCountStartLsn(walrcv->latestValidRecord);
    if (XLByteLT(latestValidRecord, Lcrecptr))
        ereport(LOG, (errmsg("latest valid record at %X/%X, wal receiver start point at %X/%X",
                             (uint32)(latestValidRecord >> 32), (uint32)latestValidRecord, (uint32)(Lcrecptr >> 32),
                             (uint32)Lcrecptr)));

    SendPostmasterSignal(PMSIGNAL_START_WALRECEIVER);
}

/*
 * Returns the last+1 byte position that walreceiver has written.
 *
 * Optionally, returns the previous chunk start, that is the first byte
 * written in the most recent walreceiver flush cycle.	Callers not
 * interested in that value may pass NULL for latestChunkStart.
 */
XLogRecPtr GetWalRcvWriteRecPtr(XLogRecPtr *latestChunkStart)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    XLogRecPtr recptr;
    errno_t rc = 0;

    SpinLockAcquire(&walrcv->mutex);
    recptr = walrcv->receivedUpto;
    if (latestChunkStart != NULL) {
        /* FUTURE CASE: */
        rc = strncpy_s((char *)latestChunkStart, sizeof(XLogRecPtr), (char *)&walrcv->latestChunkStart,
                       sizeof(XLogRecPtr) - 1);
        securec_check(rc, "\0", "\0");
    }

    SpinLockRelease(&walrcv->mutex);

    return recptr;
}


XLogRecPtr GetWalStartPtr()
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    XLogRecPtr recptr = InvalidXLogRecPtr;

    SpinLockAcquire(&walrcv->mutex);
    WalRcvCtlBlock *ctlBlock = walrcv->walRcvCtlBlock;
    if (ctlBlock != NULL) {
        recptr = ctlBlock->walStart;
    }

    SpinLockRelease(&walrcv->mutex);
    return recptr;
}


bool WalRcvAllReplayIsDone()
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    XLogRecPtr theLatestReplayedRecPtr = InvalidXLogRecPtr;
    XLogRecPtr theLatestReceivedRecPtr = InvalidXLogRecPtr;

    theLatestReplayedRecPtr = GetXLogReplayRecPtr(NULL, NULL);

    SpinLockAcquire(&walrcv->mutex);
    theLatestReceivedRecPtr = walrcv->receiver_received_location;
    SpinLockRelease(&walrcv->mutex);

    if (XLByteLT(theLatestReplayedRecPtr, theLatestReceivedRecPtr)) {
        ereport(
            LOG,
            (errmsg("still waiting for the redo on the standby: the latest replayed %X/%X, the latest received %X/%X.",
                    (uint32)(theLatestReplayedRecPtr >> 32), (uint32)theLatestReplayedRecPtr,
                    (uint32)(theLatestReceivedRecPtr >> 32), (uint32)theLatestReceivedRecPtr)));

        return false;
    }

    ereport(LOG, (errmsg("all redo done on the standby: the latest replayed %X/%X, the latest received %X/%X.",
                         (uint32)(theLatestReplayedRecPtr >> 32), (uint32)theLatestReplayedRecPtr,
                         (uint32)(theLatestReceivedRecPtr >> 32), (uint32)theLatestReceivedRecPtr)));

    return true;
}

bool WalRcvIsDone()
{
    if (g_instance.attr.attr_storage.enable_mix_replication)
        return DataQueueIsEmpty(t_thrd.dataqueue_cxt.DataWriterQueue);
    else
        return walRcvCtlBlockIsEmpty();
}

/*
 * Returns the replication apply delay in ms or -1
 * if the apply delay info is not available
 */
int GetReplicationApplyDelay(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    XLogRecPtr receivePtr;
    XLogRecPtr replayPtr;

    long secs;
    int usecs;
    TimestampTz chunkReplayStartTime;

    SpinLockAcquire(&walrcv->mutex);
    receivePtr = walrcv->receivedUpto;
    SpinLockRelease(&walrcv->mutex);

    replayPtr = GetXLogReplayRecPtr(NULL);
    if (XLByteEQ(receivePtr, replayPtr))
        return 0;

    chunkReplayStartTime = GetCurrentChunkReplayStartTime();
    if (chunkReplayStartTime == 0)
        return -1;

    TimestampDifference(chunkReplayStartTime, GetCurrentTimestamp(), &secs, &usecs);

    return (((int)secs * 1000) + (usecs / 1000));
}

/*
 * Returns the network latency in ms, note that this includes any
 * difference in clock settings between the servers, as well as timezone.
 */
int GetReplicationTransferLatency(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    TimestampTz lastMsgSendTime;
    TimestampTz lastMsgReceiptTime;

    long secs = 0;
    int usecs = 0;
    int ms;

    SpinLockAcquire(&walrcv->mutex);
    lastMsgSendTime = walrcv->lastMsgSendTime;
    lastMsgReceiptTime = walrcv->lastMsgReceiptTime;
    SpinLockRelease(&walrcv->mutex);

    TimestampDifference(lastMsgSendTime, lastMsgReceiptTime, &secs, &usecs);

    ms = ((int)secs * 1000) + (usecs / 1000);

    return ms;
}

int GetWalRcvDummyStandbySyncPercent(void)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    int percent = 0;

    SpinLockAcquire(&walrcv->mutex);
    percent = walrcv->dummyStandbySyncPercent;
    SpinLockRelease(&walrcv->mutex);

    return percent;
}

void SetWalRcvDummyStandbySyncPercent(int percent)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    SpinLockAcquire(&walrcv->mutex);
    walrcv->dummyStandbySyncPercent = percent;
    SpinLockRelease(&walrcv->mutex);
}

/*
 * We check the conninfo one by one. We should consider the method later.
 */
ReplConnInfo *GetRepConnArray(int *cur_idx)
{
    int loop_retry = 0;

    if (*cur_idx < 0 || *cur_idx > MAX_REPLNODE_NUM) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid replication node index:%d", *cur_idx)));
    }

    /* do ... while  until the node is avaliable to me */
    while (loop_retry++ < MAX_REPLNODE_NUM) {
        if (*cur_idx == MAX_REPLNODE_NUM)
            *cur_idx = 1;

        if (t_thrd.postmaster_cxt.ReplConnArray[(*cur_idx)] != NULL) {
            break;
        }
        (*cur_idx)++;
    }

    return (*cur_idx < MAX_REPLNODE_NUM) ? t_thrd.postmaster_cxt.ReplConnArray[*cur_idx] : NULL;
}

void get_failover_host_conninfo_for_dummy(int *repl)
{
    FILE *fp = NULL;
    char newHostPath[MAXPGPATH];
    int ret = 0;
    int uselessSubIdx = 0;
    errno_t rc = EOK;

    if (!dummyStandbyMode || repl == NULL) {
        return;
    }

    /* 1. init file path */
    rc = memset_s(newHostPath, sizeof(newHostPath), 0, sizeof(newHostPath));
    securec_check(rc, "\0", "\0");

    /* 2. get file path */
    ret = snprintf_s(newHostPath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", t_thrd.proc_cxt.DataDir, FAILOVER_HOST_FOR_DUMMY);
    securec_check_ss(ret, "\0", "\0");

    /* 3. open file */
    fp = fopen(newHostPath, "r");
    if (fp == NULL) {
        ereport(LOG, (errmsg("open file failed: %s", newHostPath)));
        return;
    }

    /* 4. get info, the second idx is useless, just for compaitble with pre version. */
    ret = fscanf_s(fp, "%d/%d", repl, &uselessSubIdx);
    if (ret < 0 && !feof(fp)) {
        *repl = -1;
        ereport(LOG, (errmsg("read conninfo failed: %s", newHostPath)));
    }

    /* 5. clear the resource. */
    (void)fclose(fp);
    fp = NULL;
    return;
}

static int get_repl_idx(const char *host, int port)
{
    int i = 0;
    int replIdx = -1;

    for (i = 0; i < MAX_REPLNODE_NUM; ++i) {
        if (t_thrd.postmaster_cxt.ReplConnArray[i] != NULL &&
            strcmp(t_thrd.postmaster_cxt.ReplConnArray[i]->remotehost, host) == 0 &&
            t_thrd.postmaster_cxt.ReplConnArray[i]->remoteport == port) {
            replIdx = i;
            break;
        }
    }

    return replIdx;
}

void set_failover_host_conninfo_for_dummy(const char *remote_host, int remote_port)
{
    FILE *fp = NULL;
    char newHostPath[MAXPGPATH];
    int replIdx = -1;
    int ret = 0;

    if (!dummyStandbyMode || remote_host == NULL || remote_port == 0) {
        return;
    }

    /* 1. get file path */
    ret = snprintf_s(newHostPath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", t_thrd.proc_cxt.DataDir, FAILOVER_HOST_FOR_DUMMY);
    securec_check_ss(ret, "\0", "\0");

    /* 2. get repl idx */
    replIdx = get_repl_idx(remote_host, remote_port);
    if (replIdx < 0) {
        ereport(LOG, (errmsg("remote client is not in replconninfo: %s/%d", remote_host, remote_port)));
        return;
    }

    /* 3. truncate the old file. */
    fp = fopen(newHostPath, "w");
    if (fp == NULL) {
        ereport(LOG, (errmsg("open file failed: %s", newHostPath)));
        return;
    }

    /*
     * 4. write conninfo idx
     * the second idx is useless
     * just for compaitble with pre version.
     */
    ret = fprintf(fp, "%d/0", replIdx - 1);
    if (ret < 0) {
        ereport(LOG, (errmsg("write conninfo failed: %s", newHostPath)));
        (void)fclose(fp);
        fp = NULL;
        return;
    }

    /* 5. free the resource. */
    (void)fflush(fp);
    (void)fclose(fp);
    fp = NULL;

    /* 6. success */
    return;
}

void clean_failover_host_conninfo_for_dummy(void)
{
    char newHostPath[MAXPGPATH];
    int ret = 0;
    errno_t rc = EOK;

    rc = memset_s(newHostPath, sizeof(newHostPath), 0, sizeof(newHostPath));
    securec_check(rc, "\0", "\0");

    ret = snprintf_s(newHostPath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", t_thrd.proc_cxt.DataDir, FAILOVER_HOST_FOR_DUMMY);
    securec_check_ss(ret, "\0", "\0");

    if (unlink(newHostPath) < 0) {
        ereport(LOG, (errmsg("remove %s failed", newHostPath)));
    } else {
        ereport(LOG, (errmsg("remove %s success", newHostPath)));
    }
}

void set_wal_rcv_write_rec_ptr(XLogRecPtr rec_ptr)
{
    /* use volatile pointer to prevent code rearrangement */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    if (XLByteLT(rec_ptr, walrcv->receivedUpto)) {
        walrcv->receivedUpto = rec_ptr;
    }
    if (XLByteLT(rec_ptr, walrcv->latestChunkStart)) {
        walrcv->latestChunkStart = rec_ptr;
    }
    SpinLockRelease(&walrcv->mutex);
}
