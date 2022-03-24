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
#include "access/multi_redo_api.h"
#include "libpq/libpq-fe.h"
#include "postmaster/startup.h"
#include "postmaster/postmaster.h"
#include "replication/dataqueue.h"
#include "replication/replicainternal.h"
#include "replication/walreceiver.h"
#include "replication/slot.h"
#include "replication/dcf_replication.h"
#include "replication/walsender_private.h"
#include "storage/pmsignal.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "gssignal/gs_signal.h"
#include "cipher.h"
#include "openssl/ssl.h"

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
static const int MAX_CONNECT_ERROR_COUNT = 3000;

static void SetWalRcvConninfo(ReplConnTarget conn_target);
static void SetFailoverFailedState(void);
static int cmp_min_lsn(const void *a, const void *b);
extern void SetDataRcvDummyStandbySyncPercent(int percent);

/*
 * How long to wait for walreceiver to start up after requesting
 * postmaster to launch it. In seconds.
 */
#define WALRCV_STARTUP_TIMEOUT 3
#define FAILOVER_HOST_FOR_DUMMY "failover_host_for_dummy"
static int  NORMAL_IP_LEN = 16; /* ipv4 len */
static const char* HADRUSERINFO_CONIG_NAME = "hadr_user_info";

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
    if (walrcb->walFreeOffset == walrcb->walWriteOffset) {
        retState = true;
    }

    SpinLockRelease(&walrcb->mutex);
    LWLockRelease(WALWriteLock);
    return retState;
}

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

static int GetConnectErrorCont(int index)
{
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    int ret = 0;
    SpinLockAcquire(&hashmdata->mutex);
    ret = hashmdata->disconnect_count[index];
    SpinLockRelease(&hashmdata->mutex);
    return ret;
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
    int useIndex = 0;

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

    SpinLockAcquire(&hashmdata->mutex);
    int prev_index = hashmdata->prev_repl;
    SpinLockRelease(&hashmdata->mutex);

    if (prev_index > 0 && (GetConnectErrorCont(prev_index) < MAX_CONNECT_ERROR_COUNT) &&
        IS_CN_DISASTER_RECOVER_MODE) {
        t_thrd.walreceiverfuncs_cxt.WalReplIndex = prev_index;
        ereport(LOG, (errmsg(" SetWalRcvConninfo reuse connection %d.", prev_index)));
        pg_usleep(200000L);
    }

    /*
     * Skip other connections if you specify a connection host.
     */
    while (checknum--) {
        char* ipNoZone = NULL;
        char ipNoZoneData[IP_LEN] = {0};
        conninfo = GetRepConnArray(&t_thrd.walreceiverfuncs_cxt.WalReplIndex);
        if (conninfo == NULL) {
            t_thrd.walreceiverfuncs_cxt.WalReplIndex++;
            break;
        }
        
        /* remove any '%zone' part from an IPv6 address string */
        ipNoZone = remove_ipv6_zone(conninfo->remotehost, ipNoZoneData, IP_LEN);
        if (connNode.conn_mode == SPECIFY_CONNECTION &&
            (conninfo != NULL &&
            strcmp(connNode.disable_conn_node_host, (char *)ipNoZone) == 0) &&
            connNode.disable_conn_node_port == conninfo->remoteport) {
            useIndex = t_thrd.walreceiverfuncs_cxt.WalReplIndex;
            t_thrd.walreceiverfuncs_cxt.WalReplIndex++;
            break;
        }

        if (connNode.conn_mode != SPECIFY_CONNECTION) {
            useIndex = t_thrd.walreceiverfuncs_cxt.WalReplIndex;
            t_thrd.walreceiverfuncs_cxt.WalReplIndex++;
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
#ifdef ENABLE_LITE_MODE
        if (*conninfo->sslmode != '\0') {
            rcs = snprintf_s((char *)walrcv->conninfo, MAXCONNINFO, MAXCONNINFO - 1,
                             "%s sslmode=%s", (char *)walrcv->conninfo, conninfo->sslmode);
        }
#endif
        securec_check_ss(rcs, "\0", "\0");
        walrcv->conninfo[MAXCONNINFO - 1] = '\0';
        walrcv->conn_errno = NONE_ERROR;
        walrcv->conn_target = conn_target;
        SpinLockRelease(&walrcv->mutex);
        ereport(LOG, (errmsg("wal receiver try to connect to %s index %d .", walrcv->conninfo, useIndex)));
        SpinLockAcquire(&hashmdata->mutex);
        if (!IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE)
            hashmdata->current_repl = useIndex;
        else
            hashmdata->current_repl = MAX_REPLNODE_NUM + useIndex;
        SpinLockRelease(&hashmdata->mutex);
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
        t_thrd.walreceiverfuncs_cxt.WalRcv->rcvDoneFromShareStorage = false;
        t_thrd.walreceiverfuncs_cxt.WalRcv->shareStorageTerm = 1;
        SpinLockInit(&t_thrd.walreceiverfuncs_cxt.WalRcv->mutex);
        SpinLockInit(&t_thrd.walreceiverfuncs_cxt.WalRcv->exitLock);
    }
}

bool WalRcvIsRunning(void)
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    WalRcvState state;
    SpinLockAcquire(&walrcv->mutex);
    state = walrcv->walRcvState;
    SpinLockRelease(&walrcv->mutex);
    return (state == WALRCV_RUNNING);
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

/* trim char  ':' and '%' */
static char* trim_ipv6_char(char* str, char* dest)
{
    char* s = dest;
    char* cp_location = str;
    int len = 0;

    /* if ipv4,do nothing */
    if (strchr(str, ':') == NULL) {
        return str;
    }

    for (; *cp_location != '\0' && len < NORMAL_IP_LEN; cp_location++) {

        /* skip the char  ':' and '%' */
        if (*cp_location == ':' || *cp_location == '%') {
            continue;
        }

        *s = *cp_location;
        s++;
        len++;
    }
    *s = '\0';

    return dest;
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
    if (IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE && replIdx >= MAX_REPLNODE_NUM) {
        replIdx = replIdx - MAX_REPLNODE_NUM;
    }
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
            char slotData[NAMEDATALEN] = {'\0'};
            char *slotTmp = NULL;

            /* trim char  ':' and '%' */
            slotTmp = trim_ipv6_char(conninfo->localhost, slotData);

            rc = snprintf_s((char *)walrcv->slotname, NAMEDATALEN, NAMEDATALEN - 1, "%s_%s_%d",
                            g_instance.attr.attr_common.PGXCNodeName, slotTmp, conninfo->localport);
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

#ifndef ENABLE_MULTIPLE_NODES
    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf && t_thrd.dcf_cxt.dcfCtxInfo != nullptr)
        t_thrd.dcf_cxt.dcfCtxInfo->isWalRcvReady = false;
#endif

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
    if (IS_SHARED_STORAGE_MODE) {
        ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.shareStorageXLogCtl;
        ReadShareStorageCtlInfo(ctlInfo);
        if ((uint64)g_instance.attr.attr_storage.xlog_file_size != ctlInfo->xlogFileSize) {
            ereport(FATAL, (errmsg("maybe primary cluster changed xlog_file_size to %lu, current is %lu,"
                "we need exit for change.", ctlInfo->xlogFileSize, g_instance.attr.attr_storage.xlog_file_size)));
        }
    }

    if (HasBuildReason()) {
        ereport(LOG, (errmsg("Stop to start walreceiver due to have build reason")));
        pg_usleep(500000L);
        return;
    }
    SpinLockAcquire(&g_instance.comm_cxt.localinfo_cxt.disable_conn_node.info_lck);
    knl_g_disconn_node_context_data disconn_node =
        g_instance.comm_cxt.localinfo_cxt.disable_conn_node.disable_conn_node_data;
    SpinLockRelease(&g_instance.comm_cxt.localinfo_cxt.disable_conn_node.info_lck);

    if (disconn_node.conn_mode == PROHIBIT_CONNECTION) {
        ereport(LOG, (errmsg("Stop to start walreceiver in disable connect mode")));
        pg_usleep(500000L);
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
    if (conn_target == REPCONNTARGET_SHARED_STORAGE) {
        Lcrecptr -= Lcrecptr % XLogSegSize;
    } else if (Lcrecptr % XLogSegSize != 0) {
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
    walrcv->latestRecordLen = latestRecordLen;
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
    ReplConnInfo *replConnInfo = NULL;
    replconninfo** replConnInfoArray;

    if (*cur_idx < 0 || *cur_idx > MAX_REPLNODE_NUM) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid replication node index:%d", *cur_idx)));
    }
    if (!IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE)
        replConnInfoArray = &t_thrd.postmaster_cxt.ReplConnArray[0];
    else
        replConnInfoArray = &t_thrd.postmaster_cxt.CrossClusterReplConnArray[0];

    /* do ... while  until the node is avaliable to me */
    while (loop_retry++ < MAX_REPLNODE_NUM) {
        if (*cur_idx == MAX_REPLNODE_NUM)
            *cur_idx = 1;

        replConnInfo = replConnInfoArray[*cur_idx];
        if (replConnInfo != NULL) {
            if (t_thrd.postmaster_cxt.HaShmData->is_cross_region) {
                if (t_thrd.postmaster_cxt.HaShmData->is_hadr_main_standby || IS_PGXC_COORDINATOR) {
                    if (replConnInfo->isCrossRegion) {
                        return replConnInfo;
                    }
                } else {
                    if (replConnInfo->isCascade || (!replConnInfo->isCascade && !replConnInfo->isCrossRegion)) {
                        return replConnInfo;
                    }
                }
            } else {
                return replConnInfo;
            }
        }
        (*cur_idx)++;
    }

    return NULL;
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
    char* ipNoZone = NULL;
    char ipNoZoneData[IP_LEN] = {0};

    for (i = 0; i < MAX_REPLNODE_NUM; ++i) {
        ReplConnInfo* replconninfo = t_thrd.postmaster_cxt.ReplConnArray[i];
        if (replconninfo == NULL) {
            continue;
        }

        /* remove any '%zone' part from an IPv6 address string */
        ipNoZone = remove_ipv6_zone(replconninfo->remotehost, ipNoZoneData, IP_LEN);

        if (strcmp(ipNoZone, host) == 0 &&
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

/*
 * Set the specified rebuild reason in HaShmData. when set the rebuild reason,
 * the hashmdata->current_repl implys the current replconnlist.
 * Then set the reason in the corresponding variable.
 */
static void ha_set_rebuild_reason(HaRebuildReason reason)
{
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    SpinLockAcquire(&hashmdata->mutex);
    hashmdata->repl_reason[hashmdata->current_repl] = reason;
    SpinLockRelease(&hashmdata->mutex);
}

/* set the rebuild reason and walreceiver connerror. */
void ha_set_rebuild_connerror(HaRebuildReason reason, WalRcvConnError connerror)
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    ha_set_rebuild_reason(reason);
    SpinLockAcquire(&walrcv->mutex);
    walrcv->conn_errno = connerror;
    if (reason == NONE_REBUILD && connerror == NONE_ERROR)
        walrcv->node_state = NODESTATE_NORMAL;
    SpinLockRelease(&walrcv->mutex);
    /* Only postmaster can update gaussdb.state file */
    SendPostmasterSignal(PMSIGNAL_UPDATE_HAREBUILD_REASON);
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
            } else if (AM_HADR_WAL_RECEIVER && !getPeerRole) {
                return "Main Standby";
            } else {
                return "Standby";
            }
        }
        case CASCADE_STANDBY_MODE:
            return "Cascade Standby";
        case MAIN_STANDBY_MODE:
            return "Main Standby";
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
        case DCF_LOG_LOSS_REBUILD:
            return "DCF log loss";
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
    if (local_role == NORMAL_MODE || local_role == PRIMARY_MODE || IS_OBS_DISASTER_RECOVER_MODE) {
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
    ReplConnInfo *replConnInfo = NULL;
    if (hashmdata->current_repl >= MAX_REPLNODE_NUM) {
        replConnInfo = t_thrd.postmaster_cxt.CrossClusterReplConnArray[hashmdata->current_repl - MAX_REPLNODE_NUM];
    } else {
        replConnInfo = t_thrd.postmaster_cxt.ReplConnArray[hashmdata->current_repl];
    }
    if ((replConnInfo != NULL &&
        (walrcv->conn_target == REPCONNTARGET_PRIMARY || am_cascade_standby() || IS_SHARED_STORAGE_MODE)) ||
        IS_OBS_DISASTER_RECOVER_MODE) {
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

void wal_get_ha_rebuild_reason(char *buildReason, ServerMode local_role, bool isRunning)
{
    if (IS_DN_DUMMY_STANDYS_MODE())
        wal_get_ha_rebuild_reason_with_dummy(buildReason, local_role, isRunning);
    else
        wal_get_ha_rebuild_reason_with_multi(buildReason, local_role, isRunning);
}

static int cmp_min_lsn(const void *a, const void *b)
{
    XLogRecPtr lsn1 = *((const XLogRecPtr *)a);
    XLogRecPtr lsn2 = *((const XLogRecPtr *)b);

    if (!XLByteLE(lsn1, lsn2))
        return -1;
    else if (XLByteEQ(lsn1, lsn2))
        return 0;
    else
        return 1;
}

void GetMinLsnRecordsFromHadrCascadeStandby(void)
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    XLogRecPtr standbyReceiveList[g_instance.attr.attr_storage.max_wal_senders];
    XLogRecPtr standbyFlushList[g_instance.attr.attr_storage.max_wal_senders];
    XLogRecPtr standbyApplyList[g_instance.attr.attr_storage.max_wal_senders];
    uint32 standbyFlagsList[g_instance.attr.attr_storage.max_wal_senders];
    uint32 standbyFlags = 0;
    int i;
    XLogRecPtr applyLoc = InvalidXLogRecPtr;
    XLogRecPtr ReplayReadPtr = InvalidXLogRecPtr;
    bool needReport = false;
    errno_t rc = EOK;

    rc = memset_s(standbyReceiveList, sizeof(standbyReceiveList), 0, sizeof(standbyReceiveList));
    securec_check(rc, "\0", "\0");
    rc = memset_s(standbyFlushList, sizeof(standbyFlushList), 0, sizeof(standbyFlushList));
    securec_check(rc, "\0", "\0");
    rc = memset_s(standbyApplyList, sizeof(standbyApplyList), 0, sizeof(standbyApplyList));
    securec_check(rc, "\0", "\0");
    rc = memset_s(standbyFlagsList, sizeof(standbyFlagsList), 0, sizeof(standbyFlagsList));
    securec_check(rc, "\0", "\0");

    for (i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        volatile WalSnd *walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[i];
        SpinLockAcquire(&walsnd->mutex);
        if (walsnd->pid != 0 && walsnd->pid != t_thrd.proc_cxt.MyProcPid &&
            walsnd->sendRole == SNDROLE_PRIMARY_STANDBY) {
            standbyApplyList[i] = walsnd->apply;
            standbyReceiveList[i] = walsnd->receive;
            standbyFlushList[i] = walsnd->flush;
            standbyFlagsList[i] = walsnd->replyFlags;
        }
        SpinLockRelease(&walsnd->mutex);
    }

    qsort(standbyReceiveList, g_instance.attr.attr_storage.max_wal_senders, sizeof(XLogRecPtr), cmp_min_lsn);
    qsort(standbyFlushList, g_instance.attr.attr_storage.max_wal_senders, sizeof(XLogRecPtr), cmp_min_lsn);
    qsort(standbyApplyList, g_instance.attr.attr_storage.max_wal_senders, sizeof(XLogRecPtr), cmp_min_lsn);
    applyLoc = GetXLogReplayRecPtr(NULL, &ReplayReadPtr);
    t_thrd.walreceiver_cxt.reply_message->applyRead = ReplayReadPtr;
    int min_require = Min(g_instance.attr.attr_storage.max_wal_senders, t_thrd.syncrep_cxt.SyncRepMaxPossib);
    for (i = min_require - 1 ; i >= 0; i--) {
        if (i < min_require - 1) {
            needReport = true;
        }
        if (standbyReceiveList[i] != InvalidXLogRecPtr) {
            t_thrd.walreceiver_cxt.reply_message->receive = standbyReceiveList[i];
            if (needReport) {
                ereport(DEBUG1, (errmsg(
                    "In disaster cluster, some cascade standbys are abnormal, using min valid receive location.")));
            }
        } else if (standbyReceiveList[i] == InvalidXLogRecPtr && i == 0) {
            ereport(DEBUG1, (errmsg(
                "In disaster cluster, all cascade standbys are abnormal, using local receive location.")));
        }
        if (standbyFlushList[i] != InvalidXLogRecPtr) {
            t_thrd.walreceiver_cxt.reply_message->flush = standbyFlushList[i];
            if (needReport) {
                ereport(DEBUG1, (errmsg(
                    "In disaster cluster, some cascade standbys are abnormal, using min valid flush location.")));
            }
        } else if (standbyFlushList[i] == InvalidXLogRecPtr && i == 0) {
            ereport(DEBUG1, (errmsg(
                "In disaster cluster, all cascade standbys are abnormal, using local flush location.")));
        }
        if (standbyApplyList[i] != InvalidXLogRecPtr) {
            t_thrd.walreceiver_cxt.reply_message->apply = standbyApplyList[i];
            if (needReport) {
                ereport(DEBUG1, (errmsg(
                    "In disaster cluster, some cascade standbys are abnormal, using min valid apply location.")));
            }
        } else if (standbyApplyList[i] == InvalidXLogRecPtr && i == 0) {
            t_thrd.walreceiver_cxt.reply_message->apply = applyLoc;
            ereport(DEBUG1, (errmsg(
                "In disaster cluster, all cascade standbys are abnormal, using local apply location.")));
        }
        /* all cascade standby is not pause by targetBarrier, we set flag 0 */
        standbyFlags |= standbyFlagsList[i] & IS_PAUSE_BY_TARGET_BARRIER;
    }
    SpinLockAcquire(&walrcv->mutex);
    t_thrd.walreceiver_cxt.reply_message->replyFlags |= standbyFlags;
    SpinLockRelease(&walrcv->mutex);
    if (t_thrd.walreceiver_cxt.reply_message->apply > t_thrd.walreceiver_cxt.reply_message->flush) {
        ereport(LOG, (errmsg(
            "In disaster cluster, the reply message of quorum flush location is less than replay location,"
            "flush is %X/%X, replay is %X/%X.", (uint32)(t_thrd.walreceiver_cxt.reply_message->flush >> 32),
            (uint32)t_thrd.walreceiver_cxt.reply_message->flush,
            (uint32)(t_thrd.walreceiver_cxt.reply_message->apply >> 32),
            (uint32)t_thrd.walreceiver_cxt.reply_message->apply)));
    }
}

static void GetHadrUserInfo(char *hadr_user_info)
{
    char conninfo[MAXPGPATH] = {0};
    char query[MAXPGPATH] = {0};
    char conn_error_msg[MAXPGPATH] = {0};
    PGconn* pgconn = NULL;
    PGresult* res = NULL;
    char* value = NULL;
    errno_t rc;

    rc = snprintf_s(query,
        sizeof(query),
        sizeof(query) - 1,
        "SELECT VALUE FROM GS_GLOBAL_CONFIG WHERE NAME = '%s';",
        HADRUSERINFO_CONIG_NAME);
    securec_check_ss_c(rc, "\0", "\0");
    rc = snprintf_s(conninfo,
        sizeof(conninfo),
        sizeof(conninfo) - 1,
        "dbname=postgres port=%d host=%s "
        "connect_timeout=60 application_name='local_hadr_walrcv' "
        "options='-c xc_maintenance_mode=on'",
        g_instance.attr.attr_network.PostPortNumber,
        g_instance.attr.attr_network.tcp_link_addr);
    securec_check_ss_c(rc, "\0", "\0");

    pgconn = PQconnectdb(conninfo);
    if (PQstatus(pgconn) != CONNECTION_OK) {
        rc = snprintf_s(conn_error_msg, MAXPGPATH, MAXPGPATH - 1,
                        "%s", PQerrorMessage(pgconn));
        securec_check_ss(rc, "\0", "\0");

        PQfinish(pgconn);
        ereport(ERROR, (errmsg("hadr walreceiver connect to local database fail: %s", conn_error_msg)));
        return;
    }

    res = PQexec(pgconn, query);
    if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        rc = snprintf_s(conn_error_msg, MAXPGPATH, MAXPGPATH - 1,
                        "%s", PQresultErrorMessage(res));
        securec_check_ss(rc, "\0", "\0");

        PQclear(res);
        PQfinish(pgconn);
        ereport(ERROR, (errmsg("hadr walreceiver could not obtain hadr_user_info: %s", conn_error_msg)));
        return;
    }

    value = PQgetvalue(res, 0, 0);
    rc = strcpy_s(hadr_user_info, MAXPGPATH, value);
    securec_check_ss(rc, "\0", "\0");

    PQclear(res);
    PQfinish(pgconn);
}

void GetPasswordForHadrStreamingReplication(char user[], char password[])
{
    char hadr_user_info[MAXPGPATH] = {0};
    char plain_hadr_user_info[MAXPGPATH] = {0};
    errno_t rc = EOK;

    GetHadrUserInfo(hadr_user_info);
    if (!decryptECString(hadr_user_info, plain_hadr_user_info, MAXPGPATH, HADR_MODE)) {
        rc = memset_s(plain_hadr_user_info, sizeof(plain_hadr_user_info), 0, sizeof(plain_hadr_user_info));
        securec_check(rc, "\0", "\0");
        ereport(ERROR, (errmsg("In disaster cluster, decrypt hadr_user_info fail.")));
    }
    if (sscanf_s(plain_hadr_user_info, "%[^|]|%s", user, MAXPGPATH, password, MAXPGPATH) != 2) {
        rc = memset_s(plain_hadr_user_info, sizeof(plain_hadr_user_info), 0, sizeof(plain_hadr_user_info));
        securec_check(rc, "\0", "\0");
        ereport(ERROR, (errmsg("In disaster cluster, parse plain hadr_user_info fail.")));
    }
    rc = memset_s(plain_hadr_user_info, sizeof(plain_hadr_user_info), 0, sizeof(plain_hadr_user_info));
    securec_check(rc, "\0", "\0");
}
