/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * snapcapturer.cpp
 *
 * openGauss recyclebin cleaner thread Implementation
 *
 * IDENTIFICATION
 * src/gausskernel/process/postmaster/rbcleaner.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_database.h"
#include "catalog/pg_snapshot.h"
#include "commands/dbcommands.h"
#include "fmgr.h"
#include "gssignal/gs_signal.h"
#include "knl/knl_thread.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/procarray.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/tcap.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "postmaster/rbcleaner.h"

const int RBCLEANER_INTERVAL_QUICK_MS = 10;
const int RBCLEANER_INTERVAL_MS = 3000;

/*
 * RbCleanerShmemSize
 *		Size of Rbcleaner related shared memory
 */
Size RbCleanerShmemSize()
{
    return sizeof(RbCleanerShmemStruct);
}

/*
 * RbCleanerShmemInit
 *		Allocate and initialize Rbcleanr -related shared memory
 */
void RbCleanerShmemInit(void)
{
    bool found = false;

    t_thrd.rbcleaner_cxt.RbCleanerShmem =
        (RbCleanerShmemStruct *)ShmemInitStruct("RbCleaner Data", sizeof(RbCleanerShmemStruct), &found);

    if (!found) {
        /*
         * First time through, so initialize.  Note that we zero the whole
         * requests array; this is so that CompactCheckpointerRequestQueue
         * can assume that any pad bytes in the request structs are zeroes.
         */
        errno_t ret = memset_s(t_thrd.rbcleaner_cxt.RbCleanerShmem, sizeof(RbCleanerShmemStruct),
            0, sizeof(RbCleanerShmemStruct));
        securec_check(ret, "\0", "\0");

        RbQueueInit(&t_thrd.rbcleaner_cxt.RbCleanerShmem->queue);
    }

    return;
}

static bool RbCleanerIsAlive()
{
    return pg_atomic_read_u64(&t_thrd.rbcleaner_cxt.RbCleanerShmem->rbCleanerPid) != 0;
}

static void RbCleanerAlive()
{
    int ntries;
    int maxtries = 50;

    /* Wait 5s until rbcleaner startup. */
    for (ntries = 0;; ntries++) {
        if (pg_atomic_read_u64(&t_thrd.rbcleaner_cxt.RbCleanerShmem->rbCleanerPid) != 0) {
            break;
        }
        if (ntries >= maxtries) {
            ereport(ERROR, 
                (errmsg("rbcleaner not running")));
        }
        elog(LOG, "retry to wait rbcleaner started");

        /* wait 0.1 sec, then retry */
        pg_usleep(100000L);
    }
}

static void RbCltWakeupCleaner()
{
    SetLatch(t_thrd.rbcleaner_cxt.RbCleanerShmem->rbCleanerLatch);
}

static uint32 RbCltSubmit(PurgeMsg *localMsg, bool wait = true)
{
    PurgeMsgQueue *msgQue = RbGetQueue();
    PurgeMsg *prMsg = NULL;
    uint64 currMsgId;

    while (true) {
        SpinLockAcquire(&msgQue->mutex);
        if (!RbQueueIsFull(msgQue)) {
            break;
        }
        SpinLockRelease(&msgQue->mutex);

        if (wait) {
            /* wait 0.01 sec, then retry */
            elog(LOG, "purge message queue is full, try again.");
            pg_usleep(10000L);
        } else {
            elog(LOG, "purge message queue is full, ignore.");
            return RB_INVALID_MSGID;
        }
    }
    prMsg = RbQueueNext(msgQue);

    SpinLockAcquire(&prMsg->mutex);

    /* Set id. */
    currMsgId = RbGetNextMsgId();
    Assert((currMsgId % RB_MAX_MSGQ_SIZE) == msgQue->tail);
    if (prMsg->id != RB_INVALID_MSGID) {
        elog(WARNING, "purge message %lu was overrided by %lu", prMsg->id, currMsgId);
    }
    prMsg->id = currMsgId;
    /* Set req. */
    RbMsgCopyReq(&prMsg->req, &localMsg->req);
    /* Set res. */
    RbMsgResetRes(&prMsg->res);
    /* Set Latch. */
    InitLatch(&prMsg->latch);

    RbQueuePush(msgQue);

    SpinLockRelease(&prMsg->mutex);
    SpinLockRelease(&msgQue->mutex);

    return prMsg->id;
}

static uint32 RbCltPushMsg(PurgeMsg *localMsg)
{
    uint32 id;

    /* Make sure rbcleaner alive. */
    RbCleanerAlive();

    /* Submit a purge message. */
    id = RbCltSubmit(localMsg);

    /* Wakeup cleaner to process a new prMsg. */
    RbCltWakeupCleaner();

    return id;
}

static void RbCltCancel(uint64 id)
{
    PurgeMsg *rbMsg = RbMsg(id);
    SpinLockAcquire(&rbMsg->mutex);
    if (rbMsg->res.status != PURGE_MSG_STATUS_OVERRIDE) {
        rbMsg->req.cancel = true;
    }
    SpinLockRelease(&rbMsg->mutex);
}

void RbCltProcInterrupts(int rc, uint64 id)
{
    if (((unsigned int)rc) & WL_POSTMASTER_DEATH) {
        ereport(LOG, 
            (errmsg("terminating connection due to postmaster death.")));
        proc_exit(1);
    }

    if (t_thrd.int_cxt.ProcDiePending || t_thrd.proc_cxt.proc_exit_inprogress) {
        RbCltCancel(id);
        ereport(FATAL,
            (errcode(ERRCODE_ADMIN_SHUTDOWN), 
                errmsg("canceling the wait for rbcleaner response and "
                    "terminating connection due to administrator command")));
    }

    if (t_thrd.int_cxt.QueryCancelPending) {
        RbCltCancel(id);
        ereport(ERROR,
            (errmsg("canceling wait for rbcleaner response due to user request")));
    }

    if (!RbCleanerIsAlive()) {
        ereport(ERROR, (errmsg("rbcleaner not running")));
    }
}
static void RbCltWaitMsg(uint64 id, PurgeMsg *localMsg)
{
    PurgeMsg *rbMsg = RbMsg(id);
    uint32 totalTimes = 300;
    uint32 retryNums = 0;

    while (true) {
        int rc;

        /* Clear any already-pending wakeups */
        ResetLatch(&rbMsg->latch);

        /* Wait latch for a maximum of 1 sec for a rbcleaner response. */
        rc = WaitLatch(&rbMsg->latch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 1000L);

        /* Process interrupts. */
        RbCltProcInterrupts(rc, id);

        /* Check rbcleaner response. */
        RbMsgGetRes(id, &localMsg->res, true);
        if (RbMsgIsDone(localMsg->res.status)) {
            break;
        }

        /* Break if rbcleaner not response in a maximum of 300 seconds. */
        if (retryNums > totalTimes) {
            RbCltCancel(id);
            RbMsgSetStatusErrLocal(&localMsg->res, PURGE_MSG_STATUS_ERROR, 
                ERRCODE_INTERNAL_ERROR, "wait response from rbcleaner timeout over 300 seconds");
            break;
        }
        retryNums++;
    }
}

static void RbCltExec(PurgeMsg *localMsg)
{
    int ntries;
    int maxtries = 10;

    for (ntries = 0;; ntries++) {
        uint32 id;

        /* 1. Push the purge message to rbcleaner. */
        id = RbCltPushMsg(localMsg);

        /* 2. Wait until the purge message processed. */
        RbCltWaitMsg(id, localMsg);

        /* 3. Break if res not overrided. */
        if (localMsg->res.status != PURGE_MSG_STATUS_OVERRIDE) {
            return;
        }
        elog(LOG, "purge message \"%u\" result overrided, try again", id);
    }

    elog(ERROR, "purge message result overrided over \"%d\" times", maxtries);
}

static void RbErrReportLockConflict(const PurgeMsgRes *res)
{
    const char *rbErrFormatLockConflict = "autopurge failed due to lock conflict: %s. "
        "DETAIL: %d objects purged, %d objects lock conflicted, "
        "%d objects not exist.";
    ereport(ERROR, 
        (errcode(ERRCODE_RBIN_LOCK_NOT_AVAILABLE), 
            errmsg(rbErrFormatLockConflict, res->errMsg, res->purgedNum, 
                res->skippedNum, res->undefinedNum)));
}

static void RbErrReport(PurgeMsgRes *res)
{
    const char *rbErrFormatError = "autopurge failed due to unexpected error: %s. "
        "DETAIL: %d objects purged, %d objects lock conflicted, "
        "%d objects not exist.";
    ereport(ERROR, 
        (errcode(res->errCode), 
            errmsg(rbErrFormatError, res->errMsg, res->purgedNum,
                res->skippedNum, res->undefinedNum)));
}

static void RbCltDone(PurgeMsg *msg, bool isDML = false, bool *purged = NULL)
{
    if (RbResIsError(msg->res.errCode)) {
        RbErrReport(&msg->res);
    }

    /* non-DML purge cmds */
    if (!isDML) {
        if (msg->res.skippedNum > 0) {
            RbErrReportLockConflict(&msg->res);
        }
        return;
    }

    /* DML purge cmd */
    if (msg->res.purgedNum + msg->res.undefinedNum > 0) {
        *purged = true;
    } else if (msg->res.skippedNum == 0) {
        *purged = false;
    } else {
        RbErrReportLockConflict(&msg->res);
    }
}

bool RbCltPurgeSpaceDML(Oid spcId)
{
    if (!TcapFeatureAvail()) {
        return false;
    }

    PurgeMsg prMsg;
    bool purged = false;

    if (TrRbIsEmptySpc(spcId)) {
        return false;
    }

    RbMsgInit(&prMsg, PURGE_MSG_TYPE_DML, spcId, u_sess->proc_cxt.MyDatabaseId);
    RbCltExec(&prMsg);
    RbCltDone(&prMsg, true, &purged);

    return purged;
}

void RbCltPurgeSpace(Oid spcId)
{
    if (!TcapFeatureAvail()) {
        return;
    }

    if (!TrRbIsEmptySpc(spcId)) {
        elog(ERROR, "cannot execute this command because other recycle objects depend on the object, "
            "use \"purge recyclebin\" to clean recyclebin then try again.");
    }
}

void RbCltPurgeRecyclebin()
{
    if (!TcapFeatureAvail()) {
        return;
    }

    PurgeMsg prMsg;

    if (TrRbIsEmptyDb(u_sess->proc_cxt.MyDatabaseId)) {
        return;
    }

    RbMsgInit(&prMsg, PURGE_MSG_TYPE_RECYCLEBIN, InvalidOid, u_sess->proc_cxt.MyDatabaseId);
    RbCltExec(&prMsg);
    RbCltDone(&prMsg);
}

void RbCltPurgeSchema(Oid nspId)
{
    if (!TcapFeatureAvail()) {
        return;
    }

    if (!TrRbIsEmptySchema(nspId)) {
        elog(ERROR, "cannot execute this command because other recycle objects depend on the object, "
            "use \"purge recyclebin\" to clean recyclebin then try again.");
        return;
    }
}

void RbCltPurgeUser(Oid roleId)
{
    if (!TcapFeatureAvail()) {
        return;
    }

    if (!TrRbIsEmptyUser(roleId)) {
        elog(ERROR, "cannot execute this command because other recycle objects depend on the object, "
            "use \"purge recyclebin\" to clean recyclebin then try again.");
    }
}

void RbCltPurgeDatabase(Oid dbId)
{
    if (!TcapFeatureAvail()) {
        return;
    }

    if (!TrRbIsEmptyDb(dbId)) {
        elog(ERROR, "cannot execute this command because other recycle objects depend on the object, "
            "use \"purge recyclebin\" to clean recyclebin then try again.");
    }
}

ThreadId StartRbCleaner(void)
{
    if (!IsPostmasterEnvironment) {
        return 0;
    }

    if (canAcceptConnections(false) == CAC_OK) {
        return initialize_util_thread(RBCLEANER);
    }

    ereport(LOG,
        (errmsg("not ready to start recyclebin cleaner.")));
    return 0;
}

bool IsRbCleanerProcess(void)
{
    return t_thrd.role == RBCLEANER;
}

static bool RbWorkerIsAlive()
{
    RbWorkerInfo *workInfo = RbGetWorkerInfo();
    int ntries;
    int maxtries = 500;

    /* Wait 5s until rbcleaner stopped. */
    for (ntries = 0;; ntries++) {
        ThreadId rbworkerPid = pg_atomic_read_u64(&workInfo->rbworkerPid);
        if (rbworkerPid == 0) {
            return false;
        }
        if (ntries >= maxtries) {
            ereport(LOG, (errmodule(MOD_TIMECAPSULE),
                errmsg("the old rbworker still exists in 5s, the rbworkerPid is %lu.", rbworkerPid)));
            return true;
        }
        /* wait 0.01 sec, then retry */
        pg_usleep(10000L);
    }

    return true;
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void RbSighupHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.rbcleaner_cxt.got_SIGHUP = true;

    SetLatch(&t_thrd.proc->procLatch);

    errno = saveErrno;

    elog(LOG, "rbcleaner signaled: SIGHUP");
}

static void RbCancelRbworker()
{
    RbWorkerInfo *workInfo = RbGetWorkerInfo();
    ThreadId rbworkerPid = pg_atomic_read_u64(&workInfo->rbworkerPid);
    if (rbworkerPid != 0 && gs_signal_send(rbworkerPid, SIGINT)) {
        ereport(ERROR, 
            (errmsg("could not send SIGINT signal to rbworker %lu: %m", 
                rbworkerPid)));
    }

    elog(LOG, "rbcleaner: send SIGINT to rbworker %lu", rbworkerPid);
}

/*
 * Query-cancel signal from postmaster: abort current transaction
 * at soonest convenient time
 */
void RbSigintHandler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    RbCancelRbworker();

    /*
     * Don't joggle the elbow of proc_exit
     */
    if (!t_thrd.proc_cxt.proc_exit_inprogress) {
        InterruptPending = true;
        t_thrd.int_cxt.QueryCancelPending = true;

        /*
         * in libcomm interrupt is not allow,
         * gs_r_cancel will signal libcomm and
         * libcomm will then check for interrupt.
         */
        gs_r_cancel();

        /*
         * If it's safe to interrupt, and we're waiting for input or a lock,
         * service the interrupt immediately
         */
        if (t_thrd.int_cxt.ImmediateInterruptOK && t_thrd.int_cxt.InterruptHoldoffCount == 0 &&
            t_thrd.int_cxt.CritSectionCount == 0) {
            /* bump holdoff count to make ProcessInterrupts() a no-op */
            /* until we are done getting ready for it */
            t_thrd.int_cxt.InterruptHoldoffCount++;
            LockErrorCleanup(); /* prevent CheckDeadLock from running */
            t_thrd.int_cxt.InterruptHoldoffCount--;
            ProcessInterrupts();
        }
    }

    /* If we're still here, waken anything waiting on the process latch */
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = saveErrno;
}
 
/* SIGTERM: time to die */
static void RbSigtermHander(SIGNAL_ARGS)
{
    int saveErrno = errno;

    t_thrd.rbcleaner_cxt.got_SIGTERM = true;

    SetLatch(&t_thrd.proc->procLatch);

    errno = saveErrno;

    elog(LOG, "rbcleaner signaled: SIGTERM");
}

static void RbSigquitHandler(SIGNAL_ARGS)
{   
    sigaddset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT); /* prevent nested calls */
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    on_exit_reset();

    gs_thread_exit(2);

    elog(LOG, "rbcleaner signaled: SIGQUIT");
}

/*
 * signal handle functions
 */
 
/* SIGUSR2: there is some message to purge the tablespace */
static void RbSigusr2Hander(SIGNAL_ARGS)
{
    int saveErrno = errno;

    SetLatch(&t_thrd.proc->procLatch);

    errno = saveErrno;

    elog(LOG, "rbcleaner signaled: SIGUSR2");
}

static void RbCleanerQuitAndClean(int code, Datum arg)
{
    pg_atomic_write_u64(&t_thrd.rbcleaner_cxt.RbCleanerShmem->rbCleanerPid, 0);
}

static List *RbGetDbList(uint64 id)
{
    PurgeMsg *msg = RbMsg(id);
    List *l = NIL;

    if (msg->req.dbId != InvalidOid) {
        char *dbname = NULL;
        dbname = get_database_name(msg->req.dbId);
        if (dbname == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_DATABASE),
                    errmsg("database \"%u\" does not exist", msg->req.dbId)));
        }
        return lappend(l, dbname);
    }

    Assert(id == msg->id);
    switch (msg->req.type) {
        case PURGE_MSG_TYPE_DML:
        case PURGE_MSG_TYPE_TABLESPACE:
        case PURGE_MSG_TYPE_CAS_TABLESPACE:
            l = TrGetDbListSpc(msg->req.objId);
            break;
        case PURGE_MSG_TYPE_RECYCLEBIN:
            l = TrGetDbListRcy();
            break;
        case PURGE_MSG_TYPE_CAS_SCHEMA:
            l = TrGetDbListSchema(msg->req.objId);
            break;
        case PURGE_MSG_TYPE_CAS_USER:
            l = TrGetDbListUser(msg->req.objId);
            break;
        case PURGE_MSG_TYPE_AUTO:
            l = TrGetDbListAuto();
            break;
        default:
            elog(ERROR, "unknown purge message type: %d", msg->req.type);
            break;
    }

    return l;
}

static void RbCleanerPurgeFinal(uint64 id)
{
    int errCode = *(volatile int *)&RbMsg(id)->res.errCode;

    PurgeMsgStatus status = RbResIsError(errCode) ? 
        PURGE_MSG_STATUS_ERROR : PURGE_MSG_STATUS_SUCCESS;

    RbMsgSetStatus(id, status, true);

    return;
}

static bool RbMsgStepRes(uint64 id)
{
    PurgeMsg *rbMsg = RbMsg(id);
    PurgeMsgRes localRes;

    Assert(id == rbMsg->id);
    RbMsgGetRes(id, &localRes);
    Assert(localRes.status == PURGE_MSG_STATUS_STEPDONE);
    if (RbResIsError(localRes.errCode) || localRes.skippedNum > 0) {
        return true;
    }
    if (rbMsg->req.type == PURGE_MSG_TYPE_DML) {
        return localRes.purgedNum > 0;
    }
    return localRes.skippedNum > 0;
}

static void RbCleanerProcInterrupts(int rc, uint64 id = RB_INVALID_MSGID)
{
    /* Process sinval catchup interrupts that happened while sleeping */
    ProcessCatchupInterrupt();

    /*
     * Emergency bailout if postmaster has died.  This is to avoid the
     * necessity for manual cleanup of all postmaster children.
     */
    if (((unsigned int)rc) & WL_POSTMASTER_DEATH) {
        proc_exit(1);
    }
    
    /* the normal shutdown case */
    if (t_thrd.rbcleaner_cxt.got_SIGTERM) {
        elog(LOG, "rbcleaner is shutting down.");
        proc_exit(0);
    }

    /*
     * reload the postgresql.conf
     */
    if (t_thrd.rbcleaner_cxt.got_SIGHUP) {
        t_thrd.rbcleaner_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }

    /* Process rbclt cancel message. */
    if ((id != RB_INVALID_MSGID && RbMsgCanceled(id))) {
        RbCancelRbworker();
        ereport(ERROR,
            (errmsg("canceling rbcleaner due to user request")));
    }

    /* Process rbclt cancel message. */
    if (t_thrd.int_cxt.QueryCancelPending) {
        ProcessInterrupts();
    }
}

static bool RbStartWorkerTimeout(TimestampTz launchTime)
{
    /* The rbcleaner waits for a maximum of 30 seconds for the rbworker to start. */
    return TimestampDifferenceExceeds(launchTime, GetCurrentTimestamp(), 30000);
}

static void RbCleanerPurgeImpl(uint64 id)
{
    List *l = NULL;
    ListCell *cell = NULL;
    const long int rbCleanerPurgeIntervalMs = 1000L;

    RbCleanerProcInterrupts(0, id);

    StartTransactionCommand();
    l = RbGetDbList(id);
    foreach (cell, l) {
        RbWorkerInfo *workerInfo = NULL;
        TimestampTz launchTime;

        if (RbWorkerIsAlive()) {
            ereport(ERROR, (errmodule(MOD_TIMECAPSULE),
                errmsg("start rbworker failed: rbworker already exists!")));
        }

        workerInfo = RbInitWorkerInfo(id, (char *)lfirst(cell));
        SendPostmasterSignal(PMSIGNAL_START_RB_WORKER);

        launchTime = GetCurrentTimestamp();
        while (true) {
            int rc;

            /* Clear any already-pending wakeups */
            ResetLatch(&workerInfo->latch);

            /* Wait latch for a maximum of 1 sec for a rbworker response. */
            rc = WaitLatch(&workerInfo->latch,
                WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 
                rbCleanerPurgeIntervalMs);

            RbCleanerProcInterrupts(rc, id);

            /*
             * We raise an ERROR if rbworker not enter PURGE_MSG_STATUS_STEPIN as soon,
             * unexcepted error or hang in rbworker initialization always induce the state. 
             */
            if (!RbMsgStepIn(id) && RbStartWorkerTimeout(launchTime)) {
                RbMsgSetStatusErr(id, PURGE_MSG_STATUS_STEPDONE, 
                    ERRCODE_INTERNAL_ERROR, "rbworker took too long to start over 30 seconds.");
                break;
            }

            if (RbMsgStepDone(id)) {
                break;
            }
        }

        if (RbMsgStepRes(id)) {
            break;
        }
    }
    list_free_deep(l);
    l = NULL;
    CommitTransactionCommand();

    RbCleanerPurgeFinal(id);
}

static void RbCleanerPurge()
{
    PurgeMsgQueue *msqQue = RbGetQueue();
    while (!RbQueueIsEmpty(msqQue)) {
        uint64 id = RbQueueFront(msqQue)->id;
        PG_TRY();
        {
            RbCleanerPurgeImpl(id);
        }
        PG_CATCH();
        {
            RbMsgSetStatusErr(id, PURGE_MSG_STATUS_ERROR, 
                geterrcode(), Geterrmsg(), true);
            RbQueuePop(msqQue);
            PG_RE_THROW();
        }
        PG_END_TRY();
        RbQueuePop(msqQue);
    }
}

static void RbAutoPurge()
{
    PurgeMsg prMsg;

    RbMsgInit(&prMsg, PURGE_MSG_TYPE_AUTO, InvalidOid, InvalidOid);
    (void)RbCltSubmit(&prMsg, false);
}

NON_EXEC_STATIC void RbCleanerMain()
{
    sigjmp_buf localSigjmpBuf;
    bool lastError = false;
    TimestampTz nextTimestamp;

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = RBCLEANER;

    /* tell datasender we update the values. */
    pg_memory_barrier();

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "RbCleaner";

    /* Identify myself via ps */
    init_ps_display("rbcleaner process", "", "", "");

    /* save the pid & latch into share memory */
    pg_atomic_write_u64(&t_thrd.rbcleaner_cxt.RbCleanerShmem->rbCleanerPid, t_thrd.proc_cxt.MyProcPid);
    t_thrd.rbcleaner_cxt.RbCleanerShmem->rbCleanerLatch = &t_thrd.proc->procLatch;

    ereport(LOG, (errmsg("rbcleaner started")));

    if (u_sess->attr.attr_security.PostAuthDelay) {
        pg_usleep(u_sess->attr.attr_security.PostAuthDelay * 1000000L);
    }

    SetProcessingMode(InitProcessing);
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGHUP, RbSighupHandler);
    (void)gspqsignal(SIGINT, RbSigintHandler); /* Cancel signal */
    (void)gspqsignal(SIGTERM, RbSigtermHander);
    (void)gspqsignal(SIGQUIT, RbSigquitHandler);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, RbSigusr2Hander);
    (void)gspqsignal(SIGFPE,  FloatExceptionHandler);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    /* Early initialization */
    BaseInit();

#ifndef EXEC_BACKEND
    InitProcess();
#endif

    on_proc_exit(RbCleanerQuitAndClean, 0);

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser((char*)pstrdup(DEFAULT_DATABASE), InvalidOid, NULL); 
    t_thrd.proc_cxt.PostInit->InitRbCleaner();

    SetProcessingMode(NormalProcessing);
    pgstat_report_appname("RbCleaner");
    pgstat_report_activity(STATE_IDLE, NULL);

    MemoryContext workMxt = AllocSetContextCreate(
        t_thrd.top_mem_cxt,
        "RbCleaner",
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(workMxt);

    int curTryCounter = 0;
    int* oldTryCounter = NULL;
    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Forget any pending QueryCancel request */
        t_thrd.int_cxt.QueryCancelPending = false;
        (void)disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false;

        /* Report the error to the server log */
        EmitErrorReport();

        /*
         * Abort the current transaction in order to recover.
         */
        AbortCurrentTransaction();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        LWLockReleaseAll();

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        (void)MemoryContextSwitchTo(workMxt);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(workMxt);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /* if in shutdown mode, no need for anything further; just go away */
        if (t_thrd.rbcleaner_cxt.got_SIGTERM) {
            goto shutdown;
        }

        lastError = true;
    }

    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;

    /*
     * Unblock signals in case they were blocked during long jump.
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    nextTimestamp = GetCurrentTimestamp() + USECS_PER_MINUTE;

    /* loop until shutdown request */
    while (!t_thrd.rbcleaner_cxt.got_SIGTERM && ENABLE_TCAP_RECYCLEBIN) {
        int rc;

        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.proc->procLatch);

        /*
         * Wait until 3s naptime expires or we get some type of signal (all the
         * signal handlers will wake us by calling SetLatch).
         *
         * Note: If an error occurred during the last message processing, we will 
         *     wait less time 10ms to continue the next process message quickly.
         */
        rc = WaitLatch(&t_thrd.proc->procLatch,
            WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 
            lastError ? RBCLEANER_INTERVAL_QUICK_MS : RBCLEANER_INTERVAL_MS);
        lastError = false;

        RbCleanerProcInterrupts(rc);

        if (TcapFeatureAvail()) {
            /* Do the hard work. */
            RbCleanerPurge();

            /* Do auto purge. */
            TimestampTz now = GetCurrentTimestamp();
            if (now >= nextTimestamp || now + USECS_PER_MINUTE < nextTimestamp) {
                nextTimestamp = now + USECS_PER_MINUTE;
                RbAutoPurge();
            }
        }

        MemoryContextResetAndDeleteChildren(workMxt);
    }

shutdown:
    elog(LOG, "rbcleaner is shutting down.");

    proc_exit(0);
}

/********************************************************************
 *					  Recyclebin Cleaner WorkerMain Code
 ********************************************************************/

bool IsRbWorkerProcess(void)
{
    return t_thrd.role == RBWORKER;
}

static void RbWorkerSigquitHandler(SIGNAL_ARGS)
{    
    sigaddset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT); /* prevent nested calls */
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    on_exit_reset();

    gs_thread_exit(2);

    elog(LOG, "rbworker signaled: SIGQUIT");
}

static void RbWorkerQuitAndClean(int code, Datum arg)
{
    RbWorkerInfo *workerInfo = RbGetWorkerInfo();
    pg_atomic_write_u64(&workerInfo->rbworkerPid, 0);
}

static void RbWorkerPurge(int64 id)
{
    PurgeMsgType type = RbMsg(id)->req.type;

    switch (type) {
        case PURGE_MSG_TYPE_DML:
            TrPurgeTablespaceDML(id);
            break;
        case PURGE_MSG_TYPE_TABLESPACE:
        case PURGE_MSG_TYPE_CAS_TABLESPACE:
            TrPurgeTablespace(id);
            break;
        case PURGE_MSG_TYPE_RECYCLEBIN:
            TrPurgeRecyclebin(id);
            break;
        case PURGE_MSG_TYPE_CAS_SCHEMA:
            TrPurgeSchema(id);
            break;
        case PURGE_MSG_TYPE_CAS_USER:
            TrPurgeUser(id);
            break;
        case PURGE_MSG_TYPE_AUTO:
            TrPurgeAuto(id);
            break;
        default:
            elog(ERROR, "unknown purge message type: %d", type);
            break;
    }

    RbMsgSetStatus(id, PURGE_MSG_STATUS_STEPDONE, false);
    SetLatch(&t_thrd.rbcleaner_cxt.RbCleanerShmem->workerInfo.latch);
}

NON_EXEC_STATIC void RbWorkerMain()
{
    sigjmp_buf localSigjmpBuf;
    RbWorkerInfo *workInfo = RbGetWorkerInfo();
    pg_atomic_write_u64(&workInfo->rbworkerPid, t_thrd.proc_cxt.MyProcPid);

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = RBWORKER;

    /* tell datasender we update the values. */
    pg_memory_barrier();

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "RbWorker";

    /* Identify myself via ps */
    init_ps_display("rbworker process", "", "", "");

    ereport(DEBUG1, (errmsg("rbworker started")));

    SetProcessingMode(InitProcessing);
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGHUP,  SIG_IGN);
    (void)gspqsignal(SIGINT,  StatementCancelHandler);
    (void)gspqsignal(SIGTERM, die);
    (void)gspqsignal(SIGQUIT, RbWorkerSigquitHandler);
    (void)gspqsignal(SIGALRM, handle_sig_alarm);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGFPE,  FloatExceptionHandler);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    /* We allow SIGQUIT (quickdie) at all times */
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    /* Early initialization */
    BaseInit();

#ifndef EXEC_BACKEND
    InitProcess();
#endif

    on_proc_exit(RbWorkerQuitAndClean, 0);

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser((char*)pstrdup(NameStr(workInfo->dbName)), InvalidOid, NULL); 
    t_thrd.proc_cxt.PostInit->InitRbWorker();

    SetProcessingMode(NormalProcessing);
    pgstat_report_appname("RbWorker");
    pgstat_report_activity(STATE_IDLE, NULL);

    MemoryContext workMxt = AllocSetContextCreate(
        t_thrd.top_mem_cxt,
        "RbWorker",
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(workMxt);

    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* Forget any pending QueryCancel request */
        t_thrd.int_cxt.QueryCancelPending = false;
        (void)disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false;

        /* Report the error to the server log */
        EmitErrorReport();

        /*
         * Abort the current transaction in order to recover.
         */
        AbortCurrentTransaction();

        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        /* Report the error to rbcleaner */
        RbMsgSetStatusErr(workInfo->id, PURGE_MSG_STATUS_STEPDONE, 
            geterrcode(), Geterrmsg());
        SetLatch(&t_thrd.rbcleaner_cxt.RbCleanerShmem->workerInfo.latch);

        LWLockReleaseAll();

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        (void)MemoryContextSwitchTo(workMxt);
        FlushErrorState();

        /* Flush any leaked data in the top-level context */
        MemoryContextResetAndDeleteChildren(workMxt);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        /* just go away */
        goto shutdown;
    }

    RbMsgSetStatus(workInfo->id, PURGE_MSG_STATUS_STEPIN);

    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;

    /* Set lockwait_timeout/update_lockwait_timeout to 30s avoid unexpected suspend. */
    SetConfigOption("lockwait_timeout", "30s", PGC_SUSET, PGC_S_OVERRIDE);
    SetConfigOption("update_lockwait_timeout", "30s", PGC_SUSET, PGC_S_OVERRIDE);

    /* Do the hard work */
    RbWorkerPurge(workInfo->id);

    MemoryContextResetAndDeleteChildren(workMxt);

shutdown:
    elog(DEBUG1, "rbworker is shutting down.");

    proc_exit(0);
}

