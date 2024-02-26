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
 * datasyncrep.cpp
 *
 *
 * IDENTIFICATION
 *		 src/gausskernel/storage/replication/datasyncrep.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "replication/bcm.h"
#include "replication/catchup.h"
#include "replication/dataqueue.h"
#include "replication/datasyncrep.h"
#include "replication/datasender.h"
#include "replication/datasender_private.h"
#include "replication/walsender_private.h"
#include "replication/shared_storage_walreceiver.h"
#include "replication/ss_disaster_cluster.h"
#include "replication/syncrep.h"
#include "storage/cu.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "access/xlogutils.h"

static void DataSyncRepQueueInsert(void);
static void DataSyncRepCancelWait(void);
static int DataSyncRepWakeQueue(void);
static DataQueuePtr GetMinReplyOffset(void);
#ifdef USE_ASSERT_CHECKING
static bool SyncRepQueueIsOrderedByOffset(void);
#endif

void WaitForDataSync(void)
{
    int dataSyncRepState = SYNC_REP_WAITING;

    /*
     * Fast exit if user has not requested sync replication, or there are no
     * sync replication standby names defined. Note that those standbys don't
     * need to be connected.
     */
    bool isResetBcm = !u_sess->attr.attr_storage.enable_stream_replication || !SyncRepRequested() || !SyncStandbysDefined() ||
        (t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE);
    if (isResetBcm) {
        ResetBCMArray();
        return;
    }

    if (DataQueuePtrIsInvalid(t_thrd.proc->waitDataSyncPoint)) {
        ResetBCMArray();
        return;
    }
    if (!SHMQueueIsDetached(&(t_thrd.proc->dataSyncRepLinks))) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("shm queue should be detached")));
    }
    if (t_thrd.datasender_cxt.DataSndCtl == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("DataSndCtl should not be null")));
    }

    LWLockAcquire(DataSyncRepLock, LW_EXCLUSIVE);
    if (t_thrd.proc->dataSyncRepState != SYNC_REP_NOT_WAITING) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("dataSyncRepState should be SYNC_REP_NOT_WAITING")));
    }

    if (DQByteLE(t_thrd.proc->waitDataSyncPoint, t_thrd.datasender_cxt.DataSndCtl->queue_offset)) {
        LWLockRelease(DataSyncRepLock);
        ClearBCMArray();
        return;
    }

    /*
     * Set our wait offset so DataSender will know when to wake us, and add ourselves to the queue.
     */
    t_thrd.proc->dataSyncRepState = SYNC_REP_WAITING;
    DataSyncRepQueueInsert();
    LWLockRelease(DataSyncRepLock);

    ereport(DEBUG5, (errmsg("WaitForDataSync:head2:%u/%u, tail1:%u/%u,tail2:%u/%u,wait:%u/%u",
                            t_thrd.dataqueue_cxt.DataSenderQueue->use_head2.queueid,
                            t_thrd.dataqueue_cxt.DataSenderQueue->use_head2.queueoff,
                            t_thrd.dataqueue_cxt.DataSenderQueue->use_tail1.queueid,
                            t_thrd.dataqueue_cxt.DataSenderQueue->use_tail1.queueoff,
                            t_thrd.dataqueue_cxt.DataSenderQueue->use_tail2.queueid,
                            t_thrd.dataqueue_cxt.DataSenderQueue->use_tail2.queueoff,
                            t_thrd.proc->waitDataSyncPoint.queueid, t_thrd.proc->waitDataSyncPoint.queueoff)));
    WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_DATASYNC);

    /*
     * Wait for reply_offset to be confirmed.
     *
     * Each proc has its own wait latch, so we perform a normal latch
     * check/wait loop here.
     */
    for (;;) {
        /* Must reset the latch before testing state. */
        ResetLatch(&t_thrd.proc->procLatch);

        /*
         * Acquiring the lock is not needed, the latch ensures proper barriers.
         * If it looks like we're done, we must really be done, because once
         * walsender changes the state to SYNC_REP_WAIT_COMPLETE, it will never
         * update it again, so we can't be seeing a stale value in that case.
         */
        dataSyncRepState = t_thrd.proc->dataSyncRepState;

        if (dataSyncRepState == SYNC_REP_WAIT_COMPLETE) {
            if (u_sess->attr.attr_storage.HaModuleDebug) {
                ereport(LOG, (errmsg("HA-WaitForDataSync: waitpoint %u/%u done", t_thrd.proc->waitDataSyncPoint.queueid,
                                     t_thrd.proc->waitDataSyncPoint.queueoff)));
            }
            break;
        }

        /*
         * If a wait for synchronous replication is pending, we can neither
         * acknowledge the commit nor raise ERROR or FATAL.  The latter would
         * lead the client to believe that the transaction aborted, which
         * is not true: it's already committed locally. The former is no good
         * either: the client has requested synchronous replication, and is
         * entitled to assume that an acknowledged commit is also replicated,
         * which might not be true. So in this case we issue a WARNING (which
         * some clients may be able to interpret) and shut off further output.
         * We do NOT reset t_thrd.int_cxt.ProcDiePending, so that the process will die after
         * the commit is cleaned up.
         */
        if (t_thrd.int_cxt.ProcDiePending || t_thrd.proc_cxt.proc_exit_inprogress) {
            ereport(WARNING,
                    (errcode(ERRCODE_ADMIN_SHUTDOWN),
                     errmsg("canceling the wait for synchronous replication and terminating connection due to "
                            "administrator command"),
                     errdetail("The transaction has already committed locally, but might not have been replicated to "
                               "the standby.")));
            t_thrd.postgres_cxt.whereToSendOutput = DestNone;
            DataSyncRepCancelWait();
            break;
        }

        /*
         * It's unclear what to do if a query cancel interrupt arrives.  We
         * can't actually abort at this point, but ignoring the interrupt
         * altogether is not helpful, so we just terminate the wait with a
         * suitable warning.
         */
        if (t_thrd.int_cxt.QueryCancelPending) {
            t_thrd.int_cxt.QueryCancelPending = false;
            ereport(WARNING,
                    (errmsg("canceling wait for synchronous replication due to user request"),
                     errdetail("The transaction has already committed locally, but might not have been replicated to "
                               "the standby.")));
            DataSyncRepCancelWait();
            break;
        }

        /*
         * If the postmaster dies, we'll probably never get an
         * acknowledgement, because all the wal sender processes will exit. So
         * just bail out.
         */
        if (!PostmasterIsAlive()) {
            t_thrd.int_cxt.ProcDiePending = true;
            t_thrd.postgres_cxt.whereToSendOutput = DestNone;
            DataSyncRepCancelWait();
            break;
        }

        /*
         * if we  modify the syncmode dynamically, we'll stop wait
         */
        if ((t_thrd.walsender_cxt.WalSndCtl->sync_master_standalone && !(IS_SHARED_STORAGE_MODE || SS_DORADO_CLUSTER)) ||
            u_sess->attr.attr_storage.guc_synchronous_commit <= SYNCHRONOUS_COMMIT_LOCAL_FLUSH) {
            ereport(WARNING,
                    (errmsg("canceling wait for synchronous replication due to syncmaster standalone."),
                     errdetail("The transaction has already committed locally, but might not have been replicated to "
                               "the standby.")));
            DataSyncRepCancelWait();
            break;
        }

        /*
         * If the datasender to standby is offline, we'll stop wait.
         */
        if (IsCatchupProcess() && !DataSndInProgress(SNDROLE_PRIMARY_STANDBY)) {
            ereport(WARNING, (errmsg("catchup canceling wait for synchronous replication due to datasender offline")));
            DataSyncRepCancelWait();
            break;
        }

        /*
         * Wait on latch.  Any condition that should wake us up will set the
         * latch, so no need for timeout.
         */
        (void)WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT, 1000L);
    }

    (void)pgstat_report_waitstatus(oldStatus);

    /*
     * DataSender has checked our offset and has removed us from queue. Clean up
     * state and leave.  It's OK to reset these shared memory fields without
     * holding DataSyncRepLock, because any datasenders will ignore us anyway when
     * we're not on the queue. We need a read barrier to make sure we see
     * the changes to the queue link (this might be unnecessary without
     * assertions, but better safe than sorry).
     */
    pg_read_barrier();
    Assert(SHMQueueIsDetached(&(t_thrd.proc->dataSyncRepLinks)));
    t_thrd.proc->dataSyncRepState = SYNC_REP_NOT_WAITING;
    t_thrd.proc->waitDataSyncPoint.queueid = 0;
    t_thrd.proc->waitDataSyncPoint.queueoff = 0;

    /*
     * when the data has been send to standby, we should clear the excess bcm blocks.
     */
    if (dataSyncRepState == SYNC_REP_WAIT_COMPLETE)
        ClearBCMArray();

    /*
     * After the transaction is finished or cancelled, we should reset the BCMArray.
     * In case of the transaction is cancelled, and the table is dropped, then to clear
     * the BCM maybe encounter ERROR.
     */
    ResetBCMArray();
}

/*
 * Insert t_thrd.proc into the specified SyncRepQueue
 */
static void DataSyncRepQueueInsert(void)
{
    PGPROC *proc = NULL;

    proc = (PGPROC *)SHMQueuePrev(&(t_thrd.datasender_cxt.DataSndCtl->SyncRepQueue),
                                  &(t_thrd.datasender_cxt.DataSndCtl->SyncRepQueue),
                                  offsetof(PGPROC, dataSyncRepLinks));
    while (proc != NULL) {
        /*
         * Stop at the queue element that we should after to ensure the queue
         * is ordered by Position.
         */
        if (DQByteLT(proc->waitDataSyncPoint, t_thrd.proc->waitDataSyncPoint))
            break;

        proc = (PGPROC *)SHMQueuePrev(&(t_thrd.datasender_cxt.DataSndCtl->SyncRepQueue), &(proc->dataSyncRepLinks),
                                      offsetof(PGPROC, dataSyncRepLinks));
    }

    if (proc != NULL)
        SHMQueueInsertAfter(&(proc->dataSyncRepLinks), &(t_thrd.proc->dataSyncRepLinks));
    else
        SHMQueueInsertAfter(&(t_thrd.datasender_cxt.DataSndCtl->SyncRepQueue), &(t_thrd.proc->dataSyncRepLinks));
}

/*
 * Acquire DataSyncRepLock and cancel any wait currently in progress.
 */
static void DataSyncRepCancelWait(void)
{
    (void)LWLockAcquire(DataSyncRepLock, LW_EXCLUSIVE);
    if (!SHMQueueIsDetached(&(t_thrd.proc->dataSyncRepLinks)))
        SHMQueueDelete(&(t_thrd.proc->dataSyncRepLinks));
    t_thrd.proc->dataSyncRepState = SYNC_REP_NOT_WAITING;
    LWLockRelease(DataSyncRepLock);
}

/*
 * Update the offset on each queue based upon our latest state. This
 * implements a simple policy of first-valid-standby-releases-waiter.
 *
 * Other policies are possible, which would change what we do here and what
 * perhaps also which information we store as well.
 */
void DataSyncRepReleaseWaiters(void)
{
    volatile DataSndCtlData *datasndctl = t_thrd.datasender_cxt.DataSndCtl;
    DataQueuePtr minOffSet;
    int numreceive = 0;

    (void)LWLockAcquire(DataSyncRepLock, LW_EXCLUSIVE);

    minOffSet = GetMinReplyOffset();

    SpinLockAcquire(&datasndctl->mutex);
    if (DQByteLT(datasndctl->queue_offset, minOffSet)) {
        datasndctl->queue_offset.queueid = minOffSet.queueid;
        datasndctl->queue_offset.queueoff = minOffSet.queueoff;
        SpinLockRelease(&datasndctl->mutex);

        PopFromDataQueue(((DataSndCtlData *)datasndctl)->queue_offset, t_thrd.dataqueue_cxt.DataSenderQueue);

        numreceive = DataSyncRepWakeQueue();
    } else
        SpinLockRelease(&datasndctl->mutex);

    LWLockRelease(DataSyncRepLock);

    ereport(DEBUG3, (errmsg("released %d procs up to receive %u/%u", numreceive,
                            t_thrd.datasender_cxt.MyDataSnd->receivePosition.queueid,
                            t_thrd.datasender_cxt.MyDataSnd->receivePosition.queueoff)));
}

/*
 * search the slow_offset last so that we can release up the queue;
 */
static DataQueuePtr GetMinReplyOffset(void)
{
    volatile DataSndCtlData *datasndctl = t_thrd.datasender_cxt.DataSndCtl;
    DataQueuePtr slow = (DataQueuePtr){ 0, 0 };
    int i;
    bool sender_has_invaild_position = false;

    for (i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        volatile DataSnd *datasnd = &datasndctl->datasnds[i];

        SpinLockAcquire(&datasnd->mutex);

        if (datasnd->pid != 0 && datasnd->state > DATASNDSTATE_STARTUP && datasnd->sending) {
            /*
             * To find the minimum from the standby sender and the secondery
             * sender. If datesnd1 receivePosition is 0, datesnd2 receivePosition
             * is 500, the minimum should be 0;
             */
            if (DataQueuePtrIsInvalid(datasnd->receivePosition))
                sender_has_invaild_position = true;

            if (DataQueuePtrIsInvalid(slow) || DQByteLT(datasnd->receivePosition, slow)) {
                slow.queueid = datasnd->receivePosition.queueid;
                slow.queueoff = datasnd->receivePosition.queueoff;
            }
        }

        SpinLockRelease(&datasnd->mutex);
    }

    if (sender_has_invaild_position)
        slow = (DataQueuePtr){ 0, 0 };

    return slow;
}

/*
 * Walk the specified queue from head. Set the state of any backends that
 * need to be woken, remove them from the queue, and then wake them.
 *
 * Must hold DataSyncRepLock.
 */
static int DataSyncRepWakeQueue(void)
{
    volatile DataSndCtlData *datasndctl = t_thrd.datasender_cxt.DataSndCtl;
    PGPROC *proc = NULL;
    PGPROC *thisproc = NULL;
    int numprocs = 0;

    Assert(SyncRepQueueIsOrderedByOffset());

    proc = (PGPROC *)SHMQueueNext(&(t_thrd.datasender_cxt.DataSndCtl->SyncRepQueue),
                                  &(t_thrd.datasender_cxt.DataSndCtl->SyncRepQueue),
                                  offsetof(PGPROC, dataSyncRepLinks));

    while (proc != NULL) {
        /*
         * Assume the queue is ordered by offset
         */
        if (DQByteLT(datasndctl->queue_offset, proc->waitDataSyncPoint))
            return numprocs;

        /*
         * Move to next proc, so we can delete thisproc from the queue.
         * thisproc is valid, proc may be NULL after this.
         */
        thisproc = proc;
        proc = (PGPROC *)SHMQueueNext(&(t_thrd.datasender_cxt.DataSndCtl->SyncRepQueue), &(proc->dataSyncRepLinks),
                                      offsetof(PGPROC, dataSyncRepLinks));
        /*
         * Remove thisproc from queue.
         */
        SHMQueueDelete(&(thisproc->dataSyncRepLinks));

        /*
         * WaitForDataSync() reads dataSyncRepState without holding the lock, so
         * make sure that it sees the queue link being removed before the
         * dataSyncRepState change.
         */
        pg_write_barrier();

        /*
         * Set state to complete; see WaitForDataSync() for discussion of
         * the various states.
         */
        thisproc->dataSyncRepState = SYNC_REP_WAIT_COMPLETE;

        /*
         * Wake only when we have set state and removed from queue.
         */
        SetLatch(&(thisproc->procLatch));

        numprocs++;
    }

    return numprocs;
}

#ifdef USE_ASSERT_CHECKING
static bool SyncRepQueueIsOrderedByOffset(void)
{
    PGPROC *proc = NULL;
    DataQueuePtr queueptr;

    queueptr.queueid = 0;
    queueptr.queueoff = 0;

    proc = (PGPROC *)SHMQueueNext(&(t_thrd.datasender_cxt.DataSndCtl->SyncRepQueue),
                                  &(t_thrd.datasender_cxt.DataSndCtl->SyncRepQueue),
                                  offsetof(PGPROC, dataSyncRepLinks));

    while (proc != NULL) {
        /*
         * Check the queue is ordered by offset
         */
        if (DQByteLT(proc->waitDataSyncPoint, queueptr))
            return false;

        queueptr = proc->waitDataSyncPoint;

        proc = (PGPROC *)SHMQueueNext(&(t_thrd.datasender_cxt.DataSndCtl->SyncRepQueue), &(proc->dataSyncRepLinks),
                                      offsetof(PGPROC, dataSyncRepLinks));
    }

    return true;
}
#endif
