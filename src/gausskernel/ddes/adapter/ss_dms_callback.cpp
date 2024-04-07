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
 * ---------------------------------------------------------------------------------------
 *
 * ss_dms_callback.cpp
 *        Provide callback interface for called inside DMS API
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_dms_callback.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "ddes/dms/ss_dms_callback.h"
#include "postgres.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/resowner.h"
#include "utils/postinit.h"
#include "storage/procarray.h"
#include "access/xact.h"
#include "access/transam.h"
#include "access/csnlog.h"
#include "access/xlog.h"
#include "access/multi_redo_api.h"
#include "ddes/dms/ss_dms_bufmgr.h"
#include "storage/buf/buf_internals.h"
#include "ddes/dms/ss_transaction.h"
#include "storage/smgr/segment.h"
#include "storage/sinvaladt.h"
#include "replication/walsender_private.h"
#include "replication/walreceiver.h"
#include "replication/ss_disaster_cluster.h"
#include "ddes/dms/ss_switchover.h"
#include "ddes/dms/ss_reform_common.h"
#include "ddes/dms/ss_dms_bufmgr.h"
#include "storage/file/fio_device.h"
#include "storage/buf/bufmgr.h"
#include "storage/buf/buf_internals.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"

/*
 * Wake up startup process to replay WAL, or to notice that
 * failover has been requested.
 */
void SSWakeupRecovery(void)
{
    uint32 thread_num = (uint32)g_instance.ckpt_cxt_ctl->pgwr_procs.num;
    /* need make sure pagewriter started first */
    bool need_recovery = true;

    if (SS_DISASTER_MAIN_STANDBY_NODE) {
        g_instance.dms_cxt.SSRecoveryInfo.recovery_pause_flag = false;
        return;
    }

    while (pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) != thread_num) {
        if (!RecoveryInProgress()) {
            need_recovery = false;
            break;
        }
        pg_usleep(REFORM_WAIT_TIME);
    }

    if (need_recovery) {
        g_instance.dms_cxt.SSRecoveryInfo.recovery_pause_flag = false;
    }
}

static int CBGetUpdateXid(void *db_handle, unsigned long long xid, unsigned int t_infomask, unsigned int t_infomask2,
    unsigned long long *uxid)
{
    if (!SSCanFetchLocalSnapshotTxnRelatedInfo()) {
        return DMS_ERROR;
    }

    int result = DMS_SUCCESS;
    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;

    PG_TRY();
    {
        *uxid =
            (unsigned long long)MultiXactIdGetUpdateXid((TransactionId)xid, (uint16)t_infomask, (uint16)t_infomask2);
    }
    PG_CATCH();
    {
        result = DMS_ERROR;
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        if (t_thrd.role == DMS_WORKER) {
            FlushErrorState();
        }
    }
    PG_END_TRY();
    return result;
}

static CommitSeqNo TransactionWaitCommittingCSN(dms_opengauss_xid_csn_t *xid_csn_ctx, bool *sync)
{
    bool looped = false;
    bool isCommit = (bool)xid_csn_ctx->is_committed;
    bool isMvcc = (bool)xid_csn_ctx->is_mvcc;
    bool isNest = (bool)xid_csn_ctx->is_nest;
    TransactionId xid = xid_csn_ctx->xid;
    CommitSeqNo snapshotcsn = xid_csn_ctx->snapshotcsn;
    TransactionId parentXid = InvalidTransactionId;
    SnapshotData snapshot = {SNAPSHOT_MVCC};
    snapshot.xmin = xid_csn_ctx->snapshotxmin;
    snapshot.snapshotcsn = snapshotcsn;
    CommitSeqNo csn = TransactionIdGetCommitSeqNo(xid, isCommit, isMvcc, isNest, &snapshot);

    while (COMMITSEQNO_IS_COMMITTING(csn)) {
        if (looped && isCommit) {
            ereport(DEBUG1,
                (errmodule(MOD_DMS), errmsg("committed SS xid %lu's csn %lu"
                    "is changed to FROZEN after lockwait.", xid, csn)));
            CSNLogSetCommitSeqNo(xid, 0, NULL, COMMITSEQNO_FROZEN);
            SetLatestFetchState(xid, COMMITSEQNO_FROZEN);
            /* in this case, SS tuple is visible on standby, as we already compared and waited */
            return COMMITSEQNO_FROZEN;
        } else if (looped && !isCommit) {
            ereport(DEBUG1, (errmodule(MOD_DMS),
                errmsg("SS XID %lu's csn %lu is changed to ABORT after lockwait.", xid, csn)));
            /* recheck if transaction id is finished */
            RecheckXidFinish(xid, csn);
            CSNLogSetCommitSeqNo(xid, 0, NULL, COMMITSEQNO_ABORTED);
            SetLatestFetchState(xid, COMMITSEQNO_ABORTED);
            /* in this case, SS tuple is not visible on standby */
            return COMMITSEQNO_ABORTED;
        } else {
            if (!COMMITSEQNO_IS_SUBTRANS(csn)) {
                /* If snapshotcsn lower than csn stored in csn log, don't need to wait. */
                CommitSeqNo latestCSN = GET_COMMITSEQNO(csn);
                if (latestCSN >= snapshotcsn) {
                    ereport(DEBUG1,
                        (errmodule(MOD_DMS), errmsg(
                            "snapshotcsn %lu < csn %lu stored in CSNLog, TXN invisible, no need to sync wait, XID %lu",
                            snapshotcsn,
                            latestCSN,
                            xid)));
                    /* in this case, SS tuple is not visible; to return ABORT is inappropriate, so let standby judge */
                    return latestCSN;
                }
            } else {
                parentXid = (TransactionId)GET_PARENTXID(csn);
            }

            if (u_sess->attr.attr_common.xc_maintenance_mode || t_thrd.xact_cxt.bInAbortTransaction) {
                return COMMITSEQNO_ABORTED;
            }

            // standby does not need buf lock or validation
            if (TransactionIdIsValid(parentXid)) {
                SyncLocalXidWait(parentXid);
            } else {
                SyncLocalXidWait(xid);
            }

            looped = true;
            *sync = true;
            parentXid = InvalidTransactionId;
            csn = TransactionIdGetCommitSeqNo(xid, isCommit, isMvcc, isNest, &snapshot);
        }
    }
    return csn;
}

static int CBGetTxnCSN(void *db_handle, dms_opengauss_xid_csn_t *csn_req, dms_opengauss_csn_result_t *csn_res)
{
    if (!SSCanFetchLocalSnapshotTxnRelatedInfo()) {
        return DMS_ERROR;
    }

    int ret;
	uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    PG_TRY();
    {
        bool sync = false;
        CLogXidStatus clogstatus = CLOG_XID_STATUS_IN_PROGRESS;
        XLogRecPtr lsn = InvalidXLogRecPtr;
        CommitSeqNo csn = TransactionWaitCommittingCSN(csn_req, &sync);
        clogstatus = CLogGetStatus(csn_req->xid, &lsn);
        csn_res->csn = csn;
        csn_res->sync = (unsigned char)sync;
        csn_res->clogstatus = (unsigned int)clogstatus;
        csn_res->lsn = lsn;
        ret = DMS_SUCCESS;
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        if (t_thrd.role == DMS_WORKER) {
            FlushErrorState();
        }
        ret = DMS_ERROR;
    }
    PG_END_TRY();
    return ret;
}

static int CBGetSnapshotData(void *db_handle, dms_opengauss_txn_snapshot_t *txn_snapshot, uint8 inst_id)
{   
    /* SS_MAIN_STANDBY_NODE always is in recovery progress, but it can acquire snapshot*/
    if (RecoveryInProgress() && !(SS_NORMAL_PRIMARY && SS_DISASTER_MAIN_STANDBY_NODE)) {
        return DMS_ERROR;
    }

    if (!SSCanFetchLocalSnapshotTxnRelatedInfo()) {
        return DMS_ERROR;
    }

    if (!SSCanFetchLocalSnapshotTxnRelatedInfo()) {
        return DMS_ERROR;
    }

    int retCode = DMS_ERROR;
    SnapshotData snapshot = {SNAPSHOT_MVCC};
    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;

    PG_TRY();
    {
        (void)GetSnapshotData(&snapshot, false);
        if (snapshot.xmin != InvalidTransactionId) {
            txn_snapshot->xmin = snapshot.xmin;
            txn_snapshot->xmax = snapshot.xmax;
            txn_snapshot->snapshotcsn = snapshot.snapshotcsn;
            txn_snapshot->localxmin = u_sess->utils_cxt.RecentGlobalXmin;
            if (RecordSnapshotBeforeSend(inst_id, txn_snapshot->xmin)) {
                retCode = DMS_SUCCESS;
            } else {
                retCode = DMS_ERROR;
            }
        }
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        if (t_thrd.role == DMS_WORKER) {
            FlushErrorState();
        }
    }
    PG_END_TRY();

    return retCode;
}

static int CBGetTxnSwinfo(void *db_handle, dms_opengauss_txn_sw_info_t *txn_swinfo)
{
    if (RecoveryInProgress()) {
        return DMS_ERROR;
    }

    if (!SSCanFetchLocalSnapshotTxnRelatedInfo()) {
        return DMS_ERROR;
    }

    int retCode = DMS_SUCCESS;
    uint32 slot = txn_swinfo->server_proc_slot;
    PGXACT* pgxact = &g_instance.proc_base_all_xacts[slot];
    if (g_instance.proc_base_all_procs[slot] == NULL) {
        retCode = DMS_ERROR;
    } else {
        txn_swinfo->sxid = pgxact->xid;
        txn_swinfo->scid = pgxact->cid;
    }

    return retCode;
}

static int CBGetTxnStatus(void *db_handle, unsigned long long xid, unsigned char type, unsigned char *result)
{
    if (!SSCanFetchLocalSnapshotTxnRelatedInfo()) {
        return DMS_ERROR;
    }

    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    PG_TRY();
    {
        switch (type) {
            case XID_INPROGRESS:
                *result = (unsigned char)TransactionIdIsInProgress(xid);
                break;
            case XID_COMMITTED:
                *result = (unsigned char)TransactionIdDidCommit(xid);
                break;
            default:
                PG_TRY_RETURN(DMS_ERROR);
        }
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        if (t_thrd.role == DMS_WORKER) {
            FlushErrorState();
        }
    }
    PG_END_TRY();
    return DMS_SUCCESS;
}

#define NDPGETBYTE(x, i) (*((char*)(x) + (int)((i) / BITS_PER_BYTE)))
#define NDPCLRBIT(x, i) NDPGETBYTE(x, i) &= ~(0x01 << ((i) % BITS_PER_BYTE))
#define NDPGETBIT(x, i) ((NDPGETBYTE(x, i) >> ((i) % BITS_PER_BYTE)) & 0x01)

static int CBGetPageStatus(void *db_handle, dms_opengauss_relfilenode_t *rnode, unsigned int page,
    int pagesNum, dms_opengauss_page_status_result_t *page_result)
{
    for (uint32 i = page, offset = 0; i != page + pagesNum; ++i, ++offset) {
        if (NDPGETBIT(page_result->page_map, offset)) {
            bool cached = IsPageHitBufferPool(*(RelFileNode * )(rnode), MAIN_FORKNUM, i);
            if (cached) {
                NDPCLRBIT(page_result->page_map, offset);
                --page_result->bit_count;
            }
        }
    }
    return DMS_SUCCESS;
}

static int CBGetCurrModeAndLockBuffer(void *db_handle, int buffer, unsigned char lock_mode,
    unsigned char *curr_mode)
{
    Assert((buffer - 1) >= 0);
    BufferDesc *bufHdr = GetBufferDescriptor(buffer - 1);
    *curr_mode = (unsigned char)GetHeldLWLockMode(bufHdr->content_lock); // LWLockMode
    Assert(*curr_mode == LW_EXCLUSIVE || *curr_mode == LW_SHARED);
    LockBuffer((Buffer)buffer, lock_mode); // BUFFER_LOCK_UNLOCK, BUFFER_LOCK_SHARE or BUFFER_LOCK_EXCLUSIVE
    ereport(LOG, (errmodule(MOD_DMS),
        errmsg("SS lock buf success, buffer=%d, mode=%hhu, curr_mode=%hhu", buffer, lock_mode, *curr_mode)));
    return DMS_SUCCESS;
}

static inline void SSResetDemoteReqType(void)
{
    SpinLockAcquire(&t_thrd.walsender_cxt.WalSndCtl->mutex);
    t_thrd.walsender_cxt.WalSndCtl->demotion = NoDemote;
    SpinLockRelease(&t_thrd.walsender_cxt.WalSndCtl->mutex);
}

static void SSHandleReformFailDuringDemote(DemoteMode demote_mode)
{
    ereport(WARNING,
        (errmodule(MOD_DMS),
            errmsg("[SS switchover] Failure in %s primary demote, pmState=%d, need reform rcy.",
                DemoteModeDesc(demote_mode), pmState)));

    /*
     * Shutdown checkpoint would cause concurrency as DMS is starting next round of reform.
     * If we allow ckpt to finish and recover, DMS would not be aware of the recovery process.
     * Therefore we flush as many dirty pages as we can, then trigger a DMS normal reform.
     */
    if (CheckpointInProgress() || pmState >= PM_SHUTDOWN) {
        ereport(WARNING,
            (errmodule(MOD_DMS),
                errmsg("[SS switchover DFX] reform failed after shutdown ckpt has started, exit now")));
        _exit(0);
    }

    /* backends exiting, simply rollback */
    pmState = PM_RUN;
    g_instance.demotion = NoDemote;
    SSResetDemoteReqType();
}

static int CBSwitchoverDemote(void *db_handle)
{
    DemoteMode demote_mode = FastDemote;

    /* borrows walsender lock */
    SpinLockAcquire(&t_thrd.walsender_cxt.WalSndCtl->mutex);
    if (t_thrd.walsender_cxt.WalSndCtl->demotion > NoDemote) {
        SpinLockRelease(&t_thrd.walsender_cxt.WalSndCtl->mutex);
        ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS switchover] master is doing switchover,"
            " probably standby already requested switchover.")));
        return DMS_SUCCESS;
    }
    Assert(g_instance.dms_cxt.SSClusterState == NODESTATE_NORMAL);
    Assert(SS_OFFICIAL_PRIMARY);

    t_thrd.walsender_cxt.WalSndCtl->demotion = demote_mode;
    g_instance.dms_cxt.SSClusterState = NODESTATE_PRIMARY_DEMOTING;
    g_instance.dms_cxt.SSRecoveryInfo.new_primary_reset_walbuf_flag = true;
    SpinLockRelease(&t_thrd.walsender_cxt.WalSndCtl->mutex);

    ereport(LOG,
        (errmodule(MOD_DMS), errmsg("[SS switchover] Recv %s demote request from DMS reformer.",
            DemoteModeDesc(demote_mode))));

    SendPostmasterSignal(PMSIGNAL_DEMOTE_PRIMARY);

    const int WAIT_DEMOTE = 6000;  /* wait up to 10 min in case of too many dirty pages to be flushed */
    for (int ntries = 0;; ntries++) {
        if (pmState == PM_RUN && g_instance.dms_cxt.SSClusterState == NODESTATE_PROMOTE_APPROVE) {
            SSResetDemoteReqType();
            ereport(LOG,
                (errmodule(MOD_DMS), errmsg("[SS switchover] Success in %s primary demote, running as standby,"
                    " waiting for reformer setting new role.", DemoteModeDesc(demote_mode))));
            return DMS_SUCCESS;
        } else {
            if (ntries >= WAIT_DEMOTE || dms_reform_failed()) {
                SSHandleReformFailDuringDemote(demote_mode);
                return DMS_ERROR;
            }
            ntries = 0;
        }

        CHECK_FOR_INTERRUPTS();
        pg_usleep(100000L); /* wait 0.1 sec, then retry */
    }
    return DMS_ERROR;
}

static int CBSwitchoverPromote(void *db_handle, unsigned char origPrimaryId)
{
    g_instance.dms_cxt.SSClusterState = NODESTATE_STANDBY_PROMOTING;
    g_instance.dms_cxt.SSRecoveryInfo.new_primary_reset_walbuf_flag = true;
    /* allow recovery in switchover to keep LSN in order */
    t_thrd.shemem_ptr_cxt.XLogCtl->IsRecoveryDone = false;
    t_thrd.shemem_ptr_cxt.XLogCtl->SharedRecoveryInProgress = true;
    t_thrd.shemem_ptr_cxt.ControlFile->state = DB_IN_CRASH_RECOVERY;
    pg_memory_barrier();
    ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS switchover] Starting to promote standby.")));

    SSNotifySwitchoverPromote();

    const int WAIT_PROMOTE = 1200;  /* wait 120 sec */
    for (int ntries = 0;; ntries++) {
        if (g_instance.dms_cxt.SSClusterState == NODESTATE_STANDBY_PROMOTED) {
            /* flush control file primary id in advance to save new standby's waiting time */
            SSSavePrimaryInstId(SS_MY_INST_ID);
            ereport(LOG, (errmodule(MOD_DMS),
                errmsg("[SS switchover] Standby promote: success, set new primary:%d.", SS_MY_INST_ID)));
            return DMS_SUCCESS;
        } else {
            if (ntries >= WAIT_PROMOTE || dms_reform_failed()) {
                ereport(WARNING, (errmodule(MOD_DMS),
                    errmsg("[SS switchover] Standby promote timeout, please try again later.")));
                return DMS_ERROR;
            }
            ntries = 0;
        }

        CHECK_FOR_INTERRUPTS();
        pg_usleep(100000L); /* wait 0.1 sec, then retry */
    }
    return DMS_ERROR;
}

/* only sets switchover errno, everything else set in setPrimaryId */
static void CBSwitchoverResult(void *db_handle, int result)
{
    if (result == DMS_SUCCESS) {
        ereport(LOG, (errmodule(MOD_DMS),
            errmsg("[SS switchover] Switchover success, letting reformer update roles.")));
        return;
    } else {
        /* abort and restore state */
        g_instance.dms_cxt.SSClusterState = NODESTATE_NORMAL;
        if (SS_DISASTER_STANDBY_CLUSTER) {
            g_instance.dms_cxt.SSReformInfo.in_reform = false;
        }
        ereport(WARNING, (errmodule(MOD_DMS), errmsg("[SS switchover] Switchover failed, errno: %d.", result)));
    }
}

static int SetPrimaryIdOnStandby(int primary_id)
{
    for (int ntries = 0;; ntries++) {
        SSReadControlFile(REFORM_CTRL_PAGE); /* need to double check */
        if (g_instance.dms_cxt.SSReformerControl.primaryInstId == primary_id) {
            ereport(LOG, (errmodule(MOD_DMS),
                errmsg("[SS %s] Reform success, this is a standby:%d confirming new primary:%d, confirm ntries=%d.",
                    SS_PERFORMING_SWITCHOVER ? "switchover" : "reform", SS_MY_INST_ID, primary_id, ntries)));
            return DMS_SUCCESS;
        } else {
            if (dms_reform_failed()) {
                ereport(ERROR,
                    (errmodule(MOD_DMS), errmsg("[SS %s] Failed to confirm new primary: %d,"
                        " control file indicates primary is %d; dms reform failed.",
                        SS_PERFORMING_SWITCHOVER ? "switchover" : "reform", (int)primary_id,
                        g_instance.dms_cxt.SSReformerControl.primaryInstId)));
                return DMS_ERROR;
            }
            if (ntries >= WAIT_REFORM_CTRL_REFRESH_TRIES) {
                ereport(ERROR,
                    (errmodule(MOD_DMS), errmsg("[SS %s] Failed to confirm new primary: %d,"
                        " control file indicates primary is %d; wait timeout.",
                        SS_PERFORMING_SWITCHOVER ? "switchover" : "reform", (int)primary_id,
                        g_instance.dms_cxt.SSReformerControl.primaryInstId)));
                return DMS_ERROR;
            }
        }

        CHECK_FOR_INTERRUPTS();
        pg_usleep(REFORM_WAIT_TIME); /* wait 0.01 sec, then retry */
    }

    return DMS_ERROR;
}

/* called on both new primary and all standby nodes to refresh status */
static int CBSaveStableList(void *db_handle, unsigned long long list_stable, unsigned char reformer_id,
                            unsigned long long list_in, unsigned int save_ctrl)
{
    int primary_id = (int)reformer_id;
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    g_instance.dms_cxt.SSReformerControl.primaryInstId = primary_id;
    g_instance.dms_cxt.SSReformerControl.list_stable = list_stable;
    int ret = DMS_ERROR;
    SSLockReleaseAll();
    SSSyncOldestXminWhenReform(reformer_id);

    if ((int)primary_id == SS_MY_INST_ID) {
        if (g_instance.dms_cxt.SSClusterState > NODESTATE_NORMAL) {
            Assert(g_instance.dms_cxt.SSClusterState == NODESTATE_STANDBY_PROMOTED ||
                g_instance.dms_cxt.SSClusterState == NODESTATE_STANDBY_FAILOVER_PROMOTING);
        }
        SSUpdateReformerCtrl();
        LWLockRelease(ControlFileLock);
        Assert(g_instance.dms_cxt.SSReformerControl.primaryInstId == (int)primary_id);
        ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS %s] set current instance:%d as primary.",
            SS_PERFORMING_SWITCHOVER ? "switchover" : "reform", primary_id)));
        ret = DMS_SUCCESS;
    } else { /* we are on standby */
        LWLockRelease(ControlFileLock);
        ret = SetPrimaryIdOnStandby(primary_id);
    }
    return ret;
}

static void ReleaseResource()
{
    LWLockReleaseAll();
    AbortBufferIO();
    UnlockBuffers();
    /* buffer pins are released here: */
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
    FlushErrorState();
}

static unsigned int CBIncAndGetSrsn(uint32 sessid)
{
    return ++t_thrd.dms_cxt.srsn;
}

static unsigned int CBPageHashCode(const char pageid[DMS_PAGEID_SIZE])
{
    BufferTag *tag = (BufferTag *)pageid;
    return BufTableHashCode(tag);
}

static unsigned long long CBGetPageLSN(const dms_buf_ctrl_t *buf_ctrl)
{
    Assert(buf_ctrl->buf_id < TOTAL_BUFFER_NUM);
    if (buf_ctrl->buf_id >= TOTAL_BUFFER_NUM) {
        return 0;
    }
    BufferDesc* buf_desc = GetBufferDescriptor(buf_ctrl->buf_id);
    XLogRecPtr lsn = BufferGetLSN(buf_desc);
    return lsn;
}

static unsigned long long CBGetGlobalLSN(void *db_handle)
{
    return GetInsertRecPtr();
}

static int tryEnterLocalPage(BufferTag *tag, dms_lock_mode_t mode, dms_buf_ctrl_t **buf_ctrl)
{
    bool is_seg;
    int ret = DMS_SUCCESS;
    int buf_id = -1;
    uint32 hash;
    BufferDesc *buf_desc = NULL;
    RelFileNode relfilenode = tag->rnode;
    bool get_lock = false;

#ifdef USE_ASSERT_CHECKING
    if (IsSegmentPhysicalRelNode(relfilenode)) {
        SegSpace *spc = spc_open(relfilenode.spcNode, relfilenode.dbNode, false, false);
        BlockNumber spc_nblocks = spc_size(spc, relfilenode.relNode, tag->forkNum);
        if (tag->blockNum >= spc_nblocks) {
            ereport(PANIC, (errmodule(MOD_DMS),
                errmsg("unexpected blocknum %u >= spc nblocks %u", tag->blockNum, spc_nblocks)));
        }
    }
#endif

    *buf_ctrl = NULL;
    hash = BufTableHashCode(tag);

    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    PG_TRY();
    {
        do {
            buf_id = BufTableLookup(tag, hash);
            if (buf_id < 0) {
                break;
            }

            buf_desc = GetBufferDescriptor(buf_id);
            if (IsSegmentBufferID(buf_id)) {
                (void)SegPinBuffer(buf_desc);
                is_seg = true;
            } else {
                ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
                (void)PinBuffer(buf_desc, NULL);
                is_seg = false;
            }

            if (!BUFFERTAGS_PTR_EQUAL(&buf_desc->tag, tag)) {
                DmsReleaseBuffer(buf_desc->buf_id + 1, is_seg);
                break;
            }

            bool wait_success = SSWaitIOTimeout(buf_desc);
            if (!wait_success) {
                DmsReleaseBuffer(buf_desc->buf_id + 1, is_seg);
                ret = GS_TIMEOUT;
                break;
            }

            if (!(pg_atomic_read_u64(&buf_desc->state) & BM_VALID)) {
                ereport(WARNING, (errmodule(MOD_DMS),
                    errmsg("[%d/%d/%d/%d %d-%d] try enter page failed, buffer is not valid, state = 0x%x",
                    tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
                    tag->forkNum, tag->blockNum, buf_desc->state)));
                DmsReleaseBuffer(buf_desc->buf_id + 1, is_seg);
                *buf_ctrl = NULL;
                ret = DMS_SUCCESS;
                break;
            }

            if (pg_atomic_read_u64(&buf_desc->state) & BM_IO_ERROR) {
                ereport(WARNING, (errmodule(MOD_DMS),
                    errmsg("[%d/%d/%d/%d %d-%d] try enter page failed, buffer is io error, state = 0x%x",
                    tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
                    tag->forkNum, tag->blockNum, buf_desc->state)));
                DmsReleaseBuffer(buf_desc->buf_id + 1, is_seg);
                *buf_ctrl = NULL;
                ret = DMS_SUCCESS;
                break;
            }

            LWLockMode content_mode = (mode == DMS_LOCK_SHARE) ? LW_SHARED : LW_EXCLUSIVE;
            get_lock = SSLWLockAcquireTimeout(buf_desc->content_lock, content_mode);
            if (!get_lock) {
                DmsReleaseBuffer(buf_desc->buf_id + 1, is_seg);
                ret = GS_TIMEOUT;
                ereport(WARNING, (errmodule(MOD_DMS), (errmsg("[SS lwlock][%u/%u/%u/%d %d-%u] request LWLock timeout, "
                    "buf_id:%d, lwlock:%p",
                    tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
                    tag->forkNum, tag->blockNum, buf_id, buf_desc->content_lock))));
                break;
            }
            *buf_ctrl = GetDmsBufCtrl(buf_id);
            Assert(buf_id >= 0);
            if ((*buf_ctrl)->been_loaded == false) {
                *buf_ctrl = NULL;
                LWLockRelease(buf_desc->content_lock);
                DmsReleaseBuffer(buf_desc->buf_id + 1, is_seg);
                ereport(WARNING, (errmodule(MOD_DMS),
                    errmsg("[%u/%u/%u/%d %d-%u] been_loaded marked false, page swapped out and failed to load",
                    tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
                    tag->forkNum, tag->blockNum)));
                break;
            }
            if ((*buf_ctrl)->lock_mode == DMS_LOCK_NULL) {
                ereport(WARNING, (errmodule(MOD_DMS),
                    errmsg("[%u/%u/%u/%d %d-%u] lock mode is null, still need to transfer page",
                    tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
                    tag->forkNum, tag->blockNum)));
            } else if (buf_desc->extra->seg_fileno != EXTENT_INVALID) {
                (*buf_ctrl)->seg_fileno = buf_desc->extra->seg_fileno;
                (*buf_ctrl)->seg_blockno = buf_desc->extra->seg_blockno;
            }
        } while (0);
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        ReleaseResource();
        ret = DMS_ERROR;
    }
    PG_END_TRY();

    return ret;
}

static int CBEnterLocalPage(void *db_handle, char pageid[DMS_PAGEID_SIZE], dms_lock_mode_t mode,
    dms_buf_ctrl_t **buf_ctrl)
{
    BufferTag *tag = (BufferTag *)pageid;
    return tryEnterLocalPage(tag, mode, buf_ctrl);
}

static unsigned char CBPageDirty(dms_buf_ctrl_t *buf_ctrl)
{
    Assert(buf_ctrl->buf_id < TOTAL_BUFFER_NUM);
    if (buf_ctrl->buf_id >= TOTAL_BUFFER_NUM) {
        return 0;
    }
    BufferDesc *buf_desc = GetBufferDescriptor(buf_ctrl->buf_id);
    bool is_dirty = (pg_atomic_read_u64(&buf_desc->state) & (BM_DIRTY | BM_JUST_DIRTIED)) > 0;
    return (unsigned char)is_dirty;
}

static void CBLeaveLocalPage(void *db_handle, dms_buf_ctrl_t *buf_ctrl)
{
    Assert(buf_ctrl->buf_id < TOTAL_BUFFER_NUM);
    if (buf_ctrl->buf_id >= TOTAL_BUFFER_NUM) {
        return;
    }

    if (IsSegmentBufferID(buf_ctrl->buf_id)) {
        SegUnlockReleaseBuffer(buf_ctrl->buf_id + 1);
    } else {
        UnlockReleaseBuffer(buf_ctrl->buf_id + 1);
    }
}

static char* CBGetPage(dms_buf_ctrl_t *buf_ctrl)
{
    Assert(buf_ctrl->buf_id < TOTAL_BUFFER_NUM);
    if (buf_ctrl->buf_id >= TOTAL_BUFFER_NUM) {
        return NULL;
    }
    BufferDesc *buf_desc = GetBufferDescriptor(buf_ctrl->buf_id);
    return (char *)BufHdrGetBlock(buf_desc);
}

static int CBInvalidatePage(void *db_handle, char pageid[DMS_PAGEID_SIZE], unsigned char invld_owner)
{
    int buf_id = -1;
    BufferTag* tag = (BufferTag *)pageid;
    uint32 hash;
    uint64 buf_state;
    int ret = DMS_SUCCESS;
    bool get_lock;
    bool buftag_equal = true;
    hash = BufTableHashCode(tag);
    buf_id = BufTableLookup(tag, hash);
    if (buf_id < 0) {
        /* not found in shared buffer */
        return ret;
    }

    BufferDesc *buf_desc = GetBufferDescriptor(buf_id);
    dms_buf_ctrl_t *buf_ctrl = GetDmsBufCtrl(buf_id);
    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    PG_TRY();
    {
        do {
            buf_desc = GetBufferDescriptor(buf_id);
            if (SS_PRIMARY_MODE) {
                buf_state = LockBufHdr(buf_desc);
                if (BUF_STATE_GET_REFCOUNT(buf_state) != 0 || BUF_STATE_GET_USAGECOUNT(buf_state) != 0 ||
                !BUFFERTAGS_PTR_EQUAL(&buf_desc->tag, tag)) {
                    UnlockBufHdr(buf_desc, buf_state);
                    ret = DMS_ERROR;
                    break;
                }

                if (!(buf_state & BM_VALID) || (buf_state & BM_IO_ERROR)) {
                    ereport(LOG, (errmodule(MOD_DMS),
                        errmsg("[%d/%d/%d/%d %d-%d] invalidate page, buffer is not valid or io error, state = 0x%x",
                        tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
                        tag->forkNum, tag->blockNum, buf_desc->state)));
                    UnlockBufHdr(buf_desc, buf_state);
                    buf_ctrl->lock_mode = (unsigned char)DMS_LOCK_NULL;
                    buf_ctrl->seg_fileno = EXTENT_INVALID;
                    buf_ctrl->seg_blockno = InvalidBlockNumber;
                    ret = DMS_SUCCESS;
                    break;
                }

                /* For aio (flush disk not finished), dirty, in dirty queue, dirty need flush, can't recycle */
                if (buf_desc->extra->aio_in_progress || (buf_state & BM_DIRTY) || (buf_state & BM_JUST_DIRTIED) ||
                    XLogRecPtrIsValid(pg_atomic_read_u64(&buf_desc->extra->rec_lsn)) ||
                    (buf_ctrl->state & BUF_DIRTY_NEED_FLUSH)) {
                    ereport(DEBUG1, (errmodule(MOD_DMS),
                        errmsg("[%d/%d/%d/%d %d-%d] invalidate owner rejected, buffer is dirty/permanent, state = 0x%x",
                        tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
                        tag->forkNum, tag->blockNum, buf_desc->state)));
                    ret = DMS_ERROR;
                } else {
                    buf_ctrl->lock_mode = (unsigned char)DMS_LOCK_NULL;
                    buf_ctrl->seg_fileno = EXTENT_INVALID;
                    buf_ctrl->seg_blockno = InvalidBlockNumber;
                }

                UnlockBufHdr(buf_desc, buf_state);
                break;
            }

            if (IsSegmentBufferID(buf_id)) {
                (void)SegPinBuffer(buf_desc);
            } else {
                ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
                (void)PinBuffer(buf_desc, NULL);
            }

            if (!BUFFERTAGS_PTR_EQUAL(&buf_desc->tag, tag)) {
                DmsReleaseBuffer(buf_id + 1, IsSegmentBufferID(buf_id));
                buftag_equal = false;
                break;
            }

            bool wait_success = SSWaitIOTimeout(buf_desc);
            if (!wait_success) {
                DmsReleaseBuffer(buf_id + 1, IsSegmentBufferID(buf_id));
                ret = GS_TIMEOUT;
                break;
            }

            if ((!(pg_atomic_read_u64(&buf_desc->state) & BM_VALID)) ||
                (pg_atomic_read_u64(&buf_desc->state) & BM_IO_ERROR)) {
                ereport(LOG, (errmodule(MOD_DMS),
                    errmsg("[%d/%d/%d/%d %d-%d] invalidate page, buffer is not valid or io error, state = 0x%x",
                    tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
                    tag->forkNum, tag->blockNum, buf_desc->state)));
                DmsReleaseBuffer(buf_id + 1, IsSegmentBufferID(buf_id));
                buf_ctrl->lock_mode = (unsigned char)DMS_LOCK_NULL;
                buf_ctrl->seg_fileno = EXTENT_INVALID;
                buf_ctrl->seg_blockno = InvalidBlockNumber;
                ret = DMS_SUCCESS;
                break;
            }

            get_lock = SSLWLockAcquireTimeout(buf_desc->content_lock, LW_EXCLUSIVE);
            if (!get_lock) {
                ereport(WARNING, (errmodule(MOD_DMS), (errmsg("[SS lwlock][%u/%u/%u/%d %d-%u] request LWLock timeout, "
                    "buf_id:%d, lwlock:%p",
                    tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
                    tag->forkNum, tag->blockNum, buf_id, buf_desc->content_lock))));
                ret = GS_TIMEOUT;
            } else {
                buf_ctrl->lock_mode = (unsigned char)DMS_LOCK_NULL;
                buf_ctrl->seg_fileno = EXTENT_INVALID;
                buf_ctrl->seg_blockno = InvalidBlockNumber;
                LWLockRelease(buf_desc->content_lock);
            }

            if (IsSegmentBufferID(buf_id)) {
                SegReleaseBuffer(buf_id + 1);
            } else {
                ReleaseBuffer(buf_id + 1);
            }
        } while(0);
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        /* Save error info */
        ErrorData* edata = CopyErrorData();
        FlushErrorState();
        FreeErrorData(edata);
        ereport(WARNING, (errmsg("[CBInvalidatePage] Error happend, spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u",
            tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode, tag->forkNum,
            tag->blockNum)));
        ReleaseResource();
        ret = DMS_ERROR;
    }
    PG_END_TRY();

    if (ret == DMS_SUCCESS && buftag_equal) {
        Assert(buf_ctrl->lock_mode == DMS_LOCK_NULL);
    }

    return ret;
}

static void CBVerifyPage(dms_buf_ctrl_t *buf_ctrl, char *new_page)
{
    Assert(buf_ctrl->buf_id < TOTAL_BUFFER_NUM);
    if (buf_ctrl->buf_id >= TOTAL_BUFFER_NUM) {
        return;
    }

    BufferDesc *buf_desc = GetBufferDescriptor(buf_ctrl->buf_id);

    if (buf_ctrl->seg_fileno != EXTENT_INVALID) {
        if (buf_desc->extra->seg_fileno == EXTENT_INVALID) {
            buf_desc->extra->seg_fileno = buf_ctrl->seg_fileno;
            buf_desc->extra->seg_blockno = buf_ctrl->seg_blockno;
        } else if (buf_desc->extra->seg_fileno != buf_ctrl->seg_fileno ||
                buf_desc->extra->seg_blockno != buf_ctrl->seg_blockno) {
            ereport(PANIC, (errmsg("[%u/%u/%u/%d/%d %d-%u] location mismatch, seg_fileno:%d, seg_blockno:%u",
                                buf_desc->tag.rnode.spcNode, buf_desc->tag.rnode.dbNode, buf_desc->tag.rnode.relNode,
                                buf_desc->tag.rnode.bucketNode, buf_desc->tag.rnode.opt, buf_desc->tag.forkNum,
                                buf_desc->tag.blockNum, buf_desc->extra->seg_fileno, buf_desc->extra->seg_blockno)));
        }
    }

    /* page content is not valid */
    if ((pg_atomic_read_u64(&buf_desc->state) & BM_VALID) == 0) {
        return;
    }

    /* we only verify segment-page version */
    if (!(buf_desc->extra->seg_fileno != EXTENT_INVALID || IsSegmentBufferID(buf_desc->buf_id))) {
        return;
    }

    char *page = (char *)BufHdrGetBlock(buf_desc);
    XLogRecPtr lsn_past = PageGetLSN(page);
    XLogRecPtr lsn_now = PageGetLSN(new_page);
    if ((lsn_now != InvalidXLogRecPtr) && (lsn_past > lsn_now)) {
        RelFileNode rnode = buf_desc->tag.rnode;
        ereport(PANIC, (errmsg("[%d/%d/%d/%d/%d %d-%d] now lsn(0x%llx) is less than past lsn(0x%llx)",
            rnode.spcNode, rnode.dbNode, rnode.relNode, rnode.bucketNode, rnode.opt,
            buf_desc->tag.forkNum, buf_desc->tag.blockNum,
            (unsigned long long)lsn_now, (unsigned long long)lsn_past)));
    }
    return;
}

static int CBXLogFlush(void *db_handle, unsigned long long *lsn)
{
    (void)LWLockAcquire(WALWriteLock, LW_EXCLUSIVE);
    (void)XLogBackgroundFlush();
    *lsn = GetFlushRecPtr();
    LWLockRelease(WALWriteLock);
    return GS_SUCCESS;
}

static char *CBDisplayBufferTag(char *displayBuf, unsigned int count, char *pageid)
{
    BufferTag pagetag = *(BufferTag *)pageid;
    int ret = sprintf_s(displayBuf, count, "%u/%u/%u/%d/%d %d-%u",
        pagetag.rnode.spcNode, pagetag.rnode.dbNode, pagetag.rnode.relNode, (int)pagetag.rnode.bucketNode,
        (int)pagetag.rnode.opt, pagetag.forkNum, pagetag.blockNum);
    securec_check_ss(ret, "", "");
    return displayBuf;
}

static int CBRemoveBufLoadStatus(dms_buf_ctrl_t *buf_ctrl, dms_buf_load_status_t dms_buf_load_status)
{
    switch (dms_buf_load_status) {
        case DMS_BUF_NEED_LOAD:
            buf_ctrl->state &= ~BUF_NEED_LOAD;
            break;
        case DMS_BUF_IS_LOADED:
            buf_ctrl->state &= ~BUF_IS_LOADED;
            break;
        case DMS_BUF_LOAD_FAILED:
            buf_ctrl->state &= ~BUF_LOAD_FAILED;
            break;
        case DMS_BUF_NEED_TRANSFER:
            buf_ctrl->state &= ~BUF_NEED_TRANSFER;
            break;
        default:
            Assert(0);
    }
    return DMS_SUCCESS;
}

static int CBSetBufLoadStatus(dms_buf_ctrl_t *buf_ctrl, dms_buf_load_status_t dms_buf_load_status)
{
    switch (dms_buf_load_status) {
        case DMS_BUF_NEED_LOAD:
            buf_ctrl->state |= BUF_NEED_LOAD;
            break;
        case DMS_BUF_IS_LOADED:
            buf_ctrl->state |= BUF_IS_LOADED;
            break;
        case DMS_BUF_LOAD_FAILED:
            buf_ctrl->state |= BUF_LOAD_FAILED;
            break;
        case DMS_BUF_NEED_TRANSFER:
            buf_ctrl->state |= BUF_NEED_TRANSFER;
            break;
        default:
            Assert(0);
    }
    return DMS_SUCCESS;
}

static void *CBGetHandle(unsigned int *db_handle_index, dms_session_type_e session_type)
{
    ss_fake_seesion_context_t *fs_cxt = &g_instance.dms_cxt.SSFakeSessionCxt;
    SpinLockAcquire(&fs_cxt->lock);
    if (!fs_cxt->fake_sessions[fs_cxt->quickFetchIndex]) {
        int index = fs_cxt->quickFetchIndex;
        fs_cxt->fake_sessions[index] = true;
        fs_cxt->quickFetchIndex++;
        if (fs_cxt->quickFetchIndex >= fs_cxt->fake_session_cnt) {
            fs_cxt->quickFetchIndex = 0;
        }
        SpinLockRelease(&fs_cxt->lock);
        *db_handle_index = index + fs_cxt->session_start;
        return &g_instance.proc_base->allProcs[index + fs_cxt->session_start];
    }

    int start_index = fs_cxt->quickFetchIndex;
    int cur_index = 0;
    bool found = false;
    for (int i = 0; i < (int)fs_cxt->fake_session_cnt; i++) {
        cur_index = (start_index + i) % fs_cxt->fake_session_cnt;
        if (!fs_cxt->fake_sessions[cur_index]) {
            found = true;
            break;
        }
    }

    if (!found) {
        SpinLockRelease(&fs_cxt->lock);
        ereport(PANIC, (errmsg("[SS] can not find a session. please check")));
    }

    fs_cxt->quickFetchIndex = cur_index + 1;
    if (fs_cxt->quickFetchIndex >= fs_cxt->fake_session_cnt) {
       fs_cxt->quickFetchIndex = 0;
    }
    SpinLockRelease(&fs_cxt->lock);
    *db_handle_index = cur_index;
    return &g_instance.proc_base->allProcs[cur_index + fs_cxt->session_start];
}

static void CBReleaseHandle(void *db_handle)
{
    ss_fake_seesion_context_t *fs_cxt = &g_instance.dms_cxt.SSFakeSessionCxt;
    int index = ((char*)db_handle - (char*)&g_instance.proc_base->allProcs[fs_cxt->session_start]) / sizeof(PGPROC*);
    fs_cxt->fake_sessions[index] = false;
}

static char *CBMemAlloc(void *context, unsigned int size)
{
    char *ptr = NULL;
    MemoryContext old_cxt = MemoryContextSwitchTo(t_thrd.dms_cxt.msgContext);
    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;

    PG_TRY();
    {
        ptr = (char *)palloc(size);
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        if (t_thrd.role == DMS_WORKER) {
            FlushErrorState();
        }
    }
    PG_END_TRY();

    (void)MemoryContextSwitchTo(old_cxt);
    return ptr;
}

static void CBMemFree(void *context, void *pointer)
{
    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;

    PG_TRY();
    {
        pfree(pointer);
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        if (t_thrd.role == DMS_WORKER) {
            FlushErrorState();
        }
    }
    PG_END_TRY();
}

static void CBMemReset(void *context)
{
    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    PG_TRY();
    {
        MemoryContextReset(t_thrd.dms_cxt.msgContext);
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        if (t_thrd.role == DMS_WORKER) {
            FlushErrorState();
        }
    }
    PG_END_TRY();
}

static int32 CBProcessLockAcquire(char *data, uint32 len)
{
    if (unlikely(len != sizeof(SSBroadcastDDLLock))) {
        ereport(DEBUG1, (errmsg("invalid broadcast ddl lock message")));
        return DMS_ERROR;
    }

    SSBroadcastDDLLock *ssmsg = (SSBroadcastDDLLock *)data;
    LockAcquireResult res;
    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    PG_TRY();
    {
        res = LockAcquire(&(ssmsg->locktag), ssmsg->lockmode, false, ssmsg->dontWait);
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        res = LOCKACQUIRE_NOT_AVAIL;
        ereport(WARNING, (errmsg("SS Standby process DDLLockAccquire got in PG_CATCH")));
        if (t_thrd.role == DMS_WORKER) {
            FlushErrorState();
        }
    }
    PG_END_TRY();

    if (!(ssmsg->dontWait) && res == LOCKACQUIRE_NOT_AVAIL) {
        ereport(WARNING, (errmsg("SS process DDLLockAccquire request failed!")));
        return DMS_ERROR;
    }
    return DMS_SUCCESS;
}

static int32 CBProcessLockRelease(char *data, uint32 len)
{
    if (unlikely(len != sizeof(SSBroadcastDDLLock))) {
        ereport(DEBUG1, (errmsg("invalid lock release message")));
        return DMS_ERROR;
    }

    SSBroadcastDDLLock *ssmsg = (SSBroadcastDDLLock *)data;
    int res = DMS_SUCCESS;
    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    PG_TRY();
    {
        (void)LockRelease(&(ssmsg->locktag), ssmsg->lockmode, ssmsg->sessionlock);
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        res = DMS_ERROR;
        ereport(WARNING, (errmsg("SS process DDLLockRelease request failed!")));
        if (t_thrd.role == DMS_WORKER) {
            FlushErrorState();
        }
    }
    PG_END_TRY();

    return res;
}

static int32 CBProcessReleaseAllLock(uint32 len)
{
    if (unlikely(len != sizeof(SSBroadcastCmdOnly))) {
        return DMS_ERROR;
    }

    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    int res = DMS_SUCCESS;
    PG_TRY();
    {
        LockErrorCleanup();
        LockReleaseAll(DEFAULT_LOCKMETHOD, true);
        LockReleaseAll(USER_LOCKMETHOD, true);
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        res = DMS_ERROR;
        ereport(WARNING, (errmsg("SS process DDLLockReleaseAll request failed!")));
        if (t_thrd.role == DMS_WORKER) {
            FlushErrorState();
        }
    }
    PG_END_TRY();

    return res;
}

static int32 CBProcessBroadcast(void *db_handle, dms_broadcast_context_t *broad_ctx)
{
    char *data = broad_ctx->data;
    unsigned int len = broad_ctx->len;
    char *output_msg = broad_ctx->output_msg;
    unsigned int *output_msg_len = broad_ctx->output_msg_len;
    int32 ret = DMS_SUCCESS;
    SSBroadcastOp bcast_op = *(SSBroadcastOp *)data;

    *output_msg_len = 0;
    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;

    PG_TRY();
    {
        switch (bcast_op) {
            case BCAST_SI:
                ret = SSProcessSharedInvalMsg(data, len);
                break;
            case BCAST_SEGDROPTL:
                ret = SSProcessSegDropTimeline(data, len);
                break;
            case BCAST_DROP_REL_ALL_BUFFER:
                ret = SSProcessDropRelAllBuffer(data, len);
                break;
            case BCAST_DROP_REL_RANGE_BUFFER:
                ret = SSProcessDropRelRangeBuffer(data, len);
                break;
            case BCAST_DROP_DB_ALL_BUFFER:
                ret = SSProcessDropDBAllBuffer(data, len);
                break;
            case BCAST_DROP_SEG_SPACE:
                ret = SSProcessDropSegSpace(data, len);
                break;
            case BCAST_DDLLOCK:
                ret = CBProcessLockAcquire(data, len);
                break;
            case BCAST_DDLLOCKRELEASE:
                ret = CBProcessLockRelease(data, len);
                break;
            case BCAST_DDLLOCKRELEASE_ALL:
                ret = CBProcessReleaseAllLock(len);
                break;
            case BCAST_CHECK_DB_BACKENDS:
                ret = SSCheckDbBackends(data, len, output_msg, output_msg_len);
                break;
            case BCAST_SEND_SNAPSHOT:
                ret = SSUpdateLatestSnapshotOfStandby(data, len);
                break;
            case BCAST_RELOAD_REFORM_CTRL_PAGE:
                ret = SSReloadReformCtrlPage(len);
                break;
            default:
                ereport(WARNING, (errmodule(MOD_DMS), errmsg("invalid broadcast operate type")));
                ret = DMS_ERROR;
                break;
        }
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        if (t_thrd.role == DMS_WORKER) {
            FlushErrorState();
        }
    }
    PG_END_TRY();

    return ret;
}

static int32 CBProcessBroadcastAck(void *db_handle, dms_broadcast_context_t *broad_ctx)
{
    char *data = broad_ctx->data;
    unsigned int len = broad_ctx->len;
    int32 ret = DMS_SUCCESS;
    SSBroadcastOpAck bcast_op = *(SSBroadcastOpAck *)data;

    switch (bcast_op) {
        case BCAST_CHECK_DB_BACKENDS_ACK:
            ret = SSCheckDbBackendsAck(data, len);
            break;
        default:
            ereport(WARNING, (errmodule(MOD_DMS), errmsg("invalid broadcast ack type")));
            ret = DMS_ERROR;
    }
    return ret;
}

static int CBGetDmsStatus(void *db_handle)
{
    return (int)g_instance.dms_cxt.dms_status;
}

static void CBSetDmsStatus(void *db_handle, int dms_status)
{
    g_instance.dms_cxt.dms_status = (dms_status_t)dms_status;
}

static bool SSCheckBufferIfCanGoRebuild(BufferDesc* buf_desc, uint64 buf_state)
{
    bool ret = false;
    dms_buf_ctrl_t *buf_ctrl = GetDmsBufCtrl(buf_desc->buf_id);
    if ((buf_state & BM_VALID) && (buf_ctrl->lock_mode != (unsigned char)DMS_LOCK_NULL)) {
        ret = true;
    } else if ((buf_state & BM_TAG_VALID) && (buf_ctrl->lock_mode != (unsigned char)DMS_LOCK_NULL)) {
        if (LWLockConditionalAcquire(buf_desc->io_in_progress_lock, LW_SHARED)) {
            ret = true;
        } else {
            /*
             * In the condition of (Phase1)readbuffer_common->(Phase2)dms_request_page->(Phase3)seg_read->(Phase4)lock
             * seg_head buffer, and stuck in Phase4 as reform happened. It will block the request of data pase as the
             * lock mode of dms_buf_ctrl was already set to DMS_LOCK_EXCLUSIVE and IO is still in process.
             * In order to get rid of this dilemma, we force set the lock mode back to null and don't rebuild this
             * page. The stucked process will request the page again when it add content lock and the reformer will
             * become owner when it request the page.
             */
            ereport(WARNING, (errmsg("[%u/%u/%u/%d/0 %d-%u] Set lock mode to NULL, desc state:%lu, ctrl state:%u, lock mode:%d.",
                buf_desc->tag.rnode.spcNode, buf_desc->tag.rnode.dbNode, buf_desc->tag.rnode.relNode,
                buf_desc->tag.rnode.bucketNode, buf_desc->tag.forkNum, buf_desc->tag.blockNum, buf_state, buf_ctrl->state, buf_ctrl->lock_mode)));
            buf_ctrl->lock_mode = DMS_LOCK_NULL;
        }
    }

    return ret;
}

static int32 SSRebuildBuf(BufferDesc *buf_desc, unsigned char thread_index)
{
#ifdef USE_ASSERT_CHECKING
    if (IsSegmentPhysicalRelNode(buf_desc->tag.rnode)) {
        SegNetPageCheckDiskLSN(buf_desc, RBM_NORMAL, NULL);
    } else {
        SmgrNetPageCheckDiskLSN(buf_desc, RBM_NORMAL, NULL);
    }
#endif

    dms_buf_ctrl_t *buf_ctrl = GetDmsBufCtrl(buf_desc->buf_id);
    Assert(buf_ctrl != NULL);
    Assert(buf_ctrl->is_edp != 1);
    Assert(XLogRecPtrIsValid(g_instance.dms_cxt.ckptRedo));
    dms_context_t dms_ctx;
    InitDmsBufContext(&dms_ctx, buf_desc->tag);
    dms_ctrl_info_t ctrl_info = { 0 };
    ctrl_info.ctrl = *buf_ctrl;
    ctrl_info.lsn = (unsigned long long)BufferGetLSN(buf_desc);
    ctrl_info.is_dirty = (buf_desc->state & (BM_DIRTY | BM_JUST_DIRTIED)) > 0 ? true : false;
    int ret = dms_buf_res_rebuild_drc_parallel(&dms_ctx, &ctrl_info, thread_index);
    if (ret != DMS_SUCCESS) {
        ereport(WARNING, (errmsg("Failed to rebuild page, rel:%u/%u/%u/%d, forknum:%d, blocknum:%u.",
            buf_desc->tag.rnode.spcNode, buf_desc->tag.rnode.dbNode, buf_desc->tag.rnode.relNode,
            buf_desc->tag.rnode.bucketNode, buf_desc->tag.forkNum, buf_desc->tag.blockNum)));
        return ret;
    }
    return DMS_SUCCESS;
}

static int32 CBDrcBufRebuildInternal(int begin, int len, unsigned char thread_index)
{
    uint64 buf_state;
    Assert(begin >= 0 && len > 0 && (begin + len) <= TOTAL_BUFFER_NUM);
    for (int i = begin; i < begin + len; i++) {
        BufferDesc *buf_desc = GetBufferDescriptor(i);
        bool need_rebuild = true;
        if (LWLockConditionalAcquire(buf_desc->content_lock, LW_EXCLUSIVE)) {
            buf_state = LockBufHdr(buf_desc);
            if ((buf_state & BM_VALID) && !(buf_state & BM_DIRTY)) {
                dms_buf_ctrl_t *buf_ctrl = GetDmsBufCtrl(buf_desc->buf_id);
                buf_ctrl->lock_mode = DMS_LOCK_NULL;
                need_rebuild = false;
            }
            UnlockBufHdr(buf_desc, buf_state);
            LWLockRelease(buf_desc->content_lock);
        } 
        
        if (need_rebuild) {
            buf_state = LockBufHdr(buf_desc);
            if (SSCheckBufferIfCanGoRebuild(buf_desc, buf_state)) {
                int ret = SSRebuildBuf(buf_desc, thread_index);
                if (ret != DMS_SUCCESS) {
                    if (LWLockHeldByMe(buf_desc->io_in_progress_lock)) {
                        LWLockRelease(buf_desc->io_in_progress_lock);
                    }
                    UnlockBufHdr(buf_desc, buf_state);
                    return ret;
                }
            }
            if (LWLockHeldByMe(buf_desc->io_in_progress_lock)) {
                LWLockRelease(buf_desc->io_in_progress_lock);
            }
            UnlockBufHdr(buf_desc, buf_state);
        }
    }
    ereport(LOG, (errmodule(MOD_DMS),
        errmsg("[SS reform] rebuild buf thread_index:%d, buf_if start from:%d to:%d, max_buf_id:%d",
        (int)thread_index, begin, (begin + len - 1), (TOTAL_BUFFER_NUM - 1))));
    return GS_SUCCESS;
}

/* 
    * as you can see, thread_num represets the number of thread. thread_index reprsents the n-th thread, begin from 0.
    * special case: 
    *   when parallel disable, rebuild phase still call this function, 
    *   do you think thread_num is 1, and thread_index is 0 ?
    *   actually thread_num and thread_index are 255. It just a agreement in DMS
    */
const int dms_invalid_thread_index = 255;
const int dms_invalid_thread_num = 255;
static int32 CBDrcBufRebuildParallel(void* db_handle, unsigned char thread_index, unsigned char thread_num)
{
    Assert((thread_index == dms_invalid_thread_index && thread_num == dms_invalid_thread_num) ||
            (thread_index != dms_invalid_thread_index && thread_num != dms_invalid_thread_num &&
            thread_index < thread_num));
    int buf_num = TOTAL_BUFFER_NUM / thread_num;
    int buf_begin = thread_index * buf_num;
    if (thread_index == thread_num - 1) {
        buf_num = TOTAL_BUFFER_NUM - buf_begin;
    }

    if (thread_index == dms_invalid_thread_index && thread_num == dms_invalid_thread_num) {
        buf_begin = 0;
        buf_num = TOTAL_BUFFER_NUM;
    }
    return CBDrcBufRebuildInternal(buf_begin, buf_num, thread_index);
}

static int32 CBDrcBufValidate(void *db_handle)
{
    /* Load Control File */
    int src_id = SSGetPrimaryInstId();
    SSReadControlFile(src_id, true);
    int buf_cnt = 0;

    uint64 buf_state;
    ereport(LOG, (errmodule(MOD_DMS),
        errmsg("[SS reform]CBDrcBufValidate starts before reform done.")));
    for (int i = 0; i < TOTAL_BUFFER_NUM; i++) {
        BufferDesc *buf_desc = GetBufferDescriptor(i);
        buf_state = LockBufHdr(buf_desc);
        if ((buf_state & BM_VALID) || (buf_state & BM_TAG_VALID)) {
            BufValidateDrc(buf_desc);
            buf_cnt++;
        }
        UnlockBufHdr(buf_desc, buf_state);
    }

    ereport(LOG, (errmodule(MOD_DMS),
        errmsg("[SS reform]CBDrcBufValidate %d buffers success.", buf_cnt)));
    return GS_SUCCESS;
}

// used for find bufferdesc in dms
// no need WaitIO to check valid bit is set or not, we use spinlock to guarantee to lock_mode
static BufferDesc* SSGetBufferDesc(char *pageid)
{
    int buf_id;
    BufferTag *tag = (BufferTag *)pageid;
    BufferDesc *buf_desc = NULL;
    RelFileNode relfilenode = tag->rnode;
    uint32 hash = BufTableHashCode(tag);
    bool retry = false;

#ifdef USE_ASSERT_CHECKING
    if (IsSegmentPhysicalRelNode(relfilenode)) {
        SegSpace *spc = spc_open(relfilenode.spcNode, relfilenode.dbNode, false, false);
        BlockNumber spc_nblocks = spc_size(spc, relfilenode.relNode, tag->forkNum);
        if (tag->blockNum >= spc_nblocks) {
            ereport(PANIC, (errmodule(MOD_DMS),
                errmsg("unexpected blocknum %u >= spc nblocks %u", tag->blockNum, spc_nblocks)));
        }
    }
#endif

    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    PG_TRY();
    {
        do {
            buf_id = BufTableLookup(tag, hash);
            if (buf_id < 0) {
                buf_desc = NULL;
                break;
            }
            
            buf_desc = GetBufferDescriptor(buf_id);
            (void)SSPinBuffer(buf_desc);
            if (!BUFFERTAGS_PTR_EQUAL(&buf_desc->tag, tag)) {
                SSUnPinBuffer(buf_desc);
                buf_desc = NULL;
                retry = true;
            } else {
                retry = false;
            }
        } while (retry);
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        ReleaseResource();
    }
    PG_END_TRY();
    return buf_desc;
}

static int CBConfirmOwner(void *db_handle, char *pageid, unsigned char *lock_mode, unsigned char *is_edp,
    unsigned long long *edp_lsn)
{
    *is_edp = (unsigned char)false;
    dms_buf_ctrl_t *buf_ctrl = NULL;
    BufferDesc *buf_desc = SSGetBufferDesc(pageid);
    if (buf_desc == NULL) {
        *lock_mode = (uint8)DMS_LOCK_NULL;
        return GS_SUCCESS;
    }

    buf_ctrl = GetDmsBufCtrl(buf_desc->buf_id);
    LWLockAcquire((LWLock*)buf_ctrl->ctrl_lock, LW_EXCLUSIVE);
    *lock_mode = buf_ctrl->lock_mode;
#ifdef USE_ASSERT_CHECKING
    if (buf_ctrl->is_edp) {
        BufferTag *tag = &buf_desc->tag;
        ereport(PANIC, (errmsg("[SS][%u/%u/%u/%d %d-%u] CBConfirmOwner, do not allow edp exist, please check.",
            tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
            tag->forkNum, tag->blockNum)));
    }
#endif
    LWLockRelease((LWLock*)buf_ctrl->ctrl_lock);
    SSUnPinBuffer(buf_desc);
    return GS_SUCCESS;
}

static int CBConfirmConverting(void *db_handle, char *pageid, unsigned char smon_chk,
    unsigned char *lock_mode, unsigned long long *edp_map, unsigned long long *lsn)
{
    *lsn = 0; // lsn not used in dms, so need to waste time to PageGetLSN
    *edp_map = 0;
    
    BufferDesc *buf_desc = SSGetBufferDesc(pageid);
    if (buf_desc == NULL) {
        *lock_mode = (uint8)DMS_LOCK_NULL;
        return GS_SUCCESS;
    }

    dms_buf_ctrl_t *buf_ctrl = GetDmsBufCtrl(buf_desc->buf_id);
    LWLockAcquire((LWLock*)buf_ctrl->ctrl_lock, LW_EXCLUSIVE);
    *lock_mode = buf_ctrl->lock_mode;
#ifdef USE_ASSERT_CHECKING
    if (buf_ctrl->is_edp) {
        BufferTag *tag = &buf_desc->tag;
        ereport(PANIC, (errmsg("[SS][%u/%u/%u/%d %d-%u] CBConfirmConverting, do not allow edp exist, please check.",
            tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
            tag->forkNum, tag->blockNum)));
    }
#endif
    LWLockRelease((LWLock*)buf_ctrl->ctrl_lock);
    SSUnPinBuffer(buf_desc);
    return GS_SUCCESS;
}

static int CBGetStableList(void *db_handle, unsigned long long *list_stable, unsigned char *reformer_id)
{
    *list_stable = g_instance.dms_cxt.SSReformerControl.list_stable;
    *reformer_id = (uint8)g_instance.dms_cxt.SSReformerControl.primaryInstId;
    return GS_SUCCESS;
}

static int CBStartup(void *db_handle)
{
    g_instance.dms_cxt.SSRecoveryInfo.ready_to_startup = true;
    return GS_SUCCESS;
}

static int CBRecoveryStandby(void *db_handle, int inst_id)
{
    Assert(inst_id == g_instance.attr.attr_storage.dms_attr.instance_id);
    ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS reform] Recovery as standby")));

    if (!SSRecoveryNodes()) {
        ereport(WARNING, (errmodule(MOD_DMS), errmsg("[SS reform] Recovery failed")));
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static int CBRecoveryPrimary(void *db_handle, int inst_id)
{
    Assert(g_instance.dms_cxt.SSReformerControl.primaryInstId == inst_id ||
        g_instance.dms_cxt.SSReformerControl.primaryInstId == -1);
    g_instance.dms_cxt.SSRecoveryInfo.in_flushcopy = false;
    ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS reform] Recovery as primary, will replay xlog from inst:%d",
                         g_instance.dms_cxt.SSReformerControl.primaryInstId)));

    /* Release my own lock before recovery */
    SSLockReleaseAll();
    SSWakeupRecovery();
    if (!SSRecoveryNodes()) {
        g_instance.dms_cxt.SSRecoveryInfo.recovery_pause_flag = true;
        ereport(WARNING, (errmodule(MOD_DMS), errmsg("[SS reform] Recovery failed")));
        return GS_ERROR;
    }

    g_instance.dms_cxt.SSRecoveryInfo.recovery_pause_flag = true;
    return GS_SUCCESS;
}

static int CBFlushCopy(void *db_handle, char *pageid)
{
    // only 1) primary restart 2) failover need flush_copy
    if (SS_REFORM_REFORMER && g_instance.dms_cxt.dms_status == DMS_STATUS_IN && !SS_STANDBY_FAILOVER) {
        return GS_SUCCESS;
    }

    if (SS_REFORM_REFORMER && !g_instance.dms_cxt.SSRecoveryInfo.in_flushcopy) {
        g_instance.dms_cxt.SSRecoveryInfo.in_flushcopy = true;
        smgrcloseall();
    }

    BufferTag* tag = (BufferTag*)pageid;
    Buffer buffer;

    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    PG_TRY();
    {
        buffer = SSReadBuffer(tag, RBM_NORMAL);
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        /* Save error info */
        ErrorData* edata = CopyErrorData();
        FlushErrorState();
        FreeErrorData(edata);
        ereport(PANIC, (errmsg("[SS flush copy] Error happend, spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u",
                        tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
                        tag->forkNum, tag->blockNum)));
    }
    PG_END_TRY();

    /**
     *  when remote DB instance reboot, this round reform fail
     *  primary node may fail to get page from remote node which reboot, this phase should return fail
     */
    if (BufferIsInvalid(buffer)) {
        if (dms_reform_failed()) {
            SSWaitStartupExit();
            return GS_ERROR;
        } else {
            Assert(0);
        }
    }

    Assert(XLogRecPtrIsValid(g_instance.dms_cxt.ckptRedo));
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    if (t_thrd.dms_cxt.flush_copy_get_page_failed) {
        t_thrd.dms_cxt.flush_copy_get_page_failed = false;
        SSWaitStartupExit();
        return GS_ERROR;
    }
    BufferDesc* buf_desc = GetBufferDescriptor(buffer - 1);
    XLogRecPtr pagelsn = BufferGetLSN(buf_desc);
    if (XLByteLT(g_instance.dms_cxt.ckptRedo, pagelsn)) {
        dms_buf_ctrl_t *buf_ctrl = GetDmsBufCtrl(buffer - 1);
        buf_ctrl->state |= BUF_DIRTY_NEED_FLUSH;
        ereport(LOG, (errmsg("[SS] Mark need flush in flush copy, spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u, page lsn (0x%llx)",
                            tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
                            tag->forkNum, tag->blockNum, (unsigned long long)pagelsn)));
    } else {
        ereport(LOG, (errmsg("[SS] ready to flush copy, spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u, page lsn (0x%llx)",
                        tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
                        tag->forkNum, tag->blockNum, (unsigned long long)pagelsn)));
    }
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);
    return GS_SUCCESS;
}

static void SSFailoverPromoteNotify()
{
    if (g_instance.dms_cxt.SSRecoveryInfo.startup_reform) {
        g_instance.dms_cxt.SSRecoveryInfo.restart_failover_flag = true;
        ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS failover] do failover when DB restart.")));
    } else {
        SendPostmasterSignal(PMSIGNAL_DMS_FAILOVER_STARTUP);
        ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS failover] do failover when DB alive")));
    }
}

static int CBFailoverPromote(void *db_handle)
{
    SSClearSegCache();
    SSFailoverPromoteNotify();

    while (true) {
        if (SS_STANDBY_FAILOVER && g_instance.pid_cxt.StartupPID != 0) {
            ereport(LOG, (errmodule(MOD_DMS), errmsg("startup thread success.")));
            return GS_SUCCESS;
        }
        pg_usleep(REFORM_WAIT_TIME);
    }
}

static int CBGetDBPrimaryId(void *db_handle, unsigned int *primary_id)
{
    *primary_id = (unsigned int)g_instance.dms_cxt.SSReformerControl.primaryInstId;
    return GS_SUCCESS;
}

/* 
 * Currently only used in SS switchover. To prevent state machine misjudgement,
 * DSS status, dms_role, SSClusterState must be set atommically.
 * DSS recommends we retry dss_set_server_status if it failed.
 */
static void CBReformSetDmsRole(void *db_handle, unsigned int reformer_id)
{
    ss_reform_info_t *reform_info = &g_instance.dms_cxt.SSReformInfo;
    dms_role_t new_dms_role = reformer_id == (unsigned int)SS_MY_INST_ID ? DMS_ROLE_REFORMER : DMS_ROLE_PARTNER;
    if (new_dms_role == DMS_ROLE_REFORMER) {
        ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS switchover]begin to set currrent DSS as primary")));
        SSGrantDSSWritePermission();
        g_instance.dms_cxt.SSClusterState = NODESTATE_STANDBY_PROMOTING;
    }

    reform_info->dms_role = new_dms_role;
    ereport(LOG, (errmodule(MOD_DMS),
        errmsg("[SS switchover]role and lock switched, updated inst:%d with role:%d success",
            SS_MY_INST_ID, reform_info->dms_role)));
    /* we need change ha cur mode for switchover in ss double cluster here */
    if (SS_DISASTER_CLUSTER) {
        SSDisasterUpdateHAmode();
    }
}

static void ReformCleanBackends()
{
    /* cluster has no transactions during startup reform */
    if (!g_instance.dms_cxt.SSRecoveryInfo.startup_reform) {
        SendPostmasterSignal(PMSIGNAL_DMS_REFORM);
    }

    int ticks = 0;
    while (true) {
        if (g_instance.pid_cxt.StartupPID != 0) {
            if (ticks++ > REFORM_START_CLEAN_TICKS) {
                ereport(WARNING, (errmodule(MOD_DMS),
                    errmsg("[SS reform] StartupXLOG debris sigterm timeout 10s, exit now")));
                _exit(0);
            }
            pg_usleep(REFORM_WAIT_LONG);
            continue;
        }
        
        if (dms_reform_failed()) {
            ereport(WARNING, (errmodule(MOD_DMS), errmsg("[SS reform]reform failed during caneling backends")));
            return;
        }
        if (g_instance.dms_cxt.SSRecoveryInfo.reform_ready || g_instance.dms_cxt.SSRecoveryInfo.startup_reform) {
            ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS reform]reform ready, backends have been terminated")));
            return;
        }
        pg_usleep(REFORM_WAIT_TIME);
    }
}

static void FailoverCleanBackends()
{
    if (g_instance.dms_cxt.SSRecoveryInfo.startup_reform) {
        return;
    }

    if (ENABLE_ONDEMAND_REALTIME_BUILD && SS_STANDBY_MODE) {
        OnDemandWaitRealtimeBuildShutDownInPartnerFailover();
    }

    /**
     * for failover: wait for backend threads to exit, at most 30s
     * why wait code write this
     *      step 1, sned signal to tell thread to exit
     *      step 2, PM detected backend exit
     *      step 3, reform proc wait
     */
    g_instance.dms_cxt.SSRecoveryInfo.no_backend_left = false;
    SendPostmasterSignal(PMSIGNAL_DMS_FAILOVER_TERM_BACKENDS);
    long max_wait_time = 30000000L;
    long wait_time = 0;
    while (true) {
        if (g_instance.dms_cxt.SSRecoveryInfo.no_backend_left && !CheckpointInProgress()) {
            ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS failover] backends exit successfully")));
            break;
        }
        if (wait_time > max_wait_time) {
            ereport(WARNING, (errmodule(MOD_DMS), errmsg("[SS failover] failover failed, backends can not exit")));
            _exit(0);
        }

        if (dms_reform_failed()) {
            ereport(WARNING, (errmodule(MOD_DMS), errmsg("[SS failover] reform failed during clean backends")));
            return;
        }

        pg_usleep(REFORM_WAIT_TIME);
        wait_time += REFORM_WAIT_TIME;
    }
}

static int reform_type_str_len = 30;
static void ReformTypeToString(dms_reform_type_t reform_type, char* ret_str)
{
    switch (reform_type)
    {
    case DMS_REFORM_TYPE_FOR_NORMAL_OPENGAUSS:
        strcpy_s(ret_str, reform_type_str_len, "normal reform");
        break;
    case DMS_REFORM_TYPE_FOR_FAILOVER_OPENGAUSS:
        strcpy_s(ret_str, reform_type_str_len, "failover reform");
        break;
    case DMS_REFORM_TYPE_FOR_SWITCHOVER_OPENGAUSS:
        strcpy_s(ret_str, reform_type_str_len, "switchover reform");
        break;
    case DMS_REFORM_TYPE_FOR_FULL_CLEAN:
        strcpy_s(ret_str, reform_type_str_len, "full clean reform");
        break;
    default:
        strcpy_s(ret_str, reform_type_str_len, "unknown");
        break;
    } 
    return;
}

static void SSXminInfoPrepare()
{
    ss_xmin_info_t *xmin_info = &g_instance.dms_cxt.SSXminInfo;
    if (g_instance.dms_cxt.SSReformInfo.dms_role == DMS_ROLE_REFORMER) {
        SpinLockAcquire(&xmin_info->global_oldest_xmin_lock);
        xmin_info->prev_global_oldest_xmin = xmin_info->global_oldest_xmin;
        xmin_info->global_oldest_xmin_active = false;
        xmin_info->global_oldest_xmin = MaxTransactionId;
        SpinLockRelease(&xmin_info->global_oldest_xmin_lock);
        for (int i = 0; i < DMS_MAX_INSTANCES; i++) {
            ss_node_xmin_item_t *item = &xmin_info->node_table[i];
            SpinLockAcquire(&item->item_lock);
            item->active = false;
            item->notify_oldest_xmin = MaxTransactionId;
            SpinLockRelease(&item->item_lock);
        }

        if (!SSPerformingStandbyScenario()) {
            SpinLockAcquire(&xmin_info->snapshot_available_lock);
            xmin_info->snapshot_available = false;
            SpinLockRelease(&xmin_info->snapshot_available_lock);
        }
    }
    xmin_info->bitmap_active_nodes = 0;
}

static void FailoverStartNotify(dms_reform_start_context_t *rs_cxt)
{
    ss_reform_info_t *reform_info = &g_instance.dms_cxt.SSReformInfo;
    if (reform_info->reform_type == DMS_REFORM_TYPE_FOR_FAILOVER_OPENGAUSS) {
        g_instance.dms_cxt.SSRecoveryInfo.in_failover = true;
        g_instance.dms_cxt.SSRecoveryInfo.recovery_pause_flag = true;
        if (rs_cxt->role == DMS_ROLE_REFORMER) {
            g_instance.dms_cxt.dw_init = false;
            // variable set order: SharedRecoveryInProgress -> failover_ckpt_status -> dms_role
            volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
            SpinLockAcquire(&xlogctl->info_lck);
            xlogctl->IsRecoveryDone = false;
            xlogctl->SharedRecoveryInProgress = true;
            SpinLockRelease(&xlogctl->info_lck);
            t_thrd.shemem_ptr_cxt.ControlFile->state = DB_IN_CRASH_RECOVERY;
            pg_memory_barrier();
            g_instance.dms_cxt.SSRecoveryInfo.failover_ckpt_status = NOT_ALLOW_CKPT;
            g_instance.dms_cxt.SSClusterState = NODESTATE_STANDBY_FAILOVER_PROMOTING;
            /* Backends should exit in here, this step should be bring forward and not in CBFailoverPromote */
            pmState = PM_WAIT_BACKENDS;
            ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS failover] failover trigger.")));
        }
    }
}

static void CBReformStartNotify(void *db_handle, dms_reform_start_context_t *rs_cxt)
{
    ereport(LOG, (errmsg("[SS Reform] starts, pmState=%d, SSClusterState=%d, demotion=%d-%d, rec=%d",
        pmState, g_instance.dms_cxt.SSClusterState, g_instance.demotion,
        t_thrd.walsender_cxt.WalSndCtl->demotion, t_thrd.xlog_cxt.InRecovery)));

    ss_reform_info_t *reform_info = &g_instance.dms_cxt.SSReformInfo;
    reform_info->is_hashmap_constructed = false;
    reform_info->reform_type = rs_cxt->reform_type;
    g_instance.dms_cxt.SSClusterState = NODESTATE_NORMAL;
    g_instance.dms_cxt.SSRecoveryInfo.reform_ready = false;
    g_instance.dms_cxt.SSRecoveryInfo.in_flushcopy = false;
    g_instance.dms_cxt.SSRecoveryInfo.startup_need_exit_normally = false;
    g_instance.dms_cxt.resetSyscache = true;
    g_instance.dms_cxt.SSRecoveryInfo.in_failover = false;
    FailoverStartNotify(rs_cxt);

    reform_info->reform_start_time = GetCurrentTimestamp();
    reform_info->bitmap_nodes = rs_cxt->bitmap_participated;
    reform_info->bitmap_reconnect = rs_cxt->bitmap_reconnect;
    reform_info->dms_role = rs_cxt->role;
    SSXminInfoPrepare();
    reform_info->in_reform = true;

    char reform_type_str[reform_type_str_len] = {0};
    ReformTypeToString(reform_info->reform_type, reform_type_str);
    ereport(LOG, (errmodule(MOD_DMS),
        errmsg("[SS reform] dms reform start, role:%d, reform type:SS %s, standby scenario:%d",
            reform_info->dms_role, reform_type_str, SSPerformingStandbyScenario())));
    if (reform_info->dms_role == DMS_ROLE_REFORMER) {
        SSGrantDSSWritePermission();
    }

    int old_primary = SSGetPrimaryInstId();
    SSReadControlFile(old_primary, true);
    g_instance.dms_cxt.SSReformInfo.old_bitmap = g_instance.dms_cxt.SSReformerControl.list_stable;
    ereport(LOG, (errmsg("[SS reform] old cluster node bitmap: %lld", g_instance.dms_cxt.SSReformInfo.old_bitmap)));

    if (g_instance.dms_cxt.SSRecoveryInfo.in_failover) {
        FailoverCleanBackends();
    } else if (SSBackendNeedExitScenario()) {
        ReformCleanBackends();
    }

    if (SS_DISASTER_CLUSTER && reform_info->reform_type != DMS_REFORM_TYPE_FOR_SWITCHOVER_OPENGAUSS) {
        SSDisasterUpdateHAmode();
    }
}

static int CBReformDoneNotify(void *db_handle)
{
    if (g_instance.dms_cxt.SSRecoveryInfo.in_failover) {
        g_instance.dms_cxt.SSRecoveryInfo.in_failover = false;
        if (SS_REFORM_REFORMER) {
            ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS failover] failover success, instance:%d become primary.",
                g_instance.attr.attr_storage.dms_attr.instance_id)));
        }
    }

    if (SS_DISASTER_CLUSTER) {
        SSDisasterUpdateHAmode();
    }
   
    /* SSClusterState and in_reform must be set atomically */
    g_instance.dms_cxt.SSRecoveryInfo.startup_reform = false;
    g_instance.dms_cxt.SSRecoveryInfo.restart_failover_flag = false;
    g_instance.dms_cxt.SSRecoveryInfo.failover_ckpt_status = NOT_ACTIVE;
    Assert(g_instance.dms_cxt.SSRecoveryInfo.in_flushcopy == false);
    g_instance.dms_cxt.SSReformInfo.new_bitmap = g_instance.dms_cxt.SSReformerControl.list_stable;
    ereport(LOG, (errmsg("[SS reform] new cluster node bitmap: %lld", g_instance.dms_cxt.SSReformInfo.new_bitmap)));
    g_instance.dms_cxt.SSReformInfo.reform_end_time = GetCurrentTimestamp();
    g_instance.dms_cxt.SSReformInfo.reform_success = true;
    ereport(LOG,
            (errmodule(MOD_DMS),
                errmsg("[SS reform/SS switchover/SS failover] Reform success, instance:%d is running.",
                       g_instance.attr.attr_storage.dms_attr.instance_id)));

    /* reform success indicates that reform of primary and standby all complete, then update gaussdb.state */
    g_instance.dms_cxt.dms_status = (dms_status_t)DMS_STATUS_IN;
    SendPostmasterSignal(PMSIGNAL_DMS_REFORM_DONE);
    g_instance.dms_cxt.SSClusterState = NODESTATE_NORMAL;
    g_instance.dms_cxt.SSReformInfo.in_reform = false;
    return GS_SUCCESS;
}

static int CBXLogWaitFlush(void *db_handle, unsigned long long lsn)
{
    XLogWaitFlush(lsn);
    return GS_SUCCESS;
}

static int CBDBCheckLock(void *db_handle)
{
    if (t_thrd.storage_cxt.num_held_lwlocks > 0) {
        ereport(PANIC, (errmsg("hold lock, lock address:%p, lock mode:%u",
            t_thrd.storage_cxt.held_lwlocks[0].lock, t_thrd.storage_cxt.held_lwlocks[0].mode)));
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

static int CBCacheMsg(void *db_handle, char* msg)
{
    errno_t rc = memcpy_s(t_thrd.dms_cxt.msg_backup, sizeof(t_thrd.dms_cxt.msg_backup), msg, 
                        sizeof(t_thrd.dms_cxt.msg_backup));
    securec_check(rc, "\0", "\0");
    return GS_SUCCESS;
}

static int CBMarkNeedFlush(void *db_handle, char *pageid, unsigned char *is_edp)
{
    *is_edp = false;
    BufferTag *tag = (BufferTag *)pageid;
    BufferDesc *buf_desc = SSGetBufferDesc(pageid);
    if (buf_desc == NULL) {
        ereport(LOG, (errmodule(MOD_DMS),
            errmsg("[SS][%u/%u/%u/%d %d-%u] CBMarkNeedFlush, buf_desc not found",
                tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
                tag->forkNum, tag->blockNum)));
        return DMS_ERROR;
    }

    dms_buf_ctrl_t *buf_ctrl = GetDmsBufCtrl(buf_desc->buf_id);
    LWLockAcquire((LWLock*)buf_ctrl->ctrl_lock, LW_EXCLUSIVE);
    if (!DMS_BUF_CTRL_IS_OWNER(buf_ctrl)) {
        LWLockRelease((LWLock*)buf_ctrl->ctrl_lock);
        ereport(LOG, (errmodule(MOD_DMS),
            errmsg("[SS][%u/%u/%u/%d %d-%u] CBMarkNeedFlush, do not have current page",
            tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
            tag->forkNum, tag->blockNum)));
        SSUnPinBuffer(buf_desc);
        return DMS_ERROR;
    }
    LWLockRelease((LWLock*)buf_ctrl->ctrl_lock);

#ifdef USE_ASSERT_CHECKING
    if (buf_ctrl->is_edp) {
        ereport(PANIC, (errmsg("[SS][%u/%u/%u/%d %d-%u] CBMarkNeedFlush, do not allow edp exist, please check.",
            tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
            tag->forkNum, tag->blockNum)));
    }
#endif
    ereport(LOG, (errmsg("[SS] CBMarkNeedFlush found buf: %u/%u/%u/%d %d-%u",
        tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode, tag->rnode.bucketNode,
        tag->forkNum, tag->blockNum)));
    SSUnPinBuffer(buf_desc);
    return DMS_SUCCESS;
}

static int CBUpdateNodeOldestXmin(void *db_handle, uint8 inst_id, unsigned long long oldest_xmin)
{
    SSUpdateNodeOldestXmin(inst_id, oldest_xmin);
    return GS_SUCCESS;
}

void DmsCallbackThreadShmemInit(unsigned char need_startup, char **reg_data)
{
    /* in dorado mode, we need to wait sharestorageinit finished */
    while (!g_instance.dms_cxt.SSRecoveryInfo.dorado_sharestorage_inited && SS_DORADO_CLUSTER) {
        pg_usleep(REFORM_WAIT_TIME);
    }
    IsUnderPostmaster = true;
    // to add cnt, avoid postmain execute proc_exit to free shmem now
    (void)pg_atomic_add_fetch_u32(&g_instance.dms_cxt.inDmsThreShmemInitCnt, 1);

    // postmain execute proc_exit now, share mem maybe shdmt, exit this thread now.
    if (pg_atomic_read_u32(&g_instance.dms_cxt.inProcExitCnt) > 0) {
        (void)pg_atomic_sub_fetch_u32(&g_instance.dms_cxt.inDmsThreShmemInitCnt, 1);
        ThreadExitCXX(0);
    }
    EarlyBindingTLSVariables();
    MemoryContextInit();
    knl_thread_init(DMS_WORKER);
    *reg_data = (char *)&t_thrd;
    t_thrd.fake_session = create_session_context(t_thrd.top_mem_cxt, 0);
    t_thrd.fake_session->status = KNL_SESS_FAKE;
    u_sess = t_thrd.fake_session;
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    if (!need_startup) {
        t_thrd.proc_cxt.MyProgName = "DMS WORKER";
        t_thrd.dms_cxt.is_reform_proc = false;
    } else {
        t_thrd.proc_cxt.MyProgName = "DMS REFORM PROC";
        t_thrd.dms_cxt.is_reform_proc = true;
    }
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    SelfMemoryContext = THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT);
    /* memory context will be used by DMS message process functions */
    t_thrd.dms_cxt.msgContext = AllocSetContextCreate(TopMemoryContext,
        "DMSWorkerContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    /* create timer with thread safe */
    if (gs_signal_createtimer() < 0) {
        ereport(FATAL, (errmsg("create timer fail at thread : %lu", t_thrd.proc_cxt.MyProcPid)));
    }
    CreateLocalSysDBCache();
    InitShmemForDmsCallBack();
    Assert(t_thrd.utils_cxt.CurrentResourceOwner == NULL);
    t_thrd.utils_cxt.CurrentResourceOwner =
        ResourceOwnerCreate(NULL, "dms worker", THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    SharedInvalBackendInit(false, false);
    pgstat_initialize();
    u_sess->attr.attr_common.Log_line_prefix = "\%m \%u \%d \%h \%p \%S ";
    log_timezone = g_instance.dms_cxt.log_timezone;
    (void)pg_atomic_sub_fetch_u32(&g_instance.dms_cxt.inDmsThreShmemInitCnt, 1);
    t_thrd.postgres_cxt.whereToSendOutput = (int)DestNone;
}

int CBOndemandRedoPageForStandby(void *block_key, int32 *redo_status)
{
    BufferTag* tag = (BufferTag *)block_key;

    Assert(SS_PRIMARY_MODE);
    // do nothing if not in ondemand recovery
    if (!SS_IN_ONDEMAND_RECOVERY) {
        ereport(DEBUG1, (errmsg("[On-demand] Ignore standby redo page request, spc/db/rel/bucket "
                         "fork-block: %u/%u/%u/%d %d-%u", tag->rnode.spcNode, tag->rnode.dbNode,
                         tag->rnode.relNode, tag->rnode.bucketNode, tag->forkNum, tag->blockNum)));
        *redo_status = ONDEMAND_REDO_SKIP;
        return GS_SUCCESS;;
    }

    Buffer buffer = InvalidBuffer;
    uint32 saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    *redo_status = ONDEMAND_REDO_DONE;
    smgrcloseall();
    PG_TRY();
    {
        buffer = SSReadBuffer(tag, RBM_NORMAL);
        if (BufferIsInvalid(buffer)) {
            *redo_status = ONDEMAND_REDO_FAIL;
        } else {
            ReleaseBuffer(buffer);
        }
    }
    PG_CATCH();
    {
        t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
        /* Save error info */
        ErrorData* edata = CopyErrorData();
        ereport(WARNING,
            (errmsg("[On-demand] Error happend when primary redo page for standby, spc/db/rel/bucket "
                "fork-block: %u/%u/%u/%d %d-%u", tag->rnode.spcNode, tag->rnode.dbNode,
                tag->rnode.relNode, tag->rnode.bucketNode, tag->forkNum, tag->blockNum),
                errdetail("%s", edata->detail)));
        FlushErrorState();
        FreeErrorData(edata);
        *redo_status = ONDEMAND_REDO_ERROR;
    }
    PG_END_TRY();

    ereport(DEBUG1, (errmsg("[On-demand] Redo page for standby done, spc/db/rel/bucket fork-block: %u/%u/%u/%d %d-%u, "
                            "redo status: %d", tag->rnode.spcNode, tag->rnode.dbNode, tag->rnode.relNode,
                            tag->rnode.bucketNode, tag->forkNum, tag->blockNum, *redo_status)));
    return GS_SUCCESS;;
}

void CBGetBufInfo(char* resid, stat_buf_info_t *buf_info)
{
    BufferTag tag;
    errno_t err = memcpy_s(&tag, DMS_RESID_SIZE, resid, DMS_RESID_SIZE);
    securec_check(err, "\0", "\0");
    buftag_get_buf_info(tag, buf_info);
}

static void CBBufCtrlRecycle(void *db_handle)
{
    SSTryEliminateBuf(TRY_ELIMINATE_BUF_TIMES);
}

void DmsThreadDeinit()
{
    proc_exit(0);
}

int CBDoCheckpointImmediately(unsigned long long *ckpt_lsn)
{
    Assert(SS_PRIMARY_MODE);

    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_WAIT);
    *ckpt_lsn = (unsigned long long)t_thrd.shemem_ptr_cxt.ControlFile->checkPoint;
    return GS_SUCCESS;
}

void DmsInitCallback(dms_callback_t *callback)
{
    // used in reform
    callback->get_list_stable = CBGetStableList;
    callback->save_list_stable = CBSaveStableList;
    callback->opengauss_startup = CBStartup;
    callback->opengauss_recovery_standby = CBRecoveryStandby;
    callback->opengauss_recovery_primary = CBRecoveryPrimary;
    callback->get_dms_status = CBGetDmsStatus;
    callback->set_dms_status = CBSetDmsStatus;
    callback->dms_reform_rebuild_parallel = CBDrcBufRebuildParallel;
    callback->dms_thread_init = DmsCallbackThreadShmemInit;
    callback->confirm_owner = CBConfirmOwner;
    callback->confirm_converting = CBConfirmConverting;
    callback->flush_copy = CBFlushCopy;
    callback->get_db_primary_id = CBGetDBPrimaryId;
    callback->failover_promote_opengauss = CBFailoverPromote;
    callback->reform_start_notify = CBReformStartNotify;
    callback->reform_set_dms_role = CBReformSetDmsRole;
    callback->opengauss_ondemand_redo_buffer = CBOndemandRedoPageForStandby;

    callback->inc_and_get_srsn = CBIncAndGetSrsn;
    callback->get_page_hash_val = CBPageHashCode;
    callback->read_local_page4transfer = CBEnterLocalPage;
    callback->leave_local_page = CBLeaveLocalPage;
    callback->page_is_dirty = CBPageDirty;
    callback->get_page = CBGetPage;
    callback->set_buf_load_status = CBSetBufLoadStatus;
    callback->remove_buf_load_status = CBRemoveBufLoadStatus;
    callback->invalidate_page = CBInvalidatePage;
    callback->get_db_handle = CBGetHandle;
    callback->release_db_handle = CBReleaseHandle;
    callback->display_pageid = CBDisplayBufferTag;
    callback->verify_page = CBVerifyPage;

    callback->mem_alloc = CBMemAlloc;
    callback->mem_free = CBMemFree;
    callback->mem_reset = CBMemReset;

    callback->get_page_lsn = CBGetPageLSN;
    callback->get_global_lsn = CBGetGlobalLSN;
    callback->log_flush = CBXLogFlush;
    callback->process_broadcast = CBProcessBroadcast;
    callback->process_broadcast_ack = CBProcessBroadcastAck;

    callback->get_opengauss_xid_csn = CBGetTxnCSN;
    callback->get_opengauss_update_xid = CBGetUpdateXid;
    callback->get_opengauss_txn_status = CBGetTxnStatus;
    callback->opengauss_lock_buffer = CBGetCurrModeAndLockBuffer;
    callback->get_opengauss_txn_snapshot = CBGetSnapshotData;
    callback->get_opengauss_txn_of_master = CBGetTxnSwinfo;
    callback->get_opengauss_page_status = CBGetPageStatus;

    callback->log_output = NULL;

    callback->switchover_demote = CBSwitchoverDemote;
    callback->switchover_promote_opengauss = CBSwitchoverPromote;
    callback->set_switchover_result = CBSwitchoverResult;
    callback->reform_done_notify = CBReformDoneNotify;
    callback->log_wait_flush = CBXLogWaitFlush;
    callback->drc_validate = CBDrcBufValidate;
    callback->db_check_lock = CBDBCheckLock;
    callback->cache_msg = CBCacheMsg;
    callback->need_flush = CBMarkNeedFlush;
    callback->update_node_oldest_xmin = CBUpdateNodeOldestXmin;

    callback->get_buf_info = CBGetBufInfo;
    callback->buf_ctrl_recycle = CBBufCtrlRecycle;
    callback->dms_thread_deinit = DmsThreadDeinit;
    callback->opengauss_do_ckpt_immediate = CBDoCheckpointImmediately;
}
