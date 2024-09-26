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
 * ss_dms_recovery.cpp
 *        Provide common interface for recovery within DMS reform process.
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_dms_recovery.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xlog.h"
#include "access/xact.h"
#include "access/multi_redo_api.h"
#include "storage/standby.h"
#include "storage/pmsignal.h"
#include "storage/buf/bufmgr.h"
#include "storage/dss/fio_dss.h"
#include "storage/smgr/fd.h"
#include "storage/smgr/segment.h"
#include "postmaster/postmaster.h"
#include "storage/file/fio_device.h"
#include "replication/ss_disaster_cluster.h"
#include "replication/walsender_private.h"
#include "ddes/dms/ss_dms_bufmgr.h"
#include "ddes/dms/ss_dms_recovery.h"
#include "ddes/dms/ss_reform_common.h"
#include "ddes/dms/ss_transaction.h"
#include "access/double_write.h"
#include "access/twophase.h"
#include <sys/types.h>
#include <dirent.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "access/xlogproc.h"

int SSGetPrimaryInstId()
{
    return g_instance.dms_cxt.SSReformerControl.primaryInstId;
}

void SSSavePrimaryInstId(int id)
{
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    g_instance.dms_cxt.SSReformerControl.primaryInstId = id;
    SSUpdateReformerCtrl();
    LWLockRelease(ControlFileLock);
}

/**
 * find reform failed in recovery phase, maybe other node restart
 * pageredo or startup thread may trapped in LockBuffer for page request
 * to solve this: reform_proc thread need exit process
 * 
 * reform failed during recovery phase has three situation
 * 1) primary restart 2) restart failover 3) alive failover
 *  
 * 1) primary restart 2) restart failover:
 *      gaussdb will restart
 * 3) alive failover:
 *      try to exit startup thread, 
 *      if success, gaussdb still alive and prepare next reform
 *      if not, gaussdb need exit cause pageredo or startup may trapped in LockBuffer
*/
bool SSRecoveryNodes()
{
    bool result = false;
    
    long max_wait_time = 300000000L;
    long wait_time = 0;
    while (true) {
        /** why use lock:
         * time1 startup thread: update IsRecoveryDone, not finish UpdateControlFile
         * time2 reform_proc: finish reform, think ControlFile is ok
         * time3 DB crash
         * time4 read the checkpoint which created before failover. oops, it is wrong
         */
        LWLockAcquire(ControlFileLock, LW_SHARED);
        if (!SS_DISASTER_MAIN_STANDBY_NODE && t_thrd.shemem_ptr_cxt.XLogCtl->IsRecoveryDone &&
            t_thrd.shemem_ptr_cxt.ControlFile->state == DB_IN_PRODUCTION) {
            LWLockRelease(ControlFileLock);
            result = true;
            break;
        }
        LWLockRelease(ControlFileLock);

        /* do not wait when on-demand HashMap build done */
        if (SS_ONDEMAND_BUILD_DONE) {
            result = true;
            break;
        }

        /* If main standby is set hot standby to on, when it reach consistency or recovery all xlogs in disk,
         * recovery phase could be regarded successful in hot_standby thus set pmState = PM_HOT_STANDBY, which
         * indicate database systerm is ready to accept read only connections.
         */
        if (SS_DISASTER_MAIN_STANDBY_NODE && pmState == PM_HOT_STANDBY) {
            result = true;
            break;
        }

        if (dms_reform_failed()) {
            ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS reform] reform fail in recovery, pmState=%d, SSClusterState=%d,"
                    "demotion=%d-%d, rec=%d", pmState, g_instance.dms_cxt.SSClusterState, g_instance.demotion,
                    t_thrd.walsender_cxt.WalSndCtl->demotion, t_thrd.shemem_ptr_cxt.XLogCtl->IsRecoveryDone)));
            SSWaitStartupExit();
            result = false;
            break;
        }

        if ((wait_time % max_wait_time) == 0 && wait_time != 0) {
            ereport(WARNING, (errmodule(MOD_DMS), errmsg("[SS reform] wait recovery to"
                    " apply done for %ld us.", wait_time)));
        }

        pg_usleep(REFORM_WAIT_TIME);
        wait_time += REFORM_WAIT_TIME;
    }
    ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS reform] recovery success, pmState=%d, SSClusterState=%d, demotion=%d-%d, rec=%d",
                  pmState, g_instance.dms_cxt.SSClusterState, g_instance.demotion,
                  t_thrd.walsender_cxt.WalSndCtl->demotion, t_thrd.shemem_ptr_cxt.XLogCtl->IsRecoveryDone)));
    return result;
}

bool SSRecoveryApplyDelay()
{
    if (SS_DISASTER_STANDBY_CLUSTER) {
        return true;
    }
    if (IsOndemandExtremeRtoMode) {
        OnDemandNotifyHashMapPruneIfNeed();
    }
    while (g_instance.dms_cxt.SSRecoveryInfo.recovery_pause_flag || SS_ONDEMAND_RECOVERY_PAUSE) {
        /* might change the trigger file's location */
        RedoInterruptCallBack();
        pg_usleep(REFORM_WAIT_TIME);
    }

    return true;
}

/* initialize reformer ctrl parameter when initdb */
void SSInitReformerControlPages(void)
{
    /*
     * If already exists control file, reformer page must have been initialized
     */
    struct stat st;
    if (stat(XLOG_CONTROL_FILE, &st) == 0 && S_ISREG(st.st_mode)) {
        SSReadControlFile(REFORM_CTRL_PAGE);
        if (g_instance.dms_cxt.SSReformerControl.primaryInstId == SS_MY_INST_ID) {
            (void)printf("[SS] ERROR: files from last install must be cleared.\n");
            ereport(ERROR, (errmsg("Files from last initdb not cleared")));
        }
        (void)printf("[SS] Current node:%d acknowledges cluster PRIMARY node:%d.\n",
            SS_MY_INST_ID, g_instance.dms_cxt.SSReformerControl.primaryInstId);
        return;
    }

    int fd = -1;
    char buffer[PG_CONTROL_SIZE] __attribute__((__aligned__(ALIGNOF_BUFFER))); /* need to be aligned */
    errno_t errorno = EOK;

    /*
     * Initialize list_stable and primaryInstId
     * First node to initdb is chosen as primary for now, and for first-time cluster startup.
     */
    Assert(stat(XLOG_CONTROL_FILE, &st) != 0 || !S_ISREG(st.st_mode));
    g_instance.dms_cxt.SSReformerControl.list_stable = 0;
    g_instance.dms_cxt.SSReformerControl.primaryInstId = SS_MY_INST_ID;
    g_instance.dms_cxt.SSReformerControl.recoveryInstId = INVALID_INSTANCEID;
    g_instance.dms_cxt.SSReformerControl.version = REFORM_CTRL_VERSION;
    g_instance.dms_cxt.SSReformerControl.clusterStatus = CLUSTER_NORMAL;
    g_instance.dms_cxt.SSReformerControl.clusterRunMode = RUN_MODE_PRIMARY;
    (void)printf("[SS] Current node:%d initdb first, will become PRIMARY for first-time SS cluster startup.\n",
        SS_MY_INST_ID);

    /* Contents are protected with a CRC */
    INIT_CRC32C(g_instance.dms_cxt.SSReformerControl.crc);
    COMP_CRC32C(g_instance.dms_cxt.SSReformerControl.crc, (char *)&g_instance.dms_cxt.SSReformerControl,
                offsetof(ss_reformer_ctrl_t, crc));
    FIN_CRC32C(g_instance.dms_cxt.SSReformerControl.crc);

    if (sizeof(ss_reformer_ctrl_t) > PG_CONTROL_SIZE) {
        ereport(PANIC, (errmsg("[SS] sizeof(ControlFileData) is larger than PG_CONTROL_SIZE; fix either one")));
    }

    errorno = memset_s(buffer, PG_CONTROL_SIZE, 0, PG_CONTROL_SIZE);
    securec_check(errorno, "", "");

    errorno = memcpy_s(buffer, PG_CONTROL_SIZE, &g_instance.dms_cxt.SSReformerControl, sizeof(ss_reformer_ctrl_t));
    securec_check(errorno, "", "");

    fd = BasicOpenFile(XLOG_CONTROL_FILE, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("[SS] could not create control file \"%s\": %m", XLOG_CONTROL_FILE)));
    }

    SSWriteInstanceControlFile(fd, buffer, REFORM_CTRL_PAGE, PG_CONTROL_SIZE);
    if (close(fd)) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("[SS] could not close control file: %m")));
    }
}

void SShandle_promote_signal()
{
    if (pmState == PM_WAIT_BACKENDS) {
        /* 
         * SS_ONDEMAND_REALTIME_BUILD_DISABLED represent 2 conditions
         * 1. SS_PRIMARY_MODE
         * 2. SS_STANDBY_MODE and not ENABLE_ONDEMAND_REALTIME_BUILD
         */
        if (SS_ONDEMAND_REALTIME_BUILD_DISABLED) {
            g_instance.pid_cxt.StartupPID = initialize_util_thread(STARTUP);
        }
        Assert(g_instance.pid_cxt.StartupPID != 0);
        pmState = PM_STARTUP;
    }

    ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS reform][SS failover] initialize startup thread start,"
            "pmState: %d.", pmState)));
}


void ss_failover_dw_init_internal()
{
    /*
     * step 1: remove self dw file dw_exit close self dw
     * step 2: load old primary dw ,and finish dw recovery, exit
     * step 3: rebuild dw file and init self dw
     */

    char *dssdir = g_instance.attr.attr_storage.dss_attr.ss_dss_data_vg_name;
    int old_primary_id = g_instance.dms_cxt.SSReformerControl.primaryInstId;
    int self_id = g_instance.attr.attr_storage.dms_attr.instance_id;
    if (!g_instance.dms_cxt.SSRecoveryInfo.startup_reform) {
        dw_exit(true);
        dw_exit(false);
    }

    dw_exit(true);
    dw_exit(false);
    ss_initdwsubdir(dssdir, old_primary_id);
    dw_ext_init();
    dw_init();
    g_instance.dms_cxt.finishedRecoverOldPrimaryDWFile = true;
    dw_exit(true);
    dw_exit(false);
    ss_initdwsubdir(dssdir, self_id);
    dw_ext_init();
    dw_init();
    g_instance.dms_cxt.finishedRecoverOldPrimaryDWFile = false;
    ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS reform][SS failover] dw init finish")));
}

void ss_failover_dw_init()
{
    for (int i = 0; i < g_instance.ckpt_cxt_ctl->pgwr_procs.num; i++) {
        if (g_instance.pid_cxt.PageWriterPID[i] != 0) {
            signal_child(g_instance.pid_cxt.PageWriterPID[i], SIGTERM, -1);
        }
    }
    ckpt_shutdown_pagewriter();
    ss_failover_dw_init_internal();
    g_instance.dms_cxt.dw_init = true;
}

XLogRecPtr SSOndemandRequestPrimaryCkptAndGetRedoLsn()
{
    XLogRecPtr primaryRedoLsn = InvalidXLogRecPtr;

    ereport(DEBUG1, (errmodule(MOD_DMS),
        errmsg("[SS][On-demand] start request primary node %d do checkpoint", SS_PRIMARY_ID)));
    if (SS_ONDEMAND_REALTIME_BUILD_NORMAL) {
        dms_context_t dms_ctx;
        InitDmsContext(&dms_ctx);
        dms_ctx.xmap_ctx.dest_id = (unsigned int)SS_PRIMARY_ID;
        if (dms_req_opengauss_immediate_checkpoint(&dms_ctx, (unsigned long long *)&primaryRedoLsn) == GS_SUCCESS) {
            ereport(DEBUG1, (errmodule(MOD_DMS),
                errmsg("[SS][On-demand] request primary node %d checkpoint success, redoLoc %X/%X", SS_PRIMARY_ID,
                    (uint32)(primaryRedoLsn << 32), (uint32)primaryRedoLsn)));
            return primaryRedoLsn;
        }
        ereport(DEBUG1, (errmodule(MOD_DMS),
            errmsg("[SS][On-demand] request primary node %d checkpoint failed", SS_PRIMARY_ID)));
    }

    // read from DMS failed, so read from DSS
    SSReadControlFile(SS_PRIMARY_ID, true);
    primaryRedoLsn = g_instance.dms_cxt.ckptRedo;
    ereport(DEBUG1, (errmodule(MOD_DMS),
        errmsg("[SS][On-demand] read primary node %d checkpoint loc in control file, redoLoc %X/%X", SS_PRIMARY_ID,
            (uint32)(primaryRedoLsn << 32), (uint32)primaryRedoLsn)));
    return primaryRedoLsn;
}

void StartupOndemandRecovery()
{
    g_instance.dms_cxt.SSRecoveryInfo.in_ondemand_recovery = true;
    g_instance.dms_cxt.SSRecoveryInfo.cluster_ondemand_status = CLUSTER_IN_ONDEMAND_BUILD;
    /* for other nodes in cluster and ondeamnd recovery failed */
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    g_instance.dms_cxt.SSReformerControl.clusterStatus = CLUSTER_IN_ONDEMAND_BUILD;
    g_instance.dms_cxt.SSReformerControl.recoveryInstId = g_instance.dms_cxt.SSRecoveryInfo.recovery_inst_id;
    SSUpdateReformerCtrl();
    LWLockRelease(ControlFileLock);
    SSRequestAllStandbyReloadReformCtrlPage();
    SetOndemandExtremeRtoMode();
}

void OndemandRealtimeBuildHandleFailover()
{
    Assert(SS_ONDEMAND_REALTIME_BUILD_NORMAL);

    SSReadControlFile(SSGetPrimaryInstId());
    if (u_sess->storage_cxt.pendingOps == NULL) {
        InitSync();
        ss_failover_dw_init();
        EnableSyncRequestForwarding();
    } else {
        ss_failover_dw_init();
    }
    StartupOndemandRecovery();
    StartupReplicationSlots();
    restoreTwoPhaseData();
    OnDemandUpdateRealtimeBuildPrunePtr();
    SendPostmasterSignal(PMSIGNAL_RECOVERY_STARTED);
    while (g_instance.dms_cxt.SSRecoveryInfo.recovery_pause_flag) {
        pg_usleep(REFORM_WAIT_TIME);
    }
    g_instance.dms_cxt.SSRecoveryInfo.ondemand_realtime_build_status = BUILD_TO_REDO;
    ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS][On-demand] Node:%d receive failover signal, "
        "close realtime build and start ondemand build, set status to BUILD_TO_REDO.", SS_MY_INST_ID)));
}

XLogRedoAction SSCheckInitPageXLog(XLogReaderState *record, uint8 block_id, RedoBufferInfo *redo_buf)
{
    if (!ENABLE_DMS || SS_DISASTER_STANDBY_CLUSTER) {
        return BLK_NEEDS_REDO;
    }

    XLogRedoAction action = BLK_NEEDS_REDO;
    RedoBufferTag blockinfo;
    if (!XLogRecGetBlockTag(record, block_id, &(blockinfo.rnode), &(blockinfo.forknum), &(blockinfo.blkno),
        &(blockinfo.pblk))) {
            ereport(PANIC, (errmsg("[SS redo] failed to locate backup block with ID %d.", block_id)));
    }
    bool skip = false;
    char pageid[DMS_PAGEID_SIZE] = {0};
    errno_t rc = memcpy_s(pageid, DMS_PAGEID_SIZE, &blockinfo, sizeof(BufferTag));
    securec_check(rc, "\0", "\0");
    dms_recovery_page_need_skip(pageid, record->EndRecPtr, (unsigned char*)&skip);

    if (skip) {
        XLogPhyBlock *pblk = (blockinfo.pblk.relNode != InvalidOid) ? &blockinfo.pblk : NULL;
        bool tde = false;
        if (record->isTde) {
            tde = InsertTdeInfoToCache(blockinfo.rnode, record->blocks[block_id].tdeinfo);
        }
        Buffer buf = XLogReadBufferExtended(blockinfo.rnode, blockinfo.forknum, blockinfo.blkno,
            RBM_NORMAL_NO_LOG, pblk, tde);
        if (BufferIsInvalid(buf)) {
            ereport(PANIC, (errmsg("[SS redo][%u/%u/%u/%d %d-%u] buffer should be found.",
                blockinfo.rnode.spcNode, blockinfo.rnode.dbNode, blockinfo.rnode.relNode,
                blockinfo.rnode.bucketNode, blockinfo.forknum, blockinfo.blkno))); 
        }
        redo_buf->buf = buf;
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        SSCheckBufferIfNeedMarkDirty(redo_buf->buf);
        action = BLK_DONE;
    }
    return action;
}

XLogRedoAction SSCheckInitPageXLogSimple(XLogReaderState *record, uint8 block_id, RedoBufferInfo *redo_buf)
{
    XLogRedoAction action = SSCheckInitPageXLog(record, block_id, redo_buf);
    if (action == BLK_DONE) {
        if (IsSegmentBufferID(redo_buf->buf - 1)) {
            SegUnlockReleaseBuffer(redo_buf->buf);
        } else {
            UnlockReleaseBuffer(redo_buf->buf);
        }
    }
    return action;
}

// used for extreme recovery and on-demand recovery
bool SSPageReplayNeedSkip(RedoBufferInfo *bufferinfo, XLogRecPtr xlogLsn, XLogRecPtr *pageLsn)
{
    if (!ENABLE_DMS || SS_DISASTER_STANDBY_CLUSTER) {
        return BLK_NEEDS_REDO;
    }

    RedoBufferTag *blockinfo = &bufferinfo->blockinfo;
    BufferTag tag;
    tag.rnode.spcNode = blockinfo->rnode.spcNode;
    tag.rnode.dbNode = blockinfo->rnode.dbNode;
    tag.rnode.relNode = blockinfo->rnode.relNode;
    tag.rnode.bucketNode = blockinfo->rnode.bucketNode;
    tag.rnode.opt = blockinfo->rnode.opt;
    tag.forkNum = blockinfo->forknum;
    tag.blockNum = blockinfo->blkno;

    uint32 hash = BufTableHashCode(&tag);
    bool valid = false;
    bool retry = false;
    int buf_id;
    BufferDesc *buf_desc;
    do {
        buf_id = BufTableLookup(&tag, hash);
        if (buf_id < 0) {
            return false;
        }

        buf_desc = GetBufferDescriptor(buf_id);
        valid = SSPinBuffer(buf_desc);
        if (!BUFFERTAGS_PTR_EQUAL(&buf_desc->tag, &tag)) {
            SSUnPinBuffer(buf_desc);
            retry = true;
        } else {
            break;
        }
    } while (retry);

    if (!valid) {
        SSUnPinBuffer(buf_desc);
        return false;
    }
    
    LWLockAcquire(buf_desc->content_lock, LW_SHARED);
    dms_buf_ctrl_t *buf_ctrl = GetDmsBufCtrl(buf_id);
    if (DMS_BUF_CTRL_IS_OWNER(buf_ctrl)) {
        Buffer buf = buf_id + 1;
        Page page = BufferGetPage(buf);
        *pageLsn = PageGetLSN(page);

        if (buf_ctrl->state & BUF_DIRTY_NEED_FLUSH && XLByteLT(*pageLsn, xlogLsn)) {
            ereport(PANIC, (errmodule(MOD_DMS), errmsg("[SS redo][%u/%u/%u/%d %d-%u] page should be newest but not, "
                    "xlogLsn:%lu, pageLsn:%lu",
                    blockinfo->rnode.spcNode, blockinfo->rnode.dbNode, blockinfo->rnode.relNode,
                    blockinfo->rnode.bucketNode, blockinfo->forknum, blockinfo->blkno,
                    xlogLsn, *pageLsn)));
        }

        if (XLByteLE(xlogLsn, *pageLsn)) {
            bufferinfo->buf = buf;
            bufferinfo->lsn = xlogLsn;
            bufferinfo->pageinfo.page = page;
            bufferinfo->pageinfo.pagesize = BufferGetPageSize(buf);
#ifdef USE_ASSERT_CHECKING
            ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS redo][%u/%u/%u/%d %d-%u] page skip replay, "
                "xlogLsn:%lu, pageLsn:%lu",
                blockinfo->rnode.spcNode, blockinfo->rnode.dbNode, blockinfo->rnode.relNode,
                blockinfo->rnode.bucketNode, blockinfo->forknum, blockinfo->blkno,
                xlogLsn, *pageLsn)));
#endif
            // do not release content_lock
            return true;
        }
    }

    LWLockRelease(buf_desc->content_lock);
    SSUnPinBuffer(buf_desc);
    return false;
}