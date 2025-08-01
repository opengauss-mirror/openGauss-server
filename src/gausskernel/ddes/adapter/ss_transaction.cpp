/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * ss_transaction.cpp
 *  ss transaction related
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_transaction.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "utils/snapshot.h"
#include "utils/postinit.h"
#include "utils/knl_globalsysdbcache.h"
#include "storage/procarray.h"
#include "storage/buf/bufmgr.h"
#include "storage/smgr/segment_internal.h"
#include "access/multi_redo_api.h"
#include "ddes/dms/ss_transaction.h"
#include "ddes/dms/ss_reform_common.h"
#include "ddes/dms/ss_dms_bufmgr.h"
#include "storage/sinvaladt.h"
#include "replication/libpqsw.h"
#include "replication/walsender.h"
#include "replication/ss_disaster_cluster.h"
#ifdef ENABLE_HTAP
#include "access/htap/imcs_hash_table.h"
#include "access/htap/imcs_ctlg.h"
#include "access/htap/ss_imcucache_mgr.h"
#endif

static inline void txnstatusNetworkStats(uint64 timeDiff);
static inline void txnstatusHashStats(uint64 timeDiff);

#define SYNC_TRY_COUNT 3
#define TxnStatusCalcStats(startTime, endTime, timeDiff, isHash) \
 do {                                                            \
    (void)INSTR_TIME_SET_CURRENT(endTime);                       \
    INSTR_TIME_SUBTRACT(endTime, startTime);                     \
    timeDiff = INSTR_TIME_GET_MICROSEC(endTime);                 \
    if (isHash) {                                                \
        txnstatusHashStats((uint64)timeDiff);                    \
    } else {                                                     \
        txnstatusNetworkStats((uint64)timeDiff);                 \
    }                                                            \
 } while (0)

void SSStandbyGlobalInvalidSharedInvalidMessages(const SharedInvalidationMessage* msg, Oid tsid);

static Snapshot SSGetSnapshotDataFromMaster(Snapshot snapshot)
{
    dms_opengauss_txn_snapshot_t dms_snapshot;
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);

    do {
        dms_ctx.xmap_ctx.dest_id = (unsigned int)SS_PRIMARY_ID;
        if (dms_request_opengauss_txn_snapshot(&dms_ctx, &dms_snapshot) == DMS_SUCCESS) {
            break;
        }

        if (AM_WAL_SENDER && SS_IN_REFORM) {
            return NULL;
        }

        if (SSBackendNeedExitScenario() && SS_AM_WORKER) {
            return NULL;
        }
        if (SS_IN_FAILOVER && t_thrd.role == TRACK_STMT_WORKER) {
            return NULL;
        }
        pg_usleep(USECS_PER_SEC);

    } while (true);

    snapshot->xmin = dms_snapshot.xmin;
    snapshot->xmax = dms_snapshot.xmax;
    snapshot->snapshotcsn = dms_snapshot.snapshotcsn;
    if (ENABLE_SS_BCAST_SNAPSHOT) {
        t_thrd.dms_cxt.latest_snapshot_xmin = dms_snapshot.xmin;
        t_thrd.dms_cxt.latest_snapshot_csn = dms_snapshot.snapshotcsn;
        t_thrd.dms_cxt.latest_snapshot_xmax = dms_snapshot.xmax;
        ereport(DEBUG1,
            (errmsg("Get Local snapshot from master, xmin/xmax/csn:%lu/%lu/%lu", t_thrd.dms_cxt.latest_snapshot_xmin,
            t_thrd.dms_cxt.latest_snapshot_xmax, t_thrd.dms_cxt.latest_snapshot_csn)));
    }
    if (!TransactionIdIsValid(t_thrd.pgxact->xmin)) {
        t_thrd.pgxact->xmin = u_sess->utils_cxt.TransactionXmin = snapshot->xmin;
    }

    if (!TransactionIdIsNormal(dms_snapshot.localxmin)) {
        u_sess->utils_cxt.RecentGlobalXmin = FirstNormalTransactionId;
    } else {
        u_sess->utils_cxt.RecentGlobalXmin = dms_snapshot.localxmin;
    }

    u_sess->utils_cxt.RecentGlobalDataXmin = u_sess->utils_cxt.RecentGlobalXmin;
    u_sess->utils_cxt.RecentXmin = snapshot->xmin;
    return snapshot;
}

Snapshot SSGetSnapshotData(Snapshot snapshot)
{
    /* For cm agent, it only query the system status using the parameter in memory. So don't need MVCC */
    if (u_sess->libpq_cxt.IsConnFromCmAgent) {
        if (SS_IN_REFORM && !SSPerformingStandbyScenario()) {
            ereport(ERROR, (errmsg("failed to request snapshot as current node is in reform.")));
        }

        snapshot = SnapshotNow;
        if (!TransactionIdIsNormal(u_sess->utils_cxt.RecentGlobalXmin)) {
            u_sess->utils_cxt.RecentGlobalXmin = FirstNormalTransactionId;
        }
        u_sess->utils_cxt.RecentGlobalDataXmin = u_sess->utils_cxt.RecentGlobalXmin;
        return snapshot;
    }

    if (!ENABLE_SS_BCAST_SNAPSHOT ||
        (g_instance.dms_cxt.latest_snapshot_xmax == InvalidTransactionId &&
        t_thrd.dms_cxt.latest_snapshot_xmax == InvalidTransactionId)) {
        return SSGetSnapshotDataFromMaster(snapshot);
    } else if (TransactionIdPrecedes(t_thrd.dms_cxt.latest_snapshot_xmax, g_instance.dms_cxt.latest_snapshot_xmax)) {
        SpinLockAcquire(&g_instance.dms_cxt.set_snapshot_mutex);
        t_thrd.dms_cxt.latest_snapshot_xmin = g_instance.dms_cxt.latest_snapshot_xmin;
        t_thrd.dms_cxt.latest_snapshot_csn = g_instance.dms_cxt.latest_snapshot_csn;
        t_thrd.dms_cxt.latest_snapshot_xmax = g_instance.dms_cxt.latest_snapshot_xmax;
        SpinLockRelease(&g_instance.dms_cxt.set_snapshot_mutex);
        ereport(DEBUG1,
            (errmsg("Update Local snapshot from global, xmin/xmax/csn:%lu/%lu/%lu", t_thrd.dms_cxt.latest_snapshot_xmin,
            t_thrd.dms_cxt.latest_snapshot_xmax, t_thrd.dms_cxt.latest_snapshot_csn)));
    }

    Assert(t_thrd.dms_cxt.latest_snapshot_xmax != InvalidTransactionId);
    if (g_instance.dms_cxt.latest_snapshot_xmax == InvalidTransactionId) {
        return SSGetSnapshotDataFromMaster(snapshot);
    }
    snapshot->xmin = t_thrd.dms_cxt.latest_snapshot_xmin;
    snapshot->xmax = t_thrd.dms_cxt.latest_snapshot_xmax;
    snapshot->snapshotcsn = t_thrd.dms_cxt.latest_snapshot_csn;
    ereport(DEBUG1, (errmsg("Current Use snapshot, xmin/xmax/csn:%lu/%lu/%lu", snapshot->xmin,
        snapshot->xmax, snapshot->snapshotcsn)));
    if (!TransactionIdIsValid(t_thrd.pgxact->xmin)) {
        t_thrd.pgxact->xmin = u_sess->utils_cxt.TransactionXmin = snapshot->xmin;
    }

    if (!TransactionIdIsNormal(u_sess->utils_cxt.RecentGlobalXmin)) {
        u_sess->utils_cxt.RecentGlobalXmin = FirstNormalTransactionId;
    }
    u_sess->utils_cxt.RecentGlobalDataXmin = u_sess->utils_cxt.RecentGlobalXmin;
    u_sess->utils_cxt.RecentXmin = snapshot->xmin;
    return snapshot;
}

int SSUpdateLatestSnapshotOfStandby(char *data, uint32 len, char *output_msg, uint32 *output_msg_len)
{
    if (unlikely(len != sizeof(SSBroadcastSnapshot))) {
        ereport(DEBUG1, (errmsg("invalid broadcast set snapshot message")));
        return DMS_ERROR;
    }

    if (ENABLE_SS_BCAST_GETOLDESTXMIN && output_msg != NULL && output_msg_len != NULL) {
        SSBroadcastXminAck* getXminReq = (SSBroadcastXminAck *)output_msg;
        getXminReq->type = BCAST_GET_XMIN_ACK;
        GetOldestGlobalProcXmin(&(getXminReq->xmin));
        *output_msg_len = sizeof(SSBroadcastXminAck);
    }

    SSBroadcastSnapshot *received_data = (SSBroadcastSnapshot *)data;
    if (TransactionIdPrecedes(received_data->xmax, g_instance.dms_cxt.latest_snapshot_xmax)) {
        ereport(WARNING, (errmsg("Receive oldest one, can't update:%lu/%lu", received_data->xmin,
            g_instance.dms_cxt.latest_snapshot_xmax)));
        return DMS_SUCCESS;
    }
    SpinLockAcquire(&g_instance.dms_cxt.set_snapshot_mutex);
    g_instance.dms_cxt.latest_snapshot_xmin = received_data->xmin;
    g_instance.dms_cxt.latest_snapshot_csn = received_data->csn;
    g_instance.dms_cxt.latest_snapshot_xmax = received_data->xmax;
    SpinLockRelease(&g_instance.dms_cxt.set_snapshot_mutex);
    ereport(DEBUG1, (errmsg("Receive and set global snapshot xmin/xmax/csn:%lu/%lu/%lu", received_data->xmin,
        received_data->xmax, received_data->csn)));
    return DMS_SUCCESS;
}

static int SSTransactionIdGetCSN(dms_opengauss_xid_csn_t *dms_txn_info, dms_opengauss_csn_result_t *xid_csn_result)
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    dms_ctx.xid_ctx.inst_id = (unsigned char)SS_PRIMARY_ID;

    return dms_request_opengauss_xid_csn(&dms_ctx, dms_txn_info, xid_csn_result);
}

static inline void txnstatusNetworkStats(uint64 timeDiff)
{
    ereport(DEBUG1, (errmodule(MOD_SS_TXNSTATUS),
            errmsg("SSTxnStatusCache niotimediff=%luus", timeDiff)));
    g_instance.dms_cxt.SSDFxStats.txnstatus_network_io_gets++;
    g_instance.dms_cxt.SSDFxStats.txnstatus_total_niogets_time += timeDiff;
}

static inline void txnstatusHashStats(uint64 timeDiff)
{
    ereport(DEBUG1, (errmodule(MOD_SS_TXNSTATUS),
            errmsg("SSTxnStatusCache hashtimediff=%luus", timeDiff)));
    g_instance.dms_cxt.SSDFxStats.txnstatus_hashcache_gets++;
    g_instance.dms_cxt.SSDFxStats.txnstatus_total_hcgets_time += timeDiff;
}

/*
 * Use TxnStatusCache regardless of the snapshot might result in erroneous visibility check.
 * trustFrozen: whether frozen CSN can be used to determine visibility. if xid > snapshotxid,
 * then still fetch from remote clogstatus cache. is clogstatus the most accurate?
 */
static inline bool SnapshotSatisfiesTSC(TransactionId transactionId, Snapshot snapshot,
    bool isMvcc, bool *trustFrozen)
{
    if (snapshot == NULL || !isMvcc) {
        return true;
    } else {
        if (IsVersionMVCCSnapshot(snapshot) || (snapshot->satisfies == SNAPSHOT_DECODE_MVCC)) {
            return false;
        }
        if (IsMVCCSnapshot(snapshot) && !TransactionIdPrecedes(transactionId, snapshot->xmin)) {
            *trustFrozen = false;
        }
    }
    return true;
}

static inline bool IsCommitSeqNoDefinitive(CommitSeqNo csn)
{
    return (COMMITSEQNO_IS_ABORTED(csn) || COMMITSEQNO_IS_COMMITTED(csn));
}

static inline bool IsClogStatusDefinitive(CLogXidStatus status)
{
    return (status != CLOG_XID_STATUS_IN_PROGRESS && status != CLOG_XID_STATUS_SUB_COMMITTED);
}

/*
 * xid -> csnlog status
 * is_committed: if true, then no need to fetch xid status from clog
 */
CommitSeqNo SSTransactionIdGetCommitSeqNo(TransactionId transactionId, bool isCommit, bool isMvcc, bool isNest,
    Snapshot snapshot, bool* sync)
{
    instr_time startTime;
    instr_time endTime;
    PgStat_Counter timeDiff = 0;
    (void)INSTR_TIME_SET_CURRENT(startTime);

    if ((snapshot == NULL || !IsVersionMVCCSnapshot(snapshot)) &&
        TransactionIdEquals(transactionId, t_thrd.xact_cxt.cachedFetchCSNXid)) {
        g_instance.dms_cxt.SSDFxStats.txnstatus_varcache_gets++;
        t_thrd.xact_cxt.latestFetchCSNXid = t_thrd.xact_cxt.cachedFetchCSNXid;
        t_thrd.xact_cxt.latestFetchCSN = t_thrd.xact_cxt.cachedFetchCSN;
        return t_thrd.xact_cxt.cachedFetchCSN;
    }
    if (!TransactionIdIsNormal(transactionId)) {
        g_instance.dms_cxt.SSDFxStats.txnstatus_varcache_gets++;
        t_thrd.xact_cxt.latestFetchCSNXid = InvalidTransactionId;
        if (TransactionIdEquals(transactionId, BootstrapTransactionId) ||
            TransactionIdEquals(transactionId, FrozenTransactionId)) {
            return COMMITSEQNO_FROZEN;
        }
        return COMMITSEQNO_ABORTED;
    }

    CommitSeqNo cachedCSN = InvalidCommitSeqNo;
    bool trustFrozen = false;
    if (ENABLE_SS_TXNSTATUS_CACHE && SnapshotSatisfiesTSC(transactionId, snapshot, isMvcc, &trustFrozen)) {
        bool cached = false;
        uint32 hashcode = XidHashCode(&transactionId);
        cached = TxnStatusCacheLookup(&transactionId, hashcode, &cachedCSN);
        if (cached) {
            Assert(IsCommitSeqNoDefinitive(cachedCSN));
            if (!COMMITSEQNO_IS_FROZEN(cachedCSN) || trustFrozen) {
                TxnStatusCalcStats(startTime, endTime, timeDiff, true);
#ifndef USE_ASSERT_CHECKING
                return cachedCSN;
#endif
            }
        }
    }

    CommitSeqNo csn = 0; // COMMITSEQNO_INPROGRESS by default
    CLogXidStatus clogstatus = CLOG_XID_STATUS_IN_PROGRESS;
    XLogRecPtr lsn = InvalidXLogRecPtr;
    dms_opengauss_csn_result_t xid_csn_result = { 0 };
    dms_opengauss_xid_csn_t dms_txn_info;
    dms_txn_info.xid = transactionId;
    dms_txn_info.is_committed = (unsigned char)isCommit;
    dms_txn_info.is_mvcc = (unsigned char)isMvcc;
    dms_txn_info.is_nest = (unsigned char)isNest;
    if (snapshot != NULL) {
        dms_txn_info.snapshotcsn = snapshot->snapshotcsn;
        dms_txn_info.snapshotxmin = snapshot->xmin;
    } else {
        dms_txn_info.snapshotcsn = InvalidCommitSeqNo;
        dms_txn_info.snapshotxmin = InvalidTransactionId;
    }

    if (SS_IN_REFORM && SSBackendNeedExitScenario() && SS_AM_WORKER) {
        ereport(ERROR, (errmsg("SSTransactionIdGetCommitSeqNo failed during reform, xid=%lu.", transactionId)));
    }

    do {
        if (SSTransactionIdGetCSN(&dms_txn_info, &xid_csn_result) == DMS_SUCCESS) {
            csn = xid_csn_result.csn;
            clogstatus = (int)xid_csn_result.clogstatus;
            lsn = xid_csn_result.lsn;
            if (sync != NULL && (bool)xid_csn_result.sync) {
                *sync = (bool)xid_csn_result.sync;
                ereport(DEBUG1, (errmsg("SS primary xid sync success, xid=%lu.", transactionId)));
            }
            if (snapshot != NULL) {
                ereport(DEBUG1, (errmsg("SS get txn info success, xid=%lu, snapshot=%lu-%lu-%lu, csn=%lu.", transactionId,
                    snapshot->xmin, snapshot->xmax, snapshot->snapshotcsn, csn)));
            } else {
                ereport(DEBUG1, (errmsg("SS get txn info success, snapshot is NULL")));
            }
            break;
        } else {
            if (SS_IN_REFORM && SSBackendNeedExitScenario() && SS_AM_WORKER) {
                ereport(FATAL, (errmsg("SSTransactionIdGetCommitSeqNo failed during reform, xid=%lu.", transactionId)));
            }
            pg_usleep(USECS_PER_SEC);
            continue;
        }
    } while (true);

#ifdef USE_ASSERT_CHECKING
    /* DEBUG mode validates cache-networkIO consistency before returning cache */
    if (cachedCSN != InvalidCommitSeqNo && (!COMMITSEQNO_IS_FROZEN(cachedCSN) || trustFrozen)) {
        Assert(((COMMITSEQNO_IS_COMMITTED(csn) && COMMITSEQNO_IS_COMMITTED(cachedCSN))) ||
            (COMMITSEQNO_IS_ABORTED(csn) && COMMITSEQNO_IS_ABORTED(cachedCSN)));
        return cachedCSN;
    }
#endif

    if (IsCommitSeqNoDefinitive(csn)) {
        t_thrd.xact_cxt.cachedFetchCSNXid = transactionId;
        t_thrd.xact_cxt.cachedFetchCSN = csn;

        if (ENABLE_SS_TXNSTATUS_CACHE && IsClogStatusDefinitive(clogstatus)) {
            /* clogstat might be in-progress, therefore we want to be discreet */
            uint32 hashcode = XidHashCode(&transactionId);
            LWLock *partitionLock = TxnStatusCachePartitionLock(hashcode);
            LWLockAcquire(partitionLock, LW_EXCLUSIVE);
            int ret = TxnStatusCacheInsert(&transactionId, hashcode, csn, clogstatus);
            if (ret != GS_SUCCESS) {
                ereport(PANIC, (errmsg("SSTxnStatusCache insert failed, xid=%lu.", transactionId)));
            }
            LWLockRelease(partitionLock);
        }
    }

    if (IsClogStatusDefinitive(clogstatus)) {
        t_thrd.xact_cxt.cachedFetchXid = transactionId;
        t_thrd.xact_cxt.cachedFetchXidStatus = clogstatus;
        t_thrd.xact_cxt.cachedCommitLSN = lsn;
    }

    TxnStatusCalcStats(startTime, endTime, timeDiff, false);
    return csn;
}

/*
 * xid -> clog status
 * true if given transaction committed
 */
void SSTransactionIdDidCommit(TransactionId transactionId, bool* ret_did_commit)
{
    bool did_commit = false;
    bool remote_get = false;

    if (TransactionIdEquals(transactionId, t_thrd.xact_cxt.cachedFetchXid)) {
        t_thrd.xact_cxt.latestFetchXid = t_thrd.xact_cxt.cachedFetchXid;
        t_thrd.xact_cxt.latestFetchXidStatus = t_thrd.xact_cxt.cachedFetchXidStatus;
        if (t_thrd.xact_cxt.cachedFetchXidStatus == CLOG_XID_STATUS_COMMITTED)
            did_commit = true;
    }

    if (!TransactionIdIsNormal(transactionId)) {
        t_thrd.xact_cxt.latestFetchXid = InvalidTransactionId;
        if (TransactionIdEquals(transactionId, BootstrapTransactionId)) {
            did_commit = true;
        } else if (TransactionIdEquals(transactionId, FrozenTransactionId)) {
            did_commit = true;
        }
    }

    if (!did_commit) {
        dms_context_t dms_ctx;
        InitDmsContext(&dms_ctx);
        dms_ctx.xid_ctx.xid = *(uint64 *)(&transactionId);

        do {
            dms_ctx.xid_ctx.inst_id = (unsigned char)SS_PRIMARY_ID;
            if (dms_request_opengauss_txn_status(&dms_ctx, (uint8)XID_COMMITTED, (uint8 *)&did_commit)
                == DMS_SUCCESS) {
                remote_get = true;
                ereport(DEBUG1,
                    (errmsg("SS get txn did_commit success, xid=%lu, did_commit=%d.",
                        transactionId, did_commit)));
                break;
            } else {
                if (SS_IN_REFORM && SSBackendNeedExitScenario() && SS_AM_WORKER) {
                    ereport(FATAL, (errmsg("SSTransactionIdDidCommit failed during reform, xid=%lu.", transactionId)));
                }
                pg_usleep(USECS_PER_SEC);
                continue;
            }
        } while (true);
    }

    if (did_commit && remote_get) {
        t_thrd.xact_cxt.cachedFetchXid = transactionId;
        t_thrd.xact_cxt.cachedFetchXidStatus = CLOG_XID_STATUS_COMMITTED;
        t_thrd.xact_cxt.latestFetchXid = transactionId;
        t_thrd.xact_cxt.latestFetchXidStatus = CLOG_XID_STATUS_COMMITTED;
    }
    *ret_did_commit = did_commit;
}

/* xid -> clog status */
/* true if given transaction in progress */
void SSTransactionIdIsInProgress(TransactionId transactionId, bool *in_progress)
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    dms_ctx.xid_ctx.xid = *(uint64 *)(&transactionId);

    do {
        dms_ctx.xid_ctx.inst_id = (unsigned char)SS_PRIMARY_ID;
        if (dms_request_opengauss_txn_status(&dms_ctx, (uint8)XID_INPROGRESS, (uint8 *)in_progress) == DMS_SUCCESS) {
            ereport(DEBUG1, (errmsg("SS get txn in_progress success, xid=%lu, in_progress=%d.",
                transactionId, *in_progress)));
            break;
        } else {
            if (SS_IN_REFORM && SSBackendNeedExitScenario() && SS_AM_WORKER) {
                ereport(FATAL, (errmsg("SSTransactionIdIsInProgress failed during reform, xid=%lu.", transactionId)));
            }
            pg_usleep(USECS_PER_SEC);
            continue;
        }
    } while (true);
}

TransactionId SSMultiXactIdGetUpdateXid(TransactionId xmax, uint16 t_infomask, uint16 t_infomask2)
{
    TransactionId update_xid;
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);

    dms_ctx.xid_ctx.xid = *(uint64 *)(&xmax);
    dms_ctx.xid_ctx.inst_id = (unsigned char)SS_PRIMARY_ID;

    do {
        if (dms_request_opengauss_update_xid(&dms_ctx, t_infomask, t_infomask2, (unsigned long long *)&update_xid)
            == DMS_SUCCESS) {
            ereport(DEBUG1, (errmsg("SS get update xid success, multixact xid=%lu, uxid=%lu.", xmax, update_xid)));
            break;
        } else {
            if (SS_IN_REFORM && SSBackendNeedExitScenario() && SS_AM_WORKER) {
                ereport(FATAL, (errmsg("SSMultiXactIdGetUpdateXid failed during reform, xid=%lu.", xmax)));
            }
            pg_usleep(USECS_PER_SEC);
            continue;
        }
    } while (true);

    return update_xid;
}

void SSSendLatestSnapshotToStandby(TransactionId xmin, TransactionId xmax, CommitSeqNo csn)
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    int ret;
    SSBroadcastSnapshot latest_snapshot;
    latest_snapshot.xmin = xmin;
    latest_snapshot.xmax = xmax;
    latest_snapshot.csn = csn;
    latest_snapshot.type = BCAST_SEND_SNAPSHOT;
    dms_broadcast_info_t dms_broad_info = {
        .data = (char *)&latest_snapshot,
        .len = sizeof(SSBroadcastSnapshot),
        .output = NULL,
        .output_len = NULL,
        .scope = DMS_BROADCAST_ONLINE_LIST,
        .inst_map = 0,
        .timeout = SS_BROADCAST_WAIT_ONE_SECOND,
        .handle_recv_msg = (unsigned char)false,
        .check_session_kill = (unsigned char)true
    };
    do {
        ret = dms_broadcast_msg(&dms_ctx, &dms_broad_info);

        if (ret == DMS_SUCCESS) {
            return;
        }

        pg_usleep(5000L);
    } while (ret != DMS_SUCCESS);
}

void SSIsPageHitDms(RelFileNode& node, BlockNumber page, int pagesNum, uint64 *pageMap, int *bitCount)
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);

    dms_ctx.rfn.inst_id = (unsigned char)SS_PRIMARY_ID;
    dms_ctx.rfn.rnode = *(dms_opengauss_relfilenode_t *)(&node);

    if (dms_request_opengauss_page_status(&dms_ctx, page, pagesNum, pageMap, bitCount) != DMS_SUCCESS) {
        *bitCount = 0;
        ereport(DEBUG1, (errmsg("SS get page map failed, buffer_id = %u.", page)));
        return;
    }
    ereport(DEBUG1, (errmsg("SS get page map success, buffer_id = %u.", page)));
}

int SSReloadReformCtrlPage(uint32 len)
{
    if (unlikely(len != sizeof(SSBroadcastCmdOnly))) {
        return DMS_ERROR;
    }

    SSReadReformerCtrl();
    return DMS_SUCCESS;
}

int SSCheckDbBackends(char *data, uint32 len, char *output_msg, uint32 *output_msg_len)
{
    if (unlikely(len != sizeof(SSBroadcastDbBackends))) {
        return DMS_ERROR;
    }

    SSBroadcastDbBackends *checkDbBackendsMsg = (SSBroadcastDbBackends *)data;
    SSBroadcastDbBackendsAck *checkDbBackendsReq = (SSBroadcasDbBackendsAck *)output_msg;
    int notherbackends, npreparedxacts;

    (void)CountOtherDBBackends(checkDbBackendsMsg->dbid, &notherbackends, &npreparedxacts);

    checkDbBackendsReq->type = BCAST_CHECK_DB_BACKENDS_ACK;
    checkDbBackendsReq->count = notherbackends + npreparedxacts;
    *output_msg_len = sizeof(SSBroadcastDbBackendsAck);
    return DMS_SUCCESS;
}

int SSCheckDbBackendsAck(char *data, unsigned int len)
{
    SSBroadcastDbBackendsAck *ack_data = (SSBroadcasDbBackendsAck *)data;

    if (len != sizeof(SSBroadcastDbBackendsAck)) {
        ereport(WARNING, (errmsg("SS get check other db backends failed.")));
        return DMS_ERROR;
    }

    // Is other backends running in the given DB?
    if (ack_data->count != 0) {
        return DMS_EXIST_RUNNING_BACKENDS;
    }

    return DMS_NO_RUNNING_BACKENDS;
}

bool SSCheckDbBackendsFromAllStandby(Oid dbid)
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    SSBroadcastDbBackends backends_data;
    backends_data.type = BCAST_CHECK_DB_BACKENDS;
    backends_data.dbid = dbid;
    dms_broadcast_info_t dms_broad_info = {
        .data = (char *)&backends_data,
        .len = sizeof(SSBroadcastDbBackends),
        .output = NULL,
        .output_len = NULL,
        .scope = DMS_BROADCAST_ONLINE_LIST,
        .inst_map = 0,
        .timeout = SS_BROADCAST_WAIT_FIVE_SECONDS,
        .handle_recv_msg = (unsigned char)true,
        .check_session_kill = (unsigned char)true
    };

    int ret = dms_broadcast_msg(&dms_ctx, &dms_broad_info);
    if (ret != DMS_NO_RUNNING_BACKENDS) {
        return true;
    }
    return false;
}

void SSRequestAllStandbyReloadReformCtrlPage()
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    int ret;
    SSBroadcastCmdOnly ssmsg;
    ssmsg.type = BCAST_RELOAD_REFORM_CTRL_PAGE;
    dms_broadcast_info_t dms_broad_info = {
        .data = (char *)&ssmsg,
        .len = sizeof(SSBroadcastCmdOnly),
        .output = NULL,
        .output_len = NULL,
        .scope = DMS_BROADCAST_ONLINE_LIST,
        .inst_map = 0,
        .timeout = SS_BROADCAST_WAIT_ONE_SECOND,
        .handle_recv_msg = (unsigned char)false,
        .check_session_kill = (unsigned char)true
    };
    ret = dms_broadcast_msg(&dms_ctx, &dms_broad_info);
    if (ret != DMS_SUCCESS) {
        ereport(DEBUG1, (errmsg("SS broadcast reload reform contrl page failed!")));
    }
}

void SSDisasterBroadcastIsExtremeRedo()
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    int ret;
    SSBroadcastIsExtremeRedo ssmsg;
    ssmsg.type = BCAST_IS_EXTREME_REDO;
    ssmsg.is_enable_extreme_redo = g_instance.attr.attr_storage.recovery_parse_workers > 1;
    dms_broadcast_info_t dms_broad_info = {
        .data = (char *)&ssmsg,
        .len = sizeof(SSBroadcastIsExtremeRedo),
        .output = NULL,
        .output_len = NULL,
        .scope = DMS_BROADCAST_ONLINE_LIST,
        .inst_map = 0,
        .timeout = SS_BROADCAST_WAIT_ONE_SECOND,
        .handle_recv_msg = (unsigned char)false,
        .check_session_kill = (unsigned char)true
    };
    ret = dms_broadcast_msg(&dms_ctx, &dms_broad_info);
    if (ret != DMS_SUCCESS) {
        ereport(DEBUG1, (errmsg("SS broadcast is extreme redo failed!")));
    }
}

void SSSendSharedInvalidMessages(const SharedInvalidationMessage *msgs, int n)
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    for (int i = 0; i < n; i++) {
        SharedInvalidationMessage *msg = (SharedInvalidationMessage *)(msgs + i);
        SSBroadcastSI ssmsg;
        ssmsg.tablespaceid = u_sess->proc_cxt.MyDatabaseTableSpace;
        if (msg->id == SHAREDINVALRELMAP_ID) {
            Assert(ssmsg.tablespaceid != InvalidOid);
        }
        ssmsg.type = BCAST_SI;
        if (msg->id >= SHAREDINVALDB_ID) {
            errno_t rc =
                memcpy_s(&(ssmsg.msg), sizeof(SharedInvalidationMessage), msg, sizeof(SharedInvalidationMessage));
            securec_check_c(rc, "", "");
        } else {
            ereport(DEBUG1, (errmsg("invalid shared invalidation msg type!")));
            return;
        }
        int backup_output = t_thrd.postgres_cxt.whereToSendOutput;
        t_thrd.postgres_cxt.whereToSendOutput = DestNone;
        int ret = dms_broadcast_opengauss_ddllock(&dms_ctx, (char *)&ssmsg, sizeof(SSBroadcastSI),
            (unsigned char)false, SS_BROADCAST_WAIT_FIVE_SECONDS, (unsigned char)SHARED_INVAL_MSG);
        if (ret != DMS_SUCCESS) {
            ereport(DEBUG1, (errmsg("SS broadcast SI msg failed!")));
        }
        t_thrd.postgres_cxt.whereToSendOutput = backup_output;
    }
}

void SSBCastDropRelAllBuffer(RelFileNode *rnodes, int rnode_len)
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);

    if (rnode_len <= 0 || rnode_len > DROP_BUFFER_USING_HASH_DEL_REL_NUM_THRESHOLD) {
        return;
    }

    uint32 bytes = (uint32)(sizeof(RelFileNode) * rnode_len);
    SSBroadcastDropRelAllBuffer *msg = (SSBroadcastDropRelAllBuffer *)palloc(
        sizeof(SSBroadcastDropRelAllBuffer) + bytes);
    msg->type = BCAST_DROP_REL_ALL_BUFFER;
    msg->size = rnode_len;
    errno_t rc = memcpy_s(msg->rnodes, bytes, rnodes, bytes);
    securec_check_c(rc, "", "");

    int output_backup = t_thrd.postgres_cxt.whereToSendOutput;
    t_thrd.postgres_cxt.whereToSendOutput = DestNone;
    int ret = dms_broadcast_opengauss_ddllock(&dms_ctx, (char *)msg, sizeof(SSBroadcastDropRelAllBuffer) + bytes,
        (unsigned char)false, SS_BROADCAST_WAIT_FIVE_SECONDS, (unsigned char)DROP_BUF_MSG);
    if (ret != DMS_SUCCESS) {
        ereport(DEBUG1, (errmsg("SS broadcast drop rel all buffer msg failed, rnode=[%d/%d/%d/%d]",
            rnodes->spcNode, rnodes->dbNode, rnodes->relNode, rnodes->bucketNode)));
    }
    t_thrd.postgres_cxt.whereToSendOutput = output_backup;
}

void SSBCastDropRelRangeBuffer(RelFileNode node, ForkNumber forkNum, BlockNumber firstDelBlock)
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);

    SSBroadcastDropRelRangeBuffer *msg = (SSBroadcastDropRelRangeBuffer *)palloc(
        sizeof(SSBroadcastDropRelRangeBuffer));
    msg->type = BCAST_DROP_REL_RANGE_BUFFER;
    msg->node = node;
    msg->forkNum = forkNum;
    msg->firstDelBlock = firstDelBlock;

    int output_backup = t_thrd.postgres_cxt.whereToSendOutput;
    t_thrd.postgres_cxt.whereToSendOutput = DestNone;
    int ret = dms_broadcast_opengauss_ddllock(&dms_ctx, (char *)msg, sizeof(SSBroadcastDropRelRangeBuffer),
        (unsigned char)false, SS_BROADCAST_WAIT_FIVE_SECONDS, (unsigned char)DROP_BUF_MSG);
    if (ret != DMS_SUCCESS) {
        ereport(DEBUG1, (errmsg("SS broadcast drop rel range buffer msg failed, rnode=[%d/%d/%d/%d],"
            "firstDelBlock=%u", node.spcNode, node.dbNode, node.relNode, node.bucketNode, firstDelBlock)));
    }
    t_thrd.postgres_cxt.whereToSendOutput = output_backup;
}

void SSBCastDropDBAllBuffer(Oid dbid)
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);

    SSBroadcastDropDBAllBuffer *msg = (SSBroadcastDropDBAllBuffer *)palloc(
        sizeof(SSBroadcastDropDBAllBuffer));
    msg->type = BCAST_DROP_DB_ALL_BUFFER;
    msg->dbid = dbid;

    int output_backup = t_thrd.postgres_cxt.whereToSendOutput;
    t_thrd.postgres_cxt.whereToSendOutput = DestNone;
    int ret = dms_broadcast_opengauss_ddllock(&dms_ctx, (char *)msg, sizeof(SSBroadcastDropDBAllBuffer),
        (unsigned char)false, SS_BROADCAST_WAIT_FIVE_SECONDS, (unsigned char)DROP_BUF_MSG);
    if (ret != DMS_SUCCESS) {
        ereport(DEBUG1, (errmsg("SS broadcast drop db all buffer msg failed, db=%d", dbid)));
    }
    t_thrd.postgres_cxt.whereToSendOutput = output_backup;
}

void SSBCastDropSegSpace(Oid spcNode, Oid dbNode)
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);

    SSBroadcastDropSegSpace *msg = (SSBroadcastDropSegSpace *)palloc(
        sizeof(SSBroadcastDropSegSpace));
    msg->type = BCAST_DROP_SEG_SPACE;
    msg->spcNode = spcNode;
    msg->dbNode = dbNode;

    int output_backup = t_thrd.postgres_cxt.whereToSendOutput;
    t_thrd.postgres_cxt.whereToSendOutput = DestNone;
    int ret = dms_broadcast_opengauss_ddllock(&dms_ctx, (char *)msg, sizeof(SSBroadcastDropSegSpace),
        (unsigned char)false, SS_BROADCAST_WAIT_FIVE_SECONDS, (unsigned char)DROP_BUF_MSG);
    if (ret != DMS_SUCCESS) {
        ereport(DEBUG1, (errmsg("SS broadcast drop seg space msg failed, spc=%d, db=%d", spcNode, dbNode)));
    }
    t_thrd.postgres_cxt.whereToSendOutput = output_backup;
}

int SSProcessSharedInvalMsg(char *data, uint32 len)
{
    if (unlikely(len != sizeof(SSBroadcastSI))) {
        ereport(DEBUG1, (errmsg("invalid broadcast SI message")));
        return DMS_ERROR;
    }

    SSBroadcastSI* ssmsg = (SSBroadcastSI *)data;
    if (ssmsg->msg.id == SHAREDINVALDB_ID) {
        NotifyGscDropDB(ssmsg->msg.db.dbId, ssmsg->msg.db.need_clear);
        return DMS_SUCCESS;
    }

    /* process msg one by one */
    if (EnableGlobalSysCache()) {
        SSStandbyGlobalInvalidSharedInvalidMessages(&(ssmsg->msg), ssmsg->tablespaceid);
    }
    SIInsertDataEntries(&(ssmsg->msg), 1);
    if (ENABLE_GPC && g_instance.plan_cache != NULL) {
        g_instance.plan_cache->InvalMsg(&(ssmsg->msg), 1);
    }
    return DMS_SUCCESS;
}

void SSUpdateSegDropTimeline(uint32 seg_drop_timeline)
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    SSBroadcastSegDropTL ssmsg;
    ssmsg.type = BCAST_SEGDROPTL;
    ssmsg.seg_drop_timeline = seg_drop_timeline;
    int output_backup = t_thrd.postgres_cxt.whereToSendOutput;
    t_thrd.postgres_cxt.whereToSendOutput = DestNone;
    dms_broadcast_info_t dms_broad_info = {
        .data = (char *)&ssmsg,
        .len = sizeof(SSBroadcastSegDropTL),
        .output = NULL,
        .output_len = NULL,
        .scope = DMS_BROADCAST_ONLINE_LIST,
        .inst_map = 0,
        .timeout = SS_BROADCAST_WAIT_FIVE_SECONDS,
        .handle_recv_msg = (unsigned char)false,
        .check_session_kill = (unsigned char)true
    };
    int ret = dms_broadcast_msg(&dms_ctx, &dms_broad_info);
    if (ret != DMS_SUCCESS) {
        ereport(DEBUG1, (errmsg("SS broadcast seg_drop_timeline failed!")));
    }
    t_thrd.postgres_cxt.whereToSendOutput = output_backup;
}

int SSProcessSegDropTimeline(char *data, uint32 len)
{
    if (unlikely(len != sizeof(SSBroadcastSegDropTL))) {
        ereport(DEBUG1, (errmsg("invalid broadcast seg drop tl message")));
        return DMS_ERROR;
    }

    SSBroadcastSegDropTL* ssmsg = (SSBroadcastSegDropTL *)data;
    pg_atomic_write_u32(&g_instance.segment_cxt.segment_drop_timeline, ssmsg->seg_drop_timeline);
    return DMS_SUCCESS;
}

int SSProcessDropRelAllBuffer(char *data, uint32 len)
{
    if (unlikely(len < sizeof(SSBroadcastDropRelAllBuffer))) {
        ereport(DEBUG1, (errmodule(MOD_DMS), errmsg("invalid drop rel buffer message")));
        return DMS_ERROR;
    }

    SSBroadcastDropRelAllBuffer *msg = (SSBroadcastDropRelAllBuffer *)data;
    int rnode_len = msg->size;
    RelFileNode *rnodes = msg->rnodes;

    if (unlikely(rnode_len <= 0)) {
        return DMS_SUCCESS;
    }

    if (unlikely(rnode_len > DROP_BUFFER_USING_HASH_DEL_REL_NUM_THRESHOLD)) {
        ereport(DEBUG1, (errmodule(MOD_DMS), errmsg("invalid buffer message is invalidate")));
        return DMS_ERROR;
    }

    if (unlikely(len != (sizeof(SSBroadcastDropRelAllBuffer) + rnode_len * sizeof(RelFileNode)))) {
        ereport(DEBUG1, (errmodule(MOD_DMS), errmsg("invalid drop rel buffer message")));
        return DMS_ERROR;
    }

    DropRelFileNodeAllBuffersUsingScan(rnodes, rnode_len);
    return DMS_SUCCESS;
}

int SSProcessDropRelRangeBuffer(char *data, uint32 len)
{
    if (unlikely(len != sizeof(SSBroadcastDropRelRangeBuffer))) {
        ereport(DEBUG1, (errmodule(MOD_DMS), errmsg("invalid drop rel range buffer message")));
        return DMS_ERROR;
    }
    SSBroadcastDropRelRangeBuffer *msg = (SSBroadcastDropRelRangeBuffer *)data;
    DropRelFileNodeShareBuffers(msg->node, msg->forkNum, msg->firstDelBlock);
    return DMS_SUCCESS;
}

int SSProcessDropDBAllBuffer(char *data, uint32 len)
{
    if (unlikely(len != sizeof(SSBroadcastDropDBAllBuffer))) {
        ereport(DEBUG1, (errmodule(MOD_DMS), errmsg("invalid drop db all buffer message")));
        return DMS_ERROR;
    }

    SSBroadcastDropDBAllBuffer *msg = (SSBroadcastDropDBAllBuffer *)data;
    DropDatabaseBuffers(msg->dbid);
    return DMS_SUCCESS;
}

int SSProcessDropSegSpace(char *data, uint32 len)
{
    if (unlikely(len != sizeof(SSBroadcastDropSegSpace))) {
        ereport(DEBUG1, (errmodule(MOD_DMS), errmsg("invalid drop seg space message")));
        return DMS_ERROR;
    }

    SSBroadcastDropSegSpace *msg = (SSBroadcastDropSegSpace *)data;
    SSDrop_seg_space(msg->spcNode, msg->dbNode);
    return DMS_SUCCESS;
}

static void SSFlushGlobalByInvalidMsg(int8 id, Oid db_id, uint32 hash_value, bool reset)
{
    if (db_id == InvalidOid) {
        GlobalSysTabCache *global_systab = g_instance.global_sysdbcache.GetSharedGSCEntry()->m_systabCache;
        global_systab->InvalidTuples(id, hash_value, reset);
    } else {
        GlobalSysDBCacheEntry *entry = g_instance.global_sysdbcache.FindTempGSCEntry(db_id);
        if (entry == NULL) {
            return;
        }
        entry->m_systabCache->InvalidTuples(id, hash_value, reset);
        g_instance.global_sysdbcache.ReleaseTempGSCEntry(entry);
    }
}

static void SSInvalidateGlobalCatalog(const SharedInvalidationMessage* msg)
{
    for (int8 cache_id = 0; cache_id < SysCacheSize; cache_id++) {
        if (cacheinfo[cache_id].reloid == msg->cat.catId) {
            SSFlushGlobalByInvalidMsg(cache_id, msg->cat.dbId, 0, true);
        }
    }
}

static void SSInvalidateRelCache(const SharedInvalidationMessage* msg)
{
    if (msg->rc.dbId == InvalidOid) {
        GlobalTabDefCache *global_systab = g_instance.global_sysdbcache.GetSharedGSCEntry()->m_tabdefCache;
        global_systab->Invalidate(msg->rc.dbId, msg->rc.relId);
    } else {
        GlobalSysDBCacheEntry *entry = g_instance.global_sysdbcache.FindTempGSCEntry(msg->rc.dbId);
        if (entry == NULL) {
            return;
        }
        entry->m_tabdefCache->Invalidate(msg->rc.dbId, msg->rc.relId);
        g_instance.global_sysdbcache.ReleaseTempGSCEntry(entry);
    }
}

static void SSInvalidateRelmap(const SharedInvalidationMessage* msg, Oid tsid)
{
    /*First reload relmap file, then invalidate global relmap cache */
    char* database_path = GetDatabasePath(msg->rm.dbId, tsid);
    bool shared = (msg->rm.dbId == InvalidOid) ? true : false;
    LWLockAcquire(RelationMappingLock, LW_EXCLUSIVE);
    char *unaligned_buf = (char*)palloc0(sizeof(RelMapFile) + ALIGNOF_BUFFER);
    RelMapFile* new_relmap = (RelMapFile*)BUFFERALIGN(unaligned_buf);
    if (u_sess->proc_cxt.DatabasePath == NULL || strcmp(u_sess->proc_cxt.DatabasePath, database_path)) {
        u_sess->proc_cxt.DatabasePath = database_path;
    }
    load_relmap_file(shared, new_relmap);
    if (shared) {
        GlobalSysDBCacheEntry *global_db = g_instance.global_sysdbcache.GetSharedGSCEntry();
        global_db->m_relmapCache->UpdateBy(new_relmap);
    } else {
        GlobalSysDBCacheEntry *entry = g_instance.global_sysdbcache.FindTempGSCEntry(msg->rm.dbId);
        if (entry == NULL) {
            LWLockRelease(RelationMappingLock);
            pfree(unaligned_buf);
            pfree(database_path);
            return;
        }
        entry->m_relmapCache->UpdateBy(new_relmap);
        g_instance.global_sysdbcache.ReleaseTempGSCEntry(entry);
    }
    LWLockRelease(RelationMappingLock);
    pfree(unaligned_buf);
    pfree(database_path);
}

static void SSInvalidatePartCache(const SharedInvalidationMessage* msg)
{
    GlobalSysDBCacheEntry *entry = g_instance.global_sysdbcache.FindTempGSCEntry(msg->pc.dbId);
    if (entry == NULL) {
        return;
    }
    entry->m_partdefCache->Invalidate(msg->pc.dbId, msg->pc.partId);
    g_instance.global_sysdbcache.ReleaseTempGSCEntry(entry);
}

static void SSInvalidateCatCache(const SharedInvalidationMessage* msg)
{
    SSFlushGlobalByInvalidMsg(msg->cc.id, msg->cc.dbId, msg->cc.hashValue, false);
}

void SSStandbyGlobalInvalidSharedInvalidMessages(const SharedInvalidationMessage* msg, Oid tsid)
{
    Assert(EnableGlobalSysCache());
    switch (msg->id) {
        case SHAREDINVALCATALOG_ID: { /* reset system table */
            SSInvalidateGlobalCatalog(msg);
            break;
        }
        case SHAREDINVALRELCACHE_ID: { /* invalid table and call callbackfunc registered on the table */
            SSInvalidateRelCache(msg);
            break;
        }
        case SHAREDINVALRELMAP_ID: { /* invalid relmap cxt */
            SSInvalidateRelmap(msg, tsid);
            break;
        }
        case SHAREDINVALPARTCACHE_ID: { /* invalid partcache and call callbackfunc registered on the part */
            SSInvalidatePartCache(msg);
            break;
        }
        case SHAREDINVALSMGR_ID:
        case SHAREDINVALHBKTSMGR_ID:
        case SHAREDINVALFUNC_ID: {
            break;
        }
        default:{
            if (msg->id >= 0) { /* invalid catcache, most cases are ddls on rel */
                SSInvalidateCatCache(msg);
            } else {
                ereport(FATAL, (errmsg("unrecognized SI message ID: %d", msg->id)));
            }
        }
    }
}

/* Send to master to update my transaction state or snapshot info */
void SSStandbyUpdateRedirectInfo()
{
    dms_opengauss_txn_sw_info_t dms_sw_info;
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);

    RedirectManager* redirect_manager = get_redirect_manager();
    dms_sw_info.sxid = redirect_manager->ss_standby_sxid;
    dms_sw_info.scid = redirect_manager->ss_standby_scid;
    dms_sw_info.server_proc_slot = redirect_manager->server_proc_slot;

    do {
        dms_ctx.xmap_ctx.dest_id = (unsigned int)SS_PRIMARY_ID;
        if (dms_request_opengauss_txn_of_master(&dms_ctx, &dms_sw_info) == DMS_SUCCESS) {
            break;
        }

        if (SS_IN_REFORM) {
            return;
        }

        pg_usleep(USECS_PER_SEC);
    } while (true);

    redirect_manager->ss_standby_sxid = dms_sw_info.sxid;
    redirect_manager->ss_standby_scid = dms_sw_info.scid;
}

bool SSCanFetchLocalSnapshotTxnRelatedInfo()
{
    if (SS_DISASTER_STANDBY_CLUSTER) {
        /* Main standby always recovery, when dispatcher thread recovery standby xlog type
         * in parallel recovery. Standby xlog type need get local snapshot, so return true.
         */
        if (SS_NORMAL_PRIMARY || (SS_REFORM_REFORMER && SS_DISASTER_MAIN_STANDBY_NODE)) {
            return true;
        }

        return false;
    }

    if (SS_IN_REFORM && t_thrd.role == STARTUP) {
        return true;
    }

    if (SS_NORMAL_PRIMARY) {
        return true;
    } else if (SS_PERFORMING_SWITCHOVER) {
        if (SS_REFORM_REFORMER && g_instance.dms_cxt.SSClusterState < NODESTATE_PROMOTE_APPROVE) {
            return true;
        }
    } else if (SS_REFORM_REFORMER && SSPerformingStandbyScenario()) {
        return true;
    } else if (SS_REFORM_REFORMER && SS_ONDEMAND_BUILD_DONE) {
        ss_xmin_info_t *xmin_info = &g_instance.dms_cxt.SSXminInfo;
        SpinLockAcquire(&xmin_info->snapshot_available_lock);
        bool snap_available = xmin_info->snapshot_available;
        SpinLockRelease(&xmin_info->snapshot_available_lock);
        return snap_available;
    }
    
    return false;
}

int SSGetOldestXmin(char *data, uint32 len, char *output_msg, uint32 *output_msg_len)
{
    if (unlikely(len != sizeof(SSBroadcastXmin))) {
        ereport(DEBUG1, (errmsg("invalid broadcast xmin message")));
        return DMS_ERROR;
    }

    SSBroadcastXminAck* getXminReq = (SSBroadcastXminAck *)output_msg;
    getXminReq->type = BCAST_GET_XMIN_ACK;
    getXminReq->xmin = InvalidTransactionId;
    GetOldestGlobalProcXmin(&(getXminReq->xmin));
    *output_msg_len = sizeof(SSBroadcastXminAck);
    return DMS_SUCCESS;
}

/* Calbulate the oldest xmin during broadcast xmin ack */
int SSGetOldestXminAck(SSBroadcastXminAck *ack_data)
{
    TransactionId xmin_ack = pg_atomic_read_u64(&g_instance.dms_cxt.xminAck);
    if (TransactionIdIsValid(ack_data->xmin) && TransactionIdIsNormal(ack_data->xmin) &&
        TransactionIdPrecedes(ack_data->xmin, xmin_ack)) {
        pg_atomic_write_u64(&g_instance.dms_cxt.xminAck, ack_data->xmin);
    }
    return DMS_SUCCESS;
}

bool SSGetOldestXminFromAllStandby(TransactionId xmin, TransactionId xmax, CommitSeqNo csn)
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    SSBroadcastXminAck xmin_bcast_ack;
    unsigned int len_of_ack = sizeof(SSBroadcastXminAck);
    SSBroadcastSnapshot latest_snapshot;
    dms_broadcast_info_t dms_broad_info;
    SSBroadcastXmin xmin_data;
    if (ENABLE_SS_BCAST_SNAPSHOT) {
        latest_snapshot.xmin = xmin;
        latest_snapshot.xmax = xmax;
        latest_snapshot.csn = csn;
        latest_snapshot.type = BCAST_SEND_SNAPSHOT;
        dms_broad_info = {
            .data = (char *)&latest_snapshot,
            .len = sizeof(SSBroadcastSnapshot),
            .output = (char *)&xmin_bcast_ack,
            .output_len = &len_of_ack,
            .scope = DMS_BROADCAST_ONLINE_LIST,
            .inst_map = 0,
            .timeout = SS_BROADCAST_WAIT_ONE_SECOND,
            .handle_recv_msg = (unsigned char)true,
            .check_session_kill = (unsigned char)true
        };
    } else {
        xmin_data.type = BCAST_GET_XMIN;
        xmin_data.xmin = InvalidTransactionId;
        dms_broad_info = {
            .data = (char *)&xmin_data,
            .len = sizeof(SSBroadcastXmin),
            .output = (char *)&xmin_bcast_ack,
            .output_len = &len_of_ack,
            .scope = DMS_BROADCAST_ONLINE_LIST,
            .inst_map = 0,
            .timeout = SS_BROADCAST_WAIT_ONE_SECOND,
            .handle_recv_msg = (unsigned char)true,
            .check_session_kill = (unsigned char)true
        };
    }

    pg_atomic_write_u64(&g_instance.dms_cxt.xminAck, MaxTransactionId);

    bool bcast_snapshot = ENABLE_SS_BCAST_SNAPSHOT;
    while (dms_broadcast_msg(&dms_ctx, &dms_broad_info) != DMS_SUCCESS) {
        if (bcast_snapshot) {
            pg_usleep(5000L);
        } else {
            return false;
        }
    }
    return true;
}

/**
 * @brief Priamry node broadcast to standby node update realtime-build logctrl enable.
 *
 * @param canncelInReform   true: happend when primary node reload recovery_time_target,
 *                              need to canncel broadcast in reform;
 *                          false happend when primary node enable recovery_time_target
 *                              before reform finish, wait until broadcast finish.
 */
void SSBroadcastRealtimeBuildLogCtrlEnable(bool canncelInReform)
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    int ret;
    SSBroadcastRealtimeBuildLogCtrl logCtrl;
    logCtrl.type = BCAST_REALTIME_BUILD_LOG_CTRL_ENABLE;
    logCtrl.enableLogCtrl = ENABLE_REALTIME_BUILD_TARGET_RTO;
    dms_broadcast_info_t dms_broad_info = {
        .data = (char *)&logCtrl,
        .len = sizeof(SSBroadcastRealtimeBuildLogCtrl),
        .output = NULL,
        .output_len = NULL,
        .scope = DMS_BROADCAST_ONLINE_LIST,
        .inst_map = 0,
        .timeout = SS_BROADCAST_WAIT_ONE_SECOND,
        .handle_recv_msg = (unsigned char)false,
        .check_session_kill = (unsigned char)true
    };

    if (canncelInReform && SS_IN_REFORM) {
        return;
    }

    do {
        ret = dms_broadcast_msg(&dms_ctx, &dms_broad_info);
        if (ret == DMS_SUCCESS || (canncelInReform && SS_IN_REFORM)) {
            break;
        }
        pg_usleep(5000L);
    } while (ret != DMS_SUCCESS);
    if (ret == DMS_SUCCESS) {
        ereport(LOG,
                (errmsg("[SS reform][On-demand] notify standby node update realtime-build log ctrl enable success, "
                        "enableLogCtrl: %d", logCtrl.enableLogCtrl)));
    }
}

/*
 *  Primary node notify standby nodes whether enable or disable recovery_time_target.
 */
int SSUpdateRealtimeBuildLogCtrl(char* data, uint32 len)
{
    if (unlikely(len != sizeof(SSBroadcastRealtimeBuildLogCtrl))) {
        return DMS_ERROR;
    }
    if (!ENABLE_ONDEMAND_REALTIME_BUILD || !SS_STANDBY_MODE) {
        return DMS_SUCCESS;
    }
    SSBroadcastRealtimeBuildLogCtrl *logCtrlEnable = (SSBroadcastRealtimeBuildLogCtrl *)data;

    realtime_build_log_ctrl_status oldState = g_instance.dms_cxt.SSRecoveryInfo.realtimeBuildLogCtrlStatus;
    if (!logCtrlEnable->enableLogCtrl) {
        g_instance.dms_cxt.SSRecoveryInfo.realtimeBuildLogCtrlStatus = DISABLE;
    } else if (oldState == DISABLE) {
        g_instance.dms_cxt.SSRecoveryInfo.realtimeBuildLogCtrlStatus = ENABLE_LOG_CTRL;
    }

    ereport(LOG, (errmodule(MOD_DMS),
            errmsg("[On-demand] Update standby realtime-build log ctrl %s, "
                "enableLogCtrl: %s, enable_ondemand_realtime_build: true.",
                logCtrlEnable->enableLogCtrl ? "enable" : "disable",
                logCtrlEnable->enableLogCtrl ? "true" : "false")));
    return DMS_SUCCESS;
}

/* report realtime-build ptr to primary */
bool SSReportRealtimeBuildPtr(XLogRecPtr realtimeBuildPtr)
{
    if (!SS_STANDBY_ENABLE_TARGET_RTO) {
        return false;
    }
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    int ret;
    SSBroadcastRealtimeBuildPtr reportMessage;
    reportMessage.type = BCAST_REPORT_REALTIME_BUILD_PTR;
    reportMessage.realtimeBuildPtr = realtimeBuildPtr;
    reportMessage.srcInstId = SS_MY_INST_ID;
    dms_broadcast_info_t dms_broad_info = {
        .data = (char *)&reportMessage,
        .len = sizeof(SSBroadcastRealtimeBuildPtr),
        .output = NULL,
        .output_len = NULL,
        .scope = DMS_BROADCAST_SPECIFY_LIST,
        .inst_map = (unsigned long long)1 << SS_PRIMARY_ID,
        .timeout = SS_BROADCAST_WAIT_ONE_SECOND,
        .handle_recv_msg = (unsigned char)false,
        .check_session_kill = (unsigned char)true
    };

    do {
        ret = dms_broadcast_msg(&dms_ctx, &dms_broad_info);
        if (ret == DMS_SUCCESS) {
            break;
        }
        if (!SS_STANDBY_ENABLE_TARGET_RTO) {
            break;
        }
        pg_usleep(5000L);
    } while (ret != DMS_SUCCESS);
    return ret == DMS_SUCCESS;
}

int SSGetStandbyRealtimeBuildPtr(char* data, uint32 len)
{
    if (unlikely(len != sizeof(SSBroadcastRealtimeBuildPtr))) {
        return DMS_ERROR;
    }
    if (!ENABLE_ONDEMAND_REALTIME_BUILD || !SS_PRIMARY_MODE || SS_IN_REFORM) {
        return DMS_SUCCESS;
    }
    SSBroadcastRealtimeBuildPtr *receiveMessage = (SSBroadcastRealtimeBuildPtr *)data;
    XLogRecPtr realtimePtr = receiveMessage->realtimeBuildPtr;
    int srcId = receiveMessage->srcInstId;
    if (((0x1 << srcId) & g_instance.dms_cxt.SSReformInfo.new_bitmap) == 0) {
        ereport(WARNING, (errmodule(MOD_DMS),
            errmsg("[SS][On-demand] Get invalid realtime-build ptr from standby inst_id: %d, "
                   "replayEndRecPtr: %X/%X, ignore it.",
                   srcId, (uint32)(realtimePtr >> 32), (uint32)realtimePtr)));
        return DMS_SUCCESS;
    }
    realtime_build_ctrl_t *rtBuildCtrl = &g_instance.dms_cxt.SSRecoveryInfo.rtBuildCtrl[srcId];
    if (g_instance.dms_cxt.SSRecoveryInfo.realtimeBuildLogCtrlStatus == DISABLE) {
        g_instance.dms_cxt.SSRecoveryInfo.realtimeBuildLogCtrlStatus = ENABLE_LOG_CTRL;
        ereport(LOG, (errmodule(MOD_DMS),
            errmsg("[SS][On-demand] Get realtime-build ptr from standby inst_id: %d,"
                   "enable realtime-build log ctrl, replayEndRecPtr: %X/%X",
                   srcId, (uint32)(realtimePtr >> 32), (uint32)realtimePtr)));
    } else if (rtBuildCtrl->replyTime == 0) {
        ereport(LOG, (errmodule(MOD_DMS),
            errmsg("[SS][On-demand] Get realtime-build ptr from standby inst_id: %d first time, "
                   "replayEndRecPtr: %X/%X",
                   srcId, (uint32)(realtimePtr >> 32), (uint32)realtimePtr)));
    }

    // If the time interval betwen two reply < 100 ms, ignore this reply.
    TimestampTz currentTime = GetCurrentTimestamp();
    if (SSLogCtrlCalculateTimeDiff(rtBuildCtrl->replyTime, currentTime) <= MIN_REPLY_MILLISEC_TIME_DIFF) {
        return DMS_SUCCESS;
    }
    rtBuildCtrl->prevReplyTime = rtBuildCtrl->replyTime;
    rtBuildCtrl->replyTime = currentTime;
    rtBuildCtrl->prevBuildPtr = rtBuildCtrl->realtimeBuildPtr;
    rtBuildCtrl->realtimeBuildPtr = realtimePtr;
    if (rtBuildCtrl->prevBuildPtr > rtBuildCtrl->realtimeBuildPtr) {
        ereport(WARNING, (errmodule(MOD_DMS),
            errmsg("[SS][On-demand] Get realtimeBuild lsn from standby node %d is less than prevBuildPtr, "
                   " prevBuildPtr: %X/%X, realtime-build ptr: %X/%X.", srcId,
                   (uint32)(rtBuildCtrl->prevBuildPtr >> 32), (uint32)rtBuildCtrl->prevBuildPtr,
                   (uint32)(rtBuildCtrl->realtimeBuildPtr >> 32), (uint32)rtBuildCtrl->realtimeBuildPtr)));
    } else if (rtBuildCtrl->prevBuildPtr != InvalidXLogRecPtr) {
        ereport(DEBUG4, (errmodule(MOD_DMS),
            errmsg("[SS][On-demand] Get realtimeBuild lsn from standby inst_id %d, "
                   " prevBuildPtr: %X/%X, realtime-build ptr: %X/%X.", srcId,
                   (uint32)(rtBuildCtrl->prevBuildPtr >> 32), (uint32)rtBuildCtrl->prevBuildPtr,
                   (uint32)(rtBuildCtrl->realtimeBuildPtr >> 32), (uint32)rtBuildCtrl->realtimeBuildPtr)));
        if (rtBuildCtrl->prevBuildPtr != InvalidXLogRecPtr) {
            SSRealtimebuildLogCtrl(srcId);
        }
    }
    return DMS_SUCCESS;
}

/* broadcast to standby node synchronization GUC */
void SSBroadcastSyncGUC()
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    int ret;
    int count = 0;
    SSBroadcastSyncGUCst syncGUC;
    syncGUC.type = BCAST_CONFIG_SYNC;
    dms_broadcast_info_t dms_broad_info = {
        .data = (char *)&syncGUC,
        .len = sizeof(SSBroadcastSyncGUCst),
        .output = NULL,
        .output_len = NULL,
        .scope = DMS_BROADCAST_ONLINE_LIST,
        .inst_map = 0,
        .timeout = SS_BROADCAST_WAIT_FIVE_SECONDS,
        .handle_recv_msg = (unsigned char)false,
        .check_session_kill = (unsigned char)true
    };

    do {
        count++;
        ret = dms_broadcast_msg(&dms_ctx, &dms_broad_info);
        if (ret == DMS_SUCCESS) {
            break;
        }
        pg_usleep(USECS_PER_SEC);
    } while (ret != DMS_SUCCESS && count <= SYNC_TRY_COUNT);
    if (ret == DMS_SUCCESS) {
        ereport(LOG, (errmsg("notify standby node synchronization GUC success")));
    }
}

int SSUpdateLocalConfFile(char* data, uint32 len)
{
    if (SS_PRIMARY_MODE) {
        return DMS_SUCCESS;
    }

    /* notify postmaster load the shared config file */
    if (gs_signal_send(PostmasterPid, SIGHUP) != 0) {
        ereport(WARNING, (errmsg("send SIGHUP to PM failed")));
        return DMS_ERROR;
    }
    return DMS_SUCCESS;
}

int SSDisasterUpdateIsEnableExtremeRedo(char* data, uint32 len)
{
    if (unlikely(len != sizeof(SSBroadcastIsExtremeRedo))) {
        return DMS_ERROR;
    }
    SSBroadcastIsExtremeRedo *ssmsg = (SSBroadcastIsExtremeRedo *)data;
    g_instance.dms_cxt.SSRecoveryInfo.is_disaster_extreme_redo = ssmsg->is_enable_extreme_redo;
    return DMS_SUCCESS;
}

#ifdef ENABLE_HTAP
void SSBroadcastIMCStoreVacuum(int chunkNum, Oid rid, uint32 rgid, TransactionId xid, bool actived,
    int cols, CUDesc** CUDescs, CU** CUs)
{
    size_t size = sizeof(SSIMCStoreVacuum);
    if (actived) {
        size += cols * (sizeof(CUDesc) + sizeof(CU));
    }
    SSIMCStoreVacuum* info = (SSIMCStoreVacuum*)palloc(size);
    info->type = BCAST_IMCSTORE_VACUUM;
    info->rid = rid;
    info->rgid = rgid;
    info->xid = xid;
    info->cols = cols;
    info->actived = actived;
    info->chunkNum = chunkNum;
    if (actived) {
        errno_t rc;
        char* ptr = (char*)(info + 1);
        size_t restSize = size - sizeof(SSIMCStoreVacuum);
        for (int col = 0; col < cols; ++col) {
            rc = memcpy_s(ptr, restSize, CUDescs[col], sizeof(CUDesc));
            securec_check_c(rc, "", "");
            ptr += sizeof(CUDesc);
            restSize -= sizeof(CUDesc);
            rc = memcpy_s(ptr, restSize, CUs[col], sizeof(CU));
            securec_check_c(rc, "", "");
            ptr += sizeof(CU);
            restSize -= sizeof(CU);
        }
    }

    dms_broadcast_info_t dms_broad_info = {
        .data = (char *)info,
        .len = size,
        .output = NULL,
        .output_len = NULL,
        .scope = DMS_BROADCAST_ONLINE_LIST,
        .inst_map = 0,
        .timeout = SS_BROADCAST_WAIT_ONE_SECOND,
        .handle_recv_msg = (unsigned char)false,
        .check_session_kill = (unsigned char)true
    };
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    int ret = dms_broadcast_msg(&dms_ctx, &dms_broad_info);
    if (ret != DMS_SUCCESS) {
        ereport(WARNING, (errmsg("SS broadcast imcstore vacuum infomation failed!")));
    }
}

int32 SSLoadIMCStoreVacuum(char *data, uint32 len)
{
    SSIMCStoreVacuum* info = (SSIMCStoreVacuum*)data;

    IMCSDesc *imcsDesc = IMCS_HASH_TABLE->GetImcsDesc(info->rid);
    if (imcsDesc == NULL) {
        return DMS_SUCCESS;
    }

    if (imcsDesc->imcsStatus != IMCS_POPULATE_COMPLETE) {
        return DMS_SUCCESS;
    }

    imcsDesc->shareMemPool->ShmChunkMmapAll(info->chunkNum);
    imcsDesc->shareMemPool->FlushShmChunkAll(ShmCacheOpt::RACK_INVALID);

    if (!info->actived) {
        IMCStoreSyncVacuumPushWork(info->rid, info->rgid, info->xid, 0, nullptr, nullptr);
        return DMS_SUCCESS;
    }

    MemoryContext oldcontext = MemoryContextSwitchTo(imcsDesc->imcuDescContext);
    CUDesc** CUDescs = (CUDesc**)palloc(sizeof(CUDesc*) * info->cols);
    CU** CUs = (CU**)palloc(sizeof(CU*) * info->cols);

    errno_t rc;
    len -= sizeof(SSIMCStoreVacuum);
    char* ptr = data + sizeof(SSIMCStoreVacuum);
    uint32 newCuSize = 0;
    for (int i = 0; i < info->cols; ++i) {
        CUDescs[i] = New(CurrentMemoryContext) CUDesc();
        CUs[i] = New(CurrentMemoryContext) CU();

        rc = memcpy_s(CUDescs[i], sizeof(CUDesc), ptr, sizeof(CUDesc) > len ? len : sizeof(CUDesc));
        securec_check(rc, "\0", "\0");
        ptr += sizeof(CUDesc);
        len -= sizeof(CUDesc);
        rc = memcpy_s(CUs[i], sizeof(CU), ptr, sizeof(CU) > len ? len : sizeof(CU));
        securec_check(rc, "\0", "\0");
        ptr += sizeof(CU);
        len -= sizeof(CU);

        newCuSize += CUs[i]->m_cuSize;
    }
    MemoryContextSwitchTo(oldcontext);
    IMCStoreSyncVacuumPushWork(info->rid, info->rgid, info->xid, newCuSize, CUDescs, CUs);
    return DMS_SUCCESS;
}

void SSBroadcastIMCStoreVacuumLocalMemory(Oid rid, uint32 rgid, TransactionId xid)
{
    size_t size = sizeof(SSIMCStoreVacuum);
    SSIMCStoreVacuum* info = (SSIMCStoreVacuum*)palloc(size);
    info->type = BCAST_IMCSTORE_VACUUM_LOCAL_MEMORY;
    info->rid = rid;
    info->rgid = rgid;
    info->xid = xid;
    info->cols = 0;
    info->actived = false;
    info->chunkNum = 0;

    dms_broadcast_info_t dms_broad_info = {
        .data = (char *)info,
        .len = size,
        .output = NULL,
        .output_len = NULL,
        .scope = DMS_BROADCAST_ONLINE_LIST,
        .inst_map = 0,
        .timeout = SS_BROADCAST_WAIT_FIVE_SECONDS,
        .handle_recv_msg = (unsigned char)false,
        .check_session_kill = (unsigned char)true
    };
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    elog(DEBUG1, "SS broadcast imcstore for local memory to standby: rid(%u), rgid(%u), xid(%u).", rid, rgid, xid);
    int ret = dms_broadcast_msg(&dms_ctx, &dms_broad_info);
    if (ret != DMS_SUCCESS) {
        ereport(WARNING, (errmsg("SS broadcast imcstore vacuum for local memory infomation failed!")));
    }
    elog(DEBUG1, "Succeeded to SS broadcast imcstore for local memory.");
}

int32 SSLoadIMCStoreVacuumLocalMemory(char *data, uint32 len)
{
    SSIMCStoreVacuum* info = (SSIMCStoreVacuum*)data;
    elog(DEBUG1, "SS recieve imcstore vacuum for local memory from primary: rid(%u), rgid(%u), xid(%u).",
        info->rid, info->rgid, info->xid);
    if (!SS_IMCU_CACHE->CheckRGOwnedByCurNode(info->rgid)) {
        return DMS_SUCCESS;
    }

    IMCSDesc *imcsDesc = IMCS_HASH_TABLE->GetImcsDesc(info->rid);
    if (imcsDesc == NULL) {
        return DMS_SUCCESS;
    }

    if (imcsDesc->imcsStatus != IMCS_POPULATE_COMPLETE) {
        return DMS_SUCCESS;
    }

    IMCStoreVacuumPushWork(info->rid, info->rgid, info->xid);
    elog(DEBUG1, "Succeed to process imcstore vacuum for local memory from succeed.");
    return DMS_SUCCESS;
}

void SSNotifyPrimaryVacuumLocalMemorySuccess(Oid rid, uint32 rgid, TransactionId xid)
{
    size_t size = sizeof(SSIMCStoreVacuum);
    SSIMCStoreVacuum* info = (SSIMCStoreVacuum*)palloc(size);
    info->type = BCAST_NOITIFY_PRIMARY_LOCAL_MEMORY_SUCCESS;
    info->rid = rid;
    info->rgid = rgid;
    info->xid = xid;
    info->cols = 0;
    info->actived = false;
    info->chunkNum = 0;

    dms_broadcast_info_t dms_broad_info = {
        .data = (char *)info,
        .len = size,
        .output = NULL,
        .output_len = NULL,
        .scope = DMS_BROADCAST_SPECIFY_LIST,
        .inst_map = (unsigned long long)1 << SS_PRIMARY_ID,
        .timeout = SS_BROADCAST_WAIT_FIVE_SECONDS,
        .handle_recv_msg = (unsigned char)false,
        .check_session_kill = (unsigned char)true
    };
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    elog(DEBUG1, "Notify primary node that the vacuum for local memory successfully: rid(%u), rgid(%u), xid(%u).",
        rid, rgid, xid);
    int ret = dms_broadcast_msg(&dms_ctx, &dms_broad_info);
    if (ret != DMS_SUCCESS) {
        ereport(WARNING, (errmsg("Notify primary node that the vacuum for local memory infomation failed!")));
    }
    elog(DEBUG1, "Notify primary node successfully.");
}

int32 SSUpdateIMCStoreVacuumLocalMemoryDelta(char *data, uint32 len)
{
    SSIMCStoreVacuum* info = (SSIMCStoreVacuum*)data;
    elog(DEBUG1, "SS primary node recieve the request to vaccum imcstore delta for local memory:"
        "rid(%u), rgid(%u), xid(%u).", info->rid, info->rgid, info->xid);
    IMCSDesc *imcsDesc = IMCS_HASH_TABLE->GetImcsDesc(info->rid);
    if (imcsDesc == NULL) {
        return DMS_SUCCESS;
    }
    if (imcsDesc->imcsStatus != IMCS_POPULATE_COMPLETE) {
        return DMS_SUCCESS;
    }
    RowGroup* rowgroup = imcsDesc->GetRowGroup(info->rgid);
    rowgroup->m_delta->Vacuum(info->xid);
    imcsDesc->UnReferenceRowGroup();
    elog(DEBUG1, "SS primary node vaccum imcstore delta successfully.");
    return DMS_SUCCESS;
}
#endif