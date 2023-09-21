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
#include "storage/procarray.h"
#include "storage/buf/bufmgr.h"
#include "storage/smgr/segment_internal.h"
#include "ddes/dms/ss_transaction.h"
#include "ddes/dms/ss_reform_common.h"
#include "ddes/dms/ss_dms_bufmgr.h"
#include "storage/sinvaladt.h"

void SSStandbyGlobalInvalidSharedInvalidMessages(const SharedInvalidationMessage* msg, Oid tsid);

Snapshot SSGetSnapshotData(Snapshot snapshot)
{
    dms_opengauss_txn_snapshot_t dms_snapshot;
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    if (SS_IN_REFORM) {
        ereport(DEBUG1, (errmsg("[SS reform] SSGetSnapshotData returns NULL in reform.")));
        return NULL;
    }

    do {
        dms_ctx.xmap_ctx.dest_id = (unsigned int)SS_PRIMARY_ID;
        if (dms_request_opengauss_txn_snapshot(&dms_ctx, &dms_snapshot) == DMS_SUCCESS) {
            break;
        }

        if (SS_IN_REFORM) {
            ereport(DEBUG1, (errmsg("[SS reform] SSGetSnapshotData returns NULL in reform.")));
            return NULL;
        }
        pg_usleep(USECS_PER_SEC);

    } while (true);

    snapshot->xmin = dms_snapshot.xmin;
    snapshot->xmax = dms_snapshot.xmax;
    snapshot->snapshotcsn = dms_snapshot.snapshotcsn;
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

static int SSTransactionIdGetCSN(dms_opengauss_xid_csn_t *dms_txn_info, dms_opengauss_csn_result_t *xid_csn_result)
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    dms_ctx.xid_ctx.inst_id = (unsigned char)SS_PRIMARY_ID;

    return dms_request_opengauss_xid_csn(&dms_ctx, dms_txn_info, xid_csn_result);
}

/*
 * xid -> csnlog status
 * is_committed: if true, then no need to fetch xid status from clog
 */
CommitSeqNo SSTransactionIdGetCommitSeqNo(TransactionId transactionId, bool isCommit, bool isMvcc, bool isNest,
    Snapshot snapshot, bool* sync)
{
    if ((snapshot == NULL || !IsVersionMVCCSnapshot(snapshot)) &&
        TransactionIdEquals(transactionId, t_thrd.xact_cxt.cachedFetchCSNXid)) {
        t_thrd.xact_cxt.latestFetchCSNXid = t_thrd.xact_cxt.cachedFetchCSNXid;
        t_thrd.xact_cxt.latestFetchCSN = t_thrd.xact_cxt.cachedFetchCSN;
        return t_thrd.xact_cxt.cachedFetchCSN;
    }
    if (!TransactionIdIsNormal(transactionId)) {
        t_thrd.xact_cxt.latestFetchCSNXid = InvalidTransactionId;
        if (TransactionIdEquals(transactionId, BootstrapTransactionId) ||
            TransactionIdEquals(transactionId, FrozenTransactionId)) {
            return COMMITSEQNO_FROZEN;
        }
        return COMMITSEQNO_ABORTED;
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

    if (SS_IN_REFORM && (t_thrd.role == WORKER || t_thrd.role == THREADPOOL_WORKER || t_thrd.role == STREAM_WORKER)) {
        ereport(FATAL, (errmsg("SSTransactionIdGetCommitSeqNo failed during reform, xid=%lu.", transactionId)));
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
            if (SS_IN_REFORM &&
                (t_thrd.role == WORKER || t_thrd.role == THREADPOOL_WORKER || t_thrd.role == STREAM_WORKER)) {
                ereport(FATAL, (errmsg("SSTransactionIdGetCommitSeqNo failed during reform, xid=%lu.", transactionId)));
            }
            pg_usleep(USECS_PER_SEC);
            continue;
        }
    } while (true);

    if (COMMITSEQNO_IS_COMMITTED(csn) || COMMITSEQNO_IS_ABORTED(csn)) {
        t_thrd.xact_cxt.cachedFetchCSNXid = transactionId;
        t_thrd.xact_cxt.cachedFetchCSN = csn;
    }

    if (clogstatus != CLOG_XID_STATUS_IN_PROGRESS && clogstatus != CLOG_XID_STATUS_SUB_COMMITTED) {
        t_thrd.xact_cxt.cachedFetchXid = transactionId;
        t_thrd.xact_cxt.cachedFetchXidStatus = clogstatus;
        t_thrd.xact_cxt.cachedCommitLSN = lsn;
    }

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
                if (SS_IN_REFORM &&
                    (t_thrd.role == WORKER || t_thrd.role == THREADPOOL_WORKER || t_thrd.role == STREAM_WORKER)) {
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
            if (SS_IN_REFORM &&
                (t_thrd.role == WORKER || t_thrd.role == THREADPOOL_WORKER || t_thrd.role == STREAM_WORKER)) {
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
            if (SS_IN_REFORM &&
                (t_thrd.role == WORKER || t_thrd.role == THREADPOOL_WORKER || t_thrd.role == STREAM_WORKER)) {
                ereport(FATAL, (errmsg("SSMultiXactIdGetUpdateXid failed during reform, xid=%lu.", xmax)));
            }
            pg_usleep(USECS_PER_SEC);
            continue;
        }
    } while (true);

    return update_xid;
}

int SSGetOldestXmin(char *data, uint32 len, char *output_msg, uint32 *output_msg_len)
{
    if (unlikely(len != sizeof(SSBroadcastXmin))) {
        ereport(DEBUG1, (errmsg("invalid broadcast xmin message")));
        return DMS_ERROR;
    }

    SSBroadcastXminAck* getXminReq = (SSBroadcastXminAck *)output_msg;
    getXminReq->type = BCAST_GET_XMIN_ACK;
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

bool SSGetOldestXminFromAllStandby()
{
    dms_context_t dms_ctx;
    InitDmsContext(&dms_ctx);
    SSBroadcastXmin xmin_data;
    xmin_data.type = BCAST_GET_XMIN;
    xmin_data.xmin = InvalidTransactionId;
    pg_atomic_write_u64(&g_instance.dms_cxt.xminAck, MaxTransactionId);
    int ret = dms_broadcast_msg(&dms_ctx, (char *)&xmin_data, sizeof(SSBroadcastXmin),
        (unsigned char)true, SS_BROADCAST_WAIT_FIVE_SECONDS);
    if (ret != DMS_SUCCESS) {
        return false;
    }
    return true;
}

int SSReloadReformCtrlPage(uint32 len)
{
    if (unlikely(len != sizeof(SSBroadcastCmdOnly))) {
        return DMS_ERROR;
    }

    SSReadControlFile(REFORM_CTRL_PAGE);
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

    int ret = dms_broadcast_msg(&dms_ctx, (char *)&backends_data, sizeof(SSBroadcastDbBackends),
        (unsigned char)true, SS_BROADCAST_WAIT_FIVE_SECONDS);
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
    do {
        ret = dms_broadcast_msg(&dms_ctx, (char *)&ssmsg, sizeof(SSBroadcastCmdOnly),
            (unsigned char)false, SS_BROADCAST_WAIT_ONE_SECOND);

        if (ret == DMS_SUCCESS) {
            return;
        }
        pg_usleep(5000L);
    } while (ret != DMS_SUCCESS);
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
        if (msg->id >= SHAREDINVALFUNC_ID) {
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
    int ret = dms_broadcast_msg(&dms_ctx, (char *)&ssmsg, sizeof(SSBroadcastSegDropTL), (unsigned char)false,
        SS_BROADCAST_WAIT_FIVE_SECONDS);
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
