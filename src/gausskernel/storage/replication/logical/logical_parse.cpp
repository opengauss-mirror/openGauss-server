/* ---------------------------------------------------------------------------------------
 * *
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
 * logical_parse.cpp
 * This module includes parsing the xlog log, splicing the parsed tuples,
 * and then decoding these tuples by the decoding module.
 * In addition to DML logs, it also contains toast tuple splicing logic.
 *
 * IDENTIFICATION
 *	src/gausskernel/storage/replication/logical/logical_parse.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"

#include "catalog/pg_control.h"

#include "storage/standby.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"

#include "utils/memutils.h"
#include "utils/relfilenodemap.h"
#include "utils/atomic.h"
#include "cjson/cJSON.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"
#include "replication/parallel_decode.h"
#include "replication/parallel_reorderbuffer.h"
#include "replication/logical_parse.h"

extern void ParseUpdateXlog(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker);
extern void ParseDeleteXlog(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker);
extern void ParseCommitXlog(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, TransactionId xid, CommitSeqNo csn,
                         Oid dboid, TimestampTz commit_time, int nsubxacts, TransactionId *sub_xids, int ninval_msgs,
                         SharedInvalidationMessage *msgs, ParallelDecodeReaderWorker *worker);
extern void ParseXactOp(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker);
void ParseUInsert(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker);
void ParseUUpdate(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker);
void ParseUDelete(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker);
void ParseMultiInsert(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker);
void ParseUMultiInsert(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker);

void ParseHeap3Op(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker);
void ParseUheapOp(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker);
extern Pointer GetXlrec(XLogReaderState *record);

/*
 * After initially parsing the log, put tuple change into the queue
 * and wait for the decoder thread to parse it.
 */
 void PutChangeQueue(int slotId, ParallelReorderBufferChange *change) {
    int decodeWorkerId = g_Logicaldispatcher[slotId].decodeWorkerId;
    LogicalQueue* changeQueue = g_Logicaldispatcher[slotId].decodeWorkers[decodeWorkerId]->changeQueue;
 
    g_Logicaldispatcher[slotId].decodeWorkerId = (decodeWorkerId + 1) % GetDecodeParallelism(slotId);
    LogicalQueuePut(changeQueue, change);
}

static bool ParallelFilterByOrigin(ParallelLogicalDecodingContext *ctx, RepOriginId origin_id)
{
    ParallelDecodingData* data = (ParallelDecodingData *)ctx->output_plugin_private;
    origin_id = (int)((uint32)(origin_id) & TOAST_MASK);
    if (data->pOptions.only_local && origin_id != InvalidRepOriginId) {
        return true;
    }
    return false;
}

/*
 * If an invalidation message is in the commit log,
 * all decoder threads are updated catalog cache from disk synchronously.
 */
void setInvalidationsMessageToDecodeQueue(ParallelLogicalDecodingContext *ctx, ParallelDecodeReaderWorker *worker,
                                                          int ninval_msgs, SharedInvalidationMessage *msgs) {
    int slotId = worker->slotId;
    ParallelReorderBufferChange *change = NULL;
    int rc = 0;

    if (ninval_msgs > 0) {
        for (int i = 0; i < GetDecodeParallelism(slotId); i++) {
            change = ParallelReorderBufferGetChange(ctx->reorder, slotId);

            change->action = PARALLEL_REORDER_BUFFER_INVALIDATIONS_MESSAGE;
            MemoryContext oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
            change->invalidations = (SharedInvalidationMessage *)palloc0(sizeof(SharedInvalidationMessage) * ninval_msgs);

            rc = memcpy_s(change->invalidations, sizeof(SharedInvalidationMessage) * ninval_msgs, msgs,
                sizeof(SharedInvalidationMessage) * ninval_msgs);
            securec_check(rc, "", "");

            MemoryContextSwitchTo(oldCtx);
            change->ninvalidations = ninval_msgs;
            PutChangeQueue(slotId, change);
        }
    }
}

/*
 * Advance restart_lsn and catalog_xmin, the walsender thread receives
 * the flush LSN returned by the client to promote slot LSN and xmin.
 */
void ParseRunningXactsXlog(ParallelLogicalDecodingContext *ctx, XLogRecPtr lsn, xl_running_xacts *running,
    ParallelDecodeReaderWorker *worker)
{
    ParallelReorderBufferChange *change = NULL;
    int slotId = worker->slotId;
    ParallelDecodeReaderWorker* readWorker = g_Logicaldispatcher[slotId].readWorker;
    SpinLockAcquire(&(readWorker->rwlock));
    XLogRecPtr candidateOldestXminLsn = readWorker->candidate_oldest_xmin_lsn;
    XLogRecPtr candidateOldestXmin = readWorker->candidate_oldest_xmin;
    XLogRecPtr currentLsn = readWorker->current_lsn;
    XLogRecPtr restartLsn = readWorker->restart_lsn;
    XLogRecPtr flushLSN = readWorker->flushLSN;
    SpinLockRelease(&(readWorker->rwlock));
    int id = g_Logicaldispatcher[slotId].decodeWorkerId;

    LogicalIncreaseXminForSlot(candidateOldestXminLsn, candidateOldestXmin);

    if (restartLsn != InvalidXLogRecPtr) {
        LogicalIncreaseRestartDecodingForSlot(currentLsn, restartLsn);
    }

    if (!XLByteEQ(flushLSN, InvalidXLogRecPtr)) {
        LogicalConfirmReceivedLocation(flushLSN);
    }

    change = ParallelReorderBufferGetChange(ctx->reorder, slotId);
    change->action = PARALLEL_REORDER_BUFFER_CHANGE_RUNNING_XACT;
    change->oldestXmin = running->oldestRunningXid;
    change->lsn = lsn;

    g_Logicaldispatcher[slotId].decodeWorkerId = (id + 1) % GetDecodeParallelism(slotId);
    LogicalQueuePut(g_Logicaldispatcher[slotId].decodeWorkers[id]->changeQueue, change);
}


/*
 * Filter out records that we don't need to decode.
 */
static bool ParallelFilterRecord(ParallelLogicalDecodingContext *ctx, XLogReaderState *r, uint8 flags, RelFileNode* rnode)
{
    if (ParallelFilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return true;
    }
    if (((flags & XLZ_UPDATE_PREFIX_FROM_OLD) != 0) || ((flags & XLZ_UPDATE_SUFFIX_FROM_OLD) != 0)) {
        ereport(WARNING, (errmsg("update tuple has affix, don't decode it")));
        return true;
    }
    XLogRecGetBlockTag(r, 0, rnode, NULL, NULL);
    if (rnode->dbNode != ctx->slot->data.database) {
        return true;
    }
    return false;
}

/*
 * Splice toast tuples
 * The return value represents whether the current tuple needs to be decoded.
 * Ordinary tables (non system tables, toast tables, etc.) need to be decoded.
 */
void SplicingToastTuple(ParallelReorderBufferChange *change, ParallelLogicalDecodingContext *ctx, bool *needDecode,
    bool istoast, ParallelDecodeReaderWorker *worker, bool isHeap)
{
    Oid reloid;
    Relation relation = NULL;
    Oid partitionReltoastrelid = InvalidOid;
    ParallelReorderBufferTXN *txn;
    if (u_sess->utils_cxt.HistoricSnapshot == NULL) {
        u_sess->utils_cxt.HistoricSnapshot = GetLocalSnapshot(ctx->context);
    }
    u_sess->utils_cxt.HistoricSnapshot->snapshotcsn = change->data.tp.snapshotcsn;

    /*
     * It is neither a toast table nor a toast tuple,
     * and there is no need to trigger toast tuple splicing logic.
     */
    if (!istoast) {
        txn = ParallelReorderBufferTXNByXid(ctx->reorder, change->xid, false, NULL, change->lsn, true);
        if (txn == NULL || txn->toast_hash == NULL) {
            *needDecode = true;
            return;
        }
    }

    bool isSegment = IsSegmentFileNode(change->data.tp.relnode);
    reloid = RelidByRelfilenode(change->data.tp.relnode.spcNode, change->data.tp.relnode.relNode, isSegment);

    if (reloid == InvalidOid) {
        reloid = PartitionRelidByRelfilenode(change->data.tp.relnode.spcNode,
            change->data.tp.relnode.relNode, partitionReltoastrelid, NULL,isSegment);
    }

    /*
     * Catalog tuple without data, emitted while catalog was
     * in the process of being rewritten.
     */
    if (reloid == InvalidOid) {
        /*
         * description:
         * When we try to decode a table who is already dropped.
         * Maybe we could not find its relnode. In this time, we will not decode this log.
         */
        ereport(DEBUG1, (errmsg("could not lookup relation %s",
            relpathperm(change->data.tp.relnode, MAIN_FORKNUM))));
        *needDecode = false;
        return;
    }
    
    relation = RelationIdGetRelation(reloid);
    if (relation == NULL) {
        ereport(DEBUG1, (errmsg("could open relation descriptor %s",
            relpathperm(change->data.tp.relnode, MAIN_FORKNUM))));
        *needDecode = false;
        return;
    }

    if (CSTORE_NAMESPACE == get_rel_namespace(RelationGetRelid(relation))) {
        RelationClose(relation);
        *needDecode = false;
        return ;
    }

    /*
     * If the table to which this tuple belongs is a toast table,
     * put it into the toast list. If it is a normal table,
     * query whether there is a toast record before it.
     * If so, splice it into a complete toast tuple.
     */
    if (CheckToastTuple(change, ctx, relation, istoast)) {
        *needDecode = false;
    } else if (!IsToastRelation(relation)) {
        ToastTupleReplace(ctx->reorder, relation, change, partitionReltoastrelid, worker, isHeap);
        *needDecode = true;
    }

    return;
}

/*
 * Judge whether this tuple is a toast tuple, and if so,
 * put it into toast. In the hash table, wait for decoding to the main table.
 * Return value means whether current table is as toast table.
 */
bool CheckToastTuple(ParallelReorderBufferChange *change, ParallelLogicalDecodingContext *ctx, Relation relation,
    bool istoast)
{
    if (!istoast) {
        return false;
    }

    if ((change->action != PARALLEL_REORDER_BUFFER_CHANGE_INSERT &&
        change->action != PARALLEL_REORDER_BUFFER_CHANGE_UINSERT) || change->data.tp.newtuple == NULL) {
        return false;
    }

    if (RelationIsLogicallyLogged(relation)) {
        /*
         * For now ignore sequence changes entirely. Most of
         * the time they don't log changes using records we
         * understand, so it doesn't make sense to handle the
         * few cases we do.
         */
         
        if (relation->rd_rel->relkind == RELKIND_SEQUENCE) {
        } else if (!IsToastRelation(relation)) { /* user-triggered change */
            return false;
        } else if (change->action == PARALLEL_REORDER_BUFFER_CHANGE_INSERT ||
            change->action == PARALLEL_REORDER_BUFFER_CHANGE_UINSERT) {
            ToastTupleAppendChunk(ctx->reorder, relation, change);
            return true;
        }
    }
    return false;
}

/*
 * Parse commit log and set commit info to decode queue.
 */
void ParseCommitXlog(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, TransactionId xid, CommitSeqNo csn,
                         Oid dboid, TimestampTz commit_time, int nsubxacts, TransactionId *sub_xids, int ninval_msgs,
                         SharedInvalidationMessage *msgs, ParallelDecodeReaderWorker *worker)
{
    ParallelReorderBufferChange *change = NULL;
    int slotId = worker->slotId;

    setInvalidationsMessageToDecodeQueue(ctx, worker, ninval_msgs, msgs);

    change = ParallelReorderBufferGetChange(ctx->reorder, slotId);

    change->action = PARALLEL_REORDER_BUFFER_CHANGE_COMMIT;
    change->xid = xid;
    change->csn = csn;
    change->lsn =ctx->reader->ReadRecPtr;
    change->finalLsn = buf->origptr;
    change->endLsn = buf->endptr;
    change->nsubxacts = nsubxacts;
    change->commitTime = commit_time;

    if (nsubxacts > 0) {
        MemoryContext oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
        change->subXids = (TransactionId *)palloc0(sizeof(TransactionId) * nsubxacts);
        MemoryContextSwitchTo(oldCtx);
        errno_t rc =
            memcpy_s(change->subXids, sizeof(TransactionId) * nsubxacts, sub_xids, sizeof(TransactionId) * nsubxacts);
        securec_check(rc, "", "");
    }
    PutChangeQueue(slotId, change);
}

/*
 * Parse abort log and set commit info to decode queue
 */
void ParseAbortXlog(ParallelLogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid, TransactionId *sub_xids,
                        int nsubxacts, ParallelDecodeReaderWorker *worker)
{
    ParallelReorderBufferChange *change = NULL;
    int slotId = worker->slotId;

    change = ParallelReorderBufferGetChange(ctx->reorder, slotId);

    change->action = PARALLEL_REORDER_BUFFER_CHANGE_ABORT;
    change->xid = xid;
    change->lsn =ctx->reader->ReadRecPtr;
    change->finalLsn = lsn;
    change->nsubxacts = nsubxacts;
    if (nsubxacts > 0) {
        MemoryContext oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
        change->subXids = (TransactionId *)palloc0(sizeof(TransactionId) * nsubxacts);
        errno_t rc =
            memcpy_s(change->subXids, sizeof(TransactionId) * nsubxacts, sub_xids, sizeof(TransactionId) * nsubxacts);
        securec_check(rc, "", "");
        MemoryContextSwitchTo(oldCtx);
    }
    PutChangeQueue(slotId, change);
}

/*
 * Handle rmgr HEAP2_ID records.
 */
void ParseHeap2Op(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker)
{
    uint8 info = XLogRecGetInfo(buf->record) & XLOG_HEAP_OPMASK;
    switch (info) {
        case XLOG_HEAP2_MULTI_INSERT:
            ParseMultiInsert(ctx, buf, worker);
            break;
        case XLOG_HEAP2_FREEZE:
            /*
             * Although these records only exist to serve the needs of logical
             * decoding, all the work happens as part of crash or archive
             * recovery, so we don't need to do anything here.
             */
            break;
        /*
         * Everything else here is just low level physical stuff we're
         * not interested in.
         */
        case XLOG_HEAP2_CLEAN:
        case XLOG_HEAP2_CLEANUP_INFO:
        case XLOG_HEAP2_VISIBLE:
        case XLOG_HEAP2_LOGICAL_NEWPAGE:
        case XLOG_HEAP2_BCM:
        case XLOG_HEAP2_PAGE_UPGRADE:

            break;
        default:
            ereport(WARNING,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unexpected RM_HEAP2_ID record type: %u", info)));
    }
}

/*
 * Handle rmgr HEAP_ID records for DecodeRecordIntoReorderBuffer().
 */
void ParseHeapOp(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker)
{
    uint8 info = XLogRecGetInfo(buf->record) & XLOG_HEAP_OPMASK;


    switch (info) {
        case XLOG_HEAP_INSERT:
            ParseInsertXlog(ctx, buf, worker);
            break;

        /*
         * Treat HOT update as normal updates. There is no useful
         * information in the fact that we could make it a HOT update
         * locally and the WAL layout is compatible.
         */
        case XLOG_HEAP_HOT_UPDATE:
        case XLOG_HEAP_UPDATE:
            ParseUpdateXlog(ctx, buf, worker);
            break;

        case XLOG_HEAP_DELETE:
            ParseDeleteXlog(ctx, buf, worker);
            break;

        case XLOG_HEAP_NEWPAGE:
            /*
             * This is only used in places like indexams and CLUSTER which
             * don't contain changes relevant for logical replication.
             */
            break;

        case XLOG_HEAP_INPLACE:
            /*
             * Inplace updates are only ever performed on catalog tuples and
             * can, per definition, not change tuple visibility.  Since we
             * don't decode catalog tuples, we're not interested in the
             * record's contents.
             *
             * In-place updates can be used either by XID-bearing transactions
             * (e.g.  in CREATE INDEX CONCURRENTLY) or by XID-less
             * transactions (e.g.  VACUUM).  In the former case, the commit
             * record will include cache invalidations, so we mark the
             * transaction as catalog modifying here. Currently that's
             * redundant because the commit will do that as well, but once we
             * support decoding in-progress relations, this will be important.
             */
             break;

        case XLOG_HEAP_BASE_SHIFT:
            break;

        case XLOG_HEAP_LOCK:
            /* we don't care about row level locks for now */
            break;

        default:
            ereport(WARNING,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unexpected RM_HEAP_ID record type: %u", info)));
            break;
    }
}

void ParseNewCid(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker)
{
    int slotId = worker->slotId;
    ParallelReorderBufferChange *change = ParallelReorderBufferGetChange(ctx->reorder, slotId);
    change->action = PARALLEL_REORDER_BUFFER_NEW_CID;
    change->xid = XLogRecGetXid(buf->record);
    PutChangeQueue(slotId, change);
}

void ParseHeap3Op(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker)
{
    uint8 info = XLogRecGetInfo(buf->record) & XLOG_HEAP_OPMASK;

    switch (info) {
        case XLOG_HEAP3_NEW_CID: {
            ParseNewCid(ctx, buf, worker);
            break;
        }
        case XLOG_HEAP3_REWRITE:
            break;
        case XLOG_HEAP3_INVALID:
            break;
        default:
            ereport(WARNING,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unexpected RM_HEAP3_ID record type: %u", info)));
    }
}

void ParseUheapOp(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker)
{
    uint8 info = XLogRecGetInfo(buf->record) & XLOG_UHEAP_OPMASK;

    switch (info) {
        case XLOG_UHEAP_INSERT:
            ParseUInsert(ctx, buf, worker);
            break;

        case XLOG_UHEAP_UPDATE:
            ParseUUpdate(ctx, buf, worker);
            break;

        case XLOG_UHEAP_DELETE:
            ParseUDelete(ctx, buf, worker);
            break;

        case XLOG_UHEAP_FREEZE_TD_SLOT:
        case XLOG_UHEAP_INVALID_TD_SLOT:
        case XLOG_UHEAP_CLEAN:
            break;

        case XLOG_UHEAP_MULTI_INSERT:
            ParseUMultiInsert(ctx, buf, worker);
            break;

        default:
            ereport(WARNING,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unexpected RM_HEAP3_ID record type: %u", info)));
    }
}

/*
 * Parse XLOG_HEAP_INSERT (not MULTI_INSERT!) records into decode change queue.
 *
 * Deletes can contain the new tuple.
 */
void ParseInsertXlog(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker)
{
    XLogReaderState *r = buf->record;
    xl_heap_insert *xlrec = NULL;
    ParallelReorderBufferChange *change = NULL;
    RelFileNode target_node = {0, 0, 0, 0};
    xlrec = (xl_heap_insert *)GetXlrec(r);
    CommitSeqNo curCSN = *(CommitSeqNo *)((char *)xlrec + SizeOfHeapInsert);
    int rc = 0;
    bool needDecode = false;
    /* only interested in our database */
    Size tuplelen;
    char *tupledata = XLogRecGetBlockData(r, 0, &tuplelen);
    int slotId = worker->slotId;

    if (tuplelen == 0 && !AllocSizeIsValid(tuplelen)) {
        ereport(ERROR, (errmsg("ParseInsertXlog tuplelen is invalid(%lu), don't decode it", tuplelen)));
        return;
    }
    XLogRecGetBlockTag(r, 0, &target_node, NULL, NULL);
    if (target_node.dbNode != ctx->slot->data.database) {
        return;
    }

    /* output plugin doesn't look for this origin, no need to queue */
    if (ParallelFilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }

    /*
     * Reuse the origin field in the log. If the highest bit is non-zero,
     * the current table is considered to be toast table.
     */
    bool istoast = (((uint32)(XLogRecGetOrigin(r)) & TOAST_FLAG) != 0);
    if (istoast) {
        ereport(DEBUG2, (errmsg("ParallelDecodeInsert %d", istoast)));
    }

    change = ParallelReorderBufferGetChange(ctx->reorder, slotId);
    change->action = PARALLEL_REORDER_BUFFER_CHANGE_INSERT;
    change->lsn = ctx->reader->ReadRecPtr;
    change->xid = XLogRecGetXid(r);
    rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &target_node, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");

    if (xlrec->flags & XLH_INSERT_CONTAINS_NEW_TUPLE) {
        change->data.tp.newtuple = ParallelReorderBufferGetTupleBuf(ctx->reorder, tuplelen, worker, true);
        DecodeXLogTuple(tupledata, tuplelen, change->data.tp.newtuple, true);
    }
    change->data.tp.snapshotcsn = curCSN;
    change->data.tp.clear_toast_afterwards = true;

    SplicingToastTuple(change, ctx, &needDecode, istoast, worker, true);

    if (needDecode) {
        PutChangeQueue(slotId, change);
    }
}

/*
 * Parse XLOG_UHEAP_INSERT (not MULTI_INSERT!) records into decode change queue.
 *
 * Deletes can contain the new tuple.
 */
void ParseUInsert(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker)
{
    XLogReaderState *r = buf->record;
    XlUHeapInsert *xlrec = NULL;
    ParallelReorderBufferChange *change = NULL;
    RelFileNode target_node = {0, 0, 0, 0};
    xlrec = (XlUHeapInsert *)UGetXlrec(r);
    CommitSeqNo curCSN = *(CommitSeqNo *)((char *)xlrec + SizeOfUHeapInsert);
    bool needDecode = false;

    /* only interested in our database */
    Size tuplelen;
    char *tupledata = XLogRecGetBlockData(r, 0, &tuplelen);
    int slotId = worker->slotId;

    if (tuplelen == 0 && !AllocSizeIsValid(tuplelen)) {
        ereport(ERROR, (errmsg("ParseUinsert tuplelen is invalid(%lu), don't decode it", tuplelen)));
        return;
    }
    XLogRecGetBlockTag(r, 0, &target_node, NULL, NULL);

    if (target_node.dbNode != ctx->slot->data.database) {
        return;
    }

    /* output plugin doesn't look for this origin, no need to queue */
    if (ParallelFilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }

    change = ParallelReorderBufferGetChange(ctx->reorder, slotId);
    change->action = PARALLEL_REORDER_BUFFER_CHANGE_UINSERT;
    change->lsn = ctx->reader->ReadRecPtr;
    change->xid = UHeapXlogGetCurrentXid(r, true);
    errno_t rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &target_node, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");

    if (xlrec->flags & XLH_INSERT_CONTAINS_NEW_TUPLE) {
        change->data.tp.newtuple = ParallelReorderBufferGetTupleBuf(ctx->reorder, tuplelen, worker, false);
        DecodeXLogTuple(tupledata, tuplelen, change->data.tp.newtuple, false);
    }
    change->data.tp.snapshotcsn = curCSN;
    change->data.tp.clear_toast_afterwards = true;

    bool isToast = (((uint32)(XLogRecGetOrigin(r)) & TOAST_FLAG) != 0);
    SplicingToastTuple(change, ctx, &needDecode, isToast, worker, false);
    if (needDecode) {
        PutChangeQueue(slotId, change);
    }
}

/*
 * Parse XLOG_HEAP_UPDATE records into decode change queue.
 *
 * Updates can possibly contain a new tuple and the old primary key.
 */
void ParseUpdateXlog(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker)
{
    XLogReaderState *r = buf->record;
    xl_heap_update *xlrec = NULL;
    ParallelReorderBufferChange *change = NULL;
    RelFileNode target_node = {0, 0, 0, 0};
    Size datalen_new;
    Size tuplelen_new;
    char *data_new = NULL;
    Size datalen_old;
    Size tuplelen_old;
    char *data_old = NULL;
    int slotId = worker->slotId;
    bool is_init = false;
    bool needDecode = false;

    Size heapUpdateSize = 0;
    if ((XLogRecGetInfo(r) & XLOG_TUPLE_LOCK_UPGRADE_FLAG) == 0) {
        heapUpdateSize = SizeOfOldHeapUpdate;
    } else {
        heapUpdateSize = SizeOfHeapUpdate;
    }
    xlrec = (xl_heap_update *)GetXlrec(r);
    CommitSeqNo curCSN = *(CommitSeqNo *)((char *)xlrec + heapUpdateSize);
    int rc = 0;
    XLogRecGetBlockTag(r, 0, &target_node, NULL, NULL);

    /* only interested in our database */
    if (target_node.dbNode != ctx->slot->data.database) {
        return;
    }

    data_new = XLogRecGetBlockData(r, 0, &datalen_new);
    tuplelen_new = datalen_new - SizeOfHeapHeader;
    if (tuplelen_new == 0 && !AllocSizeIsValid(tuplelen_new)) {
        ereport(WARNING, (errmsg("tuplelen is invalid(%lu), tuplelen, don't decode it", tuplelen_new)));
        return;
    }

    /* adapt 64 xid, if this tuple is the first tuple of a new page */
    is_init = (XLogRecGetInfo(r) & XLOG_HEAP_INIT_PAGE) != 0;
    /* caution, remaining data in record is not aligned */
    data_old = (char *)xlrec + heapUpdateSize + sizeof(CommitSeqNo);
    if (is_init) {
        datalen_old = XLogRecGetDataLen(r) - heapUpdateSize - sizeof(CommitSeqNo) - sizeof(TransactionId);
    } else {
        datalen_old = XLogRecGetDataLen(r) - heapUpdateSize - sizeof(CommitSeqNo);
    }
    tuplelen_old = datalen_old - SizeOfHeapHeader;
    if (tuplelen_old == 0 && !AllocSizeIsValid(tuplelen_old)) {
        ereport(WARNING, (errmsg("tuplelen is invalid(%lu), tuplelen, don't decode it", tuplelen_old)));
        return;
    }

    /* output plugin doesn't look for this origin, no need to queue */
    if (ParallelFilterByOrigin(ctx, XLogRecGetOrigin(r)))
        return;

    /*
     * Reuse the origin field in the log. If the highest bit is non-zero,
     * the current table is considered to be toast table.
     */
    bool istoast = (((uint32)(XLogRecGetOrigin(r)) & TOAST_FLAG) != 0);

    change = ParallelReorderBufferGetChange(ctx->reorder, slotId);
    change->action = PARALLEL_REORDER_BUFFER_CHANGE_UPDATE;
    change->lsn = ctx->reader->ReadRecPtr;
    change->xid = XLogRecGetXid(r);
    rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &target_node, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");

    if (xlrec->flags & XLH_UPDATE_CONTAINS_NEW_TUPLE) {
        change->data.tp.newtuple = ParallelReorderBufferGetTupleBuf(ctx->reorder, tuplelen_new, worker, true);
        DecodeXLogTuple(data_new, datalen_new, change->data.tp.newtuple, true);
    }
    if (xlrec->flags & XLH_UPDATE_CONTAINS_OLD) {
        change->data.tp.oldtuple = ParallelReorderBufferGetTupleBuf(ctx->reorder, tuplelen_old, worker, true);
        DecodeXLogTuple(data_old, datalen_old, change->data.tp.oldtuple, true);
    }

    change->data.tp.snapshotcsn = curCSN;
    change->data.tp.clear_toast_afterwards = true;

    SplicingToastTuple(change, ctx, &needDecode, istoast, worker, true);

    if (needDecode) {
        PutChangeQueue(slotId, change);
    }
}

/*
 * Parse XLOG_UHEAP_UPDATE records into decode change queue.
 *
 * Deletes can possibly contain the old primary key.
 */

void ParseUUpdate(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker)
{
    XLogReaderState *r = buf->record;
    RelFileNode targetNode = {0, 0, 0, 0};
    XlUHeapUpdate *xlrec = (XlUHeapUpdate *)UGetXlrec(r);
    CommitSeqNo curCSN = *(CommitSeqNo *)((char *)xlrec + SizeOfUHeapUpdate);
    int slotId = worker->slotId;
    bool isInplaceUpdate = (xlrec->flags & XLZ_NON_INPLACE_UPDATE) == 0;
    bool needDecode = false;
    if (ParallelFilterRecord(ctx, r, xlrec->flags, &targetNode)) {
        return;
    }
    Size datalenNew = 0;
    char *dataNew = XLogRecGetBlockData(r, 0, &datalenNew);

    if (datalenNew == 0 && !AllocSizeIsValid(datalenNew)) {
        ereport(WARNING, (errmsg("tuplelen is invalid(%lu), don't decode it", datalenNew)));
        return;
    }

    Size tuplelenOld = XLogRecGetDataLen(r) - SizeOfUHeapUpdate - sizeof(CommitSeqNo);
    char *dataOld = (char *)xlrec + SizeOfUHeapUpdate + sizeof(CommitSeqNo);
    UpdateOldTupleCalc(isInplaceUpdate, r, &dataOld, &tuplelenOld);

    if (tuplelenOld == 0 && !AllocSizeIsValid(tuplelenOld)) {
        ereport(ERROR, (errmsg("tuplelen is invalid(%lu), don't decode it", tuplelenOld)));
        return;
    }

    bool isToast = (((uint32)(XLogRecGetOrigin(r)) & TOAST_FLAG) != 0);
    ParallelReorderBufferChange *change = ParallelReorderBufferGetChange(ctx->reorder, slotId);
    change->action = PARALLEL_REORDER_BUFFER_CHANGE_UUPDATE;
    change->lsn = ctx->reader->ReadRecPtr;
    change->xid = UHeapXlogGetCurrentXid(r, true);

    int rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &targetNode, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");
    change->data.tp.newtuple = ParallelReorderBufferGetTupleBuf(ctx->reorder, datalenNew, worker, false);

    DecodeXLogTuple(dataNew, datalenNew, change->data.tp.newtuple, false);
    if (xlrec->flags & XLZ_HAS_UPDATE_UNDOTUPLE) {
        change->data.tp.oldtuple = ParallelReorderBufferGetTupleBuf(ctx->reorder, tuplelenOld, worker, false);
        if (!isInplaceUpdate) {
            DecodeXLogTuple(dataOld, tuplelenOld, change->data.tp.oldtuple, false);
        } else if ((xlrec->flags & XLOG_UHEAP_CONTAINS_OLD_HEADER) != 0) {
            int undoXorDeltaSize = *(int *)dataOld;
            dataOld += sizeof(int) + undoXorDeltaSize;
            tuplelenOld -= sizeof(int) + undoXorDeltaSize;
            DecodeXLogTuple(dataOld, tuplelenOld, change->data.tp.oldtuple, false);
        } else {
            ereport(LOG, (errmsg("current tuple is not fully logged, don't decode it")));
            return;
        }
    }
    change->data.tp.snapshotcsn = curCSN;
    change->data.tp.clear_toast_afterwards = true;

    SplicingToastTuple(change, ctx, &needDecode, isToast, worker, false);
    if (needDecode) {
        PutChangeQueue(slotId, change);
    }
}

/*
 * Parse XLOG_HEAP_UPDATE records into decode change queue.
 *
 * Deletes can possibly contain the old primary key.
 */
void ParseDeleteXlog(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker)
{
    XLogReaderState *r = buf->record;
    xl_heap_delete *xlrec = NULL;
    
    int slotId = worker->slotId;
    ParallelReorderBufferChange *change = NULL;
    RelFileNode target_node = {0, 0, 0, 0};
    int rc = 0;
    Size datalen = 0;
    bool needDecode = false;
    bool istoast = (((uint32)(XLogRecGetOrigin(r)) & TOAST_FLAG) != 0);
    if (istoast) {
        ereport(DEBUG2, (errmsg("ParallelDecodeDelete %d", istoast)));
    }

    Size heapDeleteSize = 0;
    if ((XLogRecGetInfo(r) & XLOG_TUPLE_LOCK_UPGRADE_FLAG) == 0) {
        heapDeleteSize = SizeOfOldHeapDelete;
    } else {
        heapDeleteSize = SizeOfHeapDelete;
    }
    xlrec = (xl_heap_delete *)GetXlrec(r);
    CommitSeqNo curCSN = *(CommitSeqNo *)((char *)xlrec + heapDeleteSize);

    /* only interested in our database */
    XLogRecGetBlockTag(r, 0, &target_node, NULL, NULL);
    if (target_node.dbNode != ctx->slot->data.database) {
        return;
    }
    /* output plugin doesn't look for this origin, no need to queue */
    if (ParallelFilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }

    datalen = XLogRecGetDataLen(r) - heapDeleteSize;
    if (datalen == 0 && !AllocSizeIsValid(datalen)) {
        ereport(WARNING, (errmsg("tuplelen is invalid(%lu), tuplelen, don't decode it", datalen)));
        return;
    }

    change = ParallelReorderBufferGetChange(ctx->reorder, slotId);
    change->action = PARALLEL_REORDER_BUFFER_CHANGE_DELETE;
    change->lsn = ctx->reader->ReadRecPtr;
    change->xid = XLogRecGetXid(r);
    rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &target_node, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");

    /* old primary key stored */
    if (xlrec->flags & XLH_DELETE_CONTAINS_OLD) {
        Assert(XLogRecGetDataLen(r) > (heapDeleteSize + SizeOfHeapHeader));
        change->data.tp.oldtuple = ParallelReorderBufferGetTupleBuf(ctx->reorder, datalen, worker, true);

        DecodeXLogTuple((char *)xlrec + heapDeleteSize + sizeof(CommitSeqNo), datalen - sizeof(CommitSeqNo),
            change->data.tp.oldtuple, true);
    }
    change->data.tp.snapshotcsn = curCSN;
    change->data.tp.clear_toast_afterwards = true;

    SplicingToastTuple(change, ctx, &needDecode, istoast, worker, true);
    if (needDecode) {
        PutChangeQueue(slotId, change);
    }
}

void ParseUDelete(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker)
{
    XLogReaderState *r = buf->record;
    XlUHeapDelete *xlrec = NULL;
    int slotId = worker->slotId;
    RelFileNode targetNode = {0, 0, 0, 0};
    bool needDecode = false;
    bool isToast = (((uint32)(XLogRecGetOrigin(r)) & TOAST_FLAG) != 0);
    xlrec = (XlUHeapDelete *)UGetXlrec(r);
    CommitSeqNo curCSN = *(CommitSeqNo *)((char *)xlrec + SizeOfUHeapDelete);

    XLogRecGetBlockTag(r, 0, &targetNode, NULL, NULL);
    if (targetNode.dbNode != ctx->slot->data.database) {
        return;
    }
    /* output plugin doesn't look for this origin, no need to queue */
    if (ParallelFilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }

    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapDelete + sizeof(CommitSeqNo));
    Size datalen = XLogRecGetDataLen(r) - SizeOfUHeapDelete - SizeOfXLUndoHeader - sizeof(CommitSeqNo);
    Size addLen = 0;
    UpdateUndoBody(&addLen, xlundohdr->flag);

    Size metaLen = DecodeUndoMeta((char*)xlrec + SizeOfUHeapDelete + sizeof(CommitSeqNo) + SizeOfXLUndoHeader + addLen);
    addLen += metaLen;

    if (datalen == 0 && !AllocSizeIsValid(datalen)) {
        ereport(WARNING, (errmsg("tuplelen is invalid(%lu), don't decode it", datalen)));
        return;
    }

    ParallelReorderBufferChange* change = ParallelReorderBufferGetChange(ctx->reorder, slotId);
    change->action = PARALLEL_REORDER_BUFFER_CHANGE_UDELETE;
    change->lsn = ctx->reader->ReadRecPtr;
    change->xid = UHeapXlogGetCurrentXid(r, true);

    int rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &targetNode, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");

    change->data.tp.oldtuple = ParallelReorderBufferGetTupleBuf(ctx->reorder, datalen, worker, false);
    DecodeXLogTuple((char *)xlrec + SizeOfUHeapDelete + sizeof(CommitSeqNo) + SizeOfXLUndoHeader + addLen,
        datalen - addLen, change->data.tp.oldtuple, false);

    change->data.tp.snapshotcsn = curCSN;
    change->data.tp.clear_toast_afterwards = true;

    SplicingToastTuple(change, ctx, &needDecode, isToast, worker, false);
    if (needDecode) {
        PutChangeQueue(slotId, change);
    }
}

void ParseMultiInsert(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker)
{
    XLogReaderState *r = buf->record;
    xl_heap_multi_insert *xlrec = NULL;
    int i = 0;
    char *data = NULL;
    char *tupledata = NULL;
    Size tuplelen = 0;
    RelFileNode rnode = {0, 0, 0, 0};
    int rc = 0;
    xlrec = (xl_heap_multi_insert *)GetXlrec(r);
    CommitSeqNo curCSN = *(CommitSeqNo *)((char *)xlrec + SizeOfHeapMultiInsert);
    int slotId = worker->slotId;
    if (xlrec->isCompressed)
        return;
    /* only interested in our database */
    XLogRecGetBlockTag(r, 0, &rnode, NULL, NULL);
    if (rnode.dbNode != ctx->slot->data.database)
        return;
    /* output plugin doesn't look for this origin, no need to queue */
    if (ParallelFilterByOrigin(ctx, XLogRecGetOrigin(r)))
        return;

    tupledata = XLogRecGetBlockData(r, 0, &tuplelen);
    data = tupledata;
    for (i = 0; i < xlrec->ntuples; i++) {
        ParallelReorderBufferChange *change = NULL;
        xl_multi_insert_tuple *xlhdr = NULL;
        int datalen = 0;
        ReorderBufferTupleBuf *tuple = NULL;

        change = ParallelReorderBufferGetChange(ctx->reorder, slotId);
        change->action = PARALLEL_REORDER_BUFFER_CHANGE_INSERT;
        change->lsn = ctx->reader->ReadRecPtr;
        change->xid = XLogRecGetXid(r);
        rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &rnode, sizeof(RelFileNode));
        securec_check(rc, "", "");

        /*
         * CONTAINS_NEW_TUPLE will always be set currently as multi_insert
         * isn't used for catalogs, but better be future proof.
         *
         * We decode the tuple in pretty much the same way as DecodeXLogTuple,
         * but since the layout is slightly different, we can't use it here.
         */
        if (xlrec->flags & XLH_INSERT_CONTAINS_NEW_TUPLE) {
            HeapTupleHeader header;
            xlhdr = (xl_multi_insert_tuple *)SHORTALIGN(data);
            data = ((char *)xlhdr) + SizeOfMultiInsertTuple;
            datalen = xlhdr->datalen;
            if (datalen != 0 && AllocSizeIsValid((uint)datalen)) {
                change->data.tp.newtuple = ParallelReorderBufferGetTupleBuf(ctx->reorder, datalen, worker, true);
                tuple = change->data.tp.newtuple;
                header = tuple->tuple.t_data;

                /* not a disk based tuple */
                ItemPointerSetInvalid(&tuple->tuple.t_self);

                /*
                 * We can only figure this out after reassembling the
                 * transactions.
                 */
                tuple->tuple.t_bucketId = InvalidBktId;
                tuple->tuple.t_tableOid = InvalidOid;
                tuple->tuple.t_len = datalen + SizeofHeapTupleHeader;

                rc = memset_s(header, SizeofHeapTupleHeader, 0, SizeofHeapTupleHeader);
                securec_check(rc, "\0", "\0");
                rc = memcpy_s((char *)tuple->tuple.t_data + SizeofHeapTupleHeader, datalen, (char *)data, datalen);
                securec_check(rc, "\0", "\0");

                header->t_hoff = xlhdr->t_hoff;
                header->t_infomask = xlhdr->t_infomask;
                header->t_infomask2 = xlhdr->t_infomask2;
            } else {
                ereport(ERROR, (errmsg("tuplelen is invalid(%d), tuplelen, don't decode it", datalen)));
                return;
            }
            data += datalen;
        }
        /*
         * Reset toast reassembly state only after the last row in the last
         * xl_multi_insert_tuple record emitted by one heap_multi_insert()
         * call.
         */
        if ((xlrec->flags & XLH_INSERT_LAST_IN_MULTI) && ((i + 1) == xlrec->ntuples)) {
            change->data.tp.clear_toast_afterwards = true;
        } else {
            change->data.tp.clear_toast_afterwards = false;
        }
        change->data.tp.snapshotcsn = curCSN;

        PutChangeQueue(slotId, change);
    }
}

void ParseUMultiInsert(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker)
{
    XLogReaderState *r = buf->record;
    Size tuplelen = 0;
    RelFileNode rnode = {0, 0, 0, 0};
    CommitSeqNo curCSN = 0;
    XlUHeapMultiInsert *xlrec = (XlUHeapMultiInsert *)UGetMultiInsertXlrec(r, &curCSN);
    int slotId = worker->slotId;

    XLogRecGetBlockTag(r, 0, &rnode, NULL, NULL);
    if (rnode.dbNode != ctx->slot->data.database) {
        return;
    }
    /* output plugin doesn't look for this origin, no need to queue */
    if (ParallelFilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }
    char *data = XLogRecGetBlockData(r, 0, &tuplelen);
    for (int i = 0; i < xlrec->ntuples; i++) {
        ParallelReorderBufferChange *change = ParallelReorderBufferGetChange(ctx->reorder, slotId);
        change->action = PARALLEL_REORDER_BUFFER_CHANGE_UINSERT;
        change->lsn = ctx->reader->ReadRecPtr;
        change->xid = UHeapXlogGetCurrentXid(r, true);
        int rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &rnode, sizeof(RelFileNode));
        securec_check(rc, "", "");

        /*
         * CONTAINS_NEW_TUPLE will always be set currently as multi_insert
         * isn't used for catalogs, but better be future proof.
         *
         * We decode the tuple in pretty much the same way as DecodeXLogTuple,
         * but since the layout is slightly different, we can't use it here.
         */
        if (xlrec->flags & XLOG_UHEAP_CONTAINS_NEW_TUPLE) {
            XlMultiInsertUTuple *xlhdr = (XlMultiInsertUTuple *)data;
            data = ((char *)xlhdr) + SizeOfMultiInsertUTuple;
            int len = xlhdr->datalen;
            if (len != 0 && AllocSizeIsValid((uint)len)) {
                change->data.tp.newtuple = ParallelReorderBufferGetTupleBuf(ctx->reorder, len, worker, false);
                ReorderBufferTupleBuf* tuple = change->data.tp.newtuple;
                UHeapTupleData utuple = *(UHeapTupleData *)(&tuple->tuple);
                UHeapDiskTuple header = utuple.disk_tuple;

                /* not a disk based tuple */
                ItemPointerSetInvalid(&utuple.ctid);

                /*
                 * We can only figure this out after reassembling the
                 * transactions.
                 */
                utuple.table_oid = InvalidOid;
                utuple.t_bucketId = InvalidBktId;
                utuple.disk_tuple_size = len + SizeOfUHeapDiskTupleData;

                rc = memset_s(header, SizeOfUHeapDiskTupleData, 0, SizeOfUHeapDiskTupleData);
                securec_check(rc, "\0", "\0");
                rc = memcpy_s((char *)utuple.disk_tuple + SizeOfUHeapDiskTupleData, len, (char *)data, len);
                securec_check(rc, "\0", "\0");

                header->flag = xlhdr->flag;
                header->flag2 = xlhdr->flag2;
                header->t_hoff = xlhdr->t_hoff;
            } else {
                ereport(ERROR, (errmsg("tuplelen is invalid(%d), don't decode it", len)));
                return;
            }
            data += len;
        }

        /*
         * Reset toast reassembly state only after the last row in the last
         * xl_multi_insert_tuple record emitted by one heap_multi_insert()
         * call.
         */
        if ((i + 1) == xlrec->ntuples) {
            change->data.tp.clear_toast_afterwards = true;
        } else {
            change->data.tp.clear_toast_afterwards = false;
        }
        change->data.tp.snapshotcsn = curCSN;
        PutChangeQueue(slotId, change);
    }
}

/*
 * Handle rmgr STANDBY_ID records for DecodeRecordIntoReorderBuffer().
 */
void ParseStandbyOp(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker)
{
    XLogReaderState *r = buf->record;
    uint8 info = XLogRecGetInfo(r) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_RUNNING_XACTS: {
            xl_running_xacts *running = (xl_running_xacts *)buf->record_data;
            ParseRunningXactsXlog(ctx, buf->origptr, running, worker);
        } break;
        case XLOG_STANDBY_CSN:
        case XLOG_STANDBY_LOCK:
        case XLOG_STANDBY_CSN_ABORTED:
        case XLOG_STANDBY_CSN_COMMITTING:
            break;
        default:
            ereport(WARNING, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected RM_STANDBY_ID record type: %u", info)));
    }
}

/*
 * Take every XLogReadRecord()ed record and perform the actions required to
 * decode it using the output plugin already setup in the logical decoding
 * context.
 *
 * We also support the ability to fast forward thru records, skipping some
 * record types completely - see individual record types for details.
 */
void ParseProcessRecord(ParallelLogicalDecodingContext *ctx, XLogReaderState *record, ParallelDecodeReaderWorker *worker)
{
    XLogRecordBuffer buf = { 0, 0, NULL, NULL };

    buf.origptr = ctx->reader->ReadRecPtr;
    buf.endptr = ctx->reader->EndRecPtr;
    buf.record = record;
    buf.record_data = GetXlrec(record);

    /* cast so we get a warning when new rmgrs are added */
    switch ((RmgrIds)XLogRecGetRmid(record)) {
        /*
         * Rmgrs we care about for logical decoding. Add new rmgrs in
         * rmgrlist.h's order.
         */
        case RM_XLOG_ID:
            break;

        case RM_XACT_ID:
            ParseXactOp(ctx, &buf, worker);
            break;

        case RM_STANDBY_ID:
            ParseStandbyOp(ctx, &buf, worker);
            break;

        case RM_HEAP2_ID:
            ParseHeap2Op(ctx, &buf, worker);
            break;

        case RM_HEAP_ID:
            ParseHeapOp(ctx, &buf, worker);
            break;
        case RM_HEAP3_ID:
            ParseHeap3Op(ctx, &buf, worker);
            break;
        case RM_UHEAP_ID:
            ParseUheapOp(ctx, &buf, worker);
            break;
        default:
            break;
    }
}

/*
 * Handle rmgr XACT_ID records for parsing records into ParallelReorderBuffer.
 */
void ParseXactOp(ParallelLogicalDecodingContext *ctx, XLogRecordBuffer *buf, ParallelDecodeReaderWorker *worker)
{
    XLogReaderState *r = buf->record;
    uint8 info = XLogRecGetInfo(r) & ~XLR_INFO_MASK;
    /*
     * No point in doing anything yet, data could not be decoded anyway. It's
     * ok not to call ReorderBufferProcessXid() in that case, except in the
     * assignment case there'll not be any later records with the same xid;
     * and in the assignment case we'll not decode those xacts.
     */

    switch (info) {
        case XLOG_XACT_COMMIT: {
            xl_xact_commit *xlrec = NULL;
            TransactionId *subxacts = NULL;
            SharedInvalidationMessage *invals = NULL;

            xlrec = (xl_xact_commit *)buf->record_data;

            subxacts = (TransactionId *)&(xlrec->xnodes[xlrec->nrels]);
            invals = (SharedInvalidationMessage *)&(subxacts[xlrec->nsubxacts]);

            ParseCommitXlog(ctx, buf, XLogRecGetXid(r), xlrec->csn, xlrec->dbId, xlrec->xact_time, xlrec->nsubxacts,
                         subxacts, xlrec->nmsgs, invals, worker);

            break;
        }
        case XLOG_XACT_COMMIT_PREPARED: {
            /* Prepared commits contain a normal commit record... */
            xl_xact_commit_prepared *prec = (xl_xact_commit_prepared *)buf->record_data;
            xl_xact_commit *xlrec = &prec->crec;

            TransactionId *subxacts = (TransactionId *)&(xlrec->xnodes[xlrec->nrels]);
            SharedInvalidationMessage *invals = (SharedInvalidationMessage *)&(subxacts[xlrec->nsubxacts]);

            ParseCommitXlog(ctx, buf, prec->xid, xlrec->csn, xlrec->dbId, xlrec->xact_time, xlrec->nsubxacts, subxacts,
                         xlrec->nmsgs, invals, worker);
            break;
        }
        case XLOG_XACT_COMMIT_COMPACT: {
            xl_xact_commit_compact *xlrec = NULL;

            xlrec = (xl_xact_commit_compact *)buf->record_data;
            
            ParseCommitXlog(ctx, buf, XLogRecGetXid(r), xlrec->csn, InvalidOid, xlrec->xact_time, xlrec->nsubxacts,
                xlrec->subxacts, 0, NULL, worker);
            break;
        }
        case XLOG_XACT_ABORT: {
            xl_xact_abort *xlrec = NULL;
            TransactionId *sub_xids = NULL;

            xlrec = (xl_xact_abort *)buf->record_data;

            sub_xids = (TransactionId *)&(xlrec->xnodes[xlrec->nrels]);

            ParseAbortXlog(ctx, buf->origptr, XLogRecGetXid(r), sub_xids, xlrec->nsubxacts, worker);
            break;
        }
        case XLOG_XACT_ABORT_WITH_XID: {
            xl_xact_abort *xlrec = (xl_xact_abort *)buf->record_data;
            TransactionId curId = XLogRecGetXid(r);
            TransactionId *sub_xids = (TransactionId *)(&(xlrec->xnodes[xlrec->nrels]));
            curId = *(TransactionId *)((char*)&(xlrec->xnodes[xlrec->nrels]) +
                (unsigned)(xlrec->nsubxacts) * sizeof(TransactionId));

            ParseAbortXlog(ctx, buf->origptr, curId, sub_xids, xlrec->nsubxacts, worker);
            break;
        }
        case XLOG_XACT_ABORT_PREPARED: {
            xl_xact_abort_prepared *prec = NULL;
            xl_xact_abort *xlrec = NULL;
            TransactionId *sub_xids = NULL;
            
            /* prepared abort contain a normal commit abort... */
            prec = (xl_xact_abort_prepared *)buf->record_data;
            xlrec = &prec->arec;
            
            sub_xids = (TransactionId *)&(xlrec->xnodes[xlrec->nrels]);
            
            /* r->xl_xid is committed in a separate record */
            ParseAbortXlog(ctx, buf->origptr, XLogRecGetXid(r), sub_xids, xlrec->nsubxacts, worker);
            break;
        }
        case XLOG_XACT_ASSIGNMENT:
        case XLOG_XACT_PREPARE:
            break;
        default:
            ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected RM_XACT_ID record type: %u", info)));
    }
}

