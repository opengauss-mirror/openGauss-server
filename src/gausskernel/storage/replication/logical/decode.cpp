/* -------------------------------------------------------------------------
 *
 * decode.cpp
 *	  This module decodes WAL records read using xlogreader.h's APIs for the
 *	  purpose of logical decoding by passing information to the
 *	  reorderbuffer module (containing the actual changes) and to the
 *	  snapbuild module to build a fitting catalog snapshot (to be able to
 *	  properly decode the changes in the reorderbuffer).
 *
 * NOTE:
 *	  This basically tries to handle all low level xlog stuff for
 *	  reorderbuffer.c and snapbuild.c. There's some minor leakage where a
 *	  specific record's struct is used to pass data along, but those just
 *	  happen to contain the right amount of data in a convenient
 *	  format. There isn't and shouldn't be much intelligence about the
 *	  contents of records in here except turning them into a more usable
 *	  format.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	src/gausskernel/storage/replication/logical/decode.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"

#include "catalog/pg_control.h"

#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/origin.h"
#include "replication/snapbuild.h"
#include "replication/ddlmessage.h"
#include "storage/standby.h"

#include "utils/memutils.h"
#include "utils/relfilenodemap.h"
#include "utils/lsyscache.h"


/* RMGR Handlers */
static void DecodeXLogOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeHeapOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void AreaDecodeHeapOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

static void DecodeHeap2Op(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void AreaDecodeHeap2Op(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

static void DecodeHeap3Op(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void DecodeUheapOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void AreaDecodeUheapOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

static void DecodeXactOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void AreaDecodeXactOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

static void DecodeStandbyOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

/* individual record(group)'s handlers */
static void DecodeInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void AreaDecodeInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

static void DecodeUInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void AreaDecodeUInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

static void DecodeUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void AreaDecodeUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

static void DecodeUUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void AreaDecodeUUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

static void DecodeDelete(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void AreaDecodeDelete(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

static void DecodeUDelete(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void AreaDecodeUDelete(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

static void DecodeMultiInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void AreaDecodeMultiInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

static void DecodeUMultiInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
static void AreaDecodeUMultiInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);

static void DecodeCommit(LogicalDecodingContext *ctx, XLogRecordBuffer *buf, TransactionId xid, CommitSeqNo csn,
                         Oid dboid, TimestampTz commit_time, int nsubxacts, TransactionId *sub_xids, int ninval_msgs,
                         SharedInvalidationMessage *msg, xl_xact_origin *origin);
static void DecodeAbort(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid, TransactionId *sub_xids,
                        int nsubxacts);

/* common function to decode tuples */
extern ReorderBufferTXN *ReorderBufferTXNByXid(ReorderBuffer *rb, TransactionId xid, bool create, bool *is_new,XLogRecPtr lsn, bool create_as_top);

extern Pointer UGetXlrec(XLogReaderState * record);

Pointer GetXlrec(XLogReaderState *record)
{
    Pointer rec_data;
    bool isinit = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;

    rec_data = (Pointer)XLogRecGetData(record);
    if (isinit) {
        rec_data += sizeof(TransactionId);
    }
    return rec_data;
}

Pointer UGetXlrec(XLogReaderState *record)
{
    Pointer recData = (Pointer)XLogRecGetData(record);
    return recData;
}

size_t DecodeUndoMeta(const char* data)
{
    uint64 info = (*(uint64*)data) & XLOG_UNDOMETA_INFO_SLOT;
    if (info == 0) {
        return sizeof(uint64);
    } else {
        return sizeof(uint64) + sizeof(Oid);
    }
}

Pointer UGetMultiInsertXlrec(XLogReaderState *record, CommitSeqNo* curCSN)
{
    Pointer recData = (Pointer)XLogRecGetData(record);
    bool isinit = (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_PAGE) != 0;
    XlUndoHeader *xlundohdr = (XlUndoHeader *)(recData);
    Size headerLen = SizeOfXLUndoHeader + sizeof(UndoRecPtr);

    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        headerLen += sizeof(UndoRecPtr);
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        headerLen += sizeof(UndoRecPtr);
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        headerLen += sizeof(Oid);
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_CURRENT_XID) != 0) {
        headerLen += sizeof(TransactionId);
    }
    recData += headerLen;
    Size metaLen = DecodeUndoMeta((char*)recData);
    recData += metaLen;
    if (isinit) {
        recData += sizeof(TransactionId) + sizeof(uint16);
    }
    if ((record->decoded_record->xl_term & XLOG_CONTAIN_CSN) != 0) {
        *curCSN = *(CommitSeqNo *)recData;
        recData += sizeof(CommitSeqNo);
    }
    return recData;
}

bool IsRecordProcess(XLogReaderState *record)
{
    switch ((RmgrIds)XLogRecGetRmid(record)) {
        case RM_SMGR_ID:
        case RM_CLOG_ID:
        case RM_DBASE_ID:
        case RM_TBLSPC_ID:
        case RM_MULTIXACT_ID:
        case RM_RELMAP_ID:
        case RM_BTREE_ID:
        case RM_HASH_ID:
        case RM_GIN_ID:
        case RM_GIST_ID:
        case RM_SEQ_ID:
        case RM_SPGIST_ID:
        case RM_SLOT_ID:
        case RM_BARRIER_ID:
        case RM_UHEAP2_ID:
        case RM_UNDOLOG_ID:
        case RM_UHEAPUNDO_ID:
        case RM_UNDOACTION_ID:
        case RM_UBTREE_ID:
        case RM_UBTREE2_ID:
        case RM_SEGPAGE_ID:
        case RM_REPLORIGIN_ID:
#ifdef ENABLE_MOT
        case RM_MOT_ID:
#endif
            return false;
        default:
            return true;
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
void LogicalDecodingProcessRecord(LogicalDecodingContext *ctx, XLogReaderState *record)
{
    XLogRecordBuffer buf = { 0, 0, NULL, NULL };

    buf.origptr = ctx->reader->ReadRecPtr;
    buf.endptr = ctx->reader->EndRecPtr;
    buf.record = record;
    buf.record_data = GetXlrec(record);
    if (!IsRecordProcess(record)) {
        return;
    }

    ResourceOwner tmpOwner = t_thrd.utils_cxt.CurrentResourceOwner;
    /* cast so we get a warning when new rmgrs are added */
    switch ((RmgrIds)XLogRecGetRmid(record)) {
        /*
         * Rmgrs we care about for logical decoding. Add new rmgrs in
         * rmgrlist.h's order.
         */
        case RM_XLOG_ID:
            DecodeXLogOp(ctx, &buf);
            break;

        case RM_XACT_ID:
            DecodeXactOp(ctx, &buf);
            break;

        case RM_STANDBY_ID:
            DecodeStandbyOp(ctx, &buf);
            break;

        case RM_HEAP2_ID:
            DecodeHeap2Op(ctx, &buf);
            break;

        case RM_HEAP_ID:
            DecodeHeapOp(ctx, &buf);
            break;
        case RM_HEAP3_ID:
            DecodeHeap3Op(ctx, &buf);
            break;
        case RM_UHEAP_ID:
            DecodeUheapOp(ctx, &buf);
            break;
        case RM_LOGICALDDLMSG_ID:
            logicalddl_decode(ctx, &buf);
            break;
        default:
            break;
    }
    t_thrd.utils_cxt.CurrentResourceOwner = tmpOwner;
}

/*
 * Process the record for area decoding.
 */
void AreaLogicalDecodingProcessRecord(LogicalDecodingContext *ctx, XLogReaderState *record)
{
    XLogRecordBuffer buf = { 0, 0, NULL, NULL };

    buf.origptr = ctx->reader->ReadRecPtr;
    buf.endptr = ctx->reader->EndRecPtr;
    buf.record = record;
    buf.record_data = GetXlrec(record);
    if(!IsRecordProcess(record)) {
        return;
    }

    /* cast so we get a warning when new rmgrs are added */
    switch ((RmgrIds)XLogRecGetRmid(record)) {
        case RM_XACT_ID:
            AreaDecodeXactOp(ctx, &buf);
            break;
        case RM_HEAP2_ID:
            AreaDecodeHeap2Op(ctx, &buf);
            break;
        case RM_HEAP_ID:
            AreaDecodeHeapOp(ctx, &buf);
            break;
        case RM_XLOG_ID:
        case RM_STANDBY_ID:
        case RM_HEAP3_ID:
            break;
        case RM_UHEAP_ID:
            AreaDecodeUheapOp(ctx, &buf);
            break;
        default:
            ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected rmgr_id: %d", (RmgrIds)XLogRecGetRmid(buf.record))));
    }
}


/*
 * Handle rmgr XLOG_ID records for DecodeRecordIntoReorderBuffer().
 */
static void DecodeXLogOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    SnapBuild *builder = ctx->snapshot_builder;
    XLogReaderState *r = buf->record;
    uint8 info = XLogRecGetInfo(r) & ~XLR_INFO_MASK;

    ReorderBufferProcessXid(ctx->reorder, XLogRecGetXid(buf->record), buf->origptr);

    switch (info) {
        /* this is also used in END_OF_RECOVERY checkpoints */
        case XLOG_CHECKPOINT_SHUTDOWN:
        case XLOG_END_OF_RECOVERY:
            SnapBuildSerializationPoint(builder, buf->origptr);
            break;
        case XLOG_CHECKPOINT_ONLINE:
            /*
             * a RUNNING_XACTS record will have been logged near to this, we
             * can restart from there.
             */
            break;
        case XLOG_NOOP:
        case XLOG_NEXTOID:
        case XLOG_SWITCH:
        case XLOG_BACKUP_END:
        case XLOG_PARAMETER_CHANGE:
        case XLOG_RESTORE_POINT:
        case XLOG_FPW_CHANGE:
        case XLOG_FPI_FOR_HINT:
        case XLOG_FPI:
        case XLOG_DELAY_XLOG_RECYCLE:
            break;
        default:
            ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected RM_XLOG_ID record type: %u", info)));
    }
}

/*
 * Handle rmgr XACT_ID records for DecodeRecordIntoReorderBuffer().
 */
static void DecodeXactOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    SnapBuild *builder = ctx->snapshot_builder;
    ReorderBuffer *reorder = ctx->reorder;
    XLogReaderState *r = buf->record;
    uint8 info = XLogRecGetInfo(r) & ~XLR_INFO_MASK;
    bool compress = (bool)(XLogRecGetInfo(r) & XLR_REL_COMPRESS);
    /*
     * No point in doing anything yet, data could not be decoded anyway. It's
     * ok not to call ReorderBufferProcessXid() in that case, except in the
     * assignment case there'll not be any later records with the same xid;
     * and in the assignment case we'll not decode those xacts.
     */
    if (SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT)
        return;

    switch (info) {
        case XLOG_XACT_COMMIT: {
            xl_xact_commit *xlrec = NULL;
            TransactionId *subxacts = NULL;
            SharedInvalidationMessage *invals = NULL;
            xl_xact_origin *origin = NULL;

            xlrec = (xl_xact_commit *)buf->record_data;

            subxacts = GET_SUB_XACTS(xlrec->xnodes, xlrec->nrels, compress);
            invals = (SharedInvalidationMessage *)&(subxacts[xlrec->nsubxacts]);
            if (xlrec->xinfo | XACT_HAS_ORIGIN) {
                origin = (xl_xact_origin*)GetRepOriginPtr((char*)xlrec->xnodes, xlrec->xinfo,
                    xlrec->nsubxacts, xlrec->nmsgs, xlrec->nrels, xlrec->nlibrary, compress);
            }

            DecodeCommit(ctx, buf, XLogRecGetXid(r), xlrec->csn, xlrec->dbId, xlrec->xact_time, xlrec->nsubxacts,
                         subxacts, xlrec->nmsgs, invals, origin);

            break;
        }
        case XLOG_XACT_COMMIT_PREPARED: {
            xl_xact_commit_prepared *prec = NULL;
            xl_xact_commit *xlrec = NULL;
            TransactionId *subxacts = NULL;
            SharedInvalidationMessage *invals = NULL;
            xl_xact_origin *origin = NULL;

            /* Prepared commits contain a normal commit record... */
            prec = (xl_xact_commit_prepared *)buf->record_data;
            xlrec = &prec->crec;

            subxacts = GET_SUB_XACTS(xlrec->xnodes, xlrec->nrels, compress);
            invals = (SharedInvalidationMessage *)&(subxacts[xlrec->nsubxacts]);

            if (xlrec->xinfo | XACT_HAS_ORIGIN) {
                origin = (xl_xact_origin*)GetRepOriginPtr((char*)xlrec->xnodes, xlrec->xinfo,
                    xlrec->nsubxacts, xlrec->nmsgs, xlrec->nrels, xlrec->nlibrary, compress);
            }

            DecodeCommit(ctx, buf, prec->xid, xlrec->csn, xlrec->dbId, xlrec->xact_time, xlrec->nsubxacts, subxacts,
                         xlrec->nmsgs, invals, origin);

            break;
        }
        case XLOG_XACT_COMMIT_COMPACT: {
            xl_xact_commit_compact *xlrec = NULL;

            xlrec = (xl_xact_commit_compact *)buf->record_data;

            DecodeCommit(ctx, buf, XLogRecGetXid(r), xlrec->csn, InvalidOid, xlrec->xact_time, xlrec->nsubxacts,
                         xlrec->subxacts, 0, NULL, NULL);
            break;
        }
        case XLOG_XACT_ABORT: {
            xl_xact_abort *xlrec = NULL;
            TransactionId *sub_xids = NULL;

            xlrec = (xl_xact_abort *)buf->record_data;
            sub_xids = GET_SUB_XACTS(xlrec->xnodes, xlrec->nrels, compress);

            DecodeAbort(ctx, buf->origptr, XLogRecGetXid(r), sub_xids, xlrec->nsubxacts);
            break;
        }
        case XLOG_XACT_ABORT_WITH_XID: {
            xl_xact_abort *xlrec = NULL;
            TransactionId *sub_xids = NULL;

            xlrec = (xl_xact_abort *)buf->record_data;
            TransactionId curId = XLogRecGetXid(r);
            sub_xids = GET_SUB_XACTS(xlrec->xnodes, xlrec->nrels, compress);
            curId = *(TransactionId *)((char *)sub_xids + (unsigned)(xlrec->nsubxacts) * sizeof(TransactionId));

            DecodeAbort(ctx, buf->origptr, curId, sub_xids, xlrec->nsubxacts);
            break;
        }
        case XLOG_XACT_ABORT_PREPARED: {
            xl_xact_abort_prepared *prec = NULL;
            xl_xact_abort *xlrec = NULL;
            TransactionId *sub_xids = NULL;

            /* prepared abort contain a normal commit abort... */
            prec = (xl_xact_abort_prepared *)buf->record_data;
            xlrec = &prec->arec;
            sub_xids = GET_SUB_XACTS(xlrec->xnodes, xlrec->nrels, compress);

            /* r->xl_xid is committed in a separate record */
            DecodeAbort(ctx, buf->origptr, prec->xid, sub_xids, xlrec->nsubxacts);
            break;
        }
        case XLOG_XACT_ASSIGNMENT: {
            xl_xact_assignment *xlrec = NULL;
            int i;
            TransactionId *sub_xid = NULL;

            xlrec = (xl_xact_assignment *)buf->record_data;

            sub_xid = &xlrec->xsub[0];

            for (i = 0; i < xlrec->nsubxacts; i++) {
                ReorderBufferAssignChild(reorder, xlrec->xtop, *(sub_xid++), buf->origptr);
            }
            break;
        }
        case XLOG_XACT_PREPARE:
            /*
             * Currently decoding ignores PREPARE TRANSACTION and will just
             * decode the transaction when the COMMIT PREPARED is sent or
             * throw away the transaction's contents when a ROLLBACK PREPARED
             * is received. In the future we could add code to expose prepared
             * transactions in the changestream allowing for a kind of
             * distributed 2PC.
             */
            ReorderBufferProcessXid(reorder, XLogRecGetXid(r), buf->origptr);
            break;
        default:
            ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected RM_XACT_ID record type: %u", info)));
    }
}

static void SetTxnVal(ReorderBufferTXN *txn, TransactionId xid, XLogRecPtr final_lsn,
            XLogRecPtr end_lsn, RepOriginId origin_id, CommitSeqNo csn, TimestampTz commit_time)
{
    txn->xid = xid;
    txn->final_lsn = final_lsn;
    txn->end_lsn = end_lsn;
    txn->origin_id = origin_id;
    if (csn != InvalidCommitSeqNo) {
        txn->csn = csn;
    }
    if (commit_time != -1) {
        txn->commit_time = commit_time;
    }
}

/*
 * Handle rmgr XACT_ID records for DecodeRecordIntoReorderBuffer().
 */
static void AreaDecodeXactOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    uint8 info = XLogRecGetInfo(r) & ~XLR_INFO_MASK;
    ReorderBuffer *rb = ctx->reorder;
    ReorderBufferTXN *txn = (struct ReorderBufferTXN *)palloc(sizeof(struct ReorderBufferTXN));
    bool compress = (bool)(XLogRecGetInfo(r) & XLR_REL_COMPRESS);
    if (txn == NULL)
        return;

    switch (info) {
        case XLOG_XACT_COMMIT: {
            xl_xact_commit *xlrec = NULL;
            TransactionId *subxacts = NULL;

            xlrec = (xl_xact_commit *)buf->record_data;

            subxacts = GET_SUB_XACTS(xlrec->xnodes, xlrec->nrels, compress);
            for (int nxact = 0; nxact < xlrec->nsubxacts; nxact++) {
                TransactionId subxid = subxacts[nxact];
                SetTxnVal(txn, subxid, buf->origptr, buf->endptr, XLogRecGetOrigin(buf->record), xlrec->csn, xlrec->xact_time);
                rb->commit(rb, txn, buf->origptr);
            }

            TransactionId xid = XLogRecGetXid(r);
            SetTxnVal(txn, xid, buf->origptr, buf->endptr, XLogRecGetOrigin(buf->record), xlrec->csn, xlrec->xact_time);
            rb->commit(rb, txn, buf->origptr);

            break;
        }
        case XLOG_XACT_COMMIT_PREPARED: {
            xl_xact_commit_prepared *prec = NULL;
            xl_xact_commit *xlrec = NULL;
            TransactionId *subxacts = NULL;

            /* Prepared commits contain a normal commit record... */
            prec = (xl_xact_commit_prepared *)buf->record_data;
            xlrec = &prec->crec;

            subxacts = GET_SUB_XACTS(xlrec->xnodes, xlrec->nrels, compress);

            TransactionId xid = prec->xid;
            SetTxnVal(txn, xid, buf->origptr, buf->endptr, XLogRecGetOrigin(buf->record), xlrec->csn, xlrec->xact_time);
            rb->commit(rb, txn, buf->origptr);

            break;
        }
        case XLOG_XACT_COMMIT_COMPACT: {
            xl_xact_commit_compact *xlrec = NULL;
            xlrec = (xl_xact_commit_compact *)buf->record_data;
            TransactionId xid = XLogRecGetXid(r);
            SetTxnVal(txn, xid, buf->origptr, buf->endptr, XLogRecGetOrigin(buf->record), xlrec->csn, xlrec->xact_time);
            rb->commit(rb, txn, buf->origptr);
            break;
        }
        case XLOG_XACT_ABORT: {
            TransactionId xid = XLogRecGetXid(r);
            xl_xact_abort *xlrec = NULL;
            xlrec = (xl_xact_abort *)buf->record_data;
            SetTxnVal(txn, xid, buf->origptr, buf->endptr, XLogRecGetOrigin(buf->record), InvalidCommitSeqNo, -1);
            rb->abort(rb, txn);
            break;
        }
        case XLOG_XACT_ABORT_WITH_XID: {
            xl_xact_abort *xlrec = NULL;
            TransactionId *sub_xids = NULL;
            xlrec = (xl_xact_abort *)buf->record_data;
            sub_xids = GET_SUB_XACTS(xlrec->xnodes, xlrec->nrels, compress);
            TransactionId xid = XLogRecGetXid(r);
            xid = *(TransactionId *)((char *)sub_xids +
                    (unsigned)(xlrec->nsubxacts) * sizeof(TransactionId));
            SetTxnVal(txn, xid, buf->origptr, buf->endptr, XLogRecGetOrigin(buf->record), InvalidCommitSeqNo, -1);
            rb->abort(rb, txn);

            break;
        }
        case XLOG_XACT_ABORT_PREPARED: {
            xl_xact_abort_prepared *prec = NULL;
            prec = (xl_xact_abort_prepared *)buf->record_data;
            TransactionId xid = prec->xid;
            SetTxnVal(txn, xid, buf->origptr, buf->endptr, XLogRecGetOrigin(buf->record), InvalidCommitSeqNo, -1);
            rb->abort(rb, txn);
            break;
        }
        case XLOG_XACT_ASSIGNMENT:
            break;
        case XLOG_XACT_PREPARE:
            SetTxnVal(txn, XLogRecGetXid(r), buf->origptr, buf->endptr, XLogRecGetOrigin(buf->record), InvalidCommitSeqNo, -1);
            rb->prepare(rb,txn);
            break;
        default:
            ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected RM_XACT_ID record type: %u", info)));
    }
}


/*
 * Handle rmgr STANDBY_ID records for DecodeRecordIntoReorderBuffer().
 */
static void DecodeStandbyOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    SnapBuild *builder = ctx->snapshot_builder;
    XLogReaderState *r = buf->record;
    uint8 info = XLogRecGetInfo(r) & ~XLR_INFO_MASK;
    XLogRecPtr lsn = buf->endptr;

    ReorderBufferProcessXid(ctx->reorder, XLogRecGetXid(r), buf->origptr);

    switch (info) {
        case XLOG_RUNNING_XACTS: {
            xl_running_xacts *running = (xl_running_xacts *)buf->record_data;
            SnapBuildProcessRunningXacts(builder, buf->origptr, running);
            /*
             * Abort all transactions that we keep track of, that are
             * older than the record's oldestRunningXid. This is the most
             * convenient spot for doing so since, in contrast to shutdown
             * or end-of-recovery checkpoints, we have information about
             * all running transactions which includes prepared ones,
             * while shutdown checkpoints just know that no non-prepared
             * transactions are in progress.
             */
            ReorderBufferAbortOld(ctx->reorder, running->oldestRunningXid, lsn);
        } break;
        case XLOG_STANDBY_LOCK:
        case XLOG_STANDBY_CSN:
        case XLOG_STANDBY_CSN_COMMITTING:
        case XLOG_STANDBY_CSN_ABORTED:

            break;
        default:
            ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected RM_STANDBY_ID record type: %u", info)));
    }
}

/*
 * Decode multi insert for area decoding.
 */
static void AreaDecodeHeap2Op(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    uint8 info = XLogRecGetInfo(buf->record) & XLOG_HEAP_OPMASK;
    TransactionId xid = XLogRecGetXid(buf->record);
    ctx->write_xid = xid;

    switch (info) {
        case XLOG_HEAP2_MULTI_INSERT:
                AreaDecodeMultiInsert(ctx, buf);
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
            ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected RM_HEAP2_ID record type: %u", info)));
    }
}



/*
 * Handle rmgr HEAP2_ID records for DecodeRecordIntoReorderBuffer().
 */
static void DecodeHeap2Op(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    uint8 info = XLogRecGetInfo(buf->record) & XLOG_HEAP_OPMASK;
    TransactionId xid = XLogRecGetXid(buf->record);
    SnapBuild *builder = ctx->snapshot_builder;

    ReorderBufferProcessXid(ctx->reorder, xid, buf->origptr);

    /*
     * If we don't have snapshot or we are just fast-forwarding, there is no
     * point in decoding changes.
     */
    if (SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT || ctx->fast_forward)
        return;

    switch (info) {
        case XLOG_HEAP2_MULTI_INSERT:
            if (!ctx->fast_forward && SnapBuildProcessChange(builder, xid, buf->origptr))
                DecodeMultiInsert(ctx, buf);
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

            break;
        default:
            ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected RM_HEAP2_ID record type: %u", info)));
    }
}

/*
 * Handle rmgr HEAP2_ID records for DecodeRecordIntoReorderBuffer().
 */
static void DecodeHeap3Op(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    uint8 info = XLogRecGetInfo(buf->record) & XLOG_HEAP_OPMASK;
    TransactionId xid = XLogRecGetXid(buf->record);
    SnapBuild *builder = ctx->snapshot_builder;

    ReorderBufferProcessXid(ctx->reorder, xid, buf->origptr);

    /*
     * If we don't have snapshot or we are just fast-forwarding, there is no
     * point in decoding changes.
     */
    if (SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT || ctx->fast_forward)
        return;

    switch (info) {
        case XLOG_HEAP3_NEW_CID: {
            xl_heap_new_cid *xlrec = NULL;
            int bucket_id = 0;
            xlrec = (xl_heap_new_cid *)buf->record_data;
            bucket_id = XLogRecGetBucketId(buf->record);
            SnapBuildProcessNewCid(ctx, xid, buf->origptr, xlrec, bucket_id);
            break;
        }
        case XLOG_HEAP3_INVALID:
            break;
        case XLOG_HEAP3_REWRITE:
            break;
        default:
            ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected RM_HEAP3_ID record type: %u", info)));
    }
}

/*
 * Handle rmgr HEAP_ID records for DecodeRecordIntoReorderBuffer().
 */
static void DecodeHeapOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    uint8 info = XLogRecGetInfo(buf->record) & XLOG_HEAP_OPMASK;
    TransactionId xid = XLogRecGetXid(buf->record);
    SnapBuild *builder = ctx->snapshot_builder;

    ReorderBufferProcessXid(ctx->reorder, xid, buf->origptr);

    /*
     * If we don't have snapshot or we are just fast-forwarding, there is no
     * point in decoding data changes.
     */
    if (SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT || ctx->fast_forward)
        return;

    switch (info) {
        case XLOG_HEAP_INSERT:
            if (SnapBuildProcessChange(builder, xid, buf->origptr))
                DecodeInsert(ctx, buf);
            break;

        /*
         * Treat HOT update as normal updates. There is no useful
         * information in the fact that we could make it a HOT update
         * locally and the WAL layout is compatible.
         */
        case XLOG_HEAP_HOT_UPDATE:
        case XLOG_HEAP_UPDATE:
            if (SnapBuildProcessChange(builder, xid, buf->origptr))
                DecodeUpdate(ctx, buf);
            break;

        case XLOG_HEAP_DELETE:
            if (SnapBuildProcessChange(builder, xid, buf->origptr))
                DecodeDelete(ctx, buf);
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
            if (!TransactionIdIsValid(xid))
                break;

            SnapBuildProcessChange(builder, xid, buf->origptr);
            ReorderBufferXidSetCatalogChanges(ctx->reorder, xid, buf->origptr);
            break;

        case XLOG_HEAP_LOCK:
            /* we don't care about row level locks for now */
            break;

        case XLOG_HEAP_BASE_SHIFT:
            break;

        default:
            ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected RM_HEAP_ID record type: %u", info)));
            break;
    }
}

/*
 * Handle rmgr HEAP_ID records for area decoding.
 */
static void AreaDecodeHeapOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    uint8 info = XLogRecGetInfo(buf->record) & XLOG_HEAP_OPMASK;
    TransactionId xid = XLogRecGetXid(buf->record);
    ctx->write_xid = xid;

    switch (info) {
        case XLOG_HEAP_INSERT:
            AreaDecodeInsert(ctx, buf);
            break;

        /*
         * Treat HOT update as normal updates. There is no useful
         * information in the fact that we could make it a HOT update
         * locally and the WAL layout is compatible.
         */
        case XLOG_HEAP_HOT_UPDATE:
        case XLOG_HEAP_UPDATE:
            AreaDecodeUpdate(ctx, buf);
            break;

        case XLOG_HEAP_DELETE:
            AreaDecodeDelete(ctx, buf);
            break;

        case XLOG_HEAP_NEWPAGE:
            /*
             * This is only used in places like indexams and CLUSTER which
             * don't contain changes relevant for logical replication.
             */
            break;

        case XLOG_HEAP_INPLACE:
            break;

        case XLOG_HEAP_LOCK:
            /* we don't care about row level locks for now */
            break;

        case XLOG_HEAP_BASE_SHIFT:
            break;

        default:
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected RM_HEAP_ID record type: %u", info)));
            break;
    }
}

static void DecodeUheapOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    uint8 info = XLogRecGetInfo(buf->record) & XLOG_UHEAP_OPMASK;
    TransactionId xid = XLogRecGetXid(buf->record);
    SnapBuild *builder = ctx->snapshot_builder;

    ReorderBufferProcessXid(ctx->reorder, xid, buf->origptr);

    /*
     * If we don't have snapshot or we are just fast-forwarding, there is no
     * point in decoding data changes.
     */
    if (SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT || ctx->fast_forward)
        return;

    switch (info) {
        case XLOG_UHEAP_INSERT:
            if (SnapBuildProcessChange(builder, xid, buf->origptr)) {
                DecodeUInsert(ctx, buf);
            }
            break;

        case XLOG_UHEAP_UPDATE:
            if (SnapBuildProcessChange(builder, xid, buf->origptr)) {
                DecodeUUpdate(ctx, buf);
            }
            break;

        case XLOG_UHEAP_DELETE:
            if (SnapBuildProcessChange(builder, xid, buf->origptr)) {
                DecodeUDelete(ctx, buf);
            }
            break;

        case XLOG_UHEAP_FREEZE_TD_SLOT:
        case XLOG_UHEAP_INVALID_TD_SLOT:
        case XLOG_UHEAP_CLEAN:
            break;

        case XLOG_UHEAP_MULTI_INSERT:
            if (SnapBuildProcessChange(builder, xid, buf->origptr)) {
                DecodeUMultiInsert(ctx, buf);
            }
            break;
        default:
            ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected RM_UHEAP_ID record type: %u", info)));
            break;
    }
}

static void AreaDecodeUheapOp(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    uint8 info = XLogRecGetInfo(buf->record) & XLOG_UHEAP_OPMASK;
    TransactionId xid = XLogRecGetXid(buf->record);
    ctx->write_xid = xid;
    switch (info) {
        case XLOG_UHEAP_INSERT:
            AreaDecodeUInsert(ctx, buf);
            break;
        case XLOG_UHEAP_UPDATE:
            AreaDecodeUUpdate(ctx, buf);
            break;

        case XLOG_UHEAP_DELETE:
            AreaDecodeUDelete(ctx, buf);
            break;

        case XLOG_UHEAP_FREEZE_TD_SLOT:
        case XLOG_UHEAP_INVALID_TD_SLOT:
        case XLOG_UHEAP_CLEAN:
            break;

        case XLOG_UHEAP_MULTI_INSERT:
            AreaDecodeUMultiInsert(ctx, buf);
            break;
        default:
            ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unexpected RM_UHEAP_ID record type: %u", info)));
            break;
    }
}

bool FilterByOrigin(LogicalDecodingContext *ctx, RepOriginId origin_id)
{
    origin_id = (int)((uint32)(origin_id) & TOAST_MASK);

    if (ctx->callbacks.filter_by_origin_cb == NULL)
        return false;

    return filter_by_origin_cb_wrapper(ctx, origin_id);
}

static void AreaDecodingChange(ReorderBufferChange *change, LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    change->lsn = buf->origptr;

    Relation relation = NULL;
    Oid reloid;
    Oid partitionReltoastrelid = InvalidOid;
    bool isSegment = IsSegmentFileNode(change->data.tp.relnode);

    reloid = HeapGetRelid(change->data.tp.relnode.spcNode, change->data.tp.relnode.relNode, partitionReltoastrelid,
        NULL, isSegment);
    /*
     * Catalog tuple without data, emitted while catalog was
     * in the process of being rewritten.
     */
    if (reloid == InvalidOid && change->data.tp.newtuple == NULL && change->data.tp.oldtuple == NULL)
       return;
    else if (reloid == InvalidOid) {
       /*
        * When we try to decode a table who is already dropped.
        * Maybe we could not find it relnode.In this time, we will undecode this log.
        * It will be solve when we use MVCC.
        */
        ereport(DEBUG1, (errmodule(MOD_LOGICAL_DECODE), errmsg("could not lookup relation %s",
            relpathperm(change->data.tp.relnode, MAIN_FORKNUM))));
        return;
    }

    relation = RelationIdGetRelation(reloid);
    if (relation == NULL) {
       ereport(DEBUG1, (errmodule(MOD_LOGICAL_DECODE), errmsg("could open relation descriptor %s",
           relpathperm(change->data.tp.relnode, MAIN_FORKNUM))));
       return;
    }

    if (CSTORE_NAMESPACE == get_rel_namespace(RelationGetRelid(relation))) {
        return;
    }

    if (RelationIsLogicallyLogged(relation)) {
        /*
         * For now ignore sequence changes entirely. Most of
         * the time they don't log changes using records we
         * understand, so it doesn't make sense to handle the
         * few cases we do.
         */
        if (relation->rd_rel->relkind != RELKIND_SEQUENCE &&
                   !IsToastRelation(relation)) {
            ctx->reorder->apply_change(ctx->reorder, NULL, relation, change);
        }
    }
    RelationClose(relation);
}

/*
 * Handle rmgr LOGICALDDLMSG_ID records for DecodeRecordIntoReorderBuffer()
 */
void
logicalddl_decode(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    SnapBuild *builder = ctx->snapshot_builder;
    XLogReaderState *r = buf->record;
    TransactionId xid = XLogRecGetXid(r);
    uint8 info = XLogRecGetInfo(r) & ~XLR_INFO_MASK;
    RepOriginId origin_id = XLogRecGetOrigin(r);
    xl_logical_ddl_message *message;

    if (info != XLOG_LOGICAL_DDL_MESSAGE)
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errmsg("unexpected RM_LOGICALDDLMSG_ID record type: %u", info)));

    ReorderBufferProcessXid(ctx->reorder, XLogRecGetXid(r), buf->origptr);

    /*
     * If we don't have snapshot or we are just fast-forwarding, there is no
     * point in decoding ddl messages.
     */
    if (SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT ||
        ctx->fast_forward)
        return;

    message = (xl_logical_ddl_message *)XLogRecGetData(r);

    if (message->dbId != ctx->slot->data.database ||
        FilterByOrigin(ctx, origin_id))
        return;

    if (SnapBuildProcessChange(builder, xid, buf->origptr))
        ReorderBufferQueueDDLMessage(ctx, xid, buf->endptr,
                                    message->message,
                                    message->message_size,
                                    message->message + message->prefix_size,
                                    message->relid, message->cmdtype);

}

/*
 * Consolidated commit record handling between the different form of commit
 * records.
 */
static void DecodeCommit(LogicalDecodingContext *ctx, XLogRecordBuffer *buf, TransactionId xid, CommitSeqNo csn,
                         Oid dboid, TimestampTz commit_time, int nsubxacts, TransactionId *sub_xids, int ninval_msgs,
                         SharedInvalidationMessage *msgs, xl_xact_origin *origin)
{
    int i;
    XLogRecPtr origin_id = XLogRecGetOrigin(buf->record);
    XLogRecPtr origin_lsn = origin == NULL ? InvalidXLogRecPtr : origin->origin_lsn;

    /*
     * Process invalidation messages, even if we're not interested in the
     * transaction's contents, since the various caches need to always be
     * consistent.
     */
    if (ninval_msgs > 0) {
        if (!ctx->fast_forward)
            ReorderBufferAddInvalidations(ctx->reorder, xid, buf->origptr, ninval_msgs, msgs);
        ReorderBufferXidSetCatalogChanges(ctx->reorder, xid, buf->origptr);
    }

    SnapBuildCommitTxn(ctx, buf->origptr, xid, nsubxacts, sub_xids);

    /* ----
     * Check whether we are interested in this specific transaction, and tell
     * the reorderbuffer to forget the content of the (sub-)transactions
     * if not.
     *
     * There basically two reasons we might not be interested in this
     * transaction:
     * 1) We might not be interested in decoding transactions up to this
     *	LSN. This can happen because we previously decoded it and now just
     *	are restarting or if we haven't assembled a consistent snapshot yet.
     * 2) The transaction happened in another database.
     *
     * 4) We are doing fast-forwarding

     * We can't just use ReorderBufferAbort() here, because we need to execute
     * the transaction's invalidations.  This currently won't be needed if
     * we're just skipping over the transaction because currently we only do
     * so during startup, to get to the first transaction the client needs. As
     * we have reset the catalog caches before starting to read WAL, and we
     * haven't yet touched any catalogs, there can't be anything to invalidate.
     * But if we're "forgetting" this commit because it's it happened in
     * another database, the invalidations might be important, because they
     * could be for shared catalogs and we might have loaded data into the
     * relevant syscaches.
     * ---
     */
    if (SnapBuildXactNeedsSkip(ctx->snapshot_builder, buf->origptr) ||
        (dboid != InvalidOid && dboid != ctx->slot->data.database) || ctx->fast_forward ||
        FilterByOrigin(ctx, origin_id)) {
        for (i = 0; i < nsubxacts; i++) {
            ReorderBufferForget(ctx->reorder, *sub_xids, buf->origptr);
            sub_xids++;
        }
        ReorderBufferForget(ctx->reorder, xid, buf->origptr);

        return;
    }

    /* tell the reorderbuffer about the surviving subtransactions */
    for (i = 0; i < nsubxacts; i++) {
        ReorderBufferCommitChild(ctx->reorder, xid, *sub_xids, buf->origptr, buf->endptr);
        sub_xids++;
    }

    /* replay actions of all transaction + subtransactions in order */
    ReorderBufferCommit(ctx->reorder, xid, buf->origptr, buf->endptr, origin_id, origin_lsn, csn, commit_time);
}

/*
 * Get the data from the various forms of abort records and pass it on to
 * snapbuild.c and reorderbuffer.c
 */
static void DecodeAbort(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid, TransactionId *sub_xids,
                        int nsubxacts)
{
    int i;

    for (i = 0; i < nsubxacts; i++) {
        ReorderBufferAbort(ctx->reorder, *sub_xids, lsn);
        sub_xids++;
    }

    ReorderBufferAbort(ctx->reorder, xid, lsn);
}

/*
 * Parse XLOG_HEAP_INSERT (not MULTI_INSERT!) records into tuplebufs.
 *
 * Deletes can contain the new tuple.
 */
static void DecodeInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    RelFileNode target_node = {0, 0, 0, 0, 0};
    xl_heap_insert *xlrec = (xl_heap_insert *)GetXlrec(r);
    CommitSeqNo curCSN = InvalidCommitSeqNo;
    bool hasCSN = (r->decoded_record->xl_term & XLOG_CONTAIN_CSN) == 0 ? false : true;
    if (hasCSN) {
        curCSN = *(CommitSeqNo *)((char *)xlrec + SizeOfHeapInsert);
    }

    /* only interested in our database */
    Size tuplelen;
    char *tupledata = XLogRecGetBlockData(r, 0, &tuplelen);
    XLogRecGetBlockTag(r, 0, &target_node, NULL, NULL);
    if (target_node.dbNode != ctx->slot->data.database)
        return;
    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
        return;

    /* if the insert belongs to partition-split, no need to decode */
    if (xlrec->flags & XLH_INSERT_SPLIT_PARTITION)
        return;

    ReorderBufferChange *change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_INSERT;
    change->origin_id = XLogRecGetOrigin(r);
    errno_t rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &target_node, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");

    if (xlrec->flags & XLH_INSERT_CONTAINS_NEW_TUPLE) {
        change->data.tp.newtuple = ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);

        DecodeXLogTuple(tupledata, tuplelen, change->data.tp.newtuple, true);
    }

    change->data.tp.snapshotcsn = curCSN;
    change->data.tp.clear_toast_afterwards = true;

    ReorderBufferQueueChange(ctx, XLogRecGetXid(r), buf->origptr, change);
}

/*
 * Decode insert sql for area decoding.
 *
 */
static void AreaDecodeInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    RelFileNode target_node = {0, 0, 0, 0, 0};
    xl_heap_insert *xlrec = (xl_heap_insert *)GetXlrec(r);
    CommitSeqNo curCSN = InvalidCommitSeqNo;
    bool hasCSN = (r->decoded_record->xl_term & XLOG_CONTAIN_CSN) == 0 ? false : true;
    if (hasCSN) {
        curCSN = *(CommitSeqNo *)((char *)xlrec + SizeOfHeapInsert);
    }
    Size tuplelen;
    char *tupledata = XLogRecGetBlockData(r, 0, &tuplelen);
    XLogRecGetBlockTag(r, 0, &target_node, NULL, NULL);
    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }
    ReorderBufferChange *change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_INSERT;
    change->origin_id = XLogRecGetOrigin(r);

    errno_t rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &target_node, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");

    if (xlrec->flags & XLH_INSERT_CONTAINS_NEW_TUPLE) {
        change->data.tp.newtuple = ReorderBufferGetTupleBuf(ctx->reorder, tuplelen);

        DecodeXLogTuple(tupledata, tuplelen, change->data.tp.newtuple, true);
    }

    change->data.tp.snapshotcsn = curCSN;
    change->data.tp.clear_toast_afterwards = true;
    AreaDecodingChange(change, ctx, buf);
}


/*
 * Parse XLOG_HEAP_UINSERT (not MULTI_UINSERT!) records into tuplebufs.
 */
static void DecodeUInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    RelFileNode targetNode = {0, 0, 0, 0, 0};
    XlUHeapInsert *xlrec = (XlUHeapInsert *)UGetXlrec(r);
    CommitSeqNo curCSN = InvalidCommitSeqNo;
    bool hasCSN = (r->decoded_record->xl_term & XLOG_CONTAIN_CSN) == 0 ? false : true;
    if (hasCSN) {
        curCSN = *(CommitSeqNo *)((char *)xlrec + SizeOfUHeapInsert);
    }
    Size tuplelen = 0;
    char *tupledata = XLogRecGetBlockData(r, 0, &tuplelen);
    XLogRecGetBlockTag(r, 0, &targetNode, NULL, NULL);
    if (targetNode.dbNode != ctx->slot->data.database) {
        return;
    }
    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r)))
        return;

    ReorderBufferChange *change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_UINSERT;
    change->origin_id = XLogRecGetOrigin(r);
    errno_t rc = memcpy_s(&change->data.utp.relnode, sizeof(RelFileNode), &targetNode, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");

    if (xlrec->flags & XLOG_UHEAP_CONTAINS_NEW_TUPLE) {
        change->data.utp.newtuple = ReorderBufferGetUTupleBuf(ctx->reorder, tuplelen);
        DecodeXLogTuple(tupledata, tuplelen, (ReorderBufferTupleBuf *)change->data.utp.newtuple, false);
    }
    change->data.utp.snapshotcsn = curCSN;

    change->data.tp.clear_toast_afterwards = true;
    ReorderBufferQueueChange(ctx, UHeapXlogGetCurrentXid(r, hasCSN), buf->origptr, change);
}

/*
 * Parse XLOG_HEAP_UINSERT (not MULTI_UINSERT!) records into tuplebufs for area decoding.
 */
static void AreaDecodeUInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    RelFileNode targetNode = {0, 0, 0, 0, 0};
    XLogReaderState *r = buf->record;
    XlUHeapInsert *xlrec = (XlUHeapInsert *)UGetXlrec(r);
    CommitSeqNo curCSN = InvalidCommitSeqNo;
    bool hasCSN = (r->decoded_record->xl_term & XLOG_CONTAIN_CSN) == 0 ? false : true;
    if (hasCSN) {
        curCSN = *(CommitSeqNo *)((char *)xlrec + SizeOfUHeapInsert);
    }
    Size tuplelen = 0;
    char *tupledata = XLogRecGetBlockData(r, 0, &tuplelen);

    XLogRecGetBlockTag(r, 0, &targetNode, NULL, NULL);

    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }
    ReorderBufferChange *change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_UINSERT;
    change->origin_id = XLogRecGetOrigin(r);
    int rc = memcpy_s(&change->data.utp.relnode, sizeof(RelFileNode), &targetNode, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");
    if (xlrec->flags & XLOG_UHEAP_CONTAINS_NEW_TUPLE) {
        change->data.utp.newtuple = ReorderBufferGetUTupleBuf(ctx->reorder, tuplelen);
        DecodeXLogTuple(tupledata, tuplelen, (ReorderBufferTupleBuf *)change->data.utp.newtuple, false);
    }
    change->data.utp.snapshotcsn = curCSN;
    change->data.utp.clear_toast_afterwards = true;
    AreaDecodingChange(change, ctx, buf);
}


/*
 * Parse XLOG_HEAP_UPDATE and XLOG_HEAP_HOT_UPDATE, which have the same layout
 * in the record, from wal into proper tuplebufs.
 *
 * Updates can possibly contain a new tuple and the old primary key.
 */
static void DecodeUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    RelFileNode target_node = {0, 0, 0, 0, 0};
    xl_heap_update *xlrec = (xl_heap_update *)GetXlrec(r);
    CommitSeqNo curCSN = InvalidCommitSeqNo;
    Size heapUpdateSize = 0;
    if ((XLogRecGetInfo(r) & XLOG_TUPLE_LOCK_UPGRADE_FLAG) == 0) {
        heapUpdateSize = SizeOfOldHeapUpdate;
    } else {
        heapUpdateSize = SizeOfHeapUpdate;
    }
    bool hasCSN = (r->decoded_record->xl_term & XLOG_CONTAIN_CSN) == 0 ? false : true;
    if (hasCSN) {
        curCSN = *(CommitSeqNo *)((char *)xlrec + heapUpdateSize);
    }

    XLogRecGetBlockTag(r, 0, &target_node, NULL, NULL);
    /* only interested in our database */
    if (target_node.dbNode != ctx->slot->data.database) {
        return;
    }
    Size datalen_new = 0;
    char *data_new = XLogRecGetBlockData(r, 0, &datalen_new);
    Size tuplelen_new = datalen_new - SizeOfHeapHeader;
    Size datalen_old = 0;

    /* adapt 64 xid, if this tuple is the first tuple of a new page */
    bool is_init = (XLogRecGetInfo(r) & XLOG_HEAP_INIT_PAGE) != 0;
    /* caution, remaining data in record is not aligned */
    char *data_old = (char *)xlrec + heapUpdateSize + (hasCSN ? sizeof(CommitSeqNo) : 0);
    if (is_init) {
        datalen_old = XLogRecGetDataLen(r) - heapUpdateSize - sizeof(TransactionId);
    } else {
        datalen_old = XLogRecGetDataLen(r) - heapUpdateSize;
    }
    datalen_old -= hasCSN ? sizeof(CommitSeqNo) : 0;
    Size tuplelen_old = datalen_old - SizeOfHeapHeader;

    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }

    ReorderBufferChange *change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_UPDATE;
    change->origin_id = XLogRecGetOrigin(r);
    errno_t rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &target_node, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");
    if (xlrec->flags & XLH_UPDATE_CONTAINS_NEW_TUPLE) {
        change->data.tp.newtuple = ReorderBufferGetTupleBuf(ctx->reorder, tuplelen_new);
        DecodeXLogTuple(data_new, datalen_new, change->data.tp.newtuple, true);
    }
    if (xlrec->flags & XLH_UPDATE_CONTAINS_OLD) {
        change->data.tp.oldtuple = ReorderBufferGetTupleBuf(ctx->reorder, tuplelen_old);

        DecodeXLogTuple(data_old, datalen_old, change->data.tp.oldtuple, true);
    }

    change->data.tp.snapshotcsn = curCSN;
    change->data.tp.clear_toast_afterwards = true;

    ReorderBufferQueueChange(ctx, XLogRecGetXid(r), buf->origptr, change);
}

/*
 * Decode update sql for area decoding.
 *
 */
static void AreaDecodeUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    RelFileNode target_node = {0, 0, 0, 0, 0};
    xl_heap_update *xlrec = (xl_heap_update *)GetXlrec(r);
    CommitSeqNo curCSN = InvalidCommitSeqNo;
    Size heapUpdateSize = 0;
    if ((XLogRecGetInfo(r) & XLOG_TUPLE_LOCK_UPGRADE_FLAG) == 0) {
        heapUpdateSize = SizeOfOldHeapUpdate;
    } else {
        heapUpdateSize = SizeOfHeapUpdate;
    }
    bool hasCSN = (r->decoded_record->xl_term & XLOG_CONTAIN_CSN) == 0 ? false : true;
    if (hasCSN) {
        curCSN = *(CommitSeqNo *)((char *)xlrec + heapUpdateSize);
    }
    XLogRecGetBlockTag(r, 0, &target_node, NULL, NULL);
    Size datalen_new = 0;
    char *data_new = XLogRecGetBlockData(r, 0, &datalen_new);
    Size tuplelen_new = datalen_new - SizeOfHeapHeader;
    Size datalen_old = 0;

    /* adapt 64 xid, if this tuple is the first tuple of a new page */
    bool is_init = (XLogRecGetInfo(r) & XLOG_HEAP_INIT_PAGE) != 0;
    /* caution, remaining data in record is not aligned */
    char *data_old = (char *)xlrec + heapUpdateSize + (hasCSN ? sizeof(CommitSeqNo) : 0);
    if (is_init) {
        datalen_old = XLogRecGetDataLen(r) - heapUpdateSize - sizeof(TransactionId);
    } else {
        datalen_old = XLogRecGetDataLen(r) - heapUpdateSize;
    }
    datalen_old -= hasCSN ? sizeof(CommitSeqNo) : 0;
    Size tuplelen_old = datalen_old - SizeOfHeapHeader;

    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }

    ReorderBufferChange *change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_UPDATE;
    change->origin_id = XLogRecGetOrigin(r);
    errno_t rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &target_node, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");
    if (xlrec->flags & XLH_UPDATE_CONTAINS_OLD) {
        change->data.tp.oldtuple = ReorderBufferGetTupleBuf(ctx->reorder, tuplelen_old);
        DecodeXLogTuple(data_old, datalen_old, change->data.tp.oldtuple, true);
    }
    if (xlrec->flags & XLH_UPDATE_CONTAINS_NEW_TUPLE) {
        change->data.tp.newtuple = ReorderBufferGetTupleBuf(ctx->reorder, tuplelen_new);
        DecodeXLogTuple(data_new, datalen_new, change->data.tp.newtuple, true);
    }
    change->data.tp.snapshotcsn = curCSN;
    change->data.tp.clear_toast_afterwards = true;
    AreaDecodingChange(change, ctx, buf);
}



/*
 * Filter out records that we don't need to decode.
 */
bool FilterRecord(LogicalDecodingContext *ctx, XLogReaderState *r, uint8 flags, RelFileNode* rnode)
{
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return true;
    }
    if (((flags & XLZ_UPDATE_PREFIX_FROM_OLD) != 0) || ((flags & XLZ_UPDATE_SUFFIX_FROM_OLD) != 0)) {
        ereport(LOG, (errmodule(MOD_LOGICAL_DECODE), errmsg("update tuple has affix, don't decode it")));
        return true;
    }
    XLogRecGetBlockTag(r, 0, rnode, NULL, NULL);
    if (rnode->dbNode != ctx->slot->data.database) {
        return true;
    }
    return false;
}

void UpdateUndoBody(Size* addLenPtr, char* data, uint8 flag, uint32* toastLen)
{
    if ((flag & XLOG_UNDO_HEADER_HAS_SUB_XACT) != 0) {
        *addLenPtr += sizeof(bool);
    }
    if ((flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        *addLenPtr += sizeof(UndoRecPtr);
    }
    if ((flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        *addLenPtr += sizeof(UndoRecPtr);
    }
    if ((flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        *addLenPtr += sizeof(Oid);
    }
    if ((flag & XLOG_UNDO_HEADER_HAS_CURRENT_XID) != 0) {
        *addLenPtr += sizeof(TransactionId);
    }
    if ((flag & XLOG_UNDO_HEADER_HAS_TOAST) != 0) {
        *toastLen = *(uint32 *)(data + *addLenPtr);
        ereport(DEBUG2, (errmodule(MOD_LOGICAL_DECODE), errmsg("UpdateUndoBody toastLen = %u", *toastLen)));
        *addLenPtr += sizeof(uint32);
    }
}
/*
 * Calc the length of old tuples in XLOG_UHEAP_UPDATEs.
 */
char *UpdateOldTupleCalc(bool isInplaceUpdate, XLogReaderState *r, char **tupleOld, Size *tuplelenOld, uint32* toastLen)
{
    XlUndoHeader *xlundohdr = (XlUndoHeader *)(*tupleOld);
    Size addLen = SizeOfXLUndoHeader;
    UpdateUndoBody(&addLen, *tupleOld, xlundohdr->flag, toastLen);
    *tupleOld += addLen;
    *tuplelenOld -= addLen + *toastLen;
    char *toastData = *tupleOld;
    *tupleOld += *toastLen;
    addLen = 0;
    if (!isInplaceUpdate) {
        XlUndoHeader *xlnewundohdr = (XlUndoHeader *)(*tupleOld);
        addLen += SizeOfXLUndoHeader;
        if ((xlnewundohdr->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
            addLen += sizeof(UndoRecPtr);
        }
        if ((xlnewundohdr->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
            addLen += sizeof(UndoRecPtr);
        }
        if ((xlnewundohdr->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
            addLen += sizeof(Oid);
        }
    }

    Size metaLen = DecodeUndoMeta(*tupleOld + addLen);
    addLen += metaLen;
    *tupleOld += addLen;
    *tuplelenOld -= addLen;
    if ((XLogRecGetInfo(r) & XLOG_UHEAP_INIT_PAGE) != 0) {
        *tupleOld += sizeof(TransactionId) + sizeof(uint16);
        *tuplelenOld -= sizeof(TransactionId) + sizeof(uint16);
    }
    return toastData;
}

/*
 * Parse XLOG_UHEAP_UPDATEs.
 */
static void DecodeUUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    RelFileNode targetNode = {0, 0, 0, 0, 0};
    XlUHeapUpdate *xlrec = (XlUHeapUpdate *)UGetXlrec(r);
    CommitSeqNo curCSN = InvalidCommitSeqNo;
    bool hasCSN = (r->decoded_record->xl_term & XLOG_CONTAIN_CSN) == 0 ? false: true;
    if (hasCSN) {
        curCSN = *(CommitSeqNo *)((char *)xlrec + SizeOfUHeapUpdate);
    }
    bool isInplaceUpdate = (xlrec->flags & XLZ_NON_INPLACE_UPDATE) == 0;
    if (FilterRecord(ctx, r, xlrec->flags, &targetNode)) {
        return;
    }
    Size datalenNew = 0;
    char *dataNew = XLogRecGetBlockData(r, 0, &datalenNew);

    Size tuplelenOld = XLogRecGetDataLen(r) - SizeOfUHeapUpdate - (hasCSN ? sizeof(CommitSeqNo) : 0);
    char *dataOld = (char *)xlrec + SizeOfUHeapUpdate + (hasCSN ? sizeof(CommitSeqNo) : 0);
    uint32 toastLen = 0;
    bool hasToast = false;
    char *toastPtr = UpdateOldTupleCalc(isInplaceUpdate, r, &dataOld, &tuplelenOld, &toastLen);
    char *toastData = NULL;
    if (toastLen > 0) {
        toastData = (char *)palloc0(toastLen);
        errno_t rc = memcpy_s(toastData, toastLen, toastPtr, toastLen);
        securec_check(rc, "", "");
        hasToast = true;
    }

    if (toastLen == 0 && (tuplelenOld == 0 || !AllocSizeIsValid(tuplelenOld))) {
        ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE),
            errmsg("tuplelen is invalid(%lu), don't decode it", tuplelenOld)));
        return;
    }

    ReorderBufferChange *change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_UUPDATE;
    change->origin_id = XLogRecGetOrigin(r);
    errno_t rc = memcpy_s(&change->data.utp.relnode, sizeof(RelFileNode), &targetNode, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");
    change->data.utp.newtuple = ReorderBufferGetUTupleBuf(ctx->reorder, datalenNew);

    DecodeXLogTuple(dataNew, datalenNew, (ReorderBufferTupleBuf *)change->data.utp.newtuple, false);
    if (xlrec->flags & XLZ_HAS_UPDATE_UNDOTUPLE) {
        if (!hasToast) {
            change->data.utp.oldtuple = ReorderBufferGetUTupleBuf(ctx->reorder, tuplelenOld);
            if (!isInplaceUpdate) {
                DecodeXLogTuple(dataOld, tuplelenOld, (ReorderBufferTupleBuf *)change->data.utp.oldtuple, false);
            } else if ((xlrec->flags & XLOG_UHEAP_CONTAINS_OLD_HEADER) != 0) {
                int undoXorDeltaSize = *(int *)dataOld;
                dataOld += sizeof(int) + undoXorDeltaSize;
                tuplelenOld -= sizeof(int) + undoXorDeltaSize;
                DecodeXLogTuple(dataOld, tuplelenOld, (ReorderBufferTupleBuf *)change->data.utp.oldtuple, false);
            } else {
                ereport(LOG, (errmodule(MOD_LOGICAL_DECODE),
                    errmsg("current tuple is not fully logged, don't decode it")));
                return;
            }
        } else {
            change->data.utp.oldtuple = ReorderBufferGetUTupleBuf(ctx->reorder, toastLen);
            DecodeUHeapToastTuple(toastData, toastLen, (ReorderBufferTupleBuf *)change->data.utp.oldtuple);
        }
    }

    change->data.utp.snapshotcsn = curCSN;
    change->data.utp.clear_toast_afterwards = true;
    ReorderBufferQueueChange(ctx, UHeapXlogGetCurrentXid(r, hasCSN), buf->origptr, change);
    if (toastData != NULL) {
        pfree(toastData);
    }
}

static void AreaDecodeUUpdate(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    RelFileNode targetNode = {0, 0, 0, 0, 0};
    XlUHeapUpdate *xlrec = (XlUHeapUpdate *)UGetXlrec(r);
    CommitSeqNo curCSN = InvalidCommitSeqNo;
    XLogRecGetBlockTag(r, 0, &targetNode, NULL, NULL);
    bool hasCSN = (r->decoded_record->xl_term & XLOG_CONTAIN_CSN) == 0 ? false: true;
    if (hasCSN) {
        curCSN = *(CommitSeqNo *)((char *)xlrec + SizeOfUHeapUpdate);
    }
    bool isInplaceUpdate = (xlrec->flags & XLZ_NON_INPLACE_UPDATE) == 0;
    Size tuplelenOld = XLogRecGetDataLen(r) - SizeOfUHeapUpdate - (hasCSN ? sizeof(CommitSeqNo) : 0);
    char *dataOld = (char *)xlrec + SizeOfUHeapUpdate + (hasCSN ? sizeof(CommitSeqNo) : 0);
    uint32 toastLen = 0;
    char *toastPtr = UpdateOldTupleCalc(isInplaceUpdate, r, &dataOld, &tuplelenOld, &toastLen);
    bool hasToast = false;
    char *toastData = NULL;
    if (toastLen > 0) {
        toastData = (char *)palloc0(toastLen);
        errno_t rc = memcpy_s(toastData, toastLen, toastPtr, toastLen);
        securec_check(rc, "", "");
        hasToast = true;
    }

    if (toastLen == 0 && (tuplelenOld == 0 || !AllocSizeIsValid(tuplelenOld))) {
        ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE),
            errmsg("tuplelen is invalid(%lu), don't decode it", tuplelenOld)));
        return;
    }

    Size datalenNew = 0;
    char *dataNew = XLogRecGetBlockData(r, 0, &datalenNew);

    ReorderBufferChange *change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_UUPDATE;
    change->origin_id = XLogRecGetOrigin(r);
    int ret = memcpy_s(&change->data.utp.relnode, sizeof(RelFileNode), &targetNode, sizeof(RelFileNode));
    securec_check(ret, "\0", "\0");
    change->data.utp.newtuple = ReorderBufferGetUTupleBuf(ctx->reorder, datalenNew);

    DecodeXLogTuple(dataNew, datalenNew, (ReorderBufferTupleBuf *)change->data.utp.newtuple, false);
    if (xlrec->flags & XLZ_HAS_UPDATE_UNDOTUPLE) {
        if (!hasToast) {
            change->data.utp.oldtuple = ReorderBufferGetUTupleBuf(ctx->reorder, tuplelenOld);
            if (!isInplaceUpdate) {
                DecodeXLogTuple(dataOld, tuplelenOld, (ReorderBufferTupleBuf *)change->data.utp.oldtuple, false);
            } else if ((xlrec->flags & XLOG_UHEAP_CONTAINS_OLD_HEADER) != 0) {
                int undoXorDeltaSize = *(int *)dataOld;
                dataOld += sizeof(int) + undoXorDeltaSize;
                tuplelenOld -= sizeof(int) + undoXorDeltaSize;
                DecodeXLogTuple(dataOld, tuplelenOld, (ReorderBufferTupleBuf *)change->data.utp.oldtuple, false);
            } else {
                ereport(LOG, (errmodule(MOD_LOGICAL_DECODE),
                    errmsg("current tuple is not fully logged, don't decode it")));
                return;
            }
        } else {
            change->data.utp.oldtuple = ReorderBufferGetUTupleBuf(ctx->reorder, toastLen);
            DecodeUHeapToastTuple(toastData, toastLen, (ReorderBufferTupleBuf *)change->data.utp.oldtuple);
        }
    }
    change->data.utp.snapshotcsn = curCSN;
    change->data.utp.clear_toast_afterwards = true;
    AreaDecodingChange(change, ctx, buf);
    if (toastData != NULL) {
        pfree(toastData);
    }
}

/*
 * Parse XLOG_HEAP_DELETE from wal into proper tuplebufs.
 *
 * Deletes can possibly contain the old primary key.
 */
static void DecodeDelete(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    RelFileNode target_node = {0, 0, 0, 0, 0};
    xl_heap_delete *xlrec = (xl_heap_delete *)GetXlrec(r);
    Size heapDeleteSize;
    if ((XLogRecGetInfo(r) & XLOG_TUPLE_LOCK_UPGRADE_FLAG) == 0) {
        heapDeleteSize = SizeOfOldHeapDelete;
    } else {
        heapDeleteSize = SizeOfHeapDelete;
    }

    CommitSeqNo curCSN = InvalidCommitSeqNo;
    bool hasCSN = (r->decoded_record->xl_term & XLOG_CONTAIN_CSN) == 0 ? false: true;
    if (hasCSN) {
        curCSN = *(CommitSeqNo *)((char *)xlrec + heapDeleteSize);
    }

    /* only interested in our database */
    XLogRecGetBlockTag(r, 0, &target_node, NULL, NULL);
    if (target_node.dbNode != ctx->slot->data.database) {
        return;
    }
    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }

    Size datalen = XLogRecGetDataLen(r) - heapDeleteSize;
    ReorderBufferChange *change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_DELETE;
    change->origin_id = XLogRecGetOrigin(r);
    errno_t rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &target_node, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");

    /* old primary key stored */
    if (xlrec->flags & XLH_DELETE_CONTAINS_OLD) {
        Assert(XLogRecGetDataLen(r) > (heapDeleteSize + SizeOfHeapHeader));
        change->data.tp.oldtuple = ReorderBufferGetTupleBuf(ctx->reorder, datalen);

        DecodeXLogTuple((char *)xlrec + heapDeleteSize + (hasCSN ? sizeof(CommitSeqNo) : 0),
            datalen - (hasCSN ? sizeof(CommitSeqNo) : 0), change->data.tp.oldtuple, true);
    }
    change->data.tp.snapshotcsn = curCSN;
    change->data.tp.clear_toast_afterwards = true;

    ReorderBufferQueueChange(ctx, XLogRecGetXid(r), buf->origptr, change);
}

/*
 * Decode insert sql for area decoding.
 *
 */
static void AreaDecodeDelete(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    RelFileNode target_node = {0, 0, 0, 0, 0};
    xl_heap_delete *xlrec = (xl_heap_delete *)GetXlrec(r);
    Size heapDeleteSize = 0;
    if ((XLogRecGetInfo(r) & XLOG_TUPLE_LOCK_UPGRADE_FLAG) == 0) {
        heapDeleteSize = SizeOfOldHeapDelete;
    } else {
        heapDeleteSize = SizeOfHeapDelete;
    }

    CommitSeqNo curCSN = InvalidCommitSeqNo;
    bool hasCSN = (r->decoded_record->xl_term & XLOG_CONTAIN_CSN) == 0 ? false: true;
    if (hasCSN) {
        curCSN = *(CommitSeqNo *)((char *)xlrec + heapDeleteSize);
    }
    XLogRecGetBlockTag(r, 0, &target_node, NULL, NULL);
    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }

    Size datalen = XLogRecGetDataLen(r) - heapDeleteSize;
    ReorderBufferChange *change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_DELETE;
    change->origin_id = XLogRecGetOrigin(r);
    errno_t rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &target_node, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");

    /* old primary key stored */
    if (xlrec->flags & XLH_DELETE_CONTAINS_OLD) {
        Assert(XLogRecGetDataLen(r) > (heapDeleteSize + SizeOfHeapHeader));
        change->data.tp.oldtuple = ReorderBufferGetTupleBuf(ctx->reorder, datalen);

        DecodeXLogTuple((char *)xlrec + heapDeleteSize + (hasCSN ? sizeof(CommitSeqNo) : 0),
            datalen - (hasCSN ? sizeof(CommitSeqNo) : 0), change->data.tp.oldtuple, true);
    }
    change->data.tp.snapshotcsn = curCSN;
    change->data.tp.clear_toast_afterwards = true;
    AreaDecodingChange(change, ctx, buf);

}

/*
 * Parse XLOG_UHEAP_DELETE from wal into proper tuplebufs.
 *
 * Deletes can possibly contain the old primary key.
 */
static void DecodeUDelete(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    XlUHeapDelete *xlrec = NULL;
    RelFileNode targetNode = {0, 0, 0, 0, 0};
    xlrec = (XlUHeapDelete *)UGetXlrec(r);
    CommitSeqNo curCSN = InvalidCommitSeqNo;
    bool hasCSN = (r->decoded_record->xl_term & XLOG_CONTAIN_CSN) == 0 ? false: true;
    if (hasCSN) {
        curCSN = *(CommitSeqNo *)((char *)xlrec + SizeOfUHeapDelete);
    }

    XLogRecGetBlockTag(r, 0, &targetNode, NULL, NULL);
    if (targetNode.dbNode != ctx->slot->data.database) {
        return;
    }
    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapDelete + (hasCSN ? sizeof(CommitSeqNo) : 0));
    bool hasToast = (xlundohdr->flag & XLOG_UNDO_HEADER_HAS_TOAST) != 0;
    Size datalen = XLogRecGetDataLen(r) - SizeOfUHeapDelete - SizeOfXLUndoHeader - (hasCSN ? sizeof(CommitSeqNo) : 0);
    Size addLen = 0;
    uint32 toastLen = 0;
    UpdateUndoBody(&addLen, (char *)xlundohdr + SizeOfXLUndoHeader, xlundohdr->flag, &toastLen);
    char *toastData = NULL;
    if (toastLen > 0) {
        toastData = (char *)palloc0(toastLen);
        errno_t rc = memcpy_s(toastData, toastLen,
            (char *)xlrec + SizeOfUHeapDelete + (hasCSN ? sizeof(CommitSeqNo) : 0) + SizeOfXLUndoHeader + addLen,
            toastLen);
        securec_check(rc, "\0", "\0");
    }
    addLen += toastLen;

    Size metaLen = DecodeUndoMeta((char*)xlrec + SizeOfUHeapDelete + (hasCSN ? sizeof(CommitSeqNo) : 0) +
        SizeOfXLUndoHeader + addLen);
    addLen += metaLen;
    if (toastLen == 0 && (datalen == 0 || !AllocSizeIsValid(datalen))) {
        ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE),
            errmsg("tuplelen is invalid(%lu), don't decode it", datalen)));
        return;
    }
    ReorderBufferChange* change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_UDELETE;
    change->origin_id = XLogRecGetOrigin(r);
    errno_t rc = memcpy_s(&change->data.utp.relnode, sizeof(RelFileNode), &targetNode, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");

    char *dataold =
        (char *)xlrec + SizeOfUHeapDelete + (hasCSN ? sizeof(CommitSeqNo) : 0) + SizeOfXLUndoHeader + addLen;
    if (!hasToast) {
        change->data.utp.oldtuple = ReorderBufferGetUTupleBuf(ctx->reorder, datalen -addLen);
        DecodeXLogTuple(dataold, datalen - addLen, (ReorderBufferTupleBuf *)change->data.utp.oldtuple, false);
    } else {
        change->data.utp.oldtuple = ReorderBufferGetUTupleBuf(ctx->reorder, toastLen);
        DecodeUHeapToastTuple(toastData, toastLen, (ReorderBufferTupleBuf *)change->data.utp.oldtuple);
    }
    change->data.utp.clear_toast_afterwards = true;
    change->data.utp.snapshotcsn = curCSN;
    ReorderBufferQueueChange(ctx, UHeapXlogGetCurrentXid(r, hasCSN), buf->origptr, change);
    if (toastData != NULL) {
        pfree(toastData);
    }
}


static void AreaDecodeUDelete(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    XlUHeapDelete *xlrec = NULL;
    RelFileNode targetNode = {0, 0, 0, 0, 0};
    xlrec = (XlUHeapDelete *)UGetXlrec(r);
    CommitSeqNo curCSN = InvalidCommitSeqNo;
    bool hasCSN = (r->decoded_record->xl_term & XLOG_CONTAIN_CSN) == 0 ? false: true;
    if (hasCSN) {
        curCSN = *(CommitSeqNo *)((char *)xlrec + SizeOfUHeapDelete);
    }
    XLogRecGetBlockTag(r, 0, &targetNode, NULL, NULL);

    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapDelete + (hasCSN ? sizeof(CommitSeqNo) : 0));
    bool hasToast = (xlundohdr->flag & XLOG_UNDO_HEADER_HAS_TOAST) != 0;
    Size datalen = XLogRecGetDataLen(r) - SizeOfUHeapDelete - SizeOfXLUndoHeader - (hasCSN ? sizeof(CommitSeqNo) : 0);
    Size addLen = 0;
    uint32 toastLen = 0;
    UpdateUndoBody(&addLen, (char *)xlundohdr + SizeOfXLUndoHeader, xlundohdr->flag, &toastLen);
    char *toastData = NULL;
    if (toastLen > 0) {
        toastData = (char *)palloc0(toastLen);
        errno_t rc = memcpy_s(toastData, toastLen,
            (char *)xlrec + SizeOfUHeapDelete + (hasCSN ? sizeof(CommitSeqNo) : 0) + SizeOfXLUndoHeader + addLen,
            toastLen);
        securec_check(rc, "\0", "\0");
    }
    addLen += toastLen;

    Size metaLen = DecodeUndoMeta((char*)xlrec + SizeOfUHeapDelete + (hasCSN ? sizeof(CommitSeqNo) : 0) +
        SizeOfXLUndoHeader + addLen);
    addLen += metaLen;

    ReorderBufferChange* change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_UDELETE;
    change->origin_id = XLogRecGetOrigin(r);
    errno_t rc = memcpy_s(&change->data.utp.relnode, sizeof(RelFileNode), &targetNode, sizeof(RelFileNode));
    securec_check(rc, "\0", "\0");

    char *dataold =
        (char *)xlrec + SizeOfUHeapDelete + (hasCSN ? sizeof(CommitSeqNo) : 0) + SizeOfXLUndoHeader + addLen;

    change->data.utp.oldtuple = ReorderBufferGetUTupleBuf(ctx->reorder, datalen);
    if (!hasToast) {
        change->data.utp.oldtuple = ReorderBufferGetUTupleBuf(ctx->reorder, datalen -addLen);
        DecodeXLogTuple(dataold, datalen - addLen, (ReorderBufferTupleBuf *)change->data.utp.oldtuple, false);
    } else {
        change->data.utp.oldtuple = ReorderBufferGetUTupleBuf(ctx->reorder, toastLen);
        DecodeUHeapToastTuple(toastData, toastLen, (ReorderBufferTupleBuf *)change->data.utp.oldtuple);
    }

    change->data.utp.clear_toast_afterwards = true;
    change->data.utp.snapshotcsn = curCSN;
    AreaDecodingChange(change, ctx, buf);
    if (toastData != NULL) {
        pfree(toastData);
    }
}

/*
 * Decode XLOG_HEAP2_MULTI_INSERT_insert record into multiple tuplebufs.
 *
 * Currently MULTI_INSERT will always contain the full tuples.
 */
static void DecodeMultiInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    Size tuplelen = 0;
    RelFileNode rnode = {0, 0, 0, 0, 0};
    xl_heap_multi_insert *xlrec = (xl_heap_multi_insert *)GetXlrec(r);
    CommitSeqNo curCSN = InvalidCommitSeqNo;
    bool hasCSN = (r->decoded_record->xl_term & XLOG_CONTAIN_CSN) == 0 ? false: true;
    if (hasCSN) {
        curCSN = *(CommitSeqNo *)((char *)xlrec + SizeOfHeapMultiInsert);
    }

    if (xlrec->isCompressed) {
        return;
    }
    /* only interested in our database */
    XLogRecGetBlockTag(r, 0, &rnode, NULL, NULL);
    if (rnode.dbNode != ctx->slot->data.database) {
        return;
    }
    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }

    char *tupledata = XLogRecGetBlockData(r, 0, &tuplelen);
    char *data = tupledata;
    for (int i = 0; i < xlrec->ntuples; i++) {
        xl_multi_insert_tuple *xlhdr = NULL;
        int datalen = 0;

        ReorderBufferChange *change = ReorderBufferGetChange(ctx->reorder);
        change->action = REORDER_BUFFER_CHANGE_INSERT;
        change->origin_id = XLogRecGetOrigin(r);
        errno_t rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &rnode, sizeof(RelFileNode));
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
            if ((data - tupledata) % ALIGNOF_SHORT == 0) {
                xlhdr = (xl_multi_insert_tuple *)data;
            } else {
                xlhdr = (xl_multi_insert_tuple *)(data + ALIGNOF_SHORT - (data - tupledata) % ALIGNOF_SHORT);
            }
            data = ((char *)xlhdr) + SizeOfMultiInsertTuple;
            datalen = xlhdr->datalen;
            if (datalen != 0 && AllocSizeIsValid((uint)datalen)) {
                change->data.tp.newtuple = ReorderBufferGetTupleBuf(ctx->reorder, datalen);
                ReorderBufferTupleBuf *tuple = change->data.tp.newtuple;
                header = tuple->tuple.t_data;

                /* not a disk based tuple */
                ItemPointerSetInvalid(&tuple->tuple.t_self);

                /*
                 * We can only figure this out after reassembling the
                 * transactions.
                 */
                tuple->tuple.t_tableOid = InvalidOid;
                tuple->tuple.t_bucketId = InvalidBktId;
                tuple->tuple.t_len = datalen + SizeofHeapTupleHeader;

                rc = memset_s(header, SizeofHeapTupleHeader, 0, SizeofHeapTupleHeader);
                securec_check(rc, "\0", "\0");
                rc = memcpy_s((char *)tuple->tuple.t_data + SizeofHeapTupleHeader, datalen, (char *)data, datalen);
                securec_check(rc, "\0", "\0");

                header->t_infomask = xlhdr->t_infomask;
                header->t_infomask2 = xlhdr->t_infomask2;
                header->t_hoff = xlhdr->t_hoff;
            } else {
                ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE),
                    errmsg("tuplelen is invalid(%d), tuplelen, don't decode it", datalen)));
                return;
            }
            data += datalen;
        }
        change->data.tp.snapshotcsn = curCSN;
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

        ReorderBufferQueueChange(ctx, XLogRecGetXid(r), buf->origptr, change);
    }
}

/*
 * Decode multiInsert sql for area decoding.
 *
 */
static void AreaDecodeMultiInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    Size tuplelen = 0;
    RelFileNode rnode = {0, 0, 0, 0, 0};
    xl_heap_multi_insert *xlrec = (xl_heap_multi_insert *)GetXlrec(r);
    CommitSeqNo curCSN = InvalidCommitSeqNo;
    bool hasCSN = (r->decoded_record->xl_term & XLOG_CONTAIN_CSN) == 0 ? false: true;
    if (hasCSN) {
        curCSN = *(CommitSeqNo *)((char *)xlrec + SizeOfHeapMultiInsert);
    }

    if (xlrec->isCompressed) {
        return;
    }
    XLogRecGetBlockTag(r, 0, &rnode, NULL, NULL);
    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }

    char *tupledata = XLogRecGetBlockData(r, 0, &tuplelen);
    char *data = tupledata;
    for (int i = 0; i < xlrec->ntuples; i++) {
        xl_multi_insert_tuple *xlhdr = NULL;

        ReorderBufferChange *change = ReorderBufferGetChange(ctx->reorder);
        change->action = REORDER_BUFFER_CHANGE_INSERT;
        change->origin_id = XLogRecGetOrigin(r);
        errno_t rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), &rnode, sizeof(RelFileNode));
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
            if ((data - tupledata) % ALIGNOF_SHORT == 0) {
                xlhdr = (xl_multi_insert_tuple *)data;
            } else {
                xlhdr = (xl_multi_insert_tuple *)(data + ALIGNOF_SHORT - (data - tupledata) % ALIGNOF_SHORT);
            }
            data = ((char *)xlhdr) + SizeOfMultiInsertTuple;
            int datalen = xlhdr->datalen;
            if (datalen != 0 && AllocSizeIsValid((uint)datalen)) {
                change->data.tp.newtuple = ReorderBufferGetTupleBuf(ctx->reorder, datalen);
                ReorderBufferTupleBuf *tuple = change->data.tp.newtuple;
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
                ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE),
                    errmsg("tuplelen is invalid(%d), tuplelen, don't decode it", datalen)));
                return;
            }
            data = data + datalen;
        }
        change->data.tp.snapshotcsn = curCSN;

        if ((xlrec->flags & XLH_INSERT_LAST_IN_MULTI) && ((i + 1) == xlrec->ntuples)) {
            change->data.tp.clear_toast_afterwards = true;
        } else {
            change->data.tp.clear_toast_afterwards = false;
        }
       AreaDecodingChange(change, ctx, buf);
    }
}

static ReorderBufferChange* GetUheapChange(LogicalDecodingContext *ctx, XLogReaderState *r, RelFileNode* node)
{
    ReorderBufferChange *change = ReorderBufferGetChange(ctx->reorder);
    change->action = REORDER_BUFFER_CHANGE_UINSERT;
    change->origin_id = XLogRecGetOrigin(r);
    errno_t rc = memcpy_s(&change->data.tp.relnode, sizeof(RelFileNode), node, sizeof(RelFileNode));
    securec_check(rc, "", "");
    return change;
}

static void DecodeUMultiInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    Size tuplelen = 0;
    RelFileNode rnode = {0, 0, 0, 0, 0};
    CommitSeqNo curCSN = 0;
    XlUHeapMultiInsert *xlrec = (XlUHeapMultiInsert *)UGetMultiInsertXlrec(r, &curCSN);

    XLogRecGetBlockTag(r, 0, &rnode, NULL, NULL);
    if (rnode.dbNode != ctx->slot->data.database) {
        return;
    }
    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }
    char *data = XLogRecGetBlockData(r, 0, &tuplelen);
    for (int i = 0; i < xlrec->ntuples; i++) {
        ReorderBufferChange *change = GetUheapChange(ctx, r, &rnode);

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
                change->data.utp.newtuple = ReorderBufferGetUTupleBuf(ctx->reorder, len);
                ReorderBufferUTupleBuf *tuple = change->data.utp.newtuple;
                UHeapDiskTuple header = tuple->tuple.disk_tuple;

                /* not a disk based tuple */
                ItemPointerSetInvalid(&tuple->tuple.ctid);

                /*
                 * We can only figure this out after reassembling the
                 * transactions.
                 */
                tuple->tuple.table_oid = InvalidOid;
                tuple->tuple.t_bucketId = InvalidBktId;
                tuple->tuple.disk_tuple_size = len + SizeOfUHeapDiskTupleData;

                errno_t rc = memset_s(header, SizeOfUHeapDiskTupleData, 0, SizeOfUHeapDiskTupleData);
                securec_check(rc, "\0", "\0");
                rc = memcpy_s((char *)tuple->tuple.disk_tuple + SizeOfUHeapDiskTupleData, len, (char *)data, len);
                securec_check(rc, "\0", "\0");

                header->flag = xlhdr->flag;
                header->flag2 = xlhdr->flag2;
                header->t_hoff = xlhdr->t_hoff;
            } else {
                ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE),
                    errmsg("tuplelen is invalid(%d), don't decode it", len)));
                return;
            }
            data += len;
        }

        /*
         * Reset toast reassembly state only after the last row in the last
         * xl_multi_insert_tuple record emitted by one heap_multi_insert()
         * call.
         */
        if ((xlrec->flags & XLOG_UHEAP_INSERT_LAST_IN_MULTI) && (i + 1) == xlrec->ntuples) {
            change->data.utp.clear_toast_afterwards = true;
        } else {
            change->data.utp.clear_toast_afterwards = false;
        }
        change->data.utp.snapshotcsn = curCSN;
        ReorderBufferQueueChange(ctx, UHeapXlogGetCurrentXid(r, false), buf->origptr, change);
    }
}

static void AreaDecodeUMultiInsert(LogicalDecodingContext *ctx, XLogRecordBuffer *buf)
{
    XLogReaderState *r = buf->record;
    int i;
    Size tuplelen = 0;
    RelFileNode rnode = {0, 0, 0, 0, 0};
    CommitSeqNo curCSN = 0;
    XlUHeapMultiInsert *xlrec = (XlUHeapMultiInsert *)UGetMultiInsertXlrec(r, &curCSN);

    XLogRecGetBlockTag(r, 0, &rnode, NULL, NULL);

    /* output plugin doesn't look for this origin, no need to queue */
    if (FilterByOrigin(ctx, XLogRecGetOrigin(r))) {
        return;
    }
    char *data = XLogRecGetBlockData(r, 0, &tuplelen);
    for (i = 0; i < xlrec->ntuples; i++) {
        ReorderBufferChange *change = GetUheapChange(ctx, r, &rnode);

        if (xlrec->flags & XLOG_UHEAP_CONTAINS_NEW_TUPLE) {
            XlMultiInsertUTuple *xlhdr = (XlMultiInsertUTuple *)data;
            int len = xlhdr->datalen;
            data = ((char *)xlhdr) + SizeOfMultiInsertUTuple;
            if (len != 0 && AllocSizeIsValid((uint)len)) {
                change->data.utp.newtuple = ReorderBufferGetUTupleBuf(ctx->reorder, len);
                ReorderBufferUTupleBuf *tuple = change->data.utp.newtuple;
                UHeapDiskTuple header = tuple->tuple.disk_tuple;

                /* not a disk based tuple */
                ItemPointerSetInvalid(&tuple->tuple.ctid);

                tuple->tuple.t_bucketId = InvalidBktId;
                tuple->tuple.table_oid = InvalidOid;
                tuple->tuple.disk_tuple_size = len + SizeOfUHeapDiskTupleData;

                errno_t ret = memset_s(header, SizeOfUHeapDiskTupleData, 0, SizeOfUHeapDiskTupleData);
                securec_check(ret, "\0", "\0");
                ret = memcpy_s((char *)tuple->tuple.disk_tuple + SizeOfUHeapDiskTupleData, len, (char *)data, len);
                securec_check(ret, "\0", "\0");

                header->flag2 = xlhdr->flag2;
                header->t_hoff = xlhdr->t_hoff;
                header->flag = xlhdr->flag;
            } else {
                ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE),
                    errmsg("tuplelen is invalid(%d), don't decode it", len)));
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
            change->data.utp.clear_toast_afterwards = true;
        } else {
            change->data.utp.clear_toast_afterwards = false;
        }
        change->data.tp.snapshotcsn = curCSN;
        AreaDecodingChange(change, ctx, buf);
    }
}

/*
 * Read a HeapTuple as WAL logged by heap_insert, heap_update and heap_delete
 * (but not by heap_multi_insert) into a tuplebuf.
 *
 * The size 'len' and the pointer 'data' in the record need to be
 * computed outside as they are record specific.
 */
void DecodeXLogTuple(const char *data, Size len, ReorderBufferTupleBuf *tuple, bool isHeapTuple)
{
    int datalen = 0;
    int rc = 0;

    if (isHeapTuple) {
        xl_heap_header xlhdr;
        datalen = len - SizeOfHeapHeader;
        Assert(datalen >= 0);
        tuple->tuple.t_len = datalen + SizeofHeapTupleHeader;
        HeapTupleHeader header = tuple->tuple.t_data;

        /* not a disk based tuple */
        ItemPointerSetInvalid(&tuple->tuple.t_self);

        /* we can only figure this out after reassembling the transactions */
        tuple->tuple.t_tableOid = InvalidOid;
        tuple->tuple.t_bucketId = InvalidBktId;

        /* data is not stored aligned, copy to aligned storage */
        rc = memcpy_s((char *)&xlhdr, SizeOfHeapHeader, data, SizeOfHeapHeader);
        securec_check(rc, "", "");
        rc = memset_s(header, SizeofHeapTupleHeader, 0, SizeofHeapTupleHeader);
        securec_check(rc, "", "");
        rc = memcpy_s(((char *)tuple->tuple.t_data) + SizeofHeapTupleHeader, datalen, data + SizeOfHeapHeader, datalen);
        securec_check(rc, "", "");

        tuple->tuple.t_len = datalen + SizeofHeapTupleHeader;
        header->t_infomask = xlhdr.t_infomask;
        header->t_infomask2 = xlhdr.t_infomask2;
        header->t_hoff = xlhdr.t_hoff;
    } else {
        XlUHeapHeader xlhdr;
        datalen = len - SizeOfUHeapHeader;
        Assert(datalen >= 0);
        UHeapDiskTuple header;
        /* in this case, we reinterpret the tupleData as UHeapTupleData */
        UHeapTupleData* utuple = (UHeapTupleData *)(&tuple->tuple);

        header = utuple->disk_tuple;

        /* not a disk based tuple */
        ItemPointerSetInvalid(&utuple->ctid);

        /* we can only figure this out after reassembling the transactions */
        utuple->table_oid = InvalidOid;
        utuple->t_bucketId = InvalidBktId;

        /* data is not stored aligned, copy to aligned storage */
        rc = memcpy_s((char *)&xlhdr, SizeOfUHeapHeader, data, SizeOfUHeapHeader);
        securec_check(rc, "", "");
        rc = memset_s(header, SizeOfUHeapDiskTupleData, 0, SizeOfUHeapDiskTupleData);
        securec_check(rc, "", "");
        rc = memcpy_s(((char *)utuple->disk_tuple) + SizeOfUHeapDiskTupleData, datalen, data + SizeOfUHeapHeader,
            datalen);
        securec_check(rc, "", "");
        utuple->disk_tuple_size = datalen + SizeOfUHeapDiskTupleData;

        header->flag = xlhdr.flag;
        header->flag2 = xlhdr.flag2;
        header->t_hoff = xlhdr.t_hoff;
    }
}

void DecodeUHeapToastTuple(const char * toastData, Size len, ReorderBufferTupleBuf *tuple)
{
    UHeapTupleData *utuple = (UHeapTupleData *)(&tuple->tuple);
    UHeapDiskTuple header = utuple->disk_tuple;
    XlUHeapHeader xlhdr;

    ItemPointerSetInvalid(&utuple->ctid);
    utuple->table_oid = InvalidOid;
    utuple->t_bucketId = InvalidBktId;
    errno_t rc = 0;
    rc = memcpy_s((char *)&xlhdr, SizeOfUHeapHeader, toastData, SizeOfUHeapHeader);
    securec_check(rc, "", "");
    rc = memset_s(header, SizeOfUHeapDiskTupleData, 0, SizeOfUHeapDiskTupleData);
    securec_check(rc, "", "");
    rc = memcpy_s(((char *)utuple->disk_tuple) + SizeOfUHeapDiskTupleData, len - SizeOfUHeapHeader,
        toastData + SizeOfUHeapHeader, len - SizeOfUHeapHeader);
    securec_check(rc, "", "");
    utuple->disk_tuple_size = len - SizeOfUHeapHeader + SizeOfUHeapDiskTupleData;
    header->flag = xlhdr.flag;
    header->flag2 = xlhdr.flag2;
    header->t_hoff = xlhdr.t_hoff;
}

