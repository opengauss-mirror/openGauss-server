/* -------------------------------------------------------------------------
 *
 * knl_uredo.cpp
 * WAL replay logic for uheap.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/storage/access/ustore/knl_uredo.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "access/parallel_recovery/page_redo.h"
#include "access/xlogutils.h"
#include "catalog/pg_tablespace.h"
#include "storage/standby.h"
#include "access/ustore/knl_uredo.h"
#include "access/ustore/knl_upage.h"
#include "access/ustore/knl_undorequest.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "access/ustore/knl_uheap.h"
#include "access/ustore/undo/knl_uundoxlog.h"
#include "access/ustore/knl_whitebox_test.h"
#include "securec.h"
#include "storage/freespace.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/multi_redo_api.h"

using namespace undo;

static const int FREESPACE_FRACTION = 5;

union TupleBuffer {
    UHeapDiskTupleData hdr;
    char data[MaxPossibleUHeapTupleSize];
};

struct UpdateRedoBuffers {
    RedoBufferInfo oldbuffer;
    RedoBufferInfo newbuffer;
};

struct UpdateRedoTuples {
    UHeapTupleData *oldtup;
    UHeapDiskTuple newtup;
};

struct UpdateRedoAffixLens {
    uint16 prefixlen;
    uint16 suffixlen;
};

static UHeapDiskTuple GetUHeapDiskTupleFromRedoData(char *data, Size *datalen, TupleBuffer &tbuf, 
    const bool hasInvalidLockerTdId)
{
    UHeapDiskTuple disktup;
    XlUHeapHeader xlhdr;
    errno_t rc;

    rc = memcpy_s((char *)&xlhdr, SizeOfUHeapHeader, data, SizeOfUHeapHeader);
    securec_check(rc, "\0", "\0");
    data += SizeOfUHeapHeader;

    disktup = &tbuf.hdr;
    rc = memset_s((char *)disktup, SizeOfUHeapDiskTupleData, 0, SizeOfUHeapDiskTupleData);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s((char *)disktup + SizeOfUHeapDiskTupleData, *datalen, data, *datalen);
    securec_check(rc, "\0", "\0");
    *datalen += SizeOfUHeapDiskTupleData;

    UHeapTupleHeaderSetTDSlot(disktup, xlhdr.td_id);

    if (hasInvalidLockerTdId) {
        UHeapTupleHeaderSetLockerTDSlot(disktup, InvalidTDSlotId);
    } else {
        UHeapTupleHeaderSetLockerTDSlot(disktup, xlhdr.locker_td_id);
    }

    disktup->xid = (ShortTransactionId)FrozenTransactionId;
    disktup->flag2 = xlhdr.flag2;
    disktup->flag = xlhdr.flag;
    disktup->t_hoff = xlhdr.t_hoff;

    return disktup;
}

static UndoRecPtr PrepareAndInsertUndoRecordForInsertRedo(XLogReaderState *record, 
    const BlockNumber blkno, uint32 *skipSize, const bool tryPrepare)
{
    XLogRecPtr lsn = record->EndRecPtr;
    TransactionId xid = XLogRecGetXid(record);
    undo::XlogUndoMeta undometa;

    UndoRecPtr *blkprev;
    UndoRecPtr *prevurp;
    Oid *partitionOid;
    UndoRecPtr invalidUrp = INVALID_UNDO_REC_PTR;
    Oid invalidPartitionOid = 0;

    bool hasCSN = (record->decoded_record->xl_term & XLOG_CONTAIN_CSN) == XLOG_CONTAIN_CSN;
    XlUHeapInsert *xlrec = (XlUHeapInsert *)XLogRecGetData(record);
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapInsert + (hasCSN ? sizeof(CommitSeqNo) : 0));
    char *currLogPtr = ((char *)xlundohdr + SizeOfXLUndoHeader);
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        blkprev = (UndoRecPtr *) ((char *)currLogPtr);
        currLogPtr += sizeof(UndoRecPtr);
        *skipSize += sizeof(UndoRecPtr);
    } else {
        blkprev = &invalidUrp;
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        prevurp = (UndoRecPtr *) ((char *)currLogPtr);
        currLogPtr += sizeof(UndoRecPtr);
        *skipSize += sizeof(UndoRecPtr);
    } else {
        prevurp = &invalidUrp;
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        partitionOid = (Oid *) ((char *)currLogPtr);
        currLogPtr += sizeof(Oid);
        *skipSize += sizeof(Oid);
    } else {
        partitionOid = &invalidPartitionOid;
    }

    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_CURRENT_XID) != 0) {
        /* ignore current xid for replay */
        currLogPtr += sizeof(TransactionId);
        *skipSize += sizeof(TransactionId);
    }

    XlogUndoMeta *xlundometa = (XlogUndoMeta *)((char *)currLogPtr);
    UndoRecPtr urecptr = xlundohdr->urecptr;

    /* copy xlundometa to local struct */
    CopyUndoMeta(*xlundometa, undometa);
    *skipSize += undometa.Size();

    if (tryPrepare) {
        bool skipInsert = IsSkipInsertUndo(urecptr);
        if (skipInsert) {
            undometa.SetInfo(XLOG_UNDOMETA_INFO_SKIP);
        }

        /* We need to pass in tablespace and relfilenode in PrepareUndo but we never explicitly
         * wrote those information in the xlundohdr because we can grab them from the XLOG record itself.
         */
        RelFileNode targetNode = { 0 };
        bool res PG_USED_FOR_ASSERTS_ONLY = XLogRecGetBlockTag(record, 0, &targetNode, NULL, NULL);
        Assert(res == true);

        UndoRecord *undorec = u_sess->ustore_cxt.undo_records[0];
        undorec->SetUrp(urecptr);
        urecptr = UHeapPrepareUndoInsert(xlundohdr->relOid, *partitionOid,
            targetNode.relNode, targetNode.spcNode, UNDO_PERMANENT,
            InvalidBuffer, xid, 0, *blkprev, *prevurp, blkno, xlundohdr, &undometa);
        ereport(DEBUG2, (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT("redo:undoptr=%lu, xid %lu, partoid=%u, spcoid=%u."),
                urecptr, xid, *partitionOid, targetNode.spcNode)));
        /* recover undo record */
        Assert(urecptr == xlundohdr->urecptr);
        undorec->SetOffset(xlrec->offnum);
        if (!skipInsert) {
            /* Insert the Undo record into the undo store */
            InsertPreparedUndo(u_sess->ustore_cxt.urecvec, lsn);
        }
        
        undo::RedoUndoMeta(record, &undometa, xlundohdr->urecptr, u_sess->ustore_cxt.urecvec->LastRecord(), 
            u_sess->ustore_cxt.urecvec->LastRecordSize());
        UHeapResetPreparedUndo(UNDO_PERMANENT, UNDO_PTR_GET_ZONE_ID(urecptr));
    }

    return urecptr;
}

static XLogRedoAction GetInsertRedoAction(XLogReaderState *record, RedoBufferInfo *buffer, const uint32 skipSize)
{
    XLogRedoAction action;

    if (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_PAGE) {
        bool hasCSN = (record->decoded_record->xl_term & XLOG_CONTAIN_CSN) == XLOG_CONTAIN_CSN;
        XlUHeapInsert *xlrec = (XlUHeapInsert *)XLogRecGetData(record);
        XlUndoHeader *xlundohdr =
            (XlUndoHeader *)((char *)xlrec + SizeOfUHeapInsert + (hasCSN ? sizeof(CommitSeqNo) : 0));
        TransactionId *xidBase = (TransactionId *)((char *)xlundohdr + SizeOfXLUndoHeader + skipSize);
        uint16 *tdCount = (uint16 *)((char *)xidBase + sizeof(TransactionId));

        XLogInitBufferForRedo(record, 0, buffer);
        Page page = buffer->pageinfo.page;

        if (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_TOAST_PAGE) {
            UPageInit<UPAGE_TOAST>(page, buffer->pageinfo.pagesize, UHEAP_SPECIAL_SIZE);
        } else {
            UPageInit<UPAGE_HEAP>(page, buffer->pageinfo.pagesize, UHEAP_SPECIAL_SIZE, *tdCount);
        }

        UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
        uheappage->pd_xid_base = *xidBase;
        uheappage->pd_multi_base = 0;
        action = BLK_NEEDS_REDO;
    } else {
        action = XLogReadBufferForRedo(record, 0, buffer);
    }

    return action;
}

static void PerformInsertRedoAction(XLogReaderState *record, const Buffer buf, const UndoRecPtr urecptr, 
    TupleBuffer &tbuf)
{
    TransactionId xid = XLogRecGetXid(record);
    XlUHeapInsert *xlrec = (XlUHeapInsert *)XLogRecGetData(record);
    Size datalen;
    int tdSlotId;
    UHeapBufferPage bufpage;

    char *data = XLogRecGetBlockData(record, 0, &datalen);
    if (datalen < SizeOfUHeapHeader) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Datalen less than SizeOfUHeapHeader")));
    }
    Size newlen = datalen - SizeOfUHeapHeader;
    Page page = BufferGetPage(buf);
    if (UHeapPageGetMaxOffsetNumber(page) + 1 < xlrec->offnum) {
        elog(PANIC, "invalid max offset number");
    }

    /*
     * For UHeap, in case of "SELECT INTO" statement, length of data will
     * be equal to the UHeap header size, but in heap, it will be always
     * greater than heap header size, because in heap, we have one byte
     * alignment in case of zero byte data length.
     */
    Assert(datalen > SizeOfUHeapHeader && newlen <= MaxPossibleUHeapTupleSize);
    UHeapDiskTuple utup = GetUHeapDiskTupleFromRedoData(data, &newlen, tbuf, true);

    bufpage.buffer = buf;
    bufpage.page = NULL;

    if (UPageAddItem(NULL, &bufpage, (Item)utup, newlen, xlrec->offnum, true) == InvalidOffsetNumber) {
        elog(PANIC, "failed to add tuple");
    }

    /* decrement the potential freespace of this page */
    UHeapRecordPotentialFreeSpace(buf, -1 * SHORTALIGN(newlen));

    // set undo
    tdSlotId = UHeapTupleHeaderGetTDSlot(utup);
    UHeapPageSetUndo(buf, tdSlotId, xid, urecptr);

    PageSetLSN(page, record->EndRecPtr);
    MarkBufferDirty(buf);
}

void UHeapXlogInsert(XLogReaderState *record)
{
    RedoBufferInfo buffer;
    RelFileNode targetNode;
    BlockNumber blkno = InvalidBlockNumber;
    XLogRedoAction action;
    uint32 skipSize = 0;
    TupleBuffer tbuf;

    bool allReplay = !AmPageRedoWorker() || !SUPPORT_USTORE_UNDO_WORKER;
    bool onlyReplayUndo = allReplay ? false : parallel_recovery::DoPageRedoWorkerReplayUndo();

    WHITEBOX_TEST_STUB(UHEAP_XLOG_INSERT_FAILED, WhiteboxDefaultErrorEmit);

    XLogRecGetBlockTag(record, 0, &targetNode, NULL, &blkno);

    UndoRecPtr urecptr = PrepareAndInsertUndoRecordForInsertRedo(record, blkno, &skipSize, 
        (allReplay || onlyReplayUndo));
    if (allReplay || !onlyReplayUndo) {
        action = GetInsertRedoAction(record, &buffer, skipSize);
        if (action == BLK_NEEDS_REDO) {
            PerformInsertRedoAction(record, buffer.buf, urecptr, tbuf);
        }

        if (BufferIsValid(buffer.buf)) {
            UnlockReleaseBuffer(buffer.buf);
        }
    }
}

static UndoRecPtr PrepareAndInsertUndoRecordForDeleteRedo(XLogReaderState *record, 
    UHeapTupleData *utup, const BlockNumber blkno, const bool tryPrepare, TupleBuffer &tbuf)
{
    XLogRecPtr lsn = record->EndRecPtr;
    TransactionId xid = XLogRecGetXid(record);
    Size recordlen = XLogRecGetDataLen(record);
    undo::XlogUndoMeta undometa;

    UndoRecPtr *blkprev;
    UndoRecPtr *prevurp;
    Oid *partitionOid;
    bool *hasSubXact;
    UndoRecPtr invalidUrp = INVALID_UNDO_REC_PTR;
    Oid invalidPartitionOid = 0;
    bool defaultHasSubXact = false;
    uint32 readSize = 0;

    bool hasCSN = (record->decoded_record->xl_term & XLOG_CONTAIN_CSN) == XLOG_CONTAIN_CSN;
    XlUHeapDelete *xlrec = (XlUHeapDelete *)XLogRecGetData(record);
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapDelete + (hasCSN ? sizeof(CommitSeqNo) : 0));
    char *currLogPtr = ((char *)xlundohdr + SizeOfXLUndoHeader);
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_SUB_XACT) != 0) {
        hasSubXact = (bool *) ((char *)currLogPtr);
        currLogPtr += sizeof(bool);
        readSize += sizeof(bool);
    } else {
        hasSubXact = &defaultHasSubXact;
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        blkprev = (UndoRecPtr *) ((char *)currLogPtr);
        currLogPtr += sizeof(UndoRecPtr);
        readSize += sizeof(UndoRecPtr);
    } else {
        blkprev = &invalidUrp;
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        prevurp = (UndoRecPtr *) ((char *)currLogPtr);
        currLogPtr += sizeof(UndoRecPtr);
        readSize += sizeof(UndoRecPtr);
    } else {
        prevurp = &invalidUrp;
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        partitionOid = (Oid *) ((char *)currLogPtr);
        currLogPtr += sizeof(Oid);
        readSize += sizeof(Oid);
    } else {
        partitionOid = &invalidPartitionOid;
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_CURRENT_XID) != 0) {
        /* ignore current xid for replay */
        currLogPtr += sizeof(TransactionId);
        readSize += sizeof(TransactionId);
    }

    undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)((char *)currLogPtr);
    UndoRecPtr urecptr = xlundohdr->urecptr;

    /* copy xlundometa to local struct */
    CopyUndoMeta(*xlundometa, undometa);
    uint32 undoMetaSize = undometa.Size();
    /*
     * If the WAL stream contains undo tuple, then replace it with the
     * explicitly stored tuple.
     */
    Size datalen = recordlen - SizeOfXLUndoHeader - SizeOfUHeapDelete - undoMetaSize - SizeOfUHeapHeader -
        readSize - (hasCSN ? sizeof(CommitSeqNo) : 0);
    char *data = (char *)xlrec + SizeOfUHeapDelete + SizeOfXLUndoHeader + undoMetaSize + readSize;

    utup->disk_tuple = GetUHeapDiskTupleFromRedoData(data, &datalen, tbuf, false);
    utup->disk_tuple_size = datalen;

    if (tryPrepare) {
        bool skipInsert = IsSkipInsertUndo(urecptr);
        if (skipInsert) {
            undometa.SetInfo(XLOG_UNDOMETA_INFO_SKIP);
        }
        
        /* recover undo record */
        TD oldTD;
        oldTD.xactid = xlrec->oldxid;
        oldTD.undo_record_ptr = INVALID_UNDO_REC_PTR;
        UndoRecord *undorec = u_sess->ustore_cxt.undo_records[0];
        undorec->SetUrp(urecptr);

        /* We need to pass in tablespace and relfilenode in PrepareUndo but we never explicitly
         * wrote those information in the xlundohdr because we can grab them from the XLOG record itself.
         */
        RelFileNode targetNode = { 0 };
        bool res PG_USED_FOR_ASSERTS_ONLY = XLogRecGetBlockTag(record, 0, &targetNode, NULL, NULL);
        Assert(res == true);

        urecptr = UHeapPrepareUndoDelete(xlundohdr->relOid, *partitionOid,
            targetNode.relNode, targetNode.spcNode, UNDO_PERMANENT,
            InvalidBuffer, xlrec->offnum, xid, 
            *hasSubXact ? TopSubTransactionId : InvalidSubTransactionId, 0,
            *blkprev, *prevurp, &oldTD, utup, blkno, xlundohdr, &undometa);
        Assert(urecptr == xlundohdr->urecptr);
        undorec->SetOffset(xlrec->offnum);
        if (!skipInsert) {
            /* Insert the Undo record into the undo store */
            InsertPreparedUndo(u_sess->ustore_cxt.urecvec, lsn);
        }
        undo::RedoUndoMeta(record, &undometa, xlundohdr->urecptr, u_sess->ustore_cxt.urecvec->LastRecord(), 
            u_sess->ustore_cxt.urecvec->LastRecordSize());
        UHeapResetPreparedUndo(UNDO_PERMANENT, UNDO_PTR_GET_ZONE_ID(urecptr));
    }

    return urecptr;
}

static void PerformDeleteRedoAction(XLogReaderState *record, UHeapTupleData *utup, 
    RedoBufferInfo *buffer, const UndoRecPtr urecptr)
{
    XLogRecPtr lsn = record->EndRecPtr;
    XlUHeapDelete *xlrec = (XlUHeapDelete *)XLogRecGetData(record);
    TransactionId xid = XLogRecGetXid(record);
    Size datalen = utup->disk_tuple_size;
    Page page = buffer->pageinfo.page;
    RowPtr *rp;

    if (UHeapPageGetMaxOffsetNumber(page) >= xlrec->offnum) {
        rp = UPageGetRowPtr(page, xlrec->offnum);
    } else {
        elog(PANIC, "invalid rp");
    }

    /* increment the potential freespace of this page */
    UHeapRecordPotentialFreeSpace(buffer->buf, SHORTALIGN(datalen));

    utup->disk_tuple = (UHeapDiskTuple)UPageGetRowData(page, rp);
    utup->disk_tuple_size = RowPtrGetLen(rp);
    UHeapTupleHeaderSetTDSlot(utup->disk_tuple, xlrec->td_id);
    utup->disk_tuple->flag = xlrec->flag;
    UHeapPageSetUndo(buffer->buf, xlrec->td_id, xid, urecptr);

    /* Mark the page as a candidate for pruning */
    UPageSetPrunable(page, xid);

    PageSetLSN(page, lsn);
    MarkBufferDirty(buffer->buf);
}

static void UHeapXlogDelete(XLogReaderState *record)
{
    RedoBufferInfo buffer;
    UHeapTupleData utup;
    RelFileNode targetNode;
    BlockNumber blkno = InvalidBlockNumber;
    ItemPointerData targetTid;
    XLogRedoAction action;
    TupleBuffer tbuf;
    XlUHeapDelete *xlrec = (XlUHeapDelete *)XLogRecGetData(record);
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapDelete);

    bool allReplay = !AmPageRedoWorker() || !SUPPORT_USTORE_UNDO_WORKER;
    bool onlyReplayUndo = allReplay ? false : parallel_recovery::DoPageRedoWorkerReplayUndo();

    WHITEBOX_TEST_STUB(UHEAP_XLOG_DELETE_FAILED, WhiteboxDefaultErrorEmit);

    XLogRecGetBlockTag(record, 0, &targetNode, NULL, &blkno);
    ItemPointerSetBlockNumber(&targetTid, blkno);
    ItemPointerSetOffsetNumber(&targetTid, xlrec->offnum);

    utup.table_oid = xlundohdr->relOid;
    utup.ctid = targetTid;

    UndoRecPtr urecptr = PrepareAndInsertUndoRecordForDeleteRedo(record, &utup, blkno, 
        (allReplay || onlyReplayUndo), tbuf);

    if (allReplay || !onlyReplayUndo) {
        action = XLogReadBufferForRedo(record, 0, &buffer);
        if (action == BLK_NEEDS_REDO) {
            PerformDeleteRedoAction(record, &utup, &buffer, urecptr);
        }

        if (BufferIsValid(buffer.buf)) {
            UnlockReleaseBuffer(buffer.buf);
        }
    }
}

static void UHeapXlogFreezeTdSlot(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    XlUHeapFreezeTdSlot *xlrec = (XlUHeapFreezeTdSlot *)XLogRecGetData(record);
    int *frozenSlots = (int *)((char *)xlrec + SizeOfUHeapFreezeTDSlot);
    RedoBufferInfo buffer;
    Page page;
    XLogRedoAction action;
    RelFileNode rnode = ((RelFileNode) {0, 0, 0, -1});
    BlockNumber blkno;
    int nFrozen = xlrec->nFrozen;
    int slotNo = 0;

    WHITEBOX_TEST_STUB(UHEAP_XLOG_FREEZE_TD_SLOT_FAILED, WhiteboxDefaultErrorEmit);

    XLogRecGetBlockTag(record, 0, &rnode, NULL, &blkno);

    if (InHotStandby && TransactionIdIsValid(xlrec->latestFrozenXid))
        ResolveRecoveryConflictWithSnapshot(xlrec->latestFrozenXid, rnode);

    action = XLogReadBufferForRedo(record, 0, &buffer);

    /*
     * skip redo processing when action is BLK_NOTFOUND, which indicates
     * a truncated or deleted block
     */
    if (action == BLK_NOTFOUND)
        return;

    if (action == BLK_NEEDS_REDO) {
        page = buffer.pageinfo.page;
        UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
        TD *transinfo = tdPtr->td_info;

        UHeapFreezeOrInvalidateTuples(buffer.buf, nFrozen, frozenSlots, true);

        for (int i = 0; i < nFrozen; i++) {
            TD *thistrans;

            slotNo = frozenSlots[i];
            thistrans = &transinfo[slotNo];

            /* Any pending frozen trans slot should have xid within range of latestxid */
            Assert(!TransactionIdFollows(thistrans->xactid, xlrec->latestFrozenXid));

            thistrans->xactid = InvalidTransactionId;
            thistrans->undo_record_ptr = INVALID_UNDO_REC_PTR;
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer.buf);
    }

    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);
}

static void UHeapXlogInvalidTdSlot(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    uint16 *nCompletedSlots = (uint16 *)XLogRecGetData(record);
    int *completedXactSlots = (int *)((char *)nCompletedSlots + sizeof(uint16));
    RedoBufferInfo buffer;
    Page page;
    XLogRedoAction action;
    int slotNo = 0;

    WHITEBOX_TEST_STUB(UHEAP_XLOG_INVALID_TD_SLOT_FAILED, WhiteboxDefaultErrorEmit);

    action = XLogReadBufferForRedo(record, 0, &buffer);

    /*
     * skip redo processing when action is BLK_NOTFOUND, which indicates
     * a truncated or deleted block
     */
    if (action == BLK_NOTFOUND)
        return;

    if (action == BLK_NEEDS_REDO) {
        page = buffer.pageinfo.page;
        UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
        TD *transinfo = tdPtr->td_info;

        UHeapFreezeOrInvalidateTuples(buffer.buf, *nCompletedSlots, completedXactSlots, false);

        for (int i = 0; i < *nCompletedSlots; i++) {
            slotNo = completedXactSlots[i];
            transinfo[slotNo].xactid = InvalidTransactionId;
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer.buf);
    }

    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);
}

static void PerformCleanRedoAction(XLogReaderState *record, RedoBufferInfo *buffer, Size *freespace)
{
    XlUHeapClean *xlrec = (XlUHeapClean *)XLogRecGetData(record);
    XLogRecPtr lsn = record->EndRecPtr;
    Page page = buffer->pageinfo.page;
    Size datalen;
    UPruneState prstate;
    int ndeleted = xlrec->ndeleted;
    int ndead = xlrec->ndead;

    OffsetNumber *deleted = (OffsetNumber *)XLogRecGetBlockData(record, 0, &datalen);
    OffsetNumber *end = (OffsetNumber *)((char *)deleted + datalen);
    OffsetNumber *nowdead = deleted + (ndeleted * 2);
    OffsetNumber *nowunused = nowdead + ndead;

    int nunused = (end - nowunused);
    Assert(nunused >= 0);

    OffsetNumber *targetOffnum;
    OffsetNumber tmpTargetOff;
    OffsetNumber *offnum;
    Size *spaceRequired;
    Size tmpSpcRqd;
    int i;
    /* Update all item pointers per the record, and repair fragmentation */
    if (xlrec->flags & XLZ_CLEAN_CONTAINS_OFFSET) {
        targetOffnum = (OffsetNumber *)((char *)xlrec + SizeOfUHeapClean);
        spaceRequired = (Size *)((char *)targetOffnum + sizeof(OffsetNumber));
    } else {
        targetOffnum = &tmpTargetOff;
        *targetOffnum = InvalidOffsetNumber;
        spaceRequired = &tmpSpcRqd;
        *spaceRequired = 0;
    }

    offnum = deleted;
    for (i = 0; i < ndeleted; i++) {
        prstate.nowdeleted[i] = *offnum++;
    }

    offnum = nowdead;
    for (i = 0; i < ndead; i++) {
        prstate.nowdead[i] = *offnum++;
    }

    offnum = nowunused;
    for (i = 0; i < nunused; i++) {
        prstate.nowunused[i] = *offnum++;
    }

    prstate.ndeleted = ndeleted;
    prstate.ndead = ndead;
    prstate.nunused = nunused;
    UHeapPagePruneExecute(buffer->buf, *targetOffnum, &prstate);

    if (xlrec->flags & XLZ_CLEAN_ALLOW_PRUNING) {
        bool pruned PG_USED_FOR_ASSERTS_ONLY = false;

        UPageRepairFragmentation(buffer->buf, *targetOffnum, *spaceRequired, &pruned, false);

        /*
         * Pruning must be successful at redo time, otherwise the page
         * contents on master and standby might differ.
         */
        Assert(pruned);
    }

    *freespace = PageGetUHeapFreeSpace(page); /* needed to update FSM
                                              * below */

    /*
     * Note: we don't worry about updating the page's prunability hints.
     * At worst this will cause an extra prune cycle to occur soon.
     */

    PageSetLSN(page, lsn);
    MarkBufferDirty(buffer->buf);
}

static void UHeapXlogClean(XLogReaderState *record)
{
    XlUHeapClean *xlrec = (XlUHeapClean *)XLogRecGetData(record);
    RedoBufferInfo buffer;
    Size freespace = 0;
    RelFileNode rnode = ((RelFileNode) {0, 0, 0, -1});
    BlockNumber blkno = InvalidBlockNumber;
    XLogRedoAction action;

    WHITEBOX_TEST_STUB(UHEAP_XLOG_CLEAN_FAILED, WhiteboxDefaultErrorEmit);

    XLogRecGetBlockTag(record, 0, &rnode, NULL, &blkno);

    /*
     * We're about to remove tuples. In Hot Standby mode, ensure that there's
     * no queries running for which the removed tuples are still visible.
     *
     * Not all INPLACEHEAP_CLEAN records remove tuples with xids, so we only want to
     * conflict on the records that cause MVCC failures for user queries. If
     * latestRemovedXid is invalid, skip conflict processing.
     */
    if (InHotStandby && TransactionIdIsValid(xlrec->latestRemovedXid))
        ResolveRecoveryConflictWithSnapshot(xlrec->latestRemovedXid, rnode);

    /*
     * If we have a full-page image, restore it (using a cleanup lock) and
     * we're done.
     */
    action = XLogReadBufferForRedo(record, 0, &buffer);

    if (action == BLK_NEEDS_REDO) {
        PerformCleanRedoAction(record, &buffer, &freespace);
    }

    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);

    /*
     * Update the FSM as well.
     *
     * XXX: Don't do this if the page was restored from full page image. We
     * don't bother to update the FSM in that case, it doesn't need to be
     * totally accurate anyway.
     */
    if (action == BLK_NEEDS_REDO)
        XLogRecordPageWithFreeSpace(rnode, blkno, freespace);
}

static void GetXidbaseAndAffixLensFromUpdateRedo(char *curxlogptr, XLogReaderState *record,
    TransactionId **xidBase, uint16 **tdCount, UpdateRedoAffixLens *affixLens)
{
    XlUHeapUpdate *xlrec = (XlUHeapUpdate *)XLogRecGetData(record);

    if (xlrec->flags & XLZ_NON_INPLACE_UPDATE) {
        if (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_PAGE) {
            /* has xidBase */
            *xidBase = (TransactionId *)curxlogptr;
            curxlogptr += sizeof(TransactionId);
            *tdCount = (uint16 *)curxlogptr;
            curxlogptr += sizeof(uint16);
        }
    } else {
        int *undoXorDeltaSizePtr = (int *)curxlogptr;
        int undoXorDeltaSize = *undoXorDeltaSizePtr;
        curxlogptr += sizeof(int);

        char *xorCurxlogptr = curxlogptr;
        curxlogptr += undoXorDeltaSize;

        uint8 *tHoffPtr = (uint8 *)xorCurxlogptr;
        uint8 tHoff = *tHoffPtr;
        /* sizeof(uint8) is the size for tHoff */
        xorCurxlogptr += sizeof(uint8) + tHoff - OffsetTdId;

        uint8 *flagsPtr = (uint8 *)xorCurxlogptr;
        uint8 flags = *flagsPtr;
        xorCurxlogptr += sizeof(uint8);

        if (flags & UREC_INPLACE_UPDATE_XOR_PREFIX) {
            uint16 *prefixlenPtr = (uint16 *)(xorCurxlogptr);
            xorCurxlogptr += sizeof(uint16);
            affixLens->prefixlen = *prefixlenPtr;
        }
        if (flags & UREC_INPLACE_UPDATE_XOR_SUFFIX) {
            uint16 *suffixlenPtr = (uint16 *)(xorCurxlogptr);
            xorCurxlogptr += sizeof(uint16);
            affixLens->suffixlen = *suffixlenPtr;
        }
    }
}

static UndoRecPtr PrepareAndInsertUndoRecordForUpdateRedo(XLogReaderState *record,
    UHeapTupleData *oldtup, XlUndoHeader **xlnewundohdr, char **xidBasePos, TupleBuffer &tbuf)
{
    XLogRecPtr lsn = record->EndRecPtr;
    Size recordlen = XLogRecGetDataLen(record);
    TransactionId xid = XLogRecGetXid(record);
    undo::XlogUndoMeta undometa;
    UndoRecPtr newUrecptr = INVALID_UNDO_REC_PTR;

    UndoRecPtr *blkprev;
    UndoRecPtr *prevurp;
    Oid *partitionOid;
    bool *hasSubXact;
    UndoRecPtr *newblkprev;
    UndoRecPtr *newprevurp;
    Oid *newpartitionOid;
    UndoRecPtr invalidUrp = INVALID_UNDO_REC_PTR;
    Oid invalidPartitionOid = 0;
    bool defaultHasSubXact = false;
    uint32 readSize = 0;
    
    int undoXorDeltaSize = 0;
    bool inplaceUpdate = true;
    char *xlogXorDelta = NULL;

    bool hasCSN = (record->decoded_record->xl_term & XLOG_CONTAIN_CSN) == XLOG_CONTAIN_CSN;
    XlUHeapUpdate *xlrec = (XlUHeapUpdate *)XLogRecGetData(record);
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapUpdate + (hasCSN ? sizeof(CommitSeqNo) : 0));
    UndoRecPtr urecptr = xlundohdr->urecptr;
    char *curxlogptr = ((char *)xlundohdr) + SizeOfXLUndoHeader;
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_SUB_XACT) != 0) {
        hasSubXact = (bool *) ((char *)curxlogptr);
        curxlogptr += sizeof(bool);
        readSize += sizeof(bool);
    } else {
        hasSubXact = &defaultHasSubXact;
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        blkprev = (UndoRecPtr *) ((char *)curxlogptr);
        curxlogptr += sizeof(UndoRecPtr);
        readSize += sizeof(UndoRecPtr);
    } else {
        blkprev = &invalidUrp;
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        prevurp = (UndoRecPtr *) ((char *)curxlogptr);
        curxlogptr += sizeof(UndoRecPtr);
        readSize += sizeof(UndoRecPtr);
    } else {
        prevurp = &invalidUrp;
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        partitionOid = (Oid *) ((char *)curxlogptr);
        curxlogptr += sizeof(Oid);
        readSize += sizeof(Oid);
    } else {
        partitionOid = &invalidPartitionOid;
    }

    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_CURRENT_XID) != 0) {
        /* ignore current xid for replay */
        curxlogptr += sizeof(TransactionId);
        readSize += sizeof(TransactionId);
    }

    bool allReplay = !AmPageRedoWorker() || !SUPPORT_USTORE_UNDO_WORKER;
    bool onlyReplayUndo = allReplay ? false : parallel_recovery::DoPageRedoWorkerReplayUndo();

    if (xlrec->flags & XLZ_NON_INPLACE_UPDATE) {
        *xlnewundohdr = (XlUndoHeader *)curxlogptr;
        curxlogptr += SizeOfXLUndoHeader;
        inplaceUpdate = false;
        if (((*xlnewundohdr)->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
            newblkprev = (UndoRecPtr *) ((char *)curxlogptr);
            curxlogptr += sizeof(UndoRecPtr);
            readSize += sizeof(UndoRecPtr);
        } else {
            newblkprev =  &invalidUrp;
        }
        if (((*xlnewundohdr)->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
            newprevurp = (UndoRecPtr *) ((char *)curxlogptr);
            curxlogptr += sizeof(UndoRecPtr);
            readSize += sizeof(UndoRecPtr);
        } else {
            newprevurp = &invalidUrp;
        }
        if (((*xlnewundohdr)->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
            newpartitionOid = (Oid *) ((char *)curxlogptr);
            curxlogptr += sizeof(Oid);
            readSize += sizeof(Oid);
        } else {
            newpartitionOid = &invalidPartitionOid;
        }
    }

    undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)curxlogptr;
    CopyUndoMeta(*xlundometa, undometa);
    uint32 undoMetaSize = undometa.Size();
    curxlogptr += undoMetaSize;
    *xidBasePos = curxlogptr;

    if (inplaceUpdate) {
        int *undoXorDeltaSizePtr = (int *)curxlogptr;
        undoXorDeltaSize = *undoXorDeltaSizePtr;
        curxlogptr += sizeof(int);
        xlogXorDelta = curxlogptr;
    } else {
        Size initPageXtraInfo = 0;

        if (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_PAGE) {
            /* has xidBase */
            initPageXtraInfo = sizeof(TransactionId) + sizeof(uint16);
            curxlogptr += initPageXtraInfo;
        }

        char *data = (char *)curxlogptr;
        Size datalen = recordlen - SizeOfUHeapHeader - SizeOfXLUndoHeader - SizeOfUHeapUpdate -
            undoMetaSize - SizeOfXLUndoHeader - initPageXtraInfo - readSize - (hasCSN ? sizeof(CommitSeqNo) : 0);

        oldtup->disk_tuple = GetUHeapDiskTupleFromRedoData(data, &datalen, tbuf, false);
        oldtup->disk_tuple_size = datalen;
    }

    BlockNumber oldblk = InvalidBlockNumber;
    BlockNumber newblk = InvalidBlockNumber;
    RelFileNode rnode;
    XLogRecGetBlockTag(record, 0, &rnode, NULL, &newblk);
    if (XLogRecGetBlockTag(record, 1, NULL, NULL, &oldblk)) {
        Assert(!inplaceUpdate);
    } else {
        oldblk = newblk;
    }

    ItemPointerData oldtid, newtid;
    ItemPointerSet(&oldtid, oldblk, xlrec->old_offnum);
    ItemPointerSet(&newtid, newblk, xlrec->new_offnum);

    /* Construct old tuple for undo record */
    oldtup->table_oid = xlundohdr->relOid;
    oldtup->ctid = oldtid;

    if (allReplay || onlyReplayUndo) {
        bool skipInsert = IsSkipInsertUndo(urecptr);
        if (skipInsert) {
            undometa.SetInfo(XLOG_UNDOMETA_INFO_SKIP);
        }
        TD oldTD;
        oldTD.xactid = xlrec->oldxid;
        oldTD.undo_record_ptr = INVALID_UNDO_REC_PTR;
        UndoRecord *oldundorec = u_sess->ustore_cxt.undo_records[0];
        oldundorec->SetUrp(urecptr);
        UndoRecord *newundorec = NULL;
        if (!inplaceUpdate) {
            newundorec = u_sess->ustore_cxt.undo_records[1];
            newundorec->SetUrp((*xlnewundohdr)->urecptr);
        }
        /* We need to pass in tablespace and relfilenode in PrepareUndo but we never explicitly
         * wrote those information in the xlundohdr because we can grab them from the XLOG record itself.
         * Here we grab the first block tag. If the record has more than one blocks, then their tags
         * should be the same.
         */
        RelFileNode targetNode = { 0 };
        bool res PG_USED_FOR_ASSERTS_ONLY = XLogRecGetBlockTag(record, 0, &targetNode, NULL, NULL);
        Assert(res == true);
        urecptr = UHeapPrepareUndoUpdate(xlundohdr->relOid, *partitionOid,
            targetNode.relNode, targetNode.spcNode,
            UNDO_PERMANENT, InvalidBuffer, InvalidBuffer, xlrec->old_offnum, xid,
            *hasSubXact ? TopSubTransactionId : InvalidSubTransactionId, 0, *blkprev,
            inplaceUpdate ? *blkprev : *newblkprev, *prevurp, &oldTD, oldtup,
            inplaceUpdate, &newUrecptr, undoXorDeltaSize, oldblk, newblk, xlundohdr, &undometa);
        Assert(urecptr == xlundohdr->urecptr);

        if (!skipInsert) {
            if (!inplaceUpdate) {
                newundorec->SetOffset(xlrec->new_offnum);
                appendBinaryStringInfo(oldundorec->Rawdata(), (char *)&newtid, sizeof(ItemPointerData));
            } else {
                UndoRecord *undorec = (*u_sess->ustore_cxt.urecvec)[0];
                appendBinaryStringInfo(undorec->Rawdata(), xlogXorDelta, undoXorDeltaSize);
            }

            if (*hasSubXact) {
                SubTransactionId subxid = TopSubTransactionId;

                oldundorec->SetUinfo(UNDO_UREC_INFO_CONTAINS_SUBXACT);
                appendBinaryStringInfo(oldundorec->Rawdata(), (char *)&subxid, sizeof(SubTransactionId));
            }

            /* Insert the Undo record into the undo store */
            InsertPreparedUndo(u_sess->ustore_cxt.urecvec, lsn);
        }

        undo::RedoUndoMeta(record, &undometa, xlundohdr->urecptr, u_sess->ustore_cxt.urecvec->LastRecord(), 
            u_sess->ustore_cxt.urecvec->LastRecordSize());
        UHeapResetPreparedUndo(UNDO_PERMANENT, UNDO_PTR_GET_ZONE_ID(urecptr));
    }

    return urecptr;
}

static void PerformUpdateOldRedoAction(XLogReaderState *record, UHeapTupleData *oldtup, 
    UpdateRedoBuffers *buffers, const UndoRecPtr urecptr, const bool sameBlock)
{
    XLogRecPtr lsn = record->EndRecPtr;
    TransactionId xid = XLogRecGetXid(record);
    XlUHeapUpdate *xlrec = (XlUHeapUpdate *)XLogRecGetData(record);
    Buffer oldbuf = buffers->oldbuffer.buf;
    Page oldpage = buffers->oldbuffer.pageinfo.page;
    RowPtr *rp = NULL;

    if (UHeapPageGetMaxOffsetNumber(oldpage) >= xlrec->old_offnum) {
        rp = UPageGetRowPtr(oldpage, xlrec->old_offnum);
    } else {
        elog(PANIC, "invalid rp");
    }

    /* Ensure old tuple points to the tuple in page. */
    oldtup->disk_tuple = (UHeapDiskTuple)UPageGetRowData(oldpage, rp);
    oldtup->disk_tuple_size = RowPtrGetLen(rp);
    oldtup->disk_tuple->flag = xlrec->old_tuple_flag;

    UHeapTupleHeaderSetTDSlot(oldtup->disk_tuple, xlrec->old_tuple_td_id);

    if (!sameBlock) {
        UHeapPageSetUndo(oldbuf, xlrec->old_tuple_td_id, xid, urecptr);
    }

    /* Mark the page as a candidate for pruning  and update the page potential freespace */
    if (xlrec->flags & XLZ_NON_INPLACE_UPDATE) {
        UPageSetPrunable(oldpage, xid);
        if (buffers->newbuffer.buf != oldbuf) {
            UHeapRecordPotentialFreeSpace(oldbuf, SHORTALIGN(oldtup->disk_tuple_size));
        }
    } else if (xlrec->flags & XLZ_BLOCK_INPLACE_UPDATE) {
        UPageSetPrunable(oldpage, xid);
        UHeapRecordPotentialFreeSpace(oldbuf, SHORTALIGN(oldtup->disk_tuple_size));
    }

    PageSetLSN(oldpage, lsn);
    MarkBufferDirty(oldbuf);
}

static XLogRedoAction GetUpdateNewRedoAction(XLogReaderState *record, UpdateRedoBuffers *buffers, 
    const XLogRedoAction oldaction, const TransactionId *xidBase, const uint16 *tdCount, const bool sameBlock)
{
    XLogRedoAction newaction;

    if (sameBlock) {
        buffers->newbuffer.buf = buffers->oldbuffer.buf;
        buffers->newbuffer.pageinfo.page = buffers->oldbuffer.pageinfo.page;
        newaction = oldaction;
    } else if (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_PAGE) {
        XLogInitBufferForRedo(record, 0, &buffers->newbuffer);
        Page newpage = buffers->newbuffer.pageinfo.page;

        if (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_TOAST_PAGE) {
            UPageInit<UPAGE_TOAST>(newpage, buffers->newbuffer.pageinfo.pagesize, UHEAP_SPECIAL_SIZE);
        } else {
            UPageInit<UPAGE_HEAP>(newpage, buffers->newbuffer.pageinfo.pagesize, UHEAP_SPECIAL_SIZE, *tdCount);
        }

        UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)newpage;
        uheappage->pd_xid_base = *xidBase;
        uheappage->pd_multi_base = 0;
        newaction = BLK_NEEDS_REDO;
    } else {
        newaction = XLogReadBufferForRedo(record, 0, &buffers->newbuffer);
    }

    return newaction;
}

static uint32 GetUHeapDiskTupleFromUpdateNewRedoData(XLogReaderState *record, UpdateRedoTuples *tuples,
    UpdateRedoAffixLens *affixLens, TupleBuffer &tbuf, const bool sameBlock)
{
    Size datalen;
    errno_t rc;
    XlUHeapHeader xlhdr;
    XlUHeapUpdate *xlrec = (XlUHeapUpdate *)XLogRecGetData(record);

    char *recdata = XLogRecGetBlockData(record, 0, &datalen);
    char *recdataEnd = recdata + datalen;

    if (xlrec->flags & XLZ_NON_INPLACE_UPDATE) {
        if (xlrec->flags & XLZ_UPDATE_PREFIX_FROM_OLD) {
            Assert(sameBlock);
            rc = memcpy_s(&affixLens->prefixlen, sizeof(uint16), recdata, sizeof(uint16));
            securec_check(rc, "\0", "\0");
            recdata += sizeof(uint16);
        }

        if (xlrec->flags & XLZ_UPDATE_SUFFIX_FROM_OLD) {
            Assert(sameBlock);
            rc = memcpy_s(&affixLens->suffixlen, sizeof(uint16), recdata, sizeof(uint16));
            securec_check(rc, "\0", "\0");
            recdata += sizeof(uint16);
        }
    }

    rc = memcpy_s((char *)&xlhdr, SizeOfUHeapHeader, recdata, SizeOfUHeapHeader);
    securec_check(rc, "\0", "\0");
    recdata += SizeOfUHeapHeader;

    Size tuplen = recdataEnd - recdata;
    Assert(tuplen <= MaxPossibleUHeapTupleSize);

    tuples->newtup = &tbuf.hdr;
    rc = memset_s((char *)tuples->newtup, SizeOfUHeapDiskTupleData, 0, SizeOfUHeapDiskTupleData);
    securec_check(rc, "\0", "\0");

    /*
     * Reconstruct the new tuple using the prefix and/or suffix from the
     * old tuple, and the data stored in the WAL record.
     */
    char *newp = (char *)tuples->newtup + SizeOfUHeapDiskTupleData;
    if (affixLens->prefixlen > 0) {
        int len;

        /* copy bitmap [+ padding] [+ oid] from WAL record */
        len = xlhdr.t_hoff - SizeOfUHeapDiskTupleData;
        if (len > 0) {
            rc = memcpy_s(newp, tuplen, recdata, len);
            securec_check(rc, "\0", "\0");
            recdata += len;
            newp += len;
        }

        /* copy prefix from old tuple */
        rc = memcpy_s(newp, affixLens->prefixlen, (char *)tuples->oldtup->disk_tuple + 
            tuples->oldtup->disk_tuple->t_hoff, affixLens->prefixlen);
        securec_check(rc, "\0", "\0");
        newp += affixLens->prefixlen;

        /* copy new tuple data from WAL record */
        len = tuplen - (xlhdr.t_hoff - SizeOfUHeapDiskTupleData);
        if (len > 0) {
            rc = memcpy_s(newp, tuplen, recdata, len);
            securec_check(rc, "\0", "\0");
            recdata += len;
            newp += len;
        }
    } else {
        /*
         * copy bitmap [+ padding] [+ oid] + data from record, all in one
         * go
         */
        rc = memcpy_s(newp, tuplen, recdata, tuplen);
        securec_check(rc, "\0", "\0");
        recdata += tuplen;
        newp += tuplen;
    }

    Assert(recdata == recdataEnd);

    /* copy suffix from old tuple */
    if (affixLens->suffixlen > 0) {
        rc = memcpy_s(newp, affixLens->suffixlen,
            (char *)tuples->oldtup->disk_tuple + tuples->oldtup->disk_tuple_size - affixLens->suffixlen,
            affixLens->suffixlen);
        securec_check(rc, "\0", "\0");
    }

    uint32 newlen = SizeOfUHeapDiskTupleData + tuplen + affixLens->prefixlen + affixLens->suffixlen;
    UHeapTupleHeaderSetTDSlot(tuples->newtup, xlhdr.td_id);
    UHeapTupleHeaderSetLockerTDSlot(tuples->newtup, xlhdr.locker_td_id);
    tuples->newtup->xid = (ShortTransactionId)FrozenTransactionId;
    tuples->newtup->flag2 = xlhdr.flag2;
    tuples->newtup->flag = xlhdr.flag;
    tuples->newtup->t_hoff = xlhdr.t_hoff;

    return newlen;
}

static Size PerformUpdateNewRedoAction(XLogReaderState *record, UpdateRedoBuffers *buffers, 
    UpdateRedoTuples *tuples, const uint32 newlen, XlUndoHeader *xlnewundohdr, 
    const UndoRecPtr urecptr, const bool sameBlock)
{
    XLogRecPtr lsn = record->EndRecPtr;
    TransactionId xid = XLogRecGetXid(record);
    XlUHeapUpdate *xlrec = (XlUHeapUpdate *)XLogRecGetData(record);

    Buffer oldbuf = buffers->oldbuffer.buf;
    Buffer newbuf = buffers->newbuffer.buf;
    Page oldpage = buffers->oldbuffer.pageinfo.page;
    Page newpage = buffers->newbuffer.pageinfo.page;

    /* max offset number should be valid */
    Assert(UHeapPageGetMaxOffsetNumber(newpage) + 1 >= xlrec->new_offnum);

    if (xlrec->flags & XLZ_NON_INPLACE_UPDATE) {
        UHeapBufferPage bufpage = {newbuf, NULL};

        if (UPageAddItem(NULL, &bufpage, (Item)tuples->newtup, newlen, xlrec->new_offnum, true) ==
            InvalidOffsetNumber) {
            elog(PANIC, "failed to add tuple");
        }

        /* Update the page potential freespace */
        if (newbuf != oldbuf) {
            UHeapRecordPotentialFreeSpace(newbuf, -1 * SHORTALIGN(newlen));
        } else {
            int delta = newlen - tuples->oldtup->disk_tuple_size;
            UHeapRecordPotentialFreeSpace(newbuf, -1 * SHORTALIGN(delta));
        }

        if (sameBlock) {
            UHeapPageSetUndo(newbuf, xlrec->old_tuple_td_id, xid, xlnewundohdr->urecptr);
        } else {
            UHeapPageSetUndo(newbuf, tuples->newtup->td_id, xid, xlnewundohdr->urecptr);
        }
    } else if (xlrec->flags & XLZ_BLOCK_INPLACE_UPDATE) {
        RowPtr *rp = UPageGetRowPtr(oldpage, xlrec->old_offnum);
        PutBlockInplaceUpdateTuple(oldpage, (Item)tuples->newtup, rp, newlen);
        /* update the potential freespace */
        UHeapRecordPotentialFreeSpace(newbuf, newlen);
        Assert(oldpage == newpage);
        UPageSetPrunable(newpage, XLogRecGetXid(record));
        UHeapPageSetUndo(newbuf, xlrec->old_tuple_td_id, xid, urecptr);
    } else {
        /*
         * For inplace updates, we copy the entire data portion including
         * the tuple header.
         */
        RowPtr *rp = UPageGetRowPtr(oldpage, xlrec->old_offnum);
        RowPtrChangeLen(rp, newlen);
        errno_t rc = memcpy_s((char *)tuples->oldtup->disk_tuple, newlen, (char *)tuples->newtup, newlen);
        securec_check(rc, "\0", "\0");

        if (newlen < tuples->oldtup->disk_tuple_size) {
            /* new tuple is smaller, a prunable candidate */
            Assert(oldpage == newpage);
            UPageSetPrunable(newpage, XLogRecGetXid(record));
        }

        UHeapPageSetUndo(newbuf, xlrec->old_tuple_td_id, xid, urecptr);
    }

    Size freespace = PageGetUHeapFreeSpace(newpage); /* needed to update FSM
                                                 * below */

    PageSetLSN(newpage, lsn);
    MarkBufferDirty(newbuf);

    return freespace;
}

static void UHeapXlogUpdate(XLogReaderState *record)
{
    XlUndoHeader *xlnewundohdr = NULL;
    UpdateRedoBuffers buffers;
    RelFileNode rnode = {0};
    BlockNumber oldblk = InvalidBlockNumber;
    BlockNumber newblk = InvalidBlockNumber;
    UHeapTupleData oldtup;
    UpdateRedoTuples tuples;
    XLogRedoAction oldaction, newaction;
    TupleBuffer tbuf;
    UpdateRedoAffixLens affixLens = {0, 0};
    uint32 newlen = 0;
    Size freespace = 0;
    bool sameBlock = false;

    bool allReplay = !AmPageRedoWorker() || !SUPPORT_USTORE_UNDO_WORKER;
    bool onlyReplayUndo = allReplay ? false : parallel_recovery::DoPageRedoWorkerReplayUndo();

    char *xidBasePos = NULL;
    TransactionId *xidBase = NULL;
    uint16 *tdCount = NULL;
    XlUHeapUpdate *xlrec = (XlUHeapUpdate *)XLogRecGetData(record);
    bool inplaceUpdate = !(xlrec->flags & XLZ_NON_INPLACE_UPDATE);

    WHITEBOX_TEST_STUB(UHEAP_XLOG_UPDATE_FAILED, WhiteboxDefaultErrorEmit);

    XLogRecGetBlockTag(record, 0, &rnode, NULL, &newblk);
    if (XLogRecGetBlockTag(record, 1, NULL, NULL, &oldblk)) {
        Assert(!inplaceUpdate);
    } else {
        oldblk = newblk;
        sameBlock = true;
    }

    UndoRecPtr urecptr = PrepareAndInsertUndoRecordForUpdateRedo(record, &oldtup, &xlnewundohdr, &xidBasePos, tbuf);
    GetXidbaseAndAffixLensFromUpdateRedo(xidBasePos, record, &xidBase, &tdCount, &affixLens);
    tuples.oldtup = &oldtup;

    if (allReplay || !onlyReplayUndo) {
        /* Read old page */
        oldaction = XLogReadBufferForRedo(record, sameBlock ? 0 : 1, &buffers.oldbuffer);

        /* read new page */
        newaction = GetUpdateNewRedoAction(record, &buffers, oldaction, xidBase, tdCount, sameBlock);

        /* recover old tuple on data page */
        if (oldaction == BLK_NEEDS_REDO) {
            PerformUpdateOldRedoAction(record, &oldtup, &buffers, urecptr, sameBlock);
        }

        /*
         * recover new tuple on data page
         * First, construct new tuple from xlog record
         */
        if (newaction == BLK_NEEDS_REDO) {
            newlen = GetUHeapDiskTupleFromUpdateNewRedoData(record, &tuples, &affixLens, tbuf, sameBlock);

            freespace = PerformUpdateNewRedoAction(record, &buffers, &tuples, newlen, xlnewundohdr, urecptr, sameBlock);
        }

        if (BufferIsValid(buffers.newbuffer.buf) && buffers.newbuffer.buf != buffers.oldbuffer.buf) {
            UnlockReleaseBuffer(buffers.newbuffer.buf);
        }

        if (BufferIsValid(buffers.oldbuffer.buf)) {
            UnlockReleaseBuffer(buffers.oldbuffer.buf);
        }

        /* may should free space */
        if (newaction == BLK_NEEDS_REDO && !inplaceUpdate && freespace < BLCKSZ / FREESPACE_FRACTION) {
            XLogRecordPageWithFreeSpace(rnode, newblk, freespace);
        }
    }
}

static int GetOffsetRangesForMultiInsert(UHeapFreeOffsetRanges **ufreeOffsetRanges, char* data, UndoRecPtr **urpvec)
{
    /* allocate the information related to offset ranges */
    char *rangesData = data;

    int nranges = *(int *)rangesData;
    rangesData += sizeof(int);
    *urpvec = (UndoRecPtr *)rangesData;
    rangesData += nranges * sizeof(UndoRecPtr);

    *ufreeOffsetRanges = (UHeapFreeOffsetRanges *)palloc0(sizeof(UHeapFreeOffsetRanges));
    Assert(nranges > 0);
    errno_t rc = memcpy_s(&(*ufreeOffsetRanges)->startOffset[0], sizeof(OffsetNumber) * nranges, (char *)rangesData,
        sizeof(OffsetNumber) * nranges);
    securec_check(rc, "\0", "\0");
    rangesData += sizeof(OffsetNumber) * nranges;
    rc = memcpy_s(&(*ufreeOffsetRanges)->endOffset[0], sizeof(OffsetNumber) * nranges, (char *)rangesData,
        sizeof(OffsetNumber) * nranges);
    securec_check(rc, "\0", "\0");
    rangesData += sizeof(OffsetNumber) * nranges;

    return nranges;
}

static UndoRecPtr PrepareAndInsertUndoRecordForMultiInsertRedo(XLogReaderState *record, const BlockNumber blkno, 
    XlUHeapMultiInsert **xlrec, UHeapFreeOffsetRanges **ufreeOffsetRanges, TransactionId **xidBase,
    uint16 **tdCount)
{
    URecVector *urecvec = NULL;
    UndoRecPtr *urpvec = NULL;
    undo::XlogUndoMeta undometa;
    UndoRecPtr *blkprev;
    UndoRecPtr *prevurp;
    Oid *partitionOid;
    UndoRecPtr invalidUrp = INVALID_UNDO_REC_PTR;
    Oid invalidPartitionOid = 0;

    XLogRecPtr lsn = record->EndRecPtr;
    TransactionId xid = XLogRecGetXid(record);
    bool isinit = (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_PAGE) != 0;
    bool allReplay = !AmPageRedoWorker() || !SUPPORT_USTORE_UNDO_WORKER;
    bool onlyReplayUndo = allReplay ? false : parallel_recovery::DoPageRedoWorkerReplayUndo();

    XlUndoHeader *xlundohdr = (XlUndoHeader *)XLogRecGetData(record);
    char *curxlogptr = (char *)xlundohdr + SizeOfXLUndoHeader;
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        blkprev = (UndoRecPtr *) ((char *)curxlogptr);
        Assert(*blkprev != INVALID_UNDO_REC_PTR);
        curxlogptr += sizeof(UndoRecPtr);
    } else {
        blkprev = &invalidUrp;
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        prevurp = (UndoRecPtr *) ((char *)curxlogptr);
        curxlogptr += sizeof(UndoRecPtr);
    } else {
        prevurp = &invalidUrp;
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        partitionOid = (Oid *) ((char *)curxlogptr);
        curxlogptr += sizeof(Oid);
    } else {
        partitionOid = &invalidPartitionOid;
    }

    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_CURRENT_XID) != 0) {
        /* ignore current xid for replay */
        curxlogptr += sizeof(TransactionId);
    }

    UndoRecPtr *last_urecptr = (UndoRecPtr *)curxlogptr;
    UndoRecPtr urecptr = *last_urecptr;
    curxlogptr = (char *)last_urecptr + sizeof(*last_urecptr);
    undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)curxlogptr;

    /* copy xlundometa to local struct */
    CopyUndoMeta(*xlundometa, undometa);
    uint32 undoMetaSize = undometa.Size();
    curxlogptr += undoMetaSize;

    if (isinit) {
        *xidBase = (TransactionId *)curxlogptr;
        curxlogptr += sizeof(TransactionId);
        *tdCount = (uint16 *)curxlogptr;
        curxlogptr += sizeof(uint16);
    }

    bool hasCSN = (record->decoded_record->xl_term & XLOG_CONTAIN_CSN) == XLOG_CONTAIN_CSN;
    curxlogptr = curxlogptr + (hasCSN ? sizeof(CommitSeqNo) : 0);
    (*xlrec) = (XlUHeapMultiInsert *)curxlogptr;
    curxlogptr = (char *)*xlrec + SizeOfUHeapMultiInsert;

    /* fetch number of distinct ranges */
    int nranges = GetOffsetRangesForMultiInsert(ufreeOffsetRanges, curxlogptr, &urpvec);

    if (allReplay || onlyReplayUndo) {
        bool skipInsert = IsSkipInsertUndo(urecptr);
        if (skipInsert) {
            undometa.SetInfo(XLOG_UNDOMETA_INFO_SKIP);
        }
        bool skipUndo = ((*xlrec)->flags & XLZ_INSERT_IS_FROZEN);
        
        /* We need to pass in tablespace and relfilenode in PrepareUndo but we never explicitly
         * wrote those information in the xlundohdr because we can grab them from the XLOG record itself.
         */
        RelFileNode targetNode = { 0 };
        bool res PG_USED_FOR_ASSERTS_ONLY = XLogRecGetBlockTag(record, 0, &targetNode, NULL, NULL);
        Assert(res == true);

        /* pass first undo record in and let UHeapPrepareUndoMultiInsert set urecptr according to xlog */
        urecptr = UHeapPrepareUndoMultiInsert(xlundohdr->relOid, *partitionOid, targetNode.relNode,
            targetNode.spcNode, UNDO_PERMANENT, InvalidBuffer, nranges, xid, InvalidCommandId, *blkprev,
            *prevurp, &urecvec, NULL, urpvec, blkno, xlundohdr, &undometa);
        /*
         * We can skip inserting undo records if the tuples are to be marked as
         * frozen.
         */
        elog(LOG, "Undo record prepared: %d for Block Number: %d", nranges, blkno);
        if (!skipUndo && !skipInsert) {
            for (int i = 0; i < nranges; i++) {
                initStringInfo((*urecvec)[i]->Rawdata());
                appendBinaryStringInfo((*urecvec)[i]->Rawdata(), (char *)&(*ufreeOffsetRanges)->startOffset[i],
                    sizeof(OffsetNumber));
                appendBinaryStringInfo((*urecvec)[i]->Rawdata(), (char *)&(*ufreeOffsetRanges)->endOffset[i],
                    sizeof(OffsetNumber));
            }

            /*
             * undo should be inserted at same location as it was during the
             * actual insert (DO operation).
             */
            Assert((*urecvec)[0]->Urp() == xlundohdr->urecptr);
            InsertPreparedUndo(urecvec, lsn);
        }

        undo::RedoUndoMeta(record, &undometa, xlundohdr->urecptr, urecvec->LastRecord(), urecvec->LastRecordSize());
        UHeapResetPreparedUndo(UNDO_PERMANENT, UNDO_PTR_GET_ZONE_ID(urecptr));
        urecvec->Destroy();
    }

    return urecptr;
}

static XLogRedoAction GetMultiInsertRedoAction(XLogReaderState *record, RedoBufferInfo *buffer, TransactionId *xidBase,
                                               uint16 *tdCount)
{
    XLogRedoAction action;
    bool isinit = (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_PAGE) != 0;
    if (isinit) {
        XLogInitBufferForRedo(record, 0, buffer);
        Page page = buffer->pageinfo.page;
        if (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_TOAST_PAGE) {
            UPageInit<UPAGE_TOAST>(page, buffer->pageinfo.pagesize, UHEAP_SPECIAL_SIZE);
        } else {
            UPageInit<UPAGE_HEAP>(page, buffer->pageinfo.pagesize, UHEAP_SPECIAL_SIZE, *tdCount);
        }
        UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
        uheappage->pd_xid_base = *xidBase;
        uheappage->pd_multi_base = *xidBase;
        action = BLK_NEEDS_REDO;
    } else {
        action = XLogReadBufferForRedo(record, 0, buffer);
    }

    return action;
}

static UHeapDiskTuple GetUHeapDiskTupleFromMultiInsertRedoData(char **data, int *datalen, TupleBuffer &tbuf)
{
    UHeapDiskTuple disktup;
    XlMultiInsertUTuple *xlhdr;
    errno_t rc;

    xlhdr = (XlMultiInsertUTuple *)(*data);
    *data = ((char *)xlhdr) + SizeOfMultiInsertUTuple;

    *datalen = xlhdr->datalen;
    Assert(*datalen <= (int)MaxPossibleUHeapTupleSize);
    disktup = &tbuf.hdr;

    rc = memset_s((char *)disktup, SizeOfUHeapDiskTupleData, 0, SizeOfUHeapDiskTupleData);
    securec_check_c(rc, "\0", "\0");
    /* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
    rc = memcpy_s((char *)disktup + SizeOfUHeapDiskTupleData, *datalen, (char *)*data, *datalen);
    securec_check_c(rc, "\0", "\0");
    *data += *datalen;

    *datalen += SizeOfUHeapDiskTupleData;
    // the same as UHeapXlogInsert
    UHeapTupleHeaderSetTDSlot(disktup, xlhdr->td_id);
    UHeapTupleHeaderSetLockerTDSlot(disktup, InvalidTDSlotId);
    disktup->flag2 = xlhdr->flag2;
    disktup->flag = xlhdr->flag;
    disktup->t_hoff = xlhdr->t_hoff;

    return disktup;
}

static void PerformMultiInsertRedoAction(XLogReaderState *record, XlUHeapMultiInsert *xlrec,
    RedoBufferInfo *buffer, const UndoRecPtr urecptr, UHeapFreeOffsetRanges *ufreeOffsetRanges)
{
    TupleBuffer tbuf;
    int tdSlot = 0;
    int newlen;
    Size len;
    bool firstTime = true;
    int prevTdSlot = -1;
    bool skipUndo = (xlrec->flags & XLZ_INSERT_IS_FROZEN);
    bool isinit = (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_PAGE) != 0;
    TransactionId xid = XLogRecGetXid(record);
    Page page = buffer->pageinfo.page;
    UHeapBufferPage bufpage;
    OffsetNumber offnum = ufreeOffsetRanges->startOffset[0];

    /* Tuples are stored as block data */
    char *tupdata = XLogRecGetBlockData(record, 0, &len);
    char *endptr = tupdata + len;

    for (int i = 0, j = 0; i < xlrec->ntuples; i++, offnum++) {
        /*
         * If we're reinitializing the page, the tuples are stored in
         * order from FirstOffsetNumber. Otherwise there's an array of
         * offsets in the WAL record, and the tuples come after that.
         */
        if (isinit) {
            offnum = FirstOffsetNumber + i;
        } else {
            /*
             * Change the offset range if we've reached the end of current
             * range.
             */
            if (offnum > ufreeOffsetRanges->endOffset[j]) {
                j++;
                offnum = ufreeOffsetRanges->startOffset[j];
            }
        }

        /* max offset should be valid */
        Assert(UHeapPageGetMaxOffsetNumber(page) + 1 >= offnum);

        UHeapDiskTuple uhtup = GetUHeapDiskTupleFromMultiInsertRedoData(&tupdata, &newlen, tbuf);

        bufpage.buffer = buffer->buf;
        bufpage.page = NULL;
        if (UPageAddItem(NULL, &bufpage, (Item)uhtup, newlen, offnum, true) == InvalidOffsetNumber) {
            elog(PANIC, "failed to add tuple");
        }

        /* decrement the potential freespace of this page */
        UHeapRecordPotentialFreeSpace(buffer->buf, SHORTALIGN(newlen));

        if (!skipUndo) {
            tdSlot = UHeapTupleHeaderGetTDSlot(uhtup);

            if (firstTime) {
                prevTdSlot = tdSlot;
                firstTime = false;
            } else {
                /* All the tuples must refer to same transaction slot. */
                Assert(prevTdSlot == tdSlot);
                prevTdSlot = tdSlot;
            }
        }
    }

    if (!skipUndo) {
        UHeapPageSetUndo(buffer->buf, tdSlot, xid, urecptr);
    }
    PageSetLSN(page, record->EndRecPtr);
    MarkBufferDirty(buffer->buf);

    if (tupdata != endptr) {
        elog(PANIC, "total tuple length mismatch");
    }
}

static void UHeapXlogMultiInsert(XLogReaderState *record)
{
    RelFileNode rnode;
    BlockNumber blkno = InvalidBlockNumber;
    RedoBufferInfo buffer;
    XlUHeapMultiInsert *xlrec = NULL;
    XLogRedoAction action = BLK_NOTFOUND;
    UHeapFreeOffsetRanges *ufreeOffsetRanges = NULL;
    TransactionId *xidBase = NULL;
    uint16 *tdCount = NULL;

    bool allReplay = !AmPageRedoWorker() || !SUPPORT_USTORE_UNDO_WORKER;
    bool onlyReplayUndo = allReplay ? false : parallel_recovery::DoPageRedoWorkerReplayUndo();

    WHITEBOX_TEST_STUB(UHEAP_XLOG_MULTI_INSERT_FAILED, WhiteboxDefaultErrorEmit);

    XLogRecGetBlockTag(record, 0, &rnode, NULL, &blkno);

    UndoRecPtr urecptr = PrepareAndInsertUndoRecordForMultiInsertRedo(record, blkno, &xlrec,
        &ufreeOffsetRanges, &xidBase, &tdCount);

    if (allReplay || !onlyReplayUndo) {
        action = GetMultiInsertRedoAction(record, &buffer, xidBase, tdCount);
    }

    /* Apply the wal for data */
    if (action == BLK_NEEDS_REDO) {
        PerformMultiInsertRedoAction(record, xlrec, &buffer, urecptr, ufreeOffsetRanges);
    }

    pfree(ufreeOffsetRanges);

    if (allReplay || !onlyReplayUndo) {
        if (BufferIsValid(buffer.buf)) {
            UnlockReleaseBuffer(buffer.buf);
        }
    }
}

static void UHeapXlogBaseShift(XLogReaderState *record)
{
    RedoBufferInfo buffer;
    XLogRecPtr lsn = record->EndRecPtr;

    if (XLogReadBufferForRedo(record, HEAP_BASESHIFT_ORIG_BLOCK_NUM, &buffer) == BLK_NEEDS_REDO) {
        char *maindata = XLogRecGetData(record);

        XlUHeapBaseShift *xlrec = (XlUHeapBaseShift *)maindata;
        Page page = buffer.pageinfo.page;

        UHeapPageShiftBase(InvalidBuffer, page, xlrec->multi, xlrec->delta);

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer.buf);
    }

    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

static void UHeapXlogExtendTDSlot(XLogReaderState *record)
{
    RedoBufferInfo buffer;
    XLogRecPtr lsn = record->EndRecPtr;
    Page   page;
    XLogRedoAction action = (XLogRedoAction)-1;
    errno_t ret = EOK;
    XlUHeapExtendTdSlots *xlrec = NULL;

    xlrec = (XlUHeapExtendTdSlots *)XLogRecGetData(record);
    action = XLogReadBufferForRedo(record, 0, &buffer);
    page = BufferGetPage(buffer.buf);

    if (action == BLK_NEEDS_REDO) {
        int i;
        uint8 extendedSlots;
        uint8 prevSlots;
        size_t linePtrSize;
        char *start;
        char *end;
        TD *thistrans;
        UHeapPageTDData *tdPtr;
        UHeapPageHeaderData *phdr = (UHeapPageHeaderData *)page;

        prevSlots = xlrec->nPrevSlots;
        extendedSlots = xlrec->nExtended;
        tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
        Assert(prevSlots == phdr->td_count);

        /*
         * Move the line pointers ahead in the page to make room for
         * added transaction slots.
         */
        start = page + SizeOfUHeapPageHeaderData + (prevSlots * sizeof(TD));
        end = page + phdr->pd_lower;
        linePtrSize = end - start;

        ret = memmove_s((char *)start + (extendedSlots * sizeof(TD)), linePtrSize, start, linePtrSize);
        securec_check(ret, "\0", "\0");

        /*
         * Initialize the new transaction slots
         */
        for (i = prevSlots; i < prevSlots + extendedSlots; i++) {
            thistrans = &tdPtr->td_info[i];
            thistrans->xactid = InvalidTransactionId;
            thistrans->undo_record_ptr = INVALID_UNDO_REC_PTR;
        }

        /*
         * Re-initialize number of txn slots and begining
         * of free space in the header.
        */
        phdr->td_count = prevSlots + extendedSlots;
        phdr->pd_lower += extendedSlots * sizeof(TD);

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer.buf);
    }

    if (BufferIsValid(buffer.buf))
        UnlockReleaseBuffer(buffer.buf);
}

#ifdef ENABLE_MULTIPLE_NODES
const static bool SUPPORT_HOT_STANDBY = false; /* don't support consistency view */
#else
const static bool SUPPORT_HOT_STANDBY = true;
#endif

static void UHeapXlogFreeze(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    XlUHeapFreeze *xlrec = (XlUHeapFreeze *)XLogRecGetData(record);
    TransactionId cutoffXid = xlrec->cutoff_xid;
    RedoBufferInfo buffer;
    UHeapTupleData utuple;

    /*
     * In Hot Standby mode, ensure that there's no queries running which still
     * consider the frozen xids as running.
     */
    if (InHotStandby && SUPPORT_HOT_STANDBY) {
        RelFileNode rnode;

        (void)XLogRecGetBlockTag(record, HEAP_FREEZE_ORIG_BLOCK_NUM, &rnode, NULL, NULL);
        ResolveRecoveryConflictWithSnapshot(cutoffXid, rnode);
    }

    if (XLogReadBufferForRedo(record, HEAP_FREEZE_ORIG_BLOCK_NUM, &buffer) == BLK_NEEDS_REDO) {
        char *maindata = XLogRecGetData(record);
        Size blkdatalen;
        char *blkdata = NULL;
        blkdata = XLogRecGetBlockData(record, HEAP_FREEZE_ORIG_BLOCK_NUM, &blkdatalen);

        Page page = buffer.pageinfo.page;
        TransactionId cutoffXid = xlrec->cutoff_xid;
        OffsetNumber *offsets = (OffsetNumber *)maindata;
        OffsetNumber *offsetsEnd = NULL;

        if (blkdatalen > 0) {
            offsetsEnd = (OffsetNumber *)((char *)offsets + blkdatalen);

            while (offsets < offsetsEnd) {
                RowPtr *rowptr = UPageGetRowPtr(page, *offsets);

                Assert(RowPtrIsNormal(rowptr));

                utuple.disk_tuple = (UHeapDiskTuple)UPageGetRowData(page, rowptr);
                utuple.disk_tuple_size = RowPtrGetLen(rowptr);
                UHeapTupleCopyBaseFromPage(&utuple, page);

                TransactionId xid = UHeapTupleGetRawXid(&utuple);
                if (TransactionIdIsNormal(xid) && TransactionIdPrecedes(xid, cutoffXid)) {
                    UHeapTupleSetRawXid((&utuple), FrozenTransactionId);
                }

                offsets++;
            }
        }

        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

void UHeapRedo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    /*
     * These operations don't overwrite MVCC data so no conflict processing is
     * required. The ones in heap2 rmgr do.
     */
    switch (info & XLOG_UHEAP_OPMASK) {
        case XLOG_UHEAP_INSERT:
            UHeapXlogInsert(record);
            break;
        case XLOG_UHEAP_DELETE:
            UHeapXlogDelete(record);
            break;
        case XLOG_UHEAP_UPDATE:
            UHeapXlogUpdate(record);
            break;
        case XLOG_UHEAP_FREEZE_TD_SLOT:
            UHeapXlogFreezeTdSlot(record);
            break;
        case XLOG_UHEAP_INVALID_TD_SLOT:
            UHeapXlogInvalidTdSlot(record);
            break;
        case XLOG_UHEAP_CLEAN:
            UHeapXlogClean(record);
            break;
        case XLOG_UHEAP_MULTI_INSERT:
            UHeapXlogMultiInsert(record);
            break;
        default:
            ereport(PANIC, (errmsg("UHeapRedo: unknown op code %u", (uint8)info)));
    }
    elog(DEBUG2, "UHeapRedo called lsn %016lx", record->EndRecPtr);
}

void UHeap2Redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    /*
     * These operations don't overwrite MVCC data so no conflict processing is
     * required. The ones in heap2 rmgr do.
     */
    switch (info & XLOG_UHEAP_OPMASK) {
        case XLOG_UHEAP2_BASE_SHIFT:
            UHeapXlogBaseShift(record);
            break;
        case XLOG_UHEAP2_FREEZE:
            UHeapXlogFreeze(record);
            break;
        case XLOG_UHEAP2_EXTEND_TD_SLOTS:
            UHeapXlogExtendTDSlot(record);
            break;
        default:
            ereport(PANIC, (errmsg("UHeap2Redo: unknown op code %u", (uint8)info)));
    }
    elog(DEBUG2, "UHeap2Redo called lsn %016lx", record->EndRecPtr);
}

static void UHeapUndoXlogPageRestore(char *curxlogptr, Buffer buffer, Page page)
{
    /* Restore updated line pointers */
    OffsetNumber *xlogMinLPOffset = (OffsetNumber *)curxlogptr;
    curxlogptr += sizeof(OffsetNumber);
    OffsetNumber *xlogMaxLPOffset = (OffsetNumber *)curxlogptr;
    curxlogptr += sizeof(OffsetNumber);

    Assert(*xlogMinLPOffset > InvalidOffsetNumber);
    Assert(*xlogMaxLPOffset <= MaxOffsetNumber);

    if (*xlogMaxLPOffset >= *xlogMinLPOffset) {
        size_t lpSize = (*xlogMaxLPOffset - *xlogMinLPOffset + 1) * sizeof(RowPtr);
        error_t rc = memcpy_s((char *)UPageGetRowPtr(page, *xlogMinLPOffset), lpSize, curxlogptr, lpSize);
        securec_check_c(rc, "\0", "\0");
        curxlogptr += lpSize;

        /* Restore updated tuples data */
        Offset *xlogCopyStartOffset = (Offset *)curxlogptr;
        curxlogptr += sizeof(Offset);
        Offset *xlogCopyEndOffset = (Offset *)curxlogptr;
        curxlogptr += sizeof(Offset);

        Assert(*xlogCopyStartOffset > (Offset)SizeOfUHeapPageHeaderData);
        Assert(*xlogCopyEndOffset <= BLCKSZ);

        if (*xlogCopyEndOffset > *xlogCopyStartOffset) {
            size_t dataSize = *xlogCopyEndOffset - *xlogCopyStartOffset;
            rc = memcpy_s((char *)page + *xlogCopyStartOffset, dataSize, curxlogptr, dataSize);
            securec_check_c(rc, "\0", "\0");
            curxlogptr += dataSize;
        }

        /* Restore updated page headers */
        TransactionId *pdPruneXid = (TransactionId *)curxlogptr;
        curxlogptr += sizeof(TransactionId);
        uint16 *pdFlags = (uint16 *)curxlogptr;
        curxlogptr += sizeof(uint16);
        uint16 *potentialFreespace = (uint16 *)curxlogptr;
        curxlogptr += sizeof(uint16);

        UHeapPageHeaderData *phdr = (UHeapPageHeaderData *)page;
        phdr->pd_flags = *pdFlags;
        phdr->pd_prune_xid = *pdPruneXid;
        phdr->potential_freespace = *potentialFreespace;
    }

    /* Restore TD slot */
    int *tdSlotId = (int *)curxlogptr;
    curxlogptr += sizeof(int);
    TransactionId *xid = (TransactionId *)curxlogptr;
    curxlogptr += sizeof(TransactionId);
    UndoRecPtr *slotPrevUrp = (UndoRecPtr *)curxlogptr;
    curxlogptr += sizeof(UndoRecPtr);

    UHeapPageSetUndo(buffer, *tdSlotId, *xid, *slotPrevUrp);
}

/*
 * replay of undo page operation
 */
static void UHeapUndoXlogPage(XLogReaderState *record)
{
    RedoBufferInfo redoBuffInfo;
    uint8 *flags = (uint8 *)XLogRecGetData(record);
    char *curxlogptr = (char *)((char *)flags + sizeof(uint8));
    XLogRedoAction action = XLogReadBufferForRedo(record, 0, &redoBuffInfo);
    Buffer buf = redoBuffInfo.buf;

    if (action == BLK_NEEDS_REDO) {
        Page page = BufferGetPage(buf);

        if (*flags & XLU_INIT_PAGE) {
            TransactionId *xidBase = (TransactionId *)curxlogptr;
            curxlogptr += sizeof(TransactionId);
            uint16 *tdCount = (uint16 *)curxlogptr;
            curxlogptr += sizeof(uint16);

            if (*flags & XLOG_UHEAP_INIT_TOAST_PAGE) {
                UPageInit<UPAGE_TOAST>(page, BufferGetPageSize(buf), UHEAP_SPECIAL_SIZE);
            } else {
                UPageInit<UPAGE_HEAP>(page, BufferGetPageSize(buf), UHEAP_SPECIAL_SIZE, *tdCount);
            }

            UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
            uheappage->pd_xid_base = *xidBase;
            uheappage->pd_multi_base = 0;
        } else {
            /* Restore Rolledback Page */
            UHeapUndoXlogPageRestore(curxlogptr, buf, page);
        }

        PageSetLSN(page, record->EndRecPtr);
        MarkBufferDirty(buf);
    }


    if (BufferIsValid(buf))
        UnlockReleaseBuffer(buf);
}

static void UHeapUndoXlogResetXid(XLogReaderState *record)
{
    RedoBufferInfo redoBuffInfo;
    XLogRecPtr lsn = record->EndRecPtr;
    XlUHeapUndoResetSlot *xlrec = (XlUHeapUndoResetSlot *)XLogRecGetData(record);

    XLogRedoAction action = XLogReadBufferForRedo(record, 0, &redoBuffInfo);

    Buffer buf = redoBuffInfo.buf;
    if (action == BLK_NEEDS_REDO) {
        UHeapPageSetUndo(buf, xlrec->td_slot_id, InvalidTransactionId, xlrec->urec_ptr);

        PageSetLSN(BufferGetPage(buf), lsn);
        MarkBufferDirty(buf);
    }

    if (BufferIsValid(buf))
        UnlockReleaseBuffer(buf);
}

static void UHeapUndoXlogAbortSpecinsert(XLogReaderState *record)
{
    RedoBufferInfo redoBuffInfo;
    uint8 *flags = (uint8 *)XLogRecGetData(record);
    XLogRecPtr lsn = record->EndRecPtr;
    XlUHeapUndoAbortSpecInsert *xlrec = (XlUHeapUndoAbortSpecInsert *)((char *)flags + sizeof(uint8));

    XLogRedoAction action = XLogReadBufferForRedo(record, 0, &redoBuffInfo);

    Buffer buf = redoBuffInfo.buf;

    if (action == BLK_NEEDS_REDO) {
        bool relhasindex = *flags & XLU_ABORT_SPECINSERT_REL_HAS_INDEX;
        bool isPageInit = *flags & XLU_ABORT_SPECINSERT_INIT_PAGE;
        bool hasValidPrevurp = *flags & XLU_ABORT_SPECINSERT_PREVURP_VALID;
        bool hasValidXid = *flags & XLU_ABORT_SPECINSERT_XID_VALID;
        TransactionId *xid = NULL, *xidbase = NULL;
        uint16 *tdCount = NULL;
        UndoRecPtr *prevurp = NULL;
        char *curxlogptr = (char *)((char *)xlrec + SizeOfUHeapUndoAbortSpecInsert);
        int tdSlot;

        if (isPageInit) {
            xidbase = (TransactionId *)curxlogptr;
            curxlogptr += sizeof(TransactionId);
            tdCount = (uint16 *)curxlogptr;
            curxlogptr += sizeof(uint16);
        }

        if (hasValidXid) {
            xid = (TransactionId *)curxlogptr;
            curxlogptr += sizeof(TransactionId);
        }

        if (hasValidPrevurp) {
            prevurp = (UndoRecPtr *)curxlogptr;
            curxlogptr += sizeof(UndoRecPtr);
        }

        ExecuteUndoForInsertRecovery(buf, xlrec->offset, XLogRecGetXid(record), relhasindex, &tdSlot);

        UHeapPageSetUndo(buf, tdSlot, (xid == NULL) ? InvalidTransactionId : *xid,
            (prevurp == NULL) ? INVALID_UNDO_REC_PTR : *prevurp);

        if (isPageInit) {
            Page page = BufferGetPage(buf);
            UPageInit<UPAGE_HEAP>(page, BufferGetPageSize(buf), UHEAP_SPECIAL_SIZE, *tdCount);
            UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
            uheappage->pd_xid_base = *xidbase;
            uheappage->pd_multi_base = 0;
        }

        PageSetLSN(BufferGetPage(buf), lsn);
        MarkBufferDirty(buf);
    }

    if (BufferIsValid(buf))
        UnlockReleaseBuffer(buf);
}

void UHeapUndoRedo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_UHEAPUNDO_PAGE:
            UHeapUndoXlogPage(record);
            break;
        case XLOG_UHEAPUNDO_RESET_SLOT:
            UHeapUndoXlogResetXid(record);
            break;
        case XLOG_UHEAPUNDO_ABORT_SPECINSERT:
            UHeapUndoXlogAbortSpecinsert(record);
            break;
        default:
            elog(PANIC, "UHeapUndoRedo: unknown op code %u", info);
    }
}

static TransactionId UHeapXlogGetCurrentXidInsert(XLogReaderState *record, bool hasCSN)
{
    XlUHeapInsert *xlrec = (XlUHeapInsert *)XLogRecGetData(record);
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapInsert +
        (hasCSN ? sizeof(CommitSeqNo) : 0));
    bool hasCurrentXid = ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_CURRENT_XID) != 0);

    if (!hasCurrentXid) {
        return XLogRecGetXid(record);
    }

    char *currLogPtr = ((char *)xlundohdr + SizeOfXLUndoHeader);

    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        currLogPtr += sizeof(UndoRecPtr);
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        currLogPtr += sizeof(UndoRecPtr);
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        currLogPtr += sizeof(Oid);
    }

    return *(TransactionId*)(currLogPtr);
}

static TransactionId UHeapXlogGetCurrentXidDelete(XLogReaderState *record, bool hasCSN)
{
    XlUHeapDelete *xlrec = (XlUHeapDelete *)XLogRecGetData(record);
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapDelete +
            (hasCSN ? sizeof(CommitSeqNo) : 0));
    bool hasCurrentXid = ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_CURRENT_XID) != 0);
    if (!hasCurrentXid) {
        return XLogRecGetXid(record);
    }

    char *currLogPtr = ((char *)xlundohdr + SizeOfXLUndoHeader);

    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_SUB_XACT) != 0) {
        currLogPtr += sizeof(bool);
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        currLogPtr += sizeof(UndoRecPtr);
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        currLogPtr += sizeof(UndoRecPtr);
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        currLogPtr += sizeof(Oid);
    }

    return *(TransactionId*)(currLogPtr);
}

static TransactionId UHeapXlogGetCurrentXidUpdate(XLogReaderState *record, bool hasCSN)
{
    XlUHeapUpdate *xlrec = (XlUHeapUpdate *)XLogRecGetData(record);
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapUpdate +
        (hasCSN ? sizeof(CommitSeqNo) : 0));
    bool hasCurrentXid = ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_CURRENT_XID) != 0);

    if (!hasCurrentXid) {
        return XLogRecGetXid(record);
    }

    char *currLogPtr = (char *)xlundohdr + SizeOfXLUndoHeader;

    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_SUB_XACT) != 0) {
        currLogPtr += sizeof(bool);
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        currLogPtr += sizeof(UndoRecPtr);
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        currLogPtr += sizeof(UndoRecPtr);
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        currLogPtr += sizeof(Oid);
    }

    return *(TransactionId*)(currLogPtr);
}

static TransactionId UHeapXlogGetCurrentXidMultiInsert(XLogReaderState *record)
{
    XlUndoHeader *xlundohdr = (XlUndoHeader *)XLogRecGetData(record);
    bool hasCurrentXid = ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_CURRENT_XID) != 0);

    if (!hasCurrentXid) {
        return XLogRecGetXid(record);
    }

    char *curxlogptr = (char *)xlundohdr + SizeOfXLUndoHeader;

    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        curxlogptr += sizeof(UndoRecPtr);
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        curxlogptr += sizeof(UndoRecPtr);
    }
    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        curxlogptr += sizeof(Oid);
    }

    return *(TransactionId*)(curxlogptr);
}

TransactionId UHeapXlogGetCurrentXid(XLogReaderState *record, bool hasCSN)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info & XLOG_UHEAP_OPMASK) {
        case XLOG_UHEAP_INSERT:
            return UHeapXlogGetCurrentXidInsert(record, hasCSN);
        case XLOG_UHEAP_DELETE:
            return UHeapXlogGetCurrentXidDelete(record, hasCSN);
        case XLOG_UHEAP_UPDATE:
            return UHeapXlogGetCurrentXidUpdate(record, hasCSN);
        case XLOG_UHEAP_FREEZE_TD_SLOT:
        case XLOG_UHEAP_INVALID_TD_SLOT:
        case XLOG_UHEAP_CLEAN:
            break;
        case XLOG_UHEAP_MULTI_INSERT:
            /* The way we get current xid in MULTI_INSERT is not affected */
            return UHeapXlogGetCurrentXidMultiInsert(record);
        default:
            ereport(PANIC, (errmsg("UHeapRedo: unknown op code %u", (uint8)info)));    
    }

    return XLogRecGetXid(record); 
}
