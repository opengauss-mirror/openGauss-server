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
 * knl_uextremeredo.cpp
 * WAL extreme replay logic for uheap.
 *
 * IDENTIFICATION
 * opengauss_server/src/storage/access/ustore/knl_uextremeredo.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogutils.h"
#include "catalog/pg_tablespace.h"
#include "storage/standby.h"
#include "access/extreme_rto/page_redo.h"
#include "access/ustore/knl_uredo.h"
#include "access/ustore/knl_uextremeredo.h"
#include "access/ustore/knl_upage.h"
#include "access/ustore/knl_undorequest.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "access/ustore/knl_uheap.h"
#include "access/ustore/undo/knl_uundoxlog.h"
#include "securec.h"
#include "storage/freespace.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/undo/knl_uundotxn.h"
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
    const bool hasInvalidLockerTDId)
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

    if (hasInvalidLockerTDId) {
        UHeapTupleHeaderSetLockerTDSlot(disktup, InvalidTDSlotId);
    } else {
        UHeapTupleHeaderSetLockerTDSlot(disktup, xlhdr.locker_td_id);
    }

    disktup->flag2 = xlhdr.flag2;
    disktup->flag = xlhdr.flag;
    disktup->t_hoff = xlhdr.t_hoff;

    return disktup;
}

static XLogRecParseState *UHeapXlogInsertParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;

    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    if (!extreme_rto::RedoWorkerIsUndoSpaceWorker()) {
        XLogRecSetBlockDataState(record, UHEAP_INSERT_ORIG_BLOCK_NUM, recordstatehead);
    } else {
        XLogRecSetUHeapUndoBlockState(record, UHEAP_UNDO_ORIG_BLOCK_NUM, recordstatehead);
    }

    return recordstatehead;
}

static XLogRecParseState *UHeapXlogDeleteParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    if (!extreme_rto::RedoWorkerIsUndoSpaceWorker()) {
        XLogRecSetBlockDataState(record, UHEAP_DELETE_ORIG_BLOCK_NUM, recordstatehead);
    } else {
        XLogRecSetUHeapUndoBlockState(record, UHEAP_UNDO_ORIG_BLOCK_NUM, recordstatehead);
    }

    return recordstatehead;
}

static XLogRecParseState *UHeapXlogUpdateParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    XLogRecParseState *blockstate = NULL;
    BlockNumber newblk, oldblk;

    XLogRecGetBlockTag(record, UHEAP_UPDATE_NEW_BLOCK_NUM, NULL, NULL, &newblk);

    if (!XLogRecGetBlockTag(record, UHEAP_UPDATE_OLD_BLOCK_NUM, NULL, NULL, &oldblk)) {
        oldblk = newblk;
    }

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    if (!extreme_rto::RedoWorkerIsUndoSpaceWorker()) {
        XLogRecSetBlockDataState(record, UHEAP_UPDATE_NEW_BLOCK_NUM, recordstatehead);
        XLogRecSetAuxiBlkNumState(&recordstatehead->blockparse.extra_rec.blockdatarec, oldblk, InvalidForkNumber);
        if (oldblk != newblk) {
            Assert(((XlUHeapUpdate *)XLogRecGetData(record))->flags & XLZ_NON_INPLACE_UPDATE);
            (*blocknum)++;
            XLogParseBufferAllocListFunc(record, &blockstate, recordstatehead);
            if (blockstate == NULL) {
                return NULL;
            }
            XLogRecSetBlockDataState(record, UHEAP_UPDATE_OLD_BLOCK_NUM, blockstate);
            XLogRecSetAuxiBlkNumState(&blockstate->blockparse.extra_rec.blockdatarec, newblk, InvalidForkNumber);
        }
    } else {
        XLogRecSetUHeapUndoBlockState(record, UHEAP_UPDATE_NEW_BLOCK_NUM, recordstatehead);
        recordstatehead->blockparse.extra_rec.blockundorec.updateUndoParse.newblk = newblk;
        recordstatehead->blockparse.extra_rec.blockundorec.updateUndoParse.oldblk = oldblk;
    }
    return recordstatehead;
}

static XLogRecParseState *UHeapXlogMultiInsertParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    if (!extreme_rto::RedoWorkerIsUndoSpaceWorker()) {
        XLogRecSetBlockDataState(record, UHEAP_MULTI_INSERT_ORIG_BLOCK_NUM, recordstatehead);
    } else {
        XLogRecSetUHeapUndoBlockState(record, UHEAP_UNDO_ORIG_BLOCK_NUM, recordstatehead);
    }

    return recordstatehead;
}

static XLogRecParseState *UHeapXlogFreezeTDParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, UHEAP_FREEZE_TD_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

static XLogRecParseState *UHeapXlogInvalidTDParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, UHEAP_INVALID_TD_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

static XLogRecParseState *UHeapXlogCleanParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, UHEAP_CLEAN_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

XLogRecParseState *UHeapRedoParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;

    *blocknum = 0;
    /*
     * These operations don't overwrite MVCC data so no conflict processing is
     * required. The ones in heap2 rmgr do.
     */

    switch (info & XLOG_UHEAP_OPMASK) {
        case XLOG_UHEAP_INSERT:
            recordblockstate = UHeapXlogInsertParseBlock(record, blocknum);
            break;
        case XLOG_UHEAP_DELETE:
            recordblockstate = UHeapXlogDeleteParseBlock(record, blocknum);
            break;
        case XLOG_UHEAP_UPDATE:
            recordblockstate = UHeapXlogUpdateParseBlock(record, blocknum);
            break;
        case XLOG_UHEAP_FREEZE_TD_SLOT:
            recordblockstate = UHeapXlogFreezeTDParseBlock(record, blocknum);
            break;
        case XLOG_UHEAP_INVALID_TD_SLOT:
            recordblockstate = UHeapXlogInvalidTDParseBlock(record, blocknum);
            break;
        case XLOG_UHEAP_CLEAN:
            recordblockstate = UHeapXlogCleanParseBlock(record, blocknum);
            break;
        case XLOG_UHEAP_MULTI_INSERT:
            recordblockstate = UHeapXlogMultiInsertParseBlock(record, blocknum);
            break;
        default:
            ereport(PANIC, (errmsg("UHeapRedoParseToBlock: unknown op code %u", info)));
    }
    return recordblockstate;
}

static XLogRecParseState *UHeap2XlogBaseShiftParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, UHEAP2_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

static XLogRecParseState *UHeap2XlogFreezeParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, UHEAP2_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

static XLogRecParseState *UHeap2XlogExtendTDSlotsParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, UHEAP2_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

XLogRecParseState *UHeap2RedoParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;

    *blocknum = 0;
    /*
     * These operations don't overwrite MVCC data so no conflict processing is
     * required. The ones in heap2 rmgr do.
     */

    switch (info & XLOG_UHEAP_OPMASK) {
        case XLOG_UHEAP2_BASE_SHIFT:
            recordblockstate = UHeap2XlogBaseShiftParseBlock(record, blocknum);
            break;
        case XLOG_UHEAP2_FREEZE:
            recordblockstate = UHeap2XlogFreezeParseBlock(record, blocknum);
            break;
        case XLOG_UHEAP2_EXTEND_TD_SLOTS:
            recordblockstate = UHeap2XlogExtendTDSlotsParseBlock(record, blocknum);
            break;
        default:
            ereport(PANIC, (errmsg("UHeap2RedoParseToBlock: unknown op code %u", info)));
    }

    return recordblockstate;
}

static XLogRecParseState *UHeapXlogUndoDiscardParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetUndoBlockState(record, UHEAP_UNDO_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

static XLogRecParseState *UHeapXlogUndoUnlinkParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetUndoBlockState(record, UHEAP_UNDO_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

static XLogRecParseState *UHeapXlogUndoExtendParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetUndoBlockState(record, UHEAP_UNDO_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

static XLogRecParseState *UHeapXlogUndoCleanParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetUndoBlockState(record, UHEAP_UNDO_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

XLogRecParseState *UHeapUndoRedoParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;
    *blocknum = 0;
    /*
     * These operations don't overwrite MVCC data so no conflict processing is
     * required. The ones in heap2 rmgr do.
     */

    switch (info & XLOG_UHEAP_OPMASK) {
        case XLOG_UNDO_DISCARD:
            recordblockstate = UHeapXlogUndoDiscardParseBlock(record, blocknum);
            break;
        case XLOG_UNDO_UNLINK:
        case XLOG_SLOT_UNLINK:
            recordblockstate = UHeapXlogUndoUnlinkParseBlock(record, blocknum);
            break;
        case XLOG_UNDO_EXTEND:
        case XLOG_SLOT_EXTEND:
            recordblockstate = UHeapXlogUndoExtendParseBlock(record, blocknum);
            break;
        case XLOG_UNDO_CLEAN:
        case XLOG_SLOT_CLEAN:
            recordblockstate = UHeapXlogUndoCleanParseBlock(record, blocknum);
            break;
        default:
            ereport(PANIC, (errmsg("UHeapUndoRedoParseToBlock: unknown op code %u", info)));
    }
    return recordblockstate;
}

static XLogRecParseState *UHeapXlogUheapUndoResetSlotParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, UHEAP_UNDOACTION_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

static XLogRecParseState *UHeapXlogUheapUndoPageParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, UHEAP_UNDOACTION_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

static XLogRecParseState *UHeapXlogUheapUndoAbortSpecInsertParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetBlockDataState(record, UHEAP_UNDOACTION_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

XLogRecParseState *UHeapUndoActionRedoParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;
    *blocknum = 0;

    switch (info & XLOG_UHEAP_OPMASK) {
        case XLOG_UHEAPUNDO_RESET_SLOT:
            recordblockstate = UHeapXlogUheapUndoResetSlotParseBlock(record, blocknum);
            break;
        case XLOG_UHEAPUNDO_PAGE:
            recordblockstate = UHeapXlogUheapUndoPageParseBlock(record, blocknum);
            break;
        case XLOG_UHEAPUNDO_ABORT_SPECINSERT:
            recordblockstate = UHeapXlogUheapUndoAbortSpecInsertParseBlock(record, blocknum);
            break;
        default:
            ereport(PANIC, (errmsg("UHeapUndoActionRedoParseToBlock: unknown op code %u", info)));
    }

    return recordblockstate;
}

static XLogRecParseState *UHeapRollbackFinishParseBlock(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }
    XLogRecSetRollbackFinishBlockState(record, UHEAP_UNDO_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

XLogRecParseState *UHeapRollbackFinishRedoParseToBlock(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordblockstate = NULL;
    *blocknum = 0;

    switch (info & XLOG_UHEAP_OPMASK) {
        case XLOG_ROLLBACK_FINISH:
            recordblockstate = UHeapRollbackFinishParseBlock(record, blocknum);
            break;
        default:
            ereport(PANIC, (errmsg("UHeapRollbackFinishRedoParseToBlock: unknown op code %u", info)));
    }

    return recordblockstate;
}

static char *ReachXlUndoHeaderEnd(XlUndoHeader *xlundohdr)
{
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
    return currLogPtr;
}

void UHeapXlogInsertOperatorPage(RedoBufferInfo *buffer, void *recorddata, bool isinit, bool istoast, void *blkdata,
    Size datalen, TransactionId recxid, Size *freespace)
{
    char *data = (char *)blkdata;
    Page page = buffer->pageinfo.page;
    XlUHeapInsert *xlrec = (XlUHeapInsert *)recorddata;
    ItemPointerData targetTid;
    Size newlen = datalen - SizeOfUHeapHeader;
    UHeapBufferPage bufpage;
    TupleBuffer tbuf;

    errno_t rc = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
    securec_check(rc, "\0", "\0");

    Assert(datalen > SizeOfUHeapHeader && newlen <= MaxPossibleUHeapTupleSize);
    UHeapDiskTuple utup = GetUHeapDiskTupleFromRedoData(data, &newlen, tbuf, true);

    undo::XlogUndoMeta undometa;
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapInsert);
    char *currLogPtr = ReachXlUndoHeaderEnd(xlundohdr);
    XlogUndoMeta *xlundometa = (XlogUndoMeta *)((char *)currLogPtr);
    UndoRecPtr urecptr = xlundohdr->urecptr;

    if (isinit) {
        /* copy xlundometa to local struct */
        CopyUndoMeta(*xlundometa, undometa);
        uint32 size = undometa.Size();
        TransactionId *xidBase = (TransactionId *)((char *)xlundometa + size);
        uint16 *tdCount = (uint16 *)((char *)xidBase + sizeof(TransactionId));
        if (istoast) {
            UPageInit<UPAGE_TOAST>(page, buffer->pageinfo.pagesize, UHEAP_SPECIAL_SIZE);
        } else {
            UPageInit<UPAGE_HEAP>(page, buffer->pageinfo.pagesize, UHEAP_SPECIAL_SIZE, *tdCount);
        }
        UHeapPageHeaderData *uheappage = (UHeapPageHeaderData*)page;
        uheappage->pd_xid_base = *xidBase;
        uheappage->pd_multi_base = 0;
    }

    ItemPointerSetBlockNumber(&targetTid, buffer->blockinfo.blkno);
    ItemPointerSetOffsetNumber(&targetTid, xlrec->offnum);

    OffsetNumber maxoff = UHeapPageGetMaxOffsetNumber(page);
    if (maxoff + 1 < xlrec->offnum)
        ereport(PANIC, (errmsg("UHeapXlogInsertOperatorPage: invalid max offset number")));

    bufpage.buffer = buffer->buf;
    bufpage.page = NULL;
    if (UPageAddItem(NULL, &bufpage, (Item)utup, newlen, xlrec->offnum, true) == InvalidOffsetNumber)
        ereport(PANIC, (errmsg("UHeapXlogInsertOperatorPage: failed to add tuple")));

    UHeapRecordPotentialFreeSpace(buffer->buf, -1 * SHORTALIGN(newlen));

    // set undo
    int tdSlotId = UHeapTupleHeaderGetTDSlot(utup);
    UHeapPageSetUndo(buffer->buf, tdSlotId, recxid, urecptr);

    PageSetLSN(page, buffer->lsn);
}

void UHeapXlogDeleteOperatorPage(RedoBufferInfo *buffer, void *recorddata, Size recordlen, TransactionId recxid)
{
    Page page = buffer->pageinfo.page;
    TupleBuffer tbuf;
    errno_t rc = EOK;
    rc = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
    securec_check(rc, "\0", "\0");

    XlUHeapDelete *xlrec = (XlUHeapDelete *)recorddata;
    ItemPointerData targetTid;

    UHeapTupleData utup;
    undo::XlogUndoMeta undometa;
    RowPtr *rp;
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapDelete);
    char *currLogPtr = ReachXlUndoHeaderEnd(xlundohdr);
    XlogUndoMeta *xlundometa = (XlogUndoMeta *)((char *)currLogPtr);
    UndoRecPtr urecptr = xlundohdr->urecptr;

    /* copy xlundometa to local struct */
    CopyUndoMeta(*xlundometa, undometa);
    uint32 undoMetaSize = undometa.Size();
    /*
     * If the WAL stream contains undo tuple, then replace it with the
     * explicitly stored tuple.
     */
    Size datalen = recordlen - SizeOfXLUndoHeader - SizeOfUHeapDelete - undoMetaSize - SizeOfUHeapHeader;
    char *data = (char *)xlrec + SizeOfUHeapDelete + SizeOfXLUndoHeader + undoMetaSize;

    /*
     * If the WAL stream contains undo tuple, then replace it with the
     * explicitly stored tuple.
     */
    utup.disk_tuple = GetUHeapDiskTupleFromRedoData(data, &datalen, tbuf, false);
    utup.disk_tuple_size = datalen;

    ItemPointerSetBlockNumber(&targetTid, buffer->blockinfo.blkno);
    ItemPointerSetOffsetNumber(&targetTid, xlrec->offnum);

    utup.table_oid = xlundohdr->relOid;
    utup.ctid = targetTid;

    OffsetNumber maxoff = UHeapPageGetMaxOffsetNumber(page);
    if (maxoff >= xlrec->offnum) {
        rp = UPageGetRowPtr(page, xlrec->offnum);
    } else {
        elog(PANIC, "UHeapXlogDeleteOperatorPage: xlog delete offset is greater than the max offset on page");
    }

    /* increment the potential freespace of this page */
    UHeapRecordPotentialFreeSpace(buffer->buf, SHORTALIGN(datalen));

    utup.disk_tuple = (UHeapDiskTuple)UPageGetRowData(page, rp);
    utup.disk_tuple_size = RowPtrGetLen(rp);
    UHeapTupleHeaderSetTDSlot(utup.disk_tuple, xlrec->td_id);
    utup.disk_tuple->flag = xlrec->flag;
    UHeapPageSetUndo(buffer->buf, xlrec->td_id, recxid, urecptr);

    /* Mark the page as a candidate for pruning */
    UPageSetPrunable(page, recxid);

    PageSetLSN(page, buffer->lsn);
}

void UHeapXlogUpdateOperatorOldpage(UpdateRedoBuffers* buffers, void *recorddata,
    bool inplaceUpdate, bool blockInplaceUpdate, UHeapTupleData *oldtup, bool sameBlock,
    BlockNumber blk, TransactionId recordxid)
{
    XLogRecPtr lsn = buffers->oldbuffer.lsn;
    Page oldpage = buffers->oldbuffer.pageinfo.page;
    Pointer recData = (Pointer)recorddata;
    XlUHeapUpdate *xlrec = (XlUHeapUpdate *)recData;
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapUpdate);
    UndoRecPtr urecptr = xlundohdr->urecptr;
    Buffer oldbuf = buffers->oldbuffer.buf;
    RowPtr *rp = NULL;
    if (UHeapPageGetMaxOffsetNumber(oldpage) >= xlrec->old_offnum) {
        rp = UPageGetRowPtr(oldpage, xlrec->old_offnum);
    } else {
        elog(PANIC, "invalid rp");
    }

    ItemPointerData oldtid, newtid;
    ItemPointerSet(&oldtid, blk, xlrec->old_offnum);
    ItemPointerSet(&newtid, buffers->newbuffer.blockinfo.blkno, xlrec->new_offnum);

    /* Construct old tuple for undo record */
    oldtup->table_oid = xlundohdr->relOid;
    oldtup->ctid = oldtid;

    /* Ensure old tuple points to the tuple in page. */
    oldtup->disk_tuple = (UHeapDiskTuple)UPageGetRowData(oldpage, rp);
    oldtup->disk_tuple_size = RowPtrGetLen(rp);
    oldtup->disk_tuple->flag = xlrec->old_tuple_flag;

    UHeapTupleHeaderSetTDSlot(oldtup->disk_tuple, xlrec->old_tuple_td_id);

    if (!sameBlock) {
        UHeapPageSetUndo(oldbuf, xlrec->old_tuple_td_id, recordxid, urecptr);
    }

    /* Mark the page as a candidate for pruning  and update the page potential freespace */
    if (!inplaceUpdate || blockInplaceUpdate) {
        UPageSetPrunable(oldpage, recordxid);

        if (!sameBlock) {
            UHeapRecordPotentialFreeSpace(oldbuf, SHORTALIGN(oldtup->disk_tuple_size));
        }
    }

    PageSetLSN(oldpage, lsn);
}

Size UHeapXlogUpdateOperatorNewpage(UpdateRedoBuffers* buffers, void *recorddata,
    bool inplaceUpdate, bool blockInplaceUpdate, void *blkdata, UHeapTupleData *oldtup,
    Size recordlen, Size data_len, bool isinit, bool istoast, bool sameBlock,
    TransactionId recordxid, UpdateRedoAffixLens *affixLens)
{
    XLogRecPtr lsn = buffers->newbuffer.lsn;
    TupleBuffer tbuf;
    uint32 newlen = 0;
    XlUndoHeader *xlnewundohdr = NULL;
    undo::XlogUndoMeta undometa;
    TransactionId *xidBase = NULL;
    uint16 *tdCount = NULL;

    Pointer recData = (Pointer)recorddata;
    XlUHeapUpdate *xlrec = (XlUHeapUpdate *)recData;
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + SizeOfUHeapUpdate);
    char *curxlogptr = ReachXlUndoHeaderEnd(xlundohdr);
    UndoRecPtr urecptr = xlundohdr->urecptr;
    errno_t rc = EOK;

    if (!inplaceUpdate) {
        xlnewundohdr = (XlUndoHeader *)curxlogptr;
        curxlogptr += SizeOfXLUndoHeader;

        if (((xlnewundohdr)->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
            curxlogptr += sizeof(UndoRecPtr);
        }
        if (((xlnewundohdr)->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
            curxlogptr += sizeof(UndoRecPtr);
        }
        if (((xlnewundohdr)->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
            curxlogptr += sizeof(Oid);
        }
    }

    undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)curxlogptr;
    CopyUndoMeta(*xlundometa, undometa);
    uint32 undoMetaSize = undometa.Size();
    curxlogptr += undoMetaSize;

    if (inplaceUpdate) {
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
    } else {
        Size initPageXtraInfo = 0;
        if (isinit) {
            /* has xidBase */
            xidBase = (TransactionId *)curxlogptr;
            curxlogptr += sizeof(TransactionId);
            initPageXtraInfo += sizeof(TransactionId);
            tdCount = (uint16 *)curxlogptr;
            curxlogptr += sizeof(uint16);
            initPageXtraInfo += sizeof(uint16);

            Page newpage = buffers->newbuffer.pageinfo.page;
            if (istoast) {
                UPageInit<UPAGE_TOAST>(newpage, buffers->newbuffer.pageinfo.pagesize, UHEAP_SPECIAL_SIZE);
            } else {
                UPageInit<UPAGE_HEAP>(newpage, buffers->newbuffer.pageinfo.pagesize, UHEAP_SPECIAL_SIZE, *tdCount);
            }

            UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)newpage;
            uheappage->pd_xid_base = *xidBase;
            uheappage->pd_multi_base = 0;
        }
    }

    XlUHeapHeader xlhdr;
    char *recdata = (char *)blkdata;
    char *recdataEnd = recdata + data_len;

    if (!inplaceUpdate) {
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

    newlen = SizeOfUHeapDiskTupleData + tuplen + affixLens->prefixlen + affixLens->suffixlen;

    UHeapDiskTuple newtup = &tbuf.hdr;
    rc = memset_s((char *)newtup, SizeOfUHeapDiskTupleData, 0, SizeOfUHeapDiskTupleData);
    securec_check(rc, "\0", "\0");

    /*
     * Reconstruct the new tuple using the prefix and/or suffix from the
     * old tuple, and the data stored in the WAL record.
     */

    bool onlyCopyDelta = (inplaceUpdate && oldtup->disk_tuple->t_hoff == xlhdr.t_hoff &&
        (newlen == oldtup->disk_tuple_size) && !blockInplaceUpdate);
    uint32 bitmaplen = 0;
    uint32 deltalen = 0;
    char *bitmapData = NULL;
    char *deltaData = NULL;
    char *newp = NULL;
    if (!onlyCopyDelta) {
        newp = (char *)newtup + SizeOfUHeapDiskTupleData;
    }

    if (affixLens->prefixlen > 0) {
        int len;

        /* copy bitmap [+ padding] [+ oid] from WAL record */
        len = xlhdr.t_hoff - SizeOfUHeapDiskTupleData;
        if (len > 0) {
            if (onlyCopyDelta) {
                bitmaplen = len;
                bitmapData = recdata;
            } else {
                rc = memcpy_s(newp, tuplen, recdata, len);
                securec_check(rc, "\0", "\0");
                newp += len;
            }
            recdata += len;
        }

        /* copy prefix from old tuple */
        if (!onlyCopyDelta) {
            rc = memcpy_s(newp, affixLens->prefixlen, (char *)oldtup->disk_tuple +
                oldtup->disk_tuple->t_hoff, affixLens->prefixlen);
            securec_check(rc, "\0", "\0");
            newp += affixLens->prefixlen;
        }

        /* copy new tuple data from WAL record */
        len = tuplen - (xlhdr.t_hoff - SizeOfUHeapDiskTupleData);
        if (len > 0) {
            if (onlyCopyDelta) {
                deltalen = len;
                deltaData = recdata;
            } else {
                rc = memcpy_s(newp, tuplen, recdata, len);
                securec_check(rc, "\0", "\0");
                newp += len;
            }
            recdata += len;
        }
    } else {
        /*
         * copy bitmap [+ padding] [+ oid] + data from record, all in one
         * go
         */
        if (onlyCopyDelta) {
            deltalen = tuplen;
            deltaData = recdata;
        } else {
            rc = memcpy_s(newp, tuplen, recdata, tuplen);
            securec_check(rc, "\0", "\0");
            newp += tuplen;
        }
        recdata += tuplen;
    }

    Assert(recdata == recdataEnd);

    /* copy suffix from old tuple */
    if (affixLens->suffixlen > 0 && !onlyCopyDelta) {
        rc = memcpy_s(newp, affixLens->suffixlen, (char *)oldtup->disk_tuple + oldtup->disk_tuple_size -
            affixLens->suffixlen, affixLens->suffixlen);
        securec_check(rc, "\0", "\0");
    }

    if (onlyCopyDelta) {
        UHeapTupleHeaderSetTDSlot(oldtup->disk_tuple, xlhdr.td_id);
        UHeapTupleHeaderSetLockerTDSlot(oldtup->disk_tuple, xlhdr.locker_td_id);
        oldtup->disk_tuple->flag2 = xlhdr.flag2;
        oldtup->disk_tuple->flag = xlhdr.flag;
        oldtup->disk_tuple->t_hoff = xlhdr.t_hoff;
    } else {
        UHeapTupleHeaderSetTDSlot(newtup, xlhdr.td_id);
        UHeapTupleHeaderSetLockerTDSlot(newtup, xlhdr.locker_td_id);
        newtup->flag2 = xlhdr.flag2;
        newtup->flag = xlhdr.flag;
        newtup->t_hoff = xlhdr.t_hoff;
    }

    Buffer oldbuf = buffers->oldbuffer.buf;
    Buffer newbuf = buffers->newbuffer.buf;
    Page oldpage = buffers->oldbuffer.pageinfo.page;
    Page newpage = buffers->newbuffer.pageinfo.page;

    /* max offset number should be valid */
    Assert(UHeapPageGetMaxOffsetNumber(newpage) + 1 >= xlrec->new_offnum);

    if (blockInplaceUpdate) {
        Assert(!onlyCopyDelta);
        RowPtr *rp = UPageGetRowPtr(oldpage, xlrec->old_offnum);
        PutBlockInplaceUpdateTuple(oldpage, (Item)newtup, rp, newlen);
        /* update the potential freespace */
        UHeapRecordPotentialFreeSpace(newbuf, newlen);
        Assert(oldpage == newpage);
        UPageSetPrunable(newpage, recordxid);
        UHeapPageSetUndo(newbuf, xlrec->old_tuple_td_id, recordxid, urecptr);
    } else if (inplaceUpdate) {
        /*
         * For inplace updates, we copy the entire data portion including
         * the tuple header.
         */
        RowPtr *rp = UPageGetRowPtr(oldpage, xlrec->old_offnum);
        RowPtrChangeLen(rp, newlen);
        if (onlyCopyDelta) {
            // only copy delta
            if (affixLens->prefixlen > 0) {
                if (bitmaplen > 0) {
                    rc = memcpy_s((char *)oldtup->disk_tuple->data, bitmaplen, bitmapData, bitmaplen);
                    securec_check(rc, "\0", "\0");
                }
            }

            if (deltalen > 0) {
                rc = memcpy_s((char *)oldtup->disk_tuple->data + bitmaplen + affixLens->prefixlen, deltalen, deltaData,
                    deltalen);
                securec_check(rc, "\0", "\0");
            }
        } else {
            // use new constructed tuple
            rc = memcpy_s((char *)oldtup->disk_tuple, newlen, (char *)newtup, newlen);
            securec_check(rc, "\0", "\0");
        }

        if (newlen < oldtup->disk_tuple_size) {
            /* new tuple is smaller, a prunable candidate */
            Assert(oldpage == newpage);
            UPageSetPrunable(newpage, recordxid);
        }

        UHeapPageSetUndo(newbuf, xlrec->old_tuple_td_id, recordxid, urecptr);
    } else {
        UHeapBufferPage bufpage = {newbuf, NULL};
        if (UPageAddItem(NULL, &bufpage, (Item)newtup, newlen, xlrec->new_offnum, true) == InvalidOffsetNumber) {
            elog(PANIC, "failed to add tuple");
        }

        /* Update the page potential freespace */
        if (newbuf != oldbuf) {
            UHeapRecordPotentialFreeSpace(newbuf, -1 * SHORTALIGN(newlen));
        } else {
            int delta = newlen - oldtup->disk_tuple_size;
            UHeapRecordPotentialFreeSpace(newbuf, -1 * SHORTALIGN(delta));
        }

        if (sameBlock) {
            UHeapPageSetUndo(newbuf, xlrec->old_tuple_td_id, recordxid, xlnewundohdr->urecptr);
        } else {
            UHeapPageSetUndo(newbuf, newtup->td_id, recordxid, xlnewundohdr->urecptr);
        }
    }

    Size freespace = PageGetUHeapFreeSpace(newpage); /* needed to update FSM */

    PageSetLSN(newpage, lsn, false);

    return freespace;
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
    // the same as UheapXlogInsert
    UHeapTupleHeaderSetTDSlot(disktup, xlhdr->td_id);
    UHeapTupleHeaderSetLockerTDSlot(disktup, InvalidTDSlotId);
    disktup->flag2 = xlhdr->flag2;
    disktup->flag = xlhdr->flag;
    disktup->t_hoff = xlhdr->t_hoff;

    return disktup;
}

void UHeapXlogMultiInsertOperatorPage(RedoBufferInfo *buffer, void *recorddata, bool isinit, bool istoast,
    void *blkdata, Size datalen, TransactionId recxid, Size *freespace)
{
    char *data = (char *)blkdata;
    Page page = buffer->pageinfo.page;
    XlUHeapMultiInsert *xlrec = (XlUHeapMultiInsert *)recorddata;
    errno_t rc = EOK;
    UHeapBufferPage bufpage;
    TupleBuffer tbuf;
    rc = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
    securec_check_ss(rc, "\0", "\0");

    UHeapFreeOffsetRanges *ufreeOffsetRanges = NULL;
    TransactionId *xidBase = NULL;
    uint16 *tdCount = NULL;

    undo::XlogUndoMeta undometa;
    XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)xlrec + 0); // Size described in dispatcher.cpp
    char *curxlogptr = ReachXlUndoHeaderEnd(xlundohdr);
    UndoRecPtr *lastUrecptr = (UndoRecPtr *)curxlogptr;
    UndoRecPtr urecptr = *lastUrecptr;
    curxlogptr = (char *)lastUrecptr + sizeof(*lastUrecptr);
    undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)curxlogptr;
    
    /* copy xlundometa to local struct */
    CopyUndoMeta(*xlundometa, undometa);
    uint32 undoMetaSize = undometa.Size();
    curxlogptr += undoMetaSize;
    
    if (isinit) {
        xidBase = (TransactionId *)curxlogptr;
        curxlogptr += sizeof(TransactionId);
        tdCount = (uint16 *)curxlogptr;
        curxlogptr += sizeof(uint16);

        if (istoast) {
            UPageInit<UPAGE_TOAST>(page, buffer->pageinfo.pagesize, UHEAP_SPECIAL_SIZE);
        } else {
            UPageInit<UPAGE_HEAP>(page, buffer->pageinfo.pagesize, UHEAP_SPECIAL_SIZE, *tdCount);
        }
        UHeapPageHeaderData *uheappage = (UHeapPageHeaderData*)page;
        uheappage->pd_xid_base = *xidBase;
        uheappage->pd_multi_base = *xidBase;
    }

    xlrec = (XlUHeapMultiInsert *)((char *)curxlogptr);
    curxlogptr = (char *)xlrec + SizeOfUHeapMultiInsert;
    UndoRecPtr *urpvec = NULL;

    /* fetch number of distinct ranges */
    GetOffsetRangesForMultiInsert(&ufreeOffsetRanges, curxlogptr, &urpvec);

    int newlen;
    int tdSlot = 0;
    bool firstTime = true;
    int prevTDSlot = -1;
    OffsetNumber offnum = ufreeOffsetRanges->startOffset[0];
    bool skipUndo = (xlrec->flags & XLZ_INSERT_IS_FROZEN);

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

        UHeapDiskTuple uhtup = GetUHeapDiskTupleFromMultiInsertRedoData(&data, &newlen, tbuf);

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
                prevTDSlot = tdSlot;
                firstTime = false;
            } else {
                /* All the tuples must refer to same transaction slot. */
                Assert(prevTDSlot == tdSlot);
                prevTDSlot = tdSlot;
            }
        }
    }

    if (!skipUndo) {
        UHeapPageSetUndo(buffer->buf, tdSlot, recxid, urecptr);
    }

    pfree(ufreeOffsetRanges);
    PageSetLSN(page, buffer->lsn);
}

void UHeapXlogFreezeTDOperatorPage(RedoBufferInfo *buffer, void *recorddata)
{
    XlUHeapFreezeTdSlot *xlrec = (XlUHeapFreezeTdSlot *)recorddata;
    int *frozenSlots = (int *)((char *)xlrec + SizeOfUHeapFreezeTDSlot);
    int nFrozen = xlrec->nFrozen;
    int slotNo = 0;

    Page page = buffer->pageinfo.page;
    UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
    TD *transinfo = tdPtr->td_info;

    if (InHotStandby && TransactionIdIsValid(xlrec->latestFrozenXid))
        ResolveRecoveryConflictWithSnapshot(xlrec->latestFrozenXid, buffer->blockinfo.rnode);

    UHeapFreezeOrInvalidateTuples(buffer->buf, nFrozen, frozenSlots, true);

    for (int i = 0; i < nFrozen; i++) {
        TD *thistrans;

        slotNo = frozenSlots[i];
        thistrans = &transinfo[slotNo];

        /* Any pending frozen trans slot should have xid within range of latestxid */
        Assert(!TransactionIdFollows(thistrans->xactid, xlrec->latestFrozenXid));

        thistrans->xactid = InvalidTransactionId;
        thistrans->undo_record_ptr = INVALID_UNDO_REC_PTR;
    }

    PageSetLSN(page, buffer->lsn);
}

void UHeapXlogInvalidTDOperatorPage(RedoBufferInfo *buffer, void *recorddata)
{
    uint16 *nCompletedSlots = (uint16 *)recorddata;
    int *completedXactSlots = (int *)((char *)nCompletedSlots + sizeof(uint16));
    int slotNo = 0;

    Page page = buffer->pageinfo.page;
    UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
    TD *transinfo = tdPtr->td_info;

    UHeapFreezeOrInvalidateTuples(buffer->buf, *nCompletedSlots, completedXactSlots, false);

    for (int i = 0; i < *nCompletedSlots; i++) {
        slotNo = completedXactSlots[i];
        transinfo[slotNo].xactid = InvalidTransactionId;
    }

    PageSetLSN(page, buffer->lsn);
}

void UHeapXlogCleanOperatorPrunePage(RedoBufferInfo *buffer, void *recorddata, void *blkdata,
    Size datalen, OffsetNumber *targetOffnum)
{
    XlUHeapClean *xlrec = (XlUHeapClean *)recorddata;
    UPruneState prstate;

    int ndeleted = xlrec->ndeleted;
    int ndead = xlrec->ndead;

    OffsetNumber *deleted = (OffsetNumber *)blkdata;
    OffsetNumber *end = (OffsetNumber *)((char *)deleted + datalen);
    OffsetNumber *nowdead = deleted + (ndeleted * 2);
    OffsetNumber *nowunused = nowdead + ndead;

    int nunused = (end - nowunused);
    Assert(nunused >= 0);

    OffsetNumber *offnum = deleted;
    for (int i = 0; i < ndeleted; i++) {
        prstate.nowdeleted[i] = *offnum++;
    }

    offnum = nowdead;
    for (int i = 0; i < ndead; i++) {
        prstate.nowdead[i] = *offnum++;
    }

    offnum = nowunused;
    for (int i = 0; i < nunused; i++) {
        prstate.nowunused[i] = *offnum++;
    }

    prstate.ndeleted = ndeleted;
    prstate.ndead = ndead;
    prstate.nunused = nunused;
    UHeapPagePruneExecute(buffer->buf, *targetOffnum, &prstate);
}

void UHeapXlogCleanOperatorPage(RedoBufferInfo *buffer, void *recorddata, void *blkdata, Size datalen)
{
    XlUHeapClean *xlrec = (XlUHeapClean *)recorddata;
    Page page = buffer->pageinfo.page;

    OffsetNumber *targetOffnum;
    OffsetNumber tmpTargetOff;
    Size *spaceRequired;
    Size tmpSpcRqd;

    /*
     * We're about to remove tuples. In Hot Standby mode, ensure that there's
     * no queries running for which the removed tuples are still visible.
     *
     * Not all UHEAP_CLEAN records remove tuples with xids, so we only want to
     * conflict on the records that cause MVCC failures for user queries. If
     * latestRemovedXid is invalid, skip conflict processing.
     */
    if (InHotStandby && TransactionIdIsValid(xlrec->latestRemovedXid))
        ResolveRecoveryConflictWithSnapshot(xlrec->latestRemovedXid, buffer->blockinfo.rnode);

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

    if (blkdata != NULL) {
        UHeapXlogCleanOperatorPrunePage(buffer, recorddata, blkdata, datalen, targetOffnum);
    }

    if (xlrec->flags & XLZ_CLEAN_ALLOW_PRUNING) {
        bool pruned PG_USED_FOR_ASSERTS_ONLY = false;

        UPageRepairFragmentation(buffer->buf, *targetOffnum, *spaceRequired, &pruned, false);

        /*
         * Pruning must be successful at redo time, otherwise the page
         * contents on master and standby might differ.
         */
        Assert(pruned);
    }

    Size freespace = PageGetUHeapFreeSpace(page); /* needed to update FSM
                                              * below */

    /*
     * Note: we don't worry about updating the page's prunability hints.
     * At worst this will cause an extra prune cycle to occur soon.
     */

    PageSetLSN(page, buffer->lsn);

    /*
     * Update the FSM as well.
     *
     * XXX: Don't do this if the page was restored from full page image. We
     * don't bother to update the FSM in that case, it doesn't need to be
     * totally accurate anyway.
     */
    XLogRecordPageWithFreeSpace(buffer->blockinfo.rnode, buffer->blockinfo.blkno, freespace);
}

static void UHeapXlogInsertBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    bool isinit = (XLogBlockHeadGetInfo(blockhead) & XLOG_UHEAP_INIT_PAGE) != 0;
    bool istoast = (XLogBlockHeadGetInfo(blockhead) & XLOG_UHEAP_INIT_TOAST_PAGE) != 0;
    TransactionId recordxid = XLogBlockHeadGetXid(blockhead);
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        Size blkdatalen;
        char *blkdata = NULL;
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
        Assert(blkdata != NULL);
        UHeapXlogInsertOperatorPage(bufferinfo, maindata, isinit, istoast, (void *)blkdata, blkdatalen, recordxid,
            NULL);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UHeapXlogDeleteBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    TransactionId recordxid = XLogBlockHeadGetXid(blockhead);
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        Size recordlen = datadecode->main_data_len;
        UHeapXlogDeleteOperatorPage(bufferinfo, (void *)maindata, recordlen, recordxid);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UHeapXlogUpdateBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    UpdateRedoBuffers buffers;
    char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
    XlUHeapUpdate *xlrec = (XlUHeapUpdate *)maindata;
    bool isinit = (XLogBlockHeadGetInfo(blockhead) & XLOG_UHEAP_INIT_PAGE) != 0;
    bool istoast = (XLogBlockHeadGetInfo(blockhead) & XLOG_UHEAP_INIT_TOAST_PAGE) != 0;
    bool inplaceUpdate = !(xlrec->flags & XLZ_NON_INPLACE_UPDATE);
    bool blockInplaceUpdate = (xlrec->flags & XLZ_BLOCK_INPLACE_UPDATE);
    TransactionId recordxid = XLogBlockHeadGetXid(blockhead);
    bool sameBlock = false;
    XLogRedoAction action;
    UpdateRedoAffixLens affixLens = {0, 0};
    UHeapTupleData oldtup;
    Size freespace = 0;
    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        if (XLogBlockDataGetBlockId(datadecode) == UHEAP_UPDATE_NEW_BLOCK_NUM) {
            buffers.newbuffer = *bufferinfo;
            Size blkdatalen;
            char *blkdata = NULL;
            BlockNumber oldblk = XLogBlockDataGetAuxiBlock1(datadecode);
            if (oldblk == bufferinfo->blockinfo.blkno) {
                sameBlock = true;
                buffers.oldbuffer.buf = buffers.newbuffer.buf;
                buffers.oldbuffer.pageinfo.page = buffers.newbuffer.pageinfo.page;
                buffers.oldbuffer.lsn = buffers.newbuffer.lsn;

                UHeapXlogUpdateOperatorOldpage(&buffers, (void *)maindata, inplaceUpdate,
                    blockInplaceUpdate, &oldtup, sameBlock, oldblk, recordxid);
            }

            blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
            Assert(blkdata != NULL);
            Size recordlen = datadecode->main_data_len;
            Size dataLen = datadecode->blockdata.data_len;
            freespace = UHeapXlogUpdateOperatorNewpage(&buffers, (void *)maindata, inplaceUpdate,
                blockInplaceUpdate, (void *)blkdata,  &oldtup, recordlen, dataLen, isinit,
                istoast, sameBlock, recordxid, &affixLens);
            /* may should free space */
            if (!inplaceUpdate && freespace < BLCKSZ / FREESPACE_FRACTION) {
                RelFileNode rnode;
                rnode.spcNode = blockhead->spcNode;
                rnode.dbNode = blockhead->dbNode;
                rnode.relNode = blockhead->relNode;
                rnode.bucketNode = blockhead->bucketNode;
                rnode.opt = blockhead->opt;
                XLogRecordPageWithFreeSpace(rnode, bufferinfo->blockinfo.blkno, freespace);
            }
        } else {
            buffers.oldbuffer = *bufferinfo;
            BlockNumber newblk = XLogBlockDataGetAuxiBlock1(datadecode);
            if (newblk == bufferinfo->blockinfo.blkno) {
                sameBlock = true;
                buffers.newbuffer.buf = buffers.oldbuffer.buf;
                buffers.newbuffer.pageinfo.page = buffers.oldbuffer.pageinfo.page;
            }

            UHeapXlogUpdateOperatorOldpage(&buffers, (void *)maindata, inplaceUpdate,
                blockInplaceUpdate, &oldtup, sameBlock, newblk, recordxid);
        }

        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UHeapXlogMultiInsertBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{   
    bool isinit = (XLogBlockHeadGetInfo(blockhead) & XLOG_UHEAP_INIT_PAGE) != 0;
    bool istoast = (XLogBlockHeadGetInfo(blockhead) & XLOG_UHEAP_INIT_TOAST_PAGE) != 0;
    TransactionId recordxid = XLogBlockHeadGetXid(blockhead);
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        Size blkdatalen;
        char *blkdata = NULL;
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
        Assert(blkdata != NULL);
        UHeapXlogMultiInsertOperatorPage(bufferinfo, maindata, isinit, istoast, (void *)blkdata, blkdatalen, recordxid,
            NULL);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UHeapXlogFreezeTDBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        UHeapXlogFreezeTDOperatorPage(bufferinfo, (void *)maindata);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UHeapXlogInvalidTDBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        UHeapXlogInvalidTDOperatorPage(bufferinfo, (void *)maindata);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UHeapXlogCleanBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        Size blkdatalen;
        char *blkdata = NULL;
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
        UHeapXlogCleanOperatorPage(bufferinfo, (void *)maindata, (void *)blkdata, blkdatalen);
        MakeRedoBufferDirty(bufferinfo);
    }
}

void UHeapRedoDataBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;

    switch (info & XLOG_UHEAP_OPMASK) {
        case XLOG_UHEAP_INSERT:
            UHeapXlogInsertBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UHEAP_DELETE:
            UHeapXlogDeleteBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UHEAP_UPDATE:
            UHeapXlogUpdateBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UHEAP_MULTI_INSERT:
            UHeapXlogMultiInsertBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UHEAP_FREEZE_TD_SLOT:
            UHeapXlogFreezeTDBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UHEAP_INVALID_TD_SLOT:
            UHeapXlogInvalidTDBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UHEAP_CLEAN:
            UHeapXlogCleanBlock(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("UHeapRedoDataBlock: unknown op code %u", info)));
    }
}

#ifdef ENABLE_MULTIPLE_NODES
const static bool SUPPORT_HOT_STANDBY = false; /* don't support consistency view */
#else
const static bool SUPPORT_HOT_STANDBY = true;
#endif

void UHeap2XlogFreezeOperatorPage(RedoBufferInfo *buffer, void *recorddata, void *blkdata, Size datalen)
{
    XlUHeapFreeze *xlrec = (XlUHeapFreeze *)recorddata;
    TransactionId cutoffXid = xlrec->cutoff_xid;
    Page page = buffer->pageinfo.page;

    OffsetNumber *offsets = (OffsetNumber *)recorddata;
    OffsetNumber *offsetsEnd = NULL;
    UHeapTupleData utuple;

    /*
     * In Hot Standby mode, ensure that there's no queries running which still
     * consider the frozen xids as running.
     */
    if (InHotStandby && SUPPORT_HOT_STANDBY) {
        ResolveRecoveryConflictWithSnapshot(cutoffXid, buffer->blockinfo.rnode);
    }

    if (datalen > 0) {
        offsetsEnd = (OffsetNumber *)((char *)offsets + datalen);

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
}

void UHeap2XlogExtendTDSlotsOperatorPage(RedoBufferInfo *buffer, void *recorddata)
{
    XLogRecPtr lsn = buffer->lsn;
    Page page = buffer->pageinfo.page;
    UHeapPageHeaderData *phdr = (UHeapPageHeaderData *)page;
    UHeapPageTDData *tdPtr = (UHeapPageTDData *)PageGetTDPointer(page);
    TD* thistrans;

    XlUHeapExtendTdSlots *xlrec = (XlUHeapExtendTdSlots *)recorddata;
    uint8 prevSlots = xlrec->nPrevSlots;
    uint8 extendedSlots = xlrec->nExtended;
    Assert(prevSlots == phdr->td_count);

    /*
     * Move the line pointers ahead in the page to make room for
     * added transaction slots.
     */
    char *start = page + SizeOfUHeapPageHeaderData + (prevSlots * sizeof(TD));
    char *end = page + phdr->pd_lower;
    size_t linePtrSize = end - start;

    errno_t ret = memmove_s((char*)start + (extendedSlots * sizeof(TD)), linePtrSize, start, linePtrSize);
    securec_check(ret, "", "");

    /*
     * Initialize the new transaction slots
     */
    for (int i = prevSlots; i < prevSlots + extendedSlots; i++) {
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
}

static void UHeap2XlogBaseShiftBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        XlUHeapBaseShift *xlrec = (XlUHeapBaseShift *)maindata;
        Page page = bufferinfo->pageinfo.page;

        UHeapPageShiftBase(InvalidBuffer, page, xlrec->multi, xlrec->delta);

        PageSetLSN(page, bufferinfo->lsn);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UHeap2XlogFreezeBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        Size blkdatalen;
        char *blkdata = NULL;
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);

        blkdata = XLogBlockDataGetBlockData(datadecode, &blkdatalen);
        UHeap2XlogFreezeOperatorPage(bufferinfo, (void *)maindata, (void *)blkdata, blkdatalen);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UHeap2XlogExtendTDSlotsBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        UHeap2XlogExtendTDSlotsOperatorPage(bufferinfo, (void *)maindata);
        MakeRedoBufferDirty(bufferinfo);
    }
}

void UHeap2RedoDataBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;

    switch (info & XLOG_UHEAP_OPMASK) {
        case XLOG_UHEAP2_BASE_SHIFT:
            UHeap2XlogBaseShiftBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UHEAP2_FREEZE:
            UHeap2XlogFreezeBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UHEAP2_EXTEND_TD_SLOTS:
            UHeap2XlogExtendTDSlotsBlock(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("UHeap2RedoDataBlock: unknown op code %u", info)));
    }
}

static void RedoUndoInsertBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    TransactionId recxid = blockdatarec->insertUndoParse.recxid;
    undo::XlogUndoMeta *xlundometa = &(blockdatarec->insertUndoParse.xlundometa);
    XlUndoHeader *xlundohdr = &(blockdatarec->insertUndoParse.xlundohdr);
    XlUndoHeaderExtra *xlundohdrextra = &(blockdatarec->insertUndoParse.xlundohdrextra);
    XLogRecPtr lsn = blockdatarec->insertUndoParse.lsn;
    UndoRecPtr urecptr = xlundohdr->urecptr;
    bool skipInsert = undo::IsSkipInsertUndo(urecptr);
    if (skipInsert) {
        xlundometa->SetInfo(XLOG_UNDOMETA_INFO_SKIP);
    }

    /* We need to pass in tablespace and relfilenode in PrepareUndo but we never explicitly
     * wrote those information in the xlundohdr because we can grab them from the XLOG record itself.
     */
    Oid relNode = blockdatarec->insertUndoParse.relNode;
    Oid spcNode = blockdatarec->insertUndoParse.spcNode;
    UndoRecord *undorec = u_sess->ustore_cxt.undo_records[0];
    undorec->SetUrp(urecptr);
    urecptr = UHeapPrepareUndoInsert(xlundohdr->relOid, xlundohdrextra->partitionOid, relNode, spcNode,
        UNDO_PERMANENT, InvalidBuffer, recxid, 0, xlundohdrextra->blkprev, xlundohdrextra->prevurp,
        blockdatarec->insertUndoParse.blkno, xlundohdr, xlundometa);
    Assert(urecptr == xlundohdr->urecptr);
    undorec->SetOffset(blockdatarec->insertUndoParse.offnum);
    if (!skipInsert) {
        InsertPreparedUndo(u_sess->ustore_cxt.urecvec, lsn);
    }

    XLogReaderState record;
    XLogRecord decodedRecord;
    decodedRecord.xl_xid = recxid;
    record.decoded_record = &decodedRecord;
    record.EndRecPtr = lsn;

    undo::RedoUndoMeta(&record, xlundometa, urecptr, u_sess->ustore_cxt.urecvec->LastRecord(), 
        u_sess->ustore_cxt.urecvec->LastRecordSize());
    UHeapResetPreparedUndo(UNDO_PERMANENT, UNDO_PTR_GET_ZONE_ID(urecptr));
}

static void RedoUndoDeleteBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    TransactionId recxid = blockdatarec->deleteUndoParse.recxid;
    undo::XlogUndoMeta *xlundometa = &(blockdatarec->deleteUndoParse.xlundometa);
    XlUndoHeader *xlundohdr = &(blockdatarec->deleteUndoParse.xlundohdr);
    XlUndoHeaderExtra *xlundohdrextra = &(blockdatarec->deleteUndoParse.xlundohdrextra);
    XLogRecPtr lsn = blockdatarec->deleteUndoParse.lsn;
    UndoRecPtr urecptr = xlundohdr->urecptr;
    ItemPointerData targetTid;
    bool skipInsert = undo::IsSkipInsertUndo(urecptr);
    if (skipInsert) {
        xlundometa->SetInfo(XLOG_UNDOMETA_INFO_SKIP);
    }

    /* recover undo record */
    UHeapTupleData utup;
    undo::XlogUndoMeta undometa;

    /* copy xlundometa to local struct */
    CopyUndoMeta(*xlundometa, undometa);

    /*
     * If the WAL stream contains undo tuple, then replace it with the
     * explicitly stored tuple.
     */
    Size datalen = blockdatarec->recordlen;
    char *data = blockdatarec->maindata;

    /*
     * If the WAL stream contains undo tuple, then replace it with the
     * explicitly stored tuple.
     */
    ItemPointerSetBlockNumber(&targetTid, blockdatarec->deleteUndoParse.blkno);
    ItemPointerSetOffsetNumber(&targetTid, ((XlUHeapDelete *)(blockdatarec->maindata))->offnum);
    TupleBuffer tbuf;
    errno_t rc = EOK;
    rc = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
    securec_check_ss(rc, "\0", "\0");
    utup.table_oid = xlundohdr->relOid;
    utup.ctid = targetTid;
    utup.disk_tuple = GetUHeapDiskTupleFromRedoData(data, &datalen, tbuf, false);
    utup.disk_tuple_size = datalen;

    TD oldTD;
    oldTD.xactid = blockdatarec->deleteUndoParse.oldxid;
    oldTD.undo_record_ptr = INVALID_UNDO_REC_PTR;
    UndoRecord *undorec = u_sess->ustore_cxt.undo_records[0];
    undorec->SetUrp(urecptr);

    /* We need to pass in tablespace and relfilenode in PrepareUndo but we never explicitly
     * wrote those information in the xlundohdr because we can grab them from the XLOG record itself.
     */
    Oid relNode = blockdatarec->deleteUndoParse.relNode;
    Oid spcNode = blockdatarec->deleteUndoParse.spcNode;

    urecptr = UHeapPrepareUndoDelete(xlundohdr->relOid, xlundohdrextra->partitionOid, relNode,
        spcNode, UNDO_PERMANENT, InvalidBuffer, blockdatarec->deleteUndoParse.offnum, recxid,
        xlundohdrextra->hasSubXact ? TopSubTransactionId : InvalidSubTransactionId, 0,
        xlundohdrextra->blkprev, xlundohdrextra->prevurp, &oldTD, &utup, blockdatarec->deleteUndoParse.blkno,
        xlundohdr, xlundometa);
    Assert(urecptr == xlundohdr->urecptr);
    undorec->SetOffset(blockdatarec->deleteUndoParse.offnum);
    if (!skipInsert) {
        /* Insert the Undo record into the undo store */
        InsertPreparedUndo(u_sess->ustore_cxt.urecvec, lsn);
    }

    XLogReaderState record;
    XLogRecord decodedRecord;
    decodedRecord.xl_xid = recxid;
    record.decoded_record = &decodedRecord;
    record.EndRecPtr = lsn;

    undo::RedoUndoMeta(&record, xlundometa, urecptr, u_sess->ustore_cxt.urecvec->LastRecord(),
        u_sess->ustore_cxt.urecvec->LastRecordSize());
    UHeapResetPreparedUndo(UNDO_PERMANENT, UNDO_PTR_GET_ZONE_ID(urecptr));
}

static void RedoUndoUpdateBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    TransactionId recxid = blockdatarec->updateUndoParse.recxid;
    XlUndoHeader *xlundohdr = &(blockdatarec->updateUndoParse.xlundohdr);
    XlUndoHeaderExtra *xlundohdrextra = &(blockdatarec->updateUndoParse.xlundohdrextra);
    XlUndoHeader *xlnewundohdr = &(blockdatarec->updateUndoParse.xlnewundohdr);
    XlUndoHeaderExtra *xlnewundohdrextra = &(blockdatarec->updateUndoParse.xlnewundohdrextra);
    undo::XlogUndoMeta *xlundometa = &(blockdatarec->updateUndoParse.xlundometa);

    XLogRecPtr lsn = blockdatarec->updateUndoParse.lsn;
    UndoRecPtr urecptr = xlundohdr->urecptr;
    UndoRecPtr newUrecptr = INVALID_UNDO_REC_PTR;
    bool inplaceUpdate = blockdatarec->updateUndoParse.inplaceUpdate;
    bool skipInsert = undo::IsSkipInsertUndo(urecptr);
    if (skipInsert) {
        xlundometa->SetInfo(XLOG_UNDOMETA_INFO_SKIP);
    }

    TupleBuffer tbuf;
    TD oldTD;
    oldTD.xactid = blockdatarec->updateUndoParse.oldxid;
    oldTD.undo_record_ptr = INVALID_UNDO_REC_PTR;
    UndoRecord *oldundorec = u_sess->ustore_cxt.undo_records[0];
    UndoRecord *newundorec = NULL;
    oldundorec->SetUrp(urecptr);
    errno_t rc = EOK;

    if (!inplaceUpdate) {
        newundorec = u_sess->ustore_cxt.undo_records[1];
        newundorec->SetUrp(xlnewundohdr->urecptr);
    }

    undo::XlogUndoMeta undometa;
    CopyUndoMeta(*xlundometa, undometa);

    UHeapTupleData oldtup;
    if (!inplaceUpdate) {
        char *data = blockdatarec->maindata;
        Size datalen = blockdatarec->recordlen;

        rc = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
        securec_check_ss(rc, "\0", "\0");

        oldtup.disk_tuple = GetUHeapDiskTupleFromRedoData(data, &datalen, tbuf, false);
        oldtup.disk_tuple_size = datalen;
    }

    ItemPointerData oldtid, newtid;
    ItemPointerSet(&oldtid, blockdatarec->updateUndoParse.oldblk, blockdatarec->updateUndoParse.old_offnum);
    ItemPointerSet(&newtid, blockdatarec->updateUndoParse.newblk, blockdatarec->updateUndoParse.new_offnum);

    /* Construct old tuple for undo record */
    oldtup.table_oid = xlundohdr->relOid;
    oldtup.ctid = oldtid;

    /* We need to pass in tablespace and relfilenode in PrepareUndo but we never explicitly
     * wrote those information in the xlundohdr because we can grab them from the XLOG record itself.
     */
    Oid relNode = blockdatarec->updateUndoParse.relNode;
    Oid spcNode = blockdatarec->updateUndoParse.spcNode;

    urecptr = UHeapPrepareUndoUpdate(xlundohdr->relOid, xlundohdrextra->partitionOid, relNode,
        spcNode, UNDO_PERMANENT, InvalidBuffer, InvalidBuffer, blockdatarec->updateUndoParse.old_offnum,
        recxid, xlundohdrextra->hasSubXact ? TopSubTransactionId : InvalidSubTransactionId, 0, xlundohdrextra->blkprev,
        inplaceUpdate ? xlundohdrextra->blkprev : xlnewundohdrextra->blkprev, xlundohdrextra->prevurp,
        &oldTD, &oldtup, inplaceUpdate, &newUrecptr, blockdatarec->updateUndoParse.undoXorDeltaSize,
        blockdatarec->updateUndoParse.oldblk, blockdatarec->updateUndoParse.newblk, xlundohdr, xlundometa);
    Assert(urecptr == xlundohdr->urecptr);

    if (!skipInsert) {
        if (!inplaceUpdate) {
            newundorec->SetOffset(blockdatarec->updateUndoParse.new_offnum);
            appendBinaryStringInfo(oldundorec->Rawdata(), (char *)&newtid, sizeof(ItemPointerData));
        } else {
            UndoRecord *undorec = (*u_sess->ustore_cxt.urecvec)[0];
            appendBinaryStringInfo(undorec->Rawdata(), 
                blockdatarec->updateUndoParse.xlogXorDelta, 
                blockdatarec->updateUndoParse.undoXorDeltaSize);
        }

        if (xlundohdrextra->hasSubXact) {
            SubTransactionId subxid = TopSubTransactionId;

            oldundorec->SetUinfo(UNDO_UREC_INFO_CONTAINS_SUBXACT);
            appendBinaryStringInfo(oldundorec->Rawdata(), (char *)&subxid, sizeof(SubTransactionId));
        }

        /* Insert the Undo record into the undo store */
        InsertPreparedUndo(u_sess->ustore_cxt.urecvec, lsn);
    }

    XLogReaderState record;
    XLogRecord decodedRecord;
    decodedRecord.xl_xid = recxid;
    record.decoded_record = &decodedRecord;
    record.EndRecPtr = lsn;

    undo::RedoUndoMeta(&record, xlundometa, urecptr, u_sess->ustore_cxt.urecvec->LastRecord(),
        u_sess->ustore_cxt.urecvec->LastRecordSize());
    UHeapResetPreparedUndo(UNDO_PERMANENT, UNDO_PTR_GET_ZONE_ID(urecptr));
}

static void RedoUndoMultiInsertBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{   
    TransactionId recxid = blockdatarec->multiInsertUndoParse.recxid;
    XLogRecPtr lsn = blockdatarec->multiInsertUndoParse.lsn;

    XlUndoHeader *xlundohdr = &(blockdatarec->multiInsertUndoParse.xlundohdr);
    XlUndoHeaderExtra *xlundohdrextra = &(blockdatarec->multiInsertUndoParse.xlundohdrextra);
    UndoRecPtr urecptr = blockdatarec->multiInsertUndoParse.last_urecptr;
    undo::XlogUndoMeta *xlundometa = &(blockdatarec->multiInsertUndoParse.xlundometa);

    /* copy xlundometa to local struct */
    undo::XlogUndoMeta undometa;
    CopyUndoMeta(*xlundometa, undometa);

    XlUHeapMultiInsert *xlrec = (XlUHeapMultiInsert *)blockdatarec->maindata;
    char *curxlogptr = (char *)xlrec + SizeOfUHeapMultiInsert;

    UHeapFreeOffsetRanges *ufreeOffsetRanges = NULL;
    URecVector *urecvec = NULL;
    UndoRecPtr *urpvec = NULL;

    /* fetch number of distinct ranges */
    int nranges = GetOffsetRangesForMultiInsert(&ufreeOffsetRanges, curxlogptr, &urpvec);

    bool skipInsert = undo::IsSkipInsertUndo(urecptr);
    bool skipUndo = blockdatarec->multiInsertUndoParse.skipUndo;
    if (skipInsert) {
        xlundometa->SetInfo(XLOG_UNDOMETA_INFO_SKIP);
    }

    /* We need to pass in tablespace and relfilenode in PrepareUndo but we never explicitly
     * wrote those information in the xlundohdr because we can grab them from the XLOG record itself.
     */
    Oid relNode = blockdatarec->multiInsertUndoParse.relNode;
    Oid spcNode = blockdatarec->multiInsertUndoParse.spcNode;

    /* pass first undo record in and let UHeapPrepareUndoMultiInsert set urecptr according to xlog */
    urecptr = UHeapPrepareUndoMultiInsert(xlundohdr->relOid, xlundohdrextra->partitionOid, relNode,
        spcNode, UNDO_PERMANENT, InvalidBuffer, nranges, recxid, InvalidCommandId, xlundohdrextra->blkprev,
        xlundohdrextra->prevurp, &urecvec, NULL, urpvec, 
        blockdatarec->multiInsertUndoParse.blkno, xlundohdr, xlundometa);

    /*
     * We can skip inserting undo records if the tuples are to be marked as
     * frozen.
     */
    elog(LOG, "Undo record prepared: %d for Block Number: %d", nranges, blockdatarec->multiInsertUndoParse.blkno);
    if (!skipUndo && !skipInsert) {
        for (int i = 0; i < nranges; i++) {
            initStringInfo((*urecvec)[i]->Rawdata());
            appendBinaryStringInfo((*urecvec)[i]->Rawdata(), (char *)&(ufreeOffsetRanges)->startOffset[i],
                sizeof(OffsetNumber));
            appendBinaryStringInfo((*urecvec)[i]->Rawdata(), (char *)&(ufreeOffsetRanges)->endOffset[i],
                sizeof(OffsetNumber));
        }

        /*
         * undo should be inserted at same location as it was during the
         * actual insert (DO operation).
         */
        Assert((*urecvec)[0]->Urp() == xlundohdr->urecptr);
        InsertPreparedUndo(urecvec, lsn);
    }

    XLogReaderState record;
    XLogRecord decodedRecord;
    decodedRecord.xl_xid = recxid;
    record.decoded_record = &decodedRecord;
    record.EndRecPtr = lsn;

    undo::RedoUndoMeta(&record, &undometa, xlundohdr->urecptr, urecvec->LastRecord(), urecvec->LastRecordSize());
    UHeapResetPreparedUndo(UNDO_PERMANENT, UNDO_PTR_GET_ZONE_ID(urecptr));
    urecvec->Destroy();
    pfree(ufreeOffsetRanges);
}

void RedoUHeapUndoBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;

    switch (info & XLOG_UHEAP_OPMASK) {
        case XLOG_UHEAP_INSERT:
            RedoUndoInsertBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UHEAP_DELETE:
            RedoUndoDeleteBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UHEAP_UPDATE:
            RedoUndoUpdateBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UHEAP_MULTI_INSERT:
            RedoUndoMultiInsertBlock(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("RedoUHeapUndoBlock: unknown op code %u", info)));
    }
}

static void RedoUndoDiscardBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    int zoneId = blockdatarec->undoDiscardParse.zoneId;
    UndoSlotPtr endSlot = blockdatarec->undoDiscardParse.endSlot;
    UndoSlotPtr startSlot = blockdatarec->undoDiscardParse.startSlot;
    UndoRecPtr endUndoPtr = blockdatarec->undoDiscardParse.endUndoPtr;
    TransactionId recycledXid = blockdatarec->undoDiscardParse.recycledXid;
    XLogRecPtr lsn = blockdatarec->undoDiscardParse.lsn;

    UndoZone *zone = UndoZoneGroup::GetUndoZone(zoneId);
    if (zone == NULL) {
        return;
    }
    if (zone->GetLSN() < lsn) {
        zone->LockUndoZone();
        Assert(startSlot == zone->GetRecycle());
        zone->ForgetUndoBuffer(UNDO_PTR_GET_OFFSET(zone->GetForceDiscard()),
            UNDO_PTR_GET_OFFSET(endUndoPtr), UNDO_DB_OID);
        zone->ForgetUndoBuffer(UNDO_PTR_GET_OFFSET(startSlot),
            UNDO_PTR_GET_OFFSET(endSlot), UNDO_SLOT_DB_OID);
        zone->SetRecycle(endSlot);
        zone->SetDiscard(endUndoPtr);
        zone->SetForceDiscard(endUndoPtr);
        zone->SetRecycleXid(recycledXid);
        zone->MarkDirty();
        zone->SetLSN(lsn);
        zone->UnlockUndoZone();
    }
    return;
}

static void RedoUndoUnlinkBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    int zoneId = blockdatarec->undoUnlinkParse.zoneId;
    Assert(IS_VALID_ZONE_ID(zoneId));
    UndoZone *zone = UndoZoneGroup::GetUndoZone(zoneId);
    if (zone == NULL) {
        return;
    }
    UndoSpace *usp = zone->GetUndoSpace();
    XLogRecPtr unlinkLsn = blockdatarec->undoUnlinkParse.unlinkLsn;
    UndoLogOffset newHead = blockdatarec->undoUnlinkParse.headOffset;
    UndoLogOffset head = usp->Head();

    if (usp->LSN() < unlinkLsn) {
        zone->ForgetUndoBuffer(head, newHead, UNDO_DB_OID);
        usp->LockSpace();
        usp->MarkDirty();
        usp->UnlinkUndoLog(zoneId, newHead, UNDO_DB_OID);
        usp->SetLSN(unlinkLsn);
        usp->UnlockSpace();
    }
}

static void RedoSlotUnlinkBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    int zoneId = blockdatarec->undoUnlinkParse.zoneId;
    Assert(IS_VALID_ZONE_ID(zoneId));
    UndoZone *zone = UndoZoneGroup::GetUndoZone(zoneId);
    if (zone == NULL) {
        return;
    }
    UndoSpace *usp = zone->GetSlotSpace();
    XLogRecPtr unlinkLsn = blockdatarec->undoUnlinkParse.unlinkLsn;
    UndoLogOffset newHead = blockdatarec->undoUnlinkParse.headOffset;
    UndoLogOffset head = usp->Head();

    if (usp->LSN() < unlinkLsn) {
        zone->ForgetUndoBuffer(head, newHead, UNDO_SLOT_DB_OID);
        usp->LockSpace();
        usp->MarkDirty();
        usp->UnlinkUndoLog(zoneId, newHead, UNDO_SLOT_DB_OID);
        usp->SetLSN(unlinkLsn);
        usp->UnlockSpace();
    }
}

static void RedoUndoExtendBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    int zoneId = blockdatarec->undoExtendParse.zoneId;
    Assert(IS_VALID_ZONE_ID(zoneId));
    XLogRecPtr extendLsn = blockdatarec->undoExtendParse.extendLsn;
    UndoLogOffset newTail = blockdatarec->undoExtendParse.tailOffset;
    UndoZone *zone = UndoZoneGroup::GetUndoZone(zoneId);
    if (zone == NULL) {
        return;
    }
    UndoSpace *usp = zone->GetUndoSpace();
    if (usp->LSN() < extendLsn) {
        usp->LockSpace();
        usp->MarkDirty();
        usp->ExtendUndoLog(zoneId, newTail, UNDO_DB_OID);
        usp->SetLSN(extendLsn);
        usp->UnlockSpace();
    }
}

static void RedoSlotExtendBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    int zoneId = blockdatarec->undoExtendParse.zoneId;
    Assert(IS_VALID_ZONE_ID(zoneId));
    XLogRecPtr extendLsn = blockdatarec->undoExtendParse.extendLsn;
    UndoLogOffset newTail = blockdatarec->undoExtendParse.tailOffset;
    UndoZone *zone = UndoZoneGroup::GetUndoZone(zoneId);
    if (zone == NULL) {
        return;
    }
    UndoSpace *usp = zone->GetSlotSpace();
    if (usp->LSN() < extendLsn) {
        usp->LockSpace();
        usp->MarkDirty();
        usp->ExtendUndoLog(zoneId, newTail, UNDO_SLOT_DB_OID);
        usp->SetLSN(extendLsn);
        usp->UnlockSpace();
    }
}

static void RedoUndoCleanBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    int zoneId = blockdatarec->undoCleanParse.zoneId;
    Assert(IS_VALID_ZONE_ID(zoneId));
    XLogRecPtr cleanLsn = blockdatarec->undoCleanParse.cleanLsn;
    UndoLogOffset newTail = blockdatarec->undoCleanParse.tailOffset;
    UndoZone *zone = UndoZoneGroup::GetUndoZone(zoneId);
    if (zone == NULL) {
        return;
    }
    UndoSpace *usp = zone->GetUndoSpace();
    if (usp->LSN() < cleanLsn) {
        usp->LockSpace();
        if ((usp->Head() + UNDO_LOG_SEGMENT_SIZE != usp->Tail()) || (usp->Head() != newTail)) {
            ereport(PANIC, (errmsg(UNDOFORMAT("zone %d not used but head %lu + segment size != tail %lu, newTail=%lu."),
                zoneId, usp->Head(), usp->Tail(), newTail)));
        }
        usp->MarkDirty();
        usp->UnlinkUndoLog(zoneId, usp->Tail(), UNDO_DB_OID);
        usp->SetHead(newTail);
        usp->SetTail(newTail);
        usp->SetLSN(cleanLsn);
        usp->UnlockSpace();
    }
}

static void RedoSlotCleanBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    int zoneId = blockdatarec->undoCleanParse.zoneId;
    Assert(IS_VALID_ZONE_ID(zoneId));
    XLogRecPtr cleanLsn = blockdatarec->undoCleanParse.cleanLsn;
    UndoLogOffset newTail = blockdatarec->undoCleanParse.tailOffset;
    UndoZone *zone = UndoZoneGroup::GetUndoZone(zoneId);
    if (zone == NULL) {
        return;
    }
    UndoSpace *usp = zone->GetSlotSpace();
    if (usp->LSN() < cleanLsn) {
        usp->LockSpace();
        if ((usp->Head() + UNDO_META_SEGMENT_SIZE != usp->Tail()) || (usp->Head() != newTail)) {
            ereport(PANIC, (errmsg(UNDOFORMAT("zone %d not used but head %lu + segment size != tail %lu, newTail=%lu."),
                zoneId, usp->Head(), usp->Tail(), newTail)));
        }
        usp->MarkDirty();
        usp->UnlinkUndoLog(zoneId, usp->Tail(), UNDO_SLOT_DB_OID);
        usp->SetHead(newTail);
        usp->SetTail(newTail);
        usp->SetLSN(cleanLsn);
        usp->UnlockSpace();
    }
}

void RedoUndoBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;
    
    switch (info & XLOG_UHEAP_OPMASK) {
        case XLOG_UNDO_DISCARD:
            RedoUndoDiscardBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UNDO_UNLINK:
            RedoUndoUnlinkBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_SLOT_UNLINK:
            RedoSlotUnlinkBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UNDO_EXTEND:
            RedoUndoExtendBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_SLOT_EXTEND:
            RedoSlotExtendBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UNDO_CLEAN:
            RedoUndoCleanBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_SLOT_CLEAN:
            RedoSlotCleanBlock(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("RedoUndoBlock: unknown op code %u", info)));
    }
}

static void UHeapUndoXlogPageOperatorRestorePage(char *curxlogptr, RedoBufferInfo *buffer)
{
    Page page = buffer->pageinfo.page;

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

    UHeapPageSetUndo(buffer->buf, *tdSlotId, *xid, *slotPrevUrp);
}

static void UHeapUndoXlogPageOperatorPage(RedoBufferInfo *buffer, void *recorddata)
{
    uint8 *flags = (uint8 *)recorddata;
    Page page = buffer->pageinfo.page;
    char *curxlogptr = (char *)((char *)flags + sizeof(uint8));

    if (*flags & XLU_INIT_PAGE) {
        TransactionId *xidBase = (TransactionId *)curxlogptr;
        curxlogptr += sizeof(TransactionId);
        uint16 *tdCount = (uint16 *)curxlogptr;
        curxlogptr += sizeof(uint16);

        if (*flags & XLOG_UHEAP_INIT_TOAST_PAGE) {
            UPageInit<UPAGE_TOAST>(page, buffer->pageinfo.pagesize, UHEAP_SPECIAL_SIZE);
        } else {
            UPageInit<UPAGE_HEAP>(page, buffer->pageinfo.pagesize, UHEAP_SPECIAL_SIZE, *tdCount);
        }

        UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
        uheappage->pd_xid_base = *xidBase;
        uheappage->pd_multi_base = 0;
    } else {
        /* Restore Rolledback Page */
        UHeapUndoXlogPageOperatorRestorePage(curxlogptr, buffer);
    }

    PageSetLSN(page, buffer->lsn);
}

void UHeapUndoXlogAbortSpecInsertOperatorPage(RedoBufferInfo *buffer, void *recorddata, TransactionId recordxid)
{
    uint8 *flags = (uint8 *)recorddata;
    XLogRecPtr lsn = buffer->lsn;
    Buffer buf = buffer->buf;
    Page page = buffer->pageinfo.page;
    XlUHeapUndoAbortSpecInsert *xlrec = (XlUHeapUndoAbortSpecInsert *)((char *)flags + sizeof(uint8));

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

    ExecuteUndoForInsertRecovery(buf, xlrec->offset, recordxid, relhasindex, &tdSlot);

    UHeapPageSetUndo(buf, tdSlot, (xid == NULL) ? InvalidTransactionId : *xid,
        (prevurp == NULL) ? INVALID_UNDO_REC_PTR : *prevurp);

    if (isPageInit) {
        UPageInit<UPAGE_HEAP>(page, buffer->pageinfo.pagesize, UHEAP_SPECIAL_SIZE, *tdCount);
        UHeapPageHeaderData *uheappage = (UHeapPageHeaderData *)page;
        uheappage->pd_xid_base = *xidbase;
        uheappage->pd_multi_base = 0;
    }

    PageSetLSN(page, lsn);
}

static void UHeapUndoXlogResetSlotBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        XlUHeapUndoResetSlot *xlrec = (XlUHeapUndoResetSlot *)maindata;

        UHeapPageSetUndo(bufferinfo->buf, xlrec->td_slot_id, InvalidTransactionId, xlrec->urec_ptr);

        PageSetLSN(bufferinfo->pageinfo.page, bufferinfo->lsn);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UHeapUndoXlogPageBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        UHeapUndoXlogPageOperatorPage(bufferinfo, (void *)maindata);
        MakeRedoBufferDirty(bufferinfo);
    }
}

static void UHeapUndoXlogAbortSpecInsertBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec,
    RedoBufferInfo *bufferinfo)
{
    XLogBlockDataParse *datadecode = blockdatarec;
    XLogRedoAction action;

    action = XLogCheckBlockDataRedoAction(datadecode, bufferinfo);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogBlockDataGetMainData(datadecode, NULL);
        TransactionId recordxid = XLogBlockHeadGetXid(blockhead);
        UHeapUndoXlogAbortSpecInsertOperatorPage(bufferinfo, (void *)maindata, recordxid);
        MakeRedoBufferDirty(bufferinfo);
    }
}

void RedoUndoActionBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;
    
    switch (info & XLOG_UHEAP_OPMASK) {
        case XLOG_UHEAPUNDO_RESET_SLOT:
            UHeapUndoXlogResetSlotBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UHEAPUNDO_PAGE:
            UHeapUndoXlogPageBlock(blockhead, blockdatarec, bufferinfo);
            break;
        case XLOG_UHEAPUNDO_ABORT_SPECINSERT:
            UHeapUndoXlogAbortSpecInsertBlock(blockhead, blockdatarec, bufferinfo);
            break;
        default:
            ereport(PANIC, (errmsg("RedoUndoActionBlock: unknown op code %u", info)));
    }
}

void RedoRollbackFinishBlock(XLogBlockHead *blockhead, XLogBlockUndoParse *blockdatarec, RedoBufferInfo *bufferinfo)
{ 
    uint8 info = XLogBlockHeadGetInfo(blockhead) & ~XLR_INFO_MASK;
    UndoSlotPtr slotPtr = blockdatarec->rollbackFinishParse.slotPtr;
    XLogRecPtr lsn = blockdatarec->rollbackFinishParse.lsn;

    if ((info & XLOG_UHEAP_OPMASK) != XLOG_ROLLBACK_FINISH) {
        ereport(PANIC, (errmsg("RedoRollbackBlock: unknown op code %u", info)));
    }
    if (!IsSkipInsertSlot(slotPtr)) {
        UndoSlotBuffer buf;
        buf.PrepareTransactionSlot(slotPtr);
        LockBuffer(buf.Buf(), BUFFER_LOCK_EXCLUSIVE);
        Page page = BufferGetPage(buf.Buf());
        if (PageGetLSN(page) < lsn) {
            TransactionSlot *slot = buf.FetchTransactionSlot(slotPtr);
            slot->UpdateRollbackProgress();
            MarkBufferDirty(buf.Buf());
            PageSetLSN(page, lsn);
        }
        UnlockReleaseBuffer(buf.Buf());
    }
    return;
}
