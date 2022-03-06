/* -------------------------------------------------------------------------
 *
 * uheapdesc.cpp
 *     rmgr descriptor routines for src/gausskernel/storage/access/ustore/knl_uredo.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/rmgrdesc/uheapdesc.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/ustore/knl_uredo.h"
#include "access/ustore/undo/knl_uundoxlog.h"
#include "access/ustore/undo/knl_uundotxn.h"
#include "access/xlog.h"
#include "access/ustore/knl_upage.h"

char *GetUndoHeader(XlUndoHeader *xlundohdr, Oid *partitionOid, UndoRecPtr *blkprev, UndoRecPtr *prevUrp,
                    TransactionId *subXid)
{
    Assert(xlundohdr != NULL && partitionOid != NULL && blkprev != NULL && prevUrp != NULL && subXid != NULL);
    char *currLogPtr = ((char *)xlundohdr + SizeOfXLUndoHeader);

    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_SUB_XACT) != 0) {
        currLogPtr += sizeof(bool);
    }

    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_BLK_PREV) != 0) {
        *blkprev = *(UndoRecPtr *) ((char *)currLogPtr);
        Assert(*blkprev != INVALID_UNDO_REC_PTR);
        currLogPtr += sizeof(UndoRecPtr);
    }

    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PREV_URP) != 0) {
        *prevUrp = *(UndoRecPtr *) ((char *)currLogPtr);
        currLogPtr += sizeof(UndoRecPtr);
    }

    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_PARTITION_OID) != 0) {
        *partitionOid = *(Oid *) ((char *)currLogPtr);
        currLogPtr += sizeof(Oid);
    }

    if ((xlundohdr->flag & XLOG_UNDO_HEADER_HAS_CURRENT_XID) != 0) {
        *subXid = *(TransactionId *) ((char *)currLogPtr);
        currLogPtr += sizeof(TransactionId);
    }

    return currLogPtr;
}

const char* uheap_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    info &= XLOG_UHEAP_OPMASK;
    switch (info) {
        case XLOG_UHEAP_INSERT:
            return "unheap_insert";
            break;
        case XLOG_UHEAP_DELETE:
            return "unheap_delete";
            break;
        case XLOG_UHEAP_UPDATE:
            return "unheap_update";
            break;
        case XLOG_UHEAP_FREEZE_TD_SLOT:
            return "unheap_freeze";
            break;
        case XLOG_UHEAP_INVALID_TD_SLOT:
            return "unheap_invalid_slot";
            break;
        case XLOG_UHEAP_CLEAN:
            return "unheap_clean";
            break;
        case XLOG_UHEAP_MULTI_INSERT:
            return "unheap_multi_insert";
            break;
        default:
            return "unknown_type";
            break;
    }
}


/*
 * For pg_xlogdump to dump out xlog info
 */
void UHeapDesc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    TransactionId xid = XLogRecGetXid(record);
    TransactionId subXid = InvalidTransactionId;
    Oid partitionOid = InvalidOid;
    UndoRecPtr blkprev = INVALID_UNDO_REC_PTR;
    UndoRecPtr prevUrp = INVALID_UNDO_REC_PTR;
    char *currLogPtr;

    info &= XLOG_UHEAP_OPMASK;
    switch (info) {
        case XLOG_UHEAP_INSERT: {
            XlUHeapInsert *xlrec = (XlUHeapInsert *)rec;
            bool isInit = (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_PAGE) != 0;
            if (isInit) {
                appendStringInfo(buf, "XLOG_UHEAP_INSERT insert(init): ");
            } else {
                appendStringInfo(buf, "XLOG_UHEAP_INSERT insert: ");
            }
            appendStringInfo(buf, "off %u, flag %u. ", (uint16)xlrec->offnum, (uint8)xlrec->flags);
            XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)rec + SizeOfUHeapInsert);
            currLogPtr = GetUndoHeader(xlundohdr, &partitionOid, &blkprev, &prevUrp, &subXid);
            appendStringInfo(buf,
                "urecptr %016lx, blkprev %016lx, prevurp %016lx, relOid %u, partitionOid %u, flag %u, subXid %lu. ",
                xlundohdr->urecptr, blkprev, prevUrp, xlundohdr->relOid, partitionOid, xlundohdr->flag, subXid);
            undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)((char *)currLogPtr);

            if (isInit) {
                TransactionId *xidBase = (TransactionId *)((char *)xlundometa + xlundometa->Size());
                uint16 *tdCount = (uint16 *)((char *)xidBase + sizeof(TransactionId));
                appendStringInfo(buf, "xidBase %lu tdCount %u. ", *xidBase, *tdCount);
            }

            appendStringInfo(buf,
                "zone %d slot offset %lu: dbid %u, xid %lu, lastrecsize %u, allocate %d switch zone %d.",
                (int)UNDO_PTR_GET_ZONE_ID(xlundohdr->urecptr), xlundometa->slotPtr, xlundometa->dbid,
                xid, xlundometa->lastRecordSize, xlundometa->IsTranslot(), xlundometa->IsSwitchZone());
            break;
        }
        case XLOG_UHEAP_MULTI_INSERT: {
            XlUndoHeader *xlundohdr = (XlUndoHeader *)rec;
            char *curxlogptr = (char *)xlundohdr + SizeOfXLUndoHeader;
            curxlogptr = GetUndoHeader(xlundohdr, &partitionOid, &blkprev, &prevUrp, &subXid);

            UndoRecPtr *last_urecptr = (UndoRecPtr *)curxlogptr;
            curxlogptr = (char *)last_urecptr + sizeof(*last_urecptr);

            undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)curxlogptr;
            curxlogptr = (char *)xlundometa + xlundometa->Size();
            
            bool isinit = (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_PAGE) != 0;

            TransactionId *xidBase = NULL;
            uint16 *tdCount = NULL;
            if (isinit) {
                xidBase = (TransactionId *)curxlogptr;
                curxlogptr += sizeof(TransactionId);
                tdCount = (uint16 *)curxlogptr;
                curxlogptr += sizeof(uint16);
            }

            XlUHeapMultiInsert *xlrec = (XlUHeapMultiInsert *)((char *)curxlogptr);
            curxlogptr = (char *)xlrec + SizeOfUHeapMultiInsert;
            int nranges = *(int *)curxlogptr;

            if (isinit) {
                appendStringInfo(buf, "XLOG_UHEAP_MULTI_INSERT (init): ");
            } else {
                appendStringInfo(buf, "XLOG_UHEAP_MULTI_INSERT : ");
            }
            appendStringInfo(buf, "ntuples %u, flag %u, nranges %d. ", (int)xlrec->ntuples, (uint8)xlrec->flags,
                nranges);
            appendStringInfo(buf, "urecptr %016lx, blkprev %016lx, prevurp %016lx, last_urecptr %016lx, subXid %lu,",
                xlundohdr->urecptr, blkprev, prevUrp, *last_urecptr, subXid);
            appendStringInfo(buf, "relOid %u, partitionOid %u, flag %u. ",
                xlundohdr->relOid, partitionOid, xlundohdr->flag);

            if (isinit) {
                appendStringInfo(buf, "xidBase %lu tdCount %u. ", *xidBase, *tdCount);
            }

            appendStringInfo(buf,
                "zone %d slot offset %lu: dbid %u, xid %lu, lastrecsize %u, allocate %d switch zone %d.",
                (int)UNDO_PTR_GET_ZONE_ID(xlundohdr->urecptr), xlundometa->slotPtr, xlundometa->dbid,
                xid, xlundometa->lastRecordSize, xlundometa->IsTranslot(), xlundometa->IsSwitchZone());
            break;
        }
        case XLOG_UHEAP_DELETE: {
            XlUHeapDelete *xlrec = (XlUHeapDelete *)rec;
            appendStringInfo(buf, "XLOG_UHEAP_DELETE: ");
            appendStringInfo(buf, "oldxid %lu, off %u, td_id %u, flag %u. ", xlrec->oldxid,
                (uint16)xlrec->offnum, (uint8)xlrec->td_id, (uint8)xlrec->flag);

            XlUndoHeader *xlundohdr = (XlUndoHeader *)((char *)rec + SizeOfUHeapDelete);
            currLogPtr = GetUndoHeader(xlundohdr, &partitionOid, &blkprev, &prevUrp, &subXid);
            appendStringInfo(buf,
                "urecptr %016lx, blkprev %016lx, prevurp %016lx, relOid %u, partitionOid %u, subXid %lu. ",
                xlundohdr->urecptr, blkprev, prevUrp, xlundohdr->relOid, partitionOid, subXid);
            undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)currLogPtr;
            appendStringInfo(buf,
                "zone %d slot offset %lu: dbid %u, xid %lu, lastrecsize %u, allocate %d switch zone %d.",
                (int)UNDO_PTR_GET_ZONE_ID(xlundohdr->urecptr), xlundometa->slotPtr, xlundometa->dbid,
                xid, xlundometa->lastRecordSize, xlundometa->IsTranslot(), xlundometa->IsSwitchZone());
            break;
        }
        case XLOG_UHEAP_UPDATE: {
            Size datalen;
            XlUHeapHeader xlhdr;
            errno_t rc;
            char *recdata = XLogRecGetBlockData(record, 0, &datalen);
            char *recdataEnd = recdata + datalen;

            XlUndoHeader *xlundohdr = NULL;
            XlUHeapUpdate *xlrec = (XlUHeapUpdate *)rec;
            appendStringInfo(buf, "XLOG_UHEAP_UPDATE: ");
            appendStringInfo(buf,
                "oldxid %lu, Old offset %u, New offset %u, old_tuple_td_id %u, old_tuple_flag %u. ",
                xlrec->oldxid, (uint16)xlrec->old_offnum, (uint16)xlrec->new_offnum,
                (uint8)xlrec->old_tuple_td_id, (uint16)xlrec->old_tuple_flag);
            xlundohdr = (XlUndoHeader *)((char *)rec + SizeOfUHeapUpdate);
            currLogPtr = GetUndoHeader(xlundohdr, &partitionOid, &blkprev, &prevUrp, &subXid);
            appendStringInfo(buf,
                "urecptr %016lx, blkprev %016lx, prevurp %016lx, relOid %u, partitionOid %u, flag %u, subXid %lu. ",
                xlundohdr->urecptr, blkprev, prevUrp, xlundohdr->relOid, partitionOid, xlundohdr->flag, subXid);

            if (xlrec->flags & XLZ_NON_INPLACE_UPDATE) {
                appendStringInfo(buf, "NON_INPLACE_UPDATE. ");
                xlundohdr = (XlUndoHeader *)((char *)currLogPtr);
                currLogPtr = GetUndoHeader(xlundohdr, &partitionOid, &blkprev, &prevUrp, &subXid);
                appendStringInfo(buf,
                    "relOid %u, urecptr %016lx, blkprev %016lx, prevurp %016lx, newflag %u, subXid %lu,",
                    xlundohdr->relOid, xlundohdr->urecptr, blkprev, prevUrp, xlundohdr->flag, subXid);
            } else if (xlrec->flags & XLZ_BLOCK_INPLACE_UPDATE) {
                appendStringInfo(buf, "BLOCK_INPLACE_UPDATE. ");
            } else {
                appendStringInfo(buf, "INPLACE_UPDATE. ");
            }

            undo::XlogUndoMeta *xlundometa = (undo::XlogUndoMeta *)currLogPtr;
            appendStringInfo(buf,
                "zone %d slot offset %lu: dbid %u, xid %lu, lastrecsize %u, allocate %d switch zone %d.",
                (int)UNDO_PTR_GET_ZONE_ID(xlundohdr->urecptr), xlundometa->slotPtr, xlundometa->dbid,
                xid, xlundometa->lastRecordSize, xlundometa->IsTranslot(), xlundometa->IsSwitchZone());
            currLogPtr = currLogPtr + xlundometa->Size();

            if (!(xlrec->flags & XLZ_NON_INPLACE_UPDATE)) {
                int *undoXorDeltaSizePtr = (int *)currLogPtr;
                int undoXorDeltaSize = *undoXorDeltaSizePtr;
                currLogPtr += sizeof(int);
                char *xorCurxlogptr = currLogPtr;
                currLogPtr += undoXorDeltaSize;
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
                    appendStringInfo(buf, "prefixlen %u ", *prefixlenPtr);
                }
                if (flags & UREC_INPLACE_UPDATE_XOR_SUFFIX) {
                    uint16 *suffixlenPtr = (uint16 *)(xorCurxlogptr);
                    xorCurxlogptr += sizeof(uint16);
                    appendStringInfo(buf, "suffixlen %u ", *suffixlenPtr);
                }
            } else {
                if (XLogRecGetInfo(record) & XLOG_UHEAP_INIT_PAGE) {
                    TransactionId *xidBase = (TransactionId *)currLogPtr;
                    currLogPtr += sizeof(TransactionId);
                    uint16 *tdCount = (uint16 *)currLogPtr;
                    currLogPtr += sizeof(uint16);
                    appendStringInfo(buf, "xidBase %lu tdCount %u. ", *xidBase, *tdCount);
                }

                if (xlrec->flags & XLZ_UPDATE_PREFIX_FROM_OLD) {
                    uint16 prefixlen = 0;
                    rc = memcpy_s(&prefixlen, sizeof(uint16), recdata, sizeof(uint16));
                    securec_check(rc, "\0", "\0");
                    recdata += sizeof(uint16);
                    appendStringInfo(buf, "prefixlen %u ", prefixlen);
                }

                if (xlrec->flags & XLZ_UPDATE_SUFFIX_FROM_OLD) {
                    uint16 suffixlen = 0;
                    rc = memcpy_s(&suffixlen, sizeof(uint16), recdata, sizeof(uint16));
                    securec_check(rc, "\0", "\0");
                    recdata += sizeof(uint16);
                    appendStringInfo(buf, "suffixlen %u ", suffixlen);
                }
            }

            rc = memcpy_s((char *)&xlhdr, SizeOfUHeapHeader, recdata, SizeOfUHeapHeader);
            securec_check(rc, "\0", "\0");
            recdata += SizeOfUHeapHeader;

            Size len = (recdataEnd - recdata) - (xlhdr.t_hoff - SizeOfUHeapDiskTupleData);
            appendStringInfo(buf, "difflen %lu tHoff %u. td_id %u, locker_td_id %u.",
                len, xlhdr.t_hoff, (uint8)xlhdr.td_id, (uint8)xlhdr.locker_td_id);
            break;
        }
        case XLOG_UHEAP_FREEZE_TD_SLOT: {
            XlUHeapFreezeTdSlot *xlrec = (XlUHeapFreezeTdSlot *)rec;
            appendStringInfo(buf, "XLOG_UHEAP_FREEZE_TD_SLOT: ");
            appendStringInfo(buf, "nFrozen %u, latestFrozenXid %lu ", (uint16)xlrec->nFrozen, xlrec->latestFrozenXid);
            int *frozen_slots = (int *)((char *)rec + SizeOfUHeapFreezeTDSlot);
            appendStringInfo(buf, "frozen_slots: ");
            for (int i = 0; i < xlrec->nFrozen; i++) {
                if (i == 0)
                    appendStringInfo(buf, "%u", frozen_slots[i]);
                else
                    appendStringInfo(buf, " ,%u", frozen_slots[i]);
            }
            break;
        }
        case XLOG_UHEAP_INVALID_TD_SLOT: {
            uint16 *nCompletedXactSlots = (uint16 *)rec;
            int *completed_xact_slots = (int *)((char *)rec + sizeof(uint16));
            appendStringInfo(buf, "XLOG_UHEAP_INVALID_TD_SLOT: ");
            appendStringInfo(buf, "nCompletedXactSlots %u ", *nCompletedXactSlots);
            appendStringInfo(buf, "completed slots: ");
            for (int i = 0; i < *nCompletedXactSlots; i++) {
                if (i == 0)
                    appendStringInfo(buf, "%u", completed_xact_slots[i]);
                else
                    appendStringInfo(buf, " ,%u", completed_xact_slots[i]);
            }
            break;
        }
        case XLOG_UHEAP_CLEAN: {
            XlUHeapClean *xlrec = (XlUHeapClean *)rec;
            appendStringInfo(buf, "XLOG_UHEAP_CLEAN: ");
            appendStringInfo(buf, "remxid %lu ndeleted: %d ndead: %d flags: %d",
                             xlrec->latestRemovedXid, xlrec->ndeleted, xlrec->ndead, xlrec->flags);
            break;
        }
        default:
            appendStringInfo(buf, "UNKNOWN");
    }
}

const char* uheap2_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    info &= XLOG_UHEAP_OPMASK;
    switch (info) {
        case XLOG_UHEAP2_BASE_SHIFT:
            return "uheap2_base_shift";
            break;
        case XLOG_UHEAP2_FREEZE:
            return "uheap2_freeze";
            break;
        case XLOG_UHEAP2_EXTEND_TD_SLOTS:
            return "uheap2_extend_slot";
            break;
        default:
            return "unknown_type";
            break;
    }
}

void UHeap2Desc(StringInfo buf, XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    TransactionId xid = XLogRecGetXid(record);

    info &= XLOG_UHEAP_OPMASK;
    switch (info) {
        case XLOG_UHEAP2_BASE_SHIFT: {
            XlUHeapBaseShift *xlrec = (XlUHeapBaseShift *)XLogRecGetData(record);
            appendStringInfo(buf, "XLOG_UHEAP2_BASE_SHIFT: delta: %ld%s", xlrec->delta, (xlrec->multi ? " multi": ""));
            break;
        }
        case XLOG_UHEAP2_FREEZE: {
            XlUHeapFreeze *xlrec = (XlUHeapFreeze *)XLogRecGetData(record);
            appendStringInfo(buf, "XLOG_UHEAP2_FREEZE: curoff_xid: %lu", xlrec->cutoff_xid);
            break;
        }
        case XLOG_UHEAP2_EXTEND_TD_SLOTS: {
            XlUHeapExtendTdSlots *xlrec = (XlUHeapExtendTdSlots *)XLogRecGetData(record);
            appendStringInfo(buf, "XLOG_UHEAP2_EXTEND_TD_SLOTS: nExtended: %u, nPrevSlot: %u xid: %lu. ",
                             xlrec->nExtended, xlrec->nPrevSlots, xid);
            break;
        }
        default:
            appendStringInfo(buf, "UNKNOWN");
    }
}

const char* uheap_undo_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    if (info == XLOG_UHEAPUNDO_PAGE) {
        return "uheap_undo_page";
    } else if (info == XLOG_UHEAPUNDO_RESET_SLOT) {
        return "uheap_undo_reset_slot";
    } else if (info == XLOG_UHEAPUNDO_ABORT_SPECINSERT) {
        return "uheap_undo_abort";
    } else {
        return "unknown_type";
    }
}


void UHeapUndoDesc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    if (info == XLOG_UHEAPUNDO_PAGE) {
        uint8 *flags = (uint8 *)rec;
        appendStringInfo(buf, "is_page_initialized: %c ", (*flags & XLU_INIT_PAGE) ? 'T' : 'F');
        char *curxlogptr = (char *)((char *)flags + sizeof(uint8));
        if (*flags & XLU_INIT_PAGE) {
            TransactionId *xidBase = (TransactionId *)curxlogptr;
            curxlogptr += sizeof(TransactionId);
            uint16 *tdCount = (uint16 *)curxlogptr;
            curxlogptr += sizeof(uint16);
            appendStringInfo(buf, "xidBase: %lu, ", *xidBase);
            appendStringInfo(buf, "tdCount: %d ", *tdCount);
        } else {
            OffsetNumber *xlogMinLPOffset = (OffsetNumber *)curxlogptr;
            curxlogptr += sizeof(OffsetNumber);
            OffsetNumber *xlogMaxLPOffset = (OffsetNumber *)curxlogptr;
            curxlogptr += sizeof(OffsetNumber);

            appendStringInfo(buf, "xlogMinLPOffset: %d, ", *xlogMinLPOffset);
            appendStringInfo(buf, "xlogMaxLPOffset: %d, ", *xlogMaxLPOffset);

            if (*xlogMaxLPOffset >= *xlogMinLPOffset) {
                size_t lpSize = (*xlogMaxLPOffset - *xlogMinLPOffset + 1) * sizeof(RowPtr);
                curxlogptr += lpSize;

                /* Restore updated tuples data */
                Offset *xlogCopyStartOffset = (Offset *)curxlogptr;
                curxlogptr += sizeof(Offset);
                Offset *xlogCopyEndOffset = (Offset *)curxlogptr;
                curxlogptr += sizeof(Offset);

                appendStringInfo(buf, "xlogCopyStartOffset: %d, ", *xlogCopyStartOffset);
                appendStringInfo(buf, "xlogCopyEndOffset: %d, ", *xlogCopyEndOffset);

                if (*xlogCopyEndOffset > *xlogCopyStartOffset) {
                    size_t dataSize = *xlogCopyEndOffset - *xlogCopyStartOffset;
                    curxlogptr += dataSize;
                }

                /* Restore updated page headers */
                TransactionId *pdPruneXid = (TransactionId *)curxlogptr;
                curxlogptr += sizeof(TransactionId);
                uint16 *pdFlags = (uint16 *)curxlogptr;
                curxlogptr += sizeof(uint16);
                uint16 *potentialFreespace = (uint16 *)curxlogptr;
                curxlogptr += sizeof(uint16);

                appendStringInfo(buf, "pdPruneXid: %lu, ", *pdPruneXid);
                appendStringInfo(buf, "pdFlags: %d, ", *pdFlags);
                appendStringInfo(buf, "potentialFreespace: %d, ", *potentialFreespace);
            }

            /* Restore TD slot */
            int *tdSlotId = (int *)curxlogptr;
            curxlogptr += sizeof(int);
            TransactionId *xid = (TransactionId *)curxlogptr;
            curxlogptr += sizeof(TransactionId);
            UndoRecPtr *slotPrevUrp = (UndoRecPtr *)curxlogptr;
            curxlogptr += sizeof(UndoRecPtr);

            appendStringInfo(buf, "tdSlotId: %d, ", *tdSlotId);
            appendStringInfo(buf, "xid: %lu, ", *xid);
            appendStringInfo(buf, "slotPrevUrp: %lu ", *slotPrevUrp);
        }
    } else if (info == XLOG_UHEAPUNDO_RESET_SLOT) {
        XlUHeapUndoResetSlot *xlrec = (XlUHeapUndoResetSlot *)rec;
        appendStringInfo(buf, "urec_ptr %lu td_slot_id %u", xlrec->urec_ptr, xlrec->td_slot_id);
    } else if (info == XLOG_UHEAPUNDO_ABORT_SPECINSERT) {
        uint8 *flags = (uint8 *)XLogRecGetData(record);
        XlUHeapUndoAbortSpecInsert *xlrec = (XlUHeapUndoAbortSpecInsert *)((char *)flags + sizeof(uint8));
        
        appendStringInfo(buf, "offset %d", xlrec->offset);
    } else {
        appendStringInfo(buf, "UNKNOWN");
    }
}
