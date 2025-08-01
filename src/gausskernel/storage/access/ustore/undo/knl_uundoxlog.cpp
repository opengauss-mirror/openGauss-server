/* -------------------------------------------------------------------------
 *
 * knl_uundoxlog.cpp
 * c++ code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/undo/knl_uundoxlog.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/transam.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/undo/knl_uundoxlog.h"
#include "access/ustore/undo/knl_uundozone.h"
#include "access/ustore/undo/knl_uundotxn.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "knl/knl_thread.h"
#include "storage/standby.h"

namespace undo {
void XlogExtendUndoSpaceReplay(const XlogUndoExtend *xlrec, XLogRecPtr extendLsn)
{
    Assert(xlrec != NULL);

    int zoneId = UNDO_PTR_GET_ZONE_ID(xlrec->tail);
    Assert(IS_VALID_ZONE_ID(zoneId));
    UndoZone *zone = UndoZoneGroup::GetUndoZone(zoneId);
    if (zone == NULL) {
        return;
    }
    Assert(UNDO_PTR_GET_ZONE_ID(xlrec->tail) == UNDO_PTR_GET_ZONE_ID(xlrec->prevtail));
    UndoSpace *usp = zone->GetUndoSpace();

    if (usp->LSN() < extendLsn) {
        UndoLogOffset newTail = UNDO_PTR_GET_OFFSET(xlrec->tail);
        usp->LockSpace();
        usp->MarkDirty();
        usp->ExtendUndoLog(zoneId, newTail, UNDO_DB_OID);
        usp->SetLSN(extendLsn);
        usp->UnlockSpace();
    }
    return;
}

void XlogUndoUnlinkReplay(const XlogUndoUnlink *xlrec, XLogRecPtr unlinkLsn)
{
    Assert(xlrec != NULL);

    int zoneId = UNDO_PTR_GET_ZONE_ID(xlrec->head);
    Assert(IS_VALID_ZONE_ID(zoneId));
    UndoZone *zone = UndoZoneGroup::GetUndoZone(zoneId);
    if (zone == NULL) {
        return;
    }
    Assert(UNDO_PTR_GET_ZONE_ID(xlrec->head) == UNDO_PTR_GET_ZONE_ID(xlrec->prevhead));
    UndoSpace *usp = zone->GetUndoSpace();

    if (usp->LSN() < unlinkLsn) {
        UndoLogOffset newHead = UNDO_PTR_GET_OFFSET(xlrec->head);
        UndoLogOffset head = usp->Head();
        Assert(head == UNDO_PTR_GET_OFFSET(xlrec->prevhead));
        zone->ForgetUndoBuffer(head, newHead, UNDO_DB_OID);
        usp->LockSpace();
        usp->MarkDirty();
        usp->UnlinkUndoLog(zoneId, newHead, UNDO_DB_OID);
        usp->SetLSN(unlinkLsn);
        usp->UnlockSpace();
    }
    return;
}

void XlogExtendSlotSpaceReplay(const XlogUndoExtend *xlrec, XLogRecPtr extendLsn)
{
    Assert(xlrec != NULL);

    int zoneId = UNDO_PTR_GET_ZONE_ID(xlrec->tail);
    Assert(IS_VALID_ZONE_ID(zoneId));
    UndoZone *zone = UndoZoneGroup::GetUndoZone(zoneId);
    if (zone == NULL) {
        return;
    }
    Assert(UNDO_PTR_GET_ZONE_ID(xlrec->tail) == UNDO_PTR_GET_ZONE_ID(xlrec->prevtail));
    UndoSpace *usp = zone->GetSlotSpace();

    if (usp->LSN() < extendLsn) {
        UndoLogOffset newTail = UNDO_PTR_GET_OFFSET(xlrec->tail);
        usp->LockSpace();
        usp->MarkDirty();
        usp->ExtendUndoLog(zoneId, newTail, UNDO_SLOT_DB_OID);
        usp->SetLSN(extendLsn);
        usp->UnlockSpace();
    }
    return;
}

void XlogSlotUnlinkReplay(const XlogUndoUnlink *xlrec, XLogRecPtr unlinkLsn)
{
    Assert(xlrec != NULL);

    int zoneId = UNDO_PTR_GET_ZONE_ID(xlrec->head);
    Assert(IS_VALID_ZONE_ID(zoneId));
    UndoZone *zone = UndoZoneGroup::GetUndoZone(zoneId);
    if (zone == NULL) {
        return;
    }
    Assert(UNDO_PTR_GET_ZONE_ID(xlrec->head) == UNDO_PTR_GET_ZONE_ID(xlrec->prevhead));
    UndoSpace *usp = zone->GetSlotSpace();

    if (usp->LSN() < unlinkLsn) {
        UndoLogOffset newHead = UNDO_PTR_GET_OFFSET(xlrec->head);
        UndoLogOffset head = usp->Head();
        Assert(head == UNDO_PTR_GET_OFFSET(xlrec->prevhead));
        zone->ForgetUndoBuffer(head, newHead, UNDO_SLOT_DB_OID);
        usp->LockSpace();
        usp->MarkDirty();
        usp->UnlinkUndoLog(zoneId, newHead, UNDO_SLOT_DB_OID);
        usp->SetLSN(unlinkLsn);
        usp->UnlockSpace();
    }
    return;
}

void XlogUndoDiscardReplay(const XlogUndoDiscard *xlrec, XLogRecPtr lsn)
{
    Assert(xlrec != NULL);
    UndoZone *zone = UndoZoneGroup::GetUndoZone(UNDO_PTR_GET_ZONE_ID(xlrec->startSlot));
    if (zone == NULL) {
        return;
    }
    if (InHotStandby) {
        if (!IsSkipInsertSlot(xlrec->startSlot)) {
            UndoSlotBuffer buf;
            buf.PrepareTransactionSlot(xlrec->startSlot);
            LockBuffer(buf.Buf(), BUFFER_LOCK_EXCLUSIVE);
            Page page = BufferGetPage(buf.Buf());
            if (PageGetLSN(page) < lsn) {
                UndoSlotPtr recycle = xlrec->startSlot;
                while (recycle < xlrec->endSlot) {
                    TransactionSlot *slot = buf.FetchTransactionSlot(recycle);
                    ResolveRecoveryConflictWithSnapshotOid(slot->XactId(), slot->DbId(), lsn);
                    recycle = GetNextSlotPtr(recycle);
                }
            }
            UnlockReleaseBuffer(buf.Buf());
        }
    }
    if (zone->GetLSN() < lsn) {
        zone->LockUndoZone();
        Assert(xlrec->startSlot == zone->GetRecycleTSlotPtr());
        zone->SetRecycleTSlotPtr(xlrec->endSlot);
        zone->SetDiscardURecPtr(xlrec->endUndoPtr);
        zone->SetForceDiscardURecPtr(xlrec->endUndoPtr);
        zone->SetRecycleXid(xlrec->recycledXid);
        zone->MarkDirty();
        zone->SetLSN(lsn);
        zone->UnlockUndoZone();
    }
    return;
}

void UndoXlogRedo(XLogReaderState *record)
{
    Assert(record != NULL);

    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    void *xlrec = (void *)XLogRecGetData(record);

    switch (info) {
        case XLOG_UNDO_UNLINK:
            XlogUndoUnlinkReplay((XlogUndoUnlink *)xlrec, record->EndRecPtr);
            break;
        case XLOG_UNDO_EXTEND:
            XlogExtendUndoSpaceReplay((XlogUndoExtend *)xlrec, record->EndRecPtr);
            break;
        case XLOG_SLOT_UNLINK:
            XlogSlotUnlinkReplay((XlogUndoUnlink *)xlrec, record->EndRecPtr);
            break;
        case XLOG_SLOT_EXTEND:
            XlogExtendSlotSpaceReplay((XlogUndoExtend *)xlrec, record->EndRecPtr);
            break;
        case XLOG_UNDO_DISCARD:
            XlogUndoDiscardReplay((XlogUndoDiscard *)xlrec, record->ReadRecPtr);
            break;
        default:
            ereport(PANIC, (errmsg(UNDOFORMAT("Unknown op code %u"), info)));
    }
}

XLogRecPtr WriteUndoXlog(void *xlrec, uint8 type)
{
    Assert(xlrec != NULL);
    XLogRecPtr xlogRecPtr = InvalidXLogRecPtr;

    switch (type) {
        case XLOG_UNDO_EXTEND:
            XLogBeginInsert();
            XLogRegisterData((char *)xlrec, sizeof(XlogUndoExtend));
            xlogRecPtr = XLogInsert(RM_UNDOLOG_ID, XLOG_UNDO_EXTEND);
            break;
        case XLOG_UNDO_UNLINK:
            XLogBeginInsert();
            XLogRegisterData((char *)xlrec, sizeof(XlogUndoUnlink));
            xlogRecPtr = XLogInsert(RM_UNDOLOG_ID, XLOG_UNDO_UNLINK);
            break;
        case XLOG_SLOT_EXTEND:
            XLogBeginInsert();
            XLogRegisterData((char *)xlrec, sizeof(XlogUndoExtend));
            xlogRecPtr = XLogInsert(RM_UNDOLOG_ID, XLOG_SLOT_EXTEND);
            break;
        case XLOG_SLOT_UNLINK:
            XLogBeginInsert();
            XLogRegisterData((char *)xlrec, sizeof(XlogUndoUnlink));
            xlogRecPtr = XLogInsert(RM_UNDOLOG_ID, XLOG_SLOT_UNLINK);
            break;
        case XLOG_UNDO_DISCARD:
            XLogBeginInsert();
            XLogRegisterData((char *)xlrec, sizeof(XlogUndoDiscard));
            xlogRecPtr = XLogInsert(RM_UNDOLOG_ID, XLOG_UNDO_DISCARD);
            break;
        default:
            ereport(PANIC, (errmsg(UNDOFORMAT("Unknown type %u"), type)));
    }

    return xlogRecPtr;
}

void LogUndoMeta(const XlogUndoMeta *xlum)
{
    XLogRegisterData((char *)xlum, xlum->Size());
}

void CopyUndoMeta(const XlogUndoMeta &src, XlogUndoMeta &dest)
{
    dest.slotPtr = src.slotPtr;
    dest.info = src.info;
    dest.lastRecordSize = src.lastRecordSize;
    if (src.IsTranslot()) {
        dest.dbid = src.dbid;
    }
}

void UndoXlogRollbackFinishRedo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XlogRollbackFinish *xlrec = NULL;

    switch (info) {
        case XLOG_ROLLBACK_FINISH:
            xlrec = (XlogRollbackFinish *)XLogRecGetData(record);
            RedoRollbackFinish(xlrec->slotPtr, record->EndRecPtr);
            break;
        default:
            elog(PANIC, "UndoXlogRollbackFinishRedo: unknown op code %u", info);
    }
}
}
