/* -------------------------------------------------------------------------
 *
 * knl_uundoapi.cpp
 * c++ code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/undo/knl_uundoapi.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/undo/knl_uundozone.h"
#include "access/ustore/undo/knl_uundospace.h"
#include "access/ustore/undo/knl_uundotxn.h"
#include "access/ustore/undo/knl_uundotype.h"
#include "access/ustore/undo/knl_uundoxlog.h"
#include "access/ustore/knl_whitebox_test.h"
#include "access/transam.h"
#include "catalog/pg_class.h"
#include "knl/knl_session.h"
#include "knl/knl_thread.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/smgr/fd.h"
#include "storage/ipc.h"
#include "threadpool/threadpool.h"
#include "utils/builtins.h"

namespace undo {
void AllocateTransSlot(UndoSlotPtr slotPtr, UndoZone *zone, TransactionId xid, UndoPersistence upersistence)
{
    TransactionSlot *slot = zone->AllocTransactionSlot(slotPtr, xid, u_sess->proc_cxt.MyDatabaseId);

    WHITEBOX_TEST_STUB(UNDO_ALLOCATE_TRANS_SLOT_FAILED, WhiteboxDefaultErrorEmit);

    if (slot == NULL) {
        ereport(PANIC, (errmsg(UNDOFORMAT("zone %d cannot allocate transaction slot."),
            zone->GetZoneId())));
    }
    t_thrd.undo_cxt.slots[upersistence] = (void *)slot;
}

bool CheckNeedSwitch(UndoPersistence upersistence, uint64 size, UndoRecPtr undoPtr)
{
    int zid;
    if (t_thrd.xlog_cxt.InRecovery) {
        zid = UNDO_PTR_GET_ZONE_ID(undoPtr);
    } else {
        zid = t_thrd.undo_cxt.zids[upersistence];
    }
    Assert(zid != INVALID_ZONE_ID);
    UndoZone *uzone = UndoZoneGroup::GetUndoZone(zid, true, upersistence);
    if (uzone == NULL) {
        ereport(PANIC, (errmsg("CheckNeedSwitch: uzone is NULL")));
    }
    return uzone->CheckNeedSwitch(size);
}

void RollbackIfUndoExceeds(TransactionId xid, uint64 size)
{
    t_thrd.undo_cxt.transUndoSize += size;
    uint64 transUndoThresholdSize = (uint64)u_sess->attr.attr_storage.undo_limit_size_transaction * BLCKSZ;
    if ((!t_thrd.xlog_cxt.InRecovery) && (t_thrd.undo_cxt.transUndoSize > transUndoThresholdSize)) {
        ereport(ERROR, (errmsg(UNDOFORMAT("xid %lu, the undo size %lu of the transaction exceeds the threshold %lu."),
            xid, t_thrd.undo_cxt.transUndoSize, transUndoThresholdSize)));
    }
    return;
}

UndoRecPtr AllocateUndoSpace(TransactionId xid, UndoPersistence upersistence, uint64 size,
    bool needSwitch, XlogUndoMeta *xlundometa)
{
    if (!g_instance.attr.attr_storage.enable_ustore) {
        ereport(ERROR, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("Ustore is disabled, "
            "please set GUC enable_ustore=on and restart database."))));
    }
    int zid = t_thrd.undo_cxt.zids[upersistence];
    Assert(zid != INVALID_ZONE_ID);
    UndoZone *uzone = UndoZoneGroup::GetUndoZone(zid, false, upersistence);
    if (uzone == NULL) {
        ereport(PANIC, (errmsg("AllocateUndoSpace: uzone is NULL")));
    }
    Assert(upersistence == uzone->GetPersitentLevel());

    if (unlikely(needSwitch)) {
        uzone = UndoZoneGroup::SwitchZone(zid, upersistence);
        xlundometa->SetInfo(XLOG_UNDOMETA_INFO_SWITCH);
    }

    if (xid != t_thrd.undo_cxt.prevXid[upersistence]) {
        t_thrd.undo_cxt.transUndoSize = 0;
    }
    RollbackIfUndoExceeds(xid, size);
    UndoRecPtr urecptr = uzone->AllocateSpace(size);

    if (xid != t_thrd.undo_cxt.prevXid[upersistence] || needSwitch) {
        UndoSlotPtr ptr = uzone->AllocateSlotSpace();
        t_thrd.undo_cxt.slotPtr[upersistence] = ptr;
    }
    UndoSlotPtr slotPtr = t_thrd.undo_cxt.slotPtr[upersistence];
    xlundometa->slotPtr = UNDO_PTR_GET_OFFSET(slotPtr);
    ereport(DEBUG1, (errmodule(MOD_UNDO),
        errmsg(UNDOFORMAT("space %d xid %lu allocate space undo ptr from %lu to %lu size %lu"),
            uzone->GetZoneId(), xid, urecptr, uzone->GetInsert(), size)));

    return urecptr;
}

UndoRecPtr AdvanceUndoPtr(UndoRecPtr undoPtr, uint64 size)
{
    UndoLogOffset oldInsert = UNDO_PTR_GET_OFFSET(undoPtr);
    UndoLogOffset newInsert = UNDO_LOG_OFFSET_PLUS_USABLE_BYTES(oldInsert, size);
    int zid = UNDO_PTR_GET_ZONE_ID(undoPtr);
    Assert(newInsert % BLCKSZ >= UNDO_LOG_BLOCK_HEADER_SIZE);
    return MAKE_UNDO_PTR(zid, newInsert);
}

void PrepareUndoMeta(XlogUndoMeta *meta, UndoPersistence upersistence, UndoRecPtr lastRecord, UndoRecPtr lastRecordSize)
{
    int zid = t_thrd.undo_cxt.zids[upersistence];
    UndoZone *uzone = UndoZoneGroup::GetUndoZone(zid, false, upersistence);
    uzone->LockUndoZone();

    WHITEBOX_TEST_STUB(UNDO_PREPAR_ZONE_FAILED, WhiteboxDefaultErrorEmit);

    if (upersistence == UNDO_PERMANENT) {
        uzone->MarkDirty();
    }
    uzone->AdvanceInsert(UNDO_PTR_GET_OFFSET(lastRecord), lastRecordSize);
    if (uzone->GetForceDiscard() > uzone->GetInsert()) {
        ereport(PANIC, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("zone %d discard %lu > insert %lu."),
            uzone->GetZoneId(), uzone->GetForceDiscard(), uzone->GetInsert())));
    }
    uzone->GetSlotBuffer().Lock();
    BufferDesc *buf = GetBufferDescriptor(uzone->GetSlotBuffer().Buf() - 1);
    if (!UndoSlotBuffer::IsSlotBufferValid(buf, zid, meta->slotPtr)) {
        ereport(PANIC, (errmsg(UNDOFORMAT("invalid cached slot buffer %d slot ptr %lu."), 
            uzone->GetSlotBuffer().Buf(), meta->slotPtr)));
    }
    return;
}

void FinishUndoMeta(XlogUndoMeta *meta, UndoPersistence upersistence)
{
    int zid = t_thrd.undo_cxt.zids[upersistence];
    UndoZone *uzone = UndoZoneGroup::GetUndoZone(zid, false, upersistence);
    if (uzone == NULL) {
        ereport(PANIC, (errmsg("FinishUndoMeta: uzone is NULL")));
    }
    uzone->GetSlotBuffer().UnLock();
    uzone->UnlockUndoZone();
    return;
}

void UpdateTransactionSlot(TransactionId xid, XlogUndoMeta *meta, UndoRecPtr startUndoPtr,
    UndoPersistence upersistence)
{
    int zid = t_thrd.undo_cxt.zids[upersistence];
    Assert(zid != INVALID_ZONE_ID);
    bool allocateTranslot = false;
    UndoZone *uzone = UndoZoneGroup::GetUndoZone(zid, false, upersistence);
    if (uzone == NULL) {
        ereport(PANIC, (errmsg("UpdateTransactionSlot: uzone is NULL")));
    }

    if (xid != t_thrd.undo_cxt.prevXid[upersistence] || meta->IsSwitchZone()) {
        allocateTranslot = true;
        t_thrd.undo_cxt.prevXid[upersistence] = xid;
        AllocateTransSlot(t_thrd.undo_cxt.slotPtr[upersistence], uzone, xid, upersistence);
        ereport(DEBUG4, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("zone %d prevXid %lu current xid %lu"), zid,
            t_thrd.undo_cxt.prevXid[upersistence], xid)));
    }

    WHITEBOX_TEST_STUB(UNDO_UPDATE_TRANSACTION_SLOT_FAILED, WhiteboxDefaultErrorEmit);

    TransactionSlot *slot = (TransactionSlot *)t_thrd.undo_cxt.slots[upersistence];
    if (slot->XactId() != xid || slot->DbId() != u_sess->proc_cxt.MyDatabaseId) {
        ereport(PANIC,
            (errmsg(UNDOFORMAT("slot check invalid: zone %d slotptr %lu xid %lu != xid %lu, slot dbid %u != dbid %u."),
                zid, meta->slotPtr, slot->XactId(), xid, slot->DbId(), u_sess->proc_cxt.MyDatabaseId)));
    }
    ereport(DEBUG2, (errmodule(MOD_UNDO),
        errmsg(UNDOFORMAT("update zone %d, slotptr %lu xid %lu dbid %u: old start %lu end %lu, new start %lu end %lu."),
            zid, meta->slotPtr, xid, slot->DbId(), slot->StartUndoPtr(), slot->EndUndoPtr(),
            startUndoPtr, uzone->GetInsert())));
    slot->Update(startUndoPtr, uzone->GetInsert());
    Assert(slot->DbId() == u_sess->proc_cxt.MyDatabaseId);
    uzone->GetSlotBuffer().MarkDirty();
    if (allocateTranslot) {
        meta->dbid = u_sess->proc_cxt.MyDatabaseId;
        meta->SetInfo(XLOG_UNDOMETA_INFO_SLOT);
        Assert(meta->dbid != INVALID_DB_OID);
    }
    return;
}

void SetUndoMetaLSN(XlogUndoMeta *meta, XLogRecPtr lsn)
{
    int zid = t_thrd.undo_cxt.zids[UNDO_PERMANENT];
    UndoZone *uzone = UndoZoneGroup::GetUndoZone(zid, false, UNDO_PERMANENT);
    if (uzone == NULL) {
        ereport(PANIC, (errmsg("SetUndoMetaLSN: uzone is NULL")));
    }
    uzone->SetLSN(lsn);
    uzone->GetSlotBuffer().SetLSN(lsn);
}

void RedoUndoMeta(XLogReaderState *record, XlogUndoMeta *meta, UndoRecPtr startUndoPtr, 
    UndoRecPtr lastRecord, uint32 lastRecordSize)
{
    UndoZone *zone = UndoZoneGroup::GetUndoZone(UNDO_PTR_GET_ZONE_ID(startUndoPtr));
    if (zone == NULL) {
        return;
    }
    TransactionId xid = XLogRecGetXid(record);
    XLogRecPtr lsn = record->EndRecPtr;
    if (zone->GetLSN() < lsn) {
        zone->LockUndoZone();
        zone->AdvanceInsert(UNDO_PTR_GET_OFFSET(lastRecord), lastRecordSize);
        if (meta->IsTranslot() || meta->IsSwitchZone()) {
            zone->SetAllocate(GetNextSlotPtr(meta->slotPtr));
        }
        zone->MarkDirty();
        zone->SetLSN(lsn);
        zone->UnlockUndoZone();
    }
    UndoSlotPtr slotPtr = MAKE_UNDO_PTR(zone->GetZoneId(), meta->slotPtr);
    if (!IsSkipInsertSlot(slotPtr)) {
        UndoSlotBuffer buf;
        buf.PrepareTransactionSlot(slotPtr);
        LockBuffer(buf.Buf(), BUFFER_LOCK_EXCLUSIVE);
        Page page = BufferGetPage(buf.Buf());
        if (PageGetLSN(page) < lsn) {
            TransactionSlot *slot = buf.FetchTransactionSlot(slotPtr);
            if (meta->IsTranslot() || meta->IsSwitchZone()) {
                slot->Init(xid, meta->dbid);
            }
            UndoRecPtr endUndoPtr = zone->CalculateInsert(UNDO_PTR_GET_OFFSET(lastRecord), lastRecordSize);
            ereport(DEBUG2, (errmodule(MOD_UNDO),
                errmsg(UNDOFORMAT("redometa:zone %d, slotptr=%lu, lastRecordSize=%u, xid=%lu, start=%lu, end=%lu."),
                    zone->GetZoneId(), slotPtr, lastRecordSize, xid, startUndoPtr, endUndoPtr)));
            slot->Update(startUndoPtr, endUndoPtr);
            MarkBufferDirty(buf.Buf());
            PageSetLSN(page, lsn);
        }
        UnlockReleaseBuffer(buf.Buf());
    }
    return;
}

/* Check undo record valid.. */
UndoRecordState CheckUndoRecordValid(UndoRecPtr urp, bool checkForceRecycle)
{
    if (!IS_VALID_UNDO_REC_PTR(urp)) {
        return UNDO_RECORD_INVALID;
    }
    int zid = UNDO_PTR_GET_ZONE_ID(urp);
    Assert(IS_VALID_ZONE_ID(zid));
    UndoZone *uzone = UndoZoneGroup::GetUndoZone(zid, false);
    if (uzone == NULL) {
        return UNDO_RECORD_INVALID;
    } else {
        return uzone->CheckUndoRecordValid(UNDO_PTR_GET_OFFSET(urp), checkForceRecycle);
    }
}

/*
 * skip prepare undo record when undo record was invalid.
 */
bool IsSkipInsertUndo(UndoRecPtr urp)
{
    Assert(IS_VALID_UNDO_REC_PTR(urp));
    int zid = UNDO_PTR_GET_ZONE_ID(urp);
    UndoZone *uzone = UndoZoneGroup::GetUndoZone(zid);
    if (uzone == NULL) {
        return true;
    }
    UndoSpace *space = uzone->GetUndoSpace();
    UndoLogOffset offset = UNDO_PTR_GET_OFFSET(urp);
    if (offset > space->Head() && offset < space->Tail()) {
        return false;
    } else if (offset >= space->Tail()) {
        ereport(PANIC, (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT("Space allocation tail=%lu is faster than undo insert offset=%lu."),
                space->Tail(), offset)));
    }
    return true;
}

bool IsSkipInsertSlot(UndoSlotPtr slotPtr)
{
    Assert(IS_VALID_UNDO_REC_PTR(slotPtr));
    int zid = UNDO_PTR_GET_ZONE_ID(slotPtr);
    UndoZone *uzone = UndoZoneGroup::GetUndoZone(zid, true);
    if (uzone == NULL) {
        return true;
    }
    UndoSpace *space = uzone->GetSlotSpace();
    UndoLogOffset offset = UNDO_PTR_GET_OFFSET(slotPtr);
    if (offset > space->Head() && offset < space->Tail()) {
        return false;
    } else if (offset >= space->Tail()) {
        ereport(PANIC, (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT("Space allocation tail=%lu is faster than slot insert offset=%lu."),
                space->Tail(), offset)));
    }
    return true;
}

/*
 * Persist undospace and transactionGroup metadata to disks. The size of
 * the metadata page is 4 KB. The format is as follows
 * ---------|---------|-------|---------|---------|---------|
 * undoMeta  undoMeta  ...    |TransGrp  TransGrp  ...
 * ---------|---------|-------|---------|---------|---------|
 * ->         4K            <-|->          4K             ->|
 */
void CheckPointUndoSystemMeta(XLogRecPtr checkPointRedo)
{
#ifndef ENABLE_MULTIPLE_NODES
    if (g_instance.undo_cxt.uZoneCount == 0) {
        return;
    }
    /* Open undo meta file. */
    if (t_thrd.role == CHECKPOINT_THREAD) {
        TransactionId oldestXidInUndo = pg_atomic_read_u64(&g_instance.undo_cxt.oldestXidInUndo);
        ereport(LOG, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT(
            "undo metadata checkPointRedo = %lu, oldestXidInUndo = %lu."),
            checkPointRedo, oldestXidInUndo)));
        int fd = BasicOpenFile(UNDO_META_FILE, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("could not open file \%s", UNDO_META_FILE)));
            return;
        }

        /* Checkpoint undospace meta first. */
        UndoZone::CheckPointUndoZone(fd);
        UndoSpace::CheckPointUndoSpace(fd, UNDO_LOG_SPACE);
        UndoSpace::CheckPointUndoSpace(fd, UNDO_SLOT_SPACE);

        /* Flush buffer data and close fd. */
        pgstat_report_waitevent(WAIT_EVENT_UNDO_META_SYNC);
        PGSTAT_INIT_TIME_RECORD();
        PGSTAT_START_TIME_RECORD();
        fsync(fd);
        PGSTAT_END_TIME_RECORD(DATA_IO_TIME);
        pgstat_report_waitevent(WAIT_EVENT_END);
        close(fd);
    }
#endif
}

void InitUndoCountThreshold()
{
    uint32 undoMemFactor = 4;
    uint32 undoCountThreshold = 0;
    uint32 maxConn = g_instance.attr.attr_network.MaxConnections;
    uint32 maxThreadNum = 0;
    

    if (ENABLE_THREAD_POOL) {
        maxThreadNum = g_threadPoolControler->GetThreadNum();
    }

    undoCountThreshold = (maxConn >= maxThreadNum) ? undoMemFactor * maxConn : undoMemFactor * maxThreadNum;
    g_instance.undo_cxt.undoCountThreshold = (g_instance.undo_cxt.uZoneCount >= undoCountThreshold) ?
        undoMemFactor * g_instance.undo_cxt.uZoneCount : undoCountThreshold;
    g_instance.undo_cxt.undoCountThreshold = (g_instance.undo_cxt.undoCountThreshold > UNDO_ZONE_COUNT) ?
        g_instance.undo_cxt.undoCountThreshold : UNDO_ZONE_COUNT;
}

static bool InitZoneMeta(int fd)
{
    int rc = 0;
    uint64 writeSize = 0;
    uint32 ret = 0;
    uint32 totalZonePageCnt = 0;
    char metaPageBuffer[UNDO_META_PAGE_SIZE] = {'\0'};
    pg_crc32 zoneMetaPageCrc = 0;

    /* Init undospace meta, persist meta info into disk. */
    UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOZONE_COUNT_PER_PAGE, totalZonePageCnt);
    for (uint32 loop = 0; loop < PERSIST_ZONE_COUNT; loop++) {
        uint32 zoneId = loop;
        uint32 offset = zoneId % UNDOZONE_COUNT_PER_PAGE;
        undo::UndoZoneMetaInfo *uzoneMetaPoint = NULL;

        if (zoneId % UNDOZONE_COUNT_PER_PAGE == 0) {
            rc = memset_s(metaPageBuffer, UNDO_META_PAGE_SIZE, 0, UNDO_META_PAGE_SIZE);
            securec_check(rc, "\0", "\0");

            /* On last page, count of undospace meta maybe less than UNDOSPACE_COUNT_PER_PAGE. */
            if ((uint32)(zoneId / UNDOZONE_COUNT_PER_PAGE) + 1 == totalZonePageCnt) {
                writeSize = (PERSIST_ZONE_COUNT - (totalZonePageCnt - 1) * UNDOZONE_COUNT_PER_PAGE) *
                    sizeof(undo::UndoZoneMetaInfo);
            } else {
                writeSize = sizeof(undo::UndoZoneMetaInfo) * UNDOZONE_COUNT_PER_PAGE;
            }
        }

        uzoneMetaPoint = (undo::UndoZoneMetaInfo *)(metaPageBuffer + offset * sizeof(undo::UndoZoneMetaInfo));
        uzoneMetaPoint->version = UNDO_ZONE_META_VERSION;
        uzoneMetaPoint->lsn = 0;
        uzoneMetaPoint->insert = UNDO_LOG_BLOCK_HEADER_SIZE;
        uzoneMetaPoint->discard = UNDO_LOG_BLOCK_HEADER_SIZE;
        uzoneMetaPoint->forceDiscard = UNDO_LOG_BLOCK_HEADER_SIZE;
        uzoneMetaPoint->recycleXid = 0;
        uzoneMetaPoint->allocate = UNDO_LOG_BLOCK_HEADER_SIZE;
        uzoneMetaPoint->recycle = UNDO_LOG_BLOCK_HEADER_SIZE;

        if ((zoneId + 1) % UNDOZONE_COUNT_PER_PAGE == 0 || (zoneId == PERSIST_ZONE_COUNT - 1 &&
            ((uint32)(zoneId / UNDOZONE_COUNT_PER_PAGE) + 1 == totalZonePageCnt))) {
            INIT_CRC32C(zoneMetaPageCrc);
            COMP_CRC32C(zoneMetaPageCrc, (void *)metaPageBuffer, writeSize);
            FIN_CRC32C(zoneMetaPageCrc);
            *(pg_crc32 *)(metaPageBuffer + writeSize) = zoneMetaPageCrc;

            ret = write(fd, (void *)metaPageBuffer, UNDO_META_PAGE_SIZE);
            if (ret != UNDO_META_PAGE_SIZE) {
                ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION),
                    errmsg("[INIT UNDO] Write undozone meta info fail, expect size(%u), real size(%u).",
                        UNDO_META_PAGE_SIZE, ret)));
                return false;
            }
        }
    }
    return true;
}

static bool InitSpaceMeta(int fd)
{
    int rc = 0;
    uint64 writeSize = 0;
    uint32 ret = 0;
    uint32 totalUspPageCnt = 0;
    char metaPageBuffer[UNDO_META_PAGE_SIZE] = {'\0'};
    pg_crc32 spaceMetaPageCrc = 0;

    /* Init undospace meta, persist meta info into disk. */
    UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOSPACE_COUNT_PER_PAGE, totalUspPageCnt);
    for (uint32 loop = 0; loop < PERSIST_ZONE_COUNT; loop++) {
        uint32 zoneId = loop;
        uint32 offset = zoneId % UNDOSPACE_COUNT_PER_PAGE;
        undo::UndoSpaceMetaInfo *uspMetaPoint = NULL;

        if (zoneId % UNDOSPACE_COUNT_PER_PAGE == 0) {
            rc = memset_s(metaPageBuffer, UNDO_META_PAGE_SIZE, 0, UNDO_META_PAGE_SIZE);
            securec_check(rc, "\0", "\0");

            /* On last page, count of undospace meta maybe less than UNDOSPACE_COUNT_PER_PAGE. */
            if ((uint32)(zoneId / UNDOSPACE_COUNT_PER_PAGE) + 1 == totalUspPageCnt) {
                writeSize = (PERSIST_ZONE_COUNT - (totalUspPageCnt - 1) * UNDOSPACE_COUNT_PER_PAGE) *
                    sizeof(undo::UndoSpaceMetaInfo);
            } else {
                writeSize = sizeof(undo::UndoSpaceMetaInfo) * UNDOSPACE_COUNT_PER_PAGE;
            }
        }

        uspMetaPoint = (undo::UndoSpaceMetaInfo *)(metaPageBuffer + offset * sizeof(undo::UndoSpaceMetaInfo));
        uspMetaPoint->version = UNDO_SPACE_META_VERSION;
        uspMetaPoint->lsn = 0;
        uspMetaPoint->head = 0;
        uspMetaPoint->tail = 0;

        if ((zoneId + 1) % UNDOSPACE_COUNT_PER_PAGE == 0 || (zoneId == PERSIST_ZONE_COUNT - 1 &&
            ((uint32)(zoneId / UNDOSPACE_COUNT_PER_PAGE) + 1 == totalUspPageCnt))) {
            INIT_CRC32C(spaceMetaPageCrc);
            COMP_CRC32C(spaceMetaPageCrc, (void *)metaPageBuffer, writeSize);
            FIN_CRC32C(spaceMetaPageCrc);
            *(pg_crc32 *)(metaPageBuffer + writeSize) = spaceMetaPageCrc;

            ret = write(fd, (void *)metaPageBuffer, UNDO_META_PAGE_SIZE);
            if (ret != UNDO_META_PAGE_SIZE) {
                ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION),
                    errmsg("[INIT UNDO] Write undospace meta info fail, expect size(%u), real size(%u).",
                        UNDO_META_PAGE_SIZE, ret)));
                return false;
            }
        }
    }
    return true;
}

static void InitUndoMeta(void)
{
    int rc = 0;
    char undoFilePath[MAXPGPATH] = {'\0'};
    char tmpUndoFile[MAXPGPATH] = {'\0'};

    ereport(LOG, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Begin init undo subsystem meta.")));

    CheckUndoDirectory();
    rc = sprintf_s(undoFilePath, sizeof(undoFilePath), "%s", UNDO_META_FILE);
    securec_check_ss_c(rc, "\0", "\0");
    rc = sprintf_s(tmpUndoFile, sizeof(tmpUndoFile), "%s_%s", UNDO_META_FILE, "tmp");
    securec_check_ss_c(rc, "\0", "\0");

    if (access(undoFilePath, F_OK) != 0) {
        /* First, delete tmpUndoFile. */
        unlink(tmpUndoFile);
        int fd = open(tmpUndoFile, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            ereport(PANIC, (errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("[INIT UNDO] Open %s file failed, error (%s).", tmpUndoFile, strerror(errno))));
            return;
        }

        /* init undo zone meta */
        if (!InitZoneMeta(fd)) {
            goto ERROR_PROC;
        }
        /* init undo space meta */
        if (!InitSpaceMeta(fd)) {
            goto ERROR_PROC;
        }
        /* init slot space meta */
        if (!InitSpaceMeta((fd))) {
            goto ERROR_PROC;
        }

        /* Flush buffer to disk and close fd. */
        fsync(fd);
        close(fd);

        /* Rename tmpUndoFile to real undoFile. */
        if (rename(tmpUndoFile, undoFilePath) != 0) {
            unlink(tmpUndoFile);
            ereport(PANIC, (errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("[INIT UNDO] Rename tmp undo meta file failed.")));
            unlink(tmpUndoFile);
            ereport(PANIC, (errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("[INIT UNDO] Init undo subsystem meta failed, exit.")));
        }
        ereport(LOG, (errcode(ERRCODE_DATA_EXCEPTION),
            errmsg("[INIT UNDO] Init undo subsystem meta successfully.")));
        return;

    ERROR_PROC:
        close(fd);
        unlink(tmpUndoFile);
        ereport(PANIC, (errcode(ERRCODE_DATA_EXCEPTION),
            errmsg("[INIT UNDO] Init undo subsystem meta failed, exit.")));
    }
}

void RecoveryUndoSystemMeta(void)
{
    if (t_thrd.role == STARTUP) {
        /* Ensure that the undometa file exists. */
        if (access(UNDO_META_FILE, F_OK) != 0) {
            if (t_thrd.proc->workingVersionNum < USTORE_VERSION) {
                InitUndoMeta();
            } else {
                ereport(PANIC, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Undo meta file does't exists.")));
                return;
            }
        }
#ifndef ENABLE_MULTIPLE_NODES
        int fd = BasicOpenFile(UNDO_META_FILE, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            ereport(PANIC, (errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("Open file(%s), return code desc(%s)", UNDO_META_FILE, strerror(errno))));
            return;
        }
        g_instance.undo_cxt.undoTotalSize = 0;
        g_instance.undo_cxt.undoMetaSize = 0;
        /* Recover undospace meta. */
        undo::UndoZone::RecoveryUndoZone(fd);
        /* Recover undospace meta. */
        undo::UndoSpace::RecoveryUndoSpace(fd, UNDO_LOG_SPACE);
        /* Recover slotspace meta. */
        undo::UndoSpace::RecoveryUndoSpace(fd, UNDO_SLOT_SPACE);

        /* Close fd. */
        close(fd);
#endif
    }
}

void AllocateUndoZone()
{
#ifndef ENABLE_MULTIPLE_NODES
    AllocateZonesBeforXid();
#endif
}

void RedoRollbackFinish(UndoSlotPtr slotPtr, XLogRecPtr lsn)
{
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

void UpdateRollbackFinish(UndoSlotPtr slotPtr)
{
    DECLARE_NODE_COUNT();
    int zid = (int)UNDO_PTR_GET_ZONE_ID(slotPtr);
    undo::UndoSlotBuffer buf;
    buf.PrepareTransactionSlot(slotPtr);
    LockBuffer(buf.Buf(), BUFFER_LOCK_EXCLUSIVE);
    Page page = BufferGetPage(buf.Buf());
    undo::TransactionSlot *slot = buf.FetchTransactionSlot(slotPtr);
    Assert(slot->XactId() != InvalidTransactionId);
    Assert(slot->DbId() != InvalidOid);
    TransactionId oldestXidInUndo = pg_atomic_read_u64(&g_instance.undo_cxt.oldestXidInUndo);

    if (TransactionIdPrecedes(slot->XactId(), oldestXidInUndo)) {
        ereport(WARNING, (errmsg(UNDOFORMAT("curr xid having undo %lu < oldestXidInUndo %lu."),
            slot->XactId(), oldestXidInUndo)));
    }

    /* only persist level space need update transaction slot. */
    START_CRIT_SECTION();
    slot->UpdateRollbackProgress();
    ereport(DEBUG1, (errmsg(UNDOFORMAT(
        "update zone %d slot %lu xid %lu dbid %u rollback progress from start %lu to end %lu, oldestXidInUndo %lu."),
        zid, slotPtr, slot->XactId(), slot->DbId(), slot->StartUndoPtr(), slot->EndUndoPtr(),
        oldestXidInUndo)));

    XLogRecPtr lsn;
    /* WAL log the rollback progress so it can be replayed */
    if (IS_PERSIST_LEVEL(zid, nodeCount)) {
        undo::XlogRollbackFinish xlrec;
        xlrec.slotPtr = slotPtr;
        XLogBeginInsert();
        XLogRegisterData((char *)&xlrec, sizeof(xlrec));
        lsn = XLogInsert(RM_UNDOACTION_ID, XLOG_ROLLBACK_FINISH);
        PageSetLSN(page, lsn);
    }
    MarkBufferDirty(buf.Buf());
    END_CRIT_SECTION();

    UnlockReleaseBuffer(buf.Buf());
    return;
}

void OnUndoProcExit(int code, Datum arg)
{
    ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("on undo exit, thrd: %d"), t_thrd.myLogicTid)));
    for (auto i = 0; i < UNDO_PERSISTENCE_LEVELS; i++) {
        UndoPersistence upersistence = static_cast<UndoPersistence>(i);
        int zid = t_thrd.undo_cxt.zids[upersistence];
        if (!IS_VALID_ZONE_ID(zid)) {
            continue;
        }

        t_thrd.undo_cxt.zids[upersistence] = INVALID_ZONE_ID;
        UndoZoneGroup::ReleaseZone(zid, upersistence);
    }
}

void UndoLogInit(void)
{
    on_shmem_exit(OnUndoProcExit, 0);
}

UndoRecPtr GetPrevUrp(UndoRecPtr currUrp)
{
    int zoneId = UNDO_PTR_GET_ZONE_ID(currUrp);
    UndoLogOffset offset = UNDO_PTR_GET_OFFSET(currUrp);
    UndoRecordSize prevLen = UndoRecord::GetPrevRecordLen(currUrp, NULL);
    return MAKE_UNDO_PTR(zoneId, offset - prevLen);
}

void ReleaseSlotBuffer()
{
    if (g_instance.undo_cxt.uZoneCount == 0) {
        return;
    }
    for (auto i = 0; i < UNDO_PERSISTENCE_LEVELS; i++) {
        UndoPersistence upersistence = static_cast<UndoPersistence>(i);
        int zid = t_thrd.undo_cxt.zids[upersistence];
        if (!IS_VALID_ZONE_ID(zid)) {
            continue;
        }
        UndoZone *uzone = (UndoZone *)g_instance.undo_cxt.uZones[zid];
        if (uzone == NULL) {
            continue;
        }
        uzone->ReleaseSlotBuffer();
    }
}
} // namespace undo
