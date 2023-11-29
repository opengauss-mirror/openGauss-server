/* -------------------------------------------------------------------------
 *
 * knl_uundozone.cpp
 *    c++ code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/ustore/undo/knl_uundozone.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <inttypes.h>

#include "access/ustore/undo/knl_uundozone.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/undo/knl_uundotxn.h"
#include "access/ustore/undo/knl_uundospace.h"
#include "access/ustore/knl_whitebox_test.h"
#include "access/multi_redo_api.h"
#include "knl/knl_thread.h"
#include "miscadmin.h"
#include "storage/smgr/fd.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/ipc.h"
#include "threadpool/threadpool.h"
#include "utils/builtins.h"

namespace undo {

const int MAX_REALLOCATE_TIMES = 10;

UndoZone::UndoZone()
{
    MarkClean();
    InitLock();
    SetAttach(0);
    SetPersitentLevel(UNDO_PERSISTENT_BUTT);
    SetLSN(0);
    SetInsertURecPtr(UNDO_LOG_BLOCK_HEADER_SIZE);
    SetDiscardURecPtr(UNDO_LOG_BLOCK_HEADER_SIZE);
    set_discard_urec_ptr_exrto(UNDO_LOG_BLOCK_HEADER_SIZE);
    SetForceDiscardURecPtr(UNDO_LOG_BLOCK_HEADER_SIZE);
    set_force_discard_urec_ptr_exrto(UNDO_LOG_BLOCK_HEADER_SIZE);
    SetAllocateTSlotPtr(UNDO_LOG_BLOCK_HEADER_SIZE);
    SetRecycleTSlotPtr(UNDO_LOG_BLOCK_HEADER_SIZE);
    set_recycle_tslot_ptr_exrto(UNDO_LOG_BLOCK_HEADER_SIZE);
    SetFrozenSlotPtr(INVALID_UNDO_SLOT_PTR);
    SetRecycleXid(InvalidTransactionId);
    set_recycle_xid_exrto(InvalidTransactionId);
    SetFrozenXid(InvalidTransactionId);
    InitSlotBuffer();
    SetAttachPid(0);

    undoSpace_.MarkClean();
    undoSpace_.LockInit();
    undoSpace_.SetLSN(0);
    undoSpace_.SetHead(0);
    undoSpace_.set_head_exrto(0);
    undoSpace_.SetTail(0);

    slotSpace_.MarkClean();
    slotSpace_.LockInit();
    slotSpace_.SetLSN(0);
    slotSpace_.SetHead(0);
    slotSpace_.set_head_exrto(0);
    slotSpace_.SetTail(0);
}

bool UndoZone::CheckNeedSwitch(UndoRecordSize size)
{
    UndoLogOffset newInsert = UNDO_LOG_OFFSET_PLUS_USABLE_BYTES(insertURecPtr_, size);
    if (unlikely(newInsert > UNDO_LOG_MAX_SIZE)) {
        return true;
    }
    return false;
}

bool UndoZone::CheckRecycle(UndoRecPtr starturp, UndoRecPtr endurp, bool isexrto)
{
    int startZid = UNDO_PTR_GET_ZONE_ID(starturp);
    int endZid = UNDO_PTR_GET_ZONE_ID(endurp);
    UndoLogOffset start = UNDO_PTR_GET_OFFSET(starturp);
    UndoLogOffset end = UNDO_PTR_GET_OFFSET(endurp);
    WHITEBOX_TEST_STUB(UNDO_CHECK_RECYCLE_FAILED, WhiteboxDefaultErrorEmit);
    if (isexrto) {
        if ((startZid == endZid) && (forceDiscardURecPtr_ <= insertURecPtr_) && (end <= insertURecPtr_) &&
            (start < end)) {
            return true;
        }
    } else {
        if ((startZid == endZid) && (forceDiscardURecPtr_ <= insertURecPtr_) && (end <= insertURecPtr_) &&
            (start < end) && (start == forceDiscardURecPtr_)) {
            return true;
        }
    }
    ereport(WARNING, (errmodule(MOD_UNDO),
                      errmsg(UNDOFORMAT("check_recycle: zone:%d, startZid:%d, endZid:%d, start:%lu, end:%lu, "
                                        "forceDiscardURecPtr_:%lu, insertURecPtr_:%lu."),
                             zid_, startZid, endZid, start, end, forceDiscardURecPtr_, insertURecPtr_)));
    return false;
}

/* 
 * Check whether the undo record is discarded or not. If it's already discarded
 * return false otherwise return true. Caller must hold the space discardLock_.
 */
UndoRecordState UndoZone::CheckUndoRecordValid(UndoLogOffset offset, bool checkForceRecycle,
    TransactionId *lastXid)
{
    Assert((offset < UNDO_LOG_MAX_SIZE) && (offset >= UNDO_LOG_BLOCK_HEADER_SIZE));
    Assert(forceDiscardURecPtr_ <= insertURecPtr_);

    if (offset >= this->insertURecPtr_) {
        ereport(DEBUG1, (errmsg(UNDOFORMAT("The undo record not insert yet: zid=%d, insert=%lu, offset=%lu."),
            this->zid_, this->insertURecPtr_, offset)));
        return UNDO_RECORD_NOT_INSERT;
    }
    if (offset >= this->forceDiscardURecPtr_) {
        return UNDO_RECORD_NORMAL;
    }
    if (lastXid != NULL) {
        *lastXid = recycleXid_;
    }
    if (offset >= this->discardURecPtr_ && checkForceRecycle) {
        TransactionId recycleXmin;
        TransactionId oldestXmin = GetOldestXminForUndo(&recycleXmin);
        if (TransactionIdPrecedes(recycleXid_, recycleXmin)) {
            ereport(DEBUG1, (errmsg(
                UNDOFORMAT("oldestxmin %lu, recycleXmin %lu > recyclexid %lu: zid=%d,"
                "forceDiscardURecPtr=%lu, discardURecPtr=%lu, offset=%lu."),
                oldestXmin, recycleXmin, recycleXid_, this->zid_, this->forceDiscardURecPtr_,
                this->discardURecPtr_, offset)));
            return UNDO_RECORD_DISCARD;
        }
        ereport(DEBUG1, (errmsg(UNDOFORMAT("The record has been force recycled: zid=%d, forceDiscardURecPtr=%lu, "
            "discardURecPtr=%lu, offset=%lu."),
            this->zid_, this->forceDiscardURecPtr_, this->discardURecPtr_, offset)));
        return UNDO_RECORD_FORCE_DISCARD;
    }
    return UNDO_RECORD_DISCARD;
}

/*
 * Check whether the undo record is discarded or not. If it's already discarded
 * return false otherwise return true. Caller must hold the space discardLock_.
 */
UndoRecordState UndoZone::check_record_valid_exrto(UndoLogOffset offset, bool check_force_recycle,
    TransactionId *last_xid) const
{
    Assert((offset < UNDO_LOG_MAX_SIZE) && (offset >= UNDO_LOG_BLOCK_HEADER_SIZE));
    Assert(force_discard_urec_ptr_exrto <= insertURecPtr_);
 
    if (offset >= this->insertURecPtr_) {
        ereport(DEBUG1, (errmsg(UNDOFORMAT("The undo record not insert yet: zid=%d, "
            "insert=%lu, offset=%lu."),
            this->zid_, this->insertURecPtr_, offset)));
        return UNDO_RECORD_NOT_INSERT;
    }
    if (offset >= this->force_discard_urec_ptr_exrto) {
        return UNDO_RECORD_NORMAL;
    }
    if (last_xid != NULL) {
        *last_xid = recycle_xid_exrto;
    }
    if (offset >= this->discard_urec_ptr_exrto && check_force_recycle) {
        TransactionId recycle_xmin;
        TransactionId oldest_xmin = GetOldestXminForUndo(&recycle_xmin);
        if (TransactionIdPrecedes(recycle_xid_exrto, recycle_xmin)) {
            ereport(DEBUG1, (errmsg(
                UNDOFORMAT("oldestxmin %lu, recycle_xmin %lu > recyclexid_exrto %lu: zid=%d,"
                "force_discard_urec_ptr_exrto=%lu, discard_urec_ptr_exrto=%lu, offset=%lu."),
                oldest_xmin, recycle_xmin, recycle_xid_exrto, this->zid_, this->force_discard_urec_ptr_exrto,
                this->discard_urec_ptr_exrto, offset)));
            return UNDO_RECORD_DISCARD;
        }
        ereport(DEBUG1, (errmsg(UNDOFORMAT("The record has been force recycled: zid=%d, "
            "force_discard_urec_ptr_exrto=%lu, "
            "discard_urec_ptr_exrto=%lu, offset=%lu."),
            this->zid_, this->force_discard_urec_ptr_exrto, this->discard_urec_ptr_exrto, offset)));
        return UNDO_RECORD_FORCE_DISCARD;
    }
    return UNDO_RECORD_DISCARD;
}

/*
 * Drop all buffers for the given undo log, from the start to end.
 */
void UndoZone::ForgetUndoBuffer(UndoLogOffset start, UndoLogOffset end, uint32 dbId)
{
    BlockNumber startBlock;
    BlockNumber endBlock;
    RelFileNode rnode;

    UNDO_PTR_ASSIGN_REL_FILE_NODE(rnode, MAKE_UNDO_PTR(zid_, start), dbId);
    startBlock = start / BLCKSZ;
    endBlock = end / BLCKSZ;

    while (startBlock < endBlock) {
        ForgetBuffer(rnode, UNDO_FORKNUM, startBlock);
        ForgetLocalBuffer(rnode, UNDO_FORKNUM, startBlock++);
    }
    return;
}

TransactionSlot *UndoZone::AllocTransactionSlot(UndoSlotPtr slotPtr, TransactionId xid, Oid dbid)
{
    if  (!TransactionIdEquals(xid, GetTopTransactionId())) {
        ereport(PANIC, (errmsg(UNDOFORMAT("init slot %lu xid %lu but not topxid %lu."),
            slotPtr, xid, GetTopTransactionId())));
    }
    TransactionSlot *slot = buf_.FetchTransactionSlot(slotPtr);
    slot->Init(xid, dbid);
    Assert(slot->DbId() == u_sess->proc_cxt.MyDatabaseId && TransactionIdIsValid(slot->XactId()));
    allocateTSlotPtr_ = UNDO_PTR_GET_OFFSET(undo::GetNextSlotPtr(slotPtr));
    Assert(allocateTSlotPtr_ >= recycleTSlotPtr_);
    return slot;
}

UndoRecPtr UndoZone::AllocateSpace(uint64 size)
{
    UndoLogOffset oldInsert = insertURecPtr_;
    UndoLogOffset newInsert = UNDO_LOG_OFFSET_PLUS_USABLE_BYTES(oldInsert, size);
    Assert(newInsert % UNDO_LOG_SEGMENT_SIZE != 0);
    if (unlikely(newInsert > undoSpace_.Tail())) {
        undoSpace_.LockSpace();
        UndoRecPtr prevTail = MAKE_UNDO_PTR(zid_, undoSpace_.Tail());
        undoSpace_.ExtendUndoLog(zid_, newInsert + UNDO_LOG_SEGMENT_SIZE -
            newInsert % UNDO_LOG_SEGMENT_SIZE, UNDO_DB_OID);
        if (pLevel_ == UNDO_PERMANENT) {
            START_CRIT_SECTION();
            undoSpace_.MarkDirty();
            XlogUndoExtend undoExtend;
            undoExtend.prevtail = prevTail;
            undoExtend.tail = MAKE_UNDO_PTR(zid_, undoSpace_.Tail());
            XLogRecPtr lsn = WriteUndoXlog(&undoExtend, XLOG_UNDO_EXTEND);
            undoSpace_.SetLSN(lsn);
            END_CRIT_SECTION();
        }
        undoSpace_.UnlockSpace();
    }
    return MAKE_UNDO_PTR(zid_, oldInsert);
}

UndoSlotPtr UndoZone::AllocateSlotSpace(void)
{
    UndoSlotOffset oldInsert = allocateTSlotPtr_;
    UndoSlotOffset newInsert = UNDO_LOG_OFFSET_PLUS_USABLE_BYTES(oldInsert, sizeof(TransactionSlot));
    Assert(newInsert % UNDO_META_SEGMENT_SIZE != 0);
    if (unlikely(newInsert > slotSpace_.Tail())) {
        slotSpace_.LockSpace();
        UndoRecPtr prevTail = MAKE_UNDO_PTR(zid_, slotSpace_.Tail());
        slotSpace_.ExtendUndoLog(zid_, newInsert + UNDO_META_SEGMENT_SIZE -
            newInsert % UNDO_META_SEGMENT_SIZE, UNDO_SLOT_DB_OID);
        if (pLevel_ == UNDO_PERMANENT) {
            START_CRIT_SECTION();
            slotSpace_.MarkDirty();
            XlogUndoExtend undoExtend;
            undoExtend.prevtail = prevTail;
            undoExtend.tail = MAKE_UNDO_PTR(zid_, slotSpace_.Tail());
            XLogRecPtr lsn = WriteUndoXlog(&undoExtend, XLOG_SLOT_EXTEND);
            slotSpace_.SetLSN(lsn);
            END_CRIT_SECTION();
        }
        slotSpace_.UnlockSpace();
    }
    UndoSlotPtr slotPtr = MAKE_UNDO_PTR(zid_, oldInsert);
    buf_.PrepareTransactionSlot(slotPtr, UNDOSLOTBUFFER_KEEP);
    return slotPtr;
}

/* Release undo space from starturp to endurp and advance discard. */
void UndoZone::ReleaseSpace(UndoRecPtr starturp, UndoRecPtr endurp, int *forceRecycleSize)
{
    UndoLogOffset end = UNDO_PTR_GET_OFFSET(endurp);
    int startSegno;
    UndoLogOffset head;
    if (t_thrd.undorecycler_cxt.is_recovery_in_progress) {
        head = undoSpace_.Head_exrto();
    } else {
        head = undoSpace_.Head();
    }
    startSegno = (int)(head / UNDO_LOG_SEGMENT_SIZE);
    int endSegno = (int)(end / UNDO_LOG_SEGMENT_SIZE);

    if (unlikely(startSegno < endSegno)) {
        if (unlikely(*forceRecycleSize > 0)) {
            *forceRecycleSize -= (int)(endSegno - startSegno) * UNDOSEG_SIZE;
        }
        ForgetUndoBuffer(startSegno * UNDO_LOG_SEGMENT_SIZE, endSegno * UNDO_LOG_SEGMENT_SIZE, UNDO_DB_OID);
        undoSpace_.LockSpace();
        UndoRecPtr prevHead = MAKE_UNDO_PTR(zid_, head);
        undoSpace_.UnlinkUndoLog(zid_, endSegno * UNDO_LOG_SEGMENT_SIZE, UNDO_DB_OID);
        Assert(undoSpace_.Head() <= insertURecPtr_);
        if (pLevel_ == UNDO_PERMANENT && (!t_thrd.undorecycler_cxt.is_recovery_in_progress)) {
            START_CRIT_SECTION();
            undoSpace_.MarkDirty();
            XlogUndoUnlink undoUnlink;
            undoUnlink.head = MAKE_UNDO_PTR(zid_, undoSpace_.Head());
            undoUnlink.prevhead = prevHead;
            XLogRecPtr lsn = WriteUndoXlog(&undoUnlink, XLOG_UNDO_UNLINK);
            undoSpace_.SetLSN(lsn);
            END_CRIT_SECTION();
        }
        undoSpace_.UnlockSpace();
    }
    return;
}

/* Release undo space from starturp to endurp and advance discard. */
uint64 UndoZone::release_residual_record_space()
{
    UndoLogOffset unlink_start = undoSpace_.find_oldest_offset(zid_, UNDO_DB_OID);
    UndoLogOffset unlink_end = undoSpace_.Head();
    uint64 start_segno = unlink_start / UNDO_LOG_SEGMENT_SIZE;
    uint64 end_segno = unlink_end / UNDO_LOG_SEGMENT_SIZE;
    ereport(DEBUG1, (errmodule(MOD_STANDBY_READ),
                     errmsg("release_residual_record_space start_segno:%lu end_segno:%lu.", start_segno, end_segno)));
    ForgetUndoBuffer(start_segno * UNDO_LOG_SEGMENT_SIZE, end_segno * UNDO_LOG_SEGMENT_SIZE, UNDO_DB_OID);
    undoSpace_.LockSpace();
    undoSpace_.unlink_residual_log(zid_, unlink_start, unlink_end, UNDO_DB_OID);
    undoSpace_.UnlockSpace();
    if (unlink_start > unlink_end) {
        ereport(WARNING, (errmsg(UNDOFORMAT("release_residual_record_space start:%lu "
            "is bigger than end:%lu."),
            unlink_start, unlink_end)));
        return 0;
    } else {
        return (unlink_end / UNDO_LOG_SEGMENT_SIZE) - (unlink_start / UNDO_LOG_SEGMENT_SIZE);
    }
}

/* Release slot space from starturp to endurp and advance discard. */
void UndoZone::ReleaseSlotSpace(UndoRecPtr startSlotPtr, UndoRecPtr endSlotPtr, int *forceRecycleSize)
{
    UndoLogOffset end = UNDO_PTR_GET_OFFSET(endSlotPtr);
    UndoLogOffset head;
    if (t_thrd.undorecycler_cxt.is_recovery_in_progress) {
        head = slotSpace_.Head_exrto();
    } else {
        head = slotSpace_.Head();
    }
    int startSegno = (int)(head / UNDO_META_SEGMENT_SIZE);
    int endSegno = (int)(end / UNDO_META_SEGMENT_SIZE);

    if (unlikely(startSegno < endSegno)) {
        if (unlikely(*forceRecycleSize > 0)) {
            *forceRecycleSize -= (int)(endSegno - startSegno) * UNDO_META_SEG_SIZE;
        }
        ForgetUndoBuffer(startSegno * UNDO_META_SEGMENT_SIZE, endSegno * UNDO_META_SEGMENT_SIZE, UNDO_SLOT_DB_OID);
        slotSpace_.LockSpace();
        UndoRecPtr prevHead = MAKE_UNDO_PTR(zid_, head);
        slotSpace_.UnlinkUndoLog(zid_, endSegno * UNDO_META_SEGMENT_SIZE, UNDO_SLOT_DB_OID);
        Assert(slotSpace_.Head() <= allocateTSlotPtr_);
        if (pLevel_ == UNDO_PERMANENT && !(t_thrd.undorecycler_cxt.is_recovery_in_progress)) {
            START_CRIT_SECTION();
            slotSpace_.MarkDirty();
            XlogUndoUnlink undoUnlink;
            undoUnlink.head = MAKE_UNDO_PTR(zid_, slotSpace_.Head());
            undoUnlink.prevhead = prevHead;
            XLogRecPtr lsn = WriteUndoXlog(&undoUnlink, XLOG_SLOT_UNLINK);
            slotSpace_.SetLSN(lsn);
            END_CRIT_SECTION();
        }
        slotSpace_.UnlockSpace();
    }
    return;
}

/* Release slot space from starturp to endurp and advance discard. */
uint64 UndoZone::release_residual_slot_space()
{
    UndoLogOffset unlink_start = slotSpace_.find_oldest_offset(zid_, UNDO_SLOT_DB_OID);
    UndoLogOffset unlink_end = slotSpace_.Head();
    uint64 start_segno = unlink_start / UNDO_LOG_SEGMENT_SIZE;
    uint64 end_segno = unlink_end / UNDO_LOG_SEGMENT_SIZE;
    ForgetUndoBuffer(start_segno * UNDO_LOG_SEGMENT_SIZE, end_segno * UNDO_LOG_SEGMENT_SIZE, UNDO_SLOT_DB_OID);
    slotSpace_.LockSpace();
    slotSpace_.unlink_residual_log(zid_, unlink_start, unlink_end, UNDO_SLOT_DB_OID);
    slotSpace_.UnlockSpace();
    if (unlink_start > unlink_end) {
        ereport(WARNING, (errmsg(UNDOFORMAT("release_residual_slot_space start:%lu is bigger "
            "than end:%lu."),
            unlink_start, unlink_end)));
        return 0;
    } else {
        return (unlink_end / UNDO_META_SEGMENT_SIZE) - (unlink_start / UNDO_META_SEGMENT_SIZE);
    }
}

void UndoZone::PrepareSwitch(void)
{
    WHITEBOX_TEST_STUB(UNDO_PREPARE_SWITCH_FAILED, WhiteboxDefaultErrorEmit);

    if (undoSpace_.Tail() != UNDO_LOG_MAX_SIZE) {
        ereport(PANIC, (errmsg(UNDOFORMAT(
            "Undo space switch fail, expect tail(%lu), real tail(%lu)."),
            UNDO_LOG_MAX_SIZE, undoSpace_.Tail())));
    }
    if (pLevel_ == UNDO_PERMANENT) {
        LockUndoZone();
        MarkDirty();
        insertURecPtr_ = UNDO_LOG_MAX_SIZE;
        UnlockUndoZone();
    }
    return;
}

/* Initialize parameters in undo meta. */
static void InitUndoMeta(UndoZoneMetaInfo *uspMetaPointer)
{
    uspMetaPointer->version = UNDO_ZONE_META_VERSION;
    uspMetaPointer->lsn = 0;
    uspMetaPointer->insertURecPtr = UNDO_LOG_BLOCK_HEADER_SIZE;
    uspMetaPointer->discardURecPtr = UNDO_LOG_BLOCK_HEADER_SIZE;
    uspMetaPointer->forceDiscardURecPtr = UNDO_LOG_BLOCK_HEADER_SIZE;
    uspMetaPointer->recycleXid = 0;
    uspMetaPointer->allocateTSlotPtr = UNDO_LOG_BLOCK_HEADER_SIZE;
    uspMetaPointer->recycleTSlotPtr = UNDO_LOG_BLOCK_HEADER_SIZE;
}

/* Get undo meta info from undo zone. */
static void GetMetaFromUzone(UndoZone *uzone, UndoZoneMetaInfo *uspMetaPointer)
{
    uspMetaPointer->version = UNDO_ZONE_META_VERSION;
    uspMetaPointer->lsn = uzone->GetLSN();
    uspMetaPointer->insertURecPtr = UNDO_PTR_GET_OFFSET(uzone->GetInsertURecPtr());
    uspMetaPointer->discardURecPtr = UNDO_PTR_GET_OFFSET(uzone->GetDiscardURecPtr());
    uspMetaPointer->forceDiscardURecPtr = UNDO_PTR_GET_OFFSET(uzone->GetForceDiscardURecPtr());
    uspMetaPointer->recycleXid = uzone->GetRecycleXid();
    uspMetaPointer->allocateTSlotPtr = UNDO_PTR_GET_OFFSET(uzone->GetAllocateTSlotPtr());
    uspMetaPointer->recycleTSlotPtr = UNDO_PTR_GET_OFFSET(uzone->GetRecycleTSlotPtr());
}

/*
 * Persist undospace metadata to disk. The fomart as follows
 * ----------|--------|---------|---------|--------------|
 *  undoMeta |undoMeta|undoMeta |undoMeta | pageCRC(32bit)
 * |->--------------------512---------------------------<-|
 */
void UndoZone::CheckPointUndoZone(int fd)
{
    Assert(fd > 0);
    bool retry = false;
    bool needFlushMetaPage = false;
    int rc = 0;
    uint32 ret = 0;
    uint32 totalPageCnt = 0;
    uint32 uspOffset = 0;
    uint64 writeSize = 0;
    uint64 currWritePos = 0;
    pg_crc32 metaPageCrc = 0;
    XLogRecPtr flushLsn = InvalidXLogRecPtr;
    uint32 cycle = 0;
    char uspMetaPagebuffer[UNDO_WRITE_SIZE] = {'\0'};
    UndoZone *uzone = NULL;

    /* Get total page count of storing all undospaces. */
    UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOZONE_COUNT_PER_PAGE, totalPageCnt);
    DECLARE_NODE_COUNT();
    uint32 zoneCountPerLevel = ZONE_COUNT_PER_LEVELS(nodeCount);
    for (uint32 loop = 0; loop < PERSIST_ZONE_COUNT; loop++) {
        uzone = (UndoZone *)g_instance.undo_cxt.uZones[loop];
        UndoZoneMetaInfo *uspMetaPointer = NULL;
        if (loop % UNDOZONE_COUNT_PER_WRITE == 0) {
            cycle = loop;
            if ((uint32)(PERSIST_ZONE_COUNT - loop) < UNDOZONE_COUNT_PER_WRITE) {
                writeSize = ((PERSIST_ZONE_COUNT - loop) / UNDOZONE_COUNT_PER_PAGE + 1) * UNDO_META_PAGE_SIZE;
            } else {
                writeSize = UNDO_WRITE_SIZE;
            }
            needFlushMetaPage = false;
            rc = memset_s(uspMetaPagebuffer, UNDO_WRITE_SIZE, 0, UNDO_WRITE_SIZE);
            securec_check(rc, "\0", "\0");
        }
        if (uzone != NULL && uzone->IsDirty()) {
            needFlushMetaPage = true;
        }

        if (needFlushMetaPage && 
            ((loop + 1) % UNDOZONE_COUNT_PER_WRITE == 0 || (loop + 1) == PERSIST_ZONE_COUNT)) {
            while (cycle <= loop) {
                uzone = (UndoZone *)g_instance.undo_cxt.uZones[cycle];
                uspOffset = cycle % UNDOZONE_COUNT_PER_PAGE;
                char *pageOffset = uspMetaPagebuffer + 
                        ((cycle % UNDOZONE_COUNT_PER_WRITE) / UNDOZONE_COUNT_PER_PAGE) * UNDO_META_PAGE_SIZE;
                uspMetaPointer = (UndoZoneMetaInfo *) (pageOffset + uspOffset * sizeof(UndoZoneMetaInfo));
                Assert(uspMetaPointer != NULL);

                if (uzone != NULL) {
                    uzone->LockUndoZone();
                    /* Set the initial value of flushLsn when the first undospace of each meta page is traversed. */
                    if (cycle % UNDOZONE_COUNT_PER_WRITE == 0) {
                        flushLsn = uzone->GetLSN();
                    } else {
                        /* Pick out max lsn of total undospaces on one meta page. */
                        if (uzone->GetLSN() > flushLsn) {
                            flushLsn = uzone->GetLSN();
                        }
                    }
                    /* Locate the undospace on the undo page, then refresh. */
                    if (cycle < zoneCountPerLevel) {
                        GetMetaFromUzone(uzone, uspMetaPointer);
                    } else {
                        InitUndoMeta(uspMetaPointer);
                    }
                    uzone->MarkClean();
                    /* Non-null uzone means it's used, whose bitmap is 0. Set meta info with a fake one. */
                    if (UNDO_PTR_GET_OFFSET(uzone->GetInsertURecPtr()) == UNDO_LOG_MAX_SIZE &&
                        uzone->GetAllocateTSlotPtr() == uzone->GetRecycleTSlotPtr()) {
                        GetMetaFromUzone(uzone, uspMetaPointer);
                        uzone->NotKeepBuffer();
                        uzone->UnlockUndoZone();
                        ereport(LOG, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("release zone memory %d."), cycle)));
                        delete(uzone);
                        g_instance.undo_cxt.uZones[cycle] = NULL;
                        pg_atomic_fetch_sub_u32(&g_instance.undo_cxt.uZoneCount, 1);
                        /* False equals to 0, which means zone is used; true is 1, which means zone is unused. */
                        bool isZoneFree = bms_is_member(cycle, g_instance.undo_cxt.uZoneBitmap[UNDO_PERMANENT]);
                        if (!isZoneFree) {
                            LWLockAcquire(UndoZoneLock, LW_EXCLUSIVE);
                            /* Release the memory and set the corresponding bitmap as 1(unused). */
                            g_instance.undo_cxt.uZoneBitmap[UNDO_PERMANENT] = 
                                bms_del_member(g_instance.undo_cxt.uZoneBitmap[UNDO_PERMANENT], cycle);
                            LWLockRelease(UndoZoneLock);
                        }
                        Assert(bms_num_members(g_instance.undo_cxt.uZoneBitmap[UNDO_PERMANENT]) != 0);
                    } else {
                        uzone->UnlockUndoZone();
                    }

                } else {
                    /* Either the bitmap is 0 or 1, uzone will be null. Initialize meta info. */
                    InitUndoMeta(uspMetaPointer);
                }

                /* all undospaces on the meta page are written. */
                if ((cycle + 1) % UNDOZONE_COUNT_PER_PAGE == 0 || cycle == PERSIST_ZONE_COUNT - 1) {
                    /* Flush wal buffer of undo xlog first. */
                    uint64 crcSize;
                    if (cycle == PERSIST_ZONE_COUNT - 1) {
                        crcSize = (PERSIST_ZONE_COUNT % UNDOZONE_COUNT_PER_PAGE) * sizeof(UndoZoneMetaInfo);
                    } else {
                        crcSize = UNDOZONE_COUNT_PER_PAGE * sizeof(UndoZoneMetaInfo);
                    }
                    
                    INIT_CRC32C(metaPageCrc);
                    COMP_CRC32C(metaPageCrc, (void *)pageOffset, crcSize);
                    FIN_CRC32C(metaPageCrc);
                    /* Store CRC behind the last undospace meta on the page. */
                    *(pg_crc32 *) (pageOffset + crcSize) = metaPageCrc;
                }
                if ((cycle + 1) % UNDOZONE_COUNT_PER_WRITE == 0 || cycle == PERSIST_ZONE_COUNT - 1) {
                    XLogWaitFlush(flushLsn);
                    needFlushMetaPage = false;
                    currWritePos = lseek(fd, (cycle / UNDOZONE_COUNT_PER_WRITE) * UNDO_WRITE_SIZE, SEEK_SET);
                    RE_WRITE:
                    lseek(fd, currWritePos, SEEK_SET);
                    ret = write(fd, uspMetaPagebuffer, writeSize);
                    if (ret != writeSize && !retry) {
                        retry = true;
                        goto RE_WRITE;
                    } else if (ret != writeSize && retry) {
                        ereport(ERROR, (errmsg(UNDOFORMAT("Write undo meta failed expect size(%lu) real size(%u)."),
                            writeSize, ret)));
                        return;
                    }
                    ereport(DEBUG1, (errmsg(UNDOFORMAT("undo metadata write loop %u."), cycle)));
                }
                cycle++;
            }
        }
    }
}

bool UndoZoneGroup::UndoZoneInUse(int zid, UndoPersistence upersistence)
{
    bool isZoneFree = true;
    int tmpZid = zid - (int)upersistence * PERSIST_ZONE_COUNT;
    if (!IS_VALID_ZONE_ID(tmpZid)) {
        ereport(PANIC, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("zoneid %d invalid."), zid)));
    }
    isZoneFree = bms_is_member(tmpZid, g_instance.undo_cxt.uZoneBitmap[upersistence]);
    if (isZoneFree) {
        ereport(PANIC, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("allocate zone fail."))));
        return false;
    }
    return true;
}

static void AllocateZoneMemory(UndoZone **uzone, UndoPersistence upersistence, int zid)
{
    if (*uzone == NULL) {
        int reallocateTimes = 1;
REALLOCATE:
        MemoryContext oldContext = MemoryContextSwitchTo(g_instance.undo_cxt.undoContext);
        *uzone = New(g_instance.undo_cxt.undoContext) UndoZone();
        MemoryContextSwitchTo(oldContext);
        if (*uzone == NULL && reallocateTimes <= MAX_REALLOCATE_TIMES) {
            reallocateTimes++;
            ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("reallocate times %d."), reallocateTimes)));
            goto REALLOCATE;
        }
        if (*uzone == NULL) {
            ereport(PANIC, (errmsg(UNDOFORMAT("allocate zone failed."))));
        }
        pg_atomic_fetch_add_u32(&g_instance.undo_cxt.uZoneCount, 1);
        InitZone(*uzone, zid, upersistence);
        ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("attached free zone %d."), zid)));
    } else {
        ereport(PANIC, (errmsg(UNDOFORMAT("zone reallocate."))));
    }

    pg_write_barrier();
    g_instance.undo_cxt.uZones[zid] = *uzone;
}

static void RecoveryZone(UndoZone *uzone,
    const UndoZoneMetaInfo *uspMetaInfo, const int zoneId)
{
    uzone->SetZoneId(zoneId);
    uzone->SetPersitentLevel(UNDO_PERMANENT);
    uzone->SetLSN(uspMetaInfo->lsn);
    uzone->SetInsertURecPtr(uspMetaInfo->insertURecPtr);
    uzone->SetDiscardURecPtr(uspMetaInfo->discardURecPtr);
    uzone->set_discard_urec_ptr_exrto(uspMetaInfo->discardURecPtr);
    uzone->SetForceDiscardURecPtr(uspMetaInfo->forceDiscardURecPtr);
    uzone->set_force_discard_urec_ptr_exrto(uspMetaInfo->forceDiscardURecPtr);
    uzone->SetAllocateTSlotPtr(uspMetaInfo->allocateTSlotPtr);
    uzone->SetRecycleTSlotPtr(uspMetaInfo->recycleTSlotPtr);
    uzone->set_recycle_tslot_ptr_exrto(uspMetaInfo->recycleTSlotPtr);
    uzone->SetRecycleXid(uspMetaInfo->recycleXid);
    uzone->set_recycle_xid_exrto(uspMetaInfo->recycleXid);
    ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("recovery_zone id:%d, lsn:%lu, "
        "insert_urec_ptr:%lu, discard_urec_ptr:%lu, force_discard_urec_ptr:%lu, allocate_tslot_ptr:%lu, "
        "recycle_tslot_ptr:%lu, recycle_xid:%lu."), zoneId, uspMetaInfo->lsn, uspMetaInfo->insertURecPtr,
        uspMetaInfo->discardURecPtr, uspMetaInfo->forceDiscardURecPtr, uspMetaInfo->allocateTSlotPtr,
        uspMetaInfo->recycleTSlotPtr, uspMetaInfo->recycleXid)));
}

/* Initialize parameters in the undo zone. */
void InitZone(UndoZone *uzone, const int zoneId, UndoPersistence upersistence)
{
    uzone->SetZoneId(zoneId);
    uzone->SetAttach(0);
    uzone->SetPersitentLevel(upersistence);
    uzone->SetLSN(0);
    uzone->SetInsertURecPtr(UNDO_LOG_BLOCK_HEADER_SIZE);
    uzone->SetDiscardURecPtr(UNDO_LOG_BLOCK_HEADER_SIZE);
    uzone->set_discard_urec_ptr_exrto(UNDO_LOG_BLOCK_HEADER_SIZE);
    uzone->SetForceDiscardURecPtr(UNDO_LOG_BLOCK_HEADER_SIZE);
    uzone->set_force_discard_urec_ptr_exrto(UNDO_LOG_BLOCK_HEADER_SIZE);
    uzone->SetAllocateTSlotPtr(UNDO_LOG_BLOCK_HEADER_SIZE);
    uzone->SetRecycleTSlotPtr(UNDO_LOG_BLOCK_HEADER_SIZE);
    uzone->set_recycle_tslot_ptr_exrto(UNDO_LOG_BLOCK_HEADER_SIZE);
    uzone->SetFrozenSlotPtr(INVALID_UNDO_SLOT_PTR);
    uzone->SetRecycleXid(InvalidTransactionId);
    uzone->set_recycle_xid_exrto(InvalidTransactionId);
    uzone->SetFrozenXid(InvalidTransactionId);
    uzone->SetAttachPid(0);
}

/* Initialize parameters in the undo space. */
void InitUndoSpace(UndoZone *uzone, UndoSpaceType type)
{
    UndoSpace *usp = uzone->GetSpace(type);
    usp->MarkClean();
    usp->SetLSN(0);
    usp->SetHead(0);
    usp->set_head_exrto(0);
    usp->SetTail(0);
}

void UndoZone::RecoveryUndoZone(int fd)
{
    Assert(fd > 0);
    int rc = 0;
    uint32 zoneId = 0;
    uint32 zoneMetaSize = 0;
    uint32 totalPageCnt = 0;
    pg_crc32 pageCrcVal = 0;       /* CRC store in undo meta page */
    pg_crc32 comCrcVal = 0;        /* calculating CRC current */
    char *uspMetaBuffer = NULL;
    MemoryContext oldContext = MemoryContextSwitchTo(g_instance.undo_cxt.undoContext);
    char *persistBlock = (char *)palloc0(UNDO_META_PAGE_SIZE * PAGES_READ_NUM);
    (void*)MemoryContextSwitchTo(oldContext);

    UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOZONE_COUNT_PER_PAGE, totalPageCnt);
    zoneMetaSize = totalPageCnt * UNDO_META_PAGE_SIZE / BLCKSZ;
    g_instance.undo_cxt.undoMetaSize += zoneMetaSize;

    /* Ensure read at start posistion of file. */
    lseek(fd, 0, SEEK_SET);

    if (g_instance.undo_cxt.uZones != NULL) {
        for (uint32 idx = PERSIST_ZONE_COUNT; idx < UNDO_ZONE_COUNT; idx++) {
            if (g_instance.undo_cxt.uZones[idx] != NULL) {
                UndoZone *uzone = (UndoZone *)g_instance.undo_cxt.uZones[idx];
                uzone->ForgetUndoBuffer(uzone->GetUndoSpace()->Head(), uzone->GetUndoSpace()->Tail(), UNDO_DB_OID);
                uzone->ForgetUndoBuffer(uzone->GetSlotSpace()->Head(), uzone->GetSlotSpace()->Tail(), UNDO_SLOT_DB_OID);
                UndoPersistence upersistence = UNDO_PERMANENT;
                GET_UPERSISTENCE(idx, upersistence);
                InitZone(uzone, idx, upersistence);
                InitUndoSpace(uzone, UNDO_LOG_SPACE);
                InitUndoSpace(uzone, UNDO_SLOT_SPACE);
            }
        }
    }

    for (int i = 0; i < UNDO_PERSISTENCE_LEVELS; i++) {
        UndoPersistence persistence = (UndoPersistence)i;
        if (persistence != UNDO_PERMANENT) {
            CleanUndoFileDirectory(persistence);
        }
        CheckUndoFileDirectory(persistence);
    }

    DECLARE_NODE_COUNT();
    for (zoneId = 0; zoneId < PERSIST_ZONE_COUNT; zoneId++) {
        UndoZoneMetaInfo *uspMetaInfo = NULL;
        if (zoneId % (UNDOZONE_COUNT_PER_PAGE * PAGES_READ_NUM) == 0) {
            Size readSize;
            if ((uint32)(PERSIST_ZONE_COUNT - zoneId) < UNDOZONE_COUNT_PER_PAGE * PAGES_READ_NUM) {
                readSize = ((uint32)(PERSIST_ZONE_COUNT - zoneId) / UNDOZONE_COUNT_PER_PAGE + 1) * UNDO_META_PAGE_SIZE;
            } else {
                readSize = UNDO_META_PAGE_SIZE * PAGES_READ_NUM;
            }
            rc = memset_s(persistBlock, UNDO_META_PAGE_SIZE * PAGES_READ_NUM, 0, UNDO_META_PAGE_SIZE * PAGES_READ_NUM);
            securec_check(rc, "\0", "\0");
            uint32 ret = read(fd, persistBlock, readSize);
            if (ret != readSize) {
                ereport(ERROR, (errmsg(UNDOFORMAT("Read undo meta page, expect size(%lu), real size(%u)."),
                    readSize, ret)));
                return;
            }
        }
        if (zoneId % UNDOZONE_COUNT_PER_PAGE == 0) {
            uspMetaBuffer = persistBlock + 
                ((zoneId % (UNDOZONE_COUNT_PER_PAGE * PAGES_READ_NUM)) / UNDOZONE_COUNT_PER_PAGE) * UNDO_META_PAGE_SIZE;
            uint32 count = UNDOZONE_COUNT_PER_PAGE;
            if ((uint32)(PERSIST_ZONE_COUNT - zoneId) < UNDOZONE_COUNT_PER_PAGE) {
                count = PERSIST_ZONE_COUNT - zoneId;
            }
            /* Get page CRC from uspMetaBuffer. */
            pageCrcVal = *(pg_crc32 *)(uspMetaBuffer + sizeof(undo::UndoZoneMetaInfo) * count);
            /* 
             * Calculate the CRC value based on all undospace meta information stored on the page. 
             * Then compare with pageCrcVal.
             */
            INIT_CRC32C(comCrcVal);
            COMP_CRC32C(comCrcVal, (void *)uspMetaBuffer, sizeof(undo::UndoZoneMetaInfo) * count);
            FIN_CRC32C(comCrcVal);
            if (!EQ_CRC32C(pageCrcVal, comCrcVal)) {
                ereport(ERROR,
                    (errmsg(UNDOFORMAT("Undo meta CRC calculated(%u) is different from CRC recorded(%u) in page."),
                        comCrcVal, pageCrcVal)));
                return;
            }
        }
        int offset = zoneId % UNDOZONE_COUNT_PER_PAGE;
        uspMetaInfo = (UndoZoneMetaInfo *)(uspMetaBuffer + offset * sizeof(UndoZoneMetaInfo));
        Assert(uspMetaInfo != NULL);
        if (uspMetaInfo->insertURecPtr != UNDO_LOG_BLOCK_HEADER_SIZE) {
            UndoZone *uzone = UndoZoneGroup::GetUndoZone(zoneId, true);
            RecoveryZone(uzone, uspMetaInfo, zoneId);
        }
    }
    pfree(persistBlock);
}

static int AllocateUndoZoneId (UndoPersistence upersistence)
{
    int retZid = -1;
    retZid = bms_first_member(g_instance.undo_cxt.uZoneBitmap[upersistence]);
    if (retZid >= 0 && retZid < PERSIST_ZONE_COUNT) {
        retZid += (int)upersistence * PERSIST_ZONE_COUNT;
    } else {
        retZid = INVALID_ZONE_ID;
    }
    return retZid;
}

static int ReleaseUndoZoneId(int zid, UndoPersistence upersistence)
{
    int tempZid = zid - (int)upersistence * PERSIST_ZONE_COUNT;
    if (tempZid < 0 || tempZid > PERSIST_ZONE_COUNT) {
        ereport(PANIC, (errmsg(UNDOFORMAT("release zone %d plevel %d invalid."),
            zid, (int)upersistence)));
    }
    LWLockAcquire(UndoZoneLock, LW_EXCLUSIVE);
    g_instance.undo_cxt.uZoneBitmap[upersistence] =
        bms_add_member(g_instance.undo_cxt.uZoneBitmap[upersistence], tempZid);
    if (bms_num_members(g_instance.undo_cxt.uZoneBitmap[upersistence]) == 0) {
        ereport(WARNING,
            (errmsg(UNDOFORMAT("bitmap invalid, bitmap num %d."),
            bms_num_members(g_instance.undo_cxt.uZoneBitmap[upersistence]))));
        RebuildUndoZoneBitmap();
    }
    LWLockRelease(UndoZoneLock);
    return tempZid;
}

void UndoZoneGroup::ReleaseZone(int zid, UndoPersistence upersistence)
{
    Assert(IS_VALID_ZONE_ID(zid));
    WHITEBOX_TEST_STUB(UNDO_RELEASE_ZONE_FAILED, WhiteboxDefaultErrorEmit);
    if (g_instance.undo_cxt.uZones == NULL || g_instance.undo_cxt.uZoneCount == 0) {
        return;
    }
    UndoZone *uzone = (UndoZone *)g_instance.undo_cxt.uZones[zid];
    Assert(undo::UndoZoneGroup::UndoZoneInUse(zid, upersistence));
    Assert(uzone != NULL);
    if (!UndoZoneGroup::UndoZoneInUse(zid, upersistence)) {
        ereport(PANIC, (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT("used zone %d detached, bitmap num %d."),
            zid, bms_num_members(g_instance.undo_cxt.uZoneBitmap[upersistence]))));
    }
    if (uzone != NULL) {
        if (!uzone->Attached()) {
            ereport(PANIC, (errmodule(MOD_UNDO),
                errmsg(UNDOFORMAT("used zone %d detached."), zid)));
        }
        uzone->Detach();
        uzone->NotKeepBuffer();
        uzone->InitSlotBuffer();
        uzone->SetAttachPid(0);
    }

    pg_write_barrier();
    DECLARE_NODE_NO();
    ReleaseUndoZoneId(zid, upersistence);
}

UndoZone *UndoZoneGroup::SwitchZone(int zid, UndoPersistence upersistence)
{
    if (g_instance.undo_cxt.uZoneCount > g_instance.undo_cxt.undoCountThreshold) {
        ereport(ERROR, (errmsg(UNDOFORMAT("Too many undo zones are requested, max count is %d, now is %d"),
            g_instance.undo_cxt.undoCountThreshold, g_instance.undo_cxt.uZoneCount)));
    }

    UndoZone *uzone = UndoZoneGroup::GetUndoZone(zid, true);
    int retZid = -1;
    uzone->PrepareSwitch();
    LWLockAcquire(UndoZoneLock, LW_EXCLUSIVE);
    retZid = AllocateUndoZoneId(upersistence);
    if (!IS_VALID_ZONE_ID(retZid)) {
        ereport(ERROR, (errmsg("SwitchZone: zone id is invalid, there're too many working threads.")));
    }

    UndoZone *newUzone = UndoZoneGroup::GetUndoZone(retZid, true);
    if (newUzone == NULL || newUzone->GetUndoSpace()->Tail() != 0) {
        ereport(PANIC,
            (errmsg(UNDOFORMAT("can not palloc undo zone memory, tail = %lu."),
            newUzone->GetUndoSpace()->Tail())));
    }

    WHITEBOX_TEST_STUB(UNDO_SWITCH_ZONE_FAILED, WhiteboxDefaultErrorEmit);
    newUzone->Attach();
    LWLockRelease(UndoZoneLock);
    newUzone->InitSlotBuffer();
    Assert(newUzone->GetAttachPid() == 0);
    newUzone->SetAttachPid(u_sess->attachPid);
    pg_write_barrier();
    t_thrd.undo_cxt.zids[upersistence] = retZid;

    ereport(LOG, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("old zone %d switch to new zone %d."), zid, retZid)));
    return newUzone;
}

void UndoZoneGroup::InitUndoCxtUzones()
{
    if (g_instance.undo_cxt.uZones == NULL) {
        LWLockAcquire(UndoZoneLock, LW_EXCLUSIVE);
        if (g_instance.undo_cxt.uZones == NULL) {
            MemoryContext oldContext = MemoryContextSwitchTo(g_instance.undo_cxt.undoContext);
            g_instance.undo_cxt.uZones = (void **)palloc0(UNDO_ZONE_COUNT * sizeof(void*));
            MemoryContextSwitchTo(oldContext);
        }
        if (g_instance.undo_cxt.uZones == NULL) {
            ereport(PANIC, (errmsg(UNDOFORMAT("failed to allocate memory for UndoConext."))));
        }
        LWLockRelease(UndoZoneLock);
    }
}

UndoZone* UndoZoneGroup::GetUndoZone(int zid, bool isNeedInitZone)
{
    WHITEBOX_TEST_STUB(UNDO_GET_ZONE_FAILED, WhiteboxDefaultErrorEmit);
    InitUndoCxtUzones();
    if (!IS_VALID_ZONE_ID(zid)) {
        ereport(PANIC, (errmsg(UNDOFORMAT("zoneid %d invalid."), zid)));
    }

    UndoZone *uzone = (UndoZone *)g_instance.undo_cxt.uZones[zid];
    if (uzone == NULL) {
        UndoPersistence upersistence = UNDO_PERMANENT;
        GET_UPERSISTENCE(zid, upersistence);
        if (RecoveryInProgress() || t_thrd.xlog_cxt.InRecovery) {
            AllocateZoneMemory(&uzone, upersistence, zid);
        } else if (isNeedInitZone) {
            if (!UndoZoneGroup::UndoZoneInUse(zid, upersistence)) {
                ereport(PANIC, (errmodule(MOD_UNDO),
                    errmsg(UNDOFORMAT("used zone %d detached, bitmap num %d."),
                    zid, bms_num_members(g_instance.undo_cxt.uZoneBitmap[upersistence]))));
            }
            Assert(undo::UndoZoneGroup::UndoZoneInUse(zid, upersistence));
            AllocateZoneMemory(&uzone, upersistence, zid);
        }
    }
    return uzone;
}

void AllocateZonesBeforXid()
{
    const int MAX_RETRY_TIMES = 3;
    int retry_times = 0;
    WHITEBOX_TEST_STUB(UNDO_ALLOCATE_ZONE_BEFO_XID_FAILED, WhiteboxDefaultErrorEmit);
    if (RecoveryInProgress()) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_INITIATION),
            errmsg("cannot assign TransactionIds during recovery")));
    }

    for (auto i = 0; i < UNDO_PERSISTENCE_LEVELS; i++) {
        UndoPersistence upersistence = static_cast<UndoPersistence>(i);
        int zid = -1;
        if (IS_VALID_ZONE_ID(t_thrd.undo_cxt.zids[upersistence])) {
            continue;
        }
        DECLARE_NODE_NO();
        UndoZoneGroup::InitUndoCxtUzones();
        LWLockAcquire(UndoZoneLock, LW_EXCLUSIVE);
reallocate_zone:
        zid = AllocateUndoZoneId(upersistence);
        if (!IS_VALID_ZONE_ID(zid) || (bms_num_members(g_instance.undo_cxt.uZoneBitmap[i])) == 0) {
            ereport(WARNING,
                (errmsg(UNDOFORMAT("failed to allocate a undo zone, bitmap num %d."),
                bms_num_members(g_instance.undo_cxt.uZoneBitmap[i]))));
            RebuildUndoZoneBitmap();
            goto reallocate_zone;
        }
        int bitMapIdx = (zid - (int)upersistence * PERSIST_ZONE_COUNT) / BITS_PER_BITMAPWORD;
        if (!UndoZoneGroup::UndoZoneInUse(zid, upersistence)) {
            ereport(PANIC, (errmsg(UNDOFORMAT("undo zone %d not inuse, bitmap word %" PRIu64"."),
                zid, g_instance.undo_cxt.uZoneBitmap[i]->words[bitMapIdx])));
        }
        UndoZone *uzone = UndoZoneGroup::GetUndoZone(zid, true);
        if (uzone == NULL) {
            ereport(PANIC, (errmsg(UNDOFORMAT("can not palloc undo zone memory."))));
        }
        if (uzone->GetPersitentLevel() != upersistence) {
            ereport(PANIC, (errmodule(MOD_UNDO),
                errmsg(UNDOFORMAT("zone %d uzonePersistence %d, upersistance %d, bitmap word %" PRIu64"."),
                    zid, uzone->GetPersitentLevel(), upersistence,
                    g_instance.undo_cxt.uZoneBitmap[i]->words[bitMapIdx])));
        }
        if (!uzone->Detached()) {
            ereport(WARNING, (errmodule(MOD_UNDO),
                errmsg(UNDOFORMAT("zone %d attached pid %lu, cur pid %lu, bitmap word %" PRIu64"."),
                    zid, uzone->GetAttachPid(), u_sess->attachPid,
                    g_instance.undo_cxt.uZoneBitmap[i]->words[bitMapIdx])));
            retry_times++;
            if (retry_times >= MAX_RETRY_TIMES) {
                RebuildUndoZoneBitmap();
            }
            goto reallocate_zone;
        }
        uzone->Attach();
        LWLockRelease(UndoZoneLock);
        uzone->InitSlotBuffer();
        Assert(uzone->GetAttachPid() == 0);
        uzone->SetAttachPid(u_sess->attachPid);
        pg_write_barrier();
        t_thrd.undo_cxt.zids[upersistence] = zid;
    }
    return;
}

bool VerifyUndoZone(UndoZone *uzone)
{
    if (u_sess->attr.attr_storage.ustore_verify_level <= USTORE_VERIFY_DEFAULT) {
        return true;
    }

    if (uzone->GetAllocateTSlotPtr() < uzone->GetRecycleTSlotPtr() ||
        uzone->GetInsertURecPtr() < uzone->GetForceDiscardURecPtr() ||
        uzone->GetForceDiscardURecPtr() < uzone->GetDiscardURecPtr() ||
        (TransactionIdIsValid(uzone->GetFrozenXid()) &&
        (TransactionIdFollows(uzone->GetRecycleXid(), uzone->GetFrozenXid()) ||
        TransactionIdFollows(g_instance.undo_cxt.globalFrozenXid, uzone->GetFrozenXid()))) ||
        (TransactionIdIsValid(uzone->GetRecycleXid() &&
        TransactionIdFollows(g_instance.undo_cxt.globalRecycleXid, uzone->GetRecycleXid())))) {
            ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
                "Invalid zone: allocateTSlotPtr %lu, recycleTSlotPtr %lu, frozenxid %lu, recyclexid %lu.",
                uzone->GetAllocateTSlotPtr(), uzone->GetRecycleTSlotPtr(), uzone->GetFrozenXid(),
                uzone->GetRecycleXid()));
    }

    return true;
}
} // namespace undo
