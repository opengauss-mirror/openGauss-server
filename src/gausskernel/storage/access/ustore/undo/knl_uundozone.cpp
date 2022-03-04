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

#include "access/ustore/undo/knl_uundozone.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/undo/knl_uundotxn.h"
#include "access/ustore/undo/knl_uundospace.h"
#include "access/ustore/knl_whitebox_test.h"
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

UndoZone::UndoZone() : attached_(UNDO_ZONE_DETACHED), pLevel_(UNDO_PERSISTENT_BUTT),
    lsn_(0), dirty_(UNDOZONE_CLEAN) {}

bool UndoZone::Full(void)
{
    return insertUndoPtr_ == UNDO_LOG_MAX_SIZE;
}

bool UndoZone::CheckNeedSwitch(UndoRecordSize size)
{
    UndoLogOffset newInsert = UNDO_LOG_OFFSET_PLUS_USABLE_BYTES(insertUndoPtr_, size);
    if (unlikely(newInsert > UNDO_LOG_MAX_SIZE)) {
        return true;
    }
    return false;
}

bool UndoZone::CheckRecycle(UndoRecPtr starturp, UndoRecPtr endurp)
{
    int startZid = UNDO_PTR_GET_ZONE_ID(starturp);
    int endZid = UNDO_PTR_GET_ZONE_ID(endurp);
    UndoLogOffset start = UNDO_PTR_GET_OFFSET(starturp);
    UndoLogOffset end = UNDO_PTR_GET_OFFSET(endurp);

    if (start != forceDiscard_) {
        ereport(WARNING, (errmsg(
            UNDOFORMAT("Check undo recycle_ fail, insert(%lu), discard(%lu), forceDiscard(%lu), start(%lu), end(%lu)."),
            insertUndoPtr_, discardUndoPtr_, forceDiscard_, start, end)));
    }

    WHITEBOX_TEST_STUB(UNDO_CHECK_RECYCLE_FAILED, WhiteboxDefaultErrorEmit);

    if (unlikely(startZid != endZid)) {
        ereport(PANIC, (errmsg(UNDOFORMAT("Check undo recycle_ fail(switch), startZid(%d), endZid(%d), insert(%lu), "
                                          "discard(%lu), forceDiscard(%lu), start(%lu), end(%lu)."),
            startZid, endZid, insertUndoPtr_, discardUndoPtr_, forceDiscard_, start, end)));
        return false;
    }

    if ((forceDiscard_ <= insertUndoPtr_) && (end <= insertUndoPtr_) && (start < end)) {
        return true;
    }
    ereport(PANIC, (errmsg(UNDOFORMAT(
        "Check undo recycle_ fail, zid(%d), insert(%lu), discard(%lu), forceDiscard(%lu), start(%lu), end(%lu)."),
        startZid, insertUndoPtr_, discardUndoPtr_, forceDiscard_, start, end)));
    return false;
}

/* 
 * Check whether the undo record is discarded or not. If it's already discarded
 * return false otherwise return true. Caller must hold the space discardLock_.
 */
UndoRecordState UndoZone::CheckUndoRecordValid(UndoLogOffset offset, bool checkForceRecycle)
{
    Assert((offset < UNDO_LOG_MAX_SIZE) && (offset >= UNDO_LOG_BLOCK_HEADER_SIZE));
    Assert(forceDiscard_ <= insertUndoPtr_);

    if (offset > this->insertUndoPtr_) {
        ereport(DEBUG1, (errmsg(UNDOFORMAT("The undo record not insert yet: zid=%d, insert=%lu, offset=%lu."),
            this->zid_, this->insertUndoPtr_, offset)));
        return UNDO_RECORD_NOT_INSERT;
    }
    if (offset >= this->forceDiscard_) {
        return UNDO_RECORD_NORMAL;
    }
    if (offset >= this->discardUndoPtr_ && checkForceRecycle) {
        TransactionId recycleXmin;
        TransactionId oldestXmin = GetOldestXminForUndo(&recycleXmin);
        if (TransactionIdPrecedes(recycleXid_, recycleXmin)) {
            ereport(DEBUG1, (errmsg(
                UNDOFORMAT("oldestxmin %lu, recycleXmin %lu > recyclexid %lu: zid=%d,"
                "forceDiscard=%lu, discard=%lu, offset=%lu."),
                oldestXmin, recycleXmin, recycleXid_, this->zid_, this->forceDiscard_,
                this->discardUndoPtr_, offset)));
            return UNDO_RECORD_DISCARD;
        }
        ereport(LOG, (errmsg(UNDOFORMAT("The record has been force recycled: zid=%d, forceDiscard=%lu, "
            "discard=%lu, offset=%lu."), this->zid_, this->forceDiscard_, this->discardUndoPtr_, offset)));
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
    TransactionSlot *slot = buf_.FetchTransactionSlot(slotPtr);
    slot->Init(xid, dbid);
    Assert(slot->DbId() == u_sess->proc_cxt.MyDatabaseId);
    allocate_ = UNDO_PTR_GET_OFFSET(undo::GetNextSlotPtr(slotPtr));
    Assert(allocate_ >= recycle_);
    ereport(DEBUG1, (errmodule(MOD_UNDO),
        errmsg(UNDOFORMAT("zone %d allocate_ transaction slot %lu xid %lu dbid %u."),
            zid_, slotPtr, xid, dbid)));
#ifdef DEBUG_UHEAP
    UHEAPSTAT_COUNT_UNDO_SLOTS_ALLOCATE();
#endif
    return slot;
}

UndoRecPtr UndoZone::AllocateSpace(uint64 size)
{
    UndoLogOffset oldInsert = insertUndoPtr_;
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
    UndoSlotOffset oldInsert = allocate_;
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
    buf_.PrepareTransactionSlot(slotPtr, UNDOSLOTBUFFER_KEEP | UNDOSLOTBUFFER_RBM);
    return slotPtr;
}

/* Release undo space from starturp to endurp and advance discard. */
void UndoZone::ReleaseSpace(UndoRecPtr starturp, UndoRecPtr endurp, int *forceRecycleSize)
{
    UndoLogOffset start = UNDO_PTR_GET_OFFSET(starturp);
    UndoLogOffset end = UNDO_PTR_GET_OFFSET(endurp);
    int startSegno = (int)(undoSpace_.Head() / UNDO_LOG_SEGMENT_SIZE);
    int endSegno = (int)(end / UNDO_LOG_SEGMENT_SIZE);

    ForgetUndoBuffer(start, end, UNDO_DB_OID);
    if (unlikely(startSegno < endSegno)) {
        if (unlikely(*forceRecycleSize > 0)) {
            *forceRecycleSize -= (int)(endSegno - startSegno) * UNDOSEG_SIZE;
        }
        ForgetUndoBuffer(startSegno * UNDO_LOG_SEGMENT_SIZE, endSegno * UNDO_LOG_SEGMENT_SIZE, UNDO_DB_OID);
        undoSpace_.LockSpace();
        UndoRecPtr prevHead = MAKE_UNDO_PTR(zid_, undoSpace_.Head());
        undoSpace_.UnlinkUndoLog(zid_, endSegno * UNDO_LOG_SEGMENT_SIZE, UNDO_DB_OID);
        Assert(undoSpace_.Head() <= insertUndoPtr_);
        if (pLevel_ == UNDO_PERMANENT) {
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

/* Release slot space from starturp to endurp and advance discard. */
void UndoZone::ReleaseSlotSpace(UndoRecPtr startSlotPtr, UndoRecPtr endSlotPtr, int *forceRecycleSize)
{
    UndoLogOffset start = UNDO_PTR_GET_OFFSET(startSlotPtr);
    UndoLogOffset end = UNDO_PTR_GET_OFFSET(endSlotPtr);
    int startSegno = (int)(slotSpace_.Head() / UNDO_META_SEGMENT_SIZE);
    int endSegno = (int)(end / UNDO_META_SEGMENT_SIZE);

    ForgetUndoBuffer(start, end, UNDO_SLOT_DB_OID);
    if (unlikely(startSegno < endSegno)) {
        if (unlikely(*forceRecycleSize > 0)) {
            *forceRecycleSize -= (int)(endSegno - startSegno) * UNDO_META_SEG_SIZE;
        }
        ForgetUndoBuffer(startSegno * UNDO_META_SEGMENT_SIZE, endSegno * UNDO_META_SEGMENT_SIZE, UNDO_SLOT_DB_OID);
        slotSpace_.LockSpace();
        UndoRecPtr prevHead = MAKE_UNDO_PTR(zid_, slotSpace_.Head());
        slotSpace_.UnlinkUndoLog(zid_, endSegno * UNDO_META_SEGMENT_SIZE, UNDO_SLOT_DB_OID);
        Assert(slotSpace_.Head() <= allocate_);
        if (pLevel_ == UNDO_PERMANENT) {
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

void UndoZone::PrepareSwitch(void)
{
    WHITEBOX_TEST_STUB(UNDO_PREPARE_SWITCH_FAILED, WhiteboxDefaultErrorEmit);

    if (attached_ == UNDO_ZONE_DETACHED || undoSpace_.Tail() != UNDO_LOG_MAX_SIZE) {
        ereport(PANIC, (errmsg(UNDOFORMAT(
            "Undo space switch fail, expect tail(%lu), real tail(%lu), attached(%d)."),
            UNDO_LOG_MAX_SIZE, undoSpace_.Tail(), attached_)));
    }
    if (pLevel_ == UNDO_PERMANENT) {
        LockUndoZone();
        MarkDirty();
        insertUndoPtr_ = UNDO_LOG_MAX_SIZE;
        UnlockUndoZone();
    }
    return;
}

/* Clean undo space from head to tail. */
void UndoZone::CleanUndoSpace(void)
{
    WHITEBOX_TEST_STUB(UNDO_CLEAN_SPACE_FAILED, WhiteboxDefaultErrorEmit);
    
    LockUndoZone();

    if (!NeedCleanUndoSpace()) {
        UnlockUndoZone();
        return;
    }

    if (insertUndoPtr_ != forceDiscard_) {
        ereport(PANIC, (errmsg(UNDOFORMAT("zone %d not used but insert %lu != discard %lu."),
            zid_, insertUndoPtr_, discardUndoPtr_)));
    }

    undoSpace_.LockSpace();
    if (undoSpace_.Head() == undoSpace_.Tail()) {
        undoSpace_.UnlockSpace();
        return;
    } else if (undoSpace_.Head() + UNDO_LOG_SEGMENT_SIZE != undoSpace_.Tail()) {
        ereport(PANIC, (errmsg(UNDOFORMAT("zone %d not used but head %lu + segment size != tail %lu."),
            zid_, undoSpace_.Head(), undoSpace_.Tail())));
    }
    Assert(forceDiscard_ % UNDO_LOG_SEGMENT_SIZE != 0);
    UndoLogOffset newTail = undoSpace_.Head();
    undoSpace_.UnlinkUndoLog(zid_, undoSpace_.Tail(), UNDO_DB_OID);
    undoSpace_.SetHead(newTail);
    undoSpace_.SetTail(newTail);
    if (undoSpace_.Head() > insertUndoPtr_) {
        ereport(PANIC, (errmsg(UNDOFORMAT("zone %d head %lu > insert %lu."),
            zid_, undoSpace_.Head(), insertUndoPtr_)));
    }
    if (pLevel_ == UNDO_PERMANENT) {
        START_CRIT_SECTION();
        undoSpace_.MarkDirty();
        XlogUndoClean undoClean;
        undoClean.tail = MAKE_UNDO_PTR(zid_, undoSpace_.Tail());
        XLogRecPtr lsn = WriteUndoXlog(&undoClean, XLOG_UNDO_CLEAN);
        undoSpace_.SetLSN(lsn);
        END_CRIT_SECTION();
    }
    undoSpace_.UnlockSpace();
    UnlockUndoZone();

    return;
}

/* Clean slot space from head to tail. */
void UndoZone::CleanSlotSpace(void)
{
    WHITEBOX_TEST_STUB(UNDO_CLEAN_SLOT_SPACE_FAILED, WhiteboxDefaultErrorEmit);

    LockUndoZone();

    if (!NeedCleanSlotSpace()) {
        UnlockUndoZone();
        return;
    }

    if (insertUndoPtr_ != forceDiscard_) {
        ereport(PANIC, (errmsg(UNDOFORMAT("zone %d not used but insert %lu != discard %lu."),
            zid_, insertUndoPtr_, discardUndoPtr_)));
    }

    slotSpace_.LockSpace();
    if (slotSpace_.Head() == slotSpace_.Tail()) {
        slotSpace_.UnlockSpace();
        return;
    } else if (slotSpace_.Head() + UNDO_META_SEGMENT_SIZE != slotSpace_.Tail()) {
        ereport(PANIC, (errmsg(UNDOFORMAT("zone %d not used but head %lu + segment size != tail %lu."),
            zid_, slotSpace_.Head(), slotSpace_.Tail())));
    }
    Assert(forceDiscard_ % UNDO_META_SEGMENT_SIZE != 0);
    UndoLogOffset newTail = slotSpace_.Head();
    slotSpace_.UnlinkUndoLog(zid_, slotSpace_.Tail(), UNDO_SLOT_DB_OID);
    slotSpace_.SetHead(newTail);
    slotSpace_.SetTail(newTail);
    if (slotSpace_.Head() > insertUndoPtr_) {
        ereport(PANIC, (errmsg(UNDOFORMAT("zone %d head %lu > insert %lu."),
            zid_, slotSpace_.Head(), insertUndoPtr_)));
    }
    if (pLevel_ == UNDO_PERMANENT) {
        START_CRIT_SECTION();
        slotSpace_.MarkDirty();
        XlogUndoClean undoClean;
        undoClean.tail = MAKE_UNDO_PTR(zid_, slotSpace_.Tail());
        XLogRecPtr lsn = WriteUndoXlog(&undoClean, XLOG_SLOT_CLEAN);
        slotSpace_.SetLSN(lsn);
        END_CRIT_SECTION();
    }
    slotSpace_.UnlockSpace();
    UnlockUndoZone();

    return;
}

/* Initialize parameters in undo meta. */
static void InitUndoMeta(UndoZoneMetaInfo *uspMetaPointer)
{
    uspMetaPointer->version = UNDO_ZONE_META_VERSION;
    uspMetaPointer->lsn = 0;
    uspMetaPointer->insert = UNDO_LOG_BLOCK_HEADER_SIZE;
    uspMetaPointer->discard = UNDO_LOG_BLOCK_HEADER_SIZE;
    uspMetaPointer->forceDiscard = UNDO_LOG_BLOCK_HEADER_SIZE;
    uspMetaPointer->recycleXid = 0;
    uspMetaPointer->allocate = UNDO_LOG_BLOCK_HEADER_SIZE;
    uspMetaPointer->recycle = UNDO_LOG_BLOCK_HEADER_SIZE;
}

/* Get undo meta info from undo zone. */
static void GetMetaFromUzone(UndoZone *uzone, UndoZoneMetaInfo *uspMetaPointer)
{
    uspMetaPointer->version = UNDO_ZONE_META_VERSION;
    uspMetaPointer->lsn = uzone->GetLSN();
    uspMetaPointer->insert = UNDO_PTR_GET_OFFSET(uzone->GetInsert());
    uspMetaPointer->discard = UNDO_PTR_GET_OFFSET(uzone->GetDiscard());
    uspMetaPointer->forceDiscard = UNDO_PTR_GET_OFFSET(uzone->GetForceDiscard());
    uspMetaPointer->recycleXid = uzone->GetRecycleXid();
    uspMetaPointer->allocate = UNDO_PTR_GET_OFFSET(uzone->GetAllocate());
    uspMetaPointer->recycle = UNDO_PTR_GET_OFFSET(uzone->GetRecycle());
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
                    if (UNDO_PTR_GET_OFFSET(uzone->GetInsert()) == UNDO_LOG_MAX_SIZE &&
                        uzone->GetAllocate() == uzone->GetRecycle()) {
                        GetMetaFromUzone(uzone, uspMetaPointer);
                        uzone->NotKeepBuffer();
                        uzone->UnlockUndoZone();
                        ereport(LOG, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("release zone memory %d."), cycle)));
                        delete(uzone);
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

static void AllocateZoneMemory(UndoZone **uzone, UndoPersistence upersistence, int zid, bool needCheckZone)
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
        InitUndoSpace(*uzone, UNDO_LOG_SPACE);
        InitUndoSpace(*uzone, UNDO_SLOT_SPACE);
        ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("attached free zone %d."), zid)));
    }

    if (needCheckZone) {
        bool isZoneFree = true;
        ZONEID_IS_USED(zid, upersistence);
        if (isZoneFree) {
            ereport(PANIC, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("allocate zone fail."))));
        }
    }
    (*uzone)->InitSlotBuffer();
    pg_write_barrier();
    g_instance.undo_cxt.uZones[zid] = *uzone;
}

static void RecoveryZone(UndoZone *uzone,
    const UndoZoneMetaInfo *uspMetaInfo, const int zoneId)
{
    uzone->InitLock();
    uzone->SetZoneId(zoneId);
    uzone->SetPersitentLevel(UNDO_PERMANENT);
    uzone->SetLSN(uspMetaInfo->lsn);
    uzone->SetInsert(uspMetaInfo->insert);
    uzone->SetDiscard(uspMetaInfo->discard);
    uzone->SetForceDiscard(uspMetaInfo->forceDiscard);
    uzone->SetAllocate(uspMetaInfo->allocate);
    uzone->SetRecycle(uspMetaInfo->recycle);
    uzone->SetRecycleXid(uspMetaInfo->recycleXid);
}

/* Initialize parameters in the undo zone. */
void InitZone(UndoZone *uzone, const int zoneId, UndoPersistence upersistence)
{
    uzone->InitLock();
    uzone->SetZoneId(zoneId);
    uzone->SetPersitentLevel(upersistence);
    uzone->SetLSN(0);
    uzone->SetInsert(UNDO_LOG_BLOCK_HEADER_SIZE);
    uzone->SetDiscard(UNDO_LOG_BLOCK_HEADER_SIZE);
    uzone->SetForceDiscard(UNDO_LOG_BLOCK_HEADER_SIZE);
    uzone->SetAllocate(UNDO_LOG_BLOCK_HEADER_SIZE);
    uzone->SetRecycle(UNDO_LOG_BLOCK_HEADER_SIZE);
    uzone->SetFrozenSlotPtr(INVALID_UNDO_SLOT_PTR);
    uzone->SetRecycleXid(InvalidTransactionId);
    uzone->SetAttachPid(u_sess->attachPid);
}

/* Initialize parameters in the undo space. */
void InitUndoSpace(UndoZone *uzone, UndoSpaceType type)
{
    UndoSpace *usp = uzone->GetSpace(type);
    usp->LockInit();
    usp->MarkClean();
    usp->SetLSN(0);
    usp->SetHead(0);
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
    oldContext = MemoryContextSwitchTo(g_instance.undo_cxt.undoContext);

    UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOZONE_COUNT_PER_PAGE, totalPageCnt);
    zoneMetaSize = totalPageCnt * UNDO_META_PAGE_SIZE / BLCKSZ;
    g_instance.undo_cxt.undoMetaSize += zoneMetaSize;

    /* Ensure read at start posistion of file. */
    lseek(fd, 0, SEEK_SET);

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
        if (uspMetaInfo->insert == UNDO_LOG_MAX_SIZE && uspMetaInfo->allocate == uspMetaInfo->recycle) {
            LWLockAcquire(UndoZoneLock, LW_EXCLUSIVE);
            g_instance.undo_cxt.uZoneBitmap[UNDO_PERMANENT] =
                bms_del_member(g_instance.undo_cxt.uZoneBitmap[UNDO_PERMANENT], zoneId);
            LWLockRelease(UndoZoneLock);
            continue;
        }
        if (uspMetaInfo->insert != UNDO_LOG_BLOCK_HEADER_SIZE) {
            undo::UndoZoneGroup::InitUndoCxtUzones();
            UndoZone *uzone = (UndoZone *)g_instance.undo_cxt.uZones[zoneId];
            AllocateZoneMemory(&uzone, UNDO_PERMANENT, zoneId, false);
            RecoveryZone(uzone, uspMetaInfo, zoneId);
        }
    }
    pfree(persistBlock);
}

void UndoZoneGroup::ReleaseZone(int zid, UndoPersistence upersistence)
{
    Assert(IS_VALID_ZONE_ID(zid));
    WHITEBOX_TEST_STUB(UNDO_RELEASE_ZONE_FAILED, WhiteboxDefaultErrorEmit);
    if (g_instance.undo_cxt.uZones == NULL || g_instance.undo_cxt.uZoneCount == 0) {
        return;
    }
    UndoZone *uzone = (UndoZone *)g_instance.undo_cxt.uZones[zid];

    bool isZoneFree = true;
    ZONEID_IS_USED(zid, upersistence);
    if (isZoneFree) {
        ereport(PANIC, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("release zone fail."))));
    }
    if (uzone != NULL) {
        uzone->NotKeepBuffer();
        uzone->SetAttachPid(0);
    }
    pg_write_barrier();
    DECLARE_NODE_NO();
    RELEASE_ZONEID(upersistence, zid);
}

UndoZone *UndoZoneGroup::SwitchZone(int zid, UndoPersistence upersistence)
{
    if (g_instance.undo_cxt.uZoneCount > g_instance.undo_cxt.undoCountThreshold) {
        ereport(FATAL, (errmsg(UNDOFORMAT("Too many undo zones are requested, max count is %d, now is %d"),
            g_instance.undo_cxt.undoCountThreshold, g_instance.undo_cxt.uZoneCount)));
    }
    InitUndoCxtUzones();
    UndoZone *uzone = (UndoZone *)g_instance.undo_cxt.uZones[zid];
    int retZid = -1;
    uzone->PrepareSwitch();

    DECLARE_NODE_NO();
    DECLARE_NODE_COUNT();
    ALLOCATE_ZONEID(upersistence, retZid);
    if (!IS_VALID_ZONE_ID(retZid)) {
        ereport(ERROR, (errmsg("SwitchZone: zone id is invalid, there're too many working threads.")));
    }
    uzone = (UndoZone *)g_instance.undo_cxt.uZones[retZid];
    if (uzone == NULL) {
        MemoryContext oldContext = MemoryContextSwitchTo(g_instance.undo_cxt.undoContext);
        uzone = New(g_instance.undo_cxt.undoContext) UndoZone();
        MemoryContextSwitchTo(oldContext);
        if (uzone == NULL) {
            RELEASE_ZONEID(upersistence, retZid);
            ereport(ERROR, (errmsg(UNDOFORMAT("failed to allocate memory for zone: %d."), retZid)));
        }
        pg_atomic_fetch_add_u32(&g_instance.undo_cxt.uZoneCount, 1);
        InitZone(uzone, retZid, UNDO_PERMANENT);
        InitUndoSpace(uzone, UNDO_LOG_SPACE);
        InitUndoSpace(uzone, UNDO_SLOT_SPACE);
        uzone->InitSlotBuffer();
        pg_write_barrier();
        g_instance.undo_cxt.uZones[retZid] = uzone;
    }

    WHITEBOX_TEST_STUB(UNDO_SWITCH_ZONE_FAILED, WhiteboxDefaultErrorEmit);

    if (IS_VALID_ZONE_ID(retZid)) {
        if (uzone->GetUndoSpace()->Tail() != 0) {
            ereport(PANIC, (errmsg(UNDOFORMAT("new zone is not empty, tail=%lu."), uzone->GetUndoSpace()->Tail())));
        }
        uzone->InitSlotBuffer();
        t_thrd.undo_cxt.zids[upersistence] = retZid;
    } else {
        ereport(ERROR, (errmsg(UNDOFORMAT("failed to switch a new zone."))));
    }
    ereport(LOG, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("old zone %d switch to new zone %d."), zid, retZid)));
    return uzone;
}

void UndoZoneGroup:: InitUndoCxtUzones()
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

UndoZone* UndoZoneGroup::GetUndoZone(int zid, bool isNeedInitZone, UndoPersistence upersistence)
{
    WHITEBOX_TEST_STUB(UNDO_GET_ZONE_FAILED, WhiteboxDefaultErrorEmit);

    if (!IS_VALID_ZONE_ID(zid)) {
        ereport(PANIC, (errmsg(UNDOFORMAT("zone id %d invalid."), zid)));
    }
    InitUndoCxtUzones();

    UndoPersistence upersistenceFromZoneid = UNDO_PERMANENT;
    GET_UPERSISTENCE(zid, upersistenceFromZoneid);
    UndoZone *uzone = (UndoZone *)g_instance.undo_cxt.uZones[zid];
    if (uzone == NULL) {
        if (RecoveryInProgress()) {
            /* False equals to 0, which means zone is used; true is 1, which means zone is unused. */
            bool isZoneFree = bms_is_member(zid, g_instance.undo_cxt.uZoneBitmap[upersistenceFromZoneid]);
            if (!isZoneFree) {
                /* If the zone is full, return NULL to skip redo process. */
                return NULL;
            }
            AllocateZoneMemory(&uzone, upersistenceFromZoneid, zid, false);
        } else {
            if (isNeedInitZone) {
                AllocateZoneMemory(&uzone, upersistenceFromZoneid, zid, true);
            }
        }
    }
    if (uzone != NULL && uzone->GetAttachPid() == 0) {
        uzone->SetAttachPid(u_sess->attachPid);
    }
    return uzone;
}

void AllocateZonesBeforXid()
{
    WHITEBOX_TEST_STUB(UNDO_ALLOCATE_ZONE_BEFO_XID_FAILED, WhiteboxDefaultErrorEmit);

    for (auto i = 0; i < UNDO_PERSISTENCE_LEVELS; i++) {
        UndoPersistence upersistence = static_cast<UndoPersistence>(i);
        int zid = -1;
        if (IS_VALID_ZONE_ID(t_thrd.undo_cxt.zids[upersistence]))
            continue;
        DECLARE_NODE_NO();
        ALLOCATE_ZONEID(upersistence, zid);
        if (!IS_VALID_ZONE_ID(zid)) {
            ereport(ERROR, (errmsg(UNDOFORMAT("failed to allocate a zone"))));
            return;
        }
        if (g_instance.undo_cxt.uZones != NULL) {
            UndoZone *uzone = (UndoZone *)g_instance.undo_cxt.uZones[zid];
            if (uzone != NULL) {
                uzone->InitSlotBuffer();
            }
        }
        t_thrd.undo_cxt.zids[upersistence] = zid;
    }
    return;
}
} // namespace undo
