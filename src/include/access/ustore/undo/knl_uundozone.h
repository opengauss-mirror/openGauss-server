/* -------------------------------------------------------------------------
 *
 * knl_uundozone.h
 * c++ code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/undo/knl_uundozone.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __KNL_UUNDOZONE_H__
#define __KNL_UUNDOZONE_H__

#include "access/ustore/undo/knl_uundospace.h"
#include "access/ustore/undo/knl_uundoxlog.h"
#include "access/ustore/undo/knl_uundotxn.h"
#include "access/ustore/undo/knl_uundoapi.h"

namespace undo {
#define UNDOZONE_CLEAN 0
#define UNDOZONE_DIRTY 1

#define UNDO_ZONE_META_VERSION 1

/* Information about the undospace persistency metadata. */
typedef struct UndoZoneMetaInfo {
    uint32 version;
    XLogRecPtr lsn;
    UndoSlotOffset allocate;
    UndoSlotOffset recycle;
    UndoRecPtr insert;
    UndoRecPtr discard;
    UndoRecPtr forceDiscard;
    TransactionId recycleXid;
} UndoZoneMetaInfo;

#define IS_VALID_ZONE_ID(zid) ((zid) >= 0 && (zid) < UNDO_ZONE_COUNT)
    /* The location of undo meta persistence . */
#define UNDO_META_FILE "undo/undometa"
#define UNDO_META_PAGE_SIZE 512
#define UNDO_WRITE_SIZE (UNDO_META_PAGE_SIZE * 8)
#define UNDO_META_PAGE_CRC_LENGTH 4
#define UNDO_ZONE_NUMA_GROUP 64
#define UNDOZONE_COUNT_PER_PAGE ((UNDO_META_PAGE_SIZE - UNDO_META_PAGE_CRC_LENGTH) / sizeof(undo::UndoZoneMetaInfo))
#define UNDOZONE_COUNT_PER_WRITE (UNDOZONE_COUNT_PER_PAGE * 8)

#define UNDOZONE_META_PAGE_COUNT(total, unit, count)                       \
    do {                                                                   \
        count = (total % unit == 0) ? (total / unit) : (total / unit) + 1; \
    } while (0)

#define GET_UPERSISTENCE(zid, upersistence)                                          \
    do {                                                                             \
        if (zid >= (PERSIST_ZONE_COUNT + PERSIST_ZONE_COUNT)) {                      \
            upersistence = UNDO_TEMP;                                                \
        } else if (zid >= PERSIST_ZONE_COUNT) {                                      \
            upersistence = UNDO_UNLOGGED;                                            \
        } else {                                                                     \
            upersistence = UNDO_PERMANENT;                                           \
        }                                                                            \
    } while (0)

/* Find the first 1 and set it to 0, which means the undo zone is used. */
#define ALLOCATE_ZONEID(upersistence, retZid)                                        \
    do {                                                                             \
        LWLockAcquire(UndoZoneLock, LW_EXCLUSIVE);                                   \
        retZid = bms_first_member(g_instance.undo_cxt.uZoneBitmap[upersistence]);    \
        ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("allocate zone %d."), retZid))); \
        LWLockRelease(UndoZoneLock);                                                 \
        if (upersistence == UNDO_UNLOGGED) {                                         \
            retZid += PERSIST_ZONE_COUNT;                                            \
        } else if (upersistence == UNDO_TEMP) {                                      \
            retZid += PERSIST_ZONE_COUNT + PERSIST_ZONE_COUNT;                       \
        }                                                                            \
    } while (0)

/* Reset bitmap to 1 when the zone is released. */
#define RELEASE_ZONEID(upersistence, zid)                                            \
    do {                                                                             \
        if (upersistence == UNDO_UNLOGGED) {                                         \
            zid -= PERSIST_ZONE_COUNT;                                               \
        } else if (upersistence == UNDO_TEMP) {                                      \
            zid -= PERSIST_ZONE_COUNT + PERSIST_ZONE_COUNT;                          \
        }                                                                            \
        LWLockAcquire(UndoZoneLock, LW_EXCLUSIVE);                                   \
        g_instance.undo_cxt.uZoneBitmap[upersistence] =                              \
            bms_add_member(g_instance.undo_cxt.uZoneBitmap[upersistence], zid);      \
        ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("release zone %d."), zid))); \
        LWLockRelease(UndoZoneLock);                                                 \
    } while (0)

#define ZONEID_IS_USED(zid, upersistence)                                            \
    do {                                                                             \
        int tmpZid = zid;                                                            \
        if (upersistence == UNDO_UNLOGGED) {                                         \
            tmpZid = zid - PERSIST_ZONE_COUNT;                                       \
        } else if (upersistence == UNDO_TEMP) {                                      \
            tmpZid = zid - (PERSIST_ZONE_COUNT + PERSIST_ZONE_COUNT);                \
        }                                                                            \
        isZoneFree =                                                                 \
            bms_is_member(tmpZid, g_instance.undo_cxt.uZoneBitmap[upersistence]);    \
    } while (0)

class UndoZone : public BaseObject {
public:
    UndoZone(void);
    UndoSpace undoSpace_;
    UndoSpace slotSpace_;

    // Getter.
    inline int GetZoneId(void)
    {
        return zid_;
    }
    inline UndoSlotBuffer& GetSlotBuffer()
    {
        return buf_;
    }
    inline UndoRecPtr GetInsert(void)
    {
        return MAKE_UNDO_PTR(zid_, insertUndoPtr_);
    }
    inline UndoRecPtr GetDiscard(void)
    {
        return MAKE_UNDO_PTR(zid_, discardUndoPtr_);
    }
    inline UndoRecPtr GetForceDiscard(void)
    {
        return MAKE_UNDO_PTR(zid_, forceDiscard_);
    }
    inline UndoSpace *GetUndoSpace(void)
    {
        return &undoSpace_;
    }
    inline UndoSpace *GetSlotSpace(void)
    {
        return &slotSpace_;
    }
    inline UndoSpace *GetSpace(UndoSpaceType type)
    {
        if (type == UNDO_LOG_SPACE) {
            return GetUndoSpace();
        } else {
            return GetSlotSpace();
        }
    }
    inline XLogRecPtr GetLSN(void)
    {
        return lsn_;
    }
    inline UndoPersistence GetPersitentLevel(void)
    {
        return pLevel_;
    }
    inline UndoSlotPtr GetAllocate(void)
    {
        return MAKE_UNDO_PTR(zid_, allocate_);
    }
    inline UndoSlotPtr GetRecycle(void)
    {
        return MAKE_UNDO_PTR(zid_, recycle_);
    }
    inline UndoSlotPtr GetFrozenSlotPtr(void)
    {
        return frozenSlotPtr_;
    }
    inline TransactionId GetRecycleXid(void)
    {
        return recycleXid_;
    }
    inline TransactionId GetAttachPid(void)
    {
        return attachPid_;
    }
    inline bool Attached(void)
    {
        return attached_ == UNDO_ZONE_ATTACHED;
    }
    // Setter.
    inline void SetZoneId(int zid)
    {
        zid_ = zid;
    }
    inline void SetInsert(UndoRecPtr insert)
    {
        insertUndoPtr_ = UNDO_PTR_GET_OFFSET(insert);
    }
    inline void SetDiscard(UndoRecPtr discard)
    {
        discardUndoPtr_ = UNDO_PTR_GET_OFFSET(discard);
    }
    inline void SetForceDiscard(UndoRecPtr discard)
    {
        forceDiscard_ = UNDO_PTR_GET_OFFSET(discard);
    }
    inline void SetAttachPid(ThreadId attachPid)
    {
        attachPid_ = attachPid;
    }
    inline void SetAllocate(UndoSlotPtr allocate)
    {
        allocate_ = UNDO_PTR_GET_OFFSET(allocate);
    }
    inline void SetFrozenSlotPtr(UndoSlotPtr frozenSlotPtr)
    {
        frozenSlotPtr_ = frozenSlotPtr;
    }
    inline void SetRecycle(UndoSlotPtr recycle)
    {
        recycle_ = UNDO_PTR_GET_OFFSET(recycle);
    }
    inline void SetLSN(XLogRecPtr lsn)
    {
        lsn_ = lsn;
    }
    inline void SetPersitentLevel(UndoPersistence persistLevel)
    {
        pLevel_ = persistLevel;
    }
    inline void SetRecycleXid(TransactionId recycleXid)
    {
        recycleXid_ = recycleXid;
    }
    inline bool Used(void)
    {
        return insertUndoPtr_ != forceDiscard_;
    }
    inline bool Empty(void)
    {
        return insertUndoPtr_ == UNDO_LOG_BLOCK_HEADER_SIZE;
    }
    bool Full(void);
    inline bool NeedCleanUndoSpace(void)
    {
        if (Used() || Attached() || (undoSpace_.Head() == undoSpace_.Tail())) {
            return false;
        }
        return true;
    }
    inline bool NeedCleanSlotSpace(void)
    {
        if (Used() || Attached() || (slotSpace_.Head() == slotSpace_.Tail())) {
            return false;
        }
        return true;
    }

    /* Lock and unlock undozone. */
    void InitLock(void)
    {
        lock_ = LWLockAssign(LWTRANCHE_UNDO_ZONE);
    }
    void LockUndoZone(void)
    {
        (void)LWLockAcquire(lock_, LW_EXCLUSIVE);
    }
    void UnlockUndoZone(void)
    {
        (void)LWLockRelease(lock_);
    }
    void LockInit(void)
    {
        lock_ = LWLockAssign(LWTRANCHE_UNDO_ZONE);
    }
    /* Check and set dirty. */
    bool IsDirty(void)
    {
        return dirty_ == UNDOZONE_DIRTY;
    }
    void MarkClean(void)
    {
        dirty_ = UNDOZONE_CLEAN;
    }
    void MarkDirty(void)
    {
        dirty_ = UNDOZONE_DIRTY;
    }
    inline void Attach(void)
    {
        attached_ = UNDO_ZONE_ATTACHED;
    }
    inline void Detach(void)
    {
        attached_ = UNDO_ZONE_DETACHED;
    }
    void AdvanceInsert(uint64 oldInsert, uint64 size)
    {
        insertUndoPtr_ = UNDO_LOG_OFFSET_PLUS_USABLE_BYTES(oldInsert, size);
    }
    UndoRecPtr CalculateInsert(uint64 oldInsert, uint64 size)
    {
        return MAKE_UNDO_PTR(zid_, UNDO_LOG_OFFSET_PLUS_USABLE_BYTES(oldInsert, size));
    }
    void InitSlotBuffer()
    {
        buf_.Init();
    }
    void ReleaseSlotBuffer()
    {
        buf_.Release();
    }
    void NotKeepBuffer()
    {
        buf_.NotKeepBuffer();
    }
    inline uint64 UndoSize(void)
    {
        if (insertUndoPtr_ < forceDiscard_) {
            ereport(PANIC, (errmodule(MOD_UNDO),
                errmsg(UNDOFORMAT("insertUndoPtr_ %lu < forceDiscard_ %lu."),
                    insertUndoPtr_, forceDiscard_)));
        }
        return ((insertUndoPtr_ - forceDiscard_) / BLCKSZ);
    }
    inline uint64 SlotSize(void)
    {
        if (allocate_ < recycle_) {
            ereport(PANIC, (errmodule(MOD_UNDO),
                errmsg(UNDOFORMAT("allocate_ %lu < recycle_ %lu."), allocate_, recycle_)));
        }
        return ((allocate_ - recycle_) / BLCKSZ);
    }
    bool CheckNeedSwitch(UndoRecordSize size);
    UndoRecordState CheckUndoRecordValid(UndoLogOffset offset, bool checkForceRecycle);
    bool CheckRecycle(UndoRecPtr starturp, UndoRecPtr endurp);

    UndoRecPtr AllocateSpace(uint64 size);
    void ReleaseSpace(UndoRecPtr starturp, UndoRecPtr endurp, int *forceRecycleSize);
    UndoRecPtr AllocateSlotSpace(void);
    void ReleaseSlotSpace(UndoRecPtr starturp, UndoRecPtr endurp, int *forceRecycleSize);
    void CleanUndoSpace(void);
    void CleanSlotSpace(void);
    void PrepareSwitch(void);
    UndoSlotOffset GetNextSlotPtr(UndoSlotOffset slotPtr);

    /* Drop all buffers for the given undo log. */
    void ForgetUndoBuffer(UndoLogOffset start, UndoLogOffset end, uint32 dbId);
    void PrepareTransactionSlot(UndoSlotOffset slotOffset);
    TransactionSlot *AllocTransactionSlot(UndoSlotPtr slotPtr, TransactionId xid, Oid dbid);

    /* Checkpoint meta of undospace. */
    static void CheckPointUndoZone(int fd);

    /* Recovery undospace info from persistent file. */
    static void RecoveryUndoZone(int fd);

private:
    static const int UNDO_ZONE_ATTACHED = 1;
    static const int UNDO_ZONE_DETACHED = 0;
    volatile uint32 attached_;
    UndoSlotBuffer buf_;
    UndoSlotOffset allocate_;
    UndoSlotOffset recycle_;
    UndoSlotPtr frozenSlotPtr_;
    UndoLogOffset insertUndoPtr_;
    UndoLogOffset discardUndoPtr_;
    UndoLogOffset forceDiscard_;
    UndoPersistence pLevel_;
    TransactionId recycleXid_;
    ThreadId attachPid_;
    /* Need Lock undo zone before alloc, preventing from checkpoint. */
    LWLock *lock_;
    /* Lsn for undo zone meta. */
    XLogRecPtr lsn_;
    /* Check whether undo zone is dirty. */
    bool dirty_;
    /* Zone id. */
    int zid_;
}; // class UndoZone

class UndoZoneGroup {
public:
    static int AllocateZone(UndoPersistence upersistence);
    static void ReleaseZone(int zid, UndoPersistence upersistence);
    static UndoZone *SwitchZone(int zid, UndoPersistence upersistence);
    static UndoZone *GetUndoZone(int zid, bool isNeedInitZone = true, UndoPersistence upersistence = UNDO_PERMANENT);
    static void InitUndoCxtUzones();
}; // class UndoZoneGroup

void AllocateZonesBeforXid();
void InitZone(UndoZone *uzone, const int zoneId, UndoPersistence upersistence);
void InitUndoSpace(UndoZone *uzone, UndoSpaceType type);
} // namespace undo
#endif // __KNL_UUNDOZONE_H__
