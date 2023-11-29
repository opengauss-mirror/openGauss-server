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
    UndoSlotOffset allocateTSlotPtr;
    UndoSlotOffset recycleTSlotPtr;
    UndoRecPtr insertURecPtr;
    UndoRecPtr discardURecPtr;
    UndoRecPtr forceDiscardURecPtr;
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
        upersistence = (UndoPersistence)(zid / (int)PERSIST_ZONE_COUNT);             \
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
    inline UndoRecPtr GetInsertURecPtr(void)
    {
        return MAKE_UNDO_PTR(zid_, insertURecPtr_);
    }
    inline UndoRecPtr GetDiscardURecPtr(void)
    {
        return MAKE_UNDO_PTR(zid_, discardURecPtr_);
    }
    inline UndoRecPtr GetForceDiscardURecPtr(void)
    {
        return MAKE_UNDO_PTR(zid_, forceDiscardURecPtr_);
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
    inline UndoSlotPtr GetAllocateTSlotPtr(void)
    {
        return MAKE_UNDO_PTR(zid_, allocateTSlotPtr_);
    }
    inline UndoSlotPtr GetRecycleTSlotPtr(void)
    {
        return MAKE_UNDO_PTR(zid_, recycleTSlotPtr_);
    }
    inline UndoSlotPtr get_recycle_tslot_ptr_exrto(void)
    {
        return MAKE_UNDO_PTR(zid_, recycle_tslot_ptr_exrto);
    }
    inline UndoSlotPtr GetFrozenSlotPtr(void)
    {
        return frozenSlotPtr_;
    }
    inline TransactionId GetRecycleXid(void)
    {
        return recycleXid_;
    }
    inline TransactionId get_recycle_xid_exrto(void)
    {
        return recycle_xid_exrto;
    }
    inline TransactionId GetFrozenXid(void)
    {
        return frozenXid_;
    }
    inline ThreadId GetAttachPid(void)
    {
        return attachPid_;
    }
    inline bool Attached(void)
    {
        return pg_atomic_read_u32(&attached_) == UNDO_ZONE_ATTACHED;
    }
    inline bool Detached(void)
    {
        return pg_atomic_read_u32(&attached_) == UNDO_ZONE_DETACHED;
    }
    // Setter.
    inline void SetZoneId(int zid)
    {
        zid_ = zid;
    }
    inline void SetInsertURecPtr(UndoRecPtr insert)
    {
        insertURecPtr_ = UNDO_PTR_GET_OFFSET(insert);
    }
    inline void SetDiscardURecPtr(UndoRecPtr discard)
    {
        discardURecPtr_ = UNDO_PTR_GET_OFFSET(discard);
    }
    inline void set_discard_urec_ptr_exrto(UndoRecPtr discard)
    {
        discard_urec_ptr_exrto = UNDO_PTR_GET_OFFSET(discard);
    }
    inline void SetForceDiscardURecPtr(UndoRecPtr discard)
    {
        forceDiscardURecPtr_ = UNDO_PTR_GET_OFFSET(discard);
    }
    inline void set_force_discard_urec_ptr_exrto(UndoRecPtr discard)
    {
        force_discard_urec_ptr_exrto = UNDO_PTR_GET_OFFSET(discard);
    }
    inline void SetAttachPid(ThreadId attachPid)
    {
        attachPid_ = attachPid;
    }
    inline void SetAllocateTSlotPtr(UndoSlotPtr allocate)
    {
        allocateTSlotPtr_ = UNDO_PTR_GET_OFFSET(allocate);
    }
    inline void SetFrozenSlotPtr(UndoSlotPtr frozenSlotPtr)
    {
        frozenSlotPtr_ = frozenSlotPtr;
    }
    inline void SetRecycleTSlotPtr(UndoSlotPtr recycle)
    {
        recycleTSlotPtr_ = UNDO_PTR_GET_OFFSET(recycle);
    }
    inline void set_recycle_tslot_ptr_exrto(UndoSlotPtr recycle)
    {
        recycle_tslot_ptr_exrto = UNDO_PTR_GET_OFFSET(recycle);
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
    inline void set_recycle_xid_exrto(TransactionId recycle_xid)
    {
        recycle_xid_exrto = recycle_xid;
    }
    inline void SetFrozenXid(TransactionId frozenXid)
    {
        frozenXid_ = frozenXid;
    }
    inline void SetAttach(uint32 attach)
    {
        attached_ = attach;
    }
    inline bool Used(void)
    {
        return insertURecPtr_ != forceDiscardURecPtr_;
    }
    inline bool Used_exrto(void)
    {
        return insertURecPtr_ != force_discard_urec_ptr_exrto;
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
    inline bool Attach(void)
    {
        uint32 expected = UNDO_ZONE_DETACHED;
        while (!pg_atomic_compare_exchange_u32(&attached_, &expected, UNDO_ZONE_ATTACHED)) {
            expected = UNDO_ZONE_DETACHED;
        }
        return true;
    }
    inline bool Detach(void)
    {
        uint32 expected = UNDO_ZONE_ATTACHED;
        while (!pg_atomic_compare_exchange_u32(&attached_, &expected, UNDO_ZONE_DETACHED)) {
            expected = UNDO_ZONE_ATTACHED;
        }
        return true;
    }
    void AdvanceInsertURecPtr(uint64 oldInsert, uint64 size)
    {
        insertURecPtr_ = UNDO_LOG_OFFSET_PLUS_USABLE_BYTES(oldInsert, size);
    }
    UndoRecPtr CalculateInsertURecPtr(uint64 oldInsert, uint64 size)
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
        if (insertURecPtr_ < forceDiscardURecPtr_) {
            ereport(PANIC, (errmodule(MOD_UNDO),
                errmsg(UNDOFORMAT("insertURecPtr_ %lu < forceDiscardURecPtr_ %lu."),
                    insertURecPtr_, forceDiscardURecPtr_)));
        }
        return ((insertURecPtr_ - forceDiscardURecPtr_) / BLCKSZ);
    }
    inline uint64 SlotSize(void)
    {
        if (allocateTSlotPtr_ < recycleTSlotPtr_) {
            ereport(PANIC, (errmodule(MOD_UNDO),
                errmsg(UNDOFORMAT("allocateTSlotPtr_ %lu < recycleTSlotPtr_ %lu."), allocateTSlotPtr_, recycleTSlotPtr_)));
        }
        return ((allocateTSlotPtr_ - recycleTSlotPtr_) / BLCKSZ);
    }
    bool CheckNeedSwitch(UndoRecordSize size);
    UndoRecordState CheckUndoRecordValid(UndoLogOffset offset, bool checkForceRecycle, TransactionId *lastXid);
    bool CheckRecycle(UndoRecPtr starturp, UndoRecPtr endurp, bool isexrto = false);

    UndoRecPtr AllocateSpace(uint64 size);
    void ReleaseSpace(UndoRecPtr starturp, UndoRecPtr endurp, int *forceRecycleSize);
    UndoRecPtr AllocateSlotSpace(void);
    void ReleaseSlotSpace(UndoRecPtr starturp, UndoRecPtr endurp, int *forceRecycleSize);
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
    UndoRecordState check_record_valid_exrto(UndoLogOffset offset, bool check_force_recycle,
        TransactionId *last_xid) const;
    uint64 release_residual_record_space();
    uint64 release_residual_slot_space();

private:
    static const uint32 UNDO_ZONE_ATTACHED = 1;
    static const uint32 UNDO_ZONE_DETACHED = 0;
    pg_atomic_uint32 attached_;
    UndoSlotBuffer buf_;
    UndoSlotOffset allocateTSlotPtr_;
    UndoSlotOffset recycleTSlotPtr_;
    UndoSlotPtr frozenSlotPtr_;
    UndoLogOffset insertURecPtr_;
    UndoLogOffset discardURecPtr_;
    UndoLogOffset forceDiscardURecPtr_;
    UndoPersistence pLevel_;
    TransactionId recycleXid_;
    TransactionId frozenXid_;
    ThreadId attachPid_;

    /* for extreme RTO read. */
    UndoSlotOffset recycle_tslot_ptr_exrto;
    UndoLogOffset discard_urec_ptr_exrto;
    UndoLogOffset force_discard_urec_ptr_exrto;
    TransactionId recycle_xid_exrto;

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
    static void ReleaseZone(int zid, UndoPersistence upersistence);
    static UndoZone *SwitchZone(int zid, UndoPersistence upersistence);
    static UndoZone *GetUndoZone(int zid, bool isNeedInitZone = true);
    static void InitUndoCxtUzones();
    static bool UndoZoneInUse(int zid, UndoPersistence upersistence);
}; // class UndoZoneGroup

void AllocateZonesBeforXid();
void InitZone(UndoZone *uzone, const int zoneId, UndoPersistence upersistence);
void InitUndoSpace(UndoZone *uzone, UndoSpaceType type);
bool VerifyUndoZone(UndoZone *uzone);
void exrto_recycle_residual_undo_file(char *FuncName);

} // namespace undo
#endif // __KNL_UUNDOZONE_H__
