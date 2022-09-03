/* -------------------------------------------------------------------------
 *
 * knl_uundotxn.h
 * c++ code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/undo/knl_uundotxn.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __KNL_UUNDOTXN_H__
#define __KNL_UUNDOTXN_H__

#include "access/ustore/undo/knl_uundotype.h"
#include "storage/buf/bufmgr.h"
#include "storage/lock/lwlock.h"

namespace undo {
#define TRANSLOT_ROLLBACK 0x00
#define TRANSLOT_ROLLBACK_FINISH 0x01

#define UNDOSLOTBUFFER_INIT 0x00
#define UNDOSLOTBUFFER_KEEP 0x01
#define UNDOSLOTBUFFER_RBM 0x02
#define UNDOSLOTBUFFER_PREPARED 0x04

class TransactionSlot {
public:
    TransactionSlot();
    void Init(TransactionId xid, Oid dbId);
    TransactionId XactId()
    {
        return xactId_;
    }
    void SetXactId(TransactionId xid)
    {
        xactId_ = xid;
    }
    volatile UndoRecPtr StartUndoPtr()
    {
        return startUndoPtr_;
    }
    void SetStartUndoPtr(UndoRecPtr startUndoPtr)
    {
        startUndoPtr_ = startUndoPtr;
    }
    UndoRecPtr EndUndoPtr()
    {
        return endUndoPtr_;
    }
    void SetEndUndoPtr(UndoRecPtr endUndoPtr)
    {
        endUndoPtr_ = endUndoPtr;
    }
    Oid DbId()
    {
        return dbId_;
    }
    void SetDbId(Oid dbId)
    {
        dbId_ = dbId;
    }
    void UpdateRollbackProgress()
    {
        info_ |= TRANSLOT_ROLLBACK_FINISH;
    }
    bool NeedRollback()
    {
        return !(info_ & TRANSLOT_ROLLBACK_FINISH);
    }
    void Update(UndoRecPtr start, UndoRecPtr end);
    TransactionId xactId_;
    volatile UndoRecPtr startUndoPtr_;
    UndoRecPtr endUndoPtr_;
    uint32 info_ : 8;
    uint32 pad : 24;
    Oid dbId_;
};

class UndoSlotBuffer {
public:
    UndoSlotBuffer():blk_(InvalidBlockNumber), buffer_(InvalidBuffer), info_(UNDOSLOTBUFFER_INIT) {}
    void Init()
    {
        blk_ = InvalidBlockNumber;
        buffer_ = InvalidBuffer;
        info_ = UNDOSLOTBUFFER_INIT;
    }
    Buffer Buf() 
    {
        return buffer_;
    }
    BlockNumber BufBlock() 
    {
        return blk_;
    }

    uint8 Info()
    {
        return info_;
    }
    void Lock()
    {
        LockBuffer(buffer_, BUFFER_LOCK_EXCLUSIVE);
    }
    void UnLock()
    {
        LockBuffer(buffer_, BUFFER_LOCK_UNLOCK);
    }
    void MarkDirty()
    {
        MarkBufferDirty(buffer_);
    }
    void SetInfo(uint8 info)
    {
        info_ |= info;
    }
    bool IsKeepBuffer() const
    {
        return info_ & UNDOSLOTBUFFER_KEEP;
    }
    bool IsPrepared() const
    {
        return info_ & UNDOSLOTBUFFER_PREPARED;
    }
    void UnSetInfo(uint8 info)
    {
        info_ &= ~info;
    }
    void SetLSN(XLogRecPtr lsn);
    void PrepareTransactionSlot(UndoSlotPtr slotPtr, uint8 info=UNDOSLOTBUFFER_INIT);
    TransactionSlot *FetchTransactionSlot(UndoSlotPtr slotPtr);
    void NotKeepBuffer();
    void Release();
    static bool IsSlotBufferValid(BufferDesc *buf, int zoneId, UndoSlotPtr slotPtr);
private:
    BlockNumber blk_;
    Buffer buffer_;
    uint8 info_;
};

class UndoSlotBufferCache : public BaseObject {
public:
    UndoSlotBufferCache(MemoryContext context, long size);
    UndoSlotBuffer& FetchTransactionBuffer(UndoSlotPtr slotPtr);
    void RemoveSlotBuffer(UndoSlotPtr slotPtr);
    void Destory();
private:
    void *InsertSlotBuffer(UndoSlotPtr ptr, uint32 hashValue);
    void VictimSlotBuffer();
    typedef struct SlotBufferCacheEntry {
        SlotBufferCacheEntry():tag_(INVALID_UNDO_SLOT_PTR), hit_(false), prev_(NULL), next_(NULL){}
        UndoSlotPtr tag_;
        UndoSlotBuffer buf_;
        bool hit_;
        SlotBufferCacheEntry *prev_;
        SlotBufferCacheEntry *next_;
    } SlotBufferCacheEntry;
    SlotBufferCacheEntry *head_;
    SlotBufferCacheEntry *tail_;
    SlotBufferCacheEntry *victim_;
    HTAB *hashTable_;
    int size_;
    int capacity_;
};

UndoSlotPtr GetNextSlotPtr(UndoSlotPtr slotPtr);
bool VerifyTransactionSlotValid(TransactionSlot *slot);
bool VerifyTransactionSlotBuffer(Page page);

} // namespace undo
#endif // __KNL_UUNDOTXN_H__
