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
 * knl_uundotxn.cpp
 * c++ code
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/undo/knl_uundotxn.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/multi_redo_api.h"
#include "access/ustore/undo/knl_uundotxn.h"
#include "access/ustore/knl_whitebox_test.h"
#include "access/transam.h"
#include "knl/knl_thread.h"
#include "miscadmin.h"
#include "storage/barrier.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/lock/lwlock.h"
#include "storage/standby.h"
#include "threadpool/threadpool.h"
#include "utils/atomic.h"
#include "pgstat.h"
#include "postgres_ext.h"
#include "utils/dynahash.h"

namespace undo {

void TransactionSlot::Init(TransactionId xid, Oid dbId)
{
    xactId_ = xid;
    startUndoPtr_ = INVALID_UNDO_REC_PTR;
    endUndoPtr_ = INVALID_UNDO_REC_PTR;
    info_ |= TRANSLOT_ROLLBACK;
    dbId_ = dbId;
}

#ifdef ENABLE_MULTIPLE_NODES
const static bool SUPPORT_HOT_STANDBY = false; /* don't support consistency view */
#else
const static bool SUPPORT_HOT_STANDBY = true;
#endif

void TransactionSlot::Update(UndoRecPtr start, UndoRecPtr end)
{
    WHITEBOX_TEST_STUB(UNDO_UPDATE_SLOT_FAILED, WhiteboxDefaultErrorEmit);

    endUndoPtr_ = end;
    if (end < start) {
        ereport(PANIC, (errmsg(
            UNDOFORMAT("update slot error: from start %lu to end %lu."), start, end)));
    }
    if (startUndoPtr_ == INVALID_UNDO_REC_PTR) {
        pg_write_barrier();

    WHITEBOX_TEST_STUB(UNDO_UPDATE_BEFORE_UPDATE_FAILED, WhiteboxDefaultErrorEmit);

        startUndoPtr_ = start;

    WHITEBOX_TEST_STUB(UNDO_UPDATE_AFTER_UPDATE_FAILED, WhiteboxDefaultErrorEmit);

    }
    if (UNDO_PTR_GET_ZONE_ID(start) != UNDO_PTR_GET_ZONE_ID(end)) {
        ereport(PANIC, (errmsg(UNDOFORMAT("update slot error: start zid %d != end zid %d."),
            (int)UNDO_PTR_GET_ZONE_ID(start), (int)UNDO_PTR_GET_ZONE_ID(end))));
    }
    return;
}

void UndoSlotBuffer::PrepareTransactionSlot(UndoSlotPtr slotPtr, uint8 info)
{
    WHITEBOX_TEST_STUB(UNDO_PREPARE_TRANSACTION_SLOT_FAILED, WhiteboxDefaultErrorEmit);

    if (!BufferIsInvalid(buffer_) && UNDO_PTR_GET_BLOCK_NUM(slotPtr) == blk_) {
        if (IsKeepBuffer()) {
            ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
            BufferDesc *buf = GetBufferDescriptor(buffer_ - 1);
            if (!IsSlotBufferValid(buf, UNDO_PTR_GET_ZONE_ID(slotPtr), slotPtr)) {
                ereport(PANIC, (errmsg(UNDOFORMAT("invalid cached slot buffer %d slot ptr %lu."), buffer_, slotPtr)));
            }
            bool result = PinBuffer(buf, NULL);
            ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.TopTransactionResourceOwner);
            ResourceOwnerRememberBuffer(t_thrd.utils_cxt.TopTransactionResourceOwner, buffer_);
            ResourceOwnerForgetBuffer(t_thrd.utils_cxt.CurrentResourceOwner, buffer_);
            SetInfo(UNDOSLOTBUFFER_PREPARED);
            if (!result) {
                ereport(PANIC, (errmsg(UNDOFORMAT("prepare transaction slot: %lu info: %u."), slotPtr, info)));
            }
        }
    } else {
        if (!BufferIsInvalid(buffer_)) {
            if (IsKeepBuffer()) {
                MarkBufferMetaFlag(buffer_, false);
                ereport(DEBUG1, (errmsg(UNDOFORMAT("zone %d mark buffer %d unset meta flag by blk %u info %u."), 
                    (int)(UNDO_PTR_GET_ZONE_ID(slotPtr)), buffer_, blk_, info_)));
                UnSetInfo(UNDOSLOTBUFFER_KEEP);
            }
            if (IsReviseRBM()) {
                UnSetInfo(UNDOSLOTBUFFER_RBM);
            }
        }
        blk_ = UNDO_PTR_GET_BLOCK_NUM(slotPtr);
        RelFileNode rnode;
        UNDO_PTR_ASSIGN_REL_FILE_NODE(rnode, slotPtr, UNDO_SLOT_DB_OID);
        DECLARE_NODE_COUNT();
        GET_UPERSISTENCE_BY_ZONEID((int)UNDO_PTR_GET_ZONE_ID(slotPtr), nodeCount);
        ReadBufferMode rbm = RBM_NORMAL;
        int startingByte = UNDO_PTR_GET_PAGE_OFFSET(slotPtr);
        if (info & UNDOSLOTBUFFER_RBM) {
            if (startingByte == UNDO_LOG_BLOCK_HEADER_SIZE) {
                rbm = RBM_ZERO;
            }
            SetInfo(UNDOSLOTBUFFER_RBM);
        }
        buffer_ = ReadUndoBufferWithoutRelcache(
            rnode, UNDO_FORKNUM, blk_, rbm, NULL, REL_PERSISTENCE(upersistence));
        if (BufferIsValid(buffer_)) {
            BufferDesc *buf = GetBufferDescriptor(buffer_ - 1);
            if (!IsSlotBufferValid(buf, UNDO_PTR_GET_ZONE_ID(slotPtr), slotPtr)) {
                ereport(PANIC, (errmsg(UNDOFORMAT("invalid cached slot buffer %d slot ptr %lu."), buffer_, slotPtr)));
            }
            if (info & UNDOSLOTBUFFER_KEEP) {
                MarkBufferMetaFlag(buffer_, true);
                ereport(DEBUG1, (errmsg(UNDOFORMAT("zone %d mark buffer %d set meta flag by blk %u info %u."), 
                    (int)(UNDO_PTR_GET_ZONE_ID(slotPtr)), buffer_, blk_, info_)));
                SetInfo(UNDOSLOTBUFFER_KEEP);
                ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.TopTransactionResourceOwner);
                ResourceOwnerRememberBuffer(t_thrd.utils_cxt.TopTransactionResourceOwner, buffer_);
                ResourceOwnerForgetBuffer(t_thrd.utils_cxt.CurrentResourceOwner, buffer_);
            }
        } else {
            ereport(PANIC, (errmsg(UNDOFORMAT("unknow buffer: %d"), buffer_)));
        }
        ereport(DEBUG1, (errmsg(UNDOFORMAT("zone %d prepare buffer %d by slot ptr %lu blk %u info %u."), 
            (int)(UNDO_PTR_GET_ZONE_ID(slotPtr)), buffer_, slotPtr, blk_, info_)));
        SetInfo(UNDOSLOTBUFFER_PREPARED);
    }
}

TransactionSlot *UndoSlotBuffer::FetchTransactionSlot(UndoSlotPtr slotPtr)
{
    WHITEBOX_TEST_STUB(UNDO_FETCH_TRANSACTION_SLOT_FAILED, WhiteboxDefaultErrorEmit);

    UndoSlotOffset slotOffset = UNDO_PTR_GET_OFFSET(slotPtr);
    Page page = BufferGetPage(buffer_);
    if (((PageHeader)page)->pd_upper == 0) {
        ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("INIT UNDO PAGE: urp=%lu, blockno=%u"),
            slotPtr, GetBufferDescriptor(buffer_ - 1)->tag.blockNum)));
        PageInit(page, BLCKSZ, 0);
    }
    TransactionSlot *slot = (TransactionSlot *)((char *)page + slotOffset % BLCKSZ);
    return slot;
}

void UndoSlotBuffer::Release()
{
    if (!BufferIsInvalid(buffer_)) {
        if (IsPrepared()) {
            ereport(DEBUG1, (errmsg(UNDOFORMAT("release buffer %d by blk %u info %u."), 
                buffer_, blk_, info_)));
            ReleaseBuffer(buffer_);
            UnSetInfo(UNDOSLOTBUFFER_PREPARED);
        }
    }
}

void UndoSlotBuffer::NotKeepBuffer()
{
    if (!BufferIsInvalid(buffer_)) {
        Assert(IsKeepBuffer());
        MarkBufferMetaFlag(buffer_, false);
        UnSetInfo(UNDOSLOTBUFFER_KEEP);
    }
}

void UndoSlotBuffer::SetLSN(XLogRecPtr lsn)
{
    PageSetLSN(BufferGetPage(buffer_), lsn);
}

bool UndoSlotBuffer::IsSlotBufferValid(BufferDesc *buf, int zoneId, UndoSlotPtr slotPtr)
{
    if (buf->tag.rnode.dbNode == UNDO_SLOT_DB_OID && 
        buf->tag.rnode.spcNode == DEFAULTTABLESPACE_OID && 
        (int)(buf->tag.rnode.relNode) == zoneId && 
        buf->tag.rnode.bucketNode == InvalidBktId &&
        buf->tag.forkNum == UNDO_FORKNUM &&
        buf->tag.blockNum == UNDO_PTR_GET_BLOCK_NUM(slotPtr)) {
        return true;
    } else {
        return false;
    }
}

UndoSlotBufferCache::UndoSlotBufferCache(MemoryContext context, long capacity)
{
    HASHCTL ctl = {0};
    ctl.keysize = sizeof(UndoSlotPtr);
    ctl.entrysize = sizeof(SlotBufferCacheEntry);
    ctl.hash = tag_hash;
    ctl.hcxt = context;
    hashTable_ = 
        hash_create("Slot buffer cache", capacity, &ctl, HASH_ELEM | HASH_FUNCTION);
    capacity_ = capacity;
    size_ = 0;
    head_ = NULL;
    tail_ = NULL;
    victim_ = NULL;
}

void UndoSlotBufferCache::VictimSlotBuffer()
{
    Assert(victim_ != NULL);
    SlotBufferCacheEntry *entry = victim_;
    do {
        if (entry->hit_) {
            entry->hit_ = false;
        } else {
            ereport(DEBUG1, (errmsg(UNDOFORMAT("victim entry %lu head_ %lu tail_ %lu."), 
                entry->tag_, head_->tag_, tail_->tag_)));
            RemoveSlotBuffer(entry->tag_);
            return;
        }
        entry = entry->next_;
    } while (entry != victim_);
    Assert(size_ > 0);
    Assert(tail_ != NULL);
    ereport(DEBUG1, (errmsg(UNDOFORMAT("victim entry %lu head_ %lu tail_ %lu."), 
        tail_->tag_, head_->tag_, tail_->tag_)));
    RemoveSlotBuffer(tail_->tag_);
}

void *UndoSlotBufferCache::InsertSlotBuffer(UndoSlotPtr ptr, uint32 hashValue)
{
    bool found = false;
    SlotBufferCacheEntry *entry = (SlotBufferCacheEntry *)hash_search_with_hash_value(hashTable_, &ptr, 
        hashValue, HASH_ENTER, &found);
    Assert(!found);
    if (entry == NULL) {
        ereport(ERROR, (errmodule(MOD_UNDO), errmsg("InsertSlotBuffer: Can not find the free slot")));
    }
    entry->next_ = head_;
    entry->prev_ = tail_;
    entry->tag_ = ptr;
    entry->hit_ = false;
    entry->buf_.Init();

    if ((head_ == NULL && tail_ != NULL) || (head_ != NULL && tail_ == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
            errmsg("head_ and tail_ must be both empty or both not empty")));
    }
    if (head_ == NULL && tail_ == NULL) {
        head_ = entry;
        tail_ = entry;
        victim_ = entry;
    } else {
        head_->prev_ = entry;
        tail_->next_ = entry;
        head_ = entry;
    }
    ereport(DEBUG1, (errmsg(UNDOFORMAT("insert entry %lu head_ %lu tail_ %lu."), 
        entry->tag_, head_->tag_, tail_->tag_)));
    size_++;
    return entry;
}

void UndoSlotBufferCache::RemoveSlotBuffer(UndoSlotPtr slotPtr)
{
    bool found = false;
    UndoSlotPtr ptr = MAKE_UNDO_PTR_ALIGN(slotPtr);
    SlotBufferCacheEntry *entry = (SlotBufferCacheEntry *)hash_search(hashTable_, &ptr, HASH_REMOVE, &found);
    Assert(found);
    SlotBufferCacheEntry *prev = entry->prev_;
    SlotBufferCacheEntry *next = entry->next_;
    if (entry == head_) {
        head_ = entry->next_;
    }
    if (entry == tail_) {
        tail_ = entry->prev_;
    }
    if (prev != NULL) {
        prev->next_ = entry->next_;
    }
    if (next != NULL) {
        next->prev_ = entry->prev_;
    }
    size_--;
    if (size_ != 0) {
        ereport(DEBUG1, (errmsg(UNDOFORMAT("release entry %lu head_ %lu tail_ %lu."),
            entry->tag_, head_->tag_, tail_->tag_)));
    } else {
        ereport(DEBUG1, (errmsg(UNDOFORMAT("release entry %lu.SlotBuffer is empty"),
            entry->tag_)));
    }
    return;
}

UndoSlotBuffer& UndoSlotBufferCache::FetchTransactionBuffer(UndoSlotPtr slotPtr)
{
    bool found = false;
    UndoSlotPtr ptr = MAKE_UNDO_PTR_ALIGN(slotPtr);
    uint32 hashValue = hashTable_->hash(&ptr, hashTable_->keysize);
    SlotBufferCacheEntry *entry = (SlotBufferCacheEntry *)hash_search_with_hash_value(hashTable_, &ptr, 
        hashValue, HASH_FIND, &found);
    if (found) {
        Assert(entry->tag_ == ptr);
        entry->hit_ = true;
        return entry->buf_;
    } else {
        if (size_ >= capacity_) {
            VictimSlotBuffer();
        }
        entry = (SlotBufferCacheEntry *)InsertSlotBuffer(ptr, hashValue);
        return entry->buf_;
    }
}

void UndoSlotBufferCache::Destory()
{
    hash_destroy(hashTable_);
}

UndoSlotPtr GetNextSlotPtr(UndoSlotPtr slotPtr)
{
    UndoSlotOffset slotOffset = UNDO_PTR_GET_OFFSET(slotPtr);
    BlockNumber block = slotOffset / BLCKSZ;
    UndoSlotOffset blkOffset = slotOffset % BLCKSZ;
    UndoSlotOffset offset = blkOffset + MAXALIGN(sizeof(undo::TransactionSlot));
    if (BLCKSZ - offset < MAXALIGN(sizeof(undo::TransactionSlot))) {
        offset = (block + 1) * BLCKSZ + UNDO_LOG_BLOCK_HEADER_SIZE;
    } else {
        offset += block * BLCKSZ;
    }
    Assert (offset <= UNDO_LOG_MAX_SIZE);
    return MAKE_UNDO_PTR(UNDO_PTR_GET_ZONE_ID(slotPtr), offset);
}
} // namespace undo
