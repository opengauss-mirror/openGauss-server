/* -------------------------------------------------------------------------
 *
 * knl_uundovec.cpp
 *    c++ code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/ustore/knl_uundovec.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/ustore/knl_uundovec.h"

#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/knl_whitebox_test.h"
#include "pgstat.h"
#include "storage/buf/bufmgr.h"
#include "utils/palloc.h"
#include "utils/resowner.h"
#include "utils/elog.h"

namespace {
void PrefetchUndoPages(UndoRecPtr urp, int prefetchTarget, int *prefetchPages, 
    BlockNumber startBlk, BlockNumber endBlk, UndoPersistence upersistence)
{
}

/*
 * undo_record_comparator
 *
 * qsort comparator to handle undo record for applying undo actions of the
 * transaction.
 */
int UndoRecordComparator(const void *left, const void *right)
{
    UndoRecord *luur = *((UndoRecord **) left);
    UndoRecord *ruur = *((UndoRecord **) right);

    if (luur->Tablespace() < ruur->Tablespace()) {
        return -1;
    } else if (luur->Tablespace() > ruur->Tablespace()) {
        return 1;
    } else if (luur->Relfilenode() < ruur->Relfilenode()) {
        return -1;
    } else if (luur->Relfilenode() > ruur->Relfilenode()) {
        return 1;
    } else if (luur->Blkno() == ruur->Blkno()) {
        if (luur->Index() < ruur->Index()) {
            return -1;
        } else {
            return 1;
        }
    } else if (luur->Blkno() < ruur->Blkno()) {
        return -1;
    } else {
        return 1;
    }
}

void UndoRecordSetUInfo(URecVector *uvec)
{
    auto size = uvec->Size();
    for (auto i = 0; i < size; i++) {
        UndoRecord *urec = (*uvec)[i];
        if (!urec->NeedInsert()) {
            continue;
        }
        if (urec->OldXactId() != FrozenTransactionId) {
            urec->SetUinfo(UNDO_UREC_INFO_OLDTD);
        }
        if (urec->Partitionoid() != InvalidOid) {
            urec->SetUinfo(UNDO_UREC_INFO_HAS_PARTOID);
        }
        if (urec->Tablespace() != InvalidOid) {
            urec->SetUinfo(UNDO_UREC_INFO_HAS_TABLESPACEOID);
        }
    }
}
} // namespace

URecVector::URecVector() :     ubuffers_(NULL), urecs_(NULL), size_(0),
                               capacity_(0), ubuffersIdx_(0),
                               isPrepared_(false), isInited_(false)
{
    // Intended left blank.
}

void URecVector::Initialize(int capacity, bool isPrepared)
{
    Assert(!isInited_);

    isPrepared_ = isPrepared;
    isInited_ = true;
    NewCapacity(capacity);
    if (isPrepared) {
        ubuffers_ = (UndoBuffer*)palloc0(
            capacity * MAX_BUFFER_PER_UNDO * sizeof(UndoBuffer));
    }
}

void URecVector::Destroy()
{
    Assert(isInited_);

    for (auto i = 0; i < size_; i++) {
        UndoRecord *urec = urecs_[i];
        DELETE_EX(urec);
    }
    if (urecs_ != NULL) {
        pfree(urecs_);
        urecs_ = NULL;
    }
    for (auto i = 0; i < ubuffersIdx_; i++) {
        if (BufferIsValid(ubuffers_[i].buf)) {
            UnlockReleaseBuffer(ubuffers_[i].buf);
        }
    }
    ubuffersIdx_ = 0;
    if (ubuffers_ != NULL) {
        pfree(ubuffers_);
        ubuffers_ = NULL;
    }
}

void URecVector::Reset(bool needUnlockBuffer)
{
    Assert(isInited_);

    for (auto i = 0; i < size_; i++) {
        UndoRecord *urec = urecs_[i];
        urec->Reset(INVALID_UNDO_REC_PTR);
        urec->SetNeedInsert(false);
    }

    if (needUnlockBuffer) {
        for (auto i = 0; i < ubuffersIdx_; i++) {
            if (BufferIsValid(ubuffers_[i].buf)) {
                if (t_thrd.xlog_cxt.InRecovery) {
                    UnlockReleaseBuffer(ubuffers_[i].buf);
                } else {
                    LockBuffer(ubuffers_[i].buf, BUFFER_LOCK_UNLOCK);
                }
            }
        }
    }
    ubuffersIdx_ = 0;
}

void ReleaseUndoBuffers()
{
    for (int i = 0; i < u_sess->ustore_cxt.undo_buffer_idx; i++) {
        if (BufferIsValid(u_sess->ustore_cxt.undo_buffers[i].buf)) {
            ReleaseBuffer(u_sess->ustore_cxt.undo_buffers[i].buf);
        }
    }

    u_sess->ustore_cxt.undo_buffer_idx = 0;
}

static void CacheUndoBuffer(UndoBuffer* buffer)
{
    int bufidx = u_sess->ustore_cxt.undo_buffer_idx;
    UndoBuffer* undobuf = &u_sess->ustore_cxt.undo_buffers[bufidx];

    undobuf->buf = buffer->buf;
    undobuf->blk = buffer->blk;
    undobuf->zoneId = buffer->zoneId;
    undobuf->zero = buffer->zero;
    undobuf->inUse = buffer->inUse;

    u_sess->ustore_cxt.undo_buffer_idx++;

    ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.TopTransactionResourceOwner);
    ResourceOwnerRememberBuffer(t_thrd.utils_cxt.TopTransactionResourceOwner, undobuf->buf);
    ResourceOwnerForgetBuffer(t_thrd.utils_cxt.CurrentResourceOwner, undobuf->buf);
}

// zf: UndoGetBufferSlot
int URecVector::GetUndoBufidx(RelFileNode rnode, BlockNumber blk, 
    ReadBufferMode rbm, UndoPersistence upersistence)
{
    Assert(isInited_);

    Buffer buffer = InvalidBuffer;
    auto i = 0;

    if (!t_thrd.xlog_cxt.InRecovery) {
        /* Don't do anything, if we already have a buffer pinned for the block */
        for (i = 0; i < u_sess->ustore_cxt.undo_buffer_idx; i++) {
            /* We can only allocate one undo log, so it's enough to only check block number */
            if ((blk == u_sess->ustore_cxt.undo_buffers[i].blk) &&
                (rnode.relNode == (unsigned int)u_sess->ustore_cxt.undo_buffers[i].zoneId)) {
                buffer = u_sess->ustore_cxt.undo_buffers[i].buf;

                if (!u_sess->ustore_cxt.undo_buffers[i].inUse) {
                    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
                    u_sess->ustore_cxt.undo_buffers[i].inUse = true;
                }
                Assert(BufferIsValid(buffer));
                break;
            }
        }

        bool newBuffer = i == u_sess->ustore_cxt.undo_buffer_idx;

        for (i = 0; i < ubuffersIdx_; i++) {
            if (blk == ubuffers_[i].blk) {
                if (!ubuffers_[i].inUse) {
                    ubuffers_[i].inUse = true;
                }
                break;
            }
        }

        bool addBuffer = i == ubuffersIdx_;

        if (newBuffer) {
            buffer = ReadUndoBufferWithoutRelcache(rnode, UNDO_FORKNUM, blk, rbm, NULL,
                REL_PERSISTENCE(upersistence));
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        }

        if (addBuffer) {
            ubuffers_[ubuffersIdx_].buf = buffer;
            ubuffers_[ubuffersIdx_].blk = blk;
            ubuffers_[ubuffersIdx_].zoneId = rnode.relNode;
            ubuffers_[ubuffersIdx_].zero = rbm == RBM_ZERO;
            ubuffers_[ubuffersIdx_].inUse = true;
        }

        if (newBuffer) {
            Assert(addBuffer);
            /* cache buffer in global undo_buffers array */
            CacheUndoBuffer(&ubuffers_[ubuffersIdx_]);
        }

        if (addBuffer) {
            ubuffersIdx_++;
        }
    } else {
        for (i = 0; i < ubuffersIdx_; i++) {
            if (blk == ubuffers_[i].blk) {
                if (!ubuffers_[i].inUse) {
                    LockBuffer(ubuffers_[i].buf, BUFFER_LOCK_EXCLUSIVE);
                    ubuffers_[i].inUse = true;
                }
                break;
            }
        }
    
        Buffer buffer = InvalidBuffer;
        if (i == ubuffersIdx_) {
            buffer = ReadUndoBufferWithoutRelcache(rnode, UNDO_FORKNUM, blk, rbm, NULL,
                REL_PERSISTENCE(upersistence));
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
            ubuffers_[ubuffersIdx_].buf = buffer;
            ubuffers_[ubuffersIdx_].blk = blk;
            ubuffers_[ubuffersIdx_].zoneId = rnode.relNode;
            ubuffers_[ubuffersIdx_].zero = rbm == RBM_ZERO;
            ubuffers_[ubuffersIdx_].inUse = true;
            ubuffersIdx_++;
        }
    }
    return i;
}

bool URecVector::PushBack(UndoRecord *urec)
{
    Assert(isInited_);
    Assert(urec);

    if (size_ == capacity_) {
        if (isPrepared_) {
            return false;
        }
        NewCapacity(capacity_ * UNDO_VECTOR_EXPANSION_COEFFICIENT);
    }
    urec->SetIndex(size_);
    urecs_[size_++] = urec;
    return true;
}

void URecVector::SortByBlkNo()
{
    Assert(isInited_);

    qsort((void*)urecs_, size_, sizeof(UndoRecord*), UndoRecordComparator);
}

void URecVector::NewCapacity(int newCapacity)
{
    Assert(isInited_);

    int newCapacityBytes = sizeof(UndoRecord*) * newCapacity;
    if (urecs_ == NULL) {
        urecs_ = (UndoRecord**)palloc(newCapacityBytes);
    } else {
        urecs_ = (UndoRecord**)repalloc(urecs_, newCapacityBytes);
    }
    SetUrecsZero(size_, newCapacity);
    capacity_ = newCapacity;
}

void URecVector::SetUrecsZero(int start, int end)
{
    Assert(isInited_);

    for (auto i = start; i < end; i++) {
        *(urecs_ + i) = NULL;
    }
}

uint64 URecVector::TotalSize()
{
    uint64 total = 0;
    for (auto i = 0; i < size_; i++) {
        UndoRecord *urec = urecs_[i];
        if (!urec->NeedInsert()) {
            continue;
        }
        total += urec->RecordSize();
    }
    return total;
}

UndoRecPtr URecVector::LastRecord()
{
    UndoRecPtr lastUrp = INVALID_UNDO_REC_PTR;
    for (auto i = 0; i < size_; i++) {
        UndoRecord *urec = urecs_[i];
        if (!urec->NeedInsert()) {
            continue;
        }
        lastUrp = urec->Urp();
    }
    return lastUrp;
}

UndoRecordSize URecVector::LastRecordSize()
{
    UndoRecPtr lastRecordSize = 0;
    for (auto i = 0; i < size_; i++) {
        UndoRecord *urec = urecs_[i];
        if (!urec->NeedInsert()) {
            continue;
        }
        lastRecordSize = urec->RecordSize();
    }
    return lastRecordSize;
}

static bool LoadUndoRecordRange(UndoRecord *urec, Buffer *buffer)
{
    /*
     * In one_page mode it's possible that the undo of the transaction
     * might have been applied by worker and undo got discarded. Prevent
     * discard worker from discarding undo data while we are reading it.
     * On the other word, we need copy data to avoid discarded.
     */
    UndoRecordState state = undo::CheckUndoRecordValid(urec->Urp(), false);
    if (state != UNDO_RECORD_NORMAL) {
        return false;
    }

    PG_TRY();
    {
        t_thrd.undo_cxt.fetchRecord = true;
        urec->Load(true);
    }
    PG_CATCH();
    {
        if (BufferIsValid(urec->Buff())) {
            if (urec->Buff() == *buffer) {
                *buffer = InvalidBuffer;
            }
            ReleaseBuffer(urec->Buff());
            urec->SetBuff(InvalidBuffer);
        }
        state = undo::CheckUndoRecordValid(urec->Urp(), false);
        if ((!t_thrd.undo_cxt.fetchRecord) && state == UNDO_RECORD_DISCARD) {
            return false;
        } else {
            PG_RE_THROW();
        }
    }
    PG_END_TRY();
    t_thrd.undo_cxt.fetchRecord = false;
    *buffer = urec->Buff();
    urec->SetBuff(InvalidBuffer);
    return true;
}

/*
 * FetchUndoRecordRange
 *     onePage:used for rollback in Bakend thread
 *    !onePage:used for rollback in UndoWorker thread
 */
URecVector* FetchUndoRecordRange(    __inout UndoRecPtr *startUrp,
    _in_ UndoRecPtr endUrp, _in_ int maxUndoApplySize, _in_ bool onePage)
{
    BlockNumber startBlk = InvalidBlockNumber;
    BlockNumber endBlk = InvalidBlockNumber;
    static const int urecSize = 1024;
    int totalSize = sizeof(URecVector);
    TransactionId xid = InvalidTransactionId;
    int prefetchTarget = onePage ? 0 : u_sess->storage_cxt.target_prefetch_pages;
    int prefetchPages = 0;
    Buffer buffer = InvalidBuffer;
    UndoRecPtr currUrp = *startUrp;
    UndoRecPtr prevUrp = INVALID_UNDO_REC_PTR;
    *startUrp = INVALID_UNDO_REC_PTR;

    URecVector *urecvec = New(CurrentMemoryContext) URecVector();
    urecvec->Initialize(urecSize, false);

    do {
        startBlk = UNDO_PTR_GET_BLOCK_NUM(currUrp);
        endBlk = UNDO_PTR_GET_BLOCK_NUM(endUrp);

        UndoRecord *urec = New(CurrentMemoryContext) UndoRecord();
        urec->SetUrp(currUrp);

        /* Get Undo Persistence. Stored in the variable upersistence */
        int zoneId = UNDO_PTR_GET_ZONE_ID(currUrp);
        DECLARE_NODE_COUNT();
        GET_UPERSISTENCE_BY_ZONEID(zoneId, nodeCount);

        // If next undo record pointer to be fetched is not on the same block
        // then release the old buffer.
        if (!IS_VALID_UNDO_REC_PTR(prevUrp) || UNDO_PTR_GET_ZONE_ID(prevUrp) != UNDO_PTR_GET_ZONE_ID(currUrp) ||
            UNDO_PTR_GET_BLOCK_NUM(prevUrp) != UNDO_PTR_GET_BLOCK_NUM(currUrp)) {
            if (BufferIsValid(buffer)) {
                ReleaseBuffer(buffer);
                buffer = InvalidBuffer;
            }
        } else {
            urec->SetBuff(buffer);
        }

        if (prefetchPages < prefetchTarget / 2) {
            PrefetchUndoPages(currUrp, prefetchTarget, &prefetchPages, 
                startBlk, endBlk, upersistence);
        }

        if (!LoadUndoRecordRange(urec, &buffer)) {
            break;
        }

        if (onePage) {
            if (!TransactionIdIsValid(xid)) {
                xid = urec->Xid();
            } else if (xid != urec->Xid()) {
                break;
            }
        }

        prevUrp = currUrp;
        if (onePage) {
            currUrp = urec->Blkprev();
            ereport(DEBUG5, (errmsg(UNDOFORMAT("cur urp %lu blk no %u blk prev %lu."), 
                urec->Urp(), urec->Blkno(), currUrp)));
        } else if (prevUrp == endUrp) {
            currUrp = INVALID_UNDO_REC_PTR;
            ereport(DEBUG5, (errmsg(UNDOFORMAT("prevUrp == endUrp."))));
        } else {
            currUrp = urec->Prevurp(currUrp, &buffer);
            ereport(DEBUG5, (errmsg(UNDOFORMAT("cur urp %lu blk no %u prev urp %lu."), 
                urec->Urp(), urec->Blkno(), currUrp)));
        }

        urecvec->PushBack(urec);

        if (!IS_VALID_UNDO_REC_PTR(currUrp) || prevUrp == endUrp) {
            break;
        }

        totalSize += urec->MemoryRecordSize();
        if (totalSize >= maxUndoApplySize) {
            *startUrp = currUrp;
            break;
        }

    } while (true);

    if (BufferIsValid(buffer)) {
        ReleaseBuffer(buffer);
    }

    return urecvec;
}

static bool CheckLastRecordSize(UndoRecordSize lastRecordSize, undo::XlogUndoMeta* const xlundometa)
{
    WHITEBOX_TEST_STUB(UNDO_CHECK_LAST_RECORD_SIZE_FAILED, WhiteboxDefaultErrorEmit);

    if (t_thrd.xlog_cxt.InRecovery && (lastRecordSize != xlundometa->lastRecordSize)) {
        ereport(WARNING, (errmsg(UNDOFORMAT("last record size %u != xlog last record size %u."), 
            lastRecordSize, xlundometa->lastRecordSize)));
        return false;
    } else {
        xlundometa->lastRecordSize = lastRecordSize;
    }
    return true;
}

int PrepareUndoRecord(_in_ URecVector *urecvec, _in_ UndoPersistence upersistence, 
    XlUndoHeader *xlundohdr, undo::XlogUndoMeta *xlundometa)
{
    if (urecvec == NULL) {
        return UNDO_RET_FAIL;
    }

    Assert(urecvec->Size() > 0);
    UndoRecordSetUInfo(urecvec);

    UndoRecPtr undoPtr;
    UndoRecord *urec = (*urecvec)[0];
    uint32 totalSize = urecvec->TotalSize();
    UndoRecordSize undoSize = 0;
    bool needSwitch = false;

    WHITEBOX_TEST_STUB(UNDO_PREPARE_RECORD_FAILED, WhiteboxDefaultErrorEmit);

    if (t_thrd.xlog_cxt.InRecovery) {
        undoPtr = xlundohdr->urecptr;
        needSwitch = undo::CheckNeedSwitch(upersistence, totalSize, undoPtr);
        if (needSwitch) {
            urec->SetUinfo(UNDO_UREC_INFO_TRANSAC);
        }
    } else {
        if (!g_instance.attr.attr_storage.enable_ustore) {
            ereport(ERROR, (errmsg(UNDOFORMAT("Ustore is disabled, "
                "please set GUC enable_ustore=on and restart database."))));
        }
        needSwitch = undo::CheckNeedSwitch(upersistence, totalSize);
        if (needSwitch) {
            urec->SetUinfo(UNDO_UREC_INFO_TRANSAC);
            totalSize = urecvec->TotalSize();
        }
        undoPtr = undo::AllocateUndoSpace(urec->Xid(), upersistence, totalSize, needSwitch, xlundometa);
    }

    Assert(undoPtr != INVALID_UNDO_REC_PTR);

    for (auto i = 0; i < urecvec->Size(); i++) {
        UndoRecord *urec = (*urecvec)[i];
        if (!urec->NeedInsert()) {
            continue;
        }

        UndoRecPtr urecptr = INVALID_UNDO_REC_PTR;
        undoSize = urec->RecordSize();
        if ((urecptr = urec->Prepare(upersistence, &undoPtr)) == INVALID_UNDO_REC_PTR) {
            ereport(PANIC, (errmsg(UNDOFORMAT("prepare %d bytes failed on zid %d"), 
                urec->RecordSize(), t_thrd.undo_cxt.zids[upersistence])));
            return UNDO_RET_FAIL;
        }

        if (!xlundometa->IsSkipInsert()) {
            ereport(DEBUG5, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("Uinfo = %d; Urp = %lu; Prevurp2 = %lu"), 
                urec->Uinfo(), urec->Urp(), urec->Prevurp2())));
            BlockNumber curBlk = UNDO_PTR_GET_BLOCK_NUM(urecptr);
            int startingByte = UNDO_PTR_GET_PAGE_OFFSET(urecptr);
            RelFileNode rnode;
            UNDO_PTR_ASSIGN_REL_FILE_NODE(rnode, urecptr, UNDO_DB_OID);

            ReadBufferMode rbm = RBM_NORMAL;
            if (startingByte == UNDO_LOG_BLOCK_HEADER_SIZE) {
                rbm = RBM_ZERO;
#ifdef DEBUG_UHEAP
                UHEAPSTAT_COUNT_UNDO_RECORD_PREPARE_RZERO();
#endif
            } else {
#ifdef DEBUG_UHEAP
                UHEAPSTAT_COUNT_UNDO_RECORD_PREPARE_NZERO();
#endif
            }

            UndoRecordSize curSize = 0;
            do {
                if (curBlk % UNDOSEG_SIZE == 0) {
                    rbm = RBM_NORMAL;
                }
                int bufidx = urecvec->GetUndoBufidx(rnode, curBlk, rbm, upersistence);
                if (urec->Bufidx() == -1) {
                    urec->SetBufidx(bufidx);
                }
                if (curSize == 0) {
                    curSize = BLCKSZ - startingByte;
                } else {
                    curSize += BLCKSZ - UNDO_LOG_BLOCK_HEADER_SIZE;
                }

                curBlk++;
                rbm = RBM_ZERO;

#ifdef DEBUG_UHEAP
                if (curSize < undoSize) {
                    UHEAPSTAT_COUNT_UNDO_RECORD_PREPARE_RZERO();
                }
#endif
            } while (curSize < undoSize);
        }
    }

    CheckLastRecordSize(undoSize, xlundometa);
    return UNDO_RET_SUCC;
}

void InsertPreparedUndo(_in_ URecVector *urecvec, _in_ XLogRecPtr lsn)
{
    if (urecvec == NULL) {
        return;
    }

    WHITEBOX_TEST_STUB(UNDO_INSERT_PREPARED_FAILED, WhiteboxDefaultErrorEmit);

    for (auto i = 0; i < urecvec->Size(); i++) {
        UndoRecord *urec = (*urecvec)[i];
        if (!urec->NeedInsert()) {
            continue;
        }

        urec->CheckBeforAppend();
        UndoRecordSize undoLen = urec->RecordSize();

        int startingByte = UNDO_PTR_GET_PAGE_OFFSET(urec->Urp());
        int alreadyWritten = 0;
        int bufIdx = urec->Bufidx();
        Page page = NULL;
        do {
            UndoBuffer *ubuffer = urecvec->GetUBuffer(bufIdx);
            Buffer buffer = ubuffer->buf;
            if (BufferIsValid(buffer)) {
                page = BufferGetPage(buffer);
                if (((PageHeader)page)->pd_upper == 0) {
                    ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT("INIT UNDO PAGE: urp=%lu, blockno=%u"),
                        urec->Urp(), GetBufferDescriptor(buffer - 1)->tag.blockNum)));
                    PageInit(page, BLCKSZ, 0);
                }
                if (!t_thrd.xlog_cxt.InRecovery || PageGetLSN(page) < lsn) {
                    if (urec->Append(page, startingByte, &alreadyWritten, undoLen)) {
                        MarkBufferDirty(buffer);
                        if (t_thrd.xlog_cxt.InRecovery) {
                            PageSetLSN(page, lsn);
                        }
                        break;
                    }
                    MarkBufferDirty(buffer);
                    if (t_thrd.xlog_cxt.InRecovery) {
                        PageSetLSN(page, lsn);
                    }
                }else {
                    urec->Append(page, startingByte, &alreadyWritten, undoLen);
                }
            } else {
                ereport(PANIC, (errmsg(UNDOFORMAT("unknow buffer: %d"), buffer)));
                break;
            }
            startingByte = UNDO_LOG_BLOCK_HEADER_SIZE;
            bufIdx++;
        } while (bufIdx < urecvec->UbufferIdx());
    }
}

void SetUndoPageLSN(_in_ URecVector *urecvec, _in_ XLogRecPtr lsn)
{
    for (int idx = 0; idx < urecvec->UbufferIdx(); idx++) {
        if (urecvec->GetUBuffer(idx)->inUse) {
            Page page = BufferGetPage(urecvec->GetUBuffer(idx)->buf);
            PageSetLSN(page, lsn);
        }
    }
}
