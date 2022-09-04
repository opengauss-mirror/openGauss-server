/* -------------------------------------------------------------------------
 *
 * knl_uundovec.h
 * c++ code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/knl_uundovec.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __KNL_UUNDOVEC_H__
#define __KNL_UUNDOVEC_H__

#include "access/ustore/knl_uundorecord.h"
#include "access/ustore/knl_uredo.h"
#include "access/ustore/undo/knl_uundoxlog.h"

/*
 * XXX Do we want to support undo tuple size which is more than the BLCKSZ
 * if not than undo record can spread across 2 buffers at the max.
 */
#define MAX_BUFFER_PER_UNDO 2
#define UNDO_VECTOR_EXPANSION_COEFFICIENT 2

/* Undo block number to buffer mapping. */
struct UndoBuffer {
    int zoneId;      /* zone id */
    BlockNumber blk; /* block number */
    Buffer buf;      /* buffer allocated for the block */
    bool zero;       /* new block full of zeroes */

    /*
     * Note that we keep all Undo buffers used in the current txn as pinned
     * throughout the txn and tracked in the undo_buffer array.
     * This flag is used to mark the undo buffers that are currently used
     * by the CurrentResourceOwner (ie resources used in the current sql statement)
     */
    bool inUse;
};

class URecVector : public BaseObject {
public:
    URecVector();
    UndoBuffer *ubuffers_;
    /*
     * for insert       the capacity==1,    isPrepared=true
     * for update       the capacity==2,    isPrepared=true
     * for multi insert the capacity==n,    isPrepared=true
     * for select       the capacity==1024, isPrepared=false
     */
    void Initialize(int nSize, bool isPrepared);
    void Destroy();
    void Reset(bool needUnlockBuffer = true);
    inline UndoRecord *&operator[](int i);
    int GetUndoBufidx(RelFileNode rnode, BlockNumber blk, ReadBufferMode rbm);
    bool PushBack(UndoRecord *urec);
    void SortByBlkNo();
    uint64 TotalSize();
    UndoRecPtr LastRecord();
    UndoRecordSize LastRecordSize();
    void SetMemoryContext(MemoryContext mem_cxt);
    /* The first record must be valid. */
    UndoRecPtr FirstRecord()
    {
        return urecs_[0]->Urp();
    }
    // Getter.
    inline int Size() const
    {
        return size_;
    }
    inline UndoBuffer *GetUBuffer(int bufidx) const
    {
        return ubuffers_ + bufidx;
    }
    inline int UbufferIdx() const
    {
        return ubuffersIdx_;
    }
    inline bool IsPrepared()
    {
        return isPrepared_;
    }

private:
    void NewCapacity(int newCapacity);
    void SetUrecsZero(int start, int end);

    UndoRecord **urecs_;
    int size_;
    int capacity_;
    int ubuffersIdx_;
    bool isPrepared_;
    bool isInited_;
    MemoryContext mem_context_;
}; // class URecVector

inline UndoRecord *&URecVector::operator[](int i)
{
    Assert(isInited_);
    Assert(i >= 0 && i < size_);

    return *(urecs_ + i);
}

URecVector *FetchUndoRecordRange(__inout UndoRecPtr *startUrp, _in_ UndoRecPtr endUrp, 
    _in_ int maxUndoApplySize, _in_ bool onePage);

/*
 * Prepare 0-n undo record. the urecvec is filled by caller.
 * 1. URecVector *urecvec = New(CurrentMemoryContext) URecVector();
 * 2. urecvec->Initialize(int, bool); // @sa UndoRecord::Initialize
 * 3. UndoRecord *urec = New(CurrentMemoryContext) UndoRecord();
 * 4. urec->SetXid/SetCid/SetBlkprev/SetBlkno/SetOffset/SetOldtd/SetPrevurp
 * also you can get the raw data by Rawdata() and initialze it.
 * 5. urecvec->PushBack(urec);
 * 6. loop 3-5 if you need.
 *
 * @param urecvec[__inout ], required.
 * @param upersistence[_in_ ], required.
 *
 * Returns the UNDO_RET_SUCC if succ, otherwise, return UNDO_RET_FAIL.
 */
int PrepareUndoRecord(__inout URecVector *urecvec, _in_ UndoPersistence upersistence, _in_ XlUndoHeader *xlundohdr,
    _in_ undo::XlogUndoMeta *xlundometa);

/*
 * Prepare 0-n undo record.
 *
 * @param urecvec[_in_ ], required.
 * The param is the same as the param in PrepareUndoRecord. but, the param
 * needs to be processed by PrepareUndoRecord first.
 */
void InsertPreparedUndo(_in_ URecVector *urecvec, _in_ XLogRecPtr lsn = 0);

/*
 * Set LSN of .
 *
 * @param urecvec[_in_ ], required.
 * The param is the same as the param in InsertPreparedUndo.
 */
void SetUndoPageLSN(_in_ URecVector *urecvec, _in_ XLogRecPtr lsn);

void ReleaseUndoBuffers(void);

void VerifyUndoRecordValid(UndoRecord *urec, bool needCheckXidInvalid = false);
#endif // __KNL_UUNDOVEC_H__
