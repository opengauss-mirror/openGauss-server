/* -------------------------------------------------------------------------
 *
 * knl_uundorecord.h
 * c++ code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/include/access/ustore/knl_uundorecord.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __KNL_UUNDORECORD_H__
#define __KNL_UUNDORECORD_H__

#include "access/ustore/undo/knl_uundotype.h"
#include "access/ustore/undo/knl_uundoxlog.h"
#include "access/ustore/knl_utype.h"
#include "lib/stringinfo.h"
#include "storage/buf/bufmgr.h"
#include "storage/smgr/relfilenode.h"

/*
 * Every undo record begins with an UndoRecordHeader structure, which is
 * followed by the additional structures indicated by the contents of
 * urec_info.  All structures are packed into the alignment without padding
 * bytes, and the undo record itself need not be aligned either, so care
 * must be taken when reading the header.
 */
typedef struct {
    TransactionId xid; /* Transaction id */
    CommandId cid;     /* command id */
    Oid reloid;
    Oid relfilenode;
    uint8 utype;
    uint8 uinfo;

    void Init2DefVal()
    {
        xid = InvalidTransactionId;
        cid = InvalidCommandId;
        reloid = InvalidOid;
        relfilenode = InvalidOid;
        utype = UNDO_UNKNOWN;
        uinfo = UNDO_UREC_INFO_UNKNOWN;
    }
} UndoRecordHeader;

#define SIZE_OF_UNDO_RECORD_HEADER (offsetof(UndoRecordHeader, uinfo) + sizeof(uint8)) // 22

/*
 * Identifying information for a block to which this record pertains, and
 * a pointer to the previous record for the same block.
 */
typedef struct {
    UndoRecPtr blkprev;  /* byte offset of previous undo for block */
    BlockNumber blkno;   /* block number */
    OffsetNumber offset; /* offset number */

    inline void Init2DefVal()
    {
        blkprev = INVALID_UNDO_REC_PTR;
        blkno = InvalidBlockNumber;
        offset = InvalidOffsetNumber;
    }
} UndoRecordBlock;

#define SIZE_OF_UNDO_RECORD_BLOCK (offsetof(UndoRecordBlock, offset) + sizeof(OffsetNumber)) // 12+2

/*
 * Identifying information for a transaction to which this undo belongs.  This
 * also stores the dbid and the progress of the undo apply during rollback.
 */
typedef struct {
    UndoRecPtr prevurp;

    inline void Init2DefVal()
    {
        prevurp = INVALID_UNDO_REC_PTR;
    }
} UndoRecordTransaction;

#define SIZE_OF_UNDO_RECORD_TRANSACTION (offsetof(UndoRecordTransaction, prevurp) + sizeof(UndoRecPtr)) // 0+8

typedef struct {
    TransactionId oldxactid;

    inline void Init2DefVal()
    {
        oldxactid = FrozenTransactionId;
    }
} UndoRecordOldTd;

#define SIZE_OF_UNDO_RECORD_OLDTD (offsetof(UndoRecordOldTd, oldxactid) + sizeof(TransactionId)) // 0+8

typedef struct {
    Oid partitionoid;

    inline void Init2DefVal()
    {
        partitionoid = InvalidOid;
    }
} UndoRecordPartition;

#define SIZE_OF_UNDO_RECORD_PARTITION (offsetof(UndoRecordPartition, partitionoid) + sizeof(Oid)) // 0+4

typedef struct {
    Oid tablespace;

    inline void Init2DefVal()
    {
        tablespace = InvalidOid;
    }
} UndoRecordTablespace;

#define SIZE_OF_UNDO_RECORD_TABLESPACE (offsetof(UndoRecordTablespace, tablespace) + sizeof(Oid)) // 0+4

/*
 * Information about the amount of payload data and tuple data present
 * in this record.  The payload bytes immediately follow the structures
 * specified by flag bits in urec_info, and the tuple bytes follow the
 * payload bytes.
 */
typedef struct {
    UndoRecordSize payloadlen; /* # of payload bytes */

    void Init2DefVal()
    {
        payloadlen = 0;
    }
} UndoRecordPayload;

#define SIZE_OF_UNDO_RECORD_PAYLOAD (offsetof(UndoRecordPayload, payloadlen) + sizeof(UndoRecordSize))

class UndoRecord : public BaseObject {
public:
    StringInfoData rawdata_;
    UndoRecord();
    ~UndoRecord();
    void Destroy();
    // Reset befor reuse it.
    void Reset(UndoRecPtr urp);
    void Reset2Blkprev();

    UndoRecordSize MemoryRecordSize();
    UndoRecordSize RecordSize();
    UndoRecPtr Prepare(UndoPersistence upersistence, UndoRecPtr *undoPtr);
    bool Append(_in_ Page page, _in_ int startingByte, __inout int *alreadyWritten, _in_ UndoRecordSize undoLen);
    void CheckBeforAppend();
    void Load(bool keepBuffer);
    bool ReadUndoRecord(_in_ Page page, _in_ int startingByte, __inout int *alreadyRead, _in_ bool copyData);

    // Getter
    inline int Index()
    {
        return index_;
    }
    inline TransactionId Xid()
    {
        return whdr_.xid;
    }
    inline CommandId Cid()
    {
        return whdr_.cid;
    }
    inline Oid Reloid()
    {
        return whdr_.reloid;
    }
    inline Oid Relfilenode()
    {
        return whdr_.relfilenode;
    }
    inline Oid Tablespace()
    {
        return wtspc_.tablespace;
    }
    inline Oid Partitionoid()
    {
        return wpart_.partitionoid;
    }
    inline uint8 Utype()
    {
        return whdr_.utype;
    }
    inline uint8 Uinfo()
    {
        return whdr_.uinfo;
    }
    inline bool ContainSubXact();
    inline UndoRecPtr Blkprev()
    {
        return wblk_.blkprev;
    }
    inline BlockNumber Blkno()
    {
        return wblk_.blkno;
    }
    inline OffsetNumber Offset()
    {
        return wblk_.offset;
    }
    inline TransactionId OldXactId()
    {
        return wtd_.oldxactid;
    }
    UndoRecPtr Prevurp(UndoRecPtr currUrp, Buffer *buffer);
    static UndoRecordSize GetPrevRecordLen(UndoRecPtr currUrp, Buffer *inputBuffer);
    inline StringInfoData *Rawdata()
    {
        return &rawdata_;
    }
    inline UndoRecPtr Urp()
    {
        return urp_;
    }
    inline Buffer Buff()
    {
        return buff_;
    }
    inline int Bufidx()
    {
        return bufidx_;
    }
    inline bool NeedInsert()
    {
        return needInsert_;
    }

    inline UndoRecPtr Prevurp2()
    {
        return wtxn_.prevurp;
    }

    // Setter
    inline void SetIndex(int index)
    {
        index_ = index;
    }
    inline void SetXid(TransactionId xid)
    {
        whdr_.xid = xid;
    }
    inline void SetCid(CommandId cid)
    {
        whdr_.cid = cid;
    }
    inline void SetReloid(Oid reloid)
    {
        whdr_.reloid = reloid;
    }
    inline void SetRelfilenode(Oid relfilenode)
    {
        whdr_.relfilenode = relfilenode;
    }
    inline void SetTablespace(Oid tablespace)
    {
        wtspc_.tablespace = tablespace;
    }
    inline void SetPartitionoid(Oid partitionoid)
    {
        wpart_.partitionoid = partitionoid;
    }
    inline void SetUtype(uint8 utype)
    {
        whdr_.utype = utype;
    }
    inline void SetUinfo(uint8 uinfo)
    {
        whdr_.uinfo |= uinfo;
    }
    inline void SetBlkprev(UndoRecPtr blkprev)
    {
        wblk_.blkprev = blkprev;
    }
    inline void SetBlkno(BlockNumber blk)
    {
        wblk_.blkno = blk;
    }
    inline void SetOffset(OffsetNumber offset)
    {
        wblk_.offset = offset;
    }
    inline void SetOldXactId(TransactionId xid);
    inline void SetPrevurp(UndoRecPtr prevurp)
    {
        wtxn_.prevurp = prevurp;
    }
    inline void SetUrp(UndoRecPtr urp)
    {
        urp_ = urp;
    }
    inline void SetBuff(Buffer buff)
    {
        buff_ = buff;
    }
    inline void SetBufidx(int bufidx)
    {
        bufidx_ = bufidx;
    }
    inline void SetNeedInsert(bool needInsert)
    {
        needInsert_ = needInsert;
    }

private:
    int index_;

    UndoRecordHeader whdr_;
    UndoRecordBlock wblk_;
    UndoRecordTransaction wtxn_;
    UndoRecordPayload wpay_;
    UndoRecordOldTd wtd_;
    UndoRecordPartition wpart_;
    UndoRecordTablespace wtspc_;

    UndoRecPtr urp_; /* undo record pointer */
    Buffer buff_;    /* buffer in which undo record data points */
    int bufidx_;

    bool needInsert_;
}; // class UndoRecord

inline bool UndoRecord::ContainSubXact()
{
    if ((whdr_.uinfo & UNDO_UREC_INFO_CONTAINS_SUBXACT) != 0) {
        return true;
    }
    return false;
}

inline void UndoRecord::SetOldXactId(TransactionId xid)
{
    wtd_.oldxactid = xid;
}

typedef bool (*SatisfyUndoRecordCallback)(_in_ UndoRecord *urec, _in_ BlockNumber blkno, _in_ OffsetNumber offset,
    _in_ TransactionId xid);

/*
 * Fetch the undo record for given blkno, offset and transaction id (if valid).
 *
 * @param urec[__inout ], required.
 * Undo record should set urp with a start urp or a specified urp.
 * allocate:   UndoRecord *urec = New(CurrentMemoryContext) UndoRecord();
 * ...
 * urec reuse: urec->Reset(urp);
 * ...
 * release:    DELETE_EX(urec);
 *
 * @param callback[_in_ ], optional.
 * @sa SatisfyUndoRecordCallback. The input params of the callback function
 * are blkno/offset/xid below.
 * @param blkno[_in_ ], optional.
 * @param offset[_in_ ], optional.
 * @param xid[_in_ ], optional.
 *
 * Returns the UNDO_RET_SUCC if found, otherwise, return UNDO_RET_FAIL.
 */
UndoTraversalState FetchUndoRecord(__inout UndoRecord *urec, _in_ SatisfyUndoRecordCallback callback,
    _in_ BlockNumber blkno, _in_ OffsetNumber offset, _in_ TransactionId xid, bool isNeedByPass = false);

/*
 * Example: satisfied callback function.
 */
bool InplaceSatisfyUndoRecord(_in_ UndoRecord *urec, _in_ BlockNumber blkno, _in_ OffsetNumber offset,
    _in_ TransactionId xid);

#endif // __KNL_UUNDORECORD_H__
