/* -------------------------------------------------------------------------
 *
 * knl_uundorecord.cpp
 *     c++ code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_uundorecord.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/ustore/knl_uundorecord.h"

#include "access/ustore/undo/knl_uundoapi.h"
#include "access/heapam.h"
#include "pgstat.h"
#include "storage/buf/bufmgr.h"

namespace {
const UndoRecordSize UNDO_RECORD_FIX_SIZE = SIZE_OF_UNDO_RECORD_HEADER + SIZE_OF_UNDO_RECORD_BLOCK;

bool InsertUndoBytes(_in_ const char *srcptr, _in_ int srclen, __inout char **writeptr, _in_ const char *endptr,
    __inout int *myBytesWritten, __inout int *alreadyWritten)
{
    if (*myBytesWritten >= srclen) {
        *myBytesWritten -= srclen;
        return true;
    }

    int remaining = srclen - *myBytesWritten;
    int maxWriteOnCurrPage = endptr - *writeptr;
    int canWrite = Min(remaining, maxWriteOnCurrPage);

    if (canWrite == 0) {
        return false;
    }

    errno_t rc = memcpy_s(*writeptr, maxWriteOnCurrPage, srcptr + *myBytesWritten, canWrite);
    securec_check(rc, "\0", "\0");

    *writeptr += canWrite;
    *alreadyWritten += canWrite;
    *myBytesWritten = 0;

    return (canWrite == remaining);
}

bool ReadUndoBytes(_in_ char *destptr, _in_ int destlen, __inout char **readeptr, _in_ const char *endptr,
    __inout int *myBytesRead, __inout int *alreadyRead)
{
    if (*myBytesRead >= destlen) {
        *myBytesRead -= destlen;
        return true;
    }

    int remaining = destlen - *myBytesRead;
    int maxReadOnCurrPage = endptr - *readeptr;
    int canRead = Min(remaining, maxReadOnCurrPage);

    if (canRead == 0) {
        return false;
    }

    errno_t rc = memcpy_s(destptr + *myBytesRead, remaining, *readeptr, canRead);
    securec_check(rc, "\0", "\0");

    *readeptr += canRead;
    *alreadyRead += canRead;
    *myBytesRead = 0;

    return (canRead == remaining);
}
} // namespace

UndoRecord::UndoRecord()
{
    whdr_.Init2DefVal();
    wblk_.Init2DefVal();
    wtxn_.Init2DefVal();
    wpay_.Init2DefVal();
    wtd_.Init2DefVal();
    wpart_.Init2DefVal();
    wtspc_.Init2DefVal();
    rawdata_.data = NULL;
    rawdata_.len = 0;
    SetUrp(INVALID_UNDO_REC_PTR);
    SetBuff(InvalidBuffer);
    SetBufidx(-1);
    SetNeedInsert(false);
}

UndoRecord::~UndoRecord()
{
    Reset(INVALID_UNDO_REC_PTR);
}

void UndoRecord::Destroy()
{
    Reset(INVALID_UNDO_REC_PTR);
}

void UndoRecord::Reset(UndoRecPtr urp)
{
    whdr_.Init2DefVal();
    wblk_.Init2DefVal();
    wtxn_.Init2DefVal();
    wpay_.Init2DefVal();
    wtd_.Init2DefVal();
    wpart_.Init2DefVal();
    wtspc_.Init2DefVal();

    if (BufferIsValid(buff_)) {
        if (!IS_VALID_UNDO_REC_PTR(urp) || (UNDO_PTR_GET_ZONE_ID(urp) != UNDO_PTR_GET_ZONE_ID(urp_)) ||
            (UNDO_PTR_GET_BLOCK_NUM(urp) != BufferGetBlockNumber(buff_))) {
            ReleaseBuffer(buff_);
            buff_ = InvalidBuffer;
        }
    } else {
        if (rawdata_.data != NULL) {
            pfree(rawdata_.data);
        }
    }

    rawdata_.data = NULL;
    rawdata_.len = 0;

    SetUrp(urp);
    SetBufidx(-1);
    SetNeedInsert(false);
}

void UndoRecord::Reset2Blkprev()
{
    Reset(Blkprev());
}

UndoRecordSize UndoRecord::MemoryRecordSize()
{
    return sizeof(UndoRecord) + rawdata_.len;
}

UndoRecordSize UndoRecord::RecordSize()
{
    UndoRecordSize size = UNDO_RECORD_FIX_SIZE + sizeof(UndoRecordSize);
    if ((whdr_.uinfo & UNDO_UREC_INFO_PAYLOAD) != 0) {
        size += SIZE_OF_UNDO_RECORD_PAYLOAD;
        size += rawdata_.len;
    }
    if ((whdr_.uinfo & UNDO_UREC_INFO_TRANSAC) != 0) {
        size += SIZE_OF_UNDO_RECORD_TRANSACTION;
    }
    if ((whdr_.uinfo & UNDO_UREC_INFO_OLDTD) != 0) {
        size += SIZE_OF_UNDO_RECORD_OLDTD;
    }
    if ((whdr_.uinfo & UNDO_UREC_INFO_HAS_PARTOID) != 0) {
        size += SIZE_OF_UNDO_RECORD_PARTITION;
    }
    if ((whdr_.uinfo & UNDO_UREC_INFO_HAS_TABLESPACEOID) != 0) {
        size += SIZE_OF_UNDO_RECORD_TABLESPACE;
    }

    return size;
}
UndoRecPtr UndoRecord::Prevurp(UndoRecPtr currUrp, Buffer *buffer)
{
    if (IS_VALID_UNDO_REC_PTR(wtxn_.prevurp)) {
        return wtxn_.prevurp;
    }

    int zoneId = UNDO_PTR_GET_ZONE_ID(currUrp);
    UndoLogOffset offset = UNDO_PTR_GET_OFFSET(currUrp);
    UndoRecordSize prevLen = GetPrevRecordLen(currUrp, buffer);

    ereport(DEBUG5, (errmsg(UNDOFORMAT("Prevurp zid=%d, offset=%lu, prevLen=%u"), zoneId, offset, prevLen)));

    return MAKE_UNDO_PTR(zoneId, offset - prevLen);
}

UndoRecordSize UndoRecord::GetPrevRecordLen(UndoRecPtr currUrp, Buffer *inputBuffer)
{
    int zoneId = UNDO_PTR_GET_ZONE_ID(currUrp);
    Buffer buffer = InvalidBuffer;
    bool releaseBuffer = false;
    BlockNumber blk = UNDO_PTR_GET_BLOCK_NUM(currUrp);
    RelFileNode rnode;
    UNDO_PTR_ASSIGN_REL_FILE_NODE(rnode, currUrp, UNDO_DB_OID);
    DECLARE_NODE_COUNT();
    GET_UPERSISTENCE_BY_ZONEID(zoneId, nodeCount);
    UndoRecordSize precRecLen = 0;
    UndoLogOffset pageOffset = UNDO_PTR_GET_PAGE_OFFSET(currUrp);
    Assert(pageOffset != 0);

    if (inputBuffer == NULL || !BufferIsValid(*inputBuffer)) {
        buffer =
            ReadUndoBufferWithoutRelcache(rnode, UNDO_FORKNUM, blk, RBM_NORMAL, NULL, REL_PERSISTENCE(upersistence));
        releaseBuffer = true;
    } else {
        buffer = *inputBuffer;
    }

    char *page = (char *)BufferGetPage(buffer);
    UndoRecordSize byteToRead = sizeof(UndoRecordSize);
    char prevLen[2];

    while (byteToRead > 0) {
        pageOffset -= 1;
        if (pageOffset >= UNDO_LOG_BLOCK_HEADER_SIZE) {
            prevLen[byteToRead - 1] = page[pageOffset];
            byteToRead -= 1;
        } else {
            if (releaseBuffer) {
                ReleaseBuffer(buffer);
            }
            releaseBuffer = true;
            blk -= 1;
            buffer = ReadUndoBufferWithoutRelcache(rnode, UNDO_FORKNUM, blk, RBM_NORMAL, NULL,
                REL_PERSISTENCE(upersistence));
            pageOffset = BLCKSZ;
            page = (char *)BufferGetPage(buffer);
        }
    }

    precRecLen = *(UndoRecordSize *)(prevLen);

    if (UNDO_PTR_GET_PAGE_OFFSET(currUrp) - UNDO_LOG_BLOCK_HEADER_SIZE < precRecLen) {
        precRecLen += UNDO_LOG_BLOCK_HEADER_SIZE;
    }
    if (releaseBuffer) {
        ReleaseBuffer(buffer);
    }
    if (precRecLen == 0) {
        ereport(PANIC, (errmsg(UNDOFORMAT("Currurp %lu, prevLen=%u"), currUrp, precRecLen)));
    }
    return precRecLen;
}

UndoRecPtr UndoRecord::Prepare(UndoPersistence upersistence, UndoRecPtr *undoPtr)
{
    UndoRecordSize undoSize = RecordSize();
    urp_ = *undoPtr;
    *undoPtr = undo::AdvanceUndoPtr(*undoPtr, undoSize);
    return urp_;
}

bool UndoRecord::Append(_in_ Page page, _in_ int startingByte, __inout int *alreadyWritten, UndoRecordSize undoLen)
{
    Assert(page);

    char *writeptr = (char *)page + startingByte;
    char *endptr = (char *)page + BLCKSZ;
    int myBytesWritten = *alreadyWritten;

    if (!InsertUndoBytes((char *)&whdr_, SIZE_OF_UNDO_RECORD_HEADER, &writeptr, endptr, &myBytesWritten,
        alreadyWritten)) {
        return false;
    }
    if (!InsertUndoBytes((char *)&wblk_, SIZE_OF_UNDO_RECORD_BLOCK, &writeptr, endptr, &myBytesWritten,
        alreadyWritten)) {
        return false;
    }
    if ((whdr_.uinfo & UNDO_UREC_INFO_TRANSAC) != 0) {
        if (!InsertUndoBytes((char *)&wtxn_, SIZE_OF_UNDO_RECORD_TRANSACTION, &writeptr, endptr, &myBytesWritten,
            alreadyWritten)) {
            return false;
        }
    }
    if ((whdr_.uinfo & UNDO_UREC_INFO_OLDTD) != 0) {
        if (!InsertUndoBytes((char *)&wtd_, SIZE_OF_UNDO_RECORD_OLDTD, &writeptr, endptr, &myBytesWritten,
            alreadyWritten)) {
            return false;
        }
    }
    if ((whdr_.uinfo & UNDO_UREC_INFO_HAS_PARTOID) != 0) {
        if (!InsertUndoBytes((char *)&wpart_, SIZE_OF_UNDO_RECORD_PARTITION, &writeptr, endptr, &myBytesWritten,
            alreadyWritten)) {
            return false;
        }
    }
    if ((whdr_.uinfo & UNDO_UREC_INFO_HAS_TABLESPACEOID) != 0) {
        if (!InsertUndoBytes((char *)&wtspc_, SIZE_OF_UNDO_RECORD_TABLESPACE, &writeptr, endptr, &myBytesWritten,
            alreadyWritten)) {
            return false;
        }
    }
    if ((whdr_.uinfo & UNDO_UREC_INFO_PAYLOAD) != 0) {
        wpay_.payloadlen = rawdata_.len;
        if (!InsertUndoBytes((char *)&wpay_, SIZE_OF_UNDO_RECORD_PAYLOAD, &writeptr, endptr, &myBytesWritten,
            alreadyWritten)) {
            return false;
        }
        if (wpay_.payloadlen > 0 &&
            !InsertUndoBytes((char *)rawdata_.data, rawdata_.len, &writeptr, endptr, &myBytesWritten, alreadyWritten)) {
            return false;
        }
    }
    if (!InsertUndoBytes((char *)&undoLen, sizeof(UndoRecordSize), &writeptr, endptr, &myBytesWritten,
        alreadyWritten)) {
        return false;
    }

    return true;
}

void UndoRecord::CheckBeforAppend()
{
    Assert((wpay_.payloadlen == 0) || (wpay_.payloadlen > 0 && rawdata_.data != NULL));
}

void UndoRecord::Load(bool keepBuffer)
{
    Assert(urp_ != INVALID_UNDO_REC_PTR);

    BlockNumber blk = UNDO_PTR_GET_BLOCK_NUM(urp_);
    Buffer buffer = buff_;
    int startingByte = UNDO_PTR_GET_PAGE_OFFSET(urp_);
    RelFileNode rnode;
    UNDO_PTR_ASSIGN_REL_FILE_NODE(rnode, urp_, UNDO_DB_OID);
    bool isUndoRecSplit = false;
    bool copyData = keepBuffer;

    /* Get Undo Persistence. Stored in the variable upersistence */
    int zoneId = UNDO_PTR_GET_ZONE_ID(urp_);
    DECLARE_NODE_COUNT();
    GET_UPERSISTENCE_BY_ZONEID(zoneId, nodeCount);

    if (!BufferIsValid(buffer)) {
#ifdef DEBUG_UHEAP
        UHEAPSTAT_COUNT_UNDO_PAGE_VISITS();
#endif
        buffer =
            ReadUndoBufferWithoutRelcache(rnode, UNDO_FORKNUM, blk, RBM_NORMAL, NULL, REL_PERSISTENCE(upersistence));
        buff_ = buffer;
    }

    int alreadyRead = 0;
    do {
        Page page = BufferGetPage(buffer);
        if (ReadUndoRecord(page, startingByte, &alreadyRead, copyData)) {
            break;
        }

        startingByte = UNDO_LOG_BLOCK_HEADER_SIZE;
        blk++;
        isUndoRecSplit = true;

        if (!keepBuffer) {
            ReleaseBuffer(buffer);
            buff_ = InvalidBuffer;
        }
#ifdef DEBUG_UHEAP
        UHEAPSTAT_COUNT_UNDO_PAGE_VISITS();
#endif
        buffer =
            ReadUndoBufferWithoutRelcache(rnode, UNDO_FORKNUM, blk, RBM_NORMAL, NULL, REL_PERSISTENCE(upersistence));
    } while (true);

    if (isUndoRecSplit) {
        ReleaseBuffer(buffer);
    }
}

bool UndoRecord::ReadUndoRecord(_in_ Page page, _in_ int startingByte, __inout int *alreadyRead, _in_ bool copyData)
{
    Assert(page);

    char *readptr = (char *)page + startingByte;
    char *endptr = (char *)page + BLCKSZ;
    int myBytesRead = *alreadyRead;
    bool isUndoSplited = myBytesRead > 0 ? true : false;

    if (!ReadUndoBytes((char *)&whdr_, SIZE_OF_UNDO_RECORD_HEADER, &readptr, endptr, &myBytesRead, alreadyRead)) {
        return false;
    }
    if (!ReadUndoBytes((char *)&wblk_, SIZE_OF_UNDO_RECORD_BLOCK, &readptr, endptr, &myBytesRead, alreadyRead)) {
        return false;
    }

    if ((whdr_.uinfo & UNDO_UREC_INFO_TRANSAC) != 0) {
        if (!ReadUndoBytes((char *)&wtxn_, SIZE_OF_UNDO_RECORD_TRANSACTION, &readptr, endptr, &myBytesRead,
            alreadyRead)) {
            return false;
        }
    }
    if ((whdr_.uinfo & UNDO_UREC_INFO_OLDTD) != 0) {
        if (!ReadUndoBytes((char *)&wtd_, SIZE_OF_UNDO_RECORD_OLDTD, &readptr, endptr, &myBytesRead, alreadyRead)) {
            return false;
        }
    }
    if ((whdr_.uinfo & UNDO_UREC_INFO_HAS_PARTOID) != 0) {
        if (!ReadUndoBytes((char *)&wpart_, SIZE_OF_UNDO_RECORD_PARTITION, &readptr, endptr, &myBytesRead,
            alreadyRead)) {
            return false;
        }
    }
    if ((whdr_.uinfo & UNDO_UREC_INFO_HAS_TABLESPACEOID) != 0) {
        if (!ReadUndoBytes((char *)&wtspc_, SIZE_OF_UNDO_RECORD_TABLESPACE, &readptr, endptr, &myBytesRead,
            alreadyRead)) {
            return false;
        }
    }
    if ((whdr_.uinfo & UNDO_UREC_INFO_PAYLOAD) != 0) {
        if (!ReadUndoBytes((char *)&wpay_, SIZE_OF_UNDO_RECORD_PAYLOAD, &readptr, endptr, &myBytesRead, alreadyRead)) {
            return false;
        }

        rawdata_.len = wpay_.payloadlen;
        if (rawdata_.len > 0) {
            if (!copyData && !isUndoSplited && rawdata_.len <= (endptr - readptr)) {
                rawdata_.data = readptr;
            } else {
                if (rawdata_.len > 0 && rawdata_.data == NULL) {
                    rawdata_.data = (char *)palloc0(rawdata_.len);
                }
                if (!ReadUndoBytes((char *)rawdata_.data, rawdata_.len, &readptr, endptr, &myBytesRead, alreadyRead)) {
                    return false;
                }
            }
        }
    }
    return true;
}

static UndoRecordState LoadUndoRecord(UndoRecord *urec)
{
    UndoRecordState state = undo::CheckUndoRecordValid(urec->Urp(), true);
    if (state != UNDO_RECORD_NORMAL) {
        return state;
    }

    int saveInterruptHoldoffCount = t_thrd.int_cxt.InterruptHoldoffCount;
    PG_TRY();
    {
        t_thrd.undo_cxt.fetchRecord = true;
        urec->Load(false);
    }
    PG_CATCH();
    {
        state = undo::CheckUndoRecordValid(urec->Urp(), true);
        if ((!t_thrd.undo_cxt.fetchRecord) && (state == UNDO_RECORD_DISCARD ||
            state == UNDO_RECORD_FORCE_DISCARD)) {
            t_thrd.int_cxt.InterruptHoldoffCount = saveInterruptHoldoffCount;
            if (BufferIsValid(urec->Buff())) {
                ReleaseBuffer(urec->Buff());
                urec->SetBuff(InvalidBuffer);
            }
            return state;
        } else {
            PG_RE_THROW();
        }
    }
    PG_END_TRY();
    t_thrd.undo_cxt.fetchRecord = false;
    return UNDO_RECORD_NORMAL;
}

UndoTraversalState FetchUndoRecord(__inout UndoRecord *urec, _in_ SatisfyUndoRecordCallback callback,
    _in_ BlockNumber blkno, _in_ OffsetNumber offset, _in_ TransactionId xid, bool isNeedBypass)
{
    int64 undo_chain_len = 0; /* len of undo chain for one tuple */

    Assert(urec);

    if (RecoveryInProgress()) {
        uint64 blockcnt = 0;
        while (undo::CheckUndoRecordValid(urec->Urp(), false) == UNDO_RECORD_NOT_INSERT) {
            ereport(LOG,
                (errmsg(UNDOFORMAT("urp: %ld is not replayed yet. ROS waiting for UndoRecord replay."),
                urec->Urp())));

            pg_usleep(1000L); /* 1ms */
            if (blockcnt % 1000 == 0) {
                CHECK_FOR_INTERRUPTS();
            }
        }
        if (undo::CheckUndoRecordValid(urec->Urp(), false) == UNDO_RECORD_DISCARD) {
            return UNDO_TRAVERSAL_END;
        }
    }

    do {
        UndoRecordState state = LoadUndoRecord(urec);
        if (state == UNDO_RECORD_INVALID || state == UNDO_RECORD_DISCARD) {
            return UNDO_TRAVERSAL_END;
        } else if (state == UNDO_RECORD_FORCE_DISCARD) {
            return UNDO_TRAVERSAL_ABORT;
        }

        if (isNeedBypass && TransactionIdPrecedes(urec->Xid(), g_instance.undo_cxt.oldestFrozenXid)) {
            ereport(DEBUG1, (errmsg(UNDOFORMAT("Check visibility by oldestFrozenXid"))));
            return UNDO_TRAVERSAL_STOP;
        }

        ++undo_chain_len;

        if (blkno == InvalidBlockNumber) {
            break;
        }

        if (callback(urec, blkno, offset, xid)) {
            break;
        }

        ereport(DEBUG3, (errmsg(UNDOFORMAT("fetch blkprev undo :%lu, curr undo: %lu"), urec->Blkprev(), urec->Urp())));

        urec->Reset2Blkprev();
    } while (true);

#ifdef DEBUG_UHEAP
    UHEAPSTAT_COUNT_UNDO_CHAIN_VISTIED(undo_chain_len)
#endif
    g_instance.undo_cxt.undoChainTotalSize += undo_chain_len;
    g_instance.undo_cxt.undo_chain_visited_count += 1;
    g_instance.undo_cxt.maxChainSize =
        g_instance.undo_cxt.maxChainSize > undo_chain_len ? g_instance.undo_cxt.maxChainSize : undo_chain_len;
    return UNDO_TRAVERSAL_COMPLETE;
}

bool InplaceSatisfyUndoRecord(_in_ UndoRecord *urec, _in_ BlockNumber blkno, _in_ OffsetNumber offset,
    _in_ TransactionId xid)
{
    Assert(urec != NULL);
    Assert(urec->Blkno() != InvalidBlockNumber);

    if (urec->Blkno() != blkno || (TransactionIdIsValid(xid) && !TransactionIdEquals(xid, urec->Xid()))) {
        return false;
    }

    switch (urec->Utype()) {
        case UNDO_MULTI_INSERT: {
            OffsetNumber start_offset;
            OffsetNumber end_offset;

            Assert(urec->Rawdata() != NULL);
            start_offset = ((OffsetNumber *)urec->Rawdata()->data)[0];
            end_offset = ((OffsetNumber *)urec->Rawdata()->data)[1];

            if (offset >= start_offset && offset <= end_offset) {
                return true;
            }
        } break;
        default: {
            Assert(offset != InvalidOffsetNumber);
            if (urec->Offset() == offset) {
                return true;
            }
        } break;
    }

    return false;
}
