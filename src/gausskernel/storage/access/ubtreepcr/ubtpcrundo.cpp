/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * --------------------------------------------------------------------------------------
*
 * ubtpcrundo.cpp
 *        Relaize the management of translation slot for ubtree pac page.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/ubtreepcr/ubtpcrundo.cpp
 *
 * --------------------------------------------------------------------------------------
 */


#include "access/ubtreepcr.h"

UndoRecPtr UBTreePCRPrepareUndoInsert(Oid relOid, Oid partitionOid, Oid relfilenode, Oid tablespace,
    UndoPersistence persistence, TransactionId xid, CommandId cid, UndoRecPtr prevurpInOneBlk,
    UndoRecPtr prevurpInOneXact, BlockNumber blk, XlUndoHeader *xlundohdr, undo::XlogUndoMeta *xlundometa,
    OffsetNumber offset, Buffer buf, bool selfInsert, UBTreeUndoInfo undoinfo, IndexTuple itup)
{
    UndoRecord *urec = (*t_thrd.ustore_cxt.urecvec)[0];
    Assert(tablespace != InvalidOid);

    urec->SetUtype(UNDO_UBT_INSERT);
    urec->SetUinfo(UNDO_UREC_INFO_PAYLOAD);
    urec->SetXid(xid);
    urec->SetCid(cid);
    urec->SetReloid(relOid);
    urec->SetPartitionoid(partitionOid);
    urec->SetRelfilenode(relfilenode);
    urec->SetTablespace(tablespace);
    urec->SetOffset(offset);
    urec->SetBlkprev(prevurpInOneBlk);
    urec->SetPrevurp(t_thrd.xlog_cxt.InRecovery ? prevurpInOneXact : GetCurrentTransactionUndoRecPtr(persistence));
    urec->SetNeedInsert(true);
    if (t_thrd.xlog_cxt.InRecovery) {
        urec->SetBlkno(blk);
    } else {
        if (BufferIsInvalid(buf)) {
            urec->SetBlkno(InvalidBlockNumber);
        } else {
            urec->SetBlkno(BufferGetBlockNumber(buf));
        }
    }
    /* Tell Undo chain traversal this record does not have any older version */
    if (selfInsert) {
        urec->SetOldXactId(xid);
    } else {
        urec->SetOldXactId(FrozenTransactionId);
    }

    MemoryContext old_cxt = MemoryContextSwitchTo(urec->mem_context());
    initStringInfo(urec->Rawdata());
    MemoryContextSwitchTo(old_cxt);
    appendBinaryStringInfo(urec->Rawdata(), (char*)undoinfo, sizeof(UBTreeUndoInfoData));
    IndexTupleSetSize(itup, IndexTupleSize(itup) - MAXALIGN(sizeof(IndexTupleTrxData)));
    appendBinaryStringInfo(urec->Rawdata(), (char*)itup, IndexTupleSize(itup));
    IndexTupleSetSize(itup, IndexTupleSize(itup) + MAXALIGN(sizeof(IndexTupleTrxData)));
    int status = PrepareUndoRecord(t_thrd.ustore_cxt.urecvec, persistence, xlundohdr, xlundometa);
    /* Do not continue if there was a failure during Undo preparation */
    if (status != UNDO_RET_SUCC) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Failed to generate UndoRecord")));
    }

    UndoRecPtr urecptr = urec->Urp();
    Assert(IS_VALID_UNDO_REC_PTR(urecptr));

    return urecptr;
}

IndexTuple FetchTupleFromUndoRecord(UndoRecord *urec)
{
    Assert(urec->Rawdata()->data != NULL);
    return (IndexTuple)((char*)(urec->Rawdata()->data) + SizeOfUBTreeUndoInfoData);
}

UBTreeUndoInfo FetchUndoInfoFromUndoRecord(UndoRecord *urec)
{
    Assert(urec->Rawdata()->data != NULL);
    return (UBTreeUndoInfo)((char*)(urec->Rawdata()->data));
}