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
 * ubtpcrrecycle.cpp
 *       Recycle logic for ubtree par page.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/ubtreepcr/ubtpcrrecycle.cpp
 *
 * --------------------------------------------------------------------------------------
 */


#include "access/ubtreepcr.h"


UBTRecycleQueueHeader UBTreePCRGetRecycleQueueHeader(Page page, BlockNumber blkno)
{
    return NULL;
}

Buffer UBTreePCRReadRecycleQueueBuffer(Relation rel, BlockNumber blkno)
{
    return NULL;
}

void UBTreePCRInitializeRecycleQueue(Relation rel)
{
    return;
}

void UBTreePCRTryRecycleEmptyPage(Relation rel)
{
    return;
}

void UBTreePCRRecordFreePage(Relation rel, BlockNumber blkno, TransactionId xid)
{
    return;
}

void UBTreePCRRecordEmptyPage(Relation rel, BlockNumber blkno, TransactionId xid)
{
    return;
}

void UBTreePCRRecordUsedPage(Relation rel, UBTRecycleQueueAddress addr)
{
    return;
}

Buffer UBTreePCRGetAvailablePage(Relation rel, UBTRecycleForkNumber forkNumber, UBTRecycleQueueAddress* addr, UBTreeGetNewPageStats* stats)
{
    return NULL;
}

void UBTreePCRRecycleQueueInitPage(Relation rel, Page page, BlockNumber blkno, BlockNumber prevBlkno, BlockNumber nextBlkno)
{
    return;
}

void UBtreePCRRecycleQueueChangeChain(Buffer buf, BlockNumber newBlkno, bool setNext)
{
    return;
}

void UBTreePCRRecycleQueuePageChangeEndpointLeftPage(Relation rel, Buffer buf, bool isHead)
{
    return;
}

void UBTreePCRRecycleQueuePageChangeEndpointRightPage(Relation rel, Buffer buf, bool isHead)
{
    return;
}

void UBTreePCRXlogRecycleQueueModifyPage(Buffer buf, xl_ubtree2_recycle_queue_modify *xlrec)
{
    return;
}

uint32 UBTreePCRRecycleQueuePageDump(Relation rel, Buffer buf, bool recordEachItem, TupleDesc *tupleDesc, Tuplestorestate *tupstore, uint32 cols)
{
    return 0;
}

void UBTreePCRDumpRecycleQueueFork(Relation rel, UBTRecycleForkNumber forkNum, TupleDesc *tupDesc, Tuplestorestate *tupstore, uint32 cols)
{
    return;
}

void UBTreePCRBuildCallback(Relation index, HeapTuple htup, Datum *values, const bool *isnull, bool tupleIsAlive, void *state)
{
    return;
};