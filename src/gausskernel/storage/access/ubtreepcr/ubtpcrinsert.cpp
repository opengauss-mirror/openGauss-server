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
 * ubtpcrinsert.cpp
 *  DML functions for ubtree pcr page.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/ubtreepcr/ubtpcrinsert.cpp
 *
 * --------------------------------------------------------------------------------------
 */


#include "access/ubtreepcr.h"

bool UBTreePCRDoInsert(Relation rel, IndexTuple itup, IndexUniqueCheck checkUnique, Relation heapRel)
{
    return false;
}

bool UBTreePCRDoDelete(Relation rel, IndexTuple itup, bool isRollbackIndex)
{
    return false;
}

bool UBTreePCRPagePruneOpt(Relation rel, Buffer buf, bool tryDelete, BTStack del_blknos)
{
    return false;
}

bool UBTreePCRPagePrune(Relation rel, Buffer buf, TransactionId oldestXmin, OidRBTree *invisibleParts)
{
    return false;
}

bool UBTreePCRPruneItem(Page page, OffsetNumber offnum, TransactionId oldestXmin, IndexPruneState* prstate, bool isToast)
{
    return false;
}

void UBTreePCRPagePruneExecute(Page page, OffsetNumber* nowdead, int ndead, IndexPruneState* prstate, TransactionId oldest_xmin)
{
    return;
}

void UBTreePCRPageRepairFragmentation(Relation rel, BlockNumber blkno, Page page)
{
    return;
}

void UBTreePCRInsertParent(Relation rel, Buffer buf, Buffer rbuf, BTStack stack, bool is_root, bool is_only)
{
    return;
}

void UBTreePCRFinishSplit(Relation rel, Buffer lbuf, BTStack stack)
{
    return;
}

Buffer UBTreePCRGetStackBuf(Relation rel, BTStack stack)
{
    return NULL;
}

