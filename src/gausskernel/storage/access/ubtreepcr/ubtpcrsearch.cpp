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


BTStack UBTreePCRSearch(Relation rel, BTScanInsert key, Buffer *bufP, int access, bool needStack)
{
    return NULL;
}

OffsetNumber UBTreePCRBinarySearch(Relation rel, BTScanInsert key, Buffer buf, bool fixActiveCount)
{
    return 0;
}

bool UBTreePCRFirst(IndexScanDesc scan, ScanDirection dir)
{
    return false;
}

Buffer UBTreePCRMoveRight(Relation rel, BTScanInsert itup_key, Buffer buf, bool forupdate, BTStack stack, int access)
{
    return NULL;
}

int32 UBTreePCRCompare(Relation rel, BTScanInsert key, Page page, OffsetNumber offnum, Buffer buf)
{
    return 0;
}

bool UBTreeCPRFirst(IndexScanDesc scan, ScanDirection dir)
{
    return false;
}

bool UBTreePCRNext(IndexScanDesc scan, ScanDirection dir)
{
    return false;
}

void UBTreePCRTraceTuple(IndexScanDesc scan, OffsetNumber offnum, bool isVisible, bool isHikey)
{
    return;
}

Buffer UBTreePCRGetEndPoint(Relation rel, uint32 level, bool rightmost)
{
    return false;
}

bool UBTreePCRGetTupleInternal(IndexScanDesc scan, ScanDirection dir)
{
    return false;
}