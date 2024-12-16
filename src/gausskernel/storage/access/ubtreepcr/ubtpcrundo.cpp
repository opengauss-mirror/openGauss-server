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