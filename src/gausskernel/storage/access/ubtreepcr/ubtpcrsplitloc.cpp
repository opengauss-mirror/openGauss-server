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
 * ubtpcrsplitloc.cpp
 *        Choose split point code for default openGauss btree implementation.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/ubtreepcr/ubtpcrsplitloc.cpp
 *
 * --------------------------------------------------------------------------------------
 */


#include "access/ubtreepcr.h"


OffsetNumber UBTreePCRFindsplitloc(Relation rel, Buffer buf, OffsetNumber newitemoff,
    Size newitemsz, bool* newitemonleft)
{
    return 0;
}

OffsetNumber UBTreePCRFindsplitlocInsertpt(Relation rel, Buffer buf, OffsetNumber newitemoff, Size newitemsz,
    bool *newitemonleft, IndexTuple newitem)
{
    return 0;
}