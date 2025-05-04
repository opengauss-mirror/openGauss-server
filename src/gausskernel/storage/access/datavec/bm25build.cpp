/*
* Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * bm25build.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/bm25build.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <pthread.h>
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "storage/buf/block.h"
#include "utils/memutils.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "utils/builtins.h"
#include "access/datavec/bm25.h"

#define CALLBACK_ITEM_POINTER HeapTuple hup

IndexBuildResult* bm25build_internal(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result;
    return result;
}

void bm25buildempty_internal(Relation index)
{
    IndexBuildResult *result;
}

bool bm25insert_internal(Relation index, Datum *values, ItemPointer heapCtid)
{
    BM25MetaPageData meta;
}