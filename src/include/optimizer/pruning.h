/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * ---------------------------------------------------------------------------------------
 * 
 * pruning.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/optimizer/pruning.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PRUNING_H_
#define PRUNING_H_

#include "nodes/parsenodes.h"
#include "nodes/relation.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "utils/relcache.h"

typedef PartitionMap* (*GetPartitionMapFunc)(Relation relation);

typedef enum PruningType {
    PruningPartition = 0,
    PruningSlice,
} PruningType;

typedef struct PruningContext {
    PruningType pruningType;
    GetPartitionMapFunc GetPartitionMap;

    PlannerInfo* root;
    RangeTblEntry* rte;
    EState* estate;
    Relation relation;

    /* used for slice pruning */
    Index varno;
    ParamListInfo boundParams;
} PruningContext;

typedef enum PartKeyColumnRangeMode {
    PARTKEY_RANGE_MODE_POINT = 0,
    PARTKEY_RANGE_MODE_INCREASE,
    PARTKEY_RANGE_MODE_RANGE,
    PARTKEY_RANGE_MODE_UNION
} PartKeyColumnRangeMode;

typedef enum IndexesUsableType {
    INDEXES_FULL_USABLE = 0,
    INDEXES_PARTIAL_USABLE,
    INDEXES_NONE_USABLE
} IndexesUsableType;

typedef struct PartKeyColumnRange {
    PartKeyColumnRangeMode mode;
    Const* prev;
    Const* next;
} PartKeyColumnRange;

typedef struct PartKeyRange {
    int num;
    PartKeyColumnRange columnRanges[4];
} PartKeyRange;
extern IndexesUsableType eliminate_partition_index_unusable(Oid IndexOid, PruningResult* inputPruningResult,
    PruningResult** indexUsablePruningResult, PruningResult** indexUnusablePruningResult);

void destroyPruningResult(PruningResult* pruningResult);
void partitionPruningFromBoundary(PruningContext *context, PruningResult* pruningResult);
List* restrictInfoListToExprList(List* restrictInfoList);
void generateListFromPruningBM(PruningResult* result, PartitionMap *partmap = NULL);
PruningResult* partitionPruningWalker(Expr* expr, PruningContext* pruningCtx);
PruningResult* partitionPruningForExpr(PlannerInfo* root, RangeTblEntry* rte, Relation rel, Expr* expr);
PruningResult* partitionPruningForRestrictInfo(
    PlannerInfo* root, RangeTblEntry* rte, Relation rel, List* restrictInfoList);
PruningResult* PartitionPruningForPartitionList(RangeTblEntry* rte, Relation rel);
extern PruningResult* copyPruningResult(PruningResult* srcPruningResult);
extern Oid getPartitionOidFromSequence(Relation relation, int partSeq, int partitionno = 0);
extern int varIsInPartitionKey(int attrNo, int2vector* partKeyAttrs, int partKeyNum);
extern bool checkPartitionIndexUnusable(Oid indexOid, int partItrs, PruningResult* pruning_result);

extern PruningResult* GetPartitionInfo(PruningResult* result, EState* estate, Relation current_relation);
static inline PartitionMap* GetPartitionMap(PruningContext *context)
{
    return context->GetPartitionMap(context->relation);
}
extern SubPartitionPruningResult* GetSubPartitionPruningResult(List* selectedSubPartitions, int partSeq,
    int partitionno);
void MergePartitionListsForPruning(RangeTblEntry* rte, Relation rel, PruningResult* pruningRes);
#endif /* PRUNING_H_ */
