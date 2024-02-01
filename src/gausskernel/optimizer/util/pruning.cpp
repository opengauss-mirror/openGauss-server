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
 * -------------------------------------------------------------------------
 *
 *  pruning.cpp
 *        data partition
 *
 * IDENTIFICATION
 *        src/gausskernel/optimizer/util/pruning.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/pg_type.h"
#include "catalog/index.h"
#include "commands/tablecmds.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/pruning.h"
#include "optimizer/pruningboundary.h"
#include "optimizer/subpartitionpruning.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/relcache.h"
#include "utils/partitionkey.h"
#include "catalog/pg_partition_fn.h"

static PruningResult* partitionPruningFromBoolExpr(const BoolExpr* expr, PruningContext* context);
static PruningResult* intersectChildPruningResult(const List* resultList, PruningContext* context);
static PruningResult* unionChildPruningResult(const List* resultList, PruningContext* context);
static PruningResult* partitionPruningFromScalarArrayOpExpr
    (const ScalarArrayOpExpr* arrayExpr, PruningContext* pruningCtx);
static PruningResult* recordBoundaryFromOpExpr(const OpExpr* expr, PruningContext* context);
static PruningResult* partitionPruningFromNullTest(NullTest* expr, PruningContext* context);
static void partitionFilter(PruningContext* context, PruningResult* pruningResult);
static bool partitionShouldEliminated(PruningContext* context, int partSeq, PruningBoundary* boundary);
static PartKeyRange* constructPartKeyRange(PruningContext* context, int partSeq);
static PruningBoundary* mergeBoundary(PruningBoundary* leftBoundary, PruningBoundary* rightBoundary);
static void cleanPruningBottom(PruningContext* context, PartitionIdentifier* bottomSeq, Const* value);
static void cleanPruningTop(PruningContext* context, PartitionIdentifier* topSeq, Const* value);
static void destroyPruningResultList(List* resultList);
static Node* EvalExprValueWhenPruning(PruningContext* context, Node* node);
static bool PartitionPruningForPartialListBoundary(ListPartitionMap* listMap, PruningResult* pruningResult,
    Const** keyValues);

static PruningResult* recordEqualFromOpExpr(PartitionType partType,
    const OpExpr* expr, PruningContext* context);
static PruningResult* partitionPruningFromBoolExpr(PartitionType partType,
    const BoolExpr* expr, PruningContext* context);
static PruningResult* partitionPruningFromNullTest(PartitionType partType,
    NullTest* expr, PruningContext* context);
static PruningResult* partitionPruningFromScalarArrayOpExpr(PartitionType partType,
    const ScalarArrayOpExpr* arrayExpr, PruningContext* pruningCtx);
static PruningResult* partitionEqualPruningWalker(PartitionType partType, Expr* expr, PruningContext* pruningCtx);

#define NoNeedPruning(pruningResult)                                              \
    (PruningResultIsFull(pruningResult) || PruningResultIsEmpty(pruningResult) || \
        !PointerIsValid((pruningResult)->boundary))
#define IsCleanPruningBottom(bottomSeqPtr, pruningResult, bottomValue)                     \
    ((pruningResult)->boundary->partitionKeyNum > 1 && PointerIsValid((bottomValue)[0]) && \
        !(pruningResult)->boundary->minClose[0])

#define IsCleanPruningTop(topSeqPtr, pruningResult, topValue)                                         \
    ((pruningResult)->boundary->partitionKeyNum > 1 && (topValue) && PointerIsValid((topValue)[0]) && \
        !(pruningResult)->boundary->maxClose[0])

static PartitionMap* GetRelPartitionMap(Relation relation)
{
    return relation->partMap;
}

static void CollectSubpartitionPruningResults(PruningResult* resPartition, Relation current_relation)
{
    if (!RelationIsSubPartitioned(current_relation)) {
        return;
    }

    int partSeq;
    int partitionno;
    ListCell *cell1 = NULL;
    ListCell *cell2 = NULL;
    Oid partitionOid = InvalidOid;
    Assert(list_length(resPartition->ls_rangeSelectedPartitions) == list_length(resPartition->ls_selectedPartitionnos));
    forboth (cell1, resPartition->ls_rangeSelectedPartitions, cell2, resPartition->ls_selectedPartitionnos) {
        partSeq = lfirst_int(cell1);
        partitionno = lfirst_int(cell2);
        partitionOid = getPartitionOidFromSequence(current_relation, partSeq, partitionno);
        SubPartitionPruningResult *subPartPruningRes =
            PreGetSubPartitionFullPruningResult(current_relation, partitionOid);
        if (subPartPruningRes == NULL) {
            continue;
        }
        subPartPruningRes->partSeq = partSeq;
        subPartPruningRes->partitionno = partitionno;
        resPartition->ls_selectedSubPartitions = lappend(resPartition->ls_selectedSubPartitions, subPartPruningRes);
    }
}

 /*
 * @@GaussDB@@
 * Brief
 * Description	:Get Partition Info from expr when Partition Info not in plan.
 * return value:
 */
PruningResult* GetPartitionInfo(PruningResult* result, EState* estate, Relation current_relation)
{
    PruningResult* resPartition = NULL;
    PruningContext context;

    context.pruningType = PruningPartition;
    context.GetPartitionMap = GetRelPartitionMap;
    context.root = NULL;
    context.rte = NULL;
    context.estate = estate;
    context.relation = current_relation;

    if (current_relation->partMap->type == PART_TYPE_LIST || current_relation->partMap->type == PART_TYPE_HASH) {
        resPartition = partitionEqualPruningWalker(current_relation->partMap->type, result->expr, &context);
    } else {
        resPartition = partitionPruningWalker(result->expr, &context);
    }
    partitionPruningFromBoundary(&context, resPartition);
    if (PruningResultIsFull(resPartition) ||
        (resPartition->bm_rangeSelectedPartitions == NULL && PruningResultIsSubset(resPartition))) {
        destroyPruningResult(resPartition);
        resPartition = getFullPruningResult(current_relation);
        CollectSubpartitionPruningResults(resPartition, current_relation);
        return resPartition;
    }
    if (PointerIsValid(resPartition) && !PruningResultIsFull(resPartition))
        generateListFromPruningBM(resPartition, current_relation->partMap);

    CollectSubpartitionPruningResults(resPartition, current_relation);

    return resPartition;
}

/*
 * @@GaussDB@@
 * Brief
 * Description	:
 * return value:
 */
PruningResult* copyPruningResult(PruningResult* srcPruningResult)
{
    if (srcPruningResult != NULL) {
        PruningResult* newpruningInfo = makeNode(PruningResult);

        newpruningInfo->state = srcPruningResult->state;
        newpruningInfo->bm_rangeSelectedPartitions = bms_copy(srcPruningResult->bm_rangeSelectedPartitions);
        newpruningInfo->intervalOffset = srcPruningResult->intervalOffset;
        newpruningInfo->intervalSelectedPartitions = bms_copy(srcPruningResult->intervalSelectedPartitions);
        newpruningInfo->ls_rangeSelectedPartitions = (List*)copyObject(srcPruningResult->ls_rangeSelectedPartitions);
        newpruningInfo->ls_selectedSubPartitions = (List*)copyObject(srcPruningResult->ls_selectedSubPartitions);
        newpruningInfo->paramArg = (Param *)copyObject(srcPruningResult->paramArg);
        newpruningInfo->expr = (Expr *)copyObject(srcPruningResult->expr);
        newpruningInfo->exprPart = (OpExpr *)copyObject(srcPruningResult->exprPart);
        newpruningInfo->isPbeSinlePartition = srcPruningResult->isPbeSinlePartition;
        newpruningInfo->ls_selectedPartitionnos = (List*)copyObject(srcPruningResult->ls_selectedPartitionnos);

        return newpruningInfo;
    } else {
        return NULL;
    }
}

void generateListFromPruningBM(PruningResult* result, PartitionMap *partmap)
{
    int partitions = 0;
    int i = 0;
    int tmpcheck = 0;
    int partitionno = INVALID_PARTITION_NO;
    Bitmapset* tmpset = NULL;
    result->ls_rangeSelectedPartitions = NULL;
    result->ls_selectedPartitionnos = NULL;

    tmpset = bms_copy(result->bm_rangeSelectedPartitions);
    partitions = bms_num_members(result->bm_rangeSelectedPartitions);

    for (; i < partitions; i++) {
        tmpcheck = bms_first_member(tmpset);
        AssertEreport(-1 != tmpcheck, MOD_OPT, "");
        if (-1 != tmpcheck) {
            result->ls_rangeSelectedPartitions = lappend_int(result->ls_rangeSelectedPartitions, tmpcheck);

            if (PointerIsValid(partmap)) {
                partitionno = GetPartitionnoFromSequence(partmap, tmpcheck);
                PARTITIONNO_VALID_ASSERT(partitionno);
            }
            result->ls_selectedPartitionnos = lappend_int(result->ls_selectedPartitionnos, partitionno);
        }
    }
    bms_free_ext(tmpset);
}

List* restrictInfoListToExprList(List* restrictInfoList)
{
    ListCell* cell = NULL;
    RestrictInfo* iteratorRestrict = NULL;
    List* exprList = NIL;
    Expr* expr = NULL;
    foreach (cell, restrictInfoList) {
        iteratorRestrict = (RestrictInfo*)lfirst(cell);
        if (PointerIsValid(iteratorRestrict->clause)) {
            expr = (Expr*)copyObject(iteratorRestrict->clause);
            exprList = lappend(exprList, expr);
        }
    }
    return exprList;
}

/*
 * @@GaussDB@@
 * Brief
 * Description	: eliminate partitions which don't contain those tuple satisfy expression.
 * return value:  non-eliminated partitions.
 */
PruningResult* partitionPruningForRestrictInfo(
    PlannerInfo* root, RangeTblEntry* rte, Relation rel, List* restrictInfoList)
{
    PruningResult* result = NULL;
    Expr* expr = NULL;

    incre_partmap_refcount(rel->partMap);

    if (0 == list_length(restrictInfoList)) {
        result = getFullPruningResult(rel);
    } else {
        List* exprList = NULL;
        int length = 0;

        exprList = restrictInfoListToExprList(restrictInfoList);

        length = list_length(exprList);
        if (length == 0) {
            result = getFullPruningResult(rel);
        } else if (length == 1) {
            expr = (Expr*)list_nth(exprList, 0);
            result = partitionPruningForExpr(root, rte, rel, expr);
        } else {
            expr = makeBoolExpr(AND_EXPR, exprList, 0);
            result = partitionPruningForExpr(root, rte, rel, expr);
        }
    }

    if (PointerIsValid(result) && !PruningResultIsFull(result)) {
        generateListFromPruningBM(result, rel->partMap);
    }

    if (RelationIsSubPartitioned(rel) && PointerIsValid(result)) {
        Bitmapset *partIdx = NULL;
        List* part_seqs = result->ls_rangeSelectedPartitions;
        List* partitionnos = result->ls_selectedPartitionnos;
        ListCell *cell1 = NULL;
        ListCell *cell2 = NULL;
        RangeTblEntry *partRte = (RangeTblEntry *)copyObject(rte);

        forboth(cell1, part_seqs, cell2, partitionnos)
        {
            int part_seq = lfirst_int(cell1);
            int partitionno = lfirst_int(cell2);
            Oid partOid = getPartitionOidFromSequence(rel, part_seq, partitionno);
            Partition partTable = PartitionOpenWithPartitionno(rel, partOid, partitionno, NoLock);
            Relation partRel = partitionGetRelation(rel, partTable);
            incre_partmap_refcount(partRel->partMap);
            PruningResult *subResult = NULL;
            partRte->relid = partOid;

            if (list_length(restrictInfoList) == 0) {
                subResult = getFullPruningResult(partRel);
            } else {
                subResult = partitionPruningForExpr(root, partRte, partRel, expr);
            }

            if (PointerIsValid(subResult) && !PruningResultIsEmpty(subResult)) {
                generateListFromPruningBM(subResult, partRel->partMap);
                if (bms_num_members(subResult->bm_rangeSelectedPartitions) > 0) {
                    SubPartitionPruningResult *subPruning = makeNode(SubPartitionPruningResult);
                    subPruning->partSeq = part_seq;
                    subPruning->partitionno = partitionno;
                    subPruning->bm_selectedSubPartitions = subResult->bm_rangeSelectedPartitions;
                    subPruning->ls_selectedSubPartitions = subResult->ls_rangeSelectedPartitions;
                    subPruning->ls_selectedSubPartitionnos = subResult->ls_selectedPartitionnos;
                    result->ls_selectedSubPartitions = lappend(result->ls_selectedSubPartitions,
                                                            subPruning);
                    partIdx = bms_add_member(partIdx, part_seq);
                }
            }

            decre_partmap_refcount(partRel->partMap);
            releaseDummyRelation(&partRel);
            partitionClose(rel, partTable, NoLock);
        }

        // adjust
        if (!bms_equal(result->bm_rangeSelectedPartitions, partIdx)) {
            result->bm_rangeSelectedPartitions = partIdx;
            generateListFromPruningBM(result, rel->partMap);
        }
    }

    decre_partmap_refcount(rel->partMap);
    return result;
}

/*
 * @@GaussDB@@
 * Target			: data partition
 * Brief			: select * from partition (partition_name) or select * from partition for (partition_values_list)
 * Description		: eliminate partitions which don't contain those tuple satisfy expression.
 * return value 	: non-eliminated partitions.
 */
PruningResult* singlePartitionPruningForRestrictInfo(Oid partitionOid, Relation rel)
{
    bool find = false;
    int partitionSeq = 0;
    PruningResult* pruningRes = NULL;
    int counter = 0;

    if (!PointerIsValid(rel)) {
        return NULL;
    }

    /* shouldn't happen */
    if (!PointerIsValid(rel->partMap)) {
        ereport(ERROR, (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("relation of oid=\"%u\" is not partitioned table", rel->rd_id)));
    }

    /*
     * only one possible partitionOid is InvalidOid:
     * select * from partitioned_table_name partition for (values);
     * and values pruning interval partition which does not physical exist.
     */
    if (!OidIsValid(partitionOid) && rel->partMap->type != PART_TYPE_INTERVAL) {
        return NULL;
    }

    incre_partmap_refcount(rel->partMap);

    pruningRes = makeNode(PruningResult);
    pruningRes->state = PRUNING_RESULT_SUBSET;

    /* it's a pattitioned table without interval*/
    if (rel->partMap->type == PART_TYPE_RANGE || rel->partMap->type == PART_TYPE_INTERVAL) {
        RangePartitionMap* rangePartMap = NULL;
        rangePartMap = (RangePartitionMap*)rel->partMap;

        for (counter = 0; counter < rangePartMap->rangeElementsNum; counter++) {
            /* the partition is a range partition */
            if ((rangePartMap->rangeElements + counter)->partitionOid == partitionOid) {
                find = true;
                partitionSeq = counter;

                break;
            }
        }
    } else if (PART_TYPE_LIST == rel->partMap->type) {
        ListPartitionMap* listPartMap = NULL;
        listPartMap = (ListPartitionMap*)rel->partMap;

        for (counter = 0; counter < listPartMap->listElementsNum; counter++) {
            /* the partition is a range partition */
            if ((listPartMap->listElements + counter)->partitionOid == partitionOid) {
                find = true;
                partitionSeq = counter;

                break;
            }
        }
    } else if (PART_TYPE_HASH == rel->partMap->type) {
        HashPartitionMap* hashPartMap = NULL;
        hashPartMap = (HashPartitionMap*)rel->partMap;

        for (counter = 0; counter < hashPartMap->hashElementsNum; counter++) {
            /* the partition is a range partition */
            if ((hashPartMap->hashElements + counter)->partitionOid == partitionOid) {
                find = true;
                partitionSeq = counter;

                break;
            }
        }
    } else { /* shouldn't happen */   
        pfree_ext(pruningRes);
        ereport(ERROR, (errmodule(MOD_OPT),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupport partition type")));
    }

    /* In normal condition, it should never happen.
     * But if the Query is from a view/rule contains a partition, this case may happen if the partition is dropped by
     * DDL operation, such as DROP/SPLIT/MERGE/TRUNCATE (UPDATE GLOBAL INDEX)/EXCHANGE (UPDATE GLOBAL INDEX) */
    if (!find) {
        pfree_ext(pruningRes);
        ereport(ERROR,
            (errmodule(MOD_OPT), errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("fail to find partition with oid %u for partitioned table %u", partitionOid, rel->rd_id),
            errdetail("this partition may have already been dropped by DDL operation"),
            errhint("Check if this query contains a view that refrences the target partition. "
                "If so, REBUILD this view.")));
    }

    pruningRes->bm_rangeSelectedPartitions = bms_make_singleton(partitionSeq);

    generateListFromPruningBM(pruningRes, rel->partMap);
    if (RelationIsSubPartitioned(rel)) {
        SubPartitionPruningResult *subPartPruningRes = PreGetSubPartitionFullPruningResult(rel, partitionOid);
        if (subPartPruningRes == NULL) {
            decre_partmap_refcount(rel->partMap);
            return pruningRes;
        }
        subPartPruningRes->partSeq = partitionSeq;
        subPartPruningRes->partitionno = GetPartitionnoFromSequence(rel->partMap, partitionSeq);
        pruningRes->ls_selectedSubPartitions = lappend(pruningRes->ls_selectedSubPartitions, subPartPruningRes);
    }
    decre_partmap_refcount(rel->partMap);
    return pruningRes;
}

/*
 * @@GaussDB@@
 * Target			: data subpartition
 * Brief			: select * from subpartition (subpartition_name)
 * Description		: eliminate subpartitions which don't contain those tuple satisfy expression.
 * return value 	: non-eliminated subpartitions.
 */
PruningResult* SingleSubPartitionPruningForRestrictInfo(Oid subPartitionOid, Relation rel, Oid partOid)
{
    int partitionSeq = 0;
    int subPartitionSeq = 0;
    int partitionno = INVALID_PARTITION_NO;
    int subpartitionno = INVALID_PARTITION_NO;
    PruningResult* pruningRes = NULL;

    if (!PointerIsValid(rel) || !OidIsValid(subPartitionOid) || !OidIsValid(partOid)) {
        return NULL;
    }

    /* shouldn't happen */
    if (!PointerIsValid(rel->partMap)) {
        ereport(ERROR, (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("relation of oid=\"%u\" is not partitioned table", rel->rd_id)));
    }
    pruningRes = makeNode(PruningResult);
    pruningRes->state = PRUNING_RESULT_SUBSET;

    incre_partmap_refcount(rel->partMap);
    partitionSeq = getPartitionElementsIndexByOid(rel, partOid);
    partitionno = GetPartitionnoFromSequence(rel->partMap, partitionSeq);
    /* In normal condition, it should never happen.
     * But if the Query is from a view/rule contains a subpartition, this case may happen if the parent partition of
     * this subpartition is dropped by DDL operation, such as DROP/TRUNCATE (UPDATE GLOBAL INDEX) */
    if (partitionSeq < 0) {
        ereport(ERROR,
            (errmodule(MOD_OPT), errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("fail to find partition with oid %u for partitioned table %u", partOid, rel->rd_id),
            errdetail("this partition may have already been dropped by DDL operation"),
            errhint("Check if this query contains a view that refrences a subpartition owned by the target partition. "
                "If so, REBUILD this view.")));
    }

    Partition part = partitionOpen(rel, partOid, NoLock);
    Relation partRel = partitionGetRelation(rel, part);
    incre_partmap_refcount(partRel->partMap);
    subPartitionSeq = getPartitionElementsIndexByOid(partRel, subPartitionOid);
    subpartitionno = GetPartitionnoFromSequence(partRel->partMap, subPartitionSeq);
    /* In normal condition, it should never happen.
     * But if the Query is from a view/rule contains a subpartition, this case may happen if the subpartition is dropped
     * by DDL operation, such as DROP/SPLIT/MERGE/TRUNCATE (UPDATE GLOBAL INDEX)/EXCHANGE (UPDATE GLOBAL INDEX) */
    if (subPartitionSeq < 0) {
        ereport(ERROR,
            (errmodule(MOD_OPT), errcode(ERRCODE_UNDEFINED_TABLE),
            errmsg("fail to find subpartition with oid %u for partitioned table %u", subPartitionOid, rel->rd_id),
            errdetail("this subpartition may have already been dropped by DDL operation"),
            errhint("Check if this query contains a view that refrences the target subpartition. "
                "If so, REBUILD this view.")));
    }
    decre_partmap_refcount(partRel->partMap);
    releaseDummyRelation(&partRel);
    partitionClose(rel, part, NoLock);

    SubPartitionPruningResult *subPartPruningRes = makeNode(SubPartitionPruningResult);
    subPartPruningRes->bm_selectedSubPartitions =
        bms_add_member(subPartPruningRes->bm_selectedSubPartitions, subPartitionSeq);
    subPartPruningRes->ls_selectedSubPartitions =
        lappend_int(subPartPruningRes->ls_selectedSubPartitions, subPartitionSeq);
    subPartPruningRes->ls_selectedSubPartitionnos =
        lappend_int(subPartPruningRes->ls_selectedSubPartitionnos, subpartitionno);
    subPartPruningRes->partSeq = partitionSeq;
    subPartPruningRes->partitionno = GetPartitionnoFromSequence(rel->partMap, partitionSeq);
    pruningRes->ls_selectedSubPartitions = lappend(pruningRes->ls_selectedSubPartitions, subPartPruningRes);
    pruningRes->ls_rangeSelectedPartitions = lappend_int(pruningRes->ls_rangeSelectedPartitions, partitionSeq);
    pruningRes->ls_selectedPartitionnos = lappend_int(pruningRes->ls_selectedPartitionnos, partitionno);
    pruningRes->bm_rangeSelectedPartitions = bms_make_singleton(partitionSeq);

    decre_partmap_refcount(rel->partMap);
    return pruningRes;
}

/*
 * @@GaussDB@@
 * Brief
 * Description	: eliminate partitions which don't contain those tuple satisfy expression.
 * return value:  non-eliminated partitions.
 */
PruningResult* partitionPruningForExpr(PlannerInfo* root, RangeTblEntry* rte, Relation rel, Expr* expr)
{
    PruningContext* context = NULL;
    PruningResult* result = NULL;

    if (!PointerIsValid(rel) || !PointerIsValid(rte) || !PointerIsValid(expr)) {
        ereport(ERROR, (errmodule(MOD_OPT),
                errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("partitionPruningForExpr: parameter can not be null")));
    }

    context = (PruningContext*)palloc0(sizeof(PruningContext));
    context->root = root;
    context->relation = rel;
    context->estate = NULL;
    context->rte = rte;
    context->GetPartitionMap = GetRelPartitionMap;
    context->pruningType = PruningPartition;

    bool isnull = PartExprKeyIsNull(rel);
    if (isnull) {
        if (rel->partMap != NULL && (rel->partMap->type == PART_TYPE_LIST || rel->partMap->type == PART_TYPE_HASH)) {
            // for List/Hash partitioned table
            result = partitionEqualPruningWalker(rel->partMap->type, expr, context);
        } else {
            // for Range/Interval partitioned table
            result = partitionPruningWalker(expr, context);
        }
    } else {
        result = makeNode(PruningResult);
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;      
    }

    if (result->exprPart != NULL || result->paramArg != NULL) {
        Param* paramArg = (Param *)copyObject(result->paramArg);
        bool isPbeSinlePartition = result->isPbeSinlePartition;
        destroyPruningResult(result);
        result = getFullPruningResult(rel);
        result->expr = expr;
        result->paramArg = paramArg;
        result->isPbeSinlePartition = isPbeSinlePartition;
        return result;
    }
    /* Never happen, just to be self-contained */
    if (!PointerIsValid(result)) {
        ereport(ERROR, (errmodule(MOD_OPT),
                errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("get null for partition pruning")));
    }

    partitionPruningFromBoundary(context, result);
    if (PruningResultIsFull(result)) {
        destroyPruningResult(result);
        result = getFullPruningResult(rel);
    }

    if (PointerIsValid(context)) {
        pfree_ext(context);
    }

    return result;
}

/*
 * @@GaussDB@@
 * Brief
 * Description	: the hook function for expression walker function.
 *                If the expression node is AND,OR or OpExpr(>,>=,=,<=,<), this function would pruning,
 *                else get all partitions of partitioned table.
 * return value:  true if success, else false.
 *                This function will print log, if failed, however fill the PruningReslut with full set of partitions.
 */
PruningResult* partitionPruningWalker(Expr* expr, PruningContext* pruningCtx)
{
    PruningResult* result = NULL;

    switch (nodeTag(expr)) {
        case T_BoolExpr: {
            BoolExpr* boolExpr = NULL;
            boolExpr = (BoolExpr*)expr;
            result = partitionPruningFromBoolExpr(boolExpr, pruningCtx);
        } break;
        case T_OpExpr: {
            OpExpr* opExpr = NULL;
            opExpr = (OpExpr*)expr;
            result = recordBoundaryFromOpExpr(opExpr, pruningCtx);
        } break;
        case T_NullTest: {
            NullTest* ntExpr = NULL;
            ntExpr = (NullTest*)expr;
            result = partitionPruningFromNullTest(ntExpr, pruningCtx);
        } break;
        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* arrayExpr = NULL;
            arrayExpr = (ScalarArrayOpExpr*)expr;
            result = partitionPruningFromScalarArrayOpExpr(arrayExpr, pruningCtx);
        } break;
        case T_Const: {
            Const* cst = (Const*)expr;
            if (BOOLOID == cst->consttype && !cst->constisnull && 0 == cst->constvalue) {
                result = makeNode(PruningResult);
                result->state = PRUNING_RESULT_EMPTY;
            } else {
                result = makeNode(PruningResult);
                result->state = PRUNING_RESULT_FULL;
            }
            result->isPbeSinlePartition = false;
        } break;
        default: {
            result = makeNode(PruningResult);
            result->state = PRUNING_RESULT_FULL;
            result->isPbeSinlePartition = false;
        } break;
    }

    return result;
}

static PruningResult* partitionEqualPruningWalker(PartitionType partType, Expr* expr, PruningContext* pruningCtx)
{
    PruningResult* result = NULL;

    AssertEreport(PointerIsValid(pruningCtx), MOD_OPT, "pruningCtx pointer is NULL.");
    AssertEreport(PointerIsValid(expr), MOD_OPT, "expr pointer is NULL.");

    switch (nodeTag(expr)) {
        case T_BoolExpr: {
            BoolExpr* boolExpr = NULL;

            boolExpr = (BoolExpr*)expr;

            result = partitionPruningFromBoolExpr(partType, boolExpr, pruningCtx);
        } break;
        case T_OpExpr: {
            OpExpr* opExpr = NULL;

            opExpr = (OpExpr*)expr;

            result = recordEqualFromOpExpr(partType, opExpr, pruningCtx);
        } break;
        case T_NullTest: {
            NullTest* ntExpr = NULL;
            ntExpr = (NullTest*)expr;
            result = partitionPruningFromNullTest(partType, ntExpr, pruningCtx);
        } break;
        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* arrayExpr = NULL;
            arrayExpr = (ScalarArrayOpExpr*)expr;
            result = partitionPruningFromScalarArrayOpExpr(partType, arrayExpr, pruningCtx);
        } break;
        case T_Const: {
            Const* cst = (Const*)expr;
            if (BOOLOID == cst->consttype && !cst->constisnull && 0 == cst->constvalue) {
                result = makeNode(PruningResult);
                result->state = PRUNING_RESULT_EMPTY;
            } else {
                result = makeNode(PruningResult);
                result->state = PRUNING_RESULT_FULL;
            }
            result->isPbeSinlePartition = false;
        } break;
        default: {
            result = makeNode(PruningResult);
            result->state = PRUNING_RESULT_FULL;
            result->isPbeSinlePartition = false;
        } break;
    }

    return result;
}

/*
 * @@GaussDB@@
 * Brief
 * Description	: If expression node is AND or OR, this function would be triggered.
 *                It will process left child and right child by call expression_tree_walker().
 * return value:  true if success, else false.
 *                This function will print log, if failed, however fill the PruningReslut with full set of partitions.
 */
static PruningResult* partitionPruningFromBoolExpr(const BoolExpr* expr, PruningContext* context)
{
    Expr* arg = NULL;
    List* resultList = NULL;
    ListCell* cell = NULL;
    PruningResult* result = NULL;
    PruningResult* iterator = NULL;

    if (context != NULL) {
        AssertEreport(PointerIsValid(expr), MOD_OPT, "Pointer expr is NULL.");
    }

    if (expr->boolop == NOT_EXPR) {
        result = makeNode(PruningResult);
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
        return result;
    }

    foreach (cell, expr->args) {
        arg = (Expr*)lfirst(cell);
        iterator = partitionPruningWalker(arg, context);

        if (iterator->paramArg != NULL || iterator->exprPart != NULL) {
            if (expr->boolop == OR_EXPR) {
                iterator->isPbeSinlePartition = false;
            }
            return iterator;
        }
        resultList = lappend(resultList, iterator);
    }

    switch (expr->boolop) {
        case AND_EXPR:
            result = intersectChildPruningResult(resultList, context);
            break;
        case OR_EXPR:
            result = unionChildPruningResult(resultList, context);
            result->isPbeSinlePartition = false;
            break;
        case NOT_EXPR:
        default:
            break;
    }

    destroyPruningResultList(resultList);

    return result;
}

static PruningResult* partitionPruningFromBoolExpr(PartitionType partType, const BoolExpr* expr, 
                                                           PruningContext* context)
{
    Expr* arg = NULL;
    List* resultList = NULL;
    ListCell* cell = NULL;
    PruningResult* result = NULL;
    PruningResult* iterator = NULL;

    AssertEreport(PointerIsValid(expr), MOD_OPT, "Pointer expr is NULL.");
    AssertEreport(PointerIsValid(context), MOD_OPT, "Pointer context is NULL.");
    AssertEreport(PointerIsValid(context->relation), MOD_OPT, "Pointer context->relation is NULL.");

    if (expr->boolop == NOT_EXPR) {
        result = makeNode(PruningResult);
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
        return result;
    }

    foreach (cell, expr->args) {
        arg = (Expr*)lfirst(cell);
        iterator = partitionEqualPruningWalker(partType, arg, context);

        if (iterator->paramArg != NULL || iterator->exprPart != NULL) {
            if (expr->boolop == OR_EXPR) {
                iterator->isPbeSinlePartition = false;
            }
            return iterator;
        }
        resultList = lappend(resultList, iterator);
    }

    switch (expr->boolop) {
        case AND_EXPR:
            result = intersectChildPruningResult(resultList, context);
            break;
        case OR_EXPR:
            result = unionChildPruningResult(resultList, context);
            result->isPbeSinlePartition = false;
            break;
        case NOT_EXPR:
        default:
            break;
    }

    destroyPruningResultList(resultList);

    return result;
}

static PruningResult* partitionPruningFromNullTest(NullTest* expr, PruningContext* context)
{
    PruningResult* result = NULL;
    RangePartitionMap* partMap = NULL;
    int partKeyNum = 0;
    int attrOffset = 0;
    Expr* arg = NULL;
    Var* var = NULL;
    RangeElement range;

    AssertEreport(PointerIsValid(expr), MOD_OPT, "Pointer expr is NULL.");

    arg = expr->arg;

    result = makeNode(PruningResult);

    if (T_RelabelType == nodeTag(arg)) {
        arg = ((RelabelType*)arg)->arg;
    }

    if (T_Var != nodeTag(arg)) {
        result->state = PRUNING_RESULT_FULL;
        return result;
    }

    var = (Var*)arg;

    /* Var's column MUST belongs to parition key columns */
    partMap = (RangePartitionMap*)(GetPartitionMap(context));
    partKeyNum = partMap->base.partitionKey->dim1;

    attrOffset = varIsInPartitionKey(var->varattno, partMap->base.partitionKey, partKeyNum);
    if (attrOffset != 0) {
        result->state = PRUNING_RESULT_FULL;
        return result;
    }

    if (expr->nulltesttype != IS_NULL) {
        result->state = PRUNING_RESULT_FULL;
        return result;
    }

    if (PartitionMapIsInterval(partMap)) {
        result->state = PRUNING_RESULT_EMPTY;
        return result;
    }

    range = partMap->rangeElements[partMap->rangeElementsNum - 1];

    if (!(range.boundary[0]->ismaxvalue)) {
        result->state = PRUNING_RESULT_EMPTY;
        return result;
    }

    result->state = PRUNING_RESULT_SUBSET;
    result->isPbeSinlePartition = true;

    result->bm_rangeSelectedPartitions = bms_make_singleton(partMap->rangeElementsNum - 1);

    return result;
}

static PruningResult* ListPartitionPruningFromIsNotNull(ListPartitionMap* listPartMap, PruningResult* pruning,
    int attrOffset)
{
    if (listPartMap->base.partitionKey->dim1 == 1) {
        pruning->state = PRUNING_RESULT_FULL;
        pruning->isPbeSinlePartition = false;
        return pruning;
    }

    int count;
    for (int i = 0; i < listPartMap->listElementsNum; i++) {
        ListPartElement* part = &listPartMap->listElements[i];
        /* The default partition should be selected, whether single-key or multi-keys partition. */
        if (part->boundary[0].values[0]->ismaxvalue) {
            pruning->bm_rangeSelectedPartitions = bms_add_member(pruning->bm_rangeSelectedPartitions, i);
            continue;
        }
        for (int j = 0; j < part->len; j++) {
            if (!part->boundary[j].values[attrOffset]->constisnull) {
                pruning->bm_rangeSelectedPartitions = bms_add_member(pruning->bm_rangeSelectedPartitions, i);
                break;
            }
        }
    }
    count = bms_num_members(pruning->bm_rangeSelectedPartitions);
    if (count > 0) {
        pruning->state = PRUNING_RESULT_SUBSET;
    } else {
        pruning->state = PRUNING_RESULT_EMPTY;
    }
    pruning->isPbeSinlePartition = (count == 1);
    return pruning;
}

static PruningResult* ListPartitionPruningFromIsNull(ListPartitionMap* listPartMap, PruningResult* pruning,
    int attrOffset)
{
    if (listPartMap->base.partitionKey->dim1 == 1) {
        int defaultPartitionIndex = -1;
        for (int i = 0; i < listPartMap->listElementsNum; i++) {
            ListPartElement *list = &listPartMap->listElements[i];
            if (list->boundary[0].values[0]->ismaxvalue) {
                defaultPartitionIndex = i;
                break;
            }
        }
        if (defaultPartitionIndex >= 0) {
            pruning->state = PRUNING_RESULT_SUBSET;
            pruning->isPbeSinlePartition = true;
            pruning->bm_rangeSelectedPartitions = bms_make_singleton(defaultPartitionIndex);
        } else {
            pruning->state = PRUNING_RESULT_EMPTY;
            pruning->isPbeSinlePartition = false;
        }
        return pruning;
    }
    /*
     * The partition key value of the multi-keys list partition can be NULL.
     * NullTest should be recorded in the boundary for pruning with other OpExpr or NullTest.
     */
    PruningBoundary* boundary = makePruningBoundary(listPartMap->base.partitionKey->dim1);
    Oid typid, typcoll;
    int typmod;

    get_atttypetypmodcoll(listPartMap->base.relOid, listPartMap->base.partitionKey->values[attrOffset],
        &typid, &typmod, &typcoll);
    boundary->minClose[attrOffset] = true;
    boundary->min[attrOffset] = PointerGetDatum(makeNullConst(typid, typmod, typcoll));
    boundary->maxClose[attrOffset] = true;
    boundary->max[attrOffset] = PointerGetDatum(makeNullConst(typid, typmod, typcoll));
    boundary->state = PRUNING_RESULT_SUBSET;
    pruning->boundary = boundary;
    pruning->state = PRUNING_RESULT_SUBSET;
    pruning->isPbeSinlePartition = false;
    return pruning;
}

static PruningResult* partitionPruningFromNullTest(PartitionType partType, NullTest* expr, PruningContext* context)
{
    PruningResult* result = NULL;
    int partKeyNum = 0;
    int attrOffset = 0;
    Expr* arg = NULL;
    Var* var = NULL;

    AssertEreport(PointerIsValid(expr), MOD_OPT, "Pointer expr is NULL.");

    arg = expr->arg;

    result = makeNode(PruningResult);

    if (T_RelabelType == nodeTag(arg)) {
        arg = ((RelabelType*)arg)->arg;
    }

    if (T_Var != nodeTag(arg)) {
        result->state = PRUNING_RESULT_FULL;
        return result;
    }

    var = (Var*)arg;

    /* Var's column MUST belongs to parition key columns */
    if (partType == PART_TYPE_LIST) {
        ListPartitionMap* listPartMap = (ListPartitionMap*)(context->relation->partMap);
        partKeyNum = listPartMap->base.partitionKey->dim1;
        attrOffset = varIsInPartitionKey(var->varattno, listPartMap->base.partitionKey, partKeyNum);
        if (attrOffset < 0 || attrOffset >= partKeyNum) {
            result->state = PRUNING_RESULT_FULL;
            return result;
        }
        if (expr->nulltesttype == IS_NULL) {
            return ListPartitionPruningFromIsNull((ListPartitionMap*)context->relation->partMap, result, attrOffset);
        }
        return ListPartitionPruningFromIsNotNull((ListPartitionMap*)context->relation->partMap, result, attrOffset);
    }
    /* HASH partition */
    HashPartitionMap* hashPartMap = (HashPartitionMap*)(context->relation->partMap);
    partKeyNum = hashPartMap->base.partitionKey->dim1;
    attrOffset = varIsInPartitionKey(var->varattno, hashPartMap->base.partitionKey, partKeyNum);
    if (attrOffset != 0) {
        result->state = PRUNING_RESULT_FULL;
        return result;
    }
    if (expr->nulltesttype != IS_NULL) {
        result->state = PRUNING_RESULT_FULL;
    } else {
        result->state = PRUNING_RESULT_EMPTY;
    }
    result->isPbeSinlePartition = false;

    return result;
}

static PruningResult* intersectChildPruningResult(const List* resultList, PruningContext* context)
{
    PruningResult* iteratorResult = NULL;
    PruningBoundary* tempBoundary = NULL;
    PruningResult* result = NULL;
    Bitmapset* bmsRange = NULL;
    int intervalOffset = -1;
    ListCell* cell = NULL;

    result = makeNode(PruningResult);

    result->state = PRUNING_RESULT_FULL;

    AssertEreport(resultList, MOD_OPT, "");

    foreach (cell, resultList) {
        iteratorResult = (PruningResult*)lfirst(cell);

        AssertEreport(iteratorResult, MOD_OPT, "iteratorResult context is NNULL.");
        if (iteratorResult->state == PRUNING_RESULT_EMPTY) {
            result->state = PRUNING_RESULT_EMPTY;
            result->isPbeSinlePartition = false;
            return result;
        } else if (iteratorResult->state == PRUNING_RESULT_FULL) {
            continue;
        }
        if (PruningResultIsFull(result)) {
            result->boundary = copyBoundary(iteratorResult->boundary);
            result->intervalOffset = iteratorResult->intervalOffset;
            result->bm_rangeSelectedPartitions = bms_copy(iteratorResult->bm_rangeSelectedPartitions);
            result->intervalSelectedPartitions = bms_copy(iteratorResult->intervalSelectedPartitions);
            result->state = iteratorResult->state;
            result->paramArg = (Param *)copyObject(iteratorResult->paramArg);

        } else if (result != NULL) {
            if (intervalOffset == -1 && iteratorResult->intervalOffset >= 0) {
                intervalOffset = iteratorResult->intervalOffset;
            }

            if (intervalOffset >= 0 && iteratorResult->intervalOffset >= 0 &&
                intervalOffset != iteratorResult->intervalOffset) {
                ereport(ERROR, (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        errmsg("For every node in same expression, pruning result's intervalOffset MUST be same")));
            }

            if (result->bm_rangeSelectedPartitions == NULL) {
                result->bm_rangeSelectedPartitions = bms_copy(iteratorResult->bm_rangeSelectedPartitions);
            } else if (iteratorResult->bm_rangeSelectedPartitions != NULL) {
                bmsRange =
                    bms_intersect(result->bm_rangeSelectedPartitions, iteratorResult->bm_rangeSelectedPartitions);
                bms_free_ext(result->bm_rangeSelectedPartitions);
                result->bm_rangeSelectedPartitions = bmsRange;
            }

            if (result->boundary == NULL) {
                result->boundary = copyBoundary(iteratorResult->boundary);
            } else {
                tempBoundary = mergeBoundary(result->boundary, iteratorResult->boundary);
                destroyPruningBoundary(result->boundary);
                result->boundary = tempBoundary;
            }

            if (BoundaryIsEmpty(result->boundary)) {
                result->state = PRUNING_RESULT_EMPTY;
                result->isPbeSinlePartition = false;
                break;
            }

            result->state = PRUNING_RESULT_SUBSET;
        }
        if (result->state != PRUNING_RESULT_EMPTY && iteratorResult->isPbeSinlePartition) {
            result->isPbeSinlePartition = true;
        }
    }

    if (PruningResultIsEmpty(result)) {
        destroyPruningResult(result);
        result = makeNode(PruningResult);
        result->state = PRUNING_RESULT_EMPTY;
        result->isPbeSinlePartition = false;
        result->intervalOffset = -1;
    }

    return result;
}

static PruningResult* unionChildPruningResult(const List* resultList, PruningContext* context)
{
    PruningResult* iteratorResult = NULL;
    PruningResult* result = NULL;
    Bitmapset* bmsRange = NULL;
    Bitmapset* bmsInterval = NULL;
    int intervalOffset = -1;
    ListCell* cell = NULL;

    result = makeNode(PruningResult);

    if (list_length(resultList) == 0) {
        result->state = PRUNING_RESULT_FULL;
        return result;
    }

    foreach (cell, resultList) {
        iteratorResult = (PruningResult*)lfirst(cell);
        if (!PointerIsValid(iteratorResult)) {
            continue;
        }

        partitionPruningFromBoundary(context, iteratorResult);

        if (iteratorResult->state == PRUNING_RESULT_EMPTY) {
            continue;
        } else if (iteratorResult->state == PRUNING_RESULT_FULL || iteratorResult->paramArg != NULL) {
            result->state = PRUNING_RESULT_FULL;
            return result;
        } else {
            if (intervalOffset == -1 && iteratorResult->intervalOffset >= 0) {
                intervalOffset = iteratorResult->intervalOffset;
            }

            if (intervalOffset >= 0 && iteratorResult->intervalOffset >= 0 &&
                intervalOffset != iteratorResult->intervalOffset) {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                        errmsg("For every node in same expression, pruning result's intervalOffset MUST be same")));
            }

            bmsRange = bms_union(result->bm_rangeSelectedPartitions, iteratorResult->bm_rangeSelectedPartitions);
            bms_free_ext(result->bm_rangeSelectedPartitions);
            result->bm_rangeSelectedPartitions = bmsRange;

            if (intervalOffset >= 0) {
                bmsInterval = bms_union(result->intervalSelectedPartitions, iteratorResult->intervalSelectedPartitions);

                bms_free_ext(result->intervalSelectedPartitions);
                result->intervalSelectedPartitions = bmsInterval;
            }
        }
    }

    if (bms_is_empty(result->intervalSelectedPartitions) && bms_is_empty(result->bm_rangeSelectedPartitions)) {
        destroyPruningResult(result);
        result = makeNode(PruningResult);
        result->state = PRUNING_RESULT_EMPTY;
        result->intervalOffset = -1;
    } else {
        result->intervalOffset = intervalOffset;
        result->state = PRUNING_RESULT_SUBSET;
    }
    return result;
}

static PruningResult* partitionPruningFromScalarArrayOpExpr
    (const ScalarArrayOpExpr* arrayExpr, PruningContext* pruningCtx)
{
    OpExpr* expr = NULL;
    Expr* larg = NULL;
    Expr* rarg = NULL;
    List* exprList = NULL;
    PruningResult* result = NULL;
    bool success = false;

    AssertEreport(list_length(arrayExpr->args) == 2, MOD_OPT, "Expected two operands but get exception.");

    larg = (Expr*)list_nth(arrayExpr->args, 0);
    rarg = (Expr*)list_nth(arrayExpr->args, 1);

    if (T_RelabelType == nodeTag(larg)) {
        larg = ((RelabelType*)larg)->arg;
    }

    if (T_Var != nodeTag(larg) || (T_ArrayExpr != nodeTag(rarg) && T_Const != nodeTag(rarg) &&
        T_ArrayCoerceExpr != nodeTag(rarg))) {
        result = makeNode(PruningResult);
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
        return result;
    }

    /* Do not pruning if collation of operator is different from collation of partkey. */
    if (((Var*)larg)->varcollid != arrayExpr->inputcollid) {
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
        return result;
    }

    if (T_ArrayExpr == nodeTag(rarg)) {
        List* eleList = NULL;
        ListCell* element = NULL;

        eleList = ((ArrayExpr*)rarg)->elements;

        foreach (element, eleList) {
            Expr* eleExpr = (Expr*)lfirst(element);
            List* eleArgs = NULL;

            eleArgs = list_make2(copyObject(larg), copyObject(eleExpr));

            expr = (OpExpr*)makeNode(OpExpr);
            expr->args = eleArgs;
            expr->inputcollid = arrayExpr->inputcollid;
            expr->location = 0;
            expr->opcollid = arrayExpr->opfuncid;
            expr->opfuncid = arrayExpr->opfuncid;
            expr->opno = arrayExpr->opno;
            expr->opresulttype = BOOLOID;
            expr->opretset = false;

            exprList = lappend(exprList, expr);
        }

        if (arrayExpr->useOr) {
            result = partitionPruningWalker(
                (Expr*)makeBoolExpr(OR_EXPR, exprList, 0), pruningCtx);
        } else {
            result = partitionPruningWalker(
                (Expr*)makeBoolExpr(AND_EXPR, exprList, 0), pruningCtx);
        }

        success = true;
    } else if (T_Const == nodeTag(rarg)) {
        Const* con = (Const*)rarg;
        Oid eleType = get_element_type(con->consttype);
        if (!OidIsValid(eleType))
            ereport(
                ERROR, (errmodule(MOD_OPT), (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("invalid type. "))));
        int16 typlen = get_typlen(eleType);
        bool typbyval = get_typbyval(eleType);

        /* Cannot be zero for an existing type */
        AssertEreport(typlen, MOD_OPT, "Unexpected 0 type length.");

        if (type_is_array(con->consttype) && PointerIsValid(con->constvalue) &&
            ARR_NDIM((ArrayType*)con->constvalue) == 1) {
            Datum value;
            bool isnull = false;
            Const* eleConst = NULL;
            ArrayType* arrayValue = (ArrayType*)con->constvalue;
            ArrayIterator itr = array_create_iterator(arrayValue, 0);

            while (array_iterate(itr, &value, &isnull)) {
                List* eleArgs = NULL;

                eleConst = makeConst(eleType,
                    con->consttypmod,
                    con->constcollid,
                    typlen,
                    isnull ? PointerGetDatum(NULL) : value,
                    isnull,
                    typbyval);
                eleArgs = list_make2(copyObject(larg), eleConst);

                expr = (OpExpr*)makeNode(OpExpr);
                expr->args = eleArgs;
                expr->inputcollid = arrayExpr->inputcollid;
                expr->location = 0;
                expr->opcollid = arrayExpr->opfuncid;
                expr->opfuncid = arrayExpr->opfuncid;
                expr->opno = arrayExpr->opno;
                expr->opresulttype = BOOLOID;
                expr->opretset = false;

                exprList = lappend(exprList, expr);
            }

            if (arrayExpr->useOr) {
                result = partitionPruningWalker(
                    (Expr*)makeBoolExpr(OR_EXPR, exprList, 0), pruningCtx);
            } else {
                result = partitionPruningWalker(
                    (Expr*)makeBoolExpr(AND_EXPR, exprList, 0), pruningCtx);
            }
            success = true;
        }
    } else if (T_ArrayCoerceExpr == nodeTag(rarg)) {
        List* eleList = NULL;
        ListCell* element = NULL;
        ArrayCoerceExpr* rargExpr = (ArrayCoerceExpr*)rarg;
        if (T_ArrayExpr == nodeTag(rargExpr->arg)) {
            ArrayExpr* rarg_arg = (ArrayExpr*)(rargExpr->arg);
            eleList = rarg_arg->elements;
            foreach (element, eleList) {
                Expr* eleExpr = (Expr*)lfirst(element);
                List* eleArgs = NULL;
                eleArgs = list_make2(copyObject(larg), copyObject(eleExpr));
                expr = (OpExpr*)makeNode(OpExpr);
                expr->args = eleArgs;
                expr->inputcollid = arrayExpr->inputcollid;
                expr->location = 0;
                expr->opcollid = arrayExpr->opfuncid;
                expr->opfuncid = arrayExpr->opfuncid;
                expr->opno = arrayExpr->opno;
                expr->opresulttype = BOOLOID;
                expr->opretset = false;
                exprList = lappend(exprList, expr);
            }
            if (arrayExpr->useOr) {
                result = partitionPruningWalker(
                    (Expr*)makeBoolExpr(OR_EXPR, exprList, 0), pruningCtx);
            } else {
                result = partitionPruningWalker(
                    (Expr*)makeBoolExpr(AND_EXPR, exprList, 0), pruningCtx);
            }
            success = true;
        }
    }

    if (success) {
        return result;
    } else {
        result = makeNode(PruningResult);
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
        return result;
    }
}

static PruningResult* partitionPruningFromScalarArrayOpExpr(PartitionType partType, const ScalarArrayOpExpr* arrayExpr, 
                                                                      PruningContext* pruningCtx)
{
    OpExpr* expr = NULL;
    Expr* larg = NULL;
    Expr* rarg = NULL;
    List* exprList = NULL;
    PruningResult* result = NULL;
    bool success = false;

    AssertEreport(list_length(arrayExpr->args) == 2, MOD_OPT, "Expected two operands but get exception.");

    larg = (Expr*)list_nth(arrayExpr->args, 0);
    rarg = (Expr*)list_nth(arrayExpr->args, 1);

    if (T_RelabelType == nodeTag(larg)) {
        larg = ((RelabelType*)larg)->arg;
    }

    if (T_Var != nodeTag(larg) || (T_ArrayExpr != nodeTag(rarg) && T_Const != nodeTag(rarg))) {
        result = makeNode(PruningResult);
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
        return result;
    }

    /* Do not pruning if collation of operator is different from collation of partkey. */
    if (((Var*)larg)->varcollid != arrayExpr->inputcollid) {
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
        return result;
    }

    if (T_ArrayExpr == nodeTag(rarg)) {
        List* eleList = NULL;
        ListCell* element = NULL;

        eleList = ((ArrayExpr*)rarg)->elements;

        foreach (element, eleList) {
            Expr* eleExpr = (Expr*)lfirst(element);
            List* eleArgs = NULL;

            eleArgs = list_make2(copyObject(larg), copyObject(eleExpr));

            expr = (OpExpr*)makeNode(OpExpr);
            expr->args = eleArgs;
            expr->inputcollid = arrayExpr->inputcollid;
            expr->location = 0;
            expr->opcollid = arrayExpr->opfuncid;
            expr->opfuncid = arrayExpr->opfuncid;
            expr->opno = arrayExpr->opno;
            expr->opresulttype = BOOLOID;
            expr->opretset = false;

            exprList = lappend(exprList, expr);
        }

        if (arrayExpr->useOr) {
            result = partitionEqualPruningWalker(partType, (Expr*)makeBoolExpr(OR_EXPR, exprList, 0), pruningCtx);
        } else {
            result = partitionEqualPruningWalker(partType, (Expr*)makeBoolExpr(AND_EXPR, exprList, 0), pruningCtx);
        }

        success = true;
    } else if (T_Const == nodeTag(rarg)) {
        Const* con = (Const*)rarg;
        Oid eleType = get_element_type(con->consttype);
        if (!OidIsValid(eleType))
            ereport(
                ERROR, (errmodule(MOD_OPT), (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("invalid type. "))));
        int16 typlen = get_typlen(eleType);
        bool typbyval = get_typbyval(eleType);

        /* Cannot be zero for an existing type */
        AssertEreport(typlen, MOD_OPT, "Unexpected 0 type length.");

        if (type_is_array(con->consttype) && PointerIsValid(con->constvalue) &&
            ARR_NDIM((ArrayType*)con->constvalue) == 1) {
            Datum value;
            bool isnull = false;
            Const* eleConst = NULL;
            ArrayType* arrayValue = (ArrayType*)con->constvalue;
            ArrayIterator itr = array_create_iterator(arrayValue, 0);

            while (array_iterate(itr, &value, &isnull)) {
                List* eleArgs = NULL;

                eleConst = makeConst(eleType,
                    con->consttypmod,
                    con->constcollid,
                    typlen,
                    isnull ? PointerGetDatum(NULL) : value,
                    isnull,
                    typbyval);
                eleArgs = list_make2(copyObject(larg), eleConst);

                expr = (OpExpr*)makeNode(OpExpr);
                expr->args = eleArgs;
                expr->inputcollid = arrayExpr->inputcollid;
                expr->location = 0;
                expr->opcollid = arrayExpr->opfuncid;
                expr->opfuncid = arrayExpr->opfuncid;
                expr->opno = arrayExpr->opno;
                expr->opresulttype = BOOLOID;
                expr->opretset = false;

                exprList = lappend(exprList, expr);
            }

            if (arrayExpr->useOr) {
                result = partitionEqualPruningWalker(partType, (Expr*)makeBoolExpr(OR_EXPR, exprList, 0), pruningCtx);
            } else {
                result = partitionEqualPruningWalker(partType, (Expr*)makeBoolExpr(AND_EXPR, exprList, 0), pruningCtx);
            }
            success = true;
        }
    }

    if (success) {
        return result;
    } else {
        result = makeNode(PruningResult);
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
        return result;
    }
}

static Node* EvalExprValueWhenPruning(PruningContext* context, Node* node)
{
    Node* result = NULL;
    switch (context->pruningType) {
        case PruningPartition:
            result = estimate_expression_value(context->root, node, context->estate);
            break;
        case PruningSlice:
            result = eval_const_expressions_params(NULL, node, context->boundParams);
            break;
        default:
            Assert(false);
    }
    
    return result;
}

/*
 * @@GaussDB@@
 * Brief
 * Description	: If expression node is  OpExpr(>,>=,=,<=,<), this function would be triggered.
 *                This function fill PruningReslut's boundary with const from expression.
 * return value:  true if success, else false.
 *                This function will print log, if failed, however fill the PruningReslut with full set of partitions.
 */
static PruningResult* recordBoundaryFromOpExpr(const OpExpr* expr, PruningContext* context)
{
    int partKeyNum = 0;
    int attrOffset = -1;
    char* opName = NULL;
    Expr* leftArg = NULL;
    Expr* rightArg = NULL;

    Const* constArg = NULL;
    Const* constMax = NULL;
    Var* varArg = NULL;
    RangePartitionMap* partMap = NULL;
    PruningBoundary* boundary = NULL;
    PruningResult* result = NULL;
    bool rightArgIsConst = true;
    Node* node = NULL;
    Param* paramArg = NULL;
    OpExpr* exprPart = NULL;

    if (context != NULL) {
        AssertEreport(PointerIsValid(context->relation), MOD_OPT, "Unexpected NULL pointer for context->relation.");
        AssertEreport(PointerIsValid(GetPartitionMap(context)), MOD_OPT, 
                      "Unexpected NULL pointer for context->relation->partMap.");
    }

    result = makeNode(PruningResult);

    /* length of args MUST be 2 */
    if (!PointerIsValid(expr) || list_length(expr->args) != 2 || !PointerIsValid(opName = get_opname(expr->opno))) {
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
        return result;
    }


    leftArg = (Expr*)list_nth(expr->args, 0);
    rightArg = (Expr*)list_nth(expr->args, 1);

    /* In some case, there are several levels of relabel type */
    while (leftArg && IsA(leftArg, RelabelType))
        leftArg = ((RelabelType*)leftArg)->arg;
    while (rightArg && IsA(rightArg, RelabelType))
        rightArg = ((RelabelType*)rightArg)->arg;

    if (leftArg == NULL || rightArg == NULL)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (errmsg("Could not find enough valid args for Boundary From OpExpr"))));

    if (IsA(leftArg, Var)) {
        node = EvalExprValueWhenPruning(context, (Node*)rightArg);
        if (node != NULL) {
            rightArg = (Expr*)node;
        }
    } else if (IsA(rightArg, Var)) {
        node = EvalExprValueWhenPruning(context, (Node*)leftArg);
        if (node != NULL) {
            leftArg = (Expr*)node;
        }
    }

    /* one of args MUST be Const, and another argument Must be Var */
    /* Be const or param, for PBE */
    if (!(((T_Const == nodeTag(leftArg) || T_Param == nodeTag(leftArg)
        || T_OpExpr == nodeTag(leftArg)) && T_Var == nodeTag(rightArg)) ||
        ((T_Const == nodeTag(rightArg) || T_Param == nodeTag(rightArg)
        || T_OpExpr == nodeTag(rightArg)) && T_Var == nodeTag(leftArg)))) {
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
        return result;
    }

    if (T_Var == nodeTag(rightArg)) {
        if (T_Const == nodeTag(leftArg)) {
            constArg = (Const*)leftArg;
        } else if (T_Param == nodeTag(leftArg)) {
            paramArg = (Param*)leftArg;
        } else {
            exprPart = (OpExpr*)leftArg;
        }
        varArg = (Var*)rightArg;
        rightArgIsConst = false;
    } else {
        if (T_Const == nodeTag(rightArg)) {
            constArg = (Const*)rightArg;
        } else if (T_Param == nodeTag(rightArg)) {
            paramArg = (Param*)rightArg;
        } else {
            exprPart = (OpExpr*)rightArg;
        }
        varArg = (Var*)leftArg;
    }

    /* Var MUST represents for current relation */
    if (context->pruningType == PruningPartition) {
        if (context->rte != NULL &&
            context->rte->relid != context->relation->rd_id) {
            result->state = PRUNING_RESULT_FULL;
            result->isPbeSinlePartition = false;
            return result;
        }
    } else {
        /* list/range distributed table slice pruning */
        if (varArg->varlevelsup != 0 ||
            varArg->varno != context->varno ||
            paramArg != NULL ||
            exprPart != NULL) {
            result->state = PRUNING_RESULT_FULL;
            result->isPbeSinlePartition = false;
            return result;
        }
    }

    /* Do not pruning if collation of operator is different from collation of partkey. */
    if (varArg->varcollid != expr->inputcollid) {
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
        return result;
    }

    /* Var's column MUST belongs to parition key columns */
    partMap = (RangePartitionMap*)(GetPartitionMap(context));

    partKeyNum = partMap->base.partitionKey->dim1;
    attrOffset = varIsInPartitionKey(varArg->varattno, partMap->base.partitionKey, partKeyNum);
    if (attrOffset < 0) {
        result->state = PRUNING_RESULT_FULL;
        return result;
    }
 
    if (exprPart != NULL) {
        if (!PartitionMapIsRange(partMap)) {
            result->state = PRUNING_RESULT_FULL;
            result->isPbeSinlePartition = false;
            return result;
        } else {
            result->exprPart = exprPart;
            result->state = PRUNING_RESULT_SUBSET;
            result->isPbeSinlePartition = false;
            return result;
        }
    } else if (paramArg != NULL) {
        if (paramArg->paramkind != PARAM_EXTERN || !PartitionMapIsRange(partMap)) {
            result->state = PRUNING_RESULT_FULL;
            result->isPbeSinlePartition = false;
            return result;
        } else {
            result->paramArg = paramArg;
            result->state = PRUNING_RESULT_SUBSET;
            if (0 == strcmp("=", opName)) {
                result->isPbeSinlePartition = true;
            }
            return result;
        }
    }

    if (constArg->constisnull) {
        result->state = PRUNING_RESULT_EMPTY;
        result->isPbeSinlePartition = false;
        return result;
    }

    /* initialize PruningBoundary */
    result->boundary = makePruningBoundary(partKeyNum);

    boundary = result->boundary;
    result->isPbeSinlePartition = false;

    /* decide the const is the top or bottom of boundary */
    if ((0 == strcmp(">", opName) && rightArgIsConst) || (0 == strcmp("<", opName) && !rightArgIsConst)) {
        if (constArg->constisnull) {
            boundary->minClose[attrOffset] = false;
            boundary->min[attrOffset] = PointerGetDatum(constArg);
            boundary->state = PRUNING_RESULT_EMPTY;
            result->state = PRUNING_RESULT_EMPTY;
        } else {
            boundary->minClose[attrOffset] = false;
            boundary->min[attrOffset] = PointerGetDatum(constArg);
            boundary->state = PRUNING_RESULT_SUBSET;
            result->state = PRUNING_RESULT_SUBSET;
        }
    } else if ((0 == strcmp(">=", opName) && rightArgIsConst) || (0 == strcmp("<=", opName) && !rightArgIsConst)) {
        boundary->minClose[attrOffset] = true;
        boundary->min[attrOffset] = PointerGetDatum(constArg);
        boundary->state = PRUNING_RESULT_SUBSET;
        result->state = PRUNING_RESULT_SUBSET;
    } else if (0 == strcmp("=", opName)) {
        boundary->minClose[attrOffset] = true;
        boundary->min[attrOffset] = PointerGetDatum(constArg);
        boundary->maxClose[attrOffset] = true;

        if (constArg->constisnull) {
            boundary->max[attrOffset] = PointerGetDatum(constMax);
        } else {
            boundary->max[attrOffset] = PointerGetDatum(copyObject((void*)constArg));
        }

        boundary->state = PRUNING_RESULT_SUBSET;
        result->state = PRUNING_RESULT_SUBSET;
        result->isPbeSinlePartition = true;
    } else if ((0 == strcmp("<=", opName) && rightArgIsConst) || (0 == strcmp(">=", opName) && !rightArgIsConst)) {
        boundary->maxClose[attrOffset] = true;
        boundary->max[attrOffset] = PointerGetDatum(constArg);

        if (constArg->constisnull) {
            boundary->state = PRUNING_RESULT_FULL;
            result->state = PRUNING_RESULT_FULL;
        } else {
            boundary->state = PRUNING_RESULT_SUBSET;
            result->state = PRUNING_RESULT_SUBSET;
        }
    } else if ((0 == strcmp("<", opName) && rightArgIsConst) || (0 == strcmp(">", opName) && !rightArgIsConst)) {
        boundary->maxClose[attrOffset] = false;
        boundary->max[attrOffset] = PointerGetDatum(constArg);
        boundary->state = PRUNING_RESULT_SUBSET;
        result->state = PRUNING_RESULT_SUBSET;
    } else {
        boundary->state = PRUNING_RESULT_FULL;
        result->state = PRUNING_RESULT_FULL;
    }

    result->intervalOffset = -1;

    return result;
}

static PruningResult* RecordEqualFromOpExprPart(const PartitionType partType, const PruningContext* context, const char* opName,
                                                       Const* constMax, const Var* varArg, int attrOffset, PruningResult* result, Param* paramArg,
                                                       OpExpr* exprPart, const Const* constArg, PruningBoundary* boundary)
{
    int partKeyNum = 0;

    /* Var's column MUST belongs to parition key columns */
    if (partType == PART_TYPE_LIST) {
        ListPartitionMap *partMap = (ListPartitionMap*)(context->relation->partMap);
        partKeyNum = partMap->base.partitionKey->dim1;
        attrOffset = varIsInPartitionKey(varArg->varattno, partMap->base.partitionKey, partKeyNum);
    } else if (partType == PART_TYPE_HASH) {
        HashPartitionMap *partMap = (HashPartitionMap*)(context->relation->partMap);
        partKeyNum = partMap->base.partitionKey->dim1;
        attrOffset = varIsInPartitionKey(varArg->varattno, partMap->base.partitionKey, partKeyNum);
    }

    if (attrOffset < 0) {
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
        return result;
    }

    if (exprPart != NULL) {
        result->exprPart = exprPart;
        result->state = PRUNING_RESULT_SUBSET;
        result->isPbeSinlePartition = false;
        return result;
    } else if (paramArg != NULL) {
        if (paramArg->paramkind != PARAM_EXTERN) {
            result->state = PRUNING_RESULT_FULL;
            result->isPbeSinlePartition = false;
            return result;
        } else {
            result->paramArg = paramArg;
            result->state = PRUNING_RESULT_SUBSET;
            if (0 == strcmp("=", opName)) {
                result->isPbeSinlePartition = (partKeyNum == 1);
            }
            return result;
        }
    }

    if (constArg->constisnull) {
        result->state = PRUNING_RESULT_EMPTY;
        result->isPbeSinlePartition = false;
        return result;
    }

    /* initialize PruningBoundary */
    result->boundary = makePruningBoundary(partKeyNum);

    boundary = result->boundary;

    /* decide the const is the top or bottom of boundary */
    if (strcmp("=", opName) == 0) {
        boundary->minClose[attrOffset] = true;
        boundary->min[attrOffset] = PointerGetDatum(constArg);
        boundary->maxClose[attrOffset] = true;

        if (constArg->constisnull) {
            boundary->max[attrOffset] = PointerGetDatum(constMax);
        } else {
            boundary->max[attrOffset] = PointerGetDatum(copyObject((void*)constArg));
        }

        boundary->state = PRUNING_RESULT_SUBSET;
        result->state = PRUNING_RESULT_SUBSET;
        result->isPbeSinlePartition = (partKeyNum == 1);
    } else {
        boundary->state = PRUNING_RESULT_FULL;
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
    }

    result->intervalOffset = -1;

    return result;

}

static PruningResult* recordEqualFromOpExpr(PartitionType partType, const OpExpr* expr, PruningContext* context)
{
    int attrOffset = -1;
    char* opName = NULL;
    Expr* leftArg = NULL;
    Expr* rightArg = NULL;
    Const* constArg = NULL;
    Const* constMax = NULL;
    Var* varArg = NULL;
    PruningBoundary* boundary = NULL;
    PruningResult* result = NULL;
    bool rightArgIsConst = true;
    Node* node = NULL;
    Param* paramArg = NULL;
    OpExpr* exprPart = NULL;

    AssertEreport(PointerIsValid(context), MOD_OPT, "Unexpected NULL pointer for context.");
    AssertEreport(PointerIsValid(context->relation), MOD_OPT, "Unexpected NULL pointer for context->relation.");
    AssertEreport(
        PointerIsValid(context->relation->partMap), MOD_OPT, "Unexpected NULL pointer for context->relation->partMap.");

    result = makeNode(PruningResult);

    /* length of args MUST be 2 */
    if (!PointerIsValid(expr) || list_length(expr->args) != 2 || !PointerIsValid(opName = get_opname(expr->opno))) {
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
        return result;
    }

    leftArg = (Expr*)list_nth(expr->args, 0);
    rightArg = (Expr*)list_nth(expr->args, 1);

    /* In some case, there are several levels of relabel type */
    while (leftArg && IsA(leftArg, RelabelType))
        leftArg = ((RelabelType*)leftArg)->arg;
    while (rightArg && IsA(rightArg, RelabelType))
        rightArg = ((RelabelType*)rightArg)->arg;

    if (leftArg == NULL || rightArg == NULL)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (errmsg("Could not find enough valid args for Boundary From OpExpr"))));

    if (IsA(leftArg, Var)) {
        node = EvalExprValueWhenPruning(context, (Node*)rightArg);
        if (node != NULL) {
            rightArg = (Expr*)node;
        }
    } else if (IsA(rightArg, Var)) {
        node = EvalExprValueWhenPruning(context, (Node*)leftArg);
        if (node != NULL) {
            leftArg = (Expr*)node;
        }
    }

    /* one of args MUST be Const, and another argument Must be Var */
    if (!(((T_Const == nodeTag(leftArg) || T_Param == nodeTag(leftArg)
        || T_OpExpr == nodeTag(leftArg)) && T_Var == nodeTag(rightArg)) ||
        ((T_Const == nodeTag(rightArg) || T_Param == nodeTag(rightArg)
        || T_OpExpr == nodeTag(rightArg)) && T_Var == nodeTag(leftArg)))) {
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
        return result;
    }

    if (T_Var == nodeTag(rightArg)) {
        if (T_Const == nodeTag(leftArg)) {
            constArg = (Const*)leftArg;
        } else if (T_Param == nodeTag(leftArg)) {
            paramArg = (Param*)leftArg;
        } else {
            exprPart = (OpExpr*)leftArg;
        }
        varArg = (Var*)rightArg;
        rightArgIsConst = false;
    } else {
        if (T_Const == nodeTag(rightArg)) {
            constArg = (Const*)rightArg;
        } else if (T_Param == nodeTag(rightArg)) {
            paramArg = (Param*)rightArg;
        } else {
            exprPart = (OpExpr*)rightArg;
        }
        varArg = (Var*)leftArg;
    }

    /* Var MUST represents for current relation */
    if (context->pruningType == PruningPartition) {
        if (context->rte != NULL &&
            context->rte->relid != context->relation->rd_id) {
            result->state = PRUNING_RESULT_FULL;
            result->isPbeSinlePartition = false;
            return result;
        }
    } else {
        /* list/range distributed table slice pruning */
        if (varArg->varlevelsup != 0 || varArg->varno != context->varno) {
            result->state = PRUNING_RESULT_FULL;
            result->isPbeSinlePartition = false;
            return result;
        }
    }

    /* Do not pruning if collation of operator is different from collation of partkey. */
    if (varArg->varcollid != expr->inputcollid) {
        result->state = PRUNING_RESULT_FULL;
        result->isPbeSinlePartition = false;
        return result;
    }

    PruningResult* res = RecordEqualFromOpExprPart(partType, context, opName, constMax, varArg, attrOffset, result, paramArg, exprPart, constArg, boundary);
    return res;
}

int varIsInPartitionKey(int attrNo, int2vector* partKeyAttrs, int partKeyNum)
{
    int i = 0;
    int nKeyColumn = 0;
    char* cAttNums = NULL;
    int16* attNums = NULL;

    if (!PointerIsValid(partKeyAttrs) || attrNo <= 0) {
        return -1;
    }

    nKeyColumn = ARR_DIMS(partKeyAttrs)[0];

    AssertEreport(nKeyColumn == partKeyNum, MOD_OPT, "Partition key deminsion inconsistent.");

    cAttNums = (char*)ARR_DATA_PTR(partKeyAttrs);

    attNums = (int16*)cAttNums;

    for (i = 0; i < nKeyColumn; i++) {
        if (attNums[i] == attrNo) {
            return i;
        }
    }

    return -1;
}

/*
 * @@GaussDB@@
 * Brief
 * Description	: Get eliminated partitions according to boundary of PruningResult.
 *                Fill PruningResult bitmap attribute with bitmap for eliminated partitions
 * return value:  true if success, else false.
 *                if failed, This function will print log.
 */
int getRangeEnd(PruningContext* context)
{
    int rangeEnd = -1;
    PartitionMap* partMap = GetPartitionMap(context);
    if (partMap->type == PART_TYPE_LIST) {
        rangeEnd = ((ListPartitionMap*)partMap)->listElementsNum - 1;
    } else if (partMap->type == PART_TYPE_HASH) {
        rangeEnd = ((HashPartitionMap*)partMap)->hashElementsNum - 1;
    } else {
        rangeEnd = ((RangePartitionMap*)partMap)->rangeElementsNum - 1;
    }
    return rangeEnd;
}

void partitionPruningFromBoundary(PruningContext *context, PruningResult* pruningResult)
{
    Const** bottomValue = NULL;
    Const** topValue = NULL;
    Bitmapset* rangeBms = NULL;
    int rangeStart = -1;
    int rangeEnd = -1;
    int i = 0;
    int compare = 0;
    bool isTopClosed = true;

    AssertEreport(PointerIsValid(context), MOD_OPT, "Unexpected NULL pointer for context.");
    AssertEreport(PointerIsValid(pruningResult), MOD_OPT, "Unexpected NULL pointer for pruningResult.");

    if (NoNeedPruning(pruningResult)) {
        return;
    }

    bottomValue = (Const**)pruningResult->boundary->min;
    topValue = (Const**)pruningResult->boundary->max;

    for (i = 0; i < pruningResult->boundary->partitionKeyNum; i++) {
        if (!PointerIsValid(topValue[i])) {
            topValue[i] = makeMaxConst(InvalidOid, -1, InvalidOid);
        }

        if (!(pruningResult->boundary->maxClose[i])) {
            isTopClosed = false;
        }
    }

    compare = partitonKeyCompare(bottomValue, topValue, pruningResult->boundary->partitionKeyNum,
        ((GetPartitionMap(context))->type == PART_TYPE_LIST));
    if (compare > 0) {
        pruningResult->state = PRUNING_RESULT_EMPTY;
        return;
    }

    // compare the bottom and the intervalMax, if the bottom is large than or equal than intervalMax, pruning result is
    // empty.
    if ((context->pruningType == PruningPartition) && (
        (GetPartitionMap(context))->type == PART_TYPE_LIST ||
        (GetPartitionMap(context))->type == PART_TYPE_HASH)) {
        /* If the boundary does not contain all key values, prune in PartitionPruningForPartialListBoundary. */
        if ((GetPartitionMap(context))->type == PART_TYPE_LIST && PartitionPruningForPartialListBoundary(
            (ListPartitionMap*)GetPartitionMap(context), pruningResult, bottomValue)) {
            return;
        }
        partitionRoutingForValueEqual(
            context->relation, bottomValue, pruningResult->boundary->partitionKeyNum, true, u_sess->opt_cxt.bottom_seq);
        u_sess->opt_cxt.top_seq->partArea = u_sess->opt_cxt.bottom_seq->partArea;
        u_sess->opt_cxt.top_seq->partitionId =  u_sess->opt_cxt.bottom_seq->partitionId;
        u_sess->opt_cxt.top_seq->fileExist = u_sess->opt_cxt.bottom_seq->fileExist;
        u_sess->opt_cxt.top_seq->partSeq = u_sess->opt_cxt.bottom_seq->partSeq;
    } else {
        partitionRoutingForValueRange(
            context, bottomValue, pruningResult->boundary->partitionKeyNum, true, true, u_sess->opt_cxt.bottom_seq);
        if (IsCleanPruningBottom(u_sess->opt_cxt.bottom_seq, pruningResult, bottomValue)) {
            cleanPruningBottom(context, u_sess->opt_cxt.bottom_seq, bottomValue[0]);
        }

        partitionRoutingForValueRange(
            context, topValue, pruningResult->boundary->partitionKeyNum, isTopClosed, false, u_sess->opt_cxt.top_seq);
        if (IsCleanPruningTop(u_sess->opt_cxt.top_seq, pruningResult, topValue)) {
            cleanPruningTop(context, u_sess->opt_cxt.top_seq, topValue[0]);
        }
    }
    
    rangeEnd = getRangeEnd(context);

    if (!PartitionLogicalExist(u_sess->opt_cxt.bottom_seq) && !PartitionLogicalExist(u_sess->opt_cxt.top_seq)) {
        /* pruning failed or result contains all partition */
        pruningResult->state = PRUNING_RESULT_EMPTY;
    } else if (!PartitionLogicalExist(u_sess->opt_cxt.bottom_seq)) {
        rangeStart = 0;
        rangeEnd = u_sess->opt_cxt.top_seq->partSeq;
    } else if (!PartitionLogicalExist(u_sess->opt_cxt.top_seq)) {
        rangeStart = u_sess->opt_cxt.bottom_seq->partSeq;
    } else {
        rangeStart = u_sess->opt_cxt.bottom_seq->partSeq;
        rangeEnd = u_sess->opt_cxt.top_seq->partSeq;
    }

    if (0 <= rangeStart) {
        for (i = rangeStart; i <= rangeEnd; i++) {
            rangeBms = bms_add_member(rangeBms, i);
        }

        if (PointerIsValid(pruningResult->bm_rangeSelectedPartitions)) {
            Bitmapset* tempBms = NULL;
            tempBms = bms_intersect(pruningResult->bm_rangeSelectedPartitions, rangeBms);
            bms_free_ext(pruningResult->bm_rangeSelectedPartitions);
            bms_free_ext(rangeBms);
            pruningResult->bm_rangeSelectedPartitions = tempBms;
        } else {
            pruningResult->bm_rangeSelectedPartitions = rangeBms;
        }
    }
    partitionFilter(context, pruningResult);

    if (pruningResult->boundary) {
        destroyPruningBoundary(pruningResult->boundary);
        pruningResult->boundary = NULL;
    }
}

/************************************************************************************
 *        pruning for multi-column partition key
 ************************************************************************************/
static void partitionFilter(PruningContext* context, PruningResult* pruningResult)
{
    Bitmapset* resourceBms = NULL;
    Bitmapset* result = NULL;

    if (context->pruningType == PruningPartition) {
        /* partition pruning */
        if (!RELATION_IS_PARTITIONED(context->relation) || !PartitionMapIsRange(GetPartitionMap(context)) ||
            pruningResult->boundary->partitionKeyNum == 1) {
            return;
        }
    } else {
        /* slice pruning */
        if (!PartitionMapIsRange(GetPartitionMap(context)) ||
            pruningResult->boundary->partitionKeyNum == 1) {
            return;
        }
    }
    

    if (bms_is_empty(pruningResult->bm_rangeSelectedPartitions)) {
        return;
    }

    resourceBms = bms_copy(pruningResult->bm_rangeSelectedPartitions);

    while (true) {
        int curPart;

        curPart = bms_first_member(resourceBms);
        if (curPart < 0) {
            break;
        }

        if (!partitionShouldEliminated(context, curPart, pruningResult->boundary)) {
            result = bms_add_member(result, curPart);
        }
    }

    bms_free_ext(pruningResult->bm_rangeSelectedPartitions);

    if (bms_is_empty(result)) {
        pruningResult->bm_rangeSelectedPartitions = NULL;
    } else {
        pruningResult->bm_rangeSelectedPartitions = result;
    }

    bms_free_ext(resourceBms);
}

#define CONSTISMIN(value) ((value) == NULL || (value)->constisnull)
#define CONSTISMAX(value) ((value) != NULL && (value)->ismaxvalue)

static bool partitionShouldEliminated(PruningContext* context, int partSeq, PruningBoundary* boundary)
{
    int i = 0;
    PartKeyRange* partRange = NULL;
    bool isSelected = true;

    partRange = constructPartKeyRange(context, partSeq);
    if (!PointerIsValid(partRange)) {
        return false;
    }

    for (i = 0; i < partRange->num && isSelected; i++) {
        PartKeyColumnRange* columnRange = &(partRange->columnRanges[i]);
        int prevCompareMin = 0;
        int prevCompareMax = 0;
        int nextCompareMin = 0;
        int nextCompareMax = 0;
        bool frontSectionIntersected = false;
        bool behindSectionIntersected = false;

        switch (columnRange->mode) {
            case PARTKEY_RANGE_MODE_POINT:
                AssertEreport(0 == partitonKeyCompare(&(columnRange->prev), &(columnRange->next), 1) &&
                                  !CONSTISMIN(columnRange->prev) && !CONSTISMAX(columnRange->prev),
                    MOD_OPT,
                    "");

                prevCompareMin = partitonKeyCompare(&(columnRange->prev), ((Const**)(boundary->min + i)), 1);
                prevCompareMax = partitonKeyCompare(&(columnRange->prev), ((Const**)(boundary->max + i)), 1);
                if ((prevCompareMin > 0 && prevCompareMax < 0) || (prevCompareMin == 0 && boundary->minClose[i]) ||
                    (prevCompareMax == 0 && boundary->maxClose[i])) {
                    isSelected = true;
                } else {
                    isSelected = false;
                }

                break;

            case PARTKEY_RANGE_MODE_INCREASE:
            case PARTKEY_RANGE_MODE_RANGE:
                if ((CONSTISMIN(columnRange->prev) && CONSTISMIN((Const*)(boundary->min[i]))) ||
                    (CONSTISMAX(columnRange->next) && CONSTISMAX((Const*)(boundary->max[i])))) {
                    isSelected = true;
                    break;
                }

                prevCompareMin = partitonKeyCompare(&(columnRange->prev), ((Const**)(boundary->min + i)), 1);
                prevCompareMax = partitonKeyCompare(&(columnRange->prev), ((Const**)(boundary->max + i)), 1);
                nextCompareMin = partitonKeyCompare(&(columnRange->next), ((Const**)(boundary->min + i)), 1);
                nextCompareMax = partitonKeyCompare(&(columnRange->next), ((Const**)(boundary->max + i)), 1);
                if ((prevCompareMin > 0 && prevCompareMax < 0) || (prevCompareMin < 0 && nextCompareMin > 0) ||
                    (prevCompareMax == 0 && boundary->maxClose[i]) || (nextCompareMin == 0 && boundary->minClose[i]) ||
                    (prevCompareMin == 0) || (nextCompareMax == 0)) {
                    isSelected = true;
                } else {
                    isSelected = false;
                }

                break;

            case PARTKEY_RANGE_MODE_UNION:
                AssertEreport(!CONSTISMIN(columnRange->prev) && !CONSTISMAX(columnRange->prev) &&
                                  !CONSTISMIN(columnRange->next) && !CONSTISMAX(columnRange->next),
                    MOD_OPT,
                    "");

                if (CONSTISMIN((Const*)(boundary->min[i])) || CONSTISMAX((Const*)(boundary->max[i]))) {
                    isSelected = true;
                    break;
                }

                nextCompareMin = partitonKeyCompare(&(columnRange->next), ((Const**)(boundary->min + i)), 1);
                if (nextCompareMin > 0) {
                    frontSectionIntersected = true;
                }

                if (!frontSectionIntersected) {
                    prevCompareMax = partitonKeyCompare(&(columnRange->prev), ((Const**)(boundary->max + i)), 1);
                    if ((prevCompareMax < 0) || (prevCompareMax == 0 && boundary->maxClose[i])) {
                        behindSectionIntersected = true;
                    }
                }

                if (frontSectionIntersected || behindSectionIntersected) {
                    isSelected = true;
                } else {
                    isSelected = false;
                }

                break;

            default:
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                            errmsg("unsupported partition key column range mode"))));
                break;
        }
    }

    pfree_ext(partRange);

    return !isSelected;
}

static PartKeyRange* constructPartKeyRange(PruningContext* context, int partSeq)
{
    RangeElement* prePartition = NULL;
    RangeElement* nextPartition = NULL;
    RangePartitionMap* partMap = NULL;
    PartKeyRange* result = NULL;
    Const* left = NULL;
    Const* right = NULL;
    int i = 0;
    int partKeyNum = 0;

    result = (PartKeyRange*)palloc0(sizeof(PartKeyRange));

    partMap = (RangePartitionMap*)GetPartitionMap(context);
    partKeyNum = partMap->base.partitionKey->dim1;

    nextPartition = &(partMap->rangeElements[partSeq]);

    if (partSeq == 0) {
        result->num = 1;
        result->columnRanges[0].mode = PARTKEY_RANGE_MODE_RANGE;
        result->columnRanges[0].prev = NULL;
        result->columnRanges[0].next = nextPartition->boundary[0];

        return result;
    }

    prePartition = &(partMap->rangeElements[partSeq - 1]);

    for (i = 0; i < partKeyNum; i++) {
        bool typeIsNumerable = false;
        PartKeyColumnRange* columnRange = NULL;
        int compare = 0;

        left = prePartition->boundary[i];
        right = nextPartition->boundary[i];

        if (left->consttype == INT2OID || left->consttype == INT4OID || left->consttype == INT8OID) {
            typeIsNumerable = true;
        } else {
            typeIsNumerable = false;
        }

        compare = partitonKeyCompare(&left, &right, 1);

        AssertEreport(compare <= 0, MOD_OPT, "");

        columnRange = &(result->columnRanges[i]);

        if (compare == 0) {
            columnRange->mode = PARTKEY_RANGE_MODE_POINT;
            columnRange->next = right;
            columnRange->prev = left;
            continue;
        } else {
            if (typeIsNumerable && !CONSTISMIN(left) && !CONSTISMAX(right) && right != NULL) {
                int8 leftInt = (int8)left->constvalue;
                int8 rightInt = (int8)right->constvalue;

                if (leftInt + 1 == rightInt) {
                    columnRange->mode = PARTKEY_RANGE_MODE_INCREASE;
                    columnRange->prev = left;
                    columnRange->next = right;
                    result->num = i + 1;
                    if (i + 1 >= partKeyNum) {
                        break;
                    }

                    left = *(prePartition->boundary + i + 1);
                    right = *(nextPartition->boundary + i + 1);

                    columnRange = result->columnRanges + i + 1;
                    if (left->ismaxvalue) {
                        result->columnRanges[i].mode = PARTKEY_RANGE_MODE_POINT;
                        result->columnRanges[i].prev = result->columnRanges[i].next;

                        columnRange->mode = PARTKEY_RANGE_MODE_RANGE;
                        columnRange->prev = NULL;
                        columnRange->next = right;
                        result->num++;
                    } else if (partitonKeyCompare(&left, &right, 1) > 0) {
                        columnRange->mode = PARTKEY_RANGE_MODE_UNION;
                        columnRange->prev = left;
                        columnRange->next = right;
                        result->num++;
                    }
                    break;
                } else {
                    columnRange->mode = PARTKEY_RANGE_MODE_RANGE;
                    columnRange->prev = left;
                    columnRange->next = right;
                    result->num = i + 1;
                    break;
                }
            } else {
                columnRange->mode = PARTKEY_RANGE_MODE_RANGE;
                columnRange->prev = left;
                columnRange->next = right;
                result->num = i + 1;
                break;
            }
        }
    }

    return result;
}

static PruningBoundary* mergeBoundary(PruningBoundary* leftBoundary, PruningBoundary* rightBoundary)
{
    int i = 0;
    PruningBoundary* result = NULL;
    Const* leftValue = NULL;
    Const* rightValue = NULL;

    if (!PointerIsValid(leftBoundary) && !PointerIsValid(rightBoundary)) {
        return NULL;
    }

    if (!PointerIsValid(leftBoundary)) {
        return copyBoundary(rightBoundary);
    }

    if (!PointerIsValid(rightBoundary)) {
        return copyBoundary(leftBoundary);
    }

    AssertEreport(leftBoundary->partitionKeyNum == rightBoundary->partitionKeyNum,
        MOD_OPT,
        "Expected two boundaries to have same number of partition key, run into exception.");

    if (BoundaryIsFull(leftBoundary) || BoundaryIsEmpty(rightBoundary)) {
        return copyBoundary(rightBoundary);
    }

    if (BoundaryIsFull(rightBoundary) || BoundaryIsEmpty(leftBoundary)) {
        return copyBoundary(leftBoundary);
    }

    result = makePruningBoundary(leftBoundary->partitionKeyNum);

    if (BoundaryIsEmpty(leftBoundary) || BoundaryIsEmpty(rightBoundary)) {
        result->state = PRUNING_RESULT_EMPTY;
        return result;
    }

    result->state = PRUNING_RESULT_SUBSET;

    for (; i < result->partitionKeyNum; i++) {
        int compare = 0;

        /* merge bottom value */
        leftValue = (Const*)DatumGetPointer(leftBoundary->min[i]);
        rightValue = (Const*)DatumGetPointer(rightBoundary->min[i]);
        if (!PointerIsValid(rightValue) && PointerIsValid(leftValue)) {
            result->min[i] = PointerGetDatum(copyObject((void*)leftValue));
            result->minClose[i] = leftBoundary->minClose[i];
        } else if (!PointerIsValid(leftValue) && PointerIsValid(rightValue)) {
            result->min[i] = PointerGetDatum(copyObject((void*)rightValue));
            result->minClose[i] = rightBoundary->minClose[i];
        } else if (PointerIsValid(leftValue) && PointerIsValid(rightValue)) {
            compare = partitonKeyCompare(&leftValue, &rightValue, 1, true);
            if (compare > 0) {
                result->min[i] = PointerGetDatum(copyObject((void*)leftValue));
                result->minClose[i] = leftBoundary->minClose[i];
            } else if (compare < 0) {
                result->min[i] = PointerGetDatum(copyObject((void*)rightValue));
                result->minClose[i] = rightBoundary->minClose[i];
            } else {
                result->min[i] = PointerGetDatum(copyObject((void*)leftValue));
                if (!leftBoundary->minClose[i] || !rightBoundary->minClose[i]) {
                    result->minClose[i] = false;
                } else {
                    result->minClose[i] = true;
                }
            }
        }

        /* merge top value */
        leftValue = (Const*)DatumGetPointer(leftBoundary->max[i]);
        rightValue = (Const*)DatumGetPointer(rightBoundary->max[i]);
        if (!PointerIsValid(rightValue) && PointerIsValid(leftValue)) {
            result->max[i] = PointerGetDatum(copyObject((void*)leftValue));
            result->maxClose[i] = leftBoundary->maxClose[i];
        } else if (!PointerIsValid(leftValue) && PointerIsValid(rightValue)) {
            result->max[i] = PointerGetDatum(copyObject((void*)rightValue));
            result->maxClose[i] = rightBoundary->maxClose[i];
        } else if (PointerIsValid(leftValue) && PointerIsValid(rightValue)) {
            compare = partitonKeyCompare(&leftValue, &rightValue, 1, true);
            if (compare > 0) {
                result->max[i] = PointerGetDatum(copyObject((void*)rightValue));
                result->maxClose[i] = rightBoundary->maxClose[i];
            } else if (compare < 0) {
                result->max[i] = PointerGetDatum(copyObject((void*)leftValue));
                result->maxClose[i] = leftBoundary->maxClose[i];
            } else {
                result->max[i] = PointerGetDatum(copyObject((void*)leftValue));
                if (!leftBoundary->maxClose[i] || !rightBoundary->maxClose[i]) {
                    result->maxClose[i] = false;
                } else {
                    result->maxClose[i] = true;
                }
            }
        }

        leftValue = (Const*)DatumGetPointer(result->min[i]);
        rightValue = (Const*)DatumGetPointer(result->max[i]);
        if (leftValue != NULL && rightValue != NULL) {
            compare = partitonKeyCompare(&leftValue, &rightValue, 1, true);
            if (compare > 0 || (compare == 0 && !(result->minClose[i] && result->maxClose[i]))) {
                result->state = PRUNING_RESULT_EMPTY;
                break;
            }
        }
    }

    return result;
}

static void cleanPruningBottom(PruningContext *context, PartitionIdentifier* bottomSeq, Const* value)
{
    int i = 0;
    RangePartitionMap* partMap = NULL;

    if (bottomSeq->partSeq < 0 || bottomSeq->partSeq >= ((RangePartitionMap*)GetPartitionMap(context))->rangeElementsNum ||
        value == NULL) {
        return;
    }

    incre_partmap_refcount(GetPartitionMap(context));
    partMap = (RangePartitionMap*)(GetPartitionMap(context));

    for (i = bottomSeq->partSeq; i < partMap->rangeElementsNum; i++) {
        RangeElement* range = partMap->rangeElements + i;
        int compare = 0;

        compare = partitonKeyCompare(range->boundary, &value, 1);
        if (compare <= 0) {
            continue;
        } else {
            break;
        }
    }

    if (i >= partMap->rangeElementsNum) {
        i = -1;
    }

    bottomSeq->partSeq = i;
    decre_partmap_refcount(GetPartitionMap(context));
}

static void cleanPruningTop(PruningContext *context, PartitionIdentifier* topSeq, Const* value)
{
    int i = 0;
    RangePartitionMap* partMap = NULL;

    if (topSeq->partSeq < 0 || topSeq->partSeq >= ((RangePartitionMap*)GetPartitionMap(context))->rangeElementsNum ||
        value == NULL) {
        return;
    }

    incre_partmap_refcount(GetPartitionMap(context));
    partMap = (RangePartitionMap*)(GetPartitionMap(context));

    for (i = topSeq->partSeq; i >= 0; i--) {
        RangeElement* range = partMap->rangeElements + i;
        int compare = 0;

        compare = partitonKeyCompare(range->boundary, &value, 1);
        if (compare >= 0) {
            continue;
        } else {
            break;
        }
    }

    if (i < topSeq->partSeq) {
        i++;
    }

    topSeq->partSeq = i;
    decre_partmap_refcount(GetPartitionMap(context));
}

void destroyPruningResult(PruningResult* pruningResult)
{
    if (!PointerIsValid(pruningResult)) {
        return;
    }

    if (PointerIsValid(pruningResult->bm_rangeSelectedPartitions)) {
        bms_free_ext(pruningResult->bm_rangeSelectedPartitions);
        pruningResult->bm_rangeSelectedPartitions = NULL;
    }

    if (PointerIsValid(pruningResult->intervalSelectedPartitions)) {
        bms_free_ext(pruningResult->intervalSelectedPartitions);
        pruningResult->intervalSelectedPartitions = NULL;
    }

    if (PointerIsValid(pruningResult->boundary)) {
        destroyPruningBoundary(pruningResult->boundary);
        pruningResult->boundary = NULL;
    }

    if (PointerIsValid(pruningResult->ls_rangeSelectedPartitions)) {
        list_free_ext(pruningResult->ls_rangeSelectedPartitions);
        pruningResult->ls_rangeSelectedPartitions = NIL;
    }
    if (PointerIsValid(pruningResult->ls_selectedPartitionnos)) {
        list_free_ext(pruningResult->ls_selectedPartitionnos);
        pruningResult->ls_selectedPartitionnos = NIL;
    }
    if (PointerIsValid(pruningResult->expr)) {
        pfree(pruningResult->expr);
        pruningResult->expr = NULL;
    }
    if (PointerIsValid(pruningResult->exprPart)) {
        pfree(pruningResult->exprPart);
        pruningResult->exprPart = NULL;
    }
    if (PointerIsValid(pruningResult->paramArg)) {
        pfree(pruningResult->paramArg);
        pruningResult->paramArg = NULL;
    }

    pfree_ext(pruningResult);
}

static void destroyPruningResultList(List* resultList)
{
    if (PointerIsValid(resultList)) {
        ListCell* cell = NULL;
        PruningResult* item = NULL;
        foreach (cell, resultList) {
            item = (PruningResult*)lfirst(cell);
            if (PointerIsValid(item)) {
                destroyPruningResult(item);
            }
        }
        list_free_ext(resultList);
    }

    return;
}

static Oid GetPartitionOidFromPartitionno(Relation relation, int partitionno)
{
    int totalnum;
    int i;
    Oid result = InvalidOid;
    int resultno;
    PartitionMap *partmap = relation->partMap;

    if (partitionno <= 0) {
        return InvalidOid;
    }

    if (partmap->type == PART_TYPE_RANGE || partmap->type == PART_TYPE_INTERVAL) {
        totalnum = ((RangePartitionMap*)partmap)->rangeElementsNum;
        for (i = 0; i < totalnum; i++) {
            resultno = ((RangePartitionMap*)partmap)->rangeElements[i].partitionno;
            if (partitionno == resultno) {
                result = ((RangePartitionMap*)partmap)->rangeElements[i].partitionOid;
                break;
            }
        }
    } else if (partmap->type == PART_TYPE_LIST) {
        totalnum = ((ListPartitionMap*)partmap)->listElementsNum;
        for (i = 0; i < totalnum; i++) {
            resultno = ((ListPartitionMap*)partmap)->listElements[i].partitionno;
            if (partitionno == resultno) {
                result = ((ListPartitionMap*)partmap)->listElements[i].partitionOid;
                break;
            }
        }
    } else if (partmap->type == PART_TYPE_HASH) {
        totalnum = ((HashPartitionMap*)partmap)->hashElementsNum;
        for (i = 0; i < totalnum; i++) {
            resultno = ((HashPartitionMap*)partmap)->hashElements[i].partitionno;
            if (partitionno == resultno) {
                result = ((HashPartitionMap*)partmap)->hashElements[i].partitionOid;
                break;
            }
        }
    }

    return result;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: get PartitionIdentifier for special SN in pruning result
 * Description	:
 * Notes		: start with 0
 */
Oid getPartitionOidFromSequence(Relation relation, int partSeq, int partitionno)
{
#define ReportPartseqOutRange()                                                                               \
    do {                                                                                                      \
        PARTITION_LOG(                                                                                        \
            "partSeq: %d out range of current relation partMap element num: %d. partitionno %d will be used", \
            partSeq, elementsNum, partitionno);                                                               \
    } while (0)

    Oid result = InvalidOid;
    int resultno = 0;
    int elementsNum;
    AssertEreport(PointerIsValid(relation), MOD_OPT, "Unexpected NULL pointer for relation.");
    AssertEreport(PointerIsValid(relation->partMap), MOD_OPT, "Unexpected NULL pointer for relation->partMap.");
    if (relation->partMap->type == PART_TYPE_RANGE || relation->partMap->type == PART_TYPE_INTERVAL) {
        elementsNum = ((RangePartitionMap*)relation->partMap)->rangeElementsNum;
        if (partSeq < elementsNum) {
            result = ((RangePartitionMap*)relation->partMap)->rangeElements[partSeq].partitionOid;
            resultno = ((RangePartitionMap*)relation->partMap)->rangeElements[partSeq].partitionno;
        } else {
            ReportPartseqOutRange();
        }
    } else if (relation->partMap->type == PART_TYPE_LIST) {
        elementsNum = ((ListPartitionMap*)relation->partMap)->listElementsNum;
        if (partSeq < elementsNum) {
            result = ((ListPartitionMap*)relation->partMap)->listElements[partSeq].partitionOid;
            resultno = ((ListPartitionMap*)relation->partMap)->listElements[partSeq].partitionno;
        } else {
            ReportPartseqOutRange();
        }
    } else if (relation->partMap->type == PART_TYPE_HASH) {
        elementsNum = ((HashPartitionMap*)relation->partMap)->hashElementsNum;
        if (partSeq < elementsNum) {
            result = ((HashPartitionMap*)relation->partMap)->hashElements[partSeq].partitionOid;
            resultno = ((HashPartitionMap*)relation->partMap)->hashElements[partSeq].partitionno;
        } else {
            ReportPartseqOutRange();
        }
    }

    /* if partSeq is out of range, or partitionno does not match */
    if (!OidIsValid(result) && partitionno > 0) {
        result = GetPartitionOidFromPartitionno(relation, partitionno);
    } else if (partitionno > 0 && partitionno != resultno) {
        PARTITION_LOG("partitionno does not match, src is %d, dest is %u. src partitionno will be used",
            partitionno, resultno);
        result = GetPartitionOidFromPartitionno(relation, partitionno);
    }

    if (!OidIsValid(result) && partitionno > 0) {
        bool issubpartition = RelationIsPartitionOfSubPartitionTable(relation);
        ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("could not find %s oid from %s %d for relation \"%s\"",
            issubpartition ? "subpartition" : "partition",
            issubpartition ? "subpartitionno" : "partitionno",
            partitionno,
            RelationGetRelationName(relation))));
    }

    return result;
}

/*
 * note that consts and constPointers are in and out parameter, and are provided by caller.
 */
void ConstructConstFromValues(Datum* datums, const bool* nulls, Oid* attrs, const int* colMap, int len,
    Const* consts, Const** constPointers)
{
    HeapTuple tp;

    for (int i = 0; i < len; i++) {
        int col = colMap[i];
        tp = SearchSysCache1((int)TYPEOID, ObjectIdGetDatum(attrs[col]));
        if (HeapTupleIsValid(tp)) {
            Form_pg_type typtup = (Form_pg_type)GETSTRUCT(tp);
            consts[i].xpr.type = T_Const;
            consts[i].consttype = attrs[col];
            consts[i].consttypmod = typtup->typtypmod;
            consts[i].constcollid = typtup->typcollation;
            consts[i].constlen = typtup->typlen;
            consts[i].constvalue = nulls[col] ? 0 : datums[col];
            consts[i].constisnull = nulls[col];
            consts[i].constbyval = typtup->typbyval;
            consts[i].location = -1;
            consts[i].ismaxvalue = false;
            constPointers[i] = &consts[i];

            ReleaseSysCache(tp);
        } else {
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("invalid type")));
        }
    }

    return;
}

SubPartitionPruningResult* GetSubPartitionPruningResult(List* selectedSubPartitions, int partSeq, int partitionno)
{
    ListCell* cell = NULL;
    foreach (cell, selectedSubPartitions) {
        SubPartitionPruningResult* subPartPruningResult = (SubPartitionPruningResult*)lfirst(cell);
        if (subPartPruningResult->partSeq == partSeq) {
            if (subPartPruningResult->partitionno != partitionno) {
                ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR), errmsg("the partitionno does not match, src is %d, dest is %u",
                    partitionno, subPartPruningResult->partitionno)));
            }
            return subPartPruningResult;
        }
    }
    return NULL;
}

/*
 * @@GaussDB@@
 * Brief			: delete from partition (partition_name, subpartition_name, ...)
 * Description		: eliminate partitions and subpartitions which not in the RTE (sub)partitionOidList.
 * return value 	: non-eliminated partitions and subpartitions.
 */
PruningResult* PartitionPruningForPartitionList(RangeTblEntry* rte, Relation rel)
{
    Oid partOid;
    Oid subpartOid;
    PruningResult* pruningRes = NULL;

    if (rte->partitionOidList->length == 1) {
        partOid = list_nth_oid(rte->partitionOidList, 0);
        if (rte->isContainSubPartition) {
            subpartOid = list_nth_oid(rte->subpartitionOidList, 0);
            return SingleSubPartitionPruningForRestrictInfo(subpartOid, rel, partOid);
        } else {
            return singlePartitionPruningForRestrictInfo(partOid, rel);
        }
    }
    /* shouldn't happen */
    if (!PointerIsValid(rel->partMap)) {
        ereport(ERROR, (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("relation of oid=\"%u\" is not partitioned table", rel->rd_id)));
    }

    pruningRes = makeNode(PruningResult);
    pruningRes->state = PRUNING_RESULT_SUBSET;
    MergePartitionListsForPruning(rte, rel, pruningRes);
    return pruningRes;
}

static bool PartialListBoundaryMatched(ListPartElement* part, List* keyPos, Const** keyValue)
{
    if (part->boundary[0].values[0]->ismaxvalue) {
        return true;
    }

    for (int i = 0; i < part->len; i++) {
        PartitionKey* bound = &part->boundary[i];
        ListCell* keyCell = NULL;
        foreach(keyCell, keyPos) {
            int id = lfirst_int(keyCell);
            if (ConstCompareWithNull(keyValue[id], bound->values[id], bound->values[id]->constcollid) != 0) {
                break;
            }
        }
        if (keyCell == NULL) {
            return true;
        }
    }
    return false;
}

/*
 * @@GaussDB@@
 * Brief			: Determine the number of boundary key values and prune partitions if need.
 * Description		: For multi-keys list partition table, pruning result may include many partitions
 *                    if the boundary does not contain all the key values.
 *                    In this case, partitionRoutingForValueEqual cannot be used to prune partitions.
 * return value 	: Whether the boundary does not contain all key values and the pruning ends.
 */
static bool PartitionPruningForPartialListBoundary(ListPartitionMap* listMap, PruningResult* pruningResult,
    Const** keyValues)
{
    List* keyPos = NULL;
    Bitmapset* listBms = NULL;

    for (int i = 0; i < pruningResult->boundary->partitionKeyNum; i++) {
        if (keyValues[i] != NULL) {
            keyPos = lappend_int(keyPos, i);
        }
    }
    /* If all key values exist, return false. */
    if (list_length(keyPos) == pruningResult->boundary->partitionKeyNum) {
        list_free_ext(keyPos);
        return false;
    }

    incre_partmap_refcount(&listMap->base);
    for (int i = 0; i < listMap->listElementsNum; i++) {
        if (PartialListBoundaryMatched(&listMap->listElements[i], keyPos, keyValues)) {
            listBms = bms_add_member(listBms, i);
        }
    }
    decre_partmap_refcount(&listMap->base);
    list_free_ext(keyPos);
    if (PointerIsValid(pruningResult->bm_rangeSelectedPartitions)) {
        Bitmapset* tempBms = bms_intersect(pruningResult->bm_rangeSelectedPartitions, listBms);
        bms_free_ext(pruningResult->bm_rangeSelectedPartitions);
        bms_free_ext(listBms);
        pruningResult->bm_rangeSelectedPartitions = tempBms;
    } else {
        pruningResult->bm_rangeSelectedPartitions = listBms;
    }
    if (pruningResult->boundary) {
        destroyPruningBoundary(pruningResult->boundary);
        pruningResult->boundary = NULL;
    }
    return true;
}
