/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/pruning.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "catalog/pg_partition_fn.h"

static PruningResult* partitionPruningWalker(Expr* expr, PruningContext* context);
static PruningResult* partitionPruningFromBoolExpr(const BoolExpr* expr, PruningContext* context);
static PruningResult* intersectChildPruningResult(const List* resultList, PruningContext* context);
static PruningResult* unionChildPruningResult(const List* resultList, PruningContext* context);
static PruningResult* partitionPruningFromScalarArrayOpExpr(const ScalarArrayOpExpr* arrayExpr, PruningContext* pruningCtx);
static PruningResult* recordBoundaryFromOpExpr(const OpExpr* expr, PruningContext* context);
static PruningResult* partitionPruningFromNullTest(NullTest* expr, PruningContext* context);

static void partitionPruningFromBoundary(Relation relation, PruningResult* pruningResult);
static void partitionFilter(Relation relation, PruningResult* pruningResult);
static bool partitionShouldEliminated(Relation relation, int partSeq, PruningBoundary* boundary);
static PartKeyRange* constructPartKeyRange(Relation relation, int partSeq);
static PruningBoundary* mergeBoundary(Relation relation, PruningBoundary* leftBoundary, PruningBoundary* rightBoundary);
static void cleanPruningBottom(Relation relation, PartitionIdentifier* bottomSeq, Const* value);
static void cleanPruningTop(Relation relation, PartitionIdentifier* topSeq, Const* value);
static void destroyPruningResult(PruningResult* pruningResult);
static void destroyPruningBoundary(PruningBoundary* boundary);
static void destroyPruningResultList(List* resultList);
static PruningBoundary* makePruningBoundary(int partKeyNum);
static PruningBoundary* copyBoundary(PruningBoundary* boundary);
static void generateListFromPruningBM(PruningResult* result);

PruningResult* getFullPruningResult(Relation relation)
{
    /* construct PrunningResult */
    PruningResult* pruningRes = NULL;
    RangePartitionMap* rangePartitionMap = NULL;
    int i = 0;

    if (!PointerIsValid(relation) || !PointerIsValid(relation->partMap)) {
        return NULL;
    }

    AssertEreport(relation->partMap->type == PART_TYPE_RANGE || relation->partMap->type == PART_TYPE_INTERVAL,
        MOD_OPT,
        "Unexpected partition map type: expecting RANGE or INTERVAL");

    rangePartitionMap = (RangePartitionMap*)relation->partMap;

    pruningRes = makeNode(PruningResult);
    pruningRes->state = PRUNING_RESULT_FULL;

    /* construct range bitmap */
    for (i = 0; i < rangePartitionMap->rangeElementsNum; i++) {
        pruningRes->bm_rangeSelectedPartitions = bms_add_member(pruningRes->bm_rangeSelectedPartitions, i);
        pruningRes->ls_rangeSelectedPartitions = lappend_int(pruningRes->ls_rangeSelectedPartitions, i);
    }
    if (relation->partMap->type != PART_TYPE_INTERVAL) {
        pruningRes->intervalOffset = 0;
        pruningRes->intervalSelectedPartitions = NULL;
    }

    return pruningRes;
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
        newpruningInfo->ls_rangeSelectedPartitions = list_copy(srcPruningResult->ls_rangeSelectedPartitions);

        return newpruningInfo;
    } else {
        return NULL;
    }
}
/*
 * Support partiton index unusable.
 * check if the partition index is unusable.
 */
bool checkPartitionIndexUnusable(Oid indexOid, int partItrs, PruningResult* pruning_result)
{
    Oid heapRelOid;
    Relation indexRel, heapRel;
    bool partitionIndexUnusable = true;
    ListCell* cell = NULL;
    List* part_seqs = pruning_result->ls_rangeSelectedPartitions;

    if (PointerIsValid(part_seqs))
        AssertEreport(
            partItrs == part_seqs->length, MOD_OPT, "The number of partitions does not match that of pruning result.");

    if (!OidIsValid(indexOid)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("invalid index oid to check for unusability")));
    }

    heapRelOid = IndexGetRelation(indexOid, false);
    heapRel = relation_open(heapRelOid, NoLock);
    indexRel = relation_open(indexOid, NoLock);
    if (!RelationIsPartitioned(heapRel) || !RelationIsPartitioned(indexRel) ||
        (heapRel->partMap->type != PART_TYPE_RANGE && heapRel->partMap->type != PART_TYPE_INTERVAL)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("relation %s is not partitioned when check partition index", RelationGetRelationName(heapRel))));
    }

    foreach (cell, part_seqs) {
        Oid tablepartitionid = InvalidOid;
        Oid indexpartitionid = InvalidOid;
        Partition tablepart = NULL;
        Partition indexpartition = NULL;
        List* partitionIndexOidList = NIL;
        int partSeq = lfirst_int(cell);

        tablepartitionid = getPartitionOidFromSequence(heapRel, partSeq);
        tablepart = partitionOpen(heapRel, tablepartitionid, AccessShareLock);

        /* get index partition and add it to a list for following scan */
        partitionIndexOidList = PartitionGetPartIndexList(tablepart);
        AssertEreport(PointerIsValid(partitionIndexOidList), MOD_OPT, "");
        if (!PointerIsValid(partitionIndexOidList)) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("no local indexes found for partition %s", PartitionGetPartitionName(tablepart)))));
        }
        indexpartitionid = searchPartitionIndexOid(indexOid, partitionIndexOidList);
        list_free_ext(partitionIndexOidList);
        indexpartition = partitionOpen(indexRel, indexpartitionid, AccessShareLock);

        // found a unusable index partition
        Assert(indexpartition);
        if (!indexpartition->pd_part->indisusable) {
            partitionIndexUnusable = false;
            partitionClose(indexRel, indexpartition, AccessShareLock);
            partitionClose(heapRel, tablepart, AccessShareLock);
            break;
        }

        // close index partition and table partition
        partitionClose(indexRel, indexpartition, AccessShareLock);
        partitionClose(heapRel, tablepart, AccessShareLock);
    }

    relation_close(heapRel, NoLock);
    relation_close(indexRel, NoLock);

    return partitionIndexUnusable;
}

/*
 * @@GaussDB@@
 * Brief
 * Description	: wipe out partitions whose local indexes are unusable.
 * return value:  return a pruning result without the wiped, the wiped are output as unusableIndexPruningResult
 */
IndexesUsableType eliminate_partition_index_unusable(Oid indexOid, PruningResult* inputPruningResult,
    PruningResult** indexUsablePruningResult, PruningResult** indexUnusablePruningResult)
{
    IndexesUsableType ret;
    int usable_partition_num = 0;
    int unusable_partition_num = 0;
    Oid heapRelOid;
    Relation indexRel, heapRel;
    Bitmapset* outIndexUsable_bm = NULL;
    Bitmapset* outIndexUnusable_bm = NULL;
    PruningResult* outIndexUsable_pr = NULL;
    PruningResult* outIndexUnusable_pr = NULL;
    int iterators = bms_num_members(inputPruningResult->bm_rangeSelectedPartitions);
    List* part_seqs = inputPruningResult->ls_rangeSelectedPartitions;
    ListCell* cell = NULL;

    if (PointerIsValid(part_seqs))
        AssertEreport(part_seqs->length == iterators, MOD_OPT, "");
    // sanity check
    if (!OidIsValid(indexOid)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                errmsg("invalid index oid to check for unusability")));
    }
    heapRelOid = IndexGetRelation(indexOid, false);

    heapRel = relation_open(heapRelOid, NoLock);
    indexRel = relation_open(indexOid, NoLock);
    if (!RelationIsPartitioned(heapRel) || !RelationIsPartitioned(indexRel)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmsg("relation %s is not partitioned", RelationGetRelationName(heapRel)))));
    }

    // first copy out 2 copies
    outIndexUsable_pr = copyPruningResult(inputPruningResult);
    outIndexUnusable_pr = copyPruningResult(inputPruningResult);

    // get the bm of outIndexUsable_pr, as we want to delete from it.
    outIndexUsable_bm = outIndexUsable_pr->bm_rangeSelectedPartitions;
    // free the bm of outIndexUnusable,
    // as we remove from outIndexUsable and add into outIndexUnusable
    bms_free_ext(outIndexUnusable_pr->bm_rangeSelectedPartitions);
    outIndexUnusable_pr->bm_rangeSelectedPartitions = NULL;

    // this is the scaning loop for selected partitions
    foreach (cell, part_seqs) {
        Oid tablepartitionid = InvalidOid;
        Oid indexpartitionid = InvalidOid;
        Partition tablepart = NULL;
        Partition indexpartition = NULL;
        List* partitionIndexOidList = NIL;
        int partSeq = lfirst_int(cell);

        tablepartitionid = getPartitionOidFromSequence(heapRel, partSeq);
        tablepart = partitionOpen(heapRel, tablepartitionid, AccessShareLock);

        /* get index partition and add it to a list for following scan */
        partitionIndexOidList = PartitionGetPartIndexList(tablepart);
        AssertEreport(PointerIsValid(partitionIndexOidList), MOD_OPT, "");
        if (!PointerIsValid(partitionIndexOidList)) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("no local indexes found for partition %s", PartitionGetPartitionName(tablepart)))));
        }
        indexpartitionid = searchPartitionIndexOid(indexOid, partitionIndexOidList);
        list_free_ext(partitionIndexOidList);
        indexpartition = partitionOpen(indexRel, indexpartitionid, AccessShareLock);

        // found a unusable index partition
        if (!indexpartition->pd_part->indisusable) {
            // delete partSeq from usable and add into unusable
            if (!bms_is_member(partSeq, outIndexUsable_bm) || bms_is_member(partSeq, outIndexUnusable_bm)) {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                            errmsg("bit map error when searching for unusable index partition"))));
            }
            outIndexUsable_bm = bms_del_member(outIndexUsable_bm, partSeq);
            outIndexUnusable_bm = bms_add_member(outIndexUnusable_bm, partSeq);
        }

        // close index partition and table partition, but keep the lock until executor end
        partitionClose(indexRel, indexpartition, NoLock);
        partitionClose(heapRel, tablepart, NoLock);
    }

    relation_close(heapRel, NoLock);
    relation_close(indexRel, NoLock);

    // result check
    usable_partition_num = bms_num_members(outIndexUsable_bm);
    unusable_partition_num = bms_num_members(outIndexUnusable_bm);
    if (usable_partition_num + unusable_partition_num != iterators ||
        bms_overlap(outIndexUsable_bm, outIndexUnusable_bm)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmsg("bit map error after searching for unusable index partition"))));
    }

    // set the return value
    if (usable_partition_num == iterators) {
        ret = INDEXES_FULL_USABLE;
    } else if (usable_partition_num > 0 && unusable_partition_num > 0) {
        ret = INDEXES_PARTIAL_USABLE;
    } else {
        ret = INDEXES_NONE_USABLE;
    }

    // set back the bit map
    if (usable_partition_num > 0) {
        outIndexUsable_pr->bm_rangeSelectedPartitions = outIndexUsable_bm;
        generateListFromPruningBM(outIndexUsable_pr);
        // set the output
        if (indexUsablePruningResult != NULL) {
            *indexUsablePruningResult = outIndexUsable_pr;
        }
    }
    // set back the bit map
    if (unusable_partition_num > 0) {
        outIndexUnusable_pr->bm_rangeSelectedPartitions = outIndexUnusable_bm;
        generateListFromPruningBM(outIndexUnusable_pr);
        // set the output
        if (indexUnusablePruningResult != NULL) {
            *indexUnusablePruningResult = outIndexUnusable_pr;
        }
    }
    return ret;
}

static void generateListFromPruningBM(PruningResult* result)
{
    int partitions = 0;
    int i = 0;
    int tmpcheck = 0;
    Bitmapset* tmpset = NULL;
    result->ls_rangeSelectedPartitions = NULL;

    tmpset = bms_copy(result->bm_rangeSelectedPartitions);
    partitions = bms_num_members(result->bm_rangeSelectedPartitions);

    for (; i < partitions; i++) {
        tmpcheck = bms_first_member(tmpset);
        AssertEreport(-1 != tmpcheck, MOD_OPT, "");
        if (-1 != tmpcheck) {
            result->ls_rangeSelectedPartitions = lappend_int(result->ls_rangeSelectedPartitions, tmpcheck);
        }
    }
    bms_free_ext(tmpset);
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

    if (0 == list_length(restrictInfoList)) {
        result = getFullPruningResult(rel);
    } else {
        ListCell* cell = NULL;
        RestrictInfo* iteratorRestrict = NULL;
        List* exprList = NULL;
        int length = 0;
        Expr* expr = NULL;

        foreach (cell, restrictInfoList) {
            iteratorRestrict = (RestrictInfo*)lfirst(cell);
            if (PointerIsValid(iteratorRestrict->clause)) {
                expr = (Expr*)copyObject(iteratorRestrict->clause);
                exprList = lappend(exprList, expr);
            }
        }

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

    if (PointerIsValid(result) && !PruningResultIsFull(result))
        generateListFromPruningBM(result);
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
    RangePartitionMap* rangePartMap = NULL;
    PruningResult* pruningRes = NULL;
    int counter = 0;

    if (!PointerIsValid(rel)) {
        return NULL;
    }

    /* shouldn't happen */
    if (!PointerIsValid(rel->partMap)) {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    errmsg("relation of oid=\"%u\" is not partitioned table", rel->rd_id))));
    }

    /*
     * only one possible partitionOid is InvalidOid:
     * select * from partitioned_table_name partition for (values);
     * and values pruning interval partition which does not physical exist.
     */
    if (!OidIsValid(partitionOid) && rel->partMap->type != PART_TYPE_INTERVAL) {
        return NULL;
    }

    pruningRes = makeNode(PruningResult);
    pruningRes->state = PRUNING_RESULT_SUBSET;

    /* it's a pattitioned table without interval */
    if (rel->partMap->type == PART_TYPE_RANGE || rel->partMap->type == PART_TYPE_INTERVAL) {
        rangePartMap = (RangePartitionMap*)rel->partMap;

        for (counter = 0; counter < rangePartMap->rangeElementsNum; counter++) {
            /* the partition is a range partition */
            if ((rangePartMap->rangeElements + counter)->partitionOid == partitionOid) {
                find = true;
                partitionSeq = counter;

                break;
            }
        }
    } else { /* shouldn't happen */   
        pfree_ext(pruningRes);
        ereport(
            ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupport partition type")));
    }

    /* never happen */
    if (!find) {
        pfree_ext(pruningRes);
        ereport(ERROR,
            (errmodule(MOD_OPT),
                (errcode(ERRCODE_UNDEFINED_TABLE),
                    errmsg("fail to find partition with oid %u for partitioned table %u", partitionOid, rel->rd_id))));
    }

    pruningRes->bm_rangeSelectedPartitions = bms_make_singleton(partitionSeq);

    generateListFromPruningBM(pruningRes);
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
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("partitionPruningForExpr: parameter can not be null")));
    }

    context = (PruningContext*)palloc0(sizeof(PruningContext));
    context->root = root;
    context->relation = rel;
    context->rte = rte;

    result = partitionPruningWalker(expr, context);

    /* Never happen, just to be self-contained */
    if (!PointerIsValid(result)) {
        ereport(ERROR,
            (errmodule(MOD_OPT), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("get null for partition pruning")));
    }

    partitionPruningFromBoundary(context->relation, result);
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
static PruningResult* partitionPruningWalker(Expr* expr, PruningContext* pruningCtx)
{
    PruningResult* result = NULL;

    AssertEreport(PointerIsValid(pruningCtx), MOD_OPT, "pruningCtx pointer is NULL.");
    AssertEreport(PointerIsValid(expr), MOD_OPT, "expr pointer is NULL.");

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
        } break;
        default: {
            result = makeNode(PruningResult);
            result->state = PRUNING_RESULT_FULL;
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

    AssertEreport(PointerIsValid(expr), MOD_OPT, "Pointer expr is NULL.");
    AssertEreport(PointerIsValid(context), MOD_OPT, "Pointer context is NULL.");
    AssertEreport(PointerIsValid(context->relation), MOD_OPT, "Pointer context->relation is NULL.");
    AssertEreport(PointerIsValid(context->rte), MOD_OPT, "Pointer context->rte is NULL.");

    if (expr->boolop == NOT_EXPR) {
        result = makeNode(PruningResult);
        result->state = PRUNING_RESULT_FULL;
        return result;
    }

    foreach (cell, expr->args) {
        arg = (Expr*)lfirst(cell);
        iterator = partitionPruningWalker(arg, context);

        resultList = lappend(resultList, iterator);
    }

    switch (expr->boolop) {
        case AND_EXPR:
            result = intersectChildPruningResult(resultList, context);
            break;
        case OR_EXPR:
            result = unionChildPruningResult(resultList, context);
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
    partMap = (RangePartitionMap*)(context->relation->partMap);

    partKeyNum = partMap->partitionKey->dim1;

    attrOffset = varIsInPartitionKey(var->varattno, partMap->partitionKey, partKeyNum);
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

    result->bm_rangeSelectedPartitions = bms_make_singleton(partMap->rangeElementsNum - 1);

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

    AssertEreport(PointerIsValid(context), MOD_OPT, "Pointer context is NULL.");

    result = makeNode(PruningResult);

    result->state = PRUNING_RESULT_FULL;

    AssertEreport(resultList, MOD_OPT, "");

    foreach (cell, resultList) {
        iteratorResult = (PruningResult*)lfirst(cell);

        AssertEreport(iteratorResult, MOD_OPT, "iteratorResult context is NNULL.");
        if (iteratorResult->state == PRUNING_RESULT_EMPTY) {
            result->state = PRUNING_RESULT_EMPTY;
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
        } else if (result != NULL) {
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
                tempBoundary = mergeBoundary(context->relation, result->boundary, iteratorResult->boundary);
                destroyPruningBoundary(result->boundary);
                result->boundary = tempBoundary;
            }

            if (BoundaryIsEmpty(result->boundary)) {
                result->state = PRUNING_RESULT_EMPTY;
                break;
            }

            result->state = PRUNING_RESULT_SUBSET;
        }
    }

    if (PruningResultIsEmpty(result)) {
        destroyPruningResult(result);
        result = makeNode(PruningResult);
        result->state = PRUNING_RESULT_EMPTY;
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

    AssertEreport(PointerIsValid(context), MOD_OPT, "Pointer context is NULL.");

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

        partitionPruningFromBoundary(context->relation, iteratorResult);

        if (iteratorResult->state == PRUNING_RESULT_EMPTY) {
            continue;
        } else if (iteratorResult->state == PRUNING_RESULT_FULL) {
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

static PruningResult* partitionPruningFromScalarArrayOpExpr(const ScalarArrayOpExpr* arrayExpr, PruningContext* pruningCtx)
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
            result = partitionPruningWalker((Expr*)makeBoolExpr(OR_EXPR, exprList, 0), pruningCtx);
        } else {
            result = partitionPruningWalker((Expr*)makeBoolExpr(AND_EXPR, exprList, 0), pruningCtx);
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
                result = partitionPruningWalker((Expr*)makeBoolExpr(OR_EXPR, exprList, 0), pruningCtx);
            } else {
                result = partitionPruningWalker((Expr*)makeBoolExpr(AND_EXPR, exprList, 0), pruningCtx);
            }
            success = true;
        }
    }

    if (success) {
        return result;
    } else {
        result = makeNode(PruningResult);
        result->state = PRUNING_RESULT_FULL;
        return result;
    }
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

    AssertEreport(PointerIsValid(context), MOD_OPT, "Unexpected NULL pointer for context.");
    AssertEreport(PointerIsValid(context->relation), MOD_OPT, "Unexpected NULL pointer for context->relation.");
    AssertEreport(
        PointerIsValid(context->relation->partMap), MOD_OPT, "Unexpected NULL pointer for context->relation->partMap.");

    result = makeNode(PruningResult);

    /* length of args MUST be 2 */
    if (!PointerIsValid(expr) || list_length(expr->args) != 2 || !PointerIsValid(opName = get_opname(expr->opno))) {
        result->state = PRUNING_RESULT_FULL;
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
        node = estimate_expression_value(context->root, (Node*)rightArg);
        if (node != NULL)
            rightArg = (Expr*)node;
    } else if (IsA(rightArg, Var)) {
        node = estimate_expression_value(context->root, (Node*)leftArg);
        if (node != NULL)
            leftArg = (Expr*)node;
    }

    /* one of args MUST be Const, and another argument Must be Var */
    if (!((T_Const == nodeTag(leftArg) && T_Var == nodeTag(rightArg)) ||
            (T_Var == nodeTag(leftArg) && T_Const == nodeTag(rightArg)))) {
        result->state = PRUNING_RESULT_FULL;
        return result;
    }

    if (T_Const == nodeTag(leftArg)) {
        constArg = (Const*)leftArg;
        varArg = (Var*)rightArg;
        rightArgIsConst = false;
    } else {
        constArg = (Const*)rightArg;
        varArg = (Var*)leftArg;
    }

    /* Var MUST represents for current relation */
    if (context->rte->relid != context->relation->rd_id) {
        result->state = PRUNING_RESULT_FULL;
        return result;
    }

    /* Var's column MUST belongs to parition key columns */
    partMap = (RangePartitionMap*)(context->relation->partMap);

    partKeyNum = partMap->partitionKey->dim1;

    attrOffset = varIsInPartitionKey(varArg->varattno, partMap->partitionKey, partKeyNum);
    if (attrOffset < 0) {
        result->state = PRUNING_RESULT_FULL;
        return result;
    }

    if (constArg->constisnull) {
        result->state = PRUNING_RESULT_EMPTY;
        return result;
    }

    /* initialize PruningBoundary */
    result->boundary = makePruningBoundary(partKeyNum);

    boundary = result->boundary;

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

#define NoNeedPruning(pruningResult)                                              \
    (PruningResultIsFull(pruningResult) || PruningResultIsEmpty(pruningResult) || \
        !PointerIsValid((pruningResult)->boundary))

#define IsCleanPruningBottom(bottomSeqPtr, pruningResult, bottomValue)                     \
    ((pruningResult)->boundary->partitionKeyNum > 1 && PointerIsValid((bottomValue)[0]) && \
        !(pruningResult)->boundary->minClose[0])

#define IsCleanPruningTop(topSeqPtr, pruningResult, topValue)                                         \
    ((pruningResult)->boundary->partitionKeyNum > 1 && (topValue) && PointerIsValid((topValue)[0]) && \
        !(pruningResult)->boundary->maxClose[0])

/*
 * @@GaussDB@@
 * Brief
 * Description	: Get eliminated partitions according to boundary of PruningResult.
 *                Fill PruningResult bitmap attribute with bitmap for eliminated partitions
 * return value:  true if success, else false.
 *                if failed, This function will print log.
 */
static void partitionPruningFromBoundary(Relation relation, PruningResult* pruningResult)
{
    Const** bottomValue = NULL;
    Const** topValue = NULL;
    Bitmapset* rangeBms = NULL;
    int rangeStart = -1;
    int rangeEnd = -1;
    int i = 0;
    int compare = 0;
    bool isTopClosed = true;
    RangePartitionMap* rangeMap = NULL;

    AssertEreport(PointerIsValid(relation), MOD_OPT, "Unexpected NULL pointer for relation.");
    AssertEreport(PointerIsValid(relation->partMap), MOD_OPT, "Unexpected NULL pointer for relation->partMap.");
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

    compare = partitonKeyCompare(bottomValue, topValue, pruningResult->boundary->partitionKeyNum);
    if (compare > 0) {
        pruningResult->state = PRUNING_RESULT_EMPTY;
        return;
    }

    // compare the bottom and the intervalMax, if the bottom is large than or equal than intervalMax, pruning result is
    // empty.
    partitionRoutingForValueRange(
        relation, bottomValue, pruningResult->boundary->partitionKeyNum, true, true, u_sess->opt_cxt.bottom_seq);
    if (IsCleanPruningBottom(u_sess->opt_cxt.bottom_seq, pruningResult, bottomValue)) {
        cleanPruningBottom(relation, u_sess->opt_cxt.bottom_seq, bottomValue[0]);
    }

    partitionRoutingForValueRange(
        relation, topValue, pruningResult->boundary->partitionKeyNum, isTopClosed, false, u_sess->opt_cxt.top_seq);
    if (IsCleanPruningTop(u_sess->opt_cxt.top_seq, pruningResult, topValue)) {
        cleanPruningTop(relation, u_sess->opt_cxt.top_seq, topValue[0]);
    }
    rangeMap = (RangePartitionMap*)relation->partMap;

    rangeEnd = rangeMap->rangeElementsNum - 1;

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
    partitionFilter(relation, pruningResult);

    if (pruningResult->boundary) {
        destroyPruningBoundary(pruningResult->boundary);
        pruningResult->boundary = NULL;
    }
}

/************************************************************************************
 *        pruning for multi-column partition key
 ************************************************************************************/
static void partitionFilter(Relation relation, PruningResult* pruningResult)
{
    Bitmapset* resourceBms = NULL;
    Bitmapset* result = NULL;

    if (!RELATION_IS_PARTITIONED(relation) || !PartitionMapIsRange(relation->partMap) ||
        pruningResult->boundary->partitionKeyNum == 1) {
        return;
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

        if (!partitionShouldEliminated(relation, curPart, pruningResult->boundary)) {
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

static bool partitionShouldEliminated(Relation relation, int partSeq, PruningBoundary* boundary)
{
    int i = 0;
    PartKeyRange* partRange = NULL;
    bool isSelected = true;

    AssertEreport(
        RELATION_IS_PARTITIONED(relation), MOD_OPT, "Expected the relation to be partitioned, run into exception.");
    AssertEreport(PartitionMapIsRange(relation->partMap),
        MOD_OPT,
        "Expected the partition map type to be range type, run into exception.");
    AssertEreport(boundary->partitionKeyNum > 1,
        MOD_OPT,
        "Expected the the number of partition key to be >1, run into exception.");

    partRange = constructPartKeyRange(relation, partSeq);
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

static PartKeyRange* constructPartKeyRange(Relation relation, int partSeq)
{
    RangeElement* prePartition = NULL;
    RangeElement* nextPartition = NULL;
    RangePartitionMap* partMap = NULL;
    PartKeyRange* result = NULL;
    Const* left = NULL;
    Const* right = NULL;
    int i = 0;
    int partKeyNum = 0;

    AssertEreport(RELATION_IS_PARTITIONED(relation), MOD_OPT, "Expected partitioned relation, run into exception.");
    AssertEreport(PartitionMapIsRange(relation->partMap),
        MOD_OPT,
        "Expected partition map to be range type, run into exception.");

    result = (PartKeyRange*)palloc0(sizeof(PartKeyRange));

    partMap = (RangePartitionMap*)relation->partMap;
    partKeyNum = partMap->partitionKey->dim1;

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

static PruningBoundary* mergeBoundary(Relation relation, PruningBoundary* leftBoundary, PruningBoundary* rightBoundary)
{
    int i = 0;
    PruningBoundary* result = NULL;
    Const* leftValue = NULL;
    Const* rightValue = NULL;

    if (!PointerIsValid(leftBoundary) && !PointerIsValid(rightBoundary)) {
        return NULL;
    }

    if (!PointerIsValid(leftBoundary)) {
        return copyBoundary(leftBoundary);
    }

    if (!PointerIsValid(rightBoundary)) {
        return copyBoundary(rightBoundary);
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
            compare = partitonKeyCompare(&leftValue, &rightValue, 1);
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
            compare = partitonKeyCompare(&leftValue, &rightValue, 1);
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
            compare = partitonKeyCompare(&leftValue, &rightValue, 1);
            if (compare > 0 || (compare == 0 && !(result->minClose[i] && result->maxClose[i]))) {
                result->state = PRUNING_RESULT_EMPTY;
                break;
            }
        }
    }

    return result;
}

static void cleanPruningBottom(Relation relation, PartitionIdentifier* bottomSeq, Const* value)
{
    int i = 0;
    RangePartitionMap* partMap = NULL;

    if (bottomSeq->partSeq < 0 || bottomSeq->partSeq >= ((RangePartitionMap*)relation->partMap)->rangeElementsNum ||
        value == NULL) {
        return;
    }

    incre_partmap_refcount(relation->partMap);
    partMap = (RangePartitionMap*)(relation->partMap);

    for (i = bottomSeq->partSeq; i < partMap->rangeElementsNum; i++) {
        RangeElement* range = partMap->rangeElements + i;
        int compare = 0;

        compare = partitonKeyCompare(&value, range->boundary, 1);
        if (compare >= 0) {
            continue;
        } else {
            break;
        }
    }

    if (i >= partMap->rangeElementsNum) {
        i = -1;
    }

    bottomSeq->partSeq = i;
    decre_partmap_refcount(relation->partMap);
}

static void cleanPruningTop(Relation relation, PartitionIdentifier* topSeq, Const* value)
{
    int i = 0;
    RangePartitionMap* partMap = NULL;

    if (topSeq->partSeq < 0 || topSeq->partSeq >= ((RangePartitionMap*)relation->partMap)->rangeElementsNum ||
        value == NULL) {
        return;
    }

    incre_partmap_refcount(relation->partMap);
    partMap = (RangePartitionMap*)(relation->partMap);

    for (i = topSeq->partSeq; i >= 0; i--) {
        RangeElement* range = partMap->rangeElements + i;
        int compare = 0;

        compare = partitonKeyCompare(&value, range->boundary, 1);
        if (compare <= 0) {
            continue;
        } else {
            break;
        }
    }

    if (i < topSeq->partSeq) {
        i++;
    }

    topSeq->partSeq = i;
    decre_partmap_refcount(relation->partMap);
}

static void destroyPruningResult(PruningResult* pruningResult)
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

    pfree_ext(pruningResult);
}

static void destroyPruningBoundary(PruningBoundary* boundary)
{
    int i = 0;
    Const* tempConst = NULL;

    if (!PointerIsValid(boundary)) {
        return;
    }

    for (; i < boundary->partitionKeyNum; i++) {
        tempConst = (Const*)DatumGetPointer(boundary->max[i]);
        if (tempConst != NULL) {
            pfree_ext(tempConst);
        }

        tempConst = (Const*)DatumGetPointer(boundary->min[i]);
        if (tempConst != NULL) {
            pfree_ext(tempConst);
        }
    }

    pfree_ext(boundary);
    return;
}

static PruningBoundary* makePruningBoundary(int partKeyNum)
{
    PruningBoundary* boundary = NULL;

    AssertEreport(partKeyNum > 0, MOD_OPT, "Expected positive number of partion key, run into exception.");

    boundary = (PruningBoundary*)palloc0(sizeof(PruningBoundary));

    boundary->partitionKeyNum = partKeyNum;

    boundary->max = (Datum*)palloc0(partKeyNum * sizeof(Datum));

    boundary->maxClose = (bool*)palloc0(partKeyNum * sizeof(bool));

    boundary->min = (Datum*)palloc0(partKeyNum * sizeof(Datum));

    boundary->minClose = (bool*)palloc0(partKeyNum * sizeof(bool));

    boundary->state = PRUNING_RESULT_FULL;

    return boundary;
}

static PruningBoundary* copyBoundary(PruningBoundary* boundary)
{
    PruningBoundary* result = NULL;
    int i = 0;
    Const* tempConst = NULL;

    if (!PointerIsValid(boundary)) {
        return NULL;
    }

    result = makePruningBoundary(boundary->partitionKeyNum);

    result->state = boundary->state;

    for (; i < result->partitionKeyNum; i++) {
        result->maxClose[i] = boundary->maxClose[i];
        result->minClose[i] = boundary->minClose[i];

        tempConst = (Const*)DatumGetPointer(boundary->max[i]);
        tempConst = (Const*)copyObject(tempConst);
        result->max[i] = PointerGetDatum(tempConst);

        tempConst = (Const*)DatumGetPointer(boundary->min[i]);
        tempConst = (Const*)copyObject(tempConst);
        result->min[i] = PointerGetDatum(tempConst);
    }

    return result;
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

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: get PartitionIdentifier for special SN in pruning result
 * Description	:
 * Notes		: start with 0
 */
Oid getPartitionOidFromSequence(Relation relation, int partSeq)
{
    Oid result = InvalidOid;
    AssertEreport(PointerIsValid(relation), MOD_OPT, "Unexpected NULL pointer for relation.");
    AssertEreport(PointerIsValid(relation->partMap), MOD_OPT, "Unexpected NULL pointer for relation->partMap.");

    if (relation->partMap->type == PART_TYPE_RANGE || relation->partMap->type == PART_TYPE_INTERVAL) {
        int rangeElementsNum = ((RangePartitionMap*)(relation->partMap))->rangeElementsNum;
        if (partSeq < rangeElementsNum) {
            result = ((RangePartitionMap*)(relation->partMap))->rangeElements[partSeq].partitionOid;
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                    errmsg("partSeq: %d out range of current relation partMap element num: %d.",
                        partSeq,
                        rangeElementsNum)));
        }
        /* do simple check, as rangeElements already be sorted */
        if (partSeq > 0 &&
            result == ((RangePartitionMap*)(relation->partMap))->rangeElements[partSeq - 1].partitionOid) {
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                    errmsg("Duplicate range partition map oids: %u, please try again.", result)));
        }
    } else {
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Unsupport partition strategy \"%c\"", relation->partMap->type)));
    }
    return result;
}
