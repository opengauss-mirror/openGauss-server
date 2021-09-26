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
 * ---------------------------------------------------------------------------------------
 *
 * sqladvisor_extract.cpp
 *		sqladvisor extract query reference component.
 * 
 *
 * IDENTIFICATION
 *      src/gausskernel/optimizer/commands/optimizer/sqladvisor_extract.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "nodes/pg_list.h"
#include "funcapi.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "optimizer/randomplan.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "commands/sqladvisor.h"
#include "nodes/makefuncs.h"
#include "utils/typcache.h"

static void assignOuterPlan(RecursiveExtractNamespace* dpetns, Plan* plan);
static void assignInnerPlan(RecursiveExtractNamespace* dpetns, Plan* plan, List* subplans);
static VirtualAttrGroupInfo* createVirtualAttrGroupInfo(Oid relid, List* vars);
static void extractQual(List* qual, Plan* plan, List* ancestors, List* rtable, List* subplans);
static void extractGroupbyKeys(Plan* plan, int nkeys, AttrNumber* keycols, List* ancestors,
                               List* rtable, List* subplans);
static void extractRuleExpr(Node* node, RecursiveExtractContext* context);
static void extractOperExpr(Node* node, RecursiveExtractContext* context);
static void extractVariable(Var* var, int levelsup, RecursiveExtractContext* context);
static void extractConstExpr(Const* constVal, RecursiveExtractContext* context);
static void extractAggExpr(Aggref* aggref, RecursiveExtractContext* context);
static void extractParameter(Param* node, RecursiveExtractContext* context);
static List* extractSubplan(Expr* node, List* resSubplan, List* subplans);
static List* extractVecSubplan(Expr* node, List* resSubplan, List* subplans);
static void pushExtractChildPlan(RecursiveExtractNamespace* dpetns, Plan* plan,
                                 RecursiveExtractNamespace* saveDpetns, List* subplans);
static void popExtractChildPlan(RecursiveExtractNamespace* dpetns, RecursiveExtractNamespace* saveDpetns);
List* deparseContextForExtractPlan(Node* plan, List* ancestors, List* subplans);
static void setDeparseExtractPlan(RecursiveExtractNamespace* dpetns, Plan* plan, List* subplans);
static VirtualAttrGroupInfo* getAttrInfoByVar(RangeTblEntry* rte, Var* var);
static Node* getQualCell(List* quals, Node* expr);
static JoinCell* getJoinCell(List* joinQuals, Node* expr);
static void initRecursiveExtractContext(RecursiveExtractContext* context, List* dpcontext,
                                        List* rtable, List* subplans);
static void insertJoin2CandicateTable(RangeTblEntry* rte, Node* expr, Var* arg1, Var* arg2, double rowsWeight);
static void insertQual2CandicateTable(RangeTblEntry* rte, Node* expr, Var* arg1);
static void insertGrpby2CandicateTable(RangeTblEntry* rte, Var* var, double rowsWeight);
static bool equalExpr(Node* a, Node* b);
static bool equalOpExpr(Node* a, Node* b);
static bool equalVar(Var* a, Var* b);
static bool equalConst(Const* a, Const* b);
static char* getConstCharValue(Const* constval);
static double getPlanRows(Plan* plan);
static void initAdviseTable(RangeTblEntry* rte, AdviseTable* adviseTable);
static void fillinAdviseTable(Node* node, List* rtable, double rowsWeight);
static bool isAssignReplication(Oid relid);

/* default first query is the current query */
static AdviseQuery* getCurrentQueryFromCandicateQueries()
{
    return (AdviseQuery*)linitial(u_sess->adv_cxt.candicateQueries);
}

static List* extractNodeTidScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    TidScan* tidScan = (TidScan*)plan;
    /*
        * The tidquals list has OR semantics, so be sure to show it
        * as an OR condition.
        */
    List* tidquals = ((TidScan*)plan)->tidquals;

    if (list_length(tidquals) > 1)
        tidquals = list_make1(make_orclause(tidquals));

    extractQual(tidquals, plan, ancestors, rtable, subplans);
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)tidScan->scan.plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)tidScan->scan.plan.qual, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)tidScan->tidquals, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeVecMergeJoin(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    VecMergeJoin* vecMergeJoin = (VecMergeJoin*)plan;

    extractQual(((MergeJoin*)plan)->mergeclauses, plan, ancestors, rtable, subplans);
    extractQual(((MergeJoin*)plan)->join.joinqual, plan, ancestors, rtable, subplans);
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractVecSubplan((Expr*)vecMergeJoin->join.plan.targetlist, resSubplan, subplans);
    resSubplan = extractVecSubplan((Expr*)vecMergeJoin->join.plan.qual, resSubplan, subplans);
    resSubplan = extractVecSubplan((Expr*)vecMergeJoin->join.joinqual, resSubplan, subplans);
    resSubplan = extractVecSubplan((Expr*)vecMergeJoin->join.nulleqqual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeMergeJoin(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    MergeJoin* mergeJoin = (MergeJoin*)plan;

    extractQual(mergeJoin->mergeclauses, plan, ancestors, rtable, subplans);
    extractQual(mergeJoin->join.joinqual, plan, ancestors, rtable, subplans);
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)mergeJoin->join.plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)mergeJoin->join.plan.qual, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)mergeJoin->join.joinqual, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)mergeJoin->join.nulleqqual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeVecHashJoin(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    VecHashJoin* vecHashJoin = (VecHashJoin*)plan;

    extractQual(((HashJoin*)plan)->hashclauses, plan, ancestors, rtable, subplans);
    extractQual(((HashJoin*)plan)->join.joinqual, plan, ancestors, rtable, subplans);
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)vecHashJoin->join.plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)vecHashJoin->join.plan.qual, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)vecHashJoin->join.joinqual, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)vecHashJoin->join.nulleqqual, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)vecHashJoin->hashclauses, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeHashJoin(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    HashJoin* hashJoin = (HashJoin*)plan;

    extractQual(hashJoin->hashclauses, plan, ancestors, rtable, subplans);
    extractQual(hashJoin->join.joinqual, plan, ancestors, rtable, subplans);
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)hashJoin->join.plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)hashJoin->join.plan.qual, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)hashJoin->join.joinqual, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)hashJoin->join.nulleqqual, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)hashJoin->hashclauses, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeVecAgg(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    VecAgg* vecAgg = (VecAgg*)plan;

    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    if (vecAgg->numCols > 0 && !vecAgg->groupingSets) {
        extractGroupbyKeys(outerPlan(plan), vecAgg->numCols, vecAgg->grpColIdx, ancestors, rtable, subplans);
    }

    resSubplan = extractVecSubplan((Expr*)vecAgg->plan.targetlist, resSubplan, subplans);
    resSubplan = extractVecSubplan((Expr*)vecAgg->plan.qual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeAgg(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    Agg* agg = (Agg*)plan;

    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    if (agg->numCols > 0 && !agg->groupingSets) {
        extractGroupbyKeys(outerPlan(plan), agg->numCols, agg->grpColIdx, ancestors, rtable, subplans);
    }
    
    resSubplan = extractSubplan((Expr*)agg->plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)agg->plan.qual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeBaseResult(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    BaseResult* baseResult = (BaseResult*)plan;

    extractQual((List*)baseResult->resconstantqual, plan, ancestors, rtable, subplans);
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)baseResult->plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)baseResult->plan.qual, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)baseResult->resconstantqual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeIndexScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    IndexScan* indexScan = (IndexScan*)plan;

    extractQual(indexScan->indexqualorig, plan, ancestors, rtable, subplans);
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)indexScan->scan.plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)indexScan->scan.plan.qual, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)indexScan->indexqualorig, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeIndexOnlyScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    IndexOnlyScan* indexOnlyScan = (IndexOnlyScan*)plan;

    extractQual(indexOnlyScan->indexqual, plan, ancestors, rtable, subplans);
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)indexOnlyScan->scan.plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)indexOnlyScan->scan.plan.qual, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)indexOnlyScan->indexqual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeCStoreIndexScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    CStoreIndexScan* cStoreIndexScan = (CStoreIndexScan*)plan;

    extractQual(cStoreIndexScan->indexqualorig, plan, ancestors, rtable, subplans);
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)cStoreIndexScan->scan.plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)cStoreIndexScan->indexqualorig, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeDfsIndexScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    DfsIndexScan* dfsIndexScan = (DfsIndexScan*)plan;

    extractQual(dfsIndexScan->indexqualorig, plan, ancestors, rtable, subplans);
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractVecSubplan((Expr*)dfsIndexScan->dfsScan->plan.targetlist, resSubplan, subplans);
    resSubplan = extractVecSubplan((Expr*)dfsIndexScan->dfsScan->plan.qual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeBitmapHeapScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    BitmapHeapScan* bitmapHeapScan = (BitmapHeapScan*)plan;

    extractQual(bitmapHeapScan->bitmapqualorig, plan, ancestors, rtable, subplans);
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)bitmapHeapScan->scan.plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)bitmapHeapScan->scan.plan.qual, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)bitmapHeapScan->bitmapqualorig, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeCStoreIndexHeapScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    CStoreIndexHeapScan* cStoreIndexHeapScan = (CStoreIndexHeapScan*)plan;

    extractQual(((BitmapHeapScan*)plan)->bitmapqualorig, plan, ancestors, rtable, subplans);
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)cStoreIndexHeapScan->scan.plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)cStoreIndexHeapScan->bitmapqualorig, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeSeqScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    SeqScan* seqScan = (SeqScan*)plan;
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)seqScan->plan.targetlist, resSubplan, subplans);

    if (seqScan->tablesample) {
        resSubplan = extractSubplan((Expr*)seqScan->tablesample->args, resSubplan, subplans);
        resSubplan = extractSubplan((Expr*)seqScan->tablesample->repeatable, resSubplan, subplans);
    }
    return resSubplan;
}

static List* extractNodeCStoreScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    CStoreScan* cStoreScan = (CStoreScan*)plan;
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)cStoreScan->plan.targetlist, resSubplan, subplans);

    if (cStoreScan->tablesample) {
        resSubplan = extractSubplan((Expr*)cStoreScan->tablesample->args, resSubplan, subplans);
        resSubplan = extractSubplan((Expr*)cStoreScan->tablesample->repeatable, resSubplan, subplans);
    }
    return resSubplan;
}

static List* extractNodeVecNestLoop(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    VecNestLoop* vecNestLoop = (VecNestLoop*)plan;
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    extractQual(((NestLoop*)plan)->join.joinqual, plan, ancestors, rtable, subplans);
    resSubplan = extractVecSubplan((Expr*)vecNestLoop->join.plan.targetlist, resSubplan, subplans);
    resSubplan = extractVecSubplan((Expr*)vecNestLoop->join.plan.qual, resSubplan, subplans);
    resSubplan = extractVecSubplan((Expr*)vecNestLoop->join.joinqual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeVecResult(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    VecResult* vecResult = (VecResult*)plan;
    extractQual((List*)((BaseResult*)plan)->resconstantqual, plan, ancestors, rtable, subplans);
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractVecSubplan((Expr*)vecResult->plan.targetlist, resSubplan, subplans);
    resSubplan = extractVecSubplan((Expr*)vecResult->plan.qual, resSubplan, subplans);
    resSubplan = extractVecSubplan((Expr*)vecResult->resconstantqual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeGroup(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    Group* group = (Group*)plan;
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    extractGroupbyKeys(outerPlan(plan), group->numCols, group->grpColIdx, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)group->plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)group->plan.qual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeVecGroup(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    VecGroup* vecGroup = (VecGroup*)plan;
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    extractGroupbyKeys(outerPlan(plan), vecGroup->numCols, vecGroup->grpColIdx, ancestors, rtable, subplans);
    resSubplan = extractVecSubplan((Expr*)vecGroup->plan.targetlist, resSubplan, subplans);
    resSubplan = extractVecSubplan((Expr*)vecGroup->plan.qual, resSubplan, subplans);
    return resSubplan;
}

static void extractNodeSort(Plan* plan, List* rtable)
{
    /* hook for index_advisor */
    if (u_sess->adv_cxt.getPlanInfoFunc != NULL) {
        ((get_info_from_plan_hook_type)u_sess->adv_cxt.getPlanInfoFunc) ((Node *)plan, rtable);
    }
}

static List* extractNodeValuesScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    ValuesScan* valuesScan = (ValuesScan*)plan;
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)valuesScan->scan.plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)valuesScan->scan.plan.qual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeCteScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    CteScan* cteScan = (CteScan*)plan;
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)cteScan->scan.plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)cteScan->scan.plan.qual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeWorkTableScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    WorkTableScan* workTableScan = (WorkTableScan*)plan;
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)workTableScan->scan.plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)workTableScan->scan.plan.qual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeDfsScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    DfsScan* dfsScan = (DfsScan*)plan;
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractVecSubplan((Expr*)dfsScan->plan.targetlist, resSubplan, subplans);
    resSubplan = extractVecSubplan((Expr*)dfsScan->plan.qual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeFunctionScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    FunctionScan* functionScan = (FunctionScan*)plan;
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)functionScan->scan.plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)functionScan->scan.plan.qual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeForeignScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    ForeignScan* foreignScan = (ForeignScan*)plan;
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)foreignScan->scan.plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)foreignScan->scan.plan.qual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeVecForeignScan(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    VecForeignScan* vecForeignScan = (VecForeignScan*)plan;
    extractQual(plan->qual, plan, ancestors, rtable, subplans);
    resSubplan = extractVecSubplan((Expr*)vecForeignScan->scan.plan.targetlist, resSubplan, subplans);
    resSubplan = extractVecSubplan((Expr*)vecForeignScan->scan.plan.qual, resSubplan, subplans);
    return resSubplan;
}

static List* extractNodeNestLoop(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;
    NestLoop* nestLoop = (NestLoop*)plan;        
    extractQual(nestLoop->join.joinqual, plan, ancestors, rtable, subplans);
    resSubplan = extractSubplan((Expr*)nestLoop->join.plan.targetlist, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)nestLoop->join.plan.qual, resSubplan, subplans);
    resSubplan = extractSubplan((Expr*)nestLoop->join.joinqual, resSubplan, subplans);
    return resSubplan;
}

static void extractMemberNodes(List* plans, List* ancestors, List* rtable, List* subplans)
{
    ListCell* lc = NULL;
    Plan* plan = NULL;
    foreach (lc, plans) {
        plan = (Plan*)lfirst(lc);
        extractNode(plan, ancestors, rtable, subplans);
    }
}

static void extractSpecialPlans(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    switch (nodeTag(plan)) {
        case T_SubqueryScan:
        case T_VecSubqueryScan: {
            extractNode(((SubqueryScan*)plan)->subplan, ancestors, rtable, subplans);
        } break;
        case T_VecAppend:
        case T_Append: {
            extractMemberNodes(((Append*)plan)->appendplans, ancestors, rtable, subplans);
        } break;
        case T_MergeAppend: {
            extractMemberNodes(((MergeAppend*)plan)->mergeplans, ancestors, rtable, subplans);
        } break;
        case T_BitmapAnd: {
            extractMemberNodes(((BitmapAnd*)plan)->bitmapplans, ancestors, rtable, subplans);
        } break;
        case T_BitmapOr: {
            extractMemberNodes(((BitmapOr*)plan)->bitmapplans, ancestors, rtable, subplans);
        } break;
        case T_CStoreIndexAnd: {
            extractMemberNodes(((CStoreIndexAnd*)plan)->bitmapplans, ancestors, rtable, subplans);
        } break;
        case T_CStoreIndexOr: {
            extractMemberNodes(((CStoreIndexOr*)plan)->bitmapplans, ancestors, rtable, subplans);
        } break;
        case T_ExtensiblePlan: {
            extractMemberNodes(((ExtensiblePlan*)plan)->extensible_plans, ancestors, rtable, subplans);
        } break;
        default:
            break;
    }
}

void extractNode(Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    List* resSubplan = NIL;

    switch (nodeTag(plan)) {
        case T_IndexScan: {
            resSubplan = extractNodeIndexScan(plan, ancestors, rtable, subplans); 
        } break;
        case T_IndexOnlyScan: {
            resSubplan = extractNodeIndexOnlyScan(plan, ancestors, rtable, subplans);
        } break;
        case T_BitmapIndexScan: {
            BitmapIndexScan* bitmapIndexScan = (BitmapIndexScan*)plan;
            extractQual(bitmapIndexScan->indexqualorig, plan, ancestors, rtable, subplans);
        } break;
        case T_CStoreIndexCtidScan: {
            CStoreIndexCtidScan* cStoreIndexCtidScan = (CStoreIndexCtidScan*)plan;
            extractQual(cStoreIndexCtidScan->indexqualorig, plan, ancestors, rtable, subplans);
            resSubplan = extractSubplan((Expr*)cStoreIndexCtidScan->scan.plan.targetlist,resSubplan, subplans);
        } break;
        case T_CStoreIndexScan: {
            resSubplan = extractNodeCStoreIndexScan(plan, ancestors, rtable, subplans);
        } break;
        case T_DfsIndexScan: {
            resSubplan = extractNodeDfsIndexScan(plan, ancestors, rtable, subplans);
        } break;

#ifdef PGXC
        case T_ModifyTable:
        case T_VecModifyTable: {
            ModifyTable* modify_table = (ModifyTable*)plan;
            resSubplan = extractSubplan((Expr*)modify_table->returningLists, resSubplan, subplans);
        } break;
        case T_VecRemoteQuery:
        case T_RemoteQuery: {
            RemoteQuery* remoteQuery = (RemoteQuery*)plan;
            extractQual(plan->qual, plan, ancestors, rtable, subplans);
            resSubplan = extractSubplan((Expr*)remoteQuery->scan.plan.targetlist, resSubplan, subplans);
            resSubplan = extractSubplan((Expr*)remoteQuery->scan.plan.qual, resSubplan, subplans);
        } break;
#endif
        case T_BitmapHeapScan: {
            resSubplan = extractNodeBitmapHeapScan(plan, ancestors, rtable, subplans);
        } break;
        case T_CStoreIndexHeapScan: {
            resSubplan = extractNodeCStoreIndexHeapScan(plan, ancestors, rtable, subplans);
        } break;
        case T_SeqScan: {
            resSubplan = extractNodeSeqScan(plan, ancestors, rtable, subplans);
        } break;
        case T_CStoreScan: {
            resSubplan = extractNodeCStoreScan(plan, ancestors, rtable, subplans);
        } break;
#ifdef ENABLE_MULTIPLE_NODES
        case T_TsStoreScan: {
            TsStoreScan* tsstoreScan = (TsStoreScan*)plan;
            extractQual(plan->qual, plan, ancestors, rtable, subplans);
            resSubplan = extractVecSubplan((Expr*)tsstoreScan->plan.targetlist, resSubplan, subplans);
        } break;
#endif   /* ENABLE_MULTIPLE_NODES */
        case T_ValuesScan: {
            resSubplan = extractNodeValuesScan(plan, ancestors, rtable, subplans);
        } break;
        case T_CteScan: {
            resSubplan = extractNodeCteScan(plan, ancestors, rtable, subplans);
        } break;
        case T_WorkTableScan: {
            resSubplan = extractNodeWorkTableScan(plan, ancestors, rtable, subplans);
        } break;
        case T_SubqueryScan: 
        case T_VecSubqueryScan:{
            extractQual(plan->qual, plan, ancestors, rtable, subplans);
        } break;
        case T_DfsScan: {
            resSubplan = extractNodeDfsScan(plan, ancestors, rtable, subplans);
        } break;
        case T_Stream:
        case T_VecStream: 
            break;
        case T_FunctionScan: {
            resSubplan = extractNodeFunctionScan(plan, ancestors, rtable, subplans);
        } break;
        case T_TidScan: {
            resSubplan = extractNodeTidScan(plan, ancestors, rtable, subplans);
        } break;
        case T_ForeignScan: {
            resSubplan = extractNodeForeignScan(plan, ancestors, rtable, subplans);
        } break;
        case T_VecForeignScan: {
            resSubplan = extractNodeVecForeignScan(plan, ancestors, rtable, subplans);
        } break;
        case T_VecNestLoop: {
            resSubplan = extractNodeVecNestLoop(plan, ancestors, rtable, subplans);
        } break;
        case T_NestLoop: {
            resSubplan = extractNodeNestLoop(plan, ancestors, rtable, subplans);
        } break;
        case T_VecMergeJoin: {
            resSubplan = extractNodeVecMergeJoin(plan, ancestors, rtable, subplans);
        } break;
        case T_MergeJoin: {
            resSubplan = extractNodeMergeJoin(plan, ancestors, rtable, subplans);
        } break;
        case T_VecHashJoin: {
            resSubplan = extractNodeVecHashJoin(plan, ancestors, rtable, subplans);            
        } break;
        case T_HashJoin: {
            resSubplan = extractNodeHashJoin(plan, ancestors, rtable, subplans); 
        } break;
        case T_VecAgg: {
            resSubplan = extractNodeVecAgg(plan, ancestors, rtable, subplans); 
        } break;
        case T_Agg: {
            resSubplan = extractNodeAgg(plan, ancestors, rtable, subplans);    
        } break;
        case T_Group: {
            resSubplan = extractNodeGroup(plan, ancestors, rtable, subplans);
        } break;
        case T_VecGroup: {
            resSubplan = extractNodeVecGroup(plan, ancestors, rtable, subplans);
        } break;
        case T_Sort: {
            /* hook for index_advisor */
            extractNodeSort(plan, rtable);
        } break;
        case T_VecSort:
        case T_MergeAppend: 
            break;
        case T_BaseResult: {
            resSubplan = extractNodeBaseResult(plan, ancestors, rtable, subplans); 
        } break;
        case T_VecResult: {
            resSubplan = extractNodeVecResult(plan, ancestors, rtable, subplans);
        } break;
        case T_Hash: {
            Hash* hash = (Hash*)plan;
            resSubplan = extractSubplan((Expr*)hash->plan.targetlist, resSubplan, subplans);
            resSubplan = extractSubplan((Expr*)hash->plan.qual, resSubplan, subplans);
        } break;
        case T_SetOp:
        case T_VecSetOp:  
        case T_RecursiveUnion:
            break;
        case T_WindowAgg: {
            WindowAgg* windowAgg = (WindowAgg*)plan;
            resSubplan = extractSubplan((Expr*)windowAgg->plan.targetlist, resSubplan, subplans);
        } break;
        case T_VecWindowAgg: {
            VecWindowAgg* vecWindowAgg = (VecWindowAgg*)plan;
            resSubplan = extractVecSubplan((Expr*)vecWindowAgg->plan.targetlist, resSubplan, subplans);
        } break;
        case T_Limit: {
            Limit* limit = (Limit*)plan;
            resSubplan = extractSubplan((Expr*)limit->limitOffset, resSubplan, subplans);
            resSubplan = extractSubplan((Expr*)limit->limitCount, resSubplan, subplans);
        } break;
        case T_VecLimit: {
            VecLimit* vecLimit = (VecLimit*)plan;
            resSubplan = extractSubplan((Expr*)vecLimit->limitOffset, resSubplan, subplans);
            resSubplan = extractSubplan((Expr*)vecLimit->limitCount, resSubplan, subplans);
        } break;
        default:
            break;
    }

    bool hasChildren = false;
    hasChildren = outerPlan(plan) || innerPlan(plan) ||
                  IsA(plan, ModifyTable) || IsA(plan, VecModifyTable) || IsA(plan, Append) || IsA(plan, VecAppend) ||
                  IsA(plan, MergeAppend) || IsA(plan, VecMergeAppend) || IsA(plan, BitmapAnd) || IsA(plan, BitmapOr) ||
                  IsA(plan, SubqueryScan) || IsA(plan, VecSubqueryScan || resSubplan);
    if (hasChildren) {
        ancestors = lcons(plan, ancestors);
    }

    if(plan->initPlan) {
        ListCell* lc = NULL;
        foreach (lc, plan->initPlan) {
            SubPlan* subPlan = (SubPlan*)lfirst(lc);
            Plan* plan = (Plan*)list_nth(subplans, subPlan->plan_id - 1);
            extractNode(plan, ancestors, rtable, subplans);
        }
    }

    /* lefttree */
    if (outerPlan(plan)) {
        extractNode(outerPlan(plan), ancestors, rtable, subplans);
    }

     /* righttree */
    if (innerPlan(plan)) {
        extractNode(innerPlan(plan), ancestors, rtable, subplans);
    }

    /* special child plans */
    extractSpecialPlans(plan, ancestors, rtable, subplans);
    
    if (resSubplan) {
        ListCell* lc = NULL;
        foreach (lc, resSubplan) {
            extractNode((Plan*)lfirst(lc), ancestors, rtable, subplans);
        }
    }

     /* end of child plans */
    if (hasChildren) {
        ancestors = list_delete_first(ancestors);
    }
}


static List* extractVecSubplan(Expr* node, List* resSubplan, List* subplans)
{
    if (node == NULL) {
        return resSubplan;
    }

    check_stack_depth();

    switch (nodeTag(node)) {
        case T_Aggref: {
            Aggref* aggref = (Aggref*)node;
            resSubplan = extractVecSubplan((Expr*)aggref->args, resSubplan, subplans);
        } break;
        case T_GroupingFunc:
        case T_GroupingId:
        case T_WindowFunc: {
            WindowFunc* wfunc = (WindowFunc*)node;
            resSubplan = extractVecSubplan((Expr*)wfunc->args, resSubplan, subplans);
        } break;
        case T_FuncExpr: {
            FuncExpr* funcexpr = (FuncExpr*)node;
            resSubplan = extractVecSubplan((Expr*)funcexpr->args, resSubplan, subplans);
        } break;
        case T_OpExpr: {
            OpExpr* opexpr = (OpExpr*)node;
            resSubplan = extractVecSubplan((Expr*)opexpr->args, resSubplan, subplans);
        } break;
        case T_DistinctExpr: {
            DistinctExpr* distexpr = (DistinctExpr*)node;
            resSubplan = extractVecSubplan((Expr*)distexpr->args, resSubplan, subplans);
        } break;
        case T_NullIfExpr: {
            NullIfExpr* nullifexpr = (NullIfExpr*)node;
            resSubplan = extractVecSubplan((Expr*)nullifexpr->args, resSubplan, subplans);
        } break;
        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* opexpr = (ScalarArrayOpExpr*)node;
            resSubplan = extractVecSubplan((Expr*)opexpr->args, resSubplan, subplans);
        } break;
        case T_BoolExpr: {
            BoolExpr* boolexpr = (BoolExpr*)node;
            resSubplan = extractVecSubplan((Expr*)boolexpr->args, resSubplan, subplans);
        } break;
        case T_SubPlan: {
            SubPlan* subplan = (SubPlan*)node;
            if (STREAM_RECURSIVECTE_SUPPORTED && subplan->subLinkType == CTE_SUBLINK) {
                Plan* curr_subplan = (Plan*)list_nth(subplans, subplan->plan_id - 1);
                resSubplan = lappend(resSubplan, curr_subplan);
            }            
        } break;
        case T_RelabelType: {
            RelabelType* relabel = (RelabelType*)node;
            resSubplan = extractVecSubplan(relabel->arg, resSubplan, subplans);
        } break;
        case T_CoerceViaIO: {
            CoerceViaIO* iocoerce = (CoerceViaIO*)node;
            resSubplan = extractVecSubplan(iocoerce->arg, resSubplan, subplans);
        } break;
        case T_CaseExpr: {
            CaseExpr* caseexpr = (CaseExpr*)node;
            ListCell* l = NULL;
            resSubplan = extractVecSubplan(caseexpr->arg, resSubplan, subplans);
            foreach (l, caseexpr->args) {
                CaseWhen* when = (CaseWhen*)lfirst(l);
                Assert(IsA(when, CaseWhen));
                resSubplan = extractVecSubplan(when->expr, resSubplan, subplans);
                resSubplan = extractVecSubplan(when->result, resSubplan, subplans);
            }
        } break;
        case T_CoalesceExpr: {
            CoalesceExpr* coalesceexpr = (CoalesceExpr*)node;
            resSubplan = extractVecSubplan((Expr*)coalesceexpr->args, resSubplan, subplans);
        } break;
        case T_MinMaxExpr: {
            MinMaxExpr* minmaxexpr = (MinMaxExpr*)node;
            resSubplan = extractVecSubplan((Expr*)minmaxexpr->args, resSubplan, subplans);
        } break;
        case T_NullTest: {
            NullTest* ntest = (NullTest*)node;
            resSubplan = extractVecSubplan(ntest->arg, resSubplan, subplans);
        } break;
        case T_HashFilter: {
            HashFilter* htest = (HashFilter*)node;
            resSubplan = extractVecSubplan((Expr*)htest->arg, resSubplan, subplans);
        } break;
        case T_BooleanTest: {
            BooleanTest* btest = (BooleanTest*)node;
            resSubplan = extractVecSubplan(btest->arg, resSubplan, subplans);
        } break;
        case T_TargetEntry: {
            TargetEntry* tle = (TargetEntry*)node;
            resSubplan = extractVecSubplan(tle->expr, resSubplan, subplans);
        } break;
        case T_List: {
            ListCell* l = NULL;
            foreach (l, (List*)node) {
                resSubplan = extractVecSubplan((Expr*)lfirst(l), resSubplan, subplans);
            }
        } break;
        case T_RowExpr: {
            RowExpr* rowexpr = (RowExpr*)node;
            Form_pg_attribute* attrs = NULL;
            ListCell* l = NULL;
            int i;
            TupleDesc tupdesc = NULL;

            // RowExpr is for the expansion of the function of redistribution,
            // in the normal use(u_sess->attr.attr_sql.enable_cluster_resize = false), RowExpr need to be forbidden
            if (u_sess->attr.attr_sql.enable_cluster_resize == false) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Unsupported rowexpr expression in vector engine")));
            }
                
            /* Build tupdesc to describe result tuples */
            if (rowexpr->row_typeid == RECORDOID) {
                /* generic record, use runtime type assignment */
                tupdesc = ExecTypeFromExprList(rowexpr->args, rowexpr->colnames);
            } else {
                /* it's been cast to a named type, use that */
                tupdesc = lookup_rowtype_tupdesc_copy(rowexpr->row_typeid, -1);
            }

            /* Set up evaluation, skipping any deleted columns */
            Assert(list_length(rowexpr->args) <= tupdesc->natts);
            attrs = tupdesc->attrs;
            i = 0;

            foreach (l, rowexpr->args) {
                Expr* e = (Expr*)lfirst(l);

                if (!attrs[i]->attisdropped) {
                    /*
                     * Guard against ALTER COLUMN TYPE on rowtype since
                     * the RowExpr was created.  XXX should we check
                     * typmod too?	Not sure we can be sure it'll be the
                     * same.
                     */
                    if (exprType((Node*)e) != attrs[i]->atttypid) {
                        ereport(ERROR,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                                errmsg("ROW() column has type %s instead of type %s",
                                    format_type_be(exprType((Node*)e)),
                                    format_type_be(attrs[i]->atttypid))));
                    }
                } else {
                    /*
                     * Ignore original expression and insert a NULL. We
                     * don't really care what type of NULL it is, so
                     * always make an int4 NULL.
                     */
                    e = (Expr*)makeNullConst(INT4OID, -1, InvalidOid);
                }
                resSubplan = extractVecSubplan(e, resSubplan, subplans);
                i++;
            }
        } break;
        case T_RowCompareExpr: {
            RowCompareExpr* rcexpr = (RowCompareExpr*)node;
            resSubplan = extractVecSubplan((Expr*)rcexpr->largs, resSubplan, subplans);
            resSubplan = extractVecSubplan((Expr*)rcexpr->rargs, resSubplan, subplans);  
        } break;
        case T_ConvertRowtypeExpr:
        case T_ArrayExpr:
        case T_AlternativeSubPlan: 
        case T_FieldSelect:
        case T_FieldStore:
        case T_CoerceToDomain:
        case T_CoerceToDomainValue:
        case T_CurrentOfExpr:
        case T_ArrayRef:
        case T_XmlExpr:
        case T_ArrayCoerceExpr:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_ADVISOR),
                    errmsg("Unsupported type %d in vector engine",  (int)nodeTag(node))));
        default:
            break;
    }

    return resSubplan;
}

static List* extractSubplan(Expr* node, List* resSubplan, List* subplans)
{
    if (node == NULL) {
        return NIL;
    }

    check_stack_depth();

    switch (nodeTag(node)) {
        case T_Var:
        case T_Const:
        case T_Param:
        case T_CoerceToDomainValue:
        case T_CaseTestExpr:
        case T_GroupingFunc:
        case T_GroupingId:
        case T_AlternativeSubPlan:
        case T_CurrentOfExpr:
            break;
        case T_Aggref: {
            Aggref* aggref = (Aggref*)node;
            resSubplan = extractSubplan((Expr*)aggref->aggdirectargs, resSubplan, subplans);
            resSubplan = extractSubplan((Expr*)aggref->args, resSubplan, subplans);
        } break;
        case T_WindowFunc: {
            WindowFunc* wfunc = (WindowFunc*)node;
            resSubplan = extractSubplan((Expr*)wfunc->args, resSubplan, subplans);
        } break;
        case T_ArrayRef: {
            ArrayRef* aref = (ArrayRef*)node;
            resSubplan = extractSubplan((Expr*)aref->refupperindexpr, resSubplan, subplans);
            resSubplan = extractSubplan((Expr*)aref->reflowerindexpr, resSubplan, subplans);
            resSubplan = extractSubplan(aref->refexpr, resSubplan, subplans);
            resSubplan = extractSubplan(aref->refassgnexpr, resSubplan, subplans);
        } break;
        case T_FuncExpr: {
            FuncExpr* funcexpr = (FuncExpr*)node;
            resSubplan = extractSubplan((Expr*)funcexpr->args, resSubplan, subplans);
        } break;
        case T_OpExpr: {
            OpExpr* opexpr = (OpExpr*)node;
            resSubplan = extractSubplan((Expr*)opexpr->args, resSubplan, subplans);
        } break;
        case T_DistinctExpr: {
            DistinctExpr* distinctexpr = (DistinctExpr*)node;
            resSubplan = extractSubplan((Expr*)distinctexpr->args, resSubplan, subplans);
        } break;
        case T_NullIfExpr: {
            NullIfExpr* nullifexpr = (NullIfExpr*)node;
            resSubplan = extractSubplan((Expr*)nullifexpr->args, resSubplan, subplans);
        } break;
        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* opexpr = (ScalarArrayOpExpr*)node;
            resSubplan = extractSubplan((Expr*)opexpr->args, resSubplan, subplans);
        } break;
        case T_BoolExpr: {
            BoolExpr* boolexpr = (BoolExpr*)node;
            resSubplan = extractSubplan((Expr*)boolexpr->args, resSubplan, subplans);
        } break;
        case T_SubPlan: {
            SubPlan* subplan = (SubPlan*)node;
            if (STREAM_RECURSIVECTE_SUPPORTED && subplan->subLinkType == CTE_SUBLINK) {
                Plan* curr_subplan = (Plan*)list_nth(subplans, subplan->plan_id - 1);
                resSubplan = lappend(resSubplan, curr_subplan);
            }
            
            resSubplan = extractSubplan((Expr*)subplan->testexpr, resSubplan, subplans);
            resSubplan = extractSubplan((Expr*)subplan->args, resSubplan, subplans);
        } break;
        case T_FieldSelect: {
            FieldSelect* fselect = (FieldSelect*)node;
            resSubplan = extractSubplan((Expr*)fselect->arg, resSubplan, subplans);
        } break;
        case T_FieldStore: {
            FieldStore* fstore = (FieldStore*)node;
            resSubplan = extractSubplan((Expr*)fstore->arg, resSubplan, subplans);
            resSubplan = extractSubplan((Expr*)fstore->newvals, resSubplan, subplans);
        } break;
        case T_RelabelType: {
            RelabelType* relabel = (RelabelType*)node;
            resSubplan = extractSubplan((Expr*)relabel->arg, resSubplan, subplans);
        } break;
        case T_CoerceViaIO: {
            CoerceViaIO* iocoerce = (CoerceViaIO*)node;
            resSubplan = extractSubplan((Expr*)iocoerce->arg, resSubplan, subplans);
        } break;
        case T_ArrayCoerceExpr: {
            ArrayCoerceExpr* acoerce = (ArrayCoerceExpr*)node;
            resSubplan = extractSubplan((Expr*)acoerce->arg, resSubplan, subplans);
        } break;
        case T_ConvertRowtypeExpr: {
            ConvertRowtypeExpr* convert = (ConvertRowtypeExpr*)node;
            resSubplan = extractSubplan((Expr*)convert->arg, resSubplan, subplans);
        } break;
        case T_CaseExpr: {
            CaseExpr* caseexpr = (CaseExpr*)node;
            ListCell* l = NULL;
            resSubplan = extractSubplan(caseexpr->arg, resSubplan, subplans);
            foreach (l, caseexpr->args) {
                CaseWhen* when = (CaseWhen*)lfirst(l);
                Assert(IsA(when, CaseWhen));
                resSubplan = extractSubplan((Expr*)when->expr, resSubplan, subplans);
                resSubplan = extractSubplan((Expr*)when->result, resSubplan, subplans);
            }
            resSubplan = extractSubplan((Expr*)caseexpr->defresult, resSubplan, subplans);
        } break;
        case T_ArrayExpr: {
            ArrayExpr* arrayexpr = (ArrayExpr*)node;
            resSubplan = extractSubplan((Expr*)arrayexpr->elements, resSubplan, subplans);
        } break;
        case T_RowExpr: {
            RowExpr* rowexpr = (RowExpr*)node;
            Form_pg_attribute* attrs = NULL;
            ListCell* l = NULL;
            int i;
            TupleDesc tupdesc = NULL;

            /* Build tupdesc to describe result tuples */
            if (rowexpr->row_typeid == RECORDOID) {
                /* generic record, use runtime type assignment */
                tupdesc = ExecTypeFromExprList(rowexpr->args, rowexpr->colnames);
            } else {
                /* it's been cast to a named type, use that */
                tupdesc = lookup_rowtype_tupdesc_copy(rowexpr->row_typeid, -1);
            }
            /* Set up evaluation, skipping any deleted columns */
            Assert(list_length(rowexpr->args) <= tupdesc->natts);
            attrs = tupdesc->attrs;
            i = 0;

            foreach (l, rowexpr->args) {
                Expr* e = (Expr*)lfirst(l);
                if (!attrs[i]->attisdropped) {
                    /*
                     * Guard against ALTER COLUMN TYPE on rowtype since
                     * the RowExpr was created.  XXX should we check
                     * typmod too?	Not sure we can be sure it'll be the
                     * same.
                     */
                    if (exprType((Node*)e) != attrs[i]->atttypid)
                        ereport(ERROR,
                            (errcode(ERRCODE_DATATYPE_MISMATCH),
                                errmsg("ROW() column has type %s instead of type %s",
                                    format_type_be(exprType((Node*)e)),
                                    format_type_be(attrs[i]->atttypid))));
                } else {
                    /*
                     * Ignore original expression and insert a NULL. We
                     * don't really care what type of NULL it is, so
                     * always make an int4 NULL.
                     */
                    e = (Expr*)makeNullConst(INT4OID, -1, InvalidOid);
                }
                resSubplan = extractSubplan(e, resSubplan, subplans);
                i++;
            }
        } break;
        case T_RowCompareExpr: {
            RowCompareExpr* rcexpr = (RowCompareExpr*)node;
            resSubplan = extractSubplan((Expr*)rcexpr->largs, resSubplan, subplans);
            resSubplan = extractSubplan((Expr*)rcexpr->rargs, resSubplan, subplans);
        } break;
        case T_CoalesceExpr: {
            CoalesceExpr* coalesceexpr = (CoalesceExpr*)node;
            resSubplan = extractSubplan((Expr*)coalesceexpr->args, resSubplan, subplans);
        } break;
        case T_MinMaxExpr: {
            MinMaxExpr* minmaxexpr = (MinMaxExpr*)node;
            resSubplan = extractSubplan((Expr*)minmaxexpr->args, resSubplan, subplans);
        } break;
        case T_XmlExpr: {
            XmlExpr* xexpr = (XmlExpr*)node;
            resSubplan = extractSubplan((Expr*)xexpr->named_args, resSubplan, subplans);
            resSubplan = extractSubplan((Expr*)xexpr->args, resSubplan, subplans);
        } break;
        case T_NullTest: {
            NullTest* ntest = (NullTest*)node;
            resSubplan = extractSubplan((Expr*)ntest->arg, resSubplan, subplans);
        } break;
        case T_HashFilter: {
            HashFilter* htest = (HashFilter*)node;
            resSubplan = extractSubplan((Expr*)htest->arg, resSubplan, subplans);
        } break;
        case T_BooleanTest: {
            BooleanTest* btest = (BooleanTest*)node;
            resSubplan = extractSubplan((Expr*)btest->arg, resSubplan, subplans);
        } break;
        case T_CoerceToDomain: {
            CoerceToDomain* ctest = (CoerceToDomain*)node;
            resSubplan = extractSubplan((Expr*)ctest->arg, resSubplan, subplans);
        } break;
        case T_TargetEntry: {
            TargetEntry* tle = (TargetEntry*)node;
            resSubplan = extractSubplan((Expr*)tle->expr, resSubplan, subplans);
        } break;
        case T_List: {
            ListCell* l = NULL;
            foreach (l, (List*)node) {
                resSubplan = extractSubplan((Expr*)lfirst(l), resSubplan, subplans);
            }
        }
        default:
            break;
    }
    return resSubplan;
}

/*
 * Show the group by keys for a Agg node.
 * Note: we don't extract groupingSets yet.
 */
static void extractGroupbyKeys(Plan* plan, int nkeys, AttrNumber* keycols, List* ancestors,
                               List* rtable, List* subplans)
{
    if (u_sess->adv_cxt.getPlanInfoFunc != NULL) {
        return;
    }

    List* dpcontext = NIL;
    int keyno;
    RecursiveExtractContext context;
    /* The key columns refer to the tlist of the child plan */
    ancestors = lcons(plan, ancestors);
    /* Set up deparsing context */
    dpcontext = deparseContextForExtractPlan((Node*)plan, ancestors, subplans);
    initRecursiveExtractContext(&context, dpcontext, rtable, subplans);

    for (keyno = 0; keyno < nkeys; keyno++) {
        /* find key expression in tlist */
        AttrNumber keyresno = keycols[keyno];
        TargetEntry* target = get_tle_by_resno(plan->targetlist, keyresno);

        if (target == NULL)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no tlist entry for key %d", keyresno)));
        
        /*
         * group by column may by const or opexpr. such as:
         * select 1 + 1 as c1, sum(10) from t1 where c1 = '1' group by 1
         * select t1.a + t2.a from t1 join t2 on t1.b = t2.b group by 1
         */
        if (nodeTag((Node*)target->expr) == T_Var) {
            extractRuleExpr((Node*)target->expr, &context);
        }
    }
    double rowsWeight = plan->plan_rows;
    ListCell* l = NULL;
    foreach (l, context.args) {
        Node* node = (Node*)lfirst(l);
        if (nodeTag(node) == T_Var) {
            Var* var = (Var*)node;
            RangeTblEntry* rte = rt_fetch(var->varno, context.rtable);
            insertGrpby2CandicateTable(rte, var, rowsWeight);
        }
    }

    ancestors = list_delete_first(ancestors);
}

/*
 * extract a qualifier expression (which is a List with implicit AND semantics)
 */
static void extractQual(List* qual, Plan* plan, List* ancestors, List* rtable, List* subplans)
{
    Node* node = NULL;
    List* dpcontext = NIL;

    /* No work if empty qual */
    if (qual == NIL)
        return;

    /* Convert AND list to explicit AND */
    node = (Node*)make_ands_explicit(qual);
    /* Set up deparsing context */
    dpcontext = deparseContextForExtractPlan((Node*)plan, ancestors, subplans);
    RecursiveExtractContext context;
    initRecursiveExtractContext(&context, dpcontext, rtable, subplans);
    context.rowsWeight = getPlanRows(plan);
    extractRuleExpr(node, &context);
}

static void initRecursiveExtractContext(RecursiveExtractContext* context, List* dpcontext, List* rtable, List* subplans)
{
    context->namespaces = dpcontext;
    context->rtable = rtable;
    context->subplans = subplans;
    context->rowsWeight = 0.0;
    context->args = NIL;
    context->expr = NIL;
    context->exprs = NIL;
}

static double getPlanRows(Plan* plan)
{
    double rowsWeight = 0.0;
    switch (nodeTag(plan)) {
        case T_VecNestLoop:
        case T_NestLoop:
        case T_VecMergeJoin:
        case T_MergeJoin: 
        case T_VecHashJoin:
        case T_HashJoin: {
            rowsWeight = outerPlan(plan)->plan_rows + innerPlan(plan)->plan_rows;
        } break;
        default:
            break;
    }

    return rowsWeight;
}

static void extractRuleExpr(Node* node, RecursiveExtractContext* context)
{
    if (node == NULL)
        return;
    ListCell* l = NULL;
    /*
     * Each level of get_rule_expr must emit an indivisible term
     * (parenthesized if necessary) to ensure result is reparsed into the same
     * expression tree.  The only exception is that when the input is a List,
     * we emit the component items comma-separated with no surrounding
     * decoration; this is convenient for most callers.
     */
    switch (nodeTag(node)) {
        case T_Var: {
            extractVariable((Var*)node, 0, context);
        } break;
        case T_Const: {
            extractConstExpr((Const*)node, context);
        } break;
        case T_Aggref: {
            extractAggExpr((Aggref*)node, context);
        } break;
        case T_Param: {
            extractParameter((Param*)node, context);
        } break;
        case T_OpExpr: {
            extractOperExpr(node, context);
            if (context->expr != NULL) {
                fillinAdviseTable((Node*)linitial(context->expr), context->rtable, context->rowsWeight);
            }
        } break;
        case T_ScalarArrayOpExpr: {
            extractOperExpr(node, context);
            if (context->expr != NULL) {
                fillinAdviseTable((Node*)linitial(context->expr), context->rtable, 0.0);
            }
        } break;
        case T_BoolExpr: {
            BoolExpr* expr = (BoolExpr*)node;
            foreach (l, expr->args) {
                extractRuleExpr((Node*)lfirst(l), context);
                context->exprs = list_concat(context->exprs, context->expr);
                context->expr = NIL;
            }

            BoolExpr* copy_bool = (BoolExpr*)copyObject(expr);
            pfree_ext(copy_bool->args);
            copy_bool->args = context->exprs;

            fillinAdviseTable((Node*)copy_bool, context->rtable, 0.0);
        }
            break;
        case T_RelabelType: {
            RelabelType* relabel = (RelabelType*)node;
            Node* arg = (Node*)relabel->arg;
            extractRuleExpr(arg, context);
        } break;
        case T_List: {
            foreach (l, (List*)node) {
                extractRuleExpr((Node*)lfirst(l), context);
                context->exprs = list_concat(context->exprs, context->expr);
                context->expr = NIL;
            }
        } break;
        default:
            break;
    }
}

static void extractConstExpr(Const* constVal, RecursiveExtractContext* context)
{
    context->args = lappend(context->args, (Const*)copyObject(constVal));
}

static bool IsExtractOpExprArgs(Node* node)
{
    bool result = false;
    switch (nodeTag(node)) {
        case T_Var:
        case T_Const:
        case T_Param: {
            result = true;
        } break;
        default: {
            result = false;
        } break;
    }
    return result; 
}

static void extractOperExpr(Node* node, RecursiveExtractContext* context)
{
    List* args = NIL;

    if (nodeTag(node) == T_OpExpr) {
        args = ((OpExpr*)node)->args;
    } else if (nodeTag(node) == T_ScalarArrayOpExpr) {
        args = ((ScalarArrayOpExpr*)node)->args;
    }

    if (list_length(args) == 2) {
        /* binary operator */
        Node* arg1 = (Node*)linitial(args);
        Node* arg2 = (Node*)lsecond(args);
        extractRuleExpr(arg1, context);
        extractRuleExpr(arg2, context);

        if (list_length(context->args) == 2 && IsExtractOpExprArgs(arg1) && IsExtractOpExprArgs(arg2)) {
            Node* extractArg1 = (Node*)linitial(context->args);
            Node* extractArg2 = (Node*)lsecond(context->args);

            if (nodeTag(node) == T_OpExpr) {
                OpExpr* copyOpExpr = (OpExpr*)copyObject(node);
                pfree_ext(copyOpExpr->args);
                copyOpExpr->args = lappend(copyOpExpr->args, extractArg1);
                copyOpExpr->args = lappend(copyOpExpr->args, extractArg2);
                context->expr = lappend(context->expr, (Node*)copyOpExpr);
            } else if (nodeTag(node) == T_ScalarArrayOpExpr) {
                ScalarArrayOpExpr* copyOpExpr = (ScalarArrayOpExpr*)copyObject(node);
                pfree_ext(copyOpExpr->args);
                copyOpExpr->args = lappend(copyOpExpr->args, extractArg1);
                copyOpExpr->args = lappend(copyOpExpr->args, extractArg2);
                context->expr = lappend(context->expr, (Node*)copyOpExpr);
            }
        }    
    } 

    list_free_ext(context->args);
}

static void extractVariable(Var* var, int levelsup, RecursiveExtractContext* context)
{
    RangeTblEntry* rte = NULL;
    AttrNumber attnum = InvalidAttrNumber;
    int netlevelsup;
    RecursiveExtractNamespace* dpetns = NULL;
    List* subplans = NIL;

    /* Find appropriate nesting depth */
    netlevelsup = var->varlevelsup + levelsup;
    if (netlevelsup >= list_length(context->namespaces)) {
            return ;
    }
    dpetns = (RecursiveExtractNamespace*)list_nth(context->namespaces, netlevelsup);
    subplans = context->subplans;
    /*
     * Try to find the relevant RTE in this rtable.  In a plan tree, it's
     * likely that varno is OUTER_VAR or INNER_VAR, in which case we must dig
     * down into the subplans, or INDEX_VAR, which is resolved similarly.
     */
#ifdef PGXC
    if (dpetns->remotequery) {
        rte = rt_fetch(1, context->rtable);
        attnum = var->varattno;
    } else
#endif
        if (var->varno >= 1 && var->varno <= (uint)list_length(context->rtable)) {
            rte = rt_fetch(var->varno, context->rtable);
            if (rte->rtekind == RTE_RELATION) {
                context->args = lappend(context->args, (Var*)copyObject(var));
            }
            return ;
    } else if ((var->varno == OUTER_VAR) && dpetns->outerTlist) {
        TargetEntry* tle = NULL;
        RecursiveExtractNamespace saveDpetns;
        tle = get_tle_by_resno(dpetns->outerTlist, var->varattno);
        if (tle == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("bogus varattno for OUTER_VAR var: %d", var->varattno)));

        Assert(netlevelsup == 0);
        pushExtractChildPlan(dpetns, dpetns->outerPlan, &saveDpetns, subplans);
        /*
         * Force parentheses because our caller probably assumed a Var is a
         * simple expression.
         */
        if (IsA(tle->expr, Var)) {
            extractRuleExpr((Node*)tle->expr, context);
        }
        popExtractChildPlan(dpetns, &saveDpetns);
        return ;
    } else if (var->varno == INNER_VAR && dpetns->innerTlist) {
        TargetEntry* tle = NULL;
        RecursiveExtractNamespace saveDpetns;
        tle = get_tle_by_resno(dpetns->innerTlist, var->varattno);
        if (tle == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("bogus varattno for INNER_VAR var: %d", var->varattno)));

        Assert(netlevelsup == 0);
        pushExtractChildPlan(dpetns, dpetns->innerPlan, &saveDpetns, subplans);
        /*
         * Force parentheses because our caller probably assumed a Var is a
         * simple expression.
         */
        if (IsA(tle->expr, Var)) {
            extractRuleExpr((Node*)tle->expr, context);
        }
        popExtractChildPlan(dpetns, &saveDpetns);
        return ;
    } else if (var->varno == INDEX_VAR && dpetns->indexTlist) {
        TargetEntry* tle = NULL;
        tle = get_tle_by_resno(dpetns->indexTlist, var->varattno);
        if (tle == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("bogus varattno for INDEX_VAR var: %d", var->varattno)));

        Assert(netlevelsup == 0);
        /*
         * Force parentheses because our caller probably assumed a Var is a
         * simple expression.
         */
        if (IsA(tle->expr, Var))
            extractRuleExpr((Node*)tle->expr, context);

        return ;
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("bogus varno: %d", var->varno)));
        return ; /* keep compiler quiet */
    }

    /*
     * The planner will sometimes emit Vars referencing resjunk elements of a
     * subquery's target list (this is currently only possible if it chooses
     * to generate a "physical tlist" for a SubqueryScan or CteScan node).
     * Although we prefer to print subquery-referencing Vars using the
     * subquery's alias, that's not possible for resjunk items since they have
     * no alias.  So in that case, drill down to the subplan and print the
     * contents of the referenced tlist item.  This works because in a plan
     * tree, such Vars can only occur in a SubqueryScan or CteScan node, and
     * we'll have set dpns->innerPlan to reference the child plan node.
     */
    if ((rte->rtekind == RTE_SUBQUERY || rte->rtekind == RTE_CTE) && attnum > list_length(rte->eref->colnames) &&
        dpetns->innerPlan) {
        return ;
    }

#ifdef PGXC
    if (rte->rtekind == RTE_REMOTE_DUMMY && attnum > list_length(rte->eref->colnames) && dpetns->plan) {
        return ;
    }
#endif /* PGXC */
}

static void extractAggExpr(Aggref* aggref, RecursiveExtractContext* context)
{
    if (aggref->aggkind != 'o') {
        ListCell* l = NULL;
        foreach (l, aggref->args) {
            TargetEntry* tle = (TargetEntry*)lfirst(l);
            if (tle->resjunk)
                continue;

            extractRuleExpr((Node*)tle->expr, context);
        }
    }
}

/*
 * Try to find the referenced expression for a PARAM_EXEC Param that might
 * reference a parameter supplied by an upper NestLoop or SubPlan plan node.
 *
 * If successful, return the expression and set *dpetns and *ancestorCell
 * appropriately for calling push_ancestor_plan().	If no referent can be
 * found, return NULL.
 */
static Node* findParamReferent(Param* param, RecursiveExtractContext* context,
                                RecursiveExtractNamespace** dpetnsParam, ListCell** ancestorCellParam)
{
    *dpetnsParam = NULL;
    *ancestorCellParam = NULL;
        /*
     * If it's a PARAM_EXEC parameter, look for a matching NestLoopParam or
     * SubPlan argument.  This will necessarily be in some ancestor of the
     * current expression's PlanState.
     */
    if (param->paramkind == PARAM_EXEC) {
        RecursiveExtractNamespace* dpns = NULL;
        Plan* childPlan = NULL;
        bool inSamePlanLevel = false;
        ListCell* lc = NULL;

        dpns = (RecursiveExtractNamespace*)linitial(context->namespaces);
        childPlan = dpns->plan;
        inSamePlanLevel = true;

        foreach (lc, dpns->ancestors) {
            Plan* plan = (Plan*)lfirst(lc);
            ListCell* lc2 = NULL;

            /*
             * NestLoops transmit params to their inner child only; also, once
             * we've crawled up out of a subplan, this couldn't possibly be
             * the right match.
             */
            if (IsA(plan, NestLoop) && childPlan == innerPlan(plan) && inSamePlanLevel) {
                NestLoop* nl = (NestLoop*)plan;

                foreach (lc2, nl->nestParams) {
                    NestLoopParam* nlp = (NestLoopParam*)lfirst(lc2);

                    if (nlp->paramno == param->paramid) {
                        /* Found a match, so return it */
                        *dpetnsParam = dpns;
                        *ancestorCellParam = lc;
                        context->rowsWeight = getPlanRows(plan);
                        return (Node*)nlp->paramval;
                    }
                }
            }

            /*
             * Check to see if we're crawling up from a subplan.
             */
            foreach (lc2, context->subplans) {
                SubPlan* subplan = (SubPlan*)lfirst(lc2);
                ListCell* lc3 = NULL;
                ListCell* lc4 = NULL;

                if (childPlan != (Plan*)subplan)
                    continue;

                /* Matched subplan, so check its arguments */
                forboth(lc3, subplan->parParam, lc4, subplan->args)
                {
                    int paramid = lfirst_int(lc3);
                    Node* arg = (Node*)lfirst(lc4);

                    if (paramid == param->paramid) {
                        /* Found a match, so return it */
                        *dpetnsParam = dpns;
                        *ancestorCellParam = lc;
                        context->rowsWeight = getPlanRows(plan);
                        return arg;
                    }
                }

                /* Keep looking, but we are emerging from a subplan. */
                inSamePlanLevel = false;
                break;
            }

            /*
             * Likewise check to see if we're emerging from an initplan.
             * Initplans never have any parParams, so no need to search that
             * list, but we need to know if we should reset
             * inSamePlanLevel.
             */
            foreach (lc2, plan->initPlan) {
                SubPlan* subPlan = (SubPlan*)lfirst(lc2);

                if (childPlan != (Plan*)subPlan)
                    continue;

                /* Keep looking, but we are emerging from an initplan. */
                inSamePlanLevel = false;
                break;
            }

            /* No luck, crawl up to next ancestor */
            childPlan = plan;
        }
    }

    /* No referent found */
    return NULL;
}

static void pushAncestorPlan(RecursiveExtractNamespace* dpetns, ListCell* ancestorCell,
                                RecursiveExtractNamespace* saveDpetns, List* subplans)
{
    Plan* plan = (Plan*)lfirst(ancestorCell);
    List* ancestors = NIL;

    /* Save state for restoration later */
    *saveDpetns = *dpetns;

    /* Build a new ancestor list with just this node's ancestors */
    ancestors = NIL;
    while ((ancestorCell = lnext(ancestorCell)) != NULL)
        ancestors = lappend(ancestors, lfirst(ancestorCell));
    dpetns->ancestors = ancestors;

    /* Set attention on selected ancestor */
    setDeparseExtractPlan(dpetns, plan, subplans);
}

static void popAncestorPlan(RecursiveExtractNamespace* dpetns, RecursiveExtractNamespace* saveDpetns)
{
    /* Free the ancestor list made in push_ancestor_plan */
    list_free_ext(dpetns->ancestors);

    /* Restore fields changed by push_ancestor_plan */
    *dpetns = *saveDpetns;
}

static void extractParameter(Param* param, RecursiveExtractContext* context)
{
    Node* expr = NULL;
    RecursiveExtractNamespace* dpetns = NULL;
    ListCell* ancestorCell = NULL;
    expr = findParamReferent(param, context, &dpetns, &ancestorCell);
    if (expr != NULL) {
        RecursiveExtractNamespace saveDpetns;
        /* Switch attention to the ancestor plan node */
        pushAncestorPlan(dpetns, ancestorCell, &saveDpetns, context->subplans);
        extractRuleExpr(expr, context);
        popAncestorPlan(dpetns, &saveDpetns);
    }
}

static void pushExtractChildPlan(RecursiveExtractNamespace* dpetns, Plan* plan,
                                 RecursiveExtractNamespace* saveDpetns, List* subplans)
{
    /* Save state for restoration later */
    *saveDpetns = *dpetns;
    /*
     * Currently we don't bother to adjust the ancestors list, because an
     * OUTER_VAR or INNER_VAR reference really shouldn't contain any Params
     * that would be set by the parent node itself.  If we did want to adjust
     * the list, lcons'ing dpns->planstate onto dpns->ancestors would be the
     * appropriate thing --- and pop_child_plan would need to undo the change
     * to the list.
     */
    /* Set attention on selected child */
    setDeparseExtractPlan(dpetns, plan, subplans);
}

/*
 * pop_child_plan: undo the effects of push_child_plan
 */
static void popExtractChildPlan(RecursiveExtractNamespace* dpetns, RecursiveExtractNamespace* saveDpetns)
{
    /* Restore fields changed by push_child_plan */
    *dpetns = *saveDpetns;
}


/* reference deparse_context_for_planstate */
List* deparseContextForExtractPlan(Node* plan, List* ancestors, List* subplans)
{
    RecursiveExtractNamespace* dpetns = NULL;
    dpetns = (RecursiveExtractNamespace*)palloc0(sizeof(RecursiveExtractNamespace));
    /* Initialize fields that stay the same across the whole plan tree */
    dpetns->ctes = NIL;
#ifdef PGXC
    dpetns->remotequery = false;
#endif
    /* Set our attention on the specific plan node passed in */
    setDeparseExtractPlan(dpetns, (Plan*)plan, subplans);
    dpetns->ancestors = ancestors;
    /* Return a one-deep namespace stack */
    return list_make1(dpetns);
}

/* reference set_deparse_planstate */
static void setDeparseExtractPlan(RecursiveExtractNamespace* dpetns, Plan* plan, List* subplans)
{
    dpetns->plan = plan;
    assignOuterPlan(dpetns, plan);
    assignInnerPlan(dpetns, plan, subplans);

    /* indexTlist is set only if it's an IndexOnlyScan */
    if (IsA(plan, IndexOnlyScan)) {
        dpetns->indexTlist = ((IndexOnlyScan*)plan)->indextlist;
    } else if (IsA(plan, ExtensiblePlan)) {
        dpetns->indexTlist = ((ExtensiblePlan*)plan)->extensible_plan_tlist;
    } else {
        dpetns->indexTlist = NIL;
    }
}

static void assignOuterPlan(RecursiveExtractNamespace* dpetns, Plan* plan)
{
    /*
     * We special-case Append and MergeAppend to pretend that the first child
     * plan is the OUTER referent; we have to interpret OUTER Vars in their
     * tlists according to one of the children, and the first one is the most
     * natural choice.	Likewise special-case ModifyTable to pretend that the
     * first child plan is the OUTER referent; this is to support RETURNING
     * lists containing references to non-target relations.
     */
    if (IsA(plan, Append)) {
        dpetns->outerPlan = (Plan*)linitial(((Append*)plan)->appendplans);
    } else if (IsA(plan, VecAppend)) {
        dpetns->outerPlan = (Plan*)linitial(((VecAppend*)plan)->appendplans);
    } else if (IsA(plan, MergeAppend)) {
        dpetns->outerPlan = (Plan*)linitial(((MergeAppend*)plan)->mergeplans);
    } else if (IsA(plan, ModifyTable)) {
        dpetns->outerPlan = (Plan*)linitial(((ModifyTable*)plan)->plans);
    } else if (IsA(plan, VecModifyTable)) {
        dpetns->outerPlan = (Plan*)linitial(((VecModifyTable*)plan)->plans);
    } else {
        dpetns->outerPlan = outerPlan(plan);
    }
        
    if (dpetns->outerPlan != NULL) {
        dpetns->outerTlist = dpetns->outerPlan->targetlist;
    } else {
        dpetns->outerTlist = NIL;
    }
}

static void assignInnerPlan(RecursiveExtractNamespace* dpetns, Plan* plan, List* subplans)
{
    /*
     * For a SubqueryScan, pretend the subplan is INNER referent.  (We don't
     * use OUTER because that could someday conflict with the normal meaning.)
     * Likewise, for a CteScan, pretend the subquery's plan is INNER referent.
     * For DUPLICATE KEY UPDATE we just need the inner tlist to point to the
     * excluded expression's tlist. (Similar to the SubqueryScan we don't want
     * to reuse OUTER, it's used for RETURNING in some modify table cases,
     * although not INSERT ... ON DUPLICATE KEY UPDATE).
     */
    if (IsA(plan, SubqueryScan)) {
        dpetns->innerPlan = ((SubqueryScan*)plan)->subplan;
    } else if (IsA(plan, VecSubqueryScan)) {
        dpetns->innerPlan = ((VecSubqueryScan*)plan)->subplan;
    } else if (IsA(plan, CteScan)) {
        dpetns->innerPlan = (Plan*)list_nth(subplans, ((CteScan*)plan)->ctePlanId - 1);
    } else if (IsA(plan, ModifyTable) || IsA(plan, VecModifyTable)) {
        ModifyTable* mplan = (ModifyTable*)plan;
        dpetns->outerPlan = (Plan*)linitial(mplan->plans);;
        dpetns->innerPlan = plan;
        /*
         * For merge into, we should deparse the inner plan,  since the targetlist and qual will
         * reference sourceTargetList, which comes from outer plan of the join (source table)
         */
        if (mplan->operation == CMD_MERGE) {
            Plan* jplan = dpetns->outerPlan;
            if (IsA(jplan, Stream) || IsA(jplan, VecStream))
                jplan= jplan->lefttree;
            dpetns->innerPlan = outerPlan(jplan);
        }
    } else {
        dpetns->innerPlan = innerPlan(plan);
    }

    if (IsA(plan, ModifyTable)) {
        dpetns->innerTlist = ((ModifyTable*)plan)->exclRelTlist;
    } else if (dpetns->innerPlan != NULL) {
        dpetns->innerTlist = dpetns->innerPlan->targetlist;
    } else {
        dpetns->innerTlist = NIL;
    }
}

static void initAdviseTable(RangeTblEntry* rte, AdviseTable* adviseTable)
{
    adviseTable->oid = rte->relid;
    adviseTable->tableName = pstrdup(rte->relname);
    adviseTable->totalCandidateAttrs = NIL;
    adviseTable->originDistributionKey = (List*)copyObject(rte->partAttrNum);
    adviseTable->currDistributionKey = NULL;
    adviseTable->tableGroupOffset = 0;
    adviseTable->selecteCandidateAttrs = NIL;
    adviseTable->marked = false;
    adviseTable->isAssignRelication = false;
    adviseTable->enableAdvise = true;
}

static VirtualAttrGroupInfo* getAttrInfoByVar(RangeTblEntry* rte, Var* var)
{
    bool found = false;
    AdviseTable* adviseTable = (AdviseTable*)hash_search(
        u_sess->adv_cxt.candicateTables, (void*)&(rte->relid), HASH_ENTER, &found);
    /* init a new adviseTable */
    if (!found) {
        initAdviseTable(rte, adviseTable);
    }

    VirtualAttrGroupInfo* attrInfo = NULL;
    AttrNumber* attr = (AttrNumber*)palloc(sizeof(AttrNumber));
    attr[0] = var->varattno;
    attrInfo = getVirtualAttrGroupInfo(attr, 1, adviseTable->totalCandidateAttrs);
    /* init a new virtual_attr_group_info */
    if (attrInfo == NULL) {
        attrInfo = createVirtualAttrGroupInfo(rte->relid, list_make1(var));
        adviseTable->totalCandidateAttrs = lappend(adviseTable->totalCandidateAttrs, attrInfo);
    }
    return attrInfo;
}

/*
 * we don't support multi columns yet.
 * like: select t1.a, t2.b from t1, t2 group by t1.a,t2.b;
 */
static void insertGrpby2CandicateTable(RangeTblEntry* rte, Var* var, double rowsWeight)
{
    MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->adv_cxt.SQLAdvisorContext);
    VirtualAttrGroupInfo* attrInfo = getAttrInfoByVar(rte, var);
    AdviseQuery* adviseQuery = getCurrentQueryFromCandicateQueries();
    attrInfo->groupbyCount += adviseQuery->frequence;
    attrInfo->groupbyWeight += adviseQuery->frequence * rowsWeight;
    (void)MemoryContextSwitchTo(oldcxt);
}

static void insertJoin2CandicateTable(RangeTblEntry* rte, Node* expr, Var* arg1, Var* arg2, double rowsWeight)
{
    MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->adv_cxt.SQLAdvisorContext);
    VirtualAttrGroupInfo* attrInfo = getAttrInfoByVar(rte, arg1);
    AdviseQuery* adviseQuery = getCurrentQueryFromCandicateQueries();
    JoinCell* joinCell = NULL;
    /* check memory */
    bool isRecordFrenquence = !isAssignReplication(arg1->varno) && !isAssignReplication(arg2->varno);
    attrInfo->joinCount += adviseQuery->frequence;
    /* check is exist */
    joinCell = getJoinCell(attrInfo->joinQuals, expr);
    if (joinCell == NULL) {
        joinCell = (JoinCell*)MemoryContextAlloc(u_sess->adv_cxt.SQLAdvisorContext, sizeof(JoinCell));
        joinCell->currOid = attrInfo->oid;
        joinCell->joinWithOid = (Oid)arg2->varno;
        joinCell->expr = expr;
        joinCell->joinWeight = 0.0;
        joinCell->joinNattr = 1;
        attrInfo->joinQuals = lappend(attrInfo->joinQuals, joinCell);
    }
    if (isRecordFrenquence) {
        if (u_sess->adv_cxt.adviseMode == AM_HEURISITICS) {
            joinCell->joinWeight += (double)adviseQuery->frequence;
        } else {
            joinCell->joinWeight += rowsWeight * adviseQuery->frequence;
        }
    } else {
        joinCell->joinWeight = -1.0;
    }

    (void)MemoryContextSwitchTo(oldcxt);
}

static void insertQual2CandicateTable(RangeTblEntry* rte, Node* expr, Var* arg1)
{
    MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->adv_cxt.SQLAdvisorContext);
    VirtualAttrGroupInfo* attrInfo = getAttrInfoByVar(rte, arg1);
    AdviseQuery* adviseQuery = getCurrentQueryFromCandicateQueries();
    attrInfo->qualCount += adviseQuery->frequence;
    Node* find_expr = getQualCell(attrInfo->quals, expr);
    if (find_expr == NULL) {
        attrInfo->quals = lappend(attrInfo->quals, expr);
    }

    (void)MemoryContextSwitchTo(oldcxt);
}

/* (A AND B AND C) == (C AND B AND A) */
static bool equalExpr(Node* a, Node* b)
{
    if (nodeTag(a) != nodeTag(b)) {
        return false;
    }

    if (nodeTag(a) == T_OpExpr || nodeTag(a) == T_ScalarArrayOpExpr) {
        return equalOpExpr(a, b);
    } else if (nodeTag(a) == T_Var) {
        return equalVar((Var*)a, (Var*)b);
    } else if (nodeTag(a) == T_Const) {
        return equalConst((Const*)a, (Const*)b);
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), 
            errmsg("current node type cant equal: %d", (int)nodeTag(a))));
    }

    return false;
}

/* equalOpExpr
 *   we now only think (t.a = t1.b) == (t1.b = t.a)
 *                 AND (t.a = 1) == (1 == t.a)
 * Actually, we should consider (t.a > t1.b) == (t1.b < t.a) etc.
 */
static bool equalOpExpr(Node* a, Node* b)
{
    if (nodeTag(a) == T_OpExpr) {
        OpExpr* op_a = (OpExpr*)a;
        OpExpr* op_b = (OpExpr*)b;
        if (op_a->opno == op_b->opno && list_length(op_a->args) == list_length(op_b->args) && op_a->opno == 96) {
            Node* a_arg1 = (Node*)linitial(op_a->args);
            Node* a_arg2 = (Node*)lsecond(op_a->args);
            Node* b_arg1 = (Node*)linitial(op_b->args);
            Node* b_arg2 = (Node*)lsecond(op_b->args);
            return (equalExpr(a_arg1, b_arg1) && equalExpr(a_arg2, b_arg2)) ||
                (equalExpr(a_arg1, b_arg2) && equalExpr(a_arg2, b_arg1));
        } 
    } else if (nodeTag(a) == T_ScalarArrayOpExpr) {
        ScalarArrayOpExpr* s_a = (ScalarArrayOpExpr*)a;
        ScalarArrayOpExpr* s_b = (ScalarArrayOpExpr*)b;
        if (s_a->opno == s_b->opno && s_a->useOr == s_b->useOr) {
            Node* a_arg1 = (Node*)linitial(s_a->args);
            Node* a_arg2 = (Node*)lsecond(s_a->args);
            Node* b_arg1 = (Node*)linitial(s_b->args);
            Node* b_arg2 = (Node*)lsecond(s_b->args);
            return (equalExpr(a_arg1, b_arg1) && equalExpr(a_arg2, b_arg2)) ||
                (equalExpr(a_arg1, b_arg2) && equalExpr(a_arg2, b_arg1));
        }
    }
    
    return false;
}

static bool equalVar(Var* a, Var* b)
{
    return ((Oid)a->varno == (Oid)b->varno && a->varattno == b->varattno) ? true : false;
}

static bool equalConst(Const* a, Const* b)
{
    if (a->consttype == b->consttype) {
        if(a->ismaxvalue && b->ismaxvalue) {
            return true;
        } else if (a->constisnull && b->constisnull) {
            return true;
        }
        char* extvalA = getConstCharValue(a);
        char* extvalB = getConstCharValue(b);

        if (strcmp(extvalA, extvalB) == 0) {
            return true;
        }
    }
    return false;
}

static void fillinAdviseTable(Node* node, List* rtable, double rowsWeight)
{
    if (node == NULL) {
        return;
    }

    /* hook for index_advisor */
    if (u_sess->adv_cxt.getPlanInfoFunc != NULL) {
        ((get_info_from_plan_hook_type)u_sess->adv_cxt.getPlanInfoFunc) (node, rtable);
        return;
    }
	
    MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->adv_cxt.SQLAdvisorContext);

    if (IsA(node, OpExpr)) {
        OpExpr* expr = (OpExpr*)node;
        if (list_length(expr->args) == 2) {
            Node* expr_arg1 = (Node*)linitial(expr->args);
            Node* expr_arg2 = (Node*)lsecond(expr->args);
            
            if (IsA(expr_arg1, Var) && IsA(expr_arg2, Var)) {
                Var* arg1 = (Var*)expr_arg1;
                Var* arg2 = (Var*)expr_arg2;
                RangeTblEntry* rte1 = rt_fetch(arg1->varno, rtable);
                RangeTblEntry* rte2 = rt_fetch(arg2->varno, rtable);
                /* expr must be 2 different tables. 
                 * if table type is relication, we ignore the qual.
                 */
                if (rte1->relid != rte2->relid) {
                    OpExpr* copy_expr = (OpExpr*)copyObject(expr);
                    Var* copy_arg1= (Var*)linitial(copy_expr->args);
                    Var* copy_arg2= (Var*)lsecond(copy_expr->args);
                    copy_arg1->varno = (Index)rte1->relid;
                    copy_arg2->varno = (Index)rte2->relid;
                    insertJoin2CandicateTable(rte1, (Node*)copy_expr, copy_arg1, copy_arg2, rowsWeight);
                    insertJoin2CandicateTable(rte2, (Node*)copy_expr, copy_arg2, copy_arg1, rowsWeight);
                } 
            } else if (IsA(expr_arg1, Var) && IsA(expr_arg2, Const)) {
                Var* var = (Var*)expr_arg1;
                RangeTblEntry* rte = rt_fetch(var->varno, rtable);
                OpExpr* copy_expr = (OpExpr*)copyObject(expr);
                Var* copy_var= (Var*)linitial(copy_expr->args);
                copy_var->varno = (Index)rte->relid;
                insertQual2CandicateTable(rte, (Node*)copy_expr, copy_var);
            } else if (IsA(expr_arg1, Const) && IsA(expr_arg2, Var)) {
                Var* var = (Var*)expr_arg2;
                RangeTblEntry* rte = rt_fetch(var->varno, rtable);
                OpExpr* copy_expr = (OpExpr*)copyObject(expr);
                Var* copy_var= (Var*)lsecond(copy_expr->args);
                copy_var->varno = (Index)rte->relid;
                insertQual2CandicateTable(rte, (Node*)copy_expr, copy_var);
            }
        } 
    } else if (IsA(node, ScalarArrayOpExpr)) {
        ScalarArrayOpExpr* expr = (ScalarArrayOpExpr*)node;
        Node* expr_arg1 = (Node*)linitial(expr->args);
        Node* expr_arg2 = (Node*)lsecond(expr->args);
        if (IsA(expr_arg1, Var) && IsA(expr_arg2, Const)) {
            Var* var = (Var*)expr_arg1;
            RangeTblEntry* rte = rt_fetch(var->varno, rtable);
            ScalarArrayOpExpr* copy_expr = (ScalarArrayOpExpr*)copyObject(expr);
            Var* copy_var= (Var*)linitial(copy_expr->args);
            copy_var->varno = (Index)rte->relid;
            insertQual2CandicateTable(rte, (Node*)copy_expr, copy_var);
        } else if (IsA(expr_arg1, Const) && IsA(expr_arg2, Var)) {
            Var* var = (Var*)expr_arg2;
            RangeTblEntry* rte = rt_fetch(var->varno, rtable);
            ScalarArrayOpExpr* copy_expr = (ScalarArrayOpExpr*)copyObject(expr);
            Var* copy_var= (Var*)lsecond(copy_expr->args);
            copy_var->varno = (Index)rte->relid;
            insertQual2CandicateTable(rte, (Node*)copy_expr, copy_var);
        }
    }

    (void)MemoryContextSwitchTo(oldcxt);
}

static Node* getQualCell(List* quals, Node* expr)
{
    if (quals == NIL) {
        return NULL;
    }

    ListCell* cell = NULL;
    Node* nodeCell = NULL;
    foreach (cell, quals) {
        nodeCell = (Node*)lfirst(cell);
        if (equalExpr(nodeCell, expr)) {
            break;
        }
    }
    if (cell == NULL) {
        return NULL;
    }
    
    return nodeCell;
}

static JoinCell* getJoinCell(List* joinQuals, Node* expr)
{
    if (joinQuals == NIL) {
        return NULL;
    }
    ListCell* cell = NULL;
    JoinCell* joinCell = NULL;
    foreach (cell, joinQuals) {
        joinCell = (JoinCell*)lfirst(cell);
        if (equalExpr(joinCell->expr, expr)) {
            break;
        }
    }

    if (cell == NULL) {
        return NULL;
    }

    return joinCell;
}


static char* getConstCharValue(Const* constval)
{
    Oid typoutput;
    bool typIsVarlena = false;
    getTypeOutputInfo(constval->consttype, &typoutput, &typIsVarlena);
    return OidOutputFunctionCall(typoutput, constval->constvalue);
}

static VirtualAttrGroupInfo* createVirtualAttrGroupInfo(Oid relid, List* vars)
{
    VirtualAttrGroupInfo* attrInfo = (VirtualAttrGroupInfo*)MemoryContextAlloc(
        u_sess->adv_cxt.SQLAdvisorContext, sizeof(VirtualAttrGroupInfo));
    attrInfo->oid = relid;
    attrInfo->natts = list_length(vars);
    attrInfo->attrNums = NULL;
    attrInfo->attrNames = NIL;
    attrInfo->joinQuals = NIL;
    attrInfo->quals = NIL;
    attrInfo->joinCount = 0;
    attrInfo->groupbyCount = 0;
    attrInfo->groupbyWeight = 0.0;
    attrInfo->qualCount = 0;
    attrInfo->weight = 0.0;
    ListCell* cell = NULL;
    Var* var = NULL;
    char* str = NULL;
    attrInfo->attrNums = (AttrNumber*)palloc(sizeof(AttrNumber) * list_length(vars));
    int i = 0;

    foreach (cell, vars) {
        var = (Var*)lfirst(cell);
        attrInfo->attrNums[i++] = var->varattno;
        str = pstrdup(get_attname(relid, var->varattno));
        attrInfo->attrNames = lappend(attrInfo->attrNames, str);
    }

    return attrInfo;
}

VirtualAttrGroupInfo* getVirtualAttrGroupInfo(AttrNumber* var_attno, int natts, List* attrs_list)
{
    ListCell* cell = NULL;
    VirtualAttrGroupInfo* attrInfo = NULL;
    foreach (cell, attrs_list) {
        attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
        if (compareAttrs(var_attno, natts, attrInfo->attrNums, attrInfo->natts)) {
            return attrInfo;
        }
    }
    
    return NULL;
}

/* check table type is replication */
static bool isAssignReplication(Oid relid)
{
    bool found = true;
    AdviseTable* adviseTable = (AdviseTable*)hash_search(
        u_sess->adv_cxt.candicateTables, (void*)&(relid), HASH_FIND, &found);
    
    if (found) {
        if (adviseTable->isAssignRelication) {
            return true;
        }
    }

    return false;
}
