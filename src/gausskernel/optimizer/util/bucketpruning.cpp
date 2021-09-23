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
 * bucketpruning.cpp
 *     functions related to bucketpruning
 *
 * IDENTIFICATION
 *     src/gausskernel/optimizer/util/bucketpruning.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/hash.h"
#include "parser/parsetree.h"
#include "pgxc/locator.h"
#include "optimizer/bucketinfo.h"
#include "optimizer/bucketpruning.h"
#include "optimizer/dynsmp.h"
#include "optimizer/planner.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "nodes/pg_list.h"
#include "nodes/nodeFuncs.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "pgxc/groupmgr.h"
#include "utils/plancache.h"
#include "pgxc/pgxc.h"
#include "executor/executor.h"

/*
 * BucketPruningContext
 *
 *  holds all things needed for pruning
 *  pass things around when pruning each expr
 *
 */
typedef struct BucketPruningContext {
    PlannerInfo* root;
    RelOptInfo* rel;
    RangeTblEntry* rte; /* the range table for pruning */
    AttrNumber attno;   /* the distribute key attno */
    Expr* expr;         /* the expr for pruning */
} BucketPruningContext;

/*
 * PruningStatus
 *
 * 		indicate the pruning result's status
 *
 *  BUCKETS_UNINITIED: before pruning, the result is not know
 *  BUCKETS_FULL: have to scan all the buckets
 *  BUCKETS_EMPTY: all the buckets are pruned
 *  BUCKETS_PRUNED: only some of the buckets are pruning, but not all
 */
typedef enum PruningStatus { BUCKETS_UNINITIED, BUCKETS_FULL, BUCKETS_EMPTY, BUCKETS_PRUNED } PruningStatus;

/*
 * BucketPruningResult
 *
 * 		store the intermediate pruning results
 *
 *  status: one of BUCKETS_UNINITIED/BUCKETS_FULL/BUCKETS_EMPTY/BUCKETS_PRUNED
 *          see PruningStatus for what it means
 *  buckets: a bitmapset containing which bucket id have to be scanned
 */
typedef struct BucketPruningResult {
    PruningStatus status;
    Bitmapset* buckets;
} BucketPruningResult;

static List* BucketPruningMain(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte, List* restrictInfo);
static BucketPruningContext* makePruningContext(
    PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte, List* restrictInfo);
static Expr* RestrictInfoGetExpr(List* restrictInfo);
static BucketPruningResult* BucketPruningForExpr(BucketPruningContext* bpcxt, Expr* expr);
static int getConstBucketId(Const* val, int bucketmapsize);
static BucketPruningResult* BucketPruningForBoolExpr(BucketPruningContext* bpcxt, BoolExpr* expr);
static BucketPruningResult* BucketPruningForOpExpr(BucketPruningContext* bpcxt, OpExpr* expr);
static int GetExecBucketId(ExecNodes* exec_nodes, ParamListInfo params);

/*
 * @Description: make a pruning result based on the PruningStatus
 *
 *  BUCKETS_UNINITIED: before pruning, the result is not know
 *  BUCKETS_FULL: make a bitmapset contains all the buckets
 *  BUCKETS_EMPTY: make a pruning result contain no buckets
 *  BUCKETS_PRUNED: **not allowed**
 */
static BucketPruningResult* makePruningResult(PruningStatus status, int bucketmapsize = 0)
{
    BucketPruningResult* result = (BucketPruningResult*)palloc0(sizeof(BucketPruningResult));

    Assert(status != BUCKETS_PRUNED);

    result->buckets = NULL;
    result->status = status;

    if (status == BUCKETS_FULL) {
        for (int i = 0; i < bucketmapsize; i++) {
            result->buckets = bms_add_member(result->buckets, i);
        }
    }

    return result;
}

/*
 * @Description: make a pruning result based on the Bitmapset
 *
 *  when the bitmapset is empty a EMPTY result is returned
 *  when the bitmapset is full a FULL result is returned and the bitmap set is re-used
 *  when the bitmapset is not-full a PRUNED result is returned
 */
static BucketPruningResult* makePruningResult(Bitmapset* buckets, int bucketmapsize)
{
    if (bms_is_empty(buckets)) {
        return makePruningResult(BUCKETS_EMPTY);
    }

    BucketPruningResult* result = (BucketPruningResult*)palloc0(sizeof(BucketPruningResult));

    if (bms_num_members(buckets) == bucketmapsize) {
        Assert(bucketmapsize != 0);
        result->status = BUCKETS_FULL;
    } else {
        result->status = BUCKETS_PRUNED;
    }

    result->buckets = buckets;

    return result;
}

/*
 * @Description: make a pruning result given a single bucket id
 *
 *  given a bucket id, construct a PRUNED BucketPruningResult for it
 *  if exclude is true, will make a PRUNED BucketPruningResult excludes x
 *  which quite handy in var=const and var!=const
 */
static BucketPruningResult* makePruningResult(int x, bool exclude = false)
{
    BucketPruningResult* result = (BucketPruningResult*)palloc0(sizeof(BucketPruningResult));
    if (exclude) {
        Assert(0);
        for (int i = 0; i < BUCKETDATALEN; i++) {
            if (exclude && i == x) {
                continue;
            }

            result->buckets = bms_add_member(result->buckets, i);
        }
    } else {
        result->buckets = bms_make_singleton(x);
    }

    result->status = BUCKETS_PRUNED;

    return result;
}

/*
 * @Description: intersect pruning result of a,b
 *
 *        given two pruning results of a b
 *        this function intersect the results
 *        for example a=[1,2,3] b=[2,3,4]
 *        the function return [2,3] of the intersection
 *
 * @return a list contains the intersection result
 */
static BucketPruningResult* intersectPruningResult(BucketPruningResult* a, BucketPruningResult* b, int bucketmapsize)
{
    if (a->status == BUCKETS_UNINITIED && b->status == BUCKETS_UNINITIED) {
        return makePruningResult(BUCKETS_UNINITIED);
    }

    if (a->status == BUCKETS_UNINITIED && b->status != BUCKETS_UNINITIED) {
        return b;
    }

    if (a->status != BUCKETS_UNINITIED && b->status == BUCKETS_UNINITIED) {
        return a;
    }

    if (a->status == BUCKETS_EMPTY || b->status == BUCKETS_EMPTY) {
        return makePruningResult(BUCKETS_EMPTY);
    }

    return makePruningResult(bms_intersect(a->buckets, b->buckets), bucketmapsize);
}

/*
 * @Description: union pruning result of a,b
 *
 *        given two pruning results of a b
 *        this function union the results
 *        for example a=[1,2,3] b=[2,3,4]
 *        the function return [1,2,3,4]
 *
 * @return a list contains the union result
 */
static BucketPruningResult* unionPruningResult(BucketPruningResult* a, BucketPruningResult* b, int bucketmapsize)
{
    if (a->status == BUCKETS_UNINITIED && b->status == BUCKETS_UNINITIED) {
        return makePruningResult(BUCKETS_UNINITIED);
    }

    if (a->status == BUCKETS_UNINITIED && b->status != BUCKETS_UNINITIED) {
        return b;
    }

    if (a->status != BUCKETS_UNINITIED && b->status == BUCKETS_UNINITIED) {
        return a;
    }

    if (a->status == BUCKETS_FULL || b->status == BUCKETS_FULL) {
        return makePruningResult(BUCKETS_FULL, bucketmapsize);
    }

    return makePruningResult(bms_union(a->buckets, b->buckets), bucketmapsize);
}

/*
 * @Description: for NotExpr, return buckets A without members of B
 *
 *        given two pruning results of a b
 *        this function not the results in b
 *        for example a=[1,2,3] b=[2,3,4]
 *        the function return [1]
 *
 * @return a list contains the buckets A without members of B
 */
static BucketPruningResult* notPruningResult(BucketPruningResult* a, BucketPruningResult* b, int bucketmapsize)
{
    if (a->status == BUCKETS_UNINITIED && b->status == BUCKETS_UNINITIED) {
        return makePruningResult(BUCKETS_UNINITIED);
    }

    if (a->status == BUCKETS_UNINITIED && b->status != BUCKETS_UNINITIED) {
        return notPruningResult(makePruningResult(BUCKETS_FULL, bucketmapsize), b, bucketmapsize);
    }

    if (a->status != BUCKETS_UNINITIED && b->status == BUCKETS_UNINITIED) {
        return a;
    }

    if (a->status == BUCKETS_EMPTY || b->status == BUCKETS_FULL) {
        return makePruningResult(BUCKETS_EMPTY);
    }

    return makePruningResult(bms_difference(a->buckets, b->buckets), bucketmapsize);
}

/*
 * @Description: set bucketinfo to RelOptInfo and do pruning if possible
 *
 * @in  PlannerInfo: it holds some important info we need
 * @out RelOptInfo: the bucketinfo will set to rel->bucketInfo
 * @in  RangeTblEntry: the user might have selected some buckets from sql stmt
 */
void set_rel_bucketinfo(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte)
{
    /* add the bucketinfo to the rel */
    if (rte->isbucket) {
        /* the user select the buckets from sql stmt */
        rel->bucketInfo = makeNode(BucketInfo);
        rel->bucketInfo->buckets = rte->buckets;
    } else if (rte->relhasbucket) {
        /* construct the bucket info and ready for pruning */
        rel->bucketInfo = makeNode(BucketInfo);
        rel->bucketInfo->buckets = BucketPruningMain(root, rel, rte, rel->baserestrictinfo);
    } else {
        /* the relation does not have underlying buckets */
        rel->bucketInfo = NULL;
    }

    return;
}

/*
 * @Description: given a restrictInfo list convert it into an expr
 *
 * @in  restrictInfo: a rel's restrict info
 */
static Expr* RestrictInfoGetExpr(List* restrictInfo)
{
    ListCell* cell = NULL;
    List* exprList = NIL;
    Expr* expr = NULL;

    foreach (cell, restrictInfo) {
        RestrictInfo* resinfo = (RestrictInfo*)lfirst(cell);

        if (PointerIsValid(resinfo->clause)) {
            exprList = lappend(exprList, copyObject(resinfo->clause));
        }
    }

    if (exprList == NIL) {
        return NULL;
    }

    if (list_length(exprList) == 0) {
        return NULL;
    }

    if (list_length(exprList) == 1) {
        expr = (Expr*)list_nth(exprList, 0);
    } else {
        expr = makeBoolExpr(AND_EXPR, exprList, 0);
    }

    return expr;
}

/*
 * @Description: Get Distribute Key Attno for later check if
 *               Var in "Var op Const" is referencing the
 *               Distribute key.
 *
 * @in  Oid: the oid of the relation
 * @return the distribute key attno if any
 */
AttrNumber GetDistributeKeyAttno(Oid reloid)
{
    AttrNumber keyattno = InvalidAttrNumber;

    if (IS_PGXC_COORDINATOR) {
        RelationLocInfo* locinfo = GetRelationLocInfo(reloid);

        if (locinfo != NULL && locinfo->locatorType == LOCATOR_TYPE_HASH && list_length(locinfo->partAttrNum) == 1) {
            keyattno = (int16)(linitial_int(locinfo->partAttrNum));
        }
    } else if (IS_PGXC_DATANODE) {
        Relation relation = heap_open(reloid, NoLock);
        int2vector* colids = relation->rd_bucketkey->bucketKey;

        Assert(REALTION_BUCKETKEY_VALID(relation));
        if (colids->ndim == 1 && colids->dim1 == 1) {
            keyattno = colids->values[0];
        }

        heap_close(relation, NoLock);
    }

    return keyattno;
}

/*
 * @Description: if pruning is possible fill the info need and return the BucketPruningContext
 *
 * @in  PlannerInfo: this holds many things needed during pruning
 * @in  RelOptInfo : this holds many things needed during pruning
 * @in  RangeTblEntry: this holds many things needed during pruning
 * @in  List of restrictInfo: the restrict info used for pruning
 * @return if we can do pruning return BucketPruingContext
 *         if pruning is not possible return NULL
 */
static BucketPruningContext* makePruningContext(
    PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte, List* restrictInfo)
{
    Expr* expr;
    AttrNumber attno;

    /* get expr for pruning */
    if ((expr = RestrictInfoGetExpr(restrictInfo)) == NULL) {
        return NULL;
    }

    /* get distribute key attno */
    if ((attno = GetDistributeKeyAttno(rte->relid)) == InvalidAttrNumber) {
        return NULL;
    }

    BucketPruningContext* bpcxt = (BucketPruningContext*)palloc0(sizeof(BucketPruningContext));

    bpcxt->root = root;
    bpcxt->rel = rel;
    bpcxt->rte = rte;
    bpcxt->attno = attno;
    bpcxt->expr = expr;

    return bpcxt;
}

/*
 * @Description: given a pruningResult list convert it into an List
 *
 *       In the pruning process we use bucketid bitmapset, however the
 *       BucketInfo contains a bucketid List. This function convert
 *       the bucket Bitmapset to bucket List
 */
List* PruningResultGetBucketList(BucketPruningResult* pruningResult)
{
    List* bucketlist = NIL;

    if (pruningResult->status == BUCKETS_PRUNED) {
        int bucketid = -1;
        while ((bucketid = bms_next_member(pruningResult->buckets, bucketid)) >= 0) {
            bucketlist = lappend_int(bucketlist, bucketid);
        }

        return bucketlist;
    } else {
        return NIL;
    }
}

/*
 * @Description: The Main Entrance of bucket pruning
 *
 *    for a given relation pruning buckets based on restrictInfo
 *         step1. make a BucketPruningContext
 *         step2. do pruning if possible
 *         step3. return the BucketList as the pruning results
 *
 *    we use a memory context to hold all the memory allocated
 *    during pruning, because we allocate many BucketPruningResult
 *    in the middle of pruning, have to free them though.
 *
 * @in  PlannerInfo: this holds many things needed during pruning
 * @in  RelOptInfo : this holds many things needed during pruning
 * @in  RangeTblEntry: this holds many things needed during pruning
 * @in  List of restrictInfo: the restrict info used for pruning
 * @return the pruning results
 */
static List* BucketPruningMain(PlannerInfo* root, RelOptInfo* rel, RangeTblEntry* rte, List* restrictInfo)
{
    List* bucketlist = NIL;
    BucketPruningResult* pruningResult = NULL;

    MemoryContext mcxt = AllocSetContextCreate(CurrentMemoryContext,
        "BucketPruningMemoryContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /* all the memory allocated during pruing will hold in this cxt */
    MemoryContext old = MemoryContextSwitchTo(mcxt);

    PG_TRY();
    {
        BucketPruningContext* bpcxt = makePruningContext(root, rel, rte, restrictInfo);

        if (bpcxt != NULL) {
            /* do the hard part */
            pruningResult = BucketPruningForExpr(bpcxt, bpcxt->expr);
        }
    }
    PG_CATCH();
    {
        /* restore not-pruned state */
        bucketlist = NIL;
        pruningResult = NULL;
        /* switch back to the original cxt, so the final result can be made */
        (void)MemoryContextSwitchTo(old);
        ErrorData* edata = CopyErrorData();
        elog(LOG, "[BucketPruning] bucket pruning failed : %s", edata->message);
        FreeErrorData(edata);
        FlushErrorState();
    }
    PG_END_TRY();

    /* switch back to the original cxt, so the final result can be made */
    (void)MemoryContextSwitchTo(old);

    if (pruningResult != NULL) {
        /* covert to a proper list format */
        bucketlist = PruningResultGetBucketList(pruningResult);
    }

    /* now we have all things we need, free the mem during pruning */
    MemoryContextDelete(mcxt);

    return bucketlist;
}

/*
 * @Description: do pruning for a given expr
 *
 *         this is the main workfunction for the bucket pruning
 *         which recursively goes into expr and pruning when possible
 *         currently we can only do BoolExpr and OpExpr pruning
 *
 * @in  BucketPruningContext: the context needed for pruning
 * @in  Expr: the expr used for pruning
 * @return the pruning result for the given expr
 */
static BucketPruningResult* BucketPruningForExpr(BucketPruningContext* bpcxt, Expr* expr)
{
    BucketPruningResult* result = NULL;

    switch (nodeTag(expr)) {
        case T_BoolExpr: {
            BoolExpr* boolExpr = NULL;

            boolExpr = (BoolExpr*)expr;

            result = BucketPruningForBoolExpr(bpcxt, boolExpr);
        } break;
        case T_OpExpr: {
            OpExpr* opExpr = NULL;

            opExpr = (OpExpr*)expr;

            result = BucketPruningForOpExpr(bpcxt, opExpr);
        } break;
        default: {
            return makePruningResult(BUCKETS_FULL, bpcxt->rte->bucketmapsize);
        } break;
    }

    return result;
}

/*
 * @Description: do pruning for a given BoolExpr
 *
 *         handle AND/OR/NOT cases of BoolExpr
 *
 * @in  BucketPruningContext: the context needed for pruning
 * @in  Expr: the expr used for pruning
 * @return the pruning result for the given BoolExpr
 */
static BucketPruningResult* BucketPruningForBoolExpr(BucketPruningContext* bpcxt, BoolExpr* expr)
{
    ListCell* cell = NULL;
    BucketPruningResult* result = makePruningResult(BUCKETS_UNINITIED);

    foreach (cell, expr->args) {
        Expr* childExpr = (Expr*)lfirst(cell);

        BucketPruningResult* childPruningResult = BucketPruningForExpr(bpcxt, childExpr);

        switch (expr->boolop) {
            case AND_EXPR:
                result = intersectPruningResult(result, childPruningResult, bpcxt->rte->bucketmapsize);
                break;
            case OR_EXPR:
                result = unionPruningResult(result, childPruningResult, bpcxt->rte->bucketmapsize);
                break;
            case NOT_EXPR:
                result = notPruningResult(result, childPruningResult, bpcxt->rte->bucketmapsize);
                /* fall through */
            default:
                break;
        }
    }

    return result;
}

/*
 * @Description: do pruning for a given OpExpr
 *
 *         support for "var op const" kind of OpExpr
 *         (1) the var must be a distribute col of the rel
 *         (2) the op must be =
 *         otherwise we cannot do pruning
 *
 * @in  BucketPruningContext: the context needed for pruning
 * @in  Expr: the expr used for pruning
 * @return the pruning result for the given expr
 */
static BucketPruningResult* BucketPruningForOpExpr(BucketPruningContext* bpcxt, OpExpr* expr)
{
    char* opName = NULL;
    Expr* leftArg = NULL;
    Expr* rightArg = NULL;
    Const* constArg = NULL;
    Var* varArg = NULL;
    bool rightArgIsConst = true;

    /* only handle op of 2 arguments */
    if (!PointerIsValid(expr) || list_length(expr->args) != 2 || !PointerIsValid(opName = get_opname(expr->opno))) {
        return makePruningResult(BUCKETS_FULL, bpcxt->rte->bucketmapsize);
    }

    leftArg = (Expr*)list_nth(expr->args, 0);
    rightArg = (Expr*)list_nth(expr->args, 1);

    /* handle the relabel type */
    while (leftArg && IsA(leftArg, RelabelType)) {
        leftArg = ((RelabelType*)leftArg)->arg;
    }
    while (rightArg && IsA(rightArg, RelabelType)) {
        rightArg = ((RelabelType*)rightArg)->arg;
    }

    /* we only handle var op const */
    if (!((T_Const == nodeTag(leftArg) && T_Var == nodeTag(rightArg)) ||
            (T_Var == nodeTag(leftArg) && T_Const == nodeTag(rightArg)))) {
        return makePruningResult(BUCKETS_FULL, bpcxt->rte->bucketmapsize);
    }

    if (T_Const == nodeTag(leftArg)) {
        constArg = (Const*)leftArg;
        varArg = (Var*)rightArg;
        rightArgIsConst = false;
    } else {
        constArg = (Const*)rightArg;
        varArg = (Var*)leftArg;
    }

    /* see if var is distribute key */
    RangeTblEntry* rte = (RangeTblEntry*)planner_rt_fetch((int)(varArg->varno), bpcxt->root);

    /* var is not referencing this relation */
    if (rte->relid != bpcxt->rte->relid) {
        return makePruningResult(BUCKETS_FULL, bpcxt->rte->bucketmapsize);
    }

    /* var is not referencing the distribute col */
    if (varArg->varattno != bpcxt->attno) {
        return makePruningResult(BUCKETS_FULL, bpcxt->rte->bucketmapsize);
    }

    /* time for pruning */
    int id = getConstBucketId(constArg, bpcxt->rte->bucketmapsize);

    if (pg_strcasecmp(opName, "=") == 0) {
        return makePruningResult(id);
    }

    return makePruningResult(BUCKETS_FULL, bpcxt->rte->bucketmapsize);
}

/*
 * @Description: calculate the bucketid for a const val
 *
 *         notice the algorithm must be the same with
 *         the executor, double check it when make modify
 *
 * @in  Const: the const val which we want the bucketid of it
 * @return the bucketid index of the const val
 */
static int getConstBucketId(Const* val, int bucketmapsize)
{
    uint32 hashval = 0;
    int bucketid = 0;

    if (val->constisnull) {
        return 0;
    }

    hashval = compute_hash(val->consttype, val->constvalue, LOCATOR_TYPE_HASH);

    bucketid = compute_modulo((unsigned int)(abs((int)hashval)), bucketmapsize);

    return bucketid;
}

void setCachedPlanBucketId(CachedPlan *cplan, ParamListInfo boundParams)
{
    /* run time bucket purning for generic plan */
    ListCell *p = NULL;

    /* not support global plan cache yet */
    if (g_instance.attr.attr_common.enable_global_plancache) {
        return;
    }

    foreach (p, cplan->stmt_list) {
        PlannedStmt* pstmt = (PlannedStmt*)lfirst(p);
        if (IsA(pstmt, PlannedStmt)){
            /* set the main plan */
            setPlanBucketId(pstmt->planTree, boundParams, cplan->context);
            /* also the sub plans */
            ListCell* lc = NULL;
            foreach (lc, pstmt->subplans) {
                Plan* subPlan = (Plan*)lfirst(lc);
                setPlanBucketId(subPlan, boundParams, cplan->context);
            }
        }
    }
}

static void setScanPlanBucketId(Plan* plan, ParamListInfo params, MemoryContext cxt)
{
    Scan* scan = (Scan*)plan;

    /* free the previous results */
    if (scan->bucketInfo->buckets != NIL) {
        list_free(scan->bucketInfo->buckets);
        scan->bucketInfo->buckets = NIL;
    }
    
    /* try runtime purning */
    int bucketid = GetExecBucketId(plan->exec_nodes, params);
    if (bucketid != INVALID_BUCKET_ID) {
       /* for cached plan , buckets lives in a longer memory context */
       MemoryContext oldcxt = MemoryContextSwitchTo(cxt);
       scan->bucketInfo->buckets = lappend_int(scan->bucketInfo->buckets, bucketid);
       MemoryContextSwitchTo(oldcxt);
    }
}

/*
 * Execution time determining of target bucket id
 */
void setPlanBucketId(Plan* plan, ParamListInfo params, MemoryContext cxt)
{
    if (plan == NULL)
        return;

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    /* nodes we are interested */
    switch (nodeTag(plan)) {
        case T_SeqScan:
        case T_CStoreScan:
        case T_IndexScan:
        case T_IndexOnlyScan:
        case T_BitmapHeapScan:
        case T_BitmapIndexScan:
        case T_TidScan:
        case T_CStoreIndexScan:
        case T_CStoreIndexCtidScan:
        case T_CStoreIndexHeapScan: {
            Scan* scan = (Scan*)plan;
            /* quit if not a hashbucket relation */
            if (scan->bucketInfo == NULL) {
                break;
            }
            setScanPlanBucketId(plan, params, cxt);
            break;
        }
        case T_SubqueryScan:
        case T_VecSubqueryScan: {
            SubqueryScan* sc = (SubqueryScan*)plan;

            setPlanBucketId(sc->subplan, params, cxt);
            break;
        }
        default: {
            /* recurse the left tree and right tree */
            setPlanBucketId(plan->lefttree, params, cxt);
            setPlanBucketId(plan->righttree, params, cxt);
            break;
        }
    }

    ListCell* lc = NULL;

    foreach(lc, get_plan_list(plan)) {
        setPlanBucketId((Plan*)lfirst(lc), params, cxt);
    }

    return;
}


static int GetExecBucketId(ExecNodes* exec_nodes, ParamListInfo params)
{
    bool isnull = false;
    MemoryContext oldContext;

    if (exec_nodes == NULL || exec_nodes->bucketexpr == NULL)
        return INVALID_BUCKET_ID;

    RelationLocInfo* rel_loc_info = NULL;

    if (IS_PGXC_DATANODE) {
        rel_loc_info = GetRelationLocInfoDN(exec_nodes->bucketrelid);
    } else {
        rel_loc_info = GetRelationLocInfo(exec_nodes->bucketrelid);
    }

    if (rel_loc_info == NULL) {
        return INVALID_BUCKET_ID;
    }

    int len = list_length(rel_loc_info->partAttrNum);
    /* It should switch memctx to ExprContext for makenode in ExecInitExpr */
    Datum* values = (Datum*)palloc(len * sizeof(Datum));
    bool* null = (bool*)palloc(len * sizeof(bool));
    Oid* typOid = (Oid*)palloc(len * sizeof(Oid));
    List* dist_col = NULL;
    int i = 0;

    EState* estate = CreateExecutorState();
    estate->es_param_list_info = params;
    ExprContext* exprcontext = CreateExprContext(estate);

    ListCell* cell = NULL;
    foreach (cell, exec_nodes->bucketexpr) {
        Expr* expr = (Expr*)lfirst(cell);
        oldContext = MemoryContextSwitchTo(estate->es_query_cxt);

        ExprState* exprstate = ExecInitExpr(expr, NULL);

        Datum partvalue = ExecEvalExpr(exprstate, exprcontext, &isnull, NULL);

        MemoryContextSwitchTo(oldContext);

        values[i] = partvalue;
        null[i] = isnull;
        typOid[i] = exprType((Node*)expr);
        dist_col = lappend_int(dist_col, i);
        i++;
    }

    ExecNodes* nodes = GetRelationNodes(rel_loc_info,
                                        values, null,
                                        typOid,
                                        dist_col,
                                        exec_nodes->accesstype,
                                        false,
                                        false);

    FreeExprContext(exprcontext, true);
    FreeExecutorState(estate);
    FreeRelationLocInfo(rel_loc_info);
    pfree_ext(values);
    pfree_ext(null);
    pfree_ext(typOid);
    list_free_ext(dist_col);

    return nodes->bucketid;
}

BucketInfo* CalBucketInfo(ScanState* state)
{
    Assert(state != NULL);
    if (unlikely((state->ps.plan == NULL) || (state->ps.state == NULL))) {
        return NULL;
    }

    BucketInfo* bkinfo = NULL;
    int bucketid = GetExecBucketId(state->ps.plan->exec_nodes, state->ps.state->es_param_list_info);
    if (bucketid != INVALID_BUCKET_ID) {
        bkinfo = (BucketInfo*)palloc0(sizeof(BucketInfo));
        bkinfo->buckets = NIL;
        bkinfo->buckets = lappend_int(bkinfo->buckets, bucketid);
    }

    return bkinfo;
}
