/* -------------------------------------------------------------------------
 *
 * nodeHashjoin.cpp
 *	  Routines to handle hash join nodes
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeHashjoin.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/executor.h"
#include "executor/exec/execStream.h"
#include "executor/hashjoin.h"
#include "executor/node/nodeHash.h"
#include "executor/node/nodeHashjoin.h"
#include "miscadmin.h"
#include "utils/anls_opt.h"
#include "utils/memutils.h"

/*
 * States of the ExecHashJoin state machine
 */
#define HJ_BUILD_HASHTABLE 1
#define HJ_NEED_NEW_OUTER 2
#define HJ_SCAN_BUCKET 3
#define HJ_FILL_OUTER_TUPLE 4
#define HJ_FILL_INNER_TUPLES 5
#define HJ_NEED_NEW_BATCH 6

/* Returns true if doing null-fill on outer relation */
#define HJ_FILL_OUTER(hjstate) ((hjstate)->hj_NullInnerTupleSlot != NULL)
/* Returns true if doing null-fill on inner relation */
#define HJ_FILL_INNER(hjstate) ((hjstate)->hj_NullOuterTupleSlot != NULL)

static TupleTableSlot* ExecHashJoinOuterGetTuple(PlanState* outerNode, HashJoinState* hjstate, uint32* hashvalue);
static TupleTableSlot* ExecHashJoinGetSavedTuple(
    HashJoinState* hjstate, BufFile* file, uint32* hashvalue, TupleTableSlot* tupleSlot);
static bool ExecHashJoinNewBatch(HashJoinState* hjstate);

/* ----------------------------------------------------------------
 *		ExecHashJoin
 *
 *		This function implements the Hybrid Hashjoin algorithm.
 *
 *		Note: the relation we build hash table on is the "inner"
 *			  the other one is "outer".
 * ----------------------------------------------------------------
 */
/* return: a tuple or NULL */
TupleTableSlot* ExecHashJoin(HashJoinState* node)
{
    PlanState* outerNode = NULL;
    HashState* hashNode = NULL;
    List* joinqual = NIL;
    List* otherqual = NIL;
    ExprContext* econtext = NULL;
    ExprDoneCond isDone;
    HashJoinTable hashtable;
    TupleTableSlot* outerTupleSlot = NULL;
    uint32 hashvalue;
    int batchno;
    MemoryContext oldcxt;
    JoinType jointype;

    /*
     * get information from HashJoin node
     */
    joinqual = node->js.joinqual;
    otherqual = node->js.ps.qual;
    hashNode = (HashState*)innerPlanState(node);
    outerNode = outerPlanState(node);
    hashtable = node->hj_HashTable;
    econtext = node->js.ps.ps_ExprContext;
    jointype = node->js.jointype;

    /*
     * Check to see if we're still projecting out tuples from a previous join
     * tuple (because there is a function-returning-set in the projection
     * expressions).  If so, try to project another one.
     */
    if (node->js.ps.ps_TupFromTlist) {
        TupleTableSlot* result = NULL;

        result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);
        if (isDone == ExprMultipleResult)
            return result;
        /* Done with that source tuple... */
        node->js.ps.ps_TupFromTlist = false;
    }

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.  Note this can't happen
     * until we're done projecting out tuples from a join tuple.
     */
    ResetExprContext(econtext);

    /*
     * run the hash join state machine
     */
    for (;;) {
        switch (node->hj_JoinState) {
            case HJ_BUILD_HASHTABLE: {
                /*
                 * First time through: build hash table for inner relation.
                 */
                Assert(hashtable == NULL);
                /*
                 * If the outer relation is completely empty, and it's not
                 * right/full join, we can quit without building the hash
                 * table.  However, for an inner join it is only a win to
                 * check this when the outer relation's startup cost is less
                 * than the projected cost of building the hash table.
                 * Otherwise it's best to build the hash table first and see
                 * if the inner relation is empty.	(When it's a left join, we
                 * should always make this check, since we aren't going to be
                 * able to skip the join on the strength of an empty inner
                 * relation anyway.)
                 *
                 * If we are rescanning the join, we make use of information
                 * gained on the previous scan: don't bother to try the
                 * prefetch if the previous scan found the outer relation
                 * nonempty. This is not 100% reliable since with new
                 * parameters the outer relation might yield different
                 * results, but it's a good heuristic.
                 *
                 * The only way to make the check is to try to fetch a tuple
                 * from the outer plan node.  If we succeed, we have to stash
                 * it away for later consumption by ExecHashJoinOuterGetTuple.
                 */
                // remove node->hj_streamBothSides after stream hang problem sloved.
                if (HJ_FILL_INNER(node)) {
                    /* no chance to not build the hash table */
                    node->hj_FirstOuterTupleSlot = NULL;
                } else if ((HJ_FILL_OUTER(node) || (outerNode->plan->startup_cost < hashNode->ps.plan->total_cost &&
                                                       !node->hj_OuterNotEmpty)) &&
                           !node->hj_streamBothSides) {
                    node->hj_FirstOuterTupleSlot = ExecProcNode(outerNode);
                    if (TupIsNull(node->hj_FirstOuterTupleSlot)) {
                        node->hj_OuterNotEmpty = false;

                        /*
                         * If the outer relation is completely empty, and it's not right/full join,
                         * we should deinit the consumer in right tree earlier.
                         * It should be noticed that we can not do early deinit 
                         * within predpush.
                         */
                        if (((PlanState*)node) != NULL && !CheckParamWalker((PlanState*)node)) {
                            ExecEarlyDeinitConsumer((PlanState*)node);
                        }
                        ExecEarlyFree((PlanState*)node);

                        EARLY_FREE_LOG(elog(LOG, "Early Free: HashJoin early return NULL"
                            " at node %d, memory used %d MB.", (node->js.ps.plan)->plan_node_id,
                            getSessionMemoryUsageMB()));
                        return NULL;
                    } else
                        node->hj_OuterNotEmpty = true;
                } else
                    node->hj_FirstOuterTupleSlot = NULL;

                /*
                 * create the hash table, sometimes we should keep nulls
                 */
                oldcxt = MemoryContextSwitchTo(hashNode->ps.nodeContext);
                hashtable = ExecHashTableCreate((Hash*)hashNode->ps.plan, node->hj_HashOperators,
                    HJ_FILL_INNER(node) || node->js.nulleqqual != NIL);
                MemoryContextSwitchTo(oldcxt);
                node->hj_HashTable = hashtable;

                /*
                 * execute the Hash node, to build the hash table
                 */
                WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_BUILD_HASH);
                hashNode->hashtable = hashtable;
                hashNode->ps.hbktScanSlot.currSlot = node->js.ps.hbktScanSlot.currSlot;
                (void)MultiExecProcNode((PlanState*)hashNode);
                (void)pgstat_report_waitstatus(oldStatus);

                /* Early free right tree after hash table built */
                ExecEarlyFree((PlanState*)hashNode);

                EARLY_FREE_LOG(elog(LOG, "Early Free: Hash Table for HashJoin"
                    " is built at node %d, memory used %d MB.",
                    (node->js.ps.plan)->plan_node_id, getSessionMemoryUsageMB()));

                /*
                 * If the inner relation is completely empty, and we're not
                 * doing a left outer join, we can quit without scanning the
                 * outer relation.
                 */
                if (hashtable->totalTuples == 0 && !HJ_FILL_OUTER(node)) {
                    /*
                     * When hash table size is zero, no need to fetch left tree any more and
                     * should deinit the consumer in left tree earlier.
                     * It should be noticed that we can not do early deinit 
                     * within predpush.
                     */
                    if (((PlanState*)node) != NULL && !CheckParamWalker((PlanState*)node)) {
                        ExecEarlyDeinitConsumer((PlanState*)node);
                    }

                    return NULL;
                }

                /*
                 * need to remember whether nbatch has increased since we
                 * began scanning the outer relation
                 */
                hashtable->nbatch_outstart = hashtable->nbatch;

                /*
                 * Reset OuterNotEmpty for scan.  (It's OK if we fetched a
                 * tuple above, because ExecHashJoinOuterGetTuple will
                 * immediately set it again.)
                 */
                node->hj_OuterNotEmpty = false;

                node->hj_JoinState = HJ_NEED_NEW_OUTER;
            }
            /* fall through */
            case HJ_NEED_NEW_OUTER:

                /*
                 * We don't have an outer tuple, try to get the next one
                 */
                outerTupleSlot = ExecHashJoinOuterGetTuple(outerNode, node, &hashvalue);
                if (TupIsNull(outerTupleSlot)) {
                    /* end of batch, or maybe whole join */
                    if (HJ_FILL_INNER(node)) {
                        /* set up to scan for unmatched inner tuples */
                        ExecPrepHashTableForUnmatched(node);
                        node->hj_JoinState = HJ_FILL_INNER_TUPLES;
                    } else
                        node->hj_JoinState = HJ_NEED_NEW_BATCH;
                    continue;
                }

                econtext->ecxt_outertuple = outerTupleSlot;
                node->hj_MatchedOuter = false;

                /*
                 * Find the corresponding bucket for this tuple in the main
                 * hash table or skew hash table.
                 */
                node->hj_CurHashValue = hashvalue;
                ExecHashGetBucketAndBatch(hashtable, hashvalue, &node->hj_CurBucketNo, &batchno);
                node->hj_CurSkewBucketNo = ExecHashGetSkewBucket(hashtable, hashvalue);
                node->hj_CurTuple = NULL;

                /*
                 * The tuple might not belong to the current batch (where
                 * "current batch" includes the skew buckets if any).
                 */
                if (batchno != hashtable->curbatch && node->hj_CurSkewBucketNo == INVALID_SKEW_BUCKET_NO) {
                    /*
                     * Need to postpone this outer tuple to a later batch.
                     * Save it in the corresponding outer-batch file.
                     */
                    Assert(batchno > hashtable->curbatch);
                    MinimalTuple tuple = ExecFetchSlotMinimalTuple(outerTupleSlot);
                    ExecHashJoinSaveTuple(tuple, hashvalue, &hashtable->outerBatchFile[batchno]);
                    *hashtable->spill_size += sizeof(uint32) + tuple->t_len;
                    pgstat_increase_session_spill_size(sizeof(uint32) + tuple->t_len);

                    /* Loop around, staying in HJ_NEED_NEW_OUTER state */
                    continue;
                }

                /* OK, let's scan the bucket for matches */
                node->hj_JoinState = HJ_SCAN_BUCKET;

                /* Prepare for the clear-process if necessary */
                if (jointype == JOIN_RIGHT_ANTI || jointype == JOIN_RIGHT_SEMI)
                    node->hj_PreTuple = NULL;

                /* fall through */
            case HJ_SCAN_BUCKET:

                /*
                 * We check for interrupts here because this corresponds to
                 * where we'd fetch a row from a child plan node in other join
                 * types.
                 */
                CHECK_FOR_INTERRUPTS();

                /*
                 * Scan the selected hash bucket for matches to current outer
                 */
                if (!ExecScanHashBucket(node, econtext)) {
                    /* out of matches; check for possible outer-join fill */
                    node->hj_JoinState = HJ_FILL_OUTER_TUPLE;
                    continue;
                }

                /*
                 * We've got a match, but still need to test non-hashed quals.
                 * ExecScanHashBucket already set up all the state needed to
                 * call ExecQual.
                 *
                 * If we pass the qual, then save state for next call and have
                 * ExecProject form the projection, store it in the tuple
                 * table, and return the slot.
                 *
                 * Only the joinquals determine tuple match status, but all
                 * quals must pass to actually return the tuple.
                 */
                if (joinqual == NIL || ExecQual(joinqual, econtext, false)) {
                    node->hj_MatchedOuter = true;

                    /*
                     * for right-anti join: skip and delete the matched tuple;
                     * for right-semi join: return and delete the matched tuple;
                     * for right-anti-full join: skip and delete the matched tuple;
                     */
                    if (jointype == JOIN_RIGHT_ANTI || jointype == JOIN_RIGHT_SEMI ||
                        jointype == JOIN_RIGHT_ANTI_FULL) {
                        if (node->hj_PreTuple)
                            node->hj_PreTuple->next = node->hj_CurTuple->next;
                        else if (node->hj_CurSkewBucketNo != INVALID_SKEW_BUCKET_NO)
                            hashtable->skewBucket[node->hj_CurSkewBucketNo]->tuples = node->hj_CurTuple->next;
                        else
                            hashtable->buckets[node->hj_CurBucketNo] = node->hj_CurTuple->next;
                        if (jointype == JOIN_RIGHT_ANTI || jointype == JOIN_RIGHT_ANTI_FULL)
                            continue;
                    } else {
                        HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTuple));

                        /* Anti join: we never return a matched tuple */
                        if (jointype == JOIN_ANTI || jointype == JOIN_LEFT_ANTI_FULL) {
                            node->hj_JoinState = HJ_NEED_NEW_OUTER;
                            continue;
                        }

                        /* Semi join: we'll consider returning the first match, but after
                         *	that we're done with this outer tuple */
                        if (jointype == JOIN_SEMI)
                            node->hj_JoinState = HJ_NEED_NEW_OUTER;
                    }

                    if (otherqual == NIL || ExecQual(otherqual, econtext, false)) {
                        TupleTableSlot* result = NULL;

                        result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

                        if (isDone != ExprEndResult) {
                            node->js.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
                            return result;
                        }
                    } else
                        InstrCountFiltered2(node, 1);
                } else {
                    InstrCountFiltered1(node, 1);
                    /* For right Semi/Anti join, we set hj_PreTuple following hj_CurTuple */
                    if (jointype == JOIN_RIGHT_ANTI || jointype == JOIN_RIGHT_SEMI)
                        node->hj_PreTuple = node->hj_CurTuple;
                }
                break;

            case HJ_FILL_OUTER_TUPLE:

                /*
                 * The current outer tuple has run out of matches, so check
                 * whether to emit a dummy outer-join tuple.  Whether we emit
                 * one or not, the next state is NEED_NEW_OUTER.
                 */
                node->hj_JoinState = HJ_NEED_NEW_OUTER;

                if (!node->hj_MatchedOuter && HJ_FILL_OUTER(node)) {
                    /*
                     * Generate a fake join tuple with nulls for the inner
                     * tuple, and return it if it passes the non-join quals.
                     */
                    econtext->ecxt_innertuple = node->hj_NullInnerTupleSlot;

                    if (otherqual == NIL || ExecQual(otherqual, econtext, false)) {
                        TupleTableSlot* result = NULL;

                        result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

                        if (isDone != ExprEndResult) {
                            node->js.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
                            return result;
                        }
                    } else
                        InstrCountFiltered2(node, 1);
                }
                break;

            case HJ_FILL_INNER_TUPLES:

                /*
                 * We have finished a batch, but we are doing right/full/rightAnti join,
                 * so any unmatched inner tuples in the hashtable have to be
                 * emitted before we continue to the next batch.
                 */
                if (!ExecScanHashTableForUnmatched(node, econtext)) {
                    /* no more unmatched tuples */
                    node->hj_JoinState = HJ_NEED_NEW_BATCH;
                    continue;
                }

                /*
                 * Generate a fake join tuple with nulls for the outer tuple,
                 * and return it if it passes the non-join quals.
                 */
                econtext->ecxt_outertuple = node->hj_NullOuterTupleSlot;

                if (otherqual == NIL || ExecQual(otherqual, econtext, false)) {
                    TupleTableSlot* result = NULL;

                    result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

                    if (isDone != ExprEndResult) {
                        node->js.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
                        return result;
                    }
                } else
                    InstrCountFiltered2(node, 1);
                break;

            case HJ_NEED_NEW_BATCH:

                /*
                 * Try to advance to next batch.  Done if there are no more.
                 */
                if (!ExecHashJoinNewBatch(node)) {
                    ExecEarlyFree(outerPlanState(node));
                    EARLY_FREE_LOG(elog(LOG, "Early Free: HashJoin Probe is done"
                        " at node %d, memory used %d MB.",
                        (node->js.ps.plan)->plan_node_id, getSessionMemoryUsageMB()));

                    return NULL; /* end of join */
                }
                node->hj_JoinState = HJ_NEED_NEW_OUTER;
                break;

            default:
                ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                        errmodule(MOD_EXECUTOR), errmsg("unrecognized hashjoin state: %d", (int)node->hj_JoinState)));
        }
    }
}

/* ----------------------------------------------------------------
 *		FindParam
 *
 *		Walk through plan tree and find Param node.
 * ----------------------------------------------------------------
 */
bool FindParam(Node* node_plan, void* context)
{
    if (node_plan == NULL) {
        return false;
    }

    if (IsA(node_plan, Param) && ((Param*)node_plan)->paramkind != PARAM_EXTERN) {
        ((PredpushPlanWalkerContext*)context)->predpush_stream = true;
        return true;
    }

    if (IsA(node_plan, Stream)) {
        return false;
    }

    return plan_tree_walker(node_plan, (MethodWalker)FindParam, (void*)context);
}

/* ----------------------------------------------------------------
 *		CheckParamWalker
 *
 *		Return true if we find a Param node in the plan tree.
 * ----------------------------------------------------------------
 */
bool CheckParamWalker(PlanState* plan_stat)
{
    Plan *temp_plan = plan_stat->plan;

    if (plan_stat->state != NULL) {
        PlannedStmt *temp_ps = plan_stat->state->es_plannedstmt;
        PredpushPlanWalkerContext context;
        errno_t rc = 0;
        rc = memset_s(&context, sizeof(PredpushPlanWalkerContext), 0, sizeof(PredpushPlanWalkerContext));
        securec_check(rc, "\0", "\0");

        exec_init_plan_tree_base(&context.mpwc.base, temp_ps);

        context.predpush_stream = false;

        FindParam((Node*)temp_plan, &context);
        return context.predpush_stream;
    }

    return true;
}

/* ----------------------------------------------------------------
 *		ExecInitHashJoin
 *
 *		Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
HashJoinState* ExecInitHashJoin(HashJoin* node, EState* estate, int eflags)
{
    HashJoinState* hjstate = NULL;
    Plan* outerNode = NULL;
    Hash* hashNode = NULL;
    List* lclauses = NIL;
    List* rclauses = NIL;
    List* hoperators = NIL;
    ListCell* l = NULL;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * create state structure
     */
    hjstate = makeNode(HashJoinState);
    hjstate->js.ps.plan = (Plan*)node;
    hjstate->js.ps.state = estate;
    hjstate->hj_streamBothSides = node->streamBothSides;
    hjstate->hj_rebuildHashtable = node->rebuildHashTable;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &hjstate->js.ps);

    /*
     * initialize child expressions
     */
    hjstate->js.ps.targetlist = (List*)ExecInitExpr((Expr*)node->join.plan.targetlist, (PlanState*)hjstate);
    hjstate->js.ps.qual = (List*)ExecInitExpr((Expr*)node->join.plan.qual, (PlanState*)hjstate);
    hjstate->js.jointype = node->join.jointype;
    hjstate->js.joinqual = (List*)ExecInitExpr((Expr*)node->join.joinqual, (PlanState*)hjstate);
    hjstate->js.nulleqqual = (List*)ExecInitExpr((Expr*)node->join.nulleqqual, (PlanState*)hjstate);
    hjstate->hashclauses = (List*)ExecInitExpr((Expr*)node->hashclauses, (PlanState*)hjstate);

    /*
     * initialize child nodes
     *
     * Note: we could suppress the REWIND flag for the inner input, which
     * would amount to betting that the hash will be a single batch.  Not
     * clear if this would be a win or not.
     */
    outerNode = outerPlan(node);
    hashNode = (Hash*)innerPlan(node);

    outerPlanState(hjstate) = ExecInitNode(outerNode, estate, eflags);
    innerPlanState(hjstate) = ExecInitNode((Plan*)hashNode, estate, eflags);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &hjstate->js.ps);
    hjstate->hj_OuterTupleSlot = ExecInitExtraTupleSlot(estate);

    /* set up null tuples for outer joins, if needed */
    switch (node->join.jointype) {
        case JOIN_INNER:
        case JOIN_SEMI:
        case JOIN_RIGHT_SEMI:
            break;
        case JOIN_LEFT:
        case JOIN_ANTI:
        case JOIN_LEFT_ANTI_FULL:
            hjstate->hj_NullInnerTupleSlot = ExecInitNullTupleSlot(estate, ExecGetResultType(innerPlanState(hjstate)));
            break;
        case JOIN_RIGHT:
        case JOIN_RIGHT_ANTI:
        case JOIN_RIGHT_ANTI_FULL:
            hjstate->hj_NullOuterTupleSlot = ExecInitNullTupleSlot(estate, ExecGetResultType(outerPlanState(hjstate)));
            break;
        case JOIN_FULL:
            hjstate->hj_NullOuterTupleSlot = ExecInitNullTupleSlot(estate, ExecGetResultType(outerPlanState(hjstate)));
            hjstate->hj_NullInnerTupleSlot = ExecInitNullTupleSlot(estate, ExecGetResultType(innerPlanState(hjstate)));
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_EXECUTOR),
                    errmsg("unrecognized join type: %d for hashjoin", (int)node->join.jointype)));
    }

    /*
     * now for some voodoo.  our temporary tuple slot is actually the result
     * tuple slot of the Hash node (which is our inner plan).  we can do this
     * because Hash nodes don't return tuples via ExecProcNode() -- instead
     * the hash join node uses ExecScanHashBucket() to get at the contents of
     * the hash table.	-cim 6/9/91
     */
    {
        HashState* hashstate = (HashState*)innerPlanState(hjstate);
        TupleTableSlot* slot = hashstate->ps.ps_ResultTupleSlot;

        hjstate->hj_HashTupleSlot = slot;
    }

    /*
     * initialize tuple type and projection info
     * result tupleSlot only contains virtual tuple, so the default
     * tableAm type is set to HEAP.
     */
    ExecAssignResultTypeFromTL(&hjstate->js.ps, TAM_HEAP);
    ExecAssignProjectionInfo(&hjstate->js.ps, NULL);

    ExecSetSlotDescriptor(hjstate->hj_OuterTupleSlot, ExecGetResultType(outerPlanState(hjstate)));

    /*
     * initialize hash-specific info
     */
    hjstate->hj_HashTable = NULL;
    hjstate->hj_FirstOuterTupleSlot = NULL;

    hjstate->hj_CurHashValue = 0;
    hjstate->hj_CurBucketNo = 0;
    hjstate->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
    hjstate->hj_CurTuple = NULL;

    /*
     * Deconstruct the hash clauses into outer and inner argument values, so
     * that we can evaluate those subexpressions separately.  Also make a list
     * of the hash operator OIDs, in preparation for looking up the hash
     * functions to use.
     */
    lclauses = NIL;
    rclauses = NIL;
    hoperators = NIL;
    foreach (l, hjstate->hashclauses) {
        FuncExprState* fstate = (FuncExprState*)lfirst(l);
        OpExpr* hclause = NULL;

        Assert(IsA(fstate, FuncExprState));
        hclause = (OpExpr*)fstate->xprstate.expr;
        Assert(IsA(hclause, OpExpr));
        lclauses = lappend(lclauses, linitial(fstate->args));
        rclauses = lappend(rclauses, lsecond(fstate->args));
        hoperators = lappend_oid(hoperators, hclause->opno);
    }
    hjstate->hj_OuterHashKeys = lclauses;
    hjstate->hj_InnerHashKeys = rclauses;
    hjstate->hj_HashOperators = hoperators;
    /* child Hash node needs to evaluate inner hash keys, too */
    ((HashState*)innerPlanState(hjstate))->hashkeys = rclauses;

    hjstate->js.ps.ps_TupFromTlist = false;
    hjstate->hj_JoinState = HJ_BUILD_HASHTABLE;
    hjstate->hj_MatchedOuter = false;
    hjstate->hj_OuterNotEmpty = false;

    return hjstate;
}

/* ----------------------------------------------------------------
 *		ExecEndHashJoin
 *
 *		clean up routine for HashJoin node
 * ----------------------------------------------------------------
 */
void ExecEndHashJoin(HashJoinState* node)
{
    /*
     * Free hash table
     */
    if (node->hj_HashTable) {
        ExecHashTableDestroy(node->hj_HashTable);
        node->hj_HashTable = NULL;
    }

    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->js.ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->hj_OuterTupleSlot);
    (void)ExecClearTuple(node->hj_HashTupleSlot);

    /*
     * clean up subtrees
     */
    ExecEndNode(outerPlanState(node));
    ExecEndNode(innerPlanState(node));
}

/*
 * ExecHashJoinOuterGetTuple
 *
 *		get the next outer tuple for hashjoin: either by
 *		executing the outer plan node in the first pass, or from
 *		the temp files for the hashjoin batches.
 *
 * Returns a null slot if no more outer tuples (within the current batch).
 *
 * On success, the tuple's hash value is stored at *hashvalue --- this is
 * either originally computed, or re-read from the temp file.
 */
static TupleTableSlot* ExecHashJoinOuterGetTuple(PlanState* outerNode, HashJoinState* hjstate, uint32* hashvalue)
{
    HashJoinTable hashtable = hjstate->hj_HashTable;
    int curbatch = hashtable->curbatch;
    TupleTableSlot* slot = NULL;
    /* if it is the first pass */
    if (curbatch == 0) {
        /*
         * Check to see if first outer tuple was already fetched by
         * ExecHashJoin() and not used yet.
         */
        slot = hjstate->hj_FirstOuterTupleSlot;
        if (!TupIsNull(slot))
            hjstate->hj_FirstOuterTupleSlot = NULL;
        else
            slot = ExecProcNode(outerNode);

        while (!TupIsNull(slot)) {
            /*
             * We have to compute the tuple's hash value.
             */
            ExprContext* econtext = hjstate->js.ps.ps_ExprContext;

            econtext->ecxt_outertuple = slot;
            if (ExecHashGetHashValue(hashtable,
                                    econtext,
                                    hjstate->hj_OuterHashKeys,
                                    true,                                                    /* outer tuple */
                                    HJ_FILL_OUTER(hjstate) || hjstate->js.nulleqqual != NIL, /* compute null ? */
                                    hashvalue)) {
                /* remember outer relation is not empty for possible rescan */
                hjstate->hj_OuterNotEmpty = true;

                return slot;
            }

            /*
             * That tuple couldn't match because of a NULL, so discard it and
             * continue with the next one.
             */
            slot = ExecProcNode(outerNode);
        }
    } else if (curbatch < hashtable->nbatch) {
        BufFile* file = hashtable->outerBatchFile[curbatch];

        /*
         * In outer-join cases, we could get here even though the batch file
         * is empty.
         */
        if (file == NULL)
            return NULL;

        slot = ExecHashJoinGetSavedTuple(hjstate, file, hashvalue, hjstate->hj_OuterTupleSlot);
        if (!TupIsNull(slot))
            return slot;
    }

    /* End of this batch */
    return NULL;
}

/*
 * ExecHashJoinNewBatch
 *		switch to a new hashjoin batch
 *
 * Returns true if successful, false if there are no more batches.
 */
static bool ExecHashJoinNewBatch(HashJoinState* hjstate)
{
    HashJoinTable hashtable = hjstate->hj_HashTable;
    int nbatch;
    int curbatch;
    BufFile* innerFile = NULL;
    TupleTableSlot* slot = NULL;
    uint32 hashvalue;

    nbatch = hashtable->nbatch;
    curbatch = hashtable->curbatch;

    if (curbatch > 0) {
        /*
         * We no longer need the previous outer batch file; close it right
         * away to free disk space.
         */
        if (hashtable->outerBatchFile[curbatch])
            BufFileClose(hashtable->outerBatchFile[curbatch]);
        hashtable->outerBatchFile[curbatch] = NULL;
        /* we just finished the first batch */
    } else {
        /*
         * Reset some of the skew optimization state variables, since we no
         * longer need to consider skew tuples after the first batch. The
         * memory context reset we are about to do will release the skew
         * hashtable itself.
         */
        hashtable->skewEnabled = false;
        hashtable->skewBucket = NULL;
        hashtable->skewBucketNums = NULL;
        hashtable->nSkewBuckets = 0;
        hashtable->spaceUsedSkew = 0;
    }

    /*
     * We can always skip over any batches that are completely empty on both
     * sides.  We can sometimes skip over batches that are empty on only one
     * side, but there are exceptions:
     *
     * 1. In a left/full outer join, we have to process outer batches even if
     * the inner batch is empty.  Similarly, in a right/full outer join, we
     * have to process inner batches even if the outer batch is empty.
     *
     * 2. If we have increased nbatch since the initial estimate, we have to
     * scan inner batches since they might contain tuples that need to be
     * reassigned to later inner batches.
     *
     * 3. Similarly, if we have increased nbatch since starting the outer
     * scan, we have to rescan outer batches in case they contain tuples that
     * need to be reassigned.
     */
    curbatch++;
    while (curbatch < nbatch &&
           (hashtable->outerBatchFile[curbatch] == NULL || hashtable->innerBatchFile[curbatch] == NULL)) {
        if (hashtable->outerBatchFile[curbatch] && HJ_FILL_OUTER(hjstate))
            break; /* must process due to rule 1 */
        if (hashtable->innerBatchFile[curbatch] && HJ_FILL_INNER(hjstate))
            break; /* must process due to rule 1 */
        if (hashtable->innerBatchFile[curbatch] && nbatch != hashtable->nbatch_original)
            break; /* must process due to rule 2 */
        if (hashtable->outerBatchFile[curbatch] && nbatch != hashtable->nbatch_outstart)
            break; /* must process due to rule 3 */
        /* We can ignore this batch. */
        /* Release associated temp files right away. */
        if (hashtable->innerBatchFile[curbatch])
            BufFileClose(hashtable->innerBatchFile[curbatch]);
        hashtable->innerBatchFile[curbatch] = NULL;
        if (hashtable->outerBatchFile[curbatch])
            BufFileClose(hashtable->outerBatchFile[curbatch]);
        hashtable->outerBatchFile[curbatch] = NULL;
        curbatch++;
    }

    if (curbatch >= nbatch) {
        return false; /* no more batches */
    }

    hashtable->curbatch = curbatch;

    /*
     * Reload the hash table with the new inner batch (which could be empty)
     */
    ExecHashTableReset(hashtable);

    innerFile = hashtable->innerBatchFile[curbatch];

    if (innerFile != NULL) {
        if (BufFileSeek(innerFile, 0, 0L, SEEK_SET)) {
            ereport(
                ERROR, (errcode_for_file_access(), errmsg("could not rewind hash-join build side temporary file: %m")));
        }

        while ((slot = ExecHashJoinGetSavedTuple(hjstate, innerFile, &hashvalue, hjstate->hj_HashTupleSlot))) {
            /*
             * NOTE: some tuples may be sent to future batches.  Also, it is
             * possible for hashtable->nbatch to be increased here!
             */
            ExecHashTableInsert(hashtable,
                slot,
                hashvalue,
                hjstate->js.ps.plan->righttree->plan_node_id,
                SET_DOP(hjstate->js.ps.plan->righttree->dop));
        }

        /* analysis hash table information created in memory */
        if (anls_opt_is_on(ANLS_HASH_CONFLICT))
            ExecHashTableStats(hashtable, hjstate->js.ps.plan->righttree->plan_node_id);

        /*
         * after we build the hash table, the inner batch file is no longer
         * needed
         */
        BufFileClose(innerFile);
        hashtable->innerBatchFile[curbatch] = NULL;
    }

    /*
     * Rewind outer batch file (if present), so that we can start reading it.
     */
    if (hashtable->outerBatchFile[curbatch] != NULL) {
        if (BufFileSeek(hashtable->outerBatchFile[curbatch], 0, 0L, SEEK_SET))
            ereport(
                ERROR, (errcode_for_file_access(), errmsg("could not rewind hash-join probe side temporary file: %m")));
    }

    return true;
}

/*
 * ExecHashJoinSaveTuple
 *		save a tuple to a batch file.
 *
 * The data recorded in the file for each tuple is its hash value,
 * then the tuple in MinimalTuple format.
 *
 * Note: it is important always to call this in the regular executor
 * context, not in a shorter-lived context; else the temp file buffers
 * will get messed up.
 */
void ExecHashJoinSaveTuple(MinimalTuple tuple, uint32 hashvalue, BufFile** fileptr)
{
    BufFile* file = *fileptr;
    size_t written;

    if (file == NULL) {
        /* First write to this batch file, so open it. */
        file = BufFileCreateTemp(false);
        *fileptr = file;
    }

    written = BufFileWrite(file, (void*)&hashvalue, sizeof(uint32));
    if (written != sizeof(uint32))
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not write hashvalue %u to hash-join temporary file, written length %lu.",
                    hashvalue, written)));

    written = BufFileWrite(file, (void*)tuple, tuple->t_len);
    if (written != tuple->t_len)
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not write tuple to hash-join temporary file: written length %lu, tuple length %u",
                    written, tuple->t_len)));

    /* increase current session spill count */
    pgstat_increase_session_spill();
}

/*
 * ExecHashJoinGetSavedTuple
 *		read the next tuple from a batch file.	Return NULL if no more.
 *
 * On success, *hashvalue is set to the tuple's hash value, and the tuple
 * itself is stored in the given slot.
 */
static TupleTableSlot* ExecHashJoinGetSavedTuple(
    HashJoinState* hjstate, BufFile* file, uint32* hashvalue, TupleTableSlot* tupleSlot)
{
    uint32 header[2];
    size_t nread;
    MinimalTuple tuple;

    /*
     * We check for interrupts here because this is typically taken as an
     * alternative code path to an ExecProcNode() call, which would include
     * such a check.
     */
    CHECK_FOR_INTERRUPTS();

    /*
     * Since both the hash value and the MinimalTuple length word are uint32,
     * we can read them both in one BufFileRead() call without any type
     * cheating.
     */
    nread = BufFileRead(file, (void*)header, sizeof(header));
    if (nread == 0) {
        (void)ExecClearTuple(tupleSlot);
        return NULL;
    }
    if (nread != sizeof(header)) {
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not read from hash-join temporary file: read length %zu", nread)));
    }

    if (header[1] < sizeof(uint32)) {
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("The hash-join temporary file is corrupted,hashvalue:%u, length:%u.", header[0], header[1])));
    }

    *hashvalue = header[0];
    tuple = (MinimalTuple)palloc(header[1]);
    tuple->t_len = header[1];
    nread = BufFileRead(file, (void*)((char*)tuple + sizeof(uint32)), header[1] - sizeof(uint32));
    if (nread != header[1] - sizeof(uint32)) {
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not read from hash-join temporary file(t_len:%u,nread:%lu): %m",
                    header[1], (unsigned long)nread)));
    }
    return ExecStoreMinimalTuple(tuple, tupleSlot, true);
}

void ExecReScanHashJoin(HashJoinState* node)
{
    /* Already reset, just rescan righttree and lefttree */
    if (node->js.ps.recursive_reset && node->js.ps.state->es_recursive_next_iteration) {
        if (node->js.ps.righttree->chgParam == NULL)
            ExecReScan(node->js.ps.righttree);

        if (node->js.ps.lefttree->chgParam == NULL)
            ExecReScan(node->js.ps.lefttree);

        node->js.ps.recursive_reset = false;
        return;
    }

    /*
     * In a multi-batch join, we currently have to do rescans the hard way,
     * primarily because batch temp files may have already been released. But
     * if it's a single-batch join, and there is no parameter change for the
     * inner subnode, then we can just re-use the existing hash table without
     * rebuilding it.
     */
    if (node->hj_HashTable != NULL) {
        if (!node->js.ps.plan->ispwj && node->hj_HashTable->nbatch == 1 && node->js.ps.righttree->chgParam == NULL &&
            !node->hj_rebuildHashtable && node->js.jointype != JOIN_RIGHT_SEMI &&
            node->js.jointype != JOIN_RIGHT_ANTI) {
            /*
             * Okay to reuse the hash table; needn't rescan inner, either.
             *
             * However, if it's a right/full join, we'd better reset the
             * inner-tuple match flags contained in the table.
             */
            if (HJ_FILL_INNER(node))
                ExecHashTableResetMatchFlags(node->hj_HashTable);

            /*
             * Also, we need to reset our state about the emptiness of the
             * outer relation, so that the new scan of the outer will update
             * it correctly if it turns out to be empty this time. (There's no
             * harm in clearing it now because ExecHashJoin won't need the
             * info.  In the other cases, where the hash table doesn't exist
             * or we are destroying it, we leave this state alone because
             * ExecHashJoin will need it the first time through.)
             */
            node->hj_OuterNotEmpty = false;

            /* ExecHashJoin can skip the BUILD_HASHTABLE step */
            node->hj_JoinState = HJ_NEED_NEW_OUTER;
        } else {
            /* must destroy and rebuild hash table */
            ExecHashTableDestroy(node->hj_HashTable);
            node->hj_HashTable = NULL;
            node->hj_JoinState = HJ_BUILD_HASHTABLE;

            /*
             * if chgParam of subnode is not null then plan will be re-scanned
             * by first ExecProcNode.
             */
            // swtich to next partition, in the right tree
            if (node->js.ps.righttree->chgParam == NULL)
                ExecReScan(node->js.ps.righttree);
        }
    } else {
        if (node->js.ps.plan->ispwj) {
            // no need to destroy hash table, just build it.
            node->hj_HashTable = NULL;
            node->hj_JoinState = HJ_BUILD_HASHTABLE;

            // swtich to next partition, in the right tree
            if (node->js.ps.righttree->chgParam == NULL) {
                ExecReScan(node->js.ps.righttree);
            }
        }
    }

    /* Always reset intra-tuple state */
    node->hj_CurHashValue = 0;
    node->hj_CurBucketNo = 0;
    node->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
    node->hj_CurTuple = NULL;

    node->js.ps.ps_TupFromTlist = false;
    node->hj_MatchedOuter = false;
    node->hj_FirstOuterTupleSlot = NULL;

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->js.ps.lefttree->chgParam == NULL)
        ExecReScan(node->js.ps.lefttree);
}

/*
 * @Description: Early free the memory for HashJoin.
 *
 * @param[IN] node:  executor state for HashJoin
 * @return: void
 */
void ExecEarlyFreeHashJoin(HashJoinState* node)
{
    PlanState* plan_state = &node->js.ps;

    if (plan_state->earlyFreed)
        return;

    /*
     * Free hash table
     */
    if (node->hj_HashTable) {
        ExecHashTableDestroy(node->hj_HashTable);
        node->hj_HashTable = NULL;
    }

    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->js.ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->hj_OuterTupleSlot);
    (void)ExecClearTuple(node->hj_HashTupleSlot);

    EARLY_FREE_LOG(elog(LOG,
        "Early Free: After early freeing HashJoin "
        "at node %d, memory used %d MB.",
        plan_state->plan->plan_node_id,
        getSessionMemoryUsageMB()));

    plan_state->earlyFreed = true;
    ExecEarlyFree(innerPlanState(node));
    ExecEarlyFree(outerPlanState(node));
}

/*
 * @Function: ExecReSetHashJoin()
 *
 * @Brief: Reset the hashjoin state structure including have hashtable be recreated
 *         so that in next round of iteration, the data of inner side is correct
 *
 * @Input node: hashjoin planstate node
 *
 * @Return: no return value
 */
void ExecReSetHashJoin(HashJoinState* node)
{
    Assert(EXEC_IN_RECURSIVE_MODE(node->js.ps.plan));

    /* must destroy and rebuild hash table */
    if (node->hj_HashTable != NULL) {
        ExecHashTableDestroy(node->hj_HashTable);
        node->hj_HashTable = NULL;
        node->hj_JoinState = HJ_BUILD_HASHTABLE;
    }
    ExecReSetRecursivePlanTree(node->js.ps.righttree);

    /* Always reset intra-tuple state */
    node->hj_CurHashValue = 0;
    node->hj_CurBucketNo = 0;
    node->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
    node->hj_CurTuple = NULL;

    node->js.ps.ps_TupFromTlist = false;
    node->hj_MatchedOuter = false;
    node->hj_FirstOuterTupleSlot = NULL;
    node->js.ps.recursive_reset = true;

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->js.ps.lefttree->chgParam == NULL)
        ExecReSetRecursivePlanTree(node->js.ps.lefttree);
}
