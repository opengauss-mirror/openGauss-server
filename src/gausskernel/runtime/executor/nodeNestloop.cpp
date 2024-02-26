/* -------------------------------------------------------------------------
 *
 * nodeNestloop.cpp
 *	  routines to support nest-loop joins
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeNestloop.cpp
 *
 * -------------------------------------------------------------------------
 *
 *	 INTERFACE ROUTINES
 *		ExecNestLoop	 - process a nestloop join of two plans
 *		ExecInitNestLoop - initialize the join
 *		ExecEndNestLoop  - shut down the join
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/tableam.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeNestloop.h"
#include "executor/exec/execStream.h"
#include "utils/memutils.h"
#include "executor/node/nodeHashjoin.h"

static TupleTableSlot* ExecNestLoop(PlanState* state);
static void MaterialAll(PlanState* node)
{
    if (IsA(node, MaterialState)) {
        ((MaterialState*)node)->materalAll = true;

        /*
         * Call ExecProcNode to material all the inner tuple first.
         */
        ExecProcNode(node);

        /* early free the left tree of Material. */
        ExecEarlyFree(outerPlanState(node));

        /* early deinit consumer in left tree of Material.
         * It should be noticed that we can not do early deinit 
         * within predpush.
         */
        if (node != NULL && !CheckParamWalker(node)) {
            ExecEarlyDeinitConsumer(node);
        }
    }
}

/* ----------------------------------------------------------------
 *		ExecNestLoop(node)
 *
 * old comments
 *		Returns the tuple joined from inner and outer tuples which
 *		satisfies the qualification clause.
 *
 *		It scans the inner relation to join with current outer tuple.
 *
 *		If none is found, next tuple from the outer relation is retrieved
 *		and the inner relation is scanned from the beginning again to join
 *		with the outer tuple.
 *
 *		NULL is returned if all the remaining outer tuples are tried and
 *		all fail to join with the inner tuples.
 *
 *		NULL is also returned if there is no tuple from inner relation.
 *
 *		Conditions:
 *		  -- outerTuple contains current tuple from outer relation and
 *			 the right son(inner relation) maintains "cursor" at the tuple
 *			 returned previously.
 *				This is achieved by maintaining a scan position on the outer
 *				relation.
 *
 *		Initial States:
 *		  -- the outer child and the inner child
 *			   are prepared to return the first tuple.
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ExecNestLoop(PlanState* state)
{
    NestLoopState* node = castNode(NestLoopState, state);
    TupleTableSlot* outer_tuple_slot = NULL;
    TupleTableSlot* inner_tuple_slot = NULL;
    ListCell* lc = NULL;

    /*
     * get information from the node
     */
    ENL1_printf("getting info from node");

    NestLoop* nl = (NestLoop*)node->js.ps.plan;
    List* joinqual = node->js.joinqual;
    List* otherqual = node->js.ps.qual;
    PlanState* outer_plan = outerPlanState(node);
    PlanState* inner_plan = innerPlanState(node);
    ExprContext* econtext = node->js.ps.ps_ExprContext;

    CHECK_FOR_INTERRUPTS();
    
    /*
     * Check to see if we're still projecting out tuples from a previous join
     * tuple (because there is a function-returning-set in the projection
     * expressions).  If so, try to project another one.
     */
    if (node->js.ps.ps_vec_TupFromTlist) {
        ExprDoneCond is_done;

        TupleTableSlot* result = ExecProject(node->js.ps.ps_ProjInfo, &is_done);
        if (is_done == ExprMultipleResult)
            return result;
        /* Done with that source tuple... */
        node->js.ps.ps_vec_TupFromTlist = false;
    }

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.  Note this can't happen
     * until we're done projecting out tuples from a join tuple.
     */
    ResetExprContext(econtext);

    /*
     * Ok, everything is setup for the join so now loop until we return a
     * qualifying join tuple.
     */
    ENL1_printf("entering main loop");

    if (node->nl_MaterialAll) {
        MaterialAll(inner_plan);
        node->nl_MaterialAll = false;
    }

    for (;;) {
        /*
         * If we don't have an outer tuple, get the next one and reset the
         * inner scan.
         */
        if (node->nl_NeedNewOuter) {
            ENL1_printf("getting new outer tuple");
            outer_tuple_slot = ExecProcNode(outer_plan);
            /*
             * if there are no more outer tuples, then the join is complete..
             */
            if (TupIsNull(outer_tuple_slot)) {
                ExecEarlyFree(inner_plan);
                ExecEarlyFree(outer_plan);

                EARLY_FREE_LOG(elog(LOG,
                    "Early Free: NestLoop is done "
                    "at node %d, memory used %d MB.",
                    (node->js.ps.plan)->plan_node_id,
                    getSessionMemoryUsageMB()));

                ENL1_printf("no outer tuple, ending join");

                return NULL;
            }

            ENL1_printf("saving new outer tuple information");
            econtext->ecxt_outertuple = outer_tuple_slot;
            node->nl_NeedNewOuter = false;
            node->nl_MatchedOuter = false;

            /*
             * fetch the values of any outer Vars that must be passed to the
             * inner scan, and store them in the appropriate PARAM_EXEC slots.
             */
            foreach (lc, nl->nestParams) {
                NestLoopParam* nlp = (NestLoopParam*)lfirst(lc);
                int paramno = nlp->paramno;
                ParamExecData* prm = NULL;

                prm = &(econtext->ecxt_param_exec_vals[paramno]);
                /* Param value should be an OUTER_VAR var */
                Assert(IsA(nlp->paramval, Var));
                Assert(nlp->paramval->varno == OUTER_VAR);
                Assert(nlp->paramval->varattno > 0);
                Assert(outer_tuple_slot != NULL && outer_tuple_slot->tts_tupleDescriptor != NULL);
                /* Get the Table Accessor Method*/
                prm->value = tableam_tslot_getattr(outer_tuple_slot, nlp->paramval->varattno, &(prm->isnull));
                /*
                 * the following two parameters are called when there exist
                 * join-operation with column table (see ExecEvalVecParamExec).
                 */
                prm->valueType = outer_tuple_slot->tts_tupleDescriptor->tdtypeid;
                prm->isChanged = true;
                /* Flag parameter value as changed */
                inner_plan->chgParam = bms_add_member(inner_plan->chgParam, paramno);
            }

            /*
             * now rescan the inner plan
             */
            ENL1_printf("rescanning inner plan");
            ExecReScan(inner_plan);
        }

        /*
         * we have an outerTuple, try to get the next inner tuple.
         */
        ENL1_printf("getting new inner tuple");

        /*
         * If inner plan is mergejoin, which does not cache data,
         * but will early free the left and right tree's caching memory.
         * When rescan left tree, may fail.
         */
        bool orig_value = inner_plan->state->es_skip_early_free;
        if (!IsA(inner_plan, MaterialState))
            inner_plan->state->es_skip_early_free = true;

        inner_tuple_slot = ExecProcNode(inner_plan);

        inner_plan->state->es_skip_early_free = orig_value;
        econtext->ecxt_innertuple = inner_tuple_slot;

        if (TupIsNull(inner_tuple_slot)) {
            ENL1_printf("no inner tuple, need new outer tuple");

            node->nl_NeedNewOuter = true;
#ifdef USE_SPQ
            node->nl_innerSideScanned = true;
            if (!node->nl_MatchedOuter && (node->js.jointype == JOIN_LEFT || node->js.jointype == JOIN_ANTI ||
                                              node->js.jointype == JOIN_LEFT_ANTI_FULL || node->js.jointype == JOIN_LASJ_NOTIN)) {
#else

            if (!node->nl_MatchedOuter && (node->js.jointype == JOIN_LEFT || node->js.jointype == JOIN_ANTI ||
                                              node->js.jointype == JOIN_LEFT_ANTI_FULL)) {
#endif
                /*
                 * We are doing an outer join and there were no join matches
                 * for this outer tuple.  Generate a fake join tuple with
                 * nulls for the inner tuple, and return it if it passes the
                 * non-join quals.
                 */
                econtext->ecxt_innertuple = node->nl_NullInnerTupleSlot;

                ENL1_printf("testing qualification for outer-join tuple");

                if (otherqual == NIL || ExecQual(otherqual, econtext, false)) {
                    /*
                     * qualification was satisfied so we project and return
                     * the slot containing the result tuple using
                     * function ExecProject.
                     */
                    ExprDoneCond is_done;

                    ENL1_printf("qualification succeeded, projecting tuple");

                    TupleTableSlot* result = ExecProject(node->js.ps.ps_ProjInfo, &is_done);

                    if (is_done != ExprEndResult) {
                        node->js.ps.ps_vec_TupFromTlist = (is_done == ExprMultipleResult);
                        return result;
                    }
                } else
                    InstrCountFiltered2(node, 1);
            }

            /*
             * Otherwise just return to top of loop for a new outer tuple.
             */
            continue;
        }

#ifdef USE_SPQ
        if ((node->js.jointype == JOIN_LASJ_NOTIN) && (!node->nl_innerSideScanned) &&
            (node->nl_InnerJoinKeys && IsJoinExprNull(node->nl_InnerJoinKeys, econtext))) {
            /*
             * If LASJ_NOTIN and a null was found on the inner side, all tuples
             * We'll read no more from either inner or outer subtree. To keep our
             * in outer sider will be treated as "not in" tuples in inner side.
             */
            ENL1_printf("found null tuple on the inner side, clean out");
            return NULL;
        }
#endif
        /*
         * at this point we have a new pair of inner and outer tuples so we
         * test the inner and outer tuples to see if they satisfy the node's
         * qualification.
         *
         * Only the joinquals determine MatchedOuter status, but all quals
         * must pass to actually return the tuple.
         */
        ENL1_printf("testing qualification");

        if (ExecQual(joinqual, econtext, false)) {
            node->nl_MatchedOuter = true;

            /* In an antijoin, we never return a matched tuple */
#ifdef USE_SPQ
            if (node->js.jointype == JOIN_ANTI || node->js.jointype == JOIN_LEFT_ANTI_FULL ||
                node->js.jointype == JOIN_LASJ_NOTIN) {
#else
            if (node->js.jointype == JOIN_ANTI || node->js.jointype == JOIN_LEFT_ANTI_FULL) {
#endif
                node->nl_NeedNewOuter = true;
                continue; /* return to top of loop */
            }

            if (node->js.single_match) {
                node->nl_NeedNewOuter = true;
            }

            if (otherqual == NIL || ExecQual(otherqual, econtext, false)) {
                /*
                 * qualification was satisfied so we project and return the
                 * slot containing the result tuple using ExecProject().
                 */
                ExprDoneCond is_done;

                ENL1_printf("qualification succeeded, projecting tuple");

                TupleTableSlot* result = ExecProject(node->js.ps.ps_ProjInfo, &is_done);

                if (is_done != ExprEndResult) {
                    node->js.ps.ps_vec_TupFromTlist = (is_done == ExprMultipleResult);
                    /*
                     * @hdfs
                     * Optimize plan by informational constraint.
                     */
                    if (((NestLoop*)(node->js.ps.plan))->join.optimizable) {
                        node->nl_NeedNewOuter = true;
                    }

                    return result;
                }
            } else
                InstrCountFiltered2(node, 1);
        } else
            InstrCountFiltered1(node, 1);

        /*
         * Tuple fails qual, so free per-tuple memory and try again.
         */
        ResetExprContext(econtext);

        ENL1_printf("qualification failed, looping");
    }
}

#ifdef USE_SPQ
/* ----------------------------------------------------------------
 * ExtractFuncExprArgs
 *
 * Extract the arguments of a FuncExpr or an OpExpr and append them into two
 * given lists:
 *   - lclauses for the left side of the expression,
 *   - rclauses for the right side
 *
 * This function is only used for LASJ. Once we find a NULL from inner side, we
 * can skip the join and just return an empty set as result. This is only true
 * if the equality operator is strict, that is, if a tuple from inner side is
 * NULL then the equality operator returns NULL.
 *
 * If the number of arguments is not two, we just return leaving lclauses and
 * rclauses remaining NULL. In this case, the LASJ join would be actually
 * performed.
 * ----------------------------------------------------------------
 */
static void ExtractFuncExprArgs(Expr *clause, List **lclauses, List **rclauses)
{
    if (IsA(clause, OpExpr)) {
        OpExpr *opexpr = (OpExpr *)clause;
 
        if (list_length(opexpr->args) != 2)
            return;
 
        if (!op_strict(opexpr->opno))
            return;
 
        *lclauses = lappend(*lclauses, linitial(opexpr->args));
        *rclauses = lappend(*rclauses, lsecond(opexpr->args));
    } else if (IsA(clause, FuncExpr)) {
        FuncExpr *fexpr = (FuncExpr *)clause;
 
        if (list_length(fexpr->args) != 2)
            return;
 
        if (!func_strict(fexpr->funcid))
            return;
 
        *lclauses = lappend(*lclauses, linitial(fexpr->args));
        *rclauses = lappend(*rclauses, lsecond(fexpr->args));
    } else
        elog(ERROR, "unexpected join qual in JOIN_LASJ_NOTIN join");
}
 
/* ----------------------------------------------------------------
 * SplitJoinQualExpr
 *
 * Deconstruct the join clauses into outer and inner argument values, so
 * that we can evaluate those subexpressions separately. Note: for constant
 * expression we don't need to split (MPP-21294). However, if constant expressions
 * have peer splittable expressions we *do* split those.
 *
 * This is used for NOTIN joins, as we need to look for NULLs on both
 * inner and outer side.
 *
 * XXX: This would be more appropriate in the planner.
 * ----------------------------------------------------------------
 */
static void SplitJoinQualExpr(List *joinqual, List **inner_join_keys_p, List **outer_join_keys_p)
{
    List *lclauses = NIL;
    List *rclauses = NIL;
    ListCell *lc;
 
    foreach(lc, joinqual) {
        Expr *expr = (Expr *)lfirst(lc);
 
        switch (expr->type) {
            case T_FuncExpr:
            case T_OpExpr:
                ExtractFuncExprArgs(expr, &lclauses, &rclauses);
                break;
 
            case T_BoolExpr:
            {
                BoolExpr *bexpr = (BoolExpr *)expr;
                ListCell *argslc;
 
                foreach(argslc, bexpr->args) {
                    ExtractFuncExprArgs((Expr *)lfirst(argslc), &lclauses, &rclauses);
                }
            }
            break;
 
            case T_Const:
            /*
             * Constant expressions do not need to be splitted into left and
             * right as they don't need to be considered for NULL value special
             * cases
             */
                break;
 
            default:
                elog(ERROR, "unexpected expression type in NestLoopJoin qual");
        }
    }
 
    *inner_join_keys_p = rclauses;
    *outer_join_keys_p = lclauses;
}
#endif
/* ----------------------------------------------------------------
 *		ExecInitNestLoop
 * ----------------------------------------------------------------
 */
NestLoopState* ExecInitNestLoop(NestLoop* node, EState* estate, int eflags)
{
    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    NL1_printf("ExecInitNestLoop: %s\n", "initializing node");

    /*
     * create state structure
     */
    NestLoopState* nlstate = makeNode(NestLoopState);
    nlstate->js.ps.plan = (Plan*)node;
    nlstate->js.ps.state = estate;
    nlstate->nl_MaterialAll = node->materialAll;
#ifdef USE_SPQ
    if (IS_SPQ_RUNNING) {
        nlstate->prefetch_inner = node->join.prefetch_inner;
    }
#endif
    nlstate->js.ps.ExecProcNode = ExecNestLoop;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &nlstate->js.ps);

    /*
     * initialize child expressions
     */
    if (estate->es_is_flt_frame) {
        nlstate->js.ps.qual = (List*)ExecInitQualByFlatten(node->join.plan.qual, (PlanState*)nlstate);
        nlstate->js.jointype = node->join.jointype;
        nlstate->js.joinqual = (List*)ExecInitQualByFlatten(node->join.joinqual, (PlanState*)nlstate);
        Assert(node->join.nulleqqual == NIL);
    } else {
        nlstate->js.ps.targetlist = (List*)ExecInitExprByRecursion((Expr*)node->join.plan.targetlist, (PlanState*)nlstate);
        nlstate->js.ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->join.plan.qual, (PlanState*)nlstate);
        nlstate->js.jointype = node->join.jointype;
#ifdef USE_SPQ
    if (node->join.jointype == JOIN_LASJ_NOTIN) {
        List *inner_join_keys;
        List *outer_join_keys;
        ListCell *lc;
 
        /* not initialized yet */
        Assert(nlstate->nl_InnerJoinKeys == nullptr);
        Assert(nlstate->nl_OuterJoinKeys == nullptr);
 
        SplitJoinQualExpr(node->join.joinqual,
                          &inner_join_keys,
                          &outer_join_keys);
        foreach(lc, inner_join_keys) {
            Expr *expr = (Expr *)lfirst(lc);
            ExprState *exprstate;
 
            exprstate = ExecInitExpr(expr, (PlanState *)nlstate);
 
            nlstate->nl_InnerJoinKeys = lappend(nlstate->nl_InnerJoinKeys, exprstate);
        }
        foreach(lc, outer_join_keys) {
            Expr *expr = (Expr *)lfirst(lc);
            ExprState *exprstate;
 
            exprstate = ExecInitExpr(expr, (PlanState *)nlstate);
 
            nlstate->nl_OuterJoinKeys = lappend(nlstate->nl_OuterJoinKeys, exprstate);
        }
        nlstate->js.joinqual = (List*)ExecInitExpr((Expr*)node->join.joinqual, (PlanState*)nlstate);
    } else {
        nlstate->js.joinqual = (List*)ExecInitExprByRecursion((Expr*)node->join.joinqual, (PlanState*)nlstate);
    }
#else
        nlstate->js.joinqual = (List*)ExecInitExprByRecursion((Expr*)node->join.joinqual, (PlanState*)nlstate);
#endif
    }
    Assert(node->join.nulleqqual == NIL);

    /*
     * initialize child nodes
     *
     * If we have no parameters to pass into the inner rel from the outer,
     * tell the inner child that cheap rescans would be good.  If we do have
     * such parameters, then there is no point in REWIND support at all in the
     * inner child, because it will always be rescanned with fresh parameter
     * values.
     */
    outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
    if (node->nestParams == NIL)
        eflags |= EXEC_FLAG_REWIND;
    else
        eflags &= ~EXEC_FLAG_REWIND;
    innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate, eflags);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &nlstate->js.ps);

    nlstate->js.single_match = (node->join.inner_unique || node->join.jointype == JOIN_SEMI);

    switch (node->join.jointype) {
        case JOIN_INNER:
        case JOIN_SEMI:
            break;
        case JOIN_LEFT:
        case JOIN_ANTI:
        case JOIN_LEFT_ANTI_FULL:
#ifdef USE_SPQ
        case JOIN_LASJ_NOTIN:
#endif
            nlstate->nl_NullInnerTupleSlot = ExecInitNullTupleSlot(estate, ExecGetResultType(innerPlanState(nlstate)));
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_EXECUTOR),
                    errmsg("unrecognized join type: %d when initializing nestLoop", (int)node->join.jointype)));
    }

    /*
     * initialize tuple type and projection info
     * the result in this case would hold only virtual data.
     */
    ExecAssignResultTypeFromTL(&nlstate->js.ps, TableAmHeap);
    ExecAssignProjectionInfo(&nlstate->js.ps, NULL);

    /*
     * finally, wipe the current outer tuple clean.
     */
    nlstate->js.ps.ps_vec_TupFromTlist = false;
    nlstate->nl_NeedNewOuter = true;
    nlstate->nl_MatchedOuter = false;

    NL1_printf("ExecInitNestLoop: %s\n", "node initialized");

    return nlstate;
}

/* ----------------------------------------------------------------
 *		ExecEndNestLoop
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
void ExecEndNestLoop(NestLoopState* node)
{
    NL1_printf("ExecEndNestLoop: %s\n", "ending node processing");
    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->js.ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->js.ps.ps_ResultTupleSlot);

    /*
     * close down subplans
     */
    ExecEndNode(outerPlanState(node));
    ExecEndNode(innerPlanState(node));

    NL1_printf("ExecEndNestLoop: %s\n", "node processing ended");
}

/* ----------------------------------------------------------------
 *		ExecReScanNestLoop
 * ----------------------------------------------------------------
 */
void ExecReScanNestLoop(NestLoopState* node)
{
    PlanState* outer_plan = outerPlanState(node);
    PlanState* inner_plan = innerPlanState(node);
    PlanState ps = node->js.ps;

    /*
     * If outer_plan->chgParam is not null then plan will be automatically
     * re-scanned by first ExecProcNode.
     */
    if (outer_plan->chgParam == NULL)
        ExecReScan(outer_plan);

    /*
     * Under recursive-stream condition, need to rescan
     * both outer_plan and inner_plan.
     */
    if (IS_PGXC_DATANODE && EXEC_IN_RECURSIVE_MODE(ps.plan) && ((ps.state)->es_recursive_next_iteration)) {
        ExecReScan(inner_plan);
        node->nl_MaterialAll = ((NestLoop*)ps.plan)->materialAll;
    }

    /*
     * inner_plan is re-scanned for each new outer tuple and MUST NOT be
     * re-scanned from here or you'll get troubles from inner index scans when
     * outer Vars are used as run-time keys...
     */
    node->js.ps.ps_vec_TupFromTlist = false;
    node->nl_NeedNewOuter = true;
    node->nl_MatchedOuter = false;
#ifdef USE_SPQ
    node->nl_innerSideScanned = false;
#endif
}
