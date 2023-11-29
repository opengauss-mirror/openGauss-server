/* -------------------------------------------------------------------------
 *
 * nodeMergejoin.cpp
 *	  routines supporting merge joins
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeMergejoin.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecMergeJoin			mergejoin outer and inner relations.
 *		ExecInitMergeJoin		creates and initializes run time states
 *		ExecEndMergeJoin		cleans up the node.
 *
 * NOTES
 *
 *		Merge-join is done by joining the inner and outer tuples satisfying
 *		join clauses of the form ((= outerKey innerKey) ...).
 *		The join clause list is provided by the query planner and may contain
 *		more than one (= outerKey innerKey) clause (for composite sort key).
 *
 *		However, the query executor needs to know whether an outer
 *		tuple is "greater/smaller" than an inner tuple so that it can
 *		"synchronize" the two relations. For example, consider the following
 *		relations:
 *
 *				outer: (0 ^1 1 2 5 5 5 6 6 7)	current tuple: 1
 *				inner: (1 ^3 5 5 5 5 6)			current tuple: 3
 *
 *		To continue the merge-join, the executor needs to scan both inner
 *		and outer relations till the matching tuples 5. It needs to know
 *		that currently inner tuple 3 is "greater" than outer tuple 1 and
 *		therefore it should scan the outer relation first to find a
 *		matching tuple and so on.
 *
 *		Therefore, rather than directly executing the merge join clauses,
 *		we evaluate the left and right key expressions separately and then
 *		compare the columns one at a time (see MJCompare).	The planner
 *		passes us enough information about the sort ordering of the inputs
 *		to allow us to determine how to make the comparison.  We may use the
 *		appropriate btree comparison function, since Postgres' only notion
 *		of ordering is specified by btree opfamilies.
 *
 *
 *		Consider the above relations and suppose that the executor has
 *		just joined the first outer "5" with the last inner "5". The
 *		next step is of course to join the second outer "5" with all
 *		the inner "5's". This requires repositioning the inner "cursor"
 *		to point at the first inner "5". This is done by "marking" the
 *		first inner 5 so we can restore the "cursor" to it before joining
 *		with the second outer 5. The access method interface provides
 *		routines to mark and restore to a tuple.
 *
 *
 *		Essential operation of the merge join algorithm is as follows:
 *
 *		Join {
 *			get initial outer and inner tuples				INITIALIZE
 *			do forever {
 *				while (outer != inner) {					SKIP_TEST
 *					if (outer < inner)
 *						advance outer						SKIPOUTER_ADVANCE
 *					else
 *						advance inner						SKIPINNER_ADVANCE
 *				}
 *				mark inner position							SKIP_TEST
 *				do forever {
 *					while (outer == inner) {
 *						join tuples							JOINTUPLES
 *						advance inner position				NEXTINNER
 *					}
 *					advance outer position					NEXTOUTER
 *					if (outer == mark)						TESTOUTER
 *						restore inner position to mark		TESTOUTER
 *					else
 *						break	// return to top of outer loop
 *				}
 *			}
 *		}
 *
 *		The merge join operation is coded in the fashion
 *		of a state machine.  At each state, we do something and then
 *		proceed to another state.  This state is stored in the node's
 *		execution state information and is preserved across calls to
 *		ExecMergeJoin. -cim 10/31/89
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
#include "executor/exec/execdebug.h"
#include "executor/exec/execStream.h"
#include "executor/node/nodeMergejoin.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "executor/node/nodeHashjoin.h"

/*
 * Runtime data for each mergejoin clause
 */
typedef struct MergeJoinClauseData {
    /* Executable expression trees */
    ExprState* lexpr; /* left-hand (outer) input expression */
    ExprState* rexpr; /* right-hand (inner) input expression */

    /*
     * If we have a current left or right input tuple, the values of the
     * expressions are loaded into these fields:
     */
    Datum ldatum; /* current left-hand value */
    Datum rdatum; /* current right-hand value */
    bool lisnull; /* and their isnull flags */
    bool risnull;

    /*
     * Everything we need to know to compare the left and right values is
     * stored here.
     */
    SortSupportData ssup;
} MergeJoinClauseData;

/* Result type for MJEvalOuterValues and MJEvalInnerValues */
typedef enum {
    MJEVAL_MATCHABLE,    /* normal, potentially matchable tuple */
    MJEVAL_NONMATCHABLE, /* tuple cannot join because it has a null */
    MJEVAL_ENDOFJOIN     /* end of input (physical or effective) */
} MJEvalResult;

#define MarkInnerTuple(inner_tuple_slot, merge_state) ExecCopySlot((merge_state)->mj_MarkedTupleSlot, (inner_tuple_slot))

static TupleTableSlot* ExecMergeJoin(PlanState* state);
/*
 * MJExamineQuals
 *
 * This deconstructs the list of mergejoinable expressions, which is given
 * to us by the planner in the form of a list of "leftexpr = rightexpr"
 * expression trees in the order matching the sort columns of the inputs.
 * We build an array of MergeJoinClause structs containing the information
 * we will need at runtime.  Each struct essentially tells us how to compare
 * the two expressions from the original clause.
 *
 * In addition to the expressions themselves, the planner passes the btree
 * opfamily OID, collation OID, btree strategy number (BTLessStrategyNumber or
 * BTGreaterStrategyNumber), and nulls-first flag that identify the intended
 * sort ordering for each merge key.  The mergejoinable operator is an
 * equality operator in the opfamily, and the two inputs are guaranteed to be
 * ordered in either increasing or decreasing (respectively) order according
 * to the opfamily and collation, with nulls at the indicated end of the range.
 * This allows us to obtain the needed comparison function from the opfamily.
 */
static MergeJoinClause MJExamineQuals(List* mergeclauses, Oid* mergefamilies, Oid* mergecollations,
    const int* mergestrategies, const bool* mergenullsfirst, PlanState* parent)
{
    int nClauses = list_length(mergeclauses);
    MergeJoinClause clauses = (MergeJoinClause)palloc0(nClauses * sizeof(MergeJoinClauseData));
    int iClause = 0;
    ListCell* cl = NULL;

    foreach (cl, mergeclauses) {
        OpExpr* qual = (OpExpr*)lfirst(cl);
        MergeJoinClause clause = &clauses[iClause];
        Oid opfamily = mergefamilies[iClause];
        Oid collation = mergecollations[iClause];
        StrategyNumber opstrategy = mergestrategies[iClause];
        bool nulls_first = mergenullsfirst[iClause];
        int op_strategy;
        Oid op_lefttype;
        Oid op_righttype;
        Oid sortfunc;

        if (!IsA(qual, OpExpr)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("mergejoin clause is not an OpExpr %d", nodeTag(qual))));
        }

        /*
         * Prepare the input expressions for execution.
         */
        clause->lexpr = ExecInitExpr((Expr*)linitial(qual->args), parent);
        clause->rexpr = ExecInitExpr((Expr*)lsecond(qual->args), parent);

        /* Set up sort support data */
        clause->ssup.ssup_cxt = CurrentMemoryContext;
        clause->ssup.ssup_collation = collation;
        if (opstrategy == BTLessStrategyNumber)
            clause->ssup.ssup_reverse = false;
        else if (opstrategy == BTGreaterStrategyNumber)
            clause->ssup.ssup_reverse = true;
        else /* planner screwed up */
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("unsupported mergejoin strategy %d", opstrategy)));
        clause->ssup.ssup_nulls_first = nulls_first;

        /* Extract the operator's declared left/right datatypes */
        get_op_opfamily_properties(qual->opno, opfamily, false, &op_strategy, &op_lefttype, &op_righttype);
        if (op_strategy != BTEqualStrategyNumber) /* should not happen */
            ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("cannot merge using non-equality operator %u, strategy is %d", qual->opno, op_strategy)));

        /*
         * sortsupport routine must know if abbreviation optimization is
         * applicable in principle.  It is never applicable for merge joins
         * because there is no convenient opportunity to convert to alternative
         * representation.
         */
        clause->ssup.abbreviate = false;

        /* And get the matching support or comparison function */
        sortfunc = get_opfamily_proc(opfamily, op_lefttype, op_righttype, BTSORTSUPPORT_PROC);
        if (OidIsValid(sortfunc)) {
            /* The sort support function should provide a comparator */
            OidFunctionCall1(sortfunc, PointerGetDatum(&clause->ssup));
            Assert(clause->ssup.comparator != NULL);
        } else {
            /* opfamily doesn't provide sort support, get comparison func */
            sortfunc = get_opfamily_proc(opfamily, op_lefttype, op_righttype, BTORDER_PROC);
            if (!OidIsValid(sortfunc)) /* should not happen */
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("missing support function %d(%u,%u) in opfamily %u",
                            BTORDER_PROC,
                            op_lefttype,
                            op_righttype,
                            opfamily)));
            /* We'll use a shim to call the old-style btree comparator */
            PrepareSortSupportComparisonShim(sortfunc, &clause->ssup);
        }

        iClause++;
    }

    return clauses;
}

/*
 * MJEvalOuterValues
 *
 * Compute the values of the mergejoined expressions for the current
 * outer tuple.  We also detect whether it's impossible for the current
 * outer tuple to match anything --- this is true if it yields a NULL
 * input, since we assume mergejoin operators are strict.  If the NULL
 * is in the first join column, and that column sorts nulls last, then
 * we can further conclude that no following tuple can match anything
 * either, since they must all have nulls in the first column.	However,
 * that case is only interesting if we're not in FillOuter mode, else
 * we have to visit all the tuples anyway.
 *
 * For the convenience of callers, we also make this routine responsible
 * for testing for end-of-input (null outer tuple), and returning
 * MJEVAL_ENDOFJOIN when that's seen.  This allows the same code to be used
 * for both real end-of-input and the effective end-of-input represented by
 * a first-column NULL.
 *
 * We evaluate the values in OuterEContext, which can be reset each
 * time we move to a new tuple.
 */
static MJEvalResult MJEvalOuterValues(MergeJoinState* merge_state)
{
    ExprContext* econtext = merge_state->mj_OuterEContext;
    MJEvalResult result = MJEVAL_MATCHABLE;
    int i;
    MemoryContext old_context;

    /* Check for end of outer subplan */
    if (TupIsNull(merge_state->mj_OuterTupleSlot))
        return MJEVAL_ENDOFJOIN;

    ResetExprContext(econtext);

    old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    econtext->ecxt_outertuple = merge_state->mj_OuterTupleSlot;

    for (i = 0; i < merge_state->mj_NumClauses; i++) {
        MergeJoinClause clause = &merge_state->mj_Clauses[i];

        clause->ldatum = ExecEvalExpr(clause->lexpr, econtext, &clause->lisnull, NULL);
        /* If we allow null = null, don't end join */
        if (clause->lisnull && merge_state->js.nulleqqual == NIL) {
            /* match is impossible; can we end the join early? */
            if (i == 0 && !clause->ssup.ssup_nulls_first && !merge_state->mj_FillOuter)
                result = MJEVAL_ENDOFJOIN;
            else if (result == MJEVAL_MATCHABLE)
                result = MJEVAL_NONMATCHABLE;
        }
    }

    MemoryContextSwitchTo(old_context);

    return result;
}

/*
 * MJEvalInnerValues
 *
 * Same as above, but for the inner tuple.	Here, we have to be prepared
 * to load data from either the true current inner, or the marked inner,
 * so caller must tell us which slot to load from.
 */
static MJEvalResult MJEvalInnerValues(MergeJoinState* merge_state, TupleTableSlot* inner_slot)
{
    ExprContext* econtext = merge_state->mj_InnerEContext;
    MJEvalResult result = MJEVAL_MATCHABLE;
    int i;
    MemoryContext old_context;

    /* Check for end of inner subplan */
    if (TupIsNull(inner_slot))
        return MJEVAL_ENDOFJOIN;

    ResetExprContext(econtext);

    old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    econtext->ecxt_innertuple = inner_slot;

    for (i = 0; i < merge_state->mj_NumClauses; i++) {
        MergeJoinClause clause = &merge_state->mj_Clauses[i];

        clause->rdatum = ExecEvalExpr(clause->rexpr, econtext, &clause->risnull, NULL);
        /* If we allow null = null, don't end join */
        if (clause->risnull && merge_state->js.nulleqqual == NIL) {
            /* match is impossible; can we end the join early? */
            if (i == 0 && !clause->ssup.ssup_nulls_first && !merge_state->mj_FillInner)
                result = MJEVAL_ENDOFJOIN;
            else if (result == MJEVAL_MATCHABLE)
                result = MJEVAL_NONMATCHABLE;
        }
    }

    MemoryContextSwitchTo(old_context);

    return result;
}

/*
 * MJCompare
 *
 * Compare the mergejoinable values of the current two input tuples
 * and return 0 if they are equal (ie, the mergejoin equalities all
 * succeed), >0 if outer > inner, <0 if outer < inner.
 *
 * MJEvalOuterValues and MJEvalInnerValues must already have been called
 * for the current outer and inner tuples, respectively.
 */
static int MJCompare(MergeJoinState* merge_state)
{
    int result = 0;
    bool null_eq_null = false;
    ExprContext* econtext = merge_state->js.ps.ps_ExprContext;
    int i;
    MemoryContext old_context;

    /*
     * Call the comparison functions in short-lived context, in case they leak
     * memory.
     */
    ResetExprContext(econtext);

    old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    for (i = 0; i < merge_state->mj_NumClauses; i++) {
        MergeJoinClause clause = &merge_state->mj_Clauses[i];

        /*
         * Special case for NULL-vs-NULL, else use standard comparison.
         */
        if (clause->lisnull && clause->risnull) {
            null_eq_null = true; /* NULL "=" NULL */
            continue;
        }

        result = ApplySortComparator(clause->ldatum, clause->lisnull, clause->rdatum, clause->risnull, &clause->ssup);
        if (result != 0)
            break;
    }

    /*
     * If we had any NULL-vs-NULL inputs, we do not want to report that the
     * tuples are equal.  Instead, if result is still 0, change it to +1. This
     * will result in advancing the inner side of the join.
     *
     * Likewise, if there was a constant-false join_qual, do not report
     * equality.  We have to check this as part of the mergequals, else the
     * rescan logic will do the wrong thing.
     */
    if (result == 0 && ((null_eq_null && merge_state->js.nulleqqual == NIL) || merge_state->mj_ConstFalseJoin))
        result = 1;

    MemoryContextSwitchTo(old_context);

    return result;
}

/*
 * Generate a fake join tuple with nulls for the inner tuple,
 * and return it if it passes the non-join quals.
 */
static TupleTableSlot* MJFillOuter(MergeJoinState* node)
{
    ExprContext* econtext = node->js.ps.ps_ExprContext;
    List* other_qual = node->js.ps.qual;

    ResetExprContext(econtext);

    econtext->ecxt_outertuple = node->mj_OuterTupleSlot;
    econtext->ecxt_innertuple = node->mj_NullInnerTupleSlot;

    if (ExecQual(other_qual, econtext, false)) {
        /*
         * qualification succeeded.  now form the desired projection tuple and
         * return the slot containing it.
         */
        ExprDoneCond isDone;

        MJ_printf("ExecMergeJoin: returning outer fill tuple\n");

        TupleTableSlot* result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

        if (isDone != ExprEndResult) {
            node->js.ps.ps_vec_TupFromTlist = (isDone == ExprMultipleResult);
            return result;
        }
    } else
        InstrCountFiltered2(node, 1);

    return NULL;
}

/*
 * Generate a fake join tuple with nulls for the outer tuple,
 * and return it if it passes the non-join quals.
 */
static TupleTableSlot* MJFillInner(MergeJoinState* node)
{
    ExprContext* econtext = node->js.ps.ps_ExprContext;
    List* other_qual = node->js.ps.qual;

    ResetExprContext(econtext);

    econtext->ecxt_outertuple = node->mj_NullOuterTupleSlot;
    econtext->ecxt_innertuple = node->mj_InnerTupleSlot;

    if (ExecQual(other_qual, econtext, false)) {
        /*
         * qualification succeeded.  now form the desired projection tuple and
         * return the slot containing it.
         */
        ExprDoneCond isDone;

        MJ_printf("ExecMergeJoin: returning inner fill tuple\n");

        TupleTableSlot* result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

        if (isDone != ExprEndResult) {
            node->js.ps.ps_vec_TupFromTlist = (isDone == ExprMultipleResult);
            return result;
        }
    } else
        InstrCountFiltered2(node, 1);

    return NULL;
}

/*
 * Check that a qual condition is constant true or constant false.
 * If it is constant false (or null), set *is_const_false to TRUE.
 *
 * Constant true would normally be represented by a NIL list, but we allow an
 * actual bool Const as well.  We do expect that the planner will have thrown
 * away any non-constant terms that have been ANDed with a constant false.
 */
bool check_constant_qual(List* qual, bool* is_const_false)
{
    ListCell* lc = NULL;

    foreach (lc, qual) {
        Const* con = (Const*)lfirst(lc);

        if (con == NULL || !IsA(con, Const))
            return false;
        if (con->constisnull || !DatumGetBool(con->constvalue))
            *is_const_false = true;
    }
    return true;
}

/* ----------------------------------------------------------------
 *		ExecMergeTupleDump
 *
 *		This function is called through the MJ_dump() macro
 *		when EXEC_MERGEJOINDEBUG is defined
 * ----------------------------------------------------------------
 */
#ifdef EXEC_MERGEJOINDEBUG

static void ExecMergeTupleDumpOuter(MergeJoinState* merge_state)
{
    TupleTableSlot* outerSlot = merge_state->mj_OuterTupleSlot;

    printf("==== outer tuple ====\n");
    if (TupIsNull(outerSlot))
        printf("(nil)\n");
    else
        MJ_debugtup(outerSlot);
}

static void ExecMergeTupleDumpInner(MergeJoinState* merge_state)
{
    TupleTableSlot* innerSlot = merge_state->mj_InnerTupleSlot;

    printf("==== inner tuple ====\n");
    if (TupIsNull(innerSlot))
        printf("(nil)\n");
    else
        MJ_debugtup(innerSlot);
}

static void ExecMergeTupleDumpMarked(MergeJoinState* merge_state)
{
    TupleTableSlot* marked_slot = merge_state->mj_MarkedTupleSlot;

    printf("==== marked tuple ====\n");
    if (TupIsNull(marked_slot))
        printf("(nil)\n");
    else
        MJ_debugtup(marked_slot);
}

static void ExecMergeTupleDump(MergeJoinState* merge_state)
{
    printf("******** ExecMergeTupleDump ********\n");

    ExecMergeTupleDumpOuter(merge_state);
    ExecMergeTupleDumpInner(merge_state);
    ExecMergeTupleDumpMarked(merge_state);

    printf("******** \n");
}
#endif

/* ----------------------------------------------------------------
 *		ExecMergeJoin
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ExecMergeJoin(PlanState* state)
{
    MergeJoinState* node = castNode(MergeJoinState, state);
    bool qual_result = false;
    int compare_result;
    TupleTableSlot* inner_tuple_slot = NULL;
    TupleTableSlot* outer_tuple_slot = NULL;
    ExprDoneCond isDone;

    CHECK_FOR_INTERRUPTS();

    /*
     * get information from node
     */
    PlanState* inner_plan = innerPlanState(node);
    PlanState* outer_plan = outerPlanState(node);
    ExprContext* econtext = node->js.ps.ps_ExprContext;
    List* join_qual = node->js.joinqual;
    List* other_qual = node->js.ps.qual;
    bool do_fill_outer = node->mj_FillOuter;
    bool do_fill_inner = node->mj_FillInner;

    /*
     * Check to see if we're still projecting out tuples from a previous join
     * tuple (because there is a function-returning-set in the projection
     * expressions).  If so, try to project another one.
     */
    if (node->js.ps.ps_vec_TupFromTlist) {
        TupleTableSlot* result = NULL;

        result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);
        if (isDone == ExprMultipleResult)
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
     * ok, everything is setup.. let's go to work
     */
    for (;;) {
        MJ_dump(node);

        /*
         * get the current state of the join and do things accordingly.
         */
        switch (node->mj_JoinState) {
                /*
                 * EXEC_MJ_INITIALIZE_OUTER means that this is the first time
                 * ExecMergeJoin() has been called and so we have to fetch the
                 * first matchable tuple for both outer and inner subplans. We
                 * do the outer side in INITIALIZE_OUTER state, then advance
                 * to INITIALIZE_INNER state for the inner subplan.
                 */
            case EXEC_MJ_INITIALIZE_OUTER:
                MJ_printf("ExecMergeJoin: EXEC_MJ_INITIALIZE_OUTER\n");

                outer_tuple_slot = ExecProcNode(outer_plan);
                node->mj_OuterTupleSlot = outer_tuple_slot;

                /* Compute join values and check for unmatchability */
                switch (MJEvalOuterValues(node)) {
                    case MJEVAL_MATCHABLE:
                        /* OK to go get the first inner tuple */
                        node->mj_JoinState = EXEC_MJ_INITIALIZE_INNER;
                        break;
                    case MJEVAL_NONMATCHABLE:
                        /* Stay in same state to fetch next outer tuple */
                        if (do_fill_outer) {
                            /*
                             * Generate a fake join tuple with nulls for the
                             * inner tuple, and return it if it passes the
                             * non-join quals.
                             */
                            TupleTableSlot* result = NULL;

                            result = MJFillOuter(node);
                            if (result != NULL)
                                return result;
                        }
                        break;
                    case MJEVAL_ENDOFJOIN:
                        /* No more outer tuples */
                        MJ_printf("ExecMergeJoin: nothing in outer subplan\n");
                        if (do_fill_inner) {
                            /*
                             * Need to emit right-join tuples for remaining
                             * inner tuples. We set MatchedInner = true to
                             * force the ENDOUTER state to advance inner.
                             */
                            node->mj_JoinState = EXEC_MJ_ENDOUTER;
                            node->mj_MatchedInner = true;
                            break;
                        }

                        /*
                         * If one side of MergeJoin returns 0 tuple and no need to
                         * generate a fake join tuple with nulls, we should deinit
                         * the consumer under MergeJoin earlier.
                         * It should be noticed that we can not do early deinit 
                         * within predpush.
                         */
                        if (((PlanState*)node) != NULL && !CheckParamWalker((PlanState*)node)) {
                            ExecEarlyDeinitConsumer((PlanState*)node);
                        }
                        /* Otherwise we're done. */
                        goto done;
                    default:
                        break;
                }
                break;

            case EXEC_MJ_INITIALIZE_INNER:
                MJ_printf("ExecMergeJoin: EXEC_MJ_INITIALIZE_INNER\n");

                inner_tuple_slot = ExecProcNode(inner_plan);
                node->mj_InnerTupleSlot = inner_tuple_slot;

                /* Compute join values and check for unmatchability */
                switch (MJEvalInnerValues(node, inner_tuple_slot)) {
                    case MJEVAL_MATCHABLE:

                        /*
                         * OK, we have the initial tuples.	Begin by skipping
                         * non-matching tuples.
                         */
                        node->mj_JoinState = EXEC_MJ_SKIP_TEST;
                        break;
                    case MJEVAL_NONMATCHABLE:
                        /* Mark before advancing, if wanted */
                        if (node->mj_ExtraMarks)
                            ExecMarkPos(inner_plan);
                        /* Stay in same state to fetch next inner tuple */
                        if (do_fill_inner) {
                            /*
                             * Generate a fake join tuple with nulls for the
                             * outer tuple, and return it if it passes the
                             * non-join quals.
                             */
                            TupleTableSlot* result = NULL;

                            result = MJFillInner(node);
                            if (result != NULL)
                                return result;
                        }
                        break;
                    case MJEVAL_ENDOFJOIN:
                        /* No more inner tuples */
                        MJ_printf("ExecMergeJoin: nothing in inner subplan\n");
                        if (do_fill_outer) {
                            /*
                             * Need to emit left-join tuples for all outer
                             * tuples, including the one we just fetched.  We
                             * set MatchedOuter = false to force the ENDINNER
                             * state to emit first tuple before advancing
                             * outer.
                             */
                            node->mj_JoinState = EXEC_MJ_ENDINNER;
                            node->mj_MatchedOuter = false;
                            break;
                        }

                        /*
                         * If one side of MergeJoin returns 0 tuple and no need to
                         * generate a fake join tuple with nulls, we should deinit
                         * the consumer under MergeJoin earlier.
                         * It should be noticed that we can not do early deinit 
                         * within predpush.
                         */
                        if (((PlanState*)node) != NULL && !CheckParamWalker((PlanState*)node)) {
                            ExecEarlyDeinitConsumer((PlanState*)node);
                        }

                        /* Otherwise we're done. */
                        goto done;
                    default:
                        break;
                }
                break;

                /*
                 * EXEC_MJ_JOINTUPLES means we have two tuples which satisfied
                 * the merge clause so we join them and then proceed to get
                 * the next inner tuple (EXEC_MJ_NEXTINNER).
                 */
            case EXEC_MJ_JOINTUPLES:
                MJ_printf("ExecMergeJoin: EXEC_MJ_JOINTUPLES\n");

                /*
                 * Set the next state machine state.  The right things will
                 * happen whether we return this join tuple or just fall
                 * through to continue the state machine execution.
                 */
                node->mj_JoinState = EXEC_MJ_NEXTINNER;

                /*
                 * Check the extra qual conditions to see if we actually want
                 * to return this join tuple.  If not, can proceed with merge.
                 * We must distinguish the additional joinquals (which must
                 * pass to consider the tuples "matched" for outer-join logic)
                 * from the otherquals (which must pass before we actually
                 * return the tuple).
                 *
                 * We don't bother with a ResetExprContext here, on the
                 * assumption that we just did one while checking the merge
                 * qual.  One per tuple should be sufficient.  We do have to
                 * set up the econtext links to the tuples for ExecQual to
                 * use.
                 */
                outer_tuple_slot = node->mj_OuterTupleSlot;
                econtext->ecxt_outertuple = outer_tuple_slot;
                inner_tuple_slot = node->mj_InnerTupleSlot;
                econtext->ecxt_innertuple = inner_tuple_slot;

                qual_result = (join_qual == NIL || ExecQual(join_qual, econtext));
                MJ_DEBUG_QUAL(join_qual, qual_result);

                if (qual_result) {
                    node->mj_MatchedOuter = true;
                    node->mj_MatchedInner = true;

                    /* In an antijoin, we never return a matched tuple.
                     * JOIN_RIGHT_ANTI_FULL can not create mergejoin plain,
                     * so we do not consider it here. */
                    if (node->js.jointype == JOIN_ANTI || node->js.jointype == JOIN_LEFT_ANTI_FULL) {
                        node->mj_JoinState = EXEC_MJ_NEXTOUTER;
                        break;
                    }

                    if (node->js.single_match) {
                        node->mj_JoinState = EXEC_MJ_NEXTOUTER;
                    }

                    qual_result = (other_qual == NIL || ExecQual(other_qual, econtext));
                    MJ_DEBUG_QUAL(other_qual, qual_result);

                    if (qual_result) {
                        /*
                         * qualification succeeded.  now form the desired
                         * projection tuple and return the slot containing it.
                         */
                        ExprDoneCond isDone;

                        MJ_printf("ExecMergeJoin: returning tuple\n");

                        TupleTableSlot* result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

                        if (isDone != ExprEndResult) {
                            node->js.ps.ps_vec_TupFromTlist = (isDone == ExprMultipleResult);

                            return result;
                        }
                    } else
                        InstrCountFiltered2(node, 1);
                } else
                    InstrCountFiltered1(node, 1);
                break;

                /*
                 * EXEC_MJ_NEXTINNER means advance the inner scan to the next
                 * tuple. If the tuple is not nil, we then proceed to test it
                 * against the join qualification.
                 *
                 * Before advancing, we check to see if we must emit an
                 * outer-join fill tuple for this inner tuple.
                 */
            case EXEC_MJ_NEXTINNER:
                MJ_printf("ExecMergeJoin: EXEC_MJ_NEXTINNER\n");

                if (do_fill_inner && !node->mj_MatchedInner) {
                    /*
                     * Generate a fake join tuple with nulls for the outer
                     * tuple, and return it if it passes the non-join quals.
                     */
                    node->mj_MatchedInner = true; /* do it only once */

                    TupleTableSlot* result = MJFillInner(node);
                    if (result != NULL)
                        return result;
                }

                /*
                 * now we get the next inner tuple, if any.  If there's none,
                 * advance to next outer tuple (which may be able to join to
                 * previously marked tuples).
                 *
                 * NB: must NOT do "extraMarks" here, since we may need to
                 * return to previously marked tuples.
                 */
                inner_tuple_slot = ExecProcNode(inner_plan);
                node->mj_InnerTupleSlot = inner_tuple_slot;
                MJ_DEBUG_PROC_NODE(inner_tuple_slot);
                node->mj_MatchedInner = false;

                /* Compute join values and check for unmatchability */
                switch (MJEvalInnerValues(node, inner_tuple_slot)) {
                    case MJEVAL_MATCHABLE:

                        /*
                         * Test the new inner tuple to see if it matches
                         * outer.
                         *
                         * If they do match, then we join them and move on to
                         * the next inner tuple (EXEC_MJ_JOINTUPLES).
                         *
                         * If they do not match then advance to next outer
                         * tuple.
                         */
                        compare_result = MJCompare(node);
                        MJ_DEBUG_COMPARE(compare_result);

                        if (compare_result == 0)
                            node->mj_JoinState = EXEC_MJ_JOINTUPLES;
                        else {
                            Assert(compare_result < 0);
                            node->mj_JoinState = EXEC_MJ_NEXTOUTER;
                        }
                        break;
                    case MJEVAL_NONMATCHABLE:

                        /*
                         * It contains a NULL and hence can't match any outer
                         * tuple, so we can skip the comparison and assume the
                         * new tuple is greater than current outer.
                         */
                        node->mj_JoinState = EXEC_MJ_NEXTOUTER;
                        break;
                    case MJEVAL_ENDOFJOIN:

                        /*
                         * No more inner tuples.  However, this might be only
                         * effective and not physical end of inner plan, so
                         * force mj_InnerTupleSlot to null to make sure we
                         * don't fetch more inner tuples.  (We need this hack
                         * because we are not transiting to a state where the
                         * inner plan is assumed to be exhausted.)
                         */
                        node->mj_InnerTupleSlot = NULL;
                        node->mj_JoinState = EXEC_MJ_NEXTOUTER;
                        break;
                    default:
                        break;
                }
                break;

                /* -------------------------------------------
                 * EXEC_MJ_NEXTOUTER means
                 *
                 *				outer inner
                 * outer tuple -  5		5  - marked tuple
                 *				  5		5
                 *				  6		6  - inner tuple
                 *				  7		7
                 *
                 * we know we just bumped into the
                 * first inner tuple > current outer tuple (or possibly
                 * the end of the inner stream)
                 * so get a new outer tuple and then
                 * proceed to test it against the marked tuple
                 * (EXEC_MJ_TESTOUTER)
                 *
                 * Before advancing, we check to see if we must emit an
                 * outer-join fill tuple for this outer tuple.
                 * ------------------------------------------------
                 */
            case EXEC_MJ_NEXTOUTER:
                MJ_printf("ExecMergeJoin: EXEC_MJ_NEXTOUTER\n");

                if (do_fill_outer && !node->mj_MatchedOuter) {
                    /*
                     * Generate a fake join tuple with nulls for the inner
                     * tuple, and return it if it passes the non-join quals.
                     */
                    node->mj_MatchedOuter = true; /* do it only once */

                    TupleTableSlot* result = MJFillOuter(node);
                    if (result != NULL)
                        return result;
                }

                /*
                 * now we get the next outer tuple, if any
                 */
                outer_tuple_slot = ExecProcNode(outer_plan);
                node->mj_OuterTupleSlot = outer_tuple_slot;
                MJ_DEBUG_PROC_NODE(outer_tuple_slot);
                node->mj_MatchedOuter = false;

                /* Compute join values and check for unmatchability */
                switch (MJEvalOuterValues(node)) {
                    case MJEVAL_MATCHABLE:
                        /* Go test the new tuple against the marked tuple */
                        node->mj_JoinState = EXEC_MJ_TESTOUTER;
                        break;
                    case MJEVAL_NONMATCHABLE:
                        /* Can't match, so fetch next outer tuple */
                        node->mj_JoinState = EXEC_MJ_NEXTOUTER;
                        break;
                    case MJEVAL_ENDOFJOIN:
                        /* No more outer tuples */
                        MJ_printf("ExecMergeJoin: end of outer subplan\n");
                        inner_tuple_slot = node->mj_InnerTupleSlot;
                        if (do_fill_inner && !TupIsNull(inner_tuple_slot)) {
                            /*
                             * Need to emit right-join tuples for remaining
                             * inner tuples.
                             */
                            node->mj_JoinState = EXEC_MJ_ENDOUTER;
                            break;
                        }
                        /* Otherwise we're done. */
                        goto done;
                    default:
                        break;
                }
                break;

                /* --------------------------------------------------------
                 * EXEC_MJ_TESTOUTER If the new outer tuple and the marked
                 * tuple satisfy the merge clause then we know we have
                 * duplicates in the outer scan so we have to restore the
                 * inner scan to the marked tuple and proceed to join the
                 * new outer tuple with the inner tuples.
                 *
                 * This is the case when
                 *						  outer inner
                 *							4	  5  - marked tuple
                 *			 outer tuple -	5	  5
                 *		 new outer tuple -	5	  5
                 *							6	  8  - inner tuple
                 *							7	 12
                 *
                 *				new outer tuple == marked tuple
                 *
                 * If the outer tuple fails the test, then we are done
                 * with the marked tuples, and we have to look for a
                 * match to the current inner tuple.  So we will
                 * proceed to skip outer tuples until outer >= inner
                 * (EXEC_MJ_SKIP_TEST).
                 *
                 *		This is the case when
                 *
                 *						  outer inner
                 *							5	  5  - marked tuple
                 *			 outer tuple -	5	  5
                 *		 new outer tuple -	6	  8  - inner tuple
                 *							7	 12
                 *
                 *				new outer tuple > marked tuple
                 *
                 * ---------------------------------------------------------
                 */
            case EXEC_MJ_TESTOUTER:
                MJ_printf("ExecMergeJoin: EXEC_MJ_TESTOUTER\n");

                /*
                 * Here we must compare the outer tuple with the marked inner
                 * tuple.  (We can ignore the result of MJEvalInnerValues,
                 * since the marked inner tuple is certainly matchable.)
                 */
                inner_tuple_slot = node->mj_MarkedTupleSlot;
                (void)MJEvalInnerValues(node, inner_tuple_slot);

                compare_result = MJCompare(node);
                MJ_DEBUG_COMPARE(compare_result);

                if (compare_result == 0) {
                    /*
                     * the merge clause matched so now we restore the inner
                     * scan position to the first mark, and go join that tuple
                     * (and any following ones) to the new outer.
                     *
                     * NOTE: we do not need to worry about the MatchedInner
                     * state for the rescanned inner tuples.  We know all of
                     * them will match this new outer tuple and therefore
                     * won't be emitted as fill tuples.  This works *only*
                     * because we require the extra joinquals to be constant
                     * when doing a right or full join --- otherwise some of
                     * the rescanned tuples might fail the extra joinquals.
                     * This obviously won't happen for a constant-true extra
                     * join_qual, while the constant-false case is handled by
                     * forcing the merge clause to never match, so we never
                     * get here.
                     */
                    if (!node->mj_SkipMarkRestore) {
                        ExecRestrPos(inner_plan);
                        node->mj_InnerTupleSlot = inner_tuple_slot;
                    }

                    node->mj_JoinState = EXEC_MJ_JOINTUPLES;
                } else {
                    /* ----------------
                     *	if the new outer tuple didn't match the marked inner
                     *	tuple then we have a case like:
                     *
                     *			 outer inner
                     *			   4	 4	- marked tuple
                     * new outer - 5	 4
                     *			   6	 5	- inner tuple
                     *			   7
                     *
                     *	which means that all subsequent outer tuples will be
                     *	larger than our marked inner tuples.  So we need not
                     *	revisit any of the marked tuples but can proceed to
                     *	look for a match to the current inner.	If there's
                     *	no more inners, no more matches are possible.
                     * ----------------
                     */
                    Assert(compare_result > 0);
                    inner_tuple_slot = node->mj_InnerTupleSlot;

                    /* reload comparison data for current inner */
                    switch (MJEvalInnerValues(node, inner_tuple_slot)) {
                        case MJEVAL_MATCHABLE:
                            /* proceed to compare it to the current outer */
                            node->mj_JoinState = EXEC_MJ_SKIP_TEST;
                            break;
                        case MJEVAL_NONMATCHABLE:

                            /*
                             * current inner can't possibly match any outer;
                             * better to advance the inner scan than the
                             * outer.
                             */
                            node->mj_JoinState = EXEC_MJ_SKIPINNER_ADVANCE;
                            break;
                        case MJEVAL_ENDOFJOIN:
                            /* No more inner tuples */
                            if (do_fill_outer) {
                                /*
                                 * Need to emit left-join tuples for remaining
                                 * outer tuples.
                                 */
                                node->mj_JoinState = EXEC_MJ_ENDINNER;
                                break;
                            }
                            /* Otherwise we're done. */
                            goto done;
                        default:
                            break;
                    }
                }
                break;

                /* ----------------------------------------------------------
                 * EXEC_MJ_SKIP means compare tuples and if they do not
                 * match, skip whichever is lesser.
                 *
                 * For example:
                 *
                 *				outer inner
                 *				  5		5
                 *				  5		5
                 * outer tuple -  6		8  - inner tuple
                 *				  7    12
                 *				  8    14
                 *
                 * we have to advance the outer scan
                 * until we find the outer 8.
                 *
                 * On the other hand:
                 *
                 *				outer inner
                 *				  5		5
                 *				  5		5
                 * outer tuple - 12		8  - inner tuple
                 *				 14    10
                 *				 17    12
                 *
                 * we have to advance the inner scan
                 * until we find the inner 12.
                 * ----------------------------------------------------------
                 */
            case EXEC_MJ_SKIP_TEST:
                MJ_printf("ExecMergeJoin: EXEC_MJ_SKIP_TEST\n");

                /*
                 * before we advance, make sure the current tuples do not
                 * satisfy the merge_clauses.  If they do, then we update the
                 * marked tuple position and go join them.
                 */
                compare_result = MJCompare(node);
                MJ_DEBUG_COMPARE(compare_result);

                if (compare_result == 0) {
                    if (!node->mj_SkipMarkRestore) {
                        ExecMarkPos(inner_plan);
                    }

                    if (node->mj_InnerTupleSlot == NULL) {
                        ereport(ERROR,
                                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                                 errmsg("mj_InnerTupleSlot cannot be NULL")));
                    }
                    MarkInnerTuple(node->mj_InnerTupleSlot, node);

                    node->mj_JoinState = EXEC_MJ_JOINTUPLES;
                } else if (compare_result < 0)
                    node->mj_JoinState = EXEC_MJ_SKIPOUTER_ADVANCE;
                else
                    /* compare_result > 0 */
                    node->mj_JoinState = EXEC_MJ_SKIPINNER_ADVANCE;
                break;

                /*
                 * SKIPOUTER_ADVANCE: advance over an outer tuple that is
                 * known not to join to any inner tuple.
                 *
                 * Before advancing, we check to see if we must emit an
                 * outer-join fill tuple for this outer tuple.
                 */
            case EXEC_MJ_SKIPOUTER_ADVANCE:
                MJ_printf("ExecMergeJoin: EXEC_MJ_SKIPOUTER_ADVANCE\n");

                if (do_fill_outer && !node->mj_MatchedOuter) {
                    /*
                     * Generate a fake join tuple with nulls for the inner
                     * tuple, and return it if it passes the non-join quals.
                     */
                    node->mj_MatchedOuter = true; /* do it only once */

                    TupleTableSlot* result = MJFillOuter(node);
                    if (result != NULL)
                        return result;
                }

                /*
                 * now we get the next outer tuple, if any
                 */
                outer_tuple_slot = ExecProcNode(outer_plan);
                node->mj_OuterTupleSlot = outer_tuple_slot;
                MJ_DEBUG_PROC_NODE(outer_tuple_slot);
                node->mj_MatchedOuter = false;

                /* Compute join values and check for unmatchability */
                switch (MJEvalOuterValues(node)) {
                    case MJEVAL_MATCHABLE:
                        /* Go test the new tuple against the current inner */
                        node->mj_JoinState = EXEC_MJ_SKIP_TEST;
                        break;
                    case MJEVAL_NONMATCHABLE:
                        /* Can't match, so fetch next outer tuple */
                        node->mj_JoinState = EXEC_MJ_SKIPOUTER_ADVANCE;
                        break;
                    case MJEVAL_ENDOFJOIN:
                        /* No more outer tuples */
                        MJ_printf("ExecMergeJoin: end of outer subplan\n");
                        inner_tuple_slot = node->mj_InnerTupleSlot;
                        if (do_fill_inner && !TupIsNull(inner_tuple_slot)) {
                            /*
                             * Need to emit right-join tuples for remaining
                             * inner tuples.
                             */
                            node->mj_JoinState = EXEC_MJ_ENDOUTER;
                            break;
                        }
                        /* Otherwise we're done. */
                        goto done;
                    default:
                        break;
                }
                break;

                /*
                 * SKIPINNER_ADVANCE: advance over an inner tuple that is
                 * known not to join to any outer tuple.
                 *
                 * Before advancing, we check to see if we must emit an
                 * outer-join fill tuple for this inner tuple.
                 */
            case EXEC_MJ_SKIPINNER_ADVANCE:
                MJ_printf("ExecMergeJoin: EXEC_MJ_SKIPINNER_ADVANCE\n");

                if (do_fill_inner && !node->mj_MatchedInner) {
                    /*
                     * Generate a fake join tuple with nulls for the outer
                     * tuple, and return it if it passes the non-join quals.
                     */
                    node->mj_MatchedInner = true; /* do it only once */

                    TupleTableSlot* result = MJFillInner(node);
                    if (result != NULL)
                        return result;
                }

                /* Mark before advancing, if wanted */
                if (node->mj_ExtraMarks)
                    ExecMarkPos(inner_plan);

                /*
                 * now we get the next inner tuple, if any
                 */
                inner_tuple_slot = ExecProcNode(inner_plan);
                node->mj_InnerTupleSlot = inner_tuple_slot;
                MJ_DEBUG_PROC_NODE(inner_tuple_slot);
                node->mj_MatchedInner = false;

                /* Compute join values and check for unmatchability */
                switch (MJEvalInnerValues(node, inner_tuple_slot)) {
                    case MJEVAL_MATCHABLE:
                        /* proceed to compare it to the current outer */
                        node->mj_JoinState = EXEC_MJ_SKIP_TEST;
                        break;
                    case MJEVAL_NONMATCHABLE:

                        /*
                         * current inner can't possibly match any outer;
                         * better to advance the inner scan than the outer.
                         */
                        node->mj_JoinState = EXEC_MJ_SKIPINNER_ADVANCE;
                        break;
                    case MJEVAL_ENDOFJOIN:
                        /* No more inner tuples */
                        MJ_printf("ExecMergeJoin: end of inner subplan\n");
                        outer_tuple_slot = node->mj_OuterTupleSlot;
                        if (do_fill_outer && !TupIsNull(outer_tuple_slot)) {
                            /*
                             * Need to emit left-join tuples for remaining
                             * outer tuples.
                             */
                            node->mj_JoinState = EXEC_MJ_ENDINNER;
                            break;
                        }
                        /* Otherwise we're done. */
                        goto done;
                    default:
                        break;
                }
                break;

                /*
                 * EXEC_MJ_ENDOUTER means we have run out of outer tuples, but
                 * are doing a right/full join and therefore must null-fill
                 * any remaining unmatched inner tuples.
                 */
            case EXEC_MJ_ENDOUTER:
                MJ_printf("ExecMergeJoin: EXEC_MJ_ENDOUTER\n");

                Assert(do_fill_inner);

                if (!node->mj_MatchedInner) {
                    /*
                     * Generate a fake join tuple with nulls for the outer
                     * tuple, and return it if it passes the non-join quals.
                     */
                    node->mj_MatchedInner = true; /* do it only once */

                    TupleTableSlot* result = MJFillInner(node);
                    if (result != NULL)
                        return result;
                }

                /* Mark before advancing, if wanted */
                if (node->mj_ExtraMarks)
                    ExecMarkPos(inner_plan);

                /*
                 * now we get the next inner tuple, if any
                 */
                inner_tuple_slot = ExecProcNode(inner_plan);
                node->mj_InnerTupleSlot = inner_tuple_slot;
                MJ_DEBUG_PROC_NODE(inner_tuple_slot);
                node->mj_MatchedInner = false;

                if (TupIsNull(inner_tuple_slot)) {
                    MJ_printf("ExecMergeJoin: end of inner subplan\n");
                    goto done;
                }

                /* Else remain in ENDOUTER state and process next tuple. */
                break;

                /*
                 * EXEC_MJ_ENDINNER means we have run out of inner tuples, but
                 * are doing a left/full join and therefore must null- fill
                 * any remaining unmatched outer tuples.
                 */
            case EXEC_MJ_ENDINNER:
                MJ_printf("ExecMergeJoin: EXEC_MJ_ENDINNER\n");

                Assert(do_fill_outer);

                if (!node->mj_MatchedOuter) {
                    /*
                     * Generate a fake join tuple with nulls for the inner
                     * tuple, and return it if it passes the non-join quals.
                     */
                    node->mj_MatchedOuter = true; /* do it only once */

                    TupleTableSlot* result = MJFillOuter(node);
                    if (result != NULL)
                        return result;
                }

                /*
                 * now we get the next outer tuple, if any
                 */
                outer_tuple_slot = ExecProcNode(outer_plan);
                node->mj_OuterTupleSlot = outer_tuple_slot;
                MJ_DEBUG_PROC_NODE(outer_tuple_slot);
                node->mj_MatchedOuter = false;

                if (TupIsNull(outer_tuple_slot)) {
                    MJ_printf("ExecMergeJoin: end of outer subplan\n");
                    goto done;
                }

                /* Else remain in ENDINNER state and process next tuple. */
                break;

                /*
                 * broken state value?
                 */
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized mergejoin state: %d", (int)node->mj_JoinState)));
        }
    }

done:
    ExecEarlyFree(innerPlanState(node));
    ExecEarlyFree(outerPlanState(node));
    EARLY_FREE_LOG(elog(LOG,
        "Early Free: MergeJoin is done "
        "at node %d, memory used %d MB.",
        (node->js.ps.plan)->plan_node_id,
        getSessionMemoryUsageMB()));

    return NULL;
}

/* ----------------------------------------------------------------
 *		ExecInitMergeJoin
 * ----------------------------------------------------------------
 */
MergeJoinState* ExecInitMergeJoin(MergeJoin* node, EState* estate, int eflags)
{
    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    MJ1_printf("ExecInitMergeJoin: %s\n", "initializing node");

    /*
     * create state structure
     */
    MergeJoinState* merge_state = makeNode(MergeJoinState);
    merge_state->js.ps.plan = (Plan*)node;
    merge_state->js.ps.state = estate;
    merge_state->js.ps.ExecProcNode = ExecMergeJoin;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &merge_state->js.ps);

    /*
     * we need two additional econtexts in which we can compute the join
     * expressions from the left and right input tuples.  The node's regular
     * econtext won't do because it gets reset too often.
     */
    merge_state->mj_OuterEContext = CreateExprContext(estate);
    merge_state->mj_InnerEContext = CreateExprContext(estate);

    /*
     * initialize child expressions
     */
    if (estate->es_is_flt_frame) {
        merge_state->js.ps.qual = (List*)ExecInitQualByFlatten(node->join.plan.qual, (PlanState*)merge_state);
        merge_state->js.jointype = node->join.jointype;
        merge_state->js.joinqual = (List*)ExecInitQualByFlatten(node->join.joinqual, (PlanState*)merge_state);
        merge_state->js.nulleqqual = (List*)ExecInitQualByFlatten(node->join.nulleqqual, (PlanState*)merge_state);
        merge_state->mj_ConstFalseJoin = false;
    } else {
        merge_state->js.ps.targetlist =
        (List*)ExecInitExprByRecursion((Expr*)node->join.plan.targetlist, (PlanState*)merge_state);
        merge_state->js.ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->join.plan.qual, (PlanState*)merge_state);
        merge_state->js.jointype = node->join.jointype;
        merge_state->js.joinqual = (List*)ExecInitExprByRecursion((Expr*)node->join.joinqual, (PlanState*)merge_state);
        merge_state->js.nulleqqual = (List*)ExecInitExprByRecursion((Expr*)node->join.nulleqqual, (PlanState*)merge_state);
    }
    merge_state->mj_ConstFalseJoin = false;

    Assert(node->join.joinqual == NIL || !node->skip_mark_restore);
    merge_state->mj_SkipMarkRestore = node->skip_mark_restore;

    outerPlanState(merge_state) = ExecInitNode(outerPlan(node), estate, eflags);
    innerPlanState(merge_state) =
        ExecInitNode(innerPlan(node), estate, merge_state->mj_SkipMarkRestore ? eflags : (eflags | EXEC_FLAG_MARK));

    if (IsA(innerPlan(node), Material) && (eflags & EXEC_FLAG_REWIND) == 0 && !merge_state->mj_SkipMarkRestore) {
        merge_state->mj_ExtraMarks = true;
    } else {
        merge_state->mj_ExtraMarks = false;
    }

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &merge_state->js.ps);

    merge_state->mj_MarkedTupleSlot = ExecInitExtraTupleSlot(estate);
    ExecSetSlotDescriptor(merge_state->mj_MarkedTupleSlot, ExecGetResultType(innerPlanState(merge_state)));

    merge_state->js.single_match = (node->join.inner_unique || node->join.jointype == JOIN_SEMI);

    switch (node->join.jointype) {
        case JOIN_INNER:
        case JOIN_SEMI:
            merge_state->mj_FillOuter = false;
            merge_state->mj_FillInner = false;
            break;
        case JOIN_LEFT:
        case JOIN_ANTI:
        case JOIN_LEFT_ANTI_FULL:
            merge_state->mj_FillOuter = true;
            merge_state->mj_FillInner = false;
            merge_state->mj_NullInnerTupleSlot =
                ExecInitNullTupleSlot(estate, ExecGetResultType(innerPlanState(merge_state)));
            break;
        case JOIN_RIGHT:
            /* JOIN_RIGHT_ANTI_FULL can not create mergejoin plan, igonre it. */
            merge_state->mj_FillOuter = false;
            merge_state->mj_FillInner = true;
            merge_state->mj_NullOuterTupleSlot =
                ExecInitNullTupleSlot(estate, ExecGetResultType(outerPlanState(merge_state)));

            /*
             * Can't handle right or full join with non-constant extra
             * joinclauses.  This should have been caught by planner.
             */
            if (!check_constant_qual(node->join.joinqual, &merge_state->mj_ConstFalseJoin))
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("RIGHT JOIN is only supported with merge-joinable join conditions."),
                        errhint("Try other join methods like nestloop or hashjoin.")));
            break;
        case JOIN_FULL:
            merge_state->mj_FillOuter = true;
            merge_state->mj_FillInner = true;
            merge_state->mj_NullOuterTupleSlot =
                ExecInitNullTupleSlot(estate, ExecGetResultType(outerPlanState(merge_state)));
            merge_state->mj_NullInnerTupleSlot =
                ExecInitNullTupleSlot(estate, ExecGetResultType(innerPlanState(merge_state)));

            /*
             * Can't handle right or full join with non-constant extra
             * joinclauses.  This should have been caught by planner.
             */
            if (!check_constant_qual(node->join.joinqual, &merge_state->mj_ConstFalseJoin))
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("FULL JOIN is only supported with merge-joinable join conditions."),
                        errhint("Try other join methods like nestloop or hashjoin.")));
            break;
#ifdef USE_SPQ
        case JOIN_LASJ_NOTIN:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("LASJ NOTIN JOIN is not supported with merge-joinable join conditions.")));
            break;
#endif
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized join type: %d for mergejoin.", (int)node->join.jointype)));
    }

    /*
     * initialize tuple type and projection info
     * result table tuple slot for merge join contains virtual tuple, so the
     * default tableAm type is set to HEAP.
     */
    ExecAssignResultTypeFromTL(&merge_state->js.ps, TableAmHeap);
    ExecAssignProjectionInfo(&merge_state->js.ps, NULL);
    /*
     * preprocess the merge clauses
     */
    merge_state->mj_NumClauses = list_length(node->mergeclauses);
    merge_state->mj_Clauses = MJExamineQuals(node->mergeclauses,
        node->mergeFamilies,
        node->mergeCollations,
        node->mergeStrategies,
        node->mergeNullsFirst,
        (PlanState*)merge_state);

    /*
     * initialize join state
     */
    merge_state->mj_JoinState = EXEC_MJ_INITIALIZE_OUTER;
    merge_state->js.ps.ps_vec_TupFromTlist = false;
    merge_state->mj_MatchedOuter = false;
    merge_state->mj_MatchedInner = false;
    merge_state->mj_OuterTupleSlot = NULL;
    merge_state->mj_InnerTupleSlot = NULL;

    /*
     * initialization successful
     */
    MJ1_printf("ExecInitMergeJoin: %s\n", "node initialized");

    return merge_state;
}

/* ----------------------------------------------------------------
 *		ExecEndMergeJoin
 *
 * old comments
 *		frees storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void ExecEndMergeJoin(MergeJoinState* node)
{
    MJ1_printf("ExecEndMergeJoin: %s\n", "ending node processing");
    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->js.ps);

    /*
     * clean out the tuple table
     */

    (void)ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->mj_MarkedTupleSlot);

    /*
     * shut down the subplans
     */
    ExecEndNode(innerPlanState(node));
    ExecEndNode(outerPlanState(node));

    MJ1_printf("ExecEndMergeJoin: %s\n", "node processing ended");
}

void ExecReScanMergeJoin(MergeJoinState* node)
{
    (void)ExecClearTuple(node->mj_MarkedTupleSlot);

    node->mj_JoinState = EXEC_MJ_INITIALIZE_OUTER;
    node->js.ps.ps_vec_TupFromTlist = false;
    node->mj_MatchedOuter = false;
    node->mj_MatchedInner = false;
    node->mj_OuterTupleSlot = NULL;
    node->mj_InnerTupleSlot = NULL;

    /*
     * if chgParam of subnodes is not null then plans will be re-scanned by
     * first ExecProcNode.
     */
    if (node->js.ps.lefttree->chgParam == NULL)
        ExecReScan(node->js.ps.lefttree);
    if (node->js.ps.righttree->chgParam == NULL)
        ExecReScan(node->js.ps.righttree);
}
