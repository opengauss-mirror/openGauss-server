/* -------------------------------------------------------------------------
 *
 * nodeAgg.cpp
 *	  Routines to handle aggregate nodes.
 *
 *	  ExecAgg evaluates each aggregate in the following steps:
 *
 *		 transvalue = initcond
 *		 foreach input_tuple do
 *			transvalue = transfunc(transvalue, input_value(s))
 *		 result = finalfunc(transvalue, direct_argument(s))
 *
 *	  If a finalfunc is not supplied then the result is just the ending
 *	  value of transvalue.
 *
 *	  If a normal aggregate call specifies DISTINCT or ORDER BY, we sort the
 *	  input tuples and eliminate duplicates (if required) before performing
 *	  the above-depicted process. While for ordered-set aggregate, their "Order
 *    by" inputs are their aggregate arguments, so we can do the sort job at
 *    the end.
 *
 *    For normal aggregates:
 *	  If transfunc is marked "strict" in pg_proc and initcond is NULL,
 *	  then the first non-NULL input_value is assigned directly to transvalue,
 *	  and transfunc isn't applied until the second non-NULL input_value.
 *	  The agg's first input type and transtype must be the same in this case!
 *
 *	  If transfunc is marked "strict" then NULL input_values are skipped,
 *	  keeping the previous transvalue.	If transfunc is not strict then it
 *	  is called for every input tuple and must deal with NULL initcond
 *	  or NULL input_values for itself.
 *
 *	  If finalfunc is marked "strict" then it is not called when the
 *	  ending transvalue is NULL, instead a NULL result is created
 *	  automatically (this is just the usual handling of strict functions,
 *	  of course).  A non-strict finalfunc can make its own choice of
 *	  what to return for a NULL ending transvalue.
 *
 *	  For Ordered-set aggregates:
 *    We pass both "direct" arguments and transition value to the finalfunc.
 *	  NULL placeholders are also provided as the remaining finalfunc arguments,
 *	  which correspond to the aggregated expressions.  (These arguments are
 *	  useless at runtime, but may be needed to deal with a polymorphic
 *	  aggregate's result type.)
 *
 *	  We compute aggregate input expressions and run the transition functions
 *	  in a temporary econtext (aggstate->tmpcontext).  This is reset at
 *	  least once per input tuple, so when the transvalue datatype is
 *	  pass-by-reference, we have to be careful to copy it into a longer-lived
 *	  memory context, and free the prior value to avoid memory leakage.  We
 *	  store transvalues in another set of econtexts, aggstate->aggcontexts
 *	  (one per grouping set, see below), which are also used for the hashtable
 *	  structures in AGG_HASHED mode.  These econtexts are rescanned, not just
 *	  reset, at group boundaries so that aggregate transition functions can
 *	  register shutdown callbacks via AggRegisterCallback.
 *
 *	  The node's regular econtext (aggstate->ss.ps.ps_ExprContext) is used to
 *	  run finalize functions and compute the output tuple; this context can be
 *	  reset once per output tuple.

 *
 *	  The executor's AggState node is passed as the fmgr "context" value in
 *	  all transfunc and finalfunc calls.  It is not recommended that the
 *	  transition functions look at the AggState node directly, but they can
 *	  use AggCheckCallContext() to verify that they are being called by
 *	  nodeAgg.c (and not as ordinary SQL functions).  The main reason a
 *	  transition function might want to know this is so that it can avoid
 *	  palloc'ing a fixed-size pass-by-ref transition value on every call:
 *	  it can instead just scribble on and return its left input.  Ordinarily
 *	  it is completely forbidden for functions to modify pass-by-ref inputs,
 *	  but in the aggregate case we know the left input is either the initial
 *	  transition value or a previous function result, and in either case its
 *	  value need not be preserved.	See int8inc() for an example.  Notice that
 *	  advance_transition_function() is coded to avoid a data copy step when
 *	  the previous transition value pointer is returned.  Also, some
 *	  transition functions want to store working state in addition to the
 *	  nominal transition value; they can use the memory context returned by
 *	  AggCheckCallContext() to do that.
 *
 *	  Note: AggCheckCallContext() is available as of PostgreSQL 9.0.  The
 *	  AggState is available as context in earlier releases (back to 8.1),
 *	  but direct examination of the node is needed to use it before 9.0.
 *
 *     Grouping sets:
 *
 *	  A list of grouping sets which is structurally equivalent to a ROLLUP
 *	  clause (e.g. (a,b,c), (a,b), (a)) can be processed in a single pass over
 *	  ordered data.  We do this by keeping a separate set of transition values
 *	  for each grouping set being concurrently processed; for each input tuple
 *	  we update them all, and on group boundaries we reset those states
 *	  (starting at the front of the list) whose grouping values have changed
 *	  (the list of grouping sets is ordered from most specific to least
 *	  specific).
 *
 *	  Where more complex grouping sets are used, we break them down into
 *	  "phases", where each phase has a different sort order.  During each
 *	  phase but the last, the input tuples are additionally stored in a
 *	  tuplesort which is keyed to the next phase's sort order; during each
 *	  phase but the first, the input tuples are drawn from the previously
 *	  sorted data.  (The sorting of the data for the first phase is handled by
 *	  the planner, as it might be satisfied by underlying nodes.)
 *
 *	  From the perspective of aggregate transition and final functions, the
 *	  only issue regarding grouping sets is this: a single call site (flinfo)
 *	  of an aggregate function may be used for updating several different
 *	  transition values in turn. So the function must not cache in the flinfo
 *	  anything which logically belongs as part of the transition value (most
 *	  importantly, the memory context in which the transition value exists).
 *	  The support API functions (AggCheckCallContext, AggRegisterCallback) are
 *	  sensitive to the grouping set for which the aggregate function is
 *	  currently being called.
 *
 *	  AGG_HASHED doesn't support multiple grouping sets yet.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeAgg.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/tableam.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/node/nodeAgg.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "pgxc/pgxc.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "utils/datum.h"
#include "utils/memprot.h"
#include "workload/workload.h"

static void initialize_aggregates(
    AggState* aggstate, AggStatePerAgg peragg, AggStatePerGroup pergroup, int numReset = 0);
static void advance_transition_function(
    AggState* aggstate, AggStatePerAgg peraggstate, AggStatePerGroup pergroupstate, FunctionCallInfoData* fcinfo);
static void advance_aggregates(AggState* aggstate, AggStatePerGroup pergroup);
static void process_ordered_aggregate_single(
    AggState* aggstate, AggStatePerAgg peraggstate, AggStatePerGroup pergroupstate);
static void process_ordered_aggregate_multi(
    AggState* aggstate, AggStatePerAgg peraggstate, AggStatePerGroup pergroupstate);
static void finalize_aggregate(AggState* aggstate, AggStatePerAgg peraggstate, AggStatePerGroup pergroupstate,
    Datum* resultVal, bool* resultIsNull);
static void prepare_projection_slot(AggState* aggstate, TupleTableSlot* slot, int currentSet);
static void finalize_aggregates(AggState* aggstate, AggStatePerAgg peragg, AggStatePerGroup pergroup, int currentSet);
static TupleTableSlot* project_aggregates(AggState* aggstate);

static Bitmapset* find_unaggregated_cols(AggState* aggstate);
static bool find_unaggregated_cols_walker(Node* node, Bitmapset** colnos);
static void build_hash_table(AggState* aggstate);
static AggHashEntry lookup_hash_entry(AggState* aggstate, TupleTableSlot* inputslot);
static TupleTableSlot* agg_retrieve_direct(AggState* aggstate);
static void agg_fill_hash_table(AggState* aggstate);
static TupleTableSlot* agg_retrieve_hash_table(AggState* aggstate);
static TupleTableSlot* agg_retrieve(AggState* node);
static bool prepare_data_source(AggState* node);
static TupleTableSlot* fetch_input_tuple(AggState* aggstate);

/*
 * Switch to phase "newphase", which must either be 0 (to reset) or
 * current_phase + 1. Juggle the tuplesorts accordingly.
 */
void initialize_phase(AggState* aggstate, int newphase)
{
    Assert(newphase == 0 || newphase == aggstate->current_phase + 1);

    /*
     * Whatever the previous state, we're now done with whatever input
     * tuplesort was in use.
     */
    if (aggstate->sort_in) {
        tuplesort_end(aggstate->sort_in);
        aggstate->sort_in = NULL;
    }

    if (newphase == 0) {
        /*
         * Discard any existing output tuplesort.
         */
        if (aggstate->sort_out) {
            tuplesort_end(aggstate->sort_out);
            aggstate->sort_out = NULL;
        }
    } else {
        /*
         * The old output tuplesort becomes the new input one, and this is the
         * right time to actually sort it.
         */
        aggstate->sort_in = aggstate->sort_out;
        aggstate->sort_out = NULL;
        Assert(aggstate->sort_in);
        tuplesort_performsort(aggstate->sort_in);
    }

    /*
     * If this isn't the last phase, we need to sort appropriately for the
     * next phase in sequence.
     */
    if (newphase < aggstate->numphases - 1) {
        Sort* sortnode = aggstate->phases[newphase + 1].sortnode;
        PlanState* outerNode = outerPlanState(aggstate);
        TupleDesc tupDesc = ExecGetResultType(outerNode);
        Plan* plan = aggstate->ss.ps.plan;
        int64 workMem = SET_NODEMEM(plan->operatorMemKB[0], plan->dop);
        int64 maxMem = (plan->operatorMaxMem > 0) ? SET_NODEMEM(plan->operatorMaxMem, plan->dop) : 0;

        aggstate->sort_out = tuplesort_begin_heap(tupDesc,
            sortnode->numCols,
            sortnode->sortColIdx,
            sortnode->sortOperators,
            sortnode->collations,
            sortnode->nullsFirst,
            workMem,
            false,
            maxMem,
            sortnode->plan.plan_node_id,
            SET_DOP(sortnode->plan.dop));
    }

    aggstate->current_phase = newphase;
    aggstate->phase = &aggstate->phases[newphase];
}

/*
 * Fetch a tuple from either the outer plan (for phase 0) or from the sorter
 * populated by the previous phase.  Copy it to the sorter for the next phase
 * if any.
 */
static TupleTableSlot* fetch_input_tuple(AggState* aggstate)
{
    TupleTableSlot* slot = NULL;

    if (aggstate->sort_in) {
        if (!tuplesort_gettupleslot(aggstate->sort_in, true, aggstate->sort_slot, NULL))
            return NULL;
        slot = aggstate->sort_slot;
    } else
        slot = ExecProcNode(outerPlanState(aggstate));

    if (!TupIsNull(slot) && aggstate->sort_out)
        tuplesort_puttupleslot(aggstate->sort_out, slot);

    return slot;
}

/*
 * (Re)Initialize an individual aggregate.
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void initialize_aggregate(AggState* aggstate, AggStatePerAgg peraggstate, AggStatePerGroup pergroupstate)
{
    Plan* plan = aggstate->ss.ps.plan;
    int64 local_work_mem = SET_NODEMEM(plan->operatorMemKB[0], plan->dop);
    int64 max_mem = (plan->operatorMaxMem > 0) ? SET_NODEMEM(plan->operatorMaxMem, plan->dop) : 0;

    /*
     * Start a fresh sort operation for each DISTINCT/ORDER BY aggregate.
     */
    if (peraggstate->numSortCols > 0) {
        /*
         * In case of rescan, maybe there could be an uncompleted sort
         * operation?  Clean it up if so.
         */
        if (peraggstate->sortstates[aggstate->current_set])
            tuplesort_end(peraggstate->sortstates[aggstate->current_set]);

        /*
         * We use a plain Datum sorter when there's a single input column;
         * otherwise sort the full tuple.  (See comments for
         * process_ordered_aggregate_single.)
         */
        if (peraggstate->numInputs == 1) {
            peraggstate->sortstates[aggstate->current_set] =
                tuplesort_begin_datum(peraggstate->evaldesc->attrs[0]->atttypid,
                    peraggstate->sortOperators[0],
                    peraggstate->sortCollations[0],
                    peraggstate->sortNullsFirst[0],
                    local_work_mem,
                    false);
        } else {
            peraggstate->sortstates[aggstate->current_set] = tuplesort_begin_heap(peraggstate->evaldesc,
                peraggstate->numSortCols,
                peraggstate->sortColIdx,
                peraggstate->sortOperators,
                peraggstate->sortCollations,
                peraggstate->sortNullsFirst,
                local_work_mem,
                false,
                max_mem,
                plan->plan_node_id,
                SET_DOP(plan->dop));
        }
    }

    /*
     * (Re)set transValue to the initial value.
     *
     * Note that when the initial value is pass-by-ref, we must copy it
     * (into the aggcontext) since we will pfree the transValue later.
     */
    if (peraggstate->initValueIsNull)
        pergroupstate->transValue = peraggstate->initValue;
    else {
        MemoryContext oldContext;

        oldContext = MemoryContextSwitchTo(aggstate->aggcontexts[aggstate->current_set]);
        pergroupstate->transValue =
            datumCopy(peraggstate->initValue, peraggstate->transtypeByVal, peraggstate->transtypeLen);
        MemoryContextSwitchTo(oldContext);
    }
    pergroupstate->transValueIsNull = peraggstate->initValueIsNull;

    /*
     * If the initial value for the transition state doesn't exist in the
     * pg_aggregate table then we will let the first non-NULL value
     * returned from the outer procNode become the initial value. (This is
     * useful for aggregates like max() and min().) The noTransValue flag
     * signals that we still need to do this.
     */
    pergroupstate->noTransValue = peraggstate->initValueIsNull;

#ifdef PGXC
    /*
     * (Re)set collectValue to the initial value.
     *
     * Note that when the initial value is pass-by-ref, we must copy it
     * (into the aggcontext) since we will pfree the collectValue later.
     * collection type is same as transition type.
     */
    if (peraggstate->initCollectValueIsNull)
        pergroupstate->collectValue = peraggstate->initCollectValue;
    else {
        MemoryContext oldContext;

        oldContext = MemoryContextSwitchTo(aggstate->aggcontexts[aggstate->current_set]);
        pergroupstate->collectValue =
            datumCopy(peraggstate->initCollectValue, peraggstate->transtypeByVal, peraggstate->transtypeLen);
        MemoryContextSwitchTo(oldContext);
    }
    pergroupstate->collectValueIsNull = peraggstate->initCollectValueIsNull;

    /*
     * If the initial value for the transition state doesn't exist in the
     * pg_aggregate table then we will let the first non-NULL value
     * returned from the outer procNode become the initial value. (This is
     * useful for aggregates like max() and min().) The noTransValue flag
     * signals that we still need to do this.
     */
    pergroupstate->noCollectValue = peraggstate->initCollectValueIsNull;
#endif /* PGXC */
}

/*
 * Initialize all aggregates for a new group of input values.
 *
 * If there are multiple grouping sets, we initialize only the first numReset
 * of them (the grouping sets are ordered so that the most specific one, which
 * is reset most often, is first). As a convenience, if numReset is < 1, we
 * reinitialize all sets.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void initialize_aggregates(AggState* aggstate, AggStatePerAgg peragg, AggStatePerGroup pergroup, int numReset)
{
    int aggno;
    int numGroupingSets = Max(aggstate->phase->numsets, 1);
    int setno = 0;

    if (numReset < 1) {
        numReset = numGroupingSets;
    }

    for (aggno = 0; aggno < aggstate->numaggs; aggno++) {
        AggStatePerAgg peraggstate = &peragg[aggno];

        for (setno = 0; setno < numReset; setno++) {
            AggStatePerGroup pergroupstate;

            pergroupstate = &pergroup[aggno + (setno * (aggstate->numaggs))];

            aggstate->current_set = setno;

            initialize_aggregate(aggstate, peraggstate, pergroupstate);
        }
    }
}

/*
 * Given new input value(s), advance the transition function of an aggregate.
 *
 * The new values (and null flags) have been preloaded into argument positions
 * 1 and up in fcinfo, so that we needn't copy them again to pass to the
 * transition function.  No other fields of fcinfo are assumed valid.
 *
 * It doesn't matter which memory context this is called in.
 */
static void advance_transition_function(
    AggState* aggstate, AggStatePerAgg peraggstate, AggStatePerGroup pergroupstate, FunctionCallInfoData* fcinfo)
{
    int numTransInputs = peraggstate->numTransInputs;
    MemoryContext oldContext;
    Datum newVal;
    int i;

    if (peraggstate->transfn.fn_strict) {
        /*
         * For a strict transfn, nothing happens when there's a NULL input; we
         * just keep the prior transValue.
         */
        for (i = 1; i <= numTransInputs; i++) {
            if (fcinfo->argnull[i])
                return;
        }
        if (pergroupstate->noTransValue) {
            /*
             * transValue has not been initialized. This is the first non-NULL
             * input value. We use it as the initial value for transValue. (We
             * already checked that the agg's input type is binary-compatible
             * with its transtype, so straight copy here is OK.)
             *
             * We must copy the datum into aggcontext if it is pass-by-ref. We
             * do not need to pfree the old transValue, since it's NULL.
             */
            oldContext = MemoryContextSwitchTo(aggstate->aggcontexts[aggstate->current_set]);
            pergroupstate->transValue =
                datumCopy(fcinfo->arg[1], peraggstate->transtypeByVal, peraggstate->transtypeLen);
            pergroupstate->transValueIsNull = false;
            pergroupstate->noTransValue = false;
            MemoryContextSwitchTo(oldContext);
            return;
        }
        if (pergroupstate->transValueIsNull) {
            /*
             * Don't call a strict function with NULL inputs.  Note it is
             * possible to get here despite the above tests, if the transfn is
             * strict *and* returned a NULL on a prior cycle. If that happens
             * we will propagate the NULL all the way to the end.
             */
            return;
        }
    }

    /* We run the transition functions in per-input-tuple memory context */
    oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

    /* set up aggstate->curperagg to allow get aggref */
    aggstate->curperagg = peraggstate;

    /*
     * OK to call the transition function
     */
    InitFunctionCallInfoData(
        *fcinfo, &(peraggstate->transfn), numTransInputs + 1, peraggstate->aggCollation, (Node*)aggstate, NULL);
    fcinfo->arg[0] = pergroupstate->transValue;
    fcinfo->argnull[0] = pergroupstate->transValueIsNull;
    fcinfo->argTypes[0] = InvalidOid;
    fcinfo->isnull = false; /* just in case transfn doesn't set it */

    Node *origin_fcxt = fcinfo->context;
    if (IS_PGXC_DATANODE && peraggstate->is_avg) {
        Node *fcontext = (Node *)palloc0(sizeof(Node));
        fcontext->type = (NodeTag)(peraggstate->is_avg);
        fcinfo->context = fcontext;
    }

    newVal = FunctionCallInvoke(fcinfo);
    aggstate->curperagg = NULL;
    fcinfo->context = origin_fcxt;

    /*
     * If pass-by-ref datatype, must copy the new value into aggcontext and
     * pfree the prior transValue.	But if transfn returned a pointer to its
     * first input, we don't need to do anything.
     */
    if (!peraggstate->transtypeByVal && DatumGetPointer(newVal) != DatumGetPointer(pergroupstate->transValue)) {
        if (!fcinfo->isnull) {
            MemoryContextSwitchTo(aggstate->aggcontexts[aggstate->current_set]);
            newVal = datumCopy(newVal, peraggstate->transtypeByVal, peraggstate->transtypeLen);
        }
        if (!pergroupstate->transValueIsNull)
            pfree(DatumGetPointer(pergroupstate->transValue));
    }

    pergroupstate->transValue = newVal;
    pergroupstate->transValueIsNull = fcinfo->isnull;

    MemoryContextSwitchTo(oldContext);
}

#ifdef PGXC
/*
 * Given new input value(s), advance the collection function of an aggregate.
 *
 * The new values (and null flags) have been preloaded into argument positions
 * 1 and up in fcinfo, so that we needn't copy them again to pass to the
 * collection function.  No other fields of fcinfo are assumed valid.
 *
 * It doesn't matter which memory context this is called in.
 */
static void advance_collection_function(
    AggState* aggstate, AggStatePerAgg peraggstate, AggStatePerGroup pergroupstate, FunctionCallInfoData* fcinfo)
{
    int numArguments = peraggstate->numArguments;
    Datum newVal;
    MemoryContext oldContext;

    Assert(OidIsValid(peraggstate->collectfn.fn_oid));

    /*
     * numArgument has to be one, since each Datanode is going to send a single
     * transition value
     */
    Assert(numArguments == 1);
    if (peraggstate->collectfn.fn_strict) {
        int cntArgs;
        /*
         * For a strict collectfn, nothing happens when there's a NULL input; we
         * just keep the prior transition value, transValue.
         */
        for (cntArgs = 1; cntArgs <= numArguments; cntArgs++) {
            if (fcinfo->argnull[cntArgs])
                return;
        }
        if (pergroupstate->noCollectValue) {
            /*
             * collection result has not been initialized. This is the first non-NULL
             * transition value. We use it as the initial value for collectValue.
             * Aggregate's transition and collection type are same
             * We must copy the datum into result if it is pass-by-ref. We
             * do not need to pfree the old result, since it's NULL.
             */
            oldContext = MemoryContextSwitchTo(aggstate->aggcontexts[aggstate->current_set]);
            pergroupstate->collectValue =
                datumCopy(fcinfo->arg[1], peraggstate->transtypeByVal, peraggstate->transtypeLen);
            pergroupstate->collectValueIsNull = false;
            pergroupstate->noCollectValue = false;
            MemoryContextSwitchTo(oldContext);
            return;
        }
        if (pergroupstate->collectValueIsNull) {
            /*
             * Don't call a strict function with NULL inputs.  Note it is
             * possible to get here despite the above tests, if the collectfn is
             * strict *and* returned a NULL on a prior cycle. If that happens
             * we will propagate the NULL all the way to the end.
             */
            return;
        }
    }

    /* We run the collection functions in per-input-tuple memory context */
    oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

    /*
     * OK to call the collection function
     */
    InitFunctionCallInfoData(*fcinfo, &(peraggstate->collectfn), 2, peraggstate->aggCollation, (Node*)aggstate, NULL);
    fcinfo->arg[0] = pergroupstate->collectValue;
    fcinfo->argnull[0] = pergroupstate->collectValueIsNull;
    fcinfo->argTypes[0] = InvalidOid;
    newVal = FunctionCallInvoke(fcinfo);

    /*
     * If pass-by-ref datatype, must copy the new value into aggcontext and
     * pfree the prior transValue.	But if collectfn returned a pointer to its
     * first input, we don't need to do anything.
     */
    if (!peraggstate->transtypeByVal && DatumGetPointer(newVal) != DatumGetPointer(pergroupstate->collectValue)) {
        if (!fcinfo->isnull) {
            MemoryContextSwitchTo(aggstate->aggcontexts[aggstate->current_set]);
            newVal = datumCopy(newVal, peraggstate->transtypeByVal, peraggstate->transtypeLen);
        }
        if (!pergroupstate->collectValueIsNull)
            pfree(DatumGetPointer(pergroupstate->collectValue));
    }

    pergroupstate->collectValue = newVal;
    pergroupstate->collectValueIsNull = fcinfo->isnull;

    MemoryContextSwitchTo(oldContext);
}
#endif /* PGXC */

/*
 * Advance all the aggregates for one input tuple.	The input tuple
 * has been stored in tmpcontext->ecxt_outertuple, so that it is accessible
 * to ExecEvalExpr.  pergroup is the array of per-group structs to use
 * (this might be in a hashtable entry).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void advance_aggregates(AggState* aggstate, AggStatePerGroup pergroup)
{
    int aggno;
    int setno = 0;
    int numGroupingSets = Max(aggstate->phase->numsets, 1);
    int numAggs = aggstate->numaggs;

    for (aggno = 0; aggno < aggstate->numaggs; aggno++) {
        AggStatePerAgg peraggstate = &aggstate->peragg[aggno];
        int numTransInputs = peraggstate->numTransInputs;
        int i;
        TupleTableSlot* slot = NULL;

        /* Evaluate the current input expressions for this aggregate */
        slot = ExecProject(peraggstate->evalproj, NULL);

        if (peraggstate->numSortCols > 0) {
            /* DISTINCT and/or ORDER BY case */
            Assert(slot->tts_nvalid == peraggstate->numInputs);

            /*
             * If the transfn is strict, we want to check for nullity before
             * storing the row in the sorter, to save space if there are a lot
             * of nulls.  Note that we must only check numArguments columns,
             * not numInputs, since nullity in columns used only for sorting
             * is not relevant here.
             */
            if (peraggstate->transfn.fn_strict) {
                for (i = 0; i < numTransInputs; i++) {
                    if (slot->tts_isnull[i])
                        break;
                }
                if (i < numTransInputs)
                    continue;
            }

            for (setno = 0; setno < numGroupingSets; setno++) {

                /* OK, put the tuple into the tuplesort object */
                if (peraggstate->numInputs == 1)
                    tuplesort_putdatum(peraggstate->sortstates[setno], slot->tts_values[0], slot->tts_isnull[0]);
                else
                    tuplesort_puttupleslot(peraggstate->sortstates[setno], slot);
            }
        } else {
            /* We can apply the transition function immediately */
            FunctionCallInfoData fcinfo;

            /* init the number of arguments to a function. */
            InitFunctionCallInfoArgs(fcinfo, numTransInputs + 1, 1);

            /* Load values into fcinfo */
            /* Start from 1, since the 0th arg will be the transition value */
            Assert(slot->tts_nvalid >= numTransInputs);
            for (i = 0; i < numTransInputs; i++) {
                fcinfo.arg[i + 1] = slot->tts_values[i];
                fcinfo.argnull[i + 1] = slot->tts_isnull[i];
                fcinfo.argTypes[i + 1] = InvalidOid;
            }
            for (setno = 0; setno < numGroupingSets; setno++) {
                AggStatePerGroup pergroupstate = &pergroup[aggno + (setno * numAggs)];
                aggstate->current_set = setno;
#ifdef PGXC
                /*
                 * For the agg not in the first level, besides PGXC case, we should do
                 * collection.
                 */
                if ((peraggstate->aggref->aggstage > 0 || aggstate->is_final) &&
                    need_adjust_agg_inner_func_type(peraggstate->aggref)) {
                    /*
                     * we are collecting results sent by the Datanodes, so advance
                     * collections instead of transitions
                     */
                    advance_collection_function(aggstate, peraggstate, pergroupstate, &fcinfo);
                } else
#endif
                    advance_transition_function(aggstate, peraggstate, pergroupstate, &fcinfo);
            }
        }
    }
}

/*
 * Run the transition function for a DISTINCT or ORDER BY aggregate
 * with only one input.  This is called after we have completed
 * entering all the input values into the sort object.	We complete the
 * sort, read out the values in sorted order, and run the transition
 * function on each value (applying DISTINCT if appropriate).
 *
 * Note that the strictness of the transition function was checked when
 * entering the values into the sort, so we don't check it again here;
 * we just apply standard SQL DISTINCT logic.
 *
 * The one-input case is handled separately from the multi-input case
 * for performance reasons: for single by-value inputs, such as the
 * common case of count(distinct id), the tuplesort_getdatum code path
 * is around 300% faster.  (The speedup for by-reference types is less
 * but still noticeable.)
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void process_ordered_aggregate_single(
    AggState* aggstate, AggStatePerAgg peraggstate, AggStatePerGroup pergroupstate)
{
    Datum oldVal = (Datum)0;
    bool oldIsNull = true;
    bool haveOldVal = false;
    MemoryContext workcontext = aggstate->tmpcontext->ecxt_per_tuple_memory;
    MemoryContext oldContext;
    bool isDistinct = (peraggstate->numDistinctCols > 0);
    Datum* newVal = NULL;
    bool* isNull = NULL;
    FunctionCallInfoData fcinfo;

    Assert(peraggstate->numDistinctCols < 2);

    tuplesort_performsort(peraggstate->sortstates[aggstate->current_set]);

    /* init the number of arguments to a function. */
    InitFunctionCallInfoArgs(fcinfo, peraggstate->numArguments + 1, 1);

    /* Load the column into argument 1 (arg 0 will be transition value) */
    newVal = fcinfo.arg + 1;
    isNull = fcinfo.argnull + 1;

    /*
     * Note: if input type is pass-by-ref, the datums returned by the sort are
     * freshly palloc'd in the per-query context, so we must be careful to
     * pfree them when they are no longer needed.
     */
    while (tuplesort_getdatum(peraggstate->sortstates[aggstate->current_set], true, newVal, isNull)) {
        /*
         * Clear and select the working context for evaluation of the equality
         * function and transition function.
         */
        MemoryContextReset(workcontext);
        oldContext = MemoryContextSwitchTo(workcontext);

        /*
         * If DISTINCT mode, and not distinct from prior, skip it.
         *
         * Note: we assume equality functions don't care about collation.
         */
        if (isDistinct && haveOldVal &&
            ((oldIsNull && *isNull) ||
                (!oldIsNull && !*isNull && DatumGetBool(FunctionCall2(&peraggstate->equalfns[0], oldVal, *newVal))))) {
            /* equal to prior, so forget this one */
            if (!peraggstate->inputtypeByVal && !*isNull)
                pfree(DatumGetPointer(*newVal));
        } else {
            advance_transition_function(aggstate, peraggstate, pergroupstate, &fcinfo);
            /* forget the old value, if any */
            if (!oldIsNull && !peraggstate->inputtypeByVal)
                pfree(DatumGetPointer(oldVal));
            /* and remember the new one for subsequent equality checks */
            oldVal = *newVal;
            oldIsNull = *isNull;
            haveOldVal = true;
        }

        MemoryContextSwitchTo(oldContext);
    }

    if (!oldIsNull && !peraggstate->inputtypeByVal)
        pfree(DatumGetPointer(oldVal));

    tuplesort_end(peraggstate->sortstates[aggstate->current_set]);
    peraggstate->sortstates[aggstate->current_set] = NULL;
}

/*
 * Run the transition function for a DISTINCT or ORDER BY aggregate
 * with more than one input.  This is called after we have completed
 * entering all the input values into the sort object.	We complete the
 * sort, read out the values in sorted order, and run the transition
 * function on each value (applying DISTINCT if appropriate).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void process_ordered_aggregate_multi(
    AggState* aggstate, AggStatePerAgg peraggstate, AggStatePerGroup pergroupstate)
{
    MemoryContext workcontext = aggstate->tmpcontext->ecxt_per_tuple_memory;
    FunctionCallInfoData fcinfo;
    TupleTableSlot* slot1 = peraggstate->evalslot;
    TupleTableSlot* slot2 = peraggstate->uniqslot;
    int numTransInputs = peraggstate->numTransInputs;
    int numDistinctCols = peraggstate->numDistinctCols;
    Datum newAbbrevVal = (Datum)0;
    Datum oldAbbrevVal = (Datum)0;
    bool haveOldValue = false;
    int i;

    tuplesort_performsort(peraggstate->sortstates[aggstate->current_set]);

    (void)ExecClearTuple(slot1);
    if (slot2 != NULL)
        (void)ExecClearTuple(slot2);

    while (tuplesort_gettupleslot(peraggstate->sortstates[aggstate->current_set], true, slot1, &newAbbrevVal)) {
        /*
         * Extract the first numTransInputs as datums to pass to the transfn.
         * (This will help execTuplesMatch too, so do it immediately.)
         */
        tableam_tslot_getsomeattrs(slot1, numTransInputs);

        if (numDistinctCols == 0 || !haveOldValue || newAbbrevVal != oldAbbrevVal ||
            !execTuplesMatch(
                slot1, slot2, numDistinctCols, peraggstate->sortColIdx, peraggstate->equalfns, workcontext)) {
            /* init the number of arguments to a function. */
            InitFunctionCallInfoArgs(fcinfo, numTransInputs + 1, 1);

            /* Load values into fcinfo */
            /* Start from 1, since the 0th arg will be the transition value */
            for (i = 0; i < numTransInputs; i++) {
                fcinfo.arg[i + 1] = slot1->tts_values[i];
                fcinfo.argnull[i + 1] = slot1->tts_isnull[i];
            }

            advance_transition_function(aggstate, peraggstate, pergroupstate, &fcinfo);

            if (numDistinctCols > 0) {
                /* swap the slot pointers to retain the current tuple */
                TupleTableSlot* tmpslot = slot2;

                slot2 = slot1;
                slot1 = tmpslot;
                /* avoid execTuplesMatch() calls by reusing abbreviated keys */
                oldAbbrevVal = newAbbrevVal;
                haveOldValue = true;
            }
        }

        /* Reset context each time, unless execTuplesMatch did it for us */
        if (numDistinctCols == 0)
            MemoryContextReset(workcontext);

        (void)ExecClearTuple(slot1);
    }

    if (slot2 != NULL)
        (void)ExecClearTuple(slot2);

    tuplesort_end(peraggstate->sortstates[aggstate->current_set]);
    peraggstate->sortstates[aggstate->current_set] = NULL;
}

/*
 * Compute the final value of one aggregate.
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * The finalfunction will be run, and the result delivered, in the
 * output-tuple context; caller's CurrentMemoryContext does not matter.
 */
static void finalize_aggregate(AggState* aggstate, AggStatePerAgg peraggstate, AggStatePerGroup pergroupstate,
    Datum* resultVal, bool* resultIsNull)
{
    bool anynull = false;
    FunctionCallInfoData fcinfo;
    /* record the current passed argument position */
    int args_pos = 1;
    /* For a normal agg only the transition state value being passed to the finalfn */
    int numFinalArgs = 1;
    MemoryContext oldContext;
    ListCell* lc = NULL;

    oldContext = MemoryContextSwitchTo(aggstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
    /*
     * For ordered-set aggregates, direct argument(s) alone with nulls placeholder
     * (corresponding to the aggregate-input columns) are passed to finalfn.
     */
    if (AGGKIND_IS_ORDERED_SET(peraggstate->aggref->aggkind))
        numFinalArgs += peraggstate->numArguments;

    /* init the number of arguments to a function. */
    InitFunctionCallInfoArgs(fcinfo, numFinalArgs, 1);

    /*
     * Evaluate any direct arguments for finalfn and load them into function
     * call info.
     */
    foreach (lc, peraggstate->aggrefstate->aggdirectargs) {
        fcinfo.arg[args_pos] =
            ExecEvalExpr((ExprState*)lfirst(lc), aggstate->ss.ps.ps_ExprContext, &fcinfo.argnull[args_pos], NULL);
        fcinfo.argTypes[args_pos] = ((ExprState*)lfirst(lc))->resultType;
        if (anynull == true || fcinfo.argnull[args_pos] == true)
            anynull = true;
        else
            anynull = false;
        args_pos++;
    }

#ifdef PGXC
    /*
     * If we skipped the transition phase, we have the collection result in the
     * collectValue, move it to transValue for finalization to work on.
     */
    if ((peraggstate->aggref->aggstage > 0 || aggstate->is_final) &&
        need_adjust_agg_inner_func_type(peraggstate->aggref)) {
        pergroupstate->transValue = pergroupstate->collectValue;

        pergroupstate->transValueIsNull = pergroupstate->collectValueIsNull;
    }
#endif /* PGXC */

    Assert(args_pos <= numFinalArgs);
    /*
     * Apply the agg's finalfn if one is provided, else return transValue.
     */
    if (OidIsValid(peraggstate->finalfn_oid)) {
        /* set up aggstate->curperagg to allow get aggref */
        aggstate->curperagg = peraggstate;

        InitFunctionCallInfoData(
            fcinfo, &(peraggstate->finalfn), numFinalArgs, peraggstate->aggCollation, (Node*)aggstate, NULL);
        fcinfo.arg[0] = pergroupstate->transValue;
        fcinfo.argnull[0] = pergroupstate->transValueIsNull;
        fcinfo.argTypes[0] = InvalidOid;
        if (anynull == true || pergroupstate->transValueIsNull == true)
            anynull = true;
        else
            anynull = false;
        /* Fill remaining arguments positions with nulls */
        while (args_pos < numFinalArgs) {
            fcinfo.arg[args_pos] = (Datum)0;
            fcinfo.argnull[args_pos] = true;
            fcinfo.argTypes[args_pos] = InvalidOid;
            args_pos++;
            anynull = true;
        }

        if (fcinfo.flinfo->fn_strict && anynull) {
            /* don't call a strict function with NULL inputs */
            *resultVal = (Datum)0;
            *resultIsNull = true;
        } else {
            *resultVal = FunctionCallInvoke(&fcinfo);
            *resultIsNull = fcinfo.isnull;
        }
        aggstate->curperagg = NULL;
    } else {
        *resultVal = pergroupstate->transValue;
        *resultIsNull = pergroupstate->transValueIsNull;
    }

    /*
     * If result is pass-by-ref, make sure it is in the right context.
     */
    if (!peraggstate->resulttypeByVal && !*resultIsNull &&
        !MemoryContextContains(CurrentMemoryContext, DatumGetPointer(*resultVal)))
        *resultVal = datumCopy(*resultVal, peraggstate->resulttypeByVal, peraggstate->resulttypeLen);

    MemoryContextSwitchTo(oldContext);
}

/*
 * Prepare to finalize and project based on the specified representative tuple
 * slot and grouping set.
 *
 * In the specified tuple slot, force to null all attributes that should be
 * read as null in the context of the current grouping set.  Also stash the
 * current group bitmap where GroupingExpr can get at it.
 *
 * This relies on three conditions:
 *
 * 1) Nothing is ever going to try and extract the whole tuple from this slot,
 * only reference it in evaluations, which will only access individual
 * attributes.
 *
 * 2) No system columns are going to need to be nulled. (If a system column is
 * referenced in a group clause, it is actually projected in the outer plan
 * tlist.)
 *
 * 3) Within a given phase, we never need to recover the value of an attribute
 * once it has been set to null.
 *
 * Poking into the slot this way is a bit ugly, but the consensus is that the
 * alternative was worse.
 */
static void prepare_projection_slot(AggState* aggstate, TupleTableSlot* slot, int currentSet)
{
    if (aggstate->phase->grouped_cols) {
        Bitmapset* grouped_cols = aggstate->phase->grouped_cols[currentSet];

        aggstate->grouped_cols = grouped_cols;

        if (slot->tts_isempty) {
            /*
             * Force all values to be NULL if working on an empty input tuple
             * (i.e. an empty grouping set for which no input rows were
             * supplied).
             */
            ExecStoreAllNullTuple(slot);
        } else if (aggstate->all_grouped_cols) {
            ListCell* lc = NULL;

            Assert(slot->tts_tupleDescriptor != NULL);
            /* all_grouped_cols is arranged in desc order */
            tableam_tslot_getsomeattrs(slot, linitial_int(aggstate->all_grouped_cols));

            foreach (lc, aggstate->all_grouped_cols) {
                int attnum = lfirst_int(lc);

                if (!bms_is_member(attnum, grouped_cols))
                    slot->tts_isnull[attnum - 1] = true;
            }
        }
    }
}

/*
 * Compute the final value of all aggregates for one group.
 *
 * This function handles only one grouping set at a time.
 *
 * Results are stored in the output econtext aggvalues/aggnulls.
 */
static void finalize_aggregates(AggState* aggstate, AggStatePerAgg peragg, AggStatePerGroup pergroup, int currentSet)
{
    ExprContext* econtext = aggstate->ss.ps.ps_ExprContext;
    Datum* aggvalues = econtext->ecxt_aggvalues;
    bool* aggnulls = econtext->ecxt_aggnulls;
    int aggno;

    Assert(currentSet == 0 || ((Agg*)aggstate->ss.ps.plan)->aggstrategy != AGG_HASHED);

    aggstate->current_set = currentSet;

    for (aggno = 0; aggno < aggstate->numaggs; aggno++) {
        AggStatePerAgg peraggstate = &peragg[aggno];
        AggStatePerGroup pergroupstate;

        pergroupstate = &pergroup[aggno + (currentSet * (aggstate->numaggs))];

        if (peraggstate->numSortCols > 0) {
            Assert(((Agg*)aggstate->ss.ps.plan)->aggstrategy != AGG_HASHED);

            if (peraggstate->numInputs == 1)
                process_ordered_aggregate_single(aggstate, peraggstate, pergroupstate);
            else
                process_ordered_aggregate_multi(aggstate, peraggstate, pergroupstate);
        }

        finalize_aggregate(aggstate, peraggstate, pergroupstate, &aggvalues[aggno], &aggnulls[aggno]);
    }
}

/*
 * Project the result of a group (whose aggs have already been calculated by
 * finalize_aggregates). Returns the result slot, or NULL if no row is
 * projected (suppressed by qual or by an empty SRF).
 */
static TupleTableSlot* project_aggregates(AggState* aggstate)
{
    ExprContext* econtext = aggstate->ss.ps.ps_ExprContext;

    /*
     * Check the qual (HAVING clause); if the group does not match, ignore it.
     */
    if (ExecQual(aggstate->ss.ps.qual, econtext, false)) {
        /*
         * Form and return or store a projection tuple using the aggregate
         * results and the representative input tuple.
         */
        ExprDoneCond isDone;
        TupleTableSlot* result = NULL;

        result = ExecProject(aggstate->ss.ps.ps_ProjInfo, &isDone);

        if (isDone != ExprEndResult) {
            aggstate->ss.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);
            return result;
        }
    } else
        InstrCountFiltered1(aggstate, 1);

    return NULL;
}

/*
 * find_unaggregated_cols
 *	  Construct a bitmapset of the column numbers of un-aggregated Vars
 *	  appearing in our targetlist and qual (HAVING clause)
 */
static Bitmapset* find_unaggregated_cols(AggState* aggstate)
{
    Agg* node = (Agg*)aggstate->ss.ps.plan;
    Bitmapset* colnos = NULL;

    colnos = NULL;
    (void)find_unaggregated_cols_walker((Node*)node->plan.targetlist, &colnos);
    (void)find_unaggregated_cols_walker((Node*)node->plan.qual, &colnos);
    return colnos;
}

static bool find_unaggregated_cols_walker(Node* node, Bitmapset** colnos)
{
    if (node == NULL)
        return false;
    if (IsA(node, Var)) {
        Var* var = (Var*)node;

        /* setrefs.c should have set the varno to OUTER_VAR */
        Assert(var->varno == OUTER_VAR);
        Assert(var->varlevelsup == 0);
        *colnos = bms_add_member(*colnos, var->varattno);
        return false;
    }
    if (IsA(node, Aggref) || IsA(node, GroupingFunc)) {
        /* do not descend into aggregate exprs */
        return false;
    }
    return expression_tree_walker(node, (bool (*)())find_unaggregated_cols_walker, (void*)colnos);
}

/*
 * Initialize the hash table to empty.
 *
 * The hash table always lives in the aggcontext memory context.
 */
static void build_hash_table(AggState* aggstate)
{
    Agg* node = (Agg*)aggstate->ss.ps.plan;
    MemoryContext tmpmem = aggstate->tmpcontext->ecxt_per_tuple_memory;
    Size entrysize;
    int64 workMem = SET_NODEMEM(node->plan.operatorMemKB[0], node->plan.dop);

    Assert(node->aggstrategy == AGG_HASHED);
#ifdef USE_ASSERT_CHECKING
    Assert(node->numGroups > 0);
#else
    if (node->numGroups <= 0) {
        elog(LOG, "[build_hash_table]: unexpected numGroups: %ld.", node->numGroups);
        node->numGroups = (long)LONG_MAX;
    }
#endif

    entrysize = offsetof(AggHashEntryData, pergroup) + (aggstate->numaggs) * sizeof(AggStatePerGroupData);

    aggstate->hashtable = BuildTupleHashTable(node->numCols,
        node->grpColIdx,
        aggstate->phase->eqfunctions,
        aggstate->hashfunctions,
        node->numGroups,
        entrysize,
        aggstate->aggcontexts[0],
        tmpmem,
        workMem);
}

/*
 * Create a list of the tuple columns that actually need to be stored in
 * hashtable entries.  The incoming tuples from the child plan node will
 * contain grouping columns, other columns referenced in our targetlist and
 * qual, columns used to compute the aggregate functions, and perhaps just
 * junk columns we don't use at all.  Only columns of the first two types
 * need to be stored in the hashtable, and getting rid of the others can
 * make the table entries significantly smaller.  To avoid messing up Var
 * numbering, we keep the same tuple descriptor for hashtable entries as the
 * incoming tuples have, but set unwanted columns to NULL in the tuples that
 * go into the table.
 *
 * To eliminate duplicates, we build a bitmapset of the needed columns, then
 * convert it to an integer list (cheaper to scan at runtime). The list is
 * in decreasing order so that the first entry is the largest;
 * lookup_hash_entry depends on this to use table's getsomeattrs correctly.
 * Note that the list is preserved over ExecReScanAgg, so we allocate it in
 * the per-query context (unlike the hash table itself).
 *
 * Note: at present, searching the tlist/qual is not really necessary since
 * the parser should disallow any unaggregated references to ungrouped
 * columns.  However, the search will be needed when we add support for
 * SQL99 semantics that allow use of "functionally dependent" columns that
 * haven't been explicitly grouped by.
 */
List* find_hash_columns(AggState* aggstate)
{
    Agg* node = (Agg*)aggstate->ss.ps.plan;
    Bitmapset* colnos = NULL;
    List* collist = NIL;
    int i;

    /* Find Vars that will be needed in tlist and qual */
    colnos = find_unaggregated_cols(aggstate);
    /* Add in all the grouping columns */
    for (i = 0; i < node->numCols; i++)
        colnos = bms_add_member(colnos, node->grpColIdx[i]);

    /* Convert to list, using lcons so largest element ends up first */
    collist = NIL;
    while ((i = bms_first_member(colnos)) >= 0)
        collist = lcons_int(i, collist);
    bms_free_ext(colnos);

    return collist;
}

/*
 * Estimate per-hash-table-entry overhead for the planner.
 *
 * Note that the estimate does not include space for pass-by-reference
 * transition data values, nor for the representative tuple of each group.
 */
Size hash_agg_entry_size(int numAggs)
{
    Size entrysize;

    /* This must match build_hash_table */
    entrysize = offsetof(AggHashEntryData, pergroup) + numAggs * sizeof(AggStatePerGroupData);
    entrysize = MAXALIGN(entrysize);
    /* Account for hashtable overhead (assuming fill factor = 1) */
    entrysize += 3 * sizeof(void*);
    return entrysize;
}

/*
 * Compute the hash value for a tuple
 */
uint32 ComputeHashValue(TupleHashTable hashtbl)
{
    TupleTableSlot* slot = NULL;
    TupleHashTable hashtable = hashtbl;
    int numCols = hashtable->numCols;
    AttrNumber* keyColIdx = hashtable->keyColIdx;
    FmgrInfo* hashfunctions = NULL;
    uint32 hashkey = 0;
    int i;

    /* Process the current input tuple for the table */
    slot = hashtable->inputslot;
    hashfunctions = hashtable->in_hash_funcs;

    /* Get the Table Accessor Method*/
    Assert(slot != NULL && slot->tts_tupleDescriptor != NULL);

    for (i = 0; i < numCols; i++) {
        AttrNumber att = keyColIdx[i];
        Datum attr;
        bool isNull = true;

        /* rotate hashkey left 1 bit at each step */
        hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);

        attr = tableam_tslot_getattr(slot, att, &isNull);
        
        /* treat nulls as having hash key 0 */
        if (!isNull) {
            uint32 hkey;

            hkey = DatumGetUInt32(FunctionCall1(&hashfunctions[i], attr));
            hashkey ^= hkey;
        }
    }

    hashkey = DatumGetUInt32(hash_uint32(hashkey));

    return hashkey;
}

/*
 * Find or create a hashtable entry for the tuple group containing the
 * given tuple.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static AggHashEntry lookup_hash_entry(AggState* aggstate, TupleTableSlot* inputslot)
{
    TupleTableSlot* hashslot = aggstate->hashslot;
    ListCell* l = NULL;
    AggHashEntry entry;
    bool isnew = false;
    AggWriteFileControl* TempFileControl = (AggWriteFileControl*)aggstate->aggTempFileControl;

    /* if first time through, initialize hashslot by cloning input slot */
    if (hashslot->tts_tupleDescriptor == NULL) {
        ExecSetSlotDescriptor(hashslot, inputslot->tts_tupleDescriptor);
        /* Make sure all unused columns are NULLs */
        ExecStoreAllNullTuple(hashslot);
    }

    /* transfer just the needed columns into hashslot */
    tableam_tslot_getsomeattrs(inputslot, linitial_int(aggstate->hash_needed));
    foreach (l, aggstate->hash_needed) {
        int varNumber = lfirst_int(l) - 1;

        hashslot->tts_values[varNumber] = inputslot->tts_values[varNumber];
        hashslot->tts_isnull[varNumber] = inputslot->tts_isnull[varNumber];
    }

    if (TempFileControl->spillToDisk == false || TempFileControl->finishwrite == true) {
        /* find or create the hashtable entry using the filtered tuple */
        entry = (AggHashEntry)LookupTupleHashEntry(aggstate->hashtable, hashslot, &isnew, true);
    } else {
        /* this solt need be insert into temp file instead of hash table if it is not existed in hash table */
        entry = (AggHashEntry)LookupTupleHashEntry(aggstate->hashtable, hashslot, &isnew, false);
    }

    if (isnew) {
        /* this slot is new and has be inserted to hash table */
        if (entry) {
            /* initialize aggregates for new tuple group */
            initialize_aggregates(aggstate, aggstate->peragg, entry->pergroup);
            agg_spill_to_disk(TempFileControl,
                            aggstate->hashtable,
                            aggstate->hashslot,
                            ((Agg*)aggstate->ss.ps.plan)->numGroups,
                            true,
                            aggstate->ss.ps.plan->plan_node_id,
                            SET_DOP(aggstate->ss.ps.plan->dop),
                            aggstate->ss.ps.instrument);

            if (TempFileControl->filesource && aggstate->ss.ps.instrument) {
                TempFileControl->filesource->m_spill_size = &aggstate->ss.ps.instrument->sorthashinfo.spill_size;
            }
        } else { /* this slot is new, it need be inserted to temp file */
            Assert(TempFileControl->spillToDisk == true && TempFileControl->finishwrite == false);
            uint32 hashvalue;
            MinimalTuple tuple = ExecFetchSlotMinimalTuple(inputslot);
            MemoryContext oldContext;
            /*
             * Here need switch memorycontext to ecxt_per_tuple_memory, so memory which be applyed in function
             * ComputeHashValue is freed.
             */
            oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);
            hashvalue = ComputeHashValue(aggstate->hashtable);
            MemoryContextSwitchTo(oldContext);
            TempFileControl->filesource->writeTup(tuple, hashvalue & (TempFileControl->filenum - 1));
        }
    } else if (((Agg *)aggstate->ss.ps.plan)->unique_check) {
        ereport(ERROR,
                (errcode(ERRCODE_CARDINALITY_VIOLATION),
                 errmsg("more than one row returned by a subquery used as an expression")));
    }

    return entry;
}

/* prepare_data_source
 * get next data source, if it has finished return false else return true
 */
static bool prepare_data_source(AggState* node)
{
    AggWriteFileControl* TempFileControl = (AggWriteFileControl*)node->aggTempFileControl;
    /* get data from lefttree node */
    if (TempFileControl->strategy == MEMORY_HASHAGG) {
        /*
         * To avoid unnesseray memory allocate during initialization, we move building
         * process here and hash table should only be initialized once.
         */
        if (unlikely(node->hashtable == NULL)) {
            build_hash_table(node);
        }
        TempFileControl->m_hashAggSource = New(CurrentMemoryContext) hashOpSource(outerPlanState(node));
    /* get data from temp file */
    } else if (TempFileControl->strategy == DIST_HASHAGG) { 
        TempFileControl->m_hashAggSource = TempFileControl->filesource;
        if (TempFileControl->curfile >= 0) {
            TempFileControl->filesource->close(TempFileControl->curfile);
        }
        TempFileControl->curfile++;
        while (TempFileControl->curfile < TempFileControl->filenum) {
            int currfileidx = TempFileControl->curfile;
            if (TempFileControl->filesource->m_rownum[currfileidx] != 0) {
                TempFileControl->filesource->setCurrentIdx(currfileidx);
                MemoryContextResetAndDeleteChildren(node->aggcontexts[0]);
                build_hash_table(node);

                TempFileControl->filesource->rewind(currfileidx);
                node->table_filled = false;
                node->agg_done = false;
                break;
            /* no data in this temp file */
            } else {
                TempFileControl->filesource->close(currfileidx);
                TempFileControl->curfile++;
            }
        }
        if (TempFileControl->curfile == TempFileControl->filenum) {
            return false;
        }
    } else {
        Assert(false);
    }
    TempFileControl->runState = HASHAGG_FETCH;
    return true;
}

/* agg_retrieve
 * retrieving groups from hash table;
 */
static TupleTableSlot* agg_retrieve(AggState* node)
{
    AggWriteFileControl* TempFileControl = (AggWriteFileControl*)node->aggTempFileControl;
    TupleTableSlot* tmptup = NULL;
    for (;;) {
        switch (TempFileControl->runState) {
            case HASHAGG_PREPARE: {
                if (!prepare_data_source(node)) {
                    return NULL;
                }
                break;
            }
            case HASHAGG_FETCH: {
                if (!node->table_filled)
                    agg_fill_hash_table(node);
                tmptup = agg_retrieve_hash_table(node);
                if (tmptup != NULL) {
                    return tmptup;
                } else if (tmptup == NULL && TempFileControl->spillToDisk == true) {
                    TempFileControl->runState = HASHAGG_PREPARE;
                    TempFileControl->strategy = DIST_HASHAGG;
                } else {
                    return NULL;
                }
                break;
            }
            default:
                break;
        }
    }
}

/*
 * ExecAgg -
 *
 *	  ExecAgg receives tuples from its outer subplan and aggregates over
 *	  the appropriate attribute for each aggregate function use (Aggref
 *	  node) appearing in the targetlist or qual of the node.  The number
 *	  of tuples to aggregate over depends on whether grouped or plain
 *	  aggregation is selected.	In grouped aggregation, we produce a result
 *	  row for each group; in plain aggregation there's a single result row
 *	  for the whole query.	In either case, the value of each aggregate is
 *	  stored in the expression context to be used when ExecProject evaluates
 *	  the result tuple.
 */
TupleTableSlot* ExecAgg(AggState* node)
{
    /*
     * just for cooperation analysis. do nothing if is_dummy is true.
     * is_dummy is true that means Agg node is deparsed to remote sql in ForeignScan node.
     */
    if (((Agg*)node->ss.ps.plan)->is_dummy) {
        TupleTableSlot* slot = ExecProcNode(outerPlanState(node));
        return slot;
    }

    /*
     * Check to see if we're still projecting out tuples from a previous agg
     * tuple (because there is a function-returning-set in the projection
     * expressions).  If so, try to project another one.
     */
    if (node->ss.ps.ps_TupFromTlist) {
        TupleTableSlot* result = NULL;
        ExprDoneCond isDone;

        result = ExecProject(node->ss.ps.ps_ProjInfo, &isDone);
        if (isDone == ExprMultipleResult)
            return result;
        /* Done with that source tuple... */
        node->ss.ps.ps_TupFromTlist = false;
    }

    /*
     * Exit if nothing left to do.	(We must do the ps_TupFromTlist check
     * first, because in some cases agg_done gets set before we emit the final
     * aggregate tuple, and we have to finish running SRFs for it.)
     */
    if (node->agg_done)
        return NULL;

    /* Dispatch based on strategy */
    if (((Agg*)node->ss.ps.plan)->aggstrategy == AGG_HASHED)
        return agg_retrieve(node);
    else
        return agg_retrieve_direct(node);
}

/*
 * ExecAgg for non-hashed case
 */
static TupleTableSlot* agg_retrieve_direct(AggState* aggstate)
{
    Agg* node = aggstate->phase->aggnode;
    ExprContext* econtext = NULL;
    ExprContext* tmpcontext = NULL;
    AggStatePerAgg peragg;
    AggStatePerGroup pergroup;
    TupleTableSlot* outerslot = NULL;
    TupleTableSlot* firstSlot = NULL;
    TupleTableSlot* result = NULL;
    bool hasGroupingSets = aggstate->phase->numsets > 0;
    int numGroupingSets = Max(aggstate->phase->numsets, 1);
    int currentSet;
    int nextSetSize;
    int numReset;

    /*
     * get state info from node
     *
     * econtext is the per-output-tuple expression context
     * tmpcontext is the per-input-tuple expression context
     *
     */
    econtext = aggstate->ss.ps.ps_ExprContext;

    tmpcontext = aggstate->tmpcontext;
    peragg = aggstate->peragg;
    pergroup = aggstate->pergroup;
    firstSlot = aggstate->ss.ss_ScanTupleSlot;

    /*
     * We loop retrieving groups until we find one matching
     *aggstate->ss.ps.qual
     *
     * For grouping sets, we have the invariant that aggstate->projected_set
     * is either -1 (initial call) or the index (starting from 0) in
     * gset_lengths for the group we just completed (either by projecting a
     * row or by discarding it in the qual).
     *
     * aggstate->ss.ps.qual
     */
    while (!aggstate->agg_done) {
        /*
         * Clear the per-output-tuple context for each group, as well as
         * aggcontext (which contains any pass-by-ref transvalues of the old
         * group).  Some aggregate functions store working state in child
         * contexts; those now get reset automatically without us needing to
         * do anything special.
         *
         * We use ReScanExprContext not just ResetExprContext because we want
         * any registered shutdown callbacks to be called.  That allows
         * aggregate functions to ensure they've cleaned up any non-memory
         * resources.
         *
         */
        ReScanExprContext(econtext);

        /*
         * Determine how many grouping sets need to be reset at this boundary.
         */
        if (aggstate->projected_set >= 0 && aggstate->projected_set < numGroupingSets)
            numReset = aggstate->projected_set + 1;
        else
            numReset = numGroupingSets;

        /*
         * numReset can change on a phase boundary, but that's OK; we want to
         * reset the contexts used in _this_ phase, and later, after possibly
         * changing phase, initialize the right number of aggregates for the
         * _new_ phase.
         */
        for (int i = 0; i < numReset; i++) {
            MemoryContextReset(aggstate->aggcontexts[i]);
        }

        /*
         * Check if input is complete and there are no more groups to project
         * in this phase; move to next phase or mark as done.
         */
        if (aggstate->input_done == true && aggstate->projected_set >= (numGroupingSets - 1)) {
            if (aggstate->current_phase < aggstate->numphases - 1) {
                initialize_phase(aggstate, aggstate->current_phase + 1);
                aggstate->input_done = false;
                aggstate->projected_set = -1;
                numGroupingSets = Max(aggstate->phase->numsets, 1);
                node = aggstate->phase->aggnode;
                numReset = numGroupingSets;
            } else {
                aggstate->agg_done = true;
                break;
            }
        }

        /*
         * Get the number of columns in the next grouping set after the last
         * projected one (if any). This is the number of columns to compare to
         * see if we reached the boundary of that set too.
         */
        if (aggstate->projected_set >= 0 && aggstate->projected_set < (numGroupingSets - 1))
            nextSetSize = aggstate->phase->gset_lengths[aggstate->projected_set + 1];
        else
            nextSetSize = 0;

        /* ----------
         * If a subgroup for the current grouping set is present, project it.
         *
         * We have a new group if:
         *	- we're out of input but haven't projected all grouping sets
         *	  (checked above)
         * OR
         *	  - we already projected a row that wasn't from the last grouping
         *		set
         *	  AND
         *	  - the next grouping set has at least one grouping column (since
         *		empty grouping sets project only once input is exhausted)
         *	  AND
         *	  - the previous and pending rows differ on the grouping columns
         *		of the next grouping set
         * ----------
         */
        if (aggstate->input_done || (node->aggstrategy == AGG_SORTED && aggstate->projected_set != -1 &&
                                        aggstate->projected_set < (numGroupingSets - 1) && nextSetSize > 0 &&
                                        !execTuplesMatch(econtext->ecxt_outertuple,
                                            tmpcontext->ecxt_outertuple,
                                            nextSetSize,
                                            node->grpColIdx,
                                            aggstate->phase->eqfunctions,
                                            tmpcontext->ecxt_per_tuple_memory))) {
            aggstate->projected_set += 1;

            Assert(aggstate->projected_set < numGroupingSets);
            Assert(nextSetSize > 0 || aggstate->input_done);
        } else {
            /*
             * We no longer care what group we just projected, the next
             * projection will always be the first (or only) grouping set
             * (unless the input proves to be empty).
             */
            aggstate->projected_set = 0;

            /*
             * If we don't already have the first tuple of the new group,
             * fetch it from the outer plan.
             */
            if (aggstate->grp_firstTuple == NULL) {
                outerslot = fetch_input_tuple(aggstate);
                if (!TupIsNull(outerslot)) {
                    /*
                     * Make a copy of the first input tuple; we will use this
                     * for comparisons (in group mode) and for projection.
                     */
                    aggstate->grp_firstTuple = ExecCopySlotTuple(outerslot);
                } else {
                    /* outer plan produced no tuples at all */
                    if (hasGroupingSets) {
                        /*
                         * If there was no input at all, we need to project
                         * rows only if there are grouping sets of size 0.
                         * Note that this implies that there can't be any
                         * references to ungrouped Vars, which would otherwise
                         * cause issues with the empty output slot.
                         *
                         * XXX: This is no longer true, we currently deal with
                         * this in finalize_aggregates().
                         */
                        aggstate->input_done = true;

                        while (aggstate->phase->gset_lengths[aggstate->projected_set] > 0) {
                            aggstate->projected_set += 1;
                            if (aggstate->projected_set >= numGroupingSets) {
                                /*
                                 * We can't set agg_done here because we might
                                 * have more phases to do, even though the
                                 * input is empty. So we need to restart the
                                 * whole outer loop.
                                 */
                                break;
                            }
                        }

                        if (aggstate->projected_set >= numGroupingSets)
                            continue;
                    } else {
                        aggstate->agg_done = true;
                        /* If we are grouping, we should produce no tuples too */
                        if (node->aggstrategy != AGG_PLAIN)
                            return NULL;
                    }
                }
            }

            /*
             * Initialize working state for a new input tuple group.
             */
            initialize_aggregates(aggstate, peragg, pergroup, numReset);

            if (aggstate->grp_firstTuple != NULL) {
                /*
                 * Store the copied first input tuple in the tuple table slot
                 * reserved for it.  The tuple will be deleted when it is
                 * cleared from the slot.
                 */
                (void)ExecStoreTuple(aggstate->grp_firstTuple, firstSlot, InvalidBuffer, true);
                aggstate->grp_firstTuple = NULL; /* don't keep two
                                                  * pointers */
                /* set up for first advance_aggregates call */
                tmpcontext->ecxt_outertuple = firstSlot;

                /*
                 * Process each outer-plan tuple, and then fetch the next one,
                 * until we exhaust the outer plan or cross a group boundary.
                 */
                for (;;) {
                    advance_aggregates(aggstate, pergroup);

                    /* Reset per-input-tuple context after each tuple */
                    ResetExprContext(tmpcontext);

                    outerslot = fetch_input_tuple(aggstate);
                    if (TupIsNull(outerslot)) {
                        /* no more outer-plan tuples available */
                        if (hasGroupingSets) {
                            aggstate->input_done = true;
                            break;
                        } else {
                            aggstate->agg_done = true;
                            break;
                        }
                    }
                    /* set up for next advance_aggregates call */
                    tmpcontext->ecxt_outertuple = outerslot;

                    /*
                     * If we are grouping, check whether we've crossed a group
                     * boundary.
                     */
                    if (node->aggstrategy == AGG_SORTED) {
                        if (!execTuplesMatch(firstSlot, outerslot, node->numCols, node->grpColIdx,
                                            aggstate->phase->eqfunctions, tmpcontext->ecxt_per_tuple_memory)) {
                            aggstate->grp_firstTuple = ExecCopySlotTuple(outerslot);
                            break;
                        }
                    }
                }
            }

            /*
             * Use the representative input tuple for any references to
             * non-aggregated input columns in aggregate direct args, the node
             * qual, and the tlist.  (If we are not grouping, and there are no
             * input rows at all, we will come here with an empty firstSlot
             * ... but if not grouping, there can't be any references to
             * non-aggregated input columns, so no problem.)
             */
            econtext->ecxt_outertuple = firstSlot;
        }

        Assert(aggstate->projected_set >= 0);

        currentSet = aggstate->projected_set;

        prepare_projection_slot(aggstate, econtext->ecxt_outertuple, currentSet);

        finalize_aggregates(aggstate, peragg, pergroup, currentSet);

        /*
         * If there's no row to project right now, we must continue rather
         * than returning a null since there might be more groups.
         */
        result = project_aggregates(aggstate);
        if (result != NULL)
            return result;
    }

    /* No more groups */
    return NULL;
}

/*
 * ExecAgg for hashed case: phase 1, read input and build hash table
 */
static void agg_fill_hash_table(AggState* aggstate)
{
    ExprContext* tmpcontext = NULL;
    AggHashEntry entry;
    TupleTableSlot* outerslot = NULL;
    AggWriteFileControl* TempFileControl = (AggWriteFileControl*)aggstate->aggTempFileControl;

    /*
     * get state info from node
     *
     * tmpcontext is the per-input-tuple expression context
     */
    /* tmpcontext is the per-input-tuple expression context */
    tmpcontext = aggstate->tmpcontext;

    /*
     * Process each outer-plan tuple, and then fetch the next one, until we
     * exhaust the outer plan.
     */
    WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_HASHAGG_BUILD_HASH);
    for (;;) {
        outerslot = TempFileControl->m_hashAggSource->getTup();

        if (TupIsNull(outerslot)) {
            if (!TempFileControl->spillToDisk) {
                /* Early free left tree after hash table built */
                ExecEarlyFree(outerPlanState(aggstate));

                EARLY_FREE_LOG(elog(LOG,
                    "Early Free: Hash Table for Agg"
                    " is built at node %d, memory used %d MB.",
                    (aggstate->ss.ps.plan)->plan_node_id,
                    getSessionMemoryUsageMB()));
            }
            pgstat_report_waitstatus(oldStatus);
            break;
        }

        /* set up for advance_aggregates call */
        tmpcontext->ecxt_outertuple = outerslot;

        /* Find or build hashtable entry for this tuple's group */
        entry = lookup_hash_entry(aggstate, outerslot);

        if (entry != NULL) {
            /* Advance the aggregates */
            advance_aggregates(aggstate, entry->pergroup);
        } else {
            /* this outerslot is inserted to temp table, it will be compute when the temp file be readed */
        }

        /* Reset per-input-tuple context after each tuple */
        ResetExprContext(tmpcontext);
    }

    aggstate->table_filled = true;
    if (HAS_INSTR(&aggstate->ss, true)) {        
        AggWriteFileControl *aggTempFileControl = (AggWriteFileControl*)aggstate->aggTempFileControl;
        if (aggTempFileControl->spillToDisk == false && aggTempFileControl->inmemoryRownum > 0)
            aggstate->hashtable->width /= aggTempFileControl->inmemoryRownum;
        if (aggTempFileControl->strategy == MEMORY_HASHAGG)
            aggstate->ss.ps.instrument->width = (int)aggstate->hashtable->width;
        aggstate->ss.ps.instrument->sysBusy = aggstate->hashtable->causedBySysRes;
        aggstate->ss.ps.instrument->spreadNum = aggTempFileControl->spreadNum;
    }
    if (TempFileControl->spillToDisk && TempFileControl->finishwrite == false) {
        TempFileControl->finishwrite = true;
        if (HAS_INSTR(&aggstate->ss, true)) {
            PlanState* planstate = &aggstate->ss.ps;
            planstate->instrument->sorthashinfo.hash_FileNum = (TempFileControl->filenum);
            planstate->instrument->sorthashinfo.hash_writefile = true;
        }
    }
    /* Initialize to walk the hash table */
    ResetTupleHashIterator(aggstate->hashtable, &aggstate->hashiter);
}

/*
 * ExecAgg for hashed case: phase 2, retrieving groups from hash table
 */
static TupleTableSlot* agg_retrieve_hash_table(AggState* aggstate)
{
    ExprContext* econtext = NULL;
    AggStatePerAgg peragg;
    AggStatePerGroup pergroup;
    AggHashEntry entry;
    TupleTableSlot* firstSlot = NULL;
    TupleTableSlot* result = NULL;

    /*
     * get state info from node
     */
    /* econtext is the per-output-tuple expression context */
    econtext = aggstate->ss.ps.ps_ExprContext;
    peragg = aggstate->peragg;
    firstSlot = aggstate->ss.ss_ScanTupleSlot;

    /*
     * We loop retrieving groups until we find one satisfying
     * aggstate->ss.ps.qual
     */
    while (!aggstate->agg_done) {
        /*
         * Find the next entry in the hash table
         */
        entry = (AggHashEntry)ScanTupleHashTable(&aggstate->hashiter);
        if (entry == NULL) {
            /* No more entries in hashtable, so done */
            aggstate->agg_done = TRUE;
            return NULL;
        }

        /*
         * Clear the per-output-tuple context for each group
         */
        ResetExprContext(econtext);

        /*
         * Store the copied first input tuple in the tuple table slot reserved
         * for it, so that it can be used in ExecProject.
         */
        ExecStoreMinimalTuple(entry->shared.firstTuple, firstSlot, false);

        pergroup = entry->pergroup;

        /*
         * Finalize each aggregate calculation, and stash results in the
         * per-output-tuple context.
         */
        finalize_aggregates(aggstate, peragg, pergroup, 0);

        /*
         * Use the representative input tuple for any references to
         * non-aggregated input columns in the qual and tlist.
         */
        econtext->ecxt_outertuple = firstSlot;

        /*
         * Check the qual (HAVING clause); if the group does not match, ignore
         * it and loop back to try to process another group.
         */
        result = project_aggregates(aggstate);
        if (result != NULL) {
            return result;
        }
    }

    /* No more groups */
    return NULL;
}

int getPower2Num(int num)
{
    int i = 1;
    while (i < num) {
        i <<= 1;
    }
    return i;
}

/* -----------------
 * ExecInitAgg
 *
 *	Creates the run-time information for the agg node produced by the
 *	planner and initializes its outer subtree
 * -----------------
 */
AggState* ExecInitAgg(Agg* node, EState* estate, int eflags)
{
    AggState* aggstate = NULL;
    AggStatePerAgg peragg;
    Plan* outerPlan = NULL;
    ExprContext* econtext = NULL;
    int numaggs, aggno;
    int phase;
    ListCell* l = NULL;
    Bitmapset* all_grouped_cols = NULL;
    int numGroupingSets = 1;
    int numPhases;
    int currentsortno = 0;
    int i = 0;
    int j = 0;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * create state structure
     */
    aggstate = makeNode(AggState);
    aggstate->ss.ps.plan = (Plan*)node;
    aggstate->ss.ps.state = estate;

    aggstate->aggs = NIL;
    aggstate->numaggs = 0;
    aggstate->maxsets = 0;
    aggstate->hashfunctions = NULL;
    aggstate->projected_set = -1;
    aggstate->current_set = 0;
    aggstate->peragg = NULL;
    aggstate->curperagg = NULL;
    aggstate->agg_done = false;
    aggstate->input_done = false;
    aggstate->pergroup = NULL;
    aggstate->grp_firstTuple = NULL;
    aggstate->hashtable = NULL;
    aggstate->sort_in = NULL;
    aggstate->sort_out = NULL;
    aggstate->is_final = node->is_final;

    /*
     * Calculate the maximum number of grouping sets in any phase; this
     * determines the size of some allocations.
     */
    if (node->groupingSets) {
        Assert(node->aggstrategy != AGG_HASHED);

        numGroupingSets = list_length(node->groupingSets);

        foreach (l, node->chain) {
            Agg* agg = (Agg*)lfirst(l);

            numGroupingSets = Max(numGroupingSets, list_length(agg->groupingSets));
        }
    }

    aggstate->maxsets = numGroupingSets;
    aggstate->numphases = numPhases = 1 + list_length(node->chain);

    aggstate->aggcontexts = (MemoryContext*)palloc0(sizeof(MemoryContext) * numGroupingSets);
    /*
     * Create expression contexts.  We need three or more, one for
     * per-input-tuple processing, one for per-output-tuple processing, and
     * one for each grouping set. We cheat a little
     * by using ExecAssignExprContext() to build both.
     *
     * NOTE: the details of what is stored in aggcontexts and what is stored
     * in the regular per-query memory context are driven by a simple
     * decision: we want to reset the aggcontext at group boundaries (if not
     * hashing) and in ExecReScanAgg to recover no-longer-wanted space.
     */
    ExecAssignExprContext(estate, &aggstate->ss.ps);
    aggstate->tmpcontext = aggstate->ss.ps.ps_ExprContext;

    int64 workMem = SET_NODEMEM(node->plan.operatorMemKB[0], node->plan.dop);
    int64 maxMem = (node->plan.operatorMaxMem > node->plan.operatorMemKB[0])
                       ? SET_NODEMEM(node->plan.operatorMaxMem, node->plan.dop)
                       : 0;

    /* Create memcontext. The per-tuple memory context of the
     * per-grouping-set aggcontexts replaces the standalone
     * memory context formerly used to hold transition values.
     */
    for (i = 0; i < numGroupingSets; ++i) {
        aggstate->aggcontexts[i] = AllocSetContextCreate(CurrentMemoryContext,
            "AggContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            STANDARD_CONTEXT,
            workMem * 1024L);
    }

    ExecAssignExprContext(estate, &aggstate->ss.ps);

    /*
     * tuple table initialization
     */
    ExecInitScanTupleSlot(estate, &aggstate->ss);
    ExecInitResultTupleSlot(estate, &aggstate->ss.ps);
    aggstate->hashslot = ExecInitExtraTupleSlot(estate);
    aggstate->sort_slot = ExecInitExtraTupleSlot(estate);

    /*
     * initialize child expressions
     *
     * Note: ExecInitExpr finds Aggrefs for us, and also checks that no aggs
     * contain other agg calls in their arguments.	This would make no sense
     * under SQL semantics anyway (and it's forbidden by the spec). Because
     * that is true, we don't need to worry about evaluating the aggs in any
     * particular order.
     */
    aggstate->ss.ps.targetlist = (List*)ExecInitExpr((Expr*)node->plan.targetlist, (PlanState*)aggstate);
    aggstate->ss.ps.qual = (List*)ExecInitExpr((Expr*)node->plan.qual, (PlanState*)aggstate);

    /*
     * initialize child nodes
     *
     * If we are doing a hashed aggregation then the child plan does not need
     * to handle REWIND efficiently; see ExecReScanAgg.
     */
    if (node->aggstrategy == AGG_HASHED)
        eflags &= ~EXEC_FLAG_REWIND;
    outerPlan = outerPlan(node);

    outerPlanState(aggstate) = ExecInitNode(outerPlan, estate, eflags);

    /*
     * initialize source tuple type.
     */
    ExecAssignScanTypeFromOuterPlan(&aggstate->ss);
    if (node->chain) {
        ExecSetSlotDescriptor(aggstate->sort_slot, aggstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
    }

    /*
     * Initialize result tuple type and projection info.
     * Result tuple slot of Aggregation always contains a virtual tuple,
     * Default tableAMtype for this slot is Heap.
     */
    ExecAssignResultTypeFromTL(&aggstate->ss.ps, TAM_HEAP);
    ExecAssignProjectionInfo(&aggstate->ss.ps, NULL);

    aggstate->ss.ps.ps_TupFromTlist = false;

    /*
     * get the count of aggregates in targetlist and quals
     */
    numaggs = aggstate->numaggs;
    Assert(numaggs == list_length(aggstate->aggs));
    if (numaggs <= 0) {
        /*
         * This is not an error condition: we might be using the Agg node just
         * to do hash-based grouping.  Even in the regular case,
         * constant-expression simplification could optimize away all of the
         * Aggrefs in the targetlist and qual.	So keep going, but force local
         * copy of numaggs positive so that palloc()s below don't choke.
         */
        numaggs = 1;
    }

    /*
     * For each phase, prepare grouping set data and fmgr lookup data for
     * compare functions.  Accumulate all_grouped_cols in passing.
     */
    aggstate->phases = (AggStatePerPhaseData*)palloc0(numPhases * sizeof(AggStatePerPhaseData));

    for (phase = 0; phase < numPhases; ++phase) {
        AggStatePerPhase phasedata = &aggstate->phases[phase];
        Agg* aggnode = NULL;
        Sort* sortnode = NULL;
        int num_sets;

        if (phase > 0) {
            aggnode = (Agg*)list_nth(node->chain, phase - 1);
            sortnode = (Sort*)aggnode->plan.lefttree;
            Assert(IsA(sortnode, Sort));
        } else {
            aggnode = node;
            sortnode = NULL;
        }

        phasedata->numsets = num_sets = list_length(aggnode->groupingSets);

        if (num_sets) {
            phasedata->gset_lengths = (int*)palloc(num_sets * sizeof(int));
            phasedata->grouped_cols = (Bitmapset**)palloc(num_sets * sizeof(Bitmapset*));

            i = 0;
            foreach (l, aggnode->groupingSets) {
                int current_length = list_length((List*)lfirst(l));
                Bitmapset* cols = NULL;

                /* planner forces this to be correct */
                for (j = 0; j < current_length; ++j)
                    cols = bms_add_member(cols, aggnode->grpColIdx[j]);

                phasedata->grouped_cols[i] = cols;
                phasedata->gset_lengths[i] = current_length;
                ++i;
            }

            all_grouped_cols = bms_add_members(all_grouped_cols, phasedata->grouped_cols[0]);
        } else {
            Assert(phase == 0);

            phasedata->gset_lengths = NULL;
            phasedata->grouped_cols = NULL;
        }

        /*
         * If we are grouping, precompute fmgr lookup data for inner loop.
         */
        if (aggnode->aggstrategy == AGG_SORTED) {
            Assert(aggnode->numCols > 0);

            phasedata->eqfunctions = execTuplesMatchPrepare(aggnode->numCols, aggnode->grpOperators);
        }

        phasedata->aggnode = aggnode;
        phasedata->sortnode = sortnode;
    }

    /*
     * Convert all_grouped_cols to a descending-order list.
     */
    i = -1;
    while ((i = bms_next_member(all_grouped_cols, i)) >= 0)
        aggstate->all_grouped_cols = lcons_int(i, aggstate->all_grouped_cols);

    /*
     * Hashing can only appear in the initial phase.
     */
    if (node->aggstrategy == AGG_HASHED)
        execTuplesHashPrepare(
            node->numCols, node->grpOperators, &aggstate->phases[0].eqfunctions, &aggstate->hashfunctions);

    /*
     * Initialize current phase-dependent values to initial phase
     */
    aggstate->current_phase = 0;
    initialize_phase(aggstate, 0);

    /*
     * Set up aggregate-result storage in the output expr context, and also
     * allocate my private per-agg working storage
     */
    econtext = aggstate->ss.ps.ps_ExprContext;
    econtext->ecxt_aggvalues = (Datum*)palloc0(sizeof(Datum) * numaggs);
    econtext->ecxt_aggnulls = (bool*)palloc0(sizeof(bool) * numaggs);

    peragg = (AggStatePerAgg)palloc0(sizeof(AggStatePerAggData) * numaggs);
    aggstate->peragg = peragg;

    if (node->aggstrategy == AGG_HASHED) {
        aggstate->table_filled = false;
        /* Compute the columns we actually need to hash on */
        aggstate->hash_needed = find_hash_columns(aggstate);
    } else {
        AggStatePerGroup pergroup;

        pergroup = (AggStatePerGroup)palloc0(sizeof(AggStatePerGroupData) * numaggs * numGroupingSets);
        aggstate->pergroup = pergroup;
    }

    /*
     * Perform lookups of aggregate function info, and initialize the
     * unchanging fields of the per-agg data.  We also detect duplicate
     * aggregates (for example, "SELECT sum(x) ... HAVING sum(x) > 0"). When
     * duplicates are detected, we only make an AggStatePerAgg struct for the
     * first one.  The clones are simply pointed at the same result entry by
     * giving them duplicate aggno values.
     */
    aggno = -1;
    foreach (l, aggstate->aggs) {
        AggrefExprState* aggrefstate = (AggrefExprState*)lfirst(l);
        Aggref* aggref = (Aggref*)aggrefstate->xprstate.expr;
        AggStatePerAgg peraggstate;
        Oid inputTypes[FUNC_MAX_ARGS];
        int numArguments;
        int numDirectArgs;
        int numInputs;
        int numSortCols;
        int numDistinctCols;
        List* sortlist = NIL;
        HeapTuple aggTuple;
        Form_pg_aggregate aggform;
        Oid aggtranstype;
        AclResult aclresult;
        Oid transfn_oid, finalfn_oid;
#ifdef PGXC
        Oid collectfn_oid;
        Expr* collectfnexpr = NULL;
#endif /* PGXC */
        Expr* transfnexpr = NULL;
        Expr* finalfnexpr = NULL;
        Datum textInitVal;
        ListCell* lc = NULL;

        /* Planner should have assigned aggregate to correct level */
        Assert(aggref->agglevelsup == 0);

        /* Look for a previous duplicate aggregate */
        for (i = 0; i <= aggno; i++) {
            if (equal(aggref, peragg[i].aggref) && !contain_volatile_functions((Node*)aggref))
                break;
        }
        if (i <= aggno) {
            /* Found a match to an existing entry, so just mark it */
            aggrefstate->aggno = i;
            continue;
        }

        /* Nope, so assign a new PerAgg record */
        peraggstate = &peragg[++aggno];

        /* Mark Aggref state node with assigned index in the result array */
        aggrefstate->aggno = aggno;

        /* Fill in the peraggstate data */
        peraggstate->aggrefstate = aggrefstate;
        peraggstate->aggref = aggref;

        peraggstate->sortstates = (Tuplesortstate**)palloc0(sizeof(Tuplesortstate*) * numGroupingSets);

        for (currentsortno = 0; currentsortno < numGroupingSets; currentsortno++)
            peraggstate->sortstates[currentsortno] = NULL;

        /* Fetch the pg_aggregate row */
        aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggref->aggfnoid));
        if (!HeapTupleIsValid(aggTuple))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmodule(MOD_EXECUTOR),
                    errmsg("cache lookup failed for aggregate %u", aggref->aggfnoid)));
        aggform = (Form_pg_aggregate)GETSTRUCT(aggTuple);

        /* Check permission to call aggregate function */
        aclresult = pg_proc_aclcheck(aggref->aggfnoid, GetUserId(), ACL_EXECUTE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(aggref->aggfnoid));

        peraggstate->transfn_oid = transfn_oid = aggform->aggtransfn;
        peraggstate->finalfn_oid = finalfn_oid = aggform->aggfinalfn;
#ifdef PGXC
        peraggstate->collectfn_oid = collectfn_oid = aggform->aggcollectfn;

        peraggstate->is_avg = false;
        if (finalfn_oid == 1830) {
            peraggstate->is_avg = true;
        }
#ifdef ENABLE_MULTIPLE_NODES
        /*
         * For PGXC final and collection functions are used to combine results at Coordinator,
         * disable those for Datanode
         */
        if (IS_PGXC_DATANODE) {
            if (!u_sess->exec_cxt.under_stream_runtime) {
                peraggstate->finalfn_oid = finalfn_oid = InvalidOid;
                peraggstate->collectfn_oid = collectfn_oid = InvalidOid;
            } else {
                if (need_adjust_agg_inner_func_type(peraggstate->aggref)) {
                    if (!node->is_final && !node->single_node)
                        peraggstate->finalfn_oid = finalfn_oid = InvalidOid;
                    if (aggref->aggstage == 0 && !node->is_final && !node->single_node)
                        peraggstate->collectfn_oid = collectfn_oid = InvalidOid;
                }
            }
        }
#else
        if (IS_STREAM_PLAN || StreamThreadAmI()) {
            if (need_adjust_agg_inner_func_type(peraggstate->aggref)) {
                if (!node->is_final && !node->single_node) {
                    peraggstate->finalfn_oid = finalfn_oid = InvalidOid;
                }
                if (aggref->aggstage == 0 && !node->is_final && !node->single_node) {
                    peraggstate->collectfn_oid = collectfn_oid = InvalidOid;
                }
            }
        }
#endif /* ENABLE_MULTIPLE_NODES */
#endif /* PGXC */
        /* Check that aggregate owner has permission to call component fns */
        {
            HeapTuple procTuple;
            Oid aggOwner;

            procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(aggref->aggfnoid));
            if (!HeapTupleIsValid(procTuple))
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmodule(MOD_EXECUTOR),
                        errmsg("cache lookup failed for aggregate function %u", aggref->aggfnoid)));
            aggOwner = ((Form_pg_proc)GETSTRUCT(procTuple))->proowner;
            ReleaseSysCache(procTuple);

            aclresult = pg_proc_aclcheck(transfn_oid, aggOwner, ACL_EXECUTE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(transfn_oid));
            if (OidIsValid(finalfn_oid)) {
                aclresult = pg_proc_aclcheck(finalfn_oid, aggOwner, ACL_EXECUTE);
                if (aclresult != ACLCHECK_OK)
                    aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(finalfn_oid));
            }

#ifdef PGXC
            if (OidIsValid(collectfn_oid)) {
                aclresult = pg_proc_aclcheck(collectfn_oid, aggOwner, ACL_EXECUTE);
                if (aclresult != ACLCHECK_OK)
                    aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(collectfn_oid));
            }
#endif /* PGXC */
        }

        /*
         * Get the the number of actual arguments and identify the actual
         * datatypes of the aggregate inputs (saved in inputTypes). When
         * agg accepts ANY or a polymorphic type, the actual datatype
         * could be different from the agg's declared input types.
         */
        numArguments = get_aggregate_argtypes(aggref, inputTypes, FUNC_MAX_ARGS);
        peraggstate->numArguments = numArguments;
        /* Get the direct arguments */
        numDirectArgs = list_length(aggref->aggdirectargs);
        /* Get the number of aggregated input columns */
        numInputs = list_length(aggref->args);
        peraggstate->numInputs = numInputs;

        /* Detect the number of columns passed to the transfn */
        if (AGGKIND_IS_ORDERED_SET(aggref->aggkind))
            peraggstate->numTransInputs = numInputs;
        else
            peraggstate->numTransInputs = numArguments;

        /*
         * When agg accepts ANY or a polymorphic type, resolve actual
         * type of transition state
         */
        aggtranstype = resolve_aggregate_transtype(aggref->aggfnoid, aggform->aggtranstype, inputTypes, numArguments);

        /* build expression trees using actual argument & result types */
        build_trans_aggregate_fnexprs(numArguments,
            numDirectArgs,
            AGGKIND_IS_ORDERED_SET(aggref->aggkind),
            aggref->aggvariadic,
            aggtranstype,
            inputTypes,
            aggref->aggtype,
            aggref->inputcollid,
            transfn_oid,
            finalfn_oid,
            &transfnexpr,
            &finalfnexpr);

#ifdef PGXC
        if (OidIsValid(collectfn_oid)) {
            /* we expect final function expression to be NULL in call to
             * build_aggregate_fnexprs below, since InvalidOid is passed for
             * finalfn_oid argument. Use a dummy expression to accept that.
             */
            Expr* dummyexpr = NULL;
            /*
             * for XC, we need to setup the collection function expression as well.
             * Use build_aggregate_fnexpr() with invalid final function oid, and collection
             * function information instead of transition function information.
             * We should really be adding this step inside
             * build_aggregate_fnexprs() but this way it becomes easy to merge.
             */
            build_aggregate_fnexprs(&aggtranstype,
                1,
                aggtranstype,
                aggref->aggtype,
                aggref->inputcollid,
                collectfn_oid,
                InvalidOid,
                &collectfnexpr,
                &dummyexpr);
            Assert(!dummyexpr);
        }
#endif /* PGXC */

        fmgr_info(transfn_oid, &peraggstate->transfn);
        fmgr_info_set_expr((Node*)transfnexpr, &peraggstate->transfn);

        if (OidIsValid(finalfn_oid)) {
            fmgr_info(finalfn_oid, &peraggstate->finalfn);
            fmgr_info_set_expr((Node*)finalfnexpr, &peraggstate->finalfn);
        }

#ifdef PGXC
        if (OidIsValid(collectfn_oid)) {
            fmgr_info(collectfn_oid, &peraggstate->collectfn);
            peraggstate->collectfn.fn_expr = (Node*)collectfnexpr;
        }
#endif /* PGXC */
        peraggstate->aggCollation = aggref->inputcollid;

        get_typlenbyval(aggref->aggtype, &peraggstate->resulttypeLen, &peraggstate->resulttypeByVal);
        get_typlenbyval(aggtranstype, &peraggstate->transtypeLen, &peraggstate->transtypeByVal);

        /*
         * initval is potentially null, so don't try to access it as a struct
         * field. Must do it the hard way with SysCacheGetAttr.
         */
        textInitVal = SysCacheGetAttr(AGGFNOID, aggTuple, Anum_pg_aggregate_agginitval, &peraggstate->initValueIsNull);

        if (peraggstate->initValueIsNull) {
            peraggstate->initValue = (Datum)0;
        } else {
            peraggstate->initValue = GetAggInitVal(textInitVal, aggtranstype);
        }

#ifdef PGXC
        /*
         * initval for collection function is potentially null, so don't try to
         * access it as a struct field. Must do it the hard way with
         * SysCacheGetAttr.
         */
        textInitVal =
            SysCacheGetAttr(AGGFNOID, aggTuple, Anum_pg_aggregate_agginitcollect, &peraggstate->initCollectValueIsNull);

        if (peraggstate->initCollectValueIsNull) {
            peraggstate->initCollectValue = (Datum)0;
        } else {
            peraggstate->initCollectValue = GetAggInitVal(textInitVal, aggtranstype);
        }
#endif /* PGXC */

        /*
         * If the transfn is strict and the initval is NULL, make sure input
         * type and transtype are the same (or at least binary-compatible), so
         * that it's OK to use the first input value as the initial
         * transValue.	This should have been checked at agg definition time,
         * but just in case...
         */
        if (peraggstate->transfn.fn_strict && peraggstate->initValueIsNull) {
            if (numArguments < numDirectArgs || !IsBinaryCoercible(inputTypes[numDirectArgs], aggtranstype))
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                        errmsg(
                            "aggregate %u needs to have compatible input type and transition type", aggref->aggfnoid)));
        }

        /*
         * Get a tupledesc corresponding to the inputs (including sort
         * expressions) of the agg.
         */
        peraggstate->evaldesc = ExecTypeFromTL(aggref->args, false);

        /* Create slot we're going to do argument evaluation in */
        peraggstate->evalslot = ExecInitExtraTupleSlot(estate);
        ExecSetSlotDescriptor(peraggstate->evalslot, peraggstate->evaldesc);

        /* Set up projection info for evaluation */
        peraggstate->evalproj =
            ExecBuildProjectionInfo(aggrefstate->args, aggstate->tmpcontext, peraggstate->evalslot, NULL);

        /*
         * If we're doing either DISTINCT or ORDER BY, then we have a list of
         * SortGroupClause nodes; fish out the data in them and stick them
         * into arrays.
         * For oerdered set agg, we handle the sort operation in the transfn
         * function, so we can ignore ORDER BY.
         */
        if (AGGKIND_IS_ORDERED_SET(aggref->aggkind)) {
            numSortCols = 0;
            numDistinctCols = 0;
        } else if (aggref->aggdistinct) {
            sortlist = aggref->aggdistinct;
            numSortCols = numDistinctCols = list_length(sortlist);
            Assert(numSortCols >= list_length(aggref->aggorder));
        } else {
            sortlist = aggref->aggorder;
            numSortCols = list_length(sortlist);
            numDistinctCols = 0;
        }

        peraggstate->numSortCols = numSortCols;
        peraggstate->numDistinctCols = numDistinctCols;

        if (numSortCols > 0) {
            /*
             * We don't implement DISTINCT or ORDER BY aggs in the HASHED case
             * (yet)
             */
            Assert(node->aggstrategy != AGG_HASHED);

            /* If we have only one input, we need its len/byval info. */
            if (numInputs == 1) {
                get_typlenbyval(inputTypes[numDirectArgs], &peraggstate->inputtypeLen, &peraggstate->inputtypeByVal);
            } else if (numDistinctCols > 0) {
                /* we will need an extra slot to store prior values */
                peraggstate->uniqslot = ExecInitExtraTupleSlot(estate);
                ExecSetSlotDescriptor(peraggstate->uniqslot, peraggstate->evaldesc);
            }

            /* Extract the sort information for use later */
            peraggstate->sortColIdx = (AttrNumber*)palloc(numSortCols * sizeof(AttrNumber));
            peraggstate->sortOperators = (Oid*)palloc(numSortCols * sizeof(Oid));
            peraggstate->sortCollations = (Oid*)palloc(numSortCols * sizeof(Oid));
            peraggstate->sortNullsFirst = (bool*)palloc(numSortCols * sizeof(bool));

            i = 0;
            foreach (lc, sortlist) {
                SortGroupClause* sortcl = (SortGroupClause*)lfirst(lc);
                TargetEntry* tle = get_sortgroupclause_tle(sortcl, aggref->args);

                /* the parser should have made sure of this */
                Assert(OidIsValid(sortcl->sortop));

                peraggstate->sortColIdx[i] = tle->resno;
                peraggstate->sortOperators[i] = sortcl->sortop;
                peraggstate->sortCollations[i] = exprCollation((Node*)tle->expr);
                peraggstate->sortNullsFirst[i] = sortcl->nulls_first;
                i++;
            }
            Assert(i == numSortCols);
        }

        if (aggref->aggdistinct) {
            Assert(numArguments > 0);

            /*
             * We need the equal function for each DISTINCT comparison we will
             * make.
             */
            peraggstate->equalfns = (FmgrInfo*)palloc(numDistinctCols * sizeof(FmgrInfo));

            i = 0;
            foreach (lc, aggref->aggdistinct) {
                SortGroupClause* sortcl = (SortGroupClause*)lfirst(lc);

                fmgr_info(get_opcode(sortcl->eqop), &peraggstate->equalfns[i]);
                i++;
            }
            Assert(i == numDistinctCols);
        }

        ReleaseSysCache(aggTuple);
    }

    /* Update numaggs to match number of unique aggregates found */
    aggstate->numaggs = aggno + 1;

    AggWriteFileControl* TempFilePara = (AggWriteFileControl*)palloc(sizeof(AggWriteFileControl));
    TempFilePara->strategy = MEMORY_HASHAGG;
    TempFilePara->spillToDisk = false;
    TempFilePara->totalMem = workMem * 1024L;
    TempFilePara->useMem = 0;
    TempFilePara->inmemoryRownum = 0;
    TempFilePara->finishwrite = false;
    TempFilePara->runState = HASHAGG_PREPARE;
    TempFilePara->curfile = -1;
    TempFilePara->filenum = 0;
    TempFilePara->filesource = NULL;
    TempFilePara->m_hashAggSource = NULL;
    TempFilePara->maxMem = maxMem * 1024L;
    TempFilePara->spreadNum = 0;
    aggstate->aggTempFileControl = TempFilePara;
    return aggstate;
}

Datum GetAggInitVal(Datum textInitVal, Oid transtype)
{
    Oid typinput, typioparam;
    char* strInitVal = NULL;
    Datum initVal;

    getTypeInputInfo(transtype, &typinput, &typioparam);
    strInitVal = TextDatumGetCString(textInitVal);
    initVal = OidInputFunctionCall(typinput, strInitVal, typioparam, -1);
    pfree_ext(strInitVal);
    return initVal;
}

void ExecEndAgg(AggState* node)
{
    int aggno, setno;
    AggWriteFileControl* TempFileControl = (AggWriteFileControl*)node->aggTempFileControl;
    int numGroupingSets = Max(node->maxsets, 1);
    int fileNum = TempFileControl->filenum;
    hashFileSource* file = TempFileControl->filesource;

    if (file != NULL) {
        for (int i = 0; i < fileNum; i++) {
            file->close(i);
        }
        file->freeFileSource();
    }

    /*
     * Clean up sort_slot first before tuplesort_end(node->sort_in)
     * because the minimal tuple in sort_slot may point to some memory
     * in sort_in(tuplesort) when doing external sort.
     */
    (void)ExecClearTuple(node->sort_slot);

    /* Make sure we have closed any open tuplesorts */
    if (node->sort_in) {
        tuplesort_end(node->sort_in);
        node->sort_in = NULL;
    }
    if (node->sort_out) {
        tuplesort_end(node->sort_out);
        node->sort_out = NULL;
    }

    for (aggno = 0; aggno < node->numaggs; aggno++) {
        AggStatePerAgg peraggstate = &node->peragg[aggno];

        for (setno = 0; setno < numGroupingSets; setno++) {
            if (peraggstate->sortstates[setno]) {
                tuplesort_end(peraggstate->sortstates[setno]);
                peraggstate->sortstates[setno] = NULL;
            }
        }
        if (AGGKIND_IS_ORDERED_SET(peraggstate->aggref->aggkind) && node->ss.ps.ps_ExprContext != NULL) {
            /* Ensure any agg shutdown callbacks have been called */
            ReScanExprContext(node->ss.ps.ps_ExprContext);
        }
    }

    /*
     * We don't actually free any ExprContexts here (see comment in
     * ExecFreeExprContext), just unlinking the output one from the plan node
     * suffices.
     */
    ExecFreeExprContext(&node->ss.ps);

    /* clean up tuple table */
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    for (setno = 0; setno < numGroupingSets; setno++) {
        if (node->aggcontexts[setno]) {
            MemoryContextDelete(node->aggcontexts[setno]);
            node->aggcontexts[setno] = NULL;
        }
    }

    ExecEndNode(outerPlanState(node));
}

void ExecReScanAgg(AggState* node)
{
    ExprContext* econtext = node->ss.ps.ps_ExprContext;
    PlanState* outerPlan = outerPlanState(node);
    int aggno;
    AggWriteFileControl* TempFilePara = (AggWriteFileControl*)node->aggTempFileControl;
    Agg* aggnode = (Agg*)node->ss.ps.plan;
    int numGroupingSets = Max(node->maxsets, 1);
    int setno;
    errno_t rc;

    /* Already reset, just rescan lefttree */
    bool isRescan = node->ss.ps.recursive_reset && node->ss.ps.state->es_recursive_next_iteration;
    if (isRescan) {
        if (node->ss.ps.lefttree->chgParam == NULL)
            ExecReScan(node->ss.ps.lefttree);

        node->ss.ps.recursive_reset = false;
        return;
    }

    node->agg_done = false;
    node->ss.ps.ps_TupFromTlist = false;

    if (aggnode->aggstrategy == AGG_HASHED) {
        /*
         * In the hashed case, if we haven't yet built the hash table then we
         * can just return; nothing done yet, so nothing to undo. If subnode's
         * chgParam is not NULL then it will be re-scanned by ExecProcNode,
         * else no reason to re-scan it at all.
         */
        if (!node->table_filled)
            return;

        /*
         * If we do have the hash table, and the subplan does not have any
         * parameter changes, and none of our own parameter changes affect
         * input expressions of the aggregated functions, then we can just
         * rescan the existing hash table, and have not spill to disk;
         * no need to build it again.
         */
        if (node->ss.ps.lefttree->chgParam == NULL && TempFilePara->spillToDisk == false &&
            aggnode->aggParams == NULL && !EXEC_IN_RECURSIVE_MODE(node->ss.ps.plan)) {
            ResetTupleHashIterator(node->hashtable, &node->hashiter);
            return;
        }
    }

    /* Make sure we have closed any open tuplesorts */
    for (aggno = 0; aggno < node->numaggs; aggno++) {
        for (setno = 0; setno < numGroupingSets; setno++) {
            AggStatePerAgg peraggstate = &node->peragg[aggno];

            if (peraggstate->sortstates[setno]) {
                tuplesort_end(peraggstate->sortstates[setno]);
                peraggstate->sortstates[setno] = NULL;
            }
        }
    }

    /*
     * We don't need to ReScanExprContext the output tuple context here;
     * ExecReScan already did it. But we do need to reset our per-grouping-set
     * contexts, which may have transvalues stored in them. (We use rescan
     * rather than just reset because transfns may have registered callbacks
     * that need to be run now.)
     *
     * Note that with AGG_HASHED, the hash table is allocated in a sub-context
     * of the aggcontext. This used to be an issue, but now, resetting a
     * context automatically deletes sub-contexts too.
     */
    /* Release first tuple of group, if we have made a copy */
    if (node->grp_firstTuple != NULL) {
        tableam_tops_free_tuple(node->grp_firstTuple);
        node->grp_firstTuple = NULL;
    }
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /* Forget current agg values */
    rc = memset_s(econtext->ecxt_aggvalues, sizeof(Datum) * node->numaggs, 0, sizeof(Datum) * node->numaggs);
    securec_check(rc, "\0", "\0");
    rc = memset_s(econtext->ecxt_aggnulls, sizeof(bool) * node->numaggs, 0, sizeof(bool) * node->numaggs);
    securec_check(rc, "\0", "\0");

    /*
     * Release all temp storage. Note that with AGG_HASHED, the hash table is
     * allocated in a sub-context of the aggcontext. We're going to rebuild
     * the hash table from scratch, so we need to use
     * MemoryContextResetAndDeleteChildren() to avoid leaking the old hash
     * table's memory context header.
     */
    for (setno = 0; setno < numGroupingSets; setno++) {
        MemoryContextResetAndDeleteChildren(node->aggcontexts[setno]);
    }

    if (aggnode->aggstrategy == AGG_HASHED) {
        AggWriteFileControl* TempFileControl = (AggWriteFileControl*)node->aggTempFileControl;

        int fileNum = TempFileControl->filenum;
        int64 workMem = SET_NODEMEM(aggnode->plan.operatorMemKB[0], aggnode->plan.dop);
        int64 maxMem =
            (aggnode->plan.operatorMaxMem > 0) ? SET_NODEMEM(aggnode->plan.operatorMaxMem, aggnode->plan.dop) : 0;

        hashFileSource* file = TempFileControl->filesource;

        if (file != NULL) {
            for (int i = 0; i < fileNum; i++) {
                file->close(i);
            }
            file->freeFileSource();
        }

        /*
         * After close the temp file and free the filesource, setting the filesource to NULL
         * preventing free or close wrong object the next time here.
         * Problem Scenario: when the first rescan need spill to disk and second rescan
         * doesn't need, without this set will lead core in freeFileSource as m_tuple was
         * set to null in the first rescan.
         */
        TempFileControl->filesource = NULL;

        /* Rebuild an empty hash table */
        build_hash_table(node);
        node->table_filled = false;
        /* reset hashagg temp file para */
        TempFilePara->strategy = MEMORY_HASHAGG;
        TempFilePara->spillToDisk = false;
        TempFilePara->finishwrite = false;
        TempFilePara->totalMem = workMem * 1024L;
        TempFilePara->useMem = 0;
        TempFilePara->inmemoryRownum = 0;
        TempFilePara->runState = HASHAGG_PREPARE;
        TempFilePara->curfile = -1;
        TempFilePara->filenum = 0;
        TempFilePara->maxMem = maxMem * 1024L;
        TempFilePara->spreadNum = 0;
    } else {
        /*
         * Reset the per-group state (in particular, mark transvalues null)
         */
        rc = memset_s(node->pergroup,
            sizeof(AggStatePerGroupData) * node->numaggs * numGroupingSets,
            0,
            sizeof(AggStatePerGroupData) * node->numaggs * numGroupingSets);
        securec_check(rc, "", "");
        /* reset to phase 0 */
        initialize_phase(node, 0);

        node->input_done = false;
        node->projected_set = -1;
    }

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (outerPlan->chgParam == NULL)
        ExecReScan(node->ss.ps.lefttree);
}

/*
 * AggCheckCallContext - test if a SQL function is being called as an aggregate
 *
 * The transition and/or final functions of an aggregate may want to verify
 * that they are being called as aggregates, rather than as plain SQL
 * functions.  They should use this function to do so.	The return value
 * is nonzero if being called as an aggregate, or zero if not.	(Specific
 * nonzero values are AGG_CONTEXT_AGGREGATE or AGG_CONTEXT_WINDOW, but more
 * values could conceivably appear in future.)
 *
 * If aggcontext isn't NULL, the function also stores at *aggcontext the
 * identity of the memory context that aggregate transition values are being
 * stored in.  Note that the same aggregate call site (flinfo) may be called
 * interleaved on different transition values in different contexts, so it's
 * not kosher to cache aggcontext under fn_extra.  It is, however, kosher to
 * cache it in the transvalue itself (for internal-type transvalues).
 */
int AggCheckCallContext(FunctionCallInfo fcinfo, MemoryContext* aggcontext)
{
    if (fcinfo->context && IsA(fcinfo->context, AggState)) {
        if (aggcontext != NULL) {
            AggState* aggstate = ((AggState*)fcinfo->context);
            *aggcontext = ((AggState*)fcinfo->context)->aggcontexts[aggstate->current_set];
        }
        return AGG_CONTEXT_AGGREGATE;
    }
    if (fcinfo->context && IsA(fcinfo->context, WindowAggState)) {
        if (aggcontext != NULL)
            *aggcontext = ((WindowAggState*)fcinfo->context)->aggcontext;
        return AGG_CONTEXT_WINDOW;
    }

    /* this is just to prevent "uninitialized variable" warnings */
    if (aggcontext != NULL)
        *aggcontext = NULL;
    return 0;
}

/*
 * aggregate_dummy - dummy execution routine for aggregate functions
 *
 * This function is listed as the implementation (prosrc field) of pg_proc
 * entries for aggregate functions.  Its only purpose is to throw an error
 * if someone mistakenly executes such a function in the normal way.
 *
 * Perhaps someday we could assign real meaning to the prosrc field of
 * an aggregate?
 */
Datum aggregate_dummy(PG_FUNCTION_ARGS)
{
    ereport(ERROR,
        (errcode(ERRCODE_SYNTAX_ERROR),
            errmsg("aggregate function %u called as normal function", fcinfo->flinfo->fn_oid)));
    return (Datum)0; /* keep compiler quiet */
}

FORCE_INLINE void agg_spill_to_disk(AggWriteFileControl* TempFileControl, TupleHashTable hashtable,
    TupleTableSlot* hashslot, int numGroups, bool isAgg, int planId, int dop, Instrumentation* instrument)
{
    if (TempFileControl->spillToDisk == false) {
        Assert(TempFileControl->finishwrite == false);
        AllocSetContext* set = (AllocSetContext*)(hashtable->tablecxt);
        int64 totalSize = set->totalSpace;
        TempFileControl->inmemoryRownum++; /* add 1 when insert one slot to hash table */
        /* compute totalSize of AggContext and TupleHashTable */
        int64 usedSize = totalSize + TempFileControl->inmemoryRownum * hashtable->entrysize;
        bool sysBusy = gs_sysmemory_busy(usedSize * dop, false);
        /* next slot will be inserted into temp file when that useful memory more than total memory */
        if (usedSize >= TempFileControl->totalMem || sysBusy) {
            bool memSpread = false;

            if (sysBusy) {
                hashtable->causedBySysRes = sysBusy;
                TempFileControl->totalMem = usedSize;
                set->maxSpaceSize = usedSize;
                MEMCTL_LOG(LOG,
                    "%s(%d) early spilled, workmem: %ldKB, usedmem: %ldKB",
                    isAgg ? "HashAgg" : "HashSetop",
                    planId,
                    TempFileControl->totalMem / 1024L,
                    usedSize / 1024L);
            /* check if there's enough memory for memory auto spread */
            } else if (TempFileControl->maxMem > TempFileControl->totalMem) {
                TempFileControl->totalMem = usedSize;
                int64 spreadMem = Min(Min(dywlm_client_get_memory() * 1024L, TempFileControl->totalMem),
                    TempFileControl->maxMem - TempFileControl->totalMem);
                if (spreadMem > TempFileControl->totalMem * MEM_AUTO_SPREAD_MIN_RATIO) {
                    TempFileControl->totalMem += spreadMem;
                    TempFileControl->spreadNum++;
                    set->maxSpaceSize += spreadMem;
                    memSpread = true;
                    MEMCTL_LOG(DEBUG2,
                        "%s(%d) auto mem spread %ldKB succeed, and work mem is %ldKB.",
                        isAgg ? "HashAgg" : "HashSetop",
                        planId,
                        spreadMem / 1024L,
                        TempFileControl->totalMem / 1024L);
                } else {
                    MEMCTL_LOG(LOG,
                        "%s(%d) auto mem spread %ldKB failed, and work mem is %ldKB.",
                        isAgg ? "HashAgg" : "HashSetop",
                        planId,
                        spreadMem / 1024L,
                        TempFileControl->totalMem / 1024L);
                }
            }

            /* if spilling to disk, need to record info into hashtable */
            if (!memSpread) {
                if (TempFileControl->inmemoryRownum > 0)
                    hashtable->width /= TempFileControl->inmemoryRownum;
                hashtable->add_width = false;

                TempFileControl->spillToDisk = true;

                /* cache the memory size into instrument for explain performance */
                if (instrument != NULL) {
                    instrument->memoryinfo.peakOpMemory = usedSize;
                }

                /* estimate num of temp file */
                int estsize = getPower2Num(4 * numGroups / TempFileControl->inmemoryRownum);
                TempFileControl->filenum = Max(HASH_MIN_FILENUMBER, estsize);
                TempFileControl->filenum = Min(TempFileControl->filenum, HASH_MAX_FILENUMBER);
                TempFileControl->filesource =
                    New(CurrentMemoryContext) hashFileSource(hashslot, TempFileControl->filenum);

                /* increase current session spill count */
                pgstat_increase_session_spill();
            }
        }
    }
}

/*
 * @Description: Early free the memory for Aggregation.
 *
 * @param[IN] node:  executor state for Agg
 * @return: void
 */
void ExecEarlyFreeAggregation(AggState* node)
{
    int aggno, setno;
    AggWriteFileControl* TempFileControl = (AggWriteFileControl*)node->aggTempFileControl;
    int numGroupingSets = Max(node->maxsets, 1);
    int fileNum = TempFileControl->filenum;
    hashFileSource* file = TempFileControl->filesource;
    PlanState* plan_state = &node->ss.ps;

    if (plan_state->earlyFreed)
        return;

    if (file != NULL) {
        for (int i = 0; i < fileNum; i++) {
            file->close(i);
        }
        file->freeFileSource();
    }

    /*
     * Clean up sort_slot first before tuplesort_end(node->sort_in)
     * because the minimal tuple in sort_slot may point to some memory
     * in sort_in(tuplesort) when doing external sort.
     */
    (void)ExecClearTuple(node->sort_slot);

    /* Make sure we have closed any open tuplesorts */
    if (node->sort_in) {
        tuplesort_end(node->sort_in);
        node->sort_in = NULL;
    }
    if (node->sort_out) {
        tuplesort_end(node->sort_out);
        node->sort_out = NULL;
    }

    for (aggno = 0; aggno < node->numaggs; aggno++) {
        AggStatePerAgg peraggstate = &node->peragg[aggno];

        for (setno = 0; setno < numGroupingSets; setno++) {
            if (peraggstate->sortstates[setno]) {
                tuplesort_end(peraggstate->sortstates[setno]);
                peraggstate->sortstates[setno] = NULL;
            }
        }
    }
    /* Ensure any agg shutdown callbacks have been called */
    ReScanExprContext(node->ss.ps.ps_ExprContext);
    /*
     * We don't actually free any ExprContexts here (see comment in
     * ExecFreeExprContext), just unlinking the output one from the plan node
     * suffices.
     */
    ExecFreeExprContext(&node->ss.ps);

    /* clean up tuple table */
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    for (setno = 0; setno < numGroupingSets; setno++) {
        if (node->aggcontexts[setno]) {
            MemoryContextDelete(node->aggcontexts[setno]);
            node->aggcontexts[setno] = NULL;
        }
    }

    EARLY_FREE_LOG(elog(LOG,
        "Early Free: After early freeing Agg "
        "at node %d, memory used %d MB.",
        plan_state->plan->plan_node_id,
        getSessionMemoryUsageMB()));

    plan_state->earlyFreed = true;
    ExecEarlyFree(outerPlanState(node));
}

/*
 * @Function: ExecReSetAgg()
 *
 * @Brief: Reset the agg state structure in rescan case under
 * 	recursion-stream new iteration condition.
 *
 * @Input node: node agg planstate
 *
 * @Return: no return value
 */
void ExecReSetAgg(AggState* node)
{
    Assert(IS_PGXC_DATANODE && node != NULL && (IsA(node, AggState)));
    Assert(EXEC_IN_RECURSIVE_MODE(node->ss.ps.plan));

    ExprContext* econtext = node->ss.ps.ps_ExprContext;
    PlanState* outerPlan = outerPlanState(node);
    AggWriteFileControl* TempFilePara = (AggWriteFileControl*)node->aggTempFileControl;
    Agg* aggnode = (Agg*)node->ss.ps.plan;
    int numGroupingSets = Max(node->maxsets, 1);
    int aggno;
    int setno;
    errno_t rc;

    node->agg_done = false;
    node->ss.ps.ps_TupFromTlist = false;

    /* Make sure we have closed any open tuplesorts */
    for (aggno = 0; aggno < node->numaggs; aggno++) {
        for (setno = 0; setno < numGroupingSets; setno++) {
            AggStatePerAgg peraggstate = &node->peragg[aggno];

            if (peraggstate->sortstates[setno]) {
                tuplesort_end(peraggstate->sortstates[setno]);
                peraggstate->sortstates[setno] = NULL;
            }
        }
    }

    /*
     * We don't need to ReScanExprContext the output tuple context here;
     * ExecReScan already did it. But we do need to reset our per-grouping-set
     * contexts, which may have transvalues stored in them. (We use rescan
     * rather than just reset because transfns may have registered callbacks
     * that need to be run now.)
     *
     * Note that with AGG_HASHED, the hash table is allocated in a sub-context
     * of the aggcontext. This used to be an issue, but now, resetting a
     * context automatically deletes sub-contexts too.
     */
    /* Release first tuple of group, if we have made a copy */
    if (node->grp_firstTuple != NULL) {
        tableam_tops_free_tuple(node->grp_firstTuple);
        node->grp_firstTuple = NULL;
    }
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /* Forget current agg values */
    rc = memset_s(econtext->ecxt_aggvalues, sizeof(Datum) * node->numaggs, 0, sizeof(Datum) * node->numaggs);
    securec_check(rc, "\0", "\0");
    rc = memset_s(econtext->ecxt_aggnulls, sizeof(bool) * node->numaggs, 0, sizeof(bool) * node->numaggs);
    securec_check(rc, "\0", "\0");

    /*
     * Release all temp storage. Note that with AGG_HASHED, the hash table is
     * allocated in a sub-context of the aggcontext. We're going to rebuild
     * the hash table from scratch, so we need to use
     * MemoryContextResetAndDeleteChildren() to avoid leaking the old hash
     * table's memory context header.
     */
    for (setno = 0; setno < numGroupingSets; setno++) {
        MemoryContextResetAndDeleteChildren(node->aggcontexts[setno]);
    }

    if (aggnode->aggstrategy == AGG_HASHED) {
        AggWriteFileControl* TempFileControl = (AggWriteFileControl*)node->aggTempFileControl;
        int fileNum = TempFileControl->filenum;
        int64 workMem = SET_NODEMEM(aggnode->plan.operatorMemKB[0], aggnode->plan.dop);
        int64 maxMem =
            (aggnode->plan.operatorMaxMem > 0) ? SET_NODEMEM(aggnode->plan.operatorMaxMem, aggnode->plan.dop) : 0;
        hashFileSource* file = TempFileControl->filesource;

        if (file != NULL) {
            for (int i = 0; i < fileNum; i++) {
                file->close(i);
            }
            file->freeFileSource();
        }

        /*
         * After close the temp file and free the filesource, setting the filesource to NULL
         * preventing free or close wrong object the next time here.
         * Problem Scenario: when the first rescan need spill to disk and second rescan
         * doesn't need, without this set will lead core in freeFileSource as m_tuple was
         * set to null in the first rescan.
         */
        TempFileControl->filesource = NULL;

        /* Rebuild an empty hash table */
        build_hash_table(node);
        node->table_filled = false;
        /* reset hashagg temp file para */
        TempFilePara->strategy = MEMORY_HASHAGG;
        TempFilePara->spillToDisk = false;
        TempFilePara->finishwrite = false;
        TempFilePara->totalMem = workMem * 1024L;
        TempFilePara->useMem = 0;
        TempFilePara->inmemoryRownum = 0;
        TempFilePara->runState = HASHAGG_PREPARE;
        TempFilePara->curfile = -1;
        TempFilePara->filenum = 0;
        TempFilePara->maxMem = maxMem * 1024L;
        TempFilePara->spreadNum = 0;
    } else {
        /*
         * Reset the per-group state (in particular, mark transvalues null)
         */
        rc = memset_s(node->pergroup,
            sizeof(AggStatePerGroupData) * node->numaggs * numGroupingSets,
            0,
            sizeof(AggStatePerGroupData) * node->numaggs * numGroupingSets);
        securec_check(rc, "\0", "\0");

        /* reset to phase 0 */
        initialize_phase(node, 0);

        node->input_done = false;
        node->projected_set = -1;
    }

    node->ss.ps.recursive_reset = true;

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (outerPlan->chgParam == NULL)
        ExecReSetRecursivePlanTree(outerPlanState(node));
}
