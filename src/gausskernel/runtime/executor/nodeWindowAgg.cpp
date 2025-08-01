/* -------------------------------------------------------------------------
 *
 * nodeWindowAgg.cpp
 *	  routines to handle WindowAgg nodes.
 *
 * A WindowAgg node evaluates "window functions" across suitable partitions
 * of the input tuple set.	Any one WindowAgg works for just a single window
 * specification, though it can evaluate multiple window functions sharing
 * identical window specifications.  The input tuples are required to be
 * delivered in sorted order, with the PARTITION BY columns (if any) as
 * major sort keys and the ORDER BY columns (if any) as minor sort keys.
 * (The planner generates a stack of WindowAggs with intervening Sort nodes
 * as needed, if a query involves more than one window specification.)
 *
 * Since window functions can require access to any or all of the rows in
 * the current partition, we accumulate rows of the partition into a
 * tuplestore.	The window functions are called using the WindowObject API
 * so that they can access those rows as needed.
 *
 * We also support using plain aggregate functions as window functions.
 * For these, the regular Agg-node environment is emulated for each partition.
 * As required by the SQL spec, the output represents the value of the
 * aggregate function over all rows in the current row's window frame.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeWindowAgg.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/node/nodeWindowAgg.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/sortsupport_gs.h"
#include "windowapi.h"
#include "executor/node/nodeAgg.h"

static TupleTableSlot* ExecWindowAgg(PlanState* state);
static void initialize_windowaggregate(
    WindowAggState* winstate, WindowStatePerFunc perfuncstate, WindowStatePerAgg peraggstate);
static void advance_windowaggregate(
    WindowAggState* winstate, WindowStatePerFunc perfuncstate, WindowStatePerAgg peraggstate);
static void finalize_windowaggregate(WindowAggState* winstate, WindowStatePerFunc perfuncstate,
    WindowStatePerAgg peraggstate, Datum* result, bool* is_null);

static void eval_windowaggregates(WindowAggState* winstate);
static void eval_windowfunction(WindowAggState* winstate, WindowStatePerFunc perfuncstate, Datum* result, bool* is_null);

static void begin_partition(WindowAggState* winstate);
static void spool_tuples(WindowAggState* winstate, int64 pos);
static void release_partition(WindowAggState* winstate);

static bool row_is_in_frame(WindowAggState* winstate, int64 pos, TupleTableSlot* slot);
static void update_frameheadpos(WindowObject winobj, TupleTableSlot* slot);
static void update_frametailpos(WindowObject winobj, TupleTableSlot* slot);

WindowStatePerAggData* initialize_peragg(WindowAggState* winstate, WindowFunc* wfunc, WindowStatePerAgg peraggstate);
static Datum get_agg_init_val(Datum text_init_val, Oid transtype);

static bool are_peers(WindowAggState* winstate, TupleTableSlot* slot1, TupleTableSlot* slot2);
static bool window_gettupleslot(WindowObject winobj, int64 pos, TupleTableSlot* slot);


int64 truncate_int64_overflow(int128 arg)
{
    if (arg > (int128)INT64_MAX) {
        return INT64_MAX;
    }
    if (arg < (int128)INT64_MIN) {
        return INT64_MIN;
    }
    return (int64)arg;
}

/*
 * initialize_windowaggregate
 * parallel to initialize_aggregates in nodeAgg.c
 */
static void initialize_windowaggregate(
    WindowAggState* winstate, WindowStatePerFunc perfuncstate, WindowStatePerAgg peraggstate)
{
    MemoryContext old_context;

    if (peraggstate->initValueIsNull)
        peraggstate->transValue = peraggstate->initValue;
    else {
        old_context = MemoryContextSwitchTo(winstate->aggcontext);
        peraggstate->transValue =
            datumCopy(peraggstate->initValue, peraggstate->transtypeByVal, peraggstate->transtypeLen);
        MemoryContextSwitchTo(old_context);
    }
    peraggstate->transValueIsNull = peraggstate->initValueIsNull;
    peraggstate->noTransValue = peraggstate->initValueIsNull;
    peraggstate->resultValueIsNull = true;
    peraggstate->keep_init = false;
}

void updateAggregateState(WindowStatePerAgg peraggstate, Datum *values, bool *isnulls)
{
    for (int i = 0; i < peraggstate->numKeepCols; i++) {
        KeepRank *keep_data = &peraggstate->keepRank[i];
        
        if (!keep_data->keeptypeByVal && DatumGetPointer(values[i]) != DatumGetPointer(keep_data->keepValue)
            && !keep_data->keepValueIsNull) {
            pfree(DatumGetPointer(keep_data->keepValue));
        }

        keep_data->keepValue = values[i];
        keep_data->keepValueIsNull = isnulls[i];
    }

    if (!peraggstate->transValueIsNull && !peraggstate->transtypeByVal) {
        pfree(DatumGetPointer(peraggstate->transValue));
    }
}
void initialize_peraggstate(WindowStatePerAgg peraggstate)
{
    if (peraggstate->initValueIsNull) {
        peraggstate->transValue = peraggstate->initValue;
    } else {
        peraggstate->transValue =
            datumCopy(peraggstate->initValue, peraggstate->transtypeByVal, peraggstate->transtypeLen);
    }
    
    peraggstate->transValueIsNull = peraggstate->initValueIsNull;
    peraggstate->noTransValue = peraggstate->initValueIsNull;
}

void checkAndUpdateNeedAggregate(
    int* needAgregate, WindowStatePerAgg peraggstate, KeepRank* rank_data, Datum curValue, bool curIsNull)
{
    if (rank_data->keepSort.abbrev_converter)
        *needAgregate = ApplySortAbbrevFullComparator(rank_data->keepValue,
            rank_data->keepValueIsNull, curValue, curIsNull, &rank_data->keepSort);
    else
        *needAgregate = ApplySortComparator(rank_data->keepValue,
                                            rank_data->keepValueIsNull, curValue, curIsNull, &rank_data->keepSort);
}
static int keep_rank_windowaggregate(
    WindowAggState* winstate, WindowStatePerAgg peraggstate, WindowFuncExprState* wfuncstate)
{
    int     i = 0;
    Datum   curValue;
    bool    curIsNull;
    int    needAgregate = 0;
    ListCell*       lc;
    ExprContext*    econtext = winstate->tmpcontext;

    if (peraggstate->numKeepCols <= 0) {
        return 0;
    }
    Datum* values = (Datum*) palloc0(sizeof(Datum) * peraggstate->numKeepCols);
    bool* isnulls = (bool*) palloc0(sizeof(bool) * peraggstate->numKeepCols);
    MemoryContext old_context = MemoryContextSwitchTo(winstate->aggcontext);
    /* copy value */
    foreach(lc, wfuncstate->keep_args)
    {
        ExprState* arg_state = (ExprState*)lfirst(lc);
        KeepRank* rank_data = &peraggstate->keepRank[i];
        curValue = ExecEvalExpr(arg_state, econtext, &curIsNull, NULL);
        values[i] = datumCopy(curValue, rank_data->keeptypeByVal, rank_data->keeptypeLen);
        isnulls[i] = curIsNull;
        if (needAgregate == 0 && peraggstate->keep_init) {
            checkAndUpdateNeedAggregate(&needAgregate, peraggstate, rank_data, curValue, curIsNull);
        }
        i++;
    }
    MemoryContextSwitchTo(old_context);
    if (!peraggstate->keep_init) {
        for (i = 0; i <peraggstate->numKeepCols; i++) {
            KeepRank* keep_data = &peraggstate->keepRank[i];
            /* set keep value */
            keep_data->keepValue = values[i];
            keep_data->keepValueIsNull = isnulls[i];
        }
        peraggstate->keep_init = true;
    }
    if (needAgregate == 0) {
        pfree(values);
        pfree(isnulls);
        return 0;
    }
    /*
     * keep value < current value, dense_rank last, replace keep value
     * keep value > current value, denase_rank first, replace keep value
     */
    if ((needAgregate < 0) != wfuncstate->keep_first) {
        updateAggregateState(peraggstate, values, isnulls);
        /* reset transValue */
        old_context = MemoryContextSwitchTo(winstate->aggcontext);
        initialize_peraggstate(peraggstate);
        MemoryContextSwitchTo(old_context);
        needAgregate = 0;
    } else {
        needAgregate = 1;
    }
    pfree(values);
    pfree(isnulls);
    return needAgregate;
}

/*
 * advance_windowaggregate
 * parallel to advance_aggregates in nodeAgg.c
 */
static void advance_windowaggregate(
    WindowAggState* winstate, WindowStatePerFunc perfuncstate, WindowStatePerAgg peraggstate)
{
    WindowFuncExprState* wfuncstate = perfuncstate->wfuncstate;
    int num_arguments = perfuncstate->numArguments;
    FunctionCallInfoData fcinfodata;
    FunctionCallInfo fcinfo = &fcinfodata;
    Datum new_val;
    ListCell* arg = NULL;
    int i;
    MemoryContext old_context;
    ExprContext* econtext = winstate->tmpcontext;

    if (keep_rank_windowaggregate(winstate, peraggstate, wfuncstate)) {
        return;
    }

    old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    /* init the number of arguments to a function. */
    InitFunctionCallInfoArgs(*fcinfo, num_arguments + 1, 1);

    /* We start from 1, since the 0th arg will be the transition value */
    i = 1;
    foreach (arg, wfuncstate->args) {
        ExprState* arg_state = (ExprState*)lfirst(arg);

        fcinfo->arg[i] = ExecEvalExpr(arg_state, econtext, &fcinfo->argnull[i]);
        fcinfo->argTypes[i] = arg_state->resultType;
        i++;
    }

    if (peraggstate->transfn.fn_strict) {
        /*
         * For a strict transfn, nothing happens when there's a NULL input; we
         * just keep the prior transValue.
         */
        for (i = 1; i <= num_arguments; i++) {
            Oid aggtranstype = perfuncstate->wfunc->wintype;
            ListCell* arg = list_head(perfuncstate->wfunc->args);
            TargetEntry *tle = (TargetEntry *)lfirst(arg);
            if (fcinfo->argnull[i] && strcmp(get_func_name(perfuncstate->wfunc->winfnoid), "bit_and") == 0 &&
                is_binary_type_in_dolphin(aggtranstype) &&
                peraggstate->transValueIsNull && IsA(tle, Var)) {
                MemoryContextSwitchTo(old_context);
                Var* var = (Var*)tle;
                peraggstate->transValue = get_bit_and_initval(aggtranstype, var->vartypmod);
                peraggstate->transValueIsNull = false;
                return;
            } else if (fcinfo->argnull[i]) {
                MemoryContextSwitchTo(old_context);
                return;
            }
        }
        if (peraggstate->noTransValue) {
            /*
             * transValue has not been initialized. This is the first non-NULL
             * input value. We use it as the initial value for transValue. (We
             * already checked that the agg's input type is binary-compatible
             * with its transtype, so straight copy here is OK.)
             *
             * We must copy the datum into aggcontext if it is pass-by-ref. We
             * do not need to pfree the old transValue, since it's NULL.
             */
            MemoryContextSwitchTo(winstate->aggcontext);
            peraggstate->transValue = datumCopy(fcinfo->arg[1], peraggstate->transtypeByVal, peraggstate->transtypeLen);
            peraggstate->transValueIsNull = false;
            peraggstate->noTransValue = false;
            MemoryContextSwitchTo(old_context);
            return;
        }
        if (peraggstate->transValueIsNull) {
            /*
             * Don't call a strict function with NULL inputs.  Note it is
             * possible to get here despite the above tests, if the transfn is
             * strict *and* returned a NULL on a prior cycle. If that happens
             * we will propagate the NULL all the way to the end.
             */
            MemoryContextSwitchTo(old_context);
            return;
        }
    }

    /*
     * OK to call the transition function
     */
    InitFunctionCallInfoData(
        *fcinfo, &(peraggstate->transfn), num_arguments + 1, perfuncstate->winCollation, (Node*)winstate, NULL);
    fcinfo->arg[0] = peraggstate->transValue;
    fcinfo->argnull[0] = peraggstate->transValueIsNull;
    fcinfo->argTypes[0] = InvalidOid;
    new_val = FunctionCallInvoke(fcinfo);
    /*
     * If pass-by-ref datatype, must copy the new value into aggcontext and
     * pfree the prior transValue.	But if transfn returned a pointer to its
     * first input, we don't need to do anything.
     */
    if (!peraggstate->transtypeByVal && DatumGetPointer(new_val) != DatumGetPointer(peraggstate->transValue)) {
        if (!fcinfo->isnull) {
            MemoryContextSwitchTo(winstate->aggcontext);
            new_val = datumCopy(new_val, peraggstate->transtypeByVal, peraggstate->transtypeLen);
        }
        if (!peraggstate->transValueIsNull)
            pfree(DatumGetPointer(peraggstate->transValue));
    }

    MemoryContextSwitchTo(old_context);
    peraggstate->transValue = new_val;
    peraggstate->transValueIsNull = fcinfo->isnull;
}

/*
 * finalize_windowaggregate
 * parallel to finalize_aggregate in nodeAgg.c
 */
static void finalize_windowaggregate(WindowAggState* winstate, WindowStatePerFunc perfuncstate,
    WindowStatePerAgg peraggstate, Datum* result, bool* is_null)
{
    MemoryContext old_context;

    old_context = MemoryContextSwitchTo(winstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

    /*
     * Apply the agg's finalfn if one is provided, else return transValue.
     */
    if (OidIsValid(peraggstate->finalfn_oid)) {
        FunctionCallInfoData fcinfo;

        InitFunctionCallInfoData(fcinfo, &(peraggstate->finalfn), 1, perfuncstate->winCollation, (Node*)winstate, NULL);
        fcinfo.arg[0] = peraggstate->transValue;
        fcinfo.argnull[0] = peraggstate->transValueIsNull;
        fcinfo.argTypes[0] = InvalidOid;
        if (fcinfo.flinfo->fn_strict && peraggstate->transValueIsNull) {
            /* don't call a strict function with NULL inputs */
            *result = (Datum)0;
            *is_null = true;
        } else {
            *result = FunctionCallInvoke(&fcinfo);
            *is_null = fcinfo.isnull;
        }
    } else {
        *result = peraggstate->transValue;
        *is_null = peraggstate->transValueIsNull;
    }

    /*
     * If result is pass-by-ref, make sure it is in the right context.
     */
    if (!peraggstate->resulttypeByVal && !*is_null &&
        !MemoryContextContains(CurrentMemoryContext, DatumGetPointer(*result)))
        *result = datumCopy(*result, peraggstate->resulttypeByVal, peraggstate->resulttypeLen);
    MemoryContextSwitchTo(old_context);
}

/*
 * eval_windowaggregates
 * evaluate plain aggregates being used as window functions
 *
 * Much of this is duplicated from nodeAgg.c.  But NOTE that we expect to be
 * able to call aggregate final functions repeatedly after aggregating more
 * data onto the same transition value.  This is not a behavior required by
 * nodeAgg.c.
 */
static void eval_windowaggregates(WindowAggState* winstate)
{
    WindowStatePerAgg peraggstate;
    int wfuncno, num_aggs;
    int i;
    MemoryContext old_context;
    ExprContext* econtext = NULL;
    WindowObject agg_winobj;
    TupleTableSlot* agg_row_slot = NULL;

    num_aggs = winstate->numaggs;
    if (num_aggs == 0) {
        return; /* nothing to do */
    }
    /* final output execution is in ps_ExprContext */
    econtext = winstate->ss.ps.ps_ExprContext;
    agg_winobj = winstate->agg_winobj;
    agg_row_slot = winstate->agg_row_slot;

    /*
     * Currently, we support only a subset of the SQL-standard window framing
     * rules.
     *
     * If the frame start is UNBOUNDED_PRECEDING, the window frame consists of
     * a contiguous group of rows extending forward from the start of the
     * partition, and rows only enter the frame, never exit it, as the current
     * row advances forward.  This makes it possible to use an incremental
     * strategy for evaluating aggregates: we run the transition function for
     * each row added to the frame, and run the final function whenever we
     * need the current aggregate value.  This is considerably more efficient
     * than the naive approach of re-running the entire aggregate calculation
     * for each current row.  It does assume that the final function doesn't
     * damage the running transition value, but we have the same assumption in
     * nodeAgg.c too (when it rescans an existing hash table).
     *
     * For other frame start rules, we discard the aggregate state and re-run
     * the aggregates whenever the frame head row moves.  We can still
     * optimize as above whenever successive rows share the same frame head.
     *
     * In many common cases, multiple rows share the same frame and hence the
     * same aggregate value. (In particular, if there's no ORDER BY in a RANGE
     * window, then all rows are peers and so they all have window frame equal
     * to the whole partition.)  We optimize such cases by calculating the
     * aggregate value once when we reach the first row of a peer group, and
     * then returning the saved value for all subsequent rows.
     *
     * 'aggregatedupto' keeps track of the first row that has not yet been
     * accumulated into the aggregate transition values.  Whenever we start a
     * new peer group, we accumulate forward to the end of the peer group.
     *
     * Rerunning aggregates from the frame start can be pretty slow. For
     * some aggregates like SUM and COUNT we could avoid that by implementing
     * a "negative transition function" that would be called for each row as
     * it exits the frame.	We'd have to think about avoiding recalculation of
     * volatile arguments of aggregate functions, too.
     */
    /*
     * First, update the frame head position.
     */
    update_frameheadpos(agg_winobj, winstate->temp_slot_1);

    /*
     * Initialize aggregates on first call for partition, or if the frame head
     * position moved since last time.
     */
    if (winstate->currentpos == 0 || winstate->frameheadpos != winstate->aggregatedbase) {
        /*
         * Discard transient aggregate values
         */
        MemoryContextResetAndDeleteChildren(winstate->aggcontext);

        for (i = 0; i < num_aggs; i++) {
            peraggstate = &winstate->peragg[i];
            wfuncno = peraggstate->wfuncno;
            initialize_windowaggregate(winstate, &winstate->perfunc[wfuncno], peraggstate);
        }

        /*
         * If we created a mark pointer for aggregates, keep it pushed up to
         * frame head, so that tuplestore can discard unnecessary rows.
         */
        if (agg_winobj->markptr >= 0)
            WinSetMarkPosition(agg_winobj, winstate->frameheadpos);

        /*
         * Initialize for loop below
         */
        (void)ExecClearTuple(agg_row_slot);
        winstate->aggregatedbase = winstate->frameheadpos;
        winstate->aggregatedupto = winstate->frameheadpos;
    }

    /*
     * In UNBOUNDED_FOLLOWING mode, we don't have to recalculate aggregates
     * except when the frame head moves.  In END_CURRENT_ROW mode, we only
     * have to recalculate when the frame head moves or currentpos has
     * advanced past the place we'd aggregated up to.  Check for these cases
     * and if so, reuse the saved result values.
     */
    if ((winstate->frameOptions & (FRAMEOPTION_END_UNBOUNDED_FOLLOWING | FRAMEOPTION_END_CURRENT_ROW)) &&
        winstate->aggregatedbase <= winstate->currentpos && winstate->aggregatedupto > winstate->currentpos) {
        for (i = 0; i < num_aggs; i++) {
            peraggstate = &winstate->peragg[i];
            wfuncno = peraggstate->wfuncno;
            econtext->ecxt_aggvalues[wfuncno] = peraggstate->resultValue;
            econtext->ecxt_aggnulls[wfuncno] = peraggstate->resultValueIsNull;
        }
        return;
    }

    /*
     * Advance until we reach a row not in frame (or end of partition).
     *
     * Note the loop invariant: agg_row_slot is either empty or holds the row
     * at position aggregatedupto.	We advance aggregatedupto after processing
     * a row.
     */
    for (;;) {
        /* Fetch next row if we didn't already */
        if (TupIsNull(agg_row_slot)) {
            if (!window_gettupleslot(agg_winobj, winstate->aggregatedupto, agg_row_slot))
                break; /* must be end of partition */
        }

        /* Exit loop (for now) if not in frame */
        if (!row_is_in_frame(winstate, winstate->aggregatedupto, agg_row_slot))
            break;

        /* Set tuple context for evaluation of aggregate arguments */
        winstate->tmpcontext->ecxt_outertuple = agg_row_slot;

        /* Accumulate row into the aggregates */
        for (i = 0; i < num_aggs; i++) {
            peraggstate = &winstate->peragg[i];
            wfuncno = peraggstate->wfuncno;
            advance_windowaggregate(winstate, &winstate->perfunc[wfuncno], peraggstate);
        }

        /* Reset per-input-tuple context after each tuple */
        ResetExprContext(winstate->tmpcontext);

        /* And advance the aggregated-row state */
        winstate->aggregatedupto++;
        (void)ExecClearTuple(agg_row_slot);
    }

    /*
     * finalize aggregates and fill result/isnull fields.
     */
    for (i = 0; i < num_aggs; i++) {
        Datum* result = NULL;
        bool* is_null = NULL;

        peraggstate = &winstate->peragg[i];
        wfuncno = peraggstate->wfuncno;
        result = &econtext->ecxt_aggvalues[wfuncno];
        is_null = &econtext->ecxt_aggnulls[wfuncno];
        finalize_windowaggregate(winstate, &winstate->perfunc[wfuncno], peraggstate, result, is_null);

        /*
         * save the result in case next row shares the same frame.
         *
         * XXX in some framing modes, eg ROWS/END_CURRENT_ROW, we can know in
         * advance that the next row can't possibly share the same frame. Is
         * it worth detecting that and skipping this code?
         */
        if (!peraggstate->resulttypeByVal) {
            /*
             * clear old resultValue in order not to leak memory.  (Note: the
             * new result can't possibly be the same datum as old resultValue,
             * because we never passed it to the trans function.)
             */
            if (!peraggstate->resultValueIsNull)
                pfree(DatumGetPointer(peraggstate->resultValue));

            /*
             * If pass-by-ref, copy it into our aggregate context.
             */
            if (!*is_null) {
                old_context = MemoryContextSwitchTo(winstate->aggcontext);
                peraggstate->resultValue = datumCopy(*result, peraggstate->resulttypeByVal, peraggstate->resulttypeLen);
                MemoryContextSwitchTo(old_context);
            }
        } else {
            peraggstate->resultValue = *result;
        }
        peraggstate->resultValueIsNull = *is_null;
    }
}

/*
 * eval_windowfunction
 *
 * Arguments of window functions are not evaluated here, because a window
 * function can need random access to arbitrary rows in the partition.
 * The window function uses the special WinGetFuncArgInPartition and
 * WinGetFuncArgInFrame functions to evaluate the arguments for the rows
 * it wants.
 */
static void eval_windowfunction(WindowAggState* winstate, WindowStatePerFunc perfuncstate, Datum* result, bool* is_null)
{
    FunctionCallInfoData fcinfo;
    MemoryContext old_context;
    errno_t errorno = EOK;

    old_context = MemoryContextSwitchTo(winstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

    /*
     * We don't pass any normal arguments to a window function, but we do pass
     * it the number of arguments, in order to permit window function
     * implementations to support varying numbers of arguments.  The real info
     * goes through the WindowObject, which is passed via fcinfo->context.
     */
    InitFunctionCallInfoData(fcinfo,
        &(perfuncstate->flinfo),
        perfuncstate->numArguments,
        perfuncstate->winCollation,
        (Node*)perfuncstate->winobj,
        NULL);
    /* Just in case, make all the regular argument slots be null */
    errorno = memset_s(fcinfo.argnull, perfuncstate->numArguments, true, perfuncstate->numArguments);
    securec_check(errorno, "\0", "\0");

    *result = FunctionCallInvoke(&fcinfo);
    *is_null = fcinfo.isnull;

    /*
     * Make sure pass-by-ref data is allocated in the appropriate context. (We
     * need this in case the function returns a pointer into some short-lived
     * tuple, as is entirely possible.)
     */
    if (!perfuncstate->resulttypeByVal && !fcinfo.isnull &&
        !MemoryContextContains(CurrentMemoryContext, DatumGetPointer(*result)))
        *result = datumCopy(*result, perfuncstate->resulttypeByVal, perfuncstate->resulttypeLen);

    MemoryContextSwitchTo(old_context);
}

/*
 * begin_partition
 * Start buffering rows of the next partition.
 */
static void begin_partition(WindowAggState* winstate)
{
    PlanState* outer_plan = outerPlanState(winstate);
    int numfuncs = winstate->numfuncs;
    int i;
    Plan* plan = winstate->ss.ps.plan;

    winstate->partition_spooled = false;
    winstate->framehead_valid = false;
    winstate->frametail_valid = false;
    winstate->spooled_rows = 0;
    winstate->currentpos = 0;
    winstate->frameheadpos = 0;
    winstate->frametailpos = -1;
    (void)ExecClearTuple(winstate->agg_row_slot);

    int64 operator_mem = SET_NODEMEM(plan->operatorMemKB[0], plan->dop);
    int64 max_mem = (plan->operatorMaxMem > 0) ? SET_NODEMEM(plan->operatorMaxMem, plan->dop) : 0;

    /*
     * If this is the very first partition, we need to fetch the first input
     * row to store in first_part_slot.
     */
    if (TupIsNull(winstate->first_part_slot)) {
        TupleTableSlot* outerslot = ExecProcNode(outer_plan);

        if (!TupIsNull(outerslot))
            ExecCopySlot(winstate->first_part_slot, outerslot);
        else {
            /* outer plan is empty, so we have nothing to do */
            winstate->partition_spooled = true;
            winstate->more_partitions = false;
            return;
        }
    }

    /* Create new tuplestore for this partition */
    winstate->buffer =
        tuplestore_begin_heap(false, false, operator_mem, max_mem, plan->plan_node_id, SET_DOP(plan->dop), true);

    /*
     * Set up read pointers for the tuplestore.  The current pointer doesn't
     * need BACKWARD capability, but the per-window-function read pointers do,
     * and the aggregate pointer does if frame start is movable.
     */
    winstate->current_ptr = 0; /* read pointer 0 is pre-allocated */

    /* reset default REWIND capability bit for current ptr */
    tuplestore_set_eflags(winstate->buffer, 0);

    /* create read pointers for aggregates, if needed */
    if (winstate->numaggs > 0) {
        WindowObject agg_winobj = winstate->agg_winobj;
        int readptr_flags = 0;

        /* If the frame head is potentially movable ... */
        if (!(winstate->frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING)) {
            /* ... create a mark pointer to track the frame head */
            agg_winobj->markptr = tuplestore_alloc_read_pointer(winstate->buffer, 0);
            /* and the read pointer will need BACKWARD capability */
            readptr_flags |= EXEC_FLAG_BACKWARD;
        }

        agg_winobj->readptr = tuplestore_alloc_read_pointer(winstate->buffer, readptr_flags);
        agg_winobj->markpos = -1;
        agg_winobj->seekpos = -1;

        /* Also reset the row counters for aggregates */
        winstate->aggregatedbase = 0;
        winstate->aggregatedupto = 0;
    }

    /* create mark and read pointers for each real window function */
    for (i = 0; i < numfuncs; i++) {
        WindowStatePerFunc perfuncstate = &(winstate->perfunc[i]);

        if (!perfuncstate->plain_agg) {
            WindowObject winobj = perfuncstate->winobj;

            winobj->markptr = tuplestore_alloc_read_pointer(winstate->buffer, 0);
            winobj->readptr = tuplestore_alloc_read_pointer(winstate->buffer, EXEC_FLAG_BACKWARD);
            winobj->markpos = -1;
            winobj->seekpos = -1;
        }
    }

    /*
     * Store the first tuple into the tuplestore (it's always available now;
     * we either read it above, or saved it at the end of previous partition)
     */
    tuplestore_puttupleslot(winstate->buffer, winstate->first_part_slot);
    winstate->spooled_rows++;
}

/*
 * Read tuples from the outer node, up to and including position 'pos', and
 * store them into the tuplestore. If pos is -1, reads the whole partition.
 */
static void spool_tuples(WindowAggState* winstate, int64 pos)
{
    WindowAgg* node = (WindowAgg*)winstate->ss.ps.plan;
    PlanState* outer_plan = NULL;
    TupleTableSlot* outer_slot = NULL;
    MemoryContext old_context;

    if (!winstate->buffer)
        return; /* just a safety check */
    if (winstate->partition_spooled)
        return; /* whole partition done already */

    /*
     * If the tuplestore has spilled to disk, alternate reading and writing
     * becomes quite expensive due to frequent buffer flushes.	It's cheaper
     * to force the entire partition to get spooled in one go.
     */
    if (!tuplestore_in_memory(winstate->buffer))
        pos = -1;

    outer_plan = outerPlanState(winstate);

    /* Must be in query context to call outerplan */
    old_context = MemoryContextSwitchTo(winstate->ss.ps.ps_ExprContext->ecxt_per_query_memory);

    while (winstate->spooled_rows <= pos || pos == -1) {
        outer_slot = ExecProcNode(outer_plan);
        if (TupIsNull(outer_slot)) {
            /* reached the end of the last partition */
            winstate->partition_spooled = true;
            winstate->more_partitions = false;
            break;
        }

        if (node->partNumCols > 0) {
            /* Check if this tuple still belongs to the current partition */
            if (!execTuplesMatch(winstate->first_part_slot, outer_slot, node->partNumCols, node->partColIdx,
                winstate->partEqfunctions, winstate->tmpcontext->ecxt_per_tuple_memory, node->part_collations)) {
                /*
                 * end of partition; copy the tuple for the next cycle.
                 */
                ExecCopySlot(winstate->first_part_slot, outer_slot);
                winstate->partition_spooled = true;
                winstate->more_partitions = true;
                break;
            }
        }

        /* Still in partition, so save it into the tuplestore */
        tuplestore_puttupleslot(winstate->buffer, outer_slot);
        winstate->spooled_rows++;
    }

    MemoryContextSwitchTo(old_context);
}

/*
 * release_partition
 * clear information kept within a partition, including
 * tuplestore and aggregate results.
 */
static void release_partition(WindowAggState* winstate)
{
    int i;

    for (i = 0; i < winstate->numfuncs; i++) {
        WindowStatePerFunc perfuncstate = &(winstate->perfunc[i]);

        /* Release any partition-local state of this window function */
        if (perfuncstate->winobj)
            perfuncstate->winobj->localmem = NULL;
    }

    /*
     * Release all partition-local memory (in particular, any partition-local
     * state that we might have trashed our pointers to in the above loop, and
     * any aggregate temp data).  We don't rely on retail pfree because some
     * aggregates might have allocated data we don't have direct pointers to.
     */
    MemoryContextResetAndDeleteChildren(winstate->partcontext);
    MemoryContextResetAndDeleteChildren(winstate->aggcontext);

    if (winstate->buffer)
        tuplestore_end(winstate->buffer);
    winstate->buffer = NULL;
    winstate->partition_spooled = false;
}

/*
 * row_is_in_frame
 * Determine whether a row is in the current row's window frame according
 * to our window framing rule
 *
 * The caller must have already determined that the row is in the partition
 * and fetched it into a slot.	This function just encapsulates the framing
 * rules.
 */
static bool row_is_in_frame(WindowAggState* winstate, int64 pos, TupleTableSlot* slot)
{
    int frame_options = winstate->frameOptions;

    Assert(pos >= 0); /* else caller error */

    /* First, check frame starting conditions */
    if (frame_options & FRAMEOPTION_START_CURRENT_ROW) {
        if (frame_options & FRAMEOPTION_ROWS) {
            /* rows before current row are out of frame */
            if (pos < winstate->currentpos)
                return false;
        } else if (frame_options & FRAMEOPTION_RANGE) {
            /* preceding row that is not peer is out of frame */
            if (pos < winstate->currentpos && !are_peers(winstate, slot, winstate->ss.ss_ScanTupleSlot))
                return false;
        } else
            Assert(false);
    } else if (frame_options & FRAMEOPTION_START_VALUE) {
        if (frame_options & FRAMEOPTION_ROWS) {
            int64 offset = DatumGetInt64(winstate->startOffsetValue);

            /* rows before current row + offset are out of frame */
            if (frame_options & FRAMEOPTION_START_VALUE_PRECEDING)
                offset = -offset;

            if (pos < winstate->currentpos + offset)
                return false;
        } else if (frame_options & FRAMEOPTION_RANGE) {
            /* parser should have rejected this */
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_EXECUTOR),
                    errmsg("window frame with value offset is not implemented")));
        } else
            Assert(false);
    }

    /* Okay so far, now check frame ending conditions */
    if (frame_options & FRAMEOPTION_END_CURRENT_ROW) {
        if (frame_options & FRAMEOPTION_ROWS) {
            /* rows after current row are out of frame */
            if (pos > winstate->currentpos)
                return false;
        } else if (frame_options & FRAMEOPTION_RANGE) {
            /* following row that is not peer is out of frame */
            if (pos > winstate->currentpos && !are_peers(winstate, slot, winstate->ss.ss_ScanTupleSlot))
                return false;
        } else
            Assert(false);
    } else if (frame_options & FRAMEOPTION_END_VALUE) {
        if (frame_options & FRAMEOPTION_ROWS) {
            int64 offset = DatumGetInt64(winstate->endOffsetValue);

            /* rows after current row + offset are out of frame */
            if (frame_options & FRAMEOPTION_END_VALUE_PRECEDING)
                offset = -offset;

            if (pos > winstate->currentpos + offset)
                return false;
        } else if (frame_options & FRAMEOPTION_RANGE) {
            /* parser should have rejected this */
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_EXECUTOR),
                    errmsg("window frame with value offset is not implemented")));
        } else
            Assert(false);
    }

    /* If we get here, it's in frame */
    return true;
}

/*
 * update_frameheadpos
 * make frameheadpos valid for the current row
 *
 * Uses the winobj's read pointer for any required fetches; hence, if the
 * frame mode is one that requires row comparisons, the winobj's mark must
 * not be past the currently known frame head.	Also uses the specified slot
 * for any required fetches.
 */
static void update_frameheadpos(WindowObject winobj, TupleTableSlot* slot)
{
    WindowAggState* winstate = winobj->winstate;
    WindowAgg* node = (WindowAgg*)winstate->ss.ps.plan;
    int frame_options = winstate->frameOptions;

    if (winstate->framehead_valid)
        return; /* already known for current row */

    bool isFrameHead = frame_options & FRAMEOPTION_START_UNBOUNDED_PRECEDING;
    if (isFrameHead) {
        /* In UNBOUNDED PRECEDING mode, frame head is always row 0 */
        winstate->frameheadpos = 0;
        winstate->framehead_valid = true;
    } else if (frame_options & FRAMEOPTION_START_CURRENT_ROW) {
        if (frame_options & FRAMEOPTION_ROWS) {
            /* In ROWS mode, frame head is the same as current */
            winstate->frameheadpos = winstate->currentpos;
            winstate->framehead_valid = true;
        } else if (frame_options & FRAMEOPTION_RANGE) {
            int64 fhprev;

            /* If no ORDER BY, all rows are peers with each other */
            if (node->ordNumCols == 0) {
                winstate->frameheadpos = 0;
                winstate->framehead_valid = true;
                return;
            }

            /*
             * In RANGE START_CURRENT mode, frame head is the first row that
             * is a peer of current row.  We search backwards from current,
             * which could be a bit inefficient if peer sets are large. Might
             * be better to have a separate read pointer that moves forward
             * tracking the frame head.
             */
            fhprev = winstate->currentpos - 1;
            for (;;) {
                /* assume the frame head can't go backwards */
                if (fhprev < winstate->frameheadpos)
                    break;
                if (!window_gettupleslot(winobj, fhprev, slot))
                    break; /* start of partition */
                if (!are_peers(winstate, slot, winstate->ss.ss_ScanTupleSlot))
                    break; /* not peer of current row */
                fhprev--;
            }
            winstate->frameheadpos = fhprev + 1;
            winstate->framehead_valid = true;
        } else
            Assert(false);
    } else if (frame_options & FRAMEOPTION_START_VALUE) {
        if (frame_options & FRAMEOPTION_ROWS) {
            /* In ROWS mode, bound is physically n before/after current */
            int64 offset = DatumGetInt64(winstate->startOffsetValue);

            if (frame_options & FRAMEOPTION_START_VALUE_PRECEDING)
                offset = -offset;

            winstate->frameheadpos = truncate_int64_overflow((int128)winstate->currentpos + (int128)offset);
            /* frame head can't go before first row */
            if (winstate->frameheadpos < 0)
                winstate->frameheadpos = 0;
            else if (winstate->frameheadpos > winstate->currentpos) {
                /* make sure frameheadpos is not past end of partition */
                spool_tuples(winstate, winstate->frameheadpos - 1);
                if (winstate->frameheadpos > winstate->spooled_rows)
                    winstate->frameheadpos = winstate->spooled_rows;
            }
            winstate->framehead_valid = true;
        } else if (frame_options & FRAMEOPTION_RANGE) {
            /* parser should have rejected this */
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_EXECUTOR),
                    errmsg("window frame with value offset is not implemented")));
        } else
            Assert(false);
    } else
        Assert(false);
}

/*
 * update_frametailpos
 * make frametailpos valid for the current row
 *
 * Uses the winobj's read pointer for any required fetches; hence, if the
 * frame mode is one that requires row comparisons, the winobj's mark must
 * not be past the currently known frame tail.	Also uses the specified slot
 * for any required fetches.
 */
static void update_frametailpos(WindowObject winobj, TupleTableSlot* slot)
{
    WindowAggState* winstate = winobj->winstate;
    WindowAgg* node = (WindowAgg*)winstate->ss.ps.plan;
    int frame_options = winstate->frameOptions;

    if (winstate->frametail_valid)
        return; /* already known for current row */

    if (frame_options & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) {
        /* In UNBOUNDED FOLLOWING mode, all partition rows are in frame */
        spool_tuples(winstate, -1);
        winstate->frametailpos = winstate->spooled_rows - 1;
        winstate->frametail_valid = true;
    } else if (frame_options & FRAMEOPTION_END_CURRENT_ROW) {
        if (frame_options & FRAMEOPTION_ROWS) {
            /* In ROWS mode, exactly the rows up to current are in frame */
            winstate->frametailpos = winstate->currentpos;
            winstate->frametail_valid = true;
        } else if (frame_options & FRAMEOPTION_RANGE) {
            int64 ftnext;

            /* If no ORDER BY, all rows are peers with each other */
            if (node->ordNumCols == 0) {
                spool_tuples(winstate, -1);
                winstate->frametailpos = winstate->spooled_rows - 1;
                winstate->frametail_valid = true;
                return;
            }

            /*
             * Else we have to search for the first non-peer of the current
             * row.  We assume the current value of frametailpos is a lower
             * bound on the possible frame tail location, ie, frame tail never
             * goes backward, and that currentpos is also a lower bound, ie,
             * frame end always >= current row.
             */
            ftnext = Max(winstate->frametailpos, winstate->currentpos) + 1;
            for (;;) {
                if (!window_gettupleslot(winobj, ftnext, slot))
                    break; /* end of partition */
                if (!are_peers(winstate, slot, winstate->ss.ss_ScanTupleSlot))
                    break; /* not peer of current row */
                ftnext++;
            }
            winstate->frametailpos = ftnext - 1;
            winstate->frametail_valid = true;
        } else
            Assert(false);
    } else if (frame_options & FRAMEOPTION_END_VALUE) {
        if (frame_options & FRAMEOPTION_ROWS) {
            /* In ROWS mode, bound is physically n before/after current */
            int64 offset = DatumGetInt64(winstate->endOffsetValue);

            if (frame_options & FRAMEOPTION_END_VALUE_PRECEDING)
                offset = -offset;

            winstate->frametailpos = truncate_int64_overflow((int128)winstate->currentpos + (int128)offset);
            /* smallest allowable value of frametailpos is -1 */
            if (winstate->frametailpos < 0)
                winstate->frametailpos = -1;
            else if (winstate->frametailpos > winstate->currentpos) {
                /* make sure frametailpos is not past last row of partition */
                spool_tuples(winstate, winstate->frametailpos);
                if (winstate->frametailpos >= winstate->spooled_rows)
                    winstate->frametailpos = winstate->spooled_rows - 1;
            }
            winstate->frametail_valid = true;
        } else if (frame_options & FRAMEOPTION_RANGE) {
            /* parser should have rejected this */
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_EXECUTOR),
                    errmsg("window frame with value offset is not implemented")));
        } else
            Assert(false);
    } else
        Assert(false);
}

/* -----------------
 * ExecWindowAgg
 *
 *	ExecWindowAgg receives tuples from its outer subplan and
 *	stores them into a tuplestore, then processes window functions.
 *	This node doesn't reduce nor qualify any row so the number of
 *	returned rows is exactly the same as its outer subplan's result
 *	(ignoring the case of SRFs in the targetlist, that is).
 * -----------------
 */
static TupleTableSlot* ExecWindowAgg(PlanState* state)
{
    WindowAggState* winstate = castNode(WindowAggState, state);
    TupleTableSlot* result = NULL;
    ExprDoneCond is_done;
    ExprContext* econtext = NULL;
    int i;
    int numfuncs;

    CHECK_FOR_INTERRUPTS();
    
    if (winstate->all_done)
        return NULL;

    /*
     * Check to see if we're still projecting out tuples from a previous
     * output tuple (because there is a function-returning-set in the
     * projection expressions).  If so, try to project another one.
     */
    if (winstate->ss.ps.ps_vec_TupFromTlist) {
        TupleTableSlot* res = ExecProject(winstate->ss.ps.ps_ProjInfo, &is_done);
        if (is_done == ExprMultipleResult)
            return res;
        /* Done with that source tuple... */
        winstate->ss.ps.ps_vec_TupFromTlist = false;
    }

    /*
     * Compute frame offset values, if any, during first call.
     */
    if (winstate->all_first) {
        int frameOptions = winstate->frameOptions;
        ExprContext* exprContext = winstate->ss.ps.ps_ExprContext;
        Datum value;
        bool isnull = false;
        int16 len;
        bool byval = false;

        if (frameOptions & FRAMEOPTION_START_VALUE) {
            Assert(winstate->startOffset != NULL);
            value = ExecEvalExprSwitchContext(winstate->startOffset, exprContext, &isnull, NULL);
            if (isnull)
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmodule(MOD_EXECUTOR),
                        errmsg("frame starting offset must not be null")));
            /* copy value into query-lifespan context */
            get_typlenbyval(exprType((Node*)winstate->startOffset->expr), &len, &byval);
            winstate->startOffsetValue = datumCopy(value, byval, len);
            if (frameOptions & FRAMEOPTION_ROWS) {
                /* value is known to be int8 */
                int64 offset = DatumGetInt64(value);
                if (offset < 0)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmodule(MOD_EXECUTOR),
                            errmsg("frame starting offset must not be negative")));
            }
        }
        if (frameOptions & FRAMEOPTION_END_VALUE) {
            Assert(winstate->endOffset != NULL);
            value = ExecEvalExprSwitchContext(winstate->endOffset, exprContext, &isnull, NULL);
            if (isnull)
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmodule(MOD_EXECUTOR),
                        errmsg("frame ending offset must not be null")));
            /* copy value into query-lifespan context */
            get_typlenbyval(exprType((Node*)winstate->endOffset->expr), &len, &byval);
            winstate->endOffsetValue = datumCopy(value, byval, len);
            if (frameOptions & FRAMEOPTION_ROWS) {
                /* value is known to be int8 */
                int64 offset = DatumGetInt64(value);
                if (offset < 0)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmodule(MOD_EXECUTOR),
                            errmsg("frame ending offset must not be negative")));
            }
        }
        winstate->all_first = false;
    }

restart:
    if (winstate->buffer == NULL) {
        /* Initialize for first partition and set current row = 0 */
        begin_partition(winstate);
        /* If there are no input rows, we'll detect that and exit below */
    } else {
        /* Advance current row within partition */
        winstate->currentpos++;
        /* This might mean that the frame moves, too */
        winstate->framehead_valid = false;
        winstate->frametail_valid = false;
    }

    /*
     * Spool all tuples up to and including the current row, if we haven't
     * already
     */
    spool_tuples(winstate, winstate->currentpos);

    /* Move to the next partition if we reached the end of this partition */
    if (winstate->partition_spooled && winstate->currentpos >= winstate->spooled_rows) {
        release_partition(winstate);

        if (winstate->more_partitions) {
            begin_partition(winstate);
            Assert(winstate->spooled_rows > 0);
        } else {
            winstate->all_done = true;
            return NULL;
        }
    }

    /* final output execution is in ps_ExprContext */
    econtext = winstate->ss.ps.ps_ExprContext;

    /* Clear the per-output-tuple context for current row */
    ResetExprContext(econtext);

    /*
     * Read the current row from the tuplestore, and save in ScanTupleSlot.
     * (We can't rely on the outerplan's output slot because we may have to
     * read beyond the current row.  Also, we have to actually copy the row
     * out of the tuplestore, since window function evaluation might cause the
     * tuplestore to dump its state to disk.)
     *
     * Current row must be in the tuplestore, since we spooled it above.
     */
    tuplestore_select_read_pointer(winstate->buffer, winstate->current_ptr);
    if (!tuplestore_gettupleslot(winstate->buffer, true, true, winstate->ss.ss_ScanTupleSlot))
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION),
                errmodule(MOD_EXECUTOR),
                errmsg("cannot get result from tuplestore for WinwdowsAgg")));

    /*
     * Evaluate true window functions
     */
    numfuncs = winstate->numfuncs;
    for (i = 0; i < numfuncs; i++) {
        WindowStatePerFunc perfuncstate = &(winstate->perfunc[i]);

        if (perfuncstate->plain_agg)
            continue;
        eval_windowfunction(winstate,
            perfuncstate,
            &(econtext->ecxt_aggvalues[perfuncstate->wfuncstate->wfuncno]),
            &(econtext->ecxt_aggnulls[perfuncstate->wfuncstate->wfuncno]));
    }

    /*
     * Evaluate aggregates
     */
    if (winstate->numaggs > 0)
        eval_windowaggregates(winstate);

    /*
     * Truncate any no-longer-needed rows from the tuplestore.
     */
    tuplestore_trim(winstate->buffer);

    /*
     * Form and return a projection tuple using the windowfunc results and the
     * current row.  Setting ecxt_outertuple arranges that any Vars will be
     * evaluated with respect to that row.
     */
    econtext->ecxt_outertuple = winstate->ss.ss_ScanTupleSlot;
    result = ExecProject(winstate->ss.ps.ps_ProjInfo, &is_done);

    if (is_done == ExprEndResult) {
        /* SRF in tlist returned no rows, so advance to next input tuple */
        goto restart;
    }

    winstate->ss.ps.ps_vec_TupFromTlist = (is_done == ExprMultipleResult);
    return result;
}

/* setup sort op for keep order */
static void initialize_peragg_keep(WindowFunc* wfunc, WindowStatePerAgg peraggstate)
{
    int i = 0;
    int numKeepCols;
    ListCell *sort_lc;
    ListCell *arg_lc;

    numKeepCols = peraggstate->numKeepCols = list_length(wfunc->keep_args);

    Assert(list_length(wfunc->winkporder) == numKeepCols);

    peraggstate->keepRank = (KeepRank*)palloc0(sizeof(KeepRank) * numKeepCols);
    forboth(sort_lc, wfunc->winkporder, arg_lc, wfunc->keep_args) {
        const Node *arg = (Node*)lfirst(arg_lc);
        SortGroupClause *sortcl = lfirst_node(SortGroupClause, sort_lc);
        KeepRank*  rank_data = &peraggstate->keepRank[i];
        SortSupport sortKey = &rank_data->keepSort;

        /* the parser should have made sure of this */
        Assert(OidIsValid(sortcl->sortop));

        sortKey->ssup_attno = 0; /* unused */
        sortKey->ssup_collation = exprCollation(arg);
        sortKey->ssup_nulls_first = sortcl->nulls_first;
        sortKey->abbreviate = false;
        sortKey->ssup_cxt = CurrentMemoryContext;
        PrepareSortSupportFromOrderingOp(sortcl->sortop, sortKey);
        get_typlenbyval(exprType(arg), &rank_data->keeptypeLen, &rank_data->keeptypeByVal);
        i++;
    }
}

/* -----------------
 * ExecInitWindowAgg
 *
 *	Creates the run-time information for the WindowAgg node produced by the
 *	planner and initializes its outer subtree
 * -----------------
 */
WindowAggState* ExecInitWindowAgg(WindowAgg* node, EState* estate, int eflags)
{
    Plan* left_tree = NULL;
    ExprContext* econtext = NULL;
    ExprContext* tmp_context = NULL;
    WindowStatePerFunc perfunc = NULL;
    WindowStatePerAgg peragg;
    int num_funcs;
    int wfuncno;
    int numaggs;
    int aggno;
    ListCell* l = NULL;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * create state structure
     */
    WindowAggState* winstate = makeNode(WindowAggState);
    winstate->ss.ps.plan = (Plan*)node;
    winstate->ss.ps.state = estate;
    winstate->ss.ps.ExecProcNode = ExecWindowAgg;

    int64 operator_mem = SET_NODEMEM(((Plan*)node)->operatorMemKB[0], ((Plan*)node)->dop);
    AllocSetContext* set = (AllocSetContext*)(estate->es_query_cxt);
    set->maxSpaceSize = operator_mem * 1024L + SELF_GENRIC_MEMCTX_LIMITATION;

    /*
     * Create expression contexts.	We need two, one for per-input-tuple
     * processing and one for per-output-tuple processing.	We cheat a little
     * by using ExecAssignExprContext() to build both.
     */
    ExecAssignExprContext(estate, &winstate->ss.ps);
    tmp_context = winstate->ss.ps.ps_ExprContext;
    winstate->tmpcontext = tmp_context;
    ExecAssignExprContext(estate, &winstate->ss.ps);

    /* Create long-lived context for storage of partition-local memory etc */
    winstate->partcontext = AllocSetContextCreate(CurrentMemoryContext,
        "WindowAgg_Partition",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /* Create mid-lived context for aggregate trans values etc */
    winstate->aggcontext = AllocSetContextCreate(CurrentMemoryContext,
        "WindowAgg_Aggregates",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * tuple table initialization
     */
    ExecInitScanTupleSlot(estate, &winstate->ss);
    ExecInitResultTupleSlot(estate, &winstate->ss.ps);
    winstate->first_part_slot = ExecInitExtraTupleSlot(estate);
    winstate->agg_row_slot = ExecInitExtraTupleSlot(estate);
    winstate->temp_slot_1 = ExecInitExtraTupleSlot(estate);
    winstate->temp_slot_2 = ExecInitExtraTupleSlot(estate);

    if (!estate->es_is_flt_frame)
    {
        winstate->ss.ps.targetlist = (List *)ExecInitExprByRecursion((Expr *)node->plan.targetlist, (PlanState *)winstate);
    }

    /*
     * WindowAgg nodes never have quals, since they can only occur at the
     * logical top level of a query (ie, after any WHERE or HAVING filters)
     */
    Assert(node->plan.qual == NIL);
    winstate->ss.ps.qual = NIL;

    /*
     * initialize child nodes
     */
    left_tree = outerPlan(node);
    outerPlanState(winstate) = ExecInitNode(left_tree, estate, eflags);

    /*
     * initialize source tuple type (which is also the tuple type that we'll
     * store in the tuplestore and use in all our working slots).
     */
    ExecAssignScanTypeFromOuterPlan(&winstate->ss);

    ExecSetSlotDescriptor(winstate->first_part_slot, winstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
    ExecSetSlotDescriptor(winstate->agg_row_slot, winstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
    ExecSetSlotDescriptor(winstate->temp_slot_1, winstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
    ExecSetSlotDescriptor(winstate->temp_slot_2, winstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor);

    /*
     * Initialize result tuple type and projection info.
     * result Tuple Table Slot contains virtual tuple, default tableAm type is set to HEAP.
     */
    ExecAssignResultTypeFromTL(&winstate->ss.ps);

    ExecAssignProjectionInfo(&winstate->ss.ps, NULL);

    winstate->ss.ps.ps_vec_TupFromTlist = false;

    /* Set up data for comparing tuples */
    if (node->partNumCols > 0)
        winstate->partEqfunctions = execTuplesMatchPrepare(node->partNumCols, node->partOperators);
    if (node->ordNumCols > 0)
        winstate->ordEqfunctions = execTuplesMatchPrepare(node->ordNumCols, node->ordOperators);

    /*
     * WindowAgg nodes use aggvalues and aggnulls as well as Agg nodes.
     */
    num_funcs = winstate->numfuncs;
    numaggs = winstate->numaggs;
    econtext = winstate->ss.ps.ps_ExprContext;
    econtext->ecxt_aggvalues = (Datum*)palloc0(sizeof(Datum) * num_funcs);
    econtext->ecxt_aggnulls = (bool*)palloc0(sizeof(bool) * num_funcs);

    /*
     * allocate per-wfunc/per-agg state information.
     */
    perfunc = (WindowStatePerFunc)palloc0(sizeof(WindowStatePerFuncData) * num_funcs);
    peragg = (WindowStatePerAgg)palloc0(sizeof(WindowStatePerAggData) * numaggs);
    winstate->perfunc = perfunc;
    winstate->peragg = peragg;

    wfuncno = -1;
    aggno = -1;
    foreach (l, winstate->funcs) {
        WindowFuncExprState* wfuncstate = (WindowFuncExprState*)lfirst(l);
        WindowFunc* wfunc = (WindowFunc*)wfuncstate->xprstate.expr;
        WindowStatePerFunc perfuncstate;
        AclResult aclresult;
        int i;

        if (wfunc->winref != node->winref) /* planner screwed up? */
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmodule(MOD_EXECUTOR),
                    errmsg("WindowFunc with winref %u assigned to WindowAgg with winref %u",
                        wfunc->winref,
                        node->winref)));

        /* Look for a previous duplicate window function */
        for (i = 0; i <= wfuncno; i++) {
            if (equal(wfunc, perfunc[i].wfunc) && !contain_volatile_functions((Node*)wfunc))
                break;
        }
        if (i <= wfuncno) {
            /* Found a match to an existing entry, so just mark it */
            wfuncstate->wfuncno = i;
            continue;
        }

        /* Nope, so assign a new PerAgg record */
        perfuncstate = &perfunc[++wfuncno];

        /* Mark WindowFunc state node with assigned index in the result array */
        wfuncstate->wfuncno = wfuncno;

        /* Check permission to call window function */
        aclresult = pg_proc_aclcheck(wfunc->winfnoid, GetUserId(), ACL_EXECUTE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(wfunc->winfnoid));

        /* Fill in the perfuncstate data */
        perfuncstate->wfuncstate = wfuncstate;
        perfuncstate->wfunc = wfunc;
        perfuncstate->numArguments = list_length(wfuncstate->args);

        fmgr_info_cxt(wfunc->winfnoid, &perfuncstate->flinfo, econtext->ecxt_per_query_memory);
        fmgr_info_set_expr((Node*)wfunc, &perfuncstate->flinfo);

        perfuncstate->winCollation = wfunc->inputcollid;

        get_typlenbyval(wfunc->wintype, &perfuncstate->resulttypeLen, &perfuncstate->resulttypeByVal);

        /*
         * If it's really just a plain aggregate function, we'll emulate the
         * Agg environment for it.
         */
        perfuncstate->plain_agg = wfunc->winagg;
        if (wfunc->winagg) {
            WindowStatePerAgg peraggstate;

            perfuncstate->aggno = ++aggno;
            peraggstate = &winstate->peragg[aggno];
            initialize_peragg(winstate, wfunc, peraggstate);
            peraggstate->wfuncno = wfuncno;
            initialize_peragg_keep(wfunc, peraggstate);
        } else {
            WindowObject winobj = makeNode(WindowObjectData);

            winobj->winstate = winstate;
            winobj->argstates = wfuncstate->args;
            winobj->localmem = NULL;
            winobj->is_from_last = wfunc->is_from_last;
            winobj->is_ignore_nulls = wfunc->is_ignore_nulls;
            perfuncstate->winobj = winobj;
        }
    }

    /* Update numfuncs, numaggs to match number of unique functions found */
    winstate->numfuncs = wfuncno + 1;
    winstate->numaggs = aggno + 1;

    /* Set up WindowObject for aggregates, if needed */
    if (winstate->numaggs > 0) {
        WindowObject agg_winobj = makeNode(WindowObjectData);

        agg_winobj->winstate = winstate;
        agg_winobj->argstates = NIL;
        agg_winobj->localmem = NULL;
        /* make sure markptr = -1 to invalidate. It may not get used */
        agg_winobj->markptr = -1;
        agg_winobj->readptr = -1;
        winstate->agg_winobj = agg_winobj;
    }

    /* copy frame options to state node for easy access */
    winstate->frameOptions = node->frameOptions;

    /* initialize frame bound offset expressions */
    winstate->startOffset = ExecInitExpr((Expr*)node->startOffset, (PlanState*)winstate);
    winstate->endOffset = ExecInitExpr((Expr*)node->endOffset, (PlanState*)winstate);

    winstate->all_first = true;
    winstate->partition_spooled = false;
    winstate->more_partitions = false;

    return winstate;
}

/* -----------------
 * ExecEndWindowAgg
 * -----------------
 */
void ExecEndWindowAgg(WindowAggState* node)
{
    PlanState* outer_plan = NULL;

    release_partition(node);

    pfree_ext(node->perfunc);
    pfree_ext(node->peragg);

    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);
    (void)ExecClearTuple(node->first_part_slot);
    (void)ExecClearTuple(node->agg_row_slot);
    (void)ExecClearTuple(node->temp_slot_1);
    (void)ExecClearTuple(node->temp_slot_2);

    /*
     * Free both the expr contexts.
     */
    ExecFreeExprContext(&node->ss.ps);
    node->ss.ps.ps_ExprContext = node->tmpcontext;
    ExecFreeExprContext(&node->ss.ps);

    MemoryContextDelete(node->partcontext);
    MemoryContextDelete(node->aggcontext);

    outer_plan = outerPlanState(node);
    ExecEndNode(outer_plan);
}

/* -----------------
 * ExecReScanWindowAgg
 * -----------------
 */
void ExecReScanWindowAgg(WindowAggState* node)
{
    ExprContext* econtext = node->ss.ps.ps_ExprContext;

    node->all_done = false;

    node->ss.ps.ps_vec_TupFromTlist = false;
    node->all_first = true;

    /* release tuplestore et al */
    release_partition(node);

    /* release all temp tuples, but especially first_part_slot */
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);
    (void)ExecClearTuple(node->first_part_slot);
    (void)ExecClearTuple(node->agg_row_slot);
    (void)ExecClearTuple(node->temp_slot_1);
    (void)ExecClearTuple(node->temp_slot_2);

    /* Forget current wfunc values */
    errno_t rc = memset_s(econtext->ecxt_aggvalues, sizeof(Datum) * node->numfuncs, 0, sizeof(Datum) * node->numfuncs);
    securec_check(rc, "", "");
    rc = memset_s(econtext->ecxt_aggnulls, sizeof(bool) * node->numfuncs, 0, sizeof(bool) * node->numfuncs);
    securec_check(rc, "", "");

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->ss.ps.lefttree->chgParam == NULL)
        ExecReScan(node->ss.ps.lefttree);
}

/*
 * initialize_peragg
 *
 * Almost same as in nodeAgg.c, except we don't support DISTINCT currently.
 */
WindowStatePerAggData* initialize_peragg(WindowAggState* winstate, WindowFunc* wfunc, 
    WindowStatePerAgg peraggstate)
{
    Oid input_types[FUNC_MAX_ARGS];
    int num_arguments;
    HeapTuple agg_tuple;
    Form_pg_aggregate aggform;
    Oid agg_trans_type;
    AclResult aclresult;
    Oid transfn_oid, finalfn_oid;
    Expr* transfnexpr = NULL;
    Expr* finalfnexpr = NULL;
    Datum text_initVal;
    int i;
    ListCell* lc = NULL;

    num_arguments = list_length(wfunc->args);

    i = 0;
    foreach (lc, wfunc->args) {
        input_types[i++] = exprType((Node*)lfirst(lc));
    }

    agg_tuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(wfunc->winfnoid));
    if (!HeapTupleIsValid(agg_tuple))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmodule(MOD_EXECUTOR),
                errmsg("cache lookup failed for aggregate %u of WindowsAgg", wfunc->winfnoid)));
    aggform = (Form_pg_aggregate)GETSTRUCT(agg_tuple);

    /*
     * ExecInitWindowAgg already checked permission to call aggregate function
     * ... but we still need to check the component functions
     */
    peraggstate->transfn_oid = transfn_oid = aggform->aggtransfn;
    peraggstate->finalfn_oid = finalfn_oid = aggform->aggfinalfn;

    /* Check that aggregate owner has permission to call component fns */
    {
        HeapTuple proc_tuple;
        Oid agg_owner;

        proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(wfunc->winfnoid));
        if (!HeapTupleIsValid(proc_tuple))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmodule(MOD_EXECUTOR),
                    errmsg("cache lookup failed for function %u of WindowsAgg in pg_proc", wfunc->winfnoid)));
        agg_owner = ((Form_pg_proc)GETSTRUCT(proc_tuple))->proowner;
        ReleaseSysCache(proc_tuple);

        aclresult = pg_proc_aclcheck(transfn_oid, agg_owner, ACL_EXECUTE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(transfn_oid));
        if (OidIsValid(finalfn_oid)) {
            aclresult = pg_proc_aclcheck(finalfn_oid, agg_owner, ACL_EXECUTE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(finalfn_oid));
        }
    }

    /* resolve actual type of transition state, if polymorphic */
    agg_trans_type = resolve_aggregate_transtype(wfunc->winfnoid, aggform->aggtranstype, input_types, num_arguments);

    if (winstate->ss.ps.state->es_is_flt_frame) {
        build_aggregate_transfn_expr(input_types, num_arguments, 0, false, wfunc->wintype, wfunc->inputcollid,
                                     transfn_oid, &transfnexpr);
    } else {
        /* build expression trees using actual argument & result types */
        build_trans_aggregate_fnexprs(num_arguments, 0, false, false, agg_trans_type, input_types, wfunc->wintype,
                                      wfunc->inputcollid, transfn_oid, finalfn_oid, &transfnexpr, &finalfnexpr);
    }

    fmgr_info(transfn_oid, &peraggstate->transfn);
    fmgr_info_set_expr((Node*)transfnexpr, &peraggstate->transfn);

    if (winstate->ss.ps.state->es_is_flt_frame) {
        if (OidIsValid(finalfn_oid)) {
            build_aggregate_finalfn_expr(input_types, 1, agg_trans_type, wfunc->wintype, wfunc->inputcollid,
                                         finalfn_oid, &finalfnexpr);
        }
    }

    if (OidIsValid(finalfn_oid)) {
        fmgr_info(finalfn_oid, &peraggstate->finalfn);
        fmgr_info_set_expr((Node*)finalfnexpr, &peraggstate->finalfn);
    }

    get_typlenbyval(wfunc->wintype, &peraggstate->resulttypeLen, &peraggstate->resulttypeByVal);
    get_typlenbyval(agg_trans_type, &peraggstate->transtypeLen, &peraggstate->transtypeByVal);

    /*
     * initval is potentially null, so don't try to access it as a struct
     * field. Must do it the hard way with SysCacheGetAttr.
     */
    text_initVal = SysCacheGetAttr(AGGFNOID, agg_tuple, Anum_pg_aggregate_agginitval, &peraggstate->initValueIsNull);

    if (peraggstate->initValueIsNull)
        peraggstate->initValue = (Datum)0;
    else
        peraggstate->initValue = get_agg_init_val(text_initVal, agg_trans_type);

    /*
     * If the transfn is strict and the initval is NULL, make sure input type
     * and transtype are the same (or at least binary-compatible), so that
     * it's OK to use the first input value as the initial transValue.  This
     * should have been checked at agg definition time, but just in case...
     */
    if (peraggstate->transfn.fn_strict && peraggstate->initValueIsNull) {
        if (num_arguments < 1 || !IsBinaryCoercible(input_types[0], agg_trans_type))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                    errmodule(MOD_EXECUTOR),
                    errmsg("aggregate %u needs to have compatible input type and transition type", wfunc->winfnoid)));
    }

    ReleaseSysCache(agg_tuple);

    return peraggstate;
}

static Datum get_agg_init_val(Datum text_init_val, Oid transtype)
{
    Oid typ_input, typ_io_param;
    char* str_init_val = NULL;
    Datum init_val;

    getTypeInputInfo(transtype, &typ_input, &typ_io_param);
    str_init_val = TextDatumGetCString(text_init_val);
    init_val = OidInputFunctionCall(typ_input, str_init_val, typ_io_param, -1);
    pfree_ext(str_init_val);
    return init_val;
}

/*
 * are_peers
 * compare two rows to see if they are equal according to the ORDER BY clause
 *
 * NB: this does not consider the window frame mode.
 */
static bool are_peers(WindowAggState* winstate, TupleTableSlot* slot1, TupleTableSlot* slot2)
{
    WindowAgg* node = (WindowAgg*)winstate->ss.ps.plan;

    /* If no ORDER BY, all rows are peers with each other */
    if (node->ordNumCols == 0)
        return true;

    return execTuplesMatch(slot1,
        slot2,
        node->ordNumCols,
        node->ordColIdx,
        winstate->ordEqfunctions,
        winstate->tmpcontext->ecxt_per_tuple_memory,
        node->ord_collations);
}

/*
 * window_gettupleslot
 *	Fetch the pos'th tuple of the current partition into the slot,
 *	using the winobj's read pointer
 *
 * Returns true if successful, false if no such row
 */
static bool window_gettupleslot(WindowObject winobj, int64 pos, TupleTableSlot* slot)
{
    WindowAggState* winstate = winobj->winstate;
    MemoryContext old_context;

    /* Don't allow passing -1 to spool_tuples here */
    if (pos < 0)
        return false;

    /* If necessary, fetch the tuple into the spool */
    spool_tuples(winstate, pos);

    if (pos >= winstate->spooled_rows)
        return false;

    if (pos < winobj->markpos)
        ereport(ERROR,
            (errcode(ERRCODE_FETCH_DATA_FAILED),
                errmodule(MOD_EXECUTOR),
                errmsg("cannot fetch row before WindowObject's mark position")));

    old_context = MemoryContextSwitchTo(winstate->ss.ps.ps_ExprContext->ecxt_per_query_memory);

    tuplestore_select_read_pointer(winstate->buffer, winobj->readptr);

    /*
     * There's no API to refetch the tuple at the current position. We have to
     * move one tuple forward, and then one backward.  (We don't do it the
     * other way because we might try to fetch the row before our mark, which
     * isn't allowed.)  XXX this case could stand to be optimized.
     */
    if (winobj->seekpos == pos) {
        (void)tuplestore_advance(winstate->buffer, true);
        winobj->seekpos++;
    }

    while (winobj->seekpos > pos) {
        if (!tuplestore_gettupleslot(winstate->buffer, false, true, slot))
            ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                    errmodule(MOD_EXECUTOR),
                    errmsg("cannot get result from tuplestore in WindowsAgg.")));
        winobj->seekpos--;
    }

    while (winobj->seekpos < pos) {
        if (!tuplestore_gettupleslot(winstate->buffer, true, true, slot))
            ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION),
                    errmodule(MOD_EXECUTOR),
                    errmsg("cannot get result from tuplestore in WindowsAgg.")));
        winobj->seekpos++;
    }

    MemoryContextSwitchTo(old_context);

    return true;
}

/***********************************************************************
 * API exposed to window functions
 ***********************************************************************/
/*
 * WinGetPartitionLocalMemory
 *		Get working memory that lives till end of partition processing
 *
 * On first call within a given partition, this allocates and zeroes the
 * requested amount of space.  Subsequent calls just return the same chunk.
 *
 * Memory obtained this way is normally used to hold state that should be
 * automatically reset for each new partition.	If a window function wants
 * to hold state across the whole query, fcinfo->fn_extra can be used in the
 * usual way for that.
 */
void* WinGetPartitionLocalMemory(WindowObject winobj, Size sz)
{
    Assert(WindowObjectIsValid(winobj));
    if (winobj->localmem == NULL)
        winobj->localmem = MemoryContextAllocZero(winobj->winstate->partcontext, sz);
    return winobj->localmem;
}

/*
 * WinGetCurrentPosition
 *		Return the current row's position (counting from 0) within the current
 *		partition.
 */
int64 WinGetCurrentPosition(WindowObject winobj)
{
    Assert(WindowObjectIsValid(winobj));
    return winobj->winstate->currentpos;
}

/*
 * WinGetPartitionRowCount
 *		Return total number of rows contained in the current partition.
 *
 * Note: this is a relatively expensive operation because it forces the
 * whole partition to be "spooled" into the tuplestore at once.  Once
 * executed, however, additional calls within the same partition are cheap.
 */
int64 WinGetPartitionRowCount(WindowObject winobj)
{
    Assert(WindowObjectIsValid(winobj));
    spool_tuples(winobj->winstate, -1);
    return winobj->winstate->spooled_rows;
}

/*
 * WinSetMarkPosition
 *		Set the "mark" position for the window object, which is the oldest row
 *		number (counting from 0) it is allowed to fetch during all subsequent
 *		operations within the current partition.
 *
 * Window functions do not have to call this, but are encouraged to move the
 * mark forward when possible to keep the tuplestore size down and prevent
 * having to spill rows to disk.
 */
void WinSetMarkPosition(WindowObject winobj, int64 markpos)
{
    WindowAggState* winstate = NULL;

    Assert(WindowObjectIsValid(winobj));
    winstate = winobj->winstate;

    if (markpos < winobj->markpos)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_EXECUTOR),
                errmsg("cannot move WindowObject's mark position backward")));
    tuplestore_select_read_pointer(winstate->buffer, winobj->markptr);
    while (markpos > winobj->markpos) {
        (void)tuplestore_advance(winstate->buffer, true);
        winobj->markpos++;
    }
    tuplestore_select_read_pointer(winstate->buffer, winobj->readptr);
    while (markpos > winobj->seekpos) {
        (void)tuplestore_advance(winstate->buffer, true);
        winobj->seekpos++;
    }
}

/*
 * WinRowsArePeers
 *		Compare two rows (specified by absolute position in window) to see
 *		if they are equal according to the ORDER BY clause.
 *
 * NB: this does not consider the window frame mode.
 */
bool WinRowsArePeers(WindowObject winobj, int64 pos1, int64 pos2)
{
    WindowAggState* winstate = NULL;
    WindowAgg* node = NULL;
    TupleTableSlot* slot1 = NULL;
    TupleTableSlot* slot2 = NULL;
    bool res = false;

    Assert(WindowObjectIsValid(winobj));
    winstate = winobj->winstate;
    node = (WindowAgg*)winstate->ss.ps.plan;

    /* If no ORDER BY, all rows are peers; don't bother to fetch them */
    if (node->ordNumCols == 0)
        return true;

    slot1 = winstate->temp_slot_1;
    slot2 = winstate->temp_slot_2;

    if (!window_gettupleslot(winobj, pos1, slot1))
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION),
                errmodule(MOD_EXECUTOR),
                errmsg("specified position is out of window: " INT64_FORMAT, pos1)));

    if (!window_gettupleslot(winobj, pos2, slot2))
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION),
                errmodule(MOD_EXECUTOR),
                errmsg("specified position is out of window: " INT64_FORMAT, pos2)));

    res = are_peers(winstate, slot1, slot2);

    (void)ExecClearTuple(slot1);
    (void)ExecClearTuple(slot2);

    return res;
}

/*
 * WinGetFuncArgInPartition
 *		Evaluate a window function's argument expression on a specified
 *		row of the partition.  The row is identified in lseek(2) style,
 *		i.e. relative to the current, first, or last row.
 *
 * argno: argument number to evaluate (counted from 0)
 * relpos: signed rowcount offset from the seek position
 * seektype: WINDOW_SEEK_CURRENT, WINDOW_SEEK_HEAD, or WINDOW_SEEK_TAIL
 * set_mark: If the row is found and set_mark is true, the mark is moved to
 *		the row as a side-effect.
 * isnull: output argument, receives isnull status of result
 * isout: output argument, set to indicate whether target row position
 *		is out of partition (can pass NULL if caller doesn't care about this)
 *
 * Specifying a nonexistent row is not an error, it just causes a null result
 * (plus setting *isout true, if isout isn't NULL).
 */
Datum WinGetFuncArgInPartition(
    WindowObject winobj, int argno, int relpos, int seektype, bool set_mark, bool* isnull, bool* isout)
{
    WindowAggState* winstate = NULL;
    ExprContext* econtext = NULL;
    TupleTableSlot* slot = NULL;
    bool got_tuple = false;
    int64 abs_pos;

    Assert(WindowObjectIsValid(winobj));
    winstate = winobj->winstate;
    econtext = winstate->ss.ps.ps_ExprContext;
    slot = winstate->temp_slot_1;

    switch (seektype) {
        case WINDOW_SEEK_CURRENT:
            abs_pos = winstate->currentpos + relpos;
            break;
        case WINDOW_SEEK_HEAD:
            abs_pos = relpos;
            break;
        case WINDOW_SEEK_TAIL:
            spool_tuples(winstate, -1);
            abs_pos = winstate->spooled_rows - 1 + relpos;
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_EXECUTOR),
                    errmsg("unrecognized window seek type: %d on a specified row of partition.", seektype)));
            abs_pos = 0; /* keep compiler quiet */
            break;
    }

    got_tuple = window_gettupleslot(winobj, abs_pos, slot);
    if (!got_tuple) {
        if (isout !=NULL)
            *isout = true;
        *isnull = true;
        return (Datum)0;
    } else {
        if (isout != NULL)
            *isout = false;
        if (set_mark) {
            int frameOptions = winstate->frameOptions;
            int64 mark_pos = abs_pos;

            /*
             * In RANGE mode with a moving frame head, we must not let the
             * mark advance past frameheadpos, since that row has to be
             * fetchable during future update_frameheadpos calls.
             *
             * XXX it is very ugly to pollute window functions' marks with
             * this consideration; it could for instance mask a logic bug that
             * lets a window function fetch rows before what it had claimed
             * was its mark.  Perhaps use a separate mark for frame head
             * probes?
             */
            if ((frameOptions & FRAMEOPTION_RANGE) && !(frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING)) {
                update_frameheadpos(winobj, winstate->temp_slot_2);
                if (mark_pos > winstate->frameheadpos)
                    mark_pos = winstate->frameheadpos;
            }
            WinSetMarkPosition(winobj, mark_pos);
        }
        econtext->ecxt_outertuple = slot;
        return ExecEvalExpr((ExprState*)list_nth(winobj->argstates, argno), econtext, isnull, NULL);
    }
}

/*
 * WinGetFuncArgInFrame
 *		Evaluate a window function's argument expression on a specified
 *		row of the window frame.  The row is identified in lseek(2) style,
 *		i.e. relative to the current, first, or last row.
 *
 * argno: argument number to evaluate (counted from 0)
 * relpos: signed rowcount offset from the seek position
 * seektype: WINDOW_SEEK_CURRENT, WINDOW_SEEK_HEAD, or WINDOW_SEEK_TAIL
 * set_mark: If the row is found and set_mark is true, the mark is moved to
 *		the row as a side-effect.
 * isnull: output argument, receives isnull status of result
 * isout: output argument, set to indicate whether target row position
 *		is out of frame (can pass NULL if caller doesn't care about this)
 *
 * Specifying a nonexistent row is not an error, it just causes a null result
 * (plus setting *isout true, if isout isn't NULL).
 */
Datum WinGetFuncArgInFrame(
    WindowObject winobj, int argno, int relpos, int seektype, bool set_mark, bool* isnull, bool* isout)
{
    WindowAggState* winstate = NULL;
    ExprContext* econtext = NULL;
    TupleTableSlot* slot = NULL;
    bool got_tuple = false;
    int64 abs_pos;

    if (!(WindowObjectIsValid(winobj))) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmodule(MOD_EXECUTOR),
                errmsg("window object is invalid")));
    }

    winstate = winobj->winstate;
    econtext = winstate->ss.ps.ps_ExprContext;
    slot = winstate->temp_slot_1;

    switch (seektype) {
        case WINDOW_SEEK_CURRENT:
            abs_pos = winstate->currentpos + relpos;
            break;
        case WINDOW_SEEK_HEAD:
            update_frameheadpos(winobj, slot);
            abs_pos = winstate->frameheadpos + relpos;
            break;
        case WINDOW_SEEK_TAIL:
            update_frametailpos(winobj, slot);
            abs_pos = winstate->frametailpos + relpos;
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_EXECUTOR),
                    errmsg("unrecognized window seek type: %d on a specified row of window frame", seektype)));
            abs_pos = 0; /* keep compiler quiet */
            break;
    }

    got_tuple = window_gettupleslot(winobj, abs_pos, slot);
    if (got_tuple)
        got_tuple = row_is_in_frame(winstate, abs_pos, slot);

    if (!got_tuple) {
        if (isout != NULL)
            *isout = true;
        *isnull = true;
        return (Datum)0;
    } else {
        if (isout != NULL)
            *isout = false;
        if (set_mark) {
            int frameOptions = winstate->frameOptions;
            int64 mark_pos = abs_pos;

            /*
             * In RANGE mode with a moving frame head, we must not let the
             * mark advance past frameheadpos, since that row has to be
             * fetchable during future update_frameheadpos calls.
             *
             * XXX it is very ugly to pollute window functions' marks with
             * this consideration; it could for instance mask a logic bug that
             * lets a window function fetch rows before what it had claimed
             * was its mark.  Perhaps use a separate mark for frame head
             * probes?
             */
            if ((frameOptions & FRAMEOPTION_RANGE) && !(frameOptions & FRAMEOPTION_START_UNBOUNDED_PRECEDING)) {
                update_frameheadpos(winobj, winstate->temp_slot_2);
                if (mark_pos > winstate->frameheadpos)
                    mark_pos = winstate->frameheadpos;
            }
            WinSetMarkPosition(winobj, mark_pos);
        }
        econtext->ecxt_outertuple = slot;
        return ExecEvalExpr((ExprState*)list_nth(winobj->argstates, argno), econtext, isnull, NULL);
    }
}

/*
 * WinGetFuncArgCurrent
 *		Evaluate a window function's argument expression on the current row.
 *
 * argno: argument number to evaluate (counted from 0)
 * isnull: output argument, receives isnull status of result
 *
 * Note: this isn't quite equivalent to WinGetFuncArgInPartition or
 * WinGetFuncArgInFrame targeting the current row, because it will succeed
 * even if the WindowObject's mark has been set beyond the current row.
 * This should generally be used for "ordinary" arguments of a window
 * function, such as the offset argument of lead() or lag().
 */
Datum WinGetFuncArgCurrent(WindowObject winobj, int argno, bool* isnull)
{
    WindowAggState* winstate = NULL;
    ExprContext* econtext = NULL;

    if (!(WindowObjectIsValid(winobj))) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmodule(MOD_EXECUTOR),
                errmsg("window object is invalid")));
    }
    winstate = winobj->winstate;

    econtext = winstate->ss.ps.ps_ExprContext;

    econtext->ecxt_outertuple = winstate->ss.ss_ScanTupleSlot;
    return ExecEvalExpr((ExprState*)list_nth(winobj->argstates, argno), econtext, isnull, NULL);
}
