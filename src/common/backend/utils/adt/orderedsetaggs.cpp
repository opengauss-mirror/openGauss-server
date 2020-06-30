/*-------------------------------------------------------------------------
 *
 * orderedsetaggs.c
 *>->---Ordered-set aggregate functions.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *>-  src/backend/utils/adt/orderedsetaggs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "executor/nodeAgg.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/tuplesort.h"

const char* PERCENTILE_FLOAT8 = "percentile_float8";
const char* PERCENTILE_INTERVAL = "percentile_interval";

/*
 * Release resource when doing portal drop clean
 */
static void ordered_set_shutdown(Datum arg)
{
    OrderedSetAggState* osa_state = NULL;
    osa_state = (OrderedSetAggState*)DatumGetPointer(arg);
    /* In case of already been free */
    if (osa_state->sign != 'o') {
        return;
    }
    /* If avialable mem is not enough, use temp files to perform sorting. */
    if (osa_state->sortstate) {
        tuplesort_end(osa_state->sortstate);
    }
    osa_state->sortstate = NULL;
    UnregisterExprContextCallback(osa_state->aggstate->ss.ps.ps_ExprContext, ordered_set_shutdown, arg);
}
/*
 * Initialize tuplesort state and save into OrderedSetAggState
 */
static void tuplesort_state_init(
    OrderedSetAggState* osastate, SortGroupClause* sort_clause, MemoryContext agg_context, Plan* plan)
{
    Oid sort_col_type;
    Oid sort_operator;
    Oid eq_operator;
    Oid sort_collation;
    bool sort_nulls_first = false;
    Index sort_ref;
    TargetEntry* tle = NULL;
    List* aggs = NULL;
    Expr* func_expr = NULL;
    int64 local_work_mem = SET_NODEMEM(plan->operatorMemKB[0], plan->dop);
    MemoryContext oldcontext;

    sort_ref = sort_clause->tleSortGroupRef;
    aggs = osastate->aggref->args;
    tle = get_sortgroupref_tle(sort_ref, aggs);
    func_expr = tle->expr;

    /* Datatype info */
    sort_col_type = exprType((Node*)func_expr);
    /* Oid of sort operator */
    Assert(OidIsValid(sort_clause->sortop));
    sort_operator = sort_clause->sortop;
    /* Oid of equality operator associated with sort operator */
    eq_operator = sort_clause->eqop;
    sort_collation = exprCollation((Node*)func_expr);
    sort_nulls_first = sort_clause->nulls_first;

    /* Save regarding info into osastate */
    osastate->datumtype = sort_col_type;
    get_typlenbyvalalign(sort_col_type, &osastate->typLen, &osastate->typByVal, &osastate->typAlign);
    osastate->eq_operator = eq_operator;
    /* Sort state is needed in whole aggregate lifespan */
    oldcontext = MemoryContextSwitchTo(agg_context);
    /* Initialize tuplesort state */
    osastate->sortstate =
        tuplesort_begin_datum(sort_col_type, sort_operator, sort_collation, sort_nulls_first, local_work_mem, false);
    (void)MemoryContextSwitchTo(oldcontext);
}

/*
 * Initialize ordered-set aggregate state and regarding tuplesort object
 */
static OrderedSetAggState* ordered_set_startup(PG_FUNCTION_ARGS)
{
    Aggref* agg_ref = NULL;
    Plan* plan = NULL;
    List* sort_list = NIL;
    MemoryContext agg_context;
    AggState* agg_state = NULL;
    OrderedSetAggState* osa_state = NULL;
    SortGroupClause* sort_clause = NULL;

    /* Check be called in aggregate context; get the Agg node's query-lifespan context */
    if (AggCheckCallContext(fcinfo, &agg_context) != AGG_CONTEXT_AGGREGATE) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("ordered-set aggregate called in non-aggregate context")));
    }

    if (fcinfo->context == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("ordered_set_startup function context info is null")));
    }

    plan = ((AggState*)fcinfo->context)->ss.ps.plan;
    /* Need the agg info to perform sort */
    if (IsA(fcinfo->context, AggState)) {
        agg_state = (AggState*)fcinfo->context;
        AggStatePerAgg curperagg = agg_state->curperagg;
        if (curperagg != NULL) {
            agg_ref = curperagg->aggref;
        }
    }

    if (agg_ref == NULL || agg_state == NULL) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("ordered-set aggregate called in non-aggregate context")));
    }
    if (!AGGKIND_IS_ORDERED_SET(agg_ref->aggkind)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_STATUS),
                errmsg("ordered-set aggregate support function called for non-ordered-set aggregate")));
    }
    /* Initialize ordered-set agg state which be used in the whole aggregate lifespan */
    osa_state = (OrderedSetAggState*)MemoryContextAllocZero(agg_context, sizeof(OrderedSetAggState));
    osa_state->aggstate = agg_state;
    osa_state->sign = AGGKIND_ORDERED_SET;
    osa_state->aggref = agg_ref;

    /* Ordered-set aggregate only support order by one column, agg_ref contains it info */
    sort_list = agg_ref->aggorder;

    if (list_length(sort_list) != 1) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_AGG),
                errmsg("ordered-set aggregate support function does not support multiple aggregated columns")));
    }

    sort_clause = (SortGroupClause*)linitial(sort_list);
    tuplesort_state_init(osa_state, sort_clause, agg_context, plan);

    ExprContext* cxt = NULL;

    cxt = agg_state->ss.ps.ps_ExprContext;
    RegisterExprContextCallback(cxt, ordered_set_shutdown, PointerGetDatum(osa_state));
    return osa_state;
}

/*
 * Ordered-set aggregates' transition function which
 * put datum inside the tuplesort object
 */
Datum ordered_set_transition(PG_FUNCTION_ARGS)
{
    /* First argument of fcinfo is OrderedSetAggState, second is datumn */
    OrderedSetAggState* osa_state = NULL;
    Datum datum;
    /* Create the transition state workspace  */
    if (PG_ARGISNULL(0)) {
        osa_state = ordered_set_startup(fcinfo);
    } else {
        if (AggCheckCallContext(fcinfo, NULL) != AGG_CONTEXT_AGGREGATE) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_STATUS), errmsg("ordered-set aggregate called in non-aggregate context")));
        }
        osa_state = (OrderedSetAggState*)PG_GETARG_POINTER(0);
    }

    /* Put the not null datum into the tuplesort object */
    if (!PG_ARGISNULL(1)) {
        datum = PG_GETARG_DATUM(1);
        tuplesort_putdatum(osa_state->sortstate, datum, false);
        osa_state->number_of_rows++;
    }

    PG_RETURN_POINTER(osa_state);
}

static Datum interpolate(Datum lo, Datum hi, double pct, const char* interpolate_type)
{
    if (interpolate_type == PERCENTILE_FLOAT8) {
        double lo_val = DatumGetFloat8(lo);
        double hi_val = DatumGetFloat8(hi);

        return Float8GetDatum(lo_val + (pct * (hi_val - lo_val)));
    } else if (interpolate_type == PERCENTILE_INTERVAL) {
        Datum diff_result = DirectFunctionCall2(interval_mi, hi, lo);
        Datum mul_result = DirectFunctionCall2(interval_mul, diff_result, Float8GetDatumFast(pct));

        return DirectFunctionCall2(interval_pl, mul_result, lo);
    }
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("wrong interpolate type")));
    return lo;
}

static void parameters_check(FunctionCallInfo fcinfo)
{
    if (AggCheckCallContext(fcinfo, NULL) != AGG_CONTEXT_AGGREGATE) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("ordered-set aggregate called in non-aggregate context")));
    }
    if (PG_ARGISNULL(1)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("percentile cannot be NULL")));
    }
    if (PG_GETARG_FLOAT8(1) < 0 || PG_GETARG_FLOAT8(1) > 1 || isnan(PG_GETARG_FLOAT8(1))) {
        ereport(ERROR,
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("percentile value %g is not between 0 and 1", PG_GETARG_FLOAT8(1))));
    }
}
/*
 * Final function for percentile_cont
 */
static Datum percentile_cont_final_common(FunctionCallInfo fcinfo, Oid expect_type, const char* interpolate_type)
{
    OrderedSetAggState* osa_state = NULL;
    double percen_tile;
    int64 first_pos;
    int64 second_pos;
    int64 total_row;
    Datum val;
    Datum first_pos_val;
    Datum second_pos_val;
    double proportion;
    bool is_null = true;
    /* Parameters check */
    parameters_check(fcinfo);
    /* If no tuple had been fetched, the result is NULL */
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }
    osa_state = (OrderedSetAggState*)PG_GETARG_POINTER(0);
    Assert(expect_type == osa_state->datumtype);
    /* The basic info for compute percen_tile value */
    percen_tile = PG_GETARG_FLOAT8(1);
    total_row = osa_state->number_of_rows;
    if (total_row == 0) {
        PG_RETURN_NULL();
    }
    first_pos = floor(percen_tile * (total_row - 1));
    second_pos = ceil(percen_tile * (total_row - 1));
    proportion = (percen_tile * (total_row - 1)) - first_pos;
    Assert(first_pos < total_row);

    /* Perform the sort of all tuples */
    tuplesort_performsort(osa_state->sortstate);

    if (!tuplesort_skiptuples(osa_state->sortstate, first_pos, true)) {
        ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmsg("missing row in percentile_cont")));
    }
    if (!tuplesort_getdatum(osa_state->sortstate, true, &first_pos_val, &is_null)) {
        ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmsg("missing row in percentile_cont")));
    }
    if (is_null) {
        PG_RETURN_NULL();
    }

    val = first_pos_val;
    /* If first_pos not equal to second_pos, we need exact value in result set */
    if (first_pos != second_pos) {
        if (!tuplesort_getdatum(osa_state->sortstate, true, &second_pos_val, &is_null)) {
            ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmsg("missing row in percentile_cont")));
        }

        if (is_null) {
            PG_RETURN_NULL();
        }
        val = interpolate(first_pos_val, second_pos_val, proportion, interpolate_type);
    }
    PG_RETURN_DATUM(val);
}

/*
 * percentile_cont(float8) within group (float8)
 */
Datum percentile_cont_float8_final(PG_FUNCTION_ARGS)
{
    return percentile_cont_final_common(fcinfo, FLOAT8OID, PERCENTILE_FLOAT8);
}

/*
 * percentile_cont(float8) within group (interval)
 */
Datum percentile_cont_interval_final(PG_FUNCTION_ARGS)
{
    return percentile_cont_final_common(fcinfo, INTERVALOID, PERCENTILE_INTERVAL);
}
