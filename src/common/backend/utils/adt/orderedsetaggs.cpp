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
#include "executor/node/nodeAgg.h"
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
    OrderedSetAggState* osastate = NULL;
    osastate = (OrderedSetAggState*)DatumGetPointer(arg);
    /* In case of already been free */
    if (osastate->sign != 'o')
        return;
    /* If avialable mem is not enough, use temp files to perform sorting. */
    if (osastate->sortstate)
        tuplesort_end(osastate->sortstate);
    osastate->sortstate = NULL;
    UnregisterExprContextCallback(osastate->aggstate->ss.ps.ps_ExprContext, ordered_set_shutdown, arg);
}
/*
 * Initialize tuplesort state and save into OrderedSetAggState
 */
static void tuplesort_state_init(
    OrderedSetAggState* osastate, SortGroupClause* sort_clause, MemoryContext aggcontext, Plan* plan)
{
    Oid sort_col_type;
    Oid sort_operator;
    Oid eq_operator;
    Oid sort_collation;
    bool sort_nulls_first = false;
    Index sortref;
    TargetEntry* tle = NULL;
    List* aggs = NULL;
    Expr* func_expr = NULL;
    int64 local_work_mem = SET_NODEMEM(plan->operatorMemKB[0], plan->dop);
    MemoryContext oldcontext;

    sortref = sort_clause->tleSortGroupRef;
    aggs = osastate->aggref->args;
    tle = get_sortgroupref_tle(sortref, aggs);
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
    oldcontext = MemoryContextSwitchTo(aggcontext);
    /* Initialize tuplesort state */
    osastate->sortstate =
        tuplesort_begin_datum(sort_col_type, sort_operator, sort_collation, sort_nulls_first, local_work_mem, false);
    MemoryContextSwitchTo(oldcontext);
}

/*
 * Initialize ordered-set aggregate state and regarding tuplesort object
 */
static OrderedSetAggState* ordered_set_startup(PG_FUNCTION_ARGS)
{
    Aggref* aggref = NULL;
    Plan* plan = NULL;
    List* sortlist = NIL;
    MemoryContext aggcontext;
    AggState* aggstate = NULL;
    OrderedSetAggState* osastate = NULL;
    SortGroupClause* sort_clause = NULL;

    /* Check be called in aggregate context; get the Agg node's query-lifespan context */
    if (AggCheckCallContext(fcinfo, &aggcontext) != AGG_CONTEXT_AGGREGATE)
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("ordered-set aggregate called in non-aggregate context")));

    if (fcinfo->context == NULL)
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("ordered_set_startup function context info is null")));

    plan = ((AggState*)fcinfo->context)->ss.ps.plan;
    /* Need the agg info to perform sort */
    if (IsA(fcinfo->context, AggState)) {
        aggstate = (AggState*)fcinfo->context;
        AggStatePerAgg curperagg = aggstate->curperagg;
        if (curperagg != NULL)
            aggref = curperagg->aggref;
    }

    if (aggref == NULL || aggstate == NULL)
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("ordered-set aggregate called in non-aggregate context")));
    if (!AGGKIND_IS_ORDERED_SET(aggref->aggkind))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_STATUS),
                errmsg("ordered-set aggregate support function called for non-ordered-set aggregate")));
    /* Initialize ordered-set agg state which be used in the whole aggregate lifespan */
    osastate = (OrderedSetAggState*)MemoryContextAllocZero(aggcontext, sizeof(OrderedSetAggState));
    osastate->aggstate = aggstate;
    osastate->sign = AGGKIND_ORDERED_SET;
    osastate->aggref = aggref;

    /* Ordered-set aggregate only support order by one column, aggref contains it info */
    sortlist = aggref->aggorder;

    if (list_length(sortlist) != 1)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_AGG),
                errmsg("ordered-set aggregate support function does not support multiple aggregated columns")));
    sort_clause = (SortGroupClause*)linitial(sortlist);
    tuplesort_state_init(osastate, sort_clause, aggcontext, plan);

    ExprContext* cxt = NULL;

    cxt = aggstate->ss.ps.ps_ExprContext;
    RegisterExprContextCallback(cxt, ordered_set_shutdown, PointerGetDatum(osastate));
    return osastate;
}

/*
 * Ordered-set aggregates' transition function which
 * put datum inside the tuplesort object
 */
Datum ordered_set_transition(PG_FUNCTION_ARGS)
{
    /* First argument of fcinfo is OrderedSetAggState, second is datumn */
    OrderedSetAggState* osastate = NULL;
    Datum datum;
    /* Create the transition state workspace  */
    if (PG_ARGISNULL(0))
        osastate = ordered_set_startup(fcinfo);
    else {
        if (AggCheckCallContext(fcinfo, NULL) != AGG_CONTEXT_AGGREGATE)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_STATUS), errmsg("ordered-set aggregate called in non-aggregate context")));
        osastate = (OrderedSetAggState*)PG_GETARG_POINTER(0);
    }

    /* Put the not null datum into the tuplesort object */
    if (!PG_ARGISNULL(1)) {
        datum = PG_GETARG_DATUM(1);
        tuplesort_putdatum(osastate->sortstate, datum, false);
        osastate->number_of_rows++;
    }

    PG_RETURN_POINTER(osastate);
}

static Datum interpolate(Datum lo, Datum hi, double pct, const char* interpolate_type)
{
    if (interpolate_type == PERCENTILE_FLOAT8) {
        double loval = DatumGetFloat8(lo);
        double hival = DatumGetFloat8(hi);

        return Float8GetDatum(loval + (pct * (hival - loval)));
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
    if (AggCheckCallContext(fcinfo, NULL) != AGG_CONTEXT_AGGREGATE)
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("ordered-set aggregate called in non-aggregate context")));
    if (PG_ARGISNULL(1))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("percentile cannot be NULL")));
    if (PG_GETARG_FLOAT8(1) < 0 || PG_GETARG_FLOAT8(1) > 1 || isnan(PG_GETARG_FLOAT8(1)))
        ereport(ERROR,
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("percentile value %g is not between 0 and 1", PG_GETARG_FLOAT8(1))));
}
/*
 * Final function for percentile_cont
 */
static Datum percentile_cont_final_common(FunctionCallInfo fcinfo, Oid expect_type, const char* interpolate_type)
{
    OrderedSetAggState* osastate = NULL;
    double percentile;
    int64 first_pos;
    int64 second_pos;
    int64 total_row;
    Datum val;
    Datum first_pos_val;
    Datum second_pos_val;
    double proportion;
    bool isnull = true;
    /* Parameters check */
    parameters_check(fcinfo);
    /* If no tuple had been fetched, the result is NULL */
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    osastate = (OrderedSetAggState*)PG_GETARG_POINTER(0);
    Assert(expect_type == osastate->datumtype);
    /* The basic info for compute percentile value */
    percentile = PG_GETARG_FLOAT8(1);
    total_row = osastate->number_of_rows;
    if (total_row == 0)
        PG_RETURN_NULL();
    first_pos = floor(percentile * (total_row - 1));
    second_pos = ceil(percentile * (total_row - 1));
    proportion = (percentile * (total_row - 1)) - first_pos;
    Assert(first_pos < total_row);

    /* Perform the sort of all tuples */
    tuplesort_performsort(osastate->sortstate);

    if (!tuplesort_skiptuples(osastate->sortstate, first_pos, true))
        ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmsg("missing row in percentile_cont")));

    if (!tuplesort_getdatum(osastate->sortstate, true, &first_pos_val, &isnull))
        ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmsg("missing row in percentile_cont")));
    if (isnull)
        PG_RETURN_NULL();

    val = first_pos_val;
    /* If first_pos not equal to second_pos, we need exact value in result set */
    if (first_pos != second_pos) {
        if (!tuplesort_getdatum(osastate->sortstate, true, &second_pos_val, &isnull))
            ereport(ERROR, (errcode(ERRCODE_NO_DATA_FOUND), errmsg("missing row in percentile_cont")));

        if (isnull)
            PG_RETURN_NULL();
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

/*
 * @description: get most common value from Tuplesortstate.
 * 
 * @param (IN):  osatate, OrderedSetAggState.
 * @param (IN):  equal_fn, equal function for the data type.
 * @param (IN):  fn_collation, collation for mode function.
 * @return: most common value.
 */
Datum get_mode_from_sort_state(OrderedSetAggState* osastate, FmgrInfo* equal_fn, Oid fn_collation, int64& mode_freq)
{
    bool is_null = true;
    bool last_val_is_mode = false;
    bool should_free = false;

    Datum val;
    Datum mode_val = 0;
    Datum last_val = 0;
    int64 last_val_freq = 0;

    should_free = !(osastate->typByVal);
    tuplesort_performsort(osastate->sortstate);

    /* Scan tuples and count frequencies */
    while (tuplesort_getdatum(osastate->sortstate, true, &val, &is_null)) {
        /* we don't expect any nulls, but ignore them if found */
        if (is_null)
            continue;

        if (last_val_freq == 0) {
            /* first nonnull value - it's the mode for now */
            mode_val = last_val = val;
            mode_freq = last_val_freq = 1;
            last_val_is_mode = true;
        } else if (DatumGetBool(FunctionCall2Coll(equal_fn, fn_collation, val, last_val))) {
            /* value equal to previous value, count it */
            if (last_val_is_mode)
                mode_freq++;    /* needn't maintain last_val_freq */
            else if (++last_val_freq > mode_freq) {
                /* last_val becomes new mode */
                if (should_free)
                    pfree(DatumGetPointer(mode_val));
                mode_val = last_val;
                mode_freq = last_val_freq;
                last_val_is_mode = true;
            }
            if (should_free)
                pfree(DatumGetPointer(val));
        } else {
            /* val should replace last_val */
            if (should_free && !last_val_is_mode)
                pfree(DatumGetPointer(last_val));
            last_val = val;
            last_val_freq = 1;
            last_val_is_mode = false;
        }

        CHECK_FOR_INTERRUPTS();
    }

    if (should_free && !last_val_is_mode)
        pfree(DatumGetPointer(last_val));

    PG_RETURN_DATUM(mode_val);
}

/*
 * @description: Mode() within group (anyelement).
 * The mode_final function is the final function of mode aggregate.
 * Perform sorting on osastate and then return most common value if input is not NULL.
 * 
 * @param: fcinfo, function info data.
 * @return: most common value.
 */
Datum mode_final(PG_FUNCTION_ARGS)
{
    OrderedSetAggState* osastate = NULL;
    FmgrInfo* equal_fn = NULL;
    Datum mode_val = 0;
    int64 mode_freq = 0;
    Oid collation;

    Assert(AggCheckCallContext(fcinfo, NULL) == AGG_CONTEXT_AGGREGATE);

    /* If there were no regular rows, the result is NULL */
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    osastate = (OrderedSetAggState*) PG_GETARG_POINTER(0);
    /* number_of_rows could be zero if we only saw NULL input values */
    if (osastate->number_of_rows == 0)
        PG_RETURN_NULL();
    
    /* Look up the equality function for the datatype */
    equal_fn = (FmgrInfo*)palloc0(sizeof(FmgrInfo));
    fmgr_info_cxt(get_opcode(osastate->eq_operator), equal_fn, CurrentMemoryContext);

    collation = PG_GET_COLLATION();
    mode_val = get_mode_from_sort_state(osastate, equal_fn, collation, mode_freq);
    
    if (equal_fn != NULL) {
        pfree(equal_fn);
    }

    if (mode_freq == 0) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_DATUM(mode_val);
    }
}

static void Merge(TdigestData *td);

/*
 *    add one point to TdigestData
 */

void TdAdd(TdigestData *td, double mean, int64 count)
{
    CentroidPoint cp;
    
    if ((td->merged_nodes + td->unmerged_nodes) == td->cap) {
        Merge(td);
    }
    
    cp.count = count;
    cp.mean = mean;
    td->nodes[td->merged_nodes + td->unmerged_nodes] = cp;
    td->unmerged_nodes++;
    td->unmerged_count += count;
}

/*
 *    comepare points when sort
 */

static int CompareNodes(const void *firstNode, const void *secondNode)
{
    CentroidPoint *firstCp = (CentroidPoint *)(firstNode);
    CentroidPoint *secondCp = (CentroidPoint *)(secondNode);
    if (firstCp->mean < secondCp->mean) {
        return -1;
    } else if (firstCp->mean > secondCp->mean) {
        return 1;
    } else {
        return 0;
    }
}

/*
 *    merge the points
 */

static void MergeAct(TdigestData *td, int num)
{
    int i = 1;
    int cur = 0;
    double denom = 0;
    double normalizer = 0;
    int64 CountSoFar = 0;
    double TotalCount = 0;
    TotalCount = td->merged_count + td->unmerged_count;
    denom = 2 * M_PI * TotalCount * log(TotalCount);
    normalizer = td->compression / denom;
    
    while (i < num) {
        double ProposedCount = td->nodes[cur].count + td->nodes[i].count;
        double z = ProposedCount * normalizer;
        double q0 = (double)CountSoFar / TotalCount;
        double q2 = ((double)CountSoFar + ProposedCount) / TotalCount;
        bool ShouldAdd = (z <= (q0 * (1 - q0))) && (z <= (q2 * (1 - q2)));
        if (ShouldAdd) {
            // add nodes[i] to nodes[cur]
            td->nodes[cur].count += td->nodes[i].count;
            double delta = td->nodes[i].mean - td->nodes[cur].mean;
            double WeightedDelta = (delta * (double)td->nodes[i].count) / (double)td->nodes[cur].count;
            td->nodes[cur].mean += WeightedDelta;
        } else {
            // don't add nodes[i] to nodes[cur] and move nodes[i] to nodes[cur+1]
            CountSoFar += td->nodes[cur].count;
            cur++;
            td->nodes[cur] = td->nodes[i];
        }
        if (cur != i) {
            // empty nodes[i]
            CentroidPoint cp;
            cp.count = 0;
            cp.mean = 0;
            td->nodes[i] = cp;
        }
        i++;
    }
    td->merged_nodes = cur + 1;
    td->merged_count = TotalCount;
}

static void Merge(TdigestData *td)
{
    int num = 0;

    if (td->unmerged_nodes == 0) {
        return;
    }
    // 1. sort all point
    num = td->merged_nodes + td->unmerged_nodes;
    qsort((void *)(td->nodes), num, sizeof(CentroidPoint), &CompareNodes);
    // 2. Go through all the points and merge
    MergeAct(td, num);
    // 3. set unmerged_nodes to 0
    td->unmerged_nodes = 0;
    td->unmerged_count = 0;
}

/*
 *    Calculate the percentile_of_value from all the points
 */

double CalQuantile(TdigestData *td, double val, int index, CentroidPoint *cp, double CountVal)
{
    if (val == cp->mean) {
        // 1.current point. If have the same number, take the middle numberTake the middle number
        double CountAtValue = cp->count;
        while (index < td->merged_nodes && td->nodes[index].mean == cp->mean) {
            CountAtValue += td->nodes[index].count;
            index++;
        }
        double res = (CountVal + (CountAtValue / 2)) / td->merged_count;
        return res;
    } else if (val > cp->mean) {
        // 2.biggest
        return 1;
    } else if (index == 0) {
        // 3.minimum
        return 0;
    }
    // 4.Interpolation calculation
    CentroidPoint *cpr = cp;
    CentroidPoint *cpl = cp - 1;
    CountVal -= ((double)cpl->count / 2);
    
    double m = (cpr->mean - cpl->mean) / ((double)cpl->count / 2 + (double)cpr->count / 2);
    double x = (val - cpl->mean) / m;
    double res = (CountVal + x) / td->merged_count;
    return res;
}

double TdQuantileOf(TdigestData *td, double val)
{
    double CountVal = 0;
    int i = 0;
    CentroidPoint *cp = NULL;
    double res = 0;
   
    Merge(td);
    if (td->merged_nodes == 0) {
        return NAN;
    }
     
    // find a value greater than val
    while (i < td->merged_nodes) {
        cp = &td->nodes[i];
        if (cp->mean >= val) {
            break;
        }
        CountVal += cp->count;
        i++;
    }
    i++;
    // compute result
    res =  CalQuantile(td, val, i, cp, CountVal);
    return res;
}

/*
 *    Calculate the value_of_percentile from all the points
 */
double CalValue(TdigestData *td, CentroidPoint *cp, double goal, double count, int index)
{
    double minright = 0.000000001;
    double minleft = -0.000000001;

    // 1.current point
    double DeltaK = goal - count - ((double)cp->count / 2);
    if (!(DeltaK > minright || DeltaK < minleft)) {
        return cp->mean;
    }
    // 2.biggest or minimum
    bool right = DeltaK > 0;
    if ((right && ((index + 1) == td->merged_nodes)) ||
        (!right && (index == 0))) {
        return cp->mean;
    }
    // 3.Interpolation calculation
    CentroidPoint *cpl;
    CentroidPoint *cpr;
    if (right) {
        cpl = cp;
        cpr = &td->nodes[index + 1];
        count += ((double)cpl->count / 2);
    } else {
        cpl = &td->nodes[index - 1];
        cpr = cp;
        count -= ((double)cpl->count / 2);
    }
    
    double x = goal - count;
    double m = (cpr->mean - cpl->mean) / ((double)cpl->count / 2 + (double)cpr->count / 2);
    return m * x + cpl->mean;
}

double TdValueAt(TdigestData *td, double q)
{
    int i = 0;
    double k = 0;
    double goal = 0;
    CentroidPoint *cp = NULL;
    double res = 0;

    Merge(td);
    if (td->merged_nodes == 0) {
        return NAN;
    }
    // find a count greater than cur_count
    goal = q * td->merged_count;
    while (i < td->merged_nodes) {
        cp = &td->nodes[i];
        if (k + cp->count > goal) {
            break;
        }
        k += cp->count;
        i++;
    }
    // compute result
    res = CalValue(td, cp, goal, k, i);
    return res;
}

/*
 * Inputs:   param 0-3
 *	param0:  Store results.
 *	param1:  scan data from dn and store param 1 in param 0
 *	param2:  value to calcute
 *	param3:  comperssion
 * Outputs:  param0
 *	param0:  Store results.
 */

Datum tdigest_merge(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (PG_ARGISNULL(0)) {
        if (PG_ARGISNULL(2))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("percentile cannot be NULL")));
        if (PG_ARGISNULL(1))
            PG_RETURN_NULL();

        /* This is the first non-null input. */
        double newval = PG_GETARG_FLOAT8(1);
        double compression = PG_GETARG_FLOAT8(3);
        if (compression <= 0 || compression > 500) {
            compression = 300;
        }
        int compressNum = 6;
        int compressAdd = 10;
        Size memsize = sizeof(TdigestData) +
            (((compressNum * (int)(compression)) + compressAdd) * sizeof(CentroidPoint));
        TdigestData *res = (TdigestData *)(palloc0(memsize));
        if (!res) {
            ereport(ERROR, (errmodule(MOD_OPT_AGG), errcode(ERRCODE_OUT_OF_MEMORY),
                errmsg("Failed to apply for memory"), errdetail("N/A"),
                errcause("palloc failed"),
                erraction("Check memory")));
            PG_RETURN_NULL();
        }
        SET_VARSIZE(res, memsize);
        res->compression = compression;
        res->cap = (memsize - sizeof(TdigestData)) / sizeof(CentroidPoint);

        res->valuetoc = PG_GETARG_FLOAT8(2);

        TdAdd(res, newval, 1);
        PG_RETURN_POINTER(res);
    } else {
        TdigestData* oldres = (TdigestData*)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
        /* Leave res unchanged if new input is null. */
        if (PG_ARGISNULL(1)) {
            PG_RETURN_POINTER(oldres);
        }
        /* OK to do the addition. */
        TdAdd(oldres, PG_GETARG_FLOAT8(1), 1);
        PG_RETURN_POINTER(oldres);
    }
#else
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Distributed support only")));
    PG_RETURN_NULL();
#endif
}

/*
 * Inputs:   param 0-1
 *	param0:  Store results.
 *	param1:  TdigestData from dn and merge param 1 in param 0
 * Outputs:   param 0
 *	param0:  Store results.
 */

Datum tdigest_merge_to_one(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0)) {
        if (PG_ARGISNULL(1))
            PG_RETURN_NULL();
        // get data from first dn
        TdigestData* newval = (TdigestData*)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));
        PG_RETURN_POINTER(newval);
    } else {
        // get data from next dn
        TdigestData* oldres = (TdigestData*)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
        if (PG_ARGISNULL(1)) {
            PG_RETURN_POINTER(oldres);
        }
        TdigestData* newval = (TdigestData*)PG_DETOAST_DATUM(PG_GETARG_DATUM(1));
        // merge data from dn to param 0
        Merge(oldres);
        Merge(newval);
        int i = 0;
        while (i < newval->merged_nodes) {
            CentroidPoint *cp = &newval->nodes[i];
            TdAdd(oldres, cp->mean, cp->count);
            i++;
        }
        PG_RETURN_POINTER(oldres);
    }
}

/*
 * Final function for percentile_of_value
 */
 
Datum calculate_quantile_of(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    // get final TdigestData
    TdigestData* newval = (TdigestData*)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    double value = newval->valuetoc;
    // start calculate percentile
    double res = TdQuantileOf(newval, value);
    PG_RETURN_FLOAT8(res);
}

/*
 * Inputs:   param 0-3
 *	param0:  Store results.
 *	param1:  scan data from dn and store param 1 in param 0
 *	param2:  value to calcute
 *	param3:  comperssion
 * Outputs:   param 0
 *	param0:  Store results.
 */
 
Datum tdigest_mergep(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (PG_ARGISNULL(0)) {
        if (PG_ARGISNULL(2))
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("percentile cannot be NULL")));
        if (PG_GETARG_FLOAT8(2) < 0 || PG_GETARG_FLOAT8(2) > 1 || isnan(PG_GETARG_FLOAT8(2)))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("percentile value %g is not between 0 and 1", PG_GETARG_FLOAT8(2))));
        if (PG_ARGISNULL(1))
            PG_RETURN_NULL();

        double newval = PG_GETARG_FLOAT8(1);
        double compression = PG_GETARG_FLOAT8(3);
        if (compression <= 0 || compression > 500) {
            compression = 300;
        }
        int compressNum = 6;
        int compressAdd = 10;
        Size memsize = sizeof(TdigestData) +
            (((compressNum * (int)(compression)) + compressAdd) * sizeof(CentroidPoint));
        TdigestData *res = (TdigestData *)(palloc0(memsize));
        if (!res) {
            ereport(ERROR, (errmodule(MOD_OPT_AGG), errcode(ERRCODE_OUT_OF_MEMORY),
                errmsg("Failed to apply for memory"), errdetail("N/A"),
                errcause("palloc failed"),
                erraction("Check memory")));
            PG_RETURN_NULL();
        }
        SET_VARSIZE(res, memsize);
        res->compression = compression;
        res->cap = (memsize - sizeof(TdigestData)) / sizeof(CentroidPoint);
        
        res->valuetoc = PG_GETARG_FLOAT8(2);
        /* add one point */
        TdAdd(res, newval, 1);
        PG_RETURN_POINTER(res);
    } else {
        TdigestData* oldres = (TdigestData*)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
        /* Leave res unchanged if new input is null. */
        if (PG_ARGISNULL(1)) {
            PG_RETURN_POINTER(oldres);
        }
        /* add one point */
        TdAdd(oldres, PG_GETARG_FLOAT8(1), 1);
        PG_RETURN_POINTER(oldres);
    }
#else
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Distributed support only")));
    PG_RETURN_NULL();
#endif
}

/*
 * Final function for value_of_percentile
 */
 
Datum calculate_value_at(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    TdigestData* newval = (TdigestData*)PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    double percentile = newval->valuetoc;
    // start calculate value
    double res = TdValueAt(newval, percentile);
    PG_RETURN_FLOAT8(res);
}
