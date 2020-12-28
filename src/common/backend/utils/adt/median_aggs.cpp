/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * median_aggs.cpp
 * Aggregate for computing the statistical median
 *
 * IDENTIFICATION
 * 	  src/common/backend/utils/adt/median_aggs.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/median_aggs.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

/*
 * working state for median aggregation
 */
typedef struct MedianBuildState {
    MemoryContext mcontext; /* where all the temp stuff is kept */
    Datum* dvalues;         /* array of accumulated Datums */
    uint32 maxlen;          /* allocated length of above arrays */
    uint32 nelems;          /* number of valid entries in above arrays */
    Oid dtype;              /* data type of the Datums */
    int16 typlen;           /* needed info about datatype */
    bool typbyval;
    char typalign;
} MedianBuildState;

static MedianBuildState* CreateMedianBuildState(Oid elemType, MemoryContext aggCtx)
{
    MemoryContext medianCtx, oldCtx;

    /* Make a temporary context to hold all the junk */
    medianCtx = AllocSetContextCreate(aggCtx, "AccumMedianSet", ALLOCSET_DEFAULT_MINSIZE, 
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    oldCtx = MemoryContextSwitchTo(medianCtx);

    MedianBuildState* mstate = (MedianBuildState*)palloc(sizeof(MedianBuildState));
    mstate->mcontext = medianCtx;
    mstate->maxlen = 64; /* starting size */
    mstate->dvalues = (Datum*)palloc(mstate->maxlen * sizeof(Datum));
    mstate->nelems = 0;
    mstate->dtype = elemType;
    get_typlenbyvalalign(elemType, &mstate->typlen, &mstate->typbyval, &mstate->typalign);

    (void)MemoryContextSwitchTo(oldCtx);

    return mstate;
}

/*
 * Putting an element into working state
 */
static void MedianPutDatum(MedianBuildState* mstate, Datum dvalue)
{
    MemoryContext oldCtx = MemoryContextSwitchTo(mstate->mcontext);

    /* enlarge dvalues[] if needed */
    if (mstate->nelems >= mstate->maxlen) {
        mstate->maxlen *= 2;
        mstate->dvalues = (Datum*)repalloc(mstate->dvalues, mstate->maxlen * sizeof(Datum));
    }

    /*
     * Ensure pass-by-ref stuff is copied into mcontext; and detoast it too if it's varlena. 
     */
    if (!mstate->typbyval) {
        if (mstate->typlen == -1) {
            dvalue = PointerGetDatum(PG_DETOAST_DATUM_COPY(dvalue));
        } else {
            dvalue = datumCopy(dvalue, mstate->typbyval, mstate->typlen);
        }
    }

    mstate->dvalues[mstate->nelems] = dvalue;
    mstate->nelems++;

    (void)MemoryContextSwitchTo(oldCtx);
}


/*
 * MEDIAN aggregate function
 */
Datum median_transfn(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("median is not yet supported.")));
#endif

    Oid arg1Typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
    if (arg1Typeid == InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("could not determine input data type")));
    }

    /* Get the MemoryContext to keep the working state */
    MemoryContext aggCtx; 
    if (!AggCheckCallContext(fcinfo, &aggCtx)) {
        /* cannot be called directly because of internal-type argument */
        ereport(ERROR,
            (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("median_transfn called in non-aggregate context")));
    }

    MedianBuildState* mstate = NULL;
    if (PG_ARGISNULL(0)) {
        /* Create the transition state workspace  */
        mstate = CreateMedianBuildState(arg1Typeid, aggCtx);
    } else {
        mstate = (MedianBuildState*)PG_GETARG_POINTER(0);
    }

    /* skip the null values */
    if (PG_ARGISNULL(1)) {
        PG_RETURN_POINTER(mstate);
    }

    /* the datatype must be matched */
    Assert(mstate->dtype == arg1Typeid);

    Datum elem = PG_GETARG_DATUM(1);
    MedianPutDatum(mstate, elem);

    PG_RETURN_POINTER(mstate);
}

extern int float8_cmp_internal(float8 a, float8 b);

/* The comparison function for sorting an array of FLOAT8 datums */
static int datum_float8_cmp(const void* arg1, const void* arg2)
{
    return float8_cmp_internal(*(const float8*)arg1, *(const float8*)arg2);
}

static const double HALF_FACTOR = 0.5;

/*
 * the final function for median(float8)
 */
Datum median_float8_finalfn(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("median is not yet supported.")));
#endif

    Datum result;

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL(); /* returns null if no input values */
    }

    /* cannot be called directly because of internal-type argument */
    Assert(AggCheckCallContext(fcinfo, NULL));

    MedianBuildState* mstate = (MedianBuildState*)PG_GETARG_POINTER(0);
    /* If no element had been found, the result is NULL */
    if (mstate->nelems == 0) {
        PG_RETURN_NULL();
    }

    Assert(mstate->dtype == FLOAT8OID);
    Assert(mstate->typlen == sizeof(float8));

    /* sort all elements and find the median */
    qsort(mstate->dvalues, mstate->nelems, sizeof(Datum), datum_float8_cmp);

    uint32 i = mstate->nelems / 2;
    if (mstate->nelems % 2 == 1) {
        result = mstate->dvalues[i];
    } else {
        double low = DatumGetFloat8(mstate->dvalues[i - 1]);
        double high = DatumGetFloat8(mstate->dvalues[i]);
        result = Float8GetDatum(low + (high - low) * HALF_FACTOR);
    }

    PG_RETURN_DATUM(result);
}


/* The comparison function for sorting an array of INTERVAL datums. Since the
 * INTERVAL type is pass-by-ref, thus the arguments passing to this function 
 * actually are Interval** */
static int datum_interval_cmp(const void* arg1, const void* arg2)
{
    Interval** itvl1 = (Interval**)arg1;
    Interval** itvl2 = (Interval**)arg2;
    return interval_cmp_internal(*itvl1, *itvl2);
}

/*
 * the final function for median(interval)
 */
Datum median_interval_finalfn(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("median is not yet supported.")));
#endif

    /* returns null if no input values */
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL(); 
    }

    /* cannot be called directly because of internal-type argument */
    Assert(AggCheckCallContext(fcinfo, NULL) != 0);

    MedianBuildState* mstate = (MedianBuildState*)PG_GETARG_POINTER(0);
    /* If no element had been found, the result is NULL */
    if (mstate->nelems == 0) {
        PG_RETURN_NULL();
    }

    Assert(mstate->dtype == INTERVALOID);
    Assert(mstate->typlen == sizeof(Interval));
    Assert(!mstate->typbyval);  /* INTERVAL is passed by reference */

    /* sort all elements and find the median */
    qsort(mstate->dvalues, mstate->nelems, sizeof(Datum), datum_interval_cmp);

    Datum result;
    uint32 i = mstate->nelems / 2;
    if (mstate->nelems % 2 == 1) {
        result = mstate->dvalues[i];
    } else {
        Datum low = mstate->dvalues[i - 1];
        Datum high = mstate->dvalues[i];

        /* compute the result by LOW + (HIGH - LOW) * 0.5 */
        Datum diff = DirectFunctionCall2(interval_mi, high, low);
        Datum halfdiff = DirectFunctionCall2(interval_mul, diff, Float8GetDatumFast(HALF_FACTOR));

        result = DirectFunctionCall2(interval_pl, halfdiff, low);
    }

    PG_RETURN_DATUM(result);
}
