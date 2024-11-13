#include <cmath>
#include <cstdlib>
#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/elog.h"
#include "catalog/pg_type.h"
#include "utils/corr_sk.h"

#undef gettext
#undef dgettext
#undef ngettext
#undef dngettext
#include <boost/math/distributions/students_t.hpp>

constexpr int INIT_CORR_ARRAY_LENGTH = 64;
constexpr int MAX_CORR_ARRAY_LENGTH = 524288;

constexpr float HALF = 0.5;

enum class ModeType {
    COEFFICIENT,
    ONE_SIDED_SIG,
    ONE_SIDED_SIG_POS,
    ONE_SIDED_SIG_NEG,
    TWO_SIDED_SIG,
    ILLEGAL
};

typedef struct CorrBuildState {
    MemoryContext mcontext;      /* where all the temp stuff is kept */
    Datum* x_data_array;         /* array of accumulated Datums x */
    Datum* y_data_array;         /* array of accumulated Datums y */
    uint32 maxlen;              /* allocated length of above arrays */
    uint32 count;               /* number of valid entries in above arrays */
    Oid dtype;                  /* data type of the Datums */
    int16 typlen;               /* needed info about datatype */
    bool typbyval;
    char typalign;
    ModeType mode;              /* result mode */
} CorrBuildState;


// convert text to cstring, then to ModeType
ModeType parse_mode_type(text* mode_text)
{
    char *mode = text_to_cstring(mode_text);
    if (strcmp(mode, "COEFFICIENT") == 0) {
        return ModeType::COEFFICIENT;
    } else if (strcmp(mode, "ONE_SIDED_SIG") == 0) {
        return ModeType::ONE_SIDED_SIG;
    } else if (strcmp(mode, "ONE_SIDED_SIG_POS") == 0) {
        return ModeType::ONE_SIDED_SIG_POS;
    } else if (strcmp(mode, "ONE_SIDED_SIG_NEG") == 0) {
        return ModeType::ONE_SIDED_SIG_NEG;
    } else if (strcmp(mode, "TWO_SIDED_SIG") == 0) {
        return ModeType::TWO_SIDED_SIG;
    } else {
        return ModeType::ILLEGAL;
    }
}

static CorrBuildState* CreateCorrBuildState(Oid elemType, MemoryContext aggCtx)
{
    MemoryContext corrCtx, oldCtx;

    /* Make a temporary context to hold all the junk */
    corrCtx = AllocSetContextCreate(aggCtx, "AccumCorrSet", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    oldCtx = MemoryContextSwitchTo(corrCtx);

    CorrBuildState* cstate = (CorrBuildState*)palloc(sizeof(CorrBuildState));
    cstate->mcontext = corrCtx;
    cstate->maxlen = INIT_CORR_ARRAY_LENGTH; /* starting size */
    cstate->x_data_array = (Datum*)palloc(cstate->maxlen * sizeof(Datum));
    cstate->y_data_array = (Datum*)palloc(cstate->maxlen * sizeof(Datum));
    cstate->count = 0;
    cstate->dtype = elemType;
    cstate->mode = ModeType::COEFFICIENT;
    get_typlenbyvalalign(elemType, &cstate->typlen, &cstate->typbyval, &cstate->typalign);

    (void)MemoryContextSwitchTo(oldCtx);

    return cstate;
}

static void CorrPutDatum(CorrBuildState* cstate, Datum xvalue, Datum yvalue)
{
    MemoryContext oldCtx = MemoryContextSwitchTo(cstate->mcontext);

    /* enlarge data_array[] if needed */
    if (cstate->count >= cstate->maxlen) {
        if (cstate->maxlen < MAX_CORR_ARRAY_LENGTH) {
            cstate->maxlen *= 2;
            if (cstate->maxlen > MAX_CORR_ARRAY_LENGTH) {
                cstate->maxlen = MAX_CORR_ARRAY_LENGTH;
            }
            cstate->x_data_array = (Datum*)repalloc(cstate->x_data_array, cstate->maxlen * sizeof(Datum));
            cstate->y_data_array = (Datum*)repalloc(cstate->y_data_array, cstate->maxlen * sizeof(Datum));

        } else {
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("correlation array length limit exceeded")));
        }
    }

    /* Ensure pass-by-ref stuff is copied into mcontext; and detoast it too if it's varlena. */
    if (!cstate->typbyval) {
        if (cstate->typlen == -1) {
            xvalue = PointerGetDatum(PG_DETOAST_DATUM_COPY(xvalue));
            yvalue = PointerGetDatum(PG_DETOAST_DATUM_COPY(yvalue));
        } else {
            xvalue = datumCopy(xvalue, cstate->typbyval, cstate->typlen);
            yvalue = datumCopy(yvalue, cstate->typbyval, cstate->typlen);
        }
    }
    
    cstate->x_data_array[cstate->count] = xvalue;
    cstate->y_data_array[cstate->count] = yvalue;
    cstate->count++;

    (void)MemoryContextSwitchTo(oldCtx);
}

// General initialization and state handling for transition function
static CorrBuildState* init_corr_sk_trans_fn(PG_FUNCTION_ARGS, bool check_mode)
{
    Oid arg1Typeid = get_fn_expr_argtype(fcinfo->flinfo, 1);
    if (arg1Typeid == InvalidOid) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("could not determine input data type")));
    }

    /* Get the MemoryContext to keep the working state */
    MemoryContext aggCtx;
    if (!AggCheckCallContext(fcinfo, &aggCtx)) {
        ereport(ERROR,
            (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("function called in non-aggregate context")));
    }

    CorrBuildState *cstate = NULL;
    if (PG_ARGISNULL(0)) {
        /* Create the transition state workspace */
        cstate = CreateCorrBuildState(arg1Typeid, aggCtx);
    } else {
        cstate = (CorrBuildState*)PG_GETARG_POINTER(0);
    }

    /* Skip null values */
    if (PG_ARGISNULL(1) || PG_ARGISNULL(2)) {
        return cstate;
    }

    /* Check the datatype consistency */
    Assert(cstate->dtype == arg1Typeid);

    float8 xvalue = PG_GETARG_FLOAT8(1);
    float8 yvalue = PG_GETARG_FLOAT8(2);

    CorrPutDatum(cstate, xvalue, yvalue);

    if (check_mode) {
        if (PG_ARGISNULL(3)) {
            ereport(ERROR, (errmsg("illegal argument for function")));
        }
        
        text *mode_text = PG_GETARG_TEXT_P(3);
        // text -> ModeType
        ModeType mode = parse_mode_type(mode_text);
        cstate->mode = mode;
    }

    return cstate;
}

// Handle no 3rd arg case
Datum corr_sk_trans_fn_no3(PG_FUNCTION_ARGS)
{
    CorrBuildState *cstate = init_corr_sk_trans_fn(fcinfo, false);
    PG_RETURN_POINTER(cstate);
}

// Handle with 3rd arg case
Datum corr_sk_trans_fn(PG_FUNCTION_ARGS)
{
    CorrBuildState *cstate = init_corr_sk_trans_fn(fcinfo, true);
    PG_RETURN_POINTER(cstate);
}


struct DataWithIndex {
    Datum value;
    uint32 index;
};

/* The comparison function for sorting DataWithIndex */
int compare(const void *a, const void *b) {
    DataWithIndex *dp1 = (DataWithIndex *)a;
    DataWithIndex *dp2 = (DataWithIndex *)b;
    
    if (dp1->value < dp2->value) return -1;
    if (dp1->value > dp2->value) return 1;
    return 0;
}

void calculate_ranks(Datum data[], float8 ranks[], uint32 n)
{
    struct DataWithIndex *sortedData = (struct DataWithIndex *)malloc(n * sizeof(struct DataWithIndex));

    for (uint32 i = 0; i < n; i++) {
        sortedData[i].value = data[i];
        sortedData[i].index = i;
    }

    qsort(sortedData, n, sizeof(struct DataWithIndex), compare);

    for (uint32 i = 0; i < n; i++) {
        ranks[sortedData[i].index] = (float8)(i + 1);
    }

    for (uint32 i = 0; i < n;) {
        uint32 j = i + 1;
        while (j < n && sortedData[j].value == sortedData[i].value) {
            j++;
        }
        if (j > i + 1) {
            float8 avg_rank = 0.5 * (ranks[sortedData[i].index] + ranks[sortedData[j - 1].index]);
            for (uint32 k = i; k < j; k++) {
                ranks[sortedData[k].index] = avg_rank;
            }
        }
        i = j;
    }

    free(sortedData);
}

float8 calculate_t_statistic(float8 correlation, uint32 n)
{
    return correlation * sqrt((n - 2) / (1 - correlation * correlation));
}

float8 calculate_z_statistic(float8 tau_b, uint32 n)
{
    float8 denominator = sqrt((float8)(2 * (2 * n + 5)) / (float8)(9 * n * (n - 1)));
    return tau_b / denominator;
}

float8 calculate_standard_deviation(const float8 data[], uint32 size, float8 mean)
{
    float8 sumOfSquares = 0.0;
    for (uint32 i = 0; i < size; i++) {
        sumOfSquares += (data[i] - mean) * (data[i] - mean);
    }
    return sqrt(sumOfSquares / (size - 1));
}

Datum corr_s_final_fn(PG_FUNCTION_ARGS)
{
    float8 result;

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL(); /* returns null if no input values */
    }

    CorrBuildState *state = (CorrBuildState *) PG_GETARG_POINTER(0);

    if (state->count == 0) {
        PG_RETURN_NULL();
    }

    uint32 n = state->count;
    Datum* x_data = state->x_data_array;
    Datum* y_data = state->y_data_array;

    float8* x_ranks = (float8*)palloc(n * sizeof(float8));
    float8* y_ranks = (float8*)palloc(n * sizeof(float8));
    float8* x_rank_m = (float8*)palloc(n * sizeof(float8));
    float8* y_rank_m = (float8*)palloc(n * sizeof(float8));
    float8* diff = (float8*)palloc(n * sizeof(float8));

    calculate_ranks(x_data, x_ranks, n);
    calculate_ranks(y_data, y_ranks, n);

    // Spearman's correlation coefficient
    float8 mean_rank = ((float8)((1 + n) * n) / 2.0) / n;
    float8 sum_diff = 0.0;
    
    for (uint32 i = 0; i < n; i++) {
        x_rank_m[i] = x_ranks[i] - mean_rank;
        y_rank_m[i] = y_ranks[i] - mean_rank;
        diff[i] = x_rank_m[i] * y_rank_m[i];
        sum_diff += diff[i];
    }
    float8 covariance = sum_diff / (float8)(n - 1);
    float8 xr_st_dev = calculate_standard_deviation(x_ranks, n, mean_rank);
    float8 yr_st_dev = calculate_standard_deviation(y_ranks, n, mean_rank);
    float8 spearman_rho_corr = covariance / (xr_st_dev * yr_st_dev);

    // T-statistic
    float8 t_stat = calculate_t_statistic(spearman_rho_corr, n);

    // T-distribution
    float8 df = n - 2;
    boost::math::students_t_distribution<float8> t_dist(df);

    float8 one_sided_p_value_pos = 1 - boost::math::cdf(t_dist, t_stat);
    float8 one_sided_p_value_neg = 1 - one_sided_p_value_pos;
    float8 one_sided_p_value;
    if (one_sided_p_value_pos < HALF) {
        one_sided_p_value = one_sided_p_value_pos;
    } else {
        one_sided_p_value = one_sided_p_value_neg;
    }
    float8 two_sided_p_value = 2 * one_sided_p_value;

    pfree(x_ranks);
    pfree(y_ranks);
    pfree(x_rank_m);
    pfree(y_rank_m);
    pfree(diff);

    ModeType mode = state->mode;
    if (mode == ModeType::COEFFICIENT) {
        result = spearman_rho_corr;
    } else if (mode == ModeType::ONE_SIDED_SIG || mode == ModeType::ONE_SIDED_SIG_POS) {
        result = one_sided_p_value_pos;
    } else if (mode == ModeType::ONE_SIDED_SIG_NEG) {
        result = one_sided_p_value_neg;
    } else if (mode == ModeType::TWO_SIDED_SIG) {
        result = two_sided_p_value;
    } else { // mode == ModeType::ILLEGAL
        ereport(ERROR, (errmsg("illegal argument for function")));
    }

    PG_RETURN_DATUM(Float8GetDatum(result));
}


Datum corr_k_final_fn(PG_FUNCTION_ARGS)
{
    float8 result;

    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL(); /* returns null if no input values */
    }
    
    CorrBuildState *state = (CorrBuildState *) PG_GETARG_POINTER(0);

    if (state->count == 0) {
        PG_RETURN_NULL();
    }

    uint32 n = state->count;
    Datum* x_data = state->x_data_array;
    Datum* y_data = state->y_data_array;

    float8* x_ranks = (float8*)palloc(n * sizeof(float8));
    float8* y_ranks = (float8*)palloc(n * sizeof(float8));

    calculate_ranks(x_data, x_ranks, n);
    calculate_ranks(y_data, y_ranks, n);

    // Kendall's tau-b correlation coefficient
    int concordant = 0;
    int discordant = 0;
    int tiedX = 0;
    int tiedY = 0;

    for (uint32 i = 0; i < n - 1; i++) {
        for (uint32 j = i + 1; j < n; j++) {
            if ((x_ranks[i] > x_ranks[j] && y_ranks[i] > y_ranks[j]) || (x_ranks[i] < x_ranks[j] && y_ranks[i] < y_ranks[j])) {
                concordant++;
            } else if ((x_ranks[i] > x_ranks[j] && y_ranks[i] < y_ranks[j]) || (x_ranks[i] < x_ranks[j] && y_ranks[i] > y_ranks[j])) {
                discordant++;
            }
            if (x_ranks[i] == x_ranks[j]) tiedX++;
            if (y_ranks[i] == y_ranks[j]) tiedY++;
        }
    }

    uint32 n0 = n * (n - 1) / 2;
    float8 tau_b = (float8)(concordant - discordant) / sqrt((n0 - tiedX) * (n0 - tiedY));

    // z-statistic
    float8 z_stat = calculate_z_statistic(tau_b, n);

    // Normal distribution
    boost::math::normal_distribution<> normal_dist(0.0, 1.0);
    float8 one_sided_p_value_pos = 1.0 - boost::math::cdf(normal_dist, z_stat);
    float8 one_sided_p_value_neg = boost::math::cdf(normal_dist, z_stat);
    float8 one_sided_p_value;
    if (one_sided_p_value_pos < HALF) {
        one_sided_p_value = one_sided_p_value_pos;
    } else {
        one_sided_p_value = one_sided_p_value_neg;
    }
    float8 two_sided_p_value = 2 * one_sided_p_value;

    pfree(x_ranks);
    pfree(y_ranks);

    ModeType mode = state->mode;
    if (mode == ModeType::COEFFICIENT) {
        result = tau_b;
    } else if (mode == ModeType::ONE_SIDED_SIG || mode == ModeType::ONE_SIDED_SIG_POS) {
        result = one_sided_p_value_pos;
    } else if (mode == ModeType::ONE_SIDED_SIG_NEG) {
        result = one_sided_p_value_neg;
    } else if (mode == ModeType::TWO_SIDED_SIG) {
        result = two_sided_p_value;
    } else { // mode == ModeType::ILLEGAL
        ereport(ERROR, (errmsg("illegal argument for function")));
    }

    PG_RETURN_DATUM(Float8GetDatum(result));
}