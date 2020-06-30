/* -------------------------------------------------------------------------
 *
 * tsquery_gist.c
 *	  GiST index support for tsquery
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/tsquery_gist.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/skey.h"
#include "access/gist.h"
#include "tsearch/ts_utils.h"

#define GETENTRY(vec, pos) DatumGetTSQuerySign((vec)->vector[pos].key)

Datum gtsquery_compress(PG_FUNCTION_ARGS)
{
    GISTENTRY* entry = (GISTENTRY*)PG_GETARG_POINTER(0);
    GISTENTRY* ret_val = entry;
    if (entry->leafkey) {
        TSQuerySign sign;

        ret_val = (GISTENTRY*)palloc(sizeof(GISTENTRY));
        sign = makeTSQuerySign(DatumGetTSQuery(entry->key));

        gistentryinit(*ret_val, TSQuerySignGetDatum(sign), entry->rel, entry->page, entry->offset, FALSE);
    }

    PG_RETURN_POINTER(ret_val);
}

Datum gtsquery_decompress(PG_FUNCTION_ARGS)
{
    PG_RETURN_DATUM(PG_GETARG_DATUM(0));
}

Datum gtsquery_consistent(PG_FUNCTION_ARGS)
{
    GISTENTRY* entry = (GISTENTRY*)PG_GETARG_POINTER(0);
    TSQuery query = PG_GETARG_TSQUERY(1);
    StrategyNumber strategy = (StrategyNumber)PG_GETARG_UINT16(2);

    bool* recheck = (bool*)PG_GETARG_POINTER(4);
    TSQuerySign key = DatumGetTSQuerySign(entry->key);
    TSQuerySign sq = makeTSQuerySign(query);
    bool ret_val = false;

    /* All cases served by this function are inexact */
    *recheck = true;

    switch (strategy) {
        case RTContainsStrategyNumber:
            if (GIST_LEAF(entry)) {
                ret_val = (key & sq) == sq;
            } else {
                ret_val = (key & sq) != 0;
            }
            break;
        case RTContainedByStrategyNumber:
            if (GIST_LEAF(entry)) {
                ret_val = (key & sq) == key;
            } else {
                ret_val = (key & sq) != 0;
            }
            break;
        default:
            ret_val = FALSE;
            break;
    }
    PG_RETURN_BOOL(ret_val);
}

Datum gtsquery_union(PG_FUNCTION_ARGS)
{
    GistEntryVector* entry_vec = (GistEntryVector*)PG_GETARG_POINTER(0);
    int* size = (int*)PG_GETARG_POINTER(1);
    TSQuerySign sign = 0;
    for (int i = 0; i < entry_vec->n; i++) {
        sign |= GETENTRY(entry_vec, i);
    }

    *size = sizeof(TSQuerySign);

    PG_RETURN_TSQUERYSIGN(sign);
}

Datum gtsquery_same(PG_FUNCTION_ARGS)
{
    TSQuerySign a = PG_GETARG_TSQUERYSIGN(0);
    TSQuerySign b = PG_GETARG_TSQUERYSIGN(1);
    bool* result = (bool*)PG_GETARG_POINTER(2);

    *result = (a == b) ? true : false;

    PG_RETURN_POINTER(result);
}

static int size_bit_vec(TSQuerySign sign)
{
    int size = 0;
    for (int i = 0; (unsigned int)(i) < TSQS_SIGLEN; i++) {
        size += 0x01 & (sign >> i);
    }

    return size;
}

static int hem_dist(TSQuerySign a, TSQuerySign b)
{
    TSQuerySign res = a ^ b;
    return size_bit_vec(res);
}

Datum gtsquery_penalty(PG_FUNCTION_ARGS)
{
    TSQuerySign orig_val = DatumGetTSQuerySign(((GISTENTRY*)PG_GETARG_POINTER(0))->key);
    TSQuerySign new_val = DatumGetTSQuerySign(((GISTENTRY*)PG_GETARG_POINTER(1))->key);
    float* penalty = (float*)PG_GETARG_POINTER(2);

    *penalty = hem_dist(orig_val, new_val);

    PG_RETURN_POINTER(penalty);
}

typedef struct {
    OffsetNumber pos;
    int4 cost;
} SPLITCOST;

static int compare_cost(const void* a, const void* b)
{
    if (((const SPLITCOST*)a)->cost == ((const SPLITCOST*)b)->cost) {
        return 0;
    } else {
        return (((const SPLITCOST*)a)->cost > ((const SPLITCOST*)b)->cost) ? 1 : -1;
    }
}

#define WISH_F(a, b, c) (double)(-(double)(((a) - (b)) * ((a) - (b)) * ((a) - (b))) * (c))

Datum gtsquery_picksplit(PG_FUNCTION_ARGS)
{
    GistEntryVector* entry_vec = (GistEntryVector*)PG_GETARG_POINTER(0);
    GIST_SPLITVEC* v = (GIST_SPLITVEC*)PG_GETARG_POINTER(1);
    OffsetNumber max_off = entry_vec->n - 2;
    OffsetNumber k;
    OffsetNumber j;
    TSQuerySign datum_l;
    TSQuerySign datum_r;
    int4 size_alpha;
    int4 size_beta;
    int4 size_waste;
    int4 waste = -1;
    int4 n_bytes;
    OffsetNumber seed_1 = 0;
    OffsetNumber seed_2 = 0;
    OffsetNumber* left = NULL;
    OffsetNumber* right = NULL;
    SPLITCOST* cost_vector = NULL;

    n_bytes = (max_off + 2) * sizeof(OffsetNumber);
    left = v->spl_left = (OffsetNumber*)palloc(n_bytes);
    right = v->spl_right = (OffsetNumber*)palloc(n_bytes);
    v->spl_nleft = v->spl_nright = 0;

    for (k = FirstOffsetNumber; k < max_off; k = OffsetNumberNext(k))
        for (j = OffsetNumberNext(k); j <= max_off; j = OffsetNumberNext(j)) {
            size_waste = hem_dist(GETENTRY(entry_vec, j), GETENTRY(entry_vec, k));
            if (size_waste > waste) {
                waste = size_waste;
                seed_1 = k;
                seed_2 = j;
            }
        }

    if (seed_1 == 0 || seed_2 == 0) {
        seed_1 = 1;
        seed_2 = 2;
    }

    datum_l = GETENTRY(entry_vec, seed_1);
    datum_r = GETENTRY(entry_vec, seed_2);

    max_off = OffsetNumberNext(max_off);
    cost_vector = (SPLITCOST*)palloc(sizeof(SPLITCOST) * max_off);
    for (j = FirstOffsetNumber; j <= max_off; j = OffsetNumberNext(j)) {
        cost_vector[j - 1].pos = j;
        size_alpha = hem_dist(GETENTRY(entry_vec, seed_1), GETENTRY(entry_vec, j));
        size_beta = hem_dist(GETENTRY(entry_vec, seed_2), GETENTRY(entry_vec, j));
        cost_vector[j - 1].cost = abs(size_alpha - size_beta);
    }
    qsort((void*)cost_vector, max_off, sizeof(SPLITCOST), compare_cost);

    for (k = 0; k < max_off; k++) {
        j = cost_vector[k].pos;
        if (j == seed_1) {
            *left++ = j;
            v->spl_nleft++;
            continue;
        } else if (j == seed_2) {
            *right++ = j;
            v->spl_nright++;
            continue;
        }
        size_alpha = hem_dist(datum_l, GETENTRY(entry_vec, j));
        size_beta = hem_dist(datum_r, GETENTRY(entry_vec, j));
        if (size_alpha < size_beta + WISH_F(v->spl_nleft, v->spl_nright, 0.05)) {
            datum_l |= GETENTRY(entry_vec, j);
            *left++ = j;
            v->spl_nleft++;
        } else {
            datum_r |= GETENTRY(entry_vec, j);
            *right++ = j;
            v->spl_nright++;
        }
    }

    *right = *left = FirstOffsetNumber;
    v->spl_ldatum = TSQuerySignGetDatum(datum_l);
    v->spl_rdatum = TSQuerySignGetDatum(datum_r);

    PG_RETURN_POINTER(v);
}
