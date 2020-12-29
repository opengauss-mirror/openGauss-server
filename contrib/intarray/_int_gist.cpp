/*
 * contrib/intarray/_int_gist.c
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/gist.h"
#include "access/skey.h"

#include "_int.h"

#define GETENTRY(vec, pos) ((ArrayType*)DatumGetPointer((vec)->vector[(pos)].key))

/*
** GiST support methods
*/
PG_FUNCTION_INFO_V1(g_int_consistent);
PG_FUNCTION_INFO_V1(g_int_compress);
PG_FUNCTION_INFO_V1(g_int_decompress);
PG_FUNCTION_INFO_V1(g_int_penalty);
PG_FUNCTION_INFO_V1(g_int_picksplit);
PG_FUNCTION_INFO_V1(g_int_union);
PG_FUNCTION_INFO_V1(g_int_same);

extern "C" Datum g_int_consistent(PG_FUNCTION_ARGS);
extern "C" Datum g_int_compress(PG_FUNCTION_ARGS);
extern "C" Datum g_int_decompress(PG_FUNCTION_ARGS);
extern "C" Datum g_int_penalty(PG_FUNCTION_ARGS);
extern "C" Datum g_int_picksplit(PG_FUNCTION_ARGS);
extern "C" Datum g_int_union(PG_FUNCTION_ARGS);
extern "C" Datum g_int_same(PG_FUNCTION_ARGS);

/*
** The GiST Consistent method for _intments
** Should return false if for all data items x below entry,
** the predicate x op query == FALSE, where op is the oper
** corresponding to strategy in the pg_amop table.
*/
Datum g_int_consistent(PG_FUNCTION_ARGS)
{
    GISTENTRY* entry = (GISTENTRY*)PG_GETARG_POINTER(0);
    ArrayType* query = PG_GETARG_ARRAYTYPE_P_COPY(1);
    StrategyNumber strategy = (StrategyNumber)PG_GETARG_UINT16(2);

    bool* recheck = (bool*)PG_GETARG_POINTER(4);
    bool retval = false;

    /* this is exact except for RTSameStrategyNumber */
    *recheck = (strategy == RTSameStrategyNumber);

    if (strategy == BooleanSearchStrategy) {
        retval = execconsistent((QUERYTYPE*)query, (ArrayType*)DatumGetPointer(entry->key), GIST_LEAF(entry));

        pfree(query);
        PG_RETURN_BOOL(retval);
    }

    /* sort query for fast search, key is already sorted */
    CHECKARRVALID(query);
    PREPAREARR(query);

    switch (strategy) {
        case RTOverlapStrategyNumber:
            retval = inner_int_overlap((ArrayType*)DatumGetPointer(entry->key), query);
            break;
        case RTSameStrategyNumber:
            if (GIST_LEAF(entry))
                DirectFunctionCall3(g_int_same, entry->key, PointerGetDatum(query), PointerGetDatum(&retval));
            else
                retval = inner_int_contains((ArrayType*)DatumGetPointer(entry->key), query);
            break;
        case RTContainsStrategyNumber:
        case RTOldContainsStrategyNumber:
            retval = inner_int_contains((ArrayType*)DatumGetPointer(entry->key), query);
            break;
        case RTContainedByStrategyNumber:
        case RTOldContainedByStrategyNumber:
            if (GIST_LEAF(entry))
                retval = inner_int_contains(query, (ArrayType*)DatumGetPointer(entry->key));
            else
                retval = inner_int_overlap((ArrayType*)DatumGetPointer(entry->key), query);
            break;
        default:
            retval = FALSE;
    }
    pfree(query);
    PG_RETURN_BOOL(retval);
}

Datum g_int_union(PG_FUNCTION_ARGS)
{
    GistEntryVector* entryvec = (GistEntryVector*)PG_GETARG_POINTER(0);
    int* size = (int*)PG_GETARG_POINTER(1);
    int4 i, *ptr;
    ArrayType* res = NULL;
    int totlen = 0;

    for (i = 0; i < entryvec->n; i++) {
        ArrayType* ent = GETENTRY(entryvec, i);

        CHECKARRVALID(ent);
        totlen += ARRNELEMS(ent);
    }

    res = new_intArrayType(totlen);
    ptr = ARRPTR(res);
    int restLen = totlen * sizeof(int4);

    for (i = 0; i < entryvec->n; i++) {
        ArrayType* ent = GETENTRY(entryvec, i);
        int nel;

        nel = ARRNELEMS(ent);
        int rc = memcpy_s(ptr, restLen, ARRPTR(ent), nel * sizeof(int4));
        securec_check(rc, "\0", "\0");
        ptr += nel;
        restLen -= nel * sizeof(int4);
    }

    QSORT(res, 1);
    res = _int_unique(res);
    *size = VARSIZE(res);
    PG_RETURN_POINTER(res);
}

/*
** GiST Compress and Decompress methods
*/
Datum g_int_compress(PG_FUNCTION_ARGS)
{
    GISTENTRY* entry = (GISTENTRY*)PG_GETARG_POINTER(0);
    GISTENTRY* retval = NULL;
    ArrayType* r = NULL;
    int len;
    int* dr = NULL;
    int i, min, cand;

    if (entry->leafkey) {
        r = DatumGetArrayTypePCopy(entry->key);
        CHECKARRVALID(r);
        PREPAREARR(r);

        if (ARRNELEMS(r) >= 2 * MAXNUMRANGE)
            elog(NOTICE,
                "input array is too big (%d maximum allowed, %d current), use gist__intbig_ops opclass instead",
                2 * MAXNUMRANGE - 1,
                ARRNELEMS(r));

        retval = (GISTENTRY*)palloc(sizeof(GISTENTRY));
        gistentryinit(*retval, PointerGetDatum(r), entry->rel, entry->page, entry->offset, FALSE);

        PG_RETURN_POINTER(retval);
    }

    /*
     * leaf entries never compress one more time, only when entry->leafkey
     * ==true, so now we work only with internal keys
     */

    r = DatumGetArrayTypeP(entry->key);
    CHECKARRVALID(r);
    if (ARRISEMPTY(r)) {
        if (r != (ArrayType*)DatumGetPointer(entry->key))
            pfree(r);
        PG_RETURN_POINTER(entry);
    }

    if ((len = ARRNELEMS(r)) >= 2 * MAXNUMRANGE) { /* compress */
        if (r == (ArrayType*)DatumGetPointer(entry->key))
            r = DatumGetArrayTypePCopy(entry->key);
        r = resize_intArrayType(r, 2 * (len));

        dr = ARRPTR(r);

        for (i = len - 1; i >= 0; i--)
            dr[2 * i] = dr[2 * i + 1] = dr[i];

        len *= 2;
        cand = 1;
        while (len > MAXNUMRANGE * 2) {
            min = 0x7fffffff;
            for (i = 2; i < len; i += 2) {
                if (min > (dr[i] - dr[i - 1])) {
                    min = (dr[i] - dr[i - 1]);
                    cand = i;
                }
            }
            int rc = memmove_s((void*)&dr[cand - 1], (len - cand - 1) * sizeof(int32),
                               (void*)&dr[cand + 1], (len - cand - 1) * sizeof(int32));
            securec_check(rc, "\0", "\0");
            len -= 2;
        }
        r = resize_intArrayType(r, len);
        retval = (GISTENTRY*)palloc(sizeof(GISTENTRY));
        gistentryinit(*retval, PointerGetDatum(r), entry->rel, entry->page, entry->offset, FALSE);
        PG_RETURN_POINTER(retval);
    } else
        PG_RETURN_POINTER(entry);

    PG_RETURN_POINTER(entry);
}

Datum g_int_decompress(PG_FUNCTION_ARGS)
{
    GISTENTRY* entry = (GISTENTRY*)PG_GETARG_POINTER(0);
    GISTENTRY* retval = NULL;
    ArrayType* r = NULL;
    int *dr = NULL; 
    int lenr;
    ArrayType* in = NULL;
    int lenin;
    int* din = NULL;
    int i, j;

    in = DatumGetArrayTypeP(entry->key);

    CHECKARRVALID(in);
    if (ARRISEMPTY(in)) {
        if (in != (ArrayType*)DatumGetPointer(entry->key)) {
            retval = (GISTENTRY*)palloc(sizeof(GISTENTRY));
            gistentryinit(*retval, PointerGetDatum(in), entry->rel, entry->page, entry->offset, FALSE);
            PG_RETURN_POINTER(retval);
        }

        PG_RETURN_POINTER(entry);
    }

    lenin = ARRNELEMS(in);

    if (lenin < 2 * MAXNUMRANGE) { /* not compressed value */
        if (in != (ArrayType*)DatumGetPointer(entry->key)) {
            retval = (GISTENTRY*)palloc(sizeof(GISTENTRY));
            gistentryinit(*retval, PointerGetDatum(in), entry->rel, entry->page, entry->offset, FALSE);

            PG_RETURN_POINTER(retval);
        }
        PG_RETURN_POINTER(entry);
    }

    din = ARRPTR(in);
    lenr = internal_size(din, lenin);

    r = new_intArrayType(lenr);
    dr = ARRPTR(r);

    for (i = 0; i < lenin; i += 2)
        for (j = din[i]; j <= din[i + 1]; j++)
            if ((!i) || *(dr - 1) != j)
                *dr++ = j;

    if (in != (ArrayType*)DatumGetPointer(entry->key))
        pfree(in);
    retval = (GISTENTRY*)palloc(sizeof(GISTENTRY));
    gistentryinit(*retval, PointerGetDatum(r), entry->rel, entry->page, entry->offset, FALSE);

    PG_RETURN_POINTER(retval);
}

/*
** The GiST Penalty method for _intments
*/
Datum g_int_penalty(PG_FUNCTION_ARGS)
{
    GISTENTRY* origentry = (GISTENTRY*)PG_GETARG_POINTER(0);
    GISTENTRY* newentry = (GISTENTRY*)PG_GETARG_POINTER(1);
    float* result = (float*)PG_GETARG_POINTER(2);
    ArrayType* ud = NULL;
    float tmp1, tmp2;

    ud = inner_int_union((ArrayType*)DatumGetPointer(origentry->key), (ArrayType*)DatumGetPointer(newentry->key));
    rt__int_size(ud, &tmp1);
    rt__int_size((ArrayType*)DatumGetPointer(origentry->key), &tmp2);
    *result = tmp1 - tmp2;
    pfree(ud);

    PG_RETURN_POINTER(result);
}

Datum g_int_same(PG_FUNCTION_ARGS)
{
    ArrayType* a = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType* b = PG_GETARG_ARRAYTYPE_P(1);
    bool* result = (bool*)PG_GETARG_POINTER(2);
    int4 n = ARRNELEMS(a);
    int4 *da = NULL;
    int4 *db = NULL;

    CHECKARRVALID(a);
    CHECKARRVALID(b);

    if (n != ARRNELEMS(b)) {
        *result = false;
        PG_RETURN_POINTER(result);
    }
    *result = TRUE;
    da = ARRPTR(a);
    db = ARRPTR(b);
    while (n--) {
        if (*da++ != *db++) {
            *result = FALSE;
            break;
        }
    }

    PG_RETURN_POINTER(result);
}

/*****************************************************************
** Common GiST Method
*****************************************************************/

typedef struct {
    OffsetNumber pos;
    float cost;
} SPLITCOST;

static int comparecost(const void* a, const void* b)
{
    if (((const SPLITCOST*)a)->cost == ((const SPLITCOST*)b)->cost)
        return 0;
    else
        return (((const SPLITCOST*)a)->cost > ((const SPLITCOST*)b)->cost) ? 1 : -1;
}

/*
** The GiST PickSplit method for _intments
** We use Guttman's poly time split algorithm
*/
Datum g_int_picksplit(PG_FUNCTION_ARGS)
{
    GistEntryVector* entryvec = (GistEntryVector*)PG_GETARG_POINTER(0);
    GIST_SPLITVEC* v = (GIST_SPLITVEC*)PG_GETARG_POINTER(1);
    OffsetNumber i, j;
    ArrayType *datumAlpha = NULL;
    ArrayType *datumBeta = NULL;
    ArrayType *datumL = NULL;
    ArrayType *datumR = NULL;
    ArrayType *unionD = NULL;
    ArrayType *unionDl = NULL;
    ArrayType *unionDr = NULL;
    ArrayType* interD = NULL;
    bool firsttime = false;
    float sizeAlpha, sizeBeta, sizeUnion, sizeInter;
    float sizeWaste, waste;
    float sizeL, sizeR;
    int nbytes;
    OffsetNumber seed1 = 0, seed2 = 0;
    OffsetNumber *left = NULL;
    OffsetNumber *right = NULL;
    OffsetNumber maxoff;
    SPLITCOST* costvector = NULL;

#ifdef GIST_DEBUG
    elog(DEBUG3, "--------picksplit %d", entryvec->n);
#endif

    maxoff = entryvec->n - 2;
    nbytes = (maxoff + 2) * sizeof(OffsetNumber);
    v->spl_left = (OffsetNumber*)palloc(nbytes);
    v->spl_right = (OffsetNumber*)palloc(nbytes);

    firsttime = true;
    waste = 0.0;
    for (i = FirstOffsetNumber; i < maxoff; i = OffsetNumberNext(i)) {
        datumAlpha = GETENTRY(entryvec, i);
        for (j = OffsetNumberNext(i); j <= maxoff; j = OffsetNumberNext(j)) {
            datumBeta = GETENTRY(entryvec, j);

            /* compute the wasted space by unioning these guys */
            unionD = inner_int_union(datumAlpha, datumBeta);
            rt__int_size(unionD, &sizeUnion);
            interD = inner_int_inter(datumAlpha, datumBeta);
            rt__int_size(interD, &sizeInter);
            sizeWaste = sizeUnion - sizeInter;

            pfree(unionD);

            if (interD) {
                pfree(interD);
            }

            /*
             * are these a more promising split that what we've already seen?
             */

            if (sizeWaste > waste || firsttime) {
                waste = sizeWaste;
                seed1 = i;
                seed2 = j;
                firsttime = false;
            }
        }
    }

    left = v->spl_left;
    v->spl_nleft = 0;
    right = v->spl_right;
    v->spl_nright = 0;
    if (seed1 == 0 || seed2 == 0) {
        seed1 = 1;
        seed2 = 2;
    }

    datumAlpha = GETENTRY(entryvec, seed1);
    datumL = copy_intArrayType(datumAlpha);
    rt__int_size(datumL, &sizeL);
    datumBeta = GETENTRY(entryvec, seed2);
    datumR = copy_intArrayType(datumBeta);
    rt__int_size(datumR, &sizeR);

    maxoff = OffsetNumberNext(maxoff);

    /*
     * sort entries
     */
    costvector = (SPLITCOST*)palloc(sizeof(SPLITCOST) * maxoff);
    for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
        costvector[i - 1].pos = i;
        datumAlpha = GETENTRY(entryvec, i);
        unionD = inner_int_union(datumL, datumAlpha);
        rt__int_size(unionD, &sizeAlpha);
        pfree(unionD);
        unionD = inner_int_union(datumR, datumAlpha);
        rt__int_size(unionD, &sizeBeta);
        pfree(unionD);
        costvector[i - 1].cost = Abs((sizeAlpha - sizeL) - (sizeBeta - sizeR));
    }
    qsort((void*)costvector, maxoff, sizeof(SPLITCOST), comparecost);

    /*
     * Now split up the regions between the two seeds.	An important property
     * of this split algorithm is that the split vector v has the indices of
     * items to be split in order in its left and right vectors.  We exploit
     * this property by doing a merge in the code that actually splits the
     * page.
     *
     * For efficiency, we also place the new index tuple in this loop. This is
     * handled at the very end, when we have placed all the existing tuples
     * and i == maxoff + 1.
     */

    for (j = 0; j < maxoff; j++) {
        i = costvector[j].pos;

        /*
         * If we've already decided where to place this item, just put it on
         * the right list.	Otherwise, we need to figure out which page needs
         * the least enlargement in order to store the item.
         */

        if (i == seed1) {
            *left++ = i;
            v->spl_nleft++;
            continue;
        } else if (i == seed2) {
            *right++ = i;
            v->spl_nright++;
            continue;
        }

        /* okay, which page needs least enlargement? */
        datumAlpha = GETENTRY(entryvec, i);
        unionDl = inner_int_union(datumL, datumAlpha);
        unionDr = inner_int_union(datumR, datumAlpha);
        rt__int_size(unionDl, &sizeAlpha);
        rt__int_size(unionDr, &sizeBeta);

        /* pick which page to add it to */
        if (sizeAlpha - sizeL < sizeBeta - sizeR + WISH_F(v->spl_nleft, v->spl_nright, 0.01)) {
            if (datumL) {
                pfree(datumL);
            }
            if (unionDr) {
                pfree(unionDr);
            }
            datumL = unionDl;
            sizeL = sizeAlpha;
            *left++ = i;
            v->spl_nleft++;
        } else {
            if (datumR) {
                pfree(datumR);
            }
            if (unionDl) {
                pfree(unionDl);
			}
            datumR = unionDr;
            sizeR = sizeBeta;
            *right++ = i; 
            v->spl_nright++;
        }
    }
    pfree(costvector);
    *right = *left = FirstOffsetNumber;

    v->spl_ldatum = PointerGetDatum(datumL);
    v->spl_rdatum = PointerGetDatum(datumR);

    PG_RETURN_POINTER(v);
}
