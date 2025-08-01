/* -------------------------------------------------------------------------
 *
 * gistproc.cpp
 *	  Support procedures for GiSTs over 2-D objects (boxes, polygons, circles,
 *	  points).
 *
 * This gives R-tree behavior, with Guttman's poly-time split algorithm.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/gist/gistproc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <cmath>
#include <limits>

#include "access/gist.h"
#include "access/skey.h"
#include "utils/geo_decls.h"
#include "utils/builtins.h"

static bool gist_box_leaf_consistent(BOX *key, BOX *query, StrategyNumber strategy);
static bool rtree_internal_consistent(BOX *key, BOX *query, StrategyNumber strategy);

/* Minimum accepted ratio of split */
#define LIMIT_RATIO 0.3

/* Convenience macros for NaN-aware comparisons */
#define FLOAT8_EQ(a,b) (float8_cmp_internal(a, b) == 0)
#define FLOAT8_LT(a,b) (float8_cmp_internal(a, b) < 0)
#define FLOAT8_LE(a,b) (float8_cmp_internal(a, b) <= 0)
#define FLOAT8_GT(a,b) (float8_cmp_internal(a, b) > 0)
#define FLOAT8_GE(a,b) (float8_cmp_internal(a, b) >= 0)
#define FLOAT8_MAX(a,b) (FLOAT8_GT(a, b) ? (a) : (b))
#define FLOAT8_MIN(a,b) (FLOAT8_LT(a, b) ? (a) : (b))

/**************************************************
 * Box ops
 **************************************************/
/*
 * Calculates union of two boxes, a and b. The result is stored in *n.
 */
static void rt_box_union(BOX *n, const BOX *a, const BOX *b)
{
    n->high.x = FLOAT8_MAX(a->high.x, b->high.x);
    n->high.y = FLOAT8_MAX(a->high.y, b->high.y);
    n->low.x = FLOAT8_MIN(a->low.x, b->low.x);
    n->low.y = FLOAT8_MIN(a->low.y, b->low.y);
}

/*
 * Size of a BOX for penalty-calculation purposes.
 * The result can be +Infinity, but not NaN.
 */
static double size_box(const BOX *box)
{
   /*
    * Check for zero-width cases.  Note that we define the size of a zero-
    * by-infinity box as zero.  It's important to special-case this somehow,
    * as naively multiplying infinity by zero will produce NaN.
    *
    * The less-than cases should not happen, but if they do, say "zero".
    */
   if (FLOAT8_LE(box->high.x, box->low.x) || FLOAT8_LE(box->high.y, box->low.y)) {
       return 0.0;
    }

   /*
    * We treat NaN as larger than +Infinity, so any distance involving a NaN
    * and a non-NaN is infinite.  Note the previous check eliminated the
    * possibility that the low fields are NaNs.
    */
    if (std::isnan(box->high.x) || std::isnan(box->high.y)) {
        return std::numeric_limits<double>::infinity();
    }
   return (box->high.x - box->low.x) * (box->high.y - box->low.y);
}

/*
 * Return amount by which the union of the two boxes is larger than
 * the original BOX's area.  The result can be +Infinity, but not NaN.
 */
static double box_penalty(const BOX *original, const BOX *new_box)
{
    BOX unionbox;

    rt_box_union(&unionbox, original, new_box);
    return size_box(&unionbox) - size_box(original);
}

/*
 * The GiST Consistent method for boxes
 *
 * Should return false if for all data items x below entry,
 * the predicate x op query must be FALSE, where op is the oper
 * corresponding to strategy in the pg_amop table.
 */
Datum gist_box_consistent(PG_FUNCTION_ARGS)
{
    GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
    BOX *query = PG_GETARG_BOX_P(1);
    StrategyNumber strategy = (StrategyNumber)PG_GETARG_UINT16(2);

    bool *recheck = (bool *)PG_GETARG_POINTER(4);

    /* All cases served by this function are exact */
    *recheck = false;

    if (DatumGetBoxP(entry->key) == NULL || query == NULL) {
        PG_RETURN_BOOL(FALSE);
    }

    /*
     * if entry is not leaf, use rtree_internal_consistent, else use
     * gist_box_leaf_consistent
     */
    if (GIST_LEAF(entry)) {
        PG_RETURN_BOOL(gist_box_leaf_consistent(DatumGetBoxP(entry->key), query, strategy));
    } else {
        PG_RETURN_BOOL(rtree_internal_consistent(DatumGetBoxP(entry->key), query, strategy));
    }
}

static void adjustBox(BOX *b, const BOX *addon)
{
    if (FLOAT8_LT(b->high.x, addon->high.x)) {
        b->high.x = addon->high.x;
    }
    if (FLOAT8_GT(b->low.x, addon->low.x)) {
        b->low.x = addon->low.x;
    }
    if (FLOAT8_LT(b->high.y, addon->high.y)) {
        b->high.y = addon->high.y;
    }
    if (FLOAT8_GT(b->low.y, addon->low.y)) {
        b->low.y = addon->low.y;
    }
}

/*
 * The GiST Union method for boxes
 *
 * returns the minimal bounding box that encloses all the entries in entryvec
 */
Datum gist_box_union(PG_FUNCTION_ARGS)
{
    GistEntryVector *entryvec = (GistEntryVector *)PG_GETARG_POINTER(0);
    int *sizep = (int *)PG_GETARG_POINTER(1);
    int numranges, i;
    BOX *cur, *pageunion;
    errno_t ret = EOK;

    numranges = entryvec->n;
    pageunion = (BOX *)palloc(sizeof(BOX));
    cur = DatumGetBoxP(entryvec->vector[0].key);
    ret = memcpy_s((void *)pageunion, sizeof(BOX), (void *)cur, sizeof(BOX));
    securec_check(ret, "", "");

    for (i = 1; i < numranges; i++) {
        cur = DatumGetBoxP(entryvec->vector[i].key);
        adjustBox(pageunion, cur);
    }
    *sizep = sizeof(BOX);

    PG_RETURN_POINTER(pageunion);
}

/*
 * GiST Compress methods for boxes
 *
 * do not do anything.
 */
Datum gist_box_compress(PG_FUNCTION_ARGS)
{
    PG_RETURN_POINTER(PG_GETARG_POINTER(0));
}

/*
 * GiST DeCompress method for boxes (also used for points, polygons
 * and circles)
 *
 * do not do anything --- we just use the stored box as is.
 */
Datum gist_box_decompress(PG_FUNCTION_ARGS)
{
    PG_RETURN_POINTER(PG_GETARG_POINTER(0));
}

/*
 * The GiST Penalty method for boxes (also used for points)
 *
 * As in the R-tree paper, we use change in area as our penalty metric
 */
Datum gist_box_penalty(PG_FUNCTION_ARGS)
{
    GISTENTRY *origentry = (GISTENTRY *)PG_GETARG_POINTER(0);
    GISTENTRY *newentry = (GISTENTRY *)PG_GETARG_POINTER(1);
    float *result = (float *)PG_GETARG_POINTER(2);
    BOX *origbox = DatumGetBoxP(origentry->key);
    BOX *newbox = DatumGetBoxP(newentry->key);

    *result = (float) box_penalty(origbox, newbox);
    PG_RETURN_POINTER(result);
}

/*
 * Trivial split: half of entries will be placed on one page
 * and another half - to another
 */
static void fallbackSplit(GistEntryVector *entryvec, GIST_SPLITVEC *v)
{
    OffsetNumber i, maxoff;
    BOX *unionL = NULL;
    BOX *unionR = NULL;
    int nbytes;

    maxoff = entryvec->n - 1;

    nbytes = (maxoff + 2) * sizeof(OffsetNumber);
    v->spl_left = (OffsetNumber *)palloc(nbytes);
    v->spl_right = (OffsetNumber *)palloc(nbytes);
    v->spl_nleft = v->spl_nright = 0;

    for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
        BOX *cur = DatumGetBoxP(entryvec->vector[i].key);

        if (i <= (maxoff - FirstOffsetNumber + 1) / 2) {
            v->spl_left[v->spl_nleft] = i;
            if (unionL == NULL) {
                unionL = (BOX *)palloc(sizeof(BOX));
                *unionL = *cur;
            } else {
                adjustBox(unionL, cur);
            }

            v->spl_nleft++;
        } else {
            v->spl_right[v->spl_nright] = i;
            if (unionR == NULL) {
                unionR = (BOX *)palloc(sizeof(BOX));
                *unionR = *cur;
            } else {
                adjustBox(unionR, cur);
            }

            v->spl_nright++;
        }
    }

    v->spl_ldatum = BoxPGetDatum(unionL);
    v->spl_rdatum = BoxPGetDatum(unionR);
}

/*
 * Represents information about an entry that can be placed to either group
 * without affecting overlap over selected axis ("common entry").
 */
typedef struct {
    /* Index of entry in the initial array */
    int index;
    /* Delta between penalties of entry insertion into different groups */
    double delta;
} CommonEntry;

/*
 * Context for g_box_consider_split. Contains information about currently
 * selected split and some general information.
 */
typedef struct {
    int entriesCount; /* total number of entries being split */
    BOX boundingBox;  /* minimum bounding box across all entries */

    /* Information about currently selected split follows */
    bool first; /* true if no split was selected yet */

    double leftUpper;  /* upper bound of left interval */
    double rightLower; /* lower bound of right interval */

    float4 ratio;
    float4 overlap;
    int dim;      /* axis of this split */
    double range; /* width of general MBR projection to the selected axis */
} ConsiderSplitContext;

/*
 * Interval represents projection of box to axis.
 */
typedef struct {
    double lower, upper;
} SplitInterval;

/*
 * Interval comparison function by lower bound of the interval;
 */
static int interval_cmp_lower(const void *i1, const void *i2)
{
    double lower1 = ((const SplitInterval *)i1)->lower;
    double lower2 = ((const SplitInterval *)i2)->lower;

    return float8_cmp_internal(lower1, lower2);
}

/*
 * Interval comparison function by upper bound of the interval;
 */
static int interval_cmp_upper(const void *i1, const void *i2)
{
    double upper1 = ((const SplitInterval *)i1)->upper;
    double upper2 = ((const SplitInterval *)i2)->upper;

    return float8_cmp_internal(upper1, upper2);
}

/*
 * Replace negative (or NaN) value with zero.
 */
static inline float non_negative(float val)
{
    if (val >= 0.0f) {
        return val;
    } else {
        return 0.0f;
    }
}

/*
 * Consider replacement of currently selected split with the better one.
 */
static inline void g_box_consider_split(ConsiderSplitContext *context, int dimNum, double rightLower, int minLeftCount,
                                        double leftUpper, int maxLeftCount)
{
    int leftCount, rightCount;
    float4 ratio, overlap;
    double range;

    /*
     * Calculate entries distribution ratio assuming most uniform distribution
     * of common entries.
     */
    if (minLeftCount >= (context->entriesCount + 1) / 2) {
        leftCount = minLeftCount;
    } else {
        if (maxLeftCount <= context->entriesCount / 2) {
            leftCount = maxLeftCount;
        } else {
            leftCount = context->entriesCount / 2;
        }
    }
    rightCount = context->entriesCount - leftCount;

    /*
     * Ratio of split - quotient between size of lesser group and total
     * entries count.
     */
    ratio = ((float4)Min(leftCount, rightCount)) / ((float4)context->entriesCount);
    if (ratio > LIMIT_RATIO) {
        bool selectthis = false;

        /*
         * The ratio is acceptable, so compare current split with previously
         * selected one. Between splits of one dimension we search for minimal
         * overlap (allowing negative values) and minimal ration (between same
         * overlaps. We switch dimension if find less overlap (non-negative)
         * or less range with same overlap.
         */
        if (dimNum == 0) {
            range = context->boundingBox.high.x - context->boundingBox.low.x;
        } else {
            range = context->boundingBox.high.y - context->boundingBox.low.y;
        }

        overlap = (leftUpper - rightLower) / range;

        /* If there is no previous selection, select this */
        if (context->first) {
            selectthis = true;
        } else if (context->dim == dimNum) {
            /*
             * Within the same dimension, choose the new split if it has a
             * smaller overlap, or same overlap but better ratio.
             */
            if (overlap < context->overlap || (overlap == context->overlap && ratio > context->ratio)) {
                selectthis = true;
            }
        } else {
            /*
             * Across dimensions, choose the new split if it has a smaller
             * *non-negative* overlap, or same *non-negative* overlap but
             * bigger range. This condition differs from the one described in
             * the article. On the datasets where leaf MBRs don't overlap
             * themselves, non-overlapping splits (i.e. splits which have zero
             * *non-negative* overlap) are frequently possible. In this case
             * splits tends to be along one dimension, because most distant
             * non-overlapping splits (i.e. having lowest negative overlap)
             * appears to be in the same dimension as in the previous split.
             * Therefore MBRs appear to be very prolonged along another
             * dimension, which leads to bad search performance. Using range
             * as the second split criteria makes MBRs more quadratic. Using
             * *non-negative* overlap instead of overlap as the first split
             * criteria gives to range criteria a chance to matter, because
             * non-overlapping splits are equivalent in this criteria.
             */
            if (non_negative(overlap) < non_negative(context->overlap) ||
                (range > context->range && non_negative(overlap) <= non_negative(context->overlap))) {
                selectthis = true;
            }
        }

        if (selectthis) {
            /* save information about selected split */
            context->first = false;
            context->ratio = ratio;
            context->range = range;
            context->overlap = overlap;
            context->rightLower = rightLower;
            context->leftUpper = leftUpper;
            context->dim = dimNum;
        }
    }
}

/*
 * Compare common entries by their deltas. (We assume the deltas can't be NaN.)
 */
static int common_entry_cmp(const void *i1, const void *i2)
{
    double delta1 = ((const CommonEntry *)i1)->delta;
    double delta2 = ((const CommonEntry *)i2)->delta;

    if (delta1 < delta2) {
        return -1;
    } else if (delta1 > delta2) {
        return 1;
    } else {
        return 0;
    }
}

/*
 * --------------------------------------------------------------------------
 * Double sorting split algorithm. This is used for both boxes and points.
 *
 * The algorithm finds split of boxes by considering splits along each axis.
 * Each entry is first projected as an interval on the X-axis, and different
 * ways to split the intervals into two groups are considered, trying to
 * minimize the overlap of the groups. Then the same is repeated for the
 * Y-axis, and the overall best split is chosen. The quality of a split is
 * determined by overlap along that axis and some other criteria (see
 * g_box_consider_split).
 *
 * After that, all the entries are divided into three groups:
 *
 * 1) Entries which should be placed to the left group
 * 2) Entries which should be placed to the right group
 * 3) "Common entries" which can be placed to any of groups without affecting
 *	  of overlap along selected axis.
 *
 * The common entries are distributed by minimizing penalty.
 *
 * For details see:
 * "A new double sorting-based node splitting algorithm for R-tree", A. Korotkov
 * http://syrcose.ispras.ru/2011/files/SYRCoSE2011_Proceedings.pdf#page=36
 * --------------------------------------------------------------------------
 */
Datum gist_box_picksplit(PG_FUNCTION_ARGS)
{
    GistEntryVector *entryvec = (GistEntryVector *)PG_GETARG_POINTER(0);
    GIST_SPLITVEC *v = (GIST_SPLITVEC *)PG_GETARG_POINTER(1);
    OffsetNumber i, maxoff;
    ConsiderSplitContext context;
    BOX *box = NULL;
    BOX *leftBox = NULL;
    BOX *rightBox = NULL;
    int dim, commonEntriesCount;
    SplitInterval *intervalsLower = NULL;
    SplitInterval *intervalsUpper = NULL;
    CommonEntry *commonEntries = NULL;
    int nentries;
    errno_t ret = EOK;

    ret = memset_s(&context, sizeof(ConsiderSplitContext), 0, sizeof(ConsiderSplitContext));
    securec_check(ret, "", "");

    maxoff = entryvec->n - 1;
    nentries = context.entriesCount = maxoff - FirstOffsetNumber + 1;

    /* Allocate arrays for intervals along axes */
    intervalsLower = (SplitInterval *)palloc(nentries * sizeof(SplitInterval));
    intervalsUpper = (SplitInterval *)palloc(nentries * sizeof(SplitInterval));

    /*
     * Calculate the overall minimum bounding box over all the entries.
     */
    for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
        box = DatumGetBoxP(entryvec->vector[i].key);
        if (i == FirstOffsetNumber) {
            context.boundingBox = *box;
        } else {
            adjustBox(&context.boundingBox, box);
        }
    }

    /*
     * Iterate over axes for optimal split searching.
     */
    context.first = true; /* nothing selected yet */
    for (dim = 0; dim < 2; dim++) {
        double leftUpper, rightLower;
        int i1, i2;

        /* Project each entry as an interval on the selected axis. */
        for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
            box = DatumGetBoxP(entryvec->vector[i].key);
            if (dim == 0) {
                intervalsLower[i - FirstOffsetNumber].lower = box->low.x;
                intervalsLower[i - FirstOffsetNumber].upper = box->high.x;
            } else {
                intervalsLower[i - FirstOffsetNumber].lower = box->low.y;
                intervalsLower[i - FirstOffsetNumber].upper = box->high.y;
            }
        }

        /*
         * Make two arrays of intervals: one sorted by lower bound and another
         * sorted by upper bound.
         */
        ret = memcpy_s(intervalsUpper, sizeof(SplitInterval) * nentries, intervalsLower,
                       sizeof(SplitInterval) * nentries);
        securec_check(ret, "", "");
        qsort(intervalsLower, nentries, sizeof(SplitInterval), interval_cmp_lower);
        qsort(intervalsUpper, nentries, sizeof(SplitInterval), interval_cmp_upper);

        /* ----
         * The goal is to form a left and right interval, so that every entry
         * interval is contained by either left or right interval (or both).
         *
         * For example, with the intervals (0,1), (1,3), (2,3), (2,4):
         *
         * 0 1 2 3 4
         * +-+
         *	 +---+
         *	   +-+
         *	   +---+
         *
         * The left and right intervals are of the form (0,a) and (b,4).
         * We first consider splits where b is the lower bound of an entry.
         * We iterate through all entries, and for each b, calculate the
         * smallest possible a. Then we consider splits where a is the
         * upper bound of an entry, and for each a, calculate the greatest
         * possible b.
         *
         * In the above example, the first loop would consider splits:
         * b=0: (0,1)-(0,4)
         * b=1: (0,1)-(1,4)
         * b=2: (0,3)-(2,4)
         *
         * And the second loop:
         * a=1: (0,1)-(1,4)
         * a=3: (0,3)-(2,4)
         * a=4: (0,4)-(2,4)

         * Iterate over lower bound of right group, finding smallest possible
         * upper bound of left group.
         */
        i1 = 0;
        i2 = 0;
        rightLower = intervalsLower[i1].lower;
        leftUpper = intervalsUpper[i2].lower;
        while (true) {
            /*
             * Find next lower bound of right group.
             */
            while (i1 < nentries && FLOAT8_EQ(rightLower, intervalsLower[i1].lower)) {
                if (FLOAT8_LT(leftUpper, intervalsLower[i1].upper)) {
                    leftUpper = intervalsLower[i1].upper;
                }
                i1++;
            }
            if (i1 >= nentries) {
                break;
            }
            rightLower = intervalsLower[i1].lower;

            /*
             * Find count of intervals which anyway should be placed to the
             * left group.
             */
            while (i2 < nentries && FLOAT8_LE(intervalsUpper[i2].upper, leftUpper)) {
                i2++;
            }

            // Consider found split.
            g_box_consider_split(&context, dim, rightLower, i1, leftUpper, i2);
        }

        /*
         * Iterate over upper bound of left group finding greatest possible
         * lower bound of right group.
         */
        i1 = nentries - 1;
        i2 = nentries - 1;
        rightLower = intervalsLower[i1].upper;
        leftUpper = intervalsUpper[i2].upper;
        while (true) {
            /*
             * Find next upper bound of left group.
             */
            while (i2 >= 0 && FLOAT8_EQ(leftUpper, intervalsUpper[i2].upper)) {
                if (FLOAT8_GT(rightLower, intervalsUpper[i2].lower)) {
                    rightLower = intervalsUpper[i2].lower;
                }
                i2--;
            }
            if (i2 < 0) {
                break;
            }
            leftUpper = intervalsUpper[i2].upper;

            // Find count of intervals which anyway should be placed to the right group.
            while (i1 >= 0 && FLOAT8_GE(intervalsLower[i1].lower, rightLower)) {
                i1--;
            }

            // Consider found split.
            g_box_consider_split(&context, dim, rightLower, i1 + 1, leftUpper, i2 + 1);
        }
    }

    /* * If we failed to find any acceptable splits, use trivial split. */
    if (context.first) {
        fallbackSplit(entryvec, v);
        PG_RETURN_POINTER(v);
    }

    /*
     * Ok, we have now selected the split across one axis.
     *
     * While considering the splits, we already determined that there will be
     * enough entries in both groups to reach the desired ratio, but we did
     * not memorize which entries go to which group. So determine that now.
     *
     * Allocate vectors for results
     */
    v->spl_left = (OffsetNumber *)palloc(nentries * sizeof(OffsetNumber));
    v->spl_right = (OffsetNumber *)palloc(nentries * sizeof(OffsetNumber));
    v->spl_nleft = 0;
    v->spl_nright = 0;

    /* Allocate bounding boxes of left and right groups */
    leftBox = (BOX *)palloc0(sizeof(BOX));
    rightBox = (BOX *)palloc0(sizeof(BOX));

    /*
     * Allocate an array for "common entries" - entries which can be placed to
     * either group without affecting overlap along selected axis.
     */
    commonEntriesCount = 0;
    commonEntries = (CommonEntry *)palloc(nentries * sizeof(CommonEntry));

    /* Helper macros to place an entry in the left or right group */
#define PLACE_LEFT(box, off) do { \
    if (v->spl_nleft > 0) {            \
        adjustBox(leftBox, box);       \
    } else {                           \
        *leftBox = *(box);             \
    }                                  \
    v->spl_left[v->spl_nleft++] = off; \
} while (0)

#define PLACE_RIGHT(box, off) do { \
    if (v->spl_nright > 0) {             \
        adjustBox(rightBox, box);        \
    } else {                             \
        *rightBox = *(box);              \
    }                                    \
    v->spl_right[v->spl_nright++] = off; \
} while (0)

    /*
     * Distribute entries which can be distributed unambiguously, and collect
     * common entries.
     */
    for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i)) {
        double lower, upper;

        /*
         * Get upper and lower bounds along selected axis.
         */
        box = DatumGetBoxP(entryvec->vector[i].key);
        if (context.dim == 0) {
            lower = box->low.x;
            upper = box->high.x;
        } else {
            lower = box->low.y;
            upper = box->high.y;
        }

        if (FLOAT8_LE(upper, context.leftUpper)) {
            /* Fits to the left group */
            if (FLOAT8_GE(lower, context.rightLower)) {
                /* Fits also to the right group, so "common entry" */
                commonEntries[commonEntriesCount++].index = i;
            } else {
                /* Doesn't fit to the right group, so join to the left group */
                PLACE_LEFT(box, i);
            }
        } else {
            /*
             * Each entry should fit on either left or right group. Since this
             * entry didn't fit on the left group, it better fit in the right
             * group.
             */
            Assert(FLOAT8_GE(lower, context.rightLower));

            /* Doesn't fit to the left group, so join to the right group */
            PLACE_RIGHT(box, i);
        }
    }

    /*
     * Distribute "common entries", if any.
     */
    if (commonEntriesCount > 0) {
        /*
         * Calculate minimum number of entries that must be placed in both
         * groups, to reach LIMIT_RATIO.
         */
        int m = (int)ceil(LIMIT_RATIO * (double)nentries);

        /*
         * Calculate delta between penalties of join "common entries" to
         * different groups.
         */
        for (i = 0; i < commonEntriesCount; i++) {
            box = DatumGetBoxP(entryvec->vector[commonEntries[i].index].key);
            commonEntries[i].delta = Abs(box_penalty(leftBox, box) - box_penalty(rightBox, box));
        }

        /*
         * Sort "common entries" by calculated deltas in order to distribute
         * the most ambiguous entries first.
         */
        qsort(commonEntries, commonEntriesCount, sizeof(CommonEntry), common_entry_cmp);

        /*
         * Distribute "common entries" between groups.
         */
        for (i = 0; i < commonEntriesCount; i++) {
            box = DatumGetBoxP(entryvec->vector[commonEntries[i].index].key);

            /*
             * Check if we have to place this entry in either group to achieve
             * LIMIT_RATIO.
             */
            if (v->spl_nleft + (commonEntriesCount - i) <= m) {
                PLACE_LEFT(box, commonEntries[i].index);
            } else if (v->spl_nright + (commonEntriesCount - i) <= m) {
                PLACE_RIGHT(box, commonEntries[i].index);
            } else {
                /* Otherwise select the group by minimal penalty */
                if (box_penalty(leftBox, box) < box_penalty(rightBox, box)) {
                    PLACE_LEFT(box, commonEntries[i].index);
                } else {
                    PLACE_RIGHT(box, commonEntries[i].index);
                }
            }
        }
    }

    v->spl_ldatum = PointerGetDatum(leftBox);
    v->spl_rdatum = PointerGetDatum(rightBox);
    PG_RETURN_POINTER(v);
}

/*
 * Equality method
 *
 * This is used for boxes, points, circles, and polygons, all of which store
 * boxes as GiST index entries.
 *
 * Returns true only when boxes are exactly the same.  We can't use fuzzy
 * comparisons here without breaking index consistency; therefore, this isn't
 * equivalent to box_same().
 */
Datum gist_box_same(PG_FUNCTION_ARGS)
{
    BOX *b1 = PG_GETARG_BOX_P(0);
    BOX *b2 = PG_GETARG_BOX_P(1);
    bool *result = (bool *)PG_GETARG_POINTER(2);

    if (b1 != NULL && b2 != NULL) {
        *result = (FLOAT8_EQ(b1->low.x, b2->low.x) && FLOAT8_EQ(b1->low.y, b2->low.y) &&
                FLOAT8_EQ(b1->high.x, b2->high.x) && FLOAT8_EQ(b1->high.y, b2->high.y));
    } else {
        *result = (b1 == NULL && b2 == NULL);
    }
    PG_RETURN_POINTER(result);
}

/*
 * Leaf-level consistency for boxes: just apply the query operator
 */
static bool gist_box_leaf_consistent(BOX *key, BOX *query, StrategyNumber strategy)
{
    bool retval = false;

    switch (strategy) {
        case RTLeftStrategyNumber:
            retval = DatumGetBool(DirectFunctionCall2(box_left, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTOverLeftStrategyNumber:
            retval = DatumGetBool(DirectFunctionCall2(box_overleft, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTOverlapStrategyNumber:
            retval = DatumGetBool(DirectFunctionCall2(box_overlap, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTOverRightStrategyNumber:
            retval = DatumGetBool(DirectFunctionCall2(box_overright, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTRightStrategyNumber:
            retval = DatumGetBool(DirectFunctionCall2(box_right, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTSameStrategyNumber:
            retval = DatumGetBool(DirectFunctionCall2(box_same, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTContainsStrategyNumber:
        case RTOldContainsStrategyNumber:
            retval = DatumGetBool(DirectFunctionCall2(box_contain, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTContainedByStrategyNumber:
        case RTOldContainedByStrategyNumber:
            retval = DatumGetBool(DirectFunctionCall2(box_contained, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTOverBelowStrategyNumber:
            retval = DatumGetBool(DirectFunctionCall2(box_overbelow, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTBelowStrategyNumber:
            retval = DatumGetBool(DirectFunctionCall2(box_below, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTAboveStrategyNumber:
            retval = DatumGetBool(DirectFunctionCall2(box_above, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTOverAboveStrategyNumber:
            retval = DatumGetBool(DirectFunctionCall2(box_overabove, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        default:
            retval = FALSE;
            break;
    }
    return retval;
}

/*****************************************
 * Common rtree functions (for boxes, polygons, and circles)
 *****************************************/

/*
 * Internal-page consistency for all these types
 *
 * We can use the same function since all types use bounding boxes as the
 * internal-page representation.
 */
static bool rtree_internal_consistent(BOX *key, BOX *query, StrategyNumber strategy)
{
    bool retval = false;

    switch (strategy) {
        case RTLeftStrategyNumber:
            retval = !DatumGetBool(DirectFunctionCall2(box_overright, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTOverLeftStrategyNumber:
            retval = !DatumGetBool(DirectFunctionCall2(box_right, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTOverlapStrategyNumber:
            retval = DatumGetBool(DirectFunctionCall2(box_overlap, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTOverRightStrategyNumber:
            retval = !DatumGetBool(DirectFunctionCall2(box_left, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTRightStrategyNumber:
            retval = !DatumGetBool(DirectFunctionCall2(box_overleft, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTSameStrategyNumber:
        case RTContainsStrategyNumber:
        case RTOldContainsStrategyNumber:
            retval = DatumGetBool(DirectFunctionCall2(box_contain, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTContainedByStrategyNumber:
        case RTOldContainedByStrategyNumber:
            retval = DatumGetBool(DirectFunctionCall2(box_overlap, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTOverBelowStrategyNumber:
            retval = !DatumGetBool(DirectFunctionCall2(box_above, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTBelowStrategyNumber:
            retval = !DatumGetBool(DirectFunctionCall2(box_overabove, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTAboveStrategyNumber:
            retval = !DatumGetBool(DirectFunctionCall2(box_overbelow, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        case RTOverAboveStrategyNumber:
            retval = !DatumGetBool(DirectFunctionCall2(box_below, PointerGetDatum(key), PointerGetDatum(query)));
            break;
        default:
            retval = FALSE;
            break;
    }
    return retval;
}

/**************************************************
 * Polygon ops
 **************************************************/

/*
 * GiST compress for polygons: represent a polygon by its bounding box
 */
Datum gist_poly_compress(PG_FUNCTION_ARGS)
{
    GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
    GISTENTRY *retval = NULL;
    errno_t ret = EOK;

    if (entry->leafkey) {
        retval = (GISTENTRY *)palloc(sizeof(GISTENTRY));
        if (DatumGetPointer(entry->key) != NULL) {
            POLYGON *in = DatumGetPolygonP(entry->key);
            BOX *r = NULL;

            r = (BOX *)palloc(sizeof(BOX));
            ret = memcpy_s((void *)r, sizeof(BOX), (void *)&(in->boundbox), sizeof(BOX));
            securec_check(ret, "", "");
            gistentryinit(*retval, PointerGetDatum(r), entry->rel, entry->page, entry->offset, FALSE);
        } else {
            gistentryinit(*retval, (Datum)0, entry->rel, entry->page, entry->offset, FALSE);
        }
    } else
        retval = entry;
    PG_RETURN_POINTER(retval);
}

/*
 * The GiST Consistent method for polygons
 */
Datum gist_poly_consistent(PG_FUNCTION_ARGS)
{
    GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
    POLYGON *query = PG_GETARG_POLYGON_P(1);
    StrategyNumber strategy = (StrategyNumber)PG_GETARG_UINT16(2);

    bool *recheck = (bool *)PG_GETARG_POINTER(4);
    bool result = false;

    /* All cases served by this function are inexact */
    *recheck = true;

    if (DatumGetBoxP(entry->key) == NULL || query == NULL) {
        PG_RETURN_BOOL(FALSE);
    }

    /*
     * Since the operators require recheck anyway, we can just use
     * rtree_internal_consistent even at leaf nodes.  (This works in part
     * because the index entries are bounding boxes not polygons.)
     */
    result = rtree_internal_consistent(DatumGetBoxP(entry->key), &(query->boundbox), strategy);

    /* Avoid memory leak if supplied poly is toasted */
    PG_FREE_IF_COPY(query, 1);

    PG_RETURN_BOOL(result);
}

/**************************************************
 * Circle ops
 **************************************************/

/*
 * GiST compress for circles: represent a circle by its bounding box
 */
Datum gist_circle_compress(PG_FUNCTION_ARGS)
{
    GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
    GISTENTRY *retval = NULL;

    if (entry->leafkey) {
        retval = (GISTENTRY *)palloc(sizeof(GISTENTRY));
        if (DatumGetCircleP(entry->key) != NULL) {
            CIRCLE *in = DatumGetCircleP(entry->key);
            BOX *r = (BOX *)palloc(sizeof(BOX));
            r->high.x = in->center.x + in->radius;
            r->low.x = in->center.x - in->radius;
            r->high.y = in->center.y + in->radius;
            r->low.y = in->center.y - in->radius;
            gistentryinit(*retval, PointerGetDatum(r), entry->rel, entry->page, entry->offset, FALSE);
        } else {
            gistentryinit(*retval, (Datum)0, entry->rel, entry->page, entry->offset, FALSE);
        }
    } else {
        retval = entry;
    }
    PG_RETURN_POINTER(retval);
}

/* The GiST Consistent method for circles */
Datum gist_circle_consistent(PG_FUNCTION_ARGS)
{
    GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
    CIRCLE *query = PG_GETARG_CIRCLE_P(1);
    StrategyNumber strategy = (StrategyNumber)PG_GETARG_UINT16(2);

    bool *recheck = (bool *)PG_GETARG_POINTER(4);
    BOX bbox;
    bool result = false;

    /* All cases served by this function are inexact */
    *recheck = true;

    if (DatumGetBoxP(entry->key) == NULL || query == NULL) {
        PG_RETURN_BOOL(FALSE);
    }

    /*
     * Since the operators require recheck anyway, we can just use
     * rtree_internal_consistent even at leaf nodes.  (This works in part
     * because the index entries are bounding boxes not circles.)
     */
    bbox.high.x = query->center.x + query->radius;
    bbox.low.x = query->center.x - query->radius;
    bbox.high.y = query->center.y + query->radius;
    bbox.low.y = query->center.y - query->radius;

    result = rtree_internal_consistent(DatumGetBoxP(entry->key), &bbox, strategy);

    PG_RETURN_BOOL(result);
}

/**************************************************
 * Point ops
 **************************************************/

Datum gist_point_compress(PG_FUNCTION_ARGS)
{
    GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);

    /* Point, actually */
    if (entry->leafkey) {
        BOX *box = (BOX *)palloc(sizeof(BOX));
        Point *point = DatumGetPointP(entry->key);
        GISTENTRY *retval = (GISTENTRY *)palloc(sizeof(GISTENTRY));

        box->high = box->low = *point;

        gistentryinit(*retval, BoxPGetDatum(box), entry->rel, entry->page, entry->offset, FALSE);

        PG_RETURN_POINTER(retval);
    }

    PG_RETURN_POINTER(entry);
}

#define point_point_distance(p1, p2) \
    DatumGetFloat8(DirectFunctionCall2(point_distance, PointPGetDatum(p1), PointPGetDatum(p2)))

static double computeDistance(bool isLeaf, BOX *box, Point *point)
{
    double result = 0.0;

    if (isLeaf) {
        /* simple point to point distance */
        result = point_point_distance(point, &box->low);
    } else if (point->x <= box->high.x && point->x >= box->low.x && point->y <= box->high.y && point->y >= box->low.y) {
        /* point inside the box */
        result = 0.0;
    } else if (point->x <= box->high.x && point->x >= box->low.x) {
        /* point is over or below box */
        Assert(box->low.y <= box->high.y);
        if (point->y > box->high.y) {
            result = point->y - box->high.y;
        } else if (point->y < box->low.y) {
            result = box->low.y - point->y;
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("inconsistent point values")));
        }
    } else if (point->y <= box->high.y && point->y >= box->low.y) {
        /* point is to left or right of box */
        Assert(box->low.x <= box->high.x);
        if (point->x > box->high.x) {
            result = point->x - box->high.x;
        } else if (point->x < box->low.x) {
            result = box->low.x - point->x;
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("inconsistent point values")));
        }
    } else {
        /* closest point will be a vertex */
        Point p;
        result = point_point_distance(point, &box->low);
        double subresult = point_point_distance(point, &box->high);
        if (result > subresult) {
            result = subresult;
        }

        p.x = box->low.x;
        p.y = box->high.y;
        subresult = point_point_distance(point, &p);
        if (result > subresult) {
            result = subresult;
        }

        p.x = box->high.x;
        p.y = box->low.y;
        subresult = point_point_distance(point, &p);
        if (result > subresult) {
            result = subresult;
        }
    }

    return result;
}

static bool gist_point_consistent_internal(StrategyNumber strategy, bool isLeaf, BOX *key, Point *query)
{
    bool result = false;

    switch (strategy) {
        case RTLeftStrategyNumber:
            result = FPlt(key->low.x, query->x);
            break;
        case RTRightStrategyNumber:
            result = FPgt(key->high.x, query->x);
            break;
        case RTAboveStrategyNumber:
            result = FPgt(key->high.y, query->y);
            break;
        case RTBelowStrategyNumber:
            result = FPlt(key->low.y, query->y);
            break;
        case RTSameStrategyNumber:
            if (isLeaf) {
                /* key.high must equal key.low, so we can disregard it */
                result = (FPeq(key->low.x, query->x) && FPeq(key->low.y, query->y));
            } else {
                result = (FPle(query->x, key->high.x) && FPge(query->x, key->low.x) && FPle(query->y, key->high.y) &&
                          FPge(query->y, key->low.y));
            }
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unknown strategy number: %d", strategy)));
    }

    return result;
}

#define GeoStrategyNumberOffset 20
#define PointStrategyNumberGroup 0
#define BoxStrategyNumberGroup 1
#define PolygonStrategyNumberGroup 2
#define CircleStrategyNumberGroup 3

Datum gist_point_consistent(PG_FUNCTION_ARGS)
{
    GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
    StrategyNumber strategy = (StrategyNumber)PG_GETARG_UINT16(2);
    bool *recheck = (bool *)PG_GETARG_POINTER(4);
    bool result = false;
    StrategyNumber strategyGroup = strategy / GeoStrategyNumberOffset;

    switch (strategyGroup) {
        case PointStrategyNumberGroup:
            result = gist_point_consistent_internal(strategy % GeoStrategyNumberOffset, GIST_LEAF(entry),
                                                    DatumGetBoxP(entry->key), PG_GETARG_POINT_P(1));
            *recheck = false;
            break;
        case BoxStrategyNumberGroup: {
            /*
             * The only operator in this group is point <@ box (on_pb), so
             * we needn't examine strategy again.
             *
             * For historical reasons, on_pb uses exact rather than fuzzy
             * comparisons.  We could use box_overlap when at an internal
             * page, but that would lead to possibly visiting child pages
             * uselessly, because box_overlap uses fuzzy comparisons.
             * Instead we write a non-fuzzy overlap test.
             * The same code will also serve for leaf-page tests,
             * since leaf keys have high == low.
             */
            BOX *query = PG_GETARG_BOX_P(1);
            BOX *key = DatumGetBoxP(entry->key);
            result = (key->high.x >= query->low.x && key->low.x <= query->high.x && key->high.y >= query->low.y &&
                      key->low.y <= query->high.y);
            *recheck = false;
            break;
        }
        case PolygonStrategyNumberGroup: {
            POLYGON *query = PG_GETARG_POLYGON_P(1);

            result = DatumGetBool(DirectFunctionCall5(gist_poly_consistent, PointerGetDatum(entry),
                                                      PolygonPGetDatum(query), Int16GetDatum(RTOverlapStrategyNumber),
                                                      0, PointerGetDatum(recheck)));
            if (GIST_LEAF(entry) && result) {
                /*
                 * We are on leaf page and quick check shows overlapping
                 * of polygon's bounding box and point
                 */
                BOX *box = DatumGetBoxP(entry->key);

                Assert(box->high.x == box->low.x && box->high.y == box->low.y);
                result = DatumGetBool(
                    DirectFunctionCall2(poly_contain_pt, PolygonPGetDatum(query), PointPGetDatum(&box->high)));
                *recheck = false;
            }
        } break;
        case CircleStrategyNumberGroup: {
            CIRCLE *query = PG_GETARG_CIRCLE_P(1);

            result = DatumGetBool(DirectFunctionCall5(gist_circle_consistent, PointerGetDatum(entry),
                                                      CirclePGetDatum(query), Int16GetDatum(RTOverlapStrategyNumber), 0,
                                                      PointerGetDatum(recheck)));
            if (GIST_LEAF(entry) && result) {
                /*
                 * We are on leaf page and quick check shows overlapping
                 * of polygon's bounding box and point
                 */
                BOX *box = DatumGetBoxP(entry->key);

                Assert(box->high.x == box->low.x && box->high.y == box->low.y);
                result = DatumGetBool(
                    DirectFunctionCall2(circle_contain_pt, CirclePGetDatum(query), PointPGetDatum(&box->high)));
                *recheck = false;
            }
            break;
        }
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unknown strategy number: %d", strategy)));
            result = false; /* keep compiler quiet */
    }

    PG_RETURN_BOOL(result);
}

Datum gist_point_distance(PG_FUNCTION_ARGS)
{
    GISTENTRY *entry = (GISTENTRY *)PG_GETARG_POINTER(0);
    StrategyNumber strategy = (StrategyNumber)PG_GETARG_UINT16(2);
    double distance;
    StrategyNumber strategyGroup = strategy / GeoStrategyNumberOffset;
    bool* recheck = (bool *) PG_GETARG_POINTER(4);

    switch (strategyGroup) {
        case PointStrategyNumberGroup:
            distance = computeDistance(GIST_LEAF(entry), DatumGetBoxP(entry->key), PG_GETARG_POINT_P(1));
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unknown strategy number: %d", strategy)));
            distance = 0.0; /* keep compiler quiet */
    }

    *recheck = false;
    PG_RETURN_FLOAT8(distance);
}

static float8 gist_bbox_distance(GISTENTRY *entry, Datum query, StrategyNumber strategy)
{
    StrategyNumber strategyGroup = strategy / GeoStrategyNumberOffset;
    if (strategyGroup == PointStrategyNumberGroup) {
        return computeDistance(false, DatumGetBoxP(entry->key), DatumGetPointP(query));
    }
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unknown strategy number: %d", strategy)));
    return 0.0f; /* keep compiler quiet */
}

/*
 * The inexact GiST distance methods for geometric types that store bounding
 * boxes.
 *
 * Compute lossy distance from point to index entries.  The result is inexact
 * because index entries are bounding boxes, not the exact shapes of the
 * indexed geometric types.  We use distance from point to MBR of index entry.
 * This is a lower bound estimate of distance from point to indexed geometric
 * type.
 */
Datum gist_circle_distance(PG_FUNCTION_ARGS)
{
    GISTENTRY* entry = (GISTENTRY *) PG_GETARG_POINTER(0);
    Datum query = PG_GETARG_DATUM(1);
    StrategyNumber strategy = (StrategyNumber) PG_GETARG_UINT16(2);

    bool* recheck = (bool *) PG_GETARG_POINTER(4);
    float8 distance;

    distance = gist_bbox_distance(entry, query, strategy);
    *recheck = true;

    PG_RETURN_FLOAT8(distance);
}
