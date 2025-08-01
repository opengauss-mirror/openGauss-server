/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * --------------------------------------------------------------------------------------
 *
 * ubtpcrsplitloc.cpp
 *        Choose split point code for default openGauss btree implementation.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/ubtreepcr/ubtpcrsplitloc.cpp
 *
 * --------------------------------------------------------------------------------------
 */


#include "access/ubtreepcr.h"

const int MAX_LEAF_INTERVAL = 9;
const int MAX_INTERNAL_INTERVAL = 18;
typedef enum {
    /* strategy for searching through materialized list of split points */
    SPLIT_DEFAULT,         /* give some weight to truncation */
    SPLIT_MANY_DUPLICATES, /* find minimally distinguishing point */
    SPLIT_SINGLE_VALUE     /* leave left page almost full */
} FindSplitStrat;

typedef struct {
    /* details of free space left by split */
    int16 curdelta;  /* current leftfree/rightfree delta */
    int16 leftfree;  /* space left on left page post-split */
    int16 rightfree; /* space left on right page post-split */

    /* split point identifying fields (returned by UBTreeFindsplitloc) */
    OffsetNumber firstoldonright; /* first item on new right page */
    bool newitemonleft;           /* new item goes on left, or right? */
} SplitPoint;

typedef struct {
    /* context data for UBTreeRecsplitloc */
    Relation rel;            /* index relation */
    Page page;               /* page undergoing split */
    IndexTuple newitem;      /* new item (cause of page split) */
    Size newitemsz;          /* size of new item to be inserted */
    bool is_leaf;            /* T if splitting a leaf page */
    bool is_rightmost;       /* T if splitting a rightmost page */
    OffsetNumber newitemoff; /* where the new item is to be inserted */
    int leftspace;           /* space available for items on left page */
    int rightspace;          /* space available for items on right page */
    int olddataitemstotal;   /* space taken by old items */
    Size minfirstrightsz;    /* smallest firstoldonright tuple size */

    /* candidate split point data */
    int maxsplits;      /* maximum number of splits */
    int nsplits;        /* current number of splits */
    SplitPoint *splits; /* all candidate split points for page */
    int interval;       /* current range of acceptable split points */
} FindSplitData;

typedef struct {
    /* context data for UBTreeChecksplitloc */
    Size newitemsz;          /* size of new item to be inserted */
    int fillfactor;          /* needed when splitting rightmost page */
    bool is_leaf;            /* T if splitting a leaf page */
    bool is_rightmost;       /* T if splitting a rightmost page */
    OffsetNumber newitemoff; /* where the new item is to be inserted */
    int leftspace;           /* space available for items on left page */
    int rightspace;          /* space available for items on right page */
    int olddataitemstotal;   /* space taken by old items */

    bool have_split; /* found a valid split? */

    /* these fields valid only if have_split is true */
    bool newitemonleft;      /* new item on left or right of best split */
    OffsetNumber firstright; /* best split point */
    int best_delta;          /* best size delta so far */
} FindSplitDataForLocation;

/*
 * Subroutine to record a particular point between two tuples (possibly the
 * new item) on page (ie, combination of firstright and newitemonleft
 * settings) in *state for later analysis.  This is also a convenient point
 * to check if the split is legal (if it isn't, it won't be recorded).
 *
 * firstoldonright is the offset of the first item on the original page that
 * goes to the right page, and firstoldonrightsz is the size of that tuple.
 * firstoldonright can be > max offset, which means that all the old items go
 * to the left page and only the new item goes to the right page.  In that
 * case, firstoldonrightsz is not used.
 *
 * olddataitemstoleft is the total size of all old items to the left of the
 * split point that is recorded here when legal.  Should not include
 * newitemsz, since that is handled here.
 */
static void UBTreePCRRecsplitloc(FindSplitData *state, OffsetNumber firstoldonright, bool newitemonleft,
    int olddataitemstoleft, Size firstoldonrightsz)
{
    int16 leftfree, rightfree;
    Size firstrightitemsz;
    bool newitemisfirstonright;

    /* Is the new item going to be the first item on the right page? */
    newitemisfirstonright = (firstoldonright == state->newitemoff && !newitemonleft);

    if (newitemisfirstonright) {
        firstrightitemsz = state->newitemsz;
    } else {
        firstrightitemsz = firstoldonrightsz;
    }

    /* Account for all the old tuples */
    leftfree = state->leftspace - olddataitemstoleft;
    rightfree = state->rightspace - (state->olddataitemstotal - olddataitemstoleft);

    /*
     * The first item on the right page becomes the high key of the left page;
     * therefore it counts against left space as well as right space (we
     * cannot assume that suffix truncation will make it any smaller).  When
     * index has included attributes, then those attributes of left page high
     * key will be truncated leaving that page with slightly more free space.
     * However, that shouldn't affect our ability to find valid split
     * location, since we err in the direction of being pessimistic about free
     * space on the left half.  Besides, even when suffix truncation of
     * non-TID attributes occurs, the new high key often won't even be a
     * single MAXALIGN() quantum smaller than the firstright tuple it's based
     * on.
     *
     * If we are on the leaf level, assume that suffix truncation cannot avoid
     * adding a heap TID to the left half's new high key when splitting at the
     * leaf level.  In practice the new high key will often be smaller and
     * will rarely be larger, but conservatively assume the worst case.
     */
    if (state->is_leaf) {
        leftfree -= (int16)(firstrightitemsz + MAXALIGN(sizeof(ItemPointerData)));
    } else {
        leftfree -= (int16)firstrightitemsz;
    }

    /* account for the new item */
    if (newitemonleft) {
        leftfree -= (int16)state->newitemsz;
    } else {
        rightfree -= (int16)state->newitemsz;
    }
    /*
     * If we are not on the leaf level, we will be able to discard the key
     * data from the first item that winds up on the right page.
     */
    if (!state->is_leaf) {
        int indexTupleDataSize = (MAXALIGN(sizeof(IndexTupleData)));
        rightfree += (int16)firstrightitemsz - (int16)(indexTupleDataSize + sizeof(UBTreeItemIdData));
    }

    /* Record split if legal */
    if (leftfree >= 0 && rightfree >= 0) {
        Assert(state->nsplits < state->maxsplits);

        /* Determine smallest firstright item size on page */
        state->minfirstrightsz = Min(state->minfirstrightsz, firstrightitemsz);

        state->splits[state->nsplits].curdelta = 0;
        state->splits[state->nsplits].leftfree = leftfree;
        state->splits[state->nsplits].rightfree = rightfree;
        state->splits[state->nsplits].firstoldonright = firstoldonright;
        state->splits[state->nsplits].newitemonleft = newitemonleft;
        state->nsplits++;
    }
}

/*
 * Subroutine for determining if two heap TIDS are "adjacent".
 *
 * Adjacent means that the high TID is very likely to have been inserted into
 * heap relation immediately after the low TID, probably during the current
 * transaction.
 */
static bool UBTreePCRAdjacenthtid(ItemPointer lowhtid, ItemPointer highhtid)
{
    BlockNumber lowblk, highblk;

    lowblk = ItemPointerGetBlockNumber(lowhtid);
    highblk = ItemPointerGetBlockNumber(highhtid);
    /* Make optimistic assumption of adjacency when heap blocks match */
    if (lowblk == highblk)
        return true;

    /* When heap block one up, second offset should be FirstOffsetNumber */
    if (lowblk + 1 == highblk && ItemPointerGetOffsetNumber(highhtid) == FirstOffsetNumber)
        return true;

    return false;
}


/*
 * Subroutine to determine whether or not a non-rightmost leaf page should be
 * split immediately after the would-be original page offset for the
 * new/incoming tuple (or should have leaf fillfactor applied when new item is
 * to the right on original page).  This is appropriate when there is a
 * pattern of localized monotonically increasing insertions into a composite
 * index, where leading attribute values form local groupings, and we
 * anticipate further insertions of the same/current grouping (new item's
 * grouping) in the near future.  This can be thought of as a variation on
 * applying leaf fillfactor during rightmost leaf page splits, since cases
 * that benefit will converge on packing leaf pages leaffillfactor% full over
 * time.
 *
 * We may leave extra free space remaining on the rightmost page of a "most
 * significant column" grouping of tuples if that grouping never ends up
 * having future insertions that use the free space.  That effect is
 * self-limiting; a future grouping that becomes the "nearest on the right"
 * grouping of the affected grouping usually puts the extra free space to good
 * use.
 *
 * Caller uses optimization when routine returns true, though the exact action
 * taken by caller varies.  Caller uses original leaf page fillfactor in
 * standard way rather than using the new item offset directly when *usemult
 * was also set to true here.  Otherwise, caller applies optimization by
 * locating the legal split point that makes the new tuple the very last tuple
 * on the left side of the split.
 */
static bool UBTreePCRAfternewitemoff(FindSplitData *state, OffsetNumber maxoff, int leaffillfactor, bool *usemult)
{
    int16 nkeyatts;
    UBTreeItemId itemid;
    IndexTuple tup;
    int keepnatts;

    Assert(state->is_leaf && !state->is_rightmost);

    nkeyatts = (state->rel->rd_rel->relnatts);

    /* Single key indexes not considered here */
    if (nkeyatts == 1) {
        return false;
    }

    /* Ascending insertion pattern never inferred when new item is first */
    if (state->newitemoff == P_FIRSTKEY) {
        return false;
    }

    /*
     * Only apply optimization on pages with equisized tuples, since ordinal
     * keys are likely to be fixed-width.  Testing if the new tuple is
     * variable width directly might also work, but that fails to apply the
     * optimization to indexes with a numeric_ops attribute.
     *
     * Conclude that page has equisized tuples when the new item is the same
     * width as the smallest item observed during pass over page, and other
     * non-pivot tuples must be the same width as well.  (Note that the
     * possibly-truncated existing high key isn't counted in
     * olddataitemstotal, and must be subtracted from maxoff.)
     */
    if (state->newitemsz != state->minfirstrightsz) {
        return false;
    }
    if ((int)state->newitemsz * (maxoff - 1) != state->olddataitemstotal) {
        return false;
    }

    /*
     * Avoid applying optimization when tuples are wider than a tuple
     * consisting of two non-NULL int8/int64 attributes (or four non-NULL
     * int4/int32 attributes)
     */
    if (state->newitemsz > MAXALIGN(sizeof(IndexTupleData)) + sizeof(UBTreeItemIdData)) {
        return false;
    }

    /*
     * At least the first attribute's value must be equal to the corresponding
     * value in previous tuple to apply optimization.  New item cannot be a
     * duplicate, either.
     *
     * Handle case where new item is to the right of all items on the existing
     * page.  This is suggestive of monotonically increasing insertions in
     * itself, so the "heap TID adjacency" test is not applied here.
     */
    if (state->newitemoff > maxoff) {
        itemid = UBTreePCRGetRowPtr(state->page, maxoff);
        tup = (IndexTuple)UBTreePCRGetIndexTupleByItemId(state->page, itemid);
        keepnatts = UBTreeKeepNattsFast(state->rel, tup, state->newitem);
        if (keepnatts > 1 && keepnatts <= nkeyatts) {
            *usemult = true;
            return true;
        }

        return false;
    }

    /*
     * "Low cardinality leading column, high cardinality suffix column"
     * indexes with a random insertion pattern (e.g., an index with a boolean
     * column, such as an index on '(book_is_in_print, book_isbn)') present us
     * with a risk of consistently misapplying the optimization.  We're
     * willing to accept very occasional misapplication of the optimization,
     * provided the cases where we get it wrong are rare and self-limiting.
     *
     * Heap TID adjacency strongly suggests that the item just to the left was
     * inserted very recently, which limits overapplication of the
     * optimization.  Besides, all inappropriate cases triggered here will
     * still split in the middle of the page on average.
     */
    itemid = UBTreePCRGetRowPtr(state->page, OffsetNumberPrev(state->newitemoff));
    tup = (IndexTuple)UBTreePCRGetIndexTupleByItemId(state->page, itemid);
    /* Do cheaper test first */
    if (!UBTreePCRAdjacenthtid(&tup->t_tid, &state->newitem->t_tid)) {
        return false;
    }
    /* Check same conditions as rightmost item case, too */
    keepnatts = UBTreeKeepNattsFast(state->rel, tup, state->newitem);
    if (keepnatts > 1 && keepnatts <= nkeyatts) {
        double interp = (double)state->newitemoff / ((double)maxoff + 1);
        double leaffillfactormult = (double)leaffillfactor / 100.0;

        /*
         * Don't allow caller to split after a new item when it will result in
         * a split point to the right of the point that a leaf fillfactor
         * split would use -- have caller apply leaf fillfactor instead
         */
        *usemult = interp > leaffillfactormult;

        return true;
    }

    return false;
}

/*
 * qsort-style comparator used by UBTreeDeltasortsplits()
 */
static int UBTreePCRSplitcmp(const void *arg1, const void *arg2)
{
    SplitPoint *split1 = (SplitPoint *)arg1;
    SplitPoint *split2 = (SplitPoint *)arg2;

    if (split1->curdelta > split2->curdelta)
        return 1;
    if (split1->curdelta < split2->curdelta)
        return -1;

    return 0;
}


/*
 * Subroutine to assign space deltas to materialized array of candidate split
 * points based on current fillfactor, and to sort array using that fillfactor
 */
static void UBTreePCRDeltasortsplits(FindSplitData *state, double fillfactormult, bool usemult)
{
    for (int i = 0; i < state->nsplits; i++) {
        SplitPoint *split = state->splits + i;
        int16 delta;

        if (usemult)
            delta = fillfactormult * split->leftfree - (1.0 - fillfactormult) * split->rightfree;
        else
            delta = split->leftfree - split->rightfree;

        if (delta < 0)
            delta = -delta;

        /* Save delta */
        split->curdelta = delta;
    }

    qsort(state->splits, state->nsplits, sizeof(SplitPoint), UBTreePCRSplitcmp);
}

/*
 * Subroutine to locate leftmost and rightmost splits for current/default
 * split interval.  Note that it will be the same split iff there is only one
 * split in interval.
 */
static void UBTreePCRIntervalEdges(const FindSplitData *state, SplitPoint **leftinterval, SplitPoint **rightinterval)
{
    int highsplit = Min(state->interval, state->nsplits);
    SplitPoint *deltaoptimal;

    deltaoptimal = state->splits;
    *leftinterval = NULL;
    *rightinterval = NULL;

    /*
     * Delta is an absolute distance to optimal split point, so both the
     * leftmost and rightmost split point will usually be at the end of the
     * array
     */
    for (int i = highsplit - 1; i >= 0; i--) {
        SplitPoint *distant = state->splits + i;

        if (distant->firstoldonright < deltaoptimal->firstoldonright) {
            if (*leftinterval == NULL)
                *leftinterval = distant;
        } else if (distant->firstoldonright > deltaoptimal->firstoldonright) {
            if (*rightinterval == NULL)
                *rightinterval = distant;
        } else if (!distant->newitemonleft && deltaoptimal->newitemonleft) {
            /*
             * "incoming tuple will become first on right page" (distant) is
             * to the left of "incoming tuple will become last on left page"
             * (delta-optimal)
             */
            Assert(distant->firstoldonright == state->newitemoff);
            if (*leftinterval == NULL)
                *leftinterval = distant;
        } else if (distant->newitemonleft && !deltaoptimal->newitemonleft) {
            /*
             * "incoming tuple will become last on left page" (distant) is to
             * the right of "incoming tuple will become first on right page"
             * (delta-optimal)
             */
            Assert(distant->firstoldonright == state->newitemoff);
            if (*rightinterval == NULL)
                *rightinterval = distant;
        } else {
            /* There was only one or two splits in initial split interval */
            Assert(distant == deltaoptimal);
            if (*leftinterval == NULL)
                *leftinterval = distant;
            if (*rightinterval == NULL)
                *rightinterval = distant;
        }

        if (*leftinterval && *rightinterval) {
            return;
        }
    }

    Assert(false);
}


/*
 * Subroutine to get a lastleft IndexTuple for a split point from page
 */
static inline IndexTuple UBTreePCRSplitLastleft(FindSplitData *state, SplitPoint *split)
{
    UBTreeItemId itemid;

    if (split->newitemonleft && split->firstoldonright == state->newitemoff)
        return state->newitem;

    itemid = UBTreePCRGetRowPtr(state->page, OffsetNumberPrev(split->firstoldonright));
    return (IndexTuple)UBTreePCRGetIndexTupleByItemId(state->page, itemid);
}

/*
 * Subroutine to get a firstright IndexTuple for a split point from page
 */
static inline IndexTuple UBTreePCRSplitFirstright(FindSplitData *state, SplitPoint *split)
{
    UBTreeItemId itemid;

    if (!split->newitemonleft && split->firstoldonright == state->newitemoff)
        return state->newitem;

    itemid = UBTreePCRGetRowPtr(state->page, split->firstoldonright);
    return (IndexTuple)UBTreePCRGetIndexTupleByItemId(state->page, itemid);
}

/*
 * Subroutine to decide whether split should use default strategy/initial
 * split interval, or whether it should finish splitting the page using
 * alternative strategies (this is only possible with leaf pages).
 *
 * Caller uses alternative strategy (or sticks with default strategy) based
 * on how *strategy is set here.  Return value is "perfect penalty", which is
 * passed to UBTreeBestsplitloc() as a final constraint on how far caller is
 * willing to go to avoid appending a heap TID when using the many duplicates
 * strategy (it also saves UBTreeBestsplitloc() useless cycles).
 */
static int UBTreePCRStrategy(FindSplitData *state, SplitPoint *leftpage, SplitPoint *rightpage,
    FindSplitStrat *strategy)
{
    IndexTuple leftmost, rightmost;
    SplitPoint *leftinterval = NULL;
    SplitPoint *rightinterval = NULL;
    int perfectpenalty;
    int indnkeyatts = (state->rel)->rd_rel->relnatts;

    /* Assume that alternative strategy won't be used for now */
    *strategy = SPLIT_DEFAULT;

    /*
     * Use smallest observed first right item size for entire page as perfect
     * penalty on internal pages.  This can save cycles in the common case
     * where most or all splits (not just splits within interval) have first
     * right tuples that are the same size.
     */
    if (!state->is_leaf) {
        return state->minfirstrightsz;
    }

    /*
     * Use leftmost and rightmost tuples from leftmost and rightmost splits in
     * current split interval
     */
    UBTreePCRIntervalEdges(state, &leftinterval, &rightinterval);
    leftmost = UBTreePCRSplitLastleft(state, leftinterval);
    rightmost = UBTreePCRSplitFirstright(state, rightinterval);

    /*
     * If initial split interval can produce a split point that will at least
     * avoid appending a heap TID in new high key, we're done.  Finish split
     * with default strategy and initial split interval.
     */
    perfectpenalty = UBTreeKeepNattsFast(state->rel, leftmost, rightmost);
    if (perfectpenalty <= indnkeyatts) {
        return perfectpenalty;
    }

    /*
     * Work out how caller should finish split when even their "perfect"
     * penalty for initial/default split interval indicates that the interval
     * does not contain even a single split that avoids appending a heap TID.
     *
     * Use the leftmost split's lastleft tuple and the rightmost split's
     * firstright tuple to assess every possible split.
     */
    leftmost = UBTreePCRSplitLastleft(state, leftpage);
    rightmost = UBTreePCRSplitFirstright(state, rightpage);

    /*
     * If page (including new item) has many duplicates but is not entirely
     * full of duplicates, a many duplicates strategy split will be performed.
     * If page is entirely full of duplicates, a single value strategy split
     * will be performed.
     */
    perfectpenalty = UBTreeKeepNattsFast(state->rel, leftmost, rightmost);
    if (perfectpenalty <= indnkeyatts) {
        *strategy = SPLIT_MANY_DUPLICATES;

        /*
         * Many duplicates strategy should split at either side the group of
         * duplicates that enclose the delta-optimal split point.  Return
         * indnkeyatts rather than the true perfect penalty to make that
         * happen.  (If perfectpenalty was returned here then low cardinality
         * composite indexes could have continual unbalanced splits.)
         *
         * Note that caller won't go through with a many duplicates split in
         * rare cases where it looks like there are ever-decreasing insertions
         * to the immediate right of the split point.  This must happen just
         * before a final decision is made, within UBTreeBestsplitloc().
         */
        return indnkeyatts;
    /*
     * Single value strategy is only appropriate with ever-increasing heap
     * TIDs; otherwise, original default strategy split should proceed to
     * avoid pathological performance.  Use page high key to infer if this is
     * the rightmost page among pages that store the same duplicate value.
     * This should not prevent insertions of heap TIDs that are slightly out
     * of order from using single value strategy, since that's expected with
     * concurrent inserters of the same duplicate value.
     */
    } else if (state->is_rightmost) {
        *strategy = SPLIT_SINGLE_VALUE;
    } else {
        UBTreeItemId itemid;
        IndexTuple hikey;

        itemid = UBTreePCRGetRowPtr(state->page, P_HIKEY);
        hikey = (IndexTuple)UBTreePCRGetIndexTupleByItemId(state->page, itemid);
        perfectpenalty = UBTreeKeepNattsFast(state->rel, hikey, state->newitem);
        if (perfectpenalty <= indnkeyatts) {
            *strategy = SPLIT_SINGLE_VALUE;
        } else {
            /*
             * Have caller finish split using default strategy, since page
             * does not appear to be the rightmost page for duplicates of the
             * value the page is filled with
             */
        }
    }

    return perfectpenalty;
}

/*
 * Subroutine to find penalty for caller's candidate split point.
 *
 * On leaf pages, penalty is the attribute number that distinguishes each side
 * of a split.  It's the last attribute that needs to be included in new high
 * key for left page.  It can be greater than the number of key attributes in
 * cases where a heap TID will need to be appended during truncation.
 *
 * On internal pages, penalty is simply the size of the first item on the
 * right half of the split (including line pointer overhead).  This tuple will
 * become the new high key for the left page.
 */
static int UBTreePCRSplitPenalty(FindSplitData *state, SplitPoint *split)
{
    IndexTuple lastleftuple;
    IndexTuple firstrighttuple;

    if (!state->is_leaf) {
        UBTreeItemId itemid;

        if (!split->newitemonleft && split->firstoldonright == state->newitemoff)
            return state->newitemsz;

        itemid = UBTreePCRGetRowPtr(state->page, split->firstoldonright);

        return MAXALIGN(IndexTupleSize(UBTreePCRGetIndexTupleByItemId(state->page, itemid)))
            + sizeof(UBTreeItemIdData);
    }

    lastleftuple = UBTreePCRSplitLastleft(state, split);
    firstrighttuple = UBTreePCRSplitFirstright(state, split);

    Assert(lastleftuple != firstrighttuple);
    return UBTreeKeepNattsFast(state->rel, lastleftuple, firstrighttuple);
}

/*
 * Subroutine to find the "best" split point among candidate split points.
 * The best split point is the split point with the lowest penalty among split
 * points that fall within current/final split interval.  Penalty is an
 * abstract score, with a definition that varies depending on whether we're
 * splitting a leaf page or an internal page.  See UBTreeSplitPenalty() for
 * details.
 *
 * "perfectpenalty" is assumed to be the lowest possible penalty among
 * candidate split points.  This allows us to return early without wasting
 * cycles on calculating the first differing attribute for all candidate
 * splits when that clearly cannot improve our choice (or when we only want a
 * minimally distinguishing split point, and don't want to make the split any
 * more unbalanced than is necessary).
 *
 * We return the index of the first existing tuple that should go on the right
 * page, plus a boolean indicating if new item is on left of split point.
 */
static OffsetNumber UBTreePCRBestsplitloc(FindSplitData *state, int perfectpenalty, bool *newitemonleft,
    FindSplitStrat strategy)
{
    int bestpenalty, lowsplit;
    int highsplit = Min(state->interval, state->nsplits);
    SplitPoint *final = NULL;

    bestpenalty = INT_MAX;
    lowsplit = 0;
    for (int i = lowsplit; i < highsplit; i++) {
        int penalty;

        penalty = UBTreePCRSplitPenalty(state, state->splits + i);
        if (penalty <= perfectpenalty) {
            bestpenalty = penalty;
            lowsplit = i;
            break;
        }

        if (penalty < bestpenalty) {
            bestpenalty = penalty;
            lowsplit = i;
        }
    }

    final = &state->splits[lowsplit];

    /*
     * There is a risk that the "many duplicates" strategy will repeatedly do
     * the wrong thing when there are monotonically decreasing insertions to
     * the right of a large group of duplicates.   Repeated splits could leave
     * a succession of right half pages with free space that can never be
     * used.  This must be avoided.
     *
     * Consider the example of the leftmost page in a single integer attribute
     * NULLS FIRST index which is almost filled with NULLs.  Monotonically
     * decreasing integer insertions might cause the same leftmost page to
     * split repeatedly at the same point.  Each split derives its new high
     * key from the lowest current value to the immediate right of the large
     * group of NULLs, which will always be higher than all future integer
     * insertions, directing all future integer insertions to the same
     * leftmost page.
     */
    if (strategy == SPLIT_MANY_DUPLICATES && !state->is_rightmost && !final->newitemonleft &&
        final->firstoldonright >= state->newitemoff && final->firstoldonright < state->newitemoff + MAX_LEAF_INTERVAL) {
        /*
         * Avoid the problem by performing a 50:50 split when the new item is
         * just to the right of the would-be "many duplicates" split point.
         */
        final = &state->splits[0];
    }

    *newitemonleft = final->newitemonleft;
    return final->firstoldonright;
}


/*
 * 	UBTreePCRFindsplitlocInsertpt() -- find an appropriate place to split a page.
 *
 * The idea here is to equalize the free space that will be on each split
 * page, *after accounting for the inserted tuple*.  (If we fail to account
 * for it, we might find ourselves with too little room on the page that
 * it needs to go into!)
 *
 * If the page is the rightmost page on its level, we instead try to arrange
 * to leave the left split page fillfactor% full.  In this way, when we are
 * inserting successively increasing keys (consider sequences, timestamps,
 * etc) we will end up with a tree whose pages are about fillfactor% full,
 * instead of the 50% full result that we'd get without this special case.
 * This is the same as nbtsort.c produces for a newly-created tree.  Note
 * that leaf and nonleaf pages use different fillfactors.
 *
 * We are passed the intended insert position of the new tuple, expressed as
 * the offsetnumber of the tuple it must go in front of.  (This could be
 * maxoff+1 if the tuple is to go at the end.)
 *
 * We return the index of the first existing tuple that should go on the
 * righthand page, plus a boolean indicating whether the new tuple goes on
 * the left or right page.	The bool is necessary to disambiguate the case
 * where firstright == newitemoff.
 */
OffsetNumber UBTreePCRFindsplitlocInsertpt(Relation rel, Buffer buf, OffsetNumber newitemoff, Size newitemsz,
    bool *newitemonleft, IndexTuple newitem)
{
    UBTPCRPageOpaque opaque;
    int leftspace, rightspace, olddataitemstotal, olddataitemstoleft, perfectpenalty, leaffillfactor;
    FindSplitData state;
    FindSplitStrat strategy;
    UBTreeItemId itemid;
    OffsetNumber offnum, maxoff, foundfirstright;
    double fillfactormult;
    bool usemult = false;
    SplitPoint leftpage, rightpage;
    Page page = BufferGetPage(buf);

    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    maxoff = UBTreePCRPageGetMaxOffsetNumber(page);

    /* Passed-in newitemsz is MAXALIGNED but does not include line pointer */
    newitemsz += sizeof(UBTreeItemIdData);

    /* Total free space available on a btree page, after fixed overhead */
    leftspace = rightspace = PageGetPageSize(page) - SizeOfPageHeaderData -
        SizeOfUBTreeTDData(page) - MAXALIGN(sizeof(UBTPCRPageOpaqueData));

    /* The right page will have the same high key as the old page */
    if (!P_RIGHTMOST(opaque)) {
        itemid = UBTreePCRGetRowPtr(page, P_HIKEY);
        rightspace -= (int)(MAXALIGN(IndexTupleSize(UBTreePCRGetIndexTupleByItemId(page, itemid)))
            + sizeof(UBTreeItemIdData));
    }

    /* Count up total space in data items without actually scanning 'em */
    olddataitemstotal = rightspace - (int)PageGetExactFreeSpace(page);
    leaffillfactor = RelationGetFillFactor(rel, BTREE_DEFAULT_FILLFACTOR);

    state.newitemsz = newitemsz;
    state.rel = rel;
    state.page = page;
    state.newitem = newitem;
    state.is_leaf = P_ISLEAF(opaque);
    state.is_rightmost = P_RIGHTMOST(opaque);
    state.leftspace = leftspace;
    state.rightspace = rightspace;
    state.olddataitemstotal = olddataitemstotal;
    state.minfirstrightsz = SIZE_MAX;
    state.newitemoff = newitemoff;

    /*
     * maxsplits should never exceed maxoff because there will be at most as
     * many candidate split points as there are points _between_ tuples, once
     * you imagine that the new item is already on the original page (the
     * final number of splits may be slightly lower because not all points
     * between tuples will be legal).
     */
    state.maxsplits = maxoff;
    state.splits = (SplitPoint *)palloc(sizeof(SplitPoint) * state.maxsplits);
    state.nsplits = 0;

    /*
     * Finding the best possible split would require checking all the possible
     * split points, because of the high-key and left-key special cases.
     * That's probably more work than it's worth; instead, stop as soon as we
     * find a "good-enough" split, where good-enough is defined as an
     * imbalance in free space of no more than pagesize/16 (arbitrary...) This
     * should let us stop near the middle on most pages, instead of plowing to
     * the end.
     */

    /*
     * Scan through the data items and calculate space usage for a split at
     * each possible position.
     */
    olddataitemstoleft = 0;

    for (offnum = P_FIRSTDATAKEY(opaque); offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        Size itemsz;

        itemid = UBTreePCRGetRowPtr(page, offnum);
        itemsz = MAXALIGN(IndexTupleSize(UBTreePCRGetIndexTupleByItemId(page, itemid))) + sizeof(UBTreeItemIdData);

        /*
         * Will the new item go to left or right of split?
         */
        if (offnum > newitemoff)
            UBTreePCRRecsplitloc(&state, offnum, true, olddataitemstoleft, itemsz);

        else if (offnum < newitemoff)
            UBTreePCRRecsplitloc(&state, offnum, false, olddataitemstoleft, itemsz);
        else {
            /* need to try it both ways! */
            UBTreePCRRecsplitloc(&state, offnum, true, olddataitemstoleft, itemsz);
            UBTreePCRRecsplitloc(&state, offnum, false, olddataitemstoleft, itemsz);
        }

        olddataitemstoleft += itemsz;
    }

    /*
     * Record a split after all original page data items, but before newitem.
     * (Though only when it's possible that newitem will end up alone on new
     * right page.)
     */
    Assert(olddataitemstoleft == olddataitemstotal);

    /*
     * If the new item goes as the last item, check for splitting so that all
     * the old items go to the left page and the new item goes to the right
     * page.
     */
    if (newitemoff > maxoff) {
        UBTreePCRRecsplitloc(&state, newitemoff, false, olddataitemstotal, 0);
    }

    /*
     * I believe it is not possible to fail to find a feasible split, but just
     * in case ...
     */
    if (state.nsplits == 0) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("could not find a feasible split point for index \"%s\" at blkno %u. "
                       "newitemoff %u, newitemsize %lu",
                       RelationGetRelationName(rel), BufferGetBlockNumber(buf), newitemoff, newitemsz)));
    }

    /*
     * Start search for a split point among list of legal split points.  Give
     * primary consideration to equalizing available free space in each half
     * of the split initially (start with default strategy), while applying
     * rightmost and split-after-new-item optimizations where appropriate.
     * Either of the two other fallback strategies may be required for cases
     * with a large number of duplicates around the original/space-optimal
     * split point.
     *
     * Default strategy gives some weight to suffix truncation in deciding a
     * split point on leaf pages.  It attempts to select a split point where a
     * distinguishing attribute appears earlier in the new high key for the
     * left side of the split, in order to maximize the number of trailing
     * attributes that can be truncated away.  Only candidate split points
     * that imply an acceptable balance of free space on each side are
     * considered.
     */
    if (!state.is_leaf) {
        /* fillfactormult only used on rightmost page */
        usemult = state.is_rightmost;
        fillfactormult = BTREE_NONLEAF_FILLFACTOR / 100.0;
    } else if (state.is_rightmost) {
        /* Rightmost leaf page --  fillfactormult always used */
        usemult = true;
        fillfactormult = leaffillfactor / 100.0;
    } else if (UBTreePCRAfternewitemoff(&state, maxoff, leaffillfactor, &usemult)) {
        /*
         * New item inserted at rightmost point among a localized grouping on
         * a leaf page -- apply "split after new item" optimization, either by
         * applying leaf fillfactor multiplier, or by choosing the exact split
         * point that leaves the new item as last on the left. (usemult is set
         * for us.)
         */
        if (usemult) {
            /* fillfactormult should be set based on leaf fillfactor */
            fillfactormult = leaffillfactor / 100.0;
        } else {
            /* find precise split point after newitemoff */
            for (int i = 0; i < state.nsplits; i++) {
                SplitPoint *split = state.splits + i;

                if (split->newitemonleft && newitemoff == split->firstoldonright) {
                    pfree(state.splits);
                    *newitemonleft = true;
                    return newitemoff;
                }
            }

            /*
             * Cannot legally split after newitemoff; proceed with split
             * without using fillfactor multiplier.  This is defensive, and
             * should never be needed in practice.
             */
            fillfactormult = 0.50;
        }
    } else {
        /* Other leaf page.  50:50 page split. */
        usemult = false;
        /* fillfactormult not used, but be tidy */
        fillfactormult = 0.50;
    }

    /*
     * Set an initial limit on the split interval/number of candidate split
     * points as appropriate.  The "Prefix B-Trees" paper refers to this as
     * sigma l for leaf splits and sigma b for internal ("branch") splits.
     * It's hard to provide a theoretical justification for the initial size
     * of the split interval, though it's clear that a small split interval
     * makes suffix truncation much more effective without noticeably
     * affecting space utilization over time.
     */
    state.interval = Min(Max(1, state.nsplits * 0.05), state.is_leaf ? MAX_LEAF_INTERVAL : MAX_INTERNAL_INTERVAL);

    /*
     * Save leftmost and rightmost splits for page before original ordinal
     * sort order is lost by delta/fillfactormult sort
     */
    leftpage = state.splits[0];
    rightpage = state.splits[state.nsplits - 1];

    /* Give split points a fillfactormult-wise delta, and sort on deltas */
    UBTreePCRDeltasortsplits(&state, fillfactormult, usemult);

    /*
     * Determine if default strategy/split interval will produce a
     * sufficiently distinguishing split, or if we should change strategies.
     * Alternative strategies change the range of split points that are
     * considered acceptable (split interval), and possibly change
     * fillfactormult, in order to deal with pages with a large number of
     * duplicates gracefully.
     *
     * Pass low and high splits for the entire page (actually, they're for an
     * imaginary version of the page that includes newitem).  These are used
     * when the initial split interval encloses split points that are full of
     * duplicates, and we need to consider if it's even possible to avoid
     * appending a heap TID.
     */
    perfectpenalty = UBTreePCRStrategy(&state, &leftpage, &rightpage, &strategy);

    if (strategy == SPLIT_DEFAULT) {
        /*
         * Default strategy worked out (always works out with internal page).
         * Original split interval still stands.
         */

    /*
     * Many duplicates strategy is used when a heap TID would otherwise be
     * appended, but the page isn't completely full of logical duplicates.
     *
     * The split interval is widened to include all legal candidate split
     * points.  There might be a few as two distinct values in the whole-page
     * split interval, though it's also possible that most of the values on
     * the page are unique.  The final split point will either be to the
     * immediate left or to the immediate right of the group of duplicate
     * tuples that enclose the first/delta-optimal split point (perfect
     * penalty was set so that the lowest delta split point that avoids
     * appending a heap TID will be chosen).  Maximizing the number of
     * attributes that can be truncated away is not a goal of the many
     * duplicates strategy.
     *
     * Single value strategy is used when it is impossible to avoid appending
     * a heap TID.  It arranges to leave the left page very full.  This
     * maximizes space utilization in cases where tuples with the same
     * attribute values span many pages.  Newly inserted duplicates will tend
     * to have higher heap TID values, so we'll end up splitting to the right
     * consistently.  (Single value strategy is harmless though not
     * particularly useful with !heapkeyspace indexes.)
     */
    } else if (strategy == SPLIT_MANY_DUPLICATES) {
        Assert(state.is_leaf);
        /* Shouldn't try to truncate away extra user attributes */
        Assert(perfectpenalty == (state.rel)->rd_rel->relnatts);
        /* No need to resort splits -- no change in fillfactormult/deltas */
        state.interval = state.nsplits;
    } else if (strategy == SPLIT_SINGLE_VALUE) {
        Assert(state.is_leaf);
        /* Split near the end of the page */
        usemult = true;
        fillfactormult = BTREE_SINGLEVAL_FILLFACTOR / 100.0;
        /* Resort split points with new delta */
        UBTreePCRDeltasortsplits(&state, fillfactormult, usemult);
        /* Appending a heap TID is unavoidable, so interval of 1 is fine */
        state.interval = 1;
    }

    /*
     * Search among acceptable split points (using final split interval) for
     * the entry that has the lowest penalty, and is therefore expected to
     * maximize fan-out.  Sets *newitemonleft for us.
     */
    foundfirstright = UBTreePCRBestsplitloc(&state, perfectpenalty, newitemonleft, strategy);
    pfree(state.splits);

    return foundfirstright;
}

/*
 * Subroutine to analyze a particular possible split choice (ie, firstright
 * and newitemonleft settings), and record the best split so far in *state.
 *
 * firstoldonright is the offset of the first item on the original page
 * that goes to the right page, and firstoldonrightsz is the size of that
 * tuple. firstoldonright can be > max offset, which means that all the old
 * items go to the left page and only the new item goes to the right page.
 * In that case, firstoldonrightsz is not used.
 *
 * olddataitemstoleft is the total size of all old items to the left of
 * firstoldonright.
 */
static void UBTreePCRChecksplitloc(FindSplitDataForLocation* state, OffsetNumber firstoldonright, bool newitemonleft,
    int olddataitemstoleft, Size firstoldonrightsz)
{
    int leftfree, rightfree;
    Size firstrightitemsz;
    bool newitemisfirstonright = false;

    /* Is the new item going to be the first item on the right page? */
    newitemisfirstonright = (firstoldonright == state->newitemoff && !newitemonleft);
    if (newitemisfirstonright) {
        firstrightitemsz = state->newitemsz;
    } else {
        firstrightitemsz = firstoldonrightsz;
    }

    /* Account for all the old tuples */
    leftfree = state->leftspace - olddataitemstoleft;
    rightfree = state->rightspace - (state->olddataitemstotal - olddataitemstoleft);

    /*
     * The first item on the right page becomes the high key of the left page;
     * therefore it counts against left space as well as right space. When
     * index has included attribues, then those attributes of left page high
     * key will be truncate leaving that page with slightly more free space.
     * However, that shouldn't affect our ability to find valid split
     * location, because anyway split location should exists even without high
     * key truncation.
     */
    leftfree -= firstrightitemsz;

    /* account for the new item */
    if (newitemonleft) {
        leftfree -= (int)state->newitemsz;
    } else {
        rightfree -= (int)state->newitemsz;
    }

    /*
     * If we are not on the leaf level, we will be able to discard the key
     * data from the first item that winds up on the right page.
     */
    if (!state->is_leaf) {
        int indexTupleDataSize = (MAXALIGN(sizeof(IndexTupleData)));
        rightfree += (int)firstrightitemsz - (int)(indexTupleDataSize + sizeof(UBTreeItemIdData));
    }

    /*
     * If feasible split point, remember best delta.
     */
    if (leftfree >= 0 && rightfree >= 0) {
        int delta;

        if (state->is_rightmost) {
            /*
             * If splitting a rightmost page, try to put (100-fillfactor)% of
             * free space on left page. See comments for UBTreeFindsplitloc.
             */
            delta = (state->fillfactor * leftfree) - ((100.0 - state->fillfactor) * rightfree);
        } else {
            /* Otherwise, aim for equal free space on both sides */
            delta = leftfree - rightfree;
        }

        if (delta < 0) {
            delta = -delta;
        }
        if (!state->have_split || delta < state->best_delta) {
            state->have_split = true;
            state->newitemonleft = newitemonleft;
            state->firstright = firstoldonright;
            state->best_delta = delta;
        }
    }
}

/*
 * Find an appropriate place to split a page.
 *
 * The idea here is to equalize the free space that will be on each split
 * page, *after accounting for the inserted tuple*.  (If we fail to account
 * for it, we might find ourselves with too little room on the page that
 * it needs to go into!)
 *
 * If the page is the rightmost page on its level, we instead try to arrange
 * to leave the left split page fillfactor% full.  In this way, when we are
 * inserting successively increasing keys (consider sequences, timestamps,
 * etc) we will end up with a tree whose pages are about fillfactor% full,
 * instead of the 50% full result that we'd get without this special case.
 * This is the same as nbtsort.c produces for a newly-created tree.  Note
 * that leaf and nonleaf pages use different fillfactors.
 *
 * We are passed the intended insert position of the new tuple, expressed as
 * the offsetnumber of the tuple it must go in front of.  (This could be
 * maxoff+1 if the tuple is to go at the end.)
 *
 * We return the index of the first existing tuple that should go on the
 * righthand page, plus a boolean indicating whether the new tuple goes on
 * the left or right page. The bool is necessary to disambiguate the case
 * where firstright == newitemoff.
 *
 */
OffsetNumber UBTreePCRFindsplitloc(Relation rel, Buffer buf, OffsetNumber newitemoff, Size newitemsz,
    bool* newitemonleft)
{
    UBTPCRPageOpaque opaque;
    OffsetNumber offnum;
    OffsetNumber maxoff;
    UBTreeItemId itemid;
    FindSplitDataForLocation state;
    int leftspace, rightspace, goodenough, olddataitemstotal, olddataitemstoleft;
    bool goodenoughfound = false;
    Page page = BufferGetPage(buf);

    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);

    /* Passed-in newitemsz is MAXALIGNED but does not include line pointer */
    newitemsz += sizeof(UBTreeItemIdData);

    /* Total free space available on a btree page, after fixed overhead */
    leftspace = rightspace = PageGetPageSize(page) - SizeOfPageHeaderData -
        SizeOfUBTreeTDData(page) - MAXALIGN(sizeof(UBTPCRPageOpaqueData));

    /* The right page will have the same high key as the old page */
    if (!P_RIGHTMOST(opaque)) {
        itemid = UBTreePCRGetRowPtr(page, P_HIKEY);
        rightspace -= (int)(MAXALIGN(IndexTupleSize(UBTreePCRGetIndexTupleByItemId(page, itemid)))
            + sizeof(UBTreeItemIdData));
    }

    /* Count up total space in data items without actually scanning 'em */
    olddataitemstotal = rightspace - (int)PageGetExactFreeSpace(page);

    state.newitemsz = newitemsz;
    state.is_leaf = (P_ISLEAF(opaque) > 0) ? true : false;
    state.is_rightmost = P_RIGHTMOST(opaque);
    state.have_split = false;
    if (state.is_leaf) {
        state.fillfactor = RelationGetFillFactor(rel, BTREE_DEFAULT_FILLFACTOR);
    } else {
        state.fillfactor = BTREE_NONLEAF_FILLFACTOR;
    }
    state.newitemonleft = false; /* these just to keep compiler quiet */
    state.firstright = 0;
    state.best_delta = 0;
    state.leftspace = leftspace;
    state.rightspace = rightspace;
    state.olddataitemstotal = olddataitemstotal;
    state.newitemoff = newitemoff;

    /*
     * Finding the best possible split would require checking all the possible
     * split points, because of the high-key and left-key special cases.
     * That's probably more work than it's worth; instead, stop as soon as we
     * find a "good-enough" split, where good-enough is defined as an
     * imbalance in free space of no more than pagesize/16 (arbitrary...) This
     * should let us stop near the middle on most pages, instead of plowing to
     * the end.
     */
    goodenough = leftspace / 16;

    /*
     * Scan through the data items and calculate space usage for a split at
     * each possible position.
     */
    olddataitemstoleft = 0;
    goodenoughfound = false;
    maxoff = UBTreePCRPageGetMaxOffsetNumber(page);

    for (offnum = P_FIRSTDATAKEY(opaque); offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        Size itemsz;

        itemid = UBTreePCRGetRowPtr(page, offnum);
        itemsz = MAXALIGN(IndexTupleSize(UBTreePCRGetIndexTupleByItemId(page, itemid))) + sizeof(UBTreeItemIdData);

        /*
         * Will the new item go to left or right of split?
         */
        if (offnum > newitemoff) {
            UBTreePCRChecksplitloc(&state, offnum, true, olddataitemstoleft, itemsz);
        } else if (offnum < newitemoff) {
            UBTreePCRChecksplitloc(&state, offnum, false, olddataitemstoleft, itemsz);
        } else {
            /* need to try it both ways! */
            UBTreePCRChecksplitloc(&state, offnum, true, olddataitemstoleft, itemsz);
            UBTreePCRChecksplitloc(&state, offnum, false, olddataitemstoleft, itemsz);
        }

        /* Abort scan once we find a good-enough choice */
        if (state.have_split && state.best_delta <= goodenough) {
            goodenoughfound = true;
            break;
        }

        olddataitemstoleft += itemsz;
    }

    /*
     * If the new item goes as the last item, check for splitting so that all
     * the old items go to the left page and the new item goes to the right
     * page.
     */
    if (newitemoff > maxoff && !goodenoughfound) {
        UBTreePCRChecksplitloc(&state, newitemoff, false, olddataitemstotal, 0);
    }

    /*
     * I believe it is not possible to fail to find a feasible split, but just
     * in case ...
     */
    if (!state.have_split) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("could not find a feasible split point for index \"%s\" at blkno %u. "
                       "newitemoff %u, newitemsize %lu",
                       RelationGetRelationName(rel), BufferGetBlockNumber(buf), newitemoff, newitemsz)));
    }

    if (newitemonleft != NULL) {
        *newitemonleft = state.newitemonleft;
    }
    return state.firstright;
}
