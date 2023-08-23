/* -------------------------------------------------------------------------
 *
 * unbtsplitloc.cpp
 * 	  Choose split point code for default openGauss btree implementation.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ubtree/ubtsplitloc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/nbtree.h"
#include "access/genam.h"

#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "utils/inval.h"
#include "utils/snapmgr.h"

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
} FindSplitData;

static void UBTreeChecksplitloc(FindSplitData* state, OffsetNumber firstoldonright, bool newitemonleft,
    int olddataitemstoleft, Size firstoldonrightsz);

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
OffsetNumber UBTreeFindsplitloc(Relation rel, Buffer buf, OffsetNumber newitemoff, Size newitemsz, bool* newitemonleft)
{
    UBTPageOpaqueInternal opaque;
    OffsetNumber offnum;
    OffsetNumber maxoff;
    ItemId itemid;
    FindSplitData state;
    int leftspace, rightspace, goodenough, olddataitemstotal, olddataitemstoleft;
    bool goodenoughfound = false;
    Page page = BufferGetPage(buf);

    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);

    /* Passed-in newitemsz is MAXALIGNED but does not include line pointer */
    newitemsz += sizeof(ItemIdData);

    /* Total free space available on a btree page, after fixed overhead */
    leftspace = rightspace = PageGetPageSize(page) - SizeOfPageHeaderData - MAXALIGN(sizeof(UBTPageOpaqueData));

    /* The right page will have the same high key as the old page */
    if (!P_RIGHTMOST(opaque)) {
        itemid = PageGetItemId(page, P_HIKEY);
        rightspace -= (int)(MAXALIGN(ItemIdGetLength(itemid)) + sizeof(ItemIdData));
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
    maxoff = PageGetMaxOffsetNumber(page);

    for (offnum = P_FIRSTDATAKEY(opaque); offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        Size itemsz;

        itemid = PageGetItemId(page, offnum);
        itemsz = MAXALIGN(ItemIdGetLength(itemid)) + sizeof(ItemIdData);

        /*
         * Will the new item go to left or right of split?
         */
        if (offnum > newitemoff) {
            UBTreeChecksplitloc(&state, offnum, true, olddataitemstoleft, itemsz);
        } else if (offnum < newitemoff) {
            UBTreeChecksplitloc(&state, offnum, false, olddataitemstoleft, itemsz);
        } else {
            /* need to try it both ways! */
            UBTreeChecksplitloc(&state, offnum, true, olddataitemstoleft, itemsz);
            UBTreeChecksplitloc(&state, offnum, false, olddataitemstoleft, itemsz);
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
        UBTreeChecksplitloc(&state, newitemoff, false, olddataitemstotal, 0);
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

    *newitemonleft = state.newitemonleft;
    return state.firstright;
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
static void UBTreeChecksplitloc(FindSplitData* state, OffsetNumber firstoldonright, bool newitemonleft,
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
        int indexTupleDataSize = (MAXALIGN(sizeof(IndexTupleData)) - TXNINFOSIZE);
        rightfree += (int)firstrightitemsz - (int)(indexTupleDataSize + sizeof(ItemIdData));
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

