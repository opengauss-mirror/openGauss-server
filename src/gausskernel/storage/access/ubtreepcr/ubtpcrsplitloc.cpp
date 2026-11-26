/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
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
#include "access/btree_insertpt_splitloc.h"
#include "access/ubtree.h"

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

static Size UbtpcrInsertptItemSize(Page page, OffsetNumber offnum, void *ctx)
{
    UBTreeItemId itemid;

    (void)ctx;
    itemid = UBTreePCRGetRowPtr(page, offnum);
    return MAXALIGN(IndexTupleSize(UBTreePCRGetIndexTupleByItemId(page, itemid))) + sizeof(UBTreeItemIdData);
}

static bool UbtpcrInsertptAfterNewitemoff(BTreeInsertptFindSplitData *state, OffsetNumber maxoff, int leaffillfactor,
    bool *usemult)
{
    int16 nkeyatts;
    UBTreeItemId itemid;
    IndexTuple tup;
    int keepnatts;

    Assert(state->is_leaf && !state->is_rightmost);

    nkeyatts = state->rel->rd_rel->relnatts;

    if (nkeyatts == 1)
        return false;

    if (state->newitemoff == P_FIRSTKEY)
        return false;

    if (state->newitemsz != state->minfirstrightsz)
        return false;
    if ((int)state->newitemsz * (maxoff - 1) != state->olddataitemstotal)
        return false;

    if (state->newitemsz > MAXALIGN(sizeof(IndexTupleData)) + sizeof(UBTreeItemIdData))
        return false;

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

    itemid = UBTreePCRGetRowPtr(state->page, OffsetNumberPrev(state->newitemoff));
    tup = (IndexTuple)UBTreePCRGetIndexTupleByItemId(state->page, itemid);
    if (!BTreeInsertptAdjacentHeapTid(&tup->t_tid, &state->newitem->t_tid))
        return false;
    keepnatts = UBTreeKeepNattsFast(state->rel, tup, state->newitem);
    if (keepnatts > 1 && keepnatts <= nkeyatts) {
        double interp = (double)state->newitemoff / ((double)maxoff + 1);
        double leaffillfactormult = (double)leaffillfactor / 100.0;

        *usemult = interp > leaffillfactormult;

        return true;
    }

    return false;
}

static IndexTuple UbtpcrInsertptSplitLastleft(BTreeInsertptFindSplitData *state, BTreeInsertptSplitPoint *split)
{
    UBTreeItemId itemid;

    if (split->newitemonleft && split->firstoldonright == state->newitemoff)
        return state->newitem;

    itemid = UBTreePCRGetRowPtr(state->page, OffsetNumberPrev(split->firstoldonright));
    return (IndexTuple)UBTreePCRGetIndexTupleByItemId(state->page, itemid);
}

static IndexTuple UbtpcrInsertptSplitFirstright(BTreeInsertptFindSplitData *state, BTreeInsertptSplitPoint *split)
{
    UBTreeItemId itemid;

    if (!split->newitemonleft && split->firstoldonright == state->newitemoff)
        return state->newitem;

    itemid = UBTreePCRGetRowPtr(state->page, split->firstoldonright);
    return (IndexTuple)UBTreePCRGetIndexTupleByItemId(state->page, itemid);
}

static IndexTuple UbtpcrInsertptGetHikey(Page page)
{
    UBTreeItemId itemid;

    itemid = UBTreePCRGetRowPtr(page, P_HIKEY);
    return (IndexTuple)UBTreePCRGetIndexTupleByItemId(page, itemid);
}

static int UbtpcrInsertptSplitPenalty(BTreeInsertptFindSplitData *state, BTreeInsertptSplitPoint *split)
{
    IndexTuple lastleftuple;
    IndexTuple firstrighttuple;

    if (!state->is_leaf) {
        UBTreeItemId itemid;

        if (!split->newitemonleft && split->firstoldonright == state->newitemoff)
            return (int)state->newitemsz;

        itemid = UBTreePCRGetRowPtr(state->page, split->firstoldonright);

        return (int)(MAXALIGN(IndexTupleSize(UBTreePCRGetIndexTupleByItemId(state->page, itemid)))
            + sizeof(UBTreeItemIdData));
    }

    lastleftuple = UbtpcrInsertptSplitLastleft(state, split);
    firstrighttuple = UbtpcrInsertptSplitFirstright(state, split);

    Assert(lastleftuple != firstrighttuple);
    return UBTreeKeepNattsFast(state->rel, lastleftuple, firstrighttuple);
}

static const BTreeInsertptSplitOps ubtpcr_insertpt_ops = { UbtpcrInsertptAfterNewitemoff, UbtpcrInsertptSplitLastleft,
    UbtpcrInsertptSplitFirstright, UbtpcrInsertptGetHikey, UbtpcrInsertptSplitPenalty };

/*
 * 	UBTreePCRFindsplitlocInsertpt() -- find an appropriate place to split a page.
 */
OffsetNumber UBTreePCRFindsplitlocInsertpt(Relation rel, Buffer buf, OffsetNumber newitemoff, Size newitemsz,
    bool *newitemonleft, IndexTuple newitem)
{
    UBTPCRPageOpaque opaque;
    int leftspace, rightspace, olddataitemstotal, leaffillfactor;
    BTreeInsertptFindSplitData state;
    UBTreeItemId itemid;
    OffsetNumber maxoff;
    Page page = BufferGetPage(buf);
    BlockNumber blkno = BufferGetBlockNumber(buf);

    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    maxoff = UBTreePCRPageGetMaxOffsetNumber(page);

    newitemsz += sizeof(UBTreeItemIdData);

    leftspace = rightspace = PageGetPageSize(page) - SizeOfPageHeaderData - SizeOfUBTreeTDData(page)
        - MAXALIGN(sizeof(UBTPCRPageOpaqueData));

    if (!P_RIGHTMOST(opaque)) {
        itemid = UBTreePCRGetRowPtr(page, P_HIKEY);
        rightspace -= (int)(MAXALIGN(IndexTupleSize(UBTreePCRGetIndexTupleByItemId(page, itemid)))
            + sizeof(UBTreeItemIdData));
    }

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

    state.maxsplits = maxoff;
    state.splits = (BTreeInsertptSplitPoint *)palloc(sizeof(BTreeInsertptSplitPoint) * state.maxsplits);
    state.nsplits = 0;

    BTreeInsertptCollectSplitCandidates(&state, P_FIRSTDATAKEY(opaque), maxoff, newitemoff, UbtpcrInsertptItemSize,
        NULL, sizeof(UBTreeItemIdData), false);

    return BTreeInsertptFinishChooseSplitLocation(&state, blkno, maxoff, newitemoff, leaffillfactor, newitemonleft,
        &ubtpcr_insertpt_ops);
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
