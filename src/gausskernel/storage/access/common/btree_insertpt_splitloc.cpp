/* -------------------------------------------------------------------------
 *
 * btree_insertpt_splitloc.cpp
 *    Shared insert-position-aware btree split candidate collection and choice.
 *
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/common/btree_insertpt_splitloc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/btree_insertpt_splitloc.h"
#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/genam.h"
#include "miscadmin.h"

static int BTreeInsertptSplitcmp(const void *arg1, const void *arg2);

void BTreeInsertptRecsplitloc(BTreeInsertptFindSplitData *state, OffsetNumber firstoldonright, bool newitemonleft,
    int olddataitemstoleft, Size firstoldonrightsz, Size lineptr_sz, bool internal_tuple_minus_txn)
{
    int16 leftfree, rightfree;
    Size firstrightitemsz;
    bool newitemisfirstonright;

    newitemisfirstonright = (firstoldonright == state->newitemoff && !newitemonleft);

    if (newitemisfirstonright)
        firstrightitemsz = state->newitemsz;
    else
        firstrightitemsz = firstoldonrightsz;

    leftfree = state->leftspace - olddataitemstoleft;
    rightfree = state->rightspace - (state->olddataitemstotal - olddataitemstoleft);

    if (state->is_leaf)
        leftfree -= (int16)(firstrightitemsz + MAXALIGN(sizeof(ItemPointerData)));
    else
        leftfree -= (int16)firstrightitemsz;

    if (newitemonleft)
        leftfree -= (int16)state->newitemsz;
    else
        rightfree -= (int16)state->newitemsz;

    if (!state->is_leaf) {
        Size indexTupleDataSize = MAXALIGN(sizeof(IndexTupleData));

        if (internal_tuple_minus_txn)
            indexTupleDataSize -= TXNINFOSIZE;
        rightfree += (int16)firstrightitemsz - (int16)(indexTupleDataSize + lineptr_sz);
    }

    if (leftfree >= 0 && rightfree >= 0) {
        Assert(state->nsplits < state->maxsplits);

        state->minfirstrightsz = Min(state->minfirstrightsz, firstrightitemsz);

        state->splits[state->nsplits].curdelta = 0;
        state->splits[state->nsplits].leftfree = leftfree;
        state->splits[state->nsplits].rightfree = rightfree;
        state->splits[state->nsplits].firstoldonright = firstoldonright;
        state->splits[state->nsplits].newitemonleft = newitemonleft;
        state->nsplits++;
    }
}

void BTreeInsertptCollectSplitCandidates(BTreeInsertptFindSplitData *state, OffsetNumber firstdatakey,
    OffsetNumber maxoff, OffsetNumber newitemoff, BTreeInsertptItemSizeFn item_size_fn, void *item_ctx,
    Size lineptr_sz, bool internal_tuple_minus_txn)
{
    OffsetNumber offnum;
    int olddataitemstoleft = 0;

    for (offnum = firstdatakey; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        Size itemsz = item_size_fn(state->page, offnum, item_ctx);

        if (offnum > newitemoff)
            BTreeInsertptRecsplitloc(state, offnum, true, olddataitemstoleft, itemsz, lineptr_sz,
                internal_tuple_minus_txn);
        else if (offnum < newitemoff)
            BTreeInsertptRecsplitloc(state, offnum, false, olddataitemstoleft, itemsz, lineptr_sz,
                internal_tuple_minus_txn);
        else {
            BTreeInsertptRecsplitloc(state, offnum, true, olddataitemstoleft, itemsz, lineptr_sz,
                internal_tuple_minus_txn);
            BTreeInsertptRecsplitloc(state, offnum, false, olddataitemstoleft, itemsz, lineptr_sz,
                internal_tuple_minus_txn);
        }

        olddataitemstoleft += (int)itemsz;
    }

    Assert(olddataitemstoleft == state->olddataitemstotal);

    if (newitemoff > maxoff)
        BTreeInsertptRecsplitloc(state, newitemoff, false, olddataitemstoleft, 0, lineptr_sz, internal_tuple_minus_txn);
}

bool BTreeInsertptAdjacentHeapTid(ItemPointer lowhtid, ItemPointer highhtid)
{
    BlockNumber lowblk, highblk;

    lowblk = ItemPointerGetBlockNumber(lowhtid);
    highblk = ItemPointerGetBlockNumber(highhtid);
    if (lowblk == highblk)
        return true;

    if (lowblk + 1 == highblk && ItemPointerGetOffsetNumber(highhtid) == FirstOffsetNumber)
        return true;

    return false;
}

void BTreeInsertptDeltasortsplits(BTreeInsertptFindSplitData *state, double fillfactormult, bool usemult)
{
    for (int i = 0; i < state->nsplits; i++) {
        BTreeInsertptSplitPoint *split = state->splits + i;
        int16 delta;

        if (usemult)
            delta = (int16)(fillfactormult * split->leftfree - (1.0 - fillfactormult) * split->rightfree);
        else
            delta = (int16)(split->leftfree - split->rightfree);

        if (delta < 0)
            delta = (int16)-delta;

        split->curdelta = delta;
    }

    qsort(state->splits, state->nsplits, sizeof(BTreeInsertptSplitPoint), BTreeInsertptSplitcmp);
}

static int BTreeInsertptSplitcmp(const void *arg1, const void *arg2)
{
    BTreeInsertptSplitPoint *split1 = (BTreeInsertptSplitPoint *)arg1;
    BTreeInsertptSplitPoint *split2 = (BTreeInsertptSplitPoint *)arg2;

    if (split1->curdelta > split2->curdelta)
        return 1;
    if (split1->curdelta < split2->curdelta)
        return -1;

    return 0;
}

void BTreeInsertptIntervalEdges(const BTreeInsertptFindSplitData *state, BTreeInsertptSplitPoint **leftinterval,
    BTreeInsertptSplitPoint **rightinterval)
{
    int highsplit = Min(state->interval, state->nsplits);
    BTreeInsertptSplitPoint *deltaoptimal;

    deltaoptimal = state->splits;
    *leftinterval = NULL;
    *rightinterval = NULL;

    for (int i = highsplit - 1; i >= 0; i--) {
        BTreeInsertptSplitPoint *distant = state->splits + i;

        if (distant->firstoldonright < deltaoptimal->firstoldonright) {
            if (*leftinterval == NULL)
                *leftinterval = distant;
        } else if (distant->firstoldonright > deltaoptimal->firstoldonright) {
            if (*rightinterval == NULL)
                *rightinterval = distant;
        } else if (!distant->newitemonleft && deltaoptimal->newitemonleft) {
            Assert(distant->firstoldonright == state->newitemoff);
            if (*leftinterval == NULL)
                *leftinterval = distant;
        } else if (distant->newitemonleft && !deltaoptimal->newitemonleft) {
            Assert(distant->firstoldonright == state->newitemoff);
            if (*rightinterval == NULL)
                *rightinterval = distant;
        } else {
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

int BTreeInsertptStrategy(BTreeInsertptFindSplitData *state, BTreeInsertptSplitPoint *leftpage,
    BTreeInsertptSplitPoint *rightpage, BTreeInsertptFindStrat *strategy, const BTreeInsertptSplitOps *ops)
{
    IndexTuple leftmost, rightmost;
    BTreeInsertptSplitPoint *leftinterval = NULL;
    BTreeInsertptSplitPoint *rightinterval = NULL;
    int perfectpenalty;
    int indnkeyatts = state->rel->rd_rel->relnatts;

    *strategy = BTREE_INSERTPT_SPLIT_DEFAULT;

    if (!state->is_leaf)
        return (int)state->minfirstrightsz;

    BTreeInsertptIntervalEdges(state, &leftinterval, &rightinterval);
    leftmost = ops->split_last_left(state, leftinterval);
    rightmost = ops->split_first_right(state, rightinterval);

    perfectpenalty = UBTreeKeepNattsFast(state->rel, leftmost, rightmost);
    if (perfectpenalty <= indnkeyatts) {
        return perfectpenalty;
    }

    leftmost = ops->split_last_left(state, leftpage);
    rightmost = ops->split_first_right(state, rightpage);

    perfectpenalty = UBTreeKeepNattsFast(state->rel, leftmost, rightmost);
    if (perfectpenalty <= indnkeyatts) {
        *strategy = BTREE_INSERTPT_SPLIT_MANY_DUPLICATES;
        return indnkeyatts;
    } else if (state->is_rightmost) {
        *strategy = BTREE_INSERTPT_SPLIT_SINGLE_VALUE;
    } else {
        IndexTuple hikey = ops->get_hikey_tuple(state->page);

        perfectpenalty = UBTreeKeepNattsFast(state->rel, hikey, state->newitem);
        if (perfectpenalty <= indnkeyatts) {
            *strategy = BTREE_INSERTPT_SPLIT_SINGLE_VALUE;
        } else {
        }
    }

    return perfectpenalty;
}

OffsetNumber BTreeInsertptBestsplitloc(BTreeInsertptFindSplitData *state, int perfectpenalty, bool *newitemonleft,
    BTreeInsertptFindStrat strategy, const BTreeInsertptSplitOps *ops)
{
    int bestpenalty, lowsplit;
    int highsplit = Min(state->interval, state->nsplits);
    BTreeInsertptSplitPoint *final = NULL;

    bestpenalty = INT_MAX;
    lowsplit = 0;
    for (int i = lowsplit; i < highsplit; i++) {
        int penalty;

        penalty = ops->split_penalty(state, state->splits + i);
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

    if (strategy == BTREE_INSERTPT_SPLIT_MANY_DUPLICATES && !state->is_rightmost && !final->newitemonleft &&
        final->firstoldonright >= state->newitemoff &&
        final->firstoldonright < state->newitemoff + BTREE_INSERTPT_MAX_LEAF_INTERVAL) {
        final = &state->splits[0];
    }

    *newitemonleft = final->newitemonleft;
    return final->firstoldonright;
}

OffsetNumber BTreeInsertptFinishChooseSplitLocation(BTreeInsertptFindSplitData *state, BlockNumber blkno,
    OffsetNumber maxoff, OffsetNumber newitemoff, int leaffillfactor, bool *newitemonleft,
    const BTreeInsertptSplitOps *ops)
{
    BTreeInsertptFindStrat strategy;
    BTreeInsertptSplitPoint leftpage, rightpage;
    double fillfactormult;
    bool usemult = false;
    int perfectpenalty;
    OffsetNumber foundfirstright;

    if (state->nsplits == 0)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("could not find a feasible split point for index \"%s\" at blkno %u. "
                       "newitemoff %u, newitemsize %lu",
                       RelationGetRelationName(state->rel), blkno, newitemoff, state->newitemsz)));

    if (!state->is_leaf) {
        usemult = state->is_rightmost;
        fillfactormult = BTREE_NONLEAF_FILLFACTOR / 100.0;
    } else if (state->is_rightmost) {
        usemult = true;
        fillfactormult = leaffillfactor / 100.0;
    } else if (ops->after_newitemoff(state, maxoff, leaffillfactor, &usemult)) {
        if (usemult) {
            fillfactormult = leaffillfactor / 100.0;
        } else {
            for (int i = 0; i < state->nsplits; i++) {
                BTreeInsertptSplitPoint *split = state->splits + i;

                if (split->newitemonleft && newitemoff == split->firstoldonright) {
                    pfree(state->splits);
                    *newitemonleft = true;
                    return newitemoff;
                }
            }

            fillfactormult = 0.50;
        }
    } else {
        usemult = false;
        fillfactormult = 0.50;
    }

    state->interval = Min(Max(1, state->nsplits * 0.05),
        state->is_leaf ? BTREE_INSERTPT_MAX_LEAF_INTERVAL : BTREE_INSERTPT_MAX_INTERNAL_INTERVAL);

    leftpage = state->splits[0];
    rightpage = state->splits[state->nsplits - 1];

    BTreeInsertptDeltasortsplits(state, fillfactormult, usemult);

    perfectpenalty = BTreeInsertptStrategy(state, &leftpage, &rightpage, &strategy, ops);

    if (strategy == BTREE_INSERTPT_SPLIT_DEFAULT) {
    } else if (strategy == BTREE_INSERTPT_SPLIT_MANY_DUPLICATES) {
        Assert(state->is_leaf);
        Assert(perfectpenalty == state->rel->rd_rel->relnatts);
        state->interval = state->nsplits;
    } else if (strategy == BTREE_INSERTPT_SPLIT_SINGLE_VALUE) {
        Assert(state->is_leaf);
        usemult = true;
        fillfactormult = BTREE_SINGLEVAL_FILLFACTOR / 100.0;
        BTreeInsertptDeltasortsplits(state, fillfactormult, usemult);
        state->interval = 1;
    }

    foundfirstright = BTreeInsertptBestsplitloc(state, perfectpenalty, newitemonleft, strategy, ops);
    pfree(state->splits);

    return foundfirstright;
}
