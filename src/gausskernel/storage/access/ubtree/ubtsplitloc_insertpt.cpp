/* -------------------------------------------------------------------------
 *
 * ubtsplitloc_insertpt.cpp
 * 	  Choose split point code for openGauss btree implementation (insertpt).
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
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ubtree/ubtsplitloc_insertpt.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/btree_insertpt_splitloc.h"
#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/genam.h"

#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/proc.h"
#include "utils/inval.h"
#include "utils/snapmgr.h"
#include "utils/fmgroids.h"
#include "utils/datum.h"
#include "catalog/pg_statistic.h"
#include "catalog/indexing.h"

static OffsetNumber BTreeFindsplitlocInsertptInternal(Relation rel, Page page, BlockNumber blkno,
    OffsetNumber newitemoff, Size newitemsz, bool *newitemonleft, IndexTuple newitem, Size opaqueSize);
static OffsetNumber BTreeFindsplitlocInsertptBuffer(Relation rel, Buffer buf, OffsetNumber newitemoff,
    Size newitemsz, bool *newitemonleft, IndexTuple newitem, Size opaqueSize);

static Size UbtInsertptItemSizeStd(Page page, OffsetNumber offnum, void *ctx)
{
    ItemId itemid;

    (void)ctx;
    itemid = PageGetItemId(page, offnum);
    return MAXALIGN(ItemIdGetLength(itemid)) + sizeof(ItemIdData);
}

static bool UbtInsertptAfterNewitemoff(BTreeInsertptFindSplitData *state, OffsetNumber maxoff, int leaffillfactor,
    bool *usemult)
{
    int16 nkeyatts;
    ItemId itemid;
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

    if (state->newitemsz > MAXALIGN(sizeof(IndexTupleData) + sizeof(int64) * 2) + sizeof(ItemIdData))
        return false;

    if (state->newitemoff > maxoff) {
        itemid = PageGetItemId(state->page, maxoff);
        tup = (IndexTuple)PageGetItem(state->page, itemid);
        keepnatts = UBTreeKeepNattsFast(state->rel, tup, state->newitem);
        if (keepnatts > 1 && keepnatts <= nkeyatts) {
            *usemult = true;
            return true;
        }

        return false;
    }

    itemid = PageGetItemId(state->page, OffsetNumberPrev(state->newitemoff));
    tup = (IndexTuple)PageGetItem(state->page, itemid);
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

static IndexTuple UbtInsertptSplitLastleft(BTreeInsertptFindSplitData *state, BTreeInsertptSplitPoint *split)
{
    ItemId itemid;

    if (split->newitemonleft && split->firstoldonright == state->newitemoff)
        return state->newitem;

    itemid = PageGetItemId(state->page, OffsetNumberPrev(split->firstoldonright));
    return (IndexTuple)PageGetItem(state->page, itemid);
}

static IndexTuple UbtInsertptSplitFirstright(BTreeInsertptFindSplitData *state, BTreeInsertptSplitPoint *split)
{
    ItemId itemid;

    if (!split->newitemonleft && split->firstoldonright == state->newitemoff)
        return state->newitem;

    itemid = PageGetItemId(state->page, split->firstoldonright);
    return (IndexTuple)PageGetItem(state->page, itemid);
}

static IndexTuple UbtInsertptGetHikey(Page page)
{
    ItemId itemid;

    itemid = PageGetItemId(page, P_HIKEY);
    return (IndexTuple)PageGetItem(page, itemid);
}

static int UbtInsertptSplitPenalty(BTreeInsertptFindSplitData *state, BTreeInsertptSplitPoint *split)
{
    IndexTuple lastleftuple;
    IndexTuple firstrighttuple;

    if (!state->is_leaf) {
        ItemId itemid;

        if (!split->newitemonleft && split->firstoldonright == state->newitemoff)
            return (int)state->newitemsz;

        itemid = PageGetItemId(state->page, split->firstoldonright);

        return (int)(MAXALIGN(ItemIdGetLength(itemid)) + sizeof(ItemIdData));
    }

    lastleftuple = UbtInsertptSplitLastleft(state, split);
    firstrighttuple = UbtInsertptSplitFirstright(state, split);

    Assert(lastleftuple != firstrighttuple);
    return UBTreeKeepNattsFast(state->rel, lastleftuple, firstrighttuple);
}

static const BTreeInsertptSplitOps ubt_insertpt_ops = { UbtInsertptAfterNewitemoff, UbtInsertptSplitLastleft,
    UbtInsertptSplitFirstright, UbtInsertptGetHikey, UbtInsertptSplitPenalty };

/*
 * 	UBTreeFindsplitloc() -- find an appropriate place to split a page.
 */
OffsetNumber UBTreeFindsplitlocInsertpt(Relation rel, Buffer buf, OffsetNumber newitemoff, Size newitemsz,
    bool *newitemonleft, IndexTuple newitem)
{
    return BTreeFindsplitlocInsertptBuffer(rel, buf, newitemoff, newitemsz, newitemonleft, newitem,
        sizeof(UBTPageOpaqueData));
}

OffsetNumber BTFindsplitlocInsertpt(Relation rel, Buffer buf, OffsetNumber newitemoff, Size newitemsz,
    bool *newitemonleft, IndexTuple newitem)
{
    return BTreeFindsplitlocInsertptBuffer(rel, buf, newitemoff, newitemsz, newitemonleft, newitem,
        sizeof(BTPageOpaqueData));
}

static OffsetNumber BTreeFindsplitlocInsertptBuffer(Relation rel, Buffer buf, OffsetNumber newitemoff,
    Size newitemsz, bool *newitemonleft, IndexTuple newitem, Size opaqueSize)
{
    return BTreeFindsplitlocInsertptInternal(rel, BufferGetPage(buf), BufferGetBlockNumber(buf), newitemoff,
        newitemsz, newitemonleft, newitem, opaqueSize);
}

static OffsetNumber BTreeFindsplitlocInsertptInternal(Relation rel, Page page, BlockNumber blkno,
    OffsetNumber newitemoff, Size newitemsz, bool *newitemonleft, IndexTuple newitem, Size opaqueSize)
{
    BTPageOpaqueInternal opaque;
    int leftspace, rightspace, olddataitemstotal, leaffillfactor;
    BTreeInsertptFindSplitData state;
    ItemId itemid;
    OffsetNumber maxoff;

    opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    maxoff = PageGetMaxOffsetNumber(page);

    newitemsz += sizeof(ItemIdData);

    leftspace = rightspace = PageGetPageSize(page) - SizeOfPageHeaderData - MAXALIGN(opaqueSize);

    if (!P_RIGHTMOST(opaque)) {
        itemid = PageGetItemId(page, P_HIKEY);
        rightspace -= (int)(MAXALIGN(ItemIdGetLength(itemid)) + sizeof(ItemIdData));
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

    BTreeInsertptCollectSplitCandidates(&state, P_FIRSTDATAKEY(opaque), maxoff, newitemoff, UbtInsertptItemSizeStd,
        NULL, sizeof(ItemIdData), true);

    return BTreeInsertptFinishChooseSplitLocation(&state, blkno, maxoff, newitemoff, leaffillfactor, newitemonleft,
        &ubt_insertpt_ops);
}
