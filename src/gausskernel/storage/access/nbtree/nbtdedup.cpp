/*-------------------------------------------------------------------------
 *
 * nbtdedup.c
 *	  Deduplicate btree index tuples.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/nbtree/nbtdedup.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/nbtree.h"

#ifdef USE_ASSERT_CHECKING
static bool btree_dedup_posting_valid(IndexTuple posting);
#endif

IndexTuple btree_dedup_form_posting(IndexTuple base, ItemPointer heap_tids, int num_heap_tids)
{
    uint32 key_size, new_size;
    IndexTuple itup;
    errno_t rc = EOK;

    if (btree_tuple_is_posting(base))
        key_size = btree_tuple_get_posting_off(base);
    else
        key_size = IndexTupleSize(base);

    Assert(!btree_tuple_is_pivot(base));
    Assert(num_heap_tids > 0 && num_heap_tids <= PG_UINT16_MAX);
    Assert(key_size == MAXALIGN(key_size));

    if (num_heap_tids > 1)
        new_size = MAXALIGN(key_size + num_heap_tids * sizeof(ItemPointerData));
    else
        new_size = key_size;

    Assert(new_size <= INDEX_SIZE_MASK);
    Assert(new_size == MAXALIGN(new_size));

    itup = (IndexTuple)palloc0(new_size);

    rc = memcpy_s(itup, key_size, base, key_size);
    securec_check(rc, "", "");
    itup->t_info &= ~INDEX_SIZE_MASK;
    itup->t_info |= new_size;
    if (num_heap_tids > 1) {
        btree_tuple_set_posting(itup, num_heap_tids, key_size);
        Size nhtidSize = sizeof(ItemPointerData) * num_heap_tids;
        rc = memcpy_s(btree_tuple_get_posting(itup), nhtidSize, heap_tids, nhtidSize);
        securec_check(rc, "", "");
        Assert(btree_dedup_posting_valid(itup));
    } else {
        /* Form standard non-pivot tuple */
        itup->t_info &= ~INDEX_ALT_TID_MASK;
        ItemPointerCopy(heap_tids, &itup->t_tid);
        Assert(ItemPointerIsValid(&itup->t_tid));
    }

    return itup;
}

void btree_dedup_begin(BTDedupState state, IndexTuple base, OffsetNumber base_off)
{
    Assert(state->num_heap_tids == 0);
    Assert(state->num_items == 0);
    Assert(!btree_tuple_is_pivot(base));

    if (!btree_tuple_is_posting(base)) {
        errno_t rc = memcpy_s(state->heap_tids, sizeof(ItemPointerData), &base->t_tid, sizeof(ItemPointerData));
        securec_check(rc, "", "");
        state->num_heap_tids = 1;
        state->base_tuple_size = IndexTupleSize(base);
    } else {
        int new_posting = btree_tuple_get_nposting(base);
        Size npostingSize = sizeof(ItemPointerData) * new_posting;
        errno_t rc = memcpy_s(state->heap_tids, npostingSize, btree_tuple_get_posting(base), npostingSize);
        securec_check(rc, "", "");
        state->num_heap_tids = new_posting;
        /* basetupsize should not include existing posting list */
        state->base_tuple_size = btree_tuple_get_posting_off(base);
    }

    state->num_items = 1;
    state->base = base;
    state->base_off = base_off;
    state->size_freed = MAXALIGN(IndexTupleSize(base)) + sizeof(ItemIdData);
    state->intervals[state->num_intervals].base_off = state->base_off;
}

bool btree_dedup_merge(BTDedupState state, IndexTuple itup)
{
    int num_heap_tids;
    ItemPointer heap_tids;
    Size mergedtupsz;

    Assert(!btree_tuple_is_pivot(itup));

    if (!btree_tuple_is_posting(itup)) {
        num_heap_tids = 1;
        heap_tids = &itup->t_tid;
    } else {
        num_heap_tids = btree_tuple_get_nposting(itup);
        heap_tids = btree_tuple_get_posting(itup);
    }

    mergedtupsz = MAXALIGN(state->base_tuple_size + (state->num_heap_tids + num_heap_tids) * sizeof(ItemPointerData));

    if (mergedtupsz > state->max_posting_size) {
        if (state->num_heap_tids > 50) {
            state->num_max_items++;
        }

        return false;
    }

    state->num_items++;
    Size nhtidSize = sizeof(ItemPointerData) * num_heap_tids;
    errno_t rc = memcpy_s(state->heap_tids + state->num_heap_tids, nhtidSize, heap_tids, nhtidSize);
    securec_check(rc, "", "");
    state->num_heap_tids += num_heap_tids;
    state->size_freed += MAXALIGN(IndexTupleSize(itup)) + sizeof(ItemIdData);

    return true;
}

Size btree_dedup_end(Page newpage, BTDedupState state)
{
    OffsetNumber tupoff;
    Size tuplesz;
    Size spacesaving;

    Assert(state->num_items > 0);
    Assert(state->num_items <= state->num_heap_tids);
    Assert(state->intervals[state->num_intervals].base_off == state->base_off);

    tupoff = OffsetNumberNext(PageGetMaxOffsetNumber(newpage));
    if (state->num_items == 1) {
        tuplesz = IndexTupleSize(state->base);
        if (PageAddItem(newpage, (Item)state->base, tuplesz, tupoff, false, false) == InvalidOffsetNumber) {
            elog(ERROR, "deduplication failed to add tuple to page");
        }

        spacesaving = 0;
    } else {
        IndexTuple final = btree_dedup_form_posting(state->base, state->heap_tids, state->num_heap_tids);
        tuplesz = IndexTupleSize(final);
        Assert(tuplesz <= state->max_posting_size);

        state->intervals[state->num_intervals].num_items = state->num_items;

        Assert(tuplesz == MAXALIGN(IndexTupleSize(final)));
        if (PageAddItem(newpage, (Item) final, tuplesz, tupoff, false, false) == InvalidOffsetNumber) {
            elog(ERROR, "deduplication failed to add tuple to page");
        }

        pfree(final);
        spacesaving = state->size_freed - (tuplesz + sizeof(ItemIdData));
        state->num_intervals++;
        Assert(spacesaving > 0 && spacesaving < BLCKSZ);
    }

    state->num_heap_tids = 0;
    state->num_items = 0;
    state->size_freed = 0;

    return spacesaving;
}

void btree_dedup_write_wal(BTDedupState state, Buffer buf)
{
    XLogRecPtr recptr;
    xl_btree_dedup xlrec_dedup;

    xlrec_dedup.num_intervals = state->num_intervals;

    XLogBeginInsert();
    XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
    XLogRegisterData((char *)&xlrec_dedup, SizeOfBtreeDedup);

    XLogRegisterBufData(0, (char *)state->intervals, state->num_intervals * sizeof(BTDedupIntervalData));

    recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_DEDUP);

    Page page = BufferGetPage(buf);
    PageSetLSN(page, recptr);
}

void btree_dedup_single_value_fillfactor(Page page, BTDedupState state, Size newitemsz)
{
    Size leftfree;
    Size reduction;

    leftfree = PageGetPageSize(page) - SizeOfPageHeaderData - MAXALIGN(sizeof(BTPageOpaqueData));

    leftfree -= newitemsz + MAXALIGN(sizeof(ItemPointerData));

    reduction = leftfree * ((100 - BTREE_SINGLEVAL_FILLFACTOR) / 100.0);
    if (state->max_posting_size > reduction) {
        state->max_posting_size -= reduction;
    } else {
        state->max_posting_size = 0;
    }
}

bool btree_dedup_do_single_value(Relation rel, Page page, OffsetNumber minoff, IndexTuple newitem)
{
    int nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
    ItemId itemid;
    IndexTuple itup;

    itemid = PageGetItemId(page, minoff);
    itup = (IndexTuple)PageGetItem(page, itemid);

    if (btree_num_keep_atts_fast(rel, newitem, itup) > nkeyatts) {
        itemid = PageGetItemId(page, PageGetMaxOffsetNumber(page));
        itup = (IndexTuple)PageGetItem(page, itemid);
        if (btree_num_keep_atts_fast(rel, newitem, itup) > nkeyatts) {
            return true;
        }
    }

    return false;
}

void btree_dedup_page(Relation rel, Relation heapRel, Buffer buf, IndexTuple new_tuple, Size new_tuple_size)
{
    OffsetNumber off, minoff, maxoff;
    Page page = BufferGetPage(buf);
    BTPageOpaque opaque;
    Page tmpPage;
    OffsetNumber deletable[MaxIndexTuplesPerPage];
    BTDedupState dedupState;
    int ndeletable = 0;
    Size pagesaving = 0;
    int nkeyattrs = IndexRelationGetNumberOfKeyAttributes(rel);
    bool singlevalstrat = false;

    if (RelationIsInternal(rel)) {
        ereport(LOG, (errmsg("xyh_dedup_trace:relation is system table(%s,%u)", RelationGetRelationName(rel),
                             rel->rd_node.relNode)));
        return;
    }

    if (IndexRelationGetNumberOfAttributes(rel) != IndexRelationGetNumberOfKeyAttributes(rel)) {
        ereport(LOG, (errmsg("xyh_dedup_trace:relation is include index(%s,%u)", RelationGetRelationName(rel),
                             rel->rd_node.relNode)));
        return;
    }

    opaque = (BTPageOpaque)PageGetSpecialPointer(page);
    minoff = P_FIRSTDATAKEY(&opaque->bt_internal);
    maxoff = PageGetMaxOffsetNumber(page);

    for (off = minoff; off <= maxoff; off = OffsetNumberNext(off)) {
        ItemId itemid = PageGetItemId(page, off);
        if (ItemIdIsDead(itemid)) {
            deletable[ndeletable++] = off;
        }
    }

    Size newitemsz = new_tuple_size;
    newitemsz += sizeof(ItemIdData);

    if (ndeletable > 0) {
        _bt_delitems_delete(rel, buf, deletable, ndeletable, heapRel);

        if (PageGetFreeSpace(page) >= newitemsz) {
            return;
        }

        minoff = P_FIRSTDATAKEY(&opaque->bt_internal);
        maxoff = PageGetMaxOffsetNumber(page);
    }

    dedupState = (BTDedupState)palloc(sizeof(BTDedupStateData));
    dedupState->deduplicate = true;
    dedupState->num_max_items = 0;
    dedupState->max_posting_size = Min(BTREE_MAX_ITEM_SIZE(page) / 2, INDEX_SIZE_MASK);
    dedupState->base = NULL;
    dedupState->base_off = InvalidOffsetNumber;
    dedupState->base_tuple_size = 0;
    dedupState->heap_tids = (ItemPointer)palloc(dedupState->max_posting_size);
    dedupState->num_heap_tids = 0;
    dedupState->num_items = 0;
    dedupState->size_freed = 0;
    dedupState->num_intervals = 0;

    singlevalstrat = btree_dedup_do_single_value(rel, page, minoff, new_tuple);

    tmpPage = PageGetTempPageCopySpecial(page);
    PageSetLSN(tmpPage, PageGetLSN(page));

    if (!P_RIGHTMOST(&opaque->bt_internal)) {
        ItemId hitemid = PageGetItemId(page, P_HIKEY);
        Size hitemsz = ItemIdGetLength(hitemid);
        IndexTuple hitem = (IndexTuple)PageGetItem(page, hitemid);

        if (PageAddItem(tmpPage, (Item)hitem, hitemsz, P_HIKEY, false, false) == InvalidOffsetNumber) {
            elog(ERROR, "deduplication failed to add highkey");
        }
    }

    for (off = minoff; off <= maxoff; off = OffsetNumberNext(off)) {
        ItemId itemid = PageGetItemId(page, off);
        IndexTuple itup = (IndexTuple)PageGetItem(page, itemid);

        Assert(!ItemIdIsDead(itemid));

        if (off == minoff) {
            btree_dedup_begin(dedupState, itup, off);
        } else if (dedupState->deduplicate && btree_num_keep_atts_fast(rel, dedupState->base, itup) > nkeyattrs &&
                   btree_dedup_merge(dedupState, itup)) {
        } else {
            pagesaving += btree_dedup_end(tmpPage, dedupState);

            if (singlevalstrat) {
                if (dedupState->num_max_items == 5) {
                    btree_dedup_single_value_fillfactor(page, dedupState, newitemsz);
                } else if (dedupState->num_max_items == 6) {
                    dedupState->deduplicate = false;
                    singlevalstrat = false;
                }
            }

            btree_dedup_begin(dedupState, itup, off);
        }
    }

    pagesaving += btree_dedup_end(tmpPage, dedupState);

    if (dedupState->num_intervals == 0) {
        pfree(tmpPage);
        pfree(dedupState->heap_tids);
        pfree(dedupState);
        return;
    }

    if (P_HAS_GARBAGE(&opaque->bt_internal)) {
        BTPageOpaque nopaque = (BTPageOpaque)PageGetSpecialPointer(tmpPage);
        nopaque->bt_internal.btpo_flags &= ~BTP_HAS_GARBAGE;
    }

    START_CRIT_SECTION();

    PageRestoreTempPage(tmpPage, page);
    MarkBufferDirty(buf);

    if (RelationNeedsWAL(rel)) {
        btree_dedup_write_wal(dedupState, buf);
    }

    END_CRIT_SECTION();

    Assert(pagesaving < newitemsz || PageGetExactFreeSpace(page) >= newitemsz);

    pfree(dedupState->heap_tids);
    pfree(dedupState);
}

void btree_dedup_update_posting(BTVacuumPosting vac_posting)
{
    IndexTuple orig_tuple = vac_posting->itup;
    Assert(btree_dedup_posting_valid(orig_tuple));

    int num_heap_tids = btree_tuple_get_nposting(orig_tuple) - vac_posting->num_deleted_tids;
    Assert(num_heap_tids > 0 && num_heap_tids < btree_tuple_get_nposting(orig_tuple));

    uint32 key_size = btree_tuple_get_posting_off(orig_tuple);
    uint32 new_size;
    if (num_heap_tids > 1) {
        new_size = MAXALIGN(key_size + num_heap_tids * sizeof(ItemPointerData));
    } else {
        new_size = key_size;
    }

    Assert(new_size <= INDEX_SIZE_MASK);
    Assert(new_size == MAXALIGN(new_size));

    IndexTuple updated_tuple = (IndexTuple)palloc0(new_size);
    errno_t rc = memcpy_s(updated_tuple, key_size, orig_tuple, key_size);
    securec_check(rc, "", "");
    updated_tuple->t_info &= ~INDEX_SIZE_MASK;
    updated_tuple->t_info |= new_size;

    ItemPointer posting_list;
    if (num_heap_tids > 1) {
        btree_tuple_set_posting(updated_tuple, num_heap_tids, key_size);
        posting_list = btree_tuple_get_posting(updated_tuple);
    } else {
        updated_tuple->t_info &= ~INDEX_ALT_TID_MASK;
        posting_list = &updated_tuple->t_tid;
    }

    int posting_lem = 0;
    int delete_pos = 0;
    for (int i = 0; i < btree_tuple_get_nposting(orig_tuple); i++) {
        if (delete_pos < vac_posting->num_deleted_tids && vac_posting->delete_tids[delete_pos] == i) {
            delete_pos++;
            continue;
        }
        posting_list[posting_lem++] = *btree_tuple_get_posting_n(orig_tuple, i);
    }
    Assert(posting_lem == num_heap_tids);
    Assert(delete_pos == vac_posting->num_deleted_tids);
    Assert(num_heap_tids == 1 || btree_dedup_posting_valid(updated_tuple));
    Assert(num_heap_tids > 1 || ItemPointerIsValid(&updated_tuple->t_tid));

    vac_posting->itup = updated_tuple;
}

IndexTuple btree_dedup_swap_posting(IndexTuple newitem, IndexTuple orignal_posting, int posting_off)
{
    int num_heap_tids = btree_tuple_get_nposting(orignal_posting);

    Assert(btree_dedup_posting_valid(orignal_posting));

    if (!(posting_off > 0 && posting_off < num_heap_tids))
        elog(ERROR, "posting list tuple with %d items cannot be split at offset %d", num_heap_tids, posting_off);

    IndexTuple new_posting = CopyIndexTuple(orignal_posting);
    char *replace_pos = (char *)btree_tuple_get_posting_n(new_posting, posting_off);
    char *replace_pos_right = (char *)btree_tuple_get_posting_n(new_posting, posting_off + 1);
    size_t move_bytes = (num_heap_tids - posting_off - 1) * sizeof(ItemPointerData);
    if (move_bytes > 0) {
        errno_t rc = memmove_s(replace_pos_right, move_bytes, replace_pos, move_bytes);
        securec_check(rc, "", "");
    }

    Assert(!btree_tuple_is_pivot(newitem) && !btree_tuple_is_posting(newitem));
    ItemPointerCopy(&newitem->t_tid, (ItemPointer)replace_pos);

    ItemPointerCopy(btree_tuple_get_max_heap_tid(orignal_posting), &newitem->t_tid);

    Assert(ItemPointerCompare(btree_tuple_get_max_heap_tid(new_posting), btree_tuple_get_heap_tid(newitem)) < 0);
    Assert(btree_dedup_posting_valid(new_posting));

    return new_posting;
}

#ifdef USE_ASSERT_CHECKING
static bool btree_dedup_posting_valid(IndexTuple posting)
{
    ItemPointerData last;
    ItemPointer htid;

    if (!btree_tuple_is_posting(posting) || btree_tuple_get_nposting(posting) < 2) {
        return false;
    }

    ItemPointerCopy(btree_tuple_get_heap_tid(posting), &last);
    if (!ItemPointerIsValid(&last)) {
        return false;
    }

    for (int i = 1; i < btree_tuple_get_nposting(posting); i++) {
        htid = btree_tuple_get_posting_n(posting, i);

        if (!ItemPointerIsValid(htid)) {
            return false;
        }

        if (ItemPointerCompare(htid, &last) <= 0) {
            return false;
        }

        ItemPointerCopy(htid, &last);
    }

    return true;
}
#endif