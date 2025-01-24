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
 * ubtpcrsort.cpp
 *  Build a btree from sorted input by loading leaf pages sequentially.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/ubtreepcr/ubtpcrsort.cpp
 *
 * --------------------------------------------------------------------------------------
 */


#include "access/ubtreepcr.h"
#include "commands/tablespace.h"
#include "utils/aiomem.h"

BTPageState* UBTreePCRPageState(BTWriteState* wstate, uint32 level);
static Page UBTreePCRBlNewPage(Relation rel, uint32 level);

/*
 * @brief: slide an array of ItemIds back one slot (from P_FIRSTKEY to P_HIKEY,
 * overwriting P_HIKEY). we need to do this when we discover that we have built
 * an ItemId array in what has turned out to be a P_RIGHTMOST page.
 */
static void UBTreePCRSlideLeft(Page page)
{
    OffsetNumber off;
    OffsetNumber maxoff;
    ItemId previi;
    ItemId thisii;

    if (!PageIsEmpty(page)) {
        maxoff = UBTPCRPageGetMaxOffsetNumber(page);
        previi = (ItemId)UBTreePCRGetRowPtr(page, P_HIKEY);
        for (off = P_FIRSTKEY; off <= maxoff; off = OffsetNumberNext(off)) {
            thisii = (ItemId)UBTreePCRGetRowPtr(page, off);
            *previi = *thisii;
            previi = thisii;
        }
        ((PageHeader)page)->pd_lower -= sizeof(ItemIdData);
    }
}

/*
 * emit a completed btree page, and release the working storage.
 */
static void UBTreePCRBlWritePage(BTWriteState *wstate, Page page, BlockNumber blkno)
{
    bool need_free = false;
    errno_t errorno = EOK;

    if (blkno >= wstate->btws_pages_written) {
        // check tablespace size limitation when extending BTREE file.
        STORAGE_SPACE_OPERATION(wstate->index, ((uint64)BLCKSZ) * (blkno - wstate->btws_pages_written + 1));
    }

    /* Ensure rd_smgr is open (could have been closed by relcache flush!) */
    RelationOpenSmgr(wstate->index);

    /* XLOG stuff */
    if (wstate->btws_use_wal) {
        /* We use the heap NEWPAGE record type for this */
        log_newpage(&wstate->index->rd_node, MAIN_FORKNUM, blkno, page, true);
    }

    /*
     * If we have to write pages nonsequentially, fill in the space with
     * zeroes until we come back and overwrite.  This is not logically
     * necessary on standard Unix filesystems (unwritten space will read as
     * zeroes anyway), but it should help to avoid fragmentation. The dummy
     * pages aren't WAL-logged though.
     */
    while (blkno > wstate->btws_pages_written) {
        if (!wstate->btws_zeropage) {
            ADIO_RUN()
            {
                wstate->btws_zeropage = (Page)adio_align_alloc(BLCKSZ);
                errorno = memset_s(wstate->btws_zeropage, BLCKSZ, 0, BLCKSZ);
                securec_check_c(errorno, "", "");
            }
            ADIO_ELSE()
            {
                wstate->btws_zeropage = (Page)palloc0(BLCKSZ);
            }
            ADIO_END();
            need_free = true;
        }

        /* don't set checksum for all-zero page */
        ADIO_RUN()
        {
            /* pass null buffer to lower levels to use fallocate, systables do not use fallocate,
             * relation id can distinguish systable or use table. "FirstNormalObjectId".
             * but unfortunately , but in standby, there is no relation id, so relation id has no work.
             * relation file node can not help becasue operation vacuum full or set table space can
             * change systable file node
             */
            if (u_sess->attr.attr_sql.enable_fast_allocate) {
                smgrextend(wstate->index->rd_smgr, MAIN_FORKNUM, wstate->btws_pages_written++, NULL, true);
            } else {
                smgrextend(wstate->index->rd_smgr, MAIN_FORKNUM, wstate->btws_pages_written++,
                           (char *)(char *)wstate->btws_zeropage, true);
            }
        }
        ADIO_ELSE()
        {
            smgrextend(wstate->index->rd_smgr, MAIN_FORKNUM, wstate->btws_pages_written++,
                       (char *)wstate->btws_zeropage, true);
        }
        ADIO_END();
    }

    PageSetChecksumInplace(page, blkno);

    /*
     * Now write the page.	There's no need for smgr to schedule an fsync for
     * this write; we'll do it ourselves before ending the build.
     */
    if (blkno == wstate->btws_pages_written) {
        /* extending the file... */
        smgrextend(wstate->index->rd_smgr, MAIN_FORKNUM, blkno, (char *)page, true);
        wstate->btws_pages_written++;
    } else {
        /* overwriting a block we zero-filled before */
        smgrwrite(wstate->index->rd_smgr, MAIN_FORKNUM, blkno, (char *)page, true);
    }

    ADIO_RUN()
    {
        if (need_free) {
            adio_align_free(wstate->btws_zeropage);
            wstate->btws_zeropage = NULL;
        }
        adio_align_free(page);
    }
    ADIO_ELSE()
    {
        if (need_free) {
            pfree(wstate->btws_zeropage);
            wstate->btws_zeropage = NULL;
        }
        pfree(page);
        page = NULL;
    }
    ADIO_END();
}


/*
 *	PageAddItem
 *
 *	Add an item to a page.	Return value is offset at which it was
 *	inserted, or InvalidOffsetNumber if there's not room to insert.
 *
 *	If overwrite is true, we just store the item at the specified
 *	offsetNumber (which must be either a currently-unused item pointer,
 *	or one past the last existing item).  Otherwise,
 *	if offsetNumber is valid and <= current max offset in the page,
 *	insert item into the array at that position by shuffling ItemId's
 *	down to make room.
 *	If offsetNumber is not valid, then assign one by finding the first
 *	one that is both unused and deallocated.
 *
 *	If is_heap is true, we enforce that there can't be more than
 *	MaxHeapTuplesPerPage line pointers on the page.
 *
 *	!!! EREPORT(ERROR) IS DISALLOWED HERE !!!
 */
OffsetNumber UBTPCRPageAddItem(Page page, Item item, Size size, OffsetNumber offsetNumber, bool overwrite)
{
    PageHeader phdr = (PageHeader)page;
    Size alignedSize;
    int lower;
    int upper;
    ItemId itemId;
    OffsetNumber limit;
    bool needshuffle = false;
    uint16 headersize;
    errno_t rc = EOK;

    /*
     * Be wary about corrupted page pointers
     */
    headersize = SizeOfPageHeaderData;
    if (phdr->pd_lower < headersize || phdr->pd_lower > phdr->pd_upper || phdr->pd_upper > phdr->pd_special ||
        phdr->pd_special > BLCKSZ){
        ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
            errmsg("corrupted page pointers: lower = %u, upper = %u, special = %u", phdr->pd_lower,
                    phdr->pd_upper, phdr->pd_special)));

    }

    /*
     * Select offsetNumber to place the new item at
     */
    limit = OffsetNumberNext(UBTPCRPageGetMaxOffsetNumber(page));

    /* was offsetNumber passed in? */
    if (OffsetNumberIsValid(offsetNumber)) {
        /* yes, check it */
        if (overwrite) {
            if (offsetNumber < limit) {
                itemId = UBTreePCRGetRowPtr(phdr, offsetNumber);
                if (ItemIdIsUsed(itemId) || ItemIdHasStorage(itemId)) {
                    ereport(WARNING, (errmsg("will not overwrite a used ItemId")));
                    return InvalidOffsetNumber;
                }
            }
        } else {
            if (offsetNumber < limit)
                needshuffle = true; /* need to move existing linp's */
        }
    } else {
        /* offsetNumber was not passed in, so find a free slot */
        /* if no free slot, we'll put it at limit (1st open slot) */
        if (PageHasFreeLinePointers(phdr)) {
            /*
             * Look for "recyclable" (unused) ItemId.  We check for no storage
             * as well, just to be paranoid --- unused items should never have
             * storage.
             */
            for (offsetNumber = 1; offsetNumber < limit; offsetNumber++) {
                itemId = UBTreePCRGetRowPtr(phdr, offsetNumber);
                if (!ItemIdIsUsed(itemId) && !ItemIdHasStorage(itemId))
                    break;
            }
            if (offsetNumber >= limit) {
                /* the hint is wrong, so reset it */
                PageClearHasFreeLinePointers(phdr);
            }
        } else {
            /* don't bother searching if hint says there's no free slot */
            offsetNumber = limit;
        }
    }

    if (offsetNumber > limit) {
        ereport(WARNING, (errmsg("specified item offset is too large")));
        return InvalidOffsetNumber;
    }

    /*
     * Compute new lower and upper pointers for page, see if it'll fit.
     *
     * Note: do arithmetic as signed ints, to avoid mistakes if, say,
     * alignedSize > pd_upper.
     */
    if (offsetNumber == limit || needshuffle) {
         lower = phdr->pd_lower + sizeof(ItemIdData);
    } else {
        lower = phdr->pd_lower;
    }

    alignedSize = MAXALIGN(size);

    upper = (int)phdr->pd_upper - (int)alignedSize;

    if (lower > upper) {
        return InvalidOffsetNumber;
    }
    
    /*
     * OK to insert the item.  First, shuffle the existing pointers if needed.
     */
    itemId = UBTreePCRGetRowPtr(phdr, offsetNumber);

    if (needshuffle) {
        rc = memmove_s(itemId + 1, (limit - offsetNumber) * sizeof(ItemIdData), itemId,
                       (limit - offsetNumber) * sizeof(ItemIdData));
        securec_check(rc, "", "");
    }

    /* set the item pointer */
    ItemIdSetNormal(itemId, upper, size);

    /* copy the item's data onto the page */
    rc = memcpy_s((char *)page + upper, alignedSize, item, size);
    securec_check(rc, "", "");

    /* adjust page header */
    phdr->pd_lower = (LocationIndex)lower;
    phdr->pd_upper = (LocationIndex)upper;

    return offsetNumber;
}



/*
 * Add an item to a page being built.
 *
 * The main difference between this routine and a bare PageAddItem call
 * is that this code knows that the leftmost data item on a non-leaf
 * btree page doesn't need to have a key.  Therefore, it strips such
 * items down to just the item header.
 *
 * This is almost like nbtinsert.c's _bt_pgaddtup(), but we can't use
 * that because it assumes that P_RIGHTMOST() will return the correct
 * answer for the page.  Here, we don't know yet if the page will be
 * rightmost.  Offset P_FIRSTKEY is always the first data key.
 */
static void UBTreePCRSortAddTuple(Page page, Size itemsize, IndexTuple itup,
    OffsetNumber itup_off, bool isnew, bool hasTrx)
{
    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    IndexTupleData trunctuple;

    if (!P_ISLEAF(opaque) && itup_off == P_FIRSTKEY) {
        trunctuple = *itup;
        trunctuple.t_info = sizeof(IndexTupleData);
        UBTreeTupleSetNAtts(&trunctuple, 0, false);
        itup = &trunctuple;
        itemsize = sizeof(IndexTupleData);
    }

    if (!(isnew && P_ISLEAF(opaque))) {
        /* not new(copied tuple) or internal page. Don't need to handle trx, normal insert */
        if (UBTPCRPageAddItem(page, (Item)itup, itemsize, itup_off, false) == InvalidOffsetNumber)
            ereport(PANIC, (errcode(ERRCODE_INDEX_CORRUPTED),
                    errmsg("Index tuple cant fit in the page when creating index.")));
    } else {
        Size storageSize = IndexTupleSize(itup);
        // TODO: PCR page 调试的时候确认下这里的计算, 因应该不用减
        Size newsize = storageSize;// - TXNINFOSIZE;
        // IndexTupleSetSize(itup, newsize);

        if (hasTrx) {
            /*
            * This is different from _bt_pgaddtup(), the last 16B of itup are xmin/xmax (8B each TransactionId).
            * But the actual space occupied by this part in page is 8B (4B each ShortTransactionId) with xid-base
            * optimization.
            *
            * If we assume the actual IndexTuple's length is X, there are 3 lengths:
            *      1. IndexTuple passed to _bt_buildadd() with length X + 16.
            *      2. IndexTuple on disk with length X + 8, this is the same as the length in LP.
            *      3. IndexTuple saw its length as X.
            *
            * At the beginning of _bt_buildadd(), we reset the size into X + 8 (actual storage size) to
            * test whether there is enough space.
            *
            * The actual memory size of itup here is X + 16, so we just need to:
            *      1. set IndexTuple's size into X (from X + 8).
            *      2. extract xid, and put the xid where it should be with xid-base optimization.
            *      3. call PageAddItem() with length X + 8
            */

            IndexTupleTrx tupleTrx = (IndexTupleTrx)(((char*)itup) + newsize);
            // TransactionId xmin = transInfo->xmin;
            // TransactionId xmax = transInfo->xmax;

    //         /* setup pd_xid_base */
            // IndexPagePrepareForXid(NULL, page, xmin, false, InvalidBuffer);
            // IndexPagePrepareForXid(NULL, page, xmax, false, InvalidBuffer);

    //         UstoreIndexXid uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(itup);
    //         uxid->xmin = NormalTransactionIdToShort(opaque->xid_base, xmin);
    //         uxid->xmax = NormalTransactionIdToShort(opaque->xid_base, xmax);

            if (UBTPCRPageAddItem(page, (Item)itup, storageSize, itup_off, false) == InvalidOffsetNumber) {
                ereport(PANIC, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("Index tuple cant fit in the page when creating index.")));
            }
        } else {
            /* reserve space for IndexTupleTrxData and set into Frozen and Invalid */
            ((PageHeader)page)->pd_upper -= MAXALIGN(sizeof(IndexTupleTrxData));
            IndexTupleTrx trx = (IndexTupleTrx)(((char*)page) + ((PageHeader)page)->pd_upper);
            trx->tdSlot = UBTreeFrozenTDSlotId;
            if (UBTPCRPageAddItem(page, (Item)itup, newsize, itup_off, false) == InvalidOffsetNumber) {
                ereport(PANIC, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("Index tuple cant fit in the page when creating index.")));
            }

            ItemId iid = UBTreePCRGetRowPtr(page, itup_off);
            UBTreePCRSetIndexTupleTrxSlot(trx, UBTreeFrozenTDSlotId);
        }
        
        opaque->activeTupleCount ++;
    }
}

/* ----------
 * Add an item to a disk page from the sort output.
 * Same as UBTreeBuildAdd().
 */
void UBTreePCRBuildAdd(BTWriteState *wstate, BTPageState *state, IndexTuple itup, bool hasTrx)
{
    /*
     * This is a handy place to check for cancel interrupts during the btree
     * load phase of index creation.
     */
    CHECK_FOR_INTERRUPTS();

    Page npage = state->btps_page;
    BlockNumber nblkno = state->btps_blkno;
    OffsetNumber last_off = state->btps_lastoff;
    /* Leaf case has slightly different rules due to suffix truncation */
    bool isleaf = (state->btps_level == 0);
    bool isPivot = UBTreeTupleIsPivot(itup);
    Size itupsz = IndexTupleSize(itup);

    UBTPCRPageOpaque opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(npage);

    if (!isPivot & hasTrx) {
        /* Normal index tuple, need reserve space for IndexTupleTrx */
        itupsz += sizeof(IndexTupleTrxData);
        IndexTupleSetSize(itup, itupsz);
    }

    Size pgspc = PageGetFreeSpace(npage);
    itupsz = MAXALIGN(itupsz);
    /*
     * Check whether the item can fit on a btree page at all.
     *
     * Every newly built index will treat heap TID as part of the keyspace,
     * which imposes the requirement that new high keys must occasionally have
     * a heap TID appended within _bt_truncate().  That may leave a new pivot
     * tuple one or two MAXALIGN() quantums larger than the original first
     * right tuple it's derived from.  v4 deals with the problem by decreasing
     * the limit on the size of tuples inserted on the leaf level by the same
     * small amount.  Enforce the new v4+ limit on the leaf level, and the old
     * limit on internal levels, since pivot tuples may need to make use of
     * the resered space.  This should never fail on internal pages.
     */
    if (unlikely(itupsz > UBTreePCRMaxItemSize(npage))) {
        UBTreeCheckThirdPage<UBTPCRPageOpaque>(wstate->index, wstate->heap, state->btps_level == 0, npage, itup);
    }

    /*
     * Check to see if page is "full".	It's definitely full if the item won't
     * fit.  Otherwise, compare to the target freespace derived from the
     * fillfactor.	However, we must put at least two items on each page, so
     * disregard fillfactor if we don't have that many.
     */
    if (pgspc < itupsz + (isleaf ? MAXALIGN(sizeof(ItemPointerData)) : 0) ||
        (pgspc < state->btps_full && last_off > P_FIRSTKEY && !P_LEFTMOST(opaque))) {
        /*
         * Finish off the page and write it out.
         */
        Page opage = npage;
        BlockNumber oblkno = nblkno;
        ItemId ii;
        ItemId hii;
        IndexTuple oitup;

        /* Create new page of same level */
        npage = UBTreePCRBlNewPage(wstate->index, state->btps_level);

        /* and assign it a page position */
        nblkno = wstate->btws_pages_alloced++;

        /*
         * We copy the last item on the page into the new page, and then
         * rearrange the old page so that the 'last item' becomes its high key
         * rather than a true data item.  There had better be at least two
         * items on the page already, else the page would be empty of useful
         * data.
         */
        Assert(last_off > P_FIRSTKEY);
        ii = UBTreePCRGetRowPtr(opage, last_off);
        oitup = (IndexTuple)UBTreePCRGetIndexTuple(opage, last_off);
        UBTreePCRSortAddTuple(npage, ItemIdGetLength(ii), oitup, P_FIRSTKEY, true, false);

        /*
         * Move 'last' into the high key position on opage
         */
        hii = UBTreePCRGetRowPtr(opage, P_HIKEY);
        *hii = *ii;
        ItemIdSetUnused(ii); /* redundant */
        ((PageHeader)opage)->pd_lower -= sizeof(ItemIdData);

        if (isleaf) {
            IndexTuple lastleft, truncated;
            /*
             * Truncate away any unneeded attributes from high key on leaf
             * level.  This is only done at the leaf level because downlinks
             * in internal pages are either negative infinity items, or get
             * their contents from copying from one level down.  See also:
             * _bt_split().
             *
             * We don't try to bias our choice of split point to make it more
             * likely that _bt_truncate() can truncate away more attributes,
             * whereas the split point passed to _bt_split() is chosen much
             * more delicately.  Suffix truncation is mostly useful because it
             * improves space utilization for workloads with random
             * insertions.  It doesn't seem worthwhile to add logic for
             * choosing a split point here for a benefit that is bound to be
             * much smaller.
             *
             * Since the truncated tuple is probably smaller than the
             * original, it cannot just be copied in place (besides, we want
             * to actually save space on the leaf page).  We delete the
             * original high key, and add our own truncated high key at the
             * same offset.  It's okay if the truncated tuple is slightly
             * larger due to containing a heap TID value, since this case is
             * known to BtCheckThirdPage(), which reserves space.
             *
             * Note that the page layout won't be changed very much.  oitup is
             * already located at the physical beginning of tuple space, so we
             * only shift the line pointer array back and forth, and overwrite
             * the latter portion of the space occupied by the original tuple.
             * This is fairly cheap.
             */
            ii = UBTreePCRGetRowPtr(opage, OffsetNumberPrev(last_off));
            lastleft = (IndexTuple) UBTreePCRGetIndexTuple(opage, OffsetNumberPrev(last_off));

            truncated = UBTreeTruncate(wstate->index, lastleft, oitup, wstate->inskey, false);
            /* delete "wrong" high key, insert truncated as P_HIKEY. */
            UBTreePCRPageIndexTupleDelete(opage, P_HIKEY);
            UBTreePCRSortAddTuple(opage, IndexTupleSize(truncated), truncated, P_HIKEY, false, false);
            pfree(truncated);

            /* oitup should continue to point to the page's high key */
            hii = UBTreePCRGetRowPtr(opage, P_HIKEY);
            oitup = (IndexTuple) UBTreePCRGetIndexTuple(opage, P_HIKEY);
        }

        /*
         * Link the old page into its parent, using its minimum key. If we
         * don't have a parent, we have to create one; this adds a new btree
         * level.
         */
        if (state->btps_next == NULL) {
            state->btps_next = UBTreePCRPageState((BTWriteState*)wstate, state->btps_level + 1);
        }

        Assert(state->btps_minkey != NULL);
        Assert((UBTreeTupleGetNAtts(state->btps_minkey, wstate->index) <=
                IndexRelationGetNumberOfKeyAttributes(wstate->index) &&
                UBTreeTupleGetNAtts(state->btps_minkey, wstate->index) > 0) ||
               P_LEFTMOST((UBTPCRPageOpaque) PageGetSpecialPointer(opage)));
        Assert(UBTreeTupleGetNAtts(state->btps_minkey, wstate->index) == 0 ||
               !P_LEFTMOST((UBTPCRPageOpaque) PageGetSpecialPointer(opage)));

        UBTreeTupleSetDownLink(state->btps_minkey, oblkno);
        UBTreePCRBuildAdd(wstate, state->btps_next, state->btps_minkey, false);
        pfree(state->btps_minkey);
        state->btps_minkey = NULL;

        /*
         * Save a copy of the minimum key for the new page.  We have to copy
         * it off the old page, not the new one, in case we are not at leaf
         * level.  Despite oitup is already initialized, it's important to get
         * high key from the page, since we could have replaced it with
         * truncated copy.	See comment above.
         */
        state->btps_minkey = CopyIndexTuple(oitup);

        /*
         * Set the sibling links for both pages.
         */
        {
            UBTPCRPageOpaque oopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(opage);
            UBTPCRPageOpaque nopaque = (UBTPCRPageOpaque)PageGetSpecialPointer(npage);

            oopaque->btpo_next = nblkno;
            nopaque->btpo_prev = oblkno;
            nopaque->btpo_next = P_NONE; /* redundant */

            /* The last tuple's data have been moved into the next page */
            oopaque->activeTupleCount--;
            nopaque->activeTupleCount++;
        }

        /*
         * Write out the old page.	We never need to touch it again, so we can
         * free the opage workspace too.
         */
        // TODO: 看下这个函数是否不用复制，用原有的就好了，没有是配点
        UBTreePCRBlWritePage((BTWriteState*)wstate, opage, oblkno);  

        /*
         * Reset last_off to point to new page
         */
        last_off = P_FIRSTKEY;
    }

    /*
     * Add the new item into the current page.
     */
    last_off = OffsetNumberNext(last_off);
    UBTreePCRSortAddTuple(npage, itupsz, itup, last_off, true, hasTrx);

    /*
     * If the new item is the first for its page, stash a copy for later. Note
     * this will only happen for the first item on a level; on later pages,
     * the first item for a page is copied from the prior page in the code
     * above. Since the minimum key for an entire level is only used as a
     * minus infinity downlink, and never as a high key, there is no need to
     * truncate away suffix attributes at this point.
     */
    if (last_off == OffsetNumberNext(P_HIKEY)) {
        Assert(state->btps_minkey == NULL);
        state->btps_minkey = CopyIndexTuple(itup);
        /* _bt_sortaddtup() will perform full truncation later */
        UBTreeTupleSetNAtts(state->btps_minkey, 0, false);
    }

    state->btps_page = npage;
    state->btps_blkno = nblkno;
    state->btps_lastoff = last_off;
}


/*
 * Internal routines.
 *
 * allocate workspace for a new, clean btree page, not linked to any siblings.
 */
static Page UBTreePCRBlNewPage(Relation rel, uint32 level)
{
    Page page;
    UBTPCRPageOpaque opaque;

    ADIO_RUN()
    {
        page = (Page)adio_align_alloc(BLCKSZ);
    }
    ADIO_ELSE()
    {
        page = (Page)palloc(BLCKSZ);
    }
    ADIO_END();

    /* Zero the page and set up standard page header info */
    UBTreePCRPageInit(page, BLCKSZ);

    /* Initialize BT opaque state */
    opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(page);
    opaque->btpo_prev = opaque->btpo_next = P_NONE;
    opaque->btpo.level = level;
    opaque->btpo_flags = (level > 0) ? 0 : BTP_LEAF;
    opaque->btpo_cycleid = 0;
    if (level == 0) {
        /* reserve td slots in leaf page */
        opaque->td_count = UBTREE_DEFAULT_TD_COUNT;
        ((PageHeader)page)->pd_lower += opaque->td_count * sizeof(UBTreeTDData);
        // TODO : FROZEN TD
     } 
    /* Make the P_HIKEY line pointer appear allocated */
    ((PageHeader)page)->pd_lower += sizeof(ItemIdData);

    return page;
}

/*
 * allocate and initialize a new BTPageState.  the returned structure
 * is suitable for immediate use by _bt_buildadd.
 */
BTPageState* UBTreePCRInitPageState(BTWriteState *wstate, uint32 level)
{
    BTPageState *state = (BTPageState *)palloc0(sizeof(BTPageState));

    /* create initial page for level */
    state->btps_page = UBTreePCRBlNewPage(wstate->index, level);

    /* and assign it a page position */
    state->btps_blkno = wstate->btws_pages_alloced++;

    state->btps_minkey = NULL;
    /* initialize lastoff so first item goes into P_FIRSTKEY */
    state->btps_lastoff = P_HIKEY;
    state->btps_level = level;
    /* set "full" threshold based on level.  See notes at head of file. */
    if (level > 0) {
        state->btps_full = (BLCKSZ * (100 - BTREE_NONLEAF_FILLFACTOR) / 100);
    } else {
        state->btps_full = (Size)RelationGetTargetPageFreeSpace(wstate->index, BTREE_DEFAULT_FILLFACTOR);
    }
    /* no parent level, yet */
    state->btps_next = NULL;

    return state;
}

/*
 * Read tuples in correct sort order from tuplesort, and load them into
 * btree leaves.
 */
static void UBTreePCRLoad(BTWriteState *wstate, BTSpool *btspool, BTSpool *btspool2)
{
    BTPageState *state = NULL;
    bool merge = (btspool2 != NULL);
    IndexTuple itup = NULL;
    IndexTuple itup2 = NULL;
    bool load1 = false;
    TupleDesc tupdes = RelationGetDescr(wstate->index);
    int keysz = IndexRelationGetNumberOfKeyAttributes(wstate->index);

    if (merge) {
        /*
         * Another BTSpool for dead tuples exists. Now we have to merge
         * btspool and btspool2.
         *
         * the preparation of merge
         */
        itup = tuplesort_getindextuple(btspool->sortstate, true);
        itup2 = tuplesort_getindextuple(btspool2->sortstate, true);

        for (;;) {
            if (itup == NULL && itup2 == NULL) {
                break;
            }

            /* _bt_index_tuple_compare() will take TID as tie-breaker if all key columns are equal. */
            load1 = _bt_index_tuple_compare(tupdes, wstate->inskey->scankeys, keysz, itup, itup2);

            /* When we see first tuple, create first index page */
            if (state == NULL) {
                state = UBTreePCRInitPageState(wstate, 0);
            }

            if (load1) {
                UBTreePCRBuildAdd(wstate, state, itup, false);
                itup = tuplesort_getindextuple(btspool->sortstate, true);
            } else {
                UBTreePCRBuildAdd(wstate, state, itup2, false);
                itup2 = tuplesort_getindextuple(btspool2->sortstate, true);
            }
        }
    } else {
        /* merge is unnecessary */
        while ((itup = tuplesort_getindextuple(btspool->sortstate, true)) != NULL) {
            /* When we see first tuple, create first index page */
            if (state == NULL)
                state = UBTreePCRPageState(wstate, 0);

            UBTreePCRBuildAdd(wstate, state, itup, false);
        }
    }

    /* Close down final pages and write the metapage */
    UBTreePCRUpperShutDown(wstate, state);

    /* initialize recycle queue for ubtree */
    UBTreeInitializeRecycleQueue(wstate->index);

    /*
     * If the index is WAL-logged, we must fsync it down to disk before it's
     * safe to commit the transaction.	(For a non-WAL-logged index we don't
     * care since the index will be uninteresting after a crash anyway.)
     *
     * It's obvious that we must do this when not WAL-logging the build. It's
     * less obvious that we have to do it even if we did WAL-log the index
     * pages.  The reason is that since we're building outside shared buffers,
     * a CHECKPOINT occurring during the build has no way to flush the
     * previously written data to disk (indeed it won't know the index even
     * exists).  A crash later on would replay WAL from the checkpoint,
     * therefore it wouldn't replay our earlier WAL entries. If we do not
     * fsync those pages here, they might still not be on disk when the crash
     * occurs.
     */
    if (RelationNeedsWAL(wstate->index)) {
        RelationOpenSmgr(wstate->index);
        smgrimmedsync(wstate->index->rd_smgr, MAIN_FORKNUM);
        /* for ubtree, force UBtree Recycle Queue also */
        smgrimmedsync(wstate->index->rd_smgr, FSM_FORKNUM);
    }
}

/*
 * given a spool loaded by successive calls to _bt_spool,
 * create an entire btree.
 */
void UBTreePCRLeafBuild(BTSpool *btspool, BTSpool *btspool2)
{
    BTWriteState wstate;

#ifdef BTREE_BUILD_STATS
    if (u_sess->attr.attr_resource.log_btree_build_stats) {
        ShowUsage("BTREE BUILD (Spool) STATISTICS");
        ResetUsage();
    }
#endif /* BTREE_BUILD_STATS */

    tuplesort_performsort(btspool->sortstate);
    if (btspool2 != NULL)
        tuplesort_performsort(btspool2->sortstate);

    wstate.heap = btspool->heap;
    wstate.index = btspool->index;
    wstate.inskey = UBTreeMakeScanKey(wstate.index, NULL);
    /*
     * We need to log index creation in WAL iff WAL archiving/streaming is
     * enabled UNLESS the index isn't WAL-logged anyway.
     */
    wstate.btws_use_wal = XLogIsNeeded() && RelationNeedsWAL(wstate.index);

    /* reserve the metapage */
    wstate.btws_pages_alloced = BTREE_METAPAGE + 1;
    wstate.btws_pages_written = 0;
    wstate.btws_zeropage = NULL; /* until needed */

    UBTreePCRLoad(&wstate, btspool, btspool2);
    return;
}

/*
 * Finish writing out the completed btree.
 */
void UBTreePCRUpperShutDown(BTWriteState* wstate, BTPageState* state)
{
    BTPageState *s = NULL;
    BlockNumber rootblkno = P_NONE;
    uint32 rootlevel = 0;
    Page metapage;

    /*
     * Each iteration of this loop completes one more level of the tree.
     */
    for (s = state; s != NULL; s = s->btps_next) {
        BlockNumber blkno;
        UBTPCRPageOpaque opaque;

        blkno = s->btps_blkno;
        opaque = (UBTPCRPageOpaque)PageGetSpecialPointer(s->btps_page);

        /*
         * We have to link the last page on this level to somewhere.
         *
         * If we're at the top, it's the root, so attach it to the metapage.
         * Otherwise, add an entry for it to its parent using its minimum key.
         * This may cause the last page of the parent level to split, but
         * that's not a problem -- we haven't gotten to it yet.
         */
        if (s->btps_next == NULL) {
            opaque->btpo_flags |= BTP_ROOT;
            rootblkno = blkno;
            rootlevel = s->btps_level;
        } else {
            Assert(s->btps_minkey != NULL);
            UBTreeTupleSetDownLink(s->btps_minkey, blkno);
            UBTreePCRBuildAdd(wstate, s->btps_next, s->btps_minkey, false);
            pfree(s->btps_minkey);
            s->btps_minkey = NULL;
        }

        /*
         * This is the rightmost page, so the ItemId array needs to be slid
         * back one slot.  Then we can dump out the page.
         */
        UBTreePCRSlideLeft(s->btps_page);
        UBTreePCRBlWritePage(wstate, s->btps_page, s->btps_blkno);
        s->btps_page = NULL; /* writepage freed the workspace */
    }

    /*
     * As the last step in the process, construct the metapage and make it
     * point to the new root (unless we had no data at all, in which case it's
     * set to point to "P_NONE").  This changes the index to the "valid" state
     * by filling in a valid magic number in the metapage.
     */
    // free in function _bt_blwritepage()
    ADIO_RUN()
    {
        metapage = (Page)adio_align_alloc(BLCKSZ);
    }
    ADIO_ELSE()
    {
        metapage = (Page)palloc(BLCKSZ);
    }
    ADIO_END();
    UBTreePCRInitMetaPage(metapage, rootblkno, rootlevel);
    UBTreePCRBlWritePage(wstate, metapage, BTREE_METAPAGE);
}

/*
 * allocate and initialize a new BTPageState.  the returned structure
 * is suitable for immediate use by _bt_buildadd.
 */
BTPageState* UBTreePCRPageState(BTWriteState* wstate, uint32 level)
{
    BTPageState *state = (BTPageState *)palloc0(sizeof(BTPageState));

    /* create initial page for level */
    state->btps_page = UBTreePCRBlNewPage(wstate->index, level);

    /* and assign it a page position */
    state->btps_blkno = wstate->btws_pages_alloced++;

    state->btps_minkey = NULL;
    /* initialize lastoff so first item goes into P_FIRSTKEY */
    state->btps_lastoff = P_HIKEY;
    state->btps_level = level;
    /* set "full" threshold based on level.  See notes at head of file. */
    if (level > 0) {
        state->btps_full = (BLCKSZ * (100 - BTREE_NONLEAF_FILLFACTOR) / 100);
    } else {
        state->btps_full = (Size)RelationGetTargetPageFreeSpace(wstate->index, BTREE_DEFAULT_FILLFACTOR);
    }
    /* no parent level, yet */
    state->btps_next = NULL;

    return state;

}
