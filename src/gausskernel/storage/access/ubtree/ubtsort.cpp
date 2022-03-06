/* -------------------------------------------------------------------------
 *
 * ubtsort.cpp
 *    Build a btree from sorted input by loading leaf pages sequentially.
 *
 * NOTES
 *
 * We use tuplesort.c to sort the given index tuples into order.
 * Then we scan the index tuples in order and build the btree pages
 * for each level.	We load source tuples into leaf-level pages.
 * Whenever we fill a page at one level, we add a link to it to its
 * parent level (starting a new parent level if necessary).  When
 * done, we write out each final page on each level, adding it to
 * its parent level.  When we have only one page on a level, it must be
 * the root -- it can be attached to the btree metapage and we are done.
 *
 * This code is moderately slow (~10% slower) compared to the regular
 * btree (insertion) build code on sorted or well-clustered data.  On
 * random data, however, the insertion build code is unusable -- the
 * difference on a 60MB heap is a factor of 15 because the random
 * probes into the btree thrash the buffer pool.  (NOTE: the above
 * "10%" estimate is probably obsolete, since it refers to an old and
 * not very good external sort implementation that used to exist in
 * this module.  tuplesort.c is almost certainly faster.)
 *
 * It is not wise to pack the pages entirely full, since then *any*
 * insertion would cause a split (and not only of the leaf page; the need
 * for a split would cascade right up the tree).  The steady-state load
 * factor for btrees is usually estimated at 70%.  We choose to pack leaf
 * pages to the user-controllable fill factor (default 90%) while upper pages
 * are always packed to 70%.  This gives us reasonable density (there aren't
 * many upper pages if the keys are reasonable-size) without risking a lot of
 * cascading splits during early insertions.
 *
 * Formerly the index pages being built were kept in shared buffers, but
 * that is of no value (since other backends have no interest in them yet)
 * and it created locking problems for CHECKPOINT, because the upper-level
 * pages were held exclusive-locked for long periods.  Now we just build
 * the pages in local memory and smgrwrite or smgrextend them as we finish
 * them.  They will need to be re-read into shared buffers on first use after
 * the build finishes.
 *
 * Since the index will never be used unless it is completely built,
 * from a crash-recovery point of view there is no need to WAL-log the
 * steps of the build.	After completing the index build, we can just sync
 * the whole file to disk using smgrimmedsync() before exiting this module.
 * This can be seen to be sufficient for crash recovery by considering that
 * it's effectively equivalent to what would happen if a CHECKPOINT occurred
 * just after the index build.	However, it is clearly not sufficient if the
 * DBA is using the WAL log for PITR or replication purposes, since another
 * machine would not be able to reconstruct the index from WAL.  Therefore,
 * we log the completed index pages to WAL if and only if WAL archiving is
 * active.
 *
 * This code isn't concerned about the FSM at all. The caller is responsible
 * for initializing that.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/ubtree/ubtsort.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/smgr/smgr.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/aiomem.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/tuplesort.h"
#include "commands/tablespace.h"
#include "access/transam.h"
#include "utils/builtins.h"

static const int TXN_INFO_SIZE_DIFF = (sizeof(TransactionId) - sizeof(ShortTransactionId)) * 2;

static Page UBTreeBlNewPage(Relation rel, uint32 level);
static void UBTreeSlideLeft(Page page);
static void UBTreeSortAddTuple(Page page, Size itemsize, IndexTuple itup,
    OffsetNumber itup_off, bool isnew, bool extXid);
static void UBTreeLoad(BTWriteState *wstate, BTSpool *btspool, BTSpool *btspool2);

/*
 * given a spool loaded by successive calls to _bt_spool,
 * create an entire btree.
 */
void UBTreeLeafBuild(BTSpool *btspool, BTSpool *btspool2)
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

    UBTreeLoad(&wstate, btspool, btspool2);
}

/*
 * Internal routines.
 *
 * allocate workspace for a new, clean btree page, not linked to any siblings.
 */
static Page UBTreeBlNewPage(Relation rel, uint32 level)
{
    Page page;
    UBTPageOpaqueInternal opaque;

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
    UBTreePageInit(page, BLCKSZ);

    /* Initialize BT opaque state */
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    opaque->btpo_prev = opaque->btpo_next = P_NONE;
    opaque->btpo.level = level;
    opaque->btpo_flags = (level > 0) ? 0 : BTP_LEAF;
    opaque->btpo_cycleid = 0;

    /* Make the P_HIKEY line pointer appear allocated */
    ((PageHeader)page)->pd_lower += sizeof(ItemIdData);

    return page;
}

/*
 * emit a completed btree page, and release the working storage.
 */
static void UBTreeBlWritePage(BTWriteState *wstate, Page page, BlockNumber blkno)
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
 * allocate and initialize a new BTPageState.  the returned structure
 * is suitable for immediate use by _bt_buildadd.
 */
BTPageState* UBTreePageState(BTWriteState *wstate, uint32 level)
{
    BTPageState *state = (BTPageState *)palloc0(sizeof(BTPageState));

    /* create initial page for level */
    state->btps_page = UBTreeBlNewPage(wstate->index, level);

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
 * @brief: slide an array of ItemIds back one slot (from P_FIRSTKEY to P_HIKEY,
 * overwriting P_HIKEY). we need to do this when we discover that we have built
 * an ItemId array in what has turned out to be a P_RIGHTMOST page.
 */
static void UBTreeSlideLeft(Page page)
{
    OffsetNumber off;
    OffsetNumber maxoff;
    ItemId previi;
    ItemId thisii;

    if (!PageIsEmpty(page)) {
        maxoff = PageGetMaxOffsetNumber(page);
        previi = PageGetItemId(page, P_HIKEY);
        for (off = P_FIRSTKEY; off <= maxoff; off = OffsetNumberNext(off)) {
            thisii = PageGetItemId(page, off);
            *previi = *thisii;
            previi = thisii;
        }
        ((PageHeader)page)->pd_lower -= sizeof(ItemIdData);
    }
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
static void UBTreeSortAddTuple(Page page, Size itemsize, IndexTuple itup,
    OffsetNumber itup_off, bool isnew, bool extXid)
{
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    IndexTupleData trunctuple;

    if (!P_ISLEAF(opaque) && itup_off == P_FIRSTKEY) {
        trunctuple = *itup;
        trunctuple.t_info = sizeof(IndexTupleData);
        UBTreeTupleSetNAtts(&trunctuple, 0, false);
        itup = &trunctuple;
        itemsize = sizeof(IndexTupleData);
    }

    if (!(isnew && P_ISLEAF(opaque))) {
        /* not new(copied tuple) or internal page. Don't need to handle xmin/xmax, normal insert */
        if (PageAddItem(page, (Item)itup, itemsize, itup_off, false, false) == InvalidOffsetNumber)
            ereport(PANIC,
                (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("Index tuple cant fit in the page when creating index.")));
    } else {
        Size storageSize = IndexTupleSize(itup);
        Size newsize = storageSize - TXNINFOSIZE;
        IndexTupleSetSize(itup, newsize);

        if (extXid) {
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
            IndexTransInfo *transInfo = (IndexTransInfo*)(((char*)itup) + newsize);
            TransactionId xmin = transInfo->xmin;
            TransactionId xmax = transInfo->xmax;

            /* setup pd_xid_base */
            IndexPagePrepareForXid(NULL, page, xmin, false, InvalidBuffer);
            IndexPagePrepareForXid(NULL, page, xmax, false, InvalidBuffer);

            UstoreIndexXid uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(itup);
            uxid->xmin = NormalTransactionIdToShort(opaque->xid_base, xmin);
            uxid->xmax = NormalTransactionIdToShort(opaque->xid_base, xmax);

            if (PageAddItem(page, (Item)itup, storageSize, itup_off, false, false) == InvalidOffsetNumber) {
                ereport(PANIC, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("Index tuple cant fit in the page when creating index.")));
            }
        } else {
            /* reserve space for xmin/xmax and set into Frozen and Invalid */
            ((PageHeader)page)->pd_upper -= TXNINFOSIZE;
            UstoreIndexXid uxid = (UstoreIndexXid)(((char*)page) + ((PageHeader)page)->pd_upper);
            uxid->xmin = FrozenTransactionId;
            uxid->xmax = InvalidTransactionId;

            if (PageAddItem(page, (Item)itup, newsize, itup_off, false, false) == InvalidOffsetNumber) {
                ereport(PANIC, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("Index tuple cant fit in the page when creating index.")));
            }

            ItemId iid = PageGetItemId(page, itup_off);
            ItemIdSetNormal(iid, ((PageHeader)page)->pd_upper, storageSize);
        }

        opaque->activeTupleCount++;
    }
}

/* ----------
 * Add an item to a disk page from the sort output.
 *
 * We must be careful to observe the page layout conventions of nbtsearch.c
 * - rightmost pages start data items at P_HIKEY instead of at P_FIRSTKEY.
 * - on non-leaf pages, the key portion of the first item need not be
 *	 stored, we should store only the link.
 *
 * A leaf page being built looks like
 *
 * +----------------+---------------------------------+
 * | PageHeaderData | linp0 linp1 linp2 ...			  |
 * +-----------+----+---------------------------------+
 * | ... linpN |									  |
 * +-----------+--------------------------------------+
 * |	 ^ last										  |
 * |												  |
 * +-------------+------------------------------------+
 * |			 | itemN ...						  |
 * +-------------+------------------+-----------------+
 * |		  ... item3 item2 item1 | "special space" |
 * +--------------------------------+-----------------+
 *
 * Contrast this with the diagram in bufpage.h; note the mismatch
 * between linps and items.  This is because we reserve linp0 as a
 * placeholder for the pointer to the "high key" item; when we have
 * filled up the page, we will set linp0 to point to itemN and clear
 * linpN.  On the other hand, if we find this is the last (rightmost)
 * page, we leave the items alone and slide the linp array over.
 *
 * 'last' pointer indicates the last offset added to the page.
 * ----------
 */
void UBTreeBuildAdd(BTWriteState *wstate, BTPageState *state, IndexTuple itup, bool hasxid)
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

    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(npage);

    if (!isPivot) {
        /* normal index tuple, need reserve space for xmin and xmax */
        Size itupsz = IndexTupleSize(itup);
        if (hasxid) {
            /*
             * Inserting a new IndexTuple following by 16B transaction information:
             *      the last 16B of index entries is xmin/xmax. With xid-base optimization, the
             *      actual space occupied by this part should be only 8B, set the correct size.
             */
            IndexTupleSetSize(itup, itupsz - TXN_INFO_SIZE_DIFF); /* size -= 8B */
        } else {
            IndexTupleSetSize(itup, itupsz + TXNINFOSIZE); /* size += 8B */
        }
    }

    Size pgspc = PageGetFreeSpace(npage);
    Size itupsz = IndexTupleDSize(*itup);
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
    if (unlikely(itupsz > UBTMaxItemSize(npage))) {
        UBTreeCheckThirdPage(wstate->index, wstate->heap, state->btps_level == 0, npage, itup);
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
        npage = UBTreeBlNewPage(wstate->index, state->btps_level);

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
        ii = PageGetItemId(opage, last_off);
        oitup = (IndexTuple)PageGetItem(opage, ii);
        UBTreeSortAddTuple(npage, ItemIdGetLength(ii), oitup, P_FIRSTKEY, false, false);

        /*
         * Move 'last' into the high key position on opage
         */
        hii = PageGetItemId(opage, P_HIKEY);
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
            ii = PageGetItemId(opage, OffsetNumberPrev(last_off));
            lastleft = (IndexTuple) PageGetItem(opage, ii);

            truncated = UBTreeTruncate(wstate->index, lastleft, oitup, wstate->inskey, false);
            /* delete "wrong" high key, insert truncated as P_HIKEY. */
            PageIndexTupleDelete(opage, P_HIKEY);
            UBTreeSortAddTuple(opage, IndexTupleSize(truncated), truncated, P_HIKEY, false, false);
            pfree(truncated);

            /* oitup should continue to point to the page's high key */
            hii = PageGetItemId(opage, P_HIKEY);
            oitup = (IndexTuple) PageGetItem(opage, hii);
        }

        /*
         * Link the old page into its parent, using its minimum key. If we
         * don't have a parent, we have to create one; this adds a new btree
         * level.
         */
        if (state->btps_next == NULL)
            state->btps_next = UBTreePageState((BTWriteState*)wstate, state->btps_level + 1);

        Assert(state->btps_minkey != NULL);
        Assert((UBTreeTupleGetNAtts(state->btps_minkey, wstate->index) <=
                IndexRelationGetNumberOfKeyAttributes(wstate->index) &&
                UBTreeTupleGetNAtts(state->btps_minkey, wstate->index) > 0) ||
               P_LEFTMOST((UBTPageOpaqueInternal) PageGetSpecialPointer(opage)));
        Assert(UBTreeTupleGetNAtts(state->btps_minkey, wstate->index) == 0 ||
               !P_LEFTMOST((UBTPageOpaqueInternal) PageGetSpecialPointer(opage)));

        UBTreeTupleSetDownLink(state->btps_minkey, oblkno);
        UBTreeBuildAdd(wstate, state->btps_next, state->btps_minkey, false);
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
            UBTPageOpaqueInternal oopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(opage);
            UBTPageOpaqueInternal nopaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(npage);

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
        UBTreeBlWritePage((BTWriteState*)wstate, opage, oblkno);

        /*
         * Reset last_off to point to new page
         */
        last_off = P_FIRSTKEY;
    }

    /*
     * Add the new item into the current page.
     */
    last_off = OffsetNumberNext(last_off);
    UBTreeSortAddTuple(npage, itupsz, itup, last_off, true, hasxid);

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
 * Finish writing out the completed btree.
 */
void UBTreeUpperShutDown(BTWriteState *wstate, BTPageState *state)
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
        UBTPageOpaqueInternal opaque;

        blkno = s->btps_blkno;
        opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(s->btps_page);

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
            UBTreeBuildAdd(wstate, s->btps_next, s->btps_minkey, false);
            pfree(s->btps_minkey);
            s->btps_minkey = NULL;
        }

        /*
         * This is the rightmost page, so the ItemId array needs to be slid
         * back one slot.  Then we can dump out the page.
         */
        UBTreeSlideLeft(s->btps_page);
        UBTreeBlWritePage(wstate, s->btps_page, s->btps_blkno);
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
    UBTreeInitMetaPage(metapage, rootblkno, rootlevel);
    UBTreeBlWritePage(wstate, metapage, BTREE_METAPAGE);
}

/*
 * Read tuples in correct sort order from tuplesort, and load them into
 * btree leaves.
 */
static void UBTreeLoad(BTWriteState *wstate, BTSpool *btspool, BTSpool *btspool2)
{
    BTPageState *state = NULL;
    bool merge = (btspool2 != NULL);
    IndexTuple itup = NULL;
    IndexTuple itup2 = NULL;
    bool should_free = false;
    bool should_free2 = false;
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
        itup = tuplesort_getindextuple(btspool->sortstate, true, &should_free);
        itup2 = tuplesort_getindextuple(btspool2->sortstate, true, &should_free2);

        for (;;) {
            if (itup == NULL && itup2 == NULL) {
                break;
            }

            /* _index_tuple_compare() will take TID as tie-breaker if all key columns are equal. */
            load1 = _index_tuple_compare(tupdes, wstate->inskey->scankeys, keysz, itup, itup2);

            /* When we see first tuple, create first index page */
            if (state == NULL)
                state = UBTreePageState(wstate, 0);

            if (load1) {
                UBTreeBuildAdd(wstate, state, itup, false);
                if (should_free) {
                    pfree(itup);
                    itup = NULL;
                }
                itup = tuplesort_getindextuple(btspool->sortstate, true, &should_free);
            } else {
                UBTreeBuildAdd(wstate, state, itup2, false);
                if (should_free2) {
                    pfree(itup2);
                    itup2 = NULL;
                }
                itup2 = tuplesort_getindextuple(btspool2->sortstate, true, &should_free2);
            }
        }
    } else {
        /* merge is unnecessary */
        while ((itup = tuplesort_getindextuple(btspool->sortstate, true, &should_free)) != NULL) {
            /* When we see first tuple, create first index page */
            if (state == NULL)
                state = UBTreePageState(wstate, 0);

            UBTreeBuildAdd(wstate, state, itup, false);
            if (should_free) {
                pfree(itup);
                itup = NULL;
            }
        }
    }

    /* Close down final pages and write the metapage */
    UBTreeUpperShutDown(wstate, state);

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