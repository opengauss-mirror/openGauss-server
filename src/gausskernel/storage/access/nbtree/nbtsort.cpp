/* -------------------------------------------------------------------------
 *
 * nbtsort.cpp
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
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/nbtree/nbtsort.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/tableam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/pg_partition_fn.h"
#include "miscadmin.h"
#include "storage/smgr/smgr.h"
#include "storage/file/fio_device.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/aiomem.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/tuplesort.h"
#include "commands/tablespace.h"
#include "access/transam.h"
#include "utils/builtins.h"
#include "utils/logtape.h"
#include "utils/tuplesort.h"
#include "catalog/index.h"
#include "postmaster/bgworker.h"
#include "access/ubtree.h"

static Page _bt_blnewpage(uint32 level);
static void _bt_slideleft(Page page);
static void _bt_sortaddtup(Page page, Size itemsize, IndexTuple itup, OffsetNumber itup_off);
static void _bt_load(BTWriteState *wstate, BTSpool *btspool, BTSpool *btspool2);
static void _bt_parallel_build_main(const BgWorkerContext *bwc);
static void _bt_parallel_cleanup(const BgWorkerContext *bwc);

/*
 * Unified Atomic Iterator iterates next and returns the current value.
 */
uint64 uniter_next(pg_atomic_uint64 *curiter, uint32 cycle0, uint32 cycle1)
{
    unified_atomic_iterator_t uniter;
    uint64 value;
    uint32 iter0 = INVALID_ATOMIC_ITERATOR;
    uint32 iter1 = INVALID_ATOMIC_ITERATOR;

    value = pg_atomic_barrier_read_u64(curiter);
next:
    uniter.value = value;

    if (cycle0 > 0 && cycle1 == 0) {
        /* only iter0 is valid */
        iter0 = uniter.iters[0] + 1;
    } else if (cycle0 == 0 && cycle1 > 0) {
        /* only iter1 is valid */
        iter1 = uniter.iters[1] + 1;
    } else if (cycle0 > 0 && cycle1 > 0) {
        /* both iter0 and iter1 are valid */
        iter0 = uniter.iters[0] + 1;
        iter1 = uniter.iters[1];
        if (iter0 == cycle0) {
            iter1++;
            iter0 = 0;
        }
    } else {
        /* both are invalid, should not be here */
        ereport(PANIC, (errmsg("Unexpected error occurred during index parallel building.")));
    }

    uniter.iters[0] = iter0;
    uniter.iters[1] = iter1;

    if (!pg_atomic_compare_exchange_u64(curiter, &value, uniter.value)) {
        goto next;
    }
    /* fix the initial value */
    uniter.value = value;
    if (cycle0 == 0 && uniter.iters[0] >= 0) {
        uniter.iters[0] = INVALID_ATOMIC_ITERATOR;
    }
    if (cycle1 == 0 && uniter.iters[1] >= 0) {
        uniter.iters[1] = INVALID_ATOMIC_ITERATOR;
    }
    value = uniter.value;

    return value;
}

/*
 * Interface routines
 *
 * create and initialize a spool structure
 */
BTSpool *_bt_spoolinit(Relation heap, Relation index, bool isunique, bool isdead, void *meminfo)
{
    BTSpool *btspool = (BTSpool *)palloc0(sizeof(BTSpool));
    int btKbytes;
    UtilityDesc *desc = (UtilityDesc *)meminfo;
    int maxKbytes = isdead ? 0 : desc->query_mem[1];

    btspool->heap = heap;
    btspool->index = index;
    btspool->isunique = isunique;

    /*
     * We size the sort area as maintenance_work_mem rather than work_mem to
     * speed index creation.  This should be OK since a single backend can't
     * run multiple index creations in parallel.  Note that creation of a
     * unique index actually requires two BTSpool objects.	We expect that the
     * second one (for dead tuples) won't get very full, so we give it only
     * work_mem.
     */
    if (desc->query_mem[0] > 0)
        btKbytes = isdead ? SIMPLE_THRESHOLD : desc->query_mem[0];
    else
        btKbytes = isdead ? u_sess->attr.attr_memory.work_mem : u_sess->attr.attr_memory.maintenance_work_mem;
    btspool->sortstate = tuplesort_begin_index_btree(index, isunique, btKbytes, NULL, false, maxKbytes);

    /* We seperate 32MB for spool2, so cut this from the estimation */
    if (isdead) {
        desc->query_mem[0] -= SIMPLE_THRESHOLD;
    }

    return btspool;
}

/*
 * clean up a spool structure and its substructures.
 */
void _bt_spooldestroy(BTSpool *btspool)
{
    tuplesort_end(btspool->sortstate);
    pfree(btspool);
    btspool = NULL;
}

/*
 * spool an index entry into the sort file.
 */
void _bt_spool(BTSpool *btspool, ItemPointer self, Datum *values, const bool *isnull)
{
    tuplesort_putindextuplevalues(btspool->sortstate, btspool->index, self, values, isnull);
}

/*
 * given a spool loaded by successive calls to _bt_spool,
 * create an entire btree.
 */
void _bt_leafbuild(BTSpool *btspool, BTSpool *btspool2)
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
    wstate.inskey = _bt_mkscankey(wstate.index, NULL);
    wstate.inskey->allequalimage = btree_allequalimage(wstate.index, true);

    /*
     * We need to log index creation in WAL iff WAL archiving/streaming is
     * enabled UNLESS the index isn't WAL-logged anyway.
     */
    wstate.btws_use_wal = XLogIsNeeded() && RelationNeedsWAL(wstate.index);

    /* reserve the metapage */
    wstate.btws_pages_alloced = BTREE_METAPAGE + 1;
    wstate.btws_pages_written = 0;
    wstate.btws_zeropage = NULL; /* until needed */

    _bt_load(&wstate, btspool, btspool2);
}

#ifdef USE_SPQ
/*
 * Read tuples and load them into btree
 */
void spq_load(BTWriteState wstate)
{
    _bt_load(&wstate, NULL, NULL);
}
#endif

/*
 * Internal routines.
 *
 * allocate workspace for a new, clean btree page, not linked to any siblings.
 */
static Page _bt_blnewpage(uint32 level)
{
    Page page;
    BTPageOpaqueInternal opaque;

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
    _bt_pageinit(page, BLCKSZ);

    /* Initialize BT opaque state */
    opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    opaque->btpo_prev = opaque->btpo_next = P_NONE;
    opaque->btpo.level = level;
    opaque->btpo_flags = (level > 0) ? 0 : BTP_LEAF;
    opaque->btpo_cycleid = 0;

    /* Make the P_HIKEY line pointer appear allocated */
    ((PageHeader)page)->pd_lower += sizeof(ItemIdData);

    return page;
}

static void _bt_segment_blwritepage(BTWriteState *wstate, Page page, BlockNumber blkno)
{
    if (blkno >= wstate->btws_pages_written) {
        // check tablespace size limitation when extending BTREE file.
        STORAGE_SPACE_OPERATION(wstate->index, ((uint64)BLCKSZ) * (blkno - wstate->btws_pages_written + 1));
    }

    /* Ensure rd_smgr is open (could have been closed by relcache flush!) */
    RelationOpenSmgr(wstate->index);

    /* First make sure the block is extended and set as all-zero */
    while (blkno >= wstate->btws_pages_written) {
        Buffer buf = ReadBuffer(wstate->index, P_NEW);
        Assert(BufferGetBlockNumber(buf) == wstate->btws_pages_written);
        ReleaseBuffer(buf);
        wstate->btws_pages_written++;
    }

    Buffer buf = ReadBuffer(wstate->index, blkno);
    _bt_checkbuffer_valid(wstate->index, buf);

    LockBuffer(buf, BT_WRITE);
    /* Write data to block. We must use xlog in segment-page storage, as it needs set page's LSN in any cases. */
    XLogRecPtr xlog_ptr = log_newpage(&wstate->index->rd_node, MAIN_FORKNUM, blkno, page, true);

    errno_t rc = memcpy_s(BufferGetBlock(buf), BLCKSZ, page, BLCKSZ);
    securec_check(rc, "\0", "\0");
    PageSetLSN(BufferGetPage(buf), xlog_ptr);
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
    pfree(page);
    page = NULL;
}

/*
 * emit a completed btree page, and release the working storage.
 */
static void _bt_blwritepage(BTWriteState *wstate, Page page, BlockNumber blkno)
{
    if (IsSegmentFileNode(wstate->index->rd_node)) {
        _bt_segment_blwritepage(wstate, page, blkno);
        return;
    }

    bool need_free = false;
    errno_t errorno = EOK;
    char* unalign_zerobuffer = NULL;

    /* XLOG stuff */
    if (wstate->btws_use_wal) {
        /* We use the heap NEWPAGE record type for this */
        log_newpage(&wstate->index->rd_node, MAIN_FORKNUM, blkno, page, true);
    }
    
    if (blkno >= wstate->btws_pages_written) {
        // check tablespace size limitation when extending BTREE file.
        STORAGE_SPACE_OPERATION(wstate->index, ((uint64)BLCKSZ) * (blkno - wstate->btws_pages_written + 1));
    }

    /* Ensure rd_smgr is open (could have been closed by relcache flush!) */
    RelationOpenSmgr(wstate->index);

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
                if (ENABLE_DSS) {
                    unalign_zerobuffer = (char*)palloc0(BLCKSZ + ALIGNOF_BUFFER);
                    wstate->btws_zeropage = (Page)BUFFERALIGN(unalign_zerobuffer);
                } else {
                    wstate->btws_zeropage = (Page)palloc0(BLCKSZ);
                }
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
            if (ENABLE_DSS) {
                pfree(unalign_zerobuffer);
            } else {
                pfree(wstate->btws_zeropage);
            }
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
BTPageState *_bt_pagestate(BTWriteState *wstate, uint32 level)
{
    BTPageState *state = (BTPageState *)palloc0(sizeof(BTPageState));

    /* create initial page for level */
    state->btps_page = _bt_blnewpage(level);

    /* and assign it a page position */
    state->btps_blkno = wstate->btws_pages_alloced++;

    state->btps_minkey = NULL;
    /* initialize lastoff so first item goes into P_FIRSTKEY */
    state->btps_lastoff = P_HIKEY;
    state->btps_level = level;
    state->btps_lastextra = 0;
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
static void _bt_slideleft(Page page)
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
static void _bt_sortaddtup(Page page, Size itemsize, IndexTuple itup, OffsetNumber itup_off)
{
    BTPageOpaqueInternal opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    IndexTupleData trunctuple;


    if (!P_ISLEAF(opaque) && itup_off == P_FIRSTKEY) {
        trunctuple = *itup;
        trunctuple.t_info = sizeof(IndexTupleData);
        if (t_thrd.proc->workingVersionNum >= SUPPORT_GPI_VERSION_NUM) {
            btree_tuple_set_num_of_atts(&trunctuple, 0, false);
        }
        itup = &trunctuple;
        itemsize = sizeof(IndexTupleData);
    }

    if (PageAddItem(page, (Item)itup, itemsize, itup_off, false, false) == InvalidOffsetNumber)
        ereport(ERROR, 
                (errmodule(MOD_INDEX),
                 errcode(ERRCODE_INDEX_CORRUPTED),
                 errmsg("failed to add item to the index page"),
                 errdetail("item size: %lu, Tid: block %u, offset %u,",
                           (unsigned long)itemsize,
                           BlockIdGetBlockNumber(&(itup->t_tid.ip_blkid)),
                           itup->t_tid.ip_posid),
                 errcause("System error."),
                 erraction("Check WARNINGS for the details.")));
}

/* ----------
 * Add an item to a disk page from the sort output.
 *
 * We must be careful to observe the page layout conventions of nbtsearch.c:
 * - rightmost pages start data items at P_HIKEY instead of at P_FIRSTKEY.
 * - on non-leaf pages, the key portion of the first item need not be
 *	 stored, we should store only the link.
 *
 * A leaf page being built looks like:
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
void _bt_buildadd(BTWriteState *wstate, BTPageState *state, IndexTuple itup, Size truncextra)
{
    Page npage;
    BlockNumber nblkno;
    OffsetNumber last_off;
    Size pgspc;
    Size itupsz;
    Size last_truncextra = 0;

    /*
     * This is a handy place to check for cancel interrupts during the btree
     * load phase of index creation.
     */
    CHECK_FOR_INTERRUPTS();

    npage = state->btps_page;
    nblkno = state->btps_blkno;
    last_off = state->btps_lastoff;
    last_truncextra = state->btps_lastextra;
    state->btps_lastextra = truncextra;

    pgspc = PageGetFreeSpace(npage);
    itupsz = IndexTupleDSize(*itup);
    itupsz = MAXALIGN(itupsz);

    bool is_leaf = (state->btps_level == 0);

    /*
     * Check whether the item can fit on a btree page at all. (Eventually, we
     * ought to try to apply TOAST methods if not.) We actually need to be
     * able to fit three items on every page, so restrict any one item to 1/3
     * the per-page available space. Note that at this point, itupsz doesn't
     * include the ItemId.
     *
     * NOTE: similar code appears in _bt_insertonpg() to defend against
     * oversize items being inserted into an already-existing index. But
     * during creation of an index, we don't go through there.
     */
    if (unlikely(itupsz > (Size)BTREE_MAX_ITEM_SIZE(npage))) {
        btree_check_third_page(wstate->index, wstate->heap, is_leaf, npage, itup);
    }

    /*
     * Check to see if page is "full".	It's definitely full if the item won't
     * fit.  Otherwise, compare to the target freespace derived from the
     * fillfactor.	However, we must put at least two items on each page, so
     * disregard fillfactor if we don't have that many.
     */
    if (pgspc < itupsz + (is_leaf ? MAXALIGN(sizeof(ItemPointerData)) : 0) ||
        (pgspc + last_truncextra < state->btps_full && last_off > P_FIRSTKEY)) {
        /*
         * Finish off the page and write it out.
         */
        Page opage = npage;
        BlockNumber oblkno = nblkno;
        ItemId ii;
        ItemId hii;
        IndexTuple oitup;

        /* Create new page of same level */
        npage = _bt_blnewpage(state->btps_level);

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
        _bt_sortaddtup(npage, ItemIdGetLength(ii), oitup, P_FIRSTKEY);

        /*
         * Move 'last' into the high key position on opage
         */
        hii = PageGetItemId(opage, P_HIKEY);
        *hii = *ii;
        ItemIdSetUnused(ii); /* redundant */
        ((PageHeader)opage)->pd_lower -= sizeof(ItemIdData);

        if (is_leaf) {
            IndexTuple lastleft;
            IndexTuple truncated;
            /*
             * We truncate included attributes of high key here.  Subsequent
             * insertions assume that hikey is already truncated, and so they
             * need not worry about it, when copying the high key into the
             * parent page as a downlink.
             *
             * The code above have just rearranged item pointers, but it
             * didn't save any space.  In order to save the space on page we
             * have to truly shift index tuples on the page.  But that's not
             * so bad for performance, because we operating pd_upper and don't
             * have to shift much of tuples memory.  Shift of ItemId's is
             * rather cheap, because they are small.
             */

            ii = PageGetItemId(opage, OffsetNumberPrev(last_off));
            lastleft = (IndexTuple) PageGetItem(opage, ii);
            truncated = btree_truncate(wstate->index, lastleft, oitup, wstate->inskey);
            /* delete "wrong" high key, insert keytup as P_HIKEY. */
            PageIndexTupleDelete(opage, P_HIKEY);
            _bt_sortaddtup(opage, IndexTupleSize(truncated), truncated, P_HIKEY);
            pfree(truncated);

			hii = PageGetItemId(opage, P_HIKEY);
			oitup = (IndexTuple) PageGetItem(opage, hii);
        }
        /*
         * Link the old page into its parent, using its minimum key. If we
         * don't have a parent, we have to create one; this adds a new btree
         * level.
         */
        if (state->btps_next == NULL)
            state->btps_next = _bt_pagestate(wstate, state->btps_level + 1);

        Assert(state->btps_minkey != NULL);
        Assert((BTREE_TUPLE_GET_NUM_OF_ATTS(state->btps_minkey, wstate->index) <=
                IndexRelationGetNumberOfKeyAttributes(wstate->index) &&
                BTREE_TUPLE_GET_NUM_OF_ATTS(state->btps_minkey, wstate->index) > 0) ||
                P_LEFTMOST((BTPageOpaqueInternal) PageGetSpecialPointer(opage)));
        Assert(BTREE_TUPLE_GET_NUM_OF_ATTS(state->btps_minkey, wstate->index) == 0 ||
               !P_LEFTMOST((BTPageOpaqueInternal) PageGetSpecialPointer(opage)));
        if (t_thrd.proc->workingVersionNum < SUPPORT_GPI_VERSION_NUM) {
            ItemPointerSet(&(state->btps_minkey->t_tid), oblkno, P_HIKEY);;
        } else {
            BTreeInnerTupleSetDownLink(state->btps_minkey, oblkno);
        }
        _bt_buildadd(wstate, state->btps_next, state->btps_minkey, 0);
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
            BTPageOpaqueInternal oopaque = (BTPageOpaqueInternal)PageGetSpecialPointer(opage);
            BTPageOpaqueInternal nopaque = (BTPageOpaqueInternal)PageGetSpecialPointer(npage);

            oopaque->btpo_next = nblkno;
            nopaque->btpo_prev = oblkno;
            nopaque->btpo_next = P_NONE; /* redundant */
        }

        /*
         * Write out the old page.	We never need to touch it again, so we can
         * free the opage workspace too.
         */
        _bt_blwritepage(wstate, opage, oblkno);

        /*
         * Reset last_off to point to new page
         */
        last_off = P_FIRSTKEY;
    }

    /*
     * If the new item is the first for its page, stash a copy for later. Note
     * this will only happen for the first item on a level; on later pages,
     * the first item for a page is copied from the prior page in the code
     * above. Since the minimum key for an entire level is only used as a
     * minus infinity downlink, and never as a high key, there is no need to
     * truncate away non-key attributes at this point.
     */
    if (last_off == P_HIKEY) {
        Assert(state->btps_minkey == NULL);
        state->btps_minkey = CopyIndexTuple(itup);
        /* _bt_sortaddtup() will perform full truncation later */
        if (t_thrd.proc->workingVersionNum >= SUPPORT_GPI_VERSION_NUM) {
            btree_tuple_set_num_of_atts(state->btps_minkey, 0, false);
        }
    }

    /*
     * Add the new item into the current page.
     */
    last_off = OffsetNumberNext(last_off);
    _bt_sortaddtup(npage, itupsz, itup, last_off);

    state->btps_page = npage;
    state->btps_blkno = nblkno;
    state->btps_lastoff = last_off;
}

/*
 * Finish writing out the completed btree.
 */
void _bt_uppershutdown(BTWriteState *wstate, BTPageState *state)
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
        BTPageOpaqueInternal opaque;

        blkno = s->btps_blkno;
        opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(s->btps_page);

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
            if (t_thrd.proc->workingVersionNum < SUPPORT_GPI_VERSION_NUM) {
                ItemPointerSet(&(s->btps_minkey->t_tid), blkno, P_HIKEY);
            } else {
                BTreeInnerTupleSetDownLink(s->btps_minkey, blkno);
            }
            _bt_buildadd(wstate, s->btps_next, s->btps_minkey, 0);
            pfree(s->btps_minkey);
            s->btps_minkey = NULL;
        }

        /*
         * This is the rightmost page, so the ItemId array needs to be slid
         * back one slot.  Then we can dump out the page.
         */
        _bt_slideleft(s->btps_page);
        _bt_blwritepage(wstate, s->btps_page, s->btps_blkno);
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
    _bt_initmetapage(metapage, rootblkno, rootlevel, wstate->inskey->allequalimage, IsSystemRelation(wstate->index));
    _bt_blwritepage(wstate, metapage, BTREE_METAPAGE);
}

static void btree_sort_dedup_finish_pending(BTWriteState *wstate, BTPageState *state,
                                          BTDedupState dstate)
{
    Assert(dstate->num_items > 0);

    if (dstate->num_items == 1) {
        _bt_buildadd(wstate, state, dstate->base, 0);
    } else {
        IndexTuple postingtuple;
        Size truncextra;

        postingtuple = btree_dedup_form_posting(dstate->base, dstate->heap_tids, dstate->num_heap_tids);
        truncextra = IndexTupleSize(postingtuple) - btree_tuple_get_posting_off(postingtuple);

        _bt_buildadd(wstate, state, postingtuple, truncextra);
        pfree(postingtuple);
    }

    dstate->num_max_items = 0;
    dstate->num_heap_tids = 0;
    dstate->num_items = 0;
    dstate->size_freed = 0;
}

/*
 * Read tuples in correct sort order from tuplesort, and load them into
 * btree leaves.
 */
static void _bt_load(BTWriteState *wstate, BTSpool *btspool, BTSpool *btspool2)
{
    BTPageState *state = NULL;
    bool merge = (btspool2 != NULL);
    IndexTuple itup = NULL;
    IndexTuple itup2 = NULL;
    bool load1 = false;
    TupleDesc tupdes = RelationGetDescr(wstate->index);
    int keysz = IndexRelationGetNumberOfKeyAttributes(wstate->index);
    bool isdeduplicated = (wstate->inskey->allequalimage && btree_do_dedup(wstate->heap, wstate->index) && !btspool->isunique);

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
            load1 = _bt_index_tuple_compare(tupdes, wstate->inskey->scankeys, keysz, itup, itup2);

            /* When we see first tuple, create first index page */
            if (state == NULL)
                state = _bt_pagestate(wstate, 0);

            if (load1) {
                _bt_buildadd(wstate, state, itup, 0);
                itup = tuplesort_getindextuple(btspool->sortstate, true);
            } else {
                _bt_buildadd(wstate, state, itup2, 0);
                itup2 = tuplesort_getindextuple(btspool2->sortstate, true);
            }
        }
    } else if (isdeduplicated) {
        BTDedupState dstate;

        dstate = (BTDedupState) palloc(sizeof(BTDedupStateData));
        dstate->deduplicate = true;
        dstate->num_max_items = 0;
        dstate->max_posting_size = 0;
        dstate->base = NULL;
        dstate->base_off = InvalidOffsetNumber;
        dstate->base_tuple_size = 0;
        dstate->heap_tids = NULL;
        dstate->num_heap_tids = 0;
        dstate->num_items = 0;
        dstate->size_freed = 0;
        dstate->num_intervals = 0;

        while ((itup = tuplesort_getindextuple(btspool->sortstate, true)) != NULL) {
            if (state == NULL) {
                state = _bt_pagestate(wstate, 0);

                dstate->max_posting_size = MAXALIGN_DOWN((BLCKSZ * 10 / 100)) -
                                         sizeof(ItemIdData);
                Assert(dstate->max_posting_size <= BTREE_MAX_ITEM_SIZE(state->btps_page) &&
                       dstate->max_posting_size <= INDEX_SIZE_MASK);
                dstate->heap_tids = (ItemPointer)palloc(dstate->max_posting_size);

                btree_dedup_begin(dstate, CopyIndexTuple(itup), InvalidOffsetNumber);
            } else if (btree_num_keep_atts_fast(wstate->index, dstate->base, itup) > keysz &&
                     btree_dedup_merge(dstate, itup)) {
            } else {
                btree_sort_dedup_finish_pending(wstate, state, dstate);
                pfree(dstate->base);

                btree_dedup_begin(dstate, CopyIndexTuple(itup), InvalidOffsetNumber);
            }
        }

        if (state) {
            btree_sort_dedup_finish_pending(wstate, state, dstate);
            pfree(dstate->base);
            pfree(dstate->heap_tids);
        }

        pfree(dstate);
    } else {
#ifdef USE_SPQ
        if (enable_spq_btbuild(wstate->index)) {
            while ((itup = spq_consume(wstate->spqleader)) != NULL) {
                /* When we see first tuple, create first index page */
                if (state == NULL)
                    state = _bt_pagestate(wstate, 0);

                _bt_buildadd(wstate, state, itup, 0);
            }
        } else {
#endif
        /* merge is unnecessary */
        while ((itup = tuplesort_getindextuple(btspool->sortstate, true)) != NULL) {
            /* When we see first tuple, create first index page */
            if (state == NULL)
                state = _bt_pagestate(wstate, 0);

            _bt_buildadd(wstate, state, itup, 0);
        }
#ifdef USE_SPQ
        }
#endif
    }

    /* Close down final pages and write the metapage */
    _bt_uppershutdown(wstate, state);

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
    }
}

static int IndexTupleGetNAttsByDefault(IndexTuple itup, int defValue)
{
    return UBTreeTupleIsPivot(itup) ?
           ItemPointerGetOffsetNumberNoCheck(&(itup)->t_tid) & UBT_OFFSET_MASK :
           defValue;
}

/*
 * if itup < itup2, return true;
 * if itup > itup2, return false;
 * if itup == itup2, Take TID as the tie-breaker.
 * itup == NULL && itup2 == NULL take care by caller
 */
bool _bt_index_tuple_compare(TupleDesc tupdes, ScanKey indexScanKey, int keysz, IndexTuple itup, IndexTuple itup2)
{
    /* defaultly load itup, including the itup != NULL && itup2 == NULL case. */
    if (itup == NULL && itup2 != NULL) {
        return false;
    }
    if (itup != NULL && itup2 == NULL) {
        return true;
    }
    Assert(itup != NULL && itup2 != NULL);
    int ntupkeys = IndexTupleGetNAttsByDefault(itup, keysz);
    int ntupkeys2 = IndexTupleGetNAttsByDefault(itup2, keysz);
    keysz = Min(keysz, Min(ntupkeys, ntupkeys2));
    for (int i = 1; i <= keysz; i++) {
        ScanKey entry;
        Datum attrDatum1, attrDatum2;
        bool isNull1 = false;
        bool isNull2 = false;
        int32 compare;

        entry = indexScanKey + i - 1;
        attrDatum1 = index_getattr(itup, i, tupdes, &isNull1);
        attrDatum2 = index_getattr(itup2, i, tupdes, &isNull2);
        if (isNull1) {
            if (isNull2)
                compare = 0; /* NULL "=" NULL */
            else if (entry->sk_flags & SK_BT_NULLS_FIRST)
                compare = -1; /* NULL "<" NOT_NULL */
            else
                compare = 1; /* NULL ">" NOT_NULL */
        } else if (isNull2) {
            if (entry->sk_flags & SK_BT_NULLS_FIRST)
                compare = 1; /* NOT_NULL ">" NULL */
            else
                compare = -1; /* NOT_NULL "<" NULL */
        } else {
            compare = DatumGetInt32(FunctionCall2Coll(&entry->sk_func, entry->sk_collation, attrDatum1, attrDatum2));

            if (entry->sk_flags & SK_BT_DESC)
                compare = -compare;
        }

        // check compare value, if 0 continue, else break.
        if (compare > 0) {
            return false;
        } else if (compare < 0) {
            return true;
        }
    }
    if (ntupkeys != ntupkeys2) {
        return ntupkeys < ntupkeys2;
    }

    /* we got here, key attrs are all equal, take TID as tie-breaker */
    ItemPointer tid = UBTreeTupleGetHeapTID(itup);
    ItemPointer tid2 = UBTreeTupleGetHeapTID(itup2);
    /* NULL means that TID is truncated, we treat it as minus infinity and return true when they are both NULL*/
    if (tid == NULL) {
        return true;
    }
    if (tid2 == NULL) {
        return false;
    }
    /* TIDs can still be equal in global index */
    return ItemPointerCompare(tid, tid2) <= 0;
}

List *insert_ordered_index(List *list, TupleDesc tupdes, ScanKey indexScanKey, int keysz, IndexTuple itup,
                           BlockNumber heapModifiedOffset, IndexScanDesc srcIdxRelScan)
{
    ListCell *prev = NULL;
    IndexTuple itup2 = NULL;
    BTOrderedIndexListElement *ele = NULL;

    // ignore null index tuple, but user should assure the validity of indexseq
    if (itup == NULL)
        return list;

    ele = (BTOrderedIndexListElement *)palloc0fast(sizeof(BTOrderedIndexListElement));
    if (list == NIL) {
        ele->itup = itup;
        ele->heapModifiedOffset = heapModifiedOffset;
        ele->indexScanDesc = srcIdxRelScan;
        return lcons(ele, list);
    }
    itup2 = ((BTOrderedIndexListElement *)linitial(list))->itup;
    if (itup2 == NULL) {
        ereport(ERROR, 
                (errmodule(MOD_INDEX),
                 errcode(ERRCODE_INDEX_CORRUPTED),
                 errmsg("fail to insert a tuple to an orderd index, the ordered tuple list is corrupted"),
                 errdetail("offset itup: %u, Number of attributes: %d.", heapModifiedOffset, keysz),
                 errcause("System error."),
                 erraction("Contact engineer to support.")));
    }
    /* Does the datum belong at the front? */
    if (_bt_index_tuple_compare(tupdes, indexScanKey, keysz, itup, itup2)) {
        ele->itup = itup;
        ele->heapModifiedOffset = heapModifiedOffset;
        ele->indexScanDesc = srcIdxRelScan;
        return lcons(ele, list);
    }
    /* No, so find the entry it belongs after */
    prev = list_head(list);
    for (;;) {
        ListCell *curr = lnext(prev);
        if (curr == NULL) {
            break;
        }
        itup2 = ((BTOrderedIndexListElement *)lfirst(curr))->itup;
        if (itup2 == NULL) {
            ereport(ERROR, 
                    (errmodule(MOD_INDEX),
                     errcode(ERRCODE_INDEX_CORRUPTED),
                     errmsg("fail to insert a tuple to an orderd index, the ordered tuple list is corrupted"),
                     errdetail("offset itup: %u, Number of attributes: %d.", heapModifiedOffset, keysz),
                     errcause("System error."),
                     erraction("Contact engineer to support.")));
        }
        if (_bt_index_tuple_compare(tupdes, indexScanKey, keysz, itup, itup2)) {
            break; /* it belongs after 'prev', before 'curr' */
        }

        prev = curr;
    }

    /* Insert datum into list after 'prev' */
    ele->itup = itup;
    ele->heapModifiedOffset = heapModifiedOffset;
    ele->indexScanDesc = srcIdxRelScan;
    lappend_cell(list, prev, ele);
    return list;
}

/*
 * tuplesort_estimate_shared - estimate required shared memory allocation
 *
 * nWorkers is an estimate of the number of workers (it's the number that
 * will be requested).
 */
Size tuplesort_estimate_shared(int nWorkers)
{
    Size tapesSize;

    Assert(nWorkers > 0);

    /* Make sure that BufFile shared state is MAXALIGN'd */
    tapesSize = mul_size(sizeof(TapeShare), nWorkers);
    tapesSize = MAXALIGN(add_size(tapesSize, offsetof(Sharedsort, tapes)));

    return tapesSize;
}

static BTShared* _bt_parallel_initshared(BTSpool *btspool, int *workmem, int scantuplesortstates, bool isplain)
{
    Sharedsort *sharedsort;
    Sharedsort *sharedsort2;
    BTShared *btshared;
    Size estsort;
    uint32 nparts = 0;

    if (RelationIsGlobalIndex(btspool->index)) {
        if (RelationIsSubPartitioned(btspool->heap)) {
            nparts = GetSubPartitionNumber(btspool->heap);
        } else {
            nparts = getPartitionNumber(btspool->heap->partMap);
        }
    }

    /* Store shared build state, for which we reserved space */
    btshared = (BTShared *)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
                                                  sizeof(BTShared) + (nparts) * sizeof(double));

    /* Initialize immutable state */
    if (RelationIsPartition(btspool->heap)) {
        btshared->heaprelid = GetBaseRelOidOfParition(btspool->heap);
        btshared->indexrelid = GetBaseRelOidOfParition(btspool->index);
        btshared->heappartid = RelationGetRelid(btspool->heap);
        btshared->indexpartid = RelationGetRelid(btspool->index);
    } else {
        btshared->heaprelid = RelationGetRelid(btspool->heap);
        btshared->indexrelid = RelationGetRelid(btspool->index);
        btshared->heappartid = InvalidOid;
        btshared->indexpartid = InvalidOid;
    }

    btshared->isplain = isplain;
    btshared->curiter = 0;
    btshared->isunique = btspool->isunique;
    btshared->scantuplesortstates = scantuplesortstates;
    SpinLockInit(&btshared->mutex);
    /* Initialize mutable state */
    btshared->reltuples = 0.0;
    btshared->havedead = false;
    btshared->indtuples = 0.0;
    btshared->brokenhotchain = false;
    btshared->workmem[0] = workmem[0];
    btshared->workmem[1] = workmem[1];

    if (isplain) {
        HeapParallelscanInitialize(&btshared->heapdesc, btspool->heap);
    } else if (RelationIsGlobalIndex(btspool->index)) {
        btshared->alltuples = (double *)((char *)btshared + sizeof(BTShared));
        btshared->nparts = nparts;
    }

    /*
     * Store shared tuplesort-private state, for which we reserved space.
     * Then, initialize opaque state using tuplesort routine.
     */
    estsort = tuplesort_estimate_shared(scantuplesortstates);
    sharedsort = (Sharedsort *)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), estsort);
    tuplesort_initialize_shared(sharedsort, scantuplesortstates);

    /* Unique case requires a second spool, and associated shared state */
    if (!btspool->isunique) {
        sharedsort2 = NULL;
    } else {
        /*
         * Store additional shared tuplesort-private state, for which we
         * reserved space.  Then, initialize opaque state using tuplesort
         * routine.
         */
        sharedsort2 = (Sharedsort *)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), estsort);
        tuplesort_initialize_shared(sharedsort2, scantuplesortstates);
    }
    btshared->sharedsort  = sharedsort;
    btshared->sharedsort2 = sharedsort2;
    return btshared;
}

/*
 * Create parallel context, and launch workers for leader.
 *
 * buildstate argument should be initialized (with the exception of the
 * tuplesort state in spools, which may later be created based on shared
 * state initially set up here).
 *
 * request is the target number of parallel worker processes to launch.
 *
 * Sets buildstate's BTLeader, which caller must use to shut down parallel
 * mode by passing it to _bt_end_parallel() at the very end of its index
 * build.  If not even a single worker process can be launched, this is
 * never set, and caller should proceed with a serial index build.
 */
static void _bt_begin_parallel(BTBuildState *buildstate, int request, int *workmem, bool isplain)
{
    BTShared *btshared;
    BTLeader *btleader = (BTLeader *)palloc0(sizeof(BTLeader));

    Assert(request > 0);

    btshared = _bt_parallel_initshared(buildstate->spool, workmem, request, isplain);

    /* Launch workers, saving status for leader/caller */
    btleader->nparticipanttuplesorts =
            LaunchBackgroundWorkers(request, btshared, _bt_parallel_build_main, _bt_parallel_cleanup);
    /* if all workers start failed, will series index */
    if (btleader->nparticipanttuplesorts == 0) {
        pfree_ext(btshared);
        pfree_ext(btleader);
        return;
    }
    btleader->btshared = btshared;
    /* Save leader state now that it's clear build will be parallel */
    buildstate->btleader = btleader;
}

static inline void collect_global_index_result(BTShared *btshared, const uint32 ipart, const double reltuples,
    bool isglobal)
{
    if (isglobal) {
        SpinLockAcquire(&btshared->mutex);
        int i = Min(ipart, btshared->nparts - 1);
        btshared->alltuples[i] += reltuples;
        SpinLockRelease(&btshared->mutex);
    }
}

static inline void release_target_relation(Relation *targetheap_ptr, Relation *targetindex_ptr)
{
    if (targetheap_ptr != NULL && *targetheap_ptr != NULL) {
        releaseDummyRelation(targetheap_ptr);
    }
    if (targetindex_ptr != NULL && *targetindex_ptr != NULL) {
        releaseDummyRelation(targetindex_ptr);
    }
}

static inline void ReleaseTargetPartitionList(Relation heap, Relation index, List *heapparts, List *indexparts)
{
    if (heapparts != NULL) {
        releasePartitionList(heap, &heapparts, NoLock);
    }
    if (indexparts != NULL) {
        releasePartitionList(index, &indexparts, NoLock);
    }
}

/* 
 * Unified routine to do index building parallel scan crossing
 * multiple layers (partition or hashbucket or both).
 */
static double _bt_parallel_scan_cross_level(Relation heap, Relation index, IndexInfo *indexinfo,
    IndexBuildCallback callback, void *callbackstate, BTShared *btshared)
{
    unified_atomic_iterator_t uniter;
    Relation targetheap = NULL;
    Relation targetindex = NULL;
    Partition heappart = NULL;
    Partition indexpart = NULL;
    List *heapparts = NULL;
    List *indexparts = NULL;
    oidvector *bucketlist = NULL;
    uint32 nbkts = 0;
    uint32 nparts = 0;
    uint32 ibkt, ipart;
    int2 bucketid = InvalidBktId;
    double reltuples;
    double result = 0;

    Assert(heap != NULL);
    Assert(index != NULL);

    if (RelationIsPartitioned(heap)) {
        heapparts = (RelationIsSubPartitioned(heap) ? RelationGetSubPartitionList(heap, NoLock)
                                                    : relationGetPartitionList(heap, NoLock));
        indexparts = (!RelationIsGlobalIndex(index) ? indexGetPartitionList(index, NoLock) : indexparts);
        nparts = list_length(heapparts);
    }

    if (RELATION_OWN_BUCKET(heap)) {
        bucketlist = searchHashBucketByOid(heap->rd_bucketoid);
        nbkts = bucketlist->dim1;
    }

    while (true) {
        reltuples = 0;
        uniter.value = uniter_next((pg_atomic_uint64 *)&btshared->curiter, nbkts, nparts);
        ibkt = uniter.iters[0];
        ipart = uniter.iters[1];

        if (ipart != INVALID_ATOMIC_ITERATOR) {
            if (ipart >= nparts) {
                break;
            }
            heappart = (Partition)list_nth(heapparts, ipart);
            indexpart = ((indexparts != NULL) ? (Partition)list_nth(indexparts, ipart) : indexpart);
        }

        if (ibkt != INVALID_ATOMIC_ITERATOR) {
            if (ibkt >= nbkts) {
                break;
            }
            bucketid = bucketlist->values[ibkt];
        }

        if (bucketid != InvalidBktId) {
            targetheap = bucketGetRelation(heap, heappart, bucketid);
            targetindex = ((indexpart != NULL) ? bucketGetRelation(index, indexpart, bucketid) : targetindex);
        } else if (heappart != NULL) {
            targetheap = (RelationIsSubPartitioned(heap) ? SubPartitionGetRelation(heap, heappart, NoLock)
                                                         : partitionGetRelation(heap, heappart));
            targetindex = ((indexpart != NULL) ? partitionGetRelation(index, indexpart) : targetindex);
        } else {
            /* should not be here */
            ereport(PANIC, (errmsg("Unexpected error occurred during index parallel building.")));
        }

        reltuples += tableam_index_build_scan((targetheap == NULL) ? heap : targetheap,
                                              (targetindex == NULL) ? index : targetindex,
                                              indexinfo, true, callback, callbackstate, NULL);

        collect_global_index_result(btshared, ipart, reltuples, RelationIsGlobalIndex(index));

        result += reltuples;

        release_target_relation(&targetheap, &targetindex);
    }

    ReleaseTargetPartitionList(heap, index, heapparts, indexparts);

    return result;
}

/*
 * Perform a worker's portion of a parallel sort.
 *
 * This generates a tuplesort for passed btspool, and a second tuplesort
 * state if a second btspool is need (i.e. for unique index builds).  All
 * other spool fields should already be set when this is called.
 *
 * sortmem is the amount of working memory to use within each worker,
 * expressed in KBs.
 *
 * When this returns, workers are done, and need only release resources.
 */
static void _bt_parallel_scan_and_sort(BTSpool *btspool, BTSpool *btspool2, BTShared *btshared)
{
    SortCoordinate coordinate;
    BTBuildState buildstate;
    TableScanDesc scan;
    double reltuples;
    IndexInfo *indexInfo;

    /* Initialize local tuplesort coordination state */
    coordinate = (SortCoordinate)palloc0(sizeof(SortCoordinateData));
    coordinate->isWorker = true;
    coordinate->nParticipants = -1;
    coordinate->sharedsort = btshared->sharedsort;

    int sortmem = btshared->workmem[0] / btshared->scantuplesortstates;

    /* Begin "partial" tuplesort */
    btspool->sortstate = tuplesort_begin_index_btree(btspool->index, btspool->isunique, sortmem, coordinate, false, 0);

    /*
     * Just as with serial case, there may be a second spool.  If so, a
     * second, dedicated spool2 partial tuplesort is required.
     */
    if (btspool2) {
        SortCoordinate coordinate2;

        /*
         * We expect that the second one (for dead tuples) won't get very
         * full, so we give it only work_mem (unless sortmem is less for
         * worker).  Worker processes are generally permitted to allocate
         * work_mem independently.
         */
        coordinate2 = (SortCoordinate)palloc0(sizeof(SortCoordinateData));
        coordinate2->isWorker = true;
        coordinate2->nParticipants = -1;
        coordinate2->sharedsort = btshared->sharedsort2;
        btspool2->sortstate =
            tuplesort_begin_index_btree(btspool->index, false, btshared->workmem[1], coordinate2, false, 0);
    }

    /* Fill in buildstate for _bt_build_callback() */
    buildstate.isUnique = btshared->isunique;
    buildstate.haveDead = false;
    buildstate.heapRel = btspool->heap;
    buildstate.spool = btspool;
    buildstate.spool2 = btspool2;
    buildstate.indtuples = 0;
    buildstate.btleader = NULL;

    /* Join parallel scan */
    indexInfo = BuildIndexInfo(btspool->index);
    indexInfo->ii_Concurrent = false;

    /* do the heap scan */
    IndexBuildCallback callback = (btspool->heap->rd_tam_ops == TableAmUstore)? UBTreeBuildCallback : btbuildCallback;
    if (btshared->isplain) {
        /* plain table or partition */
        scan = tableam_scan_begin_parallel(btspool->heap, &btshared->heapdesc);
        reltuples = tableam_index_build_scan(btspool->heap, btspool->index, indexInfo, true, callback,
                                             (void*)&buildstate, (TableScanDesc)scan);
    } else {
        /* global partitioned index or crossbucket index */
        reltuples = _bt_parallel_scan_cross_level(btspool->heap, btspool->index, indexInfo, callback,
            (void*)&buildstate, btshared);
    }

    /*
     * Execute this worker's part of the sort.
     *
     * Unlike leader and serial cases, we cannot avoid calling
     * tuplesort_performsort() for spool2 if it ends up containing no dead
     * tuples (this is disallowed for workers by tuplesort).
     */
    tuplesort_performsort(btspool->sortstate);
    if (btspool2)
        tuplesort_performsort(btspool2->sortstate);

    /*
     * Done.  Record ambuild statistics, and whether we encountered a broken
     * HOT chain.
     */
    SpinLockAcquire(&btshared->mutex);
    btshared->reltuples += reltuples;
    if (buildstate.haveDead)
        btshared->havedead = true;
    btshared->indtuples += buildstate.indtuples;
    if (indexInfo->ii_BrokenHotChain)
        btshared->brokenhotchain = true;
    SpinLockRelease(&btshared->mutex);

    /* We can end tuplesorts immediately */
    tuplesort_end(btspool->sortstate);
    if (btspool2)
        tuplesort_end(btspool2->sortstate);
}

/*
 * Perform work within a launched parallel process.
 */
static void _bt_parallel_build_main(const BgWorkerContext *bwc)
{
    BTShared *btshared = (BTShared *)bwc->bgshared;
    Relation  targetheap;
    Relation  targetindex;
    Partition heappart = NULL;
    Partition indexpart = NULL;
    BTSpool *btspool;
    BTSpool *btspool2;

    /* Open relations within worker. */
    Relation heap = heap_open(btshared->heaprelid, NoLock);
    Relation index = index_open(btshared->indexrelid, NoLock);

    if (OidIsValid(btshared->heappartid)) {
        heappart = partitionOpen(heap, btshared->heappartid,  NoLock);
        indexpart = partitionOpen(index, btshared->indexpartid, NoLock);
        targetheap = partitionGetRelation(heap, heappart);
        targetindex = partitionGetRelation(index, indexpart);
    } else {
        targetheap = heap;
        targetindex = index;
    }

    /* Initialize worker's own spool. */
    btspool = (BTSpool *)palloc0(sizeof(BTSpool));
    btspool->heap = targetheap;
    btspool->index = targetindex;
    btspool->isunique = btshared->isunique;

    if (!btshared->isunique) {
        btspool2 = NULL;
    } else {
        /* Allocate memory for worker's own private secondary spool. */
        btspool2 = (BTSpool *)palloc0(sizeof(BTSpool));
        /* Initialize worker's own secondary spool. */
        btspool2->heap = btspool->heap;
        btspool2->index = btspool->index;
        btspool2->isunique = false;
    }

    /* Perform sorting of spool, and possibly a spool2. */
    _bt_parallel_scan_and_sort(btspool, btspool2, btshared);

    if (OidIsValid(btshared->heappartid)) {
        releaseDummyRelation(&targetheap);
        releaseDummyRelation(&targetindex);
        partitionClose(index, indexpart, NoLock);
        partitionClose(heap, heappart, NoLock);
    }

    index_close(index, NoLock);
    heap_close(heap, NoLock);
}

static void _bt_parallel_cleanup(const BgWorkerContext *bwc)
{
    BTShared *btshared = (BTShared *)bwc->bgshared;

    /* delete shared fileset */
    Assert(btshared->sharedsort);
    SharedFileSetDeleteAll(&btshared->sharedsort->fileset);
    if (btshared->sharedsort2) {
        SharedFileSetDeleteAll(&btshared->sharedsort2->fileset);
    }

    pfree_ext(btshared->sharedsort);
    pfree_ext(btshared->sharedsort2);
}

/*
 * Within leader, wait for end of heap scan.
 *
 * When called, parallel heap scan started by _bt_begin_parallel() will
 * already be underway within worker processes (when leader participates
 * as a worker, we should end up here just as workers are finishing).
 *
 * Fills in fields needed for ambuild statistics, and lets caller set
 * field indicating that some worker encountered a broken HOT chain.
 *
 * Returns the total number of heap tuples scanned.
 */
static double _bt_parallel_heapscan(BTBuildState *buildstate, bool *brokenhotchain, double **alltuples)
{
    BTLeader *btleader = buildstate->btleader;
    BTShared *btshared = btleader->btshared;
    double reltuples;

    BgworkerListWaitFinish(&btleader->nparticipanttuplesorts);

    /* no need to lock due to all bgworkers were terminated */
    pg_memory_barrier();

    /* all done, update to the actual number of participants */
    if (btshared->sharedsort != NULL) {
        btshared->sharedsort->actualParticipants = btleader->nparticipanttuplesorts;
    }
    if (btshared->sharedsort2 != NULL) {
        btshared->sharedsort2->actualParticipants = btleader->nparticipanttuplesorts;
    }

    buildstate->haveDead = btshared->havedead;
    buildstate->indtuples = btshared->indtuples;
    *brokenhotchain = btshared->brokenhotchain;
    reltuples = btshared->reltuples;

    if (btshared->alltuples != NULL) {
        Assert(*alltuples == NULL);
        *alltuples = (double *)palloc0(btshared->nparts * sizeof(double));
        for (uint32 i = 0; i < btshared->nparts; i++) {
            (*alltuples)[i] = btshared->alltuples[i];
        }
    }

    return reltuples;
}

static double _bt_spools_heapscan_utility(Relation heap, Relation index,  BTBuildState *buildstate,
    IndexInfo *indexInfo, double **allPartTuples)
{
    double reltuples = 0;
    if (RelationIsCrossBucketIndex(index) && !RELATION_OWN_BUCKET(heap)) {
        /* should be coordinator */
        return 0;
    }

    /* Fill spool using either serial or parallel heap scan */
    if (!buildstate->btleader) {
serial_build:
        IndexBuildCallback callback = (heap->rd_tam_ops == TableAmUstore)? UBTreeBuildCallback : btbuildCallback;
        if (RelationIsGlobalIndex(index)) {
            *allPartTuples = GlobalIndexBuildHeapScan(heap, index, indexInfo, callback, (void *)buildstate);
        } else if (RelationIsCrossBucketIndex(index)) {
            reltuples = IndexBuildHeapScanCrossBucket(heap, index, indexInfo, callback, (void *)buildstate);
        } else {
            reltuples = tableam_index_build_scan(heap, index, indexInfo, true, callback, (void *)buildstate,
                NULL);
        }
    } else {
        reltuples = _bt_parallel_heapscan(buildstate, &indexInfo->ii_BrokenHotChain, allPartTuples);
        BTShared *btshared = buildstate->btleader->btshared;
        int nruns = btshared->sharedsort->actualParticipants;
        if (nruns == 0) {
            /* failed to startup any bgworker, retry to do serial build */
            goto serial_build;
        }
    }

    return reltuples;
}

/*
 * Create and initialize one or two spool structures, and save them in caller's
 * buildstate argument.  May also fill-in fields within indexInfo used by index
 * builds.
 *
 * Scans the heap, possibly in parallel, filling spools with IndexTuples.  This
 * routine encapsulates all aspects of managing parallelism.  Caller need only
 * call _bt_end_parallel() in parallel case after it is done with spool/spool2.
 *
 * Returns the total number of heap tuples scanned.
 */
double _bt_spools_heapscan(Relation heap, Relation index, BTBuildState *buildstate, IndexInfo *indexInfo,
    double **allPartTuples)
{
    UtilityDesc *desc = &indexInfo->ii_desc;
    BTSpool *btspool = (BTSpool *)palloc0(sizeof(BTSpool));
    int maxKbytes = desc->query_mem[1];
    SortCoordinate coordinate = NULL;
    double reltuples = 0;
    int workmem[2];

    /*
     * We size the sort area as maintenance_work_mem rather than work_mem to
     * speed index creation.  This should be OK since a single backend can't
     * run multiple index creations in parallel (see also: notes on
     * parallelism and maintenance_work_mem below).
     */
    btspool->heap = heap;
    btspool->index = index;
    btspool->isunique = indexInfo->ii_Unique;

    /* Save as primary spool */
    buildstate->spool = btspool;

    /*
     * We size the sort area as maintenance_work_mem rather than work_mem to
     * speed index creation.  This should be OK since a single backend can't
     * run multiple index creations in parallel.  Note that creation of a
     * unique index actually requires two BTSpool objects.	We expect that the
     * second one (for dead tuples) won't get very full, so we give it only
     * work_mem.
     */
    workmem[0] = (desc->query_mem[0] > 0) ? (desc->query_mem[0] - SIMPLE_THRESHOLD) :
                                            u_sess->attr.attr_memory.maintenance_work_mem;
    workmem[1] = (desc->query_mem[0] > 0) ? SIMPLE_THRESHOLD : u_sess->attr.attr_memory.work_mem;

    /* Attempt to launch parallel worker scan when required */
    if (indexInfo->ii_ParallelWorkers > 0) {
        Assert(!indexInfo->ii_Concurrent);
        bool isplain = !RelationIsGlobalIndex(index) && !RelationIsCrossBucketIndex(index);
        _bt_begin_parallel(buildstate, indexInfo->ii_ParallelWorkers, workmem, isplain);
    }

    /*
     * If parallel build requested and at least one worker process was
     * successfully launched, set up coordination state
     */
    if (buildstate->btleader) {
        coordinate = (SortCoordinate)palloc0(sizeof(SortCoordinateData));
        coordinate->isWorker = false;
        coordinate->nParticipants = buildstate->btleader->nparticipanttuplesorts;
        coordinate->sharedsort = buildstate->btleader->btshared->sharedsort;
    }

    /*
     * Begin serial/leader tuplesort.
     *
     * In cases where parallelism is involved, the leader receives the same
     * share of maintenance_work_mem as a serial sort (it is generally treated
     * in the same way as a serial sort once we return).  Parallel worker
     * Tuplesortstates will have received only a fraction of
     * maintenance_work_mem, though.
     *
     * We rely on the lifetime of the Leader Tuplesortstate almost not
     * overlapping with any worker Tuplesortstate's lifetime.  There may be
     * some small overlap, but that's okay because we rely on leader
     * Tuplesortstate only allocating a small, fixed amount of memory here.
     * When its tuplesort_performsort() is called (by our caller), and
     * significant amounts of memory are likely to be used, all workers must
     * have already freed almost all memory held by their Tuplesortstates
     * (they are about to go away completely, too).  The overall effect is
     * that maintenance_work_mem always represents an absolute high watermark
     * on the amount of memory used by a CREATE INDEX operation, regardless of
     * the use of parallelism or any other factor.
     */
    buildstate->spool->sortstate =
        tuplesort_begin_index_btree(index, indexInfo->ii_Unique, workmem[0], coordinate, false, maxKbytes);

    /*
     * If building a unique index, put dead tuples in a second spool to keep
     * them out of the uniqueness check.  We expect that the second spool (for
     * dead tuples) won't get very full, so we give it only work_mem.
     */
    if (indexInfo->ii_Unique) {
        BTSpool *btspool2 = (BTSpool *)palloc0(sizeof(BTSpool));
        SortCoordinate coordinate2 = NULL;

        /* Initialize secondary spool */
        btspool2->heap = heap;
        btspool2->index = index;
        btspool2->isunique = false;
        /* Save as secondary spool */
        buildstate->spool2 = btspool2;

        if (buildstate->btleader) {
            /*
             * Set up non-private state that is passed to
             * tuplesort_begin_index_btree() about the basic high level
             * coordination of a parallel sort.
             */
            coordinate2 = (SortCoordinate)palloc0(sizeof(SortCoordinateData));
            coordinate2->isWorker = false;
            coordinate2->nParticipants = buildstate->btleader->nparticipanttuplesorts;
            coordinate2->sharedsort = buildstate->btleader->btshared->sharedsort2;
        }
        /*
         * We expect that the second one (for dead tuples) won't get very
         * full, so we give it only work_mem
         */
        buildstate->spool2->sortstate = tuplesort_begin_index_btree(index, false, workmem[1], coordinate2, false, 0);
    }

    reltuples = _bt_spools_heapscan_utility(heap, index, buildstate, indexInfo, allPartTuples);
    /* okay, all heap tuples are spooled */
    if (buildstate->spool2 && !buildstate->haveDead) {
        /* spool2 turns out to be unnecessary */
        _bt_spooldestroy(buildstate->spool2);
        buildstate->spool2 = NULL;
    }

    return reltuples;
}

/*
 * Shut down workers, destroy parallel context, and end parallel mode.
 */
void _bt_end_parallel()
{
    BgworkerListSyncQuit();
}

