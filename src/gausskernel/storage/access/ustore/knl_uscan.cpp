
/* -------------------------------------------------------------------------
 *
 * knl_uscan.cpp
 * Scan logic for inplace update engine
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_uscan.cpp
 * -------------------------------------------------------------------------
 */

#include "miscadmin.h"
#include "commands/verify.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "storage/buf/bufmgr.h"
#include "storage/freespace.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/ustore/knl_uscan.h"
#include "access/ustore/knl_uvisibility.h"
#include "access/ustore/knl_uheap.h"
#include "access/ustore/knl_undorequest.h"
#include "access/ustore/knl_whitebox_test.h"
#include "pgstat.h"
#include <stdlib.h>

static const int CACHE_LINE_SZ = 64;

/* prune a page during UHeapGetPage based on heuristics */
static void UHeapGetPagePrune(UHeapScanDesc scan, Buffer buffer)
{
    Page pg;
    BlockNumber blkno;
    bool hasPruned = false;
    Size freespace = 0;

    if (!scan->rs_base.rs_rangeScanInRedis.isRangeScanInRedis) {
        pg = BufferGetPage(buffer);
        UHeapPageHeaderData *upage = (UHeapPageHeaderData *)pg;
        double thres = RelationGetTargetPageFreeSpacePrune(scan->rs_base.rs_rd, HEAP_DEFAULT_FILLFACTOR);
        /* Since heuristic probability controls the frequency of pruning,
         * (1.0-heuristic_prob) controls the threshold percentage of potential freespace to perform page pruning
         * do page pruning when potential_freespace takes up at least (1.0-heuristic_prob)
         * of all space less the reserved space */
        if (upage->potential_freespace >=
            (1.0 - FSM_UPDATE_HEURISTI_PROBABILITY) * (BLCKSZ - thres)) {
            hasPruned = UHeapPagePruneOptPage(scan->rs_base.rs_rd, buffer, GetTopTransactionIdIfAny(), true);
            blkno = BufferGetBlockNumber(buffer);
            freespace = hasPruned ? PageGetUHeapFreeSpace(pg) : 0;

            if (hasPruned) {
                RecordPageWithFreeSpace(scan->rs_base.rs_rd, blkno, freespace);
#ifdef DEBUG_UHEAP
                UHEAPSTAT_COUNT_OP_PRUNEPAGE(upd, 1);
                UHEAPSTAT_COUNT_OP_PRUNEPAGE_SPC(upd, freespace);
                UHEAPSTAT_COUNT_OP_PRUNEPAGE_SUC(upd, 1);
#endif
                UpdateFreeSpaceMap(scan->rs_base.rs_rd, blkno, blkno, freespace, false);
            }
        }
    }
}

/* Check sequential scan descriptor for partial sequential scan fallback conditions, return valid lastVar */
static inline AttrNumber UHeapCheckScanDesc(const TableScanDesc sscan)
{
    /* lastVar only valid if partial seqscan enabled, scan is initialized, and table is not partition(ed) */
    if ((!u_sess->attr.attr_storage.enable_ustore_partial_seqscan) || (sscan->lastVar < 0) || 
        (!(sscan->rs_inited)) || (!RelationIsNonpartitioned(sscan->rs_rd)) ||
        (RelationIsPartition(sscan->rs_rd))) {
        return UHEAP_SCAN_FALLBACK;
    }
    return sscan->lastVar;
}

FORCE_INLINE
bool NextUpage(UHeapScanDesc scan, ScanDirection dir, BlockNumber& page)
{
    bool finished = false;
    /*
     * advance to next/prior page and detect end of scan
     */
    if (BackwardScanDirection == dir) {
        finished = (page == scan->rs_base.rs_startblock);
        if (page == 0) {
            page = scan->rs_base.rs_nblocks;
        }
        page--;
    } else {
        page++;
        if (page >= scan->rs_base.rs_nblocks) {
            page = 0;
        }
        finished = (page == scan->rs_base.rs_startblock);

        /*
         * Report our new scan position for synchronization purposes. We
         * don't do that when moving backwards, however. That would just
         * mess up any other forward-moving scanners.
         *
         * Note: we do this before checking for end of scan so that the
         * final state of the position hint is back at the start of the
         * rel.  That's not strictly necessary, but otherwise when you run
         * the same query multiple times the starting position would shift
         * a little bit backwards on every invocation, which is confusing.
         * We don't guarantee any specific ordering in general, though.
         */
        if (scan->rs_allow_sync) {
            ss_report_location(scan->rs_base.rs_rd, page);
        }
    }

    return finished;
}

/*
 * UHeapGetPage - Same as heapgetpage, but operate on uheap page and
 * in page-at-a-time mode, visible tuples are stored in rs_visibletuples.
 *
 * It returns false, if we can't scan the page, otherwise, return true.
 */
bool UHeapGetPage(TableScanDesc sscan, BlockNumber page)
{
    UHeapScanDesc scan = (UHeapScanDesc)sscan;

    Assert(page < scan->rs_base.rs_nblocks);

    /* release previous scan buffer, if any */
    if (BufferIsValid(scan->rs_base.rs_cbuf)) {
        ReleaseBuffer(scan->rs_base.rs_cbuf);
        scan->rs_base.rs_cbuf = InvalidBuffer;
    }

#ifdef DEBUG_UHEAP
    /* Fixme: Add PG stats for getpage */
#endif

    /*
     * Be sure to check for interrupts at least once per page.  Checks at
     * higher code levels won't be able to stop a seqscan that encounters many
     * pages' worth of consecutive dead tuples.
     */
    CHECK_FOR_INTERRUPTS();

    /* read page using selected strategy */
    Buffer buffer = ReadBufferExtended(scan->rs_base.rs_rd, MAIN_FORKNUM, page, RBM_NORMAL, scan->rs_base.rs_strategy);
    scan->rs_base.rs_cblock = page;

    UHeapGetPagePrune(scan, buffer);

    WHITEBOX_TEST_STUB("UHeapGetPage_Before_LockBuffer", WhiteboxDefaultErrorEmit);

    /*
     * We must hold share lock on the buffer content while examining tuple
     * visibility.  Afterwards, however, the tuples we have found to be
     * visible are guaranteed good as long as we hold the buffer pin.
     */
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    /* initialize and check partial seq scan parameters */
    AttrNumber lastVar = UHeapCheckScanDesc(sscan);
    bool *boolArr = (lastVar > 0) ? sscan->boolArr : NULL;

    Page dp = BufferGetPage(buffer);

    if (!(scan->rs_base.rs_pageatatime)) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        scan->rs_base.rs_cbuf = buffer;
        return true;
    }

    Snapshot snapshot = scan->rs_base.rs_snapshot;

    int lines = UHeapPageGetMaxOffsetNumber(dp);
    int ntup = 0;
    RowPtr *nextTup;
    OffsetNumber lineoff;
    RowPtr *lpp;

    for (lineoff = FirstOffsetNumber, lpp = UPageGetRowPtr(dp, lineoff); lineoff <= lines; lineoff++, lpp++) {
        if (RowPtrIsNormal(lpp) || RowPtrIsDeleted(lpp)) {
            nextTup = lpp + 1;
            __builtin_prefetch(dp + nextTup->offset);
            __builtin_prefetch(dp + nextTup->offset + CACHE_LINE_SZ);
            nextTup++;
            __builtin_prefetch(dp + nextTup->offset);

            UHeapTuple resulttup;
            ItemPointerData tid;

            ItemPointerSet(&tid, page, lineoff);

            /* last five params optional, last two params are for UHeapGetTuplePartial */
            bool valid = UHeapTupleFetch(scan->rs_base.rs_rd, buffer, lineoff, snapshot, &resulttup, NULL, false, NULL,
                NULL, NULL, lastVar, boolArr);

            if (resulttup != NULL)
                Assert(resulttup->tupTableType == UHEAP_TUPLE);

            /*
             * If any prior version is visible, we pass latest visible as
             * true. The state of latest version of tuple is determined by the
             * called function.
             *
             * Note that, it's possible that tuple is updated in-place and
             * we're seeing some prior version of that. We handle that case in
             * InplaceHeapTupleHasSerializableConflictOut.
             */
            CheckForSerializableConflictOut(valid, scan->rs_base.rs_rd, (void *)&tid, buffer, snapshot);

            if (valid)
                scan->rs_visutuples[ntup++] = resulttup;
        }
    }

    UnlockReleaseBuffer(buffer);

    Assert(ntup <= CalculatedMaxUHeapTuplesPerPage(UPageGetTDSlotCount(dp)));

    scan->rs_base.rs_ntuples = ntup;

    return true;
}

/*
 * Similar to heapgettup, but for fetching UHeap tuple.
 *
 * Note that here we process only regular UHeap pages
 */
static UHeapTuple UHeapScanGetTuple(UHeapScanDesc scan, ScanDirection dir)
{
    UHeapTuple tuple = scan->rs_cutup;
    Snapshot snapshot = scan->rs_base.rs_snapshot;
    bool backward = ScanDirectionIsBackward(dir);
    BlockNumber page;
    bool finished;
    bool valid;
    Page dp;
    int lines;
    OffsetNumber lineoff = InvalidOffsetNumber;
    int linesleft;
    RowPtr *lpp;

    if (tuple != NULL)
        Assert(tuple->tupTableType == UHEAP_TUPLE);

    /*
     * calculate next starting lineoff, given scan direction
     */
    if (ScanDirectionIsForward(dir)) {
        if (!scan->rs_base.rs_inited) {
            /*
             * return null immediately if relation is empty
             */
            if (scan->rs_base.rs_nblocks == 0) {
                Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
                return NULL;
            }
            page = scan->rs_base.rs_startblock; /* first page */
            valid = UHeapGetPage(&scan->rs_base, page);
            if (!valid) {
                goto get_next_page;
            }
            lineoff = FirstOffsetNumber; /* first offnum */
            scan->rs_base.rs_inited = true;
        } else {
            /* continue from previously returned page/tuple */
            page = scan->rs_base.rs_cblock; /* current page */
            if (tuple != NULL) {
                lineoff = OffsetNumberNext(ItemPointerGetOffsetNumber(&(tuple->ctid))); /* next offnum */
            }
        }

        LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_SHARE);

        dp = BufferGetPage(scan->rs_base.rs_cbuf);
        lines = UHeapPageGetMaxOffsetNumber(dp);
        /* page and lineoff now reference the physically next tid */

        linesleft = lines - lineoff + 1;
    } else if (backward) {
        /* backward parallel scan not supported */
        if (!scan->rs_base.rs_inited) {
            /*
             * return null immediately if relation is empty
             */
            if (scan->rs_base.rs_nblocks == 0) {
                Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
                return NULL;
            }

            /*
             * Disable reporting to syncscan logic in a backwards scan; it's
             * not very likely anyone else is doing the same thing at the same
             * time, and much more likely that we'll just bollix things for
             * forward scanners.
             */
            scan->rs_allow_sync = false;
            /* start from last page of the scan */
            if (scan->rs_base.rs_startblock > 0)
                page = scan->rs_base.rs_startblock - 1;
            else
                page = scan->rs_base.rs_nblocks - 1;
            valid = UHeapGetPage(&scan->rs_base, page);
            if (!valid) {
                goto get_next_page;
            }
        } else {
            /* continue from previously returned page/tuple */
            page = scan->rs_base.rs_cblock; /* current page */
        }

        LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_SHARE);

        dp = BufferGetPage(scan->rs_base.rs_cbuf);
        // InplaceheapTodo no TestForOldSnapshot

        lines = UHeapPageGetMaxOffsetNumber(dp);

        if (!scan->rs_base.rs_inited) {
            lineoff = lines; /* final offnum */
            scan->rs_base.rs_inited = true;
        } else {
            if (tuple != NULL) {
                lineoff = OffsetNumberPrev(ItemPointerGetOffsetNumber(&(tuple->ctid))); /* previous offnum */
            }
        }
        /* page and lineoff now reference the physically previous tid */

        linesleft = lineoff;
    } else {
        if (!scan->rs_base.rs_inited || (tuple == NULL)) {
            Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
            tuple = NULL;
            return tuple;
        }

        page = ItemPointerGetBlockNumber(&(tuple->ctid));
        valid = UHeapGetPage(&scan->rs_base, page);
        if (!valid) {
            ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("Can not refetch prior page")));
            tuple = NULL;
            return tuple;
        }

        lineoff = ItemPointerGetOffsetNumber(&(tuple->ctid));
        /* Since the tuple was previously fetched, needn't lock page here */
        tuple = scan->rs_visutuples[lineoff];
        return tuple;
    }

    /*
     * advance the scan until we find a qualifying tuple or run out of stuff
     * to scan
     */
    lpp = UPageGetRowPtr(dp, lineoff);

get_next_tuple:
    while (linesleft > 0) {
        if (RowPtrIsNormal(lpp)) {
            UHeapTuple tuple = NULL;
            bool valid;

            valid = UHeapTupleFetch(scan->rs_base.rs_rd, scan->rs_base.rs_cbuf, lineoff, snapshot, &tuple, NULL, false);

            /*
             * If any prior version is visible, we pass latest visible as
             * true. The state of latest version of tuple is determined by the
             * called function.
             *
             * Note that, it's possible that tuple is updated in-place and
             * we're seeing some prior version of that. We handle that case in
             * InplaceHeapTupleHasSerializableConflictOut.
             */
            CheckForSerializableConflictOut(valid, scan->rs_base.rs_rd, (void *)&tuple->ctid, scan->rs_base.rs_cbuf,
                snapshot);

            if (valid) {
                LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_UNLOCK);
                return tuple;
            }
        }

        /*
         * otherwise move to the next item on the page
         */
        --linesleft;
        if (backward) {
            --lpp; /* move back in this page's ItemId array */
            --lineoff;
        } else {
            ++lpp; /* move forward in this page's ItemId array */
            ++lineoff;
        }
    }

    /*
     * if we get here, it means we've exhausted the items on this page and
     * it's time to move to the next.
     */
    LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_UNLOCK);

get_next_page:
    for (;;) {
        finished = NextUpage(scan, dir, page);
        /* return NULL if we've exhausted all the pages */
        if (finished) {
            if (BufferIsValid(scan->rs_base.rs_cbuf))
                ReleaseBuffer(scan->rs_base.rs_cbuf);
            scan->rs_base.rs_cbuf = InvalidBuffer;
            scan->rs_base.rs_cblock = InvalidBlockNumber;
            scan->rs_base.rs_inited = false;
            return NULL;
        }

        valid = UHeapGetPage(&scan->rs_base, page);
        if (!valid) {
            continue;
        }

        if (!scan->rs_base.rs_inited)
            scan->rs_base.rs_inited = true;

        LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_SHARE);

        dp = BufferGetPage(scan->rs_base.rs_cbuf);
        lines = UHeapPageGetMaxOffsetNumber((Page)dp);
        linesleft = lines;
        if (backward) {
            lineoff = lines;
            lpp = UPageGetRowPtr(dp, lines);
        } else {
            lineoff = FirstOffsetNumber;
            lpp = UPageGetRowPtr(dp, FirstOffsetNumber);
        }

        goto get_next_tuple;
    }
}


/*
 * ----------------
 * UHeapGetTupleFromPage - fetch next uheap tuple in page-at-a-time mode
 * ----------------
 */
UHeapTuple UHeapGetTupleFromPage(UHeapScanDesc scan, ScanDirection dir)
{
    UHeapTuple tuple = scan->rs_cutup;
    bool backward = ScanDirectionIsBackward(dir);
    BlockNumber page;
    bool finished;
    bool valid;
    int lines;
    int lineindex;
    int linesleft;
    int i = 0;

    if (tuple != NULL)
        Assert(tuple->tupTableType == UHEAP_TUPLE);

    /*
     * calculate next starting lineindex, given scan direction
     */
    if (ScanDirectionIsForward(dir)) {
        if (!scan->rs_base.rs_inited) {
            /*
             * return null immediately if relation is empty
             */
            if (scan->rs_base.rs_nblocks == 0) {
                Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
                tuple = NULL;
                return tuple;
            }

            page = scan->rs_base.rs_startblock; /* first page */
            valid = UHeapGetPage(&scan->rs_base, page);
            if (!valid) {
                goto get_next_page;
            }

            lineindex = 0;
            scan->rs_base.rs_inited = true;
        } else {
            /* continue from previously returned page/tuple */
            page = scan->rs_base.rs_cblock; /* current page */
            lineindex = scan->rs_base.rs_cindex + 1;
        }

        lines = scan->rs_base.rs_ntuples;
        /* page and lineindex now reference the next visible tid */

        linesleft = lines - lineindex;
    } else if (backward) {
        if (!scan->rs_base.rs_inited) {
            /*
             * return null immediately if relation is empty
             */
            if (scan->rs_base.rs_nblocks == 0) {
                Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
                tuple = NULL;
                return tuple;
            }

            /*
             * Disable reporting to syncscan logic in a backwards scan; it's
             * not very likely anyone else is doing the same thing at the same
             * time, and much more likely that we'll just bollix things for
             * forward scanners.
             */
            scan->rs_allow_sync = false;
            /* start from last page of the scan */
            if (scan->rs_base.rs_startblock > 0) {
                page = scan->rs_base.rs_startblock - 1;
            } else {
                page = scan->rs_base.rs_nblocks - 1;
            }
            valid = UHeapGetPage(&scan->rs_base, page);
            if (!valid) {
                goto get_next_page;
            }
        } else {
            /* continue from previously returned page/tuple */
            page = scan->rs_base.rs_cblock; /* current page */
        }

        lines = scan->rs_base.rs_ntuples;

        if (!scan->rs_base.rs_inited) {
            lineindex = lines - 1;
            scan->rs_base.rs_inited = true;
        } else {
            lineindex = scan->rs_base.rs_cindex - 1;
        }
        /* page and lineindex now reference the previous visible tid */

        linesleft = lineindex + 1;
    } else {
        /* ''no movement'' scan direction: refetch prior tuple */
        if (!scan->rs_base.rs_inited || (tuple == NULL)) {
            Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
            tuple = NULL;
            return tuple;
        }

        page = ItemPointerGetBlockNumber(&(tuple->ctid));
        valid = UHeapGetPage(&scan->rs_base, page);
        if (!valid) {
            ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("Can not refetch prior page")));
            tuple = NULL;
            return tuple;
        }

        /* Since the tuple was previously fetched, needn't lock page here */
        tuple = scan->rs_visutuples[scan->rs_base.rs_cindex];
        return tuple;
    }

get_next_tuple:

    /*
     * advance the scan until we find a qualifying tuple or run out of stuff
     * to scan
     */
    if (linesleft > 0) {
        tuple = scan->rs_visutuples[lineindex];
        scan->rs_base.rs_cindex = lineindex;
        return tuple;
    }

    /*
     * if we get here, it means we've exhausted the items on this page and
     * it's time to move to the next. For now we shall free all of the UHeap
     * tuples stored in rs_visinplacetuples. Later a better memory management is
     * required.
     * scan->rs_cutup points to an item in rs_visutuples so reset rs_cutup as well.
     */
    for (i = 0; i < scan->rs_base.rs_ntuples; i++)
        pfree(scan->rs_visutuples[i]);
    scan->rs_base.rs_ntuples = 0;
    scan->rs_cutup = NULL;

get_next_page:
    for (;;) {
        finished = NextUpage(scan, dir, page);
        /* return NULL if we've exhausted all the pages */
        if (finished) {
            if (BufferIsValid(scan->rs_base.rs_cbuf))
                ReleaseBuffer(scan->rs_base.rs_cbuf);
            scan->rs_base.rs_cbuf = InvalidBuffer;
            scan->rs_base.rs_cblock = InvalidBlockNumber;
            tuple = NULL;
            scan->rs_base.rs_inited = false;
            return tuple;
        }

        valid = UHeapGetPage(&scan->rs_base, page);
        if (!valid) {
            continue;
        }

        if (!scan->rs_base.rs_inited)
            scan->rs_base.rs_inited = true;
        lines = scan->rs_base.rs_ntuples;
        linesleft = lines;
        if (backward) {
            lineindex = lines - 1;
        } else {
            lineindex = 0;
        }

        goto get_next_tuple;
    }
}

TableScanDesc UHeapBeginScan(Relation relation, Snapshot snapshot, int nkeys)
{
    UHeapScanDesc uscan;

    /*
     * If the table is a partition table, the current scan must be used by
     * bitmapscan to scan tuples using GPI. Therefore,
     * the value of rs_rd in the scan is used to store partition-fake-relation.
     */
    if (!RelationIsPartitioned(relation)) {
        RelationIncrementReferenceCount(relation);
    }

    uscan = (UHeapScanDesc)palloc0(sizeof(UHeapScanDescData));

    uscan->rs_tupdesc = RelationGetDescr(relation);
    uscan->rs_base.rs_rd = relation;
    uscan->rs_base.rs_snapshot = snapshot;
    uscan->rs_base.rs_nkeys = nkeys;
    uscan->rs_base.rs_startblock = 0;
    uscan->rs_base.rs_ntuples = 0;
    uscan->rs_cutup = NULL;
    if (nkeys > 0) {
        uscan->rs_base.rs_key = (ScanKey)palloc0(sizeof(ScanKeyData) * nkeys);
    } else {
        uscan->rs_base.rs_key = NULL;
    }

    uscan->rs_base.rs_nblocks =
        RelationIsPartitioned(uscan->rs_base.rs_rd) ? 0 : RelationGetNumberOfBlocks(uscan->rs_base.rs_rd);
    uscan->rs_base.rs_inited = false;
    uscan->rs_base.rs_cbuf = InvalidBuffer;
    uscan->rs_base.rs_cblock = InvalidBlockNumber;

    /* Disable page-at-a-time mode if it's not a MVCC-safe snapshot. */
    uscan->rs_base.rs_pageatatime = IsMVCCSnapshot(snapshot);
    uscan->rs_base.rs_strategy = NULL;
    uscan->rs_base.rs_ss_accessor = NULL;
    uscan->rs_ctupBatch = NULL;

    return (TableScanDesc)uscan;
}

static void UHeapinitscan(TableScanDesc sscan, ScanKey key, bool isRescan)
{
    UHeapScanDesc scan = (UHeapScanDesc)sscan;
    bool allowStrat;
    bool allowSync;

    /*
     * Determine the number of blocks we have to scan.
     *
     * It is sufficient to do this once at scan start, since any tuples added
     * while the scan is in progress will be invisible to my snapshot anyway.
     * (That is not true when using a non-MVCC snapshot.  However, we couldn't
     * guarantee to return tuples added after scan start anyway, since they
     * might go into pages we already scanned.  To guarantee consistent
     * results for a non-MVCC snapshot, the caller must hold some higher-level
     * lock that ensures the interesting tuple(s) won't change.)
     */
    scan->rs_base.rs_nblocks =
        RelationIsPartitioned(scan->rs_base.rs_rd) ? 0 : RelationGetNumberOfBlocks(scan->rs_base.rs_rd);

    /*
     * If the table is large relative to NBuffers, use a bulk-read access
     * strategy and enable synchronized scanning (see syncscan.c).  Although
     * the thresholds for these features could be different, we make them the
     * same so that there are only two behaviors to tune rather than four.
     * (However, some callers need to be able to disable one or both of these
     * behaviors, independently of the size of the table; also there is a GUC
     * variable that can disable synchronized scanning.)
     *
     * Note that HeapParallelscanInitialize has a very similar test; if you
     * change this, consider changing that one, too.
     */
    if (!RelationUsesLocalBuffers(scan->rs_base.rs_rd) &&
        scan->rs_base.rs_nblocks > (uint32)g_instance.attr.attr_storage.NBuffers / BULKSCAN_BLOCKS_PER_BUFFER) {
        allowStrat = scan->rs_allow_strat;
        allowSync = scan->rs_allow_sync;
    } else
        allowStrat = allowSync = false;

    if (allowStrat) {
        /* During a rescan, keep the previous strategy object. */
        if (scan->rs_base.rs_strategy == NULL)
            scan->rs_base.rs_strategy = GetAccessStrategy(BAS_BULKREAD);
    } else {
        if (scan->rs_base.rs_strategy != NULL)
            FreeAccessStrategy(scan->rs_base.rs_strategy);
        scan->rs_base.rs_strategy = NULL;
    }

    if (isRescan) {
        scan->rs_base.rs_syncscan = (allowSync && u_sess->attr.attr_storage.synchronize_seqscans);
    } else if (allowSync && u_sess->attr.attr_storage.synchronize_seqscans) {
        scan->rs_base.rs_syncscan = true;
        scan->rs_base.rs_startblock = ss_get_location(scan->rs_base.rs_rd, scan->rs_base.rs_nblocks);
    } else {
        scan->rs_base.rs_syncscan = false;
        scan->rs_base.rs_startblock = 0;
    }

    scan->rs_base.rs_inited = false;
    scan->rs_base.rs_cbuf = InvalidBuffer;
    scan->rs_base.rs_cblock = InvalidBlockNumber;

    if (scan->rs_base.rs_rd->rd_tam_type == TAM_USTORE) {
        scan->rs_base.lastVar = -1;
        scan->rs_base.boolArr = NULL;
    }

    /* page-at-a-time fields are always invalid when not rs_inited */

    /*
     * copy the scan key, if appropriate
     */
    if (key != NULL) {
        errno_t rc = memcpy_s(scan->rs_base.rs_key, scan->rs_base.rs_nkeys * sizeof(ScanKeyData), key,
            scan->rs_base.rs_nkeys * sizeof(ScanKeyData));
        securec_check(rc, "\0", "\0");
    }

    /*
     * Currently, we only have a stats counter for sequential heap scans (but
     * e.g for bitmap scans the underlying bitmap index scans will be counted,
     * and for sample scans we update stats for tuple fetches).
     */
    if (!scan->rs_bitmapscan && !scan->rs_samplescan)
        pgstat_count_heap_scan(scan->rs_base.rs_rd);
}


void UHeapRescan(TableScanDesc sscan, ScanKey key)
{
    UHeapScanDesc scan = (UHeapScanDesc)sscan;
    /*
     * unpin scan buffers
     */
    if (BufferIsValid(scan->rs_base.rs_cbuf))
        ReleaseBuffer(scan->rs_base.rs_cbuf);

    /*
     * reinitialize scan descriptor
     */
    UHeapinitscan(sscan, key, true);
}

void UHeapEndScan(TableScanDesc scan)
{
    UHeapScanDesc uscan = (UHeapScanDesc)scan;

    /*
     * unpin scan buffers
     */
    if (BufferIsValid(uscan->rs_base.rs_cbuf))
        ReleaseBuffer(uscan->rs_base.rs_cbuf);

    /*
     * decrement relation reference count and free scan descriptor storage
     */
    if (!RelationIsPartitioned(uscan->rs_base.rs_rd)) {
        RelationDecrementReferenceCount(uscan->rs_base.rs_rd);
    }

    if (uscan->rs_base.rs_key)
        pfree(uscan->rs_base.rs_key);

    if (uscan->rs_ctupBatch != NULL) {
        pfree_ext(uscan->rs_ctupBatch);
    }

    pfree(uscan);
}

UHeapTuple UHeapGetNextSlotGuts(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
    UHeapScanDesc scan = (UHeapScanDesc)sscan;
    UHeapTuple uhtup = NULL;

    /* Possible to get here without valid table scan info */
    /* Getting whole utuple if already in a slot */
    scan->rs_base.lastVar = -1;
    scan->rs_base.boolArr = NULL;

    /*
     * The key will be passed only for catalog table scans and catalog tables
     * are always a heap table!. So in case of uheap it should be set to NULL.
     */
    Assert(scan->rs_base.rs_key == NULL);

    if (scan->rs_base.rs_pageatatime) {
        uhtup = UHeapGetTupleFromPage(scan, direction);
    } else {
        uhtup = UHeapScanGetTuple(scan, direction);
    }

    if (uhtup == NULL) {
        ExecClearTuple(slot);
        return NULL;
    }

    Assert(uhtup->tupTableType == UHEAP_TUPLE);

    scan->rs_cutup = uhtup;

    ExecStoreTuple(uhtup, slot, InvalidBuffer, !(scan->rs_base.rs_pageatatime));

    return uhtup;
}

HeapTuple UHeapGetNextSlot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
    UHeapScanDesc scan = (UHeapScanDesc)sscan;
    HeapTuple htup = NULL;
    UHeapTuple uhtup = NULL;

    if ((uhtup = UHeapGetNextSlotGuts(sscan, direction, slot)) != NULL) {
        htup = UHeapToHeap(scan->rs_tupdesc, uhtup);
    }

    return htup;
}

/*
 * UHeapSearchBuffer - search tuple satisfying snapshot
 *
 * On entry, *tid is the TID of a tuple, and buffer is the buffer holding
 * this tuple.  We search for the first visible member satisfying the given
 * snapshot. If one is found, we return the tuple, in addition to updating
 * *tid. Return NULL otherwise.
 *
 * The caller must already have pin and (at least) share lock on the buffer;
 * it is still pinned/locked at exit.  Also, We do not report any pgstats
 * count; caller may do so if wanted.
 */
UHeapTuple UHeapSearchBuffer(ItemPointer tid, Relation relation, Buffer buffer,
                             Snapshot snapshot, bool *allDead, UHeapTuple freebuf)
{
    Page dp = (Page)BufferGetPage(buffer);
    UHeapTuple pagetup = NULL;
    UHeapTuple resulttup = NULL;

    if (allDead) {
        *allDead = false;
    }

    Assert(ItemPointerGetBlockNumber(tid) == BufferGetBlockNumber(buffer));
    OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
    /* check for bogus TID */
    if (offnum < FirstOffsetNumber || offnum > UHeapPageGetMaxOffsetNumber(dp))
        return NULL;

    RowPtr *lp = UPageGetRowPtr(dp, offnum);

    /* check for unused or dead items */
    if (!(RowPtrIsNormal(lp) || RowPtrIsDeleted(lp))) {
        if (allDead) {
            *allDead = true;
        }
        return NULL;
    }

    if (!RowPtrIsDeleted(lp))
        pagetup = UHeapGetTuple(relation, buffer, offnum, freebuf);

    WHITEBOX_TEST_STUB(UHEAP_SEARCH_BUFFER_FAILED, WhiteboxDefaultErrorEmit);

    /*
     * If the record is deleted, its place in the page might have been taken
     * by another of its kind. Try to get it from the UNDO if it is still
     * visible.
     */
    UHeapTupleTransInfo tdinfo;
    bool gotTDInfo = false;
    if (UHeapTupleFetch(relation, buffer, offnum, snapshot, &resulttup, NULL, false, &tdinfo, &gotTDInfo,
        &pagetup)) {
        TransactionId insertxid = InvalidTransactionId;

        if (IsSerializableXact()) {
            if (!RowPtrIsDeleted(lp)) {
                /*
                 * To fetch the xmin (aka transaction that has inserted the
                 * tuple), we need to use the transaction slot of the tuple in
                 * the page instead of the tuple from undo, otherwise, it
                 * might traverse the wrong chain.
                 *
                 * ZBORKED: This seems like an ugly kludge.  Can't we find
                 * another way to make sure we DON'T traverse the wrong undo
                 * chain?  And what does "wrong" mean here anyway?  And why
                 * don't we have similar problems when the tuple is deleted?
                 */
                insertxid = UHeapFetchInsertXid(pagetup, buffer);
            } else
                insertxid = UHeapFetchInsertXid(resulttup, buffer);
        }

        PredicateLockTid(relation, &(resulttup->ctid), snapshot, insertxid);
    }

    /*
     * If any prior version is visible, we pass latest visible as true. The
     * state of latest version of tuple is determined by the called function.
     *
     * Note that, it's possible that tuple is updated in-place and we're
     * seeing some prior version of that. We handle that case in
     * InplaceHeapTupleHasSerializableConflictOut.
     */
    CheckForSerializableConflictOut((resulttup != NULL), relation, (void *)tid, buffer, snapshot);

    if (resulttup) {
        /* set the tid */
        *tid = resulttup->ctid;
    } else {
        /*
         * If we can't see it, maybe no one else can either.  At caller
         * request, check whether tuple is dead to all transactions.
         * This should be a quick check because we grabbed the TD information
         * from UHeapTupleFetch already.
         */
        if (allDead) {
            *allDead = UHeapTupleIsSurelyDead(pagetup, buffer, offnum, &tdinfo, gotTDInfo);
        }
    }

    /*
     * pagetup can be reused as resulttup, we cannot easily free pagetup here.
     * See UHeapScanBitmapNextBlock for detail. We also cannot pfree pagetup if
     * the caller is passing in a preallocated buffer.
     */
    if (!freebuf && pagetup && resulttup != pagetup)
        pfree(pagetup);

    if (resulttup) {
        resulttup->xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
    }

    return resulttup;
}

bool UHeapScanBitmapNextBlock(TableScanDesc sscan, const TBMIterateResult *tbmres)
{
    UHeapScanDesc scan = (UHeapScanDesc)sscan;
    BlockNumber page = tbmres->blockno;

    scan->rs_base.rs_cindex = 0;
    scan->rs_base.rs_ntuples = 0;

    /*
     * Ignore any claimed entries past what we think is the end of the
     * relation.  (This is probably not necessary given that we got at least
     * AccessShareLock on the table before performing any of the indexscans,
     * but let's be safe.)
     */
    if (page >= scan->rs_base.rs_nblocks) {
        return false;
    }

    scan->rs_base.rs_cbuf = ReleaseAndReadBuffer(scan->rs_base.rs_cbuf, scan->rs_base.rs_rd, page);
    Buffer buffer = scan->rs_base.rs_cbuf;
    Snapshot snapshot = scan->rs_base.rs_snapshot;

    int ntup = 0;

    /*
     * We must hold share lock on the buffer content while examining tuple
     * visibility.  Afterwards, however, the tuples we have found to be
     * visible are guaranteed good as long as we hold the buffer pin.
     */
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    Page dp = (Page)BufferGetPage(buffer);

    /*
     * We need two separate strategies for lossy and non-lossy cases.
     */
    if (tbmres->ntuples >= 0) {
        /*
         * Bitmap is non-lossy, so we just look through the offsets listed in
         * tbmres;
         */

        for (int curslot = 0; curslot < tbmres->ntuples; curslot++) {
            OffsetNumber offnum = tbmres->offsets[curslot];
            ItemPointerData tid;

            ItemPointerSet(&tid, page, offnum);
            UHeapTuple utuple = UHeapSearchBuffer(&tid, scan->rs_base.rs_rd, buffer, snapshot, NULL);
            if (utuple != NULL) {
                Assert(utuple->tupTableType == UHEAP_TUPLE);
                scan->rs_visutuples[ntup++] = utuple;
            }
        }
    } else {
        /*
         * Bitmap is lossy, so we must examine each item pointer on the page.
         */
        OffsetNumber maxoff = UHeapPageGetMaxOffsetNumber(dp);

        for (OffsetNumber offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
            UHeapTuple resulttup;
            ItemPointerData tid;

            RowPtr *lpp = UPageGetRowPtr(dp, offnum);
            if (!RowPtrIsNormal(lpp)) {
                continue;
            }

            ItemPointerSet(&tid, page, offnum);

            bool valid = UHeapTupleFetch(scan->rs_base.rs_rd, buffer, offnum, snapshot, &resulttup, NULL, false);
            if (valid) {
                Assert(resulttup->tupTableType == UHEAP_TUPLE);
                PredicateLockTid(scan->rs_base.rs_rd, &(resulttup->ctid), snapshot,
                    IsSerializableXact() ? UHeapFetchInsertXid(resulttup, buffer) : InvalidTransactionId);
            }

            /*
             * If any prior version is visible, we pass latest visible as
             * true. The state of latest version of tuple is determined by the
             * called function.
             *
             * Note that, it's possible that tuple is updated in-place and
             * we're seeing some prior version of that. We handle that case in
             * ZHeapTupleHasSerializableConflictOut.
             */
            CheckForSerializableConflictOut(valid, scan->rs_base.rs_rd, (void *)&tid, buffer, snapshot);

            if (valid) {
                scan->rs_visutuples[ntup++] = resulttup;
            }
        }
    }

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    Assert(ntup <= CalculatedMaxUHeapTuplesPerPage(UPageGetTDSlotCount(dp)));
    scan->rs_base.rs_ntuples = ntup;
    return true;
}

bool UHeapScanBitmapNextTuple(TableScanDesc sscan, TBMIterateResult *tbmres, TupleTableSlot *slot)
{
    UHeapScanDesc scan = (UHeapScanDesc)sscan;

    if (scan->rs_base.rs_cindex < 0 || scan->rs_base.rs_cindex >= scan->rs_base.rs_ntuples) {
        return false;
    }

    scan->rs_cutup = scan->rs_visutuples[scan->rs_base.rs_cindex];

    /*
     * Set up the result slot to point to this tuple. We don't need to keep
     * the pin on the buffer, since we only scan tuples in page mode.
     */
    ExecStoreTuple(scan->rs_cutup, slot, InvalidBuffer, true);

    scan->rs_base.rs_cindex++;

    return true;
}

void UHeapMarkPos(TableScanDesc uscan)
{
    UHeapScanDesc scan = (UHeapScanDesc) uscan;

    /* Note: no locking manipulations needed */
    if (scan->rs_cutup->disk_tuple != NULL) {
        scan->rs_mctid = scan->rs_cutup->ctid;
        if (scan->rs_base.rs_pageatatime) {
            scan->rs_mindex = scan->rs_base.rs_cindex;
        }
    } else
        ItemPointerSetInvalid(&scan->rs_mctid);
}

void UHeapRestRpos(TableScanDesc sscan)
{
    UHeapScanDesc scan = (UHeapScanDesc) sscan;
    /* XXX no amrestrpos checking that ammarkpos called */
    if (!ItemPointerIsValid(&scan->rs_mctid) && (scan->rs_cutup != NULL)) {
        scan->rs_cutup->disk_tuple = NULL;

        /*
         * unpin scan buffers
         */
        if (BufferIsValid(scan->rs_base.rs_cbuf)) {
            ReleaseBuffer(scan->rs_base.rs_cbuf);
        }
        scan->rs_base.rs_cbuf = InvalidBuffer;
        scan->rs_base.rs_cblock = InvalidBlockNumber;
        scan->rs_base.rs_inited = false;
    } else {
        /*
         * If we reached end of scan, rs_inited will now be false.	We must
         * reset it to true to keep heapgettup from doing the wrong thing.
         */
        scan->rs_base.rs_inited = true;
        
        UHeapTuple utuple;
        UHeapTupleData uheaptupdata;
        utuple = &uheaptupdata;
        struct {
            UHeapDiskTupleData hdr;
            char data[MaxPossibleUHeapTupleSize];
        } tbuf;

        errno_t errorNo = EOK;
        errorNo = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
        securec_check(errorNo, "\0", "\0");

        utuple->disk_tuple = &tbuf.hdr;
        utuple->ctid = scan->rs_mctid;
        if (scan->rs_cutup == NULL) {
            scan->rs_cutup = utuple;
        } else {
            scan->rs_cutup->ctid = scan->rs_mctid;
        }

        if (scan->rs_base.rs_pageatatime) {
            scan->rs_base.rs_cindex = scan->rs_mindex;
            UHeapGetTupleFromPage(scan, NoMovementScanDirection);
        } else {
            UHeapScanGetTuple(scan, NoMovementScanDirection);
        }
    }
}

UHeapTuple UHeapGetNext(TableScanDesc sscan, ScanDirection dir)
{
    UHeapScanDesc scan = (UHeapScanDesc)sscan;
    UHeapTuple uhtup = NULL;

    /*
     * The key will be passed only for catalog table scans and catalog tables
     * are always a heap table!. So in case of uheap it should be set to NULL.
     */
    Assert(scan->rs_base.rs_key == NULL);

    if (scan->rs_base.rs_pageatatime) {
        uhtup = UHeapGetTupleFromPage(scan, dir);
    } else {
        uhtup = UHeapScanGetTuple(scan, dir);
    }

    if (uhtup == NULL) {
        return NULL;
    }

    scan->rs_cutup = uhtup;

    return scan->rs_cutup;
}

Buffer UHeapIndexBuildNextBlock(UHeapScanDesc scan)
{
    BlockNumber blkno = InvalidBlockNumber;
    if (scan->rs_base.rs_cblock == InvalidBlockNumber) {
        /* first page, init rs_visutuples array and other information */
        blkno = 0;
        scan->rs_base.rs_ntuples = 0;
    } else {
        blkno = scan->rs_base.rs_cblock + 1;
    }
    /* cleanup the workspace */
    for (int i = 0; i < scan->rs_base.rs_ntuples; i++) {
        pfree(scan->rs_visutuples[i]);
    }
    scan->rs_base.rs_ntuples = 0;
    scan->rs_cutup = NULL;
    if (BufferIsValid(scan->rs_base.rs_cbuf)) {
        ReleaseBuffer(scan->rs_base.rs_cbuf);
        scan->rs_base.rs_cbuf = InvalidBuffer;
    }

    if (blkno >= scan->rs_base.rs_nblocks) {
        return InvalidBuffer; /* we are done */
    }
    scan->rs_base.rs_cblock = blkno;

    /* read the next page, and lock with exclusive mode */
    Buffer buf = ReadBufferExtended(scan->rs_base.rs_rd, MAIN_FORKNUM, blkno, RBM_NORMAL, scan->rs_base.rs_strategy);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    scan->rs_base.rs_cbuf = buf;
    return buf;
}

bool UHeapIndexBuildNextPage(UHeapScanDesc scan)
{
    Buffer buf = UHeapIndexBuildNextBlock(scan);
    if (!BufferIsValid(buf)) {
        return false;
    }

    Page page = BufferGetPage(buf);

    /* step 1: rollback all abort transactions */
    int numSlots = GetTDCount((UHeapPageHeaderData *)page);
    TD *tdSlots = (TD *)PageGetTDPointer(page);
    UndoRecPtr *urecptr = (UndoRecPtr *)palloc(numSlots * sizeof(UndoRecPtr));
    TransactionId *fxid = (TransactionId *)palloc(numSlots * sizeof(TransactionId));

    int nAborted = 0;
    for (int slotNo = 0; slotNo < numSlots; slotNo++) {
        TransactionId xid = tdSlots[slotNo].xactid;
        if (!TransactionIdIsValid(xid) || TransactionIdIsCurrentTransactionId(xid) || TransactionIdDidCommit(xid)) {
            continue; /* xid visible in SnapshotNow */
        }
        /* xid is abort, record and rollback later */
        urecptr[nAborted] = tdSlots[slotNo].undo_record_ptr;
        fxid[nAborted] = xid;
        nAborted++;
    }
    if (nAborted > 0) {
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        for (int i = 0; i < nAborted; i++) {
            ExecuteUndoActionsPage(urecptr[i], scan->rs_base.rs_rd, buf, fxid[i]);
        }
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    }
    pfree(urecptr);
    pfree(fxid);

    /* step 2: try prune this page */
    TransactionId xid = GetCurrentTransactionId();
    UHeapPagePruneFSM(scan->rs_base.rs_rd, buf, xid, page, BufferGetBlockNumber(buf));

    /* step 3: scan over all tuples and cache visible tuples */
    int ntup = 0;
    OffsetNumber maxoff = UHeapPageGetMaxOffsetNumber(page);
    for (OffsetNumber offnum = FirstOffsetNumber; offnum <= maxoff; offnum++) {
        RowPtr *rp = UPageGetRowPtr(page, offnum);
        if (!RowPtrIsNormal(rp)) {
            continue; /* deleted or unused is not visible here */
        }
        /* get the tuple without copy */
        UHeapTuple tuple = (UHeapTuple)uheaptup_alloc(UHeapTupleDataSize);
        tuple->disk_tuple = (UHeapDiskTuple)UPageGetRowData(page, rp);

        uint16 infomask = tuple->disk_tuple->flag;
        if ((infomask & (UHEAP_UPDATED | UHEAP_DELETED)) != 0) {
            pfree(tuple);
            continue; /* the last operation removed this tuple */
        }
        /* set up other fields */
        tuple->table_oid = RelationGetRelid(scan->rs_base.rs_rd);
        tuple->xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
        tuple->disk_tuple_size = RowPtrGetLen(rp);
        ItemPointerSet(&tuple->ctid, BufferGetBlockNumber(buf), offnum);

        /* save this tuple */
        scan->rs_visutuples[ntup++] = tuple;
    }
    scan->rs_base.rs_ntuples = ntup;
    scan->rs_base.rs_cindex = 0;

    /* drop the lock but keep the pin */
    LockBuffer(buf, BUFFER_LOCK_UNLOCK);
    return true;
}

UHeapTuple UHeapIndexBuildGetNextTuple(UHeapScanDesc scan, TupleTableSlot *slot)
{
    if (scan->rs_base.rs_snapshot->satisfies != SNAPSHOT_NOW) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("We must use SnapshotNow to build a ustore index.")));
    }
    /* get the next page after all cached tuples of the current page are returned */
    while (scan->rs_base.rs_cblock == InvalidBlockNumber || scan->rs_base.rs_cindex >= scan->rs_base.rs_ntuples) {
        if (!UHeapIndexBuildNextPage(scan)) {
            ExecClearTuple(slot);
            return NULL;
        }
    }
    int lineindex = scan->rs_base.rs_cindex;
    UHeapTuple tuple = scan->rs_visutuples[lineindex];
    scan->rs_base.rs_cindex++; /* now rs_cindex indicate the next tuple's index */
    scan->rs_cutup = tuple;

    ExecStoreTuple(tuple, slot, InvalidBuffer, false);
    return tuple;
}

/*
 * Scan one page for batch scan mode.
 * Return true if all the pages are exhausted.
 */
static void UHeapScanPagesForBatchMode(UHeapScanDesc scan, int lineIndex)
{
    int lines, rows = 0;
    UHeapScanDesc uScan = (UHeapScanDesc)scan;
    lines = scan->rs_base.rs_ntuples;

    while (lineIndex < lines) {
        scan->rs_ctupBatch[rows++] = uScan->rs_visutuples[lineIndex];
        if (rows == scan->rs_base.rs_maxScanRows) {
            break;
        }
        lineIndex++;
    }

    scan->rs_base.rs_cindex = lineIndex;
    scan->rs_base.rs_ctupRows = rows;
}

bool UHeapGetTupPageBatchmode(UHeapScanDesc scan, ScanDirection dir)
{
    BlockNumber page;

    int lineIndex;
    bool finished = false;

    scan->rs_base.rs_ctupRows = 0;

    /* IO collector and IO scheduler for seqsan */
    if (ENABLE_WORKLOAD_CONTROL) {
        IOSchedulerAndUpdate(IO_TYPE_READ, 1, IO_TYPE_ROW);
    }

    /* calculate next starting lineindex, given scan direction */
    if (!scan->rs_base.rs_inited) {
        /* return null immediately if relation is empty */
        if (scan->rs_base.rs_nblocks == 0) {
            Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
            scan->rs_base.rs_ctupRows = 0;
            return true;
        }
        page = scan->rs_base.rs_startblock;
        UHeapGetPage((TableScanDesc)scan, page);
        lineIndex = 0;
        scan->rs_base.rs_inited = true;
    } else {
        /* continue from previously returned page/tuple */
        page = scan->rs_base.rs_cblock;
        lineIndex = scan->rs_base.rs_cindex + 1;
    }

    for (;;) {
        if (lineIndex < scan->rs_base.rs_ntuples) {
            UHeapScanPagesForBatchMode(scan, lineIndex);
            break;
        }

        finished = NextUpage(scan, dir, page);
        /* return NULL if we've exhausted all the pages */
        if (finished) {
            scan->rs_base.rs_cbuf = InvalidBuffer;
            scan->rs_base.rs_cblock = InvalidBlockNumber;
            scan->rs_base.rs_inited = false;
            scan->rs_base.rs_ctupRows = 0;
            return true;
        } else {
            if (unlikely(!UHeapGetPage((TableScanDesc)scan, page))) {
                continue;
            }
            lineIndex = 0;
        }
    }

    return false;
}

static void SkipToNewUPage(UHeapScanDesc scan, ScanDirection dir, BlockNumber page,
    bool *finished, bool *isValidRelationPage)
{
    MemoryContext verifyContext = CurrentMemoryContext;
    UHeapTuple tuple = scan->rs_cutup;
    bool tryNextPage = false;

    while (!*finished) {
        /* advance to next/prior page and detect end of scan */
        *finished = NextUpage(scan, dir, page);

        /* try_next_page is used to judge whether we need to continue. */
        tryNextPage = false;

        /* return NULL if we've exhausted all the pages. */
        if (*finished) {
            if (BufferIsValid(scan->rs_base.rs_cbuf)) {
                ReleaseBuffer(scan->rs_base.rs_cbuf);
            }
            scan->rs_base.rs_cbuf = InvalidBuffer;
            scan->rs_base.rs_cblock = InvalidBlockNumber;
            tuple = NULL;
            scan->rs_base.rs_inited = false;
            return;
        }

        PG_TRY();
        {
            UHeapGetPage(&scan->rs_base, page);
        }
        PG_CATCH();
        {
            (void)MemoryContextSwitchTo(verifyContext);
            *isValidRelationPage = false;
            /*
             * VerifyAbortBufferIO is used for special error handling for verify after catching exceptions,
             * so that it can handle the next operation.
             */
            VerifyAbortBufferIO();

            FlushErrorState();
            ereport(WARNING,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("Page verification failed on complete mode. "
                           "The node is %s, invalid page %u of relation %s.%s, the file is %s.",
                        g_instance.attr.attr_common.PGXCNodeName,
                        page,
                        get_namespace_name(RelationGetNamespace(scan->rs_base.rs_rd)),
                        RelationGetRelationName(scan->rs_base.rs_rd),
                        relpathperm(scan->rs_base.rs_rd->rd_node, MAIN_FORKNUM)),
                    handle_in_client(true)));
            tryNextPage = true;
        }
        PG_END_TRY();
        if (tryNextPage) {
            continue;
        }
        return;
    }
    return;
}

static bool VerifyUHeapGetTup(UHeapScanDesc scan, ScanDirection dir)
{
    UHeapTuple tuple = scan->rs_cutup;
    BlockNumber page;
    bool finished = false;
    bool isValidPage = true;
    int lines;
    OffsetNumber lineOff;
    int linesLeft;
    MemoryContext verifyContext = CurrentMemoryContext;

    /* calculate next starting line_off, given scan direction */
    Assert(ScanDirectionIsForward(dir));

    if (!scan->rs_base.rs_inited) {
        if (scan->rs_base.rs_nblocks == 0) {
            Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
            tuple = NULL;
            return tuple;
        }
        page = scan->rs_base.rs_startblock;
        scan->rs_base.rs_cblock = page;
        PG_TRY();
        {
            UHeapGetPage(&scan->rs_base, page);
        }
        PG_CATCH();
        {
            (void)MemoryContextSwitchTo(verifyContext);
            isValidPage = false;
            /*
             * VerifyAbortBufferIO is used for special error handling for verify after catching exceptions,
             * so that it can handle the next operation.
             */
            VerifyAbortBufferIO();

            FlushErrorState();
            ereport(WARNING,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("Page verification failed on complete mode."
                           "The node is %s, invalid page %u of relation %s.%s, the file is %s.",
                        g_instance.attr.attr_common.PGXCNodeName,
                        page,
                        get_namespace_name(RelationGetNamespace(scan->rs_base.rs_rd)),
                        RelationGetRelationName(scan->rs_base.rs_rd),
                        relpathperm(scan->rs_base.rs_rd->rd_node, MAIN_FORKNUM)),
                        handle_in_client(true)));

            SkipToNewUPage(scan, dir, scan->rs_base.rs_cblock, &finished, &isValidPage);
        }
        PG_END_TRY();
        if (finished) {
            return isValidPage;
        }
        lineOff = 0;
        scan->rs_base.rs_inited = true;
    } else {
        /* continue from previously returned page/tuple */
        page = scan->rs_base.rs_cblock;
        lineOff = scan->rs_base.rs_cindex + 1;
    }

    lines = scan->rs_base.rs_ntuples;
    /* page and lineoff now reference the physically next tid */
    linesLeft = lines - lineOff;

    for (;;) {
        /*
         * advance the scan until we find a qualifying tuple or run out of stuff
         * to scan
         */
        if (linesLeft > 0) {
            tuple = scan->rs_visutuples[lineOff];
            scan->rs_base.rs_cindex = lineOff;
            return tuple;
        }

        /*
         * if we get here, it means we've exhausted the items on this page and
         * it's time to move to the next.
         */
        for (int i = 0; i < scan->rs_base.rs_ntuples; i++) {
            pfree(scan->rs_visutuples[i]);
        }
        scan->rs_base.rs_ntuples = 0;
        scan->rs_cutup = NULL;
        page = scan->rs_base.rs_cblock;
        SkipToNewUPage(scan, dir, page, &finished, &isValidPage);
        if (finished) {
            return isValidPage;
        }

        if (!scan->rs_base.rs_inited) {
            scan->rs_base.rs_inited = true;
        }
        lines = scan->rs_base.rs_ntuples;
        linesLeft = lines;
        lineOff = 0;
    }
}

UHeapTuple UHeapGetNextForVerify(TableScanDesc sscan, ScanDirection direction, bool& isValidRelationPage)
{
    UHeapScanDesc scan = (UHeapScanDesc) sscan;
    isValidRelationPage = VerifyUHeapGetTup(scan, direction);

    if (scan->rs_cutup == NULL) {
        return NULL;
    }

    return (scan->rs_cutup);
}
