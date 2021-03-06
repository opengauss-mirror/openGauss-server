/* -------------------------------------------------------------------------
 *
 * heapam.cpp
 *	  heap access method code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/heap/heapam.cpp
 *
 *
 * INTERFACE ROUTINES
 *		relation_open	- open any relation by relation OID
 *		relation_openrv - open any relation specified by a RangeVar
 *		relation_close	- close any relation
 *		heap_open		- open a heap relation by relation OID
 *		heap_openrv		- open a heap relation specified by a RangeVar
 *		heap_close		- (now just a macro for relation_close)
 *		heap_beginscan	- begin relation scan
 *		heap_rescan		- restart a relation scan
 *		heap_endscan	- end relation scan
 *		heap_getnext	- retrieve next tuple in scan
 *		heap_fetch		- retrieve tuple with given tid
 *		heap_insert		- insert tuple into a relation
 *		heap_multi_insert - insert multiple tuples into a relation
 *		heap_delete		- delete a tuple from a relation
 *		heap_update		- replace a tuple in a relation with another tuple
 *		heap_markpos	- mark scan position
 *		heap_restrpos	- restore position to marked location
 *		heap_sync		- sync heap, for when no WAL has been written
 *
 * NOTES
 *	  This file contains the heap_ routines which implement
 *	  the POSTGRES heap access method used for all POSTGRES
 *	  relations.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/heapam.h"
#include "access/hio.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/valid.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "commands/dbcommands.h"
#include "commands/verify.h"
#include "commands/matview.h"
#include "distributelayer/streamMain.h"
#include "executor/nodeModifyTable.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "replication/dataqueue.h"
#include "replication/datasender.h"
#include "replication/walsender.h"
#include "storage/buf/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "storage/standby.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/partcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/guc.h"
#include "access/cstore_insert.h"
#include "access/cstore_delete.h"
#include "vecexecutor/vectorbatch.h"
#include "access/xlogproc.h"
#include "access/multi_redo_api.h"
#include "catalog/pg_hashbucket_fn.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/access_gstrace.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/storage/ts_store_insert.h"
#endif /* ENABLE_MULTIPLE_NODES */

#ifdef PGXC
#include "pgxc/pgxc.h"
#include "pgxc/redistrib.h"
#include "replication/bcm.h"
#endif

#define DECOMPRESS_HEAP_TUPLE(_isCompressed, _heapTuple, _destTupleData, _rd_att, _heapPage) do { \
    if ((_isCompressed)) {                                                                 \
        HeapTupleData _srcTuple = *(_heapTuple);                                           \
        Assert((_heapPage));                                                               \
        /* Then set the memory for decompressed tuple */                                   \
        (_heapTuple)->t_data = (_destTupleData);                                           \
        heapCopyCompressedTuple(&_srcTuple, (_rd_att), (char *)(_heapPage), (_heapTuple)); \
    }                                                                                      \
} while (0)



/*
 * Determine the number of blocks we have to scan.
 *
 * It is sufficient to do this once at scan start, since any tuples added
 * while the scan is in progress will be invisible to my snapshot anyway.
 * (That is not true when using a non-MVCC snapshot.  However, we couldn't
 * guarantee to return tuples added after scan start anyway, since they
 * might go into pages we already scanned.	To guarantee consistent
 * results for a non-MVCC snapshot, the caller must hold some higher-level
 * lock that ensures the interesting tuple(s) won't change.)
 */
static inline void InitScanBlocks(HeapScanDesc scan, RangeScanInRedis rangeScanInRedis)
{
    BlockNumber nblocks;

    if (RelationIsPartitioned(scan->rs_base.rs_rd)) {
        /*  partition table just set Initial Value, in BitmapHeapTblNext will update */
        nblocks = InvalidBlockNumber;
    } else {
        nblocks = RelationGetNumberOfBlocks(scan->rs_base.rs_rd);
    }
    if (nblocks > 0 && rangeScanInRedis.isRangeScanInRedis) {
        ItemPointerData start_ctid;
        ItemPointerData end_ctid;

        RelationGetCtids(scan->rs_base.rs_rd, &start_ctid, &end_ctid);
        if (rangeScanInRedis.sliceTotal <= 1) {
            Assert(rangeScanInRedis.sliceIndex == 0);
            scan->rs_base.rs_nblocks = RedisCtidGetBlockNumber(&end_ctid) - RedisCtidGetBlockNumber(&start_ctid) + 1;
            scan->rs_base.rs_startblock = RedisCtidGetBlockNumber(&start_ctid);
        } else {
            ItemPointer sctid = eval_redis_func_direct_slice(&start_ctid, &end_ctid, true,
                                                             rangeScanInRedis.sliceTotal,
                                                             rangeScanInRedis.sliceIndex);
            ItemPointer ectid = eval_redis_func_direct_slice(&start_ctid, &end_ctid, false,
                                                             rangeScanInRedis.sliceTotal,
                                                             rangeScanInRedis.sliceIndex);
            scan->rs_base.rs_startblock = RedisCtidGetBlockNumber(sctid);
            scan->rs_base.rs_nblocks    = RedisCtidGetBlockNumber(ectid) - scan->rs_base.rs_startblock + 1;
        }
        ereport(LOG, (errmsg("start block is %d, nblock is %d, start_ctid is %d, end_ctid is %d, sliceTotal is %d, "
            "sliceIndex is %d", scan->rs_base.rs_startblock, scan->rs_base.rs_nblocks, RedisCtidGetBlockNumber(&start_ctid),
            RedisCtidGetBlockNumber(&end_ctid), rangeScanInRedis.sliceTotal, rangeScanInRedis.sliceIndex)));
    } else {
        scan->rs_base.rs_nblocks = nblocks;
    }
}


static HeapScanDesc heap_beginscan_internal(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
    uint32 flags, RangeScanInRedis rangeScanInRedis = {false, 0, 0});
static HeapTuple heap_prepare_insert(Relation relation, HeapTuple tup, CommandId cid, int options);
static XLogRecPtr log_heap_update(Relation reln, Buffer oldbuf, const ItemPointer from, Buffer newbuf, HeapTuple newtup,
                                  HeapTuple old_key_tup, bool all_visible_cleared, bool new_all_visible_cleared);
static void HeapSatisfiesHOTUpdate(Relation relation, Bitmapset *hot_attrs, Bitmapset *id_attrs, bool *satisfies_hot,
                                   bool *satisfies_id, HeapTuple oldtup, HeapTuple newtup, char *page);
static HeapTuple ExtractReplicaIdentity(Relation rel, HeapTuple tup, bool key_modified, bool *copy);
static void SkipToNewPage(HeapScanDesc scan, ScanDirection dir, BlockNumber page, bool &finished,
                          bool &is_valid_relation_page);
static bool VerifyHeapGetTup(HeapScanDesc scan, ScanDirection dir);
static XLogRecPtr log_heap_new_cid(Relation relation, HeapTuple tup);
extern void Start_Prefetch(TableScanDesc scan, SeqScanAccessor *pAccessor, ScanDirection dir);
extern void vacuum_set_xid_limits(Relation rel, int64 freeze_min_age, int64 freeze_table_age, TransactionId *oldestXmin,
                                  TransactionId *freezeLimit, TransactionId *freezeTableLimit);

/* ----------------
 *		initscan - scan code common to heap_beginscan and heap_rescan
 * ----------------
 */
static void initscan(HeapScanDesc scan, ScanKey key, bool is_rescan)
{
    bool allow_strat = false;
    bool allow_sync = false;

    RangeScanInRedis rangeScanInRedis = scan->rs_base.rs_rangeScanInRedis;

    InitScanBlocks(scan, rangeScanInRedis);

    /*
     * If the table is large relative to NBuffers, use a bulk-read access
     * strategy and enable synchronized scanning (see syncscan.c).	Although
     * the thresholds for these features could be different, we make them the
     * same so that there are only two behaviors to tune rather than four.
     * (However, some callers need to be able to disable one or both of these
     * behaviors, independently of the size of the table; also there is a GUC
     * variable that can disable synchronized scanning.)
     *
     * During a rescan, don't make a new strategy object if we don't have to.
     */
    if (scan->rs_base.rs_nblocks > (uint32)(g_instance.attr.attr_storage.NBuffers / 4)) {
        allow_strat = ((scan->rs_base.rs_flags & SO_ALLOW_STRAT) != 0);
        allow_sync = ((scan->rs_base.rs_flags & SO_ALLOW_SYNC) != 0);
    } else
        allow_strat = allow_sync = false;

    if (allow_strat) {
        if (scan->rs_base.rs_strategy == NULL)
            scan->rs_base.rs_strategy = GetAccessStrategy(BAS_BULKREAD);
    } else {
        if (scan->rs_base.rs_strategy != NULL)
            FreeAccessStrategy(scan->rs_base.rs_strategy);
        scan->rs_base.rs_strategy = NULL;
    }

    if (is_rescan) {
        /*
         * If rescan, keep the previous startblock setting so that rewinding a
         * cursor doesn't generate surprising results.  Reset the syncscan
         * setting, though.
         */
        scan->rs_base.rs_syncscan = (allow_sync && u_sess->attr.attr_storage.synchronize_seqscans);
    } else if (allow_sync && u_sess->attr.attr_storage.synchronize_seqscans) {
        scan->rs_base.rs_syncscan = true;
        scan->rs_base.rs_startblock = ss_get_location(scan->rs_base.rs_rd, scan->rs_base.rs_nblocks);
    } else {
        scan->rs_base.rs_syncscan = false;
        if (scan->rs_base.rs_nblocks == 0 || !rangeScanInRedis.isRangeScanInRedis) {
            scan->rs_base.rs_startblock = 0;
        }
    }

    scan->rs_base.rs_inited = false;
    scan->rs_ctup.t_data = NULL;
    ItemPointerSetInvalid(&scan->rs_ctup.t_self);
    scan->rs_base.rs_cbuf = InvalidBuffer;
    scan->rs_base.rs_cblock = InvalidBlockNumber;
    scan->rs_base.rs_ss_accessor = NULL;
    scan->dop = 1;

    /* we don't have a marked position... */
    ItemPointerSetInvalid(&(scan->rs_mctid));

    /* page-at-a-time fields are always invalid when not rs_inited
     *
     * copy the scan key, if appropriate
     */
    if (key != NULL && scan->rs_base.rs_nkeys > 0) {
        errno_t rc = EOK;
        rc = memcpy_s(scan->rs_base.rs_key, scan->rs_base.rs_nkeys * sizeof(ScanKeyData), key, scan->rs_base.rs_nkeys * sizeof(ScanKeyData));
        securec_check(rc, "\0", "\0");
    }

    /*
     * Currently, we don't have a stats counter for bitmap heap scans (but the
     * underlying bitmap index scans will be counted) or sample scans (we only
     * update stats for tuple fetches there).
     */
    if (((scan->rs_base.rs_flags & SO_TYPE_BITMAPSCAN) == 0) && ((scan->rs_base.rs_flags & SO_TYPE_SAMPLESCAN) == 0)) {
        pgstat_count_heap_scan(scan->rs_base.rs_rd);
    }
}

/*
 * heapgetpage - subroutine for heapgettup()
 *
 * This routine reads and pins the specified page of the relation.
 * In page-at-a-time mode it performs additional work, namely determining
 * which tuples on the page are visible.
 */
void heapgetpage(TableScanDesc sscan, BlockNumber page)
{
    Buffer buffer;
    Snapshot snapshot;
    Page dp;
    int lines;
    int ntup;
    OffsetNumber line_off;
    ItemId lpp;
    bool all_visible = false;

    HeapScanDesc scan = (HeapScanDesc) sscan;
    if (!scan->rs_base.rs_rangeScanInRedis.isRangeScanInRedis) {
        Assert(page < scan->rs_base.rs_nblocks);
    } else {
        Assert(page < scan->rs_base.rs_nblocks + scan->rs_base.rs_startblock);
    }

    /* release previous scan buffer, if any */
    if (BufferIsValid(scan->rs_base.rs_cbuf)) {
        ReleaseBuffer(scan->rs_base.rs_cbuf);
        scan->rs_base.rs_cbuf = InvalidBuffer;
    }

    /*
     * Be sure to check for interrupts at least once per page.	Checks at
     * higher code levels won't be able to stop a seqscan that encounters many
     * pages' worth of consecutive dead tuples.
     */
    CHECK_FOR_INTERRUPTS();

    /* read page using selected strategy */
    scan->rs_base.rs_cbuf = ReadBufferExtended(scan->rs_base.rs_rd, MAIN_FORKNUM, page, RBM_NORMAL, scan->rs_base.rs_strategy);
    scan->rs_base.rs_cblock = page;

    /* We've pinned the buffer, nobody can prune this buffer, check whether snapshot is valid. */
    CheckSnapshotIsValidException(scan->rs_base.rs_snapshot, "heapgetpage");

    if (!scan->rs_base.rs_pageatatime) {
        return;
    }

    buffer = scan->rs_base.rs_cbuf;
    snapshot = scan->rs_base.rs_snapshot;

    /*
     * Prune and repair fragmentation for the whole page, if possible.
     * No more page prune if it is a range scan during redistribution time
     * since we use append mode and never look back holes in previous pages
     * anyway.
     */
    if (!scan->rs_base.rs_rangeScanInRedis.isRangeScanInRedis) {
        heap_page_prune_opt(scan->rs_base.rs_rd, buffer);
    }

    /*
     * We must hold share lock on the buffer content while examining tuple
     * visibility.	Afterwards, however, the tuples we have found to be
     * visible are guaranteed good as long as we hold the buffer pin.
     */
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    dp = (Page)BufferGetPage(buffer);
    lines = PageGetMaxOffsetNumber(dp);
    ntup = 0;

    /*
     * If the all-visible flag indicates that all tuples on the page are
     * visible to everyone, we can skip the per-tuple visibility tests. But
     * not in hot standby mode. A tuple that's already visible to all
     * transactions in the master might still be invisible to a read-only
     * transaction in the standby.
     */
    all_visible = PageIsAllVisible(dp) && !snapshot->takenDuringRecovery;

    for (line_off = FirstOffsetNumber, lpp = PageGetItemId(dp, line_off); line_off <= lines; line_off++, lpp++) {
        if (ItemIdIsNormal(lpp)) {
            HeapTupleData loctup;
            bool valid = false;

            loctup.t_tableOid = RelationGetRelid(scan->rs_base.rs_rd);
            loctup.t_bucketId = RelationGetBktid(scan->rs_base.rs_rd);
            loctup.t_data = (HeapTupleHeader)PageGetItem((Page)dp, lpp);
            loctup.t_len = ItemIdGetLength(lpp);
            HeapTupleCopyBaseFromPage(&loctup, dp);
            ItemPointerSet(&(loctup.t_self), page, line_off);

            if (all_visible)
                valid = true;
            else
                valid = HeapTupleSatisfiesVisibility(&loctup, snapshot, buffer);

            CheckForSerializableConflictOut(valid, scan->rs_base.rs_rd, &loctup, buffer, snapshot);

            if (valid) {
                scan->rs_base.rs_vistuples[ntup++] = line_off;
            }

            ereport(DEBUG1, (errmsg("heapgetpage xid %lu ctid(%u,%d) valid %d", GetCurrentTransactionIdIfAny(), page,
                                    line_off, valid)));
        }
    }

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    Assert(ntup <= MaxHeapTuplesPerPage);
    scan->rs_base.rs_ntuples = ntup;
}

/*
 * @Description: if many tuples of the relation are deleted, when load a one page which has normal tuples, so need
 * prefetch
 * @Param[IN] dir: scan direction
 * @Param[IN] scan:  heap scan desc
 * @See also: heapgettup(); heapgettup_pagemode()
 */
void heap_prefetch(HeapScanDesc scan, ScanDirection dir)
{
    ADIO_RUN()
    {
        /* if tuples in page are all deleted, need prefetch also for performance */
        if (scan->rs_base.rs_ss_accessor != NULL) {
            Start_Prefetch((TableScanDesc)scan, scan->rs_base.rs_ss_accessor, dir);
        }
    }
    ADIO_END();
}

/*
 * @Description: Calculate the next page number.
 *
 * @param[IN] scan: heap scan describtion.
 * @param[IN] dir: scan direction.
 * @param[OUT] page: next page number.
 * @return bool: true -- scan finished.
 */
FORCE_INLINE
bool next_page(HeapScanDesc scan, ScanDirection dir, BlockNumber &page)
{
    bool finished = false;
    if (scan->dop > 1) {
        if (BackwardScanDirection == dir) {
            finished = (page == 0);
            if (finished)
                return finished;
            page--;
            if ((scan->rs_base.rs_startblock - page) % PARALLEL_SCAN_GAP == 0) {
                page -= (scan->dop - 1) * PARALLEL_SCAN_GAP;
            }
        } else {
            page++;
            if ((page - scan->rs_base.rs_startblock) % PARALLEL_SCAN_GAP == 0) {
                page += (scan->dop - 1) * PARALLEL_SCAN_GAP;
            }

            if (scan->rs_base.rs_rangeScanInRedis.isRangeScanInRedis) {
                /* Parallel workers start from different point. */
                finished =
                    (page >= scan->rs_base.rs_startblock + scan->rs_base.rs_nblocks - PARALLEL_SCAN_GAP * u_sess->stream_cxt.smp_id);
            } else {
                finished = (page >= scan->rs_base.rs_nblocks);
            }
        }
    } else {
        if (BackwardScanDirection == dir) {
            finished = (scan->rs_base.rs_startblock == page);
            if (page == 0) {
                page = scan->rs_base.rs_nblocks;
            }
            page--;
        } else {
            page++;

            if (scan->rs_base.rs_rangeScanInRedis.isRangeScanInRedis) {
                if (page >= scan->rs_base.rs_startblock + scan->rs_base.rs_nblocks) {
                    page = 0;
                }
                finished = (page == 0);
            } else {
                if (page >= scan->rs_base.rs_nblocks) {
                    page = 0;
                }
                finished = (page == scan->rs_base.rs_startblock);
            }
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
            if (scan->rs_base.rs_syncscan) {
                ss_report_location(scan->rs_base.rs_rd, page);
            }
        }
    }

    return finished;
}

/*
 * SkipToNewPage
 *
 * @Description: to get next page. If the data page is corrupted, we wil find the next page and
 *               the data page will be checked. when we find the normal data page or scan is end
 *               the function will return.
 * @in scan - the relation's heap scan description.
 * @in dir - the scan direction, The default scan is ForwardScanDirection.
 * @in&out page - the relation's current page
 * @in&out finished - judge the scan is in the end.
 * @in&out is_valid_relation_page - relation's page is valid return true, else return false.
 * @return: bool-- true is scan finished. Otherwise, return false.
 */
static void SkipToNewPage(HeapScanDesc scan, ScanDirection dir, BlockNumber page, bool &finished,
                          bool &is_valid_relation_page)
{
    MemoryContext verify_context = CurrentMemoryContext;
    HeapTuple tuple = &(scan->rs_ctup);
    bool try_next_page = false;

    while (!finished) {
        /* advance to next/prior page and detect end of scan */
        finished = next_page(scan, dir, page);

        /* try_next_page is used to judge whether we need to continue. */
        try_next_page = false;

        /* return NULL if we've exhausted all the pages. */
        if (finished) {
            if (BufferIsValid(scan->rs_base.rs_cbuf)) {
                ReleaseBuffer(scan->rs_base.rs_cbuf);
            }
            scan->rs_base.rs_cbuf = InvalidBuffer;
            scan->rs_base.rs_cblock = InvalidBlockNumber;
            tuple->t_data = NULL;
            scan->rs_base.rs_inited = false;
            return;
        }

        heap_prefetch(scan, dir);
        PG_TRY();
        {
            heapgetpage((TableScanDesc)scan, page);
        }
        PG_CATCH();
        {
            (void)MemoryContextSwitchTo(verify_context);
            is_valid_relation_page = false;
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
            try_next_page = true;
        }
        PG_END_TRY();
        if (try_next_page) {
            continue;
        }
        return;
    }
    return;
}

/*
 * VerifyHeapGetTup
 *
 * @Description: fetch next heap tuple. The main function is same to the function heapgettup, but this batch
 *               will catch all the error and print the warning and also we must deal with IO\buffer\lock
 *               exception so that we can continue to check other tuple or page. When the page corrupts, we
 *               think the page is broken so we need skip this page. We need to clean up the environment and
 *               skip this page to go into the next page. If the tuple corrupts, we need to judge the tuple
 *               corrupts or the tuple cannot be read. If tuple corrupts,we need to clean the tuple related
 *               data and skip to next tuple. If tuple cannot be read, we think the page is broken and we need
 *               to check the next page.
 * @in scan - the relation's heap scan description.
 * @in dir - the scan direction, The default scan is ForwardScanDirection.
 * @in&out page - the relation's current page
 * @in&out finished - judge the scan is in the end.
 * @return: bool
 */
static bool VerifyHeapGetTup(HeapScanDesc scan, ScanDirection dir)
{
    HeapTuple tuple = &(scan->rs_ctup);
    Snapshot snapshot = scan->rs_base.rs_snapshot;
    BlockNumber page = InvalidBlockNumber;
    bool finished = false;
    Page dp = NULL;
    int lines = 0;
    OffsetNumber line_off = InvalidOffsetNumber;
    int lines_left = 0;
    ItemId lpp;
    bool is_valid_relation_page = true;
    MemoryContext verify_context = CurrentMemoryContext;

    /*
     * calculate next starting line_off, given scan direction
     */
    Assert(ScanDirectionIsForward(dir));
    if (!scan->rs_base.rs_inited) {
        /* return null immediately if relation is empty */
        if (scan->rs_base.rs_nblocks == 0) {
            Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
            tuple->t_data = NULL;
            return is_valid_relation_page;
        }

        /* first page and first offnum */
        page = scan->rs_base.rs_startblock;
        scan->rs_base.rs_cblock = page;
        line_off = FirstOffsetNumber;
        scan->rs_base.rs_inited = true;
        PG_TRY();
        {
            heapgetpage((TableScanDesc)scan, page);
        }
        PG_CATCH();
        {
            (void)MemoryContextSwitchTo(verify_context);
            is_valid_relation_page = false;
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

            SkipToNewPage(scan, dir, scan->rs_base.rs_cblock, finished, is_valid_relation_page);
        }
        PG_END_TRY();
        if (finished) {
            return is_valid_relation_page;
        }
    } else {
        /* continue from previously returned page/tuple */
        /* page is the current page and line_off is the next offnum */
        page = scan->rs_base.rs_cblock;
        line_off = OffsetNumberNext(ItemPointerGetOffsetNumber(&(tuple->t_self)));
    }

    LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_SHARE);

    dp = (Page)BufferGetPage(scan->rs_base.rs_cbuf);
    lines = PageGetMaxOffsetNumber(dp);
    /* page and line_off now reference the physically next tid */
    lines_left = lines - line_off + 1;

    /* advance the scan until we find a qualifying tuple or run out of stuff to scan */
    lpp = PageGetItemId(dp, line_off);
    for (;;) {
        while (lines_left > 0) {
            if (ItemIdIsNormal(lpp)) {
                bool valid = false;
                tuple->t_data = (HeapTupleHeader)PageGetItem((Page)dp, lpp);
                tuple->t_len = ItemIdGetLength(lpp);
                ItemPointerSet(&(tuple->t_self), page, line_off);
                HeapTupleCopyBaseFromPage(tuple, dp);

                /* if current tuple qualifies, return it. */
                valid = HeapTupleSatisfiesVisibility(tuple, snapshot, scan->rs_base.rs_cbuf);

                CheckForSerializableConflictOut(valid, scan->rs_base.rs_rd, tuple, scan->rs_base.rs_cbuf, snapshot);

                if (valid) {
                    /* make sure this tuple is visible and then uncompress it */
                    DECOMPRESS_HEAP_TUPLE(HEAP_TUPLE_IS_COMPRESSED(tuple->t_data), tuple, &(scan->rs_ctbuf_hdr),
                                          (scan->rs_tupdesc), dp);
                }

                if (valid) {
                    LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_UNLOCK);
                    return is_valid_relation_page;
                }
            }

            /* otherwise move to the next item on the page */
            --lines_left;

            /* move forward in this page's ItemId array */
            ++lpp;
            ++line_off;
        }

        /* if we get here, it means we've exhausted the items on this page and it's time to move to the next. */
        LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_UNLOCK);
        page = scan->rs_base.rs_cblock;
        SkipToNewPage(scan, dir, page, finished, is_valid_relation_page);
        if (finished) {
            return is_valid_relation_page;
        }

        LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_SHARE);

        dp = (Page)BufferGetPage(scan->rs_base.rs_cbuf);
        lines = PageGetMaxOffsetNumber((Page)dp);
        lines_left = lines;
        line_off = FirstOffsetNumber;
        lpp = PageGetItemId(dp, FirstOffsetNumber);
    }
}

/* ----------------
 *		heapgettup - fetch next heap tuple
 *
 *		Initialize the scan if not already done; then advance to the next
 *		tuple as indicated by "dir"; return the next tuple in scan->rs_ctup,
 *		or set scan->rs_ctup.t_data = NULL if no more tuples.
 *
 * dir == NoMovementScanDirection means "re-fetch the tuple indicated
 * by scan->rs_ctup".
 *
 * Note: the reason nkeys/key are passed separately, even though they are
 * kept in the scan descriptor, is that the caller may not want us to check
 * the scankeys.
 *
 * Note: when we fall off the end of the scan in either direction, we
 * reset rs_inited.  This means that a further request with the same
 * scan direction will restart the scan, which is a bit odd, but a
 * request with the opposite scan direction will start a fresh scan
 * in the proper direction.  The latter is required behavior for cursors,
 * while the former case is generally undefined behavior in Postgres
 * so we don't care too much.
 * ----------------
 */
static void heapgettup(HeapScanDesc scan, ScanDirection dir, int nkeys, ScanKey key)
{
    HeapTuple tuple = &(scan->rs_ctup);
    Snapshot snapshot = scan->rs_base.rs_snapshot;
    bool backward = ScanDirectionIsBackward(dir);
    BlockNumber page;
    bool finished = false;
    Page dp;
    int lines;
    OffsetNumber line_off;
    int lines_left;
    ItemId lpp;

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
                tuple->t_data = NULL;
                return;
            }
            page = scan->rs_base.rs_startblock; /* first page */
            heapgetpage((TableScanDesc)scan, page);
            line_off = FirstOffsetNumber; /* first offnum */
            scan->rs_base.rs_inited = true;
        } else {
            /* continue from previously returned page/tuple */
            page = scan->rs_base.rs_cblock; /* current page */
            line_off = OffsetNumberNext(ItemPointerGetOffsetNumber(&(tuple->t_self))); /* next offnum */
        }

        LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_SHARE);

        dp = (Page)BufferGetPage(scan->rs_base.rs_cbuf);
        lines = PageGetMaxOffsetNumber(dp);
        /* page and line_off now reference the physically next tid */
        lines_left = lines - line_off + 1;
    } else if (backward) {
        if (!scan->rs_base.rs_inited) {
            /* return null immediately if relation is empty */
            if (scan->rs_base.rs_nblocks == 0) {
                Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
                tuple->t_data = NULL;
                return;
            }

            /*
             * Disable reporting to syncscan logic in a backwards scan; it's
             * not very likely anyone else is doing the same thing at the same
             * time, and much more likely that we'll just bollix things for
             * forward scanners.
             */
            scan->rs_base.rs_syncscan = false;
            /* start from last page of the scan */
            if (scan->rs_base.rs_startblock > 0)
                page = scan->rs_base.rs_startblock - 1;
            else
                page = scan->rs_base.rs_nblocks - 1;
            heapgetpage((TableScanDesc)scan, page);
        } else {
            /* continue from previously returned page/tuple */
            page = scan->rs_base.rs_cblock; /* current page */
        }

        LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_SHARE);

        dp = (Page)BufferGetPage(scan->rs_base.rs_cbuf);
        lines = PageGetMaxOffsetNumber(dp);

        if (!scan->rs_base.rs_inited) {
            line_off = lines; /* final offnum */
            scan->rs_base.rs_inited = true;
        } else {
            line_off = /* previous offnum */
                OffsetNumberPrev(ItemPointerGetOffsetNumber(&(tuple->t_self)));
        }
        /* page and line_off now reference the physically previous tid */
        lines_left = line_off;
    } else {
        /* ''no movement'' scan direction: refetch prior tuple */
        if (!scan->rs_base.rs_inited) {
            Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
            tuple->t_data = NULL;
            return;
        }

        page = ItemPointerGetBlockNumber(&(tuple->t_self));
        if (page != scan->rs_base.rs_cblock)
            heapgetpage((TableScanDesc)scan, page);

        /* Since the tuple was previously fetched, needn't lock page here */
        dp = (Page)BufferGetPage(scan->rs_base.rs_cbuf);
        line_off = ItemPointerGetOffsetNumber(&(tuple->t_self));

        /* Prevent concurrent page upgrades */
        bool is_lock = false;
        if (PageIs4BXidVersion(dp)) {
            LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_SHARE);
            is_lock = true;
        }

        lpp = PageGetItemId(dp, line_off);
        Assert(ItemIdIsNormal(lpp));
        tuple->t_data = (HeapTupleHeader)PageGetItem((Page)dp, lpp);
        tuple->t_len = ItemIdGetLength(lpp);

        HeapTupleCopyBaseFromPage(tuple, dp);
        if (is_lock) {
            LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_UNLOCK);
            is_lock = false;
        }

        DECOMPRESS_HEAP_TUPLE(HEAP_TUPLE_IS_COMPRESSED(tuple->t_data), tuple, &(scan->rs_ctbuf_hdr), (scan->rs_tupdesc),
                              dp);
        return;
    }

    /*
     * advance the scan until we find a qualifying tuple or run out of stuff
     * to scan
     */
    lpp = PageGetItemId(dp, line_off);
    for (;;) {
        while (lines_left > 0) {
            if (ItemIdIsNormal(lpp)) {
                bool valid = false;

                tuple->t_data = (HeapTupleHeader)PageGetItem((Page)dp, lpp);
                tuple->t_len = ItemIdGetLength(lpp);
                ItemPointerSet(&(tuple->t_self), page, line_off);
                HeapTupleCopyBaseFromPage(tuple, dp);

                /*
                 * if current tuple qualifies, return it.
                 */
                valid = HeapTupleSatisfiesVisibility(tuple, snapshot, scan->rs_base.rs_cbuf);

                CheckForSerializableConflictOut(valid, scan->rs_base.rs_rd, tuple, scan->rs_base.rs_cbuf, snapshot);
                if (valid) {
                    /* make sure this tuple is visible and then uncompress it */
                    DECOMPRESS_HEAP_TUPLE(HEAP_TUPLE_IS_COMPRESSED(tuple->t_data), tuple, &(scan->rs_ctbuf_hdr),
                                          (scan->rs_tupdesc), dp);

                    if (key != NULL) {
                        HeapKeyTest(tuple, (scan->rs_tupdesc), nkeys, key, valid);
                    }
                }

                if (valid) {
                    LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_UNLOCK);
                    return;
                }
            }
            /*
             * otherwise move to the next item on the page
             */
            --lines_left;
            if (backward) {
                --lpp; /* move back in this page's ItemId array */
                --line_off;
            } else {
                ++lpp; /* move forward in this page's ItemId array */
                ++line_off;
            }
        }

        /*
         * if we get here, it means we've exhausted the items on this page and
         * it's time to move to the next.
         */
        LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_UNLOCK);

        /*
         * advance to next/prior page and detect end of scan
         */
        finished = next_page(scan, dir, page);

        /*
         * return NULL if we've exhausted all the pages
         */
        if (finished) {
            if (BufferIsValid(scan->rs_base.rs_cbuf)) {
                ReleaseBuffer(scan->rs_base.rs_cbuf);
            }
            scan->rs_base.rs_cbuf = InvalidBuffer;
            scan->rs_base.rs_cblock = InvalidBlockNumber;
            tuple->t_data = NULL;
            scan->rs_base.rs_inited = false;
            return;
        }

        heap_prefetch(scan, dir);
        heapgetpage((TableScanDesc)scan, page);

        LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_SHARE);

        dp = (Page)BufferGetPage(scan->rs_base.rs_cbuf);
        lines = PageGetMaxOffsetNumber((Page)dp);
        lines_left = lines;
        if (backward) {
            line_off = lines;
            lpp = PageGetItemId(dp, lines);
        } else {
            line_off = FirstOffsetNumber;
            lpp = PageGetItemId(dp, FirstOffsetNumber);
        }
    }
}

/* ----------------
 *		heapgettup_pagemode - fetch next heap tuple in page-at-a-time mode
 *
 *		Same API as heapgettup, but used in page-at-a-time mode
 *
 * The internal logic is much the same as heapgettup's too, but there are some
 * differences: we do not take the buffer content lock (that only needs to
 * happen inside heapgetpage), and we iterate through just the tuples listed
 * in rs_vistuples[] rather than all tuples on the page.  Notice that
 * line_index is 0-based, where the corresponding loop variable line_off in
 * heapgettup is 1-based.
 * ----------------
 */
static void heapgettup_pagemode(HeapScanDesc scan, ScanDirection dir, int nkeys, ScanKey key)
{
    HeapTuple tuple = &(scan->rs_ctup);
    bool backward = ScanDirectionIsBackward(dir);
    bool is_range_scan_in_redis = scan->rs_base.rs_rangeScanInRedis.isRangeScanInRedis;
    BlockNumber page;
    bool finished = false;
    Page dp;
    int lines;
    int line_index;
    OffsetNumber line_off;
    int lines_left;
    ItemId lpp;

    /* IO collector and IO scheduler for seqsan */
    if (ENABLE_WORKLOAD_CONTROL) {
        IOSchedulerAndUpdate(IO_TYPE_READ, 1, IO_TYPE_ROW);
    }

    /*
     * calculate next starting line_index, given scan direction
     */
    if (ScanDirectionIsForward(dir) || is_range_scan_in_redis) {
        if (!scan->rs_base.rs_inited) {
            /*
             * return null immediately if relation is empty
             */
            if (scan->rs_base.rs_nblocks == 0) {
                Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
                tuple->t_data = NULL;
                return;
            }
            page = scan->rs_base.rs_startblock; /* first page */
            heapgetpage((TableScanDesc)scan, page);
            line_index = 0;
            scan->rs_base.rs_inited = true;
        } else {
            /* continue from previously returned page/tuple */
            page = scan->rs_base.rs_cblock; /* current page */
            line_index = scan->rs_base.rs_cindex + 1;
        }

        dp = (Page)BufferGetPage(scan->rs_base.rs_cbuf);
        lines = scan->rs_base.rs_ntuples;
        /* page and line_index now reference the next visible tid */
        lines_left = lines - line_index;
    } else if (backward) {
        if (!scan->rs_base.rs_inited) {
            /* return null immediately if relation is empty */
            if (scan->rs_base.rs_nblocks == 0) {
                Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
                tuple->t_data = NULL;
                return;
            }

            /*
             * Disable reporting to syncscan logic in a backwards scan; it's
             * not very likely anyone else is doing the same thing at the same
             * time, and much more likely that we'll just bollix things for
             * forward scanners.
             */
            scan->rs_base.rs_syncscan = false;
            /* start from last page of the scan */
            if (scan->rs_base.rs_startblock > 0) {
                page = scan->rs_base.rs_startblock - 1;
            } else {
                page = scan->rs_base.rs_nblocks - 1;
            }
            heapgetpage((TableScanDesc)scan, page);
        } else {
            /* continue from previously returned page/tuple */
            page = scan->rs_base.rs_cblock; /* current page */
        }

        dp = (Page)BufferGetPage(scan->rs_base.rs_cbuf);
        lines = scan->rs_base.rs_ntuples;

        if (!scan->rs_base.rs_inited) {
            line_index = lines - 1;
            scan->rs_base.rs_inited = true;
        } else {
            line_index = scan->rs_base.rs_cindex - 1;
        }
        /* page and line_index now reference the previous visible tid */
        lines_left = line_index + 1;
    } else {
        /* ''no movement'' scan direction: refetch prior tuple */
        if (!scan->rs_base.rs_inited) {
            Assert(!BufferIsValid(scan->rs_base.rs_cbuf));
            tuple->t_data = NULL;
            return;
        }

        page = ItemPointerGetBlockNumber(&(tuple->t_self));
        if (page != scan->rs_base.rs_cblock) {
            heapgetpage((TableScanDesc)scan, page);
        }

        /* Since the tuple was previously fetched, needn't lock page here */
        dp = (Page)BufferGetPage(scan->rs_base.rs_cbuf);
        line_off = ItemPointerGetOffsetNumber(&(tuple->t_self));

        /* Prevent concurrent page upgrades */
        bool is_lock = false;
        if (PageIs4BXidVersion(dp)) {
            LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_SHARE);
            is_lock = true;
        }
        lpp = PageGetItemId(dp, line_off);
        Assert(ItemIdIsNormal(lpp));

        tuple->t_data = (HeapTupleHeader)PageGetItem((Page)dp, lpp);
        tuple->t_len = ItemIdGetLength(lpp);

        HeapTupleCopyBaseFromPage(tuple, dp);

        if (is_lock) {
            LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_UNLOCK);
            is_lock = false;
        }
        /* check that rs_cindex is in sync */
        Assert(scan->rs_base.rs_cindex < scan->rs_base.rs_ntuples);
        Assert(line_off == scan->rs_base.rs_vistuples[scan->rs_base.rs_cindex]);

        DECOMPRESS_HEAP_TUPLE(HEAP_TUPLE_IS_COMPRESSED(tuple->t_data), tuple, &(scan->rs_ctbuf_hdr), (scan->rs_tupdesc),
                              dp);
        return;
    }

    /*
     * advance the scan until we find a qualifying tuple or run out of stuff
     * to scan
     */
    bool is_lock = false;
    for (;;) {
        /* Prevent concurrent page upgrades */
        if (PageIs4BXidVersion(dp) && is_lock == false) {
            LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_SHARE);
            is_lock = true;
        }

        while (lines_left > 0) {
            line_off = scan->rs_base.rs_vistuples[line_index];
            lpp = PageGetItemId(dp, line_off);
            Assert(ItemIdIsNormal(lpp));

            tuple->t_data = (HeapTupleHeader)PageGetItem((Page)dp, lpp);
            tuple->t_len = ItemIdGetLength(lpp);
            ItemPointerSet(&(tuple->t_self), page, line_off);
            HeapTupleCopyBaseFromPage(tuple, dp);

            /*
             * if the tuple is compressed, uncompress it first, because
             * 1. reduce the UNCOMPRESS number within HeapKeyTest();
             * 2. maybe reduce the number of palloc() within HeapKeyTest();
             */
            DECOMPRESS_HEAP_TUPLE(HEAP_TUPLE_IS_COMPRESSED(tuple->t_data), tuple, &(scan->rs_ctbuf_hdr),
                                  (scan->rs_tupdesc), dp);

            /*
             * if current tuple qualifies, return it.
             */
            if (key != NULL) {
                bool valid = false;

                HeapKeyTest(tuple, (scan->rs_tupdesc), nkeys, key, valid);

                if (valid) {
                    scan->rs_base.rs_cindex = line_index;
                    if (is_lock) {
                        LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_UNLOCK);
                        is_lock = false;
                    }
                    return;
                }
            } else {
                scan->rs_base.rs_cindex = line_index;
                if (is_lock) {
                    LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_UNLOCK);
                    is_lock = false;
                }
                return;
            }

            /*
             * otherwise move to the next item on the page
             */
            --lines_left;
            if (backward) {
                --line_index;
            } else {
                ++line_index;
            }
        }
        if (is_lock) {
            LockBuffer(scan->rs_base.rs_cbuf, BUFFER_LOCK_UNLOCK);
            is_lock = false;
        }
        /*
         * if we get here, it means we've exhausted the items on this page and
         * it's time to move to the next.
         */
        finished = next_page(scan, dir, page);
        /*
         * return NULL if we've exhausted all the pages
         */
        if (finished) {
            if (BufferIsValid(scan->rs_base.rs_cbuf)) {
                ReleaseBuffer(scan->rs_base.rs_cbuf);
            }
            scan->rs_base.rs_cbuf = InvalidBuffer;
            scan->rs_base.rs_cblock = InvalidBlockNumber;
            tuple->t_data = NULL;
            scan->rs_base.rs_inited = false;
            return;
        }

        heap_prefetch(scan, dir);
        heapgetpage((TableScanDesc)scan, page);

        dp = (Page)BufferGetPage(scan->rs_base.rs_cbuf);
        lines = scan->rs_base.rs_ntuples;
        lines_left = lines;
        if (backward) {
            line_index = lines - 1;
        } else {
            line_index = 0;
        }
    }
}

#if defined(DISABLE_COMPLEX_MACRO)
/*
 * This is formatted so oddly so that the correspondence to the macro
 * definition in access/htup.h is maintained.
 */
Datum fastgetattr(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull)
{
    /* make sure this tuple is not row-compressed.
     * otherwise, please call fastgetattr_with_dict().
     */
    Assert(!HEAP_TUPLE_IS_COMPRESSED(tup->t_data));
    if (attnum <= 0) {
        return (Datum)NULL;
    }

    *isnull = false;
    if (HeapTupleNoNulls(tup)) {
        if (tupleDesc->attrs[attnum - 1]->attcacheoff >= 0) {
            return fetchatt(tupleDesc->attrs[attnum - 1],
                            (char *)tup->t_data + tup->t_data->t_hoff + tupleDesc->attrs[attnum - 1]->attcacheoff);
        }
        return nocachegetattr(tup, attnum, tupleDesc);
    } else {
        if (att_isnull(attnum - 1, tup->t_data->t_bits)) {
            *isnull = true;
            return (Datum)NULL;
        }
        return nocachegetattr(tup, attnum, tupleDesc);
    }
}

Datum fastgetattr_with_dict(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull, char *pageDict)
{
    Assert(HEAP_TUPLE_IS_COMPRESSED(tup->t_data));
    Assert(attnum > 0);
    *isnull = false;

    /* case 1: this tuple has nulls, and the attnum's bit is set */
    if (HeapTupleHasNulls(tup) && att_isnull((attnum)-1, (tup)->t_data->t_bits)) {
        *(isnull) = true;
        return (Datum)NULL;
    }

    /* case 2: this tuple is compressed and has no nulls */
    return nocache_cmprs_get_attr(tup, attnum, tupleDesc, pageDict);
}

#endif /* defined(DISABLE_COMPLEX_MACRO) */

/* ----------------
 *		relation_open - open any relation by relation OID
 *
 *		If lockmode is not "NoLock", the specified kind of lock is
 *		obtained on the relation.  (Generally, NoLock should only be
 *		used if the caller knows it has some appropriate lock on the
 *		relation already.)
 *
 *		An error is raised if the relation does not exist.
 *
 *		NB: a "relation" is anything with a pg_class entry.  The caller is
 *		expected to check whether the relkind is something it can handle.
 * ----------------
 */
Relation relation_open(Oid relationId, LOCKMODE lockmode, int2 bucketId)
{
    Relation r;

    Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);
    if (IsAbortedTransactionBlockState()) {
        force_backtrace_messages = true;
        ereport(ERROR,
            (errcode(ERRCODE_RELATION_OPEN_ERROR),
                errmsg("current transaction is aborted, "
                    "commands ignored until end of transaction block, firstChar[%c]",
                    u_sess->proc_cxt.firstChar)));
    }

    /* Get the lock before trying to open the relcache entry */
    if (lockmode != NoLock) {
        LockRelationOid(relationId, lockmode);
    }

    /* The relcache does all the real work... */
    r = RelationIdGetRelation(relationId);
    if (!RelationIsValid(r)) {
        force_backtrace_messages = true;
        ereport(ERROR,
                (errcode(ERRCODE_RELATION_OPEN_ERROR), errmsg("could not open relation with OID %u", relationId)));
    }

    /* Make note that we've accessed a temporary relation */
    if (RelationUsesLocalBuffers(r)) {
        t_thrd.xact_cxt.MyXactAccessedTempRel = true;
    }
    /* Make note that we've accessed a repliacted relation */
    if (r->rd_locator_info != NULL && IsRelationReplicated(r->rd_locator_info)) {
        t_thrd.xact_cxt.MyXactAccessedRepRel = true;
    }
    pgstat_initstats(r);

    if (bucketId != InvalidBktId) {
        Assert(RELATION_OWN_BUCKET(r));
        r = bucketGetRelation(r, NULL, bucketId);
    }
    return r;
}

/* ----------------
 *		try_relation_open - open any relation by relation OID
 *
 *		Same as relation_open, except return NULL instead of failing
 *		if the relation does not exist.
 * ----------------
 */
Relation try_relation_open(Oid relationId, LOCKMODE lockmode)
{
    Relation r;

    Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

    /* Get the lock first */
    if (lockmode != NoLock) {
        LockRelationOid(relationId, lockmode);
    }

    /*
     * Now that we have the lock, probe to see if the relation really exists
     * or not.
     */
    if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relationId))) {
        /* Release useless lock */
        if (lockmode != NoLock) {
            UnlockRelationOid(relationId, lockmode);
        }

        return NULL;
    }

    /* Should be safe to do a relcache load */
    r = RelationIdGetRelation(relationId);
    if (!RelationIsValid(r)) {
        ereport(ERROR,
                (errcode(ERRCODE_RELATION_OPEN_ERROR), errmsg("could not open relation with OID %u", relationId)));
    }

    /* Make note that we've accessed a temporary relation */
    if (RelationUsesLocalBuffers(r)) {
        t_thrd.xact_cxt.MyXactAccessedTempRel = true;
    }
    /* Make note that we've accessed a repliacted relation */
    if (r->rd_locator_info != NULL && IsRelationReplicated(r->rd_locator_info)) {
        t_thrd.xact_cxt.MyXactAccessedRepRel = true;
    }
    pgstat_initstats(r);

    return r;
}

/* ----------------
 *		relation_openrv - open any relation specified by a RangeVar
 *
 *		Same as relation_open, but the relation is specified by a RangeVar.
 * ----------------
 */
Relation relation_openrv(const RangeVar *relation, LOCKMODE lockmode)
{
    Oid relOid;

    /*
     * Check for shared-cache-inval messages before trying to open the
     * relation.  This is needed even if we already hold a lock on the
     * relation, because GRANT/REVOKE are executed without taking any lock on
     * the target relation, and we want to be sure we see current ACL
     * information.  We can skip this if asked for NoLock, on the assumption
     * that such a call is not the first one in the current command, and so we
     * should be reasonably up-to-date already.  (XXX this all could stand to
     * be redesigned, but for the moment we'll keep doing this like it's been
     * done historically.)
     */
    if (lockmode != NoLock) {
        AcceptInvalidationMessages();
    }

    /* Look up and lock the appropriate relation using namespace search */
    relOid = RangeVarGetRelid(relation, lockmode, false);

    /* Let relation_open do the rest */
    return relation_open(relOid, NoLock);
}

/* ----------------
 *		relation_openrv_extended - open any relation specified by a RangeVar
 *
 *		Same as relation_openrv, but with an additional missing_ok argument
 *		allowing a NULL return rather than an error if the relation is not
 *		found.	(Note that some other causes, such as permissions problems,
 *		will still result in an ereport.)
 * ----------------
 */
Relation relation_openrv_extended(const RangeVar *relation, LOCKMODE lockmode, bool missing_ok, bool isSupportSynonym,
                                  StringInfo detailInfo)
{
    Oid relOid;
    Oid refSynOid = InvalidOid;
    Relation rel;

    /*
     * Check for shared-cache-inval messages before trying to open the
     * relation.  See comments in relation_openrv().
     */
    if (lockmode != NoLock) {
        AcceptInvalidationMessages();
    }

    /* Look up and lock the appropriate relation using namespace search */
    relOid = RangeVarGetRelidExtended(relation, lockmode, missing_ok, false, false, isSupportSynonym, NULL, NULL,
                                      detailInfo, &refSynOid);
    /* Return NULL on not-found */
    if (!OidIsValid(relOid)) {
        return NULL;
    }

    /* Let relation_open do the rest */
    rel = relation_open(relOid, NoLock);
    /* Record the refSynOid into RelationData, if exists. */
    rel->rd_refSynOid = refSynOid;

    return rel;
}

/* ----------------
 *		relation_close - close any relation
 *
 *		If lockmode is not "NoLock", we then release the specified lock.
 *
 *		Note that it is often sensible to hold a lock beyond relation_close;
 *		in that case, the lock is released automatically at xact end.
 * ----------------
 */
void relation_close(Relation relation, LOCKMODE lockmode)
{
    Relation rel = relation;
    LockRelId relid;

    if (RelationIsBucket(relation)) {
        rel = relation->parent;
        Assert(RELATION_OWN_BUCKET(rel));
        bucketCloseRelation(relation);
    }

    Assert(PointerIsValid(rel));
    Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

    relid = rel->rd_lockInfo.lockRelId;

    /* The relcache does the real work... */
    RelationClose(rel);

    if (lockmode != NoLock) {
        UnlockRelationId(&relid, lockmode);
    }
}

/* ----------------
 *		heap_open - open a heap relation by relation OID
 *
 *		This is essentially relation_open plus check that the relation
 *		is not an index nor a composite type.  (The caller should also
 *		check that it's not a view or foreign table before assuming it has
 *		storage.)
 * ----------------
 */
Relation heap_open(Oid relationId, LOCKMODE lockmode, int2 bucketid)
{
    Relation r;

    r = relation_open(relationId, lockmode, bucketid);
    if (RelationIsIndex(r)) {
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is an index", RelationGetRelationName(r))));
    } else if (r->rd_rel->relkind == RELKIND_COMPOSITE_TYPE) {
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is a composite type", RelationGetRelationName(r))));
    }

    return r;
}

/* ----------------
 *		heap_openrv - open a heap relation specified
 *		by a RangeVar node
 *
 *		As above, but relation is specified by a RangeVar.
 * ----------------
 */
Relation heap_openrv(const RangeVar *relation, LOCKMODE lockmode)
{
    Relation r;

    r = relation_openrv(relation, lockmode);
    if (RelationIsIndex(r)) {
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is an index", RelationGetRelationName(r))));
    } else if (r->rd_rel->relkind == RELKIND_COMPOSITE_TYPE) {
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is a composite type", RelationGetRelationName(r))));
    }

    return r;
}

/* ----------------
 *		heap_openrv_extended - open a heap relation specified
 *		by a RangeVar node
 *
 *		As above, but optionally return NULL instead of failing for
 *		relation-not-found.
 * ----------------
 */
Relation heap_openrv_extended(const RangeVar *relation, LOCKMODE lockmode, bool missing_ok, bool isSupportSynonym,
                              StringInfo detailInfo)
{
    Relation r = NULL;

    r = relation_openrv_extended(relation, lockmode, missing_ok, isSupportSynonym, detailInfo);
    if (r) {
        if (isSupportSynonym && detailInfo != NULL && detailInfo->len > 0) {
            /* If has some error detail infos, report it. */
            if (RelationIsIndex(r)) {
                ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is an index", RelationGetRelationName(r)),
                         errdetail("%s", detailInfo->data)));
            } else if (r->rd_rel->relkind == RELKIND_COMPOSITE_TYPE) {
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                                errmsg("\"%s\" is a composite type", RelationGetRelationName(r)),
                                errdetail("%s", detailInfo->data)));
            }
        } else {
            if (RelationIsIndex(r)) {
                ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is an index", RelationGetRelationName(r))));
            } else if (r->rd_rel->relkind == RELKIND_COMPOSITE_TYPE) {
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                                errmsg("\"%s\" is a composite type", RelationGetRelationName(r))));
            }
        }
    }

    return r;
}

/* ----------------
 *		heap_beginscan	- begin relation scan
 *
 * heap_beginscan_strat offers an extended API that lets the caller control
 * whether a nondefault buffer access strategy can be used, and whether
 * syncscan can be chosen (possibly resulting in the scan not starting from
 * block zero).  Both of these default to TRUE with plain heap_beginscan.
 *
 * heap_beginscan_bm is an alternative entry point for setting up a
 * HeapScanDesc for a bitmap heap scan.  Although that scan technology is
 * really quite unlike a standard seqscan, there is just enough commonality
 * to make it worth using the same data structure.
 * ----------------
 */
TableScanDesc heap_beginscan(Relation relation, Snapshot snapshot, int nkeys, ScanKey key, RangeScanInRedis rangeScanInRedis)
{
    uint32 flags = 0;
    if(!rangeScanInRedis.isRangeScanInRedis) {
        flags = SO_ALLOW_STRAT | SO_ALLOW_SYNC;
    }
    /* We don't allow sync buffer read if it is a range scan in redis */
    return (TableScanDesc)heap_beginscan_internal(
        relation, snapshot, nkeys, key, flags, rangeScanInRedis);
}

TableScanDesc heap_beginscan_strat(
    Relation relation, Snapshot snapshot, int nkeys, ScanKey key, bool allow_strat, bool allow_sync)
{
    uint32 flags = 0;
    if(allow_strat)
        flags |= SO_ALLOW_STRAT;
    if(allow_sync)
        flags |= SO_ALLOW_SYNC;
    return (TableScanDesc)heap_beginscan_internal(relation, snapshot, nkeys, key, flags);
}

TableScanDesc heap_beginscan_bm(Relation relation, Snapshot snapshot, int nkeys, ScanKey key)
{
    uint32 flags = SO_TYPE_BITMAPSCAN;
    return (TableScanDesc)heap_beginscan_internal(relation, snapshot, nkeys, key, flags);
}

/*
 * Description: Begin scan tuple for sample table.
 *
 * Parameters:
 *	@in relation: relation sample table
 *	@in snapshot: current activity snapshot
 *	@in nkeys: number of scan keys
 *	@in key: array of scan key descriptors
 *	@in allow_strat: allow or disallow use of access strategy
 *	@in allow_sync: allow or disallow use of syncscan
 *	@in is_range_scan_in_redis: true if it is a range scan in redistribution
 *
 * Return: HeapScanDesc
 */
TableScanDesc heap_beginscan_sampling(Relation relation, Snapshot snapshot, int nkeys, ScanKey key, bool allow_strat,
    bool allow_sync, RangeScanInRedis rangeScanInRedis)
{
    uint32 flags = SO_TYPE_SAMPLESCAN;

    if(allow_strat)
        flags |= SO_ALLOW_STRAT;
    if(allow_sync)
        flags |= SO_ALLOW_SYNC;

    return (TableScanDesc)heap_beginscan_internal(
        relation, snapshot, nkeys, key, flags, rangeScanInRedis);
}

static HeapScanDesc heap_beginscan_internal(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
    uint32 flags, RangeScanInRedis rangeScanInRedis)
{
    HeapScanDesc scan;

    /*
     * increment relation ref count while scanning relation
     *
     * This is just to make really sure the relcache entry won't go away while
     * the scan has a pointer to it.  Caller should be holding the rel open
     * anyway, so this is redundant in all normal scenarios...
     */
    if (!RelationIsPartitioned(relation)) {
        RelationIncrementReferenceCount(relation);
    } else {
        /*
         * If the table is a partition table, the current scan must be used by
         * bitmapscan to scan tuples using GPI. Therefore,
         * the value of rs_rd in the scan is used to store partition-fake-relation.
         */
        Assert((flags & SO_TYPE_BITMAPSCAN) != 0);
    }

    /*
     * allocate and initialize scan descriptor
     */
    scan = (HeapScanDesc)palloc(SizeofHeapScanDescData + MaxHeapTupleSize);

    scan->rs_base.rs_rd = relation;
    scan->rs_tupdesc = RelationGetDescr(relation);
    scan->rs_base.rs_snapshot = snapshot;
    scan->rs_base.rs_nkeys = nkeys;
    scan->rs_base.rs_flags = flags;
    scan->rs_base.rs_strategy = NULL; /* set in initscan */
    scan->rs_base.rs_rangeScanInRedis = rangeScanInRedis;

    /*
     * we can use page-at-a-time mode if it's an MVCC-safe snapshot
     */
    scan->rs_base.rs_pageatatime = IsMVCCSnapshot(snapshot);

    /*
     * For a seqscan in a serializable transaction, acquire a predicate lock
     * on the entire relation. This is required not only to lock all the
     * matching tuples, but also to conflict with new insertions into the
     * table. In an indexscan, we take page locks on the index pages covering
     * the range specified in the scan qual, but in a heap scan there is
     * nothing more fine-grained to lock. A bitmap scan is a different story,
     * there we have already scanned the index and locked the index pages
     * covering the predicate. But in that case we still have to lock any
     * matching heap tuples.
     */
    if (((scan->rs_base.rs_flags & SO_TYPE_BITMAPSCAN) == 0)) {
        PredicateLockRelation(relation, snapshot);
    }

    /* set tupTableType to HEAP_TUPLE here */
    scan->rs_ctup.tupTableType = HEAP_TUPLE;

    /* we only need to set this up once */
    scan->rs_ctup.t_tableOid = RelationGetRelid(relation);
    scan->rs_ctup.t_bucketId = RelationGetBktid(relation);
#ifdef PGXC
    scan->rs_ctup.t_xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
#endif

    /*
     * we do this here instead of in initscan() because heap_rescan also calls
     * initscan() and we don't want to allocate memory again
     */
    if (nkeys > 0) {
        scan->rs_base.rs_key = (ScanKey)palloc(sizeof(ScanKeyData) * nkeys);
    } else {
        scan->rs_base.rs_key = NULL;
    }

    initscan(scan, key, false);

    return scan;
}

/* ----------------
 *		heap_rescan		- restart a relation scan
 * ----------------
 */
void heap_rescan(TableScanDesc sscan, ScanKey key)
{
    HeapScanDesc scan = (HeapScanDesc) sscan;
    /*
     * unpin scan buffers
     */
    if (BufferIsValid(scan->rs_base.rs_cbuf)) {
        ReleaseBuffer(scan->rs_base.rs_cbuf);
    }

    /*
     * reinitialize scan descriptor
     */
    initscan(scan, key, true);
}

/* ----------------
 *		heap_endscan	- end relation scan
 *
 *		See how to integrate with index scans.
 *		Check handling if reldesc caching.
 * ----------------
 */
void heap_endscan(TableScanDesc sscan)
{
    /* Note: no locking manipulations needed
     *
     *  unpin scan buffers
     */
    HeapScanDesc scan = (HeapScanDesc)sscan;
    if (BufferIsValid(scan->rs_base.rs_cbuf)) {
        ReleaseBuffer(scan->rs_base.rs_cbuf);
    }

    /* decrement relation reference count and free scan descriptor storage */
    if (!RelationIsPartitioned(scan->rs_base.rs_rd)) {
        RelationDecrementReferenceCount(scan->rs_base.rs_rd);
    }

    if (scan->rs_base.rs_key != NULL) {
        pfree(scan->rs_base.rs_key);
        scan->rs_base.rs_key = NULL;
    }

    if (scan->rs_base.rs_strategy != NULL) {
        FreeAccessStrategy(scan->rs_base.rs_strategy);
    }

    pfree(scan);
    scan = NULL;
}
/*
 * heap_getnext	- retrieve next tuple in scan
 *
 * Fix to work with index relations.
 * We don't return the buffer anymore, but you can get it from the
 * returned HeapTuple.
 */
#ifdef HEAPDEBUGALL
#define HEAPDEBUG_1                                          \
    ereport(DEBUG2,                                          \
        (errmsg("heap_getnext([%s,nkeys=%d],dir=%d) called", \
            RelationGetRelationName(scan->rs_base.rs_rd),            \
            scan->rs_base.rs_nkeys,                                  \
            (int)direction)))
#define HEAPDEBUG_2 ereport(DEBUG2, (errmsg("heap_getnext returning EOS")))
#define HEAPDEBUG_3 ereport(DEBUG2, (errmsg("heap_getnext returning tuple")))
#else
#define HEAPDEBUG_1
#define HEAPDEBUG_2
#define HEAPDEBUG_3
#endif /* !defined(HEAPDEBUGALL) */

/*
 * heapGetNextForVerify
 *
 * @Description: fetch next heap tuple for verify.
 * @in scan - the relation's heap scan description.
 * @in direction - the scan direction, The default scan is ForwardScanDirection.
 * @in&out is_valid_relation_page - judge the relation page is valid or corrupted.
 * @return: HeapTuple
 */
HeapTuple heapGetNextForVerify(TableScanDesc sscan, ScanDirection direction, bool& is_valid_relation_page)
{
    /* Note: no locking manipulations needed */
    /* heap_getnext( info ) */
    HeapScanDesc scan = (HeapScanDesc) sscan;
    HEAPDEBUG_1;
    is_valid_relation_page = VerifyHeapGetTup(scan, direction);

    if (scan->rs_ctup.t_data == NULL) {
        /* heap_getnext returning EOS */
        HEAPDEBUG_2;
        return NULL;
    }

    /*
     * if we get here it means we have a new current scan tuple, so point to
     * the proper return buffer and return the tuple.
     *
     * heap_getnext returning tuple
     */
    HEAPDEBUG_3;

    pgstat_count_heap_getnext(scan->rs_base.rs_rd);

    Assert(!HEAP_TUPLE_IS_COMPRESSED(scan->rs_ctup.t_data));
    return &(scan->rs_ctup);
}

HeapTuple heap_getnext(TableScanDesc sscan, ScanDirection direction)
{
    HeapScanDesc scan = (HeapScanDesc) sscan;
    /* Note: no locking manipulations needed */
    HEAPDEBUG_1; /* heap_getnext( info ) */

    if (scan->rs_base.rs_pageatatime) {
        heapgettup_pagemode(scan, direction, scan->rs_base.rs_nkeys, scan->rs_base.rs_key);
    } else {
        heapgettup(scan, direction, scan->rs_base.rs_nkeys, scan->rs_base.rs_key);
    }

    if (scan->rs_ctup.t_data == NULL) {
        HEAPDEBUG_2; /* heap_getnext returning EOS */
        return NULL;
    }

    /*
     * if we get here it means we have a new current scan tuple, so point to
     * the proper return buffer and return the tuple.
     */
    HEAPDEBUG_3; /* heap_getnext returning tuple */

    pgstat_count_heap_getnext(scan->rs_base.rs_rd);

    Assert(!HEAP_TUPLE_IS_COMPRESSED(scan->rs_ctup.t_data));
    return &(scan->rs_ctup);
}

/*
 *	heap_fetch		- retrieve tuple with given tid
 *
 * On entry, tuple->t_self is the TID to fetch.  We pin the buffer holding
 * the tuple, fill in the remaining fields of *tuple, and check the tuple
 * against the specified snapshot.
 *
 * If successful (tuple found and passes snapshot time qual), then *userbuf
 * is set to the buffer holding the tuple and TRUE is returned.  The caller
 * must unpin the buffer when done with the tuple.
 *
 * If the tuple is not found (ie, item number references a deleted slot),
 * then tuple->t_data is set to NULL and FALSE is returned.
 *
 * If the tuple is found but fails the time qual check, then FALSE is returned
 * but tuple->t_data is left pointing to the tuple.
 *
 * keep_buf determines what is done with the buffer in the FALSE-result cases.
 * When the caller specifies keep_buf = true, we retain the pin on the buffer
 * and return it in *userbuf (so the caller must eventually unpin it); when
 * keep_buf = false, the pin is released and *userbuf is set to InvalidBuffer.
 *
 * stats_relation is the relation to charge the heap_fetch operation against
 * for statistical purposes.  (This could be the heap rel itself, an
 * associated index, or NULL to not count the fetch at all.)
 *
 * heap_fetch does not follow HOT chains: only the exact TID requested will
 * be fetched.
 *
 * It is somewhat inconsistent that we ereport() on invalid block number but
 * return false on invalid item number.  There are a couple of reasons though.
 * One is that the caller can relatively easily check the block number for
 * validity, but cannot check the item number without reading the page
 * himself.  Another is that when we are following a t_ctid link, we can be
 * reasonably confident that the page number is valid (since VACUUM shouldn't
 * truncate off the destination page without having killed the referencing
 * tuple first), but the item number might well not be good.
 */
bool heap_fetch(Relation relation, Snapshot snapshot, HeapTuple tuple, Buffer* userbuf,
    bool keep_buf, Relation stats_relation)
{
    ItemPointer tid = &(tuple->t_self);
    ItemId lp;
    Buffer buffer;
    Page page;
    HeapTupleData private_tuple_data = *tuple; /* private copy of tuple */
    HeapTuple private_tuple = &private_tuple_data;
    OffsetNumber offnum;
    bool valid = false;

    /* another data space must be provided for decomperssing tuple. */
    Assert(tuple && tuple->t_data);

    /*
     * Fetch and pin the appropriate page of the relation.
     */
    buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));

    /* We've pinned the buffer, nobody can prune this buffer, check whether snapshot is valid. */
    CheckSnapshotIsValidException(snapshot, "heap_fetch");

    /*
     * Need share lock on buffer to examine tuple commit status.
     */
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buffer);

    /*
     * We'd better check for out-of-range offnum in case of VACUUM since the
     * TID was obtained.
     */
    offnum = ItemPointerGetOffsetNumber(tid);
    if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page)) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        if (keep_buf) {
            *userbuf = buffer;
        } else {
            ReleaseBuffer(buffer);
            *userbuf = InvalidBuffer;
        }
        tuple->t_data = NULL;
        return false;
    }

    /*
     * get the item line pointer corresponding to the requested tid
     */
    lp = PageGetItemId(page, offnum);
    /*
     * Must check for deleted tuple.
     */
    if (!ItemIdIsNormal(lp)) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        if (keep_buf) {
            *userbuf = buffer;
        } else {
            ReleaseBuffer(buffer);
            *userbuf = InvalidBuffer;
        }
        tuple->t_data = NULL;
        return false;
    }

    /*
     * fill in *tuple fields
     */
    private_tuple->t_data = (HeapTupleHeader)PageGetItem(page, lp);
    private_tuple->t_len = ItemIdGetLength(lp);
    HeapTupleCopyBaseFromPage(private_tuple, page);
    private_tuple->t_tableOid = RelationGetRelid(relation);
    private_tuple->t_bucketId = RelationGetBktid(relation);
#ifdef PGXC
    private_tuple->t_xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
#endif

    /*
     * check time qualification of tuple, then release lock
     */
    valid = HeapTupleSatisfiesVisibility(private_tuple, snapshot, buffer);
    if (valid) {
        PredicateLockTuple(relation, private_tuple, snapshot);

        DECOMPRESS_HEAP_TUPLE(HEAP_TUPLE_IS_COMPRESSED(private_tuple->t_data), private_tuple, tuple->t_data,
                              relation->rd_att, (const char *)page);
    }
    CheckForSerializableConflictOut(valid, relation, private_tuple, buffer, snapshot);

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    /* copy heap tuple info into output <tuple>  */
    tuple->t_data = private_tuple->t_data;
    tuple->t_len = private_tuple->t_len;
    tuple->t_tableOid = private_tuple->t_tableOid;
    tuple->t_bucketId = private_tuple->t_bucketId;
    HeapTupleCopyBase(tuple, private_tuple);
#ifdef PGXC
    tuple->t_xc_node_id = private_tuple->t_xc_node_id;
#endif

    if (valid) {
        /*
         * All checks passed, so return the tuple as valid. Caller is now
         * responsible for releasing the buffer.
         */
        *userbuf = buffer;

       /* Count the successful fetch against appropriate rel, if any */
        if (stats_relation != NULL) {
            pgstat_count_heap_fetch(stats_relation);
        }
        return true;
    }

    /* Tuple failed time qual, but maybe caller wants to see it anyway. */
    if (keep_buf) {
        *userbuf = buffer;
    } else {
        ReleaseBuffer(buffer);
        *userbuf = InvalidBuffer;
    }
    return false;
}

/*
 *	heap_hot_search_buffer	- search HOT chain for tuple satisfying snapshot
 *
 * On entry, *tid is the TID of a tuple (either a simple tuple, or the root
 * of a HOT chain), and buffer is the buffer holding this tuple.  We search
 * for the first chain member satisfying the given snapshot.  If one is
 * found, we update *tid to reference that tuple's offset number, and
 * return TRUE.  If no match, return FALSE without modifying *tid.
 *
 * heap_tuple is a caller-supplied buffer.  When a match is found, we return
 * the tuple here, in addition to updating *tid.  If no match is found, the
 * contents of this buffer on return are undefined.
 *
 * If all_dead is not NULL, we check non-visible tuples to see if they are
 * globally dead; *all_dead is set TRUE if all members of the HOT chain
 * are vacuumable, FALSE if not.
 *
 * Unlike heap_fetch, the caller must already have pin and (at least) share
 * lock on the buffer; it is still pinned/locked at exit.  Also unlike
 * heap_fetch, we do not report any pgstats count; caller may do so if wanted.
 */
bool heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer, Snapshot snapshot, HeapTuple heap_tuple,
                            HeapTupleHeaderData *uncompress_tup, bool *all_dead, bool first_call)
{
    Page dp = (Page)BufferGetPage(buffer);
    TransactionId prev_xmax = InvalidTransactionId;
    OffsetNumber offnum;
    bool at_chain_start = false;
    bool valid = false;
    bool skip = false;

    /* If this is not the first call, previous call returned a (live!) tuple */
    if (all_dead != NULL) {
        *all_dead = first_call;
    }

    Assert(TransactionIdIsValid(u_sess->utils_cxt.RecentGlobalXmin));

    Assert(ItemPointerGetBlockNumber(tid) == BufferGetBlockNumber(buffer));
    offnum = ItemPointerGetOffsetNumber(tid);
    at_chain_start = first_call;
    skip = !first_call;

    heap_tuple->t_self = *tid;
    HeapTupleCopyBaseFromPage(heap_tuple, dp);

    /* Scan through possible multiple members of HOT-chain */
    for (;;) {
        ItemId lp;

        /* check for bogus TID */
        if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(dp)) {
            break;
        }

        lp = PageGetItemId(dp, offnum);
        /* check for unused, dead, or redirected items */
        if (!ItemIdIsNormal(lp)) {
            /* We should only see a redirect at start of chain */
            if (ItemIdIsRedirected(lp) && at_chain_start) {
                /* Follow the redirect */
                offnum = ItemIdGetRedirect(lp);
                at_chain_start = false;
                continue;
            }
            /* else must be end of chain */
            break;
        }

        heap_tuple->t_data = (HeapTupleHeader)PageGetItem(dp, lp);
        heap_tuple->t_len = ItemIdGetLength(lp);
        heap_tuple->t_tableOid = RelationGetRelid(relation);
        heap_tuple->t_bucketId = RelationGetBktid(relation);
        HeapTupleCopyBaseFromPage(heap_tuple, dp);
#ifdef PGXC
        heap_tuple->t_xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
#endif
        ItemPointerSetOffsetNumber(&heap_tuple->t_self, offnum);

        /*
         * Shouldn't see a HEAP_ONLY tuple at chain start.
         */
        if (at_chain_start && HeapTupleIsHeapOnly(heap_tuple)) {
            break;
        }

        /*
         * The xmin should match the previous xmax value, else chain is
         * broken.
         */
        if (TransactionIdIsValid(prev_xmax) && !TransactionIdEquals(prev_xmax, HeapTupleGetRawXmin(heap_tuple))) {
            break;
        }

        /*
         * When first_call is true (and thus, skip is initially false) we'll
         * return the first tuple we find.	But on later passes, heap_tuple
         * will initially be pointing to the tuple we returned last time.
         * Returning it again would be incorrect (and would loop forever), so
         * we skip it and return the next match we find.
         */
        if (!skip) {
            /*
             * For the benefit of logical decoding, have t_self point at the
             * element of the HOT chain we're currently investigating instead
             * of the root tuple of the HOT chain. This is important because
             * the *Satisfies routine for historical mvcc snapshots needs the
             * correct tid to decide about the visibility in some cases.
             */
            ItemPointerSet(&(heap_tuple->t_self), BufferGetBlockNumber(buffer), offnum);
            /* If it's visible per the snapshot, we must return it */
            valid = HeapTupleSatisfiesVisibility(heap_tuple, snapshot, buffer);
            CheckForSerializableConflictOut(valid, relation, heap_tuple, buffer, snapshot);

            if (SHOW_DEBUG_MESSAGE()) {
                ereport(DEBUG1,
                    (errmsg("heap_hot_search_buffer xid %lu self(%u,%hu) ctid(%u,%hu) valid %d "
                            "pointer(%u,%hu)",
                            GetCurrentTransactionIdIfAny(),
                            ItemPointerGetBlockNumber(&heap_tuple->t_self),
                            ItemPointerGetOffsetNumber(&heap_tuple->t_self),
                            ItemPointerGetBlockNumber(&heap_tuple->t_data->t_ctid),
                            ItemPointerGetOffsetNumber(&heap_tuple->t_data->t_ctid),
                            valid,
                            ItemPointerGetBlockNumber(&heap_tuple->t_data->t_ctid),
                            ItemPointerGetOffsetNumber(&heap_tuple->t_data->t_ctid))));
            }

            /* reset to original, non-redirected, tid */
            heap_tuple->t_self = *tid;

            if (valid) {
                ItemPointerSetOffsetNumber(tid, offnum);
                PredicateLockTuple(relation, heap_tuple, snapshot);
                if (all_dead != NULL) {
                    *all_dead = false;
                }
                /*
                 * If uncompress_tup is NULL, the caller will not need tuple data
                 * Only check some status
                 */
                if (uncompress_tup != NULL) {
                    DECOMPRESS_HEAP_TUPLE(HEAP_TUPLE_IS_COMPRESSED(heap_tuple->t_data), heap_tuple, uncompress_tup,
                                          relation->rd_att, (const char *)BufferGetPage(buffer));
                }
                return true;
            }
        }
        skip = false;

        /*
         * If we can't see it, maybe no one else can either.  At caller
         * request, check whether all chain members are dead to all
         * transactions.
         */
        if (all_dead && *all_dead && !HeapTupleIsSurelyDead(heap_tuple, u_sess->utils_cxt.RecentGlobalXmin)) {
            *all_dead = false;
        }

        /*
         * Check to see if HOT chain continues past this tuple; if so fetch
         * the next offnum and loop around.
         */
        if (HeapTupleIsHotUpdated(heap_tuple)) {
            Assert(ItemPointerGetBlockNumber(&heap_tuple->t_data->t_ctid) == ItemPointerGetBlockNumber(tid));
            offnum = ItemPointerGetOffsetNumber(&heap_tuple->t_data->t_ctid);
            at_chain_start = false;
            prev_xmax = HeapTupleGetRawXmax(heap_tuple);
        } else {
            break; /* end of chain */
        }
    }

    return false;
}

/*
 *	heap_hot_search		- search HOT chain for tuple satisfying snapshot
 *
 * This has the same API as heap_hot_search_buffer, except that the caller
 * does not provide the buffer containing the page, rather we access it
 * locally.
 */
bool heap_hot_search(ItemPointer tid, Relation relation, Snapshot snapshot, bool *all_dead)
{
    bool result = false;
    Buffer buffer;
    HeapTupleData heap_tuple;

    buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    result = heap_hot_search_buffer(tid, relation, buffer, snapshot, &heap_tuple, NULL, all_dead, true);
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
    ReleaseBuffer(buffer);
    return result;
}

void heap_get_max_tid(const Relation rel, ItemPointer ctid)
{
    BlockNumber blk;
    OffsetNumber offnum;
    Buffer buffer;
    Page page;

    blk = RelationGetNumberOfBlocks(rel) - 1;
    if (!BlockNumberIsValid(blk)) {
        /* the target table must be empty, we should just return (0,0) only */
        ItemPointerZero(ctid);
        return;
    }

    buffer = ReadBuffer(rel, blk);
    LockBuffer(buffer, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buffer);
    offnum = PageGetMaxOffsetNumber(page);
    UnlockReleaseBuffer(buffer);

    ItemPointerSet(ctid, blk, offnum);
    return;
}

/*
 *	heap_get_latest_tid -  get the latest tid of a specified tuple
 *
 * Actually, this gets the latest version that is visible according to
 * the passed snapshot.  You can pass SnapshotDirty to get the very latest,
 * possibly uncommitted version.
 *
 * *tid is both an input and an output parameter: it is updated to
 * show the latest version of the row.	Note that it will not be changed
 * if no version of the row passes the snapshot test.
 */
void heap_get_latest_tid(Relation relation, Snapshot snapshot, ItemPointer tid)
{
    BlockNumber blk;
    ItemPointerData ctid;
    TransactionId priorXmax;

    /* this is to avoid Assert failures on bad input */
    if (!ItemPointerIsValid(tid)) {
        return;
    }

    /*
     * Since this can be called with user-supplied TID, don't trust the input
     * too much.  (RelationGetNumberOfBlocks is an expensive check, so we
     * don't check t_ctid links again this way.  Note that it would not do to
     * call it just once and save the result, either.)
     */
    blk = ItemPointerGetBlockNumber(tid);
    if (blk >= RelationGetNumberOfBlocks(relation)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("block number %u is out of range for relation \"%s\"",
                                                                blk, RelationGetRelationName(relation))));
    }

    /*
     * Loop to chase down t_ctid links.  At top of loop, ctid is the tuple we
     * need to examine, and *tid is the TID we will return if ctid turns out
     * to be bogus.
     *
     * Note that we will loop until we reach the end of the t_ctid chain.
     * Depending on the snapshot passed, there might be at most one visible
     * version of the row, but we don't try to optimize for that.
     */
    ctid = *tid;
    priorXmax = InvalidTransactionId; /* cannot check first XMIN */
    for (;;) {
        Buffer buffer;
        Page page;
        OffsetNumber offnum;
        ItemId lp;
        HeapTupleData tp;
        bool valid = false;

        /*
         * Read, pin, and lock the page.
         */
        buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(&ctid));
        LockBuffer(buffer, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buffer);

        /*
         * Check for bogus item number.  This is not treated as an error
         * condition because it can happen while following a t_ctid link. We
         * just assume that the prior tid is OK and return it unchanged.
         */
        offnum = ItemPointerGetOffsetNumber(&ctid);
        if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page)) {
            UnlockReleaseBuffer(buffer);
            break;
        }
        lp = PageGetItemId(page, offnum);
        if (!ItemIdIsNormal(lp)) {
            UnlockReleaseBuffer(buffer);
            break;
        }

        /* OK to access the tuple */
        tp.t_self = ctid;
        tp.t_data = (HeapTupleHeader)PageGetItem(page, lp);
        tp.t_len = ItemIdGetLength(lp);
        tp.t_tableOid = RelationGetRelid(relation);
        tp.t_bucketId = RelationGetBktid(relation);
        HeapTupleCopyBaseFromPage(&tp, page);

        /*
         * After following a t_ctid link, we might arrive at an unrelated
         * tuple.  Check for XMIN match.
         */
        if (TransactionIdIsValid(priorXmax) && !TransactionIdEquals(priorXmax, HeapTupleGetRawXmin(&tp))) {
            UnlockReleaseBuffer(buffer);
            break;
        }

        /*
         * Check time qualification of tuple; if visible, set it as the new
         * result candidate.
         */
        valid = HeapTupleSatisfiesVisibility(&tp, snapshot, buffer);
        CheckForSerializableConflictOut(valid, relation, &tp, buffer, snapshot);
        if (valid) {
            *tid = ctid;
        }

        /*
         * If there's a valid t_ctid link, follow it, else we're done.
         */
        if ((tp.t_data->t_infomask & (HEAP_XMAX_INVALID | HEAP_IS_LOCKED)) ||
            ItemPointerEquals(&tp.t_self, &tp.t_data->t_ctid)) {
            UnlockReleaseBuffer(buffer);
            break;
        }

        ctid = tp.t_data->t_ctid;
        priorXmax = HeapTupleGetRawXmax(&tp);
        UnlockReleaseBuffer(buffer);
    } /* end of loop */
}

/*
 * UpdateXmaxHintBits - update tuple hint bits after xmax transaction ends
 *
 * This is called after we have waited for the XMAX transaction to terminate.
 * If the transaction aborted, we guarantee the XMAX_INVALID hint bit will
 * be set on exit.	If the transaction committed, we set the XMAX_COMMITTED
 * hint bit if possible --- but beware that that may not yet be possible,
 * if the transaction committed asynchronously.  Hence callers should look
 * only at XMAX_INVALID.
 */
static void UpdateXmaxHintBits(HeapTupleHeader tuple, Buffer buffer, TransactionId xid)
{
    Assert(TransactionIdEquals(HeapTupleHeaderGetXmax(BufferGetPage(buffer), tuple), xid));

    if (!(tuple->t_infomask & (HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID))) {
        if (TransactionIdDidCommit(xid)) {
            HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED, xid);
        } else {
            if (!LatestFetchTransactionIdDidAbort(xid)) {
                LatestTransactionStatusError(xid, NULL, "UpdateXmaxHintBits set HEAP_XMAX_INVALID xid don't abort");
            }
            HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_INVALID, InvalidTransactionId);
        }
    }
}

/*
 * GetBulkInsertState - prepare status object for a bulk insert
 */
BulkInsertState GetBulkInsertState(void)
{
    BulkInsertState bistate;

    bistate = (BulkInsertState)palloc(sizeof(BulkInsertStateData));
    bistate->strategy = GetAccessStrategy(BAS_BULKWRITE);
    bistate->current_buf = InvalidBuffer;
    return bistate;
}

/*
 * FreeBulkInsertState - clean up after finishing a bulk insert
 */
void FreeBulkInsertState(BulkInsertState bistate)
{
    if (bistate->current_buf != InvalidBuffer) {
        ReleaseBuffer(bistate->current_buf);
    }
    FreeAccessStrategy(bistate->strategy);
    pfree(bistate);
    bistate = NULL;
}

/* HeapInsertCStore - insert tuple into CStore */
void HeapInsertCStore(Relation relation, ResultRelInfo *result_rel_info, HeapTuple tup, int option)
{
    /* Description: deal with index */
    InsertArg args;
    CStoreInsert::InitInsertArg(relation, result_rel_info, false, args);
    CStoreInsert cstoreInsert(relation, args, false, NULL, NULL);

    TupleDesc tupDesc = relation->rd_att;
    Datum *val = (Datum *)palloc(sizeof(Datum) * tupDesc->natts);
    bool *null = (bool *)palloc(sizeof(bool) * tupDesc->natts);
    heap_deform_tuple(tup, tupDesc, val, null);

    bulkload_rows batchRow(tupDesc, RelationGetMaxBatchRows(relation), true);
    /* ignore returned value because only one tuple is appended into */
    (void)batchRow.append_one_tuple(val, null, tupDesc);
    cstoreInsert.SetEndFlag();
    cstoreInsert.BatchInsert(&batchRow, option);

    pfree(val);
    pfree(null);
    CStoreInsert::DeInitInsertArg(args);
    batchRow.Destroy();
    cstoreInsert.Destroy();
}

void HeapDeleteCStore(Relation relation, ItemPointer tid, Oid table_oid, Snapshot snapshot)
{
    ScalarVector rowid;
    CStoreDelete csdelete(relation, NULL, false, NULL, NULL);
    ScalarDesc desc;

    desc.typeMod = 0;
    rowid.init(CurrentMemoryContext, desc);
    rowid.m_rows = 1;
    rowid.m_vals[0] = 0;
    ItemPointer destTid = (ItemPointer)(&rowid.m_vals[0]);
    *destTid = *tid;
    csdelete.ExecDelete(relation, &rowid, snapshot, table_oid);
    pfree(rowid.m_flag);
    pfree(rowid.m_vals);
    delete rowid.m_buf;
}

#ifdef ENABLE_MULTIPLE_NODES
/* HeapInsertTsStore - insert tuple into TsStore */
void HeapInsertTsStore(Relation relation, ResultRelInfo *resultRelInfo, HeapTuple tup, int option)
{
    if (!g_instance.attr.attr_common.enable_tsdb) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("Please enable timeseries first!")));
    }
    Tsdb::TsStoreInsert *tsstoreInsert = NULL;
    tsstoreInsert = New(CurrentMemoryContext) Tsdb::TsStoreInsert(relation);

    TupleDesc tupDesc = relation->rd_att;
    Datum *val = (Datum *)palloc(sizeof(Datum) * tupDesc->natts);
    bool *null = (bool *)palloc(sizeof(bool) * tupDesc->natts);
    heap_deform_tuple(tup, tupDesc, val, null);

    tsstoreInsert->batch_insert(val, null, option, true);
    tsstoreInsert->end_batch_insert();

    pfree(val);
    pfree(null);
    delete tsstoreInsert;
    tsstoreInsert = NULL;
}
#endif /* ENABLE_MULTIPLE_NODES */

/*
 *	heap_insert		- insert tuple into a heap
 *
 * The new tuple is stamped with current transaction ID and the specified
 * command ID.
 *
 * If the HEAP_INSERT_SKIP_WAL option is specified, the new tuple is not
 * logged in WAL, even for a non-temp relation.  Safe usage of this behavior
 * requires that we arrange that all new tuples go into new pages not
 * containing any tuples from other transactions, and that the relation gets
 * fsync'd before commit.  (See also heap_sync() comments)
 *
 * The HEAP_INSERT_SKIP_FSM option is passed directly to
 * RelationGetBufferForTuple, which see for more info.
 *
 * Note that these options will be applied when inserting into the heap's
 * TOAST table, too, if the tuple requires any out-of-line data.
 *
 * The BulkInsertState object (if any; bistate can be NULL for default
 * behavior) is also just passed through to RelationGetBufferForTuple.
 *
 * The return value is the OID assigned to the tuple (either here or by the
 * caller), or InvalidOid if no OID.  The header fields of *tup are updated
 * to match the stored tuple; in particular tup->t_self receives the actual
 * TID where the tuple was stored.	But note that any toasting of fields
 * within the tuple data is NOT reflected into *tup.
 */
Oid heap_insert(Relation relation, HeapTuple tup, CommandId cid, int options, BulkInsertState bistate)
{
    TransactionId xid = GetCurrentTransactionId();
    HeapTuple heaptup;
    Buffer buffer;
    Buffer vmbuffer = InvalidBuffer;
    bool all_visible_cleared = false;
    BlockNumber rel_end_block = InvalidBlockNumber;

    /*
     * Fill in tuple header fields, assign an OID, and toast the tuple if
     * necessary.
     *
     * Note: below this point, heaptup is the data we actually intend to store
     * into the relation; tup is the caller's original untoasted data.
     */
    heaptup = heap_prepare_insert(relation, tup, cid, options);

    /* All built-in functions are hard coded, and thus they should not be inserted into catalog.pg_proc */
    if (!IsBootstrapProcessingMode() && u_sess->attr.attr_common.IsInplaceUpgrade == false) {
        Assert(!(IsProcRelation(relation) && IsSystemObjOid(HeapTupleGetOid(heaptup))));
    }

    /*
     * We're about to do the actual insert -- but check for conflict first, to
     * avoid possibly having to roll back work we've just done.
     *
     * For a heap insert, we only need to check for table-level SSI locks. Our
     * new tuple can't possibly conflict with existing tuple locks, and heap
     * page locks are only consolidated versions of tuple locks; they do not
     * lock "gaps" as index page locks do.	So we don't need to identify a
     * buffer before making the call.
     */
    CheckForSerializableConflictIn(relation, NULL, InvalidBuffer);

    if (RelationInClusterResizing(relation) && !RelationInClusterResizingReadOnly(relation)) {
        options |= HEAP_INSERT_SKIP_FSM;
        rel_end_block = RelationGetEndBlock(relation);
    }

    /*
     * Find buffer to insert this tuple into.  If the page is all visible,
     * this will also pin the requisite visibility map page.
     */
    buffer = RelationGetBufferForTuple(relation, heaptup->t_len, InvalidBuffer, options, bistate, &vmbuffer, NULL,
                                       rel_end_block);

    (void)heap_page_prepare_for_xid(relation, buffer, xid, false);
    HeapTupleCopyBaseFromPage(heaptup, BufferGetPage(buffer));

    /* NO EREPORT(ERROR) from here till changes are logged */
    START_CRIT_SECTION();

    RelationPutHeapTuple(relation, buffer, heaptup, xid);

    if (PageIsAllVisible(BufferGetPage(buffer))) {
        all_visible_cleared = true;
        PageClearAllVisible(BufferGetPage(buffer));
        visibilitymap_clear(relation, ItemPointerGetBlockNumber(&(heaptup->t_self)), vmbuffer);
    }

    /*
     * XXX Should we set PageSetPrunable on this page ?
     *
     * The inserting transaction may eventually abort thus making this tuple
     * DEAD and hence available for pruning. Though we don't want to optimize
     * for aborts, if no other tuple in this page is UPDATEd/DELETEd, the
     * aborted tuple will never be pruned until next vacuum is triggered.
     *
     * If you do add PageSetPrunable here, add it in heap_xlog_insert too.
     */
    MarkBufferDirty(buffer);

    /* XLOG stuff */
    if (!(options & HEAP_INSERT_SKIP_WAL) && RelationNeedsWAL(relation)) {
        xl_heap_insert xlrec;
        xl_heap_header xlhdr;
        XLogRecPtr recptr;
        Page page = BufferGetPage(buffer);
        uint8 info = XLOG_HEAP_INSERT;
        int bufflags = 0;

        /*
         * If this is a catalog, we need to transmit combocids to properly
         * decode, so log that as well.
         */
        if (RelationIsAccessibleInLogicalDecoding(relation)) {
            (void)log_heap_new_cid(relation, heaptup);
        }

        /*
         * If this is the single and first tuple on page, we can reinit the
         * page instead of restoring the whole thing.  Set flag, and hide
         * buffer references from XLogInsert. Moreover, if page is already
         * compressed, should not init page, or lead to inconsistency.
         */
        if (ItemPointerGetOffsetNumber(&(heaptup->t_self)) == FirstOffsetNumber &&
            PageGetMaxOffsetNumber(page) == FirstOffsetNumber && !PageIsCompressed(page)) {
            info |= XLOG_HEAP_INIT_PAGE;
            bufflags |= REGBUF_WILL_INIT;
        }

        xlrec.offnum = ItemPointerGetOffsetNumber(&heaptup->t_self);
        xlrec.flags = all_visible_cleared ? XLH_INSERT_ALL_VISIBLE_CLEARED : 0;
        Assert(ItemPointerGetBlockNumber(&heaptup->t_self) == BufferGetBlockNumber(buffer));

        /*
         * For logical decoding, we need the tuple even if we're doing a full
         * page write, so make sure it's included even if we take a full-page
         * image. (XXX We could alternatively store a pointer into the FPW).
         */
        if (RelationIsLogicallyLogged(relation)) {
            xlrec.flags |= XLH_INSERT_CONTAINS_NEW_TUPLE;
            bufflags |= REGBUF_KEEP_DATA;
        }

        XLogBeginInsert();
        if (info & XLOG_HEAP_INIT_PAGE) {
            XLogRegisterData((char *)&((HeapPageHeader)(page))->pd_xid_base, sizeof(TransactionId));
        }
        XLogRegisterData((char *)&xlrec, SizeOfHeapInsert);

        xlhdr.t_infomask2 = heaptup->t_data->t_infomask2;
        xlhdr.t_infomask = heaptup->t_data->t_infomask;
        xlhdr.t_hoff = heaptup->t_data->t_hoff;

        /*
         * note we mark xlhdr as belonging to buffer; if XLogInsert decides to
         * write the whole page to the xlog, we don't need to store
         * xl_heap_header in the xlog.
         */
        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD | bufflags);
        XLogRegisterBufData(0, (char *)&xlhdr, SizeOfHeapHeader);
        /* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
        XLogRegisterBufData(0, (char *)heaptup->t_data + offsetof(HeapTupleHeaderData, t_bits),
                            heaptup->t_len - offsetof(HeapTupleHeaderData, t_bits));

        /* filtering by origin on a row level is much more efficient */
        XLogIncludeOrigin();

        recptr = XLogInsert(RM_HEAP_ID, info);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buffer);
    if (vmbuffer != InvalidBuffer) {
        ReleaseBuffer(vmbuffer);
    }

    /*
     * If tuple is cachable, mark it for invalidation from the caches in case
     * we abort.  Note it is OK to do this after releasing the buffer, because
     * the heaptup data structure is all in local memory, not in the shared
     * buffer.
     */
    CacheInvalidateHeapTuple(relation, heaptup, NULL);

    pgstat_count_heap_insert(relation, 1);

    /*
     * If heaptup is a private copy, release it.  Don't forget to copy t_self
     * back to the caller's image, too.
     */
    if (heaptup != tup) {
        tup->t_self = heaptup->t_self;
        heap_freetuple(heaptup);
    }

    return HeapTupleGetOid(tup);
}

/*
 * heap_abort_speculative - kill a speculatively inserted tuple
 *
 * Marks a tuple that was speculatively inserted in the same command as dead,
 * by setting its xmin and xmax as committed.  That makes it appear as dead
 * to all transactions, including our own.  In particular, it makes
 * HeapTupleSatisfiesDirty() regard the tuple as dead, so that another backend
 * inserting a duplicate key value won't unnecessarily wait for our whole
 * transaction to finish (it'll just wait for our speculative insertion to
 * finish).
 *
 * Killing the tuple prevents "unprincipled deadlocks", which are deadlocks
 * that arise due to a mutual dependency that is not user visible.  By
 * definition, unprincipled deadlocks cannot be prevented by the user
 * reordering lock acquisition in client code, because the implementation level
 * lock acquisitions are not under the user's direct control.  If speculative
 * inserters did not take this precaution, then under high concurrency they
 * could deadlock with each other, which would not be acceptable.
 *
 * This is somewhat redundant with heap_delete, but we prefer to have a
 * dedicated routine with stripped down requirements.
 *
 * This routine does not affect logical decoding as it only looks at
 * confirmation records.
 */
void heap_abort_speculative(Relation relation, HeapTuple tuple)
{
    TransactionId xid = GetCurrentTransactionId();
    ItemPointer tid = &(tuple->t_self);
    ItemId lp;
    HeapTupleData tp;
    Page page;
    BlockNumber block;
    Buffer buffer;

    Assert(ItemPointerIsValid(tid));

    block = ItemPointerGetBlockNumber(tid);
    buffer = ReadBuffer(relation, block);
    page = BufferGetPage(buffer);

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    /*
     * Page can't be all visible, we just inserted into it, and are still
     * running.
     */
    Assert(!PageIsAllVisible(page));

    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
    Assert(ItemIdIsNormal(lp));

    tp.t_tableOid = RelationGetRelid(relation);
    tp.t_data = (HeapTupleHeader)PageGetItem(page, lp);
    tp.t_len = ItemIdGetLength(lp);
    tp.t_self = *tid;

    /*
     * Sanity check that the tuple really is a speculatively inserted tuple,
     * inserted by us.
     */
    if (HeapTupleHeaderGetXmin(page, tp.t_data) != xid) {
        HeapTupleCopyBaseFromPage(&tp, page);
        ereport(ERROR,
            (errmsg("attempted to kill a tuple inserted by another transaction: %lu, %lu",
             HeapTupleGetRawXmin(&tp), xid)));
    }
    Assert(!HeapTupleHeaderIsHeapOnly(tp.t_data));

    /*
     * No need to check for serializable conflicts here.  There is never a
     * need for a combocid, either.  No need to extract replica identity, or
     * do anything special with infomask bits.
     */

    START_CRIT_SECTION();

    /*
     * The tuple will become DEAD immediately.  Flag that this page
     * immediately is a candidate for pruning by setting xmin to
     * RecentGlobalXmin.  That's not pretty, but it doesn't seem worth
     * inventing a nicer API for this.
     */
    PageSetPrunable(page, xid);

    /* store transaction information of xact deleting the tuple */
    tp.t_data->t_infomask &= ~(HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI | HEAP_IS_LOCKED |
                               HEAP_MOVED);

    /*
     * Set the tuple header xmin and xmax to FrozenTransactionId.
     * This makes the tuple invisible to everyone.
     */
    HeapTupleHeaderSetXmin(page, tp.t_data, FrozenTransactionId);
    HeapTupleHeaderSetXmax(page, tp.t_data, FrozenTransactionId);

    MarkBufferDirty(buffer);

    /*
     * XLOG stuff
     *
     * The WAL records generated here match heap_delete().  The same recovery
     * routines are used.
     */
    if (RelationNeedsWAL(relation)) {
        xl_heap_delete xlrec;
        XLogRecPtr recptr;

        xlrec.flags = XLH_DELETE_IS_SUPER;
        xlrec.offnum = ItemPointerGetOffsetNumber(&tp.t_self);

        XLogBeginInsert();
        XLogRegisterData((char *)&xlrec, SizeOfHeapDelete);
        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

        /* No replica identity & replication origin logged */

        recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_DELETE);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    if (HeapTupleHasExternal(&tp))
        toast_delete(relation, &tp, HEAP_INSERT_SPECULATIVE);

    /*
     * Never need to mark tuple for invalidation, since catalogs don't support
     * speculative insertion
     */

    /* Now we can release the buffer */
    ReleaseBuffer(buffer);

    /* count deletion, as we counted the insertion too */
    pgstat_count_heap_delete(relation);
}

/**
 * @Description: Find minimum and maximum short transaction ids which occurs in the page.
 * @in: page, heap page
 * @in: multi, Whether multixact
 * @out: min, minimum short transaction ids which occurs in the page.
 * @out: max, maximum short transaction ids which occurs in the page.
 * @return: Whether the minimum and maximum short transaction ids are found
 */
static bool heap_page_xid_min_max(Page page, bool multi, ShortTransactionId *min, ShortTransactionId *max)
{
    bool found = false;
    OffsetNumber offnum = InvalidOffsetNumber;
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

    for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId itemid;
        HeapTupleHeader htup;

        itemid = PageGetItemId(page, offnum);
        if (!ItemIdIsNormal(itemid)) {
            continue;
        }

        htup = (HeapTupleHeader)PageGetItem(page, itemid);

        if (!multi) {
            if (!HeapTupleHeaderXminFrozen(htup) && TransactionIdIsNormal(htup->t_choice.t_heap.t_xmin)) {
                if (!found) {
                    *min = *max = htup->t_choice.t_heap.t_xmin;
                    found = true;
                } else {
                    *min = Min(*min, htup->t_choice.t_heap.t_xmin);
                    *max = Max(*max, htup->t_choice.t_heap.t_xmin);
                }
            }

            if (TransactionIdIsNormal(htup->t_choice.t_heap.t_xmax) && !(htup->t_infomask & HEAP_XMAX_IS_MULTI)) {
                if (!found) {
                    *min = *max = htup->t_choice.t_heap.t_xmax;
                    found = true;
                } else {
                    *min = Min(*min, htup->t_choice.t_heap.t_xmax);
                    *max = Max(*max, htup->t_choice.t_heap.t_xmax);
                }
            }
        } else {
            if (TransactionIdIsNormal(htup->t_choice.t_heap.t_xmax) && (htup->t_infomask & HEAP_XMAX_IS_MULTI)) {
                if (!found) {
                    *min = *max = htup->t_choice.t_heap.t_xmax;
                    found = true;
                } else {
                    *min = Min(*min, htup->t_choice.t_heap.t_xmax);
                    *max = Max(*max, htup->t_choice.t_heap.t_xmax);
                }
            }
        }
    }
    return found;
}

/*
 * Shift xid base in the page. WAL-logged if buffer is specified.
 * page is the heap page; delta is the size of change about xid base
 */
static void HeapPageShiftBase(Buffer buffer, Page page, bool multi, int64 delta)
{
    HeapPageHeader phdr = (HeapPageHeader)page;
    OffsetNumber offnum, maxoff;

    /* base left shift, mininum is 0 */
    if (delta < 0) {
        if (!multi) {
            if ((int64)(phdr->pd_xid_base + delta) < 0) {
                delta = -(int64)(phdr->pd_xid_base);
            }
        } else {
            if ((int64)(phdr->pd_multi_base + delta) < 0) {
                delta = -(int64)(phdr->pd_multi_base);
            }
        }
    }

    /* Iterate over page items */
    maxoff = PageGetMaxOffsetNumber(page);
    for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId itemid;
        HeapTupleHeader htup;

        itemid = PageGetItemId(page, offnum);
        if (!ItemIdIsNormal(itemid)) {
            continue;
        }

        htup = (HeapTupleHeader)PageGetItem(page, itemid);

        /* Apply xid shift to heap tuple */
        if (!multi) {
            if (!HeapTupleHeaderXminFrozen(htup) && TransactionIdIsNormal(htup->t_choice.t_heap.t_xmin)) {
                Assert((uint32)(htup->t_choice.t_heap.t_xmin - delta) >= FirstNormalTransactionId);
                Assert((uint32)(htup->t_choice.t_heap.t_xmin - delta) <= MaxShortTransactionId);
                htup->t_choice.t_heap.t_xmin -= delta;
            }

            if (TransactionIdIsNormal(htup->t_choice.t_heap.t_xmax) && !(htup->t_infomask & HEAP_XMAX_IS_MULTI)) {
                Assert((uint32)(htup->t_choice.t_heap.t_xmax - delta) >= FirstNormalTransactionId);
                Assert((uint32)(htup->t_choice.t_heap.t_xmax - delta) <= MaxShortTransactionId);
                htup->t_choice.t_heap.t_xmax -= delta;
            }
        } else {
            if (TransactionIdIsNormal(htup->t_choice.t_heap.t_xmax) && (htup->t_infomask & HEAP_XMAX_IS_MULTI)) {
                Assert((uint32)(htup->t_choice.t_heap.t_xmax - delta) >= FirstNormalTransactionId);
                Assert((uint32)(htup->t_choice.t_heap.t_xmax - delta) <= MaxShortTransactionId);
                htup->t_choice.t_heap.t_xmax -= delta;
            }
        }
    }

    /* Apply xid shift to base as well */
    if (!multi) {
        phdr->pd_xid_base += delta;
    } else {
        phdr->pd_multi_base += delta;
    }

    ereport(DEBUG1, (errmsg("The page xid_base has changed to %lu ", phdr->pd_xid_base)));

    /* Write WAL record if needed */
    if (BufferIsValid(buffer)) {
        XLogRecPtr recptr;
        xl_heap_base_shift xlrec;

        START_CRIT_SECTION();
        MarkBufferDirty(buffer);

        xlrec.multi = multi;
        xlrec.delta = delta;

        XLogBeginInsert();
        XLogRegisterData((char *)&xlrec, SizeOfHeapBaseShift);

        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

        recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_BASE_SHIFT);

        PageSetLSN(page, recptr);

        END_CRIT_SECTION();
    }
}

/*
 * Freeze xids in the single heap page. Useful when we can't fit new xid even
 * with base shift.
 * @return: nfrozen - the number of tuples successfully frozen.
 */
static int freeze_single_heap_page(Relation relation, Buffer buffer)
{
    Page page = BufferGetPage(buffer);
    OffsetNumber offnum = InvalidOffsetNumber;
    OffsetNumber maxoff = InvalidOffsetNumber;
    HeapTupleData tuple;
    int nfrozen = 0;
    OffsetNumber frozen[MaxOffsetNumber];
    TransactionId latest_removed_xid = InvalidTransactionId;
    TransactionId oldest_xmin = InvalidTransactionId;
    TransactionId freeze_xid = InvalidTransactionId;
    bool useLocalSnapshot_change = false;

    gstrace_entry(GS_TRC_ID_freeze_single_heap_page);

    vacuum_set_xid_limits(relation, 0, 0, &oldest_xmin, &freeze_xid, NULL);
    /* since xid_base must be adjusted, heap_page_prune needs to be done,
     * so t_thrd.xact_cxt.useLocalSnapshot should be set to false
     */
    if (t_thrd.xact_cxt.useLocalSnapshot) {
        t_thrd.xact_cxt.useLocalSnapshot = false;
        useLocalSnapshot_change = true;
        if (TransactionIdIsNormal(t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin)) {
            oldest_xmin = t_thrd.xact_cxt.ShmemVariableCache->recentGlobalXmin;
            if (TransactionIdIsNormal(t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin) &&
                oldest_xmin > t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin) {
                oldest_xmin = t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin;
            }
        } else if (TransactionIdIsNormal(t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin)) {
            oldest_xmin = t_thrd.xact_cxt.ShmemVariableCache->recentLocalXmin;
        }

        if (oldest_xmin <= FirstNormalTransactionId + u_sess->attr.attr_storage.vacuum_defer_cleanup_age) {
            oldest_xmin = FirstNormalTransactionId;
        } else {
            oldest_xmin -= u_sess->attr.attr_storage.vacuum_defer_cleanup_age;
        }

        freeze_xid = oldest_xmin;
        ereport(
            LOG,
            (errmsg("Set useLocalSnapshot to false to force the prune page and then adjust the xid_base. relation is "
                    "\"%s\", oldest_xmin is %lu",
                    RelationGetRelationName(relation), oldest_xmin)));
    }

    (void)heap_page_prune(relation, buffer, oldest_xmin, false, &latest_removed_xid, false);

    if (useLocalSnapshot_change) {
        t_thrd.xact_cxt.useLocalSnapshot = true;
        useLocalSnapshot_change = false;
    }

    /*
     * Now scan the page to collect vacuumable items and check for tuples
     * requiring freezing.
     */
    maxoff = PageGetMaxOffsetNumber(page);

    /*
     * Note: If you change anything in the loop below, also look at
     * heap_page_is_all_visible to see if that needs to be changed.
     */
    for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId itemid = PageGetItemId(page, offnum);
        if (!ItemIdIsNormal(itemid)) {
            continue;
        }

        tuple.t_data = (HeapTupleHeader)PageGetItem(page, itemid);
        tuple.t_len = ItemIdGetLength(itemid);
        tuple.t_tableOid = RelationGetRelid(relation);
        tuple.t_bucketId = RelationGetBktid(relation);
        HeapTupleCopyBaseFromPage(&tuple, page);

        /*
         * Each non-removable tuple must be checked to see if it needs
         * freezing.  Note we already have exclusive buffer lock.
         */
        if (heap_freeze_tuple(&tuple, freeze_xid)) {
            frozen[nfrozen++] = offnum;
        }
    } /* scan along page */

    /*
     * If we froze any tuples, mark the buffer dirty, and write a WAL
     * record recording the changes.  We must log the changes to be
     * crash-safe against future truncation of CLOG.
     */
    if (nfrozen > 0) {
        START_CRIT_SECTION();

        MarkBufferDirty(buffer);
        /* Now WAL-log freezing if necessary */
        if (RelationNeedsWAL(relation)) {
            XLogRecPtr recptr = log_heap_freeze(relation, buffer, freeze_xid, frozen, nfrozen);
            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();
    }

    gstrace_exit(GS_TRC_ID_freeze_single_heap_page);
    return nfrozen;
}

/*
 * Ensure that given xid fits base of given page.
 */
bool heap_page_prepare_for_xid(Relation relation, Buffer buffer, TransactionId xid, bool multi, bool page_replication)
{
    Page page = BufferGetPage(buffer);
    HeapPageHeader phdr;
    TransactionId base = 0;
    bool found = false;
    ShortTransactionId min = 0;
    ShortTransactionId max = 0;
    int i;
    bool need_wal = false;

    gstrace_entry(GS_TRC_ID_heap_page_prepare_for_xid);

    need_wal = page_replication ? false : RelationNeedsWAL(relation);

    /*
     * if the first change to pd_xid_base or pd_multi_base fails ,
     * will attempt to freeze this page.
     */
    phdr = (HeapPageHeader)page;
    for (i = 0; i < 2; i++) {
        base = multi ? phdr->pd_multi_base : phdr->pd_xid_base;

        /* Can we already store this xid? */
        if (xid >= base + FirstNormalTransactionId && xid <= base + MaxShortTransactionId) {
            gstrace_exit(GS_TRC_ID_heap_page_prepare_for_xid);
            return false;
        }

        if (PageGetMaxOffsetNumber(page) == InvalidOffsetNumber && !PageIsCompressed(page) && !multi) {
            TransactionId xid_base = u_sess->utils_cxt.RecentXmin - FirstNormalTransactionId;
            ereport(LOG,
                (errmsg("new page, the xid base is not correct, base is %lu, reset the xid_base to %lu",
                base, xid_base)));
            phdr->pd_xid_base = xid_base;
            return false;
        }

        /* Find minimum and maximum xids in the page */
        found = heap_page_xid_min_max(page, multi, &min, &max);
        /* No items on the page? */
        if (!found) {
            int64 delta = xid - FirstNormalTransactionId;
            delta -= (multi ? phdr->pd_multi_base : phdr->pd_xid_base);

            HeapPageShiftBase(need_wal ? buffer : InvalidBuffer, page, multi, delta);
            MarkBufferDirty(buffer);
            gstrace_exit(GS_TRC_ID_heap_page_prepare_for_xid);
            return false;
        }

        /* Can we just shift base on the page */
        if (xid < base + FirstNormalTransactionId) {
            int64 free_delta = MaxShortTransactionId - max;
            int64 required_delta = (base + FirstNormalTransactionId) - xid;

            if (required_delta <= free_delta) {
                HeapPageShiftBase(need_wal ? buffer : InvalidBuffer, page, multi, -(free_delta + required_delta) / 2);
                MarkBufferDirty(buffer);
                gstrace_exit(GS_TRC_ID_heap_page_prepare_for_xid);
                return true;
            }
        } else {
            int64 free_delta = min - FirstNormalTransactionId;
            int64 required_delta = xid - (base + MaxShortTransactionId);

            if (required_delta <= free_delta) {
                HeapPageShiftBase(need_wal ? buffer : InvalidBuffer, page, multi, (free_delta + required_delta) / 2);
                MarkBufferDirty(buffer);
                gstrace_exit(GS_TRC_ID_heap_page_prepare_for_xid);
                return true;
            }
        }

        if (i == 1) {
            break;
        }

        /* Have to try freeing the page... */
        (void)freeze_single_heap_page(relation, buffer);
    }

    if (BufferIsValid(buffer)) {
        UnlockReleaseBuffer(buffer);
    }
    ereport(ERROR,
            (errcode(ERRCODE_CANNOT_MODIFY_XIDBASE),
             errmsg("Can't fit xid into page. relation \"%s\", now xid is %lu, base is %lu, min is %u, max is %u",
                    RelationGetRelationName(relation), xid, base, min, max)));
    gstrace_exit(GS_TRC_ID_heap_page_prepare_for_xid);
    return false;
}

/**
 * @Description: It is used to optimize xid_base adjustment, Freeze a page and readjust
 * xid_base to avoid the performance degradation during write transaction.
 * @in: Relation
 * @in: Buffer
 */
bool heap_change_xidbase_after_freeze(Relation relation, Buffer buffer)
{
    Page page = BufferGetPage(buffer);
    HeapPageHeader phdr = (HeapPageHeader)page;
    TransactionId base = phdr->pd_xid_base;
    TransactionId xid = u_sess->utils_cxt.RecentXmin;
    bool found = false;
    ShortTransactionId min = 0;
    ShortTransactionId max = 0;

    gstrace_entry(GS_TRC_ID_heap_change_xidbase_after_freeze);

    /* Find minimum and maximum xids in the page */
    found = heap_page_xid_min_max(page, false, &min, &max);
    /* No items on the page? */
    if (!found) {
        int64 delta;

        delta = (xid - FirstNormalTransactionId) - phdr->pd_xid_base;

        HeapPageShiftBase(RelationNeedsWAL(relation) ? buffer : InvalidBuffer, page, false, delta);
        MarkBufferDirty(buffer);
        gstrace_exit(GS_TRC_ID_heap_change_xidbase_after_freeze);
        return false;
    }

    if (u_sess->utils_cxt.RecentXmin < base + MaxShortTransactionId) {
        TransactionId xidmin = u_sess->utils_cxt.RecentXmin > min ? min : u_sess->utils_cxt.RecentXmin;
        HeapPageShiftBase(RelationNeedsWAL(relation) ? buffer : InvalidBuffer, page, false,
                          xidmin - FirstNormalTransactionId);
        MarkBufferDirty(buffer);
        gstrace_exit(GS_TRC_ID_heap_change_xidbase_after_freeze);
        return true;
    }

    int64 free_delta = min - FirstNormalTransactionId;
    int64 required_delta = xid - (base + MaxShortTransactionId);

    if (required_delta <= free_delta) {
        HeapPageShiftBase(RelationNeedsWAL(relation) ? buffer : InvalidBuffer, page, false,
                          (free_delta + required_delta) / 2);
        MarkBufferDirty(buffer);
        gstrace_exit(GS_TRC_ID_heap_change_xidbase_after_freeze);
        return true;
    }

    ereport(
        LOG,
        (errmsg("Can't fit RecentXmin into page after freeze. relation \"%s\", now xid is %lu, base is %lu, min is %u, "
                "max is %u",
                RelationGetRelationName(relation), xid, base, min, max)));
    gstrace_exit(GS_TRC_ID_heap_change_xidbase_after_freeze);
    return false;
}

/*
 * Ensure that given xid fits base of given page.
 */
bool rewrite_page_prepare_for_xid(Page page, TransactionId xid, bool multi)
{
    HeapPageHeader phdr = (HeapPageHeader)page;
    TransactionId base = 0;
    bool found = false;
    ShortTransactionId min = 0;
    ShortTransactionId max = 0;

    if (!TransactionIdIsNormal(xid)) {
        return false;
    }

    if (!multi) {
        base = phdr->pd_xid_base;
    } else {
        base = phdr->pd_multi_base;
    }

    /* Can we already store this xid? */
    if (xid >= base + FirstNormalTransactionId && xid <= base + MaxShortTransactionId) {
        return false;
    }

    /* Find minimum and maximum xids in the page */
    found = heap_page_xid_min_max(page, multi, &min, &max);
    /* No items on the page? */
    if (!found) {
        if (!multi) {
            phdr->pd_xid_base = xid - FirstNormalTransactionId;
        } else {
            phdr->pd_multi_base = xid - FirstNormalTransactionId;
        }
        return false;
    }
    ereport(DEBUG1, (errmsg("The minimum value of xid in TupleHeader is %u and the maximum value is %u", min, max)));

    /* Can we just shift base on the page */
    if (xid < base + FirstNormalTransactionId) {
        int64 free_delta = MaxShortTransactionId - max;
        int64 required_delta = (base + FirstNormalTransactionId) - xid;

        if (required_delta <= free_delta) {
            HeapPageShiftBase(InvalidBuffer, page, multi, -(free_delta + required_delta) / 2);
            return true;
        }
    } else {
        int64 free_delta = min - FirstNormalTransactionId;
        int64 required_delta = xid - (base + MaxShortTransactionId);

        if (required_delta <= free_delta) {
            HeapPageShiftBase(InvalidBuffer, page, multi, (free_delta + required_delta) / 2);
            return true;
        }
    }

    ereport(ERROR, (errcode(ERRCODE_CANNOT_MODIFY_XIDBASE),
                    errmsg("Can't fit xid into page, now xid is %lu, base is %lu, min is %u, max is %u", xid, base, min,
                           max)));
    return false;
}

/* ----------------
 *		heap_markpos	- mark scan position
 * ----------------
 */
void heap_markpos(TableScanDesc sscan)
{
    HeapScanDesc scan = (HeapScanDesc) sscan;

    /* Note: no locking manipulations needed */
    if (scan->rs_ctup.t_data != NULL) {
        scan->rs_mctid = scan->rs_ctup.t_self;
        if (scan->rs_base.rs_pageatatime) {
            scan->rs_mindex = scan->rs_base.rs_cindex;
        }
    } else
        ItemPointerSetInvalid(&scan->rs_mctid);
}

/*
 * Subroutine for heap_insert(). Prepares a tuple for insertion. This sets the
 * tuple header fields, assigns an OID, and toasts the tuple if necessary.
 * Returns a toasted version of the tuple if it was toasted, or the original
 * tuple if not. Note that in any case, the header fields are also set in
 * the original tuple.
 */
static HeapTuple heap_prepare_insert(Relation relation, HeapTuple tup, CommandId cid, int options)
{
    if (relation->rd_rel->relhasoids) {
#ifdef NOT_USED
        /* this is redundant with an Assert in HeapTupleSetOid */
        Assert(tup->t_data->t_infomask & HEAP_HASOID);
#endif

        /* For catalogs that do not support DDL, we set new object oids here. */
        if (u_sess->attr.attr_common.IsInplaceUpgrade && OidIsValid(u_sess->upg_cxt.Inplace_upgrade_next_general_oid)) {
            HeapTupleSetOid(tup, u_sess->upg_cxt.Inplace_upgrade_next_general_oid);
            u_sess->upg_cxt.Inplace_upgrade_next_general_oid = InvalidOid;
        }

        /*
         * If the object id of this tuple has already been assigned, trust the
         * caller.	There are a couple of ways this can happen.  At initial db
         * creation, the backend program sets oids for tuples. When we define
         * an index, we set the oid.  Finally, in the future, we may allow
         * users to set their own object ids in order to support a persistent
         * object store (objects need to contain pointers to one another).
         */
        if (!OidIsValid(HeapTupleGetOid(tup))) {
            HeapTupleSetOid(tup, GetNewOid(relation));
        }
    } else {
        /* check there is not space for an OID */
        Assert(!(tup->t_data->t_infomask & HEAP_HASOID));
    }

    tup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
    tup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
    tup->t_data->t_infomask |= HEAP_XMAX_INVALID;
    HeapTupleSetXmin(tup, InvalidTransactionId);
    if (options & HEAP_INSERT_FROZEN) {
        HeapTupleHeaderSetXminFrozen(tup->t_data);
    }

    HeapTupleHeaderSetCmin(tup->t_data, cid);
    HeapTupleSetXmax(tup, 0); /* for cleanliness */
    tup->t_tableOid = RelationGetRelid(relation);
    tup->t_bucketId = RelationGetBktid(relation);
#ifdef PGXC
    tup->t_xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
#endif

    if (RelationIsRedistributeDest(relation)) {
        HeapTupleHeaderSetRedisColumns(tup->t_data);
    }

    /*
     * If the new tuple is too big for storage or contains already toasted
     * out-of-line attributes from some other relation, invoke the toaster.
     */
    if (relation->rd_rel->relkind != RELKIND_RELATION &&
        relation->rd_rel->relkind != RELKIND_MATVIEW) {
        /* toast table entries should never be recursively toasted */
        Assert(!HeapTupleHasExternal(tup));
        return tup;
    } else if (HeapTupleHasExternal(tup) || tup->t_len > TOAST_TUPLE_THRESHOLD) {
        return toast_insert_or_update(relation, tup, NULL, options, NULL);
    } else {
        return tup;
    }
}

/*
 *	heap_multi_insert	- insert multiple tuple into a heap
 *
 * This is like heap_insert(), but inserts multiple tuples in one operation.
 * That's faster than calling heap_insert() in a loop, because when multiple
 * tuples can be inserted on a single page, we can write just a single WAL
 * record covering all of them, and only need to lock/unlock the page once.
 *
 * Note: this leaks memory into the current memory context. You can create a
 * temporary context before calling this, if that's a problem.
 */
int heap_multi_insert(Relation relation, Relation parent, HeapTuple *tuples, int ntuples, CommandId cid, int options,
                      BulkInsertState bistate, HeapMultiInsertExtraArgs *args)
{
    TransactionId xid = GetCurrentTransactionId();
    HeapTuple *heap_tuples = NULL;
    const char *cmprs_data = args->dictData;
    int cmpr_size = args->dictSize;
    int i;
    int ndone;
    char *scratch = NULL;
    Page page;
    bool needwal = false;
    bool is_compressed = (cmpr_size != 0);
    Size save_free_space;
    BlockNumber rel_end_block = InvalidBlockNumber;
    bool need_tuple_data = RelationIsLogicallyLogged(relation);
    bool need_cids = RelationIsAccessibleInLogicalDecoding(relation);

    /* 1. heap bcm-based data replication feature is enable
     * 2. caller doesn't forbid the feature
     * 3. normal process but not initing cluster
     * 4. normal relation but not catalog relation
     * 5. due to index has no visibility, so relation has index
     *    do not use data replication
     */
    bool page_replication = enable_heap_bcm_data_replication() && !args->disablePageReplication && !IsInitdb &&
                            (RelationGetRelid(relation) >= FirstNormalObjectId) && !RelationGetIndexNum(parent);
    errno_t rc = EOK;
    BlockNumber blockNum; /* blknum relative to begin of reln */

    CHECK_FOR_INTERRUPTS();

    if (page_replication) {
        /* We palloc BCMElementArray before START_CRIT_SECTION,
        if palloc failed, not core but ereport */
        PallocBCMBCMElementArray();
    }

    Assert(!is_compressed || (cmprs_data != NULL));
    needwal = !(options & HEAP_INSERT_SKIP_WAL) && RelationNeedsWAL(relation);

    if (RelationInClusterResizing(relation) && !RelationInClusterResizingReadOnly(relation)) {
        options |= HEAP_INSERT_SKIP_FSM;
        rel_end_block = RelationGetEndBlock(relation);
    }
    save_free_space = RelationGetTargetPageFreeSpace(relation, HEAP_DEFAULT_FILLFACTOR);

    /* Toast and set header data in all the tuples */
    heap_tuples = (HeapTupleData **)palloc(ntuples * sizeof(HeapTuple));
    for (i = 0; i < ntuples; i++)
        heap_tuples[i] = heap_prepare_insert(relation, tuples[i], cid, options);

    /*
     * Allocate some memory to use for constructing the WAL record. Using
     * palloc() within a critical section is not safe, so we allocate this
     * beforehand.
     */
    if (needwal) {
        scratch = (char *)palloc(BLCKSZ);
    }

    /*
     * We're about to do the actual inserts -- but check for conflict first,
     * to avoid possibly having to roll back work we've just done.
     *
     * For a heap insert, we only need to check for table-level SSI locks. Our
     * new tuple can't possibly conflict with existing tuple locks, and heap
     * page locks are only consolidated versions of tuple locks; they do not
     * lock "gaps" as index page locks do.	So we don't need to identify a
     * buffer before making the call.
     */
    CheckForSerializableConflictIn(relation, NULL, InvalidBuffer);

    ndone = 0;
    while (ndone < ntuples) {
        Buffer buffer = InvalidBuffer;
        Buffer vmbuffer = InvalidBuffer;
        bool all_visible_cleared = false;
        int nthispage;
        bool tmpPageReplication = false;

        if (GetDelayXlogRecycle()) {
            tmpPageReplication = false;
        } else {
            tmpPageReplication = page_replication;
        }

        if (tmpPageReplication) {
            (void)LWLockAcquire(RowPageReplicationLock, LW_SHARED);
            if (GetDelayXlogRecycle()) {
                tmpPageReplication = false;
                LWLockRelease(RowPageReplicationLock);
            }
        }

        /* IO collector and IO scheduler */
        if (ENABLE_WORKLOAD_CONTROL) {
            IOSchedulerAndUpdate(IO_TYPE_WRITE, 1, IO_TYPE_ROW);
        }

        if (is_compressed) {
            buffer = RelationGetNewBufferForBulkInsert(relation, heap_tuples[ndone]->t_len, cmpr_size, bistate);
            page = BufferGetPage(buffer);
            PageReinitWithDict(page, cmpr_size);
        } else {
            if (tmpPageReplication) {
                /* Get new page and exclusive */
                buffer = RelationGetNewBufferForBulkInsert(relation, heap_tuples[ndone]->t_len, cmpr_size, bistate);
            } else {
                /*
                 * Find buffer where at least the next tuple will fit.	If the page is
                 * all-visible, this will also pin the requisite visibility map page.
                 */
                buffer = RelationGetBufferForTuple(relation, heap_tuples[ndone]->t_len, InvalidBuffer, options, bistate,
                                                   &vmbuffer, NULL, rel_end_block);
            }
        }
        page = BufferGetPage(buffer);
        (void)heap_page_prepare_for_xid(relation, buffer, xid, false, page_replication);

        /* NO EREPORT(ERROR) from here till changes are logged */
        START_CRIT_SECTION();

        HeapTupleCopyBaseFromPage(heap_tuples[ndone], BufferGetPage(buffer));

        /* write Page Dictionary data before any tuple is written. */
        if (is_compressed) {
            rc = memcpy_s((char *)getPageDict(page), (Size)PageGetSpecialSize(page), cmprs_data, cmpr_size);
            securec_check(rc, "\0", "\0");
        }

        /*
         * RelationGetBufferForTuple has ensured that the first tuple fits.
         * Put that on the page, and then as many other tuples as fit.
         */
        RelationPutHeapTuple(relation, buffer, heap_tuples[ndone], xid);

        /* try to insert tuple into mlog-table. */
        if (relation != NULL && relation->rd_mlogoid != InvalidOid) {
            /* judge whether need to insert into mlog-table */
            insert_into_mlog_table(relation, relation->rd_mlogoid,
                                   heap_tuples[ndone], &heap_tuples[ndone]->t_self,
                                   GetCurrentTransactionId(), 'I');
        }

        for (nthispage = 1; ndone + nthispage < ntuples; nthispage++) {
            HeapTuple heaptup = heap_tuples[ndone + nthispage];

            if (PageGetHeapFreeSpace(page) < MAXALIGN(heaptup->t_len) + save_free_space) {
                break;
            }

            HeapTupleCopyBaseFromPage(heaptup, BufferGetPage(buffer));
            RelationPutHeapTuple(relation, buffer, heaptup, xid);

            /*
             * We don't use heap_multi_insert for catalog tuples yet, but
             * better be prepared...
             */
            if (needwal && need_cids) {
                (void)log_heap_new_cid(relation, heaptup);
            }

            /* try to insert tuple into mlog-table. */
            if (relation != NULL && relation->rd_mlogoid != InvalidOid) {
                /* judge whether need to insert into mlog-table */
                insert_into_mlog_table(relation, relation->rd_mlogoid, heaptup,
                                   &heaptup->t_self,
                                   GetCurrentTransactionId(), 'I');
            }
        }

        if (PageIsAllVisible(page)) {
            all_visible_cleared = true;
            PageClearAllVisible(page);
            visibilitymap_clear(relation, BufferGetBlockNumber(buffer), vmbuffer);
        }

        /*
         * XXX Should we set PageSetPrunable on this page ? See heap_insert()
         */
        MarkBufferDirty(buffer);

        /* XLOG stuff */
        if (needwal && !tmpPageReplication) {
            XLogRecPtr recptr;
            xl_heap_multi_insert *xlrec = NULL;
            uint8 info = XLOG_HEAP2_MULTI_INSERT;
            char *tuple_data = NULL;
            int total_data_len;
            char *scratchptr = scratch;
            bool init = false;
            int bufflags = 0;
            OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

            /*
             * If the page was previously empty, we can reinit the page
             * instead of restoring the whole thing. Moreover, if page is already
             * compressed, should not init page, or lead to inconsistency.
             */
            init = (ItemPointerGetOffsetNumber(&(heap_tuples[ndone]->t_self)) == FirstOffsetNumber &&
                    maxoff == FirstOffsetNumber + nthispage - 1 && (is_compressed || !PageIsCompressed(page)));

            /* allocate xl_heap_multi_insert struct from the scratch area */
            xlrec = (xl_heap_multi_insert *)scratchptr;
            scratchptr += SizeOfHeapMultiInsert;

            /*
             * Allocate offsets array. Unless we're reinitializing the page,
             * in that case the tuples are stored in order starting at
             * FirstOffsetNumber and we don't need to store the offsets
             * explicitly.
             */
            if (!init) {
                scratchptr += nthispage * sizeof(OffsetNumber);
            }

            /* the rest of the scratch space is used for tuple data */
            tuple_data = scratchptr;

            xlrec->flags = all_visible_cleared ? XLH_INSERT_ALL_VISIBLE_CLEARED : 0;
            xlrec->ntuples = nthispage;

            /* xlog: write the dictionary between header and tuples */
            xlrec->isCompressed = is_compressed;
            if (xlrec->isCompressed) {
                /* PageDictHeaderData should be 2B aligned. */
                char *cmprsMeta = (char *)SHORTALIGN(scratchptr);
                /* first write the size of this compression meta area. */
                *((int16 *)cmprsMeta) = cmpr_size;
                cmprsMeta += sizeof(int16);
                /* then copy all the compression data. */
                rc = memcpy_s(cmprsMeta, cmpr_size, cmprs_data, cmpr_size);
                securec_check(rc, "\0", "\0");
                scratchptr = cmprsMeta + cmpr_size;
            }

            /*
             * Write out an xl_multi_insert_tuple and the tuple data itself
             * for each tuple.
             */
            for (i = 0; i < nthispage; i++) {
                HeapTuple heaptup = heap_tuples[ndone + i];
                xl_multi_insert_tuple *tuphdr = NULL;
                int datalen;

                if (!init) {
                    xlrec->offsets[i] = ItemPointerGetOffsetNumber(&heaptup->t_self);
                }
                /* xl_multi_insert_tuple needs two-byte alignment. */
                tuphdr = (xl_multi_insert_tuple *)SHORTALIGN(scratchptr);
                scratchptr = ((char *)tuphdr) + SizeOfMultiInsertTuple;

                tuphdr->t_infomask2 = heaptup->t_data->t_infomask2;
                tuphdr->t_infomask = heaptup->t_data->t_infomask;
                tuphdr->t_hoff = heaptup->t_data->t_hoff;

                /* write bitmap [+ padding] [+ oid] + data */
                datalen = heaptup->t_len - offsetof(HeapTupleHeaderData, t_bits);
                rc = memcpy_s(scratchptr, BLCKSZ, (char *)heaptup->t_data + offsetof(HeapTupleHeaderData, t_bits),
                              datalen);
                securec_check(rc, "\0", "\0");

                tuphdr->datalen = datalen;
                scratchptr += datalen;
            }
            total_data_len = scratchptr - tuple_data;
            Assert((scratchptr - scratch) < BLCKSZ);

            if (need_tuple_data) {
                xlrec->flags |= XLH_INSERT_CONTAINS_NEW_TUPLE;
            }

            /*
             * Signal that this is the last xl_heap_multi_insert record
             * emitted by this call to heap_multi_insert(). Needed for logical
             * decoding so it knows when to cleanup temporary data.
             */
            if (ndone + nthispage == ntuples) {
                xlrec->flags |= XLH_INSERT_LAST_IN_MULTI;
            }

            /*
             * If we're going to reinitialize the whole page using the WAL
             * record, hide buffer reference from XLogInsert.
             */
            if (init) {
                info |= XLOG_HEAP_INIT_PAGE;
                bufflags |= REGBUF_WILL_INIT;
            }

            /*
             * If we're doing logical decoding, include the new tuple data
             * even if we take a full-page image of the page.
             */
            if (need_tuple_data) {
                bufflags |= REGBUF_KEEP_DATA;
            }

            XLogBeginInsert();
            if (info & XLOG_HEAP_INIT_PAGE) {
                XLogRegisterData((char *)&((HeapPageHeader)(page))->pd_xid_base, sizeof(TransactionId));
            }
            XLogRegisterData((char *)xlrec, tuple_data - scratch);
            XLogRegisterBuffer(0, buffer, REGBUF_STANDARD | bufflags);

            XLogRegisterBufData(0, tuple_data, total_data_len);

            /* filtering by origin on a row level is much more efficient */
            XLogIncludeOrigin();

            recptr = XLogInsert(RM_HEAP2_ID, info);

            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();

        if (RelationNeedsWAL(relation) && tmpPageReplication) {
            BlockNumber blkno = BufferGetBlockNumber(buffer);
            log_logical_newpage(&relation->rd_node, MAIN_FORKNUM, blkno, page, buffer);

            PushHeapPageToDataQueue(buffer);
        }

        blockNum = BufferGetBlockNumber(buffer);

        UnlockReleaseBuffer(buffer);
        if (vmbuffer != InvalidBuffer) {
            ReleaseBuffer(vmbuffer);
        }

        /* Set BCM status */
        if (RelationNeedsWAL(relation) && tmpPageReplication) {
            Buffer bcmbuffer = InvalidBuffer;

            BCM_pin(relation, blockNum, &bcmbuffer);
            LockBuffer(bcmbuffer, BUFFER_LOCK_EXCLUSIVE);
            BCMSetStatusBit(relation, blockNum, bcmbuffer, NOTSYNCED);
            UnlockReleaseBuffer(bcmbuffer);
        }

        ndone += nthispage;

        if (tmpPageReplication) {
            LWLockRelease(RowPageReplicationLock);
        }

        if (is_compressed) {
            break;
        }
    }

    /*
     * If tuples are cachable, mark them for invalidation from the caches in
     * case we abort.  Note it is OK to do this after releasing the buffer,
     * because the heap_tuples data structure is all in local memory, not in
     * the shared buffer.
     */
    if (IsSystemRelation(relation)) {
        for (i = 0; i < ndone; i++)
            CacheInvalidateHeapTuple(relation, heap_tuples[i], NULL);
    }

    /*
     * Copy t_self fields back to the caller's original tuples. This does
     * nothing for untoasted tuples (tuples[i] == heap_tuples[i)], but it's
     * probably faster to always copy than check.
     */
    for (i = 0; i < ndone; i++) {
        tuples[i]->t_self = heap_tuples[i]->t_self;
    }

    pgstat_count_heap_insert(relation, ndone);

    return ndone;
}

/*
 *	simple_heap_insert - insert a tuple
 *
 * Currently, this routine differs from heap_insert only in supplying
 * a default command ID and not allowing access to the speedup options.
 *
 * This should be used rather than using heap_insert directly in most places
 * where we are modifying system catalogs.
 */
Oid simple_heap_insert(Relation relation, HeapTuple tup)
{
    return tableam_tuple_insert(relation, tup, GetCurrentCommandId(true), 0, NULL);
}

/*
 * heap_delete - delete a tuple
 *
 * NB: do not call this directly unless you are prepared to deal with
 * concurrent-update conditions.  Use simple_heap_delete instead.
 *
 * relation - table to be modified (caller must hold suitable lock)
 * tid - TID of tuple to be deleted
 * ctid - output parameter, used only for failure case (see below)
 * update_xmax - output parameter, used only for failure case (see below)
 * update_xmin - output parameter, use only for output xmin.
 * cid - delete command ID (used for visibility test, and stored into
 *     cmax if successful)
 * crosscheck - if not InvalidSnapshot, also check tuple against this
 * wait - true if should wait for any conflicting update to commit/abort
 *
 * Normal, successful return value is HeapTupleMayBeUpdated, which
 * actually means we did delete it.  Failure return codes are
 * HeapTupleSelfUpdated, HeapTupleUpdated, or HeapTupleBeingUpdated
 * (the last only possible if wait == false).
 *
 * In the failure cases, the routine returns the tuple's t_ctid and t_xmax.
 * If t_ctid is the same as tid, the tuple was deleted; if different, the
 * tuple was updated, and t_ctid is the location of the replacement tuplemin.
 * (t_xmax is needed to verify that the replacement tuple matches.)
 */
TM_Result heap_delete(Relation relation, ItemPointer tid, CommandId cid,
    Snapshot crosscheck, bool wait, TM_FailureData *tmfd, bool allow_delete_self)
{
    TM_Result result;
    TransactionId xid = GetCurrentTransactionId();
    ItemId lp;
    HeapTupleData tp;
    Page page;
    BlockNumber block;
    Buffer buffer;
    Buffer vmbuffer = InvalidBuffer;
    bool have_tuple_lock = false;
    bool is_combo = false;
    bool all_visible_cleared = false;
    OffsetNumber maxoff;
    HeapTuple old_key_tuple = NULL; /* replica identity of the tuple */
    bool old_key_copied = false;

    Assert(ItemPointerIsValid(tid));

    /* Don't allow any write/lock operator in stream. */
    Assert(!StreamThreadAmI());

    block = ItemPointerGetBlockNumber(tid);
    buffer = ReadBuffer(relation, block);
    page = BufferGetPage(buffer);
    /*
     * Before locking the buffer, pin the visibility map page if it appears to
     * be necessary.  Since we haven't got the lock yet, someone else might be
     * in the middle of changing this, so we'll need to recheck after we have
     * the lock.
     */
    if (PageIsAllVisible(page)) {
        visibilitymap_pin(relation, block, &vmbuffer);
    }

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    if (PageIs4BXidVersion(page)) {
        (void)heap_page_upgrade(relation, buffer);
    }

    /*
     * If we didn't pin the visibility map page and the page has become all
     * visible while we were busy locking the buffer, we'll have to unlock and
     * re-lock, to avoid holding the buffer lock across an I/O.  That's a bit
     * unfortunate, but hopefully shouldn't happen often.
     */
    if (vmbuffer == InvalidBuffer && PageIsAllVisible(page)) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        visibilitymap_pin(relation, block, &vmbuffer);
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    }

    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
    maxoff = PageGetMaxOffsetNumber(page);
    /* check tid */
    if (maxoff < ItemPointerGetOffsetNumber(tid) || !ItemIdIsNormal(lp) || !ItemPointerIsValid(tid)) {
        ereport(PANIC,
                (errmsg("heap_delete: invalid tid %hu, max tid %hu, rnode[%u,%u,%u], block %u", tid->ip_posid, maxoff,
                        relation->rd_node.spcNode, relation->rd_node.dbNode, relation->rd_node.relNode, block)));
    }
    tp.t_tableOid = RelationGetRelid(relation);
    tp.t_bucketId = RelationGetBktid(relation);
    tp.t_data = (HeapTupleHeader)PageGetItem(page, lp);
    tp.t_len = ItemIdGetLength(lp);
    tp.t_self = *tid;
    HeapTupleCopyBaseFromPage(&tp, page);
    tmfd->xmin = HeapTupleHeaderGetXmin(page, tp.t_data);

l1:
    result = HeapTupleSatisfiesUpdate(&tp, cid, buffer, allow_delete_self);

    if (result == TM_Invisible) {
        UnlockReleaseBuffer(buffer);
        ereport(defence_errlevel(), (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("attempted to delete invisible tuple")));
    } else if (result == TM_SelfCreated) {
        UnlockReleaseBuffer(buffer);
        /* if allow self delete, HeapTupleSelfCreated status will never be reached */
        Assert(!allow_delete_self);
        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("attempted to delete self created tuple")));
    } else if (result == TM_BeingModified && wait) {
        TransactionId xwait;
        uint16 infomask;

        /* must copy state data before unlocking buffer */
        HeapTupleCopyBaseFromPage(&tp, BufferGetPage(buffer));
        xwait = HeapTupleGetRawXmax(&tp);
        infomask = tp.t_data->t_infomask;

        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

        if (!u_sess->attr.attr_common.allow_concurrent_tuple_update) {
            ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("abort transaction due to concurrent update")));
        }

        /*
         * Acquire tuple lock to establish our priority for the tuple (see
         * heap_lock_tuple).  LockTuple will release us when we are
         * next-in-line for the tuple.
         *
         * If we are forced to "start over" below, we keep the tuple lock;
         * this arranges that we stay at the head of the line while rechecking
         * tuple state.
         */
        if (!have_tuple_lock) {
            LockTuple(relation, &(tp.t_self), ExclusiveLock, true);
            have_tuple_lock = true;
        }

        /*
         * Sleep until concurrent transaction ends.  Note that we don't care
         * if the locker has an exclusive or shared lock, because we need
         * exclusive.
         */
        if (infomask & HEAP_XMAX_IS_MULTI) {
            /* wait for multixact */
            MultiXactIdWait((MultiXactId)xwait, true);
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

            /*
             * If xwait had just locked the tuple then some other xact could
             * update this tuple before we get to this point.  Check for xmax
             * change, and start over if so.
             */
            if (!(tp.t_data->t_infomask & HEAP_XMAX_IS_MULTI) ||
                !TransactionIdEquals(HeapTupleGetRawXmax(&tp), xwait)) {
                goto l1;
            }

            /*
             * You might think the multixact is necessarily done here, but not
             * so: it could have surviving members, namely our own xact or
             * other subxacts of this backend.	It is legal for us to delete
             * the tuple in either case, however (the latter case is
             * essentially a situation of upgrading our former shared lock to
             * exclusive).	We don't bother changing the on-disk hint bits
             * since we are about to overwrite the xmax altogether.
             */
        } else {
            /* wait for regular transaction to end */
            XactLockTableWait(xwait, true);
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

            /*
             * xwait is done, but if xwait had just locked the tuple then some
             * other xact could update this tuple before we get to this point.
             * Check for xmax change, and start over if so.
             */
            if ((tp.t_data->t_infomask & HEAP_XMAX_IS_MULTI) || !TransactionIdEquals(HeapTupleGetRawXmax(&tp), xwait)) {
                goto l1;
            }

            /* Otherwise check if it committed or aborted */
            UpdateXmaxHintBits(tp.t_data, buffer, xwait);
        }

        /*
         * We may overwrite if previous xmax aborted, or if it committed but
         * only locked the tuple without updating it.
         */
        if (tp.t_data->t_infomask & (HEAP_XMAX_INVALID | HEAP_IS_LOCKED)) {
            result = TM_Ok;
        } else if (!ItemPointerEquals(&tp.t_self, &tp.t_data->t_ctid)) {
            result = TM_Updated;
        } else {
            result = TM_Deleted;
        }
    }

    if (crosscheck != InvalidSnapshot && result == TM_Ok) {
        /* Perform additional check for transaction-snapshot mode RI updates */
        if (!HeapTupleSatisfiesVisibility(&tp, crosscheck, buffer)) {
            result = TM_Updated;
        }
    }

    if (result != TM_Ok) {
        Assert(result == TM_SelfModified || result == TM_Updated || 
            result == TM_Deleted || result == TM_BeingModified);
        Assert(!(tp.t_data->t_infomask & HEAP_XMAX_INVALID));
        Assert(result != TM_Updated ||
            !ItemPointerEquals(&tp.t_self, &tp.t_data->t_ctid));
        tmfd->ctid = tp.t_data->t_ctid;
        tmfd->xmax = HeapTupleGetRawXmax(&tp);
        if (result == TM_SelfModified) {
            tmfd->cmax = HeapTupleHeaderGetCmax(tp.t_data, page);
        } else {
            tmfd->cmax = InvalidCommandId;
        }
        UnlockReleaseBuffer(buffer);
        if (have_tuple_lock) {
            UnlockTuple(relation, &(tp.t_self), ExclusiveLock);
        }
        if (vmbuffer != InvalidBuffer) {
            ReleaseBuffer(vmbuffer);
        }
        return result;
    }

    /*
     * We're about to do the actual delete -- check for conflict first, to
     * avoid possibly having to roll back work we've just done.
     */
    CheckForSerializableConflictIn(relation, &tp, buffer);

    /* replace cid with a combo cid if necessary */
    HeapTupleHeaderAdjustCmax(tp.t_data, &cid, &is_combo, buffer);

    (void)heap_page_prepare_for_xid(relation, buffer, xid, false);

    HeapTupleCopyBaseFromPage(&tp, page);
    /*
     * Compute replica identity tuple before entering the critical section so
     * we don't PANIC upon a memory allocation failure.
     */
    old_key_tuple = ExtractReplicaIdentity(relation, &tp, true, &old_key_copied);

    START_CRIT_SECTION();

    /*
     * If this transaction commits, the tuple will become DEAD sooner or
     * later.  Set flag that this page is a candidate for pruning once our xid
     * falls below the oldest_xmin horizon.	If the transaction finally aborts,
     * the subsequent page pruning will be a no-op and the hint will be
     * cleared.
     */
    PageSetPrunable(page, xid);

    if (PageIsAllVisible(page)) {
        all_visible_cleared = true;
        PageClearAllVisible(page);
        visibilitymap_clear(relation, BufferGetBlockNumber(buffer), vmbuffer);
    }

    /* store transaction information of xact deleting the tuple */
    tp.t_data->t_infomask &= ~(HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI | HEAP_IS_LOCKED |
                               HEAP_MOVED);
    HeapTupleHeaderClearHotUpdated(tp.t_data);
    HeapTupleHeaderSetXmax(page, tp.t_data, xid);
    HeapTupleHeaderSetCmax(tp.t_data, cid, is_combo);

    /* Make sure there is no forward chain link in t_ctid */
    tp.t_data->t_ctid = tp.t_self;

    MarkBufferDirty(buffer);

    /* XLOG stuff */
    if (RelationNeedsWAL(relation)) {
        xl_heap_delete xlrec;
        XLogRecPtr recptr;
        xl_heap_header xlhdr;

        /* For logical decode we need combocids to properly decode the catalog */
        if (RelationIsAccessibleInLogicalDecoding(relation)) {
            (void)log_heap_new_cid(relation, &tp);
        }

        xlrec.flags = all_visible_cleared ? XLH_DELETE_ALL_VISIBLE_CLEARED : 0;
        xlrec.offnum = ItemPointerGetOffsetNumber(&tp.t_self);

        if (old_key_tuple != NULL) {
            bool is_null = false;
            char relreplident;
            Relation rel = heap_open(RelationRelationId, AccessShareLock);
            Oid relid = RelationIsPartition(relation) ? relation->parentId : relation->rd_id;
            HeapTuple tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
            if (!HeapTupleIsValid(tuple)) {
                ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                                errmsg("pg_class entry for relid %u vanished during ExtractReplicaIdentity", relid)));
            }
            Datum replident = heap_getattr(tuple, Anum_pg_class_relreplident, RelationGetDescr(rel), &is_null);
            heap_close(rel, AccessShareLock);

            if (is_null) {
                relreplident = REPLICA_IDENTITY_NOTHING;
            } else {
                relreplident = CharGetDatum(replident);
            }

            if (relreplident == REPLICA_IDENTITY_FULL) {
                xlrec.flags |= XLH_DELETE_CONTAINS_OLD_TUPLE;
            } else {
                xlrec.flags |= XLH_DELETE_CONTAINS_OLD_KEY;
            }
        }

        XLogBeginInsert();
        XLogRegisterData((char *)&xlrec, SizeOfHeapDelete);
        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

        /*
         * Log replica identity of the deleted tuple if there is one
         */
        if (old_key_tuple != NULL) {
            xlhdr.t_infomask2 = old_key_tuple->t_data->t_infomask2;
            xlhdr.t_infomask = old_key_tuple->t_data->t_infomask;
            xlhdr.t_hoff = old_key_tuple->t_data->t_hoff;

            XLogRegisterData((char *)&xlhdr, SizeOfHeapHeader);
            XLogRegisterData((char *)old_key_tuple->t_data + offsetof(HeapTupleHeaderData, t_bits),
                             old_key_tuple->t_len - offsetof(HeapTupleHeaderData, t_bits));
        }

        /* filtering by origin on a row level is much more efficient */
        XLogIncludeOrigin();

        recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_DELETE);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    if (vmbuffer != InvalidBuffer) {
        ReleaseBuffer(vmbuffer);
    }

    /*
     * If the tuple has toasted out-of-line attributes, we need to delete
     * those items too.  We have to do this before releasing the buffer
     * because we need to look at the contents of the tuple, but it's OK to
     * release the content lock on the buffer first.
     */
    if (relation->rd_rel->relkind != RELKIND_RELATION &&
        relation->rd_rel->relkind != RELKIND_MATVIEW) {
        /* toast table entries should never be recursively toasted */
        Assert(!HeapTupleHasExternal(&tp));
    } else if (HeapTupleHasExternal(&tp)) {
        toast_delete(relation, &tp, allow_delete_self ? HEAP_INSERT_SPECULATIVE : 0);
	}

    /*
     * Mark tuple for invalidation from system caches at next command
     * boundary. We have to do this before releasing the buffer because we
     * need to look at the contents of the tuple.
     */
    CacheInvalidateHeapTuple(relation, &tp, NULL);

    /* Now we can release the buffer */
    ReleaseBuffer(buffer);

    /*
     * Release the lmgr tuple lock, if we had it.
     */
    if (have_tuple_lock) {
        UnlockTuple(relation, &(tp.t_self), ExclusiveLock);
    }

    pgstat_count_heap_delete(relation);

    if (old_key_tuple != NULL && old_key_copied) {
        heap_freetuple(old_key_tuple);
    }

    return TM_Ok;
}

/*
 *	simple_heap_delete - delete a tuple
 *
 * This routine may be used to delete a tuple when concurrent updates of
 * the target tuple are not expected (for example, because we have a lock
 * on the relation associated with the tuple).	Any failure is reported
 * via ereport().
 */
void simple_heap_delete(Relation relation, ItemPointer tid, int options)
{
    TM_Result result;
    TM_FailureData tmfd;
    bool allow_delete_self = (options & HEAP_INSERT_SPECULATIVE) ? true : false;

    result = tableam_tuple_delete(relation,
        tid,
        GetCurrentCommandId(true),
        InvalidSnapshot,
        InvalidSnapshot,
        true, /* wait for commit */
        &tmfd,
        allow_delete_self);
    switch (result) {
        case TM_SelfModified:
            /* Tuple was already updated in current command? */
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("tuple already updated by self")));
            break;

        case TM_Ok:
            /* done successfully */
            break;

        case TM_Updated:
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("tuple concurrently updated")));
            break;

        case TM_Deleted:
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("tuple concurrently deleted")));
            break;

        default:
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("unrecognized heap_delete status: %u", result)));
            break;
    }
}

/*
 *	heap_update - replace a tuple
 *
 * NB: do not call this directly unless you are prepared to deal with
 * concurrent-update conditions.  Use simple_heap_update instead.
 *
 *	relation - table to be modified (caller must hold suitable lock)
 *	otid - TID of old tuple to be replaced
 *	newtup - newly constructed tuple data to store
 *	ctid - output parameter, used only for failure case (see below)
 *	update_xmax - output parameter, used only for failure case (see below)
 *	cid - update command ID (used for visibility test, and stored into
 *		cmax/cmin if successful)
 *	crosscheck - if not InvalidSnapshot, also check old tuple against this
 *	wait - true if should wait for any conflicting update to commit/abort
 *
 * Normal, successful return value is HeapTupleMayBeUpdated, which
 * actually means we *did* update it.  Failure return codes are
 * HeapTupleSelfUpdated, HeapTupleUpdated, or HeapTupleBeingUpdated
 * (the last only possible if wait == false).
 *
 * On success, the header fields of *newtup are updated to match the new
 * stored tuple; in particular, newtup->t_self is set to the TID where the
 * new tuple was inserted, and its HEAP_ONLY_TUPLE flag is set iff a HOT
 * update was done.  However, any TOAST changes in the new tuple's
 * data are not reflected into *newtup.
 *
 * In the failure cases, the routine returns the tuple's t_ctid and t_xmax.
 * If t_ctid is the same as otid, the tuple was deleted; if different, the
 * tuple was updated, and t_ctid is the location of the replacement tuple.
 * (t_xmax is needed to verify that the replacement tuple matches.)
 */
TM_Result heap_update(Relation relation, Relation parentRelation, ItemPointer otid, HeapTuple newtup,
    CommandId cid, Snapshot crosscheck, bool wait, TM_FailureData *tmfd, bool allow_update_self)
{
    TM_Result result;
    TransactionId xid = GetCurrentTransactionId();
    Bitmapset *hot_attrs = NULL;
    Bitmapset *id_attrs = NULL;
    ItemId lp;
    HeapTupleData oldtup;
    HeapTuple heaptup;
    HeapTuple old_key_tuple = NULL;
    bool old_key_copied = false;
    Page page, newpage;
    BlockNumber block;
    Buffer buffer = InvalidBuffer;
    Buffer newbuf = InvalidBuffer;
    Buffer vmbuffer = InvalidBuffer;
    Buffer vmbuffer_new = InvalidBuffer;
    bool need_toast = false;
    bool already_marked = false;
    Size new_tup_size, pagefree;
    bool have_tuple_lock = false;
    bool is_combo = false;
    bool satisfies_hot = false;
    bool satisfies_id = false;
    bool use_hot_update = false;
    bool all_visible_cleared = false;
    bool all_visible_cleared_new = false;
    int options = 0;
    bool rel_in_redis = RelationInClusterResizing(relation);
    OffsetNumber maxoff;
    BlockNumber rel_end_block = InvalidBlockNumber;
    Assert(ItemPointerIsValid(otid));

    /* Don't allow any write/lock operator in stream. */
    Assert(!StreamThreadAmI());

    /*
     * Fetch the list of attributes to be checked for HOT update.  This is
     * wasted effort if we fail to update or have to put the new tuple on a
     * different page.	But we must compute the list before obtaining buffer
     * lock --- in the worst case, if we are doing an update on one of the
     * relevant system catalogs, we could deadlock if we try to fetch the list
     * later.  In any case, the relcache caches the data so this is usually
     * pretty cheap.
     *
     * Note that we get a copy here, so we need not worry about relcache flush
     * happening midway through.
     */
    if (parentRelation != NULL) {
        /*
         * For partitioned table , we use the parent relation to calc hot_attrs.
         */
        Assert(RELATION_IS_PARTITIONED(parentRelation) || RELATION_OWN_BUCKET(parentRelation));
        hot_attrs = RelationGetIndexAttrBitmap(parentRelation, INDEX_ATTR_BITMAP_ALL);
        id_attrs = RelationGetIndexAttrBitmap(parentRelation, INDEX_ATTR_BITMAP_IDENTITY_KEY);
    } else {
        hot_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_ALL);
        id_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_IDENTITY_KEY);
    }

    block = ItemPointerGetBlockNumber(otid);
    buffer = ReadBuffer(relation, block);
    page = BufferGetPage(buffer);
    /*
     * Before locking the buffer, pin the visibility map page if it appears to
     * be necessary.  Since we haven't got the lock yet, someone else might be
     * in the middle of changing this, so we'll need to recheck after we have
     * the lock.
     */
    if (PageIsAllVisible(page)) {
        visibilitymap_pin(relation, block, &vmbuffer);
    }

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    if (PageIs4BXidVersion(page)) {
        (void)heap_page_upgrade(relation, buffer);
    }

    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(otid));
    maxoff = PageGetMaxOffsetNumber(page);
    /* check otid */
    if (maxoff < ItemPointerGetOffsetNumber(otid) || !ItemIdIsNormal(lp) || !ItemPointerIsValid(otid)) {
        ereport(PANIC,
                (errmsg("heap_update: invalid tid %hu, max tid %hu, rnode[%u,%u,%u], block %u", otid->ip_posid, maxoff,
                        relation->rd_node.spcNode, relation->rd_node.dbNode, relation->rd_node.relNode, block)));
    }
    /*
     * Note: beyond this point, use oldtup not otid to refer to old tuple.
     * otid may very well point at newtup->t_self, which we will overwrite
     * with the new tuple's location, so there's great risk of confusion if we
     * use otid anymore.
     */
    oldtup.t_data = (HeapTupleHeader)PageGetItem(page, lp);
    oldtup.t_len = ItemIdGetLength(lp);
    oldtup.t_self = *otid;
    oldtup.t_tableOid = RelationGetRelid(relation);
    oldtup.t_bucketId = RelationGetBktid(relation);
    HeapSatisfiesHOTUpdate(relation, hot_attrs, id_attrs, &satisfies_hot, &satisfies_id, &oldtup, newtup, page);
    tmfd->xmin = HeapTupleHeaderGetXmin(page, oldtup.t_data);

l2:
    HeapTupleCopyBaseFromPage(&oldtup, BufferGetPage(buffer));
    result = HeapTupleSatisfiesUpdate(&oldtup, cid, buffer, allow_update_self);
    if (result == TM_Invisible) {
        UnlockReleaseBuffer(buffer);
        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("attempted to update invisible tuple")));
    } else if (result == TM_SelfCreated) {
        UnlockReleaseBuffer(buffer);
        /* if allow self update, HeapTupleSelfCreated status will never be reached */
        Assert(!allow_update_self);
        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("attempted to update self created tuple")));
    } else if (result == TM_BeingModified && wait) {
        TransactionId xwait;
        uint16 infomask;

        /* must copy state data before unlocking buffer */
        HeapTupleCopyBaseFromPage(&oldtup, BufferGetPage(buffer));
        xwait = HeapTupleGetRawXmax(&oldtup);
        infomask = oldtup.t_data->t_infomask;

        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

        if (!u_sess->attr.attr_common.allow_concurrent_tuple_update) {
            ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("abort transaction due to concurrent update")));
        }

        /*
         * Acquire tuple lock to establish our priority for the tuple (see
         * heap_lock_tuple).  LockTuple will release us when we are
         * next-in-line for the tuple.
         *
         * If we are forced to "start over" below, we keep the tuple lock;
         * this arranges that we stay at the head of the line while rechecking
         * tuple state.
         */
        if (!have_tuple_lock) {
            LockTuple(relation, &(oldtup.t_self), ExclusiveLock, true);
            have_tuple_lock = true;
        }

        /*
         * Sleep until concurrent transaction ends.  Note that we don't care
         * if the locker has an exclusive or shared lock, because we need
         * exclusive.
         */
        if (infomask & HEAP_XMAX_IS_MULTI) {
            /* wait for multixact */
            MultiXactIdWait((MultiXactId)xwait, true);
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

            /*
             * If xwait had just locked the tuple then some other xact could
             * update this tuple before we get to this point.  Check for xmax
             * change, and start over if so.
             */
            if (!(oldtup.t_data->t_infomask & HEAP_XMAX_IS_MULTI) ||
                !TransactionIdEquals(HeapTupleGetRawXmax(&oldtup), xwait)) {
                goto l2;
            }

            /*
             * You might think the multixact is necessarily done here, but not
             * so: it could have surviving members, namely our own xact or
             * other subxacts of this backend.	It is legal for us to update
             * the tuple in either case, however (the latter case is
             * essentially a situation of upgrading our former shared lock to
             * exclusive).	We don't bother changing the on-disk hint bits
             * since we are about to overwrite the xmax altogether.
             */
        } else {
            /* wait for regular transaction to end */
            XactLockTableWait(xwait, true);
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

            /*
             * xwait is done, but if xwait had just locked the tuple then some
             * other xact could update this tuple before we get to this point.
             * Check for xmax change, and start over if so.
             */
            if ((oldtup.t_data->t_infomask & HEAP_XMAX_IS_MULTI) ||
                !TransactionIdEquals(HeapTupleGetRawXmax(&oldtup), xwait)) {
                goto l2;
            }

            /* Otherwise check if it committed or aborted */
            UpdateXmaxHintBits(oldtup.t_data, buffer, xwait);
        }

        /*
         * We may overwrite if previous xmax aborted, or if it committed but
         * only locked the tuple without updating it.
         */
        if (oldtup.t_data->t_infomask & (HEAP_XMAX_INVALID | HEAP_IS_LOCKED)) {
            result = TM_Ok;
            ereport(DEBUG1,
                    (errmsg("heap maybe updated ctid (%u,%d) cur_xid "
                            "%lu xmin %lu xmax %lu infomask %hu",
                            ItemPointerGetBlockNumber(&oldtup.t_self), ItemPointerGetOffsetNumber(&oldtup.t_self),
                            GetCurrentTransactionIdIfAny(), HeapTupleHeaderGetXmin(page, oldtup.t_data),
                            HeapTupleHeaderGetXmax(page, oldtup.t_data), oldtup.t_data->t_infomask)));
        } else if (!ItemPointerEquals(&oldtup.t_self, &oldtup.t_data->t_ctid)) {
            result = TM_Updated;
        } else {
            result = TM_Deleted;
        }
    }

    if (crosscheck != InvalidSnapshot && result == TM_Ok) {
        /* Perform additional check for transaction-snapshot mode RI updates */
        if (!HeapTupleSatisfiesVisibility(&oldtup, crosscheck, buffer)) {
            result = TM_Updated;
            Assert(!ItemPointerEquals(&oldtup.t_self, &oldtup.t_data->t_ctid));
        }
    }

    if (result != TM_Ok) {
        Assert(result == TM_SelfModified || result == TM_Updated 
            || result == TM_Deleted || result == TM_BeingModified);
        Assert(!(oldtup.t_data->t_infomask & HEAP_XMAX_INVALID));
        Assert(result != TM_Updated ||
            !ItemPointerEquals(&oldtup.t_self, &oldtup.t_data->t_ctid));
        tmfd->ctid = oldtup.t_data->t_ctid;
        tmfd->xmax = HeapTupleGetRawXmax(&oldtup);
        if (result == TM_SelfModified) {
            tmfd->cmax = HeapTupleHeaderGetCmax(oldtup.t_data, page);
        } else {
            tmfd->cmax = InvalidCommandId;
        }
        UnlockReleaseBuffer(buffer);
        if (have_tuple_lock) {
            UnlockTuple(relation, &(oldtup.t_self), ExclusiveLock);
        }
        if (vmbuffer != InvalidBuffer) {
            ReleaseBuffer(vmbuffer);
        }
        bms_free(hot_attrs);
        bms_free(id_attrs);
        return result;
    }

    /*
     * If we didn't pin the visibility map page and the page has become all
     * visible while we were busy locking the buffer, or during some
     * subsequent window during which we had it unlocked, we'll have to unlock
     * and re-lock, to avoid holding the buffer lock across an I/O.  That's a
     * bit unfortunate, esepecially since we'll now have to recheck whether
     * the tuple has been locked or updated under us, but hopefully it won't
     * happen very often.
     */
    if (vmbuffer == InvalidBuffer && PageIsAllVisible(page)) {
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
        visibilitymap_pin(relation, block, &vmbuffer);
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
        HeapTupleCopyBaseFromPage(&oldtup, page);
        goto l2;
    }

    /*
     * We're about to do the actual update -- check for conflict first, to
     * avoid possibly having to roll back work we've just done.
     */
    CheckForSerializableConflictIn(relation, &oldtup, buffer);

    /* Fill in OID and transaction status data for newtup */
    if (relation->rd_rel->relhasoids) {
#ifdef NOT_USED
        /* this is redundant with an Assert in HeapTupleSetOid */
        Assert(newtup->t_data->t_infomask & HEAP_HASOID);
#endif
        HeapTupleSetOid(newtup, HeapTupleGetOid(&oldtup));
    } else {
        /* check there is not space for an OID */
        Assert(!(newtup->t_data->t_infomask & HEAP_HASOID));
    }

    newtup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
    newtup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
    newtup->t_data->t_infomask |= (HEAP_XMAX_INVALID | HEAP_UPDATED);

    /* Unset the HEAP_HAS_REDIS_COLUMNS bit in the new tuple to make sure hidden
     * columns added by redis (if any) are removed from the tuple.
     * Note: We never allow updates when the relation is redis destination table. */
    Assert(!relation->rd_att->tdisredistable);
    HeapTupleHeaderUnsetRedisColumns(newtup->t_data);

    heap_page_prepare_for_xid(relation, buffer, xid, false);
    HeapTupleCopyBaseFromPage(newtup, page);

    HeapTupleSetXmin(newtup, xid);
    HeapTupleHeaderSetCmin(newtup->t_data, cid);
    HeapTupleHeaderSetXmax(page, newtup->t_data, 0); /* for cleanliness */
    newtup->t_tableOid = RelationGetRelid(relation);
    newtup->t_bucketId = RelationGetBktid(relation);
#ifdef PGXC
    newtup->t_xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
#endif
    if (rel_in_redis && !RelationInClusterResizingReadOnly(relation)) {
        options |= HEAP_INSERT_SKIP_FSM;
        rel_end_block = RelationGetEndBlock(relation);
    }

    /*
     * Replace cid with a combo cid if necessary.  Note that we already put
     * the plain cid into the new tuple.
     */
    HeapTupleHeaderAdjustCmax(oldtup.t_data, &cid, &is_combo, buffer);

    /*
     * If the toaster needs to be activated, OR if the new tuple will not fit
     * on the same page as the old, then we need to release the content lock
     * (but not the pin!) on the old tuple's buffer while we are off doing
     * TOAST and/or table-file-extension work.	We must mark the old tuple to
     * show that it's already being updated, else other processes may try to
     * update it themselves.
     *
     * We need to invoke the toaster if there are already any out-of-line
     * toasted values present, or if the new tuple is over-threshold.
     */
    if (relation->rd_rel->relkind != RELKIND_RELATION &&
        relation->rd_rel->relkind != RELKIND_MATVIEW) {
        /* toast table entries should never be recursively toasted */
        Assert(!HeapTupleHasExternal(&oldtup));
        Assert(!HeapTupleHasExternal(newtup));
        need_toast = false;
    } else {
        need_toast = (HeapTupleHasExternal(&oldtup) || HeapTupleHasExternal(newtup) ||
                      newtup->t_len > TOAST_TUPLE_THRESHOLD);
    }

    pagefree = PageGetHeapFreeSpace(page);

    new_tup_size = MAXALIGN(newtup->t_len);
    if (need_toast || new_tup_size > pagefree || rel_in_redis) {
        /* Clear obsolete visibility flags ... */
        oldtup.t_data->t_infomask &= ~(HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI | HEAP_IS_LOCKED |
                                       HEAP_MOVED);
        HeapTupleClearHotUpdated(&oldtup);
        /* ... and store info about transaction updating this tuple */
        HeapTupleHeaderSetXmax(page, oldtup.t_data, xid);
        HeapTupleHeaderSetCmax(oldtup.t_data, cid, is_combo);
        /* temporarily make it look not-updated */
        oldtup.t_data->t_ctid = oldtup.t_self;
        HeapTupleCopyBaseFromPage(&oldtup, page);

        already_marked = true;
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

        /*
         * Let the toaster do its thing, if needed.
         *
         * Note: below this point, heaptup is the data we actually intend to
         * store into the relation; newtup is the caller's original untoasted
         * data.
         */
        if (need_toast) {
            /* Note we always use WAL and FSM during updates */
            heaptup = toast_insert_or_update(relation, newtup, &oldtup, allow_update_self ? HEAP_INSERT_SPECULATIVE : 0,
                                             page);
            new_tup_size = MAXALIGN(heaptup->t_len);
        } else {
            heaptup = newtup;
        }

        /*
         * Now, do we need a new page for the tuple, or not?  This is a bit
         * tricky since someone else could have added tuples to the page while
         * we weren't looking.  We have to recheck the available space after
         * reacquiring the buffer lock.  But don't bother to do that if the
         * former amount of free space is still not enough; it's unlikely
         * there's more free now than before.
         *
         * What's more, if we need to get a new page, we will need to acquire
         * buffer locks on both old and new pages.	To avoid deadlock against
         * some other backend trying to get the same two locks in the other
         * order, we must be consistent about the order we get the locks in.
         * We use the rule "lock the lower-numbered page of the relation
         * first".  To implement this, we must do RelationGetBufferForTuple
         * while not holding the lock on the old page, and we must rely on it
         * to get the locks on both pages in the correct order.
         */
        if (new_tup_size > pagefree || rel_in_redis) {
            /* Assume there's no chance to put heaptup on same page. */
            newbuf = RelationGetBufferForTuple(relation, heaptup->t_len, buffer, options, NULL, &vmbuffer_new,
                                               &vmbuffer, rel_end_block);
        } else {
            /* Re-acquire the lock on the old tuple's page. */
            LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
            /* Re-check using the up-to-date free space */
            pagefree = PageGetHeapFreeSpace(page);
            if (new_tup_size > pagefree) {
                /*
                 * Rats, it doesn't fit anymore.  We must now unlock and
                 * relock to avoid deadlock.  Fortunately, this path should
                 * seldom be taken.
                 */
                LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
                newbuf = RelationGetBufferForTuple(relation, heaptup->t_len, buffer, options, NULL, &vmbuffer_new,
                                                   &vmbuffer, rel_end_block);
            } else {
                /* OK, it fits here, so we're done. */
                newbuf = buffer;
            }
        }
    } else {
        /* No TOAST work needed, and it'll fit on same page */
        already_marked = false;
        newbuf = buffer;
        heaptup = newtup;
    }

    /*
     * We're about to create the new tuple -- check for conflict first, to
     * avoid possibly having to roll back work we've just done.
     *
     * NOTE: For a tuple insert, we only need to check for table locks, since
     * predicate locking at the index level will cover ranges for anything
     * except a table scan.  Therefore, only provide the relation.
     */
    CheckForSerializableConflictIn(relation, NULL, InvalidBuffer);

    /*
     * At this point newbuf and buffer are both pinned and locked, and newbuf
     * has enough space for the new tuple.	If they are the same buffer, only
     * one pin is held.
     */
    if (!(options & HEAP_INSERT_SKIP_FSM)) {
        if (newbuf == buffer) {
            /*
             * Since the new tuple is going into the same page, we might be able
             * to do a HOT update.	Check if any of the index columns have been
             * changed.  If not, then HOT update is possible.
             */
            if (satisfies_hot) {
                use_hot_update = true;
            }
        } else {
            /* Set a hint that the old page could use prune/defrag */
            PageSetFull(page);
        }
    }
    /*
     * Compute replica identity tuple before entering the critical section so
     * we don't PANIC upon a memory allocation failure.
     * ExtractReplicaIdentity() will return NULL if nothing needs to be
     * logged.
     */
    old_key_tuple = ExtractReplicaIdentity(relation, &oldtup, !satisfies_id, &old_key_copied);

    newpage = BufferGetPage(newbuf);
    if (newbuf != buffer) {
        /* Prepare new page for xids */
        (void)heap_page_prepare_for_xid(relation, newbuf, xid, false);
        HeapTupleCopyBaseFromPage(heaptup, newpage);
    }

    /* NO EREPORT(ERROR) from here till changes are logged */
    START_CRIT_SECTION();

    /*
     * If this transaction commits, the old tuple will become DEAD sooner or
     * later.  Set flag that this page is a candidate for pruning once our xid
     * falls below the oldest_xmin horizon.	If the transaction finally aborts,
     * the subsequent page pruning will be a no-op and the hint will be
     * cleared.
     *
     * XXX Should we set hint on newbuf as well?  If the transaction aborts,
     * there would be a prunable tuple in the newbuf; but for now we choose
     * not to optimize for aborts.	Note that heap_xlog_update must be kept in
     * sync if this decision changes.
     */
    if (!(options & HEAP_INSERT_SKIP_FSM)) {
        PageSetPrunable(page, xid);
    }

    if (use_hot_update) {
        /* Mark the old tuple as HOT-updated */
        HeapTupleSetHotUpdated(&oldtup);
        /* And mark the new tuple as heap-only */
        HeapTupleSetHeapOnly(heaptup);
        /* Mark the caller's copy too, in case different from heaptup */
        HeapTupleSetHeapOnly(newtup);
    } else {
        /* Make sure tuples are correctly marked as not-HOT */
        HeapTupleClearHotUpdated(&oldtup);
        HeapTupleClearHeapOnly(heaptup);
        HeapTupleClearHeapOnly(newtup);
    }

    RelationPutHeapTuple(relation, newbuf, heaptup, xid); /* insert new tuple */

    if (!already_marked) {
        /* Clear obsolete visibility flags ... */
        oldtup.t_data->t_infomask &= ~(HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI | HEAP_IS_LOCKED |
                                       HEAP_MOVED);
        /* ... and store info about transaction updating this tuple */
        HeapTupleHeaderSetXmax(page, oldtup.t_data, xid);
        HeapTupleHeaderSetCmax(oldtup.t_data, cid, is_combo);
    }

    /* record address of new tuple in t_ctid of old one */
    oldtup.t_data->t_ctid = heaptup->t_self;

    /* clear PD_ALL_VISIBLE flags */
    if (PageIsAllVisible(BufferGetPage(buffer))) {
        all_visible_cleared = true;
        PageClearAllVisible(BufferGetPage(buffer));
        visibilitymap_clear(relation, BufferGetBlockNumber(buffer), vmbuffer);
    }
    if (newbuf != buffer && PageIsAllVisible(BufferGetPage(newbuf))) {
        all_visible_cleared_new = true;
        PageClearAllVisible(BufferGetPage(newbuf));
        visibilitymap_clear(relation, BufferGetBlockNumber(newbuf), vmbuffer_new);
    }

    if (newbuf != buffer) {
        MarkBufferDirty(newbuf);
    }
    MarkBufferDirty(buffer);

    /* XLOG stuff */
    if (RelationNeedsWAL(relation)) {
        XLogRecPtr recptr;
        /*
         * For logical decoding we need combocids to properly decode the
         * catalog.
         */
        if (RelationIsAccessibleInLogicalDecoding(relation)) {
            (void)log_heap_new_cid(relation, &oldtup);
            (void)log_heap_new_cid(relation, heaptup);
        }
        recptr = log_heap_update(relation, buffer, &(oldtup.t_self), newbuf, heaptup, old_key_tuple,
                                 all_visible_cleared, all_visible_cleared_new);

        if (newbuf != buffer) {
            PageSetLSN(BufferGetPage(newbuf), recptr);
        }
        PageSetLSN(BufferGetPage(buffer), recptr);
    }

    END_CRIT_SECTION();

    if (newbuf != buffer) {
        LockBuffer(newbuf, BUFFER_LOCK_UNLOCK);
    }
    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    /*
     * Mark old tuple for invalidation from system caches at next command
     * boundary, and mark the new tuple for invalidation in case we abort. We
     * have to do this before releasing the buffer because oldtup is in the
     * buffer.	(heaptup is all in local memory, but it's necessary to process
     * both tuple versions in one call to inval.c so we can avoid redundant
     * sinval messages.)
     */
    CacheInvalidateHeapTuple(relation, &oldtup, heaptup);

    /* Now we can release the buffer(s) */
    if (newbuf != buffer) {
        ReleaseBuffer(newbuf);
    }
    ReleaseBuffer(buffer);
    if (BufferIsValid(vmbuffer_new)) {
        ReleaseBuffer(vmbuffer_new);
    }
    if (BufferIsValid(vmbuffer)) {
        ReleaseBuffer(vmbuffer);
    }

    /*
     * Release the lmgr tuple lock, if we had it.
     */
    if (have_tuple_lock) {
        UnlockTuple(relation, &(oldtup.t_self), ExclusiveLock);
    }

    pgstat_count_heap_update(relation, use_hot_update);

    /*
     * If heaptup is a private copy, release it.  Don't forget to copy t_self
     * back to the caller's image, too.
     */
    if (heaptup != newtup) {
        newtup->t_self = heaptup->t_self;
        heap_freetuple(heaptup);
    }

    if (old_key_tuple != NULL && old_key_copied) {
        heap_freetuple(old_key_tuple);
    }

    bms_free(hot_attrs);
    bms_free(id_attrs);

    return TM_Ok;
}
static XLogRecPtr log_heap_new_cid_insert(xl_heap_new_cid *xlrec)
{
    XLogRecPtr recptr;
    /*
     * Note that we don't need to register the buffer here, because this
     * operation does not modify the page. The insert/update/delete that
     * called us certainly did, but that's WAL-logged separately.
     */
    XLogBeginInsert();
    XLogRegisterData((char *)xlrec, SizeOfHeapNewCid);
    /* will be looked at irrespective of origin */
    recptr = XLogInsert(RM_HEAP3_ID, XLOG_HEAP3_NEW_CID);
    return recptr;
}
/*
 * Perform XLogInsert of an XLOG_HEAP2_NEW_CID record
 *
 * This is only used in wal_level >= WAL_LEVEL_LOGICAL, and only for catalog
 * tuples.
 */
static XLogRecPtr log_heap_new_cid(Relation relation, HeapTuple tup)
{
    xl_heap_new_cid xlrec;

    XLogRecPtr recptr;
    HeapTupleHeader hdr = tup->t_data;

    Assert(ItemPointerIsValid(&tup->t_self));
    Assert(tup->t_tableOid != InvalidOid);

    xlrec.top_xid = GetTopTransactionId();
    xlrec.target_node.dbNode = relation->rd_node.dbNode;
    xlrec.target_node.relNode = relation->rd_node.relNode;
    xlrec.target_node.spcNode = relation->rd_node.spcNode;
    xlrec.target_tid = tup->t_self;

    /*
     * If the tuple got inserted & deleted in the same TX we definitely have a
     * combocid, set cmin and cmax.
     */
    if (hdr->t_infomask & HEAP_COMBOCID) {
        Assert(!(hdr->t_infomask & HEAP_XMAX_INVALID));
        Assert(!(hdr->t_infomask & HEAP_XMIN_INVALID));
        xlrec.cmin = HeapTupleGetCmin(tup);
        xlrec.cmax = HeapTupleGetCmax(tup);
        xlrec.combocid = HeapTupleHeaderGetRawCommandId(hdr);
    }
    /* No combocid, so only cmin or cmax can be set by this TX */
    else {
        /*
         * Tuple inserted.
         *
         * We need to check for LOCK ONLY because multixacts might be
         * transferred to the new tuple in case of FOR KEY SHARE updates in
         * which case there will be an xmax, although the tuple just got
         * inserted.
         */
        if ((hdr->t_infomask & HEAP_XMAX_INVALID) || (hdr->t_infomask & HEAP_IS_LOCKED)) {
            xlrec.cmin = HeapTupleHeaderGetRawCommandId(hdr);
            xlrec.cmax = InvalidCommandId;
        } else {
            /* Tuple from a different tx updated or deleted. */
            xlrec.cmin = InvalidCommandId;
            xlrec.cmax = HeapTupleHeaderGetRawCommandId(hdr);
        }
        xlrec.combocid = InvalidCommandId;
    }
    recptr = log_heap_new_cid_insert(&xlrec);
    return recptr;
}

bool heap_page_upgrade(Relation relation, Buffer buffer)
{
    TransactionId xid = GetCurrentTransactionId();
    Page page = BufferGetPage(buffer);
    Size page_free_space = 0;
    Size save_free_space = 0;
    bool is_upgrade = true;
    int nline;

    /* Compute desired extra freespace due to fillfactor option */
    save_free_space = RelationGetTargetPageFreeSpace(relation, HEAP_DEFAULT_FILLFACTOR);
    page_free_space = (int)((PageHeader)page)->pd_upper - (int)((PageHeader)page)->pd_lower;

    if (page_free_space - save_free_space >= SizeOfHeapPageUpgradeData) {
        if (!PageIs4BXidVersion(page)) {
            return true;
        }

        START_CRIT_SECTION();
        PageLocalUpgrade(page);
    } else {
        nline = PageGetMaxOffsetNumber(page);
        if (nline == 1) {
            if (xid > MaxShortTransactionId) {
                if (BufferIsValid(buffer)) {
                    UnlockReleaseBuffer(buffer);
                }
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("relation \"%s\" has one big row which is not supported under 64bits XID system. "
                                "Current xid is %lu",
                                RelationGetRelationName(relation), xid)));
            } else
                ereport(WARNING,
                        (errcode(ERRCODE_WARNING),
                         errmsg("block number %u for relation \"%s\" has one big row which is not supported under "
                                "64bits XID system. Current xid is %lu",
                                BufferGetBlockNumber(buffer), RelationGetRelationName(relation), xid),
                         handle_in_client(true)));
        } else {
            if (xid > MaxShortTransactionId) {
                if (BufferIsValid(buffer)) {
                    UnlockReleaseBuffer(buffer);
                }
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("relation \"%s\" has no free space to upgrade. Current xid is %lu, please VACUUM FULL "
                                "this relation!!!",
                                RelationGetRelationName(relation), xid)));
            } else
                ereport(
                    DEBUG1,
                    (errmsg("block number %u for relation \"%s\" has no free space to upgrade. Current xid is %lu, it "
                            "is safe before XID increased to 4294967296 !",
                            BufferGetBlockNumber(buffer), RelationGetRelationName(relation), xid)));
        }
        is_upgrade = false;
        return false;
    }

    /* xlog stuff */
    if (is_upgrade) {
        MarkBufferDirty(buffer);
        if (RelationNeedsWAL(relation)) {
            XLogRecPtr recptr;

            XLogBeginInsert();
            XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

            recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_PAGE_UPGRADE);

            PageSetLSN(BufferGetPage(buffer), recptr);
        }
        END_CRIT_SECTION();
    }
    return true;
}

/*
 * Check if the specified attribute's value is same in both given tuples.
 * Subroutine for HeapSatisfiesHOTUpdate.
 */
static bool heap_tuple_attr_equals(TupleDesc tupdesc, int attrnum, HeapTuple tup1, HeapTuple tup2, char *page)
{
    Datum value1, value2;
    bool isnull1 = false;
    bool isnull2 = false;
    Form_pg_attribute att;

    /*
     * If it's a whole-tuple reference, say "not equal".  It's not really
     * worth supporting this case, since it could only succeed after a no-op
     * update, which is hardly a case worth optimizing for.
     */
    if (attrnum == 0) {
        return false;
    }

    /*
     * Likewise, automatically say "not equal" for any system attribute other
     * than OID and tableOID; we cannot expect these to be consistent in a HOT
     * chain, or even to be set correctly yet in the new tuple.
     */
    if (attrnum < 0) {
        if (attrnum != ObjectIdAttributeNumber &&
#ifdef PGXC
            attrnum != XC_NodeIdAttributeNumber && attrnum != BucketIdAttributeNumber &&
#endif
            attrnum != TableOidAttributeNumber) {
            return false;
        }
    }

    /*
     * Extract the corresponding values.  XXX this is pretty inefficient if
     * there are many indexed columns.	Should HeapSatisfiesHOTUpdate do a
     * single heap_deform_tuple call on each tuple, instead?  But that doesn't
     * work for system columns ...
     */
    if (HEAP_TUPLE_IS_COMPRESSED(tup1->t_data)) {
        value1 = heap_getattr_with_dict(tup1, attrnum, tupdesc, &isnull1, (char *)getPageDict(page));
    } else {
        value1 = heap_getattr(tup1, attrnum, tupdesc, &isnull1);
    }
    Assert(!HEAP_TUPLE_IS_COMPRESSED(tup2->t_data));
    value2 = heap_getattr(tup2, attrnum, tupdesc, &isnull2);

    /*
     * If one value is NULL and other is not, then they are certainly not
     * equal
     */
    if (isnull1 != isnull2) {
        return false;
    }

    /*
     * If both are NULL, they can be considered equal.
     */
    if (isnull1) {
        return true;
    }

    /*
     * We do simple binary comparison of the two datums.  This may be overly
     * strict because there can be multiple binary representations for the
     * same logical value.	But we should be OK as long as there are no false
     * positives.  Using a type-specific equality operator is messy because
     * there could be multiple notions of equality in different operator
     * classes; furthermore, we cannot safely invoke user-defined functions
     * while holding exclusive buffer lock.
     */
    if (attrnum <= 0) {
        /* The only allowed system columns are OIDs, so do this */
        return (DatumGetObjectId(value1) == DatumGetObjectId(value2));
    } else {
        Assert(attrnum <= tupdesc->natts);
        att = tupdesc->attrs[attrnum - 1];
        return datumIsEqual(value1, value2, att->attbyval, att->attlen);
    }
}

/*
 * Check if the old and new tuples represent a HOT-safe update. To be able
 * to do a HOT update, we must not have changed any columns used in index
 * definitions.
 *
 * The set of attributes to be checked is passed in (we dare not try to
 * compute it while holding exclusive buffer lock...)  NOTE that hot_attrs
 * is destructively modified!  That is OK since this is invoked at most once
 * by heap_update().
 *
 * Returns true if safe to do HOT update.
 */
static void HeapSatisfiesHOTUpdate(Relation relation, Bitmapset *hot_attrs, Bitmapset *id_attrs, bool *satisfies_hot,
                                   bool *satisfies_id, HeapTuple oldtup, HeapTuple newtup, char *page)
{
    int next_hot_attnum;
    int next_id_attnum;
    bool hot_result = true;
    bool id_result = true;

    /*
     * If one of these sets contains no remaining bits, bms_first_member will
     * return -1, and after adding FirstLowInvalidHeapAttributeNumber (which
     * is negative!)  we'll get an attribute number that can't possibly be
     * real, and thus won't match any actual attribute number.
     */
    next_hot_attnum = bms_first_member(hot_attrs);
    next_hot_attnum += FirstLowInvalidHeapAttributeNumber;
    next_id_attnum = bms_first_member(id_attrs);
    next_id_attnum += FirstLowInvalidHeapAttributeNumber;

    for (;;) {
        bool changed = false;
        int check_now;
        /*
         * Since the HOT attributes are a superset of the key attributes and
         * the key attributes are a superset of the id attributes, this logic
         * is guaranteed to identify the next column that needs to be
         * checked.
         */
        if (hot_result && next_hot_attnum > FirstLowInvalidHeapAttributeNumber) {
            check_now = next_hot_attnum;
        } else if (id_result && next_id_attnum > FirstLowInvalidHeapAttributeNumber) {
            check_now = next_id_attnum;
        } else {
            break;
        }

        /* See whether it changed. */
        changed = !heap_tuple_attr_equals(RelationGetDescr(relation), check_now, oldtup, newtup, page);
        if (changed) {
            if (check_now == next_hot_attnum) {
                hot_result = false;
            }
            if (check_now == next_id_attnum) {
                id_result = false;
            }

            /* if all are false now, we can stop checking */
            if (!hot_result && !id_result) {
                break;
            }
        }

        /*
         * Advance the next attribute numbers for the sets that contain
         * the attribute we just checked.  As we work our way through the
         * columns, the next_attnum values will rise; but when each set
         * becomes empty, bms_first_member() will return -1 and the attribute
         * number will end up with a value less than
         * FirstLowInvalidHeapAttributeNumber.
         */
        if (hot_result && check_now == next_hot_attnum) {
            next_hot_attnum = bms_first_member(hot_attrs);
            next_hot_attnum += FirstLowInvalidHeapAttributeNumber;
        }
        if (id_result && check_now == next_id_attnum) {
            next_id_attnum = bms_first_member(id_attrs);
            next_id_attnum += FirstLowInvalidHeapAttributeNumber;
        }
    }
    *satisfies_hot = hot_result;
    *satisfies_id = id_result;
}

/*
 *	simple_heap_update - replace a tuple
 *
 * This routine may be used to update a tuple when concurrent updates of
 * the target tuple are not expected (for example, because we have a lock
 * on the relation associated with the tuple).	Any failure is reported
 * via ereport().
 */
void simple_heap_update(Relation relation, ItemPointer otid, HeapTuple tup)
{
    TM_Result result;
    TM_FailureData tmfd;

    /* All built-in functions are hard coded, and thus they should not be updated */
    if (u_sess->attr.attr_common.IsInplaceUpgrade == false && IsProcRelation(relation) &&
        IsSystemObjOid(HeapTupleGetOid(tup))) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("All built-in functions are hard coded, and they should not be updated.")));
    }

    result = heap_update(relation, NULL, otid, tup, GetCurrentCommandId(true),
                         InvalidSnapshot, true /* wait for commit */, &tmfd);
    switch (result) {
        case TM_SelfModified:
            /* Tuple was already updated in current command? */
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("tuple already updated by self")));
            break;

        case TM_Ok:
            /* done successfully */
            break;

        case TM_Updated:
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("tuple concurrently updated")));
            break;

        case TM_Deleted:
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("tuple concurrently updated")));
            break;

        default:
            ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("unrecognized heap_update status: %u", result)));
            break;
    }
}

/*
 *	heap_lock_tuple - lock a tuple in shared or exclusive mode
 *
 * Note that this acquires a buffer pin, which the caller must release.
 *
 * Input parameters:
 *	relation: relation containing tuple (caller must hold suitable lock)
 *	tuple->t_self: TID of tuple to lock (rest of struct need not be valid)
 *	cid: current command ID (used for visibility test, and stored into
 *		tuple's cmax if lock is successful)
 *	mode: indicates if shared or exclusive tuple lock is desired
 *	nowait: if true, ereport rather than blocking if lock not available
 *
 * Output parameters:
 *	*tuple: all fields filled in
 *	*buffer: set to buffer holding tuple (pinned but not locked at exit)
 *	*ctid: set to tuple's t_ctid, but only in failure cases
 *	*update_xmax: set to tuple's xmax, but only in failure cases
 *
 * Function result may be:
 *	HeapTupleMayBeUpdated: lock was successfully acquired
 *	HeapTupleSelfUpdated: lock failed because tuple updated by self
 *	HeapTupleUpdated: lock failed because tuple updated by other xact
 *
 * In the failure cases, the routine returns the tuple's t_ctid and t_xmax.
 * If t_ctid is the same as t_self, the tuple was deleted; if different, the
 * tuple was updated, and t_ctid is the location of the replacement tuple.
 * (t_xmax is needed to verify that the replacement tuple matches.)
 *
 *
 * NOTES: because the shared-memory lock table is of finite size, but users
 * could reasonably want to lock large numbers of tuples, we do not rely on
 * the standard lock manager to store tuple-level locks over the long term.
 * Instead, a tuple is marked as locked by setting the current transaction's
 * XID as its XMAX, and setting additional infomask bits to distinguish this
 * usage from the more normal case of having deleted the tuple.  When
 * multiple transactions concurrently share-lock a tuple, the first locker's
 * XID is replaced in XMAX with a MultiTransactionId representing the set of
 * XIDs currently holding share-locks.
 *
 * When it is necessary to wait for a tuple-level lock to be released, the
 * basic delay is provided by XactLockTableWait or MultiXactIdWait on the
 * contents of the tuple's XMAX.  However, that mechanism will release all
 * waiters concurrently, so there would be a race condition as to which
 * waiter gets the tuple, potentially leading to indefinite starvation of
 * some waiters.  The possibility of share-locking makes the problem much
 * worse --- a steady stream of share-lockers can easily block an exclusive
 * locker forever.	To provide more reliable semantics about who gets a
 * tuple-level lock first, we use the standard lock manager.  The protocol
 * for waiting for a tuple-level lock is really LockTuple(), XactLockTableWait()
 *		mark tuple as locked by me UnlockTuple()
 * When there are multiple waiters, arbitration of who is to get the lock next
 * is provided by LockTuple().	However, at most one tuple-level lock will
 * be held or awaited per backend at any time, so we don't risk overflow
 * of the lock table.  Note that incoming share-lockers are required to
 * do LockTuple as well, if there is any conflict, to ensure that they don't
 * starve out waiting exclusive-lockers.  However, if there is not any active
 * conflict for a tuple, we don't incur any extra overhead.
 */
TM_Result heap_lock_tuple(Relation relation, HeapTuple tuple, Buffer* buffer, 
    CommandId cid, LockTupleMode mode, bool nowait, TM_FailureData *tmfd, bool allow_lock_self)
{
    TM_Result result;
    ItemPointer tid = &(tuple->t_self);
    ItemId lp;
    Page page;
    TransactionId xid;
    TransactionId xmax;
    uint16 old_infomask;
    uint16 new_infomask;
    LOCKMODE tuple_lock_type;
    bool have_tuple_lock = false;
    Buffer vmbuffer = InvalidBuffer;
    BlockNumber block;

    /* Don't allow any write/lock operator in stream. */
    AssertEreport(!StreamThreadAmI(), MOD_STREAM, "Unsupported lock tuple in stream.");

    /* Not support tuple concurrent update to avoid distributed deadlock. */
    if (!u_sess->attr.attr_common.allow_concurrent_tuple_update) {
        nowait = true;
    }

    tuple_lock_type = (mode == LockTupleShared) ? ShareLock : ExclusiveLock;

    block = ItemPointerGetBlockNumber(tid);
    *buffer = ReadBuffer(relation, block);

    /*
     * Before locking the buffer, pin the visibility map page if it appears to
     * be necessary.  Since we haven't got the lock yet, someone else might be
     * in the middle of changing this, so we'll need to recheck after we have
     * the lock.
     */
    if (PageIsAllVisible(BufferGetPage(*buffer))) {
        visibilitymap_pin(relation, block, &vmbuffer);
    }

    LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

    page = BufferGetPage(*buffer);
    if (PageIs4BXidVersion(page)) {
        (void)heap_page_upgrade(relation, *buffer);
    }

    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
    Assert(ItemIdIsNormal(lp));

    tuple->t_data = (HeapTupleHeader)PageGetItem(page, lp);
    tuple->t_len = ItemIdGetLength(lp);
    tuple->t_tableOid = RelationGetRelid(relation);
    tuple->t_bucketId = RelationGetBktid(relation);
#ifdef PGXC
    tuple->t_xc_node_id = u_sess->pgxc_cxt.PGXCNodeIdentifier;
#endif

l3:
    HeapTupleCopyBaseFromPage(tuple, page);
    result = HeapTupleSatisfiesUpdate(tuple, cid, *buffer, allow_lock_self);
    ereport(DEBUG1, (errmsg("heap lock tuple ctid (%u,%d) cur_xid %lu xmin "
                            "%lu xmax %lu infomask %hu result %d",
                            ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid),
                            GetCurrentTransactionIdIfAny(), HeapTupleHeaderGetXmin(page, tuple->t_data),
                            HeapTupleHeaderGetXmax(page, tuple->t_data), tuple->t_data->t_infomask, result)));

    if (result == TM_Invisible) {
        UnlockReleaseBuffer(*buffer);
        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("attempted to lock invisible tuple")));
    } else if (result == TM_SelfCreated) {
        /*
         * This is possible when the tuple is going to be updated twice in one command,
         * which should be considered as invisible (same with HeapTupleInvisible) and
         * throw an error.
         *
         * However, there is a special case: UPSERT multiple VALUES using a STREAM plan
         * e.g INSERT values(1,x),(1,x) ON DUPLICATE KEY UPDATE..
         * As we have to allow this case to be done in B compatibility,
         * we return HeapTupleSelfCreated here rather than throwing an error in
         * order to give UPSERT case the opportunity to throw a more specific error or
         * allow to UPSERT
         *
         * NOTE: multiple VALUES UPSERT using a PGXC plan is not a problem because
         * the optimizer will spilt the query into multiple commands, each of which only
         * UPSERT one VALUES().
         */
        LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
        return TM_SelfCreated;
    } else if (result == TM_BeingModified) {
        TransactionId xwait;
        uint16 infomask;

        /* must copy state data before unlocking buffer */
        xwait = HeapTupleGetRawXmax(tuple);
        infomask = tuple->t_data->t_infomask;

        LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);

        /*
         * If we wish to acquire share lock, and the tuple is already
         * share-locked by a multixact that includes any subtransaction of the
         * current top transaction, then we effectively hold the desired lock
         * already.  We *must* succeed without trying to take the tuple lock,
         * else we will deadlock against anyone waiting to acquire exclusive
         * lock.  We don't need to make any state changes in this case.
         */
        if (mode == LockTupleShared && (infomask & HEAP_XMAX_IS_MULTI) && MultiXactIdIsCurrent((MultiXactId)xwait)) {
            Assert(infomask & HEAP_XMAX_SHARED_LOCK);

            result = TM_Ok;
            goto out_unlocked;
        }

        /*
         * Acquire tuple lock to establish our priority for the tuple.
         * LockTuple will release us when we are next-in-line for the tuple.
         * We must do this even if we are share-locking.
         *
         * If we are forced to "start over" below, we keep the tuple lock;
         * this arranges that we stay at the head of the line while rechecking
         * tuple state.
         */
        if (!have_tuple_lock) {
            if (nowait) {
                if (!ConditionalLockTuple(relation, tid, tuple_lock_type)) {
                    ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                                    errmsg("could not obtain lock on row in relation \"%s\"",
                                           RelationGetRelationName(relation))));
                }
            } else {
                LockTuple(relation, tid, tuple_lock_type, true);
            }
            have_tuple_lock = true;
        }

        if (mode == LockTupleShared && (infomask & HEAP_XMAX_SHARED_LOCK)) {
            /*
             * Acquiring sharelock when there's at least one sharelocker
             * already.  We need not wait for him/them to complete.
             */
            LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

            /*
             * Make sure it's still a shared lock, else start over.  (It's OK
             * if the ownership of the shared lock has changed, though.)
             */
            if (!(tuple->t_data->t_infomask & HEAP_XMAX_SHARED_LOCK)) {
                goto l3;
            }
        } else if (infomask & HEAP_XMAX_IS_MULTI) {
            /* wait for multixact to end */
            if (nowait) {
                if (!ConditionalMultiXactIdWait((MultiXactId)xwait)) {
                    ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                                    errmsg("could not obtain lock on row in relation \"%s\"",
                                           RelationGetRelationName(relation))));
                }
            } else {
                MultiXactIdWait((MultiXactId)xwait, true);
            }

            LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

            /*
             * If xwait had just locked the tuple then some other xact could
             * update this tuple before we get to this point. Check for xmax
             * change, and start over if so.
             */
            if (!(tuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI) ||
                !TransactionIdEquals(HeapTupleGetRawXmax(tuple), xwait)) {
                goto l3;
            }

            /*
             * You might think the multixact is necessarily done here, but not
             * so: it could have surviving members, namely our own xact or
             * other subxacts of this backend.	It is legal for us to lock the
             * tuple in either case, however.  We don't bother changing the
             * on-disk hint bits since we are about to overwrite the xmax
             * altogether.
             */
        } else {
            /* wait for regular transaction to end */
            if (nowait) {
                if (!ConditionalXactLockTableWait(xwait))
                    ereport(ERROR, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                                    errmsg("could not obtain lock on row in relation \"%s\"",
                                           RelationGetRelationName(relation))));
            } else {
                XactLockTableWait(xwait, true);
            }

            LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);

            /*
             * xwait is done, but if xwait had just locked the tuple then some
             * other xact could update this tuple before we get to this point.
             * Check for xmax change, and start over if so.
             */
            if ((tuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI) ||
                !TransactionIdEquals(HeapTupleGetRawXmax(tuple), xwait)) {
                goto l3;
            }

            /* Otherwise check if it committed or aborted */
            UpdateXmaxHintBits(tuple->t_data, *buffer, xwait);
        }

        /*
         * We may lock if previous xmax aborted, or if it committed but only
         * locked the tuple without updating it.  The case where we didn't
         * wait because we are joining an existing shared lock is correctly
         * handled, too.
         */
        if (tuple->t_data->t_infomask & (HEAP_XMAX_INVALID | HEAP_IS_LOCKED)) {
            result = TM_Ok;
        } else if (!ItemPointerEquals(&tuple->t_self, &tuple->t_data->t_ctid)){
            result = TM_Updated;
        } else {
            result = TM_Deleted;
        }
    }

    if (result != TM_Ok) {
        Assert(result == TM_SelfModified || result == TM_Updated || result == TM_Deleted);
        Assert(!(tuple->t_data->t_infomask & HEAP_XMAX_INVALID));
        Assert(result != TM_Updated ||
            !ItemPointerEquals(&tuple->t_self, &tuple->t_data->t_ctid));
        tmfd->ctid = tuple->t_data->t_ctid;
        tmfd->xmax = HeapTupleGetRawXmax(tuple);
        if (result == TM_SelfModified) {
            tmfd->cmax = HeapTupleHeaderGetCmax(tuple->t_data, page);
        } else {
            tmfd->cmax = InvalidCommandId;
        }
        goto out_locked;
    }

    /*
     * We might already hold the desired lock (or stronger), possibly under a
     * different subtransaction of the current top transaction.  If so, there
     * is no need to change state or issue a WAL record.  We already handled
     * the case where this is true for xmax being a MultiXactId, so now check
     * for cases where it is a plain TransactionId.
     *
     * Note in particular that this covers the case where we already hold
     * exclusive lock on the tuple and the caller only wants shared lock. It
     * would certainly not do to give up the exclusive lock.
     */
    xmax = HeapTupleGetRawXmax(tuple);
    old_infomask = tuple->t_data->t_infomask;

    if (!(old_infomask & (HEAP_XMAX_INVALID | HEAP_XMAX_COMMITTED | HEAP_XMAX_IS_MULTI)) &&
        (mode == LockTupleShared ? (old_infomask & HEAP_IS_LOCKED) : (old_infomask & HEAP_XMAX_EXCL_LOCK)) &&
        TransactionIdIsCurrentTransactionId(xmax)) {
        LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
        /* Probably can't hold tuple lock here, but may as well check */
        if (have_tuple_lock) {
            UnlockTuple(relation, tid, tuple_lock_type);
        }
        result = TM_Ok;
        goto out_unlocked;
    }

    /*
     * Compute the new xmax and infomask to store into the tuple.  Note we do
     * not modify the tuple just yet, because that would leave it in the wrong
     * state if multixact.c elogs.
     */
    xid = GetCurrentTransactionId();

    new_infomask = old_infomask &
                   ~(HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI | HEAP_IS_LOCKED | HEAP_MOVED);

    if (mode == LockTupleShared) {
        /*
         * If this is the first acquisition of a shared lock in the current
         * transaction, set my per-backend OldestMemberMXactId setting. We can
         * be certain that the transaction will never become a member of any
         * older MultiXactIds than that.  (We have to do this even if we end
         * up just using our own TransactionId below, since some other backend
         * could incorporate our XID into a MultiXact immediately afterwards.)
         */
        MultiXactIdSetOldestMember();

        new_infomask |= HEAP_XMAX_SHARED_LOCK;

        /*
         * Check to see if we need a MultiXactId because there are multiple
         * lockers.
         *
         * HeapTupleSatisfiesUpdate will have set the HEAP_XMAX_INVALID bit if
         * the xmax was a MultiXactId but it was not running anymore. There is
         * a race condition, which is that the MultiXactId may have finished
         * since then, but that uncommon case is handled within
         * MultiXactIdExpand.
         *
         * There is a similar race condition possible when the old xmax was a
         * regular TransactionId.  We test TransactionIdIsInProgress again
         * just to narrow the window, but it's still possible to end up
         * creating an unnecessary MultiXactId.  Fortunately this is harmless.
         */
        if (!(old_infomask & (HEAP_XMAX_INVALID | HEAP_XMAX_COMMITTED))) {
            if (old_infomask & HEAP_XMAX_IS_MULTI) {
                /*
                 * If the XMAX is already a MultiXactId, then we need to
                 * expand it to include our own TransactionId.
                 */
                xid = MultiXactIdExpand((MultiXactId)xmax, xid);
                new_infomask |= HEAP_XMAX_IS_MULTI;
            } else if (TransactionIdIsInProgress(xmax)) {
                /*
                 * If the XMAX is a valid TransactionId, then we need to
                 * create a new MultiXactId that includes both the old locker
                 * and our own TransactionId.
                 */
                xid = MultiXactIdCreate(xmax, xid);
                new_infomask |= HEAP_XMAX_IS_MULTI;
            } else {
                /*
                 * Can get here iff HeapTupleSatisfiesUpdate saw the old xmax
                 * as running, but it finished before
                 * TransactionIdIsInProgress() got to run.	Treat it like
                 * there's no locker in the tuple.
                 */
            }
        } else {
            /*
             * There was no previous locker, so just insert our own
             * TransactionId.
             */
        }
    } else {
        /* We want an exclusive lock on the tuple */
        new_infomask |= HEAP_XMAX_EXCL_LOCK;
    }

    /*
     * If we didn't pin the visibility map page and the page has become all
     * visible while we were busy locking the buffer, or during some
     * subsequent window during which we had it unlocked, we'll have to unlock
     * and re-lock, to avoid holding the buffer lock across I/O.  That's a bit
     * unfortunate, especially since we'll now have to recheck whether the
     * tuple has been locked or updated under us, but hopefully it won't
     * happen very often.
     */
    if (vmbuffer == InvalidBuffer && PageIsAllVisible(page)) {
        LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);
        visibilitymap_pin(relation, block, &vmbuffer);
        LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
        goto l3;
    }

    if (TransactionIdIsNormal(xid)) {
        (void)heap_page_prepare_for_xid(relation, *buffer, xid, (new_infomask & HEAP_XMAX_IS_MULTI) ? true : false);
    }

    HeapTupleCopyBaseFromPage(tuple, page);

    START_CRIT_SECTION();

    /*
     * Store transaction information of xact locking the tuple.
     *
     * Note: Cmax is meaningless in this context, so don't set it; this avoids
     * possibly generating a useless combo CID.
     */
    tuple->t_data->t_infomask = new_infomask;
    HeapTupleHeaderClearHotUpdated(tuple->t_data);
    HeapTupleHeaderSetXmax(page, tuple->t_data, xid);
    /* Make sure there is no forward chain link in t_ctid */
    tuple->t_data->t_ctid = *tid;

    /* Clear bit on visibility map if needed */
    if (PageIsAllVisible(BufferGetPage(*buffer))) {
        visibilitymap_clear(relation, block, vmbuffer);
    }

    MarkBufferDirty(*buffer);

    /*
     * XLOG stuff.	You might think that we don't need an XLOG record because
     * there is no state change worth restoring after a crash.	You would be
     * wrong however: we have just written either a TransactionId or a
     * MultiXactId that may never have been seen on disk before, and we need
     * to make sure that there are XLOG entries covering those ID numbers.
     * Else the same IDs might be re-used after a crash, which would be
     * disastrous if this page made it to disk before the crash.  Essentially
     * we have to enforce the WAL log-before-data rule even in this case.
     * (Also, in a PITR log-shipping or 2PC environment, we have to have XLOG
     * entries for everything anyway.)
     */
    if (RelationNeedsWAL(relation)) {
        xl_heap_lock xlrec;
        XLogRecPtr recptr;

        xlrec.locking_xid = xid;
        xlrec.offnum = ItemPointerGetOffsetNumber(&tuple->t_self);
        xlrec.xid_is_mxact = ((new_infomask & HEAP_XMAX_IS_MULTI) != 0);
        xlrec.shared_lock = (mode == LockTupleShared);

        XLogBeginInsert();
        XLogRegisterData((char *)&xlrec, SizeOfHeapLock);
        XLogRegisterBuffer(0, *buffer, REGBUF_STANDARD);

        recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_LOCK);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    result = TM_Ok;

out_locked:
    LockBuffer(*buffer, BUFFER_LOCK_UNLOCK);

out_unlocked:
    if (BufferIsValid(vmbuffer)) {
        ReleaseBuffer(vmbuffer);
    }

    /*
     * Don't update the visibility map here. Locking a tuple doesn't change
     * visibility info.
     *
     * Now that we have successfully marked the tuple as locked, we can
     * release the lmgr tuple lock, if we had it.
     */
    if (have_tuple_lock) {
        UnlockTuple(relation, tid, tuple_lock_type);
    }

    return result;
}

/*
 * heap_inplace_update - update a tuple "in place" (ie, overwrite it)
 *
 * Overwriting violates both MVCC and transactional safety, so the uses
 * of this function in Postgres are extremely limited.	Nonetheless we
 * find some places to use it.
 *
 * The tuple cannot change size, and therefore it's reasonable to assume
 * that its null bitmap (if any) doesn't change either.  So we just
 * overwrite the data portion of the tuple without touching the null
 * bitmap or any of the header fields.
 *
 * tuple is an in-memory tuple structure containing the data to be written
 * over the target tuple.  Also, tuple->t_self identifies the target tuple.
 */
void heap_inplace_update(Relation relation, HeapTuple tuple)
{
    Buffer buffer;
    Page page;
    OffsetNumber offnum, maxoff;
    ItemId lp = NULL;
    HeapTupleHeader htup;
    uint32 oldlen;
    uint32 newlen;
    errno_t rc;

    buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(&(tuple->t_self)));
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    page = (Page)BufferGetPage(buffer);

    offnum = ItemPointerGetOffsetNumber(&(tuple->t_self));
    maxoff = PageGetMaxOffsetNumber(page);
    if (maxoff >= offnum) {
        lp = PageGetItemId(page, offnum);
    }

    if (maxoff < offnum || !ItemIdIsNormal(lp)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("heap_inplace_update: invalid lp")));
    }

    htup = (HeapTupleHeader)PageGetItem(page, lp);

    oldlen = ItemIdGetLength(lp) - htup->t_hoff;
    newlen = tuple->t_len - tuple->t_data->t_hoff;
    if (oldlen != newlen || htup->t_hoff != tuple->t_data->t_hoff) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("heap_inplace_update: wrong tuple length")));
    }

    /* NO EREPORT(ERROR) from here till changes are logged */
    START_CRIT_SECTION();

    rc = memcpy_s((char *)htup + htup->t_hoff, newlen, (char *)tuple->t_data + tuple->t_data->t_hoff, newlen);
    securec_check(rc, "\0", "\0");

    MarkBufferDirty(buffer);

    /* XLOG stuff */
    if (RelationNeedsWAL(relation)) {
        xl_heap_inplace xlrec;
        XLogRecPtr recptr;

        xlrec.offnum = ItemPointerGetOffsetNumber(&tuple->t_self);

        XLogBeginInsert();
        XLogRegisterData((char *)&xlrec, SizeOfHeapInplace);

        XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
        XLogRegisterBufData(0, (char *)htup + htup->t_hoff, newlen);

        recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_INPLACE);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buffer);

    /*
     * Send out shared cache inval if necessary.  Note that because we only
     * pass the new version of the tuple, this mustn't be used for any
     * operations that could change catcache lookup keys.  But we aren't
     * bothering with index updates either, so that's true a fortiori.
     */
    if (!IsBootstrapProcessingMode()) {
        CacheInvalidateHeapTuple(relation, tuple, NULL);
    }
}

/*
 * heap_freeze_tuple
 *
 * Check to see whether any of the XID fields of a tuple (xmin, xmax, xvac)
 * are older than the specified cutoff XID.  If so, replace them with
 * FrozenTransactionId or InvalidTransactionId as appropriate, and return
 * TRUE.  Return FALSE if nothing was changed.
 *
 * It is assumed that the caller has checked the tuple with
 * HeapTupleSatisfiesVacuum() and determined that it is not HEAPTUPLE_DEAD
 * (else we should be removing the tuple, not freezing it).
 *
 * NB: cutoff_xid *must* be <= the current global xmin, to ensure that any
 * XID older than it could neither be running nor seen as running by any
 * open transaction.  This ensures that the replacement will not change
 * anyone's idea of the tuple state.  Also, since we assume the tuple is
 * not HEAPTUPLE_DEAD, the fact that an XID is not still running allows us
 * to assume that it is either committed good or aborted, as appropriate;
 * so we need no external state checks to decide what to do.  (This is good
 * because this function is applied during WAL recovery, when we don't have
 * access to any such state, and can't depend on the hint bits to be set.)
 *
 * If the tuple is in a shared buffer, caller must hold an exclusive lock on
 * that buffer.
 *
 * Note: it might seem we could make the changes without exclusive lock, since
 * TransactionId read/write is assumed atomic anyway.  However there is a race
 * condition: someone who just fetched an old XID that we overwrite here could
 * conceivably not finish checking the XID against pg_clog before we finish
 * the VACUUM and perhaps truncate off the part of pg_clog he needs.  Getting
 * exclusive lock ensures no other backend is in process of checking the
 * tuple status.  Also, getting exclusive lock makes it safe to adjust the
 * infomask bits.
 */
bool heap_freeze_tuple(HeapTuple tuple, TransactionId cutoff_xid)
{
    bool changed = false;
    TransactionId xid;

    xid = HeapTupleGetRawXmin(tuple);
    if (TransactionIdIsNormal(xid) && TransactionIdPrecedes(xid, cutoff_xid)) {
        if (!RecoveryInProgress() && !TransactionIdDidCommit(xid)) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg_internal("uncommitted xmin %lu from before xid cutoff %lu needs to be frozen", xid,
                                            cutoff_xid)));
        }

        HeapTupleSetXmin(tuple, FrozenTransactionId);

        /*
         * Might as well fix the hint bits too; usually XMIN_COMMITTED will
         * already be set here, but there's a small chance not.
         */
        Assert(!HeapTupleHeaderXminInvalid(tuple->t_data));
        tuple->t_data->t_infomask |= HEAP_XMIN_COMMITTED;
        changed = true;
    }

    if (!(tuple->t_data->t_infomask & HEAP_XMAX_IS_MULTI)) {
        xid = HeapTupleGetRawXmax(tuple);
        if (TransactionIdIsNormal(xid) && TransactionIdPrecedes(xid, cutoff_xid)) {
            if (!RecoveryInProgress() && !(tuple->t_data->t_infomask & HEAP_IS_LOCKED) && TransactionIdDidCommit(xid)) {
                ereport(ERROR,
                        (errcode(ERRCODE_DATA_CORRUPTED), errmsg_internal("cannot freeze commited xmax %lu", xid)));
            }
            HeapTupleSetXmax(tuple, InvalidTransactionId);

            /*
             * The tuple might be marked either XMAX_INVALID or XMAX_COMMITTED
             * + LOCKED.  Normalize to INVALID just to be sure no one gets
             * confused.
             */
            tuple->t_data->t_infomask &= ~HEAP_XMAX_COMMITTED;
            tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
            HeapTupleHeaderClearHotUpdated(tuple->t_data);

            changed = true;
        }
    } else {
        /* ----------
         * XXX perhaps someday we should zero out very old MultiXactIds here?
         *
         * The only way a stale MultiXactId could pose a problem is if a
         * tuple, having once been multiply-share-locked, is not touched by
         * any vacuum or attempted lock or deletion for just over 4G MultiXact
         * creations, and then in the probably-narrow window where its xmax
         * is again a live MultiXactId, someone tries to lock or delete it.
         * Even then, another share-lock attempt would work fine.  An
         * exclusive-lock or delete attempt would face unexpected delay, or
         * in the very worst case get a deadlock error.  This seems an
         * extremely low-probability scenario with minimal downside even if
         * it does happen, so for now we don't do the extra bookkeeping that
         * would be needed to clean out MultiXactIds.
         * ----------
         */
    }

    return changed;
}

/*
 * heap_tuple_needs_freeze
 *
 * Check to see whether any of the XID fields of a tuple (xmin, xmax, xvac)
 * are older than the specified cutoff XID.  If so, return TRUE.
 *
 * It doesn't matter whether the tuple is alive or dead, we are checking
 * to see if a tuple needs to be removed or frozen.
 */
bool heap_tuple_needs_freeze(HeapTuple htup, TransactionId cutoff_xid, Buffer buf)
{
    TransactionId xid;
    HeapTupleHeader tuple = htup->t_data;

    xid = HeapTupleGetRawXmin(htup);
    if (TransactionIdIsNormal(xid) && TransactionIdPrecedes(xid, cutoff_xid)) {
        return true;
    }

    if (!(tuple->t_infomask & HEAP_XMAX_IS_MULTI)) {
        xid = HeapTupleGetRawXmax(htup);
        if (TransactionIdIsNormal(xid) && TransactionIdPrecedes(xid, cutoff_xid)) {
            return true;
        }
    }

    return false;
}

/* ----------------
 *		heap_restrpos	- restore position to marked location
 * ----------------
 */
void heap_restrpos(TableScanDesc sscan)
{
    HeapScanDesc scan = (HeapScanDesc) sscan;
    /* XXX no amrestrpos checking that ammarkpos called */
    if (!ItemPointerIsValid(&scan->rs_mctid)) {
        scan->rs_ctup.t_data = NULL;

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
        scan->rs_ctup.t_self = scan->rs_mctid;
        if (scan->rs_base.rs_pageatatime) {
            scan->rs_base.rs_cindex = scan->rs_mindex;
            heapgettup_pagemode(scan,
                NoMovementScanDirection,
                0, /* needn't recheck scan keys */
                NULL);
        } else
            heapgettup(scan,
                NoMovementScanDirection,
                0, /* needn't recheck scan keys */
                NULL);
    }
}

/*
 * If 'tuple' contains any visible XID greater than latest_removed_xid,
 * ratchet forwards latest_removed_xid to the greatest one found.
 * This is used as the basis for generating Hot Standby conflicts, so
 * if a tuple was never visible then removing it should not conflict
 * with queries.
 */
void HeapTupleHeaderAdvanceLatestRemovedXid(HeapTuple tuple, TransactionId *latest_removed_xid)
{
    HeapTupleHeader htup = tuple->t_data;
    TransactionId xmin = HeapTupleGetRawXmin(tuple);
    TransactionId xmax = HeapTupleGetRawXmax(tuple);

    /*
     * Ignore tuples inserted by an aborted transaction or if the tuple was
     * updated/deleted by the inserting transaction.
     *
     * Look for a committed hint bit, or if no xmin bit is set, check clog.
     * This needs to work on both master and standby, where it is used to
     * assess btree delete records.
     */
    if (HeapTupleHeaderXminCommitted(htup) || (!HeapTupleHeaderXminInvalid(htup) && TransactionIdDidCommit(xmin))) {
        if (xmax != xmin && TransactionIdFollows(xmax, *latest_removed_xid)) {
            *latest_removed_xid = xmax;
        }
    }

    /* *latest_removed_xid may still be invalid at end */
}

/*
 * Perform XLogInsert to register a heap cleanup info message. These
 * messages are sent once per VACUUM and are required because
 * of the phasing of removal operations during a lazy VACUUM.
 * see comments for vacuum_log_cleanup_info().
 */
XLogRecPtr log_heap_cleanup_info(const RelFileNode *rnode, TransactionId latest_removed_xid)
{
    xl_heap_cleanup_info xlrec;
    XLogRecPtr recptr;

    RelFileNodeRelCopy(xlrec.node, *rnode);

    xlrec.latestRemovedXid = latest_removed_xid;

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, SizeOfHeapCleanupInfo);

    recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_CLEANUP_INFO, false, rnode->bucketNode);

    return recptr;
}

/*
 * Perform XLogInsert for a heap-clean operation.  Caller must already
 * have modified the buffer and marked it dirty.
 *
 * Note: prior to Postgres 8.3, the entries in the nowunused[] array were
 * zero-based tuple indexes.  Now they are one-based like other uses
 * of OffsetNumber.
 *
 * We also include latest_removed_xid, which is the greatest XID present in
 * the removed tuples. That allows recovery processing to cancel or wait
 * for long standby queries that can still see these tuples.
 */
XLogRecPtr log_heap_clean(Relation reln, Buffer buffer, OffsetNumber *redirected, int nredirected,
                          OffsetNumber *nowdead, int ndead, OffsetNumber *nowunused, int nunused,
                          TransactionId latest_removed_xid, bool repair_fragmentation)
{
    xl_heap_clean xlrec;
    XLogRecPtr recptr;
    RelFileNode rnode;
    ForkNumber forkNum;
    BlockNumber blkNum;
    Page page;
    uint8 info;

    /* Caller should not call me on a non-WAL-logged relation */
    Assert(RelationNeedsWAL(reln));

    xlrec.latestRemovedXid = latest_removed_xid;
    xlrec.nredirected = nredirected;
    xlrec.ndead = ndead;

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, SizeOfHeapClean);

    XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
    /*
     * The OffsetNumber arrays are not actually in the buffer, but we pretend
     * that they are.  When XLogInsert stores the whole buffer, the offset
     * arrays need not be stored too.  Note that even if all three arrays are
     * empty, we want to expose the buffer as a candidate for whole-page
     * storage, since this record type implies a defragmentation operation
     * even if no item pointers changed state.
     */
    if (nredirected > 0) {
        XLogRegisterBufData(0, (char *)redirected, nredirected * sizeof(OffsetNumber) * 2);
    }

    if (ndead > 0) {
        XLogRegisterBufData(0, (char *)nowdead, ndead * sizeof(OffsetNumber));
    }

    if (nunused > 0) {
        XLogRegisterBufData(0, (char *)nowunused, nunused * sizeof(OffsetNumber));
    }

    info = XLOG_HEAP2_CLEAN;
    if (!repair_fragmentation) {
        info |= XLOG_HEAP2_NO_REPAIR_PAGE;
    }
    recptr = XLogInsert(RM_HEAP2_ID, info);

    BufferGetTag(buffer, &rnode, &forkNum, &blkNum);
    page = BufferGetPage(buffer);

    ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                     errmsg("[REDO_LOG_TRACE]log_heap_clean: ProcLastRecPtr:%lu,XactLastRecEnd:%lu,"
                            "recptr:%lu,oldPageLsn:%lu,newPageLsn:%lu, latest_removed_xid:%lu,"
                            "rnode(spcNode:%u, dbNode:%u, relNode:%u),forkNum:%d,blkNum:%u",
                            t_thrd.xlog_cxt.ProcLastRecPtr, t_thrd.xlog_cxt.XactLastRecEnd, recptr, PageGetLSN(page),
                            recptr, latest_removed_xid, rnode.spcNode, rnode.dbNode, rnode.relNode, forkNum, blkNum)));
    return recptr;
}

/*
 * Perform XLogInsert for a heap-freeze operation.	Caller must already
 * have modified the buffer and marked it dirty.
 */
XLogRecPtr log_heap_freeze(Relation reln, Buffer buffer, TransactionId cutoff_xid, OffsetNumber *offsets, int offcnt)
{
    xl_heap_freeze xlrec;
    XLogRecPtr recptr;

    /* Caller should not call me on a non-WAL-logged relation */
    Assert(RelationNeedsWAL(reln));
    /* nor when there are no tuples to freeze */
    Assert(offcnt > 0);

    xlrec.cutoff_xid = cutoff_xid;

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, SizeOfHeapFreeze);

    /*
     * The tuple-offsets array is not actually in the buffer, but pretend that
     * it is.  When XLogInsert stores the whole buffer, the offsets array need
     * not be stored too.
     */
    XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);
    XLogRegisterBufData(0, (char *)offsets, offcnt * sizeof(OffsetNumber));

    recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_FREEZE);

    return recptr;
}

XLogRecPtr log_cu_bcm(const RelFileNode *rnode, int col, uint64 block, int status, int count)
{
    xl_heap_bcm xlrec;
    XLogRecPtr recptr;

    /*
     * block is pointer to the last cu unit block;
     */
    RelFileNodeRelCopy(xlrec.node, *rnode);

    xlrec.block = block;
    xlrec.count = count;
    xlrec.status = status;
    xlrec.col = col;

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, SizeOfHeapBcm);

    recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_BCM, false, rnode->bucketNode);

    return recptr;
}

/*
 * Perform XLogInsert for a bcm set operation.  'block' is the block
 * being marked as status, and bcm_buffer is the buffer containing the
 * corresponding bcm map block.	Both should have already been modified
 * and dirtied.
 */
XLogRecPtr log_heap_bcm(const RelFileNode *rnode, int col, uint64 block, int status)
{
    xl_heap_bcm xlrec;
    XLogRecPtr recptr;

    RelFileNodeRelCopy(xlrec.node, *rnode);

    xlrec.block = block;
    xlrec.count = 1;
    xlrec.status = status;
    xlrec.col = col;

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, SizeOfHeapBcm);

    recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_BCM, false, rnode->bucketNode);

    return recptr;
}

/*
 * Perform XLogInsert for a heap-visible operation.  'block' is the block
 * being marked all-visible, and vm_buffer is the buffer containing the
 * corresponding visibility map block.	Both should have already been modified
 * and dirtied.
 *
 * If checksums are enabled, we also generate a full-page image of
 * heap_buffer, if necessary.
 */
XLogRecPtr log_heap_visible(RelFileNode rnode, BlockNumber block, Buffer heap_buffer, Buffer vm_buffer,
                            TransactionId cutoff_xid, bool free_dict)
{
    xl_heap_visible xlrec;
    XLogRecPtr recptr;
    Page page;
    int flags;

    Assert(BufferIsValid(vm_buffer));

    xlrec.block = block;
    xlrec.cutoff_xid = cutoff_xid;
    xlrec.free_dict = free_dict;

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, SizeOfHeapVisible);

    XLogRegisterBuffer(0, vm_buffer, 0);

    if (BufferIsValid(heap_buffer)) {
        page = BufferGetPage(heap_buffer);

        flags = REGBUF_STANDARD;
        if (!PageIsLogical(page) && !XLogHintBitIsNeeded()) {
            flags |= REGBUF_NO_IMAGE;
        }
        XLogRegisterBuffer(1, heap_buffer, flags);
    }

    recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_VISIBLE);

    return recptr;
}

/*
 * Perform XLogInsert for a heap-update operation.	Caller must already
 * have modified the buffer(s) and marked them dirty.
 */
static XLogRecPtr log_heap_update(Relation reln, Buffer oldbuf, const ItemPointer from, Buffer newbuf, HeapTuple newtup,
                                  HeapTuple old_key_tuple, bool all_visible_cleared, bool new_all_visible_cleared)
{
    xl_heap_update xlrec;
    xl_heap_header xlhdr;
    xl_heap_header xlhdr_idx;
    uint8 info;
    XLogRecPtr recptr;
    Page page = BufferGetPage(newbuf);
    bool need_tuple_data = RelationIsLogicallyLogged(reln);
    int bufflags;
    OffsetNumber maxoff;

    /* Caller should not call me on a non-WAL-logged relation */
    Assert(RelationNeedsWAL(reln));

    if (HeapTupleIsHeapOnly(newtup)) {
        info = XLOG_HEAP_HOT_UPDATE;
    } else {
        info = XLOG_HEAP_UPDATE;
    }

    XLogBeginInsert();
    maxoff = PageGetMaxOffsetNumber(page);
    /*
     * If new tuple is the single and first tuple on page...
     * If page is already compressed, should not init page,
     * or lead to inconsistency.
     */
    if (ItemPointerGetOffsetNumber(&(newtup->t_self)) == FirstOffsetNumber && maxoff == FirstOffsetNumber &&
        !PageIsCompressed(page)) {
        info |= XLOG_HEAP_INIT_PAGE;
        bufflags = REGBUF_STANDARD | REGBUF_WILL_INIT;
    } else
        bufflags = REGBUF_STANDARD;

    /* Prepare WAL data */
    xlrec.old_offnum = ItemPointerGetOffsetNumber(from);
    xlrec.new_offnum = ItemPointerGetOffsetNumber(&newtup->t_self);
    xlrec.flags = 0;
    if (all_visible_cleared) {
        xlrec.flags |= XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED;
    }
    if (new_all_visible_cleared) {
        xlrec.flags |= XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED;
    }
    if (need_tuple_data) {
        xlrec.flags |= XLH_UPDATE_CONTAINS_NEW_TUPLE;
        if (old_key_tuple) {
            if (reln->rd_rel->relreplident == REPLICA_IDENTITY_FULL)
                xlrec.flags |= XLH_UPDATE_CONTAINS_OLD_TUPLE;
            else
                xlrec.flags |= XLH_UPDATE_CONTAINS_OLD_KEY;
        }
    }
    if (need_tuple_data) {
        bufflags |= REGBUF_KEEP_DATA;
    }

    xlhdr.t_infomask2 = newtup->t_data->t_infomask2;
    xlhdr.t_infomask = newtup->t_data->t_infomask;
    xlhdr.t_hoff = newtup->t_data->t_hoff;

    /*
     * As with insert records, we need not store the rdata[2] segment
     * if we decide to store the whole buffer instead unless we're
     * doing logical decoding.
     */
    XLogRegisterBuffer(0, newbuf, bufflags);
    XLogRegisterBufData(0, (char *)&xlhdr, SizeOfHeapHeader);

    /* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
    XLogRegisterBufData(0, (char *)newtup->t_data + offsetof(HeapTupleHeaderData, t_bits),
                        newtup->t_len - offsetof(HeapTupleHeaderData, t_bits));

    if (oldbuf != newbuf) {
        XLogRegisterBuffer(1, oldbuf, REGBUF_STANDARD);
    }

    if (info & XLOG_HEAP_INIT_PAGE) {
        XLogRegisterData((char *)&((HeapPageHeader)(page))->pd_xid_base, sizeof(TransactionId));
    }

    XLogRegisterData((char *)&xlrec, SizeOfHeapUpdate);

    /* We need to log a tuple identity */
    if (need_tuple_data && old_key_tuple) {
        /* don't really need this, but its more comfy to decode */
        xlhdr_idx.t_infomask2 = old_key_tuple->t_data->t_infomask2;
        xlhdr_idx.t_infomask = old_key_tuple->t_data->t_infomask;
        xlhdr_idx.t_hoff = old_key_tuple->t_data->t_hoff;

        XLogRegisterData((char *)&xlhdr_idx, SizeOfHeapHeader);

        /* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
        XLogRegisterData((char *)old_key_tuple->t_data + offsetof(HeapTupleHeaderData, t_bits),
                         old_key_tuple->t_len - offsetof(HeapTupleHeaderData, t_bits));
    }

    /* filtering by origin on a row level is much more efficient */
    XLogIncludeOrigin();

    recptr = XLogInsert(RM_HEAP_ID, info);
    ereport(DEBUG4, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                     errmsg("[REDO_LOG_TRACE]log_heap_update: fromBlkNum:%u,fromOffsetNum:%hu,"
                            "newBlkNum:%u,newOffsetNum:%hu,"
                            "t_infomask2:%hu,t_infomask:%hu,t_hoff:%hhu,flags:%hhu,bufflags:%d,newLen:%u",
                            ItemPointerGetBlockNumber(from), ItemPointerGetOffsetNumber(from),
                            ItemPointerGetBlockNumber(&newtup->t_self), ItemPointerGetOffsetNumber(&newtup->t_self),
                            xlhdr.t_infomask2, xlhdr.t_infomask, xlhdr.t_hoff, xlrec.flags, bufflags, newtup->t_len)));

    return recptr;
}

/*
 * Build a heap tuple representing the configured REPLICA IDENTITY to represent
 * the old tuple in a UPDATE or DELETE.
 *
 * Returns NULL if there's no need to log an identity or if there's no suitable
 * key in the Relation relation.
 */
static HeapTuple ExtractReplicaIdentity(Relation relation, HeapTuple tp, bool key_changed, bool *copy)
{
    TupleDesc desc = RelationGetDescr(relation);
    Oid replidindex;
    Relation idx_rel;
    char relreplident;
    HeapTuple key_tuple = NULL;
    bool nulls[MaxHeapAttributeNumber];
    Datum values[MaxHeapAttributeNumber];
    int natt;
    errno_t rc = 0;
    *copy = false;

    if (!RelationIsLogicallyLogged(relation)) {
        return NULL;
    }

    bool is_null = true;
    Relation rel = heap_open(RelationRelationId, AccessShareLock);
    Oid relid = RelationIsPartition(relation) ? relation->parentId : relation->rd_id;
    HeapTuple tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                        errmsg("pg_class entry for relid %u vanished during ExtractReplicaIdentity", relid)));
    }
    Datum replident = heap_getattr(tuple, Anum_pg_class_relreplident, RelationGetDescr(rel), &is_null);
    heap_close(rel, AccessShareLock);
    heap_freetuple(tuple);

    if (is_null) {
        relreplident = REPLICA_IDENTITY_NOTHING;
    } else {
        relreplident = CharGetDatum(replident);
    }

    if (replident == REPLICA_IDENTITY_NOTHING) {
        return NULL;
    }

    if (replident == REPLICA_IDENTITY_FULL) {
        /*
         * When logging the entire old tuple, it very well could contain
         * toasted columns. If so, force them to be inlined.
         */
        if (HeapTupleHasExternal(tp)) {
            *copy = true;
            tp = toast_flatten_tuple(tp, RelationGetDescr(relation));
        }
        return tp;
    }

    /* if the key hasn't changed and we're only logging the key, we're done */
    if (!key_changed) {
        return NULL;
    }

    /* find the replica identity index */
    replidindex = RelationGetReplicaIndex(relation);
    if (!OidIsValid(replidindex)) {
        ereport(DEBUG4, (errmsg("could not find configured replica identity for table \"%s\"",
                                RelationGetRelationName(relation))));
        return NULL;
    }

    idx_rel = RelationIdGetRelation(replidindex);

    /* deform tuple, so we have fast access to columns */
    heap_deform_tuple(tp, desc, values, nulls);

    /* set all columns to NULL, regardless of whether they actually are */
    rc = memset_s(nulls, sizeof(nulls), 1, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    /*
     * Now set all columns contained in the index to NOT NULL, they cannot
     * currently be NULL.
     */
    for (natt = 0; natt < IndexRelationGetNumberOfKeyAttributes(idx_rel); natt++) {
        int attno = idx_rel->rd_index->indkey.values[natt];

        if (attno < 0) {
            /*
             * The OID column can appear in an index definition, but that's
             * OK, because we always copy the OID if present (see below).
             * Other system columns may not.
             */
            if (attno == ObjectIdAttributeNumber) {
                continue;
            }
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("system column in index")));
        }
        nulls[attno - 1] = false;
    }

    key_tuple = heap_form_tuple(desc, values, nulls);
    *copy = true;
    RelationClose(idx_rel);

    /*
     * Always copy oids if the table has them, even if not included in the
     * index. The space in the logged tuple is used anyway, so there's little
     * point in not including the information.
     */
    if (relation->rd_rel->relhasoids) {
        HeapTupleSetOid(key_tuple, HeapTupleGetOid(tp));
    }

    /*
     * If the tuple, which by here only contains indexed columns, still has
     * toasted columns, force them to be inlined. This is somewhat unlikely
     * since there's limits on the size of indexed columns, so we don't
     * duplicate toast_flatten_tuple()s functionality in the above loop over
     * the indexed columns, even if it would be more efficient.
     */
    if (HeapTupleHasExternal(key_tuple)) {
        HeapTuple oldtup = key_tuple;

        key_tuple = toast_flatten_tuple(oldtup, RelationGetDescr(relation));
        heap_freetuple(oldtup);
    }

    return key_tuple;
}

/*
 * Perform XLogInsert of a HEAP_NEWPAGE record to WAL. Caller is responsible
 * for writing the page to disk after calling this routine.
 *
 * Note: If you're using this function, you should be building pages in private
 * memory and writing them directly to smgr.  If you're using buffers, call
 * log_newpage_buffer instead.
 *
 * Note: the NEWPAGE log record is used for both heaps and indexes, so do
 * not do anything that assumes we are touching a heap.
 */
XLogRecPtr log_newpage(RelFileNode *rnode, ForkNumber forkNum, BlockNumber blkno, Page page, bool page_std)
{
    int flags;
    XLogRecPtr recptr;

    /* NO ELOG(ERROR) from here till newpage op is logged */
    START_CRIT_SECTION();

    flags = REGBUF_FORCE_IMAGE;
    if (page_std) {
        flags |= REGBUF_STANDARD;
    }

    XLogBeginInsert();
    XLogRegisterBlock(0, rnode, forkNum, blkno, page, flags);

    recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_NEWPAGE);

    /*
     * The page may be uninitialized. If so, we can't set the LSN and TLI
     * because that would corrupt the page.
     */
    if (!PageIsNew(page)) {
        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    return recptr;
}

XLogRecPtr log_logical_newpage(RelFileNode *rnode, ForkNumber forkNum, BlockNumber blk, Page page, Buffer buffer)
{
    xl_heap_logical_newpage xlrec;
    XLogRecPtr recptr;

    /* NO ELOG(ERROR) from here till newpage op is logged */
    START_CRIT_SECTION();

    xlrec.blkno = blk;
    xlrec.blockSize = BLCKSZ;

    RelFileNodeRelCopy(xlrec.node, *rnode);

    xlrec.forknum = forkNum;
    xlrec.type = ROW_STORE;
    xlrec.attid = 0;
    xlrec.offset = 0;

    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, SizeOfHeapLogicalNewPage);

    /*
     * We need not to RegisterBuffer for logical newpage. But when
     * we use pg_rewind to recover a primary to stanby maybe appear
     * another problem; Explame for the scene:
     * 1: when occur two primarys; primary1 create table t1(relfilenode
     *    is 16385), copy page A to t1(LSN for A is 100); primary2 create
     *    table t1(relfilenode is 16385), copy page B to t1(LSN for B is
     *    120); when pg_rewind primary2, then start primary2 to catchup
     *    to primary1, because of lsn, the page A will not cover page B.
     *    So we register the newpage Buffer and pg_rewind will copy A
     *    to cover B.
     */
    XLogRegisterBuffer(0, buffer, REGBUF_NO_IMAGE);

    recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_LOGICAL_NEWPAGE, false, rnode->bucketNode);

    PageSetLSN(page, recptr);
    PageSetLogical(page);

    END_CRIT_SECTION();

    return recptr;
}

XLogRecPtr log_logical_newcu(RelFileNode *rnode, ForkNumber forkNum, int attid, Size offset, int size, char *cuData)
{
    xl_heap_logical_newpage xlrec;
    XLogRecPtr recptr;

    Assert(rnode->bucketNode == InvalidBktId);
    /* NO ELOG(ERROR) from here till newpage op is logged */
    START_CRIT_SECTION();

    xlrec.blkno = 0;
    xlrec.blockSize = size;

    RelFileNodeRelCopy(xlrec.node, *rnode);

    xlrec.forknum = forkNum;
    xlrec.type = COLUMN_STORE;
    xlrec.attid = attid;
    xlrec.offset = offset;
    /* flag of save cu xlog */
    if (cuData != NULL) {
        xlrec.hasdata = true;
    } else {
        xlrec.hasdata = false;
    }
    XLogBeginInsert();
    XLogRegisterData((char *)&xlrec, SizeOfHeapLogicalNewPage);
    if (cuData != NULL) {
        XLogRegisterData(cuData, size);
    }
    recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_LOGICAL_NEWPAGE, false, rnode->bucketNode);

    END_CRIT_SECTION();

    return recptr;
}

/*
 * Perform XLogInsert of a HEAP_NEWPAGE record to WAL.
 *
 * Caller should initialize the buffer and mark it dirty before calling this
 * function.  This function will set the page LSN and TLI.
 *
 * Note: the NEWPAGE log record is used for both heaps and indexes, so do
 * not do anything that assumes we are touching a heap.
 */
XLogRecPtr log_newpage_buffer(Buffer buffer, bool page_std)
{
    Page page = BufferGetPage(buffer);
    RelFileNode rnode;
    ForkNumber forkNum;
    BlockNumber blkno;

    /* We should be in a critical section. */
    Assert(t_thrd.int_cxt.CritSectionCount > 0);

    BufferGetTag(buffer, &rnode, &forkNum, &blkno);

    return log_newpage(&rnode, forkNum, blkno, page, page_std);
}

/*
 * Handles CLEANUP_INFO
 */
static void heap_xlog_cleanup_info(XLogReaderState *record)
{
    xl_heap_cleanup_info *xlrec = (xl_heap_cleanup_info *)XLogRecGetData(record);

    RelFileNode tmp_node;
    RelFileNodeCopy(tmp_node, xlrec->node, XLogRecGetBucketId(record));

    if (InHotStandby && g_supportHotStandby) {
        ResolveRecoveryConflictWithSnapshot(xlrec->latestRemovedXid, tmp_node);
    }

    /*
     * Actual operation is a no-op. Record type exists to provide a means for
     * conflict processing to occur before we begin index vacuum actions. see
     * vacuumlazy.c and also comments in btvacuumpage()
     *
     * Backup blocks are not used in cleanup_info records
     */
    Assert(!XLogRecHasAnyBlockRefs(record));
}

/*
 * Handles HEAP2_CLEAN record type
 */
static void heap_xlog_clean(XLogReaderState *record)
{
    xl_heap_clean *xlrec = (xl_heap_clean *)XLogRecGetData(record);
    RedoBufferInfo buffer;
    Size freespace = 0;
    XLogRedoAction action;
    RelFileNode rnode;
    BlockNumber blkno;
    bool repairFragmentation = true;

    if ((XLogRecGetInfo(record) & XLOG_HEAP2_NO_REPAIR_PAGE) != 0) {
        repairFragmentation = false;
    }

    XLogRecGetBlockTag(record, HEAP_CLEAN_ORIG_BLOCK_NUM, &rnode, NULL, &blkno);

    /*
     * We're about to remove tuples. In Hot Standby mode, ensure that there's
     * no queries running for which the removed tuples are still visible.
     *
     * Not all HEAP2_CLEAN records remove tuples with xids, so we only want to
     * conflict on the records that cause MVCC failures for user queries. If
     * latestRemovedXid is invalid, skip conflict processing.
     */
    if (InHotStandby && g_supportHotStandby && TransactionIdIsValid(xlrec->latestRemovedXid)) {
        ResolveRecoveryConflictWithSnapshot(xlrec->latestRemovedXid, rnode);
    }

    /*
     * If we have a full-page image, restore it (using a cleanup lock) and
     * we're done.
     */
    action = XLogReadBufferForRedoExtended(record, HEAP_CLEAN_ORIG_BLOCK_NUM, RBM_NORMAL, true, &buffer);
    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogRecGetData(record);
        Size blkdatalen;
        char *blkdata = NULL;
        blkdata = XLogRecGetBlockData(record, HEAP_CLEAN_ORIG_BLOCK_NUM, &blkdatalen);

        HeapXlogCleanOperatorPage(&buffer, (void *)maindata, (void *)blkdata, blkdatalen, &freespace,
                                  /* for performance better not dump log */
                                  /* Update all item pointers per the record, and repair fragmentation */
                                  /* for performance better not dump log */
                                  repairFragmentation);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        /*
         * Note: we don't worry about updating the page's prunability hints. At
         * worst this will cause an extra prune cycle to occur soon.
         */
        UnlockReleaseBuffer(buffer.buf);
    }

    /*
     * Update the FSM as well.
     *
     * XXX: Don't do this if the page was restored from full page image. We
     * don't bother to update the FSM in that case, it doesn't need to be
     * totally accurate anyway.
     */
    if (action == BLK_NEEDS_REDO) {
        XLogRecordPageWithFreeSpace(rnode, blkno, freespace);
    }
}

static void heap_xlog_freeze(XLogReaderState *record)
{
    xl_heap_freeze *xlrec = (xl_heap_freeze *)XLogRecGetData(record);
    TransactionId cutoff_xid = xlrec->cutoff_xid;
    RedoBufferInfo buffer;

    /*
     * In Hot Standby mode, ensure that there's no queries running which still
     * consider the frozen xids as running.
     */
    if (InHotStandby && g_supportHotStandby) {
        RelFileNode rnode;

        (void)XLogRecGetBlockTag(record, HEAP_FREEZE_ORIG_BLOCK_NUM, &rnode, NULL, NULL);
        ResolveRecoveryConflictWithSnapshot(cutoff_xid, rnode);
    }

    if (XLogReadBufferForRedo(record, HEAP_FREEZE_ORIG_BLOCK_NUM, &buffer) == BLK_NEEDS_REDO) {
        char *maindata = XLogRecGetData(record);
        Size blkdatalen;
        char *blkdata = NULL;
        blkdata = XLogRecGetBlockData(record, HEAP_FREEZE_ORIG_BLOCK_NUM, &blkdatalen);

        HeapXlogFreezeOperatorPage(&buffer, (void *)maindata, (void *)blkdata, blkdatalen);
        /* offsets[] entries are one-based */
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

/*
 * Replay XLOG_HEAP2_VISIBLE record.
 * The critical integrity requirement here is that we must never end up with
 * a situation where the visibility map bit is set, and the page-level
 * PD_ALL_VISIBLE bit is clear. If that were to occur, then a subsequent
 * page modification would fail to clear the visibility map bit.
 */
static void heap_xlog_visible(XLogReaderState *record)
{
    xl_heap_visible *xlrec = (xl_heap_visible *)XLogRecGetData(record);
    RedoBufferInfo vmbuffer;
    RedoBufferInfo buffer;
    XLogRedoAction action;
    RelFileNode rnode;

    /* In log_heap_visible,  block 0 is vm_buffer, block 1 is heap_buffer.
     * the vm and heap must have same relfilenode. so whether use block 0 or 1
     * is correct for relfilenode */
    (void)XLogRecGetBlockTag(record, HEAP_VISIBLE_VM_BLOCK_NUM, &rnode, NULL, NULL);

    /*
     * If there are any Hot Standby transactions running that have an xmin
     * horizon old enough that this page isn't all-visible for them, they
     * might incorrectly decide that an index-only scan can skip a heap fetch.
     *
     * NB: It might be better to throw some kind of "soft" conflict here that
     * forces any index-only scan that is in flight to perform heap fetches,
     * rather than killing the transaction outright.
     */
    if (InHotStandby && g_supportHotStandby) {
        ResolveRecoveryConflictWithSnapshot(xlrec->cutoff_xid, rnode);
    }

    if (XLogRecHasBlockRef(record, HEAP_VISIBLE_DATA_BLOCK_NUM)) {
        /*
         * Read the heap page, if it was append to the buffer portion and still exists.
         * If the heap file has dropped or truncated later in recovery, we don't need
         * to update the page, but we'd better still update the visibility map.
         */
        action = XLogReadBufferForRedo(record, HEAP_VISIBLE_DATA_BLOCK_NUM, &buffer);
        if (action == BLK_NEEDS_REDO) {
            char *maindata = XLogRecGetData(record);
            HeapXlogVisibleOperatorPage(&buffer, (void *)maindata);
            MarkBufferDirty(buffer.buf);
        } else if (action == BLK_RESTORED) {
            /*
             * If heap block was backed up, we already restored it and there's
             * nothing more to do. (This can only happen with checksums or
             * wal_log_hints enabled.)
             */
        }

        if (BufferIsValid(buffer.buf)) {
            UnlockReleaseBuffer(buffer.buf);
        }
    }

    /*
     * Even if we skipped the heap page update due to the LSN interlock, it's
     * still safe to update the visibility map. Any WAL record that clears
     * the visibility map bit does so before checking the page LSN, so any
     * bits that need to be cleared will still be cleared.
     */
    if (XLogReadBufferForRedoExtended(record, HEAP_VISIBLE_VM_BLOCK_NUM, RBM_ZERO_ON_ERROR, false, &vmbuffer) ==
        BLK_NEEDS_REDO) {
        /* initialize the page if it was read as zeros */
        char *maindata = XLogRecGetData(record);
        /*
         * XLogReadBufferForRedoExtended locked the buffer. But
         * visibilitymap_set will handle locking itself.
         */
        HeapXlogVisibleOperatorVmpage(&vmbuffer, (void *)maindata);

        /*
         * Don't set the bit if replay has already passed this point.
         *
         * It might be safe to do this unconditionally; if replay has passed
         * this point, we'll replay at least as far this time as we did
         * before, and if this bit needs to be cleared, the record responsible
         * for doing so should be again replayed, and clear it.  For right
         * now, out of an abundance of conservatism, we use the same test here
         * we did for the heap page.  If this results in a dropped bit, no
         * real harm is done; and the next VACUUM will fix it.
         */

    } else if (BufferIsValid(vmbuffer.buf)) {
        UnlockReleaseBuffer(vmbuffer.buf);
    }
}

/* Replay XLOG_HEAP2_BCM record. */
void heap_bcm_redo(xl_heap_bcm *xlrec, RelFileNode node, XLogRecPtr lsn)
{
    int col = xlrec->col;
    Relation reln = CreateFakeRelcacheEntry(node);
    Buffer bcmbuffer = InvalidBuffer;

    if (col > 0) { /* cloumn store */
        BlockNumber curBcmBlock = 0;
        BlockNumber nextBcmBlock = 0;
        int i = 0;
        int align_size = CUAlignUtils::GetCuAlignSizeColumnId(col);
        /* read current bcm block */
        curBcmBlock = HEAPBLK_TO_BCMBLOCK(xlrec->block + i);
        nextBcmBlock = curBcmBlock;
        BCM_CStore_pin(reln, col, ((xlrec->block + i) * align_size), &bcmbuffer);
        LockBuffer(bcmbuffer, BUFFER_LOCK_EXCLUSIVE);

        do {
            /* deal with bcm block switch */
            if (nextBcmBlock != curBcmBlock) {
                curBcmBlock = nextBcmBlock;

                /* release last bcm block and read in the next one */
                UnlockReleaseBuffer(bcmbuffer);

                BCM_CStore_pin(reln, col, ((xlrec->block + i) * align_size), &bcmbuffer);
                LockBuffer(bcmbuffer, BUFFER_LOCK_EXCLUSIVE);
            }

            /*
             * Don't set the bit if replay has already passed this point.
             * and we are in t_thrd.xlog_cxt.InRecovery, no need to consider log_heap_bcm.
             */
            if (!XLByteLE(lsn, PageGetLSN(BufferGetPage(bcmbuffer)))) {
                BCMSetStatusBit(reln, xlrec->block + i, bcmbuffer, xlrec->status, col);
                ereport(DEBUG2, (errmsg("BCMSetStatusBit: oid:%u col:%d block:%lu status: %d", reln->rd_node.relNode,
                                        col, xlrec->block + i, NOTSYNCED)));
            }

            i++;
            nextBcmBlock = HEAPBLK_TO_BCMBLOCK(xlrec->block + i);
        } while (i < xlrec->count);

        UnlockReleaseBuffer(bcmbuffer);

    } else { /* row store */
        BCM_pin(reln, xlrec->block, &bcmbuffer);
        LockBuffer(bcmbuffer, BUFFER_LOCK_EXCLUSIVE);
        if (!XLByteLE(lsn, PageGetLSN(BufferGetPage(bcmbuffer)))) {
            BCMSetStatusBit(reln, xlrec->block, bcmbuffer, xlrec->status, col);
        }
        UnlockReleaseBuffer(bcmbuffer);
    }

    FreeFakeRelcacheEntry(reln);
}

static void heap_xlog_bcm(XLogReaderState *record)
{
    XLogRecPtr lsn = record->EndRecPtr;
    xl_heap_bcm *xlrec = (xl_heap_bcm *)XLogRecGetData(record);
    RelFileNode tmp_node;
    RelFileNodeCopy(tmp_node, xlrec->node, XLogRecGetBucketId(record));
    heap_bcm_redo(xlrec, tmp_node, lsn);
}
static void heap_xlog_newpage(XLogReaderState *record)
{
    RedoBufferInfo buffer;

    /*
     * Full-page image (FPI) records contain nothing else but a backup
     * block. The block reference must include a full-page image -
     * otherwise there would be no point in this record.
     *
     * No recovery conflicts are generated by these generic records - if a
     * resource manager needs to generate conflicts, it has to define a
     * separate WAL record type and redo routine.
     */
    if (XLogReadBufferForRedo(record, HEAP_NEWPAGE_ORIG_BLOCK_NUM, &buffer) != BLK_RESTORED) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("unexpected result when restoring backup block")));
    }
    UnlockReleaseBuffer(buffer.buf);
}

inline static void heap_xlog_allvisiblecleared(RelFileNode target_node, BlockNumber blkno)
{
    Relation reln = CreateFakeRelcacheEntry(target_node);
    Buffer vmbuffer = InvalidBuffer;
    visibilitymap_pin(reln, blkno, &vmbuffer);
    visibilitymap_clear(reln, blkno, vmbuffer);
    ReleaseBuffer(vmbuffer);
    FreeFakeRelcacheEntry(reln);
}
static void heap_xlog_delete(XLogReaderState *record)
{
    xl_heap_delete *xlrec = (xl_heap_delete *)XLogRecGetData(record);
    RedoBufferInfo buffer;

    /*
     * The visibility map may need to be fixed even if the heap page is
     * already up-to-date.
     */
    if (xlrec->flags & XLH_DELETE_ALL_VISIBLE_CLEARED) {
        RelFileNode target_node;
        BlockNumber blkno;

        XLogRecGetBlockTag(record, HEAP_DELETE_ORIG_BLOCK_NUM, &target_node, NULL, &blkno);
        heap_xlog_allvisiblecleared(target_node, blkno);
    }

    if (XLogReadBufferForRedo(record, HEAP_DELETE_ORIG_BLOCK_NUM, &buffer) == BLK_NEEDS_REDO) {
        char *maindata = XLogRecGetData(record);
        TransactionId recordxid = XLogRecGetXid(record);

        HeapXlogDeleteOperatorPage(&buffer, (void *)maindata, recordxid);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        /* Mark the page as a candidate for pruning */
        UnlockReleaseBuffer(buffer.buf);

        /* Make sure there is no forward chain link in t_ctid */
    }
}

static void heap_xlog_insert(XLogReaderState *record)
{
    Pointer rec_data = (Pointer)XLogRecGetData(record);
    bool isinit = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;
    xl_heap_insert *xlrec = NULL;
    RedoBufferInfo buffer;
    Size freespace = 0;
    XLogRedoAction action;
    RelFileNode target_node;
    BlockNumber blkno;

    rec_data = (Pointer)XLogRecGetData(record);
    if (isinit) {
        rec_data += sizeof(TransactionId);
    }
    xlrec = (xl_heap_insert *)rec_data;

    XLogRecGetBlockTag(record, HEAP_INSERT_ORIG_BLOCK_NUM, &target_node, NULL, &blkno);

    /*
     * The visibility map may need to be fixed even if the heap page is
     * already up-to-date.
     */
    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED) {
        heap_xlog_allvisiblecleared(target_node, blkno);
    }

    /*
     * If we inserted the first and only tuple on the page, re-initialize
     * the page from scratch.
     */
    if (isinit) {
        XLogInitBufferForRedo(record, HEAP_INSERT_ORIG_BLOCK_NUM, &buffer);
        action = BLK_NEEDS_REDO;
    } else {
        action = XLogReadBufferForRedo(record, HEAP_INSERT_ORIG_BLOCK_NUM, &buffer);
    }

    if (action == BLK_NEEDS_REDO) {
        char *maindata = XLogRecGetData(record);
        TransactionId recordxid = XLogRecGetXid(record);
        Size blkdatalen;
        char *blkdata = NULL;

        blkdata = XLogRecGetBlockData(record, HEAP_INSERT_ORIG_BLOCK_NUM, &blkdatalen);

        HeapXlogInsertOperatorPage(
            /* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
            &buffer, (void *)maindata, isinit, (void *)blkdata, blkdatalen, recordxid, &freespace);

        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }

    /*
     * If the page is running low on free space, update the FSM as well.
     * Arbitrarily, our definition of "low" is less than 20%. We can't do much
     * better than that without knowing the fill-factor for the table.
     *
     * XXX: Don't do this if the page was restored from full page image. We
     * don't bother to update the FSM in that case, it doesn't need to be
     * totally accurate anyway.
     */
    if (action == BLK_NEEDS_REDO && freespace < BLCKSZ / 5) {
        XLogRecordPageWithFreeSpace(target_node, blkno, freespace);
    }
}

/*
 * Handles MULTI_INSERT record type.
 */
static void heap_xlog_multi_insert(XLogReaderState *record)
{
    xl_heap_multi_insert *xlrec = NULL;
    RelFileNode rnode;
    BlockNumber blkno;
    RedoBufferInfo buffer;
    Size freespace = 0;
    bool isinit = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;
    XLogRedoAction action;
    Pointer rec_data;

    /*
     * Insertion doesn't overwrite MVCC data, so no conflict processing is
     * required.
     */
    rec_data = (Pointer)XLogRecGetData(record);
    if (isinit) {
        rec_data += sizeof(TransactionId);
    }
    xlrec = (xl_heap_multi_insert *)rec_data;

    XLogRecGetBlockTag(record, HEAP_MULTI_INSERT_ORIG_BLOCK_NUM, &rnode, NULL, &blkno);

    /*
     * The visibility map may need to be fixed even if the heap page is
     * already up-to-date.
     */
    if (xlrec->flags & XLH_INSERT_ALL_VISIBLE_CLEARED) {
        heap_xlog_allvisiblecleared(rnode, blkno);
    }

    if (isinit) {
        XLogInitBufferForRedo(record, HEAP_MULTI_INSERT_ORIG_BLOCK_NUM, &buffer);

        action = BLK_NEEDS_REDO;
    } else {
        action = XLogReadBufferForRedo(record, HEAP_MULTI_INSERT_ORIG_BLOCK_NUM, &buffer);
    }

    if (action == BLK_NEEDS_REDO) {
        /* Tuples are stored as block data */
        char *maindata = XLogRecGetData(record);
        TransactionId recordxid = XLogRecGetXid(record);
        Size blkdatalen;
        char *blkdata = NULL;
        blkdata = XLogRecGetBlockData(record, HEAP_MULTI_INSERT_ORIG_BLOCK_NUM, &blkdatalen);

        HeapXlogMultiInsertOperatorPage(

            /* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
            &buffer, (void *)maindata, isinit, (void *)blkdata, blkdatalen, recordxid, &freespace);

        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }

    /*
     * If the page is running low on free space, update the FSM as well.
     * Arbitrarily, our definition of "low" is less than 20%. We can't do much
     * better than that without knowing the fill-factor for the table.
     *
     * XXX: Don't do this if the page was restored from full page image. We
     * don't bother to update the FSM in that case, it doesn't need to be
     * totally accurate anyway.
     */
    if (action == BLK_NEEDS_REDO && freespace < BLCKSZ / 5) {
        XLogRecordPageWithFreeSpace(rnode, blkno, freespace);
    }
}

/*
 * Handles UPDATE and HOT_UPDATE
 */
static void heap_xlog_update(XLogReaderState *record, bool hot_update)
{
    xl_heap_update *xlrec = (xl_heap_update *)XLogRecGetData(record);
    RelFileNode rnode;
    BlockNumber oldblk;
    BlockNumber newblk;
    RedoBufferInfo obuffer, nbuffer;
    Size freespace = 0;
    XLogRedoAction oldaction;
    XLogRedoAction newaction;
    bool isinit = (XLogRecGetInfo(record) & XLOG_HEAP_INIT_PAGE) != 0;
    Pointer rec_data;

    rec_data = (Pointer)XLogRecGetData(record);
    if (isinit) {
        rec_data += sizeof(TransactionId);
    }

    xlrec = (xl_heap_update *)rec_data;

    XLogRecGetBlockTag(record, HEAP_UPDATE_NEW_BLOCK_NUM, &rnode, NULL, &newblk);
    if (XLogRecGetBlockTag(record, HEAP_UPDATE_OLD_BLOCK_NUM, NULL, NULL, &oldblk)) {
        /* HOT updates are never done across pages */
        Assert(!hot_update);
    } else {
        oldblk = newblk;
    }

    /*
     * The visibility map may need to be fixed even if the heap page is
     * already up-to-date.
     */
    if (xlrec->flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) {
        heap_xlog_allvisiblecleared(rnode, oldblk);
    }

    /*
     * In normal operation, it is important to lock the two pages in
     * page-number order, to avoid possible deadlocks against other update
     * operations going the other way.	However, during WAL replay there can
     * be no other update happening, so we don't need to worry about that. But
     * we *do* need to worry that we don't expose an inconsistent state to Hot
     * Standby queries --- so the original page can't be unlocked before we've
     * added the new tuple to the new page.
     *
     * Deal with old tuple version
     */

    oldaction = XLogReadBufferForRedo(record,
                                      (oldblk == newblk) ? HEAP_UPDATE_NEW_BLOCK_NUM : HEAP_UPDATE_OLD_BLOCK_NUM,
                                      &obuffer);
    if (oldaction == BLK_NEEDS_REDO) {
        /* Set forward chain link in t_ctid */

        /* Mark the page as a candidate for pruning */
        char *maindata = XLogRecGetData(record);
        TransactionId recordxid = XLogRecGetXid(record);

        HeapXlogUpdateOperatorOldpage(&obuffer, (void *)maindata, hot_update, isinit, newblk, recordxid);
        // too much log may slow down the speed of xlog, so only write log
        // when log level belows DEBUG4
        MarkBufferDirty(obuffer.buf);
    }

    /*
     * Read the page the new tuple goes into, if different from old.
     */
    if (oldblk == newblk) {
        nbuffer = obuffer;
        newaction = oldaction;
    } else if (isinit) {
        XLogInitBufferForRedo(record, HEAP_UPDATE_NEW_BLOCK_NUM, &nbuffer);
        newaction = BLK_NEEDS_REDO;
    } else {
        newaction = XLogReadBufferForRedo(record, HEAP_UPDATE_NEW_BLOCK_NUM, &nbuffer);
    }

    /*
     * The visibility map may need to be fixed even if the heap page is
     * already up-to-date.
     */
    if (xlrec->flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) {
        heap_xlog_allvisiblecleared(rnode, newblk);
    }

    /* Deal with new tuple */
    if (newaction == BLK_NEEDS_REDO) {
        char *maindata = XLogRecGetData(record);
        Size blkdatalen;
        char *blkdata = NULL;
        TransactionId recordxid = XLogRecGetXid(record);
        blkdata = XLogRecGetBlockData(record, HEAP_UPDATE_NEW_BLOCK_NUM, &blkdatalen);

        HeapXlogUpdateOperatorNewpage(&nbuffer, (void *)maindata, isinit, (void *)blkdata, blkdatalen, recordxid,
                                      &freespace);

        /* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */

        /* Make sure there is no forward chain link in t_ctid */

        MarkBufferDirty(nbuffer.buf);
    }

    if (BufferIsValid(nbuffer.buf) && nbuffer.buf != obuffer.buf) {
        UnlockReleaseBuffer(nbuffer.buf);
    }
    if (BufferIsValid(obuffer.buf)) {
        UnlockReleaseBuffer(obuffer.buf);
    }

    /*
     * If the new page is running low on free space, update the FSM as well.
     * Arbitrarily, our definition of "low" is less than 20%. We can't do much
     * better than that without knowing the fill-factor for the table.
     *
     * However, don't update the FSM on HOT updates, because after crash
     * recovery, either the old or the new tuple will certainly be dead and
     * prunable. After pruning, the page will have roughly as much free space
     * as it did before the update, assuming the new tuple is about the same
     * size as the old one.
     *
     * XXX: Don't do this if the page was restored from full page image. We
     * don't bother to update the FSM in that case, it doesn't need to be
     * totally accurate anyway.
     */
    if (newaction == BLK_NEEDS_REDO && !hot_update && freespace < BLCKSZ / 5) {
        XLogRecordPageWithFreeSpace(rnode, newblk, freespace);
    }
}

void heap_xlog_page_upgrade(XLogReaderState *record)
{
    RedoBufferInfo buffer;

    if (BLK_NEEDS_REDO == XLogReadBufferForRedo(record, HEAP_PAGE_UPDATE_ORIG_BLOCK_NUM, &buffer)) {
        HeapXlogPageUpgradeOperatorPage(&buffer);
        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

static void heap_xlog_lock(XLogReaderState *record)
{
    RedoBufferInfo buffer;

    if (XLogReadBufferForRedo(record, HEAP_LOCK_ORIG_BLOCK_NUM, &buffer) == BLK_NEEDS_REDO) {
        char *maindata = XLogRecGetData(record);

        HeapXlogLockOperatorPage(&buffer, (void *)maindata);

        /* Make sure there is no forward chain link in t_ctid */

        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

static void heap_xlog_inplace(XLogReaderState *record)
{
    RedoBufferInfo buffer;

    if (XLogReadBufferForRedo(record, HEAP_INPLACE_ORIG_BLOCK_NUM, &buffer) == BLK_NEEDS_REDO) {
        char *maindata = XLogRecGetData(record);
        Size blkdatalen;
        char *blkdata = NULL;
        blkdata = XLogRecGetBlockData(record, HEAP_INPLACE_ORIG_BLOCK_NUM, &blkdatalen);
        HeapXlogInplaceOperatorPage(&buffer, (void *)maindata, (void *)blkdata, blkdatalen);

        MarkBufferDirty(buffer.buf);
    }
    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

static void heap_xlog_base_shift(XLogReaderState *record)
{
    RedoBufferInfo buffer;

    if (XLogReadBufferForRedo(record, HEAP_BASESHIFT_ORIG_BLOCK_NUM, &buffer) == BLK_NEEDS_REDO) {
        char *maindata = XLogRecGetData(record);

        HeapXlogBaseShiftOperatorPage(&buffer, (void *)maindata);
        MarkBufferDirty(buffer.buf);
    }

    if (BufferIsValid(buffer.buf)) {
        UnlockReleaseBuffer(buffer.buf);
    }
}

void heap_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    /*
     * These operations don't overwrite MVCC data so no conflict processing is
     * required. The ones in heap2 rmgr do.
     */
    switch (info & XLOG_HEAP_OPMASK) {
        case XLOG_HEAP_INSERT:
            heap_xlog_insert(record);
            break;
        case XLOG_HEAP_DELETE:
            heap_xlog_delete(record);
            break;
        case XLOG_HEAP_UPDATE:
            heap_xlog_update(record, false);
            break;
        case XLOG_HEAP_BASE_SHIFT:
            heap_xlog_base_shift(record);
            break;
        case XLOG_HEAP_HOT_UPDATE:
            heap_xlog_update(record, true);
            break;
        case XLOG_HEAP_NEWPAGE:
            heap_xlog_newpage(record);
            break;
        case XLOG_HEAP_LOCK:
            heap_xlog_lock(record);
            break;
        case XLOG_HEAP_INPLACE:
            heap_xlog_inplace(record);
            break;
        default:
            ereport(PANIC, (errmsg("heap_redo: unknown op code %hhu", info)));
    }
}

void heap2_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info & XLOG_HEAP_OPMASK) {
        case XLOG_HEAP2_FREEZE:
            heap_xlog_freeze(record);
            break;
        case XLOG_HEAP2_CLEAN:
            heap_xlog_clean(record);
            break;
        case XLOG_HEAP2_CLEANUP_INFO:
            heap_xlog_cleanup_info(record);
            break;
        case XLOG_HEAP2_VISIBLE:
            heap_xlog_visible(record);
            break;
        case XLOG_HEAP2_BCM:
            heap_xlog_bcm(record);
            break;
        case XLOG_HEAP2_MULTI_INSERT:
            heap_xlog_multi_insert(record);
            break;
        case XLOG_HEAP2_LOGICAL_NEWPAGE:
            heap_xlog_logical_new_page(record);
            break;
        case XLOG_HEAP2_PAGE_UPGRADE:
            heap_xlog_page_upgrade(record);
            break;
        default:
            ereport(PANIC, (errmsg("heap2_redo: unknown op code %hhu", info)));
    }
}
void heap3_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info & XLOG_HEAP_OPMASK) {
        case XLOG_HEAP3_NEW_CID:
            break;
        case XLOG_HEAP3_REWRITE:
            break;
        default:
            ereport(PANIC, (errmsg("heap3_redo: unknown op code %hhu", info)));
    }
}

static HTAB *heap_bucketid_hashtbl_create()
{
    HASHCTL hashCtrl;
    HTAB *hashtbl = NULL;
    errno_t rc;

    rc = memset_s(&hashCtrl, sizeof(hashCtrl), 0, sizeof(hashCtrl));
    securec_check(rc, "", "");
    hashCtrl.hcxt = (MemoryContext)CurrentMemoryContext;
    hashCtrl.hash = tag_hash;
    hashCtrl.keysize = sizeof(int4);
    hashCtrl.entrysize = sizeof(int4);

    hashtbl = hash_create("CopyFromFlushHashTable", 64, &hashCtrl, (HASH_CONTEXT | HASH_FUNCTION | HASH_ELEM));
    return hashtbl;
}

static void heap_do_sync_disk(Relation rel)
{
    HTAB *hashtbl = NULL;
    if (RELATION_CREATE_BUCKET(rel)) {
        hashtbl = heap_bucketid_hashtbl_create();
    }

    FlushRelationBuffers(rel, hashtbl);
    RelationOpenSmgr(rel);

    if (hashtbl == NULL) {
        smgrimmedsync(rel->rd_smgr, MAIN_FORKNUM);
    } else {
        HASH_SEQ_STATUS status;
        int *bucketnode = NULL;
        hash_seq_init(&status, hashtbl);
        RelFileNode rd_node = rel->rd_node;
        while ((bucketnode = (int4 *)hash_seq_search(&status)) != NULL) {
            if (*bucketnode == InvalidBktId) {
                continue;
            }
            rd_node.bucketNode = *bucketnode;
            /* FlushRelationBuffers will have opened rd_smgr */
            SMgrRelation oreln = smgropen(rd_node, InvalidBackendId);
            smgrimmedsync(oreln, MAIN_FORKNUM);
            smgrclose(oreln);
        }
        hash_destroy(hashtbl);
    }
}

/*
 * heap_sync_internal() is a internal function called by heap_sync(),
 * no matter the rel is a non-partitioned relation or FakeRel of partition,
 * this function should work for both.
 */
void heap_sync_internal(Relation rel, Oid toastHeapOid, LOCKMODE lockmode)
{
    /* main heap */
    heap_do_sync_disk(rel);

    /* FSM is not critical, don't bother syncing it
     *
     * toast heap, if any
     */
    if (OidIsValid(toastHeapOid)) {
        Relation toastrel;
        toastrel = heap_open(toastHeapOid, lockmode);
        heap_do_sync_disk(toastrel);
        heap_close(toastrel, lockmode);
    }
}

/*
 *	heap_sync		- sync a heap, for use when no WAL has been written
 *
 * This forces the heap contents (including TOAST heap if any) down to disk.
 * If we skipped using WAL, and WAL is otherwise needed, we must force the
 * relation down to disk before it's safe to commit the transaction.  This
 * requires writing out any dirty buffers and then doing a forced fsync.
 *
 * Indexes are not touched.  (Currently, index operations associated with
 * the commands that use this are WAL-logged and so do not need fsync.
 * That behavior might change someday, but in any case it's likely that
 * any fsync decisions required would be per-index and hence not appropriate
 * to be done here.)
 */
void heap_sync(Relation rel, LOCKMODE lockmode)
{
    Assert(!RelationIsBucket(rel));

    bool heapIsPartitioned = RELATION_IS_PARTITIONED(rel);
    LOCKMODE toastLockmode = (lockmode == NoLock) ? NoLock : AccessShareLock;

    /* non-WAL-logged tables or dfs tables never need fsync */
    if (!RelationNeedsWAL(rel) || RelationIsDfsStore(rel)) {
        return;
    }

    if (!heapIsPartitioned) {
        heap_sync_internal(rel, rel->rd_rel->reltoastrelid, toastLockmode);
    } else {
        List *partitionList = relationGetPartitionList(rel, lockmode);
        ListCell *cell = NULL;
        foreach (cell, partitionList) {
            Partition partition = (Partition)lfirst(cell);
            Relation partitionRel = partitionGetRelation(rel, partition);
            heap_sync_internal(partitionRel, partition->pd_part->reltoastrelid, toastLockmode);
            releaseDummyRelation(&partitionRel);
        }
        /* remember to release partition list */
        if (partitionList != NULL) {
            releasePartitionList(rel, &partitionList, NoLock);
        }
    }
}

void partition_sync(Relation rel, Oid partitionId, LOCKMODE partitionLockmode)
{
    LOCKMODE toastLockmode = (partitionLockmode == NoLock) ? NoLock : AccessShareLock;

    /* non-WAL-logged tables or dfs tables never need fsync */
    if (!RelationNeedsWAL(rel) || RelationIsDfsStore(rel)) {
        return;
    }

    if (!RELATION_IS_PARTITIONED(rel) || !OidIsValid(partitionId)) {
        return;
    }

    Partition partition = partitionOpen(rel, partitionId, partitionLockmode);
    Relation partionRel = partitionGetRelation(rel, partition);

    heap_sync_internal(partionRel, partition->pd_part->reltoastrelid, toastLockmode);

    releaseDummyRelation(&partionRel);
    partitionClose(rel, partition, NoLock);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: open any partition by partition OID
 * Description	: If lockmode is not "NoLock", retry on performing lock the partition for retryCount times
            : if retryCount is reached, return NULL.
 * Notes		:
 */
Partition partitionOpenWithRetry(Relation relation, Oid partitionId, LOCKMODE lockmode, const char *stmt)
{
    Partition p;

    Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);
    Assert(PointerIsValid(relation));
    Assert(OidIsValid(partitionId));

    if (relation->rd_rel->relkind != RELKIND_RELATION && relation->rd_rel->relkind != RELKIND_INDEX) {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                        (errmsg("relation \"%s\" is not table or index", RelationGetRelationName(relation)))));
    }

    /* step 1: try to lock the partition */
    /* Get the lock before trying to open the relcache entry */
    if (lockmode != NoLock && !ConditionalLockPartitionWithRetry(relation, partitionId, lockmode)) {
        ereport(LOG,
                (errmsg("try to open partition \"%s.%s.%s.%s\" failed: "
                        "could not (re)acquire lock \"%d\" within timeout %d seconds, when \"%s\"",
                        get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId),
                        get_namespace_name(RelationGetNamespace(relation)), RelationGetRelationName(relation),
                        getPartitionName(partitionId, false), lockmode,
                        u_sess->attr.attr_storage.partition_lock_upgrade_timeout, stmt ? stmt : "unkown operations")));
        return NULL;
    }

    /* step 2: get the partiton object */
    /* The partcache does all the real work... */
    p = PartitionIdGetPartition(partitionId, RELATION_OWN_BUCKET(relation));

    if (!PartitionIsValid(p)) {
        ereport(ERROR,
                (errcode(ERRCODE_RELATION_OPEN_ERROR), errmsg("could not open partition with OID %u", partitionId)));
    }
    Assert(relation->rd_id == p->pd_part->parentid);

    PartitionOpenSmgr(p);

#ifdef PGXC
    if (IS_PGXC_DATANODE) {
#endif
        pgstat_initstats_partition(p);
#ifdef PGXC
    }
#endif

    return p;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: open any partition by partition OID
 * Description	: If lockmode is not "NoLock", the specified kind of lock is
 *			: obtained on the partition. (Generally, NoLock should only
 *			: be used if the caller knows it has some appropriate lock
 *			: on the partiiton already.)
 * Notes		:
 */
Partition partitionOpen(Relation relation, Oid partitionId, LOCKMODE lockmode, int2 bucketId)
{
    Partition p;

    if (!OidIsValid(partitionId)) {
        ereport(ERROR, (errcode(ERRCODE_RELATION_OPEN_ERROR), errmsg("partition %u is invalid", partitionId)));
    }

    Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);
    Assert(PointerIsValid(relation));

    /* Get the lock before trying to open the relcache entry */
    if (lockmode != NoLock) {
        if (relation->rd_rel->relkind == RELKIND_RELATION) {
            LockPartition(relation->rd_id, partitionId, lockmode, PARTITION_LOCK);

        } else if (relation->rd_rel->relkind == RELKIND_INDEX) {
            LockPartition(relation->rd_id, partitionId, lockmode, PARTITION_LOCK);
        } else {
            ereport(ERROR, (errcode(ERRCODE_RELATION_OPEN_ERROR),
                            errmsg("openning partition %u, but relation %s %u is neither table nor index", partitionId,
                                   RelationGetRelationName(relation), RelationGetRelid(relation))));
        }
    }

    /* The partcache does all the real work... */
    p = PartitionIdGetPartition(partitionId, RELATION_OWN_BUCKET(relation));

    if (!PartitionIsValid(p)) {
        ereport(ERROR,
                (errcode(ERRCODE_RELATION_OPEN_ERROR), errmsg("could not open partition with OID %u", partitionId)));
    }
    Assert(relation->rd_id == p->pd_part->parentid);

    PartitionOpenSmgr(p);

#ifdef PGXC
    if (IS_PGXC_DATANODE) {
#endif
        pgstat_initstats_partition(p);
#ifdef PGXC
    }
#endif
    if (BUCKET_NODE_IS_VALID(bucketId)) {
        p = bucketGetPartition(p, bucketId);
    }
    return p;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: open any partition by partition OID
 * Description	: Same as partitionOpen, except return NULL instead of failing
 *				: if the partition does not exist.
 * Notes		:
 */
Partition tryPartitionOpen(Relation relation, Oid partitionId, LOCKMODE lockmode)
{
    Partition p;
    PartitionIdentifier *partID = NULL;

    Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);
    Assert(PointerIsValid(relation));
    Assert(OidIsValid(partitionId));

    /* Get the lock before trying to open the relcache entry */
    if (lockmode != NoLock) {
        if (relation->rd_rel->relkind == RELKIND_RELATION) {
            partID = partOidGetPartID(relation, partitionId);
            switch (partID->partArea) {
                case PART_AREA_RANGE:
                case PART_AREA_LIST:
                case PART_AREA_HASH:
                    LockPartition(relation->rd_id, partitionId, lockmode, PARTITION_LOCK);
                    break;
                case PART_AREA_INTERVAL:
                    LockPartition(relation->rd_id, partitionId, lockmode, PARTITION_LOCK);
                    break;
                default:
                    break;
            }
            pfree(partID);
        } else if (relation->rd_rel->relkind == RELKIND_INDEX) {
            LockPartition(relation->rd_id, partitionId, lockmode, PARTITION_LOCK);
        } else {
            ereport(ERROR, (errcode(ERRCODE_RELATION_OPEN_ERROR),
                            errmsg("openning partition %u, but relation %s %u is neither table nor index", partitionId,
                                   RelationGetRelationName(relation), RelationGetRelid(relation))));
        }
    }

    /*
     * Now that we have the lock, probe to see if the partition really exists
     * or not.
     */
    if (!SearchSysCacheExists1(PARTRELID, ObjectIdGetDatum(partitionId))) {
        /* Release useless lock */
        if (lockmode != NoLock) {
            if (relation->rd_rel->relkind == RELKIND_RELATION) {
                partID = partOidGetPartID(relation, partitionId);
                switch (partID->partArea) {
                    case PART_AREA_RANGE:
                    case PART_AREA_LIST:
                    case PART_AREA_HASH:
                        UnlockPartition(relation->rd_id, partitionId, lockmode, PARTITION_LOCK);
                        break;
                    case PART_AREA_INTERVAL:
                        UnlockPartition(relation->rd_id, partitionId, lockmode, PARTITION_LOCK);
                        break;
                    default:
                        break;
                }
                pfree(partID);
            } else if (relation->rd_rel->relkind == RELKIND_INDEX) {
                UnlockPartition(relation->rd_id, partitionId, lockmode, PARTITION_LOCK);
            } else {
                ereport(ERROR, (errcode(ERRCODE_RELATION_OPEN_ERROR),
                                errmsg("closing partition %u, but relation %u is neither table nor index", partitionId,
                                       relation->rd_id)));
            }
        }
        return NULL;
    }

    /* The partcache does all the real work... */
    p = PartitionIdGetPartition(partitionId, RELATION_OWN_BUCKET(relation));

    if (!PartitionIsValid(p)) {
        ereport(ERROR,
                (errcode(ERRCODE_RELATION_OPEN_ERROR), errmsg("could not open partition with OID %u", partitionId)));
    }
    Assert(relation->rd_id == p->pd_part->parentid);

    PartitionOpenSmgr(p);

#ifdef PGXC
    if (IS_PGXC_DATANODE) {
#endif
        pgstat_initstats_partition(p);
#ifdef PGXC
    }
#endif

    return p;
}

/*
 * @brief: close the partiiton
 * If lockmode is not "NoLock", we then release the specified lock.
 * Notes: it is often sensible to hold a lock beyond partitionClose; in that case,
 * the lock is released automatically at xact end.
 */
void partitionClose(Relation relation, Partition partition, LOCKMODE lockmode)
{
    PartitionIdentifier *partID = NULL;
    Partition part = partition;

    if (PartitionIsBucket(partition)) {
        part = partition->parent;
        bucketClosePartition(partition);
    }
    Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);
    Assert(PointerIsValid(relation));
    Assert(PointerIsValid(part));
    Assert(relation->rd_id == part->pd_part->parentid);

    /* The partcache does the real work... */
    PartitionClose(part);

    if (lockmode != NoLock) {
        if (relation->rd_rel->relkind == RELKIND_RELATION) {
            partID = partOidGetPartID(relation, part->pd_id);
            switch (partID->partArea) {
                case PART_AREA_RANGE:
                case PART_AREA_LIST:
                case PART_AREA_HASH:
                    UnlockPartition(relation->rd_id, part->pd_id, lockmode, PARTITION_LOCK);
                    break;
                case PART_AREA_INTERVAL:
                    UnlockPartition(relation->rd_id, part->pd_id, lockmode, PARTITION_LOCK);
                    break;
                default:
                    break;
            }
            pfree(partID);
        } else if (relation->rd_rel->relkind == RELKIND_INDEX) {
            UnlockPartition(relation->rd_id, part->pd_id, lockmode, PARTITION_LOCK);
        } else {
            ereport(ERROR, (errcode(ERRCODE_RELATION_CLOSE_ERROR),
                            errmsg("closing partition %u, but relation %u is neither table nor index", part->pd_id,
                                   relation->rd_id)));
        }
    }
}

void PushHeapPageToDataQueue(Buffer buffer)
{
    RelFileNode rnode; /* physical relation identifier */
    ForkNumber forkNum;
    BlockNumber blockNum; /* blknum relative to begin of reln */

    BufferGetTag(buffer, &rnode, &forkNum, &blockNum);
    Assert(forkNum == MAIN_FORKNUM);

    /* Put page to sender queue */
    t_thrd.proc->waitDataSyncPoint = PushToSenderQueue(rnode, blockNum, ROW_STORE, (char *)BufferGetPage(buffer),
                                                       BLCKSZ, 0, 0);

    if (u_sess->attr.attr_storage.HaModuleDebug) {
        ereport(LOG, (errmsg("HA-PushToSenderQueue done: rnode %u/%u/%u, blockno %u, waitpoint %u/%u", rnode.spcNode,
                             rnode.dbNode, rnode.relNode, blockNum, t_thrd.proc->waitDataSyncPoint.queueid,
                             t_thrd.proc->waitDataSyncPoint.queueoff)));
    }

    /* Wake up all datasenders to send Page if replication is enabled */
    if (g_instance.attr.attr_storage.max_wal_senders > 0) {
        DataSndWakeup();
    }
}

void heap_init_parallel_seqscan(TableScanDesc sscan, int32 dop, ScanDirection dir)
{
    HeapScanDesc scan = (HeapScanDesc) sscan;

    if (!scan || scan->rs_base.rs_nblocks == 0) {
        return;
    }

    if (dop <= 1) {
        return;
    }

    scan->dop = dop;

    uint32 paral_blocks = u_sess->stream_cxt.smp_id * PARALLEL_SCAN_GAP;

    /* If not enough pages to divide into every worker. */
    if (scan->rs_base.rs_nblocks <= paral_blocks) {
        scan->rs_base.rs_startblock = 0;
        scan->rs_base.rs_nblocks = 0;
        return;
    }

    if (ScanDirectionIsBackward(dir)) {
        paral_blocks = (scan->rs_base.rs_nblocks - 1) - paral_blocks;
        if (scan->rs_base.rs_rangeScanInRedis.isRangeScanInRedis) {
            scan->rs_base.rs_startblock = paral_blocks;
        } else {
            scan->rs_base.rs_startblock += paral_blocks;
        }
        return;
    }

    /* If not range scan in redistribute, just start from 0. */
    if (scan->rs_base.rs_rangeScanInRedis.isRangeScanInRedis) {
        scan->rs_base.rs_startblock += paral_blocks;
    } else {
        scan->rs_base.rs_startblock = paral_blocks;
    }
}

IndexFetchTableData *heapam_index_fetch_begin(Relation rel)
{
    IndexFetchHeapData *hscan = (IndexFetchHeapData *)palloc(sizeof(IndexFetchHeapData));

    hscan->xs_base.rel = rel;

    return &hscan->xs_base;
}

void heapam_index_fetch_reset(IndexFetchTableData *scan)
{

}

void heapam_index_fetch_end(IndexFetchTableData *scan)
{
    IndexFetchHeapData *hscan = (IndexFetchHeapData *) scan;

    heapam_index_fetch_reset(scan);

    pfree(hscan);
}

HeapTuple heapam_index_fetch_tuple(IndexScanDesc scan, bool *all_dead)
{
    ItemPointer tid = &scan->xs_ctup.t_self;
    bool got_heap_tuple = false;
    Page page;
     
    /* We can skip the buffer-switching logic if we're in mid-HOT chain. */
    if (!scan->xs_continue_hot) {
        /* Switch to correct buffer if we don't have it already */
        Buffer prev_buf = scan->xs_cbuf;

        scan->xs_cbuf = ReleaseAndReadBuffer(scan->xs_cbuf, scan->heapRelation, ItemPointerGetBlockNumber(tid));

        /* In single mode and hot standby, we may get a null buffer if index
         * replayed before the tid replayed. This is acceptable, so we return
         * null without reporting error.
         */
#ifndef ENABLE_MULTIPLE_NODES
        if(!BufferIsValid(scan->xs_cbuf)) {
           return NULL;
        }
#endif

        /*
         * Prune page, but only if we weren't already on this page
         */
        if (prev_buf != scan->xs_cbuf)
            heap_page_prune_opt(scan->heapRelation, scan->xs_cbuf);
    }
    
    page = BufferGetPage(scan->xs_cbuf);

    /* Obtain share-lock on the buffer so we can examine visibility */
    LockBuffer(scan->xs_cbuf, BUFFER_LOCK_SHARE);
    got_heap_tuple = heap_hot_search_buffer(tid, scan->heapRelation,
        scan->xs_cbuf, scan->xs_snapshot, &scan->xs_ctup,
        &scan->xs_ctbuf_hdr, all_dead, !scan->xs_continue_hot);

    LockBuffer(scan->xs_cbuf, BUFFER_LOCK_UNLOCK);

    if (got_heap_tuple) {
        ereport(DEBUG1,
            (errmsg(
                "index fetch heap xid %lu self(%u,%hu) ctid(%u,%hu) xmin %lu xmax %lu snapshot xmin %lu xmax %lu csn %lu",
                GetCurrentTransactionIdIfAny(),
                ItemPointerGetBlockNumber(&scan->xs_ctup.t_self),
                ItemPointerGetOffsetNumber(&scan->xs_ctup.t_self),
                ItemPointerGetBlockNumber(&scan->xs_ctup.t_data->t_ctid),
                ItemPointerGetOffsetNumber(&scan->xs_ctup.t_data->t_ctid),
                HeapTupleHeaderGetXmin(page, scan->xs_ctup.t_data),
                HeapTupleHeaderGetXmax(page, scan->xs_ctup.t_data),
                scan->xs_snapshot->xmin, scan->xs_snapshot->xmax,
                scan->xs_snapshot->snapshotcsn)));

        /*
         * Only in a non-MVCC snapshot can more than one member of the HOT
         * chain be visible.
         */
        scan->xs_continue_hot = !IsMVCCSnapshot(scan->xs_snapshot);
        return &scan->xs_ctup;
    }
    
    /* We've reached the end of the HOT chain. */
    scan->xs_continue_hot = false;

    return NULL;
}
