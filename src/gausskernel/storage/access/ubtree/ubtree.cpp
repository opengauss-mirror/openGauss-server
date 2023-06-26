/* -------------------------------------------------------------------------
 *
 * nbtree.cpp
 *	  Implementation of Lehman and Yao's btree management algorithm for
 *	  openGauss.
 *
 * NOTES
 *	  This file contains only the public interface routines.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/nbtree/nbtree.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/xlog.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "storage/indexfsm.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "storage/smgr/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/aiomem.h"
#include "utils/memutils.h"
#include "vecexecutor/vecnodes.h"
#include "vecexecutor/vecnodecstorescan.h"
#include "commands/tablespace.h"

/* Working state needed by btvacuumpage */
typedef struct {
    IndexVacuumInfo *info;
    IndexBulkDeleteResult *stats;
    IndexBulkDeleteCallback callback;
    void *callback_state;
    BTCycleId cycleid;
    BlockNumber lastBlockVacuumed; /* highest blkno actually vacuumed */
    BlockNumber lastBlockLocked;   /* highest blkno we've cleanup-locked */
    BlockNumber totFreePages;      /* true total # of free pages */
    MemoryContext pagedelcontext;
} BTVacState;

static void UBTreeVacuumScan(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, BTCycleId cycleid);
static void UBTreeVacuumPage(BTVacState *vstate, OidRBTree* invisibleParts, BlockNumber blkno, BlockNumber origBlkno);

static IndexTuple UBTreeGetIndexTuple(IndexScanDesc scan, ScanDirection dir, BlockNumber heapTupleBlkOffset);
/*
 *	btbuild() -- build a new btree index.
 */
Datum ubtbuild(PG_FUNCTION_ARGS)
{
    Relation heap = (Relation)PG_GETARG_POINTER(0);
    Relation index = (Relation)PG_GETARG_POINTER(1);
    IndexInfo *indexInfo = (IndexInfo *)PG_GETARG_POINTER(2);
    IndexBuildResult *result = NULL;
    double reltuples = 0;
    BTBuildState buildstate;
    double* allPartTuples = NULL;

    buildstate.isUnique = indexInfo->ii_Unique;
    buildstate.haveDead = false;
    buildstate.heapRel = heap;
    buildstate.spool = NULL;
    buildstate.spool2 = NULL;
    buildstate.indtuples = 0;
    buildstate.btleader = NULL;

#ifdef BTREE_BUILD_STATS
    if (u_sess->attr.attr_resource.log_btree_build_stats) {
        ResetUsage();
    }
#endif /* BTREE_BUILD_STATS */

    /* We expect to be called exactly once for any index relation. If that's
     * not the case, big trouble's what we have. */
    if (RelationGetNumberOfBlocks(index) != 0) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("index \"%s\" already contains data", RelationGetRelationName(index))));
    }

    reltuples = _bt_spools_heapscan(heap, index, &buildstate, indexInfo, &allPartTuples);

    /*
     * Finish the build by (1) completing the sort of the spool file, (2)
     * inserting the sorted tuples into btree pages and (3) building the upper
     * levels.
     */
    UBTreeLeafBuild(buildstate.spool, buildstate.spool2);
    _bt_spooldestroy(buildstate.spool);
    if (buildstate.spool2) {
        _bt_spooldestroy(buildstate.spool2);
    }

    if (buildstate.btleader) {
        _bt_end_parallel();
    }

#ifdef BTREE_BUILD_STATS
    if (u_sess->attr.attr_resource.log_btree_build_stats) {
        ShowUsage("BTREE BUILD STATS");
        ResetUsage();
    }
#endif /* BTREE_BUILD_STATS */

    // Return statistics
    result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));

    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;
    result->all_part_tuples = allPartTuples;

    PG_RETURN_POINTER(result);
}

/*
 * Per-tuple callback from IndexBuildHeapScan
 */
void UBTreeBuildCallback(Relation index, HeapTuple htup, Datum *values, const bool *isnull, bool tupleIsAlive,
                            void *state)
{
    BTBuildState *buildstate = (BTBuildState *)state;

    // insert the index tuple into the appropriate spool file for subsequent processing
    if (tupleIsAlive || buildstate->spool2 == NULL) {
        _bt_spool(buildstate->spool, &htup->t_self, values, isnull);
    } else {
        /* dead tuples are put into spool2 */
        buildstate->haveDead = true;
        _bt_spool(buildstate->spool2, &htup->t_self, values, isnull);
    }

    buildstate->indtuples += 1;
}

/*
 *	btbuildempty() -- build an empty btree index in the initialization fork
 */
Datum ubtbuildempty(PG_FUNCTION_ARGS)
{
    Relation index = (Relation)PG_GETARG_POINTER(0);
    Page metapage;

    /* Construct metapage. */
    ADIO_RUN()
    {
        metapage = (Page)adio_align_alloc(BLCKSZ);
    }
    ADIO_ELSE()
    {
        metapage = (Page)palloc(BLCKSZ);
    }
    ADIO_END();

    STORAGE_SPACE_OPERATION(index, BLCKSZ);

    /* Ensure rd_smgr is open (could have been closed by relcache flush!) */
    RelationOpenSmgr(index);

    UBTreeInitMetaPage(metapage, P_NONE, 0);

    /*
     * Write the page and log it.  It might seem that an immediate sync
     * would be sufficient to guarantee that the file exists on disk, but
     * recovery itself might remove it while replaying, for example, an
     * XLOG_DBASE_CREATE or XLOG_TBLSPC_CREATE record.  Therefore, we
     * need this even when wal_level=minimal.
     */
    PageSetChecksumInplace(metapage, BTREE_METAPAGE);
    smgrwrite(index->rd_smgr, INIT_FORKNUM, BTREE_METAPAGE, (char *)metapage, true);

    log_newpage(&index->rd_smgr->smgr_rnode.node, INIT_FORKNUM, BTREE_METAPAGE, metapage, false);

    /*
     * An immediate sync is require even if we xlog'd the page, because the
     * write did not go through shared_buffers and therefore a concurrent
     * checkpoint may have move the redo pointer past our xlog record.
     */
    smgrimmedsync(index->rd_smgr, INIT_FORKNUM);

    ADIO_RUN()
    {
        adio_align_free(metapage);
    }
    ADIO_ELSE()
    {
        pfree(metapage);
    }
    ADIO_END();

    PG_RETURN_VOID();
}

bool UBTreeDelete(Relation rel, Datum* values, const bool* isnull, ItemPointer heapTCtid, bool isRollbackIndex)
{
    bool ret;
    IndexTuple itup;

    Assert(RelationIsUstoreIndex(rel));

    WHITEBOX_TEST_STUB("UBTreeDelete", WhiteboxDefaultErrorEmit);

    itup = index_form_tuple(RelationGetDescr(rel), values, isnull);
    itup->t_tid = *heapTCtid;
    ret = UBTreeDoDelete(rel, itup, isRollbackIndex);
    pfree(itup);

    return ret;
}

/*
 *	btinsert() -- insert an index tuple into a btree.
 *
 *		Descend the tree recursively, find the appropriate location for our
 *		new tuple, and put it there.
 */
Datum ubtinsert(PG_FUNCTION_ARGS)
{
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    Datum *values = (Datum *)PG_GETARG_POINTER(1);
    bool *isnull = (bool *)PG_GETARG_POINTER(2);
    ItemPointer htCtid = (ItemPointer)PG_GETARG_POINTER(3);
    Relation heapRel = (Relation)PG_GETARG_POINTER(4);
    IndexUniqueCheck checkUnique = (IndexUniqueCheck)PG_GETARG_INT32(5);
    bool result = false;
    IndexTuple itup;

    WHITEBOX_TEST_STUB("ubtinsert", WhiteboxDefaultErrorEmit);

    /* skip inserting if global temp table index does not exist */
    if (RELATION_IS_GLOBAL_TEMP(rel)) {
        if (rel->rd_smgr == NULL) {
            /* Open it at the smgr level if not already done */
            RelationOpenSmgr(rel);
        }
        if (!smgrexists(rel->rd_smgr, MAIN_FORKNUM)) {
            PG_RETURN_BOOL(result);
        }
    }

    /* generate an index tuple */
    itup = index_form_tuple(RelationGetDescr(rel), values, isnull);
    itup->t_tid = *htCtid;

    /* reserve space for xmin/xmax */
    Size newsize = IndexTupleSize(itup) + sizeof(ShortTransactionId) * 2;
    IndexTupleSetSize(itup, newsize);

    result = UBTreeDoInsert(rel, itup, checkUnique, heapRel);

    pfree(itup);

    PG_RETURN_BOOL(result);
}

/*
 *	btgettuple() -- Get the next tuple in the scan.
 */
Datum ubtgettuple(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanDirection dir = (ScanDirection)PG_GETARG_INT32(1);

    if (scan == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid arguments for function btgettuple")));
    }

    bool res = UBTreeGetTupleInternal(scan, dir);

    PG_RETURN_BOOL(res);
}

/*
 * btgetbitmap() -- gets all matching tuples, and adds them to a bitmap
 */
Datum ubtgetbitmap(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    TIDBitmap *tbm = (TIDBitmap *)PG_GETARG_POINTER(1);
    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    int64 ntids = 0;
    ItemPointer heapTid;
    Oid currPartOid;
    TBMHandler tbm_handler = tbm_get_handler(tbm);

    WHITEBOX_TEST_STUB("ubtgetbitmap", WhiteboxDefaultErrorEmit);

    /*
     * If we have any array keys, initialize them.
     */
    if (so->numArrayKeys) {
        /* punt if we have any unsatisfiable array keys */
        if (so->numArrayKeys < 0) {
            PG_RETURN_INT64(ntids);
        }

        _bt_start_array_keys(scan, ForwardScanDirection);
    }

    /* This loop handles advancing to the next array elements, if any */
    do {
        /* Fetch the first page & tuple */
        if (UBTreeFirst(scan, ForwardScanDirection)) {
            /* Save tuple ID, and continue scanning */
            heapTid = &scan->xs_ctup.t_self;
            currPartOid = so->currPos.items[so->currPos.itemIndex].partitionOid;
            tbm_handler._add_tuples(tbm, heapTid, 1, scan->xs_recheck_itup, currPartOid, InvalidBktId);
            ntids++;

            for (;;) {
                /*
                 * Advance to next tuple within page.  This is the same as the
                 * easy case in _bt_next().
                 */
                if (++so->currPos.itemIndex > so->currPos.lastItem) {
                    /* let _bt_next do the heavy lifting */
                    if (!UBTreeNext(scan, ForwardScanDirection)) {
                        break;
                    }
                }

                /* Save tuple ID, and continue scanning */
                scan->xs_recheck_itup = so->currPos.items[so->currPos.itemIndex].needRecheck;
                heapTid = &so->currPos.items[so->currPos.itemIndex].heapTid;
                currPartOid = so->currPos.items[so->currPos.itemIndex].partitionOid;
                tbm_handler._add_tuples(tbm, heapTid, 1, scan->xs_recheck_itup, currPartOid, InvalidBktId);
                ntids++;
            }
        }
        /* Now see if we have more array keys to deal with */
    } while (so->numArrayKeys && _bt_advance_array_keys(scan, ForwardScanDirection));

    PG_RETURN_INT64(ntids);
}

/*
 *	btbeginscan() -- start a scan on a btree index
 */
Datum ubtbeginscan(PG_FUNCTION_ARGS)
{
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    int nkeys = PG_GETARG_INT32(1);
    int norderbys = PG_GETARG_INT32(2);
    IndexScanDesc scan;
    BTScanOpaque so;

    WHITEBOX_TEST_STUB("ubtbeginscan", WhiteboxDefaultErrorEmit);

    /* no order by operators allowed */
    Assert(norderbys == 0);

    /* get the scan */
    scan = RelationGetIndexScan(rel, nkeys, norderbys);

    /* allocate private workspace */
    so = (BTScanOpaque)palloc(sizeof(BTScanOpaqueData));
    so->currPos.buf = so->markPos.buf = InvalidBuffer;
    if (scan->numberOfKeys > 0) {
        so->keyData = (ScanKey)palloc(scan->numberOfKeys * sizeof(ScanKeyData));
    } else {
        so->keyData = NULL;
    }

    so->xs_want_xid = false;
    so->arrayKeyData = NULL; /* assume no array keys for now */
    so->numArrayKeys = 0;
    so->arrayKeys = NULL;
    so->arrayContext = NULL;
    so->killedItems = NULL; /* until needed */
    so->numKilled = 0;

    /*
     * We don't know yet whether the scan will be index-only, so we do not
     * allocate the tuple workspace arrays until btrescan.	However, we set up
     * scan->xs_itupdesc whether we'll need it or not, since that's so cheap.
     */
    so->currTuples = so->markTuples = NULL;
    so->currPos.nextTupleOffset = 0;
    so->markPos.nextTupleOffset = 0;

    so->lastSelfModifiedItup = NULL;
    so->lastSelfModifiedItupBufferSize = 0;
    so->indexInfo = NULL;
    so->fakeEstate = NULL;

    scan->xs_itupdesc = RelationGetDescr(rel);
    scan->opaque = so;
    scan->isUpsert = false;

    PG_RETURN_POINTER(scan);
}

/*
 *	btrescan() -- rescan an index relation
 */
Datum ubtrescan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanKey scankey = (ScanKey)PG_GETARG_POINTER(1);

    /* remaining arguments are ignored */
    BTScanOpaque so = (BTScanOpaque)scan->opaque;

    /* set up this for btmerge of UStore index */
    so->xs_want_xid = scan->xs_want_xid;

    /* we aren't holding any read locks, but gotta drop the pins */
    if (BTScanPosIsValid(so->currPos)) {
        /* Before leaving current page, deal with any killed items */
        if (so->numKilled > 0) {
            _bt_killitems(scan, false);
        }
        ReleaseBuffer(so->currPos.buf);
        so->currPos.buf = InvalidBuffer;
    }

    if (BTScanPosIsValid(so->markPos)) {
        ReleaseBuffer(so->markPos.buf);
        so->markPos.buf = InvalidBuffer;
    }
    so->markItemIndex = -1;

    /*
     * Allocate tuple workspace arrays, if needed for an index-only scan and
     * not already done in a previous rescan call.	To save on palloc
     * overhead, both workspaces are allocated as one palloc block; only this
     * function and btendscan know that.
     *
     * NOTE: this data structure also makes it safe to return data from a
     * "name" column, even though btree name_ops uses an underlying storage
     * datatype of cstring.  The risk there is that "name" is supposed to be
     * padded to NAMEDATALEN, but the actual index tuple is probably shorter.
     * However, since we only return data out of tuples sitting in the
     * currTuples array, a fetch of NAMEDATALEN bytes can at worst pull some
     * data out of the markTuples array --- running off the end of memory for
     * a SIGSEGV is not possible.  Yeah, this is ugly as sin, but it beats
     * adding special-case treatment for name_ops elsewhere.
     */
    if (scan->xs_want_itup && so->currTuples == NULL) {
        /*
         * xid in UStore index page is ShortTransactionId, but if we want to
         * get them by index scan, we need to be extended to full TransactionId,
         * which may need more space. So we need extra buffer in this case.
         */
        Size extraBufsz = scan->xs_want_xid ? BLCKSZ : 0;

        so->currTuples = (char *)palloc(BLCKSZ * 2 + extraBufsz);
        so->markTuples = so->currTuples + BLCKSZ + extraBufsz;
    }

    if (so->lastSelfModifiedItup) {
        IndexTupleSetSize(((IndexTuple)(so->lastSelfModifiedItup)), 0); /* clear */
    }

    /*
     * Reset the scan keys. Note that keys ordering stuff moved to _bt_first.
     * - vadim 05/05/97
     */
    if (scankey && scan->numberOfKeys > 0) {
        errno_t rc = memmove_s(scan->keyData, scan->numberOfKeys * sizeof(ScanKeyData), scankey,
                               scan->numberOfKeys * sizeof(ScanKeyData));
        securec_check(rc, "", "");
    }
    so->numberOfKeys = 0; /* until _bt_preprocess_keys sets it */

    /* If any keys are SK_SEARCHARRAY type, set up array-key info */
    _bt_preprocess_array_keys(scan);

    PG_RETURN_VOID();
}

/*
 *	btendscan() -- close down a scan
 */
Datum ubtendscan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    BTScanOpaque so = (BTScanOpaque)scan->opaque;

    /* we aren't holding any read locks, but gotta drop the pins */
    if (BTScanPosIsValid(so->currPos)) {
        /* Before leaving current page, deal with any killed items */
        if (so->numKilled > 0) {
            _bt_killitems(scan, false);
        }
        ReleaseBuffer(so->currPos.buf);
        so->currPos.buf = InvalidBuffer;
    }

    if (BTScanPosIsValid(so->markPos)) {
        ReleaseBuffer(so->markPos.buf);
        so->markPos.buf = InvalidBuffer;
    }
    so->markItemIndex = -1;

    /* Release storage */
    FREE_POINTER(so->keyData);
    /* so->arrayKeyData and so->arrayKeys are in arrayContext */
    if (so->arrayContext != NULL) {
        MemoryContextDelete(so->arrayContext);
    }
    FREE_POINTER(so->killedItems);
    FREE_POINTER(so->currTuples);

    FREE_POINTER(so->lastSelfModifiedItup);
    if (so->fakeEstate != NULL) {
        /* this will delete memory context es_query_cxt also */
        FreeExecutorState(so->fakeEstate);
    }

    /* so->markTuples should not be pfree'd, see btrescan */
    pfree(so);

    PG_RETURN_VOID();
}

/*
 *	btmarkpos() -- save current scan position
 */
Datum ubtmarkpos(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    BTScanOpaque so = (BTScanOpaque)scan->opaque;

    /* we aren't holding any read locks, but gotta drop the pin */
    if (BTScanPosIsValid(so->markPos)) {
        ReleaseBuffer(so->markPos.buf);
        so->markPos.buf = InvalidBuffer;
    }

    /*
     * Just record the current itemIndex.  If we later step to next page
     * before releasing the marked position, _bt_steppage makes a full copy of
     * the currPos struct in markPos.  If (as often happens) the mark is moved
     * before we leave the page, we don't have to do that work.
     */
    if (BTScanPosIsValid(so->currPos)) {
        so->markItemIndex = so->currPos.itemIndex;
    } else {
        so->markItemIndex = -1;
    }

    /* Also record the current positions of any array keys */
    if (so->numArrayKeys) {
        _bt_mark_array_keys(scan);
    }

    PG_RETURN_VOID();
}

/*
 *	btrestrpos() -- restore scan to last saved position
 */
Datum ubtrestrpos(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    BTScanOpaque so = (BTScanOpaque)scan->opaque;

    /* Restore the marked positions of any array keys */
    if (so->numArrayKeys) {
        _bt_restore_array_keys(scan);
    }

    if (so->markItemIndex >= 0) {
        /*
         * The mark position is on the same page we are currently on. Just
         * restore the itemIndex.
         */
        so->currPos.itemIndex = so->markItemIndex;
    } else {
        /* we aren't holding any read locks, but gotta drop the pin */
        if (BTScanPosIsValid(so->currPos)) {
            /* Before leaving current page, deal with any killed items */
            if (so->numKilled > 0 && so->currPos.buf != so->markPos.buf) {
                _bt_killitems(scan, false);
            }
            ReleaseBuffer(so->currPos.buf);
            so->currPos.buf = InvalidBuffer;
        }

        if (BTScanPosIsValid(so->markPos)) {
            /* bump pin on mark buffer for assignment to current buffer */
            IncrBufferRefCount(so->markPos.buf);
            errno_t rc = memcpy_s(&so->currPos,
                                  offsetof(BTScanPosData, items[1]) + so->markPos.lastItem * sizeof(BTScanPosItem),
                                  &so->markPos,
                                  offsetof(BTScanPosData, items[1]) + so->markPos.lastItem * sizeof(BTScanPosItem));
            securec_check(rc, "", "");
            if (so->currTuples) {
                rc = memcpy_s(so->currTuples, (size_t)so->markPos.nextTupleOffset, so->markTuples,
                              (size_t)so->markPos.nextTupleOffset);
                securec_check(rc, "", "");
            }
        }
    }

    PG_RETURN_VOID();
}

/*
 * Bulk deletion of all index entries pointing to a set of heap tuples.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
Datum ubtbulkdelete(PG_FUNCTION_ARGS)
{
    IndexVacuumInfo *info = (IndexVacuumInfo *)PG_GETARG_POINTER(0);
    IndexBulkDeleteResult *volatile stats = (IndexBulkDeleteResult *)PG_GETARG_POINTER(1);
    Relation rel = info->index;
    BTCycleId cycleid;

    /* allocate stats if first time through, else re-use existing struct */
    if (stats == NULL)
        stats = (IndexBulkDeleteResult *)palloc0(sizeof(IndexBulkDeleteResult));

    /* Establish the vacuum cycle ID to use for this scan */
    /* The ENSURE stuff ensures we clean up shared memory on failure */
    PG_ENSURE_ERROR_CLEANUP(_bt_end_vacuum_callback, PointerGetDatum(rel));
    {
        cycleid = _bt_start_vacuum(rel);

        UBTreeVacuumScan(info, stats, cycleid);
    }
    PG_END_ENSURE_ERROR_CLEANUP(_bt_end_vacuum_callback, PointerGetDatum(rel));
    _bt_end_vacuum(rel);

    PG_RETURN_POINTER(stats);
}

/*
 * Post-VACUUM cleanup.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
Datum ubtvacuumcleanup(PG_FUNCTION_ARGS)
{
    IndexVacuumInfo *info = (IndexVacuumInfo *)PG_GETARG_POINTER(0);
    IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *)PG_GETARG_POINTER(1);

    /* No-op in ANALYZE ONLY mode */
    if (info->analyze_only) {
        PG_RETURN_POINTER(stats);
    }

    /*
     * If btbulkdelete was called, we need not do anything, just return the
     * stats from the latest btbulkdelete call.  If it wasn't called, we must
     * still do a pass over the index, to recycle any newly-recyclable pages
     * and to obtain index statistics.
     *
     * Since we aren't going to actually delete any leaf items, there's no
     * need to go through all the vacuum-cycle-ID pushups.
     */
    if (stats == NULL) {
        stats = (IndexBulkDeleteResult *)palloc0(sizeof(IndexBulkDeleteResult));
        UBTreeVacuumScan(info, stats, 0);
    }

    /* ubtree don't use Free Space Map */

    /*
     * It's quite possible for us to be fooled by concurrent page splits into
     * double-counting some index tuples, so disbelieve any total that exceeds
     * the underlying heap's count ... if we know that accurately.  Otherwise
     * this might just make matters worse.
     */
    if (!info->estimated_count && stats->num_index_tuples > info->num_heap_tuples && info->num_heap_tuples >= 0) {
        stats->num_index_tuples = info->num_heap_tuples;
    }

    PG_RETURN_POINTER(stats);
}

/*
 * btvacuumscan --- scan the index for VACUUMing purposes
 *
 * This combines the functions of looking for leaf tuples that are deletable
 * according to the vacuum callback, looking for empty pages that can be
 * deleted, and looking for old deleted pages that can be recycled.  Both
 * btbulkdelete and btvacuumcleanup invoke this (the latter only if no
 * btbulkdelete call occurred).
 *
 * The caller is responsible for initially allocating/zeroing a stats struct
 * and for obtaining a vacuum cycle ID if necessary.
 */
static void UBTreeVacuumScan(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
    BTCycleId cycleid)
{
    Relation rel = info->index;
    BTVacState vstate;
    BlockNumber numPages;
    BlockNumber blkno;
    bool needLock = false;

    /*
     * Reset counts that will be incremented during the scan; needed in case
     * of multiple scans during a single VACUUM command
     */
    stats->estimated_count = false;
    stats->num_index_tuples = 0;
    stats->pages_deleted = 0;

    /* Set up info to pass down to btvacuumpage */
    vstate.info = info;
    vstate.stats = stats;
    vstate.cycleid = cycleid;
    vstate.lastBlockVacuumed = BTREE_METAPAGE; /* Initialise at first block */
    vstate.lastBlockLocked = BTREE_METAPAGE;
    vstate.totFreePages = 0;

    /* Create a temporary memory context to run _bt_pagedel in */
    vstate.pagedelcontext = AllocSetContextCreate(CurrentMemoryContext, "_bt_pagedel", ALLOCSET_DEFAULT_MINSIZE,
                                                  ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * The outer loop iterates over all index pages except the metapage, in
     * physical order (we hope the kernel will cooperate in providing
     * read-ahead for speed).  It is critical that we visit all leaf pages,
     * including ones added after we start the scan, else we might fail to
     * delete some deletable tuples.  Hence, we must repeatedly check the
     * relation length.  We must acquire the relation-extension lock while
     * doing so to avoid a race condition: if someone else is extending the
     * relation, there is a window where bufmgr/smgr have created a new
     * all-zero page but it hasn't yet been write-locked by _bt_getbuf(). If
     * we manage to scan such a page here, we'll improperly assume it can be
     * recycled.  Taking the lock synchronizes things enough to prevent a
     * problem: either numPages won't include the new page, or _bt_getbuf
     * already has write lock on the buffer and it will be fully initialized
     * before we can examine it.  (See also vacuumlazy.c, which has the same
     * issue.)	Also, we need not worry if a page is added immediately after
     * we look; the page splitting code already has write-lock on the left
     * page before it adds a right page, so we must already have processed any
     * tuples due to be moved into such a page.
     *
     * We can skip locking for new or temp relations, however, since no one
     * else could be accessing them.
     */
    needLock = !RELATION_IS_LOCAL(rel);

    blkno = BTREE_METAPAGE + 1;
    for (;;) {
        /* Get the current relation length */
        if (needLock) {
            LockRelationForExtension(rel, ExclusiveLock);
        }
        numPages = RelationGetNumberOfBlocks(rel);
        if (needLock) {
            UnlockRelationForExtension(rel, ExclusiveLock);
        }

        /* Quit if we've scanned the whole relation */
        if (blkno >= numPages) {
            break;
        }
        /* Iterate over pages, then loop back to recheck length */
        for (; blkno < numPages; blkno++) {
            UBTreeVacuumPage(&vstate, info->invisibleParts, blkno, blkno);
        }
    }

    /*
     * If the WAL is replayed in hot standby, the replay process needs to get
     * cleanup locks on all index leaf pages, just as we've been doing here.
     * However, we won't issue any WAL records about pages that have no items
     * to be deleted.  For pages between pages we've vacuumed, the replay code
     * will take locks under the direction of the lastBlockVacuumed fields in
     * the XLOG_BTREE_VACUUM WAL records.  To cover pages after the last one
     * we vacuum, we need to issue a dummy XLOG_BTREE_VACUUM WAL record
     * against the last leaf page in the index, if that one wasn't vacuumed.
     */
    if (XLogStandbyInfoActive() && vstate.lastBlockVacuumed < vstate.lastBlockLocked) {
        Buffer buf;

        /*
         * The page should be valid, but we can't use _bt_getbuf() because we
         * want to use a nondefault buffer access strategy.  Since we aren't
         * going to delete any items, getting cleanup lock again is probably
         * overkill, but for consistency do that anyway.
         */
        buf = ReadBufferExtended(rel, MAIN_FORKNUM, vstate.lastBlockLocked, RBM_NORMAL, info->strategy);
        _bt_checkbuffer_valid(rel, buf);
        LockBufferForCleanup(buf);
        _bt_checkpage(rel, buf);
        _bt_delitems_vacuum(rel, buf, NULL, 0, NULL, 0, vstate.lastBlockVacuumed);
        _bt_relbuf(rel, buf);
    }

    MemoryContextDelete(vstate.pagedelcontext);

    /* update statistics */
    stats->num_pages = numPages;
    stats->pages_free = vstate.totFreePages;
}

/*
 * btvacuumpage --- VACUUM one page
 *
 * This processes a single page for btvacuumscan().  In some cases we
 * must go back and re-examine previously-scanned pages; this routine
 * recurses when necessary to handle that case.
 *
 * blkno is the page to process.  origBlkno is the highest block number
 * reached by the outer btvacuumscan loop (the same as blkno, unless we
 * are recursing to re-examine a previous page).
 */
static void UBTreeVacuumPage(BTVacState *vstate, OidRBTree *invisibleParts, BlockNumber blkno, BlockNumber origBlkno)
{
    IndexVacuumInfo *info = vstate->info;
    IndexBulkDeleteResult *stats = vstate->stats;
    Relation rel = info->index;
    bool deleteNow = false;
    BlockNumber recurseTo;
    Buffer buf;
    Page page;
    UBTPageOpaqueInternal opaque;

restart:
    deleteNow = false;
    recurseTo = P_NONE;

    /* call vacuum_delay_point while not holding any buffer lock */
    vacuum_delay_point();

    /*
     * We can't use _bt_getbuf() here because it always applies
     * _bt_checkpage(), which will barf on an all-zero page. We want to
     * recycle all-zero pages, not fail.  Also, we want to use a nondefault
     * buffer access strategy.
     */
    buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, info->strategy);
    _bt_checkbuffer_valid(rel, buf);
    LockBuffer(buf, BT_READ);
    page = BufferGetPage(buf);
    opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    if (!PageIsNew(page)) {
        _bt_checkpage(rel, buf);
    }

    /*
     * If we are recursing, the only case we want to do anything with is a
     * live leaf page having the current vacuum cycle ID.  Any other state
     * implies we already saw the page (eg, deleted it as being empty).
     */
    if (blkno != origBlkno) {
        if (UBTreePageRecyclable(page) || P_IGNORE(opaque) || !P_ISLEAF(opaque) ||
            opaque->btpo_cycleid != vstate->cycleid) {
            _bt_relbuf(rel, buf);
            return;
        }
    }

    /* Page is valid, see what to do with it */
    if (UBTreePageRecyclable(page)) {
        /* Okay to recycle this page */
        vstate->totFreePages++;
        stats->pages_deleted++;
    } else if (P_ISDELETED(opaque)) {
        /* Already deleted, but can't recycle yet */
        stats->pages_deleted++;
    } else if (P_ISHALFDEAD(opaque)) {
        /* Half-dead, try to delete */
        deleteNow = true;
    } else if (P_ISLEAF(opaque)) {
        /*
         * vacuum logic for multi-version index. We use UBTreePagePruneOpt() instead
         * of original vacuum.
         *
         * Note: we only prune leaf pages and never delete right most page.
         */

        /* acquire the write lock for clean up */
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

        bool ignore;
        /* prune and freeze this index page */
        FreezeSingleIndexPage(rel, buf, &ignore, InvalidTransactionId, invisibleParts);

        if (!P_RIGHTMOST(opaque) && PageGetMaxOffsetNumber(page) == 1) {
            /* already empty (only HIKEY left), ok to delete */
            deleteNow = true;
        }

        OffsetNumber minoff = P_FIRSTDATAKEY(opaque);
        OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
        /*
         * If it's now empty, try to delete; else count the live tuples. We
         * don't delete when recursing, though, to avoid putting entries into
         * freePages out-of-order (doesn't seem worth any extra code to handle
         * the case).
         */
        if (minoff <= maxoff) {
            stats->num_index_tuples += maxoff - minoff + 1;
        }
    }

    if (deleteNow) {
        /* Run pagedel in a temp context to avoid memory leakage */
        MemoryContextReset(vstate->pagedelcontext);
        MemoryContext oldcontext = MemoryContextSwitchTo(vstate->pagedelcontext);

        int ndel = UBTreePageDel(rel, buf);
        if (ndel) {
            /* successfully deleted, move to freed page queue */
            UBTreeRecordFreePage(rel, blkno, ReadNewTransactionId());
            /* count only this page, else may double-count parent */
            stats->pages_deleted++;
        }

        MemoryContextSwitchTo(oldcontext);
        /* pagedel released buffer, so we shouldn't */
    } else {
        _bt_relbuf(rel, buf);
    }

    /*
     * This is really tail recursion, but if the compiler is too stupid to
     * optimize it as such, we'd eat an uncomfortably large amount of stack
     * space per recursion level (due to the deletable[] array). A failure is
     * improbable since the number of levels isn't likely to be large ... but
     * just in case, let's hand-optimize into a loop.
     */
    if (recurseTo != P_NONE) {
        blkno = recurseTo;
        goto restart;
    }
}

/*
 *	btcanreturn() -- Check whether btree indexes support index-only scans.
 *
 * btrees always do, so this is trivial.
 */
Datum ubtcanreturn(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(true);
}

/* btmerge() -- merge 2 or more ordered btree-indexes together */
Datum ubtmerge(PG_FUNCTION_ARGS)
{
    /*
     * support max four indexes merge into one
     * must be an empty index relation
     */
    Relation dstIdxRel = (Relation)PG_GETARG_POINTER(0);
    List *srcIdxRelScans = (List *)PG_GETARG_POINTER(1);
    List *srcPartMergeOffsets = (List *)PG_GETARG_POINTER(2);
    List *orderedTupleList = NIL;
    ListCell *cell1 = NULL;
    ListCell *cell2 = NULL;
    BlockNumber offsetItup = 0;
    IndexTuple loadItup = NULL;
    ScanDirection dir = ForwardScanDirection;
    IndexScanDesc srcIdxRelScan = NULL;
    BTOrderedIndexListElement *ele = NULL;
    TupleDesc tupdes = RelationGetDescr(dstIdxRel);
    int keysz = IndexRelationGetNumberOfKeyAttributes(dstIdxRel);
    BTScanInsert itupKey = UBTreeMakeScanKey(dstIdxRel, NULL);
    BTWriteState wstate;
    BTPageState *state = NULL;
    IndexBuildResult *result = NULL;
    double indextuples = 0;

    /*
     * 2 steps:
     * 1 read in all index tuples from index files;
     * 2 merge sort, act like _bt_load.
     */
    /*
     * the preparation of merge
     * read from src index relation and insert them into ordered index list
     */
    forboth(cell1, srcIdxRelScans, cell2, srcPartMergeOffsets)
    {
        srcIdxRelScan = (IndexScanDesc)lfirst(cell1);
        offsetItup = (BlockNumber)lfirst_int(cell2);

        loadItup = UBTreeGetIndexTuple(srcIdxRelScan, dir, offsetItup);
        orderedTupleList = insert_ordered_index(orderedTupleList, tupdes, itupKey->scankeys, keysz,
            loadItup, offsetItup, srcIdxRelScan);
    }

    wstate.heap = NULL; /* we don't have heap relation info here */
    wstate.index = dstIdxRel;
    wstate.inskey = itupKey;
    /*
     * We need to log index creation in WAL iff WAL archiving/streaming is
     * enabled UNLESS the index isn't WAL-logged anyway.
     */
    wstate.btws_use_wal = XLogIsNeeded() && RelationNeedsWAL(wstate.index);

    /* reserve the metapage */
    wstate.btws_pages_alloced = BTREE_METAPAGE + 1;
    wstate.btws_pages_written = 0;
    wstate.btws_zeropage = NULL; /* until needed */

    /* This loop handles advancing to the next array elements, if any */
    while (orderedTupleList != NULL) {
        // get the first element from orderedTupleList.
        ele = (BTOrderedIndexListElement *)linitial(orderedTupleList);
        if (ele == NULL) {
            orderedTupleList = list_delete_first(orderedTupleList);
        } else {
            srcIdxRelScan = ele->indexScanDesc;
            offsetItup = ele->heapModifiedOffset;
            loadItup = ele->itup;

            /* When we see first tuple, create first index page */
            if (state == NULL) {
                state = UBTreePageState(&wstate, 0);
            }

            // add index tuple loadItup, remove it from orderedTupleList
            UBTreeBuildAdd(&wstate, state, loadItup, srcIdxRelScan->xs_want_xid);
            indextuples += 1;
            orderedTupleList = list_delete_first(orderedTupleList);
            pfree(ele);

            // load next index tuple
            loadItup = UBTreeGetIndexTuple(srcIdxRelScan, dir, offsetItup);
            // insert load index tuple into orderedTupleList
            orderedTupleList = insert_ordered_index(orderedTupleList, tupdes, itupKey->scankeys, keysz,
                loadItup, offsetItup, srcIdxRelScan);
        }
    };

    // clean up
    pfree(itupKey);

    /* Close down final pages and write the metapage */
    UBTreeUpperShutDown(&wstate, state);

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
    if (RelationNeedsWAL(wstate.index)) {
        RelationOpenSmgr(wstate.index);
        smgrimmedsync(wstate.index->rd_smgr, MAIN_FORKNUM);
    }

    /*
     * Return statistics
     */
    result = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));

    result->heap_tuples = indextuples;
    result->index_tuples = indextuples;
    PG_RETURN_POINTER(result);
}

Datum ubtoptions(PG_FUNCTION_ARGS)
{
    return btoptions(fcinfo);
}

static IndexTuple UBTreeGetIndexTuple(IndexScanDesc scan, ScanDirection dir, BlockNumber heapTupleBlkOffset)
{
    bool found = UBTreeGetTupleInternal(scan, dir);
    /* Reset kill flag immediately for safety */
    scan->kill_prior_tuple = false;

    /* If we're out of index entries, we're done */
    if (!found) {
        /* ... but first, release any held pin on a heap page */
        if (BufferIsValid(scan->xs_cbuf)) {
            ReleaseBuffer(scan->xs_cbuf);
            scan->xs_cbuf = InvalidBuffer;
        }
        return NULL;
    }

    /* Return the index tuple we found. */
    if (heapTupleBlkOffset != 0) {
        IndexTuple itup = scan->xs_itup;
        BlockNumber destBlkno = ItemPointerGetBlockNumber(&(itup->t_tid));

        destBlkno += heapTupleBlkOffset;
        ItemPointerSetBlockNumber(&(itup->t_tid), destBlkno);
    }
    return scan->xs_itup;
}

static void ReportXidBaseError(Relation rel, Buffer buf, OffsetNumber offnum, TransactionId oldXid,
    TransactionId newXid, TransactionId oldBase, TransactionId newBase)
{
    ereport(ERROR, (errcode(ERRCODE_CANNOT_MODIFY_XIDBASE),
            errmsg("Can't fit xid into page: rel \"%s\", blkno %u, offset %u, "
                   "oldXid/newXid %lu/%lu, base from %lu to %lu (delta %ld)",
                   rel ? RelationGetRelationName(rel) : "creating",
                   BufferIsValid(buf) ? BufferGetBlockNumber(buf) : 0,
                   offnum, oldXid, newXid, oldBase, newBase, newBase - oldBase),
            errhint("oldest xmin, which will become the new xid base, "
                    "may be stuck by some running sessions.")));
}

/*
 * Shift xid base in the page. WAL-logged if buffer is specified.
 * page is the heap page; delta is the size of change about xid base
 */
static void IndexPageShiftBase(Relation rel, Page page, int64 delta, bool needWal, Buffer buf)
{
    WHITEBOX_TEST_STUB("IndexPageShiftBase-begin", WhiteboxDefaultErrorEmit);
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    int64 oldBase = opaque->xid_base;
    int64 newBase = opaque->xid_base + delta;

    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    /* scan the xids for the first time, and check whether this delta is appropriate */
    for (OffsetNumber offnum = P_FIRSTDATAKEY(opaque); offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId itemid = PageGetItemId(page, offnum);
        if (!ItemIdHasStorage(itemid)) {
            continue;
        }

        IndexTuple itup = (IndexTuple)PageGetItem(page, itemid);
        UstoreIndexXid uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(itup);
        if (TransactionIdIsNormal(uxid->xmin)) {
            ShortTransactionId shortXid = uxid->xmin;
            TransactionId oldXid = ShortTransactionIdToNormal(oldBase, shortXid);
            shortXid -= delta;
            TransactionId newXid = ShortTransactionIdToNormal(newBase, shortXid);
            if (!TransactionIdEquals(oldXid, newXid)) {
                ReportXidBaseError(rel, buf, offnum, oldXid, newXid, oldBase, newBase);
            }
        }
        if (TransactionIdIsNormal(uxid->xmax)) {
            ShortTransactionId shortXid = uxid->xmax;
            TransactionId oldXid = ShortTransactionIdToNormal(oldBase, shortXid);
            shortXid -= delta;
            TransactionId newXid = ShortTransactionIdToNormal(newBase, shortXid);
            if (!TransactionIdEquals(oldXid, newXid)) {
                ReportXidBaseError(rel, buf, offnum, oldXid, newXid, oldBase, newBase);
            }
        }
    }

    /* Do the update.  No ereport(ERROR) until changes are logged */
    START_CRIT_SECTION();

    /* reset pd_prune_xid for prune hint */
    ((PageHeader)page)->pd_prune_xid = InvalidTransactionId;

    /* Iterate over page items, and shift xid */
    for (OffsetNumber offnum = P_FIRSTDATAKEY(opaque); offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId itemid = PageGetItemId(page, offnum);
        if (!ItemIdHasStorage(itemid)) {
            continue;
        }

        IndexTuple itup = (IndexTuple)PageGetItem(page, itemid);
        UstoreIndexXid uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(itup);
        /* Apply xid shift to heap tuple */
        if (TransactionIdIsNormal(uxid->xmin)) {
            uxid->xmin -= delta;
        }
        if (TransactionIdIsNormal(uxid->xmax)) {
            uxid->xmax -= delta;
        }
    }

    /* Apply xid shift to base as well */
    opaque->xid_base += delta;

    if (BufferIsValid(buf)) {
        MarkBufferDirty(buf);
    }

    /* Write WAL record if needed */
    if (needWal) {
        XLogRecPtr recptr;
        xl_ubtree2_shift_base xlrec;
        xlrec.delta = delta;

        XLogBeginInsert();
        XLogRegisterData((char*)&xlrec, SizeOfUBTree2ShiftBase);

        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);

        recptr = XLogInsert(RM_UBTREE2_ID, XLOG_UBTREE2_SHIFT_BASE);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    WHITEBOX_TEST_STUB("IndexPageShiftBase-end", WhiteboxDefaultErrorEmit);
}

void FreezeSingleIndexPage(Relation rel, Buffer buf, bool *hasPruned,
    TransactionId oldestXmin, OidRBTree *invisibleParts)
{
    int nfrozen = 0;
    OffsetNumber nowfrozen[MaxIndexTuplesPerPage];
    if (!TransactionIdIsValid(oldestXmin)) {
        oldestXmin = u_sess->utils_cxt.RecentGlobalDataXmin;
    }

    WHITEBOX_TEST_STUB("FreezeSingleIndexPage", WhiteboxDefaultErrorEmit);

    /* first prune the page, remove all Abort xid and Frozen xmax */
    *hasPruned = UBTreePagePrune(rel, buf, oldestXmin, invisibleParts);

    Page page = BufferGetPage(buf);
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    /* Iterate over page items */
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

    int nstorage = 0;
    for (OffsetNumber offnum = P_FIRSTDATAKEY(opaque); offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
        ItemId itemid = PageGetItemId(page, offnum);
        if (!ItemIdHasStorage(itemid)) {
            continue;
        }
        nstorage++;

        IndexTuple itup = (IndexTuple)PageGetItem(page, itemid);
        UstoreIndexXid uxid = (UstoreIndexXid)UstoreIndexTupleGetXid(itup);
        /* check xmin */
        if (TransactionIdIsNormal(uxid->xmin)) {
            TransactionId xmin = ShortTransactionIdToNormal(opaque->xid_base, uxid->xmin);
            if (TransactionIdPrecedes(xmin, oldestXmin)) {
                /* xmin can't be abort here */
                if (!TransactionIdDidCommit(xmin)) {
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("freeze index page failed: cannot freeze uncommited xmin %lu before xid cutoff %lu."
                                   " rel \"%s\", blkno %u, offset %u.",
                                   xmin, oldestXmin, RelationGetRelationName(rel),
                                   BufferGetBlockNumber(buf), offnum)));
                }
                uxid->xmin = FrozenTransactionId;
                nowfrozen[nfrozen++] = offnum;
            }
        }
        /* check xmax */
        if (TransactionIdIsNormal(uxid->xmax)) {
            TransactionId xmax = ShortTransactionIdToNormal(opaque->xid_base, uxid->xmax);
            if (TransactionIdPrecedes(xmax, oldestXmin)) {
                if (TransactionIdDidCommit(xmax)) {
                    ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                            errmsg("freeze index page failed: cannot freeze commited xmax %lu before xid cutoff %lu."
                                   " rel \"%s\", blkno %u, offset %u.",
                                   xmax, oldestXmin, RelationGetRelationName(rel),
                                   BufferGetBlockNumber(buf), offnum)));
                }
                uxid->xmax = InvalidTransactionId;
            }
        }
    }

    if (nstorage == 0) {
        return; /* no tuple to freeze */
    }

    START_CRIT_SECTION();

    MarkBufferDirty(buf);
    /* Write WAL record if needed */
    if (RelationNeedsWAL(rel)) {
        xl_ubtree2_freeze xlrec;
        xlrec.oldestXmin = oldestXmin;
        xlrec.blkno = BufferGetBlockNumber(buf);
        xlrec.nfrozen = nfrozen;

        XLogBeginInsert();
        XLogRegisterData((char*)&xlrec, SizeOfUBTree2Freeze);
        XLogRegisterData((char*)&nowfrozen, nfrozen * sizeof(OffsetNumber));

        XLogRegisterBuffer(0, buf, REGBUF_STANDARD);

        XLogRecPtr recptr = XLogInsert(RM_UBTREE2_ID, XLOG_UBTREE2_FREEZE);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();
}

/*
 * IndexPagePrepareForXid - make sure the given xid can fit this page
 *
 *      The return value indicates whether this page has been pruned.
 *
 *      When creating index, we will have:
 *          rel: NULL
 *          needWal: false
 *          buf: InvalidBuffer
 */
bool IndexPagePrepareForXid(Relation rel, Page page, TransactionId xid, bool needWal, Buffer buf)
{
    bool hasPruned = false;
    if (!TransactionIdIsNormal(xid)) {
        return false;
    }
    UBTPageOpaqueInternal opaque = (UBTPageOpaqueInternal)PageGetSpecialPointer(page);
    if (!P_ISLEAF(opaque)) {
        return false;
    }
    TransactionId curBase = opaque->xid_base;
    /* Can we already store this xid? */
    if (xid >= curBase + FirstNormalTransactionId && xid <= curBase + MaxShortTransactionId) {
        return false;
    }

    TransactionId oldestXmin = u_sess->utils_cxt.RecentGlobalDataXmin;
    /* remove old xids first. we don't need to do this when creating index without valid rel and buf */
    if (rel != NULL && BufferIsValid(buf)) {
        FreezeSingleIndexPage(rel, buf, &hasPruned, oldestXmin);
    }

    /* we can just choose oldest xid in undo as the new base */
    int64 newBase = oldestXmin - FirstNormalTransactionId; /* reserve 3 more xid for Invalid/Bootstrap/Frozen */
    int64 delta = newBase - curBase;
    IndexPageShiftBase(rel, page, delta, needWal, buf);
    return hasPruned;
}
