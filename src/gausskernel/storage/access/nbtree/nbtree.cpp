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
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/xlog.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "storage/indexfsm.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
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

static void btvacuumscan(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback,
                         void *callback_state, BTCycleId cycleid);
static void btvacuumpage(BTVacState *vstate, BlockNumber blkno, BlockNumber orig_blkno);

static IndexTuple btgetindextuple(IndexScanDesc scan, ScanDirection dir, BlockNumber heapTupleBlkOffset);

/*
 *	btbuild() -- build a new btree index.
 */
Datum btbuild(PG_FUNCTION_ARGS)
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
    _bt_leafbuild(buildstate.spool, buildstate.spool2);
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
void btbuildCallback(Relation index, HeapTuple htup, Datum *values, const bool *isnull, bool tupleIsAlive,
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
Datum btbuildempty(PG_FUNCTION_ARGS)
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

    _bt_initmetapage(metapage, P_NONE, 0);

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

/*
 *	btinsert() -- insert an index tuple into a btree.
 *
 *		Descend the tree recursively, find the appropriate location for our
 *		new tuple, and put it there.
 */
Datum btinsert(PG_FUNCTION_ARGS)
{
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    Datum *values = (Datum *)PG_GETARG_POINTER(1);
    bool *isnull = (bool *)PG_GETARG_POINTER(2);
    ItemPointer ht_ctid = (ItemPointer)PG_GETARG_POINTER(3);
    Relation heapRel = (Relation)PG_GETARG_POINTER(4);
    IndexUniqueCheck checkUnique = (IndexUniqueCheck)PG_GETARG_INT32(5);
    bool result = false;
    IndexTuple itup;

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
    itup->t_tid = *ht_ctid;

    result = _bt_doinsert(rel, itup, checkUnique, heapRel);

    pfree(itup);

    PG_RETURN_BOOL(result);
}

/*
 *	btgettuple() -- Get the next tuple in the scan.
 */
Datum btgettuple(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanDirection dir = (ScanDirection)PG_GETARG_INT32(1);

    if (scan == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid arguments for function btgettuple")));
    }

    bool res = _bt_gettuple_internal(scan, dir);

    PG_RETURN_BOOL(res);
}

/*
 * btgetbitmap() -- gets all matching tuples, and adds them to a bitmap
 */
Datum btgetbitmap(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    TIDBitmap *tbm = (TIDBitmap *)PG_GETARG_POINTER(1);
    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    int64 ntids = 0;
    ItemPointer heapTid;

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
        if (_bt_first(scan, ForwardScanDirection)) {
            /* Save tuple ID, and continue scanning */
            heapTid = &scan->xs_ctup.t_self;
            Oid currPartOid = so->currPos.items[so->currPos.itemIndex].partitionOid;
            int2 bucketid = so->currPos.items[so->currPos.itemIndex].bucketid;
            tbm_add_tuples(tbm, heapTid, 1, false, currPartOid, bucketid);
            ntids++;

            for (;;) {
                /*
                 * Advance to next tuple within page.  This is the same as the
                 * easy case in _bt_next().
                 */
                if (++so->currPos.itemIndex > so->currPos.lastItem) {
                    /* let _bt_next do the heavy lifting */
                    if (!_bt_next(scan, ForwardScanDirection)) {
                        break;
                    }
                }

                /* Save tuple ID, and continue scanning */
                heapTid = &so->currPos.items[so->currPos.itemIndex].heapTid;
                currPartOid = so->currPos.items[so->currPos.itemIndex].partitionOid;
                bucketid = so->currPos.items[so->currPos.itemIndex].bucketid;
                tbm_add_tuples(tbm, heapTid, 1, false, currPartOid, bucketid);
                ntids++;
            }
        }
        /* Now see if we have more array keys to deal with */
    } while (so->numArrayKeys && _bt_advance_array_keys(scan, ForwardScanDirection));

    PG_RETURN_INT64(ntids);
}

Datum cbtreegetbitmap(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    IndexSortState *sort = (IndexSortState *)PG_GETARG_POINTER(1);
    VectorBatch *tids = (VectorBatch *)PG_GETARG_POINTER(2);

    ScalarVector *vecs = tids->m_arr;
    int offset = 0;
    BTScanOpaque so = (BTScanOpaque)scan->opaque;
    ItemPointer heapTid;

    int64 ntids = 0;

    /* Fetch the first page & tuple */
    if (_bt_first(scan, ForwardScanDirection)) {
        /* Save tuple ID, and continue scanning */
        heapTid = &scan->xs_ctup.t_self;
        ItemPointer itemP = (ItemPointer) & (vecs[0].m_vals[offset]);
        itemP->ip_blkid = heapTid->ip_blkid;
        itemP->ip_posid = heapTid->ip_posid;
        offset++;
        ntids++;

        for (;;) {
            /*
             * Advance to next tuple within page.  This is the same as the
             * easy case in _bt_next().
             */
            if (++so->currPos.itemIndex > so->currPos.lastItem) {
                /* let _bt_next do the heavy lifting */
                if (!_bt_next(scan, ForwardScanDirection)) {
                    sort->m_tidEnd = true;
                    break;
                }
            }

            /* Save tuple ID, and continue scanning */
            heapTid = &so->currPos.items[so->currPos.itemIndex].heapTid;
            itemP = (ItemPointer) & (vecs[0].m_vals[offset]);
            itemP->ip_blkid = heapTid->ip_blkid;
            itemP->ip_posid = heapTid->ip_posid;
            offset++;
            if (offset == BatchMaxSize) {
                vecs[0].m_rows = offset;
                tids->m_rows = offset;
                PutBatchToSorter(sort, tids);
                tids->Reset(true);
                offset = 0;
            }
            ntids++;
        }
        if (offset > 0) {
            vecs[0].m_rows = offset;
            tids->m_rows = offset;
            PutBatchToSorter(sort, tids);
        }
    }

    PG_RETURN_INT64(ntids);
}

/*
 *	btbeginscan() -- start a scan on a btree index
 */
Datum btbeginscan(PG_FUNCTION_ARGS)
{
    Relation rel = (Relation)PG_GETARG_POINTER(0);
    int nkeys = PG_GETARG_INT32(1);
    int norderbys = PG_GETARG_INT32(2);
    IndexScanDesc scan;
    BTScanOpaque so;

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

    /* used only in ubtree */
    so->lastSelfModifiedItup = NULL;
    so->lastSelfModifiedItupBufferSize = 0;
    so->indexInfo = NULL;
    so->fakeEstate = NULL;

    scan->xs_itupdesc = RelationGetDescr(rel);
    scan->opaque = so;

    PG_RETURN_POINTER(scan);
}

/*
 *	btrescan() -- rescan an index relation
 */
Datum btrescan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc)PG_GETARG_POINTER(0);
    ScanKey scankey = (ScanKey)PG_GETARG_POINTER(1);

    /* remaining arguments are ignored */
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
        so->currTuples = (char *)palloc(BLCKSZ << 1);
        so->markTuples = so->currTuples + BLCKSZ;
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
Datum btendscan(PG_FUNCTION_ARGS)
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

    /* so->markTuples should not be pfree'd, see btrescan */
    pfree(so);

    PG_RETURN_VOID();
}

/*
 *	btmarkpos() -- save current scan position
 */
Datum btmarkpos(PG_FUNCTION_ARGS)
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
Datum btrestrpos(PG_FUNCTION_ARGS)
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
Datum btbulkdelete(PG_FUNCTION_ARGS)
{
    IndexVacuumInfo *info = (IndexVacuumInfo *)PG_GETARG_POINTER(0);
    IndexBulkDeleteResult *volatile stats = (IndexBulkDeleteResult *)PG_GETARG_POINTER(1);
    IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback)PG_GETARG_POINTER(2);
    void *callback_state = (void *)PG_GETARG_POINTER(3);
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

        btvacuumscan(info, stats, callback, callback_state, cycleid);
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
Datum btvacuumcleanup(PG_FUNCTION_ARGS)
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
        btvacuumscan(info, stats, NULL, NULL, 0);
    }

    /* Finally, vacuum the FSM */
    IndexFreeSpaceMapVacuum(info->index);

    /*
     * It's quite possible for us to be fooled by concurrent page splits into
     * double-counting some index tuples, so disbelieve any total that exceeds
     * the underlying heap's count ... if we know that accurately.  Otherwise
     * this might just make matters worse.
     */
    if (!info->estimated_count && stats->num_index_tuples > info->num_heap_tuples) {
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
static void btvacuumscan(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback,
                         void *callback_state, BTCycleId cycleid)
{
    Relation rel = info->index;
    BTVacState vstate;
    BlockNumber num_pages;
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
    vstate.callback = callback;
    vstate.callback_state = callback_state;
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
     * problem: either num_pages won't include the new page, or _bt_getbuf
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
        num_pages = RelationGetNumberOfBlocks(rel);
        if (needLock) {
            UnlockRelationForExtension(rel, ExclusiveLock);
        }

        /* Quit if we've scanned the whole relation */
        if (blkno >= num_pages) {
            break;
        }
        /* Iterate over pages, then loop back to recheck length */
        for (; blkno < num_pages; blkno++) {
            btvacuumpage(&vstate, blkno, blkno);
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
        _bt_delitems_vacuum(rel, buf, NULL, 0, vstate.lastBlockVacuumed);
        _bt_relbuf(rel, buf);
    }

    MemoryContextDelete(vstate.pagedelcontext);

    /* update statistics */
    stats->num_pages = num_pages;
    stats->pages_free = vstate.totFreePages;
}

/*
 * btvacuumpage --- VACUUM one page
 *
 * This processes a single page for btvacuumscan().  In some cases we
 * must go back and re-examine previously-scanned pages; this routine
 * recurses when necessary to handle that case.
 *
 * blkno is the page to process.  orig_blkno is the highest block number
 * reached by the outer btvacuumscan loop (the same as blkno, unless we
 * are recursing to re-examine a previous page).
 */
static void btvacuumpage(BTVacState *vstate, BlockNumber blkno, BlockNumber orig_blkno)
{
    IndexVacuumInfo *info = vstate->info;
    IndexBulkDeleteResult *stats = vstate->stats;
    IndexBulkDeleteCallback callback = vstate->callback;
    void *callback_state = vstate->callback_state;
    Relation rel = info->index;
    bool delete_now = false;
    BlockNumber recurse_to;
    Buffer buf;
    Page page;
    BTPageOpaqueInternal opaque;

restart:
    delete_now = false;
    recurse_to = P_NONE;

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
    opaque = (BTPageOpaqueInternal)PageGetSpecialPointer(page);
    if (!PageIsNew(page)) {
        _bt_checkpage(rel, buf);
    }

    /*
     * If we are recursing, the only case we want to do anything with is a
     * live leaf page having the current vacuum cycle ID.  Any other state
     * implies we already saw the page (eg, deleted it as being empty).
     */
    if (blkno != orig_blkno) {
        if (_bt_page_recyclable(page) || P_IGNORE(opaque) || !P_ISLEAF(opaque) ||
            opaque->btpo_cycleid != vstate->cycleid) {
            _bt_relbuf(rel, buf);
            return;
        }
    }

    /* Page is valid, see what to do with it */
    if (_bt_page_recyclable(page)) {
        /* Okay to recycle this page */
        RecordFreeIndexPage(rel, blkno);
        vstate->totFreePages++;
        stats->pages_deleted++;
    } else if (P_ISDELETED(opaque)) {
        /* Already deleted, but can't recycle yet */
        stats->pages_deleted++;
    } else if (P_ISHALFDEAD(opaque)) {
        /* Half-dead, try to delete */
        delete_now = true;
    } else if (P_ISLEAF(opaque)) {
        OffsetNumber deletable[MaxOffsetNumber];
        int ndeletable;
        OffsetNumber offnum, minoff, maxoff;

        /*
         * Trade in the initial read lock for a super-exclusive write lock on
         * this page.  We must get such a lock on every leaf page over the
         * course of the vacuum scan, whether or not it actually contains any
         * deletable tuples --- see nbtree/README.
         */
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        LockBufferForCleanup(buf);

        /*
         * Remember highest leaf page number we've taken cleanup lock on; see
         * notes in btvacuumscan
         */
        if (blkno > vstate->lastBlockLocked) {
            vstate->lastBlockLocked = blkno;
        }

        /*
         * Check whether we need to recurse back to earlier pages.	What we
         * are concerned about is a page split that happened since we started
         * the vacuum scan.  If the split moved some tuples to a lower page
         * then we might have missed 'em.  If so, set up for tail recursion.
         * (Must do this before possibly clearing btpo_cycleid below!)
         */
        if (vstate->cycleid != 0 && opaque->btpo_cycleid == vstate->cycleid && !(opaque->btpo_flags & BTP_SPLIT_END) &&
            !P_RIGHTMOST(opaque) && opaque->btpo_next < orig_blkno) {
            recurse_to = opaque->btpo_next;
        }

        /*
         * Scan over all items to see which ones need deleted according to the
         * callback function.
         */
        ndeletable = 0;
        minoff = P_FIRSTDATAKEY(opaque);
        maxoff = PageGetMaxOffsetNumber(page);
        if (callback) {
            for (offnum = minoff; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
                IndexTuple itup = (IndexTuple)PageGetItem(page, PageGetItemId(page, offnum));
                ItemPointer htup = &(itup->t_tid);

                /*
                 * During Hot Standby we currently assume that
                 * XLOG_BTREE_VACUUM records do not produce conflicts. That is
                 * only true as long as the callback function depends only
                 * upon whether the index tuple refers to heap tuples removed
                 * in the initial heap scan. When vacuum starts it derives a
                 * value of OldestXmin. Backends taking later snapshots could
                 * have a RecentGlobalXmin with a later xid than the vacuum's
                 * OldestXmin, so it is possible that row versions deleted
                 * after OldestXmin could be marked as killed by other
                 * backends. The callback function *could* look at the index
                 * tuple state in isolation and decide to delete the index
                 * tuple, though currently it does not. If it ever did, we
                 * would need to reconsider whether XLOG_BTREE_VACUUM records
                 * should cause conflicts. If they did cause conflicts they
                 * would be fairly harsh conflicts, since we haven't yet
                 * worked out a way to pass a useful value for
                 * latestRemovedXid on the XLOG_BTREE_VACUUM records. This
                 * applies to *any* type of index that marks index tuples as
                 * killed.
                 */
                Oid partOid = InvalidOid;
                int2 bktId = InvalidBktId;
                if (RelationIsGlobalIndex(rel)) {
                    partOid = index_getattr_tableoid(rel, itup);
                }
                if (RelationIsCrossBucketIndex(rel)) {
                    bktId = index_getattr_bucketid(rel, itup);
                }
                if (callback(htup, callback_state, partOid, bktId)) {
                    deletable[ndeletable++] = offnum;
                }
            }
        }

        /*
         * Apply any needed deletes.  We issue just one _bt_delitems_vacuum()
         * call per page, so as to minimize WAL traffic.
         */
        if (ndeletable > 0) {
            /*
             * Notice that the issued XLOG_BTREE_VACUUM WAL record includes an
             * instruction to the replay code to get cleanup lock on all pages
             * between the previous lastBlockVacuumed and this page.  This
             * ensures that WAL replay locks all leaf pages at some point.
             *
             * Since we can visit leaf pages out-of-order when recursing,
             * replay might end up locking such pages an extra time, but it
             * doesn't seem worth the amount of bookkeeping it'd take to avoid
             * that.
             */
            _bt_delitems_vacuum(rel, buf, deletable, ndeletable, vstate->lastBlockVacuumed);

            /*
             * Remember highest leaf page number we've issued a
             * XLOG_BTREE_VACUUM WAL record for.
             */
            if (blkno > vstate->lastBlockVacuumed) {
                vstate->lastBlockVacuumed = blkno;
            }

            stats->tuples_removed += ndeletable;
            /* must recompute maxoff */
            maxoff = PageGetMaxOffsetNumber(page);
        } else {
            /*
             * If the page has been split during this vacuum cycle, it seems
             * worth expending a write to clear btpo_cycleid even if we don't
             * have any deletions to do.  (If we do, _bt_delitems_vacuum takes
             * care of this.)  This ensures we won't process the page again.
             * We treat this like a hint-bit update because there's no need to
             * WAL-log it.
             */
            if (vstate->cycleid != 0 && opaque->btpo_cycleid == vstate->cycleid) {
                opaque->btpo_cycleid = 0;
                MarkBufferDirtyHint(buf, true);
            }
        }

        /*
         * If it's now empty, try to delete; else count the live tuples. We
         * don't delete when recursing, though, to avoid putting entries into
         * freePages out-of-order (doesn't seem worth any extra code to handle
         * the case).
         */
        if (minoff > maxoff) {
            delete_now = (blkno == orig_blkno);
        } else {
            stats->num_index_tuples += maxoff - minoff + 1;
        }
    }

    if (delete_now) {
        /* Run pagedel in a temp context to avoid memory leakage */
        MemoryContextReset(vstate->pagedelcontext);
        MemoryContext oldcontext = MemoryContextSwitchTo(vstate->pagedelcontext);

        int ndel = _bt_pagedel(rel, buf, NULL);
        if (ndel) {
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
    if (recurse_to != P_NONE) {
        blkno = recurse_to;
        goto restart;
    }
}

/*
 *	btcanreturn() -- Check whether btree indexes support index-only scans.
 *
 * btrees always do, so this is trivial.
 */
Datum btcanreturn(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(true);
}

/* btmerge() -- merge 2 or more ordered btree-indexes together */
Datum btmerge(PG_FUNCTION_ARGS)
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
    BlockNumber offset_itup = 0;
    IndexTuple load_itup = NULL;
    ScanDirection dir = ForwardScanDirection;
    IndexScanDesc srcIdxRelScan = NULL;
    BTOrderedIndexListElement *ele = NULL;
    TupleDesc tupdes = RelationGetDescr(dstIdxRel);
    int keysz = IndexRelationGetNumberOfKeyAttributes(dstIdxRel);
    ScanKey indexScanKey = _bt_mkscankey_nodata(dstIdxRel);
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
        offset_itup = (BlockNumber)lfirst_int(cell2);

        load_itup = btgetindextuple(srcIdxRelScan, dir, offset_itup);
        orderedTupleList = insert_ordered_index(orderedTupleList, tupdes, indexScanKey, keysz, load_itup, offset_itup,
                                                srcIdxRelScan);
    }

    wstate.index = dstIdxRel;
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
            offset_itup = ele->heapModifiedOffset;
            load_itup = ele->itup;

            /* When we see first tuple, create first index page */
            if (state == NULL) {
                state = _bt_pagestate(&wstate, 0);
            }

            // add index tuple load_itup, remove it from orderedTupleList
            _bt_buildadd(&wstate, state, load_itup);
            indextuples += 1;
            orderedTupleList = list_delete_first(orderedTupleList);
            pfree(ele);

            // load next index tuple
            load_itup = btgetindextuple(srcIdxRelScan, dir, offset_itup);
            // insert load index tuple into orderedTupleList
            orderedTupleList = insert_ordered_index(orderedTupleList, tupdes, indexScanKey, keysz, load_itup,
                                                    offset_itup, srcIdxRelScan);
        }
    };

    // clean up
    _bt_freeskey(indexScanKey);

    /* Close down final pages and write the metapage */
    _bt_uppershutdown(&wstate, state);

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

static IndexTuple btgetindextuple(IndexScanDesc scan, ScanDirection dir, BlockNumber heapTupleBlkOffset)
{
    while (true) {
        bool found = _bt_gettuple_internal(scan, dir);
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
        if (!scan->xs_want_bucketid || scan->xs_cbi_scan->mergingBktId == scan->xs_cbi_scan->bucketid) {
            break;
        }
    }

    /* Return the index tuple we found. */
    if (heapTupleBlkOffset != 0) {
        IndexTuple itup = scan->xs_itup;
        BlockNumber dest_blkno = ItemPointerGetBlockNumber(&(itup->t_tid));

        dest_blkno += heapTupleBlkOffset;
        ItemPointerSetBlockNumber(&(itup->t_tid), dest_blkno);
    }
    return scan->xs_itup;
}
