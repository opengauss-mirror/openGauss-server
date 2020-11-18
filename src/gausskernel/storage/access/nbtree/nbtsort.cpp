/* -------------------------------------------------------------------------
 *
 * nbtsort.cpp
 *		Build a btree from sorted input by loading leaf pages sequentially.
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
 *	  src/gausskernel/storage/access/nbtree/nbtsort.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
#include "access/parallel.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/index.h"
#include "libpq/pqmq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/aiomem.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/tuplesort.h"
#include "commands/tablespace.h"
#include "access/transam.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"

/*
 * DISABLE_LEADER_PARTICIPATION disables the leader's participation in
 * parallel index builds.  This may be useful as a debugging aid.
#undef DISABLE_LEADER_PARTICIPATION
 */

#define BT_SCAN_WAIT_TIME 1 /* seconds, timeout for waiting end of heap scan*/

/*
 * Status record for spooling/sorting phase.  (Note we may have two of
 * these due to the special requirements for uniqueness-checking with
 * dead tuples.)
 */
typedef struct BTSpool {
    Tuplesortstate* sortstate; /* state data for tuplesort.c */
    Relation heap;
    Relation index;
    bool isunique;
} BTSpool;

/*
 * Status for index builds performed in parallel.  This is allocated in a
 * dynamic shared memory segment.  Note that there is a separate tuplesort TOC
 * entry, private to tuplesort.c but allocated by this module on its behalf.
 */
typedef struct BTShared {
    /*
     * These fields are not modified during the sort.  They primarily exist
     * for the benefit of worker processes that need to create BTSpool state
     * corresponding to that used by the leader.
     */
    Oid heaprelid;
    Oid indexrelid;
    bool isunique;
    bool isconcurrent;
    int scantuplesortstates;

    /*
     * workersdonecv is used to monitor the progress of workers.  All parallel
     * participants must indicate that they are done before leader can use
     * mutable state that workers maintain during scan (and before leader can
     * proceed to tuplesort_performsort()).
     */
    pthread_cond_t workersdonecv;
    pthread_mutex_t mtx; /* mtx protects workersdonecv */

    /*
     * mutex protects all fields before heapdesc.
     *
     * These fields contain status information of interest to B-Tree index
     * builds that must work just the same when an index is built in parallel.
     */
    slock_t mutex;

    /*
     * Mutable state that is maintained by workers, and reported back to
     * leader at end of parallel scan.
     *
     * nparticipantsdone is number of worker processes finished.
     *
     * reltuples is the total number of input heap tuples.
     *
     * havedead indicates if RECENTLY_DEAD tuples were encountered during
     * build.
     *
     * indtuples is the total number of tuples that made it into the index.
     *
     * brokenhotchain indicates if any worker detected a broken HOT chain
     * during build.
     */
    int nparticipantsdone;
    double reltuples;
    bool havedead;
    double indtuples;
    bool brokenhotchain;

    /*
     * This variable-sized field must come last.
     *
     * See _bt_parallel_estimate_shared().
     */
    ParallelHeapScanDescData heapdesc;
} BTShared;

/*
 * Status for leader in parallel index build.
 */
typedef struct BTLeader {
    /* parallel context itself */
    ParallelContext *pcxt;

    /*
     * nparticipanttuplesorts is the exact number of worker processes
     * successfully launched, plus one leader process if it participates as a
     * worker (only DISABLE_LEADER_PARTICIPATION builds avoid leader
     * participating as a worker).
     */
    int nparticipanttuplesorts;

    /*
     * Leader process convenience pointers to shared state (leader avoids TOC
     * lookups).
     *
     * btshared is the shared state for entire build.  sharedsort is the
     * shared, tuplesort-managed state passed to each process tuplesort.
     * sharedsort2 is the corresponding btspool2 shared state, used only when
     * building unique indexes.  snapshot is the snapshot used by the scan iff
     * an MVCC snapshot is required.
     */
    BTShared *btshared;
    SharedSort *sharedsort;
    SharedSort *sharedsort2;
    Snapshot snapshot;
} BTLeader;

static void btbuildCallback(Relation index, HeapTuple htup, Datum *values, const bool *isnull, bool tupleIsAlive,
    void *state);
static double _bt_spools_heapscan(Relation heap, Relation index, BTBuildState *buildstate,
    IndexInfo *indexInfo, void* meminfo);

static Page _bt_blnewpage(uint32 level);
static void _bt_slideleft(Page page);
static void _bt_sortaddtup(Page page, Size itemsize, IndexTuple itup, OffsetNumber itup_off);
static void _bt_load(BTWriteState* wstate, BTSpool* btspool, BTSpool* btspool2);

static void _bt_begin_parallel(BTBuildState *buildstate, bool isconcurrent, int request, void *meminfo);
static void _bt_end_parallel(BTLeader *btleader);
static Size _bt_parallel_estimate_shared(Snapshot snapshot);
static double _bt_parallel_heapscan(BTBuildState *buildstate, bool *brokenhotchain);
static void _bt_leader_participate_as_worker(BTBuildState *buildstate, void *meminfo);
static void _bt_parallel_scan_and_sort(BTSpool *btspool, BTSpool *btspool2, BTShared *btshared, SharedSort *sharedsort,
    SharedSort *sharedsort2, void *meminfo, int nWorkers);

/*
 *	btbuild() -- build a new btree index.
 */
Datum btbuild(PG_FUNCTION_ARGS)
{
    Relation heap = (Relation)PG_GETARG_POINTER(0);
    Relation index = (Relation)PG_GETARG_POINTER(1);
    IndexInfo* indexInfo = (IndexInfo*)PG_GETARG_POINTER(2);
    IndexBuildResult* result = NULL;
    double reltuples = 0;
    BTBuildState buildstate;

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
        ereport(ERROR,
            (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("index \"%s\" already contains data", RelationGetRelationName(index))));
    }

    double* allPartTuples = NULL;
    if (RelationIsGlobalIndex(index)) {
        // If building a unique index, put dead tuples in a second spool to keep
        // them out of the uniqueness check.
        if (indexInfo->ii_Unique) {
            buildstate.spool2 = _bt_spoolinit(heap, index, false, true, &indexInfo->ii_desc);
        }
        buildstate.spool = _bt_spoolinit(heap, index, indexInfo->ii_Unique, false, &indexInfo->ii_desc);
        allPartTuples = GlobalIndexBuildHeapScan(heap, index, indexInfo, btbuildCallback, (void*)&buildstate);
    } else {
        reltuples = _bt_spools_heapscan(heap, index, &buildstate, indexInfo, &indexInfo->ii_desc);
    }

    /*
     * Finish the build by (1) completing the sort of the spool file, (2)
     * inserting the sorted tuples into btree pages and (3) building the upper
     * levels. Finally, it may also be necessary to end use of parallelism.
     */
    _bt_leafbuild(buildstate.spool, buildstate.spool2);
    _bt_spooldestroy(buildstate.spool);
    if (buildstate.spool2) {
        _bt_spooldestroy(buildstate.spool2);
    }
    if (buildstate.btleader) {
        _bt_end_parallel(buildstate.btleader);
    }

#ifdef BTREE_BUILD_STATS
    if (u_sess->attr.attr_resource.log_btree_build_stats) {
        ShowUsage("BTREE BUILD STATS");
        ResetUsage();
    }
#endif /* BTREE_BUILD_STATS */

    // Return statistics
    result = (IndexBuildResult*)palloc(sizeof(IndexBuildResult));

    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;
    result->all_part_tuples = allPartTuples;

    PG_RETURN_POINTER(result);
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
static double _bt_spools_heapscan(Relation heap, Relation index, BTBuildState *buildstate, IndexInfo *indexInfo,
    void *meminfo)
{
    BTSpool *btspool = (BTSpool *)palloc0(sizeof(BTSpool));
    SortCoordinate coordinate = NULL;
    double reltuples = 0;
    UtilityDesc *desc = (UtilityDesc *)meminfo;
    int btKbytes1;
    int btKbytes2;

    /*
     * We size the sort area as maintenance_work_mem rather than work_mem to
     * speed index creation.  This should be OK since a single backend can't
     * run multiple index creations in parallel.  Note that creation of a
     * unique index actually requires two BTSpool objects.	We expect that the
     * second one (for dead tuples) won't get very full, so we give it only
     * work_mem.
     */
    if (desc->query_mem[0] > 0) {
        btKbytes1 = desc->query_mem[0];
        btKbytes2 = SIMPLE_THRESHOLD;
    } else {
        btKbytes1 = u_sess->attr.attr_memory.maintenance_work_mem;
        btKbytes2 = u_sess->attr.attr_memory.work_mem;
    }
    btspool->heap = heap;
    btspool->index = index;
    btspool->isunique = indexInfo->ii_Unique;

    /* Save as primary spool */
    buildstate->spool = btspool;

    /* Attempt to launch parallel worker scan when required */
    if (indexInfo->ii_ParallelWorkers > 0) {
        _bt_begin_parallel(buildstate, indexInfo->ii_Concurrent, indexInfo->ii_ParallelWorkers, meminfo);
    }

    /*
     * If parallel build requested and at least one worker process was
     * successfully launched, set up coordination state
     */
    if (buildstate->btleader) {
        coordinate = (SortCoordinate)palloc0(sizeof(SortCoordinateData));
        coordinate->isWorker = false;
        coordinate->nParticipants = buildstate->btleader->nparticipanttuplesorts;
        coordinate->sharedsort = buildstate->btleader->sharedsort;
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
        tuplesort_begin_index_btree(index, buildstate->isUnique, btKbytes1, coordinate, false, desc->query_mem[1]);

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
            coordinate2->sharedsort = buildstate->btleader->sharedsort2;
        }

        /*
         * We expect that the second one (for dead tuples) won't get very
         * full, so we give it only work_mem
         */
        buildstate->spool2->sortstate = tuplesort_begin_index_btree(index, false, btKbytes2, coordinate2, false, 0);
    }

    /* Fill spool using either serial or parallel heap scan */
    if (!buildstate->btleader) {
        reltuples = IndexBuildHeapScan(heap, index, indexInfo, true, btbuildCallback, (void *)buildstate, NULL);
    } else {
        reltuples = _bt_parallel_heapscan(buildstate, &indexInfo->ii_BrokenHotChain);
    }

    /* okay, all heap tuples are spooled */
    if (buildstate->spool2 && !buildstate->haveDead) {
        /* spool2 turns out to be unnecessary */
        _bt_spooldestroy(buildstate->spool2);
        buildstate->spool2 = NULL;
    }

    return reltuples;
}

/*
 * Interface routines
 *
 * create and initialize a spool structure
 */
BTSpool* _bt_spoolinit(Relation heap, Relation index, bool isunique, bool isdead, void* meminfo)
{
    BTSpool* btspool = (BTSpool*)palloc0(sizeof(BTSpool));
    int btKbytes;
    UtilityDesc* desc = (UtilityDesc*)meminfo;
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
void _bt_spooldestroy(BTSpool* btspool)
{
    tuplesort_end(btspool->sortstate);
    pfree(btspool);
    btspool = NULL;
}

/*
 * spool an index entry into the sort file.
 */
void _bt_spool(BTSpool* btspool, ItemPointer self, Datum* values, const bool* isnull)
{
    tuplesort_putindextuplevalues(btspool->sortstate, btspool->index, self, values, isnull);
}

/*
 * given a spool loaded by successive calls to _bt_spool,
 * create an entire btree.
 */
void _bt_leafbuild(BTSpool* btspool, BTSpool* btspool2)
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

    wstate.index = btspool->index;

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

/*
 * Per-tuple callback from IndexBuildHeapScan
 */
static void btbuildCallback(
    Relation index, HeapTuple htup, Datum* values, const bool* isnull, bool tupleIsAlive, void* state)
{
    BTBuildState* buildstate = (BTBuildState*)state;

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

/*
 * emit a completed btree page, and release the working storage.
 */
static void _bt_blwritepage(BTWriteState* wstate, Page page, BlockNumber blkno)
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
                smgrextend(wstate->index->rd_smgr,
                    MAIN_FORKNUM,
                    wstate->btws_pages_written++,
                    (char*)(char*)wstate->btws_zeropage,
                    true);
            }
        }
        ADIO_ELSE()
        {
            smgrextend(
                wstate->index->rd_smgr, MAIN_FORKNUM, wstate->btws_pages_written++, (char*)wstate->btws_zeropage, true);
        }
        ADIO_END();
    }

    char* bufToWrite = PageDataEncryptIfNeed(page);

    PageSetChecksumInplace((Page)bufToWrite, blkno);

    /*
     * Now write the page.	There's no need for smgr to schedule an fsync for
     * this write; we'll do it ourselves before ending the build.
     */
    if (blkno == wstate->btws_pages_written) {
        /* extending the file... */
        smgrextend(wstate->index->rd_smgr, MAIN_FORKNUM, blkno, (char*)bufToWrite, true);
        wstate->btws_pages_written++;
    } else {
        /* overwriting a block we zero-filled before */
        smgrwrite(wstate->index->rd_smgr, MAIN_FORKNUM, blkno, (char*)bufToWrite, true);
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
BTPageState* _bt_pagestate(BTWriteState* wstate, uint32 level)
{
    BTPageState* state = (BTPageState*)palloc0(sizeof(BTPageState));

    /* create initial page for level */
    state->btps_page = _bt_blnewpage(level);

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
        BTreeTupleSetNAtts(&trunctuple, 0);
        itup = &trunctuple;
        itemsize = sizeof(IndexTupleData);
    }

    if (PageAddItem(page, (Item)itup, itemsize, itup_off, false, false) == InvalidOffsetNumber)
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("failed to add item to the index page")));
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
void _bt_buildadd(BTWriteState* wstate, BTPageState* state, IndexTuple itup)
{
    Page npage;
    BlockNumber nblkno;
    OffsetNumber last_off;
    Size pgspc;
    Size itupsz;

    /*
     * This is a handy place to check for cancel interrupts during the btree
     * load phase of index creation.
     */
    CHECK_FOR_INTERRUPTS();

    npage = state->btps_page;
    nblkno = state->btps_blkno;
    last_off = state->btps_lastoff;

    pgspc = PageGetFreeSpace(npage);
    itupsz = IndexTupleDSize(*itup);
    itupsz = MAXALIGN(itupsz);
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
    if (itupsz > (Size)BTMaxItemSize(npage))
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("index row size %lu exceeds maximum %lu for index \"%s\"",
                    (unsigned long)itupsz,
                    (unsigned long)BTMaxItemSize(npage),
                    RelationGetRelationName(wstate->index)),
                errhint("Values larger than 1/3 of a buffer page cannot be indexed.\n"
                        "Consider a function index of an MD5 hash of the value, "
                        "or use full text indexing.")));

    /*
     * Check to see if page is "full".	It's definitely full if the item won't
     * fit.  Otherwise, compare to the target freespace derived from the
     * fillfactor.	However, we must put at least two items on each page, so
     * disregard fillfactor if we don't have that many.
     */
    if (pgspc < itupsz || (pgspc < state->btps_full && last_off > P_FIRSTKEY)) {
        /*
         * Finish off the page and write it out.
         */
        Page opage = npage;
        BlockNumber oblkno = nblkno;
        ItemId ii;
        ItemId hii;
        IndexTuple oitup;
        IndexTuple keytup;
        BTPageOpaqueInternal opageop = (BTPageOpaqueInternal) PageGetSpecialPointer(opage);

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
        int indnatts = IndexRelationGetNumberOfAttributes(wstate->index);
        int indnkeyatts = IndexRelationGetNumberOfKeyAttributes(wstate->index);

        if (indnkeyatts != indnatts && P_ISLEAF(opageop)) {
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
            keytup = _bt_nonkey_truncate(wstate->index, oitup);
            /* delete "wrong" high key, insert keytup as P_HIKEY. */
            PageIndexTupleDelete(opage, P_HIKEY);
            _bt_sortaddtup(opage, IndexTupleSize(keytup), keytup, P_HIKEY);
        }
        /*
         * Link the old page into its parent, using its minimum key. If we
         * don't have a parent, we have to create one; this adds a new btree
         * level.
         */
        if (state->btps_next == NULL)
            state->btps_next = _bt_pagestate(wstate, state->btps_level + 1);

        Assert(state->btps_minkey != NULL);
        Assert(BTreeTupleGetNAtts(state->btps_minkey, wstate->index) ==
               IndexRelationGetNumberOfKeyAttributes(wstate->index) ||
               P_LEFTMOST(opageop));
        Assert(BTreeTupleGetNAtts(state->btps_minkey, wstate->index) == 0 ||
               !P_LEFTMOST(opageop));

        BTreeInnerTupleSetDownLink(state->btps_minkey, oblkno);
        _bt_buildadd(wstate, state->btps_next, state->btps_minkey);
        pfree(state->btps_minkey);
        state->btps_minkey = NULL;

        /*
         * Save a copy of the minimum key for the new page.  We have to copy
         * it off the old page, not the new one, in case we are not at leaf
         * level.  Despite oitup is already initialized, it's important to get
         * high key from the page, since we could have replaced it with
         * truncated copy.	See comment above.
         */
        oitup = (IndexTuple) PageGetItem(opage, PageGetItemId(opage, P_HIKEY));
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
        BTreeTupleSetNAtts(state->btps_minkey, 0);
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
void _bt_uppershutdown(BTWriteState* wstate, BTPageState* state)
{
    BTPageState* s = NULL;
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
            Assert(BTreeTupleGetNAtts(s->btps_minkey, wstate->index) ==
                   IndexRelationGetNumberOfKeyAttributes(wstate->index) ||
                   P_LEFTMOST(opaque));
            Assert(BTreeTupleGetNAtts(s->btps_minkey, wstate->index) == 0 || !P_LEFTMOST(opaque));
            BTreeInnerTupleSetDownLink(s->btps_minkey, blkno);
            _bt_buildadd(wstate, s->btps_next, s->btps_minkey);
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
    _bt_initmetapage(metapage, rootblkno, rootlevel);
    _bt_blwritepage(wstate, metapage, BTREE_METAPAGE);
}

/*
 * Read tuples in correct sort order from tuplesort, and load them into
 * btree leaves.
 */
static void _bt_load(BTWriteState* wstate, BTSpool* btspool, BTSpool* btspool2)
{
    BTPageState* state = NULL;
    bool merge = (btspool2 != NULL);
    IndexTuple itup = NULL;
    IndexTuple itup2 = NULL;
    bool should_free = false;
    bool should_free2 = false;
    bool load1 = false;
    TupleDesc tupdes = RelationGetDescr(wstate->index);
    int keysz = IndexRelationGetNumberOfKeyAttributes(wstate->index);
    ScanKey indexScanKey = NULL;

    if (merge) {
        /*
         * Another BTSpool for dead tuples exists. Now we have to merge
         * btspool and btspool2.
         *
         * the preparation of merge 
         */
        itup = tuplesort_getindextuple(btspool->sortstate, true, &should_free);
        itup2 = tuplesort_getindextuple(btspool2->sortstate, true, &should_free2);
        indexScanKey = _bt_mkscankey_nodata(wstate->index);

        for (;;) {
            if (itup == NULL && itup2 == NULL) {
                break;
            }
            load1 = _index_tuple_compare(tupdes, indexScanKey, keysz, itup, itup2);

            /* When we see first tuple, create first index page */
            if (state == NULL)
                state = _bt_pagestate(wstate, 0);

            if (load1) {
                _bt_buildadd(wstate, state, itup);
                if (should_free) {
                    pfree(itup);
                    itup = NULL;
                }
                itup = tuplesort_getindextuple(btspool->sortstate, true, &should_free);
            } else {
                _bt_buildadd(wstate, state, itup2);
                if (should_free2) {
                    pfree(itup2);
                    itup2 = NULL;
                }
                itup2 = tuplesort_getindextuple(btspool2->sortstate, true, &should_free2);
            }
        }
        _bt_freeskey(indexScanKey);
    } else {
        /* merge is unnecessary */
        while ((itup = tuplesort_getindextuple(btspool->sortstate, true, &should_free)) != NULL) {
            /* When we see first tuple, create first index page */
            if (state == NULL)
                state = _bt_pagestate(wstate, 0);

            _bt_buildadd(wstate, state, itup);
            if (should_free) {
                pfree(itup);
                itup = NULL;
            }
        }
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

/*
 * if itup <= itup2, return true;
 * if itup > itup2, return false.
 * itup == NULL && itup2 == NULL take care by caller
 */
bool _index_tuple_compare(TupleDesc tupdes, ScanKey indexScanKey, int keysz, IndexTuple itup, IndexTuple itup2)
{
    /* defaultly load itup, including the itup != NULL && itup2 == NULL case. */
    bool result = true;
    int i;

    if (itup == NULL && itup2 != NULL) {
        return false;
    }
    if (itup != NULL && itup2 == NULL) {
        return true;
    }
    Assert(itup != NULL && itup2 != NULL);
    for (i = 1; i <= keysz; i++) {
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
            result = false;
            break;
        } else if (compare < 0)
            break;
    }
    return result;
}

List* insert_ordered_index(List* list, TupleDesc tupdes, ScanKey indexScanKey, int keysz, IndexTuple itup,
    BlockNumber heapModifiedOffset, IndexScanDesc srcIdxRelScan)
{
    ListCell* prev = NULL;
    IndexTuple itup2 = NULL;
    BTOrderedIndexListElement* ele = NULL;

    // ignore null index tuple, but user should assure the validity of indexseq
    if (itup == NULL)
        return list;

    ele = (BTOrderedIndexListElement*)palloc0fast(sizeof(BTOrderedIndexListElement));
    if (list == NIL) {
        ele->itup = itup;
        ele->heapModifiedOffset = heapModifiedOffset;
        ele->indexScanDesc = srcIdxRelScan;
        return lcons(ele, list);
    }
    itup2 = ((BTOrderedIndexListElement*)linitial(list))->itup;
    if (itup == NULL && itup2 == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("index compare error, both are NULL")));
    }
    /* Does the datum belong at the front? */
    if (_index_tuple_compare(tupdes, indexScanKey, keysz, itup, itup2)) {
        ele->itup = itup;
        ele->heapModifiedOffset = heapModifiedOffset;
        ele->indexScanDesc = srcIdxRelScan;
        return lcons(ele, list);
    }
    /* No, so find the entry it belongs after */
    prev = list_head(list);
    for (;;) {
        ListCell* curr = lnext(prev);
        if (curr == NULL) {
            break;
        }
        itup2 = ((BTOrderedIndexListElement*)lfirst(curr))->itup;
        if (itup == NULL && itup2 == NULL) {
            ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("index compare error, both are NULL")));
        }
        if (_index_tuple_compare(tupdes, indexScanKey, keysz, itup, itup2)) {
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
 * Create parallel context, and launch workers for leader.
 *
 * buildstate argument should be initialized (with the exception of the
 * tuplesort state in spools, which may later be created based on shared
 * state initially set up here).
 *
 * isconcurrent indicates if operation is CREATE INDEX CONCURRENTLY.
 *
 * request is the target number of parallel worker processes to launch.
 *
 * Sets buildstate's BTLeader, which caller must use to shut down parallel
 * mode by passing it to _bt_end_parallel() at the very end of its index
 * build.  If not even a single worker process can be launched, this is
 * never set, and caller should proceed with a serial index build.
 */
static void _bt_begin_parallel(BTBuildState *buildstate, bool isconcurrent, int request, void *meminfo)
{
    int scantuplesortstates;
    Snapshot snapshot = NULL;
    Size pscanLen = offsetof(ParallelHeapScanDescData, phs_snapshot_data);
    SharedSort *sharedsort2 = NULL;
    BTSpool *btspool = buildstate->spool;
    BTLeader *btleader = (BTLeader *)palloc0(sizeof(BTLeader));
    bool leaderparticipates = true;

#ifdef DISABLE_LEADER_PARTICIPATION
    leaderparticipates = false;
#endif

    /*
     * Enter parallel mode, and create context for parallel build of btree
     * index
     */
    EnterParallelMode();
    Assert(request > 0);
    ParallelContext *pcxt = CreateParallelContext("postgres", "_bt_parallel_build_main", request);
    scantuplesortstates = leaderparticipates ? (request + 1) : request;

    /*
     * Prepare for scan of the base relation.  In a normal index build, we use
     * SnapshotAny because we must retrieve all tuples and do our own time
     * qual checks (because we have to index RECENTLY_DEAD tuples).  In a
     * concurrent build, we take a regular MVCC snapshot and index whatever's
     * live according to that.
     */
    if (!isconcurrent) {
        snapshot = SnapshotAny;
    } else {
        snapshot = RegisterSnapshot(GetTransactionSnapshot());
        pscanLen += EstimateSnapshotSpace(snapshot);
    }

    /*
     * Estimate size for at least two keys -- our own
     * PARALLEL_KEY_BTREE_SHARED workspace, and PARALLEL_KEY_TUPLESORT
     * tuplesort workspace
     */
    Size estbtshared = _bt_parallel_estimate_shared(snapshot);
    Size estsort = tuplesort_estimate_shared(scantuplesortstates);

    /* Everyone's had a chance to ask for space, so now create the DSM */
    InitializeParallelDSM(pcxt, snapshot);

    /* If no worker was available, back out (do serial build) */
    if (pcxt->nworkers == 0) {
        if (IsMVCCSnapshot(snapshot)) {
            UnregisterSnapshot(snapshot);
        }
        DestroyParallelContext(pcxt);
        ExitParallelMode();
        return;
    }

    knl_u_parallel_context *cxt = (knl_u_parallel_context *)pcxt->seg;
    MemoryContext oldcontext = MemoryContextSwitchTo(cxt->memCtx);

    /* Store shared build state, for which we reserved space */
    BTShared *btshared = (BTShared *)palloc(estbtshared);
    /* Initialize immutable state */
    btshared->heaprelid = RelationGetRelid(btspool->heap);
    btshared->indexrelid = RelationGetRelid(btspool->index);
    btshared->isunique = btspool->isunique;
    btshared->isconcurrent = isconcurrent;
    btshared->scantuplesortstates = scantuplesortstates;
    (void)pthread_cond_init(&btshared->workersdonecv, NULL);
    (void)pthread_mutex_init(&btshared->mtx, NULL);
    SpinLockInit(&btshared->mutex);
    /* Initialize mutable state */
    btshared->nparticipantsdone = 0;
    btshared->reltuples = 0.0;
    btshared->havedead = false;
    btshared->indtuples = 0.0;
    btshared->brokenhotchain = false;
    heap_parallelscan_initialize(&btshared->heapdesc, pscanLen, btspool->heap, snapshot);

    /*
     * Store shared tuplesort-private state, for which we reserved space.
     * Then, initialize opaque state using tuplesort routine.
     */
    SharedSort *sharedsort = (SharedSort *)palloc(estsort);
    tuplesort_initialize_shared(sharedsort, scantuplesortstates, pcxt->seg);

    /* Unique case requires a second spool, and associated shared state */
    if (btspool->isunique) {
        /*
         * Store additional shared tuplesort-private state, for which we
         * reserved space.  Then, initialize opaque state using tuplesort
         * routine.
         */
        sharedsort2 = (SharedSort *)palloc(estsort);
        tuplesort_initialize_shared(sharedsort2, scantuplesortstates, pcxt->seg);
    }

    /* Launch workers, saving status for leader/caller */
    LaunchParallelWorkers(pcxt);
    btleader->pcxt = pcxt;
    btleader->nparticipanttuplesorts = pcxt->nworkers_launched;
    if (leaderparticipates) {
        btleader->nparticipanttuplesorts++;
    }
    btleader->btshared = btshared;
    btleader->sharedsort = sharedsort;
    btleader->sharedsort2 = sharedsort2;
    btleader->snapshot = snapshot;

    UtilityDesc *sharedMemInfo = (UtilityDesc *)palloc(sizeof(UtilityDesc));
    int rc = memcpy_s(sharedMemInfo, sizeof(UtilityDesc), meminfo, sizeof(UtilityDesc));
    securec_check(rc, "", "");

    cxt->pwCtx->btreeInfo.btShared = btshared;
    cxt->pwCtx->btreeInfo.sharedSort = sharedsort;
    cxt->pwCtx->btreeInfo.sharedSort2 = sharedsort2;
    cxt->pwCtx->btreeInfo.meminfo = sharedMemInfo;
    Size querylen = strlen(t_thrd.postgres_cxt.debug_query_string);
    cxt->pwCtx->btreeInfo.queryText = (char *)palloc(querylen + 1);
    rc = memcpy_s(cxt->pwCtx->btreeInfo.queryText, querylen + 1, t_thrd.postgres_cxt.debug_query_string, querylen + 1);
    securec_check(rc, "", "");

    (void)MemoryContextSwitchTo(oldcontext);

    /* If no workers were successfully launched, back out (do serial build) */
    if (pcxt->nworkers_launched == 0) {
        _bt_end_parallel(btleader);
        return;
    }

    t_thrd.subrole = BACKGROUND_LEADER;

    /* Save leader state now that it's clear build will be parallel */
    buildstate->btleader = btleader;

    /* Join heap scan ourselves */
    if (leaderparticipates) {
        _bt_leader_participate_as_worker(buildstate, meminfo);
    }

    /*
     * Caller needs to wait for all launched workers when we return.  Make
     * sure that the failure-to-start case will not hang forever.
     */
    WaitForParallelWorkersToAttach(pcxt);
}

/*
 * Shut down workers, destroy parallel context, and end parallel mode.
 */
static void _bt_end_parallel(BTLeader *btleader)
{
    /* Shutdown worker processes */
    WaitForParallelWorkersToFinish(btleader->pcxt);
    /* Free last reference to MVCC snapshot, if one was used */
    if (IsMVCCSnapshot(btleader->snapshot)) {
        UnregisterSnapshot(btleader->snapshot);
    }
    DestroyParallelContext(btleader->pcxt);
    ExitParallelMode();
}

/*
 * Returns size of shared memory required to store state for a parallel
 * btree index build based on the snapshot its parallel scan will use.
 */
static Size _bt_parallel_estimate_shared(Snapshot snapshot)
{
    if (!IsMVCCSnapshot(snapshot)) {
        Assert(snapshot == SnapshotAny);
        return sizeof(BTShared);
    }

    return add_size(offsetof(BTShared, heapdesc) + offsetof(ParallelHeapScanDescData, phs_snapshot_data),
        EstimateSnapshotSpace(snapshot));
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
static double _bt_parallel_heapscan(BTBuildState *buildstate, bool *brokenhotchain)
{
    BTShared *btshared = buildstate->btleader->btshared;
    int nparticipanttuplesorts = buildstate->btleader->nparticipanttuplesorts;
    double reltuples;
    WLMContextLock btLock(&btshared->mtx);

    for (;;) {
        SpinLockAcquire(&btshared->mutex);
        if (btshared->nparticipantsdone == nparticipanttuplesorts) {
            buildstate->haveDead = btshared->havedead;
            buildstate->indtuples = btshared->indtuples;
            *brokenhotchain = btshared->brokenhotchain;
            reltuples = btshared->reltuples;
            SpinLockRelease(&btshared->mutex);
            break;
        }
        SpinLockRelease(&btshared->mutex);

        /*
         * Use pthread_cond_timedwait here in case of worker exit in error cases, and call
         * CHECK_FOR_INTERRUPTS to handle the error msg from worker.
         */
        btLock.Lock();
        btLock.ConditionTimedWait(&btshared->workersdonecv, BT_SCAN_WAIT_TIME);
        btLock.UnLock();
        CHECK_FOR_INTERRUPTS();
    }

    return reltuples;
}


/*
 * Within leader, participate as a parallel worker.
 */
static void _bt_leader_participate_as_worker(BTBuildState *buildstate, void *meminfo)
{
    BTLeader *btleader = buildstate->btleader;
    BTSpool *leaderworker2 = NULL;

    /* Allocate memory and initialize private spool */
    BTSpool *leaderworker = (BTSpool *)palloc0(sizeof(BTSpool));
    leaderworker->heap = buildstate->spool->heap;
    leaderworker->index = buildstate->spool->index;
    leaderworker->isunique = buildstate->spool->isunique;

    /* Initialize second spool, if required */
    if (btleader->btshared->isunique) {
        /* Allocate memory for worker's own private secondary spool */
        leaderworker2 = (BTSpool *)palloc0(sizeof(BTSpool));

        /* Initialize worker's own secondary spool */
        leaderworker2->heap = leaderworker->heap;
        leaderworker2->index = leaderworker->index;
        leaderworker2->isunique = false;
    }

    /* Perform work common to all participants */
    _bt_parallel_scan_and_sort(leaderworker, leaderworker2, btleader->btshared, btleader->sharedsort,
        btleader->sharedsort2, meminfo, btleader->nparticipanttuplesorts);

#ifdef BTREE_BUILD_STATS
    if (u_sess->attr.attr_resource.log_btree_build_stats) {
        ShowUsage("BTREE BUILD (Leader Partial Spool) STATISTICS");
        ResetUsage();
    }
#endif /* BTREE_BUILD_STATS */
}

/*
 * Perform work within a launched parallel process.
 */
void _bt_parallel_build_main(void *seg)
{
    BTSpool *btspool2 = NULL;
    SharedSort *sharedsort2 = NULL;
    LOCKMODE heapLockmode;
    LOCKMODE indexLockmode;

#ifdef BTREE_BUILD_STATS
    if (u_sess->attr.attr_resource.log_btree_build_stats) {
        ResetUsage();
    }
#endif /* BTREE_BUILD_STATS */

    knl_u_parallel_context *cxt = (knl_u_parallel_context *)seg;

    /* Set debug_query_string for individual workers first */
    t_thrd.postgres_cxt.debug_query_string = cxt->pwCtx->btreeInfo.queryText;

    /* Report the query string from leader */
    pgstat_report_activity(STATE_RUNNING, t_thrd.postgres_cxt.debug_query_string);

    /* Look up shared state */
    BTShared *btshared = cxt->pwCtx->btreeInfo.btShared;

    /* Open relations using lock modes known to be obtained by index.c */
    if (!btshared->isconcurrent) {
        heapLockmode = ShareLock;
        indexLockmode = AccessExclusiveLock;
    } else {
        heapLockmode = ShareUpdateExclusiveLock;
        indexLockmode = RowExclusiveLock;
    }

    /* Open relations within worker */
    Relation heapRel = heap_open(btshared->heaprelid, heapLockmode);
    Relation indexRel = index_open(btshared->indexrelid, indexLockmode);

    /* Initialize worker's own spool */
    BTSpool *btspool = (BTSpool *)palloc0(sizeof(BTSpool));
    btspool->heap = heapRel;
    btspool->index = indexRel;
    btspool->isunique = btshared->isunique;

    /* Look up shared state private to tuplesort.c */
    SharedSort *sharedsort = cxt->pwCtx->btreeInfo.sharedSort;
    if (btshared->isunique) {
        /* Allocate memory for worker's own private secondary spool */
        btspool2 = (BTSpool *)palloc0(sizeof(BTSpool));

        /* Initialize worker's own secondary spool */
        btspool2->heap = btspool->heap;
        btspool2->index = btspool->index;
        btspool2->isunique = false;
        /* Look up shared state private to tuplesort.c */
        sharedsort2 = cxt->pwCtx->btreeInfo.sharedSort2;
    }

    /* Perform sorting of spool, and possibly a spool2 */
    _bt_parallel_scan_and_sort(btspool, btspool2, btshared, sharedsort, sharedsort2, cxt->pwCtx->btreeInfo.meminfo,
        btshared->scantuplesortstates);

#ifdef BTREE_BUILD_STATS
    if (u_sess->attr.attr_resource.log_btree_build_stats) {
        ShowUsage("BTREE BUILD (Worker Partial Spool) STATISTICS");
        ResetUsage();
    }
#endif /* BTREE_BUILD_STATS */

    index_close(indexRel, indexLockmode);
    heap_close(heapRel, heapLockmode);
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
static void _bt_parallel_scan_and_sort(BTSpool *btspool, BTSpool *btspool2, BTShared *btshared, SharedSort *sharedsort,
    SharedSort *sharedsort2, void *meminfo, int nWorkers)
{
    BTBuildState buildstate;
    double reltuples;
    int btKbytes1;
    int btKbytes2;
    UtilityDesc *desc = (UtilityDesc *)meminfo;

    /* Initialize local tuplesort coordination state */
    SortCoordinate coordinate = (SortCoordinate)palloc0(sizeof(SortCoordinateData));
    coordinate->isWorker = true;
    coordinate->nParticipants = -1;
    coordinate->sharedsort = sharedsort;

    /*
     * We size the sort area as maintenance_work_mem rather than work_mem to
     * speed index creation.  This should be OK since a single backend can't
     * run multiple index creations in parallel.  Note that creation of a
     * unique index actually requires two BTSpool objects.	We expect that the
     * second one (for dead tuples) won't get very full, so we give it only
     * work_mem.
     */
    if (desc->query_mem[0] > 0) {
        btKbytes1 = desc->query_mem[0] / nWorkers;
        btKbytes2 = SIMPLE_THRESHOLD / nWorkers;
    } else {
        btKbytes1 = u_sess->attr.attr_memory.maintenance_work_mem / nWorkers;
        btKbytes2 = u_sess->attr.attr_memory.work_mem / nWorkers;
    }

    /* Begin "partial" tuplesort */
    btspool->sortstate = tuplesort_begin_index_btree(btspool->index, btspool->isunique, btKbytes1, coordinate, false,
        desc->query_mem[1] / nWorkers);

    /*
     * Just as with serial case, there may be a second spool.  If so, a
     * second, dedicated spool2 partial tuplesort is required.
     */
    if (btspool2 != NULL) {
        /*
         * We expect that the second one (for dead tuples) won't get very
         * full, so we give it only work_mem (unless sortmem is less for
         * worker).  Worker processes are generally permitted to allocate
         * work_mem independently.
         */
        SortCoordinate coordinate2 = (SortCoordinate)palloc0(sizeof(SortCoordinateData));
        coordinate2->isWorker = true;
        coordinate2->nParticipants = -1;
        coordinate2->sharedsort = sharedsort2;
        btspool2->sortstate = tuplesort_begin_index_btree(btspool->index, false,
            Min(btKbytes2, u_sess->attr.attr_memory.work_mem / nWorkers), coordinate2, false, 0);
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
    IndexInfo *indexInfo = BuildIndexInfo(btspool->index);
    indexInfo->ii_Concurrent = btshared->isconcurrent;
    HeapScanDesc scan = heap_beginscan_parallel(btspool->heap, &btshared->heapdesc);
    reltuples =
        IndexBuildHeapScan(btspool->heap, btspool->index, indexInfo, true, btbuildCallback, (void *)&buildstate, scan);

    /*
     * Execute this worker's part of the sort.
     *
     * Unlike leader and serial cases, we cannot avoid calling
     * tuplesort_performsort() for spool2 if it ends up containing no dead
     * tuples (this is disallowed for workers by tuplesort).
     */
    tuplesort_performsort(btspool->sortstate);
    if (btspool2 != NULL) {
        tuplesort_performsort(btspool2->sortstate);
    }

    /*
     * Done.  Record ambuild statistics, and whether we encountered a broken
     * HOT chain.
     */
    SpinLockAcquire(&btshared->mutex);
    btshared->nparticipantsdone++;
    btshared->reltuples += reltuples;
    if (buildstate.haveDead) {
        btshared->havedead = true;
    }
    btshared->indtuples += buildstate.indtuples;
    if (indexInfo->ii_BrokenHotChain) {
        btshared->brokenhotchain = true;
    }
    SpinLockRelease(&btshared->mutex);

    /* Notify leader */
    WLMContextLock btLock(&btshared->mtx);
    btLock.Lock();
    btLock.ConditionWakeUp(&btshared->workersdonecv);
    btLock.UnLock();

    /* We can end tuplesorts immediately */
    tuplesort_end(btspool->sortstate);
    if (btspool2 != NULL) {
        tuplesort_end(btspool2->sortstate);
    }
}

