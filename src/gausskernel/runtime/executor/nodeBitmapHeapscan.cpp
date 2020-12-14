/* -------------------------------------------------------------------------
 *
 * nodeBitmapHeapscan.cpp
 *	  Routines to support bitmapped scans of relations
 *
 * NOTE: it is critical that this plan type only be used with MVCC-compliant
 * snapshots (ie, regular snapshots, not SnapshotNow or one of the other
 * special snapshots).	The reason is that since index and heap scans are
 * decoupled, there can be no assurance that the index tuple prompting a
 * visit to a particular heap TID still exists when the visit is made.
 * Therefore the tuple might not exist anymore either (which is OK because
 * heap_fetch will cope) --- but worse, the tuple slot could have been
 * re-used for a newer tuple.  With an MVCC snapshot the newer tuple is
 * certain to fail the time qual and so it will not be mistakenly returned.
 * With SnapshotNow we might return a tuple that doesn't meet the required
 * index qual conditions.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeBitmapHeapscan.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecBitmapHeapScan			scans a relation using bitmap info
 *		ExecBitmapHeapNext			workhorse for above
 *		ExecInitBitmapHeapScan		creates and initializes state info.
 *		ExecReScanBitmapHeapScan	prepares to rescan the plan.
 *		ExecEndBitmapHeapScan		releases all storage.
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "executor/execdebug.h"
#include "executor/nodeBitmapHeapscan.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/access_gstrace.h"

#define WAIT_BITMAP_INIT_TIMEOUT 1 // 1s timeout for pthread_cond_timedwait

static TupleTableSlot* BitmapHbucketTblNext(BitmapHeapScanState* node);
static TupleTableSlot* BitmapHeapTblNext(BitmapHeapScanState* node);
static inline void BitmapDoneInitializingSharedState(ParallelBitmapHeapState *pstate);
#ifdef USE_PREFETCH
static inline void BitmapAdjustPrefetchIterator(BitmapHeapScanState *node, const TBMIterateResult *tbmres,
    TBMIterator *prefetch_iterator, TBMSharedIterator *shared_prefetch_it);
static inline void BitmapAdjustPrefetchTarget(BitmapHeapScanState *node);
#endif
static void bitgetpage(HeapScanDesc scan, TBMIterateResult* tbmres);
static void ExecInitPartitionForBitmapHeapScan(BitmapHeapScanState* scanstate, EState* estate);
static void ExecInitNextPartitionForBitmapHeapScan(BitmapHeapScanState* node);
static void BitmapHeapPrefetchNext(BitmapHeapScanState* node, HeapScanDesc scan, const TIDBitmap* tbm,
    TBMIterator** prefetch_iterator, TBMSharedIterator** shared_prefetch_it);
static bool BitmapShouldInitializeSharedState(ParallelBitmapHeapState *pstate);

/* This struct is used for partition switch while prefetch pages */
typedef struct PrefetchNode {
    BlockNumber blockNum;
    Oid partOid;
} PrefetchNode;

void BitmapHeapFree(BitmapHeapScanState* node)
{
    if (node->tbmiterator != NULL) {
        tbm_end_iterate(node->tbmiterator);
        node->tbmiterator = NULL;
    }
    if (node->prefetch_iterator != NULL) {
        tbm_end_iterate(node->prefetch_iterator);
        node->prefetch_iterator = NULL;
    }
    if (node->shared_tbmiterator != NULL) {
        tbm_end_shared_iterate(node->shared_tbmiterator);
        node->shared_tbmiterator = NULL;
    }
    if (node->shared_prefetch_iterator != NULL) {
        tbm_end_shared_iterate(node->shared_prefetch_iterator);
        node->shared_prefetch_iterator = NULL;
    }
    if (node->tbm != NULL) {
        tbm_free(node->tbm);
        node->tbm = NULL;
    }
    node->tbmres = NULL;
    node->initialized = false;
}
static TupleTableSlot* BitmapHbucketTblNext(BitmapHeapScanState* node)
{
    Assert(node->ss.ss_currentScanDesc != NULL);
    Assert(node->ss.ss_currentScanDesc->type == T_ScanDesc_HBucket);
    HBktTblScanDesc hpScan = (HBktTblScanDesc)node->ss.ss_currentScanDesc;
    TupleTableSlot* slot = NULL;
    while (true) {
        node->ss.ps.hbktScanSlot.currSlot = hpScan->curr_slot;
        node->ss.ps.lefttree->hbktScanSlot.currSlot = hpScan->curr_slot;
        slot = BitmapHeapTblNext(node);
        if (!TupIsNull(slot)) {
            return slot;
        }
        if (!hbkt_bitmapheap_scan_nextbucket(hpScan)) {
            return NULL;
        }
        BitmapHeapFree(node);
    }
}

#ifdef USE_PREFETCH
/*
 * 	BitmapAdjustPrefetchIterator - Adjust the prefetch iterator
 */
static inline void BitmapAdjustPrefetchIterator(BitmapHeapScanState *node, const TBMIterateResult *tbmres,
    TBMIterator *prefetch_iterator, TBMSharedIterator *shared_prefetch_it)
{
    ParallelBitmapHeapState *pstate = node->pstate;

    if (pstate == NULL) {
        if (node->prefetch_pages > 0) {
            /* The main iterator has closed the distance by one page */
            node->prefetch_pages--;
        } else if (prefetch_iterator != NULL) {
            /* Do not let the prefetch iterator get behind the main one */
            TBMIterateResult *tbmpre = tbm_iterate(prefetch_iterator);

            if (tbmpre == NULL || tbmpre->blockno != tbmres->blockno) {
                ereport(ERROR,
                    (errcode(ERRCODE_DATA_EXCEPTION),
                        errmodule(MOD_EXECUTOR),
                        errmsg("prefetch and main iterators are out of sync for BitmapHeapScan.")));
            }
        }
        return;
    }

    if (u_sess->storage_cxt.target_prefetch_pages > 0) {
        (void)pthread_mutex_lock(&pstate->cv_mtx);
        if (pstate->prefetch_pages > 0) {
            pstate->prefetch_pages--;
            (void)pthread_mutex_unlock(&pstate->cv_mtx);
        } else {
            /* Release the mutex before iterating */
            (void)pthread_mutex_unlock(&pstate->cv_mtx);

            /*
             * In case of shared mode, we can not ensure that the current
             * blockno of the main iterator and that of the prefetch iterator
             * are same.  It's possible that whatever blockno we are
             * prefetching will be processed by another process.  Therefore,
             * we don't validate the blockno here as we do in non-parallel
             * case.
             */
            if (shared_prefetch_it != NULL) {
                (void)tbm_shared_iterate(shared_prefetch_it);
            }
        }
    }
}

/*
 * BitmapAdjustPrefetchTarget - Adjust the prefetch target
 *
 * Increase prefetch target if it's not yet at the max.  Note that
 * we will increase it to zero after fetching the very first
 * page/tuple, then to one after the second tuple is fetched, then
 * it doubles as later pages are fetched.
 */
static inline void BitmapAdjustPrefetchTarget(BitmapHeapScanState *node)
{
    ParallelBitmapHeapState *pstate = node->pstate;

    if (pstate == NULL) {
        if (node->prefetch_target >= u_sess->storage_cxt.target_prefetch_pages) {
            /* don't increase any further */
        } else if (node->prefetch_target >= u_sess->storage_cxt.target_prefetch_pages / 2) {
            node->prefetch_target = u_sess->storage_cxt.target_prefetch_pages;
        } else if (node->prefetch_target > 0) {
            node->prefetch_target *= 2;
        } else {
            node->prefetch_target++;
        }
        return;
    }

    /* Do an unlocked check first to save spinlock acquisitions. */
    if (pstate->prefetch_target < u_sess->storage_cxt.target_prefetch_pages) {
        (void)pthread_mutex_lock(&pstate->cv_mtx);
        if (pstate->prefetch_target >= u_sess->storage_cxt.target_prefetch_pages) {
            /* don't increase any further */
        } else if (pstate->prefetch_target >= u_sess->storage_cxt.target_prefetch_pages / 2) {
            pstate->prefetch_target = u_sess->storage_cxt.target_prefetch_pages;
        } else if (pstate->prefetch_target > 0) {
            pstate->prefetch_target *= 2;
        } else {
            pstate->prefetch_target++;
        }
        (void)pthread_mutex_unlock(&pstate->cv_mtx);
    }
}
#endif /* USE_PREFETCH */

/* ----------------------------------------------------------------
 * 		BitmapHeapNext
 *
 * 		Retrieve next tuple from the BitmapHeapScan node's currentRelation
 * ----------------------------------------------------------------
 */
static TupleTableSlot* BitmapHeapTblNext(BitmapHeapScanState* node)
{
    ExprContext* econtext = NULL;
    HeapScanDesc scan = NULL;
    TIDBitmap* tbm = NULL;
    TBMIterator* tbmiterator = NULL;
    TBMSharedIterator *shared_tbmiterator = NULL;
    TBMIterateResult* tbmres = NULL;
    ParallelBitmapHeapState *pstate = node->pstate;

#ifdef USE_PREFETCH
    TBMIterator* prefetch_iterator = NULL;
    TBMSharedIterator* shared_prefetch_it = NULL;
#endif
    OffsetNumber targoffset;
    TupleTableSlot* slot = NULL;

    /*
     * extract necessary information from index scan node
     */
    econtext = node->ss.ps.ps_ExprContext;
    slot = node->ss.ss_ScanTupleSlot;
    scan = GetHeapScanDesc(node->ss.ss_currentScanDesc);
    tbm = node->tbm;
    if (pstate == NULL) {
        tbmiterator = node->tbmiterator;
    } else {
        shared_tbmiterator = node->shared_tbmiterator;
    }
    tbmres = node->tbmres;
#ifdef USE_PREFETCH
    prefetch_iterator = node->prefetch_iterator;
    shared_prefetch_it = node->shared_prefetch_iterator;
#endif

    /*
     * If we haven't yet performed the underlying index scan, do it, and begin
     * the iteration over the bitmap.
     *
     * For prefetching, we use *two* iterators, one for the pages we are
     * actually scanning and another that runs ahead of the first for
     * prefetching.  node->prefetch_pages tracks exactly how many pages ahead
     * the prefetch iterator is.  Also, node->prefetch_target tracks the
     * desired prefetch distance, which starts small and increases up to the
     * GUC-controlled maximum, target_prefetch_pages.  This is to avoid doing
     * a lot of prefetching in a scan that stops after a few tuples because of
     * a LIMIT.
     */
    if (!node->initialized) {
        if (pstate == NULL) {
            tbm = (TIDBitmap*)MultiExecProcNode(outerPlanState(node));

            if (tbm == NULL || !IsA(tbm, TIDBitmap)) {
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_EXECUTOR),
                        errmsg("unrecognized result from subplan for BitmapHeapScan.")));
            }

            node->tbm = tbm;
            node->tbmiterator = tbmiterator = tbm_begin_iterate(tbm);
            node->tbmres = tbmres = NULL;

#ifdef USE_PREFETCH
            if (u_sess->storage_cxt.target_prefetch_pages > 0) {
                node->prefetch_iterator = prefetch_iterator = tbm_begin_iterate(tbm);
                node->prefetch_pages = 0;
                node->prefetch_target = -1;
            }
#endif
        } else {
            /*
             * The leader will immediately come out of the function, but
             * others will be blocked until leader populates the TBM and wakes
             * them up.
             */
            if (BitmapShouldInitializeSharedState(pstate)) {
                tbm = (TIDBitmap *)MultiExecProcNode(outerPlanState(node));
                if (tbm == NULL || !IsA(tbm, TIDBitmap)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmodule(MOD_EXECUTOR),
                            errmsg("unrecognized result from subplan for BitmapHeapScan.")));
                }

                node->tbm = tbm;

                /*
                 * Prepare to iterate over the TBM. This will return the
                 * dsa_pointer of the iterator state which will be used by
                 * multiple processes to iterate jointly.
                 */
                pstate->tbmiterator = tbm_prepare_shared_iterate(tbm);
#ifdef USE_PREFETCH
                if (u_sess->storage_cxt.target_prefetch_pages > 0) {
                    pstate->prefetch_iterator = tbm_prepare_shared_iterate(tbm);

                    /*
                     * We don't need the mutex here as we haven't yet woke up
                     * others.
                     */
                    pstate->prefetch_pages = 0;
                    pstate->prefetch_target = -1;
                }
#endif
                /* We have initialized the shared state so wake up others. */
                BitmapDoneInitializingSharedState(pstate);
            }

            /* Allocate a private iterator and attach the shared state to it */
            node->shared_tbmiterator = shared_tbmiterator = tbm_attach_shared_iterate(pstate->tbmiterator);
            node->tbmres = tbmres = NULL;

#ifdef USE_PREFETCH
            if (u_sess->storage_cxt.target_prefetch_pages > 0) {
                node->shared_prefetch_iterator = tbm_attach_shared_iterate(pstate->prefetch_iterator);
                shared_prefetch_it = node->shared_prefetch_iterator;
            }
#endif
        }
        node->initialized = true;
    }

    for (;;) {
        Page dp;
        ItemId lp;

        /*
         * Get next page of results if needed
         */
        if (tbmres == NULL) {
            if (pstate == NULL) {
                node->tbmres = tbmres = tbm_iterate(tbmiterator);
            } else {
                node->tbmres = tbmres = tbm_shared_iterate(shared_tbmiterator);
            }
            if (tbmres == NULL) {
                /* no more entries in the bitmap */
                break;
            }

#ifdef USE_PREFETCH
            BitmapAdjustPrefetchIterator(node, tbmres, prefetch_iterator, shared_prefetch_it);
#endif /* USE_PREFETCH */

            /* Check whether switch partition-fake-rel, use rd_rel save */
            if (pstate == NULL && BitmapNodeNeedSwitchPartRel(node)) {
                GPISetCurrPartOid(node->gpi_scan, node->tbmres->partitionOid);
                if (!GPIGetNextPartRelation(node->gpi_scan, CurrentMemoryContext, AccessShareLock)) {
                    /* If the current partition is invalid, the next page is directly processed */
                    tbmres = NULL;
#ifdef USE_PREFETCH
                    BitmapHeapPrefetchNext(node, scan, tbm, &prefetch_iterator, &shared_prefetch_it);
#endif /* USE_PREFETCH */
                    continue;
                }
                scan->rs_rd = node->gpi_scan->fakePartRelation;
                scan->rs_nblocks = RelationGetNumberOfBlocks(scan->rs_rd);
            }

            /*
             * Ignore any claimed entries past what we think is the end of the
             * relation.  (This is probably not necessary given that we got at
             * least AccessShareLock on the table before performing any of the
             * indexscans, but let's be safe.)
             */
            if (tbmres->blockno >= scan->rs_nblocks) {
                node->tbmres = tbmres = NULL;
                continue;
            }

            /*
             * Fetch the current heap page and identify candidate tuples.
             */
            bitgetpage(scan, tbmres);

            /* In single mode and hot standby, we may get a null buffer if index
             * replayed before the tid replayed. This is acceptable, so we skip
             * directly without reporting error.
             */
#ifndef ENABLE_MULTIPLE_NODES
            if(!BufferIsValid(scan->rs_cbuf)) {
               node->tbmres = tbmres = NULL;
               continue;
            }
#endif

            /*
             * Set rs_cindex to first slot to examine
             */
            scan->rs_cindex = 0;

#ifdef USE_PREFETCH

            /*
             * Increase prefetch target if it's not yet at the max.  Note that
             * we will increase it to zero after fetching the very first
             * page/tuple, then to one after the second tuple is fetched, then
             * it doubles as later pages are fetched.
             */
            BitmapAdjustPrefetchTarget(node);
#endif /* USE_PREFETCH */
        } else {
            /*
             * Continuing in previously obtained page; advance rs_cindex
             */
            scan->rs_cindex++;

#ifdef USE_PREFETCH

            /*
             * Try to prefetch at least a few pages even before we get to the
             * second page if we don't stop reading after the first tuple.
             */
            if (pstate == NULL) {
                if (node->prefetch_target < u_sess->storage_cxt.target_prefetch_pages) {
                    node->prefetch_target++;
                }
            } else if (pstate->prefetch_target < u_sess->storage_cxt.target_prefetch_pages) {
                /* take spinlock while updating shared state */
                (void)pthread_mutex_lock(&pstate->cv_mtx);
                if (pstate->prefetch_target < u_sess->storage_cxt.target_prefetch_pages) {
                    pstate->prefetch_target++;
                }
                (void)pthread_mutex_unlock(&pstate->cv_mtx);
            }
#endif /* USE_PREFETCH */
        }

        /*
         * Out of range?  If so, nothing more to look at on this page
         */
        if (scan->rs_cindex < 0 || scan->rs_cindex >= scan->rs_ntuples) {
            node->tbmres = tbmres = NULL;
            continue;
        }

#ifdef USE_PREFETCH
        BitmapHeapPrefetchNext(node, scan, tbm, &prefetch_iterator, &shared_prefetch_it);
#endif /* USE_PREFETCH */

        /*
         * Okay to fetch the tuple
         */
        targoffset = scan->rs_vistuples[scan->rs_cindex];
        dp = (Page)BufferGetPage(scan->rs_cbuf);
        lp = PageGetItemId(dp, targoffset);
        Assert(ItemIdIsNormal(lp));

        scan->rs_ctup.t_data = (HeapTupleHeader)PageGetItem((Page)dp, lp);
        scan->rs_ctup.t_len = ItemIdGetLength(lp);
        scan->rs_ctup.t_tableOid = RelationGetRelid(scan->rs_rd);
        scan->rs_ctup.t_bucketId = RelationGetBktid(scan->rs_rd);
        HeapTupleCopyBaseFromPage(&scan->rs_ctup, dp);
        ItemPointerSet(&scan->rs_ctup.t_self, tbmres->blockno, targoffset);

        pgstat_count_heap_fetch(scan->rs_rd);

        /*
         * Set up the result slot to point to this tuple. Note that the slot
         * acquires a pin on the buffer.
         */
        (void)ExecStoreTuple(&scan->rs_ctup, slot, scan->rs_cbuf, false);

        /*
         * If we are using lossy info, we have to recheck the qual conditions
         * at every tuple.
         */
        if (tbmres->recheck) {
            econtext->ecxt_scantuple = slot;
            ResetExprContext(econtext);

            if (!ExecQual(node->bitmapqualorig, econtext, false)) {
                /* Fails recheck, so drop it and loop back for another */
                InstrCountFiltered2(node, 1);
                (void)ExecClearTuple(slot);
                continue;
            }
        }

        /* OK to return this tuple */
        return slot;
    }

    /*
     * if we get here it means we are at the end of the scan..
     */
    return ExecClearTuple(slot);
}

/*
 *	BitmapDoneInitializingSharedState - Shared state is initialized
 *
 *	By this time the leader has already populated the TBM and initialized the
 *	shared state so wake up other processes.
 */
static inline void BitmapDoneInitializingSharedState(ParallelBitmapHeapState *pstate)
{
    (void)pthread_mutex_lock(&pstate->cv_mtx);
    pstate->state = BM_FINISHED;
    (void)pthread_cond_broadcast(&pstate->cv);
    (void)pthread_mutex_unlock(&pstate->cv_mtx);
}

/*
 * bitgetpage - subroutine for BitmapHeapNext()
 *
 * This routine reads and pins the specified page of the relation, then
 * builds an array indicating which tuples on the page are both potentially
 * interesting according to the bitmap, and visible according to the snapshot.
 */
static void bitgetpage(HeapScanDesc scan, TBMIterateResult* tbmres)
{
    BlockNumber page = tbmres->blockno;
    Buffer buffer;
    Snapshot snapshot;
    int ntup;

    /*
     * Acquire pin on the target heap page, trading in any pin we held before.
     */
    Assert(page < scan->rs_nblocks);

    gstrace_entry(GS_TRC_ID_bitgetpage);

    scan->rs_cbuf = ReleaseAndReadBuffer(scan->rs_cbuf, scan->rs_rd, page);

    /* In single mode and hot standby, we may get a null buffer if index
     * replayed before the tid replayed. This is acceptable, so we return
     * directly without reporting error.
     */
#ifndef ENABLE_MULTIPLE_NODES
    if(!BufferIsValid(scan->rs_cbuf)) {
       gstrace_exit(GS_TRC_ID_bitgetpage);
       return;
    }
#endif

    buffer = scan->rs_cbuf;
    snapshot = scan->rs_snapshot;

    ntup = 0;

    /*
     * Prune and repair fragmentation for the whole page, if possible.
     */
    heap_page_prune_opt(scan->rs_rd, buffer);

    /*
     * We must hold share lock on the buffer content while examining tuple
     * visibility.	Afterwards, however, the tuples we have found to be
     * visible are guaranteed good as long as we hold the buffer pin.
     */
    LockBuffer(buffer, BUFFER_LOCK_SHARE);

    /*
     * We need two separate strategies for lossy and non-lossy cases.
     */
    if (tbmres->ntuples >= 0) {
        /*
         * Bitmap is non-lossy, so we just look through the offsets listed in
         * tbmres; but we have to follow any HOT chain starting at each such
         * offset.
         */
        int curslot;

        for (curslot = 0; curslot < tbmres->ntuples; curslot++) {
            OffsetNumber offnum = tbmres->offsets[curslot];
            ItemPointerData tid;
            HeapTupleData heapTuple;

            ItemPointerSet(&tid, page, offnum);
            if (heap_hot_search_buffer(&tid, scan->rs_rd, buffer, snapshot, &heapTuple, NULL, NULL, true))
                scan->rs_vistuples[ntup++] = ItemPointerGetOffsetNumber(&tid);
        }
    } else {
        /*
         * Bitmap is lossy, so we must examine each item pointer on the page.
         * But we can ignore HOT chains, since we'll check each tuple anyway.
         */
        Page dp = (Page)BufferGetPage(buffer);
        OffsetNumber maxoff = PageGetMaxOffsetNumber(dp);
        OffsetNumber offnum;

        for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum)) {
            ItemId lp;
            HeapTupleData loctup;
            bool valid = false;

            lp = PageGetItemId(dp, offnum);
            if (!ItemIdIsNormal(lp))
                continue;
            loctup.t_data = (HeapTupleHeader)PageGetItem((Page)dp, lp);
            loctup.t_len = ItemIdGetLength(lp);
            loctup.t_tableOid = RelationGetRelid(scan->rs_rd);
            loctup.t_bucketId = RelationGetBktid(scan->rs_rd);
            HeapTupleCopyBaseFromPage(&scan->rs_ctup, dp);
            HeapTupleCopyBaseFromPage(&loctup, dp);
            ItemPointerSet(&loctup.t_self, page, offnum);
            valid = HeapTupleSatisfiesVisibility(&loctup, snapshot, buffer);
            if (valid) {
                scan->rs_vistuples[ntup++] = offnum;
                PredicateLockTuple(scan->rs_rd, &loctup, snapshot);
            }
            CheckForSerializableConflictOut(valid, scan->rs_rd, &loctup, buffer, snapshot);
        }
    }

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    Assert(ntup <= MaxHeapTuplesPerPage);
    scan->rs_ntuples = ntup;
    gstrace_exit(GS_TRC_ID_bitgetpage);
}

/*
 * BitmapHeapRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool BitmapHeapRecheck(BitmapHeapScanState* node, TupleTableSlot* slot)
{
    ExprContext* econtext = NULL;

    /*
     * extract necessary information from index scan node
     */
    econtext = node->ss.ps.ps_ExprContext;

    /* Does the tuple meet the original qual conditions? */
    econtext->ecxt_scantuple = slot;

    ResetExprContext(econtext);

    return ExecQual(node->bitmapqualorig, econtext, false);
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapScan(node)
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecBitmapHeapScan(BitmapHeapScanState* node)
{
    return ExecScan(&node->ss, node->ss.ScanNextMtd, (ExecScanRecheckMtd)BitmapHeapRecheck);
}

/* ----------------------------------------------------------------
 *		ExecReScanBitmapHeapScan(node)
 * ----------------------------------------------------------------
 */
void ExecReScanBitmapHeapScan(BitmapHeapScanState* node)
{
    /*
     * deal with partitioned table
     */
    if (node->ss.isPartTbl) {
        if (!PointerIsValid(node->ss.partitions)) {
            return;
        }
        /*
         * if there are partitions for scaning, switch to the next partition;
         * else return with doing nothing
         */
        abs_tbl_endscan(node->ss.ss_currentScanDesc);

        /* switch to next partition for scan */
        ExecInitNextPartitionForBitmapHeapScan(node);
    } else {
        /* rescan to release any page pin */
        abs_tbl_rescan(node->ss.ss_currentScanDesc, NULL);
    }

    /* rescan to release any page pin */

    BitmapHeapFree(node);

    ExecScanReScan(&node->ss);

    /*
     * if chgParam of subnode is not null or the relation is a partitioned table
     * then plan will be re-scanned by first ExecProcNode.
     */
    if (node->ss.isPartTbl || !PointerIsValid(node->ss.ps.lefttree->chgParam))
        ExecReScan(node->ss.ps.lefttree);
}

/* ----------------------------------------------------------------
 *		ExecEndBitmapHeapScan
 * ----------------------------------------------------------------
 */
void ExecEndBitmapHeapScan(BitmapHeapScanState* node)
{

    /*
     * extract information from the node
     */
    Relation relation = node->ss.ss_currentRelation;

    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->ss.ps);

    /*
     * clear out tuple table slots
     */
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /*
     * close down subplans
     */
    ExecEndNode(outerPlanState(node));

    /*
     * release bitmap if any
     */
    BitmapHeapFree(node);
    if (node->ss.ss_currentScanDesc != NULL) {
        abs_tbl_endscan(node->ss.ss_currentScanDesc);
    }

    if (node->gpi_scan != NULL) {
        GPIScanEnd(node->gpi_scan);
    }

    /* close heap scan */
    if (node->ss.isPartTbl && PointerIsValid(node->ss.partitions)) {
        /* close table partition */

        Assert(node->ss.ss_currentPartition);
        releaseDummyRelation(&(node->ss.ss_currentPartition));

        releasePartitionList(node->ss.ss_currentRelation, &(node->ss.partitions), NoLock);
    }

    /*
     * close the heap relation.
     */
    ExecCloseScanRelation(relation);

}
static inline void InitBitmapHeapScanNextMtd(BitmapHeapScanState* bmstate)
{
    if (RELATION_OWN_BUCKET(bmstate->ss.ss_currentRelation)) {
        bmstate->ss.ScanNextMtd = (ExecScanAccessMtd)BitmapHbucketTblNext;
        return;
    }
    bmstate->ss.ScanNextMtd = (ExecScanAccessMtd)BitmapHeapTblNext;
}

/* ----------------------------------------------------------------
 *		ExecInitBitmapHeapScan
 *
 *		Initializes the scan's state information.
 * ----------------------------------------------------------------
 */
BitmapHeapScanState* ExecInitBitmapHeapScan(BitmapHeapScan* node, EState* estate, int eflags)
{
    BitmapHeapScanState* scanstate = NULL;
    Relation currentRelation;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * Assert caller didn't ask for an unsafe snapshot --- see comments at
     * head of file.
     */
    Assert(IsMVCCSnapshot(estate->es_snapshot));

    /*
     * create state structure
     */
    scanstate = makeNode(BitmapHeapScanState);
    scanstate->ss.ps.plan = (Plan*)node;
    scanstate->ss.ps.state = estate;

    scanstate->tbm = NULL;
    scanstate->tbmiterator = NULL;
    scanstate->tbmres = NULL;
    scanstate->prefetch_iterator = NULL;
    scanstate->prefetch_pages = 0;
    scanstate->prefetch_target = 0;
    scanstate->ss.isPartTbl = node->scan.isPartTbl;
    scanstate->ss.currentSlot = 0;
    scanstate->ss.partScanDirection = node->scan.partScanDirection;
    scanstate->pscan_len = 0;
    scanstate->initialized = false;
    scanstate->shared_tbmiterator = NULL;
    scanstate->shared_prefetch_iterator = NULL;
    scanstate->pstate = NULL;

    /* initilize Global partition index scan information */
    GPIScanInit(&scanstate->gpi_scan);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &scanstate->ss.ps);

    scanstate->ss.ps.ps_TupFromTlist = false;

    /*
     * initialize child expressions
     */
    scanstate->ss.ps.targetlist = (List*)ExecInitExpr((Expr*)node->scan.plan.targetlist, (PlanState*)scanstate);
    scanstate->ss.ps.qual = (List*)ExecInitExpr((Expr*)node->scan.plan.qual, (PlanState*)scanstate);
    scanstate->bitmapqualorig = (List*)ExecInitExpr((Expr*)node->bitmapqualorig, (PlanState*)scanstate);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &scanstate->ss.ps);
    ExecInitScanTupleSlot(estate, &scanstate->ss);

    /*
     * open the base relation and acquire appropriate lock on it.
     */
    currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid);

    scanstate->ss.ss_currentRelation = currentRelation;
    scanstate->gpi_scan->parentRelation = currentRelation;

    InitBitmapHeapScanNextMtd(scanstate);
    /*
     * Even though we aren't going to do a conventional seqscan, it is useful
     * to create a HeapScanDesc --- most of the fields in it are usable.
     */
    if (scanstate->ss.isPartTbl) {
        scanstate->ss.ss_currentScanDesc = NULL;
        ExecInitPartitionForBitmapHeapScan(scanstate, estate);

        if (node->scan.itrs > 0) {
            Partition partition = NULL;
            Relation partitiontrel = NULL;

            /* construct a dummy table relation with the next table partition for scan */
            partition = (Partition)list_nth(scanstate->ss.partitions, 0);
            partitiontrel = partitionGetRelation(currentRelation, partition);
            scanstate->ss.ss_currentPartition = partitiontrel;
            scanstate->ss.ss_currentScanDesc =
                abs_tbl_beginscan_bm(partitiontrel, estate->es_snapshot, 0, NULL, &scanstate->ss);
        }
    } else {
        scanstate->ss.ss_currentScanDesc =
            abs_tbl_beginscan_bm(currentRelation, estate->es_snapshot, 0, NULL, &scanstate->ss);
    }
    if (scanstate->ss.ss_currentScanDesc == NULL) {
        scanstate->ss.ps.stubType = PST_Scan;
    }

    /*
     * get the scan type from the relation descriptor.
     */
    ExecAssignScanType(&scanstate->ss, RelationGetDescr(currentRelation));

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(&scanstate->ss.ps);
    ExecAssignScanProjectionInfo(&scanstate->ss);

    /*
     * initialize child nodes
     *
     * We do this last because the child nodes will open indexscans on our
     * relation's indexes, and we want to be sure we have acquired a lock on
     * the relation first.
     */
    outerPlanState(scanstate) = ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * all done.
     */
    return scanstate;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Initialize the table partition and the index partition for
 *			: index sacn
 * Description	:
 * Input		:
 * Output	:
 * Notes		:
 */
static void ExecInitNextPartitionForBitmapHeapScan(BitmapHeapScanState* node)
{
    Partition currentpartition = NULL;
    Relation currentpartitionrel = NULL;
    BitmapHeapScan* plan = NULL;
    int paramno = -1;
    ParamExecData* param = NULL;

    plan = (BitmapHeapScan*)(node->ss.ps.plan);

    /* get partition sequnce */
    paramno = plan->scan.plan.paramno;
    param = &(node->ss.ps.state->es_param_exec_vals[paramno]);
    node->ss.currentSlot = (int)param->value;

    /* construct a dummy relation with the nextl table partition */
    currentpartition = (Partition)list_nth(node->ss.partitions, node->ss.currentSlot);
    currentpartitionrel = partitionGetRelation(node->ss.ss_currentRelation, currentpartition);

    /* switch the partition that needs to be scanned */
    Assert(PointerIsValid(node->ss.ss_currentPartition));
    releaseDummyRelation(&(node->ss.ss_currentPartition));
    node->ss.ss_currentPartition = currentpartitionrel;

    /* Initialize scan descriptor. */
    node->ss.ss_currentScanDesc =
        abs_tbl_beginscan_bm(currentpartitionrel, node->ss.ps.state->es_snapshot, 0, NULL, &node->ss);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: get the table partition that need to be scanned, and add it
 *			: the list for the following scanning
 * Description	:
 * Input		:
 * Output	:
 * Notes		:
 */
static void ExecInitPartitionForBitmapHeapScan(BitmapHeapScanState* scanstate, EState* estate)
{
    BitmapHeapScan* plan = NULL;
    Relation currentRelation = NULL;

    plan = (BitmapHeapScan*)scanstate->ss.ps.plan;

    currentRelation = scanstate->ss.ss_currentRelation;
    scanstate->ss.partitions = NIL;
    scanstate->ss.ss_currentPartition = NULL;

    if (plan->scan.itrs > 0) {
        LOCKMODE lock = NoLock;
        Partition tablepartition = NULL;
        bool relistarget = false;
        ListCell* cell = NULL;
        List* part_seqs = plan->scan.pruningInfo->ls_rangeSelectedPartitions;

        Assert(plan->scan.itrs == plan->scan.pruningInfo->ls_rangeSelectedPartitions->length);
        relistarget = ExecRelationIsTargetRelation(estate, plan->scan.scanrelid);
        lock = (relistarget ? RowExclusiveLock : AccessShareLock);
        scanstate->ss.lockMode = lock;

        foreach (cell, part_seqs) {
            Oid tablepartitionid = InvalidOid;
            int partSeq = lfirst_int(cell);
            /* add table partition to list */
            tablepartitionid = getPartitionOidFromSequence(currentRelation, partSeq);
            tablepartition = partitionOpen(currentRelation, tablepartitionid, lock);
            scanstate->ss.partitions = lappend(scanstate->ss.partitions, tablepartition);
        }
    }
}

/*
 * We issue prefetch requests *after* fetching the current page to try
 * to avoid having prefetching interfere with the main I/O. Also, this
 * should happen only when we have determined there is still something
 * to do on the current page, else we may uselessly prefetch the same
 * page we are just about to request for real.
 */
void BitmapHeapPrefetchNext(BitmapHeapScanState* node, HeapScanDesc scan, const TIDBitmap* tbm,
    TBMIterator** prefetch_iterator, TBMSharedIterator** shared_prefetch_it)
{
    ParallelBitmapHeapState *pstate = node->pstate;
    if ((pstate == NULL && *prefetch_iterator == NULL) ||
        (pstate != NULL && *shared_prefetch_it == NULL)) {
        return;
    }

    ADIO_RUN()
    {
        Assert(shared_prefetch_it == NULL);
        BlockNumber* blockList = NULL;
        BlockNumber* blockListPtr = NULL;
        PrefetchNode* prefetchNode = NULL;
        PrefetchNode* prefetchNodePtr = NULL;
        int prefetchNow = 0;
        int prefetchWindow = node->prefetch_target - node->prefetch_pages;

        /* We expect to prefetch at most prefetchWindow pages */
        if (prefetchWindow > 0) {
            if (tbm_is_global(tbm)) {
                prefetchNode = (PrefetchNode*)malloc(sizeof(PrefetchNode) * prefetchWindow);
                prefetchNodePtr = prefetchNode;
            }
            blockList = (BlockNumber*)palloc(sizeof(BlockNumber) * prefetchWindow);
            blockListPtr = blockList;
        }
        while (node->prefetch_pages < node->prefetch_target) {
            TBMIterateResult* tbmpre = tbm_iterate(*prefetch_iterator);

            if (tbmpre == NULL) {
                /* No more pages to prefetch */
                tbm_end_iterate(*prefetch_iterator);
                node->prefetch_iterator = *prefetch_iterator = NULL;
                break;
            }
            node->prefetch_pages++;
            /* we use PrefetchNode here to store relations between blockno and partition Oid */
            if (tbm_is_global(tbm)) {
                prefetchNodePtr->blockNum = tbmpre->blockno;
                prefetchNodePtr->partOid = tbmpre->partitionOid;
                prefetchNodePtr++;
            }
            /* For Async Direct I/O we accumulate a list and send it */
            *blockListPtr++ = tbmpre->blockno;
            prefetchNow++;
        }

        /* Send the list we generated and free it */
        if (prefetchNow) {
            if (tbm_is_global(tbm)) {
                /*
                 * we must save part Oid before switch relation, and recover it after prefetch.
                 * The reason for this is to assure correctness while getting a new tbmres.
                 */
                Oid oldOid = GPIGetCurrPartOid(node->gpi_scan);
                int blkCount = 0;
                Oid prevOid = prefetchNode[0].partOid;
                for (int i = 0; i < prefetchNow; i++) {
                    if (prefetchNode[i].partOid == prevOid) {
                        blockList[blkCount++] = prefetchNode[i].blockNum;
                    } else {
                        GPISetCurrPartOid(node->gpi_scan, prevOid);
                        if (GPIGetNextPartRelation(node->gpi_scan, CurrentMemoryContext, AccessShareLock)) {
                            PageListPrefetch(node->gpi_scan->fakePartRelation, MAIN_FORKNUM, blockList, blkCount, 0, 0);
                        }
                        blkCount = 0;
                        prevOid = prefetchNode[i].partOid;
                        blockList[blkCount++] = prefetchNode[i].blockNum;
                    }
                }
                GPISetCurrPartOid(node->gpi_scan, prevOid);
                if (GPIGetNextPartRelation(node->gpi_scan, CurrentMemoryContext, AccessShareLock)) {
                    PageListPrefetch(node->gpi_scan->fakePartRelation, MAIN_FORKNUM, blockList, blkCount, 0, 0);
                }
                /* recover old oid after prefetch switch */
                GPISetCurrPartOid(node->gpi_scan, oldOid);
            } else {
                PageListPrefetch(scan->rs_rd, MAIN_FORKNUM, blockList, prefetchNow, 0, 0);
            }
        }
        if (prefetchWindow > 0) {
            pfree_ext(blockList);
            if (tbm_is_global(tbm)) {
                pfree_ext(prefetchNode);
            }
        }
    }
    ADIO_ELSE()
    {
        if (pstate == NULL) {
            Oid oldOid = GPIGetCurrPartOid(node->gpi_scan);
            while (node->prefetch_pages < node->prefetch_target) {
                TBMIterateResult* tbmpre = tbm_iterate(*prefetch_iterator);
                Relation prefetchRel = scan->rs_rd;
                if (tbmpre == NULL) {
                    /* No more pages to prefetch */
                    tbm_end_iterate(*prefetch_iterator);
                    node->prefetch_iterator = *prefetch_iterator = NULL;
                    break;
                }
                node->prefetch_pages++;
                if (tbm_is_global(node->tbm) && GPIScanCheckPartOid(node->gpi_scan, tbmpre->partitionOid)) {
                    GPISetCurrPartOid(node->gpi_scan, tbmpre->partitionOid);
                    if (!GPIGetNextPartRelation(node->gpi_scan, CurrentMemoryContext, AccessShareLock)) {
                        /* If the current partition is invalid, the next page is directly processed */
                        tbmpre = NULL;
                        continue;
                    } else {
                        prefetchRel = node->gpi_scan->fakePartRelation;
                    }
                }
                /* For posix_fadvise() we just send the one request */
                PrefetchBuffer(prefetchRel, MAIN_FORKNUM, tbmpre->blockno);
            }
            /* recover old oid after prefetch switch */
            GPISetCurrPartOid(node->gpi_scan, oldOid);
        } else if (pstate->prefetch_pages < pstate->prefetch_target) {
            if (*shared_prefetch_it != NULL) {
                while (1) {
                    bool do_prefetch = false;

                    /*
                     * Recheck under the mutex. If some other process has already
                     * done enough prefetching then we need not to do anything.
                     */
                    (void)pthread_mutex_lock(&pstate->cv_mtx);
                    if (pstate->prefetch_pages < pstate->prefetch_target) {
                        pstate->prefetch_pages++;
                        do_prefetch = true;
                    }
                    (void)pthread_mutex_unlock(&pstate->cv_mtx);

                    if (!do_prefetch) {
                        return;
                    }
                    TBMIterateResult *tbmpre = tbm_shared_iterate(*shared_prefetch_it);
                    if (tbmpre == NULL) {
                        /* No more pages to prefetch */
                        tbm_end_shared_iterate(*shared_prefetch_it);
                        node->shared_prefetch_iterator = *shared_prefetch_it = NULL;
                        break;
                    }

                    PrefetchBuffer(scan->rs_rd, MAIN_FORKNUM, tbmpre->blockno);
                }
            }
        }
    }
    ADIO_END();
}

/* ----------------
 * 		BitmapShouldInitializeSharedState
 *
 * 		The first process to come here and see the state to the BM_INITIAL
 * 		will become the leader for the parallel bitmap scan and will be
 * 		responsible for populating the TIDBitmap.  The other processes will
 * 		be blocked by the condition variable until the leader wakes them up.
 * ---------------
 */
static bool BitmapShouldInitializeSharedState(ParallelBitmapHeapState *pstate)
{
    SharedBitmapState state;

    (void)pthread_mutex_lock(&pstate->cv_mtx);
    while (1) {
        CHECK_FOR_INTERRUPTS();
        state = pstate->state;
        if (pstate->state == BM_INITIAL) {
            pstate->state = BM_INPROGRESS;
        }

        /* Exit if bitmap is done, or if we're the leader. */
        if (state != BM_INPROGRESS) {
            break;
        }

        /*
         * Use pthread_cond_timedwait here in case of worker exit in error cases, and call
         * CHECK_FOR_INTERRUPTS to handle the error msg from worker.
         */
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += WAIT_BITMAP_INIT_TIMEOUT;
        (void)pthread_cond_timedwait(&pstate->cv, &pstate->cv_mtx, &ts);
    }
    (void)pthread_mutex_unlock(&pstate->cv_mtx);

    return (state == BM_INITIAL);
}

/* ----------------------------------------------------------------
 * 		ExecBitmapHeapEstimate
 *
 * 		Compute the amount of space we'll need in the parallel
 * 		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void ExecBitmapHeapEstimate(BitmapHeapScanState *node, ParallelContext *pcxt)
{
    EState *estate = node->ss.ps.state;

    node->pscan_len =
        add_size(offsetof(ParallelBitmapHeapState, phs_snapshot_data), EstimateSnapshotSpace(estate->es_snapshot));
}

/* ----------------------------------------------------------------
 * 		ExecBitmapHeapInitializeDSM
 *
 * 		Set up a parallel bitmap heap scan descriptor.
 * ----------------------------------------------------------------
 */
void ExecBitmapHeapInitializeDSM(BitmapHeapScanState *node, ParallelContext *pcxt, int nodeid)
{
    knl_u_parallel_context *cxt = (knl_u_parallel_context *)pcxt->seg;
    EState *estate = node->ss.ps.state;
    ParallelBitmapHeapState *pstate = (ParallelBitmapHeapState*)MemoryContextAllocZero(cxt->memCtx,
        node->pscan_len);

    pstate->tbmiterator = NULL;
    pstate->prefetch_iterator = NULL;
    pstate->prefetch_pages = 0;
    pstate->prefetch_target = 0;
    pstate->state = BM_INITIAL;
    pstate->plan_node_id = node->ss.ps.plan->plan_node_id;
    pstate->pscan_len = node->pscan_len;

    (void)pthread_cond_init(&pstate->cv, NULL);
    (void)pthread_mutex_init(&pstate->cv_mtx, NULL);
    SerializeSnapshot(estate->es_snapshot, pstate->phs_snapshot_data,
        node->pscan_len - offsetof(ParallelBitmapHeapState, phs_snapshot_data));

    cxt->pwCtx->queryInfo.bmscan[nodeid] = pstate;
    node->pstate = pstate;
}

/* ----------------------------------------------------------------
 * 		ExecBitmapHeapReInitializeDSM
 *
 * 		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void ExecBitmapHeapReInitializeDSM(BitmapHeapScanState *node, ParallelContext *pcxt)
{
    ParallelBitmapHeapState *pstate = node->pstate;

    /* If there's no DSA, there are no workers; do nothing. */
    if (t_thrd.bgworker_cxt.memCxt == NULL) {
        return;
    }
    pstate->state = BM_INITIAL;

    if (pstate->tbmiterator != NULL) {
        tbm_free_shared_area(node->tbm, pstate->tbmiterator);
    }
    if (pstate->prefetch_iterator != NULL) {
        tbm_free_shared_area(node->tbm, pstate->prefetch_iterator);
    }
    pstate->tbmiterator = NULL;
    pstate->prefetch_iterator = NULL;
}

/* ----------------------------------------------------------------
 * 		ExecBitmapHeapInitializeWorker
 *
 * 		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void ExecBitmapHeapInitializeWorker(BitmapHeapScanState *node, void *context)
{
    Assert(t_thrd.bgworker_cxt.memCxt != NULL);
    ParallelBitmapHeapState *pstate = NULL;
    knl_u_parallel_context *cxt = (knl_u_parallel_context *)context;

    for (int i = 0; i < cxt->pwCtx->queryInfo.bmscan_num; i++) {
        if (node->ss.ps.plan->plan_node_id == cxt->pwCtx->queryInfo.bmscan[i]->plan_node_id) {
            pstate = cxt->pwCtx->queryInfo.bmscan[i];
            break;
        }
    }
    if (pstate == NULL) {
        ereport(ERROR, (errmsg("could not find plan info, plan node id:%d", node->ss.ps.plan->plan_node_id)));
    }
    node->pstate = pstate;

    Snapshot snapshot = RestoreSnapshot(pstate->phs_snapshot_data,
        pstate->pscan_len - offsetof(ParallelBitmapHeapState, phs_snapshot_data));
    heap_scan_update_snapshot((HeapScanDesc)node->ss.ss_currentScanDesc, snapshot);
}

