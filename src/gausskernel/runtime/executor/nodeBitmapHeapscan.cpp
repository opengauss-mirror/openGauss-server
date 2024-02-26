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
 * Portions Copyright (c) 2021, openGauss Contributors
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
#include "catalog/pg_partition_fn.h"
#include "commands/cluster.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeBitmapHeapscan.h"
#include "pgstat.h"
#include "storage/buf/bufmgr.h"
#include "storage/predicate.h"
#include "storage/tcap.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/access_gstrace.h"
#include "nodes/execnodes.h"
#include "access/ustore/knl_uscan.h"

#include "nodes/makefuncs.h"
#include "optimizer/pruning.h"

#include "nodes/makefuncs.h"
#include "optimizer/pruning.h"

TupleTableSlot* ExecBitmapHeapScan(PlanState* state);
TupleTableSlot* BitmapHbucketTblNext(BitmapHeapScanState* node);
static TupleTableSlot* BitmapHeapTblNext(BitmapHeapScanState* node);
bool heapam_scan_bitmap_next_block(TableScanDesc scan, TBMIterateResult* tbmres,
    bool* has_cur_xact_write = NULL);
void ExecInitPartitionForBitmapHeapScan(BitmapHeapScanState* scanstate, EState* estate);
static void ExecInitNextPartitionForBitmapHeapScan(BitmapHeapScanState* node);
void BitmapHeapPrefetchNext(
    BitmapHeapScanState* node, TableScanDesc scan, const TIDBitmap* tbm, TBMIterator** prefetch_iterator);

/* This struct is used for partition switch while prefetch pages */
typedef struct PrefetchNode {
    BlockNumber blockNum;
    Oid partOid;
    int2 bktId;
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
    if (node->tbm != NULL) {
        tbm_free(node->tbm);
        node->tbm = NULL;
    }
    node->tbmres = NULL;
}
TupleTableSlot* BitmapHbucketTblNext(BitmapHeapScanState* node)
{
    Assert(node->ss.ss_currentScanDesc != NULL);
    HBktTblScanDesc hpScan = (HBktTblScanDesc)node->ss.ss_currentScanDesc;
    TupleTableSlot* slot = NULL;
    while (true) {
        node->ss.ps.hbktScanSlot.currSlot = hpScan->curr_slot;
        node->ss.ps.lefttree->hbktScanSlot.currSlot = hpScan->curr_slot;
        slot = BitmapHeapTblNext(node);

        /* for crossbucket index */
        if (tbm_is_crossbucket(node->tbm)) {
            return slot;
        }

        /* for non-crossbucket index */
        if (!TupIsNull(slot)) {
            return slot;
        }

        if (!hbkt_bitmapheap_scan_nextbucket(hpScan)) {
            return NULL;
        }

        BitmapHeapFree(node);
    }
}

bool HeapamScanBitmapNextTuple(TableScanDesc scan,
                             TBMIterateResult *tbmres,
                             TupleTableSlot *slot)
{
    HeapScanDesc hscan = (HeapScanDesc) scan;
    OffsetNumber targoffset;
    Page         dp;
    ItemId       lp;

    /*
     * Out of range?  If so, nothing more to look at on this page
     */
    if (hscan->rs_base.rs_cindex < 0 || hscan->rs_base.rs_cindex >= hscan->rs_base.rs_ntuples)
        return false;

    /*
     * Okay to fetch the tuple
     */
    targoffset = hscan->rs_base.rs_vistuples[hscan->rs_base.rs_cindex];
    dp = (Page)BufferGetPage(hscan->rs_base.rs_cbuf);
    lp = PageGetItemId(dp, targoffset);
    Assert(ItemIdIsNormal(lp));

    hscan->rs_ctup.t_data = (HeapTupleHeader)PageGetItem((Page)dp, lp);
    hscan->rs_ctup.t_len = ItemIdGetLength(lp);
    hscan->rs_ctup.t_tableOid = RelationGetRelid(hscan->rs_base.rs_rd);
    hscan->rs_ctup.t_bucketId = RelationGetBktid(hscan->rs_base.rs_rd);
    HeapTupleCopyBaseFromPage(&hscan->rs_ctup, dp);
    ItemPointerSet(&hscan->rs_ctup.t_self, tbmres->blockno, targoffset);

    pgstat_count_heap_fetch(hscan->rs_base.rs_rd);

    /*
     * Set up the result slot to point to this tuple. Note that the slot
     * acquires a pin on the buffer.
     */
    (void)ExecStoreTuple(&hscan->rs_ctup, slot, hscan->rs_base.rs_cbuf, false);

    hscan->rs_base.rs_cindex++;

    return true;
}

bool TableScanBitmapNextTuple(TableScanDesc scan, TBMIterateResult *tbmres, TupleTableSlot *slot)
{
    bool isUstore = RelationIsUstoreFormat(scan->rs_rd);
    if (isUstore) {
        return UHeapScanBitmapNextTuple(scan, tbmres, slot);
    } else {
        return HeapamScanBitmapNextTuple(scan, tbmres, slot);
    }
}

bool TableScanBitmapNextBlock(TableScanDesc scan, TBMIterateResult *tbmres, bool* has_cur_xact_write)
{
    bool isUstore = RelationIsUstoreFormat(scan->rs_rd);
    if (isUstore) {
        return UHeapScanBitmapNextBlock(scan, tbmres, has_cur_xact_write);
    } else {
        return heapam_scan_bitmap_next_block(scan, tbmres, has_cur_xact_write);
    }
}

/*
 * This is intended to locate the target child relation (partition or bucket).
 * It is only applied to the underlying scan is a global index scan (GPI or GPI+CBI).
 * 
 * Return values: 0: success; -1: fail; 1: need to prefetch.
 */
int TableScanBitmapNextTargetRel(TableScanDesc scan, BitmapHeapScanState *node)
{
    Assert(scan != NULL);
    Assert(node != NULL);
    Assert(node->tbm != NULL);
    Assert(node->tbmres != NULL);

    bool result = true;
    TIDBitmap *tbm = node->tbm;
    TBMIterateResult *tbmres = node->tbmres;
    int2 bucketid = InvalidBktId;
    bool need_reset_bucketid = false;

    /* Check whether switch partition-fake-rel, use rd_rel save. */
    if (BitmapNodeNeedSwitchPartRel(node)) { /* for global partitioned index */
        GPISetCurrPartOid(node->gpi_scan, tbmres->partitionOid);
        if (!GPIGetNextPartRelation(node->gpi_scan, CurrentMemoryContext, AccessShareLock)) {
            /* return 1 to indicate caller may need to call prefetch */
            return 1;
        }
        scan->rs_rd = node->gpi_scan->fakePartRelation;
        scan->rs_nblocks = RelationGetNumberOfBlocks(scan->rs_rd);

        /* 
         * Reset the scanning bucketid to force reloading 
         * the target bucket relation in this new partition.
         */
        need_reset_bucketid = true;
    }

    /*
     * Check whether need to switch bucket, if the underlying
     * indexscan is a cross-bucket indexscan.
     */
    if (tbm_is_crossbucket(tbm)) { /* for crossbucket index */
        bucketid = tbmres->bucketid; /* set to the current iterating bucketid */
        Assert(BUCKET_NODE_IS_VALID(bucketid));

        need_reset_bucketid = (need_reset_bucketid || (scan->rs_rd->rd_node.bucketNode != bucketid));

        if (need_reset_bucketid ) {
            cbi_set_bucketid(node->cbi_scan, InvalidBktId);
        }

        if (cbi_scan_need_change_bucket(node->cbi_scan, bucketid)) {
            cbi_set_bucketid(node->cbi_scan, bucketid);
            result = cbi_bitmapheap_scan_nextbucket((HBktTblScanDesc)node->ss.ss_currentScanDesc, node->gpi_scan,
                node->cbi_scan);
        }
    }

    return (result ? 0 : -1);
}

/* ----------------------------------------------------------------
 *		BitmapHeapNext
 *
 *		Retrieve next tuple from the BitmapHeapScan node's currentRelation
 * ----------------------------------------------------------------
 */
static TupleTableSlot* BitmapHeapTblNext(BitmapHeapScanState* node)
{
    ExprContext* econtext = NULL;
    TableScanDesc scan = NULL;
    TIDBitmap* tbm = NULL;
    TBMHandler tbm_handler;
    TBMIterator* tbmiterator = NULL;
    TBMIterateResult* tbmres = NULL;
    HBktTblScanDesc hpscan = NULL;

#ifdef USE_PREFETCH
    TBMIterator* prefetch_iterator = NULL;
#endif
    TupleTableSlot* slot = NULL;

    /*
     * extract necessary information from index scan node
     */
    econtext = node->ss.ps.ps_ExprContext;
    slot = node->ss.ss_ScanTupleSlot;
    if (node->ss.ss_currentRelation != NULL && RelationIsPartitionedHashBucketTable(node->ss.ss_currentRelation)) {
        Assert(node->ss.ss_currentScanDesc != NULL);
        hpscan = (HBktTblScanDesc)node->ss.ss_currentScanDesc;
        scan = (TableScanDesc)hpscan->currBktScan;
    } else {
        scan = GetTableScanDesc(node->ss.ss_currentScanDesc, node->ss.ss_currentRelation);
    }
    tbm = node->tbm;
    tbmiterator = node->tbmiterator;
    tbmres = node->tbmres;
#ifdef USE_PREFETCH
    prefetch_iterator = node->prefetch_iterator;
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
    if (tbm == NULL) {
        tbm = (TIDBitmap*)MultiExecProcNode(outerPlanState(node));
        tbm_handler = tbm_get_handler(tbm);

        if (tbm == NULL || !IsA(tbm, TIDBitmap)) {
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_EXECUTOR),
                    errmsg("unrecognized result from subplan for BitmapHeapScan.")));
        }

        node->tbm = tbm;
        node->tbmiterator = tbmiterator = tbm_handler._begin_iterate(tbm);
        node->tbmres = tbmres = NULL;

#ifdef USE_PREFETCH
        if (u_sess->storage_cxt.target_prefetch_pages > 0) {
            node->prefetch_iterator = prefetch_iterator = tbm_handler._begin_iterate(tbm);
            node->prefetch_pages = 0;
            node->prefetch_target = -1;
        }
#endif
    }

    /*
     * Now tbm is not NULL, we have enough information to
     * determine whether need to assign hpscan. Also need
     * to make sure we are not scanning a virtual hashbucket
     * table.
     */
    if (hpscan == NULL && tbm_is_crossbucket(tbm) && RELATION_OWN_BUCKET(node->ss.ss_currentScanDesc->rs_rd)) {
        hpscan = (HBktTblScanDesc)node->ss.ss_currentScanDesc;
    }

    for (;;) {
        /*
         * Get next page of results if needed
         */
        if (tbmres == NULL) {
            node->tbmres = tbmres = tbm_iterate(tbmiterator);
            if (tbmres == NULL) {
                /* no more entries in the bitmap */
                break;
            }

#ifdef USE_PREFETCH
            if (node->prefetch_pages > 0) {
                /* The main iterator has closed the distance by one page */
                node->prefetch_pages--;
            } else if (prefetch_iterator != NULL) {
                /* Do not let the prefetch iterator get behind the main one */
                TBMIterateResult* tbmpre = tbm_iterate(prefetch_iterator);

                if (tbmpre == NULL || tbmpre->blockno != tbmres->blockno) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DATA_EXCEPTION),
                            errmodule(MOD_EXECUTOR),
                            errmsg("prefetch and main iterators are out of sync for BitmapHeapScan.")));
                }
            }
#endif /* USE_PREFETCH */

            int rc = TableScanBitmapNextTargetRel(scan, node);
            if (rc != 0) {
                /* 
                 * If the current partition is invalid,
                 * the next page is directly processed.
                 */
                tbmres = NULL;
#ifdef USE_PREFETCH
                if (rc == 1) {
                    BitmapHeapPrefetchNext(node, scan, tbm, &prefetch_iterator);
                }
#endif /* USE_PREFETCH */
                continue;
            }

            /* update bucket scan */
            if (hpscan != NULL && scan != hpscan->currBktScan) {
                scan = hpscan->currBktScan;
            }

            /*
             * Fetch the current table page and identify candidate tuples.
             */
            if (!TableScanBitmapNextBlock(scan, tbmres, &node->ss.ps.state->have_current_xact_date)) {
                node->tbmres = tbmres = NULL;
                continue;
            }

            if (tbmres->ntuples >= 0) {
                node->exact_pages++;
            } else {
                node->lossy_pages++;
            }

#ifdef USE_PREFETCH

            /*
             * Increase prefetch target if it's not yet at the max.  Note that
             * we will increase it to zero after fetching the very first
             * page/tuple, then to one after the second tuple is fetched, then
             * it doubles as later pages are fetched.
             */
            if (node->prefetch_target >= u_sess->storage_cxt.target_prefetch_pages)
                /* don't increase any further */;
            else if (node->prefetch_target >= u_sess->storage_cxt.target_prefetch_pages / 2)
                node->prefetch_target = u_sess->storage_cxt.target_prefetch_pages;
            else if (node->prefetch_target > 0)
                node->prefetch_target *= 2;
            else
                node->prefetch_target++;
#endif /* USE_PREFETCH */
        } else {
            /*
             * Continuing in previously obtained page.
             */

#ifdef USE_PREFETCH

            /*
             * Try to prefetch at least a few pages even before we get to the
             * second page if we don't stop reading after the first tuple.
             */
            if (node->prefetch_target < u_sess->storage_cxt.target_prefetch_pages)
                node->prefetch_target++;
#endif /* USE_PREFETCH */
        }

#ifdef USE_PREFETCH
        BitmapHeapPrefetchNext(node, scan, tbm, &prefetch_iterator);
#endif /* USE_PREFETCH */

        /*
         * Attempt to fetch tuple from AM.
         */
        if (!TableScanBitmapNextTuple(scan, tbmres, slot)) {
            /* nothing more to look at on this page */
            node->tbmres = tbmres = NULL;
            continue;
        }

        /*
         * If we are using lossy info, we have to recheck the qual conditions
         * at every tuple.
         */
        if (tbmres->recheck) {
            econtext->ecxt_scantuple = slot;
            ResetExprContext(econtext);

            if (!ExecQual(node->bitmapqualorig, econtext)) {
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
 * bitgetpage - subroutine for BitmapHeapNext()
 *
 * This routine reads and pins the specified page of the relation, then
 * builds an array indicating which tuples on the page are both potentially
 * interesting according to the bitmap, and visible according to the snapshot.
 */
bool heapam_scan_bitmap_next_block(TableScanDesc scan, TBMIterateResult* tbmres, bool* has_cur_xact_write)
{
    HeapScanDesc hscan = (HeapScanDesc) scan;
    BlockNumber page = tbmres->blockno;
    Buffer buffer;
    Snapshot snapshot;
    int ntup;

    hscan->rs_base.rs_cindex = 0;
    hscan->rs_base.rs_ntuples = 0;

    /*
     * Ignore any claimed entries past what we think is the end of the
     * relation. It may have been extended after the start of our scan (we
     * only hold an AccessShareLock, and it could be inserts from this
     * backend).
     */
    if (page >= hscan->rs_base.rs_nblocks)
        return false;

    /*
     * Acquire pin on the target heap page, trading in any pin we held before.
     */

    hscan->rs_base.rs_cbuf = ReleaseAndReadBuffer(hscan->rs_base.rs_cbuf, hscan->rs_base.rs_rd, page);

    /* In hot standby, we may get a null buffer if index
     * replayed before the tid replayed. This is acceptable, so we return
     * directly without reporting error.
     */
    if (!BufferIsValid(hscan->rs_base.rs_cbuf)) {
        return false;
    }

    hscan->rs_base.rs_cblock = page;
    buffer = hscan->rs_base.rs_cbuf;
    snapshot = hscan->rs_base.rs_snapshot;

    ntup = 0;

    /*
     * Prune and repair fragmentation for the whole page, if possible.
     */
    heap_page_prune_opt(hscan->rs_base.rs_rd, buffer);

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
            if (heap_hot_search_buffer(&tid, hscan->rs_base.rs_rd, buffer, snapshot, &heapTuple, NULL, NULL, true,
                                       has_cur_xact_write))
                hscan->rs_base.rs_vistuples[ntup++] = ItemPointerGetOffsetNumber(&tid);
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
            loctup.t_tableOid = RelationGetRelid(hscan->rs_base.rs_rd);
            loctup.t_bucketId = RelationGetBktid(hscan->rs_base.rs_rd);
            HeapTupleCopyBaseFromPage(&hscan->rs_ctup, dp);
            HeapTupleCopyBaseFromPage(&loctup, dp);
            ItemPointerSet(&loctup.t_self, page, offnum);
            valid = HeapTupleSatisfiesVisibility(&loctup, snapshot, buffer);
            if (valid) {
                hscan->rs_base.rs_vistuples[ntup++] = offnum;
                PredicateLockTuple(hscan->rs_base.rs_rd, &loctup, snapshot);
            }
            CheckForSerializableConflictOut(valid, hscan->rs_base.rs_rd, (void *) &loctup, buffer, snapshot);
        }
    }

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

    Assert(ntup <= MaxHeapTuplesPerPage);
    hscan->rs_base.rs_ntuples = ntup;

    return ntup > 0;
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

    return ExecQual(node->bitmapqualorig, econtext);
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapScan(node)
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecBitmapHeapScan(PlanState* state)
{
    BitmapHeapScanState* node = castNode(BitmapHeapScanState, state);
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
    if (node->ss.isPartTbl && !(((Scan *)node->ss.ps.plan)->partition_iterator_elimination)) {
        if (!PointerIsValid(node->ss.partitions)) {
            return;
        }
        /*
         * if there are partitions for scaning, switch to the next partition;
         * else return with doing nothing
         */
        scan_handler_tbl_endscan(node->ss.ss_currentScanDesc);

        /* switch to next partition for scan */
        ExecInitNextPartitionForBitmapHeapScan(node);
    } else {
        /* rescan to release any page pin */
        scan_handler_tbl_rescan(node->ss.ss_currentScanDesc, NULL, node->ss.ss_currentRelation, true);
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
        scan_handler_tbl_endscan(node->ss.ss_currentScanDesc);
    }

    if (node->gpi_scan != NULL) {
        GPIScanEnd(node->gpi_scan);
    }

    if (node->cbi_scan != NULL) {
        cbi_scan_end(node->cbi_scan);
    }

    /* close heap scan */
    if (node->ss.isPartTbl && PointerIsValid(node->ss.partitions)) {
        /* close table partition */

        Assert(node->ss.ss_currentPartition);
        releaseDummyRelation(&(node->ss.ss_currentPartition));

        releaseSubPartitionList(node->ss.ss_currentRelation, &(node->ss.subpartitions), NoLock);
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

TableScanDesc UHeapBeginScan(Relation relation, Snapshot snapshot, int nkeys, ParallelHeapScanDesc parallel_scan);


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
    bool isUstoreRel = false;
    Snapshot scanSnap;

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
    scanstate->exact_pages = 0.0;
    scanstate->lossy_pages = 0.0;
    scanstate->prefetch_iterator = NULL;
    scanstate->prefetch_pages = 0;
    scanstate->prefetch_target = 0;
    scanstate->ss.isPartTbl = node->scan.isPartTbl;
    scanstate->ss.currentSlot = 0;
    scanstate->ss.partScanDirection = node->scan.partScanDirection;
    scanstate->ss.ps.ExecProcNode = ExecBitmapHeapScan;

    /* initialize Global partition index scan information */
    GPIScanInit(&scanstate->gpi_scan);

    /* initialize cross-bucket index scan information */
    cbi_scan_init(&scanstate->cbi_scan);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &scanstate->ss.ps);

    /*
     * initialize child expressions
     */
    if (estate->es_is_flt_frame) {
        scanstate->ss.ps.qual = (List*)ExecInitQualByFlatten(node->scan.plan.qual, (PlanState*)scanstate);
        scanstate->bitmapqualorig = (List*)ExecInitQualByFlatten(node->bitmapqualorig, (PlanState*)scanstate);
    } else {
        scanstate->ss.ps.targetlist = (List*)ExecInitExprByRecursion((Expr*)node->scan.plan.targetlist, (PlanState*)scanstate);
        scanstate->ss.ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->scan.plan.qual, (PlanState*)scanstate);
        scanstate->bitmapqualorig = (List*)ExecInitExprByRecursion((Expr*)node->bitmapqualorig, (PlanState*)scanstate);
    }

    /*
     * open the base relation and acquire appropriate lock on it.
     */
    currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid);

    scanstate->ss.ss_currentRelation = currentRelation;
    scanstate->gpi_scan->parentRelation = currentRelation;

    isUstoreRel = RelationIsUstoreFormat(currentRelation);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &scanstate->ss.ps, currentRelation->rd_tam_ops);
    ExecInitScanTupleSlot(estate, &scanstate->ss, currentRelation->rd_tam_ops);

    InitBitmapHeapScanNextMtd(scanstate);

    /*
     * Choose user-specified snapshot if TimeCapsule clause exists, otherwise 
     * estate->es_snapshot instead.
     */
    scanSnap = TvChooseScanSnap(currentRelation, &node->scan, &scanstate->ss);

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

            if (scanstate->ss.partitions != NIL) {
                /* construct a dummy table relation with the next table partition for scan */
                partition = (Partition)list_nth(scanstate->ss.partitions, 0);
                partitiontrel = partitionGetRelation(currentRelation, partition);
                scanstate->ss.ss_currentPartition = partitiontrel;

                /*
                 * Verify if a DDL operation that froze all tuples in the relation
                 * occured after taking the snapshot. Skip for explain only commands.
                 */
                if (isUstoreRel && !(eflags & EXEC_FLAG_EXPLAIN_ONLY)) {
                    TransactionId relfrozenxid64 = InvalidTransactionId;
                    getPartitionRelxids(partitiontrel, &relfrozenxid64);
                    if (TransactionIdPrecedes(FirstNormalTransactionId, scanSnap->xmax) &&
                        !TransactionIdIsCurrentTransactionId(relfrozenxid64) &&
                        TransactionIdPrecedes(scanSnap->xmax, relfrozenxid64)) {
                        ereport(ERROR, (errcode(ERRCODE_SNAPSHOT_INVALID),
                            (errmsg("Snapshot too old, BitmapHeapScan is PartTbl, the info: snapxmax is %lu, "
                                "snapxmin is %lu, csn is %lu, relfrozenxid64 is %lu, globalRecycleXid is %lu.",
                                scanSnap->xmax, scanSnap->xmin, scanSnap->snapshotcsn, relfrozenxid64,
                                g_instance.undo_cxt.globalRecycleXid))));
                    }
                }

                scanstate->ss.ss_currentScanDesc =
                    scan_handler_tbl_beginscan_bm(partitiontrel, scanSnap, 0, NULL, &scanstate->ss);
            }
        }
    } else {
        if (!isUstoreRel) {
            scanstate->ss.ss_currentScanDesc =
                scan_handler_tbl_beginscan_bm(currentRelation, scanSnap, 0, NULL, &scanstate->ss);
        } else {
            /*
             * Verify if a DDL operation that froze all tuples in the relation
             * occured after taking the snapshot. Skip for explain only commands.
             */
            if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY)) {
                TransactionId relfrozenxid64 = InvalidTransactionId;
                getRelationRelxids(currentRelation, &relfrozenxid64);
                if (TransactionIdPrecedes(FirstNormalTransactionId, scanSnap->xmax) &&
                    !TransactionIdIsCurrentTransactionId(relfrozenxid64) &&
                    TransactionIdPrecedes(scanSnap->xmax, relfrozenxid64)) {
                    ereport(ERROR, (errcode(ERRCODE_SNAPSHOT_INVALID),
                        (errmsg("Snapshot too old, BitmapHeapScan is not PartTbl, the info: snapxmax is %lu, "
                            "snapxmin is %lu, csn is %lu, relfrozenxid64 is %lu, globalRecycleXid is %lu.",
                            scanSnap->xmax, scanSnap->xmin, scanSnap->snapshotcsn, relfrozenxid64,
                            g_instance.undo_cxt.globalRecycleXid))));
                }
            }

            scanstate->ss.ss_currentScanDesc = UHeapBeginScan(currentRelation, scanSnap, 0, NULL);
        }
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
    ExecAssignResultTypeFromTL(
            &scanstate->ss.ps,
            scanstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops);

    ExecAssignScanProjectionInfo(&scanstate->ss);

    Assert(scanstate->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->td_tam_ops);

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
    int subPartParamno = -1;
    ParamExecData* SubPrtParam = NULL;

    plan = (BitmapHeapScan*)(node->ss.ps.plan);

    /* get partition sequnce */
    paramno = plan->scan.plan.paramno;
    param = &(node->ss.ps.state->es_param_exec_vals[paramno]);
    node->ss.currentSlot = (int)param->value;

    subPartParamno = plan->scan.plan.subparamno;
    SubPrtParam = &(node->ss.ps.state->es_param_exec_vals[subPartParamno]);

    /* construct a dummy relation with the nextl table partition */
    currentpartition = (Partition)list_nth(node->ss.partitions, node->ss.currentSlot);
    currentpartitionrel = partitionGetRelation(node->ss.ss_currentRelation, currentpartition);

    /* switch the partition that needs to be scanned */
    Assert(PointerIsValid(node->ss.ss_currentPartition));
    releaseDummyRelation(&(node->ss.ss_currentPartition));

    BitmapHeapScanState* bitmapState = node;
    BitmapHeapScan* bitmpHeapScan = (BitmapHeapScan*)node->ss.ps.plan;
    Snapshot scanSnap;
    scanSnap = TvChooseScanSnap(currentpartitionrel, &bitmpHeapScan->scan, &bitmapState->ss);

    if (currentpartitionrel->partMap != NULL) {
        Partition currentSubPartition = NULL;
        List* currentSubPartitionList = NULL;
        Relation currentSubPartitionRel = NULL;
        Assert(SubPrtParam != NULL);
        currentSubPartitionList = (List *)list_nth(node->ss.subpartitions, node->ss.currentSlot);
        currentSubPartition = (Partition)list_nth(currentSubPartitionList, (int)SubPrtParam->value);
        currentSubPartitionRel = partitionGetRelation(currentpartitionrel, currentSubPartition);
        releaseDummyRelation(&(currentpartitionrel));

        node->ss.ss_currentPartition = currentSubPartitionRel;
        node->gpi_scan->parentRelation = currentpartitionrel;

        /* Initialize scan descriptor. */
        node->ss.ss_currentScanDesc =
            scan_handler_tbl_beginscan_bm(currentSubPartitionRel, scanSnap, 0, NULL, &node->ss);
    } else {
        node->ss.ss_currentPartition = currentpartitionrel;

        /* Initialize scan descriptor. */
        node->ss.ss_currentScanDesc =
            scan_handler_tbl_beginscan_bm(currentpartitionrel, scanSnap, 0, NULL, &node->ss);
    }
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
void ExecInitPartitionForBitmapHeapScan(BitmapHeapScanState* scanstate, EState* estate)
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
        PruningResult* resultPlan = NULL;
        if (plan->scan.pruningInfo->expr) {
            resultPlan = GetPartitionInfo(plan->scan.pruningInfo, estate, currentRelation);
            if (ENABLE_SQL_BETA_FEATURE(PARTITION_OPFUSION)) {
                if (estate->pruningResult) {
                    destroyPruningResult(estate->pruningResult);
                }
                estate->pruningResult = resultPlan;
            }
        } else {
            resultPlan = plan->scan.pruningInfo;
        }

        if (resultPlan->ls_rangeSelectedPartitions != NULL) {
            scanstate->ss.part_id = resultPlan->ls_rangeSelectedPartitions->length;
        } else {
            scanstate->ss.part_id = 0;
        }

        ListCell* cell1 = NULL;
        ListCell* cell2 = NULL;
        List* part_seqs = resultPlan->ls_rangeSelectedPartitions;
        List* partitionnos = resultPlan->ls_selectedPartitionnos;
        Assert(list_length(part_seqs) == list_length(partitionnos));
        relistarget = ExecRelationIsTargetRelation(estate, plan->scan.scanrelid);
        lock = (relistarget ? RowExclusiveLock : AccessShareLock);
        scanstate->ss.lockMode = lock;

        forboth (cell1, part_seqs, cell2, partitionnos) {
            Oid tablepartitionid = InvalidOid;
            int partSeq = lfirst_int(cell1);
            int partitionno = lfirst_int(cell2);
            /* add table partition to list */
            tablepartitionid = getPartitionOidFromSequence(currentRelation, partSeq, partitionno);
            tablepartition = PartitionOpenWithPartitionno(currentRelation, tablepartitionid, partitionno, lock);
            scanstate->ss.partitions = lappend(scanstate->ss.partitions, tablepartition);

            if (resultPlan->ls_selectedSubPartitions != NIL) {
                Relation partRelation = partitionGetRelation(currentRelation, tablepartition);
                SubPartitionPruningResult* subPartPruningResult =
                    GetSubPartitionPruningResult(resultPlan->ls_selectedSubPartitions, partSeq, partitionno);
                if (subPartPruningResult == NULL) {
                    continue;
                }
                List *subpart_seqs = subPartPruningResult->ls_selectedSubPartitions;
                List *subpartitionnos = subPartPruningResult->ls_selectedSubPartitionnos;
                Assert(list_length(subpart_seqs) == list_length(subpartitionnos));
                List *subpartition = NULL;
                ListCell *lc1 = NULL;
                ListCell *lc2 = NULL;
                forboth (lc1, subpart_seqs, lc2, subpartitionnos) {
                    Oid subpartitionid = InvalidOid;
                    int subpartSeq = lfirst_int(lc1);
                    int subpartitionno = lfirst_int(lc2);

                    subpartitionid = getPartitionOidFromSequence(partRelation, subpartSeq, subpartitionno);
                    Partition subpart =
                        PartitionOpenWithPartitionno(partRelation, subpartitionid, subpartitionno, lock);
                    subpartition = lappend(subpartition, subpart);
                }
                releaseDummyRelation(&(partRelation));
                scanstate->ss.subPartLengthList =
                    lappend_int(scanstate->ss.subPartLengthList, list_length(subpartition));
                scanstate->ss.subpartitions = lappend(scanstate->ss.subpartitions, subpartition);
            }
        }
    }
}

static Relation BitmapHeapPrefetchTargetRel(BitmapHeapScanState* node, Oid partoid, bool partmatched,
    int2 bucketid, bool bktmatched)
{
    Relation targetheap = NULL;
    HBktTblScanDesc hpscan = (HBktTblScanDesc)node->ss.ss_currentScanDesc;
    bool partchanged = false;
    bool bktchanged = false;
    bool isgpi = OidIsValid(partoid);
    bool iscbi = BUCKET_NODE_IS_VALID(bucketid);

    if (isgpi && !partmatched) {
        GPISetCurrPartOid(node->gpi_scan, partoid);
        partchanged = GPIGetNextPartRelation(node->gpi_scan, CurrentMemoryContext, AccessShareLock);
        targetheap = node->gpi_scan->fakePartRelation;
    }

    if (iscbi) {
        if (partchanged) {
            cbi_set_bucketid(node->cbi_scan, InvalidBktId);
        } else if (!bktmatched) {
            cbi_set_bucketid(node->cbi_scan, bucketid);
        }

        targetheap = cbi_bitmapheap_scan_getbucket(hpscan, node->gpi_scan, node->cbi_scan, bucketid);
        bktchanged = (targetheap != hpscan->currBktRel);
    }

    if (!partchanged && !bktchanged) {
        return NULL;
    }

    return targetheap;
}

/* First sort by partition oid, then call PageListPrefetch to get the pages under each partition */
void BitmapHeapPrefetchWithCrossLevelIndex(BitmapHeapScanState* node, int prefetchNow, PrefetchNode* prefetchNode,
    BlockNumber* blockList)
{
    /*
     * we must save part Oid before switch relation, and recover it after prefetch.
     * The reason for this is to assure correctness while getting a new tbmres.
     */
    HBktTblScanDesc hpscan = (HBktTblScanDesc)node->ss.ss_currentScanDesc;
    Oid originOid = GPIGetCurrPartOid(node->gpi_scan);
    int2 originBktId = cbi_get_current_bucketid(node->cbi_scan);
    int blkCount = 0;
    Oid prevOid = prefetchNode[0].partOid;
    int2 prevBktId = prefetchNode[0].bktId;
    bool isgpi = OidIsValid(prevOid);
    bool iscbi = BUCKET_NODE_IS_VALID(prevBktId);
    bool partmatched, bktmatched;
    Relation targetheap = NULL;

    for (int i = 0; i < prefetchNow; i++) {
        partmatched = bktmatched = false;

        if (isgpi && prefetchNode[i].partOid == prevOid) {
            partmatched = true;
        }
        if (iscbi && prefetchNode[i].bktId == prevBktId) {
            bktmatched = true;
        }

        targetheap = BitmapHeapPrefetchTargetRel(node, prevOid, partmatched, prevBktId, bktmatched);
        if (RelationIsValid(targetheap)) {
            PageListPrefetch(targetheap, MAIN_FORKNUM, blockList, blkCount, 0, 0);
        }

        if (isgpi && !partmatched) {
            blkCount = 0;
            prevOid = prefetchNode[i].partOid;
        }
        if (iscbi && !bktmatched) {
            blkCount = 0;
            prevBktId = prefetchNode[i].bktId;

            if (RelationIsValid(targetheap) && targetheap != hpscan->currBktRel) {
                bucketCloseRelation(targetheap);
            }
        }

        blockList[blkCount++] = prefetchNode[i].blockNum;
    }

    targetheap = BitmapHeapPrefetchTargetRel(node, prevOid, false, prevBktId, false);
    if (RelationIsValid(targetheap)) {
        PageListPrefetch(targetheap, MAIN_FORKNUM, blockList, blkCount, 0, 0);
        if (targetheap != hpscan->currBktRel) {
            bucketCloseRelation(targetheap);
        }
    }

    /* recover old oid after prefetch switch */
    GPISetCurrPartOid(node->gpi_scan, originOid);
    cbi_set_bucketid(node->cbi_scan, originBktId);
}

Relation BitmapHeapPrefetchNextTargetHeap(BitmapHeapScanState* node, TBMIterateResult* tbmpre, Relation curr_targetheap)
{
    bool need_reset_bucketid = false;
    Relation targetheap = NULL;
    Relation next_targetheap = curr_targetheap;

    if (tbm_is_global(node->tbm) && GPIScanCheckPartOid(node->gpi_scan, tbmpre->partitionOid)) {
        GPISetCurrPartOid(node->gpi_scan, tbmpre->partitionOid);
        if (!GPIGetNextPartRelation(node->gpi_scan, CurrentMemoryContext, AccessShareLock)) {
            /* If the current partition is invalid, the next page is directly processed */
            return NULL;
        }

        next_targetheap = node->gpi_scan->fakePartRelation;
        need_reset_bucketid = true;
    }

    if (tbm_is_crossbucket(node->tbm)) { /* for crossbucket index */
        HBktTblScanDesc hpscan = (HBktTblScanDesc)node->ss.ss_currentScanDesc;
        int2 bucketid = tbmpre->bucketid; /* set to the current iterating bucketid */
        Assert(BUCKET_NODE_IS_VALID(bucketid));

        if (need_reset_bucketid) {
            cbi_set_bucketid(node->cbi_scan, InvalidBktId);
        }

        targetheap = cbi_bitmapheap_scan_getbucket(hpscan, node->gpi_scan, node->cbi_scan, bucketid);
        if (targetheap == NULL) {
            return NULL;
        }

        /* update target relation to prefetch */
        next_targetheap = targetheap;
    }

    return next_targetheap;
}

void BitmapHeapPrefetchNextAsync(BitmapHeapScanState* node, TableScanDesc scan, const TIDBitmap* tbm,
    TBMIterator** prefetch_iterator)
{
    BlockNumber* blockList = NULL;
    BlockNumber* blockListPtr = NULL;
    PrefetchNode* prefetchNode = NULL;
    PrefetchNode* prefetchNodePtr = NULL;
    int prefetchNow = 0;
    int prefetchWindow = node->prefetch_target - node->prefetch_pages;

    /* We expect to prefetch at most prefetchWindow pages */
    if (prefetchWindow > 0) {
        if (tbm_is_global(tbm) || tbm_is_crossbucket(tbm)) {
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
        if ((tbm_is_global(tbm) || tbm_is_crossbucket(tbm)) && prefetchNodePtr != NULL) {
            prefetchNodePtr->blockNum = tbmpre->blockno;
            prefetchNodePtr->partOid = tbmpre->partitionOid;
            prefetchNodePtr->bktId = tbmpre->bucketid;
            prefetchNodePtr++;
        }
        /* For Async Direct I/O we accumulate a list and send it */
        if (blockListPtr != NULL) {
            *blockListPtr++ = tbmpre->blockno;
        }
        prefetchNow++;
    }

    /* Send the list we generated and free it */
    if (prefetchNow && blockList != NULL) {
        if (tbm_is_global(tbm) || tbm_is_crossbucket(tbm)) {
            BitmapHeapPrefetchWithCrossLevelIndex(node, prefetchNow, prefetchNode, blockList);
        } else {
            PageListPrefetch(scan->rs_rd, MAIN_FORKNUM, blockList, prefetchNow, 0, 0);
        }
    }
    if (prefetchWindow > 0) {
        pfree_ext(blockList);
        if (tbm_is_global(tbm) || tbm_is_crossbucket(tbm)) {
            pfree_ext(prefetchNode);
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
void BitmapHeapPrefetchNext(
    BitmapHeapScanState* node, TableScanDesc scan, const TIDBitmap* tbm, TBMIterator** prefetch_iterator)
{
    if (*prefetch_iterator == NULL) {
        return;
    }

    Assert(node->tbm == tbm);

    ADIO_RUN()
    {
        /* prefetch next asynchronously */
        BitmapHeapPrefetchNextAsync(node, scan, tbm, prefetch_iterator);
    }
    ADIO_ELSE()
    {
        /* prefetch next synchronously */
        if (unlikely(tbm_is_crossbucket(tbm) || tbm_is_global(tbm))) {
            HBktTblScanDesc hpscan = NULL;
            Oid oldOid = GPIGetCurrPartOid(node->gpi_scan);
            int2 oldBktId = cbi_get_current_bucketid(node->cbi_scan);
            Relation oldheap = NULL;
            Relation prefetchRel = scan->rs_rd;
            
            while (node->prefetch_pages < node->prefetch_target) {
                TBMIterateResult* tbmpre = tbm_iterate(*prefetch_iterator);
                hpscan = (tbm_is_crossbucket(node->tbm) ? (HBktTblScanDesc)node->ss.ss_currentScanDesc : NULL);

                if (tbmpre == NULL) {
                    /* No more pages to prefetch */
                    tbm_end_iterate(*prefetch_iterator);
                    node->prefetch_iterator = *prefetch_iterator = NULL;
                    break;
                }
                node->prefetch_pages++;

                prefetchRel = BitmapHeapPrefetchNextTargetHeap(node, tbmpre, prefetchRel);
                if (prefetchRel == NULL) {
                    tbmpre = NULL;
                    continue;
                }

                /* For posix_fadvise() we just send the one request */
                PrefetchBuffer(prefetchRel, MAIN_FORKNUM, tbmpre->blockno);
                if (RelationIsValid(oldheap) && oldheap != prefetchRel && PointerIsValid(hpscan) &&
                    oldheap != hpscan->currBktRel) {
                    /* release previous bucket fake relation except the current scanning one */
                    bucketCloseRelation(oldheap);
                    /* now oldheap is NULL */
                }
                oldheap = prefetchRel;
            }

            if (RelationIsValid(oldheap) && PointerIsValid(hpscan) && oldheap != hpscan->currBktRel) {
                /* release previous bucket fake relation except the current scanning one */
                bucketCloseRelation(oldheap);
            }

            /* recover old oid after prefetch switch */
            GPISetCurrPartOid(node->gpi_scan, oldOid);
            cbi_set_bucketid(node->cbi_scan, oldBktId);
        } else {
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

                /* For posix_fadvise() we just send the one request */
                PrefetchBuffer(prefetchRel, MAIN_FORKNUM, tbmpre->blockno);
            }
        }
    }
    ADIO_END();
}
