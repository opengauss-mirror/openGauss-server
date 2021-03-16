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
#include "storage/buf/bufmgr.h"
#include "storage/predicate.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/access_gstrace.h"
#include "nodes/execnodes.h"

#include "nodes/makefuncs.h"
#include "optimizer/pruning.h"

static TupleTableSlot* BitmapHbucketTblNext(BitmapHeapScanState* node);
static TupleTableSlot* BitmapHeapTblNext(BitmapHeapScanState* node);
static void bitgetpage(TableScanDesc scan, TBMIterateResult* tbmres);
static void ExecInitPartitionForBitmapHeapScan(BitmapHeapScanState* scanstate, EState* estate);
static void ExecInitNextPartitionForBitmapHeapScan(BitmapHeapScanState* node);
void BitmapHeapPrefetchNext(
    BitmapHeapScanState* node, TableScanDesc scan, const TIDBitmap* tbm, TBMIterator** prefetch_iterator);

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
    if (node->tbm != NULL) {
        tbm_free(node->tbm);
        node->tbm = NULL;
    }
    node->tbmres = NULL;
}
static TupleTableSlot* BitmapHbucketTblNext(BitmapHeapScanState* node)
{
    Assert(node->ss.ss_currentScanDesc != NULL);
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
    TBMIterator* tbmiterator = NULL;
    TBMIterateResult* tbmres = NULL;

#ifdef USE_PREFETCH
    TBMIterator* prefetch_iterator = NULL;
#endif
    OffsetNumber targoffset;
    TupleTableSlot* slot = NULL;

    /*
     * extract necessary information from index scan node
     */
    econtext = node->ss.ps.ps_ExprContext;
    slot = node->ss.ss_ScanTupleSlot;
    scan = GetTableScanDesc(node->ss.ss_currentScanDesc, node->ss.ss_currentRelation);
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
    }

    for (;;) {
        Page dp;
        ItemId lp;

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

            /* Check whether switch partition-fake-rel, use rd_rel save */
            if (BitmapNodeNeedSwitchPartRel(node)) {
                GPISetCurrPartOid(node->gpi_scan, node->tbmres->partitionOid);
                if (!GPIGetNextPartRelation(node->gpi_scan, CurrentMemoryContext, AccessShareLock)) {
                    /* If the current partition is invalid, the next page is directly processed */
                    tbmres = NULL;
#ifdef USE_PREFETCH
                    BitmapHeapPrefetchNext(node, scan, tbm, &prefetch_iterator);
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
            if (!BufferIsValid(scan->rs_cbuf)) {
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
             * Continuing in previously obtained page; advance rs_cindex
             */
            scan->rs_cindex++;

#ifdef USE_PREFETCH

            /*
             * Try to prefetch at least a few pages even before we get to the
             * second page if we don't stop reading after the first tuple.
             */
            if (node->prefetch_target < u_sess->storage_cxt.target_prefetch_pages)
                node->prefetch_target++;
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
        BitmapHeapPrefetchNext(node, scan, tbm, &prefetch_iterator);
#endif /* USE_PREFETCH */

        /*
         * Okay to fetch the tuple
         */
        targoffset = scan->rs_vistuples[scan->rs_cindex];
        dp = (Page)BufferGetPage(scan->rs_cbuf);
        lp = PageGetItemId(dp, targoffset);
        Assert(ItemIdIsNormal(lp));

        ((HeapScanDesc)scan)->rs_ctup.t_data = (HeapTupleHeader)PageGetItem((Page)dp, lp);
        ((HeapScanDesc)scan)->rs_ctup.t_len = ItemIdGetLength(lp);
        ((HeapScanDesc)scan)->rs_ctup.t_tableOid = RelationGetRelid(scan->rs_rd);
        ((HeapScanDesc)scan)->rs_ctup.t_bucketId = RelationGetBktid(scan->rs_rd);
        HeapTupleCopyBaseFromPage(&((HeapScanDesc)scan)->rs_ctup, dp);
        ItemPointerSet(&((HeapScanDesc)scan)->rs_ctup.t_self, tbmres->blockno, targoffset);

        pgstat_count_heap_fetch(scan->rs_rd);

        /*
         * Set up the result slot to point to this tuple. Note that the slot
         * acquires a pin on the buffer.
         */
        (void)ExecStoreTuple(&((HeapScanDesc)scan)->rs_ctup, slot, scan->rs_cbuf, false);

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
 * bitgetpage - subroutine for BitmapHeapNext()
 *
 * This routine reads and pins the specified page of the relation, then
 * builds an array indicating which tuples on the page are both potentially
 * interesting according to the bitmap, and visible according to the snapshot.
 */
static void bitgetpage(TableScanDesc scan, TBMIterateResult* tbmres)
{
    BlockNumber page = tbmres->blockno;
    Buffer buffer;
    Snapshot snapshot;
    int ntup;

    /*
     * Acquire pin on the target heap page, trading in any pin we held before.
     */
    Assert(page < scan->rs_nblocks);

    scan->rs_cbuf = ReleaseAndReadBuffer(scan->rs_cbuf, scan->rs_rd, page);

    /* In single mode and hot standby, we may get a null buffer if index
     * replayed before the tid replayed. This is acceptable, so we return
     * directly without reporting error.
     */
#ifndef ENABLE_MULTIPLE_NODES
    if (!BufferIsValid(scan->rs_cbuf)) {
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
            HeapTupleCopyBaseFromPage(&((HeapScanDesc)scan)->rs_ctup, dp);
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
        scan_handler_tbl_endscan(node->ss.ss_currentScanDesc, node->ss.ss_currentRelation);

        /* switch to next partition for scan */
        ExecInitNextPartitionForBitmapHeapScan(node);
        } else {
        /* rescan to release any page pin */
        scan_handler_tbl_rescan(node->ss.ss_currentScanDesc, NULL, node->ss.ss_currentRelation);
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
        scan_handler_tbl_endscan(node->ss.ss_currentScanDesc, relation);
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
     * open the base relation and acquire appropriate lock on it.
     */
    currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid);

    scanstate->ss.ss_currentRelation = currentRelation;
    scanstate->gpi_scan->parentRelation = currentRelation;

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &scanstate->ss.ps, currentRelation->rd_tam_type);
    ExecInitScanTupleSlot(estate, &scanstate->ss, currentRelation->rd_tam_type);
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

            if (scanstate->ss.partitions != NIL) {
                /* construct a dummy table relation with the next table partition for scan */
                partition = (Partition)list_nth(scanstate->ss.partitions, 0);
                partitiontrel = partitionGetRelation(currentRelation, partition);
                scanstate->ss.ss_currentPartition = partitiontrel;
                scanstate->ss.ss_currentScanDesc =
                    scan_handler_tbl_beginscan_bm(partitiontrel, estate->es_snapshot, 0, NULL, &scanstate->ss);
            }
        }
    } else {
        scanstate->ss.ss_currentScanDesc =
           scan_handler_tbl_beginscan_bm(currentRelation, estate->es_snapshot, 0, NULL, &scanstate->ss);
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
            scanstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor->tdTableAmType);

    ExecAssignScanProjectionInfo(&scanstate->ss);

    Assert(scanstate->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->tdTableAmType != TAM_INVALID);

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
        scan_handler_tbl_beginscan_bm(currentpartitionrel, node->ss.ps.state->es_snapshot, 0, NULL, &node->ss);
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
        PruningResult* resultPlan = NULL;
        if (plan->scan.pruningInfo->expr) {
            resultPlan = GetPartitionInfo(plan->scan.pruningInfo, estate, currentRelation);
            if (estate->pruningResult) {
                destroyPruningResult(estate->pruningResult);
            }
            estate->pruningResult = resultPlan;
        } else {
            resultPlan = plan->scan.pruningInfo;
        }
        if (resultPlan->ls_rangeSelectedPartitions != NULL) {
            scanstate->part_id = resultPlan->ls_rangeSelectedPartitions->length;
        } else {
            scanstate->part_id = 0;
        }

        ListCell* cell = NULL;
        List* part_seqs = resultPlan->ls_rangeSelectedPartitions;
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

/* First sort by partition oid, then call PageListPrefetch to get the pages under each partition */
void BitmapHeapPrefetchWithGpi(
    BitmapHeapScanState* node, int prefetchNow, PrefetchNode* prefetchNode, BlockNumber* blockList)
{
    /*
     * we must save part Oid before switch relation, and recover it after prefetch.
     * The reason for this is to assure correctness while getting a new tbmres.
     */
    Oid originOid = GPIGetCurrPartOid(node->gpi_scan);
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
    GPISetCurrPartOid(node->gpi_scan, originOid);
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
    ADIO_RUN()
    {
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
            if (tbm_is_global(tbm) && prefetchNodePtr != NULL) {
                prefetchNodePtr->blockNum = tbmpre->blockno;
                prefetchNodePtr->partOid = tbmpre->partitionOid;
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
            if (tbm_is_global(tbm)) {
                BitmapHeapPrefetchWithGpi(node, prefetchNow, prefetchNode, blockList);
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
                }
                prefetchRel = node->gpi_scan->fakePartRelation;
            }
            /* For posix_fadvise() we just send the one request */
            PrefetchBuffer(prefetchRel, MAIN_FORKNUM, tbmpre->blockno);
        }
        /* recover old oid after prefetch switch */
        GPISetCurrPartOid(node->gpi_scan, oldOid);
    }
    ADIO_END();
}
