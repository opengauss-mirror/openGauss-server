/* -------------------------------------------------------------------------
 *
 * nodeSeqscan.cpp
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeSeqscan.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecSeqScan				sequentially scans a relation.
 *		ExecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitSeqScan			creates and initializes a seqscan node.
 *		ExecEndSeqScan			releases any storage allocated.
 *		ExecReScanSeqScan		rescans the relation
 *		ExecSeqMarkPos			marks scan position
 *		ExecSeqRestrPos			restores scan position
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "executor/execdebug.h"
#include "executor/nodeModifyTable.h"
#include "executor/nodeSamplescan.h"
#include "executor/nodeSeqscan.h"
#include "pgxc/redistrib.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "nodes/execnodes.h"

static AbsTblScanDesc InitBeginScan(SeqScanState* node, Relation current_relation);

extern void StrategyGetRingPrefetchQuantityAndTrigger(BufferAccessStrategy strategy, int* quantity, int* trigger);
/* ----------------------------------------------------------------
 *		prefetch_pages
 *
 *		1,Offset and last here are used as signed  quantities that can be less than 0
 *		2,Put a kludge here to make the <0 check work, to prevent propagating a negative
 *		  number of pages to PageRangePrefetch()
 * ----------------------------------------------------------------
 */
void prefetch_pages(HeapScanDesc scan, BlockNumber start_block, BlockNumber offset)
{
    /* Open it at the smgr level if not already done */
    PageRangePrefetch(scan->rs_rd, MAIN_FORKNUM, start_block, offset, 0, 0);

    return;
}

/* ----------------------------------------------------------------
 *		Start_Prefetch
 *
 *		1,Prefetch interfaces for Sequential Scans
 *		2,Start_Prefetch() calculates the optimal prefetch distance and invokes
 *		  prefetch_pages() to invoke the prefetch interface
 *		3,only the ADIO prefetch is provided here now
 * ----------------------------------------------------------------
 */
void Start_Prefetch(HeapScanDesc scan, SeqScanAccessor* p_accessor, ScanDirection dir)
{
    BlockNumber start_pref, offset, last;
    uint32 quantity, trigger;
    bool forward = false;

    /* Disable prefetch on unsupported scans */
    if (p_accessor == NULL)
        return;  // cannot prefetch this scan

    if (scan->rs_nblocks == 0)
        return;

    quantity = p_accessor->sa_prefetch_quantity;
    trigger = p_accessor->sa_prefetch_trigger;
    last = p_accessor->sa_last_prefbf;
    forward = ScanDirectionIsForward(dir);

    if (last == InvalidBlockNumber) { // first prefetch
        if (forward) {
            if (scan->rs_cblock == InvalidBlockNumber) {
                start_pref = scan->rs_startblock + 1;
            } else {
                start_pref = scan->rs_cblock + 1;
            }

            offset = 2 * quantity;
            if (start_pref + offset > scan->rs_nblocks) {
                offset = scan->rs_nblocks - start_pref;
            }

            if (((int32)offset) > 0) {
                prefetch_pages(scan, start_pref, offset);
                last = start_pref + offset - 1;
            } else
                return;  // nothing to prefetch
        } else {
            offset = 2 * quantity;
            if (scan->rs_cblock == InvalidBlockNumber) {
                start_pref = scan->rs_nblocks;
            } else {
                start_pref = scan->rs_cblock;
            }

            if (start_pref > offset)
                start_pref = start_pref - offset;
            else {
                offset = start_pref;
                start_pref = 0;
            }
            if (((int32)offset) > 0) {
                prefetch_pages(scan, start_pref, offset);
                last = start_pref;
            } else
                return;  // nothing to prefetch
        }
    } else {
        if (scan->rs_cblock == InvalidBlockNumber)
            return;  // do nothing
        if (forward) {
            if (last >= scan->rs_nblocks - 1)
                return;  // nothing to prefetch

            if (scan->rs_cblock >= last - trigger) {
                start_pref = last + 1;
                offset = quantity;
                if (start_pref + offset > scan->rs_nblocks) {
                    offset = scan->rs_nblocks - start_pref;
                }

                prefetch_pages(scan, start_pref, offset);
                last = start_pref + offset - 1;
            }
        } else {
            if (((int32)last) <= 0)
                return;  // nothing to prefetch
            if (scan->rs_cblock < last + trigger) {
                start_pref = last - quantity;
                offset = quantity;
                if ((int32)start_pref < 0) {
                    start_pref = 0;
                    offset = last;
                }
                prefetch_pages(scan, start_pref, offset);
                last = start_pref;
            }
        }
    }
    if (last != p_accessor->sa_last_prefbf) {
        if (forward) {
            ereport(DEBUG1,
                (errmodule(MOD_ADIO),
                    errmsg("start forward prefetch for %s last pref(%u), current block(%u)",
                        RelationGetRelationName(scan->rs_rd),
                        last,
                        scan->rs_cblock)));
        } else {
            ereport(DEBUG1,
                (errmodule(MOD_ADIO),
                    errmsg("start backword prefetch for %s last pref(%u), current block(%u)",
                        RelationGetRelationName(scan->rs_rd),
                        last,
                        scan->rs_cblock)));
        }
    }
    p_accessor->sa_last_prefbf = last;
}

static TupleTableSlot* SeqNext(SeqScanState* node);

static void ExecInitNextPartitionForSeqScan(SeqScanState* node);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		SeqNext
 *
 *		This is a workhorse for ExecSeqScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot* SeqNext(SeqScanState* node)
{
    HeapTuple tuple;
    AbsTblScanDesc scanDesc;
    EState* estate = NULL;
    ScanDirection direction;
    TupleTableSlot* slot = NULL;

    /*
     * get information from the estate and scan state
     */
    scanDesc = node->ss_currentScanDesc;
    estate = node->ps.state;
    direction = estate->es_direction;
    slot = node->ss_ScanTupleSlot;

    if (scanDesc == NULL) {
        /*
         * We reach here if the scan is not parallel, or if we're executing
         * a scan that was intended to be parallel serially.
         * It must be a non-partitioned table.
         */
        Assert(!node->isPartTbl);
        scanDesc = InitBeginScan(node, node->ss_currentRelation);
        node->ss_currentScanDesc = scanDesc;
    }

    GetHeapScanDesc(scanDesc)->rs_ss_accessor = node->ss_scanaccessor;

    /*
     * get the next tuple from the table for seqscan.
     */
    tuple = abs_tbl_getnext(scanDesc, direction);

    ADIO_RUN()
    {
        Start_Prefetch(GetHeapScanDesc(scanDesc), node->ss_scanaccessor, direction);
    }
    ADIO_END();

    /*
     * save the tuple and the buffer returned to us by the access methods in
     * our scan tuple slot and return the slot.  Note: we pass 'false' because
     * tuples returned by heap_getnext() are pointers onto disk pages and were
     * not created with palloc() and so should not be pfree_ext()'d.  Note also
     * that ExecStoreTuple will increment the refcount of the buffer; the
     * refcount will not be dropped until the tuple table slot is cleared.
     */
    return ExecMakeTupleSlot(tuple, GetHeapScanDesc(scanDesc), slot);
}

/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool SeqRecheck(SeqScanState* node, TupleTableSlot* slot)
{
    /*
     * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
     * (and this is very bad) - so, here we do not check are keys ok or not.
     */
    return true;
}

/* ----------------------------------------------------------------
 *		ExecSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecSeqScan(SeqScanState* node)
{
    return ExecScan((ScanState*)node, node->ScanNextMtd, (ExecScanRecheckMtd)SeqRecheck);
}

/* ----------------------------------------------------------------
 *		SeqScan_Pref_Quantity
 *
 *		1,do init prefetch quantity and prefetch trigger for adio
 *		2,prefetch quantity can not exceed NBuffers / 4
 *		3,we set prefetch trigger equal to prefetch quantity, later we can use smart preftch policy,
 *		  if used ring policy, we need check ring size and correct prefetch quantity and trigger
 * ----------------------------------------------------------------
 */
void SeqScan_Pref_Quantity(AbsTblScanDesc scan, SeqScanAccessor* p_accessor)
{
    int threshold = (g_instance.attr.attr_storage.NBuffers / 4);
    int prefetch_trigger = u_sess->attr.attr_storage.prefetch_quantity;

    p_accessor->sa_prefetch_quantity = (uint32)rtl::min(threshold, prefetch_trigger);
    p_accessor->sa_prefetch_trigger = p_accessor->sa_prefetch_quantity;

    if (scan == NULL) {
        return;
    }
    BufferAccessStrategy bas = GetHeapScanDesc(scan)->rs_strategy;
    if (bas != NULL) {
        StrategyGetRingPrefetchQuantityAndTrigger(
            bas, (int*)&p_accessor->sa_prefetch_quantity, (int*)&p_accessor->sa_prefetch_trigger);
    }
}

/* ----------------------------------------------------------------
 *		SeqScan_Init
 *
 *		API funtion to init prefetch quantity and prefetch trigger for adio
 * ----------------------------------------------------------------
 */
void SeqScan_Init(AbsTblScanDesc scan, SeqScanAccessor* p_accessor)
{
    p_accessor->sa_last_prefbf = InvalidBlockNumber;
    p_accessor->sa_pref_trigbf = InvalidBlockNumber;
    SeqScan_Pref_Quantity(scan, p_accessor);
}
extern bool reset_scan_qual(Relation curr_heap_rel, ScanState* node)
{
    if (node == NULL) {
        return false;
    }
    if (u_sess->attr.attr_sql.enable_cluster_resize && RelationInRedistribute(curr_heap_rel)) {
        List* new_qual = eval_ctid_funcs(curr_heap_rel, node->ps.plan->qual, &node->isRangeScanInRedis);
        node->ps.qual = (List*)ExecInitExpr((Expr*)new_qual, (PlanState*)&node->ps);
        node->ps.qual_is_inited = true;
        return node->isRangeScanInRedis;
    }
    if (!node->ps.qual_is_inited) {
        node->ps.qual = (List*)ExecInitExpr((Expr*)node->ps.plan->qual, (PlanState*)&node->ps);
        node->ps.qual_is_inited = true;
    }
    return false;
}
static AbsTblScanDesc InitBeginScan(SeqScanState* node, Relation current_relation)
{
    AbsTblScanDesc current_scan_desc = NULL;
    if (!node->isSampleScan) {
        current_scan_desc = abs_tbl_beginscan(current_relation, node->ps.state->es_snapshot, 0, NULL, (ScanState*)node);
    } else {
        current_scan_desc = InitSampleScanDesc((ScanState*)node, current_relation);
    }
    return current_scan_desc;
}

/* ----------------------------------------------------------------
 *		InitScanRelation
 *
 *		Set up to access the scan relation.
 * ----------------------------------------------------------------
 */
void InitScanRelation(SeqScanState* node, EState* estate)
{
    Relation current_relation;
    Relation current_part_rel = NULL;
    SeqScan* plan = NULL;
    bool is_target_rel = false;
    LOCKMODE lockmode = AccessShareLock;
    AbsTblScanDesc current_scan_desc = NULL;

    is_target_rel = ExecRelationIsTargetRelation(estate, ((SeqScan*)node->ps.plan)->scanrelid);

    /*
     * get the relation object id from the relid'th entry in the range table,
     * open that relation and acquire appropriate lock on it.
     */
    current_relation = ExecOpenScanRelation(estate, ((SeqScan*)node->ps.plan)->scanrelid);
    if (!node->isPartTbl) {
        /*
         * For non-partitioned table, we will do InitBeginScan later to check whether we can do
         * parallel scan or not. Check ExecInitSeqScan and SeqNext for details.
         * But we still need to add qual here, otherwise ExecScan will get no qual.
         */
        (void)reset_scan_qual(current_relation, node);
    } else {
        plan = (SeqScan*)node->ps.plan;

        /* initiate partition list */
        node->partitions = NULL;

        if (!is_target_rel) {
            lockmode = AccessShareLock;
        } else {
            switch (estate->es_plannedstmt->commandType) {
                case CMD_UPDATE:
                case CMD_DELETE:
                case CMD_MERGE:
                    lockmode = RowExclusiveLock;
                    break;

                case CMD_SELECT:
                    lockmode = AccessShareLock;
                    break;

                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OPERATION),
                            errmodule(MOD_EXECUTOR),
                            errmsg("invalid operation %d on partition for seqscan, allowed are UPDATE/DELETE/SELECT",
                                estate->es_plannedstmt->commandType)));
                    break;
            }
        }
        node->lockMode = lockmode;

        if (plan->itrs > 0) {
            Partition part = NULL;
            Partition currentPart = NULL;
            ListCell* cell = NULL;
            List* part_seqs = plan->pruningInfo->ls_rangeSelectedPartitions;

            Assert(plan->itrs == part_seqs->length);

            foreach (cell, part_seqs) {
                Oid tablepartitionid = InvalidOid;
                int partSeq = lfirst_int(cell);

                tablepartitionid = getPartitionOidFromSequence(current_relation, partSeq);
                part = partitionOpen(current_relation, tablepartitionid, lockmode);
                node->partitions = lappend(node->partitions, part);
            }

            /* construct HeapScanDesc for first partition */
            currentPart = (Partition)list_nth(node->partitions, 0);
            current_part_rel = partitionGetRelation(current_relation, currentPart);
            node->ss_currentPartition = current_part_rel;

            /* add qual for redis */
            current_scan_desc = InitBeginScan(node, current_part_rel);
        } else {
            node->ss_currentPartition = NULL;
            node->ps.qual = (List*)ExecInitExpr((Expr*)node->ps.plan->qual, (PlanState*)&node->ps);
        }
    }

    node->ss_currentRelation = current_relation;
    node->ss_currentScanDesc = current_scan_desc;

    /* and report the scan tuple slot's rowtype */
    ExecAssignScanType(node, RelationGetDescr(current_relation));
}
static inline void InitSeqNextMtd(SeqScan* node, SeqScanState* scanstate)
{
    if (!node->tablesample) {
        scanstate->ScanNextMtd = SeqNext;
    } else {
        if (RELATION_OWN_BUCKET(scanstate->ss_currentRelation)) {
            scanstate->ScanNextMtd = HbktSeqSampleNext;
            return;
        }
        scanstate->ScanNextMtd = SeqSampleNext;
    }
}

/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
SeqScanState* ExecInitSeqScan(SeqScan* node, EState* estate, int eflags)
{
    TableSampleClause* tsc = NULL;

    /*
     * Once upon a time it was possible to have an outerPlan of a SeqScan, but
     * not any more.
     */
    Assert(outerPlan(node) == NULL);
    Assert(innerPlan(node) == NULL);

    /*
     * create state structure
     */
    SeqScanState* scanstate = makeNode(SeqScanState);

    scanstate->ps.plan = (Plan*)node;
    scanstate->ps.state = estate;
    scanstate->isPartTbl = node->isPartTbl;
    scanstate->currentSlot = 0;
    scanstate->partScanDirection = node->partScanDirection;
    scanstate->isRangeScanInRedis = false;

    if (!node->tablesample) {
        scanstate->isSampleScan = false;
    } else {
        scanstate->isSampleScan = true;
        tsc = node->tablesample;
    }

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &scanstate->ps);

    /*
     * initialize child expressions
     */
    scanstate->ps.targetlist = (List*)ExecInitExpr((Expr*)node->plan.targetlist, (PlanState*)scanstate);

    if (node->tablesample) {
        scanstate->sampleScanInfo.args = (List*)ExecInitExpr((Expr*)tsc->args, (PlanState*)scanstate);
        scanstate->sampleScanInfo.repeatable = ExecInitExpr(tsc->repeatable, (PlanState*)scanstate);

        scanstate->sampleScanInfo.sampleType = tsc->sampleType;
        /*
         * Initialize RowTableSample
         */
        if (scanstate->sampleScanInfo.tsm_state == NULL) {
            scanstate->sampleScanInfo.tsm_state = New(CurrentMemoryContext) RowTableSample(scanstate);
        }
    }

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &scanstate->ps);
    ExecInitScanTupleSlot(estate, scanstate);

    InitScanRelation(scanstate, estate);
    ADIO_RUN()
    {
        /* add prefetch related information */
        scanstate->ss_scanaccessor = (SeqScanAccessor*)palloc(sizeof(SeqScanAccessor));
        SeqScan_Init(scanstate->ss_currentScanDesc, scanstate->ss_scanaccessor);
    }
    ADIO_END();

    /*
     * initialize scan relation
     */
    InitSeqNextMtd(node, scanstate);
    if (IsValidScanDesc(scanstate->ss_currentScanDesc)) {
        abs_tbl_init_parallel_seqscan(
            scanstate->ss_currentScanDesc, scanstate->ps.plan->dop, scanstate->partScanDirection);
    } else {
        /*
         * For non-partitioned table, ss_currentScanDesc may be none cause we will try to do parallel.
         * Check InitScanRelation and SeqNext for details.
         */
        if (!node->isPartTbl) {
            scanstate->ps.stubType = PST_None;
        } else {
            scanstate->ps.stubType = PST_Scan;
        }
    }

    scanstate->ps.ps_TupFromTlist = false;

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(&scanstate->ps);
    ExecAssignScanProjectionInfo(scanstate);

    return scanstate;
}

    /* If not enough pages to divide into every worker. */
            /* If not range scan in redistribute, just start from 0. */

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void ExecEndSeqScan(SeqScanState* node)
{
    Relation relation;
    AbsTblScanDesc scanDesc;

    /*
     * get information from node
     */
    relation = node->ss_currentRelation;
    scanDesc = node->ss_currentScanDesc;

    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->ss_ScanTupleSlot);

    /*
     * close heap scan
     */
    if (scanDesc != NULL) {
        abs_tbl_endscan(scanDesc);
    }
    if (node->isPartTbl) {
        if (PointerIsValid(node->partitions)) {
            Assert(node->ss_currentPartition);
            releaseDummyRelation(&(node->ss_currentPartition));

            releasePartitionList(node->ss_currentRelation, &(node->partitions), NoLock);
        }
    }

    ADIO_RUN()
    {
        /* add prefetch related information */
        pfree_ext(node->ss_scanaccessor);
    }
    ADIO_END();

    /*
     * close the heap relation.
     */
    if (relation) {
        ExecCloseScanRelation(relation);
    }
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void ExecReScanSeqScan(SeqScanState* node)
{
    AbsTblScanDesc scan;

    if (node->isSampleScan) {
        /* Remember we need to do BeginSampleScan again (if we did it at all) */
        (((RowTableSample*)node->sampleScanInfo.tsm_state)->resetSampleScan)();
    }

    scan = node->ss_currentScanDesc;

    if (scan != NULL) {
        if (node->isPartTbl) {
            if (PointerIsValid(node->partitions)) {
                /* end scan the prev partition first, */
                abs_tbl_endscan(scan);

                /* finally init Scan for the next partition */
                ExecInitNextPartitionForSeqScan(node);

                scan = node->ss_currentScanDesc;
            }
        } else {
            abs_tbl_rescan(scan, NULL);
        }

        abs_tbl_init_parallel_seqscan(scan, node->ps.plan->dop, node->partScanDirection);
    }
    ExecScanReScan((ScanState*)node);
}

/* ----------------------------------------------------------------
 *      ExecSeqScanEstimate
 *
 *      estimates the space required to serialize seqscan node.
 * ----------------------------------------------------------------
 */
void ExecSeqScanEstimate(SeqScanState *node, ParallelContext *pcxt)
{
    EState *estate = node->ps.state;
    node->pscan_len = heap_parallelscan_estimate(estate->es_snapshot);
}

/* ----------------------------------------------------------------
 *      ExecSeqScanInitializeDSM
 *
 *      Set up a parallel heap scan descriptor.
 * ----------------------------------------------------------------
 */
void ExecSeqScanInitializeDSM(SeqScanState *node, ParallelContext *pcxt, int nodeid)
{
    EState *estate = node->ps.state;
    knl_u_parallel_context *cxt = (knl_u_parallel_context *)pcxt->seg;

    /* Here we can't use palloc, cause we have switch to old memctx in ExecInitParallelPlan */
    cxt->pwCtx->queryInfo.pscan[nodeid] = (ParallelHeapScanDesc)MemoryContextAllocZero(cxt->memCtx, node->pscan_len);
    heap_parallelscan_initialize(cxt->pwCtx->queryInfo.pscan[nodeid], node->pscan_len, node->ss_currentRelation,
        estate->es_snapshot);
    cxt->pwCtx->queryInfo.pscan[nodeid]->plan_node_id = node->ps.plan->plan_node_id;
    node->ss_currentScanDesc =
        (AbsTblScanDesc)heap_beginscan_parallel(node->ss_currentRelation, cxt->pwCtx->queryInfo.pscan[nodeid]);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void ExecSeqScanReInitializeDSM(SeqScanState* node, ParallelContext* pcxt)
{
    HeapScanDesc scan = (HeapScanDesc)node->ss_currentScanDesc;
    heap_parallelscan_reinitialize(scan->rs_parallel);
}

/* ----------------------------------------------------------------
 *      ExecSeqScanInitializeWorker
 *
 *      Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void ExecSeqScanInitializeWorker(SeqScanState *node, void *context)
{
    ParallelHeapScanDesc pscan = NULL;
    knl_u_parallel_context *cxt = (knl_u_parallel_context *)context;

    for (int i = 0; i < cxt->pwCtx->queryInfo.pscan_num; i++) {
        if (node->ps.plan->plan_node_id == cxt->pwCtx->queryInfo.pscan[i]->plan_node_id) {
            pscan = cxt->pwCtx->queryInfo.pscan[i];
            break;
        }
    }

    if (pscan == NULL) {
        ereport(ERROR, (errmsg("could not find plan info, plan node id:%d", node->ps.plan->plan_node_id)));
    }

    node->ss_currentScanDesc = (AbsTblScanDesc)heap_beginscan_parallel(node->ss_currentRelation, pscan);
}

/* ----------------------------------------------------------------
 * 		ExecSeqMarkPos(node)
 *
 * 		Marks scan position.
 * ----------------------------------------------------------------
 */
void ExecSeqMarkPos(SeqScanState* node)
{
    abs_tbl_markpos(node->ss_currentScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecSeqRestrPos
 *
 *		Restores scan position.
 * ----------------------------------------------------------------
 */
void ExecSeqRestrPos(SeqScanState* node)
{
    /*
     * Clear any reference to the previously returned tuple.  This is needed
     * because the slot is simply pointing at scan->rs_cbuf, which
     * heap_restrpos will change; we'd have an internally inconsistent slot if
     * we didn't do this.
     */
    (void)ExecClearTuple(node->ss_ScanTupleSlot);

    abs_tbl_restrpos(node->ss_currentScanDesc);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: This does the initialization for scan partition
 * Description	:
 * Input		:
 * Output	:
 * Notes		:
 */
static void ExecInitNextPartitionForSeqScan(SeqScanState* node)
{
    Partition currentpartition = NULL;
    Relation currentpartitionrel = NULL;

    int paramno;
    ParamExecData* param = NULL;
    SeqScan* plan = NULL;

    plan = (SeqScan*)node->ps.plan;

    /* get partition sequnce */
    paramno = plan->plan.paramno;
    param = &(node->ps.state->es_param_exec_vals[paramno]);
    node->currentSlot = (int)param->value;

    /* construct HeapScanDesc for new partition */
    currentpartition = (Partition)list_nth(node->partitions, node->currentSlot);
    currentpartitionrel = partitionGetRelation(node->ss_currentRelation, currentpartition);

    releaseDummyRelation(&(node->ss_currentPartition));
    node->ss_currentPartition = currentpartitionrel;

    /* add qual for redis */

    /* update partition scan-related fileds in SeqScanState  */
    node->ss_currentScanDesc = InitBeginScan(node, currentpartitionrel);
    ADIO_RUN()
    {
        SeqScan_Init(node->ss_currentScanDesc, node->ss_scanaccessor);
    }
    ADIO_END();
}
