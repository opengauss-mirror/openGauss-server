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
#include "storage/buf/bufmgr.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/pruning.h"

extern void StrategyGetRingPrefetchQuantityAndTrigger(BufferAccessStrategy strategy, int* quantity, int* trigger);
/* ----------------------------------------------------------------
 *		prefetch_pages
 *
 *		1,Offset and last here are used as signed  quantities that can be less than 0
 *		2,Put a kludge here to make the <0 check work, to prevent propagating a negative
 *		  number of pages to PageRangePrefetch()
 * ----------------------------------------------------------------
 */
void prefetch_pages(TableScanDesc scan, BlockNumber start_block, BlockNumber offset)
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
void Start_Prefetch(TableScanDesc scan, SeqScanAccessor* p_accessor, ScanDirection dir)
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
    Tuple tuple;
    TableScanDesc scanDesc;
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
    GetTableScanDesc(scanDesc, node->ss_currentRelation)->rs_ss_accessor = node->ss_scanaccessor;

    /*
     * get the next tuple from the table for seqscan.
     */
    tuple = scan_handler_tbl_getnext(scanDesc, direction, node->ss_currentRelation);

    ADIO_RUN()
    {
        Start_Prefetch(GetTableScanDesc(scanDesc, node->ss_currentRelation), node->ss_scanaccessor, direction);
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
    return ExecMakeTupleSlot(tuple, GetTableScanDesc(scanDesc, node->ss_currentRelation), slot, node->ss_currentRelation->rd_tam_type);
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
void SeqScan_Pref_Quantity(TableScanDesc scan, SeqScanAccessor* p_accessor, Relation relation)
{
    int threshold = (g_instance.attr.attr_storage.NBuffers / 4);
    int prefetch_trigger = u_sess->attr.attr_storage.prefetch_quantity;

    p_accessor->sa_prefetch_quantity = (uint32)rtl::min(threshold, prefetch_trigger);
    p_accessor->sa_prefetch_trigger = p_accessor->sa_prefetch_quantity;

    if (scan == NULL) {
        return;
    }
    BufferAccessStrategy bas = GetTableScanDesc(scan, relation)->rs_strategy;
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
void SeqScan_Init(TableScanDesc scan, SeqScanAccessor* p_accessor, Relation relation)
{
    p_accessor->sa_last_prefbf = InvalidBlockNumber;
    p_accessor->sa_pref_trigbf = InvalidBlockNumber;
    SeqScan_Pref_Quantity(scan, p_accessor, relation);
}

RangeScanInRedis reset_scan_qual(Relation curr_heap_rel, ScanState* node)
{
    if (node == NULL) {
        return {false, 0, 0};
    }
    if (u_sess->attr.attr_sql.enable_cluster_resize && RelationInRedistribute(curr_heap_rel)) {
        List* new_qual = eval_ctid_funcs(curr_heap_rel, node->ps.plan->qual, &node->rangeScanInRedis);
        node->ps.qual = (List*)ExecInitExpr((Expr*)new_qual, (PlanState*)&node->ps);
        node->ps.qual_is_inited = true;
    } else if (!node->ps.qual_is_inited) {
        node->ps.qual = (List*)ExecInitExpr((Expr*)node->ps.plan->qual, (PlanState*)&node->ps);
        node->ps.qual_is_inited = true;
        node->rangeScanInRedis = {false,0,0};
    }
    return node->rangeScanInRedis;
}

static TableScanDesc InitBeginScan(SeqScanState* node, Relation current_relation)
{
    TableScanDesc current_scan_desc = NULL;
    if (!node->isSampleScan) {
        current_scan_desc = scan_handler_tbl_beginscan(current_relation, node->ps.state->es_snapshot, 0, NULL, (ScanState*)node);
    } else {
        current_scan_desc = InitSampleScanDesc((ScanState*)node, current_relation);
    }
    return current_scan_desc;
}

/* ----------------------------------------------------------------
 *		InitScanRelation
 *
 *		This does the initialization for scan relations and
 *		subplans of scans.
 * ----------------------------------------------------------------
 */
void InitScanRelation(SeqScanState* node, EState* estate)
{
    Relation current_relation;
    Relation current_part_rel = NULL;
    SeqScan* plan = NULL;
    bool is_target_rel = false;
    LOCKMODE lockmode = AccessShareLock;
    TableScanDesc current_scan_desc = NULL;

    is_target_rel = ExecRelationIsTargetRelation(estate, ((SeqScan*)node->ps.plan)->scanrelid);

    /*
     * get the relation object id from the relid'th entry in the range table,
     * open that relation and acquire appropriate lock on it.
     */
    current_relation = ExecOpenScanRelation(estate, ((SeqScan*)node->ps.plan)->scanrelid);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &node->ps, current_relation->rd_tam_type);
    ExecInitScanTupleSlot(estate, node, current_relation->rd_tam_type);

    if (!node->isPartTbl) {
        /* add qual for redis */
        current_scan_desc = InitBeginScan(node, current_relation);
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
        /* Generate node->partitions if exists */ 
        if (plan->itrs > 0) {
            Partition part = NULL;
            PruningResult* resultPlan = NULL;

            if (plan->pruningInfo->expr != NULL) {
                resultPlan = GetPartitionInfo(plan->pruningInfo, estate, current_relation);
            } else {
                resultPlan = plan->pruningInfo;
            }

            ListCell* cell = NULL;
            List* part_seqs = resultPlan->ls_rangeSelectedPartitions;

            foreach (cell, part_seqs) {
                Oid tablepartitionid = InvalidOid;
                int partSeq = lfirst_int(cell);

                tablepartitionid = getPartitionOidFromSequence(current_relation, partSeq);
                part = partitionOpen(current_relation, tablepartitionid, lockmode);
                node->partitions = lappend(node->partitions, part);
            }
            if (resultPlan->ls_rangeSelectedPartitions != NULL) {
                node->part_id = resultPlan->ls_rangeSelectedPartitions->length;
            } else {
                node->part_id = 0;
            }
        }

        if (node->partitions != NIL) {
            /* construct HeapScanDesc for first partition */
            Partition currentPart = (Partition)list_nth(node->partitions, 0);
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
    scanstate->rangeScanInRedis = {false,0,0};

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
        SeqScan_Init(scanstate->ss_currentScanDesc, scanstate->ss_scanaccessor, scanstate->ss_currentRelation);
    }
    ADIO_END();

    /*
     * initialize scan relation
     */
    InitSeqNextMtd(node, scanstate);
    if (IsValidScanDesc(scanstate->ss_currentScanDesc)) {
        scan_handler_tbl_init_parallel_seqscan(scanstate->ss_currentScanDesc, 
            scanstate->ps.plan->dop, scanstate->partScanDirection);
    } else {
        scanstate->ps.stubType = PST_Scan;
    }

    scanstate->ps.ps_TupFromTlist = false;

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(
            &scanstate->ps,
            scanstate->ss_currentRelation->rd_tam_type);

    ExecAssignScanProjectionInfo(scanstate);

    return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void ExecEndSeqScan(SeqScanState* node)
{
    Relation relation;
    TableScanDesc scanDesc;

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
        scan_handler_tbl_endscan((TableScanDesc)scanDesc, relation);
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
    TableScanDesc scan;

    if (node->isSampleScan) {
        /* Remember we need to do BeginSampleScan again (if we did it at all) */
        (((RowTableSample*)node->sampleScanInfo.tsm_state)->resetSampleScan)();
    }

    scan = node->ss_currentScanDesc;
    if (node->isPartTbl) {
        if (PointerIsValid(node->partitions)) {
            /* end scan the prev partition first, */
            scan_handler_tbl_endscan(scan, node->ss_currentRelation);

            /* finally init Scan for the next partition */
            ExecInitNextPartitionForSeqScan(node);

            scan = node->ss_currentScanDesc;
        }
    } else {
        scan_handler_tbl_rescan(scan, NULL, node->ss_currentRelation);
    }

    scan_handler_tbl_init_parallel_seqscan(scan, node->ps.plan->dop, node->partScanDirection);
    ExecScanReScan((ScanState*)node);
}

/* ----------------------------------------------------------------
 *		ExecSeqMarkPos(node)
 *
 *		Marks scan position.
 * ----------------------------------------------------------------
 */
void ExecSeqMarkPos(SeqScanState* node)
{
    scan_handler_tbl_markpos(node->ss_currentScanDesc);
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

    scan_handler_tbl_restrpos(node->ss_currentScanDesc);
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

    int paramno = -1;
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
        SeqScan_Init(node->ss_currentScanDesc, node->ss_scanaccessor, currentpartitionrel);
    }
    ADIO_END();
}
