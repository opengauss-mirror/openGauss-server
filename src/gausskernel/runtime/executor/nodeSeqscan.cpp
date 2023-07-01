/* -------------------------------------------------------------------------
 *
 * nodeSeqscan.cpp
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
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
#include "catalog/pg_partition_fn.h"
#include "commands/cluster.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeModifyTable.h"
#include "executor/node/nodeSamplescan.h"
#include "executor/node/nodeSeqscan.h"
#include "pgxc/redistrib.h"
#include "miscadmin.h"
#include "storage/buf/bufmgr.h"
#include "storage/tcap.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/pruning.h"
#include "parser/parsetree.h"
#include "access/ustore/knl_uheap.h"
#include "access/ustore/knl_uscan.h"
#include "optimizer/var.h"
#include "optimizer/tlist.h"

static TupleTableSlot* ExecSeqScan(PlanState* state);
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

TupleTableSlot* SeqNext(SeqScanState* node);

static void ExecInitNextPartitionForSeqScan(SeqScanState* node);

template<TableAmType type, bool hashBucket, bool pushdown>
FORCE_INLINE
void seq_scan_getnext_template(TableScanDesc scan,  TupleTableSlot* slot, ScanDirection direction,
    bool* has_cur_xact_write)
{
    Tuple tuple;
    if(hashBucket) {
        /* fall back to orign slow function. */
        tuple =  scan_handler_tbl_getnext(scan, direction, NULL, has_cur_xact_write);
    } else if (pushdown) {
        tuple = ndp_tableam->scan_getnexttuple(scan, direction, slot);
    } else if(type == TAM_HEAP) {
        tuple =  (Tuple)heap_getnext(scan, direction, has_cur_xact_write);
    } else {
        tuple =  (Tuple)UHeapGetNext(scan, direction, has_cur_xact_write);
    }
    if (hashBucket) {
        scan = ((HBktTblScanDesc)scan)->currBktScan;
    }
    if (tuple != NULL) {
         Assert(slot != NULL);
         Assert(slot->tts_tupleDescriptor != NULL);
         slot->tts_tam_ops = GetTableAmRoutine(type);
         if (type == TAM_USTORE) {
             UHeapSlotStoreUHeapTuple((UHeapTuple)tuple, slot, false, false);
         } else {
             HeapTuple htup = (HeapTuple)tuple;
             heap_slot_store_heap_tuple(htup, slot, scan->rs_cbuf, false, false);
         }
    } else {
         ExecClearTuple(slot);
    }
}

void seq_scan_getnext(TableScanDesc scan,  TupleTableSlot* slot, ScanDirection direction, 
        bool* has_cur_xact_write)
{
    Tuple tuple;
    tuple =  (Tuple)heap_getnext(scan, direction, has_cur_xact_write);
    if (tuple != NULL) {
         Assert(slot != NULL);
         Assert(slot->tts_tupleDescriptor != NULL);
         slot->tts_tam_ops = GetTableAmRoutine(TAM_HEAP);
         heap_slot_store_heap_tuple((HeapTuple)tuple, slot, scan->rs_cbuf, false, false);
    } else {
         ExecClearTuple(slot);
    }
}

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
TupleTableSlot* SeqNext(SeqScanState* node)
{
    TableScanDesc scanDesc;
    EState* estate = NULL;
    TupleTableSlot* slot = NULL;

    /*
     * get information from the estate and scan state
     */
    scanDesc = node->ss_currentScanDesc;
    estate = node->ps.state;
    slot = node->ss_ScanTupleSlot;

    GetTableScanDesc(scanDesc, node->ss_currentRelation)->rs_ss_accessor = node->ss_scanaccessor;

    /*
     * get the next tuple from the table for seqscan.
     */
    node->fillNextSlotFunc(scanDesc, slot, estate->es_direction, &node->ps.state->have_current_xact_date);

    return slot;
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

template<TableAmType tableType>
void ExecStoreTupleBatchMode(TableScanDesc scanDesc, TupleTableSlot** slot)
{
    /* sanity checks */
    Assert(scanDesc != NULL);

    /*
     * save the tuple and the buffer returned to us by the access methods in
     * our scan tuple slots and return the slots in array mode. Note
     * that ExecStoreTupleBatch will increment the refcount of the buffer; the
     * refcount will not be dropped until the tuple table slot is cleared.
     */
    for (int i = 0; i < scanDesc->rs_ctupRows; i++) {
        if (tableType == TAM_USTORE) {
            UHeapSlotStoreUHeapTuple(((UHeapScanDesc)scanDesc)->rs_ctupBatch[i],
                slot[i], false, i != 0);
        } else {
            heap_slot_store_heap_tuple(&((HeapScanDesc)scanDesc)->rs_ctupBatch[i],
                slot[i], scanDesc->rs_cbuf, false, i != 0);
        }
    }
}

bool FetchEpqTupleBatchMode(ScanState* node)
{
    EState* estate = node->ps.state;
    Index scan_rel_id = ((Scan*)node->ps.plan)->scanrelid;
    Assert(scan_rel_id > 0);
 
    if (estate->es_epqTupleSet[scan_rel_id - 1]) {
        TupleTableSlot* slot = node->scanBatchState->scanBatch.scanTupleSlotInBatch[0];
 
        /* Return empty slot if we already returned a tuple */
        if (estate->es_epqScanDone[scan_rel_id - 1]) {
            (void)ExecClearTuple(slot);
            return true;
        }
        /* Else mark to remember that we shouldn't return more */
        estate->es_epqScanDone[scan_rel_id - 1] = true;
 
        /* Return empty slot if we haven't got a test tuple */
        if (estate->es_epqTuple[scan_rel_id - 1] == NULL) {
            (void)ExecClearTuple(slot);
            return true;
        }
 
        /* Store test tuple in the plan node's scan slot */
        (void)ExecStoreTuple(estate->es_epqTuple[scan_rel_id - 1], slot, InvalidBuffer, false);
 
        return true;
    }
 
    return false;
}
static ScanBatchResult *SeqNextBatchMode(SeqScanState *node)
{
    TableScanDesc scanDesc;
    EState *estate = node->ps.state;
    ScanDirection direction;
    TupleTableSlot **slot = &(node->scanBatchState->scanBatch.scanTupleSlotInBatch[0]);
 
    /* while inside an EvalPlanQual recheck, return a test tuple */
    if (estate->es_epqTuple != NULL && FetchEpqTupleBatchMode(node)) {
        if (TupIsNull(slot[0])) {
            node->scanBatchState->scanBatch.rows = 0;
            return NULL;
        } else {
            node->scanBatchState->scanBatch.rows = 1;
            return &node->scanBatchState->scanBatch;
        }
    }

    /* get information from the estate and scan state */
    scanDesc = node->ss_currentScanDesc;
    direction = estate->es_direction;

    /* get tuples from the table. */
    scanDesc->rs_maxScanRows = node->scanBatchState->scanTupleSlotMaxNum;
    node->scanBatchState->scanfinished = tableam_scan_gettuplebatchmode(scanDesc, direction);

    if (TTS_TABLEAM_IS_USTORE(slot[0])) {
        ExecStoreTupleBatchMode<TAM_USTORE>(scanDesc, slot);
    } else {
        ExecStoreTupleBatchMode<TAM_HEAP>(scanDesc, slot);
    }

    /*
     * As all tuples are from the same page, we only pins buffer for slot[0] in ExecStoreTupleBatch.
     * Here we reset slot[0] when scandesc->rs_ctupRows equals to zero.
     */
    if (scanDesc->rs_ctupRows == 0) {
        ExecClearTuple(slot[0]);
    }
    node->scanBatchState->scanBatch.rows = scanDesc->rs_ctupRows;
    Assert(scanDesc->rs_ctupRows <= BatchMaxSize);
    return &node->scanBatchState->scanBatch;
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
static TupleTableSlot* ExecSeqScan(PlanState* state)
{
    SeqScanState* node = castNode(SeqScanState, state);
    if (unlikely(node->scanBatchMode)) {
        return (TupleTableSlot *)SeqNextBatchMode(node);
    } else {
        return ExecScan((ScanState *) node, node->ScanNextMtd, (ExecScanRecheckMtd) SeqRecheck);
    }
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

RangeScanInRedis reset_scan_qual(Relation curr_heap_rel, ScanState* node, bool isRangeScanInRedis)
{
    if (node == NULL) {
        return {isRangeScanInRedis, 0, 0};
    }
    if (u_sess->attr.attr_sql.enable_cluster_resize && RelationInRedistribute(curr_heap_rel)) {
        List* new_qual = eval_ctid_funcs(curr_heap_rel, node->ps.plan->qual, &node->rangeScanInRedis);
        if (!node->scanBatchMode) {
            if (node->ps.state->es_is_flt_frame) {
                node->ps.qual = (List*)ExecInitQualByFlatten(new_qual, (PlanState*)&node->ps);
            } else {
                node->ps.qual = (List*)ExecInitExprByRecursion((Expr*)new_qual, (PlanState*)&node->ps);
            }
        } else {
            node->ps.qual = (List*)ExecInitVecExpr((Expr*)new_qual, (PlanState*)&node->ps);
        }
        node->ps.qual_is_inited = true;
    } else if (!node->ps.qual_is_inited) {
        if (!node->scanBatchMode) {
            if (node->ps.state->es_is_flt_frame) {
                node->ps.qual = (List*)ExecInitQualByFlatten(node->ps.plan->qual, (PlanState*)&node->ps);
            } else {
                node->ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->ps.plan->qual, (PlanState*)&node->ps);
            }
        } else {
            node->ps.qual = (List*)ExecInitVecExpr((Expr*)node->ps.plan->qual, (PlanState*)&node->ps);
        }
        node->ps.qual_is_inited = true;
        node->rangeScanInRedis = {false,0,0};
    }
    return node->rangeScanInRedis;
}

static TableScanDesc InitBeginScan(SeqScanState* node, Relation current_relation)
{
    TableScanDesc current_scan_desc = NULL;
    Snapshot scanSnap;

    /*
     * Choose user-specified snapshot if TimeCapsule clause exists, otherwise 
     * estate->es_snapshot instead.
     */
    scanSnap = TvChooseScanSnap(current_relation, (Scan *)node->ps.plan, (ScanState *)node);

    if (!node->isSampleScan) {
        current_scan_desc = scan_handler_tbl_beginscan(current_relation, 
            scanSnap, 0, NULL, (ScanState*)node);
    } else {
        current_scan_desc = InitSampleScanDesc((ScanState*)node, current_relation);
    }
    return current_scan_desc;
}

TableScanDesc BeginScanRelation(SeqScanState* node, Relation relation, TransactionId relfrozenxid64, int eflags)
{
    Snapshot scanSnap;
    TableScanDesc current_scan_desc = NULL;
    bool isUstoreRel = RelationIsUstoreFormat(relation);

    /*
     * Choose user-specified snapshot if TimeCapsule clause exists, otherwise 
     * estate->es_snapshot instead.
     */
    scanSnap = TvChooseScanSnap(relation, (Scan *)node->ps.plan, (ScanState *)node);

    if (isUstoreRel) {
        /*
         * Verify if a DDL operation that froze all tuples in the relation
         * occured after taking the snapshot. Skip for explain only commands.
         */
        if (!(eflags & EXEC_FLAG_EXPLAIN_ONLY)) {
            if (TransactionIdPrecedes(FirstNormalTransactionId, scanSnap->xmax) &&
                !TransactionIdIsCurrentTransactionId(relfrozenxid64) &&
                TransactionIdPrecedes(scanSnap->xmax, relfrozenxid64)) {
                ereport(ERROR, (errcode(ERRCODE_SNAPSHOT_INVALID),
                    (errmsg("Snapshot too old, ScanRelation, the info: snapxmax is %lu, "
                        "snapxmin is %lu, csn is %lu, relfrozenxid64 is %lu, globalRecycleXid is %lu.",
                        scanSnap->xmax, scanSnap->xmin, scanSnap->snapshotcsn, relfrozenxid64,
                        g_instance.undo_cxt.globalRecycleXid))));
            }
        }

        if (!node->isPartTbl) {
            (void)reset_scan_qual(relation, node);
        }

        current_scan_desc = UHeapBeginScan(relation, scanSnap, 0, NULL);
    } else {
        current_scan_desc = InitBeginScan(node, relation);
    }

    return current_scan_desc;
}

static PruningResult *GetPartitionPruningResultInInitScanRelation(SeqScan *plan, EState *estate,
                                                                  Relation current_relation)
{
    PruningResult *resultPlan = NULL;
    if (plan->pruningInfo->expr != NULL) {
        resultPlan = GetPartitionInfo(plan->pruningInfo, estate, current_relation);
    } else {
        resultPlan = plan->pruningInfo;
    }

    return resultPlan;
}

/* ----------------------------------------------------------------
 *		InitScanRelation
 *
 *		This does the initialization for scan relations and
 *		subplans of scans.
 * ----------------------------------------------------------------
 */
void InitScanRelation(SeqScanState* node, EState* estate, int eflags)
{
    Relation current_relation;
    Relation current_part_rel = NULL;
    SeqScan* plan = NULL;
    bool is_target_rel = false;
    LOCKMODE lockmode = AccessShareLock;
    TableScanDesc current_scan_desc = NULL;
    bool isUstoreRel = false;

    is_target_rel = ExecRelationIsTargetRelation(estate, ((SeqScan*)node->ps.plan)->scanrelid);

    /*
     * get the relation object id from the relid'th entry in the range table,
     * open that relation and acquire appropriate lock on it.
     */
    current_relation = ExecOpenScanRelation(estate, ((SeqScan*)node->ps.plan)->scanrelid);

    isUstoreRel = RelationIsUstoreFormat(current_relation);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &node->ps, current_relation->rd_tam_ops);
    ExecInitScanTupleSlot(estate, node, current_relation->rd_tam_ops);

    if (((Scan*)node->ps.plan)->tablesample && node->sampleScanInfo.tsm_state == NULL) {
        if (isUstoreRel) {
            node->sampleScanInfo.tsm_state = New(CurrentMemoryContext) UstoreTableSample((ScanState*)node);
        } else {
            node->sampleScanInfo.tsm_state = New(CurrentMemoryContext) RowTableSample((ScanState*)node);
        }
    }

    if (!node->isPartTbl) {
        /* add qual for redis */
        TransactionId relfrozenxid64 = InvalidTransactionId;
        getRelationRelxids(current_relation, &relfrozenxid64);
        current_scan_desc = BeginScanRelation(node, current_relation, relfrozenxid64, eflags);
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
            PruningResult* resultPlan = GetPartitionPruningResultInInitScanRelation(plan, estate, current_relation);

            ListCell* cell1 = NULL;
            ListCell* cell2 = NULL;
            List* part_seqs = resultPlan->ls_rangeSelectedPartitions;
            List* partitionnos = resultPlan->ls_selectedPartitionnos;
            Assert(list_length(part_seqs) == list_length(partitionnos));

            forboth (cell1, part_seqs, cell2, partitionnos) {
                Oid tablepartitionid = InvalidOid;
                int partSeq = lfirst_int(cell1);
                int partitionno = lfirst_int(cell2);
                List* subpartition = NIL;

                tablepartitionid = getPartitionOidFromSequence(current_relation, partSeq, partitionno);
                part = PartitionOpenWithPartitionno(current_relation, tablepartitionid, partitionno, lockmode);
                node->partitions = lappend(node->partitions, part);
                if (resultPlan->ls_selectedSubPartitions != NIL) {
                    Relation partRelation = partitionGetRelation(current_relation, part);
                    SubPartitionPruningResult *subPartPruningResult =
                        GetSubPartitionPruningResult(resultPlan->ls_selectedSubPartitions, partSeq, partitionno);
                    if (subPartPruningResult == NULL) {
                        continue;
                    }
                    List *subpart_seqs = subPartPruningResult->ls_selectedSubPartitions;
                    List *subpartitionnos = subPartPruningResult->ls_selectedSubPartitionnos;
                    Assert(list_length(subpart_seqs) == list_length(subpartitionnos));
                    ListCell *lc1 = NULL;
                    ListCell *lc2 = NULL;
                    forboth (lc1, subpart_seqs, lc2, subpartitionnos) {
                        Oid subpartitionid = InvalidOid;
                        int subpartSeq = lfirst_int(lc1);
                        int subpartitionno = lfirst_int(lc2);

                        subpartitionid = getPartitionOidFromSequence(partRelation, subpartSeq, subpartitionno);
                        Partition subpart =
                            PartitionOpenWithPartitionno(partRelation, subpartitionid, subpartitionno, lockmode);
                        subpartition = lappend(subpartition, subpart);
                    }
                    releaseDummyRelation(&(partRelation));
                    node->subPartLengthList = lappend_int(node->subPartLengthList, list_length(subpartition));
                    node->subpartitions = lappend(node->subpartitions, subpartition);
                }
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
            if (((Scan *)node->ps.plan)->partition_iterator_elimination) {
                if (node->ps.state->es_is_flt_frame) {
                    node->ps.qual = (List*)ExecInitQualByFlatten(node->ps.plan->qual, (PlanState*)&node->ps);
                } else {
                    node->ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->ps.plan->qual, (PlanState*)&node->ps);
                }
            }

            /* add qual for redis */
            TransactionId relfrozenxid64 = InvalidTransactionId;
            getPartitionRelxids(current_part_rel, &relfrozenxid64);
            current_scan_desc = BeginScanRelation(node, current_part_rel, relfrozenxid64, eflags);
        } else {
            node->ss_currentPartition = NULL;
            if (node->ps.state->es_is_flt_frame) {
                node->ps.qual = (List*)ExecInitQualByFlatten(node->ps.plan->qual, (PlanState*)&node->ps);
            } else {
                node->ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->ps.plan->qual, (PlanState*)&node->ps);
            }
        }
    }
    node->ss_currentRelation = current_relation;
    node->ss_currentScanDesc = current_scan_desc;

    ExecAssignScanType(node, RelationGetDescr(current_relation));
}

static void InitRelationBatchScanEnv(SeqScanState *state)
{
    /* so use MemContext which is not freed at all until the end. */
    ProjectionInfo *proj = state->ps.ps_ProjInfo;
    ScanBatchState* batchstate = state->scanBatchState;
    batchstate->maxcolId = 0;

    if (proj->pi_acessedVarNumbers == NIL) {
        return;
    }

    List *pColList = proj->pi_acessedVarNumbers;

    batchstate->colNum = list_length(pColList);
    batchstate->colAttr = (ScanBatchColAttr *)palloc0(sizeof(ScanBatchColAttr) * batchstate->colNum);

    int i = 0;
    ListCell *cell = NULL;

    /* Initilize which columns should be accessed */
    foreach (cell, pColList) {
        Assert(lfirst_int(cell) > 0);
        batchstate->colAttr[i].colId = lfirst_int(cell) - 1;
        batchstate->colAttr[i].lateRead = false;
        i++;
    }

    foreach (cell, proj->pi_projectVarNumbers) {
        int colId = lfirst_int(cell) - 1;
        for (i = 0; i < batchstate->colNum; ++i) {
            if (batchstate->colAttr[i].colId == colId) {
                batchstate->colAttr[i].isProject = true;
                break;
            }
        }
    }

    /* Intilize which columns will be late read */
    foreach (cell, proj->pi_lateAceessVarNumbers) {
        int colId = lfirst_int(cell) - 1;
        for (i = 0; i < batchstate->colNum; ++i) {
            if (batchstate->colAttr[i].colId == colId) {
                batchstate->colAttr[i].lateRead = true;
                break;
            }
        }
    }
}

static SeqScanState *ExecInitSeqScanBatchMode(SeqScan *node, SeqScanState* scanstate, EState* estate)
{
    /*
     * scanBatchMode is forced set to true if its parent node is RowToVec.
     * Here we check again to see whether batch mode scan is supported.
     */
    if (node->scanBatchMode) {
        Relation currentRelation = scanstate->ss_currentRelation;
        /*
         * batch mode only supports:
         * 1. the relation scan method is not sample.
         * 2. the columns in target list and qual are supported by column store.
         * 3. the relation is not a system relation.
         * 4. the relaseion is not a hash bucket relation.
         */
        if (node->tablesample ||
            !CheckColumnsSuportedByBatchMode(scanstate->ps.plan->targetlist, node->plan.qual) ||
            currentRelation->rd_id < FirstNormalObjectId ||
            RELATION_OWN_BUCKET(currentRelation)) {
            node->scanBatchMode = false;
        }
    }
    scanstate->scanBatchMode = node->scanBatchMode;

    /*
     * init variables for batch scan mode.
     * scanstate->ss_currentScanDesc is NULL means that scan will not execute.
     */
    if (node->scanBatchMode && scanstate->ss_currentScanDesc) {
        int i = 0;
        ScanBatchState *scanBatchState = (ScanBatchState*)palloc0(sizeof(ScanBatchState));
        scanstate->scanBatchState = scanBatchState;
        scanstate->ps.subPlan = NULL;

        if (scanstate->ss_currentRelation->rd_tam_ops == TableAmHeap) {
            HeapScanDesc heapDesc = (HeapScanDesc)(scanstate->ss_currentScanDesc);
            heapDesc->rs_ctupBatch = (HeapTupleData*)palloc(sizeof(HeapTupleData) * BatchMaxSize);
        } else {
            UHeapScanDesc uheapDesc = (UHeapScanDesc)(scanstate->ss_currentScanDesc);
            uheapDesc->rs_ctupBatch = (UHeapTuple*)palloc(sizeof(UHeapTuple) * BatchMaxSize);
        }

        scanBatchState->scanBatch.scanTupleSlotInBatch =
            (TupleTableSlot**)palloc(sizeof(TupleTableSlot*) * BatchMaxSize);
        for (i = 0; i < BatchMaxSize; i++) {
            TupleTableSlot* slot = ExecAllocTableSlot(&estate->es_tupleTable,
                                                      scanstate->ss_currentRelation->rd_tam_ops);
            ExecSetSlotDescriptor(slot, scanstate->ss_ScanTupleSlot->tts_tupleDescriptor);
            scanBatchState->scanBatch.scanTupleSlotInBatch[i] = slot;
        }

        /* prepare variables for batch mode scan, also can be found in ExecInitCStoreScan */
        ExecAssignVectorForExprEval(scanstate->ps.ps_ExprContext);

        scanstate->ps.targetlist = (List *)ExecInitVecExpr((Expr *)node->plan.targetlist, (PlanState *)scanstate);

        scanBatchState->pScanBatch =
            New(CurrentMemoryContext)VectorBatch(CurrentMemoryContext, scanstate->ss_currentRelation->rd_att);

        scanBatchState->nullflag = (bool*)palloc0(sizeof(bool) * scanBatchState->pScanBatch->m_cols);

        scanstate->ps.ps_ProjInfo =
            ExecBuildVecProjectionInfo(scanstate->ps.targetlist, node->plan.qual, scanstate->ps.ps_ExprContext,
            scanstate->ps.ps_ResultTupleSlot, scanstate->ss_ScanTupleSlot->tts_tupleDescriptor);

        scanstate->ps.qual = (List *)ExecInitVecExpr((Expr *)scanstate->ps.plan->qual, (PlanState *)&scanstate->ps);

        InitRelationBatchScanEnv(scanstate);
        for (i = 0; i < scanBatchState->colNum; i++) {
            scanBatchState->maxcolId = Max(scanBatchState->maxcolId, scanBatchState->colAttr[i].colId);
        }
        scanBatchState->maxcolId++;

        /* the follow code realize OptimizeProjectionAndFilter (scanstate); */
        ProjectionInfo *proj = NULL;
        bool fSimpleMap = false;
        proj = scanstate->ps.ps_ProjInfo;

        /* Check if it is simple without need to invoke projection code */
        fSimpleMap = proj->pi_directMap && 
            (scanstate->ps.ps_ResultTupleSlot->tts_tupleDescriptor->natts == proj->pi_numSimpleVars);
        scanstate->ps.ps_ProjInfo->pi_directMap = fSimpleMap;
        scanBatchState->scanfinished = false;
    }

    return scanstate;
}

static inline void InitSeqNextMtd(SeqScan* node, SeqScanState* scanstate)
{
    if (!node->tablesample) {
        scanstate->ScanNextMtd = SeqNext;
        if (scanstate->ss_currentScanDesc != NULL && scanstate->ss_currentScanDesc->ndp_pushdown_optimized) {
            if (RELATION_OWN_BUCKET(scanstate->ss_currentRelation)) {
                if (scanstate->ss_currentRelation->rd_tam_ops == TableAmHeap)
                    scanstate->fillNextSlotFunc = seq_scan_getnext_template<TAM_HEAP, true, true>;
                else
                    scanstate->fillNextSlotFunc = seq_scan_getnext_template<TAM_USTORE, true, true>;
            } else {
                if (scanstate->ss_currentRelation->rd_tam_ops == TableAmHeap)
                    scanstate->fillNextSlotFunc = seq_scan_getnext_template<TAM_HEAP, false, true>;
                else
                    scanstate->fillNextSlotFunc = seq_scan_getnext_template<TAM_USTORE, false, true>;
            }
        } else {
            if (RELATION_OWN_BUCKET(scanstate->ss_currentRelation)) {
                if (scanstate->ss_currentRelation->rd_tam_ops == TableAmHeap)
                    scanstate->fillNextSlotFunc = seq_scan_getnext_template<TAM_HEAP, true, false>;
                else
                    scanstate->fillNextSlotFunc = seq_scan_getnext_template<TAM_USTORE, true, false>;
            } else {
                if (scanstate->ss_currentRelation->rd_tam_ops == TableAmHeap)
                    scanstate->fillNextSlotFunc = seq_scan_getnext;
                else
                    scanstate->fillNextSlotFunc = seq_scan_getnext_template<TAM_USTORE, false, false>;
            }
        }
    } else {
        if (RELATION_OWN_BUCKET(scanstate->ss_currentRelation)) {
            scanstate->ScanNextMtd = HbktSeqSampleNext;
            return;
        }
        scanstate->ScanNextMtd = SeqSampleNext;
    }
}

/*
 * Extract column numbers (var numbers) from a flattened targetlist
 * Set valid found columns to false (not null) in boolArr
 */
static inline void FlatTLtoBool(const List* targetList, bool* boolArr, AttrNumber natts)
{
    ListCell* tl = NULL;
    foreach (tl, targetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(tl);
        Var* variable = (Var*)tle->expr;
        Assert(variable != NULL); /* if this happens we've messed up */
        if ((variable->varoattno > 0) && (variable->varoattno <= natts)) {
            boolArr[variable->varoattno - 1] = false; /* sometimes varattno in parent is different */
        }
        elog(DEBUG1, "PMZ-InitSeq FlatTLtoBool: varoattno: %d", variable->varoattno);
    }
}

/*
 * Given a seq scan node's quals, extract all valid column numbers
 * Set valid found columns to false (not null) in boolArr
 */
uint16 TraverseVars(List* qual, bool* boolArr, int natts, List* vars, ListCell* l)
{
    uint16 foundvars = 0;
    foreach (l, vars) {
        Var* var = (Var*)lfirst(l);
        AttrNumber varoattno = var->varoattno;
        if ((varoattno > 0) && (varoattno <= natts)) {
            elog(DEBUG1, "PMZ-newQual: qual found varoattno: %d", varoattno);
            boolArr[varoattno - 1] = false;
            foundvars++;
        } else {
            elog(DEBUG1, "PMZ-newQual: rejecting varoattno: %d", varoattno);
        }
    }
    return foundvars;
}

static inline bool QualtoBool(List* qual, bool* boolArr, int natts)
{
    bool flag = false;
    List* vars = NIL;
    ListCell* l = NULL;
    vars = pull_var_clause((Node*)qual, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);
    if (TraverseVars(qual, boolArr, natts, vars, l) > 0)
        flag = true;
    list_free_ext(vars);
    return flag;
}

/* create a new flattened tlist from targetlist, if valid then set attributes in boolArr */
static inline void TLtoBool(List* targetlist, bool* boolArr, AttrNumber natts)
{
    PVCAggregateBehavior myaggbehavior = PVC_RECURSE_AGGREGATES;
    PVCPlaceHolderBehavior myphbehavior = PVC_RECURSE_PLACEHOLDERS;
    List* flatlist = NULL;
    flatlist = flatten_tlist(targetlist, myaggbehavior, myphbehavior);
    if ((flatlist != NULL) && (flatlist->length > 0)) {
        FlatTLtoBool(flatlist, boolArr, natts);
    }
    list_free_ext(flatlist);
}

/* return total count of non-null attributes */
static inline int BoolNatts(const bool* boolArr, AttrNumber natts)
{
    AttrNumber result = 0;
    for (int i = 0; i < natts; i++) {
        if (boolArr[i] == false) {
            result++;
        }
    }
    return result;
}

/* calulate position of last column (non-inclusive) */
static inline AttrNumber LastVarPos(bool* boolArr, AttrNumber natts)
{
    AttrNumber lastVar = natts - 1;
    while ((boolArr[lastVar] == true) && (lastVar > -1)) {
        lastVar--;
    }
    lastVar++;
    return lastVar;
}

/* get simple vars in projinfo and set corresponding bool in boolArr */
static inline void SimpleVartoBool(bool* boolArr, AttrNumber natts, ProjectionInfo* psProjInfo)
{
    for (int i = 0; i<psProjInfo->pi_numSimpleVars; i++) {
        if ((psProjInfo->pi_varNumbers[i] > 0) && (psProjInfo->pi_varNumbers[i] <= natts)) {
            boolArr[psProjInfo->pi_varNumbers[i] - 1] = false;
        }
    }
}

static inline bool CheckSeqScanFallback(const AttrNumber natts, const AttrNumber selectAtts, const AttrNumber lastVar)
{
    return ((selectAtts > natts / 2) || (selectAtts == 0) || (lastVar >= (natts * 7 / 10)) || (lastVar <= 0));
}

/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
SeqScanState* ExecInitSeqScan(SeqScan* node, EState* estate, int eflags)
{
    TableSampleClause* tsc = NULL;
    int rc;

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
    scanstate->ps.ExecProcNode = ExecSeqScan;

    if (!node->tablesample) {
        scanstate->isSampleScan = false;
    } else {
        scanstate->isSampleScan = true;
        tsc = node->tablesample;
        node->scanBatchMode = false;
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
    if(!estate->es_is_flt_frame) {
        scanstate->ps.targetlist = (List*)ExecInitExpr((Expr*)node->plan.targetlist, (PlanState*)scanstate);
    }

    if (node->tablesample) {
        scanstate->sampleScanInfo.args = ExecInitExprList(tsc->args, (PlanState*)scanstate);
        scanstate->sampleScanInfo.repeatable = ExecInitExpr(tsc->repeatable, (PlanState*)scanstate);

        scanstate->sampleScanInfo.sampleType = tsc->sampleType;
    }

    /*
     * tuple table initialization
     */

    InitScanRelation(scanstate, estate, eflags);

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

    scanstate->ps.ps_vec_TupFromTlist = false;

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(
            &scanstate->ps,
            scanstate->ss_currentRelation->rd_tam_ops);

    ExecAssignScanProjectionInfo(scanstate);

    ExecInitSeqScanBatchMode(node, scanstate, estate);

    AttrNumber natts = scanstate->ss_ScanTupleSlot->tts_tupleDescriptor->natts;
    AttrNumber lastVar = -1;
    bool *isNullProj = NULL;

    /* if scanstate or scantupleslot are NULL immediately fallback */
    if ((scanstate == NULL) || (scanstate->ss_ScanTupleSlot == NULL))
        goto fallback;

    /* if partial sequential scan is disabled by GUC, or table is not ustore, or partition table, skip */
    if ((!u_sess->attr.attr_storage.enable_ustore_partial_seqscan) ||
        (scanstate->ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops != TableAmUstore) ||
        (scanstate->ss_currentScanDesc == NULL) || (scanstate->ss_currentScanDesc->rs_rd == NULL) ||
        (!RelationIsNonpartitioned(scanstate->ss_currentScanDesc->rs_rd)) ||
        (RelationIsPartition(scanstate->ss_currentScanDesc->rs_rd))) {
        elog(DEBUG1, "Partial seq scan disabled by GUC, or table is not UStore, or table is/has partition");
        goto fallback;
    }

    /* Get number of attributes (natts), allocate null array, initialize all as null  */

    elog(DEBUG1, "PMZ-InitScan: preparing scan on relation with %d total atts", natts);
    isNullProj = (bool *)palloc(sizeof(bool) * natts);
    rc = memset_s(isNullProj, natts, 1, natts);
    securec_check(rc, "\0", "\0");

    if ((scanstate->ps.plan->flatList != NULL) && (scanstate->ps.plan->flatList->length > 0)) {
        if (scanstate->ps.ps_ProjInfo != NULL) {
            TLtoBool(scanstate->ps.plan->targetlist, isNullProj, natts);
            SimpleVartoBool(isNullProj, natts, scanstate->ps.ps_ProjInfo);
        }

        /* likely got here from ExecInitAgg, expand flatList that was passed down */
        FlatTLtoBool(scanstate->ps.plan->flatList, isNullProj, natts);
        if (scanstate->ps.plan->targetlist->length < natts)
            if (scanstate->ps.plan->targetlist->length > scanstate->ps.plan->flatList->length) {
                TLtoBool(scanstate->ps.plan->targetlist, isNullProj, natts); /* parent unaware of 'HAVING' clause */
            }

        if ((scanstate->ps.plan->qual != NULL) && (scanstate->ps.plan->qual->length > 0)) /* query has qualifications */
            if (QualtoBool(scanstate->ps.plan->qual, isNullProj, natts) == false) {
                elog(DEBUG1, "PMZ-InitScan-wfl: triggering fallback because QualtoBool failed");
                goto fallback;
            }
        lastVar = LastVarPos(isNullProj, natts);
        AttrNumber selectAtts = BoolNatts(isNullProj, natts);
        elog(DEBUG1, "PMZ-InitScan-wfl: selectAtts=%d , lastVar=%d", selectAtts, lastVar);

        if (CheckSeqScanFallback(natts, selectAtts, lastVar)) {
            elog(DEBUG1, "PMZ-InitScan-wfl: fallback because no selected atts, or writing over 5/10 of atts, or "
                         "reading over 7/10 of src tuple");
            goto fallback;
        }

        scanstate->ss_currentScanDesc->lastVar = lastVar;
        scanstate->ss_currentScanDesc->boolArr = isNullProj;
        elog(DEBUG1, "PMZ-InitScan-wfl: partial seq scan with sort/aggregation,  %d selected atts, and lastVar %d",
            selectAtts, lastVar);
    /* ProjInfo may be null if parent node is aggregate */
    } else if (scanstate->ps.ps_ProjInfo != NULL) {
        /* a basic seq scan node may contain FuncExpr in tlist so check targetlist */
        if (scanstate->ps.ps_ProjInfo->pi_numSimpleVars > 0) {
            TLtoBool(scanstate->ps.plan->targetlist, isNullProj, natts);
            /* if query has quals then parse and add them */
            if ((scanstate->ps.plan->qual != NULL) && (scanstate->ps.plan->qual->length > 0)) {
                if (QualtoBool(scanstate->ps.plan->qual, isNullProj, natts) == false) {
                    elog(DEBUG1, "PMZ-InitScan-simple: triggering fallback because QualtoBool failed");
                    goto fallback;
                }
            }
            SimpleVartoBool(isNullProj, natts, scanstate->ps.ps_ProjInfo);
            lastVar = LastVarPos(isNullProj, natts);
            AttrNumber selectAtts = BoolNatts(isNullProj, natts);
            elog(DEBUG1, "PMZ-InitScan-simple: selectAtts=%d , lastVar=%d", selectAtts, lastVar);

            if (CheckSeqScanFallback(natts, selectAtts, lastVar)) {
                elog(DEBUG1, "PMZ-InitScan-simple: fallback because no selected atts, or writing over 5/10 of atts, or "
                             "reading over 7/10 of src tuple");
                goto fallback;
            }

            scanstate->ss_currentScanDesc->lastVar = lastVar;
            scanstate->ss_currentScanDesc->boolArr = isNullProj;

            elog(DEBUG1, "PMZ-InitScan-simple: regular seq scan with %d simple vars, %d selected atts, and lastVar %d",
                scanstate->ps.ps_ProjInfo->pi_numSimpleVars, selectAtts, lastVar);
        } else {
            elog(DEBUG1, "PMZ-InitScan-simple: triggering fallback because no simple vars");
            goto fallback;
        }
    } else {
        /* usually SELECT * (no flatlist from parent or proj info or sufficiently selective targetlist) */
        elog(DEBUG1,
            "PMZ-InitScan: fallback because no parent flatlist or proj info (probably SELECT *)");
    fallback:
        if (isNullProj != NULL) {
            pfree(isNullProj);
        }
        elog(DEBUG1, "PMZ-InitScan: using fallback mode for seq scan");
        if (scanstate->ss_currentScanDesc != NULL) {
            if (scanstate->ss_currentScanDesc->rs_rd != NULL &&
                RELATION_CREATE_BUCKET(scanstate->ss_currentScanDesc->rs_rd)) {
            } else {
                scanstate->ss_currentScanDesc->lastVar = -1;
                scanstate->ss_currentScanDesc->boolArr = NULL;
            }
        } else {
            ((Plan *) node)->flatList = NULL;
            elog(DEBUG1, "PMZ-InitScan: No scan descriptor");
        }
    }

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
        scan_handler_tbl_endscan((TableScanDesc)scanDesc);
    }
    if (node->isPartTbl) {
        if (PointerIsValid(node->partitions)) {
            Assert(node->ss_currentPartition);
            if (PointerIsValid(node->subpartitions)) {
                releaseDummyRelation(&(node->ss_currentPartition));
                ListCell* cell = NULL;
                int idx = 0;
                foreach (cell, node->partitions) {
                    Partition part = (Partition)lfirst(cell);
                    Relation partRel =  partitionGetRelation(node->ss_currentRelation, part);
                    List* subPartList = (List*)list_nth(node->subpartitions, idx);
                    releasePartitionList(partRel, &subPartList, NoLock);
                    releaseDummyRelation(&(partRel));
                    idx++;
                }
                releasePartitionList(node->ss_currentRelation, &(node->partitions), NoLock);
            } else {
                releaseDummyRelation(&(node->ss_currentPartition));
                releasePartitionList(node->ss_currentRelation, &(node->partitions), NoLock);
            }
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
    if (node->isPartTbl && !(((Scan *)node->ps.plan)->partition_iterator_elimination)) {
        if (PointerIsValid(node->partitions)) {
            /* end scan the prev partition first, */
            scan_handler_tbl_endscan(scan);

            /* finally init Scan for the next partition */
            ExecInitNextPartitionForSeqScan(node);
            scan = node->ss_currentScanDesc;
        }
    } else {
        scan_handler_tbl_rescan(scan, NULL, node->ss_currentRelation);
    }

    if ((scan != NULL) && (scan->rs_rd->rd_tam_ops == TableAmUstore)) {
        scan->lastVar = -1;
        scan->boolArr = NULL;
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
    Partition currentSubPartition = NULL;
    List* currentSubPartitionList = NULL;
    Relation currentSubPartitionRel = NULL;

    int paramno = -1;
    int subPartParamno = -1;
    ParamExecData* param = NULL;
    ParamExecData* SubPrtParam = NULL;
    SeqScan* plan = NULL;

    plan = (SeqScan*)node->ps.plan;

    /* get partition sequnce */
    paramno = plan->plan.paramno;
    param = &(node->ps.state->es_param_exec_vals[paramno]);
    node->currentSlot = (int)param->value;

    subPartParamno = plan->plan.subparamno;
    SubPrtParam = &(node->ps.state->es_param_exec_vals[subPartParamno]);

    /* construct HeapScanDesc for new partition */
    currentpartition = (Partition)list_nth(node->partitions, node->currentSlot);
    currentpartitionrel = partitionGetRelation(node->ss_currentRelation, currentpartition);

    releaseDummyRelation(&(node->ss_currentPartition));

    if (currentpartitionrel->partMap != NULL) {
        Assert(SubPrtParam != NULL);
        currentSubPartitionList = (List *)list_nth(node->subpartitions, node->currentSlot);
        currentSubPartition = (Partition)list_nth(currentSubPartitionList, (int)SubPrtParam->value);
        currentSubPartitionRel = partitionGetRelation(currentpartitionrel, currentSubPartition);
        releaseDummyRelation(&(currentpartitionrel));

        node->ss_currentPartition = currentSubPartitionRel;
        /* add qual for redis */

        /* update partition scan-related fileds in SeqScanState  */
        node->ss_currentScanDesc = InitBeginScan(node, currentSubPartitionRel);
        ADIO_RUN()
        {
            SeqScan_Init(node->ss_currentScanDesc, node->ss_scanaccessor, currentSubPartitionRel);
        }
        ADIO_END();
    } else {
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

    if (node->scanBatchMode) {
        if (node->ss_currentRelation->rd_tam_ops == TableAmHeap) {
            HeapScanDesc heapDesc = (HeapScanDesc)node->ss_currentScanDesc;
            heapDesc->rs_ctupBatch = (HeapTupleData*)palloc(sizeof(HeapTupleData) * BatchMaxSize);
        } else {
            UHeapScanDesc uheapDesc = (UHeapScanDesc)node->ss_currentScanDesc;
            uheapDesc->rs_ctupBatch = (UHeapTuple*)palloc(sizeof(UHeapTuple) * BatchMaxSize);
        }
    }
}
