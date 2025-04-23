/* -------------------------------------------------------------------------
 *
 * nodeAnnIndexscan.cpp
 *	  Routines to support indexed scans of relations
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeAnnIndexscan.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecAnnIndexScan			scans a relation using an ann index
 *		AnnIndexNext				retrieve next tuple using ann index
 *		ExecInitAnnIndexScan		creates and initializes state info.
 *		ExecReScanAnnIndexScan		rescans the ann indexed relation.
 *		ExecEndAnnIndexScan		releases all storage.
 *		ExecAnnIndexMarkPos		marks scan position.
 *		ExecAnnIndexRestrPos		restores scan position.
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "catalog/pg_partition_fn.h"
#include "commands/cluster.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeIndexscan.h"
#include "optimizer/clauses.h"
#include "storage/tcap.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/executer_gstrace.h"
#include "nodes/makefuncs.h"
#include "optimizer/pruning.h"
#include "executor/node/nodeAnnIndexscan.h"

static TupleTableSlot* ExecAnnIndexScan(PlanState* state);
static TupleTableSlot* AnnIndexNext(AnnIndexScanState* node);
static void ExecInitNextPartitionForAnnIndexScan(AnnIndexScanState* node);

/* ----------------------------------------------------------------
 *		AnnIndexNext
 *
 *		Retrieve a tuple from the IndexScan node's current_relation
 *		using the index specified in the IndexScanState information.
 * ----------------------------------------------------------------
 */
static TupleTableSlot* AnnIndexNext(AnnIndexScanState* node)
{
    EState* estate = NULL;
    ExprContext* econtext = NULL;
    ScanDirection direction;
    IndexScanDesc scandesc;
    HeapTuple tuple;
    TupleTableSlot* slot = NULL;
    bool isUstore = false;

    /*
     * extract necessary information from index scan node
     */
    estate = node->ss.ps.state;
    direction = estate->es_direction;
    /* flip direction if this is an overall backward scan */
    if (ScanDirectionIsBackward(((AnnIndexScan*)node->ss.ps.plan)->indexorderdir)) {
        if (ScanDirectionIsForward(direction))
            direction = BackwardScanDirection;
        else if (ScanDirectionIsBackward(direction))
            direction = ForwardScanDirection;
    }
    econtext = node->ss.ps.ps_ExprContext;
    slot = node->ss.ss_ScanTupleSlot;
    scandesc = node->iss_ScanDesc;

    isUstore = RelationIsUstoreFormat(node->ss.ss_currentRelation);

    /*
     * ok, now that we have what we need, fetch the next tuple.
     */
    // we should change abs_idx_getnext to call IdxScanAm(scan)->idx_getnext and channge .idx_getnext in g_HeapIdxAm to
    // IndexGetnextSlot
    while (true) {
        CHECK_FOR_INTERRUPTS();

        IndexScanDesc indexScan = GetIndexScanDesc(scandesc);
        if (isUstore) {
            if (!IndexGetnextSlot(scandesc, direction, slot, &node->ss.ps.state->have_current_xact_date)) {
                break;
            }
        } else {
            if ((tuple = scan_handler_idx_getnext(scandesc, direction, InvalidOid, InvalidBktId,
                                                  &node->ss.ps.state->have_current_xact_date)) == NULL) {
                break;
            }
            /* Update indexScan, because hashbucket may switch current index in scan_handler_idx_getnext */
            indexScan = GetIndexScanDesc(scandesc);
            /*
             * Store the scanned tuple in the scan tuple slot of the scan state.
             * Note: we pass 'false' because tuples returned by amgetnext are
             * pointers onto disk pages and must not be pfree_ext()'d.
             */
            (void)ExecStoreTuple(tuple, /* tuple to store */
                slot,                   /* slot to store in */
                indexScan->xs_cbuf,     /* buffer containing tuple */
                false);                 /* don't pfree */
        }

        /*
         * If the index was lossy, we have to recheck the index quals using
         * the fetched tuple.
         */
        if (indexScan->xs_recheck) {
            econtext->ecxt_scantuple = slot;
            ResetExprContext(econtext);
            if (!ExecQual(node->indexqualorig, econtext, false)) {
                /* Fails recheck, so drop it and loop back for another */
                InstrCountFiltered2(node, 1);
                continue;
            }
        }

        return slot;
    }

    /*
     * if we get here it means the index scan failed so we are at the end of
     * the scan..
     */
    return ExecClearTuple(slot);
}

/*
 * IndexRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool AnnIndexRecheck(AnnIndexScanState* node, TupleTableSlot* slot)
{
    ExprContext* econtext = NULL;

    /*
     * extract necessary information from index scan node
     */
    econtext = node->ss.ps.ps_ExprContext;

    /* Does the tuple meet the indexqual condition? */
    econtext->ecxt_scantuple = slot;

    ResetExprContext(econtext);

    return ExecQual(node->indexqualorig, econtext, false);
}

/* ----------------------------------------------------------------
 *		ExecAnnIndexScan(node)
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ExecAnnIndexScan(PlanState* state)
{
    AnnIndexScanState* node = castNode(AnnIndexScanState, state);
    /*
     * If we have runtime keys and they've not already been set up, do it now.
     */
    if (node->iss_NumRuntimeKeys != 0 && !node->iss_RuntimeKeysReady) {
        /*
         * set a flag for partitioned table, so we can deal with it specially
         * when we rescan the partitioned table
         */
        if (node->ss.isPartTbl) {
            if (PointerIsValid(node->ss.partitions)) {
                node->ss.ss_ReScan = true;
                ExecReScan((PlanState*)node);
            }
        } else {
            ExecReScan((PlanState*)node);
        }
    } else if (DB_IS_CMPT(B_FORMAT) && node->iss_NumRuntimeKeys != 0 && u_sess->parser_cxt.has_set_uservar) {
        ExprContext* econtext = node->iss_RuntimeContext;
        ResetExprContext(econtext);
        ExecIndexEvalRuntimeKeys(econtext, node->iss_RuntimeKeys, node->iss_NumRuntimeKeys);
    }

    return ExecScan(&node->ss, (ExecScanAccessMtd)AnnIndexNext, (ExecScanRecheckMtd)AnnIndexRecheck);
}

/* ----------------------------------------------------------------
 *		ExecReScanAnnIndexScan(node)
 *
 *		Recalculates the values of any scan keys whose value depends on
 *		information known at runtime, then rescans the indexed relation.
 *
 *		Updating the scan key was formerly done separately in
 *		ExecUpdateIndexScanKeys. Integrating it into ReScan makes
 *		rescans of indices and relations/general streams more uniform.
 * ----------------------------------------------------------------
 */
void ExecReScanAnnIndexScan(AnnIndexScanState* node)
{
    /*
     * For recursive-stream rescan, if number of RuntimeKeys not euqal zero,
     * just return without rescan.
     *
     * If we are doing runtime key calculations (ie, any of the index key
     * values weren't simple Consts), compute the new key values.  But first,
     * reset the context so we don't leak memory as each outer tuple is
     * scanned.  Note this assumes that we will recalculate *all* runtime keys
     * on each call.
     */
    if (node->iss_NumRuntimeKeys != 0) {
        if (node->ss.ps.state->es_recursive_next_iteration) {
            node->iss_RuntimeKeysReady = false;
            return;
        }

        ExprContext* econtext = node->iss_RuntimeContext;

        ResetExprContext(econtext);
        ExecIndexEvalRuntimeKeys(econtext, node->iss_RuntimeKeys, node->iss_NumRuntimeKeys);
    }
    node->iss_RuntimeKeysReady = true;

    /*
     * deal with partitioned table
     */
    bool partpruning = !RelationIsSubPartitioned(node->ss.ss_currentRelation) &&
        ENABLE_SQL_BETA_FEATURE(PARTITION_OPFUSION) && list_length(node->ss.partitions) == 1;
    /* if only one partition is scaned in indexscan, we don't need do rescan for partition */
    if (node->ss.isPartTbl && !partpruning) {
        /*
         * if node->ss.ss_ReScan = true, just do rescaning as non-partitioned
         * table; else switch to next partition for scaning.
         */
        if (node->ss.ss_ReScan ||
            (((Scan *)node->ss.ps.plan)->partition_iterator_elimination)) {
            /* reset the rescan falg */
            node->ss.ss_ReScan = false;
        } else {
            if (!PointerIsValid(node->ss.partitions)) {
                /*
                 * give up rescaning the index if there is no partition to scan
                 */
                return;
            }

            /* switch to next partition for scaning */
            Assert(PointerIsValid(node->iss_ScanDesc));
            scan_handler_idx_endscan(node->iss_ScanDesc);
            /*  initialize Scan for the next partition */
            ExecInitNextPartitionForAnnIndexScan(node);
            ExecScanReScan(&node->ss);
            return;
        }
    }

    /* reset index scan */
    scan_handler_idx_rescan(
        node->iss_ScanDesc, node->iss_ScanKeys, node->iss_NumScanKeys, node->iss_OrderByKeys, node->iss_NumOrderByKeys);

    ExecScanReScan(&node->ss);
}

/* ----------------------------------------------------------------
 *		ExecEndAnnIndexScan
 * ----------------------------------------------------------------
 */
void ExecEndAnnIndexScan(AnnIndexScanState* node)
{
    Relation index_relation_desc;
    IndexScanDesc index_scan_desc;
    Relation relation;

    /*
     * extract information from the node
     */
    index_relation_desc = node->iss_RelationDesc;
    index_scan_desc = node->iss_ScanDesc;
    relation = node->ss.ss_currentRelation;

    /*
     * Free the exprcontext(s) ... now dead code, see ExecFreeExprContext
     */
#ifdef NOT_USED
    ExecFreeExprContext(&node->ss.ps);
    if (node->iss_RuntimeContext)
        FreeExprContext(node->iss_RuntimeContext, true);
#endif

    /*
     * clear out tuple table slots
     */
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /*
     * close the index relation (no-op if we didn't open it)
     */
    if (index_scan_desc)
        scan_handler_idx_endscan(index_scan_desc);

    /*
     * close the index relation (no-op if we didn't open it)
     * close the index relation if the relation is non-partitioned table
     * close the index partitions and table partitions if the relation is
     * non-partitioned table
     */
    if (node->ss.isPartTbl) {
        if (PointerIsValid(node->iss_IndexPartitionList)) {
            Assert(PointerIsValid(index_relation_desc));
            Assert(PointerIsValid(node->ss.partitions));
            Assert(node->ss.partitions->length == node->iss_IndexPartitionList->length);

            Assert(PointerIsValid(node->iss_CurrentIndexPartition));
            releaseDummyRelation(&(node->iss_CurrentIndexPartition));

            Assert(PointerIsValid(node->ss.ss_currentPartition));
            releaseDummyRelation(&(node->ss.ss_currentPartition));

            if (RelationIsSubPartitioned(relation)) {
                releaseSubPartitionList(index_relation_desc, &(node->iss_IndexPartitionList), NoLock);
                releaseSubPartitionList(node->ss.ss_currentRelation, &(node->ss.subpartitions), NoLock);
            } else {
                /* close index partition */
                releasePartitionList(node->iss_RelationDesc, &(node->iss_IndexPartitionList), NoLock);
            }

            /* close table partition */
            releasePartitionList(node->ss.ss_currentRelation, &(node->ss.partitions), NoLock);
        }
    }

    if (index_relation_desc)
        index_close(index_relation_desc, NoLock);

    /*
     * close the heap relation.
     */
    ExecCloseScanRelation(relation);
}

/* ----------------------------------------------------------------
 *		ExecAnnIndexMarkPos
 * ----------------------------------------------------------------
 */
void ExecAnnIndexMarkPos(AnnIndexScanState* node)
{
    scan_handler_idx_markpos(node->iss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecAnnIndexRestrPos
 * ----------------------------------------------------------------
 */
void ExecAnnIndexRestrPos(AnnIndexScanState* node)
{
    scan_handler_idx_restrpos(node->iss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecAnnInitIndexScan
 *
 *		Initializes the index scan's state information, creates
 *		scan keys, and opens the base and index relations.
 *
 *		Note: index scans have 2 sets of state information because
 *			  we have to keep track of the base relation and the
 *			  index relation.
 * ----------------------------------------------------------------
 */
void ExecInitAnnIndexRelation(AnnIndexScanState* node, EState* estate, int eflags)
{
    AnnIndexScanState* index_state = node;
    Snapshot scanSnap;
    Relation current_relation = index_state->ss.ss_currentRelation;
    AnnIndexScan *index_scan = (AnnIndexScan *)node->ss.ps.plan;

    /*
     * Choose user-specified snapshot if TimeCapsule clause exists, otherwise
     * estate->es_snapshot instead.
     */
    scanSnap = TvChooseScanSnap(index_state->iss_RelationDesc, &index_scan->scan, &index_state->ss);

    /* deal with partition info */
    if (index_state->ss.isPartTbl) {
        index_state->iss_ScanDesc = NULL;

        if (index_scan->scan.itrs > 0) {
            Partition current_partition = NULL;
            Partition currentindex = NULL;

            /* Initialize table partition list and index partition list for following scan */
            ExecInitPartitionForAnnIndexScan(index_state, estate);

            if (index_state->ss.partitions != NIL) {
                /* construct a dummy relation with the first table partition for following scan */
                if (RelationIsSubPartitioned(current_relation)) {
                    Partition subOnePart = (Partition)list_nth(index_state->ss.partitions, index_state->ss.currentSlot);
                    List *currentSubpartList = (List *)list_nth(index_state->ss.subpartitions, 0);
                    List *currentindexlist = (List *)list_nth(index_state->iss_IndexPartitionList, 0);
                    current_partition = (Partition)list_nth(currentSubpartList, 0);
                    currentindex = (Partition)list_nth(currentindexlist, 0);
                    Relation subOnePartRel = partitionGetRelation(index_state->ss.ss_currentRelation, subOnePart);
                    index_state->ss.ss_currentPartition =
                        partitionGetRelation(subOnePartRel, current_partition);
                    releaseDummyRelation(&subOnePartRel);
                } else {
                    current_partition = (Partition)list_nth(index_state->ss.partitions, 0);
                    currentindex = (Partition)list_nth(index_state->iss_IndexPartitionList, 0);
                    index_state->ss.ss_currentPartition =
                        partitionGetRelation(index_state->ss.ss_currentRelation, current_partition);
                }

                index_state->iss_CurrentIndexPartition =
                    partitionGetRelation(index_state->iss_RelationDesc, currentindex);

                /*
                 * Verify if a DDL operation that froze all tuples in the relation
                 * occured after taking the snapshot.
                 */
                if (RelationIsUstoreFormat(index_state->ss.ss_currentPartition)) {
                    TransactionId relfrozenxid64 = InvalidTransactionId;
                    getPartitionRelxids(index_state->ss.ss_currentPartition, &relfrozenxid64);
                    if (TransactionIdPrecedes(FirstNormalTransactionId, scanSnap->xmax) &&
                        !TransactionIdIsCurrentTransactionId(relfrozenxid64) &&
                        TransactionIdPrecedes(scanSnap->xmax, relfrozenxid64)) {
                        ereport(ERROR, (errcode(ERRCODE_SNAPSHOT_INVALID),
                            (errmsg("Snapshot too old, IndexRelation is  PartTbl, the info: snapxmax is %lu, "
                                "snapxmin is %lu, csn is %lu, relfrozenxid64 is %lu, globalRecycleXid is %lu.",
                                scanSnap->xmax, scanSnap->xmin, scanSnap->snapshotcsn, relfrozenxid64,
                                g_instance.undo_cxt.globalRecycleXid))));
                    }
                }

                /* Initialize scan descriptor for partitioned table */
                index_state->iss_ScanDesc = scan_handler_idx_beginscan(index_state->ss.ss_currentPartition,
                    index_state->iss_CurrentIndexPartition,
                    scanSnap,
                    index_state->iss_NumScanKeys,
                    index_state->iss_NumOrderByKeys,
                    (ScanState*)index_state);
            }
        }
    } else {
        /*
         * Verify if a DDL operation that froze all tuples in the relation
         * occured after taking the snapshot.
         */
        if (RelationIsUstoreFormat(current_relation)) {
            TransactionId relfrozenxid64 = InvalidTransactionId;
            getRelationRelxids(current_relation, &relfrozenxid64);
            if (TransactionIdPrecedes(FirstNormalTransactionId, scanSnap->xmax) &&
                !TransactionIdIsCurrentTransactionId(relfrozenxid64) &&
                TransactionIdPrecedes(scanSnap->xmax, relfrozenxid64)) {
                ereport(ERROR, (errcode(ERRCODE_SNAPSHOT_INVALID),
                    (errmsg("Snapshot too old, IndexRelation is not  PartTbl, the info: snapxmax is %lu, "
                        "snapxmin is %lu, csn is %lu, relfrozenxid64 is %lu, globalRecycleXid is %lu.",
                        scanSnap->xmax, scanSnap->xmin, scanSnap->snapshotcsn, relfrozenxid64,
                        g_instance.undo_cxt.globalRecycleXid))));
            }
        }

        /*
         * Initialize scan descriptor.
         */
        index_state->iss_ScanDesc = scan_handler_idx_beginscan(current_relation,
            index_state->iss_RelationDesc,
            scanSnap,
            index_state->iss_NumScanKeys,
            index_state->iss_NumOrderByKeys,
            (ScanState*)index_state);
    }

    return;
}

AnnIndexScanState* ExecInitAnnIndexScan(AnnIndexScan* node, EState* estate, int eflags)
{
    AnnIndexScanState* index_state = NULL;
    Relation current_relation;
    bool relisTarget = false;

    gstrace_entry(GS_TRC_ID_ExecInitIndexScan);
    /*
     * create state structure
     */
    index_state = makeNode(AnnIndexScanState);
    index_state->ss.ps.plan = (Plan*)node;
    index_state->ss.ps.state = estate;
    index_state->ss.isPartTbl = node->scan.isPartTbl;
    index_state->ss.currentSlot = 0;
    index_state->ss.partScanDirection = node->indexorderdir;
    index_state->ss.ps.ExecProcNode = ExecAnnIndexScan;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &index_state->ss.ps);

    index_state->ss.ps.ps_vec_TupFromTlist = false;

    /*
     * initialize child expressions
     *
     * Note: we don't initialize all of the indexqual expression, only the
     * sub-parts corresponding to runtime keys (see below).  Likewise for
     * indexorderby, if any.  But the indexqualorig expression is always
     * initialized even though it will only be used in some uncommon cases ---
     * would be nice to improve that.  (Problem is that any SubPlans present
     * in the expression must be found now...)
     */
    if (estate->es_is_flt_frame) {
        index_state->ss.ps.qual = (List*)ExecInitQualByFlatten(node->scan.plan.qual, (PlanState*)index_state);
        index_state->indexqualorig = (List*)ExecInitQualByFlatten(node->indexqualorig, (PlanState*)index_state);
    } else {
        index_state->ss.ps.targetlist = (List*)ExecInitExprByRecursion(
            (Expr*)node->scan.plan.targetlist, (PlanState*)index_state);
        index_state->ss.ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->scan.plan.qual, (PlanState*)index_state);
        index_state->indexqualorig =
            (List*)ExecInitExprByRecursion((Expr*)node->indexqualorig, (PlanState*)index_state);
    }

    /*
     * open the base relation and acquire appropriate lock on it.
     */
    current_relation = ExecOpenScanRelation(estate, node->scan.scanrelid);

    index_state->ss.ss_currentRelation = current_relation;
    index_state->ss.ss_currentScanDesc = NULL; /* no heap scan here */
    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &index_state->ss.ps, current_relation->rd_tam_ops);
    ExecInitScanTupleSlot(estate, &index_state->ss, current_relation->rd_tam_ops);

    /*
     * get the scan type from the relation descriptor.
     */
    ExecAssignScanType(&index_state->ss, CreateTupleDescCopy(RelationGetDescr(current_relation)));
    index_state->ss.ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops = current_relation->rd_tam_ops;

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(&index_state->ss.ps);

    index_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->td_tam_ops =
            index_state->ss.ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops;

    ExecAssignScanProjectionInfo(&index_state->ss);

    Assert(index_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->td_tam_ops);

    /*
     * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
     * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
     * references to nonexistent indexes.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
        gstrace_exit(GS_TRC_ID_ExecInitIndexScan);
        return index_state;
    }

    /*
     * Open the index relation.
     *
     * If the parent table is one of the target relations of the query, then
     * InitPlan already opened and write-locked the index, so we can avoid
     * taking another lock here.  Otherwise we need a normal reader's lock.
     */
    relisTarget = ExecRelationIsTargetRelation(estate, node->scan.scanrelid);
    index_state->iss_RelationDesc = index_open(node->indexid, relisTarget ? NoLock : AccessShareLock);
    if (!IndexIsUsable(index_state->iss_RelationDesc->rd_index)) {
        ereport(ERROR,
            (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("can't initialize index scans using unusable index \"%s\"",
                    RelationGetRelationName(index_state->iss_RelationDesc))));
    }
    /*
     * Initialize index-specific scan state
     */
    index_state->iss_RuntimeKeysReady = false;
    index_state->iss_RuntimeKeys = NULL;
    index_state->iss_NumRuntimeKeys = 0;

    /*
     * build the index scan keys from the index qualification
     */
    ExecIndexBuildScanKeys((PlanState*)index_state,
        index_state->iss_RelationDesc,
        node->indexqual,
        false,
        &index_state->iss_ScanKeys,
        &index_state->iss_NumScanKeys,
        &index_state->iss_RuntimeKeys,
        &index_state->iss_NumRuntimeKeys,
        NULL, /* no ArrayKeys */
        NULL);

    /*
     * any ORDER BY exprs have to be turned into scankeys in the same way
     */
    ExecIndexBuildScanKeys((PlanState*)index_state,
        index_state->iss_RelationDesc,
        node->indexorderby,
        true,
        &index_state->iss_OrderByKeys,
        &index_state->iss_NumOrderByKeys,
        &index_state->iss_RuntimeKeys,
        &index_state->iss_NumRuntimeKeys,
        NULL, /* no ArrayKeys */
        NULL);

    /*
     * If we have runtime keys, we need an ExprContext to evaluate them. The
     * node's standard context won't do because we want to reset that context
     * for every tuple.  So, build another context just like the other one...
     * -tgl 7/11/00
     */
    if (index_state->iss_NumRuntimeKeys != 0) {
        ExprContext* stdecontext = index_state->ss.ps.ps_ExprContext;

        ExecAssignExprContext(estate, &index_state->ss.ps);
        index_state->iss_RuntimeContext = index_state->ss.ps.ps_ExprContext;
        index_state->ss.ps.ps_ExprContext = stdecontext;
    } else {
        index_state->iss_RuntimeContext = NULL;
    }

    /* deal with partition info */
    ExecInitAnnIndexRelation(index_state, estate, eflags);

    /*
     * If no run-time keys to calculate, go ahead and pass the scankeys to the
     * index AM.
     */
    if (index_state->iss_ScanDesc == NULL) {
        index_state->ss.ps.stubType = PST_Scan;
    } else if (index_state->iss_NumRuntimeKeys == 0) {
        scan_handler_idx_rescan_local(index_state->iss_ScanDesc,
            index_state->iss_ScanKeys,
            index_state->iss_NumScanKeys,
            index_state->iss_OrderByKeys,
            index_state->iss_NumOrderByKeys);
    }

    /*
     * all done.
     */
    gstrace_exit(GS_TRC_ID_ExecInitIndexScan);
    return index_state;
}

/*
 * @@GaussDB@@
 * Target	: data partition
 * Brief	: construt a dummy relation with the next partition and the partitiobed
 *			: table for the following AnnIndexscan, and swith the scaning relation to
 *			: the dummy relation
 * Description	: input a AnnIndexScanState node, to construct a dummy relation
 *              : with the next partition.
 * Input		: AnnIndexScanState *node
 * Output	    : void
 * Notes		: NULL
 */
static void ExecInitNextPartitionForAnnIndexScan(AnnIndexScanState* node)
{
    Partition current_partition = NULL;
    Relation current_partition_rel = NULL;
    Partition current_index_partition = NULL;
    Relation current_index_partition_rel = NULL;
    AnnIndexScan* plan = NULL;
    int paramNo = -1;
    ParamExecData* param = NULL;
    int subPartParamno = -1;
    ParamExecData* SubPrtParam = NULL;

    AnnIndexScanState* indexState = node;
    AnnIndexScan *indexScan = (AnnIndexScan *)node->ss.ps.plan;
    Snapshot scanSnap = TvChooseScanSnap(indexState->iss_RelationDesc, &indexScan->scan, &indexState->ss);

    plan = (AnnIndexScan*)(node->ss.ps.plan);

    /* get partition sequnce */
    paramNo = plan->scan.plan.paramno;
    param = &(node->ss.ps.state->es_param_exec_vals[paramNo]);
    node->ss.currentSlot = (int)param->value;

    subPartParamno = plan->scan.plan.subparamno;
    SubPrtParam = &(node->ss.ps.state->es_param_exec_vals[subPartParamno]);

    Oid heapOid = node->iss_RelationDesc->rd_index->indrelid;
    Relation heapRelation = heap_open(heapOid, AccessShareLock);

    /* no heap scan here */
    node->ss.ss_currentScanDesc = NULL;

    /* construct a dummy relation with the next index partition */
    if (RelationIsSubPartitioned(heapRelation)) {
        Partition subOnePart = (Partition)list_nth(node->ss.partitions, node->ss.currentSlot);
        List *subPartList = (List *)list_nth(node->ss.subpartitions, node->ss.currentSlot);
        List *subIndexList = (List *)list_nth(node->iss_IndexPartitionList,
                                              node->ss.currentSlot);
        current_partition = (Partition)list_nth(subPartList, (int)SubPrtParam->value);

        Relation subOnePartRel = partitionGetRelation(node->ss.ss_currentRelation, subOnePart);

        current_partition_rel = partitionGetRelation(subOnePartRel, current_partition);
        current_index_partition = (Partition)list_nth(subIndexList, (int)SubPrtParam->value);
        releaseDummyRelation(&subOnePartRel);
    } else {
        current_partition = (Partition)list_nth(node->ss.partitions, node->ss.currentSlot);
        current_partition_rel = partitionGetRelation(node->ss.ss_currentRelation, current_partition);
        current_index_partition = (Partition)list_nth(node->iss_IndexPartitionList, node->ss.currentSlot);
    }

    current_index_partition_rel = partitionGetRelation(node->iss_RelationDesc, current_index_partition);

    Assert(PointerIsValid(node->iss_CurrentIndexPartition));
    releaseDummyRelation(&(node->iss_CurrentIndexPartition));
    node->iss_CurrentIndexPartition = current_index_partition_rel;

    /* update scan-related partition */
    releaseDummyRelation(&(node->ss.ss_currentPartition));
    node->ss.ss_currentPartition = current_partition_rel;

    /* Initialize scan descriptor. */
    node->iss_ScanDesc = scan_handler_idx_beginscan(node->ss.ss_currentPartition,
        node->iss_CurrentIndexPartition,
        scanSnap,
        node->iss_NumScanKeys,
        node->iss_NumOrderByKeys,
        (ScanState*)node);

    if (node->iss_ScanDesc != NULL) {
        scan_handler_idx_rescan_local(
            node->iss_ScanDesc, node->iss_ScanKeys, node->iss_NumScanKeys,
            node->iss_OrderByKeys, node->iss_NumOrderByKeys);
    }

    heap_close(heapRelation, AccessShareLock);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: get index partitions list and table partitions list for the
 *			    : the following AnnIndexScan
 * Description	: Init partitions list in AnnIndexScanState.
 * Input		: AnnIndexScanState* index_state, EState* estate
 * Output	    : void
 * Notes		: NULL
 */
void ExecInitPartitionForAnnIndexScan(AnnIndexScanState* index_state, EState* estate)
{
    IndexScan* plan = NULL;
    Relation current_relation = NULL;
    Partition table_partition = NULL;
    Partition index_partition = NULL;

    index_state->ss.partitions = NIL;
    index_state->ss.ss_currentPartition = NULL;
    index_state->iss_IndexPartitionList = NIL;
    index_state->iss_CurrentIndexPartition = NULL;

    plan = (IndexScan*)index_state->ss.ps.plan;
    current_relation = index_state->ss.ss_currentRelation;

    if (plan->scan.itrs > 0) {
        Oid indexid = plan->indexid;
        bool relisTarget = false;
        Partition indexpartition = NULL;
        LOCKMODE lock;

        /*
         * get relation's lockmode that hangs on whether
         * it's one of the target relations of the query
         */
        relisTarget = ExecRelationIsTargetRelation(estate, plan->scan.scanrelid);
        lock = (relisTarget ? RowExclusiveLock : AccessShareLock);
        index_state->ss.lockMode = lock;
        index_state->lockMode = lock;
        
        PruningResult* resultPlan = NULL;
        
        if (plan->scan.pruningInfo->expr != NULL) {
            resultPlan = GetPartitionInfo(plan->scan.pruningInfo, estate, current_relation);
        } else {
            resultPlan = plan->scan.pruningInfo;
        }
        
        if (resultPlan->ls_rangeSelectedPartitions != NULL) {
            index_state->ss.part_id = resultPlan->ls_rangeSelectedPartitions->length;
        } else {
            index_state->ss.part_id = 0;
        }

        ListCell* cell1 = NULL;
        ListCell* cell2 = NULL;
        List* part_seqs = resultPlan->ls_rangeSelectedPartitions;
        List* partitionnos = resultPlan->ls_selectedPartitionnos;
        Assert(list_length(part_seqs) == list_length(partitionnos));
        StringInfo partNameInfo = makeStringInfo();
        StringInfo partOidInfo = makeStringInfo();

        forboth (cell1, part_seqs, cell2, partitionnos) {
            Oid tablepartitionid = InvalidOid;
            Oid indexpartitionid = InvalidOid;
            List* partitionIndexOidList = NIL;
            int partSeq = lfirst_int(cell1);
            int partitionno = lfirst_int(cell2);

            /* get table partition and add it to a list for following scan */
            tablepartitionid = getPartitionOidFromSequence(current_relation, partSeq, partitionno);
            table_partition =
                PartitionOpenWithPartitionno(current_relation, tablepartitionid, partitionno, lock, true);
            /* Skip concurrent dropped partitions */
            if (table_partition == NULL) {
                continue;
            }
            index_state->ss.partitions = lappend(index_state->ss.partitions, table_partition);

            appendStringInfo(partNameInfo, "%s ", table_partition->pd_part->relname.data);
            appendStringInfo(partOidInfo, "%u ", tablepartitionid);

            if (RelationIsSubPartitioned(current_relation)) {
                ListCell *lc1 = NULL;
                ListCell *lc2 = NULL;
                SubPartitionPruningResult* subPartPruningResult =
                    GetSubPartitionPruningResult(resultPlan->ls_selectedSubPartitions, partSeq, partitionno);
                if (subPartPruningResult == NULL) {
                    continue;
                }
                List *subpartList = subPartPruningResult->ls_selectedSubPartitions;
                List *subpartitionnos = subPartPruningResult->ls_selectedSubPartitionnos;
                Assert(list_length(subpartList) == list_length(subpartitionnos));
                List *subIndexList = NULL;
                List *subRelationList = NULL;

                forboth (lc1, subpartList, lc2, subpartitionnos)
                {
                    int subpartSeq = lfirst_int(lc1);
                    int subpartitionno = lfirst_int(lc2);
                    Relation tablepartrel = partitionGetRelation(current_relation, table_partition);
                    Oid subpartitionid = getPartitionOidFromSequence(tablepartrel, subpartSeq, subpartitionno);
                    Partition subpart = PartitionOpenWithPartitionno(tablepartrel, subpartitionid, subpartitionno,
                                                                     AccessShareLock, true);
                    /* Skip concurrent dropped partitions */
                    if (subpart == NULL) {
                        continue;
                    }

                    partitionIndexOidList = PartitionGetPartIndexList(subpart);

                    Assert(partitionIndexOidList != NULL);
                    if (!PointerIsValid(partitionIndexOidList)) {
                        ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_WRONG_OBJECT_TYPE),
                                        errmsg("no local indexes found for partition %s",
                                               PartitionGetPartitionName(subpart))));
                    }

                    indexpartitionid = searchPartitionIndexOid(indexid, partitionIndexOidList);
                    list_free_ext(partitionIndexOidList);
                    indexpartition = partitionOpen(index_state->iss_RelationDesc, indexpartitionid, AccessShareLock);

                    releaseDummyRelation(&tablepartrel);

                    if (indexpartition->pd_part->indisusable == false) {
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_INDEX_CORRUPTED), errmodule(MOD_EXECUTOR),
                             errmsg(
                                 "can't initialize bitmap index scans using unusable local index \"%s\" for partition",
                                 PartitionGetPartitionName(indexpartition))));
                    }

                    subIndexList = lappend(subIndexList, indexpartition);
                    subRelationList = lappend(subRelationList, subpart);
                }

                index_state->iss_IndexPartitionList = lappend(index_state->iss_IndexPartitionList,
                                                              subIndexList);
                index_state->ss.subpartitions = lappend(index_state->ss.subpartitions, subRelationList);
                index_state->ss.subPartLengthList =
                    lappend_int(index_state->ss.subPartLengthList, list_length(subIndexList));
            } else {
                /* get index partition and add it to a list for following scan */
                partitionIndexOidList = PartitionGetPartIndexList(table_partition);
                Assert(PointerIsValid(partitionIndexOidList));
                if (!PointerIsValid(partitionIndexOidList)) {
                    ereport(ERROR,
                            (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("no local indexes found for partition %s",
                                                                        PartitionGetPartitionName(table_partition))));
                }
                indexpartitionid = searchPartitionIndexOid(indexid, partitionIndexOidList);
                list_free_ext(partitionIndexOidList);

                index_partition = partitionOpen(index_state->iss_RelationDesc, indexpartitionid, lock);
                if (index_partition->pd_part->indisusable == false) {
                    ereport(ERROR,
                            (errcode(ERRCODE_INDEX_CORRUPTED),
                             errmsg("can't initialize index scans using unusable local index \"%s\"",
                                    PartitionGetPartitionName(index_partition))));
                }
                index_state->iss_IndexPartitionList = lappend(index_state->iss_IndexPartitionList, index_partition);
            }
        }
        /*
         * Set the total scaned num of partition from level 1 partition, subpartition
         * list is drilled down into node->subpartitions for each node_partition entry;
         *
         * Note: we do not set is value from select partittins from pruning-result as some of
         *       pre-pruned partitions could be dropped from conecurrent DDL, node->partitions
         *       is refreshed partition list to be scanned;
         */
        if (index_state->ss.partitions != NULL) {
            index_state->ss.part_id = list_length(index_state->ss.partitions);
        } else {
            index_state->ss.part_id = 0;
        }
    }
}