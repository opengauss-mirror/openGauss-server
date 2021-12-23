/* -------------------------------------------------------------------------
 *
 * nodeIndexscan.cpp
 *	  Routines to support indexed scans of relations
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeIndexscan.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecIndexScan			scans a relation using an index
 *		IndexNext				retrieve next tuple using index
 *		ExecInitIndexScan		creates and initializes state info.
 *		ExecReScanIndexScan		rescans the indexed relation.
 *		ExecEndIndexScan		releases all storage.
 *		ExecIndexMarkPos		marks scan position.
 *		ExecIndexRestrPos		restores scan position.
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
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


static TupleTableSlot* IndexNext(IndexScanState* node);
static void ExecInitNextPartitionForIndexScan(IndexScanState* node);

/* ----------------------------------------------------------------
 *		IndexNext
 *
 *		Retrieve a tuple from the IndexScan node's current_relation
 *		using the index specified in the IndexScanState information.
 * ----------------------------------------------------------------
 */
static TupleTableSlot* IndexNext(IndexScanState* node)
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
    if (ScanDirectionIsBackward(((IndexScan*)node->ss.ps.plan)->indexorderdir)) {
        if (ScanDirectionIsForward(direction))
            direction = BackwardScanDirection;
        else if (ScanDirectionIsBackward(direction))
            direction = ForwardScanDirection;
    }
    scandesc = node->iss_ScanDesc;
    econtext = node->ss.ps.ps_ExprContext;
    slot = node->ss.ss_ScanTupleSlot;

    isUstore = RelationIsUstoreFormat(node->ss.ss_currentRelation);

    /*
     * ok, now that we have what we need, fetch the next tuple.
     */
    // we should change abs_idx_getnext to call IdxScanAm(scan)->idx_getnext and channge .idx_getnext in g_HeapIdxAm to
    // IndexGetnextSlot
    while (true) {
        IndexScanDesc indexScan = GetIndexScanDesc(scandesc);
        if (isUstore) {
            if (!IndexGetnextSlot(scandesc, direction, slot)) {
                break;
            }
        } else {
            if ((tuple = scan_handler_idx_getnext(scandesc, direction)) == NULL) {
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
static bool IndexRecheck(IndexScanState* node, TupleTableSlot* slot)
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
 *		ExecIndexScan(node)
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecIndexScan(IndexScanState* node)
{
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
    }

    return ExecScan(&node->ss, (ExecScanAccessMtd)IndexNext, (ExecScanRecheckMtd)IndexRecheck);
}

/* ----------------------------------------------------------------
 *		ExecReScanIndexScan(node)
 *
 *		Recalculates the values of any scan keys whose value depends on
 *		information known at runtime, then rescans the indexed relation.
 *
 *		Updating the scan key was formerly done separately in
 *		ExecUpdateIndexScanKeys. Integrating it into ReScan makes
 *		rescans of indices and relations/general streams more uniform.
 * ----------------------------------------------------------------
 */
void ExecReScanIndexScan(IndexScanState* node)
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
    bool partpruning = ENABLE_SQL_BETA_FEATURE(PARTITION_OPFUSION) && list_length(node->ss.partitions) == 1;
    /* if only one partition is scaned in indexscan, we don't need do rescan for partition */
    if (node->ss.isPartTbl && !partpruning) {
        /*
         * if node->ss.ss_ReScan = true, just do rescaning as non-partitioned
         * table; else switch to next partition for scaning.
         */
        if (node->ss.ss_ReScan) {
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
            ExecInitNextPartitionForIndexScan(node);
            ExecScanReScan(&node->ss);
            return;
        }
    }

    /* reset index scan */
    scan_handler_idx_rescan(
        node->iss_ScanDesc, node->iss_ScanKeys, node->iss_NumScanKeys, node->iss_OrderByKeys, node->iss_NumOrderByKeys);

    ExecScanReScan(&node->ss);
}

/*
 * ExecIndexEvalRuntimeKeys
 *		Evaluate any runtime key values, and update the scankeys.
 */
void ExecIndexEvalRuntimeKeys(ExprContext* econtext, IndexRuntimeKeyInfo* run_time_keys, int num_run_time_keys)
{
    int j;
    MemoryContext old_context;

    /* We want to keep the key values in per-tuple memory */
    old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    for (j = 0; j < num_run_time_keys; j++) {
        ScanKey scan_key = run_time_keys[j].scan_key;
        ExprState* key_expr = run_time_keys[j].key_expr;
        Datum scan_value;
        bool is_null = false;

        /*
         * For each run-time key, extract the run-time expression and evaluate
         * it with respect to the current context.	We then stick the result
         * into the proper scan key.
         *
         * Note: the result of the eval could be a pass-by-ref value that's
         * stored in some outer scan's tuple, not in
         * econtext->ecxt_per_tuple_memory.  We assume that the outer tuple
         * will stay put throughout our scan.  If this is wrong, we could copy
         * the result into our context explicitly, but I think that's not
         * necessary.
         *
         * It's also entirely possible that the result of the eval is a
         * toasted value.  In this case we should forcibly detoast it, to
         * avoid repeat detoastings each time the value is examined by an
         * index support function.
         */
        scan_value = ExecEvalExpr(key_expr, econtext, &is_null, NULL);
        if (is_null) {
            scan_key->sk_argument = scan_value;
            scan_key->sk_flags |= SK_ISNULL;
        } else {
            if (run_time_keys[j].key_toastable)
                scan_value = PointerGetDatum(PG_DETOAST_DATUM(scan_value));
            scan_key->sk_argument = scan_value;
            scan_key->sk_flags &= ~SK_ISNULL;
        }
    }

    MemoryContextSwitchTo(old_context);
}

/*
 * ExecIndexEvalArrayKeys
 *		Evaluate any array key values, and set up to iterate through arrays.
 *
 * Returns TRUE if there are array elements to consider; FALSE means there
 * is at least one null or empty array, so no match is possible.  On TRUE
 * result, the scankeys are initialized with the first elements of the arrays.
 */
bool ExecIndexEvalArrayKeys(ExprContext* econtext, IndexArrayKeyInfo* array_keys, int num_array_keys)
{
    bool result = true;
    int j;
    MemoryContext old_context;

    /* We want to keep the arrays in per-tuple memory */
    old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    for (j = 0; j < num_array_keys; j++) {
        ScanKey scan_key = array_keys[j].scan_key;
        ExprState* array_expr = array_keys[j].array_expr;
        Datum array_datum;
        bool is_null = false;
        ArrayType* array_val = NULL;
        int16 elm_len;
        bool elm_by_val = false;
        char elm_align;
        int num_elems;
        Datum* elem_values = NULL;
        bool* elem_nulls = NULL;

        /*
         * Compute and deconstruct the array expression. (Notes in
         * ExecIndexEvalRuntimeKeys() apply here too.)
         */
        array_datum = ExecEvalExpr(array_expr, econtext, &is_null, NULL);
        if (is_null) {
            result = false;
            break; /* no point in evaluating more */
        }
        array_val = DatumGetArrayTypeP(array_datum);
        /* We could cache this data, but not clear it's worth it */
        get_typlenbyvalalign(ARR_ELEMTYPE(array_val), &elm_len, &elm_by_val, &elm_align);
        deconstruct_array(
            array_val, ARR_ELEMTYPE(array_val), elm_len, elm_by_val, elm_align, &elem_values, &elem_nulls, &num_elems);
        if (num_elems <= 0) {
            result = false;
            break; /* no point in evaluating more */
        }

        /*
         * Note: we expect the previous array data, if any, to be
         * automatically freed by resetting the per-tuple context; hence no
         * pfree's here.
         */
        array_keys[j].elem_values = elem_values;
        array_keys[j].elem_nulls = elem_nulls;
        array_keys[j].num_elems = num_elems;
        scan_key->sk_argument = elem_values[0];
        if (elem_nulls[0])
            scan_key->sk_flags |= SK_ISNULL;
        else
            scan_key->sk_flags &= ~SK_ISNULL;
        array_keys[j].next_elem = 1;
    }

    MemoryContextSwitchTo(old_context);

    return result;
}

/*
 * ExecIndexAdvanceArrayKeys
 *		Advance to the next set of array key values, if any.
 *
 * Returns TRUE if there is another set of values to consider, FALSE if not.
 * On TRUE result, the scankeys are initialized with the next set of values.
 */
bool ExecIndexAdvanceArrayKeys(IndexArrayKeyInfo* array_keys, int num_array_keys)
{
    bool found = false;
    int j;

    /*
     * Note we advance the rightmost array key most quickly, since it will
     * correspond to the lowest-order index column among the available
     * qualifications.	This is hypothesized to result in better locality of
     * access in the index.
     */
    for (j = num_array_keys - 1; j >= 0; j--) {
        ScanKey scan_key = array_keys[j].scan_key;
        int next_elem = array_keys[j].next_elem;
        int num_elems = array_keys[j].num_elems;
        Datum* elem_values = array_keys[j].elem_values;
        bool* elem_nulls = array_keys[j].elem_nulls;

        if (next_elem >= num_elems) {
            next_elem = 0;
            found = false; /* need to advance next array key */
        } else {
            found = true;
        }
        scan_key->sk_argument = elem_values[next_elem];
        if (elem_nulls[next_elem]) {
            scan_key->sk_flags |= SK_ISNULL;
        } else {
            scan_key->sk_flags &= ~SK_ISNULL;
        }
        array_keys[j].next_elem = next_elem + 1;
        if (found) {
            break;
        }
    }

    return found;
}

/* ----------------------------------------------------------------
 *		ExecEndIndexScan
 * ----------------------------------------------------------------
 */
void ExecEndIndexScan(IndexScanState* node)
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
 *		ExecIndexMarkPos
 * ----------------------------------------------------------------
 */
void ExecIndexMarkPos(IndexScanState* node)
{
    scan_handler_idx_markpos(node->iss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecIndexRestrPos
 * ----------------------------------------------------------------
 */
void ExecIndexRestrPos(IndexScanState* node)
{
    scan_handler_idx_restrpos(node->iss_ScanDesc);
}

/* ----------------------------------------------------------------
 *		ExecInitIndexScan
 *
 *		Initializes the index scan's state information, creates
 *		scan keys, and opens the base and index relations.
 *
 *		Note: index scans have 2 sets of state information because
 *			  we have to keep track of the base relation and the
 *			  index relation.
 * ----------------------------------------------------------------
 */
void ExecInitIndexRelation(IndexScanState* node, EState* estate, int eflags)
{
    IndexScanState* index_state = node;
    Snapshot scanSnap;
    Relation current_relation = index_state->ss.ss_currentRelation;
    IndexScan *index_scan = (IndexScan *)node->ss.ps.plan;

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
            ExecInitPartitionForIndexScan(index_state, estate);

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
                        ereport(ERROR,
                                (errcode(ERRCODE_SNAPSHOT_INVALID),
                                 (errmsg("Snapshot too old."))));
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
                ereport(ERROR,
                        (errcode(ERRCODE_SNAPSHOT_INVALID),
                         (errmsg("Snapshot too old."))));
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

IndexScanState* ExecInitIndexScan(IndexScan* node, EState* estate, int eflags)
{
    IndexScanState* index_state = NULL;
    Relation current_relation;
    bool relis_target = false;

    gstrace_entry(GS_TRC_ID_ExecInitIndexScan);
    /*
     * create state structure
     */
    index_state = makeNode(IndexScanState);
    index_state->ss.ps.plan = (Plan*)node;
    index_state->ss.ps.state = estate;
    index_state->ss.isPartTbl = node->scan.isPartTbl;
    index_state->ss.currentSlot = 0;
    index_state->ss.partScanDirection = node->indexorderdir;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &index_state->ss.ps);

    index_state->ss.ps.ps_TupFromTlist = false;

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
    index_state->ss.ps.targetlist = (List*)ExecInitExpr((Expr*)node->scan.plan.targetlist, (PlanState*)index_state);
    index_state->ss.ps.qual = (List*)ExecInitExpr((Expr*)node->scan.plan.qual, (PlanState*)index_state);
    index_state->indexqualorig = (List*)ExecInitExpr((Expr*)node->indexqualorig, (PlanState*)index_state);

    /*
     * open the base relation and acquire appropriate lock on it.
     */
    current_relation = ExecOpenScanRelation(estate, node->scan.scanrelid);

    index_state->ss.ss_currentRelation = current_relation;
    index_state->ss.ss_currentScanDesc = NULL; /* no heap scan here */
    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &index_state->ss.ps, current_relation->rd_tam_type);
    ExecInitScanTupleSlot(estate, &index_state->ss, current_relation->rd_tam_type);

    /*
     * get the scan type from the relation descriptor.
     */
    ExecAssignScanType(&index_state->ss, CreateTupleDescCopy(RelationGetDescr(current_relation)));
    index_state->ss.ss_ScanTupleSlot->tts_tupleDescriptor->tdTableAmType = current_relation->rd_tam_type;

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(&index_state->ss.ps);

    index_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->tdTableAmType =
            index_state->ss.ss_ScanTupleSlot->tts_tupleDescriptor->tdTableAmType;

    ExecAssignScanProjectionInfo(&index_state->ss);

    Assert(index_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->tdTableAmType != TAM_INVALID);

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
    relis_target = ExecRelationIsTargetRelation(estate, node->scan.scanrelid);
    index_state->iss_RelationDesc = index_open(node->indexid, relis_target ? NoLock : AccessShareLock);
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
    ExecInitIndexRelation(index_state, estate, eflags);

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
 * ExecIndexBuildScanKeys
 *		Build the index scan keys from the index qualification expressions
 *
 * The index quals are passed to the index AM in the form of a ScanKey array.
 * This routine sets up the ScanKeys, fills in all constant fields of the
 * ScanKeys, and prepares information about the keys that have non-constant
 * comparison values.  We divide index qual expressions into five types:
 *
 * 1. Simple operator with constant comparison value ("indexkey op constant").
 * For these, we just fill in a ScanKey containing the constant value.
 *
 * 2. Simple operator with non-constant value ("indexkey op expression").
 * For these, we create a ScanKey with everything filled in except the
 * expression value, and set up an IndexRuntimeKeyInfo struct to drive
 * evaluation of the expression at the right times.
 *
 * 3. RowCompareExpr ("(indexkey, indexkey, ...) op (expr, expr, ...)").
 * For these, we create a header ScanKey plus a subsidiary ScanKey array,
 * as specified in access/skey.h.  The elements of the row comparison
 * can have either constant or non-constant comparison values.
 *
 * 4. ScalarArrayOpExpr ("indexkey op ANY (array-expression)").  If the index
 * has rd_am->amsearcharray, we handle these the same as simple operators,
 * setting the SK_SEARCHARRAY flag to tell the AM to handle them.  Otherwise,
 * we create a ScanKey with everything filled in except the comparison value,
 * and set up an IndexArrayKeyInfo struct to drive processing of the qual.
 * (Note that if we use an IndexArrayKeyInfo struct, the array expression is
 * always treated as requiring runtime evaluation, even if it's a constant.)
 *
 * 5. NullTest ("indexkey IS NULL/IS NOT NULL").  We just fill in the
 * ScanKey properly.
 *
 * This code is also used to prepare ORDER BY expressions for amcanorderbyop
 * indexes.  The behavior is exactly the same, except that we have to look up
 * the operator differently.  Note that only cases 1 and 2 are currently
 * possible for ORDER BY.
 *
 * Input params are:
 *
 * plan_state: executor state node we are working for
 * index: the index we are building scan keys for
 * quals: indexquals (or indexorderbys) expressions
 * is_order_by: true if processing ORDER BY exprs, false if processing quals
 * *run_time_keys: ptr to pre-existing IndexRuntimeKeyInfos, or NULL if none
 * *num_run_time_keys: number of pre-existing runtime keys
 *
 * Output params are:
 *
 * *scan_keys: receives ptr to array of ScanKeys
 * *num_scan_keys: receives number of scankeys
 * *run_time_keys: receives ptr to array of IndexRuntimeKeyInfos, or NULL if none
 * *num_run_time_keys: receives number of runtime keys
 * *array_keys: receives ptr to array of IndexArrayKeyInfos, or NULL if none
 * *num_array_keys: receives number of array keys
 *
 * Caller may pass NULL for array_keys and num_array_keys to indicate that
 * IndexArrayKeyInfos are not supported.
 */
void ExecIndexBuildScanKeys(PlanState* plan_state, Relation index, List* quals, bool is_order_by, ScanKey* scankeys,
    int* num_scan_keys, IndexRuntimeKeyInfo** run_time_keys, int* num_run_time_keys, IndexArrayKeyInfo** arraykeys,
    int* num_array_keys)
{
    ListCell* qual_cell = NULL;
    ScanKey scan_keys;
    IndexRuntimeKeyInfo* runtime_keys = NULL;
    IndexArrayKeyInfo* array_keys = NULL;
    int n_scan_keys;
    int n_runtime_keys;
    int max_runtime_keys;
    int n_array_keys;
    int j;

    /* Allocate array for ScanKey structs: one per qual */
    n_scan_keys = list_length(quals);
    scan_keys = (ScanKey)palloc(n_scan_keys * sizeof(ScanKeyData));

    /*
     * runtime_keys array is dynamically resized as needed.  We handle it this
     * way so that the same runtime keys array can be shared between
     * indexquals and indexorderbys, which will be processed in separate calls
     * of this function.  Caller must be sure to pass in NULL/0 for first
     * call.
     */
    runtime_keys = *run_time_keys;
    n_runtime_keys = max_runtime_keys = *num_run_time_keys;

    /* Allocate array_keys as large as it could possibly need to be */
    array_keys = (IndexArrayKeyInfo*)palloc0(n_scan_keys * sizeof(IndexArrayKeyInfo));
    n_array_keys = 0;

    /*
     * for each opclause in the given qual, convert the opclause into a single
     * scan key
     */
    j = 0;
    foreach (qual_cell, quals) {
        Expr* clause = (Expr*)lfirst(qual_cell);
        ScanKey this_scan_key = &scan_keys[j++];
        Oid opno;              /* operator's OID */
        RegProcedure opfuncid; /* operator proc id used in scan */
        Oid opfamily;          /* opfamily of index column */
        int op_strategy;       /* operator's strategy number */
        Oid op_lefttype;       /* operator's declared input types */
        Oid op_righttype;
        Expr* leftop = NULL;  /* expr on lhs of operator */
        Expr* rightop = NULL; /* expr on rhs ... */
        AttrNumber varattno;  /* att number used in scan */
        int indnkeyatts;

        indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
        if (IsA(clause, OpExpr)) {
            /* indexkey op const or indexkey op expression */
            uint32 flags = 0;
            Datum scan_value;

            opno = ((OpExpr*)clause)->opno;
            opfuncid = ((OpExpr*)clause)->opfuncid;

            /*
             * leftop should be the index key Var, possibly relabeled
             */
            leftop = (Expr*)get_leftop(clause);
            if (leftop && IsA(leftop, RelabelType))
                leftop = ((RelabelType*)leftop)->arg;
            Assert(leftop != NULL);
            if (!(IsA(leftop, Var) && ((Var*)leftop)->varno == INDEX_VAR))
                ereport(ERROR,
                    (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("indexqual for OpExpr doesn't have key on left side")));

            varattno = ((Var*)leftop)->varattno;
            if (varattno < 1 || varattno > indnkeyatts)
                ereport(ERROR,
                    (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("bogus index qualification for OpExpr, attribute number is %d.", varattno)));

            /*
             * We have to look up the operator's strategy number.  This
             * provides a cross-check that the operator does match the index.
             */
            opfamily = index->rd_opfamily[varattno - 1];

            get_op_opfamily_properties(opno, opfamily, is_order_by, &op_strategy, &op_lefttype, &op_righttype);

            if (is_order_by)
                flags |= SK_ORDER_BY;

            /*
             * rightop is the constant or variable comparison value
             */
            rightop = (Expr*)get_rightop(clause);
            if (rightop && IsA(rightop, RelabelType))
                rightop = ((RelabelType*)rightop)->arg;
            Assert(rightop != NULL);
            if (IsA(rightop, Const)) {
                /* OK, simple constant comparison value */
                scan_value = ((Const*)rightop)->constvalue;
                if (((Const*)rightop)->constisnull)
                    flags |= SK_ISNULL;
            } else {
                /* Need to treat this one as a runtime key */
                if (n_runtime_keys >= max_runtime_keys) {
                    if (max_runtime_keys == 0) {
                        max_runtime_keys = 8;
                        runtime_keys = (IndexRuntimeKeyInfo*)palloc(max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
                    } else {
                        max_runtime_keys *= 2;
                        runtime_keys = (IndexRuntimeKeyInfo*)repalloc(
                            runtime_keys, max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
                    }
                }
                runtime_keys[n_runtime_keys].scan_key = this_scan_key;
                runtime_keys[n_runtime_keys].key_expr = ExecInitExpr(rightop, plan_state);
                runtime_keys[n_runtime_keys].key_toastable = TypeIsToastable(op_righttype);
                n_runtime_keys++;
                scan_value = (Datum)0;
            }

            /*
             * initialize the scan key's fields appropriately
             */
            ScanKeyEntryInitialize(this_scan_key,
                flags,
                varattno,                       /* attribute number to scan */
                op_strategy,                    /* op's strategy */
                op_righttype,                   /* strategy subtype */
                ((OpExpr*)clause)->inputcollid, /* collation */
                opfuncid,                       /* reg proc to use */
                scan_value);                     /* constant */
        } else if (IsA(clause, RowCompareExpr)) {
            /* (indexkey, indexkey, ...) op (expression, expression, ...) */
            RowCompareExpr* rc = (RowCompareExpr*)clause;
            ListCell* largs_cell = list_head(rc->largs);
            ListCell* rargs_cell = list_head(rc->rargs);
            ListCell* opnos_cell = list_head(rc->opnos);
            ListCell* collids_cell = list_head(rc->inputcollids);
            ScanKey first_sub_key;
            int n_sub_key;

            Assert(!is_order_by);

            first_sub_key = (ScanKey)palloc(list_length(rc->opnos) * sizeof(ScanKeyData));
            n_sub_key = 0;

            /* Scan RowCompare columns and generate subsidiary ScanKey items */
            while (opnos_cell != NULL) {
                ScanKey this_sub_key = &first_sub_key[n_sub_key];
                int flags = SK_ROW_MEMBER;
                Datum scan_value;
                Oid inputcollation;

                /*
                 * leftop should be the index key Var, possibly relabeled
                 */
                leftop = (Expr*)lfirst(largs_cell);
                largs_cell = lnext(largs_cell);

                if (leftop && IsA(leftop, RelabelType))
                    leftop = ((RelabelType*)leftop)->arg;

                Assert(leftop != NULL);

                if (!(IsA(leftop, Var) && ((Var*)leftop)->varno == INDEX_VAR))
                    ereport(ERROR,
                        (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("indexqual for RowCompare expression doesn't have key on left side")));

                varattno = ((Var*)leftop)->varattno;

                /*
                 * We have to look up the operator's associated btree support
                 * function
                 */
                opno = lfirst_oid(opnos_cell);
                opnos_cell = lnext(opnos_cell);

                if (!OID_IS_BTREE(index->rd_rel->relam) || varattno < 1 || varattno > index->rd_index->indnatts)
                    ereport(ERROR,
                        (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("bogus RowCompare index qualification, attribute number is %d", varattno)));
                opfamily = index->rd_opfamily[varattno - 1];

                get_op_opfamily_properties(opno, opfamily, is_order_by, &op_strategy, &op_lefttype, &op_righttype);

                if (op_strategy != rc->rctype)
                    ereport(ERROR,
                        (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("RowCompare index qualification contains wrong operator, strategy number is %d.",
                                op_strategy)));

                opfuncid = get_opfamily_proc(opfamily, op_lefttype, op_righttype, BTORDER_PROC);

                inputcollation = lfirst_oid(collids_cell);
                collids_cell = lnext(collids_cell);

                /*
                 * rightop is the constant or variable comparison value
                 */
                rightop = (Expr*)lfirst(rargs_cell);
                rargs_cell = lnext(rargs_cell);

                if (rightop && IsA(rightop, RelabelType))
                    rightop = ((RelabelType*)rightop)->arg;

                Assert(rightop != NULL);

                if (IsA(rightop, Const)) {
                    /* OK, simple constant comparison value */
                    scan_value = ((Const*)rightop)->constvalue;
                    if (((Const*)rightop)->constisnull)
                        flags |= SK_ISNULL;
                } else {
                    /* Need to treat this one as a runtime key */
                    if (n_runtime_keys >= max_runtime_keys) {
                        if (max_runtime_keys == 0) {
                            max_runtime_keys = 8;
                            runtime_keys = (IndexRuntimeKeyInfo*)palloc(max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
                        } else {
                            max_runtime_keys *= 2;
                            runtime_keys = (IndexRuntimeKeyInfo*)repalloc(
                                runtime_keys, max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
                        }
                    }
                    runtime_keys[n_runtime_keys].scan_key = this_sub_key;
                    runtime_keys[n_runtime_keys].key_expr = ExecInitExpr(rightop, plan_state);
                    runtime_keys[n_runtime_keys].key_toastable = TypeIsToastable(op_righttype);
                    n_runtime_keys++;
                    scan_value = (Datum)0;
                }

                /*
                 * initialize the subsidiary scan key's fields appropriately
                 */
                ScanKeyEntryInitialize(this_sub_key,
                    flags,
                    varattno,       /* attribute number */
                    op_strategy,    /* op's strategy */
                    op_righttype,   /* strategy subtype */
                    inputcollation, /* collation */
                    opfuncid,       /* reg proc to use */
                    scan_value);     /* constant */
                n_sub_key++;
            }

            if (n_sub_key == 0) {
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("n_sub_key can not be zero")));
            }
            
            /* Mark the last subsidiary scankey correctly */
            first_sub_key[n_sub_key - 1].sk_flags |= SK_ROW_END;
            /*
             * We don't use ScanKeyEntryInitialize for the header because it
             * isn't going to contain a valid sk_func pointer.
             */
            errno_t errorno = memset_s(this_scan_key, sizeof(ScanKeyData), 0, sizeof(ScanKeyData));
            securec_check(errorno, "\0", "\0");
            this_scan_key->sk_flags = SK_ROW_HEADER;
            this_scan_key->sk_attno = first_sub_key->sk_attno;
            this_scan_key->sk_strategy = rc->rctype;
            /* sk_subtype, sk_collation, sk_func not used in a header */
            this_scan_key->sk_argument = PointerGetDatum(first_sub_key);
        } else if (IsA(clause, ScalarArrayOpExpr)) {
            /* indexkey op ANY (array-expression) */
            ScalarArrayOpExpr* saop = (ScalarArrayOpExpr*)clause;
            int flags = 0;
            Datum scan_value;

            Assert(!is_order_by);

            Assert(saop->useOr);
            opno = saop->opno;
            opfuncid = saop->opfuncid;

            /*
             * leftop should be the index key Var, possibly relabeled
             */
            leftop = (Expr*)linitial(saop->args);
            if (leftop && IsA(leftop, RelabelType))
                leftop = ((RelabelType*)leftop)->arg;
            Assert(leftop != NULL);
            if (!(IsA(leftop, Var) && ((Var*)leftop)->varno == INDEX_VAR))
                ereport(ERROR,
                    (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("indexqual for ScalarArray doesn't have key on left side")));

            varattno = ((Var*)leftop)->varattno;
            if (varattno < 1 || varattno > indnkeyatts)
                ereport(ERROR,
                    (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("bogus index qualification for ScalarArray, attribute number is %d.", varattno)));

            /*
             * We have to look up the operator's strategy number.  This
             * provides a cross-check that the operator does match the index.
             */
            opfamily = index->rd_opfamily[varattno - 1];

            get_op_opfamily_properties(opno, opfamily, is_order_by, &op_strategy, &op_lefttype, &op_righttype);

            /*
             * rightop is the constant or variable array value
             */
            rightop = (Expr*)lsecond(saop->args);
            if (rightop && IsA(rightop, RelabelType))
                rightop = ((RelabelType*)rightop)->arg;
            Assert(rightop != NULL);

            if (index->rd_am->amsearcharray) {
                /* Index AM will handle this like a simple operator */
                flags |= SK_SEARCHARRAY;
                if (IsA(rightop, Const)) {
                    /* OK, simple constant comparison value */
                    scan_value = ((Const*)rightop)->constvalue;
                    if (((Const*)rightop)->constisnull)
                        flags |= SK_ISNULL;
                } else {
                    /* Need to treat this one as a runtime key */
                    if (n_runtime_keys >= max_runtime_keys) {
                        if (max_runtime_keys == 0) {
                            max_runtime_keys = 8;
                            runtime_keys = (IndexRuntimeKeyInfo*)palloc(max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
                        } else {
                            max_runtime_keys *= 2;
                            runtime_keys = (IndexRuntimeKeyInfo*)repalloc(
                                runtime_keys, max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
                        }
                    }
                    runtime_keys[n_runtime_keys].scan_key = this_scan_key;
                    runtime_keys[n_runtime_keys].key_expr = ExecInitExpr(rightop, plan_state);

                    /*
                     * Careful here: the runtime expression is not of
                     * op_righttype, but rather is an array of same; so
                     * TypeIsToastable() isn't helpful.  However, we can
                     * assume that all array types are toastable.
                     */
                    runtime_keys[n_runtime_keys].key_toastable = true;
                    n_runtime_keys++;
                    scan_value = (Datum)0;
                }
            } else {
                /* Executor has to expand the array value */
                array_keys[n_array_keys].scan_key = this_scan_key;
                array_keys[n_array_keys].array_expr = ExecInitExpr(rightop, plan_state);
                /* the remaining fields were zeroed by palloc0 */
                n_array_keys++;
                scan_value = (Datum)0;
            }

            /*
             * initialize the scan key's fields appropriately
             */
            ScanKeyEntryInitialize(this_scan_key,
                flags,
                varattno,          /* attribute number to scan */
                op_strategy,       /* op's strategy */
                op_righttype,      /* strategy subtype */
                saop->inputcollid, /* collation */
                opfuncid,          /* reg proc to use */
                scan_value);        /* constant */
        } else if (IsA(clause, NullTest)) {
            /* indexkey IS NULL or indexkey IS NOT NULL */
            NullTest* ntest = (NullTest*)clause;
            int flags;

            Assert(!is_order_by);

            /*
             * argument should be the index key Var, possibly relabeled
             */
            leftop = ntest->arg;

            if (leftop && IsA(leftop, RelabelType))
                leftop = ((RelabelType*)leftop)->arg;

            Assert(leftop != NULL);

            if (!(IsA(leftop, Var) && ((Var*)leftop)->varno == INDEX_VAR))
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("NullTest indexqual has wrong key")));

            varattno = ((Var*)leftop)->varattno;

            /*
             * initialize the scan key's fields appropriately
             */
            switch (ntest->nulltesttype) {
                case IS_NULL:
                    flags = SK_ISNULL | SK_SEARCHNULL;
                    break;
                case IS_NOT_NULL:
                    flags = SK_ISNULL | SK_SEARCHNOTNULL;
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized nulltesttype: %d", (int)ntest->nulltesttype)));
                    flags = 0; /* keep compiler quiet */
                    break;
            }

            ScanKeyEntryInitialize(this_scan_key,
                flags,
                varattno,        /* attribute number to scan */
                InvalidStrategy, /* no strategy */
                InvalidOid,      /* no strategy subtype */
                InvalidOid,      /* no collation */
                InvalidOid,      /* no reg proc for this */
                (Datum)0);       /* constant */
        } else
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unsupported indexqual type: %d", (int)nodeTag(clause))));
    }

    Assert(n_runtime_keys <= max_runtime_keys);

    /* Get rid of any unused arrays */
    if (n_array_keys == 0) {
        pfree_ext(array_keys);
        array_keys = NULL;
    }

    /*
     * Return info to our caller.
     */
    *scankeys = scan_keys;
    *num_scan_keys = n_scan_keys;
    *run_time_keys = runtime_keys;
    *num_run_time_keys = n_runtime_keys;
    if (arraykeys != NULL) {
        *arraykeys = array_keys;
        *num_array_keys = n_array_keys;
    } else if (n_array_keys != 0)
        ereport(
            ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("ScalarArrayOpExpr index qual found where not allowed")));
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: construt a dummy relation with the next partition and the partitiobed
 *			: table for the following Indexscan, and swith the scaning relation to
 *			: the dummy relation
 * Description	:
 * Input		:
 * Output	:
 * Notes		:
 */
static void ExecInitNextPartitionForIndexScan(IndexScanState* node)
{
    Partition current_partition = NULL;
    Relation current_partition_rel = NULL;
    Partition current_index_partition = NULL;
    Relation current_index_partition_rel = NULL;
    IndexScan* plan = NULL;
    int param_no = -1;
    ParamExecData* param = NULL;
    int subPartParamno = -1;
    ParamExecData* SubPrtParam = NULL;

    plan = (IndexScan*)(node->ss.ps.plan);

    /* get partition sequnce */
    param_no = plan->scan.plan.paramno;
    param = &(node->ss.ps.state->es_param_exec_vals[param_no]);
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
        node->ss.ps.state->es_snapshot,
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
 *			: the following IndexScan
 * Description	:
 * Input		:
 * Output	:
 * Notes		:
 */
void ExecInitPartitionForIndexScan(IndexScanState* index_state, EState* estate)
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
        bool relis_target = false;
        Partition indexpartition = NULL;
        LOCKMODE lock;

        /*
         * get relation's lockmode that hangs on whether
         * it's one of the target relations of the query
         */
        relis_target = ExecRelationIsTargetRelation(estate, plan->scan.scanrelid);
        lock = (relis_target ? RowExclusiveLock : AccessShareLock);
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

        ListCell* cell = NULL;
        List* part_seqs = resultPlan->ls_rangeSelectedPartitions;
        foreach (cell, part_seqs) {
            Oid tablepartitionid = InvalidOid;
            Oid indexpartitionid = InvalidOid;
            List* partitionIndexOidList = NIL;
            int partSeq = lfirst_int(cell);

            /* get table partition and add it to a list for following scan */
            tablepartitionid = getPartitionOidFromSequence(current_relation, partSeq);
            table_partition = partitionOpen(current_relation, tablepartitionid, lock);
            index_state->ss.partitions = lappend(index_state->ss.partitions, table_partition);

            if (RelationIsSubPartitioned(current_relation)) {
                ListCell *lc = NULL;
                SubPartitionPruningResult* subPartPruningResult =
                    GetSubPartitionPruningResult(resultPlan->ls_selectedSubPartitions, partSeq);
                if (subPartPruningResult == NULL) {
                    continue;
                }
                List *subpartList = subPartPruningResult->ls_selectedSubPartitions;
                List *subIndexList = NULL;
                List *subRelationList = NULL;

                foreach (lc, subpartList)
                {
                    int subpartSeq = lfirst_int(lc);
                    Relation tablepartrel = partitionGetRelation(current_relation, table_partition);
                    Oid subpartitionid = getPartitionOidFromSequence(tablepartrel, subpartSeq);
                    Partition subpart = partitionOpen(tablepartrel, subpartitionid, AccessShareLock);

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
    }
}
