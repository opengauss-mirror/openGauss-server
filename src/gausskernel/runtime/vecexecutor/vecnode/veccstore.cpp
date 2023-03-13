/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * veccstore.cpp
 *    Support routines for sequential scans of column stores.
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/veccstore.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * INTERFACE ROUTINES
 *      ExecCStoreScan              sequentially scans a column store.
 *      ExecCStoreNext              retrieve next tuple in sequential order.
 *      ExecInitCStoreScan          creates and initializes a cstorescan node.
 *      ExecEndCStoreScan           releases any storage allocated.
 *      ExecReScanCStoreScan        rescans the column store
 */
#include "codegen/gscodegen.h"
#include "codegen/vecexprcodegen.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "executor/exec/execdebug.h"
#include "vecexecutor/vecnodecstorescan.h"
#include "vecexecutor/vecnoderowtovector.h"
#include "executor/node/nodeModifyTable.h"
#include "executor/node/nodeSeqscan.h"
#include "storage/cstore/cstore_compress.h"
#include "access/cstore_am.h"
#include "optimizer/clauses.h"
#include "nodes/params.h"
#include "utils/lsyscache.h"
#include "utils/datum.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "executor/node/nodeSamplescan.h"
#include "executor/node/nodeSeqscan.h"
#include "access/cstoreskey.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "catalog/indexing.h"
#include "access/heapam.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "catalog/pg_partition_fn.h"
#include "pgxc/redistrib.h"
#include "optimizer/pruning.h"


extern bool CodeGenThreadObjectReady();
extern bool CodeGenPassThreshold(double rows, int dn_num, int dop);

static CStoreStrategyNumber GetCStoreScanStrategyNumber(Oid opno);
static Datum GetParamExternConstValue(Oid left_type, Expr* expr, PlanState* ps, uint16* flag);
static void ExecInitNextPartitionForCStoreScan(CStoreScanState* node);
static void ExecCStoreBuildScanKeys(CStoreScanState* scan_stat, List* quals, CStoreScanKey* scan_keys, int* num_scan_keys,
    CStoreScanRunTimeKeyInfo** runtime_key_info, int* runtime_keys_num);
static void ExecCStoreScanEvalRuntimeKeys(
    ExprContext* expr_ctx, CStoreScanRunTimeKeyInfo* runtime_keys, int num_runtime_keys);

/* the same to CStore::SetTiming() */
#define TIMING_VECCSTORE_SCAN(_node) (NULL != (_node)->ps.instrument && (_node)->ps.instrument->need_timer)

#define VECCSTORE_SCAN_TRACE_START(_node, _desc_id)                  \
    do {                                                             \
        if (unlikely(TIMING_VECCSTORE_SCAN(_node))) {                \
            TRACK_START((_node)->ps.plan->plan_node_id, (_desc_id)); \
        }                                                            \
    } while (0)

#define VECCSTORE_SCAN_TRACE_END(_node, _desc_id)                  \
    do {                                                           \
        if (unlikely(TIMING_VECCSTORE_SCAN(_node))) {              \
            TRACK_END((_node)->ps.plan->plan_node_id, (_desc_id)); \
        }                                                          \
    } while (0)

/*
 * type conversion for cu min/max filter
 * see also: cstore_roughcheck_func.cpp
 */
static FORCE_INLINE Datum convert_scan_key_int64_if_need(Oid left_type, Oid right_type, Datum right_value)
{
    Datum v = right_value;
    switch (right_type) {
        /* int family to INT64 */
        case INT4OID: {
            v = Int64GetDatum((int64)DatumGetInt32(right_value));
            break;
        }
        case INT2OID: {
            v = Int64GetDatum((int64)DatumGetInt16(right_value));
            break;
        }
        case INT1OID: {
            v = Int64GetDatum((int64)DatumGetChar(right_value));
            break;
        }
        case DATEOID:
        case TIMEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID: {
            /* date, time etc. make right type same as left */
            if (right_type != left_type) {
                /* get type conversion function */
                HeapTuple cast_tuple =
                    SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(right_type), ObjectIdGetDatum(left_type));

                if (!HeapTupleIsValid(cast_tuple))
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("can not cast from type %s to type %s ",
                                format_type_be(right_type),
                                format_type_be(left_type))));

                Form_pg_cast cast_form = (Form_pg_cast)GETSTRUCT(cast_tuple);
                Oid func_id = cast_form->castfunc;
                char cast_method = cast_form->castmethod;

                ReleaseSysCache(cast_tuple);

                if (!OidIsValid(func_id) || cast_method != COERCION_METHOD_FUNCTION)
                    break;

                /* check function args */
                HeapTuple proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_id));

                if (!HeapTupleIsValid(proc_tuple))
                    ereport(ERROR,
                        (errmodule(MOD_VEC_EXECUTOR),
                            errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                            errmsg("cache lookup failed for function %u", func_id)));

                Form_pg_proc proc_form = (Form_pg_proc)GETSTRUCT(proc_tuple);
                int proc_nargs = proc_form->pronargs;

                ReleaseSysCache(proc_tuple);

                /* execute type conversion */
                if (proc_nargs == 1)
                    v = OidFunctionCall1(func_id, right_value);
            }
            break;
        }
        default:
            break;
    }
    return v;
}

void OptimizeProjectionAndFilter(CStoreScanState* node)
{
    ProjectionInfo* proj = NULL;
    bool simple_map = false;

    proj = node->ps.ps_ProjInfo;

    // Check if it is simple without need to invoke projection code
    //
    simple_map = proj->pi_directMap && (node->m_pCurrentBatch->m_cols == proj->pi_numSimpleVars);

    node->m_fSimpleMap = simple_map;
}

VectorBatch* ApplyProjectionAndFilter(CStoreScanState* node, VectorBatch* p_scan_batch, ExprDoneCond* done)
{
    List* qual = NIL;
    ExprContext* econtext = NULL;
    ProjectionInfo* proj = node->ps.ps_ProjInfo;
    VectorBatch* p_out_batch = NULL;
    bool simple_map = false;
    int late_read_ctid = 0;
    uint64 input_rows = p_scan_batch->m_rows;

    VECCSTORE_SCAN_TRACE_START(node, CSTORE_PROJECT);

    qual = (List*)node->ps.qual;
    econtext = node->ps.ps_ExprContext;
    p_out_batch = node->m_pCurrentBatch;
    simple_map = node->m_fSimpleMap;

    if (node->jitted_vecqual) {
        if (HAS_INSTR(node, false)) {
            node->ps.instrument->isLlvmOpt = true;
        }
    }

    if (p_scan_batch->m_rows != 0) {
        ResetExprContext(econtext);
        initEcontextBatch(p_scan_batch, NULL, NULL, NULL);
        // Evaluate the qualification clause if any.
        //
        if (qual != NULL) {
            ScalarVector* p_vector = NULL;

            if (node->jitted_vecqual)
                p_vector = node->jitted_vecqual(econtext);
            else
                p_vector = ExecVecQual(qual, econtext, false);

            // If no matched rows, fetch again.
            //
            if (p_vector == NULL) {
                p_out_batch->m_rows = 0;
                goto done;
            }

            /*
             * Call optimized PackT function when codegen is turned on.
             */
            if (econtext->ecxt_scanbatch->m_sel) {
                if (u_sess->attr.attr_sql.enable_codegen) {
                    late_read_ctid = node->m_CStore->GetLateReadCtid();
                    if (node->ss_deltaScan || late_read_ctid == -1) {
                        p_scan_batch->OptimizePack(econtext->ecxt_scanbatch->m_sel, proj->pi_PackTCopyVars);
                    } else {
                        p_scan_batch->OptimizePackForLateRead(
                            econtext->ecxt_scanbatch->m_sel, proj->pi_PackLateAccessVarNumbers, late_read_ctid);
                    }
                } else
                    p_scan_batch->Pack(econtext->ecxt_scanbatch->m_sel);
            }
        }

        // Late read these columns for non-delta data
        // Now we have finished filter check, and then we can read other columns
        //
        if (!node->ss_deltaScan) {
            VECCSTORE_SCAN_TRACE_START(node, FILL_LATER_BATCH);
            node->m_CStore->FillScanBatchLateIfNeed(p_scan_batch);
            VECCSTORE_SCAN_TRACE_END(node, FILL_LATER_BATCH);
        } else {
            node->ss_deltaScan = false;
        }

        // Project the final result
        //
        if (!simple_map) {
            p_out_batch = ExecVecProject(proj, true, done);
        } else {
            // Copy the result to output batch. Note the output batch has different column set than
            // the scan batch, so we have to remap them. Projection will handle all logics here, so
            // for non simpleMap case, we don't need to do anything.
            //
            p_out_batch->m_rows = p_scan_batch->m_rows;
            for (int i = 0; i < p_out_batch->m_cols; i++) {
                AttrNumber att = proj->pi_varNumbers[i];
                errno_t rc;

                Assert(att > 0 && att <= node->m_pScanBatch->m_cols);

                rc = memcpy_s(
                    &p_out_batch->m_arr[i], sizeof(ScalarVector), &p_scan_batch->m_arr[att - 1], sizeof(ScalarVector));
                securec_check(rc, "\0", "\0");
            }
        }
    }

    if (proj->pi_exprContext->have_vec_set_fun == false) {
        p_out_batch->m_rows = Min(p_out_batch->m_rows, p_scan_batch->m_rows);
        p_out_batch->FixRowCount();
    }

done:

    VECCSTORE_SCAN_TRACE_END(node, CSTORE_PROJECT);

    // collect information of removed rows
    InstrCountFiltered1(node, input_rows - p_out_batch->m_rows);

    // Check fullness of return batch and refill it does not contain enough?
    return p_out_batch;
}

TupleDesc BuildTupleDescByTargetList(List* tlist)
{
    ListCell* lc = NULL;
    TupleDesc desc = CreateTemplateTupleDesc(list_length(tlist), false);
    int att_num = 1;
    foreach (lc, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        Var* var = (Var*)tle->expr;
        TupleDescInitEntry(desc, (AttrNumber)att_num, tle->resname, var->vartype, var->vartypmod, 0);
        att_num++;
    }

    return desc;
}

/* ----------------------------------------------------------------
 *      ExecCStoreScan(node)
 *
 *      Scans the relation sequentially and returns the next qualifying
 *      tuple.
 *      We call the ExecScan() routine and pass it the appropriate
 *      access method functions.
 * ----------------------------------------------------------------
 */
VectorBatch* ExecCStoreScan(CStoreScanState* node)
{
    VectorBatch* p_out_batch = NULL;
    VectorBatch* p_scan_batch = NULL;

    // If we have runtime keys and they've not already been set up, do it now.
    //
    // evaluate the runtime key
    if (node->m_ScanRunTimeKeysNum && !node->m_ScanRunTimeKeysReady) {
        ExecCStoreScanEvalRuntimeKeys(node->ps.ps_ExprContext, node->m_pScanRunTimeKeys, node->m_ScanRunTimeKeysNum);
        node->m_ScanRunTimeKeysReady = true;
    }

    p_out_batch = node->m_pCurrentBatch;
    p_scan_batch = node->m_pScanBatch;

    // update cstore scan timing flag
    node->m_CStore->SetTiming(node);

    ExprDoneCond done = ExprSingleResult;
    /*
     * for function-returning-set.
     */
    if (node->ps.ps_vec_TupFromTlist) {
        Assert(node->ps.ps_ProjInfo);
        p_out_batch = ExecVecProject(node->ps.ps_ProjInfo, true, &done);
        if (p_out_batch->m_rows > 0) {
            return p_out_batch;
        }

        node->ps.ps_vec_TupFromTlist = false;
    }

restart:

    // We don't go through the regular ExecScan interface as we will handle all
    // common code ourselves here
    //
    p_scan_batch->Reset(true);
    p_out_batch->Reset(true);
    node->ps.ps_ProjInfo->pi_exprContext->current_row = 0;

    if (!node->isSampleScan) {
        node->m_CStore->RunScan(node, p_scan_batch);
    } else {
        /*
         * Sample scan for column table.
         */
        (((ColumnTableSample*)node->sampleScanInfo.tsm_state)->scanVecSample)(p_scan_batch);
    }

    if (node->m_CStore->IsEndScan() && p_scan_batch->m_rows == 0) {
        // scan delta store table if has data
        ScanDeltaStore(node, p_scan_batch, NULL);
        if (p_scan_batch->m_rows == 0)
            return p_out_batch;
    }

    p_scan_batch->FixRowCount();

    // Apply quals and projections
    //
    p_out_batch = ApplyProjectionAndFilter(node, p_scan_batch, &done);

    if (done != ExprEndResult) {
        node->ps.ps_vec_TupFromTlist = (done == ExprMultipleResult);
    }

    /* Response to the stop query flag. */
    if (unlikely(executorEarlyStop()))
        return NULL;

    if (BatchIsNull(p_out_batch)) {
        CHECK_FOR_INTERRUPTS();
        goto restart;
    }

    return p_out_batch;
}

void ReScanDeltaRelation(CStoreScanState* node)
{
    if (node->ss_currentDeltaScanDesc) {
        tableam_scan_rescan((TableScanDesc)(node->ss_currentDeltaScanDesc), NULL);
    }
    node->ss_deltaScan = false;
    node->ss_deltaScanEnd = false;
}

void InitCStoreRelation(CStoreScanState* node, EState* estate, bool idx_flag, Relation parent_rel)
{
    Relation curr_rel;
    Relation curr_part_rel = NULL;
    CStoreScan* plan = (CStoreScan*)node->ps.plan;
    bool is_target_rel = false;
    LOCKMODE lock_mode = AccessShareLock;
    TableScanDesc curr_scan_desc = NULL;

    is_target_rel = ExecRelationIsTargetRelation(estate, plan->scanrelid);

    if (!node->isPartTbl) {
        /*
         * get the relation object id from the relid'th entry in the range table,
         * open that relation and acquire appropriate lock on it.
         */
        if (!idx_flag) {
            curr_rel = ExecOpenScanRelation(estate, plan->scanrelid);
        } else {
            curr_rel = index_open(plan->scanrelid, AccessShareLock);
            Oid p_sort_idx_rel_oid = curr_rel->rd_rel->relcudescrelid;
            Assert(p_sort_idx_rel_oid != InvalidOid);
            index_close(curr_rel, NoLock);
            curr_rel = heap_open(p_sort_idx_rel_oid, AccessShareLock);
        }
        /* add qual for redis */
        if (u_sess->attr.attr_sql.enable_cluster_resize && RelationInRedistribute(curr_rel)) {
            List* new_qual = NIL;

            new_qual = eval_ctid_funcs(curr_rel, node->ps.plan->qual, &node->rangeScanInRedis);
            node->ps.qual = (List*)ExecInitVecExpr((Expr*)new_qual, (PlanState*)&node->ps);
        } else {
            node->ps.qual = (List*)ExecInitVecExpr((Expr*)node->ps.plan->qual, (PlanState*)&node->ps);
        }
    } else {
        /* get the relation object id from the relid'th entry in the range table */
        if (!idx_flag)
            curr_rel = ExecOpenScanRelation(estate, plan->scanrelid);
        else
            curr_rel = index_open(plan->scanrelid, AccessShareLock);

        /* initiate partition list */
        node->partitions = NULL;

        if (!is_target_rel) {
            lock_mode = AccessShareLock;
        } else {
            switch (estate->es_plannedstmt->commandType) {
                case CMD_UPDATE:
                case CMD_DELETE:
                case CMD_MERGE:
                    lock_mode = RowExclusiveLock;
                    break;

                case CMD_SELECT:
                    lock_mode = AccessShareLock;
                    break;

                default:
                    ereport(ERROR,
                        (errmodule(MOD_VEC_EXECUTOR),
                            errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("invalid operation on partition, allowed are UPDATE/DELETE/SELECT/MERGE")));
            }
        }
        node->lockMode = lock_mode;

        if (plan->itrs > 0) {
            Partition part = NULL;
            Partition curr_part = NULL;
            ListCell* cell = NULL;
            PruningResult* resultPlan = NULL;

            if (!idx_flag) {
                if (plan->pruningInfo->expr != NULL) {
                    resultPlan = GetPartitionInfo(plan->pruningInfo, estate, curr_rel);
                } else {
                    resultPlan = plan->pruningInfo;
                }
                List* part_seqs = resultPlan->ls_rangeSelectedPartitions;
                foreach (cell, part_seqs) {
                    Oid tbl_part_id = InvalidOid;
                    int part_seq = lfirst_int(cell);

                    tbl_part_id = getPartitionOidFromSequence(curr_rel, part_seq, INVALID_PARTITION_NO);
                    part = partitionOpen(curr_rel, tbl_part_id, lock_mode);
                    node->partitions = lappend(node->partitions, part);
                }
                if (resultPlan->ls_rangeSelectedPartitions != NULL) {
                    node->part_id = resultPlan->ls_rangeSelectedPartitions->length;
                } else {
                    node->part_id = 0;
                }
                if (node->partitions == NULL)
                    ereport(ERROR,
                        (errmodule(MOD_VEC_EXECUTOR),
                            errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                            errmsg("Fail to find partition from sequence.")));

                /* construct HeapScanDesc for first partition */
                curr_part = (Partition)list_nth(node->partitions, 0);
                curr_part_rel = partitionGetRelation(curr_rel, curr_part);
                node->ss_currentPartition = curr_part_rel;

                /* add qual for redis */
                if (u_sess->attr.attr_sql.enable_cluster_resize && RelationInRedistribute(curr_part_rel)) {
                    List* new_qual = NIL;

                    new_qual = eval_ctid_funcs(curr_part_rel, node->ps.plan->qual, &node->rangeScanInRedis);
                    node->ps.qual = (List*)ExecInitVecExpr((Expr*)new_qual, (PlanState*)&node->ps);
                } else {
                    node->ps.qual = (List*)ExecInitVecExpr((Expr*)node->ps.plan->qual, (PlanState*)&node->ps);
                }

                if (!node->isSampleScan) {
                    curr_scan_desc = tableam_scan_begin(curr_part_rel, estate->es_snapshot, 0, NULL);
                } else {
                    curr_scan_desc = (TableScanDesc)InitSampleScanDesc((ScanState*)node, curr_part_rel);
                }
            } else {
                Assert(parent_rel);

                // Now curr_rel is the index relation of logical table (parent relation)
                // parent_rel is the logical heap relation
                //
                if (plan->pruningInfo->expr != NULL) {
                    resultPlan = GetPartitionInfo(plan->pruningInfo, estate, parent_rel);
                } else {
                    resultPlan = plan->pruningInfo;
                }
                List* part_seqs = resultPlan->ls_rangeSelectedPartitions;
                foreach (cell, part_seqs) {
                    Oid tbl_part_id = InvalidOid;
                    int part_seq = lfirst_int(cell);

                    tbl_part_id = getPartitionOidFromSequence(parent_rel, part_seq, INVALID_PARTITION_NO);
                    part = partitionOpen(parent_rel, tbl_part_id, lock_mode);
                    Oid part_idx_oid = getPartitionIndexOid(plan->scanrelid, part->pd_id);
                    Assert(OidIsValid(part_idx_oid));
                    partitionClose(parent_rel, part, NoLock);

                    Partition part_idx = partitionOpen(curr_rel, part_idx_oid, lock_mode);
                    Oid idx_oid = part_idx->pd_part->relcudescrelid;
                    partitionClose(curr_rel, part_idx, NoLock);

                    Relation idx_rel = heap_open(idx_oid, lock_mode);
                    node->partitions = lappend(node->partitions, idx_rel);
                }

                if (resultPlan->ls_rangeSelectedPartitions != NULL) {
                    node->part_id = resultPlan->ls_rangeSelectedPartitions->length;
                } else {
                    node->part_id = 0;
                }
                curr_part_rel = (Relation)list_nth(node->partitions, 0);
                /* add qual for redis */
                if (u_sess->attr.attr_sql.enable_cluster_resize && RelationInRedistribute(curr_part_rel)) {
                    List* new_qual = NIL;

                    new_qual = eval_ctid_funcs(curr_part_rel, node->ps.plan->qual, &node->rangeScanInRedis);
                    node->ps.qual = (List*)ExecInitVecExpr((Expr*)new_qual, (PlanState*)&node->ps);
                } else {
                    node->ps.qual = (List*)ExecInitVecExpr((Expr*)node->ps.plan->qual, (PlanState*)&node->ps);
                }
                // Remove it ??
                //
                node->ss_currentPartition = curr_part_rel;
            }
        } else {
            node->ss_currentPartition = NULL;
            node->ps.qual = (List*)ExecInitVecExpr((Expr*)node->ps.plan->qual, (PlanState*)&node->ps);
        }
    }

    if (node->isPartTbl) {
        node->ss_currentRelation = curr_part_rel;
        node->ss_partition_parent = curr_rel;
    } else {
        node->ss_currentRelation = curr_rel;
        node->ss_partition_parent = NULL;
    }

    node->ss_currentScanDesc = curr_scan_desc;
    ExecAssignScanType(node, RelationGetDescr(curr_rel));
}

void InitScanDeltaRelation(CStoreScanState* node, EState* estate)
{
    Relation delta_rel;
    TableScanDesc delta_scan_desc;
    Relation cstore_rel = node->ss_currentRelation;

    if (node->ss_currentRelation == NULL)
        return;

    delta_rel = heap_open(cstore_rel->rd_rel->reldeltarelid, AccessShareLock);
    delta_scan_desc = tableam_scan_begin(delta_rel, estate->es_snapshot, 0, NULL);

    node->ss_currentDeltaRelation = delta_rel;
    node->ss_currentDeltaScanDesc = delta_scan_desc;
    node->ss_deltaScanEnd = false;
}

/* ----------------------------------------------------------------
 *      ExecInitCStoreScan
 * ----------------------------------------------------------------
 */
CStoreScanState* ExecInitCStoreScan(
    CStoreScan* node, Relation parent_heap_rel, EState* estate, int eflags, bool idx_flag, bool codegen_in_up_level)
{
    CStoreScanState* scan_stat = NULL;
    PlanState* plan_stat = NULL;
    ScalarDesc unknown_desc;
    TableSampleClause* tsc = NULL;

    // Column store can only be a leaf node
    //
    Assert(outerPlan(node) == NULL);
    Assert(innerPlan(node) == NULL);

    // There is no reverse scan with column store
    //
    Assert(!ScanDirectionIsBackward(estate->es_direction));

    // Create state structure
    //
    scan_stat = makeNode(CStoreScanState);
    scan_stat->ps.plan = (Plan*)node;
    scan_stat->ps.state = estate;
    scan_stat->ps.vectorized = true;
    scan_stat->isPartTbl = node->isPartTbl;
    plan_stat = &scan_stat->ps;
    scan_stat->partScanDirection = node->partScanDirection;
    scan_stat->m_isReplicaTable = node->is_replica_table;
    scan_stat->rangeScanInRedis = {false,0,0};
    if (!node->tablesample) {
        scan_stat->isSampleScan = false;
    } else {
        scan_stat->isSampleScan = true;
        tsc = node->tablesample;
    }

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &scan_stat->ps);

    // Allocate vector for qualification results
    //
    ExecAssignVectorForExprEval(scan_stat->ps.ps_ExprContext);

    // initialize child expressions
    //
    scan_stat->ps.targetlist = (List*)ExecInitVecExpr((Expr*)node->plan.targetlist, (PlanState*)scan_stat);

    if (node->tablesample) {
        scan_stat->sampleScanInfo.args = (List*)ExecInitExprByRecursion((Expr*)tsc->args, (PlanState*)scan_stat);
        scan_stat->sampleScanInfo.repeatable = ExecInitExprByRecursion(tsc->repeatable, (PlanState*)scan_stat);

        scan_stat->sampleScanInfo.sampleType = tsc->sampleType;

        /*
         * Initialize ColumnTableSample
         */
        if (scan_stat->sampleScanInfo.tsm_state == NULL) {
            scan_stat->sampleScanInfo.tsm_state = New(CurrentMemoryContext) ColumnTableSample(scan_stat);
        }
    }

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &scan_stat->ps);
    ExecInitScanTupleSlot(estate, (ScanState*)scan_stat);

    /*
     * initialize scan relation
     */
    InitCStoreRelation(scan_stat, estate, idx_flag, parent_heap_rel);
    scan_stat->ps.ps_vec_TupFromTlist = false;

#ifdef ENABLE_LLVM_COMPILE
    /*
     * First, not only consider the LLVM native object, but also consider the cost of
     * the LLVM compilation time. We will not use LLVM optimization if there is
     * not enough number of row. Second, consider codegen after we get some information
     * about the scanned relation.
     */
    scan_stat->jitted_vecqual = NULL;
    llvm::Function* jitted_vecqual = NULL;
    dorado::GsCodeGen* llvm_code_gen = (dorado::GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    bool consider_codegen = false;

    /*
     * Check whether we should do codegen here and codegen is allowed for quallist expr.
     * In case of codegen_in_up_level is true, we do not even have to do codegen for target list.
     */
    if (!codegen_in_up_level) {
        consider_codegen =
            CodeGenThreadObjectReady() &&
            CodeGenPassThreshold(((Plan*)node)->plan_rows, estate->es_plannedstmt->num_nodes, ((Plan*)node)->dop);
        if (consider_codegen) {
            jitted_vecqual = dorado::VecExprCodeGen::QualCodeGen((List*)scan_stat->ps.qual, (PlanState*)scan_stat);
            if (jitted_vecqual != NULL)
                llvm_code_gen->addFunctionToMCJit(jitted_vecqual, reinterpret_cast<void**>(&(scan_stat->jitted_vecqual)));
        }
    }
#endif

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(
            &scan_stat->ps,
            scan_stat->ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops);

    if (node->isPartTbl && scan_stat->ss_currentRelation == NULL) {
        // no data ,just return;
        return scan_stat;
    }

    /*
     * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
     * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
     * references to nonexistent indexes.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return scan_stat;

    // Init result batch and work batch. Work batch has to contain all columns as qual is
    // running against it.
    // we shall avoid with this after we fix projection elimination.
    //
    scan_stat->m_pCurrentBatch = New(CurrentMemoryContext)
        VectorBatch(CurrentMemoryContext, scan_stat->ps.ps_ResultTupleSlot->tts_tupleDescriptor);
    scan_stat->m_pScanBatch =
        New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, scan_stat->ss_currentRelation->rd_att);
    for (int vecIndex = 0; vecIndex < scan_stat->m_pScanBatch->m_cols; vecIndex++) {
        FormData_pg_attribute* attr = NULL;
        attr = &scan_stat->ss_currentRelation->rd_att->attrs[vecIndex];

        // Hack!! move me out to update pg_attribute instead
        //
        if (attr->atttypid == TIDOID) {
            attr->attlen = sizeof(int64);
            attr->attbyval = true;
        }
    }

    plan_stat->ps_ProjInfo = ExecBuildVecProjectionInfo(plan_stat->targetlist,
        node->plan.qual,
        plan_stat->ps_ExprContext,
        plan_stat->ps_ResultTupleSlot,
        scan_stat->ss_ScanTupleSlot->tts_tupleDescriptor);

    /* Set min/max optimization info to ProjectionInfo's pi_maxOrmin. */
    plan_stat->ps_ProjInfo->pi_maxOrmin = node->minMaxInfo;

    /* If exist sysattrlist, not consider LLVM optimization, while the codegen process will be terminated in
     * VarJittable */
    if (plan_stat->ps_ProjInfo->pi_sysAttrList) {
        scan_stat->m_pScanBatch->CreateSysColContainer(CurrentMemoryContext, plan_stat->ps_ProjInfo->pi_sysAttrList);
    }

#ifdef ENABLE_LLVM_COMPILE
    /**
     * Since we separate the target list elements into simple var references and
     * generic expression, we only need to deal the generic expression with LLVM
     * optimization.
     */
    llvm::Function* jitted_vectarget = NULL;
    if (consider_codegen && plan_stat->ps_ProjInfo->pi_targetlist) {
        /*
         * check if codegen is allowed for generic targetlist expr.
         * Since targetlist is evaluated in projection, add this function to
         * ps_ProjInfo.
         */
        jitted_vectarget =
            dorado::VecExprCodeGen::TargetListCodeGen(plan_stat->ps_ProjInfo->pi_targetlist, (PlanState*)scan_stat);
        if (jitted_vectarget != NULL)
            llvm_code_gen->addFunctionToMCJit(
                jitted_vectarget, reinterpret_cast<void**>(&(plan_stat->ps_ProjInfo->jitted_vectarget)));
    }
#endif

    scan_stat->m_pScanRunTimeKeys = NULL;
    scan_stat->m_ScanRunTimeKeysNum = 0;
    scan_stat->m_ScanRunTimeKeysReady = false;
    scan_stat->csss_ScanKeys = NULL;
    scan_stat->csss_NumScanKeys = 0;

    ExecCStoreBuildScanKeys(scan_stat,
        node->cstorequal,
        &scan_stat->csss_ScanKeys,
        &scan_stat->csss_NumScanKeys,
        &scan_stat->m_pScanRunTimeKeys,
        &scan_stat->m_ScanRunTimeKeysNum);

    scan_stat->m_CStore = New(CurrentMemoryContext) CStore();
    scan_stat->m_CStore->InitScan(scan_stat, GetActiveSnapshot());
    OptimizeProjectionAndFilter(scan_stat);

    /*
     * initialize delta relation
     */
    InitScanDeltaRelation(scan_stat, estate->es_snapshot);

    return scan_stat;
}

/* ----------------------------------------------------------------
 *      ExecEndCStoreScan
 *
 *      frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void ExecEndCStoreScan(CStoreScanState* node, bool idx_flag)
{
    Relation relation;
    TableScanDesc scan_desc;

    /*
     * get information from node
     */
    if (node->isPartTbl) {
        node->ss_currentRelation = node->ss_partition_parent;
    }

    relation = node->ss_currentRelation;
    scan_desc = (TableScanDesc)(node->ss_currentScanDesc);
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
     * release CStoreScan
     */
    if (node->m_CStore) {
        DELETE_EX(node->m_CStore);
    }

    /*
     * close heap scan
     */
    if (node->isPartTbl) {
        if (PointerIsValid(node->partitions)) {
            if (!idx_flag) {
                Assert(scan_desc);
                tableam_scan_end(scan_desc);
                Assert(node->ss_currentPartition);
                releaseDummyRelation(&(node->ss_currentPartition));
                releasePartitionList(node->ss_currentRelation, &(node->partitions), node->lockMode);
            } else {
                ListCell* cell = NULL;
                Relation rel;

                foreach (cell, node->partitions) {
                    rel = (Relation)lfirst(cell);
                    if (rel->rd_refcnt > 0) {
                        // destory partition ssd cache
                        relation_close(rel, NoLock);
                    }
                }
                list_free_ext(node->partitions);
            }
        }
    }

    /*
     * close the heap relation.
     */
    ExecCloseScanRelation(relation);

    EndScanDeltaRelation(node);
}

/* Build the cstore scan keys from the qual. */
static void ExecCStoreBuildScanKeys(CStoreScanState* scan_stat, List* quals, CStoreScanKey* scan_keys, int* num_scan_keys,
    CStoreScanRunTimeKeyInfo** runtime_key_info, int* runtime_keys_num)
{
    ListCell* lc = NULL;
    CStoreScanKey tmp_scan_keys;
    int n_scan_keys;
    int j;
    List* accessed_varnos = NIL;
    CStoreScanRunTimeKeyInfo* runtime_info = NULL;
    int runtime_keys;
    int max_runtime_keys;

    n_scan_keys = list_length(quals);
    // only if the indexFlag is flase and n_scan_keys is zero, we should reinit scan_keys and num_scan_keys
    //
    if (n_scan_keys == 0) {
        return;
    }

    Assert(*scan_keys == NULL);
    Assert(*num_scan_keys == 0);
    tmp_scan_keys = (CStoreScanKey)palloc0(n_scan_keys * sizeof(CStoreScanKeyData));

    accessed_varnos = scan_stat->ps.ps_ProjInfo->pi_acessedVarNumbers;

    runtime_info = *runtime_key_info;
    runtime_keys = *runtime_keys_num;
    max_runtime_keys = runtime_keys;
    j = 0;
    foreach (lc, quals) {
        Expr* clause = (Expr*)lfirst(lc);
        CStoreScanKey this_scan_key = &tmp_scan_keys[j++];
        Oid opno;
        RegProcedure opfunc_id;

        Expr* leftop = NULL;
        Expr* rightop = NULL;
        AttrNumber varattno;

        if (IsA(clause, OpExpr)) {
            uint16 flags = 0;  // no use,default 0
            Datum scan_val = 0;
            CStoreStrategyNumber strategy = InvalidCStoreStrategy;
            ListCell* lcell = NULL;
            AttrNumber count_no = 0;
            Oid left_type = InvalidOid;

            opno = ((OpExpr*)clause)->opno;
            opfunc_id = ((OpExpr*)clause)->opfuncid;

            /* Leftop should be var. Has been checked */
            leftop = (Expr*)get_leftop(clause);
            if (leftop && IsA(leftop, RelabelType))
                leftop = ((RelabelType*)leftop)->arg;
            if (leftop == NULL)
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("The left value of the expression should not be NULL")));

            /* The attribute numbers of column in cstore scan is a sequence.begin with 0. */
            varattno = ((Var*)leftop)->varattno;
            foreach (lcell, accessed_varnos) {
                if ((int)varattno == lfirst_int(lcell)) {
                    varattno = count_no;
                    break;
                }
                count_no++;
            }

            left_type = ((Var*)leftop)->vartype;

            /* Rightop should be const.Has been checked */
            rightop = (Expr*)get_rightop(clause);
            if (rightop == NULL)
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("Fail to get the right value of the expression")));

            if (IsA(rightop, Const)) {
                scan_val =
                    convert_scan_key_int64_if_need(left_type, ((Const*)rightop)->consttype, ((Const*)rightop)->constvalue);
                flags = ((Const*)rightop)->constisnull;
            } else if (IsA(rightop, RelabelType)) {
                rightop = ((RelabelType*)rightop)->arg;
                Assert(rightop != NULL);
                scan_val =
                    convert_scan_key_int64_if_need(left_type, ((Const*)rightop)->consttype, ((Const*)rightop)->constvalue);
                flags = ((Const*)rightop)->constisnull;
            } else if (nodeTag(rightop) == T_Param && ((Param*)rightop)->paramkind == PARAM_EXTERN) {
                scan_val = GetParamExternConstValue(left_type, rightop, &(scan_stat->ps), &flags);
            } else if (nodeTag(rightop) == T_Param) {
                // when the rightop is T_Param, the scan_val will be filled until rescan happens.
                //
                if (runtime_keys >= max_runtime_keys) {
                    if (max_runtime_keys == 0) {
                        max_runtime_keys = 8;
                        runtime_info =
                            (CStoreScanRunTimeKeyInfo*)palloc(max_runtime_keys * sizeof(CStoreScanRunTimeKeyInfo));
                    } else {
                        max_runtime_keys *= 2;
                        runtime_info = (CStoreScanRunTimeKeyInfo*)repalloc(
                            runtime_info, max_runtime_keys * sizeof(CStoreScanRunTimeKeyInfo));
                    }
                }
                runtime_info[runtime_keys].scan_key = this_scan_key;
                runtime_info[runtime_keys].key_expr = ExecInitExpr(rightop, NULL);
                runtime_keys++;
                scan_val = (Datum)0;
            } else
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Not support pushing  predicate with none-const external param")));
            /* Get strategy number. */
            strategy = GetCStoreScanStrategyNumber(opno);
            /* initialize the scan key's fields appropriately */
            CStoreScanKeyInit(this_scan_key,
                flags,
                varattno,
                strategy,
                ((OpExpr*)clause)->inputcollid,
                opfunc_id,
                scan_val,
                left_type);
        } else {
            pfree_ext(tmp_scan_keys);
            tmp_scan_keys = NULL;
            n_scan_keys = 0;

            break;
        }
    }

    *scan_keys = tmp_scan_keys;
    *num_scan_keys = n_scan_keys;
    *runtime_key_info = runtime_info;
    *runtime_keys_num = runtime_keys;
}

/* No metadata for the operator strategy. The followings are temporary codes.
 */
static CStoreStrategyNumber GetCStoreScanStrategyNumber(Oid opno)
{
    CStoreStrategyNumber strategy_number = InvalidCStoreStrategy;
    Relation hdesc;
    ScanKeyData skey[1];
    SysScanDesc sysscan;
    HeapTuple tuple;
    Form_pg_operator fpo;

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(opno));
    hdesc = heap_open(OperatorRelationId, AccessShareLock);
    sysscan = systable_beginscan(hdesc, OperatorOidIndexId, true, NULL, 1, skey);

    tuple = systable_getnext(sysscan);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
            (errmodule(MOD_VEC_EXECUTOR),
                errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("could not find tuple for operator %u", opno)));

    fpo = (Form_pg_operator)GETSTRUCT(tuple);

    if (strncmp(NameStr(fpo->oprname), "<", NAMEDATALEN) == 0)
        strategy_number = CStoreLessStrategyNumber;
    else if (strncmp(NameStr(fpo->oprname), ">", NAMEDATALEN) == 0)
        strategy_number = CStoreGreaterStrategyNumber;
    else if (strncmp(NameStr(fpo->oprname), "=", NAMEDATALEN) == 0)
        strategy_number = CStoreEqualStrategyNumber;
    else if (strncmp(NameStr(fpo->oprname), ">=", NAMEDATALEN) == 0)
        strategy_number = CStoreGreaterEqualStrategyNumber;
    else if (strncmp(NameStr(fpo->oprname), "<=", NAMEDATALEN) == 0)
        strategy_number = CStoreLessEqualStrategyNumber;
    else
        strategy_number = InvalidCStoreStrategy;

    systable_endscan(sysscan);
    heap_close(hdesc, AccessShareLock);

    return strategy_number;
}

static Datum GetParamExternConstValue(Oid left_type, Expr* expr, PlanState* ps, uint16* flag)
{
    Param* expression = (Param*)expr;
    int param_id = expression->paramid;
    ParamListInfo param_info = ps->state->es_param_list_info;

    /*
     * PARAM_EXTERN parameters must be sought in ecxt_param_list_info.
     */
    if (param_info && param_id > 0 && param_id <= param_info->numParams) {
        ParamExternData* prm = &param_info->params[param_id - 1];
        *flag = prm->isnull;
        if (OidIsValid(prm->ptype)) {
            return convert_scan_key_int64_if_need(left_type, prm->ptype, prm->value);
        }

    } else {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("no value found for parameter %d", param_id)));
    }

    return 0;
}

void ExecReCStoreSeqScan(CStoreScanState* node)
{
    node->m_CStore->InitReScan();
}

void ExecCStoreScanEvalRuntimeKeys(ExprContext* expr_ctx, CStoreScanRunTimeKeyInfo* runtime_keys, int num_runtime_keys)
{
    int j;
    MemoryContext old_ctx;

    /* We want to keep the key values in per-tuple memory */
    old_ctx = MemoryContextSwitchTo(expr_ctx->ecxt_per_tuple_memory);

    for (j = 0; j < num_runtime_keys; j++) {
        CStoreScanKey scan_key = runtime_keys[j].scan_key;
        ExprState* key_expr = runtime_keys[j].key_expr;
        Datum scan_val;
        bool is_null = false;

        /*
         * For each run-time key, extract the run-time expression and evaluate
         * it with respect to the current context.	We then stick the result
         * into the proper scan key.
         *
         * Note: the result of the eval could be a pass-by-ref value that's
         * stored in some outer scan's tuple, not in
         * expr_ctx->ecxt_per_tuple_memory.  We assume that the outer tuple
         * will stay put throughout our scan.  If this is wrong, we could copy
         * the result into our context explicitly, but I think that's not
         * necessary.
         *
         * It's also entirely possible that the result of the eval is a
         * toasted value.  In this case we should forcibly detoast it, to
         * avoid repeat detoastings each time the value is examined by an
         * index support function.
         */
        scan_val = ExecEvalExpr(key_expr, expr_ctx, &is_null, NULL);
        if (is_null)
            scan_key->cs_flags |= SK_ISNULL;
        else
            scan_key->cs_flags &= ~SK_ISNULL;

        scan_key->cs_argument =
            convert_scan_key_int64_if_need(scan_key->cs_left_type, ((Param*)key_expr->expr)->paramtype, scan_val);
    }

    (void)MemoryContextSwitchTo(old_ctx);
}

void ExecReSetRuntimeKeys(CStoreScanState* node)
{
    ExprContext* expr_ctx = node->ps.ps_ExprContext;
    ExecCStoreScanEvalRuntimeKeys(expr_ctx, node->m_pScanRunTimeKeys, node->m_ScanRunTimeKeysNum);
}

void ExecReScanCStoreScan(CStoreScanState* node)
{
    TableScanDesc scan;

    if (node->isSampleScan) {
        /* Remember we need to do BeginSampleScan again (if we did it at all) */
        (((ColumnTableSample*)node->sampleScanInfo.tsm_state)->resetVecSampleScan)();
    }

    if (node->m_ScanRunTimeKeysNum != 0) {
        ExecReSetRuntimeKeys(node);
    }
    node->m_ScanRunTimeKeysReady = true;

    scan = (TableScanDesc)(node->ss_currentScanDesc);
    if (node->isPartTbl && !(((Scan *)node->ps.plan)->partition_iterator_elimination)) {
        if (PointerIsValid(node->partitions)) {
            /* end scan the prev partition first, */
            tableam_scan_end(scan);
            EndScanDeltaRelation(node);

            /* finally init Scan for the next partition */
            ExecInitNextPartitionForCStoreScan(node);
        }
    }

    ExecReCStoreSeqScan(node);
    ReScanDeltaRelation(node);
}

static void ExecInitNextPartitionForCStoreScan(CStoreScanState* node)
{
    Partition curr_part = NULL;
    Relation curr_part_rel = NULL;
    TableScanDesc curr_scan_desc = NULL;
    int param_no = -1;
    ParamExecData* param = NULL;
    CStoreScan* plan = NULL;

    plan = (CStoreScan*)node->ps.plan;

    /* get partition sequnce */
    param_no = plan->plan.paramno;
    param = &(node->ps.state->es_param_exec_vals[param_no]);
    node->currentSlot = (int)param->value;

    /* construct HeapScanDesc for new partition */
    curr_part = (Partition)list_nth(node->partitions, node->currentSlot);
    curr_part_rel = partitionGetRelation(node->ss_partition_parent, curr_part);

    releaseDummyRelation(&(node->ss_currentPartition));
    node->ss_currentPartition = curr_part_rel;
    node->ss_currentRelation = curr_part_rel;

    /* add qual for redis */
    if (u_sess->attr.attr_sql.enable_cluster_resize && RelationInRedistribute(curr_part_rel)) {
        List* new_qual = NIL;

        new_qual = eval_ctid_funcs(curr_part_rel, node->ps.plan->qual, &node->rangeScanInRedis);
        node->ps.qual = (List*)ExecInitVecExpr((Expr*)new_qual, (PlanState*)&node->ps);
    }

    if (!node->isSampleScan) {
        curr_scan_desc = tableam_scan_begin(curr_part_rel, node->ps.state->es_snapshot, 0, NULL);
    } else {
        curr_scan_desc = InitSampleScanDesc((ScanState*)node, curr_part_rel);
    }

    /* update partition scan-related fileds in SeqScanState  */
    node->ss_currentScanDesc = curr_scan_desc;
    node->m_CStore->InitPartReScan(node->ss_currentRelation);

    /* reinit delta scan */
    InitScanDeltaRelation(node, node->ps.state->es_snapshot);
}
