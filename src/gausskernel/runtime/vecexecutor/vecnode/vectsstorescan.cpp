/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * vectsstorescan.cpp
 *      tsstore scan
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vectsstorescan.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "codegen/gscodegen.h"
#include "codegen/vecexprcodegen.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_proc.h"
#include "vecexecutor/vectsstorescan.h"
#include "executor/executor.h"
#include "executor/node/nodeModifyTable.h"
#include "nodes/plannodes.h"
#include "nodes/nodeFuncs.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "vecexecutor/vecnodes.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vecnodecstorescan.h"
#include "access/tableam.h"
#include "optimizer/pruning.h"

#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/cache/queryid_cachemgr.h"
#include "tsdb/storage/ts_store_search.h"
#include "tsdb/common/ts_tablecmds.h"
#endif   /* ENABLE_MULTIPLE_NODES */

extern bool CodeGenThreadObjectReady();
extern bool CodeGenPassThreshold(double rows, int dn_num, int dop);

#ifdef ENABLE_MULTIPLE_NODES
static void init_next_partition_for_tsstore_scan(TsStoreScanState* node);
static void init_tsstore_relation(TsStoreScanState* node, EState* estate);
static TsStoreScanState* build_tsstore_scan_state(TsStoreScan* node, EState* estate);
static void opt_orderby(TsStoreScan* node, TsStoreScanState* scanstate);
static LOCKMODE check_command_type(EState* estate);
#endif   /* ENABLE_MULTIPLE_NODES */

#define TIMING_VECTSSTORE_SCAN(_node) (NULL != (_node)->ps.instrument && (_node)->ps.instrument->need_timer)

#define VECTSSTORE_SCAN_TRACE_START(_node, _desc_id)                  \
    do {                                                             \
        if (unlikely(TIMING_VECTSSTORE_SCAN(_node))) {                \
            TRACK_START((_node)->ps.plan->plan_node_id, (_desc_id)); \
        }                                                            \
    } while (0)

#define VECTSSTORE_SCAN_TRACE_END(_node, _desc_id)                  \
    do {                                                           \
        if (unlikely(TIMING_VECTSSTORE_SCAN(_node))) {              \
            TRACK_END((_node)->ps.plan->plan_node_id, (_desc_id)); \
        }                                                          \
    } while (0)

#ifdef ENABLE_MULTIPLE_NODES

/*
 * Obtain current partitions, partition_parent, ScanDesc info.
 */
static void init_tsstore_relation(TsStoreScanState* node, EState* estate)
{
    Relation currentRelation;
    Relation currentPartRel = NULL;
    TsStoreScan* plan = (TsStoreScan*)node->ps.plan;
    bool isTargetRel = false;
    LOCKMODE lockmode = AccessShareLock;
    TableScanDesc currentScanDesc = NULL;

    isTargetRel = ExecRelationIsTargetRelation(estate, plan->scanrelid);

    /* get the relation object id from the relid'th entry in the range table */
    currentRelation = ExecOpenScanRelation(estate, plan->scanrelid);

    /* initiate partition list */
    node->partitions = NULL;

    if (!isTargetRel) {
        lockmode = AccessShareLock;
    } else {
        lockmode = check_command_type(estate);
    }
    node->lockMode = lockmode;

    if (plan->itrs > 0) {
        Partition part = NULL;
        Partition currentPart = NULL;
        PruningResult* resultPlan = NULL;
        if (plan->pruningInfo->expr != NULL) {
            resultPlan = GetPartitionInfo(plan->pruningInfo, estate, currentRelation);
        } else {
            resultPlan = plan->pruningInfo;
        }

        ListCell* cell1 = NULL;
        ListCell* cell2 = NULL;
        List* part_seqs = resultPlan->ls_rangeSelectedPartitions;
        List* partitionnos = resultPlan->ls_selectedPartitionnos;
        Assert(list_length(part_seqs) == list_length(partitionnos));
        /* partitions info is initialized */
        forboth (cell1, part_seqs, cell2, partitionnos) {
            Oid tablepartitionid = InvalidOid;
            int partSeq = lfirst_int(cell1);
            int partitionno = lfirst_int(cell2);

            tablepartitionid = getPartitionOidFromSequence(currentRelation, partSeq, partitionno);
            part = PartitionOpenWithPartitionno(currentRelation, tablepartitionid, partitionno, lockmode);
            node->partitions = lappend(node->partitions, part);
        }

        if (resultPlan->ls_rangeSelectedPartitions != NULL) {
            node->part_id = resultPlan->ls_rangeSelectedPartitions->length;
        } else {
            node->part_id = 0;
        }
        if (NULL == node->partitions)
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("Fail to find partition from sequence.")));

        /* construct HeapScanDesc for first partition */
        currentPart = (Partition)list_nth(node->partitions, 0);
        currentPartRel = partitionGetRelation(currentRelation, currentPart);
        node->ss_currentPartition = currentPartRel;

        /* add qual for redis */
        if (u_sess->attr.attr_sql.enable_cluster_resize && RelationInRedistribute(currentPartRel)) {
            List* new_qual = NIL;

            new_qual = eval_ctid_funcs(currentPartRel, node->ps.plan->qual, &node->rangeScanInRedis);
            node->ps.qual = (List*)ExecInitVecExpr((Expr*)new_qual, (PlanState*)&node->ps);
        } else {
            node->ps.qual = (List*)ExecInitVecExpr((Expr*)node->ps.plan->qual, (PlanState*)&node->ps);
        }

        currentScanDesc = tableam_scan_begin(currentPartRel, estate->es_snapshot, 0, NULL);

    } else {
        node->ss_currentPartition = NULL;
        node->ps.qual = (List*)ExecInitVecExpr((Expr*)node->ps.plan->qual, (PlanState*)&node->ps);
    }

    node->ss_currentRelation = currentPartRel;
    node->ss_partition_parent = currentRelation;


    node->ss_currentScanDesc = currentScanDesc;

    ExecAssignScanType(node, RelationGetDescr(currentRelation));
}

/*
 * Obtain the time range in the query filter for timeseries table to which
 * may facilitate subsequent CU query optimization.
 */
static TimeRange* get_time_range(TsStoreScanState* node)
{
    ListCell *cell = NULL;
    TimeRange* time_range = New(CurrentMemoryContext) TimeRange();
    time_range->start_timestamp = 0;
    time_range->end_timestamp = GetCurrentTimestamp();
    foreach(cell, node->ps.plan->qual) {
        if (nodeTag(lfirst(cell)) != T_OpExpr) {
            continue;
        }
        ListCell *inner_cell = NULL;
        Oid optr_oid = ((OpExpr*)lfirst(cell))->opno;
        /* qualification of time less than end_timestamp */
        if (optr_oid == TIMESTAMPLTOID   || optr_oid == TIMESTAMPLEOID ||
            optr_oid == TIMESTAMPTZLTOID || optr_oid == TIMESTAMPTZLEOID) {
            TimestampTz end_timestamp = GetCurrentTimestamp();
            foreach(inner_cell, ((OpExpr*)lfirst(cell))->args) {
                Oid data_oid = ((Const*)lfirst(inner_cell))->consttype;
                /*
                 * There is no need to distinguish timestamp and timestamptz, just deal with it as
                 * timestamptz because data processed in system or stored in backend(for timeseries is in CU)
                 * is still of type timestamptz.
                 */
                if (nodeTag(lfirst(inner_cell)) == T_Const && (data_oid == TIMESTAMPOID ||
                                                               data_oid == TIMESTAMPTZOID)) {
                    end_timestamp = ((Const*)lfirst(inner_cell))->constvalue;
                } else if (nodeTag(lfirst(inner_cell)) == T_OpExpr && 
                    (((OpExpr*)lfirst(inner_cell))->opfuncid == TIMESTAMPTZMIINTERVALFUNCOID
                    || ((OpExpr*)lfirst(inner_cell))->opfuncid == TIMESTAMPTZPLINTERVALFUNCOID)) {
                    OpExpr* expr = (OpExpr*)lfirst(inner_cell);
                    Expr* value = evaluate_expr((Expr*)expr, expr->opresulttype, -1, 0);
                    end_timestamp = DatumGetTimestampTz(((Const*)value)->constvalue);
                }
                time_range->end_timestamp =
                    end_timestamp < time_range->end_timestamp ? end_timestamp : time_range->end_timestamp;
            }
        /* qualification of time greater than start_timestamp */
        } else if (optr_oid == TIMESTAMPGTOID   || optr_oid == TIMESTAMPGEOID ||
                   optr_oid == TIMESTAMPTZGTOID || optr_oid == TIMESTAMPTZGEOID) {
            TimestampTz start_timestamp = 0;
            foreach(inner_cell, ((OpExpr*)lfirst(cell))->args) {
                Oid data_oid = ((Const*)lfirst(inner_cell))->consttype;
                /*
                 * There is no need to distinguish timestamp and timestamptz, just deal with it as
                 * timestamptz because data processed in system or stored in backend(for timeseries is in CU)
                 * is still of type timestamptz.
                 */
                if (nodeTag(lfirst(inner_cell)) == T_Const && (data_oid == TIMESTAMPOID ||
                                                               data_oid == TIMESTAMPTZOID)) {
                    start_timestamp = ((Const*)lfirst(inner_cell))->constvalue;
                } else if (nodeTag(lfirst(inner_cell)) == T_OpExpr &&
                    (((OpExpr*)lfirst(inner_cell))->opfuncid == TIMESTAMPTZMIINTERVALFUNCOID
                    || ((OpExpr*)lfirst(inner_cell))->opfuncid == TIMESTAMPTZPLINTERVALFUNCOID)) {
                    OpExpr* expr = (OpExpr*)lfirst(inner_cell);
                    Expr* value = evaluate_expr((Expr*)expr, expr->opresulttype, -1, 0);
                    start_timestamp = DatumGetTimestampTz(((Const*)value)->constvalue);
                }
                time_range->start_timestamp =
                    start_timestamp > time_range->start_timestamp ? start_timestamp : time_range->start_timestamp;
            }
        }
    }
    if (time_range->start_timestamp == 0 && time_range->end_timestamp == GetCurrentTimestamp()) {
        delete time_range;
        time_range = NULL;
    }
    return time_range;
}

static TsStoreScanState* build_tsstore_scan_state(TsStoreScan* node, EState* estate)
{
    TsStoreScanState* scanstate = nullptr;
    // Create state structure
    scanstate = makeNode(TsStoreScanState);
    scanstate->ps.plan = (Plan*)node;
    scanstate->ps.state = estate;
    scanstate->ps.vectorized = true;
    scanstate->isPartTbl = node->isPartTbl;
    scanstate->partScanDirection = node->partScanDirection;
    scanstate->rangeScanInRedis = {false, 0, 0};
    scanstate->sort_by_time_colidx = node->sort_by_time_colidx;
    scanstate->has_sort = node->has_sort;
    scanstate->limit = node->limit;
    scanstate->scaned_tuples = 0;
    scanstate->top_key_func_arg = node->top_key_func_arg;
    scanstate->early_stop = false;
    scanstate->is_simple_scan = node->is_simple_scan;
    scanstate->tags_scan_done = true;
    scanstate->first_scan = true;
    scanstate->time_range = nullptr;
    scanstate->ts_store_search = nullptr;
    scanstate->tag_rows = nullptr;
    scanstate->tag_id_num = 0;
    scanstate->only_scan_tag = (node->series_func_calls > 0);
    scanstate->ts_store_search = New(CurrentMemoryContext) TsStoreSearch();
    scanstate->only_const_col = false;
    if (!node->tablesample) {
        scanstate->isSampleScan = false;
    } else {
        /* Current not support sample scan for tsdb table */
        ereport(ERROR,
            (errmodule(MOD_TIMESERIES), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("Current not support sample scan for tsdb table.")));
    }
    return scanstate;
}

static void init_next_partition_for_tsstore_scan(TsStoreScanState* node)
{
    Partition currentpartition = NULL;
    Relation currentpartitionrel = NULL;
    TableScanDesc currentScanDesc = NULL;
    int paramno = -1;
    ParamExecData* param = NULL;
    TsStoreScan* plan = NULL;

    plan = (TsStoreScan*)node->ps.plan;
    /* get partition sequnce */
    paramno = plan->plan.paramno;
    param = &(node->ps.state->es_param_exec_vals[paramno]);
    node->currentSlot = (int)param->value;
    /* construct HeapScanDesc for new partition */
    currentpartition = (Partition)list_nth(node->partitions, node->currentSlot);
    currentpartitionrel = partitionGetRelation(node->ss_partition_parent, currentpartition);

    releaseDummyRelation(&(node->ss_currentPartition));
    node->ss_currentPartition = currentpartitionrel;
    node->ss_currentRelation = currentpartitionrel;
    if (node->ts_store_search == NULL) {
        node->ts_store_search = New(CurrentMemoryContext) TsStoreSearch();
    }
    node->ts_store_search->init_scan(node);

    if (!node->isSampleScan) {
        currentScanDesc = tableam_scan_begin(currentpartitionrel, node->ps.state->es_snapshot, 0, NULL);
    } else {
        /* Current not support sample scan for tsdb table */
        ereport(ERROR,
            (errmodule(MOD_TIMESERIES), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature"),
                errdetail("Current not support sample scan for tsdb table.")));
    }

    /* update partition scan-related fileds in SeqScanState  */
    node->ss_currentScanDesc = currentScanDesc;
}

static void opt_orderby(TsStoreScan* node, TsStoreScanState* scanstate)
{
    TupleDesc tuple_desc = RelationGetDescr(scanstate->ss_currentRelation);
    TargetEntry* att_target = NULL;
    List *target_list = node->plan.targetlist;

    att_target = (TargetEntry*)list_nth(target_list, scanstate->sort_by_time_colidx - 1);

    for(int i = 0; i < tuple_desc->natts; i++) {
        if (IsA(att_target->expr, Var) && tuple_desc->attrs[i].attkvtype == ATT_KV_TIMETAG &&
            ((Var*)(att_target->expr))->varattno == tuple_desc->attrs[i].attnum) {
            scanstate->early_stop = true;
        }
    }
}

static LOCKMODE check_command_type(EState* estate)
{
    LOCKMODE lockmode = AccessShareLock;
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
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("invalid operation on partition, allowed are UPDATE/DELETE/SELECT")));
    }
    return lockmode;
}

#endif   /* ENABLE_MULTIPLE_NODES */

TsStoreScanState* ExecInitTsStoreScan(TsStoreScan* node, Relation parentHeapRel, EState* estate, int eflags,
    bool indexFlag, bool codegenInUplevel)
{
    TsStoreScanState* scanstate = nullptr;
#ifdef ENABLE_MULTIPLE_NODES
    if (!g_instance.attr.attr_common.enable_tsdb) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("Please enable timeseries first!")));
    }
    PlanState* planstate = nullptr;
    ScalarDesc unknownDesc;

    // ts store can only be a leaf node
    Assert(outerPlan(node) == nullptr);
    Assert(innerPlan(node) == nullptr);

    // There is no reverse scan with column store
    Assert(!ScanDirectionIsBackward(estate->es_direction));

    instr_time start_time, end_time;
    INSTR_TIME_SET_CURRENT(start_time);

    // create TsStoreScanState struct
    scanstate = build_tsstore_scan_state(node, estate);

    /*
     * create expression context for node
     */
    ExecAssignExprContext(estate, &scanstate->ps);
    // Allocate vector for qualification results
    ExecAssignVectorForExprEval(scanstate->ps.ps_ExprContext);
    // initialize child expressions
    scanstate->ps.targetlist = (List*)ExecInitVecExpr((Expr*)node->plan.targetlist, (PlanState*)scanstate);
    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &scanstate->ps);
    ExecInitScanTupleSlot(estate, (ScanState*)scanstate);
    /*
     * initialize scan relation
     */
    init_tsstore_relation(scanstate, estate);
    /* If operation is DELETE, get mark status to block compaction consumer workers */
    if (estate->es_plannedstmt->commandType == CMD_DELETE) {
        ereport(LOG, (errmodule(MOD_TIMESERIES), errcode(ERRCODE_LOG), 
                errmsg("Set delete query id(%lu)", u_sess->debug_query_id)));           
        Tsdb::TableStatus::GetInstance().add_query(u_sess->debug_query_id);
    }
    scanstate->time_range = get_time_range(scanstate);

    scanstate->ps.ps_vec_TupFromTlist = false;
   
    if (scanstate->limit > 0 && scanstate->ps.qual == NULL) {
         /* if is simple sort by timestamp limit n , then earlier stop */
        if (scanstate->sort_by_time_colidx >= 0) {
            opt_orderby(node, scanstate);
        } else if (scanstate->is_simple_scan && !scanstate->has_sort) {
            /* if is simple limit n , then earlier stop */
            scanstate->early_stop = true;
        }
    }
    /*
     * First, not only consider the LLVM native object, but also consider the cost of
     * the LLVM compilation time. We will not use LLVM optimization if there is
     * not enough number of row. Second, consider codegen after we get some information
     * about the scanned relation.
     */
    scanstate->jitted_vecqual = nullptr;
    llvm::Function* jitted_vecqual = nullptr;
    dorado::GsCodeGen* llvmCodeGen = (dorado::GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    bool consider_codegen = false;
    /*
     * Check whether we should do codegen here and codegen is allowed for quallist expr.
     * In case of codegenInUplevel is true, we do not even have to do codegen for target list.
     */
    if (!codegenInUplevel) {
        consider_codegen = CodeGenThreadObjectReady() && 
        CodeGenPassThreshold(((Plan*)node)->plan_rows, estate->es_plannedstmt->num_nodes, ((Plan*)node)->dop);
        if (consider_codegen) {
            jitted_vecqual = dorado::VecExprCodeGen::QualCodeGen(scanstate->ps.qual, (PlanState*)scanstate);
            if (jitted_vecqual != nullptr)
                llvmCodeGen->addFunctionToMCJit(jitted_vecqual, reinterpret_cast<void**>(&(scanstate->jitted_vecqual)));
        }
    }
    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(&scanstate->ps); // make descriptor

    if (node->isPartTbl && scanstate->ss_currentRelation == nullptr) {
        // no data ,just return;
        return scanstate;
    }
    /*
     * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
     * here. This allows an index-advisor plugin to EXPLAIN a plan containing
     * references to nonexistent indexes.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return scanstate;
    // Init result batch and work batch. Work batch has to contain all columns as qual is
    // running against it.
    // we shall avoid with this after we fix projection elimination.
    scanstate->scanBatch = New(CurrentMemoryContext)
        VectorBatch(CurrentMemoryContext, scanstate->ss_currentRelation->rd_att);
    scanstate->currentBatch = New(CurrentMemoryContext)
        VectorBatch(CurrentMemoryContext, scanstate->ps.ps_ResultTupleSlot->tts_tupleDescriptor);
    for (int vecIndex = 0; vecIndex < scanstate->scanBatch->m_cols; vecIndex++) {
        FormData_pg_attribute* attr = nullptr;
        attr = scanstate->ss_currentRelation->rd_att->attrs[vecIndex];

        // Hack!! move me out to update pg_attribute instead
        if (attr->atttypid == TIDOID) {
            attr->attlen = sizeof(int64);
            attr->attbyval = true;
        }
    }

    planstate = &scanstate->ps;
    planstate->ps_ProjInfo = ExecBuildVecProjectionInfo(planstate->targetlist,
        node->plan.qual,
        planstate->ps_ExprContext,
        planstate->ps_ResultTupleSlot,
        scanstate->ss_ScanTupleSlot->tts_tupleDescriptor);
    scanstate->only_const_col = planstate->ps_ProjInfo->pi_const;
    /* If exist sysattrlist, not consider LLVM optimization, while the codegen process will be terminated in
     * VarJittable*/
    if (planstate->ps_ProjInfo->pi_sysAttrList) {
        scanstate->scanBatch->CreateSysColContainer(CurrentMemoryContext, planstate->ps_ProjInfo->pi_sysAttrList);
    }
    /**
     * Since we separate the target list elements into simple var references and
     * generic expression, we only need to deal the generic expression with LLVM
     * optimization.
     */
    llvm::Function* jitted_vectarget = nullptr;
    if (consider_codegen && planstate->ps_ProjInfo->pi_targetlist) {
        /*
         * check if codegen is allowed for generic targetlist expr.
         * Since targetlist is evaluated in projection, add this function to
         * ps_ProjInfo.
         */
        jitted_vectarget =
            dorado::VecExprCodeGen::TargetListCodeGen(planstate->ps_ProjInfo->pi_targetlist, (PlanState*)scanstate);

        if (jitted_vectarget != nullptr)
            llvmCodeGen->addFunctionToMCJit(
                jitted_vectarget, reinterpret_cast<void**>(&(planstate->ps_ProjInfo->jitted_vectarget)));
    }
    INSTR_TIME_SET_CURRENT(end_time);
    INSTR_TIME_SUBTRACT(end_time, start_time);
    ereport(DEBUG2, (errmodule(MOD_TIMESERIES), 
            errmsg("TSDB LOG:: ExecInitTsStoreScan: %lfms", INSTR_TIME_GET_MILLISEC(end_time))));
#endif   /* ENABLE_MULTIPLE_NODES */
    return scanstate;
}

VectorBatch* ts_apply_projection_and_filter(TsStoreScanState* node, VectorBatch* pScanBatch, ExprDoneCond* isDone)
{
    List* qual = NIL;
    ExprContext* econtext = NULL;
    ProjectionInfo* proj = node->ps.ps_ProjInfo;
    VectorBatch* pOutBatch = NULL;
    bool fSimpleMap = false;
    uint64 inputRows = pScanBatch->m_rows;

    VECTSSTORE_SCAN_TRACE_START(node, TSSTORE_PROJECT);

    qual = node->ps.qual;
    econtext = node->ps.ps_ExprContext;
    pOutBatch = node->currentBatch;
    fSimpleMap = proj->pi_directMap && (pOutBatch->m_cols == proj->pi_numSimpleVars);
    if (node->jitted_vecqual) {
        if (HAS_INSTR(node, false)) {
            node->ps.instrument->isLlvmOpt = true;
        }
    }

    if (pScanBatch->m_rows != 0) {
        ResetExprContext(econtext);
        initEcontextBatch(pScanBatch, NULL, NULL, NULL);
        // Evaluate the qualification clause if any.
        //
        if (qual != NULL) {
            ScalarVector* pVector = NULL;

            if (node->jitted_vecqual)
                pVector = node->jitted_vecqual(econtext);
            else
                pVector = ExecVecQual(qual, econtext, false);

            // If no matched rows, fetch again.
            //
            if (NULL == pVector) {
                pOutBatch->m_rows = 0;
                goto done;
            }

            /*
             * Call optimized PackT function when codegen is turned on.
             */
            if (econtext->ecxt_scanbatch->m_sel) {
                if (u_sess->attr.attr_sql.enable_codegen) {
                    pScanBatch->OptimizePack(econtext->ecxt_scanbatch->m_sel, proj->pi_PackTCopyVars);
                } else {
                    pScanBatch->Pack(econtext->ecxt_scanbatch->m_sel);
                }
            }
        }


        // Copy the result to output batch. Note the output batch has different column set than
        // the scan batch, so we have to remap them. Projection will handle all logics here, so
        // for non simpleMap case, we don't need to do anything.
        //
        if (!fSimpleMap) {
            pOutBatch = ExecVecProject(proj, true, isDone);
        } else {
            pOutBatch->m_rows = pScanBatch->m_rows;
            for (int i = 0; i < pOutBatch->m_cols; i++) {
                AttrNumber att = proj->pi_varNumbers[i];
                errno_t rc;

                Assert(att > 0 && att <= node->scanBatch->m_cols);

                rc = memcpy_s(
                    &pOutBatch->m_arr[i], sizeof(ScalarVector), &pScanBatch->m_arr[att - 1], sizeof(ScalarVector));
                securec_check(rc, "\0", "\0");
            } 
        }
    }

    if (proj->pi_exprContext->have_vec_set_fun == false) {
        pOutBatch->m_rows = Min(pOutBatch->m_rows, pScanBatch->m_rows);
        pOutBatch->FixRowCount();
    }

done:

    VECTSSTORE_SCAN_TRACE_END(node, TSSTORE_PROJECT);
    // collect information of removed rows
    InstrCountFiltered1(node, inputRows - pOutBatch->m_rows);
    // Check fullness of return batch and refill it does not contain enough?
    return pOutBatch;
}

VectorBatch* ExecTsStoreScan(TsStoreScanState* node)
{
    VectorBatch* pOutBatch = NULL;
#ifdef ENABLE_MULTIPLE_NODES
    VectorBatch* pScanBatch = NULL;

    pOutBatch = node->currentBatch;
    pScanBatch = node->scanBatch;

    ExprDoneCond isDone = ExprSingleResult;
    instr_time start_time, end_time;
    int loop_count = 0;
    node->tag_id_num = (int)node->tag_rows->row_count;
rescan:
    pScanBatch->Reset(true);
    pOutBatch->Reset(true);
    node->ps.ps_ProjInfo->pi_exprContext->current_row = 0;
    loop_count++;

    CHECK_FOR_INTERRUPTS();
    INSTR_TIME_SET_CURRENT(start_time);
    ereport(DEBUG2, (errmodule(MOD_TIMESERIES), errmsg("TSDB LOG: start ExecTsStoreScan[%d]", loop_count)));
    reset_sys_vector(node->ps.ps_ProjInfo->pi_sysAttrList, pScanBatch);
    /* 
     * init ts_store_search when first come into this function, seems the
     * top_key_func logic should move to the upper layer, but leave it here for now.
     */
    if (node->first_scan) {
        node->first_scan = false;
        node->ts_store_search->init_scan(node);
    }

    if (node->early_stop && node->scaned_tuples >= node->limit) {
        /* if previous partition has load enough tuples then return */
        return pOutBatch;
    }

    VECTSSTORE_SCAN_TRACE_START(node, TSSTORE_SEARCH);
    node->tags_scan_done = node->ts_store_search->run_scan(node, pScanBatch);
    VECTSSTORE_SCAN_TRACE_END(node, TSSTORE_SEARCH);

    if (!BatchIsNull(pScanBatch)) {
        pScanBatch->FixRowCount(); 
        pOutBatch = ts_apply_projection_and_filter(node, pScanBatch, &isDone);
        if (isDone != ExprEndResult) { 
            node->ps.ps_vec_TupFromTlist = (isDone == ExprMultipleResult); 
        }
        if (node->early_stop) {
            node->scaned_tuples += pScanBatch->m_rows;
        }
        /* we should not return null batch before all the tags has been scanned */
        if (!BatchIsNull(pOutBatch)) {
            INSTR_TIME_SET_CURRENT(end_time);
            INSTR_TIME_SUBTRACT(end_time, start_time);
            ereport(DEBUG2, (errmsg("TSDB LOG: finish ExecTsStoreScan: %lfms", INSTR_TIME_GET_MILLISEC(end_time))));
            return pOutBatch;
        } else if (!node->tags_scan_done) {
            goto rescan;
        }
    } else if (!node->tags_scan_done) {
        /* if we have done with current tagids */
        goto rescan;
    }
    INSTR_TIME_SET_CURRENT(end_time);
    INSTR_TIME_SUBTRACT(end_time, start_time);
    ereport(DEBUG2, (errmodule(MOD_TIMESERIES), 
            errmsg("TSDB LOG: finish ExecTsStoreScan: %lfms", INSTR_TIME_GET_MILLISEC(end_time))));
#endif   /* ENABLE_MULTIPLE_NODES */
    return pOutBatch;
}


void ExecEndTsStoreScan(TsStoreScanState* node, bool indexFlag)
{
#ifdef ENABLE_MULTIPLE_NODES
    Assert(!indexFlag);
    Relation relation;
    TableScanDesc scanDesc;
    /*  get information from node */
    node->ss_currentRelation = node->ss_partition_parent;

    relation = node->ss_currentRelation;
    scanDesc = node->ss_currentScanDesc;

    /* Free the exprcontext */
    ExecFreeExprContext(&node->ps);

    /* clean out the tuple table */
    ExecClearTuple(node->ps.ps_ResultTupleSlot);
    ExecClearTuple(node->ss_ScanTupleSlot);

    /* to do : do we need free ts_store_search? */
    /* close heap scan */
    if (PointerIsValid(node->partitions)) {
        Assert(scanDesc);
        tableam_scan_end(scanDesc);
        Assert(node->ss_currentPartition);
        releaseDummyRelation(&(node->ss_currentPartition));
        releasePartitionList(node->ss_currentRelation, &(node->partitions), node->lockMode);
    }
    /* close the heap relation. */
    ExecCloseScanRelation(relation);
    if (node->time_range != NULL) {
        delete node->time_range;
        node->time_range = NULL;
    }
    if(node->scanBatch != NULL) {
        delete node->scanBatch;
        node->scanBatch = NULL;
    }
    if(node->currentBatch != NULL) {
        delete node->currentBatch;
        node->currentBatch = NULL;
    }
    if (node->ts_store_search != NULL) {
        delete node->ts_store_search;
        node->ts_store_search = NULL;
    }
#endif   /* ENABLE_MULTIPLE_NODES */
}

void ExecReScanTsStoreScan(TsStoreScanState* node)
{
#ifdef ENABLE_MULTIPLE_NODES
    instr_time start_time, end_time;
    INSTR_TIME_SET_CURRENT(start_time);
    TableScanDesc scan = node->ss_currentScanDesc;
    heap_endscan(scan);
    init_next_partition_for_tsstore_scan(node);
    node->first_scan = true;
    INSTR_TIME_SET_CURRENT(end_time);
    INSTR_TIME_SUBTRACT(end_time, start_time);
    ereport(DEBUG2, (errmodule(MOD_TIMESERIES),
        errmsg("TSDB LOG:: ExecReScanTsStoreScan: %lfms", INSTR_TIME_GET_MILLISEC(end_time))));
#endif   /* ENABLE_MULTIPLE_NODES */
}

/*
 * @vectsstorescan.cpp
 * @Description: reset system vector rows to zero 
 * @in - sys_attr_list: system attrribute list
 * @in - vector
 * @return -  void
 */
void reset_sys_vector(const List* sys_attr_list, VectorBatch* vector)
{
    if (sys_attr_list == NIL) {
        return;
    }

    ListCell* cell = NULL;
    foreach(cell, sys_attr_list) {
        int col_idx = lfirst_int(cell);
        ScalarVector* sys_vec = vector->GetSysVector(col_idx);
        sys_vec->m_rows = 0;
    }    
}
