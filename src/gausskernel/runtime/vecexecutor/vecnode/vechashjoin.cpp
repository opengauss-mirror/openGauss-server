/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * vechashjoin.cpp
 *
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vechashjoin.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/vecexprcodegen.h"
#include "codegen/vechashjoincodegen.h"

#include "vecexecutor/vechashjoin.h"
#include "vecexecutor/vechashtable.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vectorbatch.inl"
#include "vectorsonic/vsonichash.h"
#include "vectorsonic/vsonichashjoin.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "executor/executor.h"
#include "commands/explain.h"
#include "utils/anls_opt.h"
#include "utils/biginteger.h"
#include "utils/builtins.h"
#include "miscadmin.h"
#include "vecexecutor/vecexpression.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "tcop/utility.h"
#include "utils/bloom_filter.h"
#include "utils/lsyscache.h"
#ifdef PGXC
#include "catalog/pgxc_node.h"
#include "pgxc/pgxc.h"
#endif

extern bool CodeGenThreadObjectReady();
extern bool CodeGenPassThreshold(double rows, int dn_num, int dop);
extern bool anls_opt_is_on(AnalysisOpt dfx_opt);
#define leftrot(x, k) (((x) << (k)) | ((x) >> (32 - (k))))

#define IS_SONIC_HASH(node) (((HashJoin*)(node)->js.ps.plan)->isSonicHash)
#define JOIN_NAME ((IS_SONIC_HASH(node)) ? "Sonic" : "")

VecHashJoinState* ExecInitVecHashJoin(VecHashJoin* node, EState* estate, int eflags)
{
    VecHashJoinState* hash_state = NULL;
    Plan* outer_node = NULL;
    Plan* inner_node = NULL;
    List* lclauses = NIL;
    List* rclauses = NIL;
    List* hoperators = NIL;
    ListCell* l = NULL;
    FmgrInfo* eqfunctions = NULL;
    ListCell* ho = NULL;
    int i;
    int key_num;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    hash_state = makeNode(VecHashJoinState);
    hash_state->js.ps.plan = (Plan*)node;
    hash_state->js.ps.state = estate;
    hash_state->js.ps.vectorized = true;
    hash_state->hashTbl = NULL;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &hash_state->js.ps);

    /*
     * initialize child expressions
     */
    hash_state->js.ps.targetlist = (List*)ExecInitVecExpr((Expr*)node->join.plan.targetlist, (PlanState*)hash_state);
    hash_state->js.ps.qual = (List*)ExecInitVecExpr((Expr*)node->join.plan.qual, (PlanState*)hash_state);
    hash_state->js.jointype = node->join.jointype;
    hash_state->js.joinqual = (List*)ExecInitVecExpr((Expr*)node->join.joinqual, (PlanState*)hash_state);
    hash_state->js.nulleqqual = (List*)ExecInitVecExpr((Expr*)node->join.nulleqqual, (PlanState*)hash_state);
    hash_state->hashclauses = (List*)ExecInitVecExpr((Expr*)node->hashclauses, (PlanState*)hash_state);

    /*
     * initialize child nodes
     *
     * Note: we could suppress the REWIND flag for the inner input, which
     * would amount to betting that the hash will be a single batch.  Not
     * clear if this would be a win or not.
     */
    outer_node = outerPlan(node);
    inner_node = innerPlan(node);

    outerPlanState(hash_state) = ExecInitNode(outer_node, estate, eflags);
    innerPlanState(hash_state) = ExecInitNode(inner_node, estate, eflags);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &hash_state->js.ps);

    /*
     * initialize tuple type and projection info
     * result tupleSlot only contains virtual tuple, so the default
     * tableAm type is set to HEAP.
     */
    ExecAssignResultTypeFromTL(&hash_state->js.ps, TAM_HEAP);

    /*
     * Check if the following exprs can be codegened or not.
     * Since most of the expression information will be used
     * later, we still need to initialize these expression.
     */
#ifdef ENABLE_LLVM_COMPILE
    dorado::GsCodeGen* llvmCodeGen = (dorado::GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    bool consider_codegen =
        CodeGenThreadObjectReady() &&
        CodeGenPassThreshold(((Plan*)outer_node)->plan_rows, estate->es_plannedstmt->num_nodes, ((Plan*)outer_node)->dop);
#endif

    if (hash_state->js.ps.targetlist) {
        hash_state->js.ps.ps_ProjInfo = ExecBuildVecProjectionInfo(hash_state->js.ps.targetlist,
            node->join.plan.qual,
            hash_state->js.ps.ps_ExprContext,
            hash_state->js.ps.ps_ResultTupleSlot,
            NULL);

#ifdef ENABLE_LLVM_COMPILE
        bool saved_codegen = consider_codegen;
        if (isIntergratedMachine) {
            consider_codegen =
                CodeGenThreadObjectReady() &&
                CodeGenPassThreshold(((Plan*)node)->plan_rows, estate->es_plannedstmt->num_nodes, ((Plan*)node)->dop);
        }

        /*
         * Since we separate the target list elements into simple var references and
         * generic expression, we only need to deal the generic expression with LLVM
         * optimization.
         */
        llvm::Function* jitted_vectarget = NULL;
        if (consider_codegen && hash_state->js.ps.ps_ProjInfo->pi_targetlist) {
            /*
             * check if codegen is allowed for generic targetlist expr.
             * Since targetlist is evaluated in projection, add this function to
             * ps_ProjInfo.
             */
            jitted_vectarget = dorado::VecExprCodeGen::TargetListCodeGen(
                hash_state->js.ps.ps_ProjInfo->pi_targetlist, (PlanState*)hash_state);

            if (jitted_vectarget != NULL) {
                llvmCodeGen->addFunctionToMCJit(
                    jitted_vectarget, reinterpret_cast<void**>(&(hash_state->js.ps.ps_ProjInfo->jitted_vectarget)));
            }
        }

        consider_codegen = saved_codegen;
#endif

        ExecAssignVectorForExprEval(hash_state->js.ps.ps_ProjInfo->pi_exprContext);
    } else {
        ExecAssignVectorForExprEval(hash_state->js.ps.ps_ExprContext);

        hash_state->js.ps.ps_ProjInfo = NULL;
    }

    lclauses = NIL;
    rclauses = NIL;
    hoperators = NIL;
    foreach (l, hash_state->hashclauses) {
        FuncExprState* fstate = (FuncExprState*)lfirst(l);
        OpExpr* hclause = NULL;

        Assert(IsA(fstate, FuncExprState));
        hclause = (OpExpr*)fstate->xprstate.expr;
        Assert(IsA(hclause, OpExpr));
        lclauses = lappend(lclauses, linitial(fstate->args));
        rclauses = lappend(rclauses, lsecond(fstate->args));
        hoperators = lappend_oid(hoperators, hclause->opno);
    }

    key_num = list_length(rclauses);
    eqfunctions = (FmgrInfo*)palloc(key_num * sizeof(FmgrInfo));
    i = 0;
    foreach (ho, hoperators) {
        Oid hashop = lfirst_oid(ho);
        Oid eq_function;

        eq_function = get_opcode(hashop);
        fmgr_info(eq_function, &eqfunctions[i]);
        i++;
    }

    hash_state->hj_OuterHashKeys = lclauses;
    hash_state->hj_InnerHashKeys = rclauses;
    hash_state->hj_HashOperators = hoperators;
    hash_state->eqfunctions = eqfunctions;
    hash_state->js.ps.ps_TupFromTlist = false;

    /* Initialize runtime bloomfilter. */
    hash_state->bf_runtime.bf_var_list = hash_state->js.ps.plan->var_list;
    hash_state->bf_runtime.bf_filter_index = hash_state->js.ps.plan->filterIndexList;
    hash_state->bf_runtime.bf_array = estate->es_bloom_filter.bfarray;

#ifdef ENABLE_LLVM_COMPILE
    /* consider codegeneration for hashjoin node with respect to innerjoin,
     * buildhashtable and probehashtable function.
     */
    if (consider_codegen && !node->isSonicHash) {
        dorado::VecHashJoinCodeGen::HashJoinCodeGen(hash_state);
    }
#endif

    return hash_state;
}

VectorBatch* ExecVecHashJoin(VecHashJoinState* node)
{
    int64 rows = 0;

    for (;;) {
        switch (node->joinState) {
            case HASH_BUILD: {
                if (IS_SONIC_HASH(node)) {
                    if (node->hashTbl == NULL)
                        node->hashTbl = New(CurrentMemoryContext) SonicHashJoin(INIT_DATUM_ARRAY_SIZE, node);

                    ((SonicHashJoin*)(node->hashTbl))->Build();
                    rows = ((SonicHashJoin*)(node->hashTbl))->getRows();
                } else {
                    if (node->hashTbl == NULL)
                        node->hashTbl = New(CurrentMemoryContext) HashJoinTbl(node);

                    ((HashJoinTbl*)(node->hashTbl))->Build();
                    rows = ((HashJoinTbl*)(node->hashTbl))->getRows();
                }

                /* Early free right tree after hash table built */
                ExecEarlyFree(innerPlanState(node));

                EARLY_FREE_LOG(elog(LOG,
                    "Early Free: Hash Table for %sHashJoin"
                    " is built at node %d, memory used %d MB.",
                    JOIN_NAME,
                    (node->js.ps.plan)->plan_node_id,
                    getSessionMemoryUsageMB()));

                if ((node->js.jointype == JOIN_RIGHT || node->js.jointype == JOIN_RIGHT_ANTI_FULL ||
                        node->js.jointype == JOIN_INNER || node->js.jointype == JOIN_SEMI ||
                        node->js.jointype == JOIN_RIGHT_SEMI || node->js.jointype == JOIN_RIGHT_ANTI) &&
                    0 == rows) {
                    // When hash table size is zero, no need to fetch left tree any more and
                    // should deinit the consumer in left tree earlier.
                    //
                    ExecEarlyDeinitConsumer((PlanState*)node);
                    return NULL;
                }
            } break;

            case HASH_PROBE: {
                instr_time start_time;
                (void)INSTR_TIME_SET_CURRENT(start_time);
                VectorBatch* result = NULL;
                if (IS_SONIC_HASH(node)) {
                    result = ((SonicHashJoin*)(node->hashTbl))->Probe();
                    ((SonicHashJoin*)(node->hashTbl))->m_probe_time += elapsed_time(&start_time);
                } else {
                    result = ((HashJoinTbl*)(node->hashTbl))->Probe();
                    ((HashJoinTbl*)(node->hashTbl))->m_probe_time += elapsed_time(&start_time);
                }

                if (BatchIsNull(result)) {
                    PlanState* plan_state = (PlanState*)node;
                    if (HAS_INSTR(&node->js, true)) {
                        if (IS_SONIC_HASH(node))
                            plan_state->instrument->sorthashinfo.hashagg_time =
                                ((SonicHashJoin*)(node->hashTbl))->m_probe_time;
                        else
                            plan_state->instrument->sorthashinfo.hashagg_time =
                                ((HashJoinTbl*)(node->hashTbl))->m_probe_time;
                    }

                    ExecEarlyFree(outerPlanState(node));
                    EARLY_FREE_LOG(elog(LOG,
                        "Early Free: %sHashJoin Probe is done"
                        " at node %d, memory used %d MB.",
                        JOIN_NAME,
                        (node->js.ps.plan)->plan_node_id,
                        getSessionMemoryUsageMB()));
                }
                return result;
            }
            default:
                break;
        }
    }
}

void ExecEndVecHashJoin(VecHashJoinState* node)
{
    void* tbl = node->hashTbl;

    if (tbl != NULL) {
        if (!IS_SONIC_HASH(node)) {
            HashJoinTbl* hj_tbl = (HashJoinTbl*)tbl;

            if (hj_tbl->m_buildFileSource) {
                Assert(hj_tbl->m_probeFileSource);

                hj_tbl->m_buildFileSource->closeAll();
                hj_tbl->m_probeFileSource->closeAll();
            }

            hj_tbl->freeMemoryContext();
        } else {
            SonicHashJoin* hj_tbl = (SonicHashJoin*)tbl;
            hj_tbl->closeAllFiles();
            hj_tbl->freeMemoryContext();
        }
    }

    ExecFreeExprContext(&node->js.ps);
    ExecEndNode(outerPlanState(node));
    ExecEndNode(innerPlanState(node));
}

HashJoinTbl::HashJoinTbl(VecHashJoinState* runtime_context)
    : m_cache(NULL), m_runtime(runtime_context), m_pLevel(NULL), m_maxPLevel(3), m_isValid(NULL), m_isWarning(false)
{
    VecHashJoin* node = NULL;
    Plan* inner_tree = NULL;
    Plan* outer_tree = NULL;
    int i;
    ListCell* l = NULL;
    ExprState* expr_state = NULL;
    Var* variable = NULL;
    ScalarDesc unknown_desc;

    m_fill_table_rows = 0;
    m_probeIdx = 0;
    m_probeStatus = 0;
    m_complicate_innerBatch = NULL;
    m_complicate_outerBatch = NULL;
    m_doProbeData = false;
    m_strategy = 0;
    m_hashTbl = NULL;
    m_cjVector = NULL;
    m_filesource = NULL;
    m_overflowsource = NULL;
    m_probOpSource = NULL;
    m_strategy = HASH_IN_MEMORY;

    node = (VecHashJoin*)m_runtime->js.ps.plan;
    inner_tree = innerPlan(node);
    outer_tree = outerPlan(node);
    m_build_time = 0.0;
    m_probe_time = 0.0;

    initMemoryControl();

    m_hashContext = AllocSetContextCreate(CurrentMemoryContext, "HashContext", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, STACK_CONTEXT, m_totalMem);

    m_tmpContext = AllocSetContextCreate(CurrentMemoryContext, "TmpHashContext", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    AddControlMemoryContext(m_runtime->js.ps.instrument, m_hashContext);

    node = (VecHashJoin*)runtime_context->js.ps.plan;

    if (node->join.jointype == JOIN_RIGHT || node->join.jointype == JOIN_RIGHT_ANTI_FULL ||
        node->join.jointype == JOIN_RIGHT_ANTI || node->join.jointype == JOIN_RIGHT_SEMI)
        m_cols = ExecTargetListLength(inner_tree->targetlist) + 1; /* add 1 column to flag match or not */
    else
        m_cols = ExecTargetListLength(inner_tree->targetlist);

    m_key = list_length(runtime_context->hj_InnerHashKeys);

    m_eqfunctions = runtime_context->eqfunctions;
    m_keyIdx = (int*)palloc(sizeof(int) * m_key);
    m_okeyIdx = (int*)palloc(sizeof(int) * m_key);
    m_simpletype = (bool*)palloc(sizeof(bool) * m_key);
    m_matchKeyFunction = (pMatchKeyFunc*)palloc(sizeof(pMatchKeyFunc) * m_key);

    i = 0;
    m_complicateJoinKey = false;
    foreach (l, runtime_context->hj_InnerHashKeys) {
        expr_state = (ExprState*)lfirst(l);
        if (IsA(expr_state->expr, Var))
            variable = (Var*)expr_state->expr;
        else if (IsA(expr_state->expr, RelabelType)) {
            RelabelType* rel_type = (RelabelType*)expr_state->expr;

            if (IsA(rel_type->arg, Var) && ((Var*)rel_type->arg)->varattno > 0)
                variable = (Var*)((RelabelType*)expr_state->expr)->arg;
            else {
                m_complicateJoinKey = true;
                break;
            }
        } else {
            m_complicateJoinKey = true;
            break;
        }

        m_keyIdx[i] = variable->varattno - 1;
        m_okeyIdx[i] = variable->varoattno - 1;
        m_simpletype[i] = simpletype(variable->vartype);
        i++;
    }

    m_outCols = ExecTargetListLength(outer_tree->targetlist);
    m_outKeyIdx = (int*)palloc(sizeof(int) * list_length(runtime_context->hj_OuterHashKeys));

    /*
     * We need to record the varoattno of the var for pushing down the filter, because the
     * varattno recored by m_outKeyIdx may be different from the original column index in
     * some cases while we need the original attNo. For example, the varattno is not equal
     * to varoattno for the index column.
     */
    m_outOKeyIdx = (int*)palloc(sizeof(int) * list_length(runtime_context->hj_OuterHashKeys));
    m_outKeyCollation = (Oid*)palloc(sizeof(Oid) * list_length(runtime_context->hj_OuterHashKeys));
    m_outerkeyType = (Oid*)palloc(sizeof(Oid) * list_length(runtime_context->hj_OuterHashKeys));

    i = 0;
    foreach (l, runtime_context->hj_OuterHashKeys) {
        expr_state = (ExprState*)lfirst(l);
        if (IsA(expr_state->expr, Var))
            variable = (Var*)expr_state->expr;
        else if (IsA(expr_state->expr, RelabelType)) {
            RelabelType* rel_type = (RelabelType*)expr_state->expr;

            if (IsA(rel_type->arg, Var) && ((Var*)rel_type->arg)->varattno > 0)
                variable = (Var*)((RelabelType*)expr_state->expr)->arg;
            else {
                m_complicateJoinKey = true;
                break;
            }

        } else {
            m_complicateJoinKey = true;
            break;
        }

        m_outKeyIdx[i] = variable->varattno - 1;
        m_outOKeyIdx[i] = variable->varoattno - 1;
        m_outKeyCollation[i] = variable->varcollid;
        m_outerkeyType[i] = variable->vartype;
        m_simpletype[i] = m_simpletype[i] && simpletype(variable->vartype);
        i++;
    }

    m_innerHashFuncs = (FmgrInfo*)palloc(m_key * sizeof(FmgrInfo));
    m_outerHashFuncs = (FmgrInfo*)palloc(m_key * sizeof(FmgrInfo));

    i = 0;
    foreach (l, m_runtime->hj_HashOperators) {
        Oid hashop = lfirst_oid(l);
        Oid left_hashfn;
        Oid right_hashfn;

        if (!get_op_hash_functions(hashop, &left_hashfn, &right_hashfn))
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                    errmsg("could not find hash function for hash operator %u", hashop)));
        fmgr_info(left_hashfn, &m_outerHashFuncs[i]);
        fmgr_info(right_hashfn, &m_innerHashFuncs[i]);
        i++;
    }

    if (u_sess->attr.attr_sql.enable_fast_numeric) {
        replace_numeric_hash_to_bi(i, m_outerHashFuncs);
        replace_numeric_hash_to_bi(i, m_innerHashFuncs);
    }

    TupleDesc inner_desc = innerPlanState(runtime_context)->ps_ResultTupleSlot->tts_tupleDescriptor;
    TupleDesc outer_desc = outerPlanState(runtime_context)->ps_ResultTupleSlot->tts_tupleDescriptor;
    TupleDesc res_desc = runtime_context->js.ps.ps_ResultTupleSlot->tts_tupleDescriptor;

    m_joinStateLog.restore = false;
    m_joinStateLog.lastBuildIdx = 0;

    m_keyDesc = (ScalarDesc*)palloc0(m_key * sizeof(ScalarDesc));

    m_colDesc = (ScalarDesc*)palloc0(m_cols * sizeof(ScalarDesc));

    m_innerBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, inner_desc);
    m_outerBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, outer_desc);
    if (m_runtime->js.joinqual != NULL) {
        m_inQualBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, inner_desc);
        m_outQualBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, outer_desc);
    } else {
        m_inQualBatch = NULL;
        m_outQualBatch = NULL;
    }

    if (m_complicateJoinKey == true) {
        m_complicate_innerBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, inner_desc);
        m_complicate_outerBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, outer_desc);
    }

    if (m_complicateJoinKey == true) {
        m_cjVector = New(CurrentMemoryContext) ScalarVector;
        m_cjVector->init(CurrentMemoryContext, unknown_desc);
    }

    m_outRawBatch = NULL;
    m_keySimple = 1;
    if (m_complicateJoinKey == false) {
        for (i = 0; i < m_key; i++) {
            m_keyDesc[i] = m_innerBatch->m_arr[m_keyIdx[i]].m_desc;
            if (m_keyDesc[i].encoded) {
                m_keySimple = 0;
            }
        }
    }

    for (i = 0; i < m_innerBatch->m_cols; i++) {
        m_colDesc[i] = m_innerBatch->m_arr[i].m_desc;
    }

    m_outSimple = true;
    for (i = 0; i < m_outerBatch->m_cols; i++) {
        if (m_outerBatch->m_arr[i].m_desc.encoded) {
            m_outSimple = false;
            break;
        }
    }

    m_innerSimple = true;
    for (i = 0; i < m_innerBatch->m_cols; i++) {
        if (m_innerBatch->m_arr[i].m_desc.encoded) {
            m_innerSimple = false;
            break;
        }
    }

    if (m_complicateJoinKey)
        m_cellSize = offsetof(hashCell, m_val) + (m_cols + 1) * sizeof(hashVal);
    else
        m_cellSize = offsetof(hashCell, m_val) + (m_cols) * sizeof(hashVal);

    if (res_desc->natts == 0) {
        ScalarDesc desc;
        m_result = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, &desc, 1);
    } else
        m_result = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, res_desc);

    m_strategy = MEMORY_HASH;
    m_buildFileSource = NULL;
    m_probeFileSource = NULL;

    for (i = 0; i < BatchMaxSize; i++)
        m_keyMatch[i] = true;

    SetJoinType();

    if (m_joinType == HASH_JOIN_SEMI || m_joinType == HASH_JOIN_RIGHT_SEMI || m_joinType == HASH_JOIN_RIGHT_ANTI) {
        cellPoint = (hashCell**)palloc(BatchMaxSize * sizeof(hashCell*));
    } else
        cellPoint = NULL;

    if (m_complicateJoinKey)
        bindingFp<true>();
    else
        bindingFp<false>();
    if (m_complicateJoinKey == false)
        ReplaceEqfunc();

    m_tupleCount = m_colWidth = 0;
    m_sysBusy = false;
    if (((Plan*)node)->operatorMaxMem > 0)
        m_maxMem = SET_NODEMEM(((Plan*)node)->operatorMaxMem, ((Plan*)node)->dop) * 1024L;
    else
        m_maxMem = 0;
    m_spreadNum = 0;
}

void HashJoinTbl::initMemoryControl()
{
    VecHashJoin* node = NULL;

    node = (VecHashJoin*)m_runtime->js.ps.plan;

    m_totalMem = SET_NODEMEM(((Plan*)node)->operatorMemKB[0], ((Plan*)node)->dop) * 1024L;

    elog(DEBUG1, "HashJoinTbl[%d]:operator Memory uses: %dKB", ((Plan*)node)->plan_node_id, (int)(m_totalMem / 1024L));

    m_availmems = m_totalMem;
}

int HashJoinTbl::calcSpillFile()
{
    VecHashJoin* node = NULL;
    Plan* inner_plan = NULL;
    int64 rows;
    int file_num;

    node = (VecHashJoin*)m_runtime->js.ps.plan;
    inner_plan = innerPlan(node);

    rows = (int64)inner_plan->plan_rows;
    elog(DEBUG1,
        "hashjoin: total size: %lu, operator size: %ld",
        (rows * (2 * sizeof(hashCell*) + Max(m_cellSize, inner_plan->plan_width))),
        m_totalMem);
    file_num = getPower2NextNum((rows * (2 * sizeof(hashCell*) + Max(m_cellSize, inner_plan->plan_width))) / m_totalMem);
    file_num = Max(32, file_num);
    file_num = Min(file_num, 1024);

    return file_num;
}

// SetJoinType
//  	Convert planner join type to vectorized hash join type
//
void HashJoinTbl::SetJoinType()
{
    VecHashJoin* node = NULL;
    node = (VecHashJoin*)m_runtime->js.ps.plan;
    switch (node->join.jointype) {
        case JOIN_INNER:
            m_joinType = HASH_JOIN_INNER;
            break;
        case JOIN_LEFT:
            m_joinType = HASH_JOIN_LEFT;
            break;
        case JOIN_RIGHT:
            m_joinType = HASH_JOIN_RIGHT;
            break;
        case JOIN_SEMI:
            m_joinType = HASH_JOIN_SEMI;
            break;
        case JOIN_ANTI:
            m_joinType = HASH_JOIN_ANTI;
            break;
        case JOIN_RIGHT_SEMI:
            m_joinType = HASH_JOIN_RIGHT_SEMI;
            break;
        case JOIN_RIGHT_ANTI:
            m_joinType = HASH_JOIN_RIGHT_ANTI;
            break;
        case JOIN_LEFT_ANTI_FULL:
            m_joinType = HASH_JOIN_LEFT_ANTI_FULL;
            break;
        case JOIN_RIGHT_ANTI_FULL:
            m_joinType = HASH_JOIN_RIGHT_ANTI_FULL;
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unsupport join type %d", node->join.jointype)));
            break;
    }
}

// PrepareProbe
//  	Do some preparation stuff before probeing
//
void HashJoinTbl::PrepareProbe()
{
    switch (m_strategy) {
        case MEMORY_HASH: {
            m_probeStatus = PROBE_FETCH;
            m_probOpSource = New(CurrentMemoryContext) hashOpSource(outerPlanState(m_runtime));
            hashSource* source = New(CurrentMemoryContext) hashMemSource(m_cache);
            {
                AutoContextSwitch mem_switch(m_hashContext);
                WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_BUILD_HASH);
                if (m_complicateJoinKey)
                    buildHashTable<true, false>(source, m_rows);
                else
                    buildHashTable<false, false>(source, m_rows);
                (void)pgstat_report_waitstatus(old_status);

                bool can_wlm_warning_statistics = false;
                /* analysis hash table information created in memory */
                if (anls_opt_is_on(ANLS_HASH_CONFLICT)) {
                    char stats[MAX_LOG_LEN];
                    m_hashTbl->Profile(stats, &can_wlm_warning_statistics);
                    ereport(LOG,
                        (errmodule(MOD_VEC_EXECUTOR),
                            errmsg("[VecHashJoin(%d)] %s", m_runtime->js.ps.plan->plan_node_id, stats)));
                } else if (u_sess->attr.attr_resource.resource_track_level >= RESOURCE_TRACK_QUERY &&
                           u_sess->attr.attr_resource.enable_resource_track && u_sess->exec_cxt.need_track_resource) {
                    char stats[MAX_LOG_LEN];
                    m_hashTbl->Profile(stats, &can_wlm_warning_statistics);
                }
                if (can_wlm_warning_statistics) {
                    pgstat_add_warning_hash_conflict();
                    if (HAS_INSTR(&m_runtime->js, true)) {
                        m_runtime->js.ps.instrument->warning |= (1 << WLM_WARN_HASH_CONFLICT);
                    }
                }
            }
        } break;
        case GRACE_HASH: {
            m_probeStatus = PROBE_PARTITION_FILE;
            m_probeIdx = 0;
        } break;
        default:
            Assert(false);
            break;
    }

    m_runtime->joinState = HASH_PROBE;
}

void HashJoinTbl::initFile(bool build_side, VectorBatch* template_batch, int file_num)
{
    /*
     * we allocate m_buildFileSource and m_probeFileSource under current node memory context,
     * and rebuild temp files as which closed after last rescan.
     */
    if (build_side) {
        hashCell* cell_array = (hashCell*)palloc0(BatchMaxSize * m_cellSize);
        if (m_buildFileSource == NULL) {
            MemoryContext stack_context = AllocSetContextCreate(CurrentMemoryContext,
                "BuildSideTempFileStackContext",
                ALLOCSET_DEFAULT_MINSIZE,
                ALLOCSET_DEFAULT_INITSIZE,
                ALLOCSET_DEFAULT_MAXSIZE,
                STACK_CONTEXT,
                m_totalMem);

            m_buildFileSource = New(CurrentMemoryContext) hashFileSource(template_batch,
                stack_context,
                m_cellSize,
                cell_array,
                m_complicateJoinKey,
                m_cols,
                file_num,
                innerPlanState(m_runtime)->ps_ResultTupleSlot->tts_tupleDescriptor);

            /* for variable attributes, sometimes we need buffers in stack_context to hold hash table, so control it */
            AddControlMemoryContext(m_runtime->js.ps.instrument, stack_context);
        } else {
            Assert(file_num > 0);
            m_buildFileSource->initFileSource(file_num);
        }

        if (m_runtime->js.ps.instrument) {
            m_buildFileSource->m_spill_size = &m_runtime->js.ps.instrument->sorthashinfo.spill_size;
        }
    } else {
        if (m_probeFileSource == NULL) {
            MemoryContext stack_context = AllocSetContextCreate(CurrentMemoryContext,
                "ProbeSideTempFileStackContext",
                ALLOCSET_DEFAULT_MINSIZE,
                ALLOCSET_DEFAULT_INITSIZE,
                ALLOCSET_DEFAULT_MAXSIZE,
                STACK_CONTEXT,
                m_totalMem);

            m_probeFileSource = New(CurrentMemoryContext) hashFileSource(template_batch,
                stack_context,
                0,
                NULL,
                m_complicateJoinKey,
                m_cols,
                file_num,
                outerPlanState(m_runtime)->ps_ResultTupleSlot->tts_tupleDescriptor);
        } else {
            Assert(file_num > 0);
            m_probeFileSource->initFileSource(file_num);
        }

        if (m_runtime->js.ps.instrument) {
            m_probeFileSource->m_spill_size = &m_runtime->js.ps.instrument->sorthashinfo.spill_size;
        }
    }
}

template <bool complicate_join_key, bool need_copy>
void HashJoinTbl::buildHashTable(hashSource* source, int64 rownum)
{
    int mask;
    int rows;
    hashCell* cell_head = NULL;
    hashCell* cell = NULL;
    int i;
    hashCell* last_cell = NULL;
    hashCell* copy_cell_array = NULL;
    hashCell* copy_cell = NULL;
    ScalarValue location;
    int hash_size;
    errno_t rc;

    hash_size = Max(MIN_HASH_TABLE_SIZE, getPower2LessNum(Min(rownum, (int)(MAX_BUCKET_NUM))));

    m_hashTbl = New(CurrentMemoryContext) vechashtable(hash_size);

    mask = hash_size - 1;

    cell_head = source->getCell();

    while (cell_head != NULL) {
        /*
         * LLVM compiled execution on CPU intensive part of buildHashTable, we
         * make a difference here to distict needcopy is true or not.
         */
        if (!need_copy && m_runtime->jitted_buildHashTable) {
            if (HAS_INSTR(&m_runtime->js, true)) {
                m_runtime->js.ps.instrument->isLlvmOpt = true;
            }

            typedef int (*buildHashTable_func)(hashCell* cell_head, vechashtable* hashTbl);
            ((buildHashTable_func)(m_runtime->jitted_buildHashTable))(cell_head, m_hashTbl);
        } else if (need_copy && m_runtime->jitted_buildHashTable_NeedCopy) {
            if (HAS_INSTR(&m_runtime->js, true)) {
                m_runtime->js.ps.instrument->isLlvmOpt = true;
            }

            typedef int (*buildHashTable_func)(hashCell* cell_head, vechashtable* hashTbl);
            ((buildHashTable_func)(m_runtime->jitted_buildHashTable_NeedCopy))(cell_head, m_hashTbl);
        } else {
            /* the original function */
            rows = cell_head->flag.m_rows;

            if (need_copy)
                copy_cell_array = (hashCell*)palloc0(rows * m_cellSize);

            if (complicate_join_key == false)
                hashCellArray(cell_head, rows, m_keyIdx, m_cacheLoc, m_innerHashFuncs);

            for (i = 0; i < rows; i++) {
                cell = GET_NTH_CELL(cell_head, i);

                if (complicate_join_key)
                    location = cell->m_val[m_cols].val & mask;
                else
                    location = m_cacheLoc[i] & mask;

                last_cell = m_hashTbl->m_data[location];

                if (need_copy) {
                    copy_cell = GET_NTH_CELL(copy_cell_array, i);
                    rc = memcpy_s(copy_cell, m_cellSize, cell, m_cellSize);
                    securec_check(rc, "\0", "\0");
                    m_hashTbl->m_data[location] = copy_cell;
                    copy_cell->flag.m_next = last_cell;
                } else {
                    m_hashTbl->m_data[location] = cell;
                    cell->flag.m_next = last_cell;
                }
            }
        }

        cell_head = source->getCell();
    }
}

void HashJoinTbl::Build()
{
    PlanState* inner_node = innerPlanState(m_runtime);
    PlanState* plan_state = NULL;
    VectorBatch* batch = NULL;
    instr_time start_time;

    for (;;) {
        batch = VectorEngine(inner_node);
        if (unlikely(BatchIsNull(batch)))
            break;

        (void)INSTR_TIME_SET_CURRENT(start_time);
        RuntimeBinding(m_funBuild, m_strategy)(batch);
        m_build_time += elapsed_time(&start_time);
    }

    (void)INSTR_TIME_SET_CURRENT(start_time);
    PushDownFilterIfNeed();
    PrepareProbe();

    plan_state = &m_runtime->js.ps;
    if (HAS_INSTR(&m_runtime->js, true)) {
        plan_state->instrument->sorthashinfo.hashbuild_time = m_build_time;
        if (MEMORY_HASH == m_strategy) {
            plan_state->instrument->sorthashinfo.hash_writefile = false;
            plan_state->instrument->sorthashinfo.spaceUsed = ((AllocSetContext*)m_hashContext)->totalSpace;
            plan_state->instrument->sorthashinfo.hash_FileNum = 0;
            plan_state->instrument->sorthashinfo.hash_spillNum = 0;
            if (m_tupleCount > 0)
                plan_state->righttree->instrument->width = (int)(m_colWidth / m_tupleCount);
        } else {
            Assert(m_buildFileSource != NULL);
            plan_state->instrument->sorthashinfo.hash_writefile = true;
            plan_state->instrument->sorthashinfo.hash_FileNum = m_buildFileSource->m_fileNum;
            plan_state->instrument->sorthashinfo.hash_spillNum = 0;
            HASH_BASED_DEBUG(recordPartitionInfo(true, -1, 0, m_buildFileSource->m_fileNum));
        }
        plan_state->instrument->spreadNum = m_spreadNum;
    }

    if (u_sess->instr_cxt.global_instr != NULL && MEMORY_HASH == m_strategy) {
        plan_state->instrument->sorthashinfo.spill_size = 0;
    }

    /* release the buffer in file handler */
    if (m_buildFileSource)
        m_buildFileSource->ReleaseAllFileHandlerBuffer();
}

/*
 * If the condition meets, push down the bloom filter which is built by the hash table into
 * the outer side.
 */
void HashJoinTbl::PushDownFilterIfNeed()
{
    filter::BloomFilter** bf_array = m_runtime->bf_runtime.bf_array;
    List* bf_var_list = m_runtime->bf_runtime.bf_var_list;

    if (u_sess->attr.attr_sql.enable_bloom_filter && MEMORY_HASH == m_strategy && !m_complicateJoinKey &&
        list_length(m_cache) != 0 && m_rows <= DEFAULT_ORC_BLOOM_FILTER_ENTRIES * 5) {
        for (int i = 0; i < list_length(bf_var_list); i++) {
            Var* var = (Var*)list_nth(bf_var_list, i);
            int idx = -1;
            for (int j = 0; j < m_key; ++j) {
                if (var->varoattno - 1 == m_okeyIdx[j] && var->varattno - 1 == m_keyIdx[j]) {
                    idx = m_keyIdx[j];
                    break;
                }
            }
            if (idx < 0) {
                continue;
            }

            Oid data_type = var->vartype;
            if (!SATISFY_BLOOM_FILTER(data_type)) {
                continue;
            }

            filter::BloomFilter* filter = filter::createBloomFilter(data_type,
                var->vartypmod,
                var->varcollid,
                HASHJOIN_BLOOM_FILTER,
                DEFAULT_ORC_BLOOM_FILTER_ENTRIES * 5,
                true);

            /*
             * For compute pool, we have to forbidden codegen of bf, since we
             * can not push down codegen expr now.
             */
            if (m_runtime->js.ps.state && m_runtime->js.ps.state->es_plannedstmt &&
                !m_runtime->js.ps.state->es_plannedstmt->has_obsrel) {
                switch (data_type) {
                    case INT2OID:
                    case INT4OID:
                    case INT8OID:
                        filter->jitted_bf_addLong = m_runtime->jitted_hashjoin_bfaddLong;
                        filter->jitted_bf_incLong = m_runtime->jitted_hashjoin_bfincLong;
                        break;
                    default:
                        /* do nothing */
                        break;
                }
            }

            ListCell* lc = NULL;
            foreach (lc, m_cache) {
                hashCell* cell_head = (hashCell*)lfirst(lc);
                int rows = cell_head->flag.m_rows;

                for (int j = 0; j < rows; j++) {
                    hashCell* cell = GET_NTH_CELL(cell_head, j);

                    /* Null value will not be joined, so we can ignore null value. */
                    if (unlikely(!IS_NULL(cell->m_val[idx].flag)))
                        filter->addDatum((Datum)cell->m_val[idx].val);
                }
            }

            int pos = list_nth_int(m_runtime->bf_runtime.bf_filter_index, i);
            bf_array[pos] = filter;
        }
    }
}

/*
 * @Description: Check if has enough memory to build hash table.
 *
 * @param[IN] rows:  row number of current batch
 * @return: bool
 */
bool HashJoinTbl::HasEnoughMem(int rows)
{
    /* To calculate both m_hashBufContext and m_hashContext */
    AllocSetContext* hash_set = (AllocSetContext*)m_hashContext;
    int64 used_size = hash_set->totalSpace;

    /* Add hash cell size used by a batch in advance */
    used_size += rows * m_cellSize;
    bool sys_busy = gs_sysmemory_busy(used_size * SET_DOP(m_runtime->js.ps.plan->dop), true);

    if (used_size > m_totalMem || sys_busy) {
        if (sys_busy) {
            m_totalMem = used_size;
            hash_set->maxSpaceSize = used_size;
            MEMCTL_LOG(LOG,
                "VecHashJoin(%d) early spilled, workmem: %ldKB, usedmem: %ldKB,"
                "current HashContext, totalSpace: %luKB, usedSpace: %luKB",
                m_runtime->js.ps.plan->plan_node_id,
                m_totalMem / 1024L,
                used_size / 1024L,
                hash_set->totalSpace / 1024L,
                (hash_set->totalSpace - hash_set->freeSpace) / 1024L);
            pgstat_add_warning_early_spill();
        } else if (m_maxMem > m_totalMem) {
            /* memory auto spread logic */
            m_totalMem = used_size;
            int64 spreadMem = Min(Min(dywlm_client_get_memory() * 1024L, m_totalMem), m_maxMem - m_totalMem);
            if (spreadMem > m_totalMem * MEM_AUTO_SPREAD_MIN_RATIO) {
                m_totalMem += spreadMem;
                hash_set->maxSpaceSize += spreadMem;
                m_spreadNum++;
                m_availmems = m_totalMem - used_size;
                MEMCTL_LOG(DEBUG2,
                    "VecHashJoin(%d) auto mem spread %ldKB succeed, and work mem is %ldKB.",
                    m_runtime->js.ps.plan->plan_node_id,
                    spreadMem / 1024L,
                    m_totalMem / 1024L);
                return true;
            }
            MEMCTL_LOG(LOG,
                "VecHashJoin(%d) auto mem spread %ldKB failed, and work mem is %ldKB.",
                m_runtime->js.ps.plan->plan_node_id,
                spreadMem / 1024L,
                m_totalMem / 1024L);
            if (m_spreadNum > 0) {
                pgstat_add_warning_spill_on_memory_spread();
            }
        }

        ereport(LOG,
            (errmodule(MOD_VEC_EXECUTOR),
                errmsg("Profiling Warning : VecHashJoin(%d) Disk Spilled.", m_runtime->js.ps.plan->plan_node_id)));

        if (!sys_busy) {
            MEMCTL_LOG(LOG,
                "VecHashJoin(%d) Disk Spilled: tatalSpace: %luKB, freeSpace: %luKB",
                m_runtime->js.ps.plan->plan_node_id,
                hash_set->totalSpace / 1024L,
                hash_set->freeSpace / 1024L);
        }

        /* next batch will be inserted into temp file */
        if (HAS_INSTR(&m_runtime->js, true)) {
            if (m_tupleCount > 0)
                m_runtime->js.ps.righttree->instrument->width = (int)(m_colWidth / m_tupleCount);
            m_runtime->js.ps.instrument->sysBusy = sys_busy;
        }
        return false;
    }

    m_availmems = m_totalMem - used_size;
    return true;
}

template <bool complicate_join_key, bool simple>
void HashJoinTbl::SaveToMemory(VectorBatch* batch)
{
    hashCell* cell = NULL;
    hashCell* cell_arr = NULL;
    int i;

    int rows = batch->m_rows;
    int cols = batch->m_cols;

    m_rows += rows;

    if (HasEnoughMem(rows) == false) {
        int file_num;
        file_num = calcSpillFile();
        m_strategy = GRACE_HASH;
        initFile(true, batch, file_num);
        m_pLevel = (uint8*)palloc(file_num * sizeof(uint8));
        m_isValid = (bool*)palloc(file_num * sizeof(bool));

        for (i = 0; i < file_num; i++) {
            m_pLevel[i] = 1;
            m_isValid[i] = true;
        }

        if (m_runtime->js.ps.instrument) {
            int64 memory_size = 0;
            CalculateContextSize(CurrentMemoryContext, &memory_size);
            if (m_runtime->js.ps.instrument->memoryinfo.peakOpMemory < memory_size)
                m_runtime->js.ps.instrument->memoryinfo.peakOpMemory = memory_size;

            /* when turn to grace_hash, we should record the peak control memory right here,
             * since the m_hashContext is full now.
             */
            memory_size = 0;
            CalculateContextSize(m_hashContext, &memory_size);
            if (m_runtime->js.ps.instrument->memoryinfo.peakControlMemory < memory_size)
                m_runtime->js.ps.instrument->memoryinfo.peakControlMemory = memory_size;
        }

        WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_WRITE_FILE);
        flushToDisk<complicate_join_key>();
        SaveToDisk<complicate_join_key, true>(batch);
        (void)pgstat_report_waitstatus(old_status);

        pgstat_increase_session_spill();
        MemoryContextReset(m_hashContext);

        return;
    }

    AutoContextSwitch mem_switch(m_hashContext);

    cell_arr = (hashCell*)palloc0(rows * m_cellSize);
    m_colWidth += rows * m_cols * sizeof(hashVal);

    if (complicate_join_key)
        CalcComplicateHashVal(batch, m_runtime->hj_InnerHashKeys, true);

    for (int j = 0; j < cols; j++) {
        ScalarVector* p_vector = &batch->m_arr[j];
        cell = cell_arr;
        if (simple || m_colDesc[j].encoded == false) {
            for (i = 0; i < rows; i++) {
                (cell)->m_val[j].val = p_vector->m_vals[i];
                (cell)->m_val[j].flag = p_vector->m_flag[i];
                (cell) = (hashCell*)((char*)cell + m_cellSize);
            }
        } else {
            for (i = 0; i < rows; i++) {
                if (likely(p_vector->IsNull(i) == false)) {
                    (cell)->m_val[j].val = addVariable(m_hashContext, p_vector->m_vals[i]);
                    m_colWidth += VARSIZE_ANY(p_vector->m_vals[i]);
                }
                (cell)->m_val[j].flag = p_vector->m_flag[i];
                (cell) = (hashCell*)((char*)cell + m_cellSize);
            }
        }
    }

    if (complicate_join_key) {
        cell = cell_arr;
        for (i = 0; i < rows; i++) {
            (cell)->m_val[m_cols].val = m_cacheLoc[i];  // last value remember hash value.
            (cell)->m_val[m_cols].flag = 0;
            (cell) = (hashCell*)((char*)cell + m_cellSize);
        }
    }

    m_cache = lcons(cell_arr, m_cache);
    cell_arr->flag.m_rows = rows;
    m_tupleCount += rows;
}

template <bool complicate_join_key>
void HashJoinTbl::flushToDisk()
{
    ListCell* l = NULL;
    hashCell* cell = NULL;
    hashCell* cell_head = NULL;
    int rows;
    int i;

    foreach (l, m_cache) {
        CHECK_FOR_INTERRUPTS();

        cell_head = (hashCell*)lfirst(l);
        rows = cell_head->flag.m_rows;
        cell_head->flag.m_next = NULL;  // reset this value as we just borrow it for put the row number

        if (!complicate_join_key)
            hashCellArray(cell_head, rows, m_keyIdx, m_cacheLoc, m_innerHashFuncs, true);

        for (i = 0; i < rows; i++) {
            cell = GET_NTH_CELL(cell_head, i);

            if (complicate_join_key)
                m_buildFileSource->writeCell(cell, DatumGetUInt32((cell)->m_val[m_cols].val));
            else
                m_buildFileSource->writeCell(cell, DatumGetUInt32(m_cacheLoc[i]));
        }
    }
}

template <bool complicate_join_key, bool build_side>
void HashJoinTbl::SaveToDisk(VectorBatch* batch)
{
    int i;
    int row = batch->m_rows;
    int* key_idx = build_side ? m_keyIdx : m_outKeyIdx;
    hashFileSource* file_source = build_side ? m_buildFileSource : m_probeFileSource;

    if (complicate_join_key) {
        if (build_side)
            CalcComplicateHashVal(batch, m_runtime->hj_InnerHashKeys, true);
        else
            CalcComplicateHashVal(batch, m_runtime->hj_OuterHashKeys, false);
    } else {
        if (build_side)
            hashBatch(batch, key_idx, m_cacheLoc, m_innerHashFuncs, true);
        else
            hashBatch(batch, key_idx, m_cacheLoc, m_outerHashFuncs, true);
    }

    WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_WRITE_FILE);
    if (complicate_join_key) {
        /* save hashvalue in case of complicate_join_key for both sides(build & probe) */
        for (i = 0; i < row; i++) {
            file_source->writeBatchWithHashval(batch, i, DatumGetUInt32(m_cacheLoc[i]));
        }
    } else {
        for (i = 0; i < row; i++) {
            file_source->writeBatch(batch, i, DatumGetUInt32(m_cacheLoc[i]));
        }
    }
    (void)pgstat_report_waitstatus(old_status);
}

VectorBatch* HashJoinTbl::Probe()
{
    return RuntimeBinding(m_probeFun, m_strategy)();
}

#define InitJoinTemplate(complicateFlag, flag)                                            \
    do {                                                                                  \
        m_joinFunArray[i++] = &HashJoinTbl::innerJoinT<complicateFlag, flag>;             \
        m_joinFunArray[i++] = &HashJoinTbl::leftJoinT<complicateFlag, flag>;              \
        m_joinFunArray[i++] = &HashJoinTbl::leftJoinWithQualT<complicateFlag, flag>;      \
        m_joinFunArray[i++] = &HashJoinTbl::rightJoinT<complicateFlag, flag>;             \
        m_joinFunArray[i++] = &HashJoinTbl::rightJoinWithQualT<complicateFlag, flag>;     \
        m_joinFunArray[i++] = &HashJoinTbl::semiJoinT<complicateFlag, flag>;              \
        m_joinFunArray[i++] = &HashJoinTbl::semiJoinWithQualT<complicateFlag, flag>;      \
        m_joinFunArray[i++] = &HashJoinTbl::antiJoinT<complicateFlag, flag>;              \
        m_joinFunArray[i++] = &HashJoinTbl::antiJoinWithQualT<complicateFlag, flag>;      \
        m_joinFunArray[i++] = &HashJoinTbl::rightSemiJoinT<complicateFlag, flag>;         \
        m_joinFunArray[i++] = &HashJoinTbl::rightSemiJoinWithQualT<complicateFlag, flag>; \
        m_joinFunArray[i++] = &HashJoinTbl::rightAntiJoinT<complicateFlag, flag>;         \
        m_joinFunArray[i++] = &HashJoinTbl::rightAntiJoinWithQualT<complicateFlag, flag>; \
        m_joinFunArray[i++] = &HashJoinTbl::antiJoinT<complicateFlag, flag>;              \
        m_joinFunArray[i++] = &HashJoinTbl::antiJoinWithQualT<complicateFlag, flag>;      \
        m_joinFunArray[i++] = &HashJoinTbl::rightAntiJoinT<complicateFlag, flag>;         \
        m_joinFunArray[i++] = &HashJoinTbl::rightAntiJoinWithQualT<complicateFlag, flag>; \
    } while (0);

template <bool complicate_join_key>
void HashJoinTbl::bindingFp()
{
    if (m_innerSimple)
        m_funBuild[0] = &HashJoinTbl::SaveToMemory<complicate_join_key, true>;
    else
        m_funBuild[0] = &HashJoinTbl::SaveToMemory<complicate_join_key, false>;

    m_funBuild[1] = &HashJoinTbl::SaveToDisk<complicate_join_key, true>;

    m_probeFun[0] = &HashJoinTbl::probeMemory;
    m_probeFun[1] = &HashJoinTbl::probeGrace;

    int i = 0;
    int idx_array[] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    int base_idx = m_keySimple ? 0 : 17;
    InitJoinTemplate(complicate_join_key, true) InitJoinTemplate(complicate_join_key, false);
    int array_idx = (m_runtime->js.joinqual == NULL) ? (2 * m_joinType) : (2 * m_joinType) + 1;
    m_joinFun = m_joinFunArray[base_idx + idx_array[array_idx]];

    if (complicate_join_key == false) {
        for (i = 0; i < m_key; i++) {
            if (m_simpletype[i])
                DispatchKeyInnerFunction(i);
            else {
                if (m_runtime->js.nulleqqual != NIL)
                    m_matchKeyFunction[i] = &HashJoinTbl::matchKey<int64, int64, false, true>;
                else
                    m_matchKeyFunction[i] = &HashJoinTbl::matchKey<int64, int64, false, false>;
            }
        }
    }
}

void HashJoinTbl::DispatchKeyInnerFunction(int key_idx)
{
    switch (m_keyDesc[key_idx].typeId) {
        case INT1OID:
            DispatchKeyOuterFunction<uint8>(key_idx);
            break;
        case INT2OID:
            DispatchKeyOuterFunction<int16>(key_idx);
            break;
        case INT4OID:
            DispatchKeyOuterFunction<int32>(key_idx);
            break;
        case INT8OID:
            DispatchKeyOuterFunction<int64>(key_idx);
            break;
        default:
            break;
    }
}

template <typename innerType>
void HashJoinTbl::DispatchKeyOuterFunction(int key_idx)
{
    switch (m_outerkeyType[key_idx]) {
        case INT1OID:
            if (m_runtime->js.nulleqqual != NIL)
                m_matchKeyFunction[key_idx] = &HashJoinTbl::matchKey<innerType, uint8, true, true>;
            else
                m_matchKeyFunction[key_idx] = &HashJoinTbl::matchKey<innerType, uint8, true, false>;
            break;
        case INT2OID:
            if (m_runtime->js.nulleqqual != NIL)
                m_matchKeyFunction[key_idx] = &HashJoinTbl::matchKey<innerType, int16, true, true>;
            else
                m_matchKeyFunction[key_idx] = &HashJoinTbl::matchKey<innerType, int16, true, false>;
            break;
        case INT4OID:
            if (m_runtime->js.nulleqqual != NIL)
                m_matchKeyFunction[key_idx] = &HashJoinTbl::matchKey<innerType, int32, true, true>;
            else
                m_matchKeyFunction[key_idx] = &HashJoinTbl::matchKey<innerType, int32, true, false>;
            break;
        case INT8OID:
            if (m_runtime->js.nulleqqual != NIL)
                m_matchKeyFunction[key_idx] = &HashJoinTbl::matchKey<innerType, int64, true, true>;
            else
                m_matchKeyFunction[key_idx] = &HashJoinTbl::matchKey<innerType, int64, true, false>;
            break;
        default:
            break;
    }
}

VectorBatch* HashJoinTbl::probeMemory()
{
    return probeHashTable(m_probOpSource);
}

template <bool complicate_join_key>
void HashJoinTbl::probePartition()
{
    VectorBatch* batch = NULL;
    PlanState* outer_node = outerPlanState(m_runtime);

    /*
     * To avoid too many file handler's buffer simultaneously, we init file for
     * probe side here (at the end of partition for build side). Since, in compress
     * mode, temp file handler's buffer > 64KB(or may be even larger), we partition
     * one by one side, and release the buffer at the end of each partition.
     *
     * Note: file handler's buffer are allocated after initFile() being called.
     */
    Assert(m_buildFileSource != NULL);
    initFile(false, m_outerBatch, m_buildFileSource->m_fileNum);

    WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_WRITE_FILE);
    for (;;) {
        batch = VectorEngine(outer_node);
        if (unlikely(BatchIsNull(batch)))
            break;

        SaveToDisk<complicate_join_key, false>(batch);
    }

    (void)pgstat_report_waitstatus(old_status);
    HASH_BASED_DEBUG(recordPartitionInfo(false, -1, 0, m_probeFileSource->m_fileNum));

    /* release the buffer in file handler */
    m_probeFileSource->ReleaseAllFileHandlerBuffer();
}

/*
 * @Description: repartition process on a specific file
 * @in source: build or probe file source
 * @in file_idx: index of the specific file which will be repartitioned
 * @return: void
 */
template <bool complicate_join_key, bool build_side>
void HashJoinTbl::RePartitionFileSource(hashFileSource* source, int file_idx)
{
    VectorBatch* batch = NULL;
    PlanState* plan_state = &m_runtime->js.ps;
    int file_num;
    int old_file_num;
    int i;
    int row; /* batch rows */
    int* key_idx = build_side ? m_keyIdx : m_outKeyIdx;
    int idx;
    HashKey hkey;
    uint32 rbit;

    Assert(m_pLevel != NULL);

    /* compute temp file numbers by: filerows & fileSize */
    if (build_side) {
        int rows; /* file rows */
        rows = source->m_rownum[file_idx];
        file_num = getPower2NextNum((rows * 2 * sizeof(hashCell*) /* necessary pointers */
                                       + source->m_fileSize[file_idx]) /
                                   m_totalMem); /* file_size/available_mem */
        file_num = Max(2, file_num);
        file_num = Min(file_num, 1024);
    } else {
        file_num = m_buildFileSource->m_fileNum - m_probeFileSource->m_fileNum;
        Assert(file_num > 0);
    }

    old_file_num = source->m_fileNum;
    source->enlargeFileSource(file_num);

    /* enlarge pLevel only once */
    if (build_side) {
        int j;
        m_pLevel = (uint8*)repalloc(m_pLevel, (old_file_num + file_num) * sizeof(uint8));
        m_isValid = (bool*)repalloc(m_isValid, (old_file_num + file_num) * sizeof(bool));
        for (j = old_file_num, i = 0; i < file_num; i++, j++) {
            m_pLevel[j] = m_pLevel[file_idx] + 1;
            m_isValid[j] = true;
        }
    }

    /* rot bits: avoid using same bits of hashvalue everytime */
    rbit = m_pLevel[file_idx] * 10;

    /* repartition process */
    WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_WRITE_FILE);
    for (;;) {
        batch = source->getBatch();
        if (unlikely(BatchIsNull(batch)))
            break;

        /* save the batch to disk */
        row = batch->m_rows;
        if (complicate_join_key) {
            int icol = batch->m_cols - 1;
            for (i = 0; i < row; i++) {
                /* hashvalue already in batch: at last column */
                hkey = DatumGetUInt32(batch->m_arr[icol].m_vals[i]);

                /* new file idx: repartition strategy */
                idx = old_file_num + (leftrot(hkey, rbit) & (file_num - 1));
                source->writeBatchWithHashval(batch, i, hkey, idx);
            }
        } else {
            /* we do not have hashvalue yet, so compute it first */
            if (build_side)
                hashBatch(batch, key_idx, m_cacheLoc, m_innerHashFuncs, true);
            else
                hashBatch(batch, key_idx, m_cacheLoc, m_outerHashFuncs, true);

            for (i = 0; i < row; i++) {
                hkey = DatumGetUInt32(m_cacheLoc[i]);
                idx = old_file_num + (leftrot(hkey, rbit) & (file_num - 1));
                source->writeBatchToFile(batch, i, idx);
            }
        }

        /* avoid memory growing since these are Big files */
        MemoryContextReset(source->m_context);
    }
    (void)pgstat_report_waitstatus(old_status);

    /* after repartition, release the buffer in file handler */
    for (i = 0; i < file_num; i++)
        source->ReleaseFileHandlerBuffer(old_file_num + i);

    HASH_BASED_DEBUG(recordPartitionInfo(build_side, file_idx, old_file_num, old_file_num + file_num));

    if (m_pLevel[file_idx] >= WARNING_SPILL_TIME) {
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->warning |= (1 << WLM_WARN_SPILL_TIMES_LARGE);
    }
    /* add instrument */
    if (build_side && HAS_INSTR(&m_runtime->js, true)) {
        plan_state->instrument->sorthashinfo.hash_FileNum += file_num;
        plan_state->instrument->sorthashinfo.hash_spillNum =
            Max(m_pLevel[file_idx], plan_state->instrument->sorthashinfo.hash_spillNum);

        if (m_pLevel[file_idx] >= WARNING_SPILL_TIME) {
            m_runtime->js.ps.instrument->warning |= (1 << WLM_WARN_SPILL_TIMES_LARGE);
        }
    }
}

void HashJoinTbl::preparePartition()
{
    int64 build_side_row_num;
    int64 probe_side_row_num;
    bool join_on_this_partition = false;
    int old_row_num;
    int old_file_num;
    int max_sub_row_num;

    WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_BUILD_HASH);
    while (m_probeIdx < m_buildFileSource->m_fileNum) {
        build_side_row_num = m_buildFileSource->m_rownum[m_probeIdx];
        probe_side_row_num = m_probeFileSource->m_rownum[m_probeIdx];

        switch (m_joinType) {
            case HASH_JOIN_INNER:
            case HASH_JOIN_SEMI:
            case HASH_JOIN_RIGHT_SEMI:
                join_on_this_partition = (build_side_row_num > 0) && (probe_side_row_num > 0);
                break;

            case HASH_JOIN_LEFT:
            case HASH_JOIN_ANTI:
            case HASH_JOIN_LEFT_ANTI_FULL:
                join_on_this_partition = (probe_side_row_num > 0);
                break;

            case HASH_JOIN_RIGHT_ANTI:
            case HASH_JOIN_RIGHT:
            case HASH_JOIN_RIGHT_ANTI_FULL:
                join_on_this_partition = (build_side_row_num > 0);
                break;

            default:
                Assert(false);
                break;
        }

        if (join_on_this_partition) {
            /* prepare the buffer in file handler */
            m_buildFileSource->PrepareFileHandlerBuffer(m_probeIdx);
            m_probeFileSource->PrepareFileHandlerBuffer(m_probeIdx);

            /* reset buffer and file descriptor */
            MemoryContextReset(m_hashContext);
            m_buildFileSource->rewind(m_probeIdx);
            m_probeFileSource->rewind(m_probeIdx);

            m_buildFileSource->setCurrentIdx(m_probeIdx);
            m_probeFileSource->setCurrentIdx(m_probeIdx);

            MemoryContextReset(m_buildFileSource->m_context);
            MemoryContextReset(m_probeFileSource->m_context);

            /*
             * Check the file source if we have enough memory for it.
             * If not, Hash Join can compare the fileSize or file_num of the file before and
             * after repartitioned to decide whether it can be ended or not.
             */
            if (m_isValid[m_probeIdx] && m_buildFileSource->m_fileSize[m_probeIdx] > m_totalMem) {
                int i;
                old_row_num = m_buildFileSource->m_rownum[m_probeIdx];
                old_file_num = m_buildFileSource->m_fileNum;

                if (m_complicateJoinKey) {
                    RePartitionFileSource<true, true>(m_buildFileSource, m_probeIdx);
                    RePartitionFileSource<true, false>(m_probeFileSource, m_probeIdx);
                } else {
                    RePartitionFileSource<false, true>(m_buildFileSource, m_probeIdx);
                    RePartitionFileSource<false, false>(m_probeFileSource, m_probeIdx);
                }

                /* If this partition is already partitioned 'm_maxPLevel' times, we would pay extra attention to it.*/
                if (m_pLevel[m_probeIdx] >= m_maxPLevel) {
                    elog(LOG,
                        "[VecHashJoin] Warning: file [%d] has partitioned %d times, which exceeds 'm_maxPLevel' times."
                        "Please pay attention to it.",
                        m_probeIdx,
                        m_pLevel[m_probeIdx]);
                }
                   
                /* Find the max rowNum from new partition files repartitioned*/
                max_sub_row_num = m_buildFileSource->m_rownum[old_file_num];
                for (i = old_file_num + 1; i < m_buildFileSource->m_fileNum; i++) {
                    max_sub_row_num = Max(max_sub_row_num, m_buildFileSource->m_rownum[i]);
                }
                    
                /*
                 * For a invalid repartition process, the m_isValid of these new partitions will be marked as false
                 * Note: these new partitions will not be repartitioned any more.
                 */
                if (max_sub_row_num == old_row_num) {
                    for (i = old_file_num; i < m_buildFileSource->m_fileNum; i++)
                        m_isValid[i] = false;
                }

                /* Close the unused file and contniue*/
                m_buildFileSource->close(m_probeIdx);
                m_probeIdx++;
                continue;
            }

            if (!m_isValid[m_probeIdx] && !m_isWarning) {
                m_isWarning = true;
                elog(LOG,
                    "[VecHashJoin] Warning: in file[%d] after %d times of repartition, data to be built hash table may "
                    "be duplicated or too large, "
                    "try ANALYZE first to get better plan.",
                    m_probeIdx,
                    m_pLevel[m_probeIdx]);
            }

            /* build hash table on context m_hashContext */
            AutoContextSwitch memguard(m_hashContext);
            if (m_complicateJoinKey == true) {
                buildHashTable<true, true>(m_buildFileSource, m_buildFileSource->m_rownum[m_probeIdx]);
            } else {
                buildHashTable<false, true>(m_buildFileSource, m_buildFileSource->m_rownum[m_probeIdx]);
            }
                
            bool can_wlm_warning_statistics = false;
            /* analysis hash table information about each file */
            if (anls_opt_is_on(ANLS_HASH_CONFLICT)) {
                char stats[MAX_LOG_LEN];
                m_hashTbl->Profile(stats, &can_wlm_warning_statistics);
                ereport(LOG,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errmsg("[VecHashJoin(%d)(temp file:%d)] %s",
                            m_runtime->js.ps.plan->plan_node_id,
                            m_probeIdx,
                            stats)));
            } else if (u_sess->attr.attr_resource.resource_track_level >= RESOURCE_TRACK_QUERY &&
                       u_sess->attr.attr_resource.enable_resource_track && u_sess->exec_cxt.need_track_resource) {
                char stats[MAX_LOG_LEN];
                m_hashTbl->Profile(stats, &can_wlm_warning_statistics);
            }
            if (can_wlm_warning_statistics) {
                pgstat_add_warning_hash_conflict();
                if (HAS_INSTR(&m_runtime->js, true)) {
                    m_runtime->js.ps.instrument->warning |= (1 << WLM_WARN_HASH_CONFLICT);
                }
            }

            break;
        } else {
            m_buildFileSource->close(m_probeIdx);
            m_probeFileSource->close(m_probeIdx);
            m_probeIdx++;
        }
    }
    (void)pgstat_report_waitstatus(old_status);
}

VectorBatch* HashJoinTbl::probeGrace()
{
    VectorBatch* res_batch = NULL;

    while (true) {
        switch (m_probeStatus) {
            case PROBE_PARTITION_FILE:
                if (m_complicateJoinKey == true)
                    probePartition<true>();
                else
                    probePartition<false>();
                m_probeStatus = PROBE_PREPARE_PAIR;
                break;

            case PROBE_PREPARE_PAIR:

                preparePartition();
                // all finished
                if (m_probeIdx == m_probeFileSource->m_fileNum)
                    return NULL;

                m_probeStatus = PROBE_FETCH;
                break;

            case PROBE_FETCH:
            case PROBE_DATA:
            case PROBE_FINAL:

                res_batch = probeHashTable(m_probeFileSource);
                if (BatchIsNull(res_batch) == false) {
                    return res_batch;
                } else {
                    // close the unused file.
                    m_buildFileSource->close(m_probeIdx);
                    m_probeFileSource->close(m_probeIdx);

                    // go to next cache pair
                    m_probeIdx++;
                    m_probeStatus = PROBE_PREPARE_PAIR;
                }
                break;
            default:
                break;
        }
    }
}

VectorBatch* HashJoinTbl::probeHashTable(hashSource* probSource)
{
    VectorBatch* res_batch = NULL;

    while (true) {
        switch (m_probeStatus) {
            case PROBE_FETCH:
                /* we can safely reset the probe file source buffer per batch-line */
                if (m_probeFileSource != NULL)
                    MemoryContextReset(m_probeFileSource->m_context);
                m_outRawBatch = probSource->getBatch();
                if (BatchIsNull(m_outRawBatch)) {
                    m_probeStatus = PROBE_FINAL;
                    m_doProbeData = true;
                } else if (m_runtime->jitted_probeHashTable) {
                    /* LLVM compiled execution on CPU intensive part of probeHashTable */
                    typedef void (*probeHashTable_func)(HashJoinTbl* HJT, VectorBatch* batch);
                    ((probeHashTable_func)(m_runtime->jitted_probeHashTable))(this, m_outRawBatch);

                    m_joinStateLog.restore = false;
                    m_probeStatus = PROBE_DATA;
                    m_doProbeData = true;
                } else {
                    int row = m_outRawBatch->m_rows;
                    int mask = m_hashTbl->m_size - 1;

                    if (m_complicateJoinKey && m_pLevel != NULL) {
                        /* grace hash join and hashvalue already in batch: at last column */
                        int icol = m_outRawBatch->m_cols - 1;

                        for (int i = 0; i < row; i++) {
                            m_cacheLoc[i] = m_outRawBatch->m_arr[icol].m_vals[i] & mask;
                            m_cellCache[i] = m_hashTbl->m_data[m_cacheLoc[i]];
                            m_match[i] = false; /* flag all the row no match */
                            m_keyMatch[i] = true;
                        }
                    } else {
                        /* in-memory hash join or grace hash without complicate_join_key */
                        if (m_complicateJoinKey)
                            CalcComplicateHashVal(m_outRawBatch, m_runtime->hj_OuterHashKeys, false);
                        else
                            hashBatch(m_outRawBatch, m_outKeyIdx, m_cacheLoc, m_outerHashFuncs);
                        for (int i = 0; i < row; i++) {
                            m_cacheLoc[i] = m_cacheLoc[i] & mask;
                            m_cellCache[i] = m_hashTbl->m_data[m_cacheLoc[i]];
                            m_match[i] = false; /* flag all the row no match */
                            m_keyMatch[i] = true;
                        }
                    }

                    m_joinStateLog.restore = false;
                    m_probeStatus = PROBE_DATA;
                    m_doProbeData = true;
                }
                break;
            case PROBE_DATA:
                res_batch = (this->*m_joinFun)(m_outRawBatch);
                if (!BatchIsNull(res_batch))
                    return res_batch;
                break;

            case PROBE_FINAL:
                return endJoin();
            default:
                break;
        }
    }
}

VectorBatch* HashJoinTbl::endJoin()
{
    VectorBatch* res_batch = NULL;

    if (m_doProbeData == false)
        return NULL;

    if (m_joinType == HASH_JOIN_RIGHT || m_joinType == HASH_JOIN_RIGHT_ANTI ||
        m_joinType == HASH_JOIN_RIGHT_ANTI_FULL) {
        hashCell* cell = NULL;
        int entry_idx = 0;
        int result_row = 0;
        ScalarVector* p_vector = NULL;
        hashVal* val = NULL;
        int i, col_idx;
        int k = m_cols - 1;

        Assert(k >= 0);
        if (m_joinStateLog.restore) {
            entry_idx = m_joinStateLog.lastBuildIdx;
            cell = m_joinStateLog.lastCell;
            m_joinStateLog.restore = false;
        }

        int hash_size = m_hashTbl->m_size;
        hashCell** hash_data = m_hashTbl->m_data;
        for (i = entry_idx; i < hash_size; i++) {
            if (cell == NULL)
                cell = hash_data[i];

            while (cell != NULL) {
                if (cell->m_val[k].val == 0) {
                    val = cell->m_val;
                    /* do not need outer rows */
                    for (col_idx = 0; col_idx < m_outerBatch->m_cols; col_idx++) {
                        p_vector = &m_outerBatch->m_arr[col_idx];
                        SET_NULL(p_vector->m_flag[result_row]);
                    }

                    /* need inner rows */
                    for (col_idx = 0; col_idx < m_innerBatch->m_cols; col_idx++) {
                        p_vector = &m_innerBatch->m_arr[col_idx];
                        p_vector->m_vals[result_row] = val[col_idx].val;
                        p_vector->m_flag[result_row] = val[col_idx].flag;
                    }

                    result_row++;
                    if (result_row == BatchMaxSize) {
                        m_innerBatch->FixRowCount(BatchMaxSize);
                        m_outerBatch->FixRowCount(BatchMaxSize);
                        res_batch = buildResult(m_innerBatch, m_outerBatch, false);

                        /* Do not return the NULL value, until exit for loop. */
                        if (!BatchIsNull(res_batch)) {
                            m_joinStateLog.restore = true;

                            if (cell->flag.m_next) {
                                m_joinStateLog.lastBuildIdx = i;
                                m_joinStateLog.lastCell = cell->flag.m_next;
                            } else {
                                m_joinStateLog.lastBuildIdx = i + 1;
                                m_joinStateLog.lastCell = NULL;
                            }

                            return res_batch;
                        } else
                            result_row = 0;
                    }
                }

                cell = cell->flag.m_next;
            }
        }

        m_doProbeData = false;
        m_innerBatch->FixRowCount(result_row);
        m_outerBatch->FixRowCount(result_row);
        if (m_innerBatch->m_rows != 0)
            return buildResult(m_innerBatch, m_outerBatch, false);
        else
            return NULL;
    }

    return NULL;
}

template <bool complicate_join_key, bool simple_key>
VectorBatch* HashJoinTbl::leftJoinT(VectorBatch* batch)
{
    int i, row_idx;
    int row = batch->m_rows;
    int result_row = m_outerBatch->m_rows;
    hashVal* val = NULL;
    int last_build_idx = 0;
    ScalarVector* p_vector = NULL;

    while (m_doProbeData) {
        last_build_idx = 0;

        if (m_joinStateLog.restore == false) {
            if (complicate_join_key)
                matchComplicateKey(batch);
            else {
                for (i = 0; i < m_key; i++)
                    RuntimeBinding(m_matchKeyFunction, i)(&batch->m_arr[m_outKeyIdx[i]], row, m_keyIdx[i], i);
            }
        } else {
            m_joinStateLog.restore = false;
            last_build_idx = m_joinStateLog.lastBuildIdx;
        }

        for (row_idx = last_build_idx; row_idx < row; row_idx++) {
            if (m_keyMatch[row_idx]) {
                m_match[row_idx] = true;
                val = m_cellCache[row_idx]->m_val;

                for (i = 0; i < m_innerBatch->m_cols; i++) {
                    p_vector = &m_innerBatch->m_arr[i];

                    p_vector->m_vals[result_row] = val[i].val;
                    p_vector->m_flag[result_row] = val[i].flag;
                }

                for (i = 0; i < m_outerBatch->m_cols; i++) {
                    p_vector = &m_outerBatch->m_arr[i];
                    p_vector->m_vals[result_row] = batch->m_arr[i].m_vals[row_idx];
                    p_vector->m_flag[result_row] = batch->m_arr[i].m_flag[row_idx];
                }
                result_row++;
            }

            if (result_row == BatchMaxSize) {
                m_innerBatch->FixRowCount(BatchMaxSize);
                m_outerBatch->FixRowCount(BatchMaxSize);
                m_joinStateLog.lastBuildIdx = row_idx + 1;
                m_joinStateLog.restore = true;
                return buildResult(m_innerBatch, m_outerBatch, false);  // has no joinqual
            }
        }

        // do the next
        m_doProbeData = false;
        for (row_idx = 0; row_idx < row; row_idx++) {
            m_keyMatch[row_idx] = true;  // reset the key match
            if (m_cellCache[row_idx] != NULL)
                m_cellCache[row_idx] = m_cellCache[row_idx]->flag.m_next;

            if (m_cellCache[row_idx] != NULL)
                m_doProbeData = true;
        }
    }

    if (m_joinStateLog.restore) {
        last_build_idx = m_joinStateLog.lastBuildIdx;
        m_joinStateLog.restore = false;
    } else
        last_build_idx = 0;

    for (row_idx = last_build_idx; row_idx < row; row_idx++) {
        if (m_match[row_idx] == false) {
            for (i = 0; i < m_outerBatch->m_cols; i++) {
                p_vector = &m_outerBatch->m_arr[i];
                p_vector->m_vals[result_row] = batch->m_arr[i].m_vals[row_idx];
                p_vector->m_flag[result_row] = batch->m_arr[i].m_flag[row_idx];
            }

            for (i = 0; i < m_innerBatch->m_cols; i++) {
                p_vector = &m_innerBatch->m_arr[i];
                SET_NULL(p_vector->m_flag[result_row]);
            }

            result_row++;
            if (result_row == BatchMaxSize) {
                m_innerBatch->FixRowCount(BatchMaxSize);
                m_outerBatch->FixRowCount(BatchMaxSize);
                m_joinStateLog.lastBuildIdx = row_idx + 1;
                m_joinStateLog.restore = true;
                return buildResult(m_innerBatch, m_outerBatch, false);  // has no joinqual
            }
        }
    }

    m_probeStatus = PROBE_FETCH;
    m_innerBatch->FixRowCount(result_row);
    m_outerBatch->FixRowCount(result_row);
    // null result
    if (m_innerBatch->m_rows != 0)
        return buildResult(m_innerBatch, m_outerBatch, false);  // has no joinqual
    else
        return NULL;
}

template <bool complicate_join_key, bool simple_key>
VectorBatch* HashJoinTbl::leftJoinWithQualT(VectorBatch* batch)
{
    int i, row_idx;
    int row = batch->m_rows;
    int result_qual_row = m_outQualBatch->m_rows;
    int result_row = m_outerBatch->m_rows;

    hashVal* val = NULL;
    int last_build_idx = 0;
    ScalarVector* p_vector = NULL;
    bool* sel = NULL;
    int org_idx = 0;

    Assert(result_qual_row == 0);

    while (m_doProbeData) {
        last_build_idx = 0;

        if (m_joinStateLog.restore == false) {
            if (complicate_join_key)
                matchComplicateKey(batch);
            else {
                for (i = 0; i < m_key; i++)
                    RuntimeBinding(m_matchKeyFunction, i)(&batch->m_arr[m_outKeyIdx[i]], row, m_keyIdx[i], i);
            }

            for (row_idx = 0; row_idx < row; row_idx++) {
                if (m_keyMatch[row_idx]) {
                    val = m_cellCache[row_idx]->m_val;

                    for (i = 0; i < m_inQualBatch->m_cols; i++) {
                        p_vector = &m_inQualBatch->m_arr[i];

                        p_vector->m_vals[result_qual_row] = val[i].val;
                        p_vector->m_flag[result_qual_row] = val[i].flag;
                        p_vector->m_rows++;
                    }

                    for (i = 0; i < m_outQualBatch->m_cols; i++) {
                        p_vector = &m_outQualBatch->m_arr[i];
                        p_vector->m_vals[result_qual_row] = batch->m_arr[i].m_vals[row_idx];
                        p_vector->m_flag[result_qual_row] = batch->m_arr[i].m_flag[row_idx];
                        p_vector->m_rows++;
                    }
                    m_reCheckCell[result_qual_row].oriIdx = row_idx;
                    result_qual_row++;
                }
            }

            // we need to see if these rows are truely pass
            if (result_qual_row > 0) {
                m_inQualBatch->m_rows += result_qual_row;
                m_outQualBatch->m_rows += result_qual_row;
                sel = checkQual(m_inQualBatch, m_outQualBatch);
                for (i = 0; i < result_qual_row; i++) {
                    org_idx = m_reCheckCell[i].oriIdx;
                    if (sel[i] == true) {
                        m_match[org_idx] = true;  // flag we truly match, as we must fill null for inner side
                    } else {
                        m_keyMatch[org_idx] = false;  // flag we are not truly match.
                    }
                }

                result_qual_row = 0;
            }

        } else {
            m_joinStateLog.restore = false;
            last_build_idx = m_joinStateLog.lastBuildIdx;
        }

        for (row_idx = last_build_idx; row_idx < row; row_idx++) {
            if (m_keyMatch[row_idx]) {
                val = m_cellCache[row_idx]->m_val;
                for (i = 0; i < m_innerBatch->m_cols; i++) {
                    p_vector = &m_innerBatch->m_arr[i];

                    p_vector->m_vals[result_row] = val[i].val;
                    p_vector->m_flag[result_row] = val[i].flag;
                    p_vector->m_rows++;
                }

                for (i = 0; i < m_outerBatch->m_cols; i++) {
                    p_vector = &m_outerBatch->m_arr[i];
                    p_vector->m_vals[result_row] = batch->m_arr[i].m_vals[row_idx];
                    p_vector->m_flag[result_row] = batch->m_arr[i].m_flag[row_idx];
                    p_vector->m_rows++;
                }

                result_row++;
            }

            if (result_row == BatchMaxSize) {
                m_innerBatch->m_rows = BatchMaxSize;
                m_outerBatch->m_rows = BatchMaxSize;
                m_joinStateLog.lastBuildIdx = row_idx + 1;
                m_joinStateLog.restore = true;
                return buildResult(m_innerBatch, m_outerBatch, false);
            }
        }

        // do the next
        m_doProbeData = false;
        for (row_idx = 0; row_idx < row; row_idx++) {
            m_keyMatch[row_idx] = true;  // reset the key match
            if (m_cellCache[row_idx] != NULL)
                m_cellCache[row_idx] = m_cellCache[row_idx]->flag.m_next;

            if (m_cellCache[row_idx] != NULL)
                m_doProbeData = true;
        }
    }

    if (m_joinStateLog.restore) {
        last_build_idx = m_joinStateLog.lastBuildIdx;
        m_joinStateLog.restore = false;
    } else
        last_build_idx = 0;

    for (row_idx = last_build_idx; row_idx < row; row_idx++) {
        if (m_match[row_idx] == false) {
            for (i = 0; i < m_outerBatch->m_cols; i++) {
                p_vector = &m_outerBatch->m_arr[i];
                p_vector->m_vals[result_row] = batch->m_arr[i].m_vals[row_idx];
                p_vector->m_flag[result_row] = batch->m_arr[i].m_flag[row_idx];
                p_vector->m_rows++;
            }

            for (i = 0; i < m_innerBatch->m_cols; i++) {
                p_vector = &m_innerBatch->m_arr[i];
                SET_NULL(p_vector->m_flag[result_row]);
                p_vector->m_rows++;
            }

            result_row++;

            if (result_row == BatchMaxSize) {
                m_innerBatch->m_rows = BatchMaxSize;
                m_outerBatch->m_rows = BatchMaxSize;
                m_joinStateLog.lastBuildIdx = row_idx + 1;
                m_joinStateLog.restore = true;
                return buildResult(m_innerBatch, m_outerBatch, false);
            }
        }
    }

    m_probeStatus = PROBE_FETCH;
    m_innerBatch->m_rows = result_row;
    m_outerBatch->m_rows = result_row;
    // null result
    if (m_innerBatch->m_rows != 0)
        return buildResult(m_innerBatch, m_outerBatch, false);
    else
        return NULL;
}

template <bool complicate_join_key, bool simple_key>
VectorBatch* HashJoinTbl::innerJoinT(VectorBatch* batch)
{
    int i, row_idx;
    int row = batch->m_rows;
    int result_row = 0;
    hashVal* val = NULL;
    int last_build_idx = 0;
    ScalarVector* p_vector = NULL;

    if (m_runtime->jitted_innerjoin) {
        /* Mark inner hash join part has been codegened */
        if (HAS_INSTR(&m_runtime->js, false))
            m_runtime->js.ps.instrument->isLlvmOpt = true;

        /* LLVM compiled execution with inlined matchKey */
        typedef int (*vechashjoin_func)(HashJoinTbl* HJT, VectorBatch* batch);
        result_row = ((vechashjoin_func)(m_runtime->jitted_innerjoin))(this, batch);

        if (result_row != BatchMaxSize)
            m_probeStatus = PROBE_FETCH;

        m_innerBatch->FixRowCount(result_row);
        m_outerBatch->FixRowCount(result_row);

        if (m_innerBatch->m_rows != 0)
            return buildResult(m_innerBatch, m_outerBatch, true);
        else
            return NULL;
    } else {
        /* original code without LLVM compiled execution */
        while (m_doProbeData) {
            last_build_idx = 0;

            if (m_joinStateLog.restore == false) {
                if (complicate_join_key)
                    matchComplicateKey(batch);
                else {
                    for (i = 0; i < m_key; i++)
                        RuntimeBinding(m_matchKeyFunction, i)(&batch->m_arr[m_outKeyIdx[i]], row, m_keyIdx[i], i);
                }
            } else {
                m_joinStateLog.restore = false;
                last_build_idx = m_joinStateLog.lastBuildIdx;
            }

            for (row_idx = last_build_idx; row_idx < row; row_idx++) {
                if (m_keyMatch[row_idx]) {
                    val = m_cellCache[row_idx]->m_val;

                    for (i = 0; i < m_innerBatch->m_cols; i++) {
                        p_vector = &m_innerBatch->m_arr[i];

                        p_vector->m_vals[result_row] = val[i].val;
                        p_vector->m_flag[result_row] = val[i].flag;
                    }

                    for (i = 0; i < m_outerBatch->m_cols; i++) {
                        p_vector = &m_outerBatch->m_arr[i];
                        p_vector->m_vals[result_row] = batch->m_arr[i].m_vals[row_idx];
                        p_vector->m_flag[result_row] = batch->m_arr[i].m_flag[row_idx];
                    }
                    result_row++;
                }

                if (result_row == BatchMaxSize) {
                    m_innerBatch->FixRowCount(BatchMaxSize);
                    m_outerBatch->FixRowCount(BatchMaxSize);
                    m_joinStateLog.lastBuildIdx = row_idx + 1;
                    m_joinStateLog.restore = true;
                    return buildResult(m_innerBatch, m_outerBatch, true);
                }
            }

            // do the next
            m_doProbeData = false;
            for (row_idx = 0; row_idx < row; row_idx++) {
                m_keyMatch[row_idx] = true;  // reset the key match
                if (m_cellCache[row_idx] != NULL)
                    m_cellCache[row_idx] = m_cellCache[row_idx]->flag.m_next;

                if (m_cellCache[row_idx] != NULL)
                    m_doProbeData = true;
            }
        }

        m_probeStatus = PROBE_FETCH;
        m_innerBatch->FixRowCount(result_row);
        m_outerBatch->FixRowCount(result_row);
        // null result
        if (m_innerBatch->m_rows != 0)
            return buildResult(m_innerBatch, m_outerBatch, true);
        else
            return NULL;
    }
}

template <bool complicate_join_key, bool simple_key>
VectorBatch* HashJoinTbl::semiJoinT(VectorBatch* batch)
{
    int i, row_idx;
    int row = batch->m_rows;
    int result_row = m_outerBatch->m_rows;
    hashVal* val = NULL;
    ScalarVector* p_vector = NULL;

    while (m_doProbeData) {
        if (complicate_join_key)
            matchComplicateKey(batch);
        else {
            for (i = 0; i < m_key; i++)
                RuntimeBinding(m_matchKeyFunction, i)(&batch->m_arr[m_outKeyIdx[i]], row, m_keyIdx[i], i);
        }

        m_doProbeData = false;
        for (row_idx = 0; row_idx < row; row_idx++) {
            if (m_keyMatch[row_idx]) {
                val = m_cellCache[row_idx]->m_val;

                // some case need inner
                for (i = 0; i < m_innerBatch->m_cols; i++) {
                    p_vector = &m_innerBatch->m_arr[i];

                    p_vector->m_vals[result_row] = val[i].val;
                    p_vector->m_flag[result_row] = val[i].flag;
                }

                // only need outer.
                for (i = 0; i < m_outerBatch->m_cols; i++) {
                    p_vector = &m_outerBatch->m_arr[i];
                    p_vector->m_vals[result_row] = batch->m_arr[i].m_vals[row_idx];
                    p_vector->m_flag[result_row] = batch->m_arr[i].m_flag[row_idx];
                }

                result_row++;
                m_cellCache[row_idx] = NULL;  // mark this row no need further compare
            } else {
                if (m_cellCache[row_idx] != NULL)
                    m_cellCache[row_idx] = m_cellCache[row_idx]->flag.m_next;

                if (m_cellCache[row_idx])
                    m_doProbeData = true;

                m_keyMatch[row_idx] = true;  // reset the key match
            }
        }
    }

    // result row will never exceed batch max size.
    Assert(result_row <= BatchMaxSize);

    m_probeStatus = PROBE_FETCH;
    m_innerBatch->FixRowCount(result_row);
    m_outerBatch->FixRowCount(result_row);
    // null result
    if (m_innerBatch->m_rows != 0)
        return buildResult(m_innerBatch, m_outerBatch, true);
    else
        return NULL;
}

// if match still can fail on qual
// so
template <bool complicate_join_key, bool simple_key>
VectorBatch* HashJoinTbl::semiJoinWithQualT(VectorBatch* batch)
{
    int i, row_idx;
    int row = batch->m_rows;
    int result_qual_row = m_inQualBatch->m_rows;
    int result_row = m_innerBatch->m_rows;
    hashVal* val = NULL;
    ScalarVector* p_vector = NULL;
    bool* sel = NULL;
    int org_idx = 0;
    errno_t rc;

    /* Set cellPoint to NULL.*/
    rc = memset_s(cellPoint, BatchMaxSize * sizeof(hashCell*), 0, BatchMaxSize * sizeof(hashCell*));
    securec_check(rc, "\0", "\0");

    Assert(result_qual_row == 0);

    while (m_doProbeData) {
        if (complicate_join_key)
            matchComplicateKey(batch);
        else {
            for (i = 0; i < m_key; i++)
                RuntimeBinding(m_matchKeyFunction, i)(&batch->m_arr[m_outKeyIdx[i]], row, m_keyIdx[i], i);
        }

        m_doProbeData = false;
        for (row_idx = 0; row_idx < row; row_idx++) {
            if (m_keyMatch[row_idx]) {
                val = m_cellCache[row_idx]->m_val;

                // need inner
                for (i = 0; i < m_inQualBatch->m_cols; i++) {
                    p_vector = &m_inQualBatch->m_arr[i];

                    p_vector->m_vals[result_qual_row] = val[i].val;
                    p_vector->m_flag[result_qual_row] = val[i].flag;
                    p_vector->m_rows++;
                }

                // need outer.
                for (i = 0; i < m_outQualBatch->m_cols; i++) {
                    p_vector = &m_outQualBatch->m_arr[i];
                    p_vector->m_vals[result_qual_row] = batch->m_arr[i].m_vals[row_idx];
                    p_vector->m_flag[result_qual_row] = batch->m_arr[i].m_flag[row_idx];
                    p_vector->m_rows++;
                }

                m_reCheckCell[result_qual_row].oriIdx = row_idx;
                result_qual_row++;
            } else {
                if (m_cellCache[row_idx] != NULL)
                    m_cellCache[row_idx] = m_cellCache[row_idx]->flag.m_next;

                if (m_cellCache[row_idx])
                    m_doProbeData = true;

                m_keyMatch[row_idx] = true;  // reset the key match
            }
        }

        // we need to see if these rows are truely pass
        if (result_qual_row > 0) {
            m_inQualBatch->m_rows += result_qual_row;
            m_outQualBatch->m_rows += result_qual_row;
            sel = checkQual(m_inQualBatch, m_outQualBatch);
            for (i = 0; i < result_qual_row; i++) {
                org_idx = m_reCheckCell[i].oriIdx;
                if (sel[i] == true) {
                    m_match[org_idx] = true;

                    /* Point to match rightTree data.*/
                    if (NULL == cellPoint[org_idx]) {
                        cellPoint[org_idx] = m_cellCache[org_idx];
                    }
                    m_cellCache[org_idx] = NULL;  // no need further compare
                } else {
                    if (m_cellCache[org_idx] != NULL)
                        m_cellCache[org_idx] = m_cellCache[org_idx]->flag.m_next;

                    if (m_cellCache[org_idx])
                        m_doProbeData = true;

                    m_keyMatch[org_idx] = true;
                }
            }

            result_qual_row = 0;
        }
    }

    Assert(result_row == 0);
    for (row_idx = 0; row_idx < row; row_idx++) {
        if (m_match[row_idx] == true) {
            // only need outer.
            for (i = 0; i < m_outerBatch->m_cols; i++) {
                p_vector = &m_outerBatch->m_arr[i];
                p_vector->m_vals[result_row] = batch->m_arr[i].m_vals[row_idx];
                p_vector->m_flag[result_row] = batch->m_arr[i].m_flag[row_idx];
                p_vector->m_rows++;
            }

            for (i = 0; i < m_inQualBatch->m_cols; i++) {
                ScalarVector* rVector = &m_innerBatch->m_arr[i];

                rVector->m_vals[result_row] = cellPoint[row_idx]->m_val[i].val;
                rVector->m_flag[result_row] = cellPoint[row_idx]->m_val[i].flag;
                rVector->m_rows++;
            }
            result_row++;
        }
    }

    m_probeStatus = PROBE_FETCH;
    m_innerBatch->m_rows = result_row;
    m_outerBatch->m_rows = result_row;
    // null result
    if (m_outerBatch->m_rows != 0)
        return buildResult(m_innerBatch, m_outerBatch, false);  // already has check Qual
    else
        return NULL;
}

/*
 * rightSemiJoinT:
 * 	Implementation of hash right semi join in vector engine.
 *
 * @PARAM:
 * 	batch: IN, a batch to be joined with the Hash Table
 * @RETURN:
 * 	matched cells assembled in a batch
 *
 * Note:
 * 	1. In this type of function (rightSemiJoin- and rightAntiJoin-), an auxiliary array
 * 	(i.e. cellPoint) is used to track post-delete matched cells in hash table.
 * 	2. m_cacheLoc[] (index of the bucket where the outer cells match) is needed for
 * 	the join. Do not modify the content of the array when enter the function.
 * 	Becareful of probeFunction in Codegen Module:
 * 		a): flag position in hashCell (RIGHT_ANTI_JOIN and RIGHT_SEMI_JOIN)
 * 		b): set m_cacheLoc[] as well in HashJoinCodeGen_probeHashTable
 */
template <bool complicate_join_key, bool simple_key>
VectorBatch* HashJoinTbl::rightSemiJoinT(VectorBatch* batch)
{
    int i, row_idx;
    int k = m_cols - 1;
    int row = batch->m_rows;
    int result_row = 0;
    int last_build_idx = 0;
    hashVal* val = NULL;
    ScalarVector* p_vector = NULL;
    hashCell* p_cell = NULL;
    hashCell** p_data = m_hashTbl->m_data;

    Assert(k >= 0);
    while (m_doProbeData) {
        last_build_idx = 0;

        /* Before check match key, we need to consist that it is a new start */
        if (m_joinStateLog.restore == false) {
            if (complicate_join_key)
                matchComplicateKey(batch);
            else {
                for (i = 0; i < m_key; i++)
                    RuntimeBinding(m_matchKeyFunction, i)(&batch->m_arr[m_outKeyIdx[i]], row, m_keyIdx[i], i);
            }
        } else {
            /* continue the job until complete: since we return the result in Hash Table */
            m_joinStateLog.restore = false;
            last_build_idx = m_joinStateLog.lastBuildIdx;
        }

        /* get matched rows only once */
        for (row_idx = last_build_idx; row_idx < row; row_idx++) {
            /*
             * Because there might be two or more probe_rows stay on the same
             * bucket (i.e. m_cellCache[j] == m_cellCache[k] for j != k),
             * we have to check the flag (m_cellCache[]->m_val[k].val) to avoid
             * re-getting the same row in the bucket.
             */
            if (m_keyMatch[row_idx] && m_cellCache[row_idx]->m_val[k].val == 0) {
                val = m_cellCache[row_idx]->m_val;

                /* mark dirty rows: actually the cell in Hash Table */
                val[k].val = 1;

                /* need inner */
                for (i = 0; i < m_innerBatch->m_cols; i++) {
                    p_vector = &m_innerBatch->m_arr[i];
                    p_vector->m_vals[result_row] = val[i].val;
                    p_vector->m_flag[result_row] = val[i].flag;
                }

                /* need outer sometimes */
                for (i = 0; i < m_outerBatch->m_cols; i++) {
                    p_vector = &m_outerBatch->m_arr[i];
                    p_vector->m_vals[result_row] = batch->m_arr[i].m_vals[row_idx];
                    p_vector->m_flag[result_row] = batch->m_arr[i].m_flag[row_idx];
                }

                /* count result row */
                result_row++;

                /* We have to build result if current batch is full even if there are some cells not-joined */
                if (result_row == BatchMaxSize) {
                    m_innerBatch->FixRowCount(BatchMaxSize);
                    m_outerBatch->FixRowCount(BatchMaxSize);
                    m_joinStateLog.lastBuildIdx = row_idx + 1;
                    m_joinStateLog.restore = true;
                    return buildResult(m_innerBatch, m_outerBatch, true);
                }
            }
        }

        /*
         * Two jobs:
         * 1. delete/mark dirty rows in Hash Table
         * 2. move onto next series of inner cells
         */
        m_doProbeData = false;
        for (row_idx = 0; row_idx < row; row_idx++) {
            /* Just reset keyMatch state for next join */
            m_keyMatch[row_idx] = true;

            /* If current outer cell matches nothing, just goto next cell */
            if ((p_cell = m_cellCache[row_idx]) == NULL)
                continue;

            /*
             * If the bucket head is unmatched, there are two scenarios:
             * a. we do the join with the head just now
             * b. we do the join with the cell behind the head just now
             */
            if (p_data[m_cacheLoc[row_idx]]->m_val[k].val == 0) {
                /*
                 * current outer cell does not match, no need to delete any cells and
                 * just remember the lastest unmacthed cell in the bucket
                 */
                if (p_cell->m_val[k].val == 0)
                    cellPoint[row_idx] = p_cell;
                /*
                 * current outer cell match a cell in the bucket (not its head),
                 * and we delete the matched cell in Hash Table
                 * Note: deleted cell are still saved in HashContext, it will not be freed until
                 * the HashContext is released.
                 */
                else
                    cellPoint[row_idx]->flag.m_next = p_cell->flag.m_next;
            } else {
                /* bucket head match: mark it and delete it later */
                m_match[row_idx] = true;
            }

            /* move onto next */
            m_cellCache[row_idx] = p_cell->flag.m_next;
            if (m_doProbeData == false && m_cellCache[row_idx])
                m_doProbeData = true;
        }

        /* delete the matched bucket head */
        for (row_idx = 0; row_idx < row; row_idx++) {
            /*
             * For a given bucket[m], there are following scenarios:
             *
             * - (both unmatch: bucket[m] is marked unmatch)
             * 	m_cellCache[i]       ->    bucket[m]  -- unmatch
             * 	m_cellCache[j]       ->    bucket[m]  -- unmatch
             *
             * - (both match: bucket[m] is marked match)
             * 	m_cellCache[i]       ->    bucket[m]  -- match
             * 	m_cellCache[j]       ->    bucket[m]  -- match
             *
             * - (match and unmatch: bucket[m] is marked match)
             * 	m_cellCache[i]       ->    bucket[m]  -- match
             * 	m_cellCache[j]       ->    bucket[m]  -- unmatch
             *
             * - (only one: bucket[m] is marked match)
             * 	m_cellCache[i]       ->    bucket[m]  -- match
             *
             * - (only one: bucket[m] is marked unmatch)
             * 	m_cellCache[i]       ->    bucket[m]  -- unmatch
             */
            if (m_match[row_idx]) {
                i = m_cacheLoc[row_idx];
                /* avoid delete more than once */
                if (p_data[i] && p_data[i]->m_val[k].val != 0)
                    p_data[i] = p_data[i]->flag.m_next;
                m_match[row_idx] = false;
            }
        }
    }

    /* result row will never exceed batch max size */
    Assert(result_row <= BatchMaxSize);

    m_probeStatus = PROBE_FETCH;
    m_innerBatch->FixRowCount(result_row);
    m_outerBatch->FixRowCount(result_row);

    /* build result */
    if (m_innerBatch->m_rows != 0)
        return buildResult(m_innerBatch, m_outerBatch, true);
    else
        return NULL;
}

/*
 * rightSemiJoinWithQualT:
 * 	Implementation of hash right semi join with qual in vector engine.
 *
 * @PARAM:
 * 	batch: IN, a batch to be joined with the Hash Table
 * @RETURN:
 * 	matched cells assembled in a batch
 *
 * Note:
 * 	1. In this type of function (rightSemiJoin- and rightAntiJoin-), an auxiliary array
 * 	(i.e. cellPoint) is used to track post-delete matched cells in hash table.
 * 	2. m_cacheLoc[] (index of the bucket where the outer cells match) is needed for
 * 	the join. Do not modify the content of the array when enter the function.
 * 	Becareful of probeFunction in Codegen Module:
 * 		a): flag position in hashCell (RIGHT_ANTI_JOIN and RIGHT_SEMI_JOIN)
 * 		b): set m_cacheLoc[] as well in HashJoinCodeGen_probeHashTable
 */
template <bool complicate_join_key, bool simple_key>
VectorBatch* HashJoinTbl::rightSemiJoinWithQualT(VectorBatch* batch)
{
    int i, row_idx;
    int k = m_cols - 1;
    int row = batch->m_rows;
    int result_qual_row = 0;
    int result_row = 0;
    hashVal* val = NULL;
    ScalarVector* p_vector = NULL;
    bool* sel = NULL;
    int org_idx, last_build_idx;
    hashCell* p_cell = NULL;
    hashCell** p_data = m_hashTbl->m_data;

    Assert(k >= 0);
    while (m_doProbeData) {
        last_build_idx = 0;

        /* Before check match key, we need to consist that it is a new start */
        if (m_joinStateLog.restore == false) {
            if (complicate_join_key)
                matchComplicateKey(batch);
            else {
                for (i = 0; i < m_key; i++)
                    RuntimeBinding(m_matchKeyFunction, i)(&batch->m_arr[m_outKeyIdx[i]], row, m_keyIdx[i], i);
            }

            for (row_idx = 0; row_idx < row; row_idx++) {
                /*
                 * check roughly and get matched inner and outer rows.
                 * In fact, if m_keyMatch[] == true, then
                 * m_cellCache[]->m_val[k].val == 0 without a doubt.
                 */
                if (m_keyMatch[row_idx]) {
                    val = m_cellCache[row_idx]->m_val;

                    /* we need both inner and outer batch for later compare */
                    for (i = 0; i < m_inQualBatch->m_cols; i++) {
                        p_vector = &m_inQualBatch->m_arr[i];
                        p_vector->m_vals[result_qual_row] = val[i].val;
                        p_vector->m_flag[result_qual_row] = val[i].flag;
                    }
                    for (i = 0; i < m_outQualBatch->m_cols; i++) {
                        p_vector = &m_outQualBatch->m_arr[i];
                        p_vector->m_vals[result_qual_row] = batch->m_arr[i].m_vals[row_idx];
                        p_vector->m_flag[result_qual_row] = batch->m_arr[i].m_flag[row_idx];
                    }
                    m_reCheckCell[result_qual_row++].oriIdx = row_idx;
                }
            }

            /* reCheck quals */
            if (result_qual_row > 0) {
                m_inQualBatch->FixRowCount(result_qual_row);
                m_outQualBatch->FixRowCount(result_qual_row);

                /*
                 * check qual: more precise comparsion and get
                 * > dirty rows: matched
                 * > unmatched
                 */
                sel = checkQual(m_inQualBatch, m_outQualBatch);
                for (i = 0; i < result_qual_row; i++) {
                    org_idx = m_reCheckCell[i].oriIdx;
                    if (sel[i] == true)
                        m_cellCache[org_idx]->m_val[k].val = 1; /* dirty rows */
                    else
                        m_keyMatch[org_idx] = false; /* unmatched */
                }

                /* reset result_qual_row for next loop */
                result_qual_row = 0;
            }
        } else {
            /* continue the job until complete: since we return the result in Hash Table */
            m_joinStateLog.restore = false;
            last_build_idx = m_joinStateLog.lastBuildIdx;
        }

        /* get matched rows only once */
        for (row_idx = last_build_idx; row_idx < row; row_idx++) {
            if (m_keyMatch[row_idx] && m_cellCache[row_idx]->m_val[k].val == 1) {
                val = m_cellCache[row_idx]->m_val;
                val[k].val = 2; /* re-dirty: same meanings when it > 1 */

                /* need inner */
                for (i = 0; i < m_innerBatch->m_cols; i++) {
                    p_vector = &m_innerBatch->m_arr[i];
                    p_vector->m_vals[result_row] = val[i].val;
                    p_vector->m_flag[result_row] = val[i].flag;
                }

                /* need outer sometimes */
                for (i = 0; i < m_outerBatch->m_cols; i++) {
                    p_vector = &m_outerBatch->m_arr[i];
                    p_vector->m_vals[result_row] = batch->m_arr[i].m_vals[row_idx];
                    p_vector->m_flag[result_row] = batch->m_arr[i].m_flag[row_idx];
                }

                /* count result row */
                result_row++;

                /* We have to build result if current batch is full even if there are some cells not-joined */
                if (result_row == BatchMaxSize) {
                    m_innerBatch->FixRowCount(BatchMaxSize);
                    m_outerBatch->FixRowCount(BatchMaxSize);
                    m_joinStateLog.lastBuildIdx = row_idx + 1;
                    m_joinStateLog.restore = true;
                    return buildResult(m_innerBatch, m_outerBatch, false);
                }
            }
        }

        /* do the next and delete dirty(include re-dirty) rows from the Hash table */
        m_doProbeData = false;
        for (row_idx = 0; row_idx < row; row_idx++) {
            /* this is a reset for next loop */
            m_keyMatch[row_idx] = true;

            /* just return if outer cell matches nothing */
            if (m_cellCache[row_idx] == NULL)
                continue;

            p_cell = m_cellCache[row_idx];

            /*
             * If the bucket head is unmatched, there are two scenarios:
             * a. do the join with the head just now
             * b. do the join with the cell behind the head just now
             */
            if (p_data[m_cacheLoc[row_idx]]->m_val[k].val == 0) {
                /*
                 * current outer cell does not match, no need to delete any cells and
                 * just remember the lastest unmacthed cell in the bucket
                 */
                if (p_cell->m_val[k].val == 0)
                    cellPoint[row_idx] = p_cell;
                /*
                 * current outer cell match a cell in the bucket (not its head),
                 * and we delete the matched cell in Hash Table
                 * Note: deleted cell are still saved in HashContext, it will not be freed until
                 * the HashContext is released.
                 */
                else
                    cellPoint[row_idx]->flag.m_next = p_cell->flag.m_next;
            } else {
                /* bucket head match: mark it and delete it later */
                m_match[row_idx] = true;
            }

            /* move onto next series */
            m_cellCache[row_idx] = p_cell->flag.m_next;
            if (m_doProbeData == false && m_cellCache[row_idx])
                m_doProbeData = true;
        }

        /*
         * delete the matched bucket head
         * Note: we just delete once
         */
        for (row_idx = 0; row_idx < row; row_idx++) {
            if (m_match[row_idx]) {
                i = m_cacheLoc[row_idx];
                /* avoid delete many times */
                if (p_data[i] && p_data[i]->m_val[k].val != 0)
                    p_data[i] = p_data[i]->flag.m_next;
                m_match[row_idx] = false;
            }
        }
    }

    /* result row will never exceed batch max size */
    Assert(result_row <= BatchMaxSize);

    m_probeStatus = PROBE_FETCH;
    m_innerBatch->FixRowCount(result_row);
    m_outerBatch->FixRowCount(result_row);

    /* build result */
    if (m_outerBatch->m_rows != 0)
        return buildResult(m_innerBatch, m_outerBatch, false);
    else
        return NULL;
}

template <bool complicate_join_key, bool simple_key>
VectorBatch* HashJoinTbl::antiJoinT(VectorBatch* batch)
{
    int i, row_idx;
    int row = batch->m_rows;
    int result_row = m_outerBatch->m_rows;
    ScalarVector* p_vector = NULL;

    while (m_doProbeData) {
        if (complicate_join_key)
            matchComplicateKey(batch);
        else {
            for (i = 0; i < m_key; i++)
                RuntimeBinding(m_matchKeyFunction, i)(&batch->m_arr[m_outKeyIdx[i]], row, m_keyIdx[i], i);
        }

        // do the next
        m_doProbeData = false;
        for (row_idx = 0; row_idx < row; row_idx++) {
            if (m_keyMatch[row_idx] == true) { // no need further compare
                m_match[row_idx] = true;
                m_cellCache[row_idx] = NULL;
            } else {
                if (m_cellCache[row_idx] != NULL)
                    m_cellCache[row_idx] = m_cellCache[row_idx]->flag.m_next;

                if (m_cellCache[row_idx])
                    m_doProbeData = true;

                m_keyMatch[row_idx] = true;  // reset the key match
            }
        }
    }

    Assert(result_row == 0);

    for (row_idx = 0; row_idx < row; row_idx++) {
        if (m_match[row_idx] == false) {
            // only need outer.
            for (i = 0; i < m_outerBatch->m_cols; i++) {
                p_vector = &m_outerBatch->m_arr[i];
                p_vector->m_vals[result_row] = batch->m_arr[i].m_vals[row_idx];
                p_vector->m_flag[result_row] = batch->m_arr[i].m_flag[row_idx];
            }

            for (i = 0; i < m_innerBatch->m_cols; i++) {
                p_vector = &m_innerBatch->m_arr[i];
                SET_NULL(p_vector->m_flag[result_row]);
            }
            result_row++;
        }
    }

    m_probeStatus = PROBE_FETCH;
    m_innerBatch->FixRowCount(result_row);
    m_outerBatch->FixRowCount(result_row);
    if (m_outerBatch->m_rows != 0)
        return buildResult(m_innerBatch, m_outerBatch, false);
    else
        return NULL;
}

template <bool complicate_join_key, bool simple_key>
VectorBatch* HashJoinTbl::antiJoinWithQualT(VectorBatch* batch)
{
    int i, row_idx;
    int row = batch->m_rows;
    int result_row = m_outerBatch->m_rows;
    int result_qual_row = m_inQualBatch->m_rows;
    hashVal* val = NULL;
    ScalarVector* p_vector = NULL;
    bool* sel = NULL;
    int org_idx;

    Assert(result_qual_row == 0);
    while (m_doProbeData) {
        if (complicate_join_key)
            matchComplicateKey(batch);
        else {
            for (i = 0; i < m_key; i++)
                RuntimeBinding(m_matchKeyFunction, i)(&batch->m_arr[m_outKeyIdx[i]], row, m_keyIdx[i], i);
        }

        m_doProbeData = false;
        // do the next
        for (row_idx = 0; row_idx < row; row_idx++) {
            if (m_keyMatch[row_idx] == true) {
                // need further compare
                val = m_cellCache[row_idx]->m_val;

                m_reCheckCell[result_qual_row].oriIdx = row_idx;

                // need inner
                for (i = 0; i < m_inQualBatch->m_cols; i++) {
                    p_vector = &m_inQualBatch->m_arr[i];

                    p_vector->m_vals[result_qual_row] = val[i].val;
                    p_vector->m_flag[result_qual_row] = val[i].flag;
                    p_vector->m_rows++;
                }

                // need outer.
                for (i = 0; i < m_outQualBatch->m_cols; i++) {
                    p_vector = &m_outQualBatch->m_arr[i];
                    p_vector->m_vals[result_qual_row] = batch->m_arr[i].m_vals[row_idx];
                    p_vector->m_flag[result_qual_row] = batch->m_arr[i].m_flag[row_idx];
                    p_vector->m_rows++;
                }

                result_qual_row++;
            } else {
                if (m_cellCache[row_idx] != NULL)
                    m_cellCache[row_idx] = m_cellCache[row_idx]->flag.m_next;

                if (m_cellCache[row_idx])
                    m_doProbeData = true;

                m_keyMatch[row_idx] = true;  // reset the key match
            }
        }

        // check the match item, it still has chance to fail on qual.
        if (result_qual_row > 0) {
            m_inQualBatch->m_rows += result_qual_row;
            m_outQualBatch->m_rows += result_qual_row;
            sel = checkQual(m_inQualBatch, m_outQualBatch);
            for (i = 0; i < result_qual_row; i++) {
                org_idx = m_reCheckCell[i].oriIdx;
                // qual still pass, we can safely pass it
                if (sel[i] == true) {
                    m_match[org_idx] = true;
                    m_cellCache[org_idx] = NULL;
                } else {
                    if (m_cellCache[org_idx] != NULL)
                        m_cellCache[org_idx] = m_cellCache[org_idx]->flag.m_next;

                    if (m_cellCache[org_idx])
                        m_doProbeData = true;
                }
            }

            result_qual_row = 0;
        }
    }

    Assert(result_row == 0);
    for (row_idx = 0; row_idx < row; row_idx++) {
        if (m_match[row_idx] == false) {
            // only need outer.
            for (i = 0; i < m_outerBatch->m_cols; i++) {
                p_vector = &m_outerBatch->m_arr[i];
                p_vector->m_vals[result_row] = batch->m_arr[i].m_vals[row_idx];
                p_vector->m_flag[result_row] = batch->m_arr[i].m_flag[row_idx];
                p_vector->m_rows++;
            }

            for (i = 0; i < m_innerBatch->m_cols; i++) {
                p_vector = &m_innerBatch->m_arr[i];
                SET_NULL(p_vector->m_flag[result_row]);
                p_vector->m_rows++;
            }
            result_row++;
        }
    }

    m_probeStatus = PROBE_FETCH;
    m_innerBatch->m_rows = result_row;
    m_outerBatch->m_rows = result_row;
    if (m_outerBatch->m_rows == 0)
        return NULL;
    else
        return buildResult(m_innerBatch, m_outerBatch, false);
}

/*
 * rightAntiJoinT:
 * 	Implementation of hash right anti join in vector engine.
 *
 * @PARAM:
 * 	batch: IN, a batch to be joined with the Hash Table
 * @RETURN:
 * 	NULL
 *
 * Note:
 * 	1. In this type of function (rightSemiJoin- and rightAntiJoin-), an auxiliary array
 * 	(i.e. cellPoint) is used to track post-delete matched cells in hash table.
 * 	2. m_cacheLoc[] (index of the bucket where the outer cells match) is needed for
 * 	the join. Do not modify the content of the array when enter the function.
 * 	Becareful of probeFunction in Codegen Module:
 * 		a): flag position in hashCell (RIGHT_ANTI_JOIN and RIGHT_SEMI_JOIN)
 * 		b): set m_cacheLoc[] as well in HashJoinCodeGen_probeHashTable
 */
template <bool complicate_join_key, bool simple_key>
VectorBatch* HashJoinTbl::rightAntiJoinT(VectorBatch* batch)
{
    int i, row_idx;
    int k = m_cols - 1;
    int row = batch->m_rows;
    hashCell* p_cell = NULL;
    hashCell** p_data = m_hashTbl->m_data;

    Assert(k >= 0);
    while (m_doProbeData) {
        if (complicate_join_key)
            matchComplicateKey(batch);
        else {
            for (i = 0; i < m_key; i++)
                RuntimeBinding(m_matchKeyFunction, i)(&batch->m_arr[m_outKeyIdx[i]], row, m_keyIdx[i], i);
        }

        /* mark dirty rows */
        for (row_idx = 0; row_idx < row; row_idx++) {
            if (m_keyMatch[row_idx])
                m_cellCache[row_idx]->m_val[k].val = 1;
        }

        /* do the next and delete dirty rows from the Hash table */
        m_doProbeData = false;
        for (row_idx = 0; row_idx < row; row_idx++) {
            m_keyMatch[row_idx] = true;
            if (m_cellCache[row_idx] == NULL)
                continue;

            p_cell = m_cellCache[row_idx];

            /* delete matched cells */
            if (p_data[m_cacheLoc[row_idx]]->m_val[k].val == 0) {
                if (p_cell->m_val[k].val == 0)
                    cellPoint[row_idx] = p_cell;
                else
                    cellPoint[row_idx]->flag.m_next = p_cell->flag.m_next;
            } else {
                /* do the anti join with bucket head just now (they match), mark it */
                m_match[row_idx] = true;
            }

            m_cellCache[row_idx] = p_cell->flag.m_next;
            if (m_doProbeData == false && m_cellCache[row_idx])
                m_doProbeData = true;
        }
        for (row_idx = 0; row_idx < row; row_idx++) {
            if (m_match[row_idx]) {
                i = m_cacheLoc[row_idx];
                if (p_data[i] && p_data[i]->m_val[k].val != 0)
                    p_data[i] = p_data[i]->flag.m_next;
                m_match[row_idx] = false;
            }
        }
    }

    m_probeStatus = PROBE_FETCH;

    /* In right Anti Join, we return results in function endJoin not here */
    return NULL;
}

/*
 * rightAntiJoinWithQualT:
 * 	Implementation of hash right anti join with qual in vector engine.
 *
 * @PARAM:
 * 	batch: IN, a batch to be joined with the Hash Table
 * @RETURN:
 * 	NULL
 *
 * Note:
 * 	1. In this type of function (rightSemiJoin- and rightAntiJoin-), an auxiliary array
 * 	(i.e. cellPoint) is used to track post-delete matched cells in hash table.
 * 	2. m_cacheLoc[] (index of the bucket where the outer cells match) is needed for
 * 	the join. Do not modify the content of the array when enter the function.
 * 	Becareful of probeFunction in Codegen Module:
 * 		a): flag position in hashCell (RIGHT_ANTI_JOIN and RIGHT_SEMI_JOIN)
 * 		b): set m_cacheLoc[] as well in HashJoinCodeGen_probeHashTable
 */
template <bool complicate_join_key, bool simple_key>
VectorBatch* HashJoinTbl::rightAntiJoinWithQualT(VectorBatch* batch)
{
    int i, row_idx;
    int k = m_cols - 1;
    int row = batch->m_rows;
    int result_qual_row = 0;
    hashVal* val = NULL;
    ScalarVector* p_vector = NULL;
    bool* sel = NULL;
    hashCell* p_cell = NULL;
    hashCell** p_data = m_hashTbl->m_data;

    Assert(k >= 0);
    while (m_doProbeData) {
        if (complicate_join_key)
            matchComplicateKey(batch);
        else {
            for (i = 0; i < m_key; i++)
                RuntimeBinding(m_matchKeyFunction, i)(&batch->m_arr[m_outKeyIdx[i]], row, m_keyIdx[i], i);
        }

        /* check roughly */
        for (row_idx = 0; row_idx < row; row_idx++) {
            /*
             * we do not check the flag (in m_cellCache[]->m_val[k].val) here,
             * because any possibilities that probe_row matches rows in HashTbl
             * should be counted in.
             */
            if (m_keyMatch[row_idx]) {
                val = m_cellCache[row_idx]->m_val;
                for (i = 0; i < m_inQualBatch->m_cols; i++) {
                    p_vector = &m_inQualBatch->m_arr[i];
                    p_vector->m_vals[result_qual_row] = val[i].val;
                    p_vector->m_flag[result_qual_row] = val[i].flag;
                }
                for (i = 0; i < m_outQualBatch->m_cols; i++) {
                    p_vector = &m_outQualBatch->m_arr[i];
                    p_vector->m_vals[result_qual_row] = batch->m_arr[i].m_vals[row_idx];
                    p_vector->m_flag[result_qual_row] = batch->m_arr[i].m_flag[row_idx];
                }

                m_reCheckCell[result_qual_row++].oriIdx = row_idx;
            }
        }

        /* reCheck quals and mark dirty rows */
        if (result_qual_row > 0) {
            m_inQualBatch->FixRowCount(result_qual_row);
            m_outQualBatch->FixRowCount(result_qual_row);
            sel = checkQual(m_inQualBatch, m_outQualBatch);
            for (i = 0; i < result_qual_row; i++) {
                if (sel[i])
                    m_cellCache[m_reCheckCell[i].oriIdx]->m_val[k].val = 1;
            }

            result_qual_row = 0;
        }

        /* do the next and delete dirty rows from the Hash table */
        m_doProbeData = false;
        for (row_idx = 0; row_idx < row; row_idx++) {
            m_keyMatch[row_idx] = true;
            if (m_cellCache[row_idx] == NULL)
                continue;

            p_cell = m_cellCache[row_idx];

            /* delete matched cells */
            if (p_data[m_cacheLoc[row_idx]]->m_val[k].val == 0) {
                if (p_cell->m_val[k].val == 0)
                    cellPoint[row_idx] = p_cell;
                else
                    cellPoint[row_idx]->flag.m_next = p_cell->flag.m_next;
            } else {
                /* do the anti join with bucket head just now (they match), mark it */
                m_match[row_idx] = true;
            }

            m_cellCache[row_idx] = p_cell->flag.m_next;
            if (m_doProbeData == false && m_cellCache[row_idx])
                m_doProbeData = true;
        }
        for (row_idx = 0; row_idx < row; row_idx++) {
            if (m_match[row_idx]) {
                i = m_cacheLoc[row_idx];
                if (p_data[i] && p_data[i]->m_val[k].val != 0)
                    p_data[i] = p_data[i]->flag.m_next;
                m_match[row_idx] = false;
            }
        }
    }

    m_probeStatus = PROBE_FETCH;

    /* In right Anti Join, we return results in function endJoin not here */
    return NULL;
}

template <bool complicate_join_key, bool simple_key>
VectorBatch* HashJoinTbl::rightJoinT(VectorBatch* batch)
{
    int i, row_idx;
    int row = batch->m_rows;
    int result_row = m_outerBatch->m_rows;
    hashVal* val = NULL;
    int last_build_idx = 0;
    ScalarVector* p_vector = NULL;

    while (m_doProbeData) {
        last_build_idx = 0;

        if (m_joinStateLog.restore == false) {
            if (complicate_join_key)
                matchComplicateKey(batch);
            else {
                for (i = 0; i < m_key; i++)
                    RuntimeBinding(m_matchKeyFunction, i)(&batch->m_arr[m_outKeyIdx[i]], row, m_keyIdx[i], i);
            }
        } else {
            m_joinStateLog.restore = false;
            last_build_idx = m_joinStateLog.lastBuildIdx;
        }

        for (row_idx = last_build_idx; row_idx < row; row_idx++) {
            if (m_keyMatch[row_idx]) {
                val = m_cellCache[row_idx]->m_val;

                m_cellCache[row_idx]->m_val[m_cols - 1].val = 1;  // set the flag

                for (i = 0; i < m_innerBatch->m_cols; i++) {
                    p_vector = &m_innerBatch->m_arr[i];

                    p_vector->m_vals[result_row] = val[i].val;
                    p_vector->m_flag[result_row] = val[i].flag;
                }

                for (i = 0; i < m_outerBatch->m_cols; i++) {
                    p_vector = &m_outerBatch->m_arr[i];
                    p_vector->m_vals[result_row] = batch->m_arr[i].m_vals[row_idx];
                    p_vector->m_flag[result_row] = batch->m_arr[i].m_flag[row_idx];
                }
                result_row++;
            }

            if (result_row == BatchMaxSize) {
                m_innerBatch->FixRowCount(BatchMaxSize);
                m_outerBatch->FixRowCount(BatchMaxSize);
                m_joinStateLog.lastBuildIdx = row_idx + 1;
                m_joinStateLog.restore = true;
                return buildResult(m_innerBatch, m_outerBatch, false);  // has no joinqual
            }
        }

        // do the next
        m_doProbeData = false;
        for (row_idx = 0; row_idx < row; row_idx++) {
            m_keyMatch[row_idx] = true;  // reset the key match
            if (m_cellCache[row_idx] != NULL)
                m_cellCache[row_idx] = m_cellCache[row_idx]->flag.m_next;

            if (m_cellCache[row_idx] != NULL)
                m_doProbeData = true;
        }
    }

    m_probeStatus = PROBE_FETCH;
    m_innerBatch->FixRowCount(result_row);
    m_outerBatch->FixRowCount(result_row);
    // null result
    if (m_innerBatch->m_rows == 0)
        return NULL;
    else
        return buildResult(m_innerBatch, m_outerBatch, false);  // has no joinqual
}

template <bool complicate_join_key, bool simple_key>
VectorBatch* HashJoinTbl::rightJoinWithQualT(VectorBatch* batch)
{
    int i, row_idx;
    int row = batch->m_rows;
    int result_row = m_outerBatch->m_rows;
    int result_qual_row = m_outQualBatch->m_rows;
    hashVal* val = NULL;
    int last_build_idx = 0;
    ScalarVector* p_vector = NULL;
    bool* sel = NULL;
    int org_idx = 0;

    while (m_doProbeData) {
        last_build_idx = 0;

        if (m_joinStateLog.restore == false) {
            if (complicate_join_key)
                matchComplicateKey(batch);
            else {
                for (i = 0; i < m_key; i++)
                    RuntimeBinding(m_matchKeyFunction, i)(&batch->m_arr[m_outKeyIdx[i]], row, m_keyIdx[i], i);
            }

            for (row_idx = 0; row_idx < row; row_idx++) {
                if (m_keyMatch[row_idx]) {
                    val = m_cellCache[row_idx]->m_val;

                    for (i = 0; i < m_inQualBatch->m_cols; i++) {
                        p_vector = &m_inQualBatch->m_arr[i];

                        p_vector->m_vals[result_qual_row] = val[i].val;
                        p_vector->m_flag[result_qual_row] = val[i].flag;
                        p_vector->m_rows++;
                    }

                    for (i = 0; i < m_outQualBatch->m_cols; i++) {
                        p_vector = &m_outQualBatch->m_arr[i];
                        p_vector->m_vals[result_qual_row] = batch->m_arr[i].m_vals[row_idx];
                        p_vector->m_flag[result_qual_row] = batch->m_arr[i].m_flag[row_idx];
                        p_vector->m_rows++;
                    }
                    m_reCheckCell[result_qual_row].oriIdx = row_idx;
                    result_qual_row++;
                }
            }

            // we need to see if these rows are truely pass
            if (result_qual_row > 0) {
                m_inQualBatch->m_rows += result_qual_row;
                m_outQualBatch->m_rows += result_qual_row;
                sel = checkQual(m_inQualBatch, m_outQualBatch);
                for (i = 0; i < result_qual_row; i++) {
                    org_idx = m_reCheckCell[i].oriIdx;
                    if (sel[i] == true) {
                        m_cellCache[org_idx]->m_val[m_cols - 1].val = 1;  // set the flag
                    } else {
                        m_keyMatch[org_idx] = false;
                    }
                }
                result_qual_row = 0;
            }
        } else {
            m_joinStateLog.restore = false;
            last_build_idx = m_joinStateLog.lastBuildIdx;
        }

        for (row_idx = last_build_idx; row_idx < row; row_idx++) {
            if (m_keyMatch[row_idx]) {
                val = m_cellCache[row_idx]->m_val;

                for (i = 0; i < m_innerBatch->m_cols; i++) {
                    p_vector = &m_innerBatch->m_arr[i];

                    p_vector->m_vals[result_row] = val[i].val;
                    p_vector->m_flag[result_row] = val[i].flag;
                    p_vector->m_rows++;
                }

                for (i = 0; i < m_outerBatch->m_cols; i++) {
                    p_vector = &m_outerBatch->m_arr[i];
                    p_vector->m_vals[result_row] = batch->m_arr[i].m_vals[row_idx];
                    p_vector->m_flag[result_row] = batch->m_arr[i].m_flag[row_idx];
                    p_vector->m_rows++;
                }
                result_row++;
            }

            if (result_row == BatchMaxSize) {
                m_innerBatch->m_rows = BatchMaxSize;
                m_outerBatch->m_rows = BatchMaxSize;
                m_joinStateLog.lastBuildIdx = row_idx + 1;
                m_joinStateLog.restore = true;
                return buildResult(m_innerBatch, m_outerBatch, false);
            }
        }

        // do the next
        m_doProbeData = false;
        for (row_idx = 0; row_idx < row; row_idx++) {
            m_keyMatch[row_idx] = true;  // reset the key match
            if (m_cellCache[row_idx] != NULL)
                m_cellCache[row_idx] = m_cellCache[row_idx]->flag.m_next;

            if (m_cellCache[row_idx] != NULL)
                m_doProbeData = true;
        }
    }

    m_probeStatus = PROBE_FETCH;
    m_innerBatch->m_rows = result_row;
    m_outerBatch->m_rows = result_row;
    // null result
    if (m_innerBatch->m_rows == 0)
        return NULL;
    else
        return buildResult(m_innerBatch, m_outerBatch, false);
}

bool* HashJoinTbl::checkQual(VectorBatch* in_batch, VectorBatch* out_batch)
{
    ExprContext* econtext = NULL;

    DBG_ASSERT(in_batch->m_rows == out_batch->m_rows);

    m_result->Reset(true);
    m_result->m_rows = in_batch->m_rows;
    if (m_runtime->js.ps.ps_ProjInfo) {
        econtext = m_runtime->js.ps.ps_ProjInfo->pi_exprContext;
    } else {
        // for count(*) we only need is the row num.
        econtext = m_runtime->js.ps.ps_ExprContext;
    }

    initEcontextBatch(NULL, out_batch, in_batch, NULL);
    econtext->ecxt_scanbatch = m_result;

    /* Mark if hashjoin has been LLVM optimized */
    if (m_runtime->jitted_joinqual) {
        if (HAS_INSTR(&m_runtime->js, false)) {
            m_runtime->js.ps.instrument->isLlvmOpt = true;
        }
    }
    /* Use LLVM optimization if machine code is generated */
    if (m_runtime->jitted_joinqual) {
        (void)m_runtime->jitted_joinqual(econtext);
    } else {
        (void)ExecVecQual(m_runtime->js.joinqual, econtext, false);
    }

    in_batch->Reset();
    out_batch->Reset();

    return econtext->ecxt_scanbatch->m_sel;
}

VectorBatch* HashJoinTbl::buildResult(VectorBatch* in_batch, VectorBatch* out_batch, bool check_qual)
{
    ExprContext* econtext = NULL;
    VectorBatch* res_batch = NULL;
    ScalarVector* p_vector = NULL;
    bool has_qual = false;

    DBG_ASSERT(in_batch->m_rows == out_batch->m_rows);
    m_result->Reset(true);
    m_result->m_rows = in_batch->m_rows;

    ResetExprContext(m_runtime->js.ps.ps_ExprContext);
    if (m_runtime->js.ps.ps_ProjInfo) {
        econtext = m_runtime->js.ps.ps_ProjInfo->pi_exprContext;
    } else {
        // for count(*) we only need is the row num.
        econtext = m_runtime->js.ps.ps_ExprContext;
    }

    initEcontextBatch(NULL, out_batch, in_batch, NULL);

    if (check_qual && m_runtime->js.joinqual != NULL) {
        has_qual = true;
        /* Mark if hashjoin has been LLVM optimized */
        if (m_runtime->jitted_joinqual) {
            if (HAS_INSTR(&m_runtime->js, false)) {
                m_runtime->js.ps.instrument->isLlvmOpt = true;
            }
        }

        econtext->ecxt_scanbatch = m_result;

        /* Use LLVM optimization if machine code is generated */
        if (m_runtime->jitted_joinqual)
            p_vector = m_runtime->jitted_joinqual(econtext);
        else
            p_vector = ExecVecQual(m_runtime->js.joinqual, econtext, false);

        if (p_vector == NULL) {
            in_batch->Reset();
            out_batch->Reset();
            return NULL;
        }
    }

    if (m_runtime->js.ps.qual != NULL) {
        has_qual = true;
        econtext->ecxt_scanbatch = m_result;
        p_vector = ExecVecQual(m_runtime->js.ps.qual, econtext, false);

        if (p_vector == NULL) {
            in_batch->Reset();
            out_batch->Reset();
            return NULL;
        }
    }

    if (has_qual) {
        in_batch->PackT<true, false>(econtext->ecxt_scanbatch->m_sel);
        out_batch->PackT<true, false>(econtext->ecxt_scanbatch->m_sel);
    }

    if (m_runtime->js.ps.ps_ProjInfo) {
        initEcontextBatch(NULL, out_batch, in_batch, NULL);
        res_batch = ExecVecProject(m_runtime->js.ps.ps_ProjInfo);
        if (res_batch->m_rows != in_batch->m_rows) {
            res_batch->FixRowCount(in_batch->m_rows);
        }
    } else {
        m_result->m_rows = out_batch->m_rows;
        m_result->m_arr[0].m_rows = out_batch->m_rows;
        res_batch = m_result;
    }

    in_batch->Reset();
    out_batch->Reset();

    return res_batch;
}

void HashJoinTbl::matchComplicateKey(VectorBatch* batch)
{
    int i, j;
    int rows = batch->m_rows;
    hashVal* val = NULL;
    ScalarVector* p_vector = NULL;
    int result_row = 0;
    ExprContext* econtext = m_runtime->js.ps.ps_ExprContext;

    ResetExprContext(econtext);

    m_complicate_innerBatch->Reset();
    m_complicate_outerBatch->Reset();

    for (i = 0; i < rows; i++) {
        if (m_cellCache[i]) {
            val = m_cellCache[i]->m_val;

            for (j = 0; j < m_complicate_innerBatch->m_cols; j++) {
                p_vector = &m_complicate_innerBatch->m_arr[j];
                p_vector->m_vals[result_row] = val[j].val;
                p_vector->m_flag[result_row] = val[j].flag;
            }

            for (j = 0; j < m_complicate_outerBatch->m_cols; j++) {
                p_vector = &m_complicate_outerBatch->m_arr[j];
                p_vector->m_vals[result_row] = batch->m_arr[j].m_vals[i];
                p_vector->m_flag[result_row] = batch->m_arr[j].m_flag[i];
            }
            result_row++;
            m_keyMatch[i] = true;
        } else {
            m_keyMatch[i] = false;
        }
    }

    m_complicate_innerBatch->FixRowCount(result_row);
    m_complicate_outerBatch->FixRowCount(result_row);

    /*
     * If m_complicate_innerBatch and m_complicate_outerBatch are both NULL,
     * no need to calculate the expression clause, return directly here.
     */
    if (unlikely(result_row == 0)) {
        return;
    }

    initEcontextBatch(m_complicate_outerBatch, m_complicate_outerBatch, m_complicate_innerBatch, NULL);

    /*
     * Mark if hashjoin has been LLVM optimized : right row we could not mask
     * if joinqual has been codegened or hashclauses has been codegened.
     */
    if (m_runtime->jitted_hashclause) {
        if (HAS_INSTR(&m_runtime->js, false)) {
            m_runtime->js.ps.instrument->isLlvmOpt = true;
        }
    }

    if (m_runtime->jitted_hashclause)
        (void)m_runtime->jitted_hashclause(econtext);
    else {
        (void)ExecVecQual(m_runtime->hashclauses, econtext, false);

        /* for noneq_join */
        bool need_furtherCheck = false;
        if (m_runtime->js.nulleqqual != NULL) {
            /* no need to calc */
            for (i = 0; i < rows; i++) {
                if (m_complicate_outerBatch->m_sel[i])
                    m_nulleqmatch[i] = false;
                else {
                    m_nulleqmatch[i] = true;
                    need_furtherCheck = true;
                }
            }

            if (need_furtherCheck) {
                bool* tmpSel = &m_complicate_outerBatch->m_sel[0];
                m_complicate_outerBatch->m_sel = &m_nulleqmatch[0];
                (void)ExecVecQual(m_runtime->js.nulleqqual, econtext, false);

                /* restore the selection */
                m_complicate_outerBatch->m_sel = tmpSel;
                for (i = 0; i < rows; i++) {
                    m_complicate_outerBatch->m_sel[i] = m_complicate_outerBatch->m_sel[i] || m_nulleqmatch[i];
                }
            }
        }
    }

    j = 0;
    for (i = 0; i < rows; i++) {
        if (m_keyMatch[i]) {
            if (!m_complicate_outerBatch->m_sel[j])
                m_keyMatch[i] = false;
            j++;
        }
    }
}

bool HashJoinTbl::simpletype(Oid type_id)
{
    switch (type_id) {
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
            return true;
        default:
            break;
    }

    return false;
}

template <typename innerType, typename outerType, bool simpleType, bool nulleqnull>
void HashJoinTbl::matchKey(ScalarVector* val, int rows, int hashValKeyIdx, int key_num)
{
    ScalarValue* value = val->m_vals;
    uint8* flag = val->m_flag;
    int i;
    FunctionCallInfoData fc_info;
    PGFunction cmp_fun = m_eqfunctions[key_num].fn_addr;
    Datum args[2];

    fc_info.arg = &args[0];
    fc_info.flinfo = (m_eqfunctions + key_num);

    for (i = 0; i < rows; i++) {
        if (m_keyMatch[i] == true && m_cellCache[i] && NOT_NULL(m_cellCache[i]->m_val[hashValKeyIdx].flag)) {
            if (likely(NOT_NULL(flag[i]))) {
                if (simpleType) {
                    m_keyMatch[i] = (innerType)m_cellCache[i]->m_val[hashValKeyIdx].val == (outerType)value[i];
                } else {
                    fc_info.arg[0] = value[i];
                    fc_info.arg[1] = m_cellCache[i]->m_val[hashValKeyIdx].val;
                    m_keyMatch[i] = cmp_fun(&fc_info);
                }
            } else {
                m_keyMatch[i] = false;
            }
        } else if (nulleqnull && m_keyMatch[i] == true && IS_NULL(flag[i]) && m_cellCache[i] &&
                   IS_NULL(m_cellCache[i]->m_val[hashValKeyIdx].flag)) {
            m_keyMatch[i] = true;
        } else {
            m_keyMatch[i] = false;
        }
    }
}

void HashJoinTbl::CalcComplicateHashVal(VectorBatch* batch, List* hash_keys, bool inner)
{
    ExprContext* econtext = m_runtime->js.ps.ps_ExprContext;
    ListCell* hk = NULL;
    ScalarVector* results = NULL;
    int rows = batch->m_rows;
    bool first_enter = true;
    bool* p_selection = NULL;
    FmgrInfo* hash_functions = NULL;
    Datum key;
    ScalarValue hash_val;

    if (rows == 0)
        return;

    m_cjVector->m_rows = 0;
    if (inner) {
        initEcontextBatch(NULL, NULL, batch, NULL);
        hash_functions = m_innerHashFuncs;
    } else {
        initEcontextBatch(NULL, batch, NULL, NULL);
        hash_functions = m_outerHashFuncs;
    }
    p_selection = batch->m_sel;
    ResetExprContext(econtext);
    AutoContextSwitch mem_switch(econtext->ecxt_per_tuple_memory);

    econtext->align_rows = rows;
    int j = 0;
    foreach (hk, hash_keys) {
        ExprState* clause = (ExprState*)lfirst(hk);

        results = VectorExprEngine(clause, econtext, p_selection, m_cjVector, NULL);

        if (first_enter) {
            for (int i = 0; i < rows; i++) {
                key = results->m_vals[i];

                if (NOT_NULL(results->m_flag[i]))
                    m_cacheLoc[i] = FunctionCall1(&hash_functions[j], key);
                else
                    m_cacheLoc[i] = 0;
            }
            first_enter = false;
        } else {
            for (int i = 0; i < rows; i++) {
                if (NOT_NULL(results->m_flag[i])) {
                    key = results->m_vals[i];
                    /* rotate hashkey left 1 bit at each rehash step */
                    hash_val = m_cacheLoc[i];
                    hash_val = (hash_val << 1) | ((hash_val & 0x80000000) ? 1 : 0);
                    hash_val ^= FunctionCall1(&hash_functions[j], key);
                    m_cacheLoc[i] = hash_val;
                }
            }
        }
        j++;
    }

    // Rehash the hash value for avoiding the key and distribute key using the same hash function.
    //
    for (int i = 0; i < rows; i++)
        m_cacheLoc[i] = hash_uint32(DatumGetUInt32(m_cacheLoc[i]));
}

void HashJoinTbl::ResetNecessary()
{
    m_joinStateLog.restore = false;
    m_joinStateLog.lastBuildIdx = 0;

    /* For partition wise join, need to rescan right trees plan */
    if (!m_runtime->js.ps.plan->ispwj && m_strategy == MEMORY_HASH && m_runtime->js.ps.righttree->chgParam == NULL &&
        !((VecHashJoin*)m_runtime->js.ps.plan)->rebuildHashTable && m_runtime->js.jointype != JOIN_RIGHT_SEMI &&
        m_runtime->js.jointype != JOIN_RIGHT_ANTI) {
        /* Okay to reuse the hash table; needn't rescan inner, either. */
        m_runtime->joinState = HASH_PROBE;
        m_probeStatus = PROBE_FETCH;
        return;
    }

    if (m_strategy == GRACE_HASH) {
        /*
         * Temp files may have already been released, must close temp files
         */

        Assert(m_probeFileSource);
        Assert(m_buildFileSource);

        m_buildFileSource->closeAll();
        m_probeFileSource->closeAll();

        m_buildFileSource->resetFileSource();
        m_probeFileSource->resetFileSource();

        if (m_pLevel) {
            pfree_ext(m_pLevel);
            m_pLevel = NULL;
        }

        if (m_isValid) {
            pfree_ext(m_isValid);
            m_isValid = NULL;
        }
    }

    MemoryContextResetAndDeleteChildren(m_hashContext);

    /*
     * hash table is under m_hashContext,  must destroy and rebuild hash table
     */
    m_runtime->joinState = HASH_BUILD;
    m_strategy = MEMORY_HASH;
    m_availmems = m_totalMem;
    m_rows = 0;
    m_probeIdx = 0;
    m_cache = NULL;
    m_isWarning = false;
    /*
     * if chgParam of subnode is not null then plan will be re-scanned
     * by first VectorEngine.
     */
    if (m_runtime->js.ps.righttree->chgParam == NULL) {
        VecExecReScan(m_runtime->js.ps.righttree);
    }
}

/* @Description: record partition information into log file
 * @in build_side: build side or not(probe side)
 * @in file_idx: index of the file if any to be repartitioned
 * @in istart: start index of the partition(also known as temp file)
 * @in iend: end index of the partitions (do not include iend)
 * @return: void
 */
void HashJoinTbl::recordPartitionInfo(bool build_side, int file_idx, int istart, int iend)
{
    const char* side = build_side ? "Build" : "Probe";
    hashFileSource* source = build_side ? m_buildFileSource : m_probeFileSource;

    Assert(istart >= 0 && istart <= iend);
    if (file_idx >= 0) {
        elog(DEBUG1,
            "[VecHashJoin_RePartition] [%s Side]: Temp File: %d, Spill Time: %d, File Size: %ld, Total Memory: %ld, "
            "File Num: %d",
            side,
            file_idx,
            m_pLevel[file_idx],
            source->m_fileSize[file_idx],
            m_totalMem,
            iend - istart);
    }

    for (int i = istart; i < iend; i++) {
        elog(DEBUG1,
            "[VecHashJoin] [%s Side]: Temp File: %d, Spill Time: %d, File Size: %ld, Total Memory: %ld",
            side,
            i,
            m_pLevel[i],
            source->m_fileSize[i],
            m_totalMem);
    }
}

void ExecReScanVecHashJoin(VecHashJoinState* node)
{

    if (!IS_SONIC_HASH(node)) {
        HashJoinTbl* hj_tbl = (HashJoinTbl*)node->hashTbl;
        if (hj_tbl != NULL)
            hj_tbl->ResetNecessary();
    } else {
        SonicHashJoin* hj_tbl = (SonicHashJoin*)node->hashTbl;
        if (hj_tbl != NULL)
            hj_tbl->ResetNecessary();
    }

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first VectorEngine.
     */
    if (node->js.ps.lefttree->chgParam == NULL)
        VecExecReScan(node->js.ps.lefttree);
}

/*
 * @Description: Early free the memory for VecHashJoin.
 *
 * @param[IN] node:  vector executor state for HashJoin
 * @return: void
 */
void ExecEarlyFreeVecHashJoin(VecHashJoinState* node)
{
    PlanState* plan_state = &node->js.ps;

    if (plan_state->earlyFreed)
        return;

    void* tbl = node->hashTbl;

    if (tbl != NULL) {
        if (!IS_SONIC_HASH(node)) {
            HashJoinTbl* hj_tbl = (HashJoinTbl*)tbl;
            if (hj_tbl->m_buildFileSource) {
                Assert(hj_tbl->m_probeFileSource);

                hj_tbl->m_buildFileSource->closeAll();
                hj_tbl->m_probeFileSource->closeAll();
            }

            hj_tbl->freeMemoryContext();
        } else {
            SonicHashJoin* hj_tbl = (SonicHashJoin*)tbl;
            hj_tbl->closeAllFiles();
            hj_tbl->freeMemoryContext();
        }
    }

    EARLY_FREE_LOG(elog(LOG,
        "Early Free: After early freeing %sHashJoin "
        "at node %d, memory used %d MB.",
        JOIN_NAME,
        plan_state->plan->plan_node_id,
        getSessionMemoryUsageMB()));

    plan_state->earlyFreed = true;
    ExecEarlyFree(innerPlanState(node));
    ExecEarlyFree(outerPlanState(node));
}
