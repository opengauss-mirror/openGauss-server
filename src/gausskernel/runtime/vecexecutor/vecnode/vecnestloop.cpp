/* ---------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * vecnestloop.cpp
 *	  routines to support nest-loop joins
 *
 * IDENTIFICATION
 *	  Code/src/gausskernel/runtime/vecexecutor/vecnode/vecnestloop.cpp
 *
 * -------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecVecNestLoop	 - process a nestloop join of two plans
 *		ExecInitVecNestLoop - initialize the join
 *		ExecEndVecNestLoop  - shut down the join
 */
#include "codegen/gscodegen.h"
#include "codegen/vecexprcodegen.h"

#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/exec/execdebug.h"
#include "executor/node/nodeNestloop.h"
#include "utils/memutils.h"
#include "utils/memprot.h"
#include "vecexecutor/vecnodes.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vecnestloop.h"

/* Define CodeGen Object */
extern bool CodeGenThreadObjectReady();
extern bool CodeGenPassThreshold(double rows, int dn_num, int dop);

static void VecMaterialAll(PlanState* node)
{
    if (IsA(node, VecMaterialState)) {
        ((VecMaterialState*)node)->materalAll = true;

        /*
         * Call ExecProcNode to material all the inner tuple first.
         */
        VectorEngine(node);

        /* early free the left tree of Material. */
        ExecEarlyFree(outerPlanState(node));

        /* early deinit consumer in left tree of Material. */
        ExecEarlyDeinitConsumer(node);
    }
}

template <bool ifTargetlistNull>
void VecNestLoopRuntime::FetchOuterT()
{
    PlanState* out_plan = outerPlanState(m_runtime);

    m_outerBatch = VectorEngine(out_plan);
    if (ifTargetlistNull == true && m_outJoinBatch == NULL && !BatchIsNull(m_outerBatch)) {
        m_outJoinBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_outerBatch);
        m_outJoinBatch->m_rows = BatchMaxSize;
        m_outJoinBatch->FixRowCount();
    }

    if (BatchIsNull(m_outerBatch))
        m_status = NL_END;
    else
        m_status = NL_NEEDNEXTOUTROW;
}

void VecNestLoopRuntime::NextOuterRow()
{
    if (m_outReadIdx == m_outerBatch->m_rows) {
        m_status = NL_NEEDNEWOUTER;
        m_outReadIdx = 0;
        return;
    }

    ScalarValue val;
    uint8 flag;
    ScalarValue* dest_val = NULL;
    uint8* dest_flag = NULL;
    NestLoop* nl = NULL;
    ExprContext* econtext = m_runtime->js.ps.ps_ExprContext;
    ListCell* lc = NULL;
    nl = (NestLoop*)m_runtime->js.ps.plan;
    PlanState* inner_plan = innerPlanState(m_runtime);

    for (int i = 0; i < m_outerBatch->m_cols; i++) {
        val = m_outerBatch->m_arr[i].m_vals[m_outReadIdx];
        flag = m_outerBatch->m_arr[i].m_flag[m_outReadIdx];

        dest_val = m_outJoinBatch->m_arr[i].m_vals;
        dest_flag = m_outJoinBatch->m_arr[i].m_flag;

        if (NOT_NULL(flag)) {
            dest_val[0] = val;
            SET_NOTNULL(dest_flag[0]);
        } else {
            SET_NULL(dest_flag[0]);
        }
        m_outJoinBatch->m_arr[i].m_rows = 1;
    }
    m_outJoinBatch->m_rows = 1;
    m_outJoinBatchRows = 1;

    foreach (lc, nl->nestParams) {
        NestLoopParam* nlp = (NestLoopParam*)lfirst(lc);
        int paramno = nlp->paramno;
        ParamExecData* prm = NULL;

        prm = &(econtext->ecxt_param_exec_vals[paramno]);
        /* Param value should be an OUTER_VAR var */
        Assert(IsA(nlp->paramval, Var));
        Assert(nlp->paramval->varno == OUTER_VAR);
        Assert(nlp->paramval->varattno > 0);
        int col_num = nlp->paramval->varattno - 1;
        ScalarVector* col = &m_outJoinBatch->m_arr[col_num];

        prm->valueType = col->m_desc.typeId;
        prm->isChanged = true;
        if (col->IsNull(0)) {
            prm->value = 0;
            prm->isnull = true;
        } else {
            prm->value = col->m_vals[0];
            prm->isnull = false;
        }
        /* Flag parameter value as changed */
        inner_plan->chgParam = bms_add_member(inner_plan->chgParam, paramno);
    }

    m_outReadIdx++;
    VecExecReScan(inner_plan);

    m_status = NL_EXECQUAL;
    m_matched = false;
}

template <JoinType type, bool doProject, bool hasJoinQual, bool hasOtherQual>
VectorBatch* VecNestLoopRuntime::JoinQualT()
{
    ExprContext* econtext = m_runtime->js.ps.ps_ExprContext;
    PlanState* in_plan = innerPlanState(m_runtime);
    VectorBatch* inner_batch = NULL;
    List* other_qual = m_runtime->js.ps.qual;
    List* join_qual = m_runtime->js.joinqual;
    VectorBatch* result = NULL;
    bool is_reset_sel = true;

    bool is_jitted_otherqual = false;
    bool is_jitted_joinqual = false;

    if (m_runtime->jitted_vecqual)
        is_jitted_otherqual = true;

    if (m_runtime->jitted_joinqual)
        is_jitted_joinqual = true;

    /* Mark if VecNestLoop has been LLVM optimized */
    if (is_jitted_otherqual || is_jitted_joinqual) {
        if (HAS_INSTR(&m_runtime->js, false)) {
            m_runtime->js.ps.instrument->isLlvmOpt = true;
        }
    }

    /*
     * If inner plan is mergejoin, which does not cache data,
     * but will early free the left and right tree's caching memory.
     * When rescan left tree, may fail.
     */
    bool orig_value = in_plan->state->es_skip_early_free;
    if (!IsA(in_plan, VecMaterialState))
        in_plan->state->es_skip_early_free = true;

    inner_batch = VectorEngine(in_plan);

    in_plan->state->es_skip_early_free = orig_value;

    econtext->ecxt_innerbatch = inner_batch;
    econtext->ecxt_scanbatch = inner_batch;
    econtext->ecxt_outerbatch = m_outJoinBatch;

    // we must align the rows for less expression computation.
    if (BatchIsNull(inner_batch) == false)
        OutJoinBatchAlignInnerJoinBatch(inner_batch->m_rows);

    /*
     * In default, we should not reset the econtext->ecxt_scanbatch->m_sel in other_qual if
     * we have joinqual. Since after joinqual, we alreay got part pSel results. once we reset
     * the econtext->ecxt_scanbatch->m_sel in other_qual, the results change. While if there
     * is no joinqual, we should do reset in other_qual.
     */
    if (hasJoinQual)
        is_reset_sel = false;

    if (BatchIsNull(inner_batch)) {
        m_status = NL_NEEDNEXTOUTROW;
        ENL1_printf("no inner tuple, need new outer tuple");
        if (m_matched == false && (type == JOIN_LEFT || type == JOIN_ANTI || type == JOIN_LEFT_ANTI_FULL)) {
            econtext->ecxt_innerbatch = m_innerNullBatch;
            econtext->ecxt_scanbatch = m_innerNullBatch;

            // we set  1 row for less expression evaluation
            OutJoinBatchAlignInnerJoinBatch(1);
            if (hasOtherQual == false ||
                (is_jitted_otherqual ? m_runtime->jitted_vecqual(econtext) : ExecVecQual(other_qual, econtext, false))) {
                if (doProject == false) {
                    result = econtext->ecxt_outerbatch;
                    result->m_rows = 1;
                } else {
                    result = ExecVecProject(m_runtime->js.ps.ps_ProjInfo);
                    if (hasOtherQual)
                        result->Pack(econtext->ecxt_scanbatch->m_sel);
                    result->m_rows = Min(1, result->m_rows);
                }
                result->FixRowCount();
            } else {
                InstrCountFiltered2(m_runtime, 1);
            }
        }
    } else if (hasJoinQual == false ||
               (is_jitted_joinqual ? m_runtime->jitted_joinqual(econtext) : ExecVecQual(join_qual, econtext, false))) {
        m_matched = true;
        if (type == JOIN_ANTI || type == JOIN_LEFT_ANTI_FULL) {
            m_status = NL_NEEDNEXTOUTROW;
            return NULL;
        }
        if (type == JOIN_SEMI) {
            m_status = NL_NEEDNEXTOUTROW;
        }
        if (hasOtherQual == false || ExecVecQual(other_qual, econtext, false, is_reset_sel)) {
            VectorBatch* outBatch = econtext->ecxt_outerbatch;
            VectorBatch* inBatch = econtext->ecxt_innerbatch;
            if (doProject == false) {
                /* m_sel is invalid if joinqual is null and other_qual is null
                 * rows of the result should be inBatch->m_rows
                 */
                if (hasJoinQual || hasOtherQual) {
                    /* We must first do pack of outBatch, then do pack of inBatch
                     * because inBatch and econtext->ecxt_scanbatch point to same inner_batch
                     * m_sel will be changed when inBatch Pack.
                     */
                    outBatch->Pack(econtext->ecxt_scanbatch->m_sel);
                    inBatch->Pack(econtext->ecxt_scanbatch->m_sel);
                    Assert(inBatch->IsValid() && outBatch->IsValid());
                    Assert(inBatch->m_rows == outBatch->m_rows);
                }
                result = inBatch;
            } else {
                if (hasJoinQual || hasOtherQual) {
                    /* We must first do pack of outBatch, then do pack of inBatch
                     * because inBatch and econtext->ecxt_scanbatch point to same inner_batch
                     * m_sel will be changed when inBatch Pack.
                     */
                    outBatch->Pack(econtext->ecxt_scanbatch->m_sel);
                    inBatch->Pack(econtext->ecxt_scanbatch->m_sel);
                    Assert(inBatch->IsValid() && outBatch->IsValid());
                    Assert(inBatch->m_rows == outBatch->m_rows);
                }

                /* m_sel is invalid if joinqual is null and otherqualis null
                 * rows of the result should be inBatch->m_rows
                 */
                result = ExecVecProject(m_runtime->js.ps.ps_ProjInfo);

                /* We need set rows of const, const rows is 1000 which generate from ExecEvalVecConst
                 * For example select count(*) statement
                 */
                result->m_rows = inBatch->m_rows;
                result->FixRowCount();
            }
            if (type == JOIN_SEMI) {
                /* only need get one row data instead of 1000 for semi join */
                result->m_rows = 1;
                result->FixRowCount();
            }

            /*
             * @hdfs
             * Optimize the nestloop plan by using informational constraint.
             */
            if (((NestLoop*)(m_runtime->js.ps.plan))->join.optimizable) {
                /*
                 * Do not need to scan inner relation brcause of using informaitona
                 * constaint here.
                 */
                result->m_rows = 1;
                result->FixRowCount();
                m_status = NL_NEEDNEXTOUTROW;
            }
        }
    }

    return result;
}

template <bool ifReturnNotFull>
VectorBatch* VecNestLoopRuntime::JoinT()
{
    ExprContext* econtext = m_runtime->js.ps.ps_ExprContext;
    if (!ifReturnNotFull) {
        m_bckBatch->Reset();
    }

    for (;;) {
        ResetExprContext(econtext);
        switch (m_status) {
            case NL_NEEDNEWOUTER: {
                InvokeFp(FetchOuter)();
                break;
            }

            case NL_NEEDNEXTOUTROW: {
                NextOuterRow();
                break;
            }

            case NL_EXECQUAL: {
                VectorBatch* batch = InvokeFp(JoinOnQual)();

                if (!ifReturnNotFull) {
                    batch = InvokeFp(WrapperBatch)(batch);
                }

                if (BatchIsNull(batch) == false) {
                    return batch;
                }
                break;
            }

            case NL_END: {
                VectorBatch* result = NULL;
                if (!ifReturnNotFull) {
                    if (m_bufferRows > 0) {
                        m_bufferRows = 0;
                        result = m_currentBatch;
                    }
                }

                ExecEarlyFree(innerPlanState(m_runtime));
                ExecEarlyFree(outerPlanState(m_runtime));

                EARLY_FREE_LOG(elog(LOG,
                    "Early Free: NestLoop is done "
                    "at node %d, memory used %d MB.",
                    (m_runtime->js.ps.plan)->plan_node_id,
                    getSessionMemoryUsageMB()));
                return result;
                // avoid compile noise
                break;
            }

            default:
                break;
        }
    }
}

template <JoinType type>
void VecNestLoopRuntime::InitT(List* join_qual, List* other_qual, ProjectionInfo* proj_info)
{
    if (proj_info) {
        if (join_qual) {
            if (other_qual)
                JoinOnQual = &VecNestLoopRuntime::JoinQualT<type, true, true, true>;
            else
                JoinOnQual = &VecNestLoopRuntime::JoinQualT<type, true, true, false>;
        } else {
            if (other_qual)
                JoinOnQual = &VecNestLoopRuntime::JoinQualT<type, true, false, true>;
            else
                JoinOnQual = &VecNestLoopRuntime::JoinQualT<type, true, false, false>;
        }
    } else {
        if (join_qual) {
            if (other_qual)
                JoinOnQual = &VecNestLoopRuntime::JoinQualT<type, false, true, true>;
            else
                JoinOnQual = &VecNestLoopRuntime::JoinQualT<type, false, true, false>;
        } else {
            if (other_qual)
                JoinOnQual = &VecNestLoopRuntime::JoinQualT<type, false, false, true>;
            else
                JoinOnQual = &VecNestLoopRuntime::JoinQualT<type, false, false, false>;
        }
    }
}

void VecNestLoopRuntime::BindingFp()
{
    List* join_qual = m_runtime->js.joinqual;
    List* other_qual = m_runtime->js.ps.qual;
    JoinType join_type = m_runtime->js.jointype;
    ProjectionInfo* proj_info = m_runtime->js.ps.ps_ProjInfo;

    switch (join_type) {
        case JOIN_ANTI: {
            InitT<JOIN_ANTI>(join_qual, other_qual, proj_info);
            break;
        }

        case JOIN_LEFT: {
            InitT<JOIN_LEFT>(join_qual, other_qual, proj_info);
            break;
        }

        case JOIN_SEMI: {
            InitT<JOIN_SEMI>(join_qual, other_qual, proj_info);
            break;
        }

        case JOIN_LEFT_ANTI_FULL: {
            InitT<JOIN_LEFT_ANTI_FULL>(join_qual, other_qual, proj_info);
            break;
        }

        default:
            /* JOIN_RIGHT_ANTI_FULL can not create nestloop paln, please
             * refer to match_unsorted_outer,
             * nestjoinOK = false;
             */
            Assert(join_type != JOIN_RIGHT_ANTI_FULL);
            InitT<JOIN_FULL>(join_qual, other_qual, proj_info);
            break;
    }

    if (m_outerTargetIsNull) {
        FetchOuter = &VecNestLoopRuntime::FetchOuterT<true>;
    } else {
        FetchOuter = &VecNestLoopRuntime::FetchOuterT<false>;
    }
}

/* ----------------------------------------------------------------
 *		ExecInitVecNestLoop
 * ----------------------------------------------------------------
 */
VecNestLoopState* ExecInitVecNestLoop(VecNestLoop* node, EState* estate, int eflags)
{
    VecNestLoopState* nlstate = NULL;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    NL1_printf("ExecInitNestLoop: %s\n", "initializing node");

    /*
     * create state structure
     */
    nlstate = makeNode(VecNestLoopState);
    nlstate->js.ps.plan = (Plan*)node;
    nlstate->js.ps.state = estate;
    nlstate->js.ps.vectorized = true;
    nlstate->nl_MaterialAll = node->materialAll;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &nlstate->js.ps);

    /*
     * initialize child expressions
     */
    nlstate->js.ps.targetlist = (List*)ExecInitVecExpr((Expr*)node->join.plan.targetlist, (PlanState*)nlstate);
    nlstate->js.ps.qual = (List*)ExecInitVecExpr((Expr*)node->join.plan.qual, (PlanState*)nlstate);
    nlstate->js.jointype = node->join.jointype;
    nlstate->js.joinqual = (List*)ExecInitVecExpr((Expr*)node->join.joinqual, (PlanState*)nlstate);
    Assert(node->join.nulleqqual == NIL);

#ifdef ENABLE_LLVM_COMPILE
    /*
     * Check if nlstate->js.joinqual and nlstate->js.ps.qual expr list could be
     * codegened or not.
     */
    llvm::Function* nl_vecqual = NULL;
    llvm::Function* nl_joinqual = NULL;
    dorado::GsCodeGen* llvm_code_gen = (dorado::GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    bool consider_codegen =
        CodeGenThreadObjectReady() &&
        CodeGenPassThreshold(((Plan*)node)->plan_rows, estate->es_plannedstmt->num_nodes, ((Plan*)node)->dop);
    if (consider_codegen) {
        nl_vecqual = dorado::VecExprCodeGen::QualCodeGen(nlstate->js.ps.qual, (PlanState*)nlstate);
        if (nl_vecqual != NULL)
            llvm_code_gen->addFunctionToMCJit(nl_vecqual, reinterpret_cast<void**>(&(nlstate->jitted_vecqual)));

        nl_joinqual = dorado::VecExprCodeGen::QualCodeGen(nlstate->js.joinqual, (PlanState*)nlstate);
        if (nl_joinqual != NULL)
            llvm_code_gen->addFunctionToMCJit(nl_joinqual, reinterpret_cast<void**>(&(nlstate->jitted_joinqual)));
    }
#endif

    /*
     * initialize child nodes
     *
     * If we have no parameters to pass into the inner rel from the outer,
     * tell the inner child that cheap rescans would be good.  If we do have
     * such parameters, then there is no point in REWIND support at all in the
     * inner child, because it will always be rescanned with fresh parameter
     * values.
     */
    outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
    if (node->nestParams == NIL)
        eflags |= EXEC_FLAG_REWIND;
    else
        eflags &= ~EXEC_FLAG_REWIND;
    innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate, eflags);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &nlstate->js.ps);

    /*
     * initialize tuple type and projection info
     */
    ExecAssignResultTypeFromTL(&nlstate->js.ps);
    PlanState* planstate = &nlstate->js.ps;
    planstate->ps_ProjInfo = ExecBuildVecProjectionInfo(
        planstate->targetlist, node->join.plan.qual, planstate->ps_ExprContext, planstate->ps_ResultTupleSlot, NULL);
    /*
     * finally, wipe the current outer tuple clean.
     */
    nlstate->js.ps.ps_vec_TupFromTlist = false;
    nlstate->nl_NeedNewOuter = true;
    nlstate->nl_MatchedOuter = false;

    ExecAssignVectorForExprEval(nlstate->js.ps.ps_ExprContext);

    nlstate->vecNestLoopRuntime = New(CurrentMemoryContext) VecNestLoopRuntime(nlstate);

    return nlstate;
}

VecNestLoopRuntime::VecNestLoopRuntime(VecNestLoopState* runtime)
{
    m_runtime = runtime;
    VecNestLoop* node = (VecNestLoop*)m_runtime->js.ps.plan;
    FormData_pg_attribute* attrs = NULL;
    int col_num = 0;
    TupleDesc outer_desc = ExecGetResultType(outerPlanState(m_runtime));
    ScalarVector* column = NULL;
    m_status = NL_NEEDNEWOUTER;
    m_SimpletargetCol = true;
    m_outerTargetIsNull = false;
    m_outerBatch = NULL;
    m_outJoinBatchRows = 0;
    if (outer_desc->natts == 0) {
        m_outerTargetIsNull = true;
        m_outJoinBatch = NULL;
    } else {
        m_outJoinBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, outer_desc);
        m_outJoinBatch->m_rows = BatchMaxSize;
        m_outJoinBatch->FixRowCount();
    }

    m_outReadIdx = 0;
    m_joinType = node->join.jointype;
    m_matched = false;

    // The four variable belows are used only ifReturnNotFull if false, but it ifReturnNotFull is true, we also create
    // them but not use them.(just occupy a bit memory).
    //
    TupleDesc target_desc = ExecGetResultType((PlanState*)m_runtime);
    m_currentBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, target_desc);
    m_bckBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, target_desc);
    m_bufferRows = 0;

    switch (m_joinType) {
        case JOIN_INNER:
        case JOIN_SEMI:
            break;
        case JOIN_LEFT:
        case JOIN_ANTI:
        case JOIN_LEFT_ANTI_FULL: {
            m_innerNullBatch = New(CurrentMemoryContext)
                VectorBatch(CurrentMemoryContext, ExecGetResultType(innerPlanState(m_runtime)));
            for (int i = 0; i < m_innerNullBatch->m_cols; i++) {
                column = &(m_innerNullBatch->m_arr[i]);
                column->m_rows = 1;
                column->SetNull(0);
            }
            m_innerNullBatch->m_rows = 1;
            break;
        }
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_EXECUTOR),
                    errmsg("unrecognized join type: %d", (int)node->join.jointype)));
    }

    attrs = target_desc->attrs;
    col_num = target_desc->natts;
    for (int i = 0; i < col_num; i++) {
        if (COL_IS_ENCODE(attrs[i].atttypid)) {
            m_SimpletargetCol = false;
            break;
        }
    }

    BindingFp();
}

VectorBatch* ExecVecNestloop(VecNestLoopState* node)
{
    if (node->nl_MaterialAll) {
        VecMaterialAll(innerPlanState(node));
        node->nl_MaterialAll = false;
    }

    VecNestLoopRuntime* runtime = (VecNestLoopRuntime*)node->vecNestLoopRuntime;
    Assert(runtime != NULL);
    // If want to output 1000 rows in one batch, then change this to false
    //
    return runtime->JoinT<true>();
}

/* ----------------------------------------------------------------
 *		ExecEndVecNestLoop
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
void ExecEndVecNestLoop(VecNestLoopState* node)
{
    NL1_printf("ExecEndNestLoop: %s\n", "ending node processing");

    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->js.ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->js.ps.ps_ResultTupleSlot);

    /*
     * close down subplans
     */
    ExecEndNode(outerPlanState(node));
    ExecEndNode(innerPlanState(node));

    NL1_printf("ExecEndNestLoop: %s\n", "node processing ended");
}

/* ----------------------------------------------------------------
 *		ExecReScanVecNestLoop
 * ----------------------------------------------------------------
 */
void ExecReScanVecNestLoop(VecNestLoopState* node)
{
    PlanState* outer_plan = outerPlanState(node);

    /*
     * If outer_plan->chgParam is not null then plan will be automatically
     * re-scanned by first ExecProcNode.
     */
    if (outer_plan->chgParam == NULL)
        VecExecReScan(outer_plan);

    /*
     * inner_plan is re-scanned for each new outer tuple and MUST NOT be
     * re-scanned from here or you'll get troubles from inner index scans when
     * outer Vars are used as run-time keys...
     */
    node->js.ps.ps_vec_TupFromTlist = false;

    VecNestLoopRuntime* runtime = (VecNestLoopRuntime*)node->vecNestLoopRuntime;
    runtime->Rescan();
}

void VecNestLoopRuntime::Rescan()
{
    m_status = NL_NEEDNEWOUTER;
    m_outReadIdx = 0;
    m_matched = false;

    // The two initialization are usable only when ifReturnNotFull if false, here we just retain them for the future
    // optimization. Any way, they do no harm.
    //
    m_bufferRows = 0;
    m_currentBatch->Reset();
}

inline void VecNestLoopRuntime::OutJoinBatchAlignInnerJoinBatch(int rows)
{
    // we have got the right rows;
    if (rows <= m_outJoinBatchRows) {
        m_outJoinBatch->m_rows = rows;
        m_outJoinBatch->FixRowCount();
        return;
    }

    ScalarValue val;
    uint8 flag;
    ScalarValue* dest_val = NULL;
    uint8* dest_flag = NULL;

    for (int i = 0; i < m_outerBatch->m_cols; i++) {
        dest_val = m_outJoinBatch->m_arr[i].m_vals;
        dest_flag = m_outJoinBatch->m_arr[i].m_flag;

        val = dest_val[0];
        flag = dest_flag[0];

        if (NOT_NULL(flag)) {
            for (int k = m_outJoinBatchRows; k < rows; k++) {
                dest_val[k] = val;
                SET_NOTNULL(dest_flag[k]);
            }
        } else {
            for (int k = m_outJoinBatchRows; k < rows; k++)
                SET_NULL(dest_flag[k]);
        }
        m_outJoinBatch->m_arr[i].m_rows = rows;
    }
    m_outJoinBatch->m_rows = rows;
    m_outJoinBatchRows = rows;
}

VecNestLoopRuntime::~VecNestLoopRuntime()
{
    m_runtime = NULL;
    m_innerNullBatch = NULL;
    m_outerBatch = NULL;
    m_outJoinBatch = NULL;
    m_currentBatch = NULL;
    m_bckBatch = NULL;
}
