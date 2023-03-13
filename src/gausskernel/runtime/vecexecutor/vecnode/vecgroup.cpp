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
 * -------------------------------------------------------------------------
 *
 * vecgroup.cpp
 *    Prototypes for vectorized group
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecgroup.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "codegen/gscodegen.h"
#include "codegen/vecexprcodegen.h"
#include "executor/executor.h"
#include "executor/node/nodeGroup.h"
#include "vecexecutor/vecgroup.h"
#include "vecexecutor/vecgrpuniq.h"

/* Define CodeGen Object */
extern bool CodeGenThreadObjectReady();
extern bool CodeGenPassThreshold(double rows, int dn_num, int dop);

static VectorBatch* ProduceBatch(VecGroupState* node);

VectorBatch* ExecVecGroup(VecGroupState* node)
{
    void** grp_container = node->container;
    uint16 i = 0;
    uint16* idx = &(node->idx);
    VectorBatch* res_batch = NULL;
    Encap* cap = (Encap*)node->cap;
    cap->eqfunctions = node->eqfunctions;
    errno_t rc;

    if (node->grp_done)
        return NULL;

    node->bckBuf->Reset();
    node->scanBatch->Reset(true);

    for (;;) {
        // Get next batch, if it is null then break the loop.
        VectorBatch* batch = VectorEngine(outerPlanState(node));
        if (unlikely(BatchIsNull(batch)))
            break;

        // Call the buildFunc to read batch into grp_container using idx as the current position which is started from 0.
        cap->batch = batch;
        (void)FunctionCall2(node->buildFunc, PointerGetDatum(node), PointerGetDatum(cap));

        // When the grp_container is full, call buildScanFunc to dump BatchMaxSize cells from grp_container to scanBatch
        // which is used to produce outerBatch, then shift the idx to start from the remain cells and reset the ones in
        // the tail.
        if (*idx >= BatchMaxSize) {
            cap->batch = node->scanBatch;
            for (i = 0; i < BatchMaxSize; i++) {
                (void)FunctionCall2(node->buildScanFunc, PointerGetDatum(grp_container[i]), PointerGetDatum(cap));
            }
            uint16 remain_grp = *idx - BatchMaxSize + 1;
            for (i = 0; i < remain_grp; i++) {
                grp_container[i] = grp_container[BatchMaxSize + i];
            }
            rc = memset_s(&grp_container[remain_grp],
                sizeof(GUCell*) * (2 * BatchMaxSize - remain_grp),
                0,
                sizeof(GUCell*) * (2 * BatchMaxSize - remain_grp));
            securec_check(rc, "\0", "\0");
            *idx = remain_grp - 1;
            res_batch = ProduceBatch(node);
            if (unlikely(BatchIsNull(res_batch))) {
                // release the space
                node->bckBuf->Reset();
                // reset the batch.
                node->scanBatch->Reset(true);
                continue;
            } else
                return res_batch;
        }
    }

    // means no data.
    if (0 == *idx && NULL == grp_container[0]) {
        return NULL;
    }

    cap->batch = node->scanBatch;
    for (i = 0; i <= *idx; i++) {
        (void)FunctionCall2(node->buildScanFunc, PointerGetDatum(grp_container[i]), PointerGetDatum(cap));
    }

    node->grp_done = true;
    return ProduceBatch(node);
}

static VectorBatch* ProduceBatch(VecGroupState* node)
{
    ExprContext* expr_context = NULL;
    VectorBatch* p_res = NULL;
    VectorBatch* batch = node->scanBatch;

    // Guard when there is no input rows
    if (batch == NULL)
        return NULL;

    expr_context = node->ss.ps.ps_ExprContext;
    ResetExprContext(expr_context);
    expr_context->ecxt_scanbatch = batch;
    expr_context->ecxt_outerbatch = batch;

    if (list_length((List*)node->ss.ps.qual) != 0) {
        ScalarVector* p_vector = NULL;

        /*
         * If grp_state->node->ss.ps.qual can be codegened, use LLVM optimization
         * first( the cost model has been already considered ).
         */
        if (node->jitted_vecqual) {
            p_vector = node->jitted_vecqual(expr_context);

            if (HAS_INSTR(&node->ss, false)) {
                node->ss.ps.instrument->isLlvmOpt = true;
            }
        } else
            p_vector = ExecVecQual(node->ss.ps.qual, expr_context, false);

        if (p_vector == NULL)
            return NULL;
        batch->Pack(expr_context->ecxt_scanbatch->m_sel);
    }

    expr_context->m_fUseSelection = node->ss.ps.ps_ExprContext->m_fUseSelection;
    p_res = ExecVecProject(node->ss.ps.ps_ProjInfo);
    return p_res;
}

VecGroupState* ExecInitVecGroup(VecGroup* node, EState* estate, int eflags)
{
    VecGroupState* grp_state = NULL;
    ScalarDesc unknown_desc;

    // check for unsupported flags
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    // create state structure
    grp_state = makeNode(VecGroupState);
    grp_state->ss.ps.vectorized = true;
    grp_state->ss.ps.plan = (Plan*)node;
    grp_state->ss.ps.state = estate;
    grp_state->grp_done = false;

    // create expression context
    ExecAssignExprContext(estate, &grp_state->ss.ps);

    // tuple table initialization
    ExecInitResultTupleSlot(estate, &grp_state->ss.ps);

    // initialize child expressions
    grp_state->ss.ps.targetlist = (List*)ExecInitVecExpr((Expr*)node->plan.targetlist, (PlanState*)grp_state);
    grp_state->ss.ps.qual = (List*)ExecInitVecExpr((Expr*)node->plan.qual, (PlanState*)grp_state);

#ifdef ENABLE_LLVM_COMPILE
    /*
     * Check if nlstate->js.joinqual and nlstate->js.ps.qual expr list could be
     * codegened or not.
     */
    llvm::Function* grp_vecqual = NULL;
    dorado::GsCodeGen* llvm_code_gen = (dorado::GsCodeGen*)t_thrd.codegen_cxt.thr_codegen_obj;
    bool consider_codegen =
        CodeGenThreadObjectReady() &&
        CodeGenPassThreshold(((Plan*)node)->plan_rows, estate->es_plannedstmt->num_nodes, ((Plan*)node)->dop);
    if (consider_codegen) {
        grp_vecqual = dorado::VecExprCodeGen::QualCodeGen((List*)grp_state->ss.ps.qual, (PlanState*)grp_state);
        if (grp_vecqual != NULL)
            llvm_code_gen->addFunctionToMCJit(grp_vecqual, reinterpret_cast<void**>(&(grp_state->jitted_vecqual)));
    }
#endif

    // initialize child nodes
    outerPlanState(grp_state) = ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * Initialize result tuple type and projection info.
     * Group node result tuple slot always holds virtual tuple, so
     * default tableAm type is set to HEAP.
     */
    ExecAssignResultTypeFromTL(&grp_state->ss.ps);

    grp_state->ss.ps.ps_ProjInfo = ExecBuildVecProjectionInfo(grp_state->ss.ps.targetlist,
        node->plan.qual,
        grp_state->ss.ps.ps_ExprContext,
        grp_state->ss.ps.ps_ResultTupleSlot,
        NULL);

    ExecAssignVectorForExprEval(grp_state->ss.ps.ps_ExprContext);

    // Precompute fmgr lookup data for inner loop
    grp_state->eqfunctions = execTuplesMatchPrepare(node->numCols, node->grpOperators);

    // Initialize the variables used in Vector execution process and bind the function in FmgrInfo.
    grp_state->cap = (void*)palloc(sizeof(Encap));
    InitGrpUniq<VecGroupState>(grp_state, node->numCols, node->grpColIdx);
    return grp_state;
}

void ExecEndVecGroup(VecGroupState* node)
{
    ExecFreeExprContext(&node->ss.ps);
    ExecEndNode(outerPlanState(node));
}

void ExecReScanVecGroup(VecGroupState* node)
{
    node->ss.ps.ps_vec_TupFromTlist = false;
    node->grp_done = false;
    ReScanGrpUniq<VecGroupState>(node);

    // if chgParam of subnode is not null then plan will be re-scanned by
    // first ExecProcNode.
    if (node->ss.ps.lefttree && node->ss.ps.lefttree->chgParam == NULL)
        VecExecReScan(node->ss.ps.lefttree);
}
