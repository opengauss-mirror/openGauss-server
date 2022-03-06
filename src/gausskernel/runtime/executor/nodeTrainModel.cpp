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
*---------------------------------------------------------------------------------------
*
* nodeTrainModel.cpp
*        Implementation of Model Training Operators
*
* IDENTIFICATION
*        src/gausskernel/runtime/executor/nodeTrainModel.cpp
*
* ---------------------------------------------------------------------------------------
*/

#include "postgres.h"
#include "funcapi.h"

#include "executor/executor.h"
#include "executor/node/nodeTrainModel.h"
#include "db4ai/db4ai_api.h"

static bool ExecFetchTrainModel(void *callback_data, ModelTuple * tuple)
{
    TrainModelState *pstate = (TrainModelState*)callback_data;
    PlanState *outer_plan = outerPlanState(pstate);
    TupleTableSlot *slot = ExecProcNode(outer_plan);
    if (TupIsNull(slot))
        return false;
    
    if (tuple != &pstate->tuple) {
        // make sure the output tuple has all information
        tuple->ncolumns = pstate->tuple.ncolumns;
        tuple->typid = pstate->tuple.typid;
        tuple->typlen = pstate->tuple.typlen;
        tuple->typbyval = pstate->tuple.typbyval;
    }
    
    // support of tuples that are (physical) - i.e., not virtual
    if (slot->tts_tuple != nullptr) {
        if (!pstate->row_allocated) {
            tuple->values = (Datum *)palloc(sizeof(Datum) * tuple->ncolumns);
            tuple->isnull = (bool *)palloc(sizeof(bool) *tuple->ncolumns);
            pstate->row_allocated = true;
        }
        /*
         *  When all or most of a tuple's fields need to be extracted,
         *  this routine will be significantly quicker than a loop around
         *  heap_getattr; the loop will become O(N^2) as soon as any
         *  noncacheable attribute offsets are involved.
         */
        heap_deform_tuple((HeapTuple)slot->tts_tuple, slot->tts_tupleDescriptor,
                          tuple->values, tuple->isnull);
    } else {
        Assert(!pstate->row_allocated);
        tuple->values = slot->tts_values;
        tuple->isnull = slot->tts_isnull;
    }
    return true;
}

static void ExecReScanTrainModel(void *callback_data)
{
    TrainModelState *pstate = (TrainModelState*)callback_data;
    PlanState *outer_plan = outerPlanState(pstate);
    ExecReScan(outer_plan);
}

TrainModelState* ExecInitTrainModel(TrainModel* pnode, EState* estate, int eflags)
{
    TrainModelState *pstate = NULL;
    Plan *outer_plan = outerPlan(pnode);
    
    // check for unsupported flags
    Assert(!(eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    
    // create state structure
    AlgorithmAPI *palgo = get_algorithm_api(pnode->algorithm);
    Assert(palgo->create != nullptr);
    pstate = palgo->create(palgo, pnode);
    pstate->ss.ps.plan = (Plan *)pnode;
    pstate->ss.ps.state = estate;
    pstate->config = pnode;
    pstate->algorithm = palgo;
    pstate->finished = 0;
    
    // Tuple table initialization
    ExecInitScanTupleSlot(estate, &pstate->ss);
    ExecInitResultTupleSlot(estate, &pstate->ss.ps);
    
    // initialize child expressions
    ExecAssignExprContext(estate, &pstate->ss.ps);
    pstate->ss.ps.targetlist = (List *)ExecInitExpr((Expr *)pnode->plan.targetlist, (PlanState *)pstate);
    
    // initialize outer plan
    PlanState *outer_plan_state = ExecInitNode(outer_plan, estate, eflags);
    outerPlanState(pstate) = outer_plan_state;
    
    // Initialize result tuple type and projection info.
    ExecAssignScanTypeFromOuterPlan(&pstate->ss); // input tuples
    ExecAssignResultTypeFromTL(&pstate->ss.ps);  // result tuple
    ExecAssignProjectionInfo(&pstate->ss.ps, NULL);
    pstate->ss.ps.ps_TupFromTlist = false;
    
    // Input tuple initialization
    TupleDesc tupdesc = ExecGetResultType(outer_plan_state);
    pstate->tuple.ncolumns = tupdesc->natts;
    pstate->tuple.typid = (Oid *)palloc(sizeof(Oid) * pstate->tuple.ncolumns);
    pstate->tuple.typbyval = (bool *)palloc(sizeof(bool) * pstate->tuple.ncolumns);
    pstate->tuple.typlen = (int16 *)palloc(sizeof(int16) * pstate->tuple.ncolumns);
    for (int c = 0; c < pstate->tuple.ncolumns; c++) {
        pstate->tuple.typid[c] = tupdesc->attrs[c]->atttypid;
        pstate->tuple.typbyval[c] = tupdesc->attrs[c]->attbyval;
        pstate->tuple.typlen[c] = tupdesc->attrs[c]->attlen;
    }

    pstate->row_allocated = false;
    pstate->fetch = ExecFetchTrainModel;
    pstate->rescan = ExecReScanTrainModel;
    pstate->callback_data = pstate;

    // Output tuple
    TupleDesc tup_desc_out = CreateTemplateTupleDesc(1, false);
    TupleDescInitEntry(tup_desc_out, (AttrNumber)1, "model", BYTEARRAYOID, -1, 0);
    BlessTupleDesc(tup_desc_out);
    ExecAssignResultType(&pstate->ss.ps, tup_desc_out);
    ExecAssignProjectionInfo(&pstate->ss.ps, nullptr);
    pstate->ss.ps.ps_TupFromTlist = false;
    pstate->ss.ps.ps_ProjInfo = nullptr;
    
    return pstate;
}

TupleTableSlot* ExecTrainModel(TrainModelState* pstate)
{
    // check if already finished
    if (pstate->finished == pstate->config->configurations)
        return NULL;
    
    // If backwards scan, just return NULL without changing state
    if (!ScanDirectionIsForward(pstate->ss.ps.state->es_direction))
        return NULL;
    
    MemoryContext oldcxt = MemoryContextSwitchTo(pstate->config->cxt);
    Model *model = nullptr;
    model = (Model *)palloc0(sizeof(Model));
    model->status = ERRCODE_INVALID_STATUS;
    model->memory_context = pstate->config->cxt;
    MemoryContextSwitchTo(oldcxt);
    
    Assert(pstate->algorithm->run != nullptr);
    pstate->algorithm->run(pstate->algorithm, pstate, &model);
    if (model->status != ERRCODE_SUCCESSFUL_COMPLETION) {
        MemoryContextSwitchTo(pstate->config->cxt);
        pfree(model);
        MemoryContextSwitchTo(oldcxt);
        return NULL;
    }
    
    TupleTableSlot *slot = pstate->ss.ps.ps_ResultTupleSlot;
    Datum *values = slot->tts_values;
    values[0] = PointerGetDatum(model);
    ExecClearTuple(slot);
    ExecStoreVirtualTuple(slot);
    
    return slot;
}

void ExecEndTrainModel(TrainModelState* pstate)
{
    AlgorithmAPI *palgo = get_algorithm_api(pstate->config->algorithm);
    Assert(palgo->end != nullptr);
    palgo->end(palgo, pstate);
    
    if (pstate->row_allocated) {
        pfree(pstate->tuple.values);
        pfree(pstate->tuple.isnull);
    }
    pfree(pstate->tuple.typid);
    pfree(pstate->tuple.typbyval);
    pfree(pstate->tuple.typlen);
    
    ExecClearTuple(pstate->ss.ps.ps_ResultTupleSlot);
    
    ExecFreeExprContext(&pstate->ss.ps);
    ExecEndNode(outerPlanState(pstate));
    pfree(pstate);
}

