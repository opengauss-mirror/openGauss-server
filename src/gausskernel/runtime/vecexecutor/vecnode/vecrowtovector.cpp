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
 * vecrowtovector.cpp
 *    Convert underlying iterator row output to vector batch
 *
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecrowtovector.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/tableam.h"
#include "executor/executor.h"
#include "vecexecutor/vecnoderowtovector.h"
#include "utils/memutils.h"
#include "catalog/pg_type.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "storage/item/itemptr.h"

static void CheckTypeSupportRowToVec(List* targetlist);

/*
 * @Description: Pack one tuple into vectorbatch.
 *
 * @IN pBatch: Target vectorized data.
 * @IN slot:   source data of one slot.
 * @IN transformContext: switch to this context to avoid memory leak.
 * @return: Return true if pBatch is full, else return false.
 */
bool VectorizeOneTuple(_in_ VectorBatch* pBatch, _in_ TupleTableSlot* slot, _in_ MemoryContext transformContext)
{
    bool may_more = false;
    int i, j;

    /* Switch to Current Transfform Context */
    MemoryContext old_context = MemoryContextSwitchTo(transformContext);

    /*
     * Extract all the values of the old tuple.
     */
    Assert(slot != NULL && slot->tts_tupleDescriptor != NULL);

    tableam_tslot_getallattrs(slot);

    j = pBatch->m_rows;
    for (i = 0; i < slot->tts_nvalid; i++) {
        int type_len;
        Form_pg_attribute attr = slot->tts_tupleDescriptor->attrs[i];

        pBatch->m_arr[i].m_desc.typeId = attr->atttypid;

        if (slot->tts_isnull[i] == false) {
            type_len = attr->attlen;
            switch (type_len) {
                case sizeof(char):
                case sizeof(int16):
                case sizeof(int32):
                case sizeof(Datum):
                    pBatch->m_arr[i].m_vals[j] = slot->tts_values[i];
                    break;
                case 12:
                case 16:
                case 64:
                case -2:
                    pBatch->m_arr[i].AddVar(slot->tts_values[i], j);
                    break;
                case -1: {
                    Datum v = PointerGetDatum(PG_DETOAST_DATUM(slot->tts_values[i]));
                    /* if numeric cloumn, try to convert numeric to big integer */
                    if (attr->atttypid == NUMERICOID) {
                        v = try_convert_numeric_normal_to_fast(v);
                    }
                    pBatch->m_arr[i].AddVar(v, j);
                    /* because new memory may be created, so we have to check and free in time. */
                    if (DatumGetPointer(slot->tts_values[i]) != DatumGetPointer(v)) {
                        pfree(DatumGetPointer(v));
                    }
                    break;
                }
                case 6:
                    if (attr->atttypid == TIDOID && attr->attbyval == false) {
                        pBatch->m_arr[i].m_vals[j] = 0;
                        ItemPointer dest_tid = (ItemPointer)(pBatch->m_arr[i].m_vals + j);
                        ItemPointer src_tid = (ItemPointer)DatumGetPointer(slot->tts_values[i]);
                        *dest_tid = *src_tid;
                    } else {
                        pBatch->m_arr[i].AddVar(slot->tts_values[i], j);
                    }
                    break;
                default:
                    ereport(ERROR, (errcode(ERRCODE_INDETERMINATE_DATATYPE), errmsg("unsupported datatype branch")));
            }

            SET_NOTNULL(pBatch->m_arr[i].m_flag[j]);
        } else {
            SET_NULL(pBatch->m_arr[i].m_flag[j]);
        }
    }

    pBatch->m_rows++;
    if (pBatch->m_rows == BatchMaxSize) {
        may_more = true;
    }

    /* Switch to OldContext */
    (void)MemoryContextSwitchTo(old_context);

    return may_more;
}

/*
 * @Description: Vectorized Operator--Convert row data to vector batch.
 *
 * @IN state: Row To Vector State.
 * @return: Return the batch of row table data, return NULL otherwise.
 */
VectorBatch* ExecRowToVec(RowToVecState* state)
{
    int i;
    PlanState* outer_plan = NULL;
    TupleTableSlot* outer_slot = NULL;
    VectorBatch* batch = state->m_pCurrentBatch;

    /* Reset Current ecxt_per_tuple_memory Context */
    ExprContext* econtext = state->ps.ps_ExprContext;
    ResetExprContext(econtext);

    /* Get state info from node */
    outer_plan = outerPlanState(state);
    batch->Reset();

    /*
     * ExecProcNode() may restart if we invoke it after it return NULL
     * so we have to guard it ourselves.
     */
    if (state->m_fNoMoreRows) {
        goto done;
    }

    /*
     * Process each outer-plan tuple, and then fetch the next one, until we
     * exhaust the outer plan.
     */
    for (;;) {
        outer_slot = ExecProcNode(outer_plan);
        if (TupIsNull(outer_slot)) {
            state->m_fNoMoreRows = true;
            break;
        }

        /*
         * Vectorize one tuple and switch to ecxt_per_tuple_memory of
         * exprcontext.
         */
        if (VectorizeOneTuple(batch, outer_slot, econtext->ecxt_per_tuple_memory)) {
            /* It is full now, now return current batch */
            break;
        }
    }

done:
    for (i = 0; i < batch->m_cols; i++) {
        batch->m_arr[i].m_rows = batch->m_rows;
    }

    return batch;
}

RowToVecState* ExecInitRowToVec(RowToVec* node, EState* estate, int eflags)
{
    RowToVecState* state = NULL;

    /*
     * create state structure
     */
    state = makeNode(RowToVecState);
    state->ps.plan = (Plan*)node;
    state->ps.state = estate;
    state->ps.vectorized = true;

    CheckTypeSupportRowToVec(node->plan.targetlist);

    /*
     * tuple table initialization
     *
     * sort nodes only return scan tuples from their sorted relation.
     */
    ExecInitResultTupleSlot(estate, &state->ps);

    /* Allocate vector buffers */
    state->m_fNoMoreRows = false;

    /*
     * initialize child nodes
     *
     * We shield the child node from the need to support REWIND, BACKWARD, or
     * MARK/RESTORE.
     */
    outerPlanState(state) = ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &state->ps);

    /*
     * initialize tuple type.  no need to initialize projection info because
     * this node doesn't do projections.
     */
    ExecAssignResultTypeFromTL(
            &state->ps,
            ExecGetResultType(outerPlanState(state))->tdTableAmType);

    TupleDesc res_desc = state->ps.ps_ResultTupleSlot->tts_tupleDescriptor;
    state->m_pCurrentBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, res_desc);
    state->ps.ps_ProjInfo = NULL;

    return state;
}

void ExecEndRowToVec(RowToVecState* node)
{
    node->m_pCurrentBatch = NULL;

    /*
     * We don't actually free any ExprContexts here (see comment in
     * ExecFreeExprContext), just unlinking the output one from the plan node
     * suffices.
     */
    ExecFreeExprContext(&node->ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);

    /*
     * shut down subplans
     */
    ExecEndNode(outerPlanState(node));
}

void ExecReScanRowToVec(RowToVecState* node)
{
    node->m_fNoMoreRows = false;
    node->m_pCurrentBatch->m_rows = 0;
    ExecReScan(node->ps.lefttree);
}

/*
 * Check if there is any data type unsupported by cstore. If so, stop rowtovec
 */
static void CheckTypeSupportRowToVec(List* targetlist)
{
    ListCell* cell = NULL;
    TargetEntry* entry = NULL;
    Var* var = NULL;
    foreach(cell, targetlist) {
        entry = (TargetEntry*)lfirst(cell);
        if (IsA(entry->expr, Var)) {
            var = (Var*)entry->expr;
            if (var->varattno > 0 && var->varoattno > 0 && !IsTypeSupportedByCStore(var->vartype, var->vartypmod)) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("type \"%s\" is not supported in column store",
                        format_type_with_typemod(var->vartype, var->vartypmod))));
            }
        }
    }
}
