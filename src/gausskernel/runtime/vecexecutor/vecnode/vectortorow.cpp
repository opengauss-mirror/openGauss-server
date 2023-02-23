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
 * vectortorow.cpp
 *     convert underlying vector oriented node output to row oriented
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vectortorow.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "utils/biginteger.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "fmgr.h"
#include "vecexecutor/vecnodevectorow.h"
#include "executor/executor.h"
#include "utils/memutils.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "access/cstore_am.h"
#include "vecexecutor/vecnodes.h"
#include "vecexecutor/vectorbatch.h"
#include "vecexecutor/vecexecutor.h"
#include "storage/item/itemptr.h"


static TupleTableSlot* ExecVecToRow(PlanState* pstate);

/* Convert one column of the entire batch from vector store to row store.
 * typid in template is the OID of the column data type. */
template <int typid>
void DevectorizeOneColumn(VecToRowState* state, ScalarVector* pColumn, int rows, int cols, int i)
{
    int k;
    ScalarValue* val_arr = pColumn->m_vals;
    ScalarValue val;
    Datum tmp;

    /* Loop to get every value of the column in this batch. */
    for (int j = 0; j < rows; j++) {
        k = j * cols + i;

        if (state->m_ttsisnull[k])
            continue;

        val = val_arr[j];

        switch (typid) {
            case VARCHAROID:
                state->m_ttsvalues[k] = ScalarVector::Decode(val_arr[j]);
                break;
            case TIMETZOID: {
                tmp = ScalarVector::Decode(val);
                char* result = NULL;

                result = (char*)tmp + VARHDRSZ_SHORT;

                state->m_ttsvalues[k] = PointerGetDatum(result);
            } break;
            case TIDOID: {
                state->m_ttsvalues[k] = PointerGetDatum(val_arr + j);
                break;
            }
            case NAMEOID: {
                state->m_ttsvalues[k] = PointerGetDatum(VARDATA_ANY(val));
                break;
            }
            case UNKNOWNOID: {
                tmp = ScalarVector::Decode(val);
                char* result = NULL;
                if (VARATT_IS_1B(tmp))
                    result = (char*)tmp + VARHDRSZ_SHORT;
                else
                    result = (char*)tmp + VARHDRSZ;
                state->m_ttsvalues[k] = PointerGetDatum(result);
            } break;
            /* case -2: represent type that  is encode, not inline */
            case -2:
                state->m_ttsvalues[k] = ScalarVector::Decode(val);
                break;
            default:
                state->m_ttsvalues[k] = (Datum)val;
                break;
        }
    }
}

/* Convert the entire batch from vector store to row store. */
void DevectorizeOneBatch(VecToRowState* state)
{
    int i;
    int j;
    int rows;
    int cols;
    VectorBatch* current_batch = NULL;
    ScalarVector* column = NULL;
    MemoryContext old_context;

    current_batch = state->m_pCurrentBatch;
    rows = current_batch->m_rows;
    cols = state->nattrs;

    /* Allocate memory for m_ttsvalues, which is for storing the column values; and
     * for m_ttsisnull, which is to indicate if the column value is null. Both are
     * placed in VecToRowState. */
    if (state->m_ttsvalues == NULL) {
        state->m_ttsvalues = (Datum*)palloc(sizeof(Datum) * cols * BatchMaxSize);
        state->m_ttsisnull = (bool*)palloc(sizeof(bool) * cols * BatchMaxSize);
    }

    /* Loop to process the entire batch, column by column. */
    ExprContext* econtext = state->ps.ps_ExprContext;
    ResetExprContext(econtext);

    old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    for (i = 0; i < cols; i++) {
        column = &current_batch->m_arr[i];

        /* handle the case of Const; also handle case of NULL. */
        for (j = 0; j < rows; j++)
            state->m_ttsisnull[j * cols + i] = IS_NULL(column->m_flag[j]);

        state->devectorizeFunRuntime[i](state, column, rows, cols, i);
    }

    (void)MemoryContextSwitchTo(old_context);
    return;
}

static TupleTableSlot* ExecVecToRow(PlanState* pstate) /* return: a tuple or NULL */
{
    VecToRowState* state = castNode(VecToRowState, pstate);
    PlanState* outer_plan = NULL;
    TupleTableSlot* tuple = state->tts;
    VectorBatch* current_batch = NULL;
    int tuple_subscript;

    current_batch = state->m_pCurrentBatch;
    if (BatchIsNull(current_batch)) {
        outer_plan = outerPlanState(state);
        current_batch = VectorEngine(outer_plan);
        if (BatchIsNull(current_batch))  // no more rows
            return NULL;

        state->m_pCurrentBatch = current_batch;
        state->m_currentRow = 0;
        // Convert the batch into row based tuple
        DevectorizeOneBatch(state);
        outer_plan->ps_rownum += current_batch->m_rows;
    }

    // retrieve rows from current batch
    tuple_subscript = state->m_currentRow * state->nattrs;
    (void)ExecClearTuple(tuple);
    for (int i = 0; i < state->nattrs; i++) {
        tuple->tts_values[i] = state->m_ttsvalues[tuple_subscript + i];
        tuple->tts_isnull[i] = state->m_ttsisnull[tuple_subscript + i];
    }
    state->m_currentRow++;

    if (state->m_currentRow >= current_batch->m_rows) {
        // make it empty as all rows in the batch done
        current_batch->m_rows = 0;
        state->m_currentRow = 0;
    }

    ExecStoreVirtualTuple(tuple);
    return tuple;
}

void RecordCstorePartNum(VecToRowState* state, const VecToRow* node)
{
    // record partition num.If there is no partition table, it is set to 0
    if (unlikely(IS_PGXC_DATANODE && NeedStubExecution(outerPlan(node)))) {
        state->part_id = 0;
    } else {
        switch (nodeTag(outerPlan(node))) {
            case T_CStoreScan:
            case T_CStoreIndexScan:
            case T_CStoreIndexCtidScan:
            case T_CStoreIndexHeapScan:
                state->part_id = ((ScanState*)outerPlanState(state))->part_id;
                state->subpartitions = ((ScanState*)outerPlanState(state))->subpartitions;
                state->subPartLengthList = ((ScanState*)outerPlanState(state))->subPartLengthList;
                break;

#ifdef ENABLE_MULTIPLE_NODES
            case T_TsStoreScan:
                state->part_id = ((TsStoreScanState*)outerPlanState(state))->part_id;
                break;
#endif

            default:
                state->part_id = 0;
                break;
        }
    }
}

VecToRowState* ExecInitVecToRow(VecToRow* node, EState* estate, int eflags)
{
    VecToRowState* state = NULL;

    /*
     * create state structure
     */
    state = makeNode(VecToRowState);
    state->ps.plan = (Plan*)node;
    state->ps.state = estate;
    state->ps.vectorized = false;
    state->ps.ExecProcNode = ExecVecToRow;

    /*
     * tuple table initialization
     *
     * sort nodes only return scan tuples from their sorted relation.
     */
    ExecInitResultTupleSlot(estate, &state->ps);

    /*
     * initialize child nodes
     *
     * We shield the child node from the need to support REWIND, BACKWARD, or
     * MARK/RESTORE.
     */
    if ((uint32)eflags & EXEC_FLAG_BACKWARD)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("column store doesn't support backward scan")));
    outerPlanState(state) = ExecInitNode(outerPlan(node), estate, eflags);

    RecordCstorePartNum(state, node);

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
            ExecGetResultType(outerPlanState(state))->td_tam_ops);

    state->ps.ps_ProjInfo = NULL;
    state->m_currentRow = 0;
    state->m_pCurrentBatch = NULL;
    state->nattrs = ExecGetResultType(&state->ps)->natts;
    state->tts = state->ps.ps_ResultTupleSlot;
    (void)ExecClearTuple(state->tts);
    state->tts->tts_nvalid = state->nattrs;
    state->tts->tts_flags &= ~TTS_FLAG_EMPTY;
    state->devectorizeFunRuntime = (DevectorizeFun*)palloc0(state->nattrs * sizeof(DevectorizeFun));
    for (int i = 0; i < state->nattrs; i++) {
        state->tts->tts_isnull[i] = false;
        int type_id = state->tts->tts_tupleDescriptor->attrs[i].atttypid;
        if (COL_IS_ENCODE(type_id)) {
            switch (type_id) {
                case BPCHAROID:
                case TEXTOID:
                case VARCHAROID:
                    state->devectorizeFunRuntime[i] = DevectorizeOneColumn<VARCHAROID>;
                    break;
                case NAMEOID:
                    state->devectorizeFunRuntime[i] = DevectorizeOneColumn<NAMEOID>;
                    break;
                case TIMETZOID:
                case TINTERVALOID:
                case INTERVALOID:
                case MACADDROID:
                case UUIDOID:
                    state->devectorizeFunRuntime[i] = DevectorizeOneColumn<TIMETZOID>;
                    break;
                case UNKNOWNOID:
                case CSTRINGOID:
                    state->devectorizeFunRuntime[i] = DevectorizeOneColumn<UNKNOWNOID>;
                    break;
                default:
                    state->devectorizeFunRuntime[i] = DevectorizeOneColumn<-2>;
                    break;
            }
        } else {
            if (type_id == TIDOID)
                state->devectorizeFunRuntime[i] = DevectorizeOneColumn<TIDOID>;
            else
                state->devectorizeFunRuntime[i] = DevectorizeOneColumn<-1>;
        }
    }

    state->m_ttsvalues = NULL;
    state->m_ttsisnull = NULL;

    return state;
}

void ExecEndVecToRow(VecToRowState* node)
{
    // clean out the tuple table
    //
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);

    // shut down subplans
    //
    ExecEndNode(outerPlanState(node));
}

void ExecReScanVecToRow(VecToRowState* node)
{
    node->m_currentRow = 0;
    node->m_pCurrentBatch = NULL;

    VecExecReScan(node->ps.lefttree);
}
