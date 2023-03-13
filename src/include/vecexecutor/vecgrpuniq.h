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
 * vecgrpuniq.h
 *     Prototypes for vectorized unique and group
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecgrpuniq.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECGRPUNIQ_H
#define VECGRPUNIQ_H
#include "vecexecutor/vectorbatch.h"
#include "vecexecutor/vecexecutor.h"

// The value of cell which is contained in GUCell
//
struct GUCellVal {
    ScalarValue val;
    uint8 flag;
};

// The container to store cells which are used to produce outer batch
//
struct GUCell {
    int dummy; /* a dummy field to avoid compile error for sigle var-len array in an empty struct*/
    GUCellVal val[FLEXIBLE_ARRAY_MEMBER];
};

// The encapsulation of VectorBatch, cols and colIdx. Use this to reduce the parameter number,
// create and fill it in vecgroup and vecunique seperately to avoid using if in template because
// of the different name of colIdx(grpColIdx, uniqColIdx).
//
struct Encap {
    VectorBatch* batch;
    int cols;
    int outerCols;
    AttrNumber* colIdx;
    FmgrInfo* eqfunctions;
};

inline ScalarValue AddVar(VarBuf* buf, ScalarValue value)
{
    return PointerGetDatum(buf->Append(DatumGetPointer(value), VARSIZE_ANY(value)));
}

template <bool simple>
static void InitCellValue(Encap* cap, GUCell* cell, int idx, VarBuf* currentBuf)
{
    int k = 0;
    ScalarVector* pVector = NULL;
    VectorBatch* batch = cap->batch;
    ScalarVector* arr = batch->m_arr;
    GUCellVal* val = cell->val;
    int outerCols = cap->outerCols;

    for (k = 0; k < outerCols; k++) {
        pVector = &arr[k];
        if (simple || pVector->m_desc.encoded == false) {
            val[k].val = pVector->m_vals[idx];
            val[k].flag = pVector->m_flag[idx];
        } else {
            if (likely(pVector->IsNull(idx) == false))
                val[k].val = AddVar(currentBuf, pVector->m_vals[idx]);

            val[k].flag = pVector->m_flag[idx];
        }
    }
}

template <bool simpleKey>
static bool Match(Encap* cap, int batchIdx, GUCell* cell)
{
    bool match = true;
    int i = 0;
    ScalarVector* pVector = NULL;
    ScalarDesc desc;
    GUCellVal* cellVal = NULL;
    Datum key1;
    Datum key2;
    ScalarVector* arr = cap->batch->m_arr;
    GUCellVal* val = cell->val;
    int cols = cap->cols;
    AttrNumber* colIdx = cap->colIdx;
    FmgrInfo* eqfuncs = cap->eqfunctions;

    for (i = 0; i < cols; i++) {
        pVector = &arr[colIdx[i] - 1];
        cellVal = &val[colIdx[i] - 1];
        desc = pVector->m_desc;

        if (BOTH_NOT_NULL(pVector->m_flag[batchIdx], cellVal->flag)) {
            if (simpleKey || desc.encoded == false) {
                if (pVector->m_vals[batchIdx] == cellVal->val)
                    continue;
                else
                    return false;
            } else {
                key1 = ScalarVector::Decode(pVector->m_vals[batchIdx]);
                key2 = ScalarVector::Decode(cellVal->val);

                match = DatumGetBool(FunctionCall2((eqfuncs + i), key1, key2));
                if (match == false)
                    return false;
            }
        }
        // both null is equal
        else if (IS_NULL(pVector->m_flag[batchIdx]) && IS_NULL(cellVal->flag))
            continue;
        else if (IS_NULL(pVector->m_flag[batchIdx]) || IS_NULL(cellVal->flag))
            return false;
    }

    return true;
}

// Build the container filled with cells which not match others.
//
template <bool simple, typename T>
Datum BuildFunc(PG_FUNCTION_ARGS)
{
    T* node = (T*)PG_GETARG_DATUM(0);
    Encap* cap = (Encap*)PG_GETARG_DATUM(1);
    VectorBatch* batch = cap->batch;
    int i = 0;
    int rows = batch->m_rows;
    int cellSize = node->cellSize;
    GUCell* cell = NULL;
    void** container = node->container;
    VarBuf* currentBuf = node->currentBuf;
    uint16* idx = &(node->idx);

    for (i = 0; i < rows; i++) {
        cell = (GUCell*)container[*idx];
        if (cell == NULL || !Match<simple>(cap, i, cell)) {
            // means we got match fail
            if (cell != NULL) {
                (*idx)++;
                if (*idx == BatchMaxSize) {
                    // switch  buffer
                    VarBuf* tmp;
                    tmp = node->currentBuf;
                    node->currentBuf = node->bckBuf;
                    node->bckBuf = tmp;
                    currentBuf = node->currentBuf;
                }
            }
            cell = (GUCell*)currentBuf->Allocate(cellSize);
            container[*idx] = cell;

            InitCellValue<simple>(cap, cell, i, currentBuf);
        }
    }

    PG_FREE_IF_COPY(node, 0);
    PG_FREE_IF_COPY(cap, 1);

    PG_RETURN_DATUM(0);
}

Datum BuildScanBatch(PG_FUNCTION_ARGS);

template <typename T>
static void BindingFp(T* node)
{
    node->buildScanFunc->fn_addr = BuildScanBatch;
    if (node->keySimple) {
        node->buildFunc->fn_addr = BuildFunc<true, T>;
    } else {
        node->buildFunc->fn_addr = BuildFunc<false, T>;
    }
}

template <typename T>
static void ReplaceGrpUniqEqfunc(Encap* cap, FormData_pg_attribute* attrs)
{
    AttrNumber* colIdx = cap->colIdx;
    FmgrInfo* eqfunctions = cap->eqfunctions;

    for (int i = 0; i < cap->cols; i++) {
        switch (attrs[colIdx[i] - 1].atttypid) {
            case TIMETZOID:
                eqfunctions[i].fn_addr = timetz_eq_withhead;
                break;
            case TINTERVALOID:
                eqfunctions[i].fn_addr = tintervaleq_withhead;
                break;
            case INTERVALOID:
                eqfunctions[i].fn_addr = interval_eq_withhead;
                break;
            case NAMEOID:
                eqfunctions[i].fn_addr = nameeq_withhead;
                break;
            default:
                break;
        }
    }
}

template <typename T>
void InitGrpUniq(T* state, int cols, AttrNumber* colIdx)
{
    int i = 0;
    Encap* cap = (Encap*)state->cap;

    // Initialize the variables used in Vector execution process and bind the function in FmgrInfo.
    state->idx = 0;
    state->keySimple = true;
    state->container = (void**)palloc0(2 * BatchMaxSize * sizeof(GUCell*));

    TupleDesc outDesc = outerPlanState(state)->ps_ResultTupleSlot->tts_tupleDescriptor;
    state->cellSize = offsetof(GUCell, val) + outDesc->natts * sizeof(GUCellVal);
    cap->cols = cols;
    cap->outerCols = outDesc->natts;
    cap->colIdx = colIdx;
    cap->eqfunctions = state->eqfunctions;
    FormData_pg_attribute* attrs = outDesc->attrs;

    for (i = 0; i < cap->outerCols; i++) {
        if (COL_IS_ENCODE(attrs[i].atttypid)) {
            state->keySimple = false;
            break;
        }
    }

    ReplaceGrpUniqEqfunc<T>(cap, attrs);

    state->scanBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, outDesc);
    state->currentBuf = New(CurrentMemoryContext) VarBuf(CurrentMemoryContext);
    state->currentBuf->Init();
    state->bckBuf = New(CurrentMemoryContext) VarBuf(CurrentMemoryContext);
    state->bckBuf->Init();
    state->buildFunc = (FmgrInfo*)palloc0(sizeof(FmgrInfo));
    state->buildScanFunc = (FmgrInfo*)palloc0(sizeof(FmgrInfo));
    BindingFp<T>(state);
}

template <typename T>
void ReScanGrpUniq(T* node)
{
    errno_t rc;
    // clear the container and buffers which will be used in the next scan with a clean status.
    rc = memset_s(node->container, 2 * BatchMaxSize * sizeof(GUCell*), 0, 2 * BatchMaxSize * sizeof(GUCell*));
    securec_check(rc, "\0", "\0");
    node->idx = 0;
    node->currentBuf->Reset();
}

#endif /* VECGRPUNIQ_H */
