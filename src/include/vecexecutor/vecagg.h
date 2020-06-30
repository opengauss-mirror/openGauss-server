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
 * vecagg.h
 *
 *
 * IDENTIFICATION
 *        src/include/vecexecutor/vecagg.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECAGG_H_
#define VECAGG_H_

#include "vecexecutor/vechashtable.h"
#include "vecexecutor/vecnodes.h"
#include "utils/batchsort.h"

extern VecAggState* ExecInitVecAggregation(VecAgg* node, EState* estate, int eflags);
extern VectorBatch* ExecVecAggregation(VecAggState* node);
extern void ExecEndVecAggregation(VecAggState* node);
extern void ExecReScanVecAggregation(VecAggState* node);
extern void ExecEarlyFreeVecAggregation(VecAggState* node);

#define AGG_PREPARE 0
#define AGG_BUILD 1
#define AGG_FETCH 2
#define AGG_RETURN 3
#define AGG_RETURN_LAST 4
#define AGG_RETURN_NULL 5

struct finalAggInfo {
    int idx;
    VecAggInfo* info;
};

typedef struct SortDistinct {
    Batchsortstate** batchsortstate;
    VectorBatch** aggDistinctBatch;
    ScalarValue* lastVal;
    hashCell* m_distinctLoc;
    int* lastValLen;
} SortDistinct;

class BaseAggRunner : public hashBasedOperator {
public:
    BaseAggRunner(VecAggState* runtime, bool is_hash_agg);
    BaseAggRunner();

    void init_aggInfo(int agg_num, VecAggInfo* agg_info);

    virtual bool ResetNecessary(VecAggState* node) = 0;

    virtual VectorBatch* Run() = 0;

    void (BaseAggRunner::*m_buildScanBatch)(hashCell* cell);

    void init_index();

    template <bool simple, bool sortAgg>
    void initCellValue(VectorBatch* batch, hashCell* cell, int idx);

    void BatchAggregation(VectorBatch* batch);

    template <bool simpleKey>
    bool match_key(VectorBatch* batch, int batch_idx, hashCell* cell);

    void AggregationOnScalar(VecAggInfo* agg_info, ScalarVector* p_vector, int idx, hashCell** location);

    // build scan batch
    void BuildScanBatchSimple(hashCell* cell);

    void BuildScanBatchFinal(hashCell* cell);

    // build result batch
    VectorBatch* ProducerBatch();

    /* Filter repeat value which be used when agg need distinct.*/
    bool FilterRepeatValue(FmgrInfo* equalfns, int curr_set, int aggno, bool first);

    /*Add for sort agg*/
    void BatchSortAggregation(int i, int work_mem, int max_mem, int plan_id, int dop);

    void initialize_sortstate(int work_mem, int max_mem, int plan_id, int dop);

    void build_batch();

    /*keep last values, when need distinct operation. This value need compare with the values of next batch*/
    void copyLastValues(Oid type_oid, ScalarValue m_vals, int aggno, int curr_set);

    void BatchNoSortAgg(VectorBatch* batch);

    void AppendBatchForSortAgg(VectorBatch* batch, int start, int end, int curr_set);

public:
    VecAggState* m_runtime;

    int m_cellVarLen;
    // how many aggregation function (column)
    int m_aggNum;

    bool* m_aggCount;

    // the agg index in the tuple,as some agg function may split into two function
    // e.g avg
    int* m_aggIdx;

    int* m_cellBatchMap;

    int* m_keyIdxInCell;

    bool m_finish;

    bool m_hasDistinct;

    int m_keySimple;  // whether the key is simple. 1:is simple 2: not simple
                      // simple means there is no variable length data type or complex expression evaluation.

    // the index which need do some final calc.
    finalAggInfo* m_finalAggInfo;

    int m_finalAggNum;

    // Batch for scan the hash table.
    VectorBatch* m_scanBatch;

    // projection batch. The projection code binds column offset with outer batch, so we
    // shall keep the outer batch to leverage it. Notice that we have actually copied
    // accessible columns to the hash table already (due to aggregation semantics), so
    // we simply use outer batch offset only.
    //
    VectorBatch* m_proBatch;

    // same structure as outer node return batch
    VectorBatch* m_outerBatch;

    // runtime state.
    int m_runState;

    // the location for entry to be insert
    hashCell* m_Loc[BatchMaxSize];

    SortDistinct* m_sortDistinct;

    VarBuf* m_sortCurrentBuf;

    int m_projected_set;

    /* expression-evaluation context */
    ExprContext* m_econtext;

private:
    void init_Baseindex();
    void prepare_projection_batch();

    virtual void BindingFp() = 0;
};

#define DO_AGGEGATION(tbl) (((BaseAggRunner*)tbl)->Run)()

template <bool simple, bool sort_agg>
void BaseAggRunner::initCellValue(VectorBatch* batch, hashCell* cell, int idx)
{
    int k;
    ScalarVector* p_vector = NULL;

    for (k = 0; k < m_cellVarLen; k++) {
        p_vector = &batch->m_arr[m_cellBatchMap[k]];

        if (simple || p_vector->m_desc.encoded == false) {
            cell->m_val[k].val = p_vector->m_vals[idx];
            cell->m_val[k].flag = p_vector->m_flag[idx];
        } else {
            if (likely(p_vector->IsNull(idx) == false)) {
                /* For hashagg, CurrentMemoryContext is hashcell context */
                cell->m_val[k].val = sort_agg ? addToVarBuffer(m_sortCurrentBuf, p_vector->m_vals[idx])
                                             : addVariable(CurrentMemoryContext, p_vector->m_vals[idx]);
                if (!sort_agg && m_tupleCount >= 0)
                    m_colWidth += VARSIZE_ANY(p_vector->m_vals[idx]);
            }

            cell->m_val[k].flag = p_vector->m_flag[idx];
        }
    }

    // set the init value.
    for (k = 0; k < m_aggNum; k++) {
        if (m_aggCount[k]) {
            cell->m_val[m_aggIdx[k]].val = 0;
            SET_NOTNULL(cell->m_val[m_aggIdx[k]].flag);
        } else
            SET_NULL(cell->m_val[m_aggIdx[k]].flag);
    }
}

template <bool simple_key>
bool BaseAggRunner::match_key(VectorBatch* batch, int batch_idx, hashCell* cell)
{
    bool match = true;
    int i;
    ScalarVector* p_vector = NULL;
    hashVal* hash_val = NULL;
    FunctionCallInfoData fcinfo;
    Datum args[2];

    fcinfo.arg = &args[0];

    for (i = 0; i < m_key; i++) {
        p_vector = &batch->m_arr[m_keyIdx[i]];
        hash_val = &cell->m_val[m_keyIdxInCell[i]];

        if (BOTH_NOT_NULL(p_vector->m_flag[batch_idx], hash_val->flag)) {
            if (simple_key || p_vector->m_desc.encoded == false) {
                if (p_vector->m_vals[batch_idx] == hash_val->val)
                    continue;
                else
                    return false;
            } else {
                fcinfo.arg[0] = ScalarVector::Decode(p_vector->m_vals[batch_idx]);
                fcinfo.arg[1] = ScalarVector::Decode(hash_val->val);
                match = m_eqfunctions[i].fn_addr(&fcinfo);
                if (match == false)
                    return false;
            }
        } else if (IS_NULL(p_vector->m_flag[batch_idx]) && IS_NULL(hash_val->flag))  // both null is equal
            continue;
        else if (IS_NULL(p_vector->m_flag[batch_idx]) || IS_NULL(hash_val->flag))
            return false;
    }

    return true;
}

#endif
