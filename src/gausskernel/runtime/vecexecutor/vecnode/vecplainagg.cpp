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
 * vecplainagg.cpp
 *       Routines to handle vector plain aggregae nodes.
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecplainagg.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/node/nodeAgg.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "utils/datum.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "vecexecutor/vechashagg.h"
#include "vecexecutor/vechashtable.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/vecfunc.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "nodes/execnodes.h"
#include "pgxc/pgxc.h"
#include "utils/int8.h"
#include "vecexecutor/vecplainagg.h"

void PlainAggRunner::BindingFp()
{
    if (m_hasDistinct) {
        plainAggregation = &PlainAggRunner::RunPlain<true>;
    } else {
        plainAggregation = &PlainAggRunner::RunPlain<false>;
    }
}

/*
 * @Description: plain Agg constructed function.
 */
PlainAggRunner::PlainAggRunner(VecAggState* runtime) : BaseAggRunner(runtime, false)
{
    hashCell* cell = NULL;
    Assert(m_key == 0);

    m_runtime->phase = &m_runtime->phases[m_runtime->current_phase];
    /* Init sort state to distinct operate. count(distinct) e.g */
    initialize_sortstate(
        u_sess->attr.attr_memory.work_mem, 0, runtime->ss.ps.plan->plan_node_id, SET_DOP(runtime->ss.ps.plan->dop));

    m_hashTbl = New(CurrentMemoryContext) vechashtable(Max(m_runtime->phase->numsets, 1));

    for (int i = 0; i < m_hashTbl->m_size; i++) {
        cell = (hashCell*)palloc0(m_cellSize);
        m_hashTbl->m_data[i] = cell;
        cell->flag.m_next = NULL;

        /* set the init value. */
        for (int k = 0; k < m_aggNum; k++) {
            if (m_aggCount[k]) {
                cell->m_val[m_aggIdx[k]].val = 0;
                SET_NOTNULL(cell->m_val[m_aggIdx[k]].flag);
            } else
                SET_NULL(cell->m_val[m_aggIdx[k]].flag);
        }
    }

    BindingFp();
}

/*
 * @Description: compute agg.
 */
template <bool hasDistinct>
void PlainAggRunner::buildPlaintAgg(VectorBatch* outer_batch, bool first_batch)
{
    ScalarVector* p_vector = NULL;
    hashCell* cell = NULL;
    hashCell** hash_data = m_hashTbl->m_data;

    int numset = Max(m_runtime->phase->numsets, 1);

    for (int i = 0; i < numset; i++) {
        cell = hash_data[i];
        for (int k = 0; k < BatchMaxSize; k++) {
            m_Loc[k] = cell;
        }

        if (m_cellVarLen > 0 && first_batch) {
            for (int k = 0; k < m_cellVarLen; k++) {
                p_vector = &outer_batch->m_arr[m_cellBatchMap[k]];
                cell->m_val[k].val = p_vector->m_vals[0];
                cell->m_val[k].flag = p_vector->m_flag[0];
            }
        }

        if (hasDistinct) {
            m_sortDistinct[i].m_distinctLoc = cell;
            AppendBatchForSortAgg(outer_batch, 0, outer_batch->m_rows, i);
            BatchNoSortAgg(outer_batch);
        } else {
            BatchAggregation(outer_batch);
        }
    }
}

/*
 * @Description: get data, compute agg and return result.
 */
template <bool hasDistinct>
VectorBatch* PlainAggRunner::RunPlain()
{
    if (m_finish) {
        return NULL;
    }

    VectorBatch* outer_batch = NULL;
    VectorBatch* res_batch = NULL;
    PlanState* outPlan = NULL;
    bool first_batch = true;
    hashCell** hash_data = m_hashTbl->m_data;

    outPlan = outerPlanState(m_runtime);

    while (true) {
        switch (m_runState) {
            case AGG_PREPARE: {
                for (;;) {
                    outer_batch = VectorEngine(outPlan);
                    if (unlikely(BatchIsNull(outer_batch))) {
                        if (hasDistinct) {
                            int numset = Max(m_runtime->phase->numsets, 1);
                            for (int i = 0; i < numset; i++) {
                                BatchSortAggregation(i,
                                    u_sess->attr.attr_memory.work_mem,
                                    false,
                                    m_runtime->ss.ps.plan->plan_node_id,
                                    SET_DOP(m_runtime->ss.ps.plan->dop));
                            }
                        }
                        break;
                    }
                    /* Compute aggregation */
                    buildPlaintAgg<hasDistinct>(outer_batch, first_batch);
                    first_batch = false;
                }
                m_projected_set = 0;
                m_runState = AGG_FETCH;
                break;
            }
            case AGG_FETCH: {
                int numset = Max(m_runtime->phase->numsets, 1);
                if (m_projected_set < numset) {
                    m_runtime->projected_set = m_projected_set;
                    m_scanBatch->Reset();
                    ResetExprContext(m_runtime->ss.ps.ps_ExprContext);
                    InvokeFp(m_buildScanBatch)(hash_data[m_projected_set]);
                    res_batch = ProducerBatch();
                    m_projected_set++;
                    return res_batch;
                }
                m_finish = true;
                return NULL;
            }
            default:
                break;
        }
    }
}

/*
 * @Description: entrance function of plain agg.
 */
VectorBatch* PlainAggRunner::Run()
{
    return InvokeFp(plainAggregation)();
}

/*
 * @Description: plain agg reset.
 * @in node: Current agg node.
 */
bool PlainAggRunner::ResetNecessary(VecAggState* node)
{
    VecAgg* aggnode = (VecAgg*)node->ss.ps.plan;

    m_finish = false;

    /*
     * If lefttree chgParam is not null, or parameter changes affect
     * input expressions of the aggregated functions; we need rescan
     * lower node.
     */
    if (node->ss.ps.lefttree->chgParam == NULL && aggnode->aggParams == NULL) {
        m_runState = AGG_FETCH;
        m_projected_set = 0;
        return false;
    }

    /* And rebuild empty hashtable if needed */
    m_runState = AGG_PREPARE;

    int hash_size = m_hashTbl->m_size;

    delete m_hashTbl;

    m_hashTbl = New(CurrentMemoryContext) vechashtable(hash_size);

    hashCell** hash_data = m_hashTbl->m_data;

    hashCell* cell = NULL;
    for (int i = 0; i < hash_size; i++) {
        cell = (hashCell*)palloc0(m_cellSize);
        hash_data[i] = cell;
        cell->flag.m_next = NULL;

        /* set the init value. */
        for (int k = 0; k < m_aggNum; k++) {
            if (m_aggCount[k]) {
                cell->m_val[m_aggIdx[k]].val = 0;
                SET_NOTNULL(cell->m_val[m_aggIdx[k]].flag);
            } else
                SET_NULL(cell->m_val[m_aggIdx[k]].flag);
        }
    }

    return true;
}

/*
 * @Description: plain agg end, free resources.
 */
void PlainAggRunner::endPlainAgg()
{
    int setno;
    int num_grouping_sets = Max(m_runtime->maxsets, 1);

    if (m_sortDistinct) {
        for (setno = 0; setno < num_grouping_sets; setno++) {
            for (int i = 0; i < m_aggNum; i++) {
                if (m_sortDistinct[setno].batchsortstate[i]) {
                    batchsort_end(m_sortDistinct[setno].batchsortstate[i]);
                    m_sortDistinct[setno].batchsortstate[i] = NULL;
                }
            }
        }
    }
}
