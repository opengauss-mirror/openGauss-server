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
 * vecsortagg.cpp
 *       Routines to handle vector sort aggregae nodes.
 *
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecsortagg.cpp
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
#include "vecexecutor/vecsortagg.h"
#include "vecexecutor/vecagg.h"

/*
 * @Description:Initialize sort and phase informantion to next group by operation.
 */
void SortAggRunner::init_phase()
{
    /*
     * If this isn't the last phase, we need to sort appropriately for the
     * next phase in sequence.
     */
    if (m_runtime->current_phase < m_runtime->numphases - 1) {
        Sort* sort_node = m_runtime->phases[m_runtime->current_phase + 1].sortnode;
        PlanState* outer_node = outerPlanState(m_runtime);
        TupleDesc tup_desc = ExecGetResultType(outer_node);
        int64 work_mem = SET_NODEMEM(sort_node->plan.operatorMemKB[0], sort_node->plan.dop);
        int64 max_mem =
            (sort_node->plan.operatorMaxMem > 0) ? SET_NODEMEM(sort_node->plan.operatorMaxMem, sort_node->plan.dop) : 0;

        m_batchSortOut = batchsort_begin_heap(tup_desc,
            sort_node->numCols,
            sort_node->sortColIdx,
            sort_node->sortOperators,
            sort_node->collations,
            sort_node->nullsFirst,
            work_mem,
            false,
            max_mem);
    }

    Assert(m_runtime->current_phase <= m_runtime->numphases);
    if (m_runtime->current_phase < m_runtime->numphases) {
        /* Point to current_phase. */
        m_runtime->phase = &m_runtime->phases[m_runtime->current_phase];
    }
}

/*
 * @Description: Set grouping column, this function will be called when switch phase.
 */
void SortAggRunner::set_key()
{
    int i = 0;
    m_key = m_runtime->phase->gset_lengths[0];

    for (i = 0; i < m_key; i++) {
        m_keyIdx[i] = m_runtime->phase->aggnode->grpColIdx[i] - 1;
    }

    for (i = 0; i < m_key; i++) {
        bool is_found = false;
        for (int k = 0; k < m_cellVarLen; k++) {
            if (m_keyIdx[i] == m_cellBatchMap[k]) {
                is_found = true;
                m_keyIdxInCell[i] = k;
            }
        }
        Assert(is_found);
    }

    m_eqfunctions = m_runtime->phase->eqfunctions;
}

/*
 * @Description: Switch to next phase.
 */
void SortAggRunner::switch_phase()
{
    /* Reset batchSortIn, current phase has done. */
    if (m_batchSortIn) {
        batchsort_end(m_batchSortIn);
        m_batchSortIn = NULL;
    }

    if (m_batchSortOut) {
        m_batchSortIn = m_batchSortOut;
        batchsort_performsort(m_batchSortIn);
        m_batchSortOut = NULL;
    }

    m_runtime->current_phase++;

    init_phase();

    /* Set next group columns in this phase.*/
    if (m_runtime->current_phase < m_runtime->numphases) {
        set_key();
    }
}

void SortAggRunner::BindingFp()
{
    if (m_keySimple) {
        if (m_hasDistinct) {
            m_buildSortFun = &SortAggRunner::buildSortAgg<true, true>;
        } else {
            m_buildSortFun = &SortAggRunner::buildSortAgg<true, false>;
        }
    } else {
        if (m_hasDistinct) {
            m_buildSortFun = &SortAggRunner::buildSortAgg<false, true>;
        } else {
            m_buildSortFun = &SortAggRunner::buildSortAgg<false, false>;
        }
    }

    /* Need be init at here, because can is Ap function operator. */
    if (m_finalAggNum > 0)
        m_buildScanBatch = &BaseAggRunner::BuildScanBatchFinal;
    else
        m_buildScanBatch = &BaseAggRunner::BuildScanBatchSimple;
}

/*
 * @Description: Init index to Ap function.
 */
void SortAggRunner::init_indexForApFun()
{
    int i = 0;
    int index = 0;
    ListCell* lc = NULL;
    Bitmapset* all_need_cols = NULL;

    /* Get all columns participate in group*/
    foreach (lc, m_runtime->all_grouped_cols) {
        int var_number = lfirst_int(lc) - 1;
        all_need_cols = bms_add_member(all_need_cols, var_number);
    }

    /* Get other columns not participate in group*/
    foreach (lc, m_runtime->hash_needed) {
        int var_number = lfirst_int(lc) - 1;
        all_need_cols = bms_add_member(all_need_cols, var_number);
    }

    m_cellVarLen = bms_num_members(all_need_cols);
    m_cellBatchMap = (int*)palloc(sizeof(int) * m_cellVarLen);

    /*
     * m_cellBatchMap Mapping position of outbatch column in cell
     * for example, m_cellBatchMap[0] = 3 say: batch's 3 column keep in 0 column of cell
     */
    while ((index = bms_first_member(all_need_cols)) >= 0) {
        m_cellBatchMap[i] = index;
        i++;
    }

    int max_group_col_len = 0;
    for (i = 0; i < m_runtime->numphases; i++) {
        int group_col = m_runtime->phases[i].aggnode->numCols;
        if (group_col > max_group_col_len) {
            max_group_col_len = group_col;
        }
    }

    /* Keep current carry on group cols, their values will be seted in execution */
    m_keyIdx = (int*)palloc(sizeof(int) * max_group_col_len);

    m_keyIdxInCell = (int*)palloc(sizeof(int) * max_group_col_len);

    set_key();
    init_aggInfo(m_runtime->numaggs, m_runtime->aggInfo);
}

/*
 * @Description: Sort agg constructed function.
 */
SortAggRunner::SortAggRunner(VecAggState* runtime)
    : BaseAggRunner(runtime, false),
      m_FreeMem(false),
      m_ApFun(false),
      m_noData(true),
      m_batchSortIn(NULL),
      m_batchSortOut(NULL),
      m_SortBatch(NULL),
      m_sortSource(NULL)
{
    VecAgg* node = NULL;
    errno_t rc;

    node = (VecAgg*)(runtime->ss.ps.plan);

    init_phase();
    /* OLAP function*/
    if (node->groupingSets) {
        m_ApFun = true;
        init_indexForApFun();
        if (m_runtime->numphases > 1) {
            /* Store return results of phase sort. */
            m_SortBatch = New(CurrentMemoryContext)
                VectorBatch(CurrentMemoryContext, m_runtime->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
        }

        build_batch();
        m_cellSize = offsetof(hashCell, m_val) + m_cols * sizeof(hashVal);
    }

    m_groupState = GET_MATCH_KEY;

    /* Init sort state for distinct operate. */
    int64 workmem = SET_NODEMEM(node->plan.operatorMemKB[0], node->plan.dop);
    int64 maxmem = (node->plan.operatorMaxMem > 0) ? SET_NODEMEM(node->plan.operatorMaxMem, node->plan.dop) : 0;
    initialize_sortstate(workmem, maxmem, node->plan.plan_node_id, SET_DOP(node->plan.dop));

    int numset = Max(m_runtime->maxsets, 1);

    m_sortGrps = (GroupintAtomContainer*)palloc0(numset * sizeof(GroupintAtomContainer));
    for (int i = 0; i < numset; i++) {
        rc = memset_s(
            m_sortGrps[i].sortGrp, 2 * BatchMaxSize * sizeof(hashCell*), 0, 2 * BatchMaxSize * sizeof(hashCell*));
        securec_check(rc, "\0", "\0");

        m_sortGrps[i].sortGrpIdx = 0;
        m_sortGrps[i].sortCurrentBuf = New(CurrentMemoryContext) VarBuf(CurrentMemoryContext);
        m_sortGrps[i].sortCurrentBuf->Init();
        m_sortGrps[i].sortBckBuf = New(CurrentMemoryContext) VarBuf(CurrentMemoryContext);
        m_sortGrps[i].sortBckBuf->Init();
    }

    m_runState = AGG_FETCH;
    m_prepareState = GET_SOURCE;
    BindingFp();
}

/*
 * @Description: get data source. Data may be from lefttree,
 * perhaps come from agg self sort result for Ap function.
 */
hashSource* SortAggRunner::GetSortSource()
{
    hashSource* ps = NULL;

    Assert(m_runtime->current_phase <= m_runtime->numphases);
    if (m_runtime->current_phase == m_runtime->numphases) {
        return NULL;
    } else if (m_batchSortIn) {
        ps = New(CurrentMemoryContext) hashSortSource(m_batchSortIn, m_SortBatch);
    } else {
        ps = New(CurrentMemoryContext) hashOpSource(outerPlanState(m_runtime));
    }
    return ps;
}

/*
 * @Description: Free memory usage of return batch.
 */
template <bool lastCall>
void SortAggRunner::FreeSortGrpMem(int num)
{
    int i, j, idx;
    Oid type_id;
    hashVal temp_val;
    FmgrInfo* final_flinfo = NULL;
    FmgrInfo* agg_flinfo = NULL;
    void* value_loc = NULL;
    errno_t rc;

    /* Find agg column and free m_sortGrp value.*/
    for (i = 0; i < m_aggNum; i++) {
        /*
         * If column is vec avg type and data type is int1/int2/int4/folat4/float8,
         * we don't need to free the memory. Because this memory is in ecxt_per_tuple_memory, they will
         * be automatic reset.
         */
        final_flinfo = m_runtime->aggInfo[i].vec_final_function.flinfo;
        agg_flinfo = m_runtime->aggInfo[i].vec_agg_function.flinfo;
        type_id = agg_flinfo->fn_rettype;

        if (final_flinfo != NULL && ((type_id == INT8ARRAYOID) || (type_id == FLOAT8ARRAYOID))) {
            continue;
        }

        if (COL_IS_ENCODE(type_id)) {
            idx = m_aggIdx[i];
            for (j = 0; j < num; j++) {
                temp_val = ((hashCell*)(m_sortGrps[m_projected_set].sortGrp[j]))->m_val[idx];
                if (NOT_NULL(temp_val.flag)) {
                    value_loc = DatumGetPointer(temp_val.val);
                    if (value_loc != NULL)
                        pfree_ext(value_loc);
                }
            }
        }
    }

    if (!lastCall) {
        int remain_grp = m_sortGrps[m_projected_set].sortGrpIdx - BatchMaxSize + 1;
        hashCell** sort = m_sortGrps[m_projected_set].sortGrp;

        for (i = 0; i < remain_grp; i++)
            sort[i] = sort[BatchMaxSize + i];

        const int free_len = 2 * BatchMaxSize - remain_grp;

        rc = memset_s(&sort[remain_grp], free_len * sizeof(hashCell*), 0, free_len * sizeof(hashCell*));

        securec_check(rc, "\0", "\0");
        /* Reset the sort group index.*/
        m_sortGrps[m_projected_set].sortGrpIdx = remain_grp - 1;

        m_sortGrps[m_projected_set].sortBckBuf->Reset();

        m_scanBatch->Reset();
    } else {
        rc = memset_s(m_sortGrps[m_projected_set].sortGrp,
            2 * BatchMaxSize * sizeof(hashCell*),
            0,
            2 * BatchMaxSize * sizeof(hashCell*));

        securec_check(rc, "\0", "\0");

        m_sortGrps[m_projected_set].sortGrpIdx = 0;
        /* release the space*/
        m_sortGrps[m_projected_set].sortBckBuf->Reset();
        /* reset the batch.*/
        m_scanBatch->Reset();
    }
}

/*
 * @Description: set group column number, it is used when switch grouping in same phase
 * @in current_grp - group frequency  in current phase
 */
bool SortAggRunner::set_group_num(int current_grp)
{
    /* Simple group by */
    if (m_ApFun == false) {
        if (current_grp > 0) {
            return false;
        }
    } else {
        if (current_grp == m_runtime->phase->numsets) {
            return false;
        } else {
            /* Set the long of group column, which must be less than or equal to m_runtime->phase->gset_lengths[0] */
            m_key = m_runtime->phase->gset_lengths[current_grp];

            return true;
        }
    }

    return true;
}

/*
 * @Description: Match and Aggregation compute
 * @in current_grp -  group frequency  in current phase
 * @in batch - current batch
 */
template <bool simple, bool hashDistinct>
void SortAggRunner::BatchMatchAndAgg(int current_grp, VectorBatch* batch)
{
    hashCell* cell = NULL;
    hashCell** sort_grp;
    int current_sortGrpIdx;
    int diff_group_row_num = 0;
    int nrows = batch->m_rows;

    current_sortGrpIdx = m_sortGrps[current_grp].sortGrpIdx;
    m_sortCurrentBuf = m_sortGrps[current_grp].sortCurrentBuf;
    sort_grp = m_sortGrps[current_grp].sortGrp;

    /* Compute agg for all data, no need group.*/
    if (m_key == 0) {
        for (int i = 0; i < nrows; i++) {
            cell = sort_grp[current_sortGrpIdx];
            if (cell == NULL) {
                cell = (hashCell*)m_sortCurrentBuf->Allocate(m_cellSize);
                sort_grp[current_sortGrpIdx] = cell;
                initCellValue<simple, true>(batch, cell, i);
            }
            m_Loc[i] = cell;
        }
    } else {
        for (int i = 0; i < nrows; i++) {
            cell = sort_grp[current_sortGrpIdx];

            /*
             * Divide 'if(cell == NULL || match_key<simple>(batch, i, cell)==false)'
             * into two parts, and do match_key only when cell!= NULL
             */
            bool matched_keys = false;
            if (cell != NULL) {
                if (m_runtime->jitted_SortAggMatchKey) {
                    /* mark if math_key of sortagg has been codegened or not */
                    if (HAS_INSTR(&m_runtime->ss, false)) {
                        m_runtime->ss.ps.instrument->isLlvmOpt = true;
                    }

                    typedef bool (*match_key_func)(
                        SortAggRunner* tbl, VectorBatch* batch, int batchIdx, hashCell* cell);
                    matched_keys = ((match_key_func)(m_runtime->jitted_SortAggMatchKey))(this, batch, i, cell);
                } else
                    matched_keys = match_key<simple>(batch, i, cell);
            }

            /* means we got match fail*/
            if (cell == NULL || matched_keys == false) {
                if (cell != NULL) {
                    /* if we got no match, we should inc sort group index*/
                    current_sortGrpIdx++;

                    if (current_sortGrpIdx == BatchMaxSize) {
                        /* switch  buffer*/
                        VarBuf* tmp = NULL;
                        tmp = m_sortGrps[current_grp].sortCurrentBuf;
                        m_sortGrps[current_grp].sortCurrentBuf = m_sortGrps[current_grp].sortBckBuf;
                        m_sortGrps[current_grp].sortBckBuf = tmp;
                        m_sortCurrentBuf = m_sortGrps[current_grp].sortCurrentBuf;
                    }
                    if (hashDistinct) {
                        m_sortDistinct[current_grp].m_distinctLoc = cell;
                        AppendBatchForSortAgg(batch, diff_group_row_num, i, current_grp);
                        BatchSortAggregation(current_grp,
                            u_sess->attr.attr_memory.work_mem,
                            false,
                            m_runtime->ss.ps.plan->plan_node_id,
                            SET_DOP(m_runtime->ss.ps.plan->dop));
                        diff_group_row_num = i;
                    }
                }
                cell = (hashCell*)m_sortCurrentBuf->Allocate(m_cellSize);
                sort_grp[current_sortGrpIdx] = cell;
                initCellValue<simple, true>(batch, cell, i);
            }

            Assert(cell != NULL);
            m_Loc[i] = cell;
        }
    }

    m_sortGrps[current_grp].sortGrpIdx = current_sortGrpIdx;

    if (hashDistinct) {
        m_sortDistinct[current_grp].m_distinctLoc = cell;
        AppendBatchForSortAgg(batch, diff_group_row_num, nrows, current_grp);
        BatchNoSortAgg(batch);
    } else {
        BatchAggregation(batch);
    }
}

/*
 * @Description: Grouping and Agg to batch
 * @in batch - current batch
 */
template <bool simple, bool hashDistinct>
void SortAggRunner::buildSortAgg(VectorBatch* batch)
{
    int current_grp = 0;

    while (true) {
        switch (m_groupState) {
            /*
             * Get grouping column number.
             * For Ap function, we can need more than one group to this batch.
             */
            case GET_MATCH_KEY:
                if (!set_group_num(current_grp)) {
                    return;
                }
                m_groupState = SORT_MATCH;
                break;
            case SORT_MATCH:
                BatchMatchAndAgg<simple, hashDistinct>(current_grp, batch);
                current_grp++;
                m_groupState = GET_MATCH_KEY;
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND), errmsg("Unsupported state in vec sort agg")));
                break;
        }
    }
}

/*
 * @Description: entrance function. execute start from here.
 * @return - result of group and agg.
 */
VectorBatch* SortAggRunner::Run()
{
    for (;;) {
        switch (m_prepareState) {
            /* Get data source it possible be leftree or sort state*/
            case GET_SOURCE: {
                m_sortSource = GetSortSource();
                if (m_sortSource == NULL) {
                    m_finish = true;
                    return NULL;
                }
                m_prepareState = RUN_SORT;
                break;
            }
            /* Execute group and agg*/
            case RUN_SORT: {
                VectorBatch* batch = RunSort();
                if (BatchIsNull(batch)) {
                    if (m_finish) {
                        return NULL;
                    }
                    m_prepareState = SWITCH_PARSE;
                } else {
                    return batch;
                }
                break;
            }
            /* Switch parse if exist*/
            case SWITCH_PARSE: {
                switch_phase();
                m_prepareState = GET_SOURCE;
                break;
            }
            default:
                break;
        }
    }
}

/*
 * @Description: Return data when rows > 1000.
 * @return - if rows > 1000 return result of group and agg else return null.
 */
VectorBatch* SortAggRunner::ReturnData()
{
    VectorBatch* res_batch = NULL;
    int numset = Max(m_runtime->phase->numsets, 1);

    while (m_projected_set < numset) {
        if (m_projected_set != -1 && m_FreeMem) {
            Assert(m_projected_set >= 0);

            /* Free memory which is used by previous return data.*/
            FreeSortGrpMem<false>(BatchMaxSize);
        }

        m_projected_set++;
        m_runtime->projected_set = m_projected_set;
        if (m_projected_set < numset) {
            int total_num = m_sortGrps[m_projected_set].sortGrpIdx;
            m_FreeMem = false;
            if (total_num >= BatchMaxSize) {
                hashCell** sort = m_sortGrps[m_projected_set].sortGrp;

                for (int j = 0; j < BatchMaxSize; j++)
                    InvokeFp(m_buildScanBatch)(sort[j]);

                res_batch = ProducerBatch();
                if (unlikely(BatchIsNull(res_batch))) {
                    m_FreeMem = true;
                    continue;
                } else {
                    m_FreeMem = true;
                    return res_batch;
                }
            }
        }
    }
    return NULL;
}

/*
 * @Description: return last batch data.
 * @return - return last batch data.
 */
VectorBatch* SortAggRunner::ReturnLastData()
{
    VectorBatch* res_batch = NULL;
    int numset = Max(m_runtime->phase->numsets, 1);

    while (m_projected_set < numset) {
        if (m_projected_set != -1) {
            Assert(m_projected_set >= 0);

            /* Free memory which is used by previous return date.*/
            FreeSortGrpMem<true>(m_sortGrps[m_projected_set].sortGrpIdx);
        }

        m_projected_set++;
        m_runtime->projected_set = m_projected_set;
        if (m_projected_set < numset) {

            int total_num = m_sortGrps[m_projected_set].sortGrpIdx;
            if (total_num == 0 && m_sortGrps[m_projected_set].sortGrp[0] == NULL) {
                continue;
            } else {
                hashCell** sort = m_sortGrps[m_projected_set].sortGrp;
                for (int i = 0; i <= total_num; i++) {
                    InvokeFp(m_buildScanBatch)(sort[i]);
                }
                res_batch = ProducerBatch();
                if (BatchIsNull(res_batch)) {
                    continue;
                } else {
                    return res_batch;
                }
            }
        }
    }
    return NULL;
}

/*
 * @Description: set one row data its value is null and set initial value to agg.
 */
void SortAggRunner::BuildNullScanBatch()
{
    int i;
    int nrows = m_scanBatch->m_rows;
    int scan_batch_cols = m_cellVarLen + m_aggNum;
    int col_idx = 0;
    ScalarVector* vector = NULL;

    for (i = 0; i < scan_batch_cols; i++) {
        vector = &m_scanBatch->m_arr[i];
        SET_NULL(vector->m_flag[nrows]);
        vector->m_rows++;
    }

    for (i = 0; i < m_aggNum; i++) {
        if (m_aggCount[i]) {
            vector = &m_scanBatch->m_arr[col_idx + m_cellVarLen];
            vector->m_vals[nrows] = 0;
            SET_NOTNULL(vector->m_flag[nrows]);
        }
        col_idx++;
    }

    m_scanBatch->m_rows++;
}

/*
 * @Description: We need return one row data for the case of group by column is null when left tree return data is null
 * This function is called when data of lefttree is null.
 */
VectorBatch* SortAggRunner::ReturnNullData()
{
    VectorBatch* result_batch = NULL;
    int numset = Max(m_runtime->phase->numsets, 1);

    while (m_runtime->phase->gset_lengths && m_projected_set < numset) {
        if (0 == m_runtime->phase->gset_lengths[m_projected_set]) {
            /* m_scanBatch need build only one row, result of return should be same */
            if (m_scanBatch->m_rows == 0) {
                BuildNullScanBatch();
            }
            m_runtime->projected_set = m_projected_set;
            result_batch = ProducerBatch();
            m_projected_set++;
            if (!BatchIsNull(result_batch)) {
                return result_batch;
            } else {
                return NULL;
            }
        }
        m_projected_set++;
    }
    return NULL;
}

/*
 * @Description: To current data source, get data, group and compute agg; return compute result.
 * @return - result of group and agg.
 */
VectorBatch* SortAggRunner::RunSort()
{
    VectorBatch* outer_batch = NULL;
    VectorBatch* result_batch = NULL;
    int numset = Max(m_runtime->phase->numsets, 1);
    Plan* plan = m_runtime->ss.ps.plan;
    int64 workmem = SET_NODEMEM(plan->operatorMemKB[0], plan->dop);
    int64 maxmem = (plan->operatorMaxMem > 0) ? SET_NODEMEM(plan->operatorMaxMem, plan->dop) : 0;

    if (m_finish)
        return NULL;

    ResetExprContext(m_runtime->ss.ps.ps_ExprContext);

    for (;;) {
        switch (m_runState) {
            /* Get data, grouping and  compute agg*/
            case AGG_FETCH:
                outer_batch = m_sortSource->getBatch();
                if (unlikely(BatchIsNull(outer_batch))) {
                    if (m_noData) {
                        m_runState = AGG_RETURN_NULL;
                        m_projected_set = 0;
                        break;
                    }
                    if (m_hasDistinct) {
                        /* compute last batch when numSortCols > 0 */
                        for (int i = 0; i < numset; i++) {
                            BatchSortAggregation(i, workmem, maxmem, plan->plan_node_id, SET_DOP(plan->dop));
                        }
                    }
                    /* return last data */
                    m_projected_set = -1;
                    m_runState = AGG_RETURN_LAST;
                    break;
                }

                m_noData = false;
                if (m_batchSortOut) {
                    m_batchSortOut->sort_putbatch(m_batchSortOut, outer_batch, 0, outer_batch->m_rows);
                }

                InvokeFp(m_buildSortFun)(outer_batch);
                m_projected_set = -1;
                m_FreeMem = false;
                m_runState = AGG_RETURN;
                break;
            /* Return data when rows > 1000 */
            case AGG_RETURN:
                result_batch = ReturnData();
                if (result_batch != NULL) {
                    return result_batch;
                } else {
                    Assert(m_projected_set == numset);
                    m_runState = AGG_FETCH;
                }
                break;
            /* Return last data, when outerBatch is null*/
            case AGG_RETURN_LAST:
                result_batch = ReturnLastData();
                if (result_batch != NULL) {
                    return result_batch;
                } else {
                    Assert(m_projected_set == numset);
                    m_runState = AGG_FETCH;
                    return NULL;
                }
                break;
            /* Return data for the case of group by column is null and left tree return data is null*/
            case AGG_RETURN_NULL:
                result_batch = ReturnNullData();
                if (result_batch != NULL) {
                    return result_batch;
                } else {
                    m_finish = true;
                    return NULL;
                }
            default:
                break;
        }
    }
}

/*
 * @Description: Sort agg reset
 */
bool SortAggRunner::ResetNecessary(VecAggState* node)
{
    m_finish = false;
    m_prepareState = GET_SOURCE;
    m_runState = AGG_FETCH;
    m_noData = true;
    m_FreeMem = false;
    if (m_batchSortIn) {
        batchsort_end(m_batchSortIn);
        m_batchSortIn = NULL;
    }
    if (m_batchSortOut) {
        batchsort_end(m_batchSortOut);
        m_batchSortOut = NULL;
    }

    m_runtime->current_phase = 0;
    init_phase();
    if (m_ApFun) {
        set_key();
    }

    int numset = Max(m_runtime->maxsets, 1);
    for (int i = 0; i < numset; i++) {
        errno_t rc = memset_s(
            m_sortGrps[i].sortGrp, 2 * BatchMaxSize * sizeof(hashCell*), 0, 2 * BatchMaxSize * sizeof(hashCell*));

        securec_check(rc, "\0", "\0");

        m_sortGrps[i].sortGrpIdx = 0;
        m_sortGrps[i].sortBckBuf->Reset();
        m_scanBatch->Reset();
    }

    return true;
}

/*
 * @Description: Sort agg end, free all resource.
 */
void SortAggRunner::endSortAgg()
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

    /* Free buf mem*/
    int numset = Max(m_runtime->maxsets, 1);
    for (int i = 0; i < numset; i++) {
        m_sortGrps[i].sortBckBuf->DeInit();
        m_scanBatch->Reset();
    }
}
