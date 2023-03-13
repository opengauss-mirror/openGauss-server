/* ---------------------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * vecsubplan.cpp
 *
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecsubplan.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "executor/executor.h"
#include "executor/exec/execStream.h"
#include "vecexecutor/vectorbatch.h"
#include "vecexecutor/vecexpression.h"
#include "vecexecutor/vecexecutor.h"
#include "utils/memutils.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include "optimizer/clauses.h"
#include "executor/node/nodeSubplan.h"
#include "executor/executor.h"

static TupleTableSlot* GetSlotfromBatchRow(TupleTableSlot* slot, VectorBatch* batch, int n_row);

extern ScalarVector* ExecVectorExprEngineSwitchContext(
    ExprState* expr, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone);

/*
 * @Description: ExecVecHashSubplan
 * store subselect result in an in-memory hash table
 * @Param[template] subType: subLink type
 * @param[IN] subPlanClause:  VecHash SubPlan state
 * @param[IN] econtext: a context for expression qualifications and tuple projections
 * @param[IN] pSelection: if pSelection not null, pSelection[i] means whether econtext->ecxt_xxxbatch[i] used in
 * expression evaluation, or all econtext->ecxt_xxxbatch[] would be used.
 * @param[OUT] pVector: a vector to save result
 * @return: result scalarvector
 */
template <SubLinkType subType>
static ScalarVector* ExecVecHashSubplan(
    SubPlanState* subPlanClause, ExprContext* econtext, const bool* pSelection, ScalarVector* pVector)
{
    SubPlan* sub_plan = (SubPlan*)subPlanClause->xprstate.expr;
    PlanState* plan_state = subPlanClause->planstate;
    TupleTableSlot* slot = subPlanClause->projLeft->pi_slot;
    VectorBatch* batch = NULL;
    int res_vector_idx = 0;
    int m_rows = 0;

    /* Shouldn't have any direct correlation Vars */
    if (sub_plan->parParam != NIL || subPlanClause->args != NIL)
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("hashed sub_plan with direct correlation not supported")));

    /*
     * If first time through or we need to rescan the sub_plan, build the hash
     * table.
     */
    if (subPlanClause->hashtable == NULL || plan_state->chgParam != NULL)
        buildSubPlanHash(subPlanClause, econtext);

    /*
     * Evaluate lefthand expressions and form a projection tuple. First we
     * have to set the econtext to use (hack alert!).
     */
    subPlanClause->projLeft->pi_exprContext = econtext;
    batch = ExecVecProject(subPlanClause->projLeft);
    if (!BatchIsNull(batch))
        m_rows = batch->m_rows;

    /*
     * The result for an empty sub_plan is always FALSE; no need to evaluate
     * lefthand side.
     */
    if (!subPlanClause->havehashrows && !subPlanClause->havenullrows) {
        for (int i = 0; i < m_rows; i++) {
            pVector->m_vals[i] = BoolGetDatum(false);
            SET_NOTNULL(pVector->m_flag[i]);
        }
        pVector->m_rows = m_rows;
        return pVector;
    }

    /*
     * Note: because we are typically called in a per-tuple context, we have
     * to explicitly clear the projected tuple before returning. Otherwise,
     * we'll have a double-free situation: the per-tuple context will probably
     * be reset before we're called again, and then the tuple slot will think
     * it still needs to free the tuple.
     */
    /*
     * If the LHS is all non-null, probe for an exact match in the main hash
     * table.  If we find one, the result is TRUE. Otherwise, scan the
     * partly-null table to see if there are any rows that aren't provably
     * unequal to the LHS; if so, the result is UNKNOWN.  (We skip that part
     * if we don't care about UNKNOWN.) Otherwise, the result is FALSE.
     *
     * Note: the reason we can avoid a full scan of the main hash table is
     * that the combining operators are assumed never to yield NULL when both
     * inputs are non-null.  If they were to do so, we might need to produce
     * UNKNOWN instead of FALSE because of an UNKNOWN result in comparing the
     * LHS to some main-table entry --- which is a comparison we will not even
     * make, unless there's a chance match of hash keys.
     */
    for (int i = 0; i < m_rows; i++) {
        /* skip those filtered rows, no need to search hash table. */
        if (econtext->m_fUseSelection && pSelection && !pSelection[i]) {
            res_vector_idx++;
            continue;
        }

        slot = GetSlotfromBatchRow(slot, batch, i);
        if (slotNoNulls(slot)) {
            if (subPlanClause->havehashrows &&
                    FindTupleHashEntry(subPlanClause->hashtable,
                                       slot,
                                       subPlanClause->cur_eq_funcs,
                                       subPlanClause->lhs_hash_funcs) != NULL) {
                pVector->m_vals[res_vector_idx] = BoolGetDatum(true);
                SET_NOTNULL(pVector->m_flag[res_vector_idx]);
                res_vector_idx++;
                continue;
            }
            if (subPlanClause->havenullrows && subPlanClause->hashnulls != NULL &&
                    findPartialMatch(subPlanClause->hashnulls, slot, subPlanClause->cur_eq_funcs)) {
                pVector->m_vals[res_vector_idx] = BoolGetDatum(false);
                SET_NULL(pVector->m_flag[res_vector_idx]);
                res_vector_idx++;
                continue;
            }
            pVector->m_vals[res_vector_idx] = BoolGetDatum(false);
            SET_NOTNULL(pVector->m_flag[res_vector_idx]);
            res_vector_idx++;
            continue;
        }

        /*
         * When the LHS is partly or wholly NULL, we can never return TRUE. If we
         * don't care about UNKNOWN, just return FALSE.  Otherwise, if the LHS is
         * wholly NULL, immediately return UNKNOWN.  (Since the combining
         * operators are strict, the result could only be FALSE if the sub-select
         * were empty, but we already handled that case.) Otherwise, we must scan
         * both the main and partly-null tables to see if there are any rows that
         * aren't provably unequal to the LHS; if so, the result is UNKNOWN.
         * Otherwise, the result is FALSE.
         */
        if (subPlanClause->hashnulls == NULL) {
            pVector->m_vals[res_vector_idx] = BoolGetDatum(false);
            SET_NOTNULL(pVector->m_flag[res_vector_idx]);
            res_vector_idx++;
            continue;
        }

        /* Scan partly-null table first, since more likely to get a match */
        if (slotAllNulls(slot) || (subPlanClause->havenullrows &&
                                      findPartialMatch(subPlanClause->hashnulls, slot, subPlanClause->cur_eq_funcs))) {
            pVector->m_vals[res_vector_idx] = BoolGetDatum(false);
            SET_NULL(pVector->m_flag[res_vector_idx]);
            res_vector_idx++;
            continue;
        }
        if (subPlanClause->havehashrows &&
            findPartialMatch(subPlanClause->hashtable, slot, subPlanClause->cur_eq_funcs)) {
            pVector->m_vals[res_vector_idx] = BoolGetDatum(false);
            SET_NULL(pVector->m_flag[res_vector_idx]);
            res_vector_idx++;
            continue;
        }

        pVector->m_vals[res_vector_idx] = BoolGetDatum(false);
        SET_NOTNULL(pVector->m_flag[res_vector_idx]);
        res_vector_idx++;
        continue;
    }

    pVector->m_rows = res_vector_idx;
    return pVector;
}

/*
 * @Description: InitSystemColumns
 *    init system column from batch for col-related expression
 * @param[IN] subPlan_syscolumns: system columns of subplan batch
 * @param[IN] batch_syscolumns: system columns of vector batch to be used in expression evaluation
 * @return: void
 */
static void InitSystemColumns(SysColContainer* subplan_syscolumns, SysColContainer* batch_syscolumns)
{
    subplan_syscolumns->sysColumns = batch_syscolumns->sysColumns;
    subplan_syscolumns->m_ppColumns = New(CurrentMemoryContext) ScalarVector[subplan_syscolumns->sysColumns];

    int total_sys_columns = -FirstLowInvalidHeapAttributeNumber;
    for (int i = 0; i < total_sys_columns; i++) {
        subplan_syscolumns->sysColumpMap[i] = batch_syscolumns->sysColumpMap[i];
    }

    ScalarDesc desc;

    for (int i = 0; i < subplan_syscolumns->sysColumns; i++) {
        desc.typeId = batch_syscolumns->m_ppColumns[i].m_desc.typeId;
        desc.typeMod = batch_syscolumns->m_ppColumns[i].m_desc.typeMod;
        desc.encoded = batch_syscolumns->m_ppColumns[i].m_desc.encoded;
        subplan_syscolumns->m_ppColumns[i].init(CurrentMemoryContext, desc);
    }
}

/*
 * @Description: FetchOneSystemColumnRowByIdx
 *    get one system column row from batch for col-related expression
 * @param[IN] subPlan_syscolumns: system columns of subplan batch
 * @param[IN] batch_syscolumns: system columns of vector batch to be used in expression evaluation
 * @param[IN] idx: the index of the outerbatch
 * @return: void
 */
static void FetchOneSystemColumnRowByIdx(
    SysColContainer* subplan_syscolumns, SysColContainer* batch_syscolumns, int idx)
{
    ScalarValue val;
    uint8 flag;
    ScalarValue* dest_val = NULL;
    uint8* dest_flag = NULL;

    for (int i = 0; i < subplan_syscolumns->sysColumns; i++) {
        val = batch_syscolumns->m_ppColumns[i].m_vals[idx];
        flag = batch_syscolumns->m_ppColumns[i].m_flag[idx];

        dest_val = subplan_syscolumns->m_ppColumns[i].m_vals;
        dest_flag = subplan_syscolumns->m_ppColumns[i].m_flag;

        if (NOT_NULL(flag)) {
            dest_val[0] = val;
            SET_NOTNULL(dest_flag[0]);
        } else {
            SET_NULL(dest_flag[0]);
        }
        subplan_syscolumns->m_ppColumns[i].m_rows = 1;
    }
}

/*
 * @Description: FetchOneRowByIdx
 *    get one row from batch for col-related expression
 * @param[template] varno: to mark which exprbatch to  be used
 * @param[IN] subPlanClause: a subplan state
 * @param[IN] outerbatch: a vector batch to be used in expression evaluation
 * @param[IN] idx: the index of the outerbatch
 * @return: void
 */
template <int varno>
static void FetchOneRowByIdx(SubPlanState* subPlanClause, VectorBatch* batch, int idx)
{
    ScalarValue val;
    uint8 flag;
    ScalarValue* dest_val = NULL;
    uint8* dest_flag = NULL;
    VectorBatch* result_batch = NULL;

    if (batch == NULL)
        return;

    switch (varno) {
        case 0:
            if (subPlanClause->outExprBatch == NULL) {
                subPlanClause->outExprBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, batch);
                subPlanClause->outExprBatch->m_rows = 1;
                subPlanClause->outExprBatch->FixRowCount();
                if (batch->m_sysColumns != NULL) {
                    subPlanClause->outExprBatch->m_sysColumns = (SysColContainer*)palloc(sizeof(SysColContainer));
                    InitSystemColumns(subPlanClause->outExprBatch->m_sysColumns, batch->m_sysColumns);
                }
            }

            result_batch = subPlanClause->outExprBatch;
            break;
        case 1:
            if (subPlanClause->innerExprBatch == NULL) {
                subPlanClause->innerExprBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, batch);
                subPlanClause->innerExprBatch->m_rows = 1;
                subPlanClause->innerExprBatch->FixRowCount();
                if (batch->m_sysColumns != NULL) {
                    subPlanClause->innerExprBatch->m_sysColumns = (SysColContainer*)palloc(sizeof(SysColContainer));
                    InitSystemColumns(subPlanClause->innerExprBatch->m_sysColumns, batch->m_sysColumns);
                }
            }

            result_batch = subPlanClause->innerExprBatch;
            break;
        case 2:
            if (subPlanClause->scanExprBatch == NULL) {
                subPlanClause->scanExprBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, batch);
                subPlanClause->scanExprBatch->m_rows = 1;
                subPlanClause->scanExprBatch->FixRowCount();
                if (batch->m_sysColumns != NULL) {
                    subPlanClause->scanExprBatch->m_sysColumns = (SysColContainer*)palloc(sizeof(SysColContainer));
                    InitSystemColumns(subPlanClause->scanExprBatch->m_sysColumns, batch->m_sysColumns);
                }
            }

            result_batch = subPlanClause->scanExprBatch;
            break;
        case 3:
            if (subPlanClause->aggExprBatch == NULL) {
                subPlanClause->aggExprBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, batch);
                subPlanClause->aggExprBatch->m_rows = 1;
                subPlanClause->aggExprBatch->FixRowCount();
                if (batch->m_sysColumns != NULL) {
                    subPlanClause->aggExprBatch->m_sysColumns = (SysColContainer*)palloc(sizeof(SysColContainer));
                    InitSystemColumns(subPlanClause->aggExprBatch->m_sysColumns, batch->m_sysColumns);
                }
            }

            result_batch = subPlanClause->aggExprBatch;
            break;
        default:
            break;
    }

    if (idx < batch->m_rows) {
        for (int i = 0; i < batch->m_cols; i++) {
            val = batch->m_arr[i].m_vals[idx];
            flag = batch->m_arr[i].m_flag[idx];

            dest_val = result_batch->m_arr[i].m_vals;
            dest_flag = result_batch->m_arr[i].m_flag;

            if (NOT_NULL(flag)) {
                dest_val[0] = val;
                SET_NOTNULL(dest_flag[0]);
            } else {
                SET_NULL(dest_flag[0]);
            }
            result_batch->m_arr[i].m_rows = 1;
        }

        if (batch->m_sysColumns != NULL) {
            FetchOneSystemColumnRowByIdx(result_batch->m_sysColumns, batch->m_sysColumns, idx);
        }
    }
}

/*
 * @Description: ProcessRelatedSubplan
 * For correlated subplan, deal one tuple at a time.
 * If sublink type is ANY_SUBLINK, we don't fetch more tuple if one tuple satisfies the condition
 * If sublink type is ALL_SUBLINK, we don't fetch more tuple if one tuple doesn't satisfy the condition
 * @param[template] subType: sublink type
 * @param[IN] pSel: to indicate wheather the result to be evaluated
 * @param[IN] pVector: a vector to save result
 * @param[IN] result: a result of the expression evaluation to be used filling pVector
 * @param[IN] pResVectorIdx: the current row to be fiiled of the pVector
 * @param[IN] break_flag: a flag to mark wheather early exit the loop
 * @return: void
 */
template <SubLinkType subType>
void ProcessRelatedSubplan(bool* pSel, ScalarVector* pVector, ScalarVector* result, int pResVectorIdx, bool* break_flag)
{
    if (pSel[0]) {
        if (IS_NULL(result->m_flag[0]))
            SET_NULL(pVector->m_flag[pResVectorIdx]);
        else {
            if (subType == ANY_SUBLINK) {
                if (result->m_vals[0]) {
                    pVector->m_vals[pResVectorIdx] = 1;
                    SET_NOTNULL(pVector->m_flag[pResVectorIdx]);
                    pSel[0] = false;
                    (*break_flag) = true;
                }
            } else if (subType == ALL_SUBLINK) {
                if (!result->m_vals[0]) {
                    pVector->m_vals[pResVectorIdx] = 0;
                    SET_NOTNULL(pVector->m_flag[pResVectorIdx]);
                    pSel[0] = false;
                    (*break_flag) = true;
                }
            } else {
                pVector->m_vals[pResVectorIdx] = result->m_vals[0];
                SET_NOTNULL(pVector->m_flag[pResVectorIdx]);
                pSel[0] = false;
            }
        }
    }
}

/*
 * @Description: ProcessUnrelatedSubplan
 * For un-correlated subplan, deal more than one tuple at a time.
 * If sublink type is ANY_SUBLINK, we don't fetch more tuple if one tuple satisfies the condition
 * If sublink type is ALL_SUBLINK, we don't fetch more tuple if one tuple doesn't satisfy the condition
 * @param[template] subType: sublink type
 * @param[IN] pSel: to indicate wheather the result to be evaluated
 * @param[IN] pVector: a vector to save result
 * @param[IN] result: a result of the expression evaluation to be used filling pVector
 * @param[IN] pResVectorIdx: the current row to be fiiled of the pVector
 * @param[IN] break_flag: a flag to mark wheather early exit the loop
 * @return: void
 */
template <SubLinkType subType>
void ProcessUnrelatedSubplan(
    bool* pSel, ScalarVector* pVector, ScalarVector* result, int* pResVectorIdx, bool* break_flag)
{
    int i = 0;

    if (subType == ANY_SUBLINK || subType == ALL_SUBLINK) {
        for (i = 0; i < result->m_rows; i++) {
            if (pSel[i]) {
                if (IS_NULL(result->m_flag[i]))
                    SET_NULL(pVector->m_flag[i]);
                else {
                    if (subType == ANY_SUBLINK && result->m_vals[i]) {
                        pVector->m_vals[i] = 1;
                        SET_NOTNULL(pVector->m_flag[i]);
                        pSel[i] = false;
                    }

                    if (subType == ALL_SUBLINK && !result->m_vals[i]) {
                        pVector->m_vals[i] = 0;
                        SET_NOTNULL(pVector->m_flag[i]);
                        pSel[i] = false;
                    }
                }
            }
        }

        bool flag = true;
        for (i = 0; i < result->m_rows; i++) {
            if (pSel[i]) {
                flag = false;
                break;
            }
        }

        /*
         * All rows in the batch finish evaluating ALL/ANY expression,
         * don't need to fetch more tuple in Subplan and can exit early.
         */
        if (flag) {
            (*break_flag) = true;
        }

        (*pResVectorIdx) = result->m_rows;
    } else {
        if (IS_NULL(result->m_flag[*pResVectorIdx])) {
            SET_NULL(pVector->m_flag[*pResVectorIdx]);
        } else {
            pVector->m_vals[*pResVectorIdx] = result->m_vals[*pResVectorIdx];
            SET_NOTNULL(pVector->m_flag[*pResVectorIdx]);
        }
        pSel[*pResVectorIdx] = false;
    }
}

/*
 * @Description: ExecVecScanSubplan
 * Default case where we have to rescan subplan each time
 * @Param[template] subType: subplan type
 * @Param[template] correlated: indicate subplan is correlated or non-correlated
 * @Param[template] subType: subLink type
 * @param[IN] subPlanClause:  VecScan SubPlan state
 * @param[IN] econtext: a context for expression qualifications and tuple projections
 * @param[IN] pSelection: if pSelection not null, pSelection[i] means whether econtext->ecxt_xxxbatch[i] used in
 * expression evaluation, or all econtext->ecxt_xxxbatch[] would be used.
 * @param[OUT] pVector: a vector to save result
 * @return: result scalarvector
 */
template <SubLinkType subType, bool correlated>
static ScalarVector* ExecVecScanSubplan(
    SubPlanState* subPlanClause, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    SubPlan* sub_plan = (SubPlan*)subPlanClause->xprstate.expr;
    PlanState* plan_state = subPlanClause->planstate;
    ListCell* var = NULL;
    ListCell* l = NULL;
    ScalarVector** param_vector_array = NULL;
    int res_vector_idx = 0;
    ScalarDesc desc;
    int row_index = 0;
    int para_rows = 0;
    bool found = false;
    bool break_flag = false;
    VectorBatch* outer_batch = NULL;
    VectorBatch* inner_batch = NULL;
    VectorBatch* scan_batch = NULL;
    VectorBatch* agg_batch = NULL;
    bool saved_use_selection = econtext->m_fUseSelection;
    int idx = 0;
    errno_t rc = 0;
    bool* sel = subPlanClause->pSel;
    int temp_pararows = BatchMaxSize;
    int old_align_rows = 0;
    TupleTableSlot* slot = NULL;
    bool is_null = false;
    ScalarVector* tmpvec = subPlanClause->tempvector;
    MemoryContext oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

    if (pSelection != NULL)
        rc = memcpy_s(sel, sizeof(bool) * BatchMaxSize, pSelection, sizeof(bool) * BatchMaxSize);
    else
        rc = memset_s(sel, sizeof(bool) * BatchMaxSize, 1, sizeof(bool) * BatchMaxSize);
    securec_check(rc, "\0", "\0");

    if (subType == ANY_SUBLINK) {
        rc = memset_s(pVector->m_vals, sizeof(ScalarValue) * BatchMaxSize, 0, sizeof(ScalarValue) * BatchMaxSize);
        securec_check(rc, "\0", "\0");
        rc = memset_s(pVector->m_flag, sizeof(uint8) * BatchMaxSize, 0, sizeof(uint8) * BatchMaxSize);
        securec_check(rc, "\0", "\0");
    }

    if (subType == ALL_SUBLINK) {
        for (int j = 0; j < BatchMaxSize; j++)
            pVector->m_vals[j] = 1;
        rc = memset_s(pVector->m_flag, sizeof(uint8) * BatchMaxSize, 0, sizeof(uint8) * BatchMaxSize);
        securec_check(rc, "\0", "\0");
    }

    /* Reset m_buf memory */
    pVector->m_buf->Reset();

    if (correlated) {
        // get using batch from context
        outer_batch = econtext->ecxt_outerbatch;
        inner_batch = econtext->ecxt_innerbatch;
        scan_batch = econtext->ecxt_scanbatch;
        agg_batch = econtext->ecxt_aggbatch;

        Assert(list_length(sub_plan->parParam) == list_length(subPlanClause->args));
        Assert(econtext->align_rows != 0);

        // initialize the array container.
        if (unlikely(subPlanClause->pParamVectorArray == NULL)) {
            param_vector_array = (ScalarVector**)palloc0(sizeof(ScalarVector*) * list_length(subPlanClause->args));
            subPlanClause->pParamVectorTmp =
                (ScalarVector**)palloc0(sizeof(ScalarVector*) * list_length(subPlanClause->args));
            int index = 0;
            foreach (l, sub_plan->parParam) {
                int paramid = lfirst_int(l);
                ParamExecData* prm = &(econtext->ecxt_param_exec_vals[paramid]);
                param_vector_array[index] = New(CurrentMemoryContext) ScalarVector();
                desc.typeId = prm->valueType;
                desc.encoded = COL_IS_ENCODE(desc.typeId);
                param_vector_array[index]->init(CurrentMemoryContext, desc);
                index++;
            }
            subPlanClause->pParamVectorArray = param_vector_array;
            subPlanClause->idx = 0;
        }
    }
    idx = 0;
    // evaluate each  para vector.
    foreach (var, subPlanClause->args) {
        subPlanClause->pParamVectorTmp[idx] = ExecVectorExprEngineSwitchContext(
            (ExprState*)lfirst(var), econtext, pSelection, subPlanClause->pParamVectorArray[idx], isDone);

        temp_pararows = Min(temp_pararows, subPlanClause->pParamVectorTmp[idx]->m_rows);
        idx++;
    }

    // reset the row index.
    subPlanClause->idx = 0;
    if (subPlanClause->pParamVectorTmp && subPlanClause->pParamVectorTmp[0])
        para_rows = temp_pararows;

    /* skip early free for rescan subplan */
    bool orig_early_free = plan_state->state->es_skip_early_free;
    bool orig_early_deinit = plan_state->state->es_skip_early_deinit_consumer;

    plan_state->state->es_skip_early_free = true;
    plan_state->state->es_skip_early_deinit_consumer = true;

    // double loop
    // first loop the array to set the parameter;
    do {
        ArrayBuildState* astate = NULL;
        idx = 0;
        row_index = subPlanClause->idx;

        /* For correlated sub_plan, skip those filtered rows to reduce rescan times. */
        if (correlated && econtext->m_fUseSelection && pSelection && !pSelection[row_index]) {
            subPlanClause->idx++;
            res_vector_idx++;
            continue;
        }

        foreach (l, sub_plan->parParam) {
            int paramid = lfirst_int(l);
            ParamExecData* prm = &(econtext->ecxt_param_exec_vals[paramid]);

            // set the parameter
            prm->valueType = subPlanClause->pParamVectorTmp[idx]->m_desc.typeId;

            GenericArgExtract func = ChooseExtractFun(prm->valueType);
            Datum value = subPlanClause->pParamVectorTmp[idx]->m_vals[row_index];
            prm->value = func(&value);

            prm->isnull = IS_NULL(subPlanClause->pParamVectorTmp[idx]->m_flag[row_index]);
            prm->isChanged = true;

            // set the change bitmap set.
            plan_state->chgParam = bms_add_member(plan_state->chgParam, paramid);
            idx++;
        }

        if (correlated) {
            /*
             * The batch in the context to be saved up, and then each time the structure of
             * a row of batch in the context for the vector expression calculate.
             * The batch will be restored at the end of this function.
             */
            FetchOneRowByIdx<0>(subPlanClause, outer_batch, row_index);
            FetchOneRowByIdx<1>(subPlanClause, inner_batch, row_index);
            FetchOneRowByIdx<2>(subPlanClause, scan_batch, row_index);
            FetchOneRowByIdx<3>(subPlanClause, agg_batch, row_index);

            econtext->ecxt_outerbatch = subPlanClause->outExprBatch;
            econtext->ecxt_innerbatch = subPlanClause->innerExprBatch;
            econtext->ecxt_scanbatch = subPlanClause->scanExprBatch;
            econtext->ecxt_aggbatch = subPlanClause->aggExprBatch;
        }

        subPlanClause->idx++;
        ExecReScan(plan_state);

        // second loop to get all rows for this sub plan.
        found = false;
        break_flag = false;
        do {
            /*
             * the boundary should be row to vector
             * OrigValue stores the value of the outermost sub_plan
             */

            slot = ExecProcNode(plan_state);

            if (unlikely(TupIsNull(slot)))
                break;

            if (correlated) {
                // In correlated sub_plan, expression only need one row
                if (pSelection != NULL)
                    sel[0] = pSelection[row_index];
                else
                    sel[0] = 1;
            }

            // build result
            switch (subType) {
                case EXISTS_SUBLINK: {
                    pVector->m_vals[res_vector_idx] = BoolGetDatum(true);
                    SET_NOTNULL(pVector->m_flag[res_vector_idx]);
                    found = true;
                    break_flag = true;
                } break;

                case EXPR_SUBLINK: {
                    // flag find one row
                    if (found) {
                        ereport(ERROR,
                            (errcode(ERRCODE_CARDINALITY_VIOLATION),
                                errmsg("more than one row returned by a subquery used as an expression")));
                    }

                    found = true;
                    /* Get the Table Accessor Method*/
                    Assert(slot->tts_tupleDescriptor != NULL);
                    Datum res = tableam_tslot_getattr(slot, 1, &is_null);
                    if (pVector->m_desc.typeId == 0) {
                        pVector->m_desc.typeId = slot->tts_tupleDescriptor->attrs[0].atttypid;
                        pVector->m_desc.encoded = COL_IS_ENCODE(pVector->m_desc.typeId);
                    }
                    if (!is_null) {
                        if (COL_IS_ENCODE(pVector->m_desc.typeId)) {
                            ScalarValue result;
                            pVector->m_desc.encoded = true;
                            /*
                             *  In vector engine, if one type's length > 8, we will treat it as a varlena type.
                             *  So add header for these types.
                             *  Like: MACADDR/TIMETZ/TINTERVAL/INTERVAL/NAME/UNKNOWN/CSTRING
                             */
                            result =
                                ScalarVector::DatumToScalar(res, slot->tts_tupleDescriptor->attrs[0].atttypid, false);
                            /* Need copy to encode datatype */
                            int key_size = VARSIZE_ANY(result);
                            char* addr = pVector->m_buf->Allocate(key_size);
                            rc = memcpy_s(addr, key_size, DatumGetPointer(result), key_size);
                            securec_check(rc, "\0", "\0");
                            pVector->m_vals[res_vector_idx] = PointerGetDatum(addr);
                            if (result != res)
                                pfree(DatumGetPointer(result));
                        } else {
                            /* In vector engine, tid data should be copyed */
                            if (slot->tts_tupleDescriptor->attrs[0].atttypid == TIDOID) {
                                pVector->m_vals[res_vector_idx] = 0;
                                ItemPointer dest_tid = (ItemPointer)&pVector->m_vals[res_vector_idx];
                                ItemPointer src_tid = (ItemPointer)DatumGetPointer(res);
                                *dest_tid = *src_tid;
                                pVector->m_desc.typeId = INT8OID;
                            } else
                                pVector->m_vals[res_vector_idx] = res;
                        }
                        SET_NOTNULL(pVector->m_flag[res_vector_idx]);
                    } else {
                        SET_NULL(pVector->m_flag[res_vector_idx]);
                    }
                } break;
                case ARRAY_SUBLINK: {
                    Datum dvalue;
                    bool disnull = false;

                    found = true;
                    /* stash away current value */
                    /* Get the Table Accessor Method*/
                    Assert(slot->tts_tupleDescriptor != NULL);
                    dvalue = tableam_tslot_getattr(slot, 1, &disnull);
                    astate = accumArrayResult(astate, dvalue, disnull, sub_plan->firstColType, oldcontext);
                } break;
                case ANY_SUBLINK:
                case ALL_SUBLINK:
                case ROWCOMPARE_SUBLINK: {
                    /* cannot allow multiple input tuples for ROWCOMPARE sublink either */
                    if (subType == ROWCOMPARE_SUBLINK && found)
                        ereport(ERROR,
                            (errcode(ERRCODE_CARDINALITY_VIOLATION),
                                errmsg("more than one row returned by a subquery used as an expression")));
                    found = true;
                    ListCell* plst = NULL;
                    int col = 1;
                    foreach (plst, sub_plan->paramIds) {
                        int paramid = lfirst_int(plst);
                        ParamExecData* prmdata = NULL;

                        prmdata = &(econtext->ecxt_param_exec_vals[paramid]);
                        Assert(prmdata->execPlan == NULL);
                        /* Get the Table Accessor Method*/
                        Assert(slot->tts_tupleDescriptor != NULL);
                        prmdata->value = tableam_tslot_getattr(slot, col, &(prmdata->isnull));
                        prmdata->isChanged = true;
                        prmdata->valueType = slot->tts_tupleDescriptor->attrs[col - 1].atttypid;
                        col++;
                    }

                    ScalarVector* result = NULL;
                    MemoryContext old_context;

                    econtext->m_fUseSelection = true;

                    /*
                     * Switch to ecxt_per_batch_memory so that we can reset these memory as soon as we
                     * get the result from VectorExprEngine.
                     */
                    if (correlated) {
                        old_align_rows = econtext->align_rows;
                        econtext->align_rows = 1;
                    }

                    MemoryContextReset(subPlanClause->ecxt_per_batch_memory);
                    old_context = MemoryContextSwitchTo(subPlanClause->ecxt_per_batch_memory);
                    result = VectorExprEngine(subPlanClause->testexpr, econtext, sel, tmpvec, isDone);
                    (void)MemoryContextSwitchTo(old_context);

                    if (correlated)
                        econtext->align_rows = old_align_rows;
                    /*
                     * Consider a test case
                     *
                     * create table ta1 (v1 int, v2 int)  with(orientation=column);
                     * insert into ta1 values(1,2),(2,3),(3,4);
                     *
                     * create table ta2 (v1 int, v2 int)  with(orientation=column);
                     * insert into ta2 values(1,2),(2,3),(3,4);
                     *
                     *	For the result set returned by sub_plan, each time we fetch one tuple call the ExecProcNode,
                     *	put the attribute of the tuple to ParamExecData for the caculation of expressions.
                     * (1).  non-correlated sub-query
                     *	eg.  select * from ta1 where ta1.v1 < any (select v2 from ta2);
                     *	For non-correlated sub_plan, the loops is the rows returned by the sub_plan.
                     *	For this case, loops is 3
                     *	first loop: (1,2,3) op (1)
                     *	second loop:  (1,2,3) op (2)
                     *	third loop:  (1,2,3) op (3)
                     * (2). correlated sub-query
                     * eg.	select * from ta1 where ta1.v1 < any (select ta2.v2 from ta2 where ta2.v1 = ta1.v2);
                     * For correlated sub_plan, the loops is (the rows of ta1)  *  (the rows returned by sub_plan rely on
                     *ta1.v2) . for (ta1.v1,ta1.v2) = (1,2), sub_plan returns 2, only one loop 1 op 2 for (ta1.v1,ta1.v2)
                     *= (2,3), sub_plan returns 3, only one loop 2 op 3 for (ta1.v1,ta1.v2) = (3,4), sub_plan returns no
                     *tuple
                     */
                     
                    if (correlated)
                        ProcessRelatedSubplan<subType>(sel, pVector, result, res_vector_idx, &break_flag);
                    else
                        ProcessUnrelatedSubplan<subType>(sel, pVector, result, &res_vector_idx, &break_flag);
                } break;
                default:
                    break;
            }
        } while (break_flag == false);

        if (subType == ARRAY_SUBLINK) {
            Datum result;
            /* We return the result in the caller's context */
            if (astate != NULL)
                result = makeArrayResult(astate, oldcontext);
            else
                result = PointerGetDatum(construct_empty_array(sub_plan->firstColType));

            pVector->m_vals[res_vector_idx] = result;
            SET_NOTNULL(pVector->m_flag[res_vector_idx]);
        } else if (found == false) {
            switch (subType) {
                // not get , set false
                case EXISTS_SUBLINK: {
                    pVector->m_vals[res_vector_idx] = BoolGetDatum(false);
                    SET_NOTNULL(pVector->m_flag[res_vector_idx]);
                } break;

                case EXPR_SUBLINK:
                case ROWCOMPARE_SUBLINK: {
                    pVector->m_vals[res_vector_idx] = (Datum)0;
                    SET_NULL(pVector->m_flag[res_vector_idx]);
                } break;
                default:
                    break;
            }
        }

        if ((subType != ANY_SUBLINK && subType != ALL_SUBLINK) || correlated)
            res_vector_idx++;
    } while (subPlanClause->idx < para_rows);

    plan_state->state->es_skip_early_free = orig_early_free;
    plan_state->state->es_skip_early_deinit_consumer = orig_early_deinit;

    if (correlated) {
        // restore econtext's batch
        econtext->ecxt_outerbatch = outer_batch;
        econtext->ecxt_innerbatch = inner_batch;
        econtext->ecxt_scanbatch = scan_batch;
        econtext->ecxt_aggbatch = agg_batch;
    }

    (void)MemoryContextSwitchTo(oldcontext);

    econtext->m_fUseSelection = saved_use_selection;

    if (res_vector_idx == 0 && (subType == ANY_SUBLINK || subType == ALL_SUBLINK))
        pVector->m_rows = BatchMaxSize;
    else
        pVector->m_rows = res_vector_idx;

    return pVector;
}

template <SubLinkType subType>
ScalarVector* ExecVecSubPlan(
    SubPlanState* subPlanClause, ExprContext* econtext, bool* pSelection, ScalarVector* pVector, ExprDoneCond* isDone)
{
    SubPlan* sub_plan = (SubPlan*)subPlanClause->xprstate.expr;
    EState* estate = subPlanClause->planstate->state;
    ScanDirection direction;
    ScalarVector* retval = NULL;

    /* Sanity checks */
    if (sub_plan->subLinkType == CTE_SUBLINK)
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("CTE subplans should not be executed via ExecSubPlan")));
    if (sub_plan->setParam != NIL)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot set parent params from subquery")));
    /*
     * When executing a SubPlan in an expression, the EState's direction field was left alone,
     * resulting in an attempt to execute the subplan backwards if it was encountered during a
     * backwards scan of a cursor. Under this condition, saving estate->es_direction first. Then
     * forward scan mode is forcibly set. After the subplan is executed, restore the estate->es_direction.
     */
    direction = estate->es_direction;
    estate->es_direction = ForwardScanDirection;

    if (sub_plan->useHashTable) {
        retval = ExecVecHashSubplan<subType>(subPlanClause, econtext, pSelection, pVector);
    } else {
        if (list_length(subPlanClause->args) > 0) {
            retval = ExecVecScanSubplan<subType, true>(subPlanClause, econtext, pSelection, pVector, isDone);
        } else {
            retval = ExecVecScanSubplan<subType, false>(subPlanClause, econtext, pSelection, pVector, isDone);
        }
    }

    /* restore the configuration of direction */
    estate->es_direction = direction;

    return retval;
}

/*
 * @Description: get_func_num
 * For Hash subplan, subexpressions will be initialized for row expression and vector expression. It must be restore the
 * List after row expression initialized if parent is a VecAggState or VecWindowAggState.
 * @param[IN] parent: a plan state
 * @return: return the number of aggs of funcs
 */
int get_func_num(PlanState* parent)
{
    if (IsA(parent, VecAggState)) {
        AggState* aggstate = (AggState*)parent;
        return aggstate->numaggs;
    } else if (IsA(parent, VecWindowAggState)) {
        WindowAggState* winstate = (WindowAggState*)parent;
        return winstate->numfuncs;
    }

    return 0;
}

/*
 * @Description: restore_list_value
 * For Hash subplan, subexpressions will be initialized for row expression and vector expression. It must be restore the
 * List after row expression initialized if parent is a VecAggState or VecWindowAggState.
 * @param[IN] parent: a plan state
 * @param[IN] agg_num: a list length to be remembered by get_func_num.
 * @return: void
 */
void restore_list_value(PlanState* parent, int agg_num)
{
    if (IsA(parent, VecAggState)) {
        AggState* aggstate = (AggState*)parent;
        if (aggstate->numaggs != agg_num) {
            if (aggstate->numaggs > agg_num) {
                int i = aggstate->numaggs - agg_num;
                do {
                    aggstate->aggs = list_delete_first(aggstate->aggs);
                    i--;
                } while (i > 0);
                aggstate->numaggs = agg_num;
            }
        }
    }

    if (IsA(parent, VecWindowAggState)) {
        WindowAggState* winstate = (WindowAggState*)parent;
        if (winstate->numfuncs != agg_num) {
            if (winstate->numfuncs > agg_num) {
                int i = winstate->numfuncs - agg_num;
                do {
                    winstate->funcs = list_delete_first(winstate->funcs);
                    i--;
                } while (i > 0);

                winstate->numfuncs = agg_num;
            }
        }
    }
}

/*
 * @Description: ExecInitVecSubPlan
 *
 * Create a SubPlanState for a SubPlan; this is the SubPlan-specific part
 * of ExecInitVecExpr().  We split it out so that it can be used for InitPlans
 * as well as regular SubPlans.  Note that we don't link the SubPlan into
 * the parent's subPlan list, because that shouldn't happen for InitPlans.
 * Instead, ExecInitVecExpr() does that one part.
 *
 */
SubPlanState* ExecInitVecSubPlan(SubPlan* subplan, PlanState* parent)
{
    SubPlanState* sstate = makeNode(SubPlanState);
    EState* estate = parent->state;
    ScalarDesc unknown_desc;
    int agg_nums = 0;

    ScalarVector* (*ptr)(SubPlanState* subPlanClause,
                        ExprContext* econtext,
                        bool* pSelection,
                        ScalarVector* pVector,
                        ExprDoneCond* isDone) = NULL;

    /* Make a dedicated memory context for the vector expression. */
    sstate->ecxt_per_batch_memory = AllocSetContextCreate(estate->es_query_cxt,
        "ExprVecContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    switch (subplan->subLinkType) {
        case EXISTS_SUBLINK:
            ptr = ExecVecSubPlan<EXISTS_SUBLINK>;
            break;

        case ALL_SUBLINK:
            ptr = ExecVecSubPlan<ALL_SUBLINK>;
            break;

        case EXPR_SUBLINK:
            ptr = ExecVecSubPlan<EXPR_SUBLINK>;
            break;

        case ANY_SUBLINK:
            ptr = ExecVecSubPlan<ANY_SUBLINK>;
            break;

        case ROWCOMPARE_SUBLINK:
            ptr = ExecVecSubPlan<ROWCOMPARE_SUBLINK>;
            break;

        case ARRAY_SUBLINK:
            ptr = ExecVecSubPlan<ARRAY_SUBLINK>;
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unsupported vector sub plan type %d", subplan->subLinkType)));
    }
    sstate->xprstate.vecExprFun = (VectorExprFun)ptr;
    sstate->xprstate.expr = (Expr*)subplan;

    /* Link the SubPlanState to already-initialized subplan */
    sstate->planstate = (PlanState*)list_nth(estate->es_subplanstates, subplan->plan_id - 1);

    /* Initialize subexpressions */
    if (subplan->useHashTable) {
        agg_nums = get_func_num(parent);
        sstate->row_testexpr = ExecInitExpr((Expr*)subplan->testexpr, parent);
        restore_list_value(parent, agg_nums);
    }

    sstate->testexpr = ExecInitVecExpr((Expr*)subplan->testexpr, parent);
    sstate->args = (List*)ExecInitVecExpr((Expr*)subplan->args, parent);

    /*
     * initialize my state
     */
    sstate->curTuple = NULL;
    sstate->curArray = PointerGetDatum(NULL);
    sstate->projLeft = NULL;
    sstate->projRight = NULL;
    sstate->hashtable = NULL;
    sstate->hashnulls = NULL;
    sstate->hashtablecxt = NULL;
    sstate->hashtempcxt = NULL;
    sstate->innerecontext = NULL;
    sstate->keyColIdx = NULL;
    sstate->tab_hash_funcs = NULL;
    sstate->tab_eq_funcs = NULL;
    sstate->lhs_hash_funcs = NULL;
    sstate->cur_eq_funcs = NULL;
    sstate->tempvector = New(CurrentMemoryContext) ScalarVector();
    sstate->tempvector->init(CurrentMemoryContext, unknown_desc);

    sstate->pSel = (bool*)palloc(sizeof(bool) * BatchMaxSize);

    /*
     * If this plan is un-correlated or undirect correlated one and want to
     * set params for parent plan then mark parameters as needing evaluation.
     *
     * A CTE subplan's output parameter is never to be evaluated in the normal
     * way, so skip this in that case.
     *
     * Note that in the case of un-correlated subqueries we don't care about
     * setting parent->chgParam here: indices take care about it, for others -
     * it doesn't matter...
     */
    bool isSetParam = subplan->setParam != NIL && subplan->subLinkType != CTE_SUBLINK;
    if (isSetParam) {
        ListCell* lst = NULL;

        foreach (lst, subplan->setParam) {
            int paramid = lfirst_int(lst);
            ParamExecData* prm = &(estate->es_param_exec_vals[paramid]);

            prm->execPlan = sstate;
        }
    }

    /*
     * If we are going to hash the subquery output, initialize relevant stuff.
     * (We don't create the hashtable until needed, though.)
     */
    if (subplan->useHashTable) {
        int ncols, i;
        TupleDesc tup_desc;
        TupleTableSlot* slot = NULL;
        List* oplist = NIL;
        List* lefttlist = NIL;
        List* righttlist = NIL;
        List* leftptlist = NIL;
        List* rightptlist = NIL;
        ListCell* l = NULL;

        /* We need a memory context to hold the hash table(s) */
        sstate->hashtablecxt = AllocSetContextCreate(CurrentMemoryContext,
            "Subplan HashTable Context",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
        /* and a small one for the hash tables to use as temp storage */
        sstate->hashtempcxt = AllocSetContextCreate(CurrentMemoryContext,
            "Subplan HashTable Temp Context",
            ALLOCSET_SMALL_MINSIZE,
            ALLOCSET_SMALL_INITSIZE,
            ALLOCSET_SMALL_MAXSIZE);
        /* and a short-lived exprcontext for function evaluation */
        sstate->innerecontext = CreateExprContext(estate);
        /* Silly little array of column numbers 1..n */
        ncols = list_length(subplan->paramIds);
        sstate->keyColIdx = (AttrNumber*)palloc(ncols * sizeof(AttrNumber));
        for (i = 0; i < ncols; i++)
            sstate->keyColIdx[i] = i + 1;

        /*
         * We use ExecProject to evaluate the lefthand and righthand
         * expression lists and form tuples.  (You might think that we could
         * use the sub-select's output tuples directly, but that is not the
         * case if we had to insert any run-time coercions of the sub-select's
         * output datatypes; anyway this avoids storing any resjunk columns
         * that might be in the sub-select's output.) Run through the
         * combining expressions to build tlists for the lefthand and
         * righthand sides.  We need both the ExprState list (for ExecProject)
         * and the underlying parse Exprs (for ExecTypeFromTL).
         *
         * We also extract the combining operators themselves to initialize
         * the equality and hashing functions for the hash tables.
         */
        if (IsA(sstate->testexpr->expr, OpExpr)) {
            /* single combining operator */
            oplist = list_make1(sstate->testexpr);
        } else if (and_clause((Node*)sstate->testexpr->expr)) {
            /* multiple combining operators */
            Assert(IsA(sstate->testexpr, BoolExprState));
            oplist = ((BoolExprState*)sstate->testexpr)->args;
        } else {
            /* shouldn't see anything else in a hashable subplan */
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized testexpr type: %d", (int)nodeTag(sstate->testexpr->expr))));
            oplist = NIL; /* keep compiler quiet */
        }
        Assert(list_length(oplist) == ncols);

        lefttlist = righttlist = NIL;
        leftptlist = rightptlist = NIL;
        sstate->tab_hash_funcs = (FmgrInfo*)palloc(ncols * sizeof(FmgrInfo));
        sstate->tab_eq_funcs = (FmgrInfo*)palloc(ncols * sizeof(FmgrInfo));
        sstate->lhs_hash_funcs = (FmgrInfo*)palloc(ncols * sizeof(FmgrInfo));
        sstate->cur_eq_funcs = (FmgrInfo*)palloc(ncols * sizeof(FmgrInfo));
        i = 1;
        foreach (l, oplist) {
            FuncExprState* fstate = (FuncExprState*)lfirst(l);
            OpExpr* opexpr = (OpExpr*)fstate->xprstate.expr;
            ExprState* exstate = NULL;
            Expr* expr = NULL;
            TargetEntry* tle = NULL;
            GenericExprState* tlestate = NULL;
            Oid rhs_eq_oper;
            Oid left_hashfn;
            Oid right_hashfn;

            Assert(IsA(fstate, FuncExprState));
            Assert(IsA(opexpr, OpExpr));
            Assert(list_length(fstate->args) == 2);

            /* Process lefthand argument */
            exstate = (ExprState*)linitial(fstate->args);
            expr = exstate->expr;
            tle = makeTargetEntry(expr, i, NULL, false);
            tlestate = makeNode(GenericExprState);
            tlestate->xprstate.expr = (Expr*)tle;
            tlestate->xprstate.evalfunc = NULL;
            tlestate->arg = exstate;
            lefttlist = lappend(lefttlist, tlestate);
            leftptlist = lappend(leftptlist, tle);

            /* Lookup the equality function (potentially cross-type) */
            fmgr_info(opexpr->opfuncid, &sstate->cur_eq_funcs[i - 1]);
            fmgr_info_set_expr((Node*)opexpr, &sstate->cur_eq_funcs[i - 1]);

            /* Look up the equality function for the RHS type */
            if (!get_compatible_hash_operators(opexpr->opno, NULL, &rhs_eq_oper))
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                        errmsg("could not find compatible hash operator for operator %u", opexpr->opno)));
            fmgr_info(get_opcode(rhs_eq_oper), &sstate->tab_eq_funcs[i - 1]);

            /* Lookup the associated hash functions */
            if (!get_op_hash_functions(opexpr->opno, &left_hashfn, &right_hashfn))
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_FUNCTION),
                        errmsg("could not find hash function for hash operator %u", opexpr->opno)));
            fmgr_info(left_hashfn, &sstate->lhs_hash_funcs[i - 1]);
            fmgr_info(right_hashfn, &sstate->tab_hash_funcs[i - 1]);

            i++;
        }
        if (estate->es_is_flt_frame) {
            if (IsA(subplan->testexpr, OpExpr)) {
                /* single combining operator */
                oplist = list_make1(subplan->testexpr);
            } else if (and_clause((Node*)subplan->testexpr)) {
                /* multiple combining operators */
                Assert(IsA(subplan->testexpr, BoolExpr));
                oplist = ((BoolExpr*)subplan->testexpr)->args;
            } else {
                /* shouldn't see anything else in a hashable subplan */
                ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                         errmsg("unrecognized testexpr type: %d", (int)nodeTag(subplan->testexpr))));
                oplist = NIL; /* keep compiler quiet */
            }
            Assert(list_length(oplist) == ncols);

            i = 1;
            foreach (l, oplist) {
                OpExpr* opexpr = (OpExpr*)lfirst(l);
                Expr* expr = NULL;
                TargetEntry* tle = NULL;

                Assert(IsA(opexpr, OpExpr));
                Assert(list_length(opexpr->args) == 2);

                /* Process righthand argument */
                expr = (Expr*)lsecond(opexpr->args);
                tle = makeTargetEntry(expr, i, NULL, false);
                righttlist = lappend(righttlist, tle);

                i++;
            }
        } else {
            if (IsA(sstate->row_testexpr->expr, OpExpr)) {
                /* single combining operator */
                oplist = list_make1(sstate->row_testexpr);
            } else if (and_clause((Node*)sstate->row_testexpr->expr)) {
                /* multiple combining operators */
                Assert(IsA(sstate->row_testexpr, BoolExprState));
                oplist = ((BoolExprState*)sstate->row_testexpr)->args;
            } else {
                /* shouldn't see anything else in a hashable subplan */
                ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                         errmsg("unrecognized testexpr type: %d", (int)nodeTag(sstate->row_testexpr->expr))));
                oplist = NIL; /* keep compiler quiet */
            }
            Assert(list_length(oplist) == ncols);

            i = 1;
            foreach (l, oplist) {
                FuncExprState* fstate = (FuncExprState*)lfirst(l);
                ExprState* exstate = NULL;
                Expr* expr = NULL;
                TargetEntry* tle = NULL;
                GenericExprState* tlestate = NULL;

                Assert(IsA(fstate, FuncExprState));
                Assert(IsA((OpExpr*)fstate->xprstate.expr, OpExpr));
                Assert(list_length(fstate->args) == 2);

                /* Process righthand argument */
                exstate = (ExprState*)lsecond(fstate->args);
                expr = exstate->expr;
                tle = makeTargetEntry(expr, i, NULL, false);
                tlestate = makeNode(GenericExprState);
                tlestate->xprstate.expr = (Expr*)tle;
                tlestate->xprstate.evalfunc = NULL;
                tlestate->arg = exstate;
                righttlist = lappend(righttlist, tlestate);
                rightptlist = lappend(rightptlist, tle);

                i++;
            }
        }

        /*
         * Construct tupdescs, slots and projection nodes for left and right
         * sides.  The lefthand expressions will be evaluated in the parent
         * plan node's exprcontext, which we don't have access to here.
         * Fortunately we can just pass NULL for now and fill it in later
         * (hack alert!).  The righthand expressions will be evaluated in our
         * own innerecontext.
         */
        tup_desc = ExecTypeFromTL(leftptlist, false);
        slot = ExecInitExtraTupleSlot(estate);
        ExecSetSlotDescriptor(slot, tup_desc);
        sstate->projLeft = ExecBuildVecProjectionInfo(lefttlist, NULL, NULL, slot, NULL);

        if (estate->es_is_flt_frame) {
            tup_desc = ExecTypeFromTL(righttlist, false);
            slot = ExecInitExtraTupleSlot(estate);
            ExecSetSlotDescriptor(slot, tup_desc);
            sstate->projRight = ExecBuildProjectionInfoByFlatten(righttlist, sstate->innerecontext, slot, sstate->planstate, NULL);
        } else {
            tup_desc = ExecTypeFromTL(rightptlist, false);
            slot = ExecInitExtraTupleSlot(estate);
            ExecSetSlotDescriptor(slot, tup_desc);
            sstate->projRight = ExecBuildProjectionInfoByRecursion(righttlist, sstate->innerecontext, slot, NULL);
        }
    }

    return sstate;
}

/*
 * @Description: GetSlotfromBatchRow
 * convert  one row of a batch to a slot
 * @Param[OUT] slot: 	slot to store it in
 * @Param[IN] batch: the batch to be converted
 * @Param[IN] n_row:  the specified row to be converted
 * @return: return a slot
 */
static TupleTableSlot* GetSlotfromBatchRow(TupleTableSlot* slot, VectorBatch* batch, int n_row)
{
    int ncols = slot->tts_tupleDescriptor->natts;
    (void)ExecClearTuple(slot);
    ScalarVector* vector = NULL;
    ScalarValue val;
    Oid typid;
    for (int i = 0; i < ncols; i++) {
        vector = &batch->m_arr[i];
        if (IS_NULL(vector->m_flag[n_row])) {
            slot->tts_isnull[i] = true;
            continue;
        }
        val = vector->m_vals[n_row];
        typid = vector->m_desc.typeId;
        switch (typid) {
            case TIMETZOID:
            case TINTERVALOID:
            case INTERVALOID:
            case NAMEOID:
            case MACADDROID: {
                char* result = NULL;

                result = (char*)val + VARHDRSZ_SHORT;

                slot->tts_values[i] = PointerGetDatum(result);
            } break;
            case TIDOID: {
                slot->tts_values[i] = PointerGetDatum(val);
                break;
            }
            case UNKNOWNOID:
            case CSTRINGOID: {
                char* result = NULL;
                if (VARATT_IS_1B(val))
                    result = (char*)val + VARHDRSZ_SHORT;
                else
                    result = (char*)val + VARHDRSZ;
                slot->tts_values[i] = PointerGetDatum(result);
            } break;
            default:
                slot->tts_values[i] = (Datum)val;
                break;
        }

        slot->tts_isnull[i] = false;
    }

    return ExecStoreVirtualTuple(slot);
}
