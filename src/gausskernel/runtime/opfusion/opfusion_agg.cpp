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
 * opfusion_agg.cpp
 *        Definition of aggregate operator for bypass executor.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/opfusion/opfusion_agg.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "opfusion/opfusion_agg.h"

#include "access/tableam.h"
#include "catalog/pg_aggregate.h"

AggFusion::AggFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    MemoryContext old_context = NULL;

    if (!IsGlobal()) {
        old_context = MemoryContextSwitchTo(m_global->m_context);
        InitGlobals();
        MemoryContextSwitchTo(old_context);
    } else {
        m_c_global = ((AggFusion*)(psrc->opFusionObj))->m_c_global;
    }
    old_context = MemoryContextSwitchTo(m_local.m_localContext);
    InitLocals(params);
    MemoryContextSwitchTo(old_context);
}

void AggFusion::InitGlobals()
{
    m_c_global = (AggFusionGlobalVariable*)palloc0(sizeof(AggFusionGlobalVariable));
    m_global->m_reloid = 0;
    Agg *aggnode = (Agg *)m_global->m_planstmt->planTree;

    /* agg init */
    List *targetList = aggnode->plan.targetlist;
    TargetEntry *tar = (TargetEntry *)linitial(targetList);
    Aggref *aggref = (Aggref *)tar->expr;

    switch (aggref->aggfnoid) {
        case INT2SUMFUNCOID:
            m_c_global->m_aggSumFunc = &AggFusion::agg_int2_sum;
            break;
        case INT4SUMFUNCOID:
            m_c_global->m_aggSumFunc = &AggFusion::agg_int4_sum;
            break;
        case INT8SUMFUNCOID:
            m_c_global->m_aggSumFunc = &AggFusion::agg_int8_sum;
            break;
        case NUMERICSUMFUNCOID:
            m_c_global->m_aggSumFunc = &AggFusion::agg_numeric_sum;
            break;
        default:
            elog(ERROR, "unsupported aggfnoid %u for bypass.", aggref->aggfnoid);
            break;
    }

    HeapTuple aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggref->aggfnoid));
    if (!HeapTupleIsValid(aggTuple)) {
        elog(ERROR, "cache lookup failed for aggregate %u",
             aggref->aggfnoid);
    }
    ReleaseSysCache(aggTuple);

    m_global->m_tupDesc = ExecTypeFromTL(targetList, false);
    m_global->m_attrno = (int16 *) palloc(m_global->m_tupDesc->natts * sizeof(int16));

    /* m_global->m_tupDesc->natts always be 1 currently. */
    TargetEntry *res = NULL;
    res = (TargetEntry *)linitial(aggref->args);
    Var *var = (Var *)res->expr;
    m_global->m_attrno[0] = var->varattno;
}

void AggFusion::InitLocals(ParamListInfo params)
{
    initParams(params);
    m_local.m_receiver = NULL;
    m_local.m_isInsideRec = true;

    Agg *aggnode = (Agg *)m_global->m_planstmt->planTree;
    Node* scan = JudgePlanIsPartIterator(aggnode->plan.lefttree);
    m_local.m_scan = ScanFusion::getScanFusion(scan, m_global->m_planstmt,
                                               m_local.m_outParams ? m_local.m_outParams : m_local.m_params);

    m_local.m_reslot = MakeSingleTupleTableSlot(m_global->m_tupDesc);
    m_local.m_values = (Datum*)palloc0(m_global->m_tupDesc->natts * sizeof(Datum));
    m_local.m_isnull = (bool*)palloc0(m_global->m_tupDesc->natts * sizeof(bool));
}

bool AggFusion::execute(long max_rows, char *completionTag)
{
    max_rows = FETCH_ALL;
    bool success = false;

    TupleTableSlot *reslot = m_local.m_reslot;
    Datum* values = m_local.m_values;
    bool * isnull = m_local.m_isnull;
    for (int i = 0; i < m_global->m_tupDesc->natts; i++) {
        isnull[i] = true;
    }

    /* step 2: begin scan */
    m_local.m_scan->refreshParameter(m_local.m_outParams == NULL ? m_local.m_params : m_local.m_outParams);

    m_local.m_scan->Init(max_rows);

    setReceiver();

    long nprocessed = 0;
    TupleTableSlot *slot = NULL;
    while ((slot = m_local.m_scan->getTupleSlot()) != NULL) {
        CHECK_FOR_INTERRUPTS();
        nprocessed++;
        {
            AutoContextSwitch memSwitch(m_local.m_tmpContext);
            for (int i = 0; i < m_global->m_tupDesc->natts; i++) {
                reslot->tts_values[i] = slot->tts_values[m_global->m_attrno[i] - 1];
                reslot->tts_isnull[i] = slot->tts_isnull[m_global->m_attrno[i] - 1];
                (this->*(m_c_global->m_aggSumFunc))(&values[i], isnull[i],
                        &reslot->tts_values[i], reslot->tts_isnull[i]);
                isnull[i] = false;
            }
        }

        if (nprocessed >= max_rows) {
            break;
        }
    }

    HeapTuple tmptup = (HeapTuple)tableam_tops_form_tuple(m_global->m_tupDesc, values, isnull, HEAP_TUPLE);
    (void)ExecStoreTuple(tmptup, reslot, InvalidBuffer, false);

    tableam_tslot_getsomeattrs(reslot, m_global->m_tupDesc->natts);

    (*m_local.m_receiver->receiveSlot)(reslot, m_local.m_receiver);

    tpslot_free_heaptuple(m_local.m_reslot);

    success = true;

    /* step 3: done */
    if (m_local.m_isInsideRec == true) {
        (*m_local.m_receiver->rDestroy)(m_local.m_receiver);
    }

    m_local.m_isCompleted = true;
    m_local.m_scan->End(true);

    errno_t errorno = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
            "SELECT %lu", nprocessed);
    securec_check_ss(errorno, "\0", "\0");

    return success;
}

void
AggFusion::agg_int2_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull)
{
    int64 newval;

    if (unlikely(inIsNull)) {
        return;
    }

    if (unlikely(transIsNull)) {
        newval = (int64)DatumGetInt16(*inVal);
        *transVal = Int64GetDatum(newval);
        return;
    }

    int64 oldsum = DatumGetInt64(*transVal);
    newval = oldsum + (int64)DatumGetInt16(*inVal);
    *transVal = Int64GetDatum(newval);
}

void
AggFusion::agg_int4_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull)
{
    int64 newval;

    if (unlikely(inIsNull)) {
        return;
    }

    if (unlikely(transIsNull)) {
        newval = (int64)DatumGetInt32(*inVal);
        *transVal = Int64GetDatum(newval);
        return;
    }

    int64 oldsum = DatumGetInt64(*transVal);
    newval = oldsum + (int64)DatumGetInt32(*inVal);
    *transVal = Int64GetDatum(newval);
}

void
AggFusion::agg_int8_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull)
{
    Numeric     num1;
    Numeric     num2;
    Numeric     res;
    NumericVar  result;
    int64       val;
    Datum       newVal;
    NumericVar      arg1;
    NumericVar      arg2;

    if (unlikely(inIsNull)) {
        return;
    }

    if (unlikely(transIsNull)) {
        val = DatumGetInt64(*inVal);
        init_var(&result);
        int64_to_numericvar(val, &result);
        res = make_result(&result);
        free_var(&result);
        newVal = NumericGetDatum(res);
        newVal = datumCopy(newVal, false, -1);
        *transVal = newVal;

        return;
    }

    val = DatumGetInt64(*inVal);
    init_var(&result);
    int64_to_numericvar(val, &result);
    res = make_result(&result);
    free_var(&result);

    num1 = DatumGetNumeric(*transVal);
    num2 = res;

    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);
    init_var(&result);
    add_var(&arg1, &arg2, &result);

    res = make_result(&result);

    free_var(&result);
    newVal = NumericGetDatum(res);
    newVal = datumCopy(newVal, false, -1);

    if (likely(!transIsNull)) {
        pfree(DatumGetPointer(*transVal));
    }

    *transVal = newVal;

    return;
}

void AggFusion::agg_numeric_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull)
{
    Numeric     num1;
    Numeric     num2;
    Numeric     res;
    NumericVar  result;
    Datum       newVal;
    NumericVar      arg1;
    NumericVar      arg2;

    if (unlikely(inIsNull)) {
        return;
    }

    if (unlikely(transIsNull)) {
        *transVal = datumCopy(*inVal, false, -1);
        return;
    }

    num1 = DatumGetNumeric(*transVal);
    num2 = DatumGetNumeric(*inVal);

    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);
    init_var(&result);
    add_var(&arg1, &arg2, &result);

    res = make_result(&result);

    free_var(&result);
    newVal = NumericGetDatum(res);
    newVal = datumCopy(newVal, false, -1);

    if (likely(!transIsNull)) {
        pfree(DatumGetPointer(*transVal));
    }

    *transVal = newVal;

    return;
}