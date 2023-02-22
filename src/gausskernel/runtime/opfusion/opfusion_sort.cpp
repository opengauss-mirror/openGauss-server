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
 * opfusion_sort.cpp
 *        Definition of sort operator for bypass executor.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/opfusion/opfusion_sort.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "opfusion/opfusion_sort.h"

#include "access/tableam.h"

SortFusion::SortFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    MemoryContext old_context = NULL;
    if (!IsGlobal()) {
        old_context = MemoryContextSwitchTo(m_global->m_context);
        InitGlobals();
        MemoryContextSwitchTo(old_context);
    }
    old_context = MemoryContextSwitchTo(m_local.m_localContext);
    InitLocals(params);
    MemoryContextSwitchTo(old_context);
}

void SortFusion::InitLocals(ParamListInfo params)
{
    Plan *node = (Plan*)m_global->m_planstmt->planTree->lefttree;
    Sort *sortnode = (Sort *)m_global->m_planstmt->planTree;
    m_c_local.m_scanDesc = ExecTypeFromTL(node->targetlist, false);
    initParams(params);
    m_local.m_receiver = NULL;
    m_local.m_isInsideRec = true;
    m_local.m_scan = ScanFusion::getScanFusion((Node*)sortnode->plan.lefttree, m_global->m_planstmt,
                                               m_local.m_outParams ? m_local.m_outParams : m_local.m_params);
    m_c_local.m_scanDesc->td_tam_ops = m_local.m_scan->m_tupDesc->td_tam_ops;
    if (!IsGlobal())
        m_global->m_tupDesc->td_tam_ops = m_local.m_scan->m_tupDesc->td_tam_ops;

    m_local.m_reslot = MakeSingleTupleTableSlot(m_global->m_tupDesc, false, m_local.m_scan->m_tupDesc->td_tam_ops);
    m_local.m_values = (Datum*)palloc0(m_global->m_tupDesc->natts * sizeof(Datum));
    m_local.m_isnull = (bool*)palloc0(m_global->m_tupDesc->natts * sizeof(bool));
}

void SortFusion::InitGlobals()
{
    m_global->m_reloid = 0;
    Sort *sortnode = (Sort *)m_global->m_planstmt->planTree;
    m_global->m_tupDesc = ExecCleanTypeFromTL(sortnode->plan.targetlist, false);
    m_global->m_attrno = (int16 *)palloc(m_global->m_tupDesc->natts * sizeof(int16));

    ListCell *lc = NULL;
    int cur_resno = 1;
    foreach (lc, sortnode->plan.targetlist) {
        TargetEntry *res = (TargetEntry *)lfirst(lc);
        if (res->resjunk)
            continue;

        Var *var = (Var *)res->expr;
        m_global->m_attrno[cur_resno - 1] = var->varattno;
        cur_resno++;
    }
}

bool SortFusion::execute(long max_rows, char *completionTag)
{
    max_rows = FETCH_ALL;
    bool success = false;
    TimestampTz startTime = 0;
    TupleTableSlot *reslot = m_local.m_reslot;
    Datum *values = m_local.m_values;
    bool  *isnull = m_local.m_isnull;
    for (int i = 0; i < m_global->m_tupDesc->natts; i++) {
        isnull[i] = true;
    }

    UpdateUniqueSQLSortStats(NULL, &startTime);
    /* prepare */
    m_local.m_scan->refreshParameter(m_local.m_outParams == NULL ? m_local.m_params : m_local.m_outParams);

    m_local.m_scan->Init(max_rows);

    setReceiver();

    Tuplesortstate *tuplesortstate = NULL;
    Sort       *sortnode = (Sort *)m_global->m_planstmt->planTree;
    int64       sortMem = SET_NODEMEM(sortnode->plan.operatorMemKB[0], sortnode->plan.dop);
    int64       maxMem = (sortnode->plan.operatorMaxMem > 0) ?
        SET_NODEMEM(sortnode->plan.operatorMaxMem, sortnode->plan.dop) : 0;

    {
        AutoContextSwitch memSwitch(m_local.m_tmpContext);
        tuplesortstate = tuplesort_begin_heap(m_c_local.m_scanDesc,
                sortnode->numCols,
                sortnode->sortColIdx,
                sortnode->sortOperators,
                sortnode->collations,
                sortnode->nullsFirst,
                sortMem,
                false,
                maxMem,
                sortnode->plan.plan_node_id,
                SET_DOP(sortnode->plan.dop));
    }

    /* receive data from indexscan node */
    long nprocessed = 0;
    TupleTableSlot *slot = NULL;
    while ((slot = m_local.m_scan->getTupleSlot()) != NULL) {
        tuplesort_puttupleslot(tuplesortstate, slot);
    }

    /* sort all data */
    tuplesort_performsort(tuplesortstate);

    /* analyze the tuplesortstate information for update unique sql sort info */
    UpdateUniqueSQLSortStats(tuplesortstate, &startTime);

    /* send sorted data to client */
    slot = MakeSingleTupleTableSlot(m_c_local.m_scanDesc);
    while (tuplesort_gettupleslot(tuplesortstate, true, slot, NULL)) {
        tableam_tslot_getsomeattrs(slot, m_c_local.m_scanDesc->natts);
        for (int i = 0; i < m_global->m_tupDesc->natts; i++) {
            values[i] = slot->tts_values[m_global->m_attrno[i] - 1];
            isnull[i] = slot->tts_isnull[m_global->m_attrno[i] - 1];
        }

        HeapTuple tmptup = (HeapTuple)tableam_tops_form_tuple(m_global->m_tupDesc, values, isnull, 
                                                              m_global->m_tupDesc->td_tam_ops);
        (void)ExecStoreTuple(tmptup, reslot, InvalidBuffer, false);
        (*m_local.m_receiver->receiveSlot)(reslot, m_local.m_receiver);
        tableam_tops_free_tuple(tmptup);

        CHECK_FOR_INTERRUPTS();
        nprocessed++;
        if (nprocessed >= max_rows) {
            break;
        }
    }

    success = true;

    /* step 3: done */
    if (m_local.m_isInsideRec == true) {
        (*m_local.m_receiver->rDestroy)(m_local.m_receiver);
    }

    m_local.m_isCompleted = true;
    m_local.m_scan->End(true);
    tuplesort_end(tuplesortstate);

    errno_t errorno = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
            "SELECT %lu", nprocessed);
    securec_check_ss(errorno, "\0", "\0");

    return success;
}
