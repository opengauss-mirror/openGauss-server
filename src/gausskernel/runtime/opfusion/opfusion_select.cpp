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
 * opfusion_select.cpp
 *        Definition of select operator for bypass executor.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/opfusion/opfusion_select.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "opfusion/opfusion_select.h"

SelectFusion::SelectFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    MemoryContext old_context = NULL;
    if (!IsGlobal()) {
        old_context = MemoryContextSwitchTo(m_global->m_context);
        InitGlobals();
        MemoryContextSwitchTo(old_context);
    } else {
        m_c_global = ((SelectFusion*)(psrc->opFusionObj))->m_c_global;
    }
    old_context = MemoryContextSwitchTo(m_local.m_localContext);
    InitLocals(params);
    MemoryContextSwitchTo(old_context);
}

void SelectFusion::InitGlobals()
{
    m_global->m_reloid = 0;
    m_c_global = (SelectFusionGlobalVariable*)palloc0(sizeof(SelectFusionGlobalVariable));

    m_c_global->m_limitCount = -1;
    m_c_global->m_limitOffset = -1;

    /* get limit num */
    if (IsA(m_global->m_planstmt->planTree, Limit)) {
        Limit* limit = (Limit*)m_global->m_planstmt->planTree;
        if (limit->limitCount != NULL && IsA(limit->limitCount, Const) && !((Const*)limit->limitCount)->constisnull) {
            m_c_global->m_limitCount = DatumGetInt64(((Const*)limit->limitCount)->constvalue);
        }
        if (limit->limitOffset != NULL && IsA(limit->limitOffset, Const) &&
            !((Const*)limit->limitOffset)->constisnull) {
            m_c_global->m_limitOffset = DatumGetInt64(((Const*)limit->limitOffset)->constvalue);
        }
    }
}

void SelectFusion::InitLocals(ParamListInfo params)
{
    m_local.m_tmpvals = NULL;
    m_local.m_values = NULL;
    m_local.m_isnull = NULL;
    m_local.m_tmpisnull = NULL;
    initParams(params);
    m_local.m_receiver = NULL;
    m_local.m_isInsideRec = true;
    
    Node* node = NULL;
    if (IsA(m_global->m_planstmt->planTree, Limit)) {
        node = JudgePlanIsPartIterator(m_global->m_planstmt->planTree->lefttree);
    } else {
        node = JudgePlanIsPartIterator(m_global->m_planstmt->planTree);
    }
    m_local.m_scan = ScanFusion::getScanFusion(node, m_global->m_planstmt,
                                               m_local.m_outParams ? m_local.m_outParams : m_local.m_params);
    if (!IsGlobal()) {
        MemoryContext old_context = MemoryContextSwitchTo(m_global->m_context);
        m_global->m_tupDesc = CreateTupleDescCopy(m_local.m_scan->m_tupDesc);
        MemoryContextSwitchTo(old_context);
    }
    m_local.m_reslot = NULL;
}

bool SelectFusion::execute(long max_rows, char* completionTag)
{
    MemoryContext oldContext = MemoryContextSwitchTo(m_local.m_tmpContext);

    bool success = false;
    int64 start_row = 0;
    int64 get_rows = 0;

    /*******************
     * step 1: prepare *
     *******************/
    start_row = m_c_global->m_limitOffset >= 0 ? m_c_global->m_limitOffset : start_row;
    int64 alreadyfetch = (m_local.m_position > start_row) ? (m_local.m_position - start_row) : 0;
    /* no limit get fetch size rows */
    get_rows = max_rows;
    if (m_c_global->m_limitCount >= 0) {
        /* fetch size, limit */
        int64 limit_row = (m_c_global->m_limitCount - alreadyfetch > 0) ? m_c_global->m_limitCount - alreadyfetch : 0;
        get_rows = (limit_row > max_rows) ? max_rows : limit_row;
    }

    /**********************
     * step 2: begin scan *
     **********************/
    if (m_local.m_position == 0) {
        m_local.m_scan->refreshParameter(m_local.m_outParams == NULL ? m_local.m_params : m_local.m_outParams);
        m_local.m_scan->Init(max_rows);
    }
    setReceiver();

    unsigned long nprocessed = 0;
    /* put selected tuple into receiver */
    TupleTableSlot* offset_reslot = NULL;
    while (m_local.m_position < (long)start_row && (offset_reslot = m_local.m_scan->getTupleSlot()) != NULL) {
        tpslot_free_heaptuple(offset_reslot);
        m_local.m_position++;
    }
    if (m_local.m_position < (long)start_row) {
        Assert(offset_reslot == NULL);
        get_rows = 0;
        m_local.m_isCompleted = true;
    }
    while (nprocessed < (unsigned long)get_rows && (m_local.m_reslot = m_local.m_scan->getTupleSlot()) != NULL) {
        CHECK_FOR_INTERRUPTS();
        m_local.m_position++;
        nprocessed++;
        (*m_local.m_receiver->receiveSlot)(m_local.m_reslot, m_local.m_receiver);
        tpslot_free_heaptuple(m_local.m_reslot);
    }
    if (!ScanDirectionIsNoMovement(*(m_local.m_scan->m_direction))) {
        bool has_complete = (max_rows == 0 || nprocessed < (unsigned long)max_rows);
        if (has_complete) {
            m_local.m_isCompleted = true;
        }
    } else {
        m_local.m_isCompleted = true;
    }
    /* for unnamed portal, should no need to wait for next E msg */
    if (m_local.m_portalName == NULL || m_local.m_portalName[0] == '\0') {
        m_local.m_isCompleted = true;
    }
    success = true;

    /****************
     * step 3: done *
     ****************/
    if (m_local.m_isInsideRec) {
        (*m_local.m_receiver->rDestroy)(m_local.m_receiver);
    }
    m_local.m_scan->End(m_local.m_isCompleted);
    if (m_local.m_isCompleted) {
        m_local.m_position = 0;
    }
    errno_t errorno =
        snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "SELECT %lu", nprocessed);
    securec_check_ss(errorno, "\0", "\0");
    MemoryContextSwitchTo(oldContext);

    /* instr unique sql - we assume that this is no nesting calling of Fusion::execute */
    UniqueSQLStatCountReturnedRows(nprocessed);

    return success;
}

void SelectFusion::close()
{
    if (m_local.m_isCompleted == false) {
        m_local.m_scan->End(true);
        m_local.m_isCompleted = true;
        m_local.m_position = 0;
        UnregisterSnapshot(m_local.m_snapshot);
        m_local.m_snapshot = NULL;
    }
}
