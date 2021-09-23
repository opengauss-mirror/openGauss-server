
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
 * opfusion_mot.cpp
 *        Definition of mot's select and modify operator for bypass executor.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/opfusion/opfusion_mot.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "opfusion/opfusion_mot.h"

#include "storage/mot/jit_exec.h"

#ifdef ENABLE_MOT
MotJitSelectFusion::MotJitSelectFusion(
    MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
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

void MotJitSelectFusion::InitGlobals()
{
    if (m_global->m_planstmt->planTree->targetlist) {
        m_global->m_tupDesc = ExecCleanTypeFromTL(m_global->m_planstmt->planTree->targetlist, false);
    } else {
        ereport(ERROR,
                (errmodule(MOD_EXECUTOR),
                 errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                 errmsg("unrecognized node type: %d when executing executor node.",
                        (int)nodeTag(m_global->m_planstmt->planTree))));
    }
}

void MotJitSelectFusion::InitLocals(ParamListInfo params)
{
    initParams(params);
    m_local.m_receiver = NULL;
    m_local.m_isInsideRec = true;
    m_local.m_reslot = MakeSingleTupleTableSlot(m_global->m_tupDesc);
}

bool MotJitSelectFusion::execute(long max_rows, char* completionTag)
{
    ParamListInfo params = (m_local.m_outParams != NULL) ? m_local.m_outParams : m_local.m_params;
    bool success = false;
    setReceiver();
    unsigned long nprocessed = 0;
    bool finish = false;
    int rc = 0;
    while (!finish) {
        uint64_t tpProcessed = 0;
        int scanEnded = 0;
        rc = JitExec::JitExecQuery(m_global->m_cacheplan->mot_jit_context, params, m_local.m_reslot,
                                   &tpProcessed, &scanEnded);
        if (scanEnded || (tpProcessed == 0) || (rc != 0)) {
            // raise flag so that next round we will bail out (current tuple still must be reported to user)
            finish = true;
        }
        CHECK_FOR_INTERRUPTS();
        if (tpProcessed > 0) {
            nprocessed++;
            (*m_local.m_receiver->receiveSlot)(m_local.m_reslot, m_local.m_receiver);
            (void)ExecClearTuple(m_local.m_reslot);
            if ((max_rows != FETCH_ALL) && (nprocessed == (unsigned long)max_rows)) {
                finish = true;
            }
        }
    }

    success = true;

    if (m_local.m_isInsideRec) {
        (*m_local.m_receiver->rDestroy)(m_local.m_receiver);
    }

    m_local.m_position = 0;
    m_local.m_isCompleted = true;

    errno_t errorno = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
                                 "SELECT %lu", nprocessed);
    securec_check_ss(errorno, "\0", "\0");

    return success;
}

MotJitModifyFusion::MotJitModifyFusion(
    MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
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

void MotJitModifyFusion::InitGlobals()
{
    m_global->m_reloid = getrelid(linitial_int(m_global->m_planstmt->resultRelations), m_global->m_planstmt->rtable);
    ModifyTable* node = (ModifyTable*)m_global->m_planstmt->planTree;
    BaseResult* baseresult = (BaseResult*)linitial(node->plans);
    List* targetList = baseresult->plan.targetlist;
    m_global->m_tupDesc = ExecTypeFromTL(targetList, false);
    m_global->m_paramNum = 0;
}

void MotJitModifyFusion::InitLocals(ParamListInfo params)
{
    ModifyTable* node = (ModifyTable*)m_global->m_planstmt->planTree;
    m_c_local.m_cmdType = node->operation;
    m_c_local.m_estate = CreateExecutorState();
    m_c_local.m_estate->es_range_table = m_global->m_planstmt->rtable;

    /* init param */
    m_local.m_reslot = MakeSingleTupleTableSlot(m_global->m_tupDesc);
    initParams(params);
    m_local.m_receiver = NULL;
    m_local.m_isInsideRec = true;
}

bool MotJitModifyFusion::execute(long max_rows, char* completionTag)
{
    bool success = false;
    uint64_t tpProcessed = 0;
    int scanEnded = 0;
    ParamListInfo params = (m_local.m_outParams != NULL) ? m_local.m_outParams : m_local.m_params;
    int rc = JitExec::JitExecQuery(m_global->m_cacheplan->mot_jit_context, params, m_local.m_reslot,
                                   &tpProcessed, &scanEnded);
    if (rc == 0) {
        (void)ExecClearTuple(m_local.m_reslot);
        success = true;
        errno_t ret = EOK;
        switch (m_c_local.m_cmdType)
        {
            case CMD_INSERT:
                ret = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
                    "INSERT 0 %lu", tpProcessed);
                securec_check_ss(ret, "\0", "\0");
                break;
            case CMD_UPDATE:
                ret = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
                    "UPDATE %lu", tpProcessed);
                securec_check_ss(ret, "\0", "\0");
                break;
            case CMD_DELETE:
                ret = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
                    "DELETE %lu", tpProcessed);
                securec_check_ss(ret, "\0", "\0");
                break;
            default:
                break;
        }
    }

    m_local.m_isCompleted = true;

    return success;
}
#endif /* ENABLE_MOT */