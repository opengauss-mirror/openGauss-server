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
 * vecnestloop.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecnestloop.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECNESTLOOP_H
#define VECNESTLOOP_H

#include "vecexecutor/vecnodes.h"

extern VectorBatch* ExecVecNestloop(VecNestLoopState* node);
extern VecNestLoopState* ExecInitVecNestLoop(VecNestLoop* node, EState* estate, int eflags);
extern void ExecEndVecNestLoop(VecNestLoopState* node);
extern void ExecReScanVecNestLoop(VecNestLoopState* node);

#define NL_NEEDNEWOUTER 1
#define NL_NEEDNEXTOUTROW 2
#define NL_EXECQUAL 3
#define NL_END 4

class VecNestLoopRuntime : public BaseObject {

public:
    VecNestLoopRuntime(VecNestLoopState* runtime);

    template <bool ifReturnNotFull>
    VectorBatch* JoinT();

    void Rescan();

    /* To avoid Coverity Warning: missing_user_dtor */
    virtual ~VecNestLoopRuntime();

private:
    template <bool ifTargetlistNull>
    void FetchOuterT();

    void NextOuterRow();

    void OutJoinBatchAlignInnerJoinBatch(int rows);

    template <JoinType type, bool doProject, bool hasJoinQual, bool hasOtherQual>
    VectorBatch* JoinQualT();

    template <JoinType type>
    void InitT(List* joinqual, List* otherqual, ProjectionInfo* projInfo);

    void BindingFp();

    VectorBatch* (VecNestLoopRuntime::*WrapperBatch)(VectorBatch* batch);

    VectorBatch* (VecNestLoopRuntime::*JoinOnQual)();

    void (VecNestLoopRuntime::*FetchOuter)();

private:
    VecNestLoopState* m_runtime;

    // runtime status
    //
    int m_status;

    // a batch with 1 null row, for left/anti join
    //
    VectorBatch* m_innerNullBatch;

    // outer batch
    //
    VectorBatch* m_outerBatch;

    // 1 row from outer batch, but we set for a batch max size
    //
    VectorBatch* m_outJoinBatch;

    int m_outReadIdx;

    JoinType m_joinType;

    bool m_matched;

    VectorBatch* m_currentBatch;

    VectorBatch* m_bckBatch;

    int m_bufferRows;

    bool m_SimpletargetCol;

    bool m_outerTargetIsNull;

    int m_outJoinBatchRows;
};

#endif
