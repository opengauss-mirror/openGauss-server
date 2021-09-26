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
 * opfusion_mot.h
 *        Declaration of mot's select and modify operator for bypass executor.
 *
 * IDENTIFICATION
 *        src/include/opfusion/opfusion_mot.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_MOT_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_MOT_H_

#include "opfusion/opfusion.h"

#ifdef ENABLE_MOT
class MotJitSelectFusion : public OpFusion {
public:
    MotJitSelectFusion(MemoryContext context, CachedPlanSource *psrc, List *plantree_list, ParamListInfo params);

    ~MotJitSelectFusion() {};

    bool execute(long max_rows, char *completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();
};

class MotJitModifyFusion : public OpFusion {
public:
    MotJitModifyFusion(MemoryContext context, CachedPlanSource *psrc, List *plantree_list, ParamListInfo params);

    ~MotJitModifyFusion() {};

    bool execute(long max_rows, char *completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();
private:
    struct MotJitModifyFusionLocaleVariable {
        CmdType m_cmdType;
        EState* m_estate;
    };
    MotJitModifyFusionLocaleVariable m_c_local;
};
#endif /* ENABLE_MOT */

#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_MOT_H_ */