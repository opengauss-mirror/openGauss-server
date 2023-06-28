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
 * opfusion_update.h
 *        Declaration of update operator for bypass executor.
 *
 * IDENTIFICATION
 *        src/include/opfusion/opfusion_update.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_UPDATE_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_UPDATE_H_

#include "opfusion/opfusion.h"

class UpdateFusion : public OpFusion {
public:
    UpdateFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    ~UpdateFusion(){};

    bool execute(long max_rows, char* completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();

    void refreshTargetParameterIfNecessary();

    void InitBaseParam(List* targetList);

    bool ResetReuseFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

private:
    HeapTuple heapModifyTuple(HeapTuple tuple);

    UHeapTuple uheapModifyTuple(UHeapTuple tuple, Relation rel);

    unsigned long ExecUpdate(Relation rel, ResultRelInfo* resultRelInfo);

    struct VarLoc {
        int varNo;
        int scanKeyIndx;
    };
    struct UpdateFusionGlobalVariable {
        /* targetlist */
        int m_targetConstNum;
        
        int m_targetParamNum;
        
        VarLoc* m_targetVarLoc;
        
        int m_varNum;
        
        ConstLoc* m_targetConstLoc;
        
        ParamLoc* m_targetParamLoc;
        
        /* for func/op expr calculation */
        FuncExprInfo* m_targetFuncNodes;
        
        int m_targetFuncNum;
    };
    UpdateFusionGlobalVariable* m_c_global;

    struct UpdateFusionLocaleVariable {
        EState* m_estate;
        Datum* m_curVarValue;
        bool* m_curVarIsnull;
    };

    UpdateFusionLocaleVariable m_c_local;
};

#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_UPDATE_H_ */