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
 * opfusion_insert.h
 *        Declaration of insert operator for bypass executor.
 *
 * IDENTIFICATION
 *        src/include/opfusion/opfusion_insert.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_INSERT_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_INSERT_H_

#include "opfusion/opfusion.h"

class InsertFusion : public OpFusion {
public:
    InsertFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    ~InsertFusion(){};

    bool execute(long max_rows, char* completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();

    void refreshParameterIfNecessary();

    virtual bool ResetReuseFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

private:

    unsigned long ExecInsert(Relation rel, ResultRelInfo* resultRelInfo);

    void InitBaseParam(List* targetList);

    struct InsertFusionGlobalVariable {
        /* for func/op expr calculation */
        FuncExprInfo* m_targetFuncNodes;
        
        int m_targetFuncNum;
        
        int m_targetParamNum;

        int m_targetConstNum;

        ConstLoc* m_targetConstLoc;
    };
    InsertFusionGlobalVariable* m_c_global;

    struct InsertFusionLocaleVariable {
        EState* m_estate;
        Datum* m_curVarValue;
        bool* m_curVarIsnull;
    };

    InsertFusionLocaleVariable m_c_local;
};

class InsertSubFusion : public OpFusion {
public:
    InsertSubFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    ~InsertSubFusion(){};

    bool execute(long max_rows, char* completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();

    void InitPlan();

private:

    unsigned long ExecInsert(Relation rel, ResultRelInfo* resultRelInfo);

    struct VarLoc {
        int varNo;
        int scanKeyIndx;
    };
    struct InsertSubFusionGlobalVariable {
        int m_targetParamNum;

        int m_targetConstNum;

        ConstLoc* m_targetConstLoc;

        int m_varNum;

        VarLoc* m_targetVarLoc;
    };
    InsertSubFusionGlobalVariable* m_c_global;

    struct InsertSubFusionLocaleVariable {
        EState* m_estate; /* Top estate*/
        Datum* m_curVarValue;
        bool* m_curVarIsnull;
        Plan* m_plan;
        SeqScan* m_ss_plan;
        PlanState* m_ps;
        SeqScanState* m_sub_ps;
        ModifyTableState* m_mt_state;
    };

    InsertSubFusionLocaleVariable m_c_local;
};

#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_INSERT_H_ */