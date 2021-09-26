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
 * opfusion_delete.h
 *        Declaration of delete operator for bypass executor.
 *
 * IDENTIFICATION
 *        src/include/opfusion/opfusion_delete.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_DELETE_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_DELETE_H_

#include "opfusion/opfusion.h"

class DeleteFusion : public OpFusion {
public:
    DeleteFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    ~DeleteFusion(){};

    bool execute(long max_rows, char* completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();
private:
    struct DeleteFusionLocaleVariable {
        EState* m_estate;
    };

    unsigned long ExecDelete(Relation rel, ResultRelInfo* resultRelInfo);

    DeleteFusionLocaleVariable m_c_local;
};

#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_DELETE_H_ */