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
 * opfusion_unique_sort.h
 *        Declaration of sort operator for bypass executor.
 *
 * IDENTIFICATION
 *        src/include/opfusion/opfusion_unique_sort.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_UNIQUE_SORT_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_UNIQUE_SORT_H_

#include "opfusion/opfusion.h"


class UniqueSortFusion : public OpFusion {
public:

    UniqueSortFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    ~UniqueSortFusion(){};

    bool execute(long max_rows, char *completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();

protected:
    struct UniqueSortFusionLocaleVariable {
        TupleDesc  m_scanDesc;
    };
    UniqueSortFusionLocaleVariable m_c_local;
    FmgrInfo* m_eqfunctions;
    Unique* m_unique_plan_node;
};

#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_UNIQUE_SORT_H_ */