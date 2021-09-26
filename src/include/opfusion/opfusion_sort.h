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
 * opfusion_sort.h
 *        Declaration of sort operator for bypass executor.
 *
 * IDENTIFICATION
 *        src/include/opfusion/opfusion_sort.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_SORT_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_SORT_H_

#include "opfusion/opfusion.h"

class SortFusion: public OpFusion {

public:

    SortFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    ~SortFusion(){};

    bool execute(long max_rows, char *completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();

protected:

    struct SortFusionLocaleVariable {
        TupleDesc  m_scanDesc;
    };

    SortFusionLocaleVariable m_c_local;
};

#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_SORT_H_ */