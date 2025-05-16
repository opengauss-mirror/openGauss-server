/*
*   Copyright (c) 2020 Huawei Technologies Co.,Ltd.
*   openGauss is licensed under Mulan PSL v2.
*   You can use this software according to the terms and conditions of the Mulan PSL v2.
*   You may obtain a copy of Mulan PSL v2 at:
*        http://license.coscl.org.cn/MulanPSL2
*   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
*   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
*   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
*   See the Mulan PSL v2 for more details.
*   opfusion_select.h
*      Declaration of select operator for bypass executor.
*   IDENTIFICATION
*      src/include/opfusion/opfusion_select.h
*/
#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_SELECT_FOR_ANN_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_SELECT_FOR_ANN_H_

#include "opfusion/opfusion.h"

class SelectForAnnFusion : public OpFusion {
public:

    SelectForAnnFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);
    ~SelectForAnnFusion(){};
    bool execute(long max_rows, char* completionTag);
    void close();
    void InitLocals(ParamListInfo params);
    void InitGlobals();
    void InitLimit(ParamListInfo params);
private:

    struct SelectForAnnFusionGlobalVariable {
        int64 m_limitCount;
        int64 m_limitOffset;
    };
    SelectForAnnFusionGlobalVariable* m_c_global;
    bool reInitLimit;
    Node* nodeLimitCount;
    Node* nodeLimitOffset;
};
#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_SELECT_H_ */