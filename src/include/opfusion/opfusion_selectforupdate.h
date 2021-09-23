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
 * opfusion_selectforupdate.h
 *        Declaration of select ... for update statement for bypass executor.
 *
 * IDENTIFICATION
 *        src/include/opfusion/opfusion_selectforupdate.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_SELECTFORUPDATE_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_SELECTFORUPDATE_H_

#include "opfusion/opfusion.h"

class SelectForUpdateFusion : public OpFusion {
public:
    SelectForUpdateFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    ~SelectForUpdateFusion(){};

    bool execute(long max_rows, char* completionTag);

    void close();

private:
    unsigned long ExecSelectForUpdate(Relation rel, ResultRelInfo* resultRelInfo);

    void InitLocals(ParamListInfo params);

    void InitGlobals();
private:
    struct SelectForUpdateFusionGlobalVariable {
        int64 m_limitCount;
        int64 m_limitOffset;
    };
    SelectForUpdateFusionGlobalVariable* m_c_global;

    struct SelectForUpdateFusionLocaleVariable {
        EState* m_estate;
    };
    SelectForUpdateFusionLocaleVariable m_c_local;
};

#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_SELECTFORUPDATE_H_ */