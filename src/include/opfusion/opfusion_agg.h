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
 * opfusion_agg.h
 *        Declaration of aggregate operator for bypass executor.
 *
 * IDENTIFICATION
 *        src/include/opfusion/opfusion_agg.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_OPFUSION_OPFUSION_AGG_H_
#define SRC_INCLUDE_OPFUSION_OPFUSION_AGG_H_

#include "opfusion/opfusion.h"

class AggFusion : public OpFusion {

public:
    AggFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params);

    ~AggFusion(){};

    bool execute(long max_rows, char* completionTag);

    void InitLocals(ParamListInfo params);

    void InitGlobals();

protected:

    typedef void (AggFusion::*aggSumFun)(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    /* agg sum function */
    void agg_int2_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    void agg_int4_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    void agg_int8_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    void agg_numeric_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    inline void init_var_from_num(Numeric num, NumericVar *dest)
    {
        Assert(!NUMERIC_IS_BI(num));
        dest->ndigits = NUMERIC_NDIGITS(num);
        dest->weight = NUMERIC_WEIGHT(num);
        dest->sign = NUMERIC_SIGN(num);
        dest->dscale = NUMERIC_DSCALE(num);
        dest->digits = NUMERIC_DIGITS(num);
        dest->buf = NULL;       /* digits array is not palloc'd */
    }

    struct AggFusionGlobalVariable {
        aggSumFun m_aggSumFunc;
    };
    AggFusionGlobalVariable* m_c_global;
};

#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_AGG_H_ */