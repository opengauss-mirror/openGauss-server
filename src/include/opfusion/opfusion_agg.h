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

    typedef void (AggFusion::*aggFun)(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    typedef long (AggFusion::*aggProcessFunc)(long max_rows, Datum* values, bool* isnull);

    /* agg sum function */
    void agg_int2_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    void agg_int4_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    void agg_int8_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    void agg_numeric_sum(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    void agg_any_count(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    void agg_star_count(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    long agg_process_special_count(long max_rows, Datum* values, bool* isnull);

    long agg_process_common(long max_rows, Datum* values, bool* isnull);

    void agg_int2_sum_ext(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    void agg_int4_sum_ext(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    void agg_float4_sum_ext(Datum *transVal, bool transIsNull, Datum *inVal, bool inIsNull);

    bool InitDynamicOidFunc(Aggref *aggref);

    inline void init_var_from_num(Numeric num, NumericVar *dest)
    {
        Assert(!NUMERIC_IS_BI(num));
        uint16 n_header = SHORT_NUMERIC_N_HEADER(num);
        bool is_short = NUMERIC_IS_SHORT(num);
        dest->ndigits = NUMERIC_NDIGITS(num);
        if (is_short) {
            dest->weight = GET_SHORT_NUMERIC_WEIGHT(n_header);
            dest->sign = GET_SHORT_NUMERIC_SIGN(n_header);
            dest->dscale = GET_SHORT_NUMERIC_DSCALE(n_header);
            dest->digits = GET_SHORT_NUMERIC_DIGITS(num);
        } else {
            dest->weight = GET_LONG_NUMERIC_WEIGHT(num);
            dest->sign = GET_LONG_NUMERIC_SIGN(num);
            dest->dscale = GET_LONG_NUMERIC_DSCALE(num);
            dest->digits = GET_LONG_NUMERIC_DIGITS(num);
        }
        dest->buf = NULL;       /* digits array is not palloc'd */
    }

    struct AggFusionGlobalVariable {
        aggFun m_aggFunc;
        aggProcessFunc m_aggProcessFunc;
        Oid m_aggFnOid;
        bool m_aggCountNull;
    };
    AggFusionGlobalVariable* m_c_global;
};

#endif /* SRC_INCLUDE_OPFUSION_OPFUSION_AGG_H_ */
