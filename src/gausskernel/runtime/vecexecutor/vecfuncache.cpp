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
 *  vecfuncache.cpp
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecfuncache.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "utils/biginteger.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_proc.h"
#include "vecexecutor/vecfunc.h"
#include "utils/date.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "vecexecutor/vecwindowagg.h"

extern bool is_searchserver_api_load();
extern void add_simsearch_vfunc_hash();
void InitVecsubarray(void);

static VecFuncCacheEntry vec_func_table[] = {{1724,
                                                 {
                                                     vnumeric_op<numeric_add, BIADD>,
                                                 }},
    {2114, /* sum(numeric) */
        {},
        {vnumeric_sum, vnumeric_sum},
        {vsnumeric_sum, vsnumeric_sum}},
    {1726,
        {
            vnumeric_op<numeric_mul, BIMUL>,
        }},
    {1725,
        {
            vnumeric_op<numeric_sub, BISUB>,
        }},
    {1727,
        {
            vnumeric_op<numeric_div, BIDIV>,
        }},
    {1721,
        {
            vnumeric_op<numeric_ge, BIGE>,
        }},
    {1720,
        {
            vnumeric_op<numeric_gt, BIGT>,
        }},
    {1723,
        {
            vnumeric_op<numeric_le, BILE>,
        }},
    {1722,
        {
            vnumeric_op<numeric_lt, BILT>,
        }},
    {1718,
        {
            vnumeric_op<numeric_eq, BIEQ>,
        }},
    {1719,
        {
            vnumeric_op<numeric_ne, BINEQ>,
        }},
    {111,
        {
            vnumeric_fac,
        }},
    {2103, /* avg(numeric) */
        {

        },
        {vnumeric_avg<true>, vnumeric_avg<false>},
        {vsnumeric_avg<true>, vsnumeric_avg<false>},
        {vnumeric_avg_final<true, true>,
            vnumeric_avg_final<true, false>,
            vnumeric_avg_final<false, true>,
            vnumeric_avg_final<false, false>},
        {vsnumeric_avg_final<true, true>,
            vsnumeric_avg_final<true, false>,
            vsnumeric_avg_final<false, true>,
            vsnumeric_avg_final<false, false>}},
    {2146, /* min(numeric) */
        {

        },
        {
            vnumeric_min_max<numeric_smaller, true>,
            vnumeric_min_max<numeric_smaller, true>,
        },
        {
            vsnumeric_min_max<numeric_smaller, true>,
            vsnumeric_min_max<numeric_smaller, true>,
        }},
    {2130, /* max(numeric) */
        {

        },
        {
            vnumeric_min_max<numeric_larger, false>,
            vnumeric_min_max<numeric_larger, false>,
        },
        {
            vsnumeric_min_max<numeric_larger, false>,
            vsnumeric_min_max<numeric_larger, false>,
        }},
    {2138, /* min(date) */
        {

        },
        {
            vint_min_max<DateADT, SOP_LT>,
            vint_min_max<DateADT, SOP_LT>,
        }},
    {2122, /* max(date) */
        {

        },
        {
            vint_min_max<DateADT, SOP_GT>,
            vint_min_max<DateADT, SOP_GT>,
        }},
    {2021,
        {
            vtimestamp_part,
        }},
    {2052,
        {
            vint_sop<SOP_EQ, Timestamp>,
        }},
    {2053,
        {
            vint_sop<SOP_NEQ, Timestamp>,
        }},
    {2054,
        {
            vint_sop<SOP_LT, Timestamp>,
        }},
    {2055,
        {
            vint_sop<SOP_LE, Timestamp>,
        }},
    {2057,
        {
            vint_sop<SOP_GT, Timestamp>,
        }},
    {2056,
        {
            vint_sop<SOP_GE, Timestamp>,
        }},
    {2142, /* min(timestamp) */
        {

        },
        {
            vint_min_max<Timestamp, SOP_LT>,
            vint_min_max<Timestamp, SOP_LT>,
        }},
    {2126, /* max(timestamp) */
        {

        },
        {
            vint_min_max<Timestamp, SOP_GT>,
            vint_min_max<Timestamp, SOP_GT>,
        }},
    {9009, /* min(smalldatetime) */
        {

        },
        {
            vint_min_max<Timestamp, SOP_LT>,
            vint_min_max<Timestamp, SOP_LT>,
        }},
    {9010, /* max(smalldatetime) */
        {

        },
        {
            vint_min_max<Timestamp, SOP_GT>,
            vint_min_max<Timestamp, SOP_GT>,
        }},
    {2143, /* min(timestamptz) */
        {

        },
        {
            vint_min_max<Timestamp, SOP_LT>,
            vint_min_max<Timestamp, SOP_LT>,
        }},
    {2127, /* max(timestamptz) */
        {

        },
        {
            vint_min_max<Timestamp, SOP_GT>,
            vint_min_max<Timestamp, SOP_GT>,
        }},
    {2139, /* min(time) */
        {

        },
        {
            vint_min_max<TimeADT, SOP_LT>,
            vint_min_max<TimeADT, SOP_LT>,
        }},
    {2123, /* max(time) */
        {

        },
        {
            vint_min_max<TimeADT, SOP_GT>,
            vint_min_max<TimeADT, SOP_GT>,
        }},
    {2140, /* min(timetz) */
        {

        },
        {
            vtimetz_min_max<timetz_smaller>,
            vtimetz_min_max<timetz_smaller>,
        }},
    {2124, /* max(timetz) */
        {

        },
        {
            vtimetz_min_max<timetz_larger>,
            vtimetz_min_max<timetz_larger>,
        }},
    {408,
        {
            vbpchar<name_bpchar>,
        }},
    {668,
        {
            vbpchar<bpchar>,
        }},
    {860,
        {
            vbpchar<char_bpchar>,
        }},
    {3192,
        {
            vbpchar<int4_bpchar>,
        }},
    {1048,
        {
            vbpchar_sop<SOP_EQ>,
        }},
    {1053,
        {
            vbpchar_sop<SOP_NEQ>,
        }},
    {1049,
        {
            vbpchar_sop<SOP_LT>,
        }},
    {1050,
        {
            vbpchar_sop<SOP_LE>,
        }},
    {1051,
        {
            vbpchar_sop<SOP_GT>,

        }},
    {1052,
        {
            vbpchar_sop<SOP_GE>,

        }},
    {2244, /* max(bpchar) */
        {

        },
        {
            vbpchar_min_max<bpcharcmp, 1>,
            vbpchar_min_max<bpcharcmp, 1>,
        }},
    {2245, /* min(bpchar) */
        {

        },
        {
            vbpchar_min_max<bpcharcmp, -1>,
            vbpchar_min_max<bpcharcmp, -1>,
        }},
    {1631,
        {
            vtextlike,  // bpcharlike
        }},
    {1632,
        {
            vtextnlike,  // bpcharnlike
        }},
    {1569,
        {
            vtextlike,  // like
        }},
    {1570,
        {
            vtextnlike,  // notlike
        }},
    {850,
        {
            vtextlike,  // textlike

        }},
    {851,
        {
            vtextnlike,  // textnlike
        }},
    {67,
        {
            vtext_sop<SOP_EQ>,
        }},
    {157,
        {
            vtextne,  // textne
        }},
    {741,
        {
            vtext_sop<SOP_LE>,

        }},
    {740,
        {
            vtext_sop<SOP_LT>,
        }},
    {743,
        {
            vtext_sop<SOP_GE>,

        }},
    {742,
        {
            vtext_sop<SOP_GT>,

        }},
    {2129, /* max(text) */
        {

        },
        {
            vbpchar_min_max<bttextcmp, 1>,
            vbpchar_min_max<bttextcmp, 1>,
        }},
    {2145, /* min(text) */
        {

        },
        {
            vbpchar_min_max<bttextcmp, -1>,
            vbpchar_min_max<bttextcmp, -1>,
        }},
    {141,
        {
            vint4mul,
        }},
    {177,
        {
            vint4pl,
        }},
    {181,
        {
            vint4mi,

        }},
    {65,
        {
            vint_sop<SOP_EQ, int32>,

        }},
    {144,
        {
            vint_sop<SOP_NEQ, int32>,

        }},
    {150,
        {
            vint_sop<SOP_GE, int32>,

        }},
    {147,
        {
            vint_sop<SOP_GT, int32>,

        }},
    {149,
        {
            vint_sop<SOP_LE, int32>,

        }},
    {66,
        {
            vint_sop<SOP_LT, int32>,

        }},
    {2101, /* avg(int4) */
        {

        },
        {vint_avg<4, true>, vint_avg<4, false>},
        {vsint_avg<4, true>, vsint_avg<4, false>},
        {vint_avg_final<true>, vint_avg_final<false>, vint_avg_final<true>, vint_avg_final<false>},
        {vsint_avg_final<true>, vsint_avg_final<false>, vsint_avg_final<true>, vsint_avg_final<false>}},
    {2102, /* avg(int2) */
        {

        },
        {vint_avg<2, true>, vint_avg<2, false>},
        {vsint_avg<2, true>, vsint_avg<2, false>},
        {vint_avg_final<true>, vint_avg_final<false>, vint_avg_final<true>, vint_avg_final<false>},
        {vsint_avg_final<true>, vsint_avg_final<false>, vsint_avg_final<true>, vsint_avg_final<false>}},
    {5537, /* avg(int1) */
        {

        },
        {vint_avg<1, true>, vint_avg<1, false>},
        {vsint_avg<1, true>, vsint_avg<1, false>},
        {vint_avg_final<true>, vint_avg_final<false>, vint_avg_final<true>, vint_avg_final<false>},
        {vsint_avg_final<true>, vsint_avg_final<false>, vsint_avg_final<true>, vsint_avg_final<false>}},
    {465,
        {
            vint8mul<int64, int64>,

        }},
    {1280,
        {
            vint8mul<int32, int64>,

        }},
    {1276,
        {
            vint8mul<int64, int32>,

        }},
    {463,
        {
            vint8pl<int64, int64>,

        }},
    {1278,
        {
            vint8pl<int32, int64>,

        }},
    {1274,
        {
            vint8pl<int64, int32>,

        }},
    {464,
        {
            vint8mi<int64, int64>,

        }},
    {1275,
        {
            vint8mi<int64, int32>,

        }},
    {1279,
        {
            vint8mi<int32, int64>,

        }},
    {2107, /* sum(int8) */
        {

        },
        {
            vint8_sum<true>,
            vint8_sum<false>,
        },
        {
            vsint8_sum<true>,
            vsint8_sum<false>,
        }},
    {2131, /* min(int8) */
        {

        },
        {
            vint_min_max<int64, SOP_LT>,
            vint_min_max<int64, SOP_LT>,
        },
        {
            vsint_min_max<int64, SOP_LT>,
            vsint_min_max<int64, SOP_LT>,
        }},
    {2147, /* count(expr) */
        {

        },
        {
            vector_count<true, false>,
            vector_count<false, false>,
        },
        {vsonic_count<true, false>, vsonic_count<false, false>}},
    {2803, /* count(*) */
        {

        },
        {
            vector_count<true, true>,
            vector_count<false, true>,
        },
        {vsonic_count<true, true>, vsonic_count<false, true>}},
    {2115, /* max(int8) */
        {

        },
        {
            vint_min_max<int64, SOP_GT>,
            vint_min_max<int64, SOP_GT>,
        },
        {
            vsint_min_max<int64, SOP_GT>,
            vsint_min_max<int64, SOP_GT>,
        }},
    {2100, /* avg(int8) */
        {

        },
        {vint8_avg<true>, vint8_avg<false>},
        {vsint8_avg<true>, vsint8_avg<false>},
        {vnumeric_avg_final<true, true>,
            vnumeric_avg_final<true, false>,
            vnumeric_avg_final<false, true>,
            vnumeric_avg_final<false, false>},
        {vsnumeric_avg_final<true, true>,
            vsnumeric_avg_final<true, false>,
            vsnumeric_avg_final<false, true>,
            vsnumeric_avg_final<false, false>}},
    {2108, /* sum(int4) */
        {

        },
        {vint_sum<true, true>, vint_sum<true, false>},
        {
            vsint_sum<true, true>,
            vsint_sum<true, false>,
        }},
    {2109, /* sum(int2) */
        {

        },
        {
            vint_sum<false, true>,
            vint_sum<false, false>,
        },
        {
            vsint_sum<false, true>,
            vsint_sum<false, false>,
        }},
    {2132, /* min(int4) */
        {

        },
        {
            vint_min_max<int32, SOP_LT>,
            vint_min_max<int32, SOP_LT>,
        },
        {
            vsint_min_max<int32, SOP_LT>,
            vsint_min_max<int32, SOP_LT>,
        }},
    {2116, /* max(int4) */
        {

        },
        {
            vint_min_max<int32, SOP_GT>,
            vint_min_max<int32, SOP_GT>,
        },
        {
            vsint_min_max<int32, SOP_GT>,
            vsint_min_max<int32, SOP_GT>,
        }},
    {2133, /* min(int2) */
        {

        },
        {
            vint_min_max<int16, SOP_LT>,
            vint_min_max<int16, SOP_LT>,
        },
        {
            vsint_min_max<int16, SOP_LT>,
            vsint_min_max<int16, SOP_LT>,
        }},
    {2117, /* max(int2) */
        {

        },
        {
            vint_min_max<int16, SOP_GT>,
            vint_min_max<int16, SOP_GT>,
        },
        {
            vsint_min_max<int16, SOP_GT>,
            vsint_min_max<int16, SOP_GT>,
        }},

    {5538, /* max(int1) */
        {

        },
        {
            vint_min_max<uint8, SOP_GT>,
            vint_min_max<uint8, SOP_GT>,
        }},
    {2134, /* min(oid) */
        {

        },
        {
            vint_min_max<uint32, SOP_LT>,
            vint_min_max<uint32, SOP_LT>,
        }},
    {2118, /* max(oid) */
        {

        },
        {
            vint_min_max<uint32, SOP_GT>,
            vint_min_max<uint32, SOP_GT>,
        }},
    {1704, {vnumeric_abs}},
    {1705, {vnumeric_abs}},
    {1396, {vector_abs<int8abs>}},
    {1397, {vector_abs<int4abs>}},
    {467,
        {
            vint8_sop<SOP_EQ, int64, int64>,

        }},
    {474,
        {
            vint8_sop<SOP_EQ, int64, int32>,

        }},
    {852,
        {
            vint8_sop<SOP_EQ, int32, int64>,

        }},
    {468,
        {
            vint8_sop<SOP_NEQ, int64, int64>,

        }},
    {853,
        {
            vint8_sop<SOP_NEQ, int32, int64>,

        }},
    {475,
        {
            vint8_sop<SOP_NEQ, int64, int32>,

        }},
    {472,
        {
            vint8_sop<SOP_GE, int64, int64>,

        }},
    {479,
        {
            vint8_sop<SOP_GE, int64, int32>,

        }},
    {857,
        {
            vint8_sop<SOP_GE, int32, int64>,

        }},
    {470,
        {
            vint8_sop<SOP_GT, int64, int64>,

        }},
    {855,
        {
            vint8_sop<SOP_GT, int32, int64>,

        }},
    {477,
        {
            vint8_sop<SOP_GT, int64, int32>,

        }},
    {471,
        {
            vint8_sop<SOP_LE, int64, int64>,

        }},
    {478,
        {
            vint8_sop<SOP_LE, int64, int32>,

        }},
    {856,
        {
            vint8_sop<SOP_LE, int32, int64>,

        }},
    {469,
        {
            vint8_sop<SOP_LT, int64, int64>,

        }},
    {854,
        {
            vint8_sop<SOP_LT, int32, int64>,

        }},
    {476,
        {
            vint8_sop<SOP_LT, int64, int32>,

        }},
    {287,
        {
            vfloat4_sop<float4eq>,

        }},
    {288,
        {
            vfloat4_sop<float4ne>,

        }},
    {292,
        {
            vfloat4_sop<float4ge>,
        }},
    {291,
        {
            vfloat4_sop<float4gt>,

        }},
    {290,
        {
            vfloat4_sop<float4le>,

        }},
    {289,
        {
            vfloat4_sop<float4lt>,

        }},
    {293,
        {
            vfloat4_sop<float8eq>,

        }},
    {294,
        {
            vfloat4_sop<float8ne>,

        }},
    {298,
        {
            vfloat4_sop<float8ge>,

        }},
    {297,
        {
            vfloat4_sop<float8gt>,

        }},
    {296,
        {
            vfloat4_sop<float8le>,

        }},
    {295,
        {
            vfloat4_sop<float8lt>,

        }},
    {204,
        {
            vfloat4_mop<float4pl>,
        }},
    {2110, /* sum(float4) */
        {},
        {vfloat_sum<float4pl>, vfloat_sum<float4pl>}},
    {218,
        {
            vfloat8_mop<float8pl>,
        }},
    {2111, /* sum(float8) */
        {

        },
        {vfloat_sum<float8pl>, vfloat_sum<float8pl>}},
    {2105, /* avg(float8) */
        {

        },
        {vfloat8_avg<true, false>, vfloat8_avg<false, false>},
        {

        },
        {vfloat_avg_final<true, true, false>,
            vfloat_avg_final<true, false, false>,
            vfloat_avg_final<false, true, false>,
            vfloat_avg_final<false, false, false>}},
    {2104, /* avg(float4) */
        {

        },
        {vfloat4_avg<true, false>, vfloat4_avg<false, false>},
        {

        },
        {vfloat_avg_final<true, true, false>,
            vfloat_avg_final<true, false, false>,
            vfloat_avg_final<false, true, false>,
            vfloat_avg_final<false, false, false>}},
    {2136, /* min(float8) */
        {

        },
        {
            vfloat_min_max<float8smaller>,
            vfloat_min_max<float8smaller>,
        }},
    {2135, /* min(float4) */
        {

        },
        {
            vfloat_min_max<float4smaller>,
            vfloat_min_max<float4smaller>,
        }},
    {2120, /* max(float8) */
        {

        },
        {
            vfloat_min_max<float8larger>,
            vfloat_min_max<float8larger>,
        }},
    {2119, /* max(float4) */
        {

        },
        {
            vfloat_min_max<float4larger>,
            vfloat_min_max<float4larger>,
        }},
    {401,
        {
            vtrim1<rtrim1>,  // text
        }},
    {881,
        {
            vtrim1<ltrim1>,  // ltrim1
        }},
    {882,
        {
            vtrim1<rtrim1>,  // rtrim1
        }},
    {885,
        {
            vtrim1<btrim1>,  // btrim1
        }},
    {936,
        {
            vtext_substr<false, true>,  // text_substr_null
        }},
    {937,
        {
            vtext_substr<false, false>,  // text_substr_no_len_null
        }},
    {877,
        {
            vtext_substr<true, true>,  // text_substr_orclcompat
        }},
    {883,
        {
            vtext_substr<true, false>,  // text_substr_no_len_orclcompat
        }},
    {870, {vlower}},
    {871, {vupper}},
    {873, {vlpad}},
    {2106, /* avg(interval) */
        {

        },
        {vinterval_avg<true>, vinterval_avg<false>},
        {

        },
        {vinterval_avg_final<true>, vinterval_avg_final<false>, vinterval_avg_final<true>, vinterval_avg_final<false>}},
    {1169,
        {
            vintervalpl,
        }},
    {2113, /* sum(interval) */
        {

        },
        {
            vinterval_sum,
            vinterval_sum,
        }},
    {2128, /* max(interval) */
        {

        },
        {
            vtimetz_min_max<interval_larger>,
            vtimetz_min_max<interval_larger>,
        }},
    {2144, /* min(interval) */
        {

        },
        {
            vtimetz_min_max<interval_smaller>,
            vtimetz_min_max<interval_smaller>,
        }},

    {894,
        {
            vint8pl<int64, int64>  // cashpl
        }},
    {2112, /* sum(cash) */
        {

        },
        {
            vcash_sum,
            vcash_sum,
        }},
    {2141, /* min(cash) */
        {

        },
        {
            vint_min_max<int64, SOP_LT>,
            vint_min_max<int64, SOP_LT>,
        }},
    {2125, /* max(cash) */
        {

        },
        {
            vint_min_max<int64, SOP_GT>,
            vint_min_max<int64, SOP_GT>,
        }},

    // window function
    {3100,
        {
            vwindow_row_number,
            vwindowfunc_withsort<WINDOW_ROWNUMBER>,
        }},
    {3101,
        {
            vwindow_rank,
            vwindowfunc_withsort<WINDOW_RANK>,
        }},
    {3102,
        {
            vwindow_denserank,
            vwindowfunc_withsort<WINDOW_RANK>,
        }},

    /* unsupportted vector function */
    {1292,
        {
            vctid_sop<tideq>,
        }},  // tideq
    {1265,
        {
            vctid_sop<tidne>,
        }},  // tidne
    {2791,
        {
            vctid_sop<tidlt>,
        }},  // tidlt
    {2790,
        {
            vctid_sop<tidgt>,
        }},  // tidgt
    {2793,
        {
            vctid_sop<tidle>,
        }},  // tidle
    {2792,
        {
            vctid_sop<tidge>,
        }},  // tidge
    {1318,
        {
            vbpcharlen,
        }},
    {4046,
        {
            vgetbucket,
        }},
    {4048,
        {
            vtable_data_skewness,
        }},
    /* replace int1_numeric to int1_numeric_bi */
    {5521,
        {

        },
        {

        },
        {

        },
        {int1_numeric_bi}},
    /* replace int2_numeric to int2_numeric_bi */
    {1782,
        {

        },
        {

        },
        {

        },
        {int2_numeric_bi}},
    /* replace int4_numeric to int4_numeric_bi */
    {1740,
        {

        },
        {

        },
        {

        },
        {int4_numeric_bi}},
    /* replace int8_numeric to int8_numeric_bi */
    {1781,
        {

        },
        {

        },
        {

        },
        {int8_numeric_bi}},
    {2712, /* stddev_samp(int8) */
        {

        },
        {vint_stddev_samp<8, true>, vint_stddev_samp<8, false>},
        {

        },
        {vnumeric_stddev_samp_final<true, true>,
            vnumeric_stddev_samp_final<true, false>,
            vnumeric_stddev_samp_final<false, true>,
            vnumeric_stddev_samp_final<false, false>}},
    {2713, /* stddev_samp(int4) */
        {

        },
        {vint_stddev_samp<4, true>, vint_stddev_samp<4, false>},
        {

        },
        {vnumeric_stddev_samp_final<true, true>,
            vnumeric_stddev_samp_final<true, false>,
            vnumeric_stddev_samp_final<false, true>,
            vnumeric_stddev_samp_final<false, false>}},
    {2714, /* stddev_samp(int2) */
        {

        },
        {vint_stddev_samp<2, true>, vint_stddev_samp<2, false>},
        {

        },
        {vnumeric_stddev_samp_final<true, true>,
            vnumeric_stddev_samp_final<true, false>,
            vnumeric_stddev_samp_final<false, true>,
            vnumeric_stddev_samp_final<false, false>}},
    {2715, /* stddev_samp(float4) */
        {

        },
        {vfloat4_avg<true, true>, vfloat4_avg<false, true>},
        {

        },
        {vfloat_avg_final<true, true, true>,
            vfloat_avg_final<true, false, true>,
            vfloat_avg_final<false, true, true>,
            vfloat_avg_final<false, false, true>}},
    {2716, /* stddev_samp(float8) */
        {

        },
        {vfloat8_avg<true, true>, vfloat8_avg<false, true>},
        {

        },
        {vfloat_avg_final<true, true, true>,
            vfloat_avg_final<true, false, true>,
            vfloat_avg_final<false, true, true>,
            vfloat_avg_final<false, false, true>}},
    {2717, /* stddev_samp(numeric) */
        {

        },
        {vint_stddev_samp<-1, true>, vint_stddev_samp<-1, false>},
        {

        },
        {vnumeric_stddev_samp_final<true, true>,
            vnumeric_stddev_samp_final<true, false>,
            vnumeric_stddev_samp_final<false, true>,
            vnumeric_stddev_samp_final<false, false>}},
    {2154, /* stddev(int8) */
        {

        },
        {vint_stddev_samp<8, true>, vint_stddev_samp<8, false>},
        {

        },
        {vnumeric_stddev_samp_final<true, true>,
            vnumeric_stddev_samp_final<true, false>,
            vnumeric_stddev_samp_final<false, true>,
            vnumeric_stddev_samp_final<false, false>}},
    {2155, /* stddev(int4) */
        {

        },
        {vint_stddev_samp<4, true>, vint_stddev_samp<4, false>},
        {

        },
        {vnumeric_stddev_samp_final<true, true>,
            vnumeric_stddev_samp_final<true, false>,
            vnumeric_stddev_samp_final<false, true>,
            vnumeric_stddev_samp_final<false, false>}},
    {2156, /* stddev(int2) */
        {

        },
        {vint_stddev_samp<2, true>, vint_stddev_samp<2, false>},
        {

        },
        {vnumeric_stddev_samp_final<true, true>,
            vnumeric_stddev_samp_final<true, false>,
            vnumeric_stddev_samp_final<false, true>,
            vnumeric_stddev_samp_final<false, false>}},
    {2157, /* stddev(float4) */
        {

        },
        {vfloat4_avg<true, true>, vfloat4_avg<false, true>},
        {

        },
        {vfloat_avg_final<true, true, true>,
            vfloat_avg_final<true, false, true>,
            vfloat_avg_final<false, true, true>,
            vfloat_avg_final<false, false, true>}},
    {2158, /* stddev(float8) */
        {

        },
        {vfloat8_avg<true, true>, vfloat8_avg<false, true>},
        {

        },
        {vfloat_avg_final<true, true, true>,
            vfloat_avg_final<true, false, true>,
            vfloat_avg_final<false, true, true>,
            vfloat_avg_final<false, false, true>}},
    {2159, /* stddev(numeric) */
        {

        },
        {vint_stddev_samp<-1, true>, vint_stddev_samp<-1, false>},
        {

        },
        {vnumeric_stddev_samp_final<true, true>,
            vnumeric_stddev_samp_final<true, false>,
            vnumeric_stddev_samp_final<false, true>,
            vnumeric_stddev_samp_final<false, false>}}};

void InitGlobalVecFuncMap()
{
    VecFuncCacheEntry* entry = NULL;
    bool found = false;
    int i;
    int j;
    HASHCTL hash_ctl;

    errno_t rc = 0;

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");

    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(VecFuncCacheEntry);
    hash_ctl.hash = oid_hash;
    hash_ctl.hcxt = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR);
    g_instance.vec_func_hash = hash_create("VecFuncHash", 4096, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    int nvecfunction = (sizeof(vec_func_table) / sizeof(VecFuncCacheEntry));
    for (i = 0; i < nvecfunction; i++) {
        entry =
            (VecFuncCacheEntry*)hash_search(g_instance.vec_func_hash, &vec_func_table[i].fn_oid, HASH_ENTER, &found);

        if (entry != NULL) {
            entry->fn_oid = vec_func_table[i].fn_oid;
            for (j = 0; j < FUNCACHE_NUM; j++) {
                entry->vec_fn_cache[j] = vec_func_table[i].vec_fn_cache[j];
                entry->vec_agg_cache[j] = vec_func_table[i].vec_agg_cache[j];
                entry->vec_sonic_agg_cache[j] = vec_func_table[i].vec_sonic_agg_cache[j];
                entry->vec_transform_function[j] = vec_func_table[i].vec_transform_function[j];
                entry->vec_sonic_transform_function[j] = vec_func_table[i].vec_sonic_transform_function[j];
            }
        }
    }

    InitVecsubarray();
}

/*
 * Initialize vector function by funcid during program startup.
 */
void InitVecFuncMap(void)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (is_searchserver_api_load()) {
        add_simsearch_vfunc_hash();
    }
#endif
}

#define InitSubstrFuncTemplate(eml)                                     \
    do {                                                                \
        substr_Array[i++] = &vec_text_substr<false, false, eml, false>; \
        substr_Array[i++] = &vec_text_substr<false, false, eml, true>;  \
        substr_Array[i++] = &vec_text_substr<false, true, eml, false>;  \
        substr_Array[i++] = &vec_text_substr<false, true, eml, true>;   \
        substr_Array[i++] = &vec_text_substr<true, false, eml, false>;  \
        substr_Array[i++] = &vec_text_substr<true, false, eml, true>;   \
        substr_Array[i++] = &vec_text_substr<true, true, eml, false>;   \
        substr_Array[i++] = &vec_text_substr<true, true, eml, true>;    \
    } while (0);

sub_Array substr_Array[32];

void InitVecsubarray(void)
{
    int i = 0;

    InitSubstrFuncTemplate(1);
    InitSubstrFuncTemplate(2);
    InitSubstrFuncTemplate(3);
    InitSubstrFuncTemplate(4);
};
