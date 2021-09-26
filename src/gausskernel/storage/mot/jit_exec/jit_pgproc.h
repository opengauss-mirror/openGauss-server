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
 * -------------------------------------------------------------------------
 *
 * jit_pgproc.h
 *    JIT PGPROC Operators.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_pgproc.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_PGPROC_H
#define JIT_PGPROC_H

#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/int8.h"

/*---------------------------  PGPROC Operator ---------------------------*/
// all definitions taken from the following files:
// src/backend/utils/adt/bool.cpp
// src/backend/utils/adt/char.cpp
// src/backend/utils/adt/int.cpp
// src/backend/utils/adt/int8.cpp
// src/backend/utils/adt/date.cpp
// src/backend/utils/adt/varchar.cpp
// src/backend/utils/adt/varlena.cpp
// src/backend/utils/adt/float.cpp
// src/backend/utils/adt/numeric.cpp
/**
 * @define Utility macro for making definitions that apply on all supported operators.
 * @detail The macro should be used as follows:
 * @code{.cpp}
 * #define APPLY_UNARY_OPERATOR(funcid, name) ...;
 * #define APPLY_BINARY_OPERATOR(funcid, name) ...;
 * #define APPLY_TERNARY_OPERATOR(funcid, name) ...;
 * #define APPLY_UNARY_CAST_OPERATOR(funcid, name) ...;
 * #define APPLY_BINARY_CAST_OPERATOR(funcid, name) ...;
 * #define APPLY_TERNARY_CAST_OPERATOR(funcid, name) ...;
 *
 * APPLY_OPERATORS()
 *
 * #undef APPLY_UNARY_OPERATOR
 * #undef APPLY_BINARY_OPERATOR
 * #undef APPLY_TERNARY_OPERATOR
 * #undef APPLY_UNARY_CAST_OPERATOR
 * #undef APPLY_BINARY_CAST_OPERATOR
 * #undef APPLY_TERNARY_CAST_OPERATOR
 * @endcode
 */
#define APPLY_OPERATORS()                                  \
    APPLY_UNARY_OPERATOR(CHARTOBPCHARFUNCOID, char_bpchar) \
    APPLY_BINARY_OPERATOR(60, booleq)                      \
    APPLY_BINARY_OPERATOR(61, chareq)                      \
    APPLY_UNARY_CAST_OPERATOR(77, chartoi4)                \
    APPLY_UNARY_CAST_OPERATOR(78, i4tochar)                \
    APPLY_UNARY_CAST_OPERATOR(313, i2toi4)                 \
    APPLY_UNARY_CAST_OPERATOR(314, i4toi2)                 \
    APPLY_UNARY_CAST_OPERATOR(2557, int4_bool)             \
    APPLY_UNARY_CAST_OPERATOR(2558, bool_int4)             \
    APPLY_UNARY_CAST_OPERATOR(3180, int2_bool)             \
    APPLY_UNARY_CAST_OPERATOR(3181, bool_int2)             \
    APPLY_BINARY_OPERATOR(65, int4eq)                      \
    APPLY_BINARY_OPERATOR(63, int2eq)                      \
    APPLY_BINARY_OPERATOR(158, int24eq)                    \
    APPLY_BINARY_OPERATOR(159, int42eq)                    \
    APPLY_UNARY_OPERATOR(212, int4um)                      \
    APPLY_UNARY_OPERATOR(1912, int4up)                     \
    APPLY_BINARY_OPERATOR(177, int4pl)                     \
    APPLY_BINARY_OPERATOR(181, int4mi)                     \
    APPLY_BINARY_OPERATOR(141, int4mul)                    \
    APPLY_BINARY_OPERATOR(154, int4div)                    \
    APPLY_UNARY_OPERATOR(766, int4inc)                     \
    APPLY_UNARY_OPERATOR(213, int2um)                      \
    APPLY_UNARY_OPERATOR(1911, int2up)                     \
    APPLY_BINARY_OPERATOR(176, int2pl)                     \
    APPLY_BINARY_OPERATOR(180, int2mi)                     \
    APPLY_BINARY_OPERATOR(152, int2mul)                    \
    APPLY_BINARY_OPERATOR(153, int2div)                    \
    APPLY_BINARY_OPERATOR(178, int24pl)                    \
    APPLY_BINARY_OPERATOR(182, int24mi)                    \
    APPLY_BINARY_OPERATOR(170, int24mul)                   \
    APPLY_BINARY_OPERATOR(172, int24div)                   \
    APPLY_BINARY_OPERATOR(179, int42pl)                    \
    APPLY_BINARY_OPERATOR(183, int42mi)                    \
    APPLY_BINARY_OPERATOR(171, int42mul)                   \
    APPLY_BINARY_OPERATOR(173, int42div)                   \
    APPLY_BINARY_OPERATOR(156, int4mod)                    \
    APPLY_BINARY_OPERATOR(155, int2mod)                    \
    APPLY_UNARY_OPERATOR(1251, int4abs)                    \
    APPLY_UNARY_OPERATOR(1253, int2abs)                    \
    APPLY_BINARY_OPERATOR(1898, int4and)                   \
    APPLY_BINARY_OPERATOR(1899, int4or)                    \
    APPLY_BINARY_OPERATOR(1900, int4xor)                   \
    APPLY_BINARY_OPERATOR(1902, int4shl)                   \
    APPLY_BINARY_OPERATOR(1903, int4shr)                   \
    APPLY_UNARY_OPERATOR(1901, int4not)                    \
    APPLY_BINARY_OPERATOR(1892, int2and)                   \
    APPLY_BINARY_OPERATOR(1893, int2or)                    \
    APPLY_BINARY_OPERATOR(1894, int2xor)                   \
    APPLY_BINARY_OPERATOR(1896, int2shl)                   \
    APPLY_BINARY_OPERATOR(1897, int2shr)                   \
    APPLY_UNARY_OPERATOR(1895, int2not)                    \
    APPLY_BINARY_OPERATOR(6101, int1and)                   \
    APPLY_BINARY_OPERATOR(6102, int1or)                    \
    APPLY_BINARY_OPERATOR(6103, int1xor)                   \
    APPLY_BINARY_OPERATOR(6105, int1shl)                   \
    APPLY_BINARY_OPERATOR(6106, int1shr)                   \
    APPLY_UNARY_OPERATOR(6104, int1not)                    \
    APPLY_BINARY_OPERATOR(5547, int1eq)                    \
    APPLY_UNARY_CAST_OPERATOR(5523, i1toi2)                \
    APPLY_UNARY_CAST_OPERATOR(5524, i2toi1)                \
    APPLY_UNARY_CAST_OPERATOR(5525, i1toi4)                \
    APPLY_UNARY_CAST_OPERATOR(5526, i4toi1)                \
    APPLY_UNARY_CAST_OPERATOR(5527, i1toi8)                \
    APPLY_UNARY_CAST_OPERATOR(5528, i8toi1)                \
    APPLY_UNARY_CAST_OPERATOR(5529, i1tof4)                \
    APPLY_UNARY_CAST_OPERATOR(5531, i1tof8)                \
    APPLY_UNARY_CAST_OPERATOR(5530, f4toi1)                \
    APPLY_UNARY_CAST_OPERATOR(5532, f8toi1)                \
    APPLY_UNARY_CAST_OPERATOR(5533, int1_bool)             \
    APPLY_UNARY_CAST_OPERATOR(5534, bool_int1)             \
    APPLY_UNARY_CAST_OPERATOR(3189, int1_interval)         \
    APPLY_UNARY_CAST_OPERATOR(3190, int2_interval)         \
    APPLY_UNARY_CAST_OPERATOR(3191, int4_interval)         \
    APPLY_UNARY_OPERATOR(6108, int1um)                     \
    APPLY_UNARY_OPERATOR(6107, int1up)                     \
    APPLY_BINARY_OPERATOR(6109, int1pl)                    \
    APPLY_BINARY_OPERATOR(6110, int1mi)                    \
    APPLY_BINARY_OPERATOR(6111, int1mul)                   \
    APPLY_BINARY_OPERATOR(6112, int1div)                   \
    APPLY_UNARY_OPERATOR(6113, int1abs)                    \
    APPLY_BINARY_OPERATOR(6114, int1mod)                   \
    APPLY_UNARY_OPERATOR(6117, int1inc)                    \
    APPLY_BINARY_OPERATOR(467, int8eq)                     \
    APPLY_BINARY_OPERATOR(474, int84eq)                    \
    APPLY_BINARY_OPERATOR(852, int48eq)                    \
    APPLY_BINARY_OPERATOR(1856, int82eq)                   \
    APPLY_BINARY_OPERATOR(1850, int28eq)                   \
    APPLY_UNARY_OPERATOR(462, int8um)                      \
    APPLY_UNARY_OPERATOR(1910, int8up)                     \
    APPLY_BINARY_OPERATOR(463, int8pl)                     \
    APPLY_BINARY_OPERATOR(464, int8mi)                     \
    APPLY_BINARY_OPERATOR(465, int8mul)                    \
    APPLY_BINARY_OPERATOR(466, int8div)                    \
    APPLY_UNARY_OPERATOR(1230, int8abs)                    \
    APPLY_BINARY_OPERATOR(945, int8mod)                    \
    APPLY_UNARY_OPERATOR(1219, int8inc)                    \
    APPLY_BINARY_OPERATOR(1274, int84pl)                   \
    APPLY_BINARY_OPERATOR(1275, int84mi)                   \
    APPLY_BINARY_OPERATOR(1276, int84mul)                  \
    APPLY_BINARY_OPERATOR(1277, int84div)                  \
    APPLY_BINARY_OPERATOR(1278, int48pl)                   \
    APPLY_BINARY_OPERATOR(1279, int48mi)                   \
    APPLY_BINARY_OPERATOR(1280, int48mul)                  \
    APPLY_BINARY_OPERATOR(1281, int48div)                  \
    APPLY_BINARY_OPERATOR(837, int82pl)                    \
    APPLY_BINARY_OPERATOR(838, int82mi)                    \
    APPLY_BINARY_OPERATOR(839, int82mul)                   \
    APPLY_BINARY_OPERATOR(840, int82div)                   \
    APPLY_BINARY_OPERATOR(841, int28pl)                    \
    APPLY_BINARY_OPERATOR(942, int28mi)                    \
    APPLY_BINARY_OPERATOR(943, int28mul)                   \
    APPLY_BINARY_OPERATOR(948, int28div)                   \
    APPLY_BINARY_OPERATOR(1904, int8and)                   \
    APPLY_BINARY_OPERATOR(1905, int8or)                    \
    APPLY_BINARY_OPERATOR(1906, int8xor)                   \
    APPLY_BINARY_OPERATOR(1908, int8shl)                   \
    APPLY_BINARY_OPERATOR(1909, int8shr)                   \
    APPLY_UNARY_OPERATOR(1907, int8not)                    \
    APPLY_UNARY_CAST_OPERATOR(3177, int8_bool)             \
    APPLY_UNARY_CAST_OPERATOR(3178, bool_int8)             \
    APPLY_UNARY_CAST_OPERATOR(481, int48)                  \
    APPLY_UNARY_CAST_OPERATOR(480, int84)                  \
    APPLY_UNARY_CAST_OPERATOR(754, int28)                  \
    APPLY_UNARY_CAST_OPERATOR(714, int82)                  \
    APPLY_UNARY_CAST_OPERATOR(482, i8tod)                  \
    APPLY_UNARY_CAST_OPERATOR(483, dtoi8)                  \
    APPLY_UNARY_CAST_OPERATOR(652, i8tof)                  \
    APPLY_UNARY_CAST_OPERATOR(653, ftoi8)                  \
    APPLY_BINARY_OPERATOR(1086, date_eq)                   \
    APPLY_BINARY_OPERATOR(1140, date_mi)                   \
    APPLY_BINARY_OPERATOR(1141, date_pli)                  \
    APPLY_BINARY_OPERATOR(1142, date_mii)                  \
    APPLY_BINARY_OPERATOR(2340, date_eq_timestamp)         \
    APPLY_BINARY_OPERATOR(2353, date_eq_timestamptz)       \
    APPLY_BINARY_OPERATOR(2366, timestamp_eq_date)         \
    APPLY_BINARY_OPERATOR(2379, timestamptz_eq_date)       \
    APPLY_BINARY_OPERATOR(2071, date_pl_interval)          \
    APPLY_BINARY_OPERATOR(2072, date_mi_interval)          \
    APPLY_UNARY_CAST_OPERATOR(2024, date_timestamp)        \
    APPLY_UNARY_CAST_OPERATOR(2029, timestamp_date)        \
    APPLY_UNARY_CAST_OPERATOR(1174, date_timestamptz)      \
    APPLY_UNARY_CAST_OPERATOR(1178, timestamptz_date)      \
    APPLY_UNARY_CAST_OPERATOR(1179, abstime_date)          \
    APPLY_UNARY_OPERATOR(3944, time_transform)             \
    APPLY_BINARY_OPERATOR(1968, time_scale)                \
    APPLY_BINARY_OPERATOR(1145, time_eq)                   \
    APPLY_UNARY_CAST_OPERATOR(1316, timestamp_time)        \
    APPLY_UNARY_CAST_OPERATOR(2019, timestamptz_time)      \
    APPLY_BINARY_OPERATOR(2025, datetime_timestamp)        \
    APPLY_UNARY_CAST_OPERATOR(1370, time_interval)         \
    APPLY_UNARY_CAST_OPERATOR(1419, interval_time)         \
    APPLY_BINARY_OPERATOR(1690, time_mi_time)              \
    APPLY_BINARY_OPERATOR(1747, time_pl_interval)          \
    APPLY_BINARY_OPERATOR(1969, timetz_scale)              \
    APPLY_BINARY_OPERATOR(1352, timetz_eq)                 \
    APPLY_BINARY_OPERATOR(1749, timetz_pl_interval)        \
    APPLY_BINARY_OPERATOR(1750, timetz_mi_interval)        \
    APPLY_UNARY_CAST_OPERATOR(2046, timetz_time)           \
    APPLY_UNARY_CAST_OPERATOR(2047, time_timetz)           \
    APPLY_UNARY_CAST_OPERATOR(1388, timestamptz_timetz)    \
    APPLY_BINARY_OPERATOR(1297, datetimetz_timestamptz)    \
    APPLY_BINARY_OPERATOR(2037, timetz_zone)               \
    APPLY_BINARY_OPERATOR(2038, timetz_izone)              \
    APPLY_BINARY_OPERATOR(5580, smalldatetime_eq)          \
    APPLY_UNARY_OPERATOR(3917, timestamp_transform)        \
    APPLY_BINARY_OPERATOR(1961, timestamp_scale)           \
    APPLY_BINARY_OPERATOR(1967, timestamptz_scale)         \
    APPLY_BINARY_OPERATOR(2052, timestamp_eq)              \
    APPLY_BINARY_OPERATOR(2522, timestamp_eq_timestamptz)  \
    APPLY_BINARY_OPERATOR(2529, timestamptz_eq_timestamp)  \
    APPLY_UNARY_OPERATOR(3918, interval_transform)         \
    APPLY_BINARY_OPERATOR(1200, interval_scale)            \
    APPLY_BINARY_OPERATOR(1162, interval_eq)               \
    APPLY_BINARY_OPERATOR(2031, timestamp_mi)              \
    APPLY_UNARY_OPERATOR(2711, interval_justify_interval)  \
    APPLY_UNARY_OPERATOR(1175, interval_justify_hours)     \
    APPLY_UNARY_OPERATOR(1295, interval_justify_days)      \
    APPLY_BINARY_OPERATOR(2032, timestamp_pl_interval)     \
    APPLY_BINARY_OPERATOR(2033, timestamp_mi_interval)     \
    APPLY_BINARY_OPERATOR(1189, timestamptz_pl_interval)   \
    APPLY_BINARY_OPERATOR(1190, timestamptz_mi_interval)   \
    APPLY_UNARY_OPERATOR(1168, interval_um)                \
    APPLY_BINARY_OPERATOR(1169, interval_pl)               \
    APPLY_BINARY_OPERATOR(1170, interval_mi)               \
    APPLY_BINARY_OPERATOR(1618, interval_mul)              \
    APPLY_BINARY_OPERATOR(1624, mul_d_interval)            \
    APPLY_BINARY_OPERATOR(1326, interval_div)              \
    APPLY_UNARY_CAST_OPERATOR(2028, timestamp_timestamptz) \
    APPLY_UNARY_CAST_OPERATOR(2027, timestamptz_timestamp) \
    APPLY_TERNARY_CAST_OPERATOR(668, bpchar)               \
    APPLY_UNARY_OPERATOR(3097, varchar_transform)          \
    APPLY_TERNARY_CAST_OPERATOR(669, varchar)              \
    APPLY_BINARY_OPERATOR(1048, bpchareq)                  \
    APPLY_BINARY_OPERATOR(67, texteq)                      \
    APPLY_UNARY_OPERATOR(207, float4abs)                   \
    APPLY_UNARY_OPERATOR(206, float4um)                    \
    APPLY_UNARY_OPERATOR(1913, float4up)                   \
    APPLY_UNARY_OPERATOR(221, float8abs)                   \
    APPLY_UNARY_OPERATOR(220, float8um)                    \
    APPLY_UNARY_OPERATOR(1914, float8up)                   \
    APPLY_BINARY_OPERATOR(204, float4pl)                   \
    APPLY_BINARY_OPERATOR(205, float4mi)                   \
    APPLY_BINARY_OPERATOR(202, float4mul)                  \
    APPLY_BINARY_OPERATOR(203, float4div)                  \
    APPLY_BINARY_OPERATOR(218, float8pl)                   \
    APPLY_BINARY_OPERATOR(219, float8mi)                   \
    APPLY_BINARY_OPERATOR(216, float8mul)                  \
    APPLY_BINARY_OPERATOR(217, float8div)                  \
    APPLY_BINARY_OPERATOR(287, float4eq)                   \
    APPLY_BINARY_OPERATOR(293, float8eq)                   \
    APPLY_UNARY_CAST_OPERATOR(311, ftod)                   \
    APPLY_UNARY_CAST_OPERATOR(312, dtof)                   \
    APPLY_UNARY_CAST_OPERATOR(317, dtoi4)                  \
    APPLY_UNARY_CAST_OPERATOR(237, dtoi2)                  \
    APPLY_UNARY_CAST_OPERATOR(316, i4tod)                  \
    APPLY_UNARY_CAST_OPERATOR(235, i2tod)                  \
    APPLY_UNARY_CAST_OPERATOR(319, ftoi4)                  \
    APPLY_UNARY_CAST_OPERATOR(238, ftoi2)                  \
    APPLY_UNARY_CAST_OPERATOR(318, i4tof)                  \
    APPLY_UNARY_CAST_OPERATOR(236, i2tof)                  \
    APPLY_UNARY_OPERATOR(228, dround)                      \
    APPLY_UNARY_OPERATOR(2320, dceil)                      \
    APPLY_UNARY_OPERATOR(2309, dfloor)                     \
    APPLY_UNARY_OPERATOR(2310, dsign)                      \
    APPLY_UNARY_OPERATOR(229, dtrunc)                      \
    APPLY_UNARY_OPERATOR(230, dsqrt)                       \
    APPLY_UNARY_OPERATOR(231, dcbrt)                       \
    APPLY_UNARY_OPERATOR(232, dpow)                        \
    APPLY_UNARY_OPERATOR(233, dexp)                        \
    APPLY_UNARY_OPERATOR(234, dlog1)                       \
    APPLY_UNARY_OPERATOR(1339, dlog10)                     \
    APPLY_UNARY_OPERATOR(1601, dacos)                      \
    APPLY_UNARY_OPERATOR(1600, dasin)                      \
    APPLY_UNARY_OPERATOR(1602, datan)                      \
    APPLY_UNARY_OPERATOR(1603, datan2)                     \
    APPLY_UNARY_OPERATOR(1605, dcos)                       \
    APPLY_UNARY_OPERATOR(1607, dcot)                       \
    APPLY_UNARY_OPERATOR(1604, dsin)                       \
    APPLY_UNARY_OPERATOR(1606, dtan)                       \
    APPLY_UNARY_OPERATOR(1608, degrees)                    \
    APPLY_UNARY_OPERATOR(1610, dpi)                        \
    APPLY_UNARY_OPERATOR(1609, radians)                    \
    APPLY_UNARY_OPERATOR(1598, drandom)                    \
    APPLY_UNARY_OPERATOR(1599, setseed)                    \
    APPLY_BINARY_OPERATOR(281, float48pl)                  \
    APPLY_BINARY_OPERATOR(282, float48mi)                  \
    APPLY_BINARY_OPERATOR(279, float48mul)                 \
    APPLY_BINARY_OPERATOR(280, float48div)                 \
    APPLY_BINARY_OPERATOR(285, float84pl)                  \
    APPLY_BINARY_OPERATOR(286, float84mi)                  \
    APPLY_BINARY_OPERATOR(283, float84mul)                 \
    APPLY_BINARY_OPERATOR(284, float84div)                 \
    APPLY_BINARY_OPERATOR(299, float48eq)                  \
    APPLY_BINARY_OPERATOR(305, float84eq)                  \
    APPLY_UNARY_OPERATOR(3157, numeric_transform)          \
    APPLY_BINARY_CAST_OPERATOR(1703, numeric)              \
    APPLY_UNARY_OPERATOR(1704, numeric_abs)                \
    APPLY_UNARY_OPERATOR(1771, numeric_uminus)             \
    APPLY_UNARY_OPERATOR(1915, numeric_uplus)              \
    APPLY_UNARY_OPERATOR(1706, numeric_sign)               \
    APPLY_UNARY_OPERATOR(1707, numeric_round)              \
    APPLY_UNARY_OPERATOR(1709, numeric_trunc)              \
    APPLY_UNARY_OPERATOR(2167, numeric_ceil)               \
    APPLY_UNARY_OPERATOR(1712, numeric_floor)              \
    APPLY_BINARY_OPERATOR(1718, numeric_eq)                \
    APPLY_BINARY_OPERATOR(1724, numeric_add)               \
    APPLY_BINARY_OPERATOR(1725, numeric_sub)               \
    APPLY_BINARY_OPERATOR(1726, numeric_mul)               \
    APPLY_BINARY_OPERATOR(1727, numeric_div)               \
    APPLY_BINARY_OPERATOR(1980, numeric_div_trunc)         \
    APPLY_BINARY_OPERATOR(1729, numeric_mod)               \
    APPLY_UNARY_OPERATOR(1764, numeric_inc)                \
    APPLY_UNARY_OPERATOR(111, numeric_fac)                 \
    APPLY_UNARY_OPERATOR(1731, numeric_sqrt)               \
    APPLY_UNARY_OPERATOR(1733, numeric_exp)                \
    APPLY_UNARY_OPERATOR(1735, numeric_ln)                 \
    APPLY_BINARY_OPERATOR(1737, numeric_log)               \
    APPLY_BINARY_OPERATOR(1739, numeric_power)             \
    APPLY_UNARY_CAST_OPERATOR(1740, int4_numeric)          \
    APPLY_UNARY_CAST_OPERATOR(1744, numeric_int4)          \
    APPLY_UNARY_CAST_OPERATOR(1781, int8_numeric)          \
    APPLY_UNARY_CAST_OPERATOR(1779, numeric_int8)          \
    APPLY_UNARY_CAST_OPERATOR(1783, numeric_int2)          \
    APPLY_UNARY_CAST_OPERATOR(5521, int1_numeric)          \
    APPLY_UNARY_CAST_OPERATOR(5522, numeric_int1)          \
    APPLY_UNARY_CAST_OPERATOR(1743, float8_numeric)        \
    APPLY_UNARY_CAST_OPERATOR(1746, numeric_float8)        \
    APPLY_UNARY_CAST_OPERATOR(1742, float4_numeric)        \
    APPLY_UNARY_CAST_OPERATOR(1745, numeric_float4)        \
    APPLY_BINARY_OPERATOR(2746, int8_avg_accum)            \
    APPLY_BINARY_OPERATOR(1963, int4_avg_accum)            \
    APPLY_BINARY_OPERATOR(1962, int2_avg_accum)            \
    APPLY_BINARY_OPERATOR(5548, int1_avg_accum)            \
    APPLY_BINARY_OPERATOR(208, float4_accum)               \
    APPLY_BINARY_OPERATOR(222, float8_accum)               \
    APPLY_BINARY_OPERATOR(2858, numeric_avg_accum)         \
    APPLY_BINARY_OPERATOR(2996, int8_sum_to_int8)          \
    APPLY_BINARY_OPERATOR(1842, int8_sum)                  \
    APPLY_BINARY_OPERATOR(1841, int4_sum)                  \
    APPLY_BINARY_OPERATOR(1840, int2_sum)                  \
    APPLY_BINARY_OPERATOR(1236, int8larger)                \
    APPLY_BINARY_OPERATOR(768, int4larger)                 \
    APPLY_BINARY_OPERATOR(770, int2larger)                 \
    APPLY_BINARY_OPERATOR(6115, int1larger)                \
    APPLY_BINARY_OPERATOR(209, float4larger)               \
    APPLY_BINARY_OPERATOR(223, float8larger)               \
    APPLY_BINARY_OPERATOR(1767, numeric_larger)            \
    APPLY_BINARY_OPERATOR(2036, timestamp_larger)          \
    APPLY_BINARY_OPERATOR(1138, date_larger)               \
    APPLY_BINARY_OPERATOR(1063, bpchar_larger)             \
    APPLY_BINARY_OPERATOR(458, text_larger)                \
    APPLY_BINARY_OPERATOR(1237, int8smaller)               \
    APPLY_BINARY_OPERATOR(769, int4smaller)                \
    APPLY_BINARY_OPERATOR(771, int2smaller)                \
    APPLY_BINARY_OPERATOR(211, float4smaller)              \
    APPLY_BINARY_OPERATOR(224, float8smaller)              \
    APPLY_BINARY_OPERATOR(1766, numeric_smaller)           \
    APPLY_BINARY_OPERATOR(2035, timestamp_smaller)         \
    APPLY_BINARY_OPERATOR(1139, date_smaller)              \
    APPLY_BINARY_OPERATOR(1064, bpchar_smaller)            \
    APPLY_BINARY_OPERATOR(459, text_smaller)               \
    APPLY_BINARY_OPERATOR(468, int8ne)                     \
    APPLY_BINARY_OPERATOR(469, int8lt)                     \
    APPLY_BINARY_OPERATOR(470, int8gt)                     \
    APPLY_BINARY_OPERATOR(471, int8le)                     \
    APPLY_BINARY_OPERATOR(472, int8ge)                     \
    APPLY_BINARY_OPERATOR(144, int4ne)                     \
    APPLY_BINARY_OPERATOR(66, int4lt)                      \
    APPLY_BINARY_OPERATOR(147, int4gt)                     \
    APPLY_BINARY_OPERATOR(149, int4le)                     \
    APPLY_BINARY_OPERATOR(150, int4ge)                     \
    APPLY_BINARY_OPERATOR(145, int2ne)                     \
    APPLY_BINARY_OPERATOR(64, int2lt)                      \
    APPLY_BINARY_OPERATOR(146, int2gt)                     \
    APPLY_BINARY_OPERATOR(148, int2le)                     \
    APPLY_BINARY_OPERATOR(151, int2ge)                     \
    APPLY_BINARY_OPERATOR(5508, int1ne)                    \
    APPLY_BINARY_OPERATOR(5509, int1lt)                    \
    APPLY_BINARY_OPERATOR(5511, int1gt)                    \
    APPLY_BINARY_OPERATOR(5510, int1le)                    \
    APPLY_BINARY_OPERATOR(5512, int1ge)                    \
    APPLY_BINARY_OPERATOR(288, float4ne)                   \
    APPLY_BINARY_OPERATOR(289, float4lt)                   \
    APPLY_BINARY_OPERATOR(291, float4gt)                   \
    APPLY_BINARY_OPERATOR(290, float4le)                   \
    APPLY_BINARY_OPERATOR(292, float4ge)                   \
    APPLY_BINARY_OPERATOR(294, float8ne)                   \
    APPLY_BINARY_OPERATOR(295, float8lt)                   \
    APPLY_BINARY_OPERATOR(297, float8gt)                   \
    APPLY_BINARY_OPERATOR(296, float8le)                   \
    APPLY_BINARY_OPERATOR(298, float8ge)                   \
    APPLY_BINARY_OPERATOR(1719, numeric_ne)                \
    APPLY_BINARY_OPERATOR(1722, numeric_lt)                \
    APPLY_BINARY_OPERATOR(1720, numeric_gt)                \
    APPLY_BINARY_OPERATOR(1723, numeric_le)                \
    APPLY_BINARY_OPERATOR(1721, numeric_ge)                \
    APPLY_BINARY_OPERATOR(2053, timestamp_ne)              \
    APPLY_BINARY_OPERATOR(2054, timestamp_lt)              \
    APPLY_BINARY_OPERATOR(2057, timestamp_gt)              \
    APPLY_BINARY_OPERATOR(2055, timestamp_le)              \
    APPLY_BINARY_OPERATOR(2056, timestamp_ge)              \
    APPLY_BINARY_OPERATOR(1091, date_ne)                   \
    APPLY_BINARY_OPERATOR(1087, date_lt)                   \
    APPLY_BINARY_OPERATOR(1089, date_gt)                   \
    APPLY_BINARY_OPERATOR(1088, date_le)                   \
    APPLY_BINARY_OPERATOR(1090, date_ge)                   \
    APPLY_BINARY_OPERATOR(1053, bpcharne)                  \
    APPLY_BINARY_OPERATOR(1049, bpcharlt)                  \
    APPLY_BINARY_OPERATOR(1051, bpchargt)                  \
    APPLY_BINARY_OPERATOR(1050, bpcharle)                  \
    APPLY_BINARY_OPERATOR(1052, bpcharge)                  \
    APPLY_BINARY_OPERATOR(157, textne)                     \
    APPLY_BINARY_OPERATOR(740, text_lt)                    \
    APPLY_BINARY_OPERATOR(742, text_gt)                    \
    APPLY_BINARY_OPERATOR(741, text_le)                    \
    APPLY_BINARY_OPERATOR(743, text_ge)

// aggregate operators have different identifiers for the same functions, so we define them separately
// nevertheless, the operators are defined as well in APPLY_OPERATORS()
#define APPLY_AGG_OPERATORS()                                     \
    APPLY_BINARY_OPERATOR(INT8AVGFUNCOID, int8_avg_accum)         \
    APPLY_BINARY_OPERATOR(INT4AVGFUNCOID, int4_avg_accum)         \
    APPLY_BINARY_OPERATOR(INT2AVGFUNCOID, int2_avg_accum)         \
    APPLY_BINARY_OPERATOR(5537, int1_avg_accum)                   \
    APPLY_BINARY_OPERATOR(2104, float4_accum)                     \
    APPLY_BINARY_OPERATOR(2105, float8_accum)                     \
    APPLY_BINARY_OPERATOR(NUMERICAVGFUNCOID, numeric_avg_accum)   \
    APPLY_BINARY_OPERATOR(2996, int8_sum_to_int8)                 \
    APPLY_BINARY_OPERATOR(INT8SUMFUNCOID, int8_sum)               \
    APPLY_BINARY_OPERATOR(INT4SUMFUNCOID, int4_sum)               \
    APPLY_BINARY_OPERATOR(INT2SUMFUNCOID, int2_sum)               \
    APPLY_BINARY_OPERATOR(2110, float4pl)                         \
    APPLY_BINARY_OPERATOR(2111, float8pl)                         \
    APPLY_BINARY_OPERATOR(NUMERICSUMFUNCOID, numeric_add)         \
    APPLY_BINARY_OPERATOR(INT8LARGERFUNCOID, int8larger)          \
    APPLY_BINARY_OPERATOR(INT4LARGERFUNCOID, int4larger)          \
    APPLY_BINARY_OPERATOR(INT2LARGERFUNCOID, int2larger)          \
    APPLY_BINARY_OPERATOR(5538, int1larger)                       \
    APPLY_BINARY_OPERATOR(2119, float4larger)                     \
    APPLY_BINARY_OPERATOR(2120, float8larger)                     \
    APPLY_BINARY_OPERATOR(NUMERICLARGERFUNCOID, numeric_larger)   \
    APPLY_BINARY_OPERATOR(2126, timestamp_larger)                 \
    APPLY_BINARY_OPERATOR(2122, date_larger)                      \
    APPLY_BINARY_OPERATOR(2244, bpchar_larger)                    \
    APPLY_BINARY_OPERATOR(2129, text_larger)                      \
    APPLY_BINARY_OPERATOR(INT8SMALLERFUNCOID, int8smaller)        \
    APPLY_BINARY_OPERATOR(INT4SMALLERFUNCOID, int4smaller)        \
    APPLY_BINARY_OPERATOR(INT2SMALLERFUNCOID, int2smaller)        \
    APPLY_BINARY_OPERATOR(2135, float4smaller)                    \
    APPLY_BINARY_OPERATOR(2120, float8smaller)                    \
    APPLY_BINARY_OPERATOR(NUMERICSMALLERFUNCOID, numeric_smaller) \
    APPLY_BINARY_OPERATOR(2142, timestamp_smaller)                \
    APPLY_BINARY_OPERATOR(2138, date_smaller)                     \
    APPLY_BINARY_OPERATOR(2245, bpchar_smaller)                   \
    APPLY_BINARY_OPERATOR(2145, text_smaller)                     \
    APPLY_UNARY_OPERATOR(2803, int8inc)

#endif
