/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * hll_mpp.h
 *    Realize the distribute aggregation of HLL algorithm.
 *
 * IDENTIFICATION
 *    src/include/utils/hll_mpp.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef HLL_MPP_H
#define HLL_MPP_H

#include "fmgr.h"

/* numbers of parameter in hll_add_trans functions */
#define HLL_AGG_PARAMETER0 2
#define HLL_AGG_PARAMETER1 3
#define HLL_AGG_PARAMETER2 4
#define HLL_AGG_PARAMETER3 5
#define HLL_AGG_PARAMETER4 6

Datum hll_add_trans0(PG_FUNCTION_ARGS);
Datum hll_add_trans1(PG_FUNCTION_ARGS);
Datum hll_add_trans2(PG_FUNCTION_ARGS);
Datum hll_add_trans3(PG_FUNCTION_ARGS);
Datum hll_add_trans4(PG_FUNCTION_ARGS);
Datum hll_union_collect(PG_FUNCTION_ARGS);
Datum hll_union_trans(PG_FUNCTION_ARGS);
Datum hll_pack(PG_FUNCTION_ARGS);
Datum hll_trans_recv(PG_FUNCTION_ARGS);
Datum hll_trans_send(PG_FUNCTION_ARGS);
Datum hll_trans_in(PG_FUNCTION_ARGS);
Datum hll_trans_out(PG_FUNCTION_ARGS);

double estimate_hllagg_size(double numGroups, List *tleList);

#endif
