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
 * hll_function.h
 *    Realize the upper-layer functions of HLL algorithm.
 *
 * IDENTIFICATION
 *    src/include/utils/hll_function.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef HLL_FUNCTION_H
#define HLL_FUNCTION_H

#include "fmgr.h"

/* numbers of parameter in hll and hll_empty functions */
#define HLL_PARAMETER0 0
#define HLL_PARAMETER1 1
#define HLL_PARAMETER2 2
#define HLL_PARAMETER3 3
#define HLL_PARAMETER4 4

Datum hll(PG_FUNCTION_ARGS);
Datum hll_in(PG_FUNCTION_ARGS);
Datum hll_out(PG_FUNCTION_ARGS);
Datum hll_recv(PG_FUNCTION_ARGS);
Datum hll_send(PG_FUNCTION_ARGS);

Datum hll_typmod_in(PG_FUNCTION_ARGS);
Datum hll_typmod_out(PG_FUNCTION_ARGS);

Datum hll_hashval_in(PG_FUNCTION_ARGS);
Datum hll_hashval_out(PG_FUNCTION_ARGS);
Datum hll_hashval(PG_FUNCTION_ARGS);
Datum hll_hashval_int4(PG_FUNCTION_ARGS);
Datum hll_hashval_eq(PG_FUNCTION_ARGS);
Datum hll_hashval_ne(PG_FUNCTION_ARGS);

Datum hll_hash_1byte(PG_FUNCTION_ARGS);
Datum hll_hash_1bytes(PG_FUNCTION_ARGS);
Datum hll_hash_2byte(PG_FUNCTION_ARGS);
Datum hll_hash_2bytes(PG_FUNCTION_ARGS);
Datum hll_hash_4byte(PG_FUNCTION_ARGS);
Datum hll_hash_4bytes(PG_FUNCTION_ARGS);
Datum hll_hash_8byte(PG_FUNCTION_ARGS);
Datum hll_hash_8bytes(PG_FUNCTION_ARGS);
Datum hll_hash_varlena(PG_FUNCTION_ARGS);
Datum hll_hash_varlenas(PG_FUNCTION_ARGS);
Datum hll_hash_any(PG_FUNCTION_ARGS);
Datum hll_hash_anys(PG_FUNCTION_ARGS);

Datum hll_eq(PG_FUNCTION_ARGS);
Datum hll_ne(PG_FUNCTION_ARGS);

Datum hll_add(PG_FUNCTION_ARGS);
Datum hll_add_rev(PG_FUNCTION_ARGS);
Datum hll_empty0(PG_FUNCTION_ARGS);
Datum hll_empty1(PG_FUNCTION_ARGS);
Datum hll_empty2(PG_FUNCTION_ARGS);
Datum hll_empty3(PG_FUNCTION_ARGS);
Datum hll_empty4(PG_FUNCTION_ARGS);
Datum hll_empty5(PG_FUNCTION_ARGS);
Datum hll_union(PG_FUNCTION_ARGS);
Datum hll_cardinality(PG_FUNCTION_ARGS);

Datum hll_print(PG_FUNCTION_ARGS);
Datum hll_type(PG_FUNCTION_ARGS);
Datum hll_log2m(PG_FUNCTION_ARGS);
Datum hll_log2explicit(PG_FUNCTION_ARGS);
Datum hll_log2sparse(PG_FUNCTION_ARGS);
Datum hll_duplicatecheck(PG_FUNCTION_ARGS);

/* these functions are not supported again */
Datum hll_expthresh(PG_FUNCTION_ARGS);
Datum hll_regwidth(PG_FUNCTION_ARGS);
Datum hll_schema_version(PG_FUNCTION_ARGS);
Datum hll_sparseon(PG_FUNCTION_ARGS);

#endif
