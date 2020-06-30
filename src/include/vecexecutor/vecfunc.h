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
 * vecfunc.h 
 * 
 * 
 * IDENTIFICATION
 *        src/include/vecexecutor/vecfunc.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VECFUNC_H
#define VECFUNC_H

#include "utils/hsearch.h"

#include "utils/builtins.h"
#include "utils/fmgrtab.h"
#include "mb/pg_wchar.h"

#define FUNCACHE_NUM 8

typedef struct {
    /* fn_oid is the hash key and so must be first! */
    Oid fn_oid;                                /* OID of an external C function */
    VectorFunction vec_fn_cache[FUNCACHE_NUM]; /* address of its info record */
    VectorFunction vec_agg_cache[FUNCACHE_NUM];

    /*
     * function cache pointer to the sonic hash agg function if supported
     */
    VectorFunction vec_sonic_agg_cache[FUNCACHE_NUM];

    /* vec_transform_function has three applications
     * 1. this function is used for the last stage of avg(vec_aggfinal_function);
     * 2. we can replace some native PGFunctions to our transformed function for some reasons;
     *    (such as: int_numeric TO int_numeric_bi)
     * 3. this interface is reserved for later use.
     */
    PGFunction vec_transform_function[FUNCACHE_NUM];

    /*
     * sonic transform functions, which have the same meanings as vec_transform_function, but
     * used only for sonic cases.
     */
    PGFunction vec_sonic_transform_function[FUNCACHE_NUM];

} VecFuncCacheEntry;

typedef Datum (*sub_Array)(Datum str, int32 start, int32 length, bool* isnull, mblen_converter fun_mblen);

extern sub_Array substr_Array[32];

#endif /* VECFUNC_H */
