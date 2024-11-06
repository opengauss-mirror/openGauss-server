/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 * corr_sk.h
 * Aggregate for computing the statistical correlation
 *
 * IDENTIFICATION
 * 	  src/include/utils/corr_sk.h
 * -------------------------------------------------------------------------
 */

#ifndef CORR_SK_H
#define CORR_SK_H

#include "fmgr.h"

extern Datum corr_sk_trans_fn(PG_FUNCTION_ARGS);
extern Datum corr_sk_trans_fn_no3(PG_FUNCTION_ARGS);
extern Datum corr_s_final_fn(PG_FUNCTION_ARGS);
extern Datum corr_k_final_fn(PG_FUNCTION_ARGS);

#endif /* CORR_SK_H */