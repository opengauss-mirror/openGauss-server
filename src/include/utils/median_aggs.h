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
 *
 * median_aggs.h
 * Aggregate for computing the statistical median
 *
 * IDENTIFICATION
 * 	  src/include/utils/median_aggs.h
 * 
 * -------------------------------------------------------------------------
 */

#ifndef MEDIAN_AGGS_H
#define MEDIAN_AGGS_H

#include "fmgr.h"

extern Datum median_transfn(PG_FUNCTION_ARGS);
extern Datum median_float8_finalfn(PG_FUNCTION_ARGS);
extern Datum median_interval_finalfn(PG_FUNCTION_ARGS);

#endif /* MEDIAN_AGGS_H */