/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 *---------------------------------------------------------------------------------------
 * db4ai_common.h
 *
 * IDENTIFICATION
 *    src/include/db4ai/db4ai_common.h
 *---------------------------------------------------------------------------------------
 */

#ifndef DB4AI_COMMON_H
#define DB4AI_COMMON_H

#include "utils/builtins.h" 
#include "utils/timestamp.h"

#define ITER_MAX 10000
#define MAX_BATCH_SIZE  0x0fffff

uint64_t time_diff(struct timespec *time_p1, struct timespec *time_p2);
double interval_to_sec(double time_interval);
double interval_to_msec(double time_interval);

Datum float8_get_datum(Oid type, float8 value);
float8 datum_get_float8(Oid type, Datum datum);
int32 datum_get_int(Oid type, Datum datum);

Datum string_to_datum(const char *str, Oid datatype);

void check_hyper_bounds(unsigned int num_x, unsigned int num_y, const char *hyper);

#endif /* DB4AI_COMMON_H */
