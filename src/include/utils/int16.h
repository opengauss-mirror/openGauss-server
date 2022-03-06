/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * int16.h
 *    Declarations for operations on 1216-bit integers.
 *
 * IDENTIFICATION
 *    src/include/utils/int16.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef INT16_H
#define INT16_H

#include "fmgr.h"

extern bool scanint16(const char* str, bool errorOK, int128* result);

extern Datum int16in(PG_FUNCTION_ARGS);
extern Datum int16out(PG_FUNCTION_ARGS);
extern Datum int16recv(PG_FUNCTION_ARGS);
extern Datum int16send(PG_FUNCTION_ARGS);

extern Datum int16eq(PG_FUNCTION_ARGS);
extern Datum int16ne(PG_FUNCTION_ARGS);
extern Datum int16lt(PG_FUNCTION_ARGS);
extern Datum int16gt(PG_FUNCTION_ARGS);
extern Datum int16le(PG_FUNCTION_ARGS);
extern Datum int16ge(PG_FUNCTION_ARGS);

extern Datum int16pl(PG_FUNCTION_ARGS);
extern Datum int16mi(PG_FUNCTION_ARGS);
extern Datum int16mul(PG_FUNCTION_ARGS);
extern Datum int16div(PG_FUNCTION_ARGS);

extern Datum int1_16(PG_FUNCTION_ARGS);
extern Datum int16_1(PG_FUNCTION_ARGS);
extern Datum int2_16(PG_FUNCTION_ARGS);
extern Datum int16_2(PG_FUNCTION_ARGS);
extern Datum int4_16(PG_FUNCTION_ARGS);
extern Datum int16_4(PG_FUNCTION_ARGS);
extern Datum int8_16(PG_FUNCTION_ARGS);
extern Datum int16_8(PG_FUNCTION_ARGS);
extern Datum dtoi16(PG_FUNCTION_ARGS);
extern Datum i16tod(PG_FUNCTION_ARGS);
extern Datum ftoi16(PG_FUNCTION_ARGS);
extern Datum i16tof(PG_FUNCTION_ARGS);
extern Datum oidtoi16(PG_FUNCTION_ARGS);
extern Datum i16tooid(PG_FUNCTION_ARGS);
extern Datum int16_bool(PG_FUNCTION_ARGS);
extern Datum bool_int16(PG_FUNCTION_ARGS);
extern Datum int16_numeric(PG_FUNCTION_ARGS);
extern Datum numeric_int16(PG_FUNCTION_ARGS);

#endif /* INT16_H */