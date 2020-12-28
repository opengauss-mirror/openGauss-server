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
 * float.h
 *
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_fmt/float.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef FLOAT_H
#define FLOAT_H

#include "postgres_fe.h"
#include <string>

bool scan_float4(const char *num, float4 *res, char *errMsg);
bool float4toa(float4 num, char *ascii);
bool scan_float8(const char *num, float8 *res, char *errMsg);
bool float8toa(float8 num, char *ascii);

#endif