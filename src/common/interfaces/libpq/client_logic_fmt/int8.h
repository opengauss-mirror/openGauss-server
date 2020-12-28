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
 * int8.h
 *
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_fmt/int8.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef INT8_H
#define INT8_H

#include "postgres_fe.h"
#include <string>

bool scanint8(const char *str, bool errorOK, int64 *result, char *err_msg);

#endif
