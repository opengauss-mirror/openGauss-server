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
 * numutils.h
 *
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_fmt/numutils.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef NUMUTILS_H
#define NUMUTILS_H

#include "postgres_fe.h"
#include <string>

typedef struct pg_conn PGconn;

bool fe_pg_atoi8(const PGconn* conn, const char *s, int8 *res, char *err_msg);
bool fe_pg_strtoint16(const PGconn* conn, const char *s, int16 *res, char *err_msg);
bool fe_pg_strtoint32(const PGconn* conn, const char *s, int32 *res, char *err_msg);
bool fe_pg_ctoa(uint8 i, char *a);
bool fe_pg_itoa(int16 i, char *a);
bool fe_pg_ltoa(int32 value, char *a);
bool fe_pg_lltoa(int64 value, char *a);

#endif