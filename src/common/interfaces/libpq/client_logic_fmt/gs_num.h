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
 * gs_num.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\gs_num.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_NUM_H
#define GS_NUM_H

#include <string>
#include "postgres_fe.h"

typedef struct pg_conn PGconn;

unsigned char *int1_bin(const PGconn* conn, const char *text, size_t *binary_size, char *err_msg);
char *int1_bout(const unsigned char *binary, size_t size, size_t *result_size);
unsigned char *int1_badjust(unsigned char *binary, size_t *binary_size, const char *err_msg);
unsigned char *int1_brestore(const unsigned char *binary, size_t size, size_t *result_size, const char *err_msg);
unsigned char *int2_bin(const PGconn* conn, const char *text, size_t *binary_size, char *err_msg);
char *int2_bout(const unsigned char *binary, size_t size, size_t *result_size);
unsigned char *int2_badjust(unsigned char *binary, size_t *binary_size, const char *err_msg);
unsigned char *int2_brestore(const unsigned char *binary, size_t size, size_t *result_size, const char *err_msg);
unsigned char *int4_bin(const PGconn* conn, const char *text, size_t *binary_size, char *err_msg);
char *int4_bout(const unsigned char *binary, size_t size, size_t *result_size);
unsigned char *int4_badjust(unsigned char *binary, size_t *binary_size, const char *err_msg);
unsigned char *int4_brestore(const unsigned char *binary, size_t size, size_t *result_size, const char *err_msg);
unsigned char *int8_bin(const PGconn* conn, const char *text, size_t *binary_size, char *err_msg);
char *int8_bout(const unsigned char *binary, size_t size, size_t *result_size);
unsigned char *int8_badjust(unsigned char *binary, size_t *binary_size, const char *err_msg);
unsigned char *int8_brestore(const unsigned char *binary, size_t size, size_t *result_size, const char *err_msg);
unsigned char *float4_bin(const char *text, size_t *binary_size, char *err_msg);
char *float4_bout(const unsigned char *binary, size_t size, size_t *result_size);
unsigned char *float8_bin(const char *text, size_t *binary_size, char *err_msg);
char *float8_bout(const unsigned char *binary, size_t size, size_t *result_size);
unsigned char *numeric_bin(const PGconn* conn, const char *text, int atttypmod, size_t *binary_size, char *err_msg);
char *numeric_bout(const unsigned char *binary, size_t size, size_t *result_size);
unsigned char *numeric_badjust(unsigned char *binary, size_t *binary_size, int atttypmod, char *err_msg);

#endif