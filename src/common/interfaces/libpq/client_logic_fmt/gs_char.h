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
 * gs_char.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\gs_char.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef GS_CHAR_H
#define GS_CHAR_H

#include <string>
#include "postgres_fe.h"

unsigned char *char_bin(const char *text, size_t *binary_size, const char *err_msg);
char *char_bout(const unsigned char *binary, size_t size, size_t *result_size);
unsigned char *bytea_bin(const char *text, size_t *binary_size, char *err_msg);
char *bytea_bout(const unsigned char *binary, size_t size, size_t *result_size);
unsigned char *fallback_bin(const char *text, size_t *binary_size);
char *fallback_bout(const unsigned char *binary, size_t size, size_t *result_size);
unsigned char *fallback_brestore(const unsigned char *binary, size_t size, size_t *result_size, const char *err_msg);
unsigned char *varchar_badjust(unsigned char *binary, size_t *binary_size, int atttypmod, char *err_msg);
unsigned char *nvarchar2_badjust(unsigned char *binary, size_t *binary_size, int atttypmod, char *err_msg);
unsigned char *bpchar_badjust(unsigned char *binary, size_t *binary_size, int atttypmod, char *err_msg);

#endif