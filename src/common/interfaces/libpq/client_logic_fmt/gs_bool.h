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
 * gs_bool.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\gs_bool.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef GS_BOOL_H
#define GS_BOOL_H

#include "postgres_fe.h"
#include <string>

unsigned char *bool_bin(const char *text, const Oid typelem, const int atttypmod, size_t *binary_size,
    const char *err_msg);
char *bool_bout(const unsigned char *binary, size_t size, Oid typelem, int atttypmod, size_t *result_size);
unsigned char *int1_badjust(unsigned char *binary, Oid typelem, int atttypmod, char *err_msg);
bool int1_brestore(const unsigned char *binary, size_t size, Oid typelem, int atttypmod, unsigned char *res,
    char *err_msg);

#endif