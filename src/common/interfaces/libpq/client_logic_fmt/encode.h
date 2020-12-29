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
 * encode.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\encode.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef ENCODE_H
#define ENCODE_H

#include "postgres_fe.h"

unsigned hex_encode(const unsigned char *src, unsigned len, char *dst);
unsigned hex_decode(const char *src, unsigned len, unsigned char *dst);

#endif