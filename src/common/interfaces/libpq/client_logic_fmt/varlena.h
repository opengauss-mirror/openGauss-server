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
 * varlena.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\varlena.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef VARLENA_H
#define VARLENA_H

#include <string>

unsigned char *byteain(const char *inputText, size_t *binary_size, char *err_msg);
char *byteaout(const unsigned char *data, size_t size, size_t *result_size);

#endif