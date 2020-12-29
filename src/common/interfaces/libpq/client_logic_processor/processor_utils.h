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
 * processor_utils.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\processor_utils.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PROCESSOR_UTILS_H
#define PROCESSOR_UTILS_H

#include "datatypes.h"
#include "libpq-int.h"

struct List;
bool name_list_to_cstring(const List *names, char *buffer, size_t buffer_len);

#endif