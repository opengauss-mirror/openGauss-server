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
 * client_logic_common.h
 *
 * IDENTIFICATION
 *	  src\include\client_logic\client_logic_common.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_CL_COMMON_H
#define GS_CL_COMMON_H

#include <string>
#include "cstrings_map.h"

enum class ArgsParseState {
    KEY,
    VALUE,
    FLUSH
};

typedef CStringsMap StringArgsVec;
#endif
