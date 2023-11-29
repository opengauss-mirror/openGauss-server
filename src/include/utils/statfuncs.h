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
 * ---------------------------------------------------------------------------------------
 *
 * statfuncs.h
 *     Macro definitions and methods related to statistical methods
 *
 * IDENTIFICATION
 *        src/include/utils/statfuncs.h
 * ---------------------------------------------------------------------------------------
 */
#ifndef STAT_FUNCS_H
#define STAT_FUNCS_H

#include "fmgr/fmgr_core.h"
#include "tuplestore.h"

#define STAT_OPER_DISABLE -1
#define STAT_OPER_RESET  0
#define STAT_OPER_ENABLE  1
#define STAT_OPER_GET 2

#define UINT8OID XIDOID

extern Tuplestorestate* BuildTupleResult(FunctionCallInfo fcinfo, TupleDesc* tupdesc);

#endif
