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
 * ts_zh_pound.h
 *        '#' split functions for full-text search of Chinese
 * 
 * 
 * IDENTIFICATION
 *        src/include/tsearch/ts_zh_pound.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef TS_ZH_POUND_H
#define TS_ZH_POUND_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "fmgr.h"

/* pound parser functions for chinese */
extern Datum pound_start(PG_FUNCTION_ARGS);
extern Datum pound_nexttoken(PG_FUNCTION_ARGS);
extern Datum pound_end(PG_FUNCTION_ARGS);
extern Datum pound_lextype(PG_FUNCTION_ARGS);

#endif /* TS_ZH_POUND_H */