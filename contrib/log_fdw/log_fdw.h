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
 * log_fdw.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        contrib/log_fdw/log_fdw.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _LOG_FDW_H_
#define _LOG_FDW_H_

#include "fmgr.h"

/*
 * SQL functions
 */
extern "C" Datum log_fdw_handler(PG_FUNCTION_ARGS);
extern "C" Datum log_fdw_validator(PG_FUNCTION_ARGS);

#endif /*_LOG_FDW_H_ */
