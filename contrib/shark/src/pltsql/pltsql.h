/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2026. All rights reserved.
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
 * --------------------------------------------------------------------------------------
 * 
 * pltsql.h
 * 
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * IDENTIFICATION
 *        \contrib\shark\src\pltsql\pltsql.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PLTSQL_H
#define PLTSQL_H

#include "utils/plpgsql.h"

/**********************************************************************
 * Definitions
 **********************************************************************/
 
extern int pltsql_yyparse(void);
extern bool pltsql_is_token_match2(int token, int token_next);
extern bool pltsql_is_token_match(int token);
extern int pltsql_yylex(void);
extern void pltsql_push_back_token(int token);
extern void pltsql_scanner_init(const char* str);
extern "C" Datum pltsql_call_handler(PG_FUNCTION_ARGS);
extern "C" Datum pltsql_inline_handler(PG_FUNCTION_ARGS);
extern "C" Datum pltsql_validator(PG_FUNCTION_ARGS);

extern PLpgSQL_function* pltsql_compile(FunctionCallInfo fcinfo, bool forValidator, bool isRecompile = false);
extern void pltsql_scanner_init(const char* str);
extern void pltsql_scanner_finish(void);
extern PLpgSQL_function* pltsql_compile_inline(char* proc_source);

extern bool check_vaild_username(const char* name);

#endif /* PLTSQL_H */