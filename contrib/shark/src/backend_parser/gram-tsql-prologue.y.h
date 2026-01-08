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
 * gram-tsql-prologue.y.h
 *    functions for parser
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    contrib/shark/src/backend_parser/gram-tsql-prologue.y.h
 *
 *-------------------------------------------------------------------------
 */
#include "commands/dbcommands.h"
static void pgtsql_base_yyerror(YYLTYPE * yylloc, core_yyscan_t yyscanner, const char *msg);
List *TsqlSystemFuncName2(char *name);
static List* make_no_reseed_func(char* table_name, bool with_no_msgs, bool reseed_to_max);
static List* make_reseed_func(char* table_name, Node* new_seed, bool with_no_msgs);
static List* make_func_call_func(List* funcname,  List* args);
static char* quote_identifier_wrapper(char* ident, core_yyscan_t yyscanner);
static Node* TsqlMakeAnonyBlockFuncStmt(int flag, const char *str);
extern Oid get_language_oid(const char* langname, bool missing_ok);
static Node* makeTSQLHexStringConst(char *str, int location);
static Node* TsqlFunctionTryCast(Node* arg, TypeName* typname, int location);
static Node* TsqlFunctionConvert(TypeName* typname, Node* arg, Node* style, bool is_try, int location);
static Node* DoTypeCast(TypeName* typname, bool is_try, Node* arg, List* args, int location);
static bool is_qualifed_char_type(char* typename_string);
static void add_default_typmod(TypeName* typname);