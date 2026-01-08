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
 * scan-tsql-prologue.l.h
 *      Constant data exported from this file.  This array maps from the
 *      zero-based keyword numbers returned by ScanKeywordLookup to the
 *      Bison token numbers needed by gram.y.  This is exported because
 *      callers need to pass it to scanner_init, if they are using the
 *      standard keyword list ScanKeywords.
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    contrib/shark/src/backend_parser/scan-tsql-prologue.l.h
 *
 *-------------------------------------------------------------------------
 */
#define PG_KEYWORD(kwname, value, category) value,

const uint16 pgtsql_ScanKeywordTokens[] = {
#include "src/backend_parser/kwlist.h"
};

#undef PG_KEYWORD

/*
 *  The following macro will inject DIALECT_TSQL value
 *  as the first token in the string being parsed.
 *  We use this mechanism to choose different dialects
 *  within the parser.  See the corresponding code
 *  in scanner_init()
 */

#define YY_USER_INIT                            \
	if (g_instance.raw_parser_hook[DB_CMPT_D] && GetSessionContext()->dialect_sql) \
	{                                             \
		GetSessionContext()->dialect_sql = false; \
		*yylloc = 0;                              \
		return DIALECT_TSQL;                       \
	}

/* need to undef to prevent an infinite-loop calling
 * pgtsql_core_yylex(...) inside pgtsql_core_yylex(...)
 */
#undef PG_YYLEX

#undef YY_DECL
#define YY_DECL int pgtsql_core_yylex \
               (YYSTYPE * yylval_param, YYLTYPE * yylloc_param , yyscan_t yyscanner)

extern bool IsTsqlAtatGlobalVar(const char *varname);
static bool IsTsqlTranStmt(const char *haystack, int haystackLen);
