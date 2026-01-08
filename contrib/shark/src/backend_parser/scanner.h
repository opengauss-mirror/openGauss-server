/*-------------------------------------------------------------------------
 *
 * scanner.h
 *    API for the core scanner (flex machine)
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Portions Copyright (c) 2015-2017, Ivan Kochurkin (kvanttt@gmail.com), Positive Technologies.
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    contrib/shark/src/backend_parser/scanner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGTSQL_SCANNER_H
#define PGTSQL_SCANNER_H

#include "parser/scanner.h"

extern const uint16 pgtsql_ScanKeywordTokens[];

extern int	pgtsql_core_yylex(core_YYSTYPE *lvalp, YYLTYPE * llocp, core_yyscan_t yyscanner);

core_yyscan_t
			pgtsql_scanner_init(const char *str,
								core_yy_extra_type *yyext,
								const ScanKeywordList *keywordlist,
								const uint16 *keyword_tokens);

void
			pgtsql_scanner_finish(core_yyscan_t yyscanner);

#endif							/* PGTSQL_SCANNER_H */
