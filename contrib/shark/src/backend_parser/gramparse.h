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
 * gramparse.h
 *		Shared definitions for the "raw" parser (flex and bison phases only)
 *
 * NOTE: this file is only meant to be included in the core parsing files,
 * ie, parser.c, gram.y, scan.l, and keywords.c.  Definitions that are needed
 * outside the core parser should be in parser.h.
 *
 * Portions Copyright (c) 2026, Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/shark/src/backend_parser/gramparse.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PGTSQL_GRAMPARSE_H
#define PGTSQL_GRAMPARSE_H

#include "src/backend_parser/scanner.h"

#include "src/backend_parser/gram-backend.hpp"
#include "parser/gramparse.h"

typedef struct base_yy_extra_type pgtsql_base_yy_extra_type;

/* from parser.c */
extern int	pgtsql_base_yylex(YYSTYPE *lvalp, YYLTYPE * llocp,
							  core_yyscan_t yyscanner);

/* from pgtsql_gram.y */
extern void pgtsql_parser_init(pgtsql_base_yy_extra_type *yyext);
extern int	pgtsql_base_yyparse(core_yyscan_t yyscanner);
extern int	pgtsql_base_yydebug;

#endif /* PGTSQL_GRAMPARSE_H */
