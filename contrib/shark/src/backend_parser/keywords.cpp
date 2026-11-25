/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * --------------------------------------------------------------------------------------
 *
 * keywords.cpp
 *    lexical token lookup for key words in shark
 *
 * Portions Copyright (c) 2020, AWS
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    contrib/shark/src/backend_parser/keywords.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"

#include "parser/keywords.h"


/* ScanKeywordList lookup data for SQL keywords */

#include "src/backend_parser/kwlist_d.h"

/* Keyword categories for SQL keywords */

#define PG_KEYWORD(kwname, value, category, collabel) category,

const uint8 pgtsql_ScanKeywordCategories[PGTSQL_SCANKEYWORDS_NUM_KEYWORDS] = {
#include "src/backend_parser/kwlist.h"
};

#undef PG_KEYWORD

/* Keyword can-be-direct-label flags for SQL keywords */

#define PG_KEYWORD(kwname, value, category, collabel) collabel,

#define DIRECT_LABEL true
#define AS_LABEL false

const bool	ScanKeywordDirectLabel[PGTSQL_SCANKEYWORDS_NUM_KEYWORDS] = {
#include "src/backend_parser/kwlist.h"
};

#undef PG_KEYWORD
#undef DIRECT_LABEL
#undef AS_LABEL