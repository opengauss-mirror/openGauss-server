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
 */

/* -------------------------------------------------------------------------
 *
 * keywords.c
 *	  lexical token lookup for key words in openGauss
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/interfaces/ecpg/preproc/keywords.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres_fe.h"

/*
 * This is much trickier than it looks.  We are #include'ing kwlist.h
 * but the token numbers that go into the table are from preproc.h
 * not the backend's gram.h.  Therefore this token table will match
 * the ScanKeywords table supplied from common/keywords.c, including all
 * keywords known to the backend, but it will supply the token numbers used
 * by ecpg's grammar, which is what we need.  The ecpg grammar must
 * define all the same token names the backend does, else we'll get
 * undefined-symbol failures in this compile.
 */

#include "parser/keywords.h"
#include "type.h"
#include "preproc.hpp"


#define PG_KEYWORD(kwname, value, category) value,

const uint16 SQLScanKeywordTokens[] = {
#include "parser/kwlist.h"
};

#undef PG_KEYWORD
