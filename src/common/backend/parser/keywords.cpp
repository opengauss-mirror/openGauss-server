/* -------------------------------------------------------------------------
 *
 * keywords.cpp
 *	  lexical token lookup for key words in openGauss
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/parser/keywords.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "nodes/parsenodes.h"
#include "parser/gramparse.h"

#define PG_KEYWORD(a, b, c) {a, b, c},

const ScanKeyword ScanKeywords[] = {
#include "parser/kwlist.h"
};

const int NumScanKeywords = lengthof(ScanKeywords);

