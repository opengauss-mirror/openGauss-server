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
#include "c.h"
#include "parser/keywords.h"

/*ScanKeywordList lookup.data.for.SQL.keywords.*/
#include "parser/kwlist_d.h"

#define PG_KEYWORD(kwname,value,category) category,

const uint8 ScanKeywordCategories[SCANKEYWORDS_NUM_KEYWORDS] = {
    #include "parser/kwlist.h"
};

#undef PG_KEYWORD

