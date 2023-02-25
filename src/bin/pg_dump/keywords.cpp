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
 *	  src/bin/pg_dump/keywords.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "parser/keywords.h"

#include "parser/kwlist_d.h"

#define PG_KEYWORD(kwname,value,category) category,

const uint8 ScanKeywordCategories[SCANKEYWORDS_NUM_KEYWORDS] = {
    #include "parser/kwlist.h"
};

#undef PG_KEYWORD

/*
 * We don't need the token number, so leave it out to avoid requiring other
 * backend headers.
 */
