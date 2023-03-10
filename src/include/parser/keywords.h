/* -------------------------------------------------------------------------
 *
 * keywords.h
 *	  lexical token lookup for key words in openGauss
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/keywords.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef KEYWORDS_H
#define KEYWORDS_H

/* Keyword categories --- should match lists in gram.y */
#define UNRESERVED_KEYWORD 0
#define COL_NAME_KEYWORD 1
#define TYPE_FUNC_NAME_KEYWORD 2
#define RESERVED_KEYWORD 3

#include "parser/kwlookup.h"
typedef struct PlpgsqlKeywordValue {
    int16 procedure;
    int16 function;
    int16 begin;
    int16 select;
    int16 update;
    int16 insert;
    int16 Delete;
    int16 merge;
} PlpgsqlKeywordValue;

extern PGDLLIMPORT const ScanKeywordList ScanKeywords;
extern PGDLLIMPORT const uint8 ScanKeywordCategories[];

/* Globals from keywords.c */
extern const ScanKeywordList SQLScanKeywords[];

#endif /* KEYWORDS_H */
