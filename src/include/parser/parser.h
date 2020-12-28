/* -------------------------------------------------------------------------
 *
 * parser.h
 *		Definitions for the "raw" parser (flex and bison phases only)
 *
 * This is the external API for the raw lexing/parsing functions.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parser.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PARSER_H
#define PARSER_H

#ifndef FRONTEND_PARSER
#include "nodes/parsenodes.h"
#else
#include "nodes/parsenodes_common.h"
#endif /* FRONTEND_PARSER */

#include "parser/backslash_quotes.h"

#define UPSERT_TO_MERGE_VERSION_NUM 92022

/* Primary entry point for the raw parsing functions */
extern List* raw_parser(const char* str, List** query_string_locationlist = NULL);

#ifdef FRONTEND_PARSER

class PGClientLogic;

extern List *fe_raw_parser(PGClientLogic*, const char *str, List **query_string_locationlist = NULL);
#endif /* FRONTEND_PARSER */

/* Utility functions exported by gram.y (perhaps these should be elsewhere) */
extern List* SystemFuncName(char* name);
extern TypeName* SystemTypeName(char* name);
extern Node* makeBoolAConst(bool state, int location);
extern char** get_next_snippet(
    char** query_string_single, const char* query_string, List* query_string_locationlist, int* stmt_num);

extern void fixResTargetNameWithAlias(List* clause_list, const char* aliasname);

#endif /* PARSER_H */
