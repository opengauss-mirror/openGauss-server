/* ---------------------------------------------------------------------------------------
 * 
 * syncrep_gramparse.h
 *		Shared definitions for the "raw" syncrep_parser (flex and bison phases only)
 *
 * NOTE: this file is only meant to be includedd in the core parsing files.
 * 		copy from parser/gramparse.h
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/syncrep_gramparse.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _SYNCREP_GRAMPARSE_H
#define _SYNCREP_GRAMPARSE_H

#include "syncrep.h"
#include "syncrep_gram.hpp"

extern int syncrep_yyparse(syncrep_scanner_yyscan_t yyscanner);
extern int syncrep_yylex(YYSTYPE* lvalp, YYLTYPE* llocp, syncrep_scanner_yyscan_t yyscanner);
extern syncrep_scanner_yyscan_t syncrep_scanner_init(const char* query_string);
extern void syncrep_scanner_finish(syncrep_scanner_yyscan_t yyscanner);

#endif /* _SYNCREP_GRAMPARSE_H */
