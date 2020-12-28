/* ---------------------------------------------------------------------------------------
 * 
 * repl_gramparse.h
 *        Shared definitions for the "raw" syncrep_parser (flex and bison phases only)
 *
 * NOTE: this file is only meant to be includedd in the core parsing files.
 * 		copy from parser/gramparse.h
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/repl_gramparse.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _REPL_GRAMPARSE_H
#define _REPL_GRAMPARSE_H

#include "repl.h"
#include "repl_gram.hpp"

extern int replication_yyparse(replication_scanner_yyscan_t yyscanner);
extern int replication_yylex(YYSTYPE* lvalp, YYLTYPE* llocp, replication_scanner_yyscan_t yyscanner);
extern void replication_yyerror(YYLTYPE* yylloc, replication_scanner_yyscan_t yyscanner, const char* msg);
extern replication_scanner_yyscan_t replication_scanner_init(const char* query_string);
extern void replication_scanner_finish(replication_scanner_yyscan_t yyscanner);
extern void replication_scanner_yyerror(const char* message, replication_scanner_yyscan_t yyscanner);

#endif /* _SYNCREP_GRAMPARSE_H */
