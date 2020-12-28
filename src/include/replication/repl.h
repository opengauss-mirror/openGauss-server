/* ---------------------------------------------------------------------------------------
 * 
 * repl.h
 *        MPPDB High Available internal declarations
 * 
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * 
 * IDENTIFICATION
 *        src/include/replication/repl.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _REPL_H
#define _REPL_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/xlogdefs.h"

typedef void* replication_scanner_yyscan_t;

typedef union replication_scanner_YYSTYPE {
    char* str;
    bool boolval;
    int ival;
    XLogRecPtr recptr;
    Node* node;
    List* list;
    DefElem* defelt;
} replication_scanner_YYSTYPE;

extern int replication_scanner_yylex(
    replication_scanner_YYSTYPE* lvalp, YYLTYPE* llocp, replication_scanner_yyscan_t yyscanner);
extern void replication_scanner_yyerror(const char* message, replication_scanner_yyscan_t yyscanner);

#endif /* _REPLICA_INTERNAL_H */
