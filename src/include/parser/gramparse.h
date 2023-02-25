/* -------------------------------------------------------------------------
 *
 * gramparse.h
 *		Shared definitions for the "raw" parser (flex and bison phases only)
 *
 * NOTE: this file is only meant to be included in the core parsing files,
 * ie, parser.c, gram.y, scan.l, and keywords.c.  Definitions that are needed
 * outside the core parser should be in parser.h.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/gramparse.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GRAMPARSE_H
#define GRAMPARSE_H

#include "parser/scanner.h"

/*
 * NB: include gram.h only AFTER including scanner.h, because scanner.h
 * is what #defines YYLTYPE.
 */

#ifndef FRONTEND_PARSER
#ifdef PARSE_HINT_h
#include "parser/hint_gram.hpp"
#else
#include "parser/gram.hpp"
#endif
#else
#include "nodes/value.h"
#include "frontend_parser/gram.hpp"
#endif /* FRONTEND_PARSER */

/* When dealing with DECLARE foo CURSOR case, we must look ahead 2 token after DECLARE to meet CURSOR */
#define MAX_LOOKAHEAD_NUM 2

/*
 * The YY_EXTRA data that a flex scanner allows us to pass around.	Private
 * state needed for raw parsing/lexing goes here.
 */
typedef struct base_yy_extra_type {
    /*
     * Fields used by the core scanner.
     */
    core_yy_extra_type core_yy_extra;

    /*
     * State variables for base_yylex().
     */
    int lookahead_num;                                /* lookahead num. Currently can be:0,1,2 */
    int lookahead_token[MAX_LOOKAHEAD_NUM];           /* token lookahead type */
    core_YYSTYPE lookahead_yylval[MAX_LOOKAHEAD_NUM]; /* yylval for lookahead token */
    YYLTYPE lookahead_yylloc[MAX_LOOKAHEAD_NUM];      /* yylloc for lookahead token */

    /*
     * State variables that belong to the grammar.
     */
    List* parsetree; /* final parse result is delivered here */
} base_yy_extra_type;

#ifdef FRONTEND_PARSER
typedef struct fe_base_yy_extra_type {
    /*
     * Fields used by the core scanner.
     */
    fe_core_yy_extra_type core_yy_extra;

    /*
     * State variables for base_yylex().
     */
    int lookahead_num;                                /* lookahead num. Currently can be:0,1,2 */
    int lookahead_token[MAX_LOOKAHEAD_NUM];           /* token lookahead type */
    core_YYSTYPE lookahead_yylval[MAX_LOOKAHEAD_NUM]; /* yylval for lookahead token */
    YYLTYPE lookahead_yylloc[MAX_LOOKAHEAD_NUM];      /* yylloc for lookahead token */

    /*
     * State variables that belong to the grammar.
     */
    List *parsetree; /* final parse result is delivered here */
} fe_base_yy_extra_type;

#endif /* FRONTEND_PARSER */

typedef struct hint_yy_extra_type {
    /*
     * The string the scanner is physically scanning.  We keep this mainly so
     * that we can cheaply compute the offset of the current token (yytext).
     */
    char* scanbuf;
    Size scanbuflen;

    /*
     * The keyword list to use.
     */
	const ScanKeywordList *keywordlist;
	const uint16 *keyword_tokens;

    /*
     * literalbuf is used to accumulate literal values when multiple rules are
     * needed to parse a single literal.  Call startlit() to reset buffer to
     * empty, addlit() to add text.  NOTE: the string in literalbuf is NOT
     * necessarily null-terminated, but there always IS room to add a trailing
     * null at offset literallen.  We store a null only when we need it.
     */
    char* literalbuf; /* palloc'd expandable buffer */
    int literallen;   /* actual current string length */
    int literalalloc; /* current allocated buffer size */

    int xcdepth;     /* depth of nesting in slash-star comments */
    char* dolqstart; /* current $foo$ quote start string */

    /* first part of UTF16 surrogate pair for Unicode escapes */
    int32 utf16_first_part;

    /* state variables for literal-lexing warnings */
    bool warn_on_first_escape;
    bool saw_non_ascii;
    bool ident_quoted;
    bool warnOnTruncateIdent;

    /* record the message need by multi-query. */
    List* query_string_locationlist; /* record the end location of each single query */
    bool in_slash_proc_body;         /* check whether it's in a slash proc body */
    int paren_depth;                 /* record the current depth in	the '(' and ')' */
    bool is_createstmt;              /* check whether it's a create statement. */
    bool is_hint_str;                /* current identifier is in hint comment string */
    List* parameter_list;            /* placeholder parameter list */
} hint_yy_extra_type;

#ifdef PARSE_HINT_h
extern int yylex(YYSTYPE* lvalp, yyscan_t yyscanner);
extern int yyparse(yyscan_t yyscanner);
#else

/*
 * In principle we should use yyget_extra() to fetch the yyextra field
 * from a yyscanner struct.  However, flex always puts that field first,
 * and this is sufficiently performance-critical to make it seem worth
 * cheating a bit to use an inline macro.
 */
#define pg_yyget_extra(yyscanner) (*((base_yy_extra_type**)(yyscanner)))

#ifdef FRONTEND_PARSER
#define fe_pg_yyget_extra(yyscanner) (*((fe_base_yy_extra_type **) (yyscanner)))
#endif /* FRONTEND_PARSER */

/* from parser.c */
extern int base_yylex(YYSTYPE* lvalp, YYLTYPE* llocp, core_yyscan_t yyscanner);

/* from gram.y */
extern void parser_init(base_yy_extra_type* yyext);
extern int base_yyparse(core_yyscan_t yyscanner);

#ifdef FRONTEND_PARSER
extern void fe_parser_init(fe_base_yy_extra_type *yyext);
#endif /* FRONTEND_PARSER */

#endif

#endif /* GRAMPARSE_H */
