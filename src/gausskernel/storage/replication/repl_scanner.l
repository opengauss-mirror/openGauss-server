%{
/* -------------------------------------------------------------------------
 *
 * repl_scanner.l
 *	  a lexical scanner for the replication commands
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/repl_scanner.l
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "parser/scansup.h"

/* Avoid exit() on fatal scanner errors (a bit ugly -- see yy_fatal_error) */
#undef fprintf
#define fprintf(file, fmt, msg) ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s", msg)));

#undef yylloc
#undef yyerror
#undef yylval

#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wunused-variable"
#define YYSTYPE			replication_scanner_YYSTYPE
#define yyerror(msg)	replication_scanner_yyerror(msg, yyscanner)

static void startlit(void);
static char *litbufdup(void);
static void addlit(char *ytext, int yleng);
static void addlitchar(unsigned char ychar);

/*
 * define replication_scanner_yylex for flex >= 2.6
 */
#if FLEX_MAJOR_VERSION >= 2 && FLEX_MINOR_VERSION >= 6
#define YY_DECL int replication_scanner_yylex \
               (YYSTYPE * yylval_param, YYLTYPE * yylloc_param , yyscan_t yyscanner)
#endif

%}

%option reentrant
%option 8bit
%option bison-bridge bison-locations
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option noyyalloc
%option noyyrealloc
%option noyyfree
%option warn
%option prefix="replication_scanner_yy"

%x xq xd

/* Extended quote
 * xqdouble implements embedded quote, ''''
 */
xqstart			{quote}
xqdouble		{quote}{quote}
xqinside		[^']+

/* Double quote
 * Allows embedded spaces and other special characters into identifiers.
 */
dquote			\"
xdstart			{dquote}
xdstop			{dquote}
xddouble		{dquote}{dquote}
xdinside		[^"]+

hexdigit		[0-9A-Za-z]+
integer			[0-9]+

quote			'
quotestop		{quote}

ident_start		[A-Za-z\200-\377_]
ident_cont		[A-Za-z\200-\377_0-9\$]

identifier		{ident_start}{ident_cont}*

%%

BASE_BACKUP			{ return K_BASE_BACKUP; }
FAST			{ return K_FAST; }
FETCH_MOT_CHECKPOINT	{ return K_FETCH_MOT_CHECKPOINT; }
IDENTIFY_SYSTEM		{ return K_IDENTIFY_SYSTEM; }
IDENTIFY_VERSION	{ return K_IDENTIFY_VERSION; }
IDENTIFY_MODE		{ return K_IDENTIFY_MODE; }
IDENTIFY_MAXLSN		{ return K_IDENTIFY_MAXLSN; }
IDENTIFY_CONSISTENCE	{ return K_IDENTIFY_CONSISTENCE; }
IDENTIFY_CHANNEL	{ return K_IDENTIFY_CHANNEL; }
IDENTIFY_AZ		{ return K_IDENTIFY_AZ; }
LABEL			{ return K_LABEL; }
NOWAIT			{ return K_NOWAIT; }
BUILDSTANDBY            { return K_BUILDSTANDBY; }
COPYSECUREFILE            { return K_COPYSECUREFILE; }
NEEDUPGRADEFILE         { return K_NEEDUPGRADEFILE; }
OBSMODE                 { return K_OBSMODE; }
PROGRESS			{ return K_PROGRESS; }
WAL			{ return K_WAL; }
TABLESPACE_MAP			{ return K_TABLESPACE_MAP; }
DATA		{ return K_DATA; }
START_REPLICATION	{ return K_START_REPLICATION; }
ADVANCE_REPLICATION	{ return K_ADVANCE_REPLICATION; }
CREATE_REPLICATION_SLOT		{ return K_CREATE_REPLICATION_SLOT; }
DROP_REPLICATION_SLOT		{ return K_DROP_REPLICATION_SLOT; }
PHYSICAL			{ return K_PHYSICAL; }
LOGICAL 			{ return K_LOGICAL; }
SLOT				{ return K_SLOT; }

","				{ return ','; }
";"				{ return ';'; }
"("				{ return '('; }
")"				{ return ')'; }

[\n]			;
[\t]			;
" "				;

{hexdigit}+\/{hexdigit}+		{
					uint32 hi,
								lo;
					if (sscanf(yytext, "%X/%X", &hi, &lo) != 2)
						yyerror("invalid streaming start location");
					yylval->recptr = (((uint64) hi) << 32) | lo;
					return RECPTR;
				}

{integer}			{
					long val;
					char *endptr;
					val = strtol(yytext, &endptr, 10);
					if (*endptr != '\0' || errno == ERANGE)
						yyerror("integer too large");
					yylval->ival = val;
					return ICONST;
				}

{xqstart}		{
					BEGIN(xq);
					startlit();
				}
				
<xq>{quotestop}	{
					yyless(1);
					BEGIN(INITIAL);
					yylval->str = litbufdup();
					return SCONST;
				}
<xq>{xqdouble} {
					addlitchar('\'');
				}
<xq>{xqinside}  {
					addlit(yytext, yyleng);
				}

{xdstart}		{
					BEGIN(xd);
					startlit();
				}

<xd>{xdstop}	{
					int len;
					yyless(1);
					BEGIN(INITIAL);
					yylval->str = litbufdup();
					len = strlen(yylval->str);
					truncate_identifier(yylval->str, len, true);
					return IDENT;
				}

<xd>{xdinside}  {
					addlit(yytext, yyleng);
				}

{identifier}	{
					int len = strlen(yytext);

					yylval->str = downcase_truncate_identifier(yytext, len, true);
					return IDENT;
				}

<xq,xd><<EOF>>	{ yyerror("unterminated quoted string"); }


<<EOF>>			{
					yyterminate();
				}

.				{
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("syntax error: unexpected character \"%s\"", yytext)));
				}
%%


static void
startlit(void)
{
	initStringInfo(t_thrd.replscanner_cxt.litbuf);
}

static char *
litbufdup(void)
{
	return t_thrd.replscanner_cxt.litbuf->data;
}

static void
addlit(char *ytext, int yleng)
{
	appendBinaryStringInfo(t_thrd.replscanner_cxt.litbuf, ytext, yleng);
}

static void
addlitchar(unsigned char ychar)
{
	appendStringInfoChar(t_thrd.replscanner_cxt.litbuf, ychar);
}

void
replication_scanner_yyerror(const char *message, replication_scanner_yyscan_t yyscanner)
{
	/* report only the first error in a parse operation */
	return;
}

replication_scanner_yyscan_t
replication_scanner_init(const char *str)
{
	Size		slen = strlen(str);
	char	   *scanbuf;
	yyscan_t	scanner;
	errno_t     rc;

	if (yylex_init(&scanner) != 0) {
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("yyscanner init failed: %m")));
	}

	/*
	 * Make a scan buffer with special termination needed by flex.
	 */
	scanbuf = (char *) palloc(slen + 2);
	rc = memcpy_s(scanbuf, slen, str, slen);
	securec_check(rc, "\0", "\0");

	scanbuf[slen] = scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;
	t_thrd.replscanner_cxt.scanbufhandle = yy_scan_buffer(scanbuf, slen + 2, scanner);

	return scanner;
}

void
replication_scanner_finish(replication_scanner_yyscan_t yyscanner)
{
	yy_delete_buffer(t_thrd.replscanner_cxt.scanbufhandle, yyscanner);
	t_thrd.replscanner_cxt.scanbufhandle = NULL;
	yylex_destroy(yyscanner);
}

void *
replication_scanner_yyalloc(yy_size_t bytes, replication_scanner_yyscan_t yyscanner)
{
    return palloc(bytes);
}

void *
replication_scanner_yyrealloc(void *ptr, yy_size_t bytes, replication_scanner_yyscan_t yyscanner)
{
    if (ptr)
        return repalloc(ptr, bytes);
    else
        return palloc(bytes);
}

void
replication_scanner_yyfree(void *ptr, replication_scanner_yyscan_t yyscanner)
{
    if (ptr)
	{
        pfree(ptr);
		ptr = NULL;
	}
}
