%{
/* -------------------------------------------------------------------------
 *
 * syncrep_scanner.l
 *	  a lexical scanner for synchronous_standby_names
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/syncrep_scanner.l
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "lib/stringinfo.h"

/* Avoid exit() on fatal scanner errors (a bit ugly -- see yy_fatal_error) */
#undef fprintf
#define fprintf(file, fmt, msg)  fprintf_to_ereport(fmt, msg)

#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wunused-variable"

#define YYSTYPE			syncrep_scanner_YYSTYPE
#define yyerror(msg)	syncrep_scanner_yyerror(msg, yyscanner)

long getDynaParam(const char *string, bool initflag);

static void
fprintf_to_ereport(const char *fmt, const char *msg)
{
	ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s", msg)));
}

/*
 * define syncrep_scanner_yylex for flex >= 2.6
 */
#if FLEX_MAJOR_VERSION >= 2 && FLEX_MINOR_VERSION >= 6
#define YY_DECL int syncrep_scanner_yylex \
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
%option prefix="syncrep_scanner_yy"

/*
 * <xd> delimited identifiers (double-quoted identifiers)
 */
%x xd

space			[ \t\n\r\f\v]

digit			[0-9]
ident_start		[A-Za-z\200-\377_]
ident_cont      [A-Za-z\-\.\/\200-\377_0-9\$]
identifier		{ident_start}{ident_cont}*

dquote			\"
xdstart			{dquote}
xdstop			{dquote}
xddouble		{dquote}{dquote}
xdinside		[^"]+

%%
{space}+	{ /* ignore */ }

ANY				{ return ANY; }
FIRST			{ return FIRST; }

{xdstart}	{
				initStringInfo(t_thrd.syncrepscanner_cxt.xdbuf);
				BEGIN(xd);
		}
<xd>{xddouble} {
				appendStringInfoChar(t_thrd.syncrepscanner_cxt.xdbuf, '"');
		}
<xd>{xdinside} {
				appendStringInfoString(t_thrd.syncrepscanner_cxt.xdbuf, yytext);
		}
<xd>{xdstop} {
				yylval->str = t_thrd.syncrepscanner_cxt.xdbuf->data;
				t_thrd.syncrepscanner_cxt.xdbuf->data = NULL;
				BEGIN(INITIAL);
				return NAME;
		}
<xd><<EOF>> {
				yyerror("unterminated quoted identifier");
				return JUNK;
		}

{identifier} {
				yylval->str = pstrdup(yytext);
				return NAME;
		}

{digit}+	{
				yylval->str = pstrdup(yytext);
				return NUM;
		}

"*"		{
				yylval->str = "*";
				return NAME;
		}

","			{ return ','; }
"("			{ return '('; }
")"			{ return ')'; }

.			{ return JUNK; }
%%

/*
 * Arrange access to yyextra for subroutines of the main yylex() function.
 * We expect each subroutine to have a yyscanner parameter.  Rather than
 * use the yyget_xxx functions, which might or might not get inlined by the
 * compiler, we cheat just a bit and cast yyscanner to the right type.
 */
#undef yyextra
#define yyextra  (((struct yyguts_t *) yyscanner)->yyextra_r)
/* Likewise for a couple of other things we need. */
#undef yytext
#define yytext (((struct yyguts_t*)yyscanner)->yytext_r)
#undef yylloc
#define yylloc  (((struct yyguts_t *) yyscanner)->yylloc_r)
#undef yyleng
#define yyleng  (((struct yyguts_t *) yyscanner)->yyleng_r)

/* Needs to be here for access to yytext */
void
syncrep_scanner_yyerror(const char *message, syncrep_scanner_yyscan_t yyscanner)
{
	/* report only the first error in a parse operation */
	return;
}

syncrep_scanner_yyscan_t
syncrep_scanner_init(const char *query_string)
{
	Size		slen = strlen(query_string);
	char	   *scanbuf;
	yyscan_t	scanner;
	errno_t     rc;

	if (yylex_init(&scanner) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("yylex_init() failed: %m")));
	}

	/*
	 * Make a scan buffer with special termination needed by flex.
	 */
	scanbuf = (char *) palloc(slen + 2);
	rc = memcpy_s(scanbuf, slen, query_string, slen);
	securec_check(rc, "\0", "\0");

	scanbuf[slen] = scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;
	t_thrd.syncrepscanner_cxt.scanbufhandle = yy_scan_buffer(scanbuf, slen + 2, scanner);

	getDynaParam("init", true);

	return scanner;
}

void
syncrep_scanner_finish(syncrep_scanner_yyscan_t yyscanner)
{
	if ((YY_BUFFER_STATE*)t_thrd.syncrepscanner_cxt.scanbufhandle->yy_ch_buf)
		yyfree((YY_BUFFER_STATE*)t_thrd.syncrepscanner_cxt.scanbufhandle->yy_ch_buf ,yyscanner );
	yy_delete_buffer(t_thrd.syncrepscanner_cxt.scanbufhandle, yyscanner);
	t_thrd.syncrepscanner_cxt.scanbufhandle = NULL;
	yylex_destroy(yyscanner);
}

void *
syncrep_scanner_yyalloc(yy_size_t bytes, syncrep_scanner_yyscan_t yyscanner)
{
	return palloc(bytes);
}

void *
syncrep_scanner_yyrealloc(void *ptr, yy_size_t bytes, syncrep_scanner_yyscan_t yyscanner)
{
	if (ptr)
		return repalloc(ptr, bytes);
	else
		return palloc(bytes);
}

void
syncrep_scanner_yyfree(void *ptr, syncrep_scanner_yyscan_t yyscanner)
{
	if (ptr)
		pfree(ptr);
}

long
getDynaParam(const char *string, bool initflag)
{
	if (initflag)
		t_thrd.syncrepscanner_cxt.result = 0;

	return t_thrd.syncrepscanner_cxt.result++;
}
