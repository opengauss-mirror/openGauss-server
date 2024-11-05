%{
/*-------------------------------------------------------------------------
 *
 * jsonpath_scan.l
 *	Lexical parser for jsonpath datatype
 *
 * Splits jsonpath string into tokens represented as JsonPathString structs.
 * Decodes unicode and hex escaped strings.
 *
 * Copyright (c) 2019-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/jsonpath_scan.l
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

/*
 * NB: include jsonpath_gram.h only AFTER including jsonpath_internal.h,
 * because jsonpath_internal.h contains the declaration for JsonPathString.
 */
#include "utils/jsonpath.h"

#include "mb/pg_wchar.h"

#include "nodes/pg_list.h"
#include "utils/pl_package.h"
#include "utils/plpgsql.h"

/* Avoid exit() on fatal scanner errors (a bit ugly -- see yy_fatal_error) */
#undef fprintf
#define fprintf(file, fmt, msg)  fprintf_to_ereport(fmt, msg)

#define YY_DECL int jsonpath_yylex \
               (YYSTYPE * yylval_param, YYLTYPE * yylloc_param)
#define SET_YYLLOC()  (*(yylloc))

static StringInfoData scanstring;
static yy_buffer_state* scanbufhandle;
static char* scanbuf;

static void addstring(bool init, char *s, int l);
static void addchar(bool init, char c);
static enum yytokentype checkKeyword(void);
static int	process_integer_literal(const char *token, YYSTYPE *lval);
static void
fprintf_to_ereport(const char *fmt, const char *msg)
{
	ereport(ERROR, (errmsg_internal("%s", msg)));
}

/* LCOV_EXCL_START */

%}

%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option warn
%option prefix="jsonpath_yy"
%option bison-bridge
%option noyyalloc
%option noyyrealloc
%option noyyfree


/*
 * We use exclusive states for quoted and non-quoted strings,
 * quoted variable names and C-style comments.
 * Exclusive states:
 *  <xq> - quoted strings
 */
%x xq
%x xnq
%x xvq

special		[\?\%\$\.\[\]\{\}\(\)\|\&\!\=\<\>\@\#\,\*:\-\+\/]
blank		[ \t\n\r\f]
/* "other" means anything that's not special, blank, or '\' or '"' */
other		[^\?\%\$\.\[\]\{\}\(\)\|\&\!\=\<\>\@\#\,\*:\-\+\/\\\" \t\n\r\f]

digit	[0-9]

/* DecimalInteger in ECMAScript; must not start with 0 unless it's exactly 0 */
integer	(0|[1-9](_?{digit})*)
/* DecimalDigits in ECMAScript; only used as part of other rules */
digits	{digit}(_?{digit})*

decimal		({integer}\.{digits}?|\.{digits})
real		({integer}|{decimal})[Ee][-+]?{digits}
realfail	({integer}|{decimal})[Ee][-+]

integer_junk	{integer}{other}
decimal_junk	{decimal}{other}
real_junk		{real}{other}

%% 
<xnq>{other}+					{
									addstring(false, yytext, yyleng);
								}

<xnq>{blank}+					{
									yylval->str = scanstring.data;
									BEGIN INITIAL;
									return checkKeyword();
								}

<xnq>({special}|\")				{
									yylval->str = scanstring.data;
									yyless(0);
									BEGIN INITIAL;
									return checkKeyword();
								}

<xnq><<EOF>>					{
									yylval->str = scanstring.data;
									BEGIN INITIAL;
									return checkKeyword();
								}

<xnq,xq,xvq>\\b				{ addchar(false, '\b'); }

<xnq,xq,xvq>\\f				{ addchar(false, '\f'); }

<xnq,xq,xvq>\\n				{ addchar(false, '\n'); }

<xnq,xq,xvq>\\r				{ addchar(false, '\r'); }

<xnq,xq,xvq>\\t				{ addchar(false, '\t'); }

<xnq,xq,xvq>\\v				{ addchar(false, '\v'); }

<xnq,xq,xvq>\\.				{ addchar(false, yytext[1]); }

<xnq,xq,xvq>\\				{
							  jsonpath_yyerror("unexpected end after backslash");
							  yyterminate();
							}

<xq,xvq><<EOF>>				{
							  jsonpath_yyerror("unterminated quoted string");
							  yyterminate();
							}

<xq>\"							{
									yylval->str = scanstring.data;
									BEGIN INITIAL;
									return STRING_P;
								}

{special}						{ return *yytext; }

{blank}+						{ /* ignore */ }

{real}							{
									addstring(true, yytext, yyleng);
									addchar(false, '\0');
									yylval->str = scanstring.data;
									return NUMERIC_P;
								}

{decimal}						{
									addstring(true, yytext, yyleng);
									addchar(false, '\0');
									yylval->str = scanstring.data;
									return NUMERIC_P;
								}

{integer}					{
									return process_integer_literal(yytext, yylval);
								}

{realfail}						{
									jsonpath_yyerror("invalid numeric literal");
									yyterminate();
								}
{integer_junk}				{
									jsonpath_yyerror("trailing junk after numeric literal");
									yyterminate();
								}
{decimal_junk}					{
									jsonpath_yyerror("trailing junk after numeric literal");
									yyterminate();
								}
{real_junk}						{
									jsonpath_yyerror("trailing junk after numeric literal");
									yyterminate();
								}
\"								{
									addchar(true, '\0');
									BEGIN xq;
								}

\\								{
									yyless(0);
									addchar(true, '\0');
									BEGIN xnq;
								}

{other}+						{
									addstring(true, yytext, yyleng);
									BEGIN xnq;
								}

<<EOF>>							{
									yyterminate();
								}

%%

/* LCOV_EXCL_STOP */

/*
 * Called before any actual parsing is done
 */
static void
jsonpath_scanner_init(const char *str, int slen)
{
	if (slen <= 0)
		slen = strlen(str);

	/*
	 * Might be left over after ereport()
	 */
	yy_init_globals();

	/*
	 * Make a scan buffer with special termination needed by flex.
	 */

	scanbuf = (char*)palloc(slen + 2);
	memcpy(scanbuf, str, slen);
	scanbuf[slen] = scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;
	scanbufhandle = yy_scan_buffer(scanbuf, slen + 2);

	BEGIN(INITIAL);
}

/*
 * Called after parsing is done to clean up after jsonpath_scanner_init()
 */
static void
jsonpath_scanner_finish(void)
{
	yy_delete_buffer(scanbufhandle);
	pfree(scanbuf);
}

/* Add set of bytes at "s" of length "l" to scanstring */
static void addstring(bool init, char *s, int l)
{
	if (init) {
		initStringInfo(&scanstring);
	}
	appendStringInfo(&scanstring, "%s", s);
}

/* Add single byte "c" to scanstring */
static void addchar(bool init, char c)
{
	if (init) {
		initStringInfo(&scanstring);
	}
	enlargeStringInfo(&scanstring, 1);
	scanstring.data[scanstring.len] = c;
	if (c != '\0')
		scanstring.len++;
}

typedef struct JsonPathKeyword
{
	int16		len;
	bool		lowercase;
	int			val;
	const char *keyword;
} JsonPathKeyword;

static const JsonPathKeyword keywords[] = {
	{ 2, false,	TO_P,		"to"},
	{ 3, false,	ABS_P,		"abs"},
	{ 4, false,	DATE_P,		"date"},
	{ 5, false,	FLOOR_P,	"floor"},
	{ 6, false,	DOUBLE_P,	"double"},
	{ 7, false,	BOOLEAN_P,	"boolean"},
	{ 7, false,	CEILING_P,	"ceiling"}
};

/* Check if current scanstring value is a keyword */
static enum yytokentype checkKeyword()
{
	int			res = IDENT_P;
	int			diff;
	const JsonPathKeyword  *StopLow = keywords,
						   *StopHigh = keywords + lengthof(keywords),
						   *StopMiddle;

	if (scanstring.len > keywords[lengthof(keywords) - 1].len)
		return (yytokentype)res;

	while (StopLow < StopHigh)
	{
		StopMiddle = StopLow + ((StopHigh - StopLow) >> 1);

		if (StopMiddle->len == scanstring.len)
			diff = pg_strncasecmp(StopMiddle->keyword, scanstring.data,
								  scanstring.len);
		else
			diff = StopMiddle->len - scanstring.len;

		if (diff < 0)
			StopLow = StopMiddle + 1;
		else if (diff > 0)
			StopHigh = StopMiddle;
		else
		{
			if (StopMiddle->lowercase)
				diff = strncmp(StopMiddle->keyword, scanstring.data,
							   scanstring.len);

			if (diff == 0)
				res = StopMiddle->val;

			break;
		}
	}

	return (yytokentype)res;
}

static int process_integer_literal(const char *token, YYSTYPE *lval)
{
	long		val;
	char	   *endptr;

	errno = 0;
	val = strtol(token, &endptr, 10);
	if (*endptr != '\0' || errno == ERANGE
#ifdef HAVE_LONG_INT_64
		/* if long > 32 bits, check for overflow of int4 */
		|| val != (long) ((int32) val)
#endif
		)
	{
		/* integer too large, treat it as a float */
		lval->str = pstrdup(token);
		return NUMERIC_P;
	}
	lval->integer = val;
	return INT_P;
}

/* Interface to jsonpath parser */
JsonPathItem * ParseJsonPath(const char *str, int len)
{
	JsonPathItem	*parseresult;

	jsonpath_scanner_init(str, len);

	if (jsonpath_yyparse(&parseresult) != 0)
		jsonpath_yyerror("invalid input"); /* shouldn't happen */

	jsonpath_scanner_finish();

	return parseresult;
}

/*
 * Interface functions to make flex use palloc() instead of malloc().
 * It'd be better to make these static, but flex insists otherwise.
 */

void * jsonpath_yyalloc(yy_size_t bytes)
{
	return palloc(bytes);
}

void * jsonpath_yyrealloc(void *ptr, yy_size_t bytes)
{
	if (ptr)
		return repalloc(ptr, bytes);
	else
		return palloc(bytes);
}

void jsonpath_yyfree(void *ptr)
{
	if (ptr)
		pfree(ptr);
}