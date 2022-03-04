%{
#include "parser/parse_hint.h"
#include "parser/scansup.h"
#include "utils/memutils.h"
#include "parser/parser.h"				/* only needed for GUC variables */
#include "mb/pg_wchar.h"


#include "parser/gramparse.h"

#undef yyextra
#define yyextra  (((struct yyguts_t *) yyscanner)->yyextra_r)

/* Likewise for a couple of other things we need. */
#undef yylloc
#define yylloc  (((struct yyguts_t *) yyscanner)->yylloc_r)
#undef yyleng
#define yyleng  (((struct yyguts_t *) yyscanner)->yyleng_r)

#define startlit()  ( yyextra->literallen = 0 )

#define YY_EXTRA_TYPE hint_yy_extra_type *

static const hintKeyword parsers[] =
{
	{HINT_NESTLOOP, NestLoop_P},
	{HINT_MERGEJOIN, MergeJoin_P},
	{HINT_HASHJOIN, HashJoin_P},
	{HINT_NO, No_P},
	{HINT_LEADING, Leading_P},
	{HINT_ROWS, Rows_P},
	{HINT_BROADCAST, Broadcast_P},
	{HINT_REDISTRIBUTE, Redistribute_P},
	{HINT_BLOCKNAME, BlockName_P},
	{HINT_TABLESCAN, TableScan_P},
	{HINT_INDEXSCAN, IndexScan_P},
	{HINT_INDEXONLYSCAN, IndexOnlyScan_P},
	{HINT_SKEW, Skew_P},
	{HINT_MULTI_NODE, HINT_MULTI_NODE_P},
	{HINT_NULL, NULL_P},
	{HINT_TRUE, TRUE_P},
	{HINT_FALSE, FALSE_P},
	{HINT_PRED_PUSH, Predpush_P},
	{HINT_PRED_PUSH_SAME_LEVEL, PredpushSameLevel_P},
	{HINT_REWRITE, Rewrite_P},
	{HINT_GATHER, Gather_P},
	{HINT_SET, Set_P},
	{HINT_CPLAN, USE_CPLAN_P},
	{HINT_GPLAN, USE_GPLAN_P},
	{HINT_NO_EXPAND, No_expand_P},
	{HINT_NO_GPC, NO_GPC_P},
};

static const hintKeyword* HintKeywordLookup(const char *str);
static int process_integer_literal(const char *token, YYSTYPE *lval);
static char * litbufdup(yyscan_t yyscanner);
static void addlit(const char *ytext, int yleng, yyscan_t yyscanner);
static void addlitchar(unsigned char ychar, yyscan_t yyscanner);
extern void hint_scanner_yyerror(const char *msg, yyscan_t yyscanner);
static char *litbuf_udeescape(unsigned char escape, yyscan_t yyscanner);
static unsigned int hexval(unsigned char c, yyscan_t yyscanner);
static unsigned char unescape_single_char(unsigned char c, yyscan_t yyscanner);
static bool is_utf16_surrogate_first(pg_wchar c);
static bool is_utf16_surrogate_second(pg_wchar c);
static pg_wchar surrogate_pair_to_codepoint(pg_wchar first, pg_wchar second);
static void addunicode(pg_wchar c, yyscan_t yyscanner);
static void check_string_escape_warning(unsigned char ychar, yyscan_t yyscanner);
static void check_escape_warning(yyscan_t yyscanner);
static void check_unicode_value(pg_wchar c, char *loc, yyscan_t yyscanner);

%}

%option 8bit
%option noyywrap
%option noyyalloc
%option noyyrealloc
%option noyyfree
%option reentrant
%option bison-bridge
%option noinput
%option nounput
%option never-interactive
%option warn
%option yylineno

/*s
 * OK, here is a short description of lex/flex rules behavior.
 * The longest pattern which matches an input string is always chosen.
 * For equal-length patterns, the first occurring in the rules list is chosen.
 * INITIAL is the starting state, to which all non-conditional rules apply.
 * Exclusive states change parsing rules while the state is active.  When in
 * an exclusive state, only those rules defined for that state apply.
 *
 * We use exclusive states for quoted strings, extended comments,
 * and to eliminate parsing troubles for numeric strings.
 * Exclusive states:
 *  <xb> bit string literal
 *  <xc> extended C-style comments
 *  <xd> delimited identifiers (double-quoted identifiers)
 *  <xh> hexadecimal numeric string
 *  <xq> standard quoted strings
 *  <xe> extended quoted strings (support backslash escape sequences)
 *  <xdolq> $foo$ quoted strings
 *  <xui> quoted identifier with Unicode escapes
 *  <xus> quoted string with Unicode escapes
 *  <xeu> Unicode surrogate pair in extended quoted string
 */

%x xb
%x xd
%x xh
%x xe
%x xq
%x xdolq
%x xui
%x xus
%x xeu

digit			[0-9]
ident_start		[A-Za-z\200-\377_]
ident_cont		[A-Za-z\200-\377_0-9\$\#]

identifier      {ident_start}{ident_cont}*
integer			{digit}+
decimal		(({digit}*\.{digit}+)|({digit}+\.{digit}*))
decimalfail 	{digit}+\.\.
real			({integer}|{decimal})[Ee][-+]?{digit}+
realfail1		({integer}|{decimal})[Ee]
realfail2		({integer}|{decimal})[Ee][-+]

space			[ \t\n\r\f]
self			[,()\[\].';\:\+\-\*\/\\\%\^\<\>\=\~\!\@\#\^\&\|\`\?]

horiz_space		[ \t\f]
newline			[\n\r]
non_newline		[^\n\r]

comment			("--"{non_newline}*)

whitespace		({space}+|{comment})

/*
 * SQL requires at least one newline in the whitespace separating
 * string literals that are to be concatenated.  Silly, but who are we
 * to argue?  Note that {whitespace_with_newline} should not have * after
 * it, whereas {whitespace} should generally have a * after it...
 */

special_whitespace		({space}+|{comment}{newline})
horiz_whitespace		({horiz_space}|{comment})
whitespace_with_newline	({horiz_whitespace}*{newline}{special_whitespace}*)

/*
 * To ensure that {quotecontinue} can be scanned without having to back up
 * if the full pattern isn't matched, we include trailing whitespace in
 * {quotestop}.  This matches all cases where {quotecontinue} fails to match,
 * except for {quote} followed by whitespace and just one "-" (not two,
 * which would start a {comment}).  To cover that we have {quotefail}.
 * The actions for {quotestop} and {quotefail} must throw back characters
 * beyond the quote proper.
 */
quote			'
quotestop		{quote}{whitespace}*
quotecontinue	{quote}{whitespace_with_newline}{quote}
quotefail		{quote}{whitespace}*"-"

/* Bit string
 * It is tempting to scan the string for only those characters
 * which are allowed. However, this leads to silently swallowed
 * characters if illegal characters are included in the string.
 * For example, if xbinside is [01] then B'ABCD' is interpreted
 * as a zero-length string, and the ABCD' is lost!
 * Better to pass the string forward and let the input routines
 * validate the contents.
 */
xbstart			[bB]{quote}
xbinside		[^']*

/* Hexadecimal number */
xhstart			[xX]{quote}
xhinside		[^']*

/* National character */
xnstart			[nN]{quote}

/* Quoted string that allows backslash escapes */
xestart			[eE]{quote}
xeinside		[^\\']+
xeescape		[\\][^0-7]
xeoctesc		[\\][0-7]{1,3}
xehexesc		[\\]x[0-9A-Fa-f]{1,2}
xeunicode		[\\](u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})
xeunicodefail	[\\](u[0-9A-Fa-f]{0,3}|U[0-9A-Fa-f]{0,7})

/* Extended quote
 * xqdouble implements embedded quote, ''''
 */
xqstart			{quote}
xqdouble		{quote}{quote}
xqinside		[^']+

/* $foo$ style quotes ("dollar quoting")
 * The quoted string starts with $foo$ where "foo" is an optional string
 * in the form of an identifier, except that it may not contain "$",
 * and extends to the first occurrence of an identical string.
 * There is *no* processing of the quoted text.
 *
 * {dolqfailed} is an error rule to avoid scanner backup when {dolqdelim}
 * fails to match its trailing "$".
 */
dolq_start		[A-Za-z\200-\377_]
dolq_cont		[A-Za-z\200-\377_0-9]
dolqdelim		\$({dolq_start}{dolq_cont}*)?\$
dolqfailed		\${dolq_start}{dolq_cont}*
dolqinside		[^$]+

/* Double quote
 * Allows embedded spaces and other special characters into identifiers.
 */
dquote			\"
xdstart			{dquote}
xdstop			{dquote}
xddouble		{dquote}{dquote}
xdinside		[^"]+

/* Unicode escapes */
uescape			[uU][eE][sS][cC][aA][pP][eE]{whitespace}*{quote}[^']{quote}
/* error rule to avoid backup */
uescapefail		("-"|[uU][eE][sS][cC][aA][pP][eE]{whitespace}*"-"|[uU][eE][sS][cC][aA][pP][eE]{whitespace}*{quote}[^']|[uU][eE][sS][cC][aA][pP][eE]{whitespace}*{quote}|[uU][eE][sS][cC][aA][pP][eE]{whitespace}*|[uU][eE][sS][cC][aA][pP]|[uU][eE][sS][cC][aA]|[uU][eE][sS][cC]|[uU][eE][sS]|[uU][eE]|[uU])

/* Quoted identifier with Unicode escapes */
xuistart		[uU]&{dquote}
xuistop1		{dquote}{whitespace}*{uescapefail}?
xuistop2		{dquote}{whitespace}*{uescape}

/* Quoted string with Unicode escapes */
xusstart		[uU]&{quote}
xusstop1		{quote}{whitespace}*{uescapefail}?
xusstop2		{quote}{whitespace}*{uescape}

/* error rule to avoid backup */
xufailed		[uU]&

%%
{whitespace}	{
					/* ignore */
				}
{xbstart}		{
					/* Binary bit type.
					 * At some point we should simply pass the string
					 * forward to the parser and label it there.
					 * In the meantime, place a leading "b" on the string
					 * to mark it for the input routine as a binary string.
					 */
					BEGIN(xb);
					startlit();
					addlitchar('b', yyscanner);
				}
<xb>{quotestop}	|
<xb>{quotefail} {
					yyless(1);
					BEGIN(INITIAL);
					yylval->str = litbufdup(yyscanner);
					yyextra->is_hint_str = true;
					return BCONST;
				}
<xh>{xhinside}	|
<xb>{xbinside}	{
					addlit(yytext, yyleng, yyscanner);
				}
<xh>{quotecontinue}	|
<xb>{quotecontinue}	{
					/* ignore */
				}
<xb><<EOF>>		{ hint_scanner_yyerror("unterminated bit string literal", yyscanner);}

{xhstart}		{
					/* Hexadecimal bit type.
					 * At some point we should simply pass the string
					 * forward to the parser and label it there.
					 * In the meantime, place a leading "x" on the string
					 * to mark it for the input routine as a hex string.
					 */
					BEGIN(xh);
					startlit();
					addlitchar('x', yyscanner);
				}
<xh>{quotestop}	|
<xh>{quotefail} {
					yyless(1);
					BEGIN(INITIAL);
					yylval->str = litbufdup(yyscanner);
					yyextra->is_hint_str = true;
					return XCONST;
				}
<xh><<EOF>>		{ hint_scanner_yyerror("unterminated hexadecimal string literal", yyscanner);}

{xnstart}		{
					/* National character.
					 * We will pass this along as a normal character string,
					 * but preceded with an internally-generated "NCHAR".
					 */
					const ScanKeyword *keyword;

					yyless(1);				/* eat only 'n' this time */

					keyword = ScanKeywordLookup("nchar",
												yyextra->keywords,
												yyextra->num_keywords);
					if (keyword != NULL)
					{
						yyextra->is_hint_str = true;
						return keyword->value;
					}
					else
					{
						/* If NCHAR isn't a keyword, just return "n" */
						yylval->str = pstrdup("n");
						yyextra->ident_quoted = false;
						yyextra->is_hint_str = true;
						return IDENT;
					}
				}

{xqstart}		{
					yyextra->warn_on_first_escape = true;
					yyextra->saw_non_ascii = false;
					if (u_sess->attr.attr_sql.standard_conforming_strings)
						BEGIN(xq);
					else
						BEGIN(xe);
					startlit();
				}
{xestart}		{
					yyextra->warn_on_first_escape = false;
					yyextra->saw_non_ascii = false;
					BEGIN(xe);
					startlit();
				}
{xusstart}		{
					if (!u_sess->attr.attr_sql.standard_conforming_strings)
						hint_scanner_yyerror("unsafe use of string constant with Unicode escapes. String constants with Unicode escapes cannot be used when standard_conforming_strings is off.", yyscanner);

					BEGIN(xus);
					startlit();
				}
<xq,xe>{quotestop}	|
<xq,xe>{quotefail} {
					yyless(1);
					BEGIN(INITIAL);
					/*
					 * check that the data remains valid if it might have been
					 * made invalid by unescaping any chars.
					 */
					if (yyextra->saw_non_ascii)
						pg_verifymbstr(yyextra->literalbuf,
									   yyextra->literallen,
									   false);
					yylval->str = litbufdup(yyscanner);
					yyextra->is_hint_str = true;
					return SCONST;
				}
<xus>{xusstop1} {
					/* throw back all but the quote */
					yyless(1);
					BEGIN(INITIAL);
					yylval->str = litbuf_udeescape('\\', yyscanner);
					yyextra->is_hint_str = true;
					return SCONST;
		}
<xus>{xusstop2} {
					BEGIN(INITIAL);
					yylval->str = litbuf_udeescape(yytext[yyleng-2], yyscanner);
					yyextra->is_hint_str = true;
					return SCONST;
		}
<xq,xe,xus>{xqdouble} {
					addlitchar('\'', yyscanner);
				}
<xq,xus>{xqinside}  {
					addlit(yytext, yyleng, yyscanner);
				}
<xe>{xeinside}  {
					addlit(yytext, yyleng, yyscanner);
				}
<xe>{xeunicode} {
					pg_wchar c = strtoul(yytext+2, NULL, 16);

					check_escape_warning(yyscanner);

					if (is_utf16_surrogate_first(c))
					{
						yyextra->utf16_first_part = c;
						BEGIN(xeu);
					}
					else if (is_utf16_surrogate_second(c))
						hint_scanner_yyerror("invalid Unicode surrogate pair", yyscanner);
					else
						addunicode(c, yyscanner);
				}
<xeu>{xeunicode} {
					pg_wchar c = strtoul(yytext+2, NULL, 16);

					if (!is_utf16_surrogate_second(c))
						hint_scanner_yyerror("invalid Unicode surrogate pair", yyscanner);

					c = surrogate_pair_to_codepoint(yyextra->utf16_first_part, c);

					addunicode(c, yyscanner);

					BEGIN(xe);
				}
<xeu>.			{ hint_scanner_yyerror("invalid Unicode surrogate pair", yyscanner); }
<xeu>\n			{ hint_scanner_yyerror("invalid Unicode surrogate pair", yyscanner); }
<xeu><<EOF>>	{ hint_scanner_yyerror("invalid Unicode surrogate pair", yyscanner); }
<xe,xeu>{xeunicodefail}	{
						hint_scanner_yyerror("invalid Unicode escape. Unicode escapes must be \\uXXXX or \\UXXXXXXXX.", yyscanner);
				}
<xe>{xeescape}  {
					if (yytext[1] == '\'')
					{
						if (u_sess->attr.attr_sql.backslash_quote == BACKSLASH_QUOTE_OFF ||
							(u_sess->attr.attr_sql.backslash_quote == BACKSLASH_QUOTE_SAFE_ENCODING &&
							 PG_ENCODING_IS_CLIENT_ONLY(pg_get_client_encoding())))
							hint_scanner_yyerror("unsafe use of \\' in a string literal. Use '' to write quotes in strings. \\' is insecure in client-only encodings.", yyscanner);
					}
					check_string_escape_warning(yytext[1], yyscanner);
					addlitchar(unescape_single_char(yytext[1], yyscanner),
							   yyscanner);
				}
<xe>{xeoctesc}  {
					unsigned char c = strtoul(yytext+1, NULL, 8);

					check_escape_warning(yyscanner);
					addlitchar(c, yyscanner);
					if (c == '\0' || IS_HIGHBIT_SET(c))
						yyextra->saw_non_ascii = true;
				}
<xe>{xehexesc}  {
					unsigned char c = strtoul(yytext+2, NULL, 16);

					check_escape_warning(yyscanner);
					addlitchar(c, yyscanner);
					if (c == '\0' || IS_HIGHBIT_SET(c))
						yyextra->saw_non_ascii = true;
				}
<xq,xe,xus>{quotecontinue} {
					/* ignore */
				}
<xe>.			{
					/* This is only needed for \ just before EOF */
					addlitchar(yytext[0], yyscanner);
				}
<xq,xe,xus><<EOF>>		{ hint_scanner_yyerror("unterminated quoted string", yyscanner); }

{dolqdelim}		{
					yyextra->dolqstart = pstrdup(yytext);
					BEGIN(xdolq);
					startlit();
				}
{dolqfailed}	{
					/* throw back all but the initial "$" */
					yyless(1);
					/* and treat it as {other} */
					yyextra->is_hint_str = true;
					return yytext[0];
				}
<xdolq>{dolqdelim} {
					if (strcmp(yytext, yyextra->dolqstart) == 0)
					{
						pfree(yyextra->dolqstart);
						yyextra->dolqstart = NULL;
						BEGIN(INITIAL);
						yylval->str = litbufdup(yyscanner);
						yyextra->is_hint_str = true;
						return SCONST;
					}
					else
					{
						/*
						 * When we fail to match $...$ to dolqstart, transfer
						 * the $... part to the output, but put back the final
						 * $ for rescanning.  Consider $delim$...$junk$delim$
						 */
						addlit(yytext, yyleng-1, yyscanner);
						yyless(yyleng-1);
					}
				}
<xdolq>{dolqinside} {
					addlit(yytext, yyleng, yyscanner);
				}
<xdolq>{dolqfailed} {
					addlit(yytext, yyleng, yyscanner);
				}
<xdolq>.		{
					/* This is only needed for $ inside the quoted text */
					addlitchar(yytext[0], yyscanner);
				}
<xdolq><<EOF>>	{ hint_scanner_yyerror("unterminated dollar-quoted string", yyscanner); }

{xdstart}		{
					BEGIN(xd);
					startlit();
				}
{xuistart}		{
					BEGIN(xui);
					startlit();
				}
<xd>{xdstop}	{
					char		   *ident;

					BEGIN(INITIAL);
					if (yyextra->literallen == 0)
						hint_scanner_yyerror("zero-length delimited identifier", yyscanner);
					ident = litbufdup(yyscanner);
					if (yyextra->literallen >= NAMEDATALEN)
						truncate_identifier(ident, yyextra->literallen, yyextra->warnOnTruncateIdent);
					yylval->str = ident;
					yyextra->ident_quoted = true;
					yyextra->is_hint_str = true;
					return IDENT;
				}
<xui>{xuistop1}	{
					char		   *ident;
					int             identlen;

					BEGIN(INITIAL);
					if (yyextra->literallen == 0)
						hint_scanner_yyerror("zero-length delimited identifier", yyscanner);
					ident = litbuf_udeescape('\\', yyscanner);
					identlen = strlen(ident);
					if (identlen >= NAMEDATALEN)
						truncate_identifier(ident, identlen, yyextra->warnOnTruncateIdent);
					yylval->str = ident;
					/* throw back all but the quote */
					yyless(1);
					yyextra->ident_quoted = false;
					yyextra->is_hint_str = true;
					return IDENT;
				}
<xui>{xuistop2}	{
					char		   *ident;
					int             identlen;

					BEGIN(INITIAL);
					if (yyextra->literallen == 0)
						hint_scanner_yyerror("zero-length delimited identifier", yyscanner);
					ident = litbuf_udeescape(yytext[yyleng - 2], yyscanner);
					identlen = strlen(ident);
					if (identlen >= NAMEDATALEN)
						truncate_identifier(ident, identlen, yyextra->warnOnTruncateIdent);
					yylval->str = ident;
					yyextra->ident_quoted = false;
					yyextra->is_hint_str = true;
					return IDENT;
				}
<xd,xui>{xddouble}	{
					addlitchar('"', yyscanner);
				}
<xd,xui>{xdinside}	{
					addlit(yytext, yyleng, yyscanner);
				}
<xd,xui><<EOF>>		{ hint_scanner_yyerror("unterminated quoted identifier", yyscanner); }

{xufailed}	{
					char		   *ident;

					/* throw back all but the initial u/U */
					yyless(1);
					/* and treat it as {identifier} */
					ident = downcase_truncate_identifier(yytext, yyleng, yyextra->warnOnTruncateIdent);
					yylval->str = ident;
					yyextra->ident_quoted = false;
					yyextra->is_hint_str = true;
					return IDENT;
				}
{identifier}  {
			const hintKeyword *keyword = NULL;
			char             *ident;


			keyword = HintKeywordLookup(yytext);

			if (keyword != NULL)
			{
				return keyword->value;
			}
			else
			{
				ident = downcase_truncate_identifier(yytext, yyleng, true);
				yylval->str = ident;
				return IDENT;
			}
	      }
{integer}		{
			return process_integer_literal(yytext, yylval);
	      	}
{decimal}		{
					yylval->str = pstrdup(yytext);
					return FCONST;
				}
{decimalfail}	{
					/* throw back the .., and treat as integer */
					yyless(yyleng-2);
					yyextra->is_hint_str = true;
					return process_integer_literal(yytext, yylval);
				}
{real}			{
					yylval->str = pstrdup(yytext);
					return FCONST;
				}
{realfail1}		{
					/*
					 * throw back the [Ee], and treat as {decimal}.  Note
					 * that it is possible the input is actually {integer},
					 * but since this case will almost certainly lead to a
					 * syntax error anyway, we don't bother to distinguish.
					 */
					yyless(yyleng-1);
					yylval->str = pstrdup(yytext);
					return FCONST;
				}
{realfail2}		{
					/* throw back the [Ee][+-], and proceed as above */
					yyless(yyleng-2);
					yylval->str = pstrdup(yytext);
					return FCONST;
				}
{self}		{
			return yytext[0];
		}

%%

void *
yyalloc(yy_size_t bytes, yyscan_t yyscanner)
{
	return palloc(bytes);
}

void *
yyrealloc(void *ptr, yy_size_t bytes, yyscan_t yyscanner)
{
	if (ptr)
		return repalloc(ptr, bytes);
	else
		return palloc(bytes);
}

void
yyfree(void *ptr, yyscan_t yyscanner)
{
	if (ptr)
		pfree(ptr);
}


yyscan_t
hint_scanner_init(const char *str, hint_yy_extra_type *yyext)
{
	size_t		slen = strlen(str);
	yyscan_t	scanner;
	errno_t errorno = EOK;


	if (yylex_init(&scanner) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("yylex_init() failed: %m")));

	yyset_extra(yyext, scanner);

	/*
	 * Make a scan buffer with special termination needed by flex.
	 */
	yyext->scanbuf = (char *) palloc(slen + 2);
	yyext->scanbuflen = slen;
	errorno = memcpy_s(yyext->scanbuf, slen+2, str, slen);
	securec_check_c(errorno, "\0", "\0");

	yyext->scanbuf[slen] = yyext->scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;
	yy_scan_buffer(yyext->scanbuf,slen + 2,scanner);

	/* initialize literal buffer to a reasonable but expansible size */
	yyext->literalalloc = 1024;
	yyext->literalbuf = (char *) palloc(yyext->literalalloc);
	yyext->literallen = 0;

	return scanner;

}

static const hintKeyword*
HintKeywordLookup(const char *str)
{
	const hintKeyword* start = parsers;

	for (int i = 0; i < (int) lengthof(parsers); i++, start++)	
	{
		if (strcasecmp(start->name, str) == 0)
		{
			return start;
		}
	}

	return NULL;
}

void
hint_scanner_destroy(yyscan_t yyscanner)
 {
	/* copied from scanner_finish */
	pfree(yyextra->scanbuf);
	pfree(yyextra->literalbuf);
	yylex_destroy(yyscanner);
}

void
hint_scanner_yyerror(const char *msg, yyscan_t yyscanner)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "LINE %d: %s at '%s'", yyget_lineno(yyscanner), msg, yyget_text(yyscanner));
	u_sess->parser_cxt.hint_warning = lappend(u_sess->parser_cxt.hint_warning, makeString(buf.data));

	return;
}

static int
process_integer_literal(const char *token, YYSTYPE *lval)
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
		return FCONST;
	}
	lval->ival = val;
	return ICONST;
}


/*
 * Create a palloc'd copy of literalbuf, adding a trailing null.
 */
static char *
litbufdup(yyscan_t yyscanner)
{
	int			llen = yyextra->literallen;
	char	   *newm;

	newm = (char *)palloc(llen + 1);
	memcpy(newm, yyextra->literalbuf, llen);
	newm[llen] = '\0';
	return newm;
}


static void
addlit(const char *ytext, int yleng, yyscan_t yyscanner)
{
	/* enlarge buffer if needed */
	if ((yyextra->literallen + yleng) >= yyextra->literalalloc)
	{
		do
		{
			yyextra->literalalloc *= 2;
		} while ((yyextra->literallen + yleng) >= yyextra->literalalloc);

		/*when yytext is larger than 512M, its double will exceed 1G, so we use repalloc_huge */
		yyextra->literalbuf = (char *) repalloc_huge(yyextra->literalbuf,
												yyextra->literalalloc);
	}
	/* append new data */
	memcpy(yyextra->literalbuf + yyextra->literallen, ytext, yleng);
	yyextra->literallen += yleng;
}


static void
addlitchar(unsigned char ychar, yyscan_t yyscanner)
{
	/* enlarge buffer if needed */
	if ((yyextra->literallen + 1) >= yyextra->literalalloc)
	{
		yyextra->literalalloc *= 2;
		yyextra->literalbuf = (char *) repalloc(yyextra->literalbuf,
												yyextra->literalalloc);
	}
	/* append new data */
	yyextra->literalbuf[yyextra->literallen] = ychar;
	yyextra->literallen += 1;
}

static unsigned int
hexval(unsigned char c, yyscan_t yyscanner)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 0xA;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 0xA;

	hint_scanner_yyerror("invalid hexadecimal digit", yyscanner);

	return 0; /* not reached */
}

static void
check_unicode_value(pg_wchar c, char *loc, yyscan_t yyscanner)
{
	if (GetDatabaseEncoding() == PG_UTF8)
		return;

	if (c > 0x7F)
	{
		hint_scanner_yyerror("Unicode escape values cannot be used for code point values above 007F when the server encoding is not UTF8", yyscanner);
	}
}

static bool
is_utf16_surrogate_first(pg_wchar c)
{
	return (c >= 0xD800 && c <= 0xDBFF);
}

static bool
is_utf16_surrogate_second(pg_wchar c)
{
	return (c >= 0xDC00 && c <= 0xDFFF);
}

static pg_wchar
surrogate_pair_to_codepoint(pg_wchar first, pg_wchar second)
{
	return ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF);
}

static void
addunicode(pg_wchar c, yyscan_t yyscanner)
{
	char buf[8];

	if (c == 0 || c > 0x10FFFF)
		hint_scanner_yyerror("invalid Unicode escape value", yyscanner);
	if (c > 0x7F)
	{
		if (GetDatabaseEncoding() != PG_UTF8)
			hint_scanner_yyerror("Unicode escape values cannot be used for code point values above 007F when the server encoding is not UTF8", yyscanner);
		yyextra->saw_non_ascii = true;
	}
	unicode_to_utf8(c, (unsigned char *) buf);
	addlit(buf, pg_mblen(buf), yyscanner);
}

static char *
litbuf_udeescape(unsigned char escape, yyscan_t yyscanner)
{
	char *newm;
	char *litbuf, *in, *out;
	pg_wchar pair_first = 0;

	if (isxdigit(escape)
		|| escape == '+'
		|| escape == '\''
		|| escape == '"'
		|| scanner_isspace(escape))
	{
		hint_scanner_yyerror("invalid Unicode escape character", yyscanner);
	}

	/* Make literalbuf null-terminated to simplify the scanning loop */
	litbuf = yyextra->literalbuf;
	litbuf[yyextra->literallen] = '\0';

	/*
	 * This relies on the subtle assumption that a UTF-8 expansion
	 * cannot be longer than its escaped representation.
	 */
	newm = (char *)palloc(yyextra->literallen + 1);

	in = litbuf;
	out = newm;
	while (*in)
	{
		if (in[0] == escape)
		{
			if (in[1] == escape)
			{
				if (pair_first)
				{
					hint_scanner_yyerror("invalid Unicode surrogate pair", yyscanner);
				}
				*out++ = escape;
				in += 2;
			}
			else if (isxdigit((unsigned char) in[1]) &&
					 isxdigit((unsigned char) in[2]) &&
					 isxdigit((unsigned char) in[3]) &&
					 isxdigit((unsigned char) in[4]))
			{
				pg_wchar unicode;

				unicode = (hexval(in[1], yyscanner) << 12) +
					(hexval(in[2], yyscanner) << 8) +
					(hexval(in[3], yyscanner) << 4) +
					hexval(in[4], yyscanner);
				check_unicode_value(unicode, in, yyscanner);
				if (pair_first)
				{
					if (is_utf16_surrogate_second(unicode))
					{
						unicode = surrogate_pair_to_codepoint(pair_first, unicode);
						pair_first = 0;
					}
					else
					{
						hint_scanner_yyerror("invalid Unicode surrogate pair", yyscanner);
					}
				}
				else if (is_utf16_surrogate_second(unicode))
					hint_scanner_yyerror("invalid Unicode surrogate pair", yyscanner);

				if (is_utf16_surrogate_first(unicode))
					pair_first = unicode;
				else
				{
					unicode_to_utf8(unicode, (unsigned char *) out);
					out += pg_mblen(out);
				}
				in += 5;
			}
			else if (in[1] == '+' &&
					 isxdigit((unsigned char) in[2]) &&
					 isxdigit((unsigned char) in[3]) &&
					 isxdigit((unsigned char) in[4]) &&
					 isxdigit((unsigned char) in[5]) &&
					 isxdigit((unsigned char) in[6]) &&
					 isxdigit((unsigned char) in[7]))
			{
				pg_wchar unicode;

				unicode = (hexval(in[2], yyscanner) << 20) +
					(hexval(in[3], yyscanner) << 16) +
					(hexval(in[4], yyscanner) << 12) +
					(hexval(in[5], yyscanner) << 8) +
					(hexval(in[6], yyscanner) << 4) +
					hexval(in[7], yyscanner);
				check_unicode_value(unicode, in, yyscanner);
				if (pair_first)
				{
					if (is_utf16_surrogate_second(unicode))
					{
						unicode = surrogate_pair_to_codepoint(pair_first, unicode);
						pair_first = 0;
					}
					else
					{
						hint_scanner_yyerror("invalid Unicode surrogate pair", yyscanner);
					}
				}
				else if (is_utf16_surrogate_second(unicode))
					hint_scanner_yyerror("invalid Unicode surrogate pair", yyscanner);

				if (is_utf16_surrogate_first(unicode))
					pair_first = unicode;
				else
				{
					unicode_to_utf8(unicode, (unsigned char *) out);
					out += pg_mblen(out);
				}
				in += 8;
			}
			else
			{
				hint_scanner_yyerror("invalid Unicode escape value", yyscanner);
			}
		}
		else
		{
			if (pair_first)
			{
				hint_scanner_yyerror("invalid Unicode surrogate pair", yyscanner);
			}
			*out++ = *in++;
		}
	}

	/* unfinished surrogate pair? */
	if (pair_first)
	{
		hint_scanner_yyerror("invalid Unicode surrogate pair", yyscanner);
	}

	*out = '\0';
	/*
	 * We could skip pg_verifymbstr if we didn't process any non-7-bit-ASCII
	 * codes; but it's probably not worth the trouble, since this isn't
	 * likely to be a performance-critical path.
	 */
	pg_verifymbstr(newm, out - newm, false);
	return newm;
}

static unsigned char
unescape_single_char(unsigned char c, yyscan_t yyscanner)
{
	switch (c)
	{
		case 'b':
			return '\b';
		case 'f':
			return '\f';
		case 'n':
			return '\n';
		case 'r':
			return '\r';
		case 't':
			return '\t';
		default:
			/* check for backslash followed by non-7-bit-ASCII */
			if (c == '\0' || IS_HIGHBIT_SET(c))
				yyextra->saw_non_ascii = true;

			return c;
	}
}

static void
check_string_escape_warning(unsigned char ychar, yyscan_t yyscanner)
{
	if (ychar == '\'')
	{
		if (yyextra->warn_on_first_escape && u_sess->attr.attr_sql.escape_string_warning)
			hint_scanner_yyerror("nonstandard use of \\' in a string literal. Use '' to write quotes in strings, or use the escape string syntax (E'...').", yyscanner);

		yyextra->warn_on_first_escape = false;	/* warn only once per string */
	}
	else if (ychar == '\\')
	{
		if (yyextra->warn_on_first_escape && u_sess->attr.attr_sql.escape_string_warning)
			hint_scanner_yyerror("nonstandard use of \\\\ in a string literal. Use the escape string syntax for backslashes, e.g., E'\\\\'.", yyscanner);
		yyextra->warn_on_first_escape = false;	/* warn only once per string */
	}
	else
		check_escape_warning(yyscanner);
}

static void
check_escape_warning(yyscan_t yyscanner)
{
	if (yyextra->warn_on_first_escape && u_sess->attr.attr_sql.escape_string_warning)
		hint_scanner_yyerror("nonstandard use of escape in a string literal. Use the escape string syntax for escapes, e.g., E'\\r\\n'.", yyscanner);
	yyextra->warn_on_first_escape = false;	/* warn only once per string */
}


