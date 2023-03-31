%{
/*-------------------------------------------------------------------------
 *
 * scan.l
 *	  lexical scanner for PostgreSQL
 *
 * NOTE NOTE NOTE:
 *
 * The rules in this file must be kept in sync with psql's lexer!!!
 *
 * The rules are designed so that the scanner never has to backtrack,
 * in the sense that there is always a rule that can match the input
 * consumed so far (the rule action may internally throw back some input
 * with yyless(), however).  As explained in the flex manual, this makes
 * for a useful speed increase --- about a third faster than a plain -CF
 * lexer, in simple testing.  The extra complexity is mostly in the rules
 * for handling float numbers and continued string literals.  If you change
 * the lexical rules, verify that you haven't broken the no-backtrack
 * property by running flex with the "-b" option and checking that the
 * resulting "lex.backup" file says that no backing up is needed.  (As of
 * Postgres 9.2, this check is made automatically by the Makefile.)
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/parser/scan.l
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include <stdint.h>
#include <ctype.h>
#include <unistd.h>
#include <string>
#include "datatypes.h"
#include "nodes/pg_list.h"
#include "nodes/feparser_memutils.h"

#include "parser/scanner.h"
#include "parser/scansup.h"

#include "mb/pg_wchar.h"
#include "datatypes.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "catalog/pg_attribute.h"
#include "access/tupdesc.h"
#include "nodes/parsenodes_common.h"
#include "parser/parser.h"
#include "libpq/cl_state.h"

#include "gram.hpp"


#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wunused-variable"

/*
 * Constant data exported from this file.  This array maps from the
 * zero-based keyword numbers returned by ScanKeywordLookup to the
 * Bison token numbers needed by gram.y.  This is exported because
 * callers need to pass it to scanner_init, if they are using the
 * standard keyword list ScanKeywords.
 */
#define PG_KEYWORD(kwname, value, category) value,

const uint16 ScanKeywordTokens[] = {
#include "parser/kwlist.h"
};

#undef PG_KEYWORD

/*
 * Set the type of YYSTYPE.
 */
#define YYSTYPE core_YYSTYPE

/*
 * Set the type of yyextra.  All state variables used by the scanner should
 * be in yyextra, *not* statically allocated.
 */
#define YY_EXTRA_TYPE fe_core_yy_extra_type *

long getDynaParamSeq(const char *string, bool initflag, bool placeholder, core_yyscan_t yyscanner);

/*
 * Each call to yylex must set yylloc to the location of the found token
 * (expressed as a byte offset from the start of the input text).
 * When we parse a token that requires multiple lexer rules to process,
 * this should be done in the first such rule, else yylloc will point
 * into the middle of the token.
 */
#define SET_YYLLOC()  (*(yylloc) = yytext - yyextra->scanbuf)

/*
 * Advance yylloc by the given number of bytes.
 */
#define ADVANCE_YYLLOC(delta)  ( *(yylloc) += (delta) )

#define startlit()  ( yyextra->literallen = 0 )
static void addlit(char *ytext, int yleng, core_yyscan_t yyscanner);
static void addlitchar(unsigned char ychar, core_yyscan_t yyscanner);
static char *litbufdup(core_yyscan_t yyscanner);
static char *litbuf_udeescape(unsigned char escape, core_yyscan_t yyscanner);
static unsigned char unescape_single_char(unsigned char c, core_yyscan_t yyscanner);
static int	process_integer_literal(const char *token, YYSTYPE *lval);
static bool is_utf16_surrogate_first(pg_wchar c);
static bool is_utf16_surrogate_second(pg_wchar c);
static pg_wchar surrogate_pair_to_codepoint(pg_wchar first, pg_wchar second);
static void addunicode(pg_wchar c, yyscan_t yyscanner);

#define yyerror(msg)  scanner_yyerror(msg, yyscanner)

#define lexer_errposition()  scanner_errposition(*(yylloc), yyscanner)

static void check_string_escape_warning(unsigned char ychar, core_yyscan_t yyscanner);
static void check_escape_warning(core_yyscan_t yyscanner);
static bool is_trans_stmt(const char *haystack, int haystack_len);
/*
 * Work around a bug in flex 2.5.35: it emits a couple of functions that
 * it forgets to emit declarations for.  Since we use -Wmissing-prototypes,
 * this would cause warnings.  Providing our own declarations should be
 * harmless even when the bug gets fixed.
 */
extern int	core_yyget_column(yyscan_t yyscanner);
extern void core_yyset_column(int column_no, yyscan_t yyscanner);

/* clear key message that may appear in core file for security */
extern THR_LOCAL bool clear_key_memory;
%}

%option reentrant
%option bison-bridge
%option bison-locations
%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option noyyalloc
%option noyyrealloc
%option noyyfree
%option warn
%option prefix="core_yy"

/*
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
%x xc
%x xd
%x xh
%x xe
%x xq
%x xdolq
%x xui
%x xus
%x xeu

/*
 * In order to make the world safe for Windows and Mac clients as well as
 * Unix ones, we accept either \n or \r as a newline.  A DOS-style \r\n
 * sequence will be seen as two successive newlines, but that doesn't cause
 * any problems.  Comments that start with -- and extend to the next
 * newline are treated as equivalent to a single whitespace character.
 *
 * NOTE a fine point: if there is no newline following --, we will absorb
 * everything to the end of the input as a comment.  This is correct.  Older
 * versions of Postgres failed to recognize -- as a comment if the input
 * did not end with a newline.
 *
 * XXX perhaps \f (formfeed) should be treated as a newline as well?
 *
 * XXX if you change the set of whitespace characters, fix scanner_isspace()
 * to agree, and see also the plpgsql lexer.
 */

space			[ \t\n\r\f]
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


/* C-style comments
 *
 * The "extended comment" syntax closely resembles allowable operator syntax.
 * The tricky part here is to get lex to recognize a string starting with
 * slash-star as a comment, when interpreting it as an operator would produce
 * a longer match --- remember lex will prefer a longer match!  Also, if we
 * have something like plus-slash-star, lex will think this is a 3-character
 * operator whereas we want to see it as a + operator and a comment start.
 * The solution is two-fold:
 * 1. append {op_chars}* to xcstart so that it matches as much text as
 *    {operator} would. Then the tie-breaker (first matching rule of same
 *    length) ensures xcstart wins.  We put back the extra stuff with yyless()
 *    in case it contains a star-slash that should terminate the comment.
 * 2. In the operator rule, check for slash-star within the operator, and
 *    if found throw it back with yyless().  This handles the plus-slash-star
 *    problem.
 * Dash-dash comments have similar interactions with the operator rule.
 */
xcstart			\/\*{op_chars}*
xcstop			\*+\/
xcinside		[^*/]+

digit			[0-9]
ident_start		[A-Za-z\200-\377_]
ident_cont		[A-Za-z\200-\377_0-9\$\#]

identifier		{ident_start}{ident_cont}*

typecast		"::"
plus_join		"(+)"
dot_dot			\.\.
colon_equals	":="
para_equals	"=>"

/*
 * "self" is the set of chars that should be returned as single-character
 * tokens.  "op_chars" is the set of chars that can make up "Op" tokens,
 * which can be one or more characters long (but if a single-char token
 * appears in the "self" set, it is not to be returned as an Op).  Note
 * that the sets overlap, but each has some chars that are not in the other.
 *
 * If you change either set, adjust the character lists appearing in the
 * rule for "operator"!
 */
self			[,()\[\].;\:\+\-\*\/\%\^\<\>\=]
op_chars		[\~\!\@\#\^\&\|\`\?\+\-\*\/\%\<\>\=]
operator		{op_chars}+

/* we no longer allow unary minus in numbers.
 * instead we pass it separately to parser. there it gets
 * coerced via doNegate() -- Leon aug 20 1999
 *
* {decimalfail} is used because we would like "1..10" to lex as 1, dot_dot, 10.
*
 * {realfail1} and {realfail2} are added to prevent the need for scanner
 * backup when the {real} rule fails to match completely.
 */

integer			{digit}+
decimal			(({digit}*\.{digit}+)|({digit}+\.{digit}*))
decimalfail		{digit}+\.\.
real			({integer}|{decimal})[Ee][-+]?{digit}+
realfail1		({integer}|{decimal})[Ee]
realfail2		({integer}|{decimal})[Ee][-+]

param			\${integer}

newParam		:({identifier}|{integer})

other			.

/*
 * Dollar quoted strings are totally opaque, and no escaping is done on them.
 * Other quoted strings must allow some special characters such as single-quote
 *  and newline.
 * Embedded single-quotes are implemented both in the SQL standard
 *  style of two adjacent single quotes "''" and in the Postgres/Java style
 *  of escaped-quote "\'".
 * Other embedded escaped characters are matched explicitly and the leading
 *  backslash is dropped from the string.
 * Note that xcstart must appear before operator, as explained above!
 *  Also whitespace (comment) must appear before operator.
 */

%%

{whitespace}	{
					/* ignore */
				}

{xcstart}	{
					/* Set location in case of syntax error in comment */
					SET_YYLLOC();
					yyextra->xcdepth = 0;
					BEGIN(xc);
					/* Put back any characters past slash-star; see above */
					yyless(2);
					if (yyextra->is_hint_str)
					{
						startlit();
						addlit(yytext, yyleng, yyscanner);
					}
		}

<xc>{xcstart}	{
					(yyextra->xcdepth)++;
					/* Put back any characters past slash-star; see above */
					yyless(2);
					if (yyextra->is_hint_str)
					{
						addlit(yytext, yyleng, yyscanner);
					}
		}

<xc>{xcstop}	{
					if (yyextra->xcdepth <= 0)
						BEGIN(INITIAL);
					else
						(yyextra->xcdepth)--;

					if (yyextra->is_hint_str)
					{	
						addlit(yytext, yyleng, yyscanner);
						yylval->str = litbufdup(yyscanner);
						yyextra->is_hint_str = false;
						return COMMENTSTRING;
					}
		}

<xc>{xcinside}	{
					if (yyextra->is_hint_str)
					{
						addlit(yytext, yyleng, yyscanner);
					}
		}

<xc>{op_chars}	{
					if (yyextra->is_hint_str)
					{
						addlit(yytext, yyleng, yyscanner);
					}
		}

<xc>\*+		{
					if (yyextra->is_hint_str)
					{
						addlit(yytext, yyleng, yyscanner);
					}
		}

<xc><<EOF>>		{ yyerror("unterminated /* comment"); return 0;}

{xbstart}		{
					/* Binary bit type.
					 * At some point we should simply pass the string
					 * forward to the parser and label it there.
					 * In the meantime, place a leading "b" on the string
					 * to mark it for the input routine as a binary string.
					 */
					SET_YYLLOC();
					BEGIN(xb);
					startlit();
					addlitchar('b', yyscanner);
				}
<xb>{quotestop}	|
<xb>{quotefail} {
					yyless(1);
					BEGIN(INITIAL);
					yylval->str = litbufdup(yyscanner);
					yyextra->is_hint_str = false;
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
<xb><<EOF>>		{ yyerror("unterminated bit string literal"); return 0;}

{xhstart}		{
					/* Hexadecimal bit type.
					 * At some point we should simply pass the string
					 * forward to the parser and label it there.
					 * In the meantime, place a leading "x" on the string
					 * to mark it for the input routine as a hex string.
					 */
					SET_YYLLOC();
					BEGIN(xh);
					startlit();
					addlitchar('x', yyscanner);
				}
<xh>{quotestop}	|
<xh>{quotefail} {
					yyless(1);
					BEGIN(INITIAL);
					yylval->str = litbufdup(yyscanner);
					yyextra->is_hint_str = false;
					return XCONST;
		}
<xh><<EOF>>		{ yyerror("unterminated hexadecimal string literal"); return 0;}

{xnstart}		{
					/* National character.
					 * We will pass this along as a normal character string,
					 * but preceded with an internally-generated "NCHAR".
					 */
					int kwnum;
					
					SET_YYLLOC();
					yyless(1);				/* eat only 'n' this time */

					kwnum = ScanKeywordLookup("nchar",
											  yyextra->keywordlist);
					if (kwnum >= 0)
					{
						yylval->keyword = GetScanKeyword(kwnum, yyextra->keywordlist);
						yyextra->is_hint_str = false;
						return yyextra->keyword_tokens[kwnum];;
					}
					else
					{
						/* If NCHAR isn't a keyword, just return "n" */
						yylval->str = feparser_strdup("n");
						yyextra->ident_quoted = false;
						yyextra->is_hint_str = false;
						return IDENT;
					}
				}

{xqstart}		{
					yyextra->warn_on_first_escape = true;
					yyextra->saw_non_ascii = false;
					SET_YYLLOC();
					if (yyextra->m_clientLogic->gucParams.standard_conforming_strings)
						BEGIN(xq);
					else
						BEGIN(xe);
					startlit();
				}
{xestart}		{
					yyextra->warn_on_first_escape = false;
					yyextra->saw_non_ascii = false;
					SET_YYLLOC();
					BEGIN(xe);
					startlit();
				}
{xusstart}		{
					SET_YYLLOC();
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("unsafe use of string constant with Unicode escapes"),
								 errdetail("String constants with Unicode escapes cannot be used when standard_conforming_strings is off."),
								 lexer_errposition()));
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
					yyextra->is_hint_str = false;
					return SCONST;
				}
<xus>{xusstop1} {
					/* throw back all but the quote */
					yyless(1);
					BEGIN(INITIAL);
					yylval->str = litbuf_udeescape('\\', yyscanner);
					yyextra->is_hint_str = false;
					return SCONST;
		}
<xus>{xusstop2} {
					BEGIN(INITIAL);
					yylval->str = litbuf_udeescape(yytext[yyleng-2], yyscanner);
					yyextra->is_hint_str = false;
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
						yyerror("invalid Unicode surrogate pair");
					else
						addunicode(c, yyscanner);
				}
<xeu>{xeunicode} {
					pg_wchar c = strtoul(yytext+2, NULL, 16);

					if (!is_utf16_surrogate_second(c))
						yyerror("invalid Unicode surrogate pair");

					c = surrogate_pair_to_codepoint(yyextra->utf16_first_part, c);

					addunicode(c, yyscanner);

					BEGIN(xe);
				}
<xeu>.			{ yyerror("invalid Unicode surrogate pair"); }
<xeu>\n			{ yyerror("invalid Unicode surrogate pair"); }
<xeu><<EOF>>	{ yyerror("invalid Unicode surrogate pair"); }
<xe,xeu>{xeunicodefail}	{
					    printf("invalid Unicode escape\n");
                        
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
								 errmsg("invalid Unicode escape"),
								 errhint("Unicode escapes must be \\uXXXX or \\UXXXXXXXX."),
								 lexer_errposition()));
				}
<xe>{xeescape}  {
					if (yytext[1] == '\'')
					{
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
<xq,xe,xus><<EOF>>		{ yyerror("unterminated quoted string"); return 0;}

{dolqdelim}		{
					SET_YYLLOC();
					yyextra->dolqstart = feparser_strdup(yytext);
					BEGIN(xdolq);
					startlit();
				}
{dolqfailed}	{
					SET_YYLLOC();
					/* throw back all but the initial "$" */
					yyless(1);
					/* and treat it as {other} */
					yyextra->is_hint_str = false;
					return yytext[0];
				}
<xdolq>{dolqdelim} {
					if (strcmp(yytext, yyextra->dolqstart) == 0)
					{
						feparser_free(yyextra->dolqstart);
						yyextra->dolqstart = NULL;
						BEGIN(INITIAL);
						yylval->str = litbufdup(yyscanner);
						yyextra->is_hint_str = false;
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
<xdolq><<EOF>>	{ yyerror("unterminated dollar-quoted string"); return 0;}

{xdstart}		{
					SET_YYLLOC();
					BEGIN(xd);
					startlit();
				}
{xuistart}		{
					SET_YYLLOC();
					BEGIN(xui);
					startlit();
				}
<xd>{xdstop}	{
					char		   *ident;

					BEGIN(INITIAL);
					if (yyextra->literallen == 0)
						yyerror("zero-length delimited identifier");
					ident = litbufdup(yyscanner);
					if (yyextra->literallen >= NAMEDATALEN)
						truncate_identifier(ident, yyextra->literallen, yyextra->warnOnTruncateIdent);
					yylval->str = ident;
					yyextra->ident_quoted = true;
					yyextra->is_hint_str = false;
					return IDENT;
				}
<xui>{xuistop1}	{
					char		   *ident;
					int             identlen;

					BEGIN(INITIAL);
					if (yyextra->literallen == 0)
						yyerror("zero-length delimited identifier");
					ident = litbuf_udeescape('\\', yyscanner);
					identlen = strlen(ident);
					if (identlen >= NAMEDATALEN)
						truncate_identifier(ident, identlen, yyextra->warnOnTruncateIdent);
					yylval->str = ident;
					/* throw back all but the quote */
					yyless(1);
					yyextra->ident_quoted = false;
					yyextra->is_hint_str = false;
					return IDENT;
				}
<xui>{xuistop2}	{
					char		   *ident;
					int             identlen;

					BEGIN(INITIAL);
					if (yyextra->literallen == 0)
						yyerror("zero-length delimited identifier");
					ident = litbuf_udeescape(yytext[yyleng - 2], yyscanner);
					identlen = strlen(ident);
					if (identlen >= NAMEDATALEN)
						truncate_identifier(ident, identlen, yyextra->warnOnTruncateIdent);
					yylval->str = ident;
					yyextra->ident_quoted = false;
					yyextra->is_hint_str = false;
					return IDENT;
				}
<xd,xui>{xddouble}	{
					addlitchar('"', yyscanner);
				}
<xd,xui>{xdinside}	{
					addlit(yytext, yyleng, yyscanner);
				}
<xd,xui><<EOF>>		{ yyerror("unterminated quoted identifier"); return 0;}

{xufailed}	{
					char		   *ident;

					SET_YYLLOC();
					/* throw back all but the initial u/U */
					yyless(1);
					/* and treat it as {identifier} */
					ident = downcase_truncate_identifier(yytext, yyleng, yyextra->warnOnTruncateIdent);
					yylval->str = ident;
					yyextra->ident_quoted = false;
					yyextra->is_hint_str = false;
					return IDENT;
				}

{typecast}		{
					SET_YYLLOC();
					yyextra->is_hint_str = false;
					return TYPECAST;
				}

{plus_join}	{
					SET_YYLLOC();
					yyextra->is_hint_str = false;
					return ORA_JOINOP;
				}

{dot_dot}		{
					SET_YYLLOC();
					yyextra->is_hint_str = false;
					return DOT_DOT;
				}

{colon_equals}	{
					SET_YYLLOC();
					yyextra->is_hint_str = false;
					return COLON_EQUALS;
				}

{para_equals}	{
					SET_YYLLOC();
					yyextra->is_hint_str = false;
					return PARA_EQUALS;
				}

{self}			{
					SET_YYLLOC();
					/*
					 * Get the semicolon which is not in proc body nor in the '( )', treat it
					 * as end flag of a single query and store it in locationlist.
					 */
					if (yyextra->dolqstart == NULL)
					{
						if (yytext[0] == '(')
							yyextra->paren_depth++;
						else if (yytext[0] == ')' && yyextra->paren_depth > 0)
							yyextra->paren_depth--;
						else if (yytext[0] == ';' && yyextra->paren_depth == 0 && !yyextra->in_slash_proc_body)
							yyextra->query_string_locationlist = lappend_int(yyextra->query_string_locationlist, *yylloc);
					}
					yyextra->is_hint_str = false;
					return yytext[0];
				}

{operator}		{
					/*
					 * Check for embedded slash-star or dash-dash; those
					 * are comment starts, so operator must stop there.
					 * Note that slash-star or dash-dash at the first
					 * character will match a prior rule, not this one.
					 */
					int		nchars = yyleng;
					char   *slashstar = strstr(yytext, "/*");
					char   *dashdash = strstr(yytext, "--");

					if (slashstar && dashdash)
					{
						/* if both appear, take the first one */
						if (slashstar > dashdash)
							slashstar = dashdash;
					}
					else if (!slashstar)
						slashstar = dashdash;
					if (slashstar)
						nchars = slashstar - yytext;

					/*
					 * For SQL compatibility, '+' and '-' cannot be the
					 * last char of a multi-char operator unless the operator
					 * contains chars that are not in SQL operators.
					 * The idea is to lex '=-' as two operators, but not
					 * to forbid operator names like '?-' that could not be
					 * sequences of SQL operators.
					 */
					while (nchars > 1 &&
						   (yytext[nchars-1] == '+' ||
							yytext[nchars-1] == '-'))
					{
						int		ic;

						for (ic = nchars-2; ic >= 0; ic--)
						{
							if (strchr("~!@#^&|`?%", yytext[ic]))
								break;
						}
						if (ic >= 0)
							break; /* found a char that makes it OK */
						nchars--; /* else remove the +/-, and check again */
					}

					SET_YYLLOC();

					if (nchars < (int)yyleng)
					{
						/* Strip the unwanted chars from the token */
						yyless(nchars);
						/*
						 * If what we have left is only one char, and it's
						 * one of the characters matching "self", then
						 * return it as a character token the same way
						 * that the "self" rule would have.
						 */
						if (nchars == 1 &&
							strchr(",()[].;:+-*/%^<>=", yytext[0]))
						{
							yyextra->is_hint_str = false;
							return yytext[0];
						}
					}

					/*
					 * Complain if operator is too long.  Unlike the case
					 * for identifiers, we make this an error not a notice-
					 * and-truncate, because the odds are we are looking at
					 * a syntactic mistake anyway.
					 */
					if (nchars >= NAMEDATALEN)
						yyerror("operator too long");

					/* Convert "!=" operator to "<>" for compatibility */
					if (strcmp(yytext, "!=") == 0)
					{
						yylval->str = feparser_strdup("<>");
						yyextra->is_hint_str = false;
						return CmpOp;
					}
					else if (strcmp(yytext, ">=") == 0 || strcmp(yytext, "<=") == 0 || strcmp(yytext, "<>") == 0)
					{
						yylval->str = feparser_strdup(yytext);
						yyextra->is_hint_str = false;
						return CmpOp;
					}
					else
						yylval->str = feparser_strdup(yytext);
					yyextra->is_hint_str = false;
					return Op;
				}

{param}			{
					SET_YYLLOC();
					yylval->ival = getDynaParamSeq(yytext + 1, false, false, yyscanner);
					yyextra->is_hint_str = false;
					return PARAM;
				}
{newParam}		{
					SET_YYLLOC();
					yylval->ival = getDynaParamSeq(yytext + 1, false, true, yyscanner);
					yyextra->is_hint_str = false;
					return PARAM;
				}
{integer}		{
					SET_YYLLOC();
					yyextra->is_hint_str = false;
					return process_integer_literal(yytext, yylval);
				}
{decimal}		{
					SET_YYLLOC();
					yylval->str = feparser_strdup(yytext);
					yyextra->is_hint_str = false;
					return FCONST;
				}
{decimalfail}	{
					/* throw back the .., and treat as integer */
					yyless(yyleng-2);
					SET_YYLLOC();
					yyextra->is_hint_str = false;
					return process_integer_literal(yytext, yylval);
				}
{real}			{
					SET_YYLLOC();
					yylval->str = feparser_strdup(yytext);
					yyextra->is_hint_str = false;
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
					SET_YYLLOC();
					yylval->str = feparser_strdup(yytext);
					yyextra->is_hint_str = false;
					return FCONST;
				}
{realfail2}		{
					/* throw back the [Ee][+-], and proceed as above */
					yyless(yyleng-2);
					SET_YYLLOC();
					yylval->str = feparser_strdup(yytext);
					yyextra->is_hint_str = false;
					return FCONST;
				}


{identifier}	{
					char		   *ident;
					int kwnum;

					SET_YYLLOC();

					/* Is it a keyword? */
					kwnum = ScanKeywordLookup(yytext,
											  yyextra->keywordlist);
					if (kwnum >= 0)
					{
						yylval->keyword = GetScanKeyword(kwnum, yyextra->keywordlist);
						uint16 token = yyextra->keyword_tokens[kwnum];

						/* Find the CREATE PROCEDURE syntax and set dolqstart. */
						if (token == CREATE)
						{
							yyextra->is_createstmt = true;
						}
						else if (token == TRIGGER && yyextra->is_createstmt)
						{
							/* Create trigger don't need set dolqstart */
							yyextra->is_createstmt = false;
						}
						else if ((token == PROCEDURE || token == FUNCTION)
								 && yyextra->is_createstmt)
						{
							/* Make yyextra->dolqstart not NULL means its in a proc with $$. */
							yyextra->dolqstart = "";
						}
						else if (token == BEGIN_P)
						{
							/* cases that have to be a trans stmt and fall quickly */
							if (yyg->yy_hold_char == ';' || /* found ';' after 'begin' */
								yyg->yy_hold_char == '\0')  /* found '\0' after 'begin' */
								return BEGIN_NON_ANOYBLOCK;
							/* look for other transaction stmt */
							if (is_trans_stmt(yyextra->scanbuf, yyextra->scanbuflen))
								return BEGIN_NON_ANOYBLOCK;
						}
						else if (token == SELECT)
						{
							yyextra->is_hint_str = true;
						}

						return token;
					}

					/*
					 * No.  Convert the identifier to lower case, and truncate
					 * if necessary.
					 */
					ident = downcase_truncate_identifier(yytext, yyleng, yyextra->warnOnTruncateIdent);
					yylval->str = ident;
					yyextra->ident_quoted = false;
					yyextra->is_hint_str = false;
					return IDENT;
				}

{other}			{
					SET_YYLLOC();
					yyextra->is_hint_str = false;
					return yytext[0];
				}

<<EOF>>			{
					SET_YYLLOC();
					yyterminate();
				}

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
#undef yylloc
#define yylloc  (((struct yyguts_t *) yyscanner)->yylloc_r)
#undef yyleng
#define yyleng  (((struct yyguts_t *) yyscanner)->yyleng_r)


void
scanner_yyerror(const char *message, core_yyscan_t yyscanner)
{
    fe_rethrow(yyscanner);
}


/*
 * Called before any actual parsing is done
 */
core_yyscan_t
fe_scanner_init(const char *str,
			 fe_core_yy_extra_type *yyext,
			 const ScanKeywordList *keywordlist,
			 const uint16 *keyword_tokens)
{
	Size		slen = strlen(str);
	yyscan_t	scanner;

	if (yylex_init(&scanner) != 0)
		printf("yylex_init() failed: %m\n");
        //in this location - yyscanner is still not defined and therefore we can't use ereport macro

	core_yyset_extra(yyext, scanner);

	yyext->keywordlist = keywordlist;
	yyext->keyword_tokens = keyword_tokens;
	yyext->in_slash_proc_body = false;
	yyext->paren_depth = 0;
	yyext->query_string_locationlist = NIL;
	yyext->is_createstmt = false;
	yyext->dolqstart = NULL;
	yyext->is_hint_str = false;
	yyext->parameter_list = NIL;

	/*
	 * Make a scan buffer with special termination needed by flex.
	 */
	yyext->scanbuf = (char *) feparser_malloc(slen + 2);
	yyext->scanbuflen = slen;
	memcpy(yyext->scanbuf, str, slen);
	yyext->scanbuf[slen] = yyext->scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;
	yy_scan_buffer(yyext->scanbuf, slen + 2, scanner);

	/* initialize literal buffer to a reasonable but expansible size */
	yyext->literalalloc = 1024;
	yyext->literalbuf = (char *) feparser_malloc(yyext->literalalloc);
	yyext->literallen = 0;
	yyext->warnOnTruncateIdent = true;

	// Added CALL for procedure and function
	getDynaParamSeq("init", true, true, NULL);

	return scanner;
}


/*
 * Called after parsing is done to clean up after scanner_init()
 */
void
scanner_finish(core_yyscan_t yyscanner)
{
    // updated by the grammer - the variable is never true
	if (yyextra->scanbuflen >= 8192)
		feparser_free(yyextra->scanbuf);
	if (yyextra->literalalloc >= 8192)
		feparser_free(yyextra->literalbuf);
	if (yyextra->parameter_list)
	{
		list_free_deep(yyextra->parameter_list);
		yyextra->parameter_list = NIL;
	}
}


static void
addlit(char *ytext, int yleng, core_yyscan_t yyscanner)
{
	/* enlarge buffer if needed */
	if ((yyextra->literallen + yleng) >= yyextra->literalalloc)
	{
		do
		{
			yyextra->literalalloc *= 2;
		} while ((yyextra->literallen + yleng) >= yyextra->literalalloc);

		/*when yytext is larger than 512M, its double will exceed 1G, so we use refeparser_malloc_huge */
		yyextra->literalbuf = (char *) feparser_realloc(yyextra->literalbuf,
 												yyextra->literalalloc);
	}
	/* append new data */
	memcpy(yyextra->literalbuf + yyextra->literallen, ytext, yleng);
	yyextra->literallen += yleng;
}


static void
addlitchar(unsigned char ychar, core_yyscan_t yyscanner)
{
	/* enlarge buffer if needed */
	if ((yyextra->literallen + 1) >= yyextra->literalalloc)
	{
		yyextra->literalalloc *= 2;
		yyextra->literalbuf = (char *) feparser_realloc(yyextra->literalbuf,
 												yyextra->literalalloc);
	}
	/* append new data */
	yyextra->literalbuf[yyextra->literallen] = ychar;
	yyextra->literallen += 1;
}


/*
 * Create a feparser_malloc'd copy of literalbuf, adding a trailing null.
 */
static char *
litbufdup(core_yyscan_t yyscanner)
{
	int			llen = yyextra->literallen;
	char	   *newm;

	newm = (char *)feparser_malloc(llen + 1);
	memcpy(newm, yyextra->literalbuf, llen);
	newm[llen] = '\0';
	return newm;
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
		lval->str = feparser_strdup(token);
		return FCONST;
	}
	lval->ival = val;
	return ICONST;
}

static unsigned int
hexval(unsigned char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 0xA;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 0xA;
    printf("invalid hexadecimal digit\n");
    /*
	ereport(ERROR,
		(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		errmsg("invalid hexadecimal digit")));
    */
	return -1; /* not reached */
}

static void
check_unicode_value(pg_wchar c, const char *loc, core_yyscan_t yyscanner)
{
	if (GetDatabaseEncoding() == PG_UTF8)
		return;

	if (c > 0x7F)
	{
		ADVANCE_YYLLOC(loc - yyextra->literalbuf + 3);   /* 3 for U&" */
		yyerror("Unicode escape values cannot be used for code point values above 007F when the server encoding is not UTF8");
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
addunicode(pg_wchar c, core_yyscan_t yyscanner)
{
	char buf[8];

	if (c == 0 || c > 0x10FFFF)
		yyerror("invalid Unicode escape value");
	if (c > 0x7F)
	{
		if (GetDatabaseEncoding() != PG_UTF8)
			yyerror("Unicode escape values cannot be used for code point values above 007F when the server encoding is not UTF8");
		yyextra->saw_non_ascii = true;
	}
	unicode_to_utf8(c, (unsigned char *) buf);
	addlit(buf, pg_mblen(buf), yyscanner);
}

static char *
litbuf_udeescape(unsigned char escape, core_yyscan_t yyscanner)
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
		ADVANCE_YYLLOC(yyextra->literallen + yyleng + 1);
		yyerror("invalid Unicode escape character");
	}

	/* Make literalbuf null-terminated to simplify the scanning loop */
	litbuf = yyextra->literalbuf;
	litbuf[yyextra->literallen] = '\0';

	/*
	 * This relies on the subtle assumption that a UTF-8 expansion
	 * cannot be longer than its escaped representation.
	 */
	newm = (char *)feparser_malloc(yyextra->literallen + 1);

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
					ADVANCE_YYLLOC(in - litbuf + 3);   /* 3 for U&" */
					yyerror("invalid Unicode surrogate pair");
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

				unicode = (hexval(in[1]) << 12) +
					(hexval(in[2]) << 8) +
					(hexval(in[3]) << 4) +
					hexval(in[4]);
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
						ADVANCE_YYLLOC(in - litbuf + 3);   /* 3 for U&" */
						yyerror("invalid Unicode surrogate pair");
					}
				}
				else if (is_utf16_surrogate_second(unicode))
					yyerror("invalid Unicode surrogate pair");

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

				unicode = (hexval(in[2]) << 20) +
					(hexval(in[3]) << 16) +
					(hexval(in[4]) << 12) +
					(hexval(in[5]) << 8) +
					(hexval(in[6]) << 4) +
					hexval(in[7]);
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
						ADVANCE_YYLLOC(in - litbuf + 3);   /* 3 for U&" */
						yyerror("invalid Unicode surrogate pair");
					}
				}
				else if (is_utf16_surrogate_second(unicode))
					yyerror("invalid Unicode surrogate pair");

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
				ADVANCE_YYLLOC(in - litbuf + 3);   /* 3 for U&" */
				yyerror("invalid Unicode escape value");
			}
		}
		else
		{
			if (pair_first)
			{
				ADVANCE_YYLLOC(in - litbuf + 3);   /* 3 for U&" */
				yyerror("invalid Unicode surrogate pair");
			}
			*out++ = *in++;
		}
	}

	/* unfinished surrogate pair? */
	if (pair_first)
	{
		ADVANCE_YYLLOC(in - litbuf + 3);			/* 3 for U&" */
		yyerror("invalid Unicode surrogate pair");
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
unescape_single_char(unsigned char c, core_yyscan_t yyscanner)
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
check_string_escape_warning(unsigned char ychar, core_yyscan_t yyscanner)
{
	if (ychar == '\'')
	{
			ereport(WARNING,
					(errcode(ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER),
					 errmsg("nonstandard use of \\' in a string literal"),
					 errhint("Use '' to write quotes in strings, or use the escape string syntax (E'...')."),
					 lexer_errposition()));
		yyextra->warn_on_first_escape = false;	/* warn only once per string */
	}
	else if (ychar == '\\')
	{
			ereport(WARNING,
					(errcode(ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER),
					 errmsg("nonstandard use of \\\\ in a string literal"),
					 errhint("Use the escape string syntax for backslashes, e.g., E'\\\\'."),
					 lexer_errposition()));
		yyextra->warn_on_first_escape = false;	/* warn only once per string */
	}
	else
		check_escape_warning(yyscanner);
}

static void
check_escape_warning(core_yyscan_t yyscanner)
{
        
		ereport(WARNING,
				(errcode(ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER),
				 errmsg("nonstandard use of escape in a string literal"),
				 errhint("Use the escape string syntax for escapes, e.g., E'\\r\\n'."),
				 lexer_errposition())); 
	yyextra->warn_on_first_escape = false;	/* warn only once per string */
}

/*
 * Interface functions to make flex use feparser_malloc() instead of feparser_malloc().
 * It'd be better to make these static, but flex insists otherwise.
 */

void *
core_yyalloc(yy_size_t bytes, core_yyscan_t yyscanner)
{
	return feparser_malloc(bytes);
}

void *
core_yyrealloc(void *ptr, yy_size_t bytes, core_yyscan_t yyscanner)
{
	if (ptr)
		return feparser_realloc(ptr, bytes);
	else
		return feparser_malloc(bytes);
}

void
core_yyfree(void *ptr, core_yyscan_t yyscanner)
{
	if (ptr)
		feparser_free(ptr);
}


/*
 * @Description:  get the parameter sequence of dynamic SQL
 * @in string: parameter name
 * @in initflag:  mark the operation is init or not
 * @in placeholder: the flag to mark the binding parameter is placeholder or dollar quoting
 * @in yyscanner: for yyextra
 * @return - the sequence number of the parameter
 */
long 
getDynaParamSeq(const char *string, bool initflag, bool placeholder, core_yyscan_t yyscanner)
{
	static THR_LOCAL bool	has_dollar = false;
	static THR_LOCAL bool	has_placeholder = false;
	int result = 0;
	char* str = NULL;
	const ListCell *cell;

	if (initflag)
	{ 
		has_dollar = false;
		has_placeholder = false;
		return 0;
	}

	if (placeholder == false)
	{
		if (has_placeholder) {
			printf("It is forbidden to use placeholder and dollar quoting together.\n");
            
			ereport(ERROR, 
					(errcode(ERRCODE_SYNTAX_ERROR), 
					errmsg("It is forbidden to use placeholder and dollar quoting together.")));
		}
		has_dollar = true;
		return atol(string);
	}

	has_placeholder = true;
	if (has_dollar)
		printf("It is forbidden to use placeholder and dollar quoting together.\n");
        
		ereport(ERROR, 
				(errcode(ERRCODE_SYNTAX_ERROR), 
				errmsg("It is forbidden to use placeholder and dollar quoting together.")));


	foreach(cell, yyextra->parameter_list)
	{
		result++;
		if (strcmp((char*)(lfirst(cell)),string) == 0)
			return result;
	}

	str = feparser_strdup(string);
	yyextra->parameter_list = lappend(yyextra->parameter_list, (void*)str);

	return result + 1;
}

/*
 * @Description: if we found begin, check if is a transaction stmt
 * @param[IN] haystack:  the give source string
 * @param[IN] haystack_len: the length of haystack. Note that haystack may have been separated into words by '\0',
 							so haystack_len is needed.
 * @return: true is a transaction stmt, false if not.
 *
 * we have to deal with a tricky case in which we recieve a sql like "begin   " which is not terminated with ';' and
 * followed by servral blank char. In this case we add a variable 'found_non_blank_char' to handle this case.
 * if we haven't found any non blank char in the sql, consider it to be a transaction stmt.
 */
static bool
is_trans_stmt(const char *haystack, int haystack_len)
{
	char *tempstr = (char *)feparser_malloc0(haystack_len + 1);
	char *temp = tempstr;
	int line = 1; /* lineno of haystack which split by \0 */
	bool found_non_blank_char = false; /* mark if we find a non blank char after begin */
	errno_t rc = EOK;

	/* we have to make a copy, since haystack is const char* */
	rc = memcpy_s(tempstr, haystack_len + 1, haystack, haystack_len);
	securec_check_c(rc, "\0", "\0");

	/* find if the 2nd line is prefixed by a valid transaction token */
	while (temp < tempstr + haystack_len)
	{
		/* there may be '\0' in the string, and should be skipped */
		if (*temp == '\0')
		{
			temp++;
			line++;
			/* we only search the 2nd line */
			if (line > 2)
				break;
		}
		/* skip the blank char */
		else if (isspace(*temp))
		{
			temp++;
		}
		else
		{
			/* we found a non blank char after begin, do further checking */
			if (line == 2)
				found_non_blank_char = true;
			/* For a transaction statement, all possible tokens after BEGIN are here */
			if (line == 2 &&(pg_strncasecmp(temp, "transaction", strlen("transaction")) == 0 ||
							  pg_strncasecmp(temp, "work", strlen("work")) == 0 ||
							  pg_strncasecmp(temp, "isolation", strlen("isolation")) == 0 ||
							  pg_strncasecmp(temp, "read", strlen("read")) == 0 ||
							  pg_strncasecmp(temp, "deferrable", strlen("deferrable")) == 0 ||
							  pg_strncasecmp(temp, "not", strlen("not")) == 0 ||
							  pg_strncasecmp(temp, ";", strlen(";")) == 0))
			{
				feparser_free(tempstr);
				return true;
			}

			temp += strlen(temp);
		}
	}

	feparser_free (tempstr);

	/*
	 * if all the char after begin are blank
	 *    it is a trans stmt
	 * else
	 *    it is a anaynomous block stmt
	 */
	return found_non_blank_char ? false : true;
}

