/* -------------------------------------------------------------------------
 *
 * pl_scanner.c
 *	  lexical scanning for PL/pgSQL
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  contrib/shark/src/pltsql/pl_scanner.c
 *
 * -------------------------------------------------------------------------
 */
#include "c.h"
#include "pltsql.h"
#include "pl_gram.hpp"
#include "catalog/pg_object.h"
#include "src/backend_parser/scanner.h"

/*
 * A word about keywords:
 *
 * We keep reserved and unreserved keywords in separate headers.  Be careful
 * not to put the same word in both headers.  Also be sure that pl_gram.y's
 * unreserved_keyword production agrees with the unreserved header.  The
 * reserved keywords are passed to the core scanner, so they will be
 * recognized before (and instead of) any variable name.  Unreserved
 * words are checked for separately, after determining that the identifier
 * isn't a known variable name.  If plpgsql_IdentifierLookup is DECLARE then
 * no variable names will be recognized, so the unreserved words always work.
 * (Note in particular that this helps us avoid reserving keywords that are
 * only needed in DECLARE sections.)
 *
 * In certain contexts it is desirable to prefer recognizing an unreserved
 * keyword over recognizing a variable name.  Those cases are handled in
 * gram.y using tok_is_keyword().
 *
 * For the most part, the reserved keywords are those that start a PL/pgSQL
 * statement (and so would conflict with an assignment to a variable of the
 * same name).	We also don't sweat it much about reserving keywords that
 * are reserved in the core grammar.  Try to avoid reserving other words.
 */

/*
 * Lists of keyword (name, token-value, category) entries.
 *
 * !!WARNING!!: These lists must be sorted by ASCII name, because binary
 *		 search is used to locate entries.
 *
 * Be careful not to put the same word in both lists.  Also be sure that
 * gram.y's unreserved_keyword production agrees with the second list.
 */

/* ScanKeywordList lookup data for PL/pgSQL keywords */
#include "pl_reserved_kwlist_d.h"
#include "pl_unreserved_kwlist_d.h"

/* Token codes for PL/pgSQL keywords */
#define PG_KEYWORD(kwname, value) value,

static const uint16 ReservedPLKeywordTokens[] = {
#include "pl_reserved_kwlist.h"
};
static const uint16 UnreservedPLKeywordTokens[] = {
#include "pl_unreserved_kwlist.h"
};

#undef PG_KEYWORD

static const struct PlpgsqlKeywordValue keywordsValue = {
    .procedure = K_PROCEDURE,
    .function = K_FUNCTION,
    .begin = K_BEGIN,
    .select = K_SELECT,
    .update = K_UPDATE,
    .insert = K_INSERT,
    .Delete = K_DELETE,
    .merge = K_MERGE
};

/* Auxiliary data about a token (other than the token type) */
typedef struct {
    YYSTYPE lval; /* semantic information */
    YYLTYPE lloc; /* offset in scanbuf */
    int leng;     /* length in bytes */
} TokenAuxData;

THR_LOCAL TokenAuxData pushback_auxdata[MAX_PUSHBACKS];

/* Internal functions */
static int internal_yylex(TokenAuxData* auxdata);
static void push_back_token(int token, TokenAuxData* auxdata);
static void location_lineno_init(void);
static int get_self_defined_tok(int tok_flag);
static int plpgsql_parse_cursor_attribute(int* loc);
static int plpgsql_parse_declare(int* loc);


/*
 * This is the yylex routine called from the PL/pgSQL grammar.
 * It is a wrapper around the core lexer, with the ability to recognize
 * PL/pgSQL variables and return them as special T_DATUM tokens.  If a
 * word or compound word does not match any variable name, or if matching
 * is turned off by plpgsql_IdentifierLookup, it is returned as
 * T_WORD or T_CWORD respectively, or as an unreserved keyword if it
 * matches one of those.
 */
int pltsql_yylex(void)
{
    int tok1;
    int loc = 0;
    TokenAuxData aux1;
    int kwnum;
    /* enum flag for returning specified tokens */
    int tok_flag = -1;
    int dbl_tok_flag = -1;
    int trip_tok_flag = -1;
    int quad_tok_flag = -1;

    /* parse cursor attribute, return token and location */
    tok1 = plpgsql_parse_cursor_attribute(&loc);
    if (tok1 != -1) {
        plpgsql_yylloc = loc;
        return tok1;
    }
    if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
        /* parse declare condition */
        tok1 = plpgsql_parse_declare(&loc);
        if (tok1 != -1) {
            plpgsql_yylloc = loc;
            return tok1;
        }
    }

    tok1 = internal_yylex(&aux1);
    if (tok1 == IDENT || tok1 == PARAM || tok1 == T_SQL_BULK_EXCEPTIONS) {
        int tok2;
        TokenAuxData aux2;
        char* tok1_val = NULL;
        if(tok1 == PARAM)
            tok1_val = aux1.lval.str;

        tok2 = internal_yylex(&aux2);
        if (tok2 == '.') {
            int tok3;
            TokenAuxData aux3;

            tok3 = internal_yylex(&aux3);
            if (tok3 == IDENT || (tok1 == IDENT && (tok3 == K_DELETE || tok3 == K_CLOSE || tok3 == K_OPEN))) {
                int tok4;
                TokenAuxData aux4;

                tok4 = internal_yylex(&aux4);
                if (tok4 == '.') {
                    int tok5;
                    TokenAuxData aux5;

                    tok5 = internal_yylex(&aux5);
                    if (tok5 == IDENT || (tok5 == K_DELETE && tok3 == IDENT && tok1 == IDENT)) {
                        int tok6;
                        TokenAuxData aux6;

                        tok6 = internal_yylex(&aux6);
                        if (tok6 == '.') {
                            int tok7;
                            TokenAuxData aux7;

                            tok7 = internal_yylex(&aux7);
                            if (tok7 == IDENT ||
                                (tok7 == K_DELETE && tok5 == IDENT && tok3 == IDENT && tok1 == IDENT)) {
                                if (plpgsql_parse_quadword(aux1.lval.str, aux3.lval.str, aux5.lval.str, aux7.lval.str,
                                    &aux1.lval.wdatum, &aux1.lval.cword, &quad_tok_flag)) {
                                    if (quad_tok_flag != -1) {
                                        tok1 = get_self_defined_tok(quad_tok_flag);
                                    } else {
                                        tok1 = T_DATUM;
                                    }
                                } else {
                                    tok1 = T_CWORD;
                                }
                            } else {
                                /* not A.B.C.D, so just process A.B.C */
                                push_back_token(tok7, &aux7);
                                push_back_token(tok6, &aux6);
                                if (plpgsql_parse_tripword(aux1.lval.str, aux3.lval.str, aux5.lval.str,
                                    &aux1.lval.wdatum, &aux1.lval.cword, &trip_tok_flag)) {
                                    if (trip_tok_flag != -1) {
                                        tok1 = get_self_defined_tok(trip_tok_flag);
                                    } else {
                                        tok1 = T_DATUM;
                                    }
                                } else {
                                    tok1 = T_CWORD;
                                }
                            }
                        } else {
                            /* not A.B.C.D, so just process A.B.C */
                            push_back_token(tok6, &aux6);
                            if (plpgsql_parse_tripword(aux1.lval.str, aux3.lval.str, aux5.lval.str,
                                &aux1.lval.wdatum, &aux1.lval.cword, &trip_tok_flag)) {
                                if (trip_tok_flag != -1) {
                                    tok1 = get_self_defined_tok(trip_tok_flag);
                                } else {
                                    tok1 = T_DATUM;
                                }
                            } else {
                                tok1 = T_CWORD;
                            }
                        }
                    } else {
                        /* not A.B.C, so just process A.B */
                        push_back_token(tok5, &aux5);
                        push_back_token(tok4, &aux4);
                        if (plpgsql_parse_dblword(
                                aux1.lval.str, aux3.lval.str, &aux1.lval.wdatum, &aux1.lval.cword, &dbl_tok_flag)) {
                            if (dbl_tok_flag != -1) {
                                tok1 = get_self_defined_tok(dbl_tok_flag);
                            } else {
                                tok1 = T_DATUM;
                            }
                        } else {
                            tok1 = T_CWORD;
                        }
                    }
                } else {
                    /* not A.B.C, so just process A.B */
                    push_back_token(tok4, &aux4);
                    if (plpgsql_parse_dblword(
                            aux1.lval.str, aux3.lval.str, &aux1.lval.wdatum, &aux1.lval.cword, &dbl_tok_flag)) {
                        if (dbl_tok_flag != -1) {
                            tok1 = get_self_defined_tok(dbl_tok_flag);
                        } else {
                            tok1 = T_DATUM;
                        }
                    } else {
                        tok1 = T_CWORD;
                    }
                }
            } else {
                /* not A.B, so just process A */
                push_back_token(tok3, &aux3);
                push_back_token(tok2, &aux2);
                if (plpgsql_parse_word(
                    aux1.lval.str, u_sess->plsql_cxt.curr_compile_context->core_yy->scanbuf + aux1.lloc, &aux1.lval.wdatum,
                    &aux1.lval.word, &tok_flag)) {
                    /* Get self defined token */
                    if (tok_flag != -1) {
                        tok1 = get_self_defined_tok(tok_flag);
                    } else {
                        tok1 = T_DATUM;
                    }
                } else if (!aux1.lval.word.quoted &&
                           (kwnum = ScanKeywordLookup(aux1.lval.word.ident,
													  &UnreservedPLKeywords)) >= 0) {
                    aux1.lval.keyword = GetScanKeyword(kwnum,
													   &UnreservedPLKeywords);
					tok1 = UnreservedPLKeywordTokens[kwnum];
                } else {
                    tok1 = T_WORD;
                }
            }
        } else {
            /* not A.B, so just process A */
            push_back_token(tok2, &aux2);
            if (plpgsql_parse_word(
                aux1.lval.str, u_sess->plsql_cxt.curr_compile_context->core_yy->scanbuf + aux1.lloc, &aux1.lval.wdatum,
                &aux1.lval.word, &tok_flag)) {
                /* Get self defined token */
                if (tok_flag != -1) {
                    tok1 = get_self_defined_tok(tok_flag);
                } else {
                    tok1 = T_DATUM;
                }
            } else if (!aux1.lval.word.quoted &&
                       (kwnum = ScanKeywordLookup(aux1.lval.word.ident,
											      &UnreservedPLKeywords)) >= 0) {
                aux1.lval.keyword = GetScanKeyword(kwnum,
								                   &UnreservedPLKeywords);
                tok1 = UnreservedPLKeywordTokens[kwnum];
            } else if (aux1.lval.str[0] == ':' && tok1 == PARAM) {
                /* Check place holder, it should be like :({identifier}|{integer})
                 * It is a placeholder in exec statement. */
                if(u_sess->attr.attr_sql.sql_compatibility == B_FORMAT)
                {
                    if(pg_strcasecmp(tok1_val, ":loop") == 0)
                        tok1 = T_LABELLOOP;
                    else if(pg_strcasecmp(tok1_val, ":while") == 0)
                        tok1 = T_LABELWHILE;
                    else if(pg_strcasecmp(tok1_val, ":repeat") == 0)
                        tok1 = T_LABELREPEAT;
                    else
                        tok1 = T_PLACEHOLDER;
                }
                else
                {
                    tok1 = T_PLACEHOLDER;
                }
            } else if ((OBJECTTYPE_MEMBER_PROC == u_sess->plsql_cxt.typfunckind
                        || OBJECTTYPE_CONSTRUCTOR_PROC == u_sess->plsql_cxt.typfunckind
                        || OBJECTTYPE_DEFAULT_CONSTRUCTOR_PROC == u_sess->plsql_cxt.typfunckind
                        || OBJECTTYPE_MAP_PROC == u_sess->plsql_cxt.typfunckind
                        || OBJECTTYPE_ORDER_PROC == u_sess->plsql_cxt.typfunckind)
                        && ('=' == tok2 || COLON_EQUALS == tok2 || '[' == tok2 || '(' == tok2)) {
                /* check self.A while creating type body */
                List* idents_bak = aux1.lval.cword.idents;
                if (plpgsql_parse_dblword("self", aux1.lval.str, &aux1.lval.wdatum, &aux1.lval.cword, &dbl_tok_flag)) {
                    tok1 = T_DATUM;
                } else {
                    /* reset */
                    aux1.lval.cword.idents = idents_bak;
                    tok1 = T_DATUM;
                }
            } else {
                tok1 = T_WORD;
            }
        }
    } else {
        /* Not a potential plpgsql variable name, just return the data */
    }

    plpgsql_yylval = aux1.lval;
    plpgsql_yylloc = aux1.lloc;
    u_sess->plsql_cxt.curr_compile_context->plpgsql_yyleng = aux1.leng;
    return tok1;
}

int pltsql_yylex_single(void)
{
    int tok1;
    int loc = 0;
    TokenAuxData aux1;
    int kwnum;
    /* enum flag for returning specified tokens */
    int tok_flag = -1;

    /* parse cursor attribute, return token and location */
    tok1 = plpgsql_parse_cursor_attribute(&loc);
    if (tok1 != -1) {
        plpgsql_yylloc = loc;
        return tok1;
    }

    tok1 = internal_yylex(&aux1);
    if (tok1 == IDENT || tok1 == PARAM) {
        if (plpgsql_parse_word(
            aux1.lval.str, u_sess->plsql_cxt.curr_compile_context->core_yy->scanbuf + aux1.lloc, &aux1.lval.wdatum,
            &aux1.lval.word, &tok_flag)) {
            /* Get self defined token */
            if (tok_flag != -1) {
                tok1 = get_self_defined_tok(tok_flag);
            } else {
                tok1 = T_DATUM;
            }
        } else if (!aux1.lval.word.quoted &&
            (kwnum = ScanKeywordLookup(aux1.lval.word.ident,
									   &UnreservedPLKeywords)) >= 0) {
                    aux1.lval.keyword = GetScanKeyword(kwnum,
												       &UnreservedPLKeywords);
                    tok1 = UnreservedPLKeywordTokens[kwnum];
            } else {
                tok1 = T_WORD;
            }
    } else {
        /* Not a potential plpgsql variable name, just return the data */
    }

    plpgsql_yylval = aux1.lval;
    plpgsql_yylloc = aux1.lloc;
    u_sess->plsql_cxt.curr_compile_context->plpgsql_yyleng = aux1.leng;
    return tok1;
}

/*
 * Internal yylex function.  This wraps the core lexer and adds one feature:
 * a token pushback stack.	We also make a couple of trivial single-token
 * translations from what the core lexer does to what we want, in particular
 * interfacing from the core_YYSTYPE to YYSTYPE union.
 */
static int internal_yylex(TokenAuxData* auxdata)
{
    int token;
    const char* yytext = NULL;

    errno_t rc = memset_s(auxdata, sizeof(TokenAuxData), 0, sizeof(TokenAuxData));
    securec_check(rc, "\0", "\0");
    Assert(u_sess->plsql_cxt.curr_compile_context);
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    if (curr_compile->num_pushbacks > 0) {
        curr_compile->num_pushbacks--;
        token = curr_compile->pushback_token[curr_compile->num_pushbacks];
        *auxdata = pushback_auxdata[curr_compile->num_pushbacks];
    } else {
        token = pgtsql_core_yylex(&auxdata->lval.core_yystype, &auxdata->lloc, curr_compile->yyscanner);

        /* remember the length of yytext before it gets changed */
        yytext = curr_compile->core_yy->scanbuf + auxdata->lloc;
        auxdata->leng = strlen(yytext);

        /* Check for << >> and #, which the core considers operators */
        if (token == Op) {
            if (strcmp(auxdata->lval.str, "<<") == 0) {
                token = LESS_LESS;
            } else if (strcmp(auxdata->lval.str, ">>") == 0) {
                token = GREATER_GREATER;
            } else if (strcmp(auxdata->lval.str, "#") == 0) {
                token = '#';
            }
        } else if (token == PARAM) {
            /* The core returns PARAM as ival, but we treat it like IDENT */
            auxdata->lval.str = pstrdup(yytext);
        }
    }

    return token;
}

/*
 * Push back a token to be re-read by next internal_yylex() call.
 */
static void push_back_token(int token, TokenAuxData* auxdata)
{
    Assert(u_sess->plsql_cxt.curr_compile_context);
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    if (curr_compile->num_pushbacks >= MAX_PUSHBACKS) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_INVALID_OPTION),
                errmsg("too many tokens %d pushed back, max push back token is: %d",
                    curr_compile->num_pushbacks,
                    MAX_PUSHBACKS)));
    }
    curr_compile->pushback_token[curr_compile->num_pushbacks] = token;
    pushback_auxdata[curr_compile->num_pushbacks] = *auxdata;
    curr_compile->num_pushbacks++;
}

/*
 * Push back a single token to be re-read by next pltsql_yylex() call.
 *
 * NOTE: this does not cause yylval or yylloc to "back up".  Also, it
 * is not a good idea to push back a token code other than what you read.
 */
void pltsql_push_back_token(int token)
{
    TokenAuxData auxdata;

    auxdata.lval = plpgsql_yylval;
    auxdata.lloc = plpgsql_yylloc;
    auxdata.leng = u_sess->plsql_cxt.curr_compile_context->plpgsql_yyleng;
    push_back_token(token, &auxdata);
}


/* initialize or reset the state for plpgsql_location_to_lineno */
static void location_lineno_init(void)
{
    Assert(u_sess->plsql_cxt.curr_compile_context);
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;

    curr_compile->cur_line_start = curr_compile->scanorig;
    curr_compile->cur_line_num = 1;

    curr_compile->cur_line_end = strchr(curr_compile->cur_line_start, '\n');
}

/*
 * Called before any actual parsing is done
 *
 * Note: the passed "str" must remain valid until pltsql_scanner_finish().
 * Although it is not fed directly to flex, we need the original string
 * to cite in error messages.
 */
void pltsql_scanner_init(const char* str)
{
    Assert(u_sess->plsql_cxt.curr_compile_context);
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    /* Start up the core scanner */
    curr_compile->yyscanner =
        scanner_init(str, curr_compile->core_yy, &ReservedPLKeywords, ReservedPLKeywordTokens);
    curr_compile->core_yy->isPlpgsqlKeyWord = true;
    curr_compile->core_yy->plKeywordValue = &keywordsValue;

    /*
     * curr_compile->scanorig points to the original string, which unlike the scanner's
     * scanbuf won't be modified on-the-fly by flex.  Notice that although
     * yytext points into scanbuf, we rely on being able to apply locations
     * (offsets from string start) to curr_compile->scanorig as well.
     */
    curr_compile->scanorig = str;

    /* Other setup */
    curr_compile->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_NORMAL;

    curr_compile->num_pushbacks = 0;

    location_lineno_init();
    /*
     * Note, we do plpgsql SQLs parsing under "PL/pgSQL Function" memory context,
     * so it is safe to reset it to NIL in plpgsql execution startup time, even
     * in case of execution error-out, it better to help avoid access to in garbage
     * pointer
     */
    curr_compile->goto_labels = NIL;
}

/*
 * Called after parsing is done to clean up after pltsql_scanner_init()
 */
void pltsql_scanner_finish(void)
{
    Assert(u_sess->plsql_cxt.curr_compile_context);
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    /* release storage */
    pgtsql_scanner_finish(curr_compile->yyscanner);
    /* avoid leaving any dangling pointers */
    curr_compile->yyscanner = NULL;
    curr_compile->scanorig = NULL;
}

/*
 * convert self defined token flag to token
 */
static int get_self_defined_tok(int tok_flag)
{
    Assert(tok_flag != -1);
    switch (tok_flag) {
        case PLPGSQL_TOK_REFCURSOR:
            return T_REFCURSOR;
        case PLPGSQL_TOK_VARRAY:
            return T_VARRAY;
        case PLPGSQL_TOK_VARRAY_FIRST:
            return T_ARRAY_FIRST;
        case PLPGSQL_TOK_VARRAY_LAST:
            return T_ARRAY_LAST;
        case PLPGSQL_TOK_VARRAY_COUNT:
            return T_ARRAY_COUNT;
        case PLPGSQL_TOK_VARRAY_EXTEND:
            return T_ARRAY_EXTEND;
        case PLPGSQL_TOK_VARRAY_EXISTS:
            return T_ARRAY_EXISTS;
        case PLPGSQL_TOK_VARRAY_PRIOR:
            return T_ARRAY_PRIOR;
        case PLPGSQL_TOK_VARRAY_NEXT:
            return T_ARRAY_NEXT;
        case PLPGSQL_TOK_VARRAY_DELETE:
            return T_ARRAY_DELETE;
        case PLPGSQL_TOK_VARRAY_TRIM:
            return T_ARRAY_TRIM;
        case PLPGSQL_TOK_VARRAY_VAR:
            return T_VARRAY_VAR;
        case PLPGSQL_TOK_RECORD:
            return T_RECORD;
        case PLPGSQL_TOK_TABLE:
            return T_TABLE;
        case PLPGSQL_TOK_TABLE_VAR:
            return T_TABLE_VAR;
        case PLPGSQL_TOK_PACKAGE_VARIABLE:
            return T_PACKAGE_VARIABLE;
        case PLPGSQL_TOK_OBJECT_TYPE_VAR_METHOD:
            return T_OBJECT_TYPE_VAR_METHOD;
        default:
            ereport(ERROR,
                (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("unknown plpgsql token: %d", tok_flag)));
    }
    /*never reach here*/
    return -1;
}

/*
 * parse the cursor attributes and get token.
 * if match the cursor attributes, return token,
 * and param loc return the location.
 * if not match, return -1,
 * and push back tokens which were get before.
 */
static int plpgsql_parse_cursor_attribute(int* loc)
{
    TokenAuxData aux1;
    TokenAuxData aux2;
    TokenAuxData aux3;
    TokenAuxData aux4;
    TokenAuxData aux5;
    int tok1;
    int tok2;
    int tok3;
    int tok4;
    int tok5;
    int token = -1;
    PLpgSQL_nsitem* ns = NULL;
    bool pkgCursor = false;

    if (u_sess->parser_cxt.in_package_function_compile) {
        return token;
    }
    /* coverity warning clear */
    tok1 = internal_yylex(&aux1);
    if (tok1 != IDENT && tok1 != PARAM) {
        push_back_token(tok1, &aux1);
        return token;
    }
    tok2 = internal_yylex(&aux2);
    if (tok2 != '%' && tok2 != '.') {
        push_back_token(tok2, &aux2);
        push_back_token(tok1, &aux1);
        return token;
    }
    if (tok2 == '.') {
        pkgCursor = true;
    }
    tok3 = internal_yylex(&aux3);
    if (tok3 != IDENT) {
        push_back_token(tok3, &aux3);
        push_back_token(tok2, &aux2);
        push_back_token(tok1, &aux1);
        return token;
    }
    if (pkgCursor) {
        tok4 = internal_yylex(&aux4);
        if (tok4 != '%') {
            push_back_token(tok4, &aux4);
            push_back_token(tok3, &aux3);
            push_back_token(tok2, &aux2);
            push_back_token(tok1, &aux1);
            return token;
        }
        tok5 = internal_yylex(&aux5);
        if (tok5 != IDENT) {
            push_back_token(tok5, &aux5);
            push_back_token(tok4, &aux4);
            push_back_token(tok3, &aux3);
            push_back_token(tok2, &aux2);
            push_back_token(tok1, &aux1);
            return token;
        }
    }

    if (!pkgCursor) {
        /* match implicit cursor attributes */
        if (strncasecmp(aux1.lval.str, "SQL", 3) == 0) {
            if (strncasecmp(aux3.lval.str, "ISOPEN", 6) == 0) {
                token = T_SQL_ISOPEN;
            }
            if (strncasecmp(aux3.lval.str, "FOUND", 5) == 0) {
                token = T_SQL_FOUND;
            }
            if (strncasecmp(aux3.lval.str, "NOTFOUND", 8) == 0) {
                token = T_SQL_NOTFOUND;
            }
            if (strncasecmp(aux3.lval.str, "ROWCOUNT", 8) == 0) {
                token = T_SQL_ROWCOUNT;
            }
            if (strncasecmp(aux3.lval.str, "BULK_EXCEPTIONS", strlen("BULK_EXCEPTIONS")) == 0) {
                token = T_SQL_BULK_EXCEPTIONS;
            }
        }
        /* match explicit cursor attributes */
        if (strncasecmp(aux1.lval.str, "SQL", 3) != 0) {
            if (strncasecmp(aux3.lval.str, "ISOPEN", 6) == 0) {
                token = T_CURSOR_ISOPEN;
            }
            if (strncasecmp(aux3.lval.str, "FOUND", 5) == 0) {
                token = T_CURSOR_FOUND;
            }
            if (strncasecmp(aux3.lval.str, "NOTFOUND", 8) == 0) {
                token = T_CURSOR_NOTFOUND;
            }
            if (strncasecmp(aux3.lval.str, "ROWCOUNT", 8) == 0) {
                token = T_CURSOR_ROWCOUNT;
            }
        }
    } else {
        /* match package cursor attributes */
        if (strncasecmp(aux3.lval.str, "SQL", 3) != 0) {
            if (strncasecmp(aux5.lval.str, "ISOPEN", 6) == 0) {
                token = T_PACKAGE_CURSOR_ISOPEN;
            }
            if (strncasecmp(aux5.lval.str, "FOUND", 5) == 0) {
                token = T_PACKAGE_CURSOR_FOUND;
            }
            if (strncasecmp(aux5.lval.str, "NOTFOUND", 8) == 0) {
                token = T_PACKAGE_CURSOR_NOTFOUND;
            }
            if (strncasecmp(aux5.lval.str, "ROWCOUNT", 8) == 0) {
                token = T_PACKAGE_CURSOR_ROWCOUNT;
            }
        }
    }
    switch (token) {
        case T_SQL_ISOPEN:
        case T_SQL_FOUND:
        case T_SQL_NOTFOUND:
        case T_SQL_ROWCOUNT:
            /* get the cursor attribute location */
            *loc = aux1.lloc;
            break;
        case T_SQL_BULK_EXCEPTIONS:
            push_back_token(token, &aux3);
            return -1;
            break;
        case T_CURSOR_ISOPEN:
        case T_CURSOR_FOUND:
        case T_CURSOR_NOTFOUND:
        case T_CURSOR_ROWCOUNT:
            /* check the valid of cursor variable */
            ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, aux1.lval.str, NULL, NULL, NULL);
            if (ns != NULL && ns->itemtype == PLPGSQL_NSTYPE_VAR) {
                PLpgSQL_var* var = (PLpgSQL_var*)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[ns->itemno];
                if (!(var != NULL && var->datatype && var->datatype->typoid == REFCURSOROID)) {
                    ereport(ERROR,
                        (errmodule(MOD_PLSQL),
                            errcode(ERRCODE_INVALID_CURSOR_DEFINITION),
                            errmsg("%s isn't a cursor", var ? var->refname : "")));
                }
                aux1.lval.ival = var->dno;
            } else {
                ereport(ERROR,
                    (errmodule(MOD_PLSQL),
                        errcode(ERRCODE_INVALID_CURSOR_DEFINITION),
                        errmsg("undefined cursor: %s", aux1.lval.str)));
            }

            /* get the cursor attribute location */
            *loc = aux1.lloc;
            plpgsql_yylval = aux1.lval;
            break;
        case T_PACKAGE_CURSOR_ISOPEN:
        case T_PACKAGE_CURSOR_FOUND:
        case T_PACKAGE_CURSOR_NOTFOUND:
        case T_PACKAGE_CURSOR_ROWCOUNT:
            /* check the valid of cursor variable */
            ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, aux1.lval.str, aux3.lval.str, NULL, NULL);
            if (ns == NULL) {
                List *idents = list_make2(makeString(aux1.lval.str), makeString(aux3.lval.str));
                int dno = plpgsql_pkg_add_unknown_var_to_namespace(idents);
                if (dno != -1) {
                    ns = plpgsql_ns_lookup(plpgsql_ns_top(), false, aux1.lval.str, aux3.lval.str, NULL, NULL);
                }
                list_free_deep(idents);
            }
            if (ns != NULL && ns->itemtype == PLPGSQL_NSTYPE_VAR) {
                PLpgSQL_var* var = (PLpgSQL_var*)u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[ns->itemno];
                if (!(var != NULL && var->datatype && var->datatype->typoid == REFCURSOROID)) {
                    ereport(ERROR,
                        (errmodule(MOD_PLSQL),
                            errcode(ERRCODE_INVALID_CURSOR_DEFINITION),
                            errmsg("%s.%s isn't a cursor", aux1.lval.str, aux3.lval.str)));
                }
                aux1.lval.wdatum.ident = aux1.lval.str;
                aux1.lval.wdatum.dno = var->dno;
            } else {
                ereport(ERROR,
                    (errmodule(MOD_PLSQL),
                        errcode(ERRCODE_INVALID_CURSOR_DEFINITION),
                        errmsg("undefined cursor: %s.%s", aux1.lval.str, aux3.lval.str)));
            }

            /* get the cursor attribute location */
            *loc = aux1.lloc;
            plpgsql_yylval = aux1.lval;
            break;
        default:
            /* not match, push back tokens which were get before */
            if (pkgCursor) {
                push_back_token(tok5, &aux5);
                push_back_token(tok4, &aux4);
            }
            push_back_token(tok3, &aux3);
            push_back_token(tok2, &aux2);
            push_back_token(tok1, &aux1);
            break;
    }
    return token;
}

static int plpgsql_parse_declare(int* loc)
{
    TokenAuxData aux1;
    int tok1 = -1;
    int token = -1;

    if (u_sess->parser_cxt.in_package_function_compile) {
        return token;
    }

    tok1 = internal_yylex(&aux1);
    if (tok1 == K_DECLARE) {
        TokenAuxData aux2;
        TokenAuxData aux3;

        int tok2 = -1;
        int tok3 = -1;
        tok2 = internal_yylex(&aux2);
        tok3 = internal_yylex(&aux3);
        if (tok3 != IDENT || aux3.lval.str == NULL) {
            push_back_token(tok3, &aux3);
            push_back_token(tok2, &aux2);
            push_back_token(tok1, &aux1);
            return token;
        }
        if (strcasecmp(aux3.lval.str, "cursor") == 0) {
            u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_DECLARE;
            token = T_DECLARE_CURSOR;
            push_back_token(tok3, &aux3);
            push_back_token(tok2, &aux2);
            /* get the declare attribute location */
            *loc = aux1.lloc;
            plpgsql_yylval = aux1.lval;
        } else if (strcasecmp(aux3.lval.str, "condition") == 0) {
            u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_DECLARE;
            token = T_DECLARE_CONDITION;
            push_back_token(tok3, &aux3);
            push_back_token(tok2, &aux2);
            /* get the declare attribute location */
            *loc = aux1.lloc;
            plpgsql_yylval = aux1.lval;
        } else if (strcasecmp(aux3.lval.str, "handler") == 0) {
            u_sess->plsql_cxt.curr_compile_context->plpgsql_IdentifierLookup = IDENTIFIER_LOOKUP_DECLARE;
            token = T_DECLARE_HANDLER;
            push_back_token(tok3, &aux3);
            push_back_token(tok2, &aux2);
            /* get the declare attribute location */
            *loc = aux1.lloc;
            plpgsql_yylval = aux1.lval;
        } else {
                push_back_token(tok3, &aux3);
                push_back_token(tok2, &aux2);
                push_back_token(tok1, &aux1);
        }
    } else {
        push_back_token(tok1, &aux1);
    }
    return token;
}
/*
 * a convenient method to see if the next two tokens are what we expected
 */
bool pltsql_is_token_match2(int token, int token_next)
{
    TokenAuxData aux;
    int tok = -1;
    int tok_next = -1;

    tok = pltsql_yylex();
    if (token == tok) {
        pltsql_push_back_token(tok);
        /* use internal_yylex to get the aux data, so it can be pushed back. */
        tok = internal_yylex(&aux);
        /* coverity warning clear */
        if (tok >= INT_MAX) {
            return false;
        }
        tok_next = pltsql_yylex();
        if (tok_next >= INT_MAX) {
            return false;
        }
        pltsql_push_back_token(tok_next);
        push_back_token(tok, &aux);
        if (token_next == tok_next) {
            return true;
        } else {
            return false;
        }
    } else {
        pltsql_push_back_token(tok);
        return false;
    }
}

/*
 * a convenient method to see if the next token is what we expected
 */
bool pltsql_is_token_match(int token)
{
    int tok = -1;
    tok = pltsql_yylex();
    if (tok == token) {
        pltsql_push_back_token(tok);
        return true;
    }
    pltsql_push_back_token(tok);
    return false;
}

/*
 * a convenient method to see if the next token is keyword
 */
bool pltsql_is_token_keyword(int token)
{
#define MIN(A, B) ((B) < (A) ? (B) : (A))
#define MAX(A, B) ((B) > (A) ? (B) : (A))
    if ((token >= MIN(ReservedPLKeywordTokens[0], UnreservedPLKeywordTokens[0])) 
        && (token <= MAX(ReservedPLKeywordTokens[lengthof(ReservedPLKeywordTokens) - 1],
                         UnreservedPLKeywordTokens[lengthof(UnreservedPLKeywordTokens) - 1]))) {
        return true;
    } else {
        return false;
    }
}