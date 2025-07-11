#include "postgres.h"
#include "storage/proc.h"
#include "knl/knl_variable.h"
#include "parser/parser.h"
#include "shark.h"

#include "gramparse.h"

#include "src/backend_parser/kwlist_d.h"

#include "miscadmin.h"

extern void resetOperatorPlusFlag();

static bool is_prefer_parse_cursor_parentheses_as_expr()
{
    return PREFER_PARSE_CURSOR_PARENTHESES_AS_EXPR;
}

static bool is_cursor_function_exist()
{
    return get_func_oid("cursor", InvalidOid, NULL, false) != InvalidOid;
}

static bool is_select_stmt_definitely(int start_token) {
    return start_token == SELECT || start_token == WITH;
}

static void resetIsTimeCapsuleFlag()
{
    u_sess->parser_cxt.isTimeCapsule = false;
}

static void resetHasPartitionComment()
{
    u_sess->parser_cxt.hasPartitionComment = false;
}

static void resetCreateFuncFlag()
{
    u_sess->parser_cxt.isCreateFuncOrProc = false;
}

static void resetForbidTruncateFlag()
{
    u_sess->parser_cxt.isForbidTruncate = false;
}

static void resetHasSetUservarFlag()
{
    u_sess->parser_cxt.has_set_uservar = false;
}

List* tsql_raw_parser(const char* str, List** query_string_locationlist)
{
    core_yyscan_t yyscanner;
    pgtsql_base_yy_extra_type yyextra;
    int yyresult;

    /* reset u_sess->parser_cxt.stmt_contains_operator_plus */
    resetOperatorPlusFlag();
    
    /* reset u_sess->parser_cxt.hasPartitionComment */
    resetHasPartitionComment();
    
    /* reset u_sess->parser_cxt.isTimeCapsule */
    resetIsTimeCapsuleFlag();

    /* reset u_sess->parser_cxt.isCreateFuncOrProc */
    resetCreateFuncFlag();

    /* reset u_sess->parser_cxt.isForbidTruncate */
    resetForbidTruncateFlag();

    /* reset u_sess->parser_cxt.has_set_uservar */
    resetHasSetUservarFlag();

    /* initialize the flex scanner */
    yyscanner = pgtsql_scanner_init(str, &yyextra.core_yy_extra, &pgtsql_ScanKeywords, pgtsql_ScanKeywordTokens);

    /* base_yylex() only needs this much initialization */
    yyextra.lookahead_len = 0;

    /* initialize the bison parser */
    pgtsql_parser_init(&yyextra);

    /* Parse! */
    yyresult = pgtsql_base_yyparse(yyscanner);

    /* Clean up (release memory) */
    pgtsql_scanner_finish(yyscanner);

    if (yyresult) { /* error */
        return NIL;
    }

    /* Get the locationlist of multi-query through lex. */
    if (query_string_locationlist != NULL) {
        *query_string_locationlist = yyextra.core_yy_extra.query_string_locationlist;

        /* Deal with the query sent from client without semicolon at the end. */
        if (PointerIsValid(*query_string_locationlist) &&
            (size_t)lfirst_int(list_tail(*query_string_locationlist)) < (strlen(str) - 1)) {
            *query_string_locationlist = lappend_int(*query_string_locationlist, strlen(str));
        }
    }

    return yyextra.parsetree;
}

#define GET_NEXT_TOKEN_WITHOUT_YY()                                            \
    do {                                                                       \
        if (lookahead_len) {                                                   \
            base_yy_lookahead& lookahead = lookaheads[lookahead_len - 1];      \
            next_token = lookahead.token;                                      \
            next_yyleng = lookahead.yyleng;                                    \
            lvalp->core_yystype = lookahead.yylval;                            \
            *llocp = lookahead.yylloc;                                         \
            scanbuf[lookahead.prev_hold_char_loc] = lookahead.prev_hold_char;  \
            scanbuf[lookahead.yylloc + lookahead.yyleng] = '\0';               \
            lookahead_len--;                                                   \
        } else {                                                               \
            next_token = core_yylex(&(lvalp->core_yystype), llocp, yyscanner); \
            next_yyleng = pg_yyget_leng(yyscanner);                            \
        }                                                                      \
    } while (0)

#define GET_NEXT_TOKEN()                  \
    do {                                  \
        cur_yylval = lvalp->core_yystype; \
        cur_yylloc = *llocp;              \
        GET_NEXT_TOKEN_WITHOUT_YY();      \
    } while (0)

#define SET_LOOKAHEAD_TOKEN()                                        \
    do {                                                             \
        lookahead_len = 1;                                           \
        base_yy_lookahead& lookahead = lookaheads[0];                \
        lookahead.token = next_token;                                \
        lookahead.yylval = lvalp->core_yystype;                      \
        lookahead.yylloc = *llocp;                                   \
        lookahead.yyleng = next_yyleng;                              \
        lookahead.prev_hold_char_loc = cur_yylloc + cur_yyleng;      \
        lookahead.prev_hold_char = scanbuf[cur_yylloc + cur_yyleng]; \
    } while (0)

#define PARSE_CURSOR_PARENTHESES_AS_EXPR()                                    \
    do {                                                                      \
        cur_token = CURSOR_EXPR;                                              \
        lookaheads[0].token = next_token_2;                                   \
        lookaheads[0].yylval = lvalp->core_yystype;                           \
        lookaheads[0].yylloc = *llocp;                                        \
        lookaheads[0].yyleng = next_yyleng_2;                                 \
        lookaheads[0].prev_hold_char_loc = cur_yylloc_2 + next_yyleng_1;      \
        lookaheads[0].prev_hold_char = scanbuf[cur_yylloc_2 + next_yyleng_1]; \
        lookahead_len = 1;                                                    \
        lvalp->core_yystype = core_yystype_2;                                 \
        *llocp = cur_yylloc_2;                                                \
    } while (0)

#define SET_LOOKAHEAD_2_TOKEN()                                               \
    do {                                                                      \
        lookaheads[1].token = next_token_1;                                   \
        lookaheads[1].yylval = core_yystype_2;                                \
        lookaheads[1].yylloc = cur_yylloc_2;                                  \
        lookaheads[1].yyleng = next_yyleng_1;                                 \
        lookaheads[1].prev_hold_char_loc = cur_yylloc_1 + cur_yyleng_1;       \
        lookaheads[1].prev_hold_char = scanbuf[cur_yylloc_1 + cur_yyleng_1];  \
        lookaheads[0].token = next_token_2;                                   \
        lookaheads[0].yylval = lvalp->core_yystype;                           \
        lookaheads[0].yylloc = *llocp;                                        \
        lookaheads[0].yyleng = next_yyleng_2;                                 \
        lookaheads[0].prev_hold_char_loc = cur_yylloc_2 + next_yyleng_1;      \
        lookaheads[0].prev_hold_char = scanbuf[cur_yylloc_2 + next_yyleng_1]; \
        lookahead_len = 2;                                                    \
        lvalp->core_yystype = core_yystype_1;                                 \
        *llocp = cur_yylloc_1;                                                \
        scanbuf[cur_yylloc_1 + cur_yyleng_1] = '\0';                          \
    } while (0)

#define SET_LOOKAHEAD_3_TOKEN()                     \
    do {                                            \
        lookaheads[2].token = next_token_2;         \
        lookaheads[2].yylval = core_yystype_3;      \
        lookaheads[2].yylloc = cur_yylloc_3;        \
        lookaheads[1].token = next_token_1;         \
        lookaheads[1].yylval = core_yystype_2;      \
        lookaheads[1].yylloc = cur_yylloc_2;        \
        lookaheads[0].token = next_token_3;         \
        lookaheads[0].yylval = lvalp->core_yystype; \
        lookaheads[0].yylloc = *llocp;              \
        lookahead_len = 3;                          \
        lvalp->core_yystype = core_yystype_1;       \
        *llocp = cur_yylloc_1;                      \
    } while (0)

/*
 * Intermediate filter between parser and core lexer (core_yylex in scan.l).
 *
 * The filter is needed because in some cases the standard SQL grammar
 * requires more than one token lookahead.    We reduce these cases to one-token
 * lookahead by combining tokens here, in order to keep the grammar LALR(1).
 *
 * Using a filter is simpler than trying to recognize multiword tokens
 * directly in scan.l, because we'd have to allow for comments between the
 * words.  Furthermore it's not clear how to do it without re-introducing
 * scanner backtrack, which would cost more performance than this filter
 * layer does.
 *
 * The filter also provides a convenient place to translate between
 * the core_YYSTYPE and YYSTYPE representations (which are really the
 * same thing anyway, but notationally they're different).
 */
int pgtsql_base_yylex(YYSTYPE* lvalp, YYLTYPE* llocp, core_yyscan_t yyscanner)
{
    pgtsql_base_yy_extra_type* yyextra = pg_yyget_extra(yyscanner);
    char* scanbuf = yyextra->core_yy_extra.scanbuf;
    base_yy_lookahead* lookaheads = yyextra->lookaheads;
    int& lookahead_len = yyextra->lookahead_len;
    int cur_token;
    YYLTYPE cur_yyleng = 0;
    int next_token;
    YYLTYPE next_yyleng;
    core_YYSTYPE cur_yylval;
    YYLTYPE cur_yylloc = 0;
    int next_token_1 = 0;
    YYLTYPE next_yyleng_1 = 0;
    core_YYSTYPE core_yystype_1;
    YYLTYPE cur_yylloc_1 = 0;
    YYLTYPE cur_yyleng_1 = 0;
    int next_token_2 = 0;
    YYLTYPE next_yyleng_2 = 0;
    core_YYSTYPE core_yystype_2;
    YYLTYPE cur_yylloc_2 = 0;
    int next_token_3 = 0;
    core_YYSTYPE core_yystype_3;
    YYLTYPE cur_yylloc_3 = 0;

    /* Get next token --- we might already have it */
    if (lookahead_len != 0) {
        const base_yy_lookahead& lookahead = lookaheads[lookahead_len - 1];
        cur_token = lookahead.token;
        cur_yyleng = lookahead.yyleng;
        lvalp->core_yystype = lookahead.yylval;
        *llocp = lookahead.yylloc;
        scanbuf[lookahead.prev_hold_char_loc] = lookahead.prev_hold_char;
        scanbuf[lookahead.yylloc + lookahead.yyleng] = '\0';
        lookahead_len--;
    } else {
        cur_token = pgtsql_core_yylex(&(lvalp->core_yystype), llocp, yyscanner);
        cur_yyleng = pg_yyget_leng(yyscanner);
    }

    if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT
        && lookahead_len == 0
        && !IsInitdb
        && !u_sess->attr.attr_common.IsInplaceUpgrade) {
        bool is_last_colon;
        if (cur_token == int(';')) {
            is_last_colon = true;
        } else {
            is_last_colon = false;
        }
        if (yyextra->core_yy_extra.is_delimiter_name == true) {
            if (strcmp(";",u_sess->attr.attr_common.delimiter_name) == 0) {
                cur_token = END_OF_INPUT_COLON;
            } else {
                if (yyextra->core_yy_extra.is_last_colon == false ) {
                    cur_token = END_OF_INPUT_COLON;
                } else {
                    cur_token = END_OF_INPUT;
                }
            }
        }
        if (yyextra->core_yy_extra.is_proc_end == true) {
            cur_token = END_OF_PROC;
        }
        yyextra->core_yy_extra.is_proc_end = false;
        yyextra->core_yy_extra.is_delimiter_name = false;
        yyextra->core_yy_extra.is_last_colon = is_last_colon;
    }

    /* Do we need to look ahead for a possible multiword token? */
    switch (cur_token) {
        case NULLS_P:
            /*
             * NULLS FIRST and NULLS LAST must be reduced to one token
             */
            GET_NEXT_TOKEN();
            switch (next_token) {
                case FIRST_P:
                    cur_token = NULLS_FIRST;
                    break;
                case LAST_P:
                    cur_token = NULLS_LAST;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;

        case NOT:
            /*
             * @hdfs
             *  In order to solve the conflict in gram.y, NOT and ENFORCED must be reduced to one token.
             */
            GET_NEXT_TOKEN();

            switch (next_token) {
                case ENFORCED:
                    cur_token = NOT_ENFORCED;
                    break;
                case IN_P:
                    cur_token = NOT_IN;
                    break;
                case BETWEEN:
                    cur_token = NOT_BETWEEN;
                    break;
                case LIKE:
                    cur_token = NOT_LIKE;
                    break;
                case ILIKE:
                    cur_token = NOT_ILIKE;
                    break;
                case SIMILAR:
                    cur_token = NOT_SIMILAR;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();

                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;

        case EVENT:
            /*
             * Event trigger must be reduced to one token
             */
            GET_NEXT_TOKEN();
            switch (next_token) {
                case TRIGGER:
                    cur_token = EVENT_TRIGGER;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();

                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case WITH:
            GET_NEXT_TOKEN();
            core_yystype_1 = cur_yylval;
            cur_yylloc_1 = cur_yylloc;
            cur_yyleng_1 = cur_yyleng;
            next_token_1 = next_token;
            next_yyleng_1 = next_yyleng;

            switch (next_token) {
                /*
                * WITH TIME must be reduced to one token
                */
                case TIME:
                    cur_token = WITH_TIME;
                    break;
                case '(':
                    GET_NEXT_TOKEN();
                    core_yystype_2 = cur_yylval;
                    cur_yylloc_2 = cur_yylloc;
                    next_token_2 = next_token;
                    next_yyleng_2 = next_yyleng;

                    switch (next_token) {
                        case TSQL_NOLOCK:
                        case TSQL_READUNCOMMITTED:
                        case TSQL_UPDLOCK:
                        case TSQL_REPEATABLEREAD:
                        case SERIALIZABLE:
                        case TSQL_READCOMMITTED:
                        case TSQL_TABLOCK:
                        case TSQL_TABLOCKX:
                        case TSQL_PAGLOCK:
                        case TSQL_ROWLOCK:
                        case NOWAIT:
                        case TSQL_READPAST:
                        case TSQL_XLOCK:
                        case SNAPSHOT:
                        case TSQL_NOEXPAND:
                            cur_token = WITH_paren;
                            // back to current position
                        default:
                            /* save the lookahead token for next time */
                            SET_LOOKAHEAD_2_TOKEN();
                            break;
                    }
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();

                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        
        case '(':
            if (ENABLE_TABLE_HINT_IDENTIFIER) {
                break;
            }

            GET_NEXT_TOKEN();
            switch (next_token) {
                case TSQL_NOLOCK:
                case TSQL_READUNCOMMITTED:
                case TSQL_UPDLOCK:
                case TSQL_REPEATABLEREAD:
                case SERIALIZABLE:
                case TSQL_READCOMMITTED:
                case TSQL_TABLOCK:
                case TSQL_TABLOCKX:
                case TSQL_PAGLOCK:
                case TSQL_ROWLOCK:
                case NOWAIT:
                case TSQL_READPAST:
                case TSQL_XLOCK:
                case SNAPSHOT:
                case TSQL_NOEXPAND:
                    cur_token = TSQL_HINT_START_BRACKET;
                    // back to current position
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();

                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;

        case INCLUDING:

            /*
             * INCLUDING ALL must be reduced to one token
             */
            GET_NEXT_TOKEN();

            switch (next_token) {
                case ALL:
                    cur_token = INCLUDING_ALL;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();

                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;

        case RENAME:

            /*
             * RENAME PARTITION must be reduced to one token
             */
            GET_NEXT_TOKEN();

            switch (next_token) {
                case PARTITION:
                    cur_token = RENAME_PARTITION;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;

        case PARTITION:

            /*
             * RENAME PARTITION must be reduced to one token
             */
            GET_NEXT_TOKEN();

            switch (next_token) {
                case FOR:
                    cur_token = PARTITION_FOR;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case SUBPARTITION:

            GET_NEXT_TOKEN();

            switch (next_token) {
                case FOR:
                    cur_token = SUBPARTITION_FOR;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case ADD_P:
            /*
             * ADD PARTITION must be reduced to one token
             */
            GET_NEXT_TOKEN();

            switch (next_token) {
                case PARTITION:
                    cur_token = ADD_PARTITION;
                    break;
                case SUBPARTITION:
                    cur_token = ADD_SUBPARTITION;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;

        case DROP:

            /*
             * DROP PARTITION must be reduced to one token
             */
            GET_NEXT_TOKEN();

            switch (next_token) {
                case PARTITION:
                    cur_token = DROP_PARTITION;
                    break;
                case SUBPARTITION:
                    cur_token = DROP_SUBPARTITION;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case REBUILD:

            /*
             * REBUILD PARTITION must be reduced to one token
             */
            GET_NEXT_TOKEN();

            switch (next_token) {
                case PARTITION:
                    cur_token = REBUILD_PARTITION;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case MODIFY_P:
            /*
             * MODIFY PARTITION must be reduced to one token
             */
            GET_NEXT_TOKEN();

            switch (next_token) {
                case PARTITION:
                    cur_token = MODIFY_PARTITION;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case DECLARE:
            /*
             * DECLARE foo CUROSR must be looked ahead, and if determined as a DECLARE_CURSOR, we should set the yylaval
             * and yylloc back, letting the parser read the cursor name correctly.
             * here we may still have token in lookaheads, so use GET_NEXT_TOKEN to get
             */
            GET_NEXT_TOKEN();
            /* get first token after DECLARE. We don't care what it is */
            lookaheads[1].token = next_token;
            lookaheads[1].yylval = lvalp->core_yystype;
            lookaheads[1].yylloc = *llocp;
            lookaheads[1].yyleng = next_yyleng;
            lookaheads[1].prev_hold_char_loc = cur_yylloc + cur_yyleng;
            lookaheads[1].prev_hold_char = scanbuf[cur_yylloc + cur_yyleng];

            /* 
             * get the second token after DECLARE. If it is cursor grammar, we are sure that this is a cursr stmt
             * in fact we don't have any token in lookaheads here for sure, cause MAX_LOOKAHEAD_LEN is 2.
             * but maybe someday MAX_LOOKAHEAD_LEN increase, so we still use GET_NEXT_TOKEN_WITHOUT_SET_CURYY
             */
            GET_NEXT_TOKEN_WITHOUT_YY();
            lookaheads[0].token = next_token;
            lookaheads[0].yylval = lvalp->core_yystype;
            lookaheads[0].yylloc = *llocp;
            lookaheads[0].yyleng = next_yyleng;
            lookaheads[0].prev_hold_char_loc = lookaheads[1].yylloc + lookaheads[1].yyleng;
            lookaheads[0].prev_hold_char = scanbuf[lookaheads[1].yylloc + lookaheads[1].yyleng];
            lookahead_len = 2;

            switch (next_token) {
                case CURSOR:
                case BINARY:
                case INSENSITIVE:
                case NO:
                case SCROLL:
                    cur_token = DECLARE_CURSOR;
                    /* and back up the output info to cur_token  because we should read cursor name correctly. */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
                default:
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case VALID:

            /*
             * VALID BEGIN must be reduced to one token, to avoid conflict with BEGIN TRANSACTIOn and BEGIN anonymous
             * block.
             */
            GET_NEXT_TOKEN();

            switch (next_token) {
                case BEGIN_P:
                case BEGIN_NON_ANOYBLOCK:
                    cur_token = VALID_BEGIN;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case START:
            /*
             * START WITH must be reduced to one token, to allow START as table / column alias.
             */
            GET_NEXT_TOKEN();

            switch (next_token) {
                case WITH:
                    cur_token = START_WITH;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case CONNECT:
            /*
             * CONNECT BY must be reduced to one token, to allow CONNECT as table / column alias.
             */
            GET_NEXT_TOKEN();

            switch (next_token) {
                case BY:
                    cur_token = CONNECT_BY;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case ON:
            /* here we may still have token in lookaheads, so use GET_NEXT_TOKEN to get */
            GET_NEXT_TOKEN();
            /* get first token after ON (Normal UPDATE). We don't care what it is */
            lookaheads[1].token = next_token;
            lookaheads[1].yylval = lvalp->core_yystype;
            lookaheads[1].yylloc = *llocp;
            lookaheads[1].yyleng = next_yyleng;
            lookaheads[1].prev_hold_char_loc = cur_yylloc + cur_yyleng;
            lookaheads[1].prev_hold_char = scanbuf[cur_yylloc + cur_yyleng];

            /*
             * get the second token after ON.
             * in fact we don't have any token in lookaheads here for sure, cause MAX_LOOKAHEAD_LEN is 2.
             * but maybe someday MAX_LOOKAHEAD_LEN increase, so we still use GET_NEXT_TOKEN_WITHOUT_SET_CURYY
             */
            GET_NEXT_TOKEN_WITHOUT_YY();
            lookaheads[0].token = next_token;
            lookaheads[0].yylval = lvalp->core_yystype;
            lookaheads[0].yylloc = *llocp;
            lookaheads[0].yyleng = next_yyleng;
            lookaheads[0].prev_hold_char_loc = lookaheads[1].yylloc + lookaheads[1].yyleng;
            lookaheads[0].prev_hold_char = scanbuf[lookaheads[1].yylloc + lookaheads[1].yyleng];
            lookahead_len = 2;

            switch (next_token) {
                case CURRENT_TIMESTAMP:
                case CURRENT_TIME:
                case CURRENT_DATE:
                case LOCALTIME:
                case LOCALTIMESTAMP:
                    cur_token = ON_UPDATE_TIME;
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
                default:
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case SHOW:
            /*
             * SHOW ERRORS must be reduced to one token, to allow ERRORS as table / column alias.
             */
            GET_NEXT_TOKEN();

            switch (next_token) {
                case ERRORS:
                    cur_token = SHOW_ERRORS;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case USE_P:
        /*
         * USE INDEX \USE KEY must be reduced to one token,to allow KEY\USE as table / column alias.
         */
            GET_NEXT_TOKEN();

            switch (next_token) {
                case KEY:
                    cur_token = USE_INDEX;
                    break;
                case INDEX:
                    cur_token = USE_INDEX;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case FORCE:
        /*
         * FORCE INDEX \FORCE KEY must be reduced to one token,to allow KEY\FORCE as table / column alias.
         */
            GET_NEXT_TOKEN();

            switch (next_token) {
                case KEY:
                    cur_token = FORCE_INDEX;
                    break;
                case INDEX:
                    cur_token = FORCE_INDEX;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case IGNORE:
        /*
         * IGNORE INDEX \IGNORE KEY must be reduced to one token,to allow KEY\IGNORE as table / column alias.
         */
            GET_NEXT_TOKEN();

            switch (next_token) {
                case KEY:
                case INDEX:
                    cur_token = IGNORE_INDEX;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case CURSOR:
            GET_NEXT_TOKEN();
            core_yystype_1 = cur_yylval;  // the value of cursor
            cur_yylloc_1 = cur_yylloc;    // the lloc of cursor
            cur_yyleng_1 = cur_yyleng;    // the yyleng of cursor
            next_token_1 = next_token;    // the token after curosr
            next_yyleng_1 = next_yyleng;  // the yylneg after curosr
            if (next_token_1 != '(') {
                /* save the lookahead token for next time */
                SET_LOOKAHEAD_TOKEN();
                /* and back up the output info to cur_token */
                lvalp->core_yystype = cur_yylval;
                *llocp = cur_yylloc;
                scanbuf[cur_yylloc + cur_yyleng] = '\0';
            } else {
                GET_NEXT_TOKEN();
                core_yystype_2 = cur_yylval;   // the value after cursor
                cur_yylloc_2 = cur_yylloc;     // the lloc after cursor
                next_token_2 = next_token;     // the token after after curosr
                next_yyleng_2 = next_yyleng;   // the yyleng after after curosr

                if (next_token_1 == '(' && (is_select_stmt_definitely(next_token))) {
                    PARSE_CURSOR_PARENTHESES_AS_EXPR();
                } else if (is_prefer_parse_cursor_parentheses_as_expr() && !is_cursor_function_exist()) {
                    PARSE_CURSOR_PARENTHESES_AS_EXPR();
                } else {
                    SET_LOOKAHEAD_2_TOKEN();
                }
                if (t_thrd.proc->workingVersionNum < CURSOR_EXPRESSION_VERSION_NUMBER &&
                    cur_token == CURSOR_EXPR) {
                        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Unsupported feature: cursor expression during the upgrade")));
                }
            }
            break;
        case LATERAL_P:
            GET_NEXT_TOKEN();
            core_yystype_1 = cur_yylval;  // the value of cursor
            cur_yylloc_1 = cur_yylloc;    // the lloc of cursor
            cur_yyleng_1 = cur_yyleng;    // the yyleng of cursor
            next_token_1 = next_token;    // the token after curosr
            next_yyleng_1 = next_yyleng;  // the yyleng after curosr
            if (next_token_1 != IDENT) {
                /* save the lookahead token for next time */
                SET_LOOKAHEAD_TOKEN();
                /* and back up the output info to cur_token */
                lvalp->core_yystype = cur_yylval;
                *llocp = cur_yylloc;
                scanbuf[cur_yylloc + cur_yyleng] = '\0';
            } else {
                GET_NEXT_TOKEN();
                core_yystype_2 = cur_yylval;   // the value after cursor
                cur_yylloc_2 = cur_yylloc;     // the lloc after cursor
                next_token_2 = next_token;     // the token after after curosr
                next_yyleng_2 = next_yyleng;   // the yyleng after after curosr
                if (next_token_1 == IDENT && next_token == '(') {
                    cur_token = LATERAL_EXPR;
                }
                SET_LOOKAHEAD_2_TOKEN();
        }
        break;
        case STATIC_P:
            GET_NEXT_TOKEN();
            switch (next_token) {
                /* static function/procedure */
                case FUNCTION:
                    cur_token = STATIC_FUNCTION;
                    break;
                case PROCEDURE:
                    cur_token = STATIC_PROCEDURE;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case MEMBER:
            GET_NEXT_TOKEN();
            switch (next_token) {
                /* MEMBER function/procedure */
                case FUNCTION:
                    cur_token = MEMBER_FUNCTION;
                    break;
                case PROCEDURE:
                    cur_token = MEMBER_PROCEDURE;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case CONSTRUCTOR:
            GET_NEXT_TOKEN();
            switch (next_token) {
                /* MEMBER function/procedure */
                case FUNCTION:
                    cur_token = CONSTRUCTOR_FUNCTION;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case MAP:
            GET_NEXT_TOKEN();
            switch (next_token) {
                case MEMBER:
                    cur_token = MAP_MEMBER;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case SELF:
            GET_NEXT_TOKEN();
            switch (next_token) {
                case INOUT:
                    cur_token = SELF_INOUT;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case FALSE_P:
            /* ERROR ON ERROR, TRUE ON ERROR and FALSE ON ERROR must be reduced to one token */
            GET_NEXT_TOKEN();
            core_yystype_1 = cur_yylval;
            cur_yylloc_1 = cur_yylloc;
            cur_yyleng_1 = cur_yyleng;
            next_token_1 = next_token;
            next_yyleng_1 = next_yyleng;

            switch (next_token) {
                case ON:
                    GET_NEXT_TOKEN();
                    core_yystype_2 = cur_yylval;
                    cur_yylloc_2 = cur_yylloc;
                    next_token_2 = next_token;
                    next_yyleng_2 = next_yyleng;

                    switch (next_token) {
                        case ERROR_P:
                            cur_token = FALSE_ON_ERROR;
                            break;
                        default:
                            SET_LOOKAHEAD_2_TOKEN();
                            break;
                    }
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case TRUE_P:
            GET_NEXT_TOKEN();
            core_yystype_1 = cur_yylval;
            cur_yylloc_1 = cur_yylloc;
            cur_yyleng_1 = cur_yyleng;
            next_token_1 = next_token;
            next_yyleng_1 = next_yyleng;

            switch (next_token) {
                case ON:
                    GET_NEXT_TOKEN();
                    core_yystype_2 = cur_yylval;
                    cur_yylloc_2 = cur_yylloc;
                    next_token_2 = next_token;
                    next_yyleng_2 = next_yyleng;

                    switch (next_token) {
                        case ERROR_P:
                            cur_token = TRUE_ON_ERROR;
                            break;
                        default:
                            SET_LOOKAHEAD_2_TOKEN();
                            break;
                    }
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case ERROR_P:
            GET_NEXT_TOKEN();
            core_yystype_1 = cur_yylval;
            cur_yylloc_1 = cur_yylloc;
            cur_yyleng_1 = cur_yyleng;
            next_token_1 = next_token;
            next_yyleng_1 = next_yyleng;

            switch (next_token) {
                case ON:
                    GET_NEXT_TOKEN();
                    core_yystype_2 = cur_yylval;
                    cur_yylloc_2 = cur_yylloc;
                    next_token_2 = next_token;
                    next_yyleng_2 = next_yyleng;

                    switch (next_token) {
                        case ERROR_P:
                            cur_token = ERROR_ON_ERROR;
                            break;
                        default:
                            SET_LOOKAHEAD_2_TOKEN();
                            break;
                    }
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case RAW:
            GET_NEXT_TOKEN();
            core_yystype_1 = cur_yylval;
            cur_yylloc_1 = cur_yylloc;
            next_token_1 = next_token;

            if (next_token == '(') {
                GET_NEXT_TOKEN();
                core_yystype_2 = cur_yylval;
                cur_yylloc_2 = cur_yylloc;
                next_token_2 = next_token;

                if (next_token == ICONST) {
                    GET_NEXT_TOKEN();
                    core_yystype_3 = cur_yylval;
                    cur_yylloc_3 = cur_yylloc;
                    next_token_3 = next_token;
                    if (next_token == ')') {
                        cur_token = RAW;
                    } else {
                        SET_LOOKAHEAD_3_TOKEN();
                    }
                } else {
                    SET_LOOKAHEAD_2_TOKEN();
                }
            } else {
                /* save the lookahead token for next time */
                SET_LOOKAHEAD_TOKEN();
                /* and back up the output info to cur_token */
                lvalp->core_yystype = cur_yylval;
                *llocp = cur_yylloc;
            }
            break;
        case UNIQUE:
            GET_NEXT_TOKEN();
            core_yystype_1 = cur_yylval;
            cur_yylloc_1 = cur_yylloc;
            cur_yyleng_1 = cur_yyleng;
            next_token_1 = next_token;
            next_yyleng_1 = next_yyleng;

            switch (next_token) {
                case TSQL_NONCLUSTERED:
                    cur_token = TSQL_UNIQUE_NONCLUSTERED;
                    break;
                case TSQL_CLUSTERED:
                    cur_token = TSQL_UNIQUE_CLUSTERED;
                    break;
                default:
                    /* save the lookahead token for next time */
                    SET_LOOKAHEAD_TOKEN();
                    /* and back up the output info to cur_token */
                    lvalp->core_yystype = cur_yylval;
                    *llocp = cur_yylloc;
                    scanbuf[cur_yylloc + cur_yyleng] = '\0';
                    break;
            }
            break;
        case PRIMARY:
            GET_NEXT_TOKEN();
            core_yystype_1 = cur_yylval;
            cur_yylloc_1 = cur_yylloc;
            cur_yyleng_1 = cur_yyleng;
            next_token_1 = next_token;
            next_yyleng_1 = next_yyleng;

            if (next_token == KEY) {
                GET_NEXT_TOKEN();
                core_yystype_2 = cur_yylval;
                cur_yylloc_2 = cur_yylloc;
                next_token_2 = next_token;
                next_yyleng_2 = next_yyleng;

                switch (next_token) {
                    case TSQL_NONCLUSTERED:
                        cur_token = TSQL_PRIMAY_KEY_NONCLUSTERED;
                        break;
                    case TSQL_CLUSTERED:
                        cur_token = TSQL_PRIMAY_KEY_CLUSTERED;
                        break;
                    default:
                        SET_LOOKAHEAD_2_TOKEN();
                        break;
                }
            } else {
                /* save the lookahead token for next time */
                SET_LOOKAHEAD_TOKEN();
                /* and back up the output info to cur_token */
                lvalp->core_yystype = cur_yylval;
                *llocp = cur_yylloc;
                scanbuf[cur_yylloc + cur_yyleng] = '\0';
                break;
            }
            break;
        default:
            break;
    }

    return cur_token;
}