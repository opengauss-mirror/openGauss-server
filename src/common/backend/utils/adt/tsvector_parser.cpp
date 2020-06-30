/* -------------------------------------------------------------------------
 *
 * tsvector_parser.c
 *	  Parser for tsvector
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/tsvector_parser.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "tsearch/ts_locale.h"
#include "tsearch/ts_utils.h"

const int STATE_LEN = 32;

/*
 * Private state of tsvector parser.  Note that tsquery also uses this code to
 * parse its input, hence the boolean flags.  The two flags are both true or
 * both false in current usage, but we keep them separate for clarity.
 * is_tsquery affects *only* the content of error messages.
 */
struct TSVectorParseStateData {
    char* prsbuf;    /* next input character */
    char* bufstart;  /* whole string (used only for errors) */
    char* word;      /* buffer to hold the current word */
    int len;         /* size in bytes allocated for 'word' */
    int eml;         /* max bytes per character */
    bool oprisdelim; /* treat ! | * ( ) as delimiters? */
    bool is_tsquery; /* say "tsquery" not "tsvector" in errors? */
};

/*
 * Initializes parser for the input string. If oprisdelim is set, the
 * following characters are treated as delimiters in addition to whitespace:
 * ! | & ( )
 */
TSVectorParseState init_tsvector_parser(char* input, bool oprisdelim, bool is_tsquery)
{
    TSVectorParseState state;

    state = (TSVectorParseState)palloc(sizeof(struct TSVectorParseStateData));
    state->prsbuf = input;
    state->bufstart = input;
    state->len = 32;
    state->word = (char*)palloc(state->len);
    state->eml = pg_database_encoding_max_length();
    state->oprisdelim = oprisdelim;
    state->is_tsquery = is_tsquery;

    return state;
}

/*
 * Reinitializes parser to parse 'input', instead of previous input.
 */
void reset_tsvector_parser(TSVectorParseState state, char* input)
{
    state->prsbuf = input;
}

/*
 * Shuts down a tsvector parser.
 */
void close_tsvector_parser(TSVectorParseState state)
{
    pfree_ext(state->word);
    pfree_ext(state);
}

/* increase the size of 'word' if needed to hold one more character */
#define RESIZEPRSBUF                                                \
    do {                                                            \
        int clen = cur_pos - state->word;                            \
        if (clen + state->eml >= state->len) {                      \
            state->len *= 2;                                        \
            state->word = (char*)repalloc(state->word, state->len); \
            cur_pos = state->word + clen;                            \
        }                                                           \
    } while (0)

#define ISOPERATOR(x) (pg_mblen(x) == 1 && (*(x) == '!' || *(x) == '&' || *(x) == '|' || *(x) == '(' || *(x) == ')'))

/* Fills gettoken_tsvector's output parameters, and returns true */
#define RETURN_TOKEN                        \
    do {                                    \
        if (pos_ptr != NULL) {              \
            *pos_ptr = pos;                 \
            *poslen = npos;                 \
        } else if (pos != NULL)             \
            pfree_ext(pos);                 \
                                            \
        if (strval != NULL)                 \
            *strval = state->word;          \
        if (lenval != NULL)                 \
            *lenval = cur_pos - state->word; \
        if (endptr != NULL)                 \
            *endptr = state->prsbuf;        \
        return true;                        \
    } while (0)

/* State codes used in gettoken_tsvector */
#define WAITWORD 1
#define WAITENDWORD 2
#define WAITNEXTCHAR 3
#define WAITENDCMPLX 4
#define WAITPOSINFO 5
#define INPOSINFO 6
#define WAITPOSDELIM 7
#define WAITCHARCMPLX 8

#define PRSSYNTAXERROR prs_syntax_error(state)

const int POSA_LEN_BASE = 4;

static void prs_syntax_error(TSVectorParseState state)
{
    ereport(ERROR,
        (errcode(ERRCODE_SYNTAX_ERROR),
            state->is_tsquery ? errmsg("syntax error in tsquery: \"%s\"", state->bufstart)
                                : errmsg("syntax error in tsvector: \"%s\"", state->bufstart)));
}

/*
 * Get next token from string being parsed. Returns true if successful,
 * false if end of input string is reached.  On success, these output
 * parameters are filled in:
 *
 * *strval		pointer to token
 * *lenval		length of *strval
 * *pos_ptr		pointer to a palloc'd array of positions and weights
 *				associated with the token. If the caller is not interested
 *				in the information, NULL can be supplied. Otherwise
 *				the caller is responsible for pfreeing the array.
 * *poslen		number of elements in *pos_ptr
 * *endptr		scan resumption point
 *
 * Pass NULL for unwanted output parameters.
 */
bool gettoken_tsvector(
    TSVectorParseState state, char** strval, int* lenval, WordEntryPos** pos_ptr, int* poslen, char** endptr)
{
    int old_state = 0;
    char* cur_pos = state->word;
    int state_code = WAITWORD;

    /*
     * pos is for collecting the comma delimited list of positions followed by
     * the actual token.
     */
    WordEntryPos* pos = NULL;
    int npos = 0;    /* elements of pos used */
    int posalen = 0; /* allocated size of pos */

    while (1) {
        if (state_code == WAITWORD) {
            if (*(state->prsbuf) == '\0') {
                return false;
            } else if (t_iseq(state->prsbuf, '\'')) {
                state_code = WAITENDCMPLX;
            } else if (t_iseq(state->prsbuf, '\\')) {
                state_code = WAITNEXTCHAR;
                old_state = WAITENDWORD;
            } else if (state->oprisdelim && ISOPERATOR(state->prsbuf)) {
                PRSSYNTAXERROR;
            } else if (!t_isspace(state->prsbuf)) {
                COPYCHAR(cur_pos, state->prsbuf);
                cur_pos += pg_mblen(state->prsbuf);
                state_code = WAITENDWORD;
            }
        } else if (state_code == WAITNEXTCHAR) {
            if (*(state->prsbuf) == '\0') {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("there is no escaped character: \"%s\"", state->bufstart)));
            } else {
                RESIZEPRSBUF;
                COPYCHAR(cur_pos, state->prsbuf);
                cur_pos += pg_mblen(state->prsbuf);
                Assert(old_state != 0);
                state_code = old_state;
            }
        } else if (state_code == WAITENDWORD) {
            if (t_iseq(state->prsbuf, '\\')) {
                state_code = WAITNEXTCHAR;
                old_state = WAITENDWORD;
            } else if (t_isspace(state->prsbuf) || *(state->prsbuf) == '\0' ||
                       (state->oprisdelim && ISOPERATOR(state->prsbuf))) {
                RESIZEPRSBUF;
                if (cur_pos == state->word) {
                    PRSSYNTAXERROR;
                }
                *(cur_pos) = '\0';
                RETURN_TOKEN;
            } else if (t_iseq(state->prsbuf, ':')) {
                if (cur_pos == state->word) {
                    PRSSYNTAXERROR;
                }
                *(cur_pos) = '\0';
                if (state->oprisdelim) {
                    RETURN_TOKEN;
                } else {
                    state_code = INPOSINFO;
                }
            } else {
                RESIZEPRSBUF;
                COPYCHAR(cur_pos, state->prsbuf);
                cur_pos += pg_mblen(state->prsbuf);
            }
        } else if (state_code == WAITENDCMPLX) {
            if (t_iseq(state->prsbuf, '\'')) {
                state_code = WAITCHARCMPLX;
            } else if (t_iseq(state->prsbuf, '\\')) {
                state_code = WAITNEXTCHAR;
                old_state = WAITENDCMPLX;
            } else if (*(state->prsbuf) == '\0')
                PRSSYNTAXERROR;
            else {
                RESIZEPRSBUF;
                COPYCHAR(cur_pos, state->prsbuf);
                cur_pos += pg_mblen(state->prsbuf);
            }
        } else if (state_code == WAITCHARCMPLX) {
            if (t_iseq(state->prsbuf, '\'')) {
                RESIZEPRSBUF;
                COPYCHAR(cur_pos, state->prsbuf);
                cur_pos += pg_mblen(state->prsbuf);
                state_code = WAITENDCMPLX;
            } else {
                RESIZEPRSBUF;
                *(cur_pos) = '\0';
                if (cur_pos == state->word) {
                    PRSSYNTAXERROR;
                }
                if (state->oprisdelim) {
                    RETURN_TOKEN;
                } else {
                    state_code = WAITPOSINFO;
                }
                continue; /* recheck current character */
            }
        } else if (state_code == WAITPOSINFO) {
            if (t_iseq(state->prsbuf, ':')) {
                state_code = INPOSINFO;
            } else {
                RETURN_TOKEN;
            }
        } else if (state_code == INPOSINFO) {
            if (t_isdigit(state->prsbuf)) {
                if (posalen == 0) {
                    posalen = POSA_LEN_BASE;
                    pos = (WordEntryPos*)palloc(sizeof(WordEntryPos) * posalen);
                    npos = 0;
                } else if (npos + 1 >= posalen) {
                    posalen *= 2;
                    pos = (WordEntryPos*)repalloc(pos, sizeof(WordEntryPos) * posalen);
                }
                npos++;
                WEP_SETPOS(pos[npos - 1], (unsigned int)LIMITPOS(atoi(state->prsbuf)));
                /* we cannot get here in tsquery, so no need for 2 errmsgs */
                if (WEP_GETPOS(pos[npos - 1]) == 0) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("wrong position info in tsvector: \"%s\"", state->bufstart)));
                }
                WEP_SETWEIGHT(pos[npos - 1], 0);
                state_code = WAITPOSDELIM;
            } else {
                PRSSYNTAXERROR;
            }
        } else if (state_code == WAITPOSDELIM) {
            if (t_iseq(state->prsbuf, ',')) {
                state_code = INPOSINFO;
            }
            else if (t_iseq(state->prsbuf, 'a') || t_iseq(state->prsbuf, 'A') || t_iseq(state->prsbuf, '*')) {
                if (WEP_GETWEIGHT(pos[npos - 1])) {
                    PRSSYNTAXERROR;
                }
                WEP_SETWEIGHT(pos[npos - 1], 3);
            } else if (t_iseq(state->prsbuf, 'b') || t_iseq(state->prsbuf, 'B')) {
                if (WEP_GETWEIGHT(pos[npos - 1])) {
                    PRSSYNTAXERROR;
                }
                WEP_SETWEIGHT(pos[npos - 1], 2);
            } else if (t_iseq(state->prsbuf, 'c') || t_iseq(state->prsbuf, 'C')) {
                if (WEP_GETWEIGHT(pos[npos - 1])) {
                    PRSSYNTAXERROR;
                }
                WEP_SETWEIGHT(pos[npos - 1], 1);
            } else if (t_iseq(state->prsbuf, 'd') || t_iseq(state->prsbuf, 'D')) {
                if (WEP_GETWEIGHT(pos[npos - 1])) {
                    PRSSYNTAXERROR;
                }
                WEP_SETWEIGHT(pos[npos - 1], 0);
            } else if (t_isspace(state->prsbuf) || *(state->prsbuf) == '\0') {
                RETURN_TOKEN;
            } else if (!t_isdigit(state->prsbuf)) {
                PRSSYNTAXERROR;
            }
        } else { /* internal error */
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized state in gettoken_tsvector: %d", state_code)));
        }
        /* get next char */
        state->prsbuf += pg_mblen(state->prsbuf);
    }

    return false;
}
