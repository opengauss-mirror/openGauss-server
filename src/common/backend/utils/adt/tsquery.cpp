/* -------------------------------------------------------------------------
 *
 * tsquery.c
 *	  I/O functions for tsquery
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/tsquery.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "tsearch/ts_locale.h"
#include "tsearch/ts_utils.h"
#include "utils/builtins.h"
#include "utils/memutils.h"


/* FTS operator priorities, see ts_type.h */
const int tsearch_op_priority[OP_COUNT] = {3, 2, 1};

struct TSQueryParserStateData {
    /* State for gettoken_query */
    char* buffer; /* entire string we are scanning */
    char* buf;    /* current scan point */
    int state;
    int count; /* nesting count, incremented by (,
                * decremented by ) */

    /* polish (prefix) notation in list, filled in by push* functions */
    List* polstr;

    /*
     * Strings from operands are collected in op. curop is a pointer to the
     * end of used space of op.
     */
    char* op;
    char* curop;
    int lenop;  /* allocated size of op */
    int sumlen; /* used size of op */

    /* state for value's parser */
    TSVectorParseState valstate;
};

/* parser's states */
#define WAITOPERAND 1
#define WAITOPERATOR 2
#define WAITFIRSTOPERAND 3
#define WAITSINGLEOPERAND 4

/*
 * subroutine to parse the modifiers (weight and prefix flag currently)
 * part, like ':1AB' of a query.
 */
static char* get_modifiers(const char* buf, int16* weight, bool* prefix)
{
    *weight = 0;
    *prefix = false;

    if (!t_iseq(buf, ':'))
        return (char*)buf;

    buf++;
    while (*buf && pg_mblen(buf) == 1) {
        switch (*buf) {
            case 'a':
            case 'A':
                *weight = (uint16)(*weight) | (1 << 3);
                break;
            case 'b':
            case 'B':
                *weight = (uint16)(*weight) | (1 << 2);
                break;
            case 'c':
            case 'C':
                *weight = (uint16)(*weight) | (1 << 1);
                break;
            case 'd':
            case 'D':
                *weight = (uint16)(*weight) | 1;
                break;
            case '*':
                *prefix = true;
                break;
            default:
                return (char*)buf;
        }
        buf++;
    }

    return (char*)buf;
}

/*
 * token types for parsing
 */
typedef enum { PT_END = 0, PT_ERR = 1, PT_VAL = 2, PT_OPR = 3, PT_OPEN = 4, PT_CLOSE = 5 } ts_tokentype;

/*
 * get token from query string
 *
 * *operator is filled in with OP_* when return values is PT_OPR
 * *strval, *lenval and *weight are filled in when return value is PT_VAL
 */
static ts_tokentype gettoken_query(
    TSQueryParserState state, int8* opera, int* lenval, char** strval, int16* weight, bool* prefix)
{
    *weight = 0;
    *prefix = false;

    while (1) {
        switch (state->state) {
            case WAITFIRSTOPERAND:
            case WAITOPERAND:
                if (t_iseq(state->buf, '!')) {
                    (state->buf)++; /* can safely ++, t_iseq guarantee
                                     * that pg_mblen()==1 */
                    *opera = OP_NOT;
                    state->state = WAITOPERAND;
                    return PT_OPR;
                } else if (t_iseq(state->buf, '(')) {
                    state->count++;
                    (state->buf)++;
                    state->state = WAITOPERAND;
                    return PT_OPEN;
                } else if (t_iseq(state->buf, ':')) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("syntax error in tsquery: \"%s\"", state->buffer)));
                } else if (!t_isspace(state->buf)) {
                    /*
                     * We rely on the tsvector parser to parse the value for
                     * us
                     */
                    reset_tsvector_parser(state->valstate, state->buf);
                    if (gettoken_tsvector(state->valstate, strval, lenval, NULL, NULL, &state->buf)) {
                        state->buf = get_modifiers(state->buf, weight, prefix);
                        state->state = WAITOPERATOR;
                        return PT_VAL;
                    } else if (state->state == WAITFIRSTOPERAND)
                        return PT_END;
                    else
                        ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("no operand in tsquery: \"%s\"", state->buffer)));
                }
                break;
            case WAITOPERATOR:
                if (t_iseq(state->buf, '&')) {
                    state->state = WAITOPERAND;
                    *opera = OP_AND;
                    (state->buf)++;
                    return PT_OPR;
                }
                if (t_iseq(state->buf, '|')) {
                    state->state = WAITOPERAND;
                    *opera = OP_OR;
                    (state->buf)++;
                    return PT_OPR;
                } else if (t_iseq(state->buf, ')')) {
                    (state->buf)++;
                    state->count--;
                    return (state->count < 0) ? PT_ERR : PT_CLOSE;
                } else if (*(state->buf) == '\0')
                    return (state->count) ? PT_ERR : PT_END;
                else if (!t_isspace(state->buf))
                    return PT_ERR;
                break;
            case WAITSINGLEOPERAND:
                if (*(state->buf) == '\0')
                    return PT_END;
                *strval = state->buf;
                *lenval = strlen(state->buf);
                state->buf += strlen(state->buf);
                state->count++;
                return PT_VAL;
            default:
                return PT_ERR;
                break;
        }
        state->buf += pg_mblen(state->buf);
    }
    return PT_END;
}

/*
 * Push an operator to state->polstr
 */
void pushOperator(TSQueryParserState state, int8 oper)
{
    QueryOperator* tmp = NULL;

    Assert(oper == OP_NOT || oper == OP_AND || oper == OP_OR);

    tmp = (QueryOperator*)palloc0(sizeof(QueryOperator));
    tmp->type = QI_OPR;
    tmp->oper = oper;
    /* left is filled in later with findoprnd */

    state->polstr = lcons(tmp, state->polstr);
}

static void pushValue_internal(
    TSQueryParserState state, pg_crc32 valcrc, int distance, int lenval, int weight, bool prefix)
{
    QueryOperand* tmp = NULL;

    if (distance >= MAXSTRPOS)
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("value is too big in tsquery: \"%s\"", state->buffer)));
    if (lenval >= MAXSTRLEN)
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("operand is too long in tsquery: \"%s\"", state->buffer)));

    tmp = (QueryOperand*)palloc0(sizeof(QueryOperand));
    tmp->type = QI_VAL;
    tmp->weight = weight;
    tmp->prefix = prefix;
    tmp->valcrc = (int32)valcrc;
    tmp->length = lenval;
    tmp->distance = distance;

    state->polstr = lcons(tmp, state->polstr);
}

/*
 * Push an operand to state->polstr.
 *
 * strval must point to a string equal to state->curop. lenval is the length
 * of the string.
 */
void pushValue(TSQueryParserState state, const char* strval, int lenval, int2 weight, bool prefix)
{
    pg_crc32 valcrc;
    errno_t rc;

    if (lenval >= MAXSTRLEN)
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("word is too long in tsquery: \"%s\"", state->buffer)));

    INIT_CRC32(valcrc);
    COMP_CRC32(valcrc, strval, lenval);
    FIN_CRC32(valcrc);
    pushValue_internal(state, valcrc, state->curop - state->op, lenval, weight, prefix);

    /* append the value string to state.op, enlarging buffer if needed first */
    while (state->curop - state->op + lenval + 1 >= state->lenop) {
        int used = state->curop - state->op;

        state->lenop *= 2;
        state->op = (char*)repalloc((void*)state->op, state->lenop);
        state->curop = state->op + used;
    }
    rc = memcpy_s((void*)state->curop, (uint4)lenval, (void*)strval, (uint4)lenval);
    securec_check(rc, "\0", "\0");

    state->curop += lenval;
    *(state->curop) = '\0';
    state->curop++;
    state->sumlen += lenval + 1 /* \0 */;
}

/*
 * Push a stopword placeholder to state->polstr
 */
void pushStop(TSQueryParserState state)
{
    QueryOperand* tmp = NULL;

    tmp = (QueryOperand*)palloc0(sizeof(QueryOperand));
    tmp->type = QI_VALSTOP;

    state->polstr = lcons(tmp, state->polstr);
}

#define STACKDEPTH 32

/*
 * Make polish (prefix) notation of query.
 *
 * See parse_tsquery for explanation of pushval.
 */
static void makepol(TSQueryParserState state, PushFunction pushval, Datum opaque)
{
    int8 opera = 0;
    ts_tokentype type;
    int lenval = 0;
    char* strval = NULL;
    int8 opstack[STACKDEPTH];
    int lenstack = 0;
    int16 weight = 0;
    bool prefix = false;

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    while ((type = gettoken_query(state, &opera, &lenval, &strval, &weight, &prefix)) != PT_END) {
        switch (type) {
            case PT_VAL:
                pushval(opaque, state, strval, lenval, weight, prefix);
                while (lenstack && (opstack[lenstack - 1] == OP_AND || opstack[lenstack - 1] == OP_NOT)) {
                    lenstack--;
                    pushOperator(state, opstack[lenstack]);
                }
                break;
            case PT_OPR:
                if (lenstack && opera == OP_OR)
                    pushOperator(state, OP_OR);
                else {
                    if (lenstack == STACKDEPTH) /* internal error */
                        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("tsquery stack too small")));
                    opstack[lenstack] = opera;
                    lenstack++;
                }
                break;
            case PT_OPEN:
                makepol(state, pushval, opaque);

                while (lenstack && (opstack[lenstack - 1] == OP_AND || opstack[lenstack - 1] == OP_NOT)) {
                    lenstack--;
                    pushOperator(state, opstack[lenstack]);
                }
                break;
            case PT_CLOSE:
                while (lenstack) {
                    lenstack--;
                    pushOperator(state, opstack[lenstack]);
                };
                return;
            case PT_ERR:
            default:
                ereport(
                    ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("syntax error in tsquery: \"%s\"", state->buffer)));
        }
    }
    while (lenstack) {
        lenstack--;
        pushOperator(state, opstack[lenstack]);
    }
}

static void findoprnd_recurse(QueryItem* ptr, uint32* pos, int nnodes)
{
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if (*pos >= (uint32)nnodes)
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("malformed tsquery: operand not found")));

    if (ptr[*pos].type == QI_VAL || ptr[*pos].type == QI_VALSTOP) /* need to handle VALSTOP here, they
                                                                   * haven't been cleaned away yet. */
    {
        (*pos)++;
    } else {
        Assert(ptr[*pos].type == QI_OPR);

        if (ptr[*pos].qoperator.oper == OP_NOT) {
            ptr[*pos].qoperator.left = 1;
            (*pos)++;
            findoprnd_recurse(ptr, pos, nnodes);
        } else {
            QueryOperator* curitem = &ptr[*pos].qoperator;
            int tmp = *pos;

            Assert(curitem->oper == OP_AND || curitem->oper == OP_OR);

            (*pos)++;
            findoprnd_recurse(ptr, pos, nnodes);
            curitem->left = *pos - tmp;
            findoprnd_recurse(ptr, pos, nnodes);
        }
    }
}

/*
 * Fills in the left-fields previously left unfilled. The input
 * QueryItems must be in polish (prefix) notation.
 */
static void findoprnd(QueryItem* ptr, int size)
{
    uint32 pos;

    pos = 0;
    findoprnd_recurse(ptr, &pos, size);

    if (pos != (uint32)size)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("malformed tsquery: extra nodes")));
}

/*
 * Each value (operand) in the query is be passed to pushval. pushval can
 * transform the simple value to an arbitrarily complex expression using
 * pushValue and pushOperator. It must push a single value with pushValue,
 * a complete expression with all operands, or a a stopword placeholder
 * with pushStop, otherwise the prefix notation representation will be broken,
 * having an operator with no operand.
 *
 * opaque is passed on to pushval as is, pushval can use it to store its
 * private state.
 *
 * The returned query might contain QI_STOPVAL nodes. The caller is responsible
 * for cleaning them up (with clean_fakeval)
 */
TSQuery parse_tsquery(char* buf, PushFunction pushval, Datum opaque, bool isplain)
{
    struct TSQueryParserStateData state;
    int i;
    TSQuery query;
    int commonlen;
    QueryItem* ptr = NULL;
    ListCell* cell = NULL;

    /* init state */
    state.buffer = buf;
    state.buf = buf;
    state.state = (isplain) ? WAITSINGLEOPERAND : WAITFIRSTOPERAND;
    state.count = 0;
    state.polstr = NIL;

    /* init value parser's state */
    state.valstate = init_tsvector_parser(state.buffer, true, true);

    /* init list of operand */
    state.sumlen = 0;
    state.lenop = 64;
    state.curop = state.op = (char*)palloc(state.lenop);
    *(state.curop) = '\0';

    /* parse query & make polish notation (postfix, but in reverse order) */
    makepol(&state, pushval, opaque);

    close_tsvector_parser(state.valstate);

    if (list_length(state.polstr) == 0) {
        ereport(NOTICE, (errmsg("text-search query doesn't contain lexemes: \"%s\"", state.buffer)));
        query = (TSQuery)palloc(HDRSIZETQ);
        SET_VARSIZE(query, HDRSIZETQ);
        query->size = 0;
        return query;
    }

    if (TSQUERY_TOO_BIG(list_length(state.polstr), state.sumlen))
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("tsquery is too large")));
    commonlen = COMPUTESIZE(list_length(state.polstr), state.sumlen);

    /* Pack the QueryItems in the final TSQuery struct to return to caller */
    query = (TSQuery)palloc0(commonlen);
    SET_VARSIZE(query, commonlen);
    query->size = list_length(state.polstr);
    ptr = GETQUERY(query);
    int prt_len = commonlen - HDRSIZETQ;

    /* Copy QueryItems to TSQuery */
    i = 0;
    errno_t rc = 0;
    foreach (cell, state.polstr) {
        QueryItem* item = (QueryItem*)lfirst(cell);

        switch (item->type) {
            case QI_VAL:
                rc = memcpy_s(&ptr[i], sizeof(QueryItem), item, sizeof(QueryOperand));
                securec_check(rc, "\0", "\0");
                break;
            case QI_VALSTOP:
                ptr[i].type = QI_VALSTOP;
                break;
            case QI_OPR:
                rc = memcpy_s(&ptr[i], sizeof(QueryItem), item, sizeof(QueryOperator));
                securec_check(rc, "\0", "\0");
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized QueryItem type: %d", item->type)));
                break;
        }
        i++;
    }

    /* Copy all the operand strings to TSQuery */
    if (state.sumlen > 0) {
        int residual_length = prt_len - ((TSQuery)(query))->size * sizeof(QueryItem);
        Assert(residual_length == state.sumlen);
        rc = memcpy_s((void*)GETOPERAND(query), residual_length, (void*)state.op, state.sumlen);
        securec_check(rc, "\0", "\0");
    }
    pfree_ext(state.op);

    /* Set left operand pointers for every operator. */
    findoprnd(ptr, query->size);

    return query;
}

static void pushval_asis(
    Datum opaque, TSQueryParserState state, const char* strval, int lenval, int16 weight, bool prefix)
{
    pushValue(state, strval, lenval, weight, prefix);
}

/*
 * in without morphology
 */
Datum tsqueryin(PG_FUNCTION_ARGS)
{
    char* in = PG_GETARG_CSTRING(0);

    PG_RETURN_TSQUERY(parse_tsquery(in, pushval_asis, PointerGetDatum(NULL), false));
}

/*
 * out function
 */
typedef struct {
    QueryItem* curpol;
    char* buf;
    char* cur;
    char* op;
    int buflen;
} INFIX;

/* Makes sure inf->buf is large enough for adding 'addsize' bytes */
#define RESIZEBUF(inf, addsize)                                          \
    while (((inf)->cur - (inf)->buf) + (addsize) + 1 >= (inf)->buflen) { \
        int len = (inf)->cur - (inf)->buf;                               \
        (inf)->buflen *= 2;                                              \
        (inf)->buf = (char*)repalloc((void*)(inf)->buf, (inf)->buflen);  \
        (inf)->cur = (inf)->buf + len;                                   \
    }

/*
 * recursive walk on tree and print it in
 * infix (human-readable) view
 */
static void infix(INFIX *in, int parentPriority, bool rightPhraseOp)
{
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    errno_t rc = 0;

    if (in->curpol->type == QI_VAL) {
        QueryOperand* curpol = &in->curpol->qoperand;
        char* op = in->op + curpol->distance;
        int clen;

        RESIZEBUF(in, curpol->length * (pg_database_encoding_max_length() + 1) + 2 + 6);
        *(in->cur) = '\'';
        in->cur++;
        while (*op) {
            if (t_iseq(op, '\'')) {
                *(in->cur) = '\'';
                in->cur++;
            } else if (t_iseq(op, '\\')) {
                *(in->cur) = '\\';
                in->cur++;
            }
            COPYCHAR(in->cur, op);

            clen = pg_mblen(op);
            op += clen;
            in->cur += clen;
        }
        *(in->cur) = '\'';
        in->cur++;
        if (curpol->weight || curpol->prefix) {
            *(in->cur) = ':';
            in->cur++;
            if (curpol->prefix) {
                *(in->cur) = '*';
                in->cur++;
            }
            if (curpol->weight & (1 << 3)) {
                *(in->cur) = 'A';
                in->cur++;
            }
            if (curpol->weight & (1 << 2)) {
                *(in->cur) = 'B';
                in->cur++;
            }
            if (curpol->weight & (1 << 1)) {
                *(in->cur) = 'C';
                in->cur++;
            }
            if (curpol->weight & 1) {
                *(in->cur) = 'D';
                in->cur++;
            }
        }
        *(in->cur) = '\0';
        in->curpol++;
    } else if (in->curpol->qoperator.oper == OP_NOT) {
        int priority = QO_PRIORITY(in->curpol);

        if (priority < parentPriority) {
            RESIZEBUF(in, 2);
            sprintf(in->cur, "( ");
            in->cur = strchr(in->cur, '\0');
        }
        RESIZEBUF(in, 1);
        *(in->cur) = '!';
        in->cur++;
        *(in->cur) = '\0';
        in->curpol++;

        infix(in, priority, false);
        if (priority < parentPriority) {
            RESIZEBUF(in, 2);
            rc = sprintf_s(in->cur, in->buflen - (in->cur - in->buf), " )");
            securec_check_ss(rc, "\0", "\0");
            in->cur = strchr(in->cur, '\0');
        }
    } else {
        int8  op = in->curpol->qoperator.oper;
        int priority = QO_PRIORITY(in->curpol);
        INFIX nrm;
        bool needParenthesis = false;

        in->curpol++;
        if (priority < parentPriority || rightPhraseOp) {
            needParenthesis = true;
            RESIZEBUF(in, 2);
            rc = sprintf_s(in->cur, in->buflen - (in->cur - in->buf), "( ");
            securec_check_ss(rc, "\0", "\0");
            in->cur = strchr(in->cur, '\0');
        }

        nrm.curpol = in->curpol;
        nrm.op = in->op;
        nrm.buflen = 16;
        nrm.cur = nrm.buf = (char*)palloc(sizeof(char) * nrm.buflen);

        /* get right operand */
        infix(&nrm, priority, false);

        /* get & print left operand */
        in->curpol = nrm.curpol;
        infix(in, priority, false);

        /* print operator & right operand */
        RESIZEBUF(in, 3 + (nrm.cur - nrm.buf));
        switch (op) {
            case OP_OR:
                rc = sprintf_s(in->cur, in->buflen - (in->cur - in->buf), " | %s", nrm.buf);
                securec_check_ss(rc, "\0", "\0");
                break;
            case OP_AND:
                rc = sprintf_s(in->cur, in->buflen - (in->cur - in->buf), " & %s", nrm.buf);
                securec_check_ss(rc, "\0", "\0");
                break;
            default:
                /* OP_NOT is handled in above if-branch */
                ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized operator type: %d", op)));
        }
        in->cur = strchr(in->cur, '\0');
        pfree_ext(nrm.buf);

        if (needParenthesis) {
            RESIZEBUF(in, 2);
            rc = sprintf_s(in->cur, in->buflen - (in->cur - in->buf), " )");
            securec_check_ss(rc, "\0", "\0");
            in->cur = strchr(in->cur, '\0');
        }
    }
}

Datum tsqueryout(PG_FUNCTION_ARGS)
{
    TSQuery query = PG_GETARG_TSQUERY(0);
    INFIX nrm;

    if (query->size == 0) {
        char* b = (char*)palloc(1);

        *b = '\0';
        PG_RETURN_POINTER(b);
    }
    nrm.curpol = GETQUERY(query);
    nrm.buflen = 32;
    nrm.cur = nrm.buf = (char*)palloc(sizeof(char) * nrm.buflen);
    *(nrm.cur) = '\0';
    nrm.op = GETOPERAND(query);
    infix(&nrm, -1, false);

    PG_FREE_IF_COPY(query, 0);
    PG_RETURN_CSTRING(nrm.buf);
}

/*
 * Binary Input / Output functions. The binary format is as follows:
 *
 * uint32	 number of operators/operands in the query
 *
 * Followed by the operators and operands, in prefix notation. For each
 * operand:
 *
 * uint8	type, QI_VAL
 * uint8	weight
 *			operand text in client encoding, null-terminated
 * uint8	prefix
 *
 * For each operator:
 * uint8	type, QI_OPR
 * uint8	operator, one of OP_AND, OP_OR, OP_NOT.
 */
Datum tsquerysend(PG_FUNCTION_ARGS)
{
    TSQuery query = PG_GETARG_TSQUERY(0);
    StringInfoData buf;
    int i;
    QueryItem* item = GETQUERY(query);

    pq_begintypsend(&buf);

    pq_sendint32(&buf, query->size);
    for (i = 0; i < query->size; i++) {
        pq_sendint8(&buf, item->type);

        switch (item->type) {
            case QI_VAL:
                pq_sendint8(&buf, item->qoperand.weight);
                pq_sendint8(&buf, item->qoperand.prefix);
                pq_sendstring(&buf, GETOPERAND(query) + item->qoperand.distance);
                break;
            case QI_OPR:
                pq_sendint8(&buf, item->qoperator.oper);
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized tsquery node type: %d", item->type)));
        }
        item++;
    }

    PG_FREE_IF_COPY(query, 0);

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum tsqueryrecv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    TSQuery query;
    int i, len;
    QueryItem* item = NULL;
    int datalen;
    char* ptr = NULL;
    uint32 size;
    const char** operands = NULL;

    size = pq_getmsgint(buf, sizeof(uint32));
    if (size > (MaxAllocSize - (Size)HDRSIZETQ) / sizeof(QueryItem)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid size of tsquery")));
    }

    /* Allocate space to temporarily hold operand strings */
    operands = (const char**)palloc(size * sizeof(char*));

    /* Allocate space for all the QueryItems. */
    len = HDRSIZETQ + sizeof(QueryItem) * size;
    query = (TSQuery)palloc0(len);
    query->size = size;
    item = GETQUERY(query);

    datalen = 0;
    for (i = 0; i < (int)size; i++) {
        item->type = (int8)pq_getmsgint(buf, sizeof(int8));

        if (item->type == QI_VAL) {
            size_t val_len; /* length after recoding to server encoding */
            uint8 weight;
            uint8 prefix;
            const char* val = NULL;
            pg_crc32 valcrc;

            weight = (uint8)pq_getmsgint(buf, sizeof(uint8));
            prefix = (uint8)pq_getmsgint(buf, sizeof(uint8));
            val = pq_getmsgstring(buf);
            val_len = strlen(val);

            /* Sanity checks */

            if (weight > 0xF)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG), errmsg("invalid tsquery: invalid weight bitmap")));

            if (val_len > MAXSTRLEN)
                ereport(
                    ERROR, (errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG), errmsg("invalid tsquery: operand too long")));

            if (datalen > MAXSTRPOS)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
                        errmsg("invalid tsquery: total operand length exceeded")));

            /* Looks valid. */

            INIT_CRC32(valcrc);
            COMP_CRC32(valcrc, val, val_len);
            FIN_CRC32(valcrc);

            item->qoperand.weight = weight;
            item->qoperand.prefix = (prefix) ? true : false;
            item->qoperand.valcrc = (int32)valcrc;
            item->qoperand.length = val_len;
            item->qoperand.distance = datalen;

            /*
             * Operand strings are copied to the final struct after this loop;
             * here we just collect them to an array
             */
            operands[i] = val;

            datalen += val_len + 1; /* + 1 for the '\0' terminator */
        } else if (item->type == QI_OPR) {
            int8 oper;

            oper = (int8)pq_getmsgint(buf, sizeof(int8));
            if (oper != OP_NOT && oper != OP_OR && oper != OP_AND)
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("invalid tsquery: unrecognized operator type %d", (int)oper)));
            if (i == int(size - 1))
                ereport(
                    ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("invalid pointer to right operand")));

            item->qoperator.oper = oper;
        } else
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized tsquery node type: %d", item->type)));

        item++;
    }

    /* Enlarge buffer to make room for the operand values. */
    query = (TSQuery)repalloc(query, (Size)len + (Size)datalen);
    item = GETQUERY(query);
    ptr = GETOPERAND(query);
    if (len != (int)HDRSIZETQ + (int)(((TSQuery)(query))->size * sizeof(QueryItem))) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid len: %d", len)));
    }
    int ptr_len = datalen;

    /*
     * Fill in the left-pointers. Checks that the tree is well-formed as a
     * side-effect.
     */
    findoprnd(item, size);

    errno_t rc = 0;
    /* Copy operands to output struct */
    for (i = 0; i < (int)size; i++) {
        if (item->type == QI_VAL) {
            rc = memcpy_s(ptr, ptr_len, operands[i], item->qoperand.length + 1);
            securec_check(rc, "\0", "\0");
            ptr += item->qoperand.length + 1;
            ptr_len -= (item->qoperand.length + 1);
        }
        item++;
    }

    pfree_ext(operands);

    Assert(ptr - GETOPERAND(query) == datalen);

    SET_VARSIZE(query, len + datalen);

    PG_RETURN_TSVECTOR(query);
}

/*
 * debug function, used only for view query
 * which will be executed in non-leaf pages in index
 */
Datum tsquerytree(PG_FUNCTION_ARGS)
{
    ts_check_feature_disable();
    TSQuery query = PG_GETARG_TSQUERY(0);
    INFIX nrm;
    text* res = NULL;
    QueryItem* q = NULL;
    int len;

    if (query->size == 0) {
        res = (text*)palloc(VARHDRSZ);
        SET_VARSIZE(res, VARHDRSZ);
        PG_RETURN_POINTER(res);
    }

    q = clean_NOT(GETQUERY(query), &len);

    if (q == NULL) {
        res = cstring_to_text("T");
    } else {
        nrm.curpol = q;
        nrm.buflen = 32;
        nrm.cur = nrm.buf = (char*)palloc(sizeof(char) * nrm.buflen);
        *(nrm.cur) = '\0';
        nrm.op = GETOPERAND(query);
        infix(&nrm, -1, false);
        res = cstring_to_text_with_len(nrm.buf, nrm.cur - nrm.buf);
        pfree_ext(nrm.buf);
        pfree_ext(q);
    }

    PG_FREE_IF_COPY(query, 0);

    PG_RETURN_TEXT_P(res);
}
