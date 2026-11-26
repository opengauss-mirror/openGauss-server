/*-------------------------------------------------------------------------
 *
 * jsonpath.cpp
 *     Input/output and supporting routines for jsonpath
 *
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * Copyright (c) 2019-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/common/backend/utils/adt/jsonpath.cpp
 *
 *-------------------------------------------------------------------------
 */

 /* jsonpath expression is a chain of path items.  First path item is $, $var,
 * literal or arithmetic expression.  Subsequent path items are accessors
 * (.key, .*, [subscripts], [*]), filters (? (predicate)) and methods (.type(),
 * .size() etc).
 *
 * For instance, structure of path items for simple expression:
 *
 *        $.a[*].type()
 *
 * is pretty evident:
 *
 *        $ => .a => [*] => .type()
 *
 * Some path items such as arithmetic operations, predicates or array
 * subscripts may comprise subtrees.  For instance, more complex expression
 *
 *        ($.a + $[1 to 5, 7] ? (@ > 3).double()).type()
 *
 * have following structure of path items:
 *
 *              +  =>  .type()
 *          ___/ \___
 *         /           \
 *        $ => .a     $  =>  []  =>    ?  =>  .double()
 *                          _||_        |
 *                         /      \     >
 *                        to      to   / \
 *                       / \      /   @   3
 *                      1   5  7
 *
 * Binary encoding of jsonpath constitutes a sequence of 4-bytes aligned
 * variable-length path items connected by links.  Every item has a header
 * consisting of item type (enum JsonPathItemType) and offset of next item
 * (zero means no next item).  After the header, item may have payload
 * depending on item type.  For instance, payload of '.key' accessor item is
 * length of key name and key name itself.  Payload of '>' arithmetic operator
 * item is offsets of right and left operands.
 *
 * So, binary representation of sample expression above is:
 * (bottom arrows are next links, top lines are argument links)
 *
 *                                  _____
 *         _____                  ___/____ \                __
 *      _ /_      \         _____/__/____ \ \       __    _ /_ \
 *     / /  \    \       /    /  /     \ \ \       /  \  / /  \ \
 * +(LR)  $ .a    $  [](* to *, * to *) 1 5 7 ?(A)  >(LR)   @ 3 .double() .type()
 * |      |  ^    |  ^|                         ^|                      ^           ^
 * |      |__|    |__||________________________||___________________|           |
 * |_______________________________________________________________________|
 *
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/formatting.h"
#include "utils/json.h"
#include "utils/builtins.h"
#include "utils/jsonpath.h"

typedef struct FlattenJsonPathContext {
    int32 pos;
    int32 chld;
} FlattenJsonPathContext;

static Datum jsonPathFromCstring(char *in, int len, struct Node *escontext);
static char* jsonPathToCstring(StringInfo out, JsonPath *in,
                               int estimatedLen);
static void flattenJsonPathParseItem(StringInfo buf, int *result,
                                     JsonPathParseItem *item,
                                     int nestingLevel, bool insideArraySubscript);
static void flattenIndexArray(StringInfo buf, int32 pos, JsonPathParseItem* item,
    int nestingLevel);
static void flattenUnaryExpr(StringInfo buf, FlattenJsonPathContext* fcxt,
    JsonPathParseItem* item, int nestingLevel, bool insideArraySubscript);
static void flattenBinaryExpr(StringInfo buf, FlattenJsonPathContext* fcxt,
    JsonPathParseItem* item, int nestingLevel, bool insideArraySubscript);
static void flattenLikeRegex(StringInfo buf, FlattenJsonPathContext* fcxt,
    JsonPathParseItem* item, int nestingLevel, bool insideArraySubscript);
static void alignStringInfoInt(StringInfo buf);
static int32 reserveSpaceForItemPointer(StringInfo buf);
static int    operationPriority(JsonPathItemType op);

static void printJsonPathItem(StringInfo buf, JsonPathItem *v, bool printBrackets);
static void printAnyKey(StringInfo buf, JsonPathItem* v);
static void printKey(StringInfo buf, JsonPathItem* v);
static void printBool(StringInfo buf, JsonPathItem* v);
static void printNumeric(StringInfo buf, JsonPathItem* v);
static void printIndexArray(StringInfo buf, JsonPathItem* v);
static void printAny(StringInfo buf, JsonPathItem* v);
static void printBinaryExpr(StringInfo buf, JsonPathItem* v, bool printBrackets, JsonPathItem* elem);
static void printPlusMinus(StringInfo buf, JsonPathItem* v, bool printBrackets, JsonPathItem* elem);
static void printNot(StringInfo buf, JsonPathItem* v, JsonPathItem* elem);
static void printFilter(StringInfo buf, JsonPathItem* v, JsonPathItem* elem);
static void printIsUnknown(StringInfo buf, JsonPathItem* v, JsonPathItem* elem);
static void printExists(StringInfo buf, JsonPathItem* v, JsonPathItem* elem);
static void printLikeRegex(StringInfo buf, JsonPathItem* v, bool printBrackets, JsonPathItem* elem);
static void printDecimal(StringInfo buf, JsonPathItem* v, JsonPathItem* elem);
/**************************** INPUT/OUTPUT ********************************/

/*
 * jsonpath type input function
 */
Datum jsonpath_in(PG_FUNCTION_ARGS)
{
    char *in = PG_GETARG_CSTRING(0);
    int len = strlen(in);

    return jsonPathFromCstring(in, len, fcinfo->context);
}

/*
 * jsonpath type recv function
 *
 * The type is sent as text in binary mode, so this is almost the same
 * as the input function, but it's prefixed with a version number so we
 * can change the binary format sent in future if necessary. For now,
 * only version 1 is supported.
 */
Datum jsonpath_recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    int version = pq_getmsgint(buf, 1);
    char *str;
    int nbytes;

    str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);

    return jsonPathFromCstring(str, nbytes, NULL);
}

/*
 * jsonpath type output function
 */
Datum jsonpath_out(PG_FUNCTION_ARGS)
{
    JsonPath *in = PG_GETARG_JSONPATH_P(0);

    PG_RETURN_CSTRING(jsonPathToCstring(NULL, in, VARSIZE(in)));
}

/*
 * jsonpath type send function
 *
 * Just send jsonpath as a version number, then a string of text
 */
Datum jsonpath_send(PG_FUNCTION_ARGS)
{
    JsonPath *in = PG_GETARG_JSONPATH_P(0);
    StringInfoData buf;
    StringInfoData jtext;
    int version = JSONPATH_VERSION;

    initStringInfo(&jtext);
    (void) jsonPathToCstring(&jtext, in, VARSIZE(in));

    pq_begintypsend(&buf);
    pq_sendint8(&buf, version);
    pq_sendtext(&buf, jtext.data, jtext.len);
    pfree(jtext.data);

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

JsonPath* DatumGetJsonPathP(Datum d)
{
    return (JsonPath *) PG_DETOAST_DATUM(d);
}

JsonPath* DatumGetJsonPathPCopy(Datum d)
{
    return (JsonPath *) PG_DETOAST_DATUM_COPY(d);
}

bool jspHasNext(JsonPathItem* jsp)
{
    return ((jsp)->nextPos > 0);
}

bool jspIsScalar(JsonPathItemType type)
{
    return ((type) >= JPI_NULL && (type) <= JPI_BOOL);
}

/*
 * Converts C-string to a jsonpath value.
 *
 * Uses jsonpath parser to turn string into an AST, then
 * flattenJsonPathParseItem() does second pass turning AST into binary
 * representation of jsonpath.
 */
static Datum jsonPathFromCstring(char *in, int len, struct Node *escontext)
{
    JsonPathParseResult *jsonpath = ParseJsonPath(in, len, escontext);
    JsonPath *res;
    StringInfoData buf;

    if (!jsonpath) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"", "jsonpath",
                        in)));
    }

    if (DB_IS_CMPT(A_FORMAT) && jsonpath->expr->type != JPI_ROOT) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for type %s: \"%s\"", "jsonpath",
                        in),
                 errdetail("A_FORMAT jsonpath must start with a dollar sign ($) character")));
    }

    initStringInfo(&buf);

    int estimatedScale = 4;
    enlargeStringInfo(&buf, estimatedScale * len);

    appendStringInfoSpaces(&buf, JSONPATH_HDRSZ);

    flattenJsonPathParseItem(&buf, NULL, jsonpath->expr, 0, false);

    res = (JsonPath *) buf.data;
    SET_VARSIZE(res, buf.len);
    res->header = JSONPATH_VERSION;
    if (jsonpath->lax) {
        res->header |= JSONPATH_LAX;
    }

    PG_RETURN_JSONPATH_P(res);
}

/*
 * Converts jsonpath value to a C-string.
 *
 * If 'out' argument is non-null, the resulting C-string is stored inside the
 * StringBuffer.  The resulting string is always returned.
 */
static char* jsonPathToCstring(StringInfo out, JsonPath *in, int estimatedLen)
{
    StringInfoData buf;
    JsonPathItem v;

    if (!out) {
        out = &buf;
        initStringInfo(out);
    }
    enlargeStringInfo(out, estimatedLen);

    if (!(in->header & JSONPATH_LAX)) {
        appendStringInfoString(out, "strict ");
    }

    JspInit(&v, in);
    printJsonPathItem(out, &v, true);

    return out->data;
}

/*
 * Recursive function converting given jsonpath parse item and all its
 * children into a binary representation.
 * buf: the buffer where the result binary representation output to.
 * result: current child node's binary representation start position in buf.
 * item: the jsonpath item to be converted.
 * nestingLevel: indicates whether it's at root level,
 *      used to prevent illegal usage of JPI_CURRENT.
 * insideArraySubscript: indicates whether it's dealing with an array's subscripts.
 */
static void flattenJsonPathParseItem(StringInfo buf, int *result,
                         JsonPathParseItem *item, int nestingLevel,
                         bool insideArraySubscript)
{
    /* position from beginning of jsonpath data */
    FlattenJsonPathContext fcxt;
    fcxt.pos = buf->len - JSONPATH_HDRSZ;
    fcxt.chld = (result == NULL) ? 0 : *result;
    int32 chld = 0;
    int32 next;
    int argNestingLevel = 0;

    check_stack_depth();
    CHECK_FOR_INTERRUPTS();

    appendStringInfoChar(buf, (char) (item->type));

    /*
     * We align buffer to int32 because a series of int32 values often goes
     * after the header, and we want to read them directly by dereferencing
     * int32 pointer (see jspInitByBuffer()).
     */
    alignStringInfoInt(buf);

    /*
     * Reserve space for next item pointer.  Actual value will be recorded
     * later, after next and children items processing.
     */
    next = reserveSpaceForItemPointer(buf);

    switch (item->type) {
        case JPI_STRING:
        case JPI_VARIABLE:
        case JPI_KEY:
            appendBinaryStringInfo(buf, (char*)(&item->value.string.len),
                                   sizeof(item->value.string.len));
            appendBinaryStringInfo(buf, item->value.string.val,
                                   item->value.string.len);
            appendStringInfoChar(buf, '\0');
            break;
        case JPI_NUMERIC:
            appendBinaryStringInfo(buf, (char*)(item->value.numeric),
                                   VARSIZE(item->value.numeric));
            break;
        case JPI_BOOL:
            appendBinaryStringInfo(buf, (char*)(&item->value.boolean),
                                   sizeof(item->value.boolean));
            break;
        case JPI_AND:
        case JPI_OR:
        case JPI_EQUAL:
        case JPI_NOT_EQUAL:
        case JPI_LESS:
        case JPI_GREATER:
        case JPI_LESS_OR_EQUAL:
        case JPI_GREATER_OR_EQUAL:
        case JPI_ADD:
        case JPI_SUB:
        case JPI_MUL:
        case JPI_DIV:
        case JPI_MOD:
        case JPI_STARTS_WITH:
        case JPI_DECIMAL:
            flattenBinaryExpr(buf, &fcxt, item, nestingLevel, insideArraySubscript);
            break;
        case JPI_LIKE_REGEX:
            flattenLikeRegex(buf, &fcxt, item, nestingLevel, insideArraySubscript);
            break;
        case JPI_FILTER:
        case JPI_NOT:
        case JPI_IS_UNKNOWN:
        case JPI_EXISTS:
        case JPI_PLUS:
        case JPI_MINUS:
            flattenUnaryExpr(buf, &fcxt, item, nestingLevel, insideArraySubscript);
            break;
        case JPI_NULL:
            break;
        case JPI_ROOT:
            break;
        case JPI_ANY_ARRAY:
        case JPI_ANY_KEY:
            break;
        case JPI_CURRENT:
            if (nestingLevel <= 0) {
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("@ is not allowed in root expressions")));
            }
            break;
        case JPI_LAST:
            if (!insideArraySubscript) {
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("LAST is allowed only in array subscripts")));
            }
            break;
        case JPI_INDEX_ARRAY:
            flattenIndexArray(buf, fcxt.pos, item, nestingLevel);
            break;
        case JPI_ANY:
            appendBinaryStringInfo(buf,
                                   (char*)(&item->value.anybounds.first),
                                   sizeof(item->value.anybounds.first));
            appendBinaryStringInfo(buf,
                                   (char*)(&item->value.anybounds.last),
                                   sizeof(item->value.anybounds.last));
            break;
        case JPI_DATE:
        case JPI_DATETIME:
        case JPI_TIME:
        case JPI_TIME_TZ:
        case JPI_TIMESTAMP:
        case JPI_TIMESTAMP_TZ:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("jsonpath item method .%s() is not supported.", JspOperationName(item->type))));
        case JPI_TYPE:
        case JPI_SIZE:
        case JPI_ABS:
        case JPI_FLOOR:
        case JPI_CEILING:
        case JPI_KEYVALUE:
        case JPI_NUMBER:
        case JPI_STRING_FUNC:
        case JPI_DOUBLE:
        case JPI_BOOLEAN:
        case JPI_BIGINT:
        case JPI_INTEGER:
            break;
        default:
            ereport(ERROR, (
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized jsonpath item type: %d", item->type)));
    }

    if (item->next) {
        flattenJsonPathParseItem(buf, &chld,
                                 item->next, nestingLevel,
                                 insideArraySubscript);
        chld -= fcxt.pos;
        *(int32 *) (buf->data + next) = chld;
    }

    if (result) {
        *result = fcxt.pos;
    }
}

/*
 * Align StringInfo to int by adding zero padding bytes
 */
static void alignStringInfoInt(StringInfo buf)
{
    int paddingLen = INTALIGN(buf->len) - buf->len;
    while (paddingLen > 0) {
        appendStringInfoCharMacro(buf, 0);
        paddingLen--;
    }
}

/*
 * Reserve space for int32 JsonPathItem pointer.  Now zero pointer is written,
 * actual value will be recorded at '(int32 *) &buf->data[pos]' later.
 */
static int32 reserveSpaceForItemPointer(StringInfo buf)
{
    int32 pos = buf->len;
    int32 ptr = 0;

    appendBinaryStringInfo(buf, (char*)(&ptr), sizeof(ptr));

    return pos;
}

/*
 * Prints text representation of given jsonpath item and all its children.
 * buf: the buffer where the result text representation output to.
 * v: the jsonpath item to be printed.
 * printBracketes: indicates whether it's needed to print brackets before and after
 *      a certain item due to priority consideration.
 */
static void printJsonPathItem(StringInfo buf, JsonPathItem *v, bool printBrackets)
{
    JsonPathItem elem;
    int32 len;
    char *str;
    check_stack_depth();
    CHECK_FOR_INTERRUPTS();

    switch (v->type) {
        case JPI_NULL:
            appendStringInfoString(buf, "null");
            break;
        case JPI_STRING:
            str = jspGetString(v, &len);
            escape_json_with_len(buf, str, len);
            break;
        case JPI_NUMERIC:
            printNumeric(buf, v);
            break;
        case JPI_BOOL:
            printBool(buf, v);
            break;
        case JPI_AND:
        case JPI_OR:
        case JPI_EQUAL:
        case JPI_NOT_EQUAL:
        case JPI_LESS:
        case JPI_GREATER:
        case JPI_LESS_OR_EQUAL:
        case JPI_GREATER_OR_EQUAL:
        case JPI_ADD:
        case JPI_SUB:
        case JPI_MUL:
        case JPI_DIV:
        case JPI_MOD:
        case JPI_STARTS_WITH:
            printBinaryExpr(buf, v, printBrackets, &elem);
            break;
        case JPI_NOT:
            printNot(buf, v, &elem);
            break;
        case JPI_IS_UNKNOWN:
            printIsUnknown(buf, v, &elem);
            break;
        case JPI_PLUS:
        case JPI_MINUS:
            printPlusMinus(buf, v, printBrackets, &elem);
            break;
        case JPI_ANY_ARRAY:
            appendStringInfoString(buf, "[*]");
            break;
        case JPI_ANY_KEY:
            printAnyKey(buf, v);
            break;
        case JPI_INDEX_ARRAY:
            printIndexArray(buf, v);
            break;
        case JPI_ANY:
            printAny(buf, v);
            break;
        case JPI_KEY:
            printKey(buf, v);
            break;
        case JPI_CURRENT:
            appendStringInfoChar(buf, '@');
            break;
        case JPI_ROOT:
            appendStringInfoChar(buf, '$');
            break;
        case JPI_VARIABLE:
            appendStringInfoChar(buf, '$');
            str = jspGetString(v, &len);
            escape_json_with_len(buf, str, len);
            break;
        case JPI_FILTER:
            printFilter(buf, v, &elem);
            break;
        case JPI_EXISTS:
            printExists(buf, v, &elem);
            break;
        case JPI_TYPE:
            appendStringInfoString(buf, ".type()");
            break;
        case JPI_SIZE:
            appendStringInfoString(buf, ".size()");
            break;
        case JPI_ABS:
            appendStringInfoString(buf, ".abs()");
            break;
        case JPI_FLOOR:
            appendStringInfoString(buf, ".floor()");
            break;
        case JPI_CEILING:
            appendStringInfoString(buf, ".ceiling()");
            break;
        case JPI_KEYVALUE:
            appendStringInfoString(buf, ".keyvalue()");
            break;
        case JPI_DECIMAL:
            printDecimal(buf, v, &elem);
            break;
        case JPI_NUMBER:
            appendStringInfoString(buf, ".number()");
            break;
        case JPI_STRING_FUNC:
            appendStringInfoString(buf, ".string()");
            break;
        case JPI_DOUBLE:
            appendStringInfoString(buf, ".double()");
            break;
        case JPI_BOOLEAN:
            appendStringInfoString(buf, ".boolean()");
            break;
        case JPI_INTEGER:
            appendStringInfoString(buf, ".integer()");
            break;
        case JPI_BIGINT:
            appendStringInfoString(buf, ".bigint()");
            break;
        case JPI_LAST:
            appendStringInfoString(buf, "last");
            break;
        case JPI_LIKE_REGEX:
            printLikeRegex(buf, v, printBrackets, &elem);
            break;
        default:
            elog(ERROR, "unrecognized jsonpath item type: %d", v->type);
    }

    if (jspGetNext(v, &elem)) {
        printJsonPathItem(buf, &elem, true);
    }
}

const char* JspOperationName(JsonPathItemType type)
{
    switch (type) {
        case JPI_AND:
            return "&&";
        case JPI_OR:
            return "||";
        case JPI_EQUAL:
            return "==";
        case JPI_NOT_EQUAL:
            return "!=";
        case JPI_LESS:
            return "<";
        case JPI_GREATER:
            return ">";
        case JPI_LESS_OR_EQUAL:
            return "<=";
        case JPI_GREATER_OR_EQUAL:
            return ">=";
        case JPI_ADD:
        case JPI_PLUS:
            return "+";
        case JPI_SUB:
        case JPI_MINUS:
            return "-";
        case JPI_MUL:
            return "*";
        case JPI_DIV:
            return "/";
        case JPI_MOD:
            return "%";
        case JPI_TYPE:
            return "type";
        case JPI_SIZE:
            return "size";
        case JPI_ABS:
            return "abs";
        case JPI_FLOOR:
            return "floor";
        case JPI_CEILING:
            return "ceiling";
        case JPI_KEYVALUE:
            return "keyvalue";
        case JPI_STARTS_WITH:
            return "starts with";
        case JPI_LIKE_REGEX:
            return "like_regex";
        case JPI_DECIMAL:
            return "decimal";
        case JPI_NUMBER:
            return "number";
        case JPI_STRING_FUNC:
            return "string";
        case JPI_DOUBLE:
            return "double";
        case JPI_BOOLEAN:
            return "boolean";
        case JPI_INTEGER:
            return "integer";
        case JPI_BIGINT:
            return "bigint";
        case JPI_DATE :
            return "date";
        case JPI_DATETIME:
            return "datetime";
        case JPI_TIME:
            return "time";
        case JPI_TIME_TZ:
            return "time_tz";
        case JPI_TIMESTAMP:
            return "timestamp";
        case JPI_TIMESTAMP_TZ:
            return "timestamp_tz";
        default:
            elog(ERROR, "unrecognized jsonpath item type: %d", type);
            return NULL;
    }
}

static int operationPriority(JsonPathItemType op)
{
    int priority = 1;
    /* accumulate priority while falling through,
     * to avoid using magic number */
    switch (op) {
        /* highest priority */
        case JPI_PLUS:
        case JPI_MINUS:
            priority++;
        /* fall through to next lower priority */
        case JPI_MUL:
        case JPI_DIV:
        case JPI_MOD:
            priority++;
        /* fall through to next lower priority */
        case JPI_ADD:
        case JPI_SUB:
            priority++;
        /* fall through to next lower priority */
        case JPI_EQUAL:
        case JPI_NOT_EQUAL:
        case JPI_LESS:
        case JPI_GREATER:
        case JPI_LESS_OR_EQUAL:
        case JPI_GREATER_OR_EQUAL:
        case JPI_STARTS_WITH:
            priority++;
        /* fall through to next lower priority */
        case JPI_AND:
            priority++;
        /* fall through to next lower priority */
        case JPI_OR:
            break;
        /* invalid priority */
        default:
            return PG_INT32_MAX;
    }
    return priority;
}

static void flattenIndexArray(StringInfo buf, int32 pos,
    JsonPathParseItem* item, int nestingLevel)
{
    int32 nelems = item->value.array.nelems;
    int offset;
    int i;
    int twoIndex = 2;

    appendBinaryStringInfo(buf, (char*)(&nelems), sizeof(nelems));
    offset = buf->len;
    appendStringInfoSpaces(buf, sizeof(int32) * twoIndex * nelems);

    for (i = 0; i < nelems; i++) {
        int32 *ppos;
        int32 topos;
        int32 frompos;

        flattenJsonPathParseItem(buf, &frompos,
                                   item->value.array.elems[i].from,
                                   nestingLevel, true);
        frompos -= pos;

        if (item->value.array.elems[i].to) {
            flattenJsonPathParseItem(buf, &topos,
                                      item->value.array.elems[i].to,
                                      nestingLevel, true);
            topos -= pos;
        } else {
            topos = 0;
        }

        ppos = (int32 *) &buf->data[offset + i * twoIndex * sizeof(int32)];

        ppos[0] = frompos;
        ppos[1] = topos;
    }
}

static void flattenUnaryExpr(StringInfo buf, FlattenJsonPathContext* fcxt,
    JsonPathParseItem* item, int nestingLevel, bool insideArraySubscript)
{
    int32 arg = reserveSpaceForItemPointer(buf);
    if (!item->value.arg) {
        fcxt->chld = fcxt->pos;
    } else {
        flattenJsonPathParseItem(
            buf, &(fcxt->chld), item->value.arg,
            (item->type == JPI_FILTER)? (nestingLevel + 1) : nestingLevel,
            insideArraySubscript);
    }
    *(int32 *) (buf->data + arg) = (fcxt->chld - fcxt->pos);
}

static void flattenBinaryExpr(StringInfo buf, FlattenJsonPathContext* fcxt,
    JsonPathParseItem* item, int nestingLevel, bool insideArraySubscript)
{
    /*
    * First, reserve place for left/right arg's positions, then
    * record both args and sets actual position in reserved
    * places.
    */
    int32 left = reserveSpaceForItemPointer(buf);
    int32 right = reserveSpaceForItemPointer(buf);

    if (!item->value.args.left) {
        fcxt->chld = fcxt->pos;
    } else {
        flattenJsonPathParseItem(buf, &(fcxt->chld),
                                 item->value.args.left,
                                 nestingLevel,
                                 insideArraySubscript);
    }
    *(int32 *) (buf->data + left) = (fcxt->chld - fcxt->pos);

    if (!item->value.args.right) {
        fcxt->chld = fcxt->pos;
    } else {
        flattenJsonPathParseItem(buf, &(fcxt->chld),
                                    item->value.args.right,
                                    nestingLevel,
                                    insideArraySubscript);
    }
    *(int32 *) (buf->data + right) = (fcxt->chld - fcxt->pos);
}

static void flattenLikeRegex(StringInfo buf, FlattenJsonPathContext* fcxt,
    JsonPathParseItem* item, int nestingLevel, bool insideArraySubscript)
{
    int32 offs;

    appendBinaryStringInfo(buf,
                            (char*)(&item->value.like_regex.flags),
                            sizeof(item->value.like_regex.flags));
    offs = reserveSpaceForItemPointer(buf);
    appendBinaryStringInfo(buf,
                            (char*)(&item->value.like_regex.patternlen),
                            sizeof(item->value.like_regex.patternlen));
    appendBinaryStringInfo(buf, item->value.like_regex.pattern,
                            item->value.like_regex.patternlen);
    appendStringInfoChar(buf, '\0');

    flattenJsonPathParseItem(buf, &(fcxt->chld),
                                item->value.like_regex.expr,
                                nestingLevel,
                                insideArraySubscript);
    *(int32 *) (buf->data + offs) = (fcxt->chld - fcxt->pos);
}

static void printIndexArray(StringInfo buf, JsonPathItem* v)
{
    int i;
    appendStringInfoChar(buf, '[');
    for (i = 0; i < v->content.array.nelems; i++) {
        JsonPathItem from;
        JsonPathItem to;
        bool range = jspGetArraySubscript(v, &from, &to, i);

        if (i) {
            appendStringInfoChar(buf, ',');
        }

        printJsonPathItem(buf, &from, false);

        if (range) {
            appendStringInfoString(buf, " to ");
            printJsonPathItem(buf, &to, false);
        }
    }
    appendStringInfoChar(buf, ']');
}

/*
 * Prints text representation of JPI_NUMERIC (numeric literal) item.
 */
static void printNumeric(StringInfo buf, JsonPathItem* v)
{
    if (jspHasNext(v)) {
        appendStringInfoChar(buf, '(');
    }
    appendStringInfoString(buf,
                            DatumGetCString(DirectFunctionCall1(numeric_out,
                                            NumericGetDatum(jspGetNumeric(v)))));
    if (jspHasNext(v)) {
        appendStringInfoChar(buf, ')');
    }
}

/*
 * Prints text representation of JPI_BOOL (boolean literal) item.
 */
static void printBool(StringInfo buf, JsonPathItem* v)
{
    if (jspGetBool(v)) {
        appendStringInfoString(buf, "true");
    } else {
        appendStringInfoString(buf, "false");
    }
}

/*
 * Prints text representation of JPI_KEY (.key accessor) item.
 */
static void printKey(StringInfo buf, JsonPathItem* v)
{
    int32 len;
    char* str;
    appendStringInfoChar(buf, '.');
    str = jspGetString(v, &len);
    escape_json_with_len(buf, str, len);
}

/*
 * Prints text representation of JPI_ANY_KEY (.* accessor) item.
 */
static void printAnyKey(StringInfo buf, JsonPathItem* v)
{
    appendStringInfoChar(buf, '.');
    appendStringInfoChar(buf, '*');
}

/*
 * Prints text representation of JPI_ANY (.** accessor) item.
 */
static void printAny(StringInfo buf, JsonPathItem* v)
{
    appendStringInfoChar(buf, '.');

    if (v->content.anybounds.first == 0 &&
        v->content.anybounds.last == PG_UINT32_MAX) {
        appendStringInfoString(buf, "**");
    } else if (v->content.anybounds.first == v->content.anybounds.last) {
        if (v->content.anybounds.first == PG_UINT32_MAX) {
            appendStringInfoString(buf, "**{last}");
        } else {
            appendStringInfo(buf, "**{%u}",
                                v->content.anybounds.first);
        }
    } else if (v->content.anybounds.first == PG_UINT32_MAX) {
        appendStringInfo(buf, "**{last to %u}",
                            v->content.anybounds.last);
    } else if (v->content.anybounds.last == PG_UINT32_MAX) {
        appendStringInfo(buf, "**{%u to last}",
                            v->content.anybounds.first);
    } else {
        appendStringInfo(buf, "**{%u to %u}",
                            v->content.anybounds.first,
                            v->content.anybounds.last);
    }
}

/*
 * Prints text representation of binary arithmetic expression.
 */
static void printBinaryExpr(StringInfo buf, JsonPathItem* v,
    bool printBrackets, JsonPathItem* elem)
{
    if (printBrackets) {
        appendStringInfoChar(buf, '(');
    }
    JspGetLeftArg(v, elem);
    printJsonPathItem(buf, elem,
                        operationPriority(elem->type) <=
                        operationPriority(v->type));
    appendStringInfoChar(buf, ' ');
    appendStringInfoString(buf, JspOperationName(v->type));
    appendStringInfoChar(buf, ' ');
    JspGetRightArg(v, elem);
    printJsonPathItem(buf, elem,
                        operationPriority(elem->type) <=
                        operationPriority(v->type));
    if (printBrackets) {
        appendStringInfoChar(buf, ')');
    }
}

/*
 * Prints text representation of JPI_PLUS (+ expr) or JPI_MINUS item (- expr).
 */
static void printPlusMinus(StringInfo buf, JsonPathItem* v,
    bool printBrackets, JsonPathItem* elem)
{
    if (printBrackets) {
        appendStringInfoChar(buf, '(');
    }
    appendStringInfoChar(buf, v->type == JPI_PLUS ? '+' : '-');
    JspGetArg(v, elem);
    printJsonPathItem(buf, elem,
                        operationPriority(elem->type) <=
                        operationPriority(v->type));
    if (printBrackets) {
        appendStringInfoChar(buf, ')');
    }
}

/*
 * Prints text representation of JPI_NOT item (! predicate).
 */
static void printNot(StringInfo buf, JsonPathItem* v, JsonPathItem* elem)
{
    appendStringInfoString(buf, "!(");
    JspGetArg(v, elem);
    printJsonPathItem(buf, elem, false);
    appendStringInfoChar(buf, ')');
}

/*
 * Prints text representation of JPI_FILTER.
 */
static void printFilter(StringInfo buf, JsonPathItem* v, JsonPathItem* elem)
{
    appendStringInfoString(buf, "?(");
    JspGetArg(v, elem);
    printJsonPathItem(buf, elem, false);
    appendStringInfoChar(buf, ')');
}

/*
 * Prints text representation of IS UNKNOWN predicate.
 */
static void printIsUnknown(StringInfo buf, JsonPathItem* v, JsonPathItem* elem)
{
    appendStringInfoChar(buf, '(');
    JspGetArg(v, elem);
    printJsonPathItem(buf, elem, false);
    appendStringInfoString(buf, ") is unknown");
}

/*
 * Prints text representation of EXISTS(expr) predicate.
 */
static void printExists(StringInfo buf, JsonPathItem* v, JsonPathItem* elem)
{
    appendStringInfoString(buf, "exists (");
    JspGetArg(v, elem);
    printJsonPathItem(buf, elem, false);
    appendStringInfoChar(buf, ')');
}

/*
 * Prints text representation of LIKE_REGEX predicate.
 */
static void printLikeRegex(StringInfo buf, JsonPathItem* v, bool printBrackets, JsonPathItem* elem)
{
    if (printBrackets) {
        appendStringInfoChar(buf, '(');
    }

    jspInitByBuffer(elem, v->base, v->content.like_regex.expr);
    printJsonPathItem(buf, elem,
                        operationPriority(elem->type) <=
                        operationPriority(v->type));

    appendStringInfoString(buf, " like_regex ");

    escape_json_with_len(buf,
                            v->content.like_regex.pattern,
                            v->content.like_regex.patternlen);

    if (v->content.like_regex.flags) {
        appendStringInfoString(buf, " flag \"");

        if (v->content.like_regex.flags & JSP_REGEX_ICASE) {
            appendStringInfoChar(buf, 'i');
        }
        if (v->content.like_regex.flags & JSP_REGEX_DOTALL) {
            appendStringInfoChar(buf, 's');
        }
        if (v->content.like_regex.flags & JSP_REGEX_MLINE) {
            appendStringInfoChar(buf, 'm');
        }
        if (v->content.like_regex.flags & JSP_REGEX_WSPACE) {
            appendStringInfoChar(buf, 'x');
        }
        if (v->content.like_regex.flags & JSP_REGEX_QUOTE) {
            appendStringInfoChar(buf, 'q');
        }

        appendStringInfoChar(buf, '"');
    }

    if (printBrackets) {
        appendStringInfoChar(buf, ')');
    }
}

/*
 * Prints text representation of JPI_DECIMAL (.decimal([p[,s]])) item.
 */
static void printDecimal(StringInfo buf, JsonPathItem* v, JsonPathItem* elem)
{
    appendStringInfoString(buf, ".decimal(");
    if (v->content.args.left) {
        JspGetLeftArg(v, elem);
        printJsonPathItem(buf, elem, false);
    }
    if (v->content.args.right) {
        appendStringInfoChar(buf, ',');
        JspGetRightArg(v, elem);
        printJsonPathItem(buf, elem, false);
    }
    appendStringInfoChar(buf, ')');
}

/******************* Support functions for JsonPath *************************/

/*
 * Support macros to read stored values
 */

static inline char ReadByte(char* base, int* pos)
{
    char c = (char)(*(uint8*)((base) + (*pos)));
    *pos += 1;
    return c;
}

static inline int32 ReadInt32(char* base, int* pos)
{
    int32 val = *(uint32*)((base) + (*pos));
    *pos += sizeof(int32);
    return val;
}

static inline int32* ReadInt32N(char* base, int* pos, int n)
{
    int32* vals = (int32*)((base) + (*pos));
    *pos += (sizeof(int32) * n);
    return vals;
}

/*
 * Read root node and fill root node representation
 */
void JspInit(JsonPathItem *v, JsonPath *js)
{
    Assert((js->header & ~JSONPATH_LAX) == JSONPATH_VERSION);
    jspInitByBuffer(v, js->data, 0);
}

/*
 * Read node from buffer and fill its representation
 */
void jspInitByBuffer(JsonPathItem *v, char *base, int32 pos)
{
    v->base = base + pos;

    v->type = (JsonPathItemType)ReadByte(base, &pos);
    pos = INTALIGN((uintptr_t) (base + pos)) - (uintptr_t) base;
    v->nextPos = ReadInt32(base, &pos);

    switch (v->type) {
        case JPI_NULL:
        case JPI_ROOT:
        case JPI_CURRENT:
        case JPI_ANY_ARRAY:
        case JPI_ANY_KEY:
        case JPI_LAST:
        case JPI_SIZE:
        case JPI_TYPE:
        case JPI_ABS:
        case JPI_FLOOR:
        case JPI_CEILING:
        case JPI_KEYVALUE:
        case JPI_NUMBER:
        case JPI_STRING_FUNC:
        case JPI_DOUBLE:
        case JPI_BOOLEAN:
        case JPI_BIGINT:
        case JPI_INTEGER:
            break;
        case JPI_STRING:
        case JPI_KEY:
        case JPI_VARIABLE:
            v->content.value.datalen = ReadInt32(base, &pos);
            /* FALLTHROUGH */
        case JPI_NUMERIC:
        case JPI_BOOL:
            v->content.value.data = base + pos;
            break;
        case JPI_AND:
        case JPI_OR:
        case JPI_EQUAL:
        case JPI_NOT_EQUAL:
        case JPI_LESS:
        case JPI_GREATER:
        case JPI_LESS_OR_EQUAL:
        case JPI_GREATER_OR_EQUAL:
        case JPI_ADD:
        case JPI_SUB:
        case JPI_MUL:
        case JPI_DIV:
        case JPI_MOD:
        case JPI_STARTS_WITH:
        case JPI_DECIMAL:
            v->content.args.left = ReadInt32(base, &pos);
            v->content.args.right = ReadInt32(base, &pos);
            break;
        case JPI_NOT:
        case JPI_FILTER:
        case JPI_IS_UNKNOWN:
        case JPI_EXISTS:
        case JPI_PLUS:
        case JPI_MINUS:
            v->content.arg = ReadInt32(base, &pos);
            break;
        case JPI_INDEX_ARRAY: {
            int twoIndexes = 2;
            v->content.array.nelems = ReadInt32(base, &pos);
            v->content.array.elems =
                (JsonPathItem::JsonPathContent::IndexArray::ArrayIndexRange*)
                ReadInt32N(base, &pos, v->content.array.nelems * twoIndexes);
            break;
        }
        case JPI_ANY:
            v->content.anybounds.first = ReadInt32(base, &pos);
            v->content.anybounds.last = ReadInt32(base, &pos);
            break;
        case JPI_LIKE_REGEX:
            v->content.like_regex.flags = ReadInt32(base, &pos);
            v->content.like_regex.expr = ReadInt32(base, &pos);
            v->content.like_regex.patternlen = ReadInt32(base, &pos);
            v->content.like_regex.pattern = base + pos;
            break;
        default:
            ereport(ERROR, (
                errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized jsonpath item type: %d", v->type)));
    }
}

void JspGetArg(JsonPathItem *v, JsonPathItem *a)
{
    Assert(v->type == JPI_NOT ||
           v->type == JPI_FILTER ||
           v->type == JPI_IS_UNKNOWN ||
           v->type == JPI_EXISTS ||
           v->type == JPI_PLUS ||
           v->type == JPI_MINUS);

    jspInitByBuffer(a, v->base, v->content.arg);
}

bool jspGetNext(JsonPathItem *v, JsonPathItem *a)
{
    if (jspHasNext(v)) {
        Assert(v->type >= JPI_NULL &&
               v->type <= JPI_TIMESTAMP_TZ);
        if (a) {
            jspInitByBuffer(a, v->base, v->nextPos);
        }
        return true;
    }

    return false;
}

void JspGetLeftArg(JsonPathItem *v, JsonPathItem *a)
{
    Assert(v->type == JPI_AND ||
           v->type == JPI_OR ||
           v->type == JPI_EQUAL ||
           v->type == JPI_NOT_EQUAL ||
           v->type == JPI_LESS ||
           v->type == JPI_GREATER ||
           v->type == JPI_LESS_OR_EQUAL ||
           v->type == JPI_GREATER_OR_EQUAL ||
           v->type == JPI_ADD ||
           v->type == JPI_SUB ||
           v->type == JPI_MUL ||
           v->type == JPI_DIV ||
           v->type == JPI_MOD ||
           v->type == JPI_DECIMAL ||
           v->type == JPI_STARTS_WITH);

    jspInitByBuffer(a, v->base, v->content.args.left);
}

void JspGetRightArg(JsonPathItem *v, JsonPathItem *a)
{
    Assert(v->type == JPI_AND ||
           v->type == JPI_OR ||
           v->type == JPI_EQUAL ||
           v->type == JPI_NOT_EQUAL ||
           v->type == JPI_LESS ||
           v->type == JPI_GREATER ||
           v->type == JPI_LESS_OR_EQUAL ||
           v->type == JPI_GREATER_OR_EQUAL ||
           v->type == JPI_ADD ||
           v->type == JPI_SUB ||
           v->type == JPI_MUL ||
           v->type == JPI_DIV ||
           v->type == JPI_MOD ||
           v->type == JPI_DECIMAL ||
           v->type == JPI_STARTS_WITH);

    jspInitByBuffer(a, v->base, v->content.args.right);
}

bool jspGetBool(JsonPathItem *v)
{
    Assert(v->type == JPI_BOOL);

    return (bool) *v->content.value.data;
}

Numeric jspGetNumeric(JsonPathItem *v)
{
    Assert(v->type == JPI_NUMERIC);

    return (Numeric) v->content.value.data;
}

char* jspGetString(JsonPathItem *v, int32 *len)
{
    Assert(v->type == JPI_KEY ||
           v->type == JPI_STRING ||
           v->type == JPI_VARIABLE);

    if (len) {
        *len = v->content.value.datalen;
    }
    return v->content.value.data;
}

bool jspGetArraySubscript(JsonPathItem *v, JsonPathItem *from, JsonPathItem *to,
    int i)
{
    Assert(v->type == JPI_INDEX_ARRAY);

    jspInitByBuffer(from, v->base, v->content.array.elems[i].from);

    if (!v->content.array.elems[i].to) {
        return false;
    }

    jspInitByBuffer(to, v->base, v->content.array.elems[i].to);

    return true;
}