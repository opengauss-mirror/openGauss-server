/* -------------------------------------------------------------------------
 *
 * quote.c
 *    Functions for quoting identifiers and literals
 *
 * Portions Copyright (c) 2000-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *    src/backend/utils/adt/quote.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "mb/pg_wchar.h"
#include "utils/builtins.h"

/*
 * quote_ident -
 *    returns a properly quoted identifier
 */
Datum quote_ident(PG_FUNCTION_ARGS)
{
    text* t = PG_GETARG_TEXT_PP(0);
    const char* q_str = NULL;
    char* str = NULL;

    str = text_to_cstring(t);
    q_str = quote_identifier(str);
    PG_RETURN_TEXT_P(cstring_to_text(q_str));
}

/*
 * quote_literal_internal -
 *    helper function for quote_literal and quote_literal_cstr
 *
 * NOTE: think not to make this function's behavior change with
 * standard_conforming_strings.  We don't know where the result
 * literal will be used, and so we must generate a result that
 * will work with either setting.  Take a look at what dblink
 * uses this for before thinking you know better.
 */
static size_t quote_literal_internal(char* dst, const char* src, size_t len)
{
    const char* s = NULL;
    char* save_dst = dst;
    int char_len;

    s = src;
    while (s < src + len) {
        if (*s == '\\') {
            *dst++ = ESCAPE_STRING_SYNTAX;
            break;
        }
        s += pg_mblen(s);
    }

    *dst++ = '\'';
    while (len > 0) {
        if (SQL_STR_DOUBLE(*src, true))
            *dst++ = *src;
        char_len = pg_mblen(src);

        for (int i = 0; i < char_len; i++)
            *dst++ = *src++;

        len -= char_len;
    }
    *dst++ = '\'';

    return dst - save_dst;
}

/*
 * quote_literal -
 *    returns a properly quoted literal
 */
Datum quote_literal(PG_FUNCTION_ARGS)
{
    text* t = PG_GETARG_TEXT_P(0);
    text* result = NULL;
    char* cp1 = NULL;
    char* cp2 = NULL;

    int len = VARSIZE(t) - VARHDRSZ;
    if (unlikely(len < 0)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("The length should not be nagative: %d.\n", len)));
    }
    /* We make a worst-case result area; wasting a little space is OK */
    result = (text*)palloc(len * 2 + 3 + VARHDRSZ);

    cp1 = VARDATA(t);
    cp2 = VARDATA(result);

    SET_VARSIZE(result, VARHDRSZ + quote_literal_internal(cp2, cp1, len));

    PG_RETURN_TEXT_P(result);
}

/*
 * quote_literal_cstr -
 *    returns a properly quoted literal
 */
char* quote_literal_cstr(const char* raw_str)
{
    char* result = NULL;
    int len;
    int new_len;

    len = strlen(raw_str);
    /* We make a worst-case result area; wasting a little space is OK */
    result = (char*)palloc(len * 2 + 3 + 1);

    new_len = quote_literal_internal(result, raw_str, len);
    result[new_len] = '\0';

    return result;
}

/*
 * quote_nullable -
 *    Returns a properly quoted literal, with null values returned
 *    as the text string 'NULL'.
 */
Datum quote_nullable(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0))
        PG_RETURN_TEXT_P(cstring_to_text("NULL"));
    else
        PG_RETURN_DATUM(DirectFunctionCall1(quote_literal, PG_GETARG_DATUM(0)));
}
