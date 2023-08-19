/* -------------------------------------------------------------------------
 * oracle_compat.c
 *	Oracle compatible functions.
 *
 * Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/oracle_compat.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_proc.h"
#include "common/int.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "mb/pg_wchar.h"
#include "parser/parser.h"
#include "miscadmin.h"

static text* dotrim(const char* string, int stringlen, const char* set, int setlen, bool doltrim, bool dortrim);

/********************************************************************
 *
 * lower
 *
 * Syntax:
 *
 *	 text lower(text string)
 *
 * Purpose:
 *
 *	 Returns string, with all letters forced to lowercase.
 *
 ********************************************************************/

Datum lower(PG_FUNCTION_ARGS)
{
    text* in_string = PG_GETARG_TEXT_PP(0);
    FUNC_CHECK_HUGE_POINTER(false, in_string, "lower()");

    char* out_string = NULL;
    text* result = NULL;

    out_string = str_tolower(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string), PG_GET_COLLATION());
    result = cstring_to_text(out_string);
    pfree_ext(out_string);

    PG_RETURN_TEXT_P(result);
}

/********************************************************************
 *
 * upper
 *
 * Syntax:
 *
 *	 text upper(text string)
 *
 * Purpose:
 *
 *	 Returns string, with all letters forced to uppercase.
 *
 ********************************************************************/

Datum upper(PG_FUNCTION_ARGS)
{
    text* in_string = PG_GETARG_TEXT_PP(0);
    FUNC_CHECK_HUGE_POINTER(false, in_string, "upper()");

    char* out_string = NULL;
    text* result = NULL;

    out_string = str_toupper(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string), PG_GET_COLLATION());
    result = cstring_to_text(out_string);
    pfree_ext(out_string);

    PG_RETURN_TEXT_P(result);
}

/********************************************************************
 *
 * initcap
 *
 * Syntax:
 *
 *	 text initcap(text string)
 *
 * Purpose:
 *
 *	 Returns string, with first letter of each word in uppercase, all
 *	 other letters in lowercase. A word is defined as a sequence of
 *	 alphanumeric characters, delimited by non-alphanumeric
 *	 characters.
 *
 ********************************************************************/

Datum initcap(PG_FUNCTION_ARGS)
{
    text* in_string = PG_GETARG_TEXT_PP(0);
    char* out_string = NULL;
    text* result = NULL;
    FUNC_CHECK_HUGE_POINTER(false, in_string, "initcap()");

    out_string = str_initcap(VARDATA_ANY(in_string), VARSIZE_ANY_EXHDR(in_string), PG_GET_COLLATION());
    result = cstring_to_text(out_string);
    pfree_ext(out_string);

    PG_RETURN_TEXT_P(result);
}

/********************************************************************
 *
 * lpad
 *
 * Syntax:
 *
 *	 text lpad(text string1, int4 len, text string2)
 *
 * Purpose:
 *
 *	 Returns string1, left-padded to length len with the sequence of
 *	 characters in string2.  If len is less than the length of string1,
 *	 instead truncate (on the right) to len.
 *
 ********************************************************************/

Datum lpad(PG_FUNCTION_ARGS)
{
    text* string1 = PG_GETARG_TEXT_PP(0);
    int32 len = PG_GETARG_INT32(1);
    text* string2 = PG_GETARG_TEXT_PP(2);
    text* ret = NULL;
    char *ptr1 = NULL, *ptr2 = NULL, *ptr2start = NULL, *ptr2end = NULL, *ptr_ret = NULL;
    int m, s1len, s2len;

    int bytelen;
    errno_t ss_rc;

    FUNC_CHECK_HUGE_POINTER(false, string1, "lpad()");

    /* Negative len is silently taken as zero */
    if (len < 0)
        len = 0;

    s1len = VARSIZE_ANY_EXHDR(string1);
    if (s1len < 0)
        s1len = 0; /* shouldn't happen */

    s2len = VARSIZE_ANY_EXHDR(string2);
    if (s2len < 0)
        s2len = 0; /* shouldn't happen */

    s1len = pg_mbstrlen_with_len(VARDATA_ANY(string1), s1len);

    if (s1len > len)
        s1len = len; /* truncate string1 to len chars */

    if (s2len <= 0)
        len = s1len; /* nothing to pad with, so don't pad */

    bytelen = pg_database_encoding_max_length() * len;

    /* check for integer overflow */
    if (len != 0 && bytelen / pg_database_encoding_max_length() != len)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("requested length too large")));

    ret = (text*)palloc(VARHDRSZ + bytelen);

    m = len - s1len;

    ptr2 = ptr2start = VARDATA_ANY(string2);
    ptr2end = ptr2 + s2len;
    ptr_ret = VARDATA(ret);
    int tmp_len = 0;

    while (m--) {
        int mlen = pg_mblen(ptr2);

        ss_rc = memcpy_s(ptr_ret, bytelen - tmp_len, ptr2, mlen);
        securec_check(ss_rc, "\0", "\0");
        ptr_ret += mlen;
        tmp_len += mlen;
        ptr2 += mlen;
        if (ptr2 == ptr2end) /* wrap around at end of s2 */
            ptr2 = ptr2start;
    }

    ptr1 = VARDATA_ANY(string1);

    tmp_len = 0;
    while (s1len--) {
        int mlen = pg_mblen(ptr1);

        ss_rc = memcpy_s(ptr_ret, bytelen - tmp_len, ptr1, mlen);
        securec_check(ss_rc, "\0", "\0");
        ptr_ret += mlen;
        tmp_len += mlen;
        ptr1 += mlen;
    }

    SET_VARSIZE(ret, ptr_ret - (char*)ret);

    if (0 == VARSIZE_ANY_EXHDR(ret) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT &&
        !ACCEPT_EMPTY_STR && !RETURN_NS)
        PG_RETURN_NULL();
    else
        PG_RETURN_TEXT_P(ret);
}

/********************************************************************
 *
 * rpad
 *
 * Syntax:
 *
 *	 text rpad(text string1, int4 len, text string2)
 *
 * Purpose:
 *
 *	 Returns string1, right-padded to length len with the sequence of
 *	 characters in string2.  If len is less than the length of string1,
 *	 instead truncate (on the right) to len.
 *
 ********************************************************************/

Datum rpad(PG_FUNCTION_ARGS)
{
    text* string1 = PG_GETARG_TEXT_PP(0);
    int32 len = PG_GETARG_INT32(1);
    text* string2 = PG_GETARG_TEXT_PP(2);
    text* ret = NULL;
    char *ptr1 = NULL, *ptr2 = NULL, *ptr2start = NULL, *ptr2end = NULL, *ptr_ret = NULL;
    int m, s1len, s2len;
    FUNC_CHECK_HUGE_POINTER(false, string1, "rpad()");

    int bytelen;
    errno_t ss_rc;

    /* Negative len is silently taken as zero */
    if (len < 0)
        len = 0;

    s1len = VARSIZE_ANY_EXHDR(string1);
    if (s1len < 0)
        s1len = 0; /* shouldn't happen */

    s2len = VARSIZE_ANY_EXHDR(string2);
    if (s2len < 0)
        s2len = 0; /* shouldn't happen */

    s1len = pg_mbstrlen_with_len(VARDATA_ANY(string1), s1len);

    if (s1len > len)
        s1len = len; /* truncate string1 to len chars */

    if (s2len <= 0)
        len = s1len; /* nothing to pad with, so don't pad */

    bytelen = pg_database_encoding_max_length() * len;

    /* Check for integer overflow */
    if (len != 0 && bytelen / pg_database_encoding_max_length() != len)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("requested length too large")));

    ret = (text*)palloc(VARHDRSZ + bytelen);
    m = len - s1len;

    ptr1 = VARDATA_ANY(string1);
    ptr_ret = VARDATA(ret);
    int tmp_len = 0;

    while (s1len--) {
        int mlen = pg_mblen(ptr1);

        ss_rc = memcpy_s(ptr_ret, bytelen - tmp_len, ptr1, mlen);
        securec_check(ss_rc, "\0", "\0");
        ptr_ret += mlen;
        tmp_len += mlen;
        ptr1 += mlen;
    }

    ptr2 = ptr2start = VARDATA_ANY(string2);
    ptr2end = ptr2 + s2len;

    tmp_len = 0;
    while (m--) {
        int mlen = pg_mblen(ptr2);

        ss_rc = memcpy_s(ptr_ret, bytelen - tmp_len, ptr2, mlen);
        securec_check(ss_rc, "\0", "\0");
        ptr_ret += mlen;
        tmp_len += mlen;
        ptr2 += mlen;
        if (ptr2 == ptr2end) /* wrap around at end of s2 */
            ptr2 = ptr2start;
    }

    SET_VARSIZE(ret, ptr_ret - (char*)ret);

    if (0 == VARSIZE_ANY_EXHDR(ret) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT &&
        !ACCEPT_EMPTY_STR && !RETURN_NS)
        PG_RETURN_NULL();
    else
        PG_RETURN_TEXT_P(ret);
}

/********************************************************************
 *
 * btrim
 *
 * Syntax:
 *
 *	 text btrim(text string, text set)
 *
 * Purpose:
 *
 *	 Returns string with characters removed from the front and back
 *	 up to the first character not in set.
 *
 ********************************************************************/

Datum btrim(PG_FUNCTION_ARGS)
{
    text* string = PG_GETARG_TEXT_PP(0);
    text* set = PG_GETARG_TEXT_PP(1);
    text* ret = NULL;

    FUNC_CHECK_HUGE_POINTER(false, string, "btrim()");

    ret = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string), VARDATA_ANY(set), VARSIZE_ANY_EXHDR(set), true, true);

    if ((ret == NULL || 0 == VARSIZE_ANY_EXHDR(ret)) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT &&
        !ACCEPT_EMPTY_STR)
        PG_RETURN_NULL();
    else
        PG_RETURN_TEXT_P(ret);
}

/********************************************************************
 *
 * btrim1 --- btrim with set fixed as ' '
 *
 ********************************************************************/

Datum btrim1(PG_FUNCTION_ARGS)
{
    text* string = PG_GETARG_TEXT_PP(0);
    text* ret = NULL;

    FUNC_CHECK_HUGE_POINTER(false, string, "btrim1()");

    ret = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string), " ", 1, true, true);

    if ((ret == NULL || 0 == VARSIZE_ANY_EXHDR(ret)) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT &&
        !ACCEPT_EMPTY_STR)
        PG_RETURN_NULL();
    else
        PG_RETURN_TEXT_P(ret);
}

/*
 * Common implementation for btrim, ltrim, rtrim
 */
static text* dotrim(const char* string, int stringlen, const char* set, int setlen, bool doltrim, bool dortrim)
{
    int i;

    /* Nothing to do if either string or set is empty */
    if (stringlen > 0 && setlen > 0) {
        if (pg_database_encoding_max_length() > 1) {
            /*
             * In the multibyte-encoding case, build arrays of pointers to
             * character starts, so that we can avoid inefficient checks in
             * the inner loops.
             */
            const char** stringchars;
            const char** setchars;
            int* stringmblen = NULL;
            int* setmblen = NULL;
            int stringnchars;
            int setnchars;
            int resultndx;
            int resultnchars;
            const char* p = NULL;
            int len;
            int mblen;
            const char* str_pos = NULL;
            int str_len;

            stringchars = (const char**)palloc(stringlen * sizeof(char*));
            stringmblen = (int*)palloc(stringlen * sizeof(int));
            stringnchars = 0;
            p = string;
            len = stringlen;
            while (len > 0) {
                stringchars[stringnchars] = p;
                stringmblen[stringnchars] = mblen = pg_mblen(p);
                stringnchars++;
                p += mblen;
                len -= mblen;
            }

            setchars = (const char**)palloc(setlen * sizeof(char*));
            setmblen = (int*)palloc(setlen * sizeof(int));
            setnchars = 0;
            p = set;
            len = setlen;
            while (len > 0) {
                setchars[setnchars] = p;
                setmblen[setnchars] = mblen = pg_mblen(p);
                setnchars++;
                p += mblen;
                len -= mblen;
            }

            resultndx = 0; /* index in stringchars[] */
            resultnchars = stringnchars;

            if (doltrim) {
                while (resultnchars > 0) {
                    str_pos = stringchars[resultndx];
                    str_len = stringmblen[resultndx];
                    for (i = 0; i < setnchars; i++) {
                        if (str_len == setmblen[i] && memcmp(str_pos, setchars[i], str_len) == 0)
                            break;
                    }
                    if (i >= setnchars)
                        break; /* no match here */
                    string += str_len;
                    stringlen -= str_len;
                    resultndx++;
                    resultnchars--;
                }
            }

            if (dortrim) {
                while (resultnchars > 0) {
                    str_pos = stringchars[resultndx + resultnchars - 1];
                    str_len = stringmblen[resultndx + resultnchars - 1];
                    for (i = 0; i < setnchars; i++) {
                        if (str_len == setmblen[i] && memcmp(str_pos, setchars[i], str_len) == 0)
                            break;
                    }
                    if (i >= setnchars)
                        break; /* no match here */
                    stringlen -= str_len;
                    resultnchars--;
                }
            }

            pfree_ext(stringchars);
            pfree_ext(stringmblen);
            pfree_ext(setchars);
            pfree_ext(setmblen);
        } else {
            /*
             * In the single-byte-encoding case, we don't need such overhead.
             */
            if (doltrim) {
                while (stringlen > 0) {
                    char str_ch = *string;

                    for (i = 0; i < setlen; i++) {
                        if (str_ch == set[i])
                            break;
                    }
                    if (i >= setlen)
                        break; /* no match here */
                    string++;
                    stringlen--;
                }
            }

            if (dortrim) {
                while (stringlen > 0) {
                    char str_ch = string[stringlen - 1];

                    for (i = 0; i < setlen; i++) {
                        if (str_ch == set[i])
                            break;
                    }
                    if (i >= setlen)
                        break; /* no match here */
                    stringlen--;
                }
            }
        }
    }

    /* Return selected portion of string */
    return cstring_to_text_with_len(string, stringlen);
}

/********************************************************************
 *
 * byteatrim
 *
 * Syntax:
 *
 *	 bytea byteatrim(byta string, bytea set)
 *
 * Purpose:
 *
 *	 Returns string with characters removed from the front and back
 *	 up to the first character not in set.
 *
 * Cloned from btrim and modified as required.
 ********************************************************************/

Datum byteatrim(PG_FUNCTION_ARGS)
{
    bytea* string = PG_GETARG_BYTEA_PP(0);
    bytea* set = PG_GETARG_BYTEA_PP(1);
    bytea* ret = NULL;
    char *ptr = NULL, *end = NULL, *ptr2 = NULL, *ptr2start = NULL, *end2 = NULL;
    int m, stringlen, setlen;

    FUNC_CHECK_HUGE_POINTER(false, string, "byteatrim()");

    stringlen = VARSIZE_ANY_EXHDR(string);
    setlen = VARSIZE_ANY_EXHDR(set);

    if (stringlen <= 0 || setlen <= 0)
        PG_RETURN_BYTEA_P(string);

    m = stringlen;
    ptr = VARDATA_ANY(string);
    end = ptr + stringlen - 1;
    ptr2start = VARDATA_ANY(set);
    end2 = ptr2start + setlen - 1;

    while (m > 0) {
        ptr2 = ptr2start;
        while (ptr2 <= end2) {
            if (*ptr == *ptr2)
                break;
            ++ptr2;
        }
        if (ptr2 > end2)
            break;
        ptr++;
        m--;
    }

    while (m > 0) {
        ptr2 = ptr2start;
        while (ptr2 <= end2) {
            if (*end == *ptr2)
                break;
            ++ptr2;
        }
        if (ptr2 > end2)
            break;
        end--;
        m--;
    }

    ret = (bytea*)palloc(VARHDRSZ + m);
    SET_VARSIZE(ret, VARHDRSZ + m);
    if (m > 0) {
        errno_t ss_rc = memcpy_s(VARDATA(ret), m, ptr, m);
        securec_check(ss_rc, "\0", "\0");
    }
    PG_RETURN_BYTEA_P(ret);
}

/********************************************************************
 *
 * ltrim
 *
 * Syntax:
 *
 *	 text ltrim(text string, text set)
 *
 * Purpose:
 *
 *	 Returns string with initial characters removed up to the first
 *	 character not in set.
 *
 ********************************************************************/

Datum ltrim(PG_FUNCTION_ARGS)
{
    text* string = PG_GETARG_TEXT_PP(0);
    text* set = PG_GETARG_TEXT_PP(1);
    text* ret = NULL;

    FUNC_CHECK_HUGE_POINTER(false, string, "ltrim()");

    ret = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string), VARDATA_ANY(set), VARSIZE_ANY_EXHDR(set), true, false);

    if ((ret == NULL || 0 == VARSIZE_ANY_EXHDR(ret)) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT &&
        !ACCEPT_EMPTY_STR)
        PG_RETURN_NULL();
    else
        PG_RETURN_TEXT_P(ret);
}

/********************************************************************
 *
 * ltrim1 --- ltrim with set fixed as ' '
 *
 ********************************************************************/

Datum ltrim1(PG_FUNCTION_ARGS)
{
    text* string = PG_GETARG_TEXT_PP(0);
    text* ret = NULL;
    FUNC_CHECK_HUGE_POINTER(false, string, "ltrim1()");

    ret = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string), " ", 1, true, false);

    if ((ret == NULL || 0 == VARSIZE_ANY_EXHDR(ret)) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT &&
        !ACCEPT_EMPTY_STR)
        PG_RETURN_NULL();
    else
        PG_RETURN_TEXT_P(ret);
}

/********************************************************************
 *
 * rtrim
 *
 * Syntax:
 *
 *	 text rtrim(text string, text set)
 *
 * Purpose:
 *
 *	 Returns string with final characters removed after the last
 *	 character not in set.
 *
 ********************************************************************/

Datum rtrim(PG_FUNCTION_ARGS)
{
    text* string = PG_GETARG_TEXT_PP(0);
    text* set = PG_GETARG_TEXT_PP(1);
    text* ret = NULL;
    FUNC_CHECK_HUGE_POINTER(false, string, "rtrim()");

    ret = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string), VARDATA_ANY(set), VARSIZE_ANY_EXHDR(set), false, true);

    if ((ret == NULL || 0 == VARSIZE_ANY_EXHDR(ret)) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT &&
        !ACCEPT_EMPTY_STR)
        PG_RETURN_NULL();
    else
        PG_RETURN_TEXT_P(ret);
}

/********************************************************************
 *
 * rtrim1 --- rtrim with set fixed as ' '
 *
 ********************************************************************/

Datum rtrim1(PG_FUNCTION_ARGS)
{
    text* string = PG_GETARG_TEXT_PP(0);
    text* ret = NULL;
    FUNC_CHECK_HUGE_POINTER(false, string, "rtrim1");

    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && CHAR_COERCE_COMPAT
        && fcinfo->flinfo && fcinfo->flinfo->fn_oid == RTRIM1FUNCOID) {
        /*
         * char(n) will not ignore the tailing blanks in A_FORMAT compatibility.
         * here, we just return original input.
         */
        PG_RETURN_TEXT_P(string);
    } else {
        ret = dotrim(VARDATA_ANY(string), VARSIZE_ANY_EXHDR(string), " ", 1, false, true);
    }

    if ((ret == NULL || 0 == VARSIZE_ANY_EXHDR(ret)) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT &&
        !ACCEPT_EMPTY_STR)
        PG_RETURN_NULL();
    else
        PG_RETURN_TEXT_P(ret);
}

/********************************************************************
 *
 * translate
 *
 * Syntax:
 *
 *	 text translate(text string, text from, text to)
 *
 * Purpose:
 *
 *	 Returns string after replacing all occurrences of characters in from
 *	 with the corresponding character in to.  If from is longer than to,
 *	 occurrences of the extra characters in from are deleted.
 *	 Improved by Edwin Ramirez <ramirez@doc.mssm.edu>.
 *
 ********************************************************************/

Datum translate(PG_FUNCTION_ARGS)
{
    text* string = PG_GETARG_TEXT_PP(0);
    text* from = PG_GETARG_TEXT_PP(1);
    text* to = PG_GETARG_TEXT_PP(2);
    text* result = NULL;
    char *from_ptr = NULL, *to_ptr = NULL;
    char *source = NULL, *target = NULL;
    int m, fromlen, tolen, retlen, i;
    int worst_len;
    int len;
    int source_len;
    int from_index;
    errno_t ss_rc;

    FUNC_CHECK_HUGE_POINTER(false, string, "translate()");

    m = VARSIZE_ANY_EXHDR(string);
    if (m <= 0)
        PG_RETURN_TEXT_P(string);
    source = VARDATA_ANY(string);

    fromlen = VARSIZE_ANY_EXHDR(from);
    from_ptr = VARDATA_ANY(from);
    tolen = VARSIZE_ANY_EXHDR(to);
    to_ptr = VARDATA_ANY(to);

    /*
     * The worst-case expansion is to substitute a max-length character for a
     * single-byte character at each position of the string.
     */
    worst_len = pg_database_encoding_max_length() * m;

    /* check for integer overflow */
    if (worst_len / pg_database_encoding_max_length() != m)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("requested length too large")));

    result = (text*)palloc(worst_len + VARHDRSZ);
    target = VARDATA(result);
    retlen = 0;
    int tmp_len = 0;

    while (m > 0) {
        source_len = pg_mblen(source);
        from_index = 0;

        for (i = 0; i < fromlen; i += len) {
            len = pg_mblen(&from_ptr[i]);
            if (len == source_len && memcmp(source, &from_ptr[i], len) == 0)
                break;

            from_index++;
        }
        if (i < fromlen) {
            /* substitute */
            char* p = to_ptr;

            for (i = 0; i < from_index; i++) {
                p += pg_mblen(p);
                if (p >= (to_ptr + tolen))
                    break;
            }
            if (p < (to_ptr + tolen)) {
                len = pg_mblen(p);
                ss_rc = memcpy_s(target, worst_len - tmp_len, p, len);
                securec_check(ss_rc, "\0", "\0");
                target += len;
                tmp_len += len;
                retlen += len;
            }

        } else {
            /* no match, so copy */
            ss_rc = memcpy_s(target, worst_len - tmp_len, source, source_len);
            securec_check(ss_rc, "\0", "\0");
            target += source_len;
            tmp_len += source_len;
            retlen += source_len;
        }

        source += source_len;
        m -= source_len;
    }

    if (0 == retlen && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && !ACCEPT_EMPTY_STR) {
        PG_RETURN_NULL();
    } else {
        SET_VARSIZE(result, retlen + VARHDRSZ);

        /*
         * The function result is probably much bigger than needed, if we're using
         * a multibyte encoding, but it's not worth reallocating it; the result
         * probably won't live long anyway.
         */

        PG_RETURN_TEXT_P(result);
    }
}

/********************************************************************
 *
 * ascii
 *
 * Syntax:
 *
 *	 int ascii(text string)
 *
 * Purpose:
 *
 *	 Returns the decimal representation of the first character from
 *	 string.
 *	 If the string is empty we return 0.
 *	 If the database encoding is UTF8, we return the Unicode codepoint.
 *	 If the database encoding is any other multi-byte encoding, we
 *	 return the value of the first byte if it is an ASCII character
 *	 (range 1 .. 127), or raise an error.
 *	 For all other encodings we return the value of the first byte,
 *	 (range 1..255).
 *
 ********************************************************************/

Datum ascii(PG_FUNCTION_ARGS)
{
    text* string = PG_GETARG_TEXT_PP(0);
    int encoding = GetDatabaseEncoding();
    unsigned char* data = NULL;

    FUNC_CHECK_HUGE_POINTER(false, string, "ascii()");

    if (VARSIZE_ANY_EXHDR(string) <= 0)
        PG_RETURN_INT32(0);

    data = (unsigned char*)VARDATA_ANY(string);

    if (encoding == PG_UTF8 && *data > 127) {
        /* return the code point for Unicode */

        int result = 0, tbytes = 0, i;

        if (*data >= 0xF0) {
            result = *data & 0x07;
            tbytes = 3;
        } else if (*data >= 0xE0) {
            result = *data & 0x0F;
            tbytes = 2;
        } else {
            Assert(*data > 0xC0);
            result = *data & 0x1f;
            tbytes = 1;
        }

        Assert(tbytes > 0);

        for (i = 1; i <= tbytes; i++) {
            if ((data[i] & 0xC0) != 0x80) {
                report_invalid_encoding(PG_UTF8, (const char*)data, tbytes);
            } else {
                result = (result << 6) + (data[i] & 0x3f);
            }
        }

        PG_RETURN_INT32(result);
    } else {
        if (pg_encoding_max_length(encoding) > 1 && *data > 127)
            ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("requested character too large")));

        PG_RETURN_INT32((int32)*data);
    }
}

/********************************************************************
 *
 * chr
 *
 * Syntax:
 *
 *	 text chr(int val)
 *
 * Purpose:
 *
 *	Returns the character having the binary equivalent to val.
 *
 * For UTF8 we treat the argumwent as a Unicode code point.
 * For other multi-byte encodings we raise an error for arguments
 * outside the strict ASCII range (1..127).
 *
 * It's important that we don't ever return a value that is not valid
 * in the database encoding, so that this doesn't become a way for
 * invalid data to enter the database.
 *
 ********************************************************************/

Datum chr(PG_FUNCTION_ARGS)
{
    uint32 cvalue = PG_GETARG_UINT32(0);
    text* result = NULL;
    int encoding = GetDatabaseEncoding();

    if (encoding == PG_UTF8 && cvalue > 127) {
        /* for Unicode we treat the argument as a code point */
        int bytes;
        char* wch = NULL;

        /* We only allow valid Unicode code points */
        if (cvalue > 0x001fffff)
            ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("requested character too large for encoding: %u", cvalue)));

        if (cvalue > 0xffff)
            bytes = 4;
        else if (cvalue > 0x07ff)
            bytes = 3;
        else
            bytes = 2;

        result = (text*)palloc(VARHDRSZ + bytes);
        SET_VARSIZE(result, VARHDRSZ + bytes);
        wch = VARDATA(result);

        if (bytes == 2) {
            wch[0] = 0xC0 | ((cvalue >> 6) & 0x1F);
            wch[1] = 0x80 | (cvalue & 0x3F);
        } else if (bytes == 3) {
            wch[0] = 0xE0 | ((cvalue >> 12) & 0x0F);
            wch[1] = 0x80 | ((cvalue >> 6) & 0x3F);
            wch[2] = 0x80 | (cvalue & 0x3F);
        } else {
            wch[0] = 0xF0 | ((cvalue >> 18) & 0x07);
            wch[1] = 0x80 | ((cvalue >> 12) & 0x3F);
            wch[2] = 0x80 | ((cvalue >> 6) & 0x3F);
            wch[3] = 0x80 | (cvalue & 0x3F);
        }

    }

    else {
        bool is_mb = false;

        /*
         * Error out on arguments that make no sense or that we can't validly
         * represent in the encoding.
         */

        if (cvalue == 0)
            PG_RETURN_NULL();

        is_mb = pg_encoding_max_length(encoding) > 1;

        if ((is_mb && (cvalue > 127)) || (!is_mb && (cvalue > 255)))
            ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("requested character too large for encoding: %u", cvalue)));

        result = (text*)palloc(VARHDRSZ + 1);
        SET_VARSIZE(result, VARHDRSZ + 1);
        *VARDATA(result) = (char)cvalue;
    }

    PG_RETURN_TEXT_P(result);
}

/********************************************************************
 *
 * repeat
 *
 * Syntax:
 *
 *	 text repeat(text string, int val)
 *
 * Purpose:
 *
 *	Repeat string by val.
 *
 ********************************************************************/

Datum repeat(PG_FUNCTION_ARGS)
{
    text* string = PG_GETARG_TEXT_PP(0);
    int32 count = PG_GETARG_INT32(1);

    FUNC_CHECK_HUGE_POINTER(false, string, "repeat()");

    text* result = NULL;
    int slen, tlen;
    int i;
    char *cp = NULL, *sp = NULL;

    if (count < 0)
        count = 0;

    slen = VARSIZE_ANY_EXHDR(string);

    if (unlikely(pg_mul_s32_overflow(count, slen, &tlen)) || unlikely(pg_add_s32_overflow(tlen, VARHDRSZ, &tlen)))
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("requested length too large")));

    result = (text*)palloc(tlen);

    SET_VARSIZE(result, tlen);
    cp = VARDATA(result);
    sp = VARDATA_ANY(string);
    for (i = 0; i < count; i++) {
        errno_t ss_rc = memcpy_s(cp, tlen, sp, slen);
        securec_check(ss_rc, "\0", "\0");
        cp += slen;
        tlen -= slen;
    }

    if (0 == VARSIZE_ANY_EXHDR(result) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT &&
        !ACCEPT_EMPTY_STR && !RETURN_NS) {
        PG_RETURN_NULL();
    } else
        PG_RETURN_TEXT_P(result);
}
