/* -------------------------------------------------------------------------
 *
 * varchar.c
 *	  Functions for the built-in types char(n) and varchar(n).
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/varchar.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "access/tuptoaster.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "mb/pg_wchar.h"
#include "utils/sortsupport.h"
#include "vecexecutor/vectorbatch.h"

#include "miscadmin.h"

#define NOT_NULL_ARG(n)                                         \
    do {                                                        \
        if (PG_GETARG_POINTER(n) == NULL) {                     \
            ereport(ERROR,                                      \
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),       \
                    errmsg("null value not allowed"),           \
                    errhint("%dth argument is NULL.", n + 1))); \
        }                                                       \
    } while (0)


/* common code for bpchartypmodin and varchartypmodin */
static int32 anychar_typmodin(ArrayType* ta, const char* typname)
{
    int32 typmod;
    int32* tl = NULL;
    int n;

    tl = ArrayGetIntegerTypmods(ta, &n);

    /*
     * we're not too tense about good error message here because grammar
     * shouldn't allow wrong number of modifiers for CHAR
     */
    if (n != 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid type modifier")));

    if (*tl < 1)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("length for type %s must be at least 1", typname)));
    if (*tl > MaxAttrSize)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("length for type %s cannot exceed %d", typname, MaxAttrSize)));

    /*
     * For largely historical reasons, the typmod is VARHDRSZ plus the number
     * of characters; there is enough client-side code that knows about that
     * that we'd better not change it.
     */
    typmod = VARHDRSZ + *tl;

    return typmod;
}

/* common code for bpchartypmodout and varchartypmodout */
static char* anychar_typmodout(int32 typmod)
{
    const size_t buffer_len = 64;

    char* res = (char*)palloc(buffer_len);
    errno_t ss_rc = 0;

    if (typmod > VARHDRSZ) {
        ss_rc = snprintf_s(res, buffer_len, buffer_len - 1, "(%d)", (int)(typmod - VARHDRSZ));
        securec_check_ss(ss_rc, "\0", "\0");
    } else
        *res = '\0';

    return res;
}

/*
 * CHAR() and VARCHAR() types are part of the SQL standard. CHAR()
 * is for blank-padded string whose length is specified in CREATE TABLE.
 * VARCHAR is for storing string whose length is at most the length specified
 * at CREATE TABLE time.
 *
 * It's hard to implement these types because we cannot figure out
 * the length of the type from the type itself. I changed (hopefully all) the
 * fmgr calls that invoke input functions of a data type to supply the
 * length also. (eg. in INSERTs, we have the tupleDescriptor which contains
 * the length of the attributes and hence the exact length of the char() or
 * varchar(). We pass this to bpcharin() or varcharin().) In the case where
 * we cannot determine the length, we pass in -1 instead and the input
 * converter does not enforce any length check.
 *
 * We actually implement this as a varlena so that we don't have to pass in
 * the length for the comparison functions. (The difference between these
 * types and "text" is that we truncate and possibly blank-pad the string
 * at insertion time.)
 *
 *															  - ay 6/95
 */

/*****************************************************************************
 *	 bpchar - char()														 *
 *****************************************************************************/

/*
 * bpchar_input -- common guts of bpcharin and bpcharrecv
 *
 * s is the input text of length len (may not be null-terminated)
 * atttypmod is the typmod value to apply
 *
 * Note that atttypmod is measured in characters, which
 * is not necessarily the same as the number of bytes.
 *
 * If the input string is too long, raise an error, unless the extra
 * characters are spaces, in which case they're truncated.  (per SQL)
 */
static BpChar* bpchar_input(const char* s, size_t len, int32 atttypmod)
{
    BpChar* result = NULL;
    char* r = NULL;
    size_t maxlen;
    errno_t ss_rc = 0;

    /* If typmod is -1 (or invalid), use the actual string length */
    if (atttypmod < (int32)VARHDRSZ)
        maxlen = len;
    else {

        maxlen = atttypmod - VARHDRSZ;
        if (len > maxlen) {

            if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && CHAR_COERCE_COMPAT)
                ereport(ERROR,
                    (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                        errmsg("value too long for type character(%d)", (int)maxlen)));

            /* Verify that extra characters are spaces, and clip them off */
            size_t mbmaxlen = pg_mbcharcliplen(s, len, maxlen);
            size_t j;

            /*
             * at this point, len is the actual BYTE length of the input
             * string, maxlen is the max number of CHARACTERS allowed for this
             * bpchar type, mbmaxlen is the length in BYTES of those chars.
             */
            for (j = mbmaxlen; j < len; j++) {
                if (s[j] != ' ')
                    ereport(ERROR,
                        (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                            errmsg("value too long for type character(%d)", (int)maxlen)));
            }

            /*
             * Now we set maxlen to the necessary byte length, not the number
             * of CHARACTERS!
             */
            maxlen = len = mbmaxlen;
        }
    }

    result = (BpChar*)palloc(maxlen + VARHDRSZ);
    SET_VARSIZE(result, maxlen + VARHDRSZ);
    r = VARDATA(result);
    if (len > 0) {
        ss_rc = memcpy_s(r, len, s, len);
        securec_check(ss_rc, "\0", "\0");
    }

    /* blank pad the string if necessary */
    if (maxlen > len) {
        ss_rc = memset_s(r + len, maxlen - len, ' ', maxlen - len);
        securec_check(ss_rc, "\0", "\0");
    }

    return result;
}

/*
 * Convert a C string to CHARACTER internal representation.  atttypmod
 * is the declared length of the type plus VARHDRSZ.
 */
Datum bpcharin(PG_FUNCTION_ARGS)
{
    char* s = PG_GETARG_CSTRING(0);

#ifdef NOT_USED
    Oid typelem = PG_GETARG_OID(1);
#endif
    int32 atttypmod = PG_GETARG_INT32(2);
    BpChar* result = NULL;

    result = bpchar_input(s, strlen(s), atttypmod);
    PG_RETURN_BPCHAR_P(result);
}

/*
 * Convert a CHARACTER value to a C string.
 *
 * Uses the text conversion functions, which is only appropriate if BpChar
 * and text are equivalent types.
 */
Datum bpcharout(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);

    PG_RETURN_CSTRING(TextDatumGetCString(txt));
}

/*
 *		bpcharrecv			- converts external binary format to bpchar
 */
Datum bpcharrecv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);

#ifdef NOT_USED
    Oid typelem = PG_GETARG_OID(1);
#endif
    int32 atttypmod = PG_GETARG_INT32(2);
    BpChar* result = NULL;
    char* str = NULL;
    int nbytes;

    str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    result = bpchar_input(str, nbytes, atttypmod);
    pfree_ext(str);
    PG_RETURN_BPCHAR_P(result);
}

/*
 *		bpcharsend			- converts bpchar to binary format
 */
Datum bpcharsend(PG_FUNCTION_ARGS)
{
    /* Exactly the same as textsend, so share code */
    return textsend(fcinfo);
}

/*
 * Converts a CHARACTER type to the specified size.
 *
 * maxlen is the typmod, ie, declared length plus VARHDRSZ bytes.
 * isExplicit is true if this is for an explicit cast to char(N).
 *
 * Truncation rules: for an explicit cast, silently truncate to the given
 * length; for an implicit cast, raise error unless extra characters are
 * all spaces.	(This is sort-of per SQL: the spec would actually have us
 * raise a "completion condition" for the explicit cast case, but Postgres
 * hasn't got such a concept.)
 */
Datum bpchar(PG_FUNCTION_ARGS)
{
    BpChar* source = PG_GETARG_BPCHAR_PP(0);
    int32 maxlen = PG_GETARG_INT32(1);
    bool isExplicit = PG_GETARG_BOOL(2);
    BpChar* result = NULL;
    int32 len;
    char* r = NULL;
    char* s = NULL;
    int i;
    errno_t ss_rc = 0;

    /* No work if typmod is invalid */
    if (maxlen < (int32)VARHDRSZ)
        PG_RETURN_BPCHAR_P(source);

    maxlen -= VARHDRSZ;

    len = VARSIZE_ANY_EXHDR(source);
    s = VARDATA_ANY(source);

    /* No work if supplied data matches typmod already */
    if (len == maxlen)
        PG_RETURN_BPCHAR_P(source);

    if (len > maxlen) {

        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && CHAR_COERCE_COMPAT)
            ereport(ERROR,
                (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                    errmsg("value too long for type character(%d)", maxlen)));

        /* Verify that extra characters are spaces, and clip them off */
        size_t maxmblen;

        maxmblen = pg_mbcharcliplen(s, len, maxlen);

        if (!isExplicit) {
            for (i = maxmblen; i < len; i++)
                if (s[i] != ' ')
                    ereport(ERROR,
                        (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                            errmsg("value too long for type character(%d)", maxlen)));
        }

        len = maxmblen;

        /*
         * At this point, maxlen is the necessary byte length, not the number
         * of CHARACTERS!
         *
         * Now, only explicit cast char type data should pad blank space.
         */
        if (!isExplicit)
            maxlen = len;
    }

    Assert(maxlen >= len);

    result = (BpChar*)palloc(maxlen + VARHDRSZ);
    SET_VARSIZE(result, maxlen + VARHDRSZ);
    r = VARDATA(result);

    if (len > 0) {
        ss_rc = memcpy_s(r, len, s, len);
        securec_check(ss_rc, "\0", "\0");
    }

    /* blank pad the string if necessary */
    if (maxlen > len) {
        ss_rc = memset_s(r + len, maxlen - len, ' ', maxlen - len);
        securec_check(ss_rc, "\0", "\0");
    }

    PG_RETURN_BPCHAR_P(result);
}

/* char_bpchar()
 * Convert char to bpchar(1).
 */
Datum char_bpchar(PG_FUNCTION_ARGS)
{
    char c = PG_GETARG_CHAR(0);
    BpChar* result = NULL;

    result = (BpChar*)palloc(VARHDRSZ + 1);

    SET_VARSIZE(result, VARHDRSZ + 1);
    *(VARDATA(result)) = c;

    PG_RETURN_BPCHAR_P(result);
}

/* bpchar_name()
 * Converts a bpchar() type to a NameData type.
 */
Datum bpchar_name(PG_FUNCTION_ARGS)
{
    BpChar* s = PG_GETARG_BPCHAR_PP(0);
    char* s_data = NULL;
    Name result;
    int len;
    errno_t ss_rc = 0;

    len = VARSIZE_ANY_EXHDR(s);
    s_data = VARDATA_ANY(s);

    /* Truncate oversize input */
    if (len >= NAMEDATALEN)
        len = pg_mbcliplen(s_data, len, NAMEDATALEN - 1);

    /* Remove trailing blanks */
    while (len > 0) {
        if (s_data[len - 1] != ' ')
            break;
        len--;
    }

    /* We use palloc0 here to ensure result is zero-padded */
    result = (Name)palloc0(NAMEDATALEN);
    if (len > 0) {
        ss_rc = memcpy_s(NameStr(*result), len, s_data, len);
        securec_check(ss_rc, "\0", "\0");
    }

    PG_RETURN_NAME(result);
}

/* name_bpchar()
 * Converts a NameData type to a bpchar type.
 *
 * Uses the text conversion functions, which is only appropriate if BpChar
 * and text are equivalent types.
 */
Datum name_bpchar(PG_FUNCTION_ARGS)
{
    Name s = PG_GETARG_NAME(0);
    BpChar* result = NULL;

    result = (BpChar*)cstring_to_text(NameStr(*s));
    PG_RETURN_BPCHAR_P(result);
}

Datum bpchartypmodin(PG_FUNCTION_ARGS)
{
    ArrayType* ta = PG_GETARG_ARRAYTYPE_P(0);

    PG_RETURN_INT32(anychar_typmodin(ta, "char"));
}

Datum bpchartypmodout(PG_FUNCTION_ARGS)
{
    int32 typmod = PG_GETARG_INT32(0);

    PG_RETURN_CSTRING(anychar_typmodout(typmod));
}

/*****************************************************************************
 *	 varchar - varchar(n)
 *
 * Note: varchar piggybacks on type text for most operations, and so has no
 * C-coded functions except for I/O and typmod checking.
 *****************************************************************************/

/*
 * varchar_input -- common guts of varcharin and varcharrecv
 *
 * s is the input text of length len (may not be null-terminated)
 * atttypmod is the typmod value to apply
 *
 * Note that atttypmod is measured in characters, which
 * is not necessarily the same as the number of bytes.
 *
 * If the input string is too long, raise an error, unless the extra
 * characters are spaces, in which case they're truncated.  (per SQL)
 *
 * Uses the C string to text conversion function, which is only appropriate
 * if VarChar and text are equivalent types.
 */
static VarChar* varchar_input(const char* s, size_t len, int32 atttypmod)
{
    VarChar* result = NULL;
    size_t maxlen;

    maxlen = atttypmod - VARHDRSZ;

    if (len > maxlen && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && CHAR_COERCE_COMPAT)
        ereport(ERROR,
            (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                errmsg("value too long for type character varying(%d)", (int)maxlen)));

    if (atttypmod >= (int32)VARHDRSZ && len > maxlen) {
        /* Verify that extra characters are spaces, and clip them off */
        size_t mbmaxlen = pg_mbcharcliplen(s, len, maxlen);
        size_t j;

        for (j = mbmaxlen; j < len; j++) {
            if (s[j] != ' ')
                ereport(ERROR,
                    (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                        errmsg("value too long for type character varying(%d)", (int)maxlen)));
        }

        len = mbmaxlen;
    }

    result = (VarChar*)cstring_to_text_with_len(s, len);
    return result;
}

/*
 * Convert a C string to VARCHAR internal representation.  atttypmod
 * is the declared length of the type plus VARHDRSZ.
 */
Datum varcharin(PG_FUNCTION_ARGS)
{
    char* s = PG_GETARG_CSTRING(0);

#ifdef NOT_USED
    Oid typelem = PG_GETARG_OID(1);
#endif
    int32 atttypmod = PG_GETARG_INT32(2);
    VarChar* result = NULL;

    result = varchar_input(s, strlen(s), atttypmod);
    PG_RETURN_VARCHAR_P(result);
}

/*
 * Convert a VARCHAR value to a C string.
 *
 * Uses the text to C string conversion function, which is only appropriate
 * if VarChar and text are equivalent types.
 */
Datum varcharout(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);

    PG_RETURN_CSTRING(TextDatumGetCString(txt));
}

/*
 *		varcharrecv			- converts external binary format to varchar
 */
Datum varcharrecv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);

#ifdef NOT_USED
    Oid typelem = PG_GETARG_OID(1);
#endif
    int32 atttypmod = PG_GETARG_INT32(2);
    VarChar* result = NULL;
    char* str = NULL;
    int nbytes;

    str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    result = varchar_input(str, nbytes, atttypmod);
    pfree_ext(str);
    PG_RETURN_VARCHAR_P(result);
}

/*
 *		varcharsend			- converts varchar to binary format
 */
Datum varcharsend(PG_FUNCTION_ARGS)
{
    /* Exactly the same as textsend, so share code */
    return textsend(fcinfo);
}

/*
 * varchar_transform()
 * Flatten calls to varchar's length coercion function that set the new maximum
 * length >= the previous maximum length.  We can ignore the isExplicit
 * argument, since that only affects truncation cases.
 */
Datum varchar_transform(PG_FUNCTION_ARGS)
{
    FuncExpr* expr = (FuncExpr*)PG_GETARG_POINTER(0);
    Node* ret = NULL;
    Node* typmod = NULL;

    Assert(IsA(expr, FuncExpr));
    Assert(list_length(expr->args) >= 2);

    typmod = (Node*)lsecond(expr->args);

    if (IsA(typmod, Const) && !((Const*)typmod)->constisnull) {
        Node* source = (Node*)linitial(expr->args);
        int32 old_typmod = exprTypmod(source);
        int32 new_typmod = DatumGetInt32(((Const*)typmod)->constvalue);
        int32 old_max = old_typmod - VARHDRSZ;
        int32 new_max = new_typmod - VARHDRSZ;

        if (new_typmod < 0 || (old_typmod >= 0 && old_max <= new_max))
            ret = relabel_to_typmod(source, new_typmod);
    }

    PG_RETURN_POINTER(ret);
}

/*
 * Converts a VARCHAR type to the specified size.
 *
 * maxlen is the typmod, ie, declared length plus VARHDRSZ bytes.
 * isExplicit is true if this is for an explicit cast to varchar(N).
 *
 * Truncation rules: for an explicit cast, silently truncate to the given
 * length; for an implicit cast, raise error unless extra characters are
 * all spaces.	(This is sort-of per SQL: the spec would actually have us
 * raise a "completion condition" for the explicit cast case, but Postgres
 * hasn't got such a concept.)
 */
Datum varchar(PG_FUNCTION_ARGS)
{
    VarChar* source = PG_GETARG_VARCHAR_PP(0);
    int32 typmod = PG_GETARG_INT32(1);
    bool isExplicit = PG_GETARG_BOOL(2);
    int32 len, maxlen;
    size_t maxmblen;
    int i;
    char* s_data = NULL;

    len = VARSIZE_ANY_EXHDR(source);
    s_data = VARDATA_ANY(source);
    maxlen = typmod - VARHDRSZ;

    /* No work if typmod is invalid or supplied data fits it already */
    if (maxlen < 0 || len <= maxlen)
        PG_RETURN_VARCHAR_P(source);

    /* only reach here if string is too long... */
    if (len > maxlen && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && CHAR_COERCE_COMPAT)
        ereport(ERROR,
            (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                errmsg("value too long for type character varying(%d)", maxlen)));

    /* truncate multibyte string preserving multibyte boundary */
    maxmblen = pg_mbcharcliplen(s_data, len, maxlen);

    if (!isExplicit) {
        for (i = maxmblen; i < len; i++)
            if (s_data[i] != ' ')
                ereport(ERROR,
                    (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                        errmsg("value too long for type character varying(%d)", maxlen)));
    }

    PG_RETURN_VARCHAR_P((VarChar*)cstring_to_text_with_len(s_data, maxmblen));
}

Datum varchartypmodin(PG_FUNCTION_ARGS)
{
    ArrayType* ta = PG_GETARG_ARRAYTYPE_P(0);

    PG_RETURN_INT32(anychar_typmodin(ta, "varchar"));
}

Datum varchartypmodout(PG_FUNCTION_ARGS)
{
    int32 typmod = PG_GETARG_INT32(0);

    PG_RETURN_CSTRING(anychar_typmodout(typmod));
}

/*****************************************************************************
 * Exported functions
 *****************************************************************************/

/* "True" length (not counting trailing blanks) of a BpChar */
int bcTruelen(BpChar* arg)
{
    char* s = VARDATA_ANY(arg);
    int i;
    int len;

    len = VARSIZE_ANY_EXHDR(arg);
    for (i = len - 1; i >= 0; i--) {
        if (s[i] != ' ')
            break;
    }
    return i + 1;
}

/*
 * len stands for the length of char, we can use bpchartruelen
 * to get the ture length of bpchar faster.
 */
int bpchartruelen(const char* s, int len)
{
    int i;

    /*
     * Note that we rely on the assumption that ' ' is a singleton unit on
     * every supported multibyte server encoding.
     */
    for (i = len - 1; i >= 0; i--) {
        if (s[i] != ' ')
            break;
    }
    return i + 1;
}

// return number of char in a char(n) type string.
// when calculating the length,we do not ignoring the trailing
// spaces, which is different with pg9.2
Datum bpcharlen(PG_FUNCTION_ARGS)
{
    BpChar* arg = PG_GETARG_BPCHAR_PP(0);
    int len;

    /* get number of bytes, ignoring trailing spaces */
    if (DB_IS_CMPT(PG_FORMAT | B_FORMAT)) {
        len = bcTruelen(arg);
    } else {
        len = VARSIZE_ANY_EXHDR(arg);
    }

    /* in multibyte encoding, convert to number of characters */
    if (pg_database_encoding_max_length() != 1)
        len = pg_mbstrlen_with_len(VARDATA_ANY(arg), len);

    PG_RETURN_INT32(len);
}

// return number of byte in a char(n) type string.
// when calculating the length,we do not ignoring the trailing spaces.
Datum bpcharlenb(PG_FUNCTION_ARGS)
{
    BpChar* arg = PG_GETARG_BPCHAR_PP(0);
    int len;

    /* get number of bytes, not ignoring trailing spaces */
    len = VARSIZE_ANY_EXHDR(arg);

    PG_RETURN_INT32(len);
}

Datum bpcharoctetlen(PG_FUNCTION_ARGS)
{
    Datum arg = PG_GETARG_DATUM(0);

    FUNC_CHECK_HUGE_POINTER(PG_ARGISNULL(0), DatumGetPointer(arg), "bpcharoctetlen()");

    /* We need not detoast the input at all */
    PG_RETURN_INT32(toast_raw_datum_size(arg) - VARHDRSZ);
}

/*****************************************************************************
 *	Comparison Functions used for bpchar
 *
 * Note: btree indexes need these routines not to leak memory; therefore,
 * be careful to free working copies of toasted datums.  Most places don't
 * need to be so careful.
 *****************************************************************************/

Datum bpchareq(PG_FUNCTION_ARGS)
{
    BpChar* arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar* arg2 = PG_GETARG_BPCHAR_PP(1);
    int len1, len2;
    bool result = false;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    /*
     * Since we only care about equality or not-equality, we can avoid all the
     * expense of strcoll() here, and just do bitwise comparison.
     */
    if (len1 != len2)
        result = false;
    else
        result = (memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), len1) == 0);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result);
}

Datum bpcharne(PG_FUNCTION_ARGS)
{
    BpChar* arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar* arg2 = PG_GETARG_BPCHAR_PP(1);
    int len1, len2;
    bool result = false;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    /*
     * Since we only care about equality or not-equality, we can avoid all the
     * expense of strcoll() here, and just do bitwise comparison.
     */
    if (len1 != len2)
        result = true;
    else
        result = (memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), len1) != 0);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result);
}

int bpcharcase(PG_FUNCTION_ARGS)
{
    BpChar* arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar* arg2 = PG_GETARG_BPCHAR_PP(1);
    int len1, len2;
    int cmp;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(VARDATA_ANY(arg1), len1, VARDATA_ANY(arg2), len2, PG_GET_COLLATION());

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);
    return cmp;
}
Datum bpcharlt(PG_FUNCTION_ARGS)
{
    int cmp = bpcharcase(fcinfo);
    PG_RETURN_BOOL(cmp < 0);
}

Datum bpcharle(PG_FUNCTION_ARGS)
{
    int cmp = bpcharcase(fcinfo);
    PG_RETURN_BOOL(cmp <= 0);
}

Datum bpchargt(PG_FUNCTION_ARGS)
{
    int cmp = bpcharcase(fcinfo);
    PG_RETURN_BOOL(cmp > 0);
}

Datum bpcharge(PG_FUNCTION_ARGS)
{
    int cmp = bpcharcase(fcinfo);
    PG_RETURN_BOOL(cmp >= 0);
}

Datum bpcharcmp(PG_FUNCTION_ARGS)
{
    int cmp = bpcharcase(fcinfo);
    PG_RETURN_INT32(cmp);
}

Datum bpchar_sortsupport(PG_FUNCTION_ARGS)
{
    SortSupport ssup = (SortSupport)PG_GETARG_POINTER(0);
    Oid collid = ssup->ssup_collation;
    MemoryContext oldcontext;

    oldcontext = MemoryContextSwitchTo(ssup->ssup_cxt);

    /* Use generic string SortSupport */
    varstr_sortsupport(ssup, collid, true);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_VOID();
}

Datum bpchar_larger(PG_FUNCTION_ARGS)
{
    BpChar* arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar* arg2 = PG_GETARG_BPCHAR_PP(1);
    int len1, len2;
    int cmp;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(VARDATA_ANY(arg1), len1, VARDATA_ANY(arg2), len2, PG_GET_COLLATION());

    PG_RETURN_BPCHAR_P((cmp >= 0) ? arg1 : arg2);
}

Datum bpchar_smaller(PG_FUNCTION_ARGS)
{
    BpChar* arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar* arg2 = PG_GETARG_BPCHAR_PP(1);
    int len1, len2;
    int cmp;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    cmp = varstr_cmp(VARDATA_ANY(arg1), len1, VARDATA_ANY(arg2), len2, PG_GET_COLLATION());

    PG_RETURN_BPCHAR_P((cmp <= 0) ? arg1 : arg2);
}

/* Cast int4 -> bpchar */
Datum int4_bpchar(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    BpChar* result = NULL;
    char* s = (char*)palloc(12); /* sign, 10 digits, '\0' */

    pg_ltoa(arg1, s);
    result = bpchar_input(s, strlen(s), -1);
    PG_RETURN_BPCHAR_P(result);
}

/*
 * @Description: int1 convert to bpchar.
 * @in arg1 - tinyint type numeric.
 * @return bpchar type string.
 */
Datum int1_bpchar(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    char* tmp = NULL;
    Datum result;

    tmp = DatumGetCString(DirectFunctionCall1(int1out, arg1));

    result = DirectFunctionCall3(bpcharin, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

/*
 * @Description: int2 convert to bpchar.
 * @in arg1 - smallint type numeric.
 * @return bpchar type string.
 */
Datum int2_bpchar(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    char* tmp = NULL;
    Datum result;

    tmp = DatumGetCString(DirectFunctionCall1(int2out, arg1));

    result = DirectFunctionCall3(bpcharin, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

/*
 * @Description: float4 convert to bpchar.
 * @in arg1 - float4 type numeric.
 * @return - bpchar type string.
 */
Datum float4_bpchar(PG_FUNCTION_ARGS)
{
    float4 arg1 = PG_GETARG_FLOAT4(0);
    char* tmp = NULL;
    Datum result;

    tmp = DatumGetCString(DirectFunctionCall1(float4out, Float4GetDatum(arg1)));

    result = DirectFunctionCall3(bpcharin, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

/*
 * @Description: float8 convert to bpchar.
 * @in arg1 - float8 type numeric.
 * @return - bpchar type string.
 */
Datum float8_bpchar(PG_FUNCTION_ARGS)
{
    float8 arg1 = PG_GETARG_FLOAT8(0);
    char* tmp = NULL;
    Datum result;

    tmp = DatumGetCString(DirectFunctionCall1(float8out, Float8GetDatum(arg1)));

    result = DirectFunctionCall3(bpcharin, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

/*
 * bpchar needs a specialized hash function because we want to ignore
 * trailing blanks in comparisons.
 *
 * Note: currently there is no need for locale-specific behavior here,
 * but if we ever change the semantics of bpchar comparison to trust
 * strcoll() completely, we'd need to do something different in non-C locales.
 */
Datum hashbpchar(PG_FUNCTION_ARGS)
{
    BpChar* key = PG_GETARG_BPCHAR_PP(0);
    char* keydata = NULL;
    int keylen;
    Datum result;

    keydata = VARDATA_ANY(key);
    keylen = bcTruelen(key);

    result = hash_any((unsigned char*)keydata, keylen);

    /* Avoid leaking memory for toasted inputs */
    PG_FREE_IF_COPY(key, 0);

    return result;
}

/*
 * The following operators support character-by-character comparison
 * of bpchar datums, to allow building indexes suitable for LIKE clauses.
 * Note that the regular bpchareq/bpcharne comparison operators are assumed
 * to be compatible with these!
 */

static int internal_bpchar_pattern_compare(BpChar* arg1, BpChar* arg2)
{
    int result;
    int len1, len2;

    len1 = bcTruelen(arg1);
    len2 = bcTruelen(arg2);

    result = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
    if (result != 0)
        return result;
    else if (len1 < len2)
        return -1;
    else if (len1 > len2)
        return 1;
    else
        return 0;
}

Datum bpchar_pattern_lt(PG_FUNCTION_ARGS)
{
    NOT_NULL_ARG(0);
    NOT_NULL_ARG(1);

    BpChar* arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar* arg2 = PG_GETARG_BPCHAR_PP(1);
    int result;

    result = internal_bpchar_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result < 0);
}

Datum bpchar_pattern_le(PG_FUNCTION_ARGS)
{
    NOT_NULL_ARG(0);
    NOT_NULL_ARG(1);

    BpChar* arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar* arg2 = PG_GETARG_BPCHAR_PP(1);
    int result;

    result = internal_bpchar_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result <= 0);
}

Datum bpchar_pattern_ge(PG_FUNCTION_ARGS)
{
    NOT_NULL_ARG(0);
    NOT_NULL_ARG(1);

    BpChar* arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar* arg2 = PG_GETARG_BPCHAR_PP(1);
    int result;

    result = internal_bpchar_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result >= 0);
}

Datum bpchar_pattern_gt(PG_FUNCTION_ARGS)
{
    NOT_NULL_ARG(0);
    NOT_NULL_ARG(1);

    BpChar* arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar* arg2 = PG_GETARG_BPCHAR_PP(1);
    int result;

    result = internal_bpchar_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result > 0);
}

Datum btbpchar_pattern_cmp(PG_FUNCTION_ARGS)
{
    NOT_NULL_ARG(0);
    NOT_NULL_ARG(1);

    BpChar* arg1 = PG_GETARG_BPCHAR_PP(0);
    BpChar* arg2 = PG_GETARG_BPCHAR_PP(1);
    int result;

    result = internal_bpchar_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_INT32(result);
}

/* add for  nvarchar2 data type */
static NVarChar2* nvarchar2_input(const char* s, size_t len, int32 atttypmod)
{
    NVarChar2* result = NULL;
    size_t maxlen;

    maxlen = atttypmod - VARHDRSZ;

    if (len > maxlen && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && CHAR_COERCE_COMPAT)
        ereport(ERROR,
            (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                errmsg("value too long for type nvarchar2(%d)", (int)maxlen)));

    if (atttypmod >= (int32)VARHDRSZ && len > maxlen) {
        /* Verify that extra characters are spaces, and clip them off */
        size_t mbmaxlen = pg_mbcharcliplen_orig(s, len, maxlen);
        size_t j;

        for (j = mbmaxlen; j < len; j++) {
            if (s[j] != ' ')
                ereport(ERROR,
                    (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                        errmsg("value too long for type nvarchar2(%d)", (int)maxlen)));
        }

        len = mbmaxlen;
    }

    result = (NVarChar2*)cstring_to_text_with_len(s, len);
    return result;
}

/*
 * Convert a C string to VARCHAR internal representation.  atttypmod
 * is the declared length of the type plus VARHDRSZ.
 */
Datum nvarchar2in(PG_FUNCTION_ARGS)
{
    char* s = PG_GETARG_CSTRING(0);

#ifdef NOT_USED
    Oid typelem = PG_GETARG_OID(1);
#endif
    int32 atttypmod = PG_GETARG_INT32(2);
    NVarChar2* result = NULL;

    result = nvarchar2_input(s, strlen(s), atttypmod);
    PG_RETURN_NVARCHAR2_P(result);
}

/*
 * Convert a VARCHAR value to a C string.
 *
 * Uses the text to C string conversion function, which is only appropriate
 * if VarChar and text are equivalent types.
 */
Datum nvarchar2out(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);

    PG_RETURN_CSTRING(TextDatumGetCString(txt));
}

/*
 *		varcharrecv			- converts external binary format to varchar
 */
Datum nvarchar2recv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);

#ifdef NOT_USED
    Oid typelem = PG_GETARG_OID(1);
#endif
    int32 atttypmod = PG_GETARG_INT32(2);
    NVarChar2* result = NULL;
    char* str = NULL;
    int nbytes;

    str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    result = nvarchar2_input(str, nbytes, atttypmod);
    pfree_ext(str);
    PG_RETURN_NVARCHAR2_P(result);
}

/*
 *		varcharsend			- converts varchar to binary format
 */
Datum nvarchar2send(PG_FUNCTION_ARGS)
{
    /* Exactly the same as textsend, so share code */
    return textsend(fcinfo);
}

/*
 * Converts a VARCHAR type to the specified size.
 *
 * maxlen is the typmod, ie, declared length plus VARHDRSZ bytes.
 * isExplicit is true if this is for an explicit cast to varchar(N).
 *
 * Truncation rules: for an explicit cast, silently truncate to the given
 * length; for an implicit cast, raise error unless extra characters are
 * all spaces.	(This is sort-of per SQL: the spec would actually have us
 * raise a "completion condition" for the explicit cast case, but Postgres
 * hasn't got such a concept.)
 */
Datum nvarchar2(PG_FUNCTION_ARGS)
{
    NVarChar2* source = PG_GETARG_NVARCHAR2_PP(0);
    int32 typmod = PG_GETARG_INT32(1);
    bool isExplicit = PG_GETARG_BOOL(2);
    int32 len, maxlen;
    size_t maxmblen;
    int i;
    char* s_data = NULL;

    len = VARSIZE_ANY_EXHDR(source);
    s_data = VARDATA_ANY(source);
    maxlen = typmod - VARHDRSZ;

    /* No work if typmod is invalid or supplied data fits it already */
    if (maxlen < 0 || len <= maxlen)
        PG_RETURN_NVARCHAR2_P(source);

    if (len > maxlen && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && CHAR_COERCE_COMPAT)
        ereport(ERROR,
            (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                errmsg("value too long for type nvarchar2(%d)", maxlen)));

    /* only reach here if string is too long... */

    /* truncate multibyte string preserving multibyte boundary */
    maxmblen = pg_mbcharcliplen_orig(s_data, len, maxlen);

    if (!isExplicit) {
        for (i = maxmblen; i < len; i++)
            if (s_data[i] != ' ')
                ereport(ERROR,
                    (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                        errmsg("value too long for type nvarchar2(%d)", maxlen)));
    }

    PG_RETURN_NVARCHAR2_P((NVarChar2*)cstring_to_text_with_len(s_data, maxmblen));
}

Datum nvarchar2typmodin(PG_FUNCTION_ARGS)
{
    ArrayType* ta = PG_GETARG_ARRAYTYPE_P(0);

    PG_RETURN_INT32(anychar_typmodin(ta, "nvarchar2"));
}

Datum nvarchar2typmodout(PG_FUNCTION_ARGS)
{
    int32 typmod = PG_GETARG_INT32(0);

    PG_RETURN_CSTRING(anychar_typmodout(typmod));
}

/*
 * Convert all C string to VARCHAR internal representation.  atttypmod
 * is the declared length of the type plus VARHDRSZ.
 * vectorize function
 */

static FORCE_INLINE bool datumlike(Datum datum1, Datum datum2)
{
    Size size1 = VARSIZE_ANY_EXHDR(datum1);
    Size size2 = VARSIZE_ANY_EXHDR(datum2);
    return GenericMatchText(VARDATA_ANY(datum1), size1, VARDATA_ANY(datum2), size2) == LIKE_TRUE;
}

ScalarVector* vtextlike(PG_FUNCTION_ARGS)
{
    ScalarVector* VecParg1 = PG_GETARG_VECTOR(0);
    ScalarVector* VecParg2 = PG_GETARG_VECTOR(1);
    int32 nvalues = PG_GETARG_INT32(2);
    ScalarVector* VecResult = PG_GETARG_VECTOR(3);
    bool* pselection = PG_GETARG_SELECTION(4);
    ScalarValue* presult = VecResult->m_vals;
    ScalarValue* parg1 = VecParg1->m_vals;
    ScalarValue* parg2 = VecParg2->m_vals;
    uint8* flag1 = VecParg1->m_flag;
    uint8* flag2 = VecParg2->m_flag;
    uint8* ResultFlag = VecResult->m_flag;
    Datum arg1;
    Datum arg2;
    int i;
    bool result = false;

    if (pselection != NULL) {
        for (i = 0; i < nvalues; i++) {
            if (pselection[i]) {
                if (IS_NULL(flag1[i]) || IS_NULL(flag2[i])) {
                    SET_NULL(ResultFlag[i]);
                } else {
                    arg1 = ScalarVector::Decode(parg1[i]);
                    arg2 = ScalarVector::Decode(parg2[i]);
                    result = datumlike(arg1, arg2);
                    presult[i] = result;
                    SET_NOTNULL(ResultFlag[i]);
                }
            }
        }
    } else {
        for (i = 0; i < nvalues; i++) {
            if (IS_NULL(flag1[i]) || IS_NULL(flag2[i])) {
                SET_NULL(ResultFlag[i]);
            } else {
                arg1 = ScalarVector::Decode(parg1[i]);
                arg2 = ScalarVector::Decode(parg2[i]);
                result = datumlike(arg1, arg2);
                presult[i] = result;
                SET_NOTNULL(ResultFlag[i]);
            }
        }
    }

    VecResult->m_rows = nvalues;

    return VecResult;
}

ScalarVector* vtextnlike(PG_FUNCTION_ARGS)
{
    ScalarVector* VecParg1 = PG_GETARG_VECTOR(0);
    ScalarVector* VecParg2 = PG_GETARG_VECTOR(1);
    int32 nvalues = PG_GETARG_INT32(2);
    ScalarVector* VecResult = PG_GETARG_VECTOR(3);
    bool* pselection = PG_GETARG_SELECTION(4);
    ScalarValue* parg1 = VecParg1->m_vals;
    ScalarValue* parg2 = VecParg2->m_vals;
    uint8* flag1 = VecParg1->m_flag;
    uint8* flag2 = VecParg2->m_flag;
    uint8* ResultFlag = VecResult->m_flag;
    ScalarValue* presult = VecResult->m_vals;
    Datum arg1;
    Datum arg2;
    int i;
    bool result = false;

    if (pselection != NULL) {
        for (i = 0; i < nvalues; i++) {
            if (pselection[i]) {
                if (IS_NULL(flag1[i]) || IS_NULL(flag2[i])) {
                    SET_NULL(ResultFlag[i]);
                } else {
                    arg1 = ScalarVector::Decode(parg1[i]);
                    arg2 = ScalarVector::Decode(parg2[i]);
                    result = !datumlike(arg1, arg2);
                    presult[i] = result;
                    SET_NOTNULL(ResultFlag[i]);
                }
            }
        }
    } else {
        for (i = 0; i < nvalues; i++) {
            if (IS_NULL(flag1[i]) || IS_NULL(flag2[i])) {
                SET_NULL(ResultFlag[i]);
            } else {
                arg1 = ScalarVector::Decode(parg1[i]);
                arg2 = ScalarVector::Decode(parg2[i]);
                result = !datumlike(arg1, arg2);
                presult[i] = result;
                SET_NOTNULL(ResultFlag[i]);
            }
        }
    }

    VecResult->m_rows = nvalues;
    return VecResult;
}

/********************************************************************
 *
 * vlower  vectorize function
 *
 * Purpose:
 *	 Returns string, with all letters forced to lowercase.
 *
 *********************************************************************
 */

ScalarVector* vlower(PG_FUNCTION_ARGS)
{
    ScalarVector* VecParg1 = PG_GETARG_VECTOR(0);
    int32 nvalues = PG_GETARG_INT32(1);
    ScalarVector* VecResult = PG_GETARG_VECTOR(2);
    bool* pselection = PG_GETARG_SELECTION(3);
    ScalarValue* parg1 = VecParg1->m_vals;
    uint8* flag = VecParg1->m_flag;
    uint8* ResultFlag = VecResult->m_flag;
    Oid collation = PG_GET_COLLATION();
    Datum value;
    int i;

    if (pselection != NULL) {
        for (i = 0; i < nvalues; i++) {
            if (pselection[i]) {
                if (IS_NULL(flag[i])) {
                    SET_NULL(ResultFlag[i]);
                } else {
                    value = ScalarVector::Decode(parg1[i]);
                    VecResult->m_vals[i] = DirectFunctionCall1Coll(lower, collation, value);
                    SET_NOTNULL(ResultFlag[i]);
                }
            }
        }
    } else {
        for (i = 0; i < nvalues; i++) {
            if (IS_NULL(flag[i])) {
                SET_NULL(ResultFlag[i]);
            } else {
                value = ScalarVector::Decode(parg1[i]);
                VecResult->m_vals[i] = DirectFunctionCall1Coll(lower, collation, value);
                SET_NOTNULL(ResultFlag[i]);
            }
        }
    }

    VecResult->m_rows = nvalues;

    return VecResult;
}

/********************************************************************
 *
 * vupper
 *
 * Purpose:
 *
 *	 Returns string, with all letters forced to uppercase.
 *
 *********************************************************************
 */
ScalarVector* vupper(PG_FUNCTION_ARGS)
{
    ScalarVector* VecParg1 = PG_GETARG_VECTOR(0);
    int32 nvalues = PG_GETARG_INT32(1);
    ScalarVector* VecResult = PG_GETARG_VECTOR(2);
    bool* pselection = PG_GETARG_SELECTION(3);
    ScalarValue* parg1 = VecParg1->m_vals;
    uint8* flag = VecParg1->m_flag;
    uint8* ResultFlag = VecResult->m_flag;
    Oid collation = PG_GET_COLLATION();
    Datum value;
    int i;

    if (pselection != NULL) {
        for (i = 0; i < nvalues; i++) {
            if (pselection[i]) {
                if (IS_NULL(flag[i])) {
                    SET_NULL(ResultFlag[i]);
                } else {
                    value = ScalarVector::Decode(parg1[i]);
                    VecResult->m_vals[i] = DirectFunctionCall1Coll(upper, collation, value);
                    SET_NOTNULL(ResultFlag[i]);
                }
            }
        }
    } else {
        for (i = 0; i < nvalues; i++) {
            if (IS_NULL(flag[i])) {
                SET_NULL(ResultFlag[i]);
            } else {
                value = ScalarVector::Decode(parg1[i]);
                VecResult->m_vals[i] = DirectFunctionCall1Coll(upper, collation, value);
                SET_NOTNULL(ResultFlag[i]);
            }
        }
    }

    VecResult->m_rows = nvalues;

    return VecResult;
}

/*
 * The internal realization of function vlpad.
 */
static void vlpad_internal(ScalarVector* parg1, ScalarVector* parg2, ScalarVector* VecRet, uint8* pflagsRes, int len,
    int eml, int idx, mblen_converter fun_mblen)
{
    int s1len;
    int s2len;
    int bytelen;
    int m;
    char* ptr1 = NULL;
    char* ptr2 = NULL;
    char* ptr2start = NULL;
    char* ptr2end = NULL;
    char* ptr_ret = NULL;
    text* ret = NULL;
    errno_t rc = EOK;

    s1len = (VARSIZE_ANY_EXHDR(parg1->m_vals[idx]) > 0) ? VARSIZE_ANY_EXHDR(parg1->m_vals[idx]) : 0;
    s2len = (VARSIZE_ANY_EXHDR(parg2->m_vals[idx]) > 0) ? VARSIZE_ANY_EXHDR(parg2->m_vals[idx]) : 0;
    s1len = pg_mbstrlen_with_len_eml(VARDATA_ANY(parg1->m_vals[idx]), s1len, eml);
    s1len = (s1len > len) ? len : s1len;
    len = (s2len <= 0) ? s1len : len;
    bytelen = eml * len;

    if (len != 0 && bytelen / eml != len)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("requested length too large")));

    ret = (text*)palloc((VARHDRSZ + bytelen));
    m = len - s1len;

    ptr2 = ptr2start = VARDATA_ANY(parg2->m_vals[idx]);
    ptr2end = ptr2 + s2len;
    ptr_ret = VARDATA(ret);

    while (m--) {
        int mlen = fun_mblen((const unsigned char*)ptr2);

        rc = memcpy_s(ptr_ret, mlen, ptr2, mlen);
        securec_check(rc, "\0", "\0");
        ptr_ret += mlen;
        ptr2 += mlen;
        if (ptr2 == ptr2end)
            ptr2 = ptr2start;
    }

    ptr1 = VARDATA_ANY(parg1->m_vals[idx]);
    while (s1len--) {
        int mlen = fun_mblen((const unsigned char*)ptr1);

        rc = memcpy_s(ptr_ret, mlen, ptr1, mlen);
        securec_check(rc, "\0", "\0");
        ptr_ret += mlen;
        ptr1 += mlen;
    }

    SET_VARSIZE(ret, ptr_ret - (char*)ret);

    if (0 == VARSIZE_ANY_EXHDR(ret) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && !RETURN_NS) {
        SET_NULL(pflagsRes[idx]);
    } else {
        VecRet->m_vals[idx] = PointerGetDatum(ret);
        SET_NOTNULL(pflagsRes[idx]);
    }
}

/********************************************************************
 *
 * vlpad
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
ScalarVector* vlpad(PG_FUNCTION_ARGS)
{
    ScalarVector* parg1 = PG_GETARG_VECTOR(0);
    uint8* flag1 = parg1->m_flag;
    ScalarVector* parg2 = PG_GETARG_VECTOR(1);
    uint8* flag2 = parg2->m_flag;
    ScalarVector* parg3 = PG_GETARG_VECTOR(2);
    uint8* flag3 = parg3->m_flag;
    int32 nvalues = PG_GETARG_INT32(3);
    ScalarVector* VecRet = PG_GETARG_VECTOR(4);
    uint8* pflagsRes = (uint8*)(VecRet->m_flag);
    bool* pselection = PG_GETARG_SELECTION(5);
    int eml;
    int len;
    int i;

    /*
     * The encoding method is defined after database is setted.
     */
    eml = pg_database_encoding_max_length();
    mblen_converter fun_mblen;
    fun_mblen = *pg_wchar_table[GetDatabaseEncoding()].mblen;

    if (pselection != NULL) {
        for (i = 0; i < nvalues; i++) {
            // if this line not selected, skip it
            if (!pselection[i])
                continue;
            /*
             * if one of the string,length,fill value in i'th row
             * is null, set pflagsRes[i] to NULL, continue
             */
            if (IS_NULL(flag1[i]) || IS_NULL(flag2[i]) || IS_NULL(flag3[i]))
                SET_NULL(pflagsRes[i]);
            else {
                /*
                 * Get the length value of lpad function,
                 * negative len is silently taken as zero
                 */
                len = DatumGetInt32(parg2->m_vals[i]);
                len = (len < 0) ? 0 : len;
                vlpad_internal(parg1, parg3, VecRet, pflagsRes, len, eml, i, fun_mblen);
            }
        }
    } else {
        for (i = 0; i < nvalues; i++) {
            /*
             * if one of the string,length,fill value in i'th row
             * is null, set pflagsRes[i] to NULL, continue
             */
            if (IS_NULL(flag1[i]) || IS_NULL(flag2[i]) || IS_NULL(flag3[i]))
                SET_NULL(pflagsRes[i]);
            else {
                /*
                 * Get the length value of lpad function,
                 * negative len is silently taken as zero
                 */
                len = DatumGetInt32(parg2->m_vals[i]);
                len = (len < 0) ? 0 : len;
                vlpad_internal(parg1, parg3, VecRet, pflagsRes, len, eml, i, fun_mblen);
            }
        }
    }

    PG_GETARG_VECTOR(4)->m_rows = nvalues;
    return PG_GETARG_VECTOR(4);
}

Datum varchar_int4(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(varcharout, txt));

    result = DirectFunctionCall1(int4in, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum bpchar_int4(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(bpcharout, txt));

    result = DirectFunctionCall1(int4in, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum timestamp_varchar(PG_FUNCTION_ARGS)
{
    Timestamp timestamp = PG_GETARG_TIMESTAMP(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(timestamp_out, timestamp));

    result = DirectFunctionCall3(varcharin, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

/*
 * @Description: int1 convert to varchar
 * @in arg1 - tinyint type numeric.
 * @return bpchar type string.
 */
Datum int1_varchar(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(int1out, arg1));

    result = DirectFunctionCall3(varcharin, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum int2_varchar(PG_FUNCTION_ARGS)
{
    int16 arg1 = PG_GETARG_INT16(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(int2out, arg1));

    result = DirectFunctionCall3(varcharin, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum int4_varchar(PG_FUNCTION_ARGS)
{
    int32 arg1 = PG_GETARG_INT32(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(int4out, arg1));

    result = DirectFunctionCall3(varcharin, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum float4_varchar(PG_FUNCTION_ARGS)
{
    float4 num = PG_GETARG_FLOAT4(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(float4out, Float4GetDatum(num)));

    result = DirectFunctionCall3(varcharin, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum float8_varchar(PG_FUNCTION_ARGS)
{
    float8 num = PG_GETARG_FLOAT8(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(float8out, Float8GetDatum(num)));

    result = DirectFunctionCall3(varcharin, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum varchar_timestamp(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(varcharout, txt));

    result = DirectFunctionCall3(timestamp_in, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

/*
 * @Description: int1 convert to nvarchar2
 * @in arg1 -  tinyint type numeric.
 * @return nvarchar2 type string.
 */
Datum int1_nvarchar2(PG_FUNCTION_ARGS)
{
    uint8 arg1 = PG_GETARG_UINT8(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(int1out, arg1));

    result = DirectFunctionCall3(nvarchar2in, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum bpchar_timestamp(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    char* tmp = NULL;
    Datum result;
    tmp = DatumGetCString(DirectFunctionCall1(bpcharout, txt));

    result = DirectFunctionCall3(timestamp_in, CStringGetDatum(tmp), ObjectIdGetDatum(0), Int32GetDatum(-1));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

/*
 * vector_bpcharlen
 *
 * Has the same function as bpcharlen
 */
ScalarVector* vbpcharlen(PG_FUNCTION_ARGS)
{
    ScalarVector* varg = PG_GETARG_VECTOR(0);
    uint8* vflag = varg->m_flag;
    int32 nvalues = PG_GETARG_INT32(1);
    ScalarVector* vresult = PG_GETARG_VECTOR(2);
    uint8* pflagRes = vresult->m_flag;
    bool* pselection = PG_GETARG_SELECTION(3);
    int k;
    int len;
    int eml;
    eml = pg_database_encoding_max_length();

    if (pselection != NULL) {
        for (k = 0; k < nvalues; k++) {
            if (pselection[k]) {
                if (NOT_NULL(vflag[k])) {
                    len = VARSIZE_ANY_EXHDR(varg->m_vals[k]);
                    if (eml != 1)
                        len = pg_mbstrlen_with_len_eml(VARDATA_ANY(varg->m_vals[k]), len, eml);
                    vresult->m_vals[k] = Int32GetDatum(len);
                    SET_NOTNULL(pflagRes[k]);
                } else {
                    SET_NULL(pflagRes[k]);
                }
            }
        }
    } else {
        for (k = 0; k < nvalues; k++) {
            if (NOT_NULL(vflag[k])) {
                len = VARSIZE_ANY_EXHDR(varg->m_vals[k]);
                if (eml != 1)
                    len = pg_mbstrlen_with_len_eml(VARDATA_ANY(varg->m_vals[k]), len, eml);
                vresult->m_vals[k] = Int32GetDatum(len);
                SET_NOTNULL(pflagRes[k]);
            } else {
                SET_NULL(pflagRes[k]);
            }
        }
    }

    vresult->m_rows = nvalues;
    return vresult;
}
