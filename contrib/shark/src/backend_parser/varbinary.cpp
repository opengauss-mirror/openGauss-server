/*-------------------------------------------------------------------------
 *
 * varbinary.c
 *      Functions for the variable-length binary type.
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <cstring>

#include "access/hash.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "common/int.h"
#include "lib/hyperloglog.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "parser/parser.h"
#include "parser/scansup.h"
#include "port/pg_bswap.h"
#include "regex/regex.h"
#include "replication/logicalworker.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/sortsupport.h"
#include "lib/stringinfo.h"

PG_FUNCTION_INFO_V1(varbinaryin);
PG_FUNCTION_INFO_V1(varbinaryout);
PG_FUNCTION_INFO_V1(varbinaryrecv);
PG_FUNCTION_INFO_V1(varbinarysend);
PG_FUNCTION_INFO_V1(varbinary);
PG_FUNCTION_INFO_V1(varbinarytypmodin);
PG_FUNCTION_INFO_V1(varbinarytypmodout);
PG_FUNCTION_INFO_V1(byteavarbinary);
PG_FUNCTION_INFO_V1(varbinarybytea);
PG_FUNCTION_INFO_V1(varcharvarbinary);
PG_FUNCTION_INFO_V1(bpcharvarbinary);
PG_FUNCTION_INFO_V1(varbinarybpchar);
PG_FUNCTION_INFO_V1(varbinaryvarchar);
PG_FUNCTION_INFO_V1(int2varbinary);
PG_FUNCTION_INFO_V1(int4varbinary);
PG_FUNCTION_INFO_V1(int8varbinary);
PG_FUNCTION_INFO_V1(varbinaryint2);
PG_FUNCTION_INFO_V1(varbinaryint4);
PG_FUNCTION_INFO_V1(varbinaryint8);
PG_FUNCTION_INFO_V1(float4varbinary);
PG_FUNCTION_INFO_V1(float8varbinary);

extern "C" Datum varbinaryin(PG_FUNCTION_ARGS);
extern "C" Datum varbinaryout(PG_FUNCTION_ARGS);
extern "C" Datum varbinaryrecv(PG_FUNCTION_ARGS);
extern "C" Datum varbinarysend(PG_FUNCTION_ARGS);
extern "C" Datum varbinary(PG_FUNCTION_ARGS);
extern "C" Datum varbinarytypmodin(PG_FUNCTION_ARGS);
extern "C" Datum varbinarytypmodout(PG_FUNCTION_ARGS);
extern "C" Datum byteavarbinary(PG_FUNCTION_ARGS);
extern "C" Datum varbinarybytea(PG_FUNCTION_ARGS);
extern "C" Datum varcharvarbinary(PG_FUNCTION_ARGS);
extern "C" Datum bpcharvarbinary(PG_FUNCTION_ARGS);
extern "C" Datum varbinarybpchar(PG_FUNCTION_ARGS);
extern "C" Datum varbinaryvarchar(PG_FUNCTION_ARGS);
extern "C" Datum int2varbinary(PG_FUNCTION_ARGS);
extern "C" Datum int4varbinary(PG_FUNCTION_ARGS);
extern "C" Datum int8varbinary(PG_FUNCTION_ARGS);
extern "C" Datum varbinaryint2(PG_FUNCTION_ARGS);
extern "C" Datum varbinaryint4(PG_FUNCTION_ARGS);
extern "C" Datum varbinaryint8(PG_FUNCTION_ARGS);
extern "C" Datum float4varbinary(PG_FUNCTION_ARGS);
extern "C" Datum float8varbinary(PG_FUNCTION_ARGS);

/*****************************************************************************
 *     USER I/O ROUTINES                                                         *
 *****************************************************************************/

#define VAL(CH)            ((CH) - '0')
#define DIG(VAL)        ((VAL) + '0')

#define MAX_BINARY_SIZE 8000
#define ROWVERSION_SIZE 8

static const int8 hexlookup[128] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

static inline char get_hex(char c)
{
    int res = -1;

    if (c > 0 && c < 127) {
        res = hexlookup[(unsigned char) c];
    }

    if (res < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("invalid hexadecimal digit: \"%c\"", c)));
    }

    return static_cast<char>(res);
}

/* A variant of PG's hex_decode function, but allows odd number of hex digits */
static uint64 hex_decode_allow_odd_digits(const char *src, unsigned len, char *dst)
{
    const char *s;
    const char *srcend;
    char        v1;
    char        v2;
    char        *p;

    srcend = src + len;
    s = src;
    p = dst;

    if (len % 2 == 1) {
        /*
         * If input has odd number of hex digits, add a 0 to the front to make
         * it even
         */
        v1 = '\0';
        v2 = get_hex(*s++);
        *p++ = v1 | v2;
    }
    /* The rest of the input must have even number of digits */
    while (s < srcend) {
        if (*s == ' ' || *s == '\n' || *s == '\t' || *s == '\r') {
            s++;
            continue;
        }
        v1 = get_hex(*s++) << 4;
        v2 = get_hex(*s++);
        *p++ = v1 | v2;
    }

    return p - dst;
}

/*
 *        varbinaryin    - input function of varbinary
 */
Datum varbinaryin(PG_FUNCTION_ARGS)
{
    char       *inputText = PG_GETARG_CSTRING(0);
    char       *rp;
    char       *tp;
    int            len;
    bytea       *result;
    int32        typmod = PG_GETARG_INT32(2);

    len = strlen(inputText);

    /*
     * Assume that input string is already hex encoded for following cases:
     * 1. Typmode is TSQL_HEX_CONST_TYPMOD
     * 2. dump_restore GUC is set.
     * 3. This is logical replication applyworker.
     */
    if (typmod == TSQL_HEX_CONST_TYPMOD || IsLogicalWorker()) {
        /*
         * calculate length of the binary code e.g. 0xFF should be 1 byte
         * (plus VARHDRSZ) and 0xF should also be 1 byte (plus VARHDRSZ).
         */
        int            bc = (len - 1) / 2 + VARHDRSZ;    /* maximum possible length */

        if (typmod >= (int32) VARHDRSZ && bc > typmod) {
            /* Verify that extra bytes are zeros, and clip them off */
            char    *temp_result;
            size_t    i;

            temp_result = (char*)palloc0(bc);
            bc = hex_decode_allow_odd_digits(inputText + 2, len - 2, temp_result);

            for (i = (typmod - VARHDRSZ); i < bc; i++) {
                if (temp_result[i] != 0) {
                    ereport(ERROR,
                            (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                            errmsg("String or binary data would be truncated.\n"
                                    "The statement has been terminated.")));
                }
            }
            pfree(temp_result);
            bc = typmod;
            len = (typmod - VARHDRSZ) * 2 + 2;
        }

        result = (bytea*)palloc(bc);
        bc = hex_decode_allow_odd_digits(inputText + 2, len - 2, VARDATA(result));
        SET_VARSIZE(result, bc + VARHDRSZ); /* actual length */

        PG_RETURN_BYTEA_P(result);
    }

    tp = inputText;

    result = (bytea *) palloc(len + VARHDRSZ);
    SET_VARSIZE(result, len + VARHDRSZ);

    rp = VARDATA(result);
    errno_t rc = memcpy_s(rp, len, tp, len);
    securec_check(rc, "\0", "\0");

    PG_RETURN_BYTEA_P(result);
}

/*
 *        varbinaryout        - converts to printable representation of byte array
 *
 *        In the traditional escaped format, non-printable characters are
 *        printed as '\nnn' (octal) and '\' as '\\'.
 *      This routine is copied from byteaout
 */
Datum varbinaryout(PG_FUNCTION_ARGS)
{
    bytea       *vlena = PG_GETARG_BYTEA_PP(0);
    char       *result;
    char       *rp;

    if (u_sess->attr.attr_common.bytea_output == BYTEA_OUTPUT_HEX) {
        /* Print hex format */
        rp = result = (char*)palloc(VARSIZE_ANY_EXHDR(vlena) * 2 + 2 + 1);
        *rp++ = '0';
        *rp++ = 'x';
        rp += hex_encode(VARDATA_ANY(vlena), VARSIZE_ANY_EXHDR(vlena), rp);
    } else if (u_sess->attr.attr_common.bytea_output == BYTEA_OUTPUT_ESCAPE) {
        /* Print traditional escaped format */
        char       *vp;
        int            len;
        int            i;

        len = 1;                /* empty string has 1 char */
        vp = VARDATA_ANY(vlena);
        for (i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++) {
            if (*vp == '\\') {
                len += 2;
            } else if ((unsigned char) *vp < 0x20 || (unsigned char) *vp > 0x7e) {
                len += 4;
            } else {
                len++;
            }
        }
        rp = result = (char *) palloc(len);
        vp = VARDATA_ANY(vlena);
        for (i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++) {
            if (*vp == '\\') {
                *rp++ = '\\';
                *rp++ = '\\';
            } else if ((unsigned char) *vp < 0x20 || (unsigned char) *vp > 0x7e) {
                int            val;    /* holds unprintable chars */

                val = *vp;
                rp[0] = '\\';
                rp[3] = DIG(val & 07);
                val >>= 3;
                rp[2] = DIG(val & 07);
                val >>= 3;
                rp[1] = DIG(val & 03);
                rp += 4;
            } else {
                *rp++ = *vp;
            }
        }
    } else {
        elog(ERROR, "unrecognized bytea_output setting: %d",
             u_sess->attr.attr_common.bytea_output);
        rp = result = nullptr;        /* keep compiler quiet */
    }
    
    if (rp) {
        *rp = '\0';
    }

    PG_RETURN_CSTRING(result);
}

/*
 *        varbinaryrecv    - converts external binary format to bytea
 */
Datum varbinaryrecv(PG_FUNCTION_ARGS)
{
    StringInfo    buf = (StringInfo) PG_GETARG_POINTER(0);
    bytea       *result;
    int            nbytes;

    nbytes = buf->len - buf->cursor;
    result = (bytea *) palloc(nbytes + VARHDRSZ);
    SET_VARSIZE(result, nbytes + VARHDRSZ);
    pq_copymsgbytes(buf, VARDATA(result), nbytes);
    PG_RETURN_BYTEA_P(result);
}

/*
 *        varbinarysend    - converts bytea to binary format
 *
 * This is a special case: just copy the input...
 */
Datum varbinarysend(PG_FUNCTION_ARGS)
{
    bytea       *vlena = PG_GETARG_BYTEA_P_COPY(0);

    PG_RETURN_BYTEA_P(vlena);
}

/*
 * Converts a VARBINARY type to the specified size.
 *
 * maxlen is the typmod, ie, declared length plus VARHDRSZ bytes.
 *
 * Truncation rules: for an explicit cast, silently truncate to the given
 * length; for an implicit cast, raise error.
 * (This is sort-of per SQL: the spec would actually have us
 * raise a "completion condition" for the explicit cast case, but Postgres
 * hasn't got such a concept.)
 */
Datum varbinary(PG_FUNCTION_ARGS)
{
    bytea       *source = PG_GETARG_BYTEA_PP(0);
    char        *data = VARDATA_ANY(source);
    int32       typmod = PG_GETARG_INT32(1);
    bool        isExplicit = PG_GETARG_BOOL(2);
    int32       len;
    int32       maxlen;

    len = VARSIZE_ANY_EXHDR(source);

    /* If typmod is -1 (or invalid), use the actual length */
    if (typmod < (int32) VARHDRSZ)
        maxlen = len;
    else
        maxlen = typmod - VARHDRSZ;

    if (!isExplicit) {
        if (len > maxlen) {
            ereport(ERROR,
                (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                 errmsg("String or binary data would be truncated.\n"
                        "The statement has been terminated.")));
        }
    }

    /* No work if typmod is invalid or supplied data fits it already */
    if (maxlen < 0 || len <= maxlen) {
        PG_RETURN_BYTEA_P(source);
    }

    /*
     * Truncate the input data using cstring_to_text_with_len, notice text and
     * bytea actually have the same struct.
     */
    PG_RETURN_BYTEA_P((bytea *) cstring_to_text_with_len(data, maxlen));
}

/* common code for varbinarytypmodin, bpchartypmodin and varchartypmodin */
static int32 anychar_typmodin(ArrayType *ta, const char *typname)
{
    int32        typmod;
    int32       *tl;
    int            n;

    tl = ArrayGetIntegerTypmods(ta, &n);
    /* Allow typmod of VARBINARY(MAX) to go through as is */
    if (*tl == TSQL_MAX_TYPMOD) {
        return *tl;
    }

    /*
     * we're not too tense about good error message here because grammar
     * shouldn't allow wrong number of modifiers for CHAR
     */
    if (n != 1) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid type modifier")));
    }
    if (*tl < 1) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("length for type %s must be at least 1", typname)));
    }
    if (*tl > MaxAttrSize) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("length for type %s cannot exceed %d",
                        typname, MaxAttrSize)));
    }

    /*
     * For largely historical reasons, the typmod is VARHDRSZ plus the number
     * of characters; there is enough client-side code that knows about that
     * that we'd better not change it.
     */
    typmod = VARHDRSZ + *tl;

    return typmod;
}

/*
 * code for varbinarytypmodout
 * copied from bpchartypmodout and varchartypmodout
 */
static char* anychar_typmodout(int32 typmod)
{
    char       *res = (char *) palloc(64);

    if (typmod > VARHDRSZ) {
        errno_t rc = snprintf_s(res, sizeof(res), sizeof(res) - 1, "(%d)", (int) (typmod - VARHDRSZ));
        securec_check_ss(rc, "", "");
    } else {
        *res = '\0';
    }

    return res;
}

Datum varbinarytypmodin(PG_FUNCTION_ARGS)
{
    ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);

    PG_RETURN_INT32(anychar_typmodin(ta, "varbinary"));
}

Datum varbinarytypmodout(PG_FUNCTION_ARGS)
{
    int32        typmod = PG_GETARG_INT32(0);

    PG_RETURN_CSTRING(anychar_typmodout(typmod));
}


static void reverse_memcpy(char *dst, char *src, size_t n)
{
    size_t        i;

    for (i = 0; i < n; i++) {
        dst[n - 1 - i] = src[i];
    }
}

/*
 *   Cast functions
 */
Datum byteavarbinary(PG_FUNCTION_ARGS)
{
    bytea       *source = PG_GETARG_BYTEA_PP(0);

    PG_RETURN_BYTEA_P(source);
}

Datum varbinarybytea(PG_FUNCTION_ARGS)
{
    bytea       *source = PG_GETARG_BYTEA_PP(0);

    PG_RETURN_BYTEA_P(source);
}

Datum varcharvarbinary(PG_FUNCTION_ARGS)
{
    VarChar    *source = PG_GETARG_VARCHAR_PP(0);
    char       *data = VARDATA_ANY(source);
    char       *rp;
    size_t        len = VARSIZE_ANY_EXHDR(source);
    int32        typmod = PG_GETARG_INT32(1);
    int32        maxlen;
    bytea       *result;

    /* If typmod is -1 (or invalid), use the actual length */
    if (typmod < (int32) VARHDRSZ) {
        maxlen = len;
    } else {
        maxlen = typmod - VARHDRSZ;
    }

    if (len > maxlen) {
        len = maxlen;
    }

    result = (bytea *) palloc(maxlen + VARHDRSZ);
    SET_VARSIZE(result, maxlen + VARHDRSZ);

    rp = VARDATA(result);
    errno_t rc = memcpy_s(rp, maxlen, data, len);
    securec_check(rc, "\0", "\0");

    /* NULL pad the rest of the space */
    rc = memset_s(rp + len, maxlen, '\0', maxlen - len);
    securec_check(rc, "", "");
    PG_RETURN_BYTEA_P(result);
}

Datum bpcharvarbinary(PG_FUNCTION_ARGS)
{
    BpChar           *source = PG_GETARG_BPCHAR_PP(0);
    char           *data = VARDATA_ANY(source);
    char           *rp;
    size_t        len = VARSIZE_ANY_EXHDR(source);
    int32        typmod = PG_GETARG_INT32(1);
    int32        maxlen;
    bytea           *result;

    /* If typmod is -1 (or invalid), use the actual length */
    if (typmod < (int32) VARHDRSZ) {
        maxlen = len;
    } else {
        maxlen = typmod - VARHDRSZ;
    }

    if (len > maxlen) {
        len = maxlen;
    }

    result = (bytea *) palloc(len + VARHDRSZ);
    SET_VARSIZE(result, len + VARHDRSZ);

    rp = VARDATA(result);
    errno_t rc = memcpy_s(rp, len, data, len);
    securec_check(rc, "\0", "\0");

    PG_RETURN_BYTEA_P(result);
}

Datum varbinarybpchar(PG_FUNCTION_ARGS)
{
    bytea       *source = PG_GETARG_BYTEA_PP(0);
    char       *data = VARDATA_ANY(source);        /* Source data is server encoded */
    BpChar    *result;
    size_t        len = VARSIZE_ANY_EXHDR(source);
    int32        typmod = -1;
    int32        maxlen = -1;

    /*
     * Check whether the typmod argument exists, so that we
     * will not be reading any garbage values for typmod
     * which might cause Invalid read such as BABEL-4475
     */
    if (PG_NARGS() > 1) {
        typmod = PG_GETARG_INT32(1);
        maxlen = typmod - VARHDRSZ;
    }

    if (maxlen < 0 || len <= maxlen) {
        maxlen = len;
    }

    result = (BpChar *) cstring_to_text_with_len(data, maxlen);

    PG_RETURN_BPCHAR_P(result);
}


/*
 * This function is currently being called with 1 and 3 arguments,
 * Currently, the third argument is not being parsed in this function,
 * Check for the number of args needs to be added if the third arg is
 * being parsed in future
 */
Datum varbinaryvarchar(PG_FUNCTION_ARGS)
{
    bytea       *source = PG_GETARG_BYTEA_PP(0);
    char       *data = VARDATA_ANY(source);        /* Source data is server encoded */
    VarChar    *result;
    size_t        len = VARSIZE_ANY_EXHDR(source);
    int32        typmod = -1;
    int32        maxlen = -1;

    /*
     * Check whether the typmod argument exists, so that we
     * will not be reading any garbage values for typmod
     * which might cause Invalid read such as BABEL-4475
     */
    if (PG_NARGS() > 1) {
        typmod = PG_GETARG_INT32(1);
        maxlen = typmod - VARHDRSZ;
    }

    if (maxlen < 0 || len <= maxlen) {
        maxlen = len;
    }

    result = (VarChar *) cstring_to_text_with_len(data, maxlen);

    PG_RETURN_VARCHAR_P(result);
}

Datum int2varbinary(PG_FUNCTION_ARGS)
{
    int16        input = PG_GETARG_INT16(0);
    int32        typmod = PG_GETARG_INT32(1);
    int32        maxlen;
    int            len = sizeof(int16);
    int            actual_len;
    bytea       *result;
    char       *rp;

    /* If typmod is -1 (or invalid), use the actual length */
    if (typmod < (int32) VARHDRSZ) {
        maxlen = len;
    } else {
        maxlen = typmod - VARHDRSZ;
    }

    actual_len = maxlen < len ? maxlen : len;

    result = (bytea *) palloc(actual_len + VARHDRSZ);
    SET_VARSIZE(result, actual_len + VARHDRSZ);

    rp = VARDATA(result);
    /* Need reverse copy because endianness is different in MSSQL */
    reverse_memcpy(rp, (char *) &input, actual_len);

    PG_RETURN_BYTEA_P(result);
}

Datum int4varbinary(PG_FUNCTION_ARGS)
{
    int32        input = PG_GETARG_INT32(0);
    int32        typmod = PG_GETARG_INT32(1);
    int32        maxlen;
    int            len = sizeof(int32);
    int            actual_len;
    bytea       *result;
    char       *rp;

    /* If typmod is -1 (or invalid), use the actual length */
    if (typmod < (int32) VARHDRSZ) {
        maxlen = len;
    } else {
        maxlen = typmod - VARHDRSZ;
    }

    actual_len = maxlen < len ? maxlen : len;

    result = (bytea *) palloc(actual_len + VARHDRSZ);
    SET_VARSIZE(result, actual_len + VARHDRSZ);

    rp = VARDATA(result);
    /* Need reverse copy because endianness is different in MSSQL */
    reverse_memcpy(rp, (char *) &input, actual_len);

    PG_RETURN_BYTEA_P(result);
}

Datum int8varbinary(PG_FUNCTION_ARGS)
{
    int64        input = PG_GETARG_INT64(0);
    int32        typmod = PG_GETARG_INT32(1);
    int32        maxlen;
    int            len = sizeof(int64);
    int            actual_len;
    bytea       *result;
    char       *rp;

    /* If typmod is -1 (or invalid), use the actual length */
    if (typmod < (int32) VARHDRSZ) {
        maxlen = len;
    } else {
        maxlen = typmod - VARHDRSZ;
    }

    actual_len = maxlen < len ? maxlen : len;

    result = (bytea *) palloc(actual_len + VARHDRSZ);
    SET_VARSIZE(result, actual_len + VARHDRSZ);

    rp = VARDATA(result);
    /* Need reverse copy because endianness is different in MSSQL */
    reverse_memcpy(rp, (char *) &input, actual_len);

    PG_RETURN_BYTEA_P(result);
}

Datum varbinaryint2(PG_FUNCTION_ARGS)
{
    bytea       *source = PG_GETARG_BYTEA_PP(0);
    char       *data = VARDATA_ANY(source);
    int32        len;
    int32        result_len;
    int16       *result = (int16*)palloc0(sizeof(int16));

    len = VARSIZE_ANY_EXHDR(source);
    result_len = len > sizeof(int16) ? sizeof(int16) : len;
    reverse_memcpy((char *) result, data, result_len);

    PG_RETURN_INT16(*result);
}

Datum varbinaryint4(PG_FUNCTION_ARGS)
{
    bytea       *source = PG_GETARG_BYTEA_PP(0);
    char       *data = VARDATA_ANY(source);
    int32        len;
    int32        result_len;
    int32       *result = (int32*)palloc0(sizeof(int32));

    len = VARSIZE_ANY_EXHDR(source);
    result_len = len > sizeof(int32) ? sizeof(int32) : len;
    reverse_memcpy((char *) result, data, result_len);

    PG_RETURN_INT32(*result);
}

Datum varbinaryint8(PG_FUNCTION_ARGS)
{
    bytea       *source = PG_GETARG_BYTEA_PP(0);
    char       *data = VARDATA_ANY(source);
    int32        len;
    int32        result_len;
    int64       *result = (int64*)palloc0(sizeof(int64));

    len = VARSIZE_ANY_EXHDR(source);
    result_len = len > sizeof(int64) ? sizeof(int64) : len;
    reverse_memcpy((char *) result, data, result_len);

    PG_RETURN_INT64(*result);
}

Datum float4varbinary(PG_FUNCTION_ARGS)
{
    float4        input = PG_GETARG_FLOAT4(0);
    int32        typmod = PG_GETARG_INT32(1);
    int32        maxlen;
    int            len = sizeof(float4);
    int            actual_len;
    bytea       *result;
    char       *rp;

    /* If typmod is -1 (or invalid), use the actual length */
    if (typmod < (int32) VARHDRSZ) {
        maxlen = len;
    } else {
        maxlen = typmod - VARHDRSZ;
    }

    actual_len = maxlen < len ? maxlen : len;

    result = (bytea *) palloc(actual_len + VARHDRSZ);
    SET_VARSIZE(result, actual_len + VARHDRSZ);

    rp = VARDATA(result);
    /* Need reverse copy because endianness is different in MSSQL */
    reverse_memcpy(rp, (char *) &input, actual_len);

    PG_RETURN_BYTEA_P(result);
}

Datum float8varbinary(PG_FUNCTION_ARGS)
{
    float8        input = PG_GETARG_FLOAT8(0);
    int32        typmod = PG_GETARG_INT32(1);
    int32        maxlen;
    int            len = sizeof(float8);
    int            actual_len;
    bytea       *result;
    char       *rp;

    /* If typmod is -1 (or invalid), use the actual length */
    if (typmod < (int32) VARHDRSZ) {
        maxlen = len;
    } else {
        maxlen = typmod - VARHDRSZ;
    }

    actual_len = maxlen < len ? maxlen : len;

    result = (bytea *) palloc(actual_len + VARHDRSZ);
    SET_VARSIZE(result, actual_len + VARHDRSZ);

    rp = VARDATA(result);
    /* Need reverse copy because endianness is different in MSSQL */
    reverse_memcpy(rp, (char *) &input, actual_len);

    PG_RETURN_BYTEA_P(result);
}


int8 varbinarycompare(bytea *source1, bytea *source2);

int8 inline varbinarycompare(bytea *source1, bytea *source2)
{
    char       *data1 = VARDATA_ANY(source1);
    int32        len1 = VARSIZE_ANY_EXHDR(source1);
    char       *data2 = VARDATA_ANY(source2);
    int32        len2 = VARSIZE_ANY_EXHDR(source2);

    unsigned char byte1;
    unsigned char byte2;
    int32        maxlen = len2 > len1 ? len2 : len1;

    /* loop all the bytes */
    for (int i = 0; i < maxlen; i++) {
        byte1 = i < len1 ? data1[i] : 0;
        byte2 = i < len2 ? data2[i] : 0;
        /* we've found a different byte */
        if (byte1 > byte2)
            return 1;
        else if (byte1 < byte2)
            return -1;
    }
    return 0;
}

PG_FUNCTION_INFO_V1(varbinary_eq);
PG_FUNCTION_INFO_V1(varbinary_neq);
PG_FUNCTION_INFO_V1(varbinary_gt);
PG_FUNCTION_INFO_V1(varbinary_geq);
PG_FUNCTION_INFO_V1(varbinary_lt);
PG_FUNCTION_INFO_V1(varbinary_leq);
PG_FUNCTION_INFO_V1(varbinary_cmp);

extern "C" Datum varbinary_eq(PG_FUNCTION_ARGS);
extern "C" Datum varbinary_neq(PG_FUNCTION_ARGS);
extern "C" Datum varbinary_gt(PG_FUNCTION_ARGS);
extern "C" Datum varbinary_geq(PG_FUNCTION_ARGS);
extern "C" Datum varbinary_lt(PG_FUNCTION_ARGS);
extern "C" Datum varbinary_leq(PG_FUNCTION_ARGS);
extern "C" Datum varbinary_cmp(PG_FUNCTION_ARGS);


Datum varbinary_eq(PG_FUNCTION_ARGS)
{
    bytea       *source1 = PG_GETARG_BYTEA_PP(0);
    bytea       *source2 = PG_GETARG_BYTEA_PP(1);
    bool        result = varbinarycompare(source1, source2) == 0;

    PG_RETURN_BOOL(result);
}

Datum varbinary_neq(PG_FUNCTION_ARGS)
{
    bytea       *source1 = PG_GETARG_BYTEA_PP(0);
    bytea       *source2 = PG_GETARG_BYTEA_PP(1);
    bool        result = varbinarycompare(source1, source2) != 0;

    PG_RETURN_BOOL(result);
}

Datum varbinary_gt(PG_FUNCTION_ARGS)
{
    bytea       *source1 = PG_GETARG_BYTEA_PP(0);
    bytea       *source2 = PG_GETARG_BYTEA_PP(1);
    bool        result = varbinarycompare(source1, source2) > 0;

    PG_RETURN_BOOL(result);
}

Datum varbinary_geq(PG_FUNCTION_ARGS)
{
    bytea       *source1 = PG_GETARG_BYTEA_PP(0);
    bytea       *source2 = PG_GETARG_BYTEA_PP(1);
    bool        result = varbinarycompare(source1, source2) >= 0;

    PG_RETURN_BOOL(result);
}

Datum varbinary_lt(PG_FUNCTION_ARGS)
{
    bytea       *source1 = PG_GETARG_BYTEA_PP(0);
    bytea       *source2 = PG_GETARG_BYTEA_PP(1);
    bool        result = varbinarycompare(source1, source2) < 0;

    PG_RETURN_BOOL(result);
}

Datum varbinary_leq(PG_FUNCTION_ARGS)
{
    bytea       *source1 = PG_GETARG_BYTEA_PP(0);
    bytea       *source2 = PG_GETARG_BYTEA_PP(1);
    bool        result = varbinarycompare(source1, source2) <= 0;

    PG_RETURN_BOOL(result);
}


Datum varbinary_cmp(PG_FUNCTION_ARGS)
{
    bytea       *source1 = PG_GETARG_BYTEA_PP(0);
    bytea       *source2 = PG_GETARG_BYTEA_PP(1);

    PG_RETURN_INT32(varbinarycompare(source1, source2));
}

PG_FUNCTION_INFO_V1(varbinary_length);
extern "C" Datum varbinary_length(PG_FUNCTION_ARGS);

Datum varbinary_length(PG_FUNCTION_ARGS)
{
    bytea       *source = PG_GETARG_BYTEA_PP(0);
    int32        limit = VARSIZE_ANY_EXHDR(source);

    PG_RETURN_INT32(limit);
}
