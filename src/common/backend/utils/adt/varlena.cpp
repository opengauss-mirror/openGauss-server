/* -------------------------------------------------------------------------
 *
 * varlena.c
 *	  Functions for the variable-length built-in types.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/varlena.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <limits.h>

#include "access/hash.h"
#include "access/tuptoaster.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "common/int.h"
#include "lib/hyperloglog.h"
#include "libpq/md5.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "port/pg_bswap.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/pg_locale.h"
#include "parser/parser.h"
#include "utils/int8.h"
#include "utils/sortsupport.h"
#include "executor/node/nodeSort.h"
#include "pgxc/groupmgr.h"

#define JUDGE_INPUT_VALID(X, Y) ((NULL == (X)) || (NULL == (Y)))
#define GET_POSITIVE(X) ((X) > 0 ? (X) : ((-1) * (X)))
static int getResultPostionReverse(text* textStr, text* textStrToSearch, int32 beginIndex, int occurTimes);
static int getResultPostion(text* textStr, text* textStrToSearch, int32 beginIndex, int occurTimes);

typedef struct varlena unknown;
typedef struct varlena VarString;

typedef struct {
    bool use_wchar;  /* T if multibyte encoding */
    char* str1;      /* use these if not use_wchar */
    char* str2;      /* note: these point to original texts */
    pg_wchar* wstr1; /* use these if use_wchar */
    pg_wchar* wstr2; /* note: these are palloc'd */
    int len1;        /* string lengths in logical characters */
    int len2;
    /* Skip table for Boyer-Moore-Horspool search algorithm: */
    int skiptablemask;  /* mask for ANDing with skiptable subscripts */
    int skiptable[256]; /* skip distance for given mismatched char */
} TextPositionState;

typedef struct {
    char* buf1; /* 1st string, or abbreviation original string
                 * buf */
    char* buf2; /* 2nd string, or abbreviation strxfrm() buf */
    int buflen1;
    int buflen2;
    int last_len1;     /* Length of last buf1 string/strxfrm() input */
    int last_len2;     /* Length of last buf2 string/strxfrm() blob */
    int last_returned; /* Last comparison result (cache) */
    bool cache_blob;   /* Does buf2 contain strxfrm() blob, etc? */
    bool collate_c;
    bool bpchar;                /* Sorting pbchar, not varchar/text/bytea? */
    bool estimating;            /* true if estimating cardinality
                                 * refer to NumericSortSupport */
    hyperLogLogState abbr_card; /* Abbreviated key cardinality state */
    /* hyperLogLogState full_card;  Full key cardinality state */
    /* Don't use abbr_card/full_card to evaluate weather abort
     * fast comparation or not, use abbr_card/input_count instead
     * like numeric_sortsupport does.
     */
    int64 input_count; /* number of non-null values seen */
    double prop_card;  /* Required cardinality proportion */
#ifdef HAVE_LOCALE_T
    pg_locale_t locale;
#endif
} VarStringSortSupport;

/*
 * This should be large enough that most strings will fit, but small enough
 * that we feel comfortable putting it on the stack
 */
#define TEXTBUFLEN 1024

#define DatumGetUnknownP(X) ((unknown*)PG_DETOAST_DATUM(X))
#define DatumGetUnknownPCopy(X) ((unknown*)PG_DETOAST_DATUM_COPY(X))
#define PG_GETARG_UNKNOWN_P(n) DatumGetUnknownP(PG_GETARG_DATUM(n))
#define PG_GETARG_UNKNOWN_P_COPY(n) DatumGetUnknownPCopy(PG_GETARG_DATUM(n))
#define PG_RETURN_UNKNOWN_P(x) PG_RETURN_POINTER(x)

static int varstrfastcmp_c(Datum x, Datum y, SortSupport ssup);
static int bpcharfastcmp_c(Datum x, Datum y, SortSupport ssup);
static int varstrfastcmp_locale(Datum x, Datum y, SortSupport ssup);
static int varstrcmp_abbrev(Datum x, Datum y, SortSupport ssup);
static Datum varstr_abbrev_convert(Datum original, SortSupport ssup);
static bool varstr_abbrev_abort(int memtupcount, SortSupport ssup);
static int text_position(text* t1, text* t2);
static void text_position_setup(text* t1, text* t2, TextPositionState* state);
static int text_position_next(int start_pos, TextPositionState* state);
static void text_position_cleanup(TextPositionState* state);
static text* text_catenate(text* t1, text* t2);
static text* text_overlay(text* t1, text* t2, int sp, int sl);
static void appendStringInfoText(StringInfo str, const text* t);
static bytea* bytea_catenate(bytea* t1, bytea* t2);
static bytea* bytea_substring(Datum str, int S, int L, bool length_not_specified);
static bytea* bytea_substring_orclcompat(Datum str, int S, int L, bool length_not_specified);
static bytea* bytea_overlay(bytea* t1, bytea* t2, int sp, int sl);
static StringInfo makeStringAggState(FunctionCallInfo fcinfo);

static Datum text_to_array_internal(PG_FUNCTION_ARGS);
static text* array_to_text_internal(FunctionCallInfo fcinfo, ArrayType* v, char* fldsep, char* null_string);

static bool text_format_parse_digits(const char** ptr, const char* end_ptr, int* value);
static const char* text_format_parse_format(
    const char* start_ptr, const char* end_ptr, int* argpos, int* widthpos, int* flags, int* width);
static void text_format_string_conversion(
    StringInfo buf, char conversion, FmgrInfo* typOutputInfo, Datum value, bool isNull, int flags, int width);

static void text_format_append_string(StringInfo buf, const char* str, int flags, int width);

// adapt A db's substrb
static text* get_substring_really(Datum str, int32 start, int32 length, bool length_not_specified);

/*****************************************************************************
 *	 CONVERSION ROUTINES EXPORTED FOR USE BY C CODE							 *
 *****************************************************************************/

#define TEXTISORANULL(t) ((t) == NULL || VARSIZE_ANY_EXHDR(t) == 0)

/*
 * cstring_to_text
 *
 * Create a text value from a null-terminated C string.
 *
 * The new text value is freshly palloc'd with a full-size VARHDR.
 */
text* cstring_to_text(const char* s)
{
    return cstring_to_text_with_len(s, strlen(s));
}

/*
 * cstring_to_text_with_len
 *
 * Same as cstring_to_text except the caller specifies the string length;
 * the string need not be null_terminated.
 */
text* cstring_to_text_with_len(const char* s, size_t len)
{
    text* result = (text*)palloc0(len + VARHDRSZ);

    SET_VARSIZE(result, len + VARHDRSZ);
    if (len > 0) {
        int rc = memcpy_s(VARDATA(result), len, s, len);
        securec_check(rc, "\0", "\0");
    }
    return result;
}

bytea* cstring_to_bytea_with_len(const char* s, int len)
{
    bytea *result = (bytea *) palloc0(len + VARHDRSZ);

    SET_VARSIZE(result, len + VARHDRSZ);
    if (len > 0) {
        int rc = memcpy_s(VARDATA(result), len, s, len);
        securec_check(rc, "\0", "\0");
    }

    return result;
}

BpChar* cstring_to_bpchar_with_len(const char* s, int len)
{
    BpChar *result = (BpChar *) palloc0(len + VARHDRSZ);

    SET_VARSIZE(result, len + VARHDRSZ);
    if (len > 0) {
        int rc = memcpy_s(VARDATA(result), len, s, len);
        securec_check(rc, "\0", "\0");
    }

    return result;
}

/*
 * text_to_cstring
 *
 * Create a palloc'd, null-terminated C string from a text value.
 *
 * We support being passed a compressed or toasted text value.
 * This is a bit bogus since such values shouldn't really be referred to as
 * "text *", but it seems useful for robustness.  If we didn't handle that
 * case here, we'd need another routine that did, anyway.
 */
char* text_to_cstring(const text* t)
{
    if (unlikely(t == NULL)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("invalid null pointer input for text_to_cstring()")));
    }
    FUNC_CHECK_HUGE_POINTER(false, t, "text_to_cstring()");

    /* must cast away the const, unfortunately */
    text* tunpacked = pg_detoast_datum_packed((struct varlena*)t);
    int len = VARSIZE_ANY_EXHDR(tunpacked);
    char* result = NULL;

    result = (char*)palloc(len + 1);
    MemCpy(result, VARDATA_ANY(tunpacked), len);
    result[len] = '\0';

    if (tunpacked != t)
        pfree_ext(tunpacked);

    return result;
}

/*
 * text_to_cstring_buffer
 *
 * Copy a text value into a caller-supplied buffer of size dst_len.
 *
 * The text string is truncated if necessary to fit.  The result is
 * guaranteed null-terminated (unless dst_len == 0).
 *
 * We support being passed a compressed or toasted text value.
 * This is a bit bogus since such values shouldn't really be referred to as
 * "text *", but it seems useful for robustness.  If we didn't handle that
 * case here, we'd need another routine that did, anyway.
 */
void text_to_cstring_buffer(const text* src, char* dst, size_t dst_len)
{
    /* must cast away the const, unfortunately */

    FUNC_CHECK_HUGE_POINTER((src == NULL), src, "text_to_cstring_buffer()");

    text* srcunpacked = pg_detoast_datum_packed((struct varlena*)src);
    size_t src_len = VARSIZE_ANY_EXHDR(srcunpacked);

    if (dst_len > 0) {
        dst_len--;
        if (dst_len >= src_len)
            dst_len = src_len;
        else /* ensure truncation is encoding-safe */
            dst_len = pg_mbcliplen(VARDATA_ANY(srcunpacked), src_len, dst_len);
        if (dst_len > 0) {
            int rc = memcpy_s(dst, dst_len, VARDATA_ANY(srcunpacked), dst_len);
            securec_check(rc, "\0", "\0");
        }
        dst[dst_len] = '\0';
    }

    if (srcunpacked != src)
        pfree_ext(srcunpacked);
}

/*****************************************************************************
 *	 USER I/O ROUTINES														 *
 *****************************************************************************/
#define VAL(CH) ((CH) - '0')
#define DIG(VAL) ((VAL) + '0')

/*
 *		byteain			- converts from printable representation of byte array
 *
 *		Non-printable characters must be passed as '\nnn' (octal) and are
 *		converted to internal form.  '\' must be passed as '\\'.
 *		ereport(ERROR, ...) if bad form.
 *
 *		BUGS:
 *				The input is scanned twice.
 *				The error checking of input is minimal.
 */
Datum byteain(PG_FUNCTION_ARGS)
{
    char* inputText = PG_GETARG_CSTRING(0);
    char* tp = NULL;
    char* rp = NULL;
    int bc;
    int cl;
    bytea* result = NULL;

    /* Recognize hex input */
    if (inputText[0] == '\\' && inputText[1] == 'x') {
        size_t len = strlen(inputText);

        bc = (len - 2) / 2 + VARHDRSZ; /* maximum possible length */
        result = (bytea*)palloc(bc);
        bc = hex_decode(inputText + 2, len - 2, VARDATA(result));
        SET_VARSIZE(result, bc + VARHDRSZ); /* actual length */

        PG_RETURN_BYTEA_P(result);
    }

    /* Else, it's the traditional escaped style */
    bc = 0;
    tp = inputText;
    while (*tp != '\0') {
        if (tp[0] != '\\') {
            cl = pg_mblen(tp);
            tp += cl;
            bc += cl;
        } else if ((tp[0] == '\\') && (tp[1] >= '0' && tp[1] <= '3') && (tp[2] >= '0' && tp[2] <= '7') &&
                   (tp[3] >= '0' && tp[3] <= '7')) {
            tp += 4;
            bc++;
        } else if ((tp[0] == '\\') && (tp[1] == '\\')) {
            tp += 2;
            bc++;
        } else {
            /*
             * one backslash, not followed by another or ### valid octal
             */
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for type bytea")));
        }
    }

    bc += VARHDRSZ;

    result = (bytea*)palloc(bc);
    SET_VARSIZE(result, bc);

    tp = inputText;
    rp = VARDATA(result);
    while (*tp != '\0') {
        if (tp[0] != '\\') {
            cl = pg_mblen(tp);
            for (int i = 0; i < cl; i++)
                *rp++ = *tp++;
        } else if ((tp[0] == '\\') && (tp[1] >= '0' && tp[1] <= '3') && (tp[2] >= '0' && tp[2] <= '7') &&
                   (tp[3] >= '0' && tp[3] <= '7')) {
            bc = VAL(tp[1]);
            bc <<= 3;
            bc += VAL(tp[2]);
            bc <<= 3;
            *rp++ = bc + VAL(tp[3]);

            tp += 4;
        } else if ((tp[0] == '\\') && (tp[1] == '\\')) {
            *rp++ = '\\';
            tp += 2;
        } else {
            /*
             * We should never get here. The first pass should not allow it.
             */
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input syntax for type bytea")));
        }
    }

    PG_RETURN_BYTEA_P(result);
}

/*
 *		byteaout		- converts to printable representation of byte array
 *
 *		In the traditional escaped format, non-printable characters are
 *		printed as '\nnn' (octal) and '\' as '\\'.
 */
Datum byteaout(PG_FUNCTION_ARGS)
{
    bytea* vlena = PG_GETARG_BYTEA_PP(0);
    char* result = NULL;
    char* rp = NULL;

    if (u_sess->attr.attr_common.bytea_output == BYTEA_OUTPUT_HEX) {
        /* Print hex format */
        rp = result = (char*)palloc(VARSIZE_ANY_EXHDR(vlena) * 2 + 2 + 1);
        *rp++ = '\\';
        *rp++ = 'x';
        rp += hex_encode(VARDATA_ANY(vlena), VARSIZE_ANY_EXHDR(vlena), rp);
    } else if (u_sess->attr.attr_common.bytea_output == BYTEA_OUTPUT_ESCAPE) {
        /* Print traditional escaped format */
        char* vp = NULL;
        int len;
        int i;

        len = 1; /* empty string has 1 char */
        vp = VARDATA_ANY(vlena);
        for (i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++) {
            if (*vp == '\\')
                len += 2;
            else if ((unsigned char)*vp < 0x20 || (unsigned char)*vp > 0x7e)
                len += 4;
            else
                len++;
        }
        rp = result = (char*)palloc(len);
        vp = VARDATA_ANY(vlena);
        for (i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++) {
            if (*vp == '\\') {
                *rp++ = '\\';
                *rp++ = '\\';
            } else if ((unsigned char)*vp < 0x20 || (unsigned char)*vp > 0x7e) {
                int val; /* holds unprintable chars */

                val = *vp;
                rp[0] = '\\';
                rp[3] = DIG(val & 07);
                val >>= 3;
                rp[2] = DIG(val & 07);
                val >>= 3;
                rp[1] = DIG(val & 03);
                rp += 4;
            } else
                *rp++ = *vp;
        }
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized bytea_output setting: %d", u_sess->attr.attr_common.bytea_output)));
        rp = result = NULL; /* keep compiler quiet */
    }
    *rp = '\0';

    /* free memory if allocated by the toaster */
    PG_FREE_IF_COPY(vlena, 0);

    PG_RETURN_CSTRING(result);
}

// input interface of RAW type
Datum rawin(PG_FUNCTION_ARGS)
{
    Datum fmt = DirectFunctionCall1(textin, CStringGetDatum(pstrdup("HEX")));
    Datum result;
    char* cstring_arg1 = PG_GETARG_CSTRING(0);
    char* tmp = NULL;
    int len = 0;
    Datum arg1;
    errno_t rc = EOK;

    len = strlen(cstring_arg1);

    if (0 != (len % 2)) {
        tmp = (char*)palloc0(len + 2);
        tmp[0] = '0';
        rc = strncat_s(tmp, len + 2, cstring_arg1, len + 1);
        securec_check(rc, tmp, "\0");
        arg1 = DirectFunctionCall1(textin, CStringGetDatum(tmp));
    } else
        arg1 = DirectFunctionCall1(textin, PG_GETARG_DATUM(0));

    result = DirectFunctionCall2(binary_decode, arg1, fmt);

    return result;
}

// output interface of RAW type
Datum rawout(PG_FUNCTION_ARGS)
{
    /*fcinfo->fncollation is set to 0 when calling Macro FuncCall1,
     *so the collation value needs to be reset.
     */
    if (!OidIsValid(fcinfo->fncollation))
        fcinfo->fncollation = DEFAULT_COLLATION_OID;

    bytea* data = PG_GETARG_BYTEA_P(0);
    text* ans = NULL;
    int datalen = 0;
    int resultlen = 0;
    int ans_len = 0;
    char* out_string = NULL;

    datalen = VARSIZE(data) - VARHDRSZ;

    resultlen = datalen << 1;

    if (resultlen < (int)(MaxAllocSize - VARHDRSZ) * 2) {
        ans = (text*)palloc_huge(CurrentMemoryContext, VARHDRSZ + resultlen);
        ans_len = hex_encode(VARDATA(data), datalen, VARDATA(ans));
        /* Make this FATAL 'cause we've trodden on memory ... */
        if (ans_len > resultlen)
            ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("overflow - encode estimate too small")));

        SET_VARSIZE(ans, VARHDRSZ + ans_len);
        
        out_string = str_toupper_for_raw(VARDATA_ANY(ans), VARSIZE_ANY_EXHDR(ans), PG_GET_COLLATION());
    } else {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("blob length: %d ,out of memory", resultlen)));
    }
    pfree_ext(ans);
    PG_RETURN_CSTRING(out_string);
}

// Implements interface of rawtohex(text)
Datum rawtotext(PG_FUNCTION_ARGS)
{
    Datum arg1 = PG_GETARG_DATUM(0);

    FUNC_CHECK_HUGE_POINTER(PG_ARGISNULL(0), DatumGetPointer(arg1), "rawtotext()");
    Datum cstring_result;
    Datum result;

    cstring_result = DirectFunctionCall1Coll(rawout, PG_GET_COLLATION(), arg1);

    result = DirectFunctionCall1(textin, cstring_result);

    PG_RETURN_TEXT_P(result);
}

// Implements interface of hextoraw(raw)
Datum texttoraw(PG_FUNCTION_ARGS)
{
    Datum arg1 = PG_GETARG_DATUM(0);

    FUNC_CHECK_HUGE_POINTER(PG_ARGISNULL(0), DatumGetPointer(arg1), "rawtotext()");
    Datum result;
    Datum cstring_arg1;

    cstring_arg1 = DirectFunctionCall1(textout, arg1);
    result = DirectFunctionCall1(rawin, cstring_arg1);

    PG_RETURN_BYTEA_P(result);
}

/*
 *		bytearecv			- converts external binary format to bytea
 */
Datum bytearecv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    bytea* result = NULL;
    int nbytes;

    nbytes = buf->len - buf->cursor;

    /* Because of the hex encoding when output a blob, the output size of blob
       will be twice(as func hex_enc_len). And the max palloc size is 1GB, so
       we only input blobs less than 500M */
    if ((long)nbytes* 2 + VARHDRSZ > (long)(MaxAllocSize) * 2) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                        errmsg("blob/bytea size:%d, only can recevie blob/bytea less than 1000M ", nbytes)));
    }
    result = (bytea*)palloc_huge(CurrentMemoryContext, nbytes + VARHDRSZ);
    SET_VARSIZE(result, nbytes + VARHDRSZ);
    pq_copymsgbytes(buf, VARDATA(result), nbytes);
    PG_RETURN_BYTEA_P(result);
}

/*
 *		byteasend			- converts bytea to binary format
 *
 * This is a special case: just copy the input...
 */
Datum byteasend(PG_FUNCTION_ARGS)
{
    bytea* vlena = PG_GETARG_BYTEA_P_COPY(0);

    PG_RETURN_BYTEA_P(vlena);
}

Datum bytea_string_agg_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        bytea* value = PG_GETARG_BYTEA_PP(1);

        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2)) {
            bytea* delim = PG_GETARG_BYTEA_PP(2);

            appendBinaryStringInfo(state, VARDATA_ANY(delim), VARSIZE_ANY_EXHDR(delim));
        }

        appendBinaryStringInfo(state, VARDATA_ANY(value), VARSIZE_ANY_EXHDR(value));
    }

    /*
     * The transition type for string_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum bytea_string_agg_finalfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    /* cannot be called directly because of internal-type argument */
    Assert(AggCheckCallContext(fcinfo, NULL));

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    if (state != NULL) {
        bytea* result = NULL;
        errno_t rc = 0;

        result = (bytea*)palloc(state->len + VARHDRSZ);
        SET_VARSIZE(result, state->len + VARHDRSZ);
        if (state->len > 0) {
            rc = memcpy_s(VARDATA(result), state->len, state->data, state->len);
            securec_check(rc, "\0", "\0");
        }
        PG_RETURN_BYTEA_P(result);
    } else
        PG_RETURN_NULL();
}

/*
 *		textin			- converts "..." to internal representation
 */
Datum textin(PG_FUNCTION_ARGS)
{
    char* inputText = PG_GETARG_CSTRING(0);
    PG_RETURN_TEXT_P(cstring_to_text(inputText));
}

/*
 *		textout			- converts internal representation to "..."
 */
Datum textout(PG_FUNCTION_ARGS)
{
    Datum txt = PG_GETARG_DATUM(0);
    if (VARATT_IS_HUGE_TOAST_POINTER(DatumGetPointer(txt))) {
        int len = strlen("<CLOB>") + 1;
        char *res = (char *)palloc(len);
        errno_t rc = strcpy_s(res, len, "<CLOB>");
        securec_check_c(rc, "\0", "\0");
        PG_RETURN_CSTRING(res);
    }
    PG_RETURN_CSTRING(TextDatumGetCString(txt));
}

/*
 *		textrecv			- converts external binary format to text
 */
Datum textrecv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    text* result = NULL;
    char* str = NULL;
    int nbytes;

    str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);

    result = cstring_to_text_with_len(str, nbytes);
    pfree_ext(str);
    PG_RETURN_TEXT_P(result);
}

/*
 *		textsend			- converts text to binary format
 */
Datum textsend(PG_FUNCTION_ARGS)
{
    text* t = PG_GETARG_TEXT_PP(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendtext(&buf, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 *		unknownin			- converts "..." to internal representation
 */
Datum unknownin(PG_FUNCTION_ARGS)
{
    char* str = PG_GETARG_CSTRING(0);

    /* representation is same as cstring */
    PG_RETURN_CSTRING(pstrdup(str));
}

/*
 *		unknownout			- converts internal representation to "..."
 */
Datum unknownout(PG_FUNCTION_ARGS)
{
    /* representation is same as cstring */
    char* str = PG_GETARG_CSTRING(0);

    PG_RETURN_CSTRING(pstrdup(str));
}

/*
 *		unknownrecv			- converts external binary format to unknown
 */
Datum unknownrecv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    char* str = NULL;
    int nbytes;

    str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    /* representation is same as cstring */
    PG_RETURN_CSTRING(str);
}

/*
 *		unknownsend			- converts unknown to binary format
 */
Datum unknownsend(PG_FUNCTION_ARGS)
{
    /* representation is same as cstring */
    char* str = PG_GETARG_CSTRING(0);
    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendtext(&buf, str, strlen(str));
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

static Datum text_length_huge(Datum str)
{
    if (pg_database_encoding_max_length() == 1) {
        PG_RETURN_INT64(toast_raw_datum_size(str) - VARHDRSZ);
    } else {
        int64 result = 0;
        text* t = DatumGetTextPP(str);
        result = calculate_huge_length(t);

        if ((Pointer)(t) != (Pointer)(str))
            pfree_ext(t);

        PG_RETURN_INT64(result);
    }
}

/* ========== PUBLIC ROUTINES ========== */

/*
 * textlen -
 *	  returns the logical length of a text*
 *	   (which is less than the VARSIZE of the text*)
 */
Datum textlen(PG_FUNCTION_ARGS)
{
    Datum str = fetch_real_lob_if_need(PG_GETARG_DATUM(0));

    if (VARATT_IS_HUGE_TOAST_POINTER((varlena *)DatumGetTextPP(str))) {
        return text_length_huge(str);
    } else {
        /* try to avoid decompressing argument */
        PG_RETURN_INT32(text_length(str));
    }
}

/*
 * text_length -
 *	Does the real work for textlen()
 *
 *	This is broken out so it can be called directly by other string processing
 *	functions.	Note that the argument is passed as a Datum, to indicate that
 *	it may still be in compressed form.  We can avoid decompressing it at all
 *	in some cases.
 */
int32 text_length(Datum str)
{
    /* fastpath when max encoding length is one */
    if (pg_database_encoding_max_length() == 1)
        PG_RETURN_INT32(toast_raw_datum_size(str) - VARHDRSZ);
    else {
        text* t = DatumGetTextPP(str);
        int32 result = 0;

        result = pg_mbstrlen_with_len(VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
        if ((Pointer)(t) != (Pointer)(str))
            pfree_ext(t);

        PG_RETURN_INT32(result);
    }
}

/*
 * textoctetlen -
 *	  returns the physical length of a text*
 *	   (which is less than the VARSIZE of the text*)
 */
Datum textoctetlen(PG_FUNCTION_ARGS)
{
    Datum str = PG_GETARG_DATUM(0);

    FUNC_CHECK_HUGE_POINTER(PG_ARGISNULL(0), DatumGetPointer(str), "textoctetlen()");

    /* We need not detoast the input at all */
    PG_RETURN_INT32(toast_raw_datum_size(str) - VARHDRSZ);
}

/*
 * textcat -
 *	  takes two text* and returns a text* that is the concatenation of
 *	  the two.
 *
 * Rewritten by Sapa, sapa@hq.icb.chel.su. 8-Jul-96.
 * Updated by Thomas, Thomas.Lockhart@jpl.nasa.gov 1997-07-10.
 * Allocate space for output in all cases.
 * XXX - thomas 1997-07-10
 */
Datum textcat(PG_FUNCTION_ARGS)
{
    // Empty string to NULL
    text* t1 = NULL;
    text* t2 = NULL;

    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
            PG_RETURN_NULL();
        else if (PG_ARGISNULL(0)) {
            t2 = PG_GETARG_TEXT_PP(1);
            PG_RETURN_TEXT_P(t2);
        } else if (PG_ARGISNULL(1)) {
            t1 = PG_GETARG_TEXT_PP(0);
            PG_RETURN_TEXT_P(t1);
        } else {
            t1 = PG_GETARG_TEXT_PP(0);
            t2 = PG_GETARG_TEXT_PP(1);
            PG_RETURN_TEXT_P(text_catenate(t1, t2));
        }
    } else {
        if (!PG_ARGISNULL(0) && !PG_ARGISNULL(1)) {
            t1 = PG_GETARG_TEXT_PP(0);
            t2 = PG_GETARG_TEXT_PP(1);
            PG_RETURN_TEXT_P(text_catenate(t1, t2));
        } else {
            PG_RETURN_NULL();
        }
    }
}

/*
 * text_catenate
 *	Guts of textcat(), broken out so it can be used by other functions
 *
 * Arguments can be in short-header form, but not compressed or out-of-line
 */
static text* text_catenate(text* t1, text* t2)
{
    text* result = NULL;
    int64 len1, len2, len;
    char* ptr = NULL;
    int rc = 0;

    if (VARATT_IS_HUGE_TOAST_POINTER(t1)) {
        struct varatt_lob_external large_toast_pointer;

        VARATT_EXTERNAL_GET_HUGE_POINTER(large_toast_pointer, t1);
        len1 = large_toast_pointer.va_rawsize;
    } else {
        len1 = VARSIZE_ANY_EXHDR(t1);
    }

    if (VARATT_IS_HUGE_TOAST_POINTER(t2)) {
        struct varatt_lob_external large_toast_pointer;

        VARATT_EXTERNAL_GET_HUGE_POINTER(large_toast_pointer, t2);
        len2 = large_toast_pointer.va_rawsize;
    } else {
        len2 = VARSIZE_ANY_EXHDR(t2);
    }

    /* paranoia ... probably should throw error instead? */
    if (len1 < 0)
        len1 = 0;
    if (len2 < 0)
        len2 = 0;

    len = len1 + len2 + VARHDRSZ;
    if (len > MAX_TOAST_CHUNK_SIZE + VARHDRSZ) {
#ifdef ENABLE_MULTIPLE_NODES
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Un-support clob/blob type more than 1GB for distributed system")));
#endif
        Oid toastOid = get_toast_oid();
        result = text_catenate_huge(t1, t2, toastOid);
    } else {
        result = (text*)palloc(len);

        /* Set size of result string... */
        SET_VARSIZE(result, len);

        /* Fill data field of result string... */
        ptr = VARDATA(result);
        if (len1 > 0) {
            rc = memcpy_s(ptr, len1, VARDATA_ANY(t1), len1);
            securec_check(rc, "\0", "\0");
        }
        if (len2 > 0) {
            rc = memcpy_s(ptr + len1, len2, VARDATA_ANY(t2), len2);
            securec_check(rc, "\0", "\0");
        }
    }

    return result;
}
void text_to_bktmap(text* gbucket, uint2* bktmap, int *bktlen)
{
    int s_idx = 0;
    int dest_idx = 0;
    int res_idx = 0;
    int bucket_nid = 0;
    char dest_str[MAX_NODE_DIG + 1] = {0};
    char* s = text_to_cstring(gbucket);
    int len_text = text_length((Datum)gbucket);
    while (s_idx < len_text && s[s_idx] != '\0' && res_idx < BUCKETDATALEN) {
        if (s[s_idx] == ',') {
            dest_str[dest_idx] = '\0';
            bucket_nid = atoi(dest_str);
            if (bucket_nid > MAX_DATANODE_NUM || bucket_nid < 0)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("Node id out of range")));
            if (bktmap) {
                bktmap[res_idx] = bucket_nid;
            }
            res_idx++;
            dest_idx = 0;
        } else {
            if (dest_idx >= MAX_NODE_DIG)
                ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("Node id is too long")));
            dest_str[dest_idx++] = s[s_idx];
        }
        s_idx++;
    }
    if (dest_idx > 0 && res_idx < BUCKETDATALEN) {
        dest_str[dest_idx] = '\0';
        if (bktmap) {
            bktmap[res_idx] = atoi(dest_str);
        }
        *bktlen = ++res_idx;
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("bucket map string is invalid")));
    }
    
    pfree_ext(s);
}
/*
 * charlen_to_bytelen()
 *	Compute the number of bytes occupied by n characters starting at *p
 *
 * It is caller's responsibility that there actually are n characters;
 * the string need not be null-terminated.
 */
static int charlen_to_bytelen(const char* p, int n)
{
    if (pg_database_encoding_max_length() == 1) {
        /* Optimization for single-byte encodings */
        return n;
    } else {
        const char* s = NULL;

        for (s = p; n > 0; n--)
            s += pg_mblen(s);

        return s - p;
    }
}

/*
 * text_substr()
 * Return a substring starting at the specified position.
 * - thomas 1997-12-31
 *
 * Input:
 *	- string
 *	- starting position (is one-based)
 *	- string length
 *
 * If the starting position is zero or less, then return from the start of the string
 *	adjusting the length to be consistent with the "negative start" per SQL92.
 * If the length is less than zero, return the remaining string.
 *
 * Added multibyte support.
 * - Tatsuo Ishii 1998-4-21
 * Changed behavior if starting position is less than one to conform to SQL92 behavior.
 * Formerly returned the entire string; now returns a portion.
 * - Thomas Lockhart 1998-12-10
 * Now uses faster TOAST-slicing interface
 * - John Gray 2002-02-22
 * Remove "#ifdef MULTIBYTE" and test for encoding_max_length instead. Change
 * behaviors conflicting with SQL92 to meet SQL92 (if E = S + L < S throw
 * error; if E < 1, return '', not entire string). Fixed MB related bug when
 * S > LC and < LC + 4 sometimes garbage characters are returned.
 * - Joe Conway 2002-08-10
 */
Datum text_substr(PG_FUNCTION_ARGS)
{
    text* result = NULL;

    result = text_substring(PG_GETARG_DATUM(0), PG_GETARG_INT32(1), PG_GETARG_INT32(2), false);

    PG_RETURN_TEXT_P(result);
}

/*
 * text_substr_null's function is same to text_substr, only with different return empty values.
 * when return value is a empty values, then return NULL to adapt NULL test.
 */
Datum text_substr_null(PG_FUNCTION_ARGS)
{
    Datum result;
    Datum str = PG_GETARG_DATUM(0);
    int32 start = PG_GETARG_INT32(1);
    int32 length = PG_GETARG_INT32(2);
    int32 eml = pg_database_encoding_max_length();
    bool is_compress = false;
    int baseIdx;
    bool is_null = false;
    mblen_converter fun_mblen;
    fun_mblen = *pg_wchar_table[GetDatabaseEncoding()].mblen;

    is_compress = (VARATT_IS_COMPRESSED(DatumGetPointer(str)) || VARATT_IS_EXTERNAL(DatumGetPointer(str)));
    // orclcompat is false withlen is true
    baseIdx = 2 + (int)is_compress + (eml - 1) * 8;

    result = (*substr_Array[baseIdx])(str, start, length, &is_null, fun_mblen);

    if (is_null == true)
        PG_RETURN_NULL();
    else
        return result;
}

Datum text_substr_no_len_null(PG_FUNCTION_ARGS)
{
    Datum result;
    Datum str = PG_GETARG_DATUM(0);
    int32 start = PG_GETARG_INT32(1);
    int32 eml = pg_database_encoding_max_length();
    bool is_compress = false;
    int baseIdx;
    bool is_null = false;
    mblen_converter fun_mblen;
    fun_mblen = *pg_wchar_table[GetDatabaseEncoding()].mblen;

    is_compress = (VARATT_IS_COMPRESSED(DatumGetPointer(str)) || VARATT_IS_EXTERNAL(DatumGetPointer(str)));
    // orclcompat is false withlen is false
    baseIdx = (int)is_compress + (eml - 1) * 8;

    result = (*substr_Array[baseIdx])(str, start, 0, &is_null, fun_mblen);

    if (is_null == true)
        PG_RETURN_NULL();
    else
        return result;
}
/*
 * text_substr_no_len -
 *	  Wrapper to avoid opr_sanity failure due to
 *	  one function accepting a different number of args.
 */
Datum text_substr_no_len(PG_FUNCTION_ARGS)
{
    text* result = NULL;

    result = text_substring(PG_GETARG_DATUM(0), PG_GETARG_INT32(1), -1, true);

    PG_RETURN_TEXT_P(result);
}

/*
 * text_substring -
 *	Does the real work for text_substr() and text_substr_no_len()
 *
 *	This is broken out so it can be called directly by other string processing
 *	functions.	Note that the argument is passed as a Datum, to indicate that
 *	it may still be in compressed/toasted form.  We can avoid detoasting all
 *	of it in some cases.
 *
 *	The result is always a freshly palloc'd datum.
 */
text* text_substring(Datum str, int32 start, int32 length, bool length_not_specified)
{
    int32 eml = pg_database_encoding_max_length();
    int32 S = start; /* start position */
    int32 S1;        /* adjusted start position */
    int32 L1;        /* adjusted substring length */

    /* life is easy if the encoding max length is 1 */
    if (eml == 1) {
        S1 = Max(S, 1);

        if (length_not_specified) /* special case - get length to end of
                                   * string */
            L1 = -1;
        else {
            /* end position */
            int E = S + length;

            /*
             * A negative value for L is the only way for the end position to
             * be before the start. SQL99 says to throw an error.
             */
            if (E < S)
                ereport(ERROR, (errcode(ERRCODE_SUBSTRING_ERROR), errmsg("negative substring length not allowed")));

            /*
             * A zero or negative value for the end position can happen if the
             * start was negative or one. SQL99 says to return a zero-length
             * string.
             */
            if (E < 1)
                return cstring_to_text("");

            L1 = E - S1;
        }

        /*
         * If the start position is past the end of the string, SQL99 says to
         * return a zero-length string -- PG_GETARG_TEXT_P_SLICE() will do
         * that for us. Convert to zero-based starting position
         */
        return DatumGetTextPSlice(str, S1 - 1, L1);
    } else if (eml > 1) {
        /*
         * When encoding max length is > 1, we can't get LC without
         * detoasting, so we'll grab a conservatively large slice now and go
         * back later to do the right thing
         */
        int32 slice_start;
        int32 slice_size;
        int32 slice_strlen;
        text* slice = NULL;
        int32 E1;
        int32 i;
        char* p = NULL;
        char* s = NULL;
        text* ret = NULL;

        /*
         * if S is past the end of the string, the tuple toaster will return a
         * zero-length string to us
         */
        S1 = Max(S, 1);

        /*
         * We need to start at position zero because there is no way to know
         * in advance which byte offset corresponds to the supplied start
         * position.
         */
        slice_start = 0;

        if (length_not_specified) /* special case - get length to end of
                                   * string */
            slice_size = L1 = -1;
        else {
            int E = S + length;

            /*
             * A negative value for L is the only way for the end position to
             * be before the start. SQL99 says to throw an error.
             */
            if (E < S)
                ereport(ERROR, (errcode(ERRCODE_SUBSTRING_ERROR), errmsg("negative substring length not allowed")));

            /*
             * A zero or negative value for the end position can happen if the
             * start was negative or one. SQL99 says to return a zero-length
             * string.
             */
            if (E < 1)
                return cstring_to_text("");

            /*
             * if E is past the end of the string, the tuple toaster will
             * truncate the length for us
             */
            L1 = E - S1;

            /*
             * Total slice size in bytes can't be any longer than the start
             * position plus substring length times the encoding max length.
             */
            slice_size = (S1 + L1) * eml;
        }

        /*
         * If we're working with an untoasted source, no need to do an extra
         * copying step.
         */
        if (VARATT_IS_COMPRESSED(DatumGetPointer(str)) || VARATT_IS_EXTERNAL(DatumGetPointer(str)))
            slice = DatumGetTextPSlice(str, slice_start, slice_size);
        else
            slice = (text*)DatumGetPointer(str);

        /* see if we got back an empty string */
        if (VARSIZE_ANY_EXHDR(slice) == 0) {
            if (slice != (text*)DatumGetPointer(str))
                pfree_ext(slice);
            return cstring_to_text("");
        }

        /* Now we can get the actual length of the slice in MB characters */
        slice_strlen = pg_mbstrlen_with_len(VARDATA_ANY(slice), VARSIZE_ANY_EXHDR(slice));

        /*
         * Check that the start position wasn't > slice_strlen. If so, SQL99
         * says to return a zero-length string.
         */
        if (S1 > slice_strlen) {
            if (slice != (text*)DatumGetPointer(str))
                pfree_ext(slice);
            return cstring_to_text("");
        }

        /*
         * Adjust L1 and E1 now that we know the slice string length. Again
         * remember that S1 is one based, and slice_start is zero based.
         */
        if (L1 > -1)
            E1 = Min(S1 + L1, slice_start + 1 + slice_strlen);
        else
            E1 = slice_start + 1 + slice_strlen;

        /*
         * Find the start position in the slice; remember S1 is not zero based
         */
        p = VARDATA_ANY(slice);
        for (i = 0; i < S1 - 1; i++)
            p += pg_mblen(p);

        /* hang onto a pointer to our start position */
        s = p;

        /*
         * Count the actual bytes used by the substring of the requested
         * length.
         */
        for (i = S1; i < E1; i++)
            p += pg_mblen(p);

        ret = (text*)palloc(VARHDRSZ + (p - s));
        SET_VARSIZE(ret, VARHDRSZ + (p - s));
        if (p - s > 0) {
            int rc = memcpy_s(VARDATA(ret), VARSIZE(ret), s, (p - s));
            securec_check(rc, "\0", "\0");
        }
        if (slice != (text*)DatumGetPointer(str))
            pfree_ext(slice);

        return ret;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid backend encoding: encoding max length < 1")));
    }

    /* not reached: suppress compiler warning */
    return NULL;
}

// adapt A db's substr(text str,integer start,integer length)
// when start<0, amend the sartPosition to abs(start) from last char,
// then search backward
Datum text_substr_orclcompat(PG_FUNCTION_ARGS)
{
    Datum result;
    Datum str = PG_GETARG_DATUM(0);
    int32 start = PG_GETARG_INT32(1);
    int32 length = PG_GETARG_INT32(2);
    bool is_null = false;
    bool is_compress = false;
    int baseIdx;
    int32 eml = pg_database_encoding_max_length();
    mblen_converter fun_mblen;
    fun_mblen = *pg_wchar_table[GetDatabaseEncoding()].mblen;

    FUNC_CHECK_HUGE_POINTER(PG_ARGISNULL(0), DatumGetPointer(str), "text_substr()");

    is_compress = (VARATT_IS_COMPRESSED(DatumGetPointer(str)) || VARATT_IS_EXTERNAL(DatumGetPointer(str)));
    // orclcompat is true, withlen is true
    baseIdx = 6 + (int)is_compress + (eml - 1) * 8;

    result = (*substr_Array[baseIdx])(str, start, length, &is_null, fun_mblen);

    if (is_null == true)
        PG_RETURN_NULL();
    else
        return result;
}

// adapt A db's substr(text str,integer start)
// when start<0, amend the sartPosition to abs(start) from last char,
// then search backward
Datum text_substr_no_len_orclcompat(PG_FUNCTION_ARGS)
{
    Datum result;
    Datum str = PG_GETARG_DATUM(0);
    int32 start = PG_GETARG_INT32(1);
    int32 eml = pg_database_encoding_max_length();
    bool is_compress = false;
    int baseIdx;
    bool is_null = false;
    mblen_converter fun_mblen;
    fun_mblen = *pg_wchar_table[GetDatabaseEncoding()].mblen;

    FUNC_CHECK_HUGE_POINTER(PG_ARGISNULL(0), DatumGetPointer(str), "text_substr()");

    is_compress = (VARATT_IS_COMPRESSED(DatumGetPointer(str)) || VARATT_IS_EXTERNAL(DatumGetPointer(str)));
    // orclcompat is true, withlen is false
    baseIdx = 4 + (int)is_compress + (eml - 1) * 8;

    result = (*substr_Array[baseIdx])(str, start, 0, &is_null, fun_mblen);

    if (is_null == true)
        PG_RETURN_NULL();
    else
        return result;
}

/*
 * textoverlay
 *	Replace specified substring of first string with second
 *
 * The SQL standard defines OVERLAY() in terms of substring and concatenation.
 * This code is a direct implementation of what the standard says.
 */
Datum textoverlay(PG_FUNCTION_ARGS)
{
    text* t1 = PG_GETARG_TEXT_PP(0);
    text* t2 = PG_GETARG_TEXT_PP(1);

    FUNC_CHECK_HUGE_POINTER(false, t1, "textoverlay()");
    int sp = PG_GETARG_INT32(2); /* substring start position */
    int sl = PG_GETARG_INT32(3); /* substring length */

    PG_RETURN_TEXT_P(text_overlay(t1, t2, sp, sl));
}

Datum textoverlay_no_len(PG_FUNCTION_ARGS)
{
    text* t1 = PG_GETARG_TEXT_PP(0);
    text* t2 = PG_GETARG_TEXT_PP(1);
    FUNC_CHECK_HUGE_POINTER(false, t1, "textoverlay()");
    int sp = PG_GETARG_INT32(2); /* substring start position */
    int sl;

    sl = text_length(PointerGetDatum(t2)); /* defaults to length(t2) */
    PG_RETURN_TEXT_P(text_overlay(t1, t2, sp, sl));
}

static text* text_overlay(text* t1, text* t2, int sp, int sl)
{
    text* result = NULL;
    text* s1 = NULL;
    text* s2 = NULL;
    int sp_pl_sl;

    /*
     * Check for possible integer-overflow cases.  For negative sp, throw a
     * "substring length" error because that's what should be expected
     * according to the spec's definition of OVERLAY().
     */
    if (sp <= 0)
        ereport(ERROR, (errcode(ERRCODE_SUBSTRING_ERROR), errmsg("negative substring length not allowed")));
    if (pg_add_s32_overflow(sp, sl, &sp_pl_sl))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));

    s1 = text_substring(PointerGetDatum(t1), 1, sp - 1, false);
    s2 = text_substring(PointerGetDatum(t1), sp_pl_sl, -1, true);
    result = text_catenate(s1, t2);
    result = text_catenate(result, s2);

    return result;
}

/*
 * textpos -
 *	  Return the position of the specified substring.
 *	  Implements the SQL92 POSITION() function.
 *	  Ref: A Guide To The SQL Standard, Date & Darwen, 1997
 * - thomas 1997-07-27
 */
Datum textpos(PG_FUNCTION_ARGS)
{
    text* str = PG_GETARG_TEXT_PP(0);
    text* search_str = PG_GETARG_TEXT_PP(1);
    FUNC_CHECK_HUGE_POINTER(PG_ARGISNULL(0), str, "textpos()");

    PG_RETURN_INT32((int32)text_position(str, search_str));
}

/*
 * text_position -
 *	Does the real work for textpos()
 *
 * Inputs:
 *		t1 - string to be searched
 *		t2 - pattern to match within t1
 * Result:
 *		Character index of the first matched char, starting from 1,
 *		or 0 if no match.
 *
 *	This is broken out so it can be called directly by other string processing
 *	functions.
 */
static int text_position(text* t1, text* t2)
{
    TextPositionState state;
    int result;

    text_position_setup(t1, t2, &state);
    result = text_position_next(1, &state);
    text_position_cleanup(&state);
    return result;
}

/*
 * text_position_setup, text_position_next, text_position_cleanup -
 *	Component steps of text_position()
 *
 * These are broken out so that a string can be efficiently searched for
 * multiple occurrences of the same pattern.  text_position_next may be
 * called multiple times with increasing values of start_pos, which is
 * the 1-based character position to start the search from.  The "state"
 * variable is normally just a local variable in the caller.
 */

static void text_position_setup(text* t1, text* t2, TextPositionState* state)
{
    int len1 = VARSIZE_ANY_EXHDR(t1);
    int len2 = VARSIZE_ANY_EXHDR(t2);

    if (pg_database_encoding_max_length() == 1) {
        /* simple case - single byte encoding */
        state->use_wchar = false;
        state->str1 = VARDATA_ANY(t1);
        state->str2 = VARDATA_ANY(t2);
        state->len1 = len1;
        state->len2 = len2;
    } else {
        /* not as simple - multibyte encoding */
        pg_wchar *p1, *p2;

        p1 = (pg_wchar*)palloc((len1 + 1) * sizeof(pg_wchar));
        len1 = pg_mb2wchar_with_len(VARDATA_ANY(t1), p1, len1);
        p2 = (pg_wchar*)palloc((len2 + 1) * sizeof(pg_wchar));
        len2 = pg_mb2wchar_with_len(VARDATA_ANY(t2), p2, len2);

        state->use_wchar = true;
        state->wstr1 = p1;
        state->wstr2 = p2;
        state->len1 = len1;
        state->len2 = len2;
    }

    /*
     * Prepare the skip table for Boyer-Moore-Horspool searching.  In these
     * notes we use the terminology that the "haystack" is the string to be
     * searched (t1) and the "needle" is the pattern being sought (t2).
     *
     * If the needle is empty or bigger than the haystack then there is no
     * point in wasting cycles initializing the table.	We also choose not to
     * use B-M-H for needles of length 1, since the skip table can't possibly
     * save anything in that case.
     */
    if (len1 >= len2 && len2 > 1) {
        int searchlength = len1 - len2;
        int skiptablemask;
        int last;
        int i;

        /*
         * First we must determine how much of the skip table to use.  The
         * declaration of TextPositionState allows up to 256 elements, but for
         * short search problems we don't really want to have to initialize so
         * many elements --- it would take too long in comparison to the
         * actual search time.	So we choose a useful skip table size based on
         * the haystack length minus the needle length.  The closer the needle
         * length is to the haystack length the less useful skipping becomes.
         *
         * Note: since we use bit-masking to select table elements, the skip
         * table size MUST be a power of 2, and so the mask must be 2^N-1.
         */
        if (searchlength < 16)
            skiptablemask = 3;
        else if (searchlength < 64)
            skiptablemask = 7;
        else if (searchlength < 128)
            skiptablemask = 15;
        else if (searchlength < 512)
            skiptablemask = 31;
        else if (searchlength < 2048)
            skiptablemask = 63;
        else if (searchlength < 4096)
            skiptablemask = 127;
        else
            skiptablemask = 255;
        state->skiptablemask = skiptablemask;

        /*
         * Initialize the skip table.  We set all elements to the needle
         * length, since this is the correct skip distance for any character
         * not found in the needle.
         */
        for (i = 0; i <= skiptablemask; i++)
            state->skiptable[i] = len2;

        /*
         * Now examine the needle.	For each character except the last one,
         * set the corresponding table element to the appropriate skip
         * distance.  Note that when two characters share the same skip table
         * entry, the one later in the needle must determine the skip
         * distance.
         */
        last = len2 - 1;

        if (!state->use_wchar) {
            const char* str2 = state->str2;

            for (i = 0; i < last; i++)
                state->skiptable[(unsigned char)str2[i] & (unsigned int)skiptablemask] = last - i;
        } else {
            const pg_wchar* wstr2 = state->wstr2;

            for (i = 0; i < last; i++)
                state->skiptable[wstr2[i] & (unsigned int)skiptablemask] = last - i;
        }
    }
}

static int text_position_next(int start_pos, TextPositionState* state)
{
    int haystack_len = state->len1;
    int needle_len = state->len2;
    int skiptablemask = state->skiptablemask;

    Assert(start_pos > 0); /* else caller error */

    if (needle_len <= 0)
        return start_pos; /* result for empty pattern */

    start_pos--; /* adjust for zero based arrays */

    /* Done if the needle can't possibly fit */
    if (haystack_len < start_pos + needle_len)
        return 0;

    if (!state->use_wchar) {
        /* simple case - single byte encoding */
        const char* haystack = state->str1;
        const char* needle = state->str2;
        const char* haystack_end = &haystack[haystack_len];
        const char* hptr = NULL;

        if (needle_len == 1) {
            /* No point in using B-M-H for a one-character needle */
            char nchar = *needle;

            hptr = &haystack[start_pos];
            while (hptr < haystack_end) {
                if (*hptr == nchar)
                    return hptr - haystack + 1;
                hptr++;
            }
        } else {
            const char* needle_last = &needle[needle_len - 1];

            /* Start at startpos plus the length of the needle */
            hptr = &haystack[start_pos + needle_len - 1];
            while (hptr < haystack_end) {
                /* Match the needle scanning *backward* */
                const char* nptr = NULL;
                const char* p = NULL;

                nptr = needle_last;
                p = hptr;
                while (*nptr == *p) {
                    /* Matched it all?	If so, return 1-based position */
                    if (nptr == needle)
                        return p - haystack + 1;
                    nptr--, p--;
                }

                /*
                 * No match, so use the haystack char at hptr to decide how
                 * far to advance.	If the needle had any occurrence of that
                 * character (or more precisely, one sharing the same
                 * skiptable entry) before its last character, then we advance
                 * far enough to align the last such needle character with
                 * that haystack position.	Otherwise we can advance by the
                 * whole needle length.
                 */
                hptr += state->skiptable[(unsigned char)*hptr & (unsigned int)skiptablemask];
            }
        }
    } else {
        /* The multibyte char version. This works exactly the same way. */
        const pg_wchar* haystack = state->wstr1;
        const pg_wchar* needle = state->wstr2;
        const pg_wchar* haystack_end = &haystack[haystack_len];
        const pg_wchar* hptr = NULL;

        if (needle_len == 1) {
            /* No point in using B-M-H for a one-character needle */
            pg_wchar nchar = *needle;

            hptr = &haystack[start_pos];
            while (hptr < haystack_end) {
                if (*hptr == nchar)
                    return hptr - haystack + 1;
                hptr++;
            }
        } else {
            const pg_wchar* needle_last = &needle[needle_len - 1];

            /* Start at startpos plus the length of the needle */
            hptr = &haystack[start_pos + needle_len - 1];
            while (hptr < haystack_end) {
                /* Match the needle scanning *backward* */
                const pg_wchar* nptr = NULL;
                const pg_wchar* p = NULL;

                nptr = needle_last;
                p = hptr;
                while (*nptr == *p) {
                    /* Matched it all?	If so, return 1-based position */
                    if (nptr == needle)
                        return p - haystack + 1;
                    nptr--, p--;
                }

                /*
                 * No match, so use the haystack char at hptr to decide how
                 * far to advance.	If the needle had any occurrence of that
                 * character (or more precisely, one sharing the same
                 * skiptable entry) before its last character, then we advance
                 * far enough to align the last such needle character with
                 * that haystack position.	Otherwise we can advance by the
                 * whole needle length.
                 */
                hptr += state->skiptable[*hptr & (unsigned int)skiptablemask];
            }
        }
    }

    return 0; /* not found */
}

static void text_position_cleanup(TextPositionState* state)
{
    if (state->use_wchar) {
        pfree_ext(state->wstr1);
        pfree_ext(state->wstr2);
    }
}

/* varstr_cmp()
 * Comparison function for text strings with given lengths.
 * Includes locale support, but must copy strings to temporary memory
 *	to allow null-termination for inputs to strcoll().
 * Returns an integer less than, equal to, or greater than zero, indicating
 * whether arg1 is less than, equal to, or greater than arg2.
 */
int varstr_cmp(char* arg1, int len1, char* arg2, int len2, Oid collid)
{
    int result;

    /*
     * Unfortunately, there is no strncoll(), so in the non-C locale case we
     * have to do some memory copying.	This turns out to be significantly
     * slower, so we optimize the case where LC_COLLATE is C.  We also try to
     * optimize relatively-short strings by avoiding palloc/pfree overhead.
     */
    if (lc_collate_is_c(collid)) {
        result = memcmp(arg1, arg2, Min(len1, len2));
        if ((result == 0) && (len1 != len2))
            result = (len1 < len2) ? -1 : 1;
    } else {
        char a1buf[TEXTBUFLEN];
        char a2buf[TEXTBUFLEN];
        char *a1p = NULL, *a2p = NULL;

#ifdef HAVE_LOCALE_T
        pg_locale_t mylocale = 0;
#endif

        if (collid != DEFAULT_COLLATION_OID) {
            if (!OidIsValid(collid)) {
                /*
                 * This typically means that the parser could not resolve a
                 * conflict of implicit collations, so report it that way.
                 */
                ereport(ERROR,
                    (errcode(ERRCODE_INDETERMINATE_COLLATION),
                        errmsg("could not determine which collation to use for string comparison"),
                        errhint("Use the COLLATE clause to set the collation explicitly.")));
            }
#ifdef HAVE_LOCALE_T
            mylocale = pg_newlocale_from_collation(collid);
#endif
        }

        /*
         * memcmp() can't tell us which of two unequal strings sorts first, but
         * it's a cheap way to tell if they're equal.  Testing shows that
         * memcmp() followed by strcoll() is only trivially slower than
         * strcoll() by itself, so we don't lose much if this doesn't work out
         * very often, and if it does - for example, because there are many
         * equal strings in the input - then we win big by avoiding expensive
         * collation-aware comparisons.
         */
        if (len1 == len2 && memcmp(arg1, arg2, len1) == 0)
            return 0;

#ifdef WIN32
        /* Win32 does not have UTF-8, so we need to map to UTF-16 */
        if (GetDatabaseEncoding() == PG_UTF8) {
            int a1len;
            int a2len;
            int r;

            if (len1 >= TEXTBUFLEN / 2) {
                a1len = len1 * 2 + 2;
                a1p = palloc(a1len);
            } else {
                a1len = TEXTBUFLEN;
                a1p = a1buf;
            }
            if (len2 >= TEXTBUFLEN / 2) {
                a2len = len2 * 2 + 2;
                a2p = palloc(a2len);
            } else {
                a2len = TEXTBUFLEN;
                a2p = a2buf;
            }

            /* stupid Microsloth API does not work for zero-length input */
            if (len1 == 0)
                r = 0;
            else {
                r = MultiByteToWideChar(CP_UTF8, 0, arg1, len1, (LPWSTR)a1p, a1len / 2);
                if (!r)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_CHARACTER_VALUE_FOR_CAST),
                            errmsg("could not convert string to UTF-16: error code %lu", GetLastError())));
            }
            ((LPWSTR)a1p)[r] = 0;

            if (len2 == 0)
                r = 0;
            else {
                r = MultiByteToWideChar(CP_UTF8, 0, arg2, len2, (LPWSTR)a2p, a2len / 2);
                if (!r)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_CHARACTER_VALUE_FOR_CAST),
                            errmsg("could not convert string to UTF-16: error code %lu", GetLastError())));
            }
            ((LPWSTR)a2p)[r] = 0;

            errno = 0;
#ifdef HAVE_LOCALE_T
            if (mylocale)
                result = wcscoll_l((LPWSTR)a1p, (LPWSTR)a2p, mylocale);
            else
#endif
                result = wcscoll((LPWSTR)a1p, (LPWSTR)a2p);
            if (result == 2147483647) /* _NLSCMPERROR; missing from mingw
                                       * headers */
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("could not compare Unicode strings: %m")));

            /*
             * In some locales wcscoll() can claim that nonidentical strings
             * are equal.  Believing that would be bad news for a number of
             * reasons, so we follow Perl's lead and sort "equal" strings
             * according to strcmp (on the UTF-8 representation).
             */
            if (result == 0) {
                result = memcmp(arg1, arg2, Min(len1, len2));
                if ((result == 0) && (len1 != len2))
                    result = (len1 < len2) ? -1 : 1;
            }

            if (a1p != a1buf)
                pfree_ext(a1p);
            if (a2p != a2buf)
                pfree_ext(a2p);

            return result;
        }
#endif /* WIN32 */

        if (len1 >= TEXTBUFLEN)
            a1p = (char*)palloc(len1 + 1);
        else
            a1p = a1buf;
        if (len2 >= TEXTBUFLEN)
            a2p = (char*)palloc(len2 + 1);
        else
            a2p = a2buf;

        errno_t err = EOK;
        if (len1 > 0) {
            err = memcpy_s(a1p, len1, arg1, len1);
            securec_check(err, "\0", "\0");
        }
        a1p[len1] = '\0';
        if (len2 > 0) {
            err = memcpy_s(a2p, len2, arg2, len2);
            securec_check(err, "\0", "\0");
        }
        a2p[len2] = '\0';

#ifdef HAVE_LOCALE_T
        if (mylocale)
            result = strcoll_l(a1p, a2p, mylocale);
        else
#endif
            result = strcoll(a1p, a2p);

        /*
         * In some locales strcoll() can claim that nonidentical strings are
         * equal.  Believing that would be bad news for a number of reasons,
         * so we follow Perl's lead and sort "equal" strings according to
         * strcmp().
         */
        if (result == 0)
            result = strcmp(a1p, a2p);

        if (a1p != a1buf)
            pfree_ext(a1p);
        if (a2p != a2buf)
            pfree_ext(a2p);
    }

    return result;
}

/* text_cmp()
 * Internal comparison function for text strings.
 * Returns -1, 0 or 1
 */
int text_cmp(text* arg1, text* arg2, Oid collid)
{
    char *a1p = NULL, *a2p = NULL;
    int len1, len2;

    a1p = VARDATA_ANY(arg1);
    a2p = VARDATA_ANY(arg2);

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    return varstr_cmp(a1p, len1, a2p, len2, collid);
}

/*
 * Comparison functions for text strings.
 *
 * Note: btree indexes need these routines not to leak memory; therefore,
 * be careful to free working copies of toasted datums.  Most places don't
 * need to be so careful.
 */

Datum texteq(PG_FUNCTION_ARGS)
{
    Datum arg1 = PG_GETARG_DATUM(0);
    Datum arg2 = PG_GETARG_DATUM(1);

    if (VARATT_IS_HUGE_TOAST_POINTER(DatumGetPointer(arg1)) || VARATT_IS_HUGE_TOAST_POINTER(DatumGetPointer(arg2))) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("texteq could not support more than 1GB clob/blob data")));
    }
    bool result = false;
    Size len1, len2;

    /*
     * Since we only care about equality or not-equality, we can avoid all the
     * expense of strcoll() here, and just do bitwise comparison.  In fact, we
     * don't even have to do a bitwise comparison if we can show the lengths
     * of the strings are unequal; which might save us from having to detoast
     * one or both values.
     */
    len1 = toast_raw_datum_size(arg1);
    len2 = toast_raw_datum_size(arg2);
    if (len1 != len2)
        result = false;
    else {
        text* targ1 = DatumGetTextPP(arg1);
        text* targ2 = DatumGetTextPP(arg2);

        result = (memcmp(VARDATA_ANY(targ1), VARDATA_ANY(targ2), len1 - VARHDRSZ) == 0);

        PG_FREE_IF_COPY(targ1, 0);
        PG_FREE_IF_COPY(targ2, 1);
    }

    PG_RETURN_BOOL(result);
}

Datum textne(PG_FUNCTION_ARGS)
{
    Datum arg1 = PG_GETARG_DATUM(0);
    Datum arg2 = PG_GETARG_DATUM(1);
    if (VARATT_IS_HUGE_TOAST_POINTER(DatumGetPointer(arg1)) || VARATT_IS_HUGE_TOAST_POINTER(DatumGetPointer(arg2))) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("textne could not support more than 1GB clob/blob data")));
    }
    bool result = false;
    Size len1, len2;

    /* See comment in texteq() */
    len1 = toast_raw_datum_size(arg1);
    len2 = toast_raw_datum_size(arg2);
    if (len1 != len2)
        result = true;
    else {
        text* targ1 = DatumGetTextPP(arg1);
        text* targ2 = DatumGetTextPP(arg2);

        result = (memcmp(VARDATA_ANY(targ1), VARDATA_ANY(targ2), len1 - VARHDRSZ) != 0);

        PG_FREE_IF_COPY(targ1, 0);
        PG_FREE_IF_COPY(targ2, 1);
    }

    PG_RETURN_BOOL(result);
}

/*
 * The internal realization of function vtextne.
 */
template <bool m_const1, bool m_const2>
static void vtextne_internal(ScalarVector* arg1, uint8* pflags1, ScalarVector* arg2, uint8* pflags2,
    ScalarVector* vresult, uint8* pflagRes, Size len, text* targ, int idx)
{
    if (BOTH_NOT_NULL(pflags1[idx], pflags2[idx])) {
        Size len1 = m_const1 ? len : toast_raw_datum_size(arg1->m_vals[idx]);
        Size len2 = m_const2 ? len : toast_raw_datum_size(arg2->m_vals[idx]);
        if (len1 != len2)
            vresult->m_vals[idx] = BoolGetDatum(true);
        else {
            text* targ1 = m_const1 ? targ : DatumGetTextPP(arg1->m_vals[idx]);
            text* targ2 = m_const2 ? targ : DatumGetTextPP(arg2->m_vals[idx]);
            bool result = memcmp(VARDATA_ANY(targ1), VARDATA_ANY(targ2), len1 - VARHDRSZ) != 0;
            vresult->m_vals[idx] = BoolGetDatum(result);
        }

        // Since pflagRes can be resued by the other Batch, the pflagRes must be
        // set not null here if both two args are not null.
        //
        SET_NOTNULL(pflagRes[idx]);
    } else {
        SET_NULL(pflagRes[idx]);
    }
}

ScalarVector* vtextne(PG_FUNCTION_ARGS)
{
    ScalarVector* arg1 = PG_GETARG_VECTOR(0);
    ScalarVector* arg2 = PG_GETARG_VECTOR(1);
    uint8* pflags1 = arg1->m_flag;
    uint8* pflags2 = arg2->m_flag;
    int32 nvalues = PG_GETARG_INT32(2);
    ScalarVector* vresult = PG_GETARG_VECTOR(3);
    uint8* pflagRes = (uint8*)(vresult->m_flag);
    bool* pselection = PG_GETARG_SELECTION(4);
    int k;
    Size len = 0;
    text* targ = NULL;

    if (arg1->m_const && NOT_NULL(pflags1[0])) {
        len = toast_raw_datum_size(arg1->m_vals[0]);
        targ = DatumGetTextPP(arg1->m_vals[0]);
    }
    if (arg2->m_const && NOT_NULL(pflags2[0])) {
        len = toast_raw_datum_size(arg2->m_vals[0]);
        targ = DatumGetTextPP(arg2->m_vals[0]);
    }

    /*
     * Since if both arg1->m_const and arg2->m_const are true,
     * we would never enter here. We only consider three cases.
     */
    if (pselection != NULL) {
        for (k = 0; k < nvalues; k++) {
            if (pselection[k]) {
                if (!arg1->m_const && arg2->m_const)
                    vtextne_internal<false, true>(arg1, pflags1, arg2, pflags2, vresult, pflagRes, len, targ, k);
                else if (arg1->m_const && !arg2->m_const)
                    vtextne_internal<true, false>(arg1, pflags1, arg2, pflags2, vresult, pflagRes, len, targ, k);
                else
                    vtextne_internal<false, false>(arg1, pflags1, arg2, pflags2, vresult, pflagRes, len, targ, k);
            }
        }
    } else {
        for (k = 0; k < nvalues; k++) {
            if (!arg1->m_const && arg2->m_const)
                vtextne_internal<false, true>(arg1, pflags1, arg2, pflags2, vresult, pflagRes, len, targ, k);
            else if (arg1->m_const && !arg2->m_const)
                vtextne_internal<true, false>(arg1, pflags1, arg2, pflags2, vresult, pflagRes, len, targ, k);
            else
                vtextne_internal<false, false>(arg1, pflags1, arg2, pflags2, vresult, pflagRes, len, targ, k);
        }
    }

    PG_GETARG_VECTOR(3)->m_rows = nvalues;
    return PG_GETARG_VECTOR(3);
}

Datum text_lt(PG_FUNCTION_ARGS)
{
    text* arg1 = PG_GETARG_TEXT_PP(0);
    text* arg2 = PG_GETARG_TEXT_PP(1);
    FUNC_CHECK_HUGE_POINTER(false, arg1, "text_lt()");
    bool result = false;

    result = (text_cmp(arg1, arg2, PG_GET_COLLATION()) < 0);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result);
}

Datum text_le(PG_FUNCTION_ARGS)
{
    text* arg1 = PG_GETARG_TEXT_PP(0);
    text* arg2 = PG_GETARG_TEXT_PP(1);
    FUNC_CHECK_HUGE_POINTER(false, arg1, "text_le()");

    bool result = false;

    result = (text_cmp(arg1, arg2, PG_GET_COLLATION()) <= 0);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result);
}

Datum text_gt(PG_FUNCTION_ARGS)
{
    text* arg1 = PG_GETARG_TEXT_PP(0);
    text* arg2 = PG_GETARG_TEXT_PP(1);
    FUNC_CHECK_HUGE_POINTER(false, arg1, "text_gt()");

    bool result = false;

    result = (text_cmp(arg1, arg2, PG_GET_COLLATION()) > 0);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result);
}

Datum text_ge(PG_FUNCTION_ARGS)
{
    text* arg1 = PG_GETARG_TEXT_PP(0);
    text* arg2 = PG_GETARG_TEXT_PP(1);
    FUNC_CHECK_HUGE_POINTER(false, arg1, "text_ge()");

    bool result = false;

    result = (text_cmp(arg1, arg2, PG_GET_COLLATION()) >= 0);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result);
}

Datum bttextcmp(PG_FUNCTION_ARGS)
{
    text* arg1 = PG_GETARG_TEXT_PP(0);
    text* arg2 = PG_GETARG_TEXT_PP(1);
    FUNC_CHECK_HUGE_POINTER(false, arg1, "bttextcmp()");

    int32 result;

    result = text_cmp(arg1, arg2, PG_GET_COLLATION());

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_INT32(result);
}

Datum bttextsortsupport(PG_FUNCTION_ARGS)
{
    SortSupport ssup = (SortSupport)PG_GETARG_POINTER(0);
    Oid collid = ssup->ssup_collation;
    MemoryContext oldcontext;

    oldcontext = MemoryContextSwitchTo(ssup->ssup_cxt);

    /* Use generic string SortSupport */
    varstr_sortsupport(ssup, collid, false);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_VOID();
}

/*
 * Generic sortsupport interface for character type's operator classes.
 * Includes locale support, and support for BpChar semantics (i.e. removing
 * trailing spaces before comparison).
 *
 * Relies on the assumption that text, VarChar, BpChar, and bytea all have the
 * same representation.  Callers that always use the C collation (e.g.
 * non-collatable type callers like bytea) may have NUL bytes in their strings;
 * this will not work with any other collation, though.
 */
void varstr_sortsupport(SortSupport ssup, Oid collid, bool bpchar)
{
    bool abbreviate = ssup->abbreviate;
    bool collate_c = false;
    VarStringSortSupport* sss = NULL;

#ifdef HAVE_LOCALE_T
    pg_locale_t locale = 0;
#endif

    /*
     * If possible, set ssup->comparator to a function which can be used to
     * directly compare two datums.  If we can do this, we'll avoid the
     * overhead of a trip through the fmgr layer for every comparison, which
     * can be substantial.
     *
     * Most typically, we'll set the comparator to varstrfastcmp_locale, which
     * uses strcoll() to perform comparisons and knows about the special
     * requirements of BpChar callers.  However, if LC_COLLATE = C, we can
     * make things quite a bit faster with varstrfastcmp_c or bpcharfastcmp_c,
     * both of which use memcmp() rather than strcoll().
     *
     * There is a further exception on Windows.  When the database encoding is
     * UTF-8 and we are not using the C collation, complex hacks are required.
     * We don't currently have a comparator that handles that case, so we fall
     * back on the slow method of having the sort code invoke bttextcmp() (in
     * the case of text) via the fmgr trampoline.
     */
    if (lc_collate_is_c(collid)) {
        if (!bpchar)
            ssup->comparator = varstrfastcmp_c;
        else
            ssup->comparator = bpcharfastcmp_c;

        collate_c = true;
    }
#ifdef WIN32
    else if (GetDatabaseEncoding() == PG_UTF8)
        return;
#endif
    else {
        ssup->comparator = varstrfastcmp_locale;

        /*
         * We need a collation-sensitive comparison.  To make things faster,
         * we'll figure out the collation based on the locale id and cache the
         * result.
         */
        if (collid != DEFAULT_COLLATION_OID) {
            if (!OidIsValid(collid)) {
                /*
                 * This typically means that the parser could not resolve a
                 * conflict of implicit collations, so report it that way.
                 */
                ereport(ERROR,
                    (errcode(ERRCODE_INDETERMINATE_COLLATION),
                        errmsg("could not determine which collation to use for string comparison"),
                        errhint("Use the COLLATE clause to set the collation explicitly.")));
            }
#ifdef HAVE_LOCALE_T
            locale = pg_newlocale_from_collation(collid);
#endif
        }
    }

    /*
     * Unfortunately, it seems that abbreviation for non-C collations is
     * broken on many common platforms; testing of multiple versions of glibc
     * reveals that, for many locales, strcoll() and strxfrm() do not return
     * consistent results, which is fatal to this optimization.  While no
     * other libc other than Cygwin has so far been shown to have a problem,
     * we take the conservative course of action for right now and disable
     * this categorically.  (Users who are certain this isn't a problem on
     * their system can define TRUST_STRXFRM.)
     *
     * Even apart from the risk of broken locales, it's possible that there
     * are platforms where the use of abbreviated keys should be disabled at
     * compile time.  Having only 4 byte datums could make worst-case
     * performance drastically more likely, for example.  Moreover, Darwin's
     * strxfrm() implementations is known to not effectively concentrate a
     * significant amount of entropy from the original string in earlier
     * transformed blobs.  It's possible that other supported platforms are
     * similarly encumbered.  So, if we ever get past disabling this
     * categorically, we may still want or need to disable it for particular
     * platforms.
     */

    /*
     * If we're using abbreviated keys, or if we're using a locale-aware
     * comparison, we need to initialize a StringSortSupport object.  Both
     * cases will make use of the temporary buffers we initialize here for
     * scratch space (and to detect requirement for BpChar semantics from
     * caller), and the abbreviation case requires additional state.
     */
    if (abbreviate || !collate_c) {
        sss = (VarStringSortSupport*)palloc(sizeof(VarStringSortSupport));
        sss->buf1 = (char*)palloc(TEXTBUFLEN);
        sss->buflen1 = TEXTBUFLEN;
        sss->buf2 = (char*)palloc(TEXTBUFLEN);
        sss->buflen2 = TEXTBUFLEN;
        /* Start with invalid values */
        sss->last_len1 = -1;
        sss->last_len2 = -1;
        /* Initialize */
        sss->last_returned = 0;
#ifdef HAVE_LOCALE_T
        sss->locale = locale;
#endif

        /*
         * To avoid somehow confusing a strxfrm() blob and an original string,
         * constantly keep track of the variety of data that buf1 and buf2
         * currently contain.
         *
         * Comparisons may be interleaved with conversion calls.  Frequently,
         * conversions and comparisons are batched into two distinct phases,
         * but the correctness of caching cannot hinge upon this.  For
         * comparison caching, buffer state is only trusted if cache_blob is
         * found set to false, whereas strxfrm() caching only trusts the state
         * when cache_blob is found set to true.
         *
         * Arbitrarily initialize cache_blob to true.
         */
        sss->cache_blob = true;
        sss->collate_c = collate_c;
        sss->bpchar = bpchar;
        ssup->ssup_extra = sss;

        /*
         * If possible, plan to use the abbreviated keys optimization.  The
         * core code may switch back to authoritative comparator should
         * abbreviation be aborted.
         */
        if (abbreviate) {
            sss->prop_card = 0.20;
            initHyperLogLog(&sss->abbr_card, 10);
            sss->input_count = 0;
            sss->estimating = true;
            ssup->abbrev_full_comparator = ssup->comparator;
            ssup->comparator = varstrcmp_abbrev;
            ssup->abbrev_converter = varstr_abbrev_convert;
            ssup->abbrev_abort = varstr_abbrev_abort;
        }
    }
}

/*
 * sortsupport comparison func (for C locale case)
 */
static int varstrfastcmp_c(Datum x, Datum y, SortSupport ssup)
{
    text* arg1 = DatumGetTextPP(x);
    text* arg2 = DatumGetTextPP(y);
    char *a1p = NULL, *a2p = NULL;
    int len1, len2, result;

    a1p = VARDATA_ANY(arg1);
    a2p = VARDATA_ANY(arg2);

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    result = memcmp(a1p, a2p, Min(len1, len2));
    if ((result == 0) && (len1 != len2))
        result = (len1 < len2) ? -1 : 1;

    /* We can't afford to leak memory here. */
    if (PointerGetDatum(arg1) != x)
        pfree_ext(arg1);
    if (PointerGetDatum(arg2) != y)
        pfree_ext(arg2);

    return result;
}

/*
 * sortsupport comparison func (for C locale case)
 * for characher(n)
 */
static int bpcharfastcmp_c(Datum x, Datum y, SortSupport ssup)
{
    BpChar* arg1 = DatumGetBpCharPP(x);
    BpChar* arg2 = DatumGetBpCharPP(y);
    int len1, len2;
    int cmp;

    char* a1p = VARDATA_ANY(arg1);
    char* a2p = VARDATA_ANY(arg2);

    len1 = bpchartruelen(a1p, VARSIZE_ANY_EXHDR(arg1));
    len2 = bpchartruelen(a2p, VARSIZE_ANY_EXHDR(arg2));

    cmp = memcmp(a1p, a2p, Min(len1, len2));
    if ((cmp == 0) && (len1 != len2))
        cmp = (len1 < len2) ? -1 : 1;

    /* We can't afford to leak memory here. */
    if (PointerGetDatum(arg1) != x)
        pfree_ext(arg1);
    if (PointerGetDatum(arg2) != y)
        pfree_ext(arg2);

    return cmp;
}

/*
 * sortsupport comparison func (for locale case)
 */
static int varstrfastcmp_locale(Datum x, Datum y, SortSupport ssup)
{
    VarString* arg1 = DatumGetTextPP(x);
    VarString* arg2 = DatumGetTextPP(y);
    bool arg1_match = false;
    VarStringSortSupport* sss = (VarStringSortSupport*)ssup->ssup_extra;
    errno_t rc = EOK;

    /* working state */
    char *a1p = NULL, *a2p = NULL;
    int len1, len2, result;

    a1p = VARDATA_ANY(arg1);
    a2p = VARDATA_ANY(arg2);

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    /* Fast pre-check for equality, as discussed in varstr_cmp() */
    if (len1 == len2 && memcmp(a1p, a2p, len1) == 0) {
        /*
         * No change in buf1 or buf2 contents, so avoid changing last_len1 or
         * last_len2.  Existing contents of buffers might still be used by
         * next call.
         *
         * It's fine to allow the comparison of BpChar padding bytes here,
         * even though that implies that the memcmp() will usually be
         * performed for BpChar callers (though multibyte characters could
         * still prevent that from occurring).  The memcmp() is still very
         * cheap, and BpChar's funny semantics have us remove trailing spaces
         * (not limited to padding), so we need make no distinction between
         * padding space characters and "real" space characters.
         */
        result = 0;
        goto done;
    }

    if (sss->bpchar) {
        /* Get true number of bytes, ignoring trailing spaces */
        len1 = bpchartruelen(a1p, len1);
        len2 = bpchartruelen(a2p, len2);
    }

    if (len1 >= sss->buflen1) {
        pfree_ext(sss->buf1);
        sss->buflen1 = Max(len1 + 1, Min(sss->buflen1 * 2, (int)MaxAllocSize));
        sss->buf1 = (char*)MemoryContextAlloc(ssup->ssup_cxt, sss->buflen1);
    }
    if (len2 >= sss->buflen2) {
        pfree_ext(sss->buf2);
        sss->buflen2 = Max(len2 + 1, Min(sss->buflen2 * 2, (int)MaxAllocSize));
        sss->buf2 = (char*)MemoryContextAlloc(ssup->ssup_cxt, sss->buflen2);
    }

    /*
     * We're likely to be asked to compare the same strings repeatedly, and
     * memcmp() is so much cheaper than strcoll() that it pays to try to cache
     * comparisons, even though in general there is no reason to think that
     * that will work out (every string datum may be unique).  Caching does
     * not slow things down measurably when it doesn't work out, and can speed
     * things up by rather a lot when it does.  In part, this is because the
     * memcmp() compares data from cachelines that are needed in L1 cache even
     * when the last comparison's result cannot be reused.
     */
    arg1_match = true;
    if (len1 != sss->last_len1 || memcmp(sss->buf1, a1p, len1) != 0) {
        arg1_match = false;
        rc = memcpy_s(sss->buf1, sss->buflen1, a1p, len1);
        securec_check(rc, "\0", "\0");
        sss->buf1[len1] = '\0';
        sss->last_len1 = len1;
    }

    /*
     * If we're comparing the same two strings as last time, we can return the
     * same answer without calling strcoll() again.  This is more likely than
     * it seems (at least with moderate to low cardinality sets), because
     * quicksort compares the same pivot against many values.
     */
    if (len2 != sss->last_len2 || memcmp(sss->buf2, a2p, len2) != 0) {
        rc = memcpy_s(sss->buf2, sss->buflen2, a2p, len2);
        securec_check(rc, "\0", "\0");
        sss->buf2[len2] = '\0';
        sss->last_len2 = len2;
    } else if (arg1_match && !sss->cache_blob) {
        /* Use result cached following last actual strcoll() call */
        result = sss->last_returned;
        goto done;
    }

#ifdef HAVE_LOCALE_T
    if (sss->locale)
        result = strcoll_l(sss->buf1, sss->buf2, sss->locale);
    else
#endif
        result = strcoll(sss->buf1, sss->buf2);

    /*
     * In some locales strcoll() can claim that nonidentical strings are
     * equal. Believing that would be bad news for a number of reasons, so we
     * follow Perl's lead and sort "equal" strings according to strcmp().
     */
    if (result == 0)
        result = strcmp(sss->buf1, sss->buf2);

    /* Cache result, perhaps saving an expensive strcoll() call next time */
    sss->cache_blob = false;
    sss->last_returned = result;
done:
    /* We can't afford to leak memory here. */
    if (PointerGetDatum(arg1) != x)
        pfree_ext(arg1);
    if (PointerGetDatum(arg2) != y)
        pfree_ext(arg2);

    return result;
}

/*
 * Abbreviated key comparison func
 */
static int varstrcmp_abbrev(Datum x, Datum y, SortSupport ssup)
{
    /*
     * When 0 is returned, the core system will call varstrfastcmp_c()
     * (bpcharfastcmp_c() in BpChar case) or varstrfastcmp_locale().  Even a
     * strcmp() on two non-truncated strxfrm() blobs cannot indicate *equality*
     * authoritatively, for the same reason that there is a strcoll()
     * tie-breaker call to strcmp() in varstr_cmp().
     */
    if (x > y)
        return 1;
    else if (x == y)
        return 0;
    else
        return -1;
}

/*
 * Conversion routine for sortsupport.  Converts original to abbreviated key
 * representation.  Our encoding strategy is simple -- pack the first 8 bytes
 * of a strxfrm() blob into a Datum (on little-endian machines, the 8 bytes are
 * stored in reverse order), and treat it as an unsigned integer.  When the "C"
 * locale is used, or in case of bytea, just memcpy() from original instead.
 */
static Datum varstr_abbrev_convert(Datum original, SortSupport ssup)
{
    VarStringSortSupport* sss = (VarStringSortSupport*)ssup->ssup_extra;
    VarString* authoritative = DatumGetTextPP(original);
    char* authoritative_data = VARDATA_ANY(authoritative);

    /* working state */
    Datum res;
    char* pres = NULL;
    int len;
    uint32 hash;
    errno_t rc = EOK;

    /* input cout increase by one */
    sss->input_count++;

    pres = (char*)&res;
    /* memset(), so any non-overwritten bytes are NUL */
    rc = memset_s(pres, sizeof(Datum), 0, sizeof(Datum));
    securec_check(rc, "\0", "\0");
    len = VARSIZE_ANY_EXHDR(authoritative);

    /* Get number of bytes, ignoring trailing spaces */
    if (sss->bpchar)
        len = bpchartruelen(authoritative_data, len);

    /*
     * If we're using the C collation, use memcpy(), rather than strxfrm(), to
     * abbreviate keys.  The full comparator for the C locale is always
     * memcmp().  It would be incorrect to allow bytea callers (callers that
     * always force the C collation -- bytea isn't a collatable type, but this
     * approach is convenient) to use strxfrm().  This is because bytea
     * strings may contain NUL bytes.  Besides, this should be faster, too.
     *
     * More generally, it's okay that bytea callers can have NUL bytes in
     * strings because varstrcmp_abbrev() need not make a distinction between
     * terminating NUL bytes, and NUL bytes representing actual NULs in the
     * authoritative representation.  Hopefully a comparison at or past one
     * abbreviated key's terminating NUL byte will resolve the comparison
     * without consulting the authoritative representation; specifically, some
     * later non-NUL byte in the longer string can resolve the comparison
     * against a subsequent terminating NUL in the shorter string.  There will
     * usually be what is effectively a "length-wise" resolution there and
     * then.
     *
     * If that doesn't work out -- if all bytes in the longer string
     * positioned at or past the offset of the smaller string's (first)
     * terminating NUL are actually representative of NUL bytes in the
     * authoritative binary string (perhaps with some *terminating* NUL bytes
     * towards the end of the longer string iff it happens to still be small)
     * -- then an authoritative tie-breaker will happen, and do the right
     * thing: explicitly consider string length.
     */
    if (sss->collate_c) {
        rc = memcpy_s(pres, sizeof(Datum), authoritative_data, Min((Size)len, sizeof(Datum)));
        securec_check(rc, "\0", "\0");
    } else {
        Size bsize;

        /*
         * We're not using the C collation, so fall back on strxfrm.
         */

        /* By convention, we use buffer 1 to store and NUL-terminate */
        if (len >= sss->buflen1) {
            pfree_ext(sss->buf1);
            sss->buflen1 = Max(len + 1, Min(sss->buflen1 * 2, (int)MaxAllocSize));
            sss->buf1 = (char*)palloc(sss->buflen1);
        }

        /* Might be able to reuse strxfrm() blob from last call */
        if (sss->last_len1 == len && sss->cache_blob && memcmp(sss->buf1, authoritative_data, len) == 0) {
            rc = memcpy_s(pres, sizeof(Datum), sss->buf2, Min(sizeof(Datum), (Size)sss->last_len2));
            securec_check(rc, "\0", "\0");
            /* No change affecting cardinality, so no hashing required */
            goto done;
        }

        /* Just like strcoll(), strxfrm() expects a NUL-terminated string */
        rc = memcpy_s(sss->buf1, sss->buflen1, authoritative_data, len);
        securec_check(rc, "\0", "\0");
        sss->buf1[len] = '\0';
        sss->last_len1 = len;

        for (;;) {
#ifdef HAVE_LOCALE_T
            if (sss->locale)
                bsize = strxfrm_l(sss->buf2, sss->buf1, sss->buflen2, sss->locale);
            else
#endif
                bsize = strxfrm(sss->buf2, sss->buf1, sss->buflen2);

            sss->last_len2 = bsize;
            if ((int)bsize < sss->buflen2)
                break;

            /*
             * The C standard states that the contents of the buffer is now
             * unspecified.  Grow buffer, and retry.
             */
            pfree_ext(sss->buf2);
            sss->buflen2 = Max(bsize + 1, Min((Size)sss->buflen2 * 2, MaxAllocSize));
            sss->buf2 = (char*)palloc(sss->buflen2);
        }

        /*
         * Every Datum byte is always compared.  This is safe because the
         * strxfrm() blob is itself NUL terminated, leaving no danger of
         * misinterpreting any NUL bytes not intended to be interpreted as
         * logically representing termination.
         *
         * (Actually, even if there were NUL bytes in the blob it would be
         * okay.  See remarks on bytea case above.)
         */
        rc = memcpy_s(pres, sizeof(Datum), sss->buf2, Min(sizeof(Datum), bsize));
        securec_check(rc, "\0", "\0");
    }

    /*
     * Maintain approximate cardinality of both abbreviated keys and original,
     * authoritative keys using HyperLogLog.  Used as cheap insurance against
     * the worst case, where we do many string transformations for no saving
     * in full strcoll()-based comparisons.  These statistics are used by
     * varstr_abbrev_abort().
     *
     * First, Hash key proper, or a significant fraction of it.  Mix in length
     * in order to compensate for cases where differences are past
     * PG_CACHE_LINE_SIZE bytes, so as to limit the overhead of hashing.
     */

    /* Hash abbreviated key */
    if (sss->estimating) {
#if SIZEOF_DATUM == 8
        {
            uint32 lohalf, hihalf;

            lohalf = (uint32)res;
            hihalf = (uint32)(res >> 32);
            hash = DatumGetUInt32(hash_uint32(lohalf ^ hihalf));
        }
#else /* SIZEOF_DATUM != 8 */
        hash = DatumGetUInt32(hash_uint32((uint32)res));
#endif

        addHyperLogLog(&sss->abbr_card, hash);
    }
    /* Cache result, perhaps saving an expensive strxfrm() call next time */
    sss->cache_blob = true;
done:

    /*
     * Byteswap on little-endian machines.
     *
     * This is needed so that varstrcmp_abbrev() (an unsigned integer 3-way
     * comparator) works correctly on all platforms.  If we didn't do this,
     * the comparator would have to call memcmp() with a pair of pointers to
     * the first byte of each abbreviated key, which is slower.
     */
    res = DatumBigEndianToNative(res);

    /* Don't leak memory here */
    if (PointerGetDatum(authoritative) != original)
        pfree_ext(authoritative);

    return res;
}

/*
 * Callback for estimating effectiveness of abbreviated key optimization, using
 * heuristic rules.  Returns value indicating if the abbreviation optimization
 * should be aborted, based on its projected effectiveness.
 */
static bool varstr_abbrev_abort(int memtupcount, SortSupport ssup)
{
    VarStringSortSupport* sss = (VarStringSortSupport*)ssup->ssup_extra;
    double abbrev_distinct, key_distinct;

    Assert(ssup->abbreviate);

    /* Have a little patience */
    if (memtupcount < 100 || !sss->estimating)
        return false;

    abbrev_distinct = estimateHyperLogLog(&sss->abbr_card);
    key_distinct = sss->input_count;

    /*
     * If we have >100k distinct values, then even if we were sorting many
     * billion rows we'd likely still break even, and the penalty of undoing
     * that many rows of abbrevs would probably not be worth it. Stop even
     * counting at that point.
     * refers to numeric_abbrev_abort
     */
    if (abbrev_distinct > 100000.0) {
#ifdef TRACE_SORT
        if (u_sess->attr.attr_common.trace_sort)
            elog(LOG,
                "varstr_abbrev: estimation ends at cardinality %f"
                " after " INT64_FORMAT " values (%d rows)",
                abbrev_distinct,
                sss->input_count,
                memtupcount);
#endif
        sss->estimating = false;
        return false;
    }

    /*
     * Clamp cardinality estimates to at least one distinct value.  While
     * NULLs are generally disregarded, if only NULL values were seen so far,
     * that might misrepresent costs if we failed to clamp.
     */
    if (abbrev_distinct <= 1.0)
        abbrev_distinct = 1.0;

    if (key_distinct <= 1.0)
        key_distinct = 1.0;

        /*
         * In the worst case all abbreviated keys are identical, while at the same
         * time there are differences within full key strings not captured in
         * abbreviations.
         */
#ifdef TRACE_SORT
    if (u_sess->attr.attr_common.trace_sort) {
        double norm_abbrev_card = abbrev_distinct / (double)memtupcount;

        elog(LOG,
            "varstr_abbrev: abbrev_distinct after %d: %f "
            "(key_distinct: %f, norm_abbrev_card: %f, prop_card: %f)",
            memtupcount,
            abbrev_distinct,
            key_distinct,
            norm_abbrev_card,
            sss->prop_card);
    }
#endif

    /*
     * If the number of distinct abbreviated keys approximately matches the
     * number of distinct authoritative original keys, that's reason enough to
     * proceed.  We can win even with a very low cardinality set if most
     * tie-breakers only memcmp().  This is by far the most important
     * consideration.
     *
     * While comparisons that are resolved at the abbreviated key level are
     * considerably cheaper than tie-breakers resolved with memcmp(), both of
     * those two outcomes are so much cheaper than a full strcoll() once
     * sorting is underway that it doesn't seem worth it to weigh abbreviated
     * cardinality against the overall size of the set in order to more
     * accurately model costs.  Assume that an abbreviated comparison, and an
     * abbreviated comparison with a cheap memcmp()-based authoritative
     * resolution are equivalent.
     */
    if (abbrev_distinct > key_distinct * sss->prop_card) {
        /*
         * When we have exceeded 10,000 tuples, decay required cardinality
         * aggressively for next call.
         *
         * This is useful because the number of comparisons required on
         * average increases at a linearithmic rate, and at roughly 10,000
         * tuples that factor will start to dominate over the linear costs of
         * string transformation (this is a conservative estimate).  The decay
         * rate is chosen to be a little less aggressive than halving -- which
         * (since we're called at points at which memtupcount has doubled)
         * would never see the cost model actually abort past the first call
         * following a decay.  This decay rate is mostly a precaution against
         * a sudden, violent swing in how well abbreviated cardinality tracks
         * full key cardinality.  The decay also serves to prevent a marginal
         * case from being aborted too late, when too much has already been
         * invested in string transformation.
         *
         * It's possible for sets of several million distinct strings with
         * mere tens of thousands of distinct abbreviated keys to still
         * benefit very significantly.  This will generally occur provided
         * each abbreviated key is a proxy for a roughly uniform number of the
         * set's full keys. If it isn't so, we hope to catch that early and
         * abort.  If it isn't caught early, by the time the problem is
         * apparent it's probably not worth aborting.
         */
        if (memtupcount > 10000)
            sss->prop_card *= 0.65;

        return false;
    }

    /*
     * Abort abbreviation strategy.
     *
     * The worst case, where all abbreviated keys are identical while all
     * original strings differ will typically only see a regression of about
     * 10% in execution time for small to medium sized lists of strings.
     * Whereas on modern CPUs where cache stalls are the dominant cost, we can
     * often expect very large improvements, particularly with sets of strings
     * of moderately high to high abbreviated cardinality.  There is little to
     * lose but much to gain, which our strategy reflects.
     */
#ifdef TRACE_SORT
    if (u_sess->attr.attr_common.trace_sort)
        elog(LOG,
            "varstr_abbrev: aborted abbreviation at %d "
            "(abbrev_distinct: %f, key_distinct: %f, prop_card: %f)",
            memtupcount,
            abbrev_distinct,
            key_distinct,
            sss->prop_card);
#endif

    return true;
}

Datum text_larger(PG_FUNCTION_ARGS)
{
    text* arg1 = PG_GETARG_TEXT_PP(0);
    text* arg2 = PG_GETARG_TEXT_PP(1);
    FUNC_CHECK_HUGE_POINTER(false, arg1, "text_larger()");
    text* result = NULL;

    result = ((text_cmp(arg1, arg2, PG_GET_COLLATION()) > 0) ? arg1 : arg2);

    PG_RETURN_TEXT_P(result);
}

Datum text_smaller(PG_FUNCTION_ARGS)
{
    text* arg1 = PG_GETARG_TEXT_PP(0);
    text* arg2 = PG_GETARG_TEXT_PP(1);
    FUNC_CHECK_HUGE_POINTER(false, arg1, "text_smaller()");
    text* result = NULL;

    result = ((text_cmp(arg1, arg2, PG_GET_COLLATION()) < 0) ? arg1 : arg2);

    PG_RETURN_TEXT_P(result);
}

/*
 * The following operators support character-by-character comparison
 * of text datums, to allow building indexes suitable for LIKE clauses.
 * Note that the regular texteq/textne comparison operators are assumed
 * to be compatible with these!
 */

static int internal_text_pattern_compare(text* arg1, text* arg2)
{
    int result;
    int len1, len2;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

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

Datum text_pattern_lt(PG_FUNCTION_ARGS)
{
    text* arg1 = PG_GETARG_TEXT_PP(0);
    text* arg2 = PG_GETARG_TEXT_PP(1);

    FUNC_CHECK_HUGE_POINTER(false, arg1, "text_pattern()");
    int result;

    result = internal_text_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result < 0);
}

Datum text_pattern_le(PG_FUNCTION_ARGS)
{
    text* arg1 = PG_GETARG_TEXT_PP(0);
    text* arg2 = PG_GETARG_TEXT_PP(1);

    FUNC_CHECK_HUGE_POINTER(false, arg1, "text_pattern()");
    int result;

    result = internal_text_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result <= 0);
}

Datum text_pattern_ge(PG_FUNCTION_ARGS)
{
    text* arg1 = PG_GETARG_TEXT_PP(0);
    text* arg2 = PG_GETARG_TEXT_PP(1);
    FUNC_CHECK_HUGE_POINTER(false, arg1, "text_pattern()");

    int result;

    result = internal_text_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result >= 0);
}

Datum text_pattern_gt(PG_FUNCTION_ARGS)
{
    text* arg1 = PG_GETARG_TEXT_PP(0);
    text* arg2 = PG_GETARG_TEXT_PP(1);
    FUNC_CHECK_HUGE_POINTER(false, arg1, "text_pattern()");

    int result;

    result = internal_text_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result > 0);
}

Datum bttext_pattern_cmp(PG_FUNCTION_ARGS)
{
    text* arg1 = PG_GETARG_TEXT_PP(0);
    text* arg2 = PG_GETARG_TEXT_PP(1);
    FUNC_CHECK_HUGE_POINTER(false, arg1, "btext_pattern()");

    int result;

    result = internal_text_pattern_compare(arg1, arg2);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_INT32(result);
}

/* -------------------------------------------------------------
 * byteaoctetlen
 *
 * get the number of bytes contained in an instance of type 'bytea'
 * -------------------------------------------------------------
 */
Datum byteaoctetlen(PG_FUNCTION_ARGS)
{
    Datum str = PG_GETARG_DATUM(0);

    FUNC_CHECK_HUGE_POINTER(PG_ARGISNULL(0), PG_GETARG_TEXT_PP(0), "byteaoctetlen()");

    /* We need not detoast the input at all */
    PG_RETURN_INT32(toast_raw_datum_size(str) - VARHDRSZ);
}

/*
 * byteacat -
 *	  takes two bytea* and returns a bytea* that is the concatenation of
 *	  the two.
 *
 * Cloned from textcat and modified as required.
 */
Datum byteacat(PG_FUNCTION_ARGS)
{
    bytea* t1 = PG_GETARG_BYTEA_PP(0);
    bytea* t2 = PG_GETARG_BYTEA_PP(1);

    PG_RETURN_BYTEA_P(bytea_catenate(t1, t2));
}

/*
 * bytea_catenate
 *	Guts of byteacat(), broken out so it can be used by other functions
 *
 * Arguments can be in short-header form, but not compressed or out-of-line
 */
static bytea* bytea_catenate(bytea* t1, bytea* t2)
{
    bytea* result = NULL;
    int len1, len2, len;
    char* ptr = NULL;
    int rc = 0;

    if (VARATT_IS_HUGE_TOAST_POINTER(t1)) {
        struct varatt_lob_external large_toast_pointer;

        VARATT_EXTERNAL_GET_HUGE_POINTER(large_toast_pointer, t1);
        len1 = large_toast_pointer.va_rawsize;
    } else {
        len1 = VARSIZE_ANY_EXHDR(t1);
    }

    if (VARATT_IS_HUGE_TOAST_POINTER(t2)) {
        struct varatt_lob_external large_toast_pointer;

        VARATT_EXTERNAL_GET_HUGE_POINTER(large_toast_pointer, t2);
        len2 = large_toast_pointer.va_rawsize;
    } else {
        len2 = VARSIZE_ANY_EXHDR(t2);
    }

    /* paranoia ... probably should throw error instead? */
    if (len1 < 0)
        len1 = 0;
    if (len2 < 0)
        len2 = 0;

    len = len1 + len2 + VARHDRSZ;

    if (len > MAX_TOAST_CHUNK_SIZE + VARHDRSZ) {
#ifdef ENABLE_MULTIPLE_NODES
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Un-support clob/blob type more than 1GB for distributed system")));
#endif
        Oid toastOid = get_toast_oid();
        result = text_catenate_huge(t1, t2, toastOid);
    } else {
        result = (bytea*)palloc(len);

        /* Set size of result string... */
        SET_VARSIZE(result, len);

        /* Fill data field of result string... */
        ptr = VARDATA(result);
        if (len1 > 0) {
            rc = memcpy_s(ptr, len1, VARDATA_ANY(t1), len1);
            securec_check(rc, "\0", "\0");
        }
        if (len2 > 0) {
            rc = memcpy_s(ptr + len1, len2, VARDATA_ANY(t2), len2);
            securec_check(rc, "\0", "\0");
        }
    }
    return result;
}

#define PG_STR_GET_BYTEA(str_) DatumGetByteaP(DirectFunctionCall1(byteain, CStringGetDatum(str_)))

/*
 * bytea_substr()
 * Return a substring starting at the specified position.
 * Cloned from text_substr and modified as required.
 *
 * Input:
 *	- string
 *	- starting position (is one-based)
 *	- string length (optional)
 *
 * If the starting position is zero or less, then return from the start of the string
 * adjusting the length to be consistent with the "negative start" per SQL92.
 * If the length is less than zero, an ERROR is thrown. If no third argument
 * (length) is provided, the length to the end of the string is assumed.
 */
Datum bytea_substr(PG_FUNCTION_ARGS)
{
    PG_RETURN_BYTEA_P(bytea_substring(PG_GETARG_DATUM(0), PG_GETARG_INT32(1), PG_GETARG_INT32(2), false));
}

/*
 * bytea_substr_no_len -
 *	  Wrapper to avoid opr_sanity failure due to
 *	  one function accepting a different number of args.
 */
Datum bytea_substr_no_len(PG_FUNCTION_ARGS)
{
    PG_RETURN_BYTEA_P(bytea_substring(PG_GETARG_DATUM(0), PG_GETARG_INT32(1), -1, true));
}

static bytea* bytea_substring(Datum str, int S, int L, bool length_not_specified)
{
    int S1; /* adjusted start position */
    int L1; /* adjusted substring length */

    S1 = Max(S, 1);

    if (length_not_specified) {
        /*
         * Not passed a length - DatumGetByteaPSlice() grabs everything to the
         * end of the string if we pass it a negative value for length.
         */
        L1 = -1;
    } else {
        /* end position */
        int E = S + L;

        /*
         * A negative value for L is the only way for the end position to be
         * before the start. SQL99 says to throw an error.
         */
        if (E < S)
            ereport(ERROR, (errcode(ERRCODE_SUBSTRING_ERROR), errmsg("negative substring length not allowed")));

        /*
         * A zero or negative value for the end position can happen if the
         * start was negative or one. SQL99 says to return a zero-length
         * string.
         */
        if (E < 1)
            return PG_STR_GET_BYTEA("");

        L1 = E - S1;
    }

    /*
     * If the start position is past the end of the string, SQL99 says to
     * return a zero-length string -- DatumGetByteaPSlice() will do that for
     * us. Convert to zero-based starting position
     */
    return DatumGetByteaPSlice(str, S1 - 1, L1);
}

// adapt A db's substr(bytea str,integer start,integer length)
// when start<0, amend the sartPosition to abs(start) from last char,
// then search backward
Datum bytea_substr_orclcompat(PG_FUNCTION_ARGS)
{
    bytea* result = NULL;
    Datum str = PG_GETARG_DATUM(0);
    int32 start = PG_GETARG_INT32(1);
    int32 length = PG_GETARG_INT32(2);

    int32 total = 0;

    total = toast_raw_datum_size(str) - VARHDRSZ;
    if ((length < 0) || (start > total) || (start + total < 0)) {
        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT ||
            u_sess->attr.attr_sql.sql_compatibility == B_FORMAT)
            PG_RETURN_NULL();
        else {
            result = PG_STR_GET_BYTEA("");
            PG_RETURN_BYTEA_P(result);
        }
    }
    /*
     * the param length_not_specified is false,
     * the param length is used
     */
    result = bytea_substring_orclcompat(str, start, length, false);

    if ((NULL == result || 0 == VARSIZE_ANY_EXHDR(result)) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
        PG_RETURN_NULL();
    else
        PG_RETURN_BYTEA_P(result);
}

// adapt A db's substr(bytea x,integer y)
// when start<0, amend the sartPosition to abs(start) from last char,
// then search backward
Datum bytea_substr_no_len_orclcompat(PG_FUNCTION_ARGS)
{
    bytea* result = NULL;
    Datum str = PG_GETARG_DATUM(0);
    int32 start = PG_GETARG_INT32(1);
    int32 total = 0;

    total = toast_raw_datum_size(str) - VARHDRSZ;
    if ((start > total) || (start + total < 0)) {
        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
            PG_RETURN_NULL();
        else {
            result = PG_STR_GET_BYTEA("");
            PG_RETURN_BYTEA_P(result);
        }
    }
    /*
     * the param length_not_specified is true,
     * the param length is not used, and the length is set -1 as invalid flag
     */
    result = bytea_substring_orclcompat(str, start, -1, true);

    if ((NULL == result || 0 == VARSIZE_ANY_EXHDR(result)) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
        PG_RETURN_NULL();
    else
        PG_RETURN_BYTEA_P(result);
}

// Does the real work for bytea_substr_orclcompat() and bytea_substr_no_len_orclcompat().
// The result is always a freshly palloc'd datum.
// when length_not_specified==false, the param length is not used,
// usually called by text_substr_no_len_orclcompat().
// when length_not_specified==true, the param length is not used,
// sually called by text_substr_orclcompat()
static bytea* bytea_substring_orclcompat(Datum str, int S, int L, bool length_not_specified)
{
    int32 S1 = 0; /* adjusted start position */
    int32 L1 = 0; /* adjusted substring length */
    int32 total = 0;

    total = toast_raw_datum_size(str) - VARHDRSZ;

    /*
     * amend the start position. when S < 0,
     * amend the sartPosition to abs(start) from last char,
     * when s==0, the start position is set 1
     */
    if (S < 0) {
        S = total + S + 1;
    } else if (0 == S) {
        S = 1;
    }

    S1 = Max(S, 1);
    /* length_not_specified==true, the param length is not used */
    if (length_not_specified) {
        /*
         * Not passed a length - DatumGetByteaPSlice() grabs everything to the
         * end of the string if we pass it a negative value for length.
         */
        L1 = -1;
    }

    /* length_not_specified==false, the param length is used */
    else {
        /* end position */
        int E = S + L;

        /*
         * A negative value for L is the only way for the end position to be
         * before the start. SQL99 says to throw an error.
         */
        if (E < S)
            ereport(ERROR, (errcode(ERRCODE_SUBSTRING_ERROR), errmsg("negative substring length not allowed")));

        /*
         * A zero or negative value for the end position can happen if the
         * start was negative or one. SQL99 says to return a zero-length
         * string.
         */
        if (E < 1)
            return PG_STR_GET_BYTEA("");

        L1 = E - S1;
    }

    /*
     * If the start position is past the end of the string, SQL99 says to
     * return a zero-length string -- DatumGetByteaPSlice() will do that for
     * us. Convert to zero-based starting position
     */
    return DatumGetByteaPSlice(str, S1 - 1, L1);
}

/*
 * byteaoverlay
 *	Replace specified substring of first string with second
 *
 * The SQL standard defines OVERLAY() in terms of substring and concatenation.
 * This code is a direct implementation of what the standard says.
 */
Datum byteaoverlay(PG_FUNCTION_ARGS)
{
    bytea* t1 = PG_GETARG_BYTEA_PP(0);
    bytea* t2 = PG_GETARG_BYTEA_PP(1);
    int sp = PG_GETARG_INT32(2); /* substring start position */
    int sl = PG_GETARG_INT32(3); /* substring length */

    PG_RETURN_BYTEA_P(bytea_overlay(t1, t2, sp, sl));
}

Datum byteaoverlay_no_len(PG_FUNCTION_ARGS)
{
    bytea* t1 = PG_GETARG_BYTEA_PP(0);
    bytea* t2 = PG_GETARG_BYTEA_PP(1);
    int sp = PG_GETARG_INT32(2); /* substring start position */
    int sl;

    sl = VARSIZE_ANY_EXHDR(t2); /* defaults to length(t2) */
    PG_RETURN_BYTEA_P(bytea_overlay(t1, t2, sp, sl));
}

static bytea* bytea_overlay(bytea* t1, bytea* t2, int sp, int sl)
{
    bytea* result = NULL;
    bytea* s1 = NULL;
    bytea* s2 = NULL;
    int sp_pl_sl;

    /*
     * Check for possible integer-overflow cases.  For negative sp, throw a
     * "substring length" error because that's what should be expected
     * according to the spec's definition of OVERLAY().
     */
    if (sp <= 0)
        ereport(ERROR, (errcode(ERRCODE_SUBSTRING_ERROR), errmsg("negative substring length not allowed")));
    if (pg_add_s32_overflow(sp, sl, &sp_pl_sl))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("integer out of range")));

    s1 = bytea_substring(PointerGetDatum(t1), 1, sp - 1, false);
    s2 = bytea_substring(PointerGetDatum(t1), sp_pl_sl, -1, true);
    result = bytea_catenate(s1, t2);
    result = bytea_catenate(result, s2);

    return result;
}

/*
 * byteapos -
 *	  Return the position of the specified substring.
 *	  Implements the SQL92 POSITION() function.
 * Cloned from textpos and modified as required.
 */
Datum byteapos(PG_FUNCTION_ARGS)
{
    bytea* t1 = PG_GETARG_BYTEA_PP(0);
    bytea* t2 = PG_GETARG_BYTEA_PP(1);
    int pos;
    int px, p;
    int len1, len2;
    char *p1 = NULL, *p2 = NULL;

    len1 = VARSIZE_ANY_EXHDR(t1);
    len2 = VARSIZE_ANY_EXHDR(t2);

    if (len2 <= 0)
        PG_RETURN_INT32(1); /* result for empty pattern */

    p1 = VARDATA_ANY(t1);
    p2 = VARDATA_ANY(t2);

    pos = 0;
    px = (len1 - len2);
    for (p = 0; p <= px; p++) {
        if ((*p2 == *p1) && (memcmp(p1, p2, len2) == 0)) {
            pos = p + 1;
            break;
        }
        p1++;
    }

    PG_RETURN_INT32(pos);
}

/* -------------------------------------------------------------
 * byteaGetByte
 *
 * this routine treats "bytea" as an array of bytes.
 * It returns the Nth byte (a number between 0 and 255).
 * -------------------------------------------------------------
 */
Datum byteaGetByte(PG_FUNCTION_ARGS)
{
    bytea* v = PG_GETARG_BYTEA_PP(0);
    int32 n = PG_GETARG_INT32(1);
    int len;
    int byte;

    len = VARSIZE_ANY_EXHDR(v);

    if (n < 0 || n >= len)
        ereport(
            ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("index %d out of valid range, 0..%d", n, len - 1)));

    byte = ((unsigned char*)VARDATA_ANY(v))[n];

    PG_RETURN_INT32(byte);
}

/* -------------------------------------------------------------
 * byteaGetBit
 *
 * This routine treats a "bytea" type like an array of bits.
 * It returns the value of the Nth bit (0 or 1).
 *
 * -------------------------------------------------------------
 */
Datum byteaGetBit(PG_FUNCTION_ARGS)
{
    bytea* v = PG_GETARG_BYTEA_PP(0);
    int32 n = PG_GETARG_INT32(1);
    int byteNo, bitNo;
    int len;
    int byte;

    len = VARSIZE_ANY_EXHDR(v);

    if (n < 0 || n >= len * 8)
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("index %d out of valid range, 0..%d", n, len * 8 - 1)));

    byteNo = n / 8;
    bitNo = n % 8;

    byte = ((unsigned char*)VARDATA_ANY(v))[byteNo];

    if ((unsigned int)byte & (unsigned int)(1 << bitNo))
        PG_RETURN_INT32(1);
    else
        PG_RETURN_INT32(0);
}

/* -------------------------------------------------------------
 * byteaSetByte
 *
 * Given an instance of type 'bytea' creates a new one with
 * the Nth byte set to the given value.
 *
 * -------------------------------------------------------------
 */
Datum byteaSetByte(PG_FUNCTION_ARGS)
{
    bytea* v = PG_GETARG_BYTEA_P(0);
    int32 n = PG_GETARG_INT32(1);
    int32 newByte = PG_GETARG_INT32(2);
    int len;
    bytea* res = NULL;
    errno_t rc = 0;

    len = VARSIZE(v) - VARHDRSZ;

    if (n < 0 || n >= len)
        ereport(
            ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("index %d out of valid range, 0..%d", n, len - 1)));

    /*
     * Make a copy of the original varlena.
     */
    res = (bytea*)palloc(VARSIZE(v));
    rc = memcpy_s((char*)res, VARSIZE(v), (char*)v, VARSIZE(v));
    securec_check(rc, "\0", "\0");

    /*
     * Now set the byte.
     */
    ((unsigned char*)VARDATA(res))[n] = newByte;

    PG_RETURN_BYTEA_P(res);
}

/* -------------------------------------------------------------
 * byteaSetBit
 *
 * Given an instance of type 'bytea' creates a new one with
 * the Nth bit set to the given value.
 *
 * -------------------------------------------------------------
 */
Datum byteaSetBit(PG_FUNCTION_ARGS)
{
    bytea* v = PG_GETARG_BYTEA_P(0);
    int32 n = PG_GETARG_INT32(1);
    int32 newBit = PG_GETARG_INT32(2);
    bytea* res = NULL;
    int len;
    int oldByte, newByte;
    int byteNo, bitNo;

    len = VARSIZE(v) - VARHDRSZ;

    if (n < 0 || n >= len * 8)
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("index %d out of valid range, 0..%d", n, len * 8 - 1)));

    byteNo = n / 8;
    bitNo = n % 8;

    /*
     * sanity check!
     */
    if (newBit != 0 && newBit != 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("new bit must be 0 or 1")));

    /*
     * Make a copy of the original varlena.
     */
    res = (bytea*)palloc(VARSIZE(v));
    MemCpy((char*)res, (char*)v, VARSIZE(v));

    /*
     * Update the byte.
     */
    oldByte = ((unsigned char*)VARDATA(res))[byteNo];

    if (newBit == 0)
        newByte = (unsigned int)oldByte & (unsigned int)(~(1 << bitNo));
    else
        newByte = (unsigned int)oldByte | (unsigned int)(1 << bitNo);

    ((unsigned char*)VARDATA(res))[byteNo] = newByte;

    PG_RETURN_BYTEA_P(res);
}

/* text_name()
 * Converts a text type to a Name type.
 */
Datum text_name(PG_FUNCTION_ARGS)
{
    text* s = PG_GETARG_TEXT_PP(0);
    Name result;
    int len;

    len = VARSIZE_ANY_EXHDR(s);

    /* Truncate oversize input */
    if (len >= NAMEDATALEN)
        len = pg_mbcliplen(VARDATA_ANY(s), len, NAMEDATALEN - 1);

    /* We use palloc0 here to ensure result is zero-padded */
    result = (Name)palloc0(NAMEDATALEN);
    MemCpy(NameStr(*result), VARDATA_ANY(s), len);

    PG_RETURN_NAME(result);
}

/* name_text()
 * Converts a Name type to a text type.
 */
Datum name_text(PG_FUNCTION_ARGS)
{
    Name s = PG_GETARG_NAME(0);

    PG_RETURN_TEXT_P(cstring_to_text(NameStr(*s)));
}

/*
 * textToQualifiedNameList - convert a text object to list of names
 *
 * This implements the input parsing needed by nextval() and other
 * functions that take a text parameter representing a qualified name.
 * We split the name at dots, downcase if not double-quoted, and
 * truncate names if they're too long.
 */
List* textToQualifiedNameList(text* textval)
{
    char* rawname = NULL;
    List* result = NIL;
    List* namelist = NIL;
    ListCell* l = NULL;

    /* Convert to C string (handles possible detoasting). */
    /* Note we rely on being able to modify rawname below. */
    rawname = text_to_cstring(textval);

    if (!SplitIdentifierString(rawname, '.', &namelist))
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid name syntax")));

    if (namelist == NIL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid name syntax")));

    foreach (l, namelist) {
        char* curname = (char*)lfirst(l);

        result = lappend(result, makeString(pstrdup(curname)));
    }

    pfree_ext(rawname);
    list_free_ext(namelist);

    return result;
}

/*
 * SplitIdentifierString --- parse a string containing identifiers
 *
 * This is the guts of textToQualifiedNameList, and is exported for use in
 * other situations such as parsing GUC variables.	In the GUC case, it's
 * important to avoid memory leaks, so the API is designed to minimize the
 * amount of stuff that needs to be allocated and freed.
 *
 * Inputs:
 *	rawstring: the input string; must be overwritable!	On return, it's
 *			   been modified to contain the separated identifiers.
 *	separator: the separator punctuation expected between identifiers
 *			   (typically '.' or ',').	Whitespace may also appear around
 *			   identifiers.
 * Outputs:
 *	namelist: filled with a palloc'd list of pointers to identifiers within
 *			  rawstring.  Caller should list_free_ext() this even on error return.
 *
 * Returns TRUE if okay, FALSE if there is a syntax error in the string.
 *
 * Note that an empty string is considered okay here, though not in
 * textToQualifiedNameList.
 */
bool SplitIdentifierString(char* rawstring, char separator, List** namelist, bool downCase, bool truncateToolong)
{
    char* nextp = rawstring;
    bool done = false;
    errno_t ss_rc = 0;

    *namelist = NIL;

    while (isspace((unsigned char)*nextp))
        nextp++; /* skip leading whitespace */

    if (*nextp == '\0')
        return true; /* allow empty string */

    /* At the top of the loop, we are at start of a new identifier. */
    char* curname = NULL;
    char* endp = NULL;
    char* downname = NULL;
    do {
        if (*nextp == '\"') {
            /* Quoted name --- collapse quote-quote pairs, no downcasing */
            curname = nextp + 1;
            for (;;) {
                endp = strchr(nextp + 1, '\"');
                if (endp == NULL)
                    return false; /* mismatched quotes */
                if (endp[1] != '\"')
                    break; /* found end of quoted name */
                /* Collapse adjacent quotes into one quote, and look again */
                if (strlen(endp) > 0) {
                    ss_rc = memmove_s(endp, strlen(endp), endp + 1, strlen(endp));
                    securec_check(ss_rc, "\0", "\0");
                }
                nextp = endp;
            }
            /* endp now points at the terminating quote */
            nextp = endp + 1;
        } else {
            /* Unquoted name --- extends to separator or whitespace */
            int len;

            curname = nextp;
            while (*nextp && *nextp != separator && !isspace((unsigned char)*nextp))
                nextp++;
            endp = nextp;
            if (curname == nextp)
                return false; /* empty unquoted name not allowed */

            /*
             * Downcase the identifier, using same code as main lexer does.
             *
             * XXX because we want to overwrite the input in-place, we cannot
             * support a downcasing transformation that increases the string
             * length.	This is not a problem given the current implementation
             * of downcase_truncate_identifier, but we'll probably have to do
             * something about this someday.
             */
            len = endp - curname;

            /*
             * If downCase is false need not to convert to lowercase when this function is called by
             * get_typeoid_with_namespace, because this curname is come from datanode's system table, if we will it
             * alter to lower that will this schema or columnName can not be found.
             */
            if (downCase) {
                downname = downcase_truncate_identifier(curname, len, false);

                Assert(strlen(downname) <= (unsigned int)(len));
                strncpy(curname, downname, len);
                pfree_ext(downname);
            }
        }

        while (isspace((unsigned char)*nextp))
            nextp++; /* skip trailing whitespace */

        if (*nextp == separator) {
            nextp++;
            while (isspace((unsigned char)*nextp))
                nextp++; /* skip leading whitespace for next */
                         /* we expect another name, so done remains false */
        } else if (*nextp == '\0')
            done = true;
        else
            return false; /* invalid syntax */

        /* Now safe to overwrite separator with a null */
        *endp = '\0';
        if (truncateToolong == true) {
            /* Truncate name if it's overlength */
            truncate_identifier(curname, strlen(curname), false);
        }
        /*
         * Finished isolating current name --- add it to list
         */
        *namelist = lappend(*namelist, curname);

        /* Loop back if we didn't reach end of string */
    } while (!done);

    return true;
}

bool SplitIdentifierInteger(char* rawstring, char separator, List** namelist)
{
    const int LEN = 2;
    char* nextp = rawstring;
    bool done = false;

    *namelist = NIL;

    while (isspace((unsigned char)*nextp))
        nextp++; /* skip leading whitespace */

    if (*nextp == '\0')
        return true; /* allow empty string */

    /* At the top of the loop, we are at start of a new identifier. */
    char* curname = NULL;
    char* endp = NULL;
    do {
        /* extends to separator or whitespace */
        int len;

        curname = nextp;
        while (*nextp && *nextp != separator && !isspace((unsigned char)*nextp))
            nextp++;
        endp = nextp;
        if (curname == nextp)
            return false; /* empty unquoted name not allowed */

        len = endp - curname;

        if (len > LEN)
            return false;

        for (int i = 0; i < len; i++) {
            unsigned char ch = (unsigned char)curname[i];
            if (ch < '0' || ch > '9')
                return false;
        }

        while (isspace((unsigned char)*nextp))
            nextp++; /* skip trailing whitespace */

        if (*nextp == separator) {
            nextp++;
            while (isspace((unsigned char)*nextp))
                nextp++; /* skip leading whitespace for next */
                         /* we expect another name, so done remains false */
        } else if (*nextp == '\0')
            done = true;
        else
            return false; /* invalid syntax */

        /* Now safe to overwrite separator with a null */
        *endp = '\0';

        /*
         * Finished isolating current name --- add it to list
         */
        *namelist = lappend(*namelist, curname);

        /* Loop back if we didn't reach end of string */
    } while (!done);

    return true;
}

/*****************************************************************************
 *	Comparison Functions used for bytea
 *
 * Note: btree indexes need these routines not to leak memory; therefore,
 * be careful to free working copies of toasted datums.  Most places don't
 * need to be so careful.
 *****************************************************************************/

Datum byteaeq(PG_FUNCTION_ARGS)
{
    Datum arg1 = PG_GETARG_DATUM(0);
    Datum arg2 = PG_GETARG_DATUM(1);
    bool result = false;
    Size len1, len2;
    /*
     * We can use a fast path for unequal lengths, which might save us from
     * having to detoast one or both values.
     */
    len1 = toast_raw_datum_size(arg1);
    len2 = toast_raw_datum_size(arg2);
    if (len1 != len2)
        result = false;
    else {
        bytea* barg1 = DatumGetByteaPP(arg1);
        bytea* barg2 = DatumGetByteaPP(arg2);

        result = (memcmp(VARDATA_ANY(barg1), VARDATA_ANY(barg2), len1 - VARHDRSZ) == 0);

        PG_FREE_IF_COPY(barg1, 0);
        PG_FREE_IF_COPY(barg2, 1);
    }

    PG_RETURN_BOOL(result);
}

Datum byteane(PG_FUNCTION_ARGS)
{
    Datum arg1 = PG_GETARG_DATUM(0);
    Datum arg2 = PG_GETARG_DATUM(1);
    bool result = false;
    Size len1, len2;

    FUNC_CHECK_HUGE_POINTER(PG_ARGISNULL(0), DatumGetPointer(arg1), "byteane()");

    /*
     * We can use a fast path for unequal lengths, which might save us from
     * having to detoast one or both values.
     */
    len1 = toast_raw_datum_size(arg1);
    len2 = toast_raw_datum_size(arg2);
    if (len1 != len2)
        result = true;
    else {
        bytea* barg1 = DatumGetByteaPP(arg1);
        bytea* barg2 = DatumGetByteaPP(arg2);

        result = (memcmp(VARDATA_ANY(barg1), VARDATA_ANY(barg2), len1 - VARHDRSZ) != 0);

        PG_FREE_IF_COPY(barg1, 0);
        PG_FREE_IF_COPY(barg2, 1);
    }

    PG_RETURN_BOOL(result);
}

Datum bytealt(PG_FUNCTION_ARGS)
{
    bytea* arg1 = PG_GETARG_BYTEA_PP(0);
    bytea* arg2 = PG_GETARG_BYTEA_PP(1);

    FUNC_CHECK_HUGE_POINTER(false, arg1, "bytealt()");
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp < 0) || ((cmp == 0) && (len1 < len2)));
}

Datum byteale(PG_FUNCTION_ARGS)
{
    bytea* arg1 = PG_GETARG_BYTEA_PP(0);
    bytea* arg2 = PG_GETARG_BYTEA_PP(1);
    int len1, len2;
    int cmp;

    FUNC_CHECK_HUGE_POINTER(false, arg1, "bytealt()");

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp < 0) || ((cmp == 0) && (len1 <= len2)));
}

Datum byteagt(PG_FUNCTION_ARGS)
{
    bytea* arg1 = PG_GETARG_BYTEA_PP(0);
    bytea* arg2 = PG_GETARG_BYTEA_PP(1);
    int len1, len2;
    int cmp;
    FUNC_CHECK_HUGE_POINTER(false, arg1, "byteagt()");

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp > 0) || ((cmp == 0) && (len1 > len2)));
}

Datum byteage(PG_FUNCTION_ARGS)
{
    bytea* arg1 = PG_GETARG_BYTEA_PP(0);
    bytea* arg2 = PG_GETARG_BYTEA_PP(1);
    int len1, len2;
    int cmp;
    FUNC_CHECK_HUGE_POINTER(false, arg1, "bytealt()");

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp > 0) || ((cmp == 0) && (len1 >= len2)));
}

Datum byteacmp(PG_FUNCTION_ARGS)
{
    bytea* arg1 = PG_GETARG_BYTEA_PP(0);
    bytea* arg2 = PG_GETARG_BYTEA_PP(1);
    FUNC_CHECK_HUGE_POINTER(false, arg1, "byteacmp()");
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
    if ((cmp == 0) && (len1 != len2))
        cmp = (len1 < len2) ? -1 : 1;

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_INT32(cmp);
}

Datum bytea_sortsupport(PG_FUNCTION_ARGS)
{
    SortSupport ssup = (SortSupport)PG_GETARG_POINTER(0);
    MemoryContext oldcontext;

    oldcontext = MemoryContextSwitchTo(ssup->ssup_cxt);

    /* Use generic string SortSupport, forcing "C" collation */
    varstr_sortsupport(ssup, C_COLLATION_OID, false);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_VOID();
}

Datum raweq(PG_FUNCTION_ARGS)
{
    bytea* arg1 = PG_GETARG_BYTEA_PP(0);
    bytea* arg2 = PG_GETARG_BYTEA_PP(1);
    int len1, len2;
    bool result = false;

    FUNC_CHECK_HUGE_POINTER(PG_ARGISNULL(0), arg1, "raweq()");

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    /* fast path for different-length inputs */
    if (len1 != len2)
        result = false;
    else
        result = (memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), len1) == 0);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result);
}

Datum rawne(PG_FUNCTION_ARGS)
{
    bytea* arg1 = PG_GETARG_BYTEA_PP(0);
    bytea* arg2 = PG_GETARG_BYTEA_PP(1);
    int len1, len2;
    bool result = false;
    FUNC_CHECK_HUGE_POINTER(false, arg1, "rawne()");

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    /* fast path for different-length inputs */
    if (len1 != len2)
        result = true;
    else
        result = (memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), len1) != 0);

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL(result);
}

Datum rawlt(PG_FUNCTION_ARGS)
{
    bytea* arg1 = PG_GETARG_BYTEA_PP(0);
    bytea* arg2 = PG_GETARG_BYTEA_PP(1);
    int len1, len2;
    int cmp;
    FUNC_CHECK_HUGE_POINTER(false, arg1, "rawlt()");

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp < 0) || ((cmp == 0) && (len1 < len2)));
}

Datum rawle(PG_FUNCTION_ARGS)
{
    bytea* arg1 = PG_GETARG_BYTEA_PP(0);
    bytea* arg2 = PG_GETARG_BYTEA_PP(1);
    int len1, len2;
    int cmp;
    FUNC_CHECK_HUGE_POINTER(false, arg1, "rawle()");

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp < 0) || ((cmp == 0) && (len1 <= len2)));
}

Datum rawgt(PG_FUNCTION_ARGS)
{
    bytea* arg1 = PG_GETARG_BYTEA_PP(0);
    bytea* arg2 = PG_GETARG_BYTEA_PP(1);
    int len1, len2;
    int cmp;
    FUNC_CHECK_HUGE_POINTER(false, arg1, "rawgt()");

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp > 0) || ((cmp == 0) && (len1 > len2)));
}

Datum rawge(PG_FUNCTION_ARGS)
{
    bytea* arg1 = PG_GETARG_BYTEA_PP(0);
    bytea* arg2 = PG_GETARG_BYTEA_PP(1);
    int len1, len2;
    int cmp;
    FUNC_CHECK_HUGE_POINTER(false, arg1, "rawge()");

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp > 0) || ((cmp == 0) && (len1 >= len2)));
}

Datum rawcmp(PG_FUNCTION_ARGS)
{
    bytea* arg1 = PG_GETARG_BYTEA_PP(0);
    bytea* arg2 = PG_GETARG_BYTEA_PP(1);
    int len1, len2;
    int cmp;
    FUNC_CHECK_HUGE_POINTER(false, arg1, "rawcmp()");

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
    if ((cmp == 0) && (len1 != len2))
        cmp = (len1 < len2) ? -1 : 1;

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_INT32(cmp);
}

Datum rawcat(PG_FUNCTION_ARGS)
{
    bytea* t1 = PG_GETARG_BYTEA_PP(0);
    bytea* t2 = PG_GETARG_BYTEA_PP(1);
    int len1, len2, len;
    bytea* result = NULL;
    char* ptr = NULL;
    errno_t rc = EOK;

    len1 = VARSIZE_ANY_EXHDR(t1);
    if (len1 < 0)
        len1 = 0;

    len2 = VARSIZE_ANY_EXHDR(t2);
    if (len2 < 0)
        len2 = 0;

    len = len1 + len2 + VARHDRSZ;
    result = (bytea*)palloc(len);

    /* Set size of result string*/
    SET_VARSIZE(result, len);

    /* Fill data field of result string*/
    ptr = VARDATA(result);
    if (len1 > 0) {
        rc = memcpy_s(ptr, len1, VARDATA_ANY(t1), len1);
        securec_check(rc, "\0", "\0");
    }
    if (len2 > 0) {
        rc = memcpy_s(ptr + len1, len2, VARDATA_ANY(t2), len2);
        securec_check(rc, "\0", "\0");
    }

    PG_RETURN_BYTEA_P(result);
}

/*
 * appendStringInfoText
 *
 * Append a text to str.
 * Like appendStringInfoString(str, text_to_cstring(t)) but faster.
 */
static void appendStringInfoText(StringInfo str, const text* t)
{
    appendBinaryStringInfo(str, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
}

/*
 * replace_text_with_two_args
 * replace all occurrences of 'old_sub_str' in 'orig_str'
 * with '' to form 'new_str'
 *
 * the effect is equivalent to
 *
 * delete all occurrences of 'old_sub_str' in 'orig_str'
 * to form 'new_str'
 *
 * returns 'orig_str' if 'old_sub_str' == '' or 'orig_str' == ''
 * otherwise returns 'new_str'
 */
Datum replace_text_with_two_args(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    if (PG_ARGISNULL(1))
        PG_RETURN_TEXT_P(PG_GETARG_TEXT_PP(0));

    return DirectFunctionCall3(replace_text,
        PG_GETARG_DATUM(0),
        PG_GETARG_DATUM(1),
        CStringGetTextDatum("\0"));
}

/*
 * replace_text
 * replace all occurrences of 'old_sub_str' in 'orig_str'
 * with 'new_sub_str' to form 'new_str'
 *
 * returns 'orig_str' if 'old_sub_str' == '' or 'orig_str' == ''
 * otherwise returns 'new_str'
 */
Datum replace_text(PG_FUNCTION_ARGS)
{
    text* src_text = NULL;
    text* from_sub_text = NULL;
    text* to_sub_text = NULL;
    int src_text_len;
    int from_sub_text_len;
    TextPositionState state;
    text* ret_text = NULL;
    int start_posn;
    int curr_posn;
    int chunk_len;
    char* start_ptr = NULL;
    StringInfoData str;

    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    src_text = PG_GETARG_TEXT_PP(0);

    if (PG_ARGISNULL(1))
        PG_RETURN_TEXT_P(src_text);

    from_sub_text = PG_GETARG_TEXT_PP(1);

    if (!PG_ARGISNULL(2))
        to_sub_text = PG_GETARG_TEXT_PP(2);

    if (VARATT_IS_HUGE_TOAST_POINTER(src_text) || VARATT_IS_HUGE_TOAST_POINTER(from_sub_text)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("replace() arguments cannot exceed 1GB")));
    }

    text_position_setup(src_text, from_sub_text, &state);

    /*
     * Note: we check the converted string length, not the original, because
     * they could be different if the input contained invalid encoding.
     */
    src_text_len = state.len1;
    from_sub_text_len = state.len2;

    /* Return unmodified source string if empty source or pattern */
    if (src_text_len < 1 || from_sub_text_len < 1) {
        text_position_cleanup(&state);
        PG_RETURN_TEXT_P(src_text);
    }

    start_posn = 1;
    curr_posn = text_position_next(1, &state);

    /* When the from_sub_text is not found, there is nothing to do. */
    if (curr_posn == 0) {
        text_position_cleanup(&state);
        PG_RETURN_TEXT_P(src_text);
    }

    /* start_ptr points to the start_posn'th character of src_text */
    start_ptr = VARDATA_ANY(src_text);

    initStringInfo(&str);

    do {
        CHECK_FOR_INTERRUPTS();

        /* copy the data skipped over by last text_position_next() */
        chunk_len = charlen_to_bytelen(start_ptr, curr_posn - start_posn);
        appendBinaryStringInfo(&str, start_ptr, chunk_len);

        if (to_sub_text != NULL)
            appendStringInfoText(&str, to_sub_text);

        start_posn = curr_posn;
        start_ptr += chunk_len;
        start_posn += from_sub_text_len;
        start_ptr += charlen_to_bytelen(start_ptr, from_sub_text_len);

        curr_posn = text_position_next(start_posn, &state);
    } while (curr_posn > 0);

    /* copy trailing data */
    chunk_len = ((char*)src_text + VARSIZE_ANY(src_text)) - start_ptr;
    appendBinaryStringInfo(&str, start_ptr, chunk_len);

    text_position_cleanup(&state);

    ret_text = cstring_to_text_with_len(str.data, str.len);
    pfree_ext(str.data);

    if (VARHDRSZ == VARSIZE(ret_text) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
        PG_RETURN_NULL();
    else
        PG_RETURN_TEXT_P(ret_text);
}

/*
 * check_replace_text_has_escape_char
 *
 * check whether replace_text contains escape char.
 */
static bool check_replace_text_has_escape_char(const text* replace_text)
{
    const char* p = VARDATA_ANY(replace_text);
    const char* p_end = p + VARSIZE_ANY_EXHDR(replace_text);

    if (pg_database_encoding_max_length() == 1) {
        for (; p < p_end; p++) {
            if (*p == '\\')
                return true;
        }
    } else {
        for (; p < p_end; p += pg_mblen(p)) {
            if (*p == '\\')
                return true;
        }
    }

    return false;
}

/*
 * appendStringInfoRegexpSubstr
 *
 * Append replace_text to str, substituting regexp back references for
 * \n escapes.	start_ptr is the start of the match in the source string,
 * at logical character position data_pos.
 */
static void appendStringInfoRegexpSubstr(
    StringInfo str, text* replace_text, regmatch_t* pmatch, char* start_ptr, int data_pos)
{
    const char* p = VARDATA_ANY(replace_text);
    const char* p_end = p + VARSIZE_ANY_EXHDR(replace_text);
    int eml = pg_database_encoding_max_length();

    for (;;) {
        const char* chunk_start = p;
        int so;
        int eo;

        /* Find next escape char. */
        if (eml == 1) {
            for (; p < p_end && *p != '\\'; p++)
                /* nothing */;
        } else {
            for (; p < p_end && *p != '\\'; p += pg_mblen(p))
                /* nothing */;
        }

        /* Copy the text we just scanned over, if any. */
        if (p > chunk_start)
            appendBinaryStringInfo(str, chunk_start, p - chunk_start);

        /* Done if at end of string, else advance over escape char. */
        if (p >= p_end)
            break;
        p++;

        if (p >= p_end) {
            /* Escape at very end of input.  Treat same as unexpected char */
            appendStringInfoChar(str, '\\');
            break;
        }

        if (*p >= '1' && *p <= '9') {
            /* Use the back reference of regexp. */
            int idx = *p - '0';

            so = pmatch[idx].rm_so;
            eo = pmatch[idx].rm_eo;
            p++;
        } else if (*p == '&') {
            /* Use the entire matched string. */
            so = pmatch[0].rm_so;
            eo = pmatch[0].rm_eo;
            p++;
        } else if (*p == '\\') {
            /* \\ means transfer one \ to output. */
            appendStringInfoChar(str, '\\');
            p++;
            continue;
        } else {
            /*
             * If escape char is not followed by any expected char, just treat
             * it as ordinary data to copy.  (XXX would it be better to throw
             * an error?)
             */
            appendStringInfoChar(str, '\\');
            continue;
        }

        if (so != -1 && eo != -1) {
            /*
             * Copy the text that is back reference of regexp.	Note so and eo
             * are counted in characters not bytes.
             */
            char* chunk_start = NULL;
            int chunk_len;

            Assert(so >= data_pos);
            chunk_start = start_ptr;
            chunk_start += charlen_to_bytelen(chunk_start, so - data_pos);
            chunk_len = charlen_to_bytelen(chunk_start, eo - so);
            appendBinaryStringInfo(str, chunk_start, chunk_len);
        }
    }
}

#define REGEXP_REPLACE_BACKREF_CNT 10

/*
 * replace_text_regexp
 *
 * replace text that matches to regexp in src_text to replace_text.
 *
 * Note: to avoid having to include regex.h in builtins.h, we declare
 * the regexp argument as void *, but really it's regex_t *.
 * occur : the n-th matched occurrence, start from 1.
 */
text* replace_text_regexp(text* src_text, void* regexp, text* replace_text, int position, int occur)
{
    text* ret_text = NULL;
    regex_t* re = (regex_t*)regexp;
    int src_text_len = VARSIZE_ANY_EXHDR(src_text);
    StringInfoData buf;
    regmatch_t pmatch[REGEXP_REPLACE_BACKREF_CNT];
    pg_wchar* data = NULL;
    size_t data_len;
    int search_start = position - 1;
    int data_pos;
    int count = 0;
    int replace_len;
    char* start_ptr = NULL;
    bool have_escape = false;

    initStringInfo(&buf);

    /* Convert data string to wide characters. */
    data = (pg_wchar*)palloc((src_text_len + 1) * sizeof(pg_wchar));
    data_len = pg_mb2wchar_with_len(VARDATA_ANY(src_text), data, src_text_len);
    if ((unsigned int)(position) > data_len) {
        return src_text;
    }
    /* Check whether replace_text has escape char. */
    if (replace_text != NULL)
        have_escape = check_replace_text_has_escape_char(replace_text);

    /* start_ptr points to the data_pos'th character of src_text */
    start_ptr = (char*)VARDATA_ANY(src_text);
    data_pos = 0;

    while ((unsigned int)(search_start) <= data_len) {
        int regexec_result;

        CHECK_FOR_INTERRUPTS();

        regexec_result = pg_regexec(re,
            data,
            data_len,
            search_start,
            NULL, /* no details */
            REGEXP_REPLACE_BACKREF_CNT,
            pmatch,
            0);

        if (regexec_result == REG_NOMATCH)
            break;

        if (regexec_result != REG_OKAY) {
            char errMsg[100];

            pg_regerror(regexec_result, re, errMsg, sizeof(errMsg));
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_REGULAR_EXPRESSION), errmsg("regular expression failed: %s", errMsg)));
        }

        /*
         * Copy the text to the left of the match position.  Note we are given
         * character not byte indexes.
         */
        if (pmatch[0].rm_so - data_pos > 0) {
            int chunk_len;

            chunk_len = charlen_to_bytelen(start_ptr, pmatch[0].rm_so - data_pos);
            appendBinaryStringInfo(&buf, start_ptr, chunk_len);

            /*
             * Advance start_ptr over that text, to avoid multiple rescans of
             * it if the replace_text contains multiple back-references.
             */
            start_ptr += chunk_len;
            data_pos = pmatch[0].rm_so;
        }

        count++;

        replace_len = charlen_to_bytelen(start_ptr, pmatch[0].rm_eo - data_pos);

        if (occur == 0 || count == occur) {
            /*
             * Copy the replace_text. Process back references when the
             * replace_text has escape characters.
             */
            if (replace_text != NULL && have_escape)
                appendStringInfoRegexpSubstr(&buf, replace_text, pmatch, start_ptr, data_pos);
            else if (replace_text != NULL)
                appendStringInfoText(&buf, replace_text);
        } else {
            /* not the n-th matched occurrence */
            appendBinaryStringInfo(&buf, start_ptr, replace_len);
        }

        /* Advance start_ptr and data_pos over the matched text. */
        start_ptr += replace_len;
        data_pos = pmatch[0].rm_eo;

        /*
         * Advance search position.	Normally we start the next search at the
         * end of the previous match; but if the match was of zero length, we
         * have to advance by one character, or we'd just find the same match
         * again.
         */
        search_start = data_pos;
        if (pmatch[0].rm_so == pmatch[0].rm_eo)
            search_start++;
    }

    /*
     * Copy the text to the right of the last match.
     */
    if ((unsigned int)(data_pos) < data_len) {
        int chunk_len;

        chunk_len = ((char*)src_text + VARSIZE_ANY(src_text)) - start_ptr;
        appendBinaryStringInfo(&buf, start_ptr, chunk_len);
    }

    ret_text = cstring_to_text_with_len(buf.data, buf.len);
    pfree_ext(buf.data);
    pfree_ext(data);

    return ret_text;
}

/*
 * split_text
 * parse input string
 * return ord item (1 based)
 * based on provided field separator
 */
Datum split_text(PG_FUNCTION_ARGS)
{
    text* inputstring = PG_GETARG_TEXT_PP(0);
    text* fldsep = PG_GETARG_TEXT_PP(1);
    int fldnum = PG_GETARG_INT32(2);
    int inputstring_len;
    int fldsep_len;
    TextPositionState state;
    int start_posn;
    int end_posn;
    text* result_text = NULL;

    /* field number is 1 based */
    if (fldnum < 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("field position must be greater than zero")));

    text_position_setup(inputstring, fldsep, &state);

    /*
     * Note: we check the converted string length, not the original, because
     * they could be different if the input contained invalid encoding.
     */
    inputstring_len = state.len1;
    fldsep_len = state.len2;

    /* return empty string for empty input string */
    if (inputstring_len < 1) {
        text_position_cleanup(&state);

        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && !RETURN_NS) {
            PG_RETURN_NULL();
        }

        PG_RETURN_TEXT_P(cstring_to_text(""));
    }

    /* empty field separator */
    if (fldsep_len < 1) {
        text_position_cleanup(&state);
        /* if first field, return input string, else empty string */
        if (fldnum == 1) {
            PG_RETURN_TEXT_P(inputstring);
        }

        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && !RETURN_NS) {
            PG_RETURN_NULL();
        }

        PG_RETURN_TEXT_P(cstring_to_text(""));
    }

    /* identify bounds of first field */
    start_posn = 1;
    end_posn = text_position_next(1, &state);

    /* special case if fldsep not found at all */
    if (end_posn == 0) {
        text_position_cleanup(&state);
        /* if field 1 requested, return input string, else empty string */
        if (fldnum == 1) {
            PG_RETURN_TEXT_P(inputstring);
        }

        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
            PG_RETURN_NULL();
        }

        PG_RETURN_TEXT_P(cstring_to_text(""));
    }

    while (end_posn > 0 && --fldnum > 0) {
        /* identify bounds of next field */
        start_posn = end_posn + fldsep_len;
        end_posn = text_position_next(start_posn, &state);
    }

    text_position_cleanup(&state);

    if (fldnum > 0) {
        /* N'th field separator not found */
        /* if last field requested, return it, else empty string */
        if (fldnum == 1)
            result_text = text_substring(PointerGetDatum(inputstring), start_posn, -1, true);
        else
            result_text = cstring_to_text("");
    } else {
        /* non-last field requested */
        result_text = text_substring(PointerGetDatum(inputstring), start_posn, end_posn - start_posn, false);
    }

    if (TEXTISORANULL(result_text) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        PG_RETURN_NULL();
    }

    PG_RETURN_TEXT_P(result_text);
}

/*
 * Convenience function to return true when two text params are equal.
 */
static bool text_isequal(text* txt1, text* txt2)
{
    return DatumGetBool(DirectFunctionCall2(texteq, PointerGetDatum(txt1), PointerGetDatum(txt2)));
}

/*
 * text_to_array
 * parse input string and return text array of elements,
 * based on provided field separator
 */
Datum text_to_array(PG_FUNCTION_ARGS)
{
    return text_to_array_internal(fcinfo);
}

/*
 * text_to_array_null
 * parse input string and return text array of elements,
 * based on provided field separator and null string
 *
 * This is a separate entry point only to prevent the regression tests from
 * complaining about different argument sets for the same internal function.
 */
Datum text_to_array_null(PG_FUNCTION_ARGS)
{
    return text_to_array_internal(fcinfo);
}

/*
 * common code for text_to_array and text_to_array_null functions
 *
 * These are not strict so we have to test for null inputs explicitly.
 */
static Datum text_to_array_internal(PG_FUNCTION_ARGS)
{
    text* inputstring = NULL;
    text* fldsep = NULL;
    text* null_string = NULL;
    int inputstring_len;
    int fldsep_len;
    char* start_ptr = NULL;
    text* result_text = NULL;
    bool is_null = false;
    ArrayBuildState* astate = NULL;

    /* when input string is NULL, then result is NULL too */
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    inputstring = PG_GETARG_TEXT_PP(0);

    /* fldsep can be NULL */
    if (!PG_ARGISNULL(1))
        fldsep = PG_GETARG_TEXT_PP(1);
    else
        fldsep = NULL;

    /* null_string can be NULL or omitted */
    if (PG_NARGS() > 2 && !PG_ARGISNULL(2))
        null_string = PG_GETARG_TEXT_PP(2);
    else
        null_string = NULL;

    if (fldsep != NULL) {
        /*
         * Normal case with non-null fldsep.  Use the text_position machinery
         * to search for occurrences of fldsep.
         */
        TextPositionState state;
        int fldnum;
        int start_posn;
        int end_posn;
        int chunk_len;

        text_position_setup(inputstring, fldsep, &state);

        /*
         * Note: we check the converted string length, not the original,
         * because they could be different if the input contained invalid
         * encoding.
         */
        inputstring_len = state.len1;
        fldsep_len = state.len2;

        /* return empty array for empty input string */
        if (inputstring_len < 1) {
            text_position_cleanup(&state);
            PG_RETURN_ARRAYTYPE_P(construct_empty_array(TEXTOID));
        }

        /*
         * empty field separator: return the input string as a one-element
         * array
         */
        if (fldsep_len < 1) {
            text_position_cleanup(&state);
            /* single element can be a NULL too */
            is_null = null_string ? text_isequal(inputstring, null_string) : false;
            PG_RETURN_ARRAYTYPE_P(create_singleton_array(fcinfo, TEXTOID, PointerGetDatum(inputstring), is_null, 1));
        }

        start_posn = 1;
        /* start_ptr points to the start_posn'th character of inputstring */
        start_ptr = VARDATA_ANY(inputstring);

        for (fldnum = 1;; fldnum++) { /* field number is 1 based */
            CHECK_FOR_INTERRUPTS();

            end_posn = text_position_next(start_posn, &state);

            chunk_len = (end_posn == 0) ? (((char*)inputstring + VARSIZE_ANY(inputstring)) - start_ptr) :
                charlen_to_bytelen(start_ptr, end_posn - start_posn);

            /* must build a temp text datum to pass to accumArrayResult */
            result_text = cstring_to_text_with_len(start_ptr, chunk_len);
            is_null = null_string ? text_isequal(result_text, null_string) : false;

            /* stash away this field */
            astate = accumArrayResult(astate, PointerGetDatum(result_text), is_null, TEXTOID, CurrentMemoryContext);

            pfree_ext(result_text);

            if (end_posn == 0)
                break;

            start_posn = end_posn;
            start_ptr += chunk_len;
            start_posn += fldsep_len;
            start_ptr += charlen_to_bytelen(start_ptr, fldsep_len);
        }

        text_position_cleanup(&state);
    } else {
        /*
         * When fldsep is NULL, each character in the inputstring becomes an
         * element in the result array.  The separator is effectively the
         * space between characters.
         */
        inputstring_len = VARSIZE_ANY_EXHDR(inputstring);

        /* return empty array for empty input string */
        if (inputstring_len < 1)
            PG_RETURN_ARRAYTYPE_P(construct_empty_array(TEXTOID));

        start_ptr = VARDATA_ANY(inputstring);

        while (inputstring_len > 0) {
            int chunk_len = pg_mblen(start_ptr);

            CHECK_FOR_INTERRUPTS();

            /* must build a temp text datum to pass to accumArrayResult */
            result_text = cstring_to_text_with_len(start_ptr, chunk_len);
            is_null = null_string ? text_isequal(result_text, null_string) : false;

            /* stash away this field */
            astate = accumArrayResult(astate, PointerGetDatum(result_text), is_null, TEXTOID, CurrentMemoryContext);

            pfree_ext(result_text);

            start_ptr += chunk_len;
            inputstring_len -= chunk_len;
        }
    }

    PG_RETURN_ARRAYTYPE_P(makeArrayResult(astate, CurrentMemoryContext));
}

/*
 * array_to_text
 * concatenate Cstring representation of input array elements
 * using provided field separator
 */
Datum array_to_text(PG_FUNCTION_ARGS)
{
    ArrayType* v = PG_GETARG_ARRAYTYPE_P(0);
    char* fldsep = text_to_cstring(PG_GETARG_TEXT_PP(1));
    text* result = NULL;

    result = array_to_text_internal(fcinfo, v, fldsep, NULL);

    /* To A db, empty string need return NULL.*/
    if (0 == VARSIZE_ANY_EXHDR(result) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_TEXT_P(result);
    }
}

/*
 * array_to_text_null
 * concatenate Cstring representation of input array elements
 * using provided field separator and null string
 *
 * This version is not strict so we have to test for null inputs explicitly.
 */
Datum array_to_text_null(PG_FUNCTION_ARGS)
{
    ArrayType* v = NULL;
    char* fldsep = NULL;
    char* null_string = NULL;
    text* result = NULL;

    /* returns NULL when first or second parameter is NULL */
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
        PG_RETURN_NULL();

    v = PG_GETARG_ARRAYTYPE_P(0);
    fldsep = text_to_cstring(PG_GETARG_TEXT_PP(1));

    /* NULL null string is passed through as a null pointer */
    if (!PG_ARGISNULL(2))
        null_string = text_to_cstring(PG_GETARG_TEXT_PP(2));
    else
        null_string = NULL;

    result = array_to_text_internal(fcinfo, v, fldsep, null_string);

    /* To A db, empty string need return NULL.*/
    if (0 == VARSIZE_ANY_EXHDR(result) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        PG_RETURN_NULL();
    } else {
        PG_RETURN_TEXT_P(result);
    }
}

/*
 * common code for array_to_text and array_to_text_null functions
 */
static text* array_to_text_internal(FunctionCallInfo fcinfo, ArrayType* v, char* fldsep, char* null_string)
{
    text* result = NULL;
    int nitems, *dims = NULL, ndims;
    Oid element_type;
    int typlen;
    bool typbyval = false;
    char typalign;
    StringInfoData buf;
    bool printed = false;
    char* p = NULL;
    bits8* bitmap = NULL;
    int bitmask;
    int i;
    ArrayMetaState* my_extra = NULL;

    ndims = ARR_NDIM(v);
    dims = ARR_DIMS(v);
    nitems = ArrayGetNItems(ndims, dims);

    /* if there are no elements, return an empty string */
    if (nitems == 0)
        return cstring_to_text_with_len("", 0);

    element_type = ARR_ELEMTYPE(v);
    initStringInfo(&buf);

    /*
     * We arrange to look up info about element type, including its output
     * conversion proc, only once per series of calls, assuming the element
     * type doesn't change underneath us.
     */
    my_extra = (ArrayMetaState*)fcinfo->flinfo->fn_extra;
    if (my_extra == NULL) {
        fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt, sizeof(ArrayMetaState));
        my_extra = (ArrayMetaState*)fcinfo->flinfo->fn_extra;
        my_extra->element_type = ~element_type;
    }

    if (my_extra->element_type != element_type) {
        /*
         * Get info about element type, including its output conversion proc
         */
        get_type_io_data(element_type,
            IOFunc_output,
            &my_extra->typlen,
            &my_extra->typbyval,
            &my_extra->typalign,
            &my_extra->typdelim,
            &my_extra->typioparam,
            &my_extra->typiofunc);
        fmgr_info_cxt(my_extra->typiofunc, &my_extra->proc, fcinfo->flinfo->fn_mcxt);
        my_extra->element_type = element_type;
    }
    typlen = my_extra->typlen;
    typbyval = my_extra->typbyval;
    typalign = my_extra->typalign;

    p = ARR_DATA_PTR(v);
    bitmap = ARR_NULLBITMAP(v);
    bitmask = 1;

    for (i = 0; i < nitems; i++) {
        Datum itemvalue;
        char* value = NULL;

        /* Get source element, checking for NULL */
        if (bitmap && (*bitmap & bitmask) == 0) {
            /* if null_string is NULL, we just ignore null elements */
            if (null_string != NULL) {
                if (printed)
                    appendStringInfo(&buf, "%s%s", fldsep, null_string);
                else
                    appendStringInfoString(&buf, null_string);
                printed = true;
            }
        } else {
            itemvalue = fetch_att(p, typbyval, typlen);

            value = OutputFunctionCall(&my_extra->proc, itemvalue);

            if (printed)
                appendStringInfo(&buf, "%s%s", fldsep, value);
            else
                appendStringInfoString(&buf, value);
            printed = true;

            p = att_addlength_pointer(p, typlen, p);
            p = (char*)att_align_nominal(p, typalign);
        }

        /* advance bitmap pointer if any */
        if (bitmap != NULL) {
            bitmask <<= 1;
            if (bitmask == 0x100) {
                bitmap++;
                bitmask = 1;
            }
        }
    }

    result = cstring_to_text_with_len(buf.data, buf.len);
    pfree_ext(buf.data);

    return result;
}

#define HEXBASE 16
/*
 * Convert a int32 to a string containing a base 16 (hex) representation of
 * the number.
 */
Datum to_hex32(PG_FUNCTION_ARGS)
{
    uint32 value = (uint32)PG_GETARG_INT32(0);
    char* ptr = NULL;
    const char* digits = "0123456789abcdef";
    char buf[32]; /* bigger than needed, but reasonable */

    ptr = buf + sizeof(buf) - 1;
    *ptr = '\0';

    do {
        *--ptr = digits[value % HEXBASE];
        value /= HEXBASE;
    } while (ptr > buf && value);

    PG_RETURN_TEXT_P(cstring_to_text(ptr));
}

/*
 * Convert a int64 to a string containing a base 16 (hex) representation of
 * the number.
 */
Datum to_hex64(PG_FUNCTION_ARGS)
{
    uint64 value = (uint64)PG_GETARG_INT64(0);
    char* ptr = NULL;
    const char* digits = "0123456789abcdef";
    char buf[32]; /* bigger than needed, but reasonable */

    ptr = buf + sizeof(buf) - 1;
    *ptr = '\0';

    do {
        *--ptr = digits[value % HEXBASE];
        value /= HEXBASE;
    } while (ptr > buf && value);

    PG_RETURN_TEXT_P(cstring_to_text(ptr));
}

/*
 * Create an md5 hash of a text string and return it as hex
 *
 * md5 produces a 16 byte (128 bit) hash; double it for hex
 */
#define MD5_HASH_LEN 32

Datum md5_text(PG_FUNCTION_ARGS)
{
    text* in_text = PG_GETARG_TEXT_PP(0);
    size_t len;
    char hexsum[MD5_HASH_LEN + 1];

    /* Calculate the length of the buffer using varlena metadata */
    len = VARSIZE_ANY_EXHDR(in_text);

    /* get the hash result */
    if (pg_md5_hash(VARDATA_ANY(in_text), len, hexsum) == false)
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

    /* convert to text and return it */
    PG_RETURN_TEXT_P(cstring_to_text(hexsum));
}

/*
 * Create an md5 hash of a bytea field and return it as a hex string:
 * 16-byte md5 digest is represented in 32 hex characters.
 */
Datum md5_bytea(PG_FUNCTION_ARGS)
{
    bytea* in = PG_GETARG_BYTEA_PP(0);
    size_t len;
    char hexsum[MD5_HASH_LEN + 1];

    len = VARSIZE_ANY_EXHDR(in);
    if (pg_md5_hash(VARDATA_ANY(in), len, hexsum) == false)
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

    PG_RETURN_TEXT_P(cstring_to_text(hexsum));
}

/*
 * Return the size of a datum, possibly compressed
 *
 * Works on any data type
 */
Datum pg_column_size(PG_FUNCTION_ARGS)
{
    Datum value = PG_GETARG_DATUM(0);
    int32 result;
    int typlen;

    /* On first call, get the input type's typlen, and save at *fn_extra */
    if (fcinfo->flinfo->fn_extra == NULL) {
        /* Lookup the datatype of the supplied argument */
        Oid argtypeid = get_fn_expr_argtype(fcinfo->flinfo, 0);

        typlen = get_typlen(argtypeid);
        if (typlen == 0) /* should not happen */
            ereport(
                ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", argtypeid)));

        fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt, sizeof(int));
        *((int*)fcinfo->flinfo->fn_extra) = typlen;
    } else
        typlen = *((int*)fcinfo->flinfo->fn_extra);

    if (typlen == -1) {
        /* varlena type, possibly toasted */
        result = toast_datum_size(value);
    } else if (typlen == -2) {
        /* cstring */
        result = strlen(DatumGetCString(value)) + 1;
    } else {
        /* ordinary fixed-width type */
        result = typlen;
    }

    PG_RETURN_INT32(result);
}

/*
 * @Description: This function is used to calculate the size of a datum
 *
 * @IN PG_FUNCTION_ARGS: any data type
 * @return: the byte size of the data
 */
Datum datalength(PG_FUNCTION_ARGS)
{
    Datum value = PG_GETARG_DATUM(0);
    int32 result = 0;

    /* Lookup the datatype of the supplied argument */
    Oid argtypeid = get_fn_expr_argtype(fcinfo->flinfo, 0);

    switch (argtypeid) {
        case INT1OID:          /* for TINYINT */
        case INT2OID:          /* for SMALLINT */
        case INT4OID:          /* for INTEGER */
        case INT8OID:          /* for BIGINT */
        case FLOAT4OID:        /* for FLOAT4 */
        case FLOAT8OID:        /* for FLOAT8 */
        case BOOLOID:          /* for BOOLEAN */
        case CHAROID:          /* for CHAR */
        case DATEOID:          /* for DATE */
        case TIMEOID:          /* for TIME */
        case TIMETZOID:        /* for TIMEZ */
        case TIMESTAMPOID:     /* for TIMESTAMP */
        case TIMESTAMPTZOID:   /* for TIMESTAMPTZOID */
        case SMALLDATETIMEOID: /* for SMALLDATETIME */
        case INTERVALOID:      /* for INTERVAL */
        case TINTERVALOID:     /* for TINTERVAL */
        case RELTIMEOID:       /* for RELTIME */
        case ABSTIMEOID:       /* for ABSTIME */
        {
            result = get_typlen(argtypeid);
            break;
        }
        case BPCHAROID: /* for BPCHAR */
        {
            BpChar* arg = PG_GETARG_BPCHAR_PP(0);
            result = VARSIZE_ANY_EXHDR(arg);
            break;
        }
        case VARCHAROID:   /* for VARCHAR */
        case NVARCHAR2OID: /* for NVARCHAR */
        case TEXTOID:      /* for TEXT */
        case CLOBOID:      /* for CLOB */
        {
            result = toast_raw_datum_size(value) - VARHDRSZ;
            break;
        }
        case NUMERICOID: /* for NUMERIC */
        {
            Numeric num = PG_GETARG_NUMERIC(0);
            result = get_ndigit_from_numeric(num);
            break;
        }
        default:
            ereport(ERROR,
                (errmodule(MOD_FUNCTION),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unsupport type %s", get_typename(argtypeid))));
    }

    PG_RETURN_INT32(result);
}

/*
 * string_agg - Concatenates values and returns string.
 *
 * Syntax: string_agg(value text, delimiter text) RETURNS text
 *
 * Note: Any NULL values are ignored. The first-call delimiter isn't
 * actually used at all, and on subsequent calls the delimiter precedes
 * the associated value.
 */

/* subroutine to initialize state */
static StringInfo makeStringAggState(FunctionCallInfo fcinfo)
{
    StringInfo state;
    MemoryContext aggcontext;
    MemoryContext oldcontext;

    if (!AggCheckCallContext(fcinfo, &aggcontext)) {
        /* cannot be called directly because of internal-type argument */
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("string_agg_transfn called in non-aggregate context")));
    }

    /*
     * Create state in aggregate context.  It'll stay there across subsequent
     * calls.
     */
    oldcontext = MemoryContextSwitchTo(aggcontext);
    state = makeStringInfo();
    MemoryContextSwitchTo(oldcontext);

    return state;
}

Datum string_agg_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, PG_GETARG_TEXT_PP(2)); /* delimiter */

        appendStringInfoText(state, PG_GETARG_TEXT_PP(1)); /* value */
    }

    /*
     * The transition type for string_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum string_agg_finalfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    /* cannot be called directly because of internal-type argument */
    Assert(AggCheckCallContext(fcinfo, NULL));

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    if (state != NULL)
        PG_RETURN_TEXT_P(cstring_to_text_with_len(state->data, state->len));
    else
        PG_RETURN_NULL();
}

/*
 * checksumtext_agg_transfn - sum the oldsumofhashvalue and the value of text input and returns numeric.
 *
 * Syntax: checksumtext_agg_transfn(numeric, text) RETURNS numeric
 *
 */
Datum checksumtext_agg_transfn(PG_FUNCTION_ARGS)
{
    Numeric oldsumofhashval;
    Datum newhashval;
    int64 hashval;

    if (PG_ARGISNULL(0)) {
        /* No non-null input seen so far... */
        if (PG_ARGISNULL(1))
            PG_RETURN_NULL(); /* still no non-null */
        /* This is the first non-null input. */
        hashval = (int64)DirectFunctionCall1(hashtext, PG_GETARG_DATUM(1));
        newhashval = DirectFunctionCall1(int8_numeric, hashval);
        PG_RETURN_DATUM(newhashval);
    }

    oldsumofhashval = PG_GETARG_NUMERIC(0);

    /* Leave oldsumofhashval unchanged if new input is null. */
    if (PG_ARGISNULL(1))
        PG_RETURN_NUMERIC(oldsumofhashval);

    /* OK to do the addition. */
    hashval = (int64)DirectFunctionCall1(hashtext, PG_GETARG_DATUM(1));
    newhashval = DirectFunctionCall1(int8_numeric, hashval);

    PG_RETURN_DATUM(DirectFunctionCall2(numeric_add, NumericGetDatum(oldsumofhashval), newhashval));
}

Datum list_agg_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, PG_GETARG_TEXT_PP(2)); /* delimiter */

        appendStringInfoText(state, PG_GETARG_TEXT_PP(1)); /* value */
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum list_agg_finalfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    /* cannot be called directly because of internal-type argument */
    Assert(AggCheckCallContext(fcinfo, NULL));

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    if (state != NULL)
        PG_RETURN_TEXT_P(cstring_to_text_with_len(state->data, state->len));
    else
        PG_RETURN_NULL();
}

Datum list_agg_noarg2_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, cstring_to_text("")); /* delimiter */

        appendStringInfoText(state, PG_GETARG_TEXT_PP(1)); /* value */
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum int2_list_agg_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, PG_GETARG_TEXT_PP(2)); /* delimiter */

        appendStringInfo(state, "%hd", PG_GETARG_INT16(1)); /* value */
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum int2_list_agg_noarg2_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, cstring_to_text("")); /* delimiter */

        appendStringInfo(state, "%hd", PG_GETARG_INT16(1)); /* value */
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum int4_list_agg_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, PG_GETARG_TEXT_PP(2)); /* delimiter */

        appendStringInfo(state, "%d", PG_GETARG_INT32(1)); /* value */
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum int4_list_agg_noarg2_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, cstring_to_text("")); /* delimiter */

        appendStringInfo(state, "%d", PG_GETARG_INT32(1)); /* value */
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum int8_list_agg_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, PG_GETARG_TEXT_PP(2)); /* delimiter */

        appendStringInfo(state, "%ld", PG_GETARG_INT64(1)); /* value */
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum int8_list_agg_noarg2_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, cstring_to_text("")); /* delimiter */

        appendStringInfo(state, "%ld", PG_GETARG_INT64(1)); /* value */
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum float4_list_agg_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, PG_GETARG_TEXT_PP(2)); /* delimiter */

        appendStringInfo(state, "%f", PG_GETARG_FLOAT4(1)); /* value */
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum float4_list_agg_noarg2_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, cstring_to_text("")); /* delimiter */

        appendStringInfo(state, "%lf", PG_GETARG_FLOAT4(1)); /* value */
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum float8_list_agg_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, PG_GETARG_TEXT_PP(2)); /* delimiter */

        appendStringInfo(state, "%lf", PG_GETARG_FLOAT8(1)); /* value */
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum float8_list_agg_noarg2_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, cstring_to_text("")); /* delimiter */

        appendStringInfo(state, "%lf", PG_GETARG_FLOAT8(1)); /* value */
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum numeric_list_agg_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        char* val = NULL;
        Numeric num = PG_GETARG_NUMERIC(1);

        if (NUMERIC_IS_BI(num)) {
            num = makeNumericNormal(num);
        }
        val = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(num)));

        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, PG_GETARG_TEXT_PP(2)); /* delimiter */

        appendStringInfo(state, "%s", val); /* value */
        pfree_ext(val);
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum numeric_list_agg_noarg2_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        char* val = NULL;
        Numeric num = PG_GETARG_NUMERIC(1);

        if (NUMERIC_IS_BI(num)) {
            num = makeNumericNormal(num);
        }
        val = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(num)));

        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, cstring_to_text("")); /* delimiter */

        appendStringInfo(state, "%s", val); /* value */
        pfree_ext(val);
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum date_list_agg_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        char* val = NULL;
        DateADT dateVal = PG_GETARG_DATEADT(1);
        val = DatumGetCString(DirectFunctionCall1(date_out, dateVal));

        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, PG_GETARG_TEXT_PP(2)); /* delimiter */

        appendStringInfo(state, "%s", val); /* value */
        pfree_ext(val);
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum date_list_agg_noarg2_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        char* val = NULL;
        DateADT dateVal = PG_GETARG_DATEADT(1);
        val = DatumGetCString(DirectFunctionCall1(date_out, dateVal));

        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, cstring_to_text("")); /* delimiter */

        appendStringInfo(state, "%s", val); /* value */
        pfree_ext(val);
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum timestamp_list_agg_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        char* val = NULL;
        Timestamp timestamp = PG_GETARG_TIMESTAMP(1);
        val = DatumGetCString(DirectFunctionCall1(timestamp_out, timestamp));

        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, PG_GETARG_TEXT_PP(2)); /* delimiter */

        appendStringInfo(state, "%s", val); /* value */
        pfree_ext(val);
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum timestamp_list_agg_noarg2_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        char* val = NULL;
        Timestamp timestamp = PG_GETARG_TIMESTAMP(1);
        val = DatumGetCString(DirectFunctionCall1(timestamp_out, timestamp));

        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, cstring_to_text("")); /* delimiter */

        appendStringInfo(state, "%s", val); /* value */
        pfree_ext(val);
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum timestamptz_list_agg_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        char* val = NULL;
        TimestampTz dt = PG_GETARG_TIMESTAMPTZ(1);
        val = DatumGetCString(DirectFunctionCall1(timestamptz_out, dt));

        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, PG_GETARG_TEXT_PP(2)); /* delimiter */

        appendStringInfo(state, "%s", val); /* value */
        pfree_ext(val);
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum timestamptz_list_agg_noarg2_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        char* val = NULL;
        TimestampTz dt = PG_GETARG_TIMESTAMPTZ(1);
        val = DatumGetCString(DirectFunctionCall1(timestamptz_out, dt));

        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, cstring_to_text("")); /* delimiter */

        appendStringInfo(state, "%s", val); /* value */
        pfree_ext(val);
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum interval_list_agg_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        char* val = NULL;
        Interval* span = PG_GETARG_INTERVAL_P(1);
        val = DatumGetCString(DirectFunctionCall1(interval_out, PointerGetDatum(span)));

        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, PG_GETARG_TEXT_PP(2)); /* delimiter */

        appendStringInfo(state, "%s", val); /* value */
        pfree_ext(val);
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

Datum interval_list_agg_noarg2_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        char* val = NULL;
        Interval* span = PG_GETARG_INTERVAL_P(1);
        val = DatumGetCString(DirectFunctionCall1(interval_out, PointerGetDatum(span)));

        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2))
            appendStringInfoText(state, cstring_to_text("")); /* delimiter */

        appendStringInfo(state, "%s", val); /* value */
        pfree_ext(val);
    }

    /*
     * The transition type for list_agg() is declared to be "internal",
     * which is a pass-by-value type the same size as a pointer.
     */
    PG_RETURN_POINTER(state);
}

static void check_huge_toast_pointer(Datum value, Oid valtype)
{
    if ((valtype == TEXTOID || valtype == CLOBOID || valtype == BLOBOID) &&
        VARATT_IS_HUGE_TOAST_POINTER(DatumGetPointer(value))) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("concat could not support more than 1GB clob/blob data")));
    }
}

/*
 * Implementation of both concat() and concat_ws().
 *
 * sepstr/seplen describe the separator.  argidx is the first argument
 * to concatenate (counting from zero).
 */
static text* concat_internal(const char* sepstr, int seplen, int argidx, FunctionCallInfo fcinfo, bool is_concat_ws)
{
    text* result = NULL;
    StringInfoData str;
    bool first_arg = true;
    int i;
    if (CONCAT_VARIADIC) {
        if (get_fn_expr_variadic(fcinfo->flinfo)) {
            ArrayType* arr = NULL;

            /* concat(VARIADIC NULL) is defined as NULL */
            if (PG_ARGISNULL(argidx))
                PG_RETURN_NULL();

            /*
             * Non-null argument had better be an array.  We assume that any call
             * context that could let get_fn_expr_variadic return true will have
             * checked that a VARIADIC-labeled parameter actually is an array.  So
             * it should be okay to just Assert that it's an array rather than
             * doing a full-fledged error check.
             */
            if (!OidIsValid(get_base_element_type(get_fn_expr_argtype(fcinfo->flinfo, argidx)))) {
                ereport(ERROR,
                    (errcode(ERRCODE_INDETERMINATE_DATATYPE),
                        errmsg("could not determine data type of concat() input to variadic")));
            } else {
                /* OK, safe to fetch the array value */
                arr = PG_GETARG_ARRAYTYPE_P(argidx);
            }
            /*
             * And serialize the array.  We tell array_to_text to ignore null
             * elements, which matches the behavior of the loop below.
             */
            return array_to_text_internal(fcinfo, arr, pstrdup(sepstr), NULL);
        }
    }

    initStringInfo(&str);

    for (i = argidx; i < PG_NARGS(); i++) {
        if (!PG_ARGISNULL(i)) {
            Datum value = PG_GETARG_DATUM(i);
            Oid valtype;
            Oid typOutput;
            bool typIsVarlena = false;

            /* add separator if appropriate */
            if (first_arg)
                first_arg = false;
            else
                appendBinaryStringInfo(&str, sepstr, seplen);

            /* call the appropriate type output function, append the result */
            valtype = get_fn_expr_argtype(fcinfo->flinfo, i);
            check_huge_toast_pointer(value, valtype);
            if (!OidIsValid(valtype))
                ereport(ERROR, (errcode(ERRCODE_INDETERMINATE_DATATYPE),
                        errmsg("could not determine data type of concat() input")));
            getTypeOutputInfo(valtype, &typOutput, &typIsVarlena);
            appendStringInfoString(&str, OidOutputFunctionCall(typOutput, value));
        } else if (PG_ARGISNULL(i) && u_sess->attr.attr_sql.sql_compatibility == B_FORMAT && !is_concat_ws) {
            pfree_ext(str.data);
            PG_RETURN_NULL();
        }
    }

    result = cstring_to_text_with_len(str.data, str.len);
    pfree_ext(str.data);

    if ((result == NULL || (0 == VARSIZE_ANY_EXHDR(result) && !DB_IS_CMPT(B_FORMAT | PG_FORMAT))) &&
        (CONCAT_VARIADIC || DB_IS_CMPT(A_FORMAT)))
        PG_RETURN_NULL();
    else
        return result;
}

/*
 * Concatenate all arguments. NULL arguments are ignored.
 */
Datum text_concat(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(concat_internal("", 0, 0, fcinfo, false));
}

/*
 * Concatenate all but first argument value with separators. The first
 * parameter is used as the separator. NULL arguments are ignored.
 */
Datum text_concat_ws(PG_FUNCTION_ARGS)
{
    /* return NULL when separator is NULL */
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    char* sep = text_to_cstring(PG_GETARG_TEXT_PP(0));
    text* result = concat_internal(sep, strlen(sep), 1, fcinfo, true);

    if (result == NULL)
        PG_RETURN_NULL();

    PG_RETURN_TEXT_P(result);
}

/*
 * Return first n characters in the string. When n is negative,
 * return all but last |n| characters.
 */
Datum text_left(PG_FUNCTION_ARGS)
{
    text* str = PG_GETARG_TEXT_PP(0);
    if (VARATT_IS_HUGE_TOAST_POINTER((varlena *)str)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("text_left could not support more than 1GB clob/blob data")));
    }
    const char* p = VARDATA_ANY(str);
    int len = VARSIZE_ANY_EXHDR(str);
    int n = PG_GETARG_INT32(1);
    int part_off = 0;
    int rlen;
    text* part_str = NULL;

    if (n < 0) {
        n = pg_mbstrlen_with_len(p, len) + n;
    }

    if (n >= 0) {
        part_str = text_substring(PointerGetDatum(str), 1, n, false);
        if (part_str != NULL) {
            part_off = VARSIZE_ANY_EXHDR(part_str);
            pfree_ext(part_str);
        }
    }

    rlen = pg_mbcharcliplen(p, len, part_off);
    if (0 == rlen && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        PG_RETURN_NULL();
    }

    PG_RETURN_TEXT_P(cstring_to_text_with_len(p, rlen));
}
/*
 * Return last n characters in the string. When n is negative,
 * return all but first |n| characters.
 */
Datum text_right(PG_FUNCTION_ARGS)
{
    text* str = PG_GETARG_TEXT_PP(0);
    if (VARATT_IS_HUGE_TOAST_POINTER((varlena *)str)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("text_right could not support more than 1GB clob/blob data")));
    }
    const char* p = VARDATA_ANY(str);
    int len = VARSIZE_ANY_EXHDR(str);
    int n = PG_GETARG_INT32(1);
    int part_off = 0;
    int off;
    text* part_str = NULL;

    if (n < 0)
        n = -n;
    else
        n = pg_mbstrlen_with_len(p, len) - n;

    if (n >= 0) {
        part_str = text_substring(PointerGetDatum(str), 1, n, false);
        if (part_str != NULL) {
            part_off = VARSIZE_ANY_EXHDR(part_str);
            pfree_ext(part_str);
        }
    }
    off = pg_mbcharcliplen(p, len, part_off);
    if (0 == (len - off) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        PG_RETURN_NULL();
    }

    PG_RETURN_TEXT_P(cstring_to_text_with_len(p + off, len - off));
}

/*
 * Return reversed string
 */
Datum text_reverse(PG_FUNCTION_ARGS)
{
    text* str = PG_GETARG_TEXT_PP(0);
    if (VARATT_IS_HUGE_TOAST_POINTER((varlena *)str)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("text_reverse could not support more than 1GB clob/blob data")));
    }
    const char* p = VARDATA_ANY(str);
    int len = VARSIZE_ANY_EXHDR(str);
    const char* endp = p + len;
    text* result = NULL;
    char* dst = NULL;
    int rc = 0;
    result = (text*)palloc(len + VARHDRSZ);
    dst = (char*)VARDATA(result) + len;
    SET_VARSIZE(result, len + VARHDRSZ);

    if (pg_database_encoding_max_length() > 1) {
        /* multibyte version */
        while (p < endp) {
            int sz;

            sz = pg_mblen(p);
            dst -= sz;
            if (sz > 0) {
                rc = memcpy_s(dst, sz, p, sz);
                securec_check(rc, "\0", "\0");
            }
            p += sz;
        }
    } else {
        /* single byte version */
        while (p < endp)
            *(--dst) = *p++;
    }

    PG_RETURN_TEXT_P(result);
}

/*
 * Support macros for text_format()
 */
#define TEXT_FORMAT_FLAG_MINUS 0x0001 /* is minus flag present? */

#define ADVANCE_PARSE_POINTER(ptr, end_ptr)                                                                          \
    do {                                                                                                             \
        if (++(ptr) >= (end_ptr))                                                                                    \
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unterminated conversion specifier"))); \
    } while (0)

/*
 * Returns a formated string
 */
Datum text_format(PG_FUNCTION_ARGS)
{
    text* fmt = NULL;
    StringInfoData str;
    const char* cp = NULL;
    const char* start_ptr = NULL;
    const char* end_ptr = NULL;
    text* result = NULL;
    int arg = 0;
    bool funcvariadic = false;
    int nargs;
    Datum* elements = NULL;
    bool* nulls = NULL;
    Oid element_type = InvalidOid;
    Oid prev_type = InvalidOid;
    Oid prev_width_type = InvalidOid;
    FmgrInfo typoutputfinfo;
    FmgrInfo typoutputinfo_width;

    /* When format string is null, immediately return null */
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();

    /* If argument is marked VARIADIC, expand array into elements */
    if (get_fn_expr_variadic(fcinfo->flinfo)) {
        ArrayType* arr = NULL;
        int16 elmlen;
        bool elmbyval = false;
        char elmalign;
        int nitems = 0;

        /* Should have just the one argument */
        Assert(PG_NARGS() == 2);

        /* If argument is NULL, we treat it as zero-length array */
        if (PG_ARGISNULL(1))
            nitems = 0;
        else {
            /*
             * Non-null argument had better be an array.  We assume that any
             * call context that could let get_fn_expr_variadic return true
             * will have checked that a VARIADIC-labeled parameter actually is
             * an array.  So it should be okay to just Assert that it's an
             * array rather than doing a full-fledged error check.
             */
            Assert(OidIsValid(get_base_element_type(get_fn_expr_argtype(fcinfo->flinfo, 1))));

            /* OK, safe to fetch the array value */
            arr = PG_GETARG_ARRAYTYPE_P(1);

            /* Get info about array element type */
            element_type = ARR_ELEMTYPE(arr);
            get_typlenbyvalalign(element_type, &elmlen, &elmbyval, &elmalign);

            /* Extract all array elements */
            deconstruct_array(arr, element_type, elmlen, elmbyval, elmalign, &elements, &nulls, &nitems);
        }

        nargs = nitems + 1;
        funcvariadic = true;
    } else {
        /* Non-variadic case, we'll process the arguments individually */
        nargs = PG_NARGS();
        funcvariadic = false;
    }

    /* Setup for main loop. */
    fmt = PG_GETARG_TEXT_PP(0);
    start_ptr = VARDATA_ANY(fmt);
    end_ptr = start_ptr + VARSIZE_ANY_EXHDR(fmt);
    initStringInfo(&str);
    arg = 1; /* next argument position to print */

    /* Scan format string, looking for conversion specifiers. */
    for (cp = start_ptr; cp < end_ptr; cp++) {
        int argpos;
        int widthpos;
        int flags;
        int width;
        Datum value;
        bool isNull = false;
        Oid typid;

        /*
         * If it's not the start of a conversion specifier, just copy it to
         * the output buffer.
         */
        if (*cp != '%') {
            appendStringInfoCharMacro(&str, *cp);
            continue;
        }

        ADVANCE_PARSE_POINTER(cp, end_ptr);

        /* Easy case: %% outputs a single % */
        if (*cp == '%') {
            appendStringInfoCharMacro(&str, *cp);
            continue;
        }

        /* Parse the optional portions of the format specifier */
        cp = text_format_parse_format(cp, end_ptr, &argpos, &widthpos, &flags, &width);

        /*
         * Next we should see the main conversion specifier.  Whether or not
         * an argument position was present, it's known that at least one
         * character remains in the string at this point.  Experience suggests
         * that it's worth checking that that character is one of the expected
         * ones before we try to fetch arguments, so as to produce the least
         * confusing response to a mis-formatted specifier.
         */
        if (strchr("sIL", *cp) == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unrecognized conversion specifier \"%c\"", *cp)));

        /* If indirect width was specified, get its value */
        if (widthpos >= 0) {
            /* Collect the specified or next argument position */
            if (widthpos > 0)
                arg = widthpos;
            if (arg >= nargs)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("too few arguments for format")));

            /* Get the value and type of the selected argument */
            if (!funcvariadic) {
                value = PG_GETARG_DATUM(arg);
                isNull = PG_ARGISNULL(arg);
                typid = get_fn_expr_argtype(fcinfo->flinfo, arg);
            } else {
                value = elements[arg - 1];
                isNull = nulls[arg - 1];
                typid = element_type;
            }
            if (!OidIsValid(typid))
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("could not determine data type of format() input")));

            arg++;

            /* We can treat NULL width the same as zero */
            if (isNull)
                width = 0;
            else if (typid == INT4OID)
                width = DatumGetInt32(value);
            else if (typid == INT2OID)
                width = DatumGetInt16(value);
            else {
                /* For less-usual datatypes, convert to text then to int */
                char* str = NULL;

                if (typid != prev_width_type) {
                    Oid typoutputfunc;
                    bool typIsVarlena = false;

                    getTypeOutputInfo(typid, &typoutputfunc, &typIsVarlena);
                    fmgr_info(typoutputfunc, &typoutputinfo_width);
                    prev_width_type = typid;
                }

                str = OutputFunctionCall(&typoutputinfo_width, value);

                /* pg_strtoint32 will complain about bad data or overflow */
                width = pg_strtoint32(str);

                pfree_ext(str);
            }
        }

        /* Collect the specified or next argument position */
        if (argpos > 0)
            arg = argpos;
        if (arg >= nargs)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("too few arguments for format")));

        /* Get the value and type of the selected argument */
        if (!funcvariadic) {
            value = PG_GETARG_DATUM(arg);
            isNull = PG_ARGISNULL(arg);
            typid = get_fn_expr_argtype(fcinfo->flinfo, arg);
        } else {
            value = elements[arg - 1];
            isNull = nulls[arg - 1];
            typid = element_type;
        }
        if (!OidIsValid(typid))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("could not determine data type of format() input")));

        arg++;

        /*
         * Get the appropriate typOutput function, reusing previous one if
         * same type as previous argument.  That's particularly useful in the
         * variadic-array case, but often saves work even for ordinary calls.
         */
        if (typid != prev_type) {
            Oid typoutputfunc;
            bool typIsVarlena = false;

            getTypeOutputInfo(typid, &typoutputfunc, &typIsVarlena);
            fmgr_info(typoutputfunc, &typoutputfinfo);
            prev_type = typid;
        }

        /*
         * And now we can format the value.
         */
        switch (*cp) {
            case 's':
            case 'I':
            case 'L':
                text_format_string_conversion(&str, *cp, &typoutputfinfo, value, isNull, flags, width);
                break;
            default:
                /* should not get here, because of previous check */
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("unrecognized conversion specifier \"%c\"", *cp)));
                break;
        }
    }

    /* Don't need deconstruct_array results anymore. */
    if (elements != NULL)
        pfree_ext(elements);
    if (nulls != NULL)
        pfree_ext(nulls);

    /* Generate results. */
    result = cstring_to_text_with_len(str.data, str.len);
    pfree_ext(str.data);

    if ((result == NULL || VARSIZE_ANY_EXHDR(result) == 0) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
        PG_RETURN_NULL();
    else
        PG_RETURN_TEXT_P(result);
}

/*
 * Parse contiguous digits as a decimal number.
 *
 * Returns true if some digits could be parsed.
 * The value is returned into *value, and *ptr is advanced to the next
 * character to be parsed.
 *
 * Note parsing invariant: at least one character is known available before
 * string end (end_ptr) at entry, and this is still true at exit.
 */
static bool text_format_parse_digits(const char** ptr, const char* end_ptr, int* value)
{
    bool found = false;
    const char* cp = *ptr;
    int val = 0;

    while (*cp >= '0' && *cp <= '9') {
        int8 digit = (*cp - '0');

        if (pg_mul_s32_overflow(val, 10, &val) || pg_add_s32_overflow(val, digit, &val))
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("number is out of range")));
        ADVANCE_PARSE_POINTER(cp, end_ptr);
        found = true;
    }

    *ptr = cp;
    *value = val;

    return found;
}

/*
 * Parse a format specifier (generally following the SUS printf spec).
 *
 * We have already advanced over the initial '%', and we are looking for
 * [argpos][flags][width]type (but the type character is not consumed here).
 *
 * Inputs are start_ptr (the position after '%') and end_ptr (string end + 1).
 * Output parameters:
 *	argpos: argument position for value to be printed.  -1 means unspecified.
 *	widthpos: argument position for width.  Zero means the argument position
 *			was unspecified (ie, take the next arg) and -1 means no width
 *			argument (width was omitted or specified as a constant).
 *	flags: bitmask of flags.
 *	width: directly-specified width value.  Zero means the width was omitted
 *			(note it's not necessary to distinguish this case from an explicit
 *			zero width value).
 *
 * The function result is the next character position to be parsed, ie, the
 * location where the type character is/should be.
 *
 * Note parsing invariant: at least one character is known available before
 * string end (end_ptr) at entry, and this is still true at exit.
 */
static const char* text_format_parse_format(
    const char* start_ptr, const char* end_ptr, int* argpos, int* widthpos, int* flags, int* width)
{
    const char* cp = start_ptr;
    int n;

    /* set defaults for output parameters */
    *argpos = -1;
    *widthpos = -1;
    *flags = 0;
    *width = 0;

    /* try to identify first number */
    if (text_format_parse_digits(&cp, end_ptr, &n)) {
        if (*cp != '$') {
            /* Must be just a width and a type, so we're done */
            *width = n;
            return cp;
        }
        /* The number was argument position */
        *argpos = n;
        /* Explicit 0 for argument index is immediately refused */
        if (n == 0)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("format specifies argument 0, but arguments are numbered from 1")));
        ADVANCE_PARSE_POINTER(cp, end_ptr);
    }

    /* Handle flags (only minus is supported now) */
    while (*cp == '-') {
        *flags = (unsigned int)(*flags) | TEXT_FORMAT_FLAG_MINUS;
        ADVANCE_PARSE_POINTER(cp, end_ptr);
    }

    if (*cp == '*') {
        /* Handle indirect width */
        ADVANCE_PARSE_POINTER(cp, end_ptr);
        if (text_format_parse_digits(&cp, end_ptr, &n)) {
            /* number in this position must be closed by $ */
            if (*cp != '$')
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("width argument position must be ended by \"$\"")));
            /* The number was width argument position */
            *widthpos = n;
            /* Explicit 0 for argument index is immediately refused */
            if (n == 0)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("format specifies argument 0, but arguments are numbered from 1")));
            ADVANCE_PARSE_POINTER(cp, end_ptr);
        } else
            *widthpos = 0; /* width's argument position is unspecified */
    } else {
        /* Check for direct width specification */
        if (text_format_parse_digits(&cp, end_ptr, &n))
            *width = n;
    }

    /* cp should now be pointing at type character */
    return cp;
}

/*
 * Format a %s, %I, or %L conversion
 */
static void text_format_string_conversion(
    StringInfo buf, char conversion, FmgrInfo* typOutputInfo, Datum value, bool isNull, int flags, int width)
{
    char* str = NULL;

    /* Handle NULL arguments before trying to stringify the value. */
    if (isNull) {
        if (conversion == 's')
            text_format_append_string(buf, "", flags, width);
        else if (conversion == 'L')
            text_format_append_string(buf, "NULL", flags, width);
        else if (conversion == 'I')
            ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("null values cannot be formatted as an SQL identifier")));
        return;
    }

    /* Stringify. */
    str = OutputFunctionCall(typOutputInfo, value);

    /* Escape. */
    if (conversion == 'I') {
        /* quote_identifier may or may not allocate a new string. */
        text_format_append_string(buf, quote_identifier(str), flags, width);
    } else if (conversion == 'L') {
        char* qstr = quote_literal_cstr(str);

        text_format_append_string(buf, qstr, flags, width);
        /* quote_literal_cstr() always allocates a new string */
        pfree_ext(qstr);
    } else
        text_format_append_string(buf, str, flags, width);

    /* Cleanup. */
    pfree_ext(str);
}

/*
 * Append str to buf, padding as directed by flags/width
 */
static void text_format_append_string(StringInfo buf, const char* str, int flags, int width)
{
    bool align_to_left = false;
    int len;

    /* fast path for typical easy case */
    if (width == 0) {
        appendStringInfoString(buf, str);
        return;
    }

    if (width < 0) {
        /* Negative width: implicit '-' flag, then take absolute value */
        align_to_left = true;
        /* -INT_MIN is undefined */
        if (width <= INT_MIN)
            ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("number is out of range")));
        width = -width;
    } else if ((unsigned int)flags & TEXT_FORMAT_FLAG_MINUS)
        align_to_left = true;

    len = pg_mbstrlen(str);
    if (align_to_left) {
        /* left justify */
        appendStringInfoString(buf, str);
        if (len < width)
            appendStringInfoSpaces(buf, width - len);
    } else {
        /* right justify */
        if (len < width)
            appendStringInfoSpaces(buf, width - len);
        appendStringInfoString(buf, str);
    }
}

/*
 * text_format_nv - nonvariadic wrapper for text_format function.
 *
 * note: this wrapper is necessary to pass the sanity check in opr_sanity,
 * which checks that all built-in functions that share the implementing C
 * function take the same number of arguments.
 */
Datum text_format_nv(PG_FUNCTION_ARGS)
{
    return text_format(fcinfo);
}

// scan the source string forward
static int getResultPostion(text* textStr, text* textStrToSearch, int32 beginIndex, int occurTimes)
{
    int i = 0;
    int result = 0;
    int scanIndex = 0;
    TextPositionState state = {0};

    if (JUDGE_INPUT_VALID(textStr, textStrToSearch)) {
        return 0;
    }

    text_position_setup(textStr, textStrToSearch, &state);
    scanIndex = beginIndex;
    result = 0;

    for (i = 0; i < occurTimes; ++i) {
        result = text_position_next(scanIndex, &state);
        if (!result) {
            break;
        }

        scanIndex = result + 1;
    }

    text_position_cleanup(&state);

    return result;
}

// scan the source string backward
static int getResultPostionReverse(text* textStr, text* textStrToSearch, int32 beginIndex, int occurTimes)
{
    int result = 0;
    int len = 0;
    int count = 1;
    int retPos = 0;
    int scanPos = 0;
    TextPositionState state = {0};

    if (JUDGE_INPUT_VALID(textStr, textStrToSearch)) {
        return 0;
    }

    len = VARSIZE_ANY_EXHDR(textStr);

    text_position_setup(textStr, textStrToSearch, &state);

    for (scanPos = len + 1 + beginIndex; scanPos > 0; scanPos--) {
        retPos = text_position_next(scanPos, &state);

        /* finding new substring until position is different with last substring */
        if ((0 != retPos) && (scanPos == retPos)) {
            if (count >= occurTimes) {
                result = retPos;
                break;
            }

            count++;
        }
    }

    text_position_cleanup(&state);

    return result;
}

// start from beginIndex, find the first position of textStrToSearch in textStr
int text_instr_3args(text* textStr, text* textStrToSearch, int32 beginIndex)
{
    if (JUDGE_INPUT_VALID(textStr, textStrToSearch) || (0 == beginIndex))
        return 0;

    return text_instr_4args(textStr, textStrToSearch, beginIndex, 1);
}

// search from beginIndex,return the position of textStrToSearch when finding occurTimes in textStr
int text_instr_4args(text* textStr, text* textStrToSearch, int32 beginIndex, int occurTimes)
{
    int result = 0;
    int len = 0;
    int searchLen = 0;

    if (JUDGE_INPUT_VALID(textStr, textStrToSearch) || (0 == beginIndex)) {
        return 0;
    }

    len = VARSIZE_ANY_EXHDR(textStr);
    searchLen = VARSIZE_ANY_EXHDR(textStrToSearch);

    if ((searchLen > len) || (GET_POSITIVE(beginIndex) > len)) {
        return 0;
    }

    if (beginIndex > 0) {
        result = getResultPostion(textStr, textStrToSearch, beginIndex, occurTimes);
    } else { /* beginIndex<0, scan the source string backward */
        result = getResultPostionReverse(textStr, textStrToSearch, beginIndex, occurTimes);
    }

    return result;
}

// instr(varchar string, varchar string_to_search, integer beg_index)
// start from position beg_index, get the index of the first match of string_to_search in string
// character sets considered, different character sets, different characters in the number of bytes
// for example,a Chinese character stored in three bytes
Datum instr_3args(PG_FUNCTION_ARGS)
{
    text* text_str = PG_GETARG_TEXT_P(0);
    text* text_str_to_search = PG_GETARG_TEXT_P(1);
    int32 beg_index = PG_GETARG_INT32(2);

    return (Int32GetDatum(text_instr_3args(text_str, text_str_to_search, beg_index)));
}
Datum instr_4args(PG_FUNCTION_ARGS)
{
    text* text_str = PG_GETARG_TEXT_P(0);
    text* text_str_to_search = PG_GETARG_TEXT_P(1);
    int32 beg_index = PG_GETARG_INT32(2);
    int occur_index = PG_GETARG_INT32(3);

    return Int32GetDatum(text_instr_4args(text_str, text_str_to_search, beg_index, occur_index));
}

// adapt A db's empty_blob ()
Datum get_empty_blob(PG_FUNCTION_ARGS)
{
    bytea* result = NULL;
    int32 length = VARHDRSZ;

    result = (bytea*)palloc(length);
    SET_VARSIZE(result, VARHDRSZ);
    PG_RETURN_BYTEA_P(result);
}

// adapt A db's substrb(text str,integer start,integer length)
Datum substrb_with_lenth(PG_FUNCTION_ARGS)
{
    text* result = NULL;
    Datum str = PG_GETARG_DATUM(0);
    int32 start = PG_GETARG_INT32(1);
    int32 length = PG_GETARG_INT32(2);

    int32 total = 0;
    total = toast_raw_datum_size(str) - VARHDRSZ;
    if ((length < 0) || (total == 0) || (start > total) || (start + total < 0)) {
        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
            PG_RETURN_NULL();
        else {
            result = cstring_to_text("");
            PG_RETURN_TEXT_P(result);
        }
    }

    result = get_substring_really(str, start, length, false);
    if ((NULL == result || 0 == VARSIZE_ANY_EXHDR(result)) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
        PG_RETURN_NULL();
    PG_RETURN_TEXT_P(result);
}

// adapt A db's substr(text str,integer start)
Datum substrb_without_lenth(PG_FUNCTION_ARGS)
{
    text* result = NULL;
    Datum str = PG_GETARG_DATUM(0);
    int32 start = PG_GETARG_INT32(1);

    int32 total = 0;
    total = toast_raw_datum_size(str) - VARHDRSZ;
    if ((total == 0) || (start > total) || (start + total < 0)) {
        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
            PG_RETURN_NULL();
        else {
            result = cstring_to_text("");
            PG_RETURN_TEXT_P(result);
        }
    }

    result = get_substring_really(str, start, -1, true);
    if ((NULL == result || 0 == VARSIZE_ANY_EXHDR(result)) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
        PG_RETURN_NULL();
    PG_RETURN_TEXT_P(result);
}

static int32 tail_part_of_mbchar(const char* mbstr, int32 index)
{
    int i;
    int mblen;
    Assert(index > 0);
    for (i = 1; i < index; i += mblen) {
        mblen = pg_mblen(mbstr);
        mbstr += mblen;
    }
    return i - index;
}

static int32 front_part_of_mbchar(const char* mbstr, int32 index)
{
    int i;
    int mblen = pg_mblen(mbstr);
    Assert(index > 0);
    for (i = 1; i <= index; i += mblen) {
        mblen = pg_mblen(mbstr);
        mbstr += mblen;
    }
    return (mblen - (i - index) + 1) % mblen;
}

/*
 * This function does the real work for substrb_with_lenth() and substrb_without_lenth().
 */
static text* get_substring_really(Datum str, int32 start, int32 length, bool length_not_specified)
{
    text* ret = NULL;
    int32 total = toast_raw_datum_size(str) - VARHDRSZ;
    int32 start_pos = start;
    int32 end_pos;
    int32 len;
    text* slice = NULL;
    int i;
    int32 start_pos_filled_bytes;
    int32 end_pos_filled_bytes;
    errno_t rc = EOK;

    /* amend the start position and end position */
    if (start < 0)
        start_pos += total + 1;
    else if (0 == start)
        start_pos = 1;

    if (length_not_specified)
        end_pos = total;
    else
        end_pos = start_pos + length - 1 > total ? total : start_pos + length - 1;

    len = end_pos - start_pos + 1;
    if (0 == len)
        return cstring_to_text("");

    /*
     * If we're working with an untoasted source, no need to do an extra
     * copying step.
     */
    ret = (text*)palloc(VARHDRSZ + len);
    if (VARATT_IS_COMPRESSED(DatumGetPointer(str)) || VARATT_IS_EXTERNAL(DatumGetPointer(str)))
        slice = DatumGetTextPSlice(str, 0, end_pos);
    else
        slice = (text*)DatumGetPointer(str);

    SET_VARSIZE(ret, VARHDRSZ + len);
    rc = memcpy_s(VARDATA_ANY(ret), len, VARDATA_ANY(slice) + start_pos - 1, len);
    securec_check(rc, "\0", "\0");

    /* get the bytes of the uncomplete mbchar */
    start_pos_filled_bytes = tail_part_of_mbchar(VARDATA_ANY(slice), start_pos);
    end_pos_filled_bytes = length_not_specified ? 0 : front_part_of_mbchar(VARDATA_ANY(slice), end_pos);

    /* fill the uncomplete mbchar with blank */
    for (i = 0; i < start_pos_filled_bytes && start_pos + i <= end_pos; i++)
        ret->vl_dat[i] = ' ';
    for (i = 0; i < end_pos_filled_bytes && end_pos - i >= start_pos; i++)
        ret->vl_dat[len - i - 1] = ' ';

    if (slice != (text*)DatumGetPointer(str))
        pfree_ext(slice);

    return ret;
}

Datum float4_text(PG_FUNCTION_ARGS)
{
    float4 num = PG_GETARG_FLOAT4(0);
    char* tmp = NULL;
    Datum result;

    tmp = DatumGetCString(DirectFunctionCall1(float4out, Float4GetDatum(num)));
    result = DirectFunctionCall1(textin, CStringGetDatum(tmp));
    pfree_ext(tmp);

    PG_RETURN_DATUM(result);
}

Datum float8_text(PG_FUNCTION_ARGS)
{
    float8 num = PG_GETARG_FLOAT8(0);
    char* tmp = NULL;
    Datum result;

    tmp = DatumGetCString(DirectFunctionCall1(float8out, Float8GetDatum(num)));
    result = DirectFunctionCall1(textin, CStringGetDatum(tmp));

    pfree_ext(tmp);
    PG_RETURN_DATUM(result);
}
