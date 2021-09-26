/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * clientlogic_bytea.cpp
 *
 * IDENTIFICATION
 *	  src\common\backend\utils\adt\clientlogic_bytea.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#include "postgres.h"

#include <limits.h>

#include "access/hash.h"
#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "catalog/pg_collation.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "catalog/gs_column_keys.h"
#include "common/int.h"
#include "lib/hyperloglog.h"
#include "libpq/md5.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "port/pg_bswap.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/clientlogic_bytea.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "parser/parser.h"
#include "utils/int8.h"
#include "utils/sortsupport.h"
#include "utils/syscache.h"
#include "executor/node/nodeSort.h"
#define byteawithoutorderwithequalcol bytea
#define byteawithoutordercol bytea
#define JUDGE_INPUT_VALID(X, Y)		((NULL == (X)) || (NULL == (Y)))
#define GET_POSITIVE(X)				((X) > 0 ? (X) : ((-1) * (X)))
#define IS_VAL_ESCAPE_CHAR(val) (((val[0]) == '\\') && ((val[1]) >= '0' && (val[1]) <= '3') && \
    ((val[2]) >= '0' && (val[2]) <= '7') && ((val[3]) >= '0' && (val[3]) <= '7'))
#define IS_TWO_BACKSLASH(val)   (((val[0]) == '\\') && ((val[1]) == '\\'))
#define IS_INVALID_INPUT(val)   ((strlen(val)) != 0 && (strlen(val)) < 12 && \
    !((strlen(val)) == 2 && (val[0]) == '\\' && (val[1]) == 'x'))

/* GUC variable */
#define BYTEA_WITHOUT_ORDER_WITH_EQAL_COL_OUTPUT ENCRYPTEDCOL_OUTPUT_HEX
#define BYTEA_WITHOUT_ORDER_COL_OUTPUT ENCRYPTEDCOL_OUTPUT_HEX

typedef struct varlena unknown;
typedef struct varlena VarString;

typedef struct {
    bool use_wchar;     /* T if multibyte encoding */
    char *str1;         /* use these if not use_wchar */
    char *str2;         /* note: these point to original texts */
    pg_wchar *wstr1;    /* use these if use_wchar */
    pg_wchar *wstr2;    /* note: these are palloc'd */
    int len1;           /* string lengths in logical characters */
    int len2;           /* Skip table for Boyer-Moore-Horspool search algorithm: */
    int skiptablemask;  /* mask for ANDing with skiptable subscripts */
    int skiptable[256]; /* skip distance for given mismatched char */
} TextPositionState;

typedef struct {
    char *buf1; /* 1st string, or abbreviation original string buf */
    char *buf2; /* 2nd string, or abbreviation strxfrm() buf */
    int buflen1;
    int buflen2;
    int last_len1;     /* Length of last buf1 string/strxfrm() input */
    int last_len2;     /* Length of last buf2 string/strxfrm() blob */
    int last_returned; /* Last comparison result (cache) */
    bool cache_blob;   /* Does buf2 contain strxfrm() blob, etc? */
    bool collate_c;
    bool bpchar;      /* Sorting pbchar, not varchar/text/byteawithoutorderwithequalcol? */
    bool estimating;  /* true if estimating cardinality refer to NumericSortSupport */
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

#define DatumGetUnknownP(X) ((unknown *)PG_DETOAST_DATUM(X))
#define DatumGetUnknownPCopy(X) ((unknown *)PG_DETOAST_DATUM_COPY(X))
#define PG_GETARG_UNKNOWN_P(n) DatumGetUnknownP(PG_GETARG_DATUM(n))
#define PG_GETARG_UNKNOWN_P_COPY(n) DatumGetUnknownPCopy(PG_GETARG_DATUM(n))
#define PG_RETURN_UNKNOWN_P(x) PG_RETURN_POINTER(x)

static byteawithoutorderwithequalcol *byteawithoutorderwithequalcol_catenate(byteawithoutorderwithequalcol *t1,
    byteawithoutorderwithequalcol *t2);
static byteawithoutorderwithequalcol *byteawithoutorderwithequalcol_substring(Datum str, int S, int L,
    bool length_not_specified);
static byteawithoutorderwithequalcol *byteawithoutorderwithequalcol_substring_orclcompat(Datum str, int S, int L,
    bool length_not_specified);
static byteawithoutorderwithequalcol *byteawithoutorderwithequalcol_overlay(byteawithoutorderwithequalcol *t1,
    byteawithoutorderwithequalcol *t2, int sp, int sl);
static StringInfo makeStringAggState(FunctionCallInfo fcinfo);

/* ****************************************************************************
 * USER I/O ROUTINES                                                       *
 * *************************************************************************** */


#define VAL(CH) ((CH) - '0')
#define DIG(VAL) ((VAL) + '0')

/*
 * byteawithoutorderwithequalcolin         - converts from printable representation of byte array
 *
 * Non-printable characters must be passed as '\nnn' (octal) and are
 * converted to internal form.  '\' must be passed as '\\'.
 * ereport(ERROR, ...) if bad form.
 *
 * BUGS: The input is scanned twice and the error checking of input is minimal.
 * 
 */
const bool checkColumnSettingId(const Oid cek)
{
    return SearchSysCacheExists1(COLUMNSETTINGDISTID, ObjectIdGetDatum(cek));
}

const Oid getOidFromInputText(const char *inputText)
{
    if (inputText == NULL) {
        return 0;
    }
    char buf[2 * sizeof(Oid) + 1] = {
        inputText[6],
        inputText[7],
        inputText[4],
        inputText[5],
        inputText[2],
        inputText[3],
        inputText[0],
        inputText[1],
        '\0'
    };
    const int base = 16;
    return strtol(buf, NULL, base);
}

const void check_cek_oid(const char *inputText)
{
    char buf[3] = { /* 3 is to prevent subscript out of bounds */
        inputText[2], /* 2 is the 2nd bit */
        inputText[3], /* 3 is the 3rd bit */
        0
    };
    const int base = 16;
    size_t count = strtol(buf, NULL, base);
    for (size_t i = 0; i < count; i++) {
        const Oid cek = getOidFromInputText((inputText + 4));
        if (
#ifdef ENABLE_MULTIPLE_NODES
            IS_PGXC_COORDINATOR &&
#endif
            !checkColumnSettingId(cek))
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_ENCRYPTED_COLUMN_DATA), errmsg("cek with OID %u not found", cek)));
    }
}

Datum
byteawithoutordercolin(PG_FUNCTION_ARGS) 
{
    char *inputText = PG_GETARG_CSTRING(0);
    size_t len = strlen(inputText);
    /* 12 : the minimum length used to ensure the correct input parameters */
    if (IS_INVALID_INPUT(inputText)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for type byteawithoutordercol")));
    }
    int bc;
    byteawithoutordercol *result = NULL;

    char *temp = NULL;
    char *rp = NULL;
    int cl;
    /* Recognize hex input */
    if (inputText[0] == '\\' && inputText[1] == 'x') {
        if (len != 2) { /* 2: inputValue is '\x' */
            check_cek_oid(inputText);
        }

        bc = (len - 2) / 2 + VARHDRSZ; /* maximum possible length */
        result = (byteawithoutordercol *)palloc(bc);
        bc = hex_decode(inputText + 2, len - 2, VARDATA(result)); /* 2 is the length of '\\x' */
        SET_VARSIZE(result, bc + VARHDRSZ); /* actual length */

        PG_RETURN_ENCRYPTEDCOL_P(result);
    } else if (inputText[0] != '\0') {
        ereport(ERROR, (errcode(ERRCODE_INVALID_ENCRYPTED_COLUMN_DATA),
            errmsg("invalid input syntax for type byteawithoutordercol")));
    }
    bc = 0;
    temp = inputText;
    while (*temp != '\0') {
        if (temp[0] != '\\') {
            cl = pg_mblen(temp);
            temp += cl;
            bc += cl;
        } else if (IS_VAL_ESCAPE_CHAR(temp)) {
            temp += 4; /* 4 is first 4 bits */
            bc++;
        } else if (IS_TWO_BACKSLASH(temp)) {
            temp += 2; /* 2 is first 2 bits */
            bc++;
        } else {
            /* one backslash, not followed by another or ### valid octal */
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for type byteawithoutordercol")));
        }
    }

    bc += VARHDRSZ;

    result = (byteawithoutordercol *)palloc(bc);
    SET_VARSIZE(result, bc);

    temp = inputText;
    rp = VARDATA(result);
    while (*temp != '\0') {
        if (temp[0] != '\\') {
            cl = pg_mblen(temp);
            for (int i = 0; i < cl; i++) {
                *rp++ = *temp++;
            }
        } else if (IS_VAL_ESCAPE_CHAR(temp)) {
            const int escape_char_bits = 3;
            bc = VAL(temp[1]);
            bc <<= escape_char_bits;
            bc += VAL(temp[2]);
            bc <<= escape_char_bits;
            *rp++ = bc + VAL(temp[3]);

            temp += 4; /* 4 is bits of '\\000' ~ '\\377' */
        } else if (IS_TWO_BACKSLASH(temp)) {
            *rp++ = '\\';
            temp += 2; /* 2 is bits of two backslash */
        } else {
            // * We should never get here. The first pass should not allow it.
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for type byteawithoutordercol")));
        }
    }

    PG_RETURN_ENCRYPTEDCOL_P(result);
}

Datum byteawithoutordercolout(PG_FUNCTION_ARGS)
{
    byteawithoutordercol *vlena = PG_GETARG_ENCRYPTEDCOL_PP(0);
    char *result = NULL;
    char *rp = NULL;

    if (BYTEA_WITHOUT_ORDER_COL_OUTPUT == ENCRYPTEDCOL_OUTPUT_HEX) {
        /* Print hex format */
        rp = result = (char *)palloc(VARSIZE_ANY_EXHDR(vlena) * 2 + 2 + 1);
        *rp++ = '\\';
        *rp++ = 'x';
        rp += hex_encode(VARDATA_ANY(vlena), VARSIZE_ANY_EXHDR(vlena), rp);
    } else if (BYTEA_WITHOUT_ORDER_COL_OUTPUT == ENCRYPTEDCOL_OUTPUT_ESCAPE) {
        /* Print traditional escaped format */
        char *vp = NULL;
        int len;
        int i;

        len = 1; /* empty string has 1 char */
        vp = VARDATA_ANY(vlena);
        for (i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++) {
            if (*vp == '\\') {
                len += 2;
            } else if ((unsigned char)*vp < 0x20 || (unsigned char)*vp > 0x7e) {
                len += 4;
            } else {
                len++;
            }
        }
        rp = result = (char *)palloc(len);
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
            } else {
                *rp++ = *vp;
            }
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
            errmsg("unrecognized  BYTEA_WITHOUT_ORDER_COL_OUTPUT setting: %d", BYTEA_WITHOUT_ORDER_COL_OUTPUT)));
        rp = result = NULL; /* keep compiler quiet */
    }
    *rp = '\0';

    /* free memory if allocated by the toaster */
    PG_FREE_IF_COPY(vlena, 0);

    PG_RETURN_CSTRING(result);
}

Datum byteawithoutorderwithequalcolin(PG_FUNCTION_ARGS)
{
    char *inputValue = PG_GETARG_CSTRING(0);
    size_t len = strlen(inputValue);
    /* 12 : the minimum length used to ensure the correct input parameters */
    if (IS_INVALID_INPUT(inputValue)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for type byteawithoutorderwithequalcol")));
    }
    int bc;
    byteawithoutorderwithequalcol *result = NULL;

    char *tp = NULL;
    char *rp = NULL;
    int cl;
    /* Recognize hex input */
    if (inputValue[0] == '\\' && inputValue[1] == 'x') {
        if (len != 2) { /* 2: inputValue is '\x' */
            check_cek_oid(inputValue);
        }

        bc = (len - 2) / 2 + VARHDRSZ; /* maximum possible length */
        result = (byteawithoutorderwithequalcol *)palloc(bc);
        bc = hex_decode(inputValue + 2, len - 2, VARDATA(result));
        SET_VARSIZE(result, bc + VARHDRSZ); /* actual length */

        PG_RETURN_ENCRYPTEDCOL_P(result);
    } else if (inputValue[0] != '\0') {
        ereport(ERROR, (errcode(ERRCODE_INVALID_ENCRYPTED_COLUMN_DATA),
            errmsg("invalid input syntax for type byteawithoutorderwithequalcol")));
    }
    bc = 0;
    tp = inputValue;
    while (*tp != '\0') {
        if (tp[0] != '\\') {
            cl = pg_mblen(tp);
            tp += cl;
            bc += cl;
        } else if (IS_VAL_ESCAPE_CHAR(tp)) {
            tp += 4;
            bc++;
        } else if (IS_TWO_BACKSLASH(tp)) {
            tp += 2;
            bc++;
        } else {
            /* one backslash, not followed by another or ### valid octal */
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for type byteawithoutorderwithequalcol")));
        }
    }

    bc += VARHDRSZ;

    result = (byteawithoutorderwithequalcol *)palloc(bc);
    SET_VARSIZE(result, bc);

    tp = inputValue;
    rp = VARDATA(result);
    while (*tp != '\0') {
        if (tp[0] != '\\') {
            cl = pg_mblen(tp);
            for (int i = 0; i < cl; i++) {
                *rp++ = *tp++;
            }
        } else if (IS_VAL_ESCAPE_CHAR(tp)) {
            bc = VAL(tp[1]);
            bc <<= 3;
            bc += VAL(tp[2]);
            bc <<= 3;
            *rp++ = bc + VAL(tp[3]);

            tp += 4;
        } else if (IS_TWO_BACKSLASH(tp)) {
            *rp++ = '\\';
            tp += 2;
        } else {
            /* We should never get here. The first pass should not allow it. */
            ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                errmsg("invalid input syntax for type byteawithoutorderwithequalcol")));
        }
    }

    PG_RETURN_ENCRYPTEDCOL_P(result);
}

/*
 * byteawithoutorderwithequalcolout        - converts to printable representation of byte array
 *
 * In the traditional escaped format, non-printable characters are
 * printed as '\nnn' (octal) and '\' as '\\'.
 */
Datum byteawithoutorderwithequalcolout(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *vlena = PG_GETARG_ENCRYPTEDCOL_PP(0);
    char *result = NULL;
    char *rp = NULL;

    if (BYTEA_WITHOUT_ORDER_WITH_EQAL_COL_OUTPUT == ENCRYPTEDCOL_OUTPUT_HEX) {
        /* Print hex format */
        rp = result = (char *)palloc(VARSIZE_ANY_EXHDR(vlena) * 2 + 2 + 1);
        *rp++ = '\\';
        *rp++ = 'x';
        rp += hex_encode(VARDATA_ANY(vlena), VARSIZE_ANY_EXHDR(vlena), rp);
    } else if (BYTEA_WITHOUT_ORDER_WITH_EQAL_COL_OUTPUT == ENCRYPTEDCOL_OUTPUT_ESCAPE) {
        /* Print traditional escaped format */
        char *vp = NULL;
        int len;
        int i;

        len = 1; /* empty string has 1 char */
        vp = VARDATA_ANY(vlena);
        for (i = VARSIZE_ANY_EXHDR(vlena); i != 0; i--, vp++) {
            if (*vp == '\\') {
                len += 2;
            } else if ((unsigned char)*vp < 0x20 || (unsigned char)*vp > 0x7e) {
                len += 4;
            } else {
                len++;
            }
        }
        rp = result = (char *)palloc(len);
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
            } else {
                *rp++ = *vp;
            }
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
            errmsg("unrecognized BYTEA_WITHOUT_ORDER_WITH_EQAL_COL_OUTPUT setting: %d",
                BYTEA_WITHOUT_ORDER_WITH_EQAL_COL_OUTPUT)));
        rp = result = NULL; /* keep compiler quiet */
    }
    *rp = '\0';

    /* free memory if allocated by the toaster */
    PG_FREE_IF_COPY(vlena, 0);

    PG_RETURN_CSTRING(result);
}

/*
 * byteawithoutorderwithequalcolrecv           - converts external binary format to byteawithoutorderwithequalcol
 */
Datum byteawithoutorderwithequalcolrecv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    byteawithoutorderwithequalcol *result = NULL;
    int nbytes;

    nbytes = buf->len - buf->cursor;
    result = (byteawithoutorderwithequalcol *)palloc(nbytes + VARHDRSZ);
    SET_VARSIZE(result, nbytes + VARHDRSZ);
    pq_copymsgbytes(buf, VARDATA(result), nbytes);
    PG_RETURN_ENCRYPTEDCOL_P(result);
}

/*
 * byteawithoutorderwithequalcolsend           - converts byteawithoutorderwithequalcol to binary format
 *
 * This is a special case: just copy the input...
 */
Datum byteawithoutorderwithequalcolsend(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *vlena = PG_GETARG_ENCRYPTEDCOL_P_COPY(0);

    PG_RETURN_ENCRYPTEDCOL_P(vlena);
}

/*
 * byteawithoutordercolrecv - converts external binary format to byteawithoutordercol
 */
Datum byteawithoutordercolrecv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo)PG_GETARG_POINTER(0);
    byteawithoutordercol *result = NULL;
    int nbytes = buf->len - buf->cursor;
    result = (byteawithoutordercol *)palloc(nbytes + VARHDRSZ);
    SET_VARSIZE(result, nbytes + VARHDRSZ);
    pq_copymsgbytes(buf, VARDATA(result), nbytes);
    PG_RETURN_ENCRYPTEDCOL_P(result);
}

/*
 * byteawithoutordercolsend - converts byteawithoutordercol to binary format
 *
 * This is a special case: just copy the input...
 */
Datum byteawithoutordercolsend(PG_FUNCTION_ARGS)
{
    byteawithoutordercol *vlena = (byteawithoutordercol *)PG_GETARG_ENCRYPTEDCOL_P_COPY(0);

    PG_RETURN_ENCRYPTEDCOL_P(vlena);
}

Datum byteawithoutorderwithequalcol_string_agg_transfn(PG_FUNCTION_ARGS)
{
    StringInfo state;

    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);

    /* Append the value unless null. */
    if (!PG_ARGISNULL(1)) {
        byteawithoutorderwithequalcol *value = PG_GETARG_ENCRYPTEDCOL_PP(1);

        /* On the first time through, we ignore the delimiter. */
        if (state == NULL)
            state = makeStringAggState(fcinfo);
        else if (!PG_ARGISNULL(2)) {
            byteawithoutorderwithequalcol *delim = PG_GETARG_ENCRYPTEDCOL_PP(2);

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

Datum byteawithoutorderwithequalcol_string_agg_finalfn(PG_FUNCTION_ARGS)
{
    StringInfo state;
    /* cannot be called directly because of internal-type argument */
    Assert(AggCheckCallContext(fcinfo, NULL));
    state = PG_ARGISNULL(0) ? NULL : (StringInfo)PG_GETARG_POINTER(0);
    if (state != NULL) {
        byteawithoutorderwithequalcol *result = NULL;

        result = (byteawithoutorderwithequalcol *)palloc(state->len + VARHDRSZ);
        SET_VARSIZE(result, state->len + VARHDRSZ);
        errno_t rc = EOK;
        rc = memcpy_s(VARDATA(result), state->len, state->data, state->len);
        securec_check(rc, "\0", "\0");
        PG_RETURN_ENCRYPTEDCOL_P(result);
    } else
        PG_RETURN_NULL();
}

/* ========== PUBLIC ROUTINES ========== */

/*
 * The internal realization of function vtextne.
 */
template<bool m_const1, bool m_const2>
static void vtextne_internal(ScalarVector *arg1, uint8 *pflags1, ScalarVector *arg2, uint8 *pflags2,
    ScalarVector *vresult, uint8 *pflagRes, Size len, text *targ, int idx)
{
    if (BOTH_NOT_NULL(pflags1[idx], pflags2[idx])) {
        Size len1 = m_const1 ? len : toast_raw_datum_size(arg1->m_vals[idx]);
        Size len2 = m_const2 ? len : toast_raw_datum_size(arg2->m_vals[idx]);
        if (len1 != len2)
            vresult->m_vals[idx] = BoolGetDatum(true);
        else {
            text *targ1 = m_const1 ? targ : DatumGetTextPP(arg1->m_vals[idx]);
            text *targ2 = m_const2 ? targ : DatumGetTextPP(arg2->m_vals[idx]);
            bool result = memcmp(VARDATA_ANY(targ1), VARDATA_ANY(targ2), len1 - VARHDRSZ) != 0;
            vresult->m_vals[idx] = BoolGetDatum(result);
        }

        /*
         * Since pflagRes can be resued by the other Batch, the pflagRes must be
         * set not null here if both two args are not null.
         */
        SET_NOTNULL(pflagRes[idx]);
    } else {
        SET_NULL(pflagRes[idx]);
    }
}

/* -------------------------------------------------------------
 * byteawithoutorderwithequalcoloctetlen
 *
 * get the number of bytes contained in an instance of type 'byteawithoutorderwithequalcol'
 * -------------------------------------------------------------
 */
Datum byteawithoutorderwithequalcoloctetlen(PG_FUNCTION_ARGS)
{
    Datum str = PG_GETARG_DATUM(0);

    /* We need not detoast the input at all */
    PG_RETURN_INT32(toast_raw_datum_size(str) - VARHDRSZ);
}

/*
 * byteawithoutorderwithequalcolcat -
 * takes two byteawithoutorderwithequalcol* and returns a byteawithoutorderwithequalcol* that is the concatenation of
 * the two.
 *
 * Cloned from textcat and modified as required.
 */
Datum byteawithoutorderwithequalcolcat(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *t1 = PG_GETARG_ENCRYPTEDCOL_PP(0);
    byteawithoutorderwithequalcol *t2 = PG_GETARG_ENCRYPTEDCOL_PP(1);

    PG_RETURN_ENCRYPTEDCOL_P(byteawithoutorderwithequalcol_catenate(t1, t2));
}

/*
 * byteawithoutorderwithequalcol_catenate
 * Guts of byteawithoutorderwithequalcolcat(), broken out so it can be used by other functions
 *
 * Arguments can be in short-header form, but not compressed or out-of-line
 */
static byteawithoutorderwithequalcol *byteawithoutorderwithequalcol_catenate(byteawithoutorderwithequalcol *t1,
    byteawithoutorderwithequalcol *t2)
{
    byteawithoutorderwithequalcol *result = NULL;
    int len1, len2, len;
    char *ptr = NULL;
    errno_t rc = EOK;

    len1 = VARSIZE_ANY_EXHDR(t1);
    len2 = VARSIZE_ANY_EXHDR(t2);

    /* paranoia ... probably should throw error instead? */
    if (len1 < 0) {
        len1 = 0;
    }
    if (len2 < 0) {
        len2 = 0;
    }

    len = len1 + len2 + VARHDRSZ;
    result = (byteawithoutorderwithequalcol *)palloc(len);

    /* Set size of result string... */
    SET_VARSIZE(result, len);

    /* Fill data field of result string... */
    ptr = VARDATA(result);
    if (len1 > 0) {
        rc = memcpy_s(ptr, len, VARDATA_ANY(t1), len1);
        securec_check(rc, "\0", "\0");
    }
    if (len2 > 0) {
        rc = memcpy_s(ptr + len1, len - len1, VARDATA_ANY(t2), len2);
        securec_check(rc, "\0", "\0");
    }

    return result;
}

#define PG_STR_GET_ENCRYPTEDCOL(str_) \
    DatumGetEncryptedcolP(DirectFunctionCall1(byteawithoutorderwithequalcolin, CStringGetDatum(str_)))

/*
 * byteawithoutorderwithequalcol_substr()
 * Return a substring starting at the specified position.
 * Cloned from text_substr and modified as required.
 *
 * Input: string / starting position (is one-based) / string length (optional)
 *
 * If the starting position is zero or less, then return from the start of the string
 * adjusting the length to be consistent with the "negative start" per SQL92.
 * If the length is less than zero, an ERROR is thrown. If no third argument
 * (length) is provided, the length to the end of the string is assumed.
 */
Datum byteawithoutorderwithequalcol_substr(PG_FUNCTION_ARGS)
{
    PG_RETURN_ENCRYPTEDCOL_P(
        byteawithoutorderwithequalcol_substring(PG_GETARG_DATUM(0), PG_GETARG_INT32(1), PG_GETARG_INT32(2), false));
}

/*
 * byteawithoutorderwithequalcol_substr_no_len -
 * Wrapper to avoid opr_sanity failure due to
 * one function accepting a different number of args.
 */
Datum byteawithoutorderwithequalcol_substr_no_len(PG_FUNCTION_ARGS)
{
    PG_RETURN_ENCRYPTEDCOL_P(byteawithoutorderwithequalcol_substring(PG_GETARG_DATUM(0), PG_GETARG_INT32(1), -1, true));
}

static byteawithoutorderwithequalcol *byteawithoutorderwithequalcol_substring(Datum str, int S, int L,
    bool length_not_specified)
{
    int S1; /* adjusted start position */
    int L1; /* adjusted substring length */

    S1 = Max(S, 1);

    if (length_not_specified) {
        /*
         * Not passed a length - DatumGetEncryptedcolPSlice() grabs everything to the
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
            return PG_STR_GET_ENCRYPTEDCOL("");

        L1 = E - S1;
    }

    /*
     * If the start position is past the end of the string, SQL99 says to
     * return a zero-length string -- DatumGetEncryptedcolPSlice() will do that for
     * us. Convert to zero-based starting position
     */
    return DatumGetEncryptedcolPSlice(str, S1 - 1, L1);
}

/*
 * adapt oracle's substr(byteawithoutorderwithequalcol str,integer start,integer length)
 * when start<0, amend the sartPosition to abs(start) from last char,
 * then search backward
 */
Datum byteawithoutorderwithequalcol_substr_orclcompat(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *result = NULL;
    Datum str = PG_GETARG_DATUM(0);
    int32 start = PG_GETARG_INT32(1);
    int32 length = PG_GETARG_INT32(2);

    int32 total = 0;

    total = toast_raw_datum_size(str) - VARHDRSZ;
    if ((length < 0) || (start > total) || (start + total < 0)) {
        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
            PG_RETURN_NULL();
        else {
            result = PG_STR_GET_ENCRYPTEDCOL("");
            PG_RETURN_ENCRYPTEDCOL_P(result);
        }
    }
    /* the param length_not_specified is false, the param length is used */
    result = byteawithoutorderwithequalcol_substring_orclcompat(str, start, length, false);
    if ((result == NULL || 0 == VARSIZE_ANY_EXHDR(result)) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
        PG_RETURN_NULL();
    else
        PG_RETURN_ENCRYPTEDCOL_P(result);
}

/*
 * adapt oracle's substr(byteawithoutorderwithequalcol x,integer y)
 * when start<0, amend the sartPosition to abs(start) from last char,
 * then search backward
 */
Datum byteawithoutorderwithequalcol_substr_no_len_orclcompat(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *result = NULL;
    Datum str = PG_GETARG_DATUM(0);
    int32 start = PG_GETARG_INT32(1);
    int32 total = 0;

    total = toast_raw_datum_size(str) - VARHDRSZ;
    if ((start > total) || (start + total < 0)) {
        if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
            PG_RETURN_NULL();
        else {
            result = PG_STR_GET_ENCRYPTEDCOL("");
            PG_RETURN_ENCRYPTEDCOL_P(result);
        }
    }
    /*
     * the param length_not_specified is true,
     * the param length is not used, and the length is set -1 as invalid flag
     */
    result = byteawithoutorderwithequalcol_substring_orclcompat(str, start, -1, true);
    if ((result == NULL || 0 == VARSIZE_ANY_EXHDR(result)) && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
        PG_RETURN_NULL();
    else
        PG_RETURN_ENCRYPTEDCOL_P(result);
}

/*
 * Does the real work for byteawithoutorderwithequalcol_substr_orclcompat() and
 * byteawithoutorderwithequalcol_substr_no_len_orclcompat(). The result is always a freshly palloc'd datum. when
 * length_not_specified==false, the param length is not used, usually called by text_substr_no_len_orclcompat(). when
 * length_not_specified==true, the param length is not used, sually called by text_substr_orclcompat()
 */
static byteawithoutorderwithequalcol *byteawithoutorderwithequalcol_substring_orclcompat(Datum str, int S, int L,
    bool length_not_specified)
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
    } else if (S == 0) {
        S = 1;
    }

    S1 = Max(S, 1);
    /* length_not_specified==true, the param length is not used */
    if (length_not_specified) {
        /*
         * Not passed a length - DatumGetEncryptedcolPSlice() grabs everything to the
         * end of the string if we pass it a negative value for length.
         */
        L1 = -1;
    } else {
        /* length_not_specified==false, the param length is used */
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
            return PG_STR_GET_ENCRYPTEDCOL("");

        L1 = E - S1;
    }

    /*
     * If the start position is past the end of the string, SQL99 says to
     * return a zero-length string -- DatumGetEncryptedcolPSlice() will do that for
     * us. Convert to zero-based starting position
     */
    return DatumGetEncryptedcolPSlice(str, S1 - 1, L1);
}

/*
 * byteawithoutorderwithequalcoloverlay
 * Replace specified substring of first string with second
 *
 * The SQL standard defines OVERLAY() in terms of substring and concatenation.
 * This code is a direct implementation of what the standard says.
 */
Datum byteawithoutorderwithequalcoloverlay(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *t1 = PG_GETARG_ENCRYPTEDCOL_PP(0);
    byteawithoutorderwithequalcol *t2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int sp = PG_GETARG_INT32(2); /* substring start position */
    int sl = PG_GETARG_INT32(3); /* substring length */

    PG_RETURN_ENCRYPTEDCOL_P(byteawithoutorderwithequalcol_overlay(t1, t2, sp, sl));
}

Datum byteawithoutorderwithequalcoloverlay_no_len(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *t1 = PG_GETARG_ENCRYPTEDCOL_PP(0);
    byteawithoutorderwithequalcol *t2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int sp = PG_GETARG_INT32(2); /* substring start position */
    int sl;

    sl = VARSIZE_ANY_EXHDR(t2); /* defaults to length(t2) */
    PG_RETURN_ENCRYPTEDCOL_P(byteawithoutorderwithequalcol_overlay(t1, t2, sp, sl));
}

static byteawithoutorderwithequalcol *byteawithoutorderwithequalcol_overlay(byteawithoutorderwithequalcol *t1,
    byteawithoutorderwithequalcol *t2, int sp, int sl)
{
    byteawithoutorderwithequalcol *result = NULL;
    byteawithoutorderwithequalcol *s1 = NULL;
    byteawithoutorderwithequalcol *s2 = NULL;
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

    s1 = byteawithoutorderwithequalcol_substring(PointerGetDatum(t1), 1, sp - 1, false);
    s2 = byteawithoutorderwithequalcol_substring(PointerGetDatum(t1), sp_pl_sl, -1, true);
    result = byteawithoutorderwithequalcol_catenate(s1, t2);
    result = byteawithoutorderwithequalcol_catenate(result, s2);

    return result;
}


/*
 * byteawithoutorderwithequalcolpos -
 * Return the position of the specified substring.
 * Implements the SQL92 POSITION() function.
 * Cloned from textpos and modified as required.
 */
Datum byteawithoutorderwithequalcolpos(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *t1 = PG_GETARG_ENCRYPTEDCOL_PP(0);
    byteawithoutorderwithequalcol *t2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
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
        };
        p1++;
    };

    PG_RETURN_INT32(pos);
}

/* -------------------------------------------------------------
 * byteawithoutorderwithequalcolGetByte
 *
 * this routine treats "byteawithoutorderwithequalcol" as an array of bytes.
 * It returns the Nth byte (a number between 0 and 255).
 * -------------------------------------------------------------
 */
Datum byteawithoutorderwithequalcolGetByte(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *v = PG_GETARG_ENCRYPTEDCOL_PP(0);
    int32 n = PG_GETARG_INT32(1);
    int len;
    int byte;
    len = VARSIZE_ANY_EXHDR(v);
    if (n < 0 || n >= len)
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("index %d out of valid range, 0..%d", n, len - 1)));

    byte = ((unsigned char *)VARDATA_ANY(v))[n];
    PG_RETURN_INT32(byte);
}

/* -------------------------------------------------------------
 * byteawithoutorderwithequalcolGetBit
 *
 * This routine treats a "byteawithoutorderwithequalcol" type like an array of bits.
 * It returns the value of the Nth bit (0 or 1).
 *
 * -------------------------------------------------------------
 */
Datum byteawithoutorderwithequalcolGetBit(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *v = PG_GETARG_ENCRYPTEDCOL_PP(0);
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
    byte = ((unsigned char *)VARDATA_ANY(v))[byteNo];
    if (byte & (1 << bitNo))
        PG_RETURN_INT32(1);
    else
        PG_RETURN_INT32(0);
}

/* -------------------------------------------------------------
 * byteawithoutorderwithequalcolSetByte
 *
 * Given an instance of type 'byteawithoutorderwithequalcol' creates a new one with
 * the Nth byte set to the given value.
 * -------------------------------------------------------------
 */
Datum byteawithoutorderwithequalcolSetByte(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *v = PG_GETARG_ENCRYPTEDCOL_P(0);
    int32 n = PG_GETARG_INT32(1);
    int32 newByte = PG_GETARG_INT32(2);
    int len;
    byteawithoutorderwithequalcol *res = NULL;
    errno_t rc = EOK;
    len = VARSIZE(v) - VARHDRSZ;
    if (n < 0 || n >= len)
        ereport(ERROR,
            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("index %d out of valid range, 0..%d", n, len - 1)));

    /*
     * Make a copy of the original varlena.
     */
    res = (byteawithoutorderwithequalcol *)palloc(VARSIZE(v));
    rc = memcpy_s((char *)res, VARSIZE(v), (char *)v, VARSIZE(v));
    securec_check(rc, "\0", "\0");
    /*
     * Now set the byte.
     */
    ((unsigned char *)VARDATA(res))[n] = newByte;

    PG_RETURN_ENCRYPTEDCOL_P(res);
}

/* -------------------------------------------------------------
 * byteawithoutorderwithequalcolSetBit
 *
 * Given an instance of type 'byteawithoutorderwithequalcol' creates a new one with
 * the Nth bit set to the given value.
 *
 * -------------------------------------------------------------
 */
Datum byteawithoutorderwithequalcolSetBit(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *v = PG_GETARG_ENCRYPTEDCOL_P(0);
    int32 n = PG_GETARG_INT32(1);
    int32 newBit = PG_GETARG_INT32(2);
    byteawithoutorderwithequalcol *res = NULL;
    int len;
    int oldByte, newByte;
    int byteNo, bitNo;
    errno_t rc = EOK;

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
    res = (byteawithoutorderwithequalcol *)palloc(VARSIZE(v));
    rc = memcpy_s((char *)res, VARSIZE(v), (char *)v, VARSIZE(v));
    securec_check(rc, "\0", "\0");

    /*
     * Update the byte.
     */
    oldByte = ((unsigned char *)VARDATA(res))[byteNo];

    if (newBit == 0)
        newByte = oldByte & (~(1 << bitNo));
    else
        newByte = oldByte | (1 << bitNo);

    ((unsigned char *)VARDATA(res))[byteNo] = newByte;

    PG_RETURN_ENCRYPTEDCOL_P(res);
}

/* ****************************************************************************
 * Comparison Functions used for byteawithoutorderwithequalcol
 *
 * Note: btree indexes need these routines not to leak memory; therefore,
 * be careful to free working copies of toasted datums.  Most places don't
 * need to be so careful.
 * *************************************************************************** */

Datum byteawithoutorderwithequalcoleq(PG_FUNCTION_ARGS)
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
        byteawithoutorderwithequalcol *barg1 = DatumGetEncryptedcolPP(arg1);
        byteawithoutorderwithequalcol *barg2 = DatumGetEncryptedcolPP(arg2);

        result = (memcmp(VARDATA_ANY(barg1), VARDATA_ANY(barg2), len1 - VARHDRSZ) == 0);

        PG_FREE_IF_COPY(barg1, 0);
        PG_FREE_IF_COPY(barg2, 1);
    }

    PG_RETURN_BOOL(result);
}

Datum byteawithoutorderwithequalcolne(PG_FUNCTION_ARGS)
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
        result = true;
    else {
        byteawithoutorderwithequalcol *barg1 = DatumGetEncryptedcolPP(arg1);
        byteawithoutorderwithequalcol *barg2 = DatumGetEncryptedcolPP(arg2);

        result = (memcmp(VARDATA_ANY(barg1), VARDATA_ANY(barg2), len1 - VARHDRSZ) != 0);

        PG_FREE_IF_COPY(barg1, 0);
        PG_FREE_IF_COPY(barg2, 1);
    }

    PG_RETURN_BOOL(result);
}

Datum byteawithoutorderwithequalcollt(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *arg1 = PG_GETARG_ENCRYPTEDCOL_PP(0);
    byteawithoutorderwithequalcol *arg2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp < 0) || ((cmp == 0) && (len1 < len2)));
}

Datum byteawithoutorderwithequalcolle(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *arg1 = PG_GETARG_ENCRYPTEDCOL_PP(0);
    byteawithoutorderwithequalcol *arg2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp < 0) || ((cmp == 0) && (len1 <= len2)));
}

Datum byteawithoutorderwithequalcolgt(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *arg1 = PG_GETARG_ENCRYPTEDCOL_PP(0);
    byteawithoutorderwithequalcol *arg2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp > 0) || ((cmp == 0) && (len1 > len2)));
}

Datum byteawithoutorderwithequalcolge(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *arg1 = PG_GETARG_ENCRYPTEDCOL_PP(0);
    byteawithoutorderwithequalcol *arg2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp > 0) || ((cmp == 0) && (len1 >= len2)));
}

Datum byteawithoutorderwithequalcolcmp(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *arg1 = PG_GETARG_ENCRYPTEDCOL_PP(0);
    byteawithoutorderwithequalcol *arg2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
    if ((cmp == 0) && (len1 != len2)) {
        cmp = (len1 < len2) ? -1 : 1;
    }

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_INT32(cmp);
}
Datum byteawithoutorderwithequalcoleqbytear(PG_FUNCTION_ARGS)
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
        byteawithoutorderwithequalcol *barg1 = DatumGetEncryptedcolPP(arg1);
        bytea *barg2 = DatumGetByteaPP(arg2);

        result = (memcmp(VARDATA_ANY(barg1), VARDATA_ANY(barg2), len1 - VARHDRSZ) == 0);

        PG_FREE_IF_COPY(barg1, 0);
        PG_FREE_IF_COPY(barg2, 1);
    }

    PG_RETURN_BOOL(result);
}

Datum byteawithoutorderwithequalcolnebytear(PG_FUNCTION_ARGS)
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
        result = true;
    else {
        byteawithoutorderwithequalcol *barg1 = DatumGetEncryptedcolPP(arg1);
        bytea *barg2 = DatumGetByteaPP(arg2);

        result = (memcmp(VARDATA_ANY(barg1), VARDATA_ANY(barg2), len1 - VARHDRSZ) != 0);

        PG_FREE_IF_COPY(barg1, 0);
        PG_FREE_IF_COPY(barg2, 1);
    }

    PG_RETURN_BOOL(result);
}

Datum byteawithoutorderwithequalcolltbytear(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *arg1 = PG_GETARG_ENCRYPTEDCOL_PP(0);
    bytea *arg2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp < 0) || ((cmp == 0) && (len1 < len2)));
}

Datum byteawithoutorderwithequalcollebytear(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *arg1 = PG_GETARG_ENCRYPTEDCOL_PP(0);
    bytea *arg2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp < 0) || ((cmp == 0) && (len1 <= len2)));
}

Datum byteawithoutorderwithequalcolgtbytear(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *arg1 = PG_GETARG_ENCRYPTEDCOL_PP(0);
    bytea *arg2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp > 0) || ((cmp == 0) && (len1 > len2)));
}

Datum byteawithoutorderwithequalcolgebytear(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *arg1 = PG_GETARG_ENCRYPTEDCOL_PP(0);
    bytea *arg2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp > 0) || ((cmp == 0) && (len1 >= len2)));
}

Datum byteawithoutorderwithequalcolcmpbytear(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *arg1 = PG_GETARG_ENCRYPTEDCOL_PP(0);
    bytea *arg2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
    if ((cmp == 0) && (len1 != len2)) {
        cmp = (len1 < len2) ? -1 : 1;
    }

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_INT32(cmp);
}

Datum byteawithoutorderwithequalcoleqbyteal(PG_FUNCTION_ARGS)
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
        bytea *barg1 = DatumGetByteaPP(arg1);
        byteawithoutorderwithequalcol *barg2 = DatumGetEncryptedcolPP(arg2);

        result = (memcmp(VARDATA_ANY(barg1), VARDATA_ANY(barg2), len1 - VARHDRSZ) == 0);

        PG_FREE_IF_COPY(barg1, 0);
        PG_FREE_IF_COPY(barg2, 1);
    }

    PG_RETURN_BOOL(result);
}

Datum byteawithoutorderwithequalcolnebyteal(PG_FUNCTION_ARGS)
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
        result = true;
    else {
        bytea *barg1 = DatumGetByteaPP(arg1);
        byteawithoutorderwithequalcol *barg2 = DatumGetEncryptedcolPP(arg2);

        result = (memcmp(VARDATA_ANY(barg1), VARDATA_ANY(barg2), len1 - VARHDRSZ) != 0);

        PG_FREE_IF_COPY(barg1, 0);
        PG_FREE_IF_COPY(barg2, 1);
    }

    PG_RETURN_BOOL(result);
}

Datum byteawithoutorderwithequalcolltbyteal(PG_FUNCTION_ARGS)
{
    bytea *arg1 = PG_GETARG_BYTEA_PP(0);
    byteawithoutorderwithequalcol *arg2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp < 0) || ((cmp == 0) && (len1 < len2)));
}

Datum byteawithoutorderwithequalcollebyteal(PG_FUNCTION_ARGS)
{
    bytea *arg1 = PG_GETARG_BYTEA_PP(0);
    byteawithoutorderwithequalcol *arg2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp < 0) || ((cmp == 0) && (len1 <= len2)));
}

Datum byteawithoutorderwithequalcolgtbyteal(PG_FUNCTION_ARGS)
{
    bytea *arg1 = PG_GETARG_BYTEA_PP(0);
    byteawithoutorderwithequalcol *arg2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp > 0) || ((cmp == 0) && (len1 > len2)));
}

Datum byteawithoutorderwithequalcolgebyteal(PG_FUNCTION_ARGS)
{
    bytea *arg1 = PG_GETARG_BYTEA_PP(0);
    byteawithoutorderwithequalcol *arg2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_BOOL((cmp > 0) || ((cmp == 0) && (len1 >= len2)));
}

Datum byteawithoutorderwithequalcolcmpbyteal(PG_FUNCTION_ARGS)
{
    bytea *arg1 = PG_GETARG_BYTEA_PP(0);
    byteawithoutorderwithequalcol *arg2 = PG_GETARG_ENCRYPTEDCOL_PP(1);
    int len1, len2;
    int cmp;

    len1 = VARSIZE_ANY_EXHDR(arg1);
    len2 = VARSIZE_ANY_EXHDR(arg2);

    cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
    if ((cmp == 0) && (len1 != len2)) {
        cmp = (len1 < len2) ? -1 : 1;
    }

    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    PG_RETURN_INT32(cmp);
}


Datum byteawithoutorderwithequalcol_sortsupport(PG_FUNCTION_ARGS)
{
    SortSupport ssup = (SortSupport)PG_GETARG_POINTER(0);
    MemoryContext oldcontext;

    oldcontext = MemoryContextSwitchTo(ssup->ssup_cxt);

    /* Use generic string SortSupport, forcing "C" collation */
    varstr_sortsupport(ssup, C_COLLATION_OID, false);

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_VOID();
}


#define REGEXP_REPLACE_BACKREF_CNT 10
#define HEXBASE 16
#define MD5_HASH_LEN 32

/*
 * Create an md5 hash of a byteawithoutorderwithequalcol field and return it as a hex string,
 * 16-byte md5 digest is represented in 32 hex characters.
 */
Datum md5_byteawithoutorderwithequalcol(PG_FUNCTION_ARGS)
{
    byteawithoutorderwithequalcol *in = PG_GETARG_ENCRYPTEDCOL_PP(0);
    size_t len;
    char hexsum[MD5_HASH_LEN + 1];

    len = VARSIZE_ANY_EXHDR(in);
    if (pg_md5_hash(VARDATA_ANY(in), len, hexsum) == false)
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

    PG_RETURN_TEXT_P(cstring_to_text(hexsum));
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
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
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

#define TEXT_FORMAT_FLAG_MINUS 0x0001
#define ADVANCE_PARSE_POINTER(ptr, end_ptr)                                                                           \
    do {                                                                                                              \
        if (++(ptr) >= (end_ptr))                                                                                     \
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), -errmsg("unterminated conversion specifier"))); \
    } while (0)

Datum byteawithoutorderwithequalcoltypmodin(PG_FUNCTION_ARGS)
{
    ArrayType *ta = PG_GETARG_ARRAYTYPE_P(0);

    int32 typmod;
    int32 *tl = NULL;
    int n;

    tl = ArrayGetIntegerTypmods(ta, &n);

    /*
     * we're not too tense about good error message here because grammar
     * shouldn't allow wrong number of modifiers for CHAR
     */
    if (n != 1)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid type modifier")));

    if ((tl == NULL) || (*tl == InvalidOid))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid Oid received in typmod")));

    /*
     * For largely historical reasons, the typmod is VARHDRSZ plus the number
     * of characters; there is enough client-side code that knows about that
     * that we'd better not change it.
     */
    typmod = *tl;

    return typmod;
}

Datum byteawithoutorderwithequalcoltypmodout(PG_FUNCTION_ARGS)
{
    static const int ec_typmod_buffer_len = 2 + log10(INT_MAX) + 1; /* 2 is for (), max size + 1 for \0 */
    int32 typmod = PG_GETARG_INT32(0);
    char *res = (char *)palloc(ec_typmod_buffer_len);
    errno_t ss_rc = 0;
    ss_rc = snprintf_s(res, ec_typmod_buffer_len, ec_typmod_buffer_len, "(%d)", (int)typmod);
    securec_check_ss(ss_rc, "\0", "\0");
    PG_RETURN_CSTRING(res);
}
