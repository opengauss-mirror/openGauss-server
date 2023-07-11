/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * nlssort.cpp
 * routines to support nlssort
 *
 * IDENTIFICATION
 * src/common/backend/utils/adt/nlssort.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "knl/knl_session.h"

#include "../mb/nlssort/nlssort_pinyin_map1_simple.map"
#include "../mb/nlssort/nlssort_pinyin_map1_complex.map"
#include "../mb/nlssort/nlssort_pinyin_map3.map"
#include "../mb/nlssort/nlssort_pinyin_map5.map"

bool check_nlssort_args(const char *argname, char **sort_method);

char *nlssort_pinyin(const char *chars_to_be_encoded, const char **sort_method);
char *remove_trailing_spaces(const char *src_str);
bool code_is_supported(uint32 code);
bool code_is_complex(uint32 code);
void append_str_simple(uint32 code, StringInfo chars_encoded_part, int part,
    const nlssort_encode_simple *map, int mapsize);
void append_str_complex(uint32 code, StringInfo chars_encoded_part,
    const nlssort_encode_complex *map, int mapsize);
static int compare_simple(const void *p1, const void *p2);
static int compare_complex(const void *p1, const void *p2);

/*
 * Find the corresponding encoding rule according to the second argument.
 * Return the code of the first argument under this encoding rule.
 *
 * Now only support "nls_sort=schinese_pinyin_m" and "nls_sort=generic_m_ci".
 */
Datum nlssort(PG_FUNCTION_ARGS)
{
    char *chars_to_be_encoded = NULL;
    char *nlssort_arg = NULL;
    char *sort_method = NULL;
    char *chars_encoded = NULL;

    if (PG_ARGISNULL(1)) {
        PG_RETURN_NULL();
    }

    FUNC_CHECK_HUGE_POINTER(PG_ARGISNULL(0), PG_GETARG_TEXT_P(0), "nlssort()");

    nlssort_arg = text_to_cstring(PG_GETARG_TEXT_P(1));
    if (check_nlssort_args(nlssort_arg, &sort_method)) {
        /* the first argument is null or "" */
        if (PG_ARGISNULL(0)) {
            PG_RETURN_NULL();
        }
        if (VARSIZE_ANY_EXHDR(PG_GETARG_TEXT_P(0)) == 0) {
            if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && !ACCEPT_EMPTY_STR) {
                PG_RETURN_NULL();
            } else {
                PG_RETURN_TEXT_P(cstring_to_text("\0"));
            }
        }

        /* encode the first argument with "gb18030" */
        Datum tmp = DirectFunctionCall3(pg_convert, PG_GETARG_DATUM(0), CStringGetDatum(GetDatabaseEncodingName()),
            CStringGetDatum("gb18030"));
        chars_to_be_encoded = text_to_cstring(DatumGetTextP(tmp));

        chars_encoded = (char *)nlssort_pinyin(chars_to_be_encoded, (const char **)&sort_method);
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_OPT),
            errmsg("Sort method %s is not supported!", nlssort_arg), errdetail("Not support the given sort method."),
            errcause("Error in the nlssort parameter."), erraction("Please check and revise your parameter.")));
    }

    pfree_ext(chars_to_be_encoded);
    pfree_ext(nlssort_arg);
    pfree_ext(sort_method);

    PG_RETURN_TEXT_P(cstring_to_text(chars_encoded));
}

/*
 * Check whether the nlssort argument is legal.
 *
 * Now only support "nls_sort=schinese_pinyin_m" and "nls_sort=generic_m_ci".
 */
bool check_nlssort_args(const char *argname, char **sort_method)
{
    List *sortlist = NULL;
    char *nlskey = NULL;
    char *nlsvalue = NULL;

    char *raw = pstrdup(argname);

    /*
     * Split string like: NLS_SORT=SCHINESE_PINYIN_M.
     * Converting strings to lowercase and removing back and forth spaces.
     */
    if (!SplitIdentifierString(raw, '=', &sortlist) || list_length(sortlist) != 2) {
        list_free(sortlist);
        FREE_POINTER(raw);
        return false;
    }

    nlskey = (char *)linitial(sortlist);
    if (strcmp(nlskey, "nls_sort")) {
        list_free(sortlist);
        FREE_POINTER(raw);
        return false;
    }

    nlsvalue = (char *)lsecond(sortlist);
    if (strcmp(nlsvalue, "schinese_pinyin_m") && strcmp(nlsvalue, "generic_m_ci")) {
        list_free(sortlist);
        FREE_POINTER(raw);
        return false;
    }

    *sort_method = pstrdup(nlsvalue);

    list_free(sortlist);
    FREE_POINTER(raw);

    return true;
}

/*
 * Encoding rules for "nls_sort=schinese_pinyin_m" && "nls_sort=generic_m_ci"
 *
 * The code corresponding to the given char/string can be divided into 5 parts.
 * 1. get it via pinyin_encode_map1; get empty string when not contains
 * 2. "0000"
 * 3. get it via pinyin_encode_map3; get empty string when not contains
 * 4. "00"
 * 5. get it via pinyin_encode_map5; get empty string when not contains
 * Concatenate the 5 parts to get the code (when the sort method is "nls_sort=generic_m_ci",
 * just concatenate the top 3 parts to get the code).
 *
 * preprocess:
 * 1. erase the trailing spaces in the given string
 * 2. when the given string is a pure space string, just leave one space
 */
char *nlssort_pinyin(const char *chars_to_be_encoded, const char **sort_method)
{
    int len, total_len;
    unsigned char *preprocessed_str, *preprocessed_str_copy;
    StringInfoData chars_encoded, chars_encoded_1, chars_encoded_3, chars_encoded_5;

    initStringInfo(&chars_encoded);
    initStringInfo(&chars_encoded_1);
    initStringInfo(&chars_encoded_3);
    initStringInfo(&chars_encoded_5);

    preprocessed_str = (unsigned char *)remove_trailing_spaces(chars_to_be_encoded);
    preprocessed_str_copy = preprocessed_str;

    uint32 code = 0;
    for (total_len = strlen((char *)preprocessed_str); total_len > 0; total_len -= len) {
        len = pg_encoding_mblen(PG_GB18030, (char *)preprocessed_str);
        if (len == 1) {
            code = *preprocessed_str++;
        } else if (len == 2) {
            code = *preprocessed_str++ << 8;
            code |= *preprocessed_str++;
        } else if (len == 4) {
            code = *preprocessed_str++ << 24;
            code |= *preprocessed_str++ << 16;
            code |= *preprocessed_str++ << 8;
            code |= *preprocessed_str++;
        } else {
            ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmodule(MOD_OPT),
                errmsg("Error in convert the first argument to gb18030 encoding!"),
                errdetail("A correct gb18030 character size should be 1, 2 or 4."),
                errcause("Error in the function \"pg_convert\"."), erraction("Please check and revise your input.")));
        }

        if (code_is_supported(code)) {
            /* select PINYIN_map1_S or PINYIN_map1_C according to the code */
            code_is_complex(code) ?
                append_str_complex(code, &chars_encoded_1, PINYIN_map1_C, lengthof(PINYIN_map1_C)) :
                append_str_simple(code, &chars_encoded_1, 1, PINYIN_map1_S, lengthof(PINYIN_map1_S));
            append_str_simple(code, &chars_encoded_3, 3, PINYIN_map3, lengthof(PINYIN_map3));
            append_str_simple(code, &chars_encoded_5, 5, PINYIN_map5, lengthof(PINYIN_map5));
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_OPT),
                errmsg("There is some char not supported with nlssort code!"),
                errdetail("Can't find the corresponding nlssort code in all the nlssort_pinyin_map*."),
                errcause("Invalid parameter value."), erraction("Please check and revise your parameter.")));
        }
    }

    appendStringInfo(&chars_encoded, "%s", chars_encoded_1.data);
    appendStringInfo(&chars_encoded, "%s", "0000");
    appendStringInfo(&chars_encoded, "%s", chars_encoded_3.data);

    /* the sort method is not "nls_sort=generic_m_ci" */
    if (strcmp(*sort_method, "schinese_pinyin_m") == 0) {
        appendStringInfo(&chars_encoded, "%s", "00");
        appendStringInfo(&chars_encoded, "%s", chars_encoded_5.data);
    }

    pfree_ext(preprocessed_str_copy);
    pfree_ext(chars_encoded_1.data);
    pfree_ext(chars_encoded_3.data);
    pfree_ext(chars_encoded_5.data);

    return chars_encoded.data;
}

/*
 * Erase the trailing spaces in the given string.
 * When the given string is a pure space string, just leave one space.
 *
 * "src_str" is known to be not null and strlen(src_str) > 0.
 */
char *remove_trailing_spaces(const char *src_str)
{
    bool is_all_space = true;
    int len = strlen(src_str);
    Assert(len > 0);
    int buf_size = strlen(src_str) + 1;
    char *dst_str = (char *)palloc(buf_size);

    /* check if all chars in "src_str"(known to be not null) is space */
    for (int i = strlen(src_str) - 1; i >= 0; i--) {
        if (src_str[i] != ' ') {
            is_all_space = false;
            break;
        }
    }

    if (unlikely(is_all_space)) {
        dst_str[0] = ' ';
        dst_str[1] = '\0';
    } else {
        while (len > 0 && src_str[len - 1] == ' ') {
            len--;
        }
        errno_t rc = strncpy_s(dst_str, buf_size, src_str, len);
        securec_check(rc, "\0", "\0");
        dst_str[len] = '\0';
    }

    return dst_str;
}

/*
 * Determine whether the code is supported (according to gb18030).
 * the code of gb18030 is 1 byte 2 bytes or 4 bytes.
 *
 *                1                   2                     3              4
 * 1 byte:   0x00 ~ 0x7F
 * 2 bytes:  0x81 ~ 0xFE  0x40 ~ 0x7E or 0x80 ~ 0xFE
 * 4 bytes:  0x81 ~ 0xFE         0x30 ~ 0x39           0x81 ~ 0xFE    0x30 ~ 0x39
 */
bool code_is_supported(uint32 code)
{
    bool res = false;

    if (code >= 0x0 && code <= 0x7F) {                               /* 1 byte */
        res = true;
    } else if (code >= 0x8140 && code <= 0xFEFE) {
        uint32 v1 = code & 0xFF;
        uint32 v2 = (code >> 4) & 0xF;
        if (v1 != 0x7F && v1 != 0xFF && (v2 >= 0x4 && v2 <= 0xF)) {  /* 2 bytes */
            res = true;
        }
    } else if ((code >= 0x81308130 && code <= 0x8431A439) ||
               (code >= 0x95328230 && code <= 0x9A348439)) {         /* 4 bytes */
        uint32 v1 = code & 0xFF;
        uint32 v2 = (code >> 8) & 0xFF;
        uint32 v3 = (code >> 16) & 0xFF;
        uint32 v4 = (code >> 12) & 0xF;
        if ((v1 >= 0x30 && v1 <= 0x39) && (v2 != 0x80 && v2 != 0xFF) && (v3 >= 0x30 && v3 <= 0x39) &&
            (v4 >= 0x8 && v4 <= 0xF)) {
            res = true;
        }
    }

    return res;
}

/*
 * Determine whether the code is complex.
 */
bool code_is_complex(uint32 code)
{
    bool res = false;
    static uint32 complex_code_arr[] = {0x81318334, 0x81318335, 0x81318336, 0x81318337, 0x81318338, 0x81319332,
        0x81319334, 0x81319531, 0x8131e131, 0x8131e132, 0x81328932, 0x81328935, 0x81328936, 0x81329038, 0x81329632,
        0x81329633, 0x81329634, 0x8132a238, 0x8132ae38, 0x8132af35, 0x8132af36, 0x8132af38, 0x8132bc36, 0x8132bc37,
        0x8132bc38, 0x8132cb30, 0x8132cb32, 0x8132cb34, 0x8132f339, 0x8132f431, 0x8132f533, 0x81338738, 0x8135a232,
        0x8135a234, 0x8135a236, 0x8135a238, 0x8135a330, 0x8135a334, 0x8135a735, 0x8135a737, 0x8135a830, 0x8135a831,
        0x8135a833};

    if (code >= 0x95328230 && code <= 0x9A3484391) {
        res = true;
    }

    for (unsigned int i = 0; i < sizeof(complex_code_arr) / sizeof(uint32); i++) {
        if (code == complex_code_arr[i]) {
            res = true;
            break;
        }
    }

    return res;
}

/*
 * Get the nlssort code (simple) according to the gb18030 code.
 * And append it to the correct part.
 */
void append_str_simple(uint32 code, StringInfo chars_encoded_part, int part,
    const nlssort_encode_simple *map, int mapsize)
{
    const nlssort_encode_simple *p = NULL;
    p = (nlssort_encode_simple *)bsearch(&code, map, mapsize, sizeof(nlssort_encode_simple), compare_simple);
    if (p != NULL) {
        if (part == 1) {
            if (p->nlssort_code & 0xFFFF0000) {
                appendStringInfo(chars_encoded_part, "%.8X", p->nlssort_code);
            } else {
                appendStringInfo(chars_encoded_part, "%.4X", p->nlssort_code);
            }
        } else {
            if (p->nlssort_code & 0xFF000000) {
                appendStringInfo(chars_encoded_part, "%.8X", p->nlssort_code);
            } else if (p->nlssort_code & 0xFF0000) {
                appendStringInfo(chars_encoded_part, "%.6X", p->nlssort_code);
            } else if (p->nlssort_code & 0xFF00) {
                appendStringInfo(chars_encoded_part, "%.4X", p->nlssort_code);
            } else {
                appendStringInfo(chars_encoded_part, "%.2X", p->nlssort_code);
            }
        }
    }
}

/*
 * Get the nlssort code (complex) according to the gb18030 code.
 * And append it to the correct part.
 */
void append_str_complex(uint32 code, StringInfo chars_encoded_part, const nlssort_encode_complex *map, int mapsize)
{
    const nlssort_encode_complex *p = NULL;
    p = (nlssort_encode_complex *)bsearch(&code, map, mapsize, sizeof(nlssort_encode_complex), compare_complex);
    if (p != NULL) {
        appendStringInfo(chars_encoded_part, "%.16lX", p->nlssort_code);
    }
}

/*
 * comparison routine for bsearch()
 * this routine is intended for gb18030 code -> nlssort code
 */
static int compare_simple(const void *p1, const void *p2)
{
    uint32 v1, v2;

    v1 = *(const uint32 *)p1;
    v2 = ((const nlssort_encode_simple *)p2)->gb18030_code;

    return (v1 > v2) ? 1 : ((v1 == v2) ? 0 : -1);
}

static int compare_complex(const void *p1, const void *p2)
{
    uint32 v1, v2;

    v1 = *(const uint32 *)p1;
    v2 = ((const nlssort_encode_complex *)p2)->gb18030_code;

    return (v1 > v2) ? 1 : ((v1 == v2) ? 0 : -1);
}
