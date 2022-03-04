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
 * gs_char.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\gs_char.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#define ereport(a, b) return NULL;
#include "gs_char.h"
#include "varlena.h"
#include "libpq-int.h"

int pg_mbcharcliplen(const char *mbstr, int len, int limit);
int pg_mbcharcliplen_orig(const char *mbstr, int len, int limit);

/*
 * char_bin -
 * converts char style to binary array
 */
unsigned char *char_bin(const char *text, size_t *binary_size, const char *err_msg)
{
    unsigned char *binary = (unsigned char *)malloc(1);
    if (binary == NULL) {
        return NULL;
    }
    binary[0] = text[0];
    *binary_size = 1;
    return binary;
}

/*
 * char_bout -
 * converts binary array to char style
 */
char *char_bout(const unsigned char *binary, size_t size, size_t *result_size)
{
    if (size != 1) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported result size: %zu", size)));
    }

    char *text = (char *)malloc(1);
    if (text == NULL) {
        return NULL;
    }
    text[0] = (char)binary[0];
    *result_size = 1;
    return text;
}

/*
 * bytea_bin -
 * converts hex format/escaped style byte array to binary array
 */
unsigned char *bytea_bin(const char *text, size_t *binary_size, char *err_msg)
{
    return byteain(text, binary_size, err_msg);
}

/*
 * bytea_bout -
 * converts binary array to hex format/escaped style byte array
 */
char *bytea_bout(const unsigned char *binary, size_t size, size_t *result_size)
{
    return byteaout(binary, size, result_size);
}

unsigned char *fallback_bin(const char *text, size_t *binary_size)
{
    size_t text_size = strlen(text);
    unsigned char *binary = (unsigned char *)malloc(text_size + 1);
    if (binary == NULL) {
        return NULL;
    }
    errno_t rcs = EOK;
    rcs = memcpy_s(binary, text_size + 1, text, text_size);
    securec_check_c(rcs, "", "");
    binary[text_size] = '\0';
    *binary_size = text_size;
    return binary;
}

char *fallback_bout(const unsigned char *binary, size_t size, size_t *result_size)
{
    char *text = (char *)malloc(size + 1);
    if (text == NULL) {
        return NULL;
    }
    check_memcpy_s(memcpy_s(text, size + 1, binary, size));
    text[size] = '\0';
    *result_size = size;
    return text;
}

unsigned char *fallback_brestore(const unsigned char *binary, size_t size, size_t *result_size, const char *err_msg)
{
    unsigned char *result = (unsigned char *)malloc(size + 1);
    if (result == NULL) {
        return NULL;
    }
    check_memcpy_s(memcpy_s(result, size + 1, binary, size));
    result[size] = '\0';
    *result_size = size;
    return result;
}

unsigned char *varchar_badjust(unsigned char *binary, size_t *binary_size, int atttypmod, char *err_msg)
{
    size_t maxlen;
    size_t len = *binary_size;
    const char *s = (const char *)binary;

    maxlen = atttypmod - VARHDRSZ;

    if (atttypmod >= (int32)VARHDRSZ && len > maxlen) {
        /* Verify that extra characters are spaces, and clip them off */
        size_t mbmaxlen = pg_mbcharcliplen(s, len, maxlen);
        size_t j;

        for (j = mbmaxlen; j < len; j++) {
            if (s[j] != ' ') {
                libpq_free(binary);
                check_sprintf_s(
                    sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " value too long for type nvarchar2(%zu)\n", maxlen));
                return NULL;
            }
        }

        len = mbmaxlen;
        *binary_size = len;
    }
    return binary;
}

unsigned char *nvarchar2_badjust(unsigned char *binary, size_t *binary_size, int atttypmod, char *err_msg)
{
    size_t maxlen;
    size_t len = *binary_size;
    const char *s = (const char *)binary;

    maxlen = atttypmod - VARHDRSZ;

    if (atttypmod >= (int32)VARHDRSZ && len > maxlen) {
        /* Verify that extra characters are spaces, and clip them off */
        size_t mbmaxlen = pg_mbcharcliplen_orig(s, len, maxlen);
        size_t j;

        for (j = mbmaxlen; j < len; j++) {
            if (s[j] != ' ') {
                libpq_free(binary);
                check_sprintf_s(
                    sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " value too long for type nvarchar2(%zu)\n", maxlen));
                return NULL;
            }
        }

        len = mbmaxlen;
        *binary_size = len;
    }
    return binary;
}

unsigned char *bpchar_badjust(unsigned char *binary, size_t *binary_size, int atttypmod, char *err_msg)
{
    size_t maxlen;
    size_t len = *binary_size;
    const char *s = (const char *)binary;

    /* If typmod is -1 (or invalid), use the actual string length */
    if (atttypmod < (int32)VARHDRSZ) {
        maxlen = len;
    } else {
        maxlen = atttypmod - VARHDRSZ;
        if (len > maxlen) {
            /* Verify that extra characters are spaces, and clip them off */
            size_t mbmaxlen = pg_mbcharcliplen(s, len, maxlen);
            size_t j;

            /*
             * at this point, len is the actual BYTE length of the input
             * string, maxlen is the max number of CHARACTERS allowed for this
             * bpchar type, mbmaxlen is the length in BYTES of those chars.
             */
            for (j = mbmaxlen; j < len; j++) {
                if (s[j] != ' ') {
                    libpq_free(binary);
                    check_sprintf_s(
                        sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " value too long for type character(%zu)\n", maxlen));
                    return NULL;
                }
            }
            /*
             * Now we set maxlen to the necessary byte length, not the number
             * of CHARACTERS!
             */
            maxlen = len = mbmaxlen;
        }
    }
    binary = (unsigned char *)libpq_realloc(binary, *binary_size, maxlen);
    if (binary == NULL) {
        return NULL;
    }
    *binary_size = maxlen;
    /*
     * fill with spaces till max size
     */
    for (size_t i = len; i < maxlen; ++i) {
        binary[i] = ' ';
    }
    return binary;
}
