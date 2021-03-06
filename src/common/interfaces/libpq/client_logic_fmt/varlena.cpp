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
 * varlena.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\varlena.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#define ereport(a, b) return NULL;
#include <string>
#include "postgres_fe.h"
#include "nodes/pg_list.h"
#include "encode.h"
#include "libpq-int.h"

int pg_mblen(const char *mbstr);

typedef enum {
    BYTEA_OUTPUT_ESCAPE,
    BYTEA_OUTPUT_HEX
} ByteaOutputType;

THR_LOCAL int fe_bytea_output = BYTEA_OUTPUT_HEX; /* ByteaOutputType, but int for GUC enum */

#define IS_VAL_ESCAPE_CHAR(val) (((val[0]) == '\\') && ((val[1]) >= '0' && (val[1]) <= '3') && \
    ((val[2]) >= '0' && (val[2]) <= '7') && ((val[3]) >= '0' && (val[3]) <= '7'))
#define IS_TWO_BACKSLASH(val)   (((val[0]) == '\\') && ((val[1]) == '\\'))

/* ****************************************************************************
 * USER I/O ROUTINES                                                       *
 * *************************************************************************** */

#define VAL(CH) ((CH) - '0')
#define DIG(VAL) ((VAL) + '0')

/*
 * byteain - converts from printable representation of byte array
 *
 * Non-printable characters must be passed as '\nnn' (octal) and are
 * converted to internal form.  '\' must be passed as '\\'.
 * ereport(ERROR, ...) if bad form.
 *
 * BUGS: The input is scanned twice and the error checking of input is minimal.
 * 
 */
unsigned char *byteain(const char *inputText, size_t *binary_size, char *err_msg)
{
    const char *tp = NULL;
    unsigned char *rp = NULL;
    int bc;
    int cl;

    unsigned char *binary = NULL;
    /* Recognize hex input */
    if (inputText[0] == '\\' && inputText[1] == 'x') {
        size_t len = strlen(inputText);
        /* the first 2 is the length of '\\x', the second 2 is data length after hex decode */
        size_t binary_len = (len - 2) / 2;
        binary = (unsigned char *)malloc(binary_len);
        if (binary == NULL) {
            return NULL;
        }
        errno_t rc = EOK;
        rc = memset_s(binary, binary_len, 0, binary_len);
        securec_check_c(rc, "\0", "\0");
        /* 2 is the length of '\\x' */
        bc = hex_decode(inputText + 2, len - 2, binary);
        if (bc == -1) {
            free(binary);
            binary = NULL;
            return NULL;
        }
        *binary_size = bc;
        return binary;
    }

    /* Else, it's the traditional escaped style */
    bc = 0;
    tp = inputText;
    while (*tp != '\0') {
        if (tp[0] != '\\') {
            cl = pg_mblen(tp);
            tp += cl;
            bc += cl;
        } else if (IS_VAL_ESCAPE_CHAR(tp)) {
            tp += 4; /* 4 is first 4 bits */
            bc++;
        } else if (IS_TWO_BACKSLASH(tp)) {
            tp += 2; /* 2 is first 2 bits */
            bc++;
        } else {
            /* one backslash, not followed by another or ### valid octal */
            check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " invalid input syntax for type bytea\n"));
            return NULL;
        }
    }

    binary = (unsigned char *)malloc(bc);
    if (binary == NULL) {
        return NULL;
    }
    errno_t rc = EOK;
    rc = memset_s(binary, bc, 0, bc);
    securec_check_c(rc, "\0", "\0");
    *binary_size = bc;
    tp = inputText;
    rp = binary;
    while (*tp != '\0') {
        if (tp[0] != '\\') {
            cl = pg_mblen(tp);
            for (int i = 0; i < cl; i++) {
                *rp++ = *tp++;
            }
        } else if (IS_VAL_ESCAPE_CHAR(tp)) {
            const int bits_count = 3;
            bc = VAL(tp[1]);
            bc <<= bits_count;
            bc += VAL(tp[2]);
            bc <<= bits_count;
            *rp++ = bc + VAL(tp[3]);

            tp += 4; /* 4 is first 4 bits */
        } else if (IS_TWO_BACKSLASH(tp)) {
            *rp++ = '\\';
            tp += 2; /* 2 is first 2 bits */
        } else {
            /* We should never get here. The first pass should not allow it. */
            check_sprintf_s(sprintf_s(err_msg, MAX_ERRMSG_LENGTH, " invalid input syntax for type bytea\n"));
            if (binary != NULL) {
                free(binary);
                binary = NULL;
            }
            return NULL;
        }
    }

    return binary;
}

/*
 * byteaout - converts to printable representation of byte array
 *
 * In the traditional escaped format, non-printable characters are
 * printed as '\nnn' (octal) and '\' as '\\'.
 */
char *byteaout(const unsigned char *data, size_t size, size_t *result_size)
{
    char *rp = NULL;
    char *result = NULL;

    if (fe_bytea_output == BYTEA_OUTPUT_HEX) {
        /* Print hex format */
        result = (char *)malloc(size * 2 + 2);
        if (result == NULL) {
            return NULL;
        }
        errno_t rc = EOK;
        rc = memset_s(result, size * 2 + 2, 0, size * 2 + 2);
        securec_check_c(rc, "\0", "\0");
        rp = result;
        *rp++ = '\\';
        *rp++ = 'x';
        rp += hex_encode(data, size, rp);
    } else if (fe_bytea_output == BYTEA_OUTPUT_ESCAPE) {
        /* Print traditional escaped format */
        const char *vp = NULL;
        int len;
        int i;

        len = 0;
        vp = (const char *)data;
        for (i = size; i != 0; i--, vp++) {
            if (*vp == '\\') {
                len += 2;
            } else if ((unsigned char)*vp < 0x20 || (unsigned char)*vp > 0x7e) {
                len += 4;
            } else {
                len++;
            }
        }
        result = (char *)malloc(len);
        if (result == NULL) {
            return NULL;
        }
        errno_t rc = EOK;
        rc = memset_s(result, len, 0, len);
        securec_check_c(rc, "\0", "\0");
        rp = result;
        vp = (const char *)data;
        for (i = size; i != 0; i--, vp++) {
            if (*vp == '\\') {
                *rp++ = '\\';
                *rp++ = '\\';
            } else if ((unsigned char)*vp < 0x20 || (unsigned char)*vp > 0x7e) {
                unsigned int val; /* holds unprintable chars */

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
            errmsg("unrecognized bytea_output setting: %d", fe_bytea_output)));
    }

    *result_size = (rp - result);
    return result;
}
