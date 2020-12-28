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
 * encode.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\encode.cpp
 *
 * -------------------------------------------------------------------------
 */
#define ereport(a, b) return -1;
#include "postgres_fe.h"
#include "nodes/pg_list.h"

/*
 * HEX
 */

static const char hextbl[] = "0123456789abcdef";

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

/* HEX encode */
unsigned hex_encode(const unsigned char *src, unsigned len, char *dst)
{
    const unsigned char *end = src + len;

    while (src < end) {
        *dst++ = hextbl[(*src >> 4) & 0xF]; /* 4 is get High four bits */
        *dst++ = hextbl[*src & 0xF];        /* get low four bits */
        src++;
    }
    return len * 2; /* 2 is because the length of hex is twice as much as binary */
}

static inline char get_hex(char c)
{
    int res = -1;

    if (c > 0 && c < 127) { /* 127 is the most of char c, except a sign bit */
        res = hexlookup[(unsigned char)c];
    }

    return (char)res;
}

/* HEX decode */
unsigned hex_decode(const char *src, unsigned len, unsigned char *dst)
{
    const char *s = NULL;
    const char *srcend;
    unsigned char v1, v2;
    unsigned char *p;

    srcend = src + len;
    s = src;
    p = dst;
    while (s < srcend) {
        if (*s == ' ' || *s == '\n' || *s == '\t' || *s == '\r') {
            s++;
            continue;
        }
        v1 = get_hex(*s++);
        if (v1 < 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid hexadecimal digit: \"%c\"", c)));
        }

        if (s >= srcend) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid hexadecimal data: odd number of digits")));
        }

        v2 = get_hex(*s++);
        if (v2 < 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid hexadecimal digit: \"%c\"", c)));
        }

        *p++ = (v1 << 4) | v2; /* 4 is four high bits */
    }

    return p - dst;
}
