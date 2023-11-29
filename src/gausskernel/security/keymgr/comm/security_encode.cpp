/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * security_encode.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/comm/security_encode.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "keymgr/comm/security_encode.h"
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include "keymgr/comm/security_error.h"
#include "keymgr/comm/security_utils.h"

#define HEX_CODE_LEN 2
#define HEX_SHIFT_LEN 4

static int hex_atoi(char hex)
{
    if (hex >= '0' && hex <= '9') {
        return hex - '0';
    } else if (hex >= 'a' && hex <= 'f') {
        return (hex - 'a') + 0xa;
    } else if (hex >= 'A' && hex <= 'F') {
        return (hex - 'A') + 0xa;
    }

    return 0;
}

KmStr km_hex_encode(KmUnStr str)
{
    size_t i;
    const char *hextbl = "0123456789abcdef";
    KmStr hex = {0};
    size_t hexlen = 0;

    hex.val = (char *)km_alloc(str.len * HEX_CODE_LEN + 1);
    if (hex.val == NULL) {
        return hex;
    }

    for (i = 0; i < str.len; i++) {
        hex.val[hexlen++] = hextbl[(str.val[i] & 0xf0) >> HEX_SHIFT_LEN];
        hex.val[hexlen++] = hextbl[str.val[i] & 0x0f];
    }
    hex.val[hexlen] = '\0';
    hex.len = hexlen;

    return hex;
}

KmUnStr km_hex_decode(KmStr hex)
{
    KmUnStr str = {0};
    size_t pos = 0;
    /* for bit operation, we need to use 'unsigned int' */
    unsigned int low;
    unsigned int high;

    str.val = (unsigned char *)km_alloc((size_t)hex.len / HEX_CODE_LEN + 1);
    if (str.val == NULL) {
        return str;
    }

    for (; pos < hex.len; pos += HEX_CODE_LEN) {
        high = (unsigned int)hex_atoi(hex.val[pos]);
        low = (unsigned int)hex_atoi(hex.val[pos + 1]);
        str.val[(int) pos / HEX_CODE_LEN] = ((high << HEX_SHIFT_LEN) & 0xff) ^ (low & 0xff);
    }

    str.val[(int) pos / HEX_CODE_LEN] = '\0';
    str.len = pos / HEX_CODE_LEN;

    return str;
}

/*
 * - in : 'str'
 * - out: 'str' + 'sha256'
 * - do : claculate sha256 of string, and append the sha256 to string
 */
#define SHA256_LEN 32
static KmUnStr sha_join(KmUnStr str)
{
    KmUnStr sha = {0};
    size_t i;

    sha.val = (unsigned char *)km_alloc(str.len + SHA256_LEN + 1);
    if (sha.val == NULL) {
        return sha;
    }

    for (i = 0; i < str.len; i++) {
        sha.val[i] = str.val[i];
    }

    (void)SHA256(str.val, str.len, sha.val + str.len);
    sha.len = str.len + SHA256_LEN;

    return sha;
}

static KmUnStr sha_strip(KmUnStr sha)
{
    unsigned char check[SHA256_LEN + 1];
    KmUnStr str = {0};
    size_t i;

    if (sha.len <= SHA256_LEN) {
        return str;
    }

    str.len = sha.len - SHA256_LEN;

    str.val = (unsigned char *)km_alloc(str.len + 1);
    if (str.val == NULL) {
        return str;
    }

    (void)SHA256(sha.val, str.len, check);
    for (i = 0; i < SHA256_LEN; i++) {
        if (sha.val[i + str.len] != check[i]) {
            (void)printf("check sha256 err\n");
            return str;
        }
    }

    for (i = 0; i < str.len; i++) {
        str.val[i] = sha.val[i];
    }

    return str;
}

KmStr km_sha_encode(KmUnStr str)
{
    KmUnStr sha = {0};
    KmStr shahex = {0};

    sha = sha_join(str);
    if (sha.val == NULL) {
        return shahex;
    }

    shahex = km_hex_encode(sha);
    km_safe_free(sha.val);

    return shahex;
}

KmUnStr km_sha_decode(KmStr shahex)
{
    KmUnStr sha = {0};
    KmUnStr str = {0};

    sha = km_hex_decode(shahex);
    if (sha.val == NULL) {
        return str;
    }

    str = sha_strip(sha);
    km_safe_free(sha.val);

    return str;
}

/* replace the special character with %* */
size_t km_url_encode(const char *in, char *out, size_t outsz)
{
    const char *legal = ".~_-/";
    const char *hextbl = "0123456789ABCDEF";
    size_t outuse = 0;

    for (size_t i = 0; i < strlen(in) && outuse < outsz; i++) {
        if (isalnum((unsigned char)in[i]) || strchr(legal, in[i]) != NULL) {
            out[outuse++] = in[i];
        } else {
            out[outuse++] = '%';
            out[outuse++] = hextbl[(unsigned char)in[i] >> HEX_SHIFT_LEN];
            out[outuse++] = hextbl[(unsigned char)in[i] & 0xf];
        }
    }

    if (out[outuse - 1] != '/') {
        out[outuse++] = '/';
    }

    return outuse;
}

size_t km_hex_sha(const char *data, size_t datalen, char *hexbuf, size_t bufsz)
{
#define KM_SHA256_LEN 32
    unsigned char sha[KM_SHA256_LEN];
    const char hextbl[] = "0123456789abcdef";
    size_t bufuse = 0;

    (void)SHA256((const unsigned char *)data, datalen, sha);

    for (size_t i = 0; i < KM_SHA256_LEN && bufuse < bufsz; i++) {
        hexbuf[bufuse++] = hextbl[(sha[i] & 0xf0) >> HEX_SHIFT_LEN];
        hexbuf[bufuse++] = hextbl[sha[i] & 0x0f];
    }
    hexbuf[bufuse] = '\0';
    return bufuse;
}