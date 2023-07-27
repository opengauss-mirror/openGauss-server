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
 * security_aksk.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/comm/security_aksk.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "keymgr/comm/security_aksk.h"
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <openssl/sha.h>
#include <openssl/hmac.h>
#include "keymgr/comm/security_error.h"
#include "keymgr/comm/security_encode.h"

void km_aksk_add_reqhdr(AkSkPara *para, const char *hdr)
{
    if (para->hdrcnt >= KM_MAX_HTTP_HDR_NUM - 1) {
        return;
    }
    para->reqhdrs[para->hdrcnt++] = hdr;
}

/*
 * input: http headers, e.g
 *     Header1: value1
 *       Header2: some white-space space at the header
 *     Header3: value3
 * output: snapshort of http headers, e.g
 *     header1;header2;header3
 */
void km_aksk_format_reqhdr(AkSkPara *para, char *buf, size_t bufsz)
{
    size_t i;
    size_t j;
    size_t bufuse = 0;
    const char *hdr;
    
    for (i = 0; i < para->hdrcnt; i++) {
        hdr = para->reqhdrs[i];
        for (; hdr[0] == ' '; hdr++) {};
        for (j = 0; j < strlen(hdr); j++) {
            if (bufuse >= bufsz) {
                break;
            } else if (hdr[j] == ' ' || hdr[j] == ':') {
                buf[bufuse++] = ';';
                break;
            } else {
                buf[bufuse++] = tolower(hdr[j]);
            }
        }
    }

    if (bufuse > 0) {
        buf[bufuse - 1] = '\0';
    }
}

/* please make sure the format of header is 'key: value', do not add any white-space before ':' */
int km_hdr_cmp_hook(const void *header1, const void *header2)
{
    const char *hdr1;
    const char *hdr2;
    char *key1 = NULL;
    char *key2 = NULL;
    size_t len1;
    size_t len2;
    int ret = -1;

    if (header1 == NULL || header2 == NULL) {
        return -1;
    }

    hdr1 = *(char **)header1;
    hdr2 = *(char **)header2;

    for (len1 = 0; len1 < strlen(hdr1); len1++) {
        if (hdr1[len1] == ':') {
            key1 = km_strndup(hdr1, len1);
            break;
        }
    }
    for (len2 = 0; len2 < strlen(hdr2); len2++) {
        if (hdr2[len2] == ':') {
            key2 = km_strndup(hdr2, len2);
            break;
        }
    }
    if (key1 != NULL && key2 != NULL) {
        ret = strcasecmp(key1, key2);
    }
    km_safe_free(key1);
    km_safe_free(key2);

    return ret;
}

AkSkSign km_aksk_sign(AkSkPara *para)
{
#define KM_TIME_HDR_SZ 60
#define KM_YEAR_UTC_OFF 1900
#define KM_AUTH_HDR_SZ 1024
#define KM_ALL_REQ_HDR 256
#define KM_TOTAL_BUF_SZ 2048
#define KM_SIGN_LEN 128

    AkSkSign sign = {0};
    time_t current;
    struct tm cur;
    errno_t rc;

    /* add http header: host */
    size_t hostsz = strlen("Host: __") + strlen(para->host);
    sign.host = (char *)km_alloc(hostsz);
    rc = sprintf_s(sign.host, hostsz, "Host: %s", para->host);
    km_securec_check_ss(rc, "", "");
    km_aksk_add_reqhdr(para, sign.host);

    /* add http header: date */
    sign.date = (char *)km_alloc(KM_TIME_HDR_SZ);
    current = time(NULL);
    if (gmtime_r(&current, &cur) == NULL) {
        return sign;
    }
    char fmtdate[KM_TIME_HDR_SZ] = {0};
    rc = sprintf_s(fmtdate, KM_TIME_HDR_SZ, "%04d%02d%02dT%02d%02d%02dZ",
        cur.tm_year + KM_YEAR_UTC_OFF, cur.tm_mon + 1, cur.tm_mday, cur.tm_hour, cur.tm_min, cur.tm_sec);
    km_securec_check_ss(rc, "", "");
    rc = sprintf_s(sign.date, KM_TIME_HDR_SZ, "X-Sdk-Date: %s", fmtdate);
    km_securec_check_ss(rc, "", "");
    km_aksk_add_reqhdr(para, sign.date);

    qsort(para->reqhdrs, para->hdrcnt, sizeof(const char *), km_hdr_cmp_hook);

    /*
     * httpbuf =
     *      method\n
     *      uri\n
     *      query\n
     *      header1\n
     *      header2\n
     *      \n
     *      header1:header2\n
     *      hex(sha(body))
     */
    char *httpbuf = (char *)km_alloc(KM_TOTAL_BUF_SZ);
    char *buf = httpbuf;
    size_t bufuse = 0;
    size_t i;
    size_t j;
    char hdrshot[KM_ALL_REQ_HDR];

    /* method */
    for (i = 0; i < strlen(para->method); i++) {
        buf[bufuse++] = para->method[i];
    }
    buf[bufuse++] = '\n';
    /* uri */
    bufuse += km_url_encode(para->uri, buf + bufuse, KM_TOTAL_BUF_SZ - bufuse);
    buf[bufuse++] = '\n';
    /* query, but we have no query, so skip it */
    buf[bufuse++] = '\n';
    /* all reqheaders */
    for (i = 0; i < para->hdrcnt; i++) {
        bool iskey = true;
        const char *hdr = para->reqhdrs[i];
        for (j = 0; j < strlen(hdr); j++) {
            if (hdr[j] == ' ') {
                continue;
            } else if (hdr[j] == ':') {
                iskey = false;
                buf[bufuse++] = ':';
            } else {
                buf[bufuse++] = iskey ? tolower(hdr[j]) : hdr[j];
            }
        }
        buf[bufuse++] = '\n';
    }
    buf[bufuse++] = '\n';
    /* snapshot of reqheaders  */
    km_aksk_format_reqhdr(para, hdrshot, KM_ALL_REQ_HDR);
    for (i = 0; i < strlen(hdrshot); i++) {
        buf[bufuse++] = hdrshot[i];
    }
    buf[bufuse++] = '\n';
    /* http body -> sha -> hex */
    bufuse += km_hex_sha(para->reqbody, strlen(para->reqbody), buf + bufuse, KM_TOTAL_BUF_SZ - bufuse);
    buf[bufuse] = '\0';

    /*
     * signin =
     *      SDK-HMAC-SHA256\n
     *      fmtdate\n
     *      hex(sha(httpbuf))
     * signout = HMAC(signin)
     * signouthex = hex(signout)
     */
    char bufsha[KM_SIGN_LEN];
    char signin[KM_SIGN_LEN];
    unsigned char signout[KM_SIGN_LEN];
    unsigned int signsz;
    km_hex_sha(buf, bufuse, bufsha, KM_SIGN_LEN);
    km_safe_free(httpbuf);
    rc = sprintf_s(signin, KM_SIGN_LEN, "SDK-HMAC-SHA256\n%s\n%s", fmtdate, bufsha);
    km_securec_check_ss(rc, "", "");
    HMAC(EVP_sha256(), para->sk, strlen(para->sk), (unsigned char *)signin, strlen(signin), signout, &signsz);
    KmStr signouthex = km_hex_encode({signout, signsz});
    if (signouthex.val == NULL) {
        return sign;
    }

    /* add http header: authorization */
    sign.signature = (char *)km_alloc(KM_AUTH_HDR_SZ);
    rc = sprintf_s(sign.signature, KM_AUTH_HDR_SZ,
        "Authorization: SDK-HMAC-SHA256 Access=%s, SignedHeaders=%s, Signature=%s", para->ak, hdrshot, signouthex.val);
    km_securec_check_ss(rc, "", "");
    km_safe_free(signouthex.val);
    km_aksk_add_reqhdr(para, sign.signature);

    return sign;
}