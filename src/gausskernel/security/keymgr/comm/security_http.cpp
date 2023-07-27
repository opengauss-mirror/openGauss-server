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
 * security_http.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/comm/security_http.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "keymgr/comm/security_http.h"
#include <stdio.h>
#include <pthread.h>
#include "keymgr/comm/security_utils.h"
#include "keymgr/comm/security_httpscan.h"
#include "keymgr/comm/security_aksk.h"

#define CHK_CURL(http, ret)                                                                                     \
    if ((ret) != CURLE_OK) {                                                                                    \
        km_err_msg((http)->err, "curl error, err code: %d, err message: %s", (ret), curl_easy_strerror((ret))); \
    }

/*
 * init curl once. see what the curl say:
 *      - "curl_global_init() should be invoked exactly once for each application"
 *      - "This function is not thread-safe!"
 */
static void httpmgr_init_curl(HttpMgr *http)
{
    static pthread_mutex_t initlock = PTHREAD_MUTEX_INITIALIZER;
    static bool initcurl = false;

    (void)pthread_mutex_lock(&initlock);
    if (!initcurl) {
        CURLcode ret = curl_global_init(CURL_GLOBAL_ALL);
        CHK_CURL(http, ret);
        initcurl = true;
    }
    (void)pthread_mutex_unlock(&initlock);
}

/*
 * implemente based on libcurl, support : requset line, requeset header, request body
 */
HttpMgr *httpmgr_new(KmErr *errbuf)
{
#define HTTP_TIME_OUT 15

    HttpMgr *http = (HttpMgr *)km_alloc_zero(sizeof(HttpMgr));
    if (http == NULL) {
        return NULL;
    }

    http->err = errbuf;

    httpmgr_init_curl(http);

    http->client = curl_easy_init();
    if (http->client == NULL) {
        km_free(http);
        return NULL;
    }

    http->resscan = resscan_new(http->err);
    if (http->resscan == NULL) {
        curl_easy_cleanup(http->client);
        km_free(http);
        return NULL;
    }
    http->timeout = HTTP_TIME_OUT;
    http->sslverify = 0;

    (void)curl_easy_setopt(http->client, CURLOPT_TIMEOUT, http->timeout);

    return http;
}

void httpmgr_free(HttpMgr *http)
{
    if (http == NULL) {
        return;
    }

    curl_easy_cleanup(http->client);

    km_safe_free(http->url);

    if (http->reqhdrs != NULL) {
        curl_slist_free_all(http->reqhdrs);
    }

    resscan_free(http->resscan);
    http->resscan = NULL;

    km_safe_free(http->reshdrs);
    km_safe_free(http->reqlen);

    km_safe_free(http->ak);
    km_safe_free(http->sk);

    km_free(http);
}

void httpmgr_set_req_line(HttpMgr *http, const char *url, CURLoption method, const char *cacert)
{
    bool setca;
    
    if (km_err_catch(http->err)) {
        return;
    }

    /* reset error buffer */
    if (url == NULL) {
        km_err_msg(http->err, "set url to null");
        return;
    }
    km_safe_free(http->url);

    http->url = km_strdup(url);
    if (http->url == NULL) {
        km_err_msg(http->err, "failed to copy and store url");
        return;
    }

    http->method = method;

    CURLcode ret = curl_easy_setopt(http->client, CURLOPT_HTTPPOST, http->method);
    CHK_CURL(http, ret);
    ret = curl_easy_setopt(http->client, CURLOPT_URL, http->url);
    CHK_CURL(http, ret);

    setca = (cacert != NULL) ? true : false;
    (void)curl_easy_setopt(http->client, CURLOPT_SSL_VERIFYPEER, (long)setca);
    (void)curl_easy_setopt(http->client, CURLOPT_SSL_VERIFYHOST, (long)setca);

    if (setca) {
        (void)curl_easy_setopt(http->client, CURLOPT_CAINFO, cacert);
    }

#ifdef ENABLE_KM_DEBUG
    resscan_output(http->resscan, "@request\n");
    resscan_output(http->resscan, url);
    resscan_output(http->resscan, "\n");
#endif
}

/*
 * you can call this function more than once.
 * we will free all headers at each call of httpmgr_receive()
 */
void httpmgr_set_req_header(HttpMgr *http, const char *header)
{
    if (km_err_catch(http->err)) {
        return;
    }

    if (header == NULL) {
        km_err_msg(http->err, "set http request header to null");
        return;
    }
    http->reqhdrs = curl_slist_append(http->reqhdrs, header);
    if (http->reqhdrs == NULL) {
        km_err_msg(http->err, "failed to set http request header");
    }

#ifdef ENABLE_KM_DEBUG
    resscan_output(http->resscan, header);
    resscan_output(http->resscan, "\n");
#endif
}

char *httpmgr_get_req_body_len(HttpMgr *http, char *reqbody)
{
#define REQ_LEN_BUF_SZ 64
    errno_t rc = 0;

    if (reqbody == NULL) {
        return NULL;
    }

    if (http->reqlen == NULL) {
        http->reqlen = (char *)km_alloc(REQ_LEN_BUF_SZ);
    }

    if (http->reqlen == NULL) {
        return NULL;
    }

    rc = sprintf_s(http->reqlen, REQ_LEN_BUF_SZ, "Content-Length: %lu", strlen(reqbody));
    km_securec_check_ss(rc, "", "");

    return http->reqlen;
}

/* called only once */
void httpmgr_set_req_body(HttpMgr *http, char *reqbody)
{
    if (km_err_catch(http->err)) {
        return;
    }
    if (reqbody == NULL) {
        km_err_msg(http->err, "set http requet body to null");
    }
    http->reqbody = reqbody;
    CURLcode ret = curl_easy_setopt(http->client, CURLOPT_POSTFIELDS, reqbody);
    CHK_CURL(http, ret);

#ifdef ENABLE_KM_DEBUG
    resscan_output(http->resscan, reqbody);
    resscan_output(http->resscan, "\n");
#endif
}

/*
 * remain[i] : if copy field in http response
 *      if 1: copy, and store in http->resscan
 *      if 0: do not copy, which means we will lose this field
 *
 * remain[0] : if copy response line
 * remain[1] : if copy response header
 * remain[2] : if copy response body
 */
void httpmgr_set_response(HttpMgr *http, char remain[3], const char *hdrkey)
{
    if (km_err_catch(http->err)) {
        return;
    }

    resscan_set_receive(http->resscan, remain, hdrkey);
}

static size_t http_res_hook_func(void *curline, size_t charsz, size_t curlinelen, void *scan)
{
    char *line = (char *)curline;
    resscan_line((ResScan *)scan, line, curlinelen);
    return charsz * curlinelen;
}

void httpmgr_aksk_sign(HttpMgr *http)
{
#define KM_HOST_LEN 64
    char *start;
    char host[KM_HOST_LEN] = {0};
    struct curl_slist *hdr;
    size_t i;

    if (km_err_catch(http->err)) {
        return;
    }

    if (http->ak == NULL || http->sk == NULL) {
        km_err_msg(http->err, "the ak or sk is null");
        return;
    }

    if (http->url == NULL || http->reqbody == NULL) {
        km_err_msg(http->err, "the http url or http request is null");
        return;
    }

    /* find host from url */
    start = strstr(http->url, "://");
    if (start == NULL) {
        km_err_msg(http->err, "failed to find '://' from url '%s'", http->url);
        return;
    }
    start += strlen("://");
    for (i = 0; start[i] != '/' && start[i] != '\0'; i++) {
        if (i == KM_HOST_LEN - 1) {
            km_err_msg(http->err, "the host name in url '%s' is too long, maximum is 64", http->url);
            return;
        }
        host[i] = start[i];
    }
    host[i] = '\0';

    if (strlen(host) < strlen("x.x")) {
        km_err_msg(http->err, "the host name in url '%s' is too short", http->url);
        return;
    }

    AkSkPara para = {
        http->ak,
        http->sk,

        "POST", /* only sopport post for now */
        host,
        start + i, /* uri */

        {0}, /* headers, set next */
        0,

        http->reqbody
    };

    for (hdr = http->reqhdrs; hdr != NULL; hdr = hdr->next) {
        km_aksk_add_reqhdr(&para, hdr->data);
    }

    AkSkSign sign = km_aksk_sign(&para);
    if (sign.signature == NULL) {
        km_err_msg(http->err, "failed to sign http request message of '%s'", http->url);
    } else {
        httpmgr_set_req_header(http, sign.host);
        httpmgr_set_req_header(http, sign.date);
        httpmgr_set_req_header(http, sign.signature);
    }

    km_safe_free(sign.host);
    km_safe_free(sign.date);
    km_safe_free(sign.signature);
}

/*
 * send http request message, and store response in http->resscan.
 */
void httpmgr_receive(HttpMgr *http)
{
    if (http->ak != NULL || http->sk != NULL) {
        httpmgr_aksk_sign(http);
    }

    /* make sure set http request field succeed */
    if (km_err_catch(http->err)) {
        return;
    }

    if (http->resscan == NULL) {
        km_err_msg(http->err, "please set http response receiver");
        return;
    }

    CURLcode ret = curl_easy_setopt(http->client, CURLOPT_HTTPHEADER, http->reqhdrs);
    CHK_CURL(http, ret);
    ret = curl_easy_setopt(http->client, CURLOPT_HEADER, 1);
    CHK_CURL(http, ret);
    ret = curl_easy_setopt(http->client, CURLOPT_WRITEFUNCTION, http_res_hook_func);
    CHK_CURL(http, ret);
    ret = curl_easy_setopt(http->client, CURLOPT_WRITEDATA, http->resscan);
    CHK_CURL(http, ret);
    ret = curl_easy_setopt(http->client, CURLOPT_FOLLOWLOCATION, 1);
    CHK_CURL(http, ret);

    /* check curl error */
    if (km_err_catch(http->err)) {
        curl_slist_free_all(http->reqhdrs);
        http->reqhdrs = NULL;
        return;
    }

#ifdef ENABLE_KM_DEBUG
    resscan_output(http->resscan, "@receive\n");
#endif
    ret = curl_easy_perform(http->client);
    CHK_CURL(http, ret);
    ret = curl_easy_getinfo(http->client, CURLINFO_RESPONSE_CODE, &http->state);
    CHK_CURL(http, ret);

    curl_slist_free_all(http->reqhdrs);
    http->reqhdrs = NULL;
    http->reqbody = NULL;
#ifdef ENABLE_KM_DEBUG
    resscan_finish(http->resscan);
#endif
}
