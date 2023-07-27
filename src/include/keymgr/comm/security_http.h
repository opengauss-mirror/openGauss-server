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
 * security_http.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/comm/security_http.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef _KM_HTTP_H_
#define _KM_HTTP_H_

#include <curl/curl.h>
#include "keymgr/comm/security_httpscan.h"

#define HTTP_MIN_UNEXCEPTED_CODE 300

typedef struct {
    CURL *client;

    /* http config */
    int timeout;
    int sslverify;

    /* http request line (save for error reporting) */
    char *url;
    CURLoption method;
    /* http request header */
    struct curl_slist *reqhdrs;
    char *reqlen; /* store request header 'Content-Length: ...', it's necessary for some http server */
    const char *reqbody;

    ResScan *resscan;

    /* http response line */
    long state;
    /* http response header */
    char* reshdrs;
    /* http response body: we store them in 'resscan' */

    /* if use aksk, generate signature of http message */
    char *ak;
    char *sk;

    KmErr *err;
} HttpMgr;

HttpMgr *httpmgr_new(KmErr *errbuf);
void httpmgr_free(HttpMgr *mgr);

#define httpmgr_ak_set(http, akval) (http)->ak = km_strdup((akval))
#define httpmgr_sk_set(http, skval) (http)->sk = km_strdup((skval))

void httpmgr_set_req_line(HttpMgr *mgr, const char *url, CURLoption method, const char *cacert);
void httpmgr_set_req_header(HttpMgr *mgr, const char *header);
void httpmgr_set_req_body(HttpMgr *mgr, char *reqbody);
char *httpmgr_get_req_body_len(HttpMgr *http, char *reqbody);

void httpmgr_set_response(HttpMgr *mgr, char remain[3], const char *hdrkey);
void httpmgr_receive(HttpMgr *mgr);
#ifdef ENABLE_KM_DEBUG
#define httpmgr_set_output(mgr, file) resscan_set_output((mgr)->resscan, (file))
#endif

#define httpmgr_get_status(http) (http)->state
#define httpmgr_get_res_header(http) (http)->resscan->header
#define httpmgr_get_res_body(http) (http)->resscan->body

#endif