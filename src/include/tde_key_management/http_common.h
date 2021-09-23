/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * http_common.h
 *    http request common utility
 *
 * IDENTIFICATION
 *    src/include/tde_key_management/http_common.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SEC_HTTP_COMMON_H
#define SEC_HTTP_COMMON_H

#include <stdio.h>
#include "cjson/cJSON.h"
#include "curl/curl.h"
#include "securec.h"
#include "tde_key_management/data_common.h"

typedef enum {
    /* http */
    TDE_HTTP_SUCCEED = 0,
    TDE_HTTP_UNKNOWN_ERR,
    /* cjson */
    TDE_CJSON_PARSE_ERR,
    TDE_FIND_CSJON_ERR,
    TDE_SET_CJSON_VALUE_ERR,
    /* curl */
    TDE_CURL_INIT_ERR,
    TDE_CURL_ERR,
    /* memory set */
    TDE_MALLOC_MEM_ERR,
    TDE_CHECK_BUF_LEN_ERR,
} HttpErrCode;

typedef enum {
    HTTP_OK = 200,
    HTTP_ACCEPT = 202,
    HTTP_NO_CONTENT = 204,
    HTTP_MULTIPLE_CHICES = 300,
    HTTP_BAD_REQUEST = 400,
    HTTP_UNAUTHORIZED = 401,
    HTTP_FORBIDDEN = 403,
    HTTP_NOT_FOUND = 404,
    HTTP_METHOD_NOT_ALLOWED = 405,
    HTTP_NOT_ACCEPTABLE = 406,
    HTTP_PROXY_AUTHENTICATION_REQUIRED = 407,
    HTTP_REQUEST_TIMEOUT = 408,
    HTTP_CONFLICT = 409,
    HTTP_INTERNAL_SERVER_ERROR = 500,
    HTTP_NOT_IMPLEMENTED = 501,
    HTTP_BAD_GATEWAY = 502,
    HTTP_SERVICE_UNAVAILABLE = 503,
    HTTP_GATEWAY_TIMEOUT = 504,
} HttpStatusCode;

typedef enum {
    HTTP_POST,
    HTTP_GET,
} HttpMethod;

typedef enum {
    HTTP_MSG = 0,
    HTTP_RESLINE,
    HTTP_RESHEADER,
    HTTP_RESBODY,
} HttpResListType;

typedef struct HttpReqMsg {
    /* request line */
    HttpMethod method;
    const char *url;
    char *version;
    /* request body */
    const char *body;
    /* request header */
    const char **header_list;
} HttpReqMsg;

typedef struct HttpResMsg {
    /* reponse line */
    char *version;
    HttpStatusCode *status_code;
    char *status_code_desc;
    /* reponse header */
    const char **header_list;
    /* reponse body */
    char *body;
} HttpResMsg;

typedef struct HttpConfig {
    int timeout;
    HttpResListType res_part;
} HttpConfig;

typedef struct HttpResStrList {
    const char *remote_server;
    HttpResListType filter_res_type;
    HttpResListType cur_pos;
    AdvStrList *str_list;
    size_t node_cnt;
    size_t loop_cnt;
    bool is_stop_dump;
} HttpResList;

class HttpCommon : public BaseObject {
public:
    static HttpErrCode http_request(HttpReqMsg* http_req_msg, HttpConfig *http_config, AdvStrList **http_res_list);
private:
    static HttpErrCode set_http_config(CURL *http_obj, HttpConfig *http_config);
    static HttpErrCode set_http_reqline(CURL *http_obj, HttpMethod method, const char *url);
    static HttpErrCode set_http_reqheader(CURL *http_obj, const char *http_heaser_list[], 
        struct curl_slist* ret_header_list);
    static HttpErrCode set_http_reqbody(CURL *http_obj, const char *http_body);
    static HttpErrCode get_http_resmsg(CURL *http_obj, HttpResListType res_part, AdvStrList **response_msg);
    static size_t dump_http_response_msg_callback(void *tmp_cur_res_str, size_t chr_size, size_t cur_res_size,
        void *tmp_http_res_list);
    static void check_curl_ret(CURLcode curl_ret);
};

#endif
