
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
 * cmkem_comm_http.h
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/cmkem_comm_http.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CMKEM_COMM_HTTP_H
#define CMKEM_COMM_HTTP_H

#include "cmkem_comm.h"
#include "cjson/cJSON.h"
#include "curl/curl.h"

const int MAX_URL_BUF_LEN = 256;

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

/* 
 * it's hard to accept the param order of this struct HttpReqMsg is different from that of HTTP message
 * however, only put the string array(param header_list) in the tail of the struct, can it be initialized
 */
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
    HttpStatusCode status_code;

    /* reponse header */
    CmkemStrList *header_list;

    /* reponse body */
    char *body;
} HttpResMsg;

typedef struct HttpConfig {
    int timeout;
    HttpResListType res_part;
} HttpConfig;

/* 
 * the http response message include (1 response line (2 response header (3 response body 
 * @ param filter_res_type - only dump messages of the specified type
 * @ param cur_pos - record current position of http resonse message
 * @ param str_list - dump all message to string list 
 */
typedef struct HttpResStrList {
    const char *remote_server;
    HttpResListType filter_res_type;
    HttpResListType cur_pos;
    CmkemStrList *str_list;
    size_t node_cnt;
    bool is_stop_dump;

    size_t loop_cnt;
} HttpResList;

typedef struct {
    const char *src_value;
    const char *dest_value;
} ReplaceJsonTempValue;

#define check_curl_ret(curl_ret)                                                                                    \
    do {                                                                                                            \
        if ((curl_ret) != CURLE_OK) {                                                                               \
            cmkem_errmsg("curl error. err code: '%lu', err msg: '%s.", (curl_ret), curl_easy_strerror((curl_ret))); \
            return CMKEM_CURL_ERR;                                                                                  \
        }                                                                                                           \
    } while (0)

extern CmkemErrCode traverse_jsontree_with_raplace_value(cJSON *json_tree, ReplaceJsonTempValue replace_rules[],
    size_t rule_cnt);
extern CmkemErrCode traverse_jsontree_with_get_value(cJSON *json_tree, const char **key_list, CmkemStrList *value_list);
extern CmkemErrCode set_http_config(CURL *http_obj, HttpConfig *http_config);
extern CmkemErrCode set_http_reqline(CURL *http_obj, HttpMethod method, const char *url, char *version);
extern CmkemErrCode set_http_reqheader(CURL *http_obj, const char *http_heaser_list[],
    struct curl_slist* ret_header_list);
extern CmkemErrCode set_http_reqbody(CURL *http_obj, const char *http_body);
extern CmkemErrCode get_http_resmsg(CURL *http_obj, HttpResListType res_part, CmkemStrList **response_msg,
    long *stat_code);
extern CmkemErrCode http_request(HttpReqMsg* http_req_msg, HttpConfig *http_config, CmkemStrList **http_res_list,
    HttpStatusCode *stat_code);
extern char *find_resheader(CmkemStrList *resheader_list, const char *resheader_type);
extern CmkemErrCode find_value_from_http_resbody(const char *http_resbody, const char **key_list,
    CmkemStrList **value_list);

#endif /* CMKEM_COMM_HTTP_H */
