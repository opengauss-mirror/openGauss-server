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
 * http_common.cpp
 *    http request common utility function
 *
 * IDENTIFICATION
 *    src/gausskernel/security/tde_key_management/http_common.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "tde_key_management/http_common.h"
#include "securec.h"
#include "utils/elog.h"

HttpErrCode HttpCommon::http_request(HttpReqMsg* http_req_msg, HttpConfig *http_config, AdvStrList **http_res_list)
{
    CURL *sender = NULL;
    struct curl_slist *http_header = NULL;
    HttpErrCode ret = TDE_HTTP_SUCCEED;

    curl_global_init(CURL_GLOBAL_ALL);
    sender = curl_easy_init();
    if (sender == NULL) {
        ereport(ERROR, (errcode(ERRCODE_CANNOT_CONNECT_NOW), errmsg("failed to init curl")));
        return TDE_CURL_ERR;
    }
    ret = set_http_config(sender, http_config);
    if (ret != TDE_HTTP_SUCCEED) {
        curl_easy_cleanup(sender);
        return ret;
    }
    ret = set_http_reqline(sender, http_req_msg->method, http_req_msg->url);
    if (ret != TDE_HTTP_SUCCEED) {
        curl_easy_cleanup(sender);
        return ret;
    }
    ret = set_http_reqheader(sender, http_req_msg->header_list, http_header);
    curl_slist_free_all(http_header);
    if (ret != TDE_HTTP_SUCCEED) {
        curl_easy_cleanup(sender);
        return ret;
    }
    ret = set_http_reqbody(sender, http_req_msg->body);
    if (ret != TDE_HTTP_SUCCEED) {
        curl_easy_cleanup(sender);
        return ret;
    }
    ret = get_http_resmsg(sender, http_config->res_part, http_res_list);
    curl_easy_cleanup(sender);
    if (ret != TDE_HTTP_SUCCEED) {
        return ret;
    }
    curl_global_cleanup();
    return TDE_HTTP_SUCCEED;
}

HttpErrCode HttpCommon::set_http_config(CURL *http_obj, HttpConfig *http_config)
{
    CURLcode curl_ret = CURLE_OK;
    const int connect_timeout = 10;

    curl_ret = curl_easy_setopt(http_obj, CURLOPT_TIMEOUT, http_config->timeout);
    check_curl_ret(curl_ret);
    curl_ret = curl_easy_setopt(http_obj, CURLOPT_CONNECTTIMEOUT, connect_timeout);
    check_curl_ret(curl_ret);
    curl_ret = curl_easy_setopt(http_obj, CURLOPT_SSL_VERIFYPEER, false);
    check_curl_ret(curl_ret);
    curl_ret = curl_easy_setopt(http_obj, CURLOPT_SSL_VERIFYHOST, false);
    check_curl_ret(curl_ret);
    return TDE_HTTP_SUCCEED;
}

void HttpCommon::check_curl_ret(CURLcode curl_ret)
{
    if (curl_ret != CURLE_OK) {
        ereport(ERROR, (errcode(ERRCODE_CANNOT_CONNECT_NOW), 
            errmsg("curl error. err code: '%lu', err msg: '%s.", (long unsigned int)curl_ret, 
                curl_easy_strerror(curl_ret))));
    }
    return;
}

HttpErrCode HttpCommon::set_http_reqline(CURL *http_obj, HttpMethod method, const char *url)
{
    CURLcode curl_ret = CURLE_OK;

    switch (method) {
        case HTTP_POST:
            curl_ret = curl_easy_setopt(http_obj, CURLOPT_HTTPPOST, 1);
            break;
        case HTTP_GET:
            curl_ret = curl_easy_setopt(http_obj, CURLOPT_HTTPGET, 1);
            break;
        default:
            break;
    }
    check_curl_ret(curl_ret);
    curl_ret = curl_easy_setopt(http_obj, CURLOPT_URL, url);
    check_curl_ret(curl_ret);
    return TDE_HTTP_SUCCEED;
}

HttpErrCode HttpCommon::set_http_reqheader(CURL *http_obj, const char *http_heaser_list[],
    struct curl_slist* ret_header_list)
{
    struct curl_slist *tmp_http_header = NULL;
    CURLcode curl_ret = CURLE_OK;

    for (size_t i = 0; http_heaser_list[i] != NULL; i++) {
        tmp_http_header = curl_slist_append(tmp_http_header, http_heaser_list[i]);
        if (tmp_http_header == NULL) {
            ereport(ERROR, (errcode(ERRCODE_CANNOT_CONNECT_NOW), 
                errmsg("failed to set http header: '%s'", http_heaser_list[i])));
            return TDE_CURL_ERR;
        }
    }
    curl_ret = curl_easy_setopt(http_obj, CURLOPT_HTTPHEADER, tmp_http_header);
    check_curl_ret(curl_ret);
    ret_header_list = tmp_http_header;
    return TDE_HTTP_SUCCEED;
}

HttpErrCode HttpCommon::set_http_reqbody(CURL *http_obj, const char *http_body)
{
    CURLcode curl_ret = curl_easy_setopt(http_obj, CURLOPT_POSTFIELDS, http_body);
    check_curl_ret(curl_ret);
    return TDE_HTTP_SUCCEED;
}

HttpErrCode HttpCommon::get_http_resmsg(CURL *http_obj, HttpResListType res_part, AdvStrList **response_msg)
{
    HttpResList http_res_list = {"", res_part, HTTP_RESLINE, NULL, 0, 0, false};
    const int status_code = 300;
    long http_status_code = 0;
    CURLcode curl_ret = CURLE_OK;

    http_res_list.str_list = malloc_advstr_list();
    if (http_res_list.str_list == NULL) {
        ereport(ERROR, (errcode(ERRCODE_CANNOT_CONNECT_NOW), errmsg("failed to malloc memory")));
        return TDE_MALLOC_MEM_ERR;
    }
    curl_ret = curl_easy_setopt(http_obj, CURLOPT_HEADER, 1);
    check_curl_ret(curl_ret);
    curl_ret = curl_easy_setopt(http_obj, CURLOPT_WRITEFUNCTION, dump_http_response_msg_callback);
    check_curl_ret(curl_ret);
    curl_ret = curl_easy_setopt(http_obj, CURLOPT_WRITEDATA, (void *) &http_res_list);
    check_curl_ret(curl_ret);
    curl_ret = curl_easy_perform(http_obj);
    check_curl_ret(curl_ret);
    curl_ret = curl_easy_getinfo(http_obj, CURLINFO_RESPONSE_CODE, &http_status_code);
    check_curl_ret(curl_ret);
    if (http_status_code > status_code) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_CANNOT_CONNECT_NOW), 
            errmsg("curl error code is %d", (int)http_status_code), errdetail("N/A"), 
            errcause("http request status error"), erraction("check curl retrun code status")));
    }
    *response_msg = http_res_list.str_list;
    return TDE_HTTP_SUCCEED;
}

size_t HttpCommon::dump_http_response_msg_callback(void *tmp_cur_res_str, size_t chr_size, size_t cur_res_size, 
    void *tmp_http_res_list)
{
    bool is_need_dump = false;
    const int loop_num = 2;
    HttpResList *http_res_list = NULL;

    if (tmp_cur_res_str == NULL || tmp_http_res_list == NULL) {
        return 0;
    }
    http_res_list = (HttpResList *)tmp_http_res_list;
    if (http_res_list->is_stop_dump) {
        return chr_size * cur_res_size;
    }
    const char *cur_res_str = (const char *)tmp_cur_res_str;
    if (http_res_list->filter_res_type == HTTP_MSG) {
        is_need_dump = true;
    } else {
        switch (http_res_list->cur_pos) {
            case HTTP_RESLINE:
                if (http_res_list->filter_res_type == HTTP_RESLINE) {
                    is_need_dump = true;
                    http_res_list->is_stop_dump = true;
                }
                if (http_res_list->loop_cnt == loop_num) {
                    http_res_list->cur_pos = HTTP_RESHEADER;
                }
                http_res_list->loop_cnt++;
                break;
            case HTTP_RESHEADER:
                if (http_res_list->filter_res_type == HTTP_RESHEADER) {
                    is_need_dump = true;
                }
                if (strlen(cur_res_str) == strlen("\r\n") && cur_res_str[0] == '\r' && cur_res_str[1] == '\n') {
                    http_res_list->cur_pos = HTTP_RESBODY;
                }
                break;
            case HTTP_RESBODY:
                if (http_res_list->filter_res_type == HTTP_RESBODY) {
                    is_need_dump = true;
                }
                http_res_list->cur_pos = HTTP_RESLINE;
                break;
            default:
                break;
        }
    }
    if (is_need_dump) {
        tde_append_node(http_res_list->str_list, cur_res_str);
    }
    return chr_size * cur_res_size;
}
