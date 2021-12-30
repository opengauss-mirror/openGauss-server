
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
 * cmkem_comm_http.cpp
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/cmkem_comm_http.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "pg_config.h"
#include "cmkem_comm_http.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "curl/curl.h"
#include "cjson/cJSON.h"

#ifdef ENABLE_UT
#define static
#endif

static size_t dump_http_response_msg_callback(void *tmp_cur_res_str, size_t chr_size, size_t cur_res_size,
    void *tmp_http_res_list)
{
    bool is_need_dump = false;
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
                if (http_res_list->loop_cnt == 2) {
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
        cmkem_list_append(http_res_list->str_list, cur_res_str);
    }

    return chr_size * cur_res_size;
}

/* any traversal method is ok, we use inorder travers here */
CmkemErrCode traverse_jsontree_with_raplace_value(cJSON *json_tree, ReplaceJsonTempValue replace_rules[],
    size_t rule_cnt)
{
    char *new_value = NULL;
    CmkemErrCode ret = CMKEM_SUCCEED;

    if (json_tree == NULL) {
        return CMKEM_SUCCEED;
    }

    if (cJSON_IsString(json_tree)) {
        for (size_t i = 0; i < rule_cnt; i++) {
            if (strcmp(replace_rules[i].src_value, cJSON_GetStringValue(json_tree)) == 0) {
                new_value = cJSON_SetValuestring(json_tree, replace_rules[i].dest_value);
                if (new_value == NULL) {
                    cmkem_errmsg("failed to set the value of json tree.");
                    return CMKEM_SET_CJSON_VALUE_ERR;
                }
            }
        }
    }

    ret = traverse_jsontree_with_raplace_value(json_tree->next, replace_rules, rule_cnt);
    check_cmkem_ret(ret);

    return traverse_jsontree_with_raplace_value(json_tree->child, replace_rules, rule_cnt);
}

CmkemErrCode traverse_jsontree_with_get_value(cJSON *json_tree, const char **key_list, CmkemStrList *value_list)
{
    CmkemErrCode ret = CMKEM_SUCCEED;

    /* we can make sure key_list[TAIL] == NULL */
    if (json_tree == NULL || key_list[0] == NULL) {
        return CMKEM_SUCCEED;
    }

    if (json_tree->string != NULL && strcmp(key_list[0], json_tree->string) == 0) {
        cmkem_list_append(value_list, cJSON_GetStringValue(json_tree));
        return traverse_jsontree_with_get_value(json_tree, key_list + 1, value_list);
    }

    ret = traverse_jsontree_with_get_value(json_tree->next, key_list, value_list);
    check_cmkem_ret(ret);

    return traverse_jsontree_with_get_value(json_tree->child, key_list, value_list);
}

CmkemErrCode set_http_config(CURL *http_obj, HttpConfig *http_config)
{
    CURLcode curl_ret = CURLE_OK;
    
    curl_ret = curl_easy_setopt(http_obj, CURLOPT_TIMEOUT, http_config->timeout);
    check_curl_ret(curl_ret);

    curl_ret = curl_easy_setopt(http_obj, CURLOPT_SSL_VERIFYPEER, false);
    check_curl_ret(curl_ret);

    curl_ret = curl_easy_setopt(http_obj, CURLOPT_SSL_VERIFYHOST, false);
    check_curl_ret(curl_ret);

    return CMKEM_SUCCEED;
}

CmkemErrCode set_http_reqline(CURL *http_obj, HttpMethod method, const char *url, char *version)
{
    CURLcode curl_ret = CURLE_OK;

    if (method == HTTP_POST) {
        curl_ret = curl_easy_setopt(http_obj, CURLOPT_HTTPPOST, 1);
    } else {
        cmkem_errmsg("invalid http method, only support HTTP REQUEST now.");
        return CMKEM_CURL_INIT_ERR;
    }

    curl_ret = curl_easy_setopt(http_obj, CURLOPT_URL, url);
    check_curl_ret(curl_ret);

    return CMKEM_SUCCEED;
}

CmkemErrCode set_http_reqheader(CURL *http_obj, const char *http_heaser_list[],
    struct curl_slist **ret_header_list)
{
    CURLcode curl_ret = CURLE_OK;

    for (size_t i = 0; http_heaser_list[i] != NULL; i++) {
        *ret_header_list = curl_slist_append(*ret_header_list, http_heaser_list[i]);
        if (*ret_header_list == NULL) {
            cmkem_errmsg("failed to set http header: '%s'.", http_heaser_list[i]);
            return CMKEM_CURL_ERR;
        }
    }

    curl_ret = curl_easy_setopt(http_obj, CURLOPT_HTTPHEADER, *ret_header_list);
    check_curl_ret(curl_ret);

    return CMKEM_SUCCEED;
}

CmkemErrCode set_http_reqbody(CURL *http_obj, const char *http_body)
{
    CURLcode curl_ret = curl_easy_setopt(http_obj, CURLOPT_POSTFIELDS, http_body);
    check_curl_ret(curl_ret);

    return CMKEM_SUCCEED;
}

CmkemErrCode get_http_resmsg(CURL *http_obj, HttpResListType res_part, CmkemStrList **response_msg, long *stat_code)
{
    HttpResList http_res_list = {"", res_part, HTTP_RESLINE, NULL, 0, false, 0};
    long http_stat_code = 0;
    CURLcode curl_ret = CURLE_OK;

    http_res_list.str_list = malloc_cmkem_list();
    if (http_res_list.str_list == NULL) {
        return CMKEM_MALLOC_MEM_ERR;
    }

    curl_easy_setopt(http_obj, CURLOPT_HEADER, 1); /* set the localtion of write_data */

    curl_easy_setopt(http_obj, CURLOPT_WRITEFUNCTION, dump_http_response_msg_callback);

    curl_easy_setopt(http_obj, CURLOPT_WRITEDATA, (void *) &http_res_list);

    curl_easy_setopt(http_obj, CURLOPT_FOLLOWLOCATION, 1);

    curl_ret = curl_easy_perform(http_obj);
    if (curl_ret != CURLE_OK) {
        cmkem_list_free(http_res_list.str_list);
        cmkem_errmsg("curl error. err code: '%lu', err msg: '%s'.", curl_ret, curl_easy_strerror(curl_ret));
        return CMKEM_CURL_ERR;   
    }

    curl_ret = curl_easy_getinfo(http_obj, CURLINFO_RESPONSE_CODE, &http_stat_code);
    if (curl_ret != CURLE_OK) {
        cmkem_list_free(http_res_list.str_list);
        cmkem_errmsg("curl error. err code: '%lu', err msg: '%s'.", curl_ret, curl_easy_strerror(curl_ret));
        return CMKEM_CURL_ERR;   
    }

    *response_msg = http_res_list.str_list;
    *stat_code = http_stat_code;
    return CMKEM_SUCCEED;
}

CmkemErrCode http_request(HttpReqMsg* http_req_msg, HttpConfig *http_config, CmkemStrList **http_res_list,
    HttpStatusCode *stat_code)
{
    CURL *sender = NULL;
    struct curl_slist *http_header = NULL;
    long http_stat_code = 0;
    CmkemErrCode ret = CMKEM_SUCCEED;

    curl_global_init(CURL_GLOBAL_ALL);
    sender = curl_easy_init();
    if (sender == NULL) {
        return CMKEM_CURL_ERR;
    }

    ret = set_http_config(sender, http_config);
    if (ret != CMKEM_SUCCEED) {
        curl_easy_cleanup(sender);
        return ret;
    }

    ret = set_http_reqline(sender, http_req_msg->method, http_req_msg->url, http_req_msg->version);
    if (ret != CMKEM_SUCCEED) {
        curl_easy_cleanup(sender);
        return ret;
    }

    ret = set_http_reqheader(sender, http_req_msg->header_list, &http_header);
    if (ret != CMKEM_SUCCEED) {
        curl_slist_free_all(http_header);
        curl_easy_cleanup(sender);
        return ret;
    }

    ret = set_http_reqbody(sender, http_req_msg->body);
    if (ret != CMKEM_SUCCEED) {
        curl_slist_free_all(http_header);
        curl_easy_cleanup(sender);
        return ret;
    }

    ret = get_http_resmsg(sender, http_config->res_part, http_res_list, &http_stat_code);
    curl_slist_free_all(http_header);
    curl_easy_cleanup(sender);
    if (ret != CMKEM_SUCCEED) {
        return ret;
    }

    curl_global_cleanup();
    *stat_code = (HttpStatusCode)http_stat_code;
    
    return CMKEM_SUCCEED;
}

char *find_resheader(CmkemStrList *resheader_list, const char *resheader_type)
{
    char *ret = NULL;
    
    for (size_t i = 0; i < cmkem_list_len(resheader_list); i++) {
        CmkemStrList *cur_header = cmkem_split(cmkem_list_val(resheader_list, i), ':');
        if (cur_header == NULL) {
            return NULL;
        }

        /* the cur_header should be like ["$key", "$value"] */
        if (cmkem_list_len(cur_header) > 1 && strcmp(resheader_type, cmkem_list_val(cur_header, 0)) == 0) {
            ret = cmkem_list_val(cur_header, 1);
            free_cmkem_list_with_skip(cur_header, 1);
            break;
        } else {
            cmkem_list_free(cur_header);
        }
    }

    return ret;
}

CmkemErrCode find_value_from_http_resbody(const char *http_resbody, const char **key_list,
    CmkemStrList **value_list)
{
    size_t key_cnt = 0;
    
    cJSON *json_tree = cJSON_Parse(http_resbody);
    if (json_tree == NULL) {
        cmkem_errmsg("fail to convert http response body to cjson tree.");
        return CMKEM_CJSON_PARSE_ERR;
    }

    *value_list = malloc_cmkem_list();
    if (*value_list == NULL) {
        cJSON_Delete(json_tree);
        return CMKEM_MALLOC_MEM_ERR;
    }

    for (size_t i = 0; key_list[i] != NULL; i++) {
        key_cnt++;
    } 

    /* we will check 'key_cn' and 'value_cnt', so, the return value of this func is unimportant */
    (void)traverse_jsontree_with_get_value(json_tree, key_list, *value_list);
    cJSON_Delete(json_tree);

    if (key_cnt != cmkem_list_len(*value_list)) {
        cmkem_errmsg("%u keys are given, but only find %u values from list.", key_cnt, cmkem_list_len(*value_list));
        cmkem_list_free(*value_list);
        return CMKEM_FIND_CSJON_ERR;
    }

    return CMKEM_SUCCEED;
}
