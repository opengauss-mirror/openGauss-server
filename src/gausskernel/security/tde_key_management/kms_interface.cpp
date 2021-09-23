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
 * kms_interface.cpp
 *    KMS interface is the external interface of KMS management
 *
 * IDENTIFICATION
 *    src/gausskernel/security/tde_key_management/kms_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "tde_key_management/kms_interface.h"
#include "tde_key_management/http_common.h"

KMSInterface::KMSInterface()
{}

KMSInterface::~KMSInterface()
{}

DekInfo* KMSInterface::create_kms_dek(const char* cmk_id)
{
    DekInfo* create_dek_info = NULL;
    AdvStrList *http_msg_list = NULL;
    /* check token valid */
    if (!(TDE::CKMSMessage::get_instance().check_token_valid())) {
        /* get token */
        get_kms_token();
    }
    /* create KMS DEK */
    http_msg_list = kms_restful_get_dek(cmk_id, NULL, KMS_GEN_DEK);
    create_dek_info = parser_http_array(http_msg_list, KMS_GEN_DEK);
    tde_list_free(http_msg_list);
    if (create_dek_info == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("creating KMS DEK is NULL")));
        return NULL;
    }
    return create_dek_info;
}

char* KMSInterface::get_kms_dek(const char* cmk_id, const char* dek_cipher)
{
    char* dek_plain = NULL;
    AdvStrList *http_msg_list = NULL;
    /* check token valid */
    if (!(TDE::CKMSMessage::get_instance().check_token_valid())) {
        /* get token */
        get_kms_token();
    }
    /* create KMS DEK */
    http_msg_list = kms_restful_get_dek(cmk_id, dek_cipher, KMS_GET_DEK);
    dek_plain = parser_http_string(http_msg_list, KMS_GET_DEK);
    tde_list_free(http_msg_list);
    if (dek_plain == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("get KMS DEK plaintext is NULL")));
        return NULL;
    }
    return dek_plain;
}

void KMSInterface::get_kms_token()
{
    bool result = false;
    char* token = NULL;
    char* temp_token = NULL;
    char* agency_token = NULL;
    char* temp_agency_token = NULL;
    AdvStrList *http_msg_list = NULL;
    AdvStrList *http_agency_msg_list = NULL;
    errno_t rc = EOK;
    int len = 0;

    /* get internal user B token */
    http_msg_list = kms_restful_token();
    temp_token = parser_http_string(http_msg_list, IAM_TOKEN);
    /* combine token string */
    len = strlen(req_token_tag) + 1 + strlen(temp_token) + 1;
    token = (char*)palloc0(len);
    rc = snprintf_s(token, len, len, "%s:%s", req_token_tag, temp_token);
    securec_check_ss(rc, "\0", "\0");
    /* get user A agency token */
    http_agency_msg_list = kms_restful_agency_token(token);
    temp_agency_token = parser_http_string(http_agency_msg_list, IAM_AGENCY_TOKEN);
    /* combine agency token string */
    len = strlen(req_token_tag) + 1 + strlen(temp_agency_token) + 1;
    agency_token = (char*)palloc0(len);
    rc = snprintf_s(agency_token, len, len, "%s:%s", req_token_tag, temp_agency_token);
    securec_check_ss(rc, "\0", "\0");
    /* cache all token */
    result = TDE::CKMSMessage::get_instance().save_token(token, agency_token);
    tde_list_free(http_msg_list);
    tde_list_free(http_agency_msg_list);
    if (result == false) {
        pfree_ext(token);
        pfree_ext(agency_token);
        pfree_ext(temp_token);
        pfree_ext(temp_agency_token);
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("Could KMS token caching is failed")));
        return;
    }
    pfree_ext(token);
    pfree_ext(agency_token);
    pfree_ext(temp_token);
    pfree_ext(temp_agency_token);
    return;
}

AdvStrList* KMSInterface::kms_restful_token()
{
    HttpErrCode ret = TDE_HTTP_SUCCEED;
    char* http_body = NULL;
    char* token_url = NULL;
    char* project_name = NULL;
    AdvStrList *http_msg_list = NULL;
    int len = 0;
    errno_t rc = EOK;

    project_name = TDE::CKMSMessage::get_instance().token_info->project_name;
    len = strlen(url_iam_head) + strlen(url_token) + strlen(project_name) + 1;
    token_url = (char*)palloc0(len);
    rc = memcpy_s(token_url, len, url_iam_head, strlen(url_iam_head));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s((token_url + strlen(url_iam_head)), len, project_name, strlen(project_name));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s((token_url + strlen(url_iam_head) + strlen(project_name)), len, url_token, (strlen(url_token) + 1));
    securec_check(rc, "\0", "\0");

    http_body = TDE::CKMSMessage::get_instance().get_iam_token_json();
    const char* http_head_list[] = {content_type, NULL};
    HttpReqMsg http_req_msg = {HTTP_POST, token_url, NULL, http_body, http_head_list};
    HttpConfig http_config = {time_out, HTTP_MSG};
    ret = HttpCommon::http_request(&http_req_msg, &http_config, &http_msg_list);
    /* token_url is not NULL */
    pfree_ext(token_url);
    pfree_ext(http_body);
    if (ret != TDE_HTTP_SUCCEED) {
        tde_list_free(http_msg_list);
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("http request failed"), errdetail("N/A"), errcause("http request error"), 
            erraction("check KMS or IAM connect or config parameter")));
    }
    return http_msg_list;
}

AdvStrList* KMSInterface::kms_restful_agency_token(char* token)
{
    HttpErrCode ret = TDE_HTTP_SUCCEED;
    char* http_body = NULL;
    char* agency_token_url = NULL;
    char* project_name = NULL;
    AdvStrList *http_msg_list = NULL;
    int len = 0;
    errno_t rc = EOK;

    project_name = TDE::CKMSMessage::get_instance().agency_token_info->project_name;
    len = strlen(url_iam_head) + strlen(url_token) + strlen(project_name) + 1;
    agency_token_url = (char*)palloc0(len);
    rc = memcpy_s(agency_token_url, len, url_iam_head, strlen(url_iam_head));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s((agency_token_url + strlen(url_iam_head)), len, project_name, strlen(project_name));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s((agency_token_url + strlen(url_iam_head) + strlen(project_name)), len, url_token, 
        (strlen(url_token) + 1));
    securec_check(rc, "\0", "\0");

    http_body = TDE::CKMSMessage::get_instance().get_iam_agency_token_json();
    const char* http_head_list[] = {content_type, token, NULL};
    HttpReqMsg http_req_msg = {HTTP_POST, agency_token_url, NULL, http_body, http_head_list};
    HttpConfig http_config = {time_out, HTTP_MSG};
    ret = HttpCommon::http_request(&http_req_msg, &http_config, &http_msg_list);
    /* agency_token_url is not NULL */
    pfree_ext(agency_token_url);
    pfree_ext(http_body);
    if (ret != TDE_HTTP_SUCCEED) {
        tde_list_free(http_msg_list);
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("http request failed"), errdetail("N/A"), errcause("http request error"), 
            erraction("check KMS or IAM connect or config parameter")));
    }
    return http_msg_list;
}

AdvStrList* KMSInterface::kms_restful_get_dek(const char* cmk_id, const char* dek_cipher, ResetApiType api_type)
{
    HttpErrCode ret = TDE_HTTP_SUCCEED;
    char* http_body = NULL;
    char* project_name = NULL;
    char* project_id = NULL;
    char* dek_url = NULL;
    AdvStrList *http_msg_list = NULL;
    int len = 0;
    errno_t rc = EOK;

    project_name = TDE::CKMSMessage::get_instance().kms_info->project_name;
    project_id = TDE::CKMSMessage::get_instance().kms_info->project_id;
    if (api_type == KMS_GEN_DEK) {
        len = strlen(url_kms_head) + strlen(project_name) + strlen(url_kms_path) + strlen(project_id) + 
            strlen(url_create_dek) + 1;
        dek_url = (char*)palloc0(len);
        rc = snprintf_s(dek_url, len, len, "%s%s%s%s%s", url_kms_head, project_name, url_kms_path, project_id, 
            url_create_dek);
        securec_check_ss(rc, "\0", "\0");
        http_body = TDE::CKMSMessage::get_instance().get_create_dek_json(cmk_id);
    } else if (api_type == KMS_GET_DEK) {
        len = strlen(url_kms_head) + strlen(project_name) + strlen(url_kms_path) + strlen(project_id) + 
            strlen(url_get_dek) + 1;
        dek_url = (char*)palloc0(len);
        rc = snprintf_s(dek_url, len, len, "%s%s%s%s%s", url_kms_head, project_name, url_kms_path, project_id, 
            url_get_dek);
        securec_check_ss(rc, "\0", "\0");
        http_body = TDE::CKMSMessage::get_instance().get_decrypt_dek_json(cmk_id, dek_cipher);
    }
    const char* http_head_list[] = {content_type, TDE::CKMSMessage::get_instance().tde_agency_token, NULL};
    HttpReqMsg http_req_msg = {HTTP_POST, dek_url, NULL, http_body, http_head_list};
    HttpConfig http_config = {time_out, HTTP_RESBODY};
    ret = HttpCommon::http_request(&http_req_msg, &http_config, &http_msg_list);
    pfree_ext(dek_url);
    pfree_ext(http_body);
    if (ret != TDE_HTTP_SUCCEED) {
        tde_list_free(http_msg_list);
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("http request failed"), errdetail("N/A"), errcause("http request error"), 
            erraction("check KMS or IAM connect or config parameter")));
    }
    return http_msg_list;
}

char* KMSInterface::parser_http_string(AdvStrList* http_msg_list, ResetApiType api_type)
{
    char* result = NULL;
    char* token_tmp = NULL;
    char* json_string = NULL;
    errno_t rc = EOK;

    if ((api_type == IAM_TOKEN) || (api_type == IAM_AGENCY_TOKEN)) {
        token_tmp = find_resheader(http_msg_list, get_token_tag);
        if (token_tmp == NULL) {
            ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
                errmsg("get iam token or iam agency token is NULL"), errdetail("N/A"), 
                errcause("connect IAM failed"), 
                erraction("check if your env can connect with IAM server")));
        }
        result = (char*)palloc0(strlen(token_tmp) + 1);
        rc = memcpy_s(result, (strlen(token_tmp) + 1), token_tmp, (strlen(token_tmp) + 1));
        securec_check(rc, "\0", "\0");
    } else if (api_type == KMS_GET_DEK) {
        cJSON *dek_json = cJSON_Parse(tde_get_val(http_msg_list, 0));
        if (cJSON_GetObjectItem(dek_json, data_key) == NULL) {
            ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
                errmsg("KMS dek json key is NULL"), errdetail("N/A"), 
                errcause("KMS return value error"), 
                erraction("check KMS config paramenter")));
        }
        json_string = cJSON_GetObjectItem(dek_json, data_key)->valuestring;
        if (json_string == NULL) {
            cJSON_Delete(dek_json);
            ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
                errmsg("get kms dek is NULL"), errdetail("N/A"), 
                errcause("connect KMS failed"), 
                erraction("check if your env can connect with KMS server")));
        }
        result = (char*)palloc0(strlen(json_string) + 1);
        rc = memcpy_s(result, (strlen(json_string) + 1), json_string, (strlen(json_string) + 1));
        securec_check(rc, "\0", "\0");
        cJSON_Delete(dek_json);
    }
    return result;
}

DekInfo* KMSInterface::parser_http_array(AdvStrList* http_msg_list, ResetApiType api_type)
{
    DekInfo* dek_info = NULL;
    cJSON *dek_json = NULL;
    char* plain_json = NULL;
    char* cipher_json = NULL;
    errno_t rc = EOK;

    if (api_type == KMS_GEN_DEK) {
        dek_json = cJSON_Parse(tde_get_val(http_msg_list, 0));
        if ((cJSON_GetObjectItem(dek_json, plain_text) == NULL) || 
            (cJSON_GetObjectItem(dek_json, cipher_text) == NULL)) {
            ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
                errmsg("KMS dek json key is NULL"), errdetail("N/A"), 
                errcause("KMS return value error"), 
                erraction("check KMS config paramenter")));
        }
        plain_json = cJSON_GetObjectItem(dek_json, plain_text)->valuestring;
        cipher_json = cJSON_GetObjectItem(dek_json, cipher_text)->valuestring;
        if ((plain_json == NULL) || (cipher_json == NULL)) {
            cJSON_Delete(dek_json);
            return NULL;
        }
        dek_info = (DekInfo*)palloc0(sizeof(DekInfo));
        dek_info->plain = (char*)palloc0(strlen(plain_json) + 1);
        rc = memcpy_s(dek_info->plain, (strlen(plain_json) + 1), plain_json, (strlen(plain_json) + 1));
        securec_check(rc, "\0", "\0");
        dek_info->cipher = (char*)palloc0(strlen(cipher_json) + 1);
        rc = memcpy_s(dek_info->cipher, (strlen(cipher_json) + 1), cipher_json, (strlen(cipher_json) + 1));
        securec_check(rc, "\0", "\0");
        cJSON_Delete(dek_json);
    }
    return dek_info;
}

char *KMSInterface::find_resheader(AdvStrList *resheader_list, const char *resheader_type)
{
    char *ret = NULL;
    
    for (size_t i = 0; i < tde_list_len(resheader_list); i++) {
        AdvStrList *cur_header = tde_split_node(tde_get_val(resheader_list, i), ':');
        if ((cur_header == NULL) || (tde_get_val(cur_header, 0) == NULL)) {
            ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
                errmsg("get http header is NULL"), errdetail("N/A"), 
                errcause("http request failed"), 
                erraction("check IAM config parameter")));
        }
        if (tde_list_len(resheader_list) > 1 && strcmp(resheader_type, tde_get_val(cur_header, 0)) == 0) {
            ret = tde_get_val(cur_header, 1);
            free_advstr_list_with_skip(cur_header, 1);
            break;
        } else {
            free_advstr_list(cur_header);
        }
    }
    return ret;
}
