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
 * register_huawei_kms.cpp
 *      Huawei KMS is an online key management service provided by Huawei Cloud.
 *      We support Huawei KMS to provide cmk entities for us, and we can send CEK entities
 *      to it for encrypting and decrypting
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/register_huawei_kms.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cmkem_version_control.h"
#ifdef ENABLE_HUAWEI_KMS

#include "register_huawei_kms.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

#include "cjson/cJSON.h"
#include "cmkem_comm.h"
#include "cmkem_comm_http.h"
#include "cmkem_comm_algorithm.h"
#include "reg_hook_frame.h"

#include "./kms_restful_temp.ini"

#ifdef ENABLE_UT
#define static
#endif

typedef struct CachedAuthInfo {
    char user_name[USER_NAME_BUF_LEN];
    char password[PASSWD_BUF_LEN];
    char domain_name[DOMAIN_NAME_BUF_LEN];
    char project_name[PROJ_NAME_BUF_LEN];
    char project_id[PROJ_ID_BUF_LEN];
    bool has_get_token;
    char req_header_token[MAX_TOKEN_BUF_LEN]; /* X-Auth-Token:{TOKEN} */
} CachedAuthInfo;

typedef struct KmsInfo {
    const char *kms_key;
    char *kms_value;
    size_t value_buf_len;
} KmsInfo;

CachedAuthInfo *kms_cache_tbl[KMS_CACHE_TBL_LEN] = {0};
static const char *supported_algorithms[] = {"AES_256", NULL};

static CachedAuthInfo *malloc_kms_cache(size_t cache_id);
static CmkemErrCode are_all_kms_infos_set(CachedAuthInfo *cache);
static void set_kms_cache_auth_token(CachedAuthInfo *cache, const char *token);
static CmkemErrCode get_kms_cache_ptr(size_t cache_id, CachedAuthInfo **cache);
static CmkemErrCode get_kms_token_from_iam(CachedAuthInfo *cache, char **token);

static CmkemErrCode check_cmk_algo_validity(const char *cmk_algo);
static CmkemErrCode check_cmk_id_validity(CmkIdentity *cmk_identity);
static CmkemErrCode check_cmk_entity_validity(CmkIdentity *cmk_identity);
static CmkemErrCode check_plain_len_constant(CmkemUStr *plain);
static ProcessPolicy create_cmk_obj_hookfunc(CmkIdentity *cmk_identity);
static ProcessPolicy encrypt_cek_plain_hookfunc(CmkemUStr *cek_plain, CmkIdentity *cmk_identity,
    CmkemUStr **cek_cipher);
static ProcessPolicy decrypt_cek_cipher_hookfunc(CmkemUStr *cek_cipher, CmkIdentity *cmk_identity,
    CmkemUStr **cek_plain);

/*
 * these templates are HTTP request bodys of restful apis of Huawei Cloud IAM and KMS
 * we need to use a definete json template, and pass it parameters such as user_name, password, project_name...
 */
cJSON *get_json_temp(KmsHttpMsgType json_tree_type)
{
    const char *temp_in = NULL;
    cJSON *json_out = NULL;
    
    typedef struct {
        KmsHttpMsgType type;
        const char *temp;
    } JsonTemp;

    const JsonTemp json_tbl[] = {
        {IAM_AUTH_REQ, temp_iam_auth_req},
        {KMS_SELECT_CMK_REQ, temp_kms_select_key_req},
        {KMS_ENC_CEK_REQ, temp_kms_enc_key_req},
        {KMS_DEC_CEK_REQ, temp_kms_dec_key_req},
    };

    temp_in = json_tbl[json_tree_type].temp;

    /*
     * the format of the temp_in is : "(str_with_bracket)", we should remove the backet
     * temp_in + 1 : remove the left bracket
     * strlen(temp_in) - 2 : remove the right bracket
     */
    json_out = cJSON_ParseWithLength(temp_in + 1, strlen(temp_in) - 2);

    return json_out;
}

char *get_iam_auth_req_jsontemp(const char *user_name, const char *password, const char *domain_name,
    const char *project_name)
{        
    cJSON* json_temp = NULL;
    char *json_temp_str = NULL;
    ReplaceJsonTempValue replace_rules[] = {
        {"$user_name$", user_name},
        {"$password$", password},
        {"$domain_name$", domain_name},
        {"$project_name$", project_name},
    };
    size_t rule_cnt = sizeof(replace_rules) / sizeof(replace_rules[0]);
    CmkemErrCode ret = CMKEM_SUCCEED;
    
    json_temp = get_json_temp(IAM_AUTH_REQ);
    if (json_temp == NULL) {
        cmkem_errmsg("failed to get json template to construct the http message of IAM auth request.");
        return NULL;
    }

    ret = traverse_jsontree_with_raplace_value(json_temp, replace_rules, rule_cnt);
    if (ret != CMKEM_SUCCEED) {
        cJSON_Delete(json_temp);
        return NULL;
    }

    json_temp_str = cJSON_Print(json_temp);
    cJSON_Delete(json_temp);
    return json_temp_str;
}

char *get_select_cmk_jsontemp(const char *key_id)
{
    cJSON* json_temp = NULL;
    char *json_temp_str = NULL;
    ReplaceJsonTempValue replace_rules[] = {
        {"$cmk_id$", key_id},
    };
    size_t rule_cnt = sizeof(replace_rules) / sizeof(replace_rules[0]);
    CmkemErrCode ret = CMKEM_SUCCEED;

    json_temp = get_json_temp(KMS_SELECT_CMK_REQ);
    if (json_temp == NULL) {
        cmkem_errmsg("failed to get json template to construct the http message of KMS select key request.");
        return NULL;
    }

    ret = traverse_jsontree_with_raplace_value(json_temp, replace_rules, rule_cnt);
    if (ret != CMKEM_SUCCEED) {
        cJSON_Delete(json_temp);
        return NULL;
    }

    json_temp_str = cJSON_Print(json_temp);
    cJSON_Delete(json_temp);
    return json_temp_str;
}

char *get_enc_cek_plain_jsontemp(const char *cmk_id, const char *cek_plain, size_t cek_plain_len)
{
    cJSON* json_temp = NULL;
    char *json_temp_str = NULL;
    char cek_plain_len_str[ITOA_BUF_LEN] = {0};
    CmkemErrCode ret = CMKEM_SUCCEED;
    itoa(cek_plain_len, cek_plain_len_str, ITOA_BUF_LEN);
    ReplaceJsonTempValue replace_rules[] = {
        {"$cmk_id$", cmk_id},
        {"$cek_plain$", cek_plain},
        {"$cek_plain_len$", cek_plain_len_str},
    };
    size_t rule_cnt = sizeof(replace_rules) / sizeof(replace_rules[0]);

    json_temp = get_json_temp(KMS_ENC_CEK_REQ);
    if (json_temp == NULL) {
        cmkem_errmsg("failed to get json template to construct the http message of IAM auth request.");
        return NULL;
    }

    ret = traverse_jsontree_with_raplace_value(json_temp, replace_rules, rule_cnt);
    if (ret != CMKEM_SUCCEED) {
        cJSON_Delete(json_temp);
        return NULL;
    }

    json_temp_str = cJSON_Print(json_temp);
    cJSON_Delete(json_temp);
    return json_temp_str;
}

char *get_dec_cek_cipher_jsontemp(const char *cmk_id, const char *cek_cipher, size_t cek_cipher_len)
{
    cJSON* json_temp = NULL;
    char cek_cipher_len_str[ITOA_BUF_LEN] = {0};
    CmkemErrCode ret = CMKEM_SUCCEED;
    itoa(cek_cipher_len, cek_cipher_len_str, ITOA_BUF_LEN);
    ReplaceJsonTempValue replace_rules[] = {
        {"$cmk_id$", cmk_id},
        {"$cek_cipher$", cek_cipher},
        {"$cek_cipher_len$", cek_cipher_len_str},
    };
    size_t rule_cnt = sizeof(replace_rules) / sizeof(replace_rules[0]);
    char *json_temp_str = NULL;

    json_temp = get_json_temp(KMS_DEC_CEK_REQ);
    if (json_temp == NULL) {
        cmkem_errmsg("failed to get json template to construct the http message of KMS decrypt key request.");
        return NULL;
    }

    ret = traverse_jsontree_with_raplace_value(json_temp, replace_rules, rule_cnt);
    if (ret != CMKEM_SUCCEED) {
        cJSON_Delete(json_temp);
        return NULL;
    }

    json_temp_str = cJSON_Print(json_temp);
    cJSON_Delete(json_temp);

    return json_temp_str;
}

void get_iam_url(const char *project_name, char *url_buf, size_t buf_len)
{
    errno_t rc = sprintf_s(url_buf, buf_len, "https://iam.%s.myhuaweicloud.com/v3/auth/tokens", project_name);
    securec_check_ss_c(rc, "", "");
}

CmkemErrCode get_kms_url(size_t cache_id, KmsHttpMsgType kms_msg_type, char *url_buf, size_t url_buf_len) 
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    char url_file_path_buf[MAX_URL_BUF_LEN] = {0};
    errno_t rc = 0;
    CachedAuthInfo *cache = NULL;
    
    ret = get_kms_cache_ptr(cache_id, &cache);
    check_cmkem_ret(ret);

    ret = get_url_file_path(kms_msg_type, cache->project_id, url_file_path_buf, MAX_URL_BUF_LEN);
    check_cmkem_ret(ret);

    rc = sprintf_s(url_buf, url_buf_len, "https://kms.%s.myhuaweicloud.com%s", cache->project_name, url_file_path_buf);
    securec_check_ss_c(rc, "", "");

    return CMKEM_SUCCEED;
}

CmkemErrCode get_url_file_path(KmsHttpMsgType kms_msg_type, const char *project_id, char *url_file_path_buf,
    size_t buf_len)
{
    errno_t rc = 0;
    
    switch (kms_msg_type) {
        case KMS_SELECT_CMK_REQ:
            rc = sprintf_s(url_file_path_buf, buf_len, "/v1.0/%s/kms/describe-key", project_id);
            securec_check_ss_c(rc, "", "");
            return CMKEM_SUCCEED;
        case KMS_ENC_CEK_REQ:
            rc = sprintf_s(url_file_path_buf, buf_len, "/v1.0/%s/kms/encrypt-datakey", project_id);
            securec_check_ss_c(rc, "", "");
            return CMKEM_SUCCEED;
        case KMS_DEC_CEK_REQ:
            rc = sprintf_s(url_file_path_buf, buf_len, "/v1.0/%s/kms/decrypt-datakey", project_id);
            securec_check_ss_c(rc, "", "");
            return CMKEM_SUCCEED;
        default:
            break;
    }

    return CMKEM_UNKNOWN_ERR;
}

CmkemErrCode get_kms_err_type(const char *kms_errmsg_body)
{
    const char *key_list[] = {"error_code", NULL};
    CmkemStrList *value_list = NULL;
    CmkemErrCode ret = CMKEM_SUCCEED;

    if (kms_errmsg_body == NULL) {
        return CMKEM_KMS_SERVER_ERR;
    }

    ret = find_value_from_http_resbody(kms_errmsg_body, key_list, &value_list);
    check_cmkem_ret(ret);

    if (strcasecmp(cmkem_list_val(value_list, 0), "kms.0303") == 0) {
        cmkem_list_free(value_list);
        return CMKEM_TOKEN_EXPIRED_ERR;
    }
    cmkem_list_free(value_list);
    
    return CMKEM_KMS_SERVER_ERR;
}

static CachedAuthInfo *malloc_kms_cache(size_t cache_id)
{
    errno_t rc = 0;
    
    if (cache_id >= KMS_CACHE_TBL_LEN) {
        return NULL;
    }

    CachedAuthInfo *cache = (CachedAuthInfo *)malloc(sizeof(CachedAuthInfo));
    if (cache == NULL) {
        return NULL;
    }

    rc = memset_s(cache, sizeof(CachedAuthInfo), 0, sizeof(CachedAuthInfo));
    securec_check_c(rc, "", "");

    kms_cache_tbl[cache_id] = cache;

    return cache;
}

void free_kms_cache(size_t cache_id)
{
    if (cache_id >= KMS_CACHE_TBL_LEN) {
        return;
    }

    errno_t rc = memset_s(kms_cache_tbl[cache_id], sizeof(CachedAuthInfo), 0, sizeof(CachedAuthInfo));
    securec_check_c(rc, "", "");

    cmkem_free(kms_cache_tbl[cache_id]);
}

static CmkemErrCode are_all_kms_infos_set(CachedAuthInfo *cache)
{
    if (is_str_empty(cache->user_name) ||
        is_str_empty(cache->password) ||
        is_str_empty(cache->domain_name) ||
        is_str_empty(cache->project_name) ||
        is_str_empty(cache->project_id)) {
        cmkem_errmsg("please make sure all Huawei IAM and KMS info have been set, including: "
            "{iamUser, iamPassword, kmsDomain, kmsProjectName, kmsProjectId}.");
        return CMKEM_CHECK_INPUT_AUTH_ERR;
    }

    return CMKEM_SUCCEED;
}

CmkemErrCode set_kms_cache_auth_info(size_t cache_id, const char *key, const char *value)
{
    CachedAuthInfo *cache = NULL;
    CmkemErrCode ret = CMKEM_SUCCEED;
    ret = get_kms_cache_ptr(cache_id, &cache);
    if (ret == CMKEM_CACHE_IS_EMPTY) {
        cache = malloc_kms_cache(cache_id);
        if (cache == NULL) {
            ret = CMKEM_MALLOC_MEM_ERR;
            cmkem_errmsg("failed to malloc memory as client cache whose cache id is '%u'.", cache_id);
        } else {
            ret = CMKEM_SUCCEED;
        }
    }
    check_cmkem_ret(ret);
    
    KmsInfo kms_info_map[] = {
        {"iamUser", cache->user_name, USER_NAME_BUF_LEN},
        {"iamPassword", cache->password, PASSWD_BUF_LEN},
        {"kmsDomain", cache->domain_name, DOMAIN_NAME_BUF_LEN},
        {"kmsProjectName", cache->project_name, PROJ_NAME_BUF_LEN},
        {"kmsProjectId", cache->project_id, PROJ_ID_BUF_LEN}};
    errno_t rc = 0;

    if (key == NULL) {
        cmkem_errmsg("the kms info should be in {iamUser, iamPassword, kmsDomain, kmsProjectName, kmsProjectId}.");
        return CMKEM_CHECK_INPUT_AUTH_ERR;
    }

    for (size_t i = 0; i < sizeof(kms_info_map) / sizeof(kms_info_map[0]); i++) {
        if (strcmp(key, kms_info_map[i].kms_key) == 0) {
            if (value == NULL || strlen(value) >= kms_info_map[i].value_buf_len) {
                cmkem_errmsg("the value length of '%s' is invalid.", kms_info_map[i].kms_key);
                return CMKEM_CHECK_INPUT_AUTH_ERR;
            }

            rc = strcpy_s(kms_info_map[i].kms_value, kms_info_map[i].value_buf_len, value);
            securec_check_c(rc, "", "");
        }
    }

    return CMKEM_SUCCEED;
}

static void set_kms_cache_auth_token(CachedAuthInfo *cache, const char *token)
{
    if (cache != NULL) {
        errno_t rc = sprintf_s(cache->req_header_token, MAX_TOKEN_BUF_LEN, "X-Auth-Token:%s", token);
        securec_check_ss_c(rc, "", "");
        cache->has_get_token = true;
    }
}

static CmkemErrCode get_kms_cache_ptr(size_t cache_id, CachedAuthInfo **cache)
{    
    if (cache_id >= KMS_CACHE_TBL_LEN) {
        cmkem_errmsg("the client cahche id '%u' is invalid.", cache_id);
        return CMKEM_CHECK_CHACHE_ID_ERR;
    }

    if (kms_cache_tbl[cache_id] == NULL) {
        cmkem_errmsg("the cache whose cache id is '%u' is empty.", cache_id);
        return CMKEM_CACHE_IS_EMPTY;
    }

    *cache = kms_cache_tbl[cache_id];
    return CMKEM_SUCCEED;
}

CmkemErrCode get_kms_cache_token(size_t cache_id, char **req_token)
{
    CachedAuthInfo *cache = NULL;
    CmkemErrCode ret = CMKEM_SUCCEED;
    char *token = NULL;

    ret = get_kms_cache_ptr(cache_id, &cache);
    if (ret == CMKEM_CACHE_IS_EMPTY) {
        cmkem_errmsg("failed to get token when attempting to access the KMS server, please provide this information: "
            "{iamUser, iamPassword, kmsDomain, kmsProjectName, kmsProjectId}.");
        return CMKEM_GET_TOKEN_ERR;
    }
    check_cmkem_ret(ret);

    if (!cache->has_get_token) {
        ret = get_kms_token_from_iam(cache, &token);
        check_cmkem_ret(ret);

        set_kms_cache_auth_token(cache, token);
        cmkem_free(token);
    }

    *req_token = cache->req_header_token;
    return CMKEM_SUCCEED;
}

CmkemErrCode check_token_exist(size_t cache_id)
{   
    CachedAuthInfo *cache = NULL;
    CmkemErrCode ret = CMKEM_SUCCEED;

    ret = get_kms_cache_ptr(cache_id, &cache);
    check_cmkem_ret(ret);
    
    if (cache->has_get_token == true) {
        return CMKEM_SUCCEED;
    } else {
        cmkem_errmsg("unable to establish connection with kms server, because token cannot be found.");
        return CMKEM_GET_TOKEN_ERR;
    }
}

CmkemErrCode refresh_cahced_token(size_t cache_id)
{
    printf("NOTICE: your IAM token has expired, we are going to update it.\n");
    
    CachedAuthInfo *cache = NULL;
    errno_t rc = 0;
    CmkemErrCode ret = CMKEM_SUCCEED;
    char *token = NULL;

    ret = get_kms_cache_ptr(cache_id, &cache);
    check_cmkem_ret(ret);

    rc = memset_s(cache->req_header_token, MAX_TOKEN_BUF_LEN, 0, MAX_TOKEN_BUF_LEN);
    securec_check_c(rc, "", "");
    cache->has_get_token = false;

    ret = get_kms_token_from_iam(cache, &token);
    check_cmkem_ret(ret);

    set_kms_cache_auth_token(cache, token);
    cmkem_free(token);
    
    return CMKEM_SUCCEED;
}

CmkemErrCode catch_iam_server_err(CmkemStrList *http_res_list, HttpStatusCode http_stat_code)
{
    char *iam_resbody = NULL;

    if (http_stat_code < HTTP_BAD_REQUEST) {
        return CMKEM_SUCCEED;
    } else if (http_stat_code >= HTTP_INTERNAL_SERVER_ERROR) {
        cmkem_errmsg("iam or porxy server error. http status code: %d.", http_stat_code);
        return CMKEM_IAM_SERVER_ERR;
    }

    iam_resbody = cmkem_list_val(http_res_list, -1); /* -1 == tail */

    cmkem_errmsg("iam server error. http status code: %d, iam server error message : %s.", http_stat_code,
        iam_resbody);
    return CMKEM_IAM_SERVER_ERR;
}

CmkemErrCode catch_kms_server_err(size_t cache_id, HttpReqMsg* http_req_msg, HttpConfig *http_config, 
    CmkemStrList **http_res_list, HttpStatusCode *http_stat_code)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    char *huaweikms_resbody = cmkem_list_val(*http_res_list, 0);
    
    if (*http_stat_code < HTTP_BAD_REQUEST) {
        return CMKEM_SUCCEED;
    } else if (*http_stat_code >= HTTP_INTERNAL_SERVER_ERROR) {
        cmkem_errmsg("kms or porxy server error. http status code: %d.", *http_stat_code);
        return CMKEM_KMS_SERVER_ERR;
    }

    ret = get_kms_err_type(huaweikms_resbody);
    if (ret != CMKEM_TOKEN_EXPIRED_ERR) {
        cmkem_errmsg("kms server error. http status code: %d, kms server error message : %s.", *http_stat_code,
            huaweikms_resbody);
        return CMKEM_KMS_SERVER_ERR;
    }

    ret = refresh_cahced_token(cache_id);
    check_cmkem_ret(ret);

    cmkem_list_free(*http_res_list); /* free old http response message, and try to get a new one */
    ret = http_request(http_req_msg, http_config, http_res_list, http_stat_code);
    check_cmkem_ret(ret);

    if (*http_stat_code >= HTTP_BAD_REQUEST) {
        cmkem_errmsg("kms server error. http status code: %d, kms server error message : %s.", *http_stat_code,
            huaweikms_resbody);
        cmkem_list_free(*http_res_list);
        return CMKEM_KMS_SERVER_ERR;
    }

    return CMKEM_SUCCEED;
}

/* 
 * hex_cek = hex(raw_cek) || sha256(hex(raw_cek))
 * eg. raw_cek = "b";
 *      hex_cek = "62" + "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d"
 * so, hex_cek_len = 2 * raw_cek_len + 32 * 2
 */
size_t get_hexcek_len_from_rawcek_len(size_t raw_cek_len)
{
    return HEX_SIZE * raw_cek_len + SHA256_HASH_LEN * HEX_SIZE;
}

size_t get_rawcek_len_from_hexcek_len(size_t hex_cek_len)
{
    return (hex_cek_len - HEX_SIZE * SHA256_HASH_LEN) / HEX_SIZE;
}

size_t get_rawcek_len_from_hex_cek_cipher_len(size_t hex_cek_len)
{
    return (hex_cek_len - HEX_SIZE * SHA256_HASH_LEN - HEX_SIZE * 92) / HEX_SIZE;
}

static CmkemErrCode get_kms_token_from_iam(CachedAuthInfo *cache, char **token)
{
    char iam_url[MAX_URL_BUF_LEN] = {0};
    const char *http_resheader[] = {"Content-Type:application/json", "charset=utf8", NULL};
    const char *res_token_tag = "X-Subject-Token";
    CmkemErrCode ret = CMKEM_SUCCEED;

    ret = are_all_kms_infos_set(cache);
    check_cmkem_ret(ret);

    char *http_reqbody = get_iam_auth_req_jsontemp(cache->user_name, cache->password, cache->domain_name,
        cache->project_name);
    if (http_reqbody == NULL) {
        return CMKEM_SET_CJSON_VALUE_ERR;
    }

    get_iam_url(cache->project_name, iam_url, MAX_URL_BUF_LEN);

    HttpReqMsg http_req_msg = {HTTP_POST, iam_url, NULL, http_reqbody, http_resheader};
    HttpConfig http_config = {DEFAULT_HTTP_TIME_OUT, HTTP_MSG};
    CmkemStrList *http_res_msg_list = NULL;
    HttpStatusCode http_stat_code;

    ret = http_request(&http_req_msg, &http_config, &http_res_msg_list, &http_stat_code);
    cmkem_free(http_reqbody);
    if (ret != CMKEM_SUCCEED) {
        return ret;
    }

    ret = catch_iam_server_err(http_res_msg_list, http_stat_code);
    if (ret != CMKEM_SUCCEED) {
        cmkem_list_free(http_res_msg_list);
        return ret;
    }

    *token = find_resheader(http_res_msg_list, res_token_tag);
    cmkem_list_free(http_res_msg_list);
    if (*token == NULL) {
        cmkem_errmsg("failed to find '%s' from http response header.", res_token_tag);
        return CMKEM_GET_TOKEN_ERR;
    }

    return CMKEM_SUCCEED;
}

CmkemErrCode select_cmk_entity_from_huaweikms(size_t cache_id, const char *cmk_id, char **huaweikms_resbody)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    
    char *req_header_token = NULL;
    ret = get_kms_cache_token(cache_id, &req_header_token);
    check_cmkem_ret(ret);

    char url[MAX_URL_BUF_LEN] = {0};
    ret = get_kms_url(cache_id, KMS_SELECT_CMK_REQ, url, MAX_URL_BUF_LEN);
    check_cmkem_ret(ret);

    char *http_reqbody = get_select_cmk_jsontemp(cmk_id);
    if (http_reqbody == NULL) {
        return CMKEM_SET_CJSON_VALUE_ERR;
    }

    const char *http_resheader_list[] = {"Content-Type:application/json", req_header_token, NULL};
    HttpReqMsg http_req_msg = {HTTP_POST, url, NULL, http_reqbody, http_resheader_list};
    HttpConfig http_config = {DEFAULT_HTTP_TIME_OUT, HTTP_RESBODY};
    CmkemStrList *http_resbody = NULL;
    HttpStatusCode http_stat_code;
    
    ret = http_request(&http_req_msg, &http_config, &http_resbody, &http_stat_code);
    if (ret != CMKEM_SUCCEED) {
        cmkem_free(http_reqbody);
        return ret;
    }
    
    ret = catch_kms_server_err(cache_id, &http_req_msg, &http_config, &http_resbody, &http_stat_code);
    cmkem_free(http_reqbody);
    if (ret != CMKEM_SUCCEED) {
        cmkem_list_free(http_resbody);
        return ret;
    }

    *huaweikms_resbody = cmkem_list_val(http_resbody, 0);
    free_cmkem_list_with_skip(http_resbody, 0);
    return CMKEM_SUCCEED;
}

CmkemErrCode encrypt_cek_plain_by_huaweikms(size_t cache_id, CmkemStr *hex_cek_plain_join_hash, const char *cmk_id,
    char **huaweikms_resbody)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    
    char *req_header_token = NULL;
    ret = get_kms_cache_token(cache_id, &req_header_token);
    check_cmkem_ret(ret);
    
    char url[MAX_URL_BUF_LEN] = {0};
    ret = get_kms_url(cache_id, KMS_ENC_CEK_REQ, url, MAX_URL_BUF_LEN);
    check_cmkem_ret(ret);
    
    char *http_reqbody = get_enc_cek_plain_jsontemp(cmk_id,
        hex_cek_plain_join_hash->str_val,
        get_rawcek_len_from_hexcek_len(hex_cek_plain_join_hash->str_len));
    if (http_reqbody == NULL) {
        return CMKEM_SET_CJSON_VALUE_ERR;
    }

    const char *http_resheader_list[] = {"Content-Type:application/json", req_header_token, NULL};
    HttpReqMsg http_req_msg = {HTTP_POST, url, NULL, http_reqbody, http_resheader_list};
    HttpConfig http_config = {DEFAULT_HTTP_TIME_OUT, HTTP_RESBODY};
    CmkemStrList *http_resbody = NULL;
    HttpStatusCode http_stat_code;

    ret = http_request(&http_req_msg, &http_config, &http_resbody, &http_stat_code);
    if (ret != CMKEM_SUCCEED) {
        cmkem_free(http_reqbody);
        return ret;
    }

    ret = catch_kms_server_err(cache_id, &http_req_msg, &http_config, &http_resbody, &http_stat_code);
    cmkem_free(http_reqbody);
    if (ret != CMKEM_SUCCEED) {
        cmkem_list_free(http_resbody);
        return ret;
    }

    *huaweikms_resbody = cmkem_list_val(http_resbody, 0);
    free_cmkem_list_with_skip(http_resbody, 0);
    return CMKEM_SUCCEED;
}

CmkemErrCode decrypt_cek_plain_by_huaweikms(size_t cache_id, CmkemStr *hex_cek_cipher_join_hash, const char *cmk_id,
    char **huaweikms_resbody)
{
    CmkemErrCode ret = CMKEM_SUCCEED;

    char *req_header_token = NULL;
    ret = get_kms_cache_token(cache_id, &req_header_token);
    check_cmkem_ret(ret);
    
    char url[MAX_URL_BUF_LEN] = {0};
    ret = get_kms_url(cache_id, KMS_DEC_CEK_REQ, url, MAX_URL_BUF_LEN);
    check_cmkem_ret(ret);
    
    char *http_reqbody = get_dec_cek_cipher_jsontemp(cmk_id,
        hex_cek_cipher_join_hash->str_val,
        get_rawcek_len_from_hex_cek_cipher_len(hex_cek_cipher_join_hash->str_len));
    if (http_reqbody == NULL) {
        return CMKEM_SET_CJSON_VALUE_ERR;
    }

    const char *http_resheader_list[] = {"Content-Type:application/json", req_header_token, NULL};
    HttpReqMsg http_req_msg = {HTTP_POST, url, NULL, http_reqbody, http_resheader_list};
    HttpConfig http_config = {DEFAULT_HTTP_TIME_OUT, HTTP_RESBODY};
    CmkemStrList *http_resbody = NULL;
    HttpStatusCode http_stat_code;

    ret = http_request(&http_req_msg, &http_config, &http_resbody, &http_stat_code);
    if (ret != CMKEM_SUCCEED) {
        cmkem_free(http_reqbody);
        return ret;
    }

    ret = catch_kms_server_err(cache_id, &http_req_msg, &http_config, &http_resbody, &http_stat_code);
    cmkem_free(http_reqbody);
    if (ret != CMKEM_SUCCEED) {
        cmkem_list_free(http_resbody);
        return ret;
    }

    *huaweikms_resbody = cmkem_list_val(http_resbody, 0);
    free_cmkem_list_with_skip(http_resbody, 0);
    return CMKEM_SUCCEED;
}

static CmkemErrCode check_cmk_algo_validity(const char *cmk_algo)
{
    char error_msg_buf[MAX_CMKEM_ERRMSG_BUF_SIZE] = {0};
    error_t rc = 0;
    
    for (size_t i = 0; supported_algorithms[i] != NULL; i++) {
        if (strcasecmp(cmk_algo, supported_algorithms[i]) == 0) {
            return CMKEM_SUCCEED;
        }
    }

    rc = sprintf_s(error_msg_buf, MAX_CMKEM_ERRMSG_BUF_SIZE, "unpported algorithm '%s', huawei kms only support: ",
        cmk_algo);
    securec_check_ss_c(rc, "", "");

    for (size_t i = 0; supported_algorithms[i] != NULL; i++) {
        rc = strcat_s(error_msg_buf, MAX_CMKEM_ERRMSG_BUF_SIZE, supported_algorithms[i]);
        securec_check_c(rc, "\0", "\0");
        rc = strcat_s(error_msg_buf, MAX_CMKEM_ERRMSG_BUF_SIZE, "  ");
        securec_check_c(rc, "\0", "\0");
    }

    cmkem_errmsg(error_msg_buf);
    return CMKEM_CHECK_ALGO_ERR;
}

static CmkemErrCode check_cmk_id_validity(CmkIdentity *cmk_identity)
{
    if (strlen(cmk_identity->cmk_id_str) != strlen("00000000-0000-0000-0000-000000000000")) {
        cmkem_errmsg("the length of cmk id is invalid.");
        return CMKEM_CHECK_CMK_ID_ERR;
    }
    
    return CMKEM_SUCCEED;
}

static CmkemErrCode check_cmk_entity_validity(CmkIdentity *cmk_identity)
{
    char *huaweikms_resbody = NULL;
    const char *key_list[] = {"scheduled_deletion_date", "key_state", NULL};
    CmkemStrList *value_list = NULL;
    CmkemErrCode ret = CMKEM_SUCCEED;

    ret = select_cmk_entity_from_huaweikms(cmk_identity->client_cache_id, cmk_identity->cmk_id_str, &huaweikms_resbody);
    check_cmkem_ret(ret);

    ret = find_value_from_http_resbody(huaweikms_resbody, key_list, &value_list);
    cmkem_free(huaweikms_resbody);
    check_cmkem_ret(ret);

    /* cmkem_list_val(value_list, 0) = value of key_list[0] = $scheduled_deletion_date */
    if (strlen(cmkem_list_val(value_list, 0)) > 0) {
        cmkem_errmsg("cmk entity '%s' is already scheduled to be deleted, please use another cmk entity.",
            cmk_identity->cmk_id_str);
        cmkem_list_free(value_list);
        return CMKEM_CHECK_IDENTITY_ERR;
    }

    /* cmkem_list_val(value_list, 1) = value of key_list[1] = $key_state */
    if (strcmp(cmkem_list_val(value_list, 1), "2") != 0) {
        cmkem_errmsg("cmk entity '%s' is unavailable.", cmk_identity->cmk_id_str);
        cmkem_list_free(value_list);
        return CMKEM_CHECK_IDENTITY_ERR;
    }
    cmkem_list_free(value_list);

    return CMKEM_SUCCEED;
}

static CmkemErrCode check_plain_len_constant(CmkemUStr *plain)
{
    if (plain->ustr_len > 0 && plain->ustr_len % KMS_PLAIN_PACKET_LEN == 0) {
        return CMKEM_SUCCEED;
    }

    cmkem_errmsg("the length of the cek value must be exactly divided by %d.", KMS_PLAIN_PACKET_LEN);
    return CMKEM_ENCRYPT_CEK_ERR;
}


static ProcessPolicy create_cmk_obj_hookfunc(CmkIdentity *cmk_identity)
{
    CmkemErrCode ret = CMKEM_SUCCEED;

    if (cmk_identity->cmk_store == NULL || strcasecmp(cmk_identity->cmk_store, "huawei_kms") != 0) {
        return POLICY_CONTINUE;
    }

    if (cmk_identity->cmk_id_str == NULL) {
        cmkem_errmsg("failed to create client master key, failed to find arg: KEY_PATH.");
        return POLICY_ERROR;
    }

    if (cmk_identity->cmk_algo == NULL) {
        cmkem_errmsg("failed to create client master key, failed to find arg: ALGORITHM.");
        return POLICY_ERROR;
    }

    ret = check_cmk_algo_validity(cmk_identity->cmk_algo);
    if (ret != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    ret = check_cmk_id_validity(cmk_identity);
    if (ret != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    ret = check_cmk_entity_validity(cmk_identity);
    if (ret != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    return POLICY_BREAK;
}

static ProcessPolicy encrypt_cek_plain_hookfunc(CmkemUStr *cek_plain, CmkIdentity *cmk_identity, CmkemUStr **cek_cipher)
{
    char *huaweikms_resbody = NULL;
    const char *key_list[] = {"cipher_text", "datakey_length", NULL};
    CmkemStrList *value_list = NULL;
    CmkemStr *hex_cek_plain_join_hash = NULL;
    CmkemStr *hex_cek_cipher_join_hash = NULL;
    CmkemErrCode ret = CMKEM_SUCCEED;

    if (cmk_identity->cmk_store == NULL || strcasecmp(cmk_identity->cmk_store, "huawei_kms") != 0) {
        return POLICY_CONTINUE;
    }

    if (check_plain_len_constant(cek_plain) != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    ret = get_hex_join_hash_from_ustr(cek_plain, &hex_cek_plain_join_hash);
    if (ret != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    ret = encrypt_cek_plain_by_huaweikms(cmk_identity->client_cache_id, hex_cek_plain_join_hash,
        cmk_identity->cmk_id_str, &huaweikms_resbody);
    free_cmkem_str(hex_cek_plain_join_hash);
    if (ret != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    ret = find_value_from_http_resbody(huaweikms_resbody, key_list, &value_list);
    cmkem_free(huaweikms_resbody);
    if (ret != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    hex_cek_cipher_join_hash = conv_str_to_cmkem_str(cmkem_list_val(value_list, 0));
    cmkem_list_free(value_list);
    if (hex_cek_cipher_join_hash == NULL) {
        return POLICY_ERROR;
    }

    ret = get_ustr_from_hex_join_hash(hex_cek_cipher_join_hash, cek_cipher);

    free_cmkem_str(hex_cek_cipher_join_hash);
    if (ret != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    return POLICY_BREAK;
}

static ProcessPolicy decrypt_cek_cipher_hookfunc(CmkemUStr *cek_cipher, CmkIdentity *cmk_identity,
    CmkemUStr **cek_plain)
{ 
    CmkemErrCode ret = CMKEM_SUCCEED;
    char *huaweikms_resbody = NULL;
    const char *key_list[] = {"data_key", "datakey_length", NULL};
    CmkemStrList *value_list = NULL;
    CmkemStr *hex_cek_cipher_join_hash = NULL;
    CmkemStr *hex_cek_plain_join_hash = NULL;

    if (cmk_identity->cmk_store == NULL || strcasecmp(cmk_identity->cmk_store, "huawei_kms") != 0) {
        return POLICY_CONTINUE;
    }

    ret = get_hex_join_hash_from_ustr(cek_cipher, &hex_cek_cipher_join_hash);
    if (ret != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    ret = decrypt_cek_plain_by_huaweikms(cmk_identity->client_cache_id, hex_cek_cipher_join_hash,
        cmk_identity->cmk_id_str, &huaweikms_resbody);
    free_cmkem_str(hex_cek_cipher_join_hash);
    if (ret != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    ret = find_value_from_http_resbody(huaweikms_resbody, key_list, &value_list);
    cmkem_free(huaweikms_resbody);
    if (ret != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    hex_cek_plain_join_hash = conv_str_to_cmkem_str(cmkem_list_val(value_list, 0));
    cmkem_list_free(value_list);
    if (hex_cek_plain_join_hash == NULL) {
        return POLICY_ERROR;
    }

    *cek_plain = hex_to_ustr(hex_cek_plain_join_hash);
    free_cmkem_str(hex_cek_plain_join_hash);
    
    return POLICY_BREAK;
}

int reg_cmke_manager_huwei_kms_main()
{
    CmkEntityManager huawei_kms = {
        create_cmk_obj_hookfunc,
        encrypt_cek_plain_hookfunc,
        decrypt_cek_cipher_hookfunc,
        NULL, /* drop_cmk_obj_hook_func: no need */
        NULL, /* post_create_cmk_obj_hook_func: no need */
    };
    
    return (reg_cmk_entity_manager(huawei_kms) == CMKEM_SUCCEED) ? 0 : -1;
}

#endif /* ENABLE_HUAWEI_KMS */
