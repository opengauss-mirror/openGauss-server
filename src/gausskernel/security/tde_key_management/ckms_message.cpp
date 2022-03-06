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
 * ckms_message.cpp
 *    Cloud KMS and IAM message is the external interface of JSON management
 *
 * IDENTIFICATION
 *    src/gausskernel/security/tde_key_management/ckms_message.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <fstream>
#include <iostream>
#include "tde_key_management/ckms_message.h"
#include "utils/elog.h"
#include "knl/knl_instance.h"
#include "utils/memutils.h"
#include "./kms_httpmsg.ini"

namespace TDE {
using namespace std;
CKMSMessage::CKMSMessage()
{
    tde_message_mem = nullptr;
    tde_token = NULL;
    tde_agency_token = NULL;
    token_info = NULL;
    agency_token_info = NULL;
    kms_info = NULL;
    token_timestamp = 0;
}

CKMSMessage::~CKMSMessage()
{
    clear();
}

void CKMSMessage::init()
{
    if (tde_message_mem == nullptr) {
        tde_message_mem = AllocSetContextCreate(g_instance.cache_cxt.global_cache_mem, "TDE_MESSAGE_CONTEXT",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    }
}

void CKMSMessage::clear()
{
    errno_t rc = 0;

    if (token_info->password != NULL) {
        rc = memset_s(token_info->password, strlen(token_info->password), 0, strlen(token_info->password));
        securec_check(rc, "\0", "\0");
    }
    pfree_ext(tde_token);
    pfree_ext(tde_agency_token);

    pfree_ext(token_info->domain_name);
    pfree_ext(token_info->password);
    pfree_ext(token_info->project_name);
    pfree_ext(token_info->user_name);
    pfree_ext(token_info);

    pfree_ext(agency_token_info->domain_name);
    pfree_ext(agency_token_info->agency_name);
    pfree_ext(agency_token_info->project_name);
    pfree_ext(agency_token_info);

    pfree_ext(kms_info->project_id);
    pfree_ext(kms_info->project_name);
    pfree_ext(kms_info);
}

bool CKMSMessage::check_token_valid()
{
    TimestampTz current_time = 0;
    current_time = GetCurrentTimestamp();
    if (tde_token == NULL) {
        return false;
    }
    if (tde_agency_token == NULL) {
        return false;
    }
    if ((current_time - token_timestamp) > (token_valid_hours * USECS_PER_HOUR)) {
        return false;
    }
    return true;
}

char* CKMSMessage::read_kms_info_from_file()
{
    char file_path[PATH_MAX] = {0};
    char* data_directory = NULL;
    char* buffer = NULL;
    int path_len = 0;
    int json_len = 0;
    int max_json_len = 1024;
    errno_t rc = EOK;

    data_directory = g_instance.attr.attr_common.data_directory;
    if (data_directory == NULL) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("get gaussdb data directory path is NULL"), errdetail("N/A"), 
            errcause("data directory path not set"), 
            erraction("check if guc data_directory is exist")));
        return NULL;
    }
    path_len = strlen(data_directory) + strlen(tde_config) + strlen(token_file);
    rc = snprintf_s(file_path, PATH_MAX, path_len, "%s%s%s", data_directory, tde_config, token_file);
    securec_check_ss(rc, "\0", "\0");
    fstream json_file(file_path, ios::in | ios::binary);
    if (!json_file) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_FILE_READ_FAILED), 
            errmsg("unable to open kms_iam_info.json file"), errdetail("file path: %s", file_path), 
            errcause("file not exist or broken"), erraction("check the kms_iam_info.json file")));
    }

    json_file.seekg(0, ios::end);
    json_len = json_file.tellg();
    if (json_len > max_json_len) {
        json_file.close();
        ereport(ERROR,
            (errmodule(MOD_SEC_TDE), errcode(ERRCODE_FILE_READ_FAILED),
                errmsg("kms_iam_info.json file length is bigger than max_len"),
                    errdetail("file path: $TDE_PATH/tde_config/kms_iam_info.json"), errcause("file context is wrong"),
                        erraction("check the kms_iam_info.json file")));
    }
    json_file.seekg(0, ios::beg);
    buffer = (char*)palloc0(json_len + 1);
    json_file.read(buffer, json_len + 1);
    json_file.close();
    return buffer;
}

GS_UCHAR* CKMSMessage::get_cipher_rand()
{
    GS_UCHAR* plain_text = NULL;
    KeyMode keymode = SERVER_MODE;
    char* key_dir = NULL;
    char* data_directory = NULL;
    int path_len = 0;
    errno_t rc = EOK;

    data_directory = g_instance.attr.attr_common.data_directory;
    if (data_directory == NULL) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("get gaussdb data directory path is NULL"), errdetail("N/A"), 
            errcause("data directory path not set"), 
            erraction("check if guc data_directory is exist")));
        return NULL;
    }
    path_len = strlen(data_directory) + strlen(tde_config) + strlen(key) + 1;
    key_dir = (char*)palloc0(path_len);
    rc = snprintf_s(key_dir, path_len, (path_len - 1), "%s%s%s", data_directory, tde_config, key);
    securec_check_ss(rc, "\0", "\0");
    plain_text = (GS_UCHAR*)palloc0(CIPHER_LEN + 1);
    decode_cipher_files(keymode, NULL, key_dir, plain_text);
    pfree_ext(key_dir);
    if (strlen((char*)plain_text) == 0) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("can not get password plaintext"), errdetail("N/A"), errcause("file not exist or broken"), 
            erraction("check the password cipher rand file")));
    }
    return plain_text;
}

void CKMSMessage::fill_token_info(cJSON* internal_json)
{
    char* username = NULL;
    char* password = NULL;
    char* domain_name = NULL;
    char* project_name = NULL;
    errno_t rc = EOK;

    if ((cJSON_GetObjectItem(internal_json, "username") == NULL) || 
        (cJSON_GetObjectItem(internal_json, "domain_name") == NULL) || 
        (cJSON_GetObjectItem(internal_json, "project_name") == NULL)) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("IAM info json key is NULL"), errdetail("N/A"), 
            errcause("IAM info value error"), 
            erraction("check tde_config kms_iam_info.json file")));
    }
    MemoryContext old = MemoryContextSwitchTo(tde_message_mem);
    token_info = (TokenInfo*)palloc0(sizeof(TokenInfo));
    username = cJSON_GetObjectItem(internal_json, "username")->valuestring;
    token_info->user_name = (char*)palloc0(strlen(username) + 1);
    rc = memcpy_s(token_info->user_name, (strlen(username) + 1), username, (strlen(username) + 1));
    securec_check(rc, "\0", "\0");
    domain_name = cJSON_GetObjectItem(internal_json, "domain_name")->valuestring;
    token_info->domain_name = (char*)palloc0(strlen(domain_name) + 1);
    rc = memcpy_s(token_info->domain_name, (strlen(domain_name) + 1), domain_name, (strlen(domain_name) + 1));
    securec_check(rc, "\0", "\0");
    project_name = cJSON_GetObjectItem(internal_json, "project_name")->valuestring;
    token_info->project_name = (char*)palloc0(strlen(project_name) + 1);
    rc = memcpy_s(token_info->project_name, (strlen(project_name) + 1), project_name, (strlen(project_name) + 1));
    securec_check(rc, "\0", "\0");
    password = (char*)get_cipher_rand();
    if (password == NULL) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("get internal password is NULL"), errdetail("N/A"), errcause("cipher rand file missing"), 
            erraction("check password cipher rand file")));
        return;
    }
    token_info->password = (char*)palloc0(strlen(password) + 1);
    rc = memcpy_s(token_info->password, (strlen(password) + 1), password, (strlen(password) + 1));
    securec_check(rc, "\0", "\0");
    MemoryContextSwitchTo(old);
    rc = memset_s(password, strlen(password), 0, strlen(password));
    securec_check(rc, "\0", "\0");
    pfree_ext(password);
    return;
}

void CKMSMessage::fill_agency_info(cJSON* agency_json)
{
    char* domain_name = NULL;
    char* agency_name = NULL;
    char* project_name = NULL;
    errno_t rc = EOK;

    if ((cJSON_GetObjectItem(agency_json, "domain_name") == NULL) || 
        (cJSON_GetObjectItem(agency_json, "agency_name") == NULL) || 
        (cJSON_GetObjectItem(agency_json, "project_name") == NULL)) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("IAM info json key is NULL"), errdetail("N/A"), 
            errcause("IAM info value error"), 
            erraction("check tde_config kms_iam_info.json file")));
    }
    MemoryContext old = MemoryContextSwitchTo(tde_message_mem);
    agency_token_info = (AgencyTokenInfo*)palloc0(sizeof(AgencyTokenInfo));
    domain_name = cJSON_GetObjectItem(agency_json, "domain_name")->valuestring;
    agency_token_info->domain_name = (char*)palloc0(strlen(domain_name) + 1);
    rc = memcpy_s(agency_token_info->domain_name, (strlen(domain_name) + 1), domain_name, 
        (strlen(domain_name) + 1));
    securec_check(rc, "\0", "\0");
    agency_name = cJSON_GetObjectItem(agency_json, "agency_name")->valuestring;
    agency_token_info->agency_name = (char*)palloc0(strlen(agency_name) + 1);
    rc = memcpy_s(agency_token_info->agency_name, (strlen(agency_name) + 1), agency_name, 
        (strlen(agency_name) + 1));
    securec_check(rc, "\0", "\0");
    project_name = cJSON_GetObjectItem(agency_json, "project_name")->valuestring;
    agency_token_info->project_name = (char*)palloc0(strlen(project_name) + 1);
    rc = memcpy_s(agency_token_info->project_name, (strlen(project_name) + 1), project_name, 
        (strlen(project_name) + 1));
    securec_check(rc, "\0", "\0");
    MemoryContextSwitchTo(old);
    return;
}

void CKMSMessage::fill_kms_info(cJSON* kms_json)
{
    char* project_name = NULL;
    char* project_id = NULL;
    errno_t rc = EOK;

    if ((cJSON_GetObjectItem(kms_json, "project_name") == NULL) || 
        (cJSON_GetObjectItem(kms_json, "project_id") == NULL)) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("KMS info json key is NULL"), errdetail("N/A"), 
            errcause("KMS info value error"), 
            erraction("check tde_config kms_iam_info.json file")));
    }
    MemoryContext old = MemoryContextSwitchTo(tde_message_mem);
    kms_info = (KmsInfo*)palloc0(sizeof(KmsInfo));
    project_name = cJSON_GetObjectItem(kms_json, "project_name")->valuestring;
    kms_info->project_name = (char*)palloc0(strlen(project_name) + 1);
    rc = memcpy_s(kms_info->project_name, (strlen(project_name) + 1), project_name, 
        (strlen(project_name) + 1));
    securec_check(rc, "\0", "\0");
    project_id = cJSON_GetObjectItem(kms_json, "project_id")->valuestring;
    kms_info->project_id = (char*)palloc0(strlen(project_id) + 1);
    rc = memcpy_s(kms_info->project_id, (strlen(project_id) + 1), project_id, 
        (strlen(project_id) + 1));
    securec_check(rc, "\0", "\0");
    MemoryContextSwitchTo(old);
    return;
}

void CKMSMessage::parser_json_file(char* buffer)
{
    cJSON* json_root = NULL;
    cJSON* internal_json = NULL;
    cJSON* agency_json = NULL;
    cJSON* kms_json = NULL;

    json_root = cJSON_Parse(buffer);
    if (!json_root) {
        pfree_ext(buffer);
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_FILE_READ_FAILED), 
            errmsg("unable to get json file"), errdetail("N/A"), errcause("parse json file failed"), 
            erraction("check the kms_iam_info.json file format")));
    }
    internal_json = cJSON_GetObjectItem(json_root, "internal_user_info");
    if (internal_json) {
        fill_token_info(internal_json);
    }
    agency_json = cJSON_GetObjectItem(json_root, "agency_user_info");
    if (agency_json) {
        fill_agency_info(agency_json);
    }
    kms_json = cJSON_GetObjectItem(json_root, "kms_info");
    if (kms_json) {
        fill_kms_info(kms_json);
    }
    pfree_ext(buffer);
    cJSON_Delete(json_root);
    return;
}

void CKMSMessage::load_user_info()
{
    char* read_buffer = NULL;

    /* read KMS and IAM info from kms_iam_info.json */
    read_buffer = read_kms_info_from_file();
    /* parser json file */
    parser_json_file(read_buffer);
    return;
}
char* CKMSMessage::get_kms_json(ReplaceJsonValue input_json[], KmsHttpMsgType json_type, size_t count)
{
    HttpErrCode ret = TDE_HTTP_SUCCEED;
    cJSON* json = NULL;
    char* json_string = NULL;
    char* temp_json = NULL;
    errno_t rc = EOK;

    /* json_type is KmsHttpMsgType */
    json = get_json_temp(json_type);
    if (json == NULL) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("get JSON tree is NULL"), errdetail("N/A"), errcause("get KMS JSON tree failed"), 
            erraction("check input prarmeter or config.ini file")));
        return NULL;
    }
    ret = traverse_jsontree_with_raplace_value(json, input_json, count);
    if (ret != TDE_HTTP_SUCCEED) {
        cJSON_Delete(json);
        return NULL;
    }
    temp_json = cJSON_Print(json);
    if (temp_json == NULL) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("get JSON tree is NULL"), errdetail("N/A"), errcause("get KMS JSON tree failed"), 
            erraction("check input prarmeter or config.ini file")));
    }
    json_string = (char*)palloc0(strlen(temp_json) + 1);
    rc = memcpy_s(json_string, (strlen(temp_json) + 1), temp_json, (strlen(temp_json) + 1));
    securec_check(rc, "\0", "\0");
    cJSON_free(temp_json);
    cJSON_Delete(json);
    return json_string;
}

char* CKMSMessage::get_iam_token_json()
{
    char* token = NULL;
    size_t count = 0;
    ReplaceJsonValue json[] = {
        {"$user_name$", token_info->user_name},
        {"$password$", token_info->password},
        {"$domain_name$", token_info->domain_name},
        {"$project_name$", token_info->project_name},
    };
    count = sizeof(json) / sizeof(json[0]);
    token = get_kms_json(json, IAM_AUTH_REQ, count);
    return token;
}

char* CKMSMessage::get_iam_agency_token_json()
{
    char* agency_token = NULL;
    size_t count = 0;
    ReplaceJsonValue json[] = {
        {"$domain_name$", agency_token_info->domain_name},
        {"$agency_name$", agency_token_info->agency_name},
        {"$project_name$", agency_token_info->project_name},
    };
    count = sizeof(json) / sizeof(json[0]);
    agency_token = get_kms_json(json, IAM_AGENCY_TOKEN_REQ, count);
    return agency_token;
}

char* CKMSMessage::get_create_dek_json(const char* cmk_id)
{
    char* req_body = NULL;
    size_t count = 0;
    ReplaceJsonValue json[] = {
        {"$cmk_id$", cmk_id},
    };
    count = sizeof(json) / sizeof(json[0]);
    req_body = get_kms_json(json, TDE_GEN_DEK_REQ, count);
    return req_body;
}

char* CKMSMessage::get_decrypt_dek_json(const char* cmk_id, const char* dek_cipher)
{
    char* req_body = NULL;
    size_t count = 0;
    ReplaceJsonValue json[] = {
        {"$dek_cipher$", dek_cipher},
        {"$cmk_id$", cmk_id},
    };
    count = sizeof(json) / sizeof(json[0]);
    req_body = get_kms_json(json, TDE_DEC_DEK_REQ, count);
    return req_body;
}

bool CKMSMessage::save_token(const char* token, const char* agency_token)
{
    errno_t rc = EOK;

    if ((token == NULL) || (agency_token == NULL)) {
        return false;
    }
    MemoryContext old = MemoryContextSwitchTo(tde_message_mem);
    if (tde_token == NULL) {
        tde_token = (char*)palloc0(strlen(token) + 1);
    }
    rc = memcpy_s(tde_token, (strlen(token) + 1), token, (strlen(token) + 1));
    securec_check(rc, "\0", "\0");
    if (tde_agency_token == NULL) {
        tde_agency_token = (char*)palloc0(strlen(agency_token) + 1);
    }
    rc = memcpy_s(tde_agency_token, (strlen(agency_token) + 1), agency_token, (strlen(agency_token) + 1));
    securec_check(rc, "\0", "\0");
    token_timestamp = GetCurrentTimestamp();
    MemoryContextSwitchTo(old);
    return true;
}

HttpErrCode CKMSMessage::traverse_jsontree_with_raplace_value(cJSON *json_tree, ReplaceJsonValue replace_rules[], 
    size_t rule_cnt)
{
    char *new_value = NULL;
    HttpErrCode ret = TDE_HTTP_SUCCEED;

    if (json_tree == NULL) {
        return TDE_HTTP_SUCCEED;
    }
    if (cJSON_IsString(json_tree)) {
        for (size_t i = 0; i < rule_cnt; i++) {
            if (cJSON_GetStringValue(json_tree) == NULL) {
                ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
                        errmsg("failed to get json tree"), errdetail("N/A"), 
                        errcause("config.ini json tree error"), 
                        erraction("check input prarmeter or config.ini file")));
            }
            if (strcmp(replace_rules[i].src_value, cJSON_GetStringValue(json_tree)) == 0) {
                new_value = cJSON_SetValuestring(json_tree, replace_rules[i].dest_value);
                if (new_value == NULL) {
                    ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
                        errmsg("failed to set the value of json tree"), errdetail("N/A"), 
                        errcause("config.ini json tree error"), 
                        erraction("check input prarmeter or config.ini file")));
                    return TDE_SET_CJSON_VALUE_ERR;
                }
            }
        }
    }
    ret = traverse_jsontree_with_raplace_value(json_tree->next, replace_rules, rule_cnt);
    if (ret != TDE_HTTP_SUCCEED) {
        return ret;
    }
    return traverse_jsontree_with_raplace_value(json_tree->child, replace_rules, rule_cnt);
}

cJSON *CKMSMessage::get_json_temp(KmsHttpMsgType json_tree_type)
{
    switch (json_tree_type) {
        case IAM_AUTH_REQ:
            return cJSON_Parse(iam_auth_token_req);
        case IAM_AGENCY_TOKEN_REQ:
            return cJSON_Parse(iam_agency_token_req);
        case TDE_GEN_DEK_REQ:
            return cJSON_Parse(kms_create_dek_req);
        case TDE_DEC_DEK_REQ:
            return cJSON_Parse(kms_decrypt_dek_req);
        default:
            break;
    }
    return NULL;
}
}
