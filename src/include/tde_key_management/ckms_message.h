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
 * ckms_message.h
 *    Cloud KMS and IAM message is the external interface of JSON management
 *
 * IDENTIFICATION
 *    src/include/tde_key_management/ckms_message.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SEC_CKMS_MESSAGE_H
#define SEC_CKMS_MESSAGE_H

#include "tde_key_management/data_common.h"
#include "tde_key_management/http_common.h"
#include "cipher.h"

namespace TDE {
class CKMSMessage : public BaseObject {
public:
    static CKMSMessage& get_instance()
    {
        static CKMSMessage instance;
        return instance;
    }
    void init();
    void clear();
    char* get_iam_token_json();
    char* get_iam_agency_token_json();
    char* get_create_dek_json(const char* cmk_id);
    char* get_decrypt_dek_json(const char* cmk_id, const char* dek_cipher);
    bool check_token_valid();
    bool save_token(const char* token, const char* agency_token);
    void load_user_info();

public:
    char* tde_token;
    char* tde_agency_token;
    TimestampTz token_timestamp;
    /* internal user B info */
    TokenInfo* token_info;
    /* user A info */
    AgencyTokenInfo* agency_token_info;
    /* KMS info */
    KmsInfo* kms_info;

private:
    CKMSMessage();
    ~CKMSMessage();
    char* get_kms_json(ReplaceJsonValue input_json[], KmsHttpMsgType json_type, size_t count);
    HttpErrCode traverse_jsontree_with_raplace_value(cJSON *json_tree, ReplaceJsonValue replace_rules[], 
        size_t rule_cnt);
    cJSON *get_json_temp(KmsHttpMsgType json_tree_type);
    char* read_kms_info_from_file();
    void parser_json_file(char* buffer);
    void fill_kms_info(cJSON* kms_json);
    void fill_agency_info(cJSON* agency_json);
    void fill_token_info(cJSON* internal_json);
    GS_UCHAR* get_cipher_rand();

private:
    MemoryContext tde_message_mem;
    const int token_valid_hours = 23;
    const char* token_file = "kms_iam_info.json";
    const char* tde_config = "/tde_config/";
    const char* key = "key_0";
};
}
#endif

