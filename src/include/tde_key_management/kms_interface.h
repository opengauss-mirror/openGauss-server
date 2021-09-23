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
 * kms_interface.h
 *    KMS interface is the external interface of KMS management
 *
 * IDENTIFICATION
 *    src/include/tde_key_management/kms_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SEC_KMS_INTERFACE_H
#define SEC_KMS_INTERFACE_H

#include "tde_key_management/ckms_message.h"

class KMSInterface : public BaseObject {
public:
    KMSInterface();
    ~KMSInterface();
    DekInfo* create_kms_dek(const char* cmk_id);
    char* get_kms_dek(const char* cmk_id, const char* dek_cipher);
    void get_kms_token();

private:
    AdvStrList* kms_restful_token();
    AdvStrList* kms_restful_agency_token(char* token);
    AdvStrList* kms_restful_get_dek(const char* cmk_id, const char* dek_cipher, ResetApiType api_type);
    char* parser_http_string(AdvStrList* http_msg_list, ResetApiType api_type);
    DekInfo* parser_http_array(AdvStrList* http_msg_list, ResetApiType api_type);
    char *find_resheader(AdvStrList *resheader_list, const char *resheader_type);

private:
    const int time_out = 15;
    /* token info */
    const char* content_type = "Content-Type:application/json;charset=UTF-8";
    const char* get_token_tag = "X-Subject-Token";
    const char* req_token_tag = "X-Auth-Token";
    /* IAM url info */
    const char* url_iam_head = "https://iam.";
    const char* url_token = ".myhuaweicloud.com/v3/auth/tokens";
    /* KMS get dek plaintext */
    const char* data_key = "data_key";
    /* KMS get dek ciphertext and plaintext */
    const char* plain_text = "plain_text";
    const char* cipher_text = "cipher_text";
    /* KMS create and get DEK url info */
    const char* url_kms_head = "https://kms.";
    const char* url_kms_path = ".myhuaweicloud.com/v1.0/";
    const char* url_create_dek = "/kms/create-datakey";
    const char* url_get_dek = "/kms/decrypt-datakey";
};

#endif
