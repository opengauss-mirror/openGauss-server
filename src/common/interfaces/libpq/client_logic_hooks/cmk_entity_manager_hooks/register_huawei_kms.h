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
 * register_huawei_kms.h
 *      Huawei KMS is an online key management service provided by Huawei Cloud.
 *      We support Huawei KMS to provide cmk entities for us, and we can send CEK entities
 *      to it for encrypting and decrypting
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/register_huawei_kms.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REGISTER_HUAWEI_KMS_H
#define REGISTER_HUAWEI_KMS_H

#include "cmkem_comm.h"
#include "cmkem_comm_http.h"

const int USER_NAME_BUF_LEN = 24;
const int PASSWD_BUF_LEN = 24;
const int DOMAIN_NAME_BUF_LEN = 24;
const int PROJ_NAME_BUF_LEN = 24;
const int PROJ_ID_BUF_LEN = 128;
const int MAX_TOKEN_BUF_LEN = 10240;
const int DEFAULT_HTTP_TIME_OUT = 15;
const int KMS_CACHE_TBL_LEN = 1024;
const int KMS_PLAIN_PACKET_LEN = 16;

typedef enum {
    IAM_AUTH_REQ = 0,
    KMS_SELECT_CMK_REQ,
    KMS_ENC_CEK_REQ,
    KMS_DEC_CEK_REQ,
} KmsHttpMsgType;

extern CmkemErrCode get_kms_err_type(const char *kms_errmsg_body);
extern cJSON *get_json_temp(KmsHttpMsgType json_tree_type);
extern char *get_iam_auth_req_jsontemp(const char *user_name, const char *password, const char *domain_name,
    const char *project_name);
extern char *get_select_cmk_jsontemp(const char *key_id);
extern char *get_enc_cek_plain_jsontemp(const char *cmk_id, const char *cek_plain, size_t cek_plain_len);
extern char *get_dec_cek_cipher_jsontemp(const char *cmk_id, const char *cek_cipher, size_t cek_cipher_len);
extern void get_iam_url(const char *project_name, char *url_buf, size_t buf_len);
extern CmkemErrCode get_kms_url(size_t cache_id, KmsHttpMsgType kms_msg_type, char *url_buf, size_t url_buf_len);
extern CmkemErrCode get_url_file_path(KmsHttpMsgType kms_msg_type, const char *project_id, char *url_file_path_buf,
    size_t buf_len);

extern void free_kms_cache(size_t cache_id);
extern CmkemErrCode set_kms_cache_auth_info(size_t cache_id, const char *key, const char *value);
extern char *get_kms_cache_token(size_t cache_id);
extern CmkemErrCode check_token_exist(size_t cache_id);
extern CmkemErrCode refresh_cahced_token(size_t cache_id);

extern CmkemErrCode catch_iam_server_err(CmkemStrList *http_res_list, HttpStatusCode http_stat_code);
extern CmkemErrCode catch_kms_server_err(size_t cache_id, HttpReqMsg* http_req_msg, HttpConfig *http_config,
    CmkemStrList **http_res_list, HttpStatusCode *http_stat_code);
extern size_t get_hexcek_len_from_rawcek_len(size_t raw_cek_len);
extern size_t get_rawcek_len_from_hexcek_len(size_t hex_cek_len);
extern size_t get_rawcek_len_from_hex_kms_cek_len(size_t hex_cek_len);

extern CmkemErrCode select_cmk_entity_from_huaweikms(size_t cache_id, const char *cmk_id, char **huaweikms_resbody);
extern CmkemErrCode encrypt_cek_plain_by_huaweikms(size_t cache_id, CmkemStr *hex_cek_plain_join_hash,
    const char *cmk_id, char **huaweikms_resbody);
extern CmkemErrCode decrypt_cek_plain_by_huaweikms(size_t cache_id, CmkemStr *hex_cek_cipher_join_hash,
    const char *cmk_id, char **huaweikms_resbody);

extern int reg_cmke_manager_huwei_kms_main();


#endif /* REGISTER_HUAWEI_KMS_H */
