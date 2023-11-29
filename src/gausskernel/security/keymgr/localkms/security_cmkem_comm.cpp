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
 * security_cmkem_comm.cpp
 *      some common functions, include:
 *          1. error code and error process
 *          2. string process
 *          3. format and conversion
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/localkms/security_cmkem_comm.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "keymgr/localkms/security_cmkem_comm.h"

#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include "securec.h"
#include "securec_check.h"
#include "keymgr/comm/security_error.h"

static char cmkem_errmsg_buf[MAX_CMKEM_ERRMSG_BUF_SIZE] = {0};

/*
 * in some cases, where there is an error, we will report the error in detail.
 * if not, we will use simple and general error information according to the error code
 */
static CmkemErrMsg simple_errmsg_list[] = {
    {CMKEM_SUCCEED, "no error message, the result is 'SUCCEED'."},
    {CMKEM_UNKNOWN_ERR, "unknown error."},
    {CMKEM_CHECK_STORE_ERR, "failed to check the value of arg: 'KEY_STORE'."},
    {CMKEM_CHECK_ALGO_ERR, "failed to check the value of arg: 'ALGORITHM'."},
    {CMKEM_CHECK_CMK_ID_ERR, "failed to check the value of arg: 'KEY_PATH'."},
    {CMKEM_CHECK_IDENTITY_ERR, "faiel to check the usability of cmk entity."},
    {CMKEM_CHECK_INPUT_AUTH_ERR, "the authentication parameters are illegal."},
    {CMKEM_CHECK_PROJECT_NAME_ERR, "failed to check HuaWei KMS project name."},
    {CMKEM_GET_TOKEN_ERR, "failed to get token."},
    {CMKEM_WRITE_TO_BIO_ERR, "failed to wirite key to BIO buffer."},
    {CMKEM_READ_FROM_BIO_ERR, "failed to read key from BIO buffer."},
    {CMKEM_GET_RAND_NUM_ERR, "failed to generate a random num."},
    {CMKEM_DERIVED_KEY_ERR, "failed to derived key."},
    {CMKEM_GEN_RSA_KEY_ERR, "failed to create rsa key pair."},
    {CMKEM_GEN_SM2_KEY_ERR, "failed to create sm2 key pair."},
    {CMKEM_RSA_DECRYPT_ERR, "failed to decrypt cipher with RSA algorithm."},
    {CMKEM_RSA_ENCRYPT_ERR, "failed to encrypt plain with RSA algorithm."},
    {CMKEM_CHECK_HASH_ERR, "failed check the hash of data."},
    {CMKEM_EVP_ERR, "openssl evp error."},
    {CMKEM_SM2_ENC_ERR, "failed to encrypt plain with SM2 algorithm."},
    {CMKEM_SM2_DEC_ERR, "failed to decrypt cipher with SM2 algorithm."},
    {CMKEM_REG_CMK_MANAGER_ERR, "failed to register cmk entity manager."},
    {CMKEM_REGISTRE_FUNC_ERR, "failed to register hook function of cmk manager."},
    {CMKEM_CREATE_CMK_ERR, "failed to create client master key object."},
    {CMKEM_ENCRYPT_CEK_ERR, "failed to encrypt column encryption key plain with client master key."},
    {CMKEM_DECRYPT_CEK_ERR, "failed to decrypt column encryption key cipher with client master key."},
    {CMKEM_DROP_CMK_ERR, "failed to drop client master key object."},
    {CMKEM_GET_ENV_VAL_ERR, "failed to get environment value from system."},
    {CMKEM_CHECK_ENV_VAL_ERR, "the environment value is illegal."},
    {CMKEM_SET_ENV_VALUE_ERR, "failed to set global environment value."},
    {CMKEM_GS_KTOOL_ERR, "gs_ktool error."},
    {CMKEM_IAM_SERVER_ERR, "Huawei IAM server error."},
    {CMKEM_CHECK_CHACHE_ID_ERR, "the client cache id is invalid."},
    {CMKEM_CACHE_IS_EMPTY, "the specific cache block is empty."},
    {CMKEM_TOKEN_EXPIRED_ERR, "the token has expired."},
    {CMKEM_KMS_SERVER_ERR, "Huawei KMS server error."},
    {CMKEM_ENC_CMK_ERR, "failed to encrypt cmk entity plain."},
    {CMKEM_DEC_CMK_ERR, "failed to decrypt cmk entity cipher."},
    {CMKEM_CREATE_FILE_ERR, "failed to create file."},
    {CMKEM_FIND_FILE_ERR, "failed to find file."},
    {CMKEM_OPEN_FILE_ERR, "failed to open file."},
    {CMKEM_READ_FILE_ERR, "failed to read file."},
    {CMKEM_READ_FILE_STATUS_ERR, "failed to read file status."},
    {CMKEM_WRITE_FILE_ERR, "failed to write to file."},
    {CMKEM_REMOVE_FILE_ERR, "failed to remove file."},
    {CMKEM_MALLOC_MEM_ERR, "failed to malloc memory from heap."},
    {CMKEM_CHECK_BUF_LEN_ERR, "the buffer size is too short."},
    {CMKEM_CJSON_PARSE_ERR, "failed to parser string with 'CJSON'."},
    {CMKEM_FIND_CSJON_ERR, "failed to find node from cjson tree."},
    {CMKEM_SET_CJSON_VALUE_ERR, "failed to set the value of cjson tree node."},
    {CMKEM_CURL_INIT_ERR, "failed to init curl."},
    {CMKEM_CURL_ERR, "curl error."},
};

/*
 * write error message to err_msg buffer, if need error message, can chose:
 *      1. cmkem_list_print error message.
 *      2. copy/dump error message to new err_msg buffer.
 */
void write_cmkem_errmsg(const char *errmsg)
{
    errno_t rc = 0;

    if (cmkem_errmsg_buf == NULL) {
        return;
    }
    
    rc = memset_s(cmkem_errmsg_buf, MAX_CMKEM_ERRMSG_BUF_SIZE, 0, sizeof(cmkem_errmsg_buf));
    km_securec_check(rc, "", "");

    rc = sprintf_s(cmkem_errmsg_buf, MAX_CMKEM_ERRMSG_BUF_SIZE, "%s", errmsg);
    km_securec_check_ss(rc, "", "");
}

const char *get_cmkem_errmsg(CmkemErrCode err_code)
{
    if (cmkem_errmsg_buf == NULL) {
        return NULL;
    }
    
    if (strlen(cmkem_errmsg_buf) == 0) {
        return simple_errmsg_list[err_code].err_msg;
    }
    
    return (const char *)cmkem_errmsg_buf;
}

void erase_data(void *data, size_t data_len)
{
    errno_t rc = 0;
    
    if (data == NULL) {
        return;
    }

    rc = memset_s(data, data_len, 0, data_len);
    km_securec_check(rc, "", "");
}

void free_cmkem_str(CmkemStr *advstr_ptr)
{
    if (advstr_ptr != NULL) {
        km_safe_free(advstr_ptr->str_val);
        km_safe_free(advstr_ptr);
    }

    advstr_ptr = NULL;
}

CmkemUStr *malloc_cmkem_ustr(size_t ust_buf_len)
{
    if (ust_buf_len == 0) {
        return NULL;
    }
    
    CmkemUStr *key_str = (CmkemUStr *)km_alloc_zero(sizeof(CmkemUStr));
    if (key_str == NULL) {
        return NULL;
    }

    key_str->ustr_val = (unsigned char *)km_alloc_zero(ust_buf_len);
    if (key_str->ustr_val == NULL) {
        km_safe_free(key_str);
        return NULL;
    }

    key_str->ustr_len = 0;
    return key_str;
}

void free_cmkem_ustr(CmkemUStr *cmkem_ustr)
{
    if (cmkem_ustr != NULL) {
        km_safe_free(cmkem_ustr->ustr_val);
        km_safe_free(cmkem_ustr);
    }

    cmkem_ustr = NULL;
}

void free_cmkem_ustr_with_erase(CmkemUStr *cmkem_ustr)
{
    if (cmkem_ustr != NULL) {
        erase_data(cmkem_ustr->ustr_val, cmkem_ustr->ustr_len);
        km_safe_free(cmkem_ustr->ustr_val);
        km_safe_free(cmkem_ustr);
    }

    cmkem_ustr = NULL;
}
