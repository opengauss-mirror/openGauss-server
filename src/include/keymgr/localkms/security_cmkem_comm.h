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
 * security_cmkem_comm.h
 *      some common functions, include:
 *          1. error code and error process
 *          2. string process
 *          3. format and conversion
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/localkms/security_cmkem_comm.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CMKEM_COMM_H
#define CMKEM_COMM_H

#include <stdio.h>
#include "securec.h"
#include "securec_check.h"

/* error porcess */
typedef enum {
    CMKEM_SUCCEED = 0,
    CMKEM_UNKNOWN_ERR,
    /* check user input */
    CMKEM_CHECK_STORE_ERR,
    CMKEM_CHECK_ALGO_ERR,
    CMKEM_CHECK_CMK_ID_ERR,
    CMKEM_CHECK_IDENTITY_ERR,
    CMKEM_CHECK_INPUT_AUTH_ERR,
    CMKEM_CHECK_PROJECT_NAME_ERR,
    CMKEM_GET_TOKEN_ERR,
    /* openssl */
    CMKEM_WRITE_TO_BIO_ERR,
    CMKEM_READ_FROM_BIO_ERR,
    CMKEM_GET_RAND_NUM_ERR,
    CMKEM_DERIVED_KEY_ERR,
    CMKEM_GEN_RSA_KEY_ERR,
    CMKEM_GEN_SM2_KEY_ERR,
    CMKEM_RSA_DECRYPT_ERR,
    CMKEM_RSA_ENCRYPT_ERR,
    CMKEM_CHECK_HASH_ERR,
    CMKEM_EVP_ERR,
    CMKEM_SM2_ENC_ERR,
    CMKEM_SM2_DEC_ERR,
    /* cmk entity management hook functions */
    CMKEM_REG_CMK_MANAGER_ERR,
    CMKEM_REGISTRE_FUNC_ERR,
    CMKEM_CREATE_CMK_ERR,
    CMKEM_ENCRYPT_CEK_ERR,
    CMKEM_DECRYPT_CEK_ERR,
    CMKEM_DROP_CMK_ERR,
    /* system */
    CMKEM_GET_ENV_VAL_ERR,
    CMKEM_CHECK_ENV_VAL_ERR,
    CMKEM_SET_ENV_VALUE_ERR,
    /* gs_ktool */
    CMKEM_GS_KTOOL_ERR,
    /* huawei iam */
    CMKEM_IAM_SERVER_ERR,
    /* huawei kms */
    CMKEM_CHECK_CHACHE_ID_ERR,
    CMKEM_CACHE_IS_EMPTY,
    CMKEM_TOKEN_EXPIRED_ERR,
    CMKEM_KMS_SERVER_ERR,
    /* encrypt & decrypt */
    CMKEM_ENC_CMK_ERR,
    CMKEM_DEC_CMK_ERR,
    /* file */
    CMKEM_CREATE_FILE_ERR,
    CMKEM_FIND_FILE_ERR,
    CMKEM_OPEN_FILE_ERR,
    CMKEM_READ_FILE_ERR,
    CMKEM_READ_FILE_STATUS_ERR,
    CMKEM_WRITE_FILE_ERR,
    CMKEM_REMOVE_FILE_ERR,
    /* memory */
    CMKEM_MALLOC_MEM_ERR,
    CMKEM_CHECK_BUF_LEN_ERR,
    /* cjson */
    CMKEM_CJSON_PARSE_ERR,
    CMKEM_FIND_CSJON_ERR,
    CMKEM_SET_CJSON_VALUE_ERR,
    /* curl */
    CMKEM_CURL_INIT_ERR,
    CMKEM_CURL_ERR,
} CmkemErrCode; /* Client Master Key Entity Management Error Code */

const int MAX_CMKEM_ERRMSG_BUF_SIZE = 4096;
const int ITOA_BUF_LEN = 6;
const int HEX_SIZE = 2;

typedef struct CmkemErrMsg {
    CmkemErrCode err_code;
    const char *err_msg;
} CmkemErrMsg;

typedef struct CmkemStr {
    char *str_val;
    size_t str_len;
} CmkemStr;

typedef struct CmkemUStr {
    unsigned char *ustr_val;
    size_t ustr_len;
} CmkemUStr;

typedef struct CmkemStrNode {
    char *str_val;
    struct CmkemStrNode *next;
} CmkemStrNode;

typedef struct CmkemStrList {
    int node_cnt;
    CmkemStrNode *first_node;
} CmkemStrList;

typedef struct CmkIdentity {
    const char* cmk_store;
    const char *cmk_id_str;
    const char *cmk_algo;
} CmkIdentity;

#define cmkem_errmsg(format_str, ...)                                                            \
    do {                                                                                         \
        errno_t kms_err_rc = 0;                                                                  \
        char tmp_buf[MAX_CMKEM_ERRMSG_BUF_SIZE] = {0};                                           \
        kms_err_rc = sprintf_s(tmp_buf, MAX_CMKEM_ERRMSG_BUF_SIZE, (format_str), ##__VA_ARGS__); \
        securec_check_ss_c(kms_err_rc, "", "");                                                  \
        write_cmkem_errmsg(tmp_buf);                                                             \
    } while (0)

#define check_cmkem_ret(ret)          \
    do {                              \
        if ((ret) != CMKEM_SUCCEED) { \
            return ret;               \
        }                             \
    } while (0)

#define cmkem_free(ptr)      \
    do {                     \
        if ((ptr) != NULL) { \
            free((ptr));     \
            (ptr) = NULL;    \
        }                    \
    } while (0)

/* error process */
extern void write_cmkem_errmsg(const char *errmsg);
extern const char *get_cmkem_errmsg(CmkemErrCode err_code);
extern void erase_data(void *data, size_t data_len);

/* advanced string */
extern void free_cmkem_str(CmkemStr *cmkem_str_ptr);
extern CmkemUStr *malloc_cmkem_ustr(size_t key_buf_len);
extern void free_cmkem_ustr(CmkemUStr *cmkem_ustr);
extern void free_cmkem_ustr_with_erase(CmkemUStr *cmkem_ustr);

extern CmkemStr *ustr_to_hex(CmkemUStr *ustr);
extern CmkemUStr *hex_to_ustr(CmkemStr *hex);

#endif /* CMKEM_COMM_H */
