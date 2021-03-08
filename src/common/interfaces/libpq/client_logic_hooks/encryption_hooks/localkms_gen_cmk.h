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
 * localkms_gen_cmk.h
 *      Use openssl to generate cmk, then encrypt cmk plain and store cmk cipher in file.
 *      At the same time, we need to store/read the iv and salt that are used to derive a key to
 *      encrypt/decrypt cmk plain.
 * 
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\encryption_hooks\localkms_gen_cmk.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef LOCAL_KMS_H
#define LOCAL_KMS_H

#include <stdio.h>
#include <limits.h>
#include "client_logic/client_logic_enums.h"
#if ((!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS)))
#include "libpq/cl_state.h"
#endif

const int MAX_KEY_PATH_LEN = 64;
const int MIN_KEY_PATH_LEN = 1;
const int ITERATE_ROUD = 10000;

const int DEFAULT_RSA_KEY_LEN = 3072;
const int KEY_METERIAL_LEN = 32;
const int DRIVED_KEY_LEN = 64;

const int KEY_FILE_HEADER_LEN = 20;

enum KmsErrType {
    SUCCEED = 0,
    FAILED,
    
    PATH_TOO_LONG,
    PATH_TOO_SHORT,
    CONTAIN_INVALID_CHAR,
    
    KEY_FILES_EXIST,
    KEY_FILES_NONEXIST,

    CREAT_FILE_ERR,
    OPEN_FILE_ERR,
    READ_FILE_ERR,
    WRITE_FILE_ERR,
    
    ENC_CEK_ERR,
    DEC_CEK_ERR,
    ENC_CMK_ERR,
    DEC_CMK_ERR,
    DERIVE_KEY_ERR,

    HEAP_ERR
};

typedef struct RsaKeyStr {
    unsigned char *pub_key;
    unsigned char *priv_key;
    size_t pub_key_len;
    size_t priv_key_len;
} RsaKeyStr;

typedef struct RealCmkPath {
    char real_pub_cmk_path[PATH_MAX];
    char real_priv_cmk_path[PATH_MAX];
} RealCmkPath;

typedef enum {
    CREATE_KEY_FILE,
    READ_KEY_FILE,
    REMOVE_KEY_FILE
} CheckKeyFileType;

#if ((!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS)))
extern void localkms_post_process(CEQueryType qry_type, ExecStatusType backend_exec_ret, const char *query_args);
#endif

/* safe check */
extern bool check_algo_type_kms(CmkAlgorithm algo_type);
extern KmsErrType check_file_exist(const char *real_file_path, CheckKeyFileType chk_type);
extern KmsErrType get_and_check_real_key_path(const char *cmk_path, RealCmkPath *real_cmk_path,
    CheckKeyFileType chk_type);

/* read and write files that store the ciphers of keys */
extern KmsErrType create_file_and_write(const char *real_path, const unsigned char *content, const size_t content_len,
    bool is_write_header);
extern KmsErrType read_content_from_file(const char *real_path, unsigned char *buf, const size_t buf_len,
    size_t *content_len);
extern void rm_cmk_store_file(RealCmkPath real_cmk_path);

/* generate cmk, encrypt cmk and write cmk cipher to file, or read cmk cipher from file and decrypt them */
extern RsaKeyStr *generate_cmk_kms();
extern void free_rsa_str(RsaKeyStr *rsa_key_str);
extern KmsErrType write_cmk_plain_kms(char *real_cmk_path, const size_t path_len, const unsigned char *cmk_plain,
    const size_t plain_len);
extern KmsErrType read_iv_and_salt(const char *real_cmk_path, unsigned char *iv, const size_t iv_len,
    unsigned char *salt, const size_t salt_len);
extern KmsErrType read_cmk_plain_kms(const char *real_cmk_path, unsigned char *cmk_plain, size_t *plain_len);

/* encrypt cek with cmk plain */
extern KmsErrType encrypt_cek_use_ras2048(const unsigned char *cek_plain, const size_t plain_len, 
    const char *real_pub_cmk_path, const size_t path_len, unsigned char *cek_cipher, size_t &cipher_len);
extern KmsErrType decrypt_cek_use_rsa2048(const unsigned char *cek_cipher, const size_t cipher_len,
    const char *real_priv_cmk_path, const size_t path_len, unsigned char *cek_plain, size_t *cek_plain_len);

/* handle kms error */
extern void handle_kms_err(KmsErrType err_type);

#endif