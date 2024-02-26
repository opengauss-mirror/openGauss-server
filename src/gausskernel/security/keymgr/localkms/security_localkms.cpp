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
 * security_localkms.cpp
 *      localkms is a lightweight key management component.
 *      different from other key management tools and services, localkms automatically generates CMKE when CREATE CMKO,
 *      it cannot manage key entities independently.
 *      localkms use openssl to generate cmk, then encrypt cmk plain and store cmk cipher in file.
 *      at the same time, we need to store/read the iv and salt that are used to derive a key to
 *      encrypt/decrypt cmk plain.
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/localkms/security_localkms.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "keymgr/localkms/security_localkms.h"
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <limits.h>
#include <openssl/rsa.h>
#include <openssl/pem.h>
#include "securec.h"
#include "securec_check.h"
#include "keymgr/comm/security_utils.h"
#include "keymgr/localkms/security_cmkem_comm_algorithm.h"
#include "keymgr/localkms/security_file_enc.h"
#include "keymgr/encrypt/security_sm2_enc_key.h"
#include "keymgr/security_key_mgr.h"

const int MAX_KEY_PATH_LEN = 64;
const int MIN_KEY_PATH_LEN = 1;

static const char *g_support_algo[] = {"RSA_2048", "RSA_3072", "SM2", NULL};

LocalKmsMgr *localkms_new(KmErr *err)
{
    LocalKmsMgr *kms = (LocalKmsMgr *)km_alloc_zero(sizeof(LocalKmsMgr));
    if (kms == NULL) {
        return NULL;
    }

    kms->kmgr.err = err;
    CmkemErrCode ret = set_global_env_value();
    if (ret != CMKEM_SUCCEED) {
        km_free(kms);
        km_err_msg(err, "%s", get_cmkem_errmsg(ret));
        return NULL;
    }
    kms->getpath = 1;
    
    return kms;
}

void localkms_free(LocalKmsMgr *kms)
{
    if (kms == NULL) {
        return;
    }

    km_free(kms);
}

/* 
 * check the KEY_PATH value in SQL :
 *      CREATE CLIENT MASTER KEY xxx (KEY_STORE = localkms, KEY_PATH = xxx, ...)
 * only support 0-9, a-z, A_Z, '_', '.', '-'
 */
static CmkemErrCode check_key_path_value(const char *key_path_value)
{
    char cur_char = 0;
    const char legal_symbol[] = {'_', '.', '-'};

    for (size_t i = 0; i < strlen(key_path_value); i++) {
        cur_char = key_path_value[i];
        if (cur_char >= '0' && cur_char <= '9') {
            continue;
        } else if (cur_char >= 'A' && cur_char <= 'Z') {
            continue;
        } else if (cur_char >= 'a' && cur_char <= 'z') {
            continue;
        } else if (strchr(legal_symbol, cur_char) != NULL) {
                continue;
        } else {
            cmkem_errmsg("the key_path value '%s' contains invalid charachter '%c'.", key_path_value, cur_char);
            return CMKEM_CHECK_CMK_ID_ERR;
        }
    }

    return CMKEM_SUCCEED;
}


/*
 * check the $key_path_value in SQL :
 *      CREATE CLIENT MASTER KEY xxx (KEY_STORE = localkms, KEY_PATH = $key_path_value, ...)
 * 1. is the length of $key_path_value in range (MIN_KEY_PATH_LEN, MAX_KEY_PATH_LEN) ?
 * 2. does $key_path_value contain unsupport char ?
 * 3. is environment variable $LOCALKMS_FILE_PATH/$GAUSSHOME set ?
 * 4. are the cmk and rand keys named according $key_path_value exist ?
 */
static CmkemErrCode check_cmk_id_validity(const char *keyid)
{
    char cmk_file_path[PATH_MAX] = {0};
    CmkemErrCode ret = CMKEM_SUCCEED;
    LocalkmsFileType all_file[] = {PUB_KEY_FILE, PRIV_KEY_FILE, PUB_KEY_ENC_IV_FILE, PRIV_KEY_ENC_IV_FILE};

    if (strlen(keyid) < MIN_KEY_PATH_LEN || strlen(keyid) > MAX_KEY_PATH_LEN) {
        return CMKEM_CHECK_CMK_ID_ERR;
    }

    ret = check_key_path_value(keyid);
    if (ret != CMKEM_SUCCEED) {
        return ret;
    }

    ret = set_global_env_value();
    if (ret != CMKEM_SUCCEED) {
        return ret;
    }

    for (size_t i = 0; i < sizeof(all_file) / sizeof(all_file[0]); i++) {
        get_file_path_from_cmk_id(keyid, all_file[i], cmk_file_path, PATH_MAX);
        ret = check_file_exist(cmk_file_path, CREATE_KEY_FILE);
        if (ret != CMKEM_SUCCEED) {
            return ret;
        }
    }

    return CMKEM_SUCCEED;
}

/* 
 * for now, localkms only supports asymmetric cryptographic algorithm :
 *      1. RSA_2048 (not safe now, please use 3072)
 *      2. RSA_3072
 *      3. SM3 (for legal reasons, it is only supported in China)
 */
static CmkemErrCode check_cmk_algo_validity(const char *cmk_algo)
{
    char error_msg_buf[MAX_CMKEM_ERRMSG_BUF_SIZE] = {0};
    error_t rc = 0;

    for (size_t i = 0; g_support_algo[i] != NULL; i++) {
        if (strcasecmp(cmk_algo, g_support_algo[i]) == 0) {
            return CMKEM_SUCCEED;
        }
    }

    rc = sprintf_s(error_msg_buf, MAX_CMKEM_ERRMSG_BUF_SIZE, "unspported algorithm '%s', localkms only support: ",
        cmk_algo);
    securec_check_ss_c(rc, "", "");

    for (size_t i = 0; g_support_algo[i] != NULL; i++) {
        rc = strcat_s(error_msg_buf, MAX_CMKEM_ERRMSG_BUF_SIZE, g_support_algo[i]);
        securec_check_c(rc, "\0", "\0");
        rc = strcat_s(error_msg_buf, MAX_CMKEM_ERRMSG_BUF_SIZE, "  ");
        securec_check_c(rc, "\0", "\0");
    }

    cmkem_errmsg("%s", error_msg_buf);
    return CMKEM_CHECK_ALGO_ERR;
}

/* 
 * rsa keys generated by openssl::RSA_generate_key_ex() are RSA type,
 * so, we will temporarily write to the bio buffer at first,
 * then, read out and stored as strings from the bio buffer
 */
static CmkemErrCode write_rsa_keypair_to_file(RSA *rsa_key_pair, const char *cmk_path)
{
    BIO *tmp_pub_key = NULL;
    BIO *tmp_priv_key = NULL;
    CmkemUStr *rsa_pub_key = NULL;
    CmkemUStr *rsa_priv_key = NULL;
    char cmk_pub_file_path[PATH_MAX] = {0};
    char cmk_priv_file_path[PATH_MAX] = {0};
    CmkemErrCode ret = CMKEM_SUCCEED;
    
    ret = write_rsa_keypair_to_biobuf(rsa_key_pair, &tmp_pub_key, &tmp_priv_key);
    if (ret != CMKEM_SUCCEED) {
        return ret;
    }

    ret = read_rsa_key_from_biobuf(tmp_pub_key, &rsa_pub_key);
    BIO_free(tmp_pub_key);
    if (ret != CMKEM_SUCCEED) {
        BIO_free(tmp_priv_key);
        return ret;
    }

    ret = read_rsa_key_from_biobuf(tmp_priv_key, &rsa_priv_key);
    BIO_free(tmp_priv_key);
    if (ret != CMKEM_SUCCEED) {
        free_cmkem_ustr_with_erase(rsa_pub_key);
        return ret;
    }

    get_file_path_from_cmk_id(cmk_path, PUB_KEY_FILE, cmk_pub_file_path, PATH_MAX);
    get_file_path_from_cmk_id(cmk_path, PRIV_KEY_FILE, cmk_priv_file_path, PATH_MAX);

    ret = encrypt_and_write_key(cmk_pub_file_path, rsa_pub_key);
    free_cmkem_ustr_with_erase(rsa_pub_key);
    if (ret != CMKEM_SUCCEED) {
        free_cmkem_ustr_with_erase(rsa_priv_key);
        return ret;
    }

    ret = encrypt_and_write_key(cmk_priv_file_path, rsa_priv_key);
    free_cmkem_ustr_with_erase(rsa_priv_key);
    
    return ret;
}

static CmkemErrCode create_and_write_rsa_key_pair(const char *cmk_id, size_t rsa_key_len)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    RSA *rsa_key_pair = NULL;

    rsa_key_pair = create_rsa_keypair(rsa_key_len);
    if (rsa_key_pair == NULL) {
        return CMKEM_GEN_RSA_KEY_ERR;
    }

    ret = write_rsa_keypair_to_file(rsa_key_pair, cmk_id);
    RSA_free(rsa_key_pair);
    check_cmkem_ret(ret);

    return CMKEM_SUCCEED;
}

static CmkemErrCode write_sm2_keypair_to_file(Sm2KeyPair *sm2_key_pair, const char *cmk_path)
{
    char cmk_pub_file_path[PATH_MAX] = {0};
    char cmk_priv_file_path[PATH_MAX] = {0};
    CmkemErrCode ret = CMKEM_SUCCEED;

    get_file_path_from_cmk_id(cmk_path, PUB_KEY_FILE, cmk_pub_file_path, PATH_MAX);
    get_file_path_from_cmk_id(cmk_path, PRIV_KEY_FILE, cmk_priv_file_path, PATH_MAX);

    ret = encrypt_and_write_key(cmk_pub_file_path, sm2_key_pair->pub_key);
    check_cmkem_ret(ret);

    ret = encrypt_and_write_key(cmk_priv_file_path, sm2_key_pair->priv_key);
    check_cmkem_ret(ret);

    return CMKEM_SUCCEED;
}

static CmkemErrCode create_and_write_sm2_key_pair(const char *cmk_id)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    Sm2KeyPair *sm2_key_pair = NULL;

    sm2_key_pair = generate_encrypt_pair_key();
    if (sm2_key_pair == NULL) {
        return CMKEM_GEN_SM2_KEY_ERR;
    }

    ret = write_sm2_keypair_to_file(sm2_key_pair, cmk_id);
    free_cmkem_ustr_with_erase(sm2_key_pair->priv_key);
    free_cmkem_ustr_with_erase(sm2_key_pair->pub_key);
    km_free(sm2_key_pair);
    check_cmkem_ret(ret);

    return CMKEM_SUCCEED;
}

CmkemErrCode read_and_decrypt_bio_cmk(const char *key_path, AsymmetricKeyType key_type, BIO **key_plain)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    int bio_write_len = 0;
    CmkemUStr *tmp_key_plain = NULL;
    BIO *ret_key_plain = NULL;

    ret = read_and_decrypt_cmk(key_path, key_type, &tmp_key_plain);
    check_cmkem_ret(ret);

    ret_key_plain = BIO_new(BIO_s_mem());
    if (ret_key_plain == NULL) {
        free_cmkem_ustr_with_erase(tmp_key_plain);
        return CMKEM_MALLOC_MEM_ERR;
    }

    bio_write_len = BIO_write(ret_key_plain, tmp_key_plain->ustr_val, tmp_key_plain->ustr_len);
    free_cmkem_ustr_with_erase(tmp_key_plain);
    if (bio_write_len < 0) {
        return CMKEM_WRITE_TO_BIO_ERR;
    }

    *key_plain = ret_key_plain;
    return CMKEM_SUCCEED;
}

static CmkemErrCode encrypt_cek_with_rsa(CmkemUStr *cek_plain, const char *cmk_id_str, CmkemUStr **cek_cipher)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    BIO *cmk_plain = NULL;
    RSA *rsa_cmk_plain = NULL;
    CmkemUStr *ret_cek_cipher = NULL;
    int enc_ret = 0;

    ret = read_and_decrypt_bio_cmk(cmk_id_str, PUBLIC_KEY, &cmk_plain);
    check_cmkem_ret(ret);

    rsa_cmk_plain = PEM_read_bio_RSA_PUBKEY(cmk_plain, NULL, NULL, NULL);
    BIO_free(cmk_plain);
    if (rsa_cmk_plain == NULL) {
        return CMKEM_READ_FROM_BIO_ERR;
    }

    ret_cek_cipher = malloc_cmkem_ustr(MAX_ASYMM_KEY_BUF_LEN);
    if (ret_cek_cipher == NULL) {
        RSA_free(rsa_cmk_plain);
        return CMKEM_MALLOC_MEM_ERR;
    }

    enc_ret = RSA_public_encrypt(cek_plain->ustr_len, cek_plain->ustr_val, ret_cek_cipher->ustr_val, rsa_cmk_plain,
        RSA_PKCS1_OAEP_PADDING);
    RSA_free(rsa_cmk_plain);
    if (enc_ret == -1) {
        free_cmkem_ustr(ret_cek_cipher);
        return CMKEM_RSA_ENCRYPT_ERR;
    }

    ret_cek_cipher->ustr_len = (size_t)enc_ret;
    *cek_cipher = ret_cek_cipher;

    return CMKEM_SUCCEED;
}

static CmkemErrCode decrypt_cek_with_rsa(CmkemUStr *cek_cipher, const char *cmk_id_str, CmkemUStr **cek_plain)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    BIO *cmk_plain = NULL;
    RSA *rsa_cmk_plain = NULL;
    CmkemUStr *ret_cek_plain = NULL;
    int enc_ret = 0;

    ret = read_and_decrypt_bio_cmk(cmk_id_str, PRIVATE_KEY, &cmk_plain);
    check_cmkem_ret(ret);

    rsa_cmk_plain = PEM_read_bio_RSAPrivateKey(cmk_plain, NULL, NULL, NULL);
    BIO_free(cmk_plain);
    if (rsa_cmk_plain == NULL) {
        return CMKEM_READ_FROM_BIO_ERR;
    }

    ret_cek_plain = malloc_cmkem_ustr(MAX_ASYMM_KEY_BUF_LEN);
    if (ret_cek_plain == NULL) {
        RSA_free(rsa_cmk_plain);
        return CMKEM_MALLOC_MEM_ERR;
    }

    enc_ret = RSA_private_decrypt(cek_cipher->ustr_len, cek_cipher->ustr_val, ret_cek_plain->ustr_val, rsa_cmk_plain,
        RSA_PKCS1_OAEP_PADDING);
    RSA_free(rsa_cmk_plain);
    if (enc_ret == -1) {
        free_cmkem_ustr_with_erase(ret_cek_plain);
        return CMKEM_RSA_DECRYPT_ERR;
    }

    ret_cek_plain->ustr_len = (size_t) enc_ret;
    *cek_plain = ret_cek_plain;

    return CMKEM_SUCCEED;
}

static CmkemErrCode encrypt_cek_with_sm2(CmkemUStr *cek_plain, const char *cmk_id_str, CmkemUStr **cek_cipher)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    CmkemUStr *cmk_plain = NULL;

    ret = read_and_decrypt_cmk(cmk_id_str, PUBLIC_KEY, &cmk_plain);
    check_cmkem_ret(ret);

    ret = encrypt_with_sm2_pubkey(cek_plain, cmk_plain, cek_cipher);
    free_cmkem_ustr_with_erase(cmk_plain);
    check_cmkem_ret(ret);

    return CMKEM_SUCCEED;
}

static CmkemErrCode decrypt_cek_with_sm2(CmkemUStr *cek_cipher, const char *cmk_id_str, CmkemUStr **cek_plain)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    CmkemUStr *cmk_plain = NULL;

    ret = read_and_decrypt_cmk(cmk_id_str, PRIVATE_KEY, &cmk_plain);
    check_cmkem_ret(ret);

    ret = decrypt_with_sm2_privkey(cek_cipher, cmk_plain, cek_plain);
    free_cmkem_ustr_with_erase(cmk_plain);
    check_cmkem_ret(ret);

    return CMKEM_SUCCEED;
}

KeyMgr *kms_new(KmErr *err)
{
    return (KeyMgr *)localkms_new(err);
}

void kms_free(KeyMgr *kmgr)
{
    localkms_free((LocalKmsMgr *)kmgr);
}

void kms_mk_create(KeyMgr *kmgr, KeyInfo info)
{
    LocalKmsMgr *kms = (LocalKmsMgr *)(void *)kmgr;
    CmkemErrCode ret = CMKEM_UNKNOWN_ERR;

    switch (get_algo_by_str(info.algo)) {
        case AT_RSA_2048:
            ret = create_and_write_rsa_key_pair(info.id, RSA2048_KEN_LEN);
            return;
        case AT_RSA_3072:
            ret = create_and_write_rsa_key_pair(info.id, RSA3072_KEN_LEN);
            break;
        case AT_SM2:
            ret = create_and_write_sm2_key_pair(info.id);
            break;
        default:
            break;
    }

    if (ret != CMKEM_SUCCEED) {
        km_err_msg(kms->kmgr.err, "%s", get_cmkem_errmsg(ret));
    }
}

static void rm_file(const char *file)
{
    if (remove(file) != 0) {
        (void)printf("failed to remove file '%s'.\n", file);
    }
}

void kms_mk_delete(KeyMgr *kmgr, KeyInfo info)
{
    char pub_cmk_file[PATH_MAX] = {0};
    char priv_cmk_file[PATH_MAX] = {0};
    errno_t rc = 0;

    get_file_path_from_cmk_id(info.id, PUB_KEY_FILE, pub_cmk_file, PATH_MAX);
    get_file_path_from_cmk_id(info.id, PRIV_KEY_FILE, priv_cmk_file, PATH_MAX);

    rm_file(pub_cmk_file);
    rm_file(priv_cmk_file);

    rc = strcat_s(pub_cmk_file, PATH_MAX, ".rand");
    securec_check_c(rc, "", "");
    rc = strcat_s(priv_cmk_file, PATH_MAX, ".rand");
    securec_check_c(rc, "", "");

    rm_file(pub_cmk_file);
    rm_file(priv_cmk_file);
}

char *kms_mk_select(KeyMgr *kmgr, KeyInfo info)
{
    LocalKmsMgr *kms = (LocalKmsMgr *)(void *)kmgr;
    CmkemErrCode ret = CMKEM_UNKNOWN_ERR;

    if (info.id == NULL) {
        km_err_msg(kms->kmgr.err, "failed to create client master key, failed to find arg: KEY_PATH.");
        return NULL;
    }

    if (info.algo == NULL) {
        km_err_msg(kms->kmgr.err, "failed to create client master key, failed to find arg: ALGORITHM.");
        return NULL;
    }

    ret = check_cmk_id_validity(info.id);
    if (ret != CMKEM_SUCCEED) {
        km_err_msg(kms->kmgr.err, "%s", get_cmkem_errmsg(ret));
        return NULL;
    }

    ret = check_cmk_algo_validity(info.algo);
    if (ret != CMKEM_SUCCEED) {
        km_err_msg(kms->kmgr.err, "%s", get_cmkem_errmsg(ret));
        return NULL;
    }

    return strdup("active");
}

KmUnStr kms_mk_encrypt(KeyMgr *kmgr, KeyInfo info, KmUnStr plain)
{
    LocalKmsMgr *kms = (LocalKmsMgr *)(void *)kmgr;
    CmkemErrCode ret = CMKEM_UNKNOWN_ERR;
    KmUnStr cipher = {0};

    CmkemUStr _plain = {plain.val, plain.len};
    CmkemUStr *_cipher = NULL;

    switch (get_algo_by_str(info.algo)) {
        case AT_RSA_2048:
            ret = encrypt_cek_with_rsa(&_plain, info.id, &_cipher);
            break;
        case AT_RSA_3072:
            ret = encrypt_cek_with_rsa(&_plain, info.id, &_cipher);
            break;
        case AT_SM2:
            ret = encrypt_cek_with_sm2(&_plain, info.id, &_cipher);
            break;
        default:
            break;
    }

    if (ret != CMKEM_SUCCEED) {
        km_err_msg(kms->kmgr.err, "%s", get_cmkem_errmsg(ret));
        return cipher;
    }

    cipher.val = _cipher->ustr_val;
    cipher.len = _cipher->ustr_len;
    km_safe_free(_cipher);
    return cipher;
}

KmUnStr kms_mk_decrypt(KeyMgr *kmgr, KeyInfo info, KmUnStr cipher)
{
    LocalKmsMgr *kms = (LocalKmsMgr *)(void *)kmgr;
    CmkemErrCode ret = CMKEM_UNKNOWN_ERR;
    KmUnStr plain = {0};

    CmkemUStr _cipher = {cipher.val, cipher.len};
    CmkemUStr *_plain = NULL;

    switch (get_algo_by_str(info.algo)) {
        case AT_RSA_2048:
        case AT_RSA_3072:
            ret = decrypt_cek_with_rsa(&_cipher, info.id, &_plain);
            break;
        case AT_SM2:
            ret = decrypt_cek_with_sm2(&_cipher, info.id, &_plain);
            break;
        default:
            break;
    }

    if (ret != CMKEM_SUCCEED) {
        km_err_msg(kms->kmgr.err, "%s", get_cmkem_errmsg(ret));
        return plain;
    }

    plain.val = _plain->ustr_val;
    plain.len = _plain->ustr_len;
    return plain;
}

KeyMethod localkms = {
    "localkms",

    kms_new, /* kmgr_new */
    kms_free, /* kmgr_free */
    NULL, /* kmgr_set_arg */

    kms_mk_create, /* mk_create */
    kms_mk_delete, /* mk_delete */
    kms_mk_select, /* mk_select */
    kms_mk_encrypt, /* mk_encrypt */
    kms_mk_decrypt, /* mk_decrypt */

    NULL, /* dk_create */
};