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
 * register_local_kms.cpp
 *      localkms is a lightweight key management component.
 *      different from other key management tools and services, localkms automatically generates CMKE when CREATE CMKO,
 *      it cannot manage key entities independently.
 *      localkms use openssl to generate cmk, then encrypt cmk plain and store cmk cipher in file.
 *      at the same time, we need to store/read the iv and salt that are used to derive a key to
 *      encrypt/decrypt cmk plain.
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/register_local_kms.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cmkem_version_control.h"
#ifdef ENABLE_LOCAL_KMS

#include "register_local_kms.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pthread.h>
#include <limits.h>
#include <openssl/rand.h>
#include <openssl/rsa.h>
#include <openssl/pem.h>
#include "securec.h"
#include "securec_check.h"
#include "cmkem_comm.h"
#include "aead_aes_hamc_enc_key.h"
#include "encrypt_decrypt.h"
#include "sm2_enc_key.h"
#include "cmkem_comm_algorithm.h"
#include "reg_hook_frame.h"

const int MAX_KEY_PATH_LEN = 64;
const int MIN_KEY_PATH_LEN = 1;
const int KEY_METERIAL_LEN = 32;
const int DRIVED_KEY_LEN = 64;
const int ITERATE_ROUD = 10000;
const int KEY_FILE_HEADER_LEN = 20;

typedef enum {
    CREATE_KEY_FILE,
    READ_KEY_FILE,
    REMOVE_KEY_FILE
} CheckKeyFileType;

typedef enum {
    PUB_KEY_FILE,
    PRIV_KEY_FILE,
    PUB_KEY_ENC_IV_FILE,
    PRIV_KEY_ENC_IV_FILE,
} LocalkmsFileType;

static char g_env_value[PATH_MAX] = {0};
static pthread_mutex_t g_env_lock;
static const char *supported_algorithms[] = {"RSA_2048", "RSA_3072", "SM2", NULL};

/* check the values obtained from the environment or input by the user */
static CmkemErrCode check_env_value(const char *path_value);
static CmkemErrCode check_key_path_value(const char *key_path_value);
static CmkemErrCode check_file_exist(const char *real_file_path, CheckKeyFileType chk_type);
static CmkemErrCode check_cmk_id_validity(CmkIdentity *cmk_identity);
static CmkemErrCode check_cmk_algo_validity(const char *cmk_algo);

/* set the environment variables needed for localkms to work */
static char *get_env_value(const char *name);
static CmkemErrCode set_global_env_value();

/* 
 * encrypt cmk plain and write cmk plain cipher to file 
 * also, read cmk plain cipher from file, and decrypt it to get cmk plain
 */
static void get_file_path_from_cmk_id(const char *cmk_id_str, LocalkmsFileType file_type, char *file_path_buf,
    size_t buf_len);
static CmkemErrCode create_file_and_write(const char *real_path, const unsigned char *content, size_t content_len,
    bool is_write_header);
static void rm_file(const char *file_path);
static void rm_file_with_retry(const char *file_path);
static CmkemErrCode encrypt_and_write_key(const char *key_file_path, CmkemUStr *key_plain);
static CmkemErrCode write_rsa_keypair_to_file(RSA *rsa_key_pair, const char *cmk_path);
static CmkemErrCode read_content_from_file(const char *real_path, unsigned char *buf, const size_t buf_len,
    size_t *content_len);
static CmkemErrCode read_cmk_plain(const char *real_cmk_path, unsigned char *cmk_plain, size_t *plain_len);
static CmkemErrCode read_and_decrypt_bio_cmk(const char *key_path, AsymmetricKeyType key_type, BIO **key_plain);
static CmkemErrCode read_rand_and_drive_key(const char *rand_path, CmkemUStr **drived_key);
static CmkemErrCode read_iv_and_salt(const char *rand_path, unsigned char *iv, size_t iv_len, unsigned char *salt,
    size_t salt_len);

static CmkemErrCode create_and_write_rsa_key_pair(const char *cmk_id, size_t rsa_key_len);
static CmkemErrCode create_and_write_sm2_key_pair(const char *cmk_id);
static CmkemErrCode write_sm2_keypair_to_file(Sm2KeyPair *sm2_key_pair, const char *cmk_path);
static CmkemErrCode encrypt_cek_with_rsa(CmkemUStr *cek_plain, const char *cmk_id_str, CmkemUStr **cek_cipher);
static CmkemErrCode decrypt_cek_with_rsa(CmkemUStr *cek_cipher, const char *cmk_id_str, CmkemUStr **cek_plain);
static CmkemErrCode encrypt_cek_with_sm2(CmkemUStr *cek_plain, const char *cmk_id_str, CmkemUStr **cek_cipher);
static CmkemErrCode decrypt_cek_with_sm2(CmkemUStr *cek_cipher, const char *cmk_id_str, CmkemUStr **cek_plain);

/* hook functions */
static ProcessPolicy create_cmk_obj_hookfunc(CmkIdentity *cmk_identity);
static ProcessPolicy post_create_cmk_obj_hookfunc(CmkIdentity *cmk_identity);
static ProcessPolicy encrypt_cek_plain_hookfunc(CmkemUStr *cek_plain, CmkIdentity *cmk_identity,
    CmkemUStr **cek_cipher);
static ProcessPolicy decrypt_cek_cipher_hookfunc(CmkemUStr *cek_cipher, CmkIdentity *cmk_identity,
    CmkemUStr **cek_plain);

/* check the environment values input from the system */
static CmkemErrCode check_env_value(const char *path_value)
{
    const char danger_char_list[] = {'|', ';', '&', '$', '<', '>', '`', '\\', '\'', '\"', '{', '}', '(', ')', '[',']',
        '~', '*', '?', '!'};

    for (size_t i = 0; i < strlen(path_value); i++) {
        for (size_t j = 0; j < sizeof(danger_char_list); j++) {
            if (path_value[i] == danger_char_list[j]) {
                cmkem_errmsg("the path '%s' contains invalid character '%c'.", path_value, path_value[i]);
                return CMKEM_CHECK_ENV_VAL_ERR;
            }
        }
    }

    return CMKEM_SUCCEED;
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

/* before creating or reading cmk and rand file, we will check if the files already exist */
static CmkemErrCode check_file_exist(const char *real_file_path, CheckKeyFileType chk_type)
{
    bool is_file_exist = false;
    struct stat statbuf;

    is_file_exist = (lstat(real_file_path, &statbuf) < 0) ? false : true;
    if (chk_type == CREATE_KEY_FILE && is_file_exist) {
        cmkem_errmsg("cannot create file, the file '%s' already exists.\n", real_file_path);
        return CMKEM_CREATE_FILE_ERR;
    } else if (chk_type == READ_KEY_FILE && !is_file_exist) {
        cmkem_errmsg("cannot read file, failed to find file '%s'.\n", real_file_path);
        return CMKEM_FIND_FILE_ERR;
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
static CmkemErrCode check_cmk_id_validity(CmkIdentity *cmk_identity)
{
    char cmk_file_path[PATH_MAX] = {0};
    CmkemErrCode ret = CMKEM_SUCCEED;
    LocalkmsFileType all_file[] = {PUB_KEY_FILE, PRIV_KEY_FILE, PUB_KEY_ENC_IV_FILE, PRIV_KEY_ENC_IV_FILE};

    if (strlen(cmk_identity->cmk_id_str) < MIN_KEY_PATH_LEN || strlen(cmk_identity->cmk_id_str) > MAX_KEY_PATH_LEN) {
        return CMKEM_CHECK_CMK_ID_ERR;
    }

    ret = check_key_path_value(cmk_identity->cmk_id_str);
    if (ret != CMKEM_SUCCEED) {
        return ret;
    }

    if (strlen(g_env_value) == 0) {
        ret = set_global_env_value();
        if (ret != CMKEM_SUCCEED) {
            return ret;
        }
    }

    for (size_t i = 0; i < sizeof(all_file) / sizeof(all_file[0]); i++) {
        get_file_path_from_cmk_id(cmk_identity->cmk_id_str, all_file[i], cmk_file_path, PATH_MAX);
        ret = check_file_exist(cmk_file_path, CREATE_KEY_FILE);
        if (ret != CMKEM_SUCCEED) {
            return ret;
        }
    }

    return CMKEM_SUCCEED;
}

/* 
 * for now, localkms only supports asymmetric cryptographic algorithm : 
 *      1. RSA_2048
 *      2. RSA_3072
 *      3. SM3 (for legal reasons, it is only supported in China)
 */
static CmkemErrCode check_cmk_algo_validity(const char *cmk_algo)
{
    char error_msg_buf[MAX_CMKEM_ERRMSG_BUF_SIZE] = {0};
    error_t rc = 0;
    
    for (size_t i = 0; supported_algorithms[i] != NULL; i++) {
        if (strcasecmp(cmk_algo, supported_algorithms[i]) == 0) {
            return CMKEM_SUCCEED;
        }
    }

    rc = sprintf_s(error_msg_buf, MAX_CMKEM_ERRMSG_BUF_SIZE, "unspported algorithm '%s', localkms only support: ",
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

static char *get_env_value(const char *name)
{
    char *ret = NULL;
    (void)pthread_mutex_lock(&g_env_lock);
    ret = getenv(name);
    (void)pthread_mutex_unlock(&g_env_lock);
    return ret;
}

/* 
 * keys generated by localkms are stored under the path ：$LOCALKMS_FILE_PATH/
 * in addition, if $LOCALKMS_FILE_PATH cannot be found, we will try to find $GAUSSHOME
 */
static CmkemErrCode set_global_env_value()
{
    char *local_kms_path = NULL;
    char tmp_gausshome_buf[PATH_MAX] = {0};
    errno_t rc = 0;
    bool is_get_localkms_env = false;
    char tmp_env_val[PATH_MAX] = {0};
    CmkemErrCode ret = CMKEM_SUCCEED;

    local_kms_path = get_env_value("LOCALKMS_FILE_PATH");
    if (local_kms_path == NULL || realpath(local_kms_path, tmp_env_val) == NULL) {
        /* fail to get LOCALKMS_FILE_PATH, then try to get GAUSSHOME */
        local_kms_path = get_env_value("GAUSSHOME");
        if (local_kms_path != NULL) {
            rc = sprintf_s(tmp_gausshome_buf, sizeof(tmp_gausshome_buf), "%s/%s", local_kms_path, "/etc/localkms");
            securec_check_ss_c(rc, "", "");

            /* judge whether the $GAUSSHOME is obtained or not */
            if (realpath(tmp_gausshome_buf, tmp_env_val) != NULL) {
                is_get_localkms_env = true;
            }
        }
    } else {
        is_get_localkms_env = true;
    }

    if (!is_get_localkms_env) {
        cmkem_errmsg("failed to get the environment value : '%s' or the path : '%s'.", "$LOCALKMS_FILE_PATH",
            "$GAUSSHOME/etc/localkms/");
        return CMKEM_GET_ENV_VAL_ERR;
    }

    ret = check_env_value(tmp_env_val);
    check_cmkem_ret(ret);

    rc = strcpy_s(g_env_value, PATH_MAX, tmp_env_val);
    securec_check_c(rc, "", "");
    
    return CMKEM_SUCCEED;
}

/*
 * in SQL :
 *      CREATE CLIENT MASTER KEY xxx (KEY_STORE = localkms, KEY_PATH = $key_path_value, ...)
 * for each $key_path_value, we will create 4 file:
 *      1. $LOCALKMS_FILE_PATH/$key_path_value.pub ： store public key cipher of asymmetric key
 *      2. $LOCALKMS_FILE_PATH/$key_path_value.pub.rand ： store rands that are used to derive a key to encrypt 
 *          public key plain
 *      3. $LOCALKMS_FILE_PATH/$key_path_value.priv ： store private key cipher of asymmetric key
 *      2. $LOCALKMS_FILE_PATH/$key_path_value.priv.rand ： store rands that are used to derive a key to encrypt 
 *          private key plain
 */
static void get_file_path_from_cmk_id(const char *cmk_id_str, LocalkmsFileType file_type, char *file_path_buf,
    size_t buf_len)
{
    error_t rc = 0;
    const char *file_extension = NULL;
    
    switch (file_type) {
        case PUB_KEY_FILE:
            file_extension = ".pub";
            break;
        case PRIV_KEY_FILE:
            file_extension = ".priv";
            break;
        case PUB_KEY_ENC_IV_FILE:
            file_extension = ".pub.rand";
            break;
        case PRIV_KEY_ENC_IV_FILE:
            file_extension = ".priv.rand";
            break;
        default:
            break;
    }

    rc = sprintf_s(file_path_buf, buf_len, "%s/%s%s", g_env_value, cmk_id_str, file_extension);
    securec_check_ss_c(rc, "", "");
}

/* 
 * if para:is_write_header == true, we will create a file header, and write the size of conent to header
 * after that, if trying to read all content from file, we will read header to calcule the size of content 
 * buffer before reading all content.
 * 
 * although it's redundant to add a header, in order to keep the compatibility with the old version, we want
 * to keep using this way
 */
static CmkemErrCode create_file_and_write(const char *real_path, const unsigned char *content, size_t content_len,
    bool is_write_header)
{
    int fd = 0;
    char head[KEY_FILE_HEADER_LEN] = {0};
    errno_t rc = 0;

    fd = open(real_path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        cmkem_errmsg("failed to create file '%s'.\n", real_path);
        return CMKEM_CREATE_FILE_ERR;
    }

    if (is_write_header) {
        rc = sprintf_s(head, sizeof(head), "%lu", content_len);
        securec_check_ss_c(rc, "", "");
        write(fd, head, sizeof(head));
    }
    
    write(fd, content, content_len);
    close(fd);

    return CMKEM_SUCCEED;
}


static void rm_file(const char *file_path)
{   
    if (remove(file_path) != 0) {
#ifndef ENABLE_UT
        printf("failed to remove file '%s'.", file_path);
#endif
    }
}

static void rm_file_with_retry(const char *file_path)
{
    char tmp_path[PATH_MAX] = {0};
    
    rm_file(file_path);

    if (realpath(file_path, tmp_path) != NULL) {
        rm_file(file_path);
    }
}

static CmkemErrCode encrypt_and_write_key(const char *key_file_path, CmkemUStr *key_plain)
{
    unsigned char iv[KEY_METERIAL_LEN] = {0};
    unsigned char salt[KEY_METERIAL_LEN] = {0};
    unsigned char iv_salt_buf[sizeof(iv) + sizeof(salt)] = {0};
    unsigned char derived_key[DRIVED_KEY_LEN] = {0};
    unsigned char tmp_cipher[RSA2048_KEN_LEN] = {0};
    int tmp_cipher_len = 0;
    char rand_file_path[PATH_MAX] = {0};
    errno_t rc = 0;
    CmkemErrCode ret = CMKEM_SUCCEED;

    if (RAND_priv_bytes(iv, sizeof(iv)) != 1 || RAND_priv_bytes(salt, sizeof(salt)) != 1) {
        return CMKEM_DERIVED_KEY_ERR;
    }

    if (PKCS5_PBKDF2_HMAC((const char *)iv, sizeof(iv), salt, sizeof(salt), ITERATE_ROUD, EVP_sha256(),
        sizeof(derived_key), derived_key) != 1) {
        return CMKEM_DERIVED_KEY_ERR;
    }

    AeadAesHamcEncKey derived_aead_key = AeadAesHamcEncKey(derived_key, DRIVED_KEY_LEN);
    tmp_cipher_len = encrypt_data(key_plain->ustr_val, key_plain->ustr_len, derived_aead_key,
        EncryptionType::DETERMINISTIC_TYPE, tmp_cipher, AEAD_AES_256_CBC_HMAC_SHA256);
    if (tmp_cipher_len <= 0) {
        return CMKEM_ENC_CMK_ERR;
    }

    ret = create_file_and_write(key_file_path, tmp_cipher, (size_t)tmp_cipher_len, true);
    if (ret != CMKEM_SUCCEED) {
        return ret;
    }

    rc = sprintf_s(rand_file_path, PATH_MAX, "%s.rand", key_file_path);
    securec_check_ss_c(rc, "", "");

    for (size_t i = 0; i < sizeof(iv); i++) {
        iv_salt_buf[i] = iv[i];
    }
    for (size_t i = 0; i < sizeof(salt); i++) {
        iv_salt_buf[sizeof(iv) + i] = salt[i];
    }

    return create_file_and_write(rand_file_path, iv_salt_buf, sizeof(iv_salt_buf), true);
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

/* the input file must contain a header which store the size value of file content */
static CmkemErrCode read_content_from_file(const char *real_path, unsigned char *buf, const size_t buf_len,
    size_t *content_len)
{
    int fd = 0;
    char header[KEY_FILE_HEADER_LEN] = {0};

    fd = open(real_path, O_RDONLY, 0);
    if (fd < 0) {
        cmkem_errmsg("failed to open file '%s'.\n", real_path);
        return CMKEM_OPEN_FILE_ERR;
    }

    if (read(fd, header, sizeof(header)) < 0) {
        cmkem_errmsg("failed to read file '%s'.\n", real_path);
        close(fd);
        return CMKEM_READ_FILE_ERR;
    }
    *content_len = atoi(header);

    if (*content_len > buf_len) {
        cmkem_errmsg("the header of file '%s' is invalid.\n", real_path);
        close(fd);
        return CMKEM_READ_FILE_ERR;
    }

    if (read(fd, buf, *content_len) < 0) {
        cmkem_errmsg("failed to read from file '%s'.\n", real_path);
        close(fd);
        return CMKEM_READ_FILE_ERR;
    }

    close(fd);
    return CMKEM_SUCCEED;
}

/* 
 * cmk is stored as cihper, so, rather then read it directly, we will :
 *      1. read rand file firstly (rand_file_path = cmk_cipher_file_path.rand)
 *      2. derive a key to decrypt cmk cipher
 *      3. now, we finally get cmk plain
 */
static CmkemErrCode read_cmk_plain(const char *real_cmk_path, unsigned char *cmk_plain, size_t *plain_len)
{
    char rand_file_path[PATH_MAX] = {0};
    CmkemUStr *derivied_key = NULL;
    unsigned char cmk_cipher[RSA2048_KEN_LEN] = {0};
    size_t cmk_cipher_len = 0;
    int tmp_plainlen = 0;
    CmkemErrCode ret = CMKEM_SUCCEED;
    errno_t rc = 0;

    rc = sprintf_s(rand_file_path, PATH_MAX, "%s.rand", real_cmk_path);
    securec_check_ss_c(rc, "", "");

    ret = read_rand_and_drive_key(rand_file_path, &derivied_key);
    if (ret != CMKEM_SUCCEED) {
        free_cmkem_ustr(derivied_key);
        return ret;
    }

    ret = read_content_from_file(real_cmk_path, cmk_cipher, sizeof(cmk_cipher), &cmk_cipher_len);
    if (ret != CMKEM_SUCCEED) {
        free_cmkem_ustr(derivied_key);
        return ret;
    }

    AeadAesHamcEncKey derived_aead_key = AeadAesHamcEncKey(derivied_key->ustr_val, DRIVED_KEY_LEN);
    free_cmkem_ustr(derivied_key);
    tmp_plainlen = decrypt_data(cmk_cipher, cmk_cipher_len, derived_aead_key, cmk_plain, AEAD_AES_256_CBC_HMAC_SHA256);
    if (tmp_plainlen <= 0) {
        return CMKEM_DEC_CMK_ERR;
    }
    *plain_len = (size_t)tmp_plainlen;

    return CMKEM_SUCCEED;
}

static CmkemErrCode read_and_decrypt_cmk(const char *key_path, AsymmetricKeyType key_type, CmkemUStr **key_palin)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    char key_file_path[PATH_MAX] = {0};
    CmkemUStr *ret_key_plain = NULL;

    if (key_type == PUBLIC_KEY) {
        get_file_path_from_cmk_id(key_path, PUB_KEY_FILE, key_file_path, PATH_MAX);
    } else {
        get_file_path_from_cmk_id(key_path, PRIV_KEY_FILE, key_file_path, PATH_MAX);
    }
    
    ret_key_plain = malloc_cmkem_ustr(MAX_ASYMM_KEY_BUF_LEN);
    if (ret_key_plain == NULL) {
        return CMKEM_MALLOC_MEM_ERR;
    }

    ret = read_cmk_plain(key_file_path, ret_key_plain->ustr_val, &ret_key_plain->ustr_len);
    if (ret != CMKEM_SUCCEED) {
        return ret;
    }

    *key_palin = ret_key_plain;
    return CMKEM_SUCCEED;
}

static CmkemErrCode read_and_decrypt_bio_cmk(const char *key_path, AsymmetricKeyType key_type, BIO **key_plain)
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

/* 
 * cmk plain is encrypted byfore stored. 
 * the iv_and_salt are used to derive a key to encrypt cmk plain, as well as decrypt cmk cipher 
 */
static CmkemErrCode read_rand_and_drive_key(const char *rand_path, CmkemUStr **drived_key)
{
    unsigned char iv[KEY_METERIAL_LEN] = {0};
    unsigned char salt[KEY_METERIAL_LEN] = {0};
    CmkemUStr *reg_drived_key = NULL;
    CmkemErrCode ret = CMKEM_SUCCEED;

    ret = read_iv_and_salt(rand_path, iv, sizeof(iv), salt, sizeof(salt));
    if (ret != CMKEM_SUCCEED) {
        return ret;
    }

    reg_drived_key = malloc_cmkem_ustr(DRIVED_KEY_LEN);
    if (reg_drived_key == NULL) {
        return CMKEM_MALLOC_MEM_ERR;
    }

    if (PKCS5_PBKDF2_HMAC((const char *)iv, sizeof(iv), salt, sizeof(salt), ITERATE_ROUD, EVP_sha256(),
        DRIVED_KEY_LEN,  reg_drived_key->ustr_val) != 1) {
        free_cmkem_ustr(reg_drived_key);
        return CMKEM_DERIVED_KEY_ERR;
    }

    *drived_key = reg_drived_key;
    return CMKEM_SUCCEED;
}

static CmkemErrCode read_iv_and_salt(const char *rand_path, unsigned char *iv, size_t iv_len, unsigned char *salt,
    size_t salt_len)
{
    unsigned char iv_salt_buf[iv_len + salt_len] = {0};
    size_t iv_salt_len = 0;
    CmkemErrCode ret = CMKEM_SUCCEED;

    ret = read_content_from_file(rand_path, iv_salt_buf, sizeof(iv_salt_buf), &iv_salt_len);
    if (ret != CMKEM_SUCCEED) {
        return ret;
    } else if (iv_salt_len < sizeof(iv_salt_buf)) {
        return CMKEM_READ_FILE_ERR;
    }

    for (size_t i = 0; i < iv_len; i++) {
        iv[i] = iv_salt_buf[i];
    }
    for (size_t i = 0; i < salt_len; i++) {
        salt[i] = iv_salt_buf[iv_len + i];
    }

    return CMKEM_SUCCEED;
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

static CmkemErrCode create_and_write_sm2_key_pair(const char *cmk_id)
{
    CmkemErrCode ret = CMKEM_SUCCEED;
    Sm2KeyPair *sm2_key_pair = NULL;

    sm2_key_pair = generate_encrypt_pair_key();
    if (sm2_key_pair == NULL) {
        return CMKEM_GEN_SM2_KEY_ERR;
    }

    ret = write_sm2_keypair_to_file(sm2_key_pair, cmk_id);
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
        return CMKEM_MALLOC_MEM_ERR;
    }

    enc_ret = RSA_public_encrypt(cek_plain->ustr_len, cek_plain->ustr_val, ret_cek_cipher->ustr_val, rsa_cmk_plain,
        RSA_PKCS1_OAEP_PADDING);
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

static ProcessPolicy create_cmk_obj_hookfunc(CmkIdentity *cmk_identity)
{
    CmkemErrCode ret = CMKEM_SUCCEED;

    if (cmk_identity->cmk_store == NULL || strcasecmp(cmk_identity->cmk_store, "localkms") != 0) {
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

    ret = check_cmk_id_validity(cmk_identity);
    if (ret != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    ret = check_cmk_algo_validity(cmk_identity->cmk_algo);
    if (ret != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    return POLICY_BREAK;
}

static ProcessPolicy post_create_cmk_obj_hookfunc(CmkIdentity *cmk_identity)
{
    CmkemErrCode ret = CMKEM_SUCCEED;

    if (cmk_identity->cmk_store == NULL || strcasecmp(cmk_identity->cmk_store, "localkms") != 0) {
        return POLICY_CONTINUE;
    }

    switch (get_algo_by_str(cmk_identity->cmk_algo)) {
        case RSA_2048:
            ret = create_and_write_rsa_key_pair(cmk_identity->cmk_id_str, RSA2048_KEN_LEN);
            break;
        case RSA_3072:
            ret = create_and_write_rsa_key_pair(cmk_identity->cmk_id_str, RSA3072_KEN_LEN);
            break;
        case SM2:
            ret = create_and_write_sm2_key_pair(cmk_identity->cmk_id_str);
        default:
            break;
    }

    if (ret != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    return PLLICY_PROCESSED;
}

static ProcessPolicy encrypt_cek_plain_hookfunc(CmkemUStr *cek_plain, CmkIdentity *cmk_identity, CmkemUStr **cek_cipher)
{
    CmkemErrCode ret = CMKEM_SUCCEED;

    if (cmk_identity->cmk_store == NULL || strcasecmp(cmk_identity->cmk_store, "localkms") != 0) {
        return POLICY_CONTINUE;
    }

    switch (get_algo_by_str(cmk_identity->cmk_algo)) {
        case RSA_2048:
        case RSA_3072:
            ret = encrypt_cek_with_rsa(cek_plain, cmk_identity->cmk_id_str, cek_cipher);
            break;
        case SM2:
            ret = encrypt_cek_with_sm2(cek_plain, cmk_identity->cmk_id_str, cek_cipher);
        default:
            break;
    }

    if (ret != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    return POLICY_BREAK;
}

static ProcessPolicy decrypt_cek_cipher_hookfunc(CmkemUStr *cek_cipher, CmkIdentity *cmk_identity,
    CmkemUStr **cek_plain)
{ 
    CmkemErrCode ret = CMKEM_SUCCEED;

    if (cmk_identity->cmk_store == NULL || strcasecmp(cmk_identity->cmk_store, "localkms") != 0) {
        return POLICY_CONTINUE;
    }

    switch (get_algo_by_str(cmk_identity->cmk_algo)) {
        case RSA_2048:
        case RSA_3072:
            ret = decrypt_cek_with_rsa(cek_cipher, cmk_identity->cmk_id_str, cek_plain);
            break;
        case SM2:
            ret = decrypt_cek_with_sm2(cek_cipher, cmk_identity->cmk_id_str, cek_plain);
        default:
            break;
    }

    if (ret != CMKEM_SUCCEED) {
        return POLICY_ERROR;
    }

    return POLICY_BREAK;
}

static ProcessPolicy drop_cmk_obj_hookfunc(CmkIdentity *cmk_identity)
{
    char pub_cmk_file[PATH_MAX] = {0};
    char priv_cmk_file[PATH_MAX] = {0};
    errno_t rc = 0;
    
    if (cmk_identity->cmk_store == NULL || strcasecmp(cmk_identity->cmk_store, "localkms") != 0) {
        return POLICY_CONTINUE;
    }

    get_file_path_from_cmk_id(cmk_identity->cmk_id_str, PUB_KEY_FILE, pub_cmk_file, PATH_MAX);
    get_file_path_from_cmk_id(cmk_identity->cmk_id_str, PRIV_KEY_FILE, priv_cmk_file, PATH_MAX);

    rm_file_with_retry(pub_cmk_file);
    rm_file_with_retry(priv_cmk_file);

    rc = strcat_s(pub_cmk_file, PATH_MAX, ".rand");
    securec_check_c(rc, "", "");
    rc = strcat_s(priv_cmk_file, PATH_MAX, ".rand");
    securec_check_c(rc, "", "");

    rm_file_with_retry(pub_cmk_file);
    rm_file_with_retry(priv_cmk_file);

    return POLICY_BREAK;
}

int reg_cmke_manager_local_kms_main()
{
    if (strlen(g_env_value) == 0) {
        CmkemErrCode ret = set_global_env_value();
        if (ret != CMKEM_SUCCEED) {
            return -1;
        }
    }

    CmkEntityManager localkms = {
        create_cmk_obj_hookfunc,
        encrypt_cek_plain_hookfunc,
        decrypt_cek_cipher_hookfunc,
        drop_cmk_obj_hookfunc,
        post_create_cmk_obj_hookfunc,
    };

    return (reg_cmk_entity_manager(localkms) == CMKEM_SUCCEED) ? 0 : -1;
}

#endif /* ENABLE_LOCAL_KMS */
