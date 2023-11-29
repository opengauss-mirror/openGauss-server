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
 * security_encrypt_decrypt.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/encrypt/security_encrypt_decrypt.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#include <stdio.h>
#include <iostream>

#include "openssl/rand.h"
#include "openssl/err.h"
#include <securec.h>
#include <securec_check.h>
#include "keymgr/encrypt/security_encrypt_decrypt.h"
#include "keymgr/comm/security_error.h"

/* Key size in bytes */
const int g_key_size = 32;

const int g_auth_tag_size = 32;

const int g_gcm_auth_tag_size = 16;

/* Block size in bytes. AES uses 16 byte blocks. */
const int g_block_size = 16;

const int g_iv_size = 16; /* 128 bit */

const int g_gcm_iv_size = 12;

const int g_algo_version_size = 4;
/*
 * Minimum Length of cipher_text. This value is 4 (version byte) + 32 (authentication tag) + 16 (IV) + 16 (minimum of 1
 * block of cipher Text)
 */
static const int get_min_ciph_len(ColumnEncryptionAlgorithm algo)
{
    switch (algo) {
        case ColumnEncryptionAlgorithm::AEAD_AES_256_CBC_HMAC_SHA256:
        case ColumnEncryptionAlgorithm::AEAD_AES_128_CBC_HMAC_SHA256:
        case ColumnEncryptionAlgorithm::SM4_SM3:
            return g_algo_version_size + g_iv_size + g_auth_tag_size + g_block_size;
        case ColumnEncryptionAlgorithm::AES_256_GCM_ALGO:
            return g_algo_version_size + g_gcm_iv_size + g_gcm_auth_tag_size;
        case ColumnEncryptionAlgorithm::AES_256_CTR_ALGO:
            return g_algo_version_size + g_iv_size;
        default:
            return -1;
    }
}

static const unsigned char algo_version[4] = {'1', 0, 0, 0};

static int encrypt(EncParam *enc_param, const EVP_CIPHER *cipher, EVP_CIPHER_CTX *&ctx);
static int decrypt(EncParam *enc_param, const EVP_CIPHER *cipher, EVP_CIPHER_CTX *&ctx);
static int my_memcmp(const void *buffer1, const void *buffer2, int count);
static bool hmac_sha256_iv(AeadAesHamcEncKey &column_encryption_key, int keylen, const unsigned char *data,
    int datalen, unsigned char *result);
static bool hmac_sha256_mac(AeadAesHamcEncKey &column_encryption_key, int keylen, const unsigned char *data,
    int datalen, unsigned char *result);
static bool sm3(const unsigned char *data, int datalen, unsigned char *result);
static bool get_hash_by_cek_algo(ColumnEncryptionAlgorithm column_encryption_algorithm, int hmac_length,
    AeadAesHamcEncKey &column_encryption_key, const unsigned char *data, unsigned char *authentication_tag);

static const EVP_MD* get_evp_md_by_id(unsigned long ulAlgType)
{
    const EVP_MD* md = NULL;
    switch (ulAlgType & 0xFFFF) {
        case NID_sha256:
        case NID_hmacWithSHA256:
            md = EVP_sha256();
            break;
        case NID_undef:
            md = EVP_md_null();
            break;
        default:
            break;
    }
    return md;
}

/*
 * @Brief		: bool cached_hmac()
 * @Description	: computes hmac of a given data block. Calls init, update, and final.
 * 		  when ctx_ptr_template is passed in, this function uses that context, and algo type and keys are ignored
 *          saves time for context init
 *        when ctx_ptr is passed in, the function saves time for new/free
 * @return	: success:true, failed:false.
 */
bool cached_hmac(unsigned long algo_type, const unsigned char *key, int key_len, const unsigned char *data,
    int data_len, unsigned char *result, unsigned int *result_len, HmacCtxGroup *cached_ctx_group)
{
    static unsigned char result_container[EVP_MAX_MD_SIZE];
    static const unsigned char dummy_key[1] = {'\0'};
    if (cached_ctx_group == NULL) {
        return false;
    }
    /* new a worker ctx */
    if (cached_ctx_group->ctx_worker == NULL) {
        cached_ctx_group->ctx_worker = HMAC_CTX_new();
        if (cached_ctx_group->ctx_worker == NULL) {
            goto err;
        }
    }
    /* create a new template ctx */
    if (cached_ctx_group->ctx_template == NULL) {
        /* making new template in local_ctx_group */
        const EVP_MD* evp_md = get_evp_md_by_id(algo_type);
        cached_ctx_group->ctx_template = HMAC_CTX_new();
        if (cached_ctx_group->ctx_template == NULL) {
            goto err;
        }
        if (key == NULL && key_len == 0) {
            key = dummy_key;
        }
        if (!HMAC_Init_ex(cached_ctx_group->ctx_template, key, key_len, evp_md, NULL)) {
            goto err;
        }
    }
    /* copy contents of template to worker ctx */
    if (!HMAC_CTX_copy(cached_ctx_group->ctx_worker, cached_ctx_group->ctx_template)) {
        goto err;
    }
    
    /* run std lib func to calculate hmac */
    if (result == NULL) {
        result = result_container;
    }
    if (!HMAC_Update(cached_ctx_group->ctx_worker, data, data_len)) {
        goto err;
    }
    if (!HMAC_Final(cached_ctx_group->ctx_worker, result, result_len)) {
        goto err;
    }
    return true;
err:
    cached_ctx_group->free_hmac_ctx_all();
    return false;
}

static const EVP_CIPHER* get_evp_cipher_md_by_algo(ColumnEncryptionAlgorithm columnEncryptionAlgorithm)
{
    const EVP_CIPHER *cipher = NULL;
    switch (columnEncryptionAlgorithm) {
        case ColumnEncryptionAlgorithm::AEAD_AES_256_CBC_HMAC_SHA256:
            cipher = EVP_aes_256_cbc();
            break;
        case ColumnEncryptionAlgorithm::AEAD_AES_128_CBC_HMAC_SHA256:
            cipher = EVP_aes_128_cbc();
            break;
        case ColumnEncryptionAlgorithm::SM4_SM3:
            cipher = EVP_sm4_cbc();
            break;
        case ColumnEncryptionAlgorithm::AES_256_GCM_ALGO:
            cipher = EVP_aes_256_gcm();
            break;
        case ColumnEncryptionAlgorithm::AES_256_CTR_ALGO:
            cipher = EVP_aes_256_ctr();
            break;
        default:
            break;
    }
    return cipher;
}

/*
 * Computes a keyed hash of a given text
 * currently used for both generating a MAC and as a KDF
 */
static bool hmac_sha256_iv(AeadAesHamcEncKey &column_encryption_key, int keylen, const unsigned char *data,
    int datalen, unsigned char *result)
{
    unsigned int result_len = g_key_size;
    if (!cached_hmac(NID_hmacWithSHA256, (const unsigned char *)column_encryption_key.get_iv_key(), keylen, data,
        datalen, result, &result_len, &column_encryption_key.hmac_ctx_group_iv)) {
        return false;
    }
    return true;
}

static bool hmac_sha256_mac(AeadAesHamcEncKey &column_encryption_key, int keylen, const unsigned char *data,
    int datalen, unsigned char *result)
{
    unsigned int result_len = g_key_size;
    if (!cached_hmac(NID_hmacWithSHA256, (const unsigned char *)column_encryption_key.get_mac_key(), keylen, data,
        datalen, result, &result_len, &column_encryption_key.hmac_ctx_group_mac)) {
        return false;
    }
    return true;
}

/*
 * Computes a keyed hash of a given text by sm3
 */
static bool sm3(const unsigned char *data, int datalen, unsigned char *result)
{
    unsigned int result_len = g_key_size;
    EVP_MD_CTX *md_ctx = NULL;
    md_ctx = EVP_MD_CTX_new();
    if (md_ctx == NULL) {
        printf("ERROR(CLIENT): Fail to create the context in sm3 algorithm.\n");
        return false;
    }
    if (!EVP_DigestInit_ex(md_ctx, EVP_sm3(), NULL)) {
        printf("ERROR(CLIENT): Fail to initialise the context in sm3 algorithm.\n");
        EVP_MD_CTX_free(md_ctx);
        return false;
    }
    if (!EVP_DigestUpdate(md_ctx, data, (size_t)datalen)) {
        printf("ERROR(CLIENT): Fail to compute digest in sm3 algorithm.\n");
        EVP_MD_CTX_free(md_ctx);
        return false;
    }
    if (!EVP_DigestFinal_ex(md_ctx, result, &result_len)) {
        printf("ERROR(CLIENT): Fail to compute digest final in sm3 algorithm.\n");
        EVP_MD_CTX_free(md_ctx);
        return false;
    }
    EVP_MD_CTX_free(md_ctx);
    return true;
}

static bool get_hash_by_cek_algo(ColumnEncryptionAlgorithm column_encryption_algorithm, int hmac_length,
    AeadAesHamcEncKey &column_encryption_key, const unsigned char *data, unsigned char *authentication_tag)
{
    bool res = false;
    if (column_encryption_algorithm == ColumnEncryptionAlgorithm::SM4_SM3) {
        res = sm3(data, hmac_length, authentication_tag);
    } else {
        res = hmac_sha256_mac(column_encryption_key, g_auth_tag_size, data, hmac_length, authentication_tag);
    }
    return res;
}

/*
 * To calculate the ciphertext buffer size
 */
int get_cipher_text_size(int plain_text_size)
{
    int numBlocks = plain_text_size / g_block_size + 1;
    int cipher_len = numBlocks * g_block_size;
    /* Output buffer size = size of VersionByte + Authentication Tag + IV + cipher Text blocks. */
    return (g_algo_version_size + g_auth_tag_size + g_iv_size + cipher_len);
}

/*
 * Set iv len, tag len, cipher_start to EncParam
 */
static bool set_enc_param(ColumnEncryptionAlgorithm algo, EncParam *enc_param, int &cipher_start)
{
    cipher_start = 0;
    switch (algo) {
        case ColumnEncryptionAlgorithm::AEAD_AES_256_CBC_HMAC_SHA256:
        case ColumnEncryptionAlgorithm::AEAD_AES_128_CBC_HMAC_SHA256:
        case ColumnEncryptionAlgorithm::SM4_SM3:
            enc_param->iv_len = g_iv_size;
            enc_param->tag_len = g_auth_tag_size;
            cipher_start = g_algo_version_size + g_auth_tag_size + g_iv_size;
            break;
        case ColumnEncryptionAlgorithm::AES_256_GCM_ALGO:
            enc_param->iv_len = g_gcm_iv_size;
            enc_param->tag_len = g_gcm_auth_tag_size;
            cipher_start = g_algo_version_size + g_gcm_auth_tag_size + g_gcm_iv_size;
            break;
        case ColumnEncryptionAlgorithm::AES_256_CTR_ALGO:
            enc_param->iv_len = g_iv_size;
            enc_param->tag_len = 0;
            cipher_start = g_algo_version_size + g_iv_size;
            break;
        default:
            return false;
    }
    return true;
}

static bool is_need_hmac_check(ColumnEncryptionAlgorithm algo)
{
    if (algo == ColumnEncryptionAlgorithm::AES_256_GCM_ALGO ||
        algo == ColumnEncryptionAlgorithm::AES_256_CTR_ALGO) {
        return false;
    }
    return true;
}

/*
 * Encryption data
 */
int encrypt_data(unsigned char *plain_text, int plain_text_length, AeadAesHamcEncKey &column_encryption_key,
    EncryptionType encryption_type, unsigned char *result, ColumnEncryptionAlgorithm column_encryption_algorithm)
{
    bool is_null = (plain_text == NULL || plain_text_length <= 0 || encryption_type == EncryptionType::INVALID_TYPE ||
        result == NULL);
    if (is_null) {
        /* invalid input */
        return 0;
    }
    errno_t res = EOK;
    /* Prepare EncParam */
    EncParam enc_param;
    int cipher_start = 0;
    if (!set_enc_param(column_encryption_algorithm, &enc_param, cipher_start)) {
        return 0;
    }
    enc_param.plaintext = plain_text;
    enc_param.plaintext_len = plain_text_length;
    enc_param.tag = result;
    enc_param.ciphertext = result + cipher_start;

    /* Prepare IV.IV should be 1 single block (16 bytes) */
    unsigned char _iv [g_key_size + 1] = {0};
    unsigned char iv_truncated[g_iv_size + 1] = {0};
    if (encryption_type == EncryptionType::DETERMINISTIC_TYPE) {
        /*
         * determenistic encryption - we create an initiailization vector based on the plaintext - to make the
         * encryption CPA-secure
         * HMAC_SHA_256
         */
        bool rc = hmac_sha256_iv(column_encryption_key, g_key_size, plain_text, plain_text_length, _iv);
        if (!rc) {
            (void)printf("ERROR(CLIENT): fail to get iv value.\n");
            return 0;
        }

        /* iv is truncated to 128 bits. */
        res = memcpy_s(iv_truncated, g_iv_size + 1, _iv, g_block_size);
        if (res != EOK) {
            km_securec_check(res, "\0", "\0");
            printf("ERROR(CLIENT): fail to copy 128 bit iv from 256 bit iv value.\n");
            return 0;
        }
    } else {
        if (encryption_type != EncryptionType::RANDOMIZED_TYPE) {
            return 0;
        }

        if (RAND_priv_bytes(iv_truncated, g_block_size) != 1) {
            return 0;
        }
    }
    /* gcm's iv len is difference, truncate it */
    enc_param.iv = iv_truncated;
    if (column_encryption_algorithm == ColumnEncryptionAlgorithm::AES_256_GCM_ALGO) {
        res = memset_s(iv_truncated + g_gcm_iv_size, (g_iv_size - g_gcm_iv_size) + 1, 0,
            (g_iv_size - g_gcm_iv_size) + 1);
        km_securec_check(res, "\0", "\0");
    }

    const EVP_CIPHER *cipher = get_evp_cipher_md_by_algo(column_encryption_algorithm);
    if (cipher == NULL) {
        printf("ERROR(CLIENT): invalid column encryption encryption algorithm, please check it!.\n");
        return 0;
    }
    const unsigned char *key = column_encryption_key.get_encyption_key();
    /* Add the ciphertext  */
    int ciphertext_size = 0;
    if (column_encryption_algorithm == ColumnEncryptionAlgorithm::AEAD_AES_128_CBC_HMAC_SHA256) {
        /* iv is truncated to 128 bits. */
        int encrypt_key_len = g_key_size / 2 + 1;
        unsigned char encrypt_key[encrypt_key_len] = {0};
        res = memcpy_s(encrypt_key, encrypt_key_len, key, g_key_size / 2);
        if (res != EOK) {
            printf("ERROR(CLIENT): Fail to copy 128 bit from 256 bit key value.\n");
            km_securec_check(res, "\0", "\0");
            return 0;
        }
        enc_param.key = encrypt_key;
        enc_param.key_len = encrypt_key_len;
        ciphertext_size = encrypt(&enc_param, cipher, column_encryption_key.encrypt_ctx);
        res = memset_s(encrypt_key, encrypt_key_len, 0, encrypt_key_len);
        if (res != EOK) {
            printf("ERROR(CLIENT): Fail to clear 128 bit key value.\n");
            return 0;
        }
    } else {
        enc_param.key = key;
        enc_param.key_len = g_key_size;
        ciphertext_size = encrypt(&enc_param, cipher, column_encryption_key.encrypt_ctx);
    }
    if (ciphertext_size < 0) {
        /* failed to encrypt */
        return 0;
    }

    /* add the Algorithm Version */
    res = memcpy_s(result + enc_param.tag_len, g_algo_version_size, algo_version, g_algo_version_size);
    km_securec_check(res, "\0", "\0");

    /* add the IV */
    int ivStartIndex = enc_param.tag_len + g_algo_version_size;
    res = memcpy_s(result + ivStartIndex, enc_param.iv_len, iv_truncated, enc_param.iv_len);
    km_securec_check(res, "\0", "\0");

    /* add the HMAC (of versionbyte + IV + Ciphertext) */
    int hmacDataSize = g_algo_version_size + enc_param.iv_len + ciphertext_size;
    if (!is_need_hmac_check(column_encryption_algorithm)) {
        return (enc_param.tag_len + hmacDataSize);
    }
    bool hmac_res = get_hash_by_cek_algo(column_encryption_algorithm, hmacDataSize, column_encryption_key,
        result + enc_param.tag_len, result);
    if (!hmac_res) {
        printf("ERROR(CLIENT): Fail to compute a keyed hash of a given text.\n");
        return 0;
    }

    return (enc_param.tag_len + hmacDataSize);
}

static bool check_data_integrity_by_ctr(ColumnEncryptionAlgorithm column_encryption_algorithm,
    AeadAesHamcEncKey &column_encryption_key, EncParam *enc_param)
{
    if (column_encryption_algorithm == ColumnEncryptionAlgorithm::AES_256_CTR_ALGO) {
        /*
        * determenistic encryption - we create an initiailization vector based on the plaintext - to make the
        * encryption CPA-secure
        * HMAC_SHA_256
        */
        unsigned char iv_to_verify [g_key_size + 1] = {0};
        unsigned char iv_to_verify_truncated[g_iv_size + 1] = {0};
        bool rc = hmac_sha256_iv(column_encryption_key, g_key_size, enc_param->plaintext, enc_param->plaintext_len,
            iv_to_verify);
        if (!rc) {
            (void)printf("ERROR(CLIENT): fail to get iv value.\n");
            return false;
        }
        errno_t res = EOK;
        /* iv is truncated to 128 bits. */
        res = memcpy_s(iv_to_verify_truncated, g_iv_size + 1, iv_to_verify, g_block_size);
        if (res != EOK) {
            securec_check_c(res, "\0", "\0");
            printf("ERROR(CLIENT): fail to copy 128 bit iv from 256 bit iv value.\n");
            return false;
        }
        if (my_memcmp(enc_param->iv, iv_to_verify_truncated, enc_param->iv_len) != 0) {
            printf("ERROR(CLIENT): Fail to compute a keyed hash of a given text.\n");
            return false;
        }
    }
    return true;
}

/*
 * Decryption steps
 *  1. Validate version byte
 *  2. Validate Authentication tag
 *  3. Decrypt the message
 */
int decrypt_data(unsigned char *cipher_text, int cipher_text_length,
    AeadAesHamcEncKey &column_encryption_key, unsigned char *decryptedtext,
    ColumnEncryptionAlgorithm column_encryption_algorithm)
{
    if (cipher_text == NULL || cipher_text_length <= 0 || decryptedtext == NULL) {
        return 0;
    }

    if (cipher_text_length < get_min_ciph_len(column_encryption_algorithm)) {
        printf("ERROR(CLIENT): The length of cipher_text is invalid, cannot decrypt.\n");
        return 0;
    }
    /* Prepare EncParam */
    EncParam enc_param;
    int cipher_start_index = 0; // this is where cipher starts.
    if (!set_enc_param(column_encryption_algorithm, &enc_param, cipher_start_index)) {
        return 0;
    }
    enc_param.plaintext = decryptedtext;
    enc_param.ciphertext_len = cipher_text_length - cipher_start_index;
    enc_param.ciphertext = cipher_text + cipher_start_index;

    if (cipher_text[enc_param.tag_len] != '1') {
        /* Cipher text was computed with a different algorithm version than this. */
        printf("ERROR(CLIENT): Version byte of cipher_text is invalid, cannot decrypt.\n");
        return 0;
    }
    unsigned char iv[enc_param.iv_len] = {0};
    enc_param.iv = iv;
    errno_t rc = memcpy_s(iv, enc_param.iv_len, cipher_text + enc_param.tag_len + g_algo_version_size,
        enc_param.iv_len);
    km_securec_check(rc, "\0", "\0");

    /* Computing the authentication tag */
    unsigned char authenticationTag[enc_param.tag_len + 1] = {0};
    if (column_encryption_algorithm == ColumnEncryptionAlgorithm::AES_256_CTR_ALGO) {
        enc_param.tag = NULL;
    } else if (column_encryption_algorithm != ColumnEncryptionAlgorithm::AES_256_GCM_ALGO) {
        enc_param.tag = authenticationTag;
        int HMAC_length = cipher_text_length - enc_param.tag_len;
        bool res = get_hash_by_cek_algo(column_encryption_algorithm, HMAC_length, column_encryption_key,
            cipher_text + enc_param.tag_len, authenticationTag);
        if (!res) {
            printf("ERROR(CLIENT): Fail to compute a keyed hash of a given text.\n");
            return 0;
        }

        int cmp_result = my_memcmp(authenticationTag, cipher_text, enc_param.tag_len);
        if (cmp_result != 0) {
            /* MAC check failed */
            return 0;
        }
    } else {
        enc_param.tag = authenticationTag;
        rc = memcpy_s(authenticationTag, enc_param.tag_len, cipher_text, enc_param.tag_len);
        km_securec_check(rc, "\0", "\0");
    }

    /* Decrypt the ciphertext */
    const EVP_CIPHER *cipher = get_evp_cipher_md_by_algo(column_encryption_algorithm);
    if (cipher == NULL) {
        printf("ERROR(CLIENT): invalid column encryption encryption algorithm, please check it!.\n");
        return 0;
    }
    const unsigned char *key = column_encryption_key.get_encyption_key();
    int decryptedtext_len = 0;
    if (column_encryption_algorithm == ColumnEncryptionAlgorithm::AEAD_AES_128_CBC_HMAC_SHA256) {
        /* iv is truncated to 128 bits. */
        int encrypt_key_len = g_key_size / 2 + 1;
        unsigned char encrypt_key[encrypt_key_len] = {0};
        errno_t result = memcpy_s(encrypt_key, encrypt_key_len, key, g_key_size / 2);
        if (result != EOK) {
            printf("ERROR(CLIENT): Fail to copy 128 bit from 256 bit key value.\n");
            km_securec_check(result, "\0", "\0");
            return 0;
        }
        enc_param.key = encrypt_key;
        enc_param.key_len = encrypt_key_len;
        decryptedtext_len = decrypt(&enc_param, cipher, column_encryption_key.decrypt_ctx);
        result = memset_s(encrypt_key, encrypt_key_len, 0, encrypt_key_len);
        if (result != EOK) {
            printf("ERROR(CLIENT): Fail to clear 128 bit key value.\n");
            return 0;
        }
    } else {
        enc_param.key = key;
        enc_param.key_len = g_key_size;
        decryptedtext_len = decrypt(&enc_param, cipher, column_encryption_key.decrypt_ctx);
    }
    if (decryptedtext_len < 0) {
        return 0;
    }
    if (!check_data_integrity_by_ctr(column_encryption_algorithm, column_encryption_key, &enc_param)) {
        return 0;
    }
    /* Add a NULL terminator. We are expecting printable text */
    decryptedtext[decryptedtext_len] = '\0';
    return decryptedtext_len;
}

/*
 * encrypt plaintext thought AES_256_cbc or AES_256_gcm algorithm
 * cell_ciphertext = AES-CBC-256(enc_key, cell_iv, cell_data) with PKCS7 padding.
 */
static int encrypt(EncParam *enc_param, const EVP_CIPHER *cipher, EVP_CIPHER_CTX *&ctx)
{
    bool is_invalid = enc_param->plaintext == NULL || enc_param->plaintext_len <= 0 || enc_param->key == NULL ||
        enc_param->iv == NULL || enc_param->ciphertext == NULL;
    if (is_invalid) {
        return 0;
    }
    if (ctx == NULL) {
        /* Create and initialise the context */
        ctx = EVP_CIPHER_CTX_new();
        if (ctx == NULL) {
            printf("ERROR(CLIENT): Fail to create and initialise the context.\n");
            return -1;
        }
        /*
        * Initialise the encryption operation. IMPORTANT - ensure you use a key
        * and IV size appropriate for your cipher
        * In this example we are using 256 bit AES (i.e. a 256 bit key). The
        * IV size for *most* modes is the same as the block size. For AES this
        * is 128 bits
        */
        if (EVP_EncryptInit_ex(ctx, cipher, NULL, NULL, NULL) != 1) {
            printf("ERROR(CLIENT): Fail to create new cipher.\n");
            EVP_CIPHER_CTX_free(ctx);
            ctx = NULL;
            return -1;
        }

        if (EVP_EncryptInit_ex(ctx, NULL, NULL, enc_param->key, enc_param->iv) != 1) {
            (void)printf("ERROR(CLIENT): Fail to set key and iv.\n");
            EVP_CIPHER_CTX_free(ctx);
            ctx = NULL;
            return -1;
        }
    } else {
        if (EVP_EncryptInit_ex(ctx, NULL, NULL, NULL, enc_param->iv) != 1) {
            (void)printf("ERROR(CLIENT): Fail to set iv.\n");
            EVP_CIPHER_CTX_free(ctx);
            ctx = NULL;
            return -1;
        }
    }

    /*
     * Provide the message to be encrypted, and obtain the encrypted output.
     * EVP_EncryptUpdate can be called multiple times if necessary
     */
    int len = 0;
    int ciphertext_len = 0;
    if (EVP_EncryptUpdate(ctx, enc_param->ciphertext, &len, enc_param->plaintext, enc_param->plaintext_len) != 1) {
        printf("ERROR(CLIENT): Fail to encrypt.\n");
        EVP_CIPHER_CTX_free(ctx);
        ctx = NULL;
        return -1;
    }
    ciphertext_len = len;
    /*
     * Finalise the encryption. Further ciphertext bytes may be written at
     * this stage.
     */
    if (EVP_EncryptFinal_ex(ctx, enc_param->ciphertext + len, &len) != 1) {
        printf("ERROR(CLIENT): Fail to encrypt final.\n");
        EVP_CIPHER_CTX_free(ctx);
        ctx = NULL;
        return -1;
    }
    ciphertext_len += len;
    if (cipher == EVP_aes_256_gcm()) {
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_GET_TAG, enc_param->tag_len, enc_param->tag) != 1) {
            (void)printf("ERROR(CLIENT): Fail to get tag during encrypt data.\n");
            EVP_CIPHER_CTX_free(ctx);
            ctx = NULL;
            return -1;
        }
    }
    enc_param->ciphertext_len = ciphertext_len;

    return ciphertext_len;
}

/*
 * Decrypt the message though aes_256_cbc or AES_256_gcm algorithm
 */
static int decrypt(EncParam *enc_param, const EVP_CIPHER *cipher, EVP_CIPHER_CTX *&ctx)
{
    if (ctx == NULL) {
        ctx = EVP_CIPHER_CTX_new();
        /* Create and initialise the context */
        if (ctx == NULL) {
            printf("ERROR(CLIENT): keymanage cannot new ctx.\n");
            return -1;
        }
        /*
        * Initialise the decryption operation. IMPORTANT - ensure you use a key
        * and IV size appropriate for your cipher
        * In this example we are using 256 bit AES (i.e. a 256 bit key). The
        * IV size for *most* modes is the same as the block size. For AES this
        * is 128 bits
        */
        if (EVP_DecryptInit_ex(ctx, cipher, NULL, NULL, NULL) != 1) {
            printf("ERROR(CLIENT): cannot create new cipher.\n");
            EVP_CIPHER_CTX_free(ctx);
            ctx = NULL;
            return -1;
        }

        /* Initialize key and iv, in gcm, not necessary to set iv length if this is 12 bytes (96 bits) */
        if (EVP_DecryptInit_ex(ctx, NULL, NULL, enc_param->key, enc_param->iv) != 1) {
            (void)printf("ERROR(CLIENT): EVP_DecryptInit_ex set key and iv error.\n");
            EVP_CIPHER_CTX_free(ctx);
            ctx = NULL;
            return -1;
        }
    } else {
        if (EVP_DecryptInit_ex(ctx, NULL, NULL, NULL, enc_param->iv) != 1) {
            printf("ERROR(CLIENT): cannot create new cipher.\n");
            EVP_CIPHER_CTX_free(ctx);
            ctx = NULL;
            return -1;
        }
    }

    /*
     * Provide the message to be decrypted, and obtain the plaintext output.
     * EVP_DecryptUpdate can be called multiple times if necessary.
     */
    int len = 0;
    int plaintext_len = 0;
    if (EVP_DecryptUpdate(ctx, enc_param->plaintext, &len, enc_param->ciphertext, enc_param->ciphertext_len) != 1) {
        printf("ERROR(CLIENT): cannot EVP_EncryptUpdate.\n");
        EVP_CIPHER_CTX_free(ctx);
        ctx = NULL;
        return -1;
    }
    plaintext_len = len;

    if (cipher == EVP_aes_256_gcm()) {
        if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_SET_TAG, enc_param->tag_len, enc_param->tag) != 1) {
            (void)printf("ERROR(CLIENT): EVP_CIPHER_CTX_ctrl set tag error.\n");
            EVP_CIPHER_CTX_free(ctx);
            ctx = NULL;
            return -1;
        }
    }

    /*
     * Finalise the decryption. Further plaintext bytes may be written at
     * this stage.
     */
    if (EVP_DecryptFinal_ex(ctx, enc_param->plaintext + len, &len) != 1) {
        (void)printf(
            "ERROR(CLIENT): cannot EVP_DecryptFinal_ex. error: %s.\n", ERR_error_string(ERR_get_error(), NULL));
        EVP_CIPHER_CTX_free(ctx);
        ctx = NULL;
        return -1;
    }
    plaintext_len += len;
    enc_param->plaintext_len = plaintext_len;
    return enc_param->plaintext_len;
}

static int my_memcmp(const void *buffer1, const void *buffer2, int count)
{
    if (!count) {
        return 0;
    }
    while (count--) {
        if (*(char *)buffer1 != *(char *)buffer2) {
            return (*((unsigned char *)buffer1) - *((unsigned char *)buffer2));
        }
        buffer1 = (char *)buffer1 + 1;
        buffer2 = (char *)buffer2 + 1;
    }
    return 0;
}
