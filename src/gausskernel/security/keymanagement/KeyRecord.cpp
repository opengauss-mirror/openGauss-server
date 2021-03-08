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
 * KeyRecord.cpp
 *    The form of key record
 *
 * IDENTIFICATION
 *    src/gausskernel/security/keymanagement/KeyRecord.cpp
 *
 * ---------------------------------------------------------------------------------------
 */


#include <iostream>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include "cipher.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/guc.h"
#include "keymanagement/KeyRecord.h"

KeyRecord::KeyRecord()
{
    tde_algo = TDE_ALGO_NULL;
}

/*
 * The string encrypted_sample_string is the result of encryption using AES algorithm
 * by TRANS_ENCRYPT_SAMPLE_STRING.
 */
bool KeyRecord::create_encrypted_sample_string(std::string dek_b64)
{
    int ret = 0;
    unsigned char dek[DEK_LEN] = {};
    ret = EVP_DecodeBlock(dek, (unsigned char*)dek_b64.c_str(), dek_b64.length());
    if (ret < 0) {
        std::cout << "keymanagement DEK is wrong, base64 decode failed." << std::endl;
        return false;
    }

    EVP_CIPHER_CTX* ctx = NULL;
    const EVP_CIPHER* cipher = NULL;

    /* new cipher ctx */
    ctx = EVP_CIPHER_CTX_new();
    if (ctx == NULL) {
        std::cout << "keymanagement create encrypted sample string failed, cannot new cipher ctx." << std::endl;
        return false;
    }

    /* get aes cipher address */
    cipher = EVP_aes_128_ctr();
    if (cipher == NULL) {
        std::cout << "keymanagement create encrypted sample string failed, cannot get aes128 cipher." << std::endl;
        EVP_CIPHER_CTX_free(ctx);
        return false;
    }

    unsigned char iv[] = TRANS_ENCRYPT_SAMPLE_RNDM;
    /* init ctx */
    ret = EVP_EncryptInit(ctx, cipher, dek, iv);
    if (ret == 0) {
        std::cout << "keymanagement create encrypted sample string failed, cannot init cipher." << std::endl;
        EVP_CIPHER_CTX_free(ctx);
        return false;
    }

    unsigned char plaintext[] = TRANS_ENCRYPT_SAMPLE_STRING;
    unsigned char ciphertext[sizeof(plaintext) + 1] = {};
    int ciphertextlen = 0;

    /* Perform encryption operations */
    ret = EVP_EncryptUpdate(ctx, ciphertext, &ciphertextlen, plaintext, sizeof(plaintext));
    if (ret == 0) {
        std::cout << "keymanagement create encrypted sample string failed, cannot encrypt. Plaintext length:"
                  << sizeof(plaintext) << std::endl;
        EVP_CIPHER_CTX_free(ctx);

        return false;
    }

    /* Base64 encryption */
    unsigned char ciphertext_base64[ciphertextlen * 3];

    memset_s(ciphertext_base64, sizeof(ciphertext_base64), 0, sizeof(ciphertext_base64));
    ret = EVP_EncodeBlock(ciphertext_base64, ciphertext, ciphertextlen);
    if (ret < 0) {
        std::cout << "keymanagement create encrypted sample string failed, cannot base64 encoding." << std::endl;
        EVP_CIPHER_CTX_free(ctx);
        return false;
    }

    encrypted_sample_string = std::string((char*)ciphertext_base64);
    std::cout << "keymanagement create encrypted sample string ok." << std::endl;
    EVP_CIPHER_CTX_free(ctx);
    return true;
}

/* Decrypt encrypted_sample_string to verify the correctness of DEK. */
bool KeyRecord::check_encrypted_sample_string(unsigned char* dek)
{
    if (encrypted_sample_string.empty() || dek == NULL) {
        return false;
    }
    int ret = 0;
    unsigned char encrypted_sample_string_orign[encrypted_sample_string.length()];
    memset_s(
        encrypted_sample_string_orign, sizeof(encrypted_sample_string_orign), 0, sizeof(encrypted_sample_string_orign));
    /* Base64 decryption */
    int encrypted_sample_string_orign_length = EVP_DecodeBlock(encrypted_sample_string_orign,
        (unsigned char*)encrypted_sample_string.c_str(),
        encrypted_sample_string.length());
    if (encrypted_sample_string_orign_length <= 0) {
        std::cout << "keymanagement check encrypted sample string failed, base64 decode fail." << std::endl;
        return false;
    }

    EVP_CIPHER_CTX* ctx = NULL;
    const EVP_CIPHER* cipher = NULL;

    /* new cipher ctx */
    ctx = EVP_CIPHER_CTX_new();
    if (ctx == NULL) {
        std::cout << "keymanagement check encrypted sample string failed, cannot new ctx." << std::endl;
        return false;
    }

    /* get aes cipher address */
    cipher = EVP_aes_128_ctr();
    if (cipher == NULL) {
        std::cout << "keymanagement check encrypted sample string failed, cannot get aes128 cipher." << std::endl;
        EVP_CIPHER_CTX_free(ctx);
        return false;
    }

    unsigned char iv[] = TRANS_ENCRYPT_SAMPLE_RNDM;
    /* init ctx */
    ret = EVP_DecryptInit(ctx, cipher, dek, iv);
    if (ret == 0) {
        std::cout << "keymanagement check encrypted sample string failed, cannot init cipher." << std::endl;
        EVP_CIPHER_CTX_free(ctx);
        return false;
    }

    /* the max len of plan text is 256 */
    unsigned char out_plain_text[256] = {};
    int out_plain_text_len = 0;
    /* Perform decryption operations */
    ret = EVP_DecryptUpdate(
        ctx, out_plain_text, &out_plain_text_len, encrypted_sample_string_orign, encrypted_sample_string_orign_length);
    if (ret == 0) {
        std::cout << "keymanagement check encrypted sample string failed, decrypt failed." << std::endl;
        EVP_CIPHER_CTX_free(ctx);
        return false;
    }

    /* Comparing value */
    if (strlen((const char*)out_plain_text) != strlen(TRANS_ENCRYPT_SAMPLE_STRING) ||
        strncmp((const char*)out_plain_text, TRANS_ENCRYPT_SAMPLE_STRING, strlen(TRANS_ENCRYPT_SAMPLE_STRING)) != 0) {
        std::cout << "keymanagement check encrypted sample string failed." << std::endl;
        EVP_CIPHER_CTX_free(ctx);
        return false;
    }
    EVP_CIPHER_CTX_free(ctx);

    return true;
}

std::string KeyRecord::getEncryptedSampleStr()
{
    if (encrypted_sample_string.empty()) {
        std::cout << "keymanagement encrypted sample string is empty." << std::endl;
    }

    return encrypted_sample_string;
};

/*
 * Create the initial value needed by DEK
 */
bool KeyRecord::create_dek_iv()
{
    if (RAND_priv_bytes(dek_iv, TDE_IV_LEN) != 1) {
        return false;
    }

    return true;
}

/*
 * get the initial value if ready
 */
bool KeyRecord::getIV(unsigned char* out_iv)
{
    if (out_iv == NULL) {
        std::cout << "keymanagement out_iv is NULL." << std::endl;
        return false;
    }

    int ret = memcpy_s(out_iv, TDE_IV_LEN, dek_iv, TDE_IV_LEN);
    securec_check(ret, "\0", "\0");

    return true;
}

TDE_ALGO KeyRecord::get_tde_algo() const
{
    return tde_algo;
};

