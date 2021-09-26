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
 * File Name	: evp_cipher.cpp
 * Brief		:
 * Description	: encrypt and decrypt functions for TDE
 *
 * History	: 2021-04
 * 
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/aes/evp_cipher.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/engine.h>
#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/evp_cipher.h"

#define SM4_ENGINE_ID "kae";
THR_LOCAL ENGINE* g_engine = NULL;
THR_LOCAL bool g_init_engine = false;

/* init hardware driver for SM4. */
ENGINE* init_cipher_engine()
{
    if (g_init_engine) {
        return g_engine;
    }

    const char* id = SM4_ENGINE_ID;
    /* OPENSSL_init_crypto return 1 on success or 0 on error */
    if (OPENSSL_init_crypto(OPENSSL_INIT_LOAD_CONFIG, NULL) == 0) {
        ereport(LOG, (errmsg("OpenSSL init crypto failed for %s hardware driver", id)));
        return NULL;
    }
    g_engine = ENGINE_by_id(id);
    g_init_engine = true;
    return g_engine;
}

bool ctr_dec_partial_mode(const char* decalgoText, ENGINE* engine, const EVP_CIPHER* cipher,
    const char* cipherText, const size_t cipherLength, char* plainText, size_t* plainLength,
    unsigned char* key, unsigned char* iv)
{
    int segPlainLength = 0;
    int lastSegPlainLength = 0;
    int ret = 0;
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (ctx == NULL) {
        ereport(LOG, (errmsg("EVP_CIPHER_CTX_new failed when decrypt with %s", decalgoText)));
        return false;
    }

    /* EVP_DecryptInit_ex() and EVP_DecryptUpdate() return 1 for success and 0 for failure. */
    ret = EVP_DecryptInit_ex(ctx, cipher, engine, key, iv);
    if (ret == 0) {
        EVP_CIPHER_CTX_free(ctx);
        ereport(LOG, (errmsg("EVP_DecryptInit_ex failed when decrypt with %s", decalgoText)));
        return false;
    }

    ret = EVP_DecryptUpdate(ctx, (unsigned char*)plainText, &segPlainLength, (unsigned char*)cipherText, cipherLength);
    if (ret == 0) {
        EVP_CIPHER_CTX_free(ctx);
        ereport(LOG, (errmsg("EVP_DecryptUpdate failed when decrypt with %s", decalgoText)));
        return false;
    }

    /* EVP_DecryptFinal_ex() returns 0 if the decrypt failed or 1 for success. */
    ret = EVP_DecryptFinal_ex(ctx, (unsigned char*)plainText + segPlainLength, &lastSegPlainLength);
    if (ret == 0) {
        EVP_CIPHER_CTX_free(ctx);
        ereport(LOG, (errmsg("EVP_DecryptFinal_ex failed when decrypt with %s", decalgoText)));
        return false;
    }
    
    *plainLength = segPlainLength + lastSegPlainLength;
    EVP_CIPHER_CTX_free(ctx);
    return true;
}

bool ctr_enc_partial_mode(const char* encalgoText, ENGINE* engine, const EVP_CIPHER* cipher,
    const char* plainText, const size_t plainLength, char* cipherText, size_t* cipherLength,
    unsigned char* key, unsigned char* iv)
{
    int SegCipherLength = 0;
    int LastSegCipherLength = 0;
    int ret = 0;
    EVP_CIPHER_CTX* ctx = NULL;
    ctx = EVP_CIPHER_CTX_new();
    if (ctx == NULL) {
        ereport(LOG, (errmsg("EVP_CIPHER_CTX_new failed when encrypt with %s", encalgoText)));
        return false;
    }

    /* EVP_EncryptInit_ex(), EVP_EncryptUpdate() and EVP_EncryptFinal_ex() return 1 for success and 0 for failure. */
    ret = EVP_EncryptInit_ex(ctx, cipher, engine, key, iv);
    if (ret == 0) {
        EVP_CIPHER_CTX_free(ctx);
        ereport(LOG, (errmsg("EVP_EncryptInit_ex failed when encrypt with %s", encalgoText)));
        return false;
    }

    ret = EVP_EncryptUpdate(ctx, (unsigned char*)cipherText, &SegCipherLength, (unsigned char*)plainText, plainLength);
    if (ret == 0) {
        EVP_CIPHER_CTX_free(ctx);
        ereport(LOG, (errmsg("EVP_EncryptUpdate failed when encrypt with %s", encalgoText)));
        return false;
    }

    ret = EVP_EncryptFinal_ex(ctx, (unsigned char*)cipherText + SegCipherLength, &LastSegCipherLength);
    if (ret == 0) {
        EVP_CIPHER_CTX_free(ctx);
        ereport(LOG, (errmsg("EVP_EncryptFinal_ex failed when encrypt with %s", encalgoText)));
        return false;
    }
    
    *cipherLength = SegCipherLength + LastSegCipherLength;
    EVP_CIPHER_CTX_free(ctx);
    return true;
}

/*
 * Description:
 *      Encrypt with standard AES/SM4 algorthm using openssl functions.
 *      For SM4, engine is set for hardware instruction acceleration on ARM platform.
 * Input:
 *      plainText: plain text need to be encrypted
 *      plainLength: plain text length
 *      key: encryption key
 *      iv: initial vector
 *      algo: encryption algorithm
 * Output:
 *      cipherText: ciphertext text after encrypted
 *      cipherLength: cipher text length
 * Return:
 *      true: success
 *      false: failure
 */
bool encrypt_partial_mode(const char* plainText, const size_t plainLength, char* cipherText,
    size_t* cipherLength, unsigned char* key, unsigned char* iv, TdeAlgo algo)
{
    const char* algoText = NULL;
    ENGINE* engine = NULL; /* if engine is NULL then the default implementation is used */
    const EVP_CIPHER* cipher = NULL;

    switch (algo) {
        case TDE_ALGO_AES_128_CTR:
            algoText = "aes-128-ctr";
            cipher = EVP_aes_128_ctr();
            break;
        case TDE_ALGO_SM4_CTR:
            algoText = "sm4-ctr";
            engine = init_cipher_engine();
            cipher = EVP_sm4_ctr();
            break;
        default:
            break;
    }

    if (algoText == NULL) {
        ereport(LOG, (errmsg("the tde algo is not support")));
        return false;
    }

    if (cipher == NULL) {
        ereport(LOG, (errmsg("new cipher failed when encrypt with %s", algoText)));
        return false;
    }
    
    /* begin to encrypt now */
    bool result = ctr_enc_partial_mode(algoText, engine, cipher, plainText, plainLength, cipherText, 
        cipherLength, key, iv);
    return result;
}

/*
 * Description:
 *      Encrypt with standard AES/SM4 algorthm using openssl functions.
 *      For SM4, engine is set for hardware instruction acceleration on ARM platform.
 * Input:
 *      cipherText: ciphertext text to be decrypted
 *      cipherLength: cipher text length
 *      key: decryption key
 *      iv: initial vector
 *      algo: decryption algorithm
 * Output:
 *      plainText: plain text need after decrypted
 *      plainLength: plain text length
 * Return:
 *      true: success
 *      false: failure
 */
bool decrypt_partial_mode(const char* cipherText, const size_t cipherLength, char* plainText,
    size_t* plainLength, unsigned char* key, unsigned char* iv, TdeAlgo algo)
{
    const char* algoText = NULL;
    ENGINE* engine = NULL;
    const EVP_CIPHER* cipher = NULL;

    switch (algo) {
        case TDE_ALGO_AES_128_CTR:
            algoText = "aes-128-ctr";
            cipher = EVP_aes_128_ctr();
            break;
        case TDE_ALGO_SM4_CTR:
            algoText = "sm4-ctr";
            engine = init_cipher_engine();
            cipher = EVP_sm4_ctr();
            break;
        default:
            break;
    }

    if (algoText == NULL) {
        ereport(LOG, (errmsg("the tde algo is not support")));
        return false;
    }

    if (cipher == NULL) {
        ereport(LOG, (errmsg("new cipher failed when decrypt with %s", algoText)));
        return false;
    }

    /* begin to decrypt now */
    bool result = ctr_dec_partial_mode(algoText, engine, cipher, cipherText, cipherLength, plainText, 
        plainLength, key, iv);
    return result;
}

