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
 * Description	: encrypt and decrypt functions for MPPDB
 *
 * History	: 2019-06
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
THR_LOCAL ENGINE* engine = NULL;
THR_LOCAL bool inited = false;
/* init hardware driver. */
void init_cipher_engine()
{
    if (inited) {
        return;
    }
    const char* id = SM4_ENGINE_ID;
    if (OPENSSL_init_crypto(OPENSSL_INIT_LOAD_CONFIG, NULL) == 0) {
        fprintf(stderr, ("Openssl crypto init config fail, Maybe cann't use hardware."));
    }
    engine = ENGINE_by_id(id);
    inited = true;
}

unsigned long ctr_dec_partial_mode(const char* decalgoText, const EVP_CIPHER* cipher, const char* cipherText,
    const size_t cipherLength, char* plainText, size_t* plainLength, unsigned char* key, unsigned char* iv)
{
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (ctx == NULL) {
        fprintf(stderr, ("%s ctx new fail\n"), decalgoText);
        return 1;
    }

    if (EVP_DecryptInit_ex(ctx, cipher, NULL, key, iv) < 0) {
        fprintf(stderr, ("%s EVP_EncryptInit_ex fail\n"), decalgoText);
        EVP_CIPHER_CTX_free(ctx);
        return 1;
    }

    int SegPlainLength = 0;
    int LastSegPlainLength = 0;
    int ret =
        EVP_DecryptUpdate(ctx, (unsigned char*)plainText, &SegPlainLength, (unsigned char*)cipherText, cipherLength);
    if (ret == 0) {
        fprintf(stderr, ("%s EVP_DecryptUpdate fail\n"), decalgoText);
        EVP_CIPHER_CTX_free(ctx);
        return 1;
    }
    ret = EVP_DecryptFinal_ex(ctx, (unsigned char*)plainText + SegPlainLength, &LastSegPlainLength);
    if (ret == 0) {
        fprintf(stderr, ("%s EVP_EncryptFinal_ex fail\n"), decalgoText);
        EVP_CIPHER_CTX_free(ctx);
        return 1;
    }
    *plainLength = SegPlainLength + LastSegPlainLength;
    EVP_CIPHER_CTX_free(ctx);

    return 0;
}

unsigned long ctr_enc_partial_mode(const char* encalgoText, const EVP_CIPHER* cipher, const char* plainText,
    const size_t plainLength, char* cipherText, size_t* cipherLength, unsigned char* key, unsigned char* iv)
{
    int plen = plainLength;
    EVP_CIPHER_CTX* ctx;
    ctx = EVP_CIPHER_CTX_new();
    if (ctx == NULL) {
        fprintf(stderr, ("%s new ctx fail.\n"), encalgoText);
        return 1;
    }
    if (EVP_EncryptInit_ex(ctx, cipher, engine, key, iv) < 0) {
        fprintf(stderr, ("%s EVP_EncryptInit_ex fail.\n"), encalgoText);
        EVP_CIPHER_CTX_free(ctx);
        return 1;
    }

    int SegCipherLength = 0;
    int LastSegCipherLength = 0;
    int ret = EVP_EncryptUpdate(ctx, (unsigned char*)cipherText, &SegCipherLength, (unsigned char*)plainText, plen);
    if (ret == 0) {
        fprintf(stderr, ("%s EVP_EncryptUpdate fail.\n"), encalgoText);
        EVP_CIPHER_CTX_free(ctx);
        return 1;
    }

    ret = EVP_EncryptFinal_ex(ctx, (unsigned char*)cipherText + SegCipherLength, &LastSegCipherLength);
    if (ret == 0) {
        fprintf(stderr, ("%s EVP_EncryptFinal_ex fail.\n"), encalgoText);
        EVP_CIPHER_CTX_free(ctx);
        return 1;
    }
    *cipherLength = SegCipherLength + LastSegCipherLength;
    EVP_CIPHER_CTX_free(ctx);
    return 0;
}

/*
 * Target		:Encrypt functions for security.
 * Description		:Encrypt with standard SM4 128 algorthm using openssl functions.
 * Notes		:The Key and iv here must be the same as it  was used in decrypt.
 * Input		:plainText	plain text need to be decrypted
 * 				plainLength	plain length
 * 				Key			user assigned key
 * 				iv   	 	IV for decrypt. General get from file or ciphertext
 * Output		:cipherText	ciphertext text after encrypted
 *				cipherLength		cipherText length
 *				fail 1, success 0
 */
unsigned long sm4_ctr_enc_partial_mode(const char* plainText, const size_t plainLength, char* cipherText,
    size_t* cipherLength, unsigned char* key, unsigned char* iv)
{
    const char* sm4AlgoText = "sm4";

    init_cipher_engine();
    const EVP_CIPHER* cipher = NULL;
    cipher = EVP_sm4_ctr();
    if (cipher == NULL) {
        fprintf(stderr, ("sm4 new cipher fail.\n"));
        return 1;
    }

    unsigned ret = ctr_enc_partial_mode(sm4AlgoText, cipher, plainText, plainLength, cipherText, cipherLength, key, iv);
    if (ret == 1) {
        fprintf(stderr, ("sm4 ctr_enc_partial_mode fail.\n"));
        return 1;
    }
    return 0;
}

/*
 * Target		:Decrypt functions for security.
 * Description	:Decrypt with standard sm4 128 algorthm using openssl functions.
 * Notes		:The Key and InitVector here must be the same as it  was used in encrypt.
 * Input		:CipherText	cipher text need to be decrypted
 * 				CipherLength	ciphertext length
 * 				key		user assigned key
 * 				iv		IV for decrypt. General get from file or ciphertext
 * Output		:plainText		plain text after decrypted
 *				plainLength		plaintext length
 *				fail 1, success 0
 */
unsigned long sm4_ctr_dec_partial_mode(const char* cipherText, const size_t cipherLength, char* plainText,
    size_t* plainLength, unsigned char* key, unsigned char* iv)
{
    const char* sm4AlgoText = "sm4";

    init_cipher_engine();
    const EVP_CIPHER* cipher = NULL;
    cipher = EVP_sm4_ctr();
    if (cipher == NULL) {
        fprintf(stderr, ("sm4 Cipher new fail"));
        return 1;
    }

    unsigned ret = ctr_dec_partial_mode(sm4AlgoText, cipher, cipherText, cipherLength, plainText, plainLength, key, iv);
    if (ret == 1) {
        fprintf(stderr, ("sm4 ctr_dec_partial_mode fail.\n"));
        return 1;
    }
    return 0;
}

/*
 * This encrypt code for AES CTR operation using data availability type as FULL
 * This code uses AES128 CTR and key is 16 bytes. For AES192 CTR, AES256 CTR, operations
 * are exactly same except key should be passed as 24 bytes & 32 bytes
 */
unsigned long aes_ctr_enc_partial_mode(const char* plainText, const size_t plainLength, char* cipherText,
    size_t* cipherLength, unsigned char* key, unsigned char* iv)
{
    const char* aesAlgoText = "aes";
    const EVP_CIPHER* cipher = EVP_aes_128_ctr();
    if (cipher == NULL) {
        fprintf(stderr, ("aes new cipher fail.\n"));
        return 1;
    }
    unsigned ret = ctr_enc_partial_mode(aesAlgoText, cipher, plainText, plainLength, cipherText, cipherLength, key, iv);
    if (ret == 1) {
        fprintf(stderr, ("aes ctr_enc_partial_mode fail.\n"));
        return 1;
    }
    return 0;
}

/*
 * This decrypt code for AES CTR operation using data availability type as FULL
 * This code uses AES128 CTR and key is 16 bytes. For AES192 CTR, AES256 CTR, operations
 * are exactly same except key should be passed as 24 bytes & 32 bytes
 */
unsigned long aes_ctr_dec_partial_mode(const char* cipherText, const size_t cipherLength, char* plainText,
    size_t* plainLength, unsigned char* key, unsigned char* iv)
{
    const char* aesAlgoText = "aes";
    const EVP_CIPHER* cipher = EVP_aes_128_ctr();
    if (cipher == NULL) {
        fprintf(stderr, ("aes new cipher fail.\n"));
        return 1;
    }

    unsigned ret = ctr_dec_partial_mode(aesAlgoText, cipher, cipherText, cipherLength, plainText, plainLength, key, iv);
    if (ret == 1) {
        fprintf(stderr, ("aes ctr_dec_partial_mode fail.\n"));
        return 1;
    }
    return 0;
}
