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
 * ---------------------------------------------------------------------------------------
 * 
 * evp_cipher.h
 *      SM4 encryption algorithm
 *      Interfaces of the AES encryption algorithm
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/evp_cipher.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef EVP_CIPHER_H
#define EVP_CIPHER_H

#define KEY_128BIT_LEN  16
#define KEY_256BIT_LEN  32

/* To maintain forward compatibility, the value of enum cannot be changed */
typedef enum {
    TDE_ALGO_NONE = 0,
    TDE_ALGO_AES_128_CTR = 1,
    TDE_ALGO_AES_128_GCM = 2,
    TDE_ALGO_AES_256_CTR = 3,
    TDE_ALGO_AES_256_GCM = 4,
    TDE_ALGO_SM4_CTR = 5,
} TdeAlgo;

bool encrypt_partial_mode(const char* plainText, const size_t plainLength, char* cipherText,
    size_t* cipherLength, unsigned char* key, unsigned char* iv, TdeAlgo algo);
bool decrypt_partial_mode(const char* cipherText, const size_t cipherLength, char* plainText,
    size_t* plainLength, unsigned char* key, unsigned char* iv, TdeAlgo algo);

#endif /* EVP_CIPHER_H */
