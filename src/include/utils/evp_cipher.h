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

#ifndef SM4_H
#define SM4_H

#include <stdlib.h>

unsigned long sm4_ctr_enc_partial_mode(const char* plainText, const size_t plainLength, char* cipherText,
    size_t* cipherLength, unsigned char* key, unsigned char* iv);
unsigned long sm4_ctr_dec_partial_mode(const char* cipherText, const size_t cipherLength, char* plainText,
    size_t* plainLength, unsigned char* key, unsigned char* iv);
unsigned long aes_ctr_enc_partial_mode(const char* plainText, const size_t plainLength, char* cipherText,
    size_t* cipherLength, unsigned char* key, unsigned char* iv);
unsigned long aes_ctr_dec_partial_mode(const char* cipherText, const size_t cipherLength, char* plainText,
    size_t* plainLength, unsigned char* key, unsigned char* iv);

#endif /* SM4_H */
