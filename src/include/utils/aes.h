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
 * aes.h
 *       AES encryption algorithm
 *       Interfaces of the AES encryption algorithm
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/aes.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef AES_H
#define AES_H

#include "c.h"
#include "cipher.h"

#define MAX_DECRYPT_BUFF_LEN 1024
#define AES_GROUP_LEN 16
#define AES_MIX_LEN 4
#define KEY_MAX_LEN 32
#define KEY_LEN 16

#define AES_TRANSFORM_LEN 2

#define AES_OUTPUT_LEN(inputStrLen) \
    (((inputStrLen + AES_GROUP_LEN - 1) / AES_GROUP_LEN) * AES_GROUP_LEN * AES_TRANSFORM_LEN + 1)
#define AES_ENCRYPT_LEN(inputlen) \
    ((inputlen % AES_GROUP_LEN) ? ((inputlen / AES_GROUP_LEN) * AES_GROUP_LEN + AES_GROUP_LEN) : inputlen)

typedef struct decrypt_struct {
    unsigned char* decryptBuff;

    char currLine[MAX_DECRYPT_BUFF_LEN];
    unsigned char Key[KEY_MAX_LEN];
    bool isCurrLineProcess;
    bool encryptInclude;

    /* Encrypt gs_dump file through OpenSSL function */
    bool randget;
    unsigned char rand[RANDOM_LEN + 1];
} DecryptInfo;

extern void initDecryptInfo(DecryptInfo* pDecryptInfo);
extern char* getLineFromAesEncryptFile(FILE* source, DecryptInfo* pDecryptInfo);
extern bool writeFileAfterEncryption(
    FILE* pf, char* inputstr, int inputstrlen, int writeBufflen, unsigned char Key[], unsigned char* rand);
extern bool check_key(const char* key, int NUM);
extern void aesEncrypt(char* inputstr, unsigned long inputstrlen, char* outputstr, unsigned char Key[]);
extern void aesDecrypt(char* inputstr, unsigned long inputstrlen, char* outputstr, unsigned char Key[], bool isBinary);
extern void init_aes_vector(GS_UCHAR* aes_vector);
extern bool init_aes_vector_random(GS_UCHAR* aes_vector, size_t vector_len);
extern bool aes128Encrypt(GS_UCHAR* PlainText, GS_UINT32 PlainLen, GS_UCHAR* Key, GS_UINT32 keylen, GS_UCHAR* RandSalt,
    GS_UCHAR* CipherText, GS_UINT32* CipherLen);
extern bool aes128EncryptSpeed(GS_UCHAR* PlainText, GS_UINT32 PlainLen, GS_UCHAR* Key, GS_UCHAR* RandSalt,
    GS_UCHAR* CipherText, GS_UINT32* CipherLen);
extern bool aes128Decrypt(GS_UCHAR* CipherText, GS_UINT32 CipherLen, GS_UCHAR* Key, GS_UINT32 keylen,
    GS_UCHAR* RandSalt, GS_UCHAR* PlainText, GS_UINT32* PlainLen);
extern bool aes128DecryptSpeed(GS_UCHAR* CipherText, GS_UINT32 CipherLen, GS_UCHAR* Key, GS_UCHAR* RandSalt,
    GS_UCHAR* PlainText, GS_UINT32* PlainLen);
extern GS_UINT32 aes_ctr_enc_full_mode(const char* plainText, const size_t plainLength, char* cipherText,
    size_t* cipherLength, GS_UCHAR* key, GS_UCHAR* iv);
extern GS_UINT32 aes_ctr_dec_full_mode(const char* cipherText, const size_t cipherLength, char* plainText,
    size_t* plainLength, GS_UCHAR* key, GS_UCHAR* iv);

#endif /* AES_H */
