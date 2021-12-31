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
 * cipher.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/cipher.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CIPHER_H
#define CIPHER_H

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "c.h"

#include "utils/pg_crc.h"
#include "openssl/ssl.h"
#include "openssl/tls1.h"
#include "openssl/ossl_typ.h"
#include "openssl/obj_mac.h"

#ifdef WIN32
typedef unsigned __int64 GS_UINT64;
#else
typedef unsigned long long GS_UINT64;
#endif
#if defined(__LP64__) || defined(__64BIT__)
typedef unsigned int GS_UINT32;

typedef signed int GS_INT32;
#else
typedef unsigned long GS_UINT32;

typedef signed long GS_INT32;
#endif

typedef unsigned char GS_UCHAR;

#define CIPHER_KEY_FILE ".key.cipher"
#define RAN_KEY_FILE ".key.rand"
#define RANDOM_LEN 16
#define CIPHER_LEN 16
#define DEK_LEN 16
#define ITERATE_TIMES 10000
#define MAC_ITERATE_TIMES 10000
#define MAC_LEN 20
#define MIN_KEY_LEN 8
#define MAX_KEY_LEN 16
#define AK_LEN 512
#define SK_LEN 512
#define AK_VALID_CHRS "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
#define SK_VALID_CHRS AK_VALID_CHRS
#define DEK_SAMPLE_STRING "TRANS_ENCRYPT_SAMPLE_STRING"

#define SM4_KEY_LENGTH  16
#define SM4_BLOCK_SIZE  16
#define SM4_IV_LENGTH   SM4_BLOCK_SIZE
#define SM4_WORD_SIZE  (SM4_BLOCK_SIZE / sizeof(GS_UINT32))

typedef enum {
    UNKNOWN_KEY_MODE,
    SERVER_MODE,
    CLIENT_MODE,
    HADR_MODE,
    OBS_MODE,
    SOURCE_MODE,
    GDS_MODE,
    USER_MAPPING_MODE,
    SUBSCRIPTION_MODE
} KeyMode;

typedef struct {
    GS_UCHAR cipherkey[CIPHER_LEN + 1];   /* cipher text vector */
    GS_UCHAR key_salt[RANDOM_LEN + 1];    /* salt vector used to derive key */
    GS_UCHAR vector_salt[RANDOM_LEN + 1]; /* salt vector used to encrypt/decrypt text */
    pg_crc32 crc;
} CipherkeyFile;

typedef struct {
    GS_UCHAR randkey[CIPHER_LEN + 1];
    pg_crc32 crc;
} RandkeyFile;

typedef enum {
    OBS_CLOUD_TYPE = 0,     /* on cloud obs cipher for encrypt and decrypt ak/sk */
    INITDB_NOCLOUDOBS_TYPE, /* non-cloud obs use the cipher same as initdb */
    GSQL_SSL_TYPE,          /* gsql ssl connection cipher */
    GDS_SSL_TYPE,           /* gds ssl connection cipher */
    CIPHER_TYPE_MAX         /* The max number of types should be at the end */
} CipherType;

extern void gen_cipher_rand_files(
    KeyMode mode, const char* plain_key, const char* user_name, const char* datadir, const char* preStr);
extern void decode_cipher_files(
    KeyMode mode, const char* user_name, const char* datadir, GS_UCHAR* plainpwd, bool obs_server_mode = false);
extern bool check_input_password(const char* password);
extern bool EncryptInputKey(GS_UCHAR* pucPlainText, GS_UCHAR* initrand, GS_UCHAR* keySaltVector,
    GS_UCHAR* encryptVector, GS_UCHAR* pucCipherText, GS_UINT32* pulCLen);
extern bool ReadContentFromFile(const char* filename, void* content, size_t csize);
extern bool check_certificate_signature_algrithm(const SSL_CTX* SSL_context);
extern long check_certificate_time(const SSL_CTX* SSL_context, const int alarm_days);
extern bool CipherFileIsValid(CipherkeyFile* cipher);
extern bool RandFileIsValid(RandkeyFile* randfile);
extern void ClearCipherKeyFile(CipherkeyFile* cipher_file_content);
extern void ClearRandKeyFile(RandkeyFile* rand_file_content);
extern bool DecryptInputKey(GS_UCHAR* pucCipherText, GS_UINT32 ulCLen, GS_UCHAR* initrand, GS_UCHAR* initVector,
    GS_UCHAR* decryptVector, GS_UCHAR* pucPlainText, GS_UINT32* pulPLen);
extern bool getKeyVectorFromCipherFile(const char* cipherkeyfile, const char* cipherrndfile, GS_UCHAR* key, GS_UCHAR* vector);
extern bool encryptStringByAES128Speed(GS_UCHAR* PlainText, GS_UINT32 PlainLen, GS_UCHAR* Key, GS_UCHAR* RandSalt,
    GS_UCHAR* CipherText, GS_UINT32* CipherLen);
extern char* getGaussHome();
extern char* SEC_decodeBase64(const char* pucInBuf, GS_UINT32* pulOutBufLen);
extern char* SEC_encodeBase64(const char* pucInBuf, GS_UINT32 ulInBufLen);
extern GS_UINT32 CRYPT_encrypt(GS_UINT32 ulAlgId, const GS_UCHAR* pucKey, GS_UINT32 ulKeyLen, const GS_UCHAR* pucIV,
    GS_UINT32 ulIVLen, GS_UCHAR* pucPlainText, GS_UINT32 ulPlainLen, GS_UCHAR* pucCipherText, GS_UINT32* pulCLen);
extern GS_UINT32 CRYPT_decrypt(GS_UINT32 ulAlgId, const GS_UCHAR* pucKey, GS_UINT32 ulKeyLen, const GS_UCHAR* pucIV,
    GS_UINT32 ulIVLen, GS_UCHAR* pucCipherText, GS_UINT32 ulCLen, GS_UCHAR* pucPlainText, GS_UINT32* pulPLen);
extern GS_UINT32 CRYPT_hmac(GS_UINT32 ulAlgType, const GS_UCHAR* pucKey, GS_UINT32 upucKeyLen, const GS_UCHAR* pucData,
    GS_UINT32 ulDataLen, GS_UCHAR* pucDigest, GS_UINT32* pulDigestLen);
const EVP_MD* get_evp_md_by_id(GS_UINT32 ulAlgType);

#endif
