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
 * cipher.cpp
 *
 * IDENTIFICATION
 *    src/common/port/cipher.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cipher.h"
#include "securec.h"
#include "securec_check.h"
#include "utils/syscall_lock.h"
#include "utils/pg_crc_tables.h"
#include "openssl/rand.h"
#include "openssl/evp.h"
#include "openssl/ossl_typ.h"
#include "openssl/x509.h"
#include "openssl/ssl.h"
#include "openssl/asn1.h"
#include "openssl/hmac.h"

#define ENTROPY_F_LEN 128
#define NONCE_F_LEN 128

#ifndef CACHELINE_SIZE
#define CACHELINE_SIZE 32
#endif

#ifndef L1CACHE_SIZE
#define L1CACHE_SIZE (64 * 1024)
#endif

/* Store entropy message of the seed which used by random function. */
typedef struct {
    uint64 data;
    uint32 prev_time;
    uint32 last_delta;
    int32 last_delta2;
    uint32 n;
    double sum;
} entropy_t;

static bool gen_cipher_file(KeyMode mode, GS_UCHAR* init_rand, GS_UCHAR server_vector[], GS_UCHAR client_vector[],
    const char* plain_key, const char* user_name, const char* datadir, const char* preStr);
static bool gen_rand_file(
    KeyMode mode, GS_UCHAR* init_rand, const char* user_name, const char* datadir, const char* preStr);
static bool ReadKeyContentFromFile(KeyMode mode, const char* cipherkeyfile, const char* randfile,
    CipherkeyFile* cipher_file_content, RandkeyFile* rand_file_content, bool obsnormal_or_initdb, bool must_formfile);
static bool WriteContentToFile(const char* filename, const void* content, size_t csize);
static bool IsSpecialCharacter(char ch);
static bool isModeExists(KeyMode mode);
void ClearCipherKeyFile(CipherkeyFile* cipher_file_content);
void ClearRandKeyFile(RandkeyFile* rand_file_content);
bool init_vector_random(GS_UCHAR* init_vector, size_t vector_len);

GS_UINT32 CRYPT_decrypt(GS_UINT32 ulAlgId, const GS_UCHAR* pucKey, GS_UINT32 ulKeyLen, const GS_UCHAR* pucIV,
    GS_UINT32 ulIVLen, GS_UCHAR* pucCipherText, GS_UINT32 ulCLen, GS_UCHAR* pucPlainText, GS_UINT32* pulPLen);
GS_UINT32 CRYPT_encrypt(GS_UINT32 ulAlgId, const GS_UCHAR* pucKey, GS_UINT32 ulKeyLen, const GS_UCHAR* pucIV,
    GS_UINT32 ulIVLen, GS_UCHAR* pucPlainText, GS_UINT32 ulPlainLen, GS_UCHAR* pucCipherText, GS_UINT32* pulCLen);
GS_UINT32 CRYPT_hmac(GS_UINT32 ulAlgType, const GS_UCHAR* pucKey, GS_UINT32 upucKeyLen, const GS_UCHAR* pucData,
    GS_UINT32 ulDataLen, GS_UCHAR* pucDigest, GS_UINT32* pulDigestLen);
char* SEC_decodeBase64(const char* pucInBuf, GS_UINT32* pulOutBufLen);
char* SEC_encodeBase64(const char* pucInBuf, GS_UINT32 ulInBufLen);

/* for stored cipherfile buffer */
RandkeyFile g_rand_file_content[CIPHER_TYPE_MAX];
CipherkeyFile g_cipher_file_content[CIPHER_TYPE_MAX];

bool init_vector_random(GS_UCHAR* init_vector, size_t vector_len)
{
    errno_t errorno = EOK;
    int retval = 0;
    GS_UCHAR random_vector[RANDOM_LEN] = {0};

    retval = RAND_priv_bytes(random_vector, RANDOM_LEN);
    if (retval != 1) {
        errorno = memset_s(random_vector, RANDOM_LEN, '\0', RANDOM_LEN);
        securec_check_c(errorno, "", "");
        (void)fprintf(stderr, _("generate random initial vector failed, errcode:%d\n"), retval);
        return false;
    }

    errorno = memcpy_s(init_vector, vector_len, random_vector, RANDOM_LEN);
    securec_check_c(errorno, "", "");
    errorno = memset_s(random_vector, RANDOM_LEN, '\0', RANDOM_LEN);
    securec_check_c(errorno, "", "");
    return true;
}

/* check whether the input password(for key derivation) meet the requirements of the length and complexity */
bool check_input_password(const char* password)
{
#define PASSWD_KINDS 4
    int key_input_len = 0;
    int kinds[PASSWD_KINDS] = {0};
    int kinds_num = 0;
    const char* ptr = NULL;
    int i = 0;
    if (password == NULL) {
        (void)fprintf(stderr, _("Invalid password,please check it\n"));
        return false;
    }
    key_input_len = strlen(password);
    if (key_input_len < MIN_KEY_LEN) {
        (void)fprintf(stderr, _("Invalid password,it must contain at least eight characters\n"));
        return false;
    }
    if (key_input_len > MAX_KEY_LEN) {
        (void)fprintf(stderr, _("Invalid password,the length exceed %d\n"), MAX_KEY_LEN);
        return false;
    }
    ptr = password;
    while (*ptr != '\0') {
        if (*ptr >= 'A' && *ptr <= 'Z') {
            kinds[0]++;
        } else if (*ptr >= 'a' && *ptr <= 'z') {
            kinds[1]++;
        } else if (*ptr >= '0' && *ptr <= '9') {
            kinds[2]++;
        } else if (IsSpecialCharacter(*ptr)) {
            kinds[3]++;
        }
        ptr++;
    }
    for (i = 0; i < PASSWD_KINDS; ++i) {
        if (kinds[i] > 0) {
            kinds_num++;
        }
    }
    if (kinds_num < PASSWD_KINDS - 1) {
        (void)fprintf(stderr, _("Invalid password,it must contain at least three kinds of characters\n"));
        return false;
    }
    return true;
}

/* encrypt the plain text to cipher text */
bool EncryptInputKey(GS_UCHAR* pucPlainText, GS_UCHAR* initrand, GS_UCHAR* keySaltVector, GS_UCHAR* encryptVector,
    GS_UCHAR* pucCipherText, GS_UINT32* pulCLen)
{
    GS_UCHAR deriver_key[RANDOM_LEN] = {0};
    GS_UINT32 retval = 0;
    GS_UINT32 ulPlainLen = 0;
    errno_t rc = EOK;

    if (pucPlainText == NULL) {
        (void)fprintf(stderr, _("invalid plain text, please check it!\n"));
        return false;
    }
    ulPlainLen = strlen((const char*)pucPlainText);

    /* use PKCS5 HMAC sha256 to dump the key for encryption */
    retval = PKCS5_PBKDF2_HMAC((const char*)initrand,
        RANDOM_LEN,
        keySaltVector,
        RANDOM_LEN,
        ITERATE_TIMES,
        EVP_sha256(),
        RANDOM_LEN,
        deriver_key);
    if (retval != 1) {
        rc = memset_s(deriver_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(rc, "\0", "\0");
        (void)fprintf(stderr, _("generate the derived key failed, errcode:%u\n"), retval);
        return false;
    }

    retval = CRYPT_encrypt(NID_aes_128_cbc,
        deriver_key,
        RANDOM_LEN,
        encryptVector,
        RANDOM_LEN,
        pucPlainText,
        ulPlainLen,
        pucCipherText,
        pulCLen);
    if (retval != 0) {
        rc = memset_s(deriver_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(rc, "\0", "\0");
        (void)fprintf(stderr, _("encrypt plain text to cipher text failed, errcode:%u\n"), retval);
        return false;
    }

    rc = memset_s(deriver_key, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check_c(rc, "\0", "\0");
    return true;
}

/* decrypt the cipher text to plain text */
bool DecryptInputKey(GS_UCHAR* pucCipherText, GS_UINT32 ulCLen, GS_UCHAR* initrand, GS_UCHAR* initVector,
    GS_UCHAR* decryptVector, GS_UCHAR* pucPlainText, GS_UINT32* pulPLen)
{
    GS_UINT32 retval = 0;
    GS_UCHAR decrypt_key[RANDOM_LEN] = {0};
    errno_t rc = EOK;

    if (pucCipherText == NULL) {
        (void)fprintf(stderr, _("invalid cipher text, please check it!\n"));
        return false;
    }

    /* get the decrypt key value */
    retval = PKCS5_PBKDF2_HMAC((const char*)initrand,
        RANDOM_LEN,
        initVector,
        RANDOM_LEN,
        ITERATE_TIMES,
        EVP_sha256(),
        RANDOM_LEN,
        decrypt_key);
    if (retval != 1) {
        rc = memset_s(decrypt_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(rc, "\0", "\0");
        (void)fprintf(stderr, _("generate the derived key failed, errcode:%u\n"), retval);
        return false;
    }

    /*decrypt the cipher*/
    retval = CRYPT_decrypt(NID_aes_128_cbc,
        decrypt_key,
        RANDOM_LEN,
        decryptVector,
        RANDOM_LEN,
        pucCipherText,
        ulCLen,
        pucPlainText,
        pulPLen);

    if (retval != 0) {
        rc = memset_s(decrypt_key, RANDOM_LEN, 0, RANDOM_LEN);
        securec_check_c(rc, "\0", "\0");
        (void)fprintf(stderr, _("decrypt cipher text to plain text failed, errcode:%u\n"), retval);
        return false;
    }

    rc = memset_s(decrypt_key, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check_c(rc, "\0", "\0");
    return true;
}

/* copy the cipher text to CipherkeyFile */
static void CopyCipher(GS_UCHAR* cipher_str, /* points to cipher string */
    GS_UINT32 cipher_len,                    /* cipher string length */
    GS_UCHAR* key_salt,                      /* salt vector used to derive key */
    GS_UCHAR* vector_salt,                   /* salt vector used to encrypt plaintext */
    CipherkeyFile* content)                  /* file content buffer, be careful caller make sure it's not null */
{
    errno_t rc = EOK;

    rc = memcpy_s(content->cipherkey, CIPHER_LEN + 1, cipher_str, CIPHER_LEN);
    securec_check_c(rc, "\0", "\0");

    rc = memcpy_s(content->key_salt, RANDOM_LEN + 1, key_salt, RANDOM_LEN);
    securec_check_c(rc, "\0", "\0");

    rc = memcpy_s(content->vector_salt, RANDOM_LEN + 1, vector_salt, RANDOM_LEN);
    securec_check_c(rc, "\0", "\0");

    /* generate the crc value to protect the value in case someone modify it */
    INIT_CRC32(content->crc);
    COMP_CRC32(content->crc, (char*)content, offsetof(CipherkeyFile, crc));
    FIN_CRC32(content->crc);
}

/* copy the cipher text to RandkeyFile */
static void CopyRand(GS_UCHAR* rand_str, RandkeyFile* randfile)
{
    errno_t rc = EOK;

    /* append rand_key to the front part of cipher text */
    rc = memcpy_s(randfile->randkey, RANDOM_LEN + 1, rand_str, RANDOM_LEN);
    securec_check_c(rc, "\0", "\0");

    /* generate the crc value to protect the value in case someone modify it */
    INIT_CRC32(randfile->crc);
    COMP_CRC32(randfile->crc, (char*)randfile, offsetof(RandkeyFile, crc));
    FIN_CRC32(randfile->crc);
}

/* check the crc of rand file storing the randtext */
bool RandFileIsValid(RandkeyFile* randfile)
{
    pg_crc32 rand_crc;

    INIT_CRC32(rand_crc);
    COMP_CRC32(rand_crc, (char*)randfile, offsetof(RandkeyFile, crc));

    FIN_CRC32(rand_crc);

    if (!EQ_CRC32(randfile->crc, rand_crc)) {
        (void)fprintf(stderr, _("CRC checksum does not match value stored in file, maybe the rand file is corrupt\n"));
        return false;
    }

    return true;
}

/* check the crc of cipher file storing the ciphertext */
bool CipherFileIsValid(CipherkeyFile* cipher)
{
    pg_crc32 cipher_crc;

    INIT_CRC32(cipher_crc);
    COMP_CRC32(cipher_crc, (char*)cipher, offsetof(CipherkeyFile, crc));

    FIN_CRC32(cipher_crc);

    if (!EQ_CRC32(cipher->crc, cipher_crc)) {
        (void)fprintf(
            stderr, _("CRC checksum does not match value stored in file, maybe the cipher file is corrupt\n"));
        return false;
    }

    return true;
}

/* Read random and cipher contents of the file to buffer */
static bool ReadKeyContentFromFile(KeyMode mode, const char* cipherkeyfile, const char* randfile,
    CipherkeyFile* cipher_file_content, RandkeyFile* rand_file_content, bool obsnormal_or_initdb, bool must_formfile)
{
    errno_t rc = EOK;
    RandkeyFile* global_rand_file = NULL;
    CipherkeyFile* global_cipher_file = NULL;

    if (mode == OBS_MODE) {
        global_rand_file = &g_rand_file_content[OBS_CLOUD_TYPE];
        global_cipher_file = &g_cipher_file_content[OBS_CLOUD_TYPE];
    } else if (obsnormal_or_initdb) {
        /* Note: Data Source use initdb key file by default (datasource.key.* not given) */
        global_rand_file = &g_rand_file_content[INITDB_NOCLOUDOBS_TYPE];
        global_cipher_file = &g_cipher_file_content[INITDB_NOCLOUDOBS_TYPE];
    } else if (mode == SOURCE_MODE || mode == HADR_MODE|| mode == USER_MAPPING_MODE || mode == SUBSCRIPTION_MODE) {
        /*
         * For Data Source:
         * read key from file (datasource.key.*): we do not cache these keys here
         */
        (void)syscalllockAcquire(&read_cipher_lock);
        if (!ReadContentFromFile(cipherkeyfile, cipher_file_content, sizeof(CipherkeyFile)) ||
            !ReadContentFromFile(randfile, rand_file_content, sizeof(RandkeyFile))) {
            (void)fprintf(stderr, _("read data source cipher file or random parameter file failed.\n"));
            (void)syscalllockRelease(&read_cipher_lock);
            return false;
        }
        if (!CipherFileIsValid(cipher_file_content) || !RandFileIsValid(rand_file_content)) {
            (void)fprintf(stderr, _("data source cipher file or random parameter file is invalid.\n"));
            (void)syscalllockRelease(&read_cipher_lock);
            return false;
        }
        (void)syscalllockRelease(&read_cipher_lock);
        return true;
    } else if (mode == GDS_MODE) {
        global_rand_file = &g_rand_file_content[GDS_SSL_TYPE];
        global_cipher_file = &g_cipher_file_content[GDS_SSL_TYPE];
    } else {
        global_rand_file = &g_rand_file_content[GSQL_SSL_TYPE];
        global_cipher_file = &g_cipher_file_content[GSQL_SSL_TYPE];
    }

    (void)syscalllockAcquire(&read_cipher_lock);

    /* needn't read cipher file if content is saved in buffer */
    if (global_cipher_file->cipherkey[0] != 0 && global_rand_file->randkey[0] != 0 && !must_formfile) {
        rc = memcpy_s(cipher_file_content, sizeof(CipherkeyFile), global_cipher_file, sizeof(CipherkeyFile));
        securec_check_c(rc, "\0", "\0");
        rc = memcpy_s(rand_file_content, sizeof(RandkeyFile), global_rand_file, sizeof(RandkeyFile));
        securec_check_c(rc, "\0", "\0");
        (void)syscalllockRelease(&read_cipher_lock);
        return true;
    } else {
        if (!ReadContentFromFile(cipherkeyfile, cipher_file_content, sizeof(CipherkeyFile)) ||
            !ReadContentFromFile(randfile, rand_file_content, sizeof(RandkeyFile))) {
            (void)fprintf(stderr, _("read cipher file or random parameter from obs file failed.\n"));
            (void)syscalllockRelease(&read_cipher_lock);
            return false;
        }

        if (!CipherFileIsValid(cipher_file_content) || !RandFileIsValid(rand_file_content)) {
            (void)fprintf(stderr, _("non obs cipher file or random parameter file is invalid.\n"));
            (void)syscalllockRelease(&read_cipher_lock);
            return false;
        }
        rc = memcpy_s(global_cipher_file, sizeof(CipherkeyFile), cipher_file_content, sizeof(CipherkeyFile));
        securec_check_c(rc, "\0", "\0");
        rc = memcpy_s(global_rand_file, sizeof(RandkeyFile), rand_file_content, sizeof(RandkeyFile));
        securec_check_c(rc, "\0", "\0");
        (void)syscalllockRelease(&read_cipher_lock);
        return true;
    }
}

/* Read the contents of the file to buffer */
bool ReadContentFromFile(const char* filename, void* content, size_t csize)
{
    FILE* pfRead = NULL;
    int cnt = 0;
    /*open and read file*/
    if ((pfRead = fopen(filename, "rb")) == NULL) {
        (void)fprintf(stderr, _("could not open file \"%s\": %s\n"), filename, gs_strerror(errno));
        return false;
    }
    cnt = fread(content, csize, 1, pfRead);
    if (cnt < 0) {
        fclose(pfRead);
        (void)fprintf(stderr, _("could not read file \"%s\": %s\n"), filename, gs_strerror(errno));
        return false;
    }
    if (fclose(pfRead)) {
        (void)fprintf(stderr, _("could not close file \"%s\": %s\n"), filename, gs_strerror(errno));
        return false;
    }

    return true;
}

/* write data in buffer to file */
static bool WriteContentToFile(const char* filename, const void* content, size_t csize)
{
    FILE* pfWrite = NULL;
    int ret = 0;

    /*open and write file*/
    if ((pfWrite = fopen(filename, "wb")) == NULL) {
        (void)fprintf(stderr, _("could not open file \"%s\" for writing: %s\n"), filename, gs_strerror(errno));
        return false;
    }
    if (fwrite(content, csize, 1, pfWrite) != 1) {
        fclose(pfWrite);
        (void)fprintf(stderr, _("could not write file \"%s\": %s\n"), filename, gs_strerror(errno));
        return false;
    }

#ifdef WIN32
    ret = _chmod(filename, 0400);
#else
    ret = fchmod(pfWrite->_fileno, 0400);
#endif

    if (fclose(pfWrite)) {
        (void)fprintf(stderr, _("could not close file \"%s\": %s\n"), filename, gs_strerror(errno));
        return false;
    }

    if (-1 == ret) {
        (void)fprintf(stderr, _("could not set permissions of file \"%s\": %s\n"), filename, gs_strerror(errno));
        return false;
    }
    return true;
}

/* Judge if the KeyMode is legal */
static bool isModeExists(KeyMode mode)
{
    if (mode != SERVER_MODE && mode != CLIENT_MODE && mode != HADR_MODE &&
        mode != OBS_MODE && mode != SOURCE_MODE && mode != GDS_MODE &&
        mode != USER_MAPPING_MODE && mode != SUBSCRIPTION_MODE) {
#ifndef ENABLE_LLT
        (void)fprintf(stderr, _("AK/SK encrypt/decrypt encounters invalid key mode.\n"));
        return false;
#endif
    }
    return true;
}

/*
 * @Description : reset cipher file content.
 * @in cipher_file_context : the file need to be cleared.
 * @return		: void.
 */
void ClearCipherKeyFile(CipherkeyFile* cipher_file_content)
{
    if (cipher_file_content != NULL) {
        errno_t rc = EOK;
        rc = memset_s((char*)(cipher_file_content->cipherkey), CIPHER_LEN + 1, 0, CIPHER_LEN + 1);
        securec_check_c(rc, "", "");
        rc = memset_s((char*)(cipher_file_content->key_salt), RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
        securec_check_c(rc, "", "");
        rc = memset_s((char*)(cipher_file_content->vector_salt), RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
        securec_check_c(rc, "", "");
    }

    return;
}

/*
 * @Description : reset rand file content.
 * @in rand_file_context : the file need to be cleared.
 * @return		: void.
 */
void ClearRandKeyFile(RandkeyFile* rand_file_content)
{
    if (rand_file_content != NULL) {
        errno_t rc = EOK;
        rc = memset_s((char*)(rand_file_content->randkey), CIPHER_LEN + 1, 0, CIPHER_LEN + 1);
        securec_check_c(rc, "", "");
    }

    return;
}

/* encrypt the input key,and write the cipher to file */
static bool gen_cipher_file(KeyMode mode, /* SERVER_MODE or CLIENT_MODE or OBS_MODE or SOURCE_MODE */
    GS_UCHAR* init_rand, GS_UCHAR server_vector[], GS_UCHAR client_vector[], const char* plain_key,
    const char* user_name, const char* datadir, const char* preStr)
{
    int ret = 0;

    char cipherkeyfile[MAXPGPATH] = {0x00};
    GS_UCHAR encrypt_rand[RANDOM_LEN] = {0};
    GS_UCHAR ciphertext[CIPHER_LEN] = {0};
    GS_UCHAR* key_salt = NULL;

    GS_UINT32 cipherlen = 0;
    int retval = 0;

    CipherkeyFile cipher_file_content;

    /* check whether the key mode is valid */
    if (!isModeExists(mode)) {
#ifndef ENABLE_LLT
        goto RETURNFALSE;
#endif
    }

    /* generate init rand key */
    retval = RAND_priv_bytes(encrypt_rand, RANDOM_LEN);
    if (retval != 1) {
#ifndef ENABLE_LLT
        (void)fprintf(stderr, _("generate random key failed,errcode:%d\n"), retval);
        goto RETURNFALSE;
#endif
    }

    if (mode == SERVER_MODE) /* in server_mode,write the file with name like: server% */
    {
        ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/server%s", datadir, CIPHER_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
        key_salt = server_vector;
    } else if (mode == OBS_MODE) /* in OBS_mode,write the file with name like: obsserver% */
    {
        if (NULL == preStr) {
            ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/obsserver%s", datadir, CIPHER_KEY_FILE);
        } else {
            ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/%s%s", datadir, preStr, CIPHER_KEY_FILE);
        }
        securec_check_ss_c(ret, "\0", "\0");
        key_salt = server_vector;
    } else if (mode == SOURCE_MODE) /* in SOURCE_MODE,write the file with name like: datasource% */
    {
        ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/datasource%s", datadir, CIPHER_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
        key_salt = server_vector;
    }
    /*in client_mode,check with the user name is appointed.if so, write the files naming
                                    with %user_name%,if not,write files naming with client%*/
    else if (mode == CLIENT_MODE) {
        if (NULL == user_name) {
            ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/client%s", datadir, CIPHER_KEY_FILE);
            securec_check_ss_c(ret, "\0", "\0");
        } else {
            ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/%s%s", datadir, user_name, CIPHER_KEY_FILE);
            securec_check_ss_c(ret, "\0", "\0");
        }
        key_salt = client_vector;
    } else {
        return false;
    }

    if (!EncryptInputKey((GS_UCHAR*)plain_key, init_rand, key_salt, encrypt_rand, ciphertext, &cipherlen)) {
#ifndef ENABLE_LLT
        goto RETURNFALSE;
#endif
    }

    /*
     * Write ciphertext and encrypt rand vector to cipher_file_content
     * and generate cipher_file_context's CRC and append to the end of
     * cipher_file_context.
     */
    CopyCipher(ciphertext, cipherlen, key_salt, encrypt_rand, &cipher_file_content);

    if (!WriteContentToFile(cipherkeyfile, (const void*)&cipher_file_content, sizeof(CipherkeyFile))) {
#ifndef ENABLE_LLT
        goto RETURNFALSE;
#endif
    }

    /*
     * Change the privileges: include read & write
     * Note: it should be checked by OM tool: gs_ec.
     */
    if (mode == SOURCE_MODE || mode == CLIENT_MODE || mode == SERVER_MODE || mode == OBS_MODE) {
#ifdef WIN32
        ret = _chmod(cipherkeyfile, 0600);
#else
        ret = chmod(cipherkeyfile, 0600);
#endif
        if (ret != 0) {
#ifndef ENABLE_LLT
            (void)fprintf(
                stderr, _("could not set permissions of file \"%s\": %s\n"), cipherkeyfile, gs_strerror(errno));
            goto RETURNFALSE;
#endif
        }
    }

    /*
     * Empty ciphertext and cipher_file_content.
     * This is useful. Although ciphertext and cipher_file_content is in stack,
     * we should manually clear them.
     */
    ret = memset_s(ciphertext, CIPHER_LEN, 0, CIPHER_LEN);
    securec_check_c(ret, "\0", "\0");
    ret = memset_s((char*)&cipher_file_content, sizeof(CipherkeyFile), 0, sizeof(CipherkeyFile));
    securec_check_c(ret, "\0", "\0");

    return true;

#ifndef ENABLE_LLT
RETURNFALSE:
    /*
     * Empty ciphertext and cipher_file_content.
     * This is useful. Although ciphertext and cipher_file_content is in stack,
     * we should manually clear them.
     */
    ret = memset_s(ciphertext, CIPHER_LEN, 0, CIPHER_LEN);
    securec_check_c(ret, "\0", "\0");
    ret = memset_s((void*)&cipher_file_content, sizeof(CipherkeyFile), 0, sizeof(CipherkeyFile));
    securec_check_c(ret, "\0", "\0");

    return false;
#endif
}

/* write encryption factor to files */
static bool gen_rand_file(
    KeyMode mode, GS_UCHAR* init_rand, const char* user_name, const char* datadir, const char* preStr)
{
    int ret;
    char randfile[MAXPGPATH] = {0x00};
    RandkeyFile rand_file_content;
    FILE* pfWrite = NULL;

    /*check whether the key mode is valid*/
    if (!isModeExists(mode)) {
#ifndef ENABLE_LLT
        goto RETURNFALSE;
#endif
    }

    /*in server_mode,write the files naming with server%*/
    if (mode == SERVER_MODE) {
        ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/server%s", datadir, RAN_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
    } else if (mode == OBS_MODE) {
        if (preStr == NULL) {
            ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/obsserver%s", datadir, RAN_KEY_FILE);
        } else {
            ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/%s%s", datadir, preStr, RAN_KEY_FILE);
        }
        securec_check_ss_c(ret, "\0", "\0");
    } else if (mode == SOURCE_MODE) {
        ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/datasource%s", datadir, RAN_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
    }
    /*in client_mode,check with the user name is appointed.if so, write the files naming
    with %user_name%,if not,write files naming with client%*/
    else if (mode == CLIENT_MODE) {
        if (user_name == NULL) {
            ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/client%s", datadir, RAN_KEY_FILE);
            securec_check_ss_c(ret, "\0", "\0");
        } else {
            ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/%s%s", datadir, user_name, RAN_KEY_FILE);
            securec_check_ss_c(ret, "\0", "\0");
        }
    }
    CopyRand(init_rand, &rand_file_content);
    if (!WriteContentToFile(randfile, (const void*)&rand_file_content, sizeof(RandkeyFile))) {
#ifndef ENABLE_LLT
        goto RETURNFALSE;
#endif
    }

    /*
     * Change the privileges: include read & write
     * Note: it should be checked by OM tool: gs_ec.
     */
    if (mode == SOURCE_MODE || mode == CLIENT_MODE || mode == SERVER_MODE || mode == OBS_MODE) {
        /*open and write file*/
        if ((pfWrite = fopen(randfile, "r")) == NULL) {
            (void)fprintf(stderr, _("could not open file \"%s\" for writing: %s\n"), randfile, gs_strerror(errno));
            return false;
        }

#ifdef WIN32
        ret = _chmod(randfile, 0600);
#else
        ret = fchmod(pfWrite->_fileno, 0600);
#endif

        if (fclose(pfWrite)) {
            (void)fprintf(stderr, _("could not close file \"%s\": %s\n"), randfile, gs_strerror(errno));
            return false;
        }

        if (ret != 0) {
            (void)fprintf(stderr, _("could not set permissions of file \"%s\": %s\n"), randfile, gs_strerror(errno));
            return false;
        }
    }

    /*
     * Empty rand_file_content.
     * This is useful. Although rand_file_content is in stack,
     * we should manually clear it.
     */
    ret = memset_s((void*)&rand_file_content, sizeof(RandkeyFile), 0, sizeof(RandkeyFile));
    securec_check_c(ret, "\0", "\0");

    return true;

#ifndef ENABLE_LLT
RETURNFALSE:
    /*
     * Empty rand_file_content.
     * This is useful. Although rand_file_content is in stack,
     * we should manually clear it.
     */
    ret = memset_s((void*)&rand_file_content, sizeof(RandkeyFile), 0, sizeof(RandkeyFile));
    securec_check_c(ret, "\0", "\0");

    return false;
#endif
}

/*
 * generate the files of cipher text and encryption factor
 *     Notice: preStr is only used by OBS
 */
void gen_cipher_rand_files(
    KeyMode mode, const char* plain_key, const char* user_name, const char* datadir, const char* preStr)
{
    int retval = 0;
    GS_UCHAR init_rand[RANDOM_LEN] = {0};
    GS_UCHAR server_vector[RANDOM_LEN] = {'\0'};
    GS_UCHAR client_vector[RANDOM_LEN] = {'\0'};

    retval = RAND_priv_bytes(init_rand, RANDOM_LEN);
    if (retval != 1) {
        (void)fprintf(stderr, _("generate random key failed,errcode:%d\n"), retval);
        return;
    }

    if (mode == SERVER_MODE || mode == SOURCE_MODE || mode == OBS_MODE) {
        init_vector_random(server_vector, RANDOM_LEN);
    } else if (mode == CLIENT_MODE) {
        init_vector_random(client_vector, RANDOM_LEN);
    } else {
        (void)fprintf(stderr, _("generate cipher file failed, unknown mode:%d.\n"), mode);
        return;
    }

    if (!gen_cipher_file(mode, init_rand, server_vector, client_vector, plain_key, user_name, datadir, preStr)) {
#ifndef ENABLE_LLT
        (void)fprintf(stderr, _("generate cipher file failed.\n"));
        return;
#endif
    }
    if (!gen_rand_file(mode, init_rand, user_name, datadir, preStr)) {
#ifndef ENABLE_LLT
        (void)fprintf(stderr, _("generate random parameter file failed.\n"));
        return;
#endif
    }
}

/* decrypt the cipher text to plain text via the stored files */
void decode_cipher_files(
    KeyMode mode, const char* user_name, const char* datadir, GS_UCHAR* plainpwd, bool obsnormal_or_initdb)
{
    GS_UINT32 plainlen = 0;
    char cipherkeyfile[MAXPGPATH] = {0x00};
    char randfile[MAXPGPATH] = {0x00};
    RandkeyFile rand_file_content;
    CipherkeyFile cipher_file_content;
    int ret;

    if (!isModeExists(mode)) {
#ifndef ENABLE_LLT
        (void)fprintf(stderr, _("invalid key mode.\n"));
        return;
#endif
    }
    if (datadir == NULL) {
        (void)fprintf(stderr, _("invalid data dir,recheck it please!\n"));
        return;
    }

    /* in server_mode,read the files begin with server% */
    if (mode == SERVER_MODE || mode == GDS_MODE) {
        ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/server%s", datadir, CIPHER_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/server%s", datadir, RAN_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
    } else if (mode == OBS_MODE) {
        ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/obsserver%s", datadir, CIPHER_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/obsserver%s", datadir, RAN_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
    } else if (mode == SOURCE_MODE) {
        ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/datasource%s", datadir, CIPHER_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/datasource%s", datadir, RAN_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
    } else if (mode == USER_MAPPING_MODE) {
        ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/usermapping%s", datadir, CIPHER_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/usermapping%s", datadir, RAN_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
    } else if (mode == SUBSCRIPTION_MODE) {
        ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/subscription%s", datadir, CIPHER_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/subscription%s", datadir, RAN_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
    } else if (mode == HADR_MODE) {
        ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/hadr%s", datadir, CIPHER_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/hadr%s", datadir, RAN_KEY_FILE);
        securec_check_ss_c(ret, "\0", "\0");
    }
    /*
     * in client_mode,check with the user name is appointed.if so, read the files begin
     * with %user_name%,if not,read default files that names begining with client%
     */
    else if (mode == CLIENT_MODE) {
        if (user_name == NULL) {
            ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/client%s", datadir, CIPHER_KEY_FILE);
            securec_check_ss_c(ret, "\0", "\0");
            ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/client%s", datadir, RAN_KEY_FILE);
            securec_check_ss_c(ret, "\0", "\0");
        } else {
            ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/%s%s", datadir, user_name, CIPHER_KEY_FILE);
            securec_check_ss_c(ret, "\0", "\0");
            ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/%s%s", datadir, user_name, RAN_KEY_FILE);
            securec_check_ss_c(ret, "\0", "\0");
        }
    }

    /* firstly we get key from memory, and then try to read from file. */
    if (!ReadKeyContentFromFile(mode, cipherkeyfile, randfile, &cipher_file_content, &rand_file_content,
            obsnormal_or_initdb, false) && 
        !ReadKeyContentFromFile(mode, cipherkeyfile, randfile, &cipher_file_content, &rand_file_content,
            obsnormal_or_initdb, true)) {
        ClearCipherKeyFile(&cipher_file_content);
        ClearRandKeyFile(&rand_file_content);
        (void)fprintf(stderr, _("read cipher file or random parameter file failed.\n"));
        return;
    }

    if (!DecryptInputKey(cipher_file_content.cipherkey, CIPHER_LEN, rand_file_content.randkey,
            cipher_file_content.key_salt, cipher_file_content.vector_salt, plainpwd, &plainlen)) {
        (void)fprintf(stderr, _("decrypt key failed.\n"));
    }

    ClearCipherKeyFile(&cipher_file_content);
    ClearRandKeyFile(&rand_file_content);
}

/* check whether the character is special characters */
static bool IsSpecialCharacter(char ch)
{
    const char* spec_letters = "~!@#$%^&*()-_=+\\|[{}];:,<.>/?";
    const char* ptr = spec_letters;
    while (*ptr != '\0') {
        if (*ptr == ch) {
            return true;
        }
        ptr++;
    }
    return false;
}

/*
 * @Brief		: bool check_certificate_signature_algrithm()
 * @Description	: check the  secure signature algrithm in the certificate.
 * @return		: return true if the certificate contain unsecured signature algrithm , otherwise return false.
 */
bool check_certificate_signature_algrithm(const SSL_CTX* SSL_context)
{
    bool retVal = false;
    const int ecdsa_sec_min_length = 32;
    const int rsa_sec_min_length = 256;
    X509 *pCert = NULL;
    if (SSL_context == NULL) {
        return retVal;
    }

    pCert = SSL_CTX_get0_certificate(SSL_context);
    if (pCert != NULL) {
        /* Lookup signature algorithm digest */
        int nid;
        nid = X509_get_signature_nid(pCert);
        /* Get the publickey length, return is bytes length. */
        int pub_length;
        EVP_PKEY *pub_key = NULL;
        pub_key = X509_get0_pubkey(pCert);
        pub_length = EVP_PKEY_size(pub_key);
        /* Get the signature algorithm CID from the certificate. */
        switch (nid) {
            case NID_md5WithRSA:
            case NID_md5WithRSAEncryption:
            case NID_sha1WithRSA:
            case NID_sha1WithRSAEncryption:
            case NID_dsaWithSHA1:
            case NID_dsaWithSHA1_2:
            case NID_ecdsa_with_SHA1:
            case NID_md5_sha1:
                retVal = true;
                break;
            case NID_ecdsa_with_SHA224:
            case NID_ecdsa_with_SHA256:
            case NID_ecdsa_with_SHA384:
            case NID_ecdsa_with_SHA512:
                if (pub_length < ecdsa_sec_min_length) {
                    retVal = true;
                }
                break;
            case NID_sha256WithRSAEncryption:
            case NID_sha384WithRSAEncryption:
            case NID_sha512WithRSAEncryption:
                if (pub_length < rsa_sec_min_length) {
                    retVal = true;
                }
                break;
            default:
                break;
        }
    }
    return retVal;
}

/*
 * @Brief	: long check_certificate_time()
 * @Description	: check the time of the certificate.
 * @return	: return the number of seconds when the certificate expires less than alarm_days.
 */
long check_certificate_time(const SSL_CTX* SSL_context, const int alarm_days)
{
    if (SSL_context == NULL) {
        return 0;
    }
    X509* pstCertificate = SSL_CTX_get0_certificate(SSL_context);
    if (pstCertificate != NULL) {
        int pday = 0;
        int psec = 0;
        const ASN1_TIME* notafter = NULL;

        /* Get the notafter time form this certificate.*/
        notafter = X509_get0_notAfter(pstCertificate);
        if (notafter == NULL) {
            return 0;
        }
        int retval = ASN1_TIME_diff(&pday, &psec, NULL, notafter);
        if (retval == 0) {
            return 0;
        }

        /* Compare localTime with notafter.*/
        if (pday < alarm_days && pday >= 0) {
            int diff = pday * 24 * 60 * 60 + psec;
            return diff;
        }
    }
    return 0;
}

/* get_evp_cipher_by_id: if you need to be use,you can add some types */
const EVP_CIPHER* get_evp_cipher_by_id(GS_UINT32 ulAlgType)
{
    const EVP_CIPHER* cipher = NULL;
    switch (ulAlgType & 0xFFFF) {
        case NID_aes_128_cbc:
            cipher = EVP_aes_128_cbc();
            break;
        case NID_undef:
            cipher = EVP_enc_null();
            break;
        default:
            break;
    }
    return cipher;
}

/* get_evp_md_by_id: if you need to be use,you can add some types */
const EVP_MD* get_evp_md_by_id(GS_UINT32 ulAlgType)
{
    const EVP_MD* md = NULL;
    switch (ulAlgType & 0xFFFF) {
        case NID_sha1:
        case NID_hmac_sha1:
            md = EVP_sha1();
            break;
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
 * @Brief        : GS_UINT32 CRYPT_encrypt()
 * @Description  : encrypts plain text to cipher text using encryption algorithm.
 *		  It creates symmetric context by creating algorithm object, padding object,
 *		  opmode object.After encryption, symmetric context needs to be freed.
 * @return       : success: 0, failed: 1.
 *
 * @Notes	: the last block is not full. so here need to padding the last block.(the block size is an algorithm-related
 *parameter) 1.here *ISO/IEC 7816-4* padding method is adoptted: the first byte uses "0x80" to padding ,and the others
 *uses "0x00". Example(in the following example the block size is 8 bytes): when the last block is not full: The last
 *block has 4 bytes, so four bytes need to be filled
 *	 	 	 	 ... | DD DD DD DD DD DD DD DD | DD DD DD DD 80 00 00 00 |
 *			when the last block is full: here need to add a new block
 *				 ... | DD DD DD DD DD DD DD DD | 80 00 00 00 00 00 00 00 |
 *		  2.Default padding method of OPENSSL(this method is closed at here): Each byte is filled with the number of
 *remaining bytes Example(in the following example the block size is 8 bytes): when the last block is not full: The last
 *block has 4 bytes, so four bytes need to be filled
 *                                ... | DD DD DD DD DD DD DD DD | DD DD DD DD 04 04 04 04 |
 *                       when the last block is full: here need to add a new block
 *                                ... | DD DD DD DD DD DD DD DD | 08 08 08 08 08 08 08 08 |
 */
GS_UINT32 CRYPT_encrypt(GS_UINT32 ulAlgId, const GS_UCHAR* pucKey, GS_UINT32 ulKeyLen, const GS_UCHAR* pucIV,
    GS_UINT32 ulIVLen, GS_UCHAR* pucPlainText, GS_UINT32 ulPlainLen, GS_UCHAR* pucCipherText, GS_UINT32* pulCLen)
{
    EVP_CIPHER_CTX* ctx = NULL;
    const EVP_CIPHER* cipher = NULL;
    int enc_num = 0;
    int blocksize = 0;
    int padding_size = 0;
    int nInbufferLen = 0;
    errno_t rc = EOK;
    unsigned char* pchInbuffer = NULL;
    if (pucPlainText == NULL) {
        (void)fprintf(stderr, ("invalid plain text,please check it!\n"));
        return 1;
    }
    cipher = get_evp_cipher_by_id(ulAlgId);
    if (cipher == NULL) {
        (void)fprintf(stderr, ("invalid ulAlgType for cipher,please check it!\n"));
        return 1;
    }
    ctx = EVP_CIPHER_CTX_new();
    if (ctx == NULL) {
        (void)fprintf(stderr, ("ERROR in EVP_CIPHER_CTX_new:\n"));
        return 1;
    }
    EVP_CipherInit_ex(ctx, cipher, NULL, pucKey, pucIV, 1);

    /* open padding mode */
    EVP_CIPHER_CTX_set_padding(ctx, 1);

    /* handling the last block */
    blocksize = EVP_CIPHER_CTX_block_size(ctx);
    if (blocksize == 0) {
        (void)fprintf(stderr, ("invalid blocksize, ERROR in EVP_CIPHER_CTX_block_size\n"));
        EVP_CIPHER_CTX_free(ctx);
        return 1;
    }

    nInbufferLen = ulPlainLen % blocksize;
    padding_size = blocksize - nInbufferLen;
    pchInbuffer = (unsigned char*)OPENSSL_malloc(blocksize);
    if (pchInbuffer == NULL) {
        (void)fprintf(stderr, _("malloc failed\n"));
        EVP_CIPHER_CTX_free(ctx);
        return 1;
    }
    /* the first byte uses "0x80" to padding ,and the others uses "0x00" */
    rc = memcpy_s(pchInbuffer, blocksize, pucPlainText + (ulPlainLen - nInbufferLen), nInbufferLen);
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(pchInbuffer + nInbufferLen, padding_size, 0, padding_size);
    securec_check_c(rc, "\0", "\0");
    pchInbuffer[nInbufferLen] = 0x80;

    /* close padding mode, default padding method of OPENSSL is forbidden */
    EVP_CIPHER_CTX_set_padding(ctx, 0);

    if (!EVP_EncryptUpdate(ctx, pucCipherText, &enc_num, pucPlainText, ulPlainLen - nInbufferLen)) {
        (void)fprintf(stderr, ("ERROR in EVP_EncryptUpdate\n"));
        goto err;
    }
    *pulCLen = enc_num;
    if (!EVP_EncryptUpdate(ctx, pucCipherText + enc_num, &enc_num, pchInbuffer, blocksize)) {
        (void)fprintf(stderr, ("ERROR in EVP_EncryptUpdate\n"));
        goto err;
    }
    *pulCLen += enc_num;
    if (!EVP_EncryptFinal(ctx, pucCipherText + *pulCLen, &enc_num)) {
        (void)fprintf(stderr, ("ERROR in EVP_EncryptUpdate\n"));
        goto err;
    }
    *pulCLen += enc_num;
    rc = memset_s(pchInbuffer, blocksize, 0, blocksize);
    securec_check_c(rc, "\0", "\0");
    OPENSSL_free(pchInbuffer);
    EVP_CIPHER_CTX_free(ctx);
    return 0;

err:
    rc = memset_s(pchInbuffer, blocksize, 0, blocksize);
    securec_check_c(rc, "\0", "\0");
    OPENSSL_free(pchInbuffer);
    EVP_CIPHER_CTX_free(ctx);
    return 1;
}

/*
 * @Brief        : GS_UINT32 CRYPT_decrypt()
 * @Description  : decrypts cipher text to plain text using decryption algorithm.
 *		  It creates symmetric context by creating algorithm object, padding object,
 *		  opmode object. After decryption, symmetric context needs to be freed.
 * @return       : success: 0, failed: 1.
 *
 * @Notes        : the last block is not full. so here need to padding the last block.(the block size is an
 *algorithm-related parameter) 1.here *ISO/IEC 7816-4* padding method is adoptted: the first byte uses "0x80" to padding
 *,and the others uses "0x00". Example(in the following example the block size is 8 bytes): when the last block is not
 *full: The last block has 4 bits,so padding is required for 4 bytes
 *                                ... | DD DD DD DD DD DD DD DD | DD DD DD DD 80 00 00 00 |
 *                       when the last block is full: here need to add a new block
 *                                ... | DD DD DD DD DD DD DD DD | 80 00 00 00 00 00 00 00 |
 */
GS_UINT32 CRYPT_decrypt(GS_UINT32 ulAlgId, const GS_UCHAR* pucKey, GS_UINT32 ulKeyLen, const GS_UCHAR* pucIV,
    GS_UINT32 ulIVLen, GS_UCHAR* pucCipherText, GS_UINT32 ulCLen, GS_UCHAR* pucPlainText, GS_UINT32* pulPLen)
{
    EVP_CIPHER_CTX* ctx = NULL;
    const EVP_CIPHER* cipher = NULL;
    int dec_num = 0;
    unsigned int blocksize;
    unsigned int oLen;

    if (pucCipherText == NULL) {
        (void)fprintf(stderr, ("invalid plain text,please check it!\n"));
        return 1;
    }
    cipher = get_evp_cipher_by_id(ulAlgId);
    if (cipher == NULL) {
        (void)fprintf(stderr, ("invalid ulAlgType for cipher,please check it!\n"));
        return 1;
    }
    ctx = EVP_CIPHER_CTX_new();
    if (ctx == NULL) {
        (void)fprintf(stderr, ("ERROR in EVP_CIPHER_CTX_new:\n"));
        return 1;
    }
    EVP_CipherInit_ex(ctx, cipher, NULL, pucKey, pucIV, 0);
    EVP_CIPHER_CTX_set_padding(ctx, 0);
    if (!EVP_DecryptUpdate(ctx, pucPlainText, &dec_num, pucCipherText, ulCLen)) {
        (void)fprintf(stderr, ("ERROR in EVP_DecryptUpdate\n"));
        goto err;
    }
    *pulPLen = dec_num;
    if (!EVP_DecryptFinal(ctx, pucPlainText + dec_num, &dec_num)) {
        (void)fprintf(stderr, ("ERROR in EVP_DecryptFinal\n"));
        goto err;
    }
    *pulPLen += dec_num;

    /* padding bytes of the last block need to be removed */
    blocksize = EVP_CIPHER_CTX_block_size(ctx);
    oLen = (*pulPLen) - 1;
    while (*(pucPlainText + oLen) == 0) {
        oLen--;
    }
    if (oLen >= ((*pulPLen) - blocksize) && *(pucPlainText + oLen) == 0x80) {
        (*pulPLen) = oLen;
    } else {
        goto err;
    }

    pucPlainText[oLen] = '\0';
    EVP_CIPHER_CTX_free(ctx);
    return 0;

err:
    EVP_CIPHER_CTX_free(ctx);
    return 1;
}

/*
 * @Brief		: GS_UINT32 CRYPT_hmac()
 * @Description	: computes hmac of a given data block. Calls init, update, and final.
 * 		  This function is used when data is present all at once. There is no need of calling
 * 		  Init, Update, Final and MAC can be calculated in one go. Context is not needed here.
 * @return	: success:1, failed:0.
 */
GS_UINT32 CRYPT_hmac(GS_UINT32 ulAlgType, const GS_UCHAR* pucKey, GS_UINT32 upucKeyLen, const GS_UCHAR* pucData,
    GS_UINT32 ulDataLen, GS_UCHAR* pucDigest, GS_UINT32* pulDigestLen)
{
    const EVP_MD* evp_md = get_evp_md_by_id(ulAlgType);
    if (evp_md == NULL) {
        return 1;
    }
#ifndef WIN32
    if (!HMAC(evp_md, pucKey, (int)upucKeyLen, pucData, ulDataLen, pucDigest, pulDigestLen)) {
        return 1;
    }
#else
    if (!HMAC(evp_md, pucKey, (int)upucKeyLen, pucData, ulDataLen, pucDigest, (unsigned int*)pulDigestLen)) {
        return 1;
    }
#endif
    return 0;
}

/*
 * @Brief         : char*  SEC_encodeBase64()
 * @Description   : This function converts the given DER code to base 64 format.
 * @return        : success:the ciphertext of Base64 encoding, failed:NULL.
 */
char* SEC_encodeBase64(const char* pucInBuf, GS_UINT32 ulInBufLen)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
    BIO* b64 = NULL;
    BIO* bio = NULL;
    BUF_MEM* bptr = NULL;
    char* out_str = NULL;

    if (pucInBuf == NULL || ulInBufLen == 0) {
        return NULL;
    }

    b64 = BIO_new(BIO_f_base64());
    if (b64 == NULL) {
        return NULL;
    }
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);

    bio = BIO_new(BIO_s_mem());
    if (bio == NULL) {
        return NULL;
    }
    bio = BIO_push(b64, bio);

    BIO_write(bio, pucInBuf, ulInBufLen);
    BIO_flush(bio);

    BIO_get_mem_ptr(bio, &bptr);

    out_str = (char*)OPENSSL_malloc(bptr->length + 1);
    if (out_str == NULL) {
        BIO_free_all(bio);
        return NULL;
    }

    errno_t rc = memcpy_s(out_str, bptr->length + 1, bptr->data, bptr->length);
    securec_check_c(rc, "\0", "\0");
    out_str[bptr->length] = '\0';

    BIO_free_all(bio);
    return out_str;
#pragma GCC diagnostic pop
}

/*
 * @Brief         : char*  SEC_decodeBase64()
 * @Description   : This function is used to decode from base 64 to DER format.
 * @return        : success:the plaintext of Base64 decoding, failed:NULL.
 */
char* SEC_decodeBase64(const char* pucInBuf, GS_UINT32* pulOutBufLen)
{
    BIO* b64 = NULL;
    BIO* bio = NULL;

    if (pucInBuf == NULL || pulOutBufLen == NULL) {
        return NULL;
    }

    unsigned int in_len = strlen(pucInBuf);
    char* out_str = (char*)OPENSSL_malloc(in_len + 1);
    if (out_str == NULL) {
        *pulOutBufLen = 0;
        return NULL;
    }

    b64 = BIO_new(BIO_f_base64());
    if (b64 == NULL) {
        OPENSSL_free(out_str);
        return NULL;
    }
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);

    bio = BIO_new_mem_buf(pucInBuf, in_len);
    if (bio == NULL) {
        OPENSSL_free(out_str);
        return NULL;
    }
    bio = BIO_push(b64, bio);

    *pulOutBufLen = BIO_read(bio, out_str, in_len);
    if (*pulOutBufLen <= 0) {
        OPENSSL_free(out_str);
        out_str = NULL;
        BIO_free_all(bio);
        return NULL;
    }
    out_str[*pulOutBufLen] = '\0';

    BIO_free_all(bio);
    return out_str;
}
