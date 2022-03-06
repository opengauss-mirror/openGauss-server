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
 * File Name	: cipherfn.cpp
 * Brief		:
 * Description	: encrypt and decrypt functions for MPPDB
 *
 * History	: 2014-10
 * 
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/aes/cipherfn.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "cipher.h"
#include "utils/builtins.h"
#include "utils/aes.h"
#include "utils/evp_cipher.h"

#include "storage/lock/lwlock.h"
#include "port.h"
#include "getopt_long.h"
#include "pgxc/pgxc.h"
#include "gaussdb_version.h"
#ifdef ENABLE_GSS
#include "gssapi/gssapi_krb5.h"
#endif /* ENABLE_GSS */

#include "tde_key_management/tde_key_manager.h"

#include "openssl/rand.h"
#include "openssl/evp.h"
#include "openssl/crypto.h"

#include "utils/evp_cipher.h"

#ifdef ENABLE_UT
#define static

/* ugly hook for tde().get_key, it should be mocked in ut testcase */
const char *fake_tde_get_key(const char *str)
{
    return str;
}
#endif

const char* pgname = "gs_encrypt";
void getOBSKeyString(GS_UCHAR** cipherKey);
static GS_UCHAR* getECKeyString(KeyMode mode);
char prefixCipherKey[2] = {'\0'};
char suffixCipherKey[100] = {'\0'};
bool gs_encrypt_aes_speed(GS_UCHAR* plaintext, GS_UCHAR* key, GS_UCHAR* ciphertext, GS_UINT32* cipherlen);
bool gs_decrypt_aes_speed(
    GS_UCHAR* ciphertext, GS_UINT32 cipherlen, GS_UCHAR* key, GS_UCHAR* plaintext, GS_UINT32* plainlen);
static char* gs_strdup(const char* s);
static void do_help(void);
static void do_advice(void);
static void free_global_value(void);
static bool is_prefix_in_key_mode(const char* mode);

// Fi root certificate path
char* g_fi_ca_path = NULL;

// the prefix of the output cipher/randfile
char* g_prefix = NULL;
int g_vector_len = 0;
// key text
char* g_key = NULL;
int g_key_len = 0;
// vector text
char* g_vector = NULL;

#define EC_ENCRYPT_PREFIX "encryptOpt"

#define ENCRYPT_TYPE_SM4 "sm4"
#define ENCRYPT_TYPE_AES128 "aes128"

// free the malloc memory
#define GS_FREE(ptr)        \
    if (NULL != (ptr)) {    \
        free((char*)(ptr)); \
        ptr = NULL;         \
    }

// check the character value
#define IsIllegalCharacter(c) ((c) != '/' && !isdigit((c)) && !isalpha((c)) && (c) != '_' && (c) != '-')

/*
 * @@GaussDB@@
 * Brief            : char *gs_strdup(const char *s)
 * Description      :
 * Notes            :
 */
static char* gs_strdup(const char* s)
{
    char* result = NULL;

    result = strdup(s);
    if (result == NULL) {
        (void)fprintf(stderr, _("%s: out of memory\n\n"), pgname);
        exit(1);
    }
    return result;
}

/*
 * decrypt the ciphertext with ase128 algorithms through OpenSSL API
 * old version, but still used by the new version in case there are data encrypted
 * by gs_encrypt_aes.
 */
bool gs_decrypt_aes(GS_UCHAR* ciphertext, GS_UINT32 cipherlen, GS_UCHAR* key, GS_UCHAR* plaintext, GS_UINT32* plainlen)
{
    GS_UINT32 cipherpartlen = 0;
    GS_UCHAR* cipherpart = NULL;
    GS_UCHAR* randpart = NULL;
    bool decryptstatus = false;
    errno_t errorno = EOK;

    cipherpartlen = cipherlen - RANDOM_LEN;

    /* split the cipher text to cipherpart(ciphertext + vectorsalt) and randpart for decrypt */
    cipherpart = (GS_UCHAR*)palloc0(cipherpartlen + 1);
    randpart = (GS_UCHAR*)palloc0(RANDOM_LEN);
    errorno = memcpy_s(cipherpart, cipherpartlen + 1, ciphertext, cipherpartlen);
    securec_check(errorno, "\0", "\0");
    errorno = memcpy_s(randpart, RANDOM_LEN, ciphertext + cipherpartlen, RANDOM_LEN);
    securec_check(errorno, "\0", "\0");

    /* the real decrypt operation */
    decryptstatus = aes128Decrypt(
        cipherpart, cipherpartlen, key, (GS_UINT32)strlen((const char*)key), randpart, plaintext, plainlen);

    errorno = memset_s(cipherpart, cipherpartlen + 1, '\0', cipherpartlen + 1);
    securec_check(errorno, "\0", "\0");
    pfree_ext(cipherpart);
    errorno = memset_s(randpart, RANDOM_LEN, '\0', RANDOM_LEN);
    securec_check(errorno, "\0", "\0");
    pfree_ext(randpart);

    if (!decryptstatus) {
        return false;
    } else {
        return true;
    }
}

/*
 * Target		:Encrypt functions for security.
 * Description	:Encrypt with standard aes128 algorthm using OpenSSL functions.
 * Notes		:The Key used here must be the same as it was used in decrypt.
 * Input		:PG_FUNCTION_ARGS, containing plaintext and key
 * Output		:Pointer of the CipherText which is encrypted and encoded from plaintext
 * Revision	:Using random initial vector and one fixed salt in one thread
 * 			  in order to avoid generating deriveKey everytime.
 */
static bool gs_encrypt_aes128_function(FunctionCallInfo fcinfo, text** outtext)
{
    char* key = NULL;
    GS_UINT32 keylen = 0;
    char* plaintext = NULL;
    GS_UINT32 plaintextlen = 0;
    GS_UCHAR* ciphertext = NULL;
    GS_UINT32 ciphertextlen = 0;
    GS_UINT32 retval = 0;
    char* encodetext = NULL;
    GS_UINT32 encodetextlen = 0;
    GS_UINT32 ciphertextlen_max = 0;
    errno_t errorno = EOK;

    plaintext = text_to_cstring(PG_GETARG_TEXT_P(0));
    plaintextlen = strlen(plaintext);
    key = text_to_cstring(PG_GETARG_TEXT_P(1));
    keylen = strlen(key);

    /* The input key must shorter than RANDOM_LEN(16) */
    if (!check_input_password(key)) {
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                errmsg("The encryption key must be %d~%d bytes and contain at least three kinds of characters!",
                MIN_KEY_LEN, MAX_KEY_LEN)));
    }

    /*
     * Calculate the max length of ciphertext:
     * contain aes-expand length, IV length, Salt length and backup length.
     * Add mac text length.
     */
    ciphertextlen_max = plaintextlen + RANDOM_LEN * 4 + MAC_LEN;

    /* refer combo_encrypt_len function ,cipherlen = plainlen + 64 */
    ciphertext = (GS_UCHAR*)palloc(ciphertextlen_max);
    errorno = memset_s(ciphertext, ciphertextlen_max, '\0', ciphertextlen_max);
    securec_check(errorno, "\0", "\0");

    /* encrypt the plaintext to ciphertext */
    retval = gs_encrypt_aes_speed((GS_UCHAR*)plaintext, (GS_UCHAR*)key, ciphertext, &ciphertextlen);

    errorno = memset_s(plaintext, plaintextlen, '\0', plaintextlen);
    securec_check(errorno, "\0", "\0");
    pfree_ext(plaintext);
    errorno = memset_s(key, keylen, '\0', keylen);
    securec_check(errorno, "\0", "\0");
    pfree_ext(key);

    if (!retval) {
        errorno = memset_s(ciphertext, ciphertextlen, '\0', ciphertextlen);
        securec_check(errorno, "\0", "\0");
        pfree_ext(ciphertext);
        ereport(
            ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("encrypt the plain text failed!")));
    }

    /* encode the ciphertext for nice show and decrypt operation */
    encodetext = SEC_encodeBase64((char*)ciphertext, ciphertextlen);
    errorno = memset_s(ciphertext, ciphertextlen, '\0', ciphertextlen);
    securec_check(errorno, "\0", "\0");
    pfree_ext(ciphertext);
    if (encodetext == NULL) {
        ciphertextlen_max = 0;
        ereport(
            ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("encode the plain text failed!")));
    }
    encodetextlen = strlen(encodetext);

    *outtext = cstring_to_text(encodetext);
    errorno = memset_s(encodetext, encodetextlen, '\0', encodetextlen);
    securec_check(errorno, "\0", "\0");
    OPENSSL_free(encodetext);
    encodetext = NULL;

    return true;
}

/*
 * Target		:Decrypt functions for security.
 * Description	:Decrypt with standard aes128 algorthm using OpenSSL functions.
 * Notes		:The Key used here must be the same as it was used in encrypt.
 * Input		:PG_FUNCTION_ARGS, containing ciphertext and key
 * Output		:Pointer of the PlainText which is decrypted and decoded from ciphertext
 * Revision	:Save several derivekeys and salts in one thread
 * 			  in order to avoid generating deriveKey everytime.
 */
bool gs_decrypt_aes128_function(FunctionCallInfo fcinfo, text** outtext)
{
    GS_UCHAR* key = NULL;
    GS_UINT32 keylen = 0;
    GS_UCHAR* plaintext = NULL;
    GS_UINT32 plaintextlen = 0;
    GS_UCHAR* ciphertext = NULL;
    GS_UINT32 retval = 0;
    GS_UCHAR* decodetext = NULL;
    GS_UINT32 decodetextlen = 0;
    errno_t errorno = EOK;

    decodetext = (GS_UCHAR*)(text_to_cstring(PG_GETARG_TEXT_P(0)));
    key = (GS_UCHAR*)(text_to_cstring(PG_GETARG_TEXT_P(1)));
    keylen = strlen((const char*)key);

    /* decode the ciphertext for decrypt operation */
    ciphertext = (GS_UCHAR*)(SEC_decodeBase64((char*)decodetext, &decodetextlen));
    if ((ciphertext == NULL) || (decodetextlen <= RANDOM_LEN)) {
        if (ciphertext != NULL) {
            OPENSSL_free(ciphertext);
            ciphertext = NULL;
        }
        errorno = memset_s(decodetext, decodetextlen, '\0', decodetextlen);
        securec_check(errorno, "\0", "\0");
        pfree_ext(decodetext);
        errorno = memset_s(key, keylen, '\0', keylen);
        securec_check(errorno, "\0", "\0");
        pfree_ext(key);
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                errmsg("Decode the cipher text failed or the ciphertext is too short!")));
    }
    errorno = memset_s(decodetext, decodetextlen, '\0', decodetextlen);
    securec_check(errorno, "\0", "\0");
    pfree_ext(decodetext);

    plaintext = (GS_UCHAR*)palloc(decodetextlen);
    errorno = memset_s(plaintext, decodetextlen, '\0', decodetextlen);
    securec_check(errorno, "\0", "\0");

    /*
     *  decrypt the ciphertext to plaintext using the new version function first
     *  try :old vesion of decryption function if the new one failed.
     *  if the old one also failed, return alarm.
     */
    retval = gs_decrypt_aes_speed(ciphertext, decodetextlen, key, plaintext, &plaintextlen);
    if (!retval) {
        /* reset plaintext */
        errorno = memset_s(plaintext, decodetextlen, '\0', decodetextlen);
        securec_check(errorno, "\0", "\0");
        /* try different decrypt function */
        retval = gs_decrypt_aes(ciphertext, decodetextlen, key, plaintext, &plaintextlen);
        (void)fprintf(stderr, _("Try previous version of decrypt-function!\n"));
    }

    errorno = memset_s(key, keylen, '\0', keylen);
    securec_check(errorno, "\0", "\0");
    pfree_ext(key);

    if (!retval) {
        errorno = memset_s(ciphertext, decodetextlen, '\0', decodetextlen);
        securec_check(errorno, "\0", "\0");
        OPENSSL_free(ciphertext);
        ciphertext = NULL;

        errorno = memset_s(plaintext, plaintextlen, '\0', plaintextlen);
        securec_check(errorno, "\0", "\0");
        pfree_ext(plaintext);
        ereport(
            ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("decrypt the cipher text failed!")));
    }

    errorno = memset_s(ciphertext, decodetextlen, '\0', decodetextlen);
    securec_check(errorno, "\0", "\0");
    OPENSSL_free(ciphertext);
    ciphertext = NULL;

    *outtext = cstring_to_text((char*)plaintext);
    errorno = memset_s(plaintext, plaintextlen, '\0', plaintextlen);
    securec_check(errorno, "\0", "\0");
    pfree_ext(plaintext);

    return true;
}

/*
 *	Generate random and compute plain text length.
 *	This function use key and random to encrypt plain text
 *     ATTENTION:
 *        The cipherlen is an "IN&OUT" param!!!!!!!
 */
bool gs_encrypt_aes_128(
    GS_UCHAR* plaintext, GS_UCHAR* key, GS_UINT32 keylen, GS_UCHAR* ciphertext, GS_UINT32* cipherlen)
{
    errno_t rc = 0;
    errno_t ret = 0;
    GS_UINT32 retval = 0;
    GS_UINT32 plainlen = 0;
    GS_UCHAR init_rand[RANDOM_LEN + 1] = {0};
    GS_UCHAR tmp_cipher[CIPHER_LEN + 1] = {0};
    bool encryptstatus = false;
    char* gausshome = NULL;
    char randfile[MAXPGPATH] = {'\0'};
    char cipherfile[MAXPGPATH] = {'\0'};
    size_t cipherbufferlen = (size_t)(*cipherlen);

    /* the result of do getKeyVectorFromCipherFile */
    bool status = false;

    if (NULL == plaintext) {
        return false;
    }
    plainlen = strlen((const char*)plaintext);

    /* default branch should do decode */
    /* -f is in key mode */
    if ((NULL == g_prefix && NULL == g_key && NULL == g_vector) ||
        (NULL != g_prefix && is_prefix_in_key_mode((const char*)g_prefix))) {
        /* get a random values as salt for encrypt */
        retval = RAND_priv_bytes(init_rand, RANDOM_LEN);
        if (retval != 1) {
            (void)fprintf(stderr, _("generate random key failed, errcode:%u\n"), retval);
            return false;
        }
        /* the real encrypt operation */
        encryptstatus = aes128Encrypt(plaintext, plainlen, key, keylen, init_rand, ciphertext, cipherlen);
        if (!encryptstatus) {
            return false;
        }

        if (((size_t)(*cipherlen) + RANDOM_LEN) > cipherbufferlen) {
            (void)fprintf(stderr, _("ciphertext buffer is too small\n"));
            return false;
        }

        /* add init_rand to the end of ciphertext for decrypt */
        rc = memcpy_s(ciphertext + (*cipherlen), (Size)RANDOM_LEN, init_rand, (Size)RANDOM_LEN);
        securec_check(rc, "\0", "\0");
        return true;
    } 
        
    /* -f is not in key mode */
    if (NULL != g_prefix) {
        gausshome = gs_getenv_r("GAUSSHOME");
        char real_gausshome[PATH_MAX + 1] = {'\0'};
        if (gausshome == NULL || realpath(gausshome, real_gausshome) == NULL) {
            (void)fprintf(stderr, _("get environment of GAUSSHOME failed.\n"));
            exit(1);
        }

        check_backend_env(real_gausshome);
        ret = snprintf_s(randfile, MAXPGPATH, MAXPGPATH - 1, "%s/bin/%s.key.rand", real_gausshome, g_prefix);
        securec_check_ss(ret, "\0", "\0");
        ret = snprintf_s(cipherfile, MAXPGPATH, MAXPGPATH - 1, "%s/bin/%s.key.cipher", real_gausshome, g_prefix);
        securec_check_ss(ret, "\0", "\0");

        /* get key from file */
        status = getKeyVectorFromCipherFile(cipherfile, randfile, tmp_cipher, init_rand);
        if (!status) {
            rc = memset_s(tmp_cipher, sizeof(tmp_cipher), 0, sizeof(tmp_cipher));
            securec_check(rc, "\0", "\0");
            (void)fprintf(
                stderr, _("Failed to decrypt key from transparent encryption random file %s.\n"), randfile);
            return false;
        }
        encryptstatus =
            aes128Encrypt(plaintext, plainlen, tmp_cipher, CIPHER_LEN, init_rand, ciphertext, cipherlen);
        if (!encryptstatus) {
            rc = memset_s(tmp_cipher, sizeof(tmp_cipher), 0, sizeof(tmp_cipher));
            securec_check(rc, "\0", "\0");
            return false;
        }

        if (cipherbufferlen < ((size_t)(*cipherlen) + RANDOM_LEN)) {
            rc = memset_s(tmp_cipher, sizeof(tmp_cipher), 0, sizeof(tmp_cipher));
            securec_check(rc, "\0", "\0");
            (void)fprintf(stderr, _("ciphertext buffer is too small.\n"));
            return false;
        }
        /* add init_rand to the end of ciphertext for decrypt */
        rc = memcpy_s(ciphertext + (*cipherlen), (Size)RANDOM_LEN, init_rand, (Size)RANDOM_LEN);
        securec_check(rc, "\0", "\0");
        return true;
    } 
    
    encryptstatus = aes128Encrypt(plaintext,
        plainlen,
        (GS_UCHAR*)g_key,
        (GS_UINT32)g_key_len,
        (GS_UCHAR*)g_vector,
        ciphertext,
        cipherlen);
    if (!encryptstatus) {
        return false;
    }

    if (((size_t)(*cipherlen) + (size_t)g_vector_len) > cipherbufferlen) {
        (void)fprintf(stderr, _("ciphertext buffer is too small\n"));
        return false;
    }

    /* add init_rand to the end of ciphertext for decrypt */
    rc = memcpy_s(ciphertext + (*cipherlen), (Size)g_vector_len, g_vector, (Size)g_vector_len);
    securec_check(rc, "\0", "\0");

    return true;
}

/*
 *	Split cipher text to cipher part and random part.
 *	This function use key and random to decrypt cipher text
 */
bool gs_decrypt_aes_128(GS_UCHAR* ciphertext, GS_UINT32 cipherlen, GS_UCHAR* key, GS_UINT32 keylen, GS_UCHAR* plaintext,
    GS_UINT32* plainlen)
{
    errno_t rc;
    GS_UINT32 cipherpartlen = 0;
    GS_UCHAR* cipherpart = NULL;
    GS_UCHAR* randpart = NULL;
    bool decryptstatus = false;

    if (NULL == ciphertext) {
        return false;
    }
    /* split the cipher text to cipher(cipher + vector_salt) part and rand part for decrypt */
    cipherpartlen = cipherlen - RANDOM_LEN;

    cipherpart = (GS_UCHAR*)palloc0(cipherpartlen + 1);
    randpart = (GS_UCHAR*)palloc0(RANDOM_LEN + 1);
    rc = memcpy_s(cipherpart, cipherpartlen + 1, ciphertext, cipherpartlen);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(randpart, RANDOM_LEN + 1, ciphertext + cipherpartlen, RANDOM_LEN);
    securec_check(rc, "\0", "\0");

    /* the real decrypt operation */
    decryptstatus = aes128Decrypt(cipherpart, cipherpartlen, key, keylen, randpart, plaintext, plainlen);

    rc = memset_s(cipherpart, cipherpartlen + 1, 0, cipherpartlen + 1);
    securec_check(rc, "\0", "\0");
    pfree_ext(cipherpart);
    rc = memset_s(randpart, RANDOM_LEN, 0, RANDOM_LEN);
    securec_check(rc, "\0", "\0");
    pfree_ext(randpart);

    if (!decryptstatus) {
        return false;
    }
    return true;
}

/*
 *	Encrypt plain text to cipher text.
 *	This function use aes128 to encrypt plain text,key comes from certificate file
 */
void encryptOBS(char* srcplaintext, char destciphertext[], uint32 destcipherlength)
{
    errno_t rc = EOK;

    GS_UINT32 ciphertextlen = 512;
    GS_UCHAR* ciphertext = NULL;
    GS_UCHAR* decipherkey = NULL;
    getOBSKeyString(&decipherkey);

    ciphertext = (GS_UCHAR*)palloc(ciphertextlen);
    rc = memset_s(ciphertext, ciphertextlen, 0, ciphertextlen);
    securec_check(rc, "\0", "\0");
    if (!gs_encrypt_aes_128((GS_UCHAR*)srcplaintext, decipherkey,
        (GS_UINT32)strlen((const char*)decipherkey), ciphertext, &ciphertextlen)) {
        rc = memset_s(srcplaintext, strlen(srcplaintext) + 1, 0, strlen(srcplaintext) + 1);
        securec_check(rc, "\0", "\0");
        rc = memset_s(ciphertext, ciphertextlen, 0, ciphertextlen);
        securec_check(rc, "\0", "\0");
        pfree_ext(ciphertext);
        rc = memset_s(decipherkey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
        securec_check(rc, "\0", "\0");
        pfree_ext(decipherkey);
        ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("Encrypt OBS AK/SK failed.")));
    }

    char* encodetext = SEC_encodeBase64((char*)ciphertext, ciphertextlen + RANDOM_LEN);

    if (encodetext == NULL || destcipherlength < strlen(encodetext) + 1) {
        rc = memset_s(srcplaintext, strlen(srcplaintext) + 1, 0, strlen(srcplaintext) + 1);
        securec_check(rc, "\0", "\0");
        rc = memset_s(ciphertext, ciphertextlen, 0, ciphertextlen);
        securec_check(rc, "\0", "\0");
        pfree_ext(ciphertext);
        rc = memset_s(decipherkey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
        securec_check(rc, "\0", "\0");
        pfree_ext(decipherkey);
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("Encrypt OBS AK/SK internal error")));
    }

    rc = memcpy_s(destciphertext, destcipherlength, encodetext, (strlen(encodetext) + 1));
    securec_check(rc, "\0", "\0");
    rc = memset_s(encodetext, strlen(encodetext) + 1, 0, strlen(encodetext) + 1);
    securec_check(rc, "\0", "\0");
    OPENSSL_free(encodetext);
    encodetext = NULL;
    rc = memset_s(ciphertext, ciphertextlen, 0, ciphertextlen);
    securec_check(rc, "\0", "\0");
    pfree_ext(ciphertext);
    rc = memset_s(decipherkey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
    securec_check(rc, "\0", "\0");
    pfree_ext(decipherkey);
}

/*
 *	Decrypt cipher text to plain text.
 *	This function use aes128 to decrypt cipher text,key comes from certificate file
 */
void decryptOBS(const char* srcciphertext, char destplaintext[], uint32 destplainlength, const char* obskey)
{
    errno_t rc = EOK;

    GS_UCHAR* ciphertext = NULL;
    GS_UINT32 decodetextlen = 0;

    GS_UCHAR* plaintext = NULL;
    GS_UINT32 plaintextlen = 0;
    GS_UCHAR* decipherkey = NULL;
    if (NULL == srcciphertext || NULL == destplaintext) {
        return;
    }

    if (obskey != NULL) {
        decipherkey = (GS_UCHAR*)palloc(RANDOM_LEN + 1);
        rc = memset_s(decipherkey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
        securec_check(rc, "\0", "\0");

        int keysize = strlen(obskey);
        rc = memcpy_s(decipherkey, RANDOM_LEN + 1, obskey, keysize);
        securec_check(rc, "\0", "\0");
    } else {
        getOBSKeyString(&decipherkey);
    }
    ciphertext = (GS_UCHAR*)(SEC_decodeBase64((char*)srcciphertext, &decodetextlen));
    plaintext = (GS_UCHAR*)palloc(decodetextlen);
    rc = memset_s(plaintext, decodetextlen, 0, decodetextlen);
    securec_check(rc, "\0", "\0");
    if (!gs_decrypt_aes_128(ciphertext, decodetextlen, decipherkey,
        (GS_UINT32)strlen((const char*)decipherkey), plaintext, &plaintextlen)) {
        rc = memset_s(decipherkey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
        securec_check(rc, "\0", "\0");
        pfree_ext(decipherkey);
        ereport(ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("Decrypt OBS AK/SK failed.")));
    }

    /* 1 byte for \0 */
    if (plaintextlen + 1 > destplainlength) {
        rc = memset_s(decipherkey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
        securec_check(rc, "\0", "\0");
        pfree_ext(decipherkey);
        rc = memset_s(plaintext, decodetextlen, 0, decodetextlen);
        securec_check(rc, "\0", "\0");
        pfree_ext(plaintext);
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("Decrypt OBS AK/SK internal error.")));
    }

    rc = memcpy_s(destplaintext, destplainlength, plaintext, plaintextlen);
    securec_check(rc, "\0", "\0");
    destplaintext[plaintextlen] = '\0';

    rc = memset_s(plaintext, decodetextlen, 0, decodetextlen);
    securec_check(rc, "\0", "\0");
    pfree_ext(plaintext);

    rc = memset_s(decipherkey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
    securec_check(rc, "\0", "\0");
    pfree_ext(decipherkey);
    /*
     * ciphertext should not be null. If ciphertext is null,
     * the processing throwed ERROR after calling gs_decrypt_aes_128
     */
    Assert(NULL != ciphertext);
    rc = memset_s(ciphertext, decodetextlen, 0, decodetextlen);
    securec_check(rc, "\0", "\0");
    OPENSSL_free(ciphertext);
    ciphertext = NULL;
}

void getOBSKeyString(GS_UCHAR** cipherKey)
{
    /*
     * Notice: cypherKey will be overwritten.
     */
    Assert(NULL == *cipherKey);
    int ret = 0;

    char* cipherpath = NULL;
    char cipherkeyfile[MAXPGPATH] = {'\0'};
    char isexistscipherkeyfile[MAXPGPATH] = {'\0'};

    /*
     * Important: function getenv() is not thread safe.
     */
    LWLockAcquire(OBSGetPathLock, LW_SHARED);
    cipherpath = gs_getenv_r("GAUSSHOME");
    char real_gausshome[PATH_MAX + 1] = {'\0'};
    LWLockRelease(OBSGetPathLock);
    if (cipherpath == NULL || realpath(cipherpath, real_gausshome) == NULL) {
        (void)fprintf(stderr, _("Get environment of GAUSSHOME failed or it is invalid.\n"));
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("Failed to get OBS certificate file.")));
    }
    cipherpath = real_gausshome;
    check_backend_env(cipherpath);

    ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/bin", cipherpath);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(isexistscipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/bin/obsserver.key.cipher", cipherpath);
    securec_check_ss_c(ret, "\0", "\0");

    *cipherKey = (GS_UCHAR*)palloc(RANDOM_LEN + 1);
    ret = memset_s(*cipherKey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
    securec_check(ret, "\0", "\0");
    // proccess for unit test failed for no obsserver key
    if (file_exists(isexistscipherkeyfile)) {
        decode_cipher_files(OBS_MODE, NULL, cipherkeyfile, *cipherKey);
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FILE),
                errmsg("No key file obsserver.key.cipher"),
                errhint("Please create obsserver.key.cipher file with gs_guc, such as : gs_guc generate -S XXX -D $GAUSSHOME/bin -o "
                        "obsserver")));
    }
}

/*
 * Target		:Encrypt functions for security.
 * Description	:Generating random salt if there is not a saved one,
 * 				 and then use the random salt to encrypt plaintext.
 * Input		:Pointer of plaintext, key, ciphertext and cipherlen
 * Output		:successed: true, failed: false.
 */
bool gs_encrypt_aes_speed(GS_UCHAR* plaintext, GS_UCHAR* key, GS_UCHAR* ciphertext, GS_UINT32* cipherlen)
{
    GS_UINT32 retval = 0;
    GS_UINT32 plainlen = 0;
    GS_UCHAR init_rand[RANDOM_LEN] = {0};
    bool encryptstatus = false;
    errno_t errorno = EOK;

    /* use saved random salt unless unavailable */
    static THR_LOCAL GS_UCHAR random_salt_saved[RANDOM_LEN] = {0};
    static THR_LOCAL bool random_salt_tag = false;
    static THR_LOCAL GS_UINT64 random_salt_count = 0;

    /* A limitation of using times of one random salt */
    const GS_UINT64 random_salt_count_max = 24000000;

    if (random_salt_tag == false || random_salt_count > random_salt_count_max) {
        /* get a random values as salt for encrypt */
        retval = RAND_priv_bytes(init_rand, RANDOM_LEN);
        if (retval != 1) {
            (void)fprintf(stderr, _("generate random key failed,errcode:%u\n"), retval);
            return false;
        }
        random_salt_tag = true;
        errorno = memcpy_s(random_salt_saved, RANDOM_LEN, init_rand, RANDOM_LEN);
        securec_check(errorno, "\0", "\0");
        random_salt_count = 0;
    } else {
        errorno = memcpy_s(init_rand, RANDOM_LEN, random_salt_saved, RANDOM_LEN);
        securec_check(errorno, "\0", "\0");
        random_salt_count++;
    }

    plainlen = strlen((const char*)plaintext);

    /* the real encrypt operation */
    encryptstatus = aes128EncryptSpeed(plaintext, plainlen, key, init_rand, ciphertext, cipherlen);
    if (!encryptstatus) {
        ereport(WARNING, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("aes128EncryptSpeedFailed!")));
        return false;
    }

    /* add init_rand to the head of ciphertext for decrypt */
    GS_UCHAR mac_temp[MAC_LEN] = {0};
    errorno = memcpy_s(mac_temp, MAC_LEN, ciphertext + *cipherlen - MAC_LEN, MAC_LEN);
    securec_check(errorno, "\0", "\0");
    errorno = memcpy_s(ciphertext + *cipherlen - MAC_LEN + RANDOM_LEN, MAC_LEN, mac_temp, MAC_LEN);
    securec_check(errorno, "\0", "\0");

    GS_UCHAR temp[RANDOM_LEN] = {0};
    for (GS_UINT32 i = (*cipherlen - MAC_LEN) / RANDOM_LEN; i >= 1; --i) {
        errorno = memcpy_s(temp, RANDOM_LEN, ciphertext + (i - 1) * RANDOM_LEN, RANDOM_LEN);
        securec_check(errorno, "\0", "\0");

        errorno = memcpy_s(ciphertext + i * RANDOM_LEN, RANDOM_LEN, temp, RANDOM_LEN);
        securec_check(errorno, "\0", "\0");
    }
    errorno = memcpy_s(ciphertext, RANDOM_LEN, init_rand, RANDOM_LEN);
    securec_check(errorno, "\0", "\0");
    *cipherlen = *cipherlen + RANDOM_LEN;
    errorno = memset_s(temp, RANDOM_LEN, '\0', RANDOM_LEN);
    securec_check(errorno, "\0", "\0");
    return true;
}

/*
 * Target		:Decrypt functions for security.
 * Description	:Read random salt from ciphertext,
 * 				 and then use the random salt to decrypt plaintext.
 * Input		:Pointer of plaintext, key, ciphertext and the value of cipherlen and plainlen.
 * Output		:successed: true, failed: false.
 */
bool gs_decrypt_aes_speed(
    GS_UCHAR* ciphertext, GS_UINT32 cipherlen, GS_UCHAR* key, GS_UCHAR* plaintext, GS_UINT32* plainlen)
{
    GS_UINT32 cipherpartlen = 0;
    GS_UCHAR* cipherpart = NULL;
    GS_UCHAR* randpart = NULL;
    bool decryptstatus = false;
    errno_t errorno = EOK;

    if (cipherlen < (3 * RANDOM_LEN + MAC_LEN)) {
        (void)fprintf(stderr, _("Cipertext is too short to decrypt\n"));
        return false;
    }

    cipherpartlen = cipherlen - RANDOM_LEN;

    /* split the ciphertext to cipherpart and randpart for decrypt */
    cipherpart = (GS_UCHAR*)palloc(cipherpartlen + 1);
    randpart = (GS_UCHAR*)palloc(RANDOM_LEN);
    errorno = memcpy_s(cipherpart, cipherpartlen + 1, ciphertext + RANDOM_LEN, cipherpartlen);
    securec_check(errorno, "\0", "\0");
    errorno = memcpy_s(randpart, RANDOM_LEN, ciphertext, RANDOM_LEN);
    securec_check(errorno, "\0", "\0");

    /* the real decrypt operation */
    decryptstatus = aes128DecryptSpeed(cipherpart, cipherpartlen, key, randpart, plaintext, plainlen);

    errorno = memset_s(cipherpart, cipherpartlen + 1, '\0', cipherpartlen + 1);
    securec_check(errorno, "\0", "\0");
    pfree_ext(cipherpart);
    errorno = memset_s(randpart, RANDOM_LEN, '\0', RANDOM_LEN);
    securec_check(errorno, "\0", "\0");
    pfree_ext(randpart);

    if (!decryptstatus) {
        return false;
    } else {
        return true;
    }
}

static inline const char* GetCipherPrefix(int mode)
{
    switch (mode) {
        case SOURCE_MODE:
            return "datasource";
        case USER_MAPPING_MODE:
            return "usermapping";
        case SUBSCRIPTION_MODE:
            return "subscription";
        case HADR_MODE:
            return "hadr";
        default:
            ereport(ERROR, (errmsg("unknown key mode: %d", mode)));
            return NULL;
    }
}

/*
 * getECKeyString:
 * 	Get Extension Connector(EC) key string
 *
 * @IN/OUT cipherKey: get key string and stored in cipherKey
 * @RETURN: void
 */
static GS_UCHAR* getECKeyString(KeyMode mode)
{
    Assert(mode == SOURCE_MODE || mode == USER_MAPPING_MODE || mode == SUBSCRIPTION_MODE || mode == HADR_MODE);
    GS_UCHAR* plainkey = NULL;
    char* gshome = NULL;
    char cipherdir[MAXPGPATH] = {0};
    char cipherfile[MAXPGPATH] = {0};
    const char *cipherPrefix = GetCipherPrefix(mode);
    int ret = 0;

    /*
     * Get cipher file and prepare the plain buffer
     */
    gshome = gs_getenv_r("GAUSSHOME");
    char real_gausshome[PATH_MAX + 1] = {'\0'};
    if (gshome == NULL || realpath(gshome, real_gausshome) == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                errmsg("Failed to get EC certificate file: get env GAUSSHOME failed.")));
    }
    gshome = real_gausshome;
    check_backend_env(gshome);

    ret = snprintf_s(cipherdir, MAXPGPATH, MAXPGPATH - 1, "%s/bin", gshome);
    securec_check_ss(ret, "\0", "\0");
    ret = snprintf_s(cipherfile, MAXPGPATH, MAXPGPATH - 1, "%s/bin/%s.key.cipher", gshome, cipherPrefix);
    securec_check_ss_c(ret, "\0", "\0");

    gshome = NULL;

    /* Alloc plain key buffer and clear it */
    plainkey = (GS_UCHAR*)palloc(RANDOM_LEN + 1);
    ret = memset_s(plainkey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
    securec_check(ret, "\0", "\0");

    /*
     * Decode cipher file, if not given SOURCE cipher file, we use SERVER mode
     */
    if (file_exists(cipherfile)) {
        decode_cipher_files(mode, NULL, cipherdir, plainkey);
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FILE),
             errmsg("No key file %s.key.cipher", cipherPrefix),
             errhint("Please create %s.key.cipher file with gs_guc and gs_ssh, such as :"
                     "gs_ssh -c \"gs_guc generate -S XXX -D $GAUSSHOME/bin -o %s\"", cipherPrefix, cipherPrefix)));
    }
    /*
     * Note: plainkey is of length RANDOM_LEN + 1
     */
    return plainkey;
}

/*
 * encryptECString:
 * 	Encrypt Extension Connector(EC) string(username/password, etc.) into cipher text.
 * 	This function use aes128 to encrypt plain text; key comes from certificate file
 *
 * @IN src_plain_text: source plain text to be encrypted
 * @OUT dest_cipher_text: dest buffer to be filled with encrypted string, this buffer
 * 	should be given by caller
 * @IN dest_cipher_length: dest buffer length which is given by the caller
 * @IN mode: key mode
 * @RETURN: void
 */
void encryptECString(char* src_plain_text, char* dest_cipher_text, uint32 dest_cipher_length, int mode)
{
    GS_UINT32 ciphertextlen = 0;
    GS_UCHAR ciphertext[1024];
    GS_UCHAR* cipherkey = NULL;
    char* encodetext = NULL;
    errno_t ret = EOK;

    if (NULL == src_plain_text) {
        return;
    }

    /* First, get encrypt key */
    cipherkey = getECKeyString((KeyMode)mode);

    /* Clear cipher buffer which will be used later */
    ret = memset_s(ciphertext, sizeof(ciphertext), 0, sizeof(ciphertext));
    securec_check(ret, "\0", "\0");

    /*
     * Step-1: Cipher
     * 	src_text with cipher key -> cipher text
     */
    ciphertextlen = (GS_UINT32)sizeof(ciphertext);
    if (!gs_encrypt_aes_128((GS_UCHAR*)src_plain_text, cipherkey,
        (GS_UINT32)strlen((const char*)cipherkey), ciphertext, &ciphertextlen)) {
        ret = memset_s(src_plain_text, strlen(src_plain_text), 0, strlen(src_plain_text));
        securec_check(ret, "\0", "\0");            
        ret = memset_s(ciphertext, sizeof(ciphertext), 0, sizeof(ciphertext));
        securec_check(ret, "\0", "\0");
        ret = memset_s(cipherkey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
        securec_check(ret, "\0", "\0");
        pfree_ext(src_plain_text);
        pfree_ext(cipherkey);
        ereport(
            ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("encrypt the EC string failed!")));
    }

    /*
     * Step-2: Encode
     * 	cipher text by Base64 -> encode text
     */
    encodetext = SEC_encodeBase64((char*)ciphertext, ciphertextlen + RANDOM_LEN);

    /* Check dest buffer length */
    if (encodetext == NULL || dest_cipher_length < strlen(EC_ENCRYPT_PREFIX) + strlen(encodetext) + 1) {
        if (encodetext != NULL) {
            ret = memset_s(encodetext, strlen(encodetext), 0, strlen(encodetext));
            securec_check(ret, "\0", "\0");
            OPENSSL_free(encodetext);
            encodetext = NULL;
        }
        ret = memset_s(src_plain_text, strlen(src_plain_text), 0, strlen(src_plain_text));
        securec_check(ret, "\0", "\0");
        ret = memset_s(ciphertext, sizeof(ciphertext), 0, sizeof(ciphertext));
        securec_check(ret, "\0", "\0");
        ret = memset_s(cipherkey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
        securec_check(ret, "\0", "\0");
        pfree_ext(src_plain_text);
        pfree_ext(cipherkey);
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                errmsg("Encrypt EC internal error: dest cipher length is too short.")));
    }

    /* Copy the encrypt string into the dest buffer */
    ret = memcpy_s(dest_cipher_text, dest_cipher_length, EC_ENCRYPT_PREFIX, strlen(EC_ENCRYPT_PREFIX));
    securec_check(ret, "\0", "\0");
    ret = memcpy_s(dest_cipher_text + strlen(EC_ENCRYPT_PREFIX),
        dest_cipher_length - strlen(EC_ENCRYPT_PREFIX),
        encodetext,
        strlen(encodetext) + 1);
    securec_check(ret, "\0", "\0");

    /* Clear buffer for safety's sake */
    ret = memset_s(encodetext, strlen(encodetext), 0, strlen(encodetext));
    securec_check(ret, "\0", "\0");
    ret = memset_s(ciphertext, sizeof(ciphertext), 0, sizeof(ciphertext));
    securec_check(ret, "\0", "\0");
    ret = memset_s(cipherkey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
    securec_check(ret, "\0", "\0");

    OPENSSL_free(encodetext);
    encodetext = NULL;
    pfree_ext(cipherkey);
}

/*
 * decryptECString:
 * 	Decrypt Extension Connector(EC) string(username/password, etc.) into plain text.
 * 	This function use aes128 to decrypt cipher text; key comes from certificate file
 *
 * @IN src_cipher_text: source cipher text to be decrypted
 * @OUT dest_plain_text: dest buffer to be filled with plain text, this buffer
 * 	should be given by caller
 * @IN dest_plain_length: dest buffer length which is given by the caller
 * @IN mode: key mode
 * @RETURN: bool, true if encrypt success, false if not
 */
bool decryptECString(const char* src_cipher_text, char* dest_plain_text,
                          uint32 dest_plain_length, int mode)
{
    GS_UCHAR* ciphertext = NULL;
    GS_UINT32 ciphertextlen = 0;
    GS_UCHAR* plaintext = NULL;
    GS_UINT32 plaintextlen = 0;
    GS_UCHAR* cipherkey = NULL;
    errno_t ret = EOK;

    if (NULL == src_cipher_text || (!IsECEncryptedString(src_cipher_text) && mode != HADR_MODE)) {
        return false;
    }

    /* Get key string */
    cipherkey = getECKeyString((KeyMode)mode);

    /* Step-1: Decode */
    if (mode != HADR_MODE) {
        src_cipher_text += strlen(EC_ENCRYPT_PREFIX);
    }
    ciphertext = (GS_UCHAR*)(SEC_decodeBase64(src_cipher_text, &ciphertextlen));
    plaintext = (GS_UCHAR*)palloc(ciphertextlen);
    ret = memset_s(plaintext, ciphertextlen, 0, ciphertextlen);
    securec_check(ret, "\0", "\0");

    /* Step-2: Decipher */
    if (!gs_decrypt_aes_128(ciphertext, ciphertextlen, cipherkey,
        (GS_UINT32)strlen((const char*)cipherkey), plaintext, &plaintextlen)) {
        ret = memset_s(plaintext, plaintextlen, 0, plaintextlen);
        securec_check(ret, "\0", "\0");
        ret = memset_s(cipherkey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
        securec_check(ret, "\0", "\0");
        ret = memset_s(ciphertext, ciphertextlen, 0, ciphertextlen);
        securec_check(ret, "\0", "\0");
        pfree_ext(plaintext);
        pfree_ext(cipherkey);
        OPENSSL_free(ciphertext);
        ciphertext = NULL;
        ereport(
            ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("decrypt the EC string failed!")));
    }

    /* Check dest buffer length */
    if (plaintextlen > dest_plain_length) {
        ret = memset_s(plaintext, plaintextlen, 0, plaintextlen);
        securec_check(ret, "\0", "\0");
        ret = memset_s(cipherkey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
        securec_check(ret, "\0", "\0");
        ret = memset_s(ciphertext, ciphertextlen, 0, ciphertextlen);
        securec_check(ret, "\0", "\0");
        pfree_ext(plaintext);
        pfree_ext(cipherkey);
        OPENSSL_free(ciphertext);
        ciphertext = NULL;
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                errmsg("Decrypt EC internal error: dest plain length is too short.")));
    }

    /* Copy the decrypted string into the dest buffer */
    ret = memcpy_s(dest_plain_text, dest_plain_length, plaintext, plaintextlen);
    securec_check(ret, "\0", "\0");

    /* Clear the buffer for safety's sake */
    ret = memset_s(plaintext, plaintextlen, 0, plaintextlen);
    securec_check(ret, "\0", "\0");
    ret = memset_s(cipherkey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
    securec_check(ret, "\0", "\0");
    ret = memset_s(ciphertext, ciphertextlen, 0, ciphertextlen);
    securec_check(ret, "\0", "\0");

    pfree_ext(plaintext);
    pfree_ext(cipherkey);
    OPENSSL_free(ciphertext);
    ciphertext = NULL;
    return true;
}

/*
 * IsECEncryptedString:
 * 	check the input string whether encypted
 *
 * @IN src_cipher_text: source cipher text to be decrypted
 * @RETURN: bool, true if encrypted, false if not
 */
bool IsECEncryptedString(const char* src_cipher_text)
{
    if (NULL == src_cipher_text || strlen(src_cipher_text) <= RANDOM_LEN + strlen(EC_ENCRYPT_PREFIX)) {
        return false;
    }

    if (0 == strncmp(src_cipher_text, EC_ENCRYPT_PREFIX, strlen(EC_ENCRYPT_PREFIX))) {
        return true;
    } else {
        return false;
    }
}

/*
 * EncryptGenericOptions:
 * 	Encrypt data source or foreign data wrapper generic options, before transformation
 *
 * @IN src_options: source options to be encrypted
 * @RETURN: void
 */
void EncryptGenericOptions(List* options, const char** sensitiveOptionsArray, int arrayLength, int mode)
{
    int i;
    char* srcString = NULL;
    char encryptString[EC_CIPHER_TEXT_LENGTH] = {0};
    errno_t ret;
    ListCell* cell = NULL;
    bool isPrint = false;

    foreach (cell, options) {
        DefElem* def = (DefElem*)lfirst(cell);
        Node* arg = def->arg;

        if (def->defaction == DEFELEM_DROP || def->arg == NULL || !IsA(def->arg, String))
            continue;

        /* Get src string to be encrypted */
        srcString = strVal(def->arg);
        /* For empty value, we do not encrypt */
        if (srcString == NULL || strlen(srcString) == 0)
            continue;

        for (i = 0; i < arrayLength; i++) {
            if (pg_strcasecmp(def->defname, sensitiveOptionsArray[i]) == 0) {
                /* For string with prefix='encryptOpt' (probably encrypted), we do not encrypt again */
                if (IsECEncryptedString(srcString)) {
                    /* We report warning only once in a CREATE/ALTER stmt. */
                    if (!isPrint) {
                        ereport(NOTICE,
                            (errmodule(MOD_EC),
                                errcode(ERRCODE_INVALID_PASSWORD),
                                errmsg("Using probably encrypted option (prefix='encryptOpt') directly and it is not "
                                       "recommended."),
                                errhint("The %s object can't be used if the option is not encrypted correctly.",
                                        GetCipherPrefix(mode))));
                        isPrint = true;
                    }
                    break;
                }

                /* Encrypt the src string */
                encryptECString(srcString, encryptString, EC_CIPHER_TEXT_LENGTH, mode);

                /* Substitute the src */
                def->arg = (Node*)makeString(pstrdup(encryptString));

                /* Clear the encrypted string */
                ret = memset_s(encryptString, sizeof(encryptString), 0, sizeof(encryptString));
                securec_check(ret, "\0", "\0");

                /* Clear the src string */
                ret = memset_s(srcString, strlen(srcString), 0, strlen(srcString));
                securec_check(ret, "\0", "\0");
                pfree_ext(srcString);
                pfree_ext(arg);

                break;
            }
        }
    }
}

/*
 * DecryptOptions:
 * 	Decrypt generic options, before transformation
 *
 * @IN options: source options to be decrypted
 * @IN sensitiveOptionsArray: options name to be decrypted
 * @IN arrayLength: array length of sensitiveOptionsArray
 * @IN mode: key mode
 * @RETURN: void
 */
void DecryptOptions(List *options, const char** sensitiveOptionsArray, int arrayLength, int mode)
{
    if (options == NULL) {
        return;
    }

    ListCell *cell = NULL;
    foreach(cell, options) {
        DefElem* def = (DefElem*)lfirst(cell);
        if (def->defname == NULL || def->arg == NULL || !IsA(def->arg, String)) {
            continue;
        }

        char *str = strVal(def->arg);
        if (str == NULL || strlen(str) == 0) {
            continue;
        }

        for (int i = 0; i < arrayLength; i++) {
            if (pg_strcasecmp(def->defname, sensitiveOptionsArray[i]) == 0) {
                char plainText[EC_CIPHER_TEXT_LENGTH] = {0};

                /*
                 * If decryptECString return false, it means the stored values is not encrypted.
                 * This happened when user mapping was created in old gaussdb version.
                 */
                if (decryptECString(str, plainText, EC_CIPHER_TEXT_LENGTH, mode)) {
                    pfree_ext(str);
                    pfree_ext(def->arg);
                    def->arg = (Node*)makeString(pstrdup(plainText));
                    
                    /* Clear buffer */
                    errno_t errCode = memset_s(plainText, EC_CIPHER_TEXT_LENGTH, 0, EC_CIPHER_TEXT_LENGTH);
                    securec_check(errCode, "\0", "\0");
                }

                break;
            }
        }
    }
}

char int2hex(uint8 n)
{
    char ret;
    if (n >= 0x0 && n <= 0x9) {
        ret = n + '0';
    } else if (n >= 0xa && n <= 0xf) {
        ret = (n - 0xa) + 'a';
    } else {
        ret = '0';
    }

    return ret;
}

uint8 hex2int(char ch)
{
    uint8 ret;
    if (ch >= '0' && ch <= '9') {
        ret = ch - '0';
    } else if (ch >= 'a' && ch <= 'f') {
        ret = (ch - 'a') + 10;
    } else if (ch >= 'A' && ch <= 'F') {
        ret = (ch - 'A') + 10;
    } else {
        ret = 0;
    }

    return ret;
}

int GetTdeCipherLenByAlog(uint8 algo)
{
    int cipher_len;
    switch (algo) {
        case TDE_ALGO_AES_128_CTR:
        case TDE_ALGO_SM4_CTR:
            cipher_len = DEK_CIPHER_AES128_LEN_PAGE;
            break;
        case TDE_ALGO_AES_256_CTR:
            cipher_len = DEK_CIPHER_AES256_LEN_PAGE;
            break;
        default:
            cipher_len = DEK_CIPHER_AES128_LEN_PAGE;
            break;
    }
    return cipher_len;
}

/*
 * Description:
 * Transform TdePageInfo to TdeInfo format
 * @IN tde_page_info: the tde info read from page
 * @OUT tde_info: used for encryption and decryption
 * @RETURN NONE
 */
void transformTdeInfoFromPage(TdeInfo* tde_info, TdePageInfo* tde_page_info)
{
    int i, j;
    int cipher_len;
    errno_t ret;
    const uint8 cmk_id_format[CMK_ID_LEN] = "1234abcd-1234-abcd-1234-1234abcd1234";

    cipher_len = GetTdeCipherLenByAlog(tde_page_info->algo);
    for (i = 0, j = 0; i < cipher_len && j < DEK_CIPHER_LEN - 1; i++, j += 2) {
        tde_info->dek_cipher[j] = int2hex(tde_page_info->dek_cipher[i] >> 4);
        tde_info->dek_cipher[j + 1] = int2hex(tde_page_info->dek_cipher[i] & 0x0f);
    }

    if (j < DEK_CIPHER_LEN - 1) {
        tde_info->dek_cipher[j] = '\0';
    } else {
        tde_info->dek_cipher[DEK_CIPHER_LEN - 1] = '\0';
    }

    for (i = 0, j = 0; i < CMK_ID_LEN_PAGE && j < CMK_ID_LEN - 1; j++) {
        if (cmk_id_format[j] == '-') {
            tde_info->cmk_id[j] = '-';
            continue;
        }
        tde_info->cmk_id[j] = int2hex(tde_page_info->cmk_id[i] >> 4);
        tde_info->cmk_id[j + 1] = int2hex(tde_page_info->cmk_id[i] & 0x0f);
        i++;
        j++;
    }

    /* here j should be 36 and check it for clearing warning */
    if (j < CMK_ID_LEN - 1) {
        tde_info->cmk_id[j] = '\0';
    } else {
        tde_info->cmk_id[CMK_ID_LEN - 1] = '\0';
    }

    tde_info->algo = tde_page_info->algo;
    ret = memcpy_s(tde_info->iv, RANDOM_IV_LEN, tde_page_info->iv, RANDOM_IV_LEN);
    securec_check(ret, "\0", "\0");
    ret = memcpy_s(tde_info->tag, GCM_TAG_LEN, tde_page_info->tag, GCM_TAG_LEN);
    securec_check(ret, "\0", "\0");
}

/*
 * Description:
 * To reduce storage space usage on page, tde_info.dek_cipher and tde_info.cmk_id formats are converted
 * @IN tde_info: used for encryption and decryption
 * @OUT tde_page_info: tde info to be written to page
 * @RETURN NONE
 */
void transformTdeInfoToPage(TdeInfo* tde_info, TdePageInfo* tde_page_info)
{
    int i, j;
    int cipher_len;
    errno_t ret;

    cipher_len = GetTdeCipherLenByAlog(tde_info->algo);
    for (i = 0, j = 0; i < cipher_len && j < DEK_CIPHER_LEN - 1; i++, j += 2) {
        tde_page_info->dek_cipher[i] = hex2int(tde_info->dek_cipher[j]) << 4 | hex2int(tde_info->dek_cipher[j + 1]);
    }

    for (i = 0, j = 0; i < CMK_ID_LEN_PAGE && j < CMK_ID_LEN - 1; j++) {
        if (tde_info->cmk_id[j] == '-') {
            continue;
        }
        tde_page_info->cmk_id[i] = hex2int(tde_info->cmk_id[j]) << 4 | hex2int(tde_info->cmk_id[j + 1]);
        i++;
        j++;
    }

    tde_page_info->algo = tde_info->algo;
    ret = memcpy_s(tde_page_info->iv, RANDOM_IV_LEN, tde_info->iv, RANDOM_IV_LEN);
    securec_check(ret, "\0", "\0");
    ret = memcpy_s(tde_page_info->tag, GCM_TAG_LEN, tde_info->tag, GCM_TAG_LEN);
    securec_check(ret, "\0", "\0");
}

/*
 * Description:
 * encrypt block or CU exclude header and filling,user data must encrypt,if block or cu encrypt failed,
 * retry three times, if it still failed then quit process, shoud use panic not fatal for result in hang
 * on checkpoint or bgwriter
 * @IN plain text and plain text length
 * @OUT cipher text and cipher text length
 * @RETURN: NONE
 */
void encryptBlockOrCUData(const char* plainText, const size_t plainLength,
    char* cipherText, size_t* cipherLength, TdeInfo* tdeInfo)
{
    int retryCnt = 3; /* retry at most three times when encrypt failed */
    unsigned char* iv = tdeInfo->iv;
    TdeAlgo algo = (TdeAlgo)tdeInfo->algo;
    unsigned char key[KEY_128BIT_LEN] = {0};
    const char* plain_key = NULL;
    unsigned int i, j;
    errno_t ret = 0;

#ifndef ENABLE_UT
    /* get dek plaintext by cmk_id and dek_cipher from TDE Key Manager */
    TDEKeyManager *tde_key_manager = New(CurrentMemoryContext) TDEKeyManager();
    tde_key_manager->init();
    plain_key = tde_key_manager->get_key(tdeInfo->cmk_id, tdeInfo->dek_cipher);
#else
    /* ugly hook for TDEKeyManager->get_key() function, fake_tde_get_key should be mocked in ut */
    plain_key = fake_tde_get_key(plain_key);
#endif
    Assert(strlen(plain_key) == KEY_128BIT_LEN * 2);

    /* transform tde plaintext from hex string to int array */
    for (i = 0, j = 0; i < KEY_128BIT_LEN && j < strlen(plain_key); i++, j += 2) {
        key[i] = hex2int(plain_key[j]) << 4 | hex2int(plain_key[j + 1]);
    }
#ifndef ENABLE_UT
    DELETE_EX2(tde_key_manager);
#endif

    do {
        if (encrypt_partial_mode(plainText, plainLength, cipherText, cipherLength, key, iv, algo)) {
            break;
        }
        retryCnt--;
    } while (retryCnt > 0);

    ret = memset_s(key, KEY_128BIT_LEN, 0, KEY_128BIT_LEN);
    securec_check(ret, "\0", "\0");

    if (retryCnt == 0) {
        ereport(PANIC,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("encrypt failed after retry three times"),
                errdetail("invalid arguments or internal error")));
    }
}

/*
 * Description:
 * decrypt block or CU exclude header and filling,user data must decrypt, if block or cu decrypt failed,
 * retry three times, if it still failed then quit process, shoud use panic not fatal for result in hang
 * on autovacuum
 * @IN cipher text and cipher text length
 * @OUT plain text and plain text length
 * @RETURN: NONE
 */
void decryptBlockOrCUData(const char* cipherText, const size_t cipherLength,
    char* plainText, size_t* plainLength, TdeInfo* tdeInfo)
{
    int retryCnt = 3; /* retry at most three times when decrypt failed */
    unsigned char* iv = tdeInfo->iv;
    TdeAlgo algo = (TdeAlgo)tdeInfo->algo;
    unsigned char key[KEY_128BIT_LEN] = {0};
    const char* plain_key = NULL;
    unsigned int i, j;
    errno_t ret = 0;

#ifndef ENABLE_UT
    /* get dek plaintext by cmk_id and dek_cipher from TDE Key Manager */
    TDEKeyManager *tde_key_manager = New(CurrentMemoryContext) TDEKeyManager();
    tde_key_manager->init();
    plain_key = tde_key_manager->get_key(tdeInfo->cmk_id, tdeInfo->dek_cipher);
#else
    /* ugly hook for TDEKeyManager->get_key() function, fake_tde_get_key should be mocked in ut */
    plain_key = fake_tde_get_key(plain_key);
#endif
    Assert(strlen(plain_key) == KEY_128BIT_LEN * 2);

    /* transform tde plaintext from hex string to int array */
    for (i = 0, j = 0; i < KEY_128BIT_LEN && j < strlen(plain_key); i++, j += 2) {
        key[i] = hex2int(plain_key[j]) << 4 | hex2int(plain_key[j + 1]);
    }
#ifndef ENABLE_UT
    DELETE_EX2(tde_key_manager);
#endif

    do {
        if (decrypt_partial_mode(cipherText, cipherLength, plainText, plainLength, key, iv, algo)) {
            break;
        }
        retryCnt--;
    } while (retryCnt > 0);

    ret = memset_s(key, KEY_128BIT_LEN, 0, KEY_128BIT_LEN);
    securec_check(ret, "\0", "\0");

    if (retryCnt == 0) {
        ereport(PANIC,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("decrypt failed after retry three times"),
                errdetail("invalid arguments or internal error")));
    }
}

static void do_advice(void)
{
    (void)printf(_("Try \"%s --help\" for more information.\n"), pgname);
}

static void do_help(void)
{
    (void)printf(_("%s is an inferface to encrypt input plaintext string.\n\n"), pgname);
    (void)printf(_("Usage:\n"));
    (void)printf(_("  %s [OPTION]... PLAINTEXT\n"), pgname);
    (void)printf(_("\nGeneral options:\n"));
    (void)printf(_("  -?, --help                        show this help, then exit.\n"));
    (void)printf(_("  -V, --version                     output version information, then exit.\n"));
    (void)printf(_("  -k, --key=PASSWORD                the password for AES128.\n"));
    (void)printf(_("  -v, --vector=VectorValue          the random vector for AES128.\n"));
    (void)printf(_("  -f, --file-prefix=FilePrefix      the cipher files prefix.\n"));
    (void)printf(_("  -B, --key-base64=Value            the key value encoded in base64.\n"));
    (void)printf(_("  -D, --vector-base64=Value         the random value encoded in base64.\n"));
    (void)printf(_("  PLAINTEXT                         the plain text you want to encrypt.\n"));
}

/*
 * @@GaussDB@@
 * Brief            : IsLegalPreix()
 * Input            : prefixStr -> the input parameter value
 * Description      : check the parameter. It is only support digit/alpha/'-'/'_'
 */
bool IsLegalPreix(const char* prefixStr)
{
    int NBytes = int(strlen(prefixStr));
    for (int i = 0; i < NBytes; i++) {
        /* check whether the character is correct */
        if (IsIllegalCharacter(prefixStr[i])) {
            return false;
        }
    }
    return true;
}

static void free_global_value(void)
{
    GS_FREE(g_prefix);
    if (g_key != NULL) {
        errno_t rc = memset_s(g_key, strlen(g_key), 0, strlen(g_key));
        securec_check_c(rc, "\0", "\0");
    }
    GS_FREE(g_key);
    GS_FREE(g_vector);
}

static bool is_prefix_in_key_mode(const char* mode)
{
    int slen = strlen("server");
    int clen = strlen("client");
    int srclen = strlen("source");
    int obslen = strlen("obsserver");

    if ((0 == strncmp(mode, "server", slen) && '\0' == mode[slen]) ||
        (0 == strncmp(mode, "client", clen) && '\0' == mode[clen]) ||
        (0 == strncmp(mode, "source", srclen) && '\0' == mode[srclen]) ||
        (0 == strncmp(mode, "obsserver", obslen) && '\0' == mode[obslen])) {
        return true;
    } else {
        return false;
    }
}

#define CIPHERTEXT_LEN 512
int encrypte_main(int argc, char* const argv[])
{

#define MAXLOGINFOLEN 1024
    static struct option long_options[] = {{"help", no_argument, NULL, '?'},
        {"version", no_argument, NULL, 'V'},
        {"file-prefix", required_argument, NULL, 'f'},
        {"key", required_argument, NULL, 'k'},
        {"vector", required_argument, NULL, 'v'},
        {"key-base64", required_argument, NULL, 'B'},
        {"vector-base64", required_argument, NULL, 'D'},
        {NULL, 0, NULL, 0}};

    int ret = 0;
    int result = 0;

    GS_UCHAR* plaintext = NULL;
    GS_UCHAR* ciphertext = NULL;
    GS_UINT32 ciphertextlen = 0;
    char* encodetext = NULL;
    GS_UCHAR* decipherkey = NULL;
    char* cipherpath = NULL;
    char cipherkeyfile[MAXPGPATH] = {'\0'};
    char destciphertext[CIPHERTEXT_LEN] = {0};
    char isexistscipherkeyfile[MAXPGPATH] = {'\0'};
    char isexistsrandfile[MAXPGPATH] = {'\0'};

    int c = 0;
    int optindex = 0;
    errno_t rc = 0;

    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("gs_encrypt"));

    /* support --help and --version even if invoked as root */
    if (argc > 1) {
        if (strncmp(argv[1], "--help", sizeof("--help")) == 0 || strncmp(argv[1], "-?", sizeof("-?")) == 0) {
            do_help();
            exit(0);
        } else if (strncmp(argv[1], "-V", sizeof("-V")) == 0 ||
                   strncmp(argv[1], "--version", sizeof("--version")) == 0) {
            puts("gs_encrypt " DEF_GS_VERSION);
            exit(0);
        }
    }

    /* process command-line options */
    while ((c = getopt_long(argc, argv, "f:v:k:B:D:", long_options, &optindex)) != -1) {
        switch (c) {
            case 'f': {
                GS_FREE(g_prefix);
                g_prefix = gs_strdup(optarg);
                if (0 == strlen(g_prefix) || !IsLegalPreix(g_prefix)) {
                    (void)fprintf(stderr, _("%s: -f only be formed of 'a~z', 'A~Z', '0~9', '-', '_'\n"), pgname);
                    free_global_value();
                    do_advice();
                    exit(1);
                }
                break;
            }
            case 'v': {
                GS_FREE(g_vector);
                g_vector = gs_strdup(optarg);
                // the length of g_vector must equal to 16
                if (strlen(g_vector) != RANDOM_LEN) {
                    (void)fprintf(stderr, _("%s: the length of -v must equal to %d\n"), pgname, RANDOM_LEN);
                    free_global_value();
                    do_advice();
                    exit(1);
                }
                g_vector_len = (int)strlen(g_vector);
                break;
            }
            case 'k': {
                if (g_key != NULL) {
                    rc = memset_s(g_key, strlen(g_key), 0, strlen(g_key));
                    securec_check_c(rc, "\0", "\0");
                }
                GS_FREE(g_key);
                g_key = gs_strdup(optarg);
                if (0 == strlen(optarg) || !mask_single_passwd(optarg)) {
                    (void)fprintf(stderr, _("%s: mask passwd failed. optarg is null or out of memory!\n"), pgname);
                    free_global_value();
                    do_advice();
                    exit(1);
                }
                g_key_len = (int)strlen(g_key);
                break;
            }
            case 'B': {
                GS_UCHAR* decoded_key = NULL;
                GS_UINT32 decodelen = 0;
                if (g_key != NULL) {
                    rc = memset_s(g_key, strlen(g_key), 0, strlen(g_key));
                    securec_check_c(rc, "\0", "\0");
                }
                GS_FREE(g_key);

                decoded_key = (GS_UCHAR*)SEC_decodeBase64(optarg, &decodelen);

                if (0 == strlen(optarg) || !mask_single_passwd(optarg)) {
                    (void)fprintf(stderr, _("%s: mask base64 encoded key failed. optarg is null or out of memory!\n"), 
                        pgname);
                    free_global_value();
                    do_advice();
                    exit(1);
                }

                if (decoded_key == NULL || decodelen == 0) {
                    (void)fprintf(stderr, _("%s: failed to decode base64 encoded key.\n"), pgname);
                    free_global_value();
                    do_advice();
                    exit(1);
                }

                g_key = gs_strdup((char *)decoded_key);
                OPENSSL_free(decoded_key);
                decoded_key = NULL;
                g_key_len = (int)decodelen;
                break;
            }
            case 'D': {
                GS_UCHAR* decoded_vector = NULL;
                GS_UINT32 decodelen = 0;
                GS_FREE(g_vector);

                decoded_vector = (GS_UCHAR*)SEC_decodeBase64(optarg, &decodelen);

                if (decoded_vector == NULL || decodelen == 0) {
                    (void)fprintf(stderr, _("%s: failed to decode base64 encoded vector.\n"), pgname);
                    free_global_value();
                    do_advice();
                    exit(1);
                }
                // the length of g_vector must equal to 16
                if (decodelen != RANDOM_LEN) {
                    (void)fprintf(
                        stderr, _("%s: the decoded length of vector must be equal to %d.\n"), pgname, RANDOM_LEN);
                    free_global_value();
                    do_advice();
                    exit(1);
                }
                g_vector = gs_strdup((char *)decoded_vector);
                OPENSSL_free(decoded_vector);
                decoded_vector = NULL;
                g_vector_len = (int)decodelen;
                break;
            }
            default:
                do_help();
                exit(1);
        }
    }

    /* Get plaintext from command line */
    if (optind < argc) {
        plaintext = (GS_UCHAR*)gs_strdup(argv[optind]);
        if (!mask_single_passwd(argv[optind])) {
            (void)fprintf(stderr, _("%s: mask plaintext failed. optarg is null, or out of memory!\n"), pgname);
            free_global_value();
            do_advice();
            exit(1);
        }
        optind++;
    }

    /* Complain if any arguments remain */
    if (optind < argc) {
        (void)fprintf(stderr, _("%s: too many command-line arguments (first is \"%s\")\n"), pgname, argv[optind]);
        (void)fprintf(stderr, _("Try \"%s --help\" for more information.\n"), pgname);
        free_global_value();
        GS_FREE(plaintext);
        exit(1);
    }

    if (NULL == plaintext || 0 == strlen((char*)plaintext)) {
        (void)fprintf(stderr, _("%s: options PLAINTEXT must be input and it is not null\n"), pgname);
        free_global_value();
        GS_FREE(plaintext);
        exit(1);
    }

    /* check the parameter value */
    if ((NULL == g_key && NULL != g_vector) || (NULL != g_key && NULL == g_vector)) {
        (void)fprintf(stderr, _("%s: key and vector should be both specified.\n"), pgname);
        free_global_value();
        GS_FREE(plaintext);
        exit(1);
    }

    if (NULL != g_prefix && (NULL != g_key || NULL != g_vector)) {
        (void)fprintf(stderr, _("%s: key/vector and cipher files cannot be used together.\n"), pgname);
        free_global_value();
        GS_FREE(plaintext);
        exit(1);
    }

    if (g_key != NULL && !check_input_password(g_key)) {
        (void)fprintf(stderr, _("%s: The input key must be %d~%d bytes and "
            "contain at least three kinds of characters!\n"),
            pgname, MIN_KEY_LEN, MAX_KEY_LEN);
        free_global_value();
        GS_FREE(plaintext);
        exit(1);
    }

    cipherpath = getGaussHome();
    if (cipherpath == NULL) {
        (void)fprintf(stderr, _("get environment of GAUSSHOME failed.\n"));
        free_global_value();
        GS_FREE(plaintext);
        return -1;
    }

    char* tmp_cipherpath = (char*)malloc(strlen(cipherpath) + 1);
    if (tmp_cipherpath == NULL) {
        (void)fprintf(stderr, _("memory is temporarily unavailable.\n"));
        free_global_value();
        GS_FREE(plaintext);
        return -1;
    }

    rc = strcpy_s(tmp_cipherpath, strlen(cipherpath) + 1, cipherpath);
    securec_check(rc, "\0", "\0");

    ret = snprintf_s(cipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/bin", cipherpath);
    securec_check_ss(ret, "\0", "\0");
    /* set the g_prefix default value */
    if (NULL != g_prefix) {
        ret = snprintf_s(
            isexistscipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/bin/%s.key.cipher", tmp_cipherpath, g_prefix);
        securec_check_ss_c(ret, "\0", "\0");
        ret = snprintf_s(isexistsrandfile, MAXPGPATH, MAXPGPATH - 1, "%s/bin/%s.key.rand", tmp_cipherpath, g_prefix);
        securec_check_ss_c(ret, "\0", "\0");

        if (!file_exists(isexistscipherkeyfile) || !file_exists(isexistsrandfile)) {
            (void)fprintf(stderr,
                _("%s: options -f is not correct. Failed to get cipher file[%s] and random file[%s]\n"),
                pgname,
                isexistscipherkeyfile,
                isexistsrandfile);
            free(tmp_cipherpath);
            tmp_cipherpath = NULL;
            free_global_value();
            GS_FREE(plaintext);
            exit(1);
        }
    } else {
        ret =
            snprintf_s(isexistscipherkeyfile, MAXPGPATH, MAXPGPATH - 1, "%s/bin/obsserver.key.cipher", tmp_cipherpath);
        securec_check_ss_c(ret, "\0", "\0");
    }

    free(tmp_cipherpath);
    tmp_cipherpath = NULL;
    cipherpath = NULL;

    ciphertext = (GS_UCHAR*)malloc(RANDOM_LEN + 1);
    if (ciphertext == NULL) {
        (void)fprintf(stderr, _("memory is temporarily unavailable.\n"));
        result = -1;
        goto ENCRYPTE_MAIN_EXIT;
    }

    ret = memset_s(ciphertext, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
    securec_check(ret, "\0", "\0");

    /* default branch should do decode */
    /* -f is in key mode */

    if ((NULL == g_prefix && NULL == g_key && NULL == g_vector) ||
        (NULL != g_prefix && is_prefix_in_key_mode((const char*)g_prefix))) {
        /* get key from file */
        if (file_exists(isexistscipherkeyfile)) {
            decode_cipher_files(OBS_MODE, NULL, cipherkeyfile, ciphertext);
        } else {
            decode_cipher_files(SERVER_MODE, NULL, cipherkeyfile, ciphertext, true);
        }
    }

    decipherkey = ciphertext;
    ciphertext = (GS_UCHAR*)malloc(CIPHERTEXT_LEN);
    if (ciphertext == NULL) {
        (void)fprintf(stderr, _("memory is temporarily unavailable.\n"));
        result = -1;
        goto ENCRYPTE_MAIN_EXIT;
    }

    ret = memset_s(ciphertext, CIPHERTEXT_LEN, 0, CIPHERTEXT_LEN);
    securec_check(ret, "\0", "\0");
    ciphertextlen = CIPHERTEXT_LEN;
    if (!gs_encrypt_aes_128((GS_UCHAR*)plaintext, decipherkey,
        (GS_UINT32)strlen((const char*)decipherkey), ciphertext, &ciphertextlen)) {
        (void)fprintf(stderr, _("Encrypt input text failed.\n"));
        result = -1;
        goto ENCRYPTE_MAIN_EXIT;
    }

    encodetext = SEC_encodeBase64((char*)ciphertext, ciphertextlen + RANDOM_LEN);
    if (NULL == encodetext) {
        (void)fprintf(stderr, _("Encrypt input text internal error.\n"));
        result = -1;
        goto ENCRYPTE_MAIN_EXIT;
    }

    if (CIPHERTEXT_LEN < strlen(encodetext) + 1) {
        (void)fprintf(stderr, _("Encrypt input text internal error.\n"));
        result = -1;
        goto ENCRYPTE_MAIN_EXIT;
    }

    ret = memcpy_s(destciphertext, CIPHERTEXT_LEN, encodetext, (strlen(encodetext) + 1));
    securec_check(ret, "\0", "\0");

#ifndef ENABLE_UT
    (void)fprintf(stdout, _("%s\n"), destciphertext);
#endif

ENCRYPTE_MAIN_EXIT:
    /* clean all footprint for security. */
    if (encodetext != NULL) {
        ret = memset_s(encodetext, strlen(encodetext) + 1, 0, strlen(encodetext) + 1);
        securec_check(ret, "\0", "\0");
        OPENSSL_free(encodetext);
        encodetext = NULL;
    }

    if (ciphertext != NULL) {
        ret = memset_s(ciphertext, CIPHERTEXT_LEN, 0, CIPHERTEXT_LEN);
        securec_check(ret, "\0", "\0");
        free(ciphertext);
        ciphertext = NULL;
    }

    if (decipherkey != NULL) {
        ret = memset_s(decipherkey, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
        securec_check(ret, "\0", "\0");
        free(decipherkey);
        decipherkey = NULL;
    }

    free_global_value();
    GS_FREE(plaintext);

    return result;
}

/*
 * Read key and vector from cipher key and random file.
 * key:    must be with length of CIPHER_LEN + 1
 * vector: must be with length of CIPHER_LEN + 1.
 *         Which is the random of cipherkeyfile.
 */
bool getKeyVectorFromCipherFile(const char* cipherkeyfile, const char* cipherrndfile, GS_UCHAR* key, GS_UCHAR* vector)
{
    GS_UINT32 plainlen = 0;
    RandkeyFile rand_file_content;
    CipherkeyFile cipher_file_content;
    int ret = 0;

    if (!ReadContentFromFile(cipherkeyfile, &cipher_file_content, sizeof(cipher_file_content)) ||
        !ReadContentFromFile(cipherrndfile, &rand_file_content, sizeof(rand_file_content))) {
        ClearCipherKeyFile(&cipher_file_content);
        ClearRandKeyFile(&rand_file_content);
        (void)fprintf(stderr,
            _("Failed to read contents of cipher files of transparent encryption: %s, %s.\n"),
            cipherkeyfile,
            cipherrndfile);
        return false;
    }

    if (!CipherFileIsValid(&cipher_file_content) || !RandFileIsValid(&rand_file_content)) {
        ClearCipherKeyFile(&cipher_file_content);
        ClearRandKeyFile(&rand_file_content);
        (void)fprintf(
            stderr, _("Corrupt cipher files of transparent encryption: %s, %s.\n"), cipherkeyfile, cipherrndfile);
        return false;
    }

    if (!DecryptInputKey(cipher_file_content.cipherkey, CIPHER_LEN, rand_file_content.randkey,
        cipher_file_content.key_salt, cipher_file_content.vector_salt, key, &plainlen)) {
        ClearCipherKeyFile(&cipher_file_content);
        ClearRandKeyFile(&rand_file_content);
        return false;
    }

    ret = memcpy_s(
        vector, sizeof(rand_file_content.randkey), rand_file_content.randkey, sizeof(rand_file_content.randkey));
    securec_check_c(ret, "", "");
    return true;
}

/* Get $GAUSSHOME, and check if any injection risk in it.
 * If any, throw a PANIC error.
 */
char* getGaussHome()
{
    char* tmp = NULL;
    char* danger_token[] = {";", "`", "\\", "'", "\"", ">", "<", "$", "&", "|", "!", "\n", NULL};
    const int MAX_GAUSSHOME_LEN = 4096;

    tmp = gs_getenv_r("GAUSSHOME");
    char real_gausshome[PATH_MAX + 1] = {'\0'};
    if (tmp == NULL || realpath(tmp, real_gausshome) == NULL) {
        (void)fprintf(stderr, _("Get environment of GAUSSHOME failed.\n"));
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                errmsg("Get environment of GAUSSHOME failed.\n")));
        return NULL;
    }
    tmp = real_gausshome;

    if (strlen(tmp) > MAX_GAUSSHOME_LEN) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("environment of GAUSSHOME is too long.\n")));
        return NULL;
    }

    char* tmp_tmp = (char*)palloc(strlen(tmp) + 1);
    errno_t rc = strcpy_s(tmp_tmp, strlen(tmp) + 1, tmp);
    securec_check(rc, "\0", "\0");

    if ('\0' == *tmp_tmp) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("$GAUSSHOME is not set or it's NULL.\n")));
        return NULL;
    }
    for (int i = 0; danger_token[i] != NULL; ++i) {
        if (strstr(tmp_tmp, danger_token[i])) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Invalid character '%s' in $GAUSSHOME.\n", danger_token[i])));
            return NULL;
        }
    }

    return tmp_tmp;
}

static bool gs_encrypt_sm4_function(FunctionCallInfo fcinfo, text** outtext)
{
    char* key = NULL;
    GS_UINT32 keylen = 0;
    char* plaintext = NULL;
    GS_UINT32 plaintextlen = 0;

    char* ciphertext = NULL;
    char* encodestring = NULL;
    GS_UINT32 encodetextlen = 0;
    GS_UINT32 ciphertextlenmax = 0;
    GS_UCHAR user_key[SM4_KEY_LENGTH];
    GS_UCHAR userIv[SM4_IV_LENGTH] = {0};
    errno_t errorno = EOK;
    GS_UINT32 ret = 0;
    size_t cipherLength = 0;

    key = (text_to_cstring(PG_GETARG_TEXT_P(1)));
    keylen = strlen((const char*)key);

    if (!check_input_password(key)) {
        errorno = memset_s(key, keylen, '\0', keylen);
        securec_check(errorno, "\0", "\0");
        pfree_ext(key);
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                errmsg("The encryption key must be %d~%d bytes and contain at least three kinds of characters!",
                MIN_KEY_LEN, SM4_KEY_LENGTH)));
    }
    errorno = memcpy_s(user_key, SM4_KEY_LENGTH, (GS_UCHAR*)key, keylen);
    securec_check(errorno, "\0", "\0");

    if (keylen < SM4_KEY_LENGTH) {
        errorno = memset_s(user_key + keylen, SM4_KEY_LENGTH - keylen, '\0', SM4_KEY_LENGTH - keylen);
        securec_check(errorno, "\0", "\0");
    }

    errorno = memset_s(key, keylen, '\0', keylen);
    securec_check(errorno, "\0", "\0");
    pfree_ext(key);

    ret = RAND_priv_bytes(userIv, SM4_IV_LENGTH);
    if (ret != 1) {
        errorno = memset_s(userIv, SM4_IV_LENGTH, '\0', SM4_IV_LENGTH);
        securec_check(errorno, "", "");
        ereport(ERROR, (errmsg("generate random sm4 vector failed,errcode:%d", ret)));
    }

    /*
     * Calculate the max length of ciphertext:
     */
    plaintext = (char*)(text_to_cstring(PG_GETARG_TEXT_P(0)));
    plaintextlen = strlen((const char*)plaintext);

    ciphertextlenmax = plaintextlen + SM4_IV_LENGTH + SM4_BLOCK_SIZE - 1;
    ciphertext = (char*)palloc(ciphertextlenmax);
    errorno = memset_s(ciphertext, ciphertextlenmax, '\0', ciphertextlenmax);
    securec_check(errorno, "\0", "\0");
    errorno = memcpy_s(ciphertext, SM4_IV_LENGTH, userIv, SM4_IV_LENGTH);
    securec_check(errorno, "\0", "\0");

    ret = encrypt_partial_mode(
        plaintext, plaintextlen, ciphertext + SM4_IV_LENGTH, &cipherLength, user_key, userIv, TDE_ALGO_SM4_CTR);
    errorno = memset_s(plaintext, plaintextlen, '\0', plaintextlen);
    securec_check(errorno, "\0", "\0");
    pfree_ext(plaintext);
    if (ret !=  true) {
        errorno = memset_s(ciphertext, cipherLength, '\0', cipherLength);
        securec_check(errorno, "\0", "\0");
        pfree_ext(ciphertext);
        ereport(ERROR,
            (errmsg("sm4 encrypt fail"),
                errdetail("sm4_ctr_enc_partial_mode fail")));
    }

    cipherLength = cipherLength + SM4_IV_LENGTH;
    /* encode the ciphertext for nice show and decrypt operation */
    encodestring = SEC_encodeBase64((const char*)ciphertext, cipherLength);
    errorno = memset_s(ciphertext, cipherLength, '\0', cipherLength);
    securec_check(errorno, "\0", "\0");
    pfree_ext(ciphertext);

    if (encodestring == NULL) {
        ciphertextlenmax = 0;
        ereport(
            ERROR, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("encode the plain text failed!")));
    }
    encodetextlen = strlen((const char*)encodestring);

    *outtext = cstring_to_text((const char*)encodestring);
    errorno = memset_s(encodestring, encodetextlen, '\0', encodetextlen);
    securec_check(errorno, "\0", "\0");
    OPENSSL_free(encodestring);
    encodestring = NULL;

    return true;
}

static bool gs_decrypt_sm4_function(FunctionCallInfo fcinfo, text** outtext)
{
    char* key = NULL;
    GS_UINT32 keylen = 0;
    char* ciphertext = NULL;
    GS_UINT32 ciphertextlen = 0;
    GS_UINT32 cipherpartlen;
    char* encodetext = NULL;
    char* cipherpart = NULL;
    GS_UCHAR* decodetext = NULL;
    GS_UINT32 ret = 0;

    GS_UINT32 decodetextlen = 0;
    GS_UINT32 ciphertextlenmax = 0;
    GS_UCHAR userkey[SM4_KEY_LENGTH];
    GS_UCHAR userIv[SM4_IV_LENGTH] = {0};

    errno_t errorno = EOK;
    size_t encodetextlen = 0;

    key = (char*)(text_to_cstring(PG_GETARG_TEXT_P(1)));
    keylen = strlen((const char*)key);

    if (!check_input_password(key)) {
        errorno = memset_s(key, keylen, '\0', keylen);
        securec_check(errorno, "\0", "\0");
        pfree_ext(key);
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                errmsg("The decryption key must be %d~%d bytes and contain at least three kinds of characters!",
                MIN_KEY_LEN, SM4_KEY_LENGTH)));
    }
    errorno = memcpy_s(userkey, SM4_KEY_LENGTH, (GS_UCHAR*)key, keylen);
    securec_check(errorno, "\0", "\0");

    if (keylen < SM4_KEY_LENGTH) {
        errorno = memset_s(userkey + keylen, SM4_KEY_LENGTH - keylen, '\0', SM4_KEY_LENGTH - keylen);
        securec_check(errorno, "\0", "\0");
    }

    errorno = memset_s(key, keylen, '\0', keylen);
    securec_check(errorno, "\0", "\0");
    pfree_ext(key);
    decodetext = (GS_UCHAR*)(text_to_cstring(PG_GETARG_TEXT_P(0)));
    decodetextlen = strlen((const char*)decodetext);
    ciphertext = (char*)(SEC_decodeBase64((const char*)decodetext, &ciphertextlen));
    if ((ciphertext == NULL) || (ciphertextlen < (SM4_IV_LENGTH + 1))) {
        if (ciphertext != NULL) {
            OPENSSL_free(ciphertext);
            ciphertext = NULL;
        }
        errorno = memset_s(decodetext, decodetextlen, '\0', decodetextlen);
        securec_check(errorno, "\0", "\0");
        pfree_ext(decodetext);

        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                errmsg("Decode the cipher text failed or the ciphertext is wrong!")));
    }

    errorno = memset_s(decodetext, decodetextlen, '\0', decodetextlen);
    securec_check(errorno, "\0", "\0");
    pfree_ext(decodetext);

    errorno = memcpy_s(userIv, SM4_IV_LENGTH, ciphertext, SM4_IV_LENGTH);
    securec_check(errorno, "\0", "\0");

    ciphertextlenmax = ciphertextlen + SM4_BLOCK_SIZE - 1;
    encodetext = (char*)palloc(ciphertextlenmax);
    errorno = memset_s(encodetext, ciphertextlenmax, '\0', ciphertextlenmax);
    securec_check(errorno, "\0", "\0");

    cipherpartlen = ciphertextlen - SM4_IV_LENGTH;
    cipherpart = (char*)palloc(cipherpartlen + 1);
    errorno = memcpy_s(cipherpart, cipherpartlen + 1, ciphertext + SM4_IV_LENGTH, cipherpartlen);
    securec_check(errorno, "\0", "\0");
    errorno = memset_s(ciphertext, ciphertextlen, '\0', ciphertextlen);
    securec_check(errorno, "\0", "\0");
    OPENSSL_free(ciphertext);
    ciphertext = NULL;	

    ret = decrypt_partial_mode(
        cipherpart, cipherpartlen, encodetext, &encodetextlen, userkey, userIv, TDE_ALGO_SM4_CTR);
    if (ret !=  true) {
        errorno = memset_s(cipherpart, cipherpartlen + 1, '\0', cipherpartlen + 1);
        securec_check(errorno, "\0", "\0");
        pfree_ext(cipherpart);
        errorno = memset_s(encodetext, encodetextlen, '\0', encodetextlen);
        securec_check(errorno, "\0", "\0");
        pfree_ext(encodetext);
        ereport(ERROR,
            (errmsg("sm4 decrypt fail"),
                errdetail("sm4_ctr_dec_partial_mode fail")));
    }

    errorno = memset_s(cipherpart, cipherpartlen + 1, '\0', cipherpartlen + 1);
    securec_check(errorno, "\0", "\0");
    pfree_ext(cipherpart);

    encodetextlen = strlen((const char*)encodetext);

    *outtext = cstring_to_text((char*)encodetext);
    errorno = memset_s(encodetext, encodetextlen, '\0', encodetextlen);
    securec_check(errorno, "\0", "\0");
    pfree_ext(encodetext);
    encodetext = NULL;

    return true;
}

Datum gs_encrypt(PG_FUNCTION_ARGS)
{
    GS_UCHAR* encrypttype = NULL;
    text* outtext = NULL;
    bool status = false;

    /* check input paramaters */
    if (PG_ARGISNULL(0)) {
        fcinfo->isnull = true;
        outtext = NULL;
        PG_RETURN_TEXT_P(outtext);
    }

    if (PG_ARGISNULL(1)) {
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("The encryption key can not be empty!")));
    }

    if (PG_ARGISNULL(2)) {
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("The encryption type can not be empty!")));
    }

    encrypttype = (GS_UCHAR*)(text_to_cstring(PG_GETARG_TEXT_P(2)));
    if (strcmp((const char*)encrypttype, ENCRYPT_TYPE_SM4) == 0) {
        status = gs_encrypt_sm4_function(fcinfo, &outtext);
    } else if (strcmp((const char*)encrypttype, ENCRYPT_TYPE_AES128) == 0) {
        status = gs_encrypt_aes128_function(fcinfo, &outtext);
    } else {
        pfree_ext(encrypttype);
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                errmsg("Encryption type is wrong!")));

    }
    pfree_ext(encrypttype);

    if (!status) {
        outtext = NULL;
    }
    PG_RETURN_TEXT_P(outtext);
}

Datum gs_decrypt(PG_FUNCTION_ARGS)
{
    GS_UCHAR* decrypttype = NULL;
    text* outtext = NULL;
    bool status = false;
    /* check input paramaters */
    if (PG_ARGISNULL(0)) {
        fcinfo->isnull = true;
        outtext = NULL;
        PG_RETURN_TEXT_P(outtext);
    }

    if (PG_ARGISNULL(1)) {
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("The decryption key can not be empty!")));
    }

    if (PG_ARGISNULL(2)) {
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("The decryption type can not be empty!")));
    }

    decrypttype = (GS_UCHAR*)(text_to_cstring(PG_GETARG_TEXT_P(2)));

    if (strcmp((const char*)decrypttype, ENCRYPT_TYPE_SM4) == 0) {
        status = gs_decrypt_sm4_function(fcinfo, &outtext);
    } else if (strcmp((const char*)decrypttype, ENCRYPT_TYPE_AES128) == 0) {
        status = gs_decrypt_aes128_function(fcinfo, &outtext);
    } else {
        pfree_ext(decrypttype);
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                errmsg("Decryption type  is wrong!")));

    }
    pfree_ext(decrypttype);

    if (!status) {
        outtext = NULL;
    }
    PG_RETURN_TEXT_P(outtext);
}

Datum gs_encrypt_aes128(PG_FUNCTION_ARGS)
{
    text* outtext = NULL;
    bool status = false;
    /* check input paramaters */
    if (PG_ARGISNULL(0)) {
        fcinfo->isnull = true;
        outtext = NULL;
        PG_RETURN_TEXT_P(outtext);
    }

    if (PG_ARGISNULL(1)) {
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("The encryption key can not be empty!")));
    }
    status = gs_encrypt_aes128_function(fcinfo, &outtext);
    if (!status) {
        outtext = NULL;
    }
    PG_RETURN_TEXT_P(outtext);

}

Datum gs_decrypt_aes128(PG_FUNCTION_ARGS)
{
    text* outtext = NULL;
    bool status = false;
    /* check input paramaters */
    if (PG_ARGISNULL(0)) {
        fcinfo->isnull = true;
        outtext = NULL;
        PG_RETURN_TEXT_P(outtext);
    }
    if (PG_ARGISNULL(1)) {
        ereport(ERROR,
            (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION), errmsg("The decryption key can not be empty!")));
    }

    status = gs_decrypt_aes128_function(fcinfo, &outtext);
    if (!status) {
        outtext = NULL;
    }
    PG_RETURN_TEXT_P(outtext);
}
