/*
* Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * encrypt_decrypt.c
 *	  encrypt and decrypt functions.
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/encrypt_decrypt.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "utils/builtins.h"
#include "utils/guc.h"

#include "openssl/evp.h"
#include "openssl/err.h"

static int g_aes_block_mode_key_size[] = {128, 192, 256};
#define NUM_KEY_SIZE 3
#define BYTE_SIZE 8
#define AES_INIT_VECTOR_SIZE 16

static const EVP_CIPHER *get_cipher(AesBlockMode mode)
{
    switch (mode) {
        case AES_128_CBC:
            return EVP_aes_128_cbc();
        case AES_192_CBC:
            return EVP_aes_192_cbc();
        case AES_256_CBC:
            return EVP_aes_256_cbc();
        case AES_128_CFB1:
            return EVP_aes_128_cfb1();
        case AES_192_CFB1:
            return EVP_aes_192_cfb1();
        case AES_256_CFB1:
            return EVP_aes_256_cfb1();
        case AES_128_CFB8:
            return EVP_aes_128_cfb8();
        case AES_192_CFB8:
            return EVP_aes_192_cfb8();
        case AES_256_CFB8:
            return EVP_aes_256_cfb8();
        case AES_128_CFB128:
            return EVP_aes_128_cfb128();
        case AES_192_CFB128:
            return EVP_aes_192_cfb128();
        case AES_256_CFB128:
            return EVP_aes_256_cfb128();
        case AES_128_OFB:
            return EVP_aes_128_ofb();
        case AES_192_OFB:
            return EVP_aes_192_ofb();
        case AES_256_OFB:
            return EVP_aes_256_ofb();
        default:
            return NULL;
    }
}

static size_t get_aes_resultlen(size_t str_len, AesBlockMode mode)
{
    const EVP_CIPHER *cipher = get_cipher(mode);
    unsigned int block_size = EVP_CIPHER_block_size(cipher);

    if (block_size > 1) {
        return block_size * (str_len / block_size) + block_size;
    }
    return str_len;
}

/* Transforms an arbitrary long key into a fixed length AES key */
static void create_fixed_length_aes_key(unsigned char *key, size_t key_len, unsigned char *realkey, size_t realkey_len)
{
    unsigned char *key_left = key;
    unsigned char *key_right = key + key_len;
    unsigned char *realkey_left = realkey;
    unsigned char *realkey_right = realkey + realkey_len;

    for (; key_left < key_right; key_left++, realkey_left++) {
        if (realkey_left == realkey_right) {
            realkey_left = realkey;
        }
        *realkey_left ^= *key_left;
    }
}

/* encrypt and decrypt, clear memory about sensitive information. */
static void clear_pass_key_iv(char *str, size_t str_len, char *key, size_t key_len,
    char *iv, size_t iv_len)
{
    if (str != NULL) {
        errno_t errorno = memset_s(str, str_len, '\0', str_len);
        securec_check(errorno, "\0", "\0");
        pfree_ext(str);
    }

    if (key != NULL) {
        errno_t errorno = memset_s(key, key_len, '\0', key_len);
        securec_check(errorno, "\0", "\0");
        pfree_ext(key);
    }

    if (iv != NULL) {
        errno_t errorno = memset_s(iv, iv_len, '\0', iv_len);
        securec_check(errorno, "\0", "\0");
        pfree_ext(iv);
    }

    return;
}

static void clear_pass_key(FunctionCallInfo fcinfo)
{
    if (!PG_ARGISNULL(0)) {
        char *str = text_to_cstring(PG_GETARG_TEXT_P(0));
        errno_t errorno = memset_s(str, strlen(str), '\0', strlen(str));
        securec_check(errorno, "\0", "\0");
        pfree_ext(str);
    }

    if (!PG_ARGISNULL(1)) {
        char *key = text_to_cstring(PG_GETARG_TEXT_P(1));
        errno_t errorno = memset_s(key, strlen(key), '\0', strlen(key));
        securec_check(errorno, "\0", "\0");
        pfree_ext(key);
    }

    return;
}

static bool gs_aes_encrypt(unsigned char *src, size_t src_len, unsigned char *dst, unsigned char *key,
    size_t key_len, unsigned char *init_vector, AesBlockMode mode, size_t *resultlen)
{
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    bool res = true;
    int dst_len, bytes_written;
    const EVP_CIPHER *cipher = get_cipher(mode);
    
    size_t realkey_len = g_aes_block_mode_key_size[mode % NUM_KEY_SIZE] / BYTE_SIZE;
    unsigned char *realkey = (unsigned char *)OPENSSL_malloc(realkey_len);
    if (realkey == NULL) {
        EVP_CIPHER_CTX_free(ctx);
        return false;
    }

    errno_t rc = memset_s(realkey, realkey_len, 0, realkey_len);
    securec_check(rc, "\0", "\0");
    
    create_fixed_length_aes_key(key, key_len, realkey, realkey_len);

    if (cipher == NULL || (EVP_CIPHER_iv_length(cipher) > 0 && !init_vector)) {
        OPENSSL_free(realkey);
        EVP_CIPHER_CTX_free(ctx);
        return false;
    }

    do {
        if (EVP_EncryptInit_ex(ctx, cipher, NULL, realkey, init_vector) == 0) {
            res = false;
            break;
        }

        if (EVP_CIPHER_CTX_set_padding(ctx, 1) == 0) {
            res = false;
            break;
        }

        dst_len = 0;
        if (EVP_EncryptUpdate(ctx, dst, &bytes_written, src, (int)src_len) == 0) {
            res = false;
            break;
        }

        dst_len += bytes_written;
        if (EVP_EncryptFinal_ex(ctx, dst + bytes_written, &bytes_written) == 0) {
            res = false;
            break;
        }

        dst_len += bytes_written;
    } while (0);

    if (!res) {
        ERR_clear_error();
        OPENSSL_free(realkey);
        EVP_CIPHER_CTX_free(ctx);
        return false;
    }

    OPENSSL_free(realkey);
    EVP_CIPHER_CTX_free(ctx);
    *resultlen = dst_len;
    return true;
}

Datum aes_encrypt(PG_FUNCTION_ARGS)
{
    if (!DB_IS_CMPT(B_FORMAT)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("aes_encrypt is supported only in B-format database.")));
    }

    text *result = NULL;
    size_t aeslen = 0;
    size_t resultlen = 0;
    bool res = true;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1)) {
        clear_pass_key(fcinfo);
        PG_RETURN_NULL();
    }

    char *str = text_to_cstring(PG_GETARG_TEXT_P(0));
    size_t str_len = strlen(str);

    char *key = text_to_cstring(PG_GETARG_TEXT_P(1));
    size_t key_len = strlen(key);

    if (PG_ARGISNULL(2)) { /* attribute value cannot be NULL */
        clear_pass_key_iv(str, str_len, key, key_len, NULL, 0);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("the size of init_vector must be greater than or equal to 16.")));
    }

    char *init_vector = text_to_cstring(PG_GETARG_TEXT_P(2));
    size_t iv_len = strlen(init_vector);

    if (iv_len < AES_INIT_VECTOR_SIZE) {
        clear_pass_key_iv(str, str_len, key, key_len, init_vector, iv_len);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("the size of init_vector must be greater than or equal to 16.")));
    }

    int block_encryption_mode = u_sess->attr.attr_common.block_encryption_mode;

    aeslen = get_aes_resultlen(str_len, (AesBlockMode)block_encryption_mode);
    result = (text *)palloc(aeslen + VARHDRSZ);
    
    res = gs_aes_encrypt((unsigned char *)str, str_len,
        (unsigned char *)VARDATA(result),
        (unsigned char *)key, key_len,
        (unsigned char *)init_vector,
        (AesBlockMode)block_encryption_mode,
        &resultlen);

    if (!res) {
        clear_pass_key_iv(str, str_len, key, key_len, init_vector, iv_len);
        ereport(LOG, (errmsg("encrypt failed.")));
        PG_RETURN_NULL();
    }
    
    clear_pass_key_iv(str, str_len, key, key_len, init_vector, iv_len);
    SET_VARSIZE(result, VARHDRSZ + resultlen);
    PG_RETURN_TEXT_P(result);
}

static bool gs_aes_decrypt(unsigned char *src, size_t src_len, unsigned char *dst, unsigned char *key,
    size_t key_len, unsigned char *init_vector, AesBlockMode mode, size_t *resultlen)
{
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    bool res = true;
    int dst_len, bytes_written;
    const EVP_CIPHER *cipher = get_cipher(mode);
    
    size_t realkey_len = g_aes_block_mode_key_size[mode % NUM_KEY_SIZE] / BYTE_SIZE;
    unsigned char *realkey = (unsigned char *)OPENSSL_malloc(realkey_len);
    if (realkey == NULL) {
        EVP_CIPHER_CTX_free(ctx);
        return false;
    }

    errno_t rc = memset_s(realkey, realkey_len, 0, realkey_len);
    securec_check(rc, "\0", "\0");

    create_fixed_length_aes_key(key, key_len, realkey, realkey_len);

    if (cipher == NULL || (EVP_CIPHER_iv_length(cipher) > 0 && !init_vector)) {
        OPENSSL_free(realkey);
        EVP_CIPHER_CTX_free(ctx);
        return false;
    }

    do {
        if (EVP_DecryptInit_ex(ctx, cipher, NULL, realkey, init_vector) == 0) {
            res = false;
            break;
        }

        if (EVP_CIPHER_CTX_set_padding(ctx, 1) == 0) {
            res = false;
            break;
        }

        dst_len = 0;
        if (EVP_DecryptUpdate(ctx, dst, &bytes_written, src, (int)src_len) == 0) {
            res = false;
            break;
        }

        dst_len += bytes_written;
        if (EVP_DecryptFinal_ex(ctx, dst + bytes_written, &bytes_written) == 0) {
            res = false;
            break;
        }

        dst_len += bytes_written;
    } while (0);

    if (!res) {
        ERR_clear_error();
        OPENSSL_free(realkey);
        EVP_CIPHER_CTX_free(ctx);
        return false;
    }
    
    OPENSSL_free(realkey);
    EVP_CIPHER_CTX_free(ctx);
    *resultlen = dst_len;
    return true;
}


Datum aes_decrypt(PG_FUNCTION_ARGS)
{
    if (!DB_IS_CMPT(B_FORMAT)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("aes_decrypt is supported only in B-format database.")));
    }

    text *result = NULL;
    size_t resultlen = 0;
    bool res = true;

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1)) {
        clear_pass_key(fcinfo);
        PG_RETURN_NULL();
    }

    text *str_text = PG_GETARG_TEXT_P(0);
    char *str = text_to_cstring(str_text);
    size_t str_len = VARSIZE(str_text) - VARHDRSZ;

    char *key = text_to_cstring(PG_GETARG_TEXT_P(1));
    size_t key_len = strlen(key);

    if (PG_ARGISNULL(2)) { /* attribute value cannot be NULL */
        clear_pass_key_iv(str, str_len, key, key_len, NULL, 0);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("the size of init_vector must be greater than or equal to 16.")));
    }

    char *init_vector = text_to_cstring(PG_GETARG_TEXT_P(2));
    size_t iv_len = strlen(init_vector);

    if (iv_len < AES_INIT_VECTOR_SIZE) {
        clear_pass_key_iv(str, str_len, key, key_len, init_vector, iv_len);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("the size of init_vector must be greater than or equal to 16.")));
    }

    int block_encryption_mode = u_sess->attr.attr_common.block_encryption_mode;

    result = (text *)palloc(str_len + VARHDRSZ);

    res = gs_aes_decrypt((unsigned char *)str, str_len,
        (unsigned char *)VARDATA(result),
        (unsigned char *)key, key_len,
        (unsigned char *)init_vector,
        (AesBlockMode)block_encryption_mode,
        &resultlen);

    if (!res) {
        clear_pass_key_iv(str, str_len, key, key_len, init_vector, iv_len);
        ereport(LOG, (errmsg("decrypt failed.")));
        PG_RETURN_NULL();
    }

    clear_pass_key_iv(str, str_len, key, key_len, init_vector, iv_len);
    SET_VARSIZE(result, VARHDRSZ + resultlen);
    PG_RETURN_TEXT_P(result);
}
