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
 * encrypt_decrypt.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\encryption_hooks\encrypt_decrypt.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#include <stdio.h>
#include <iostream>

#include <openssl/rand.h>

#include <securec.h>
#include <securec_check.h>
#include "encrypt_decrypt.h"
#include "aead_aes_hamc_enc_key.h"
#include "cipher.h"

/* Key size in bytes */
static const int g_key_size = 32;

static const int g_auth_tag_size = 32;

/* Block size in bytes. AES uses 16 byte blocks. */
static const int g_block_size = 16;

static const int g_iv_size = 16; /* 128 bit */

static const int g_algo_version_size = 4;

/*
 * Minimum Length of cipher_text. This value is 4 (version byte) + 32 (authentication tag) + 16 (IV) + 16 (minimum of 1
 * block of cipher Text)
 */
static const int min_ciph_len_in_bytes_with_authen_tag =
    g_algo_version_size + g_iv_size + g_block_size + g_auth_tag_size;

static const unsigned char algo_version[4] = {'1'};

static int encrypt(const unsigned char *plaintext, int plaintext_len, const unsigned char *key, const unsigned char *iv,
    unsigned char *ciphertext, ColumnEncryptionAlgorithm algorithm);
static int decrypt(const unsigned char *ciphertext, int ciphertext_len, const unsigned char *key,
    const unsigned char *iv, unsigned char *plaintext, ColumnEncryptionAlgorithm algorithm);
static int my_memcmp(const void *buffer1, const void *buffer2, int count);
static bool hmac_sha256(const void *key, int keylen, const unsigned char *data, int datalen, unsigned char *result);

/*
 * Computes a keyed hash of a given text
 * currently used for both generating a MAC and as a KDF
 */
static bool hmac_sha256(const void *key, int keylen, const unsigned char *data, int datalen, unsigned char *result)
{
    unsigned int result_len = g_key_size;
    int ret = CRYPT_hmac(NID_hmacWithSHA256, (const GS_UCHAR *)key, keylen, data, datalen, result, &result_len);
    if (ret) {
        return false;
    }
    return true;
}

/*
 * To calculate the ciphertext buffer size
 */
int get_cipher_text_size(int plain_text_size)
{
    int numBlocks = plain_text_size / g_block_size + 1;
    int cipher_len = numBlocks * g_block_size;
    /* Output buffer size = size of VersionByte + Authentication Tag + IV + cipher Text blocks. */
    return (g_algo_version_size + g_auth_tag_size + g_iv_size + cipher_len);
}


/*
 * Encryption data
 */
int encrypt_data(const unsigned char *plain_text, int plain_text_length, const AeadAesHamcEncKey &column_encryption_key,
    EncryptionType encryption_type, unsigned char *result, ColumnEncryptionAlgorithm column_encryption_algorithm)
{
    if (plain_text == NULL || plain_text_length <= 0 || encryption_type == EncryptionType::INVALID_TYPE ||
        result == NULL) {
        /* invalid input */
        return 0;
    }

    /* Prepare IV.IV should be 1 single block (16 bytes) */
    unsigned char _iv [g_key_size + 1] = {0};
    unsigned char iv_truncated[g_iv_size + 1] = {0};
    if (encryption_type == EncryptionType::DETERMINISTIC_TYPE) {
        /*
         * determenistic encryption - we create an initiailization vector based on the plaintext - to make the
         * encryption CPA-secure
         * HMAC_SHA_256
         */
        hmac_sha256(column_encryption_key.get_iv_key(), g_auth_tag_size, plain_text, plain_text_length, _iv);

        /* iv is truncated to 128 bits. */
        errno_t res = EOK;
        res = memcpy_s(iv_truncated, g_iv_size + 1, _iv, g_block_size);
        if (res != EOK) {
            securec_check_c(res, "\0", "\0");
            printf("ERROR(CLIENT): fail to copy 128 bit iv from 256 bit iv value.\n");
            return 0;
        }
    } else {
        if (encryption_type != EncryptionType::RANDOMIZED_TYPE) {
            return 0; 
        }

        int res = RAND_priv_bytes(iv_truncated, g_block_size);
        if (res != 1) {
            return 0;
        }
    }

    /* Add the ciphertext  */
    int cipherStart = g_algo_version_size + g_auth_tag_size + g_iv_size;
    int cipherTextSize = encrypt(plain_text, plain_text_length, column_encryption_key.get_encyption_key(), iv_truncated,
        result + cipherStart, column_encryption_algorithm);
    if (cipherTextSize < 0) {
        /* failed to encrypt */
        return 0;
    }

    errno_t res = EOK;
    /* add the Algorithm Version */
    res = memcpy_s(result + g_auth_tag_size, g_algo_version_size, algo_version, g_algo_version_size);
    securec_check_c(res, "\0", "\0");

    /* add the IV */
    int ivStartIndex = g_auth_tag_size + g_algo_version_size;
    res = memcpy_s(result + ivStartIndex, g_iv_size, iv_truncated, g_iv_size);
    securec_check_c(res, "\0", "\0");

    /* add the HMAC (of versionbyte + IV + Ciphertext) */
    int hmacDataSize = g_algo_version_size + g_iv_size + cipherTextSize;
    hmac_sha256(column_encryption_key.get_mac_key(), g_auth_tag_size,
                result + g_auth_tag_size, hmacDataSize, result); 
    return (g_auth_tag_size + hmacDataSize);
}

/*
 * Decryption steps
 *  1. Validate version byte
 *  2. Validate Authentication tag
 *  3. Decrypt the message
 */
int decrypt_data(const unsigned char *cipher_text, int cipher_text_length,
    const AeadAesHamcEncKey &column_encryption_key, unsigned char *decryptedtext,
    ColumnEncryptionAlgorithm column_encryption_algorithm)
{
    if (cipher_text == NULL || cipher_text_length <= 0 || decryptedtext == NULL) {
        return 0;
    }

    if (cipher_text_length < min_ciph_len_in_bytes_with_authen_tag) {
        printf("ERROR(CLIENT): The length of cipher_text is invalid, cannot decrypt.\n");
        return 0;
    }

    if (cipher_text[g_auth_tag_size] != '1') {
        /* Cipher text was computed with a different algorithm version than this. */
        printf("ERROR(CLIENT): Version byte of cipher_text is invalid, cannot decrypt.\n");
        return 0;
    }
    unsigned char iv [g_iv_size] = {0};

    errno_t rc = memcpy_s(iv, g_iv_size, cipher_text + g_auth_tag_size + g_algo_version_size, g_iv_size);
    securec_check_c(rc, "\0", "\0");

    /* Computing the authentication tag */
    unsigned char authenticationTag [g_auth_tag_size] = {0};
    int HMAC_length = cipher_text_length - g_auth_tag_size;
    int res = hmac_sha256(column_encryption_key.get_mac_key(), g_auth_tag_size,
        cipher_text + g_auth_tag_size, HMAC_length, authenticationTag);
    if (res != 1) {
        printf("ERROR(CLIENT): Fail to compute a keyed hash of a given text.\n");
        return 0;
    }

    int cmp_result = my_memcmp(authenticationTag, cipher_text, g_auth_tag_size);
    if (cmp_result != 0) {
        /* MAC check failed */
        return 0;
    }

    /* Decrypt the ciphertext */
    int cipher_start_index = g_auth_tag_size + g_algo_version_size + g_iv_size; // this is where cipher starts.
    int cipher_value_length = cipher_text_length - cipher_start_index;
    int decryptedtext_len = decrypt(cipher_text + cipher_start_index, cipher_value_length,
        column_encryption_key.get_encyption_key(), iv, decryptedtext, column_encryption_algorithm);
    if (decryptedtext_len < 0) {
        return 0;
    }

    /* Add a NULL terminator. We are expecting printable text */
    decryptedtext[decryptedtext_len] = '\0';
    return decryptedtext_len;
}

/* 
 * encrypt plaintext thought AES_256_cbc algorithm
 * cell_ciphertext = AES-CBC-256(enc_key, cell_iv, cell_data) with PKCS7 padding.
 */
static int encrypt(const unsigned char *plaintext, int plaintext_len, const unsigned char *key, const unsigned char *iv,
    unsigned char *ciphertext, ColumnEncryptionAlgorithm algorithm)
{
    if (plaintext == NULL || plaintext_len <= 0 || key == NULL || iv == NULL || ciphertext == NULL) {
        return 0;
    }

    /* Create and initialise the context */
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    if (ctx == NULL) {
        printf("ERROR(CLIENT): Fail to create and initialise the context.\n");
        return -1;
    }
    /*
     * Initialise the encryption operation. IMPORTANT - ensure you use a key
     * and IV size appropriate for your cipher
     * In this example we are using 256 bit AES (i.e. a 256 bit key). The
     * IV size for *most* modes is the same as the block size. For AES this
     * is 128 bits
     */
    if (algorithm == ColumnEncryptionAlgorithm::AEAD_AES_256_CBC_HMAC_SHA256) {
        if (EVP_EncryptInit_ex(ctx, EVP_aes_256_cbc(), NULL, key, iv) != 1) {
            printf("ERROR(CLIENT): Fail to create new cipher.\n");
            EVP_CIPHER_CTX_free(ctx);
            return -1;
        }
    } else if (algorithm == ColumnEncryptionAlgorithm::AEAD_AES_128_CBC_HMAC_SHA256) {
        /* iv is truncated to 128 bits. */
        int encrypt_key_len = g_key_size / 2 + 1;
        unsigned char encrypt_key[encrypt_key_len] = {0};
        errno_t res = EOK;
        res = memcpy_s(encrypt_key, encrypt_key_len, key, g_key_size / 2);
        if (res != EOK) {
            printf("ERROR(CLIENT): Fail to copy 128 bit from 256 bit key value.\n");
            securec_check_c(res, "\0", "\0");
            return -1;
        }
        if (EVP_EncryptInit_ex(ctx, EVP_aes_128_cbc(), NULL, encrypt_key, iv) != 1) {
            printf("ERROR(CLIENT): Fail to create new cipher.\n");
            EVP_CIPHER_CTX_free(ctx);
            return -1;
        }
    }

    /*
     * Provide the message to be encrypted, and obtain the encrypted output.
     * EVP_EncryptUpdate can be called multiple times if necessary
     */
    int len = 0;
    int ciphertext_len = 0;
    if (EVP_EncryptUpdate(ctx, ciphertext, &len, plaintext, plaintext_len) != 1) {
        printf("ERROR(CLIENT): Fail to encrypt.\n");
        EVP_CIPHER_CTX_free(ctx);
        return -1;
    }
    ciphertext_len = len;
    /*
     * Finalise the encryption. Further ciphertext bytes may be written at
     * this stage.
     */
    if (EVP_EncryptFinal_ex(ctx, ciphertext + len, &len) != 1) {
        printf("ERROR(CLIENT): Fail to encrypt final.\n");
        EVP_CIPHER_CTX_free(ctx);
        return -1;
    }
    ciphertext_len += len;
    /* Clean up */
    EVP_CIPHER_CTX_free(ctx);

    return ciphertext_len;
}

/*
 * Decrypt the message though aes_256_cbc algorithm
 */
static int decrypt(const unsigned char *ciphertext, int ciphertext_len, const unsigned char *key,
    const unsigned char *iv, unsigned char *plaintext, ColumnEncryptionAlgorithm algorithm)
{
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    /* Create and initialise the context */
    if (ctx == NULL) {
        printf("ERROR(CLIENT): keymanagecannot new ctx.\n");
        return -1;
    }
    /*
     * Initialise the decryption operation. IMPORTANT - ensure you use a key
     * and IV size appropriate for your cipher
     * In this example we are using 256 bit AES (i.e. a 256 bit key). The
     * IV size for *most* modes is the same as the block size. For AES this
     * is 128 bits
     */
    if (algorithm == ColumnEncryptionAlgorithm::AEAD_AES_256_CBC_HMAC_SHA256) {
        if (EVP_DecryptInit_ex(ctx, EVP_aes_256_cbc(), NULL, key, iv) != 1) {
            printf("ERROR(CLIENT): cannot create new cipher.\n");
            EVP_CIPHER_CTX_free(ctx);
            return -1;
        }
    } else if (algorithm == ColumnEncryptionAlgorithm::AEAD_AES_128_CBC_HMAC_SHA256) {
        int encrypt_key_len = g_key_size / 2 + 1; /* from 256 bits to 128 bits */
        unsigned char encrypt_key[encrypt_key_len] = {0};
        errno_t res = EOK;
        res = memcpy_s(encrypt_key, encrypt_key_len, key, g_key_size / 2);
        if (res != EOK) {
            EVP_CIPHER_CTX_free(ctx);
            printf("ERROR(CLIENT): fail to copy 128 bit from 256 bit key value.\n");
            securec_check_c(res, "\0", "\0");
            return 0;
        }
        if (EVP_DecryptInit_ex(ctx, EVP_aes_128_cbc(), NULL, encrypt_key, iv) != 1) {
            printf("ERROR(CLIENT): cannot create new cipher.\n");
            EVP_CIPHER_CTX_free(ctx);
            return -1;
        }
    }

    /*
     * Provide the message to be decrypted, and obtain the plaintext output.
     * EVP_DecryptUpdate can be called multiple times if necessary.
     */
    int len = 0;
    int plaintext_len = 0;
    if (EVP_DecryptUpdate(ctx, plaintext, &len, ciphertext, ciphertext_len) != 1) {
        printf("ERROR(CLIENT): cannot EVP_EncryptUpdate.\n");
        EVP_CIPHER_CTX_free(ctx);
        return -1;
    }
    plaintext_len = len;
    /*
     * Finalise the decryption. Further plaintext bytes may be written at
     * this stage.
     */
    if (EVP_DecryptFinal_ex(ctx, plaintext + len, &len) != 1) {
        printf("ERROR(CLIENT): cannot EVP_EncryptFinal_ex.\n");
        EVP_CIPHER_CTX_free(ctx);
        return -1;
    }
    plaintext_len += len;
    /* Clean up */
    EVP_CIPHER_CTX_free(ctx);

    return plaintext_len;
}

static int my_memcmp(const void *buffer1, const void *buffer2, int count)
{
    if (!count) {
        return 0;
    }
    while (count--) {
        if (*(char *)buffer1 != *(char *)buffer2) {
            return (*((unsigned char *)buffer1) - *((unsigned char *)buffer2));
        }
        buffer1 = (char *)buffer1 + 1;
        buffer2 = (char *)buffer2 + 1;
    }
    return 0;
}
