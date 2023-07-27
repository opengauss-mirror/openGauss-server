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
 * security_encrypt_decrypt.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/encrypt/security_encrypt_decrypt.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef HAVE_AES256_CBC_ENCRYPTION_INCLUDE
#define HAVE_AES256_CBC_ENCRYPTION_INCLUDE
#include "keymgr/encrypt/security_client_logic_enums.h"
#include "keymgr/encrypt/security_aead_aes_hamc_enc_key.h"

/* Params of encrypt and decrypt */
typedef struct EncParam {
    unsigned char *plaintext;
    int plaintext_len;
    unsigned char *ciphertext;
    int ciphertext_len;
    const unsigned char *key;
    int key_len;
    unsigned char *iv;
    int iv_len;
    unsigned char *tag;
    int tag_len;
} EncParam;

/*
 * returns the recommended size for allocation for encrypt_data's result buffer
 * returns value <= 0 when provided with invalid input
 */
int get_cipher_text_size(int plainTextSize);

/*
 * Encryption Algorithm
 * cell_iv = HMAC_SHA-2-256(iv_key, cell_data) truncated to 128 bits
 * cell_ciphertext = AES-CBC-256(enc_key, cell_iv, cell_data) with PKCS7 padding.
 * (optional) cell_tag = HMAC_SHA-2-256(mac_key, versionbyte + cell_iv + cell_ciphertext + versionbyte_length)
 * cell_blob = versionbyte + [cell_tag] + cell_iv + cell_ciphertext
 * param name="plainText",Plaintext data to be encrypted
 * param name="_columnEncryptionKey". Column Encryption Key. This has a root key and three derived keys.
 * param name="SaltType".if SaltType is true,it means deterministic encryption.if SaltType is false,it means random
 * encryption *param name="result". Result is  the ciphertext corresponding to the plaintext.
 * returns the ciphertext length
 * return value == 0 on error
 */
int encrypt_data(unsigned char *plain_text, int plain_text_length, AeadAesHamcEncKey &column_encryption_key,
    EncryptionType encryption_type, unsigned char *result, ColumnEncryptionAlgorithm column_encryption_algorithm);

/*
 * Decryption steps
 *  1. Validate version byte
 *  2. Validate Authentication tag
 *  3. Decrypt the message
 */
int decrypt_data(unsigned char *cipher_text, int cipher_text_length,
    AeadAesHamcEncKey &column_encryptionKey, unsigned char *decryptedtext,
    ColumnEncryptionAlgorithm column_encryption_algorithm);

bool cached_hmac(unsigned long algo_type, const unsigned char *key, int key_len, const unsigned char *data,
    int data_len, unsigned char *result, unsigned int *result_len, HmacCtxGroup *cached_ctx_group);

#endif /* HAVE_AES256_CBC_ENCRYPTION_INCLUDE */