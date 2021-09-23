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
 * cmkem_comm_algorithm.cpp
 *      some general encryption and decryption function.
 *      you can use them to encrypt and decrypt your data, CEK entity and CMK entity.
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/cmkem_comm_algorithm.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cmkem_comm_algorithm.h"

#include <openssl/rand.h>
#include <openssl/rsa.h>
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/sha.h>
#include "encrypt_decrypt.h"
#include "aead_aes_hamc_enc_key.h"

size_t get_key_len_by_algo(AlgoType cmk_algo)
{
    switch (cmk_algo) {
        case AES_256_CBC:
            return 32;
        case SM4:
            return 32;
        case RSA_2048:
            return 256;
        case RSA_3072:
            return 384;
        case SM2:
            return 32;
        default:
            break;
    }

    return 0;
}

AlgoType get_algo_by_str(const char *algo_str)
{
    if (strcasecmp(algo_str, "AES_256_CBC") == 0) {
        return AES_256_CBC;
    } else if (strcasecmp(algo_str, "SM4") == 0) {
        return SM4;
    } else if (strcasecmp(algo_str, "RSA_2048") == 0) {
        return RSA_2048;
    } else if (strcasecmp(algo_str, "RSA_3072") == 0) {
        return RSA_3072;
    } else if (strcasecmp(algo_str, "SM2") == 0) {
        return SM2;
    } 
    
    return UNKNOWN_ALGO;
}

ColumnEncryptionAlgorithm get_algo_combination(AlgoType algo)
{
    switch (algo) {
        case AES_256_CBC:
            return AEAD_AES_256_CBC_HMAC_SHA256;
        case SM4:
            return SM4_SM3;
        default:
            return INVALID_ALGORITHM;
    }
}

CmkemErrCode get_hex_join_hash_from_ustr(CmkemUStr *ustr, CmkemStr **hex_join_hash)
{
    CmkemUStr *ustr_join_hash = NULL;
    CmkemErrCode ret = CMKEM_SUCCEED;

    ret = join_sha256(ustr, &ustr_join_hash);
    check_cmkem_ret(ret);

    *hex_join_hash = ustr_to_hex(ustr_join_hash);
    free_cmkem_ustr_with_erase(ustr_join_hash);
    if (*hex_join_hash == NULL) {
        return CMKEM_MALLOC_MEM_ERR;
    }
    
    return CMKEM_SUCCEED;
}

CmkemErrCode get_ustr_from_hex_join_hash(CmkemStr *hex_join_hash, CmkemUStr **ustr)
{
    CmkemUStr *ustr_join_hash = NULL;
    CmkemErrCode ret = CMKEM_SUCCEED;

    ustr_join_hash = hex_to_ustr(hex_join_hash);
    if (ustr_join_hash == NULL) {
        return CMKEM_MALLOC_MEM_ERR;
    }
    
    ret = strip_sha256(ustr_join_hash, ustr);
    free_cmkem_ustr_with_erase(ustr_join_hash);
    check_cmkem_ret(ret);
    
    return CMKEM_SUCCEED;
}

CmkemErrCode encrypt_with_symm_algo(AlgoType algo, CmkemUStr *plain, CmkemUStr *key, CmkemUStr **cipher)
{
    int cipher_len = 0;
    AeadAesHamcEncKey derived_key = AeadAesHamcEncKey(key->ustr_val, get_key_len_by_algo(AES_256_CBC));

    *cipher = malloc_cmkem_ustr(get_cipher_text_size(plain->ustr_len));
    if (*cipher == NULL) {
        return CMKEM_MALLOC_MEM_ERR;
    }

    cipher_len = encrypt_data(
        plain->ustr_val,
        plain->ustr_len,
        derived_key,
        EncryptionType::DETERMINISTIC_TYPE,
        (*cipher)->ustr_val,
        get_algo_combination(algo));
    if (cipher_len <= 0) {
        free_cmkem_ustr(*cipher);
        cmkem_errmsg("failed to encrypt data with AES_256_CBC.");
        return CMKEM_ENCRYPT_CEK_ERR;
    }

    (*cipher)->ustr_len = (size_t) cipher_len;
    return CMKEM_SUCCEED;
}

CmkemErrCode decrypt_with_symm_algo(AlgoType algo, CmkemUStr *cipher, CmkemUStr *key, CmkemUStr **plain)
{
    int plain_len = 0;

    AeadAesHamcEncKey derived_key = AeadAesHamcEncKey(key->ustr_val, get_key_len_by_algo(AES_256_CBC));

    *plain = malloc_cmkem_ustr(cipher->ustr_len); /* the plain cmkem_list_len must be less than cipher cmkem_list_len */
    if (*plain == NULL) {
        return CMKEM_MALLOC_MEM_ERR;
    }

    plain_len = decrypt_data(
        cipher->ustr_val,
        cipher->ustr_len,
        derived_key,
        (*plain)->ustr_val,
        get_algo_combination(algo));
    if (plain_len <= 0) {
        free_cmkem_ustr_with_erase(*plain);
        cmkem_errmsg("failed to decrypt data with AES_256_CBC.");
        return CMKEM_DECRYPT_CEK_ERR;
    }

    (*plain)->ustr_len = (size_t) plain_len;
    return CMKEM_SUCCEED;
}

RSA *create_rsa_keypair(size_t rsa_key_len)
{
    BIGNUM *rsa_n_val = NULL;
    RSA *rsa_key_pair = NULL;

    rsa_n_val = BN_new();
    if (BN_set_word(rsa_n_val, RSA_F4) != 1) { /* public exponent - 65537 */
        return NULL;
    }

    rsa_key_pair = RSA_new();
    if (rsa_key_pair == NULL) {
        BN_free(rsa_n_val);
        return NULL;
    }

    if (RSA_generate_key_ex(rsa_key_pair, rsa_key_len, rsa_n_val, NULL) != 1) {
        BN_free(rsa_n_val);
        RSA_free(rsa_key_pair);
        return NULL;
    }
    BN_free(rsa_n_val);

    return rsa_key_pair;
}

CmkemErrCode write_rsa_keypair_to_biobuf(RSA *rsa_key_pair, BIO **pub_key_biobuf, BIO **priv_key_biobuf)
{    
    int ssl_ret = 0;
    
    *pub_key_biobuf = BIO_new(BIO_s_mem());
    if (*pub_key_biobuf == NULL) {
        return CMKEM_MALLOC_MEM_ERR;
    }

    *priv_key_biobuf = BIO_new(BIO_s_mem());
    if (*priv_key_biobuf == NULL) {
        BIO_free(*pub_key_biobuf);
        return CMKEM_MALLOC_MEM_ERR;
    }

    ssl_ret = PEM_write_bio_RSA_PUBKEY(*pub_key_biobuf, rsa_key_pair);
    if (ssl_ret != 1) {
        BIO_free(*pub_key_biobuf);
        BIO_free(*priv_key_biobuf);
        return CMKEM_WRITE_TO_BIO_ERR;
    }

    ssl_ret = PEM_write_bio_RSAPrivateKey(*priv_key_biobuf, rsa_key_pair, NULL, NULL, 0, NULL, NULL);
    if (ssl_ret != 1) {
        BIO_free(*pub_key_biobuf);
        BIO_free(*priv_key_biobuf);
        return CMKEM_WRITE_TO_BIO_ERR;
    }
    
    return CMKEM_SUCCEED;
}

CmkemErrCode read_rsa_key_from_biobuf(BIO *key_biobuf, CmkemUStr **key)
{
    size_t key_len = BIO_pending(key_biobuf);
    
    *key = malloc_cmkem_ustr(key_len);
    if (*key == NULL) {
        return CMKEM_MALLOC_MEM_ERR;
    }
    
    if (BIO_read(key_biobuf, (*key)->ustr_val, key_len) < 0) {
        free_cmkem_ustr_with_erase(*key);
        return CMKEM_READ_FROM_BIO_ERR;
    }
    (*key)->ustr_len = key_len;

    return CMKEM_SUCCEED;
}

CmkemErrCode join_sha256(CmkemUStr *data, CmkemUStr **data_join_sha256)
{
    *data_join_sha256 = malloc_cmkem_ustr(data->ustr_len + SHA256_HASH_LEN + 1);
    if (*data_join_sha256 == NULL) {
        return CMKEM_MALLOC_MEM_ERR;
    }

    for (size_t i = 0; i < data->ustr_len; i++) {
        (*data_join_sha256)->ustr_val[i] = data->ustr_val[i];
    }

    SHA256(data->ustr_val, data->ustr_len, (*data_join_sha256)->ustr_val + data->ustr_len);

    (*data_join_sha256)->ustr_len = data->ustr_len + SHA256_HASH_LEN;

    return CMKEM_SUCCEED;
}

CmkemErrCode strip_sha256(CmkemUStr *data_join_sha256, CmkemUStr **data)
{
    unsigned char expected_hash[SHA256_HASH_LEN + 1] = {0};
    size_t data_len = data_join_sha256->ustr_len - SHA256_HASH_LEN;

    SHA256(data_join_sha256->ustr_val, data_len, expected_hash);

    *data = malloc_cmkem_ustr(data_len + 1);
    if (*data == NULL) {
        return CMKEM_MALLOC_MEM_ERR;
    }

    for (size_t i = 0; i < data_len; i++) {
        (*data)->ustr_val[i] = data_join_sha256->ustr_val[i];
    }
    (*data)->ustr_val[data_len] = '\0';
    (*data)->ustr_len = data_len;

    return CMKEM_SUCCEED;
}
