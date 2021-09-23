/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * sm2_enc_key.cpp
 *
 * IDENTIFICATION
 *      src\common\interfaces\libpq\client_logic_hooks\encryption_hooks\sm2_enc_key.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#include <stdio.h>
#include <iostream>
#include "openssl/obj_mac.h"
#include "openssl/ossl_typ.h"
#include "openssl/bio.h"
#include "openssl/evp.h"
#include "openssl/pem.h"
#include "openssl/ec.h"
#include "openssl/bn.h"
#include "openssl/crypto.h"
#include "openssl/opensslconf.h"
#include "openssl/err.h"
#include <securec.h>
#include <securec_check.h>
#include "sm2_enc_key.h"
#include "libpq-fe.h"

Sm2KeyPair* generate_encrypt_pair_key()
{
    Sm2KeyPair *sm2_key_pair = NULL;

    EC_KEY *encrypted_key = EC_KEY_new();
    if (encrypted_key == NULL) {
        return NULL;
    }

    EC_GROUP *encrypted_group = EC_GROUP_new_by_curve_name(NID_brainpoolP256r1);
    if (encrypted_group == NULL) {
        EC_KEY_free(encrypted_key);
        return NULL;
    }

    if (EC_KEY_set_group(encrypted_key, encrypted_group) != 1) {
        EC_GROUP_free(encrypted_group);
        EC_KEY_free(encrypted_key);
        return NULL;
    }
    EC_GROUP_free(encrypted_group);
 
    if (!EC_KEY_generate_key(encrypted_key)) {
        EC_KEY_free(encrypted_key);
        return NULL;
    }

    BIO *bio_priv_key = BIO_new(BIO_s_mem());
    BIO *bio_pub_key = BIO_new(BIO_s_mem());
 
    PEM_write_bio_ECPrivateKey(bio_priv_key, encrypted_key, NULL, NULL, 0, NULL, NULL);
    PEM_write_bio_EC_PUBKEY(bio_pub_key, encrypted_key);
    EC_KEY_free(encrypted_key);
 
    size_t pri_key_len = (size_t)BIO_pending(bio_priv_key);
    size_t pub_key_len = (size_t)BIO_pending(bio_pub_key);

    CmkemUStr *priv_key = malloc_cmkem_ustr(pri_key_len);
    if (priv_key == NULL) {
        return NULL;
    }
    
    CmkemUStr *pub_key = malloc_cmkem_ustr(pub_key_len);
    if (pub_key == NULL) {
        free_cmkem_ustr(priv_key);
        return NULL;
    }
 
    BIO_read(bio_priv_key, priv_key->ustr_val, (int)pri_key_len);
    BIO_read(bio_pub_key, pub_key->ustr_val, (int)pub_key_len);
    priv_key->ustr_len = pri_key_len;
    pub_key->ustr_len = pub_key_len;
 
    sm2_key_pair = (Sm2KeyPair *)malloc(sizeof(Sm2KeyPair));
    if (sm2_key_pair == NULL) {
        free_cmkem_ustr(priv_key);
        free_cmkem_ustr(pub_key);
        return NULL;
    }

    sm2_key_pair->priv_key = priv_key;
    sm2_key_pair->pub_key = pub_key;

    return sm2_key_pair;
}

/* openssl sm2 cipher evp using */
CmkemErrCode encrypt_with_sm2_pubkey(CmkemUStr *plain, CmkemUStr *pub_key, CmkemUStr **cipher)
{
    BIO *bio_pub_key = NULL;
    size_t bio_pub_key_len = 0;
    EC_KEY *ec_pub_key = NULL;
    EVP_PKEY* public_evp_key = NULL;
    EVP_PKEY_CTX *ctx = NULL;
    int ret = 0;
    errno_t rc = 0;
    size_t cipher_buf_len = 0;

    bio_pub_key = BIO_new(BIO_s_mem());
    if (bio_pub_key == NULL) {
        return CMKEM_MALLOC_MEM_ERR;
    }

    bio_pub_key_len = BIO_write(bio_pub_key, pub_key->ustr_val, (int)pub_key->ustr_len);
    if (bio_pub_key_len < 0) {
        cmkem_errmsg("failed to write sm2 key to from BIO.");
        BIO_free(bio_pub_key);
        rc = memset_s(pub_key->ustr_val, pub_key->ustr_len, 0, pub_key->ustr_len);
        securec_check_c(rc, "", "");
        return CMKEM_WRITE_TO_BIO_ERR;
    }

    /* read public key from BIO. */
    ec_pub_key = PEM_read_bio_EC_PUBKEY(bio_pub_key, NULL, NULL, NULL);
    BIO_free(bio_pub_key);
    if (ec_pub_key == NULL) {
        cmkem_errmsg("open_public_key failed to PEM_read_bio_EC_PUBKEY Failed.");
        return CMKEM_READ_FROM_BIO_ERR;
    }

    public_evp_key = EVP_PKEY_new();
    if (public_evp_key == NULL) {
        cmkem_errmsg("open_public_key EVP_PKEY_new failed.");
        EC_KEY_free(ec_pub_key);
        return CMKEM_MALLOC_MEM_ERR;
    }

    ret = EVP_PKEY_set1_EC_KEY(public_evp_key, ec_pub_key);
    EC_KEY_free(ec_pub_key);
    if (ret != 1) {
        cmkem_errmsg("EVP_PKEY_set1_EC_KEY failed.");
        EVP_PKEY_free(public_evp_key);
        return CMKEM_EVP_ERR;
    }

    ret = EVP_PKEY_set_alias_type(public_evp_key, EVP_PKEY_SM2);
    if (ret != 1) {
        cmkem_errmsg("EVP_PKEY_set_alias_type to EVP_PKEY_SM2 failed!");
        EVP_PKEY_free(public_evp_key);
        return CMKEM_EVP_ERR;
    }

    /* do cipher. */
    ctx = EVP_PKEY_CTX_new(public_evp_key, NULL);
    EVP_PKEY_free(public_evp_key);
    if (ctx == NULL) {
        cmkem_errmsg("EVP_PKEY_CTX_new failed.");
        return CMKEM_MALLOC_MEM_ERR;
    }

    ret = EVP_PKEY_encrypt_init(ctx);
    if (ret < 0) {
        cmkem_errmsg("sm2_pubkey_encrypt failed to EVP_PKEY_encrypt_init. ret = %d.", ret);
        EVP_PKEY_CTX_free(ctx);
        return CMKEM_EVP_ERR;
    }

    /* determine buffer length */
    ret = EVP_PKEY_encrypt(ctx, NULL, &cipher_buf_len, plain->ustr_val, plain->ustr_len);
    if (ret < 0) {
        EVP_PKEY_CTX_free(ctx);
        cmkem_errmsg("sm2_pubkey_encrypt failed to EVP_PKEY_encrypt. ret = %d.", ret);
        return CMKEM_SM2_ENC_ERR;
    }

    *cipher = malloc_cmkem_ustr(cipher_buf_len);
    if (*cipher == NULL) {
        return CMKEM_MALLOC_MEM_ERR;
    }

    ret = EVP_PKEY_encrypt(ctx, (*cipher)->ustr_val, &((*cipher)->ustr_len), plain->ustr_val, plain->ustr_len);
    EVP_PKEY_CTX_free(ctx);
    if (ret < 0) {
        free_cmkem_ustr(*cipher);
        cmkem_errmsg("sm2_pubkey_encrypt failed to EVP_PKEY_encrypt. ret = %d.", ret);
        return CMKEM_SM2_ENC_ERR;
    }

    return CMKEM_SUCCEED;
}

CmkemErrCode decrypt_with_sm2_privkey(CmkemUStr *cipher, CmkemUStr *priv_key, CmkemUStr **plain)
{
    int ret = 0;
    EC_KEY *ec_key = NULL;
    EVP_PKEY* private_evp_key = NULL;
    EVP_PKEY_CTX *ctx = NULL;
    BIO *bio_priv_key = NULL;
    int bio_priv_key_len = 0;

    bio_priv_key = BIO_new(BIO_s_mem());
    if (bio_priv_key == NULL) {
        return CMKEM_MALLOC_MEM_ERR;
    }

    bio_priv_key_len = BIO_write(bio_priv_key, priv_key->ustr_val, (int)priv_key->ustr_len);
    if (bio_priv_key_len < 0) {
        cmkem_errmsg("failed to write sm2 key to BIO.");
        BIO_free(bio_priv_key);
        return CMKEM_WRITE_TO_BIO_ERR;
    }

    /* read private key from BIO. */
    ec_key = PEM_read_bio_ECPrivateKey(bio_priv_key, &ec_key, NULL, NULL);
    BIO_free(bio_priv_key);
    if (ec_key == NULL) {
        cmkem_errmsg("open_private_key failed to PEM_read_bio_ECPrivateKey Failed.");
        return CMKEM_READ_FROM_BIO_ERR;
    }
    
    private_evp_key = EVP_PKEY_new();
    if (private_evp_key == NULL) {
        cmkem_errmsg("open_public_key EVP_PKEY_new failed.");
        EC_KEY_free(ec_key);
        return CMKEM_MALLOC_MEM_ERR;
    }

    ret = EVP_PKEY_set1_EC_KEY(private_evp_key, ec_key);
    EC_KEY_free(ec_key);
    if (ret != 1) {
        cmkem_errmsg("EVP_PKEY_set1_EC_KEY failed\n");
        EVP_PKEY_free(private_evp_key);
        return CMKEM_EVP_ERR;
    }

    ret = EVP_PKEY_set_alias_type(private_evp_key, EVP_PKEY_SM2);
    if (ret != 1) {
        cmkem_errmsg("EVP_PKEY_set_alias_type to EVP_PKEY_SM2 failed!");
        EVP_PKEY_free(private_evp_key);
        return CMKEM_EVP_ERR;
    }

    /* do cipher. */
    ctx = EVP_PKEY_CTX_new(private_evp_key, NULL);
    EVP_PKEY_free(private_evp_key);
    if (ctx == NULL) {
        cmkem_errmsg("EVP_PKEY_CTX_new failed");
        EVP_PKEY_free(private_evp_key);
        return CMKEM_MALLOC_MEM_ERR;
    }

    ret = EVP_PKEY_decrypt_init(ctx);
    if (ret < 0) {
        cmkem_errmsg("sm2 private_key decrypt failed to EVP_PKEY_decrypt_init.");
        EVP_PKEY_CTX_free(ctx);
        return CMKEM_EVP_ERR;
    }

    *plain = malloc_cmkem_ustr(cipher->ustr_len * 2);
    if (*plain == NULL) {
        return CMKEM_MALLOC_MEM_ERR;
    }

    ret = EVP_PKEY_decrypt(ctx, (*plain)->ustr_val, &((*plain)->ustr_len), cipher->ustr_val, cipher->ustr_len);
    EVP_PKEY_CTX_free(ctx);
    if (ret < 0) {
        cmkem_errmsg("sm2_prikey_decrypt failed to EVP_PKEY_decrypt.");
        free_cmkem_ustr_with_erase(*plain);
        return CMKEM_SM2_DEC_ERR;
    }

    return CMKEM_SUCCEED;
}

