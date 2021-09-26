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
 * aead_aes_hamc_enc_key.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\encryption_hooks\aead_aes_hamc_enc_key.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef HAVE_AES256_CBC_HMAC_ENCRYPTION_KEY
#define HAVE_AES256_CBC_HMAC_ENCRYPTION_KEY

#include "openssl/hmac.h"
#include "openssl/ossl_typ.h"
#include "cipher.h"

class HmacCtxGroup {
public:
    HmacCtxGroup()
    {
        ctx_worker = NULL;
        ctx_template = NULL;
    }

    ~HmacCtxGroup()
    {
        free_hmac_ctx_all();
    }

    void free_hmac_ctx_all()
    {
        free_hmac_ctx(&ctx_worker);
        free_hmac_ctx(&ctx_template);
    }

    HMAC_CTX* ctx_worker;
    HMAC_CTX* ctx_template;
private:
    void free_hmac_ctx(HMAC_CTX** ctx_tmp)
    {
        if (*ctx_tmp != NULL) {
            HMAC_CTX_free(*ctx_tmp);
            *ctx_tmp = NULL;
        }
    }
};

/*
 * Encryption key class containing 4 keys. This class is used by SqlAeadAes256CbcHmac256Algorithm and
 * SqlAes256CbcAlgorithm 1) root key - Main key that is used to derive the keys used in the encryption algorithm 2)
 * encryption key - A derived key that is used to encrypt the plain text and generate cipher text 3) mac_key - A derived
 * key that is used to compute HMAC of the cipher text 4) iv_key - A derived key that is used to generate a synthetic IV
 * from plain text data.
 */
class AeadAesHamcEncKey {
public:
    /* Derives all the required keys from the given root key */
    AeadAesHamcEncKey(unsigned char *root_key, size_t root_key_size);
    explicit AeadAesHamcEncKey();
    ~AeadAesHamcEncKey();

    /* request a new root key to be created */
    static bool generate_root_key(unsigned char *key, size_t &keySize);

    /* create 3 keys based on the root key */
    bool generate_keys(unsigned char *root_key, size_t root_key_len);

    const unsigned char *get_encyption_key() const;
    const unsigned char *get_mac_key() const;
    const unsigned char *get_iv_key() const;

    HmacCtxGroup hmac_ctx_group_iv;       /* store ctx to reuse for IV call */
    HmacCtxGroup hmac_ctx_group_mac;      /* store ctx to reuse for hmac generation or hmac check call */

private:
    bool HKDF(const unsigned char *key, int key_len, const unsigned char *data, int data_len,
        unsigned char *result);

private:
    /* Encryption Key Salt format. This is used to derive the encryption key from the root key. */
    static const unsigned char *g_encryption_key_salt_format;
    /* MAC Key Salt format. This is used to derive the MAC key from the root key. */
    static const unsigned char *g_mac_key_salt_format;
    /*
     * IV Key Salt format. This is used to derive the IV key from the root key. This is only used for Deterministic
     * encryption.
     */
    static const unsigned char *g_iv_key_salt_format; /* "Microsoft SQL Server cell IV key" */
    /* FIX: set a specific size for each key and truncate it in this class and not externally */
    static const int MAX_SIZE = 32;
    /* Encryption Key */
    unsigned char _encryption_key [MAX_SIZE + 1] = {0};
    /* MAC key */
    unsigned char _mac_key[MAX_SIZE + 1] = {0};
    /* IV Key */
    unsigned char _iv_key[MAX_SIZE + 1] = {0};

    HmacCtxGroup hmac_ctx_group_root;     /* store ctx to reuse for generating the 3 keys */
};

#endif /* HAVE_AES256_CBC_HMAC_ENCRYPTION_KEY */
