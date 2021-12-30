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
 * aead_aes_hamc_enc_key.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\encryption_hooks\aead_aes_hamc_enc_key.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#include <stdio.h>
#include <iostream>

#include <openssl/rand.h>

#include "securec.h"
#include "securec_check.h"
#include "aead_aes_hamc_enc_key.h"
#include "encrypt_decrypt.h"

const unsigned char *AeadAesHamcEncKey::g_encryption_key_salt_format =
    (unsigned char *)"mppdb cell encryption key with encryption algorithm AEAD_AES_256_CBC_HMAC_SHA256 and key length "
    ": 32"; /* MAC Key Salt format. This is used to derive the MAC key from the root key. */
const unsigned char *AeadAesHamcEncKey::g_mac_key_salt_format =
    (unsigned char *)"mppdb cell MAC key with encryption algorithm AEAD_AES_256_CBC_HMAC_SHA256 and key length : "
    "32";
/*
 * IV Key Salt format. This is used to derive the IV key from the root key. This is only used for Deterministic
 * encryption.
 */
const unsigned char *AeadAesHamcEncKey::g_iv_key_salt_format =
    (unsigned char *)"mppdb cell IV key with encryption algorithm AEAD_AES_256_CBC_HMAC_SHA256 and key length : 32";

const int RAND_COUNT = 100;
/* Derives all the required keys from the given root key */
AeadAesHamcEncKey::AeadAesHamcEncKey(unsigned char *root_key, size_t root_key_size)
{
    errno_t rc = EOK;
    generate_keys(root_key, root_key_size);
    rc = memset_s(root_key, root_key_size, 0, root_key_size);
    securec_check_c(rc, "\0", "\0");
}

AeadAesHamcEncKey::AeadAesHamcEncKey()
{
}

AeadAesHamcEncKey::~AeadAesHamcEncKey()
{
    errno_t rc = EOK;
    rc = memset_s(&_encryption_key, sizeof(_encryption_key), 0, sizeof(_encryption_key));
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(&_mac_key, sizeof(_mac_key), 0, sizeof(_mac_key));
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(&_iv_key, sizeof(_mac_key), 0, sizeof(_iv_key));
    securec_check_c(rc, "\0", "\0");
}

bool AeadAesHamcEncKey::generate_root_key(unsigned char *key, size_t &keySize)
{
    bool is_ok = true;
    int r_count = 0;

    while (r_count++ < RAND_COUNT) {
        is_ok = true;
        if (RAND_priv_bytes(key, MAX_SIZE) != 1) {
            keySize = 0;
            printf("ERROR(CLIENT):Generate random key failed.\n");
            return false;
        }
        for (int i = 0; i < MAX_SIZE; i++) {
            if (key[i] == '\0') {
                is_ok = false;
                break;
            }
        }
        if (is_ok) {
            break;
        }
    }
    if (!is_ok) {
        printf("ERROR(CLIENT):Generate random key failed.\n");
        return false;
    }
    keySize = MAX_SIZE;
    return true;
}

bool AeadAesHamcEncKey::generate_keys(unsigned char *root_key, size_t root_key_len)
{
    errno_t rc = EOK;
    /* Derive keys from the root key. Derive encryption key */
    hmac_ctx_group_iv.free_hmac_ctx_all();
    hmac_ctx_group_mac.free_hmac_ctx_all();
    hmac_ctx_group_root.free_hmac_ctx_all();
    if (!HKDF(root_key, root_key_len, g_encryption_key_salt_format, strlen((char *)g_encryption_key_salt_format),
        _encryption_key)) {
        rc = memset_s(root_key, root_key_len, 0, root_key_len);
        securec_check_c(rc, "\0", "\0");
        return false;
    }
    if (!HKDF(root_key, root_key_len, g_mac_key_salt_format, strlen((char *)g_mac_key_salt_format), _mac_key)) {
        rc = memset_s(root_key, root_key_len, 0, root_key_len);
        securec_check_c(rc, "\0", "\0");
        return false;
    }
    if (!HKDF(root_key, root_key_len, g_iv_key_salt_format, strlen((char *)g_iv_key_salt_format), _iv_key)) {
        rc = memset_s(root_key, root_key_len, 0, root_key_len);
        securec_check_c(rc, "\0", "\0");
        return false;
    }
    return true;
}

const unsigned char *AeadAesHamcEncKey::get_encyption_key() const
{
    return _encryption_key;
}

const unsigned char *AeadAesHamcEncKey::get_mac_key() const
{
    return _mac_key;
}

const unsigned char *AeadAesHamcEncKey::get_iv_key() const
{
    return _iv_key;
}

/*
 * Computes a keyed hash of a given text
 */
bool AeadAesHamcEncKey::HKDF(const unsigned char *key, int key_len, const unsigned char *data, int data_len, 
    unsigned char *result)
{
    unsigned int result_len = MAX_SIZE;
    if (!cached_hmac(NID_hmacWithSHA256, key, key_len, data, data_len, result, &result_len, &hmac_ctx_group_root)) {
        return false;
    }
    return true;
}
