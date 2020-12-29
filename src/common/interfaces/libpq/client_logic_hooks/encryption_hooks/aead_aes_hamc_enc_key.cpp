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
#include "cipher.h"

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

/* Derives all the required keys from the given root key */
AeadAesHamcEncKey::AeadAesHamcEncKey(unsigned char *root_key, size_t root_key_size)
{
    generate_keys(root_key, root_key_size);
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
    if (RAND_priv_bytes(key, MAX_SIZE) != 1) {
        keySize = 0;
        return false;
    }
    keySize = MAX_SIZE;
    return true;
}

bool AeadAesHamcEncKey::generate_keys(unsigned char *root_key, size_t root_key_len)
{
    errno_t rc = EOK;
    /* Derive keys from the root key. Derive encryption key */
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
    rc = memset_s(root_key, root_key_len, 0, root_key_len);
    securec_check_c(rc, "\0", "\0");
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
    unsigned char *result) const
{
    unsigned int result_len = MAX_SIZE;
    int ret = CRYPT_hmac(NID_hmacWithSHA256, key, key_len, data, data_len, result, &result_len);
    if (ret) {
        return false;
    }
    return true;
}
