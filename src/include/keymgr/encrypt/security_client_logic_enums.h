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
 * security_client_logic_enums.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/encrypt/security_client_logic_enums.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CLIENT_LOGIC_ENUMS_H
#define CLIENT_LOGIC_ENUMS_H

#include <string.h>

typedef enum class CmkKeyStore {
    INVALID_KEYSTORE = -1,
    LOCALKMS,
    GS_KTOOL,
} CmkKeyStore;

typedef enum class CmkAlgorithm {
    INVALID_ALGORITHM,
    RSA_2048,
    AES_256_CBC,
    SM2,
    SM4,
    AES_256_GCM
} CmkAlgorithm;

typedef enum class EncryptionType {
    INVALID_TYPE,
    RANDOMIZED_TYPE,
    DETERMINISTIC_TYPE
} EncryptionType;


typedef enum ColumnEncryptionAlgorithm {
    INVALID_ALGORITHM,
    AEAD_AES_256_CBC_HMAC_SHA256,
    AEAD_AES_128_CBC_HMAC_SHA256,
    SM4_SM3,
    AES_256_GCM_ALGO,
    AES_256_CTR_ALGO
} ColumnEncryptionAlgorithm;

inline ColumnEncryptionAlgorithm get_cek_algorithm_from_string(const char *alg)
{
    if (alg == NULL || strlen(alg) == 0) {
        return ColumnEncryptionAlgorithm::INVALID_ALGORITHM;
    }
    if (strcasecmp(alg, "AEAD_AES_256_CBC_HMAC_SHA256") == 0) {
        return ColumnEncryptionAlgorithm::AEAD_AES_256_CBC_HMAC_SHA256;
    } else if (strcasecmp(alg, "AEAD_AES_128_CBC_HMAC_SHA256") == 0) {
        return ColumnEncryptionAlgorithm::AEAD_AES_128_CBC_HMAC_SHA256;
    } else if (strcasecmp(alg, "SM4_SM3") == 0) {
        return ColumnEncryptionAlgorithm::SM4_SM3;
    } else if (strcasecmp(alg, "AES_256_GCM") == 0) {
        return ColumnEncryptionAlgorithm::AES_256_GCM_ALGO;
    }
    return ColumnEncryptionAlgorithm::INVALID_ALGORITHM;
}

inline CmkKeyStore get_key_store_from_string(const char *key_store)
{
    if (key_store == NULL || strlen(key_store) == 0) {
        return CmkKeyStore::INVALID_KEYSTORE;
    }
    if (strcasecmp(key_store, "gs_ktool") == 0) {
        return CmkKeyStore::GS_KTOOL;
    }
    if (strcasecmp(key_store, "localkms") == 0) {
        return CmkKeyStore::LOCALKMS;
    }
    return CmkKeyStore::INVALID_KEYSTORE;
}

inline CmkAlgorithm get_algorithm_from_string(const char *algorithm)
{
    if (algorithm == NULL || strlen(algorithm) == 0) {
        return CmkAlgorithm::INVALID_ALGORITHM;
    }

    if (strcasecmp(algorithm, "AES_256_CBC") == 0) {
        return CmkAlgorithm::AES_256_CBC;
    } else if (strcasecmp(algorithm, "SM4") == 0) {
        return CmkAlgorithm::SM4;
    } else if (strcasecmp(algorithm, "RSA_2048") == 0) {
        return CmkAlgorithm::RSA_2048;
    } else if (strcasecmp(algorithm, "SM2") == 0) {
        return CmkAlgorithm::SM2;
    } else if (strcasecmp(algorithm, "AES_256_GCM") == 0) {
        return CmkAlgorithm::AES_256_GCM;
    }
    
    return CmkAlgorithm::INVALID_ALGORITHM;
}

inline EncryptionType get_algorithm_type_from_string(const char *algorithm_type)
{
    if (algorithm_type == NULL || strlen(algorithm_type) == 0) {
        return EncryptionType::INVALID_TYPE;
    }
    if (strcasecmp(algorithm_type, "DETERMINISTIC") == 0) {
        return EncryptionType::DETERMINISTIC_TYPE;
    } else if (strcasecmp(algorithm_type, "RANDOMIZED") == 0) {
        return EncryptionType::RANDOMIZED_TYPE;
    }

    return EncryptionType::INVALID_TYPE;
}
#endif /* CLIENT_LOGIC_ENUMS_H */
