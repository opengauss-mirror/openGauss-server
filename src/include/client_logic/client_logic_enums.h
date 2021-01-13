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
 * client_logic_enums.h
 *
 * IDENTIFICATION
 *	  src\include\client_logic\client_logic_enums.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CLIENT_LOGIC_ENUMS_H
#define CLIENT_LOGIC_ENUMS_H

#include <string.h>

typedef enum class CmkKeyStore {
    INVALID_KEYSTORE = -1,
    LOCALKMS
} CmkKeyStore;

typedef enum class CmkAlgorithm {
    INVALID_ALGORITHM,
    RAS_2048
} CmkAlgorithm;

typedef enum class EncryptionType {
    INVALID_TYPE,
    RANDOMIZED_TYPE,
    DETERMINISTIC_TYPE
} EncryptionType;


typedef enum ColumnEncryptionAlgorithm {
    INVALID_ALGORITHM,
    AEAD_AES_256_CBC_HMAC_SHA256,
    AEAD_AES_128_CBC_HMAC_SHA256
} ColumnEncryptionAlgorithm;


inline ColumnEncryptionAlgorithm get_cek_algorithm_from_string(const char *alg)
{
    if (alg == NULL || strlen(alg) == 0) {
        return ColumnEncryptionAlgorithm::INVALID_ALGORITHM;
    }
    if (strncasecmp(alg, "AEAD_AES_256_CBC_HMAC_SHA256", strlen("AEAD_AES_256_CBC_HMAC_SHA256")) == 0) {
        return ColumnEncryptionAlgorithm::AEAD_AES_256_CBC_HMAC_SHA256;
    } else if (strncasecmp(alg, "AEAD_AES_128_CBC_HMAC_SHA256", strlen("AEAD_AES_128_CBC_HMAC_SHA256")) == 0) {
        return ColumnEncryptionAlgorithm::AEAD_AES_128_CBC_HMAC_SHA256;
    }
    return ColumnEncryptionAlgorithm::INVALID_ALGORITHM;
}

inline CmkKeyStore get_key_store_from_string(const char *key_store)
{
    if (key_store == NULL || strlen(key_store) == 0) {
        return CmkKeyStore::INVALID_KEYSTORE;
    }
    
    if (strncasecmp(key_store, "gs_ktool", strlen("gs_ktool")) == 0) {
        return CmkKeyStore::LOCALKMS;
    } 

    return CmkKeyStore::INVALID_KEYSTORE;
}

inline CmkAlgorithm get_algorithm_from_string(const char *algorithm)
{
    if (algorithm == NULL || strlen(algorithm) == 0) {
        return CmkAlgorithm::INVALID_ALGORITHM;
    }

    if (strncasecmp(algorithm, "RAS_2048", strlen("RAS_2048")) == 0) {
        return CmkAlgorithm::RAS_2048;
    }

    return CmkAlgorithm::INVALID_ALGORITHM;
}

inline EncryptionType get_algorithm_type_from_string(const char *algorithm_type)
{
    if (algorithm_type == NULL || strlen(algorithm_type) == 0) {
        return EncryptionType::INVALID_TYPE;
    }
    if (strncasecmp(algorithm_type, "DETERMINISTIC", strlen("DETERMINISTIC")) == 0) {
        return EncryptionType::DETERMINISTIC_TYPE;
    } else if (strncasecmp(algorithm_type, "RANDOMIZED", strlen("RANDOMIZED")) == 0) {
        return EncryptionType::RANDOMIZED_TYPE;
    }

    return EncryptionType::INVALID_TYPE;
}
#endif /* CLIENT_LOGIC_ENUMS_H */
