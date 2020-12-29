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
 * encryption_column_hook_executor.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\encryption_hooks\encryption_column_hook_executor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <iostream>
#include <memory>
#include <string.h>
#include <strings.h>
#include <algorithm>
#include "encryption_column_hook_executor.h"
#include "encryption_global_hook_executor.h"
#include "client_logic_cache/cached_column_setting.h"
#include "client_logic_cache/cached_column_manager.h"
#include "libpq-fe.h"
#include "cl_state.h"
#include "zlib.h"
#include "libpq-int.h"
#include "catalog/pg_type.h"

int EncryptionColumnHookExecutor::get_estimated_processed_data_size_impl(int data_size) const
{
    return get_cipher_text_size(data_size);
}

int EncryptionColumnHookExecutor::process_data_impl(bool is_during_refresh_cache, const ICachedColumn *cached_column,
    const unsigned char *data, int data_size, unsigned char *processed_data)
{
    EncryptionGlobalHookExecutor *encryption_global_hook_executor =
        dynamic_cast<EncryptionGlobalHookExecutor *>(m_global_hook_executor);
    if (!encryption_global_hook_executor) {
        return -1;
    }

    size_t encrypted_key_value_size = 0;
    const char *encrypted_key_value = NULL;

    /* get effective encrypted_key */
    get_argument("encrypted_value", &encrypted_key_value, encrypted_key_value_size);
    if (encrypted_key_value == NULL) {
        printf("ERROR(CLIENT): failed to get key_value.\n");
        return -1;
    }
    AeadAesHamcEncKey aesCbcEncryptionKey;
    unsigned char decryptedKey[MAX_CEK_LENGTH];
    errno_t rc = memset_s(decryptedKey, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
    securec_check_c(rc, "\0", "\0");
    size_t decryptedKeySize = 0;
    errno_t securec_rc = EOK;
    unsigned char *cek_keys = NULL;
    cek_keys = get_cek_keys();
    decryptedKeySize = get_cek_size();

    ColumnEncryptionAlgorithm column_encryption_algorithm = get_column_encryption_algorithm();
    if (column_encryption_algorithm == ColumnEncryptionAlgorithm::INVALID_ALGORITHM) {
        size_t algorithm_str_size(0);
        const char *algorithm_str = NULL;
        get_argument("algorithm", &algorithm_str, algorithm_str_size);
        if (algorithm_str == NULL) {
            printf("ERROR(CLIENT): failed to get column encryption algorithm.\n");
            return -1;
        } else if (strncasecmp(algorithm_str, "AEAD_AES_256_CBC_HMAC_SHA256", strlen("AEAD_AES_256_CBC_HMAC_SHA256")) ==
            0) {
            column_encryption_algorithm = ColumnEncryptionAlgorithm::AEAD_AES_256_CBC_HMAC_SHA256;
        } else if (strncasecmp(algorithm_str, "AEAD_AES_128_CBC_HMAC_SHA256", strlen("AEAD_AES_128_CBC_HMAC_SHA256")) ==
            0) {
            column_encryption_algorithm = ColumnEncryptionAlgorithm::AEAD_AES_128_CBC_HMAC_SHA256;
        } else {
            printf("ERROR(CLIENT): un-support invalid encryption algorithm.\n");
            return -1;
        }
        set_column_encryption_algorithm(column_encryption_algorithm);
    }

    if (cek_keys != NULL && decryptedKeySize != 0) {
        securec_rc = memcpy_s(decryptedKey, MAX_CEK_LENGTH, cek_keys, decryptedKeySize);
        securec_check_c(securec_rc, "", "");
    } else {
        is_during_refresh_cache = true;
        if (!deprocess_column_encryption_key(is_during_refresh_cache, encryption_global_hook_executor, decryptedKey,
            &decryptedKeySize, encrypted_key_value, &encrypted_key_value_size)) {
            return -1;
        }
        set_cek_keys((unsigned char *)decryptedKey, decryptedKeySize);
        set_cek_size(decryptedKeySize);
    }

    /* encryption key */
    if (!aesCbcEncryptionKey.generate_keys((unsigned char *)decryptedKey, decryptedKeySize)) {
        errno_t res = memset_s(decryptedKey, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
        securec_check_c(res, "\0", "\0");
        return -1;
    }
    errno_t res = memset_s(decryptedKey, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
    securec_check_c(res, "\0", "\0");
    Oid data_type = cached_column->get_data_type();
    EncryptionType encryption_type = EncryptionType::INVALID_TYPE;
    if (data_type == BYTEAWITHOUTORDERWITHEQUALCOLOID) {
        encryption_type = EncryptionType::DETERMINISTIC_TYPE;
    } else if (data_type == BYTEAWITHOUTORDERCOLOID) {
        encryption_type = EncryptionType::RANDOMIZED_TYPE;
    }
    int encryptedSize = encrypt_data(data, data_size, aesCbcEncryptionKey, encryption_type, processed_data,
        column_encryption_algorithm);
    return encryptedSize;
}

int EncryptionColumnHookExecutor::deprocess_data_impl(bool is_during_refresh_cache, const unsigned char *dataProcessed,
    int dataProcessedSize, unsigned char **data)
{
    EncryptionGlobalHookExecutor *encryption_global_hook_executor =
        dynamic_cast<EncryptionGlobalHookExecutor *>(m_global_hook_executor);
    if (!encryption_global_hook_executor) {
        return -1;
    }
    /* get effective encrypted_key */
    size_t encrypted_key_value_size = 0;
    const char *encrypted_key_value = NULL;
    get_argument("encrypted_value", &encrypted_key_value, encrypted_key_value_size);
    if (encrypted_key_value == NULL) {
        printf("ERROR(CLIENT): failed to get key_value.\n");
        return -1;
    }

    AeadAesHamcEncKey aesCbcEncryptionKey;
    unsigned char decryptedKey[MAX_CEK_LENGTH];
    errno_t securec_rc = EOK;
    securec_rc = memset_s(decryptedKey, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
    securec_check_c(securec_rc, "\0", "\0");
    size_t decryptedKeySize = 0;
    unsigned char *cek_keys = NULL;
    cek_keys = get_cek_keys();
    decryptedKeySize = get_cek_size();
    ColumnEncryptionAlgorithm column_encryption_algorithm = get_column_encryption_algorithm();
    if (column_encryption_algorithm == ColumnEncryptionAlgorithm::INVALID_ALGORITHM) {
        size_t algorithm_str_size(0);
        const char *algorithm_str = NULL;
        get_argument("algorithm", &algorithm_str, algorithm_str_size);
        if (algorithm_str == NULL) {
            printf("ERROR(CLIENT): failed to get column encryption algorithm.\n");
            return -1;
        } else if (strncasecmp(algorithm_str, "AEAD_AES_256_CBC_HMAC_SHA256", strlen("AEAD_AES_256_CBC_HMAC_SHA256")) ==
            0) {
            column_encryption_algorithm = ColumnEncryptionAlgorithm::AEAD_AES_256_CBC_HMAC_SHA256;
        } else if (strncasecmp(algorithm_str, "AEAD_AES_128_CBC_HMAC_SHA256", strlen("AEAD_AES_128_CBC_HMAC_SHA256")) ==
            0) {
            column_encryption_algorithm = ColumnEncryptionAlgorithm::AEAD_AES_128_CBC_HMAC_SHA256;
        }
        set_column_encryption_algorithm(column_encryption_algorithm);
    }
    if (cek_keys != NULL && decryptedKeySize != 0) {
        securec_rc = memcpy_s(decryptedKey, MAX_CEK_LENGTH, cek_keys, decryptedKeySize);
        securec_check_c(securec_rc, "\0", "\0");
    } else {
        if (!deprocess_column_encryption_key(is_during_refresh_cache, encryption_global_hook_executor, decryptedKey,
            &decryptedKeySize, encrypted_key_value, &encrypted_key_value_size)) {
            return -1;
        }
        set_cek_keys((unsigned char *)decryptedKey, decryptedKeySize);
        set_cek_size(decryptedKeySize);
    }

    /* encryption key */
    if (!aesCbcEncryptionKey.generate_keys((unsigned char *)decryptedKey, decryptedKeySize)) {
        errno_t res = memset_s(decryptedKey, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
        securec_check_c(res, "\0", "\0");
        return -1;
    }
    errno_t res = memset_s(decryptedKey, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
    securec_check_c(res, "\0", "\0");
    if (dataProcessedSize <= 0) {
        printf("ERROR(CLIENT): data processed size(%d) is invalid.\n", dataProcessedSize);
        return -1;
    }
    *data = (unsigned char *)malloc(dataProcessedSize);
    if (!(*data)) {
        printf("ERROR(CLIENT): allocation failure.\n");
        return -1;
    }
    res = memset_s(*data, dataProcessedSize, 0, dataProcessedSize);
    securec_check_c(res, "\0", "\0");
    int plainTextSize =
        decrypt_data(dataProcessed, dataProcessedSize, aesCbcEncryptionKey, *data, column_encryption_algorithm);
    if (plainTextSize <= 0) {
        printf("ERROR(CLIENT): failed to decrypt data.\n");
        free(*data);
        *data = NULL;
        return -1;
    }

    res = memset_s((*data) + plainTextSize, dataProcessedSize - plainTextSize, '\0', dataProcessedSize - plainTextSize);
    securec_check_c(res, "\0", "\0");
    return plainTextSize;
}

/* decrypting cek when can not get decrypted cek from cache */
bool EncryptionColumnHookExecutor::deprocess_column_encryption_key(bool is_during_refresh_cache,
    EncryptionGlobalHookExecutor *encryption_global_hook_executor, unsigned char *decryptedKey,
    size_t *decryptedKeySize, const char *encrypted_key_value, const size_t *encrypted_key_value_size) const
{
    size_t key_path_size(0);
    const char *key_path_str = NULL;
    encryption_global_hook_executor->get_argument("key_path", &key_path_str, key_path_size);
    if (key_path_str == NULL || strlen(key_path_str) == 0) {
        printf("ERROR(CLIENT): failed to get key path!\n");
        return false;
    }

    size_t key_store_size(0);
    const char *key_store_str = NULL;
    encryption_global_hook_executor->get_argument("key_store", &key_store_str, key_store_size);
    if (!key_store_str || strlen(key_store_str) == 0) {
        printf("ERROR(CLIENT): failed to get key store!\n");
        return false;
    }

    /* read cmk plain from gs_ktool */
    bool is_report_err = is_during_refresh_cache;
    unsigned char cmk_plain[DEFAULT_CMK_LEN + 1] = {0};
    unsigned int cmk_id = 0;

    /* 
     * to determine whether need to refresh cache and try PQexec(query) again :
     * case 1 : report error directly
     * case 2 : do not report error and try again
     */
    CmkKeyStore keyStore = get_key_store_from_string(key_store_str);
    if (keyStore == CmkKeyStore::GS_KTOOL) {
        if (!kt_atoi(key_path_str, &cmk_id)) {
            return false;
        }

        if (!read_cmk_plain(cmk_id, cmk_plain, is_report_err)) {
            return false;
        }

        if (!decrypt_cek_with_aes_256((const unsigned char *)encrypted_key_value, *encrypted_key_value_size, cmk_plain,
            (unsigned char *)decryptedKey, decryptedKeySize, is_report_err)) {
            return false;
        }
    } else {
        printf("ERROR(CLIENT): Invalid key store type, only support gs_ktool now.\n");
        return false;
    }

    return true;
}

bool EncryptionColumnHookExecutor::is_set_operation_allowed(const ICachedColumn *cached_column) const
{
    Oid data_type = cached_column->get_data_type();
    if (data_type == BYTEAWITHOUTORDERCOLOID) {
        return false;
    }
    return true;
}

bool EncryptionColumnHookExecutor::is_operator_allowed(const ICachedColumn *ce, const char * const op) const
{
    if (!is_set_operation_allowed(ce)) {
        return false;
    }
    const char*  allowed_operators [] = {"=", "!=", "<>"};
    for (size_t i = 0; i < sizeof(allowed_operators) / sizeof(const char *); i++) {
        if (strcmp(op, allowed_operators[i]) == 0) {
            return true;
        }
    }
    return false;
}

bool EncryptionColumnHookExecutor::pre_create(PGClientLogic &column_encryption, const StringArgs &args,
    StringArgs &new_args)
{
    EncryptionGlobalHookExecutor *encryption_global_hook_executor =
        dynamic_cast<EncryptionGlobalHookExecutor *>(m_global_hook_executor);
    if (!encryption_global_hook_executor) {
        return false;
    }

    size_t key_path_size(0);
    const char *key_path_str = NULL;
    encryption_global_hook_executor->get_argument("key_path", &key_path_str, key_path_size);
    if (key_path_str == NULL || strlen(key_path_str) == 0) {
        printf("ERROR(CLIENT): failed to get key path!\n");
        return false;
    }

    size_t key_store_size(0);
    const char *key_store_str = NULL;
    encryption_global_hook_executor->get_argument("key_store", &key_store_str, key_store_size);

    if (key_store_str == NULL || strlen(key_store_str) == 0) {
        printf("ERROR(CLIENT): failed to get key store!\n");
        return false;
    }

    CmkKeyStore keyStore = get_key_store_from_string(key_store_str);

    ColumnEncryptionAlgorithm column_encryption_algorithm(ColumnEncryptionAlgorithm::INVALID_ALGORITHM);
    const char *expected_value = NULL;
    char *common_expected_value = NULL;
    const char *algorithm_str = NULL;
    size_t expected_value_size(0);

    algorithm_str = args.find("algorithm");
    if (algorithm_str != NULL) {
        column_encryption_algorithm = get_cek_algorithm_from_string(algorithm_str);
    }
    expected_value = args.find("encrypted_value");
    if (expected_value != NULL) {
        expected_value_size = strlen(expected_value);
    }

    PGconn *conn = column_encryption.m_conn;

    if (column_encryption_algorithm == ColumnEncryptionAlgorithm::INVALID_ALGORITHM) {
        printf("ERROR(CLIENT): syntax error parsing cek creation query.\n");
        /* by returning false -  make sure the parser is not run again in search of DDL's */
        return false;
    }

    /* genereate CEK if not provided (common case) */
    if (expected_value == NULL) {
        common_expected_value = (char *)malloc(MAX_CEK_LENGTH * sizeof(char));
        expected_value = common_expected_value;
        if (expected_value == NULL) {
            printf("ERROR(CLIENT): out of memory.\n");
            return false;
        }
        AeadAesHamcEncKey::generate_root_key((unsigned char *)expected_value, expected_value_size);
    } else if (expected_value_size < (112 / 8 * 2)) {
        printfPQExpBuffer(&(conn->errorMessage), libpq_gettext("ERROR(CLIENT): encryption key too short\n"));
        return false;
    } else if (expected_value_size > MAX_CEK_LENGTH) {
        printfPQExpBuffer(&(conn->errorMessage), libpq_gettext("ERROR(CLIENT): encryption key too long\n"));
        return false;
    }
    set_cek_keys((unsigned char *)expected_value, expected_value_size);
    set_cek_size(expected_value_size);
    set_column_encryption_algorithm(column_encryption_algorithm);
    /* encrypt CEK */
    unsigned char encrypted_value[MAX_CEK_LENGTH * 2];
    size_t encrypted_value_size(0);

    /* read cmk plain from gs_ktool */
    bool is_report_err = true;
    unsigned char cmk_plain[DEFAULT_CMK_LEN + 1] = {0};
    unsigned int cmk_id = 0;

    if (keyStore == CmkKeyStore::GS_KTOOL) {
        if (!kt_atoi(key_path_str, &cmk_id)) {
            libpq_free(common_expected_value);
            return false;
        }

        if (!read_cmk_plain(cmk_id, cmk_plain, is_report_err)) {
            libpq_free(common_expected_value);
            return false;
        }

        if (!encrypt_cek_with_aes_256((unsigned char *)expected_value, expected_value_size, cmk_plain,
            (unsigned char *)encrypted_value, encrypted_value_size, is_report_err)) {
            libpq_free(common_expected_value);
            return false;
        }
    } else {
        printf("ERROR(CLIENT): Invalid key store type, only support gs_ktool now.\n");
        libpq_free(common_expected_value);
        return false;
    }

    /* escape encrypted CEK for BYTEA insertion */
    size_t encoded_encrypted_value_size;
    char *encoded_encrypted_value = (char *)PQescapeByteaConn(conn, (const unsigned char *)encrypted_value,
        encrypted_value_size, &encoded_encrypted_value_size);
    if (encoded_encrypted_value == NULL) {
        printf("ERROR(CLIENT): failed to encode encrypted values.\n");
        libpq_free(common_expected_value);
        return false;
    } else {
        new_args.set("encrypted_value", encoded_encrypted_value, encoded_encrypted_value_size - 1);
        free(encoded_encrypted_value);
        encoded_encrypted_value = NULL;
    }
    libpq_free(common_expected_value);
    return true;
}

const char *EncryptionColumnHookExecutor::get_data_type(const ColumnDef * const column)
{
    char *data_type = NULL;
    data_type = (char *)malloc(MAX_DATATYPE_LEN);
    if (NULL == data_type) {
        printf("ERROR(CLIENT): out of memory.\n");
        return data_type;
    }
    errno_t rc = EOK;
    rc = memset_s(data_type, MAX_DATATYPE_LEN, 0, MAX_DATATYPE_LEN);
    securec_check_c(rc, "\0", "\0");
    if (column->clientLogicColumnRef != NULL) {
        switch (column->clientLogicColumnRef->columnEncryptionAlgorithmType) {
            case EncryptionType::RANDOMIZED_TYPE:
                rc = memcpy_s((char *)data_type, MAX_DATATYPE_LEN, (char *)"byteawithoutorder",
                    strlen("byteawithoutorder"));
                securec_check_c(rc, "\0", "\0");
                break;
            case EncryptionType::DETERMINISTIC_TYPE:
                rc = memcpy_s((char *)data_type, MAX_DATATYPE_LEN, (char *)"byteawithoutorderwithequal",
                    strlen("byteawithoutorderwithequal"));
                securec_check_c(rc, "\0", "\0");
                break;
            default:
                rc = memcpy_s((char *)data_type, MAX_DATATYPE_LEN, (char *)"byteawithoutorderwithequal",
                    strlen("byteawithoutorderwithequal"));
                securec_check_c(rc, "\0", "\0");
                break;
        }
    }
    return data_type;
}

void EncryptionColumnHookExecutor::set_data_type(const ColumnDef * const column, ICachedColumn *ce)
{
    switch (column->clientLogicColumnRef->columnEncryptionAlgorithmType) {
        case EncryptionType::RANDOMIZED_TYPE:
            ce->set_data_type(BYTEAWITHOUTORDERCOLOID);
            break;
        case EncryptionType::DETERMINISTIC_TYPE:
            ce->set_data_type(BYTEAWITHOUTORDERWITHEQUALCOLOID);
            break;
        default:
            break;
    }
    return;
}

void EncryptionColumnHookExecutor::save_private_variables()
{
    const char *got = m_values_map.find("algorithm");
    if (got != NULL) {
        check_strncpy_s(strncpy_s(m_algorithm, sizeof(m_algorithm), got, strlen(got)));
    }
}
