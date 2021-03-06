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
#include "pg_config.h"

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

int EncryptionColumnHookExecutor::process_data_impl(const ICachedColumn *cached_column, const unsigned char *data,
    int data_size, unsigned char *processed_data)
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
    unsigned char decrypted_key[MAX_CEK_LENGTH];
    errno_t rc = memset_s(decrypted_key, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
    securec_check_c(rc, "\0", "\0");
    size_t decrypted_key_size = 0;
    errno_t securec_rc = EOK;
    unsigned char *cek_keys = NULL;
    cek_keys = get_cek_keys();
    decrypted_key_size = get_cek_size();

    ColumnEncryptionAlgorithm column_encryption_algorithm = get_column_encryption_algorithm();
    if (column_encryption_algorithm == ColumnEncryptionAlgorithm::INVALID_ALGORITHM) {
        size_t algorithm_str_size(0);
        const char *algorithm_str = NULL;
        get_argument("algorithm", &algorithm_str, algorithm_str_size);
        if (algorithm_str == NULL) {
            printf("ERROR(CLIENT): failed to get column encryption algorithm.\n");
            return -1;
        } else if (strcasecmp(algorithm_str, "AEAD_AES_256_CBC_HMAC_SHA256") == 0) {
            column_encryption_algorithm = ColumnEncryptionAlgorithm::AEAD_AES_256_CBC_HMAC_SHA256;
        } else if (strcasecmp(algorithm_str, "AEAD_AES_128_CBC_HMAC_SHA256") == 0) {
            column_encryption_algorithm = ColumnEncryptionAlgorithm::AEAD_AES_128_CBC_HMAC_SHA256;
        } else {
            printf("ERROR(CLIENT): un-support invalid encryption algorithm.\n");
            return -1;
        }
        set_column_encryption_algorithm(column_encryption_algorithm);
    }

    if (cek_keys != NULL && decrypted_key_size != 0) {
        securec_rc = memcpy_s(decrypted_key, MAX_CEK_LENGTH, cek_keys, decrypted_key_size);
        securec_check_c(securec_rc, "", "");
    } else {
        if (!deprocess_column_encryption_key(encryption_global_hook_executor, decrypted_key,
            &decrypted_key_size, encrypted_key_value, &encrypted_key_value_size)) {
            return -1;
        }
        set_cek_keys((unsigned char *)decrypted_key, decrypted_key_size);
        set_cek_size(decrypted_key_size);
    }

    /* encryption key */
    if (!aesCbcEncryptionKey.generate_keys((unsigned char *)decrypted_key, decrypted_key_size)) {
        errno_t res = memset_s(decrypted_key, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
        securec_check_c(res, "\0", "\0");
        return -1;
    }
    errno_t res = memset_s(decrypted_key, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
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

DecryptDataRes EncryptionColumnHookExecutor::deprocess_data_impl(const unsigned char *data_processed,
    int data_proceeed_size, unsigned char **data, int *data_plain_size)
{
    EncryptionGlobalHookExecutor *encryption_global_hook_executor =
        dynamic_cast<EncryptionGlobalHookExecutor *>(m_global_hook_executor);
    if (!encryption_global_hook_executor) {
        return CLIENT_HEAP_ERR;
    }

    /* get effective encrypted_key */
    size_t encrypted_key_value_size = 0;
    const char *encrypted_key_value = NULL;
    get_argument("encrypted_value", &encrypted_key_value, encrypted_key_value_size);
    if (encrypted_key_value == NULL) {
        printf("ERROR(CLIENT): failed to get key_value.\n");
        return DECRYPT_CEK_ERR;
    }

    AeadAesHamcEncKey aesCbcEncryptionKey;
    unsigned char decrypted_key[MAX_CEK_LENGTH];
    errno_t securec_rc = EOK;
    securec_rc = memset_s(decrypted_key, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
    securec_check_c(securec_rc, "\0", "\0");
    size_t decrypted_key_size = 0;
    unsigned char *cek_keys = NULL;
    cek_keys = get_cek_keys();
    decrypted_key_size = get_cek_size();
    ColumnEncryptionAlgorithm column_encryption_algorithm = get_column_encryption_algorithm();
    if (column_encryption_algorithm == ColumnEncryptionAlgorithm::INVALID_ALGORITHM) {
        size_t algorithm_str_size(0);
        const char *algorithm_str = NULL;
        get_argument("algorithm", &algorithm_str, algorithm_str_size);
        if (algorithm_str == NULL) {
            printf("ERROR(CLIENT): failed to get column encryption algorithm.\n");
            return DECRYPT_CEK_ERR;
        } else if (strcasecmp(algorithm_str, "AEAD_AES_256_CBC_HMAC_SHA256") == 0) {
            column_encryption_algorithm = ColumnEncryptionAlgorithm::AEAD_AES_256_CBC_HMAC_SHA256;
        } else if (strcasecmp(algorithm_str, "AEAD_AES_128_CBC_HMAC_SHA256") == 0) {
            column_encryption_algorithm = ColumnEncryptionAlgorithm::AEAD_AES_128_CBC_HMAC_SHA256;
        }
        set_column_encryption_algorithm(column_encryption_algorithm);
    }

    if (cek_keys != NULL && decrypted_key_size != 0) {
        securec_rc = memcpy_s(decrypted_key, MAX_CEK_LENGTH, cek_keys, decrypted_key_size);
        securec_check_c(securec_rc, "\0", "\0");
    } else {
        if (!deprocess_column_encryption_key(encryption_global_hook_executor, decrypted_key, &decrypted_key_size,
            encrypted_key_value, &encrypted_key_value_size)) {
            return DECRYPT_CEK_ERR;
        }

        set_cek_keys((unsigned char *)decrypted_key, decrypted_key_size);
        set_cek_size(decrypted_key_size);
    }

    /* encryption key */
    if (!aesCbcEncryptionKey.generate_keys((unsigned char *)decrypted_key, decrypted_key_size)) {
        errno_t res = memset_s(decrypted_key, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
        securec_check_c(res, "\0", "\0");
        return DERIVE_CEK_ERR;
    }

    errno_t res = memset_s(decrypted_key, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
    securec_check_c(res, "\0", "\0");
    if (data_proceeed_size <= 0) {
        printf("ERROR(CLIENT): data processed size(%d) is invalid.\n", data_proceeed_size);
        return INVALID_DATA_LEN;
    }
    *data = (unsigned char *)malloc(data_proceeed_size);
    if (!(*data)) {
        printf("ERROR(CLIENT): allocation failure.\n");
        return CLIENT_HEAP_ERR;
    }
    res = memset_s(*data, data_proceeed_size, 0, data_proceeed_size);
    securec_check_c(res, "\0", "\0");
    int plainTextSize =
        decrypt_data(data_processed, data_proceeed_size, aesCbcEncryptionKey, *data, column_encryption_algorithm);
    if (plainTextSize <= 0) {
        printf("ERROR(CLIENT): failed to decrypt data.\n");
        free(*data);
        *data = NULL;
        return DEC_DATA_ERR;
    }

    res = memset_s((*data) + plainTextSize, data_proceeed_size - plainTextSize, '\0',
        data_proceeed_size - plainTextSize);
    securec_check_c(res, "\0", "\0");
    *data_plain_size = plainTextSize;
    return DEC_DATA_SUCCEED;
}

/* decrypting cek when can not get decrypted cek from cache */
bool EncryptionColumnHookExecutor::deprocess_column_encryption_key(
    EncryptionGlobalHookExecutor *encryption_global_hook_executor, unsigned char *decrypted_key,
    size_t *decrypted_key_size, const char *encrypted_key_value, const size_t *encrypted_key_value_size) const
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

    CmkKeyStore keyStore = get_key_store_from_string(key_store_str);
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    /* read cmk plain from gs_ktool */
    unsigned char cmk_plain[DEFAULT_CMK_LEN + 1] = {0};
    unsigned int cmk_id = 0;

    if (keyStore == CmkKeyStore::GS_KTOOL) {
        if (!kt_atoi(key_path_str, &cmk_id)) {
            return false;
        }

        /* read cmk plain from gs_ktool */
        if (!read_cmk_plain(cmk_id, cmk_plain)) {
            return false;
        }

        if (!decrypt_cek_use_aes256((const unsigned char *)encrypted_key_value, *encrypted_key_value_size, cmk_plain,
            decrypted_key, decrypted_key_size)) {
            return false;
        }
    } else {
        printf("ERROR(CLIENT): Invalid key store type, only support gs_ktool now.\n");
        return false;
    }
#else
    RealCmkPath real_cmk_path = {0};
    KmsErrType err_type = SUCCEED;

    if (keyStore == CmkKeyStore::LOCALKMS) {
        err_type = get_and_check_real_key_path(key_path_str, &real_cmk_path, READ_KEY_FILE);
        if (err_type != SUCCEED) {
            handle_kms_err(err_type);
            return false;
        }

        if (decrypt_cek_use_rsa2048((const unsigned char *)encrypted_key_value, *encrypted_key_value_size,
            real_cmk_path.real_priv_cmk_path, sizeof(real_cmk_path.real_priv_cmk_path),
            decrypted_key, decrypted_key_size) != SUCCEED) {
            return false;
        }
    } else {
        printf("ERROR(CLIENT): Invalid key store type, only support localkms now.\n");
        return false;
    }
#endif
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

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
static bool encrypt_cek(const char *key_path_str, const unsigned char *cek_plain, size_t cek_plain_size,
    unsigned char *cek_ciph, size_t &cek_ciph_len)
{
    /* read cmk plain from gs_ktool */
    unsigned char cmk_plain[DEFAULT_CMK_LEN + 1] = {0};
    unsigned int cmk_id = 0;

    if (!kt_atoi(key_path_str, &cmk_id)) {
        return false;
    }

    if (!read_cmk_plain(cmk_id, cmk_plain)) {
        return false;
    }

    if (!encrypt_cek_use_aes256(cek_plain, cek_plain_size, cmk_plain, cek_ciph, cek_ciph_len)) {
        return false;
    }

    return true;
}
#else
static bool encrypt_cek(const char *key_path_str, const unsigned char *cek_plain, const size_t plain_len,
    unsigned char *cek_cipher, size_t &cipher_len)
{
    /* read cmk plain from localkms */
    RealCmkPath real_cmk_path = {0};
    KmsErrType err_type = SUCCEED;

    err_type = get_and_check_real_key_path(key_path_str, &real_cmk_path, READ_KEY_FILE);
    if (err_type != SUCCEED) {
        handle_kms_err(err_type);
        return false;
    }

    if (encrypt_cek_use_ras2048(cek_plain, plain_len, real_cmk_path.real_pub_cmk_path,
        sizeof(real_cmk_path.real_pub_cmk_path), cek_cipher, cipher_len) != SUCCEED) {
        handle_kms_err(err_type);
        return false;
    }

    return true;
}
#endif

static unsigned char *create_or_check_cek(unsigned char *cek_plain, size_t &cek_plain_len)
{    
    /* if user has not set cek plain by use "ENCRYPTED_VALUE=" while CREATE CEK, we will create new */
    if (cek_plain == NULL) {
        cek_plain = (unsigned char *)malloc(MAX_CEK_LENGTH * sizeof(char));
        if (cek_plain == NULL) {
            printf("ERROR(CLIENT): failed to .\n");
            return NULL;
        }

        AeadAesHamcEncKey::generate_root_key(cek_plain, cek_plain_len);
        return cek_plain;
    } else { /* else, we only need to check it */
        if (cek_plain_len < (112 / 8 * 2)) {
            printf("ERROR(CLIENT): encryption key too short\n");
            return NULL;
        } else if (cek_plain_len > MAX_CEK_LENGTH) {
            printf("ERROR(CLIENT): encryption key too long\n");
            return NULL;
        } else {
            return cek_plain;
        }
    }
}

void free_after_check(bool condition, unsigned char *ptr)
{
    if (condition) {
        free(ptr);
        ptr = NULL;
    }
}

bool EncryptionColumnHookExecutor::pre_create(PGClientLogic &column_encryption, const StringArgs &args,
    StringArgs &new_args)
{
    bool has_user_set_cek = false;
    
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
    const char *algorithm_str = NULL;
    size_t expected_value_size(0);

    algorithm_str = args.find("algorithm");
    if (algorithm_str != NULL) {
        column_encryption_algorithm = get_cek_algorithm_from_string(algorithm_str);
    }
    expected_value = args.find("encrypted_value");
    if (expected_value != NULL) {
        has_user_set_cek = true;
        expected_value_size = strlen(expected_value);
    }

    PGconn *conn = column_encryption.m_conn;

    if (column_encryption_algorithm == ColumnEncryptionAlgorithm::INVALID_ALGORITHM) {
        printf("ERROR(CLIENT): invalid algorithm.\n");
        /* by returning false -  make sure the parser is not run again in search of DDL's */
        return false;
    }

    /* genereate CEK if not provided (common case) */
    expected_value = (const char *)create_or_check_cek((unsigned char *)expected_value, expected_value_size);
    if (expected_value == NULL) {
        return false;
    }

    set_cek_keys((unsigned char *)expected_value, expected_value_size);
    set_cek_size(expected_value_size);
    set_column_encryption_algorithm(column_encryption_algorithm);
    /* encrypt CEK */
    unsigned char encrypted_value[MAX_CEK_LENGTH * 2];
    size_t encrypted_value_size(0);
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    if (keyStore == CmkKeyStore::GS_KTOOL) {
        if (!encrypt_cek(key_path_str, (unsigned char *)expected_value, expected_value_size,
            encrypted_value, encrypted_value_size)) {
            free_after_check(!has_user_set_cek, (unsigned char *)expected_value);
            return false;
        }
    } else {
        printf("ERROR(CLIENT): Invalid key store type, only support gs_ktool now.\n");
        return false;
    }
#else
    if (keyStore == CmkKeyStore::LOCALKMS) {
        if (!encrypt_cek(key_path_str, (unsigned char *)expected_value, expected_value_size, encrypted_value,
            encrypted_value_size) != SUCCEED) {
            free_after_check(!has_user_set_cek, (unsigned char *)expected_value);
            return false;
        }
    } else {
        printf("ERROR(CLIENT): Invalid key store type, only support localkms now.\n");
        return false;
    }
#endif
    free_after_check(!has_user_set_cek, (unsigned char *)expected_value);

    /* escape encrypted CEK for BYTEA insertion */
    size_t encoded_encrypted_value_size;
    char *encoded_encrypted_value = (char *)PQescapeByteaConn(conn, (const unsigned char *)encrypted_value,
        encrypted_value_size, &encoded_encrypted_value_size);
    if (encoded_encrypted_value == NULL) {
        printf("ERROR(CLIENT): failed to encode encrypted values.\n");
        return false;
    } else {
        new_args.set("encrypted_value", encoded_encrypted_value, encoded_encrypted_value_size - 1);
        free(encoded_encrypted_value);
        encoded_encrypted_value = NULL;
    }
    return true;
}

const char *EncryptionColumnHookExecutor::get_data_type(const ColumnDef * const column)
{
    char *data_type = NULL;
    data_type = (char *)malloc(MAX_DATATYPE_LEN);
    if (data_type == NULL) {
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
