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
#include "pqexpbuffer.h"
#include "catalog/pg_type.h"
#include "keymgr/security_key_adpt.h"

bool is_cmk_algo_sm(CmkAlgorithm cmk_algo)
{
    if (cmk_algo == CmkAlgorithm::SM2 || cmk_algo == CmkAlgorithm::SM4) {
        return true;
    }

    return false;
}

bool is_cek_algo_sm(ColumnEncryptionAlgorithm cek_algo)
{
    if (cek_algo == ColumnEncryptionAlgorithm::SM4_SM3) {
        return true;
    }

    return false;
}

bool is_sm_algo_used_together(CmkAlgorithm cmk_algo, ColumnEncryptionAlgorithm cek_algo)
{
    return (is_cmk_algo_sm(cmk_algo) == is_cek_algo_sm(cek_algo));
}

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
    PGClientLogic &PgClientLogic = encryption_global_hook_executor->get_client_logic();
    PGconn* conn = PgClientLogic.m_conn;
    
    size_t encrypted_key_value_size = 0;
    const char *encrypted_key_value = NULL;
    /* get effective encrypted_key */
    get_argument("encrypted_value", &encrypted_key_value, encrypted_key_value_size);
    if (encrypted_key_value == NULL) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): failed to get key_value.\n"));
        return -1;
    }

    ColumnEncryptionAlgorithm column_encryption_algorithm = get_column_encryption_algorithm();
    if (column_encryption_algorithm == ColumnEncryptionAlgorithm::INVALID_ALGORITHM) {
        size_t algorithm_str_size(0);
        const char *algorithm_str = NULL;

        get_argument("algorithm", &algorithm_str, algorithm_str_size);
        if (algorithm_str == NULL) {
            printfPQExpBuffer(&conn->errorMessage,
                libpq_gettext("ERROR(CLIENT): failed to get column encryption algorithm.\n"));
            return -1;
        } 
        
        column_encryption_algorithm = get_cek_algorithm_from_string(algorithm_str);
        if (column_encryption_algorithm == ColumnEncryptionAlgorithm::INVALID_ALGORITHM) {
            printfPQExpBuffer(&conn->errorMessage,
                libpq_gettext("ERROR(CLIENT): un-support invalid encryption algorithm.\n"));
            return -1;
        }
        set_column_encryption_algorithm(column_encryption_algorithm);
    }

    if (!is_cek_set) {
        size_t decrypted_key_size = 0;
        unsigned char encrypted_key[MAX_CEK_LENGTH];
        errno_t rc = memset_s(encrypted_key, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
        securec_check_c(rc, "\0", "\0");
        if (!deprocess_column_encryption_key(encryption_global_hook_executor, encrypted_key,
            &decrypted_key_size, encrypted_key_value, &encrypted_key_value_size)) {
            return -1;
        }
        if (!set_cek_keys((unsigned char *)encrypted_key, decrypted_key_size)) {
            return -1;
        }
        errno_t res = memset_s(encrypted_key, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
        securec_check_c(res, "\0", "\0");
    }
    Oid data_type = cached_column->get_data_type();
    EncryptionType encryption_type = EncryptionType::INVALID_TYPE;
    if (data_type == BYTEAWITHOUTORDERWITHEQUALCOLOID) {
        encryption_type = EncryptionType::DETERMINISTIC_TYPE;
    } else if (data_type == BYTEAWITHOUTORDERCOLOID) {
        encryption_type = EncryptionType::RANDOMIZED_TYPE;
    }
    int encryptedSize = encrypt_data((unsigned char *)data, data_size, aesCbcEncryptionKey, encryption_type,
        processed_data, column_encryption_algorithm);
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
    PGClientLogic &PgClientLogic = encryption_global_hook_executor->get_client_logic();
    PGconn* conn = PgClientLogic.m_conn;
    /* get effective encrypted_key */
    size_t encrypted_key_value_size = 0;
    const char *encrypted_key_value = NULL;
    get_argument("encrypted_value", &encrypted_key_value, encrypted_key_value_size);
    if (encrypted_key_value == NULL) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): failed to get key_value.\n"));
        return DECRYPT_CEK_ERR;
    }
    errno_t res;
    ColumnEncryptionAlgorithm column_encryption_algorithm = get_column_encryption_algorithm();
    if (column_encryption_algorithm == ColumnEncryptionAlgorithm::INVALID_ALGORITHM) {
        size_t algorithm_str_size(0);
        const char *algorithm_str = NULL;
        get_argument("algorithm", &algorithm_str, algorithm_str_size);
        if (algorithm_str == NULL) {
            printfPQExpBuffer(&conn->errorMessage,
                libpq_gettext("ERROR(CLIENT): failed to get column encryption algorithm.\n"));
            return DECRYPT_CEK_ERR;
        }

        column_encryption_algorithm = get_cek_algorithm_from_string(algorithm_str);
        if (column_encryption_algorithm == ColumnEncryptionAlgorithm::INVALID_ALGORITHM) {
            printfPQExpBuffer(&conn->errorMessage,
                libpq_gettext("ERROR(CLIENT): un-support invalid encryption algorithm.\n"));
            return DECRYPT_CEK_ERR;
        }
        
        set_column_encryption_algorithm(column_encryption_algorithm);
    }

    if (!is_cek_set) {
        size_t decrypted_key_size = 0;
        unsigned char decrypted_key[MAX_CEK_LENGTH];
        errno_t rc = memset_s(decrypted_key, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
        securec_check_c(rc, "\0", "\0");
        if (!deprocess_column_encryption_key(encryption_global_hook_executor, decrypted_key,
            &decrypted_key_size, encrypted_key_value, &encrypted_key_value_size)) {
            return DECRYPT_CEK_ERR;
        }
        if (!set_cek_keys((unsigned char *)decrypted_key, decrypted_key_size)) {
            return DECRYPT_CEK_ERR;
        }
        res = memset_s(decrypted_key, MAX_CEK_LENGTH, 0, MAX_CEK_LENGTH);
        securec_check_c(res, "\0", "\0");
    }

    if (data_proceeed_size <= 0) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): data processed size is invalid.\n"));
        return INVALID_DATA_LEN;
    }
    *data = (unsigned char *)malloc(data_proceeed_size);
    if (!(*data)) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): allocation failure.\n"));
        return CLIENT_HEAP_ERR;
    }
    res = memset_s(*data, data_proceeed_size, 0, data_proceeed_size);
    securec_check_c(res, "\0", "\0");
    int plainTextSize = decrypt_data((unsigned char *)data_processed, data_proceeed_size, aesCbcEncryptionKey, *data,
        column_encryption_algorithm);
    if (plainTextSize <= 0) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): failed to decrypt data.\n"));
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
    PGClientLogic &PgClientLogic = encryption_global_hook_executor->get_client_logic();
    PGconn* conn = PgClientLogic.m_conn;
    size_t unused = 0;
    const char *key_store_str = NULL;
    const char *key_path_str = NULL;
    const char *key_algo_str = NULL;

    encryption_global_hook_executor->get_argument("key_store", &key_store_str, unused);
    encryption_global_hook_executor->get_argument("key_path", &key_path_str, unused);
    encryption_global_hook_executor->get_argument("algorithm", &key_algo_str, unused);

    KeyAdpt *adpt = CL_GET_KEY_ADPT(PgClientLogic);
    KeyInfo info = {key_store_str, key_path_str, key_algo_str};
    KmUnStr cipher = {(unsigned char *)encrypted_key_value, *encrypted_key_value_size};
    KmUnStr plain = {0};
    plain = key_adpt_mk_decrypt(adpt, info, cipher);
    if (key_adpt_catch_err(adpt)) {
        printfPQExpBuffer(&conn->errorMessage, "ERROR(CLIENT): %s\n", key_adpt_get_err(adpt));
        conn->client_logic->is_external_err = true;
        return false;
    }

    size_t i = 0;
    for (; i < plain.len && i < MAX_CEK_LENGTH - 1; i++) {
        decrypted_key[i] = plain.val[i];
    }
    *decrypted_key_size = i;

    free(plain.val);

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

static unsigned char *create_or_check_cek(PGconn* conn, unsigned char *cek_plain, size_t &cek_plain_len)
{    
    /* if user has not set cek plain by use "ENCRYPTED_VALUE=" while CREATE CEK, we will create new */
    if (cek_plain == NULL) {
        cek_plain = (unsigned char *)malloc(MAX_CEK_LENGTH * sizeof(char));
        if (cek_plain == NULL) {
            printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): failed to malloc memory.\n"));
            return NULL;
        }

        AeadAesHamcEncKey::generate_root_key(cek_plain, cek_plain_len);
        return cek_plain;
    } else { /* else, we only need to check it */
        if (cek_plain_len < (112 / 8 * 2)) {
            printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): encryption key too short\n"));
            return NULL;
        } else if (cek_plain_len > MAX_CEK_LENGTH) {
            printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): encryption key too long\n"));
            return NULL;
        } else {
            return cek_plain;
        }
    }
}

void free_after_check(bool condition, unsigned char *ptr, size_t ptr_size)
{
    if (condition) {
        errno_t res = EOK;
        res = memset_s(ptr, MAX_CEK_LENGTH, 0, ptr_size);
        securec_check_c(res, "\0", "\0");
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
    PGconn *conn = column_encryption.m_conn;

    size_t unused = 0;
    const char *cmk_store_str = NULL;
    const char *cmk_path_str = NULL;
    const char *cmk_algo_str = NULL;

    encryption_global_hook_executor->get_argument("key_store", &cmk_store_str, unused);
    encryption_global_hook_executor->get_argument("key_path", &cmk_path_str, unused);
    encryption_global_hook_executor->get_argument("algorithm", &cmk_algo_str, unused);

    ColumnEncryptionAlgorithm column_encryption_algorithm(ColumnEncryptionAlgorithm::INVALID_ALGORITHM);
    char *expected_value = NULL;
    const char *expected_value_find = NULL;
    const char *algorithm_str = NULL;
    size_t expected_value_size(0);

    algorithm_str = args.find("algorithm");
    if (algorithm_str != NULL) {
        column_encryption_algorithm = get_cek_algorithm_from_string(algorithm_str);
    }
    expected_value_find = args.find("encrypted_value");
    if (expected_value_find != NULL) {
        has_user_set_cek = true;
        expected_value_size = strlen(expected_value_find);
    }

    if (column_encryption_algorithm == ColumnEncryptionAlgorithm::INVALID_ALGORITHM) {
        printfPQExpBuffer(&conn->errorMessage,
            libpq_gettext("ERROR(CLIENT): invalid column encryption key algorithm.\n"));
        /* by returning false -  make sure the parser is not run again in search of DDL's */
        return false;
    }

    CmkAlgorithm cmk_algo = get_algorithm_from_string(cmk_algo_str);
    if (!is_sm_algo_used_together(cmk_algo, column_encryption_algorithm)) {
        printfPQExpBuffer(&conn->errorMessage,
            libpq_gettext("ERROR(CLIENT): National secret algorithm must be used together.\n"));
        return false;
    }

    /* genereate CEK if not provided (common case) */
    expected_value = (char *)create_or_check_cek(conn, (unsigned char *)expected_value_find, expected_value_size);
    if (expected_value == NULL) {
        return false;
    }

    set_column_encryption_algorithm(column_encryption_algorithm);
    if (!set_cek_keys((unsigned char *)expected_value, expected_value_size)) {
        free_after_check(!has_user_set_cek, (unsigned char *)expected_value, expected_value_size);
        return false;
    }
    /* encrypt CEK */

    /* encrypt CEK */
    KeyAdpt *adpt = CL_GET_KEY_ADPT(column_encryption);
    KeyInfo info = {cmk_store_str, cmk_path_str, cmk_algo_str};
    KmUnStr plain = {(unsigned char *)expected_value, expected_value_size};
    KmUnStr cipher = {0};
    cipher = key_adpt_mk_encrypt(adpt, info, plain);
    free_after_check(!has_user_set_cek, (unsigned char *)expected_value, expected_value_size);
    if (key_adpt_catch_err(adpt)) {
        printfPQExpBuffer(&conn->errorMessage, "ERROR(CLIENT): %s\n", key_adpt_get_err(adpt));
        conn->client_logic->is_external_err = true;
        return false;
    }

    /* escape encrypted CEK for BYTEA insertion */
    size_t encoded_encrypted_value_size;
    char *encoded_encrypted_value = (char *)PQescapeByteaConn(conn, cipher.val, cipher.len,
        &encoded_encrypted_value_size);
    free(cipher.val);
    if (encoded_encrypted_value == NULL) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): failed to encode encrypted values.\n"));
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
    EncryptionGlobalHookExecutor *encryption_global_hook_executor =
        dynamic_cast<EncryptionGlobalHookExecutor *>(m_global_hook_executor);
    if (!encryption_global_hook_executor) {
        return NULL;
    }
    PGClientLogic &PgClientLogic = encryption_global_hook_executor->get_client_logic();
    PGconn* conn = PgClientLogic.m_conn;
    char *data_type = NULL;
    data_type = (char *)malloc(MAX_DATATYPE_LEN);
    if (data_type == NULL) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): out of memory.\n"));
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
