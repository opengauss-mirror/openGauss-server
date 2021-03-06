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
 * encryption_global_hook_executor.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\encryption_hooks\encryption_global_hook_executor.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "pg_config.h"

#include <strings.h>
#include <string.h>
#include <iostream>
#include "zlib.h"
#include "encryption_global_hook_executor.h"
#include "encryption_column_hook_executor.h"
#include "client_logic_cache/cached_column_setting.h"
#include "client_logic_cache/cached_global_setting.h"
#include "client_logic_cache/cached_column_manager.h"
#include "client_logic_common/client_logic_utils.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "cl_state.h"

#if ((!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS)))
static bool create_cmk(const char *key_path_str)
{
    RealCmkPath real_cmk_path = {0};
    KmsErrType err_type = SUCCEED;
    RsaKeyStr *rsa_key_str = NULL;

    err_type = get_and_check_real_key_path(key_path_str, &real_cmk_path, CREATE_KEY_FILE);
    if (err_type != SUCCEED) {
        handle_kms_err(err_type);
        return false;
    }

    rsa_key_str = generate_cmk_kms();
    if (rsa_key_str == NULL) {
        printf("ERROR(CLIENT): failed to generate rsa key.\n");
        return false;
    }

    err_type = write_cmk_plain_kms(real_cmk_path.real_pub_cmk_path, sizeof(real_cmk_path.real_pub_cmk_path),
        rsa_key_str->pub_key, rsa_key_str->pub_key_len);
    if (err_type != SUCCEED) {
        free_rsa_str(rsa_key_str);
        handle_kms_err(err_type);
        return false;
    }

    err_type = write_cmk_plain_kms(real_cmk_path.real_priv_cmk_path, sizeof(real_cmk_path.real_priv_cmk_path),
        rsa_key_str->priv_key, rsa_key_str->priv_key_len);
    if (err_type != SUCCEED) {
        free_rsa_str(rsa_key_str);
        handle_kms_err(err_type);
        return false;
    }

    free_rsa_str(rsa_key_str);
    return true;
}

#endif

bool EncryptionGlobalHookExecutor::pre_create(const StringArgs &args,
    const GlobalHookExecutor **existing_global_hook_executors, size_t existing_global_hook_executors_size)
{
    const char *key_store_str;
    const char *key_path_str;
    const char *algorithm_type_str;
    PGconn *conn = m_clientLogic.m_conn;
    key_store_str = args.find("key_store");
    key_path_str = args.find("key_path");
    algorithm_type_str = args.find("algorithm");

    /* check algorithm */
    CmkAlgorithm cmk_algo = get_algorithm_from_string(algorithm_type_str);
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    if (cmk_algo != CmkAlgorithm::AES_256_CBC) {
#else
    if (cmk_algo != CmkAlgorithm::RSA_2048) {
#endif
        printfPQExpBuffer(&conn->errorMessage,
            libpq_gettext("ERROR(CLIENT): unsupported client master key algorithm\n"));
        return false;
    }

    /* check key store */
    CmkKeyStore key_store = get_key_store_from_string(key_store_str);
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    if (key_store != CmkKeyStore::GS_KTOOL) {
#else
    if (key_store != CmkKeyStore::LOCALKMS) {
#endif
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): invalid key store\n"));
        return false;
    }

    /* check key path */
    if (key_path_str == NULL || strlen(key_path_str) == 0) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): invalid key path\n"));
        return false;
    }

    /* check same attributes are not used by existing global settings */
    for (size_t i = 0; i < existing_global_hook_executors_size; ++i) {
        const EncryptionGlobalHookExecutor *encryptionGlobalHookExecutor =
            dynamic_cast<const EncryptionGlobalHookExecutor *>(existing_global_hook_executors[i]);

        if (!encryptionGlobalHookExecutor) {
            printfPQExpBuffer(&conn->errorMessage,
                libpq_gettext("(ERROR(CLIENT): failed to retrieve encryption executor or an object already exists\n"));
            return false;
        }

        if (key_store == encryptionGlobalHookExecutor->get_key_store() &&
            strcasecmp(key_path_str, encryptionGlobalHookExecutor->get_key_path()) == 0) {
            printfPQExpBuffer(&conn->errorMessage,
                libpq_gettext("ERROR(CLIENT): key store and key path are already in use by another object\n"));
            return false;
        }
    }
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    /* generate cmk */
    unsigned int cmk_id = 0;

    if (key_store == CmkKeyStore::GS_KTOOL) {
        if (!kt_atoi(key_path_str, &cmk_id)) {
            return false;
        }

        if (!create_cmk(cmk_id)) {
            return false;
        }
    } else {
        return false;
    }
#else
    if (key_store == CmkKeyStore::LOCALKMS) {
        if (!create_cmk(key_path_str)) {
            return false;
        }
    } else {
        /* remain */
        return false;
    }
#endif
    set_keystore(key_store_str, strlen(key_store_str));
    set_keystore(key_path_str, strlen(key_path_str));

    return true;
}

#if ((!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS)))
bool EncryptionGlobalHookExecutor::get_key_path_by_cmk_name(char *key_path_buf, size_t buf_len)
{
    const char *key_path_str = NULL;
    size_t key_path_size = 0;
    error_t rc = 0;

    get_argument("key_path", &key_path_str, key_path_size);
    if (strlen(key_path_str) > buf_len) {
        printf("ERROR(CLIENT): key path value is too long.\n");
        return false;
    }

    rc = strcpy_s(key_path_buf, buf_len, key_path_str);
    securec_check_c(rc, "", "");

    return true;
}
#endif

bool EncryptionGlobalHookExecutor::process(ColumnHookExecutor *column_hook_executor)
{
    /* get Column Executor */
    Oid cekOid = column_hook_executor->getOid();
    if (cekOid == InvalidOid) {
        return false;
    }

    const CachedColumnSetting *cek = CachedColumnManager::get_instance().get_cached_column_setting_metadata(cekOid);
    if (!cek) {
        return false;
    }

    EncryptionColumnHookExecutor *columnExecutor = dynamic_cast<EncryptionColumnHookExecutor *>(cek->get_executor());
    if (!columnExecutor) {
        return false;
    }

    /* get Global Executor */
    EncryptionGlobalHookExecutor *globalExecutor =
        dynamic_cast<EncryptionGlobalHookExecutor *>(cek->get_executor()->get_global_hook_executor());
    if (!globalExecutor) {
        return false;
    }

    return true;
}

void EncryptionGlobalHookExecutor::save_private_variables()
{
    const char *got = m_values_map.find("key_store");
    if (got) {
        check_strncpy_s(strncpy_s(m_keyStore, sizeof(m_keyStore), got, strlen(got)));
    }

    got = m_values_map.find("key_path");
    if (got) {
        check_strncpy_s(strncpy_s(m_keyPath, sizeof(m_keyPath), got, strlen(got)));
    }
}

const CmkKeyStore EncryptionGlobalHookExecutor::get_key_store() const
{
    return get_key_store_from_string(m_keyStore);
}

const char *EncryptionGlobalHookExecutor::get_key_path() const
{
    return m_keyPath;
}