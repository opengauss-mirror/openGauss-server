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
#include "cmk_entity_manager_hooks/reg_cmkem_manager_main.h"
#include "cmk_entity_manager_hooks/reg_hook_frame.h"

/* 
 * while using SQL :
 *      CREATE CLIENT MASTER KEY xxx WITH(
 *          KEY_STORE = xxx, KEY_PATH = "xxx", ALGORITHM = xxx);
 * the GaussDB Kernel will process the cmk object identified by "KEY_STORE & KEY_PATH"
 * the cmk entity management tools/components/services will process the cmk enrty identified by "KEY_STORE & KEY_PATH"
 * 
 * the whole process is divided into 3 stages:
 *      (1) client : pre_create() the cmk object in SQL, call cmk entity manager to process cmk entity
 *          then, send the SQL to server
 *      (2) server : process the cmk object in SQL, return the process result to client
 *      (3) client : judge whether the result is SUCCEED or not, post_create() will call cmk entity
 *          manager to process cmk entity again
 */
bool EncryptionGlobalHookExecutor::pre_create(const StringArgs &args,
    const GlobalHookExecutor **existing_global_hook_executors, size_t existing_global_hook_executors_size)
{
    PGconn *conn = m_clientLogic.m_conn;
    const char *key_store_str = args.find("key_store");
    const char *key_path_str = args.find("key_path");
    const char *key_algo_str = args.find("algorithm");
    CmkIdentity cmk_identify = {key_store_str, key_path_str, key_algo_str, 0, m_clientLogic.client_cache_id};
    CmkemErrCode ret = CMKEM_SUCCEED;

    /* 
     * You can register your own cmk entity management tools/components/services in reg_all_cmk_entity_manager()
     * now, we have registered 3:
     *      (1) cmk entity management tool : gs_ktool. (not supported in openGauss)
     *      (2) cmk entity management component : localkms
     *      (3) cmk entity management service : HuaWei KMS (provided by HuaWei Cloud, not supported in openGauss)
     * 
     *      (*4) finally, we have a grammar check ：if cmk entity is not processed, we think it's out of control 
     *           and it is a syntax error
     */
    ret = reg_all_cmk_entity_manager();
    if (ret != CMKEM_SUCCEED) {
        printfPQExpBuffer(&conn->errorMessage, "ERROR(CLIENT): %s\n", get_cmkem_errmsg(ret));
        return false;
    }

    ret = create_cmk_obj(&cmk_identify);
    if (ret != CMKEM_SUCCEED) {
        printfPQExpBuffer(&conn->errorMessage, "ERROR(CLIENT): %s\n", get_cmkem_errmsg(ret));
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

        if (strcasecmp(key_store_str, encryptionGlobalHookExecutor->get_key_store()) == 0 &&
            strcasecmp(key_path_str, encryptionGlobalHookExecutor->get_key_path()) == 0) {
            printfPQExpBuffer(&conn->errorMessage,
                libpq_gettext("ERROR(CLIENT): key store and key path are already in use by another object\n"));
            return false;
        }
    }

    set_keystore(key_store_str, strlen(key_store_str));
    set_keypath(key_path_str, strlen(key_path_str));
    set_keyalgo(key_algo_str, strlen(key_algo_str));

    return true;
}

bool EncryptionGlobalHookExecutor::post_create(const StringArgs& args)
{
    PGconn *conn = m_clientLogic.m_conn;
    const char *key_store_str = args.find("key_store");
    const char *key_path_str = args.find("key_path");
    const char *key_algo_str = args.find("algorithm");
    CmkIdentity cmk_identify = {key_store_str, key_path_str, key_algo_str, 0, m_clientLogic.client_cache_id};
    CmkemErrCode ret = CMKEM_SUCCEED;

    ret = reg_all_cmk_entity_manager();
    if (ret != CMKEM_SUCCEED) {
        printfPQExpBuffer(&conn->errorMessage, "ERROR(CLIENT): %s\n", get_cmkem_errmsg(ret));
        return false;
    }

    ret = post_create_cmk_obj(&cmk_identify);
    if (ret != CMKEM_SUCCEED) {
        printfPQExpBuffer(&conn->errorMessage, "ERROR(CLIENT): %s\n", get_cmkem_errmsg(ret));
        return false;
    }

    return true;
}

/* 
 * while using SQL :
 *      DROP CLIENT MASTER KEY xxx;
 * the cmk entity management tools/components/services will process all the cmk entities identified 
 * by "KEY_STORE & KEY_PATH" 
 */
bool EncryptionGlobalHookExecutor::set_deletion_expected()
{
    PGconn *conn = m_clientLogic.m_conn;
    size_t unused = 0;
    const char *key_store_str = NULL;
    const char *key_path_str = NULL;
    CmkemErrCode ret = CMKEM_SUCCEED;

    get_argument("key_store", &key_store_str, unused);
    get_argument("key_path", &key_path_str, unused);

    CmkIdentity cmk_identify = {key_store_str, key_path_str, NULL, 0, m_clientLogic.client_cache_id};

    ret = reg_all_cmk_entity_manager();
    if (ret != CMKEM_SUCCEED) {
        printfPQExpBuffer(&conn->errorMessage, "ERROR(CLIENT): %s\n", get_cmkem_errmsg(ret));
        return false;
    }

    ret = drop_cmk_obj(&cmk_identify);
    if (ret != CMKEM_SUCCEED) {
        printfPQExpBuffer(&conn->errorMessage, "ERROR(CLIENT): %s\n", get_cmkem_errmsg(ret));
        return false;
    }

    return true;
}

bool EncryptionGlobalHookExecutor::process(ColumnHookExecutor *column_hook_executor)
{
    /* get Column Executor */
    Oid cekOid = column_hook_executor->getOid();
    if (cekOid == InvalidOid) {
        return false;
    }

    const CachedColumnSetting *cek = m_clientLogic.m_cached_column_manager->get_cached_column_setting_metadata(cekOid);
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
        check_strncpy_s(strncpy_s(m_key_store, sizeof(m_key_store), got, strlen(got)));
    }

    got = m_values_map.find("key_path");
    if (got) {
        check_strncpy_s(strncpy_s(m_key_path, sizeof(m_key_path), got, strlen(got)));
    }

    got = m_values_map.find("algorithm");
    if (got) {
        check_strncpy_s(strncpy_s(m_key_algo, sizeof(m_key_algo), got, strlen(got)));
    }
}

const char *EncryptionGlobalHookExecutor::get_key_store() const
{
    return m_key_store;
}

const char *EncryptionGlobalHookExecutor::get_key_path() const
{
    return m_key_path;
}

const char *EncryptionGlobalHookExecutor::get_key_algo() const
{
    return m_key_algo;
}

bool EncryptionGlobalHookExecutor::deprocess_column_setting(const unsigned char *processed_data,
    size_t processed_data_size, const char *key_store, const char *key_path, const char *key_algo, unsigned char **data,
    size_t *data_size)
{
    CmkIdentity cmk_identify = {key_store, key_path, key_algo, 0, 1};
    CmkemUStr cek_cipher = {(unsigned char *)processed_data, (size_t)processed_data_size};
    CmkemUStr *cek_plain = NULL;
    CmkemErrCode ret = CMKEM_SUCCEED;

    ret = reg_all_cmk_entity_manager();
    if (ret != CMKEM_SUCCEED) {
        /* this function is only called by gs_dump, so we can print errmsg on the terminal */
        printf("ERROR: %s", get_cmkem_errmsg(ret));
        return false;
    }

    if (decrypt_cek_cipher(&cek_cipher, &cmk_identify, &cek_plain) != CMKEM_SUCCEED) {
        return false;
    }

    *data = cek_plain->ustr_val;
    *data_size = cek_plain->ustr_len;

    cmkem_free(cek_plain);
    return true;
}
