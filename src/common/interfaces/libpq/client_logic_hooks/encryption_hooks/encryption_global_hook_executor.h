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
 * encryption_global_hook_executor.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\encryption_hooks\encryption_global_hook_executor.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef ENCRYPTION_GLOBAL_HOOK_EXECUTOR_H
#define ENCRYPTION_GLOBAL_HOOK_EXECUTOR_H

#include "pg_config.h"

#include "abstract_encryption_hook.h"
#include "global_hook_executor.h"
#include "client_logic/client_logic_enums.h"
#include <memory>
#include <vector>
#include <string>

#define MAX_KEY_STORE_SIZE 256
#define MAX_KEY_PATH_SIZE 4096
#define MAX_KEY_ALGO_SIZE 56

enum class KeyStore;

class MemCekMap;
class EncryptionGlobalHookExecutor : public GlobalHookExecutor, public AbstractEncryptionHook {
public:
    explicit EncryptionGlobalHookExecutor(PGClientLogic &columnEncryption)
        : GlobalHookExecutor { columnEncryption, "encryption" }
    {
        add_allowed_value("key_store");
        add_allowed_value("key_path");
        add_allowed_value("algorithm");
    }
    virtual ~EncryptionGlobalHookExecutor() {};
    bool pre_create(const StringArgs &args, const GlobalHookExecutor **existing_global_hook_executors,
        size_t existing_global_hook_executors_size) override;
    bool process(ColumnHookExecutor *column_hook_executor) override;
    bool set_deletion_expected() override;
    bool post_create(const StringArgs& args);
    bool deprocess_column_setting(const unsigned char *processed_data, const size_t processed_data_size, 
        const char *key_store, const char *key_path, const char *key_algo, unsigned char **data, size_t *data_size)
        override;

private:
    const char *get_key_store() const;
    const char *get_key_path() const;
    const char *get_key_algo() const;
    void save_private_variables() override;

private:
    void set_keystore(const char *keystore, size_t keystore_size)
    {
        if (keystore == NULL || keystore_size == 0) {
            return;
        }
        errno_t securec_rc = EOK;
        Assert(keystore_size <= MAX_KEY_STORE_SIZE);
        securec_rc = memcpy_s(m_key_store, MAX_KEY_STORE_SIZE, keystore, keystore_size);
        securec_check_c(securec_rc, "\0", "\0");
        return;
    }

    void set_keypath(const char *keypath, size_t keypath_size)
    {
        if (keypath == NULL || keypath_size == 0) {
            return;
        }
        errno_t securec_rc = EOK;
        Assert(keypath_size <= MAX_KEY_STORE_SIZE);
        securec_rc = memcpy_s(m_key_path, MAX_KEY_STORE_SIZE, keypath, keypath_size);
        securec_check_c(securec_rc, "\0", "\0");
        return;
    }

    void set_keyalgo(const char *cmk_algorithm, size_t cmk_algorithm_size)
    {
        if (cmk_algorithm == NULL || cmk_algorithm_size == 0) {
            return;
        }
        errno_t securec_rc = EOK;
        Assert(cmk_algorithm_size <= MAX_KEY_ALGO_SIZE);
        securec_rc = memcpy_s(m_key_algo, MAX_KEY_ALGO_SIZE, cmk_algorithm, cmk_algorithm_size);
        securec_check_c(securec_rc, "\0", "\0");
        return;
    }

    char m_key_store[MAX_KEY_STORE_SIZE] = {0};
    char m_key_path[MAX_KEY_PATH_SIZE] = {0};
    char m_key_algo[MAX_KEY_ALGO_SIZE] = {0};
};

#endif
