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
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
#include "gs_ktool_interface.h"
#else
#include "localkms_gen_cmk.h"
#endif
#include <memory>
#include <vector>
#include <string>

#define MAX_KEY_STORE_SIZE 256
#define MAX_KEY_PATH_SIZE 4096

enum class KeyStore;

class MemCekMap;
class EncryptionGlobalHookExecutor : public GlobalHookExecutor, public AbstractEncryptionHook {
public:
    explicit EncryptionGlobalHookExecutor(PGClientLogic &columnEncryption)
        : GlobalHookExecutor { columnEncryption, "encryption" }
    {
        add_allowed_value("key_store");
        add_allowed_value("key_path");
    }
    virtual ~EncryptionGlobalHookExecutor() {};
    bool pre_create(const StringArgs &args, const GlobalHookExecutor **existing_global_hook_executors,
        size_t existing_global_hook_executors_size) override;
    bool process(ColumnHookExecutor *column_hook_executor) override;
#if ((!defined(ENABLE_MULTIPLE_NODES)) && (!defined(ENABLE_PRIVATEGAUSS)))
    bool get_key_path_by_cmk_name(char *key_path_buf, size_t buf_len);
#endif

private:
    const CmkKeyStore get_key_store() const;
    const char *get_key_path() const;
    void save_private_variables() override;

private:
    void set_keystore(const char *keystore, size_t keystore_size)
    {
        if (keystore == NULL || keystore_size == 0 || m_keyStore == NULL) {
            return;
        }
        errno_t securec_rc = EOK;
        Assert(keystore_size <= MAX_KEY_STORE_SIZE);
        securec_rc = memcpy_s(m_keyStore, MAX_KEY_STORE_SIZE, keystore, keystore_size);
        securec_check_c(securec_rc, "\0", "\0");
        return;
    }

    void set_keypath(const char *keypath, size_t keypath_size)
    {
        if (keypath == NULL || keypath_size == 0 || m_keyPath == NULL) {
            return;
        }
        errno_t securec_rc = EOK;
        Assert(keypath_size <= MAX_KEY_STORE_SIZE);
        securec_rc = memcpy_s(m_keyPath, MAX_KEY_STORE_SIZE, keypath, keypath_size);
        securec_check_c(securec_rc, "\0", "\0");
        return;
    }

    char m_keyStore[MAX_KEY_STORE_SIZE] = {0};
    char m_keyPath[MAX_KEY_PATH_SIZE] = {0};
};

#endif
