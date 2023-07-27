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
 * encryption_column_hook_executor.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_hooks\encryption_hooks\encryption_column_hook_executor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ENCRYPTION_COLUMN_HOOK_EXECUTOR_H
#define ENCRYPTION_COLUMN_HOOK_EXECUTOR_H

#include "pg_config.h"

#include <memory>
#include <string>
#include <vector>
#include "column_hook_executor.h"
#include "abstract_encryption_hook.h"
#include "encryption_global_hook_executor.h"
#include "keymgr/encrypt/security_encrypt_decrypt.h"
#include "keymgr/encrypt/security_aead_aes_hamc_enc_key.h"
#include "postgres_ext.h"
#include "client_logic_processor/values_processor.h"

#define MAX_DATATYPE_LEN 30

struct ColumnDef;
class CStringsMap;
typedef CStringsMap StringArgs;
const int MAX_CEK_LENGTH = 256;
class EncryptionColumnHookExecutor : public ColumnHookExecutor, public AbstractEncryptionHook {
public:
    EncryptionColumnHookExecutor(GlobalHookExecutor *globalHookExecutor, Oid oid)
        : ColumnHookExecutor(globalHookExecutor, oid, "encryption")
    {
        init();
        add_allowed_value("encrypted_value");
        add_allowed_value("encryption_type");
        add_allowed_value("algorithm");
    }
    virtual ~EncryptionColumnHookExecutor() {}
    int get_estimated_processed_data_size_impl(int data_size) const override;
    int process_data_impl(const ICachedColumn *cached_column, const unsigned char *data,
        int data_size, unsigned char *processed_data) override;
    DecryptDataRes deprocess_data_impl(const unsigned char *data_processed,
        int data_proceeed_size, unsigned char **data, int *data_plain_size) override;
    bool is_set_operation_allowed(const ICachedColumn *cached_column) const override;
    bool is_operator_allowed(const ICachedColumn *ce, const char * const op) const override;
    bool pre_create(PGClientLogic &column_encryption, const StringArgs &args, StringArgs &new_args) override;
    void save_private_variables() override;
    const char *get_data_type(const ColumnDef * const column) override;
    void set_data_type(const ColumnDef * const column, ICachedColumn *ce) override;

private:
    void init()
    {
        is_cek_set = false;
    }
    bool deprocess_column_encryption_key(EncryptionGlobalHookExecutor *encryption_global_hook_executor,
        unsigned char *decryptedKey, size_t *decryptedKeySize, const char *encrypted_key_value, 
        const size_t *encrypted_key_value_size) const;
    bool set_cek_keys(const unsigned char *cek_keys, size_t cek_size)
    {
        if (cek_keys == NULL || cek_size == 0 || cek_size > MAX_CEK_LENGTH) {
            return false;
        }
        if (!aesCbcEncryptionKey.generate_keys((unsigned char *)cek_keys, cek_size)) {
            return false;
        }
        is_cek_set = true;
        return true;
    }

    ColumnEncryptionAlgorithm get_column_encryption_algorithm() const
    {
        return m_column_encryption_algorithm;
    }

    void set_column_encryption_algorithm(ColumnEncryptionAlgorithm column_encryption_algorithm)
    {
        m_column_encryption_algorithm = column_encryption_algorithm;
    }

    bool is_cek_set;
    ColumnEncryptionAlgorithm m_column_encryption_algorithm = ColumnEncryptionAlgorithm::INVALID_ALGORITHM;
    AeadAesHamcEncKey aesCbcEncryptionKey;         /* contains iv, encryption and mac keys */
};

#endif