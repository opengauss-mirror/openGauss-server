/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * tde_key_manager.h
 *    TDE key manager is the external interface of key management
 *
 * IDENTIFICATION
 *    src/include/tde_key_management/tde_key_manager.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SEC_TDE_KEY_MANAGER_H
#define SEC_TDE_KEY_MANAGER_H

#include "tde_key_management/data_common.h"
#include "tde_key_management/kms_interface.h"

class TDEKeyManager : public BaseObject {
public:
    TDEKeyManager();
    ~TDEKeyManager();

    void init();
    const TDEData* create_dek();
    const TDEData* get_dek(const char* cmk_id, const char* dek_cipher);
    bool save_key(const TDEData* tde_data);
    const char* get_key(const char* cmk_id, const char* dek_cipher);

private:
    char* get_cmk_id();

private:
    TDEData* tde_create_data;
    TDEData* tde_get_data;
    KMSInterface* kms_instance;
};

#endif
