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
 * ---------------------------------------------------------------------------------------
 * 
 * KeyManager.h
 *     TDE key management is the external interface of key management
 * 
 * 
 * IDENTIFICATION
 *        src/include/keymanagement/KeyManager.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SEC_KEY_KEYMANAGER_H
#define SEC_KEY_KEYMANAGER_H

#include "TDEKeysFile.h"
enum TDE_KEY_STATUS {
    CEK_NONE,
    CEK_ERROR,
    CEK_CREATED,
    DEK_NONE,
    DEK_ERROR,
    DEK_CREATED,
    DEK_IV_NONE,
    DEK_IV_ERROR,
    DEK_IV_CREATED,
    KEY_NOT_INIT,
    KEY_INITED
};

/* This class is used for key management */
class KeyManager : public TDEKeysFile {
public:
    KeyManager(const char* db_uid_in, const char* tb_uid_in);
    virtual ~KeyManager();
    /*
     * Initialization of key environment based on key algorithm and CEK key name.
     * the algo and cekname come form user.
     */
    bool init_Key(const char* algo, const char* cekname);
    /* use default value init key environment.*/
    bool init_Key();

    /* Checking whether DEK is correct will be judged by the saved encrypted_sample_string. */
    bool check_key(unsigned char* dek);

    std::string getDEK();
    TDE_KEY_STATUS getKey_status() const;

private:
    TDE_KEY_STATUS Key_status;

    bool createCEK();
    bool createDEK();

    bool fill_Disk();
    bool read_Disk();
};

#endif  // SEC_KEY_KEYMANAGER_H
