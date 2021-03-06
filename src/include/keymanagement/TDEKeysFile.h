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
 * TDEKeysFile.h
 *     encrypt and decrypt functions for MPPDB
 * 
 * 
 * IDENTIFICATION
 *        src/include/keymanagement/TDEKeysFile.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SEC_KEY_TDEKEYSFILE_H
#define SEC_KEY_TDEKEYSFILE_H

#include "KeyRecord.h"
#include <iostream>

#define TDE_KEYS_STORE_NAME "gs_tde_keys.cipher"

class TDEKeysFile : public KeyRecord {
public:
    TDEKeysFile();
    virtual ~TDEKeysFile();
    void setfile(const char* gausshome);
    bool existed() const;
    bool save();
    bool parser();
    void clear();

private:
    std::string tde_keys_store_path;
};

#endif  // SEC_KEY_TDEKEYSFILE_H
