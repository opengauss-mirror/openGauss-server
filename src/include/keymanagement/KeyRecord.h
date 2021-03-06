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
 * KeyRecord.h
 *     encrypt and decrypt functions for MPPDB
 * 
 * 
 * IDENTIFICATION
 *        src/include/keymanagement/KeyRecord.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SEC_KEY_KEYRECORD_H
#define SEC_KEY_KEYRECORD_H

#include <iostream>

const int TDE_IV_LEN = 16;

enum TDE_ALGO { TDE_ALGO_SM4_CTR_128, TDE_ALGO_AES_CTR_128, TDE_ALGO_NULL };

class KeyRecord {

public:
    KeyRecord();
    virtual ~KeyRecord() = default;

    bool create_encrypted_sample_string(std::string dek_b64);
    bool check_encrypted_sample_string(unsigned char* dek);
    std::string getEncryptedSampleStr();

    bool create_dek_iv();
    bool getIV(unsigned char* out_iv);

    TDE_ALGO get_tde_algo() const;

protected:
    std::string cek_name;
    std::string cek_version;
    std::string cek_IV;
    std::string dek_cipher;
    std::string db_uid;
    std::string tb_uid;
    std::string encrypted_sample_string;
    unsigned char dek_iv[TDE_IV_LEN] = {};
    TDE_ALGO tde_algo;
};

#endif  // SEC_KEY_KEYRECORD_H
