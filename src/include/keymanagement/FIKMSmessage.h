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
 * FIKMSmessage.h
 *     Processing of messages defined by FI KMS
 * 
 * 
 * IDENTIFICATION
 *        src/include/keymanagement/FIKMSmessage.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SEC_KEY_FIKMSMESSAGE_H
#define SEC_KEY_FIKMSMESSAGE_H

#include "cjson/cJSON.h"
#include <string>

/* The following values are defined by the RESTful interface of FI KMS */
const char* const FIKMS_MESSAGE_name = "name";
const char* const FIKMS_MESSAGE_cipher = "cipher";
const char* const FIKMS_MESSAGE_length = "length";
const char* const FIKMS_MESSAGE_material = "material";
const char* const FIKMS_MESSAGE_number_of_keys_to_generate = "number_of_keys_to_generate";
const char* const FIKMS_MESSAGE_key_name = "key-name";
const char* const FIKMS_MESSAGE_versionname = "versionName";
const char* const FIKMS_MESSAGE_iv = "iv";
const char* const FIKMS_MESSAGE_encryptedKeyVersion = "encryptedKeyVersion";
const char* const FIKMS_MESSAGE_exception = "RemoteException";

/* This class is used for FI KMS protocol parsing. */
class FIKMSmessage {
public:
    FIKMSmessage();
    ~FIKMSmessage();

    /* Parsing JSON structure from string. */
    bool parser(std::string serizial_str);

    /* Use name, cipher, length to assemble the message needed by FI KMS.Returns the result of serialization of JSON
     * information.*/
    std::string create(std::string name, std::string cipher, int length);
    /* Use name, iv, material to assemble the message needed by FI KMS.Returns the result of serialization of JSON
     * information.*/
    std::string create(std::string name, std::string iv, std::string material);

    /* Parsing Create CEK Messages. */
    bool parser_create_CEK(std::string& CEK_name);
    /* Parsing Create DEK Messages. */
    bool parser_create_DEK(std::string& DEK_IV, std::string& DEK_cipher, std::string& CEK_versionname);

    /* Parsing gte DEK Messages. */
    bool parser_get_DEK(std::string& DEK);
    /* Parsing get CEK Messages. */
    bool parser_get_CEK(const std::string& CEK_name);

private:
    cJSON* response_data;
    bool is_exception();
};

#endif  // SEC_KEY_FIKMSMESSAGE_H
