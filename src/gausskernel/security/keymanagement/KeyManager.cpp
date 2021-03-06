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
 * KeyManager.cpp
 *    TDE key management is the external interface of key management
 *
 * IDENTIFICATION
 *    src/gausskernel/security/keymanagement/KeyManager.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <iostream>

#include "keymanagement/HttpRestfulClient.h"
#include "keymanagement/FIKMSmessage.h"

#include "keymanagement/KeyManager.h"
#include "keymanagement/TDEKeysFile.h"

#include "alarm/alarm.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#define KEY_LEN 128

void ReportAlarmAbnormalTDEValue()
{
    if (g_instance.attr.attr_common.PGXCNodeName == NULL)
        return;
    Alarm alarmItem[1];
    AlarmAdditionalParam tempAdditionalParam;

    // Initialize the alarm item
    AlarmItemInitialize(alarmItem, ALM_AI_AbnormalTDEValue, ALM_AS_Reported, NULL);
    // fill the alarm message
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
        g_instance.attr.attr_common.PGXCNodeName,
        "",
        "",
        alarmItem,
        ALM_AT_Fault,
        g_instance.attr.attr_common.PGXCNodeName);
    // report the alarm
    AlarmReporter(alarmItem, ALM_AT_Fault, &tempAdditionalParam);
}

void ReportResumeAbnormalTDEValue()
{
    if (g_instance.attr.attr_common.PGXCNodeName == NULL)
        return;

    Alarm alarmItem[1];
    AlarmAdditionalParam tempAdditionalParam;

    // Initialize the alarm item
    AlarmItemInitialize(alarmItem, ALM_AI_AbnormalTDEValue, ALM_AS_Normal, NULL);
    // fill the alarm message
    WriteAlarmAdditionalInfo(
        &tempAdditionalParam, g_instance.attr.attr_common.PGXCNodeName, "", "", alarmItem, ALM_AT_Resume);
    // report the alarm
    AlarmReporter(alarmItem, ALM_AT_Resume, &tempAdditionalParam);
}

void string_replace(std::string& data, const std::string& from, const std::string& to)
{
    std::string::size_type pos = 0;
    std::string::size_type a = from.size();
    std::string::size_type b = to.size();
    while ((pos = data.find(from, pos)) != std::string::npos) {
        data.replace(pos, a, to);
        pos += b;
    }
    return;
}

KeyManager::KeyManager(const char* db_uid_in, const char* tb_uid_in)
{
    db_uid = std::string(db_uid_in);
    tb_uid = std::string(tb_uid_in);
    cek_name = "";
    Key_status = KEY_NOT_INIT;
}

KeyManager::~KeyManager()
{}

TDE_KEY_STATUS KeyManager::getKey_status() const
{
    return Key_status;
}

bool KeyManager::init_Key(const char* algo, const char* keyname)
{
    if (algo == NULL) {
        return false;
    }

    /* 11 is the length of the algorithm name  */
    const int ALGO_NAME_LENGTH = 11;
    if ((strncmp(algo, "SM4-CTR-128", ALGO_NAME_LENGTH) == 0) && (algo[ALGO_NAME_LENGTH] == '\0')) {
        tde_algo = TDE_ALGO_SM4_CTR_128;
    } else if ((strncmp(algo, "AES-CTR-128", ALGO_NAME_LENGTH) == 0) && (algo[ALGO_NAME_LENGTH] == '\0')) {
        tde_algo = TDE_ALGO_AES_CTR_128;
    } else {
        return false;
    }
    cek_name = std::string(keyname);
    return init_Key();
}

bool KeyManager::init_Key()
{
    /* Key file already existed. */
    if (read_Disk()) {
        Key_status = DEK_CREATED;
        return true;
    }

    /* Key file already existed.But read fail. */
    if (existed())
        return false;

    /* Create CEK. */
    if ((Key_status == KEY_NOT_INIT) && createCEK()) {
        Key_status = CEK_CREATED;
    }

    /* Create DEK IV. */
    if ((Key_status == CEK_CREATED) && create_dek_iv()) {
        Key_status = DEK_IV_CREATED;
    }

    /* Create DEK. */
    if (Key_status == DEK_IV_CREATED && createDEK()) {
        Key_status = DEK_CREATED;
        if (!fill_Disk()) {
            return false;
        }
    }

    return Key_status == DEK_CREATED;
}

bool KeyManager::check_key(unsigned char* dek)
{
    if (Key_status == DEK_CREATED && check_encrypted_sample_string(dek)) {
        ReportResumeAbnormalTDEValue();
        return true;
    }
    ReportAlarmAbnormalTDEValue();
    return false;
}

/* Save key record information to disk. */
bool KeyManager::fill_Disk()
{
    if (save())
        return true;
    return false;
}

/* Read key record information from disk. */
bool KeyManager::read_Disk()
{
    return parser();
}

/* Creating or getting Cluster Encryption Key from KMS. */
bool KeyManager::createCEK()
{
    FIKMSmessage message = FIKMSmessage();
    HttpRestfulClient cli = HttpRestfulClient();
    /* Use existing CEK name */
    if (!cek_name.empty()) {
        std::string res_data = cli.get("/kms/v1/keys/names");
        /* the correct message length must be greater than 2 */
        if (res_data.length() > 2 && res_data.compare("[ ]") != 0 && message.parser(res_data) &&
            message.parser_get_CEK(cek_name)) {
            std::cout << "keymanagement CEK lists:" << res_data << std::endl;
            return true;
        }
    }

    std::string send_data;
    /* Create a new CEK, Note that permissions must be configured correctly on FI KMS. */
    if (tde_algo == TDE_ALGO_SM4_CTR_128) {
        send_data = message.create(cek_name, "SM4", KEY_LEN);
    } else if (tde_algo == TDE_ALGO_AES_CTR_128) {
        send_data = message.create(cek_name, "AES", KEY_LEN);
    } else {
        /* Default is AES. */
        send_data = message.create(cek_name, "AES", KEY_LEN);
    }

    std::string res_data = cli.post("/kms/v1/keys", send_data.c_str(), send_data.size());
    if (res_data.empty()) {
        std::cout << "keymanagement createCEK response error." << std::endl;
        return false;
    }
    return message.parser(res_data) && message.parser_create_CEK(cek_version);
}

/* Creating database encryption key in KMS. */
bool KeyManager::createDEK()
{
    FIKMSmessage message = FIKMSmessage();
    HttpRestfulClient cli = HttpRestfulClient();
    /* url res defined by FI KMS interface, Generate Encrypted Key for Current KeyVersion. */
    std::string url_res = "/kms/v1/key/" + cek_name + "/_eek?eek_op=generate&num_keys=1";
    std::string res_data = cli.get(url_res.c_str());
    if (res_data.length() > 1 && message.parser(res_data) &&
        message.parser_create_DEK(cek_IV, dek_cipher, cek_version)) {
        return create_encrypted_sample_string(getDEK());
    }
    std::cout << "keymanagement createDEK:" << res_data << std::endl;
    return false;
}

/* Getting database encryption key from KMS. Returns the DEK encoded by Base64. */
std::string KeyManager::getDEK()
{
    FIKMSmessage message = FIKMSmessage();
    HttpRestfulClient cli = HttpRestfulClient();
    std::string send_data = message.create(cek_name, cek_IV, dek_cipher);

    /* url_res defined by FI KMS interface, Decrypt Encrypted Key */
    std::string url_res = "/kms/v1/keyversion/" + cek_version + "/_eek?eek_op=decrypt";
    std::string res_data = cli.post(url_res.c_str(), send_data.c_str(), send_data.size());

    std::string dek;
    if (res_data.length() > 0 && message.parser(res_data))
        (void)message.parser_get_DEK(dek);

    /* Standard Base64 formatting */
    if (dek.length() > 0) {
        string_replace(dek, "_", "/");
        string_replace(dek, "-", "+");
        /* 4 bytes aligned */
        while (dek.length() % 4) {
            dek += "=";
        }
    }
    return dek;
}

