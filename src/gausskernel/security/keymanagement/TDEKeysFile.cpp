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
 * TDEKeysFile.cpp
 *    Operation of TDE files
 *
 * IDENTIFICATION
 *    src/gausskernel/security/keymanagement/TDEKeysFile.cpp
 *
 * ---------------------------------------------------------------------------------------
 */


#include <fstream>
#include <unistd.h>

#include "TDEKeysRecords.pb.h"

#include "postgres.h"
#include "knl/knl_variable.h"
#include "alarm/alarm.h"
#include "keymanagement/TDEKeysFile.h"

void ReportAlarmAbnormalTDEKeyFile()
{
    if (g_instance.attr.attr_common.PGXCNodeName == NULL)
        return;

    Alarm alarmItem[1];
    AlarmAdditionalParam tempAdditionalParam;

    // Initialize the alarm item
    AlarmItemInitialize(alarmItem, ALM_AI_AbnormalTDEFile, ALM_AS_Reported, NULL);
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

void ReportResumeAbnormalTDEKeyFile()
{
    if (g_instance.attr.attr_common.PGXCNodeName == NULL)
        return;

    Alarm alarmItem[1];
    AlarmAdditionalParam tempAdditionalParam;

    // Initialize the alarm item
    AlarmItemInitialize(alarmItem, ALM_AI_AbnormalTDEFile, ALM_AS_Normal, NULL);
    // fill the alarm message
    WriteAlarmAdditionalInfo(
        &tempAdditionalParam, g_instance.attr.attr_common.PGXCNodeName, "", "", alarmItem, ALM_AT_Resume);
    // report the alarm
    AlarmReporter(alarmItem, ALM_AT_Resume, &tempAdditionalParam);
}

void ListTDEkey(const TDEKeysRecords& keys)
{
    std::cout << "[-]" << keys.database_id() << std::endl;
    std::cout << "[-]" << keys.table_id() << std::endl;
    std::cout << "[-]" << keys.cek_name() << std::endl;
    std::cout << "[-]" << keys.cek_iv() << std::endl;
    std::cout << "[-]" << keys.cek_version() << std::endl;
    std::cout << "[-]" << keys.dek_cipher() << std::endl;
    std::cout << "[-]" << keys.dek_status() << std::endl;
    std::cout << "[-]" << keys.encrypted_sample_string() << std::endl;
    std::cout << "[-]" << keys.algo() << std::endl;
}

TDEKeysFile::TDEKeysFile()
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    tde_keys_store_path = TDE_KEYS_STORE_NAME;
}

TDEKeysFile::~TDEKeysFile()
{
    google::protobuf::ShutdownProtobufLibrary();
}

/* Set the save path of gs_tde_keys.cipher. */
void TDEKeysFile::setfile(const char* gausshome)
{
    tde_keys_store_path = std::string(gausshome) + "/bin/" + TDE_KEYS_STORE_NAME;
    return;
}

bool TDEKeysFile::existed() const
{
    if (tde_keys_store_path.empty()) {
        return false;
    }

    if (access(tde_keys_store_path.c_str(), F_OK) != 0) {
        return false;
    }

    return true;
}

void TDEKeysFile::clear()
{
    if (existed()) {
        if (remove(tde_keys_store_path.c_str()) != 0) {
            std::cout << "keymanagement remove KeysFile fail.the keyFIle is: " << tde_keys_store_path << std::endl;
        }
    }
}

/* Save TDEKeys Records to files based on the results of KeyManager::init_Key().
   The file structure of TDEKeys Records is:
   Protobuf type			Name							C++ type
   --------------------------------------------------------------------
   bytes 					table_id						string
   bytes 					database_id 					string
   DEK_STATUS 			DEK_status						DEK_STATUS
   bytes 					CEK_name 						string
   bytes 					CEK_version 					string
   bytes 					CEK_IV 							string
   bytes 					encrypted_sample_string 		string
   bytes					DEK_cipher						string
   repeated fixed32 		DEK_IV 							int[]
   TDE_ALGO 				Algo 							TDE_ALGO
*/
bool TDEKeysFile::save()
{
    TDEKeysRecords tde_key;
    // 1.create TDEKeysRecords
    tde_key.set_cek_name(cek_name);
    tde_key.set_table_id(tb_uid);
    tde_key.set_database_id(db_uid);
    tde_key.set_dek_status(TDEKeysRecords::ALIVED);
    tde_key.set_cek_version(cek_version);
    tde_key.set_cek_iv(cek_IV);
    tde_key.set_encrypted_sample_string(encrypted_sample_string);
    tde_key.set_dek_cipher(dek_cipher);

    /* set dek_iv value */
    for (size_t i = 0; i < sizeof(dek_iv); ++i) {
        tde_key.add_dek_iv((int)dek_iv[i]);
    }

    /* set tde algo */
    if (tde_algo == TDE_ALGO_AES_CTR_128) {
        tde_key.set_algo(TDEKeysRecords_TDE_ALGO::TDEKeysRecords_TDE_ALGO_AES_CTR_128);
    } else if (tde_algo == TDE_ALGO_SM4_CTR_128) {
        tde_key.set_algo(TDEKeysRecords_TDE_ALGO::TDEKeysRecords_TDE_ALGO_SM4_CTR_128);
    } else {
        std::cout << "keymanagement save no algo. " << std::endl;
        return false;
    }

    // 2.fill in disk
    std::fstream output(tde_keys_store_path, std::ios::out | std::ios::trunc | std::ios::binary);
    if (!tde_key.SerializeToOstream(&output)) {
        std::cout << "keymanagement save failed: path{" << tde_keys_store_path << "} stream:" << output.get()
            << std::endl;
        clear();
        return false;
    } else {
        std::cout << "keymanagement Saved Successful!" << std::endl;
    }
    return true;
}

/* Read the file and parse out TDEKeysRecords.The file structure is defined in TDEKeysRecords.proto file.
   The file structure of TDEKeys Records is:
    Protobuf type			 Name							 C++ type
    --------------------------------------------------------------------
    bytes					 table_id						 string
    bytes					 database_id					 string
    DEK_STATUS 			 DEK_status 					 DEK_STATUS
    bytes					 CEK_name						 string
    bytes					 CEK_version					 string
    bytes					 CEK_IV 						 string
    bytes					 encrypted_sample_string		 string
    bytes					 DEK_cipher 					 string
    repeated fixed32		 DEK_IV 						 int[]
    TDE_ALGO				 Algo							 TDE_ALGO
*/
bool TDEKeysFile::parser()
{
    if (!existed()) {
        ReportAlarmAbnormalTDEKeyFile();
        return false;
    }
    TDEKeysRecords tde_key_read;
    std::fstream input(tde_keys_store_path, std::ios::in | std::ios::binary);
    if (!tde_key_read.ParseFromIstream(&input)) {
        ReportAlarmAbnormalTDEKeyFile();
        std::cout << "keymanagement parser error:" << input.get() << std::endl;
        return false;
    }

    db_uid = tde_key_read.database_id();
    tb_uid = tde_key_read.table_id();
    cek_name = tde_key_read.cek_name();
    cek_version = tde_key_read.cek_version();
    cek_IV = tde_key_read.cek_iv();
    dek_cipher = tde_key_read.dek_cipher();
    encrypted_sample_string = tde_key_read.encrypted_sample_string();

    /* parser dek_iv value */
    if (tde_key_read.dek_iv_size() != TDE_IV_LEN) {
        std::cout << "keymanagement dev iv error:" << tde_key_read.dek_iv_size() << std::endl;
        return false;
    }

    for (int i = 0; i < tde_key_read.dek_iv_size(); ++i) {
        dek_iv[i] = tde_key_read.dek_iv(i);
    }

    /*  parser algo value */
    TDEKeysRecords_TDE_ALGO algo = tde_key_read.algo();
    if (algo == TDEKeysRecords_TDE_ALGO::TDEKeysRecords_TDE_ALGO_AES_CTR_128) {
        tde_algo = TDE_ALGO_AES_CTR_128;
    } else if (algo == TDEKeysRecords_TDE_ALGO::TDEKeysRecords_TDE_ALGO_SM4_CTR_128) {
        tde_algo = TDE_ALGO_SM4_CTR_128;
    } else {
        ReportAlarmAbnormalTDEKeyFile();
        return false;
    }

    ReportResumeAbnormalTDEKeyFile();

    return true;
}

