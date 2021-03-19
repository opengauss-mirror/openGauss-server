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
 * FIKMSmessage.cpp
 *    handle the message from FI KMS component
 *
 * IDENTIFICATION
 *    src/gausskernel/security/keymanagement/FIKMSmessage.cpp
 *
 * ---------------------------------------------------------------------------------------
 */


#include <iostream>

#include "keymanagement/FIKMSmessage.h"
#include "postgres.h"
#include "knl/knl_variable.h"

FIKMSmessage::FIKMSmessage()
{
    response_data = NULL;
}

FIKMSmessage::~FIKMSmessage()
{
    if (response_data != NULL) {
        cJSON_Delete(response_data);
        response_data = NULL;
    }
}

/* Converting from cjson to string and freeing up the memory of JSON application. */
std::string json_to_str_and_delete_json(cJSON* monitor)
{
    char* js_str = cJSON_Print(monitor);
    cJSON_Delete(monitor);
    if (js_str == NULL)
        return "";
    std::string out = std::string(js_str);
    /* when using gs_encrypt, PGXCNodeName is NULL. When using gaussdb the PGXCNodeName
     * have value. For binary process, which do not implement cJSON_InitHooks(). for example, gs_encrypt.
     */
    if (g_instance.attr.attr_common.PGXCNodeName == NULL) {
        free(js_str);
    } else {
        /* for gaussdb process,which implement cJSON_InitHooks(). */
        cJSON_internal_free(js_str);
    }

    return out;
}

/* Conversion from str to JSON format.Most of the time, serizial_str is a data coming from the net. */
bool FIKMSmessage::parser(std::string serizial_str)
{
    if (response_data != NULL) {
        cJSON_Delete(response_data);
        response_data = NULL;
    }
    response_data = cJSON_Parse(serizial_str.c_str());
    if (response_data == NULL) {
        std::cout << "keymanagement Parse error:" << serizial_str << std::endl;
        return false;
    }
    return true;
}

/*
 * Generate the information needed to create the CEK.
 *
 * KMS's URL: POST http://HOST:PORT/kms/v1/keys
 *
 * KMS's message:
 *
 *  Content-Type: application/json
 *
 *  {
 *     "name" 	   : "<key-name>",
 *     "cipher"	   : "<cipher>",
 *     "length"	   : <length>,
 *  }
 *
 * Return:
 *        String result after JSON serialization
 */
std::string FIKMSmessage::create(std::string name, std::string cipher, int length)
{
    if (name.empty() || cipher.empty()) {
        std::cout << "keymanagement name or cipher is empty.name[" << name << "] cipher[" << cipher << "]" << std::endl;
        return "";
    }

    cJSON* monitor = cJSON_CreateObject();
    if (monitor == NULL) {
        return "";
    }
    cJSON_AddItemToObject(monitor, FIKMS_MESSAGE_name, cJSON_CreateString(name.c_str()));
    cJSON_AddItemToObject(monitor, FIKMS_MESSAGE_cipher, cJSON_CreateString(cipher.c_str()));
    cJSON_AddItemToObject(monitor, FIKMS_MESSAGE_length, cJSON_CreateNumber(length));
    return json_to_str_and_delete_json(monitor);
}

/*
 * Generate the information needed to create the DEK.
 *
 * KMS's URL: POST http://HOST:PORT/kms/v1/keyversion/<version-name>/_eek?eek_op=decrypt
 *
 * KMS's message:
 *
 *  Content-Type: application/json
 *
 *  {
 *    "name"		: "<key-name>",
 *    "iv" 			: "<iv>",			//base64
 *    "material"	: "<material>",	//base64
 *  }
 *
 * Return:
 *        String result after JSON serialization
 */
std::string FIKMSmessage::create(std::string name, std::string iv, std::string material)
{
    if (name.empty() || iv.empty() || material.empty()) {
        std::cout << "keymanagement name or cipher is empty.name[" << name << "] iv[" << iv << "] material[" << material
            << "]" << std::endl;
        return "";
    }

    cJSON* monitor = cJSON_CreateObject();
    if (monitor == NULL) {
        return "";
    }
    cJSON_AddItemToObject(monitor, FIKMS_MESSAGE_name, cJSON_CreateString(name.c_str()));
    cJSON_AddItemToObject(monitor, FIKMS_MESSAGE_iv, cJSON_CreateString(iv.c_str()));
    cJSON_AddItemToObject(monitor, FIKMS_MESSAGE_material, cJSON_CreateString(material.c_str()));

    return json_to_str_and_delete_json(monitor);
}

/*
 * Parse the return information of KMS creating DEK.Must configure the user's[mppdb] keymangent permissions in FI KMS.
 * Content-Type: application/json
 * {
 *  "name"        : "versionName",
 *  "material"    : "<material>",    //base64, not present without GET ACL
 * }
 * Return:  versionName --> CEK_versionname
 */
bool FIKMSmessage::parser_create_CEK(std::string& CEK_versionname)
{
    if (is_exception()) {
        return false;
    }

    cJSON* version = cJSON_GetObjectItem(response_data, FIKMS_MESSAGE_name);
    if (version != NULL) {
        CEK_versionname = version->valuestring;
        return true;
    }

    std::cout << "keymanagement create cek message parser error:" << CEK_versionname << std::endl;
    return false;
}

/*
 * Parse the return information of KMS creating DEK.
 * KMS's message:
 *  Content-Type: application/json
 *  [
 *    {
 *      "versionName"         : "<encryptionVersionName>",
 *      "iv"                  : "<iv>",          //base64
 *      "encryptedKeyVersion" : {
 *          "versionName"       : "EEK",
 *          "material"          : "<material>",    //base64
 *      }
 *    },
 *    {
 *      "versionName"         : "<encryptionVersionName>",
 *      "iv"                  : "<iv>",          //base64
 *      "encryptedKeyVersion" : {
 *          "versionName"       : "EEK",		   // default value

 *          "material"          : "<material>",    // base64
 *      }
 *    },
 *  ]
 *	Return:   iv                    --> cek_IV
 * 			material              --> DEK_cipher
 * 			encryptionVersionName --> CEK_versionname
 */
bool FIKMSmessage::parser_create_DEK(std::string& cek_IV, std::string& DEK_cipher, std::string& CEK_versionname)
{
    if (is_exception()) {
        return false;
    }

    if (cJSON_GetArraySize(response_data) > 0) {
        /* Because only one key is produced in gaussdb, only one value of the array is required. */
        cJSON* item = cJSON_GetArrayItem(response_data, 0);
        if (item != NULL) {
            cJSON* iv = cJSON_GetObjectItem(item, FIKMS_MESSAGE_iv);
            if (iv != NULL) {
                cek_IV = iv->valuestring;
            }
            cJSON* version = cJSON_GetObjectItem(item, FIKMS_MESSAGE_versionname);
            if (version != NULL) {
                CEK_versionname = version->valuestring;
            }
        }

        if (!cek_IV.empty()) {
            cJSON* encKV = cJSON_GetObjectItem(item, FIKMS_MESSAGE_encryptedKeyVersion);
            if (encKV != NULL) {
                cJSON* dek_cipher = cJSON_GetObjectItem(encKV, FIKMS_MESSAGE_material);
                if (dek_cipher != NULL) {
                    DEK_cipher = dek_cipher->valuestring;
                    return true;
                }
            }
        }
    }
    std::cout << "keymanagement create dek message parser error." << std::endl;
    return false;
}

/*
 * Parse the return information of KMS getting DEK.
 * KMS's message:
 * Content-Type: application/json
 * {
 *   "name"        : "EK",			  //default value
 *   "material"    : "<material>",    //base64
 * }
 *	Return: material            --> DEK
 */
bool FIKMSmessage::parser_get_DEK(std::string& DEK)
{
    if (is_exception()) {
        return false;
    }

    cJSON* dek_item = cJSON_GetObjectItem(response_data, FIKMS_MESSAGE_material);
    if (dek_item != NULL) {
        DEK = dek_item->valuestring;
        return true;
    }
    std::cout << "keymanagement get dek message parser error." << std::endl;
    return false;
}

/*
 * Parse the return information of KMS getting CEK Names.
 *
 * Content-Type: application/json
 *
 * [
 *   "<key-name>",
 *   "<key-name>",
 *   ...
 * ]
 *
 * Reurn:
 *      true  CEK_name is one if key-name.
 *      false CEK_name is not one if the key-name.
 */
bool FIKMSmessage::parser_get_CEK(const std::string& CEK_name)
{
    if (is_exception()) {
        return false;
    }

    int arrayCount = cJSON_GetArraySize(response_data);

    for (int i = 0; i < arrayCount; ++i) {
        cJSON* cek_item = cJSON_GetArrayItem(response_data, i);
        if (cek_item && CEK_name.compare(cek_item->valuestring) == 0)
            return true;
    }

    std::cout << "keymanagement get cek message parser error." << std::endl;
    return false;
};

/*
 * Check exception in the return information of KMS.
 *
 * Content-Type: application/json
 *
 * {
 *   "RemoteException" :{
 *		"message" : "<error message>",
 *		"exception" : "<Exception>",
 *		"javaClassName" : "<java exception>"
 * 		}
 * }
 *
 * Return:
 *    true is exception.
 *    false not exceprion.
 */
bool FIKMSmessage::is_exception()
{
    if (response_data == NULL) {
        std::cout << "keymanagement response_data is NULL." << std::endl;
        return true;
    }
    cJSON* remote_exception = cJSON_GetObjectItem(response_data, FIKMS_MESSAGE_exception);
    if (remote_exception != NULL) {
        std::cout << "keymanagement Remote API error." << std::endl;
        return true;
    }
    return false;
}

