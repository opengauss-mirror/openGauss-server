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
 * tde_key_manager.cpp
 *    TDE key manager is the external interface of key management
 *
 * IDENTIFICATION
 *    src/gausskernel/security/tde_key_management/tde_key_manager.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <string.h>

#include "tde_key_management/tde_key_manager.h"
#include "tde_key_management/tde_key_storage.h"
#include "utils/elog.h"
#include "securec.h"
#include "utils/memutils.h"
#include "knl/knl_session.h"

TDEKeyManager::TDEKeyManager()
{
    tde_create_data = NULL;
    tde_get_data = NULL;
    kms_instance = NULL;
}

TDEKeyManager::~TDEKeyManager()
{
    if (tde_create_data != NULL) {
        DELETE_EX2(tde_create_data);
        tde_create_data = NULL;
    }
    if (tde_get_data != NULL) {
        DELETE_EX2(tde_get_data);
        tde_get_data = NULL;
    }
    if (kms_instance != NULL) {
        DELETE_EX2(kms_instance);
        kms_instance = NULL;
    }
}

void TDEKeyManager::init()
{
    if (TDE::TDEKeyStorage::get_instance().empty()) {
        TDE::TDEKeyStorage::get_instance().init();
    }
    kms_instance = New(CurrentMemoryContext) KMSInterface();
    tde_create_data = New(CurrentMemoryContext) TDEData();
    tde_get_data = New(CurrentMemoryContext) TDEData();
    return;
}

const TDEData* TDEKeyManager::create_dek()
{
    errno_t rc = EOK;
    char* cmk_id = NULL;
    DekInfo* dek_info = NULL;

    /* use KMS interface to create DEK */
    cmk_id = get_cmk_id();
    tde_create_data->cmk_id = (char*)palloc0(strlen(cmk_id) + 1);
    rc = memcpy_s(tde_create_data->cmk_id, (strlen(cmk_id) + 1), cmk_id, (strlen(cmk_id) + 1));
    securec_check(rc, "\0", "\0");
    dek_info = kms_instance->create_kms_dek(tde_create_data->cmk_id);
    if (dek_info == NULL) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("create KMS dek failed"), errdetail("N/A"), errcause("KMS error"), 
            erraction("check KMS connect or config parameter")));
    }
    tde_create_data->dek_cipher = (char*)palloc0(strlen(dek_info->cipher) + 1);
    rc = memcpy_s(tde_create_data->dek_cipher, (strlen(dek_info->cipher) + 1), dek_info->cipher, 
        (strlen(dek_info->cipher) + 1));
    securec_check(rc, "\0", "\0");
    tde_create_data->dek_plaintext = (char*)palloc0(strlen(dek_info->plain) + 1);
    rc = memcpy_s(tde_create_data->dek_plaintext, (strlen(dek_info->plain) + 1), dek_info->plain, 
        (strlen(dek_info->plain) + 1));
    securec_check(rc, "\0", "\0");
    rc = memset_s(dek_info->cipher, strlen(dek_info->cipher), 0, strlen(dek_info->cipher));
    securec_check(rc, "\0", "\0");
    pfree_ext(dek_info->cipher);
    pfree_ext(dek_info->plain);
    pfree_ext(dek_info);
    return tde_create_data;
}

const TDEData* TDEKeyManager::get_dek(const char* cmk_id, const char* dek_cipher)
{
    errno_t rc = EOK;
    char* dek_plain = NULL;
    /* use KMS interface to get DEK */
    tde_get_data->cmk_id = (char*)palloc0(strlen(cmk_id) + 1);
    rc = memcpy_s(tde_get_data->cmk_id, (strlen(cmk_id) + 1), cmk_id, (strlen(cmk_id) + 1));
    securec_check(rc, "\0", "\0");
    dek_plain = kms_instance->get_kms_dek(tde_get_data->cmk_id, dek_cipher);
    if (dek_plain == NULL) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("get KMS dek failed"), errdetail("N/A"), errcause("KMS error"), 
            erraction("check KMS connect or config parameter")));
    }
    tde_get_data->dek_cipher = (char*)palloc0(strlen(dek_cipher) + 1);
    rc = memcpy_s(tde_get_data->dek_cipher, (strlen(dek_cipher) + 1), dek_cipher, (strlen(dek_cipher) + 1));
    securec_check(rc, "\0", "\0");
    tde_get_data->dek_plaintext = (char*)palloc0(strlen(dek_plain) + 1);
    rc = memcpy_s(tde_get_data->dek_plaintext, (strlen(dek_plain) + 1), dek_plain, (strlen(dek_plain) + 1));
    securec_check(rc, "\0", "\0");
    rc = memset_s(dek_plain, strlen(dek_plain), 0, strlen(dek_plain));
    securec_check(rc, "\0", "\0");
    pfree_ext(dek_plain);
    return tde_get_data;
}

char* TDEKeyManager::get_cmk_id()
{
    if (u_sess->attr.attr_security.tde_cmk_id == NULL || strlen(u_sess->attr.attr_security.tde_cmk_id) == 0) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("get cmk id failed for Transparent Data Encryption"),
            errdetail("guc parameter tde_cmk_id must be set correctly to use TDE feature")));
    }
    return u_sess->attr.attr_security.tde_cmk_id;
}

bool TDEKeyManager::save_key(const TDEData* tde_data)
{
    errno_t rc = EOK;
    bool result = false;
    TDECacheEntry* kms_cache_entry = NULL;
    TimestampTz cur_timestamp = 0;

    /* prepare TDE cache entry */
    kms_cache_entry = (TDECacheEntry*)palloc0(sizeof(TDECacheEntry));
    kms_cache_entry->key_cipher = (char*)palloc0(strlen(tde_data->dek_cipher) + 1);
    rc = memcpy_s(kms_cache_entry->key_cipher, (strlen(tde_data->dek_cipher) + 1), tde_data->dek_cipher, 
        (strlen(tde_data->dek_cipher) + 1));
    securec_check(rc, "\0", "\0");
    kms_cache_entry->dek_plaintext = (char*)palloc0(strlen(tde_data->dek_plaintext) + 1);
    rc = memcpy_s(kms_cache_entry->dek_plaintext, (strlen(tde_data->dek_plaintext) + 1), tde_data->dek_plaintext, 
        (strlen(tde_data->dek_plaintext) + 1));
    securec_check(rc, "\0", "\0");
    cur_timestamp = GetCurrentTimestamp();
    kms_cache_entry->timestamp = cur_timestamp;

    /* insert TDE key into TDE cache */
    result = TDE::TDEKeyStorage::get_instance().insert_cache(kms_cache_entry);
    if (!result) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), 
            errmsg("TDE cache insert failed")));
    }
    rc = memset_s(kms_cache_entry->dek_plaintext, strlen(kms_cache_entry->dek_plaintext), 0, 
        strlen(kms_cache_entry->dek_plaintext));
    securec_check(rc, "\0", "\0");
    pfree_ext(kms_cache_entry->key_cipher);
    pfree_ext(kms_cache_entry->dek_plaintext);
    pfree_ext(kms_cache_entry);
    return result;
}

const char* TDEKeyManager::get_key(const char* cmk_id, const char* dek_cipher)
{
    errno_t rc = EOK;
    char* kms_cipher = NULL;
    char* kms_plaintext = NULL;

    kms_cipher = (char*)palloc0(strlen(dek_cipher) + 1);
    rc = memcpy_s(kms_cipher, (strlen(dek_cipher) + 1), dek_cipher, 
        (strlen(dek_cipher) + 1));
    securec_check(rc, "\0", "\0");
    kms_plaintext = TDE::TDEKeyStorage::get_instance().search_cache(kms_cipher);
    if (kms_plaintext != NULL) {
        /* cache has key */
        pfree_ext(kms_cipher);
        return kms_plaintext;
    } else {
        /* key not found */
        const TDEData* tde_data = NULL;
        tde_data = get_dek(cmk_id, kms_cipher);
        if (tde_data == NULL) {
            pfree_ext(kms_cipher);
            ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
                errmsg("get KMS DEK is NULL"), errdetail("N/A"), errcause("get KMS dek_plaintext failed"), 
                erraction("check KMS network or cipher is right")));
            return kms_plaintext;
        }
        /* update to cache */
        save_key(tde_data);
        kms_plaintext = tde_data->dek_plaintext;
    }
    pfree_ext(kms_cipher);
    return kms_plaintext;
}
