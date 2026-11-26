/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
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
#include "tde_key_management/sdf_tde_provider.h"
#include "utils/elog.h"
#include "securec.h"
#include "utils/memutils.h"
#include "knl/knl_session.h"
#include "storage/ipc.h"

static void format_tde_cache_key(const char* cmkId, const char* dekCipher, char* key, Size key_size)
{
    if (cmkId == NULL || dekCipher == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("CMK_ID and DEK cipher are required for TDE key cache")));
    }
    Size cmk_len = strlen(cmkId);
    Size cipher_len = strlen(dekCipher);
    if (cmk_len > key_size || key_size - cmk_len < sizeof("|") || cipher_len > key_size - cmk_len - sizeof("|")) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("TDE cache key exceeds the maximum size")));
    }
    errno_t rc = memcpy_s(key, key_size, cmkId, cmk_len);
    securec_check(rc, "\0", "\0");
    key[cmk_len] = '|';
    rc = memcpy_s(key + cmk_len + 1, key_size - cmk_len - 1, dekCipher, cipher_len + 1);
    if (rc != EOK) {
        securec_check(rc, "\0", "\0");
    }
}

TDEKeyManager::TDEKeyManager() : tde_create_data(NULL), tde_get_data(NULL), kms_instance(NULL) {}

TDEKeyManager::~TDEKeyManager()
{
    DELETE_EX2(tde_create_data);
    DELETE_EX2(tde_get_data);
    DELETE_EX2(kms_instance);
}

void TDEKeyManager::cleanup(int code, Datum arg)
{
    TDEKeyManager* volatile* manager = (TDEKeyManager* volatile*)DatumGetPointer(arg);
    DELETE_EX2(*manager);
}

void TDEKeyManager::init()
{
    if (g_instance.attr.attr_security.tde_key_provider == TDE_KEY_PROVIDER_KMS &&
        TDE::TDEKeyStorage::get_instance().empty()) {
        TDE::TDEKeyStorage::get_instance().init();
    }
    return;
}

const TDEData* TDEKeyManager::create_dek()
{
    if (tde_create_data == NULL) {
        tde_create_data = New(CurrentMemoryContext) TDEData();
    }
    tde_create_data->cmk_id = get_cmk_id();

    if (g_instance.attr.attr_security.tde_key_provider == TDE_KEY_PROVIDER_SDF) {
        char error_msg[512] = {0};
        if (!SdfCreateTdeDek(tde_create_data->cmk_id, &tde_create_data->dek_cipher,
            error_msg, sizeof(error_msg))) {
            ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("create SDF TDE DEK failed"),
                errdetail("%s", error_msg[0] == '\0' ? "SDF provider returned no key" : error_msg),
                errhint("Check the SDF module, configured KEK slot and access password.")));
        }
        return tde_create_data;
    }

    /* use KMS interface to create DEK */
    if (kms_instance == NULL) {
        kms_instance = New(CurrentMemoryContext) KMSInterface();
    }
    DekInfo* dek_info = kms_instance->create_kms_dek(tde_create_data->cmk_id);
    if (dek_info == NULL) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("create KMS dek failed"), errdetail("N/A"), errcause("KMS error"), 
            erraction("check KMS connect or config parameter")));
    }
    /* KMS returns caller-owned palloc buffers; TDEData frees and wipes them. */
    tde_create_data->dek_cipher = dek_info->cipher;
    tde_create_data->dek_plaintext = dek_info->plain;
    pfree_ext(dek_info);
    return tde_create_data;
}

const TDEData* TDEKeyManager::get_dek(const char* cmkId, const char* dekCipher)
{
    if (tde_get_data == NULL) {
        tde_get_data = New(CurrentMemoryContext) TDEData();
    }
    /* retain the envelope and CMK_ID with the returned key material */
    tde_get_data->cmk_id = pstrdup(cmkId);
    tde_get_data->dek_cipher = pstrdup(dekCipher);

    if (g_instance.attr.attr_security.tde_key_provider == TDE_KEY_PROVIDER_SDF) {
        char error_msg[512] = {0};
        if (!SdfGetTdeDek(cmkId, dekCipher, error_msg, sizeof(error_msg))) {
            ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("get SDF TDE DEK failed"),
                errdetail("%s", error_msg[0] == '\0' ? "SDF provider returned no key" : error_msg),
                errhint("Verify that tde_key_provider matches the provider used to create this data; "
                    "changing the setting does not migrate encrypted keys. "
                    "Check the SDF module, original KEK and access password.")));
        }
        return tde_get_data;
    }

    /* use KMS interface to get DEK */
    if (kms_instance == NULL) {
        kms_instance = New(CurrentMemoryContext) KMSInterface();
    }
    tde_get_data->dek_plaintext = kms_instance->get_kms_dek(tde_get_data->cmk_id, dekCipher);
    if (tde_get_data->dek_plaintext == NULL) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), 
            errmsg("get KMS dek failed"), errdetail("N/A"), errcause("KMS error"), 
            erraction("check KMS connect or config parameter")));
    }
    return tde_get_data;
}

char* TDEKeyManager::get_cmk_id()
{
    if (!g_instance.attr.attr_security.enable_tde) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
            errmsg("get cmk id failed for Transparent Data Encryption"),
            errdetail("enable_tde must be enabled to use TDE")));
    }
    if (g_instance.attr.attr_security.tde_key_provider == TDE_KEY_PROVIDER_SDF) {
        char encoded[CMK_ID_LEN];
        if (!SdfTdeGetCmkId(encoded, sizeof(encoded))) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("SDF TDE startup configuration is not initialized")));
        }
        return pstrdup(encoded);
    }
    const char* id = u_sess->attr.attr_security.tde_cmk_id;
    if (id == NULL || strlen(id) != CMK_ID_TEXT_LEN) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("KMS tde_cmk_id must contain the 36-character CMK_ID")));
    }
    return pstrdup(id);
}

bool TDEKeyManager::save_key(const TDEData* tde_data)
{
    /* SDF caches opaque handles in the DN's shared session, never plaintext DEKs. */
    if (g_instance.attr.attr_security.tde_key_provider == TDE_KEY_PROVIDER_SDF) {
        return true;
    }
    char cache_key[CMK_ID_LEN + DEK_CIPHER_LEN + 2];
    format_tde_cache_key(tde_data->cmk_id, tde_data->dek_cipher, cache_key, sizeof(cache_key));
    TDECacheEntry entry = {0};
    entry.key_cipher = cache_key;
    entry.dek_plaintext = tde_data->dek_plaintext;
    entry.timestamp = GetCurrentTimestamp();
    return TDE::TDEKeyStorage::get_instance().insert_cache(&entry);
}

void TDEKeyManager::get_key(const char* cmkId, const char* dekCipher, char* dek_plaintext, Size output_size)
{
    if (g_instance.attr.attr_security.tde_key_provider == TDE_KEY_PROVIDER_SDF) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("SDF TDE keys are available only through the SDF page cipher interface")));
    }
    if (dek_plaintext == NULL || output_size < DEK_PLAIN_HEX_LEN + 1) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid output buffer for TDE plaintext DEK")));
    }
    dek_plaintext[0] = '\0';
    char cache_key[CMK_ID_LEN + DEK_CIPHER_LEN + 2] = {0};
    format_tde_cache_key(cmkId, dekCipher, cache_key, sizeof(cache_key));
    TDE::TDEKeyStorage& cache = TDE::TDEKeyStorage::get_instance();
    if (cache.empty()) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("TDE key cache is not initialized")));
    }
    if (cache.search_cache(cache_key, dek_plaintext, output_size)) {
        return;
    }

    TDEKeyManager* volatile manager = NULL;
    PG_ENSURE_ERROR_CLEANUP(TDEKeyManager::cleanup, PointerGetDatum(&manager));
    {
        manager = New(CurrentMemoryContext) TDEKeyManager();
        const TDEData* data = manager->get_dek(cmkId, dekCipher);
        if (data == NULL || data->dek_plaintext == NULL) {
            ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("KMS provider returned no plaintext DEK")));
        }
        manager->save_key(data);
        errno_t rc = strcpy_s(dek_plaintext, output_size, data->dek_plaintext);
        if (rc != EOK) {
            securec_check(rc, "\0", "\0");
        }
    }
    PG_END_ENSURE_ERROR_CLEANUP(TDEKeyManager::cleanup, PointerGetDatum(&manager));
    TDEKeyManager::cleanup(0, PointerGetDatum(&manager));
}
