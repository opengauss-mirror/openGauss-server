/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * security_key_adpt.cpp
 *      you can register your own key management tool/component/service here.
 *      what's more, you can choose which to use according to the version and usage scenarios.
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/keymgr/security_key_adpt.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "keymgr/security_key_adpt.h"
#include <stdio.h>
#include <pthread.h>
#include "keymgr/comm/security_version.h"
#include "keymgr/hwc/security_hwc.h"
#include "keymgr/comm/security_utils.h"
#include "keymgr/his/security_his.h"
#include "keymgr/localkms/security_localkms.h"
#include "keymgr/ktool/security_gs_ktool.h"

KeyMethod *g_key_method[MAX_KEY_MGR_NUM] = {
    &his_kms, /* HisAccount, HisSecret, HisAppId, HisEnterprise, HisIamUrl, HisKmsUrl, */

#ifndef FRONTEND
    &hcs_kms,
#endif

#ifdef ENABLE_HUAWEI_KMS
    &huawei_kms,
#endif

#ifdef ENABLE_LOCALKMS
#ifdef FRONTEND
    &localkms,
#endif
#endif

#ifdef ENABLE_GS_KTOOL
#ifdef FRONTEND
    &gs_ktool,
#endif
#endif

    NULL, /* must end with 'NULL' */
};

KeyAdpt *key_adpt_new()
{
#define KEY_ADPT_ERR_BUF_SZ 4096
    KmErr *err;
    KeyAdpt *adpt;

    err = km_err_new(KEY_ADPT_ERR_BUF_SZ);
    if (err == NULL) {
        return NULL;
    }

    adpt = (KeyAdpt *)km_cxt_alloc_zero(err->cxt, sizeof(KeyAdpt));
    if (adpt == NULL) {
        km_err_free(err);
        return NULL;
    }
    adpt->err = err;

    for (int i = 0; g_key_method[i] != NULL; i++) {
        adpt->methods[i] = g_key_method[i];
    }

    return adpt;
}

void key_adpt_free(KeyAdpt *kadpt)
{
    if (kadpt == NULL) {
        return;
    }

    for (int i = 0; kadpt->methods[i] != NULL; i++) {
        if (kadpt->methods[i]->kmgr_free != NULL && kadpt->mgrs[i] != NULL) {
            kadpt->methods[i]->kmgr_free(kadpt->mgrs[i]);
            kadpt->mgrs[i] = NULL;
        }
    }
    km_err_free(kadpt->err);
    km_free(kadpt);
}

static int key_adpt_get_index(KeyAdpt *kadpt, const char *type)
{
    if (type == NULL) {
        km_err_msg(kadpt->err, "fail to get key manager type.");
        return -1;
    }

    for (int i = 0; kadpt->methods[i] != NULL; i++) {
        if (strcasecmp(kadpt->methods[i]->type, type) != 0) {
            continue;
        }
#ifndef FRONTEND
        MemoryContext old = MemoryContextSwitchTo(kadpt->err->cxt);
#endif
        if (kadpt->mgrs[i] == NULL) {
            kadpt->mgrs[i] = kadpt->methods[i]->kmgr_new(kadpt->err);
        }
#ifndef FRONTEND
        MemoryContextSwitchTo(old);
#endif
        if (kadpt->mgrs[i] == NULL) {
            if (!km_err_catch(kadpt->err)) {
                km_err_msg(kadpt->err, "failed to init key manager of '%s'.", type);
            }
            return -1;
        }
        return i;
    }

    km_err_msg(kadpt->err, "unknown key manager type: '%s'.", type);
    return -1;
}

void key_adpt_set_arg(KeyAdpt *kadpt, const char *type, const char *key, const char *value)
{
    int idx;
    KeyMgr *kmgr;
    KeyMethod *kmethod;

    if (kadpt == NULL) {
        return;
    }

    idx = key_adpt_get_index(kadpt, type);
    if (idx < 0) {
        return;
    }

    kmethod = kadpt->methods[idx];
    kmgr = kadpt->mgrs[idx];

#ifndef FRONTEND
    MemoryContext old = MemoryContextSwitchTo(kadpt->err->cxt);
#endif
    if (kmethod->kmgr_set_arg != NULL) {
        kmethod->kmgr_set_arg(kmgr, key, value);
    }
#ifndef FRONTEND
    MemoryContextSwitchTo(old);
#endif
}

void key_adpt_set_args(KeyAdpt *kadpt, const char *type, const char *args)
{
    int idx;
    KeyMethod *kmethod;
    KvScan *scan;
    int kvcnt;

    if (kadpt == NULL) {
        return;
    }

    km_err_reset(kadpt->err);
    idx = key_adpt_get_index(kadpt, type);
    if (idx < 0) {
        return;
    }

    kmethod = kadpt->methods[idx];
    scan = kv_scan_init(args);
    for (;;) {
        kvcnt = kv_scan_exec(scan);
        if (kvcnt >= 0) {
            if (kmethod->kmgr_set_arg != NULL) {
                kmethod->kmgr_set_arg(kadpt->mgrs[idx], km_str_strip(scan->key), km_str_strip(scan->value));
            }
        } else {
            km_err_msg(kadpt->err, "failed to parse key info of '%s', error at position %d.", type, scan->curpos);
        }

        if (kvcnt <= 0) {
            break;
        }
    }
    kv_scan_drop(scan);
}

void key_adpt_mk_create(KeyAdpt *kadpt, KeyInfo info)
{
    int idx;
    KeyMgr *kmgr;
    KeyMethod *kmethod;

    if (kadpt == NULL) {
        return;
    }

    km_err_reset(kadpt->err);

    idx = key_adpt_get_index(kadpt, info.type);
    if (idx < 0) {
        return;
    }

    kmethod = kadpt->methods[idx];
    kmgr = kadpt->mgrs[idx];

#ifndef FRONTEND
    MemoryContext old = MemoryContextSwitchTo(kadpt->err->cxt);
#endif
    if (kmethod->mk_create != NULL) {
        kmethod->mk_create(kmgr, info);
    }
#ifndef FRONTEND
    MemoryContextSwitchTo(old);
#endif
}

void key_adpt_mk_delete(KeyAdpt *kadpt, KeyInfo info)
{
    int idx;
    KeyMgr *kmgr;
    KeyMethod *kmethod;

    if (kadpt == NULL) {
        return;
    }

    km_err_reset(kadpt->err);

    idx = key_adpt_get_index(kadpt, info.type);
    if (idx < 0) {
        return;
    }

    kmethod = kadpt->methods[idx];
    kmgr = kadpt->mgrs[idx];

#ifndef FRONTEND
    MemoryContext old = MemoryContextSwitchTo(kadpt->err->cxt);
#endif
    if (kmethod->mk_delete != NULL) {
        kmethod->mk_delete(kmgr, info);
    }
#ifndef FRONTEND
    MemoryContextSwitchTo(old);
#endif
}

char *key_adpt_mk_select(KeyAdpt *kadpt, KeyInfo info)
{
    int idx;
    KeyMgr *kmgr;
    KeyMethod *kmethod;
    char *keystate = NULL;

    if (kadpt == NULL) {
        return NULL;
    }

    km_err_reset(kadpt->err);

    idx = key_adpt_get_index(kadpt, info.type);
    if (idx < 0) {
        return NULL;
    }

    kmethod = kadpt->methods[idx];
    kmgr = kadpt->mgrs[idx];

#ifndef FRONTEND
    MemoryContext old = MemoryContextSwitchTo(kadpt->err->cxt);
#endif
    if (kmethod->mk_select != NULL) {
        keystate = kmethod->mk_select(kmgr, info);
    }
#ifndef FRONTEND
    MemoryContextSwitchTo(old);
#endif

    return keystate;
}

KmUnStr key_adpt_mk_encrypt(KeyAdpt *kadpt, KeyInfo info, KmUnStr plain)
{
    int idx;
    KeyMgr *kmgr;
    KeyMethod *kmethod;
    KmUnStr cipher = {0};

    if (kadpt == NULL) {
        return cipher;
    }

    km_err_reset(kadpt->err);

    idx = key_adpt_get_index(kadpt, info.type);
    if (idx < 0) {
        return cipher;
    }

    kmethod = kadpt->methods[idx];
    kmgr = kadpt->mgrs[idx];

#ifndef FRONTEND
    MemoryContext old = MemoryContextSwitchTo(kadpt->err->cxt);
#endif
    if (kmethod->mk_enrypt != NULL) {
        cipher = kmethod->mk_enrypt(kmgr, info, plain);
    }
#ifndef FRONTEND
    MemoryContextSwitchTo(old);
#endif

    return cipher;
}

KmUnStr key_adpt_mk_decrypt(KeyAdpt *kadpt, KeyInfo info, KmUnStr cipher)
{
    int idx;
    KeyMgr *kmgr;
    KeyMethod *kmethod;
    KmUnStr plain = {0};

    if (kadpt == NULL) {
        return plain;
    }

    km_err_reset(kadpt->err);

    idx = key_adpt_get_index(kadpt, info.type);
    if (idx < 0) {
        return plain;
    }

    kmethod = kadpt->methods[idx];
    kmgr = kadpt->mgrs[idx];

#ifndef FRONTEND
    MemoryContext old = MemoryContextSwitchTo(kadpt->err->cxt);
#endif
    if (kmethod->mk_decrypt != NULL) {
        plain = kmethod->mk_decrypt(kmgr, info, cipher);
    }
#ifndef FRONTEND
    MemoryContextSwitchTo(old);
#endif

    return plain;
}

KmUnStr key_adpt_dk_create(KeyAdpt *kadpt, KeyInfo info, KmUnStr *cipher)
{
    int idx;
    KeyMgr *kmgr;
    KeyMethod *kmethod;
    KmUnStr plain = {0};

    if (kadpt == NULL) {
        return plain;
    }

    km_err_reset(kadpt->err);

    idx = key_adpt_get_index(kadpt, info.type);
    if (idx < 0) {
        return plain;
    }

    kmethod = kadpt->methods[idx];
    kmgr = kadpt->mgrs[idx];

#ifndef FRONTEND
    MemoryContext old = MemoryContextSwitchTo(kadpt->err->cxt);
#endif
    if (kmethod->dk_create != NULL) {
        plain = kmethod->dk_create(kmgr, info, cipher);
    }
#ifndef FRONTEND
    MemoryContextSwitchTo(old);
#endif

    return plain;
}

bool key_adpt_catch_err(KeyAdpt *kadpt)
{
    if (kadpt == NULL) {
        return true;
    }

    return km_err_catch(kadpt->err);
}

const char *key_adpt_get_err(KeyAdpt *kadpt)
{
    if (kadpt == NULL) {
        return "key adaptor is null.";
    }
    
    if (strlen(kadpt->err->buf) != 0) {
        return kadpt->err->buf;
    }

    return "key adaptor: unknown error.";
}
