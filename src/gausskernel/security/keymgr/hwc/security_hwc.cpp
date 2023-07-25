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
 * security_hwc.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/hwc/security_hwc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "keymgr/hwc/security_hwc.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "keymgr/security_key_mgr.h"
#include "keymgr/hwc/security_hwc_kms.h"
#include "keymgr/hwc/security_hwc_iam.h"

HwcMgr *hwc_mgr_new(KmErr *err)
{
    HwcMgr *hwc;

    hwc = (HwcMgr *)km_alloc_zero(sizeof(HwcMgr));
    if (hwc == NULL) {
        return NULL;
    }

    hwc->kmgr.err = err;
    hwc->iammgr = hwc_iam_new(err);
    hwc->kmsmgr = hwc_kms_new(hwc->iammgr, err);

    if (hwc->iammgr == NULL || hwc->kmsmgr == NULL) {
        hwc_mgr_free(hwc);
        return NULL;
    }

    return hwc;
}

void hwc_mgr_read_env(HwcMgr *hwc)
{
    char *env;
    KvScan *scan;
    int kvcnt;

    if (hwc == NULL) {
        return;
    }

    if (hwc->setarg == 1) {
        return;
    }

    env = km_env_get("HUAWEI_KMS_INFO");
    if (env == NULL || strlen(env) == 0) {
        km_err_msg(hwc->kmgr.err,
            "please use environment variabl '$HUAWEI_KMS_INFO' to set identity and project information of kms.");
        return;
    }

    scan = kv_scan_init(env);
    kvcnt = kv_scan_exec(scan);
    for (;;) {
        if (kvcnt > 0) {
            hwc_mgr_set_arg(hwc, km_str_strip(scan->key), km_str_strip(scan->value));
            kvcnt = kv_scan_exec(scan);
        } else if (kvcnt == 0) {
            hwc_mgr_set_arg(hwc, km_str_strip(scan->key), km_str_strip(scan->value));
            break;
        } else {
            km_err_msg(hwc->kmgr.err, "parse key value error, error at position %d of '$HUAWEI_KMS_INFO'.",
                scan->curpos);
            break;
        }
    }
    kv_scan_drop(scan);
}

void hwc_mgr_free(HwcMgr *hwc)
{
    if (hwc == NULL) {
        return;
    }

    hwc_iam_free(hwc->iammgr);
    hwc_kms_free(hwc->kmsmgr);
    km_free(hwc);
}

void hwc_mgr_set_arg(HwcMgr *hwc, const char *key, const char *value)
{
#define MAX_ARG_LEN 512

    if (hwc == NULL) {
        return;
    }
    if (strlen(key) > MAX_ARG_LEN) {
        return;
    }
    if (strlen(value) > MAX_ARG_LEN) {
        return;
    }

    hwc->setarg = 1;
    hwc_iam_set_arg(hwc->iammgr, key, value);
    hwc_kms_set_arg(hwc->kmsmgr, key, value);
}

static KeyMgr *hwc_new(KmErr *err)
{
    return (KeyMgr *)hwc_mgr_new(err);
}

static void hwc_free(KeyMgr *kmgr)
{
    hwc_mgr_free((HwcMgr *)(void *)kmgr);
}

static void hwc_set_arg(KeyMgr *kmgr, const char *key, const char *value)
{
    hwc_mgr_set_arg((HwcMgr *)(void *)kmgr, key, value);
}

static char *hwc_mk_select(KeyMgr *kmgr, KeyInfo key)
{
    HwcMgr *hwc = (HwcMgr *)(void *)kmgr;

    if (key.algo == NULL|| strlen(key.algo) == 0) {
        km_err_msg(hwc->kmgr.err, "failed to access huawei_kms, please set key algorithm type.");
        return NULL;
    }

    if (key.id == NULL || strlen(key.id) == 0) {
        km_err_msg(hwc->kmgr.err, "failed to access huawei_kms, please set key id.");
    }

    if (strcasecmp(key.algo, "aes_256") != 0) {
        km_err_msg(hwc->kmgr.err, "invalid algorithm '%s', huawei_kms only support 'aes_256'.", key.algo);
        return NULL;
    }

    hwc_mgr_read_env(hwc);
    if (km_err_catch(hwc->kmgr.err)) {
        return NULL;
    }

    return hwc_kms_mk_select(hwc->kmsmgr, key.id);
}

static KmUnStr hwc_mk_encrypt(KeyMgr *kmgr, KeyInfo key, KmUnStr plain)
{
#define HIS_KMS_BLOCK_LEN 16
    HwcMgr *hwc = (HwcMgr *)(void *)kmgr;
    KmUnStr cipher = {0};

    if (plain.len % HIS_KMS_BLOCK_LEN != 0) {
        km_err_msg(hwc->kmgr.err, "the length of plain we sent to huawei_kms should be exactly divided by 16.");
        return cipher;
    }

    hwc_mgr_read_env(hwc);
    if (km_err_catch(hwc->kmgr.err)) {
        return cipher;
    }

    return hwc_kms_mk_encrypt(hwc->kmsmgr, key.id, plain);
}

static KmUnStr hwc_mk_decrypt(KeyMgr *kmgr, KeyInfo key, KmUnStr cipher)
{
    HwcMgr *hwc = (HwcMgr *)(void *)kmgr;
    KmUnStr plain = {0};

    hwc_mgr_read_env(hwc);
    if (km_err_catch(hwc->kmgr.err)) {
        return plain;
    }

    return hwc_kms_mk_decrypt(hwc->kmsmgr, key.id, cipher);
}

static KmUnStr hwc_dk_create(KeyMgr *kmgr, KeyInfo key, KmUnStr *cipher)
{
    HwcMgr *hwc = (HwcMgr *)(void *)kmgr;
    KmUnStr plain = {0};

    hwc_mgr_read_env(hwc);
    if (km_err_catch(hwc->kmgr.err)) {
        return plain;
    }

    return hwc_kms_dk_create(hwc->kmsmgr, key.id, cipher);
}

KeyMethod huawei_kms = {
    "huawei_kms",

    hwc_new, /* kmgr_new */
    hwc_free, /* kmgr_free */
    hwc_set_arg, /* kmgr_set_arg */

    NULL, /* mk_create */
    NULL, /* mk_delete */
    hwc_mk_select, /* mk_select */
    hwc_mk_encrypt, /* mk_encrypt */
    hwc_mk_decrypt, /* mk_decrypt */

    hwc_dk_create, /* dk_create */
};

/*
 * currently, most restful api of the Huawei Cloud Kms and Hybrid Cloud Kms are the same.
 * we just reuse the methods here
 */
KeyMethod hcs_kms = {
    "hcs_kms",

    hwc_new, /* kmgr_new */
    hwc_free, /* kmgr_free */
    hwc_set_arg, /* kmgr_set_arg */

    NULL, /* mk_create */
    NULL, /* mk_delete */
    hwc_mk_select, /* mk_select */
    hwc_mk_encrypt, /* mk_encrypt */
    hwc_mk_decrypt, /* mk_decrypt */

    hwc_dk_create, /* dk_create */
};