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
 * security_his.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/his/security_his.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "keymgr/his/security_his.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "keymgr/security_key_mgr.h"
#include "keymgr/his/security_his_kms.h"
#include "keymgr/his/security_his_iam.h"

HisMgr *his_mgr_new(KmErr *err)
{
    HisMgr *his;

    his = (HisMgr *)km_alloc_zero(sizeof(HisMgr));
    if (his == NULL) {
        return NULL;
    }

    his->kmgr.err = err;
    his->iammgr = his_iam_new(err);
    his->kmsmgr = his_kms_new(his->iammgr, err);

    if (his->iammgr == NULL || his->kmsmgr == NULL) {
        his_mgr_free(his);
        return NULL;
    }

    return his;
}

void his_mgr_read_env(HisMgr *his)
{
    char *env;
    KvScan *scan;
    int kvcnt;

    if (his == NULL) {
        return;
    }

    if (his->setarg == 1) {
        return;
    }

    env = km_env_get("HIS_KMS_INFO");
    if (env == NULL || strlen(env) == 0) {
        km_err_msg(his->kmgr.err,
            "please use environment variabl '$HIS_KMS_INFO' to set identity and project information of kms.");
        return;
    }

    scan = kv_scan_init(env);
    kvcnt = kv_scan_exec(scan);
    for (;;) {
        if (kvcnt > 0) {
            his_mgr_set_arg(his, km_str_strip(scan->key), km_str_strip(scan->value));
            kvcnt = kv_scan_exec(scan);
        } else if (kvcnt == 0) {
            his_mgr_set_arg(his, km_str_strip(scan->key), km_str_strip(scan->value));
            break;
        } else {
            km_err_msg(his->kmgr.err, "parse key value error, error at position %d of '$HIS_KMS_INFO'.", scan->curpos);
            break;
        }
    }
    kv_scan_drop(scan);
}

void his_mgr_free(HisMgr *his)
{
    if (his == NULL) {
        return;
    }

    his_iam_free(his->iammgr);
    his_kms_free(his->kmsmgr);

    km_free(his);
}

void his_mgr_set_arg(HisMgr *his, const char *key, const char *value)
{
#define MAX_ARG_LEN 512

    if (his == NULL) {
        return;
    }
    if (strlen(key) > MAX_ARG_LEN) {
        return;
    }
    if (strlen(value) > MAX_ARG_LEN) {
        return;
    }

    his->setarg = 1;
    his_iam_set_arg(his->iammgr, key, value);
    his_kms_set_arg(his->kmsmgr, key, value);
}

static KeyMgr *his_new(KmErr *err)
{
    return (KeyMgr *)his_mgr_new(err);
}

static void his_free(KeyMgr *kmgr)
{
    his_mgr_free((HisMgr *)(void *)kmgr);
}

static void his_set_arg(KeyMgr *kmgr, const char *key, const char *value)
{
    his_mgr_set_arg((HisMgr *)(void *)kmgr, key, value);
}

static char *his_mk_select(KeyMgr *kmgr, KeyInfo key)
{
    HisMgr *his = (HisMgr *)(void *)kmgr;

    if (key.algo == NULL) {
        km_err_msg(his->kmgr.err, "please set key algorithm type.");
        return NULL;
    }

    if (strcasecmp(key.algo, "aes_256") != 0) {
        km_err_msg(his->kmgr.err, "invalid algorithm '%s', his_kms only support 'aes_256'.", key.algo);
        return NULL;
    }

    his_mgr_read_env(his);
    if (km_err_catch(his->kmgr.err)) {
        return NULL;
    }

    return his_kms_mk_select(his->kmsmgr, key.id);
}

static KmUnStr his_mk_encrypt(KeyMgr *kmgr, KeyInfo key, KmUnStr plain)
{
#define HIS_KMS_BLOCK_LEN 16
    HisMgr *his = (HisMgr *)(void *)kmgr;
    KmUnStr cipher = {0};

    if (plain.len % HIS_KMS_BLOCK_LEN != 0) {
        km_err_msg(his->kmgr.err, "the length of plain we sent to his_kms should be exactly divided by 16.");
        return cipher;
    }

    his_mgr_read_env(his);
    if (km_err_catch(his->kmgr.err)) {
        return cipher;
    }

    return his_kms_mk_encrypt(his->kmsmgr, key.id, plain);
}

static KmUnStr his_mk_decrypt(KeyMgr *kmgr, KeyInfo key, KmUnStr cipher)
{
    HisMgr *his = (HisMgr *)(void *)kmgr;
    KmUnStr plain = {0};

    his_mgr_read_env(his);
    if (km_err_catch(his->kmgr.err)) {
        return plain;
    }

    return his_kms_mk_decrypt(his->kmsmgr, key.id, cipher);
}

static KmUnStr his_dk_create(KeyMgr *kmgr, KeyInfo key, KmUnStr *cipher)
{
    HisMgr *his = (HisMgr *)(void *)kmgr;
    KmUnStr plain = {0};

    his_mgr_read_env(his);
    if (km_err_catch(his->kmgr.err)) {
        return plain;
    }

    return his_kms_dk_create(his->kmsmgr, key.id, cipher);
}

KeyMethod his_kms = {
    "his_kms",

    his_new, /* kmgr_new */
    his_free, /* kmgr_free */
    his_set_arg, /* kmgr_set_arg */

    NULL, /* mk_create */
    NULL, /* mk_delete */
    his_mk_select, /* mk_select */
    his_mk_encrypt, /* mk_encrypt */
    his_mk_decrypt, /* mk_decrypt */

    his_dk_create, /* dk_create */
};