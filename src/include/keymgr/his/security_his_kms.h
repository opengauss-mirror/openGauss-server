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
 * security_his_kms.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/his/security_his_kms.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __HIS_KMS_H_
#define __HIS_KMS_H_

#include "keymgr/comm/security_http.h"
#include "keymgr/comm/security_utils.h"
#include "keymgr/his/security_his_iam.h"

#define KMS_URL_BUF_SZ 256

typedef struct {
    char *appid;
    /* default: his-op */
    char *domain;
    char *kmsurl;

    HisIamMgr *iam;

    char url[KMS_URL_BUF_SZ];
    char *reqbody;

    char *cacert;
    HttpMgr *httpmgr;

    KmErr *err;
} HisKmsMgr;

HisKmsMgr *his_kms_new(HisIamMgr *iam, KmErr *errbuf);
void his_kms_free(HisKmsMgr *kms);
void his_kms_set_arg(HisKmsMgr *kms, const char *key, const char *value);

char *his_kms_mk_select(HisKmsMgr *kms, const char *keypath);
KmUnStr his_kms_mk_encrypt(HisKmsMgr *kms, const char *keypath, KmUnStr plain);
KmUnStr his_kms_mk_decrypt(HisKmsMgr *kms, const char *keypath, KmUnStr cipher);

KmUnStr his_kms_dk_create(HisKmsMgr *kms, const char *keypath, KmUnStr *cipher);
#endif