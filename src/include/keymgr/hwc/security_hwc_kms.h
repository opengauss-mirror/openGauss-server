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
 * security_hwc_kms.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/hwc/security_hwc_kms.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __HWC_KMS_H_
#define __HWC_KMS_H_

#include "keymgr/hwc/security_hwc_iam.h"
#include "keymgr/comm/security_http.h"
#include "keymgr/comm/security_utils.h"

#define HWC_KMS_URL_SZ 256
#define HWC_PROJ_ID_HDR_SZ 128

typedef struct {
    bool aksk;
    HwcIamMgr *iam;

    char url[HWC_KMS_URL_SZ];
    char projid[HWC_PROJ_ID_HDR_SZ]; /* format: 'x-project-id: $kmsProjectId' */
    char *reqbody;
    HttpMgr *httpmgr;
    char *cacert;

    KmErr *err;
} HwcKmsMgr;

HwcKmsMgr *hwc_kms_new(HwcIamMgr *iam, KmErr *errbuf);
void hwc_kms_free(HwcKmsMgr *kms);
void hwc_kms_set_arg(HwcKmsMgr *kms, const char *key, const char *value);

char *hwc_kms_mk_select(HwcKmsMgr *kms, const char *keypath);
KmUnStr hwc_kms_mk_encrypt(HwcKmsMgr *kms, const char *keypath, KmUnStr plain);
KmUnStr hwc_kms_mk_decrypt(HwcKmsMgr *kms, const char *keypath, KmUnStr cipher);
KmUnStr hwc_kms_dk_create(HwcKmsMgr *kms, const char *keypath, KmUnStr *cipher);
#endif