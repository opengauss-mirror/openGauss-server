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
 * security_hwc_iam.h
 *
 * IDENTIFICATION
 *	 src/gausskernel/security/keymgr/include/hwc/security_hwc_iam.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __HWC_IAM_H_
#define __HWC_IAM_H_

#include "keymgr/comm/security_http.h"

typedef struct {
    char *url;
    char *username;
    char *passwd;
    char *domain;
    char *project;

    HttpMgr *httpmgr;
    char *cacert;

    char *reqbody;
    char *token;

    KmErr *err;
} HwcIamMgr;

HwcIamMgr *hwc_iam_new(KmErr *errbuf);
void hwc_iam_free(HwcIamMgr *iam);
void hwc_iam_set_arg(HwcIamMgr *iam, const char *key, const char *value);

char *hwc_iam_get_token(HwcIamMgr *iam);
char *hwc_iam_refresh_token(HwcIamMgr *iam);

#endif