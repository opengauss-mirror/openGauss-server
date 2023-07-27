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
 * security_his_iam.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/include/his/security_his_iam.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __HIS_IAM_H_
#define __HIS_IAM_H_

#include "keymgr/comm/security_http.h"

typedef struct {
    char *url;
    char *account;
    char *secret;
    char *appid;
    char *enterprise;

    char *cacert;
    HttpMgr *httpmgr;

    char *reqbody;
    char *token;

    KmErr *err;
} HisIamMgr;

HisIamMgr *his_iam_new(KmErr *errbuf);
void his_iam_free(HisIamMgr *iam);

void his_iam_set_arg(HisIamMgr *iam, const char *key, const char *value);
char *his_iam_get_token(HisIamMgr *iam);
char *his_iam_refresh_token(HisIamMgr *iam);

#endif