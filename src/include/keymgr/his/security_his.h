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
 * security_his.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/his/security_his.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __HIS_H_
#define __HIS_H_

#include "keymgr/security_key_mgr.h"
#include "keymgr/his/security_his_iam.h"
#include "keymgr/his/security_his_kms.h"

typedef struct {
    KeyMgr kmgr;

    HisIamMgr *iammgr;
    HisKmsMgr *kmsmgr;

    int setarg;
} HisMgr;

HisMgr *his_mgr_new(KmErr *err);
void his_mgr_free(HisMgr *his);
void his_mgr_set_arg(HisMgr *his, const char *key, const char *value);

#define his_mgr_get_err(his) km_err_get_msg(((HisMgr *)(his))->kmgr.err)
#define his_mgr_reset_err(his) km_err_reset((his)->kmgr.err)

extern KeyMethod his_kms;

#endif