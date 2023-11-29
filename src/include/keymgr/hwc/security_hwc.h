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
 * security_hwc.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/hwc/security_hwc.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __HWC_H_
#define __HWC_H_

#include "keymgr/security_key_mgr.h"
#include "keymgr/hwc/security_hwc_iam.h"
#include "keymgr/hwc/security_hwc_kms.h"

typedef struct {
    KeyMgr kmgr;

    HwcIamMgr *iammgr;
    HwcKmsMgr *kmsmgr;

    int setarg;
} HwcMgr;

HwcMgr *hwc_mgr_new(KmErr *err);
void hwc_mgr_free(HwcMgr *hwc);
void hwc_mgr_set_arg(HwcMgr *hwc, const char *key, const char *value);

extern KeyMethod huawei_kms;
extern KeyMethod hcs_kms;

#endif