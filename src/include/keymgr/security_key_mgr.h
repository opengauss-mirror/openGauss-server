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
 * security_key_mgr.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/keymgr/security_key_mgr.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef _KM_KEY_MGR_H_
#define _KM_KEY_MGR_H_

#include "keymgr/comm/security_error.h"
#include "keymgr/comm/security_utils.h"

/* KeyInfo */
typedef struct {
    const char *type;
    const char *id;
    const char *algo;
} KeyInfo;

/* KeyMgr */
typedef struct {
    KmErr *err;

    /* special space */
} KeyMgr;

/* KeyMgrMethod */
typedef KeyMgr* (*KeyMgrNew)(KmErr *err);
typedef void (*KeyMgrFree)(KeyMgr *kmgr);
typedef void (*KeyMgrSetArg)(KeyMgr *kmgr, const char *key, const char *value);

/* KeyMethod */
typedef void (*MasterKeyCreate)(KeyMgr *kmgr, KeyInfo info);
typedef void (*MasterKeyDelete)(KeyMgr *kmgr, KeyInfo info);
typedef KmUnStr (*MasterKeyEncrypt)(KeyMgr *kmgr, KeyInfo info, KmUnStr plain);
typedef KmUnStr (*MasterKeyDecrypt)(KeyMgr *kmgr, KeyInfo info, KmUnStr cipher);
typedef char* (*MasterKeySelect)(KeyMgr *kmgr, KeyInfo info);

typedef KmUnStr (*DataKeyCreate)(KeyMgr *kmgr, KeyInfo info, KmUnStr *cipher);

typedef struct {
    const char *type;

    /* key mgr method */
    KeyMgrNew kmgr_new;
    KeyMgrFree kmgr_free;
    KeyMgrSetArg kmgr_set_arg;

    /* key method */
    MasterKeyCreate mk_create;
    MasterKeyDelete mk_delete;
    MasterKeySelect mk_select;
    MasterKeyEncrypt mk_enrypt;
    MasterKeyDecrypt mk_decrypt;

    DataKeyCreate dk_create;
} KeyMethod;

#endif

