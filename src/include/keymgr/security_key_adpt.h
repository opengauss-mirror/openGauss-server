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
 * security_key_adpt.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/keymgr/security_key_adpt.cpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef _KM_KEY_ADPT_H_
#define _KM_KEY_ADPT_H_

#include "keymgr/security_key_mgr.h"

#define MAX_KEY_MGR_NUM 20

typedef struct {
    KmErr *err;

    KeyMethod *methods[MAX_KEY_MGR_NUM];
    KeyMgr *mgrs[MAX_KEY_MGR_NUM];
} KeyAdpt;

KeyAdpt *key_adpt_new();
void key_adpt_free(KeyAdpt *kadpt);
void key_adpt_set_arg(KeyAdpt *kadpt, const char *type, const char *key, const char *value);
void key_adpt_set_args(KeyAdpt *kadpt, const char *type, const char *args);

void key_adpt_mk_create(KeyAdpt *kadpt, KeyInfo info);
void key_adpt_mk_delete(KeyAdpt *kadpt, KeyInfo info);
char *key_adpt_mk_select(KeyAdpt *kadpt, KeyInfo info);
KmUnStr key_adpt_mk_encrypt(KeyAdpt *kadpt, KeyInfo info, KmUnStr plain);
KmUnStr key_adpt_mk_decrypt(KeyAdpt *kadpt, KeyInfo info, KmUnStr cipher);

KmUnStr key_adpt_dk_create(KeyAdpt *kadpt, KeyInfo info, KmUnStr *cipher);

bool key_adpt_catch_err(KeyAdpt *kadpt);
const char *key_adpt_get_err(KeyAdpt *kadpt);

#endif
