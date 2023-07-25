/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * security_localkms.h
 *      localkms is a lightweight key management component.
 *      different from other key management tools and services, localkms automatically generates CMKE when CREATE CMKO,
 *      it cannot manage key entities independently.
 *      localkms use openssl to generate cmk, then encrypt cmk plain and store cmk cipher in file.
 *      at the same time, we need to store/read the iv and salt that are used to derive a key to
 *      encrypt/decrypt cmk plain.
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/localkms/security_localkms.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __KM_LOCALKMS_H__
#define __KM_LOCALKMS_H__

#include "keymgr/security_key_mgr.h"

typedef struct {
    KeyMgr kmgr;

    int getpath;
} LocalKmsMgr;

extern KeyMethod localkms;

#endif /* REG_LOCALKMS_H */
