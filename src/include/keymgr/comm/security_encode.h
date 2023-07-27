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
 * security_encode.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/comm/security_encode.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef _KM_ENCODE_H_
#define _KM_ENCODE_H_

#include <stdlib.h>
#include "keymgr/comm/security_utils.h"

KmStr km_hex_encode(KmUnStr str);
KmUnStr km_hex_decode(KmStr hex);
KmStr km_sha_encode(KmUnStr str);
KmUnStr km_sha_decode(KmStr shahex);

size_t km_url_encode(const char *in, char *out, size_t outsz);
size_t km_hex_sha(const char *data, size_t datalen, char *hexbuf, size_t bufsz);

#endif