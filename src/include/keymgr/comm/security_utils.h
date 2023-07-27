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
 * security_utils.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/comm/security_utils.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __KM_UTILS_H_
#define __KM_UTILS_H_

#include <stdlib.h>
#include "securec.h"
#include "securec_check.h"
#include "keymgr/comm/security_error.h"

/* string utils */
typedef struct {
    unsigned char *val;
    size_t len;
} KmUnStr;

typedef struct {
    char *val;
    size_t len;
} KmStr;

char *km_str_strip(char *str);
char *km_env_get(const char *env);
int km_str_start_with(const char *str, const char *start);

/* array utils */
#define ARR_LEN(arr) ((int)sizeof((arr)) / sizeof((arr)[0]))

/* key-value utils */
#define KV_BUF_SZ 3072


typedef struct {
    const char *input;
    int inlen;
    int curpos; /* current scan position */
    int kvcnt;

    char key[KV_BUF_SZ];
    char value[KV_BUF_SZ];
} KvScan;

KvScan *kv_scan_init(const char *input);
void kv_scan_drop(KvScan *scan);
int kv_scan_exec(KvScan *scan);
char *km_realpath(const char *path, KmErr *err);
const char *km_str_spilt(const char *data, char *buf, size_t bufsz);

#endif