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
 * security_utils.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/comm/security_utils.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "keymgr/comm/security_utils.h"
#include <string.h>
#include <stdio.h>
#include <pthread.h>
#include <limits.h>
#include <sys/stat.h>
#include "keymgr/comm/security_error.h"

char *km_str_strip(char *str)
{
    if (str == NULL || strlen(str) == 0) {
        return NULL;
    }
    
    while (strlen(str) > 1) {
        if (str[0] == ' ') {
            str++;
        } else {
            break;
        }
    }

    for (size_t i = 0; i < strlen(str); i++) {
        if (str[i] == ' ') {
            str[i] = '\0';
            break;
        }
    }

    return str;
}

char *km_env_get(const char *env)
{
    static pthread_mutex_t envlock = PTHREAD_MUTEX_INITIALIZER;
    char *value;

    if (env == NULL) {
        return NULL;
    }

    (void)pthread_mutex_lock(&envlock);
    value = getenv(env);
    (void)pthread_mutex_unlock(&envlock);

    return value;
}

int km_str_start_with(const char *str, const char *start)
{
    if (str == NULL || strlen(str) == 0) {
        return -1;
    }

    while (strlen(str) > 1) {
        if (str[0] == ' ') {
            str++;
        } else {
            break;
        }
    }

    return strncasecmp(str, start, strlen(start));
}

KvScan *kv_scan_init(const char *input)
{
    KvScan *scan;
    
    if (input == NULL || strlen(input) == 0) {
        return NULL;
    }

    if (strlen(input) > KV_BUF_SZ) {
        return NULL;
    }

    scan = (KvScan *)km_alloc_zero(sizeof(KvScan));
    if (scan == NULL) {
        return NULL;
    }

    scan->input = input;
    scan->inlen = (int)strlen(input);

    return scan;
}

void kv_scan_drop(KvScan *scan)
{
    if (scan != NULL) {
        km_free(scan);
    }
}

/* the kmsargs format should be 'key=value, key=value, ...' */
int kv_scan_exec(KvScan *scan)
{
    bool isfind;
    int start;
    int curpos;
    char curchr;
    errno_t rc;

    if (scan == NULL) {
        return -1;
    }

    /* reset key & value buffer */
    if (scan->curpos > 0) {
        rc = memset_s(scan->key, KV_BUF_SZ, 0, KV_BUF_SZ);
        km_securec_check(rc, "", "");
        rc = memset_s(scan->value, KV_BUF_SZ, 0, KV_BUF_SZ);
        km_securec_check(rc, "", "");
    }

    isfind = false;
    start = scan->curpos;

    for (curpos = scan->curpos; curpos < scan->inlen; curpos++) {
        curchr = scan->input[curpos];
        if (curchr == '=') {
            rc = strncpy_s(scan->key, KV_BUF_SZ, scan->input + start, curpos - start);
            start = curpos + 1;
        } else if (curchr == ',') {
            rc = strncpy_s(scan->value, KV_BUF_SZ, scan->input + start, curpos - start);
            start = curpos + 1;
            isfind = true;
        } else if (curpos == scan->inlen - 1) { /* scan is complete */
            rc = strcpy_s(scan->value, KV_BUF_SZ, scan->input + start);
            isfind = true;
        } else {
            continue;
        }
        km_securec_check(rc, "", "");

        if (isfind) {
            break;
        }
    }

    scan->curpos = curpos + 1;
    if (strlen(scan->key) == 0 || strlen(scan->value) == 0) {
        return -1;
    }

    if (scan->curpos >= scan->inlen - 1) {
        return 0;
    }

    return ++scan->kvcnt;
}

char *km_realpath(const char *path, KmErr *err)
{
    char *rpath;
    struct stat statbuf;
    const char dangerchar[] = {'|', ';', '&', '$', '<', '>', '`', '\\', '\'', '\"', '{', '}', '(', ')', '[', ']', '~',
        '*', '?', '!'};

    if (path == NULL || strlen(path) == 0) {
        return NULL;
    }

    rpath = (char *)malloc(PATH_MAX);
    if (rpath == NULL) {
        km_err_msg(err, "key manager failed to malloc memory.");
        return NULL;
    }

    if (realpath(path, rpath) == NULL) {
        km_err_msg(err, "the path '%s' is not exists.", path);
        km_free(rpath);
        return NULL;
    }

    if (lstat(rpath, &statbuf) >= 0) {
        if (!S_ISREG(statbuf.st_mode) || statbuf.st_mode & (S_IRWXG | S_IRWXO) ||
            (statbuf.st_mode & S_IRWXU) == S_IRWXU) {
            km_err_msg(err, "the permisson of '%s' should be 0600(u=rw) or less.", path);
            km_free(rpath);
            return NULL;
        }
    }

    for (size_t i = 0; i < ARR_LEN(dangerchar); i++) {
        if (strchr(rpath, dangerchar[i]) != NULL) {
            km_err_msg(err, "the path '%s' contain invalid character '%c',", path, dangerchar[i]);
            km_free(rpath);
            return NULL;
        }
    }

    return rpath;
}

const char *km_str_spilt(const char *data, char *buf, size_t bufsz)
{
    const char *end;
    size_t bufuse = 0;

    if (data == NULL) {
        return NULL;
    }

    for (end = data; end[0] != '\0' && end[0] == ' '; end++) {}; /* skip whitespace at the head */
    for (; end[0] != '\0'; end++) {
        if (end[0] == ',' || (bufuse == bufsz - 1)) {
            break;
        }
        buf[bufuse++] = end[0];
    }
    for (; bufuse > 0 && buf[bufuse - 1] == ' '; bufuse--) {}; /* skip whitespace at the tail  */
    buf[bufuse] = '\0';

    return end[0] == ',' ? end + 1 : NULL;
}