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
 * security_error.h
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/include/comm/security_error.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef _KM_ERROR_H_
#define _KM_ERROR_H_

#include <stdlib.h>
#include <string.h>
#include "securec.h"
#include "securec_check.h"

#ifdef FRONTEND /* used by libpq */
#define km_securec_check_ss securec_check_ss_c
#define km_securec_check securec_check_c

#define km_alloc malloc
#define km_alloc_zero(sz) calloc(1, (sz))
#define km_cxt_alloc_zero(cxt, sz) km_alloc_zero((sz))
#define km_strdup(str) ((str) == NULL ? NULL : strdup((str)))
#define km_strndup strndup
#define km_safe_free(ptr)    \
    do {                     \
        if ((ptr) != NULL) { \
            free((ptr));     \
            (ptr) = NULL;    \
        }                    \
    } while (0)
#define km_free free

#else /* used by kernel */
#include "utils/palloc.h"
#define km_securec_check_ss securec_check_ss
#define km_securec_check securec_check

#define km_alloc palloc
#define km_alloc_zero(sz) palloc0((sz))
void *km_cxt_alloc_zero(MemoryContext cxt, size_t size);
#define km_strdup pstrdup
#define km_strndup pnstrdup
#define km_safe_free FREE_POINTER
#define km_free pfree
#endif /* end */

#define KM_EREASE_DATA(buf, bufsz, datasz)                        \
    do {                                                          \
        if ((buf) != NULL) {                                      \
            errno_t _grc = memset_s((buf), (bufsz), 0, (datasz)); \
            km_securec_check(_grc, "", "");                       \
        }                                                         \
    } while (0)

typedef enum {
    KM_ERR_INIT = 0,
    KM_ERR_MEMORY,
} KmErrCode;

typedef struct {
    char *buf;
    size_t sz; /* max buffer len */
    KmErrCode code;

#ifdef FRONTEND
    char cxt; /* not used */
#else
    MemoryContext cxt;
#endif
} KmErr;

#define km_err_code(err, code) ((err)->code = (code))
#define km_err_msg(err, fmt, ...)                                         \
    do {                                                                  \
        errno_t grc;                                                      \
        if ((err) != NULL && (err)->buf != NULL) {                        \
            grc = sprintf_s((err)->buf, (err)->sz, (fmt), ##__VA_ARGS__); \
            km_securec_check_ss(grc, "", "");                             \
        }                                                                 \
    } while (0)
#define km_err_reset(err)                                    \
    do {                                                     \
        errno_t grc;                                         \
        grc = memset_s((err)->buf, (err)->sz, 0, (err)->sz); \
        km_securec_check(grc, "", "");                       \
        (err)->code = KM_ERR_INIT;                           \
    } while (0)
#define km_err_get_msg(err) (err)->buf
#define km_err_catch(err) (strlen((err)->buf) > 0 || (err)->code != KM_ERR_INIT)

KmErr *km_err_new(size_t sz);
void km_err_free(KmErr *err);

#endif