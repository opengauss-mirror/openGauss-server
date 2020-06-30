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
 * gs_strerror.cpp
 *
 *    Prototypes and macros around system calls, used to help make
 *    threaded libraries reentrant and safe to use system.
 *
 * IDENTIFICATION
 *    src/common/port/gs_strerror.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "gs_threadlocal.h"

#include <string.h>

#ifndef ERROR_LIMIT_LEN
#define ERROR_LIMIT_LEN 256
#endif

THR_LOCAL char gs_error_buf[ERROR_LIMIT_LEN];
#ifdef __sparc
int gs_strerror(int errnum)
#else
char* gs_strerror(int errnum)
#endif
{
#ifndef WIN32
    return strerror_r(errnum, gs_error_buf, ERROR_LIMIT_LEN);

#else
    strerror_s(gs_error_buf, errnum, ERROR_LIMIT_LEN);
    return gs_error_buf;
#endif
}
