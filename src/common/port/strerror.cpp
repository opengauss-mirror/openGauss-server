/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */

/* src/port/strerror.c */

/*
 * strerror - map error number to descriptive string
 *
 * This version is obviously somewhat Unix-specific.
 *
 * based on code by Henry Spencer
 * modified for ANSI by D'Arcy J.M. Cain
 */

#include "c.h"
#include "securec.h"

extern const char* const sys_errlist[];
extern int sys_nerr;

const char* strerror(int errnum)
{
    if (errnum < 0 || errnum > sys_nerr) {
        errno_t rc = sprintf_s(t_thrd.port_cxt.buf, sizeof(t_thrd.port_cxt.buf), _("unrecognized error %d"), errnum);
        securec_check_ss_c(rc, "\0", "\0");
        return t_thrd.port_cxt.buf;
    }

    return sys_errlist[errnum];
}
