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
 */

/* -------------------------------------------------------------------------
 *
 * gethostname.cpp
 *	  gethostname using uname
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/common/port/gethostname.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "c.h"

#include <sys/utsname.h>

int gethostname(char* name, int namelen)
{
    static struct utsname mname;
    static int called = 0;
    errno_t ss_rc = 0;
    if (!called) {
        called++;
        uname(&mname);
    }
    ss_rc = strncpy_s(name, namelen, mname.nodename, ((SYS_NMLN < namelen) ? SYS_NMLN : namelen));
    securec_check_c(ss_rc, "\0", "\0");
    return 0;
}
