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
 * noblock.c
 *	  set a file descriptor as non-blocking
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/port/noblock.c
 *
 * -------------------------------------------------------------------------
 */

#include "c.h"

#include <fcntl.h>

bool pg_set_noblock(pgsocket sock)
{
#if !defined(WIN32)
    return (fcntl(sock, F_SETFL, O_NONBLOCK) != -1);
#else
    unsigned long ioctlsocket_ret = 1;

    /* Returns non-0 on failure, while fcntl() returns -1 on failure */
    return (ioctlsocket(sock, FIONBIO, &ioctlsocket_ret) == 0);
#endif
}

bool pg_set_block(pgsocket sock)
{
#if !defined(WIN32)
    int flags;

    flags = fcntl(sock, F_GETFL);
    if (flags < 0 || fcntl(sock, F_SETFL, (long)((unsigned int)flags & ~O_NONBLOCK))) {
        return false;
    }
    return true;
#else
    unsigned long ioctlsocket_ret = 0;

    /* Returns non-0 on failure, while fcntl() returns -1 on failure */
    return (ioctlsocket(sock, FIONBIO, &ioctlsocket_ret) == 0);
#endif
}
