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
 * pqsignal.cpp
 *	  reliable BSD-style signal(2) routine stolen from RWW who stole it
 *	  from Stevens...
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/pqsignal.cpp
 *
 * NOTES
 *		This shouldn't be in libpq, but the monitor and some other
 *		things need it...
 *
 * -------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <signal.h>

#include "pqsignal.h"

pqsigfunc pqsignal(int signo, pqsigfunc func)
{
#if !defined(HAVE_POSIX_SIGNALS)
    return signal(signo, func);
#else
    struct sigaction act, oact;

    act.sa_handler = func;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    if (signo != SIGALRM)
        act.sa_flags |= SA_RESTART;
#ifdef SA_NOCLDSTOP
    if (signo == SIGCHLD)
        act.sa_flags |= SA_NOCLDSTOP;
#endif
    if (sigaction(signo, &act, &oact) < 0)
        return SIG_ERR;
    return oact.sa_handler;
#endif /* !HAVE_POSIX_SIGNALS */
}
