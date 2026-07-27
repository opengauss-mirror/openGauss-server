/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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
 * ---------------------------------------------------------------------------------------
 * ub_sigbus_handler.cpp
 * SIGBUS signal handler implementation for UB transaction cache
 *
 * src/gausskernel/storage/access/transam/ub_sigbus_handler.cpp
 * ---------------------------------------------------------------------------------------
 */

#include <setjmp.h>
#include <unistd.h>
#include <signal.h>
#include "utils/elog.h"
#include "securec.h"
#include "access/ub_sigbus_handler.h"

thread_local sigjmp_buf jump_env;
thread_local volatile sig_atomic_t ub_sigbus_jump_active = 0;

#if defined(__aarch64__)
#define SIGBUS_JMP_POINT 1

static struct sigaction g_previous_sigbus_action;
static volatile sig_atomic_t g_previous_sigbus_action_valid = 0;

static void sigbus_handler(int sig, siginfo_t *si, void *ctx)
{
    (void)sig;
    (void)si;
    (void)ctx;

    if (ub_sigbus_jump_active) {
        siglongjmp(jump_env, SIGBUS_JMP_POINT);
    }

    /*
     * SIGBUS occurred outside UB protected region.
     * Restore the previous handler (e.g. BBOX handler) and re-raise,
     * so BBOX can generate a proper core dump.
     */
    if (g_previous_sigbus_action_valid) {
        (void)sigaction(SIGBUS, &g_previous_sigbus_action, NULL);
    } else {
        struct sigaction dfl;
        errno_t rc = memset_s(&dfl, sizeof(dfl), 0, sizeof(dfl));
        securec_check_c(rc, "\0", "\0");
        dfl.sa_handler = SIG_DFL;
        (void)sigemptyset(&dfl.sa_mask);
        dfl.sa_flags = 0;
        (void)sigaction(SIGBUS, &dfl, NULL);
    }

    (void)raise(SIGBUS);
}

int register_sigbus_handler(void)
{
    struct sigaction sa;
    struct sigaction old_sa;

    errno_t rc = memset_s(&sa, sizeof(sa), 0, sizeof(sa));
    securec_check_c(rc, "\0", "\0");

    sa.sa_flags = SA_SIGINFO;
    sa.sa_sigaction = sigbus_handler;
    (void)sigemptyset(&sa.sa_mask);

    /* Save the previous handler so we can chain to it on non-UB faults */
    if (sigaction(SIGBUS, NULL, &old_sa) == -1) {
        ereport(FATAL, (errmsg("[SIGBUS] get old handler for SIGBUS failed!!!")));
        return -1;
    }

    /* Avoid saving ourself as the previous handler on re-registration */
    if (!((old_sa.sa_flags & SA_SIGINFO) && old_sa.sa_sigaction == sigbus_handler)) {
        g_previous_sigbus_action = old_sa;
        g_previous_sigbus_action_valid = 1;
    }

    if (sigaction(SIGBUS, &sa, NULL) == -1) {
        ereport(FATAL, (errmsg("[SIGBUS] register handler for SIGBUS failed!!!")));
        return -1;
    }

    ereport(LOG, (errmsg("[SIGBUS] register handler for SIGBUS success!!!")));
    return 0;
}
#endif