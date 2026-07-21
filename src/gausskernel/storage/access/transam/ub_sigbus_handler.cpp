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
#include "access/ub_sigbus_handler.h"

#if defined(__aarch64__)
#define SIGBUS_JMP_POINT 1

thread_local sigjmp_buf jump_env;

static void sigbus_handler(int sig, siginfo_t *si, void *ctx)
{
    siglongjmp(jump_env, SIGBUS_JMP_POINT);
}

int register_sigbus_handler(void)
{
    struct sigaction sa = { 0 };

    sa.sa_flags = SA_SIGINFO;
    sa.sa_sigaction = sigbus_handler;

    if (sigaction(SIGBUS, &sa, NULL) == -1) {
        ereport(FATAL, (errmsg("[SIGBUS] register handler for SIGBUS failed!!!")));
        return -1;
    }

    ereport(LOG, (errmsg("[SIGBUS] register handler for SIGBUS success!!!")));
    return 0;
}

extern "C" {
__attribute__((noinline)) void execute_esb_with_fault_handler(void)
{
    extern thread_local sigjmp_buf jump_env;

    if (sigsetjmp(jump_env, 1) == 0) {
#if defined(__ARM_FEATURE_ESB)
        asm volatile("esb" ::: "memory");
#endif
    } else {
        ereport(LOG, (errmsg("[SIGBUS] barrier fault handler success!!!")));
    }
}
}
#endif