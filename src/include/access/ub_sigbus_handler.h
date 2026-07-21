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
 * ub_sigbus_handler.h
 * SIGBUS signal handler for UB transaction cache
 *
 * src/include/access/ub_sigbus_handler.h
 * ---------------------------------------------------------------------------------------
 */

#ifndef UB_SIGBUS_HANDLER_H
#define UB_SIGBUS_HANDLER_H

#include <setjmp.h>
#if defined(__aarch64__)
extern thread_local sigjmp_buf jump_env;

extern int register_sigbus_handler(void);


extern "C" {
    void execute_esb_with_fault_handler(void);
}

#define EXECUTE_ESB()                           \
    do {                                        \
        if (UB_SIGBUS_HANDLER) {                \
            execute_esb_with_fault_handler();   \
        }                                       \
    } while (0)
#else
#define EXECUTE_ESB() ((void)0)
#endif

#endif /* UB_SIGBUS_HANDLER_H */