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
#include <signal.h>
extern thread_local sigjmp_buf jump_env;
extern thread_local volatile sig_atomic_t ub_sigbus_jump_active;

#if defined(__aarch64__)
extern int register_sigbus_handler(void);

#define UB_ESB_BARRIER()    asm volatile("esb" ::: "memory")
#else
#define UB_ESB_BARRIER()  ((void)0)
#endif
#endif /* UB_SIGBUS_HANDLER_H */