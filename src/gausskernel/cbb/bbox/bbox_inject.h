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
 * bbox_inject.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_inject.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __BBOX_INJECT_H_
#define __BBOX_INJECT_H_

#include "bbox_types.h"

/* the process injected attach response in code */
s32 bbox_inject_attach(pid_t pid);

/* the process injected dettach response in code */
s32 bbox_inject_dettach(pid_t pid);

/* function parameter type */
typedef enum arg_type {
    ARG_TYPE_LONG = 1, /* integer */
    ARG_TYPE_STRING,   /* string */
} arg_type;

/* function parameter */
typedef struct bbox_arg {
    arg_type type;
    union {
        unsigned long data;
        char* string;
    };
} bbox_arg;

/* loadd the specified dynamic library */
s32 bbox_load_so(pid_t pid, char* soname);

#if defined(__aarch64__)
int ptrace_arm64_getregs(pid_t pid, struct user_pt_regs* regs);
int ptrace_arm64_setregs(pid_t pid, struct user_pt_regs* regs);
#endif

#endif
