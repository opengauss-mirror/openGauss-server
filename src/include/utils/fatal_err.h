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
 * ---------------------------------------------------------------------------------------
 *
 * fatal_err.h
 *
 * IDENTIFICATION
 *        src/include/utils/fatal_err.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef FATAL_ERR_H
#define FATAL_ERR_H

#include <signal.h>
#include <ucontext.h>

extern bool gen_err_msg(int sig, siginfo_t *si, ucontext_t *uc);
extern void output(int fd, const char *fmt, ...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

#endif
