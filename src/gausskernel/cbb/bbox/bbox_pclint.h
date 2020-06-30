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
 * bbox_pclint.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_pclint.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef BBOX_PCLINT_H
#define BBOX_PCLINT_H

#include <stdarg.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#ifdef BBOX_PCLINT

#define __thread                   /* keyword of 'thread local' which pclint not support */
#define __builtin_expect(x, y) (x) /* keyword of compiler which pclint not support */

#define va_start(ap, size)
#define va_end(ap)

#define va_arg(ap, type)

#define WIFEXITED(status) (((status)&0x7f) == 0)

#define WEXITSTATUS(status) ((unsigned int)((status)&0xff00) >> 8)

extern void* __builtin_return_address(u32 uiDepth);

extern void* __builtin_frame_address(u32 uiDepth);

extern void* __builtin_alloca(size_t size);

#endif

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif
