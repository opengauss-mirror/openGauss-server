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
 * bbox_types.h
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/bbox/bbox_types.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef BBOX_TYPES_H
#define BBOX_TYPES_H

#include <signal.h>

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#define RET_OK (0)
#define RET_ERR (-1)

typedef signed char s8;
typedef unsigned char u8;

typedef signed short s16;
typedef unsigned short u16;

typedef signed int s32;
typedef unsigned int u32;

typedef signed long long s64;
typedef unsigned long long u64;

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif
