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
 * roach_api.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/bulkload/roach_api.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef __ROACH_API_H__
#define __ROACH_API_H__

#include "nodes/nodes.h"

#ifndef __cplusplus

#ifndef bool
typedef char bool;
#endif

#ifndef true
#define true((bool)1)
#endif

#ifndef false
#define false((bool)0)
#endif

#endif /* __cplusplus */

typedef void* (*OpenFunc)(char* url, char* mode);
typedef size_t (*WriteFunc)(void* buf, size_t size, size_t len, void* roach_context, bool complete_line);
typedef size_t (*ReadFunc)(void* buf, size_t size, size_t len, void* roach_context);
typedef int (*CloseFunc)(void* roach_context);
typedef int (*ErrorFunc)(void* roach_context);

typedef struct RoachRoutine {
    NodeTag type;
    OpenFunc Open;
    WriteFunc Write;
    ReadFunc Read;
    CloseFunc Close;
    ErrorFunc Error;
} RoachRoutine;

#endif /*__ROACH_API_H__*/
