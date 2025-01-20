/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * matrix_mem.h
 *        routines to support RackMemory
 *
 *
 * IDENTIFICATION
 *        src/include/storage/matrix_mem.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef MATRIX_MEM_H
#define MATRIX_MEM_H

#include "storage/rack_mem.h"

#ifdef __aarch64__
#define ENABLE_RACK_MEM (g_instance.matrix_mem_cxt.matrix_mem_inited)
#else
#define ENABLE_RACK_MEM false
#endif

constexpr auto MATRIX_MEM_SUCCESS = 0;
constexpr auto MATRIX_MEM_ERROR = -1;

typedef struct SymbolInfo {
    char *symbolName;
    void **funcPtr;
} SymbolInfo;

typedef struct MatrixMemFunc {
    void *handle;
    void* (*rackMemMalloc)(size_t size, PerfLevel perfLevel, intptr_t attr);
    void* (*rackMemMallocAsync)(size_t size, PerfLevel perfLevel, intptr_t attr, AsyncFreeCallBack func, intptr_t ctx);
    void (*rackMemFree)(void *ptr);
    int (*rackMemFreeAsync)(void *ptr, AsyncFreeCallBack func, intptr_t ctx);
} MatrixMemFunc;

extern void MatrixMemFuncInit();

extern void MatrixMemFuncUnInit();

#endif // MATRIX_MEM_H
