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
 * matrix_mem.cpp
 *        routines to support RackMemory
 *
 *
 * IDENTIFICATION
 *        src/include/storage/matrix_mem/matrix_mem.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "dlfcn.h"
#include "securectype.h"
#include "knl/knl_instance.h"
#include "storage/matrix_mem.h"

int MaxtrixMemLoadSymbol(char *symbol, void **symLibHandle)
{
    const char *dlsymErr = NULL;
    *symLibHandle = dlsym(g_instance.matrix_mem_cxt.matrix_mem_func.handle, symbol);
    dlsymErr = dlerror();
    if (dlsymErr != NULL) {
        ereport(WARNING, (errmsg("matrix mem load symbol: %s, error: %s", symbol, dlsymErr)));
        return MATRIX_MEM_ERROR;
    }
    return MATRIX_MEM_SUCCESS;
}

int MaxtrixMemOpenDl(void **libHandle, char *symbol)
{
    *libHandle = dlopen(symbol, RTLD_LAZY);
    if (*libHandle == NULL) {
        ereport(WARNING, (errmsg("load matrix mem dynamic lib: %s, error: %s", symbol, dlerror())));
        return MATRIX_MEM_ERROR;
    }
    return MATRIX_MEM_SUCCESS;
}

void MatrixMemFuncInit()
{
    SymbolInfo symbols[] = {
        {"RackMemMalloc", (void **)&g_instance.matrix_mem_cxt.matrix_mem_func.rackMemMalloc},
        {"RackMemMallocAsync", (void **)&g_instance.matrix_mem_cxt.matrix_mem_func.rackMemMallocAsync},
        {"RackMemFree", (void **)&g_instance.matrix_mem_cxt.matrix_mem_func.rackMemFree},
        {"RackMemFreeAsync", (void **)&g_instance.matrix_mem_cxt.matrix_mem_func.rackMemFreeAsync}
    };

    struct stat st;
    if (lstat((const char*)g_instance.attr.attr_storage.lmemfabric_client_path, &st) == -1) {
        ereport(WARNING, (errmsg("load matrix mem dynamic lib error: %s, lib not exists",
            g_instance.attr.attr_storage.lmemfabric_client_path)));
        return;
    }

    if (SECUREC_UNLIKELY(MaxtrixMemOpenDl(&g_instance.matrix_mem_cxt.matrix_mem_func.handle,
        g_instance.attr.attr_storage.lmemfabric_client_path) != MATRIX_MEM_SUCCESS)) {
        return;
    }

    size_t numSymbols = sizeof(symbols) / sizeof(symbols[0]);
    for (size_t i = 0; i < numSymbols; i++) {
        if (SECUREC_UNLIKELY(MaxtrixMemLoadSymbol(symbols[i].symbolName, symbols[i].funcPtr) != MATRIX_MEM_SUCCESS)) {
            return ;
        }
    }

    /* succeeded to load */
    g_instance.matrix_mem_cxt.matrix_mem_inited = true;
}

void MatrixMemFuncUnInit()
{
    if (g_instance.matrix_mem_cxt.matrix_mem_inited) {
        (void)dlclose(g_instance.matrix_mem_cxt.matrix_mem_func.handle);
        g_instance.matrix_mem_cxt.matrix_mem_func.handle = NULL;
        g_instance.matrix_mem_cxt.matrix_mem_inited = false;
    }
}

void *RackMemMalloc(size_t size, PerfLevel perfLevel, intptr_t attr)
{
    return g_instance.matrix_mem_cxt.matrix_mem_func.rackMemMalloc(size, perfLevel, attr);
}
void *RackMemMallocAsync(size_t size, PerfLevel perfLevel, intptr_t attr, AsyncFreeCallBack func, intptr_t ctx)
{
    return g_instance.matrix_mem_cxt.matrix_mem_func.rackMemMallocAsync(size, perfLevel, attr, func, ctx);
}
void RackMemFree(void *ptr)
{
    return g_instance.matrix_mem_cxt.matrix_mem_func.rackMemFree(ptr);
}

int RackMemFreeAsync(void *ptr, AsyncFreeCallBack func, intptr_t ctx)
{
    return g_instance.matrix_mem_cxt.matrix_mem_func.rackMemFreeAsync(ptr, func, ctx);
}