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

MatrixMemFunc g_matrixMemFunc = {0};

int MaxtrixMemLoadSymbol(char *symbol, void **symLibHandle)
{
    const char *dlsymErr = NULL;
    *symLibHandle = dlsym(g_matrixMemFunc.handle, symbol);
    dlsymErr = dlerror();
    if (dlsymErr != NULL) {
#ifdef FRONTEND
        fprintf(stderr, _("matrix mem load symbol: %s, error: %s"), symbol, dlsymErr);
#else
        ereport(WARNING, (errmsg("matrix mem load symbol: %s, error: %s", symbol, dlsymErr)));
#endif
        return MATRIX_MEM_ERROR;
    }
    return MATRIX_MEM_SUCCESS;
}

int MaxtrixMemOpenDl(void **libHandle, char *symbol)
{
    *libHandle = dlopen(symbol, RTLD_LAZY);
    if (*libHandle == NULL) {
#ifdef FRONTEND
        fprintf(stderr, _("load matrix mem dynamic lib: %s, error: %s"), symbol, dlerror());
#else
        ereport(WARNING, (errmsg("load matrix mem dynamic lib: %s, error: %s", symbol, dlerror())));
#endif
        return MATRIX_MEM_ERROR;
    }
    return MATRIX_MEM_SUCCESS;
}

void MatrixMemFuncInit(char* lmemfabricClientPath)
{
    SymbolInfo symbols[] = {
        {"RackMemMalloc", (void **)&g_matrixMemFunc.rackMemMalloc},
        {"RackMemMallocAsync", (void **)&g_matrixMemFunc.rackMemMallocAsync},
        {"RackMemFree", (void **)&g_matrixMemFunc.rackMemFree},
        {"RackMemFreeAsync", (void **)&g_matrixMemFunc.rackMemFreeAsync},
        {"RackMemShmLookupShareRegions", (void **)&g_matrixMemFunc.rackMemShmLookupShareRegions},
        {"RackMemShmLookupRegionInfo", (void **)&g_matrixMemFunc.rackMemShmLookupRegionInfo},
        {"RackMemShmCreate", (void **)&g_matrixMemFunc.rackMemShmCreate},
        {"RackMemShmMmap", (void **)&g_matrixMemFunc.rackMemShmMmap},
        {"RackMemShmCacheOpt", (void **)&g_matrixMemFunc.rackMemShmCacheOpt},
        {"RackMemShmUnmmap", (void **)&g_matrixMemFunc.rackMemShmUnmmap},
        {"RackMemShmDelete", (void **)&g_matrixMemFunc.rackMemShmDelete}
    };

    struct stat st;
    if (lstat((const char*)lmemfabricClientPath, &st) == -1) {
#ifdef FRONTEND
        fprintf(stderr, _("load matrix mem dynamic lib error: %s, lib not exists"), lmemfabricClientPath);
#else
        ereport(WARNING, (errmsg("load matrix mem dynamic lib error: %s, lib not exists", lmemfabricClientPath)));
#endif
        return;
    }

    if (SECUREC_UNLIKELY(MaxtrixMemOpenDl(&g_matrixMemFunc.handle, lmemfabricClientPath) != MATRIX_MEM_SUCCESS)) {
        return;
    }

    size_t numSymbols = sizeof(symbols) / sizeof(symbols[0]);
    for (size_t i = 0; i < numSymbols; i++) {
        if (SECUREC_UNLIKELY(MaxtrixMemLoadSymbol(symbols[i].symbolName, symbols[i].funcPtr) != MATRIX_MEM_SUCCESS)) {
            return ;
        }
    }

    /* succeeded to load */
    g_matrixMemFunc.matrix_mem_inited = true;
}

void MatrixMemFuncUnInit()
{
    if (g_matrixMemFunc.matrix_mem_inited) {
        (void)dlclose(g_matrixMemFunc.handle);
        g_matrixMemFunc.handle = NULL;
        g_matrixMemFunc.matrix_mem_inited = false;
    }
}

void *RackMemMalloc(size_t size, PerfLevel perfLevel, intptr_t attr)
{
    return g_matrixMemFunc.rackMemMalloc(size, perfLevel, attr);
}
void *RackMemMallocAsync(size_t size, PerfLevel perfLevel, intptr_t attr, AsyncFreeCallBack func, intptr_t ctx)
{
    return g_matrixMemFunc.rackMemMallocAsync(size, perfLevel, attr, func, ctx);
}
void RackMemFree(void *ptr)
{
    return g_matrixMemFunc.rackMemFree(ptr);
}

int RackMemFreeAsync(void *ptr, AsyncFreeCallBack func, intptr_t ctx)
{
    return g_matrixMemFunc.rackMemFreeAsync(ptr, func, ctx);
}

int RackMemShmLookupShareRegions(const char *baseNid, ShmRegionType type, SHMRegions *regions)
{
    return g_matrixMemFunc.rackMemShmLookupShareRegions(baseNid, type, regions);
}

int RackMemShmLookupRegionInfo(SHMRegionDesc *region, SHMRegionInfo *info)
{
    return g_matrixMemFunc.rackMemShmLookupRegionInfo(region, info);
}

int RackMemShmCreate(char *name, uint64_t size, const char *baseNid, SHMRegionDesc *shmRegion)
{
    return g_matrixMemFunc.rackMemShmCreate(name, size, baseNid, shmRegion);
}

void *RackMemShmMmap(void *start, size_t length, int prot, int flags, const char *name, off_t offset)
{
    return g_matrixMemFunc.rackMemShmMmap(start, length, prot, flags, name, offset);
}

int RackMemShmCacheOpt(void *start, size_t length, ShmCacheOpt type)
{
    return g_matrixMemFunc.rackMemShmCacheOpt(start, length, type);
}

int RackMemShmUnmmap(void *start, size_t length)
{
    return g_matrixMemFunc.rackMemShmUnmmap(start, length);
}

int RackMemShmDelete(char *name)
{
    return g_matrixMemFunc.rackMemShmDelete(name);
}