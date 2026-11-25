/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * ---------------------------------------------------------------------------------------
 *
 * wd_agg_wrapper.cpp
 *        routines to support UADK hardware acceleration
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/vecexecutor/vecnode/wd_agg_wrapper.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "dlfcn.h"
#include "storage/wd_agg_wrapper.h"

UadkAggFunc g_uadkAggFunc = {0};

typedef struct SymbolInfo {
    char *symbolName;
    void **funcPtr;
} SymbolInfo;

static int UadkAggLoadSymbol(char *symbol, void **symLibHandle)
{
    const char *dlsymErr = nullptr;
    *symLibHandle = dlsym(g_uadkAggFunc.handle, symbol);
    dlsymErr = dlerror();
    if (dlsymErr != nullptr) {
        ereport(WARNING, (errmsg("UADK AGG load symbol: %s, error: %s", symbol, dlsymErr)));
        return UADK_AGG_ERROR;
    }
    return UADK_AGG_SUCCESS;
}

static int UadkAggOpenDl(void **libHandle, char *symbol)
{
    *libHandle = dlopen(symbol, RTLD_LAZY);
    if (*libHandle == nullptr) {
        ereport(WARNING, (errmsg("load UADK AGG dynamic lib for DPA: %s, error: %s", symbol, dlerror())));
        return UADK_AGG_ERROR;
    }
    return UADK_AGG_SUCCESS;
}

void UadkAggFuncInit(char* wdDaePath)
{
    SymbolInfo symbols[] = {
        {"wd_agg_init", (void **)&g_uadkAggFunc.wd_agg_init},
        {"wd_agg_uninit", (void **)&g_uadkAggFunc.wd_agg_uninit},
        {"wd_agg_alloc_sess", (void **)&g_uadkAggFunc.wd_agg_alloc_sess},
        {"wd_agg_free_sess", (void **)&g_uadkAggFunc.wd_agg_free_sess},
        {"wd_agg_set_hash_table", (void **)&g_uadkAggFunc.wd_agg_set_hash_table},
        {"wd_agg_add_input_sync", (void **)&g_uadkAggFunc.wd_agg_add_input_sync},
        {"wd_agg_get_output_sync", (void **)&g_uadkAggFunc.wd_agg_get_output_sync},
        {"wd_agg_get_table_rowsize", (void **)&g_uadkAggFunc.wd_agg_get_table_rowsize},
        {"wd_agg_rehash_sync", (void **)&g_uadkAggFunc.wd_agg_rehash_sync}
    };

    if (UadkAggOpenDl(&g_uadkAggFunc.handle, wdDaePath) != UADK_AGG_SUCCESS) {
        return;
    }

    size_t numSymbols = sizeof(symbols) / sizeof(symbols[0]);
    for (size_t i = 0; i < numSymbols; i++) {
        if (UadkAggLoadSymbol(symbols[i].symbolName, symbols[i].funcPtr) != UADK_AGG_SUCCESS) {
            return;
        }
    }

    int ret = g_uadkAggFunc.wd_agg_init((char *)"hashagg", 0, TASK_HW, nullptr);
    if (ret != 0) {
        ereport(WARNING,
            (errmodule(MOD_VEC_EXECUTOR),
             errmsg("DPA: wd_agg_init failed, ret = %d", ret)));
        return;
    }

    /* succeeded to load */
    g_uadkAggFunc.uadk_agg_inited = true;
    ereport(LOG, (errmsg("UADK AGG library loaded successfully")));
}

void UadkAggFuncUnInit()
{
    if (g_uadkAggFunc.uadk_agg_inited) {
        if (g_uadkAggFunc.wd_agg_uninit != nullptr) {
            g_uadkAggFunc.wd_agg_uninit();
        }
        (void)dlclose(g_uadkAggFunc.handle);
        g_uadkAggFunc.handle = NULL;
        g_uadkAggFunc.uadk_agg_inited = false;
    }
}

handle_t UadkAggAllocSess(struct wd_agg_sess_setup *setup)
{
    return g_uadkAggFunc.wd_agg_alloc_sess(setup);
}

void UadkAggFreeSess(handle_t h_sess)
{
    g_uadkAggFunc.wd_agg_free_sess(h_sess);
}

int UadkAggAddInputSync(handle_t h_sess, struct wd_agg_req *req)
{
    return g_uadkAggFunc.wd_agg_add_input_sync(h_sess, req);
}

int UadkAggGetOutputSync(handle_t h_sess, struct wd_agg_req *req)
{
    return g_uadkAggFunc.wd_agg_get_output_sync(h_sess, req);
}

int UadkAggGetTableRowsize(handle_t h_sess)
{
    return g_uadkAggFunc.wd_agg_get_table_rowsize(h_sess);
}

int UadkAggSetHashTable(handle_t h_sess, struct wd_dae_hash_table *info)
{
    return g_uadkAggFunc.wd_agg_set_hash_table(h_sess, info);
}

int UadkAggRehashSync(handle_t h_sess, struct wd_agg_req *req)
{
    return g_uadkAggFunc.wd_agg_rehash_sync(h_sess, req);
}
