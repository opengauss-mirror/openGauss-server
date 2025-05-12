/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * hnswadaptor.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/hnswadaptor.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <dlfcn.h>
#include "access/datavec/hnsw.h"
#include "access/datavec/utils.h"

// return PQ_ERROR if error occurs
#define PQ_RETURN_IFERR(ret)                            \
    do {                                                \
        int _status_ = (ret);                           \
        if (SECUREC_UNLIKELY(_status_ != PQ_SUCCESS)) { \
            return _status_;                            \
        }                                               \
    } while (0)

int pq_resolve_path(char* absolute_path, const char* raw_path, const char* filename)
{
    char path[MAX_PATH_LEN] = { 0 };

    if (!realpath(raw_path, path)) {
        if (errno != ENOENT && errno != EACCES) {
            return PQ_ERROR;
        }
    }

    int ret = snprintf_s(absolute_path, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s", path, filename);
    if (ret < 0) {
        return PQ_ERROR;
    }
    return PQ_SUCCESS;
}

int pq_load_symbol(char *symbol, void **sym_lib_handle)
{
#ifndef WIN32
    const char *dlsym_err = NULL;

    *sym_lib_handle = dlsym(g_pq_func.handle, symbol);
    dlsym_err = dlerror();
    if (dlsym_err != NULL) {
        ereport(FATAL, (errcode(ERRCODE_INVALID_OPERATION),
            errmsg("incompatible library \"%s\", load %s failed, %s", PQ_SO_NAME, symbol, dlsym_err)));
        return PQ_ERROR;
    }
#endif // !WIN32
    return PQ_SUCCESS;
}

#define PQ_LOAD_SYMBOL_FUNC(func) pq_load_symbol(#func, (void **)&g_pq_func.func)

int pq_open_dl(void **lib_handle, char *symbol)
{
#ifndef WIN32
    *lib_handle = dlopen(symbol, RTLD_LAZY);
    if (*lib_handle == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not load library %s, %s", PQ_SO_NAME, dlerror())));
        return PQ_ERROR;
    }
    return PQ_SUCCESS;
#else
    return PQ_ERROR;
#endif
}

void pq_close_dl(void *lib_handle)
{
#ifndef WIN32
    (void)dlclose(lib_handle);
#endif
}

int pq_load_symbols(char *lib_dl_path)
{
    PQ_RETURN_IFERR(pq_open_dl(&g_pq_func.handle, lib_dl_path));

    PQ_RETURN_IFERR(PQ_LOAD_SYMBOL_FUNC(ComputePQTable));
    PQ_RETURN_IFERR(PQ_LOAD_SYMBOL_FUNC(ComputeVectorPQCode));
    PQ_RETURN_IFERR(PQ_LOAD_SYMBOL_FUNC(GetPQDistanceTableSdc));
    PQ_RETURN_IFERR(PQ_LOAD_SYMBOL_FUNC(GetPQDistanceTableAdc));
    PQ_RETURN_IFERR(PQ_LOAD_SYMBOL_FUNC(GetPQDistance));

    return PQ_SUCCESS;
}

int pq_func_init()
{
    if (g_pq_func.inited) {
        return PQ_SUCCESS;
    }

    char lib_dl_path[MAX_PATH_LEN] = { 0 };
    char* raw_path = getenv(PQ_ENV_PATH);
    if (raw_path == nullptr) {
        ereport(ERROR, (errmsg("failed to get DATAVEC_PQ_LIB_PATH")));
        return PQ_ERROR;
    }

    int ret = pq_resolve_path(lib_dl_path, raw_path, PQ_SO_NAME);
    if (ret != PQ_SUCCESS) {
        ereport(ERROR, (errmsg(
            "failed to resolve the path of libvecturbo.so, lib_dl_path %s, raw_path %s",
            lib_dl_path, raw_path)));
        return PQ_ERROR;
    }

    ret = pq_load_symbols(lib_dl_path);
    if (ret != PQ_SUCCESS) {
        return PQ_ERROR;
    }

    g_pq_func.inited = true;
    return PQ_SUCCESS;
}

int PQInit()
{
#ifdef __x86_64__
    ereport(FATAL, (errmsg("PQ only support in arm.")));
#endif
    if (pq_func_init() != PQ_SUCCESS) {
        ereport(FATAL, (errmsg("failed to init PQ library")));
        return PQ_ERROR;
    }
    g_instance.pq_inited = true;
    return PQ_SUCCESS;
}

void PQUinit()
{
    if (!g_instance.attr.attr_storage.enable_pq || ! g_instance.pq_inited) {
        return;
    }
    g_instance.pq_inited = false;
    ereport(LOG, (errmsg("datavec PQ uninit")));
    if (g_pq_func.handle != NULL) {
        pq_close_dl(g_pq_func.handle);
        g_pq_func.handle = NULL;
        g_pq_func.inited = false;
    }
}

int ComputePQTable(VectorArray samples, PQParams *params)
{
    return g_pq_func.ComputePQTable(samples, params);
}

int ComputeVectorPQCode(float *vector, const PQParams *params, uint8 *pqCode)
{
    return g_pq_func.ComputeVectorPQCode(vector, params, pqCode);
}

int GetPQDistanceTableSdc(const PQParams *params, float *pqDistanceTable)
{
    return g_pq_func.GetPQDistanceTableSdc(params, pqDistanceTable);
}

int GetPQDistanceTableAdc(float *vector, const PQParams *params, float *pqDistanceTable)
{
    return g_pq_func.GetPQDistanceTableAdc(vector, params, pqDistanceTable);
}

int GetPQDistance(const uint8 *basecode, const uint8 *querycode, const PQParams *params,
                  const float *pqDistanceTable, float *pqDistance)
{
    return g_pq_func.GetPQDistance(basecode, querycode, params, pqDistanceTable, pqDistance);
}