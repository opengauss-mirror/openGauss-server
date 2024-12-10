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

hnswpq_func_t g_hnsw_func = {0};

// return HNSWPQ_ERROR if error occurs
#define HNSWPQ_RETURN_IFERR(ret)                            \
    do {                                                \
        int _status_ = (ret);                           \
        if (SECUREC_UNLIKELY(_status_ != HNSWPQ_SUCCESS)) { \
            return _status_;                            \
        }                                               \
    } while (0)

int hnswpq_resolve_path(char* absolute_path, const char* raw_path, const char* filename)
{
    char path[MAX_PATH_LEN] = { 0 };

    if (!realpath(raw_path, path)) {
        if (errno != ENOENT && errno != EACCES) {
            return HNSWPQ_ERROR;
        }
    }

    int ret = snprintf_s(absolute_path, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s", path, filename);
    if (ret < 0) {
        return HNSWPQ_ERROR;
    }
    return HNSWPQ_SUCCESS;
}

int hnswpq_load_symbol(char *symbol, void **sym_lib_handle)
{
#ifndef WIN32
    const char *dlsym_err = NULL;

    *sym_lib_handle = dlsym(g_hnsw_func.handle, symbol);
    dlsym_err = dlerror();
    if (dlsym_err != NULL) {
        ereport(FATAL, (errcode(ERRCODE_INVALID_OPERATION),
            errmsg("incompatible library \"%s\", load %s failed, %s", HNSWPQ_SO_NAME, symbol, dlsym_err)));
        return HNSWPQ_ERROR;
    }
#endif // !WIN32
    return HNSWPQ_SUCCESS;
}

#define HNSWPQ_LOAD_SYMBOL_FUNC(func) hnswpq_load_symbol(#func, (void **)&g_hnsw_func.func)

int hnswpq_open_dl(void **lib_handle, char *symbol)
{
#ifndef WIN32
    *lib_handle = dlopen(symbol, RTLD_LAZY);
    if (*lib_handle == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not load library %s, %s", HNSWPQ_SO_NAME, dlerror())));
        return HNSWPQ_ERROR;
    }
    return HNSWPQ_SUCCESS;
#else
    return HNSWPQ_ERROR;
#endif
}

void hnswpq_close_dl(void *lib_handle)
{
#ifndef WIN32
    (void)dlclose(lib_handle);
#endif
}

int hnswpq_load_symbols(char *lib_dl_path)
{
    HNSWPQ_RETURN_IFERR(hnswpq_open_dl(&g_hnsw_func.handle, lib_dl_path));

    HNSWPQ_RETURN_IFERR(HNSWPQ_LOAD_SYMBOL_FUNC(ComputePQTable));
    HNSWPQ_RETURN_IFERR(HNSWPQ_LOAD_SYMBOL_FUNC(ComputeVectorPQCode));
    HNSWPQ_RETURN_IFERR(HNSWPQ_LOAD_SYMBOL_FUNC(GetPQDistanceTableSdc));
    HNSWPQ_RETURN_IFERR(HNSWPQ_LOAD_SYMBOL_FUNC(GetPQDistanceTableAdc));
    HNSWPQ_RETURN_IFERR(HNSWPQ_LOAD_SYMBOL_FUNC(GetPQDistance));
    HNSWPQ_RETURN_IFERR(HNSWPQ_LOAD_SYMBOL_FUNC(Rerank));

    return HNSWPQ_SUCCESS;
}

int hnswpq_func_init()
{
    if (g_hnsw_func.inited) {
        return HNSWPQ_SUCCESS;
    }

    char lib_dl_path[MAX_PATH_LEN] = { 0 };
    char* raw_path = getenv(HNSWPQ_ENV_PATH);
    if (raw_path == nullptr) {
        ereport(ERROR, (errmsg("failed to get DATAVEC_HNSWPQ_LIB_PATH")));
        return HNSWPQ_ERROR;
    }

    int ret = hnswpq_resolve_path(lib_dl_path, raw_path, HNSWPQ_SO_NAME);
    if (ret != HNSWPQ_SUCCESS) {
        ereport(ERROR, (errmsg(
            "failed to resolve the path of libvecturbo.so, lib_dl_path %s, raw_path %s",
            lib_dl_path, raw_path)));
        return HNSWPQ_ERROR;
    }

    ret = hnswpq_load_symbols(lib_dl_path);
    if (ret != HNSWPQ_SUCCESS) {
        return HNSWPQ_ERROR;
    }

    g_hnsw_func.inited = true;
    return HNSWPQ_SUCCESS;
}

int HNSWPQInit()
{
#ifdef __x86_64__
    ereport(FATAL, (errmsg("HNSWPQ only support in arm.")));
#endif
    if (hnswpq_func_init() != HNSWPQ_SUCCESS) {
        ereport(FATAL, (errmsg("failed to init HNSWPQ library")));
        return HNSWPQ_ERROR;
    }
    g_instance.hnswpq_inited = true;
    return HNSWPQ_SUCCESS;
}

void HNSWPQUinit()
{
    if (!g_instance.attr.attr_storage.enable_hnswpq || ! g_instance.hnswpq_inited) {
        return;
    }
    g_instance.hnswpq_inited = false;
    ereport(LOG, (errmsg("datavec HNSWPQ uninit")));
    if (g_hnsw_func.handle != NULL) {
        hnswpq_close_dl(g_hnsw_func.handle);
        g_hnsw_func.handle = NULL;
        g_hnsw_func.inited = false;
    }
}

int ComputePQTable(VectorArray samples, PQParams *params)
{
    return g_hnsw_func.ComputePQTable(samples, params);
}

int ComputeVectorPQCode(float *vector, const PQParams *params, uint8 *pqCode)
{
    return g_hnsw_func.ComputeVectorPQCode(vector, params, pqCode);
}

int GetPQDistanceTableSdc(const PQParams *params, float *pqDistanceTable)
{
    return g_hnsw_func.GetPQDistanceTableSdc(params, pqDistanceTable);
}

int GetPQDistanceTableAdc(float *vector, const PQParams *params, float *pqDistanceTable)
{
    return g_hnsw_func.GetPQDistanceTableAdc(vector, params, pqDistanceTable);
}

int GetPQDistance(const uint8 *basecode, const uint8 *querycode, const PQParams *params,
                  const float *pqDistanceTable, float *PQDistance)
{
    return g_hnsw_func.GetPQDistance(basecode, querycode, params, pqDistanceTable, PQDistance);
}

int Rerank(float *query, const PQParams *params, const int candidateNum, Candidate **candidateSet, Candidate **sortSet)
{
    return g_hnsw_func.Rerank(query, params, candidateNum, candidateSet, sortSet);
}