/*
 * Copyright (c) 2023 China Unicom Co.,Ltd.
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
 * gs_uwal_adaptor.cpp
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/gs_uwal/gs_uwal_adaptor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <cstdlib>
#include <dlfcn.h>
#include <unistd.h>
#include "knl/knl_thread.h"

#ifndef MAX_PATH_LEN
#define MAX_PATH_LEN UWAL_MAX_PATH_LEN
#endif

#define UWAL_ENV_PATH "UWAL_LIB_PATH"
#define UWAL_SO_NAME "libuwal.so"

bool uwal_enabled = false;

typedef struct {
    void* g_uwal_handle;
    UwalInit init;
    UwalExit uninit;
    UwalCreate create;
    UwalDelete remove;
    UwalAppend uappend;
    UwalRead uread;
    UwalTruncate truncate;
    UwalQuery query;
    UwalQueryByUser query_by_user;
    UwalSetRewindPoint set_rewind_point;
    UwalRegisterCertVerifyFunc register_cert_verify_func;
    UwalNotifyNodeListChange notify_nodelist_change;
    UWAL_Ipv4InetToInt ipv4_inet_to_int;
} uwal_func_t;

uwal_func_t g_uwal_func;

#define UWAL_LOAD_SYMBOLS(ACTION)                                      \
    ACTION(init, UwalInit)                                             \
    ACTION(uninit, UwalExit)                                           \
    ACTION(create, UwalCreate)                                         \
    ACTION(remove, UwalDelete)                                         \
    ACTION(uappend, UwalAppend)                                        \
    ACTION(uread, UwalRead)                                            \
    ACTION(truncate, UwalTruncate)                                     \
    ACTION(query, UwalQuery)                                           \
    ACTION(query_by_user, UwalQueryByUser)                             \
    ACTION(set_rewind_point, UwalSetRewindPoint)                       \
    ACTION(register_cert_verify_func, UwalRegisterCertVerifyFunc)      \
    ACTION(notify_nodelist_change, UwalNotifyNodeListChange)           \
    ACTION(ipv4_inet_to_int, UWAL_Ipv4InetToInt)

#define UWAL_HANDLE_GET_SYM(op, name)                                          \
    do {                                                                       \
        const char *dlsym_err = nullptr;                                       \
        g_uwal_func.op = (name)dlsym(g_uwal_func.g_uwal_handle, #name);        \
        dlsym_err = dlerror();                                                 \
        if (dlsym_err != nullptr) {                                            \
            return -1;                                                         \
        }                                                                      \
    } while (0);

static int uwal_resolve_path(char* absolute_path, const char* raw_path, const char* filename)
{
    char path[MAX_PATH_LEN] = { 0 };

    if (!realpath(raw_path, path)) {
        if (errno != ENOENT && errno != EACCES) {
            return -1;
        }
    }

    int ret = snprintf_s(absolute_path, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s", path, filename);
    if (ret < 0) {
        return -1;
    }
    return 0;
}

static int uwal_open_dl(void **lib_handle, char *symbol)
{
#ifdef WIN32
    return -1;
#else
    *lib_handle = dlopen(symbol, RTLD_LAZY);
    if (*lib_handle == nullptr) {
        return -1;
    }
#endif
    return 0;
}

static int uwal_load_symbols(char* lib_dl_path)
{
    int ret = uwal_open_dl(&g_uwal_func.g_uwal_handle, lib_dl_path);
    if (ret != 0) {
        return ret;
    }
    UWAL_LOAD_SYMBOLS(UWAL_HANDLE_GET_SYM);
    uwal_enabled = true;
    return 0;
}

int uwal_init_symbols()
{
    char lib_dl_path[MAX_PATH_LEN] = { 0 };
    char* raw_path = getenv(UWAL_ENV_PATH);
    if (raw_path == nullptr) {
        return -1;
    }

    int ret = uwal_resolve_path(lib_dl_path, raw_path, UWAL_SO_NAME);
    if (ret != 0) {
        return ret;
    }
    return uwal_load_symbols(lib_dl_path);
}

int ock_uwal_init(IN const char *path, IN const UwalCfgElem *elems, IN int cnt, IN const char *ulogPath)
{
    return g_uwal_func.init(path, elems, cnt, ulogPath);
}

void ock_uwal_exit(void)
{
    g_uwal_func.uninit();
}

int ock_uwal_create(IN UwalCreateParam *param, OUT UwalVector *uwals)
{
    return g_uwal_func.create(param, uwals);
}

int ock_uwal_delete(IN UwalDeleteParam *param)
{
    return g_uwal_func.remove(param);
}

int ock_uwal_append(IN UwalAppendParam *param, OUT uint64_t *offset, OUT void* result)
{
    return g_uwal_func.uappend(param, offset, result);
}

int ock_uwal_read(IN UwalReadParam *param, OUT UwalBufferList *bufferList)
{
    return g_uwal_func.uread(param, bufferList);
}

int ock_uwal_truncate(IN const UwalId *uwalId, IN uint64_t offset)
{
    return g_uwal_func.truncate(uwalId, offset);
}

int ock_uwal_query(IN UwalQueryParam *param, OUT UwalInfo *info)
{
    return g_uwal_func.query(param, info);
}

int ock_uwal_query_by_user(IN UwalUserType user, IN UwalRouteType route, OUT UwalVector *uwals)
{
    return g_uwal_func.query_by_user(user, route, uwals);
}

int ock_uwal_set_rewind_point(IN UwalId *uwalId, IN uint64_t offset)
{
    return g_uwal_func.set_rewind_point(uwalId, offset);
}

int ock_uwal_register_cert_verify_func(int32_t (*certVerify)(void* certStoreCtx, const char *crlPath),
    int32_t (*getKeyPass)(char *keyPassBuff, uint32_t keyPassBuffLen, char *keyPassPath))
{
    return g_uwal_func.register_cert_verify_func(certVerify, getKeyPass);
}

int32_t ock_uwal_notify_nodelist_change(NodeStateList *nodeList, FinishCbFun cb, void *ctx)
{
    return g_uwal_func.notify_nodelist_change(nodeList, cb, ctx);
}

uint32_t ock_uwal_ipv4_inet_to_int(char ipv4[16UL])
{
    return g_uwal_func.ipv4_inet_to_int(ipv4);
}
