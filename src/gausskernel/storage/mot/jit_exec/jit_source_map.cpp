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
 * jit_source_map.cpp
 *    Global JIT source map.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_source_map.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "jit_source_map.h"
#include "jit_source_pool.h"
#include "utilities.h"

#include <map>

namespace JitExec {
DECLARE_LOGGER(JitSourceMap, JitExec);

// NOTE: Consider using oltp_map
typedef std::map<std::string, JitSource*> JitSourceMapType;

/** @struct Global JIT source map. */
struct JitSourceMap {
    /** @var Synchronize global map access. */
    pthread_mutex_t m_lock;  // 40 bytes

    /** @var Keep noisy lock in its own cache line. */
    uint8_t m_padding[24];

    /** @var Initialization flag. */
    uint64_t m_initialized = 0;

    /** @var The JIT source map. */
    JitSourceMapType m_sourceMap;
};

// Globals
static JitSourceMap g_jitSourceMap __attribute__((aligned(64)));

extern bool InitJitSourceMap()
{
    g_jitSourceMap.m_initialized = 0;
    int res = pthread_mutex_init(&g_jitSourceMap.m_lock, NULL);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(
            res, pthread_mutex_init, "Code Generation", "Failed to create mutex for global jit-source map");
    } else {
        g_jitSourceMap.m_initialized = 1;
    }
    return g_jitSourceMap.m_initialized != 0 ? true : false;
}

extern void DestroyJitSourceMap()
{
    if (g_jitSourceMap.m_initialized) {
        ClearJitSourceMap();
        pthread_mutex_destroy(&g_jitSourceMap.m_lock);
        g_jitSourceMap.m_initialized = 0;
    }
}

extern void ClearJitSourceMap()
{
    MOT_LOG_TRACE("Clearing global jit-source map");
    JitSourceMapType::iterator itr = g_jitSourceMap.m_sourceMap.begin();
    while (itr != g_jitSourceMap.m_sourceMap.end()) {
        JitSource* jitSource = itr->second;
        FreePooledJitSource(jitSource);
        itr = g_jitSourceMap.m_sourceMap.erase(itr);
    }
}

extern void LockJitSourceMap()
{
    pthread_mutex_lock(&g_jitSourceMap.m_lock);
}

extern void UnlockJitSourceMap()
{
    pthread_mutex_unlock(&g_jitSourceMap.m_lock);
}

extern uint32_t GetJitSourceMapSize()
{
    return g_jitSourceMap.m_sourceMap.size();
}

extern JitSource* GetCachedJitSource(const char* queryString)
{
    JitSource* result = NULL;
    JitSourceMapType::iterator itr = g_jitSourceMap.m_sourceMap.find(queryString);
    if (itr != g_jitSourceMap.m_sourceMap.end()) {
        result = itr->second;
    }
    return result;
}

extern bool AddCachedJitSource(JitSource* cachedJitSource)
{
    MOT_LOG_TRACE("Inserting JIT source %p to global source map on query string: %s",
        cachedJitSource,
        cachedJitSource->_query_string);
    char* queryString = cachedJitSource->_query_string;  // required due to compiler error
    return g_jitSourceMap.m_sourceMap.insert(JitSourceMapType::value_type(queryString, cachedJitSource)).second;
}

extern bool ContainsReadyCachedJitSource(const char* queryString)
{
    bool result = false;
    LockJitSourceMap();
    JitSourceMapType::iterator itr = g_jitSourceMap.m_sourceMap.find(queryString);
    if (itr != g_jitSourceMap.m_sourceMap.end()) {
        JitSource* jitSource = itr->second;
        if ((jitSource->_status == JIT_CONTEXT_READY) && (jitSource->_source_jit_context != NULL)) {
            result = true;
        }
    }
    UnlockJitSourceMap();
    return result;
}

extern void PurgeJitSourceMap(uint64_t relationId, bool purgeOnly)
{
    LockJitSourceMap();
    JitSourceMapType::iterator itr = g_jitSourceMap.m_sourceMap.begin();
    while (itr != g_jitSourceMap.m_sourceMap.end()) {
        JitSource* jitSource = itr->second;
        if (JitSourceRefersRelation(jitSource, relationId)) {
            MOT_LOG_TRACE("Purging cached jit-source %p by relation id %" PRIu64 " with query: %s",
                jitSource,
                relationId,
                jitSource->_query_string);
            if (purgeOnly) {
                PurgeJitSource(jitSource, relationId);
            } else {
                SetJitSourceExpired(jitSource, relationId);  // (containing jit-source deleted during db shutdown)
            }
        }
        ++itr;
    }
    UnlockJitSourceMap();
}
}  // namespace JitExec
