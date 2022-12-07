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
#include "jit_common.h"
#include "utilities.h"
#include "storage/mot/jit_exec.h"

#include <map>
#include <algorithm>

// Global and Session-local JIT Source map for Correct Transactional Behavior:
// ==========================================================================
// 1. We use a session-local on-demand map for coping with session-local DDLs. The map is used as a restricted and
//    modified view of the global map
// 2. As long as in the current transaction there was no DDL issued, we continue as usual to work with the global map.
//    (see point 4 below).
// 3. As soon as the first DDL arrives, the following changes take place:
//    A. A session-local map is created.
//    B. All affected JIT QUERY sources are cloned to local map, taking with them all session-local JIT context objects.
//       All cloned JIT sources are expired and notifications are sent to all related session-local JIT contexts.
//       This is done only for sources not already found in local map (i.e. that were not previously cloned)
//    C. All affected JIT SP query sources immediately cause for cloning of the entire SP tree, and invalidation of the
//       related JIT SP Query source (in case it should be executed in the current transaction).
//    D. Subsequent DDLs affecting local source will cause expire only in local source, and there is no need for
//       cloning again the global source.
// 4. Any attempt to search for an existing jitted query/function is made first from the local map if such one exists.
//    If it is not found in the local map, then we declare it is not found, because we cannot tell whether this
//    query/function relies on some local DDL change (and so we cannot use the global source). If a local map does not
//    exist (meaning no DDL was issued yet for the current transaction), then we search in the global map.
// 5. Any new jitted query/function is placed in the session-local map if such one exists, since we cannot tell whether
//    it relies on any previous DDL for the current transaction. It will be published during commit (see next point).
//    This includes all revalidation scenarios as well.
// 6. Once current transaction is committed, all changes in the local map are merged back to the global map as follows:
//    A. All session-local contexts are merged back to global source (restoring the back pointer accordingly)
//    B. All related contexts (including global ones) are purged to force re-fetching table/index objects in case of a
//       valid LOCAL source
//    C. All related contexts (including global ones) are invalidated to force full revalidation in case of an invalid
//       LOCAL source
// 7. Once current transaction is rolled-back, all changes in the local map are discarded as follows:
//    A. All session-local contexts are merged back to global source (restoring the back pointer accordingly)
//    B. All related session-local contexts are purged to force re-fetching table/index objects in case of a valid
//       GLOBAL source
//    C. All related session-local contexts are invalidated to force full revalidation in case of an invalid GLOBAL
//       source
//
// During first phase of implementation, jitted SPs are not taking part in transactional changes.
// Pay attention that all table/index pointer are now always taken from the current transaction, even during code-
// generation phase.

namespace JitExec {
DECLARE_LOGGER(JitSourceMap, JitExec);

typedef std::map<std::string, JitSource*> JitSourceMapType;
using JitNamespaceMapType = std::map<std::string, JitSourceMapType>;
using JitSPOidMapType = std::map<Oid, TimestampTz>;
using JitSourceSet = std::set<JitSource*>;

/** @enum Constants for stored procedure operations. */
enum class JitSPOperation {
    /** @var Denotes stored procedure replace operation. */
    JIT_SP_REPLACE,

    /** @var Denotes stored procedure drop operation. */
    JIT_SP_DROP
};

/** @typedef Maps of stored procedure operations (required for pruning). */
using JitSPOperationMap = std::map<Oid, JitSPOperation>;

/**
 * @const Format of Namespace is "%s.%u" (FunctionName.FunctionOid). Maximum length of function name is NAMEDATALEN.
 * For simplicity, we use (NAMEDATALEN * 2) as maximum namespace length.
 */
static constexpr uint32_t JIT_MAX_NAMESPACE_LEN = NAMEDATALEN * 2;

/** @struct JIT source name-space. */
struct JitNamespace {
    /** @var The identifier of the function inducing the name space.*/
    Oid m_functionId;

    /** @var The name-space. */
    char m_namespace[JIT_MAX_NAMESPACE_LEN];

    /** @var The next name-space. */
    JitNamespace* m_next;
};

/** @struct Global JIT source map. */
struct JitGlobalSourceMap {
#ifdef MOT_JIT_SOURCE_MAP_USE_RWLOCK
    /** @var Synchronize global map access. */
    pthread_rwlock_t m_rwlock;  // 56 bytes

    /** @var Keep noisy lock in its own cache line. */
    uint8_t m_padding[8];
#else
    /** @var Synchronize global map access. */
    pthread_mutex_t m_lock;  // 40 bytes

    /** @var Keep noisy lock in its own cache line. */
    uint8_t m_padding[24];
#endif

    /** @var Initialization flag. */
    uint64_t m_initialized = 0;

    /** @var The JIT name space map. */
    JitNamespaceMapType m_namespaceMap;

    /** @var Synchronize deprecate sources access. */
    pthread_mutex_t m_deprecateLock;

    /** @var List of deprecate sources pending for recycling. */
    JitSourceSet m_deprecateSources;

    /** @var List of deprecate sources ready for recycling. */
    JitSourceSet m_readyDeprecateSources;

    /** @var Map to record the dropped SP Oid. */
    JitSPOidMapType m_droppedSPOidMap;
};

/** @struct Local JIT source map. */
struct JitLocalSourceMap {
    /** @var The JIT name space map. */
    JitNamespaceMapType m_namespaceMap;

    /** @var Records SP operations done during a transaction. */
    JitSPOperationMap m_operationMap;

    /**
     * @var Records the new global sources SPs that are dropped during a transaction.
     * These will be deprecated at the end of COMMIT.
     */
    JitSourceMapType m_droppedSPMap;
};

// Global variables
static JitGlobalSourceMap g_jitSourceMap __attribute__((aligned(64)));

// forward declarations
static bool AddCachedJitSource(
    JitNamespaceMapType* namespaceMap, const char* queryNamespace, JitSource* cachedJitSource);
static JitSource* FindCachedJitSource(
    const char* queryString, const char* queryNamespace, JitNamespaceMapType* namespaceMap);
static void ClearJitSourceMapImpl(JitNamespaceMapType* namespaceMap, bool issueWarnings, bool isShutdown);

static void RestoreCachedPlanContexts(JitSource* jitSource);
static void PruneJitSourceMap();      // Not thread-safe
static void RemoveEmptyNamespaces();  // Not thread-safe
static void PurgeDeprecateQueryList(uint64_t relationId);
static void CleanUpReadyDeprecateJitSources();
static void CleanUpDeprecateJitSources();

inline JitLocalSourceMap* GetLocalSourceMap()
{
    return (JitLocalSourceMap*)u_sess->mot_cxt.jit_session_source_map;
}

inline void SetLocalSourceMap(JitLocalSourceMap* localMap)
{
    MOT_ASSERT(u_sess->mot_cxt.jit_session_source_map == nullptr);
    MOT_ASSERT(localMap != nullptr);
    u_sess->mot_cxt.jit_session_source_map = localMap;
}

inline void ClearLocalSourceMap()
{
    MOT_ASSERT(u_sess->mot_cxt.jit_session_source_map != nullptr);
    u_sess->mot_cxt.jit_session_source_map = nullptr;
}

static JitLocalSourceMap* GetOrCreateLocalSourceMap()
{
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if (localMap == nullptr) {
        size_t allocSize = sizeof(JitLocalSourceMap);
        char* buf = (char*)MOT::MemSessionAlloc(allocSize);
        if (buf == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "JIT Notify DDL",
                "Failed to allocate %u bytes for session-local JIT source map",
                allocSize);
            return nullptr;
        }
        localMap = new (buf) JitLocalSourceMap();
        SetLocalSourceMap(localMap);
        MOT_LOG_DEBUG("JIT-TXN: Created session-local JIT source map");
    }
    return localMap;
}

static void DestroyLocalSourceMap()
{
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if (localMap != nullptr) {
        MOT_LOG_DEBUG("JIT-TXN: Clearing local jit-source map");
        ClearJitSourceMapImpl(&localMap->m_namespaceMap, true, false);
        localMap->~JitLocalSourceMap();
        MOT::MemSessionFree(localMap);
        ClearLocalSourceMap();
        MOT_LOG_DEBUG("JIT-TXN: Destroyed session-local JIT source map");
    }
}

extern bool InitJitSourceMap()
{
    g_jitSourceMap.m_initialized = 0;
    // we need a recursive mutex, due to complexities when scheduling ready-for-recycle deprecate source for cleanup
    // (see RemoveJitSourceContext() calling ScheduleDeprecateJitSourceCleanUp(), which might be triggered when calling
    // HandleLocalJitSourceChanges())
    pthread_mutexattr_t attr;
    int res = pthread_mutexattr_init(&attr);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(res,
            pthread_mutexattr_init,
            "Code Generation",
            "Failed to init mutex attributes for global JIT source map");
        return false;
    }

    (void)pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);

    // create map lock
#ifdef MOT_JIT_SOURCE_MAP_USE_RWLOCK
    res = pthread_rwlock_init(&g_jitSourceMap.m_rwlock, NULL);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(
            res, pthread_rwlock_init, "Code Generation", "Failed to create lock for global JIT source map");
        return false;
    }
#else
    res = pthread_mutex_init(&g_jitSourceMap.m_lock, NULL);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(
            res, pthread_mutex_init, "Code Generation", "Failed to create lock for global JIT source map");
        return false;
    }
#endif

    // create deprecate sources lock
    res = pthread_mutex_init(&g_jitSourceMap.m_deprecateLock, &attr);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(
            res, pthread_mutex_init, "Code Generation", "Failed to create lock for deprecate JIT sources");
#ifdef MOT_JIT_SOURCE_MAP_USE_RWLOCK
        (void)pthread_rwlock_destroy(&g_jitSourceMap.m_rwlock);
#else
        (void)pthread_mutex_destroy(&g_jitSourceMap.m_lock);
#endif
        return false;
    }

    g_jitSourceMap.m_initialized = 1;
    return true;
}

extern void DestroyJitSourceMap()
{
    if (g_jitSourceMap.m_initialized) {
        ClearJitSourceMap();
        (void)pthread_mutex_destroy(&g_jitSourceMap.m_deprecateLock);
#ifdef MOT_JIT_SOURCE_MAP_USE_RWLOCK
        (void)pthread_rwlock_destroy(&g_jitSourceMap.m_rwlock);
#else
        (void)pthread_mutex_destroy(&g_jitSourceMap.m_lock);
#endif
        g_jitSourceMap.m_initialized = 0;
    }
}

static void ClearJitSourceMapImpl(JitNamespaceMapType* namespaceMap, bool issueWarnings, bool isShutdown)
{
    // since there are inter-dependencies between sources, we clear the map in repeated loops, such that independent
    // sources are first released
    MOT::LogLevel logLevel = issueWarnings ? MOT::LogLevel::LL_WARN : MOT::LogLevel::LL_TRACE;
    const char* mapType = (namespaceMap == &g_jitSourceMap.m_namespaceMap) ? "global" : "local";
    MOT_LOG_TRACE("Destroying %s source map", mapType);
    bool done = false;
    uint32_t round = 0;
    while (!done) {
        uint32_t sourcesDestroyed = 0;
        MOT_LOG_TRACE("Destroying %s source map with %u name spaces", mapType, (uint32_t)namespaceMap->size());
        JitNamespaceMapType::iterator itr = namespaceMap->begin();
        while (itr != namespaceMap->end()) {
            JitSourceMapType& sourceMap = itr->second;
            MOT_LOG_TRACE("Clearing %u JIT source objects under name space: [%s]",
                (uint32_t)sourceMap.size(),
                itr->first.c_str());
            JitSourceMapType::iterator sourceItr = sourceMap.begin();
            while (sourceItr != sourceMap.end()) {
                JitSource* jitSource = sourceItr->second;
                MOT_LOG(
                    logLevel, "Found JIT source %p while cleaning source map: %s", jitSource, jitSource->m_queryString);
                if (!CleanUpDeprecateJitSourceContexts(jitSource)) {
                    MOT_LOG(logLevel, "Skipping destroy of busy source %p: %s", jitSource, jitSource->m_queryString);
                    ++sourceItr;
                } else {
                    sourceItr = sourceMap.erase(sourceItr);
                    DestroyJitSource(jitSource);
                    ++sourcesDestroyed;
                }
            }
            if (sourceMap.empty()) {
                MOT_LOG_TRACE("Source map under name-space [%s] cleared", itr->first.c_str());
                itr = namespaceMap->erase(itr);
            } else {
                MOT_LOG(logLevel,
                    "Source map under name-space [%s] not cleared completely (%u sources left)",
                    itr->first.c_str(),
                    (uint32_t)sourceMap.size());
                ++itr;
            }
        }
        MOT_LOG_TRACE("Destroyed %u sources in round %u", sourcesDestroyed, ++round);
        if (namespaceMap->size() == 0) {
            done = true;
        } else if (!isShutdown) {
            done = true;
        } else if (sourcesDestroyed == 0) {
            MOT_LOG(logLevel, "Cannot clear %s source map (circular dependency?)", mapType);
            done = true;
        }
        if (!done) {
            if (isShutdown) {
                CleanUpDeprecateJitSources();
            } else {
                CleanUpReadyDeprecateJitSources();
            }
        }
    }

    if (!namespaceMap->empty()) {
        MOT_LOG(logLevel, "Could not clear %s source map", mapType);
    }
}

extern void ClearJitSourceMap()
{
    MOT_LOG_TRACE("Clearing global jit-source map");
    LockJitSourceMap();
    PruneJitSourceMap();
    CleanUpDeprecateJitSources();
    ClearJitSourceMapImpl(&g_jitSourceMap.m_namespaceMap, false, true);
    UnlockJitSourceMap();
}

#ifdef MOT_JIT_SOURCE_MAP_USE_RWLOCK
extern void LockJitSourceMap(bool readLock /* = false */)
{
    if (readLock) {
        (void)pthread_rwlock_rdlock(&g_jitSourceMap.m_rwlock);
    } else {
        (void)pthread_rwlock_wrlock(&g_jitSourceMap.m_rwlock);
    }
}

extern void UnlockJitSourceMap()
{
    (void)pthread_rwlock_unlock(&g_jitSourceMap.m_rwlock);
}
#else
extern void LockJitSourceMap()
{
    (void)pthread_mutex_lock(&g_jitSourceMap.m_lock);
}

extern void UnlockJitSourceMap()
{
    (void)pthread_mutex_unlock(&g_jitSourceMap.m_lock);
}
#endif

inline void LockDeprecateJitSources()
{
    (void)pthread_mutex_lock(&g_jitSourceMap.m_deprecateLock);
}

inline void UnlockDeprecateJitSources()
{
    (void)pthread_mutex_unlock(&g_jitSourceMap.m_deprecateLock);
}

static uint32_t GetJitSourceMapSize(JitNamespaceMapType* namespaceMap)
{
    uint32_t size = 0;
    JitNamespaceMapType::iterator itr = namespaceMap->begin();
    while (itr != namespaceMap->end()) {
        size += itr->second.size();
        JitSourceMapType& sourceMap = itr->second;
        JitSourceMapType::iterator mapItr = sourceMap.begin();
        // count also non-jittable sub-queries
        while (mapItr != sourceMap.end()) {
            JitSource* jitSource = mapItr->second;
            size += GetJitSourceNonJitQueryCount(jitSource);
            ++mapItr;
        }
        ++itr;
    }
    return size;
}

extern uint32_t GetJitSourceMapSize()
{
    // this is a maximum bound on map size
    uint32_t size = GetJitSourceMapSize(&g_jitSourceMap.m_namespaceMap);
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if (localMap != nullptr) {
        size += GetJitSourceMapSize(&localMap->m_namespaceMap);
    }
    return size;
}

extern bool PushJitSourceNamespace(Oid functionId, const char* queryNamespace)
{
    size_t allocSize = sizeof(JitNamespace);
    JitNamespace* nameSpace = (JitNamespace*)MOT::MemSessionAlloc(allocSize);
    if (nameSpace == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Push Namespace", "Failed to allocate %u bytes for JIT Namespace", allocSize);
        return false;
    }

    errno_t erc = strcpy_s(nameSpace->m_namespace, sizeof(nameSpace->m_namespace), queryNamespace);
    securec_check(erc, "\0", "\0");
    nameSpace->m_functionId = functionId;
    nameSpace->m_next = (JitNamespace*)u_sess->mot_cxt.jit_ns_stack;
    u_sess->mot_cxt.jit_ns_stack = nameSpace;
    MOT_LOG_TRACE("Pushed JIT SP Namespace: %s", nameSpace->m_namespace);
    return true;
}

extern void PopJitSourceNamespace()
{
    JitNamespace* nameSpace = (JitNamespace*)u_sess->mot_cxt.jit_ns_stack;
    if (nameSpace) {
        u_sess->mot_cxt.jit_ns_stack = nameSpace->m_next;
        MOT_LOG_TRACE("Popped JIT SP Namespace: %s", nameSpace->m_namespace);
        MOT::MemSessionFree(nameSpace);
    }
}

extern const char* GetActiveNamespace()
{
    JitNamespace* nameSpace = (JitNamespace*)u_sess->mot_cxt.jit_ns_stack;
    if (nameSpace == nullptr) {
        return MOT_JIT_GLOBAL_QUERY_NS;
    } else {
        return nameSpace->m_namespace;
    }
}

extern Oid GetActiveNamespaceFunctionId()
{
    JitNamespace* nameSpace = (JitNamespace*)u_sess->mot_cxt.jit_ns_stack;
    if (nameSpace == nullptr) {
        return InvalidOid;
    } else {
        return nameSpace->m_functionId;
    }
}

inline bool IsActiveNamespaceGlobal()
{
    return (u_sess->mot_cxt.jit_ns_stack == nullptr);
}

extern JitSource* GetCachedJitSource(const char* queryString, bool globalNamespace /* = false */)
{
    JitSource* jitSource = nullptr;
    const char* queryNamespace = globalNamespace ? MOT_JIT_GLOBAL_QUERY_NS : GetActiveNamespace();
    MOT_LOG_TRACE("Searching in JIT source map name space %s for query: %s", queryNamespace, queryString);

    // if local map exists, then we search only in local map
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if (localMap != nullptr) {
        MOT_LOG_TRACE("Executing search from local map");
        jitSource = FindCachedJitSource(queryString, queryNamespace, &localMap->m_namespaceMap);
    } else {
        // otherwise we search in global map
        MOT_LOG_TRACE("Executing search from global map");
        jitSource = FindCachedJitSource(queryString, queryNamespace, &g_jitSourceMap.m_namespaceMap);
    }

    const char* mapType = (localMap != nullptr) ? "local" : "global";
    if (jitSource != nullptr) {
        MOT_LOG_TRACE("JIT source %p with state %s found in %s source map",
            jitSource,
            JitCodegenStateToString(jitSource->m_codegenState),
            mapType);
    } else {
        MOT_LOG_TRACE("JIT source not found in %s source map, aborting search", mapType);
    }
    return jitSource;
}

extern bool AddCachedJitSource(JitSource* cachedJitSource, bool globalNamespace /* = false */)
{
    // We add new JIT source ALWAYS to local map (everything is transactional up to commit/rollback)
    const char* queryNamespace = globalNamespace ? MOT_JIT_GLOBAL_QUERY_NS : GetActiveNamespace();

    // if there was at least one DDL then we add the query to the local map
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if (localMap != nullptr) {
        MOT_LOG_TRACE("Inserting JIT source %p to local source map under query name-space [%s] with query string: %s",
            cachedJitSource,
            queryNamespace,
            cachedJitSource->m_queryString);

        MOT_ASSERT(cachedJitSource->m_usage == JIT_CONTEXT_LOCAL);
        return AddCachedJitSource(&localMap->m_namespaceMap, queryNamespace, cachedJitSource);
    }

    MOT_LOG_TRACE("Inserting JIT source %p to global source map under query name-space [%s] with query string: %s",
        cachedJitSource,
        queryNamespace,
        cachedJitSource->m_queryString);
    MOT_ASSERT(cachedJitSource->m_usage == JIT_CONTEXT_GLOBAL);
    return AddCachedJitSource(&g_jitSourceMap.m_namespaceMap, queryNamespace, cachedJitSource);
}

extern bool ContainsReadyCachedJitSource(
    const char* queryString, bool globalNamespace, bool allowUnavailable /* = false */)
{
    const char* queryNamespace = globalNamespace ? MOT_JIT_GLOBAL_QUERY_NS : GetActiveNamespace();
    MOT_LOG_TRACE("Searching in JIT source map name space %s for query: %s", queryNamespace, queryString);

    // search in local map first if exists
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if (localMap != nullptr) {
        MOT_LOG_TRACE("Executing search from local map");
        JitSource* jitSource = FindCachedJitSource(queryString, queryNamespace, &localMap->m_namespaceMap);
        // no need to lock local JIT source since it is accessed only by current session
        if ((jitSource != nullptr) && (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_READY) &&
            (jitSource->m_sourceJitContext != nullptr)) {
            MOT_LOG_TRACE("Ready JIT source %p found in session-local source map", jitSource);
            return true;
        }
        if (allowUnavailable && (jitSource != nullptr) &&
            (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_UNAVAILABLE)) {
            MOT_LOG_TRACE("Unavailable JIT source %p found in session-local source map, considered ready", jitSource);
            return true;
        }
        MOT_LOG_TRACE("JIT source %p found in session-local source map, but is not ready: %s", jitSource, queryString);
        return false;  // if local map exists we make decision only from local map
    }

    // otherwise we search in global map
    MOT_LOG_TRACE("Executing search from global map");
    bool result = false;
    JitCodegenState codegenState = JitCodegenState::JIT_CODEGEN_NONE;
    LockJitSourceMap();
    JitSource* jitSource = FindCachedJitSource(queryString, queryNamespace, &g_jitSourceMap.m_namespaceMap);
    if (jitSource != nullptr) {
        LockJitSource(jitSource);
        codegenState = jitSource->m_codegenState;
        if ((jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_READY) &&
            (jitSource->m_sourceJitContext != nullptr)) {
            result = true;
        } else if (allowUnavailable && (jitSource != nullptr) &&
                   (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_UNAVAILABLE)) {
            MOT_LOG_TRACE("Unavailable JIT source %p found in global source map, considered ready", jitSource);
            result = true;
        }
        UnlockJitSource(jitSource);
    }
    UnlockJitSourceMap();
    if (result) {
        MOT_LOG_TRACE("Ready JIT source %p found in global source map", jitSource);
    } else {
        MOT_LOG_TRACE("Ready JIT source NOT found (%p:%s) in global source map",
            jitSource,
            JitCodegenStateToString(codegenState));
    }
    return result;
}

extern void RemoveCachedGlobalJitSource(JitSource* jitSource, const char* nameSpace)
{
    bool found = false;
    JitNamespaceMapType::iterator itr = g_jitSourceMap.m_namespaceMap.find(nameSpace);
    if (itr != g_jitSourceMap.m_namespaceMap.end()) {
        JitSourceMapType& sourceMap = itr->second;
        JitSourceMapType::iterator sourceItr = sourceMap.find(jitSource->m_queryString);
        if (sourceItr != sourceMap.end()) {
            if (sourceItr->second != jitSource) {
                MOT_LOG_TRACE("Mismatching pointers, required %p, found %p: %s",
                    jitSource,
                    sourceItr->second,
                    jitSource->m_queryString);
            } else {
                (void)sourceMap.erase(sourceItr);
                MOT_LOG_TRACE("JIT source %p removed from global map under name-space %s: %s",
                    jitSource,
                    nameSpace,
                    jitSource->m_queryString);
                found = true;
            }
        }
    } else {
        MOT_LOG_TRACE("Name-space %s not found in global map", nameSpace);
    }

    if (!found) {
        MOT_LOG_TRACE("Failed to remove JIT source %p from global map (name-space %s not found): %s",
            jitSource,
            nameSpace,
            jitSource->m_queryString);
    }
}

extern JitSource* GetCachedGlobalJitSource(const char* queryString, const char* nameSpace)
{
    MOT_LOG_TRACE("Searching in global source map under name-space %s for query: %s", nameSpace, queryString);
    JitSource* jitSource = FindCachedJitSource(queryString, nameSpace, &g_jitSourceMap.m_namespaceMap);
    if (jitSource != nullptr) {
        MOT_LOG_TRACE("JIT source %p with state %s found in global source map",
            jitSource,
            JitCodegenStateToString(jitSource->m_codegenState));
    } else {
        MOT_LOG_TRACE("JIT source not found in global source map, aborting search");
    }
    return jitSource;
}

static void TraceJitDetail(MotJitDetail* entry)
{
    MOT_LOG_DEBUG("JIT entry: procOid=%u, query=%s, namespace=%s, jittableStatus=%s, validStatus=%s, planType=%s",
        (unsigned)entry->procOid,
        entry->query,
        entry->nameSpace,
        entry->jittableStatus,
        entry->validStatus,
        entry->planType);
}

static void GetMotJitDetail(JitSource* jitSource, MotJitDetail* entry)
{
    // get common fields
    entry->procOid = InvalidOid;
    entry->jittableStatus = pstrdup(JitSourceStatusToString(jitSource->m_status));
    entry->lastUpdatedTimestamp = jitSource->m_timestamp;

    if (jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) {
        entry->procOid = jitSource->m_functionOid;
        char* dotPtr = strchr(jitSource->m_queryString, '.');
        if (dotPtr == nullptr) {
            entry->query = pstrdup(jitSource->m_queryString);
        } else {
            // we shave off the function id
            size_t dotPos = dotPtr - jitSource->m_queryString;
            entry->query = pnstrdup(jitSource->m_queryString, dotPos);
        }
    } else {
        entry->query = pstrdup(jitSource->m_queryString);
    }

    if (jitSource->m_sourceJitContext == nullptr) {
        entry->validStatus = pstrdup(JitCodegenStateToString(jitSource->m_codegenState));
        entry->planType = pstrdup("");
    } else {
        uint8_t validState = MOT_ATOMIC_LOAD(jitSource->m_sourceJitContext->m_validState);
        entry->validStatus = pstrdup(JitContextValidStateToString(validState));
        if (jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) {
            JitFunctionContext* functionContext = (JitFunctionContext*)jitSource->m_sourceJitContext;
            entry->procOid = functionContext->m_functionOid;
            entry->planType = pstrdup("SP");
        } else {
            entry->planType = pstrdup(CommandToString(jitSource->m_commandType));
        }
    }

    entry->codegenTime = (int64)jitSource->m_codegenStats.m_codegenTime;
    entry->verifyTime = (int64)jitSource->m_codegenStats.m_verifyTime;
    entry->finalizeTime = (int64)jitSource->m_codegenStats.m_finalizeTime;
    entry->compileTime = (int64)jitSource->m_codegenStats.m_compileTime;
    TraceJitDetail(entry);
}

static uint32_t GetMotJitDetailNonJitSubQuery(JitSource* jitSource, MotJitDetail* entry)
{
    if (jitSource->m_sourceJitContext == nullptr) {
        return 0;
    }
    if (jitSource->m_sourceJitContext->m_contextType != JitContextType::JIT_CONTEXT_TYPE_FUNCTION) {
        return 0;
    }
    uint32_t count = 0;
    JitFunctionContext* jitContext = (JitFunctionContext*)jitSource->m_sourceJitContext;
    for (uint32_t i = 0; i < jitContext->m_SPSubQueryCount; ++i) {
        JitCallSite* callSite = &jitContext->m_SPSubQueryList[i];
        if (callSite->m_queryContext == nullptr) {
            entry->nameSpace = pstrdup(jitSource->m_queryString);
            entry->procOid = InvalidOid;
            entry->jittableStatus = pstrdup(JitSourceStatusToString(JitSourceStatus::JIT_SOURCE_UNJITTABLE));
            entry->lastUpdatedTimestamp = jitSource->m_timestamp;
            entry->query = pstrdup(callSite->m_queryString);
            entry->validStatus = pstrdup(JitContextValidStateToString(JIT_CONTEXT_VALID));
            entry->planType = pstrdup("FDW");
            entry->codegenTime = 0;
            entry->verifyTime = 0;
            entry->finalizeTime = 0;
            entry->compileTime = 0;
            TraceJitDetail(entry);
            ++count;
            ++entry;
        }
    }
    return count;
}

#ifdef MOT_JIT_TEST
constexpr uint32_t MOT_JIT_TEST_QUERY_LEN = 64;
extern uint64_t totalQueryExecCount;
extern uint64_t totalFunctionExecCount;
static void GetMotJitTestDetail(MotJitDetail* entry, bool query)
{
    entry->nameSpace = pstrdup("TEST");
    entry->procOid = 9999;
    entry->lastUpdatedTimestamp = 0;
    char buf[MOT_JIT_TEST_QUERY_LEN];
    errno_t rc;
    if (query) {
        entry->query = pstrdup("query_count");
        uint64_t queryCount = MOT_ATOMIC_LOAD(totalQueryExecCount);
        rc = snprintf_s(buf, MOT_JIT_TEST_QUERY_LEN, MOT_JIT_TEST_QUERY_LEN - 1, "%" PRIu64, queryCount);
        securec_check_ss(rc, "\0", "\0");
    } else {
        entry->query = pstrdup("function_count");
        uint64_t functionCount = MOT_ATOMIC_LOAD(totalFunctionExecCount);
        rc = snprintf_s(buf, MOT_JIT_TEST_QUERY_LEN, MOT_JIT_TEST_QUERY_LEN - 1, "%" PRIu64, functionCount);
        securec_check_ss(rc, "\0", "\0");
    }
    entry->jittableStatus = pstrdup(buf);
    entry->validStatus = pstrdup("");
    entry->planType = pstrdup("");
    entry->codegenTime = 0;
    entry->verifyTime = 0;
    entry->finalizeTime = 0;
    entry->compileTime = 0;
    TraceJitDetail(entry);
}
static void GetMotJitTestDetailSafe(MotJitDetail* entry, bool query)
{
    volatile MemoryContext origCxt = CurrentMemoryContext;
    PG_TRY();
    {
        GetMotJitTestDetail(entry, query);
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while getting JIT source detail test counters: %s", edata->message);
        FlushErrorState();
        FreeErrorData(edata);
    }
    PG_END_TRY();
}
#endif

static void GetMotJitDetailSafe(JitSource* jitSource, MotJitDetail* entry)
{
    volatile MemoryContext origCxt = CurrentMemoryContext;
    PG_TRY();
    {
        GetMotJitDetail(jitSource, entry);
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN(
            "Caught exception while getting JIT source '%s' detail: %s", jitSource->m_queryString, edata->message);
        FlushErrorState();
        FreeErrorData(edata);
    }
    PG_END_TRY();
}

static uint32_t GetMotJitDetailNonJitSubQuerySafe(JitSource* jitSource, MotJitDetail* entry)
{
    volatile uint32_t result = 0;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    PG_TRY();
    {
        result = GetMotJitDetailNonJitSubQuery(jitSource, entry);
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while getting JIT source '%s' non-jittable sub-query detail: %s",
            jitSource->m_queryString,
            edata->message);
        FlushErrorState();
        FreeErrorData(edata);
    }
    PG_END_TRY();
    return (uint32_t)result;
}

extern MotJitDetail* GetJitSourceDetailImpl(uint32_t* num)
{
    uint32_t maxSize = GetJitSourceMapSize();
#ifdef MOT_JIT_TEST
    maxSize += 2;
#endif
    MotJitDetail* result = (MotJitDetail*)palloc(maxSize * sizeof(MotJitDetail));
    if (result == nullptr) {
        return nullptr;
    }
    size_t allocSize = maxSize * sizeof(MotJitDetail);
    errno_t erc = memset_s(result, allocSize, 0, allocSize);
    securec_check(erc, "\0", "\0");

    // report first entries from local map if any
    MOT_LOG_DEBUG("Reporting local source map JIT detail");
    uint32_t count = 0;
    std::set<std::string> querySet;
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if (localMap != nullptr) {
        JitNamespaceMapType::iterator itr = localMap->m_namespaceMap.begin();
        while (itr != localMap->m_namespaceMap.end()) {
            JitSourceMapType& sourceMap = itr->second;
            JitSourceMapType::iterator sourceItr = sourceMap.begin();
            while (sourceItr != sourceMap.end()) {
                JitSource* jitSource = sourceItr->second;
                (void)querySet.insert(jitSource->m_queryString);
                result[count].nameSpace = pstrdup(itr->first.c_str());
                LockJitSource(jitSource);
                GetMotJitDetailSafe(jitSource, &result[count]);
                ++count;
                count += GetMotJitDetailNonJitSubQuerySafe(jitSource, &result[count]);
                UnlockJitSource(jitSource);
                ++sourceItr;
            }
            ++itr;
        }
    }

    // function are registered under the global name-space
    MOT_LOG_DEBUG("Reporting global source map JIT detail");
    JitNamespaceMapType::iterator itr = g_jitSourceMap.m_namespaceMap.begin();
    while (itr != g_jitSourceMap.m_namespaceMap.end()) {
        bool isGlobalNamespace = (strcmp(itr->first.c_str(), MOT_JIT_GLOBAL_QUERY_NS) == 0);
        JitSourceMapType& sourceMap = itr->second;
        JitSourceMapType::iterator sourceItr = sourceMap.begin();
        while (sourceItr != sourceMap.end()) {
            JitSource* jitSource = sourceItr->second;
            LockJitSource(jitSource);
            if (!isGlobalNamespace || (querySet.find(jitSource->m_queryString) == querySet.end())) {
                result[count].nameSpace = pstrdup(itr->first.c_str());
                GetMotJitDetailSafe(jitSource, &result[count]);
                ++count;
                count += GetMotJitDetailNonJitSubQuerySafe(jitSource, &result[count]);
            }
            UnlockJitSource(jitSource);
            ++sourceItr;
        }
        ++itr;
    }

#ifdef MOT_JIT_TEST
    GetMotJitTestDetailSafe(&result[count], true);
    ++count;
    GetMotJitTestDetailSafe(&result[count], false);
    ++count;
#endif
    *num = count;
    return result;
}

extern MotJitDetail* GetJitSourceDetail(uint32_t* num)
{
    *num = 0;

#ifdef MOT_JIT_SOURCE_MAP_USE_RWLOCK
    LockJitSourceMap(true);
#else
    LockJitSourceMap();
#endif

    volatile MotJitDetail* result = nullptr;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    PG_TRY();
    {
        result = GetJitSourceDetailImpl(num);
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while getting JIT source detail: %s", edata->message);
        FlushErrorState();
        FreeErrorData(edata);
    }
    PG_END_TRY();

    UnlockJitSourceMap();
    return (MotJitDetail*)result;
}

static bool PurgeJitSourceMapQuery(
    JitNamespaceMapType* namespaceMap, uint64_t relationId, bool purgeOnly, bool cloneToLocalMap)
{
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if ((localMap == nullptr) && cloneToLocalMap) {
        MOT_LOG_TRACE("Failed to get session-local source map while purging by relation");
        return false;
    }

    JitNamespaceMapType::iterator itr = namespaceMap->begin();
    while (itr != namespaceMap->end()) {
        JitSourceMapType& sourceMap = itr->second;
        JitSourceMapType::iterator sourceItr = sourceMap.begin();
        MOT_LOG_TRACE("Purging all simple-query JIT source objects under name-space [%s]", itr->first.c_str());
        while (sourceItr != sourceMap.end()) {
            JitSource* jitSource = sourceItr->second;
            MOT_LOG_DEBUG("Checking JIT source %p with source context %p, query: %s",
                jitSource,
                jitSource->m_sourceJitContext,
                jitSource->m_queryString);
            if (IsSimpleQuerySource(jitSource) && JitSourceRefersRelation(jitSource, relationId, false)) {
                // we clone to local session map only simple queries (including SP queries)
                if (cloneToLocalMap) {
                    MOT_LOG_TRACE("Candidate clone JIT source %p with source context %p to local map: %s",
                        jitSource,
                        jitSource->m_sourceJitContext,
                        jitSource->m_queryString);
                    // we clone only if it was not cloned before already
                    JitSource* localSource =
                        FindCachedJitSource(jitSource->m_queryString, itr->first.c_str(), &localMap->m_namespaceMap);
                    if (localSource == nullptr) {
                        MOT_LOG_TRACE("Cloning JIT source %p to local map: %s", jitSource, jitSource->m_queryString);
                        JitSourceOp sourceOp =
                            purgeOnly ? JitSourceOp::JIT_SOURCE_PURGE_ONLY : JitSourceOp::JIT_SOURCE_INVALIDATE;
                        localSource = CloneLocalJitSource(jitSource, purgeOnly, sourceOp);
                        if (localSource == nullptr) {
                            MOT_LOG_TRACE("Failed to clone simple-query JIT source");
                            return false;
                        }
                        if (!AddCachedJitSource(&localMap->m_namespaceMap, itr->first.c_str(), localSource)) {
                            MOT_LOG_TRACE("Failed to add simple-query JIT source to local map");
                            LockJitSource(jitSource);
                            MoveCurrentSessionContexts(localSource, jitSource, JitSourceOp::JIT_SOURCE_NO_OP);
                            UnlockJitSource(jitSource);
                            DestroyJitSource(localSource);
                            return false;
                        }
                    }

                    // make sure source in local map is purged and expired
                    jitSource = localSource;
                }

                // purge must take place in any case
                MOT_LOG_TRACE("Purging cached JIT source %p by relation id %" PRIu64
                              " under name-space [%s] with query: %s",
                    jitSource,
                    relationId,
                    itr->first.c_str(),
                    jitSource->m_queryString);
                PurgeJitSource(jitSource, relationId);
                if (!purgeOnly) {
                    // 1. in case of a simple query, then we want to set query as expired and invalidate all containing
                    //    JIT context objects in all sessions (we are guarded by PG global DDL lock)
                    // 2. in case of a SP we just want to mark the SP as invalid (i.e. some leaf requires code-
                    //    regeneration). This happens implicitly when we follow case 1.
                    // 3. in case of INVOKE context we just want the relevant SP leaf nodes to be marked as invalid.
                    //    This is achieved already by case 1.
                    if ((jitSource->m_sourceJitContext != nullptr) &&
                        (jitSource->m_sourceJitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY)) {
                        MOT_LOG_TRACE("Setting JIT source %p as expired: %s", jitSource, jitSource->m_queryString);
                        JitQueryContext* queryContext = (JitQueryContext*)jitSource->m_sourceJitContext;
                        if (queryContext->m_commandType != JIT_COMMAND_INVOKE) {
                            // (containing JIT source deleted during db shutdown)
                            SetJitSourceExpired(jitSource, relationId);
                        }
                    }
                }
            }
            ++sourceItr;
        }
        ++itr;
    }
    return true;
}

static void PurgeJitSourceMapQuery(uint64_t relationId, bool purgeOnly)
{
    // in case some JIT source is invalid, we cannot tell whether it should be moved to local map (because the table
    // is resides in the deleted context. As a result it might get revalidated on the global map and fail to follow
    // transactional semantics (since it can get revalidated based on new definitions). For this reason we create a
    // local map unconditionally, such that when revalidation takes place, we can tell that at least one DDL took place
    // in the current transaction, and so we need to clone first the jit source to the local map before revalidation
    // takes place.
    MOT_LOG_TRACE("Purging JIT source map queries by relation %" PRIu64 " (purgeOnly: %s)",
        relationId,
        purgeOnly ? "true" : "false");
    JitLocalSourceMap* localMap = GetOrCreateLocalSourceMap();
    if (localMap == nullptr) {
        MOT_LOG_ERROR("PurgeJitSourceMapQuery: Failed to create session-local source map");
        return;
    }

    MOT_LOG_TRACE("Purging local source map");
    (void)PurgeJitSourceMapQuery(&localMap->m_namespaceMap, relationId, purgeOnly, false);

    // now purge global map, such that each purged source is cloned and moved to the local map before purge/expire
    LockJitSourceMap();
    MOT_LOG_TRACE("Purging global source map");
    if (!PurgeJitSourceMapQuery(&g_jitSourceMap.m_namespaceMap, relationId, purgeOnly, true)) {
        MOT_LOG_ERROR("Failed to purge global source map by relation %" PRIu64 " (purgeOnly: %s)",
            relationId,
            purgeOnly ? "true" : "false");
    }
    UnlockJitSourceMap();

    // we purge deprecate source now, even if roll-back will be issued later, since this is a safe point to purge the
    // deprecate source and all its associated context objects (because we hold exclusive lock on relation)
    PurgeDeprecateQueryList(relationId);

    // recycle deprecate sources that are no longer referenced
    CleanUpReadyDeprecateJitSources();
}

inline bool IsInvokeSPQuerySource(JitSource* jitSource, Oid functionId)
{
    if ((jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) &&
        (jitSource->m_commandType == JIT_COMMAND_INVOKE)) {
        if (jitSource->m_functionOid == functionId) {
            return true;
        } else {
            MOT_LOG_TRACE("Invoke query refers function %u", jitSource->m_functionOid);
        }
    }
    return false;
}

inline bool IsSPSource(JitSource* jitSource, Oid functionId)
{
    return ((jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) &&
            (jitSource->m_functionOid == functionId));
}

static bool PurgeJitSourceMapSP(
    JitNamespaceMapType* namespaceMap, Oid functionId, bool replaceFunction, bool cloneToLocalMap)
{
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if ((localMap == nullptr) && cloneToLocalMap) {
        MOT_LOG_TRACE("Failed to get session-local source map while purging by function");
        return false;
    }

    JitNamespaceMapType::iterator itr = namespaceMap->begin();
    while (itr != namespaceMap->end()) {
        JitSourceMapType& sourceMap = itr->second;
        JitSourceMapType::iterator sourceItr = sourceMap.begin();
        MOT_LOG_TRACE("Purging all invoke-query/sp JIT source objects under name-space [%s]", itr->first.c_str());
        while (sourceItr != sourceMap.end()) {
            JitSource* jitSource = sourceItr->second;
            MOT_LOG_DEBUG("Checking JIT source %p with source context %p, query: %s",
                jitSource,
                jitSource->m_sourceJitContext,
                jitSource->m_queryString);
            // we treat here only invoke queries and SPs
            // we clone to local session map only top-level INVOKE queries and directly invoked SP (this can include
            // also sub-SP)
            if (cloneToLocalMap &&
                (IsInvokeSPQuerySource(jitSource, functionId) || IsSPSource(jitSource, functionId))) {
                MOT_LOG_TRACE("Candidate clone JIT source %p with source context %p to local map: %s",
                    jitSource,
                    jitSource->m_sourceJitContext,
                    jitSource->m_queryString);
                // we clone only if it was not cloned before already
                JitSource* localSource =
                    FindCachedJitSource(jitSource->m_queryString, itr->first.c_str(), &localMap->m_namespaceMap);
                if (localSource == nullptr) {
                    MOT_LOG_TRACE("Cloning JIT source %p to local map: %s", jitSource, jitSource->m_queryString);
                    // trigger full invalidation for all session-local related contexts
                    JitSourceOp sourceOp = replaceFunction ? JitSourceOp::JIT_SOURCE_REPLACE_FUNCTION
                                                           : JitSourceOp::JIT_SOURCE_DROP_FUNCTION;
                    if (jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
                        sourceOp = JitSourceOp::JIT_SOURCE_INVALIDATE;
                    }
                    localSource = CloneLocalJitSource(jitSource, false, sourceOp);
                    if (localSource == nullptr) {
                        MOT_LOG_TRACE("Failed to clone invoke-query/sp JIT source");
                        return false;
                    }
                    if (!AddCachedJitSource(&localMap->m_namespaceMap, itr->first.c_str(), localSource)) {
                        MOT_LOG_TRACE("Failed to add invoke-query/sp JIT source to local map");
                        LockJitSource(jitSource);
                        MoveCurrentSessionContexts(localSource, jitSource, JitSourceOp::JIT_SOURCE_NO_OP);
                        UnlockJitSource(jitSource);
                        DestroyJitSource(localSource);
                        return false;
                    }
                }

                // make sure source in local map is purged and expired
                jitSource = localSource;
            }
            // when a DDL affects a global source we are done, since an empty local copy was made, and it will be
            // revalidated upon first attempt to access it
            // when a DDL affects a local map copy of SP we need to expire the source and all its queries
            if (cloneToLocalMap && IsSPSource(jitSource, functionId)) {
                SetJitSourceExpired(jitSource, 0, replaceFunction);
            }
            ++sourceItr;
        }
        ++itr;
    }
    return true;
}

static bool HasLocalJitFunctionSource(Oid functionOId, const char* nameSpace)
{
    MOT_LOG_TRACE(
        "Searching for function JIT source under %s name-space in local map for function %u", nameSpace, functionOId);
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if (localMap == nullptr) {
        MOT_LOG_TRACE("No local map found");
        return false;
    }

    JitNamespaceMapType::iterator itr = localMap->m_namespaceMap.find(nameSpace);
    if (itr == localMap->m_namespaceMap.end()) {
        MOT_LOG_TRACE("Name-space %s not found in local map", nameSpace);
        return false;
    }

    bool found = false;
    JitSourceMapType& sourceMap = itr->second;
    JitSourceMapType::iterator sourceItr = sourceMap.begin();
    while (sourceItr != sourceMap.end()) {
        JitSource* jitSource = sourceItr->second;
        if (jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) {
            if (jitSource->m_functionOid == functionOId) {
                found = true;
                break;
            }
        }
        ++sourceItr;
    }

    if (!found) {
        MOT_LOG_TRACE(
            "No function JIT source found under %s name-space in local map for function %u", nameSpace, functionOId);
    }

    return found;
}

static JitSource* CreateAndAddLocalJitSPSource(Oid functionId, const char* funcName)
{
    MOT::mot_string qualifiedFunctionName;
    if (!qualifiedFunctionName.format("%s.%u", funcName, functionId)) {
        MOT_LOG_TRACE("Failed to format qualified function name %s.%u", funcName, functionId);
        return nullptr;
    }

    MOT_LOG_TRACE("Trying to install function JIT source under global name-space to local map: %s",
        qualifiedFunctionName.c_str());

    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if (localMap == nullptr) {
        MOT_LOG_TRACE("Failed to get session-local map");
        return nullptr;
    }

    JitSource* localSource = AllocJitSource(qualifiedFunctionName.c_str(), JIT_CONTEXT_LOCAL);
    if (localSource == nullptr) {
        MOT_LOG_TRACE("Failed to allocated local JIT source for function: %s", qualifiedFunctionName.c_str());
        return nullptr;
    }

    localSource->m_contextType = JitContextType::JIT_CONTEXT_TYPE_FUNCTION;
    localSource->m_commandType = JIT_COMMAND_FUNCTION;
    localSource->m_functionOid = functionId;

    MOT_LOG_TRACE(
        "Created local jit-source object %p (contextType %s, commandType %s, functionOid %u) for function: %s",
        localSource,
        JitContextTypeToString(localSource->m_contextType),
        CommandToString(localSource->m_commandType),
        localSource->m_functionOid,
        qualifiedFunctionName.c_str());

    if (!AddCachedJitSource(&localMap->m_namespaceMap, MOT_JIT_GLOBAL_QUERY_NS, localSource)) {
        MOT_LOG_TRACE(
            "Failed to add local jit-source object %p to local map for function: %s", qualifiedFunctionName.c_str());
        DestroyJitSource(localSource);
        localSource = nullptr;
        return nullptr;
    }

    MOT_LOG_TRACE("Installed local jit-source object %p to local map for function: %s",
        localSource,
        qualifiedFunctionName.c_str());
    return localSource;
}

static void PurgeJitSourceMapSP(Oid functionId, bool replaceFunction, const char* funcName)
{
    // all SP changes are transactional so we must have a ready map
    MOT_LOG_TRACE("Purging JIT source map queries by function %s.%u (replaceFunction: %s)",
        funcName,
        functionId,
        replaceFunction ? "true" : "false");
    JitLocalSourceMap* localMap = GetOrCreateLocalSourceMap();
    if (localMap == nullptr) {
        MOT_LOG_ERROR("PurgeJitSourceMapSP: Failed to create session-local source map");
        return;
    }

    MOT_LOG_TRACE("Purging local map");
    (void)PurgeJitSourceMapSP(&localMap->m_namespaceMap, functionId, replaceFunction, false);

    // record operation for later pruning
    JitSPOperation spOp = replaceFunction ? JitSPOperation::JIT_SP_REPLACE : JitSPOperation::JIT_SP_DROP;
    localMap->m_operationMap[functionId] = spOp;
    MOT_LOG_TRACE("Recorded function %u operation: %s", functionId, replaceFunction ? "REPLACE" : "DROP")

    // now purge global map, such that each purged source is cloned and moved to the local map before purge/expire
    LockJitSourceMap();

    MOT_LOG_TRACE("Purging global map");
    if (!PurgeJitSourceMapSP(&g_jitSourceMap.m_namespaceMap, functionId, replaceFunction, true)) {
        MOT_LOG_ERROR("Failed to purge global source map by function %s.%u (replaceFunction: %s)",
            funcName,
            functionId,
            replaceFunction ? "true" : "false");
    }

    // Create a JIT function source in local map, if it was not cloned into the local map.
    if (!HasLocalJitFunctionSource(functionId, MOT_JIT_GLOBAL_QUERY_NS)) {
        JitSource* localSource = CreateAndAddLocalJitSPSource(functionId, funcName);
        if (localSource != nullptr) {
            // Mark the local source as DROPPED/REPLACED.
            SetJitSourceExpired(localSource, 0, replaceFunction);
        } else {
            MOT_LOG_ERROR("Failed to install local JIT source for function: %s.%u", funcName, functionId);
        }
    }

    UnlockJitSourceMap();

    // recycle deprecate sources that are no longer referenced
    CleanUpReadyDeprecateJitSources();
}

extern void PurgeJitSourceMap(
    uint64_t objectId, JitPurgeScope purgeScope, JitPurgeAction purgeAction, const char* funcName)
{
    if (purgeScope == JIT_PURGE_SCOPE_QUERY) {
        PurgeJitSourceMapQuery(objectId, purgeAction == JIT_PURGE_ONLY);
    } else {
        PurgeJitSourceMapSP(objectId, purgeAction == JIT_PURGE_REPLACE, funcName);
    }
}

extern bool HasJitFunctionSource(Oid functionId)
{
    bool result = false;
    LockJitSourceMap();
    JitNamespaceMapType::iterator itr = g_jitSourceMap.m_namespaceMap.begin();
    while (itr != g_jitSourceMap.m_namespaceMap.end()) {
        JitSourceMapType& sourceMap = itr->second;
        JitSourceMapType::iterator sourceItr = sourceMap.begin();
        while (sourceItr != sourceMap.end()) {
            JitSource* jitSource = sourceItr->second;
            if ((jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) &&
                (jitSource->m_sourceJitContext != nullptr)) {
                JitFunctionContext* functionContext = (JitFunctionContext*)jitSource->m_sourceJitContext;
                if (functionContext->m_functionOid == functionId) {
                    result = true;
                    break;
                }
            }
            ++sourceItr;
        }
        if (result) {
            break;
        }
        ++itr;
    }
    UnlockJitSourceMap();
    return result;
}

inline JitSource* CloneAndAddGlobalSource(JitSource* localSource, const char* nameSpace)
{
    JitSource* globalSource = CloneGlobalJitSource(localSource);
    if (globalSource == nullptr) {
        MOT_LOG_ERROR("Failed to clone global JIT source: %s", localSource->m_queryString);
    } else if (!AddCachedJitSource(&g_jitSourceMap.m_namespaceMap, nameSpace, globalSource)) {
        MOT_LOG_ERROR("Failed to add global JIT source: %s", localSource->m_queryString);
        DestroyJitSource(globalSource);
    } else {
        MOT_LOG_TRACE("Added source %p to global map under name-space %s: %s",
            globalSource,
            nameSpace,
            globalSource->m_queryString);
    }
    return globalSource;
}

static void HandleLocalJitSourceChanges(bool isCommit)
{
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if (localMap == nullptr) {
        return;
    }

    // for the sake of proper commit we acquire exclusive locks over all involved SPs
    LockJitSourceMap();
    JitSourceList deprecateList;
    JitSourceList pruneList;
    JitContextList rollbackInvokeContextList;
    JitNamespaceMapType::iterator itr = localMap->m_namespaceMap.begin();
    while (itr != localMap->m_namespaceMap.end()) {
        bool isGlobalNameSpace = (itr->first.compare(MOT_JIT_GLOBAL_QUERY_NS) == 0);
        JitSourceMapType& sourceMap = itr->second;
        JitSourceMapType::iterator sourceItr = sourceMap.begin();
        while (sourceItr != sourceMap.end()) {
            const char* nameSpace = itr->first.c_str();
            JitSource* localSource = sourceItr->second;
            MOT_LOG_TRACE(
                "Searching mapping from local source %p to global: %s", localSource, localSource->m_queryString);

            if (isCommit && !IsSimpleQuerySource(localSource)) {
                JitSPOperationMap::iterator opItr = localMap->m_operationMap.find(localSource->m_functionOid);
                if (opItr != localMap->m_operationMap.end()) {
                    // Set the current TxnId as the m_expireTxnId (TxnId which expired the JIT source).
                    localSource->m_expireTxnId = GetCurrentTransactionIdIfAny();
                }
            }

            JitCodegenState prevGlobalCodegenState = JitCodegenState::JIT_CODEGEN_NONE;
            JitSource* prevGlobalSource =
                FindCachedJitSource(localSource->m_queryString, nameSpace, &g_jitSourceMap.m_namespaceMap);
            JitSource* newGlobalSource = nullptr;
            if (prevGlobalSource == nullptr) {
                // if this is a roll-back then we need to discard the local source
                if (!isCommit) {
                    MOT_LOG_TRACE("Could not find global source during roll-back, discarding local source: %s",
                        localSource->m_queryString);
                } else {
                    MOT_LOG_TRACE("Could not find global source during commit, performing full clone: %s",
                        localSource->m_queryString);
                    newGlobalSource = CloneAndAddGlobalSource(localSource, nameSpace);
                }
            } else {
                // ATTENTION: we cannot merge into global source that might be in use concurrently (because the jitted
                // function cannot be deleted while it still executes on some other thread). This can happen only with
                // SP source or INVOKE source when function was replaced or recreated (in case of top-level query and
                // SP query, locks are taken before jitted function query context is invoked, so concurrent DDL cannot
                // take place). In this case we clone a new global source, and mark the old global source as
                // deprecated. This special flag orders any connected context to fully revalidate from scratch.
                // NOTE: We also deprecate expired SP queries (i.e. they were removed during drop/replace SP)
                LockJitSource(prevGlobalSource);
                prevGlobalCodegenState = prevGlobalSource->m_codegenState;
                if (isCommit && !IsSimpleQuerySource(prevGlobalSource)) {
                    // postpone deprecate until new source is installed
                    MOT_LOG_TRACE("Removing cached JIT source %u (%p) with state %s (TxnId %" PRIu64 "/%" PRIu64 ") "
                                  "during commit (%lu) of non-simple query: %s",
                        prevGlobalSource->m_sourceId,
                        prevGlobalSource,
                        JitCodegenStateToString(prevGlobalSource->m_codegenState),
                        prevGlobalSource->m_functionTxnId,
                        prevGlobalSource->m_expireTxnId,
                        GetCurrentTransactionIdIfAny(),
                        prevGlobalSource->m_queryString);
                    RemoveCachedGlobalJitSource(prevGlobalSource, nameSpace);
                    deprecateList.PushBackDeprecate(prevGlobalSource);
                    if ((prevGlobalSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) &&
                        (prevGlobalSource->m_codegenState == JitCodegenState::JIT_CODEGEN_UNAVAILABLE)) {
                        // Someone is still compiling. We avoid setting the state as deprecate, but still put it in the
                        // deprecated list.
                        // We raise the m_deprecatedPendingCompile early enough so that the session compiling will
                        // note this after compilation and change the codegen state to JIT_CODEGEN_DEPRECATE.
                        MOT_LOG_TRACE("Marking JIT source %p with status unavailable as deprecate pending compile: %s",
                            prevGlobalSource,
                            prevGlobalSource->m_queryString);
                        prevGlobalSource->m_deprecatedPendingCompile = 1;
                    }

                    MOT_LOG_TRACE("Cloning non-simple local source %u (%p) with state %s (TxnId: %" PRIu64 "/%" PRIu64
                                  ") during commit (%lu): %s",
                        localSource->m_sourceId,
                        localSource,
                        JitCodegenStateToString(localSource->m_codegenState),
                        localSource->m_functionTxnId,
                        localSource->m_expireTxnId,
                        GetCurrentTransactionIdIfAny(),
                        localSource->m_queryString);

                    // ATTENTION: for dropped committed functions, we still clone a global copy, but we later remove it
                    // and deprecate it, because this way we have a common way of handling all involved contexts
                    // (otherwise we need to move contexts, purge, invalidate, etc. - instead we gain it from adding a
                    // a new global source and removing it later - code is simpler and reusable this way)
                    // NOTE: newGlobalSource will be locked in MergeJitSource called from CloneGlobalJitSource,
                    // but this will not cause any lock ordering problem with prevGlobalSource lock taken above, since
                    // this is a newly created source and it will be added to the global map only after the call to
                    // CloneGlobalJitSource.
                    newGlobalSource = CloneAndAddGlobalSource(localSource, nameSpace);
                } else {
                    // no problem merging to any source after rollback, because the compiled function does not change.
                    // in case of commit of simple query, the old source context is deprecated so there is no problem
                    // of delete LLVM function while someone is still using it
                    if (!isCommit) {
                        MOT_LOG_TRACE("Merging local source %u back to global during roll-back: %s",
                            localSource->m_sourceId,
                            localSource->m_queryString);
                    } else {
                        MOT_LOG_TRACE("Merging local source %u back to global during commit of simple query: %s",
                            localSource->m_sourceId,
                            localSource->m_queryString);
                    }
                    MergeJitSource(localSource, prevGlobalSource, isCommit, &rollbackInvokeContextList);
                    if (!isCommit && isGlobalNameSpace) {
                        // restore from original global source all cached plan pointers of revalidate-fail use case
                        RestoreCachedPlanContexts(prevGlobalSource);
                    }
                }
                UnlockJitSource(prevGlobalSource);
            }

            // prune global function source name space, and deprecate dropped functions
            if ((newGlobalSource != nullptr) &&
                (newGlobalSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION)) {
                // attention: at this point all activity regarding this source is either cloned or merged
                if (newGlobalSource->m_codegenState == JitCodegenState::JIT_CODEGEN_READY) {
                    // we prune only ready functions that were actually replaced
                    JitSPOperationMap::iterator opItr = localMap->m_operationMap.find(newGlobalSource->m_functionOid);
                    if ((opItr != localMap->m_operationMap.end()) &&
                        (opItr->second == JitSPOperation::JIT_SP_REPLACE)) {
                        MOT_LOG_TRACE("Scheduling ready and replaced function %u for immediate pruning",
                            newGlobalSource->m_functionOid);
                        pruneList.PushBack(newGlobalSource);
                    } else {
                        MOT_LOG_TRACE("Ready but not replaced function %u not scheduled for pruning",
                            newGlobalSource->m_functionOid);
                    }
                } else if (newGlobalSource->m_codegenState == JitCodegenState::JIT_CODEGEN_DROPPED) {
                    // Newly installed dropped source should be scheduled to be deprecated as part of post commit
                    // cleanup, except if it is unavailable.
                    if ((prevGlobalSource == nullptr) ||
                        (prevGlobalCodegenState != JitCodegenState::JIT_CODEGEN_UNAVAILABLE)) {
                        MOT_LOG_TRACE(
                            "Scheduling for post commit cleanup of global dropped function source %u (%p) : %s",
                            newGlobalSource->m_sourceId,
                            newGlobalSource,
                            newGlobalSource->m_queryString);
                        MOT_ASSERT(isGlobalNameSpace);
                        char* queryString = newGlobalSource->m_queryString;
                        (void)localMap->m_droppedSPMap.insert(
                            JitSourceMapType::value_type(queryString, newGlobalSource));
                    }
                    pruneList.PushBack(newGlobalSource);
                }
            }

            if (!isCommit) {
                if (IsInvokeQuerySource(localSource)) {
                    // In case of rollback and if this is a invoke query, we must remove all the attached
                    // invoke contexts and destroy them before the local function source is deprecated.
                    // Otherwise, deprecation of local function source will fail.
                    JitQueryContext* queryContext = nullptr;
                    MotJitContext* currItr = localSource->m_contextList;
                    while (currItr != nullptr) {
                        queryContext = (JitQueryContext*)currItr;
                        if (queryContext->m_invokeContext != nullptr) {
                            rollbackInvokeContextList.PushBack(queryContext->m_invokeContext);
                            queryContext->m_invokeContext = nullptr;
                        }
                    }

                    if (localSource->m_sourceJitContext != nullptr) {
                        queryContext = (JitQueryContext*)localSource->m_sourceJitContext;
                        if (queryContext->m_invokeContext != nullptr) {
                            rollbackInvokeContextList.PushBack(queryContext->m_invokeContext);
                            queryContext->m_invokeContext = nullptr;
                        }
                    }
                }

                // NOTE: In case of rollback, we should unlink all the session-local contexts before deprecating and
                // destroying the local JIT source.
                UnlinkLocalJitSourceContexts(localSource);
            }

            // NOTE: In case of rollback of a invoke query source (SP re-create and rollback use case), the local
            // contexts will have an invoke context attached to the function source (which is also rolled-back).
            // If we try to deprecate the function source before the invoke query source, it will fail. So we defer
            // the local source deprecation as well.
            deprecateList.PushBackDeprecate(localSource);
            ++sourceItr;
        }
        sourceMap.clear();
        ++itr;
    }
    localMap->m_namespaceMap.clear();
    localMap->m_operationMap.clear();

    // prune name spaces of all dropped functions
    MOT_LOG_TRACE("Pruning name spaces of ready-replaced and dropped functions");
    JitSource* srcItr = pruneList.Begin();
    while (srcItr != nullptr) {
        JitSource* globalSource = srcItr;
        srcItr = srcItr->m_next;
        globalSource->m_next = nullptr;
        PruneNamespace(globalSource);
    }

    // cleanup leftovers (empty name spaces left after pruning)
    RemoveEmptyNamespaces();
    UnlockJitSourceMap();

    // Destroy all the collected local invoke contexts before deprecating the local sources.
    MOT_LOG_TRACE("Destroying all local invoke contexts during rollback");
    MotJitContext* contextItr = rollbackInvokeContextList.Begin();
    while (contextItr != nullptr) {
        MotJitContext* jitContext = contextItr;
        contextItr = contextItr->m_next;
        jitContext->m_next = nullptr;
        DestroyJitContext(jitContext);
    }

    // publish changes only after all changes have been recorded on the global map (avoid race where other session
    // contexts begin to revalidate before all changes have been recorded - i.e. make change atomic as possible)
    MOT_LOG_TRACE("Deprecating sources of dropped functions and local sources");
    srcItr = deprecateList.Begin();
    while (srcItr != nullptr) {
        JitSource* jitSource = srcItr;
        srcItr = srcItr->m_nextDeprecate;
        jitSource->m_nextDeprecate = nullptr;
        MarkAndAddDeprecateJitSource(jitSource, false);
    }
}

static const char* GetJitSourceNamespace(JitSource* jitSource)
{
    const char* result = nullptr;
    LockJitSourceMap();
    JitNamespaceMapType::iterator itr = g_jitSourceMap.m_namespaceMap.begin();
    while (itr != g_jitSourceMap.m_namespaceMap.end()) {
        JitSourceMapType& sourceMap = itr->second;
        JitSourceMapType::iterator sourceItr = sourceMap.find(jitSource->m_queryString);
        if (sourceItr != sourceMap.end()) {
            result = itr->first.c_str();
            break;
        }
        ++itr;
    }
    UnlockJitSourceMap();
    return result;
}

extern JitCodegenState RevalidateJitSourceTxn(
    JitSource* jitSource, TransactionId functionTxnId /* = InvalidTransactionId */)
{
    MOT_LOG_TRACE("Revalidating JIT source %p in transaction: %s", jitSource, jitSource->m_queryString);

    // if no DDL arrived yet in this transaction we act as in the normal case
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if (localMap == nullptr) {
        MOT_LOG_TRACE("No local map found, revalidation continues in non-transactional manner");
        return RevalidateJitSource(jitSource, functionTxnId);
    }

    // if this is a local source generated in a transaction then we revalidate in-place
    if (jitSource->m_usage == JIT_CONTEXT_LOCAL) {
        MOT_LOG_TRACE("JIT source is local, revalidation continues in non-transactional manner");
        return RevalidateJitSource(jitSource, functionTxnId);
    }

    // This is a global source and a local map exists, so we revalidate into the local map, as revalidation relies on
    // definitions found only in current transaction.
    const char* nameSpace = GetJitSourceNamespace(jitSource);
    if (nameSpace == nullptr) {
        MOT_LOG_TRACE("Failed to get JIT source %p name-space: %s", jitSource, jitSource->m_queryString);
        return JitCodegenState::JIT_CODEGEN_ERROR;
    }

    // We clone only if it was not cloned before already.
    JitSource* localSource = FindCachedJitSource(jitSource->m_queryString, nameSpace, &localMap->m_namespaceMap);
    if (localSource == nullptr) {
        // We first create a local clone of the JIT source add it to the local map, then we continue to revalidate from
        // the local source, such that revalidation is isolated in the current transaction.
        MOT_LOG_TRACE(
            "Transactional revalidation: Cloning jit-source %p to local map: %s", jitSource, jitSource->m_queryString);
        localSource = CloneLocalJitSource(jitSource, false, JitSourceOp::JIT_SOURCE_INVALIDATE);
        if (localSource == nullptr) {
            MOT_LOG_TRACE("Failed to clone JIT source");
            return JitCodegenState::JIT_CODEGEN_ERROR;
        }
        if (!AddCachedJitSource(&localMap->m_namespaceMap, nameSpace, localSource)) {
            MOT_LOG_TRACE("Failed to add JIT source to local map: %s", localSource->m_queryString);
            LockJitSource(jitSource);
            MoveCurrentSessionContexts(localSource, jitSource, JitSourceOp::JIT_SOURCE_NO_OP);
            UnlockJitSource(jitSource);
            DestroyJitSource(localSource);
            return JitCodegenState::JIT_CODEGEN_ERROR;
        }
    } else {
        MOT_LOG_TRACE("Transactional revalidation: Found jit-source %p in local map: %s",
            localSource,
            localSource->m_queryString);
        LockJitSource(jitSource);
        MoveCurrentSessionContexts(jitSource, localSource, JitSourceOp::JIT_SOURCE_NO_OP);
        UnlockJitSource(jitSource);
    }

    // Revalidate the local source.
    return RevalidateJitSource(localSource, functionTxnId);
}

extern void ApplyLocalJitSourceChanges()
{
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if ((localMap != nullptr) && !localMap->m_namespaceMap.empty()) {
        MOT_LOG_DEBUG("JIT-TXN: Applying local DDL changes to JIT");
        HandleLocalJitSourceChanges(true);
        MOT_LOG_DEBUG("JIT-TXN: DONE Applying local DDL changes to JIT");
    } else {
        MOT_LOG_DEBUG("JIT-TXN: Skipping commit notification in JIT source map: no changes were recorded");
    }
}

static void RecordDroppedSP(Oid functionOid, TimestampTz timestamp)
{
    LockDeprecateJitSources();
    (void)g_jitSourceMap.m_droppedSPOidMap.insert(JitSPOidMapType::value_type(functionOid, timestamp));
    UnlockDeprecateJitSources();
}

static void CleanupDroppedSPSources()
{
    MOT_LOG_TRACE("Deprecating sources of dropped functions (post commit cleanup)");
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if (localMap == nullptr || localMap->m_droppedSPMap.empty()) {
        return;
    }

    LockJitSourceMap();
    JitSourceMapType::iterator srcItr = localMap->m_droppedSPMap.begin();
    while (srcItr != localMap->m_droppedSPMap.end()) {
        // It is possible that the installed dropped source is deprecated and freed by other sessions. So we need to
        // get the dropped source again from the global map, then remove and deprecate it only if it is still the
        // same one we installed.
        JitSource* globalSource =
            FindCachedJitSource(srcItr->first.c_str(), MOT_JIT_GLOBAL_QUERY_NS, &g_jitSourceMap.m_namespaceMap);
        if (globalSource != nullptr && globalSource == srcItr->second) {
            LockJitSource(globalSource);
            MOT_ASSERT(globalSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION);
            bool isDropped = (globalSource->m_codegenState == JitCodegenState::JIT_CODEGEN_DROPPED) ? true : false;
            Oid functionOid = globalSource->m_functionOid;
            TimestampTz timestamp = globalSource->m_timestamp;
            UnlockJitSource(globalSource);
            if (isDropped) {
                MOT_LOG_TRACE("Removing and deprecating global dropped function source %u (%p): %s",
                    globalSource->m_sourceId,
                    globalSource,
                    globalSource->m_queryString);
                RemoveCachedGlobalJitSource(globalSource, MOT_JIT_GLOBAL_QUERY_NS);
                RecordDroppedSP(functionOid, timestamp);
                MarkAndAddDeprecateJitSource(globalSource, false);
            }
        }
        ++srcItr;
    }
    UnlockJitSourceMap();
    localMap->m_droppedSPMap.clear();
}

extern void PostCommitCleanupJitSources()
{
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if (localMap != nullptr) {
        if (!localMap->m_droppedSPMap.empty()) {
            MOT_LOG_DEBUG("JIT-TXN: Cleaning up dropped functions");
            CleanupDroppedSPSources();
            MOT_LOG_DEBUG("JIT-TXN: DONE Cleaning up dropped functions");
        }
        MOT_ASSERT(localMap->m_namespaceMap.empty());
        MOT_ASSERT(localMap->m_operationMap.empty());
        MOT_ASSERT(localMap->m_droppedSPMap.empty());
        DestroyLocalSourceMap();
    } else {
        MOT_LOG_DEBUG("JIT-TXN: Skipping post commit cleanup notification in JIT source map: no changes were recorded");
    }
}

extern void RevertLocalJitSourceChanges()
{
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if ((localMap != nullptr) && !localMap->m_namespaceMap.empty()) {
        MOT_LOG_DEBUG("JIT-TXN: Rolling-back local DDL changes in JIT");
        HandleLocalJitSourceChanges(false);
        MOT_LOG_DEBUG("JIT-TXN: DONE Rolling-back local DDL changes in JIT");
    } else {
        MOT_LOG_DEBUG("JIT-TXN: Skipping rollback notification in JIT source map: no changes were recorded");
    }
    DestroyLocalSourceMap();
}

extern void CleanupLocalJitSourceChanges()
{
    JitLocalSourceMap* localMap = GetLocalSourceMap();
    if ((localMap != nullptr) && !localMap->m_namespaceMap.empty()) {
        // we treat abrupt session disconnect as rollback
        MOT_LOG_DEBUG("JIT-TXN: Cleaning-up local DDL changes in JIT");
        HandleLocalJitSourceChanges(false);
        MOT_LOG_DEBUG("JIT-TXN: DONE Cleaning-up local DDL changes in JIT");
    } else {
        MOT_LOG_DEBUG("JIT-TXN: Skipping cleanup notification in JIT source map: no changes were recorded");
    }
    DestroyLocalSourceMap();
}

extern void ScheduleDeprecateJitSourceCleanUp(JitSource* jitSource)
{
    MOT_LOG_TRACE("Scheduling deprecate JIT source %p for recycling: %s", jitSource, jitSource->m_queryString);

    LockDeprecateJitSources();

    // We should not schedule for recycling if the JIT source is not in m_deprecateSources. Otherwise we will end up
    // in double free, if the JIT source is directly recycled in MarkAndAddDeprecateJitSource and someone tries to
    // recycle from m_readyDeprecateSources.
    JitSourceSet::iterator itr = g_jitSourceMap.m_deprecateSources.find(jitSource);
    if (itr != g_jitSourceMap.m_deprecateSources.end()) {
        bool inserted = g_jitSourceMap.m_readyDeprecateSources.insert(jitSource).second;
        if (!inserted) {
            MOT_LOG_TRACE("Cannot schedule JIT source %p for recycling, item already exists: %s",
                jitSource,
                jitSource->m_queryString);
        }
    } else {
        MOT_LOG_TRACE("Cannot schedule JIT source %p for recycling, item not in deprecate sources: %s",
            jitSource,
            jitSource->m_queryString);
    }

    UnlockDeprecateJitSources();
}

static bool AddCachedJitSource(
    JitNamespaceMapType* namespaceMap, const char* queryNamespace, JitSource* cachedJitSource)
{
    char* queryString = cachedJitSource->m_queryString;  // required due to compiler error

    // access, or insert and access
    JitNamespaceMapType::iterator itr = namespaceMap->find(queryNamespace);
    if (itr == namespaceMap->end()) {
        MOT_LOG_TRACE("Could not find name-space [%s], creating a new one", queryNamespace);
        JitSourceMapType sourceMap;
        (void)sourceMap.insert(JitSourceMapType::value_type(queryString, cachedJitSource));
        return namespaceMap->insert(JitNamespaceMapType::value_type(queryNamespace, sourceMap)).second;
    } else {
        JitSourceMapType& sourceMap = itr->second;  // g_jitSourceMap.m_namespaceMap[queryNamespace];
        std::pair<JitSourceMapType::iterator, bool> pairib =
            sourceMap.insert(JitSourceMapType::value_type(queryString, cachedJitSource));
        if (!pairib.second) {
            MOT_LOG_TRACE("Cannot add JIT source %p under name-space %s: entry %p already exists",
                cachedJitSource,
                queryNamespace,
                pairib.first->second);
        }
        return pairib.second;
    }
}

static JitSource* FindCachedJitSource(
    const char* queryString, const char* queryNamespace, JitNamespaceMapType* namespaceMap)
{
    JitSource* result = nullptr;
    JitNamespaceMapType::iterator itr = namespaceMap->find(queryNamespace);
    if (itr != namespaceMap->end()) {
        JitSourceMapType& sourceMap = itr->second;
        JitSourceMapType::iterator sourceItr = sourceMap.find(queryString);
        if (sourceItr != sourceMap.end()) {
            result = sourceItr->second;
        } else {
            MOT_LOG_TRACE("Could not find in name space [%s] query: %s", queryNamespace, queryString);
        }
    } else {
        MOT_LOG_TRACE("Could not find name space [%s] while searching for query: %s", queryNamespace, queryString);
    }
    return result;
}

static void RestoreCachedPlanContexts(JitSource* jitSource)
{
    LockJitSource(jitSource);
    if ((jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_READY) &&
        (jitSource->m_sourceJitContext != nullptr)) {
        CachedPlanSource* planSource = u_sess->pcache_cxt.first_saved_plan;
        while (planSource != nullptr) {
            if ((planSource->mot_jit_context == nullptr) &&
                (strcmp(planSource->query_string, jitSource->m_queryString) == 0)) {
                MOT_ASSERT(planSource->opFusionObj == NULL);
                planSource->mot_jit_context = CloneJitContext(jitSource->m_sourceJitContext, JIT_CONTEXT_LOCAL);
                if (planSource->mot_jit_context != nullptr) {
                    InvalidateJitContext(planSource->mot_jit_context, 0, JIT_CONTEXT_INVALID);
                }
            }
            planSource = planSource->next_saved;
        }
    }
    UnlockJitSource(jitSource);
}

inline bool DeprecateListContainsJitSource(JitSource* jitSource)
{
    bool result = false;
    JitSourceSet::iterator itr = g_jitSourceMap.m_deprecateSources.find(jitSource);
    if (itr != g_jitSourceMap.m_deprecateSources.end()) {
        result = true;
    }
    return result;
}

static void PruneSourceMap(JitSource* jitSource, JitSourceMapType& sourceMap)
{
    MOT_LOG_TRACE("Pruning source map: %s", jitSource->m_queryString);
    MOT_ASSERT(jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION);
    JitFunctionContext* functionContext = nullptr;
    if (jitSource->m_sourceJitContext != nullptr) {
        functionContext = (JitFunctionContext*)jitSource->m_sourceJitContext;
    }

    // for each query source in the source map, we try to locate its matching part in the source context
    JitSourceMapType::iterator sourceItr = sourceMap.begin();
    while (sourceItr != sourceMap.end()) {
        JitSource* querySource = sourceItr->second;
        bool sourceFound = false;
        if (functionContext != nullptr) {
            MOT_LOG_TRACE("Searching call site for JIT source: %s", querySource->m_queryString);
            for (uint32_t i = 0; i < functionContext->m_SPSubQueryCount; ++i) {
                JitCallSite* callSite = &functionContext->m_SPSubQueryList[i];
                if ((callSite->m_queryContext != nullptr) &&
                    (strcmp(callSite->m_queryContext->m_jitSource->m_queryString, querySource->m_queryString) == 0)) {
                    sourceFound = true;
                    break;
                }
            }
        }
        if (!sourceFound) {
            MOT_LOG_TRACE("Pruning JIT source %u in name-space %s: %s",
                querySource->m_sourceId,
                jitSource->m_queryString,
                querySource->m_queryString);
            // we do not wish to cause revalidation in currently executing sessions, so we just mark the source
            // silently as deprecate, knowing that their parent SP context is marked as invalid anyway
            MarkAndAddDeprecateJitSource(querySource, true);
            sourceItr = sourceMap.erase(sourceItr);
        } else {
            MOT_LOG_TRACE("Call site found, JIT source %u in name-space %s retained: %s",
                querySource->m_sourceId,
                jitSource->m_queryString,
                querySource->m_queryString);
            ++sourceItr;
        }
    }
}

void PruneNamespace(JitSource* jitSource)
{
    MOT_LOG_TRACE("Pruning name-space of source %p: %s", jitSource, jitSource->m_queryString)
    JitNamespaceMapType::iterator itr = g_jitSourceMap.m_namespaceMap.find(jitSource->m_queryString);
    if (itr != g_jitSourceMap.m_namespaceMap.end()) {
        JitSourceMapType& sourceMap = itr->second;
        PruneSourceMap(jitSource, sourceMap);
    } else {
        MOT_LOG_TRACE("Name-space not found in global map: %s", jitSource->m_queryString);
    }
}

// Not thread-safe, caller must hold the source map lock.
static void PruneJitSourceMap()
{
    // prune map from dropped sources, and SP sources that contain unused queries
    MOT_LOG_TRACE("Pruning global jit-source map");
    JitNamespaceMapType::iterator itr = g_jitSourceMap.m_namespaceMap.begin();
    while (itr != g_jitSourceMap.m_namespaceMap.end()) {
        if (itr->first.compare(MOT_JIT_GLOBAL_QUERY_NS) != 0) {
            JitSource* jitSource = GetCachedJitSource(itr->first.c_str(), true);
            if (jitSource != nullptr) {
                PruneNamespace(jitSource);
            }
        }
        ++itr;
    }
}

// Not thread-safe, caller must hold the source map lock.
static void RemoveEmptyNamespaces()
{
    // prune map from empty name spaces
    MOT_LOG_TRACE("Removing empty name spaces from the global JIT source map");
    JitNamespaceMapType::iterator itr = g_jitSourceMap.m_namespaceMap.begin();
    while (itr != g_jitSourceMap.m_namespaceMap.end()) {
        JitSourceMapType& sourceMap = itr->second;
        if (sourceMap.empty()) {
            MOT_LOG_TRACE("Removing empty name-space: %s", itr->first.c_str());
            itr = g_jitSourceMap.m_namespaceMap.erase(itr);
        } else {
            ++itr;
        }
    }
}

static void PurgeDeprecateQueryList(uint64_t relationId)
{
    LockDeprecateJitSources();
    MOT_LOG_TRACE("Purging deprecate JIT query sources by relation id: %" PRIu64, relationId);
    JitSourceSet::iterator itr = g_jitSourceMap.m_deprecateSources.begin();
    while (itr != g_jitSourceMap.m_deprecateSources.end()) {
        JitSource* jitSource = *itr;
        uint32_t tableId = jitSource->m_tableId;
        if ((jitSource->m_sourceJitContext != nullptr) &&
            (jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY)) {
            tableId = (uint32_t)((JitQueryContext*)jitSource->m_sourceJitContext)->m_tableId;
        }
        MOT_LOG_TRACE("Checking source %p (table id: %u): %s", jitSource, tableId, jitSource->m_queryString);
        LockJitSource(jitSource);
        if (IsSimpleQuerySource(jitSource) && JitSourceRefersRelation(jitSource, relationId, true)) {
            MOT_LOG_TRACE("Purging deprecate JIT source %p: %s", jitSource, jitSource->m_queryString);
            PurgeJitSource(jitSource, relationId);
        }
        UnlockJitSource(jitSource);
        ++itr;
    }
    UnlockDeprecateJitSources();
}

extern void MarkAndAddDeprecateJitSource(JitSource* jitSource, bool markOnly)
{
    MOT_LOG_TRACE("Trying to mark JIT source %p as deprecated and add it for cleanup (markOnly %u): %s",
        jitSource,
        markOnly,
        jitSource->m_queryString);
    bool recycleSource = false;
    LockJitSource(jitSource);
    DeprecateJitSource(jitSource, markOnly);
    (void)CleanUpDeprecateJitSourceContexts(jitSource);
    recycleSource = IsJitSourceRecyclable(jitSource);
    if ((!recycleSource) && (jitSource->m_usage == JIT_CONTEXT_LOCAL)) {
        // ATTENTION: Deprecated local JIT source should always be recycled directly, it should never be added to the
        // deprecate list.
        MOT_ASSERT(jitSource->m_codegenState != JitCodegenState::JIT_CODEGEN_UNAVAILABLE);
        MOT_LOG_WARN("Local %s JIT source still contains registered/deprecate JIT context objects before recycling: %s",
            JitCodegenStateToString(jitSource->m_codegenState),
            jitSource->m_queryString);
        MOT_LOG_TRACE("Local JIT source %p with state %s still contains contextList %p and deprecateContextList %p "
                      "before recycling: %s",
            jitSource,
            JitCodegenStateToString(jitSource->m_codegenState),
            jitSource->m_contextList,
            jitSource->m_deprecateContextList,
            jitSource->m_queryString);
        // NOTE: This should not happen, but in any case we should unlink all the session-local contexts before
        // destroying the local JIT source.
        MOT_ASSERT(0);
        UnlinkLocalJitSourceContexts(jitSource);
        recycleSource = true;
    }
    UnlockJitSource(jitSource);

    if (!recycleSource) {
        LockDeprecateJitSources();

        // Recheck for recycling after LockDeprecateJitSources() to avoid race with ScheduleDeprecateJitSourceCleanUp()
        // which is called from RemoveJitSourceContext().
        LockJitSource(jitSource);
        recycleSource = IsJitSourceRecyclable(jitSource);
        UnlockJitSource(jitSource);

        if (!recycleSource) {
            MOT_LOG_TRACE("JIT source still in use, adding to deprecate list: %s", jitSource->m_queryString);
            MOT_ASSERT(jitSource->m_usage != JIT_CONTEXT_LOCAL);
            MOT_ASSERT(!DeprecateListContainsJitSource(jitSource));
            if (!g_jitSourceMap.m_deprecateSources.insert(jitSource).second) {
                MOT_LOG_TRACE("Failed to insert JIT source %p to deprecate list, item already exists: %s",
                    jitSource,
                    jitSource->m_queryString);
            }
        }

        UnlockDeprecateJitSources();
    }

    if (recycleSource) {
        MOT_LOG_TRACE("Deprecate JIT source not in use, destroying: %s", jitSource->m_queryString);
        DestroyJitSource(jitSource);
    }
}

static void CleanUpReadyDeprecateJitSources()
{
    LockDeprecateJitSources();
    MOT_LOG_TRACE("Cleaning up ready deprecate JIT sources");
    JitSourceSet::iterator itr = g_jitSourceMap.m_readyDeprecateSources.begin();
    while (itr != g_jitSourceMap.m_readyDeprecateSources.end()) {
        JitSource* jitSource = *itr;
        MOT_LOG_TRACE("Deprecate JIT source %p ready for recycling: %s", jitSource, jitSource->m_queryString);
        JitSourceSet::iterator itr2 = g_jitSourceMap.m_deprecateSources.find(jitSource);
        if (itr2 != g_jitSourceMap.m_deprecateSources.end()) {
            (void)g_jitSourceMap.m_deprecateSources.erase(itr2);
        } else {
            MOT_LOG_TRACE("Could not find ready-for-recycle deprecate JIT source %p in pending list: %s",
                jitSource,
                jitSource->m_queryString);
        }
        DestroyJitSource(jitSource);
        ++itr;
    }
    g_jitSourceMap.m_readyDeprecateSources.clear();
    UnlockDeprecateJitSources();
}

static void CleanUpDeprecateJitSources()
{
    LockDeprecateJitSources();
    MOT_LOG_TRACE("Cleaning up deprecate JIT sources");
    JitSourceSet::iterator itr = g_jitSourceMap.m_deprecateSources.begin();
    while (itr != g_jitSourceMap.m_deprecateSources.end()) {
        JitSource* jitSource = *itr;
        bool recycleSource = false;
        LockJitSource(jitSource);
        if ((jitSource->m_contextList == nullptr) && (jitSource->m_deprecateContextList == nullptr) &&
            (jitSource->m_codegenState != JitCodegenState::JIT_CODEGEN_UNAVAILABLE)) {
            recycleSource = true;
        }
        UnlockJitSource(jitSource);
        if (recycleSource) {
            MOT_LOG_TRACE("Deprecate JIT source %p ready for recycling: %s", jitSource, jitSource->m_queryString);
            itr = g_jitSourceMap.m_deprecateSources.erase(itr);
            DestroyJitSource(jitSource);
        } else {
            ++itr;
        }
    }
    UnlockDeprecateJitSources();
}

extern bool IsDroppedSP(Oid functionOid)
{
    bool result = false;
    LockDeprecateJitSources();
    JitSPOidMapType::iterator itr = g_jitSourceMap.m_droppedSPOidMap.find(functionOid);
    if (itr != g_jitSourceMap.m_droppedSPOidMap.end()) {
        MOT_LOG_TRACE(
            "Function %u found in dropped functions map, time: %s", functionOid, timestamptz_to_str(itr->second));
        result = true;
    }
    UnlockDeprecateJitSources();
    return result;
}

extern void CleanupConcurrentlyDroppedSPSource(const char* queryString)
{
    MOT_LOG_TRACE(
        "Trying to cleanup after the JIT source was dropped concurrently during compilation: %s", queryString);

    // The transaction which dropped this stored procedure (when we were compiling) should have installed
    // a global source with JitCodegenState::JIT_CODEGEN_DROPPED. We can safely remove and deprecate that now.
    LockJitSourceMap();
    JitSource* globalSource = FindCachedJitSource(queryString, MOT_JIT_GLOBAL_QUERY_NS, &g_jitSourceMap.m_namespaceMap);
    if (globalSource != nullptr) {
        LockJitSource(globalSource);
        MOT_ASSERT(globalSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION);
        bool isDropped = (globalSource->m_codegenState == JitCodegenState::JIT_CODEGEN_DROPPED) ? true : false;
        Oid functionOid = globalSource->m_functionOid;
        TimestampTz timestamp = globalSource->m_timestamp;
        UnlockJitSource(globalSource);
        if (isDropped) {
            RemoveCachedGlobalJitSource(globalSource, MOT_JIT_GLOBAL_QUERY_NS);
            RecordDroppedSP(functionOid, timestamp);
            // Prune the dropped function source.
            MOT_LOG_TRACE("Pruning dropped function source %p: %s", globalSource, globalSource->m_queryString);
            PruneNamespace(globalSource);
            MarkAndAddDeprecateJitSource(globalSource, false);
        }
    }
    UnlockJitSourceMap();
}
}  // namespace JitExec
