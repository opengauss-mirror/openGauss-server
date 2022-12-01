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
 * jit_source_map.h
 *    Global JIT source map.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_source_map.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_SOURCE_MAP_H
#define JIT_SOURCE_MAP_H

#include "jit_source.h"

#define MOT_JIT_SOURCE_MAP_USE_RWLOCK

namespace JitExec {
/**
 * @brief Initializes the global JIT source map.
 * @return True if initialization succeeded, otherwise false.
 */
extern bool InitJitSourceMap();

/** @brief Destroys the global JIT source map. */
extern void DestroyJitSourceMap();

/** @brief Clears all entries in the global JIT source map, and releases all associated resources. */
extern void ClearJitSourceMap();

/**
 * @brief Locks the global JIT source map for synchronized access.
 */
#ifdef MOT_JIT_SOURCE_MAP_USE_RWLOCK
extern void LockJitSourceMap(bool readLock = false);
#else
extern void LockJitSourceMap();
#endif

/**
 * @brief Unlocks the global JIT source map for synchronized access.
 */
extern void UnlockJitSourceMap();

/**
 * @brief Retrieves the umber of cached JIT source objects in the global JIT source map. */
extern uint32_t GetJitSourceMapSize();

/** @brief Pushed a name-space on the JIT source name-space stack for the current session.*/
extern bool PushJitSourceNamespace(Oid functionId, const char* queryNamespace);

/** @brief Pops a name-space from the JIT source name-space stack for the current session.*/
extern void PopJitSourceNamespace();

/** @brief Retrieves the currently active name-space, or the global name-space. */
extern const char* GetActiveNamespace();

/** @brief Retrieves the function name of the currently active name-space, or invalid id if none. */
extern Oid GetActiveNamespaceFunctionId();

/**
 * @brief Retrieves a cached JIT source by its query string (not thread safe).
 * @param queryString The query string to search.
 * @param[opt] globalNamespace Specifies whether to search in the global name-space or the active name-space.
 * @return The cached JIT source or NULL if none was found or an error occurred.
 */
extern JitSource* GetCachedJitSource(const char* queryString, bool globalNamespace = false);

/**
 * @brief Adds a new JIT source to the cached source map (not thread safe).
 * @param cachedJitSource The cached JIT source to add. This object is expected to be empty, and serves
 * as a temporary stub until JIT code is fully generated. Other threads can wait for the source to be ready.
 * @param[opt] globalNamespace Specifies whether to add in the global name-space or the active name-space.
 * @return True if the JIT source is added successfully, otherwise false.
 */
extern bool AddCachedJitSource(JitSource* cachedJitSource, bool globalNamespace = false);

/**
 * @brief Removes the given JIT source from the global source map (not thread safe).
 * @param jitSource The JIT source to be removed.
 * @param nameSpace The JIT name space this source belongs to.
 */
extern void RemoveCachedGlobalJitSource(JitSource* jitSource, const char* nameSpace);

/**
 * @brief Retrieves a cached JIT source by its query string from the global source map (not thread safe).
 * @param queryString The query string to search.
 * @param nameSpace The JIT name space to search from.
 * @return The cached JIT source or NULL if none was found or an error occurred.
 */
extern JitSource* GetCachedGlobalJitSource(const char* queryString, const char* nameSpace);

/**
 * @brief Queries whether a ready cached JIT source exists for the given query string (thread safe).
 * @param queryString The query string to search.
 * @param globalNamespace Specifies whether to search in the global name-space or the active name-space.
 * @param allowUnavailable[opt] Specifies whether to consider unavailable source (pending compilation/revalidation to
 * finish) as a ready source.
 * @return True if a ready JIT source exists for the given query, otherwise false.
 */
extern bool ContainsReadyCachedJitSource(const char* queryString, bool globalNamespace, bool allowUnavailable = false);

/**
 * @brief Purges all entries associated with a relation or stored procedure, and optionally set them as expired (not
 * thread-safe, but called from DDL, so expected not to have any race).
 * @param objectId The external identifier of the relation or stored procedure that triggers the purge.
 * @param purgeScope The directly affected JIT source objects. In case of JIT query source, then the object identifier
 * parameter denotes a relation id, otherwise it denotes a stored procedure id.
 * @param purgeAction Specifies whether to just purge all keys/indexes referring to the given relation, or should the
 * JIT context also be set as expired (which triggers re-compilation of the JIT function on sub-sequent access).
 * @param funcName The stored procedure name (applicable only if purgeScope is JIT_PURGE_SCOPE_SP).
 */
extern void PurgeJitSourceMap(
    uint64_t objectId, JitPurgeScope purgeScope, JitPurgeAction purgeAction, const char* funcName);

/** @brief Get important JIT source information (thread-safe). */
extern MotJitDetail* GetJitSourceDetail(uint32_t* num);

/** @brief Queries whether the source map contains a function source by the given id. */
extern bool HasJitFunctionSource(Oid functionId);

/** @brief Revalidates a JIT source in a transactional-safe manner. */
extern JitCodegenState RevalidateJitSourceTxn(JitSource* jitSource, TransactionId functionTxnId = InvalidTransactionId);

/** @brief Apply current transaction changes. */
extern void ApplyLocalJitSourceChanges();

/** @brief Post commit cleanup of dropped SP sources (global) in current transaction. */
extern void PostCommitCleanupJitSources();

/** @brief Revert current transaction changes. */
extern void RevertLocalJitSourceChanges();

/** @brief Clean up current transaction changes (no apply neither revert). */
extern void CleanupLocalJitSourceChanges();

/** @brief Schedules cleanup for a deprecate JIT source that is ready to be recycled. */
extern void ScheduleDeprecateJitSourceCleanUp(JitSource* jitSource);

/** @brief Prunes name-space of the given JIT SP source. */
extern void PruneNamespace(JitSource* jitSource);

/** @brief Marks the JIT source as deprecate and adds it for cleanup. */
extern void MarkAndAddDeprecateJitSource(JitSource* jitSource, bool markOnly);

/** @brief Checks whether the given function Oid is recorded as dropped. */
extern bool IsDroppedSP(Oid functionOid);

/** @brief Performs cleanup after the JIT SP source was dropped concurrently. */
extern void CleanupConcurrentlyDroppedSPSource(const char* queryString);
}  // namespace JitExec

#endif
