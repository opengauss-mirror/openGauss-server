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
extern void LockJitSourceMap();

/**
 * @brief Unlocks the global JIT source map for synchronized access.
 */
extern void UnlockJitSourceMap();

/**
 * @brief Retrieves the umber of cached jit-source objects in the global JIT source map. */
extern uint32_t GetJitSourceMapSize();

/**
 * @brief Retrieves a cached jit-source by its query string (not thread safe).
 * @param queryString The query string to search.
 * @return The cached JIT source or NULL if none was found or an error occurred.
 */
extern JitSource* GetCachedJitSource(const char* queryString);

/**
 * @brief Adds a new JIT source to the cached source map (not thread safe).
 * @param cachedJitSource The cached JIT source to add. This object is expected to be empty, and serves
 * as a temporary stub until JIT code is fully generated. Other threads can wait for the source to be ready.
 */
extern bool AddCachedJitSource(JitSource* cachedJitSource);

/**
 * @brief Queries whether a ready cached jit-source exists for the given query string (thread safe).
 * @param queryString The query string to search.
 * @return True if a ready JIT source exists for the given query, otherwise false.
 */
extern bool ContainsReadyCachedJitSource(const char* queryString);

/**
 * @brief Marks all entries associated with a relation identifier as expired.
 * @param relationId The envelope identifier of modified relation.
 * @param purgeOnly Specifies whether to just purge all keys/indexes referring to the given relation, or should the JIT
 * context also be set as expired (which triggers re-compilation of the JIT function).
 */
extern void PurgeJitSourceMap(uint64_t relationId, bool purgeOnly);
}  // namespace JitExec

#endif
