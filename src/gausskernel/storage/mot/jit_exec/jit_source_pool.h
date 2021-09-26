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
 * jit_source_pool.h
 *    Global pool of JIT sources.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_source_pool.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_SOURCE_POOL_H
#define JIT_SOURCE_POOL_H

#include "jit_source.h"

namespace JitExec {
/**
 * @brief Initializes the global pool of JIT sources.
 * @param poolSize The size of the pool.
 * @return True if initialization succeeded, otherwise false.
 */
extern bool InitJitSourcePool(uint32_t poolSize);

/** @brief Destroys the global pool of JIT sources. */
extern void DestroyJitSourcePool();

/**
 * @brief Allocates a JIT source from the pool.
 * @param queryString The query string for which a JIT source is to be allocated. Used to
 * re-initialize the pooled object.
 * @return The JIT source if allocation succeeded, otherwise NULL. Consult @ref
 * MOT::GetRootError() for further details.
 */
extern JitSource* AllocPooledJitSource(const char* queryString);

/**
 * @brief Returns a JIT source to the pool.
 * @param jitSource The JIT source to return.
 */
extern void FreePooledJitSource(JitSource* jitSource);
}  // namespace JitExec

#endif
