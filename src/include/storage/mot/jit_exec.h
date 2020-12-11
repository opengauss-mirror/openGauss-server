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
 * jit_exec.h
 *    Interface for jit execution.
 *
 * IDENTIFICATION
 *    src/include/storage/mot/jit_exec.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_EXEC_H
#define JIT_EXEC_H

#include "postgres.h"
#include "nodes/params.h"
#include "executor/tuptable.h"
#include "nodes/parsenodes.h"

#include "jit_def.h"

namespace JitExec {

// forward declaration
struct JitContext;
struct JitContextPool;
struct JitPlan;

/** @brief Initializes the JIT module for MOT. */
extern bool JitInitialize();

/** @brief Destroys the JIT module for MOT. */
extern void JitDestroy();

/** @brief Queries whether MOT JIT compilation and execution is enabled. */
extern bool IsMotCodegenEnabled();

/** @brief Quereis whether pseudo-LLVM is forced on platforms where LLVM is natively supported. */
extern bool IsMotPseudoCodegenForced();

/** @brief Queries whether informative printing is enabled for MOT JIT compilation. */
extern bool IsMotCodegenPrintEnabled();

/** @brief Queries for the per-session limit of JIT queries. */
extern uint32_t GetMotCodegenLimit();

/**
 * @brief Queries whether a SQL query to be executed by MM Engine is jittable.
 * @param query The parsed SQL query to examine.
 * @param queryString The query text.
 * @return The JIT plan if the query is jittable, otherwise NULL.
 */
extern JitPlan* IsJittable(Query* query, const char* queryString);

/**
 * @brief Generate jitted code for a query.
 * @param query The parsed SQL query for which jitted code is to be generated.
 * @param queryString The query text.
 * @param jitPlan The JIT plan produced during the call to @ref IsJittable().
 * @return The context of the jitted code required for later execution.
 */
extern JitContext* JitCodegenQuery(Query* query, const char* queryString, JitPlan* jitPlan);

/** @brief Resets the scan iteration counter for the JIT context. */
extern void JitResetScan(JitContext* jitContext);

/**
 * @brief Executed a previously jitted query.
 * @param jitContext The context produced by a previous call to @ref JitCodegenQuery().
 * @param params The list of bound parameters passed to the query.
 * @param[out] slot The slot used for reporting select result.
 * @param[out] tuplesProcessed The variable used to report the number of processed tuples.
 * @param[out] scanEnded The variable used to report if a range scan ended.
 * @return Zero if succeeded, otherwise an error code.
 * @note This function may cause transaction abort.
 */
extern int JitExecQuery(
    JitContext* jitContext, ParamListInfo params, TupleTableSlot* slot, uint64_t* tuplesProcessed, int* scanEnded);

/**
 * @brief Purges the global cache of JIT source stencils from all entries that refer the given relation id.
 * @param relationId The external identifier of the relation to be purged.
 * @param purgeOnly Specifies whether to just purge all keys/indexes referring to the given relation, or should the JIT
 * context also be set as expired (which triggers re-compilation of the JIT function).
 */
extern void PurgeJitSourceCache(uint64_t relationId, bool purgeOnly);

// externalize functions defined elsewhere

/** @brief Destroys a jit context produced by a previous call to JitCodegenQuery. */
extern void DestroyJitContext(JitContext* jitContext);

/** @brief De-allocates a jit context pool for a specific session. */
extern void FreeSessionJitContextPool(JitContextPool* jitContextPool);

/** @brief Releases all resources associated with a plan.*/
extern void JitDestroyPlan(JitPlan* plan);
}  // namespace JitExec

#endif  // JIT_EXEC_H
