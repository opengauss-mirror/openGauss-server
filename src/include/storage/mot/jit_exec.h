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
#include "nodes/execnodes.h"
#include "pgstat.h"

#include "jit_def.h"

namespace JitExec {

// forward declaration
struct MotJitContext;
struct JitContextPool;
struct JitPlan;

/** @brief Initializes the JIT module for MOT. */
extern bool JitInitialize();

/** @brief Destroys the JIT module for MOT. */
extern void JitDestroy();

/** @brief Queries whether MOT JIT compilation and execution is enabled. */
extern bool IsMotCodegenEnabled();

/** @brief Queries whether MOT JIT compilation and execution of prepared queries is enabled. */
extern bool IsMotQueryCodegenEnabled();

/** @brief Queries whether MOT JIT compilation and execution of stored procedures is enabled. */
extern bool IsMotSPCodegenEnabled();

/** @brief Queries whether informative printing is enabled for MOT JIT compilation. */
extern bool IsMotCodegenPrintEnabled();

/** @brief Queries for the per-session limit of JIT queries. */
extern uint32_t GetMotCodegenLimit();

/**
 * @brief Queries whether a SQL query to be executed by MM Engine is jittable.
 * @param query The parsed SQL query to examine.
 * @param queryString The query text.
 * @param forcePlan[opt] Specifies whether to force plan generation, even if a JIT source exists.
 * @return The JIT plan if the query is jittable, otherwise null.
 */
extern JitPlan* IsJittableQuery(Query* query, const char* queryString, bool forcePlan = false);

/**
 * @brief Queries whether a stored procedure to be executed by MM Engine is jittable.
 * @param procTuple The stored procedure entry in the system catalog.
 * @param functionOid The function identifier.
 * @param forcePlan[opt] Specifies whether to force plan generation, even if a JIT source exists.
 * @return The  JIT plan if the stored procedure is jittable, otherwise null.
 */
extern JitPlan* IsJittableFunction(
    PLpgSQL_function* function, HeapTuple procTuple, Oid functionOid, bool forcePlan = false);

/** @brief Queries whether this is an invoke query that invokes an already jitted stored procedure. */
extern bool IsInvokeReadyFunction(Query* query);

/**
 * @brief Generate jitted code for a query.
 * @param query The parsed SQL query for which jitted code is to be generated.
 * @param queryString The query text.
 * @param jitPlan The JIT plan produced during the call to @ref IsJittable().
 * @param The required resulting context usage.
 * @return The context of the jitted code required for later execution.
 */
extern MotJitContext* JitCodegenQuery(Query* query, const char* queryString, JitPlan* jitPlan, JitContextUsage usage);

/**
 * @brief Utility helper for generating jitted code for a query. It packs together the calls for @ref IsJittableQuery
 * and @ref JitCodegenQuery.
 * @param query The parsed SQL query for which jitted code is to be generated.
 * @param queryString The query text.
 * @return The context of the jitted code required for later execution.
 */
extern MotJitContext* TryJitCodegenQuery(Query* query, const char* queryString);

/**
 * @brief Generate jitted code for a stored procedure.
 * @param function The parsed stored procedure.
 * @param procTuple The stored procedure entry in the system catalog.
 * @param functionOid The function identifier.
 * @param returnSetInfo Return set information for the function (required during parsing).
 * @param jitPlan The JIT plan produced during the call to @ref IsJittable().
 * @return The context of the jitted code required for later execution.
 */
extern MotJitContext* JitCodegenFunction(PLpgSQL_function* function, HeapTuple procTuple, Oid functionOid,
    ReturnSetInfo* returnSetInfo, JitPlan* jitPlan, JitContextUsage usage);

/** @brief Resets the scan iteration counter for the JIT context. */
extern void JitResetScan(MotJitContext* jitContext);

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
    MotJitContext* jitContext, ParamListInfo params, TupleTableSlot* slot, uint64_t* tuplesProcessed, int* scanEnded);

/**
 * @brief Executed a previously jitted stored procedure.
 * @param jitContext The context produced by a previous call to @ref JitCodegenFunction().
 * @param params The list of bound parameters passed to the query.
 * @param[out] slot The slot used for reporting select result.
 * @param[out] tuplesProcessed The variable used to report the number of processed tuples.
 * @param[out] scanEnded The variable used to report if a range scan ended.
 * @return Zero if succeeded, otherwise an error code.
 * @note This function may cause transaction abort.
 */
extern int JitExecFunction(
    MotJitContext* jitContext, ParamListInfo params, TupleTableSlot* slot, uint64_t* tuplesProcessed, int* scanEnded);

/**
 * @brief Purges the global cache of JIT source stencils from all entries that refer the given relation or stored
 * procedure id.
 * @param objectId The external identifier of the relation or stored procedure that triggers the purge.
 * @param purgeScope The directly affected JIT source objects. In case of JIT query source, then the object identifier
 * parameter denotes a relation id, otherwise it denotes a stored procedure id.
 * @param purgeAction Specifies whether to just purge all keys/indexes referring to the given relation, or should the
 * JIT context also be set as expired (which triggers re-compilation of the JIT function on sub-sequent access).
 * @param funcName The stored procedure name (applicable only if purgeScope is JIT_PURGE_SCOPE_SP).
 */
extern void PurgeJitSourceCache(
    uint64_t objectId, JitPurgeScope purgeScope, JitPurgeAction purgeAction, const char* funcName);

/**
 * @brief Re-Generate JIT code for all invalidated sub-queries of a stored procedure.
 * @param jitContext The context produced by a previous call to @ref JitCodegenFunction().
 * @return True if operations succeeded, otherwise false.
 */
extern bool JitReCodegenFunctionQueries(MotJitContext* jitContext);

// externalize functions defined elsewhere

/** @brief Forces JIT context full invalidation (purge and invalidate). */
extern void ForceJitContextInvalidation(MotJitContext* jitContext);

/** @brief Queries whether a JIT context is valid. */
extern bool IsJitContextValid(MotJitContext* jitContext);

/** @brief Queries whether a JIT context is a sub-context. */
extern bool IsJitSubContext(MotJitContext* jitContext);

/** @brief Trigger code-generation of any missing sub-query. */
extern bool TryRevalidateJitContext(MotJitContext* jitContext, TransactionId functionTxnId = InvalidTransactionId);

/** @brief Queries whether this context is still waiting for source compilation to finish. */
extern bool IsJitContextPendingCompile(MotJitContext* jitContext);

/** @brief Queries whether this context is done waiting for source compilation to finish. */
extern bool IsJitContextDoneCompile(MotJitContext* jitContext);

/** @brief Queries whether this compilation for this context finished with error. */
extern bool IsJitContextErrorCompile(MotJitContext* jitContext);

/** @brief Queries current JIT context compile state. Returns true if any of the 3 possible states is true.*/
extern bool GetJitContextCompileState(MotJitContext* jitContext, bool* isPending, bool* isDone, bool* isError);

/** @brief Updates and retrieves the current context state. */
extern JitContextState GetJitContextState(MotJitContext* jitContext);

/**
 * @brief Returns a JIT context back to its source pool.
 * @param jitContext The JIT context to free.
 */
extern void FreeJitContext(MotJitContext* jitContext);

/**
 * @brief Destroys a JIT context produced by a previous call to JitCodegenQuery.
 * @detail All internal resources associated with the context object are released, and the context
 * is returned to its source context pool.
 * @param jitContext The JIT context to destroy.
 * @param[optional] isDropCachedPlan Specifies whether this is from DropCachedPlan (deallocate prepared statement).
 */
extern void DestroyJitContext(MotJitContext* jitContext, bool isDropCachedPlan = false);

/** @brief De-allocates a JIT context pool for a specific session. */
extern void FreeSessionJitContextPool(JitContextPool* jitContextPool);

/**
 * @brief Releases all resources associated with a plan.
 * @param plan The plan to destroy.
 */
extern void JitDestroyPlan(JitPlan* plan);

/** @brief Get JIT global status.*/
extern MotJitDetail* MOTGetJitDetail(uint32_t* num);

/** @brief Get JIT profile data. */
extern MotJitProfile* MOTGetJitProfile(uint32_t* num);

/** @brief Reports sub-query parse error. */
extern void JitReportParseError(ErrorData* edata, const char* queryString);

/** @brief Cleans up all resource associated with JIT transactional behavior. */
extern void CleanupJitSourceTxnState();

/** @brief Queries whether this is an invoke query plan. */
extern bool IsInvokeQueryPlan(CachedPlanSource* planSource, Oid* functionOid, TransactionId* functionTxnId);
}  // namespace JitExec

#endif  // JIT_EXEC_H
