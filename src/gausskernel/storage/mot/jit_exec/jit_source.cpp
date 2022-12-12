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
 * jit_source.cpp
 *    A stencil used for caching a compiled function and cloning other context objects with identical query.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_source.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "jit_source.h"
#include "utilities.h"
#include "utils/memutils.h"
#include "jit_statistics.h"
#include "debug_utils.h"
#include "mm_global_api.h"
#include "jit_common.h"
#include "jit_plan.h"
#include "storage/mot/jit_exec.h"
#include "jit_llvm_query_codegen.h"
#include "jit_plan_sp.h"
#include "jit_llvm_sp_codegen.h"
#include "jit_source_pool.h"
#include "jit_source_map.h"
#include "mot_list.h"

#include "executor/spi.h"

namespace JitExec {
DECLARE_LOGGER(JitSource, JitExec);

// forward declarations
static char* CloneQueryString(const char* queryString, JitContextUsage usage);
static void FreeQueryString(char* queryString, JitContextUsage usage);
static void SetJitSourceStatus(JitSource* jitSource, MotJitContext* readySourceJitContext, JitCodegenState state,
    JitCodegenStats* codegenStats = nullptr, int errorCode = 0, uint64_t relationId = 0, bool* invalidState = nullptr,
    bool functionReplace = false, JitCodegenState* newState = nullptr);
static void JitSourcePurgeContextList(JitSource* jitSource, uint64_t relationId);
static void JitSourceInformCompileDone(JitSource* jitSource, JitSourceList* parentSourceList = nullptr);
static void JitSourceInformCompileError(JitSource* jitSource, JitSourceList* parentSourceList = nullptr);
static void JitSourceListInformCompileDone(JitSourceList* sourceList, const char* queryString);
static void JitSourceListInformCompileError(JitSourceList* sourceList, const char* queryString);
static void JitSourceInvalidateContextList(JitSource* jitSource, uint64_t relationId);
static void JitSourceDeprecateContextList(JitSource* jitSource);
static void RemoveJitSourceContextImpl(JitSource* jitSource, MotJitContext* jitContext);
static bool JitQueryContextRefersSP(JitQueryContext* jitContext, Oid functionId);
static bool JitFunctionContextRefersSP(JitFunctionContext* jitContext, Oid functionId);
static Query* GetSourceQuery(const char* queryString);
static bool JitCodegenQueryInplace(Query* query, const char* queryString, JitPlan* jitPlan, JitSource* jitSource);
static JitCodegenState RegenerateQuery(JitSource* jitSource);
static JitCodegenState ReinstateReadyState(JitSource* jitSource);
static JitCodegenState RevalidateJitInvokeQuery(JitSource* jitSource);
static bool RevalidateJitFunction(JitSource* jitSource);
static JitCodegenState RevalidateJitQuerySource(JitSource* jitSource, JitCodegenState prevState);
static JitCodegenState RevalidateJitFunctionSource(JitSource* jitSource, JitCodegenState prevState);
static bool CopyJitSource(
    JitSource* source, JitSource* target, bool cloneContext, JitContextUsage usage, JitSourceOp sourceOp);
static void DeprecateSourceJitContext(JitSource* jitSource, MotJitContext** recyclableContext);
static void UnlinkDeprecateJitContext(JitSource* jitSource, MotJitContext* sourceJitContext);

#ifdef MOT_DEBUG
static bool DeprecateListContainsJitContext(JitSource* jitSource, MotJitContext* jitContext);
static bool ContextListContainsJitContext(JitSource* jitSource, MotJitContext* jitContext);
static void VerifyJitSourceStateTrans(
    JitSource* jitSource, MotJitContext* readySourceJitContext, JitCodegenState state);
#define MOT_JIT_SOURCE_VERIFY_STATE(jitSource, readySourceJitContext, state) \
    VerifyJitSourceStateTrans(jitSource, readySourceJitContext, state)
#else
#define MOT_JIT_SOURCE_VERIFY_STATE(jitSource, readySourceJitContext, state)
#endif

static TimestampTz GetCurrentTimestamp()
{
    return OidFunctionCall0Coll(2649, InvalidOid);  // clock_timestamp
}

extern bool InitJitSource(JitSource* jitSource, const char* queryString, JitContextUsage usage)
{
    jitSource->m_queryString = nullptr;
    jitSource->m_sourceJitContext = nullptr;
    jitSource->m_initialized = 0;

    jitSource->m_pendingChildCompile = 0;
    jitSource->m_codegenState = JitCodegenState::JIT_CODEGEN_UNAVAILABLE;
    jitSource->m_status = JitSourceStatus::JIT_SOURCE_INVALID;
    jitSource->m_contextType = JitContextType::JIT_CONTEXT_TYPE_INVALID;
    jitSource->m_commandType = JIT_COMMAND_INVALID;
    jitSource->m_usage = usage;
    jitSource->m_deprecatedPendingCompile = 0;
    jitSource->m_timestamp = GetCurrentTimestamp();
    jitSource->m_next = nullptr;
    jitSource->m_contextList = nullptr;
    jitSource->m_functionOid = InvalidOid;
    jitSource->m_functionTxnId = 0;
    jitSource->m_expireTxnId = 0;
    jitSource->m_tableId = 0;
    jitSource->m_innerTableId = 0;
    jitSource->m_deprecateContextList = nullptr;
    jitSource->m_codegenStats = {};

    if (queryString == nullptr) {
        jitSource->m_queryString = nullptr;
    } else {
        jitSource->m_queryString = CloneQueryString(queryString, usage);
        if (jitSource->m_queryString == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Code Generation", "Failed to clone query string");
            return false;
        }
    }

    // we need a recursive mutex, due to complexities when registering context for cleanup (see call to
    // CloneJitContext() inside WaitJitContextReady())
    // note: even though a local JIT source does not need a lock, we still create it because the local source is used
    // in many functions requiring a lock and condition variable
    pthread_mutexattr_t attr;
    int res = pthread_mutexattr_init(&attr);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(
            res, pthread_mutex_init, "Code Generation", "Failed to init mutex attributes for jit-source");
        FreeQueryString(jitSource->m_queryString, usage);
        jitSource->m_queryString = nullptr;
        return false;
    }
    (void)pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);

    res = pthread_mutex_init(&jitSource->m_lock, &attr);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(
            res, pthread_mutex_init, "Code Generation", "Failed to create mutex for jit-source");
        FreeQueryString(jitSource->m_queryString, usage);
        jitSource->m_queryString = nullptr;
        return false;
    }

    jitSource->m_initialized = 1;
    return true;
}

static bool CopyJitSource(
    JitSource* source, JitSource* target, bool cloneContext, JitContextUsage usage, JitSourceOp sourceOp)
{
    // clone the source context if required
    target->m_sourceJitContext = nullptr;
    if (cloneContext && (source->m_sourceJitContext != nullptr)) {
        target->m_sourceJitContext = CloneJitContext(source->m_sourceJitContext, usage);
        if (target->m_sourceJitContext == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Process DDL event", "Failed to clone source JIT context");
            return false;
        }
        RemoveJitSourceContext(source, target->m_sourceJitContext);
        target->m_sourceJitContext->m_jitSource = target;
        PurgeJitContext(target->m_sourceJitContext, 0);  // ensure we work with correct table/index objects
    }

    // clone the query string
    target->m_queryString = CloneQueryString(source->m_queryString, usage);
    if (target->m_queryString == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Process DDL event", "Failed to clone query string");
        return false;
    }

    // clone the rest of the members
    target->m_commandType = source->m_commandType;
    if (cloneContext) {
        target->m_codegenState = source->m_codegenState;
    } else {
        if (sourceOp == JitSourceOp::JIT_SOURCE_PURGE_ONLY) {
            target->m_codegenState = source->m_codegenState;
        } else if (sourceOp == JitSourceOp::JIT_SOURCE_INVALIDATE) {
            target->m_codegenState = JitCodegenState::JIT_CODEGEN_EXPIRED;
            target->m_status = JitSourceStatus::JIT_SOURCE_INVALID;  // move to transient state
        } else if (sourceOp == JitSourceOp::JIT_SOURCE_REPLACE_FUNCTION) {
            target->m_codegenState = JitCodegenState::JIT_CODEGEN_REPLACED;
            target->m_status = JitSourceStatus::JIT_SOURCE_INVALID;  // move to transient state
        } else if (sourceOp == JitSourceOp::JIT_SOURCE_DROP_FUNCTION) {
            target->m_codegenState = JitCodegenState::JIT_CODEGEN_DROPPED;
        }
    }

    target->m_contextType = source->m_contextType;
    target->m_timestamp = GetCurrentTimestamp();
    target->m_functionOid = source->m_functionOid;
    target->m_functionTxnId = source->m_functionTxnId;
    target->m_expireTxnId = source->m_expireTxnId;

    // move current session local contexts to target
    MoveCurrentSessionContexts(source, target, sourceOp);
    return true;
}

static void CleanJitSource(JitSource* jitSource)
{
    MOT_LOG_DEBUG("Cleaning JIT source %p with query string: %s",
        jitSource,
        (jitSource->m_queryString != nullptr) ? jitSource->m_queryString : "NULL");

    if (jitSource->m_contextList != nullptr) {
        MOT_LOG_WARN("JIT source %p still contains registered JIT context objects %p, while cleaning JIT source with "
                     "query string: %s",
            jitSource,
            jitSource->m_contextList,
            (jitSource->m_queryString != nullptr) ? jitSource->m_queryString : "NULL");
        MotJitContext* jitContext = jitSource->m_contextList;
        while (jitContext != nullptr) {
            MOT_LOG_DEBUG("JIT context %p still found in JIT source %p", jitContext, jitSource);
            jitContext->m_jitSource = nullptr;  // prevent crash during DestroyJitContext()
            jitContext = jitContext->m_nextInSource;
        }
        jitSource->m_contextList = nullptr;
    }

    if (jitSource->m_deprecateContextList != nullptr) {
        MOT_LOG_WARN("JIT source %p still contains deprecate JIT context objects %p, while cleaning JIT source with "
                     "query string: %s",
            jitSource,
            jitSource->m_deprecateContextList,
            (jitSource->m_queryString != nullptr) ? jitSource->m_queryString : "NULL");
        MotJitContext* jitContext = jitSource->m_deprecateContextList;
        while (jitContext != nullptr) {
            MOT_LOG_DEBUG("Deprecate JIT context %p still found in JIT source %p", jitContext, jitSource);
            jitContext->m_jitSource = nullptr;  // prevent crash during DestroyJitContext()
            jitContext = jitContext->m_nextDeprecate;
        }
        jitSource->m_deprecateContextList = nullptr;
    }

    if (jitSource->m_sourceJitContext != nullptr) {
        // this code is executed during shutdown, so the underlying
        // GsCodeGen object was already destroyed by the top memory context
        jitSource->m_sourceJitContext->m_jitSource = nullptr;  // suppress warning
        DestroyJitContext(jitSource->m_sourceJitContext);
        jitSource->m_sourceJitContext = nullptr;
    }
    if (jitSource->m_queryString != nullptr) {
        FreeQueryString(jitSource->m_queryString, jitSource->m_usage);
        jitSource->m_queryString = nullptr;
    }
}

extern void DestroyJitSource(JitSource* jitSource)
{
    MOT_LOG_DEBUG("Destroying JIT source %p with query string: %s",
        jitSource,
        (jitSource->m_queryString != nullptr) ? jitSource->m_queryString : "NULL");

    CleanJitSource(jitSource);
    if (jitSource->m_initialized) {
        (void)pthread_mutex_destroy(&jitSource->m_lock);
        jitSource->m_initialized = 0;
    }

    FreeJitSource(jitSource);
}

extern void ReInitJitSource(JitSource* jitSource, const char* queryString, JitContextUsage usage)
{
    jitSource->m_usage = usage;
    jitSource->m_queryString = CloneQueryString(queryString, usage);
    jitSource->m_sourceJitContext = NULL;
    jitSource->m_commandType = JIT_COMMAND_INVALID;
    jitSource->m_codegenState = JitCodegenState::JIT_CODEGEN_UNAVAILABLE;
    jitSource->m_status = JitSourceStatus::JIT_SOURCE_INVALID;
    jitSource->m_contextType = JitContextType::JIT_CONTEXT_TYPE_INVALID;
    jitSource->m_timestamp = GetCurrentTimestamp();
    jitSource->m_next = nullptr;
    jitSource->m_pendingChildCompile = 0;
    jitSource->m_deprecatedPendingCompile = 0;
    jitSource->m_functionOid = 0;
    jitSource->m_functionTxnId = 0;
    jitSource->m_expireTxnId = 0;
    jitSource->m_tableId = 0;
    jitSource->m_innerTableId = 0;
    jitSource->m_deprecateContextList = nullptr;
    jitSource->m_codegenStats = {};
}

extern void LockJitSource(JitSource* jitSource)
{
    (void)pthread_mutex_lock(&jitSource->m_lock);
}

extern void UnlockJitSource(JitSource* jitSource)
{
    (void)pthread_mutex_unlock(&jitSource->m_lock);
}

static inline bool IsInvalidatedCodegenState(JitCodegenState codegenState)
{
    if ((codegenState == JitCodegenState::JIT_CODEGEN_EXPIRED) ||
        (codegenState == JitCodegenState::JIT_CODEGEN_DROPPED) ||
        (codegenState == JitCodegenState::JIT_CODEGEN_REPLACED) ||
        (codegenState == JitCodegenState::JIT_CODEGEN_DEPRECATE)) {
        return true;
    }
    return false;
}

extern bool IsPrematureRevalidation(JitSource* jitSource, TransactionId functionTxnId)
{
    (void)pthread_mutex_lock(&jitSource->m_lock);
    MOT_ASSERT((jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) ||
               ((jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) &&
                   (jitSource->m_commandType == JIT_COMMAND_INVOKE)));
    if ((jitSource->m_functionTxnId > functionTxnId) || (jitSource->m_expireTxnId > functionTxnId) ||
        ((jitSource->m_functionTxnId == functionTxnId) && (jitSource->m_expireTxnId != InvalidTransactionId) &&
            IsInvalidatedCodegenState(jitSource->m_codegenState))) {
        MOT_LOG_TRACE("Rejecting premature re-validation attempt (Function %u TxnId from %" PRIu64 "/%" PRIu64
                      " to %" PRIu64 ") of JIT source %p with state %s for query: %s",
            jitSource->m_functionOid,
            jitSource->m_functionTxnId,
            jitSource->m_expireTxnId,
            functionTxnId,
            jitSource,
            JitCodegenStateToString(jitSource->m_codegenState),
            jitSource->m_queryString);
        (void)pthread_mutex_unlock(&jitSource->m_lock);
        return true;
    }

    MOT_LOG_TRACE("Valid re-validation attempt (Function %u TxnId from %" PRIu64 "/%" PRIu64 " to %" PRIu64 ") of JIT "
                  "source %p with state %s for query: %s",
        jitSource->m_functionOid,
        jitSource->m_functionTxnId,
        jitSource->m_expireTxnId,
        functionTxnId,
        jitSource,
        JitCodegenStateToString(jitSource->m_codegenState),
        jitSource->m_queryString);
    (void)pthread_mutex_unlock(&jitSource->m_lock);
    return false;
}

extern bool IsPrematureRevalidation(JitSource* jitSource, JitPlan* jitPlan)
{
    if (jitPlan == MOT_READY_JIT_PLAN) {
        MOT_LOG_TRACE("Rejecting possible premature re-validation attempt (Function %u TxnId %" PRIu64 "/%" PRIu64 ") "
                      "of JIT %p with state %s for query: %s",
            jitSource->m_functionOid,
            jitSource->m_functionTxnId,
            jitSource->m_expireTxnId,
            jitSource,
            JitCodegenStateToString(jitSource->m_codegenState),
            jitSource->m_queryString);
        return true;
    }

    JitFunctionPlan* functionPlan = nullptr;
    if (JitPlanHasFunctionPlan(jitPlan, &functionPlan)) {
        if (IsPrematureRevalidation(jitSource, functionPlan->m_function->fn_xmin)) {
            return true;
        }
    }

    return false;
}

static TransactionId GetFunctionTxnIdFromPlan(JitPlan* jitPlan)
{
    TransactionId functionTxnId = InvalidTransactionId;
    if (jitPlan != MOT_READY_JIT_PLAN) {
        JitFunctionPlan* functionPlan = nullptr;
        if (JitPlanHasFunctionPlan(jitPlan, &functionPlan)) {
            functionTxnId = functionPlan->m_function->fn_xmin;
        }
    }
    return functionTxnId;
}

extern JitCodegenState GetReadyJitContext(
    JitSource* jitSource, MotJitContext** readySourceJitContext, JitContextUsage usage, JitPlan* jitPlan)
{
    MOT_LOG_TRACE(
        "Trying to get ready JIT context from JIT source %p for query: %s", jitSource, jitSource->m_queryString);

    *readySourceJitContext = nullptr;

    (void)pthread_mutex_lock(&jitSource->m_lock);

    // If source was deprecated we abort
    if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_DEPRECATE) {
        (void)pthread_mutex_unlock(&jitSource->m_lock);
        MOT_LOG_TRACE("Aborting codegen in deprecate JIT source %p: %s", jitSource, jitSource->m_queryString);
        return JitCodegenState::JIT_CODEGEN_DEPRECATE;
    }

    if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_DROPPED) {
        (void)pthread_mutex_unlock(&jitSource->m_lock);
        MOT_LOG_TRACE("Aborting codegen in dropped JIT source %p: %s", jitSource, jitSource->m_queryString);
        return JitCodegenState::JIT_CODEGEN_DROPPED;
    }

    // If someone else is operating on the source then we just inform the caller
    // that the code is being generated by returning JitCodegenState::JIT_CODEGEN_UNAVAILABLE.
    if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_UNAVAILABLE) {
        MOT_LOG_TRACE("Backing-off in unavailable JIT source %p: %s", jitSource, jitSource->m_queryString);
        (void)pthread_mutex_unlock(&jitSource->m_lock);
        return JitCodegenState::JIT_CODEGEN_UNAVAILABLE;
    }

    // if the source was previously marked as pending for child compilation to finish, then check again
    if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_PENDING) {
        if (jitSource->m_pendingChildCompile == JIT_CONTEXT_PENDING_COMPILE) {
            MOT_LOG_TRACE("Backing-off in pending JIT source %p: %s", jitSource, jitSource->m_queryString);
            (void)pthread_mutex_unlock(&jitSource->m_lock);
            return JitCodegenState::JIT_CODEGEN_PENDING;
        } else {
            // whether done or error, just reset and retry
            MOT_LOG_TRACE("Child compile done/error in pending JIT source %p: %s", jitSource, jitSource->m_queryString);
            // call to revalidate below should eventually reset the pending flag
        }
    }

    JitCodegenState result = jitSource->m_codegenState;
    if ((jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_EXPIRED) ||
        (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_REPLACED) ||
        (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_ERROR)) {
        if (IsPrematureRevalidation(jitSource, jitPlan)) {
            MOT_LOG_TRACE("JIT source %p is %s, skipping premature re-validation: %s",
                jitSource,
                JitCodegenStateToString(jitSource->m_codegenState),
                jitSource->m_queryString);
            // Premature re-validation attempt: We don't change the codegen state.
            // We return to caller status-deprecate, this way the caller will abort this codegen attempt.
            result = JitCodegenState::JIT_CODEGEN_DEPRECATE;
        } else {
            // Special case: We return to caller status-expired and change internal status to unavailable, this way the
            // caller will regenerate code for the query.
            MOT_LOG_TRACE("JIT source %p moving from state %s to UNAVAILABLE/EXPIRED: %s",
                jitSource,
                JitCodegenStateToString(jitSource->m_codegenState),
                jitSource->m_queryString);
            jitSource->m_codegenState = JitCodegenState::JIT_CODEGEN_UNAVAILABLE;
            result = JitCodegenState::JIT_CODEGEN_EXPIRED;
        }
    } else if ((jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_READY) ||
               (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_PENDING)) {
        // check if we need to revalidate the source context, this can happen with INVOKE context (all query context
        // source objects are destroyed when their source expires).
        // The relevant use case here is PREPARE after invalidate.
        MOT_LOG_TRACE("JIT source %p is ready: %s", jitSource, jitSource->m_queryString);
        bool needRevalidate = ((MOT_ATOMIC_LOAD(jitSource->m_sourceJitContext->m_validState) != JIT_CONTEXT_VALID) ||
                               (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_PENDING));
        if (needRevalidate) {
            // we must unlock in order to avoid deadlock, since revalidation may take source map lock when some SP or
            // sub-query needs full code regeneration (so IsJittableFunction/Query() calls
            // ContainsReadyCachedJitSource() which locks the source map).
            MOT_LOG_TRACE("JIT source %p needs revalidation: %s", jitSource, jitSource->m_queryString);
            (void)pthread_mutex_unlock(&jitSource->m_lock);

            TransactionId functionTxnId = GetFunctionTxnIdFromPlan(jitPlan);

            // this is ugly in terms of dependencies, but we must revalidate in a transactional manner
            result = RevalidateJitSourceTxn(jitSource, functionTxnId);

            (void)pthread_mutex_lock(&jitSource->m_lock);
            result = jitSource->m_codegenState;  // get value again after re-lock due to possible concurrent deprecate
            // We should not try to regenerate code again in this attempt, even if we failed to revalidate.
            MOT_ASSERT(result != JitCodegenState::JIT_CODEGEN_EXPIRED);
        }

        // we need to re-fetch table and index objects in case TRUNCATE TABLE was issued
        if ((result == JitCodegenState::JIT_CODEGEN_READY) && !RefetchTablesAndIndices(jitSource->m_sourceJitContext)) {
            MOT_LOG_TRACE(
                "Failed to re-fetch index objects for source %p with query: %s", jitSource, jitSource->m_queryString);
            SetJitSourceError(jitSource, MOT_ERROR_CONCURRENT_MODIFICATION, &result);
        }
        if (result == JitCodegenState::JIT_CODEGEN_READY) {
            // attention: following call also makes a call to AddJitSourceContext(), which takes lock again
            *readySourceJitContext = CloneJitContext(jitSource->m_sourceJitContext, usage);
            if (*readySourceJitContext != nullptr) {
                MOT_LOG_TRACE(
                    "Registered JIT context %p in JIT source %p for cleanup", *readySourceJitContext, jitSource);
                JitStatisticsProvider::GetInstance().AddCodeCloneQuery();
            } else {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Generate JIT Code",
                    "Failed to clone ready source context %p",
                    jitSource->m_sourceJitContext);
                JitStatisticsProvider::GetInstance().AddCodeCloneErrorQuery();
            }
        }
    }

    MOT_LOG_TRACE("JIT source %p: Found %s context %p with query %s (result: %s, status: %s, source context: %p)",
        jitSource,
        JitCodegenStateToString(result),
        *readySourceJitContext,
        jitSource->m_queryString,
        JitCodegenStateToString(result),
        JitCodegenStateToString(jitSource->m_codegenState),
        jitSource->m_sourceJitContext);

    (void)pthread_mutex_unlock(&jitSource->m_lock);

    return result;
}

extern bool IsJitSourceReady(JitSource* jitSource)
{
    (void)pthread_mutex_lock(&jitSource->m_lock);
    bool isReady = (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_READY);
    (void)pthread_mutex_unlock(&jitSource->m_lock);
    return isReady;
}

extern int GetJitSourceValidState(JitSource* jitSource)
{
    int validState = JIT_CONTEXT_INVALID;
    (void)pthread_mutex_lock(&jitSource->m_lock);
    if ((jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_READY) &&
        (jitSource->m_sourceJitContext != nullptr)) {
        validState = MOT_ATOMIC_LOAD(jitSource->m_sourceJitContext->m_validState);
    }
    (void)pthread_mutex_unlock(&jitSource->m_lock);
    return validState;
}

static Query* GetSourceQuery(const char* queryString)
{
    // we make a best effort to get the query
    Query* query = (Query*)u_sess->mot_cxt.jit_pg_query;
    if (query != nullptr) {
        MOT_LOG_TRACE("Retrieved saved query");
        return query;
    }

    // get query from cached plan (it may be invalid so we need to parse ourselves)
    CachedPlanSource* psrc = u_sess->pcache_cxt.first_saved_plan;
    List* parseTreeList = nullptr;
    while (psrc != nullptr) {
        if (strcmp(psrc->query_string, queryString) == 0) {
            if (psrc->is_valid) {
                parseTreeList = psrc->query_list;
                break;
            }
            // there might be another valid plan?
        }
        psrc = psrc->next_saved;
    }

    // extract query from parse tree list
    if (parseTreeList != nullptr) {
        int queryCount = list_length(parseTreeList);
        if (queryCount == 1) {
            query = (Query*)linitial(parseTreeList);
            MOT_LOG_TRACE("Retrieved query from plan source");
            return query;
        } else {
            MOT_LOG_TRACE(
                "Cannot get query: Invalid query list size %d in plan source for query: %s", queryCount, queryString);
            return nullptr;
        }
    }

    MOT_LOG_TRACE("Cannot get query: Could not find plan source for query: %s", queryString);
    return nullptr;
}

static bool JitCodegenQueryInplace(Query* query, const char* queryString, JitPlan* jitPlan, JitSource* jitSource)
{
    MotJitContext* sourceJitContext = nullptr;
    JitCodegenStats codegenStats = {};

    uint64_t startTime = GetSysClock();
    MOT_LOG_TRACE("Generating LLVM JIT context for query: %s", queryString);
    sourceJitContext = JitCodegenLlvmQuery(query, queryString, jitPlan, codegenStats);
    uint64_t endTime = GetSysClock();
    uint64_t timeMicros = MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime);
    codegenStats.m_codegenTime = timeMicros;

    if (sourceJitContext == nullptr) {
        // notify error for all waiters - this query will never again be JITTed - cleanup only during database shutdown
        MOT_LOG_TRACE("Failed to generate code for query with JIT source %p, signaling error context for query: %s",
            jitSource,
            queryString);
        SetJitSourceError(jitSource, MOT::GetRootError());
        JitStatisticsProvider::GetInstance().AddCodeGenErrorQuery();
        return false;
    }

    MOT_LOG_TRACE("Generated JIT context %p for query: %s", sourceJitContext, queryString);

    JitCodegenState newState = JitCodegenState::JIT_CODEGEN_NONE;
    if (!SetJitSourceReady(jitSource, sourceJitContext, &codegenStats, &newState)) {
        // this is illegal state transition error in JIT source (internal bug)
        // there is already a JIT context present in the JIT source (maybe generated by another session,
        // although impossible), so we just fail JIT for this session (other sessions may still benefit from
        // existing JIT context). Pay attention that we cannot replace the JIT context in the JIT source, since
        // the JIT function in it is probably still being used by other sessions
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_STATE,
            "JIT Compile",
            "Failed to set ready source context %p to JIT source %p (newState %s), disqualifying query: %s",
            sourceJitContext,
            jitSource,
            JitCodegenStateToString(newState),
            queryString);
        DestroyJitContext(sourceJitContext);
        JitStatisticsProvider::GetInstance().AddCodeGenErrorQuery();
        return false;
    }
    MOT_LOG_TRACE("Installed ready JIT context %p for query: %s", sourceJitContext, queryString);

    // update statistics
    JitStatisticsProvider& instance = JitStatisticsProvider::GetInstance();
    instance.AddCodeGenTime(MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime));
    instance.AddCodeGenQuery();
    instance.AddCodeCloneQuery();
    return true;
}

static JitCodegenState RegenerateQuery(JitSource* jitSource)
{
    MOT_LOG_TRACE("Regenerating JIT query source %p with query: %s", jitSource, jitSource->m_queryString);

    // get the query object
    Query* query = GetSourceQuery(jitSource->m_queryString);
    if (query == nullptr) {
        MOT_LOG_TRACE("Cannot get source query for query text: %s", jitSource->m_queryString);
        return JitCodegenState::JIT_CODEGEN_ERROR;
    }

    // prepare a JIT plan
    JitPlan* jitPlan = IsJittableQuery(query, jitSource->m_queryString, true);
    if (jitPlan == nullptr) {
        MOT_LOG_TRACE("Query is unjittable: %s", jitSource->m_queryString);
        return JitCodegenState::JIT_CODEGEN_ERROR;
    }

    // generate code from plan
    bool codeGenResult = JitCodegenQueryInplace(query, jitSource->m_queryString, jitPlan, jitSource);

    // cleanup plan before returning
    if ((jitPlan != nullptr) && (jitPlan != MOT_READY_JIT_PLAN)) {
        JitDestroyPlan(jitPlan);
    }

    JitCodegenState result = JitCodegenState::JIT_CODEGEN_READY;
    if (!codeGenResult) {
        MOT_LOG_TRACE("Code generation failed for JIT query source %p query: %s", jitSource, jitSource->m_queryString);
        result = JitCodegenState::JIT_CODEGEN_ERROR;
    } else {
        // we need to check if there is dummy invoke context, meaning we need to back off and wait for compilation
        LockJitSource(jitSource);  // lock before accessing source JIT context
        MOT_LOG_TRACE("Regenerated code for JIT query source %p (state %s) with query: %s",
            jitSource,
            JitCodegenStateToString(jitSource->m_codegenState),
            jitSource->m_queryString);
        result = jitSource->m_codegenState;
        JitQueryContext* queryContext = (JitQueryContext*)jitSource->m_sourceJitContext;
        if ((jitSource->m_codegenState != JitCodegenState::JIT_CODEGEN_DEPRECATE) && (queryContext != nullptr) &&
            (queryContext->m_commandType == JIT_COMMAND_INVOKE) && (queryContext->m_invokeContext != nullptr)) {
            if (IsJitContextPendingCompile(queryContext->m_invokeContext)) {
                MOT_LOG_TRACE("Invoked context is pending compile");
                jitSource->m_codegenState = JitCodegenState::JIT_CODEGEN_PENDING;
                // a race condition is possible with the session that compiles the common SP, so be careful
                if (jitSource->m_pendingChildCompile == 0) {
                    jitSource->m_pendingChildCompile = JIT_CONTEXT_PENDING_COMPILE;
                }
                result = JitCodegenState::JIT_CODEGEN_PENDING;
            }
        }
        UnlockJitSource(jitSource);
    }
    return result;
}

/*
 * NOTE: If the source context can be recycled, it is returned in the output parameter (recyclableContext).
 * Caller's responsibility to free recyclableContext if it is not NULL, since it should be done outside the
 * JIT source lock. Otherwise, it could lead to deadlock when in RemoveJitSourceContext() when trying to
 * ScheduleDeprecateJitSourceCleanUp().
 */
static void DeprecatePendingCompileSource(JitSource* jitSource, MotJitContext** recyclableContext)
{
    *recyclableContext = nullptr;
    if (jitSource->m_deprecatedPendingCompile) {
        MOT_LOG_ERROR("JIT source %p already deprecated concurrently, changing status from %s to %s, query: %s",
            jitSource,
            JitCodegenStateToString(jitSource->m_codegenState),
            JitCodegenStateToString(JitCodegenState::JIT_CODEGEN_DEPRECATE),
            jitSource->m_queryString);

        // JIT source was deprecated by another session when we were compiling.
        // Allow the JIT source to be cleaned from deprecated list now.
        MOT_ASSERT(jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_UNAVAILABLE);
        MOT_ASSERT(jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION);

        // It is possible that new dummy contexts were attached after the m_deprecatedPendingCompile flag was raised.
        // So we try to deprecate the context list again.
        if (jitSource->m_sourceJitContext != nullptr) {
            jitSource->m_sourceJitContext->m_jitSource = nullptr;  // avoid warning
            DeprecateSourceJitContext(jitSource, recyclableContext);
        }
        JitSourceDeprecateContextList(jitSource);
        jitSource->m_codegenState = JitCodegenState::JIT_CODEGEN_DEPRECATE;
        jitSource->m_deprecatedPendingCompile = 0;
        jitSource->m_timestamp = GetCurrentTimestamp();
        (void)CleanUpDeprecateJitSourceContexts(jitSource);
    }
}

static JitCodegenState ReinstateReadyState(JitSource* jitSource)
{
    MotJitContext* recyclableContext = nullptr;
    (void)pthread_mutex_lock(&jitSource->m_lock);
    if (jitSource->m_deprecatedPendingCompile) {
        // JIT source was deprecated by another session when we were compiling.
        // Allow the JIT source to be cleaned from deprecated list now.
        DeprecatePendingCompileSource(jitSource, &recyclableContext);
    } else if (jitSource->m_codegenState != JitCodegenState::JIT_CODEGEN_DEPRECATE) {
        MOT_ATOMIC_STORE(jitSource->m_sourceJitContext->m_validState, JIT_CONTEXT_VALID);
        jitSource->m_codegenState = JitCodegenState::JIT_CODEGEN_READY;
        jitSource->m_timestamp = GetCurrentTimestamp();
    }
    JitCodegenState result = jitSource->m_codegenState;
    (void)pthread_mutex_unlock(&jitSource->m_lock);

    if (recyclableContext != nullptr) {
        DestroyJitContext(recyclableContext);
        recyclableContext = nullptr;
    }

    return result;
}

static JitCodegenState RevalidateJitInvokeQuery(JitSource* jitSource)
{
    MOT_LOG_TRACE("Re-validating JIT INVOKE query source %p with query: %s", jitSource, jitSource->m_queryString);

    JitQueryContext* queryContext = (JitQueryContext*)jitSource->m_sourceJitContext;
    uint8_t invokeValidState = MOT_ATOMIC_LOAD(queryContext->m_invokeContext->m_validState);

    // try to re-validate the underlying function context
    // case 1: the underlying function context itself is invalid and needs to be re-validated (only if function was
    // recreated already)
    // case 2: the underlying function context itself is still valid, and some sub-query/SP needs to be re-validated
    bool attemptRegen = false;
    MotJitContext* invokedContext = queryContext->m_invokeContext;
    if (!RevalidateJitContext(invokedContext)) {
        // revalidate might fail due to backing-off while other session generates code for the same invoked SP (through
        // a different invoking query)
        bool isPending = false;
        bool isDone = false;
        bool isError = false;
        if (GetJitContextCompileState(invokedContext, &isPending, &isDone, &isError)) {
            if (isPending) {
                // invoked SP is being compiled by another session (using a different invoking query), so we back-off
                // and let this query use PG until SP compilation finishes
                MOT_LOG_TRACE("Invoked context pending concurrent code generation: %s", jitSource->m_queryString);
                return JitCodegenState::JIT_CODEGEN_UNAVAILABLE;
            }
            if (isError) {
                MOT_LOG_TRACE("Failed to regenerate code for invoke code, Concurrent code generation failed: %s",
                    jitSource->m_queryString);
                return JitCodegenState::JIT_CODEGEN_ERROR;
            }
            if (isDone) {  // very rare but might happen due to race
                MOT_LOG_TRACE("Invoked context concurrent code generation done, attempting revalidation: %s",
                    jitSource->m_queryString);
                if (!RevalidateJitContext(invokedContext)) {
                    MOT_LOG_TRACE("Revalidation after compile-done failed: %s:", jitSource->m_queryString);
                    return JitCodegenState::JIT_CODEGEN_ERROR;
                }
            }
        } else {  // no concurrent compilation activity detected
            // in case of dropped and recreated function we would fail here, as the function id is obsolete, so we
            // attempt to regenerate the entire context tree
            MOT_LOG_TRACE("Failed to revalidate invoked context, attempting full code regeneration: %s",
                invokedContext->m_queryString);
            attemptRegen = true;
        }
    } else {
        // after successful revalidation we need to update the id of the invoked function
        LockJitSource(jitSource);
        if (jitSource->m_sourceJitContext != nullptr) {
            jitSource->m_functionOid =
                ((JitQueryContext*)jitSource->m_sourceJitContext)->m_invokeContext->m_functionOid;
            jitSource->m_functionTxnId =
                ((JitQueryContext*)jitSource->m_sourceJitContext)->m_invokeContext->m_functionTxnId;
            jitSource->m_expireTxnId = 0;
        }
        UnlockJitSource(jitSource);
    }

    // if the function itself was previously invalidated then we should also regenerate code for the invoke query,
    // since signature, default parameters, etc. may have changed.
    if (!attemptRegen && (invokeValidState & JIT_CONTEXT_INVALID)) {
        MOT_LOG_TRACE(
            "Invoked function %s context is invalid, regenerating invoke code", invokedContext->m_queryString);
        attemptRegen = true;
    }

    if (attemptRegen) {
        JitCodegenState result = RegenerateQuery(jitSource);
        if (result != JitCodegenState::JIT_CODEGEN_READY) {
            MOT_LOG_TRACE("Code regeneration resulted in state %s for query: %s",
                JitCodegenStateToString(result),
                jitSource->m_queryString);
            return result;
        }
        // pointers need to be fetched again, since they changed after full regeneration
        LockJitSource(jitSource);
        queryContext = (JitQueryContext*)jitSource->m_sourceJitContext;
        invokedContext = queryContext->m_invokeContext;
        UnlockJitSource(jitSource);
    }

    MOT_LOG_TRACE("Re-validated invoked context: %s", invokedContext->m_queryString);
    MOT_LOG_TRACE("Re-validated JIT invoke query source %p of query: %s", jitSource, jitSource->m_queryString);
    return JitCodegenState::JIT_CODEGEN_READY;
}

static JitCodegenState RevalidateJitQuerySource(JitSource* jitSource, JitCodegenState prevState)
{
    MOT_LOG_TRACE("Re-validating JIT query source %p with query: %s", jitSource, jitSource->m_queryString);

    JitCodegenState regenResult = JitCodegenState::JIT_CODEGEN_NONE;
    int regenError = MOT_ERROR_RESOURCE_UNAVAILABLE;
    if ((prevState == JitCodegenState::JIT_CODEGEN_EXPIRED) || (jitSource->m_sourceJitContext == nullptr)) {
        // case 1: full code regeneration is required (includes invalid invoke context)
        regenResult = RegenerateQuery(jitSource);
    } else if ((prevState == JitCodegenState::JIT_CODEGEN_READY) ||
               (prevState == JitCodegenState::JIT_CODEGEN_UNAVAILABLE) ||
               (prevState == JitCodegenState::JIT_CODEGEN_PENDING)) {
        // case 2: query is invalid, this is possible only with INVOKE query or when table pointers are invalid
        if (jitSource->m_commandType == JIT_COMMAND_INVOKE) {
            regenResult = RevalidateJitInvokeQuery(jitSource);
        } else {
            // No concurrent revalidation possible for normal queries.
            MOT_ASSERT(MOT_ATOMIC_LOAD(jitSource->m_sourceJitContext->m_validState) == JIT_CONTEXT_RELATION_INVALID);
            if (!RefetchTablesAndIndices(jitSource->m_sourceJitContext)) {
                MOT_LOG_TRACE("Failed to re-fetch tables and index objects for source %p", jitSource);
                MOT_ATOMIC_STORE(jitSource->m_sourceJitContext->m_validState, JIT_CONTEXT_INVALID);
                regenResult = JitCodegenState::JIT_CODEGEN_ERROR;
                regenError = MOT_ERROR_INTERNAL;
            } else {
                MOT_ATOMIC_STORE(jitSource->m_sourceJitContext->m_validState, JIT_CONTEXT_VALID);
                regenResult = JitCodegenState::JIT_CODEGEN_READY;
            }
        }
    } else {
        MOT_LOG_TRACE("RevalidateJitQuerySource(): Unexpected codegen state %s", JitCodegenStateToString(prevState));
        regenResult = JitCodegenState::JIT_CODEGEN_ERROR;
        regenError = MOT_ERROR_INVALID_STATE;
    }

    if (regenResult == JitCodegenState::JIT_CODEGEN_READY) {
        // It is possible that the source is deprecated concurrently, so we check that within JIT source lock inside
        // ReinstateReadyState and then set source context as valid.
        regenResult = ReinstateReadyState(jitSource);
    } else if (regenResult == JitCodegenState::JIT_CODEGEN_ERROR) {
        MOT_LOG_TRACE("Failed to regenerate code for query text: %s", jitSource->m_queryString);
        SetJitSourceError(jitSource, regenError, &regenResult);
    } else if (regenResult == JitCodegenState::JIT_CODEGEN_UNAVAILABLE) {
        // we can get here a "pending compilation" result if a different invoking query regenerates code for the same
        // SP as this query. In this case we mark the invoking query as "pending compilation". When the SP finishes
        // compilation, this invoke query will be marked as "done-compiling" to re-attempt revalidation on next
        // invocation (see JitSourceInformCompileDone() for remark on this special use-case)
        LockJitSource(jitSource);
        MOT_ASSERT(jitSource->m_commandType == JIT_COMMAND_INVOKE);
        MOT_LOG_TRACE("RevalidateJitQuerySource(): Pending compilation of invoked query source %p (state %s)",
            jitSource,
            JitCodegenStateToString(jitSource->m_codegenState));
        regenResult = jitSource->m_codegenState;
        if (jitSource->m_codegenState != JitCodegenState::JIT_CODEGEN_DEPRECATE) {
            // a race condition is possible with the session that compiles the common SP, so be careful
            if (jitSource->m_pendingChildCompile == 0) {
                jitSource->m_pendingChildCompile = JIT_CONTEXT_PENDING_COMPILE;
            }
            // even if race condition occurred, we still allow caller to collect the result on next invocation
            jitSource->m_codegenState = JitCodegenState::JIT_CODEGEN_PENDING;
            regenResult = JitCodegenState::JIT_CODEGEN_PENDING;
        }
        UnlockJitSource(jitSource);
    } else if (regenResult == JitCodegenState::JIT_CODEGEN_PENDING) {
        MOT_LOG_TRACE("RevalidateJitQuerySource(): Pending compilation of invoked query");
    } else {
        MOT_LOG_TRACE(
            "RevalidateJitQuerySource(): Unexpected resulting codegen state %s", JitCodegenStateToString(regenResult));
        SetJitSourceError(jitSource, MOT_ERROR_INVALID_STATE, &regenResult);
    }

    return regenResult;
}

static bool RevalidateJitFunction(JitSource* jitSource)
{
    MOT_LOG_TRACE("Re-validating JIT function %p (%p) with function: %s",
        jitSource,
        jitSource->m_sourceJitContext,
        jitSource->m_queryString);

    JitFunctionContext* functionContext = (JitFunctionContext*)jitSource->m_sourceJitContext;
    uint8_t validState = MOT_ATOMIC_LOAD(functionContext->m_validState);
    if (validState & JIT_CONTEXT_INVALID) {
        // case 1: function context itself is invalid and needs to be fully regenerated, we must wait for user to issue
        // "CREATE OR REPLACE FUNCTION"
        MOT_LOG_TRACE("Cannot revalidate dropped JIT function: %s, validState: %s",
            jitSource->m_queryString,
            JitContextValidStateToString(validState));
        return false;
    }

    // case 2: the underlying function context itself is still valid (some sub-query/SP needs to be re-validated)
    MOT_LOG_TRACE("Attempting to revalidate any invalid query in SP: %s", jitSource->m_queryString);
    if (!RevalidateJitContext(functionContext)) {
        MOT_LOG_TRACE("Failed to revalidate JIT function: %s", jitSource->m_queryString);
        return false;
    }

    bool result = true;
    // It is possible that the source is deprecated concurrently, so we check that within JIT source lock inside
    // ReinstateReadyState and then set source context as valid.
    if (ReinstateReadyState(jitSource) != JitCodegenState::JIT_CODEGEN_READY) {
        result = false;
    }

    MOT_LOG_TRACE("Re-validated JIT function source %p with function: %s", jitSource, jitSource->m_queryString);
    return result;
}

static JitCodegenState ProcessRegeneratedJitFunctionSource(JitSource* jitSource, MotJitContext* sourceJitContext,
    JitCodegenStats* codegenStats, const char* qualifiedFunctionName)
{
    JitCodegenState newState = JitCodegenState::JIT_CODEGEN_NONE;
    if (sourceJitContext != nullptr) {
        (void)SetJitSourceReady(jitSource, (MotJitContext*)sourceJitContext, codegenStats, &newState);
    } else {
        SetJitSourceError(jitSource, MOT_ERROR_RESOURCE_UNAVAILABLE, &newState);
    }

    if (newState == JitCodegenState::JIT_CODEGEN_DEPRECATE) {
        // ATTENTION: Do not use the jitSource after this point, as it might be removed from the deprecated list
        // and freed by other sessions.
        CleanupConcurrentlyDroppedSPSource((const char*)qualifiedFunctionName);
    } else if (newState == JitCodegenState::JIT_CODEGEN_READY) {
        bool sourceReady = false;
        if (jitSource->m_usage == JIT_CONTEXT_GLOBAL) {
            LockJitSourceMap();

            LockJitSource(jitSource);
            newState = jitSource->m_codegenState;
            if (jitSource->m_sourceJitContext == sourceJitContext) {
                if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_READY) {
                    sourceReady = true;
                } else {
                    MOT_LOG_TRACE("JIT function source %p has unexpected state %s, after re-generation: %s",
                        jitSource,
                        JitCodegenStateToString(jitSource->m_codegenState),
                        jitSource->m_queryString);
                }
            } else {
                MOT_LOG_TRACE("JIT function source %p context %p changed concurrently to %p after re-generation",
                    jitSource,
                    sourceJitContext,
                    jitSource->m_sourceJitContext);
            }
            UnlockJitSource(jitSource);

            if (sourceReady) {
                MOT_LOG_TRACE("Pruning global ready function source %p after re-generation: %s",
                    jitSource,
                    jitSource->m_queryString);
                PruneNamespace(jitSource);
            }

            UnlockJitSourceMap();
        }
    }

    return newState;
}

static JitCodegenState RegenerateJitFunctionSource(JitSource* jitSource)
{
    MOT_LOG_TRACE("Regenerating JIT function source %p: %s", jitSource, jitSource->m_queryString);
    // we fetch again function name by id so we can validate function still exists
    const char* functionName = get_func_name(jitSource->m_functionOid);
    if (functionName == nullptr) {
        // function is not found, so we set state to dropped
        MOT_LOG_TRACE("RegenerateJitFunctionSource(): cannot find function %u", jitSource->m_functionOid);
        LockJitSource(jitSource);
        jitSource->m_codegenState = JitCodegenState::JIT_CODEGEN_DROPPED;
        UnlockJitSource(jitSource);
        return JitCodegenState::JIT_CODEGEN_DROPPED;
    }

    if (u_sess->mot_cxt.jit_compile_depth > MOT_JIT_MAX_COMPILE_DEPTH) {
        MOT_LOG_TRACE("RegenerateJitFunctionSource(): Reached maximum compile depth");
        SetJitSourceError(jitSource, MOT_ERROR_RESOURCE_LIMIT);
        return JitCodegenState::JIT_CODEGEN_ERROR;
    }

    MOT::mot_string qualifiedFunctionName;
    if (!qualifiedFunctionName.format("%s.%u", functionName, jitSource->m_functionOid)) {
        MOT_LOG_TRACE("RegenerateJitFunctionSource(): Failed to format qualified function name");
        SetJitSourceError(jitSource, MOT_ERROR_RESOURCE_LIMIT);
        return JitCodegenState::JIT_CODEGEN_ERROR;
    }

    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile HeapTuple procTuple = nullptr;
    volatile PLpgSQL_function* func = nullptr;
    volatile JitPlan* plan = nullptr;
    volatile MotJitContext* sourceJitContext = nullptr;
    volatile bool nsPushed = false;
    JitCodegenStats codegenStats = {};  // accessed only when ereport was not thrown
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile const char* qualifiedFunctionNameStr = qualifiedFunctionName.c_str();
    PG_TRY();
    {
        procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(jitSource->m_functionOid));
        if (!HeapTupleIsValid(procTuple)) {
            MOT_LOG_TRACE("RegenerateJitFunctionSource(): Oid %u not found in pg_proc", jitSource->m_functionOid);
        } else {
            // now we trigger compilation of the function at this point unconditionally
            // attention: if compilation failed then ereport is thrown.
            func = GetPGCompiledFunction(jitSource->m_functionOid, functionName);
            if (func == nullptr) {
                MOT_LOG_TRACE("RegenerateJitFunctionSource(): No compilation result for function %s", functionName);
                SetJitSourceError(jitSource, MOT_ERROR_RESOURCE_LIMIT);
            } else {
                ++func->use_count;
                MOT_LOG_TRACE("RegenerateJitFunctionSource(): Increased use count of function %p to %lu: %s",
                    func,
                    func->use_count,
                    jitSource->m_queryString);

                MOT_ASSERT(func->fn_xmin > jitSource->m_functionTxnId && func->fn_xmin >= jitSource->m_expireTxnId);

                // now we try to generate JIT code for the function
                if (PushJitSourceNamespace(jitSource->m_functionOid, jitSource->m_queryString)) {
                    nsPushed = true;
                    plan = IsJittableFunction((PLpgSQL_function*)func, procTuple, jitSource->m_functionOid, true);
                    if (plan != nullptr) {
                        uint64_t startTime = GetSysClock();
                        MOT_LOG_TRACE("Generating LLVM JIT context for function: %s", functionName);
                        sourceJitContext = JitCodegenLlvmFunction((PLpgSQL_function*)func,
                            procTuple,
                            jitSource->m_functionOid,
                            nullptr,
                            (JitPlan*)plan,
                            codegenStats);
                        uint64_t endTime = GetSysClock();
                        uint64_t timeMicros = MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime);
                        codegenStats.m_codegenTime = timeMicros;
                        ((JitFunctionPlan*)plan)->m_function = nullptr;  // prevent decrease use count
                        JitDestroyPlan((JitPlan*)plan);
                        plan = nullptr;
                    }
                }
            }
        }
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while regenerating function %s: %s", functionName, edata->message);
        ereport(WARNING,
            (errmodule(MOD_MOT),
                errmsg("Caught exception while regenerating function %s: %s", functionName, edata->message),
                errdetail("%s", edata->detail)));
        FlushErrorState();
        FreeErrorData(edata);
    }
    PG_END_TRY();

    if (plan != nullptr) {
        JitDestroyPlan((JitPlan*)plan);
    }

    if (nsPushed) {
        PopJitSourceNamespace();
    }

    if (func != nullptr) {
        --func->use_count;
        MOT_LOG_TRACE("RegenerateJitFunctionSource(): Decreased use count of function %p to %lu: %s",
            func,
            func->use_count,
            jitSource->m_queryString);
    }

    if (procTuple != nullptr) {
        ReleaseSysCache(procTuple);
    }

    return ProcessRegeneratedJitFunctionSource(
        jitSource, (MotJitContext*)sourceJitContext, &codegenStats, (const char*)qualifiedFunctionNameStr);
}

static JitCodegenState RevalidateJitFunctionSource(JitSource* jitSource, JitCodegenState prevState)
{
    MOT_LOG_TRACE("Re-validating JIT function source %p with function: %s", jitSource, jitSource->m_queryString);

    // case 1: function was dropped or being replaced. If function was not created yet we will proceed to full
    // regeneration. otherwise we have nothing to do about it.
    if ((prevState == JitCodegenState::JIT_CODEGEN_DROPPED) || (prevState == JitCodegenState::JIT_CODEGEN_REPLACED)) {
        MOT_LOG_TRACE("RevalidateJitFunctionSource(): Function was dropped or replaced, attempting full code "
                      "regeneration for SP: %s",
            jitSource->m_queryString);
        JitCodegenState result = RegenerateJitFunctionSource(jitSource);
        if (result == JitCodegenState::JIT_CODEGEN_DROPPED) {
            // Function was dropped. We return JitCodegenState::JIT_CODEGEN_ERROR to indicate the re-validation failure.
            MOT_LOG_TRACE("RevalidateJitFunctionSource(): Function was dropped, signaling codegen error for SP: %s",
                jitSource->m_queryString);
            return JitCodegenState::JIT_CODEGEN_ERROR;
        }
        return result;
    }

    // lock before accessing source JIT context
    (void)pthread_mutex_lock(&jitSource->m_lock);

    // any other state but ready is unexpected and leads to error state
    if ((prevState != JitCodegenState::JIT_CODEGEN_READY) || (jitSource->m_sourceJitContext == nullptr)) {
        MOT_LOG_TRACE(
            "RevalidateJitFunctionSource(): Unexpected codegen state %s (source context %p, current state %s)",
            JitCodegenStateToString(prevState),
            jitSource->m_sourceJitContext,
            JitCodegenStateToString(jitSource->m_codegenState));
        (void)pthread_mutex_unlock(&jitSource->m_lock);
        SetJitSourceError(jitSource, MOT_ERROR_INVALID_STATE);
        return JitCodegenState::JIT_CODEGEN_ERROR;
    }

    // case 3: function is ready but invalid, so we re-validate it outside lock scope
    MOT_ASSERT(MOT_ATOMIC_LOAD(jitSource->m_sourceJitContext->m_validState) != JIT_CONTEXT_VALID);

    (void)pthread_mutex_unlock(&jitSource->m_lock);

    MOT_LOG_TRACE("RevalidateJitFunctionSource(): Function is ready but invalid, attempting revalidation for SP: %s ",
        jitSource->m_queryString);
    if (!RevalidateJitFunction(jitSource)) {
        MOT_LOG_TRACE(
            "RevalidateJitFunctionSource(): Failed to revalidate code for function: %s", jitSource->m_queryString);
        SetJitSourceError(jitSource, MOT_ERROR_RESOURCE_UNAVAILABLE);
        return JitCodegenState::JIT_CODEGEN_ERROR;
    }
    return JitCodegenState::JIT_CODEGEN_READY;
}

extern JitCodegenState RevalidateJitSource(
    JitSource* jitSource, TransactionId functionTxnId /* = InvalidTransactionId */)
{
    MOT_LOG_TRACE("Re-validating JIT source %p with query: %s", jitSource, jitSource->m_queryString);

    (void)pthread_mutex_lock(&jitSource->m_lock);
    volatile bool needUnlock = true;

    // we cannot revalidate deprecate JIT source
    if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_DEPRECATE) {
        MOT_LOG_TRACE("Cannot revalidate deprecate JIT source %p: %s", jitSource, jitSource->m_queryString);
        (void)pthread_mutex_unlock(&jitSource->m_lock);
        return JitCodegenState::JIT_CODEGEN_DEPRECATE;
    }

    if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_DROPPED) {
        MOT_LOG_TRACE("Cannot revalidate dropped JIT source %p: %s", jitSource, jitSource->m_queryString);
        (void)pthread_mutex_unlock(&jitSource->m_lock);
        return JitCodegenState::JIT_CODEGEN_DROPPED;
    }

    // If someone else is operating on the source then we just inform the caller
    // that the code is being generated by returning JitCodegenState::JIT_CODEGEN_UNAVAILABLE.
    if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_UNAVAILABLE) {
        MOT_LOG_TRACE(
            "Backing-off from revalidation in unavailable JIT source %p: %s", jitSource, jitSource->m_queryString);
        (void)pthread_mutex_unlock(&jitSource->m_lock);
        return JitCodegenState::JIT_CODEGEN_UNAVAILABLE;
    }

    // if the source was previously marked as pending for child compilation to finish, then check again
    if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_PENDING) {
        if (jitSource->m_pendingChildCompile == JIT_CONTEXT_PENDING_COMPILE) {
            MOT_LOG_TRACE(
                "Backing-off from revalidation in pending JIT source %p: %s", jitSource, jitSource->m_queryString);
            (void)pthread_mutex_unlock(&jitSource->m_lock);
            return JitCodegenState::JIT_CODEGEN_PENDING;
        } else {
            // whether done or error, just reset and retry
            MOT_LOG_TRACE(
                "Child compile done/error/none in pending JIT source %p: %s", jitSource, jitSource->m_queryString);
            jitSource->m_pendingChildCompile = 0;
        }
    }

    // if everything is fine then just return
    volatile JitCodegenState result = jitSource->m_codegenState;
    uint8_t validState = JIT_CONTEXT_INVALID;
    if (jitSource->m_sourceJitContext != nullptr) {
        validState = MOT_ATOMIC_LOAD(jitSource->m_sourceJitContext->m_validState);
    }
    MOT_LOG_TRACE("JIT source %p with source context %p is in state %s, Valid-state is: %s",
        jitSource,
        jitSource->m_sourceJitContext,
        JitCodegenStateToString(jitSource->m_codegenState),
        JitContextValidStateToString(validState));
    if ((result == JitCodegenState::JIT_CODEGEN_READY) && (validState == JIT_CONTEXT_VALID)) {
        MOT_LOG_TRACE("JIT source %p is ready and valid", jitSource);
    } else {
        if (!IsSimpleQuerySource(jitSource) &&
            ((result == JitCodegenState::JIT_CODEGEN_EXPIRED) || (result == JitCodegenState::JIT_CODEGEN_ERROR)) &&
            IsPrematureRevalidation(jitSource, functionTxnId)) {
            MOT_LOG_TRACE("Skipping premature re-validation of JIT source %p: %s", jitSource, jitSource->m_queryString);
            (void)pthread_mutex_unlock(&jitSource->m_lock);
            // Premature re-validation attempt: We don't change the codegen state.
            // We return to caller status-deprecate, this way the caller will abort this codegen attempt.
            return JitCodegenState::JIT_CODEGEN_DEPRECATE;
        }

        MOT_LOG_TRACE("JIT source %p moving from state %s to UNAVAILABLE before re-validation: %s",
            jitSource,
            JitCodegenStateToString(result),
            jitSource->m_queryString);

        // we set to unavailable state regardless of current state (whether expired due to DDL,
        // or child query/sp invalid, or dropped/replaced function, or even error state)
        jitSource->m_codegenState = JitCodegenState::JIT_CODEGEN_UNAVAILABLE;

        // revalidate/regenerate out of lock scope, so mot_jit_details does not get stuck
        (void)pthread_mutex_unlock(&jitSource->m_lock);
        needUnlock = false;

        volatile MemoryContext origCxt = CurrentMemoryContext;
        PG_TRY();
        {
            if (jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
                result = RevalidateJitQuerySource(jitSource, result);
            } else {
                result = RevalidateJitFunctionSource(jitSource, result);
            }
            if (result == JitCodegenState::JIT_CODEGEN_READY) {
                JitSourceList parentSourceList;
                (void)pthread_mutex_lock(&jitSource->m_lock);
                // attention: concurrent deprecate can happen, so we check again after re-locking
                if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_READY) {
                    JitSourceInformCompileDone(jitSource, &parentSourceList);
                }
                result = jitSource->m_codegenState;  // report deprecate if race happened
                (void)pthread_mutex_unlock(&jitSource->m_lock);
                // notify outside lock-scope to avoid deadlock
                JitSourceListInformCompileDone(&parentSourceList, jitSource->m_queryString);
            }
        }
        PG_CATCH();
        {
            (void)MemoryContextSwitchTo(origCxt);
            ErrorData* edata = CopyErrorData();
            MOT_LOG_WARN("Caught exception while re-validating JIT source for query '%s': %s",
                jitSource->m_queryString,
                edata->message);
            FlushErrorState();
            FreeErrorData(edata);
            SetJitSourceError(jitSource, MOT_ERROR_SYSTEM_FAILURE);
            result = JitCodegenState::JIT_CODEGEN_ERROR;
        }
        PG_END_TRY();
    }

    if (needUnlock) {
        (void)pthread_mutex_unlock(&jitSource->m_lock);
    }

    MOT_LOG_TRACE("Re-validate result %s for JIT source %p with query: %s",
        JitCodegenStateToString(result),
        jitSource,
        jitSource->m_queryString);
    return result;
}

extern void SetJitSourceError(JitSource* jitSource, int errorCode, JitCodegenState* newState /* = nullptr */)
{
    JitCodegenStats codegenStats = {};  // Reset the code generation stats on error.
    SetJitSourceStatus(
        jitSource, nullptr, JitCodegenState::JIT_CODEGEN_ERROR, &codegenStats, errorCode, 0, nullptr, false, newState);
}

extern void SetJitSourceExpired(JitSource* jitSource, uint64_t relationId, bool functionReplace /* = false */)
{
    // We don't reset the code generation stats when setting the source as expired.
    SetJitSourceStatus(
        jitSource, nullptr, JitCodegenState::JIT_CODEGEN_EXPIRED, nullptr, 0, relationId, nullptr, functionReplace);
    JitStatisticsProvider::GetInstance().AddCodeExpiredQuery();
}

extern bool SetJitSourceReady(JitSource* jitSource, MotJitContext* readySourceJitContext, JitCodegenStats* codegenStats,
    JitCodegenState* newState /* = nullptr */)
{
    bool result = true;
    bool invalidState = false;
    MOT_ASSERT(codegenStats != nullptr);
    SetJitSourceStatus(jitSource,
        readySourceJitContext,
        JitCodegenState::JIT_CODEGEN_READY,
        codegenStats,
        0,
        0,
        &invalidState,
        false,
        newState);
    if (invalidState) {
        result = false;
    }
    return result;
}

static char* CloneQueryString(const char* queryString, JitContextUsage usage)
{
    char* newQueryString = nullptr;
    if (queryString != nullptr) {
        size_t len = strlen(queryString);
        if (len > 0) {
            newQueryString = (char*)JitMemAlloc(len + 1, usage);
            if (newQueryString != nullptr) {
                errno_t erc = strcpy_s(newQueryString, len + 1, queryString);
                securec_check(erc, "\0", "\0");
            }
        }
    }
    return newQueryString;
}

static void FreeQueryString(char* queryString, JitContextUsage usage)
{
    if (queryString != nullptr) {
        JitMemFree(queryString, usage);
    }
}

static bool CheckInvalidStateTransition(
    JitSource* jitSource, MotJitContext* readySourceJitContext, JitCodegenState state, bool* invalidState)
{
    // ready --> ready
    if ((jitSource->m_sourceJitContext != nullptr) &&
        (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_READY) &&
        (state == JitCodegenState::JIT_CODEGEN_READY)) {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_STATE, "JIT Compile", "Cannot set context as ready: already set");
        if (invalidState) {
            *invalidState = true;
        }
        return false;
    }

    // deprecate --> not deprecate
    if ((jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_DEPRECATE) &&
        (state != JitCodegenState::JIT_CODEGEN_DEPRECATE)) {
        MOT_LOG_ERROR("Attempt to set deprecate JIT source %p as %s denied: %s",
            jitSource,
            JitCodegenStateToString(state),
            jitSource->m_queryString);
        if (invalidState) {
            *invalidState = true;
        }
        return false;
    }

    MOT_JIT_SOURCE_VERIFY_STATE(jitSource, readySourceJitContext, state);
    if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_EXPIRED) {
        if (state != JitCodegenState::JIT_CODEGEN_EXPIRED) {
            MOT_LOG_TRACE("Updated expired JIT source %p with context %p to state %s (query string: %s)",
                jitSource,
                readySourceJitContext,
                JitCodegenStateToString(state),
                jitSource->m_queryString);
        } else {
            MOT_LOG_TRACE("Duplicate attempt to set JIT source %p as expired (query string: %s)",
                jitSource,
                jitSource->m_queryString);
        }
    } else if (state == JitCodegenState::JIT_CODEGEN_EXPIRED) {
        MOT_LOG_TRACE("Setting JIT source %p as expired (query string: %s)", jitSource, jitSource->m_queryString);
    } else {
        MOT_LOG_TRACE("Installing JIT source %p with context %p (query string: %s)",
            jitSource,
            readySourceJitContext,
            jitSource->m_queryString);
    }
    return true;
}

static void SetJitSourceJittable(JitSource* jitSource, bool& compileDone, JitSourceList& parentSourceList)
{
    // Valid source context and state are just installed in the caller (in SetJitSourceStatus()).
    MOT_ASSERT(jitSource->m_sourceJitContext != nullptr);

    MotJitContext* readySourceJitContext = jitSource->m_sourceJitContext;

    // copy query string from source to context
    jitSource->m_sourceJitContext->m_queryString = jitSource->m_queryString;
    jitSource->m_sourceJitContext->m_jitSource = jitSource;
    jitSource->m_sourceJitContext->m_isSourceContext = 1;
    MOT_ASSERT(jitSource->m_contextType == readySourceJitContext->m_contextType);
    jitSource->m_commandType = readySourceJitContext->m_commandType;
    jitSource->m_status = JitSourceStatus::JIT_SOURCE_JITTABLE;
    if (jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_INVALID) {
        jitSource->m_contextType = readySourceJitContext->m_contextType;
    }
    MOT_ASSERT(jitSource->m_contextType == readySourceJitContext->m_contextType);
    if (jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) {
        jitSource->m_functionOid = ((JitFunctionContext*)readySourceJitContext)->m_functionOid;
        jitSource->m_functionTxnId = ((JitFunctionContext*)readySourceJitContext)->m_functionTxnId;
        jitSource->m_expireTxnId = 0;
    } else {
        if (jitSource->m_commandType == JIT_COMMAND_INVOKE) {
            if (((JitQueryContext*)readySourceJitContext)->m_invokeContext != nullptr) {
                jitSource->m_functionOid =
                    ((JitQueryContext*)readySourceJitContext)->m_invokeContext->m_functionOid;
                jitSource->m_functionTxnId =
                    ((JitQueryContext*)readySourceJitContext)->m_invokeContext->m_functionTxnId;
            } else {
                jitSource->m_functionOid = ((JitQueryContext*)readySourceJitContext)->m_invokedFunctionOid;
                jitSource->m_functionTxnId =
                    ((JitQueryContext*)readySourceJitContext)->m_invokedFunctionTxnId;
            }
            jitSource->m_expireTxnId = 0;
        }
    }
    if ((jitSource->m_commandType == JIT_COMMAND_INVOKE) &&
        (((JitQueryContext*)readySourceJitContext)->m_invokeContext != nullptr) &&
        IsJitContextPendingCompile(((JitQueryContext*)readySourceJitContext)->m_invokeContext)) {
        MOT_LOG_TRACE("Installed pending source context: %s", jitSource->m_queryString);
        jitSource->m_pendingChildCompile = JIT_CONTEXT_PENDING_COMPILE;
        jitSource->m_codegenState = JitCodegenState::JIT_CODEGEN_PENDING;
    } else {
        JitSourceInformCompileDone(jitSource, &parentSourceList);
        compileDone = true;
        MOT_LOG_TRACE("Compilation done: %s", jitSource->m_queryString);
    }
}

static void SetJitSourceUnjittable(
    JitSource* jitSource, uint64_t relationId, bool functionReplace, JitSourceList& parentSourceList)
{
    // Null source context and state are just installed in the caller (in SetJitSourceStatus()).
    MOT_ASSERT(jitSource->m_sourceJitContext == nullptr);

    JitCodegenState state = jitSource->m_codegenState;

    if ((jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) &&
        (jitSource->m_commandType != JIT_COMMAND_INVOKE)) {
        JitSourcePurgeContextList(jitSource, relationId);
    }
    if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_EXPIRED) {
        JitSourceInvalidateContextList(jitSource, relationId);
    } else if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_ERROR) {
        JitSourceInformCompileError(jitSource, &parentSourceList);
    } else {
        MOT_LOG_TRACE("Invalid codegen state: %s", JitCodegenStateToString(jitSource->m_codegenState));
        MOT_ASSERT(false);
    }
    jitSource->m_status = JitSourceStatus::JIT_SOURCE_UNJITTABLE;
    if (jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
        if (state != JitCodegenState::JIT_CODEGEN_ERROR) {
            jitSource->m_status = JitSourceStatus::JIT_SOURCE_INVALID;  // move to transient state
        }
    } else {
        // unless error state, we fix state to replaced or dropped
        if (state != JitCodegenState::JIT_CODEGEN_ERROR) {
            if (functionReplace) {
                // this is a short lived transient state, which is used to force code regeneration
                jitSource->m_codegenState = JitCodegenState::JIT_CODEGEN_REPLACED;
                jitSource->m_status = JitSourceStatus::JIT_SOURCE_INVALID;  // move to transient state
            } else {
                // function is dropped
                jitSource->m_codegenState = JitCodegenState::JIT_CODEGEN_DROPPED;
            }
        }
    }
}

static void SetJitSourceStatus(JitSource* jitSource, MotJitContext* readySourceJitContext, JitCodegenState state,
    JitCodegenStats* codegenStats /* = nullptr */, int errorCode /* = 0 */, uint64_t relationId /* = 0 */,
    bool* invalidState /* = nullptr */, bool functionReplace /* = false */, JitCodegenState* newState /* = nullptr */)
{
    MOT::Table* table = nullptr;
    if (readySourceJitContext && readySourceJitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
        table = ((JitQueryContext*)readySourceJitContext)->m_table;
    }

    JitSourceList parentSourceList;
    bool compileDone = false;
    MotJitContext* recyclableContext = nullptr;
    (void)pthread_mutex_lock(&jitSource->m_lock);

    MOT_LOG_TRACE("JIT source %p (codegen state %s, deprecatedPendingCompile %u): Installing ready JIT source context "
                  "%p with status %s, table %p and query: %s",
        jitSource,
        JitCodegenStateToString(jitSource->m_codegenState),
        jitSource->m_deprecatedPendingCompile,
        readySourceJitContext,
        JitCodegenStateToString(state),
        table,
        jitSource->m_queryString);
    MOT_LOG_TRACE("JIT source %p: relation-id = %" PRIu64 ", function-replace = %s",
        jitSource,
        relationId,
        functionReplace ? "true" : "false");

    if (jitSource->m_deprecatedPendingCompile) {
        // JIT source was deprecated by another session when we were compiling.
        // Allow the JIT source to be cleaned from deprecated list now.
        DeprecatePendingCompileSource(jitSource, &recyclableContext);
        JitSourceInformCompileError(jitSource, &parentSourceList);
        if (invalidState) {
            *invalidState = true;
        }
    } else {
        if (CheckInvalidStateTransition(jitSource, readySourceJitContext, state, invalidState)) {
            // cleanup old context (avoid resource leak)
            if (jitSource->m_sourceJitContext != nullptr) {
                // delay the destruction of the source context until any session is done executing
                // the old source context
                jitSource->m_sourceJitContext->m_jitSource = nullptr;  // avoid warning
                DeprecateSourceJitContext(jitSource, &recyclableContext);
            }

            // install new context
            jitSource->m_sourceJitContext = readySourceJitContext;
            jitSource->m_codegenState = state;
            if (jitSource->m_sourceJitContext != nullptr) {
                SetJitSourceJittable(jitSource, compileDone, parentSourceList);
            } else {
                // expired/error/dropped/replaced context: cleanup all related JIT context objects
                SetJitSourceUnjittable(jitSource, relationId, functionReplace, parentSourceList);
            }
            jitSource->m_timestamp = GetCurrentTimestamp();
            if (codegenStats != nullptr) {
                jitSource->m_codegenStats = *codegenStats;
            }
        }
    }

    JitCodegenState finalState = jitSource->m_codegenState;
    if (newState != nullptr) {
        *newState = jitSource->m_codegenState;
    }

    (void)pthread_mutex_unlock(&jitSource->m_lock);

    if (recyclableContext != nullptr) {
        DestroyJitContext(recyclableContext);
        recyclableContext = nullptr;
    }

    // notify outside lock-scope to avoid deadlock
    if (!parentSourceList.Empty()) {
        if (compileDone) {
            JitSourceListInformCompileDone(&parentSourceList, jitSource->m_queryString);
        } else {
            JitSourceListInformCompileError(&parentSourceList, jitSource->m_queryString);
        }
    }

    // in any case we notify change (even error or expired status)
    MOT_LOG_TRACE("JIT source %p: Notifying JIT context %p is %s (query: %s)",
        jitSource,
        jitSource->m_sourceJitContext,
        JitCodegenStateToString(finalState),
        jitSource->m_queryString);
}

extern const char* JitCodegenStateToString(JitCodegenState state)
{
    switch (state) {
        case JitCodegenState::JIT_CODEGEN_READY:
            return "ready";
        case JitCodegenState::JIT_CODEGEN_UNAVAILABLE:
            return "unavailable";
        case JitCodegenState::JIT_CODEGEN_ERROR:
            return "error";
        case JitCodegenState::JIT_CODEGEN_EXPIRED:
            return "expired";
        case JitCodegenState::JIT_CODEGEN_DROPPED:
            return "dropped";
        case JitCodegenState::JIT_CODEGEN_REPLACED:
            return "replaced";
        case JitCodegenState::JIT_CODEGEN_DEPRECATE:
            return "deprecate";
        case JitCodegenState::JIT_CODEGEN_PENDING:
            return "pending";
        case JitCodegenState::JIT_CODEGEN_NONE:
            return "none";
        default:
            return "N/A";
    }
}

extern void AddJitSourceContext(JitSource* jitSource, MotJitContext* jitContext)
{
    (void)pthread_mutex_lock(&jitSource->m_lock);
    MOT_ASSERT(!ContextListContainsJitContext(jitSource, jitContext));
    jitContext->m_nextInSource = jitSource->m_contextList;
    jitSource->m_contextList = jitContext;
    jitContext->m_jitSource = jitSource;
    jitContext->m_sourceJitContext = jitSource->m_sourceJitContext;
    if (jitSource->m_sourceJitContext != nullptr) {
        MOT_ATOMIC_INC(jitSource->m_sourceJitContext->m_useCount);
        MOT_LOG_TRACE("AddJitSourceContext(): Incremented use count of source context %p to: %u",
            jitSource->m_sourceJitContext,
            (uint32_t)MOT_ATOMIC_LOAD(jitSource->m_sourceJitContext->m_useCount));
    }
    MOT_LOG_TRACE("JIT source %p: Added JIT context %p", jitSource, jitContext);
    (void)pthread_mutex_unlock(&jitSource->m_lock);
}

extern void RemoveJitSourceContext(JitSource* jitSource, MotJitContext* cleanupContext)
{
    bool canRecycle = false;
    MotJitContext* deprecatedContext = nullptr;
    (void)pthread_mutex_lock(&jitSource->m_lock);
    // by the time we reach here, many things could have happened, so we first need to understand whether this context
    // originated from the current source context or from a deprecate one
    // even if source context is deprecate, the context is still found in the context list
    RemoveJitSourceContextImpl(jitSource, cleanupContext);
    MotJitContext* sourceContext = cleanupContext->m_sourceJitContext;
    if (sourceContext != nullptr) {
        MOT_ATOMIC_DEC(sourceContext->m_useCount);
        MOT_LOG_TRACE("RemoveJitSourceContext(): Decremented use count of source context %p to: %u",
            sourceContext,
            (uint32_t)MOT_ATOMIC_LOAD(sourceContext->m_useCount));
        if (sourceContext != jitSource->m_sourceJitContext) {
            // we are dealing with a deprecate source context
            MOT_ASSERT(MOT_ATOMIC_LOAD(sourceContext->m_validState) & JIT_CONTEXT_DEPRECATE);
            MOT_ASSERT(DeprecateListContainsJitContext(jitSource, sourceContext));
            if (MOT_ATOMIC_LOAD(sourceContext->m_useCount) == 0) {
                UnlinkDeprecateJitContext(jitSource, sourceContext);
                deprecatedContext = sourceContext;
            }
        }
        cleanupContext->m_sourceJitContext = nullptr;
    }
    // check if this is a deprecate JIT source that is ready for recycling
    canRecycle = IsJitSourceRecyclable(jitSource);
    (void)pthread_mutex_unlock(&jitSource->m_lock);

    MOT_LOG_TRACE("JIT source %p: Removed JIT context %p (%s)", jitSource, cleanupContext, jitSource->m_queryString);

    if (deprecatedContext != nullptr) {
        MOT_LOG_TRACE("Destroying deprecate JIT source context %p", deprecatedContext);
        DestroyJitContext(deprecatedContext);
        deprecatedContext = nullptr;
    }

    // schedule recycling out of lock scope to avoid deadlocks
    if (canRecycle) {
        MOT_LOG_TRACE("Deprecate JIT source %p is ready for recycling: %s", jitSource, jitSource->m_queryString);
        ScheduleDeprecateJitSourceCleanUp(jitSource);
    }
}

extern bool JitSourceRefersRelation(JitSource* jitSource, uint64_t relationId, bool searchDeprecate)
{
    bool result = false;
    (void)pthread_mutex_lock(&jitSource->m_lock);
    if (searchDeprecate) {
        MotJitContext* itr = jitSource->m_deprecateContextList;
        while (itr != nullptr) {
            if (JitContextRefersRelation(itr, relationId)) {
                result = true;
                break;
            }
            itr = itr->m_nextDeprecate;
        }
    }
    if (!result) {
        result = JitContextRefersRelation(jitSource->m_sourceJitContext, relationId);
    }
    (void)pthread_mutex_unlock(&jitSource->m_lock);
    return result;
}

extern bool JitSourceRefersSP(JitSource* jitSource, Oid functionId)
{
    bool result = false;
    if (jitSource->m_sourceJitContext != nullptr) {
        if (jitSource->m_sourceJitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
            result = JitQueryContextRefersSP((JitQueryContext*)jitSource->m_sourceJitContext, functionId);
        } else {
            result = JitFunctionContextRefersSP((JitFunctionContext*)jitSource->m_sourceJitContext, functionId);
        }
    }
    return result;
}

static bool JitQueryContextRefersSP(JitQueryContext* jitContext, Oid functionId)
{
    bool result = false;
    if (jitContext->m_commandType == JIT_COMMAND_INVOKE) {
        result = JitFunctionContextRefersSP(jitContext->m_invokeContext, functionId);
    }
    return result;
}

static bool JitFunctionContextRefersSP(JitFunctionContext* jitContext, Oid functionId)
{
    bool result = false;
    if (jitContext->m_functionOid == functionId) {
        result = true;
    } else {
        for (uint32_t i = 0; i < jitContext->m_SPSubQueryCount; ++i) {
            JitCallSite* callSite = &jitContext->m_SPSubQueryList[i];
            if (JitQueryContextRefersSP((JitQueryContext*)callSite->m_queryContext, functionId)) {
                result = true;
                break;
            }
        }
    }
    return result;
}

extern uint32_t GetJitSourceNonJitQueryCount(JitSource* jitSource)
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
            ++count;
        }
    }
    return count;
}

extern void PurgeJitSource(JitSource* jitSource, uint64_t relationId)
{
    (void)pthread_mutex_lock(&jitSource->m_lock);

    // purge source context
    if (jitSource->m_sourceJitContext != nullptr) {
        PurgeJitContext(jitSource->m_sourceJitContext, relationId);
    }

    // purge registers sessions contexts
    JitSourcePurgeContextList(jitSource, relationId);

    // we also need to purge deprecate list
    MotJitContext* itr = jitSource->m_deprecateContextList;
    while (itr != nullptr) {
        PurgeJitContext(itr, relationId);
        itr = itr->m_nextDeprecate;
    }
    (void)pthread_mutex_unlock(&jitSource->m_lock);
}

extern void DeprecateJitSource(JitSource* jitSource, bool markOnly)
{
    MotJitContext* recyclableContext = nullptr;
    (void)pthread_mutex_lock(&jitSource->m_lock);
    MOT_LOG_TRACE("Trying to mark JIT source %p with status %s as deprecated: %s",
        jitSource,
        JitCodegenStateToString(jitSource->m_codegenState),
        jitSource->m_queryString);

    // if someone is revalidating right now, we avoid deprecating the source, since revalidation might take place
    // before we even clone the context (so the use count was not incremented yet)
    if (!markOnly) {
        if (jitSource->m_codegenState != JitCodegenState::JIT_CODEGEN_UNAVAILABLE) {
            if (jitSource->m_sourceJitContext != nullptr) {
                jitSource->m_sourceJitContext->m_jitSource = nullptr;  // avoid warning
                DeprecateSourceJitContext(jitSource, &recyclableContext);
            }
        }
        JitSourceDeprecateContextList(jitSource);
    }
    if (jitSource->m_codegenState != JitCodegenState::JIT_CODEGEN_UNAVAILABLE) {
        MOT_ASSERT(jitSource->m_deprecatedPendingCompile == 0);
        // If we have raised the m_deprecatedPendingCompile flag and state was changed to
        // JitCodegenState::JIT_CODEGEN_DEPRECATE already by another session, we don't have to change it again.
        if (jitSource->m_codegenState != JitCodegenState::JIT_CODEGEN_DEPRECATE) {
            MOT_LOG_TRACE("Marking JIT source %p with status %s (deprecatedPendingCompile %u) as deprecate: %s",
                jitSource,
                JitCodegenStateToString(jitSource->m_codegenState),
                jitSource->m_deprecatedPendingCompile,
                jitSource->m_queryString);
            jitSource->m_codegenState = JitCodegenState::JIT_CODEGEN_DEPRECATE;
            jitSource->m_timestamp = GetCurrentTimestamp();
        }
    } else {
        // We raise the m_deprecatedPendingCompile flag if it was not raised already.
        if ((jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) &&
            (jitSource->m_deprecatedPendingCompile == 0)) {
            // Someone is still compiling. We avoid deprecating the source, but still put it in the deprecated list.
            // Once that compilation is finished, the status will be changed to JitCodegenState::JIT_CODEGEN_DEPRECATE
            // and then the source can be freed.
            MOT_LOG_TRACE("Marking JIT source %p with status unavailable as deprecate pending compile: %s",
                jitSource,
                jitSource->m_queryString);
            jitSource->m_deprecatedPendingCompile = 1;
        }
    }
    (void)pthread_mutex_unlock(&jitSource->m_lock);

    if (recyclableContext != nullptr) {
        DestroyJitContext(recyclableContext);
        recyclableContext = nullptr;
    }
}

extern const char* JitSourceStatusToString(JitSourceStatus status)
{
    switch (status) {
        case JitSourceStatus::JIT_SOURCE_INVALID:
            return "invalid";

        case JitSourceStatus::JIT_SOURCE_JITTABLE:
            return "jittable";

        case JitSourceStatus::JIT_SOURCE_UNJITTABLE:
            return "unjittable";

        default:
            return "N/A";
    }
}

extern JitSource* CloneLocalJitSource(JitSource* jitSource, bool cloneContext, JitSourceOp sourceOp)
{
    JitSource* localSource = AllocJitSource(jitSource->m_queryString, JIT_CONTEXT_LOCAL);
    if (localSource == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Process DDL event", "Failed to allocate pooled session-local JIT source");
        return nullptr;
    }
    MOT_LOG_TRACE(
        "Created jit-source object %p during clone-local-source: %s", localSource, localSource->m_queryString);

    (void)pthread_mutex_lock(&jitSource->m_lock);
    // move current session-local context objects from global source to local source (isolating them from outer noise)
    bool copySuccess = CopyJitSource(jitSource, localSource, cloneContext, JIT_CONTEXT_LOCAL, sourceOp);
    (void)pthread_mutex_unlock(&jitSource->m_lock);

    if (!copySuccess) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "Process DDL event", "Failed to copy JIT source");
        DestroyJitSource(localSource);
        localSource = nullptr;
    }
    return localSource;
}

extern JitSource* CloneGlobalJitSource(JitSource* jitSource)
{
    JitSource* globalSource = AllocJitSource(jitSource->m_queryString, JIT_CONTEXT_GLOBAL);
    if (globalSource == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Process DDL event", "Failed to allocate pooled global JIT source");
        return nullptr;
    }

    MOT_LOG_TRACE(
        "Created jit-source object %p during clone-global-source: %s", globalSource, globalSource->m_queryString);

    // fill in basic details and then just merge back
    globalSource->m_commandType = jitSource->m_commandType;
    globalSource->m_contextType = jitSource->m_contextType;
    globalSource->m_functionOid = jitSource->m_functionOid;
    globalSource->m_functionTxnId = jitSource->m_functionTxnId;
    globalSource->m_expireTxnId = jitSource->m_expireTxnId;

    JitContextList rollbackInvokeContextList;
    MergeJitSource(jitSource, globalSource, true, &rollbackInvokeContextList);
    MOT_ASSERT(rollbackInvokeContextList.Empty());
    return globalSource;
}

extern void MergeJitSource(
    JitSource* localSource, JitSource* globalSource, bool applyLocalChanges, JitContextList* rollbackInvokeContextList)
{
    MOT_LOG_TRACE("Merging local JIT source %p with state %s into global JIT source %p: %s",
        localSource,
        JitCodegenStateToString(localSource->m_codegenState),
        globalSource,
        localSource->m_queryString);

    bool isInvokeQuery = IsInvokeQuerySource(localSource);

    // in case of commit we want to preserve the timestamp
    TimestampTz lastChangeTimestamp = localSource->m_timestamp;

    // in case of global publish (commit use case), we first invalidate all other session's contexts (because they need
    // to get new jitted function and relation ids and pointers)
    (void)pthread_mutex_lock(&globalSource->m_lock);
    if (applyLocalChanges) {
        // we first expire all contexts (also session local because they need to use a different table pointer)
        SetJitSourceExpired(globalSource, 0, (localSource->m_codegenState == JitCodegenState::JIT_CODEGEN_REPLACED));
    }

    // then we put back all JIT context objects and invalidate them as well (whether commit occurred or rollback)
    if (localSource->m_contextList != nullptr) {
        MotJitContext* currItr = localSource->m_contextList;
        MotJitContext* nextItr = nullptr;
        while (currItr != nullptr) {
            MOT_LOG_TRACE("Moving back JIT context %p from local source %p to global source %p",
                currItr,
                localSource,
                globalSource);

            // update lists
            nextItr = currItr->m_nextInSource;
            currItr->m_nextInSource = globalSource->m_contextList;
            globalSource->m_contextList = currItr;
            currItr->m_jitSource = globalSource;

            // update fields
            currItr->m_queryString = globalSource->m_queryString;
            if (currItr->m_sourceJitContext) {
                MOT_ATOMIC_DEC(currItr->m_sourceJitContext->m_useCount);
                MOT_LOG_TRACE("MergeJitSource(): Decremented use count of source context %p to: %u",
                    currItr->m_sourceJitContext,
                    (uint32_t)MOT_ATOMIC_LOAD(currItr->m_sourceJitContext->m_useCount));
            }
            currItr->m_sourceJitContext = globalSource->m_sourceJitContext;
            if (currItr->m_sourceJitContext) {
                MOT_ATOMIC_INC(currItr->m_sourceJitContext->m_useCount);
                MOT_LOG_TRACE("MergeJitSource(): Incremented use count of source context %p to: %u",
                    currItr->m_sourceJitContext,
                    (uint32_t)MOT_ATOMIC_LOAD(currItr->m_sourceJitContext->m_useCount));
            }

            // in case of commit we need to re-fetch tables so we purge
            PurgeJitContext(currItr, 0);  // no relation id means unconditional purge
            if (!applyLocalChanges || !IsJitContextUsageGlobal(currItr->m_usage)) {
                // in case of roll-back we need to rebuild everything from scratch for current session contexts
                InvalidateJitContext(currItr, 0, JIT_CONTEXT_INVALID);  // no relation id means unconditional purge
                // we also need to make sure that the cached plan pointer is in-place (it might be remove during
                // transactional revalidation)
            }

            if (!applyLocalChanges && isInvokeQuery) {
                // In case of rollback and if this is a invoke query, we must remove the attached invoke context and
                // destroy it (done in the caller) before the local function source is deprecated.
                // Otherwise, deprecation of local function source will fail.
                JitQueryContext* queryContext = (JitQueryContext*)currItr;
                if (queryContext->m_invokeContext != nullptr) {
                    rollbackInvokeContextList->PushBack(queryContext->m_invokeContext);
                    queryContext->m_invokeContext = nullptr;
                }
            }
            currItr = nextItr;
        }
        localSource->m_contextList = nullptr;
        // retain state if committing non-ready source
        if (applyLocalChanges && (localSource->m_codegenState != JitCodegenState::JIT_CODEGEN_READY)) {
            globalSource->m_codegenState = localSource->m_codegenState;
            globalSource->m_status = localSource->m_status;
        }
    }

    // in case of global publish (commit use case), we install the locally produced context if there is any
    if (applyLocalChanges && (localSource->m_codegenState == JitCodegenState::JIT_CODEGEN_READY)) {
        MOT_ASSERT(localSource->m_sourceJitContext != nullptr);
        MOT_ASSERT(globalSource->m_sourceJitContext == nullptr);
        MotJitContext* readyJitContext = localSource->m_sourceJitContext;
        localSource->m_sourceJitContext = nullptr;
        if (readyJitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
            PurgeJitContext(readyJitContext, 0);  // force re-fetch even for source context
        }

        // install new context in global source
        (void)SetJitSourceReady(globalSource, readyJitContext, &localSource->m_codegenStats);

        // Attention: although table/index ids are valid (even after drop and create), new table pointers have not been
        // set yet in the table manager. When first session revalidates we can re-fetch table pointers, so we mark the
        // source context is invalid due to table pointers.
        if (globalSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
            MOT_ATOMIC_STORE(globalSource->m_sourceJitContext->m_validState, JIT_CONTEXT_RELATION_INVALID);
        }
        // override artificial changes in timestamp
        globalSource->m_timestamp = lastChangeTimestamp;
    }
    (void)pthread_mutex_unlock(&globalSource->m_lock);
}

extern void MoveCurrentSessionContexts(JitSource* source, JitSource* target, JitSourceOp sourceOp)
{
    MOT::SessionId currSessionId = MOT_GET_CURRENT_SESSION_ID();
    if (source->m_contextList != nullptr) {
        MotJitContext* prevItr = nullptr;
        MotJitContext* currItr = source->m_contextList;
        while (currItr != nullptr) {
            if ((currItr->m_usage == JIT_CONTEXT_LOCAL) && (currItr->m_sessionId == currSessionId)) {
                MOT_LOG_TRACE("Moving JIT context %p from source %p to source %p", currItr, source, target);

                // every moved context must be invalidated, since local session just received DDL event
                if (sourceOp == JitSourceOp::JIT_SOURCE_PURGE_ONLY) {
                    PurgeJitContext(currItr, 0);
                } else if (sourceOp == JitSourceOp::JIT_SOURCE_INVALIDATE) {
                    PurgeJitContext(currItr, 0);
                    InvalidateJitContext(currItr, 0, JIT_CONTEXT_INVALID);
                }

                // update fields
                currItr->m_jitSource = target;
                if (currItr->m_sourceJitContext) {
                    MOT_ATOMIC_DEC(currItr->m_sourceJitContext->m_useCount);
                    MOT_LOG_TRACE("MoveCurrentSessionContexts(): Decremented use count of source context %p to: %u",
                        currItr->m_sourceJitContext,
                        (uint32_t)MOT_ATOMIC_LOAD(currItr->m_sourceJitContext->m_useCount));
                }
                currItr->m_sourceJitContext = target->m_sourceJitContext;
                if (currItr->m_sourceJitContext) {
                    MOT_ATOMIC_INC(currItr->m_sourceJitContext->m_useCount);
                    MOT_LOG_TRACE("MoveCurrentSessionContexts(): Incremented use count of source context %p to: %u",
                        currItr->m_sourceJitContext,
                        (uint32_t)MOT_ATOMIC_LOAD(currItr->m_sourceJitContext->m_useCount));
                }
                currItr->m_queryString = target->m_queryString;

                // fix lists
                if (prevItr == nullptr) {  // unlink head
                    source->m_contextList = currItr->m_nextInSource;
                    currItr->m_nextInSource = target->m_contextList;
                    target->m_contextList = currItr;
                    currItr = source->m_contextList;
                } else {
                    prevItr->m_nextInSource = currItr->m_nextInSource;
                    currItr->m_nextInSource = target->m_contextList;
                    target->m_contextList = currItr;
                    currItr = prevItr->m_nextInSource;
                }
            } else {  // iterate to next context
                prevItr = currItr;
                currItr = currItr->m_nextInSource;
            }
        }
    }
}

extern bool CleanUpDeprecateJitSourceContexts(JitSource* source)
{
    bool result = false;
    JitContextList deprecatedContexts;
    LockJitSource(source);
    if (source->m_deprecateContextList != nullptr) {
        MOT_LOG_TRACE("Cleaning up deprecate JIT context objects in JIT source %p", source);
        uint32_t contextsRemoved = 0;
        MotJitContext* itr = source->m_deprecateContextList;
        MotJitContext* prev = nullptr;
        MotJitContext* next = nullptr;
        while (itr != nullptr) {
            next = itr->m_nextDeprecate;
            if (MOT_ATOMIC_LOAD(itr->m_useCount) == 0) {
                if (prev == nullptr) {  // unlinking head
                    MOT_ASSERT(source->m_deprecateContextList == itr);
                    source->m_deprecateContextList = itr->m_nextDeprecate;
                } else {
                    prev->m_nextDeprecate = itr->m_nextDeprecate;
                }
                itr->m_jitSource = nullptr;  // avoid warning
                MOT_LOG_TRACE("Removed deprecate JIT context %p in source %p: %s", itr, source, source->m_queryString);
                deprecatedContexts.PushBack(itr);
                ++contextsRemoved;
            } else {
                prev = itr;
            }
            itr = next;
        }
        MOT_LOG_TRACE("Removed %u deprecate source context objects in JIT source %p", contextsRemoved, source);
    }
    if ((source->m_deprecateContextList == nullptr) && (source->m_contextList == nullptr)) {
        result = true;
    }
    UnlockJitSource(source);

    MotJitContext* contextItr = deprecatedContexts.Begin();
    while (contextItr != nullptr) {
        MotJitContext* jitContext = contextItr;
        contextItr = contextItr->m_next;
        jitContext->m_next = nullptr;
        MOT_LOG_TRACE("Destroying deprecate JIT source context %p", jitContext);
        DestroyJitContext(jitContext);
    }

    return result;
}

static void JitSourcePurgeContextList(JitSource* jitSource, uint64_t relationId)
{
    MOT_LOG_TRACE("Purging all JIT context objects by relation id %" PRIu64, relationId);
    MotJitContext* itr = jitSource->m_contextList;
    while (itr != nullptr) {
        PurgeJitContext(itr, relationId);
        itr = itr->m_nextInSource;
    }
}

static void JitSourceInformCompileEvent(JitSource* jitSource, JitSourceList* parentSourceList, uint8_t event)
{
    MotJitContext* itr = jitSource->m_contextList;
    while (itr != nullptr) {
        if (event == JIT_CONTEXT_DONE_COMPILE) {
            if (IsJitContextPendingCompile(itr)) {
                MarkJitContextDoneCompile(itr);
            }
        } else {
            MarkJitContextErrorCompile(itr);
        }

        // special use case: this is a JIT SP source, so we want to inform also other invoke queries that use this SP
        if ((parentSourceList != nullptr) && (itr->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION)) {
            MotJitContext* parentContext = itr->m_parentContext;
            if (parentContext != nullptr) {
                JitSource* parentSource = parentContext->m_jitSource;
                if (parentSource != nullptr) {
                    parentSourceList->PushBack(parentSource);
                }
            }
        }
        MOT_ASSERT(itr->m_contextType == jitSource->m_contextType);
        itr = itr->m_nextInSource;
    }
}

static void JitSourceListInformCompileEvent(JitSourceList* sourceList, uint8_t event)
{
    JitSource* itr = sourceList->Begin();
    while (itr != nullptr) {
        JitSource* jitSource = itr;
        itr = itr->m_next;
        jitSource->m_next = nullptr;
        LockJitSource(jitSource);
        if (jitSource->m_pendingChildCompile == JIT_CONTEXT_PENDING_COMPILE) {
            if (event == JIT_CONTEXT_DONE_COMPILE) {
                // trigger attempt to revalidate on next invocation
                jitSource->m_pendingChildCompile = JIT_CONTEXT_DONE_COMPILE;
                JitSourceInformCompileDone(jitSource);
            } else {
                // trigger attempt to revalidate on next invocation
                jitSource->m_pendingChildCompile = JIT_CONTEXT_ERROR_COMPILE;
                JitSourceInformCompileError(jitSource);
            }
        }
        UnlockJitSource(jitSource);
    }
}

static void JitSourceInformCompileDone(JitSource* jitSource, JitSourceList* parentSourceList /* = nullptr */)
{
    MOT_LOG_TRACE("Informing all JIT context objects compilation is done: %s", jitSource->m_queryString);
    JitSourceInformCompileEvent(jitSource, parentSourceList, JIT_CONTEXT_DONE_COMPILE);
}

static void JitSourceInformCompileError(JitSource* jitSource, JitSourceList* parentSourceList /* = nullptr */)
{
    MOT_LOG_TRACE("Informing all JIT context objects compilation finished with error: %s", jitSource->m_queryString);
    JitSourceInformCompileEvent(jitSource, parentSourceList, JIT_CONTEXT_ERROR_COMPILE);
}

static void JitSourceListInformCompileDone(JitSourceList* sourceList, const char* queryString)
{
    MOT_LOG_TRACE("Informing all JIT source parents compilation is done: %s", queryString);
    JitSourceListInformCompileEvent(sourceList, JIT_CONTEXT_DONE_COMPILE);
}

static void JitSourceListInformCompileError(JitSourceList* sourceList, const char* queryString)
{
    MOT_LOG_TRACE("Informing all JIT source parents compilation is finished with error: %s", queryString);
    JitSourceListInformCompileEvent(sourceList, JIT_CONTEXT_ERROR_COMPILE);
}

static void JitSourceInvalidateContextList(JitSource* jitSource, uint64_t relationId)
{
    MOT_LOG_TRACE("Invalidating all JIT context objects related to source");
    MotJitContext* itr = jitSource->m_contextList;
    while (itr != nullptr) {
        InvalidateJitContext(itr, relationId);
        itr = itr->m_nextInSource;
    }
}

static void JitSourceDeprecateContextList(JitSource* jitSource)
{
    MOT_LOG_TRACE("Deprecating all JIT context objects related to JIT source %p", jitSource);
    MotJitContext* itr = jitSource->m_contextList;
    while (itr != nullptr) {
        DeprecateJitContext(itr);
        itr = itr->m_nextInSource;
    }
}

static void RemoveJitSourceContextImpl(JitSource* jitSource, MotJitContext* jitContext)
{
    MOT_ASSERT(jitContext->m_jitSource == jitSource);
    MOT_LOG_TRACE("Removing JIT context %p from source %p/%p", jitContext, jitSource, jitContext->m_sourceJitContext);
    MotJitContext* prev = nullptr;
    MotJitContext* curr = jitSource->m_contextList;
    while ((curr != nullptr) && (jitContext != curr)) {
        prev = curr;
        curr = curr->m_nextInSource;
    }
    if (curr == jitContext) {
        MOT_LOG_TRACE("JIT context %p found in source %p, now removing", jitContext, jitSource);
        if (prev != nullptr) {
            MOT_ASSERT(prev->m_nextInSource == jitContext);
            prev->m_nextInSource = curr->m_nextInSource;
        } else {  // curr is head
            MOT_ASSERT(curr == jitSource->m_contextList);
            jitSource->m_contextList = curr->m_nextInSource;
        }
    } else {
        MOT_LOG_WARN("Cannot remove JIT context %p from source %p: context not found", jitContext, jitSource);
        MOT_ASSERT(false);
    }
}

/*
 * NOTE: If the source context can be recycled, it is returned in the output parameter (recyclableContext).
 * Caller's responsibility to free recyclableContext if it is not NULL, since it should be done outside the
 * JIT source lock. Otherwise, it could lead to deadlock when in RemoveJitSourceContext() when trying to
 * ScheduleDeprecateJitSourceCleanUp().
 */
static void DeprecateSourceJitContext(JitSource* jitSource, MotJitContext** recyclableContext)
{
    MotJitContext* sourceContext = jitSource->m_sourceJitContext;
    *recyclableContext = nullptr;
    MOT_LOG_TRACE(
        "Deprecating source JIT context %p of JIT source %p: %s", sourceContext, jitSource, jitSource->m_queryString);
    if (MOT_ATOMIC_LOAD(sourceContext->m_useCount) == 0) {
        // We can safely destroy the context, but it should be done outside the JIT source lock. Otherwise, it could
        // lead to deadlock when in RemoveJitSourceContext() when trying to ScheduleDeprecateJitSourceCleanUp().
        // So we fill the output parameter and let the caller destroy the context.
        MOT_LOG_TRACE("Source JIT context %p is ready for recycling: %s", sourceContext, jitSource->m_queryString);
        *recyclableContext = sourceContext;
    } else {
        MOT_ATOMIC_STORE(sourceContext->m_validState, JIT_CONTEXT_DEPRECATE);
        MOT_ASSERT(!DeprecateListContainsJitContext(jitSource, sourceContext));
        sourceContext->m_nextDeprecate = jitSource->m_deprecateContextList;
        jitSource->m_deprecateContextList = sourceContext;
        MOT_LOG_TRACE("Source JIT context %p is pushed to JIT source %p deprecated context list: %s",
            sourceContext,
            jitSource,
            jitSource->m_queryString);
    }
    jitSource->m_sourceJitContext = nullptr;
}

static void UnlinkDeprecateJitContext(JitSource* jitSource, MotJitContext* sourceJitContext)
{
    MOT_ASSERT(MOT_ATOMIC_LOAD(sourceJitContext->m_useCount) == 0);
    MOT_LOG_TRACE("Unlinking deprecate JIT source %p source context %p: %s",
        jitSource,
        sourceJitContext,
        jitSource->m_queryString);

    // unlink deprecate source from deprecate list and destroy it
    // NOTE: at this point we are already locked when lock is needed (see RevalidateJitContext() calling
    // ReplenishJitContext(), which calls this function)
    MotJitContext* itr = jitSource->m_deprecateContextList;
    MotJitContext* prev = nullptr;
    while ((itr != nullptr) && (itr != sourceJitContext)) {
        prev = itr;
        itr = itr->m_nextDeprecate;
    }
    if (itr == nullptr) {
        MOT_LOG_TRACE("JIT context %p not found in deprecate list of JIT source %p: %s",
            sourceJitContext,
            jitSource,
            jitSource->m_queryString);
        MOT_ASSERT(false);
    } else {
        MOT_ASSERT(itr == sourceJitContext);
        if (prev == nullptr) {  // un-linking head
            MOT_ASSERT(jitSource->m_deprecateContextList == sourceJitContext);
            jitSource->m_deprecateContextList = sourceJitContext->m_nextDeprecate;
        } else {
            prev->m_nextDeprecate = sourceJitContext->m_nextDeprecate;
        }
        sourceJitContext->m_jitSource = nullptr;  // avoid warning
        MOT_LOG_TRACE("Unlinked deprecate JIT source context %p in source %p: %s",
            sourceJitContext,
            jitSource,
            jitSource->m_queryString);
    }
}

extern void UnlinkLocalJitSourceContexts(JitSource* localSource)
{
    MOT_ASSERT(localSource->m_usage == JIT_CONTEXT_LOCAL);
    MOT_LOG_DEBUG(
        "Unlinking all contexts of local JIT source %p with query string: %s", localSource, localSource->m_queryString);

    if (localSource->m_contextList != nullptr) {
        MotJitContext* itr = localSource->m_contextList;
        while (itr != nullptr) {
            MotJitContext* cleanupContext = itr;
            itr = itr->m_nextInSource;

            MOT_LOG_TRACE("Unlinking JIT context %p from local JIT source %p with query string: %s",
                cleanupContext,
                localSource,
                localSource->m_queryString);

            // Purge and invalidate the context.
            PurgeJitContext(cleanupContext, 0);
            InvalidateJitContext(cleanupContext, 0, JIT_CONTEXT_INVALID);

            // Unlink from the source.
            MotJitContext* sourceContext = cleanupContext->m_sourceJitContext;
            if (sourceContext != nullptr) {
                MOT_ATOMIC_DEC(sourceContext->m_useCount);
                MOT_LOG_TRACE("CleanupLocalJitSourceContexts(): Decremented use count of source context %p to: %u",
                    sourceContext,
                    (uint32_t)MOT_ATOMIC_LOAD(sourceContext->m_useCount));
                cleanupContext->m_sourceJitContext = nullptr;
            }
            cleanupContext->m_jitSource = nullptr;
            cleanupContext->m_nextInSource = nullptr;
        }
        localSource->m_contextList = nullptr;
    }
}

#ifdef MOT_DEBUG
static bool DeprecateListContainsJitContext(JitSource* jitSource, MotJitContext* jitContext)
{
    MotJitContext* itr = jitSource->m_deprecateContextList;
    while ((itr != nullptr) && (itr != jitContext)) {
        itr = itr->m_nextDeprecate;
    }
    return (itr == jitContext);
}

static bool ContextListContainsJitContext(JitSource* jitSource, MotJitContext* jitContext)
{
    MotJitContext* itr = jitSource->m_contextList;
    while ((itr != nullptr) && (itr != jitContext)) {
        itr = itr->m_nextInSource;
    }
    return (itr == jitContext);
}

static void VerifyJitSourceStateTrans(
    JitSource* jitSource, MotJitContext* readySourceJitContext, JitCodegenState nextState)
{
    // first verify JIT source state
    MOT_ASSERT(jitSource != nullptr);
    if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_READY) {
        MOT_ASSERT(jitSource->m_sourceJitContext != nullptr);
    } else if (jitSource->m_codegenState != JitCodegenState::JIT_CODEGEN_UNAVAILABLE &&
               jitSource->m_codegenState != JitCodegenState::JIT_CODEGEN_PENDING) {
        // unavailable state might have a JIT context when re-validating
        MOT_ASSERT(jitSource->m_sourceJitContext == nullptr);
    }

    // next verify target state
    if (nextState == JitCodegenState::JIT_CODEGEN_READY) {
        MOT_ASSERT(readySourceJitContext != nullptr);
    } else {
        MOT_ASSERT(readySourceJitContext == nullptr);
    }

    // now verify state transition
    if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_UNAVAILABLE) {
        MOT_ASSERT((nextState == JitCodegenState::JIT_CODEGEN_READY) ||
                   (nextState == JitCodegenState::JIT_CODEGEN_ERROR) ||
                   (nextState == JitCodegenState::JIT_CODEGEN_EXPIRED));
    } else if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_READY) {
        MOT_ASSERT((nextState == JitCodegenState::JIT_CODEGEN_EXPIRED) ||
                   (nextState == JitCodegenState::JIT_CODEGEN_DROPPED) ||
                   (nextState == JitCodegenState::JIT_CODEGEN_ERROR));
    } else if (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_EXPIRED) {
        MOT_ASSERT((nextState == JitCodegenState::JIT_CODEGEN_READY) ||
                   (nextState == JitCodegenState::JIT_CODEGEN_ERROR) ||
                   (nextState == JitCodegenState::JIT_CODEGEN_EXPIRED));
    } else if ((jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_DROPPED) ||
               (jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_REPLACED)) {
        MOT_ASSERT((nextState == JitCodegenState::JIT_CODEGEN_READY) ||
                   (nextState == JitCodegenState::JIT_CODEGEN_ERROR) ||
                   (nextState == JitCodegenState::JIT_CODEGEN_EXPIRED));
    }
}
#endif
}  // namespace JitExec
