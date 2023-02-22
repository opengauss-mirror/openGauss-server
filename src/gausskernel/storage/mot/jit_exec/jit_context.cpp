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
 * jit_context.cpp
 *    The context for executing a jitted function.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_context.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * ATTENTION:
 * 1. Be sure to include gscodegen.h before anything else to avoid clash with PM definition in datetime.h.
 * 2. Be sure to include libintl.h before gscodegen.h to avoid problem with gettext.
 */
#include "libintl.h"
#include "codegen/gscodegen.h"
#include "postgres.h"
#include "knl/knl_session.h"
#include "storage/ipc.h"
#include "executor/executor.h"

#include "global.h"
#include "jit_context.h"
#include "jit_context_pool.h"
#include "jit_common.h"
#include "mot_internal.h"
#include "jit_source.h"
#include "mm_global_api.h"
#include "mot_atomic_ops.h"
#include "jit_plan_sp.h"
#include "debug_utils.h"
#include "jit_source_map.h"
#include "jit_statistics.h"

namespace JitExec {
DECLARE_LOGGER(MotJitContext, JitExec);

// The global JIT context pool
static JitContextPool g_globalJitCtxPool __attribute__((aligned(64))) = {0};

// forward declarations
static JitContextPool* AllocSessionJitContextPool();

static bool AllocJitQueryExecState(JitQueryContext* jitContext);
static bool PrepareJitQueryContext(JitQueryContext* jitContext);
static bool PrepareMainSearchKey(JitQueryContext* jitContext, JitQueryExecState* execState);
static bool PrepareUpdateBitmap(JitQueryContext* jitContext, JitQueryExecState* execState);
static bool PrepareEndIteratorKey(JitQueryContext* jitContext, JitQueryExecState* execState);
static bool PrepareInnerSrearchKey(JitQueryContext* jitContext, JitQueryExecState* execState);
static bool PrepareInnerEndIteratorKey(JitQueryContext* jitContext, JitQueryExecState* execState);
static bool PrepareCompoundSubQuery(JitQueryContext* jitContext, JitQueryExecState* execState);
static bool PrepareInvokeContext(JitQueryContext* jitContext, JitQueryExecState* execState);

static bool AllocJitFunctionExecState(JitFunctionContext* jitContext);
static bool PrepareJitFunctionContext(JitFunctionContext* jitContext);
static bool PrepareCallSite(
    JitFunctionContext* jitContext, JitInvokedQueryExecState* execState, JitCallSite* callSite, int subQueryId);

static void DestroyJitQueryContext(JitQueryContext* jitContext, bool isDropCachedPlan = false);
static void DestroyJitFunctionContext(JitFunctionContext* jitContext, bool isDropCachedPlan = false);
static MOT::Key* PrepareJitSearchKey(MotJitContext* jitContext, MOT::Index* index);
static bool CloneTupleDesc(TupleDesc source, TupleDesc* target, JitContextUsage usage);
static bool CloneJitQueryContext(JitQueryContext* source, JitQueryContext* target, JitContextUsage usage);
static bool CloneJitFunctionContext(JitFunctionContext* source, JitFunctionContext* target, JitContextUsage usage);
static void CleanupJitContextPrimary(JitQueryContext* jitContext, bool isDropCachedPlan = false);
static void CleanupJitContextInner(JitQueryContext* jitContext, bool isDropCachedPlan = false);
static void CleanupJitSubQueryContextArray(JitQueryContext* jitContext, bool isDropCachedPlan = false);
static void CleanupJitSubQueryContext(
    JitSubQueryContext* subQueryContext, JitSubQueryExecState* subQueryExecState, bool isDropCachedPlan = false);
static bool JitQueryContextRefersRelation(JitQueryContext* jitContext, uint64_t relationId);
static bool JitFunctionContextRefersRelation(JitFunctionContext* jitContext, uint64_t relationId);
static void PurgeJitQueryContext(JitQueryContext* jitContext, uint64_t relationId);
static void PurgeJitFunctionContext(JitFunctionContext* jitContext, uint64_t relationId);

static bool ReplenishDatumArray(JitDatumArray* source, JitDatumArray* target, JitContextUsage usage, int depth);
static bool ReplenishInvokeParams(JitQueryContext* source, JitQueryContext* target, JitContextUsage usage, int depth);
static bool ReplenishSubQueryArray(JitQueryContext* source, JitQueryContext* target, JitContextUsage usage, int depth);
static bool ReplenishAggregateArray(JitQueryContext* source, JitQueryContext* target, JitContextUsage usage, int depth);
static bool ReplenishJitQueryContext(JitQueryContext* source, JitQueryContext* target, int depth);
static bool ReplenishParamListInfo(JitCallSite* target, JitCallSite* source, JitContextUsage usage, int depth);
static bool ReplenishJitFunctionContext(JitFunctionContext* source, JitFunctionContext* target, int depth);
static bool ReplenishJitContext(MotJitContext* source, MotJitContext* target, int depth);
static bool RevalidateJitQueryContext(MotJitContext* jitContext);
static bool RevalidateJitFunctionContext(MotJitContext* jitContext);
static void DestroyJitNonNativeSortExecState(
    JitNonNativeSortExecState* jitNonNativeSortExecState, JitContextUsage usage);
static void PropagateValidState(MotJitContext* jitContext, uint8_t invalidFlag, uint64_t relationId);

extern bool InitGlobalJitContextPool()
{
    return InitJitContextPool(&g_globalJitCtxPool, JIT_CONTEXT_GLOBAL, GetMotCodegenLimit());
}

extern void DestroyGlobalJitContextPool()
{
    DestroyJitContextPool(&g_globalJitCtxPool);
}

extern MotJitContext* AllocJitContext(JitContextUsage usage, JitContextType type)
{
    MotJitContext* result = nullptr;
    if (IsJitContextUsageGlobal(usage)) {
        // allocate from global pool
        result = AllocPooledJitContext(&g_globalJitCtxPool);
        if (result != nullptr) {
            JitStatisticsProvider::GetInstance().AddGlobalBytes((int64_t)GetJitContextSize());
        }
    } else {
        // allocate from session local pool (create pool on demand and schedule cleanup during end of session)
        if (u_sess->mot_cxt.jit_session_context_pool == nullptr) {
            u_sess->mot_cxt.jit_session_context_pool = AllocSessionJitContextPool();
            if (u_sess->mot_cxt.jit_session_context_pool == nullptr) {
                return nullptr;
            }
        }
        result = AllocPooledJitContext(u_sess->mot_cxt.jit_session_context_pool);
        if (result != nullptr) {
            result->m_sessionId = MOT_GET_CURRENT_SESSION_ID();
            JitStatisticsProvider::GetInstance().AddSessionBytes((int64_t)GetJitContextSize());
            ++u_sess->mot_cxt.jit_context_count;
            MOT_LOG_TRACE("AllocJitContext(): Current session (%u) JIT context count: %u",
                MOT_GET_CURRENT_SESSION_ID(),
                u_sess->mot_cxt.jit_context_count);
        }
    }

    if (result != nullptr) {
        result->m_usage = usage;
        result->m_contextType = type;
        MOT_LOG_TRACE("Allocated %s JIT %s context %p [%u:%u:%u]",
            JitContextUsageToString(result->m_usage),
            JitContextTypeToString(result->m_contextType),
            result,
            result->m_poolId,
            result->m_subPoolId,
            result->m_contextId);
    } else {
        MOT_LOG_ERROR(
            "Failed to allocate %s JIT %s context", JitContextUsageToString(usage), JitContextTypeToString(type));
    }
    return result;
}

extern void FreeJitContext(MotJitContext* jitContext)
{
    if (jitContext != nullptr) {
        if (IsJitContextUsageGlobal(jitContext->m_usage)) {
            FreePooledJitContext(&g_globalJitCtxPool, jitContext);
            JitStatisticsProvider::GetInstance().AddGlobalBytes(-((int64_t)GetJitContextSize()));
        } else {
            // in this scenario it is always called by the session who created the context
            --u_sess->mot_cxt.jit_context_count;
            MOT_LOG_TRACE("FreeJitContext(): Current session (%u) JIT context count: %u",
                MOT_GET_CURRENT_SESSION_ID(),
                u_sess->mot_cxt.jit_context_count);
            FreePooledJitContext(u_sess->mot_cxt.jit_session_context_pool, jitContext);
            JitStatisticsProvider::GetInstance().AddSessionBytes(-((int64_t)GetJitContextSize()));
        }
    }
}

static bool CloneDatumArray(JitDatumArray* source, JitDatumArray* target, JitContextUsage usage)
{
    uint32_t datumCount = source->m_datumCount;
    if (datumCount == 0) {
        target->m_datumCount = 0;
        target->m_datums = nullptr;
        return true;
    }

    size_t allocSize = sizeof(JitDatum) * datumCount;
    JitDatum* datumArray = (JitDatum*)JitMemAlloc(allocSize, usage);
    if (datumArray == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compile", "Failed to allocate %u bytes for datum array", (unsigned)allocSize);
        return false;
    }

    for (uint32_t i = 0; i < datumCount; ++i) {
        JitDatum* datum = (JitDatum*)&source->m_datums[i];
        datumArray[i].m_isNull = datum->m_isNull;
        datumArray[i].m_type = datum->m_type;
        if (!datum->m_isNull) {
            if (IsPrimitiveType(datum->m_type)) {
                datumArray[i].m_datum = datum->m_datum;
            } else {
                if (!CloneDatum(datum->m_datum, datum->m_type, &datumArray[i].m_datum, usage)) {
                    MOT_REPORT_ERROR(MOT_ERROR_OOM, "JIT Compile", "Failed to clone datum array entry");
                    for (uint32_t j = 0; j < i; ++j) {
                        if (!IsPrimitiveType(datumArray[j].m_type)) {
                            JitMemFree(DatumGetPointer(datumArray[j].m_datum), usage);
                        }
                    }
                    JitMemFree(datumArray, usage);
                    return false;
                }
            }
        }
    }

    target->m_datums = datumArray;
    target->m_datumCount = datumCount;
    return true;
}

extern MotJitContext* CloneJitContext(MotJitContext* source, JitContextUsage usage)
{
    const char* itemName = (source->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) ? "query" : "function";
    MOT_LOG_TRACE("Cloning %s JIT context %p (valid state %x) of %s: %s",
        JitContextUsageToString(source->m_usage),
        source,
        MOT_ATOMIC_LOAD(source->m_validState),
        itemName,
        source->m_queryString);
    MotJitContext* target = AllocJitContext(usage, source->m_contextType);
    if (target == nullptr) {
        MOT_LOG_TRACE("Failed to allocate JIT context object");
        return nullptr;
    }

    // no need to clone module/code-gen object (they are safe in the source context)
    target->m_llvmFunction = source->m_llvmFunction;
    target->m_llvmSPFunction = source->m_llvmSPFunction;
    target->m_commandType = source->m_commandType;
    target->m_validState = source->m_validState;
    target->m_queryString = source->m_queryString;
    AddJitSourceContext(source->m_jitSource, target);  // register target for cleanup due to DDL
    if (!CloneDatumArray(&source->m_constDatums, &target->m_constDatums, usage)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "JIT Compile", "Failed to clone constant datum array");
        DestroyJitContext(target);
        return nullptr;
    }

    bool result = true;
    if (source->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
        result = CloneJitQueryContext((JitQueryContext*)source, (JitQueryContext*)target, usage);
    } else if (source->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) {
        result = CloneJitFunctionContext((JitFunctionContext*)source, (JitFunctionContext*)target, usage);
    }

    if (!result) {
        MOT_LOG_TRACE("Failed to clone %s JIT context %p: %s",
            JitContextUsageToString(source->m_usage),
            source,
            itemName,
            source->m_queryString);
        DestroyJitContext(target);
        target = nullptr;
    } else {
        if (source->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
            MOT_LOG_TRACE("Cloned %s query JIT context %p into %s JIT context %p (table=%p): %s",
                JitContextUsageToString(source->m_usage),
                source,
                JitContextUsageToString(usage),
                target,
                ((JitQueryContext*)target)->m_table,
                source->m_queryString);
        } else {
            MOT_LOG_TRACE("Cloned %s function JIT context %p into %s JIT context %p (procid=%u): %s",
                JitContextUsageToString(source->m_usage),
                source,
                JitContextUsageToString(usage),
                target,
                ((JitFunctionContext*)target)->m_functionOid,
                source->m_queryString);
        }
    }

    return target;
}

static void DestroyJitNonNativeSortParamsMembers(JitNonNativeSortParams* sortParams, JitContextUsage usage)
{
    if (sortParams->sortColIdx) {
        JitMemFree(sortParams->sortColIdx, usage);
        sortParams->sortColIdx = nullptr;
    }
    if (sortParams->sortOperators) {
        JitMemFree(sortParams->sortOperators, usage);
        sortParams->sortOperators = nullptr;
    }
    if (sortParams->collations) {
        JitMemFree(sortParams->collations, usage);
        sortParams->collations = nullptr;
    }
    if (sortParams->nullsFirst) {
        JitMemFree(sortParams->nullsFirst, usage);
        sortParams->nullsFirst = nullptr;
    }
}

extern void DestroyJitNonNativeSortParams(JitNonNativeSortParams* sortParams, JitContextUsage usage)
{
    if (sortParams) {
        DestroyJitNonNativeSortParamsMembers(sortParams, usage);
        JitMemFree(sortParams, usage);
    }
}

static bool AllocJitNonNativeSortParamsMembers(JitNonNativeSortParams* sortParams, int numCols, JitContextUsage usage)
{
    if (numCols < 1) {
        MOT_LOG_ERROR("Cannot create JitNonNativeSortParams if numCols is %d", numCols);
        return false;
    }

    sortParams->numCols = numCols;
    uint32_t totalAllocSize = numCols * sizeof(AttrNumber) + 2 * numCols * sizeof(Oid) + numCols * sizeof(bool);

    sortParams->sortColIdx = (AttrNumber*)JitMemAlloc(numCols * sizeof(AttrNumber), usage);
    sortParams->sortOperators = (Oid*)JitMemAlloc(numCols * sizeof(Oid), usage);
    sortParams->collations = (Oid*)JitMemAlloc(numCols * sizeof(Oid), usage);
    sortParams->nullsFirst = (bool*)JitMemAlloc(numCols * sizeof(bool), usage);
    if (sortParams->sortColIdx == nullptr || sortParams->sortOperators == nullptr ||
        sortParams->collations == nullptr || sortParams->nullsFirst == nullptr) {
        MOT_LOG_TRACE("Generate JIT Code",
            "AllocJitNonNativeSortParamsMembers(): Failed to allocate memory for JitNonNativeSortParams members. "
            "total alloc size = %u, numCols = d%, sortColIdx = %p, sortOperators = %p, collations = %p, nullsFirst = "
            "%p",
            totalAllocSize,
            numCols,
            sortParams->sortColIdx,
            sortParams->sortOperators,
            sortParams->collations,
            sortParams->nullsFirst);
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Generate JIT Code",
            "Failed to allocate memory of size %u for Non-native sort params.",
            totalAllocSize);
        DestroyJitNonNativeSortParamsMembers(sortParams, usage);
        return false;
    }

    errno_t erc;
    erc = memset_s(sortParams->sortColIdx, numCols * sizeof(AttrNumber), 0, numCols * sizeof(AttrNumber));
    securec_check(erc, "\0", "\0");
    erc = memset_s(sortParams->sortOperators, numCols * sizeof(Oid), 0, numCols * sizeof(Oid));
    securec_check(erc, "\0", "\0");
    erc = memset_s(sortParams->collations, numCols * sizeof(Oid), 0, numCols * sizeof(Oid));
    securec_check(erc, "\0", "\0");
    erc = memset_s(sortParams->nullsFirst, numCols * sizeof(bool), 0, numCols * sizeof(bool));
    securec_check(erc, "\0", "\0");

    return true;
}

extern JitNonNativeSortParams* AllocJitNonNativeSortParams(int numCols, JitContextUsage usage)
{
    uint64_t allocSize = sizeof(struct JitNonNativeSortParams);
    JitNonNativeSortParams* jitNonNativeSortParams = (JitNonNativeSortParams*)JitMemAlloc(allocSize, usage);
    if (jitNonNativeSortParams == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Generate JIT Code",
            "Failed to allocate %u bytes for JitNonNativeSortParams",
            (unsigned)allocSize);
        return nullptr;
    }

    errno_t erc = memset_s(
        jitNonNativeSortParams, sizeof(struct JitNonNativeSortParams), 0, sizeof(struct JitNonNativeSortParams));
    securec_check(erc, "\0", "\0");

    if (!AllocJitNonNativeSortParamsMembers(jitNonNativeSortParams, numCols, usage)) {
        MOT_LOG_ERROR("Failed to allocate JitNonNativeSortParams members. numcols: %d", numCols);
        DestroyJitNonNativeSortParams(jitNonNativeSortParams, usage);
        jitNonNativeSortParams = nullptr;
    }

    return jitNonNativeSortParams;
}

// Assume all memory in dest object was already allocated
bool CopyJitNonNativeSortParams(JitNonNativeSortParams* src, JitNonNativeSortParams* dest)
{
    MOT_ASSERT(src->numCols == dest->numCols);

    if (src->numCols != dest->numCols) {
        MOT_LOG_ERROR("Cannot copy Non native sort params. src numCols (%d) is not "
                      "equal to dest numCols (%d)",
            src->numCols,
            dest->numCols);

        return false;
    }

    int numCols = src->numCols;

    // Copy data
    errno_t erc;
    erc = memcpy_s(dest->sortColIdx, numCols * sizeof(AttrNumber), src->sortColIdx, numCols * sizeof(AttrNumber));
    securec_check(erc, "\0", "\0");

    erc = memcpy_s(dest->sortOperators, numCols * sizeof(Oid), src->sortOperators, numCols * sizeof(Oid));
    securec_check(erc, "\0", "\0");

    erc = memcpy_s(dest->collations, numCols * sizeof(Oid), src->collations, numCols * sizeof(Oid));
    securec_check(erc, "\0", "\0");

    erc = memcpy_s(dest->nullsFirst, numCols * sizeof(bool), src->nullsFirst, numCols * sizeof(bool));
    securec_check(erc, "\0", "\0");

    dest->numCols = src->numCols;
    dest->plan_node_id = src->plan_node_id;
    dest->bound = src->bound;
    dest->scanDir = src->scanDir;

    return true;
}

extern JitNonNativeSortParams* CloneJitNonNativeSortParams(JitNonNativeSortParams* src, JitContextUsage usage)
{
    JitNonNativeSortParams* jitNonNativeSortParams = AllocJitNonNativeSortParams(src->numCols, usage);

    if (jitNonNativeSortParams == nullptr) {
        MOT_LOG_ERROR("Failed to allocate JitNonNativeSortParams with %d columns", src->numCols);

        return nullptr;
    }

    if (!CopyJitNonNativeSortParams(src, jitNonNativeSortParams)) {
        MOT_LOG_ERROR("Failed to copy non native params with %d columns", src->numCols);

        DestroyJitNonNativeSortParams(jitNonNativeSortParams, usage);
        jitNonNativeSortParams = nullptr;

        return nullptr;
    }

    return jitNonNativeSortParams;
}

static inline void JitQueryContextSetTablesAndIndices(JitQueryContext* source, JitQueryContext* target)
{
    target->m_table = source->m_table;
    target->m_tableId = source->m_tableId;
    target->m_index = source->m_index;
    target->m_indexId = source->m_indexId;
    target->m_innerTable = source->m_innerTable;
    target->m_innerTableId = source->m_innerTableId;
    target->m_innerIndex = source->m_innerIndex;
    target->m_innerIndexId = source->m_innerIndexId;
}

static bool CloneJitQueryContext(JitQueryContext* source, JitQueryContext* target, JitContextUsage usage)
{
    JitQueryContextSetTablesAndIndices(source, target);
    target->m_aggCount = source->m_aggCount;
    target->m_subQueryCount = source->m_subQueryCount;
    target->m_invokeParamCount = source->m_invokeParamCount;
    target->m_subQueryContext = nullptr;
    target->m_invokeContext = nullptr;

    if (source->m_invokeContext != nullptr) {
        MOT_LOG_TRACE("Cloning invoked stored procedure context");
        target->m_invokeContext = (JitFunctionContext*)CloneJitContext(source->m_invokeContext, usage);
        if (target->m_invokeContext == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Generate JIT Code", "Failed to clone invocation context");
            return false;
        }
        target->m_invokeContext->m_parentContext = target;
    }
    if (source->m_invokeParamCount > 0) {
        size_t allocSize = sizeof(JitParamInfo) * source->m_invokeParamCount;
        target->m_invokeParamInfo = (JitParamInfo*)JitMemAlloc(allocSize, usage);
        if (target->m_invokeParamInfo == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Generate JIT Code",
                "Failed to allocate %u bytes for %u invoke parameter mode array items in JIT context object",
                (unsigned)allocSize,
                (unsigned)source->m_invokeParamCount);
            return false;
        }
        errno_t erc = memcpy_s(target->m_invokeParamInfo, allocSize, source->m_invokeParamInfo, allocSize);
        securec_check(erc, "\0", "\0");
    } else {
        target->m_invokeParamInfo = nullptr;
    }

    MOT_ASSERT((target->m_invokeParamCount == 0 && target->m_invokeParamInfo == nullptr) ||
               (target->m_invokeParamCount > 0 && target->m_invokeParamInfo != nullptr));

    if (source->m_invokedQueryString != nullptr) {
        target->m_invokedQueryString = DupString(source->m_invokedQueryString, usage);
        if (target->m_invokedQueryString == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Generate JIT Code",
                "Failed to duplicate invoke query string: %s",
                source->m_invokedQueryString);
            return false;
        }
    }
    target->m_invokedFunctionOid = source->m_invokedFunctionOid;
    target->m_invokedFunctionTxnId = source->m_invokedFunctionTxnId;

    // clone sub-query tuple descriptor array
    if (target->m_subQueryCount > 0) {
        MOT_LOG_TRACE("Cloning %u sub-query data items", (unsigned)source->m_subQueryCount);
        size_t allocSize = sizeof(JitSubQueryContext) * source->m_subQueryCount;
        target->m_subQueryContext = (JitSubQueryContext*)JitMemAllocAligned(allocSize, L1_CACHE_LINE, usage);
        if (target->m_subQueryContext == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Generate JIT Code",
                "Failed to allocate %u bytes for %u sub-query data array in JIT context object",
                (unsigned)allocSize,
                (unsigned)source->m_subQueryCount);
            return false;
        }

        for (uint32_t i = 0; i < source->m_subQueryCount; ++i) {
            target->m_subQueryContext[i].m_commandType = source->m_subQueryContext[i].m_commandType;
            target->m_subQueryContext[i].m_table = source->m_subQueryContext[i].m_table;
            target->m_subQueryContext[i].m_tableId = source->m_subQueryContext[i].m_tableId;
            target->m_subQueryContext[i].m_index = source->m_subQueryContext[i].m_index;
            target->m_subQueryContext[i].m_indexId = source->m_subQueryContext[i].m_indexId;
        }
    }

    if (source->m_nonNativeSortParams) {
        target->m_nonNativeSortParams = CloneJitNonNativeSortParams(source->m_nonNativeSortParams, usage);
        if (target->m_nonNativeSortParams == nullptr) {
            MOT_LOG_ERROR("Failed to clone JitNonNativeSortParams object with %d columns",
                source->m_nonNativeSortParams->numCols);
            return false;
        }
    }

    MOT_LOG_TRACE("Cloned JIT context %p into %p (table=%p)", source, target, target->m_table);
    return true;
}

static bool CloneCallSite(
    JitFunctionContext* jitContext, int subQueryId, JitCallSite* source, JitCallSite* target, JitContextUsage usage)
{
    if (source->m_queryContext == nullptr) {
        target->m_queryContext = nullptr;
        target->m_queryString = DupString(source->m_queryString, usage);
        if (target->m_queryString == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Generate JIT Code",
                "Failed to clone call site query string: %s",
                source->m_queryString);
            return false;
        }
        MOT_LOG_TRACE("CloneCallSite(): Cloned string %p on %s scope into call site %p: %s",
            target->m_queryString,
            IsJitContextUsageGlobal(usage) ? "global" : "local",
            target,
            target->m_queryString);
    } else {
        target->m_queryContext = CloneJitContext(source->m_queryContext, usage);
        if (target->m_queryContext == nullptr) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Generate JIT Code", "Failed to clone sub-query context for function JIT context");
            return false;
        }
        target->m_queryString = nullptr;
    }
    target->m_queryCmdType = source->m_queryCmdType;

    target->m_callParamCount = source->m_callParamCount;
    if (target->m_callParamCount > 0) {
        size_t allocSize = sizeof(JitCallParamInfo) * target->m_callParamCount;
        target->m_callParamInfo = (JitCallParamInfo*)JitMemAlloc(allocSize, usage);
        if (target->m_callParamInfo == nullptr) {
            MOT_LOG_ERROR("Failed to allocate %u bytes for %d call parameters", allocSize, target->m_callParamCount);
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Generate JIT Code", "Failed to clone sub-query parameters for function JIT context");
            return false;
        }
        errno_t erc = memcpy_s(target->m_callParamInfo, allocSize, source->m_callParamInfo, allocSize);
        securec_check(erc, "\0", "\0");
    } else {
        target->m_callParamInfo = nullptr;
    }

    if (!CloneTupleDesc(source->m_tupDesc, &target->m_tupDesc, usage)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Generate JIT Code", "Failed to clone sub-query tuple descriptor");
        return false;
    }
    target->m_exprIndex = source->m_exprIndex;
    target->m_isUnjittableInvoke = source->m_isUnjittableInvoke;
    target->m_isModStmt = source->m_isModStmt;
    target->m_isInto = source->m_isInto;
    return true;
}

static bool CloneTupleDesc(TupleDesc source, TupleDesc* target, JitContextUsage usage)
{
    if (source != nullptr) {
        MemoryContext oldCtx = CurrentMemoryContext;
        if (IsJitContextUsageGlobal(usage)) {
            CurrentMemoryContext = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR);
        } else {
            CurrentMemoryContext = SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR);
        }
        *target = CreateTupleDescCopy(source);
        CurrentMemoryContext = oldCtx;
        if (*target == nullptr) {
            return false;
        }
    } else {
        *target = nullptr;
    }
    return true;
}

static bool CloneResultDescriptors(JitFunctionContext* source, JitFunctionContext* target)
{
    JitContextUsage usage = target->m_usage;

    target->m_compositeResult = source->m_compositeResult;

    if (!CloneTupleDesc(source->m_resultTupDesc, &target->m_resultTupDesc, usage)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Generate JIT Code", "Failed to clone result tuple descriptor for function JIT context");
        return false;
    }

    if (!CloneTupleDesc(source->m_rowTupDesc, &target->m_rowTupDesc, usage)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Generate JIT Code", "Failed to clone row tuple descriptor for function JIT context");
        return false;
    }

    return true;
}

static bool CloneJitFunctionContext(JitFunctionContext* source, JitFunctionContext* target, JitContextUsage usage)
{
    target->m_functionOid = source->m_functionOid;
    target->m_functionTxnId = source->m_functionTxnId;
    target->m_SPArgCount = source->m_SPArgCount;
    target->m_SPSubQueryCount = source->m_SPSubQueryCount;
    if (target->m_SPSubQueryCount == 0) {
        target->m_SPSubQueryList = nullptr;
        return true;
    }

    // in any case of failure caller will call destroy function for safe cleanup
    size_t allocSize = sizeof(JitCallSite) * target->m_SPSubQueryCount;
    target->m_SPSubQueryList = (JitCallSite*)JitMemAlloc(allocSize, usage);
    if (target->m_SPSubQueryList == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Generate JIT Code",
            "Failed to allocate %u bytes for %u sub-query data array in Function JIT context object",
            allocSize,
            (unsigned)target->m_SPSubQueryCount);
        return false;
    }
    errno_t erc = memset_s(target->m_SPSubQueryList, allocSize, 0, allocSize);
    securec_check(erc, "\0", "\0");

    for (uint32_t i = 0; i < target->m_SPSubQueryCount; ++i) {
        JitCallSite* sourceCallSite = &source->m_SPSubQueryList[i];
        JitCallSite* targetCallSite = &target->m_SPSubQueryList[i];
        if (!CloneCallSite(target, i, sourceCallSite, targetCallSite, usage)) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Generate JIT Code", "Failed to clone call-site %d for function JIT context", i);
            return false;
        }
        if (targetCallSite->m_queryContext != nullptr) {
            targetCallSite->m_queryContext->m_parentContext = target;
        }
    }
    target->m_paramCount = source->m_paramCount;
    if (target->m_paramCount == 0) {
        target->m_paramTypes = nullptr;
    } else {
        allocSize = sizeof(Oid) * target->m_paramCount;
        target->m_paramTypes = (Oid*)JitMemAlloc(allocSize, usage);
    }
    if (target->m_paramTypes == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Generate JIT Code",
            "Failed to allocate %u bytes for %u sub-query parameter array in Function JIT context object",
            allocSize,
            target->m_paramCount);
        return false;
    }
    erc = memcpy_s(target->m_paramTypes, allocSize, source->m_paramTypes, allocSize);
    securec_check(erc, "\0", "\0");

    if (!CloneResultDescriptors(source, target)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Generate JIT Code", "Failed to clone result descriptors for function JIT context");
        return false;
    }

    return true;
}

static inline List* GetSubExprTargetList(Query* query, Expr* expr, int subQueryIndex, int* subQueryCount);

static List* GetSubExprListTargetList(Query* query, List* exprList, int subQueryIndex, int* subQueryCount)
{
    // traverse all arguments, for each encountered sub-link increment the global sub-query count
    // if encountered the sub-link at the searched index, then return its target list
    List* targetList = nullptr;
    ListCell* lc = nullptr;
    foreach (lc, exprList) {
        Expr* expr = (Expr*)lfirst(lc);
        if (expr->type == T_SubLink) {
            if (*subQueryCount == subQueryIndex) {
                SubLink* subLink = (SubLink*)expr;
                Query* subQuery = (Query*)subLink->subselect;
                targetList = subQuery->targetList;
                break;
            } else {
                ++(*subQueryCount);
            }
        } else {
            // go deeper recursively
            targetList = GetSubExprTargetList(query, expr, subQueryIndex, subQueryCount);
            if (targetList != nullptr) {  // we are done
                break;
            }
            // continue searching
        }
    }
    return targetList;
}

static inline List* GetSubOpExprTargetList(Query* query, OpExpr* op_expr, int subQueryIndex, int* subQueryCount)
{
    // check the operator argument list
    List* targetList = GetSubExprListTargetList(query, op_expr->args, subQueryIndex, subQueryCount);
    return targetList;
}

static inline List* GetSubBoolExprTargetList(Query* query, BoolExpr* bool_expr, int subQueryIndex, int* subQueryCount)
{
    // check the operator argument list
    List* targetList = GetSubExprListTargetList(query, bool_expr->args, subQueryIndex, subQueryCount);
    return targetList;
}

static inline List* GetSubExprTargetList(Query* query, Expr* expr, int subQueryIndex, int* subQueryCount)
{
    // we expect either an operator or boolean expression
    List* targetList = nullptr;
    if (expr->type == T_OpExpr) {
        targetList = GetSubOpExprTargetList(query, (OpExpr*)expr, subQueryIndex, subQueryCount);
    } else if (expr->type == T_BoolExpr) {
        targetList = GetSubBoolExprTargetList(query, (BoolExpr*)expr, subQueryIndex, subQueryCount);
    }
    return targetList;
}

static inline List* GetSubQueryTargetList(Query* query, int subQueryIndex, int* subQueryCount)
{
    // search in the qualifier list for the sub-link expression at the specified index
    Node* quals = query->jointree->quals;
    Expr* expr = (Expr*)&quals[0];
    List* targetList = GetSubExprTargetList(query, expr, subQueryIndex, subQueryCount);
    return targetList;
}

static List* GetSubQueryTargetList(const char* queryString, int subQueryIndex)
{
    // search in all saved plans of current session for the plan matching the given query text
    List* targetList = nullptr;
    CachedPlanSource* psrc = u_sess->pcache_cxt.first_saved_plan;
    int subQueryCount = 0;
    while (psrc != nullptr) {
        if (strcmp(psrc->query_string, queryString) == 0) {  // query plan source found
            List* stmtList = psrc->query_list;
            Query* query = (Query*)linitial(stmtList);
            // get the target list from the sub-query in the specified index
            targetList = GetSubQueryTargetList(query, subQueryIndex, &subQueryCount);
            break;  // whether found or not, we are done
        }
        psrc = psrc->next_saved;
    }
    return targetList;
}

static bool RefetchJitFunctionContext(JitFunctionContext* functionContext)
{
    MOT_LOG_TRACE("Re-fetching tables and indices for function context %p with query: %s",
        functionContext,
        functionContext->m_queryString);
    for (uint32_t i = 0; i < functionContext->m_SPSubQueryCount; ++i) {
        JitCallSite* callSite = &functionContext->m_SPSubQueryList[i];
        if (callSite->m_queryContext != nullptr) {
            MOT_LOG_TRACE("Re-fetching tables and indices for sub-query %u in function context %p with query: %s",
                i,
                functionContext,
                callSite->m_queryContext->m_queryString);
            if (!RefetchTablesAndIndices(callSite->m_queryContext)) {
                return false;
            }
        }
    }
    return true;
}

extern bool RefetchTablesAndIndices(MotJitContext* jitContext)
{
    MOT_LOG_TRACE(
        "Re-fetching tables and indices for context %p with query: %s", jitContext, jitContext->m_queryString);
    if (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) {
        // handle function context
        return RefetchJitFunctionContext((JitFunctionContext*)jitContext);
    }

    // handle invoke context
    JitQueryContext* queryContext = (JitQueryContext*)jitContext;
    if (jitContext->m_commandType == JIT_COMMAND_INVOKE) {
        MOT_LOG_TRACE("Re-fetching tables and indices for INVOKE context %p with query: %s",
            jitContext,
            jitContext->m_queryString);
        if (queryContext->m_invokeContext != nullptr) {
            return RefetchTablesAndIndices(queryContext->m_invokeContext);
        }
        return true;
    }

    // re-fetch main table
    MOT::TxnManager* currTxn = GetSafeTxn(__FUNCTION__);
    MOT_ASSERT(currTxn != nullptr);
    if (queryContext->m_table == nullptr) {
        MOT_LOG_TRACE(
            "Re-fetching main table in query context %p with query: %s", jitContext, jitContext->m_queryString);
        queryContext->m_table = currTxn->GetTableByExternalId(queryContext->m_tableId);
        if (queryContext->m_table == nullptr) {
            MOT_LOG_TRACE("Failed to fetch table by extern id %" PRIu64 " for query: %s",
                queryContext->m_tableId,
                queryContext->m_queryString);
            return false;
        }
        MOT_LOG_TRACE("Fetched table %s (%p) by extern id %" PRIu64 " for query: %s",
            queryContext->m_table->GetTableName().c_str(),
            queryContext->m_table,
            queryContext->m_tableId,
            queryContext->m_queryString);
    }

    // re-fetch main index
    if (queryContext->m_index == nullptr) {
        MOT_LOG_TRACE("Re-fetching indices for main table in query context %p with query: %s",
            jitContext,
            jitContext->m_queryString);
        queryContext->m_index = queryContext->m_table->GetIndexByExtId(queryContext->m_indexId);
        if (queryContext->m_index == nullptr) {
            MOT_LOG_TRACE("Failed to fetch index by extern id %" PRIu64 " for query: %s",
                queryContext->m_indexId,
                queryContext->m_queryString);
            return false;
        }
        MOT_LOG_TRACE("Fetched index %s (%p) by extern id %" PRIu64 " for query: %s",
            queryContext->m_index->GetName().c_str(),
            queryContext->m_index,
            queryContext->m_indexId,
            queryContext->m_queryString);
    }

    // re-fetch inner table and index (JOIN commands only)
    if (IsJoinCommand(queryContext->m_commandType)) {
        if (queryContext->m_innerTable == nullptr) {
            MOT_LOG_TRACE("Re-fetching inner table in JOIN query context %p with query: %s",
                jitContext,
                jitContext->m_queryString);
            queryContext->m_innerTable = currTxn->GetTableByExternalId(queryContext->m_innerTableId);
            if (queryContext->m_innerTable == nullptr) {
                MOT_LOG_TRACE("Failed to fetch inner table by extern id %" PRIu64, queryContext->m_innerTableId);
                return false;
            }
        }

        if (queryContext->m_innerIndex == nullptr) {
            MOT_LOG_TRACE("Re-fetching indices for inner table in JOIN query context %p with query: %s",
                jitContext,
                jitContext->m_queryString);
            queryContext->m_innerIndex = queryContext->m_innerTable->GetIndexByExtId(queryContext->m_innerIndexId);
            if (queryContext->m_innerIndex == nullptr) {
                MOT_LOG_TRACE("Failed to fetch inner index by extern id %" PRIu64, queryContext->m_innerIndexId);
                return false;
            }
        }
    }

    // re-fetch sub-query tables and indices (COMPOUND commands only)
    if (queryContext->m_commandType == JIT_COMMAND_COMPOUND_SELECT) {
        MOT_LOG_TRACE("Re-fetching tables and indices for compound query context %p with query: %s",
            jitContext,
            jitContext->m_queryString);
        for (uint32_t i = 0; i < queryContext->m_subQueryCount; ++i) {
            JitSubQueryContext* subQueryContext = &queryContext->m_subQueryContext[i];
            if (subQueryContext->m_table == nullptr) {
                MOT_LOG_TRACE("Re-fetching table for compound sub-query %u in context %p with query: %s",
                    i,
                    jitContext,
                    jitContext->m_queryString);
                subQueryContext->m_table = currTxn->GetTableByExternalId(subQueryContext->m_tableId);
                if (subQueryContext->m_table == nullptr) {
                    MOT_LOG_TRACE(
                        "Failed to fetch sub-query %u table by extern id %" PRIu64, i, subQueryContext->m_tableId);
                    return false;
                }
            }

            if (subQueryContext->m_index == nullptr) {
                MOT_LOG_TRACE("Re-fetching indices for compound sub-query %u in context %p with query: %s",
                    i,
                    jitContext,
                    jitContext->m_queryString);
                subQueryContext->m_index = subQueryContext->m_table->GetIndexByExtId(subQueryContext->m_indexId);
                if (subQueryContext->m_index == nullptr) {
                    MOT_LOG_TRACE(
                        "Failed to fetch sub-query %u index by extern id %" PRIu64, i, subQueryContext->m_indexId);
                    return false;
                }
            }
        }
    }

    return true;
}

extern bool PrepareJitContext(MotJitContext* jitContext)
{
    bool result = false;
    if (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
        result = PrepareJitQueryContext((JitQueryContext*)jitContext);
    } else {
        result = PrepareJitFunctionContext((JitFunctionContext*)jitContext);
    }
    // reset purged flag on success, otherwise make sure it is still raised, so that next execution round will attempt
    // again to prepare the context before execution
    if (jitContext->m_execState != nullptr) {
        if (result) {
            MOT_ATOMIC_STORE(jitContext->m_execState->m_purged, false);
        } else {
            MOT_ATOMIC_STORE(jitContext->m_execState->m_purged, true);
        }
    }
    return result;
}

static bool AllocJitQueryExecState(JitQueryContext* jitContext)
{
    size_t allocSize = sizeof(JitQueryExecState);
    JitQueryExecState* execState = (JitQueryExecState*)MOT::MemSessionAlloc(allocSize);
    if (execState == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Execute JIT Query",
            "Failed to allocate %u bytes for query execution state",
            (unsigned)allocSize);
        return false;
    }

    errno_t erc = memset_s(execState, allocSize, 0, allocSize);
    securec_check(erc, "\0", "\0");

    // allocate aggregate array
    if (jitContext->m_aggCount > 0) {
        allocSize = sizeof(JitAggExecState) * jitContext->m_aggCount;
        execState->m_aggExecState = (JitAggExecState*)MOT::MemSessionAlloc(allocSize);
        if (execState->m_aggExecState == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Execute JIT Query",
                "Failed to allocate %u bytes for query aggregate execution state",
                (unsigned)allocSize);
            MOT::MemSessionFree(execState);
            return false;
        }

        errno_t erc = memset_s(execState->m_aggExecState, allocSize, 0, allocSize);
        securec_check(erc, "\0", "\0");
    }

    // allocate sub-query data array for compound commands
    if (jitContext->m_commandType == JIT_COMMAND_COMPOUND_SELECT) {
        execState->m_subQueryCount = jitContext->m_subQueryCount;
        allocSize = sizeof(JitSubQueryExecState) * execState->m_subQueryCount;
        execState->m_subQueryExecState = (JitSubQueryExecState*)MOT::MemSessionAlloc(allocSize);
        if (execState->m_subQueryExecState == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Execute JIT Query",
                "Failed to allocate %u bytes for sub-query data in query execution state",
                (unsigned)allocSize);
            MOT::MemSessionFree(execState->m_aggExecState);
            execState->m_aggExecState = nullptr;
            MOT::MemSessionFree(execState);
            execState = nullptr;
            return false;
        }

        errno_t erc = memset_s(execState->m_subQueryExecState, allocSize, 0, allocSize);
        securec_check(erc, "\0", "\0");
    }

    if (jitContext->m_nonNativeSortParams) {
        MOT_ASSERT(jitContext->m_commandType == JIT_COMMAND_RANGE_SELECT);
        allocSize = sizeof(JitNonNativeSortExecState);
        execState->m_nonNativeSortExecState = (JitNonNativeSortExecState*)MOT::MemSessionAlloc(allocSize);

        if (execState->m_nonNativeSortExecState == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Execute JIT Query",
                "Failed to allocate %u bytes for JitNonNativeSortExecState in query execution state",
                (unsigned)allocSize);

            if (execState->m_subQueryExecState) {
                MOT::MemSessionFree(execState->m_subQueryExecState);
                execState->m_subQueryExecState = nullptr;
            }

            if (execState->m_aggExecState) {
                MOT::MemSessionFree(execState->m_aggExecState);
                execState->m_aggExecState = nullptr;
            }
            MOT::MemSessionFree(execState);
            execState = nullptr;

            return false;
        }

        execState->m_nonNativeSortExecState->m_tupleSort = nullptr;
    }

    jitContext->m_execState = (JitExecState*)execState;
    return true;
}

static bool PrepareMainSearchKey(JitQueryContext* jitContext, JitQueryExecState* execState)
{
    MOT_LOG_TRACE("Preparing search key from index %s", jitContext->m_index->GetName().c_str());
    execState->m_searchKey = PrepareJitSearchKey(jitContext, jitContext->m_index);
    if (execState->m_searchKey == nullptr) {
        MOT_LOG_TRACE("Failed to allocate reusable search key for JIT context, aborting jitted code execution");
        return false;
    }

    MOT_LOG_TRACE("Prepared search key %p (%u bytes) from index %s",
        execState->m_searchKey,
        execState->m_searchKey->GetKeyLength(),
        jitContext->m_index->GetName().c_str());
    return true;
}

static bool PrepareUpdateBitmap(JitQueryContext* jitContext, JitQueryExecState* execState)
{
    int fieldCount = (int)jitContext->m_table->GetFieldCount();
    MOT_LOG_TRACE("Initializing reusable bitmap set according to %d fields (including null-bits column 0) in table %s",
        fieldCount,
        jitContext->m_table->GetLongTableName().c_str());
    void* buf = MOT::MemSessionAlloc(sizeof(MOT::BitmapSet));
    if (buf == nullptr) {
        MOT_LOG_TRACE("Failed to allocate reusable bitmap set for JIT context, aborting jitted code execution");
        return false;  // safe cleanup during destroy
    }

    uint8_t* bitmapData = (uint8_t*)MOT::MemSessionAlloc(MOT::BitmapSet::GetLength(fieldCount));
    if (bitmapData == nullptr) {
        MOT_LOG_TRACE("Failed to allocate reusable bitmap set for JIT context, aborting jitted code execution");
        MOT::MemSessionFree(buf);
        return false;  // safe cleanup during destroy
    }
    execState->m_bitmapSet = new (buf) MOT::BitmapSet(bitmapData, fieldCount);
    return true;
}

static bool PrepareEndIteratorKey(JitQueryContext* jitContext, JitQueryExecState* execState)
{
    MOT_LOG_TRACE("Preparing end iterator key for range update/select command from index %s",
        jitContext->m_index->GetName().c_str());
    execState->m_endIteratorKey = PrepareJitSearchKey(jitContext, jitContext->m_index);
    if (execState->m_endIteratorKey == nullptr) {
        MOT_LOG_TRACE("Failed to allocate reusable end iterator key for JIT context, aborting jitted code execution");
        return false;  // safe cleanup during destroy
    }

    MOT_LOG_TRACE("Prepared end iterator key %p (%u bytes) for range update/select command from index %s",
        execState->m_endIteratorKey,
        execState->m_endIteratorKey->GetKeyLength(),
        jitContext->m_index->GetName().c_str());
    return true;
}

static bool PrepareInnerSrearchKey(JitQueryContext* jitContext, JitQueryExecState* execState)
{
    MOT_LOG_TRACE(
        "Preparing inner search key  for JOIN command from index %s", jitContext->m_innerIndex->GetName().c_str());
    execState->m_innerSearchKey = PrepareJitSearchKey(jitContext, jitContext->m_innerIndex);
    if (execState->m_innerSearchKey == nullptr) {
        MOT_LOG_TRACE("Failed to allocate reusable inner search key for JIT context, aborting jitted code execution");
        return false;  // safe cleanup during destroy
    }

    MOT_LOG_TRACE("Prepared inner search key %p (%u bytes) for JOIN command from index %s",
        execState->m_innerSearchKey,
        execState->m_innerSearchKey->GetKeyLength(),
        jitContext->m_innerIndex->GetName().c_str());
    return true;
}

static bool PrepareInnerEndIteratorKey(JitQueryContext* jitContext, JitQueryExecState* execState)
{
    MOT_LOG_TRACE(
        "Preparing inner end iterator key for JOIN command from index %s", jitContext->m_innerIndex->GetName().c_str());
    execState->m_innerEndIteratorKey = PrepareJitSearchKey(jitContext, jitContext->m_innerIndex);
    if (execState->m_innerEndIteratorKey == nullptr) {
        MOT_LOG_TRACE(
            "Failed to allocate reusable inner end iterator key for JIT context, aborting jitted code execution");
        return false;  // safe cleanup during destroy
    }

    MOT_LOG_TRACE("Prepared inner end iterator key %p (%u bytes) for JOIN command from index %s",
        execState->m_innerEndIteratorKey,
        execState->m_innerEndIteratorKey->GetKeyLength(),
        jitContext->m_innerIndex->GetName().c_str());
    return true;
}

static bool PrepareCompoundSubQuery(JitQueryContext* jitContext, JitQueryExecState* execState)
{
    // allocate sub-query search keys and generate tuple table slot array using session top memory context
    for (uint32_t i = 0; i < jitContext->m_subQueryCount; ++i) {
        JitSubQueryContext* subQueryContext = &jitContext->m_subQueryContext[i];
        JitSubQueryExecState* subQueryExecState = &execState->m_subQueryExecState[i];
        if (subQueryExecState->m_tupleDesc == nullptr) {
            MOT_LOG_TRACE("Preparing sub-query %u tuple descriptor", i);
            List* targetList = GetSubQueryTargetList(jitContext->m_queryString, i);
            if (targetList == nullptr) {
                MOT_LOG_TRACE("Failed to locate sub-query %u target list", i);
                return false;  // safe cleanup during destroy
            } else {
                subQueryExecState->m_tupleDesc = ExecCleanTypeFromTL(targetList, false);
                if (subQueryExecState->m_tupleDesc == nullptr) {
                    MOT_LOG_TRACE("Failed to create sub-query %u tuple descriptor from target list", i);
                    return false;  // safe cleanup during destroy
                }
                Assert(subQueryExecState->m_tupleDesc->tdrefcount == -1);
            }
        }
        if (subQueryExecState->m_slot == nullptr) {
            MOT_ASSERT(subQueryExecState->m_tupleDesc != nullptr);
            MOT_LOG_TRACE("Preparing sub-query %u result slot", i);
            subQueryExecState->m_slot = MakeSingleTupleTableSlot(subQueryExecState->m_tupleDesc);
            if (subQueryExecState->m_slot == nullptr) {
                MOT_LOG_TRACE("Failed to generate sub-query %u tuple table slot", i);
                return false;  // safe cleanup during destroy
            }
            Assert(subQueryExecState->m_tupleDesc->tdrefcount == -1);
        }
        if (subQueryExecState->m_searchKey == nullptr) {
            MOT_LOG_TRACE(
                "Preparing sub-query %u search key from index %s", i, subQueryContext->m_index->GetName().c_str());
            subQueryExecState->m_searchKey = PrepareJitSearchKey(jitContext, subQueryContext->m_index);
            if (subQueryExecState->m_searchKey == nullptr) {
                MOT_LOG_TRACE("Failed to generate sub-query %u search key", i);
                return false;  // safe cleanup during destroy
            }
        }
        if ((subQueryContext->m_commandType == JIT_COMMAND_AGGREGATE_RANGE_SELECT) &&
            (subQueryExecState->m_endIteratorKey == nullptr)) {
            MOT_LOG_TRACE("Preparing sub-query %u end-iterator search key from index %s",
                i,
                subQueryContext->m_index->GetName().c_str());
            subQueryExecState->m_endIteratorKey = PrepareJitSearchKey(jitContext, subQueryContext->m_index);
            if (subQueryExecState->m_endIteratorKey == nullptr) {
                MOT_LOG_TRACE("Failed to generate sub-query %u end-iterator search key", i);
                return false;  // safe cleanup during destroy
            }
        }
    }
    return true;
}

static bool PrepareInvokeContext(JitQueryContext* jitContext, JitQueryExecState* execState)
{
    const char* invokedQueryString =
        jitContext->m_invokeContext ? jitContext->m_invokeContext->m_queryString : jitContext->m_invokedQueryString;
    MOT_LOG_TRACE("Preparing INVOKE command parameter list for %u parameters into function: %s",
        jitContext->m_invokeParamCount,
        invokedQueryString);

    // it is possible after revalidation that number of parameter for INVOKE changes
    if (jitContext->m_invokeParamCount == 0) {
        if (execState->m_invokeParams != nullptr) {
            MOT::MemSessionFree(execState->m_invokeParams);
            execState->m_invokeParams = nullptr;
        }
    } else {
        if (execState->m_invokeParams &&
            (execState->m_invokeParams->numParams != (int)jitContext->m_invokeParamCount)) {
            MOT::MemSessionFree(execState->m_invokeParams);
            execState->m_invokeParams = nullptr;
        }
        if (execState->m_invokeParams == nullptr) {
            execState->m_invokeParams = CreateParamListInfo(jitContext->m_invokeParamCount, false);
            if (execState->m_invokeParams == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Execute JIT",
                    "Failed to prepare %u invoke parameter",
                    jitContext->m_invokeParamCount);
                return false;  // safe cleanup during destroy
            }
        }
    }
    MOT_LOG_TRACE("Prepared INVOKE command parameter list %p for %u parameters into function: %s",
        execState->m_invokeParams,
        jitContext->m_invokeParamCount,
        invokedQueryString);

    if (jitContext->m_invokeContext != nullptr) {
        if (!PrepareJitContext(jitContext->m_invokeContext)) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Execute JIT", "Failed to prepare invoked function context: %s", invokedQueryString);
            return false;  // safe cleanup during destroy
        }
    }
    return true;
}

static bool PrepareJitQueryContext(JitQueryContext* jitContext)
{
    MOT_LOG_TRACE("Preparing context %p for query: %s", jitContext, jitContext->m_queryString);
    // allocate execution state on-demand
    if (jitContext->m_execState == nullptr) {
        if (!AllocJitQueryExecState(jitContext)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Execute JIT Query", "Failed to allocate query execution state");
            return false;
        }
    }
    JitQueryExecState* execState = (JitQueryExecState*)jitContext->m_execState;
    JitStatisticsProvider::GetInstance().AddSessionBytes((int64_t)sizeof(JitQueryExecState));

    // re-fetch all table and index objects in case they were removed after TRUNCATE TABLE
    if (!RefetchTablesAndIndices(jitContext)) {
        MOT_LOG_TRACE("Failed to re-fetch table and index objects");
        return false;  // safe cleanup during destroy
    }

    jitContext->m_rc = MOT::RC_OK;
    // allocate search key (except when executing INSERT or FULL-SCAN SELECT command)
    bool allocSearchKey = ((execState->m_searchKey == nullptr) && IsCommandUsingIndex(jitContext->m_commandType) &&
                           (jitContext->m_commandType != JIT_COMMAND_FULL_SELECT));
    if (allocSearchKey) {
        if (!PrepareMainSearchKey(jitContext, execState)) {
            return false;  // safe cleanup during destroy
        }
    }

    // allocate bitmap-set object for incremental-redo when executing UPDATE command
    bool allocBitmapSet = ((execState->m_bitmapSet == nullptr) &&
                           ((jitContext->m_commandType == JIT_COMMAND_UPDATE) ||
                            (jitContext->m_commandType == JIT_COMMAND_RANGE_UPDATE)));
    if (allocBitmapSet) {
        if (!PrepareUpdateBitmap(jitContext, execState)) {
            return false;  // safe cleanup during destroy
        }
    }

    // allocate end-iterator key object when executing range UPDATE command or special SELECT commands
    bool allocEndItrKey = ((execState->m_endIteratorKey == nullptr) && IsRangeCommand(jitContext->m_commandType));
    if (allocEndItrKey) {
        if (!PrepareEndIteratorKey(jitContext, execState)) {
            return false;  // safe cleanup during destroy
        }
    }

    // allocate inner loop search key for JOIN commands
    bool allocInnerSearchKey = ((execState->m_innerSearchKey == nullptr) && IsJoinCommand(jitContext->m_commandType));
    if (allocInnerSearchKey) {
        if (!PrepareInnerSrearchKey(jitContext, execState)) {
            return false;  // safe cleanup during destroy
        }
    }

    // allocate inner loop end-iterator search key for JOIN commands
    bool allocInnerEndItrKey =
        ((execState->m_innerEndIteratorKey == nullptr) && IsJoinCommand(jitContext->m_commandType));
    if (allocInnerEndItrKey) {
        if (!PrepareInnerEndIteratorKey(jitContext, execState)) {
            return false;  // safe cleanup during destroy
        }
    }

    // preparing outer row copy for JOIN commands
    bool allocOuterRowCopy = ((execState->m_outerRowCopy == nullptr) && IsJoinCommand(jitContext->m_commandType));
    if (allocOuterRowCopy) {
        MOT_LOG_TRACE("Preparing outer row copy for JOIN command");
        execState->m_outerRowCopy = jitContext->m_table->CreateNewRow();
        if (execState->m_outerRowCopy == nullptr) {
            MOT_LOG_TRACE("Failed to allocate reusable outer row copy for JIT context, aborting jitted code execution");
            return false;  // safe cleanup during destroy
        }
    }

    // prepare sub-query data for COMPOUND commands
    if (jitContext->m_commandType == JIT_COMMAND_COMPOUND_SELECT) {
        MemoryContext oldCtx = CurrentMemoryContext;
        CurrentMemoryContext = SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR);
        if (!PrepareCompoundSubQuery(jitContext, execState)) {
            CurrentMemoryContext = oldCtx;
            return false;  // safe cleanup during destroy
        }
        CurrentMemoryContext = oldCtx;
    }

    if (jitContext->m_commandType == JIT_COMMAND_INVOKE) {
        if (!PrepareInvokeContext(jitContext, execState)) {
            return false;  // safe cleanup during destroy
        }
    }

    return true;
}

static bool AllocJitFunctionExecState(JitFunctionContext* jitContext)
{
    size_t allocSize = sizeof(JitFunctionExecState);
    JitFunctionExecState* execState = (JitFunctionExecState*)MOT::MemSessionAlloc(allocSize);
    if (execState == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Execute JIT Query",
            "Failed to allocate %u bytes for function execution state",
            (unsigned)allocSize);
        return false;
    }

    errno_t erc = memset_s(execState, allocSize, 0, allocSize);
    securec_check(erc, "\0", "\0");

    // we might have pure SPs (i.e. no queries into MOT tables), so check for count
    if (jitContext->m_SPSubQueryCount > 0) {
        allocSize = sizeof(JitInvokedQueryExecState) * jitContext->m_SPSubQueryCount;
        execState->m_invokedQueryExecState = (JitInvokedQueryExecState*)MOT::MemSessionAlloc(allocSize);
        if (execState->m_invokedQueryExecState == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Execute JIT Query",
                "Failed to allocate %u bytes for invoked-queries in function execution state",
                (unsigned)allocSize);
            MOT::MemSessionFree(execState);
            return false;
        }

        erc = memset_s(execState->m_invokedQueryExecState, allocSize, 0, allocSize);
        securec_check(erc, "\0", "\0");
    }

    jitContext->m_execState = (JitExecState*)execState;

    return true;
}

static void GetTypeDescriptor(PLpgSQL_type* typeDesc, Form_pg_attribute attr)
{
    typeDesc->dtype = PLPGSQL_DTYPE_VAR;
    typeDesc->dno = 0;  // not used, right?
    typeDesc->ispkg = false;
    typeDesc->ttype = PLPGSQL_TTYPE_SCALAR;
    typeDesc->typoid = attr->atttypid;
    typeDesc->atttypmod = attr->atttypmod;
    typeDesc->collation = attr->attcollation;
    typeDesc->typbyval = attr->attbyval;
    typeDesc->typlen = attr->attlen;
    typeDesc->typname = attr->attname.data;
    typeDesc->typrelid = attr->attrelid;

    Oid typeInputOid = InvalidOid;
    getTypeInputInfo(attr->atttypid, &typeInputOid, &typeDesc->typioparam);
    fmgr_info(typeInputOid, &typeDesc->typinput);
}

static bool PrepareCallSite(
    JitFunctionContext* jitContext, JitInvokedQueryExecState* execState, JitCallSite* callSite, int subQueryId)
{
    // a sub-query result tuple might change, so we regenerate unconditionally work slot and result slot
    const char* queryType = callSite->m_queryContext ? "jittable" : "non-jittable";
    const char* queryString =
        callSite->m_queryContext ? callSite->m_queryContext->m_queryString : callSite->m_queryString;
    if (queryString == nullptr) {
        MOT_LOG_TRACE("Invalid call site: missing query string");
        return false;
    }
    MOT_LOG_TRACE(
        "Preparing stored-procedure %s sub-query %u invoke exec state at: %p", queryType, subQueryId, execState);
    MOT_LOG_TRACE("Preparing stored-procedure %s sub-query %u work slot: %s", queryType, subQueryId, queryString);
    MOT_ASSERT(callSite->m_tupDesc != nullptr);
    Assert(callSite->m_tupDesc->tdrefcount == -1);
    if (execState->m_workSlot != nullptr) {
        execState->m_workSlot->tts_tupleDescriptor = nullptr;
        ExecDropSingleTupleTableSlot(execState->m_workSlot);
        execState->m_workSlot = nullptr;
    }
    execState->m_workSlot = MakeSingleTupleTableSlot(callSite->m_tupDesc);
    if (execState->m_workSlot == nullptr) {
        MOT_LOG_TRACE("Failed to generate stored-procedure %s sub-query %u work tuple table slot: %s",
            queryType,
            subQueryId,
            queryString);
        return false;  // safe cleanup during destroy
    }
    Assert(callSite->m_tupDesc->tdrefcount == -1);

    MOT_LOG_TRACE("Preparing stored-procedure %s sub-query %u result slot: %s", queryType, subQueryId, queryString);
    if (execState->m_resultSlot != nullptr) {
        execState->m_resultSlot->tts_tupleDescriptor = nullptr;
        ExecDropSingleTupleTableSlot(execState->m_resultSlot);
        execState->m_resultSlot = nullptr;
    }
    execState->m_resultSlot = MakeSingleTupleTableSlot(callSite->m_tupDesc);
    if (execState->m_resultSlot == nullptr) {
        MOT_LOG_TRACE("Failed to generate stored-procedure %s sub-query %u result tuple table slot: %s",
            queryType,
            subQueryId,
            queryString);
        return false;  // safe cleanup during destroy
    }
    Assert(callSite->m_tupDesc->tdrefcount == -1);

    // a sub-query parameter list might change (in size and/or types), so we regenerate it unconditionally
    MOT_LOG_TRACE("Preparing stored-procedure %s sub-query %u parameters list of size %d: %s",
        queryType,
        subQueryId,
        jitContext->m_paramCount,
        queryString);
    if (execState->m_params != nullptr) {
        MOT::MemSessionFree(execState->m_params);
        execState->m_params = nullptr;
    }

    MOT_LOG_TRACE("Preparing stored-procedure %s sub-query %u param list: %s", queryType, subQueryId, queryString);
    execState->m_params = CreateParamListInfo(jitContext->m_paramCount, false);
    if (execState->m_params == nullptr) {
        MOT_LOG_TRACE("Failed to clone %s sub-query %u parameter list: %s", queryType, subQueryId, queryString);
        return false;  // safe cleanup during destroy
    }
    for (int i = 0; i < execState->m_params->numParams; ++i) {
        execState->m_params->params[i].isnull = true;
        execState->m_params->params[i].ptype = jitContext->m_paramTypes[i];
        execState->m_params->params[i].pflags = 0;
        MOT_LOG_DEBUG("param-type: %u", execState->m_params->params[i].ptype);
    }

    if (execState->m_resultTypes == nullptr) {
        uint32_t attrCount = (uint32_t)execState->m_resultSlot->tts_tupleDescriptor->natts;
        if (attrCount > 0) {
            uint32_t allocSize = sizeof(PLpgSQL_type) * attrCount;
            execState->m_resultTypes = (PLpgSQL_type*)MOT::MemSessionAlloc(allocSize);
            if (execState->m_resultTypes == nullptr) {
                MOT_LOG_TRACE("Failed to allocate %u bytes for %u result type descriptors", allocSize, attrCount);
                return false;
            }
            for (uint32_t i = 0; i < attrCount; ++i) {
                PLpgSQL_type* typeDesc = &execState->m_resultTypes[i];
                Form_pg_attribute attr = &execState->m_resultSlot->tts_tupleDescriptor->attrs[i];
                GetTypeDescriptor(typeDesc, attr);
            }
        }
    }

    // NOTE: JIT context of each sub-query is prepared (in PrepareCallSitePlans) only after the call site plans
    // are generated. This will ensure we have necessary locks for the sub-queries.

    return true;
}

class CallSitePlanGenerator : public JitFunctionQueryVisitor {
public:
    explicit CallSitePlanGenerator(JitFunctionContext* jitContext) : m_jitContext(jitContext)
    {
        BuildExprQueryMap();
        m_queryPlans.resize(m_jitContext->m_SPSubQueryCount, false);
    }

    ~CallSitePlanGenerator() final
    {}

    JitVisitResult OnQuery(PLpgSQL_expr* expr, PLpgSQL_row* row, int exprIndex, bool into) final
    {
        // find call site by expression index
        int queryIndex = GetQueryIndex(exprIndex);
        if (queryIndex == -1) {
            MOT_LOG_DEBUG("Cannot find call site for query with expression index %d: %s", exprIndex, expr->query);
            return JitVisitResult::JIT_VISIT_CONTINUE;
        }
        JitCallSite* callSite = &m_jitContext->m_SPSubQueryList[queryIndex];
        JitFunctionExecState* functionExecState = (JitFunctionExecState*)m_jitContext->m_execState;
        JitInvokedQueryExecState* execState = &functionExecState->m_invokedQueryExecState[queryIndex];
        const char* queryType = callSite->m_queryContext ? "jittable" : "non-jittable";
        const char* queryString = expr->query;

        if (execState->m_plan != nullptr) {
            return JitVisitResult::JIT_VISIT_CONTINUE;
        }

        // invoke call site does not need SPI plan
        if ((callSite->m_queryContext != nullptr) && (callSite->m_queryContext->m_commandType == JIT_COMMAND_INVOKE)) {
            return JitVisitResult::JIT_VISIT_CONTINUE;
        }

        MOT_LOG_TRACE("Preparing stored-procedure %s sub-query %u plan: %s", queryType, queryIndex, queryString);
        execState->m_plan = GetSpiPlan(functionExecState->m_function, expr);
        if (execState->m_plan == nullptr) {
            MOT_LOG_TRACE("Failed to prepare SPI plan for %s sub-query %u: %s", queryType, queryIndex, queryString);
            return JitVisitResult::JIT_VISIT_ERROR;  // safe cleanup during destroy
        }

        // setup parameter list
        execState->m_expr = expr;
        execState->m_params->paramFetch = plpgsql_param_fetch;
        execState->m_params->paramFetchArg = &functionExecState->m_estate;
        execState->m_params->parserSetup = (ParserSetupHook)plpgsql_parser_setup;
        execState->m_params->parserSetupArg = (void*)execState->m_expr;
        execState->m_params->params_need_process = false;

        return JitVisitResult::JIT_VISIT_CONTINUE;
    }

    void OnError(const char* stmtType, int lineNo) final
    {
        MOT_LOG_TRACE("Failed to generate call site plan while processing statement at line %d: %s", lineNo, stmtType);
    }

    bool AllQueryPlansGenerated()
    {
        for (uint32_t i = 0; i < m_jitContext->m_SPSubQueryCount; ++i) {
            if (!m_queryPlans[i]) {
                MOT_LOG_TRACE("Query plan for call site %u not generated", i);
                return false;
            }
        }
        return true;
    }

private:
    JitFunctionContext* m_jitContext;
    using JitExprQueryMap = std::map<int, int>;
    JitExprQueryMap m_exprQueryMap;
    std::vector<bool> m_queryPlans;

    void BuildExprQueryMap()
    {
        for (uint32_t i = 0; i < m_jitContext->m_SPSubQueryCount; ++i) {
            JitCallSite* callSite = &m_jitContext->m_SPSubQueryList[i];
            m_exprQueryMap.insert(JitExprQueryMap::value_type(callSite->m_exprIndex, (int)i));
        }
    }

    int GetQueryIndex(int exprIndex)
    {
        int queryIndex = -1;
        JitExprQueryMap::iterator itr = m_exprQueryMap.find(exprIndex);
        if (itr != m_exprQueryMap.end()) {
            queryIndex = itr->second;
            if (m_queryPlans[queryIndex] == true) {  // query hit twice
                MOT_LOG_TRACE("Query %d hit twice while generating call site plans", queryIndex);
                queryIndex = -1;
            } else {
                m_queryPlans[queryIndex] = true;
            }
        }
        return queryIndex;
    }
};

static bool PrepareCallSitePlans(JitFunctionContext* jitContext)
{
    CallSitePlanGenerator planGenerator(jitContext);
    JitFunctionExecState* functionExecState = (JitFunctionExecState*)jitContext->m_execState;

    SPIAutoConnect spiAutoConnect;
    if (!spiAutoConnect.IsConnected()) {
        int rc = spiAutoConnect.GetErrorCode();
        MOT_LOG_TRACE("Failed to connect to SPI while generating SPI plans for jitted function: %s",
            jitContext->m_queryString,
            SPI_result_code_string(rc),
            rc);
        return false;
    }

    if (!VisitFunctionQueries(functionExecState->m_function, &planGenerator)) {
        MOT_LOG_TRACE("Failed to generate call site plans: error encountered");
        return false;
    }

    if (!planGenerator.AllQueryPlansGenerated()) {
        MOT_LOG_TRACE("Failed to generate call site plans: not all plans generated");
        return false;
    }

    volatile bool result = true;
    volatile CachedPlan* cplan = nullptr;
    volatile JitInvokedQueryExecState* invokeExecState = nullptr;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    PG_TRY();
    {
        // Prepare JIT context of each sub-query only after the call site plans are generated. This will ensure we have
        // necessary locks for the sub-queries.
        for (uint32_t i = 0; i < jitContext->m_SPSubQueryCount; ++i) {
            JitCallSite* callSite = &jitContext->m_SPSubQueryList[i];
            // prepare JIT context state of each invoked query
            if (callSite->m_queryContext != nullptr) {
                MOT_LOG_TRACE("Preparing stored-procedure jittable sub-query %u context: %s",
                    i,
                    callSite->m_queryContext->m_queryString);

                // in case we hit parsing during revalidate, we must make sure we have up-to-date parameters
                invokeExecState = &functionExecState->m_invokedQueryExecState[i];
                functionExecState->m_estate.cur_expr = invokeExecState->m_expr;  // required by plpgsql_param_fetch
                if (invokeExecState->m_expr != nullptr) {
                    invokeExecState->m_expr->func = functionExecState->m_function;
                    invokeExecState->m_expr->func->cur_estate = (PLpgSQL_execstate*)&functionExecState->m_estate;
                }

                if (callSite->m_queryContext->m_commandType != JitExec::JIT_COMMAND_INVOKE) {
                    cplan = SPI_plan_get_cached_plan(invokeExecState->m_plan);
                    if (cplan == nullptr) {
                        MOT_LOG_ERROR("Failed to get cached plan");
                        result = false;
                        break;
                    }
                }

                result = PrepareJitContext(callSite->m_queryContext);

                if (cplan != nullptr) {
                    ReleaseCachedPlan((CachedPlan*)cplan, invokeExecState->m_plan->saved);
                    cplan = nullptr;
                }

                if (!result) {
                    MOT_LOG_TRACE("Failed to prepare sub-query %u context", i);
                    break;  // safe cleanup during destroy
                }
            }
        }

        MOT_ASSERT(cplan == nullptr);
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(origCxt);

        if (cplan != nullptr) {
            ReleaseCachedPlan((CachedPlan*)cplan, invokeExecState->m_plan->saved);
            cplan = nullptr;
        }

        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while preparing sub-query contexts for function %s: %s",
            jitContext->m_queryString,
            edata->message);
        ereport(WARNING,
            (errmodule(MOD_MOT),
                errmsg("Caught exception while preparing sub-query contexts for function %s: %s",
                    jitContext->m_queryString,
                    edata->message),
                errdetail("%s", edata->detail)));
        FlushErrorState();
        FreeErrorData(edata);
        result = false;
    }
    PG_END_TRY();

    return result;
}

static bool PrepareJitFunctionContext(JitFunctionContext* jitContext)
{
    MOT_LOG_TRACE("Preparing context %p for function: %s", jitContext, jitContext->m_queryString);
    // allocate execution state on-demand
    if (jitContext->m_execState == nullptr) {
        if (!AllocJitFunctionExecState(jitContext)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Execute JIT Query", "Failed to allocate function execution state");
            return false;
        }
    }
    JitStatisticsProvider::GetInstance().AddSessionBytes((int64_t)sizeof(JitFunctionExecState));

    // prepare compiled function
    JitFunctionExecState* execState = (JitFunctionExecState*)jitContext->m_execState;
    if (execState->m_function == nullptr) {
        execState->m_function = GetPGCompiledFunction(jitContext->m_functionOid);
        if (execState->m_function == nullptr || execState->m_function->fn_xmin != jitContext->m_functionTxnId) {
            bool functionReplaced = false;
            if (execState->m_function == nullptr) {
                MOT_REPORT_ERROR(
                    MOT_ERROR_CONCURRENT_MODIFICATION, "Execute JIT Query", "Failed to retrieve PG function");
            } else {
                functionReplaced = true;
                if (execState->m_function->fn_xmin > jitContext->m_functionTxnId) {
                    MOT_REPORT_ERROR(MOT_ERROR_CONCURRENT_MODIFICATION,
                        "Execute JIT Query",
                        "PG function definition changed from %" PRIu64 " to %" PRIu64 ", JIT context has older version",
                        jitContext->m_functionTxnId,
                        execState->m_function->fn_xmin);
                } else {
                    MOT_REPORT_ERROR(MOT_ERROR_CONCURRENT_MODIFICATION,
                        "Execute JIT Query",
                        "PG function definition changed from %" PRIu64 " to %" PRIu64 ", JIT context has newer version",
                        execState->m_function->fn_xmin,
                        jitContext->m_functionTxnId);
                }
            }

            // Expire the JIT source only if the function was dropped or the JIT context/source is based on older
            // definition of the function.
            // It is possible that JIT context/source is already based on the newer definition of the function, but
            // this session has not yet seen the newer definition. In this case, GetPGCompiledFunction will retrieve
            // the older function, so we should not expire the JIT source.
            if (execState->m_function == nullptr || execState->m_function->fn_xmin > jitContext->m_functionTxnId) {
                bool expireInvokeQuery = false;
                LockJitSource(jitContext->m_jitSource);
                if (jitContext->m_jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_READY) {
                    expireInvokeQuery = true;
                    // We are not expiring the JIT source in a transactional manner, so we cannot set the correct
                    // m_expireTxnId here. But this is fine, we can still avoid premature revalidation attempts using
                    // m_functionTxnId itself.
                    SetJitSourceExpired(jitContext->m_jitSource, 0, functionReplaced);
                }
                UnlockJitSource(jitContext->m_jitSource);
                if (expireInvokeQuery) {
                    MotJitContext* invokeQueryContext = jitContext->m_parentContext;
                    LockJitSource(invokeQueryContext->m_jitSource);
                    // We are not expiring the JIT source in a transactional manner, so we cannot set the correct
                    // m_expireTxnId here. But this is fine, we can still avoid premature revalidation attempts using
                    // m_functionTxnId itself.
                    SetJitSourceExpired(invokeQueryContext->m_jitSource, 0, functionReplaced);
                    UnlockJitSource(invokeQueryContext->m_jitSource);
                }
            }

            // we can mark this context as invalid outside lock-scope even if there is a race with compilation done
            // event, because both will lead to revalidation, so order of events is not important (see plancache.cpp)
            InvalidateJitContext(jitContext, 0);
            execState->m_function = nullptr;
            return false;
        }
        ++execState->m_function->use_count;
        MOT_LOG_TRACE("PrepareJitFunctionContext(): Increased use count of function %p to %lu: %s",
            execState->m_function,
            execState->m_function->use_count,
            jitContext->m_queryString);
    }

    // prepare minimal execution state if required
    if (execState->m_function != execState->m_estate.func) {
        PrepareExecState(&execState->m_estate, execState->m_function);
    }

    // prepare call sites
    MemoryContext oldCtx = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
    for (uint32_t i = 0; i < jitContext->m_SPSubQueryCount; ++i) {
        JitCallSite* callSite = &jitContext->m_SPSubQueryList[i];
        if (!PrepareCallSite(jitContext, &execState->m_invokedQueryExecState[i], callSite, i)) {
            (void)MemoryContextSwitchTo(oldCtx);
            return false;
        }
    }

    if (!PrepareCallSitePlans(jitContext)) {
        (void)MemoryContextSwitchTo(oldCtx);
        return false;
    }

    (void)MemoryContextSwitchTo(oldCtx);

    return true;
}

static void DestroyDatumArray(JitDatumArray* datumArray, JitContextUsage usage)
{
    if (datumArray->m_datumCount > 0) {
        MOT_ASSERT(datumArray->m_datums != nullptr);
        for (uint32_t i = 0; i < datumArray->m_datumCount; ++i) {
            if (!datumArray->m_datums[i].m_isNull && !IsPrimitiveType(datumArray->m_datums[i].m_type)) {
                JitMemFree(DatumGetPointer(datumArray->m_datums[i].m_datum), usage);
            }
        }
        JitMemFree(datumArray->m_datums, usage);
        datumArray->m_datums = nullptr;
        datumArray->m_datumCount = 0;
    }

    MOT_ASSERT(datumArray->m_datums == nullptr);
}

extern void DestroyJitContext(MotJitContext* jitContext, bool isDropCachedPlan /* = false */)
{
    if (jitContext == nullptr) {
        return;
    }

    if (isDropCachedPlan) {
        (void)EnsureSafeThreadAccess();
    }

#ifdef MOT_JIT_DEBUG
    MOT_LOG_TRACE("Destroying JIT context %p with %" PRIu64 " executions of query: %s",
        jitContext,
        jitContext->m_execState ? jitContext->m_execState->m_execCount : 0,
        jitContext->m_queryString);
#else
    MOT_LOG_TRACE("Destroying %s JIT %s context %p of query: %s",
        JitContextUsageToString(jitContext->m_usage),
        JitContextTypeToString(jitContext->m_contextType),
        jitContext,
        jitContext->m_queryString);
#endif

    // remove from JIT source
    if (jitContext->m_jitSource != nullptr) {
        RemoveJitSourceContext(jitContext->m_jitSource, jitContext);
        jitContext->m_jitSource = nullptr;
    }

    // cleanup constant datum array
    DestroyDatumArray(&jitContext->m_constDatums, jitContext->m_usage);

    // cleanup
    if (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
        DestroyJitQueryContext((JitQueryContext*)jitContext, isDropCachedPlan);
    } else {
        DestroyJitFunctionContext((JitFunctionContext*)jitContext, isDropCachedPlan);
    }

    // cleanup code generator (only in global-usage, not even global-secondary usage)
    if ((jitContext->m_codeGen != nullptr) && (jitContext->m_usage == JIT_CONTEXT_GLOBAL)) {
        FreeGsCodeGen(jitContext->m_codeGen);
        jitContext->m_codeGen = nullptr;
    }

    // return context to pool
    FreeJitContext(jitContext);
}

static void DestroyJitNonNativeSortExecState(
    JitNonNativeSortExecState* jitNonNativeSortExecState, JitContextUsage usage)
{
    MOT_ASSERT(jitNonNativeSortExecState);

    if (jitNonNativeSortExecState->m_tupleSort) {
        tuplesort_end((Tuplesortstate*)jitNonNativeSortExecState->m_tupleSort);
        jitNonNativeSortExecState->m_tupleSort = nullptr;
    }

    JitMemFree(jitNonNativeSortExecState, usage);
}

static void DestroyJitExecState(JitExecState* execState)
{
    if (execState != nullptr) {
        MOT::MemSessionFree(execState);
    }
}

static void CleanupExecStatePrimary(JitQueryContext* jitContext, JitQueryExecState* execState, bool isDropCachedPlan)
{
    if (execState == nullptr) {
        return;
    }

    if (jitContext->m_table == nullptr) {
        return;
    }

    MOT::Table* table = nullptr;
    if (isDropCachedPlan) {
        // If this is called from DropCachedPlan, lock the table. Because for deallocate and deallocate all, there is
        // no table locks taken in the envelope, so we must lock the MOT::Table before destroying keys and rows.
        if (u_sess->mot_cxt.txn_manager != nullptr) {
            table = u_sess->mot_cxt.txn_manager->GetTxnTable(jitContext->m_tableId);
            if (table != nullptr) {
                table->RdLock();
            }
        } else {
            table = MOT::GetTableManager()->GetTableSafeByExId(jitContext->m_tableId);
        }
        if (table == nullptr) {
            // Table is dropped concurrently, just set everything to nullptr and return.
            execState->m_searchKey = nullptr;
            execState->m_endIteratorKey = nullptr;
            execState->m_beginIterator = nullptr;
            execState->m_endIterator = nullptr;
            execState->m_outerRowCopy = nullptr;
            return;
        }
        MOT_ASSERT(table == jitContext->m_table);
    }

    if (jitContext->m_index != nullptr) {
        MOT::Index* index = jitContext->m_index;
        if (table != nullptr) {
            index = table->GetIndexByExtId(jitContext->m_indexId);
        } else {
            index = jitContext->m_table->GetIndexByExtId(jitContext->m_indexId);
        }

        if (jitContext->m_index == index) {
            // Destroy the keys and iterators only if the index pointer has not changed.
            if (execState->m_searchKey != nullptr) {
                index->DestroyKey(execState->m_searchKey);
            }

            if (execState->m_endIteratorKey != nullptr) {
                index->DestroyKey(execState->m_endIteratorKey);
            }

            if (execState->m_beginIterator != nullptr) {
                destroyIterator(execState->m_beginIterator);
            }

            if (execState->m_endIterator != nullptr) {
                destroyIterator(execState->m_endIterator);
            }
        }

        execState->m_searchKey = nullptr;
        execState->m_endIteratorKey = nullptr;
        execState->m_beginIterator = nullptr;
        execState->m_endIterator = nullptr;
    }

    // cleanup JOIN outer row copy
    if (execState->m_outerRowCopy != nullptr) {
        jitContext->m_table->DestroyRow(execState->m_outerRowCopy);
        execState->m_outerRowCopy = nullptr;
    }

    if (table != nullptr) {
        table->Unlock();
    }
}

static void CleanupExecStateInner(JitQueryContext* jitContext, JitQueryExecState* execState, bool isDropCachedPlan)
{
    if (execState == nullptr) {
        return;
    }

    if (jitContext->m_innerTable == nullptr) {
        return;
    }

    MOT::Table* table = nullptr;
    if (isDropCachedPlan) {
        // If this is called from DropCachedPlan, lock the table. Because for deallocate and deallocate all, there is
        // no table locks taken in the envelope, so we must lock the MOT::Table before destroying keys and rows.
        if (u_sess->mot_cxt.txn_manager != nullptr) {
            table = u_sess->mot_cxt.txn_manager->GetTxnTable(jitContext->m_innerTableId);
            if (table != nullptr) {
                table->RdLock();
            }
        } else {
            table = MOT::GetTableManager()->GetTableSafeByExId(jitContext->m_innerTableId);
        }
        if (table == nullptr) {
            // Table is dropped concurrently, just set everything to nullptr and return.
            execState->m_innerSearchKey = nullptr;
            execState->m_innerEndIteratorKey = nullptr;
            execState->m_innerBeginIterator = nullptr;
            execState->m_innerEndIterator = nullptr;
            execState->m_innerRow = nullptr;
            return;
        }
        MOT_ASSERT(table == jitContext->m_innerTable);
    }

    if (jitContext->m_innerIndex != nullptr) {
        MOT::Index* index = jitContext->m_innerIndex;
        if (table != nullptr) {
            index = table->GetIndexByExtId(jitContext->m_innerIndexId);
        } else {
            index = jitContext->m_innerTable->GetIndexByExtId(jitContext->m_innerIndexId);
        }

        if (jitContext->m_innerIndex == index) {
            // Destroy the keys and iterators only if the index pointer has not changed.
            if (execState->m_innerSearchKey != nullptr) {
                index->DestroyKey(execState->m_innerSearchKey);
            }

            if (execState->m_innerEndIteratorKey != nullptr) {
                index->DestroyKey(execState->m_innerEndIteratorKey);
            }

            if (execState->m_innerBeginIterator != nullptr) {
                destroyIterator(execState->m_innerBeginIterator);
            }

            if (execState->m_innerEndIterator != nullptr) {
                destroyIterator(execState->m_innerEndIterator);
            }
        }

        execState->m_innerSearchKey = nullptr;
        execState->m_innerEndIteratorKey = nullptr;
        execState->m_innerBeginIterator = nullptr;
        execState->m_innerEndIterator = nullptr;
    }

    // cleanup JOIN inner row
    if (execState->m_innerRow != nullptr) {
        jitContext->m_innerTable->DestroyRow(execState->m_innerRow);
        execState->m_innerRow = nullptr;
    }

    if (table != nullptr) {
        table->Unlock();
    }
}

static void CleanupSubQueryExecState(
    JitSubQueryContext* subQueryContext, JitSubQueryExecState* subQueryExecState, bool isDropCachedPlan)
{
    if (subQueryExecState->m_slot != nullptr) {
        ExecDropSingleTupleTableSlot(subQueryExecState->m_slot);
        subQueryExecState->m_slot = nullptr;
    }

    if (subQueryExecState->m_tupleDesc != nullptr) {
        FreeTupleDesc(subQueryExecState->m_tupleDesc);
        subQueryExecState->m_tupleDesc = nullptr;
    }

    if (subQueryContext->m_table == nullptr) {
        return;
    }

    if (subQueryContext->m_index == nullptr) {
        return;
    }

    MOT::Table* table = nullptr;
    if (isDropCachedPlan) {
        // If this is called from DropCachedPlan, lock the table. Because for deallocate and deallocate all, there is
        // no table locks taken in the envelope, so we must lock the MOT::Table before destroying keys.
        if (u_sess->mot_cxt.txn_manager != nullptr) {
            table = u_sess->mot_cxt.txn_manager->GetTxnTable(subQueryContext->m_tableId);
            if (table != nullptr) {
                table->RdLock();
            }
        } else {
            table = MOT::GetTableManager()->GetTableSafeByExId(subQueryContext->m_tableId);
        }
        if (table == nullptr) {
            // Table is dropped concurrently, just set everything to nullptr and return.
            subQueryExecState->m_searchKey = nullptr;
            subQueryExecState->m_endIteratorKey = nullptr;
            return;
        }
        MOT_ASSERT(table == subQueryContext->m_table);
    }

    MOT::Index* index = subQueryContext->m_index;
    if (table != nullptr) {
        index = table->GetIndexByExtId(subQueryContext->m_indexId);
    } else {
        index = subQueryContext->m_table->GetIndexByExtId(subQueryContext->m_indexId);
    }

    if (subQueryContext->m_index == index) {
        // Destroy the keys only if the index pointer has not changed.
        if (subQueryExecState->m_searchKey != nullptr) {
            index->DestroyKey(subQueryExecState->m_searchKey);
        }

        if (subQueryExecState->m_endIteratorKey != nullptr) {
            index->DestroyKey(subQueryExecState->m_endIteratorKey);
        }
    }

    subQueryExecState->m_searchKey = nullptr;
    subQueryExecState->m_endIteratorKey = nullptr;

    if (table != nullptr) {
        table->Unlock();
    }
}

static void DestroyQueryExecState(JitQueryContext* jitContext, bool isDropCachedPlan = false)
{
    JitQueryExecState* execState = (JitQueryExecState*)jitContext->m_execState;
    if (execState == nullptr) {
        return;
    }

    if ((jitContext->m_subQueryContext != nullptr) && (execState->m_subQueryExecState)) {
        for (uint32_t i = 0; i < jitContext->m_subQueryCount; ++i) {
            JitSubQueryContext* subQueryContext = &jitContext->m_subQueryContext[i];
            CleanupSubQueryExecState(subQueryContext, &execState->m_subQueryExecState[i], isDropCachedPlan);
        }
    }

    CleanupExecStatePrimary(jitContext, execState, isDropCachedPlan);

    CleanupExecStateInner(jitContext, execState, isDropCachedPlan);

    // cleanup bitmap set
    if (execState->m_bitmapSet != nullptr) {
        MOT::MemSessionFree(execState->m_bitmapSet->GetData());
        execState->m_bitmapSet->MOT::BitmapSet::~BitmapSet();
        MOT::MemSessionFree(execState->m_bitmapSet);
        execState->m_bitmapSet = nullptr;
    }

    // cleanup aggregate array
    if (execState->m_aggExecState != nullptr) {
        MOT::MemSessionFree(execState->m_aggExecState);
        execState->m_aggExecState = nullptr;
    }

    // cleanup sub-query array
    if (execState->m_subQueryExecState != nullptr) {
        MOT::MemSessionFree(execState->m_subQueryExecState);
        execState->m_subQueryExecState = nullptr;
    }

    // cleanup invoke parameters
    if (execState->m_invokeParams != nullptr) {
        MOT_ASSERT(!IsJitContextUsageGlobal(jitContext->m_usage));
        JitMemFree(execState->m_invokeParams, jitContext->m_usage);
        execState->m_invokeParams = nullptr;
    }

    if (execState->m_nonNativeSortExecState != nullptr) {
        DestroyJitNonNativeSortExecState(execState->m_nonNativeSortExecState, jitContext->m_usage);
        execState->m_nonNativeSortExecState = nullptr;
    }

    DestroyJitExecState(execState);
    jitContext->m_execState = nullptr;
    JitStatisticsProvider::GetInstance().AddSessionBytes((int64_t)sizeof(JitQueryExecState));
}

static void DestroyJitQueryContext(JitQueryContext* jitContext, bool isDropCachedPlan /* = false */)
{
    // cleanup execution state
    DestroyQueryExecState(jitContext, isDropCachedPlan);

    // cleanup sub-query data array
    CleanupJitSubQueryContextArray(jitContext, isDropCachedPlan);

    // cleanup keys(s)
    CleanupJitContextPrimary(jitContext, isDropCachedPlan);

    // cleanup JOIN keys(s)
    CleanupJitContextInner(jitContext, isDropCachedPlan);

    // cleanup parameter info array
    if (jitContext->m_invokeParamInfo != nullptr) {
        JitMemFree(jitContext->m_invokeParamInfo, jitContext->m_usage);
        jitContext->m_invokeParamInfo = nullptr;
    }

    // cleanup invoke context
    if (jitContext->m_invokeContext != nullptr) {
        DestroyJitContext(jitContext->m_invokeContext, isDropCachedPlan);
        jitContext->m_invokeContext = nullptr;
    }

    if (jitContext->m_nonNativeSortParams) {
        DestroyJitNonNativeSortParams(jitContext->m_nonNativeSortParams, jitContext->m_usage);
        jitContext->m_nonNativeSortParams = nullptr;
    }
}

static void DestroyCallSite(JitCallSite* callSite, JitContextUsage usage, bool isDropCachedPlan = false)
{
    if (callSite->m_queryContext != nullptr) {
        DestroyJitContext(callSite->m_queryContext, isDropCachedPlan);
        callSite->m_queryContext = nullptr;
    }

    if (callSite->m_queryString != nullptr) {
        JitMemFree(callSite->m_queryString, usage);
        callSite->m_queryString = nullptr;
    }

    if (callSite->m_callParamInfo != nullptr) {
        JitMemFree(callSite->m_callParamInfo, usage);
        callSite->m_callParamInfo = nullptr;
    }

    if (callSite->m_tupDesc != nullptr) {
        FreeTupleDesc(callSite->m_tupDesc);
        callSite->m_tupDesc = nullptr;
    }
}

static void DestroyJitInvokeExecState(JitInvokedQueryExecState* execState)
{
    if (execState->m_workSlot != nullptr) {
        execState->m_workSlot->tts_tupleDescriptor = nullptr;
        ExecDropSingleTupleTableSlot(execState->m_workSlot);
        execState->m_workSlot = nullptr;
    }
    if (execState->m_resultSlot != nullptr) {
        execState->m_resultSlot->tts_tupleDescriptor = nullptr;
        ExecDropSingleTupleTableSlot(execState->m_resultSlot);
        execState->m_resultSlot = nullptr;
    }
    if (execState->m_params != nullptr) {
        MOT::MemSessionFree(execState->m_params);
        execState->m_params = nullptr;
    }
    if (execState->m_plan != nullptr) {
        (void)SPI_freeplan(execState->m_plan);
        execState->m_plan = nullptr;
    }
    if (execState->m_resultTypes != nullptr) {
        MOT::MemSessionFree(execState->m_resultTypes);
        execState->m_resultTypes = nullptr;
    }
    if (execState->m_expr != nullptr) {
        execState->m_expr->func = nullptr;
        execState->m_expr = nullptr;
    }
}

static void DestroyFunctionExecState(JitFunctionContext* jitContext)
{
    JitFunctionExecState* execState = (JitFunctionExecState*)jitContext->m_execState;
    if (execState != nullptr) {
        // release compiled function
        if (execState->m_function != nullptr) {
            MOT_ASSERT(execState->m_function->use_count > 0);
            --execState->m_function->use_count;
            MOT_LOG_TRACE("DestroyFunctionExecState(): Decreased use count of function %p to %lu: %s",
                execState->m_function,
                execState->m_function->use_count,
                jitContext->m_queryString);
            execState->m_function = nullptr;
        }

        // cleanup sub-query invoke ExecState
        if (execState->m_invokedQueryExecState != nullptr) {
            for (uint32_t i = 0; i < jitContext->m_SPSubQueryCount; ++i) {
                DestroyJitInvokeExecState(&execState->m_invokedQueryExecState[i]);
            }
            MOT::MemSessionFree(execState->m_invokedQueryExecState);
            execState->m_invokedQueryExecState = nullptr;
        }

        // cleanup common attributes
        DestroyJitExecState(execState);
        jitContext->m_execState = nullptr;
        JitStatisticsProvider::GetInstance().AddSessionBytes((int64_t)sizeof(JitFunctionExecState));
    }
}

static void DestroyJitFunctionContext(JitFunctionContext* jitContext, bool isDropCachedPlan /* = false */)
{
    // cleanup execution state
    DestroyFunctionExecState(jitContext);

    // cleanup sub-query data and tuple descriptors
    if (jitContext->m_SPSubQueryList != nullptr) {
        for (uint32_t i = 0; i < jitContext->m_SPSubQueryCount; ++i) {
            JitCallSite* callSite = &jitContext->m_SPSubQueryList[i];
            DestroyCallSite(callSite, jitContext->m_usage, isDropCachedPlan);
        }

        JitMemFree(jitContext->m_SPSubQueryList, jitContext->m_usage);
    }

    // cleanup type list
    if (jitContext->m_paramTypes != nullptr) {
        JitMemFree(jitContext->m_paramTypes, jitContext->m_usage);
        jitContext->m_paramTypes = nullptr;
    }

    // cleanup result tuple descriptor
    if (jitContext->m_resultTupDesc != nullptr) {
        FreeTupleDesc(jitContext->m_resultTupDesc);
        jitContext->m_resultTupDesc = nullptr;
    }
    if (jitContext->m_rowTupDesc) {
        FreeTupleDesc(jitContext->m_rowTupDesc);
        jitContext->m_rowTupDesc = nullptr;
    }
}

static bool JitQueryContextRefersRelation(JitQueryContext* jitContext, uint64_t relationId)
{
    bool result = false;
    if (jitContext->m_tableId == relationId) {
        result = true;
    } else if (jitContext->m_indexId == relationId) {
        result = true;
    } else if (jitContext->m_innerTableId == relationId) {
        result = true;
    } else if (jitContext->m_innerIndexId == relationId) {
        result = true;
    } else if (jitContext->m_subQueryCount > 0) {
        for (uint32_t i = 0; i < jitContext->m_subQueryCount; ++i) {
            if (jitContext->m_subQueryContext[i].m_tableId == relationId) {
                result = true;
                break;
            }
            if (jitContext->m_subQueryContext[i].m_indexId == relationId) {
                result = true;
                break;
            }
        }
    } else if ((jitContext->m_commandType == JIT_COMMAND_INVOKE) && (jitContext->m_invokeContext != nullptr)) {
        result = JitFunctionContextRefersRelation(jitContext->m_invokeContext, relationId);
    }

    return result;
}

static bool JitFunctionContextRefersRelation(JitFunctionContext* jitContext, uint64_t relationId)
{
    bool result = false;
    for (uint32_t i = 0; i < jitContext->m_SPSubQueryCount; ++i) {
        JitCallSite* callSite = &jitContext->m_SPSubQueryList[i];
        if ((callSite->m_queryContext != nullptr) &&
            JitQueryContextRefersRelation((JitQueryContext*)callSite->m_queryContext, relationId)) {
            result = true;
            break;
        }
        // non-jittable sub-queries are irrelevant in this context
    }
    return result;
}

extern bool JitContextRefersRelation(MotJitContext* jitContext, uint64_t relationId)
{
    bool result = false;
    if (jitContext != nullptr) {
        if (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
            result = JitQueryContextRefersRelation((JitQueryContext*)jitContext, relationId);
        } else {
            result = JitFunctionContextRefersRelation((JitFunctionContext*)jitContext, relationId);
        }
    }
    return result;
}

extern void PurgeJitContext(MotJitContext* jitContext, uint64_t relationId)
{
    if (jitContext != nullptr) {
#ifdef MOT_JIT_DEBUG
        MOT_LOG_TRACE("Purging %s JIT %s context %p by external table %" PRIu64 " with %" PRIu64
                      " executions of query: %s",
            JitContextUsageToString(jitContext->m_usage),
            JitContextTypeToString(jitContext->m_contextType),
            jitContext,
            relationId,
            jitContext->m_execState ? jitContext->m_execState->m_execCount : 0,
            jitContext->m_queryString);
#else
        MOT_LOG_TRACE("Purging %s JIT %s context %p by external table %" PRIu64 " of query: %s",
            JitContextUsageToString(jitContext->m_usage),
            JitContextTypeToString(jitContext->m_contextType),
            jitContext,
            relationId,
            jitContext->m_queryString);
#endif

        if (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
            PurgeJitQueryContext((JitQueryContext*)jitContext, relationId);
        } else {
            PurgeJitFunctionContext((JitFunctionContext*)jitContext, relationId);
        }
        if (jitContext->m_execState != nullptr) {
            MOT_ATOMIC_STORE(jitContext->m_execState->m_purged, true);
        }
    }
}

extern void DeprecateJitContext(MotJitContext* jitContext)
{
    MOT_LOG_TRACE("Deprecating %s JIT context %p: %s",
        JitContextUsageToString(jitContext->m_usage),
        jitContext,
        jitContext->m_queryString);
    uint8_t validState = MOT_ATOMIC_LOAD(jitContext->m_validState);
    validState |= JIT_CONTEXT_DEPRECATE;
    MOT_ATOMIC_STORE(jitContext->m_validState, validState);

    PropagateValidState(jitContext, 0, 0);
}

extern bool IsJitContextValid(MotJitContext* jitContext)
{
    return (MOT_ATOMIC_LOAD(jitContext->m_validState) == JIT_CONTEXT_VALID);
}

extern bool IsJitSubContext(MotJitContext* jitContext)
{
    return IsJitSubContextInline(jitContext);
}

extern bool IsJitContextPendingCompile(MotJitContext* jitContext)
{
    return ((MOT_ATOMIC_LOAD(jitContext->m_validState) & JIT_CONTEXT_PENDING_COMPILE) == JIT_CONTEXT_PENDING_COMPILE);
}

extern bool IsJitContextDoneCompile(MotJitContext* jitContext)
{
    return ((MOT_ATOMIC_LOAD(jitContext->m_validState) & JIT_CONTEXT_DONE_COMPILE) == JIT_CONTEXT_DONE_COMPILE);
}

extern bool IsJitContextErrorCompile(MotJitContext* jitContext)
{
    return ((MOT_ATOMIC_LOAD(jitContext->m_validState) & JIT_CONTEXT_ERROR_COMPILE) == JIT_CONTEXT_ERROR_COMPILE);
}

extern bool GetJitContextCompileState(MotJitContext* jitContext, bool* isPending, bool* isDone, bool* isError)
{
    bool result = false;
    uint8_t validState = MOT_ATOMIC_LOAD(jitContext->m_validState);
    *isPending = ((validState & JIT_CONTEXT_PENDING_COMPILE) == JIT_CONTEXT_PENDING_COMPILE);
    *isDone = ((validState & JIT_CONTEXT_DONE_COMPILE) == JIT_CONTEXT_DONE_COMPILE);
    *isError = ((validState & JIT_CONTEXT_ERROR_COMPILE) == JIT_CONTEXT_ERROR_COMPILE);
    if (*isPending || *isDone || *isError) {
        result = true;
    }
    return result;
}

inline const char* JitContextStateToString(JitContextState state)
{
    switch (state) {
        case JIT_CONTEXT_STATE_INIT:
            return "INIT";
        case JIT_CONTEXT_STATE_READY:
            return "READY";
        case JIT_CONTEXT_STATE_PENDING:
            return "PENDING";
        case JIT_CONTEXT_STATE_DONE:
            return "DONE";
        case JIT_CONTEXT_STATE_ERROR:
            return "ERROR";
        case JIT_CONTEXT_STATE_INVALID:
            return "INVALID";
        case JIT_CONTEXT_STATE_FINAL:
            return "FINAL";
        default:
            return "N/A";
    }
}

extern JitContextState GetJitContextState(MotJitContext* jitContext)
{
    JitContextState state = JIT_CONTEXT_STATE_INIT;
    uint8_t validState = MOT_ATOMIC_LOAD(jitContext->m_validState);
    if (validState == JIT_CONTEXT_VALID) {
        state = JIT_CONTEXT_STATE_READY;
    } else if ((validState & JIT_CONTEXT_PENDING_COMPILE) == JIT_CONTEXT_PENDING_COMPILE) {
        state = JIT_CONTEXT_STATE_PENDING;
    } else if ((validState & JIT_CONTEXT_DONE_COMPILE) == JIT_CONTEXT_DONE_COMPILE) {
        state = JIT_CONTEXT_STATE_DONE;
    } else if ((validState & JIT_CONTEXT_ERROR_COMPILE) == JIT_CONTEXT_ERROR_COMPILE) {
        state = JIT_CONTEXT_STATE_ERROR;
    } else if ((validState & JIT_CONTEXT_INVALID) == JIT_CONTEXT_INVALID) {
        state = JIT_CONTEXT_STATE_INVALID;
    } else if ((validState & JIT_CONTEXT_CHILD_QUERY_INVALID) == JIT_CONTEXT_CHILD_QUERY_INVALID) {
        state = JIT_CONTEXT_STATE_INVALID;
    } else if ((validState & JIT_CONTEXT_CHILD_SP_INVALID) == JIT_CONTEXT_CHILD_SP_INVALID) {
        state = JIT_CONTEXT_STATE_INVALID;
    } else if ((validState & JIT_CONTEXT_RELATION_INVALID) == JIT_CONTEXT_RELATION_INVALID) {
        state = JIT_CONTEXT_STATE_INVALID;
    } else if ((validState & JIT_CONTEXT_DEPRECATE) == JIT_CONTEXT_DEPRECATE) {
        state = JIT_CONTEXT_STATE_INVALID;
    }
    MOT_LOG_TRACE("JIT context %p state is %s (validState %u): %s",
        jitContext,
        JitContextStateToString(state),
        validState,
        jitContext->m_queryString);
    MOT_ASSERT((state != JIT_CONTEXT_STATE_INIT) && (state != JIT_CONTEXT_STATE_FINAL));
    return state;
}

inline void MarkJitContextPendingCompile(MotJitContext* jitContext)
{
    // set pending state, be careful not to overwrite done or error state
    uint8_t currState = MOT_ATOMIC_LOAD(jitContext->m_validState);
    if (((currState & JIT_CONTEXT_DONE_COMPILE) == 0) && ((currState & JIT_CONTEXT_ERROR_COMPILE) == 0)) {
        uint8_t newState = currState;
        newState &= ~JIT_CONTEXT_DONE_COMPILE;
        newState &= ~JIT_CONTEXT_ERROR_COMPILE;
        newState |= JIT_CONTEXT_PENDING_COMPILE;
        // we don't care if we fail here, because done and error states take precedence over pending state
        MOT_ATOMIC_CAS(jitContext->m_validState, currState, newState);
    }
}

extern void MarkJitContextDoneCompile(MotJitContext* jitContext)
{
    // reset pending state, and set done state (it is ok to overwrite pending state)
    uint8_t validState = MOT_ATOMIC_LOAD(jitContext->m_validState);
    validState &= ~JIT_CONTEXT_PENDING_COMPILE;
    validState &= ~JIT_CONTEXT_ERROR_COMPILE;
    validState |= JIT_CONTEXT_DONE_COMPILE;
    MOT_ATOMIC_STORE(jitContext->m_validState, validState);
}

extern void MarkJitContextErrorCompile(MotJitContext* jitContext)
{
    // reset pending state, and set done state (it is ok to overwrite pending state)
    uint8_t validState = MOT_ATOMIC_LOAD(jitContext->m_validState);
    validState &= ~JIT_CONTEXT_PENDING_COMPILE;
    validState &= ~JIT_CONTEXT_DONE_COMPILE;
    validState |= JIT_CONTEXT_ERROR_COMPILE;
    MOT_ATOMIC_STORE(jitContext->m_validState, validState);
}

extern void ResetJitContextCompileState(MotJitContext* jitContext)
{
    uint8_t validState = MOT_ATOMIC_LOAD(jitContext->m_validState);
    validState &= ~JIT_CONTEXT_PENDING_COMPILE;
    validState &= ~JIT_CONTEXT_DONE_COMPILE;
    validState &= ~JIT_CONTEXT_ERROR_COMPILE;
    MOT_ATOMIC_STORE(jitContext->m_validState, validState);
}

extern void ResetErrorState(JitExecState* execState)
{
    execState->m_errorMessage = PointerGetDatum(nullptr);
    execState->m_errorDetail = PointerGetDatum(nullptr);
    execState->m_errorHint = PointerGetDatum(nullptr);
    execState->m_sqlStateString = PointerGetDatum(nullptr);
    execState->m_sqlState = 0;
    execState->m_nullColumnId = 0;
}

static inline void ResetJitContextTable(MotJitContext* jitContext, uint64_t relationId)
{
    if (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
        JitQueryContext* queryContext = (JitQueryContext*)jitContext;
        if ((queryContext->m_table != nullptr) && (queryContext->m_tableId == relationId)) {
            queryContext->m_table = nullptr;
        }
        if ((queryContext->m_innerTable != nullptr) && (queryContext->m_innerTableId == relationId)) {
            queryContext->m_innerTable = nullptr;
        }
    }
}

static void PropagateValidState(MotJitContext* jitContext, uint8_t invalidFlag, uint64_t relationId)
{
    MOT_LOG_TRACE("Propagating valid state of context %p: %s", jitContext, jitContext->m_queryString);
    if (invalidFlag == 0) {
        if (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
            invalidFlag = JIT_CONTEXT_CHILD_QUERY_INVALID;
        } else {
            invalidFlag = JIT_CONTEXT_CHILD_SP_INVALID;
        }
    }
    MOT_LOG_TRACE("Valid-state is: %s", JitContextValidStateToString(invalidFlag));

    uint8_t newValidState = 0;
    MotJitContext* parentContext = jitContext->m_parentContext;
    while (parentContext != nullptr) {
        MOT_LOG_TRACE("Invalidating %s parent JIT context %p by child %s: %s",
            JitContextUsageToString(jitContext->m_usage),
            parentContext,
            (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) ? "query" : "SP",
            parentContext->m_queryString);
        newValidState = MOT_ATOMIC_LOAD(parentContext->m_validState) | invalidFlag;
        MOT_ATOMIC_STORE(parentContext->m_validState, newValidState);
        if (parentContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
            if (relationId != 0) {
                ResetJitContextTable(jitContext, relationId);
            }
        }
        jitContext = parentContext;
        parentContext = jitContext->m_parentContext;
    }
}

extern void InvalidateJitContext(MotJitContext* jitContext, uint64_t relationId, uint8_t invalidFlag /* = 0 */)
{
    // set this context as invalid and all ancestors as child-invalid
    MOT_LOG_TRACE("Invalidating %s JIT context %p: %s",
        JitContextUsageToString(jitContext->m_usage),
        jitContext,
        jitContext->m_queryString);
    // ATTENTION: all compilation flags are reset here, as we want to attempt to revalidate anyway, but be careful not
    // to reset deprecate flag
    ResetJitContextCompileState(jitContext);
    uint8_t validState = MOT_ATOMIC_LOAD(jitContext->m_validState);
    validState |= JIT_CONTEXT_INVALID;
    MOT_ATOMIC_STORE(jitContext->m_validState, validState);

    if (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
        // either table or index was dropped. index was already nullified during purge. if this is table drop then we
        // should also nullify table pointer.
        if (relationId != 0) {
            ResetJitContextTable(jitContext, relationId);
        }
    }

    PropagateValidState(jitContext, invalidFlag, relationId);
}

static bool ReplenishDatumArray(JitDatumArray* source, JitDatumArray* target, JitContextUsage usage, int depth)
{
    MOT_LOG_TRACE("%d: Replenishing datum array with %u elements", depth, source->m_datumCount);

    if (source->m_datumCount == 0) {
        DestroyDatumArray(target, usage);
        return true;
    }

    if (source->m_datumCount == target->m_datumCount) {
        // destroy existing target datum elements and reuse them
        for (uint32_t i = 0; i < target->m_datumCount; ++i) {
            JitDatum* targetDatum = &target->m_datums[i];
            if (!targetDatum->m_isNull && !IsPrimitiveType(targetDatum->m_type)) {
                JitMemFree(DatumGetPointer(targetDatum->m_datum), usage);
            }
            targetDatum->m_isNull = true;
        }
    } else {
        DestroyDatumArray(target, usage);
        uint32_t allocSize = source->m_datumCount * sizeof(JitDatum);
        target->m_datums = (JitDatum*)JitMemAlloc(allocSize, usage);
        if (target->m_datums == nullptr) {
            MOT_LOG_TRACE("Failed allocate %u bytes while replenishing datum array", allocSize);
            return false;
        }

        target->m_datumCount = source->m_datumCount;
        for (uint32_t i = 0; i < target->m_datumCount; ++i) {
            JitDatum* targetDatum = &target->m_datums[i];
            targetDatum->m_isNull = true;
        }
    }

    // clone all datum elements
    for (uint32_t i = 0; i < source->m_datumCount; ++i) {
        JitDatum* sourceDatum = &source->m_datums[i];
        JitDatum* targetDatum = &target->m_datums[i];
        if (!sourceDatum->m_isNull) {
            if (IsPrimitiveType(sourceDatum->m_type)) {
                targetDatum->m_datum = sourceDatum->m_datum;
            } else {
                if (!CloneDatum(sourceDatum->m_datum, sourceDatum->m_type, &targetDatum->m_datum, usage)) {
                    MOT_REPORT_ERROR(MOT_ERROR_OOM, "JIT Compile", "Failed to replenish datum array entry");
                    DestroyDatumArray(target, usage);
                    return false;
                }
            }
        }
        targetDatum->m_isNull = sourceDatum->m_isNull;
        targetDatum->m_type = sourceDatum->m_type;
    }

    return true;
}

static bool ReplenishInvokeParams(JitQueryContext* source, JitQueryContext* target, JitContextUsage usage, int depth)
{
    MOT_LOG_TRACE("%d: Replenishing %u invoke parameters for SP: %s",
        depth,
        source->m_invokeParamCount,
        source->m_invokeContext ? source->m_invokeContext->m_queryString : source->m_invokedQueryString);
    if (source->m_invokeParamCount == 0) {
        if (target->m_invokeParamCount > 0) {
            JitMemFree(target->m_invokeParamInfo, usage);
        }
        target->m_invokeParamInfo = nullptr;
    } else {
        size_t allocSize = sizeof(JitParamInfo) * source->m_invokeParamCount;
        if (target->m_invokeParamCount < source->m_invokeParamCount) {
            void* buf = JitMemRealloc(target->m_invokeParamInfo, allocSize, MOT::MEM_REALLOC_COPY_ZERO, usage);
            if (buf == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Generate JIT Code",
                    "Failed to reallocate %u bytes for %u invoke parameters",
                    (unsigned)allocSize,
                    (unsigned)source->m_invokeParamCount);
                return false;
            }
            target->m_invokeParamInfo = (JitParamInfo*)buf;
        }

        MOT_ASSERT(target->m_invokeParamInfo != nullptr);
        errno_t erc = memcpy_s(target->m_invokeParamInfo, allocSize, source->m_invokeParamInfo, allocSize);
        securec_check(erc, "\0", "\0");
    }
    target->m_invokeParamCount = source->m_invokeParamCount;
    return true;
}

static bool ReplenishSubQueryArray(JitQueryContext* source, JitQueryContext* target, JitContextUsage usage, int depth)
{
    MOT_LOG_TRACE(
        "%d: Replenishing %u sub-query data items: %s", depth, source->m_subQueryCount, source->m_queryString);
    size_t allocSize = sizeof(JitSubQueryContext) * source->m_subQueryCount;
    JitMemFree(target->m_subQueryContext, usage);
    target->m_subQueryContext = (JitSubQueryContext*)JitMemAllocAligned(allocSize, L1_CACHE_LINE, usage);
    if (target->m_subQueryContext == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Generate JIT Code",
            "Failed to allocate %u bytes for %u sub-query data array in JIT context object",
            (unsigned)allocSize,
            (unsigned)source->m_subQueryCount);
        return false;
    }
    target->m_subQueryCount = source->m_subQueryCount;

    for (uint32_t i = 0; i < source->m_subQueryCount; ++i) {
        target->m_subQueryContext[i].m_commandType = source->m_subQueryContext[i].m_commandType;
        target->m_subQueryContext[i].m_table = source->m_subQueryContext[i].m_table;
        target->m_subQueryContext[i].m_tableId = source->m_subQueryContext[i].m_tableId;
        target->m_subQueryContext[i].m_index = source->m_subQueryContext[i].m_index;
        target->m_subQueryContext[i].m_indexId = source->m_subQueryContext[i].m_indexId;
    }
    return true;
}

static bool ReplenishAggregateArray(JitQueryContext* source, JitQueryContext* target, JitContextUsage usage, int depth)
{
    size_t allocSize = sizeof(JitInvokedQueryExecState) * source->m_aggCount;
    JitQueryExecState* queryExecState = (JitQueryExecState*)target->m_execState;
    if (queryExecState != nullptr) {
        void* buf = (JitAggExecState*)JitMemRealloc(
            queryExecState->m_aggExecState, allocSize, MOT::MEM_REALLOC_COPY_ZERO, usage);
        if (buf == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Generate JIT Code",
                "Failed to allocate %u bytes for %u aggregate execution state array in Query JIT context object",
                allocSize,
                (unsigned)source->m_aggCount);
            return false;
        }
        queryExecState->m_aggExecState = (JitAggExecState*)buf;
    }
    return true;
}

static bool ReplenishInvokeContext(JitQueryContext* source, JitQueryContext* target, int depth)
{
    JitContextUsage usage = target->m_usage;
    if (target->m_invokedQueryString != nullptr) {
        JitMemFree(target->m_invokedQueryString, usage);
        target->m_invokedQueryString = nullptr;
    }
    if (source->m_commandType == JIT_COMMAND_INVOKE) {
        if (source->m_invokeContext != nullptr) {
            if (target->m_invokeContext == nullptr) {
                target->m_invokeContext = (JitFunctionContext*)CloneJitContext(source->m_invokeContext, usage);
                if (target->m_invokeContext == nullptr) {
                    MOT_REPORT_ERROR(MOT_ERROR_OOM, "Generate JIT Code", "Failed to clone invocation context");
                    return false;
                }
            } else {
                MOT_LOG_TRACE("%d: Replenishing invoked stored procedure context: %s",
                    depth,
                    source->m_invokeContext->m_queryString);
                if (!ReplenishJitContext(source->m_invokeContext, target->m_invokeContext, depth + 1)) {
                    MOT_REPORT_ERROR(MOT_ERROR_OOM, "Generate JIT Code", "Failed to replenish invocation context");
                    return false;
                }
            }
        } else {  // unjittable invoked stored procedure
            // cleanup previous invoked context
            if (target->m_invokeContext != nullptr) {
                DestroyJitContext(target->m_invokeContext);
                target->m_invokeContext = nullptr;
            }
            MOT_ASSERT(source->m_invokedQueryString);
            target->m_invokedQueryString = DupString(source->m_invokedQueryString, usage);
            if (target->m_invokedQueryString == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Generate JIT Code",
                    "Failed to clone invoked query string: %s",
                    source->m_invokedQueryString);
                return false;
            }
            target->m_invokedFunctionOid = source->m_invokedFunctionOid;
            target->m_invokedFunctionTxnId = source->m_invokedFunctionTxnId;
        }
        // in either case replenish parameters passed to invoked stored procedure
        if (target->m_invokeContext != nullptr) {
            target->m_invokeContext->m_parentContext = target;
        }
        if (!ReplenishInvokeParams(source, target, usage, depth)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Generate JIT Code", "Failed to replenish invocation parameter array");
            return false;
        }
    } else {
        if (target->m_invokeParamCount > 0) {
            JitMemFree(target->m_invokeParamInfo, usage);
        }
        target->m_invokeParamInfo = nullptr;
        target->m_invokeParamCount = 0;
    }

    MOT_ASSERT((target->m_invokeParamCount == 0 && target->m_invokeParamInfo == nullptr) ||
               (target->m_invokeParamCount > 0 && target->m_invokeParamInfo != nullptr));
    return true;
}

static bool ReplenishJitQueryContext(JitQueryContext* source, JitQueryContext* target, int depth)
{
    MOT_LOG_TRACE("%d: Replenishing JIT query context %p into %p (table=%p): %s",
        depth,
        source,
        target,
        target->m_table,
        source->m_queryString);

    JitQueryContextSetTablesAndIndices(source, target);
    target->m_aggCount = source->m_aggCount;
    target->m_subQueryCount = source->m_subQueryCount;

    JitContextUsage usage = target->m_usage;

    if (!ReplenishInvokeContext(source, target, depth)) {
        MOT_LOG_TRACE("Failed to replenish invoke context");
        return false;
    }

    // replenish sub-query tuple descriptor array
    if (source->m_subQueryCount > 0) {
        if (!ReplenishSubQueryArray(source, target, usage, depth)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Generate JIT Code", "Failed to replenish sub-query array");
            return false;
        }
    } else {
        if (target->m_subQueryContext != nullptr) {
            JitMemFree(target->m_subQueryContext, usage);
            target->m_subQueryContext = nullptr;
        }
        target->m_subQueryCount = 0;
    }

    // replenish aggregate array
    if (source->m_aggCount > 0) {
        if (!ReplenishAggregateArray(source, target, usage, depth)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Generate JIT Code", "Failed to replenish aggregate array");
            return false;
        }
    } else {
        JitExec::JitQueryExecState* targetExecState = (JitExec::JitQueryExecState*)target->m_execState;
        if ((targetExecState != nullptr) && (targetExecState->m_aggExecState != nullptr)) {
            JitMemFree(targetExecState->m_aggExecState, usage);
            targetExecState->m_aggExecState = nullptr;
        }
    }

    // replenish non-native sort parameters
    if (target->m_nonNativeSortParams != nullptr) {
        DestroyJitNonNativeSortParams(target->m_nonNativeSortParams, target->m_usage);
        target->m_nonNativeSortParams = nullptr;
    }

    if (source->m_nonNativeSortParams) {
        target->m_nonNativeSortParams = CloneJitNonNativeSortParams(source->m_nonNativeSortParams, usage);
        if (target->m_nonNativeSortParams == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Generate JIT Code", "Failed to replenish non-native sort params");
            return false;
        }
    }

    if (target->m_execState != nullptr) {
        DestroyQueryExecState(target);
        target->m_execState = nullptr;

        if (!AllocJitQueryExecState(target)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Generate JIT Code", "Failed to allocate query execution state");
            return false;
        }
    }

    MOT_LOG_TRACE("%d: Replenished JIT query context %p into %p (table=%p, index=%p): %s",
        depth,
        source,
        target,
        target->m_table,
        target->m_index,
        source->m_queryString);
    return true;
}

static bool ReplenishParamListInfo(JitCallSite* target, JitCallSite* source, JitContextUsage usage, int depth)
{
    if (source->m_queryContext != nullptr) {
        MOT_LOG_TRACE("%d: Replenishing param list info of size %d: %s",
            depth,
            source->m_callParamCount,
            source->m_queryContext->m_queryString);
    } else {
        MOT_LOG_TRACE(
            "%d: Replenishing param list info of size %d: %s", depth, source->m_callParamCount, source->m_queryString);
    }
    if (source->m_callParamCount == 0) {
        if (target->m_callParamCount > 0) {
            JitMemFree(target->m_callParamInfo, usage);
            target->m_callParamInfo = nullptr;
        }
    } else {
        size_t allocSize = sizeof(JitCallParamInfo) * source->m_callParamCount;
        if (target->m_callParamCount < source->m_callParamCount) {
            void* buf = JitMemRealloc(target->m_callParamInfo, allocSize, MOT::MEM_REALLOC_COPY_ZERO, usage);
            if (buf == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Generate JIT Code",
                    "Failed to re-allocate %u bytes for sub-query parameter array of size %u in Function JIT context",
                    allocSize,
                    source->m_callParamCount);
                return false;
            }
            target->m_callParamInfo = (JitCallParamInfo*)buf;
        }
        errno_t erc = memcpy_s(target->m_callParamInfo, allocSize, source->m_callParamInfo, allocSize);
        securec_check(erc, "\0", "\0");
    }
    target->m_callParamCount = source->m_callParamCount;
    return true;
}

static bool ResizeSubQueryArray(JitFunctionContext* source, JitFunctionContext* target)
{
    JitContextUsage usage = target->m_usage;
    JitFunctionExecState* functionExecState = (JitFunctionExecState*)target->m_execState;
    uint32_t origSubQueryCount = target->m_SPSubQueryCount;
    uint64_t allocSize = sizeof(JitCallSite) * source->m_SPSubQueryCount;

    // resize sub-query array if needed (and the execution state array)
    if (target->m_SPSubQueryCount < source->m_SPSubQueryCount) {
        void* buf = JitMemRealloc(target->m_SPSubQueryList, allocSize, MOT::MEM_REALLOC_COPY_ZERO, usage);
        if (buf == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Generate JIT Code",
                "Failed to allocate %lu bytes for %u sub-query data array in Function JIT context object",
                allocSize,
                target->m_SPSubQueryCount);
            return false;
        }
        target->m_SPSubQueryList = (JitCallSite*)buf;
        target->m_SPSubQueryCount = source->m_SPSubQueryCount;

        if (functionExecState != nullptr) {
            // the execution state must be totally destroyed, as the sub-queries might have totally changed
            MOT_LOG_TRACE("Destroying all execution state objects (target query count is too small)");
            for (uint32_t i = 0; i < origSubQueryCount; ++i) {
                JitInvokedQueryExecState* subExecState = &functionExecState->m_invokedQueryExecState[i];
                DestroyJitInvokeExecState(subExecState);
            }
            // now reallocate the entire array and zero it, so PrepareCallSite will create all missing members
            allocSize = sizeof(JitInvokedQueryExecState) * source->m_SPSubQueryCount;
            MOT_LOG_TRACE(
                "Allocating %lu bytes for %u invoked query exec state objects", allocSize, source->m_SPSubQueryCount);
            buf = JitMemRealloc(functionExecState->m_invokedQueryExecState, allocSize, MOT::MEM_REALLOC_ZERO, usage);
            if (buf == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Generate JIT Code",
                    "Failed to allocate %lu bytes for %u sub-query exec state array in Function JIT context object",
                    allocSize,
                    target->m_SPSubQueryCount);
                return false;
            }
            functionExecState->m_invokedQueryExecState = (JitInvokedQueryExecState*)buf;
            MOT_LOG_TRACE("Invoke exec state array re-allocated at: %p (%lu bytes)", buf, allocSize);
        }
    } else {
        // make sure then entire execution state is discarded and rebuilt from scratch
        if (functionExecState != nullptr) {
            // the execution state must be totally destroyed, as the sub-queries might have totally changed
            MOT_LOG_TRACE("Destroying all execution state objects (target query count is large enough)");
            for (uint32_t i = 0; i < origSubQueryCount; ++i) {
                JitInvokedQueryExecState* subExecState = &functionExecState->m_invokedQueryExecState[i];
                MOT_LOG_TRACE("Destroying sub-query %u invoke exec state at: %p", i, subExecState);
                DestroyJitInvokeExecState(subExecState);
            }
        }
    }

    return true;
}

static bool ReplenishExistingSubQueries(
    uint32_t replenishCount, JitFunctionContext* source, JitFunctionContext* target, int depth)
{
    MOT_LOG_TRACE("Replenishing (%u) existing sub-queries for JIT function context %p", replenishCount, target);

    JitContextUsage usage = target->m_usage;
    for (uint32_t i = 0; i < replenishCount; ++i) {
        JitCallSite* sourceCallSite = &source->m_SPSubQueryList[i];
        JitCallSite* targetCallSite = &target->m_SPSubQueryList[i];
        if (sourceCallSite->m_queryContext) {
            MOT_LOG_TRACE("%d: Replenishing from jittable sub-query %u: %s",
                depth,
                i,
                sourceCallSite->m_queryContext->m_queryString);
        } else {
            MOT_LOG_TRACE(
                "%d: Replenishing from non-jittable sub-query %u: %s", depth, i, sourceCallSite->m_queryString);
        }
        if (targetCallSite->m_queryString != nullptr) {
            JitMemFree(targetCallSite->m_queryString, target->m_usage);
            targetCallSite->m_queryString = nullptr;
        }
        if (sourceCallSite->m_queryContext == nullptr) {
            if (targetCallSite->m_queryContext != nullptr) {
                DestroyJitContext(targetCallSite->m_queryContext);
                targetCallSite->m_queryContext = nullptr;
            }
            targetCallSite->m_queryString = DupString(sourceCallSite->m_queryString, target->m_usage);
            if (targetCallSite->m_queryString == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Generate JIT Code",
                    "Failed to clone call site query string: %s",
                    sourceCallSite->m_queryString);
                return false;
            }
            MOT_LOG_TRACE("ReplenishJitFunctionContext(): Cloned string %p on %s scope into call site %p: %s",
                targetCallSite->m_queryString,
                IsJitContextUsageGlobal(target->m_usage) ? "global" : "local",
                targetCallSite,
                targetCallSite->m_queryString);
        } else {
            if (targetCallSite->m_queryContext == nullptr) {
                // replenish from jittable sub-query into previously non-jittable sub-query
                targetCallSite->m_queryContext = CloneJitContext(sourceCallSite->m_queryContext, usage);
                if (targetCallSite->m_queryContext == nullptr) {
                    MOT_REPORT_ERROR(MOT_ERROR_OOM,
                        "Generate JIT Code",
                        "Failed to replenish sub-query context %d for function JIT context (clone failed)",
                        i);
                    return false;
                }
            } else if (!ReplenishJitContext(
                           sourceCallSite->m_queryContext, targetCallSite->m_queryContext, depth + 1)) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Generate JIT Code",
                    "Failed to replenish sub-query context %d for function JIT context",
                    i);
                return false;
            }
            targetCallSite->m_queryContext->m_parentContext = target;
        }
        targetCallSite->m_queryCmdType = sourceCallSite->m_queryCmdType;
        if (!ReplenishParamListInfo(targetCallSite, sourceCallSite, usage, depth)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Generate JIT Code",
                "Failed to replenish sub-query %d parameters for function JIT context",
                sourceCallSite->m_callParamCount);
            return false;
        }

        if (targetCallSite->m_tupDesc != nullptr) {
            FreeTupleDesc(targetCallSite->m_tupDesc);
            targetCallSite->m_tupDesc = nullptr;
        }
        if (sourceCallSite->m_tupDesc != nullptr) {
            if (!CloneTupleDesc(sourceCallSite->m_tupDesc, &targetCallSite->m_tupDesc, usage)) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Generate JIT Code",
                    "Failed to clone sub-query %d tuple descriptor for function JIT context",
                    i);
                return false;
            }
        }
        targetCallSite->m_exprIndex = sourceCallSite->m_exprIndex;
        targetCallSite->m_isUnjittableInvoke = sourceCallSite->m_isUnjittableInvoke;
        targetCallSite->m_isModStmt = sourceCallSite->m_isModStmt;
        targetCallSite->m_isInto = sourceCallSite->m_isInto;
    }

    return true;
}

static bool ReplenishResultDescriptor(JitFunctionContext* source, JitFunctionContext* target)
{
    if (target->m_resultTupDesc != nullptr) {
        FreeTupleDesc(target->m_resultTupDesc);
        target->m_resultTupDesc = nullptr;
    }
    if (target->m_rowTupDesc != nullptr) {
        FreeTupleDesc(target->m_rowTupDesc);
        target->m_rowTupDesc = nullptr;
    }

    if (!CloneResultDescriptors(source, target)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Generate JIT Code", "Failed to clone result descriptors for function JIT context");
        return false;
    }

    return true;
}

static bool ReplenishParamTypes(JitFunctionContext* source, JitFunctionContext* target)
{
    JitContextUsage usage = target->m_usage;
    if (target->m_paramTypes != nullptr) {
        JitMemFree(target->m_paramTypes, usage);
        target->m_paramTypes = nullptr;
    }

    target->m_paramCount = source->m_paramCount;
    if (source->m_paramCount > 0) {
        uint64_t allocSize = sizeof(Oid) * target->m_paramCount;
        target->m_paramTypes = (Oid*)JitMemAlloc(allocSize, usage);
        if (target->m_paramTypes == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "Generate JIT Code",
                "Failed to allocate %u bytes for %u sub-query parameter array in Function JIT context object",
                allocSize,
                target->m_paramCount);
            return false;
        }
        errno_t erc = memcpy_s(target->m_paramTypes, allocSize, source->m_paramTypes, allocSize);
        securec_check(erc, "\0", "\0");
    }

    return true;
}

static bool ReplenishJitFunctionContext(JitFunctionContext* source, JitFunctionContext* target, int depth)
{
    MOT_LOG_TRACE(
        "%d: Replenishing JIT function context %p using %p: %s", depth, target, source, source->m_queryString);
    JitContextUsage usage = target->m_usage;
    target->m_functionOid = source->m_functionOid;
    MOT_LOG_TRACE(
        "Replenishing function txn id from %" PRIu64 " to %" PRIu64, target->m_functionTxnId, source->m_functionTxnId);
    target->m_functionTxnId = source->m_functionTxnId;
    target->m_SPArgCount = source->m_SPArgCount;

    // NOTE: in any case of failure caller will call destroy function for safe cleanup

    JitFunctionExecState* functionExecState = (JitFunctionExecState*)target->m_execState;

    // ATTENTION: Cache the original sub query count in the local variable, because target->m_SPSubQueryCount will be
    // modified in ResizeSubQueryArray().
    uint32_t origSubQueryCount = target->m_SPSubQueryCount;

    // resize sub-query array if needed (and the execution state array)
    if (!ResizeSubQueryArray(source, target)) {
        MOT_LOG_TRACE(
            "ReplenishJitFunctionContext(): Failed to resize sub-query array for JIT function context %p", target);
        return false;
    }

    // release compiled function
    if ((functionExecState != nullptr) && (functionExecState->m_function != nullptr)) {
        MOT_ASSERT(functionExecState->m_function->use_count > 0);
        --functionExecState->m_function->use_count;
        MOT_LOG_TRACE("ReplenishJitFunctionContext(): Decreased use count of function %p to %lu: %s",
            functionExecState->m_function,
            functionExecState->m_function->use_count,
            target->m_queryString);
        functionExecState->m_function = nullptr;
    }

    // replenish existing sub-queries
    uint32_t replenishCount = std::min(origSubQueryCount, source->m_SPSubQueryCount);
    MOT_LOG_TRACE("Replenish count is: %u", replenishCount);
    if (!ReplenishExistingSubQueries(replenishCount, source, target, depth)) {
        MOT_LOG_TRACE("ReplenishJitFunctionContext(): Failed to replenish existing sub-queries (%u) for JIT function "
                      "context %p",
            replenishCount,
            target);
        return false;
    }

    // clone newly added sub-queries
    for (uint32_t i = replenishCount; i < source->m_SPSubQueryCount; ++i) {
        JitCallSite* sourceCallSite = &source->m_SPSubQueryList[i];
        JitCallSite* targetCallSite = &target->m_SPSubQueryList[i];
        if (sourceCallSite->m_queryContext) {
            MOT_LOG_TRACE("%d: Cloning new sub-query %u: %s", depth, i, sourceCallSite->m_queryContext->m_queryString);
        } else {
            MOT_LOG_TRACE("%d: Cloning new sub-query %u: %s", depth, i, sourceCallSite->m_queryString);
        }
        if (!CloneCallSite(target, i, sourceCallSite, targetCallSite, usage)) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Generate JIT Code", "Failed to clone call-site %d for function JIT context", i);
            return false;
        }
        if (targetCallSite->m_queryContext != nullptr) {
            targetCallSite->m_queryContext->m_parentContext = target;
        }
    }

    // release resource of unused sub-queries and their execution state objects
    if (origSubQueryCount > source->m_SPSubQueryCount) {
        for (uint32_t i = source->m_SPSubQueryCount; i < origSubQueryCount; ++i) {
            JitCallSite* callSite = &target->m_SPSubQueryList[i];
            if (callSite->m_queryContext) {
                MOT_LOG_TRACE(
                    "%d: Removing unused sub-query %u: %s", depth, i, callSite->m_queryContext->m_queryString);
            } else {
                MOT_LOG_TRACE("%d: Removing unused sub-query %u: %s", depth, i, callSite->m_queryString);
            }
            DestroyCallSite(callSite, target->m_usage);

            if ((functionExecState != nullptr) && (functionExecState->m_invokedQueryExecState != nullptr)) {
                DestroyJitInvokeExecState(&functionExecState->m_invokedQueryExecState[i]);
            }
        }
    }
    target->m_SPSubQueryCount = source->m_SPSubQueryCount;

    // replenish result tuple descriptor (even though highly unexpected)
    if (!ReplenishResultDescriptor(source, target)) {
        MOT_LOG_TRACE(
            "ReplenishJitFunctionContext(): Failed to clone result tuple desc for function JIT context %p", target);
        return false;
    }

    // replenish parameter types array
    if (!ReplenishParamTypes(source, target)) {
        MOT_LOG_TRACE("ReplenishJitFunctionContext(): Failed to replenish parameter types array for function JIT "
                      "context %p",
            target);
        return false;
    }

    if (target->m_execState != nullptr) {
        DestroyFunctionExecState(target);
        target->m_execState = nullptr;

        if (!AllocJitFunctionExecState(target)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Generate JIT Code", "Failed to allocate function execution state");
            return false;
        }
    }

    MOT_LOG_TRACE("%d: Replenished JIT function context %p into %p: %s", depth, source, target, source->m_queryString);
    return true;
}

static bool ReplenishJitContext(MotJitContext* source, MotJitContext* target, int depth)
{
    if (source->m_contextType != target->m_contextType) {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
            "JIT Compile",
            "Cannot replenish %s JIT context %p from %s JIT context %p: mismatching context type",
            JitContextTypeToString(target->m_contextType),
            target,
            JitContextTypeToString(source->m_contextType),
            source);
        return false;
    }
    const char* itemName = (source->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) ? "query" : "function";
    MOT_LOG_TRACE(
        "%d: Replenishing JIT context %p from %p of %s: %s", depth, target, source, itemName, source->m_queryString);

    // no need to clone module/codegen object (they are safe in the source context)
    JitContextUsage usage = target->m_usage;
    target->m_llvmFunction = source->m_llvmFunction;
    target->m_llvmSPFunction = source->m_llvmSPFunction;
    target->m_commandType = source->m_commandType;
    target->m_validState = source->m_validState;
    target->m_queryString = source->m_queryString;
    if (!ReplenishDatumArray(&source->m_constDatums, &target->m_constDatums, usage, depth)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "JIT Compile", "Failed to clone constant datum array");
        return false;
    }

    // pay attention: the JIT source/context might change!
    if ((target->m_jitSource != source->m_jitSource) || (target->m_sourceJitContext != source)) {
        MOT_LOG_TRACE("%d: Replenish target %p/%p from source %p/%p",
            depth,
            target->m_jitSource,
            target->m_sourceJitContext,
            source->m_jitSource,
            source);

        // attention: even if source did not change we remove and add it again so that removal from deprecate source
        // context takes place properly
        PurgeJitContext(target, 0);  // full cleanup before detaching from source
        RemoveJitSourceContext(target->m_jitSource, target);
        AddJitSourceContext(source->m_jitSource, target);
    }

    bool result = true;
    if (source->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
        result = ReplenishJitQueryContext((JitQueryContext*)source, (JitQueryContext*)target, depth);
    } else if (source->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) {
        result = ReplenishJitFunctionContext((JitFunctionContext*)source, (JitFunctionContext*)target, depth);
    }

    if (result) {
        MOT_LOG_TRACE("%d: Replenished successfully JIT context %p from %p of %s: %s",
            depth,
            target,
            source,
            itemName,
            source->m_queryString);
        MOT_ATOMIC_STORE(target->m_validState, JIT_CONTEXT_VALID);
    } else {
        MOT_LOG_TRACE(
            "Failed to replenish JIT context %p from %p of %s: %s", target, source, itemName, source->m_queryString);
    }

    return result;
}

static bool RevalidateJitQueryContext(MotJitContext* jitContext)
{
    MOT_LOG_TRACE("Re-validating JIT query context %p of query: %s", jitContext, jitContext->m_queryString);
    if (jitContext->m_commandType != JIT_COMMAND_INVOKE) {
        // this is a simple query, we declare revalidation fails to force destruction and re-creation
        MOT_LOG_TRACE("Skipping revalidation of simple query to force code regeneration");
        return false;
    }

    // we first make sure that the invoked function is valid (this is a secondary global context)
    JitQueryContext* queryContext = (JitQueryContext*)jitContext;
    MotJitContext* invokedContext = queryContext->m_invokeContext;
    if (invokedContext == nullptr) {
        MOT_LOG_TRACE("Skipping revalidation of invoke query into unjittable SP: %s", jitContext->m_queryString);
        return true;  // revalidation is OK
    }
    uint8_t validState = MOT_ATOMIC_LOAD(invokedContext->m_validState);
    MOT_ASSERT(invokedContext->m_usage == JIT_CONTEXT_GLOBAL_SECONDARY);
    MOT_ASSERT(invokedContext->m_isSourceContext == 0);
    MOT_LOG_TRACE("Triggering revalidation of invoked context %p", invokedContext);
    if (!RevalidateJitContext(invokedContext)) {
        MOT_LOG_TRACE("Invoked context %p revalidation failed", invokedContext);
        return false;
    }

    // now if the function signature changed (e.g. default values changed), we need to re-generate code for the invoked
    // query, we do so by dropping the query altogether and forcing code generation from scratch
    if (validState & JIT_CONTEXT_INVALID) {  // SP itself was replaced or dropped and recreated
        MOT_LOG_TRACE(
            "Forcing code regeneration from scratch for INVOKE of re-created SP: %s", jitContext->m_queryString);
        return false;
    }

    // otherwise the SP context was already replenished
    MOT_ATOMIC_STORE(jitContext->m_validState, JIT_CONTEXT_VALID);
    MOT_LOG_TRACE("Re-validated JIT query context %p of query: %s", jitContext, jitContext->m_queryString);
    return true;
}

static bool RevalidateJitFunctionContext(MotJitContext* jitContext)
{
    MOT_LOG_TRACE("Re-validating JIT function %p of function: %s", jitContext, jitContext->m_queryString);
    // we arrive here when a sub-query or sub-SP was invalidated
    uint8_t validState = MOT_ATOMIC_LOAD(jitContext->m_validState);
    if (validState & (JIT_CONTEXT_CHILD_QUERY_INVALID | JIT_CONTEXT_CHILD_SP_INVALID)) {
        // now regenerate sub-queries and sub-SPs (sub-queries and sub-SPs are treated alike)
        // we expect them to be cloned from the already regenerated source
        MOT_LOG_TRACE("Regenerating code for SP sub-queries");
        if (!JitReCodegenFunctionQueries(jitContext)) {
            MOT_LOG_TRACE("Failed to revalidate JIT SP sub-queries: %s", jitContext->m_queryString);
            return false;
        }
    }

    MOT_LOG_TRACE("Re-validated JIT function context %p of query: %s", jitContext, jitContext->m_queryString);
    return true;
}

static bool ReattachDeprecateContext(MotJitContext* jitContext, TransactionId functionTxnId, const char*& queryString)
{
    // first check for deprecate source
    uint8_t validState = MOT_ATOMIC_LOAD(jitContext->m_validState);
    if (validState & JIT_CONTEXT_DEPRECATE) {
        // find the latest matching source
        MOT_LOG_TRACE("Revalidating deprecate source");
        bool shouldPopNamespace = false;
        if ((jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) &&
            IsJitSubContextInline(jitContext) &&
            (jitContext->m_parentContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION)) {
            // ATTENTION: this use case is ONLY when executing a jitted SP query, during lock-acquire via
            // RevalidateCachedQuery() (see JitExecSubQuery() in jit_helpers.cpp)
            JitFunctionContext* parentContext = (JitFunctionContext*)jitContext->m_parentContext;
            // Try to get the function name to see if the procedure still exists.
            char* funcName = get_func_name(parentContext->m_functionOid);
            if (funcName == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_CONCURRENT_MODIFICATION,
                    "JIT Revalidate",
                    "Cannot attach to new source after deprecate: function %u concurrently dropped",
                    parentContext->m_functionOid);
                // ATTENTION: we remain in deprecate state - the JIT SP query will be executed as non-jittable as
                // many times as needed, until the calling SP execution finishes, and then on next round gets
                // revalidated properly with this deprecate sub-query (see JitReCodegenFunctionQueries())
                return false;
            }
            if (!PushJitSourceNamespace(parentContext->m_functionOid, parentContext->m_queryString)) {
                MOT_LOG_TRACE("JIT Revalidate: Failed to push JIT source namespace for stored procedure %s (%u)",
                    funcName,
                    parentContext->m_functionOid);
                pfree_ext(funcName);
                return false;
            }
            pfree_ext(funcName);
            shouldPopNamespace = true;
        }

        // NOTE: Once we remove the session-local JIT context from the JIT source, it is possible that the source
        // be deprecated, freed and reused for completely different query. So removing the context from old source
        // and adding it to the new source should be done atomically within the source map lock. Otherwise, it will
        // result in a complete mess (JIT context getting attached to a completely wrong source).
        LockJitSourceMap();
        JitSource* newSource = GetCachedJitSource(queryString);
        if (shouldPopNamespace) {
            PopJitSourceNamespace();
        }
        if (newSource == nullptr) {
            MOT_LOG_TRACE("Failed to find valid source");
            UnlockJitSourceMap();
            MarkJitContextErrorCompile(jitContext);  // make sure we are in error state
            return false;
        }

        if (!IsSimpleQueryContext(jitContext) && IsPrematureRevalidation(newSource, functionTxnId)) {
            MOT_LOG_TRACE("Skipping premature re-validation of JIT context %p with source %p, new source %p: %s",
                jitContext,
                jitContext->m_jitSource,
                newSource,
                queryString);
            UnlockJitSourceMap();
            MarkJitContextErrorCompile(jitContext);  // make sure we are in error state
            return false;
        }

        PurgeJitContext(jitContext, 0);  // full cleanup before detaching from source
        RemoveJitSourceContext(jitContext->m_jitSource, jitContext);
        AddJitSourceContext(newSource, jitContext);
        queryString = jitContext->m_jitSource->m_queryString;  // get the query string again from new source
        MOT_ATOMIC_STORE(jitContext->m_validState, GetJitSourceValidState(newSource));
        UnlockJitSourceMap();
        MOT_LOG_TRACE("Valid state updated to: %x", jitContext->m_validState);
    }

    return true;
}

static bool CleanupJitContextExecState(MotJitContext* jitContext)
{
    if (jitContext->m_execState != nullptr) {
        if (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
            DestroyQueryExecState((JitQueryContext*)jitContext);
            jitContext->m_execState = nullptr;

            if (!AllocJitQueryExecState((JitQueryContext*)jitContext)) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Jit Revalidate", "Failed to allocate query execution state");
                return false;
            }
        } else {
            DestroyFunctionExecState((JitFunctionContext*)jitContext);
            jitContext->m_execState = nullptr;

            if (!AllocJitFunctionExecState((JitFunctionContext*)jitContext)) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM, "Jit Revalidate", "Failed to allocate function execution state");
                return false;
            }
        }
    }

    return true;
}

extern bool RevalidateJitContext(MotJitContext* jitContext, TransactionId functionTxnId /* = InvalidTransactionId */)
{
    const char* queryString = jitContext->m_jitSource->m_queryString;
    MOT_LOG_TRACE("Re-validating JIT context %p of query: %s", jitContext, queryString);
    if (!jitContext->m_isSourceContext) {
        // first check for deprecate source
        // NOTE: queryString is passed by reference. If the deprecated context is getting attached to a new source,
        // it will be updated accordingly.
        if (!ReattachDeprecateContext(jitContext, functionTxnId, queryString)) {
            MOT_LOG_TRACE("Failed to revalidate JIT context %p, couldn't attach to a new JIT source", jitContext);
            return false;
        }

        // Cleanup execution state before revalidation. It is possible that JIT context is not purged during create
        // index as other DML operations can be executed concurrently during create index. So we must cleanup the old
        // execution state before revalidation. Otherwise, we might end up with old keys/iterators in execution state,
        // but new index pointers in JIT context.
        if (!CleanupJitContextExecState(jitContext)) {
            MOT_LOG_TRACE("Failed to revalidate JIT context %p, couldn't cleanup execution state", jitContext);
            return false;
        }

        // ATTENTION: At this point it is possible that some other session is concurrently compiling the same query. In
        // this case, we don't really care, we just try to revalidate, but we only make sure not to overwrite "done" or
        // "error" compile state with "pending" (so the race is resolved by flag precedence).
        JitCodegenState codegenState = RevalidateJitSourceTxn(jitContext->m_jitSource, functionTxnId);
        if (codegenState != JitCodegenState::JIT_CODEGEN_READY) {
            bool pendingCompile = ((codegenState == JitCodegenState::JIT_CODEGEN_UNAVAILABLE) ||
                                   (codegenState == JitCodegenState::JIT_CODEGEN_PENDING));
            if (pendingCompile) {
                MOT_LOG_TRACE("Revalidate JIT context %p pending compilation with state %s: %s",
                    jitContext,
                    JitCodegenStateToString(codegenState),
                    queryString);
                // the following call will not set pending state in case done/error was set (if compilation finished
                // after we failed to revalidate, then we will catch that on the next invocation)
                MarkJitContextPendingCompile(jitContext);
            } else {
                MOT_LOG_TRACE("Failed to revalidate JIT context %p, source re-validation failed with state %s: %s",
                    jitContext,
                    JitCodegenStateToString(codegenState),
                    queryString);
                // we remain in code-gen error state, until some DDL comes and raises invalid flag, which will trigger
                // another attempt to revalidate. We just make sure revalidate is not re-attempted by setting the valid
                // state to compile-error (and also for making sure that OpFusion does not use JIT)
                MarkJitContextErrorCompile(jitContext);
            }
            return false;
        }

        // revalidate succeeded, so we clear all compilation flags
        // we also want to avoid another attempt to revalidate - so we just set valid state to zero
        // we hold query locks so there is no possible race with concurrent compilation after successful revalidate
        MOT_LOG_TRACE("Revalidate JIT context %p source succeeded, now replenishing: %s", jitContext, queryString);
        MOT_ATOMIC_STORE(jitContext->m_validState, JIT_CONTEXT_VALID);

        // although we should be protected by plan locks, we still prefer to guard access to source context
        if (jitContext->m_jitSource->m_usage == JIT_CONTEXT_GLOBAL) {
            LockJitSource(jitContext->m_jitSource);
        }
        if (jitContext->m_jitSource->m_sourceJitContext == nullptr) {
            MOT_LOG_TRACE("Failed to revalidate JIT context %p: concurrent update", jitContext);
            if (jitContext->m_jitSource->m_usage == JIT_CONTEXT_GLOBAL) {
                UnlockJitSource(jitContext->m_jitSource);
            }
            MarkJitContextErrorCompile(jitContext);
            return false;
        }
        if (!ReplenishJitContext(jitContext->m_jitSource->m_sourceJitContext, jitContext, 0)) {
            MOT_LOG_TRACE("Failed to revalidate JIT context %p: replenish from source failed", jitContext);
            if (jitContext->m_jitSource->m_usage == JIT_CONTEXT_GLOBAL) {
                UnlockJitSource(jitContext->m_jitSource);
            }
            MarkJitContextErrorCompile(jitContext);
            return false;
        }
        if (jitContext->m_jitSource->m_usage == JIT_CONTEXT_GLOBAL) {
            UnlockJitSource(jitContext->m_jitSource);
        }

        // we force prepare after replenish on top level session-local context (we do not prepare for execution
        // secondary global context objects in JIT source objects)
        if ((jitContext->m_usage == JIT_CONTEXT_LOCAL) && (jitContext->m_execState != nullptr)) {
            MOT_ATOMIC_STORE(jitContext->m_execState->m_purged, true);
        }
    } else {
        // revalidate source context
        MOT_LOG_TRACE("Re-validating JIT source context %p of query: %s", jitContext, queryString);
        if (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
            if (!RevalidateJitQueryContext(jitContext)) {
                MOT_LOG_TRACE("Failed to revalidate JIT query source context %p of query: %s", jitContext, queryString);
                return false;
            }
        } else {
            if (!RevalidateJitFunctionContext(jitContext)) {
                MOT_LOG_TRACE(
                    "Failed to revalidate JIT function source context %p of function: %s", jitContext, queryString);
                return false;
            }
        }
    }

    MOT_ATOMIC_STORE(jitContext->m_validState, JIT_CONTEXT_VALID);
    MOT_LOG_TRACE("Re-validated JIT context %p of query: %s", jitContext, queryString);
    return true;
}

extern const char* JitContextValidStateToString(uint8_t validState)
{
    // we disregard relation-invalid and deprecate bits
    validState = validState & ~JIT_CONTEXT_RELATION_INVALID;
    validState = validState & ~JIT_CONTEXT_DEPRECATE;
    if (validState == JIT_CONTEXT_VALID) {
        return "valid";
    }
    if (validState == JIT_CONTEXT_INVALID) {
        return "invalid";
    }
    if (validState == JIT_CONTEXT_CHILD_QUERY_INVALID) {
        return "sub-query invalid";
    }
    if (validState == JIT_CONTEXT_CHILD_SP_INVALID) {
        return "sub-SP invalid";
    }
    if (validState == (JIT_CONTEXT_CHILD_QUERY_INVALID | JIT_CONTEXT_CHILD_SP_INVALID)) {
        return "sub-query/SP invalid";
    }
    if (validState == JIT_CONTEXT_PENDING_COMPILE) {
        return "pending compile";
    }
    if (validState == JIT_CONTEXT_DONE_COMPILE) {
        return "done compile";
    }
    if (validState == JIT_CONTEXT_ERROR_COMPILE) {
        return "compile error";
    }
    return "N/A";
}

static inline bool RefersRelation(JitQueryContext* jitContext, uint64_t relationId)
{
    if (((jitContext->m_table != nullptr) && (jitContext->m_tableId == relationId)) ||
        ((jitContext->m_index != nullptr) && (jitContext->m_indexId == relationId)) ||
        ((jitContext->m_innerTable != nullptr) && (jitContext->m_innerTableId == relationId)) ||
        ((jitContext->m_innerIndex != nullptr) && (jitContext->m_innerIndexId == relationId))) {
        return true;
    }
    return false;
}

static inline bool RefersRelation(JitSubQueryContext* subQueryContext, uint64_t relationId)
{
    if (((subQueryContext->m_table != nullptr) && (subQueryContext->m_tableId == relationId)) ||
        ((subQueryContext->m_index != nullptr) && (subQueryContext->m_indexId == relationId))) {
        return true;
    }
    return false;
}

static void PurgeJitQueryContext(JitQueryContext* jitContext, uint64_t relationId)
{
    bool refersRelation = false;

    if ((relationId == 0) || RefersRelation(jitContext, relationId)) {
        refersRelation = true;
    }

    if (!refersRelation) {
        for (uint32_t i = 0; i < jitContext->m_subQueryCount; ++i) {
            JitSubQueryContext* subQueryContext = &jitContext->m_subQueryContext[i];
            if (RefersRelation(subQueryContext, relationId)) {
                refersRelation = true;
                break;
            }
        }
    }

    if (refersRelation) {
        // If the relation is referred either in primary or inner or sub-query context, purge everything as the
        // containing jit-source is marked as expired.
        JitQueryExecState* execState = (JitQueryExecState*)jitContext->m_execState;

        // cleanup keys(s)
        MOT_LOG_TRACE("Purging JIT context %p primary keys by relation id %" PRIu64, jitContext, relationId);
        CleanupJitContextPrimary(jitContext);

        // cleanup JOIN keys(s)
        MOT_LOG_TRACE("Purging JIT context %p inner keys by relation id %" PRIu64, jitContext, relationId);
        CleanupJitContextInner(jitContext);

        // cleanup sub-query keys
        for (uint32_t i = 0; i < jitContext->m_subQueryCount; ++i) {
            JitSubQueryContext* subQueryContext = &jitContext->m_subQueryContext[i];
            MOT_LOG_TRACE(
                "Purging sub-query %u data in JIT context %p by relation id %" PRIu64, i, jitContext, relationId);
            CleanupJitSubQueryContext(subQueryContext, &execState->m_subQueryExecState[i]);
        }
    }

    // cleanup function context for INVOKE query
    if (jitContext->m_commandType == JIT_COMMAND_INVOKE) {
        MOT_LOG_TRACE("Purging sub-function of JIT INVOKE context %p by relationId %" PRIu64, jitContext, relationId);
        PurgeJitContext(jitContext->m_invokeContext, relationId);
    }
}

static void PurgeJitFunctionContext(JitFunctionContext* jitContext, uint64_t relationId)
{
    // purge JIT context in each sub-query call site
    for (uint32_t i = 0; i < jitContext->m_SPSubQueryCount; ++i) {
        JitCallSite* callSite = &jitContext->m_SPSubQueryList[i];
        if (callSite->m_queryContext != nullptr) {
            PurgeJitContext(callSite->m_queryContext, relationId);
        }
    }
}

static void CleanupJitContextPrimary(JitQueryContext* jitContext, bool isDropCachedPlan /* = false */)
{
    CleanupExecStatePrimary(jitContext, (JitQueryExecState*)jitContext->m_execState, isDropCachedPlan);
    jitContext->m_index = nullptr;
    jitContext->m_table = nullptr;
}

static void CleanupJitContextInner(JitQueryContext* jitContext, bool isDropCachedPlan /* = false */)
{
    CleanupExecStateInner(jitContext, (JitQueryExecState*)jitContext->m_execState, isDropCachedPlan);
    jitContext->m_innerIndex = nullptr;
    jitContext->m_innerTable = nullptr;
}

static void CleanupJitSubQueryContextArray(JitQueryContext* jitContext, bool isDropCachedPlan /* = false */)
{
    if (jitContext->m_subQueryContext != nullptr) {
        if (jitContext->m_execState != nullptr) {
            JitQueryExecState* execState = (JitQueryExecState*)jitContext->m_execState;
            MOT_LOG_TRACE("Cleaning up sub-query data array in JIT context %p", jitContext);
            if (execState->m_subQueryExecState != nullptr) {
                for (uint32_t i = 0; i < jitContext->m_subQueryCount; ++i) {
                    JitSubQueryContext* subQueryContext = &jitContext->m_subQueryContext[i];
                    MOT_LOG_TRACE("Cleaning up sub-query %u data in JIT context %p", i, jitContext);
                    CleanupJitSubQueryContext(subQueryContext, &execState->m_subQueryExecState[i]);
                }
            } else {
                for (uint32_t i = 0; i < jitContext->m_subQueryCount; ++i) {
                    JitSubQueryContext* subQueryContext = &jitContext->m_subQueryContext[i];
                    MOT_LOG_TRACE("Cleaning up sub-query %u data in JIT context %p", i, jitContext);
                    subQueryContext->m_index = nullptr;
                    subQueryContext->m_table = nullptr;
                }
            }
        }
        JitMemFree(jitContext->m_subQueryContext, jitContext->m_usage);
        jitContext->m_subQueryContext = nullptr;
    }
}

static void CleanupJitSubQueryContext(
    JitSubQueryContext* subQueryContext, JitSubQueryExecState* subQueryExecState, bool isDropCachedPlan /* = false */)
{
    CleanupSubQueryExecState(subQueryContext, subQueryExecState, isDropCachedPlan);
    subQueryContext->m_index = nullptr;
    subQueryContext->m_table = nullptr;
}

static JitContextPool* AllocSessionJitContextPool()
{
    size_t allocSize = sizeof(JitContextPool);
    JitContextPool* jitContextPool = (JitContextPool*)MOT::MemGlobalAllocAligned(allocSize, L1_CACHE_LINE);
    if (jitContextPool == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Allocate JIT Context",
            "Failed to allocate %u bytes for JIT context pool",
            (unsigned)allocSize);
    } else {
        errno_t erc = memset_s(jitContextPool, allocSize, 0, allocSize);
        securec_check(erc, "\0", "\0");

        if (!InitJitContextPool(jitContextPool, JIT_CONTEXT_LOCAL, GetMotCodegenLimit())) {
            MOT::MemGlobalFree(jitContextPool);
            jitContextPool = nullptr;
        }
    }
    return jitContextPool;
}

extern void FreeSessionJitContextPool(JitContextPool* jitContextPool)
{
    MOT_LOG_TRACE(
        "Destroy session-local JIT context pool, session context count: %u", u_sess->mot_cxt.jit_context_count);
    DestroyJitContextPool(jitContextPool);
    MOT::MemGlobalFree(jitContextPool);
}

static MOT::Key* PrepareJitSearchKey(MotJitContext* jitContext, MOT::Index* index)
{
    MOT::Key* key = index->CreateNewKey();
    if (key == nullptr) {
        MOT_LOG_TRACE("Failed to prepare for executing jitted code: Failed to create reusable search key");
    } else {
        key->InitKey((uint16_t)index->GetKeyLength());
        MOT_LOG_DEBUG("Created key %p from index %p", key, index);
    }
    return key;
}
}  // namespace JitExec
