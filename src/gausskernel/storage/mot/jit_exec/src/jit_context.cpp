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
 *    src/gausskernel/storage/mot/jit_exec/src/jit_context.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global.h"
#include "postgres.h"
#include "knl/knl_session.h"
#include "storage/ipc.h"
#include "jit_context.h"
#include "jit_context_pool.h"
#include "jit_common.h"
#include "jit_tvm.h"
#include "mot_internal.h"
#include "jit_source.h"

namespace JitExec {
DECLARE_LOGGER(JitContext, JitExec);

// The global JIT context pool
static JitContextPool g_globalJitCtxPool;

// forward declarations
static JitContextPool* AllocSessionJitContextPool();
static MOT::Key* PrepareJitSearchKey(JitContext* jitContext, MOT::Index* index);
static void CleanupJitContextPrimary(JitContext* jitContext);
static void CleanupJitContextInner(JitContext* jitContext);

// Helpers to allocate/free from top memory context
inline void* palloc_top(size_t size_bytes)
{
    MemoryContext oldCtx = CurrentMemoryContext;
    CurrentMemoryContext = u_sess->top_mem_cxt;
    void* res = palloc(size_bytes);
    CurrentMemoryContext = oldCtx;
    return res;
}

inline void pfree_top(void* obj)
{
    MemoryContext oldCtx = CurrentMemoryContext;
    CurrentMemoryContext = u_sess->top_mem_cxt;
    pfree(obj);
    CurrentMemoryContext = oldCtx;
}

extern bool InitGlobalJitContextPool()
{
    return InitJitContextPool(&g_globalJitCtxPool, JIT_CONTEXT_GLOBAL, GetMotCodegenLimit());
}

extern void DestroyGlobalJitContextPool()
{
    DestroyJitContextPool(&g_globalJitCtxPool);
}

extern JitContext* AllocJitContext(JitContextUsage usage)
{
    JitContext* result = NULL;
    if (usage == JIT_CONTEXT_GLOBAL) {
        // allocate from global pool
        result = AllocPooledJitContext(&g_globalJitCtxPool);
    } else {
        // allocate from session local pool (create pool on demand and schedule cleanup during end of session)
        if (u_sess->mot_cxt.jit_session_context_pool == NULL) {
            u_sess->mot_cxt.jit_session_context_pool = AllocSessionJitContextPool();
            if (u_sess->mot_cxt.jit_session_context_pool == NULL) {
                return NULL;
            }
        }
        result = AllocPooledJitContext(u_sess->mot_cxt.jit_session_context_pool);
    }
    return result;
}

extern void FreeJitContext(JitContext* jitContext)
{
    if (jitContext != nullptr) {
        if (jitContext->m_usage == JIT_CONTEXT_GLOBAL) {
            FreePooledJitContext(&g_globalJitCtxPool, jitContext);
        } else {
            // in this scenario it is always called by the session who created the context
            --u_sess->mot_cxt.jit_context_count;
            FreePooledJitContext(u_sess->mot_cxt.jit_session_context_pool, jitContext);
        }
    }
}

extern JitContext* CloneJitContext(JitContext* sourceJitContext)
{
    MOT_LOG_TRACE("Cloning JIT context %p of query: %s", sourceJitContext, sourceJitContext->m_queryString);
    JitContext* result = AllocJitContext(JIT_CONTEXT_LOCAL);  // clone is always for local use
    if (result == nullptr) {
        MOT_LOG_TRACE("Failed to allocate JIT context object");
    } else {
        result->m_llvmFunction = sourceJitContext->m_llvmFunction;
        result->m_tvmFunction = sourceJitContext->m_tvmFunction;
        result->m_commandType = sourceJitContext->m_commandType;
        result->m_table = sourceJitContext->m_table;
        result->m_index = sourceJitContext->m_index;
        result->m_argCount = sourceJitContext->m_argCount;
        result->m_nullColumnId = sourceJitContext->m_nullColumnId;
        result->m_queryString = sourceJitContext->m_queryString;
        result->m_innerTable = sourceJitContext->m_innerTable;
        result->m_innerIndex = sourceJitContext->m_innerIndex;
        MOT_LOG_TRACE("Cloned JIT context %p into %p (table=%p)", sourceJitContext, result, result->m_table);
    }
    return result;
}

extern bool PrepareJitContext(JitContext* jitContext)
{
    // allocate argument-is-null array
    if (jitContext->m_argIsNull == NULL) {
        MOT_LOG_TRACE("Allocating null argument array with %u slots", (unsigned)jitContext->m_argCount);
        jitContext->m_argIsNull = (int*)palloc_top(sizeof(int) * jitContext->m_argCount);
        if (jitContext->m_argIsNull == NULL) {
            MOT_LOG_TRACE("Failed to allocate null argument array in size of %d slots", jitContext->m_argCount);
            return false;
        }
    }

    // allocate search key (except when executing INSERT command)
    if ((jitContext->m_searchKey == NULL) && (jitContext->m_commandType != JIT_COMMAND_INSERT)) {
        MOT_LOG_TRACE("Preparing search key from index %s", jitContext->m_index->GetName().c_str());
        jitContext->m_searchKey = PrepareJitSearchKey(jitContext, jitContext->m_index);
        if (jitContext->m_searchKey == NULL) {
            MOT_LOG_TRACE("Failed to allocate reusable search key for JIT context, aborting jitted code execution");
            return false;  // safe cleanup during destroy
        } else {
            MOT_LOG_TRACE("Prepared search key %p (%u bytes) from index %s",
                jitContext->m_searchKey,
                jitContext->m_searchKey->GetKeyLength(),
                jitContext->m_index->GetName().c_str());
        }
    }

    // allocate bitmap-set object for incremental-redo when executing UPDATE command
    if ((jitContext->m_bitmapSet == NULL) && ((jitContext->m_commandType == JIT_COMMAND_UPDATE) ||
                                                 (jitContext->m_commandType == JIT_COMMAND_RANGE_UPDATE))) {
        int fieldCount = (int)jitContext->m_table->GetFieldCount();
        MOT_LOG_TRACE(
            "Initializing reusable bitmap set according to %d fields (including null-bits column 0) in table %s",
            fieldCount,
            jitContext->m_table->GetLongTableName().c_str());
        void* buf = palloc_top(sizeof(MOT::BitmapSet));
        if (buf == NULL) {
            MOT_LOG_TRACE("Failed to allocate reusable bitmap set for JIT context, aborting jitted code execution");
            return false;  // safe cleanup during destroy
        } else {
            jitContext->m_bitmapSet = new (buf) MOT::BitmapSet(fieldCount);
        }
    }

    // allocate end-iterator key object when executing range UPDATE command or special SELECT commands
    if ((jitContext->m_endIteratorKey == NULL) && IsRangeCommand(jitContext->m_commandType)) {
        MOT_LOG_TRACE("Preparing end iterator key for range update/select command from index %s",
            jitContext->m_index->GetName().c_str());
        jitContext->m_endIteratorKey = PrepareJitSearchKey(jitContext, jitContext->m_index);
        if (jitContext->m_endIteratorKey == NULL) {
            MOT_LOG_TRACE(
                "Failed to allocate reusable end iterator key for JIT context, aborting jitted code execution");
            return false;  // safe cleanup during destroy
        } else {
            MOT_LOG_TRACE("Prepared end iterator key %p (%u bytes) for range update/select command from index %s",
                jitContext->m_endIteratorKey,
                jitContext->m_endIteratorKey->GetKeyLength(),
                jitContext->m_index->GetName().c_str());
        }
    }

    if ((jitContext->m_innerSearchKey == NULL) && IsJoinCommand(jitContext->m_commandType)) {
        MOT_LOG_TRACE(
            "Preparing inner search key  for JOIN command from index %s", jitContext->m_innerIndex->GetName().c_str());
        jitContext->m_innerSearchKey = PrepareJitSearchKey(jitContext, jitContext->m_innerIndex);
        if (jitContext->m_innerSearchKey == NULL) {
            MOT_LOG_TRACE(
                "Failed to allocate reusable inner search key for JIT context, aborting jitted code execution");
            return false;  // safe cleanup during destroy
        } else {
            MOT_LOG_TRACE("Prepared inner search key %p (%u bytes) for JOIN command from index %s",
                jitContext->m_innerSearchKey,
                jitContext->m_innerSearchKey->GetKeyLength(),
                jitContext->m_innerIndex->GetName().c_str());
        }
    }

    if ((jitContext->m_innerEndIteratorKey == NULL) && IsJoinCommand(jitContext->m_commandType)) {
        MOT_LOG_TRACE("Preparing inner end iterator key for JOIN command from index %s",
            jitContext->m_innerIndex->GetName().c_str());
        jitContext->m_innerEndIteratorKey = PrepareJitSearchKey(jitContext, jitContext->m_innerIndex);
        if (jitContext->m_innerEndIteratorKey == NULL) {
            MOT_LOG_TRACE(
                "Failed to allocate reusable inner end iterator key for JIT context, aborting jitted code execution");
            return false;  // safe cleanup during destroy
        } else {
            MOT_LOG_TRACE("Prepared inner end iterator key %p (%u bytes) for JOIN command from index %s",
                jitContext->m_innerEndIteratorKey,
                jitContext->m_innerEndIteratorKey->GetKeyLength(),
                jitContext->m_innerIndex->GetName().c_str());
        }
    }

    if ((jitContext->m_outerRowCopy == NULL) && IsJoinCommand(jitContext->m_commandType)) {
        MOT_LOG_TRACE("Preparing outer row copy for JOIN command");
        jitContext->m_outerRowCopy = jitContext->m_table->CreateNewRow();
        if (jitContext->m_outerRowCopy == NULL) {
            MOT_LOG_TRACE("Failed to allocate reusable outer row copy for JIT context, aborting jitted code execution");
            return false;  // safe cleanup during destroy
        }
    }

    return true;
}

extern void DestroyJitContext(JitContext* jitContext)
{
    if (jitContext != nullptr) {
#ifdef MOT_JIT_DEBUG
        MOT_LOG_TRACE("Destroying JIT context %p with %" PRIu64 " executions of query: %s",
            jitContext,
            jitContext->m_execCount,
            jitContext->m_queryString);
#else
        MOT_LOG_TRACE("Destroying %s JIT context %p of query: %s",
            jitContext->m_usage == JIT_CONTEXT_GLOBAL ? "global" : "session-local",
            jitContext,
            jitContext->m_queryString);
#endif

        // remove from JIT source
        if (jitContext->m_jitSource != nullptr) {
            RemoveJitSourceContext(jitContext->m_jitSource, jitContext);
            jitContext->m_jitSource = nullptr;
        }

        // cleanup keys(s)
        CleanupJitContextPrimary(jitContext);

        // cleanup JOIN keys(s)
        CleanupJitContextInner(jitContext);

        // cleanup bitmap set
        if (jitContext->m_bitmapSet != NULL) {
            jitContext->m_bitmapSet->Destroy();
            jitContext->m_bitmapSet->MOT::BitmapSet::~BitmapSet();
            pfree_top(jitContext->m_bitmapSet);
            jitContext->m_bitmapSet = NULL;
        }

        // cleanup code generator (only in global-usage)
        if (jitContext->m_codeGen && (jitContext->m_usage == JIT_CONTEXT_GLOBAL)) {
            FreeGsCodeGen(jitContext->m_codeGen);
            jitContext->m_codeGen = NULL;
        }

        // cleanup null argument array
        if (jitContext->m_argIsNull != NULL) {
            pfree_top(jitContext->m_argIsNull);
            jitContext->m_argIsNull = NULL;
        }

        // cleanup TVM function (only in global-usage)
        if (jitContext->m_tvmFunction && (jitContext->m_usage == JIT_CONTEXT_GLOBAL)) {
            delete jitContext->m_tvmFunction;
            jitContext->m_tvmFunction = NULL;
        }

        // cleanup TVM execution context
        if (jitContext->m_execContext != NULL) {
            tvm::freeExecContext(jitContext->m_execContext);
            jitContext->m_execContext = NULL;
        }

        FreeJitContext(jitContext);
    }
}

extern void PurgeJitContext(JitContext* jitContext, uint64_t relationId)
{
    if (jitContext != nullptr) {
#ifdef MOT_JIT_DEBUG
        MOT_LOG_TRACE("Purging JIT context %p by external table %" PRIu64 " with %" PRIu64 " executions of query: %s",
            jitContext,
            relationId,
            jitContext->m_execCount,
            jitContext->m_queryString);
#else
        MOT_LOG_TRACE("Purging %s JIT context %p by external table %" PRIu64 " of query: %s",
            jitContext->m_usage == JIT_CONTEXT_GLOBAL ? "global" : "session-local",
            jitContext,
            relationId,
            jitContext->m_queryString);
#endif

        // cleanup keys(s)
        if ((jitContext->m_table != nullptr) && (jitContext->m_table->GetTableExId() == relationId)) {
            CleanupJitContextPrimary(jitContext);
        }

        // cleanup JOIN keys(s)
        if ((jitContext->m_innerTable != nullptr) && (jitContext->m_innerTable->GetTableExId() == relationId)) {
            CleanupJitContextInner(jitContext);
        }
    }
}

static void CleanupJitContextPrimary(JitContext* jitContext)
{
    if (jitContext->m_table) {
        if (jitContext->m_index) {
            if (jitContext->m_searchKey) {
                jitContext->m_index->DestroyKey(jitContext->m_searchKey);
                jitContext->m_searchKey = NULL;
            }

            if (jitContext->m_endIteratorKey != nullptr) {
                jitContext->m_index->DestroyKey(jitContext->m_endIteratorKey);
                jitContext->m_endIteratorKey = NULL;
            }
        }

        // cleanup JOIN outer row copy
        if (jitContext->m_outerRowCopy != nullptr) {
            jitContext->m_table->DestroyRow(jitContext->m_outerRowCopy);
            jitContext->m_outerRowCopy = NULL;
        }
    }
}
static void CleanupJitContextInner(JitContext* jitContext)
{
    if (jitContext->m_innerTable != nullptr) {
        if (jitContext->m_innerIndex != nullptr) {
            if (jitContext->m_innerSearchKey != nullptr) {
                jitContext->m_innerIndex->DestroyKey(jitContext->m_innerSearchKey);
                jitContext->m_innerSearchKey = NULL;
            }

            if (jitContext->m_innerEndIteratorKey != nullptr) {
                jitContext->m_innerIndex->DestroyKey(jitContext->m_innerEndIteratorKey);
                jitContext->m_innerEndIteratorKey = NULL;
            }
        }
    }
}

static JitContextPool* AllocSessionJitContextPool()
{
    JitContextPool* result = (JitContextPool*)palloc_top(sizeof(JitContextPool));
    if (result == NULL) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM, "Allocate JIT Context", "Failed to allocate buffer for JIT context");
        return NULL;
    }

    if (!InitJitContextPool(result, JIT_CONTEXT_LOCAL, GetMotCodegenLimit())) {
        pfree_top(result);
        return NULL;
    }
    return result;
}

extern void FreeSessionJitContextPool(JitContextPool* jitContextPool)
{
    DestroyJitContextPool(jitContextPool);
    pfree_top(jitContextPool);
}

static MOT::Key* PrepareJitSearchKey(JitContext* jitContext, MOT::Index* index)
{
    MOT::Key* key = index->CreateNewKey();
    if (key == NULL) {
        MOT_LOG_TRACE("Failed to prepare for executing jitted code: Failed to create reusable search key");
    } else {
        key->InitKey((uint16_t)index->GetKeyLength());
        MOT_LOG_DEBUG("Created key %p from index %p", key, index);
    }
    return key;
}
}  // namespace JitExec
