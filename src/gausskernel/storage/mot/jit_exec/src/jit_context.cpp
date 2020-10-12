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
#include "mm_global_api.h"

namespace JitExec {
DECLARE_LOGGER(JitContext, JitExec);

// The global JIT context pool
static JitContextPool g_globalJitCtxPool __attribute__((aligned(64))) = {0};

// forward declarations
static JitContextPool* AllocSessionJitContextPool();
static MOT::Key* PrepareJitSearchKey(JitContext* jitContext, MOT::Index* index);
static void CleanupJitContextPrimary(JitContext* jitContext);
static void CleanupJitContextInner(JitContext* jitContext);
static void CleanupJitContextSubQueryDataArray(JitContext* jitContext);
static void CleanupJitContextSubQueryData(JitContext::SubQueryData* subQueryData);

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
        result->m_subQueryCount = sourceJitContext->m_subQueryCount;

        // clone sub-query tuple descriptor array
        MOT_LOG_TRACE("Cloning %u sub-query data items", (unsigned)sourceJitContext->m_subQueryCount);
        if (sourceJitContext->m_subQueryCount > 0) {
            uint32_t allocSize = sizeof(JitContext::SubQueryData) * sourceJitContext->m_subQueryCount;
            result->m_subQueryData = (JitContext::SubQueryData*)MOT::MemGlobalAllocAligned(allocSize, L1_CACHE_LINE);
            if (result->m_subQueryData == nullptr) {
                MOT_REPORT_ERROR(MOT_ERROR_OOM,
                    "Generate JIT Code",
                    "Failed to allocate %u bytes for %u sub-query data array in JIT context object",
                    allocSize,
                    (unsigned)sourceJitContext->m_subQueryCount);
                FreeJitContext(result);
                result = nullptr;
            } else {
                for (uint32_t i = 0; i < sourceJitContext->m_subQueryCount; ++i) {
                    // copy known members
                    result->m_subQueryData[i].m_commandType = sourceJitContext->m_subQueryData[i].m_commandType;
                    result->m_subQueryData[i].m_table = sourceJitContext->m_subQueryData[i].m_table;
                    result->m_subQueryData[i].m_index = sourceJitContext->m_subQueryData[i].m_index;

                    // nullify other members
                    result->m_subQueryData[i].m_tupleDesc = nullptr;
                    result->m_subQueryData[i].m_slot = nullptr;
                    result->m_subQueryData[i].m_searchKey = nullptr;
                    result->m_subQueryData[i].m_endIteratorKey = nullptr;
                }
            }
        }
    }

    if (result != nullptr) {
        MOT_LOG_TRACE("Cloned JIT context %p into %p (table=%p)", sourceJitContext, result, result->m_table);
    }
    return result;
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

extern bool ReFetchIndices(JitContext* jitContext)
{
    // re-fetch main index
    if ((jitContext->m_commandType != JIT_COMMAND_INSERT) && (jitContext->m_index == nullptr)) {
        if (jitContext->m_indexId == 0) {
            MOT_LOG_TRACE("Cannot re-fetch index: missing index identifier");
            return false;
        }
        jitContext->m_index = jitContext->m_table->GetIndexByExtId(jitContext->m_indexId);
        if (jitContext->m_index == nullptr) {
            MOT_LOG_TRACE("Failed to fetch index by extern id %" PRIu64, jitContext->m_indexId);
            return false;
        }
    }

    // re-fetch inner index (JOIN commands only)
    if (IsJoinCommand(jitContext->m_commandType)) {
        if (jitContext->m_innerIndex == nullptr) {
            if (jitContext->m_innerIndexId == 0) {
                MOT_LOG_TRACE("Cannot re-fetch inner index: missing index identifier");
                return false;
            }
            jitContext->m_innerIndex = jitContext->m_innerTable->GetIndexByExtId(jitContext->m_innerIndexId);
            if (jitContext->m_innerIndex == nullptr) {
                MOT_LOG_TRACE("Failed to fetch inner index by extern id %" PRIu64, jitContext->m_innerIndexId);
                return false;
            }
        }
    }

    // re-fetch sub-query indices (COMPOUND commands only)
    if (jitContext->m_commandType == JIT_COMMAND_COMPOUND_SELECT) {
        for (uint32_t i = 0; i < jitContext->m_subQueryCount; ++i) {
            JitContext::SubQueryData* subQueryData = &jitContext->m_subQueryData[i];
            if (subQueryData->m_index == nullptr) {
                if (subQueryData->m_indexId == 0) {
                    MOT_LOG_TRACE("Cannot re-fetch sub-query %u index: missing index identifier", i);
                    return false;
                }
                subQueryData->m_index = subQueryData->m_table->GetIndexByExtId(subQueryData->m_indexId);
                if (subQueryData->m_index == nullptr) {
                    MOT_LOG_TRACE(
                        "Failed to fetch sub-query %u index by extern id %" PRIu64, i, subQueryData->m_indexId);
                    return false;
                }
            }
        }
    }

    return true;
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

    // re-fetch all index objects in case they were removed after TRUNCATE TABLE
    if (jitContext->m_commandType != JIT_COMMAND_INSERT) {
        if (!ReFetchIndices(jitContext)) {
            return false;  // safe cleanup during destroy
        }
    }

    // allocate search key (except when executing INSERT command)
    if ((jitContext->m_searchKey == nullptr) && (jitContext->m_commandType != JIT_COMMAND_INSERT)) {
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
        }

        uint8_t* bitmapData = (uint8_t*)palloc_top(MOT::BitmapSet::GetLength(fieldCount));
        if (bitmapData == NULL) {
            MOT_LOG_TRACE("Failed to allocate reusable bitmap set for JIT context, aborting jitted code execution");
            pfree_top(buf);
            return false;  // safe cleanup during destroy
        }
        jitContext->m_bitmapSet = new (buf) MOT::BitmapSet(bitmapData, fieldCount);
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

    // allocate inner loop search key for JOIN commands
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

    // allocate inner loop end-iterator search key for JOIN commands
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

    // preparing outer row copy for JOIN commands
    if ((jitContext->m_outerRowCopy == NULL) && IsJoinCommand(jitContext->m_commandType)) {
        MOT_LOG_TRACE("Preparing outer row copy for JOIN command");
        jitContext->m_outerRowCopy = jitContext->m_table->CreateNewRow();
        if (jitContext->m_outerRowCopy == NULL) {
            MOT_LOG_TRACE("Failed to allocate reusable outer row copy for JIT context, aborting jitted code execution");
            return false;  // safe cleanup during destroy
        }
    }

    // prepare sub-query data for COMPOUND commands
    if (jitContext->m_commandType == JIT_COMMAND_COMPOUND_SELECT) {
        // allocate sub-query search keys and generate tuple table slot array using session top memory context
        MemoryContext oldCtx = CurrentMemoryContext;
        CurrentMemoryContext = u_sess->top_mem_cxt;
        for (uint32_t i = 0; i < jitContext->m_subQueryCount; ++i) {
            JitContext::SubQueryData* subQueryData = &jitContext->m_subQueryData[i];
            if (subQueryData->m_tupleDesc == nullptr) {
                MOT_LOG_TRACE("Preparing sub-query %u tuple descriptor", i);
                List* targetList = GetSubQueryTargetList(jitContext->m_queryString, i);
                if (targetList == nullptr) {
                    MOT_LOG_TRACE("Failed to locate sub-query %u target list", i);
                    CurrentMemoryContext = oldCtx;
                    return false;  // safe cleanup during destroy
                } else {
                    subQueryData->m_tupleDesc = ExecCleanTypeFromTL(targetList, false);
                    if (subQueryData->m_tupleDesc == nullptr) {
                        MOT_LOG_TRACE("Failed to create sub-query %u tuple descriptor from target list", i);
                        CurrentMemoryContext = oldCtx;
                        return false;  // safe cleanup during destroy
                    }
                }
            }
            if (subQueryData->m_slot == nullptr) {
                MOT_ASSERT(subQueryData->m_tupleDesc != nullptr);
                MOT_LOG_TRACE("Preparing sub-query %u result slot", i);
                subQueryData->m_slot = MakeSingleTupleTableSlot(subQueryData->m_tupleDesc);
                if (subQueryData->m_slot == nullptr) {
                    MOT_LOG_TRACE("Failed to generate sub-query %u tuple table slot", i);
                    CurrentMemoryContext = oldCtx;
                    return false;  // safe cleanup during destroy
                }
            }
            if (subQueryData->m_searchKey == nullptr) {
                MOT_LOG_TRACE(
                    "Preparing sub-query %u search key from index %s", i, subQueryData->m_index->GetName().c_str());
                subQueryData->m_searchKey = PrepareJitSearchKey(jitContext, subQueryData->m_index);
                if (subQueryData->m_searchKey == nullptr) {
                    MOT_LOG_TRACE("Failed to generate sub-query %u search key", i);
                    CurrentMemoryContext = oldCtx;
                    return false;  // safe cleanup during destroy
                }
            }
            if ((subQueryData->m_commandType == JIT_COMMAND_AGGREGATE_RANGE_SELECT) &&
                (subQueryData->m_endIteratorKey == nullptr)) {
                MOT_LOG_TRACE("Preparing sub-query %u end-iterator search key from index %s",
                    i,
                    subQueryData->m_index->GetName().c_str());
                subQueryData->m_endIteratorKey = PrepareJitSearchKey(jitContext, subQueryData->m_index);
                if (subQueryData->m_endIteratorKey == nullptr) {
                    MOT_LOG_TRACE("Failed to generate sub-query %u end-iterator search key", i);
                    CurrentMemoryContext = oldCtx;
                    return false;  // safe cleanup during destroy
                }
            }
        }
        CurrentMemoryContext = oldCtx;
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

        // cleanup sub-query data array
        CleanupJitContextSubQueryDataArray(jitContext);

        // cleanup keys(s)
        CleanupJitContextPrimary(jitContext);

        // cleanup JOIN keys(s)
        CleanupJitContextInner(jitContext);

        // cleanup bitmap set
        if (jitContext->m_bitmapSet != NULL) {
            pfree_top(jitContext->m_bitmapSet->GetData());
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
            MOT_LOG_TRACE("Purging JIT context %p primary keys by relation id %" PRIu64, jitContext, relationId);
            CleanupJitContextPrimary(jitContext);
        }

        // cleanup JOIN keys(s)
        if ((jitContext->m_innerTable != nullptr) && (jitContext->m_innerTable->GetTableExId() == relationId)) {
            MOT_LOG_TRACE("Purging JIT context %p inner keys by relation id %" PRIu64, jitContext, relationId);
            CleanupJitContextInner(jitContext);
        }

        // cleanup sub-query keys
        for (uint32_t i = 0; i < jitContext->m_subQueryCount; ++i) {
            JitContext::SubQueryData* subQueryData = &jitContext->m_subQueryData[i];
            if ((subQueryData->m_table != nullptr) && (subQueryData->m_table->GetTableExId() == relationId)) {
                MOT_LOG_TRACE(
                    "Purging sub-query %u data in JIT context %p by relation id %" PRIu64, i, jitContext, relationId);
                CleanupJitContextSubQueryData(subQueryData);
            }
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
            jitContext->m_index = nullptr;
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
            jitContext->m_innerIndex = nullptr;
        }
    }
}

static void CleanupJitContextSubQueryDataArray(JitContext* jitContext)
{
    if (jitContext->m_subQueryData != nullptr) {
        MOT_LOG_TRACE("Cleaning up sub-query data array in JIT context %p", jitContext);
    }
    for (uint32_t i = 0; i < jitContext->m_subQueryCount; ++i) {
        JitContext::SubQueryData* subQueryData = &jitContext->m_subQueryData[i];
        MOT_LOG_TRACE("Cleaning up sub-query %u data in JIT context %p", i, jitContext);
        CleanupJitContextSubQueryData(subQueryData);
    }
    if (jitContext->m_subQueryData != nullptr) {
        MOT::MemGlobalFree(jitContext->m_subQueryData);
        jitContext->m_subQueryData = nullptr;
    }
}

static void CleanupJitContextSubQueryData(JitContext::SubQueryData* subQueryData)
{
    if (subQueryData->m_slot != nullptr) {
        ExecDropSingleTupleTableSlot(subQueryData->m_slot);
        subQueryData->m_slot = nullptr;
    }
    if (subQueryData->m_tupleDesc != nullptr) {
        FreeTupleDesc(subQueryData->m_tupleDesc);
        subQueryData->m_tupleDesc = nullptr;
    }
    if (subQueryData->m_index != nullptr) {
        if (subQueryData->m_searchKey != nullptr) {
            subQueryData->m_index->DestroyKey(subQueryData->m_searchKey);
            subQueryData->m_searchKey = nullptr;
        }
        if (subQueryData->m_endIteratorKey != nullptr) {
            subQueryData->m_index->DestroyKey(subQueryData->m_endIteratorKey);
            subQueryData->m_endIteratorKey = nullptr;
        }
        subQueryData->m_index = nullptr;
    }
}

static JitContextPool* AllocSessionJitContextPool()
{
    size_t allocSize = sizeof(JitContextPool);
    JitContextPool* jitContextPool = (JitContextPool*)MOT::MemGlobalAllocAligned(allocSize, L1_CACHE_LINE);
    if (jitContextPool == NULL) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Allocate JIT Context",
            "Failed to allocate %u bytes for JIT context pool",
            (unsigned)allocSize);
    } else {
        errno_t erc = memset_s(jitContextPool, allocSize, 0, allocSize);
        securec_check(erc, "\0", "\0");

        if (!InitJitContextPool(jitContextPool, JIT_CONTEXT_LOCAL, GetMotCodegenLimit())) {
            MOT::MemGlobalFree(jitContextPool);
            jitContextPool = NULL;
        }
    }
    return jitContextPool;
}

extern void FreeSessionJitContextPool(JitContextPool* jitContextPool)
{
    DestroyJitContextPool(jitContextPool);
    MOT::MemGlobalFree(jitContextPool);
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
