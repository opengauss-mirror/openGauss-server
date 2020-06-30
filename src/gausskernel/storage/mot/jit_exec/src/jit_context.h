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
 * jit_context.h
 *    The context for executing a jitted function.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/src/jit_context.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_CONTEXT_H
#define JIT_CONTEXT_H

#include "storage/mot/jit_def.h"
#include "jit_llvm_exec.h"
#include "jit_tvm.h"
#include "mot_engine.h"

namespace JitExec {
struct JitContextPool;
struct JitSource;

/**
 * @typedef The context for executing a jitted function.
 */
struct JitContext {
    /*---------------------- Meta-data -------------------*/
    /** @var The LLVM code generator for this query. */
    dorado::GsCodeGen* m_codeGen;  // L1 offset 0

    /** @var The LLVM jit-compiled code for executing a query. */
    JitFunc m_llvmFunction;  // L1 offset 8

    /** @var The TVM jit-compiled code for executing a query (when LLVM is unavailable on some platform). */
    tvm::Function* m_tvmFunction;  // L1 offset 16

    /** @var The type of the executed query (insert/update/select/delete). */
    JitCommandType m_commandType;  // L1 offset 24

    /** @var Specifies whether this is a global or session-local context. */
    JitContextUsage m_usage;  // L1 offset 25

    /*---------------------- Execution state -------------------*/
    /** @var Maximum number of slots in the null datum array. */
    uint16_t m_argCount;  // L1 offset 26 (constant)

    /** @var Error reporting information. The NULL violation column identifier. */
    int m_nullColumnId;  // L1 offset 28 (reusable)

    /** @var Null datum management array. */
    int* m_argIsNull;  // L1 offset 32 (reusable)

    /** @var The table on which the query is to be executed . */
    MOT::Table* m_table;  // L1 offset 40 (constant)

    /** @var The index on which the query is to be executed . */
    MOT::Index* m_index;  // L1 offset 48 (constant)

    /**
     * @var The key object used to search a row for update/select/delete. This object can be reused
     * in each query.
     */
    MOT::Key* m_searchKey;  // L1 offset 56 (reusable)

    /** @var bitmap set used for incremental redo (only during update) */
    MOT::BitmapSet* m_bitmapSet;  // L1 offset 0 (reusable)

    /**
     * @var The key object used to set the end iterator for range scans. This object can be reused
     * in each query.
     */
    MOT::Key* m_endIteratorKey;  // L1 offset 8 (reusable)

    /** @var Execution context used for pseudo-LLVM. */
    tvm::ExecContext* m_execContext;  // L1 offset 16

    /** @var Chain all JIT contexts in the same thread for cleanup during session end. */
    struct JitContext* m_next;  // L1 offset 24

    /** @var The source query string. */
    const char* m_queryString;  // L1 offset 32 (constant)

    /*---------------------- Range Scan execution state -------------------*/
    /** @var Begin iterator for range select (stateful execution). */
    MOT::IndexIterator* m_beginIterator;  // L1 offset 40

    /** @var End iterator for range select (stateful execution). */
    MOT::IndexIterator* m_endIterator;  // L1 offset 48

    /** @var Row for range select (stateful execution). */
    MOT::Row* m_row;  // L1 offset 56

    /** @var Stateful counter for limited range scans. */
    uint32_t m_limitCounter;  // L1 offset 0

    /** @var Scan ended flag (stateful execution). */
    uint32_t m_scanEnded;  // L1 offset 4

    /** @var Aggregated value used for SUM/MAX/MIN/COUNT() operators. */
    Datum m_aggValue;  // L1 offset 8

    /** @var Specifies no value was aggregated yet for MAX/MIN() operators. */
    uint64_t m_maxMinAggNull;  // L1 offset 16

    /** @var Aggregated array used for AVG() operators. */
    Datum m_avgArray;  // L1 offset 24

    /** @var A set of distinct items. */
    void* m_distinctSet;  // L1 offset 32

    /*---------------------- JOIN execution state -------------------*/
    /** @var The table used for inner scan in JOIN queries. */
    MOT::Table* m_innerTable;  // L1 offset 40 (constant)

    /** @var The index used for inner scan in JOIN queries. */
    MOT::Index* m_innerIndex;  // L1 offset 48 (constant)

    /** @var The key object used to search row or iterator in the inner scan in JOIN queries. */
    MOT::Key* m_innerSearchKey;  // L1 offset 56 (reusable)

    /** @var The key object used to search end iterator in the inner scan in JOIN queries. */
    MOT::Key* m_innerEndIteratorKey;  // L1 offset 0 (reusable)

    /** @var Begin iterator for inner scan in JOIN queries (stateful execution). */
    MOT::IndexIterator* m_innerBeginIterator;  // L1 offset 8

    /** @var End iterator for inner scan in JOIN queries (stateful execution). */
    MOT::IndexIterator* m_innerEndIterator;  // L1 offset 16

    /** @var Row for inner scan in JOIN queries (stateful execution). */
    MOT::Row* m_innerRow;  // L1 offset 24

    /** @var Copy of the outer row (SILO overrides it in the inner scan). */
    MOT::Row* m_outerRowCopy;  // L1 offset 32

    /** @var Scan ended flag (stateful execution). */
    uint64_t m_innerScanEnded;  // L1 offset 40

    /*---------------------- Cleanup -------------------*/
    /** @var Chain all context objects related to the same source, for cleanup during relation modification. */
    JitContext* m_nextInSource;  // L1 offset 48

    /** @var The JIT source from which this context originated. */
    JitSource* m_jitSource;  // L1 offset 56

    /*---------------------- Debug execution state -------------------*/
    /** @var The number of times this context was invoked for execution. */
#ifdef MOT_JIT_DEBUG
    uint64_t m_execCount;  // L1 offset 0
#endif
};

/**
 * @brief Initializes the global JIT context pool.
 * @return True if initialization succeeded, otherwise false.
 */
extern bool InitGlobalJitContextPool();

/** @brief Destroys the global JIT context pool. */
extern void DestroyGlobalJitContextPool();

/**
 * @brief Allocates a JIT context for future execution.
 * @param usage Specify the usage scope of the context (global or session-local).
 * @return The allocated JIT context, or NULL if allocation failed.
 */
extern JitContext* AllocJitContext(JitContextUsage usage);

/**
 * @brief Returns a JIT context back to its source pool.
 * @param jitContext The JIT context to free.
 */
extern void FreeJitContext(JitContext* jitContext);

/**
 * @brief Clones a JIT context.
 * @param sourceJitContext The context to clone.
 * @return The cloned context or NULL if failed.
 */
extern JitContext* CloneJitContext(JitContext* sourceJitContext);

/**
 * @brief Prepares a JIT context object for execution. All internal members are allocated and
 * initialized on-demand, according to the SQL command being executed.
 * @param jitContext The JIT context to prepare.
 * @return True if succeeded, otherwise false. Consult @ref mm_get_root_error() for further details.
 */
extern bool PrepareJitContext(JitContext* jitContext);

/**
 * @brief Destroys a JIT context produced by a previous call to JitCodegenQuery.
 * @detail All internal resources associated with the context object are released, and the context
 * is returned to its source context pool.
 * @param jitContext The JIT context to destroy.
 */
extern void DestroyJitContext(JitContext* jitContext);

/**
 * @brief Release all resources related to the primary table.
 * @param jitContext The JIT context to be purged.
 * @param relationId The external identifier of the modified relation that triggered purge.
 */
extern void PurgeJitContext(JitContext* jitContext, uint64_t relationId);
}  // namespace JitExec

#endif
