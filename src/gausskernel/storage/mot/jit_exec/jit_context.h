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
 *    src/gausskernel/storage/mot/jit_exec/jit_context.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_CONTEXT_H
#define JIT_CONTEXT_H

#include "storage/mot/jit_def.h"
#include "jit_llvm.h"
#include "jit_tvm.h"
#include "mot_engine.h"

namespace JitExec {
struct JitContextPool;
struct JitSource;

/** @struct Array of constant datum objects used in JIT execution. */
struct JitDatum {
    /** @var The constant value. */
    Datum m_datum;

    /** @var The constant type. */
    int m_type;

    /** @var The constant is-null property. */
    int m_isNull;
};

struct JitDatumArray {
    /** @var The number of constant datum objects used by the jitted function (global copy for all contexts). */
    uint64_t m_datumCount;

    /** @var The array of constant datum objects used by the jitted function (global copy for all contexts). */
    JitDatum* m_datums;
};

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
    MOT::Index* m_index;  // L1 offset 48 (may vary during TRUNCATE TABLE)

    /** @var The index identifier for re-fetching it after TRUNCATE TABLE. */
    uint64_t m_indexId;  // L1 offset 56 (constant)

    /**
     * @var The key object used to search a row for update/select/delete. This object can be reused
     * in each query.
     */
    MOT::Key* m_searchKey;  // L1 offset 0 (reusable)

    /** @var bitmap set used for incremental redo (only during update) */
    MOT::BitmapSet* m_bitmapSet;  // L1 offset 8 (reusable)

    /**
     * @var The key object used to set the end iterator for range scans. This object can be reused
     * in each query.
     */
    MOT::Key* m_endIteratorKey;  // L1 offset 16 (reusable)

    /** @var Execution context used for pseudo-LLVM. */
    tvm::ExecContext* m_execContext;  // L1 offset 24

    /** @var Chain all JIT contexts in the same thread for cleanup during session end. */
    struct JitContext* m_next;  // L1 offset 32

    /** @var The source query string. */
    const char* m_queryString;  // L1 offset 40 (constant)

    /** @var The array of constant datum objects used by the jitted function (global copy for all contexts). */
    JitDatumArray m_constDatums;

    /*---------------------- Range Scan execution state -------------------*/
    /** @var Begin iterator for range select (stateful execution). */
    MOT::IndexIterator* m_beginIterator;  // L1 offset 48

    /** @var End iterator for range select (stateful execution). */
    MOT::IndexIterator* m_endIterator;  // L1 offset 56

    /** @var Row for range select (stateful execution). */
    MOT::Row* m_row;  // L1 offset 0

    /** @var Stateful counter for limited range scans. */
    uint32_t m_limitCounter;  // L1 offset 8

    /** @var Scan ended flag (stateful execution). */
    uint32_t m_scanEnded;  // L1 offset 12

    /** @var Aggregated value used for SUM/MAX/MIN/COUNT() operators. */
    Datum m_aggValue;  // L1 offset 16

    /** @var Specifies no value was aggregated yet for MAX/MIN() operators. */
    uint64_t m_maxMinAggNull;  // L1 offset 24

    /** @var Aggregated array used for AVG() operators. */
    Datum m_avgArray;  // L1 offset 32

    /** @var A set of distinct items. */
    void* m_distinctSet;  // L1 offset 40

    /*---------------------- JOIN execution state -------------------*/
    /** @var The table used for inner scan in JOIN queries. */
    MOT::Table* m_innerTable;  // L1 offset 48 (constant)

    /** @var The index used for inner scan in JOIN queries. */
    MOT::Index* m_innerIndex;  // L1 offset 56 (may vary during TRUNCATE TABLE)

    /** @var The index identifier for re-fetching it after TRUNCATE TABLE. */
    uint64_t m_innerIndexId;  // L1 offset 0 (constant)

    /** @var The key object used to search row or iterator in the inner scan in JOIN queries. */
    MOT::Key* m_innerSearchKey;  // L1 offset 8 (reusable)

    /** @var The key object used to search end iterator in the inner scan in JOIN queries. */
    MOT::Key* m_innerEndIteratorKey;  // L1 offset 16 (reusable)

    /** @var Begin iterator for inner scan in JOIN queries (stateful execution). */
    MOT::IndexIterator* m_innerBeginIterator;  // L1 offset 24

    /** @var End iterator for inner scan in JOIN queries (stateful execution). */
    MOT::IndexIterator* m_innerEndIterator;  // L1 offset 32

    /** @var Row for inner scan in JOIN queries (stateful execution). */
    MOT::Row* m_innerRow;  // L1 offset 40

    /** @var Copy of the outer row (SILO overrides it in the inner scan). */
    MOT::Row* m_outerRowCopy;  // L1 offset 48

    /** @var Scan ended flag (stateful execution). */
    uint64_t m_innerScanEnded;  // L1 offset 56

    /*---------------------- Compound Query Execution -------------------*/
    struct SubQueryData {
        /** @var The sub-query command type (point-select, aggregate range select or limit 1 range-select). */
        JitCommandType m_commandType;  // L1 offset 0

        /** @var Align next member to 8-byte offset. */
        uint8_t m_padding[7];  // L1 offset 1

        /** @var Tuple descriptor for the sub-query result. */
        TupleDesc m_tupleDesc;  // L1 offset 8

        /** @var Sub-query execution result. */
        TupleTableSlot* m_slot;  // L1 offset 16

        /** @var The table used by the sub-query. */
        MOT::Table* m_table;  // L1 offset 24

        /** @var The index used by the sub-query. */
        MOT::Index* m_index;  // L1 offset 32

        /** @var The index identifier for re-fetching it after TRUNCATE TABLE. */
        uint64_t m_indexId;  // L1 offset 40

        /** @var The search key used by the sub-query. */
        MOT::Key* m_searchKey;  // L1 offset 48

        /** @var The search key used by a range select sub-query (whether aggregated or limited). */
        MOT::Key* m_endIteratorKey;  // L1 offset 56
    };

    /** @var Array of tuple descriptors for each sub-query. */
    SubQueryData* m_subQueryData;  // L1 offset 0

    /** @var The number of sub-queries used. */
    uint64_t m_subQueryCount;  // L1 offset 8

    /*---------------------- Cleanup -------------------*/
    /** @var Chain all context objects related to the same source, for cleanup during relation modification. */
    JitContext* m_nextInSource;  // L1 offset 16

    /** @var The JIT source from which this context originated. */
    JitSource* m_jitSource;  // L1 offset 24

    /*---------------------- Batch Execution -------------------*/
    /**
     * @var The number of times (iterations) a single stateful query was invoked. Used for distinguishing query
     * boundaries in a stateful query execution when client uses batches.
     */
    uint64_t m_iterCount;  // L1 offset 32

    /** @var The number of full query executions. */
    uint64_t m_queryCount;  // L1 offset 40

    /*---------------------- Debug execution state -------------------*/
    /** @var The number of times this context was invoked for execution. */
#ifdef MOT_JIT_DEBUG
    uint64_t m_execCount;  // L1 offset 48
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
 * @brief Re-fetches all index objects in a JIT context.
 * @param jitContext The JIT context for which index objects should be re-fetched.
 * @return True if the operation succeeded, otherwise false.
 */
extern bool ReFetchIndices(JitContext* jitContext);

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
 * @brief Release all resources related to any table used by the JIT context object.
 * @detail When a DDL command is executed (such as DROP table), all resources associated with the table must be
 * reclaimed immediately. Any attempt to do so at a later phase (i.e. after DDL ends, and other cached plans are
 * invalidated), will result in a crash (because the key/row pools, into which cached plan objects are to be returned,
 * are already destroyed during DDL execution). So we provide API to purge a JIT context object from all resources
 * associated with a relation, which can be invoked during DDL execution (so by the time the cached plan containing the
 * JIT context is invalidated, the members related to the affected MOT relation had already been reclaimed).
 * @param jitContext The JIT context to be purged.
 * @param relationId The external identifier of the modified relation that triggered purge.
 * @see SetJitSourceExpired().
 */
extern void PurgeJitContext(JitContext* jitContext, uint64_t relationId);
}  // namespace JitExec

#endif
