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
#include "mot_engine.h"
#include "executor/spi.h"
#include "utils/plpgsql.h"

/*---------------------- JIT Context Valid Flags -------------------*/
/** @define Denotes that the JIT context and all its descendants are valid. */
#define JIT_CONTEXT_VALID 0x00

/**
 * @define Denotes that the JIT context itself is invalid due to DDL. This flag exists for triggering revalidate
 * attempt.
 */
#define JIT_CONTEXT_INVALID 0x01

/** @define Denotes that the JIT context itself is valid, but one of its descendant queries is invalid. */
#define JIT_CONTEXT_CHILD_QUERY_INVALID 0x02

/** @define Denotes that the JIT context itself is valid, but one of its descendant sub-SPs is invalid. */
#define JIT_CONTEXT_CHILD_SP_INVALID 0x04

/** @define Denotes that the JIT context is valid, but needs to re-fetch relation pointers. */
#define JIT_CONTEXT_RELATION_INVALID 0x08

/** @define Denotes that the JIT context is deprecate, and needs reattachment to a valid source. */
#define JIT_CONTEXT_DEPRECATE 0x10

/** @define Denotes that the JIT context is temporarily invalid, until its source finishes compilation. */
#define JIT_CONTEXT_PENDING_COMPILE 0x20

/**
 * @define Denotes that the JIT context finished compilation successfully. This is a transient state, until it goes
 * through revalidation in the next query execution.
 */
#define JIT_CONTEXT_DONE_COMPILE 0x40

/**
 * @define Denotes that the JIT context finished compilation with error. This is a transient state, until it goes
 * through revalidation after a relevant DDL takes place.
 */
#define JIT_CONTEXT_ERROR_COMPILE 0x80

namespace JitExec {
struct JitContextPool;
struct JitSource;
struct MotJitContext;
struct JitFunctionContext;
struct JitNonNativeSortExecState;
struct JitNonNativeSortParams;

/** @struct The parameter passing mode into a stored procedure. */
enum class JitParamMode {
    /** @var Denotes the parameter is being passed directly as-is into the store procedure. */
    JIT_PARAM_DIRECT,

    /** @var Denotes the parameter is copied into the parameter list of the store procedure. */
    JIT_PARAM_COPY
};

/** @struct The execution state of a jitted query. */
struct JitExecState {
    /** @var Last expression evaluation is-null flag. */
    uint32_t m_exprIsNull;

    /** @var Last expression evaluation collation id. */
    uint32_t m_exprCollationId;

    /** @var The outer context from which this query or stored procedure was invoked. */
    MotJitContext* m_invokingContext;

    /*---------------------- LLVM exception handling execution state -------------------*/
    /** @var The current exception status (non-zero value denotes an exception is currently being handled). */
    uint64_t m_exceptionStatus;

    /** @var The current exception value (denotes the thrown value). */
    uint64_t m_exceptionValue;

    /** @var The run-time fault code.. */
    uint64_t m_faultCode;

    /** @var The jump buffer used to generate run-time fault and halt execution. */
    sigjmp_buf m_faultBuf;

    /*---------------------- Error Reporting -------------------*/
    /** @var Error reporting information. The NULL violation column identifier. */
    MOT::Table* m_nullViolationTable;

    /** @var Error message thrown from this function into the calling function. */
    Datum m_errorMessage;

    /** @var Error detail thrown from this function into the calling function. */
    Datum m_errorDetail;

    /** @var Error hint thrown from this function into the calling function. */
    Datum m_errorHint;

    /** @var SQL state thrown from this function into the calling function (string form). */
    Datum m_sqlStateString;

    /** @var SQL state thrown from this function into the calling function. */
    int m_sqlState;

    /** @var Error reporting information. The NULL violation column identifier. */
    int m_nullColumnId;

    /*---------------------- Maintenance -----------------------------*/
    /** @var Specifies whether the index objects in this execution state were reset to null after TRUNCATE TABLE. */
    volatile bool m_purged;

    /*---------------------- Debug execution state -------------------*/
#ifdef MOT_JIT_DEBUG
    /** @var The number of times this context was invoked for execution. */
    uint64_t m_execCount;
#endif
};

/** @enum JIT context type constants. */
enum class JitContextType : uint8_t {
    /** @var Invalid JIT context type. */
    JIT_CONTEXT_TYPE_INVALID,

    /** @var Query JIT context type. */
    JIT_CONTEXT_TYPE_QUERY,

    /** @var Stored Procedure JIT context type. */
    JIT_CONTEXT_TYPE_FUNCTION
};

inline const char* JitContextTypeToString(JitContextType contextType)
{
    switch (contextType) {
        case JitContextType::JIT_CONTEXT_TYPE_QUERY:
            return "Query";
        case JitContextType::JIT_CONTEXT_TYPE_FUNCTION:
            return "Function";
        case JitContextType::JIT_CONTEXT_TYPE_INVALID:
        default:
            return "Invalid";
    }
}

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

/** @struct Params for non-native sort SELECT range queries. */
struct JitNonNativeSortParams {
    /** @var Plan node id. Always set to 0 */
    int plan_node_id;

    /** @var Number of sort-key columns. */
    int numCols;

    /** @var Indexes of columns in the target list. */
    AttrNumber* sortColIdx;

    /** @var OIDs of operators to sort them by. */
    Oid* sortOperators;

    /** @var OIDs of collations. */
    Oid* collations;

    /** @var NULLS FIRST/LAST directions. */
    bool* nullsFirst;

    /** @var If bounded (bound != 0), how many tuples are needed. */
    int64 bound;

    /** @var Current scan direction. */
    ScanDirection scanDir;
};

/** @struct The context for executing a jitted function. */
struct MotJitContext {
    /*---------------------- Meta-data -------------------*/
    /** @var The LLVM code generator for this query. */
    dorado::GsCodeGen* m_codeGen;

    /** @var The LLVM jit-compiled code for executing a query. */
    JitQueryFunc m_llvmFunction;

    /** @var The LLVM jit-compiled code for executing a stored procedure. */
    JitSPFunc m_llvmSPFunction;

    /** @var The type of the executed query (insert/update/select/delete). */
    JitCommandType m_commandType;

    /** @var Specifies whether this is a global or session-local context. */
    JitContextUsage m_usage;

    /** @var Specifies whether this is a query or stored-procedure context. */
    JitContextType m_contextType;

    /** @var Specifies The validity state of the context. */
    volatile uint8_t m_validState;

    /** @var Specified whether this context resides in a @ref JitSource. */
    uint8_t m_isSourceContext;

    /** @var Used for member alignment. */
    uint8_t m_padding[3];

    /** @brief The identifier of the session to which the context object belongs (invalid for global context). */
    MOT::SessionId m_sessionId;

    /** @var The identifier of the pool to which this JIT context belongs. */
    uint32_t m_poolId;

    /** @var The identifier of the sub-pool to which this JIT context belongs. */
    uint16_t m_subPoolId;

    /** @var Used for member alignment. */
    uint8_t m_padding1[2];

    /** @var The identifier of the context within the pool to which this JIT context belongs. */
    uint32_t m_contextId;

    MOT::RC m_rc;

    uint32_t m_paddingRc;

    /** @var The source query string (or function name in case of a stored procedure). */
    const char* m_queryString;

    /** @var The array of constant datum objects used by the jitted function (global copy for all contexts). */
    JitDatumArray m_constDatums;

    /** @var The query execution state. */
    JitExecState* m_execState;

    /*---------------------- Cleanup -------------------*/
    /** @var Chain all JIT contexts in the same thread for cleanup during session end. */
    struct MotJitContext* m_next;

    /** @var Chain all context objects related to the same source, for cleanup during relation modification. */
    MotJitContext* m_nextInSource;

    /** @var Chain all deprecate context objects related to the same source, for safe delayed cleanup. */
    MotJitContext* m_nextDeprecate;

    /** @var The source JIT context from which this context was cloned (null for source context).*/
    MotJitContext* m_sourceJitContext;

    /** @brief The number of sessions using this source JIT context (zero for session contexts). */
    volatile uint64_t m_useCount;

    /** @var The JIT source from which this context originated. */
    JitSource* m_jitSource;

    /** @var The parent of this context (valid only if this is a sub-context). */
    MotJitContext* m_parentContext;
};

/** @class A simple linked list of JIT context objects. */
class JitContextList {
public:
    /** @brief Default constructor. */
    JitContextList() : m_head(nullptr), m_tail(nullptr)
    {}

    /** @brief Default destructor. */
    ~JitContextList()
    {
        m_head = nullptr;
        m_tail = nullptr;
    }

    /** @brief Pushes an element on back of list. */
    inline void PushBack(MotJitContext* jitContext)
    {
        if (m_tail == nullptr) {
            m_head = m_tail = jitContext;
        } else {
            m_tail->m_next = jitContext;
            m_tail = jitContext;
        }
        jitContext->m_next = nullptr;
    }

    /** @brief Starts iterating over list. */
    inline MotJitContext* Begin()
    {
        return m_head;
    }

    /** @brief Queries whether list is empty. */
    inline bool Empty() const
    {
        return (m_head == nullptr);
    }

private:
    /** @var The list head element. */
    MotJitContext* m_head;

    /** @var The list tail element. */
    MotJitContext* m_tail;
};

/*---------------------- Compound Query Context -------------------*/
/** @struct Sub-query context. */
struct JitSubQueryContext {
    /** @var The sub-query command type (point-select, aggregate range select or limit 1 range-select). */
    JitCommandType m_commandType;

    /** @var Align next member to 8-byte offset. */
    uint8_t m_padding[7];

    /** @var The table used by the sub-query. */
    MOT::Table* m_table;

    /** @var The table identifier for re-fetching it after TRUNCATE TABLE. */
    uint64_t m_tableId;

    /** @var The index used by the sub-query. */
    MOT::Index* m_index;

    /** @var The index identifier for re-fetching it after TRUNCATE TABLE. */
    uint64_t m_indexId;
};

/** @struct Parameter information. */
struct JitParamInfo {
    /** @var Specified parameter passing mode (direct or copy). */
    JitParamMode m_mode;

    /** @var Specifies parameter index for direct parameter. */
    int m_index;
};

/** @struct The context for executing a jitted query. */
struct JitQueryContext : public MotJitContext {
    /** @var The table on which the query is to be executed. */
    MOT::Table* m_table;

    /** @var The table identifier for re-fetching it after TRUNCATE TABLE. */
    uint64_t m_tableId;

    /** @var The index on which the query is to be executed (may vary after TRUNCATE TABLE). */
    MOT::Index* m_index;

    /** @var The index identifier for re-fetching it after TRUNCATE TABLE. */
    uint64_t m_indexId;

    /** @var The table used for inner scan in JOIN queries. */
    MOT::Table* m_innerTable;

    /** @var The table identifier for re-fetching it after TRUNCATE TABLE. */
    uint64_t m_innerTableId;

    /** @var The index used for inner scan in JOIN queries (may vary after TRUNCATE TABLE). */
    MOT::Index* m_innerIndex;

    /** @var The index identifier for re-fetching it after TRUNCATE TABLE. */
    uint64_t m_innerIndexId;

    /** @var Number of aggregators in query. */
    uint64_t m_aggCount;

    /** @var Array of tuple descriptors for each sub-query. */
    JitSubQueryContext* m_subQueryContext;

    /** @var The number of sub-queries used. */
    uint32_t m_subQueryCount;

    /** @var Number of parameters passed to invoke the stored procedure (includes default parameters). */
    uint32_t m_invokeParamCount;

    /** @var Parameter access array denoting how parameters are passed to an invoked stored procedure. */
    JitParamInfo* m_invokeParamInfo;

    /** @var The context of the invoked stored procedure. */
    JitFunctionContext* m_invokeContext;

    /** @var The query string for the unjittable invoked function. */
    char* m_invokedQueryString;

    /** @var The invoked function defining transaction identifier in case of unjittable invoked function. */
    TransactionId m_invokedFunctionTxnId;

    /** @var The invoked function identifier in case of unjittable invoked function. */
    Oid m_invokedFunctionOid;

    /** @var Align next member to 8 bytes. */
    uint32_t m_paddingQC;

    /** @var Non-native sort params */
    JitNonNativeSortParams* m_nonNativeSortParams;
};

/** @enum Kind of parameter being passed to SP sub-query*/
enum class JitCallParamKind : uint32_t {
    /** @var Denotes the parameter being passed is an SP argument. */
    JIT_CALL_PARAM_ARG,

    /** @var Denotes the parameter being passed is an SP local variable. */
    JIT_CALL_PARAM_LOCAL,
};

/** @struct Contains information about calling a query from within a stored procedure. */
struct JitCallParamInfo {
    /** @brief The index of the parameter in the parameter array passed to the sub-query*/
    int m_paramIndex;

    /** @brief The parameter type. */
    Oid m_paramType;

    /** @brief The kind of parameter being passed to the SP sub-query. */
    JitCallParamKind m_paramKind;

    /** @brief The index of the parameter in the this SP (-1 if none). */
    int m_invokeArgIndex;

    /** @brief The index of the passed local variable in the datum array of this SP (-1 if none). */
    int m_invokeDatumIndex;
};

/** @struct The call-site used for invoking a jitted-query from a jitted stored procedure. */
struct JitCallSite {
    /** @ var The JIT context for executing the called query. */
    MotJitContext* m_queryContext;

    /** @var Holds the query string for non-jittable sub-queries. */
    char* m_queryString;

    /** @var Holds the command type for non-jittable sub-queries. */
    CmdType m_queryCmdType;

    /** @var Number of parameters used in calling the query from within the SP (includes SP local variables). */
    int m_callParamCount;

    /**  @var Information requires to form the parameter list used to invoke the query. */
    JitCallParamInfo* m_callParamInfo;

    /** @var The global tuple descriptor used to prepare tuple table slot object for the sub-query result. */
    TupleDesc m_tupDesc;

    /** @var The parsed expression index according to visitation order during JIT planning. */
    int m_exprIndex;

    /** @var Denotes whether this is an unjittable invoke query. */
    uint8_t m_isUnjittableInvoke;

    /** @var Denotes whether this is a modifying statement. */
    uint8_t m_isModStmt;

    /** @var Denotes whether the query result should be put into local variable. */
    uint8_t m_isInto;

    /** @var Align structure size to multiple of 8 bytes. */
    uint8_t m_padding;
};

/** @struct The context for executing a jitted stored procedure. */
struct JitFunctionContext : public MotJitContext {
    /** @var The unique identifier of the function in pg_proc. */
    Oid m_functionOid;

    /** @var The number of arguments expected by the function (including default parameters). */
    uint32_t m_SPArgCount;

    /** @var The id of the transaction that created the compiled function. */
    TransactionId m_functionTxnId;

    /** @var The list of parameter types passed to each call site. */
    Oid* m_paramTypes;

    /** @var The number of parameters passed to each call site. */
    uint32_t m_paramCount;

    /** @var Align next member to 8 bytes. */
    uint8_t m_paddingFC[4];

    /** @var Stored procedure call site array for all queries called by the stored procedures. */
    JitCallSite* m_SPSubQueryList;

    /** @var The number of queries called by the stored procedure. */
    uint32_t m_SPSubQueryCount;

    /** @var Specifies the function returns a composite result (record). */
    uint32_t m_compositeResult;

    /** @var The function result tuple descriptor (for returning composite type). */
    TupleDesc m_resultTupDesc;

    /** @var The result row tuple descriptor (for returning composite type). */
    TupleDesc m_rowTupDesc;
};

/** @struct Direct row access execution state. */
struct JitDirectRowAccessExecState {
    /** @var Specifies whether the current row is being directly accessed. */
    int m_isRowDirect;

    /** @var Specifies whether the direct row was deleted. */
    int m_rowDeleted;

    /** @var The cached values of the row columns. */
    Datum* m_rowColumns;

    /** @var The cached values of the row column nulls. */
    bool* m_rowColumnNulls;
};

/** @struct Sub-query execution state. */
struct JitSubQueryExecState {
    /** @var Tuple descriptor for the sub-query result. */
    TupleDesc m_tupleDesc;

    /** @var Sub-query execution result. */
    TupleTableSlot* m_slot;

    /** @var The search key used by the sub-query. */
    MOT::Key* m_searchKey;

    /** @var The search key used by a range select sub-query (whether aggregated or limited). */
    MOT::Key* m_endIteratorKey;
};

/** @struct Aggregator execution state. */
struct JitAggExecState {
    /** @var Aggregated value used for SUM/MAX/MIN/COUNT() operators. */
    Datum m_aggValue;

    /** @var Aggregated value used for SUM/MAX/MIN/COUNT() operators. */
    uint64_t m_aggValueIsNull;

    /** @var Aggregated array used for AVG() operators. */
    Datum m_avgArray;

    /** @var A set of distinct items. */
    void* m_distinctSet;
};

/** @struct The execution state of a jitted query. */
struct JitQueryExecState : public JitExecState {
    /*---------------------- Point Query Execution -------------------*/
    /**
     * @var The key object used to search a row for update/select/delete. This object can be reused
     * in each query.
     */
    MOT::Key* m_searchKey;

    /** @var bitmap set used for incremental redo (only during update) */
    MOT::BitmapSet* m_bitmapSet;

    /**
     * @var The key object used to set the end iterator for range scans. This object can be reused
     * in each query.
     */
    MOT::Key* m_endIteratorKey;

    /*---------------------- Range Scan execution state -------------------*/
    /** @var Begin iterator for range select (stateful execution). */
    MOT::IndexIterator* m_beginIterator;

    /** @var End iterator for range select (stateful execution). */
    MOT::IndexIterator* m_endIterator;

    /** @var Row for range select (stateful execution). */
    MOT::Row* m_row;

    /** @var Stateful counter for limited range scans. */
    uint32_t m_limitCounter;

    /** @var Scan ended flag (stateful execution). */
    uint32_t m_scanEnded;

    /** @var Aggregates execution state. */
    JitAggExecState* m_aggExecState;

    /** @var Non native sort execution state. */
    JitNonNativeSortExecState* m_nonNativeSortExecState;

    /*---------------------- JOIN execution state -------------------*/
    /** @var The key object used to search row or iterator in the inner scan in JOIN queries. */
    MOT::Key* m_innerSearchKey;

    /** @var The key object used to search end iterator in the inner scan in JOIN queries. */
    MOT::Key* m_innerEndIteratorKey;

    /** @var Begin iterator for inner scan in JOIN queries (stateful execution). */
    MOT::IndexIterator* m_innerBeginIterator;

    /** @var End iterator for inner scan in JOIN queries (stateful execution). */
    MOT::IndexIterator* m_innerEndIterator;

    /** @var Row for inner scan in JOIN queries (stateful execution). */
    MOT::Row* m_innerRow;

    /** @var Copy of the outer row (SILO overrides it in the inner scan). */
    MOT::Row* m_outerRowCopy;

    /** @var Scan ended flag (stateful execution). */
    uint64_t m_innerScanEnded;

    /** @var Array of tuple descriptors for each sub-query. */
    JitSubQueryExecState* m_subQueryExecState;

    /** @var The number of sub-queries used. */
    uint64_t m_subQueryCount;

    /*---------------------- Store Procedure Invocation Execution -------------------*/
    /** @var Parameter array list used for invoking the stored procedure. */
    ParamListInfo m_invokeParams;

    /** @var Parameter array list passed directly from invoke context into the invoked stored procedure. */
    ParamListInfo m_directParams;

    /** @var Used for holding lock on invoke SP record during SP execution. */
    HeapTuple m_invokeTuple;

    /** @var The result slot used for invoking the stored procedure. */
    TupleTableSlot* m_invokeSlot;

    /** @var The result number of tuples processed used for invoking the stored procedure. */
    uint64_t* m_invokeTuplesProcessed;

    /** @var The result scan ended flag used for invoking the stored procedure. */
    int* m_invokeScanEnded;

    /*---------------------- Batch Execution -------------------*/
    /**
     * @var The number of times (iterations) a single stateful query was invoked. Used for distinguishing query
     * boundaries in a stateful query execution when client uses batches.
     */
    uint64_t m_iterCount;

    /** @var The number of full query executions. */
    uint64_t m_queryCount;
};

/** @struct Data required for the execution of invoked queries. */
struct JitInvokedQueryExecState {
    /** @var The result slot of the query (volatile working copy). */
    TupleTableSlot* m_workSlot;

    /**
     * @var Parameter array to invoke the query. Only parameters used by the sub-query are filled in before invoking
     * the sub-query.
     */
    ParamListInfo m_params;

    /** @var number of tuples processed by query. */
    uint64_t m_tuplesProcessed;

    /** @var Specifies whether scan ended for invoked query. */
    int m_scanEnded;

    /** @var The SPI result of executing the non-jittable sub-query. */
    int m_spiResult;

    /** @var The result slot of the query. */
    TupleTableSlot* m_resultSlot;

    /** @var SPI plan pointer for jittable sub-query locks and non-jittable sub-query execution. */
    SPIPlanPtr m_plan;

    /** @var Expression stub for non-jittable sub-query revalidation. */
    PLpgSQL_expr* m_expr;

    /** @var Array for non-jittable sub-query execution. */
    PLpgSQL_type* m_resultTypes;
};

/**
 * @struct The execution state of a jitted stored procedure.
 * @note The execution state of a sub-query invoked from a stored procedure is NOT maintained in the function execution
 * state, but rather in the execution state of each sub-query, that is pointed by its containing call site.
 */
struct JitFunctionExecState : public JitExecState {
    /** @var The sub-query execution state array. */
    JitInvokedQueryExecState* m_invokedQueryExecState;

    /** @var The parameters with which the function was invoked. */
    ParamListInfo m_functionParams;

    /** @var The compiled PG function. */
    PLpgSQL_function* m_function;

    /**
     * @var Execution state stub required for re-parse during sub-query revalidation (both jittble and non-jittable
     *  sub-query).
     */
    PLpgSQL_execstate m_estate;

    /** @var The number of active open sub-transactions (each one is opened for each sub-statement-block). */
    uint32_t m_openSubTxCount;

    /** @var The origin of the currently thrown exception. */
    int m_exceptionOrigin;

    /** @var The currently being executed sub-query id. */
    int m_currentSubQueryId;

    /** @var The current block nesting level for SPI connection management. */
    int m_spiBlockId;

    /** @var Connection identifier of SPI for this function execution. */
    int m_spiConnectId[MOT_JIT_MAX_BLOCK_DEPTH];

    /** @var Saved memory context objects for each entered nested sub-transaction. */
    MemoryContext m_savedContexts[MOT_JIT_MAX_BLOCK_DEPTH];

    /** @var The identifier of each sub-transaction in a nested block with exception. */
    SubTransactionId m_subTxns[MOT_JIT_MAX_BLOCK_DEPTH];

    /*---------------------- LLVM exception handling execution state -------------------*/
    /** @struct Single exception frame in the run-time exception stack for LLVM. */
    struct ExceptionFrame {
        /** @var The jump buffer for the frame. */
        sigjmp_buf m_jmpBuf;

        /** @var The frame index (for debugging). */
        uint64_t m_frameIndex;

        /** @var The next exception frame. */
        ExceptionFrame* m_next;
    };

    /** @var The run-time exception stack used to implement simple exceptions in LLVM. */
    ExceptionFrame* m_exceptionStack;
};

/** @struct The execution state for non-native sort query. */
struct JitNonNativeSortExecState {
    /** @var Tuple sort state for non-native sort. */
    Tuplesortstate* m_tupleSort;
};

inline bool IsJitSubContextInline(MotJitContext* jitContext)
{
    return (jitContext->m_parentContext != nullptr);
}

inline bool IsInvokeQueryContext(MotJitContext* jitContext)
{
    return ((jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) &&
            (jitContext->m_commandType == JIT_COMMAND_INVOKE));
}

inline bool IsSimpleQueryContext(MotJitContext* jitContext)
{
    return ((jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) &&
            (jitContext->m_commandType != JIT_COMMAND_INVOKE));
}

/**
 * @brief Allocate JitNonNativeSortParams struct with all members.
 * @param numCols Number of column that should be placed in order.
 * @param usage Specify the usage scope of the context (global or session-local).
 * @return Pointer to fully allocated JitNonNativeSortParams structure or NULL for failure.
 */
extern JitNonNativeSortParams* AllocJitNonNativeSortParams(int numCols, JitContextUsage usage);

/**
 * @brief Destroy Non native sort params structure.
 * @param sortParams The non native sort params structure to destroy.
 * @param usage Specify the usage scope of the cloned context (global or session-local).
 */
extern void DestroyJitNonNativeSortParams(JitNonNativeSortParams* sortParam, JitContextUsage usage);

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
 * @param type Specifies the context type (query or stored procedure).
 * @return The allocated JIT context, or NULL if allocation failed.
 */
extern MotJitContext* AllocJitContext(JitContextUsage usage, JitContextType type);

/**
 * @brief Clones a JIT context.
 * @param sourceJitContext The context to clone.
 * @param usage Specify the usage scope of the cloned context (global or session-local).
 * @return The cloned context or NULL if failed.
 */
extern MotJitContext* CloneJitContext(MotJitContext* sourceJitContext, JitContextUsage usage);

/**
 * @brief Create JIT sort exec state from JitNonNativeSortParams structure.
 * @param src The JitNonNativeSortParams to clone.
 * @param usage Specify the usage scope of the cloned context (global or session-local).
 * @return The cloned JitSortExecState or NULL if failed.
 */
extern JitNonNativeSortParams* CloneJitNonNativeSortParams(JitNonNativeSortParams* src, JitContextUsage usage);

/**
 * @brief Re-fetches all table and index objects in a JIT context.
 * @param jitContext The JIT context for which table and index objects should be re-fetched.
 * @return True if the operation succeeded, otherwise false.
 */
extern bool RefetchTablesAndIndices(MotJitContext* jitContext);

/**
 * @brief Prepares a JIT context object for execution. All internal members are allocated and
 * initialized on-demand, according to the SQL command being executed.
 * @param jitContext The JIT context to prepare.
 * @return True if succeeded, otherwise false. Consult @ref mm_get_root_error() for further details.
 */
extern bool PrepareJitContext(MotJitContext* jitContext);

/** @brief Queries whether a JIT context refers to a relation. */
extern bool JitContextRefersRelation(MotJitContext* jitContext, uint64_t relationId);

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
extern void PurgeJitContext(MotJitContext* jitContext, uint64_t relationId);

/** @brief Marks a JIT context as deprecate. */
extern void DeprecateJitContext(MotJitContext* jitContext);

/** @brief Marks recursively (from bottom to top) a JIT context as invalid. */
extern void InvalidateJitContext(MotJitContext* jitContext, uint64_t relationId, uint8_t invalidFlag = 0);

/** @brief Marks a JIT context as deprecate. */
extern void DeprecateJitContext(MotJitContext* jitContext);

/** @brief Trigger code-generation of any missing sub-query. */
extern bool RevalidateJitContext(MotJitContext* jitContext, TransactionId functionTxnId = InvalidTransactionId);

/** @brief Converts valid state flags to string. */
extern const char* JitContextValidStateToString(uint8_t validState);

/** @brief Mark context as done waiting for source compilation to finish (result is success). */
extern void MarkJitContextDoneCompile(MotJitContext* jitContext);

/** @brief Mark context as done waiting for source compilation to finish (result is error). */
extern void MarkJitContextErrorCompile(MotJitContext* jitContext);

/** @brief Set all context compile flags to zero. */
extern void ResetJitContextCompileState(MotJitContext* jitContext);

/** @brief Resets all error management members. */
extern void ResetErrorState(JitExecState* execState);
}  // namespace JitExec

#endif
