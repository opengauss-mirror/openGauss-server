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
 * jit_common.h
 *    Definitions used by LLVM jitted code.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_common.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_COMMON_H
#define JIT_COMMON_H

#include "jit_helpers.h"
#include "jit_context.h"

#include "mot_engine.h"

#include "utils/plpgsql.h"
#include "funcapi.h"

/** @define The maximum number of constant objects that can be used in a query. */
#define MOT_JIT_MAX_CONST 1024

/** @define Start profile region text. */
#define MOT_JIT_PROFILE_BEGIN_MESSAGE "MOT JIT PROFILE BEGIN"

/** @define End profile region text. */
#define MOT_JIT_PROFILE_END_MESSAGE "MOT JIT PROFILE END"

/*----------------------------- Common Fault Codes -----------------------------------*/
/** @define JIT codes low bound. */
#define JIT_FAULT_CODE_LOW 100

/** @define Internal error (probable software bug) during jitted function execution. */
#define JIT_FAULT_INTERNAL_ERROR (JIT_FAULT_CODE_LOW + 0)

/** @define Sub-transaction not closed fault. */
#define JIT_FAULT_SUB_TX_NOT_CLOSED (JIT_FAULT_CODE_LOW + 1)

/*----------------------------- Exception Oriign -----------------------------------*/
/** @define Exception origin is internal. */
#define JIT_EXCEPTION_INTERNAL 0

/** @define Exception origin is external. */
#define JIT_EXCEPTION_EXTERNAL 1

// This file contains common definitions used by LLVM jitted code
namespace JitExec {
using namespace dorado;

// forward declaration
struct JitCompoundPlan;
struct JitCallSitePlan;
struct JitFunctionPlan;

/** @struct JIT code generation statistics. */
struct JitCodegenStats {
    /** @var Total time taken to generate code. */
    uint64_t m_codegenTime;

    /** @var Time taken to verify the jitted function. */
    uint64_t m_verifyTime;

    /** @var Time taken to finalize the jitted function. */
    uint64_t m_finalizeTime;

    /** @var Time taken for the compilation (applicable only for LLVM). */
    uint64_t m_compileTime;
};

/** @brief Queries whether a fault code is a generic JIT fault code. */
inline bool IsJitRuntimeFaultCode(int faultCode)
{
    return (faultCode >= JIT_FAULT_CODE_LOW);
}

/** @brief Converts JIT generic fault code to string. */
extern const char* JitRuntimeFaultToString(int faultCode);

// common helpers for code generation
/** @brief Converts an LLVM command type to string form. */
extern const char* CommandToString(JitCommandType commandType);

/** @breif Queries whether an LLVM command requires a range scan. */
extern bool IsRangeCommand(JitCommandType commandType);

/** @breif Queries whether an LLVM command requires a JOIN scan. */
extern bool IsJoinCommand(JitCommandType commandType);

/** @breif Queries whether a JIT command is a SELECT command (includes JOIN). */
extern bool IsSelectCommand(JitCommandType commandType);

/** @breif Queries whether an LLVM command uses index objects. */
extern bool IsCommandUsingIndex(JitCommandType commandType);

/** @breif Queries whether a JIT command is a write operation command. */
extern bool IsWriteCommand(JitCommandType commandType);

/** @brief Extract table from query. */
extern MOT::Table* GetTableFromQuery(const Query* query);

/** @brief Maps a table column id to an index column id. */
extern int MapTableColumnToIndex(MOT::Table* table, const MOT::Index* index, int columnId);

/** @brief builds a map from table column ids to index column ids. */
extern int BuildColumnMap(MOT::Table* table, MOT::Index* index, int** columnMap);

/** @brief Builds an array of index column offsets. */
extern int BuildIndexColumnOffsets(MOT::Table* table, const MOT::Index* index, int** offsets);

/** @brief Queries whether a PG type is supported by MOT tables. */
extern bool IsTypeSupported(int resultType);

/** @brief Queries whether a PG type represents a string. */
extern bool IsStringType(int type);

/** @brief Queries whether a PG type represents a primitive type. */
extern bool IsPrimitiveType(int type);

/** @brief Queries whether a WHERE clause operator is supported. */
extern bool IsWhereOperatorSupported(int whereOp);

/** @brief Classifies a WHERE clause operator. */
extern JitWhereOperatorClass ClassifyWhereOperator(int whereOp);

/** @brief Queries whether a PG function is supported by MOT JIT. */
extern bool IsFuncIdSupported(int funcId);

/** @brief Queries whether the WHERE clause represents a full-prefix search. */
extern bool IsFullPrefixSearch(const int* columnArray, int columnCount, int* firstZeroColumn);

/** @brief Classifies operator sorting. */
extern JitQuerySortOrder ClassifyOperatorSortOrder(Oid op);

/** @brief Retrieves the sort order of a query if specified. */
extern JitQuerySortOrder GetQuerySortOrder(const Query* query);

/** @brief Retrieves the type of the aggregate column. */
extern int GetAggregateTupleColumnIdAndType(const Query* query, int& zeroType);

inline void* JitMemAlloc(uint64_t size, JitContextUsage usage)
{
    if (IsJitContextUsageGlobal(usage)) {
        return MOT::MemGlobalAlloc(size);
    } else {
        return MOT::MemSessionAlloc(size);
    }
}

inline void JitMemFree(void* object, JitContextUsage usage)
{
    MOT_ASSERT(object != nullptr);
    if (IsJitContextUsageGlobal(usage)) {
        MOT::MemGlobalFree(object);
    } else {
        MOT::MemSessionFree(object);
    }
}

inline void* JitMemRealloc(void* object, uint64_t newSize, MOT::MemReallocFlags flags, JitContextUsage usage)
{
    if (IsJitContextUsageGlobal(usage)) {
        return MOT::MemGlobalRealloc(object, newSize, flags);
    } else {
        return MOT::MemSessionRealloc(object, newSize, flags);
    }
}

inline void* JitMemAllocAligned(uint64_t size, uint32_t alignment, JitContextUsage usage)
{
    if (IsJitContextUsageGlobal(usage)) {
        return MOT::MemGlobalAllocAligned(size, alignment);
    } else {
        return MOT::MemSessionAllocAligned(size, alignment);
    }
}

/** @struct Compile-time table information. */
struct TableInfo {
    /** @var The table used for the query. */
    MOT::Table* m_table;

    /** @var The index used for the query. */
    MOT::Index* m_index;

    /** @var Map from table column id to index column id. */
    int* m_columnMap;

    /** @var Offsets of index columns. */
    int* m_indexColumnOffsets;

    /** @var The number of entries in the column map. */
    int m_tableColumnCount;

    /** @var The number of entries in the offset array. */
    int m_indexColumnCount;
};

/**
 * @brief Initializes compile-time table information structure.
 * @param table_info The table information structure.
 * @param table The table to be examined.
 * @param index The index to be examined.
 * @return True if succeeded, otherwise false.
 */
extern bool InitTableInfo(TableInfo* table_info, MOT::Table* table, MOT::Index* index);

/**
 * @brief Cleanup table information.
 * @param table_info The table information structure to cleanup.
 */
extern void DestroyTableInfo(TableInfo* tableInfo);

/**
 * @brief Prepares sub-query data items for a JIT context (table, index and tuple descriptor).
 * @param jitContext The JIT context to prepare.
 * @param plan The compound plan for the query.
 * @return True if operations succeeded, otherwise false.
 */
extern bool PrepareSubQueryData(JitQueryContext* jitContext, JitCompoundPlan* plan);

/** @brief Extracts the function name from a parse tree of function invocation command. */
extern const char* ExtractFunctionName(Node* parseTree);

/** @brief Prepares array of global datum objects from array of constants. */
extern bool PrepareDatumArray(Const* constArray, uint32_t constCount, JitDatumArray* datumArray);

/** @brief Clones an interval datum into global memory. */
extern bool CloneDatum(Datum source, int type, Datum* target, JitContextUsage usage);

/** @brief Prints a datum to log. */
extern void PrintDatum(MOT::LogLevel logLevel, Oid ptype, Datum datum, bool isnull);

/** @brief Convert numeric Datum to double precision value. */
extern double NumericToDouble(Datum numeric_value);

/** @struct Attributes of a parsed query in a PG/PLSQL expression. */
struct ExprQueryAttrs {
    /** @brief The SPI plan for the query. */
    SPIPlanPtr m_spiPlan;

    /** @brief The cached plan source for the query. */
    CachedPlanSource* m_planSource;

    /** @brief The parsed query tree. */
    Query* m_query;

    /** @brief Bitmap for specifying which parameter of the ones used to parse the query are actually used. */
    Bitmapset* m_paramNos;
};

/** @brief Prepares fake execution state for parsing. */
extern void PrepareExecState(PLpgSQL_execstate* estate, PLpgSQL_function* function);

/** @brief Extract plan and query from expression (requires fake execution state. */
extern bool GetExprQueryAttrs(PLpgSQL_expr* expr, PLpgSQL_function* function, ExprQueryAttrs* attrs);

/** @brief Releases the resources associated with expression query attributes. */
extern void CleanupExprQueryAttrs(ExprQueryAttrs* attrs);

/** @brief Creates a parameter array with the given amount of parameter entries. */
extern ParamListInfo CreateParamListInfo(int paramCount, bool isGlobalUsage);

/** @brief Retrieves Compiled stored procedure by id. */
extern PLpgSQL_function* GetPGCompiledFunction(Oid functionOid, const char* funcName = nullptr);

/** @convert SQL value to string. */
extern void SqlStateToCode(int sqlState, char* sqlStateCode);

extern void RaiseEreport(int sqlState, const char* errorMessage, const char* errorDetail, const char* errorHint);

/** @brief Duplicates a string. */
extern char* DupString(const char* source, JitContextUsage usage);

/** @class Utility class for managing a short-lived SPI connection. */
class SPIAutoConnect {
public:
    explicit SPIAutoConnect(bool deferConnect = false, Oid functionOid = InvalidOid)
        : m_rc(0), m_connected(false), m_connectId(-1), m_shouldDisconnect(false)
    {
        if (!deferConnect) {
            (void)Connect(functionOid);
        }
    }

    ~SPIAutoConnect()
    {
        Disconnect();
    }

    bool Connect(Oid functionOid = InvalidOid);

    inline bool IsConnected() const
    {
        return m_connected;
    }

    inline int GetErrorCode() const
    {
        return m_rc;
    }

    void Disconnect();

private:
    int m_rc;
    bool m_connected;
    int m_connectId;
    bool m_shouldDisconnect;
    enum class ErrorOp { CONNECT, DISCONNECT };

    void HandleError(ErrorOp errorOp);
    const char* ErrorOpToString(ErrorOp errorOp);
};

/** @struct PG function information. */
struct PGFunctionInfo {
    /** @var The function identifier. */
    Oid m_functionId;

    /** @var The function pointer. */
    PGFunction m_funciton;

    /** @var The number of arguments required by the function. */
    uint32_t m_args;
};

/**
 * @brief Retrieves PG function information.
 * @param functionId The function identifier.
 * @param[out] argCount The number of arguments used by the function.
 * @param[out, opt] Specifies whether the function is strict.
 * @return The function, or null if none was found.
 */
extern PGFunction GetPGFunctionInfo(Oid functionId, uint32_t* argCount, bool* isStrict = nullptr);

/**
 * @brief Retrieves PG aggregation function information.
 * @param functionId The function identifier.
 * @param[out] argCount The number of arguments used by the function.
 * @param[out, opt] isStrict Specifies whether the function is strict.
 * @return The aggregate function, or null if none was found.
 */
extern PGFunction GetPGAggFunctionInfo(Oid functionId, uint32_t* argCount, bool* isStrict = nullptr);

/**
 * @brief Retrieves PG aggregation transition function information.
 * @param functionId The function identifier.
 * @param[out] aggFunc The aggregation transition function (could be null, which is OK, check result in this case).
 * @param[out] argCount The number of arguments used by the function.
 * @param[out, opt] isStrict Specifies whether the function is strict.
 * @return True if operation succeeded, otherwise false.
 * @note It is possible that no transition function is defined for this aggregate. In this case the return value is
 * true, and the @ref aggFunc output parameter is set to null. In case of failure, @ref aggFunc is set to null, and
 * false is returned.
 */
extern bool GetPGAggTransFunctionInfo(
    Oid functionId, PGFunction* aggFunc, uint32_t* argCount, bool* isStrict = nullptr);

/**
 * @brief Retrieves PG aggregation finalization function information.
 * @param functionId The function identifier.
 * @param[out] aggFunc The aggregation finalization function (could be null, which is OK, check result in this case).
 * @param[out] argCount The number of arguments used by the function.
 * @param[out, opt] isStrict Specifies whether the function is strict.
 * @return True if operation succeeded, otherwise false.
 * @note It is possible that no finalization function is defined for this aggregate. In this case the return value is
 * true, and the @ref aggFunc output parameter is set to null. In case of failure, @ref aggFunc is set to null, and
 * false is returned.
 */
extern bool GetPGAggFinalFunctionInfo(
    Oid functionId, PGFunction* aggFunc, uint32_t* argCount, bool* isStrict = nullptr);

/** @brief Generates code from a call site plan. */
extern bool ProcessCallSitePlan(JitCallSitePlan* callSitePlan, JitCallSite* callSite, PLpgSQL_function* function);

/** @brief Generates code for an invoked stored procedure. */
extern MotJitContext* ProcessInvokedPlan(JitFunctionPlan* functionPlan, JitContextUsage usage);

/** @brief Common helper in code-generation. */
extern bool GetFuncTypeClass(JitFunctionPlan* functionPlan, TupleDesc* resultTupDesc, TypeFuncClass* typeClass);

/** @brief Prepare invoked stored procedure query string. */
extern char* MakeInvokedQueryString(char* functionName, Oid functionId);

/** @brief Switch memory context to caller's. */
extern MemoryContext SwitchToSPICallerContext();

/** @brief Make datum copy. */
extern Datum CopyDatum(Datum value, Oid type, bool isnull);
}  // namespace JitExec

#ifdef MOT_JIT_DEBUG
#define JIT_PRINT_DATUM(logLevel, msg, ptype, datum, isnull) \
    if (MOT_CHECK_LOG_LEVEL(logLevel)) {                     \
        MOT_LOG_BEGIN(logLevel, "%s: ", msg);                \
        JitExec::PrintDatum(logLevel, ptype, datum, isnull); \
        MOT_LOG_END(logLevel);                               \
    }
#define DEBUG_PRINT_DATUM(msg, ptype, datum, isnull) JIT_PRINT_DATUM(MOT::LogLevel::LL_DEBUG, msg, ptype, datum, isnull)
#else
#define JIT_PRINT_DATUM(logLevel, msg, ptype, datum, isnull)
#define DEBUG_PRINT_DATUM(msg, ptype, datum, isnull)
#endif

#endif
