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
 * jit_helpers.cpp
 *    Helper functions for JIT execution.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_helpers.cpp
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
#include "access/xact.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "parser/parse_coerce.h"
#include "access/tupconvert.h"
#include "opfusion/opfusion.h"
#include "access/tableam.h"

#include "jit_helpers.h"
#include "jit_common.h"
#include "mot_internal.h"
#include "utilities.h"
#include <unordered_set>

#include "storage/mot/mot_fdw.h"
#include "jit_llvm_util.h"
#include "jit_plan_sp.h"
#include "jit_profiler.h"
#include "jit_source.h"
#include "mot_fdw_helpers.h"

DECLARE_LOGGER(LlvmHelpers, JitExec)

struct NumericEquals : public std::binary_function<Datum, Datum, bool> {
    inline bool operator()(const Datum& lhs, const Datum& rhs) const
    {
        Datum res = DirectFunctionCall2(numeric_eq, lhs, rhs);
        return DatumGetBool(res);
    }
};

struct NumericHash : public std::unary_function<Datum, size_t> {
    inline size_t operator()(const Datum& arg) const
    {
        Numeric num = DatumGetNumeric(arg);
        NumericDigit* digits = NUMERIC_DIGITS(num);
        int ndigits = NUMERIC_NDIGITS(num);
        int nweight = NUMERIC_WEIGHT(num);
        int dscale = NUMERIC_DSCALE(num);
        int nsign = NUMERIC_SIGN(num);

        size_t hashValue = 17;
        hashValue = hashValue * 19 + ndigits;
        hashValue = hashValue * 19 + nweight;
        hashValue = hashValue * 19 + dscale;
        hashValue = hashValue * 19 + nsign;
        for (int i = 0; i < ndigits; ++i) {
            hashValue = hashValue * 19 + digits[i];
        }
        return hashValue;
    }
};

struct VarcharEquals : public std::binary_function<Datum, Datum, bool> {
    inline bool operator()(const Datum& lhs, const Datum& rhs) const
    {
        Datum res = DirectFunctionCall2(bpchareq, lhs, rhs);
        return DatumGetBool(res);
    }
};

struct VarcharHash : public std::unary_function<Datum, size_t> {
    inline size_t operator()(const Datum& arg) const
    {
        bytea* text = DatumGetByteaP(arg);
        uint32_t size = VARSIZE(text);  // includes header len VARHDRSZ
        char* src = VARDATA(text);
        uint32_t strSize = size - VARHDRSZ;

        // implement djb2
        size_t hashValue = 5381;
        for (uint32_t i = 0; i < strSize; ++i) {
            size_t c = (size_t)src[i];
            hashValue = ((hashValue << 5) + hashValue) + c;  // hash * 33 + c
        }
        return hashValue;
    }
};

typedef std::unordered_set<int64_t> DistinctIntSetType;
typedef std::unordered_set<double> DistinctDoubleSetType;
using DistinctNumericSetType = std::unordered_set<Datum, NumericHash, NumericEquals>;
using DistinctVarcharSetType = std::unordered_set<Datum, VarcharHash, VarcharEquals>;

/** @brief Helper to prepare a numeric zero. */
static Datum makeNumericZero()
{
    return DirectFunctionCall1(int4_numeric, Int32GetDatum(0));
}

/** @brief Allocates and initializes a distinct set of integers. */
static void* prepareDistinctIntSet()
{
    void* buf = MOT::MemSessionAlloc(sizeof(DistinctIntSetType));
    if (buf) {
        buf = new (buf) DistinctIntSetType();
    }
    return buf;
}

/** @brief Allocates and initializes a distinct set of double-precision values. */
static void* prepareDistinctDoubleSet()
{
    void* buf = MOT::MemSessionAlloc(sizeof(DistinctDoubleSetType));
    if (buf) {
        buf = new (buf) DistinctDoubleSetType();
    }
    return buf;
}

/** @brief Allocates and initializes a distinct set of numeric datum values. */
static void* prepareDistinctNumericSet()
{
    void* buf = MOT::MemSessionAlloc(sizeof(DistinctNumericSetType));
    if (buf) {
        buf = new (buf) DistinctNumericSetType();
    }
    return buf;
}

/** @brief Allocates and initializes a distinct set of varchar datum values. */
static void* prepareDistinctVarcharSet()
{
    void* buf = MOT::MemSessionAlloc(sizeof(DistinctVarcharSetType));
    if (buf) {
        buf = new (buf) DistinctVarcharSetType();
    }
    return buf;
}

/** @brief Inserts an integer to a distinct set of integers. */
static int insertDistinctIntItem(void* distinct_set, int64_t item)
{
    int result = 0;
    DistinctIntSetType* int_set = (DistinctIntSetType*)distinct_set;
    if (int_set->insert(item).second) {
        result = 1;
    }
    return result;
}

/** @brief Inserts a double-precision number to a distinct set of double-precision values. */
static int insertDistinctDoubleItem(void* distinct_set, double item)
{
    int result = 0;
    DistinctDoubleSetType* double_set = (DistinctDoubleSetType*)distinct_set;
    if (double_set->insert(item).second) {
        result = 1;
    }
    return result;
}

/** @brief Inserts a numeric to a distinct set of double-precision values. */
static int insertDistinctNumericItem(void* distinctSet, Datum item)
{
    int result = 0;
    DistinctNumericSetType* numericSet = (DistinctNumericSetType*)distinctSet;
    if (numericSet->insert(item).second) {
        result = 1;
    }
    return result;
}

/** @brief Inserts a varchar to a distinct set of double-precision values. */
static int insertDistinctVarcharItem(void* distinctSet, Datum item)
{
    int result = 0;
    DistinctVarcharSetType* varcharSet = (DistinctVarcharSetType*)distinctSet;
    if (varcharSet->insert(item).second) {
        MOT_LOG_DEBUG("Varchar Item inserted");
        result = 1;
    }
    return result;
}

/** @brief Destroys and frees a distinct set of integers. */
static void destroyDistinctIntSet(void* distinct_set)
{
    DistinctIntSetType* int_set = (DistinctIntSetType*)distinct_set;
    if (int_set) {
        int_set->~DistinctIntSetType();
        MOT::MemSessionFree(distinct_set);
    }
}

/** @brief Destroys and frees a distinct set of double-precision values. */
static void destroyDistinctDoubleSet(void* distinct_set)
{
    DistinctDoubleSetType* double_set = (DistinctDoubleSetType*)distinct_set;
    if (double_set) {
        double_set->~DistinctDoubleSetType();
        MOT::MemSessionFree(distinct_set);
    }
}

/** @brief Destroys and frees a distinct set of double-precision values. */
static void destroyDistinctNumericSet(void* distinctSet)
{
    DistinctNumericSetType* numericSet = (DistinctNumericSetType*)distinctSet;
    if (numericSet) {
        numericSet->~DistinctNumericSetType();
        MOT::MemSessionFree(numericSet);
    }
}

/** @brief Destroys and frees a distinct set of varchar values. */
static void destroyDistinctVarcharSet(void* distinctSet)
{
    DistinctVarcharSetType* varcharSet = (DistinctVarcharSetType*)distinctSet;
    if (varcharSet) {
        varcharSet->~DistinctVarcharSetType();
        MOT::MemSessionFree(varcharSet);
    }
}

/*---------------------------  LLVM Access Helpers ---------------------------*/
extern "C" {
void debugLog(const char* function, const char* msg)
{
    MOT_LOG_DEBUG("%s: %s", function, msg);
}

void debugLogInt(const char* msg, int arg)
{
    MOT_LOG_DEBUG(msg, arg);
}

void debugLogString(const char* msg, const char* arg)
{
    MOT_LOG_DEBUG(msg, arg);
}

void debugLogStringDatum(const char* msg, int64_t arg)
{
    DEBUG_PRINT_DATUM(msg, VARCHAROID, (Datum)arg, false);
}

void debugLogDatum(const char* msg, Datum value, int isNull, int type)
{
    DEBUG_PRINT_DATUM(msg, type, (Datum)value, isNull);
}

static void RaiseLlvmFault(int faultCode)
{
    JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    MOT_ASSERT(jitContext);
    jitContext->m_execState->m_faultCode = (uint64_t)faultCode;
    MOT_LOG_DEBUG("RaiseLlvmFault, jumping to faultBuf at %p on exec state %p: %s",
        (int8_t*)jitContext->m_execState->m_faultBuf,
        jitContext->m_execState,
        jitContext->m_queryString);
    siglongjmp(jitContext->m_execState->m_faultBuf, faultCode);
}

static void RaiseFault(int faultCode)
{
    RaiseLlvmFault(faultCode);
}

inline void RaiseAccessViolationFault()
{
    RaiseLlvmFault(LLVM_FAULT_ACCESS_VIOLATION);
}

inline void RaiseResourceLimitFault()
{
    RaiseLlvmFault(LLVM_FAULT_RESOURCE_LIMIT);
}

inline void ValidatePointer(void* ptr, const char* file, int line, const char* msg)
{
    if (ptr == nullptr) {
        MOT_LOG_ERROR("Invalid pointer accessed at %s, line %d: %s", file, line, msg);
        RaiseAccessViolationFault();
    }
}

inline void ValidateArray(int index, int arraySize, const char* file, int line, const char* msg)
{
    if (index >= arraySize) {
        MOT_LOG_ERROR("Array index %d out of bounds %d at %s, line %d: %s", index, arraySize, file, line, msg);
        RaiseAccessViolationFault();
    }
}

/** @define Helper macro for validating pointer access. */
#define VERIFY_PTR(ptr, msg) ValidatePointer(ptr, __FILE__, __LINE__, msg)

/** @define Helper macro for validating array access. */
#define VERIFY_ARRAY_ACCESS(index, arraySize, msg) ValidateArray(index, arraySize, __FILE__, __LINE__, msg)

/*---------------------------  Engine Access Helpers ---------------------------*/
int isSoftMemoryLimitReached()
{
    int result = 0;
    if (MOT::MOTEngine::GetInstance()->IsSoftMemoryLimitReached()) {
        MOT_LOG_TRACE("Memory limit reached, aborting transaction");
        result = 1;
    }
    return result;
}

MOT::MOTEngine* getMOTEngine()
{
    return MOT::MOTEngine::GetInstance();
}

MOT::Index* getPrimaryIndex(MOT::Table* table)
{
    MOT::Index* index = table->GetPrimaryIndex();
    MOT_LOG_DEBUG("Retrieved primary index %p from table %s @%p", index, table->GetTableName().c_str(), table);
    return index;
}

MOT::Index* getTableIndex(MOT::Table* table, int index_id)
{
    MOT::Index* index = table->GetSecondaryIndex(index_id);
    MOT_LOG_DEBUG("Retrieved index %p [%s]from table %s @%p by id %d",
        index,
        index->GetName().c_str(),
        table->GetTableName().c_str(),
        table,
        index_id);
    return index;
}

void InitKey(MOT::Key* key, MOT::Index* index)
{
    MOT_LOG_DEBUG("Initializing key %p by source index %s %u (%p)",
        key,
        index->GetName().c_str(),
        (unsigned)index->GetExtId(),
        index);
    key->InitKey(index->GetKeyLength());
    MOT_LOG_DEBUG("key %p initialized to %u bytes", key, key->GetKeyLength());
}

MOT::Column* getColumnAt(MOT::Table* table, int table_colid)
{
    MOT_LOG_DEBUG("Retrieving table column %d from table %p", table_colid, table);
    MOT::Column* column = table->GetField(table_colid);
    MOT_LOG_DEBUG("Retrieved table column %p id %d from table %s at %p",
        column,
        table_colid,
        table->GetTableName().c_str(),
        table);
    return column;
}

void SetExprIsNull(int isnull)
{
    MOT_LOG_DEBUG("Setting expression isnull to: %d", isnull);
    u_sess->mot_cxt.jit_context->m_execState->m_exprIsNull = isnull;
}

int GetExprIsNull()
{
    int result = (int)u_sess->mot_cxt.jit_context->m_execState->m_exprIsNull;
    MOT_LOG_DEBUG("Retrieved expression isnull: %d", result);
    return result;
}

void SetExprCollation(int collationId)
{
    MOT_LOG_DEBUG("Setting expression collation to: %d", collationId);
    u_sess->mot_cxt.jit_context->m_execState->m_exprCollationId = collationId;
}

int GetExprCollation()
{
    int result = (int)u_sess->mot_cxt.jit_context->m_execState->m_exprCollationId;
    MOT_LOG_DEBUG("Retrieved expression collation: %d", result);
    return result;
}

#ifdef MOT_JIT_DEBUG
#define GET_EXPR_IS_NULL() GetExprIsNull()
#define SET_EXPR_IS_NULL(isnull) SetExprIsNull(isnull)
#define GET_EXPR_COLLATION() GetExprCollation()
#define SET_EXPR_COLLATION(collation) SetExprCollation(collation)
#else
#define GET_EXPR_IS_NULL() (int)u_sess->mot_cxt.jit_context->m_execState->m_exprIsNull
#define SET_EXPR_IS_NULL(isnull) u_sess->mot_cxt.jit_context->m_execState->m_exprIsNull = (isnull)
#define GET_EXPR_COLLATION() (int)u_sess->mot_cxt.jit_context->m_execState->m_exprCollationId
#define SET_EXPR_COLLATION(collation) u_sess->mot_cxt.jit_context->m_execState->m_exprCollationId = (collation)
#endif

Datum GetConstAt(int constId)
{
    MOT_LOG_DEBUG("Retrieving constant datum by id %d", constId);
    Datum result = PointerGetDatum(nullptr);
    JitExec::MotJitContext* ctx = u_sess->mot_cxt.jit_context;
    if (constId < (int)ctx->m_constDatums.m_datumCount) {
        JitExec::JitDatum* datum = &ctx->m_constDatums.m_datums[constId];
        result = datum->m_datum;
        ctx->m_execState->m_exprIsNull = datum->m_isNull;
        DEBUG_PRINT_DATUM("Retrieved constant datum", datum->m_type, datum->m_datum, datum->m_isNull);
    } else {
        MOT_LOG_ERROR("Invalid constant identifier: %d", constId);
        RaiseAccessViolationFault();
    }
    return result;
}

Datum getDatumParam(ParamListInfo params, int paramid)
{
    MOT_LOG_DEBUG("Retrieving datum param at index %d", paramid);
    DEBUG_PRINT_DATUM(
        "Param value", params->params[paramid].ptype, params->params[paramid].value, params->params[paramid].isnull);
    SET_EXPR_IS_NULL(params->params[paramid].isnull);
    return params->params[paramid].value;
}

Datum readDatumColumn(MOT::Table* table, MOT::Row* row, int colid, int innerRow, int subQueryIndex)
{
    // special case: row is null (can happen with left join)
    if (row == nullptr) {
        MOT_LOG_DEBUG("readDatumColumn(): Row is null (left join?)");
        SET_EXPR_IS_NULL(1);
        return PointerGetDatum(NULL);  // return proper NULL datum
    }

    MOT::Column* column = table->GetField(colid);
    MOT_LOG_DEBUG("Reading Datum value from row %p column %p [%s] in table %p [%s]",
        row,
        column,
        column->m_name,
        table,
        table->GetTableName().c_str());
    Datum result = PointerGetDatum(NULL);  // return proper NULL datum if column value is null

    FormData_pg_attribute attr;
    attr.attnum = colid;
    attr.atttypid = ConvertMotColumnTypeToOid(column->m_type);

    bool isnull = false;
    MOTAdaptor::MOTToDatum(table, &attr, (uint8_t*)row->GetData(), &result, &isnull);
    DEBUG_PRINT_DATUM("Column value", attr.atttypid, result, isnull);
    SET_EXPR_IS_NULL((int)isnull);

    return result;
}

void writeDatumColumn(MOT::Row* row, MOT::Column* column, Datum value)
{
    MOT_LOG_DEBUG("Writing to row %p column %p datum value", row, column);
    DEBUG_PRINT_DATUM("Datum value", ConvertMotColumnTypeToOid(column->m_type), value, 0);
    MOTAdaptor::DatumToMOT(column, value, ConvertMotColumnTypeToOid(column->m_type), (uint8_t*)row->GetData());
}

void buildDatumKey(
    MOT::Column* column, MOT::Key* key, Datum value, int index_colid, int offset, int size, int value_type)
{
    int isnull = GET_EXPR_IS_NULL();
    MOT_LOG_DEBUG("buildKey: writing datum value for index column %d at key buf offset %d (col=%p, key=%p, datum=%p, "
                  "value-type=%d, key-size=%u, col-size=%u, size=%d, is-null: %d)",
        index_colid,
        offset,
        column,
        key,
        value,
        value_type,
        (unsigned)key->GetKeyLength(),
        (unsigned)column->m_size,
        size,
        isnull);
    DEBUG_PRINT_DATUM("Key Datum", value_type, value, 0);
    if (isnull) {
        MOT_LOG_DEBUG("Setting null key datum at offset %d (%d bytes)", offset, size);
        errno_t erc = memset_s(key->GetKeyBuf() + offset, key->GetKeyLength() - offset, 0x00, size);
        securec_check(erc, "\0", "\0");
    } else {
        MOTAdaptor::DatumToMOTKey(column,
            (Oid)value_type,
            value,
            (Oid)column->m_envelopeType,
            key->GetKeyBuf() + offset,
            size,
            KEY_OPER::READ_KEY_EXACT,
            0x00);
    }
}

/*---------------------------  Invoke PG Operators ---------------------------*/
static Datum MakeDatumString(const char* message)
{
    size_t strSize = strlen(message);
    size_t allocSize = VARHDRSZ + strSize + 1;
    bytea* copy = (bytea*)palloc(allocSize);
    if (copy == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Execute", "Failed to allocate %u bytes for error datum string", (unsigned)allocSize);
        return PointerGetDatum(nullptr);
    }

    errno_t erc = memcpy_s(VARDATA(copy), strSize, (uint8_t*)message, strSize);
    securec_check(erc, "\0", "\0");

    SET_VARSIZE(copy, allocSize);
    VARDATA(copy)[strSize] = 0;
    return PointerGetDatum(copy);
}

static MOT::RC HandlePGError(JitExec::JitExecState* execState, const char* operation, int spiConnectId = -1)
{
    volatile MOT::RC result = MOT::RC_ERROR;

    // first restore SPI state if ordered to do so
    if (spiConnectId >= 0) {
        SPI_disconnect(spiConnectId + 1);
        SPI_restore_connection();
    }

    // now handle error
    volatile ErrorData* edata = CopyErrorData();
    MOT_LOG_WARN("Caught exception while %s: %s", operation, edata->message);
    ereport(WARNING,
        (errmodule(MOD_MOT),
            errmsg("Caught exception while %s: %s", operation, edata->message),
            errdetail("%s", edata->detail)));

    // prepare error data and SQL state in JIT execution state
    // note: make sure all memory is allocated in caller's context
    volatile MemoryContext origCxt = JitExec::SwitchToSPICallerContext();
    PG_TRY();
    {
        execState->m_errorMessage = PointerGetDatum(nullptr);
        execState->m_errorDetail = PointerGetDatum(nullptr);
        execState->m_errorHint = PointerGetDatum(nullptr);
        execState->m_sqlStateString = PointerGetDatum(nullptr);

        execState->m_errorMessage = MakeDatumString(edata->message);
        if (edata->detail != 0) {
            execState->m_errorDetail = MakeDatumString(edata->detail);
        }
        if (edata->hint != 0) {
            execState->m_errorHint = MakeDatumString(edata->hint);
        }
        execState->m_sqlState = edata->sqlerrcode;
        if (execState->m_sqlState == ERRCODE_IN_FAILED_SQL_TRANSACTION) {
            result = MOT::RC_TXN_ABORTED;
        } else if (execState->m_sqlState == 0) {
            execState->m_sqlState = ERRCODE_PLPGSQL_ERROR;
        }
        char sqlStateCode[6] = {};
        JitExec::SqlStateToCode(execState->m_sqlState, sqlStateCode);
        execState->m_sqlStateString = MakeDatumString(sqlStateCode);
    }
    PG_CATCH();
    {
        MOT_LOG_PANIC("Failed to allocate memory in error handler");
    }
    PG_END_TRY();
    (void)MemoryContextSwitchTo(origCxt);

    // cleanup error state
    FlushErrorState();
    FreeErrorData((ErrorData*)edata);
    return result;
}

static void SetDefaultErrorData(JitExec::JitExecState* execState)
{
    // prepare error data and SQL state in JIT execution state
    // note: make sure all memory is allocated in caller's context
    volatile MemoryContext origCxt = JitExec::SwitchToSPICallerContext();
    PG_TRY();
    {
        execState->m_errorMessage = PointerGetDatum(nullptr);
        execState->m_errorDetail = PointerGetDatum(nullptr);
        execState->m_errorHint = PointerGetDatum(nullptr);
        execState->m_sqlStateString = PointerGetDatum(nullptr);

        execState->m_errorMessage = MakeDatumString("Unknown error occurred");
        execState->m_sqlState = ERRCODE_PLPGSQL_ERROR;
        char sqlStateCode[6] = {};
        JitExec::SqlStateToCode(execState->m_sqlState, sqlStateCode);
        execState->m_sqlStateString = MakeDatumString(sqlStateCode);
    }
    PG_CATCH();
    {
        MOT_LOG_PANIC("Failed to allocate memory while setting default error data");
    }
    PG_END_TRY();
    (void)MemoryContextSwitchTo(origCxt);
}

inline JitExec::JitFunctionExecState* GetFunctionExecState()
{
    JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    MOT_ASSERT(jitContext != nullptr);
    MOT_ASSERT(jitContext->m_contextType == JitExec::JitContextType::JIT_CONTEXT_TYPE_FUNCTION);
    JitExec::JitFunctionExecState* execState = (JitExec::JitFunctionExecState*)jitContext->m_execState;
    MOT_ASSERT(execState != nullptr);
    return execState;
}

static void SignalException(JitExec::MotJitContext* jitContext)
{
    if (jitContext->m_execState->m_sqlState == 0) {
        SetDefaultErrorData(jitContext->m_execState);
    }

    int faultCode = jitContext->m_execState->m_sqlState;

    MOT_LOG_DEBUG("SignalException, with faultBuf at %p on exec state %p: %s",
        (int8_t*)jitContext->m_execState->m_faultBuf,
        jitContext->m_execState,
        jitContext->m_queryString);

    if (jitContext->m_contextType == JitExec::JitContextType::JIT_CONTEXT_TYPE_FUNCTION) {
        JitExec::JitFunctionExecState* execState = GetFunctionExecState();
        if (execState->m_exceptionStack == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Execute JIT LLVM Stored Procedure",
                "Cannot signal exception: exception stack is empty, while executing %s. Aborting execution.",
                u_sess->mot_cxt.jit_context->m_queryString);
            RaiseFault(LLVM_FAULT_UNHANDLED_EXCEPTION);
        }

        // set exception value and raise exception status
        execState->m_exceptionStatus = 1;
        execState->m_exceptionValue = faultCode;

        // set exception origin
        execState->m_exceptionOrigin = JIT_EXCEPTION_EXTERNAL;
    } else {
        JitExec::JitExecState* execState = (JitExec::JitExecState*)jitContext->m_execState;
        // set exception value and raise exception status
        execState->m_exceptionStatus = 1;
        execState->m_exceptionValue = faultCode;
        RaiseFault(LLVM_FAULT_UNHANDLED_EXCEPTION);
    }
}

inline void HandlePGFunctionError(JitExec::MotJitContext* jitContext, int spiConnectId)
{
    (void)HandlePGError(jitContext->m_execState, "executing PG Function", spiConnectId);
}

Datum JitInvokePGFunction0(PGFunction fptr, int collationId)
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile Datum result = PointerGetDatum(nullptr);
    volatile bool signalException = false;
    volatile JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile int spiConnectId = SPI_connectid();

    FunctionCallInfoData fcinfo;
    InitFunctionCallInfoData(fcinfo, NULL, 0, (Oid)collationId, NULL, NULL);
    PG_TRY();
    {
        result = fptr(&fcinfo);
        SET_EXPR_IS_NULL(fcinfo.isnull);
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        HandlePGFunctionError((JitExec::MotJitContext*)jitContext, spiConnectId);
        signalException = true;
    }
    PG_END_TRY();

    if (signalException) {
        SignalException((JitExec::MotJitContext*)jitContext);
    }
    return result;
}

inline void PrepareFmgrInfo(FunctionCallInfoData* fcinfo, FmgrInfo* flinfo, FuncExpr* funcExpr, List* funcArgs,
    ListCell* cells, Const* args, Oid* argTypes, int argCount)
{
    funcExpr->xpr.type = T_FuncExpr;
    funcExpr->funcvariadic = false;

    // prepare for function arguments a list of constants nodes with corresponding argument types
    for (int i = 0; i < argCount; ++i) {
        fcinfo->argTypes[i] = argTypes[i];
        args[i].xpr.type = T_Const;
        args[i].consttype = argTypes[i];
        cells[i].data.ptr_value = &args[i];
        if (i < argCount - 1) {
            cells[i].next = &cells[i + 1];
        } else {
            cells[i].next = nullptr;
        }
    }

    funcArgs->type = T_List;
    funcArgs->length = argCount;
    funcArgs->head = &cells[0];
    funcArgs->tail = &cells[argCount - 1];

    funcExpr->args = funcArgs;
    flinfo->fn_expr = (fmNodePtr)funcExpr;
    fcinfo->flinfo = flinfo;
}

Datum JitInvokePGFunction1(PGFunction fptr, int collationId, int isStrict, Datum arg, int isnull, Oid argType)
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile Datum result = PointerGetDatum(nullptr);
    volatile bool signalException = false;
    volatile JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile int spiConnectId = SPI_connectid();

    FunctionCallInfoData fcinfo1;
    InitFunctionCallInfoData(fcinfo1, NULL, 1, (Oid)collationId, NULL, NULL);
    PG_TRY();
    {
        bool shouldCallFunc = true;
        fcinfo1.arg[0] = arg;
        fcinfo1.argnull[0] = isnull;

        // some functions require types in flinfo.fn_expr, so we fake it as required
        FmgrInfo flinfo = {};
        FuncExpr funcExpr = {};
        List funcArgs = {};
        ListCell cells[1] = {};
        Const args[1] = {};
        Oid argTypes[1] = {argType};
        PrepareFmgrInfo(&fcinfo1, &flinfo, &funcExpr, &funcArgs, cells, args, argTypes, 1);

        if (isStrict > 0) {
            for (int i = 0; i < fcinfo1.nargs; i++) {
                if (fcinfo1.argnull[i]) {
                    shouldCallFunc = false;
                    break;
                }
            }
        }

        // call the function
        if (shouldCallFunc) {
            result = fptr(&fcinfo1);
            SET_EXPR_IS_NULL(fcinfo1.isnull);
        } else {
            SET_EXPR_IS_NULL(true);
        }
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        HandlePGFunctionError((JitExec::MotJitContext*)jitContext, spiConnectId);
        signalException = true;
    }
    PG_END_TRY();

    if (signalException) {
        SignalException((JitExec::MotJitContext*)jitContext);
    }
    return result;
}

Datum JitInvokePGFunction2(PGFunction fptr, int collationId, int isStrict, Datum arg1, int isnull1, Oid argType1,
    Datum arg2, int isnull2, Oid argType2)
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile Datum result = PointerGetDatum(nullptr);
    volatile bool signalException = false;
    volatile JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile int spiConnectId = SPI_connectid();

    FunctionCallInfoData fcinfo2;
    InitFunctionCallInfoData(fcinfo2, NULL, 2, (Oid)collationId, NULL, NULL);
    PG_TRY();
    {
        bool shouldCallFunc = true;
        fcinfo2.arg[0] = arg1;
        fcinfo2.argnull[0] = isnull1;
        fcinfo2.arg[1] = arg2;
        fcinfo2.argnull[1] = isnull2;

        // some functions require types in flinfo.fn_expr, so we fake it as required
        FmgrInfo flinfo = {};
        FuncExpr funcExpr = {};
        List funcArgs = {};
        ListCell cells[2] = {};
        Const args[2] = {};
        Oid argTypes[2] = {argType1, argType2};
        PrepareFmgrInfo(&fcinfo2, &flinfo, &funcExpr, &funcArgs, cells, args, argTypes, 2);

        if (isStrict > 0) {
            for (int i = 0; i < fcinfo2.nargs; i++) {
                if (fcinfo2.argnull[i]) {
                    shouldCallFunc = false;
                    break;
                }
            }
        }

        // call the function
        if (shouldCallFunc) {
            result = fptr(&fcinfo2);
            SET_EXPR_IS_NULL(fcinfo2.isnull);
        } else {
            SET_EXPR_IS_NULL(true);
        }
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        HandlePGFunctionError((JitExec::MotJitContext*)jitContext, spiConnectId);
        signalException = true;
    }
    PG_END_TRY();

    if (signalException) {
        SignalException((JitExec::MotJitContext*)jitContext);
    }
    return result;
}

Datum JitInvokePGFunction3(PGFunction fptr, int collationId, int isStrict, Datum arg1, int isnull1, Oid argType1,
    Datum arg2, int isnull2, Oid argType2, Datum arg3, int isnull3, Oid argType3)
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile Datum result = PointerGetDatum(nullptr);
    volatile bool signalException = false;
    volatile JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile int spiConnectId = SPI_connectid();

    FunctionCallInfoData fcinfo3;
    InitFunctionCallInfoData(fcinfo3, NULL, 3, (Oid)collationId, NULL, NULL);
    PG_TRY();
    {
        bool shouldCallFunc = true;
        fcinfo3.arg[0] = arg1;
        fcinfo3.argnull[0] = isnull1;
        fcinfo3.arg[1] = arg2;
        fcinfo3.argnull[1] = isnull2;
        fcinfo3.arg[2] = arg3;
        fcinfo3.argnull[2] = isnull3;

        // some functions require types in flinfo.fn_expr, so we fake it as required
        FmgrInfo flinfo = {};
        FuncExpr funcExpr = {};
        List funcArgs = {};
        ListCell cells[3] = {};
        Const args[3] = {};
        Oid argTypes[3] = {argType1, argType2, argType3};
        PrepareFmgrInfo(&fcinfo3, &flinfo, &funcExpr, &funcArgs, cells, args, argTypes, 3);

        // call the function
        if (isStrict > 0) {
            for (int i = 0; i < fcinfo3.nargs; i++) {
                if (fcinfo3.argnull[i] == true) {
                    shouldCallFunc = false;
                    break;
                }
            }
        }
        if (shouldCallFunc) {
            result = fptr(&fcinfo3);
            SET_EXPR_IS_NULL(fcinfo3.isnull);
        } else {
            SET_EXPR_IS_NULL(true);
        }
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        HandlePGFunctionError((JitExec::MotJitContext*)jitContext, spiConnectId);
        signalException = true;
    }
    PG_END_TRY();

    if (signalException) {
        SignalException((JitExec::MotJitContext*)jitContext);
    }
    return result;
}

static Datum JitInvokePGFunctionNImpl(PGFunction fptr, int collationId, int isStrict, Datum* args, int* isnull,
    Oid* argTypes, int argCount, Oid functionId)
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile Datum result = PointerGetDatum(nullptr);
    volatile bool signalException = false;
    volatile JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile ListCell* cells = nullptr;
    volatile Const* constArgs = nullptr;
    volatile int spiConnectId = SPI_connectid();

    FunctionCallInfoData fcinfo;
    InitFunctionCallInfoData(fcinfo, NULL, argCount, (Oid)collationId, NULL, NULL);
    PG_TRY();
    {
        bool shouldCallFunc = true;
        for (int i = 0; i < argCount; ++i) {
            fcinfo.arg[i] = args[i];
            fcinfo.argnull[i] = isnull[i];
            if (isStrict && isnull[i]) {
                SET_EXPR_IS_NULL(true);
                shouldCallFunc = false;
                break;
            }
        }

        if (shouldCallFunc) {
            // some functions require types in flinfo.fn_expr, so we fake it as required
            FmgrInfo flinfo = {};
            FuncExpr funcExpr = {};
            List funcArgs = {};
            cells = (ListCell*)MOT::MemSessionAlloc(sizeof(ListCell) * argCount);
            constArgs = (Const*)MOT::MemSessionAlloc(sizeof(Const) * argCount);
            if ((cells == nullptr) || (constArgs == nullptr)) {
                ereport(ERROR,
                    (errmodule(MOD_MOT),
                        errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                        errmsg("MOT/JIT execution cannot call PG function"),
                        errdetail("Out of session memory")));
            }
            PrepareFmgrInfo(
                &fcinfo, &flinfo, &funcExpr, &funcArgs, (ListCell*)cells, (Const*)constArgs, argTypes, argCount);
            flinfo.fn_oid = functionId;  // for invoke unjittable using plpgsql_call_handler

            // call the function
            result = fptr(&fcinfo);
            SET_EXPR_IS_NULL(fcinfo.isnull);
        }
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        HandlePGFunctionError((JitExec::MotJitContext*)jitContext, spiConnectId);
        signalException = true;
    }
    PG_END_TRY();

    // cleanup
    if (cells != nullptr) {
        MOT::MemSessionFree((void*)cells);
    }
    if (args != nullptr) {
        MOT::MemSessionFree((void*)constArgs);
    }

    if (signalException) {
        SignalException((JitExec::MotJitContext*)jitContext);
    }
    return result;
}

Datum JitInvokePGFunctionN(
    PGFunction fptr, int collationId, int isStrict, Datum* args, int* isnull, Oid* argTypes, int argCount)
{
    return JitInvokePGFunctionNImpl(fptr, collationId, isStrict, args, isnull, argTypes, argCount, 0);
}

uint8_t* JitMemSessionAlloc(uint32_t size)
{
    return (uint8_t*)MOT::MemSessionAlloc(size);
}

void JitMemSessionFree(uint8_t* ptr)
{
    MOT::MemSessionFree(ptr);
}

MOT::Row* searchRow(MOT::Table* table, MOT::Key* key, int access_mode_value, int innerRow, int subQueryIndex)
{
    MOT_LOG_DEBUG("searchRow(): Searching row at table %p by key %p", table, key);
    MOT::Row* row = NULL;
    MOT::TxnManager* curr_txn = u_sess->mot_cxt.jit_txn;
    MOT::AccessType access_mode = (MOT::AccessType)access_mode_value;
    MOT::RC rc = MOT::RC_OK;

    row = curr_txn->RowLookupByKey(table, access_mode, key, rc);
    if (row == nullptr) {
        MOT_LOG_DEBUG("searchRow(): Row not found");
        if (rc != MOT::RC_OK) {
            u_sess->mot_cxt.jit_context->m_rc = rc;
            return nullptr;
        }
    } else {
        MOT_ASSERT(rc == MOT::RC_OK);
        if (access_mode == MOT::AccessType::WR) {
            rc = curr_txn->UpdateLastRowState(access_mode);
            if (rc != MOT::RC_OK) {
                u_sess->mot_cxt.jit_context->m_rc = rc;
                row = nullptr;
            } else {
                row = curr_txn->GetLastAccessedDraft();
            }
        }
    }
    MOT_LOG_DEBUG("searchRow(): Returning row: %p", row);
    return row;
}

/*---------------------------  BitmapSet Helpers ---------------------------*/
void setBit(MOT::BitmapSet* bmp, int col)
{
    MOT_LOG_DEBUG("Setting column %d at bitset %p", col, bmp);
    bmp->SetBit(col);
    MOT_LOG_DEBUG("Column %d at bitset %p set successfully", col, bmp);
}

void resetBitmapSet(MOT::BitmapSet* bmp)
{
    MOT_LOG_DEBUG("Resetting bitset %p", bmp);
    bmp->Clear();
}

/*---------------------------  General Helpers ---------------------------*/
int writeRow(MOT::Row* row, MOT::BitmapSet* bmp)
{
    MOT::RC rc = MOT::RC_ERROR;
    MOT_LOG_DEBUG("writeRow(): Writing row %p to DB", row);
    MOT::TxnManager* curr_txn = u_sess->mot_cxt.jit_txn;
    MOT_LOG_DEBUG("writeRow(): Current txn is: %p", curr_txn);
    rc = curr_txn->UpdateLastRowState(MOT::AccessType::WR);
    MOT_LOG_DEBUG("writeRow(): Row write result: %d", (int)rc);
    if (rc == MOT::RC_OK) {
        MOT_LOG_DEBUG("writeRow(): Overwriting row %p with bitset %p", row, bmp);
        rc = curr_txn->OverwriteRow(row, *bmp);
        MOT_LOG_DEBUG("writeRow(): Overwrite row result: %d", (int)rc);
    } else {
        MOT_LOG_DEBUG("writeRow(): Failed to update row %p with rc (%s)", row, MOT::RcToString(rc));
    }
    return (int)rc;
}

int getTableFieldCount(MOT::Table* table)
{
    MOT_LOG_DEBUG("Retrieving table %p field count", table);
    int result = (int)table->GetFieldCount();
    MOT_LOG_DEBUG("Table %p field count is %u", table, result);
    return result;
}

MOT::Row* createNewRow(MOT::Table* table)
{
    MOT_LOG_DEBUG("Creating new row from table %p", table);
    MOT::Row* row = table->CreateNewRow();
    MOT_LOG_DEBUG("Created new row %p", row);
    return row;
}

int insertRow(MOT::Table* table, MOT::Row* row)
{
    MOT::RC rc = MOT::RC_ERROR;
    MOT_LOG_DEBUG("insertRow(): Inserting row %p to DB", row);
    MOT::TxnManager* curr_txn = u_sess->mot_cxt.jit_txn;
    MOT_LOG_DEBUG("insertRow(): Current txn is: %p", curr_txn);
    uint8_t* bits = (uint8_t*)row->GetData();
    for (uint32_t i = 1; i < table->GetFieldCount(); i++) {
        MOT::Column* col = table->GetField(i);
        if (!col->GetIsDropped() && col->m_isNotNull && !BITMAP_GET(bits, (i - 1))) {
            MOT_LOG_DEBUG("insertRow(): Cannot set null to not-nullable column %d (%s)", i, col->m_name);
            if (u_sess->mot_cxt.jit_context != NULL) {
                u_sess->mot_cxt.jit_context->m_execState->m_nullViolationTable = table;
                u_sess->mot_cxt.jit_context->m_execState->m_nullColumnId = i;
            }
            return (int)MOT::RC_NULL_VIOLATION;
        }
    }
    rc = table->InsertRow(row, curr_txn);
    if (rc != MOT::RC_OK) {
        MOT_LOG_DEBUG("insertRow(): Insert row failed with rc: %d", (int)rc);
    }
    return (int)rc;
}

int deleteRow()
{
    MOT::RC rc = MOT::RC_ERROR;
    MOT_LOG_DEBUG("deleteRow(): Deleting row from DB");
    MOT::TxnManager* curr_txn = u_sess->mot_cxt.jit_txn;
    MOT_LOG_DEBUG("deleteRow(): Current txn is: %p", curr_txn);
    rc = curr_txn->DeleteLastRow();
    if (rc != MOT::RC_OK) {
        MOT_LOG_DEBUG("deleteRow(): Delete row failed with rc: %d", (int)rc);
    }
    return (int)rc;
}

void setRowNullBits(MOT::Table* table, MOT::Row* row)
{
    // we assume that every column not participating in INSERT is a NULL, so we nullify all row null bits
    // later, each inserted column will update its null bit status
    uint8_t* rowData = const_cast<uint8_t*>(row->GetData());
    uint8_t* bits = rowData + table->GetFieldOffset((uint64_t)0);
    MOT_LOG_DEBUG("Setting row %p null bits at address %p, %u bytes", row, bits, (unsigned)table->GetFieldSize(0));
    errno_t erc = memset_s(bits, table->GetFieldSize(0), 0x00, table->GetFieldSize(0));
    securec_check(erc, "\0", "\0");
}

int setConstNullBit(MOT::Table* table, MOT::Row* row, int table_colid, int isnull)
{
    MOT_LOG_DEBUG("Setting const null bit at table %p row %p column id %d: isnull=%d", table, row, table_colid, isnull);

    MOT::Column* column = table->GetField(table_colid);
    if (isnull && column->m_isNotNull) {
        MOT_LOG_DEBUG("Cannot set null to not-nullable column %d (%s)", table_colid, column->m_name);
        if (u_sess->mot_cxt.jit_context != NULL) {
            u_sess->mot_cxt.jit_context->m_execState->m_nullViolationTable = table;
            u_sess->mot_cxt.jit_context->m_execState->m_nullColumnId = table_colid;
        }
        return (int)MOT::RC_NULL_VIOLATION;
    }

    uint8_t* rowData = (uint8_t*)row->GetData();
    uint8_t* bits = rowData + table->GetFieldOffset((uint64_t)0);
    if (isnull) {
        BITMAP_CLEAR(bits, (table_colid - 1));  // zero-based index column id
    } else {
        BITMAP_SET(bits, (table_colid - 1));  // zero-based index column id
    }

    return (int)MOT::RC_OK;
}

int setExprResultNullBit(MOT::Table* table, MOT::Row* row, int table_colid)
{
    int isnull = GET_EXPR_IS_NULL();
    MOT_LOG_DEBUG("Setting expression result isnull at column %d to: %d", table_colid, isnull);
    return setConstNullBit(table, row, table_colid, isnull);
}

void execClearTuple(TupleTableSlot* slot)
{
    MOT_LOG_DEBUG("Clearing tuple %p", slot);
    ::ExecClearTuple(slot);
}

void execStoreVirtualTuple(TupleTableSlot* slot)
{
    MOT_LOG_DEBUG("Storing virtual tuple %p", slot);
    ::ExecStoreVirtualTuple(slot);
}

struct SelectRowFunctor {
    MOT::Table* m_table;
    MOT::Row* m_row;
    TupleTableSlot* m_slot;
    int m_tableColumnId;
    int m_tupleColumnId;

    SelectRowFunctor(MOT::Table* table, MOT::Row* row, TupleTableSlot* slot, int tableColumnId, int tupleColumnId)
        : m_table(table), m_row(row), m_slot(slot), m_tableColumnId(tableColumnId), m_tupleColumnId(tupleColumnId)
    {}

    inline void operator()()
    {
        uint8_t* rowData = const_cast<uint8_t*>(m_row->GetData());
        m_slot->tts_tupleDescriptor->attrs[m_tupleColumnId].attnum = m_tableColumnId;
        MOTAdaptor::MOTToDatum(m_table,
            &m_slot->tts_tupleDescriptor->attrs[m_tupleColumnId],
            rowData,
            &(m_slot->tts_values[m_tupleColumnId]),
            &(m_slot->tts_isnull[m_tupleColumnId]));
    }
};

void selectColumn(MOT::Table* table, MOT::Row* row, TupleTableSlot* slot, int table_colid, int tuple_colid)
{
    MOT_LOG_DEBUG("Selecting into tuple column %d from table %s, row %p column id %d [%s]",
        tuple_colid,
        table->GetTableName().c_str(),
        row,
        table_colid,
        table->GetFieldName(table_colid));
    uint8_t* rowData = const_cast<uint8_t*>(row->GetData());
    slot->tts_tupleDescriptor->attrs[tuple_colid].attnum = table_colid;
    MOTAdaptor::MOTToDatum(table,
        &slot->tts_tupleDescriptor->attrs[tuple_colid],
        rowData,
        &(slot->tts_values[tuple_colid]),
        &(slot->tts_isnull[tuple_colid]));
    DEBUG_PRINT_DATUM("Column Datum",
        slot->tts_tupleDescriptor->attrs[tuple_colid].atttypid,
        slot->tts_values[tuple_colid],
        slot->tts_isnull[tuple_colid]);
}

void setTpProcessed(uint64_t* tp_processed, uint64_t rows)
{
    MOT_LOG_DEBUG("Setting tp_processed at %p to %" PRIu64 " rows", tp_processed, rows);
    *tp_processed = rows;
}

void setScanEnded(int* scan_ended, int result)
{
    MOT_LOG_DEBUG("Setting scan_ended at %p to %d", scan_ended, result);
    *scan_ended = result;
}

/*---------------------------  Range Update Helpers ---------------------------*/
void copyKey(MOT::Index* index, MOT::Key* src_key, MOT::Key* dest_key)
{
    MOT_LOG_DEBUG("Copy key: index = %s (%p), src_key = %p (%u bytes), dest_key = %p (%u bytes)",
        index->GetName().c_str(),
        index,
        src_key,
        src_key->GetKeyLength(),
        dest_key,
        dest_key->GetKeyLength());
    dest_key->CpKey(*src_key);
    MOT_LOG_DEBUG("Copied key %p into key %p", src_key, dest_key);
}

void FillKeyPattern(MOT::Key* key, unsigned char pattern, int offset, int size)
{
    MOT_LOG_DEBUG("Filling key %p pattern %u at offset %d, size %d: %s",
        key,
        (unsigned)pattern,
        offset,
        size,
        MOT::HexStr(key->GetKeyBuf(), key->GetKeyLength()).c_str());
    MOT_ASSERT((offset + size) <= key->GetKeyLength());
    (void)key->FillPattern(pattern, size, offset);
    MOT_LOG_DEBUG("Filled key %p pattern %u at offset %d, size %d: %s",
        key,
        (unsigned)pattern,
        offset,
        size,
        MOT::HexStr(key->GetKeyBuf(), key->GetKeyLength()).c_str());
}

void adjustKey(MOT::Key* key, MOT::Index* index, unsigned char pattern)
{
    MOT_LOG_DEBUG("Adjusting key %p with pattern %u: %s",
        key,
        (unsigned)pattern,
        MOT::HexStr(key->GetKeyBuf(), key->GetKeyLength()).c_str());
    index->AdjustKey(key, pattern);
    MOT_LOG_DEBUG("Adjusted key %p with pattern %u: %s",
        key,
        (unsigned)pattern,
        MOT::HexStr(key->GetKeyBuf(), key->GetKeyLength()).c_str());
}

MOT::IndexIterator* searchIterator(MOT::Index* index, MOT::Key* key, int forward_scan, int include_bound)
{
    MOT::IndexIterator* itr = NULL;
    bool matchKey = include_bound;  // include bound in query
    bool forwardDirection = forward_scan ? true : false;
    bool found = false;

    MOT_LOG_DEBUG("searchIterator(): Creating begin iterator for index %p from key %p (include_bound=%s): %s",
        index,
        key,
        include_bound ? "true" : "false",
        MOT::HexStr(key->GetKeyBuf(), key->GetKeyLength()).c_str());
    MOT::TxnManager* curr_txn = u_sess->mot_cxt.jit_txn;
    MOT_LOG_DEBUG("searchIterator(): Current txn is: %p", curr_txn);

    itr = index->Search(key, matchKey, forwardDirection, curr_txn->GetThdId(), found);
    if (itr != nullptr) {
        if (!found) {
            MOT_LOG_DEBUG("searchIterator(): Exact match not found, still continuing, itr=%p", itr);
        } else {
            MOT_LOG_DEBUG("searchIterator(): Exact match found with iterator %p", itr);
        }
    } else {
        MOT::RC rootRc = MOT_GET_ROOT_ERROR_RC();
        u_sess->mot_cxt.jit_context->m_rc = rootRc != MOT::RC_OK ? rootRc : MOT::RC_MEMORY_ALLOCATION_ERROR;
        MOT_LOG_ERROR("searchIterator(): Failed to create iterator for index (%s) with rc (%s) rootRc (%s)",
            index->GetName().c_str(),
            MOT::RcToString(u_sess->mot_cxt.jit_context->m_rc),
            MOT::RcToString(rootRc));
    }

    return itr;
}

MOT::IndexIterator* beginIterator(MOT::Index* index)
{
    MOT::IndexIterator* itr = index->Begin(MOTCurrThreadId);
    if (itr == nullptr) {
        MOT::RC rootRc = MOT_GET_ROOT_ERROR_RC();
        u_sess->mot_cxt.jit_context->m_rc = rootRc != MOT::RC_OK ? rootRc : MOT::RC_MEMORY_ALLOCATION_ERROR;
        MOT_LOG_ERROR("beginIterator(): Failed to create iterator for index (%s) with rc (%s) rootRc (%s)",
            index->GetName().c_str(),
            MOT::RcToString(u_sess->mot_cxt.jit_context->m_rc),
            MOT::RcToString(rootRc));
    }

    return itr;
}

MOT::IndexIterator* createEndIterator(MOT::Index* index, MOT::Key* key, int forward_scan, int include_bound)
{
    MOT::IndexIterator* itr = NULL;
    bool matchKey = include_bound;  // include bound in query
    bool forwardDirection =
        forward_scan ? false : true;  // in forward scan search key or previous, in backwards scan search key or next
    bool found = false;

    MOT_LOG_DEBUG("createEndIterator(): Creating end iterator (forward_scan=%d, forwardDirection=%s, "
                  "include_bound=%s)) for index %p from key %p: %s",
        forward_scan,
        forwardDirection ? "true" : "false",
        include_bound ? "true" : "false",
        index,
        key,
        MOT::HexStr(key->GetKeyBuf(), key->GetKeyLength()).c_str());
    MOT::TxnManager* curr_txn = u_sess->mot_cxt.jit_txn;
    MOT_LOG_DEBUG("Current txn is: %p", curr_txn);

    itr = index->Search(key, matchKey, forwardDirection, curr_txn->GetThdId(), found);
    if (itr != nullptr) {
        if (!found) {
            MOT_LOG_DEBUG("createEndIterator(): Exact match not found, still continuing, itr=%p", itr);
        } else {
            MOT_LOG_DEBUG("createEndIterator(): Exact match found with iterator %p", itr);
        }
    } else {
        MOT::RC rootRc = MOT_GET_ROOT_ERROR_RC();
        u_sess->mot_cxt.jit_context->m_rc = rootRc != MOT::RC_OK ? rootRc : MOT::RC_MEMORY_ALLOCATION_ERROR;
        MOT_LOG_ERROR("createEndIterator(): Failed to create iterator for index (%s) with rc (%s) rootRc (%s)",
            index->GetName().c_str(),
            MOT::RcToString(u_sess->mot_cxt.jit_context->m_rc),
            MOT::RcToString(rootRc));
    }

    return itr;
}

int isScanEnd(MOT::Index* index, MOT::IndexIterator* itr, MOT::IndexIterator* end_itr, int forward_scan)
{
    MOT_LOG_DEBUG("isScanEnd(): Checking if scan ended");
    MOT_ASSERT(itr != nullptr);
    if (!itr->IsValid()) {
        MOT_LOG_DEBUG("isScanEnd(): begin iterator %p is not valid", itr);
        return 1;
    }

    // in case of full-scan end iterator is null, and then we can definitely conclude that scan has not ended yet
    // (since begin iterator is still valid)
    if (end_itr == nullptr) {
#ifdef MOT_JIT_FULL_SCAN
        MOT_LOG_DEBUG("isScanEnd(): full-scan not ended yet");
        return 0;
#else
        MOT_LOG_TRACE("isScanEnd(): end iterator is null. index %p", index);
        u_sess->mot_cxt.jit_context->m_rc = MOT::RC_MEMORY_ALLOCATION_ERROR;
        return 1;
#endif
    }

    // check end iterator validity
    if (!end_itr->IsValid()) {
        MOT_LOG_DEBUG("isScanEnd(): end iterator %p is not valid", end_itr);
        return 1;
    }

    // compare begin/end iterator keys of iterated rows
    const MOT::Key* startKey = reinterpret_cast<const MOT::Key*>(const_cast<void*>(itr->GetKey()));
    const MOT::Key* endKey = reinterpret_cast<const MOT::Key*>(const_cast<void*>(end_itr->GetKey()));
    if ((startKey == nullptr) || (endKey == nullptr)) {
        MOT_LOG_DEBUG("isScanEnd(): either start key or end key is invalid");
        return 1;
    }
    MOT_LOG_DEBUG("isScanEnd(): Start key: %s", MOT::HexStr(startKey->GetKeyBuf(), startKey->GetKeyLength()).c_str());
    MOT_LOG_DEBUG("isScanEnd(): End key:   %s", MOT::HexStr(endKey->GetKeyBuf(), endKey->GetKeyLength()).c_str());

    int res = 0;
    int cmpRes = memcmp(startKey->GetKeyBuf(), endKey->GetKeyBuf(), index->GetKeySizeNoSuffix());
    MOT_LOG_DEBUG("isScanEnd(): cmpRes = %d", cmpRes);
    if (forward_scan) {
        if (cmpRes > 0) {  // end key is included in scan (so == is not reported as end of scan)
            MOT_LOG_DEBUG("isScanEnd(): end of forward scan detected");
            res = 1;
        }
    } else {
        if (cmpRes < 0) {
            MOT_LOG_DEBUG("isScanEnd(): end of backward scan detected");
            res = 1;
        }
    }

    return res;
}

int CheckRowExistsInIterator(MOT::Index* index, MOT::IndexIterator* itr, MOT::IndexIterator* endItr, int forwardScan)
{
    bool rowExists = false;
    MOT::RC rc = MOT::RC_OK;

    MOT_LOG_DEBUG("CheckRowExistsInIterator(): Retrieving row from iterator %p", itr);
    MOT::TxnManager* currTxn = u_sess->mot_cxt.jit_txn;
    do {
        // get row from iterator using primary sentinel
        MOT::Sentinel* sentinel = itr->GetPrimarySentinel();
        rowExists = currTxn->IsRowExist(sentinel, rc);
        if (!rowExists) {
            if (rc != MOT::RC_OK) {
                u_sess->mot_cxt.jit_context->m_rc = rc;
                itr->Invalidate();
                break;
            }
            MOT_LOG_DEBUG("CheckRowExistsInIterator(): Encountered non-existent row during scan, advancing iterator");
            itr->Next();
            continue;
        }

        MOT_ASSERT(rc == MOT::RC_OK);
        // verify the scan did not pass the end iterator
        if (isScanEnd(index, itr, endItr, forwardScan)) {
            MOT_LOG_DEBUG("CheckRowExistsInIterator(): Detected end of scan");
            rowExists = false;
            itr->Invalidate();
            break;
        }

        // prepare already for next round
        itr->Next();
        break;
    } while (itr->IsValid());

    MOT_LOG_DEBUG(
        "CheckRowExistsInIterator(): Retrieved row %s from iterator %p", rowExists ? "exists" : "not-exist", itr);
    return rowExists ? 1 : 0;
}

MOT::Row* getRowFromIterator(MOT::Index* index, MOT::IndexIterator* itr, MOT::IndexIterator* end_itr, int access_mode,
    int forward_scan, int innerRow, int subQueryIndex)
{
    MOT::Row* row = NULL;
    MOT::TxnManager* curr_txn = u_sess->mot_cxt.jit_txn;

    MOT::RC rc = MOT::RC_OK;

    MOT_LOG_DEBUG("getRowFromIterator(): Retrieving row from iterator %p", itr);
    do {
        // get row from iterator using primary sentinel
        MOT::Sentinel* sentinel = itr->GetPrimarySentinel();
        row = curr_txn->RowLookup((MOT::AccessType)access_mode, sentinel, rc);
        if (row == NULL) {
            if (rc != MOT::RC_OK) {
                u_sess->mot_cxt.jit_context->m_rc = rc;
                itr->Invalidate();
                break;
            }
            MOT_LOG_DEBUG("getRowFromIterator(): Encountered NULL row during scan, advancing iterator");
            itr->Next();
            continue;
        }

        MOT_ASSERT(rc == MOT::RC_OK);
        // verify the scan did not pass the end iterator
        if (isScanEnd(index, itr, end_itr, forward_scan)) {
            MOT_LOG_DEBUG("getRowFromIterator(): Detected end of scan");
            row = NULL;
            itr->Invalidate();
            break;
        }

        // prepare already for next round
        itr->Next();
        break;
    } while (itr->IsValid());

    if (row) {
        if (access_mode == MOT::AccessType::WR) {
            rc = curr_txn->UpdateLastRowState((MOT::AccessType)access_mode);
            if (rc != MOT::RC_OK) {
                u_sess->mot_cxt.jit_context->m_rc = rc;
                row = nullptr;
                itr->Invalidate();
            } else {
                row = curr_txn->GetLastAccessedDraft();
            }
        }
    }
    MOT_LOG_DEBUG("getRowFromIterator(): Retrieved row %p from iterator %p", row, itr);
    return row;
}

void destroyIterator(MOT::IndexIterator* itr)
{
    MOT_LOG_DEBUG("Destroying iterator %p", itr);
    if (itr != nullptr) {
        itr->Invalidate();
        itr->Destroy();
        delete itr;
    }
}

/*---------------------------  Stateful Execution Helpers ---------------------------*/
void setStateIterator(MOT::IndexIterator* itr, int begin_itr, int inner_scan)
{
    MOT_LOG_DEBUG("Setting state iterator %p (begin_itr=%d, inner_scan=%d)", itr, begin_itr, inner_scan);
    if (u_sess->mot_cxt.jit_context) {
        JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
        if (inner_scan) {
            if (begin_itr) {
                execState->m_innerBeginIterator = itr;
            } else {
                execState->m_innerEndIterator = itr;
            }
        } else {
            if (begin_itr) {
                execState->m_beginIterator = itr;
            } else {
                execState->m_endIterator = itr;
            }
        }
    }
}

MOT::IndexIterator* getStateIterator(int begin_itr, int inner_scan)
{
    MOT::IndexIterator* result = NULL;
    if (u_sess->mot_cxt.jit_context) {
        JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
        if (inner_scan) {
            if (begin_itr) {
                result = execState->m_innerBeginIterator;
            } else {
                result = execState->m_innerEndIterator;
            }
        } else {
            if (begin_itr) {
                result = execState->m_beginIterator;
            } else {
                result = execState->m_endIterator;
            }
        }
    }
    MOT_LOG_DEBUG("Retrieved state iterator %p (begin_itr=%d, inner_scan=%d)", result, begin_itr, inner_scan);
    return result;
}

int isStateIteratorNull(int begin_itr, int inner_scan)
{
    int is_null = 0;
    if (u_sess->mot_cxt.jit_context) {
        JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
        if (inner_scan) {
            if (begin_itr) {
                is_null = execState->m_innerBeginIterator ? 0 : 1;
            } else {
                is_null = execState->m_innerEndIterator ? 0 : 1;
            }
        } else {
            if (begin_itr) {
                is_null = execState->m_beginIterator ? 0 : 1;
            } else {
                is_null = execState->m_endIterator ? 0 : 1;
            }
        }
    }
    MOT_LOG_DEBUG(
        "Checked if state iterator is null (begin_itr=%d, inner_scan=%d): result=%d", begin_itr, inner_scan, is_null);
    return is_null;
}

int isStateScanEnd(int forward_scan, int inner_scan)
{
    int result = -1;
    JitExec::JitQueryContext* jitContext = (JitExec::JitQueryContext*)u_sess->mot_cxt.jit_context;
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    if (inner_scan) {
        result = isScanEnd(
            jitContext->m_innerIndex, execState->m_innerBeginIterator, execState->m_innerEndIterator, forward_scan);
    } else {
        result = isScanEnd(jitContext->m_index, execState->m_beginIterator, execState->m_endIterator, forward_scan);
    }
    MOT_LOG_DEBUG(
        "Checked if state scan ended (forward_scan=%d, inner_scan=%d): result=%d", forward_scan, inner_scan, result);
    return result;
}

MOT::Row* getRowFromStateIterator(int access_mode, int forward_scan, int inner_scan)
{
    MOT::Row* result = NULL;
    JitExec::JitQueryContext* jitContext = (JitExec::JitQueryContext*)u_sess->mot_cxt.jit_context;
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    if (inner_scan) {
        result = getRowFromIterator(jitContext->m_innerIndex,
            execState->m_innerBeginIterator,
            execState->m_innerEndIterator,
            access_mode,
            forward_scan,
            inner_scan,
            -1);
    } else {
        result = getRowFromIterator(jitContext->m_index,
            execState->m_beginIterator,
            execState->m_endIterator,
            access_mode,
            forward_scan,
            inner_scan,
            -1);
    }
    MOT_LOG_DEBUG("Retrieved row %p from state iterator (access_mode=%d, forward_scan=%d, inner_scan=%d): result=%d",
        result,
        access_mode,
        forward_scan,
        inner_scan);
    return result;
}

void destroyStateIterators(int inner_scan)
{
    MOT_LOG_DEBUG("Destroying state iterators (inner_scan=%d)", inner_scan);
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    if (inner_scan) {
        if (execState->m_innerBeginIterator) {
            destroyIterator(execState->m_innerBeginIterator);
            execState->m_innerBeginIterator = NULL;
        }
        if (execState->m_innerEndIterator) {
            destroyIterator(execState->m_innerEndIterator);
            execState->m_innerEndIterator = NULL;
        }
    } else {
        if (execState->m_beginIterator) {
            destroyIterator(execState->m_beginIterator);
            execState->m_beginIterator = NULL;
        }
        if (execState->m_endIterator) {
            destroyIterator(execState->m_endIterator);
            execState->m_endIterator = NULL;
        }
    }
}

void setStateScanEndFlag(int scan_ended, int inner_scan)
{
    MOT_LOG_DEBUG("Setting state scan end flag to %d (inner_scan=%d)", scan_ended, inner_scan);
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    if (inner_scan) {
        execState->m_innerScanEnded = scan_ended;
    } else {
        execState->m_scanEnded = scan_ended;
    }
}

int getStateScanEndFlag(int inner_scan)
{
    int result = -1;
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    if (inner_scan) {
        result = (int)execState->m_innerScanEnded;
    } else {
        result = (int)execState->m_scanEnded;
    }
    MOT_LOG_DEBUG("Retrieved state scan end flag %d (inner_scan=%d)", result, inner_scan);
    return result;
}

void resetStateRow(int inner_scan)
{
    MOT_LOG_DEBUG("Resetting state row to NULL (inner_scan=%d)", inner_scan);
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    if (inner_scan) {
        execState->m_innerRow = NULL;
    } else {
        execState->m_row = NULL;
    }
}

void setStateRow(MOT::Row* row, int inner_scan)
{
    MOT_LOG_DEBUG("Setting state row to %p (inner_scan=%d)", row, inner_scan);
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    if (inner_scan) {
        execState->m_innerRow = row;
    } else {
        execState->m_row = row;
    }
}

MOT::Row* getStateRow(int inner_scan)
{
    MOT::Row* result = NULL;
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    if (inner_scan) {
        result = execState->m_innerRow;
    } else {
        result = execState->m_row;
    }
    MOT_LOG_DEBUG("Retrieved state row %p (inner_scan=%d)", result, inner_scan);
    return result;
}

void copyOuterStateRow()
{
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    MOT_LOG_DEBUG(
        "Copying outer state row %p into safe copy %p (for JOIN query)", execState->m_row, execState->m_outerRowCopy);
    execState->m_outerRowCopy->Copy(execState->m_row);
}

MOT::Row* getOuterStateRowCopy()
{
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    MOT_LOG_DEBUG("Retrieved outer state row copy %p (for JOIN query)", execState->m_outerRowCopy);
    return execState->m_outerRowCopy;
}

int isStateRowNull(int inner_scan)
{
    int result = -1;
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    if (inner_scan) {
        result = (execState->m_innerRow == NULL) ? 1 : 0;
    } else {
        result = (execState->m_row == NULL) ? 1 : 0;
    }
    MOT_LOG_DEBUG("Checked if state row is null (inner_scan=%d): result=%d", inner_scan, result);
    return result;
}

void resetStateLimitCounter()
{
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    MOT_LOG_DEBUG("Resetting state limit counter to 0");
    execState->m_limitCounter = 0;
}

void incrementStateLimitCounter()
{
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    ++execState->m_limitCounter;
    MOT_LOG_DEBUG("Incremented state limit counter to %u", execState->m_limitCounter);
}

int getStateLimitCounter()
{
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    MOT_LOG_DEBUG("Retrieved state limit counter with value %u", execState->m_limitCounter);
    return execState->m_limitCounter;
}

void DebugPrintNumericArray(Datum arrayDatum)
{
    ArrayType* avgArray = (ArrayType*)DatumGetPointer(arrayDatum);
    int elmlen = -1;
    int elmbyval = false;
    char elmalign = 'i';
    bool isNull;
    int idx = 0;  // 1-based index
    Datum value = array_ref(avgArray, 1, &idx, 0, elmlen, elmbyval, elmalign, &isNull);
    MOT_LOG_DEBUG("AVG Array[0] at %p", value);
    DEBUG_PRINT_DATUM("AVG Array[0]", avgArray->elemtype, value, isNull);
    idx = 1;
    value = array_ref(avgArray, 1, &idx, 0, elmlen, elmbyval, elmalign, &isNull);
    MOT_LOG_DEBUG("AVG Array[1] at %p", value);
    DEBUG_PRINT_DATUM("AVG Array[1]", avgArray->elemtype, value, isNull);
}

void prepareAvgArray(int aggIndex, int element_type, int element_count)
{
    JitExec::JitQueryExecState* queryExecState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    VERIFY_PTR(queryExecState->m_aggExecState, "NULL aggregate array");
    JitExec::JitAggExecState* execState = &queryExecState->m_aggExecState[aggIndex];
    MOT_LOG_DEBUG("Preparing AVG() array with %d elements of type %d", element_count, element_type);
    Datum* elements = (Datum*)palloc(sizeof(Datum) * element_count);
    for (int i = 0; i < element_count; ++i) {
        if (element_type == NUMERICOID) {
            elements[i] = makeNumericZero();
            DEBUG_PRINT_DATUM("AVG initial Element", NUMERICOID, elements[i], false);
        } else if (element_type == INT8OID) {
            elements[i] = Int64GetDatum(0);
        } else if (element_type == FLOAT8OID) {
            elements[i] = Float8GetDatum(0.0f);
        }
        MOT_LOG_DEBUG("AVG Element %d: %p", i, elements[i]);
    }

    // initialize values for int8 and float8
    bool elmbyval = true;  // int8, sometimes also float8 (depending on compile flags)
    int elmlen = 8;
    char elmalign = 'd';
    if (element_type == NUMERICOID) {
        elmlen = -1;  // Numeric is a varlena object (see definition of NumericData at utils/numeric.h, and numeric type
                      // in catalog/pg_type.h)
        elmbyval = false;
        elmalign = 'i';
    } else if (element_type == FLOAT8OID) {
        elmbyval = FLOAT8PASSBYVAL;
    }

    ArrayType* avg_array = construct_array(elements, element_count, element_type, elmlen, elmbyval, elmalign);
    execState->m_avgArray = PointerGetDatum(avg_array);
    execState->m_aggValueIsNull = 1;
#ifdef MOT_JIT_DEBUG
    MOT_LOG_DEBUG("AVG array at %p", execState->m_avgArray);
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_DEBUG)) {
        DebugPrintNumericArray(execState->m_avgArray);
    }
#endif
}

Datum loadAvgArray(int aggIndex)
{
    JitExec::JitQueryExecState* queryExecState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    VERIFY_PTR(queryExecState->m_aggExecState, "NULL aggregate array");
    JitExec::JitAggExecState* execState = &queryExecState->m_aggExecState[aggIndex];
    Datum result = execState->m_avgArray;
    MOT_LOG_DEBUG("Loaded AVG() array at %p", result);
#ifdef MOT_JIT_DEBUG
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_DEBUG)) {
        DebugPrintNumericArray(result);
    }
#endif
    return result;
}

void saveAvgArray(int aggIndex, Datum avg_array)
{
    JitExec::JitQueryExecState* queryExecState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    VERIFY_PTR(queryExecState->m_aggExecState, "NULL aggregate array");
    JitExec::JitAggExecState* execState = &queryExecState->m_aggExecState[aggIndex];
    MOT_LOG_DEBUG("Saving AVG() array %p", avg_array);
#ifdef MOT_JIT_DEBUG
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_DEBUG)) {
        DebugPrintNumericArray(avg_array);
    }
#endif
    execState->m_avgArray = avg_array;
    execState->m_aggValueIsNull = 0;
}

Datum ComputeAvg(PGFunction func, Datum avgArray, bool* isNull)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    InitFunctionCallInfoData(fcinfo, NULL, 1, InvalidOid, NULL, NULL);

    fcinfo.arg[0] = avgArray;
    fcinfo.argnull[0] = false;

    result = (*func)(&fcinfo);

    // communicate back is-null status
    *isNull = fcinfo.isnull;

    return result;
}

Datum computeAvgFromArray(int aggIndex, int element_type)
{
    JitExec::JitQueryExecState* queryExecState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    VERIFY_PTR(queryExecState->m_aggExecState, "NULL aggregate array");
    JitExec::JitAggExecState* execState = &queryExecState->m_aggExecState[aggIndex];

    MOT_LOG_DEBUG("Computing AVG() array %" PRIu64, (uint64_t)execState->m_avgArray);
#ifdef MOT_JIT_DEBUG
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_DEBUG)) {
        DebugPrintNumericArray(execState->m_avgArray);
    }
#endif

    // attention: in case of null result an ereport exception is thrown, so we call avg functions carefully
    bool isNull = false;
    Datum avg = PointerGetDatum(NULL);
    if (element_type == NUMERICOID) {
        avg = ComputeAvg(numeric_avg, execState->m_avgArray, &isNull);
    } else if (element_type == INT8OID) {
        avg = ComputeAvg(int8_avg, execState->m_avgArray, &isNull);
    } else if (element_type == FLOAT8OID) {
        avg = ComputeAvg(float8_avg, execState->m_avgArray, &isNull);
    }

    SET_EXPR_IS_NULL(isNull);
    return avg;
}

void resetAggValue(int aggIndex, int element_type)
{
    JitExec::JitQueryExecState* queryExecState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    VERIFY_PTR(queryExecState->m_aggExecState, "NULL aggregate array");
    JitExec::JitAggExecState* execState = &queryExecState->m_aggExecState[aggIndex];
    MOT_LOG_DEBUG("Resetting aggregated value to zero of type %d", element_type);
    if (element_type == NUMERICOID) {
        execState->m_aggValue = makeNumericZero();
    } else if (element_type == INT8OID) {
        execState->m_aggValue = Int64GetDatum(0);
    } else if (element_type == FLOAT4OID) {
        execState->m_aggValue = Float4GetDatum(((float)0.0));
    } else if (element_type == FLOAT8OID) {
        execState->m_aggValue = Float8GetDatum(((double)0.0));
    }
    execState->m_aggValueIsNull = 1;
}

Datum getAggValue(int aggIndex)
{
    JitExec::JitQueryExecState* queryExecState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    VERIFY_PTR(queryExecState->m_aggExecState, "NULL aggregate array");
    JitExec::JitAggExecState* execState = &queryExecState->m_aggExecState[aggIndex];
    Datum result = execState->m_aggValue;
    MOT_LOG_DEBUG("Retrieved aggregated value: %" PRIu64, result);
    return result;
}

void setAggValue(int aggIndex, Datum value)
{
    JitExec::JitQueryExecState* queryExecState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    VERIFY_PTR(queryExecState->m_aggExecState, "NULL aggregate array");
    JitExec::JitAggExecState* execState = &queryExecState->m_aggExecState[aggIndex];
    MOT_LOG_DEBUG("Setting aggregated value to: %" PRIu64, value);
    execState->m_aggValue = value;
    execState->m_aggValueIsNull = 0;
}

int getAggValueIsNull(int aggIndex)
{
    JitExec::JitQueryExecState* queryExecState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    VERIFY_PTR(queryExecState->m_aggExecState, "NULL aggregate array");
    JitExec::JitAggExecState* execState = &queryExecState->m_aggExecState[aggIndex];
    int result = execState->m_aggValueIsNull;
    MOT_LOG_DEBUG("Retrieved aggregated value is-null: %d", result);
    return result;
}

int setAggValueIsNull(int aggIndex, int isNull)
{
    JitExec::JitQueryExecState* queryExecState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    VERIFY_PTR(queryExecState->m_aggExecState, "NULL aggregate array");
    JitExec::JitAggExecState* execState = &queryExecState->m_aggExecState[aggIndex];
    execState->m_aggValueIsNull = isNull;
    MOT_LOG_DEBUG("Set aggregated value is-null to: %d", isNull);
    return 0;
}

void prepareDistinctSet(int aggIndex, int element_type)
{
    JitExec::JitQueryExecState* queryExecState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    VERIFY_PTR(queryExecState->m_aggExecState, "NULL aggregate array");
    JitExec::JitAggExecState* execState = &queryExecState->m_aggExecState[aggIndex];
    switch (element_type) {
        case INT8OID:
        case INT4OID:
        case INT2OID:
        case INT1OID:
            MOT_LOG_DEBUG("Preparing distinct integer set for type %d", element_type);
            execState->m_distinctSet = prepareDistinctIntSet();
            break;

        case FLOAT4OID:
        case FLOAT8OID:
            MOT_LOG_DEBUG("Preparing distinct double-precision value set for type %d", element_type);
            execState->m_distinctSet = prepareDistinctDoubleSet();
            break;

        case NUMERICOID:
            MOT_LOG_DEBUG("Preparing distinct numeric value set for type %d", element_type);
            execState->m_distinctSet = prepareDistinctNumericSet();
            break;

        case VARCHAROID:
            MOT_LOG_DEBUG("Preparing distinct varchar value set for type %d", element_type);
            execState->m_distinctSet = prepareDistinctVarcharSet();
            break;

        default:
            MOT_LOG_ERROR("Input element type %d is invalid", element_type);
            break;
    }

    if (execState->m_distinctSet == nullptr) {
        MOT_LOG_ERROR("Failed to create distinctSet for element type %d", element_type);
        RaiseResourceLimitFault();
    }
}

int insertDistinctItem(int aggIndex, int element_type, Datum item)
{
    JitExec::JitQueryExecState* queryExecState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    VERIFY_PTR(queryExecState->m_aggExecState, "NULL aggregate array");
    JitExec::JitAggExecState* execState = &queryExecState->m_aggExecState[aggIndex];
    int result = 0;
    switch (element_type) {
        case INT8OID:
            MOT_LOG_DEBUG("Inserting distinct int8 value %" PRIu64, DatumGetInt64(item));
            result = insertDistinctIntItem(execState->m_distinctSet, DatumGetInt64(item));
            break;

        case INT4OID:
            MOT_LOG_DEBUG("Inserting distinct int4 value %" PRIu64, DatumGetInt32(item));
            result = insertDistinctIntItem(execState->m_distinctSet, DatumGetInt32(item));
            break;

        case INT2OID:
            MOT_LOG_DEBUG("Inserting distinct int2 value %" PRIu64, DatumGetInt16(item));
            result = insertDistinctIntItem(execState->m_distinctSet, DatumGetInt16(item));
            break;

        case INT1OID:
            MOT_LOG_DEBUG("Inserting distinct int1 value %" PRIu64, DatumGetInt8(item));
            result = insertDistinctIntItem(execState->m_distinctSet, DatumGetInt8(item));
            break;

        case FLOAT4OID:
            MOT_LOG_DEBUG("Inserting distinct float4 value %f", (float)DatumGetFloat4(item));
            result = insertDistinctDoubleItem(execState->m_distinctSet, DatumGetFloat4(item));
            break;

        case FLOAT8OID:
            MOT_LOG_DEBUG("Inserting distinct float8 value %f", (double)DatumGetFloat8(item));
            result = insertDistinctDoubleItem(execState->m_distinctSet, DatumGetFloat8(item));
            break;

        case NUMERICOID:
            MOT_LOG_DEBUG("Inserting distinct numeric value %f", JitExec::NumericToDouble(item));
            result = insertDistinctNumericItem(execState->m_distinctSet, item);
            break;

        case VARCHAROID:
            DEBUG_PRINT_DATUM("Inserting distinct varchar value", VARCHAROID, item, false);
            result = insertDistinctVarcharItem(execState->m_distinctSet, item);
            break;

        default:
            MOT_LOG_ERROR("Input element type %d is invalid", element_type);
            break;
    }
    return result;
}

void destroyDistinctSet(int aggIndex, int element_type)
{
    JitExec::JitQueryExecState* queryExecState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    VERIFY_PTR(queryExecState->m_aggExecState, "NULL aggregate array");
    JitExec::JitAggExecState* execState = &queryExecState->m_aggExecState[aggIndex];
    switch (element_type) {
        case INT8OID:
        case INT4OID:
        case INT2OID:
        case INT1OID:
            MOT_LOG_DEBUG("Destroying distinct integer set for type %d", element_type);
            destroyDistinctIntSet(execState->m_distinctSet);
            break;

        case FLOAT4OID:
        case FLOAT8OID:
            MOT_LOG_DEBUG("Destroying distinct double-precision value set for type %d", element_type);
            destroyDistinctDoubleSet(execState->m_distinctSet);
            break;

        case NUMERICOID:
            MOT_LOG_DEBUG("Destroying distinct numeric value set for type %d", element_type);
            destroyDistinctNumericSet(execState->m_distinctSet);
            break;

        case VARCHAROID:
            MOT_LOG_DEBUG("Destroying distinct varchar value set for type %d", element_type);
            destroyDistinctVarcharSet(execState->m_distinctSet);
            break;

        default:
            MOT_LOG_ERROR("Input element type %d is invalid", element_type);
            break;
    }
}

void resetTupleDatum(TupleTableSlot* slot, int tuple_colid, int zero_type)
{
    // write zero numeric on the fly (we rely on current memory context to clean up)
    MOT_LOG_DEBUG("Resetting tuple %p column %d datum to typed zero by type %d", slot, tuple_colid, zero_type)
    switch (zero_type) {
        case NUMERICOID:
            writeTupleDatum(slot, tuple_colid, makeNumericZero(), 1);
            break;

        case FLOAT8OID:
            writeTupleDatum(slot, tuple_colid, Float8GetDatum(0), 1);
            break;

        case FLOAT4OID:
            writeTupleDatum(slot, tuple_colid, Float4GetDatum(0), 1);
            break;

        case INT8OID:
            writeTupleDatum(slot, tuple_colid, Int64GetDatum(0), 1);
            break;

        case INT4OID:
            writeTupleDatum(slot, tuple_colid, Int32GetDatum(0), 1);
            break;

        case INT2OID:
            writeTupleDatum(slot, tuple_colid, Int16GetDatum(0), 1);
            break;

        default:
            MOT_LOG_PANIC("Unexpected zero type while resetting tuple datum: %d", zero_type)
            break;
    }
}

Datum readTupleDatum(TupleTableSlot* slot, int tuple_colid)
{
    MOT_LOG_DEBUG("Reading datum from tuple column %d ", tuple_colid);
    Datum result = slot->tts_values[tuple_colid];
    DEBUG_PRINT_DATUM("Pre-sum Tuple Datum",
        slot->tts_tupleDescriptor->attrs[tuple_colid].atttypid,
        slot->tts_values[tuple_colid],
        slot->tts_isnull[tuple_colid]);
    bool isnull = (result == PointerGetDatum(NULL));
    SET_EXPR_IS_NULL((int)isnull);
    return result;
}

void writeTupleDatum(TupleTableSlot* slot, int tuple_colid, Datum datum, int isnull)
{
    MOT_LOG_DEBUG("Writing datum to tuple column %d, isnull %d", tuple_colid, isnull);
    slot->tts_values[tuple_colid] = datum;
    slot->tts_isnull[tuple_colid] = isnull;
    DEBUG_PRINT_DATUM("Post-sum Tuple Datum",
        slot->tts_tupleDescriptor->attrs[tuple_colid].atttypid,
        slot->tts_values[tuple_colid],
        slot->tts_isnull[tuple_colid]);
}

Datum SelectSubQueryResult(int subQueryIndex)
{
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    return readTupleDatum(execState->m_subQueryExecState[subQueryIndex].m_slot, 0);
}

void CopyAggregateToSubQueryResult(int subQueryIndex)
{
    // sub-query can have only 1 aggregate
    MOT_LOG_DEBUG("Copying aggregate datum to sub-query %d slot", subQueryIndex);
    JitExec::JitQueryExecState* queryExecState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    JitExec::JitAggExecState* execState = &queryExecState->m_aggExecState[0];
    writeTupleDatum(queryExecState->m_subQueryExecState[subQueryIndex].m_slot,
        0,
        execState->m_aggValue,
        execState->m_aggValueIsNull);
}

TupleTableSlot* GetSubQuerySlot(int subQueryIndex)
{
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    return execState->m_subQueryExecState[subQueryIndex].m_slot;
}

MOT::Table* GetSubQueryTable(int subQueryIndex)
{
    JitExec::JitQueryContext* jitContext = (JitExec::JitQueryContext*)u_sess->mot_cxt.jit_context;
    return jitContext->m_subQueryContext[subQueryIndex].m_table;
}

MOT::Index* GetSubQueryIndex(int subQueryIndex)
{
    JitExec::JitQueryContext* jitContext = (JitExec::JitQueryContext*)u_sess->mot_cxt.jit_context;
    return jitContext->m_subQueryContext[subQueryIndex].m_index;
}

MOT::Key* GetSubQuerySearchKey(int subQueryIndex)
{
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    return execState->m_subQueryExecState[subQueryIndex].m_searchKey;
}

MOT::Key* GetSubQueryEndIteratorKey(int subQueryIndex)
{
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)u_sess->mot_cxt.jit_context->m_execState;
    return execState->m_subQueryExecState[subQueryIndex].m_endIteratorKey;
}

int8_t* LlvmPushExceptionFrame()
{
    // allocate exception frame
    size_t allocSize = sizeof(JitExec::JitFunctionExecState::ExceptionFrame);
    JitExec::JitFunctionExecState::ExceptionFrame* exceptionFrame =
        (JitExec::JitFunctionExecState::ExceptionFrame*)MOT::MemSessionAlloc(allocSize);
    if (exceptionFrame == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "Execute JIT LLVM Stored Procedure",
            "Failed to allocate %u bytes for exception frame, while executing %s. Aborting execution.",
            (unsigned)allocSize,
            u_sess->mot_cxt.jit_context->m_queryString);
        RaiseFault(LLVM_FAULT_RESOURCE_LIMIT);
        return nullptr;  // to avoid compiler warning
    }

    // set exception frame attributes and push exception frame
    JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    uint64_t frameIndex = (execState->m_exceptionStack != nullptr) ? execState->m_exceptionStack->m_frameIndex + 1 : 0;
    exceptionFrame->m_frameIndex = frameIndex;
    exceptionFrame->m_next = execState->m_exceptionStack;
    execState->m_exceptionStack = exceptionFrame;
    int8_t* frame = (int8_t*)exceptionFrame->m_jmpBuf;
    MOT_LOG_DEBUG("Pushed exception frame #%d %p with jump buf at %p on exec state %p",
        (int)frameIndex,
        exceptionFrame,
        frame,
        execState);
    return frame;
}

int8_t* LlvmGetCurrentExceptionFrame()
{
    JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    JitExec::JitFunctionExecState::ExceptionFrame* exceptionFrame = execState->m_exceptionStack;
    if (exceptionFrame == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Execute JIT LLVM Stored Procedure",
            "Cannot get current exception frame: exception stack is empty, while executing %s. Aborting execution.",
            u_sess->mot_cxt.jit_context->m_queryString);
        RaiseFault(LLVM_FAULT_UNHANDLED_EXCEPTION);
    }

    MOT_LOG_DEBUG("Getting current exception frame #%d %p with jump buf at %p",
        (int)exceptionFrame->m_frameIndex,
        exceptionFrame,
        (int8_t*)execState->m_exceptionStack->m_jmpBuf);
    return (int8_t*)execState->m_exceptionStack->m_jmpBuf;
}

int LlvmPopExceptionFrame()
{
    JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    JitExec::JitFunctionExecState::ExceptionFrame* exceptionFrame = execState->m_exceptionStack;
    if (exceptionFrame == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Execute JIT LLVM Stored Procedure",
            "Cannot pop exception frame: exception stack is empty, while executing %s. Aborting execution.",
            u_sess->mot_cxt.jit_context->m_queryString);
        RaiseFault(LLVM_FAULT_UNHANDLED_EXCEPTION);
    }

    MOT_LOG_DEBUG("Popping exception frame #%d %p with jump buf at %p from exec state %p",
        (int)exceptionFrame->m_frameIndex,
        exceptionFrame,
        (int8_t*)exceptionFrame->m_jmpBuf,
        execState);
    execState->m_exceptionStack = exceptionFrame->m_next;
    MOT::MemSessionFree(exceptionFrame);
    return execState->m_exceptionValue;
}

void LlvmThrowException(int exceptionValue)
{
    // verify there is a handler
    JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    if (jitContext->m_contextType == JitExec::JitContextType::JIT_CONTEXT_TYPE_FUNCTION) {
        JitExec::JitFunctionExecState* execState = GetFunctionExecState();
        if (execState->m_exceptionStack == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Execute JIT LLVM Stored Procedure",
                "Cannot throw exception: exception stack is empty, while executing %s. Aborting execution.",
                u_sess->mot_cxt.jit_context->m_queryString);
            RaiseFault(LLVM_FAULT_UNHANDLED_EXCEPTION);
        }

        // set exception value and raise exception status
        execState->m_exceptionStatus = 1;
        execState->m_exceptionValue = exceptionValue;

        // jump to handler
        MOT_LOG_DEBUG("Throwing exception, jumping to frame #%d %p with jump buf at %p on exec state %p",
            (int)execState->m_exceptionStack->m_frameIndex,
            execState->m_exceptionStack,
            (int8_t*)execState->m_exceptionStack->m_jmpBuf,
            execState);
        siglongjmp(execState->m_exceptionStack->m_jmpBuf, execState->m_exceptionValue);
    } else {
        JitExec::JitExecState* execState = (JitExec::JitExecState*)jitContext->m_execState;
        // set exception value and raise exception status
        execState->m_exceptionStatus = 1;
        execState->m_exceptionValue = exceptionValue;
        RaiseFault(LLVM_FAULT_UNHANDLED_EXCEPTION);
    }
}

void LlvmRethrowException()
{
    // verify there is a handler
    JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    if (execState->m_exceptionStack == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Execute JIT LLVM Stored Procedure",
            "Cannot re-throw exception: exception stack is empty, while executing %s. Aborting execution.",
            u_sess->mot_cxt.jit_context->m_queryString);
        RaiseFault(LLVM_FAULT_UNHANDLED_EXCEPTION);
    }

    execState->m_exceptionStatus = 1;  // raise again exception status flag
    MOT_LOG_DEBUG("Re-throwing exception, jumping to frame #%d %p with jump buf at %p on exec state %p",
        (int)execState->m_exceptionStack->m_frameIndex,
        execState->m_exceptionStack,
        (int8_t*)execState->m_exceptionStack->m_jmpBuf,
        execState);
    siglongjmp(execState->m_exceptionStack->m_jmpBuf, execState->m_exceptionValue);
}

int LlvmGetExceptionValue()
{
    JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    return execState->m_exceptionValue;
}

int LlvmGetExceptionStatus()
{
    JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    return execState->m_exceptionStatus;
}

void LlvmResetExceptionStatus()
{
    JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    execState->m_exceptionStatus = 0;
}

void LlvmResetExceptionValue()
{
    JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    execState->m_exceptionValue = 0;
}

void LlvmUnwindExceptionFrame()
{
    // same as re-throw
    MOT_LOG_DEBUG("Unwinding exception")
    LlvmRethrowException();
}

void LlvmClearExceptionStack()
{
    MOT_LOG_DEBUG("Clearing exception stack");
    JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    while (execState->m_exceptionStack != nullptr) {
        JitExec::JitFunctionExecState::ExceptionFrame* exceptionFrame = execState->m_exceptionStack;
        MOT_LOG_DEBUG("Popping exception frame #%d %p with jump buf at %p from exec state %p",
            (int)exceptionFrame->m_frameIndex,
            exceptionFrame,
            (int8_t*)exceptionFrame->m_jmpBuf,
            execState);
        execState->m_exceptionStack = exceptionFrame->m_next;
        MOT::MemSessionFree(exceptionFrame);
    }
}

void LLvmPrintFrame(const char* msg, int8_t* frame)
{
    MOT_LOG_DEBUG("%s: %p", msg, frame);
}

void ValidateParams(ParamListInfo params, int paramId = -1)
{
    if (params == nullptr) {
        MOT_LOG_ERROR("Attempt to access null parameter list");
        RaiseAccessViolationFault();
    }

    // if we reached here then parameters must be not null
    MOT_ASSERT(params != nullptr);
    if (paramId != -1) {
        if (paramId >= params->numParams) {
            MOT_LOG_ERROR("Attempt to access parameter (%d items) list at %p out of range: %d",
                params->numParams,
                params,
                paramId);
            RaiseAccessViolationFault();
        }
    }
}

void JitAbortFunction(int errorCode)
{
    RaiseFault(errorCode);
}

int JitHasNullParam(ParamListInfo params)
{
    ValidateParams(params);  // if null long jump is made
    MOT_ASSERT(params != nullptr);
    for (int i = 0; i < params->numParams; ++i) {
        if (params->params[i].isnull) {
            return 1;
        }
    }
    return 0;
}

int JitGetParamCount(ParamListInfo params)
{
    ValidateParams(params);  // if null long jump is made
    MOT_ASSERT(params != nullptr);
    return params->numParams;
}

Datum JitGetParamAt(ParamListInfo params, int paramId)
{
    ValidateParams(params, paramId);  // if null long jump is made
    MOT_ASSERT(params != nullptr);
    return params->params[paramId].value;
}

int JitIsParamNull(ParamListInfo params, int paramId)
{
    ValidateParams(params, paramId);  // if null long jump is made
    int result = (params->params[paramId].isnull ? 1 : 0);
    MOT_LOG_DEBUG("JitIsParamNull(%d) = %s", paramId, result ? "true" : "false");
    return result;
}

static ParamListInfo GetParamListForParam(ParamListInfo params, int& paramId)
{
    // attention: the parameter list passed into SP has the same size as parameter list passed to the query that
    // invoked the SP, so parameter ids are the same
    JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    if (jitContext->m_contextType == JitExec::JitContextType::JIT_CONTEXT_TYPE_FUNCTION) {
        JitExec::JitFunctionExecState* execState = (JitExec::JitFunctionExecState*)jitContext->m_execState;
        JitExec::JitQueryContext* invokingContext = (JitExec::JitQueryContext*)execState->m_invokingContext;
        if (invokingContext->m_invokeParamInfo[paramId].m_mode == JitExec::JitParamMode::JIT_PARAM_DIRECT) {
            JitExec::JitQueryExecState* invokingExecState =
                (JitExec::JitQueryExecState*)execState->m_invokingContext->m_execState;
            params = invokingExecState->m_directParams;
            paramId = invokingContext->m_invokeParamInfo[paramId].m_index;
            MOT_LOG_DEBUG(
                "Using parameters at %p from invoking context for parameter at modified pos %d", params, paramId);
            return (ParamListInfo)params;
        }
    }
    MOT_LOG_DEBUG("Using original parameters at %p for parameter %d", params, paramId);
    return params;
}

Datum* JitGetParamAtRef(ParamListInfo params, int paramId)
{
    // we check at the current context whether this parameter is direct passed-as-is or not
    params = GetParamListForParam(params, paramId);
    ValidateParams(params, paramId);  // if null long jump is made
    MOT_ASSERT(params != nullptr);
    Datum* result = &(params->params[paramId].value);
    MOT_LOG_DEBUG("Retrieved param %d ref %p from params %p", paramId, result, params);
    DEBUG_PRINT_DATUM("datum value: ", params->params[paramId].ptype, *result, params->params[paramId].isnull);
    return result;
}

bool* JitIsParamNullRef(ParamListInfo params, int paramId)
{
    // we check at the current context with this parameter is direct passed-as-is or not
    params = GetParamListForParam(params, paramId);
    ValidateParams(params, paramId);  // if null long jump is made
    MOT_ASSERT(params != nullptr);
    bool* result = &params->params[paramId].isnull;
    MOT_LOG_DEBUG("Retrieved param %d is-null ref %p from params %p: %d", paramId, result, params, (int)*result);
    return result;
}

ParamListInfo GetInvokeParamListInfo()
{
    MOT_ASSERT(u_sess->mot_cxt.jit_context->m_contextType == JitExec::JitContextType::JIT_CONTEXT_TYPE_QUERY);
    JitExec::JitQueryContext* jitContext = (JitExec::JitQueryContext*)u_sess->mot_cxt.jit_context;
    MOT_ASSERT(jitContext->m_commandType == JitExec::JIT_COMMAND_INVOKE);
    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)jitContext->m_execState;
    MOT_LOG_DEBUG("Invoke params are at %p", execState->m_invokeParams);
    return execState->m_invokeParams;
}

void DestroyParamListInfo(ParamListInfo params)
{
    MOT::MemSessionFree(params);
}

void SetParamValue(ParamListInfo params, int paramId, int paramType, Datum datum, int isNull)
{
    params->params[paramId].value = datum;
    params->params[paramId].ptype = paramType;
    params->params[paramId].isnull = isNull ? true : false;
    DEBUG_PRINT_DATUM("param: ", paramType, datum, params->params[paramId].isnull);
}

void SetSPSubQueryParamValue(int subQueryId, int id, int type, Datum value, int isNull)
{
    MOT_ASSERT(u_sess->mot_cxt.jit_context->m_contextType == JitExec::JitContextType::JIT_CONTEXT_TYPE_FUNCTION);
    JitExec::JitFunctionContext* jitContext = (JitExec::JitFunctionContext*)u_sess->mot_cxt.jit_context;
    JitExec::JitFunctionExecState* functionExecState = (JitExec::JitFunctionExecState*)jitContext->m_execState;

    ParamExternData* param = &functionExecState->m_invokedQueryExecState[subQueryId].m_params->params[id];
    param->ptype = type;
    param->value = value;
    param->isnull = isNull ? true : false;
    MOT_LOG_DEBUG("Passed to SP sub-query %d param: ", subQueryId);
    DEBUG_PRINT_DATUM("Passed to SP sub-query param", type, value, isNull);
}

inline void PullUpErrorInfo(JitExec::JitExecState* source, JitExec::JitExecState* target)
{
    target->m_nullColumnId = source->m_nullColumnId;
    target->m_nullViolationTable = source->m_nullViolationTable;
    target->m_errorMessage = source->m_errorMessage;
    target->m_errorDetail = source->m_errorDetail;
    target->m_errorHint = source->m_errorHint;
    target->m_sqlState = source->m_sqlState;
    target->m_sqlStateString = source->m_sqlStateString;
}

static int InvokeJittedStoredProcedure(JitExec::JitQueryContext* jitContext)
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile int res = 0;
    volatile bool signalException = false;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile int spiConnectId = SPI_connectid();

    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)jitContext->m_execState;
    MOT_LOG_DEBUG("Invoking jittable SP %s with %u invoke parameters at %p",
        jitContext->m_invokeContext->m_queryString,
        jitContext->m_invokeParamCount,
        execState->m_invokeParams);

    MOT_ASSERT(jitContext->m_invokeContext->m_execState != nullptr);
    jitContext->m_invokeContext->m_execState->m_invokingContext = jitContext;
    jitContext->m_execState->m_sqlState = 0;
    jitContext->m_invokeContext->m_execState->m_sqlState = 0;
    PG_TRY();
    {
        res = JitExec::JitExecFunction(jitContext->m_invokeContext,
            execState->m_invokeParams,
            execState->m_invokeSlot,
            execState->m_invokeTuplesProcessed,
            execState->m_invokeScanEnded);
        MOT_LOG_DEBUG("Finished executing invoked stored procedure %s with result: %d (%s)",
            jitContext->m_invokeContext->m_queryString,
            res,
            MOT::RcToString((MOT::RC)res));
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        res = HandlePGError(jitContext->m_invokeContext->m_execState, "executing jitted SP", spiConnectId);
        signalException = true;
    }
    PG_END_TRY();

    // pull-up error info
    PullUpErrorInfo(jitContext->m_invokeContext->m_execState, jitContext->m_execState);

    // signal to jitted function that an exception is to be thrown
    if (signalException) {
        SignalException(jitContext);
    }

    return res;
}

static bool CopyHeapTupleHeader(HeapTupleHeader tupleHeader, TupleTableSlot* slot)
{
    // we fake a tuple for things to work
    HeapTupleData tupleData;
    HeapTuple tuple = &tupleData;
    tuple->t_data = tupleHeader;

    TupleDesc tupDesc = slot->tts_tupleDescriptor;
    int tupDescAttrCount = tupDesc->natts;
    int tupleAttrCount = PointerIsValid(tupleHeader) ? HeapTupleHeaderGetNatts(tupleHeader, tupDesc) : 0;
    int resultIndex = 0;
    for (int i = 0; i < tupDescAttrCount; ++i) {
        // skip dropped columns in destination
        if (tupDesc->attrs[i].attisdropped) {
            continue;
        }

        bool isNull = true;
        Datum value = (Datum)0;
        if ((i < tupleAttrCount) && !tupDesc->attrs[i].attisdropped) {
            value = SPI_getbinval(tuple, tupDesc, i + 1, &isNull);
            if (SPI_result != 0) {
                MOT_LOG_TRACE(
                    "Failed to get datum value from tuple: %s (%u)", SPI_result_code_string(SPI_result), SPI_result);
                return false;
            }
        } else {
            continue;  // skip dropped columns in source
        }
        SetSlotValue(slot, resultIndex++, value, isNull);
    }

    return true;
}

static int InvokeUnjittableStoredProcedure(JitExec::JitQueryContext* jitContext)
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile int res = 0;
    volatile bool signalException = false;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile Datum* args = nullptr;
    volatile int* isnull = nullptr;
    volatile Oid* argTypes = nullptr;
    volatile int spiConnectId = SPI_connectid();

    JitExec::JitQueryExecState* execState = (JitExec::JitQueryExecState*)jitContext->m_execState;
    MOT_LOG_DEBUG("Invoking unjittable SP %s with %u invoke parameters at %p",
        jitContext->m_invokedQueryString,
        jitContext->m_invokeParamCount,
        execState->m_invokeParams);

    execState->m_sqlState = 0;
    PG_TRY();
    {
        // prepare parameters
        int argCount = (int)jitContext->m_invokeParamCount;
        args = (Datum*)palloc(argCount * sizeof(Datum));
        isnull = (int*)palloc(argCount * sizeof(int));
        argTypes = (Oid*)palloc(argCount * sizeof(Oid));
        for (int i = 0; i < argCount; ++i) {
            if (jitContext->m_invokeParamInfo[i].m_mode == JitExec::JitParamMode::JIT_PARAM_DIRECT) {
                int paramId = jitContext->m_invokeParamInfo[i].m_index;
                MOT_LOG_DEBUG("Using direct parameter %d at modified pos %d", i, paramId);
                args[i] = execState->m_directParams->params[paramId].value;
                isnull[i] = execState->m_directParams->params[paramId].isnull;
                argTypes[i] = execState->m_directParams->params[paramId].ptype;
            } else {
                MOT_LOG_DEBUG("Using passed parameter %d", i);
                args[i] = execState->m_invokeParams->params[i].value;
                isnull[i] = execState->m_invokeParams->params[i].isnull;
                argTypes[i] = execState->m_invokeParams->params[i].ptype;
            }
        }

        // call plpgsql_call_handler
        Datum resDatum = JitInvokePGFunctionNImpl(plpgsql_call_handler,
            DEFAULT_COLLATION_OID,
            0,
            (Datum*)args,
            (int*)isnull,
            (Oid*)argTypes,
            argCount,
            jitContext->m_invokedFunctionOid);

        // now copy datum to result tuple
        if (execState->m_invokeSlot->tts_tupleDescriptor->natts > 1) {
            // result datum is a tuple already in the caller's memory context, just copy the datum
            HeapTupleHeader tupleHeader = (HeapTupleHeader)DatumGetPointer(resDatum);
            if (!CopyHeapTupleHeader(tupleHeader, execState->m_invokeSlot)) {
                MOT_LOG_TRACE("Failed to copy result slot");
                res = (int)MOT::RC_ERROR;
            }
        } else if (execState->m_invokeSlot->tts_tupleDescriptor->natts == 1) {
            // it is OK to have a boxed tuple here as a single attribute
            execState->m_invokeSlot->tts_values[0] = resDatum;
            execState->m_invokeSlot->tts_isnull[0] = false;
            (void)ExecStoreVirtualTuple(execState->m_invokeSlot);
        }
        MOT_LOG_DEBUG("Finished executing unjittable invoked stored procedure %s with result: %d (%s)",
            jitContext->m_invokedQueryString,
            res,
            MOT::RcToString((MOT::RC)res));
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        res = MOT::RC_ERROR;
        (void)MemoryContextSwitchTo(origCxt);
        res = HandlePGError(jitContext->m_execState, "executing unjittable SP", spiConnectId);
        signalException = true;
    }
    PG_END_TRY();

    // note: no need to pull-up error info, error info already produced in caller's execution state
    if (args != nullptr) {
        pfree((void*)args);
    }
    if (isnull != nullptr) {
        pfree((void*)isnull);
    }
    if (argTypes != nullptr) {
        pfree((void*)argTypes);
    }
    // signal to jitted function that an exception is to be thrown
    if (signalException) {
        SignalException(jitContext);
    }

    return res;
}

int InvokeStoredProcedure()
{
    JitExec::JitQueryContext* jitContext = (JitExec::JitQueryContext*)u_sess->mot_cxt.jit_context;
    MOT_ASSERT(u_sess->mot_cxt.jit_context->m_contextType == JitExec::JitContextType::JIT_CONTEXT_TYPE_QUERY);
    MOT_ASSERT(jitContext->m_commandType == JitExec::JIT_COMMAND_INVOKE);

    int result = 0;
    if (jitContext->m_invokeContext != nullptr) {
        result = InvokeJittedStoredProcedure(jitContext);
    } else {
        result = InvokeUnjittableStoredProcedure(jitContext);
    }
    return result;
}

int IsCompositeResult(TupleTableSlot* slot)
{
    int res = 0;
    if ((slot->tts_tupleDescriptor->natts == 1) && slot->tts_tupleDescriptor->attrs[0].atttypid == RECORDOID) {
        res = 1;
    }
    MOT_LOG_DEBUG("Result is composite: %d", res);
    return res;
}

Datum* CreateResultDatums()
{
    MOT_ASSERT(u_sess->mot_cxt.jit_context->m_contextType == JitExec::JitContextType::JIT_CONTEXT_TYPE_FUNCTION);
    JitExec::JitFunctionContext* jitContext = (JitExec::JitFunctionContext*)u_sess->mot_cxt.jit_context;
    int natts = jitContext->m_rowTupDesc->natts;
    Datum* dvalues = (Datum*)palloc0(natts * sizeof(Datum));
    MOT_LOG_DEBUG("Created %d datums for heap tuple at %p", natts, dvalues);
    return dvalues;
}

bool* CreateResultNulls()
{
    MOT_ASSERT(u_sess->mot_cxt.jit_context->m_contextType == JitExec::JitContextType::JIT_CONTEXT_TYPE_FUNCTION);
    JitExec::JitFunctionContext* jitContext = (JitExec::JitFunctionContext*)u_sess->mot_cxt.jit_context;
    int natts = jitContext->m_rowTupDesc->natts;
    bool* nulls = (bool*)palloc(natts * sizeof(bool));
    MOT_LOG_DEBUG("Created %d nulls for heap tuple at %p", natts, nulls);
    return nulls;
}

void SetResultValue(Datum* datums, bool* nulls, int index, Datum value, int isNull)
{
    MOT_LOG_DEBUG("Setting heap tuple datum/null pair at index %d", index);
    datums[index] = value;
    nulls[index] = isNull ? true : false;
}

Datum CreateResultHeapTuple(Datum* dvalues, bool* nulls)
{
    MOT_ASSERT(u_sess->mot_cxt.jit_context->m_contextType == JitExec::JitContextType::JIT_CONTEXT_TYPE_FUNCTION);
    JitExec::JitFunctionContext* jitContext = (JitExec::JitFunctionContext*)u_sess->mot_cxt.jit_context;

    // we do not initialize anything, caller is responsible for that
    HeapTuple tuple = heap_form_tuple(jitContext->m_rowTupDesc, dvalues, nulls);
    MOT_LOG_DEBUG(
        "Created heap tuple %p from datum/null arrays using tuple descriptor %p", tuple, jitContext->m_rowTupDesc);

    pfree_ext(dvalues);
    pfree_ext(nulls);

    TupleConversionMap* tupMap = convert_tuples_by_position(jitContext->m_rowTupDesc,
        jitContext->m_resultTupDesc,
        gettext_noop("returned record type does not match expected record type"),
        jitContext->m_functionOid);

    if (tupMap != nullptr) {
        tuple = do_convert_tuple(tuple, tupMap);
        free_conversion_map(tupMap);
        MOT_LOG_DEBUG("Converted heap tuple at %p", tuple);
    }

    return PointerGetDatum(tuple);
}

void SetSlotValue(TupleTableSlot* slot, int tupleColId, Datum value, int isNull)
{
    MOT_LOG_DEBUG("Storing value in tuple column %d", tupleColId);
    slot->tts_values[tupleColId] = value;
    slot->tts_isnull[tupleColId] = isNull ? true : false;
    DEBUG_PRINT_DATUM("Slot Datum",
        slot->tts_tupleDescriptor->attrs[tupleColId]->atttypid,
        slot->tts_values[tupleColId],
        slot->tts_isnull[tupleColId]);
}

Datum GetSlotValue(TupleTableSlot* slot, int tupleColId)
{
    MOT_LOG_DEBUG("Retrieving datum value in tuple %p column %d", slot, tupleColId);
    Datum result = slot->tts_values[tupleColId];
    DEBUG_PRINT_DATUM("Slot Datum",
        slot->tts_tupleDescriptor->attrs[tupleColId]->atttypid,
        slot->tts_values[tupleColId],
        slot->tts_isnull[tupleColId]);
    return result;
}

int GetSlotIsNull(TupleTableSlot* slot, int tupleColId)
{
    MOT_LOG_DEBUG("Retrieving datum is-null in tuple column %d", tupleColId);
    int result = slot->tts_isnull[tupleColId] == true ? 1 : 0;
    MOT_LOG_DEBUG("Slot Datum is-null: %d", result);
    return result;
}

static void ReleaseOneSubTransaction(bool rollback, uint32_t openSubTxCount)
{
    SubTransactionId subXid = InvalidSubTransactionId;
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_DEBUG)) {
        subXid = ::GetCurrentSubTransactionId();
    }
    if (rollback) {
        MOT_LOG_DEBUG("Rolling back current sub-transaction #%u: id %" PRIu64, openSubTxCount, (uint64_t)subXid);
        ::RollbackAndReleaseCurrentSubTransaction();
    } else {
        MOT_LOG_DEBUG("Committing current sub-transaction #%u: id %" PRIu64, openSubTxCount, (uint64_t)subXid);
        // if current txn is already in aborted unrecoverable state then report error
        if (u_sess->mot_cxt.jit_txn->IsTxnAborted()) {
            raiseAbortTxnError();
        } else {
            ::ReleaseCurrentSubTransaction();
        }
    }
}

SubTransactionId JitGetCurrentSubTransactionId()
{
    return ::GetCurrentSubTransactionId();
}

void JitReleaseAllSubTransactions(bool rollback, SubTransactionId untilSubXid)
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    volatile JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    volatile bool signalException = false;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile int spiConnectId = SPI_connectid();

    if (rollback) {
        MOT_LOG_DEBUG("Rolling-back %u open sub-transactions", execState->m_openSubTxCount);
    } else {
        MOT_LOG_DEBUG("Committing %u open sub-transactions", execState->m_openSubTxCount);
    }

    PG_TRY();
    {
        SubTransactionId subXid = ::GetCurrentSubTransactionId();
        while ((execState->m_openSubTxCount > 0) && (subXid != untilSubXid)) {
            ReleaseOneSubTransaction(rollback, execState->m_openSubTxCount);
            --execState->m_openSubTxCount;
        }
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        const char* action = rollback ? "rolling back all sub-transactions" : "committing all sub-transactions";
        (void)MemoryContextSwitchTo(origCxt);
        (void)HandlePGError(jitContext->m_execState, action, spiConnectId);
        signalException = true;
    }
    PG_END_TRY();

    if (signalException) {
        if (jitContext->m_execState->m_sqlState == 0) {
            // note: we could use a more informative error code
            jitContext->m_execState->m_sqlState = ERRCODE_FDW_ERROR;
        }
        SignalException((JitExec::MotJitContext*)jitContext);
    }
    execState->m_openSubTxCount = 0;  // must be set to zero even if encountered error
}

void JitBeginBlockWithExceptions()
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    volatile JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    volatile bool signalException = false;
    volatile MemoryContext origCxt = CurrentMemoryContext;

    if (execState->m_spiBlockId >= MOT_JIT_MAX_BLOCK_DEPTH) {
        MOT_LOG_ERROR("Cannot push SPI block, reached maximum allowed %d", (int)MOT_JIT_MAX_BLOCK_DEPTH);
        RaiseResourceLimitFault();
    }

    MOT_LOG_DEBUG("Entering statement block with exceptions, with SPI nesting %d", execState->m_spiBlockId);
    PG_TRY();
    {
        if (u_sess->mot_cxt.jit_txn->IsTxnAborted()) {
            raiseAbortTxnError();
        }
        int connectid = SPI_connectid();
        execState->m_spiConnectId[execState->m_spiBlockId] = connectid;
        execState->m_savedContexts[execState->m_spiBlockId] = CurrentMemoryContext;
        MOT_LOG_DEBUG("SPI connect id: depth=%d, saved=%d, cxt=[%s @%p]",
            execState->m_spiBlockId,
            connectid,
            CurrentMemoryContext->name,
            CurrentMemoryContext);
        ::BeginInternalSubTransaction(NULL);
        SubTransactionId subXid = ::GetCurrentSubTransactionId();
        MOT_LOG_DEBUG("Started sub-transaction #%u: id %" PRId64, execState->m_openSubTxCount, subXid);
        ++execState->m_openSubTxCount;
        execState->m_subTxns[execState->m_spiBlockId] = subXid;
        ++execState->m_spiBlockId;
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        (void)HandlePGError(jitContext->m_execState, "SPI_connectid()");
        signalException = true;
    }
    PG_END_TRY();

    if (signalException) {
        if (jitContext->m_execState->m_sqlState == 0) {
            // note: we could use a more informative error code
            jitContext->m_execState->m_sqlState = ERRCODE_FDW_ERROR;
        }
        SignalException((JitExec::MotJitContext*)jitContext);
    }
}

void JitEndBlockWithExceptions()
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    volatile JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    volatile bool signalException = false;
    volatile MemoryContext origCxt = CurrentMemoryContext;

    MOT_LOG_DEBUG("Exiting statement block with exceptions");
    if (execState->m_spiBlockId == 0) {
        MOT_LOG_ERROR("Cannot end block with exceptions: no block is active");
        RaiseAccessViolationFault();
    }

    PG_TRY();
    {
        --execState->m_spiBlockId;
        SubTransactionId subXid = ::GetCurrentSubTransactionId();
        if (subXid == execState->m_subTxns[execState->m_spiBlockId]) {
            MOT_LOG_DEBUG("Committing current sub-transaction #%" PRIu64 ": id %" PRIu64,
                execState->m_openSubTxCount,
                (uint64_t)subXid);
            // if current txn is already in aborted unrecoverable state then report error
            if (u_sess->mot_cxt.jit_txn->IsTxnAborted()) {
                raiseAbortTxnError();
            } else {
                ::ReleaseCurrentSubTransaction();
                --execState->m_openSubTxCount;
            }
        }
        int savedConnectId = execState->m_spiConnectId[execState->m_spiBlockId];
        MOT_LOG_DEBUG("SPI connect id before calling restore: depth=%d, saved=%d, actual=%d",
            execState->m_spiBlockId,
            savedConnectId,
            SPI_connectid());
        SPI_restore_connection();
        MOT_LOG_DEBUG("SPI connect id after calling restore: %d", SPI_connectid());
        execState->m_spiConnectId[execState->m_spiBlockId] = -1;
        MemoryContext savedCxt = execState->m_savedContexts[execState->m_spiBlockId];
        MOT_ASSERT(savedCxt != nullptr);
        if (savedCxt == nullptr) {
            MOT_LOG_ERROR("Cannot end block with exceptions: saved memory context is null");
            RaiseAccessViolationFault();
        }
        (void)MemoryContextSwitchTo(savedCxt);
        MOT_LOG_DEBUG("Restored saved memory context: %s @%p", savedCxt->name, savedCxt);
        execState->m_savedContexts[execState->m_spiBlockId] = nullptr;
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        (void)HandlePGError(jitContext->m_execState, "SPI_restore_connection()");
        signalException = true;
    }
    PG_END_TRY();

    if (signalException) {
        if (jitContext->m_execState->m_sqlState == 0) {
            // note: we could use a more informative error code
            jitContext->m_execState->m_sqlState = ERRCODE_FDW_ERROR;
        }
        SignalException((JitExec::MotJitContext*)jitContext);
    }
}

void JitCleanupBlockAfterException()
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    volatile JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    volatile bool signalException = false;
    volatile MemoryContext origCxt = CurrentMemoryContext;

    MOT_LOG_DEBUG("Cleaning up statement block with exception");
    if (execState->m_spiBlockId == 0) {
        MOT_LOG_ERROR("Cannot cleanup block with exceptions: no block is active");
        RaiseAccessViolationFault();
    }

    PG_TRY();
    {
        --execState->m_spiBlockId;
        SubTransactionId subXid = ::GetCurrentSubTransactionId();
        if (subXid == execState->m_subTxns[execState->m_spiBlockId]) {
            MOT_LOG_DEBUG("Rolling back current sub-transaction #%" PRIu64 ": id %" PRIu64,
                execState->m_openSubTxCount,
                (uint64_t)subXid);
            ::RollbackAndReleaseCurrentSubTransaction();
            --execState->m_openSubTxCount;
        }
        int savedConnectId = execState->m_spiConnectId[execState->m_spiBlockId];
        MOT_LOG_DEBUG("SPI connect id before calling disconnect/restore: depth=%d, saved=%d, actual=%d",
            execState->m_spiBlockId,
            savedConnectId,
            SPI_connectid());
        SPI_disconnect(savedConnectId + 1);
        MOT_LOG_DEBUG("SPI connect id after calling disconnect: %d", SPI_connectid());
        SPI_restore_connection();
        MOT_LOG_DEBUG("SPI connect id after calling restore: %d", SPI_connectid());
        execState->m_spiConnectId[execState->m_spiBlockId] = -1;
        MemoryContext savedCxt = execState->m_savedContexts[execState->m_spiBlockId];
        MOT_ASSERT(savedCxt != nullptr);
        if (savedCxt == nullptr) {
            MOT_LOG_ERROR("Cannot cleanup block with exceptions: saved memory context is null");
            RaiseAccessViolationFault();
        }
        (void)MemoryContextSwitchTo(savedCxt);
        MOT_LOG_DEBUG("Restored saved memory context: %s @%p", savedCxt->name, savedCxt);
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        (void)HandlePGError(jitContext->m_execState, "SPI_restore_connection()");
        signalException = true;
    }
    PG_END_TRY();

    if (signalException) {
        if (jitContext->m_execState->m_sqlState == 0) {
            // note: we could use a more informative error code
            jitContext->m_execState->m_sqlState = ERRCODE_FDW_ERROR;
        }
        SignalException((JitExec::MotJitContext*)jitContext);
    }
}

void JitSetExceptionOrigin(int origin)
{
    JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    execState->m_exceptionOrigin = origin;
}

int JitGetExceptionOrigin()
{
    JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    return execState->m_exceptionOrigin;
}

int* JitGetExceptionOriginRef()
{
    JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    return &execState->m_exceptionOrigin;
}

void JitCleanupBeforeReturn(SubTransactionId initSubTxnId)
{
    // close all open blocks (commit sub-txns and release SPI resources)
    JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    while (execState->m_spiBlockId > 0) {
        JitEndBlockWithExceptions();
        if (LlvmGetExceptionStatus() != 0) {
            MOT_LOG_DEBUG("JitCleanupBeforeReturn(): Exception thrown, aborting");
            return;
        }
    }

    // roll back all open sub-transactions (there shouldn't be by now - this is a safety check)
    SubTransactionId subTxnId = ::GetCurrentSubTransactionId();
    JitReleaseAllSubTransactions(true, subTxnId);
}

static JitExec::JitFunctionExecState* GetFunctionExecStateVerify(int subQueryId)
{
    JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    JitExec::JitFunctionExecState* execState = GetFunctionExecState();
    JitExec::JitFunctionContext* functionContext = (JitExec::JitFunctionContext*)jitContext;
    if (subQueryId >= (int)functionContext->m_SPSubQueryCount) {
        MOT_REPORT_ERROR(MOT_ERROR_INDEX_OUT_OF_RANGE,
            "Execute JIT Function",
            "Attempt to access out-of-range sub-query %d denied: Current function has only %d sub-queries",
            subQueryId,
            (int)functionContext->m_SPSubQueryCount);
        RaiseAccessViolationFault();
    }
    return execState;
}

static void PrepareInvokeParams(JitExec::JitFunctionContext* functionContext,
    JitExec::JitFunctionExecState* functionExecState, JitExec::JitInvokedQueryExecState* invokeExecState,
    int subQueryId)
{
    // pass parameters coming from stored procedure parameters, some of which may be directly passed from the query
    // that invoked the stored procedure
    JitExec::JitQueryContext* invokingContext = (JitExec::JitQueryContext*)functionExecState->m_invokingContext;
    JitExec::JitQueryExecState* invokingExecState = (JitExec::JitQueryExecState*)invokingContext->m_execState;
    JitExec::JitParamInfo* paramInfo = invokingContext->m_invokeParamInfo;
    JitExec::JitCallSite* callSite = &functionContext->m_SPSubQueryList[subQueryId];
    MOT_LOG_DEBUG("Using direct params at %p", invokingExecState->m_directParams);

    // note: stored procedure arguments are NOT necessarily starting from index zero
    for (int i = 0; i < callSite->m_callParamCount; ++i) {
        JitExec::JitCallParamInfo* callParamInfo = &callSite->m_callParamInfo[i];
        int paramId = callParamInfo->m_invokeArgIndex;
        // local variables were already passed by jitted SP, we need to pass only SP parameters into called query
        if (callParamInfo->m_paramKind == JitExec::JitCallParamKind::JIT_CALL_PARAM_ARG) {
            // get SP parameter index from datum index
            MOT_ASSERT(paramId < (int)invokingContext->m_invokeParamCount);
            if (paramId >= (int)invokingContext->m_invokeParamCount) {
                MOT_REPORT_ERROR(MOT_ERROR_INDEX_OUT_OF_RANGE,
                    "JIT Execute Stored Procedure",
                    "Invalid parameter index %d, when invoking sub-query %d (%s) of stored procedure %s",
                    paramId,
                    subQueryId,
                    callSite->m_queryContext->m_queryString,
                    functionContext->m_queryString);
                RaiseAccessViolationFault();
            }
            if (paramInfo[paramId].m_mode == JitExec::JitParamMode::JIT_PARAM_DIRECT) {
                // pass parameter directly from the query that invoked this SP into the called sub-query
                // get invoke query parameter index from SP parameter index
                int pos = paramInfo[paramId].m_index;
                MOT_LOG_DEBUG("Passing direct parameter %d at caller pos %d", i, pos);
                invokeExecState->m_params->params[paramId].value = invokingExecState->m_directParams->params[pos].value;
                invokeExecState->m_params->params[paramId].isnull =
                    invokingExecState->m_directParams->params[pos].isnull;
            } else {
                // pass parameter from the SP itself (since it was modified from INVOKE query into SP)
                MOT_LOG_DEBUG("Passing parameter copy %d", i);
                invokeExecState->m_params->params[paramId].value =
                    functionExecState->m_functionParams->params[paramId].value;
                invokeExecState->m_params->params[paramId].isnull =
                    functionExecState->m_functionParams->params[paramId].isnull;
            }
        } else {
            MOT_LOG_DEBUG("Passing local variable %d (already set)", i);
        }
        if (!invokeExecState->m_params->params[paramId].isnull) {
            DEBUG_PRINT_DATUM("Passing parameter to sub-query",
                invokeExecState->m_params->params[paramId].ptype,
                invokeExecState->m_params->params[paramId].value,
                invokeExecState->m_params->params[paramId].isnull);
        }
    }
}

static void VerifySpiResult(int rc, JitExec::JitCallSite* callSite)
{
    switch (rc) {
        case SPI_OK_SELECT:
        case SPI_OK_SELINTO:
        case SPI_OK_UTILITY:
        case SPI_OK_REWRITTEN:
            AssertEreport(!callSite->m_isModStmt, MOD_MOT, "It should not be mod stmt.");
            break;

        case SPI_OK_INSERT:
        case SPI_OK_UPDATE:
        case SPI_OK_DELETE:
        case SPI_OK_INSERT_RETURNING:
        case SPI_OK_UPDATE_RETURNING:
        case SPI_OK_DELETE_RETURNING:
        case SPI_OK_MERGE:
            AssertEreport(callSite->m_isModStmt, MOD_MOT, "mod stmt is required.");
            break;

        case SPI_ERROR_COPY:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_MOT),
                    errmsg("cannot COPY to/from client in PL/pgSQL")));
            break;

        case SPI_ERROR_TRANSACTION:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_MOT),
                    errmsg("only support commit/rollback transaction statements."),
                    errhint("Use a BEGIN block with an EXCEPTION clause instead of begin/end transaction.")));
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_MOT),
                    errmsg("SPI_execute_plan_with_paramlist failed executing query \"%s\": %s",
                        callSite->m_queryString,
                        SPI_result_code_string(rc))));
            break;
    }

    if (callSite->m_isInto) {
        if (SPI_tuptable == nullptr) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_MOT),
                    errmsg("INTO used with a command that cannot return data")));
        }
    } else {
        if (SPI_tuptable != nullptr) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmodule(MOD_MOT),
                    errmsg("query has no destination for result data"),
                    (rc == SPI_OK_SELECT)
                        ? errhint("If you want to discard the results of a SELECT, use PERFORM instead.")
                        : 0));
        }
    }
}

// based on convert_value_to_string() from pl_exec.cpp
static char* ConvertValueToString(Datum value, Oid valtype)
{
    char* result = nullptr;
    Oid typoutput;
    bool typIsVarlena = false;

    getTypeOutputInfo(valtype, &typoutput, &typIsVarlena);
    result = OidOutputFunctionCall(typoutput, value);

    return result;
}

static Datum CastValue(PLpgSQL_type* typeDesc, Datum value, Oid valueType, bool isNull)
{
    Oid funcId = InvalidOid;
    CoercionPathType coercionPath = COERCION_PATH_NONE;
    Oid requiredType = typeDesc->typoid;
    FmgrInfo* requiredInput = &typeDesc->typinput;
    Oid requiredTypeIoParam = typeDesc->typioparam;
    int32 requiredTypemod = typeDesc->atttypmod;

    if (valueType != requiredType || requiredTypemod != -1) {
        if (!isNull) {
            if (valueType == UNKNOWNOID) {
                valueType = TEXTOID;
                value = DirectFunctionCall1(textin, value);
            }

            // get the implicit cast function from valueType to requiredType
            coercionPath = find_coercion_pathway(requiredType, valueType, COERCION_ASSIGNMENT, &funcId);
            if (funcId != InvalidOid &&
                !(coercionPath == COERCION_PATH_COERCEVIAIO || coercionPath == COERCION_PATH_ARRAYCOERCE)) {
                value = OidFunctionCall1(funcId, value);
                value = pl_coerce_type_typmod(value, requiredType, requiredTypemod);
            } else {
                char* extVal = ConvertValueToString(value, valueType);
                value = InputFunctionCall(requiredInput, extVal, requiredTypeIoParam, requiredTypemod);
                pfree_ext(extVal);
            }
        } else {
            value = InputFunctionCall(requiredInput, NULL, requiredTypeIoParam, requiredTypemod);
        }
    }

    return value;
}

static bool CopyHeapTuple(
    JitExec::JitInvokedQueryExecState* invokeExecState, HeapTuple tuple, TupleDesc tupleDesc, bool switchContext = true)
{
    // switch to memory context of caller
    MemoryContext oldCtx = switchContext ? JitExec::SwitchToSPICallerContext() : nullptr;

    // code adapted from exec_move_row() in pl_exec.cpp
    int tupleDescAttrCount = tupleDesc ? tupleDesc->natts : 0;
    int tupleAttrCount = HeapTupleIsValid(tuple) ? HeapTupleHeaderGetNatts(tuple->t_data, tupleDesc) : 0;
    int resultIndex = 0;
    for (int i = 0; i < tupleDescAttrCount; ++i) {
        // skip dropped columns in destination
        if (tupleDesc->attrs[i].attisdropped) {
            continue;
        }

        bool isNull = true;
        Datum value = (Datum)0;
        if ((i < tupleAttrCount) && !tupleDesc->attrs[i].attisdropped) {
            value = SPI_getbinval(tuple, tupleDesc, i + 1, &isNull);
            if (SPI_result != 0) {
                MOT_LOG_TRACE(
                    "Failed to get datum value from tuple: %s (%u)", SPI_result_code_string(SPI_result), SPI_result);
                return false;
            }
        } else {
            continue;  // skip dropped columns in source
        }
        Oid type = SPI_gettypeid(tupleDesc, i + 1);

        // perform proper type conversion as in exec_assign_value() at pl_exec.cpp
        PLpgSQL_type* resultType = &invokeExecState->m_resultTypes[resultIndex];
        Datum resValue = CastValue(resultType, value, type, isNull);
        resValue = JitExec::CopyDatum(resValue, resultType->typoid, isNull);
        SetSlotValue(invokeExecState->m_resultSlot, resultIndex++, resValue, isNull);
    }

    // restore current memory context
    if (oldCtx) {
        MOT_LOG_TRACE("Switching back to memory context %s @%p", oldCtx->name, oldCtx);
        (void)MemoryContextSwitchTo(oldCtx);
    }
    return true;
}

static void CopyTupleTableSlot(JitExec::JitInvokedQueryExecState* invokeExecState, TupleTableSlot* tuple)
{
    // we might get a minimal tuple from sort result
    if (tuple->tts_mintuple != nullptr) {
        HeapTuple htup = heap_tuple_from_minimal_tuple(tuple->tts_mintuple);
        if (!HeapTupleIsValid(htup)) {
            ereport(ERROR, (errmodule(MOD_MOT), errcode(ERRCODE_PLPGSQL_ERROR), errmsg("Failed to form heap tuple")));
        }
        if (!CopyHeapTuple(invokeExecState, htup, tuple->tts_tupleDescriptor, false)) {
            ereport(ERROR, (errmodule(MOD_MOT), errcode(ERRCODE_PLPGSQL_ERROR), errmsg("Failed to copy heap tuple")));
        }
        return;
    }

    // switch to memory context of caller (except for when processing minimal tuple of sort query result)
    MemoryContext oldCtx = JitExec::SwitchToSPICallerContext();

    // copy slot values to memory context of caller
    TupleDesc tupDesc = tuple->tts_tupleDescriptor;
    int resultIndex = 0;
    for (int i = 0; i < tupDesc->natts; ++i) {
        // skip dropped columns in destination
        if (tupDesc->attrs[i].attisdropped) {
            continue;
        }

        bool isNull = tuple->tts_isnull[i];
        Datum value = tuple->tts_values[i];
        Oid type = tuple->tts_tupleDescriptor->attrs[i].atttypid;

        // perform proper type conversion as in exec_assign_value() at pl_exec.cpp
        PLpgSQL_type* resultType = &invokeExecState->m_resultTypes[resultIndex];
        Datum resValue = CastValue(resultType, value, type, isNull);
        resValue = JitExec::CopyDatum(resValue, resultType->typoid, isNull);
        SetSlotValue(invokeExecState->m_resultSlot, resultIndex++, resValue, isNull);
    }

    // restore current memory context
    if (oldCtx) {
        MOT_LOG_TRACE("Switching back to memory context %s @%p", oldCtx->name, oldCtx);
        (void)MemoryContextSwitchTo(oldCtx);
    }
}

inline MOT::RC HandleSubQueryError(JitExec::JitExecState* execState, bool isJittable, int spiConnectId)
{
    const char* operation = isJittable ? "executing jitted SP sub-query" : "executing non-jitted SP sub-query";
    return HandlePGError(execState, operation, spiConnectId);
}

static bool RecompilePlanIfNeeded(
    JitExec::JitFunctionContext* jitContext, int subQueryId, JitExec::JitInvokedQueryExecState* invokeExecState)
{
    if ((invokeExecState->m_plan == nullptr) || needRecompilePlan(invokeExecState->m_plan)) {
        MOT_LOG_TRACE("SPI plan needs recompilation");
        // clean up old plan
        if (invokeExecState->m_plan != nullptr) {
            MOT_LOG_TRACE("Destroying old plan");
            (void)SPI_freeplan(invokeExecState->m_plan);
            invokeExecState->m_plan = nullptr;
        }

        // regenerate new plan
        MOT_LOG_TRACE("Regenerating new plan");
        invokeExecState->m_plan = JitExec::PrepareSpiPlan(jitContext, subQueryId, &invokeExecState->m_expr);
        if (invokeExecState->m_plan == nullptr) {
            MOT_LOG_TRACE("Failed to prepare SPI plan for non-jittable sub-query %u", subQueryId);
            return false;
        }

        // do what PG does...
        if (ENABLE_CN_GPC && g_instance.plan_cache->CheckRecreateSPICachePlan(invokeExecState->m_plan)) {
            g_instance.plan_cache->RecreateSPICachePlan(invokeExecState->m_plan);
        }
    }

    // setup parameter list for correct parsing (in case we hit parsing in RevalidateCachedQuery)
    JitExec::JitFunctionExecState* functionExecState = GetFunctionExecStateVerify(subQueryId);
    invokeExecState->m_params->parserSetupArg = (void*)invokeExecState->m_expr;
    functionExecState->m_estate.cur_expr = invokeExecState->m_expr;  // required by plpgsql_param_fetch
    if (invokeExecState->m_expr != nullptr) {
        invokeExecState->m_expr->func = functionExecState->m_function;
        invokeExecState->m_expr->func->cur_estate = &functionExecState->m_estate;
    }

    return true;
}

static void PrintResultSlot(TupleTableSlot* resultSlot)
{
    if (TTS_EMPTY(resultSlot)) {
        MOT_LOG_DEBUG("Query result is empty");
    } else {
        MOT_LOG_BEGIN(MOT::LogLevel::LL_DEBUG, "Query result slot:");
        for (int i = 0; i < resultSlot->tts_tupleDescriptor->natts; ++i) {
            MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "Datum[%d]: ", i);
            Oid type = resultSlot->tts_tupleDescriptor->attrs[i].atttypid;
            JitExec::PrintDatum(MOT::LogLevel::LL_DEBUG, type, resultSlot->tts_values[i], resultSlot->tts_isnull[i]);
            MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "\n");
        }
        MOT_LOG_END(MOT::LogLevel::LL_DEBUG);
    }
}

static inline const char* GetDatumCString(Datum value)
{
    bytea* txt = DatumGetByteaP(value);
    return VARDATA(txt);
}

static inline void ReportErrorFromExecState(JitExec::JitExecState* execState)
{
    void* detailDatum = DatumGetPointer(execState->m_errorDetail);
    void* hintDatum = DatumGetPointer(execState->m_errorHint);
    if ((detailDatum != nullptr) && (hintDatum != nullptr)) {
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(execState->m_sqlState),
                errmsg("%s", GetDatumCString(execState->m_errorMessage)),
                errdetail("%s", GetDatumCString(execState->m_errorDetail)),
                errhint("%s", GetDatumCString(execState->m_errorHint))));
    } else if (detailDatum != nullptr) {
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(execState->m_sqlState),
                errmsg("%s", GetDatumCString(execState->m_errorMessage)),
                errdetail("%s", GetDatumCString(execState->m_errorDetail))));
    } else if (hintDatum != nullptr) {
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(execState->m_sqlState),
                errmsg("%s", GetDatumCString(execState->m_errorMessage)),
                errhint("%s", GetDatumCString(execState->m_errorHint))));
    } else {
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(execState->m_sqlState),
                errmsg("%s", GetDatumCString(execState->m_errorMessage))));
    }
}

static int ExecNonJitSubQueryPlan(
    JitExec::JitCallSite* callSite, JitExec::JitInvokedQueryExecState* invokeExecState, int tcount, uint32_t subQueryId)
{
    // NOTE: we need to check here for a special use case: this is a jittable query that is executed once as non-
    // jittable, and during this execution it was revalidated and executed as jittable through Executor path.
    // In this case we need to check whether an error occurred, and if so report it properly

    // in order to make sure order of events described below is correct, we need to make sure error state is reset
    if (callSite->m_queryContext != nullptr) {
        JitExec::JitExecState* execState = callSite->m_queryContext->m_execState;
        if (execState != nullptr) {
            ResetErrorState(execState);
        }
    }

    // execute the query as non-jittable through SPI
    // as explained above, it might be revalidated and transform into a jittable query, and get executed as jittable
    // through the Executor path
    int rc = SPI_execute_plan_with_paramlist(invokeExecState->m_plan, invokeExecState->m_params, false, tcount);

    // first check if this was a non-jittable execution of an invalid jittable query
    if (callSite->m_queryContext != nullptr) {
        // we avoid checking the validity of the query (so we have indication the jittable execution took place), since
        // the valid bit might be overwritten (context invalidated) after jitted execution.
        // instead, we just check the SQL state in the execution state - if it is not zero then definitely a jitted
        // execution took place and it failed (the use case of non-failed jitted execution is of no interest here)
        JitExec::JitExecState* execState = callSite->m_queryContext->m_execState;
        if ((execState != nullptr) && (execState->m_sqlState != 0)) {  // the execution ended with error
            // query was executed as jittable and an error occurred, so we throw the same error again
            ReportErrorFromExecState(execState);
        }
    }

    // next check return code from SPI execution
    invokeExecState->m_spiResult = rc;
    VerifySpiResult(rc, callSite);
    rc = 0;  // we are good, report success for now

    // now handle result tuple if needed
    invokeExecState->m_tuplesProcessed = SPI_processed;
    MOT_LOG_TRACE("Sub-query %u returned %u tuples processed: %s", subQueryId, SPI_processed, callSite->m_queryString);
    if ((callSite->m_queryCmdType == CMD_SELECT) && (SPI_processed > 0)) {
        SPITupleTable* tupTab = SPI_tuptable;
        if (!CopyHeapTuple(invokeExecState, tupTab->vals[0], tupTab->tupdesc)) {
            rc = SPI_ERROR_OPUNKNOWN;
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_PLPGSQL_ERROR),
                    errmsg("Failed to copy result tuple to caller's context for non-jittable sub-query %u: %s",
                        subQueryId,
                        callSite->m_queryString)));
        }
        if (MOT_CHECK_DEBUG_LOG_LEVEL()) {
            PrintResultSlot(invokeExecState->m_resultSlot);
        }
    }

    // cleanup
    if (SPI_tuptable != nullptr) {
        SPI_freetuptable(SPI_tuptable);
        SPI_tuptable = nullptr;
    }
    return rc;
}

static int JitExecNonJitSubQuery(int subQueryId, int tcount)
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile int result = 0;
    volatile bool signalException = false;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    volatile bool isSpiPushed = false;
    volatile int spiConnectId = SPI_connectid();

    JitExec::JitFunctionExecState* functionExecState = GetFunctionExecStateVerify(subQueryId);
    JitExec::JitFunctionContext* functionContext = (JitExec::JitFunctionContext*)jitContext;
    JitExec::JitCallSite* callSite = &functionContext->m_SPSubQueryList[subQueryId];
    JitExec::JitInvokedQueryExecState* invokeExecState = &functionExecState->m_invokedQueryExecState[subQueryId];
    bool planSourceHadContext = false;
    if (invokeExecState->m_plan != nullptr && invokeExecState->m_plan->plancache_list != NIL) {
        CachedPlanSource* planSource = (CachedPlanSource*)linitial(invokeExecState->m_plan->plancache_list);
        if (planSource->mot_jit_context != nullptr) {
            planSourceHadContext = true;
        }
    }

    // get a valid query string
    const char* queryString = callSite->m_queryString;
    if ((queryString == nullptr) && (callSite->m_queryContext != nullptr)) {
        queryString = callSite->m_queryContext->m_queryString;
    }
    MOT_ASSERT(queryString != nullptr);

    MOT_LOG_DEBUG("Executing non-jittable sub-query: %s", queryString)
    jitContext->m_execState->m_sqlState = 0;
    PG_TRY();
    {
        // run sub-query to completion, in strict execution it is expected to return only one tuple, otherwise none.
        MOT_LOG_DEBUG("Invoking SP non-jittable sub-query %d with params: %p", subQueryId, invokeExecState->m_params);

        // we push SPI context only if we expect to call another SP,
        // otherwise we continue working in current memory context
        if (callSite->m_isUnjittableInvoke) {
            MOT_LOG_DEBUG("Calling SPI_push()")
            SPI_push();
            isSpiPushed = true;
        }

        // ensure we have a prepared SPI plan
        if (!RecompilePlanIfNeeded(functionContext, subQueryId, invokeExecState)) {
            MOT_LOG_TRACE("Failed to recompile SPI plan for non-jittable sub-query %u: %s", subQueryId, queryString);
            // throw ereport to get proper error handling
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_PLPGSQL_ERROR),
                    errmsg("Failed to recompile SPI plan for non-jittable sub-query %u: %s",
                        subQueryId,
                        callSite->m_queryString)));
        } else if (invokeExecState->m_plan == nullptr) {
            MOT_LOG_TRACE("Found unprepared SPI plan for non-jittable sub-query %u: %s", subQueryId, queryString);
            // throw ereport to get proper error handling
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_PLPGSQL_ERROR),
                    errmsg("Found unprepared SPI plan for non-jittable sub-query %u: %s", subQueryId, queryString)));
        } else {
            // prepare invoke parameters
            PrepareInvokeParams(functionContext, functionExecState, invokeExecState, subQueryId);

            // now execute the plan
            (void)::ExecClearTuple(invokeExecState->m_resultSlot);
            result = ExecNonJitSubQueryPlan(callSite, invokeExecState, tcount, subQueryId);
            if (result != 0) {
                // throw ereport to get proper error handling
                MOT_LOG_TRACE("Failed to execute non-jittable sub-query %u: %s", subQueryId, queryString);
                if (jitContext->m_execState->m_sqlState == 0) {
                    ereport(ERROR,
                        (errmodule(MOD_MOT),
                            errcode(ERRCODE_PLPGSQL_ERROR),
                            errmsg("Failed to execute non-jittable sub-query %u: %s", subQueryId, queryString)));
                } else {
                    signalException = true;
                }
            }
            MOT_LOG_DEBUG("Finished executing non-jittable SP sub-query: %s", queryString);
        }

        // ATTENTION: it is possible that we arrived here through a jittable sub-query in intermediate state
        // (e.g. pending compile), and due to revalidation the jit context was deleted in the plan, so we must update
        // the call site, otherwise we will crash during context destruction
        CachedPlanSource* planSource = (CachedPlanSource*)linitial(invokeExecState->m_plan->plancache_list);
        if ((planSourceHadContext) && (planSource->mot_jit_context == nullptr) &&
            (callSite->m_queryContext != nullptr)) {
            // transform into non-jittable sub-query
            // the query string is safe to duplicate, as it comes from the JIT source
            callSite->m_queryContext = nullptr;
            if (callSite->m_queryString == nullptr) {
                callSite->m_queryString = JitExec::DupString(queryString, JitExec::JitContextUsage::JIT_CONTEXT_LOCAL);
                if (callSite->m_queryString == nullptr) {
                    ereport(ERROR,
                        (errmodule(MOD_MOT),
                            errmsg("Failed to allocate local memory for query string: %s", queryString)));
                }
            }
        }
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(origCxt);
        HandleSubQueryError(jitContext->m_execState, false, spiConnectId);
        isSpiPushed = false;  // SPI connection already restored to original state in HandlePGError()
        signalException = true;
    }
    PG_END_TRY();

    u_sess->mot_cxt.jit_txn->FinishStatement();

    if (isSpiPushed) {
        SPI_pop();
    }

    if (signalException) {
        SignalException((JitExec::MotJitContext*)jitContext);
    }

    // ATTENTION: original SPI result is passed to caller for further inspection through the execution state of the
    // backing context
    return result;
}

void JitReleaseNonJitSubQueryResources(int subQueryId)
{
    JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    JitExec::JitFunctionContext* functionContext = (JitExec::JitFunctionContext*)jitContext;
    JitExec::JitCallSite* callSite = &functionContext->m_SPSubQueryList[subQueryId];
    JitExec::JitQueryContext* subQueryContext = (JitExec::JitQueryContext*)callSite->m_queryContext;
    if (subQueryContext == nullptr) {
        if (SPI_tuptable != nullptr) {
            SPI_freetuptable(SPI_tuptable);
            SPI_tuptable = nullptr;
        }
    }
}

int JitExecSubQuery(int subQueryId, int tcount)
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    volatile JitExec::JitFunctionExecState* functionExecState = GetFunctionExecStateVerify(subQueryId);
    JitExec::JitFunctionContext* functionContext = (JitExec::JitFunctionContext*)jitContext;
    JitExec::JitCallSite* callSite = &functionContext->m_SPSubQueryList[subQueryId];
    volatile JitExec::JitQueryContext* subQueryContext = (JitExec::JitQueryContext*)callSite->m_queryContext;

    MOT_LOG_DEBUG("Executing sub-query %d with tcount %d", subQueryId, tcount);
    if ((subQueryContext == nullptr) || IsJitContextPendingCompile((JitExec::MotJitContext*)subQueryContext) ||
        IsJitContextErrorCompile((JitExec::MotJitContext*)subQueryContext)) {
        MOT_LOG_DEBUG("Executing non-jittable sub-query %u: %s", subQueryId, callSite->m_queryString);
        return JitExecNonJitSubQuery(subQueryId, tcount);
    }

    // if context is invalid it will be revalidated when we acquire locks, and if that fails we will transform into
    // non-jittable sub-query
    MOT_LOG_DEBUG("Executing jittable sub-query %u: %s", subQueryId, subQueryContext->m_queryString);

    // ATTENTION: current sub-query id must be exposed so that result collection can take place
    functionExecState->m_currentSubQueryId = subQueryId;

    // function parameters already passed into sub-query parameters by previous instructions

    // ATTENTION: sub-query result slot is maintained in sub-query exec state

    // ATTENTION: current call might generate ereport that will end entire function execution, so we need to catch it
    // and translate it so it an be caught by exception handler block in jitted code
    jitContext->m_execState->m_sqlState = 0;
    subQueryContext->m_execState->m_sqlState = 0;
    MOT_LOG_DEBUG("(Before sub-query) Current sub-transaction #%" PRIu64 ": id %" PRIu64,
        functionExecState->m_openSubTxCount,
        (uint64_t)::GetCurrentSubTransactionId());

    // in case we hit parsing during revalidate, we must make sure we have up-to-date parameters
    volatile JitExec::JitInvokedQueryExecState* invokeExecState =
        &functionExecState->m_invokedQueryExecState[subQueryId];
    invokeExecState->m_params->parserSetupArg = (void*)invokeExecState->m_expr;
    functionExecState->m_estate.cur_expr = invokeExecState->m_expr;  // required by plpgsql_param_fetch
    if (invokeExecState->m_expr != nullptr) {
        invokeExecState->m_expr->func = functionExecState->m_function;
        invokeExecState->m_expr->func->cur_estate = (PLpgSQL_execstate*)&functionExecState->m_estate;
    }
    MOT_LOG_DEBUG("Invoking SP sub-query %d with params: %p", subQueryId, invokeExecState->m_params);
    invokeExecState->m_spiResult = SPI_OK_SELECT;

    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile int result = 0;
    volatile bool signalException = false;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile SubTransactionId subXid = ::GetCurrentSubTransactionId();
    volatile CachedPlan* cplan = nullptr;
    volatile bool planTransformed = false;
    volatile bool planExecuteAsNonJit = false;
    volatile bool pushedActiveSnapshot = false;
    volatile bool isSpiPushed = false;
    volatile int spiConnectId = SPI_connectid();

    // run sub-query to completion, in strict execution it is expected to return only one tuple, otherwise none.
    PG_TRY();
    {
        // acquire plan locks before execution - this may trigger JIT context revalidation (this is good)
        // attention: we do not need full analysis, just revalidation and parse
        if (subQueryContext->m_commandType != JitExec::JIT_COMMAND_INVOKE) {
            // we need to make sure the plan source points to the jit context to be executed, so revalidation occurs
            const char* queryString = subQueryContext->m_jitSource->m_queryString;
            MOT_ASSERT(list_length(invokeExecState->m_plan->plancache_list) == 1);
            CachedPlanSource* planSource = (CachedPlanSource*)linitial(invokeExecState->m_plan->plancache_list);
            MOT_ASSERT(planSource->mot_jit_context == nullptr || (planSource->mot_jit_context == subQueryContext));
            if (planSource->mot_jit_context == nullptr) {
                planSource->mot_jit_context = (JitExec::MotJitContext*)subQueryContext;
            }

            cplan = SPI_plan_get_cached_plan(invokeExecState->m_plan);
            if (cplan == nullptr) {
                MOT_LOG_ERROR("Failed to get cached plan");
            }

            // it is possible at this point that query got disqualified and is not jittable now
            if (planSource->mot_jit_context == nullptr) {
                // we transform this sub-query into a non-jittable one and then execute it
                // the query string is safe to duplicate, as it comes from the JIT source
                MOT_LOG_TRACE("Jitted sub-query became unjittable after revalidation: %s", queryString);
                callSite->m_queryString = JitExec::DupString(queryString, JitExec::JitContextUsage::JIT_CONTEXT_LOCAL);
                if (callSite->m_queryString == nullptr) {
                    ereport(ERROR,
                        (errmodule(MOD_MOT),
                            errmsg("Failed to allocate local memory for query string: %s", queryString)));
                }
                callSite->m_queryContext = nullptr;  // already deleted during revalidation
                planTransformed = true;
            } else if (!JitExec::IsJitContextValid(planSource->mot_jit_context)) {
                MOT_LOG_TRACE("Jitted sub-query revalidation failed, executing as non-jittable: %s", queryString);
                planExecuteAsNonJit = true;
            }
        } else {
            if (IsJitContextDoneCompile((JitExec::MotJitContext*)subQueryContext)) {
                MOT_LOG_TRACE("Revalidate sub-query %d after compile-done: %s",
                    subQueryId,
                    subQueryContext->m_jitSource->m_queryString);
                if (!RevalidateJitContext((JitExec::MotJitContext*)subQueryContext)) {
                    MOT_LOG_TRACE("Revalidate sub-query %d after compile-done failed, executing as non-jittable: %s",
                        subQueryId,
                        subQueryContext->m_jitSource->m_queryString);
                    planExecuteAsNonJit = true;
                }
            }
        }

        if (planTransformed || planExecuteAsNonJit) {
            if (planTransformed) {
                MOT_ASSERT(callSite->m_queryString != nullptr);
                MOT_LOG_TRACE("Executing transformed SP query as non-jittable: %s", callSite->m_queryString);
            } else {
                MOT_LOG_TRACE(
                    "Executing invalid SP query as non-jittable: %s", callSite->m_queryContext->m_queryString);
            }
            result = JitExecNonJitSubQuery(subQueryId, tcount);
        } else {
            PushActiveSnapshot(GetTransactionSnapshot());
            pushedActiveSnapshot = false;

            // we push SPI context only if we expect to call another SP,
            // otherwise we continue working in current memory context
            if (subQueryContext->m_commandType == JitExec::JIT_COMMAND_INVOKE) {
                SPI_push();
                isSpiPushed = true;
            }

            // run sub-query to completion, in strict execution it is expected to return only one tuple, otherwise none.
            JitExec::JitInvokedQueryExecState* subQueryExecState =
                &functionExecState->m_invokedQueryExecState[subQueryId];
            MOT_LOG_DEBUG("Invoking SP sub-query %d with params: %p", subQueryId, subQueryExecState->m_params);

            // pass parameters coming from stored procedure parameters, some of which may be directly passed from the
            // query that invoked the stored procedure
            PrepareInvokeParams(
                functionContext, (JitExec::JitFunctionExecState*)functionExecState, subQueryExecState, subQueryId);

            // setup invoking context
            subQueryContext->m_execState->m_invokingContext = functionContext;

            // make sure result is cleared - so caller can tell zero tuples were retrieved
            (void)::ExecClearTuple(subQueryExecState->m_resultSlot);
            JitResetScan((JitExec::MotJitContext*)subQueryContext);  // signal new scan is starting
            if ((subQueryContext->m_commandType == JitExec::JIT_COMMAND_RANGE_SELECT) ||
                (subQueryContext->m_commandType == JitExec::JIT_COMMAND_RANGE_JOIN)) {
                unsigned long nprocessed = 0;
                // save only first slot for strict execution, the rest can be overwritten on work slot
                TupleTableSlot* slot = subQueryExecState->m_resultSlot;
                bool finish = false;
                while (!finish) {
                    uint64_t tuplesProcessed = 0;
                    result = JitExec::JitExecQuery((JitExec::MotJitContext*)subQueryContext,
                        subQueryExecState->m_params,
                        slot,
                        &tuplesProcessed,
                        &subQueryExecState->m_scanEnded);
                    if (subQueryExecState->m_scanEnded || (tuplesProcessed == 0) || (result != 0)) {
                        // raise flag so that next round we will bail out (current tuple still must be reported to user)
                        finish = true;
                    }
                    if (tuplesProcessed > 0) {
                        ++nprocessed;
                        if ((tcount != 0) && (nprocessed == (unsigned long)tcount)) {
                            finish = true;
                        }
                    }
                    // copy first slot if needed and switch to work slot from now on
                    if (slot == subQueryExecState->m_resultSlot) {
                        // we need to copy slot if there is a minimal tuple or SPI push was called and the tuple is not
                        // empty
                        if ((slot->tts_mintuple != nullptr) || (isSpiPushed && !TTS_EMPTY(slot))) {
                            CopyTupleTableSlot(subQueryExecState, subQueryExecState->m_resultSlot);
                        }
                    }
                    slot = subQueryExecState->m_workSlot;
                }
                subQueryExecState->m_tuplesProcessed = nprocessed;
                if ((nprocessed > 0) && (result == MOT::RC_LOCAL_ROW_NOT_FOUND)) {
                    // if the first rounds were fine and retrieved rows, but the last iteration found no row, then we
                    // should not report any error - this is normal
                    result = MOT::RC_OK;
                }
            } else {
                // in this case we disregard tcount, since this is a point query, or even insert/delete/update
                result = JitExec::JitExecQuery((JitExec::MotJitContext*)subQueryContext,
                    subQueryExecState->m_params,
                    subQueryExecState->m_resultSlot,
                    &subQueryExecState->m_tuplesProcessed,
                    &subQueryExecState->m_scanEnded);
            }

            CommandCounterIncrement();
            PopActiveSnapshot();
            pushedActiveSnapshot = false;

            MOT_LOG_DEBUG("Finished executing jitted SP sub-query with %" PRIu64 " tuples and result: %s",
                subQueryExecState->m_tuplesProcessed,
                MOT::RcToString((MOT::RC)result));
            if ((result != MOT::RC_OK) && (result != MOT::RC_LOCAL_ROW_NOT_FOUND)) {
                // throw ereport to get proper error handling
                MOT_LOG_TRACE(
                    "Failed to execute jittable sub-query %u: %s", subQueryId, subQueryContext->m_queryString);
                if (subQueryContext->m_execState->m_sqlState == 0) {
                    ereport(ERROR,
                        (errmodule(MOD_MOT),
                            errcode(ERRCODE_PLPGSQL_ERROR),
                            errmsg("Failed to execute jittable sub-query %u: %s",
                                subQueryId,
                                subQueryContext->m_queryString)));
                }
            } else if (result != MOT::RC_LOCAL_ROW_NOT_FOUND) {
                if (MOT_CHECK_DEBUG_LOG_LEVEL()) {
                    PrintResultSlot(subQueryExecState->m_resultSlot);
                }
            }
            // cleanup SPI stack if needed before handling any error
            if (isSpiPushed) {
                SPI_pop();
                isSpiPushed = false;
            }
        }

        // release plan - locks are released by resource owner during commit!
        if (cplan != nullptr) {
            ReleaseCachedPlan((CachedPlan*)cplan, invokeExecState->m_plan->saved);
        }
    }
    PG_CATCH();
    {
        // first restore memory context
        CurrentMemoryContext = origCxt;

        // release cached plan or planner locks
        if (cplan != nullptr) {
            ReleaseCachedPlan((CachedPlan*)cplan, invokeExecState->m_plan->saved);
        }

        // print error
        HandleSubQueryError(subQueryContext->m_execState, true, spiConnectId);
        isSpiPushed = false;  // SPI connection already restored to original state in HandlePGError()
        signalException = true;

        if (pushedActiveSnapshot) {
            PopActiveSnapshot();
        }

        // in debug mode we validate sub-tx state
        SubTransactionId endSubXid = ::GetCurrentSubTransactionId();
        if (subXid != endSubXid) {
            MOT_LOG_WARN("Invalid sub-tx state: entered with %" PRIu64 ", exited with " PRIu64, subXid, endSubXid);
        }
        MOT_ASSERT(subXid == endSubXid);
        // rollback all open sub-txns until the one we started with
    }
    PG_END_TRY();

    MOT_ASSERT(!isSpiPushed);
    // pull-up error info
    if (!planTransformed && !planExecuteAsNonJit) {
        PullUpErrorInfo(subQueryContext->m_execState, jitContext->m_execState);
        u_sess->mot_cxt.jit_txn->FinishStatement();
    }

    // reset sub-query id
    functionExecState->m_currentSubQueryId = -1;

    if (!planTransformed && !planExecuteAsNonJit && signalException) {
        SignalException((JitExec::MotJitContext*)jitContext);
    }

    // return execution result
    return result;
}

int JitGetTuplesProcessed(int subQueryId)
{
    JitExec::JitFunctionExecState* functionExecState = GetFunctionExecStateVerify(subQueryId);
    JitExec::JitInvokedQueryExecState* invokeExecState = &functionExecState->m_invokedQueryExecState[subQueryId];
    return invokeExecState->m_tuplesProcessed;
}

Datum JitGetSubQuerySlotValue(int subQueryId, int tupleColId)
{
    JitExec::JitFunctionExecState* functionExecState = GetFunctionExecStateVerify(subQueryId);
    TupleTableSlot* slot = functionExecState->m_invokedQueryExecState[subQueryId].m_resultSlot;
    return GetSlotValue(slot, tupleColId);
}

int JitGetSubQuerySlotIsNull(int subQueryId, int tupleColId)
{
    JitExec::JitFunctionExecState* functionExecState = GetFunctionExecStateVerify(subQueryId);
    TupleTableSlot* slot = functionExecState->m_invokedQueryExecState[subQueryId].m_resultSlot;
    return GetSlotIsNull(slot, tupleColId);
}

HeapTuple JitGetSubQueryResultHeapTuple(int subQueryId)
{
    HeapTuple heapTuple = (HeapTuple)DatumGetPointer(JitGetSubQuerySlotValue(subQueryId, 0));
    MOT_LOG_DEBUG("Retrieving heap tuple for sub-query %d at %p", subQueryId, heapTuple);
    return heapTuple;
}

Datum JitGetHeapTupleValue(HeapTuple tuple, int subQueryId, int columnId, int* isNull)
{
    MOT_ASSERT(u_sess->mot_cxt.jit_context->m_contextType == JitExec::JitContextType::JIT_CONTEXT_TYPE_FUNCTION);
    JitExec::JitFunctionContext* jitContext = (JitExec::JitFunctionContext*)u_sess->mot_cxt.jit_context;
    MOT_ASSERT(subQueryId < (int)jitContext->m_SPSubQueryCount);
    JitExec::MotJitContext* subContext = jitContext->m_SPSubQueryList[subQueryId].m_queryContext;
    MOT_ASSERT(subContext->m_contextType == JitExec::JitContextType::JIT_CONTEXT_TYPE_QUERY);
    MOT_ASSERT(subContext->m_commandType == JitExec::JIT_COMMAND_INVOKE);
    TupleDesc tupDesc = ((JitExec::JitQueryContext*)subContext)->m_invokeContext->m_resultTupDesc;

    MOT_LOG_DEBUG("Retrieving sub-query %d heap tuple %p attribute %d using tuple descriptor %p",
        subQueryId,
        tuple,
        columnId,
        tupDesc);
    bool isNullBool = false;
    Datum value = SPI_getbinval(tuple, tupDesc, columnId + 1, &isNullBool);
    *isNull = isNullBool ? 1 : 0;
    DEBUG_PRINT_DATUM("Datum: ", tupDesc->attrs[columnId]->atttypid, value, isNullBool);
    return value;
}

int JitGetSpiResult(int subQueryId)
{
    JitExec::JitFunctionExecState* functionExecState = GetFunctionExecStateVerify(subQueryId);
    JitExec::JitInvokedQueryExecState* invokeExecState = &functionExecState->m_invokedQueryExecState[subQueryId];
    int rc = invokeExecState->m_spiResult;
    MOT_LOG_DEBUG("Returning sub-query SPI result %d: %s", rc, SPI_result_code_string(rc));
    return rc;
}

inline Datum JitConvertViaStringImpl(Datum value, Oid resultType, Oid targetType, int typeMod)
{
    // convert result type to target type via string
    // convert result to string using type output function
    Oid typeOutput;
    bool typeIsVarlena = false;
    getTypeOutputInfo(resultType, &typeOutput, &typeIsVarlena);
    char* valueStr = OidOutputFunctionCall(typeOutput, value);
    MOT_LOG_DEBUG("JitConvertViaString(): Intermediate str result = %s", valueStr);

    // convert string to target type using type input function
    Oid typeInput;
    Oid typeIoParam;
    getTypeInputInfo(targetType, &typeInput, &typeIoParam);
    Datum result = OidInputFunctionCall(typeInput, valueStr, typeIoParam, typeMod);
    pfree_ext(valueStr);

    return result;
}

Datum JitConvertViaString(Datum value, Oid resultType, Oid targetType, int typeMod)
{
    MOT_LOG_DEBUG("JitConvertViaString() resultType = %d, targetType=%d, typeMod=%d", resultType, targetType, typeMod);

    volatile Datum result = PointerGetDatum(nullptr);
    volatile bool signalException = false;
    volatile JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    MOT_ASSERT(jitContext->m_contextType == JitExec::JitContextType::JIT_CONTEXT_TYPE_FUNCTION);
    volatile JitExec::JitFunctionExecState* execState = (JitExec::JitFunctionExecState*)jitContext->m_execState;
    volatile MemoryContext origCxt = (execState->m_spiBlockId >= 1)
                                         ? MemoryContextSwitchTo(execState->m_savedContexts[0])
                                         : JitExec::SwitchToSPICallerContext();
    volatile int spiConnectId = SPI_connectid();
    PG_TRY();
    {
        result = JitConvertViaStringImpl(value, resultType, targetType, typeMod);
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        HandlePGFunctionError((JitExec::MotJitContext*)jitContext, spiConnectId);
        signalException = true;
    }
    PG_END_TRY();
    if (origCxt != nullptr) {
        (void)MemoryContextSwitchTo(origCxt);
    }

    if (signalException) {
        SignalException((JitExec::MotJitContext*)jitContext);
    }
    return result;
}

static Datum JitCastValueImpl(Datum value, Oid sourceType, Oid targetType, int typeMod, CoercionPathType coercePath,
    Oid funcId, CoercionPathType coercePath2, Oid funcId2, int nargs, bool& exceptionSignaled)
{
    // temporary calculation made on current memory context
    volatile Datum result = PointerGetDatum(nullptr);
    volatile bool signalException = false;
    volatile JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile int spiConnectId = SPI_connectid();
    PG_TRY();
    {
        MOT_ASSERT((targetType != sourceType) || (typeMod != -1));
        if ((funcId != InvalidOid) &&
            !(coercePath == COERCION_PATH_COERCEVIAIO || coercePath == COERCION_PATH_ARRAYCOERCE)) {
            // call convert once
            result = OidFunctionCall1(funcId, value);
            if ((coercePath2 == COERCION_PATH_FUNC) && OidIsValid(funcId2)) {
                if (nargs == 1) {
                    result = OidFunctionCall1(funcId2, result);
                } else if (nargs == 2) {
                    result = OidFunctionCall2(funcId2, result, typeMod);
                } else if (nargs == 3) {
                    result = OidFunctionCall3(funcId2, result, typeMod, 0);
                } else {
                    ereport(ERROR,
                        (errmodule(MOD_MOT),
                            errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY),
                            errmsg("Unexpected number of arguments for conversion function: %u", funcId)));
                }
            }
        } else {
            result = JitConvertViaStringImpl(value, sourceType, targetType, typeMod);
        }
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        HandlePGFunctionError((JitExec::MotJitContext*)jitContext, spiConnectId);
        signalException = true;
    }
    PG_END_TRY();
    if (origCxt != nullptr) {
        (void)MemoryContextSwitchTo(origCxt);
    }

    if (signalException) {
        SignalException((JitExec::MotJitContext*)jitContext);
        exceptionSignaled = true;
    }

    return result;
}

Datum JitCastValue(Datum value, Oid sourceType, Oid targetType, int typeMod, int coercePath, Oid funcId,
    int coercePath2, Oid funcId2, int nargs, int typeByVal)
{
    volatile Datum result = PointerGetDatum(nullptr);
    bool exceptionSignaled = false;

    MOT_LOG_DEBUG("Casting value from type %u to type %u by typeMod %d, typeByVal = %d",
        sourceType,
        targetType,
        typeMod,
        typeByVal);

    // make the cast
    if ((targetType != sourceType) || (typeMod != -1)) {
        result = JitCastValueImpl(value,
            sourceType,
            targetType,
            typeMod,
            (CoercionPathType)coercePath,
            funcId,
            (CoercionPathType)coercePath2,
            funcId2,
            nargs,
            exceptionSignaled);

        // check if exception occurred
        if (exceptionSignaled) {
            return result;
        }
    } else {
        // direct assignment - no cast needed (impossible when typeByVal == 0, because in this case we would not emit
        // call to JitCastValue in the first place...)
        MOT_ASSERT(!typeByVal);
        result = value;
    }

    // if type is passed by value we are done
    if (typeByVal) {
        return result;
    }

    // copy datum to caller's memory context
    volatile JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    MOT_ASSERT(jitContext->m_contextType == JitExec::JitContextType::JIT_CONTEXT_TYPE_FUNCTION);
    volatile JitExec::JitFunctionExecState* execState = (JitExec::JitFunctionExecState*)jitContext->m_execState;
    volatile MemoryContext origCxt = (execState->m_spiBlockId >= 1)
                                         ? MemoryContextSwitchTo(execState->m_savedContexts[0])
                                         : JitExec::SwitchToSPICallerContext();
    volatile int spiConnectId = SPI_connectid();
    volatile bool signalException = false;
    PG_TRY();
    {
        result = JitExec::CopyDatum(result, targetType, false);
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        HandlePGFunctionError((JitExec::MotJitContext*)jitContext, spiConnectId);
        signalException = true;
    }
    PG_END_TRY();
    if (origCxt != nullptr) {
        (void)MemoryContextSwitchTo(origCxt);
    }

    if (signalException) {
        SignalException((JitExec::MotJitContext*)jitContext);
    }

    return result;
}

void JitSaveErrorInfo(Datum errorMessage, int sqlState, Datum sqlStateString)
{
    JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    MOT_ASSERT(jitContext != nullptr);
    JitExec::JitExecState* execState = jitContext->m_execState;
    MOT_ASSERT(execState != nullptr);
    execState->m_errorMessage = errorMessage;
    execState->m_sqlState = sqlState;
    execState->m_sqlStateString = sqlStateString;
}

Datum JitGetErrorMessage()
{
    JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    MOT_ASSERT(jitContext != nullptr);
    JitExec::JitExecState* execState = jitContext->m_execState;
    MOT_ASSERT(execState != nullptr);
    return execState->m_errorMessage;
}

int JitGetSqlState()
{
    JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    MOT_ASSERT(jitContext != nullptr);
    JitExec::JitExecState* execState = jitContext->m_execState;
    MOT_ASSERT(execState != nullptr);
    return execState->m_sqlState;
}

Datum JitGetSqlStateString()
{
    JitExec::MotJitContext* jitContext = u_sess->mot_cxt.jit_context;
    MOT_ASSERT(jitContext != nullptr);
    JitExec::JitExecState* execState = jitContext->m_execState;
    MOT_ASSERT(execState != nullptr);
    return execState->m_sqlStateString;
}

Datum JitGetDatumIsNotNull(int isNull)
{
    return BoolGetDatum(!isNull ? true : false);
}

void EmitProfileData(uint32_t functionId, uint32_t regionId, int beginRegion)
{
    JitExec::JitProfiler::GetInstance()->EmitProfileData(functionId, regionId, beginRegion != 0);
}
}  // extern "C"
