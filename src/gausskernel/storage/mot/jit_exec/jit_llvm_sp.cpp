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
 * jit_llvm_sp.cpp
 *    LLVM-jitted stored-procedure execution.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_llvm_sp.cpp
 *
 * -------------------------------------------------------------------------
 */
/*
 * ATTENTION: Be sure to include jit_llvm_sp.h before anything else because of gscodegen.h
 * See jit_llvm_sp.h for more details.
 */
#include "jit_llvm_sp.h"
#include "global.h"
#include "postgres.h"
#include "catalog/pg_operator.h"
#include "utils/fmgroids.h"
#include "nodes/parsenodes.h"
#include "storage/ipc.h"
#include "nodes/pg_list.h"
#include "utils/elog.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "catalog/pg_aggregate.h"
#include "executor/spi.h"
#include "nodes/nodeFuncs.h"
#include "utils/syscache.h"
#include "storage/mot/jit_exec.h"
#include "knl/knl_session.h"
#include "parser/parse_coerce.h"
#include "utils/plpgsql.h"
#include "fmgr.h"
#include "funcapi.h"

#include "jit_util.h"
#include "jit_source_map.h"

#include "mot_engine.h"
#include "utilities.h"
#include "debug_utils.h"
#include "jit_plan.h"
#include "jit_plan_sp.h"
#include "mm_global_api.h"
#include "jit_llvm_util.h"
#include "jit_llvm_funcs.h"
#include "jit_profiler.h"

namespace JitExec {
DECLARE_LOGGER(JitLlvmSp, JitExec)

static bool ProcessFunctionAction(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_function* function);
static bool ProcessStatementBlock(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_block* stmt);
static bool ProcessStatementAssign(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_assign* stmt);
static bool ProcessStatementIf(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_if* stmt);
static bool ProcessStatementGoto(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_goto* stmt);
static bool ProcessStatementCase(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_case* stmt);
static bool ProcessStatementLoop(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_loop* stmt);
static bool ProcessStatementWhile(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_while* stmt);
static bool ProcessStatementForI(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_fori* stmt);
static bool ProcessStatementForS(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_fors* stmt);
static bool ProcessStatementForC(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_forc* stmt);
static bool ProcessStatementForEach(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_foreach_a* stmt);
static bool ProcessStatementExit(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_exit* stmt);
static bool ProcessStatementReturn(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_return* stmt);
static bool ProcessStatementReturnNext(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_return_next* stmt);
static bool ProcessStatementReturnQuery(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_return_query* stmt);
static bool ProcessStatementRaise(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_raise* stmt);
static bool ProcessStatementExecSql(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_execsql* stmt);
static bool ProcessStatementPerform(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_perform* stmt);
static bool ProcessStatementDynExecute(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_dynexecute* stmt);
static bool ProcessStatementDynForS(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_dynfors* stmt);
static bool ProcessStatementGetDiag(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_getdiag* stmt);
static bool ProcessStatementOpen(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_open* stmt);
static bool ProcessStatementFetch(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_fetch* stmt);
static bool ProcessStatementClose(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_close* stmt);

static llvm::Value* GetLocalOrParamByVarNo(JitLlvmFunctionCodeGenContext* ctx, int varno);
static llvm::Value* GetIsNullLocalOrParamByVarNo(JitLlvmFunctionCodeGenContext* ctx, int varno);
static bool ProcessStatementLabel(JitLlvmFunctionCodeGenContext* ctx, const char* label, const char* defaultName);
static llvm::Value* ProcessExpr(JitLlvmFunctionCodeGenContext* ctx, Expr* expr, Oid* resultType);
static bool IsSimpleExprStatic(JitLlvmFunctionCodeGenContext* ctx, Expr* expr);
static bool DefineBlockLocalVar(JitLlvmFunctionCodeGenContext* ctx, int varIndex);
static llvm::Value* ExecSubQuery(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_expr* expr, int tcount, bool strict,
    bool mod, bool into, int* subQueryId, bool isPerform = false);
static llvm::Value* ProcessExpr(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_expr* expr, Oid* resultType);
static bool AssignValue(JitLlvmFunctionCodeGenContext* ctx, int varNo, PLpgSQL_expr* expr);
static void CheckThrownException(JitLlvmFunctionCodeGenContext* ctx);

static llvm::Value* AddGetExprIsNull(JitLlvmFunctionCodeGenContext* ctx);

static bool AddSetSqlStateAndErrorMessage(JitLlvmFunctionCodeGenContext* ctx, const char* errorMessage, int sqlState);

static bool AssignScalarValue(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_var* target, llvm::Value* assignee,
    llvm::Value* assigneeIsNull, llvm::Value* value, llvm::Value* isNull, Oid sourceType);

#ifdef IssueDebugLog
#undef IssueDebugLog
#endif

#ifdef MOT_JIT_DEBUG
inline llvm::Value* StringToValue(JitLlvmFunctionCodeGenContext* ctx, const char* str)
{
    llvm::Value* strVal = ctx->m_builder->CreateGlobalStringPtr(str);
    return strVal;
}

inline llvm::Value* ValueToInt32(JitLlvmFunctionCodeGenContext* ctx, llvm::Value* value)
{
    llvm::Value* int32Val = value;
    if (value->getType()->getIntegerBitWidth() < 32) {
        int32Val = ctx->m_builder->CreateCast(llvm::Instruction::CastOps::SExt, value, ctx->INT32_T);
    } else if (value->getType()->getIntegerBitWidth() > 32) {
        int32Val = ctx->m_builder->CreateCast(llvm::Instruction::CastOps::Trunc, value, ctx->INT32_T);
    }
    return int32Val;
}

inline void IssueDebugLogImpl(JitLlvmFunctionCodeGenContext* ctx, const char* function, const char* msg)
{
    llvm::Value* functionPtr = StringToValue(ctx, function);
    llvm::Value* msgPtr = StringToValue(ctx, msg);

    // a debug call may sometimes be emitted after terminator (return, branch, etc.), so we need to inject it before
    // the terminator
    llvm::Instruction* terminator = ctx->m_builder->GetInsertBlock()->getTerminator();
    if (terminator == nullptr) {
        llvm_util::AddFunctionCall(ctx, ctx->m_debugLogFunc, functionPtr, msgPtr, nullptr);
    } else {
        llvm_util::InsertFunctionCall(terminator, ctx, ctx->m_debugLogFunc, functionPtr, msgPtr, nullptr);
    }
}

inline void IssueDebugLogIntVarImpl(JitLlvmFunctionCodeGenContext* ctx, const char* msg, llvm::Value* arg)
{
    llvm::Value* msgPtr = StringToValue(ctx, msg);
    llvm::Value* argInt32Val = ValueToInt32(ctx, arg);

    // a debug call may sometimes be emitted after terminator (return, branch, etc.), so we need to inject it before
    // the terminator
    llvm::Instruction* terminator = ctx->m_builder->GetInsertBlock()->getTerminator();
    if (terminator == nullptr) {
        llvm_util::AddFunctionCall(ctx, ctx->m_debugLogIntFunc, msgPtr, argInt32Val, nullptr);
    } else {
        llvm_util::InsertFunctionCall(terminator, ctx, ctx->m_debugLogIntFunc, msgPtr, argInt32Val, nullptr);
    }
}

inline void IssueDebugLogIntImpl(JitLlvmFunctionCodeGenContext* ctx, const char* msg, int arg)
{
    IssueDebugLogIntVarImpl(ctx, msg, JIT_CONST_INT32(arg));
}

inline void IssueDebugLogStringImpl(JitLlvmFunctionCodeGenContext* ctx, const char* msg, const char* arg)
{
    llvm::Value* msgPtr = StringToValue(ctx, msg);
    llvm::Value* argPtr = StringToValue(ctx, arg);

    // a debug call may sometimes be emitted after terminator (return, branch, etc.), so we need to inject it before
    // the terminator
    llvm::Instruction* terminator = ctx->m_builder->GetInsertBlock()->getTerminator();
    if (terminator == nullptr) {
        llvm_util::AddFunctionCall(ctx, ctx->m_debugLogStringFunc, msgPtr, argPtr, nullptr);
    } else {
        llvm_util::InsertFunctionCall(terminator, ctx, ctx->m_debugLogStringFunc, msgPtr, argPtr, nullptr);
    }
}

inline void IssueDebugLogStringDatumVarImpl(JitLlvmFunctionCodeGenContext* ctx, const char* msg, llvm::Value* arg)
{
    llvm::Value* msgPtr = StringToValue(ctx, msg);

    // a debug call may sometimes be emitted after terminator (return, branch, etc.), so we need to inject it before
    // the terminator
    llvm::Instruction* terminator = ctx->m_builder->GetInsertBlock()->getTerminator();
    if (terminator == nullptr) {
        llvm_util::AddFunctionCall(ctx, ctx->m_debugLogStringDatumFunc, msgPtr, arg, nullptr);
    } else {
        llvm_util::InsertFunctionCall(terminator, ctx, ctx->m_debugLogStringDatumFunc, msgPtr, arg, nullptr);
    }
}

inline void IssueDebugLogStringDatumImpl(JitLlvmFunctionCodeGenContext* ctx, const char* msg, Datum arg)
{
    IssueDebugLogStringDatumVarImpl(ctx, msg, JIT_CONST_INT64(arg));
}

inline void IssueDebugLogDatumImpl(
    JitLlvmFunctionCodeGenContext* ctx, const char* msg, llvm::Value* datum, llvm::Value* isNull, int type)
{
    llvm::Value* msgPtr = StringToValue(ctx, msg);
    llvm::ConstantInt* typeValue = JIT_CONST_INT32(type);
    llvm::Value* isNullInt32Val = ValueToInt32(ctx, isNull);

    // a debug call may sometimes be emitted after terminator (return, branch, etc.), so we need to inject it before
    // the terminator
    llvm::Instruction* terminator = ctx->m_builder->GetInsertBlock()->getTerminator();
    if (terminator == nullptr) {
        llvm_util::AddFunctionCall(ctx, ctx->m_debugLogDatumFunc, msgPtr, datum, isNullInt32Val, typeValue, nullptr);
    } else {
        llvm_util::InsertFunctionCall(
            terminator, ctx, ctx->m_debugLogDatumFunc, msgPtr, datum, isNullInt32Val, typeValue, nullptr);
    }
}

#define JIT_DEBUG_LOG(msg) IssueDebugLogImpl((JitLlvmFunctionCodeGenContext*)ctx, __FUNCTION__, msg)
#define JIT_DEBUG_LOG_INT(msg, arg) IssueDebugLogIntImpl((JitLlvmFunctionCodeGenContext*)ctx, msg, arg)
#define JIT_DEBUG_LOG_INT_VAR(msg, arg) IssueDebugLogIntVarImpl((JitLlvmFunctionCodeGenContext*)ctx, msg, arg)
#define JIT_DEBUG_LOG_STRING(msg, arg) IssueDebugLogStringImpl((JitLlvmFunctionCodeGenContext*)ctx, msg, arg)
#define JIT_DEBUG_LOG_STRING_DATUM(msg, arg) IssueDebugLogStringDatumImpl((JitLlvmFunctionCodeGenContext*)ctx, msg, arg)
#define JIT_DEBUG_LOG_STRING_DATUM_VAR(msg, arg) \
    IssueDebugLogStringDatumVarImpl((JitLlvmFunctionCodeGenContext*)ctx, msg, arg)
#define JIT_DEBUG_LOG_DATUM(msg, datum, isNull, type) \
    IssueDebugLogDatumImpl((JitLlvmFunctionCodeGenContext*)ctx, msg, datum, isNull, type)

#define JIT_DEBUG_LOG_VARNO(varNo)                                                                          \
    do {                                                                                                    \
        PLpgSQL_datum* target = ctx->_compiled_function->datums[varNo];                                     \
        if (target->dtype == PLPGSQL_DTYPE_VAR) {                                                           \
            JIT_DEBUG_LOG_INT("Scalar Varno %d", varNo);                                                    \
            llvm::Value* value = GetLocalOrParamByVarNo(ctx, varNo);                                        \
            llvm::Value* valueIsNull = GetIsNullLocalOrParamByVarNo(ctx, varNo);                            \
            llvm::Value* loadedValue = ctx->m_builder->CreateLoad(ctx->DATUM_T, value, true);               \
            llvm::Value* loadedIsNull = ctx->m_builder->CreateLoad(ctx->BOOL_T, valueIsNull, true);         \
            JIT_DEBUG_LOG_DATUM(                                                                            \
                "Scalar Varno value", loadedValue, loadedIsNull, ((PLpgSQL_var*)target)->datatype->typoid); \
        } else {                                                                                            \
            JIT_DEBUG_LOG_INT("Non-scalar varno %d", varNo);                                                \
        }                                                                                                   \
    } while (0)

#else
#define JIT_DEBUG_LOG(msg)
#define JIT_DEBUG_LOG_INT(msg, arg)
#define JIT_DEBUG_LOG_INT_VAR(msg, arg)
#define JIT_DEBUG_LOG_STRING(msg, arg)
#define JIT_DEBUG_LOG_STRING_DATUM(msg, arg)
#define JIT_DEBUG_LOG_STRING_DATUM_VAR(msg, arg)
#define JIT_DEBUG_LOG_DATUM(msg, datum, isNull, type)
#define JIT_DEBUG_LOG_VARNO(varNo)
#endif

#ifdef MOT_JIT_DEBUG
inline void DefineDebugLog(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_debugLogFunc = llvm_util::DefineFunction(module, ctx->VOID_T, "debugLog", ctx->STR_T, ctx->STR_T, nullptr);
}
inline void DefineDebugLogInt(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_debugLogIntFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "debugLogInt", ctx->STR_T, ctx->INT32_T, nullptr);
}
inline void DefineDebugLogString(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_debugLogStringFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "debugLogString", ctx->STR_T, ctx->STR_T, nullptr);
}
inline void DefineDebugLogStringDatum(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_debugLogStringDatumFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "debugLogStringDatum", ctx->STR_T, ctx->INT64_T, nullptr);
}
inline void DefineDebugLogDatum(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_debugLogDatumFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "debugLogDatum", ctx->STR_T, ctx->DATUM_T, ctx->INT32_T, ctx->INT32_T, nullptr);
}
#endif

inline void DefineGetCurrentSubTransactionId(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getCurrentSubTransactionIdFunc =
        llvm_util::DefineFunction(module, ctx->INT64_T, "JitGetCurrentSubTransactionId", nullptr);
}

inline void DefineBeginBlockWithExceptions(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_beginBlockWithExceptionsFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "JitBeginBlockWithExceptions", nullptr);
}

inline void DefineEndBlockWithExceptions(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_endBlockWithExceptionsFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "JitEndBlockWithExceptions", nullptr);
}

inline void DefineCleanupBlockAfterException(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_cleanupBlockAfterExceptionFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "JitCleanupBlockAfterException", nullptr);
}

inline void DefineGetExceptionOrigin(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getExceptionOriginFunc = llvm_util::DefineFunction(module, ctx->INT32_T, "JitGetExceptionOrigin", nullptr);
}

inline void DefineSetExceptionOrigin(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_setExceptionOriginFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "JitSetExceptionOrigin", ctx->INT32_T, nullptr);
}

inline void DefineCleanupBeforeReturn(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_cleanupBeforeReturnFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "JitCleanupBeforeReturn", ctx->INT64_T, nullptr);
}

inline void DefineConvertViaString(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_convertViaStringFunc = llvm_util::DefineFunction(
        module, ctx->DATUM_T, "JitConvertViaString", ctx->DATUM_T, ctx->INT32_T, ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void DefineCastValue(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_castValueFunc = llvm_util::DefineFunction(module,
        ctx->DATUM_T,
        "JitCastValue",
        ctx->DATUM_T,
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

inline void DefineSaveErrorInfo(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_saveErrorInfoFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "JitSaveErrorInfo", ctx->DATUM_T, ctx->INT32_T, ctx->DATUM_T, nullptr);
}

inline void DefineGetErrorMessage(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getErrorMessageFunc = llvm_util::DefineFunction(module, ctx->DATUM_T, "JitGetErrorMessage", nullptr);
}

inline void DefineGetSqlState(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getSqlStateFunc = llvm_util::DefineFunction(module, ctx->INT32_T, "JitGetSqlState", nullptr);
}

inline void DefineGetSqlStateString(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getSqlStateStringFunc = llvm_util::DefineFunction(module, ctx->DATUM_T, "JitGetSqlStateString", nullptr);
}

inline void DefineGetDatumIsNotNull(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getDatumIsNotNullFunc =
        llvm_util::DefineFunction(module, ctx->DATUM_T, "JitGetDatumIsNotNull", ctx->INT32_T, nullptr);
}

inline void DefineGetExprIsNull(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getExprIsNullFunc = llvm_util::DefineFunction(module, ctx->INT32_T, "GetExprIsNull", nullptr);
}

inline void DefineSetExprIsNull(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_setExprIsNullFunc = llvm_util::DefineFunction(module, ctx->VOID_T, "SetExprIsNull", ctx->INT32_T, nullptr);
}

inline void DefineGetExprCollation(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getExprCollationFunc = llvm_util::DefineFunction(module, ctx->INT32_T, "GetExprCollation", nullptr);
}

inline void DefineSetExprCollation(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_setExprCollationFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "SetExprCollation", ctx->INT32_T, nullptr);
}

inline void DefineExecClearTuple(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_execClearTupleFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "execClearTuple", ctx->TupleTableSlotType->getPointerTo(), nullptr);
}

inline void DefineExecStoreVirtualTuple(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_execStoreVirtualTupleFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "execStoreVirtualTuple", ctx->TupleTableSlotType->getPointerTo(), nullptr);
}

inline void DefineGetParamAtRef(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getParamAtRefFunc = llvm_util::DefineFunction(module,
        ctx->DATUM_T->getPointerTo(),
        "JitGetParamAtRef",
        ctx->ParamListInfoDataType->getPointerTo(),
        ctx->INT32_T,
        nullptr);
}

inline void DefineIsParamNullRef(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_isParamNullRefFunc = llvm_util::DefineFunction(module,
        ctx->BOOL_T->getPointerTo(),
        "JitIsParamNullRef",
        ctx->ParamListInfoDataType->getPointerTo(),
        ctx->INT32_T,
        nullptr);
}

inline void DefineIsCompositeResult(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_isCompositeResultFunc = llvm_util::DefineFunction(
        module, ctx->INT32_T, "IsCompositeResult", ctx->TupleTableSlotType->getPointerTo(), nullptr);
}

inline void DefineCreateResultDatums(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_createResultDatumsFunc =
        llvm_util::DefineFunction(module, ctx->DATUM_T->getPointerTo(), "CreateResultDatums", nullptr);
}

inline void DefineCreateResultNulls(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_createResultNullsFunc =
        llvm_util::DefineFunction(module, ctx->INT8_T->getPointerTo(), "CreateResultNulls", nullptr);
}

inline void DefineSetResultValue(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_setResultValueFunc = llvm_util::DefineFunction(module,
        ctx->VOID_T,
        "SetResultValue",
        ctx->DATUM_T->getPointerTo(),
        ctx->INT8_T->getPointerTo(),
        ctx->INT32_T,
        ctx->DATUM_T,
        ctx->INT32_T,
        nullptr);
}

inline void DefineCreateResultHeapTuple(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_createResultHeapTupleFunc = llvm_util::DefineFunction(module,
        ctx->DATUM_T,
        "CreateResultHeapTuple",
        ctx->DATUM_T->getPointerTo(),
        ctx->INT8_T->getPointerTo(),
        nullptr);
}

inline void DefineSetSlotValue(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_setSlotValueFunc = llvm_util::DefineFunction(module,
        ctx->VOID_T,
        "SetSlotValue",
        ctx->TupleTableSlotType->getPointerTo(),
        ctx->INT32_T,
        ctx->DATUM_T,
        ctx->INT32_T,
        nullptr);
}

inline void DefineSetSPSubQueryParamValue(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_setSPSubQueryParamValueFunc = llvm_util::DefineFunction(module,
        ctx->VOID_T,
        "SetSPSubQueryParamValue",
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->DATUM_T,
        ctx->INT32_T,
        nullptr);
}

inline void DefineExecuteSubQuery(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_executeSubQueryFunc =
        llvm_util::DefineFunction(module, ctx->INT32_T, "JitExecSubQuery", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void DefineReleaseNonJitSubQueryResources(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_releaseNonJitSubQueryResourcesFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "JitReleaseNonJitSubQueryResources", ctx->INT32_T, nullptr);
}

inline void DefineGetTuplesProcessed(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getTuplesProcessedFunc =
        llvm_util::DefineFunction(module, ctx->INT32_T, "JitGetTuplesProcessed", ctx->INT32_T, nullptr);
}

inline void DefineGetSubQuerySlotValue(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getSubQuerySlotValueFunc =
        llvm_util::DefineFunction(module, ctx->DATUM_T, "JitGetSubQuerySlotValue", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void DefineGetSubQuerySlotIsNull(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getSubQuerySlotIsNullFunc = llvm_util::DefineFunction(
        module, ctx->INT32_T, "JitGetSubQuerySlotIsNull", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void DefineGetSubQueryResultHeapTuple(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getSubQueryResultHeapTupleFunc = llvm_util::DefineFunction(
        module, ctx->INT8_T->getPointerTo(), "JitGetSubQueryResultHeapTuple", ctx->INT32_T, nullptr);
}

inline void DefineGetHeapTupleValue(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getHeapTupleValueFunc = llvm_util::DefineFunction(module,
        ctx->DATUM_T,
        "JitGetHeapTupleValue",
        ctx->INT8_T->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T->getPointerTo(),
        nullptr);
}

inline void DefineGetSpiResult(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getSpiResultFunc = llvm_util::DefineFunction(module, ctx->INT32_T, "JitGetSpiResult", ctx->INT32_T, nullptr);
}

inline void DefineSetTpProcessed(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_setTpProcessedFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "setTpProcessed", ctx->INT64_T->getPointerTo(), ctx->INT64_T, nullptr);
}

inline void DefineSetScanEnded(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_setScanEndedFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "setScanEnded", ctx->INT32_T->getPointerTo(), ctx->INT32_T, nullptr);
}

inline void DefineGetConstAt(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getConstAtFunc = llvm_util::DefineFunction(module, ctx->DATUM_T, "GetConstAt", ctx->INT32_T, nullptr);
}

inline void DefineAbortFunction(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_abortFunctionFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "JitAbortFunction", ctx->INT32_T, nullptr);
}

inline void DefineLlvmClearExceptionStack(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_llvmClearExceptionStackFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "LlvmClearExceptionStack", nullptr);
}

inline void DefineEmitProfileData(JitLlvmFunctionCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_emitProfileDataFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "EmitProfileData", ctx->INT32_T, ctx->INT32_T, ctx->INT32_T, nullptr);
}

/** @brief Define all LLVM prototypes. */
void InitCodeGenContextFuncs(JitLlvmFunctionCodeGenContext* ctx)
{
    llvm::Module* module = ctx->m_codeGen->module();

    // define all function calls
#ifdef MOT_JIT_DEBUG
    DefineDebugLog(ctx, module);
    DefineDebugLogInt(ctx, module);
    DefineDebugLogString(ctx, module);
    DefineDebugLogStringDatum(ctx, module);
    DefineDebugLogDatum(ctx, module);
#endif
    DefineGetCurrentSubTransactionId(ctx, module);
    DefineBeginBlockWithExceptions(ctx, module);
    DefineEndBlockWithExceptions(ctx, module);
    DefineCleanupBlockAfterException(ctx, module);
    DefineGetExceptionOrigin(ctx, module);
    DefineSetExceptionOrigin(ctx, module);
    DefineCleanupBeforeReturn(ctx, module);
    DefineConvertViaString(ctx, module);
    DefineCastValue(ctx, module);
    DefineSaveErrorInfo(ctx, module);
    DefineGetErrorMessage(ctx, module);
    DefineGetSqlState(ctx, module);
    DefineGetSqlStateString(ctx, module);
    DefineGetDatumIsNotNull(ctx, module);
    DefineGetExprIsNull(ctx, module);
    DefineSetExprIsNull(ctx, module);
    DefineGetExprCollation(ctx, module);
    DefineSetExprCollation(ctx, module);
    DefineExecClearTuple(ctx, module);
    DefineExecStoreVirtualTuple(ctx, module);
    DefineGetParamAtRef(ctx, module);
    DefineIsParamNullRef(ctx, module);
    DefineIsCompositeResult(ctx, module);
    DefineCreateResultDatums(ctx, module);
    DefineCreateResultNulls(ctx, module);
    DefineSetResultValue(ctx, module);
    DefineCreateResultHeapTuple(ctx, module);
    DefineSetSlotValue(ctx, module);
    DefineSetSPSubQueryParamValue(ctx, module);
    DefineExecuteSubQuery(ctx, module);
    DefineReleaseNonJitSubQueryResources(ctx, module);
    DefineGetTuplesProcessed(ctx, module);
    DefineGetSubQuerySlotValue(ctx, module);
    DefineGetSubQuerySlotIsNull(ctx, module);
    DefineGetSubQueryResultHeapTuple(ctx, module);
    DefineGetHeapTupleValue(ctx, module);
    DefineGetSpiResult(ctx, module);
    DefineSetTpProcessed(ctx, module);
    DefineSetScanEnded(ctx, module);
    DefineGetConstAt(ctx, module);
    DefineAbortFunction(ctx, module);
    DefineLlvmClearExceptionStack(ctx, module);
    DefineEmitProfileData(ctx, module);
}

/** @brief Define all LLVM used types synonyms. */
void InitCodeGenContextTypes(JitLlvmFunctionCodeGenContext* ctx)
{
    llvm::LLVMContext& context = ctx->m_codeGen->context();

    // PG types
    ctx->ParamListInfoDataType = llvm::StructType::create(context, "ParamListInfoData");
    ctx->TupleTableSlotType = llvm::StructType::create(context, "TupleTableSlot");
    ctx->NumericDataType = llvm::StructType::create(context, "NumericData");
    ctx->VarCharType = llvm::StructType::create(context, "VarChar");
    ctx->BpCharType = llvm::StructType::create(context, "BpChar");

    // MOT types
    ctx->RowType = llvm::StructType::create(context, "Row");
}

/** @brief Initializes a context for compilation. */
static bool InitLlvmFunctionCodeGenContext(JitLlvmFunctionCodeGenContext* ctx, GsCodeGen* codeGen,
    GsCodeGen::LlvmBuilder* builder, PLpgSQL_function* function, ReturnSetInfo* returnSetInfo)
{
    llvm_util::InitLlvmCodeGenContext(ctx, codeGen, builder);
    InitCodeGenContextTypes(ctx);
    InitCodeGenContextFuncs(ctx);

    ctx->m_constCount = 0;
    size_t allocSize = sizeof(Const) * MOT_JIT_MAX_CONST;
    ctx->m_constValues = (Const*)MOT::MemSessionAlloc(allocSize);
    if (ctx->m_constValues == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Compile",
            "Failed to allocate %u bytes for constant array during code-generation context",
            allocSize);
        return false;
    }

    allocSize = sizeof(bool) * MOT_JIT_MAX_CONST;
    ctx->m_selfManaged = (bool*)MOT::MemSessionAlloc(allocSize);
    if (ctx->m_selfManaged == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Compile",
            "Failed to allocate %u bytes for Boolean array during code-generation context",
            allocSize);
        MOT::MemSessionFree(ctx->m_constValues);
        return false;
    }

    // we need an execution state for preparing a plan (see pl_exec.cpp)
    allocSize = sizeof(PLpgSQL_execstate);
    PLpgSQL_execstate* estate = (PLpgSQL_execstate*)MOT::MemSessionAlloc(allocSize);
    if (estate == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Compile",
            "Failed to allocate %u bytes for parser execution state",
            (unsigned)allocSize);
        MOT::MemSessionFree(ctx->m_selfManaged);
        MOT::MemSessionFree(ctx->m_constValues);
        return false;
    }

    // setup function execution state (used during plan preparation)
    plpgsql_estate_setup(estate, function, returnSetInfo);
    function->cur_estate = estate;
    ctx->_compiled_function = function;

    // reserve space for all parameters and locals
    ctx->m_datums.resize(function->ndatums);
    ctx->m_datumNulls.resize(function->ndatums);

    ctx->m_usingMagicVars = true;
    ctx->m_exceptionBlockCount = 0;
    ctx->m_processedBlock = nullptr;

    return true;
}

/** @brief Destroys a compilation context. */
static void DestroyLlvmFunctionCodeGenContext(JitLlvmFunctionCodeGenContext* ctx)
{
    if (ctx) {
        // cleanup execution state
        if (ctx->_compiled_function && ctx->_compiled_function->cur_estate) {
            plpgsql_destroy_econtext(ctx->_compiled_function->cur_estate);
            exec_eval_cleanup(ctx->_compiled_function->cur_estate);
            ctx->_compiled_function->cur_estate = nullptr;
        }
        if (ctx->m_constValues != nullptr) {
            for (uint32_t i = 0; i < ctx->m_constCount; ++i) {
                if (ctx->m_selfManaged[i]) {
                    MOT::MemSessionFree(DatumGetPointer(ctx->m_constValues[i].constvalue));
                }
            }
            MOT::MemSessionFree(ctx->m_constValues);
        }
        if (ctx->m_selfManaged != nullptr) {
            MOT::MemSessionFree(ctx->m_selfManaged);
        }
        if (ctx->m_resultTupDesc != nullptr) {
            FreeTupleDesc(ctx->m_resultTupDesc);
            ctx->m_resultTupDesc = nullptr;
        }
        if (ctx->m_rowTupDesc != nullptr) {
            FreeTupleDesc(ctx->m_rowTupDesc);
            ctx->m_rowTupDesc = nullptr;
        }
    }
    llvm_util::DestroyLlvmCodeGenContext(ctx);
}

static bool ValueEquals(Datum lhs, Datum rhs, int type)
{
    switch (type) {
        case BOOLOID:
            return DatumGetBool(DirectFunctionCall2(booleq, lhs, rhs));

        case CHAROID:
            return DatumGetBool(DirectFunctionCall2(chareq, lhs, rhs));

        case INT1OID:
            return DatumGetBool(DirectFunctionCall2(int1eq, lhs, rhs));

        case INT2OID:
            return DatumGetBool(DirectFunctionCall2(int2eq, lhs, rhs));

        case INT4OID:
            return DatumGetBool(DirectFunctionCall2(int4eq, lhs, rhs));

        case INT8OID:
            return DatumGetBool(DirectFunctionCall2(int8eq, lhs, rhs));

        case TIMESTAMPOID:
            return DatumGetBool(DirectFunctionCall2(timestamp_eq, lhs, rhs));

        case DATEOID:
            return DatumGetBool(DirectFunctionCall2(date_eq, lhs, rhs));

        case FLOAT4OID:
            return DatumGetBool(DirectFunctionCall2(float4eq, lhs, rhs));

        case FLOAT8OID:
            return DatumGetBool(DirectFunctionCall2(float8eq, lhs, rhs));

        case VARCHAROID:
        case BPCHAROID:
            return DatumGetBool(DirectFunctionCall2(bpchareq, lhs, rhs));

        case TEXTOID:
            return DatumGetBool(DirectFunctionCall2(texteq, lhs, rhs));

        case BYTEAOID:
            return DatumGetBool(DirectFunctionCall2(byteaeq, lhs, rhs));

        case TIMESTAMPTZOID: {
            Datum lhsTimestamp = DirectFunctionCall1(timestamptz_timestamp, lhs);
            Datum rhsTimestamp = DirectFunctionCall1(timestamptz_timestamp, rhs);
            return DatumGetBool(DirectFunctionCall2(timestamp_eq, lhsTimestamp, rhsTimestamp));
        }

        case INTERVALOID:
            return DatumGetBool(DirectFunctionCall2(interval_eq, lhs, rhs));

        case TINTERVALOID:
            return DatumGetBool(DirectFunctionCall2(tintervaleq, lhs, rhs));

        case NUMERICOID:
            return DatumGetBool(DirectFunctionCall2(numeric_eq, lhs, rhs));

        default:
            return false;
    }
}

static int FindConstValue(Const* constValues, uint32_t count, Datum value, Oid type, bool isNull)
{
    for (uint32_t i = 0; i < count; ++i) {
        if (constValues[i].consttype == type) {
            // if both are null then we have a match
            if (constValues[i].constisnull && isNull) {
                return (int)i;
            }
            // if only one is null then we definitely don't have a match
            if (constValues[i].constisnull || isNull) {
                continue;
            }
            if (ValueEquals(constValues[i].constvalue, value, type)) {
                return (int)i;
            }
        }
    }
    return -1;
}

static int AllocateConstId(JitLlvmFunctionCodeGenContext* ctx, Oid type, Datum value, bool isNull,
    bool selfManaged = false, bool* pooled = nullptr)
{
    int res = FindConstValue(ctx->m_constValues, ctx->m_constCount, value, type, isNull);
    if (res >= 0) {
        if (pooled != nullptr) {
            *pooled = true;
        }
        return res;
    }

    if (ctx->m_constCount == MOT_JIT_MAX_CONST) {
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
            "JIT Compile",
            "Cannot allocate constant identifier, reached limit of %u",
            ctx->m_constCount);
        return -1;
    }

    res = ctx->m_constCount++;
    ctx->m_constValues[res].consttype = type;
    ctx->m_constValues[res].constisnull = isNull;
    if (isNull || IsPrimitiveType(type) || selfManaged) {
        ctx->m_constValues[res].constvalue = value;
        ctx->m_selfManaged[res] = selfManaged;
    } else {
        // attention: we must clone a non-primitive, not managed, not null datum, since the plan from which the datum
        // stems can be deleted when processing a simple expression
        if (!CloneDatum(value, type, &ctx->m_constValues[res].constvalue, JIT_CONTEXT_LOCAL)) {
            MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT, "JIT Compile", "Failed to clone datum");
            return -1;
        }
        ctx->m_selfManaged[res] = true;
    }

    MOT_LOG_TRACE("Allocated constant id: %d", res);
    DEBUG_PRINT_DATUM("Allocated constant", type, value, isNull);
    return res;
}

inline llvm::Type* GetOidType(JitLlvmFunctionCodeGenContext* ctx, Oid resultType)
{
    // all types are datum
    if (IsTypeSupported(resultType)) {
        return ctx->DATUM_T;
    } else if (resultType == UNKNOWNOID) {
        MOT_LOG_TRACE("Allowing special type UNKNOWNOID");
        return ctx->DATUM_T;
    } else {
        MOT_LOG_TRACE("Cannot get LLVM type by OID %d", (int)resultType);
        return nullptr;
    }
}

static llvm::Value* DefineVarLocal(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_var* var)
{
    // we expect scalar variable type to be predefined
    llvm::Value* result = nullptr;
    MOT_LOG_TRACE("Defining local scalar variable %s with type %d", var->refname, var->datatype->typoid);
    llvm::Type* type = GetOidType(ctx, var->datatype->typoid);
    if (type == nullptr) {
        MOT_LOG_TRACE("Cannot define local variable for scalar variable %s: type not found", var->refname);
    } else {
        // all local variables are defined as DATUM type
        // ATTENTION: sometimes locals have identical name, so we allow duplicate name
        result = ctx->m_builder->CreateAlloca(type, 0, nullptr, var->refname);
    }

    return result;
}

static bool DefineBlockLocalVar(JitLlvmFunctionCodeGenContext* ctx, int varIndex)
{
    // this function is called once for defining all function local and magic variables (but not arguments)
    bool result = false;
    llvm::Value* localVar = nullptr;

    MOT_LOG_TRACE("Defining block local variable by varno %d", varIndex);
    MOT_ASSERT(varIndex < ctx->_compiled_function->ndatums);
    PLpgSQL_datum* datum = ctx->_compiled_function->datums[varIndex];

    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VAR:
            MOT_LOG_TRACE("Defining scalar block local variable %s", ((PLpgSQL_var*)datum)->refname);
            localVar = DefineVarLocal(ctx, (PLpgSQL_var*)datum);
            break;

        case PLPGSQL_DTYPE_ROW:
            MOT_LOG_TRACE("Defining row block local variable");
            // this is really a non-existing variable, just a vector of local var reference by id used in INTO clause
            // it should point to the result slot of the last executed query
            result = true;
            break;

        case PLPGSQL_DTYPE_RECORD:
            // record type variable is not supported
            MOT_LOG_TRACE("Unsupported record type variable");
            result = false;
            break;

        case PLPGSQL_DTYPE_REC:
            MOT_LOG_TRACE("Defining rec/record block local variable");
            // this is really a non-existing variable, just a map of local var reference by name used by RECFIELD vars
            // it should point to the result slot of the last executed query
            result = true;
            break;

        case PLPGSQL_DTYPE_RECFIELD:
            MOT_LOG_TRACE("Defining rec-field block local variable");
            // this is really a non-existing variable, just a reference to a record field
            result = true;
            break;

        case PLPGSQL_DTYPE_ARRAYELEM:
            MOT_LOG_TRACE("Defining array-element block local variable");
            // this is really a non-existing variable, just a reference to an array element
            result = true;
            break;

        case PLPGSQL_DTYPE_EXPR:
            MOT_LOG_TRACE("Defining expr block local variable");
            break;

        default:
            MOT_LOG_TRACE("Invalid parameter d-type: %d", datum->dtype);
            break;
    }

    // save local variable in datum map of compile-context and set isnull to true, unless var has other indication
    if (localVar != nullptr) {
        MOT_ASSERT(ctx->m_datums[varIndex] == nullptr);
        ctx->m_datums[varIndex] = localVar;

        // uninitialized local variables are set to null
        MOT_ASSERT(ctx->m_datumNulls[varIndex] == nullptr);
        std::string varName = "isnull";
        bool initIsNull = true;
        if (datum->dtype == PLPGSQL_DTYPE_VAR) {
            PLpgSQL_var* var = (PLpgSQL_var*)datum;
            varName = std::string(var->refname) + "_isnull";
            initIsNull = var->isnull;
        }
        llvm::Value* isNullValue = ctx->m_builder->CreateAlloca(ctx->BOOL_T, 0, nullptr, varName.c_str());
        if (isNullValue) {
            ctx->m_builder->CreateStore(JIT_CONST_BOOL(initIsNull), isNullValue, true);
            ctx->m_datumNulls[varIndex] = isNullValue;
            result = true;
        } else {
            MOT_LOG_TRACE("Failed to generate is_null value for local variable");
        }
    }

    return result;
}

static bool InitBlockLocalVar(JitLlvmFunctionCodeGenContext* ctx, int varIndex)
{
    bool result = false;

    MOT_LOG_TRACE("Initializing block local variable by var-no %d", varIndex);
    MOT_ASSERT(varIndex < ctx->_compiled_function->ndatums);
    PLpgSQL_datum* datum = ctx->_compiled_function->datums[varIndex];

    switch (datum->dtype) {
        case PLPGSQL_DTYPE_VAR:
            MOT_LOG_DEBUG("Initializing scalar block local variable %s", ((PLpgSQL_var*)datum)->refname);
            if (((PLpgSQL_var*)datum)->default_val != nullptr) {
                result = AssignValue(ctx, varIndex, ((PLpgSQL_var*)datum)->default_val);
            } else {
                result = true;  // no initialization needed
            }
            break;

        case PLPGSQL_DTYPE_ROW:
            MOT_LOG_DEBUG("Initializing row block local variable");
            // this is really a non-existing variable, just a vector of local var reference by id used in INTO clause
            // it requires no special initialization
            result = true;
            break;

        case PLPGSQL_DTYPE_REC:
            MOT_LOG_DEBUG("Initializing rec block local variable");
            // this is really a non-existing variable, just a map of local var reference by name used by RECFIELD var
            // it requires no special initialization
            result = true;
            break;

        case PLPGSQL_DTYPE_RECORD:
            // this is Oracle-style record, we don't support it
            MOT_LOG_TRACE("Unsupported record type variable");
            break;

        case PLPGSQL_DTYPE_RECFIELD:
            MOT_LOG_DEBUG("Initializing rec-field block local variable");
            // this is really a non-existing variable, just a reference to a record field
            result = true;
            break;

        case PLPGSQL_DTYPE_ARRAYELEM:
            MOT_LOG_DEBUG("Initializing array-element block local variable");
            // this is really a non-existing variable, just a reference to an array element
            result = true;
            break;

        case PLPGSQL_DTYPE_EXPR:
            MOT_LOG_DEBUG("Initializing expr block local variable");
            break;

        default:
            MOT_LOG_TRACE("Invalid parameter d-type: %d", datum->dtype);
            break;
    }

    return result;
}

inline void AddSetExprIsNull(JitLlvmFunctionCodeGenContext* ctx, llvm::Value* isNull)
{
    llvm_util::AddFunctionCall(ctx, ctx->m_setExprIsNullFunc, isNull, nullptr);
}

inline llvm::Value* AddGetExprIsNull(JitLlvmFunctionCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_getExprIsNullFunc, nullptr);
}

inline void AddSetExprCollation(JitLlvmFunctionCodeGenContext* ctx, llvm::Value* collation)
{
    llvm_util::AddFunctionCall(ctx, ctx->m_setExprCollationFunc, collation, nullptr);
}

inline llvm::Value* AddGetExprCollation(JitLlvmFunctionCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_getExprCollationFunc, nullptr);
}

inline void AddExecClearTuple(JitLlvmFunctionCodeGenContext* ctx)
{
    llvm::Value* slot = ctx->m_builder->CreateLoad(ctx->m_slot, true);
    llvm_util::AddFunctionCall(ctx, ctx->m_execClearTupleFunc, slot, nullptr);
}

inline void AddExecStoreVirtualTuple(JitLlvmFunctionCodeGenContext* ctx)
{
    llvm::Value* slot = ctx->m_builder->CreateLoad(ctx->m_slot, true);
    llvm_util::AddFunctionCall(ctx, ctx->m_execStoreVirtualTupleFunc, slot, nullptr);
}

inline llvm::Value* AddGetParam(JitLlvmFunctionCodeGenContext* ctx, int paramId)
{
    llvm::Value* paramsValue = ctx->m_builder->CreateLoad(ctx->m_params, true);
    llvm::ConstantInt* paramIdValue = llvm::ConstantInt::get(ctx->INT32_T, paramId, true);
    return llvm_util::AddFunctionCall(ctx, ctx->m_getParamAtRefFunc, paramsValue, paramIdValue, nullptr);
}

inline llvm::Value* AddIsParamNull(JitLlvmFunctionCodeGenContext* ctx, int paramId)
{
    llvm::Value* paramsValue = ctx->m_builder->CreateLoad(ctx->m_params, true);
    llvm::ConstantInt* paramIdValue = llvm::ConstantInt::get(ctx->INT32_T, paramId, true);
    return llvm_util::AddFunctionCall(ctx, ctx->m_isParamNullRefFunc, paramsValue, paramIdValue, nullptr);
}

inline llvm::Value* AddIsCompositeResult(JitLlvmFunctionCodeGenContext* ctx)
{
    llvm::Value* slot = ctx->m_builder->CreateLoad(ctx->m_slot, true);
    return llvm_util::AddFunctionCall(ctx, ctx->m_isCompositeResultFunc, slot, nullptr);
}

inline llvm::Value* AddCreateResultDatums(JitLlvmFunctionCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_createResultDatumsFunc, nullptr);
}

inline llvm::Value* AddCreateResultNulls(JitLlvmFunctionCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_createResultNullsFunc, nullptr);
}

inline void AddSetResultValue(JitLlvmFunctionCodeGenContext* ctx, llvm::Value* datums, llvm::Value* nulls, int index,
    llvm::Value* datum, llvm::Value* isNull)
{
    (void)llvm_util::AddFunctionCall(
        ctx, ctx->m_setResultValueFunc, datums, nulls, JIT_CONST_INT32(index), datum, isNull, nullptr);
}

inline llvm::Value* AddCreateResultHeapTuple(
    JitLlvmFunctionCodeGenContext* ctx, llvm::Value* datums, llvm::Value* nulls)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_createResultHeapTupleFunc, datums, nulls, nullptr);
}

inline void AddSetSlotValue(JitLlvmFunctionCodeGenContext* ctx, int tupleColId, llvm::Value* value, llvm::Value* isNull)
{
    llvm::Value* slot = ctx->m_builder->CreateLoad(ctx->m_slot, true);
    llvm::ConstantInt* tupleColIdValue = llvm::ConstantInt::get(ctx->INT32_T, tupleColId, true);
    llvm_util::AddFunctionCall(ctx, ctx->m_setSlotValueFunc, slot, tupleColIdValue, value, isNull, nullptr);
}

inline void AddSetSPSubQueryParamValue(JitLlvmFunctionCodeGenContext* ctx, int subQueryId, int paramId, int paramType,
    llvm::Value* paramValue, llvm::Value* isNull)
{
    llvm_util::AddFunctionCall(ctx,
        ctx->m_setSPSubQueryParamValueFunc,
        JIT_CONST_INT32(subQueryId),
        JIT_CONST_INT32(paramId),
        JIT_CONST_INT32(paramType),
        paramValue,
        isNull,
        nullptr);
}

inline llvm::Value* AddExecuteSubQuery(JitLlvmFunctionCodeGenContext* ctx, int subQueryId, int tcount)
{
    llvm::ConstantInt* subQueryIdValue = llvm::ConstantInt::get(ctx->INT32_T, subQueryId, true);
    llvm::ConstantInt* tcountValue = llvm::ConstantInt::get(ctx->INT32_T, tcount, true);
    return llvm_util::AddFunctionCall(ctx, ctx->m_executeSubQueryFunc, subQueryIdValue, tcountValue, nullptr);
}

inline llvm::Value* AddReleaseNonJitSubQueryResources(JitLlvmFunctionCodeGenContext* ctx, int subQueryId)
{
    return llvm_util::AddFunctionCall(
        ctx, ctx->m_releaseNonJitSubQueryResourcesFunc, JIT_CONST_INT32(subQueryId), nullptr);
}

inline llvm::Value* AddGetTuplesProcessed(JitLlvmFunctionCodeGenContext* ctx, int subQueryId)
{
    llvm::ConstantInt* subQueryIdValue = llvm::ConstantInt::get(ctx->INT32_T, subQueryId, true);
    return llvm_util::AddFunctionCall(ctx, ctx->m_getTuplesProcessedFunc, subQueryIdValue, nullptr);
}

inline llvm::Value* AddGetSubQuerySlotValue(JitLlvmFunctionCodeGenContext* ctx, int subQueryId, int tupleColId)
{
    llvm::ConstantInt* subQueryIdValue = llvm::ConstantInt::get(ctx->INT32_T, subQueryId, true);
    llvm::ConstantInt* tupleColIdValue = llvm::ConstantInt::get(ctx->INT32_T, tupleColId, true);
    return llvm_util::AddFunctionCall(ctx, ctx->m_getSubQuerySlotValueFunc, subQueryIdValue, tupleColIdValue, nullptr);
}

inline llvm::Value* AddGetSubQuerySlotIsNull(JitLlvmFunctionCodeGenContext* ctx, int subQueryId, int tupleColId)
{
    llvm::ConstantInt* subQueryIdValue = llvm::ConstantInt::get(ctx->INT32_T, subQueryId, true);
    llvm::ConstantInt* tupleColIdValue = llvm::ConstantInt::get(ctx->INT32_T, tupleColId, true);
    return llvm_util::AddFunctionCall(ctx, ctx->m_getSubQuerySlotIsNullFunc, subQueryIdValue, tupleColIdValue, nullptr);
}

inline llvm::Value* AddGetSubQueryResultHeapTuple(JitLlvmFunctionCodeGenContext* ctx, int subQueryId)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_getSubQueryResultHeapTupleFunc, JIT_CONST_INT32(subQueryId), nullptr);
}

inline llvm::Value* AddGetHeapTupleValue(
    JitLlvmFunctionCodeGenContext* ctx, llvm::Value* heapTuple, int subQueryId, int columnId, llvm::Value* isNullRef)
{
    llvm::ConstantInt* subQueryIdValue = JIT_CONST_INT32(subQueryId);
    llvm::ConstantInt* columnIdValue = JIT_CONST_INT32(columnId);
    return llvm_util::AddFunctionCall(
        ctx, ctx->m_getHeapTupleValueFunc, heapTuple, subQueryIdValue, columnIdValue, isNullRef, nullptr);
}

inline llvm::Value* AddGetSpiResult(JitLlvmFunctionCodeGenContext* ctx, int subQueryId)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_getSpiResultFunc, JIT_CONST_INT32(subQueryId), nullptr);
}

inline void AddSetTpProcessed(JitLlvmFunctionCodeGenContext* ctx, llvm::Value* value)
{
    llvm::Value* tp = ctx->m_builder->CreateLoad(ctx->m_tuplesProcessed, true);
    llvm_util::AddFunctionCall(ctx, ctx->m_setTpProcessedFunc, tp, value, nullptr);
}

inline void AddSetScanEnded(JitLlvmFunctionCodeGenContext* ctx, llvm::Value* result)
{
    llvm::Value* scanEnded = ctx->m_builder->CreateLoad(ctx->m_scanEnded, true);
    llvm_util::AddFunctionCall(ctx, ctx->m_setScanEndedFunc, scanEnded, result, nullptr);
}

inline llvm::Value* AddGetCurrentSubTransactionId(JitLlvmFunctionCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_getCurrentSubTransactionIdFunc, nullptr);
}

inline void AddBeginBlockWithExceptions(JitLlvmFunctionCodeGenContext* ctx)
{
    llvm_util::AddFunctionCall(ctx, ctx->m_beginBlockWithExceptionsFunc, nullptr);
}

inline void AddEndBlockWithExceptions(JitLlvmFunctionCodeGenContext* ctx)
{
    llvm_util::AddFunctionCall(ctx, ctx->m_endBlockWithExceptionsFunc, nullptr);
}

inline void AddEndAllBlocksWithExceptions(JitLlvmFunctionCodeGenContext* ctx)
{
    for (int i = 0; i < ctx->m_exceptionBlockCount; ++i) {
        AddEndBlockWithExceptions(ctx);
        CheckThrownException(ctx);
    }
}

inline void AddCleanupBlockAfterException(JitLlvmFunctionCodeGenContext* ctx)
{
    llvm_util::AddFunctionCall(ctx, ctx->m_cleanupBlockAfterExceptionFunc, nullptr);
}

inline llvm::Value* AddGetExceptionOrigin(JitLlvmFunctionCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_getExceptionOriginFunc, nullptr);
}

inline void AddSetExceptionOrigin(JitLlvmFunctionCodeGenContext* ctx, int exceptionOrigin)
{
    llvm_util::AddFunctionCall(ctx, ctx->m_setExceptionOriginFunc, JIT_CONST_INT32(exceptionOrigin), nullptr);
}

inline void AddCleanupBeforeReturn(JitLlvmFunctionCodeGenContext* ctx, llvm::Value* untilSubXid)
{
    llvm_util::AddFunctionCall(ctx, ctx->m_cleanupBeforeReturnFunc, untilSubXid, nullptr);
}

inline llvm::Value* AddConvertViaString(JitLlvmFunctionCodeGenContext* ctx, llvm::Value* value, llvm::Value* resultType,
    llvm::Value* targetType, llvm::Value* typeMod)
{
    return llvm_util::AddFunctionCall(
        ctx, ctx->m_convertViaStringFunc, value, resultType, targetType, typeMod, nullptr);
}

inline llvm::Value* AddCastValue(JitLlvmFunctionCodeGenContext* ctx, llvm::Value* value, Oid sourceType, Oid targetType,
    int typeMod, CoercionPathType coercePath, Oid funcId, CoercionPathType coercePath2, Oid funcId2, int nargs,
    int typeByVal)
{
    return llvm_util::AddFunctionCall(ctx,
        ctx->m_castValueFunc,
        value,
        JIT_CONST_INT32(sourceType),
        JIT_CONST_INT32(targetType),
        JIT_CONST_INT32(typeMod),
        JIT_CONST_INT32(coercePath),
        JIT_CONST_INT32(funcId),
        JIT_CONST_INT32(coercePath2),
        JIT_CONST_INT32(funcId2),
        JIT_CONST_INT32(nargs),
        JIT_CONST_INT32(typeByVal),
        nullptr);
}

inline void AddSaveErrorInfo(
    JitLlvmFunctionCodeGenContext* ctx, llvm::Value* errorMessage, llvm::Value* sqlState, llvm::Value* sqlStateString)
{
    llvm_util::AddFunctionCall(ctx, ctx->m_saveErrorInfoFunc, errorMessage, sqlState, sqlStateString, nullptr);
}

inline llvm::Value* AddGetErrorMessage(JitLlvmFunctionCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_getErrorMessageFunc, nullptr);
}

inline llvm::Value* AddGetSqlState(JitLlvmFunctionCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_getSqlStateFunc, nullptr);
}

inline llvm::Value* AddGetSqlStateString(JitLlvmFunctionCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_getSqlStateStringFunc, nullptr);
}

inline llvm::Value* AddGetDatumIsNotNull(JitLlvmFunctionCodeGenContext* ctx, llvm::Value* isNull)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_getDatumIsNotNullFunc, isNull, nullptr);
}

inline llvm::Value* AddGetConstAt(JitLlvmFunctionCodeGenContext* ctx, int constId)
{
    llvm::ConstantInt* constIdValue = llvm::ConstantInt::get(ctx->INT32_T, constId, true);
    return llvm_util::AddFunctionCall(ctx, ctx->m_getConstAtFunc, constIdValue, nullptr);
}

inline void AddAbortFunction(JitLlvmFunctionCodeGenContext* ctx, int faultCode)
{
    llvm::ConstantInt* faultCodeValue = llvm::ConstantInt::get(ctx->INT32_T, faultCode, true);
    llvm_util::AddFunctionCall(ctx, ctx->m_abortFunctionFunc, faultCodeValue, nullptr);
}

inline void AddLlvmClearExceptionStack(JitLlvmFunctionCodeGenContext* ctx)
{
    llvm_util::AddFunctionCall(ctx, ctx->m_llvmClearExceptionStackFunc, nullptr);
}

inline void AddEmitProfileData(
    JitLlvmFunctionCodeGenContext* ctx, uint32_t functionId, uint32_t regionId, bool startRegion)
{
    llvm::ConstantInt* functionIdValue = llvm::ConstantInt::get(ctx->INT32_T, functionId, true);
    llvm::ConstantInt* regionIdValue = llvm::ConstantInt::get(ctx->INT32_T, regionId, true);
    llvm::ConstantInt* startRegionValue = llvm::ConstantInt::get(ctx->INT32_T, startRegion ? 1 : 0, true);
    llvm_util::AddFunctionCall(
        ctx, ctx->m_emitProfileDataFunc, functionIdValue, regionIdValue, startRegionValue, nullptr);
}

inline void InjectProfileData(JitLlvmFunctionCodeGenContext* ctx, const char* regionName, bool beginRegion)
{
    if (!MOT::GetGlobalConfiguration().m_enableCodegenProfile) {
        return;
    }

    JitProfiler* jitProfiler = JitProfiler::GetInstance();
    const char* qualifiedName = GetActiveNamespace();
    uint32_t profileFunctionId = jitProfiler->GetProfileFunctionId(qualifiedName, ctx->m_plan->_function_id);
    uint32_t profileRegionId = jitProfiler->GetProfileRegionId(qualifiedName, regionName);
    AddEmitProfileData(ctx, profileFunctionId, profileRegionId, beginRegion);
}

static bool CreateJittedFunction(
    JitLlvmFunctionCodeGenContext* ctx, const char* functionName, const char* functionSource, bool isStrict)
{
    // although the function has a native signature as in the stored procedure text, we actually use
    // a different signature, to accommodate for runtime needs:
    // 1. Return value is integer (as in query jitted function) to indicate execution status
    // 2. result datum or tuple (and its is-null property) is reported via the slot parameter
    // 3. Return value is actually a tuple slot (good also for "returns set of")
    // 4. In addition, the parameter list also contains datum+isnull pairs
    // 5. Function invocation is a regular query, so for quick execution we need both jitted query and jitted function
    //    to have the same signature.
    // 6. In fact, parameters are local variable pointers to datum objects in the ParamListInfo parameter

    llvm::Value* llvmargs[MOT_JIT_FUNC_ARG_COUNT];
    GsCodeGen::FnPrototype fn_prototype(ctx->m_codeGen, functionName, ctx->INT32_T);

    fn_prototype.addArgument(GsCodeGen::NamedVariable("params", ctx->ParamListInfoDataType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("slot", ctx->TupleTableSlotType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("tp_processed", ctx->INT64_T->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("scan_ended", ctx->INT32_T->getPointerTo()));

    ctx->m_jittedFunction = fn_prototype.generatePrototype(ctx->m_builder, &llvmargs[0]);

    // get the arguments
    int arg_index = 0;
    llvm::Value* paramStore =
        ctx->m_builder->CreateAlloca(ctx->ParamListInfoDataType->getPointerTo(), 0, nullptr, "param_s");
    llvm::Value* slotStore =
        ctx->m_builder->CreateAlloca(ctx->TupleTableSlotType->getPointerTo(), 0, nullptr, "slot_s");
    llvm::Value* tpStore = ctx->m_builder->CreateAlloca(ctx->INT64_T->getPointerTo(), 0, nullptr, "tp_s");
    llvm::Value* scanStore = ctx->m_builder->CreateAlloca(ctx->INT32_T->getPointerTo(), 0, nullptr, "scan_s");
    ctx->m_builder->CreateStore(llvmargs[arg_index++], paramStore, true);
    ctx->m_builder->CreateStore(llvmargs[arg_index++], slotStore, true);
    ctx->m_builder->CreateStore(llvmargs[arg_index++], tpStore, true);
    ctx->m_builder->CreateStore(llvmargs[arg_index++], scanStore, true);
    ctx->m_params = paramStore;
    ctx->m_slot = slotStore;
    ctx->m_tuplesProcessed = tpStore;
    ctx->m_scanEnded = scanStore;

    JIT_DEBUG_LOG("Starting execution of jitted function");

    // prepare datum values for parameters:
    // 1. in this translation process, we rely on the fact that function parameters varnos are always first,
    // starting from zero
    // 2. pay attention that all parameter types are DATUM_T
    // 3. for the case of default parameters, we make sure that invoke code generation passes all missing parameters
    // so that the function being invoked always gets all required parameters.
    // 4. in this process we collect all default values into an array to be used by invoke code generation
    int argCount = ctx->_compiled_function->fn_nargs;
    MOT_LOG_TRACE("Initializing %d function arguments", argCount);
    InjectProfileData(ctx, MOT_JIT_PROFILE_REGION_TOTAL, true);
    InjectProfileData(ctx, MOT_JIT_PROFILE_REGION_DEF_VARS, true);
    for (int i = 0; i < argCount; ++i) {
        int argVarNo = ctx->_compiled_function->fn_argvarnos[i];
        std::string pName = "p_" + std::to_string(argVarNo);
        std::string pNullName = "pNull_" + std::to_string(argVarNo);
        MOT_ASSERT(ctx->m_datums[argVarNo] == nullptr);
        MOT_ASSERT(ctx->m_datumNulls[argVarNo] == nullptr);
        MOT_LOG_TRACE("Defining parameter %d (global index %d)", i, argVarNo);
        llvm::Value* pStore = ctx->m_builder->CreateAlloca(ctx->DATUM_T->getPointerTo(), 0, nullptr, pName.c_str());
        llvm::Value* pNullStore =
            ctx->m_builder->CreateAlloca(ctx->BOOL_T->getPointerTo(), 0, nullptr, pNullName.c_str());
        llvm::Value* param = AddGetParam(ctx, i);

        if (param == nullptr) {
            MOT_LOG_TRACE("Failed to define parameter %d (global index %d)", i, argVarNo);
            return false;
        }

        ctx->m_builder->CreateStore(param, pStore, true);
        llvm::Value* isParamNull = AddIsParamNull(ctx, i);

        if (isParamNull == nullptr) {
            MOT_LOG_TRACE("Failed to define parameter %d (global index %d)", i, argVarNo);
            return false;
        }

        ctx->m_builder->CreateStore(isParamNull, pNullStore, true);
        // attention: returned values from AddGetParam() is l-value (pointer)
        ctx->m_datums[argVarNo] = ctx->m_builder->CreateLoad(pStore, true);
        ctx->m_datumNulls[argVarNo] = ctx->m_builder->CreateLoad(pNullStore, true);
    }

    InjectProfileData(ctx, MOT_JIT_PROFILE_REGION_DEF_VARS, false);

    OpenCompileFrame(ctx);

    // generate code for strict functions - check no null arguments
    if (isStrict) {
        for (int i = 0; i < argCount; ++i) {
            int argVarNo = ctx->_compiled_function->fn_argvarnos[i];
            JIT_IF_BEGIN(has_null_param);
            llvm::Value* isNull = ctx->m_builder->CreateLoad(ctx->m_datumNulls[argVarNo], true);
            JIT_IF_EVAL(isNull);
            {
                // this is OK - strict execution forces returning nullptr tuple
                AddExecClearTuple(ctx);
                JIT_RETURN(JIT_CONST_INT32(0));
            }
            JIT_IF_END();
        }
    }

    return true;
}

static MotJitContext* FinalizeCodegen(
    JitLlvmFunctionCodeGenContext* ctx, JitFunctionPlan* plan, const char* functionName, JitCodegenStats& codegenStats)
{
    CloseCompileFrame(ctx);
    // do GsCodeGen stuff to wrap up
    uint64_t startTime = GetSysClock();
    if (!ctx->m_codeGen->verifyFunction(ctx->m_jittedFunction)) {
        MOT_LOG_ERROR("Failed to generate jitted code for stored procedure: Failed to verify jit llvm function");
#ifdef LLVM_ENABLE_DUMP
        ctx->m_jittedFunction->dump();
#else
        ctx->m_jittedFunction->print(llvm::errs(), nullptr, false, true);
#endif
        (void)fflush(stderr);
        // print informative error
        llvm::Function::iterator itr = ctx->m_jittedFunction->begin();
        for (; itr != ctx->m_jittedFunction->end(); ++itr) {
            if (itr->empty()) {
                MOT_LOG_ERROR("Block %s is empty", itr->getName().data());
                break;
            }
            if (!itr->back().isTerminator()) {
                MOT_LOG_ERROR("Block %s does not end with terminator", itr->getName().data())
                break;
            }
        }

        return nullptr;
    }
    uint64_t endTime = GetSysClock();
    uint64_t timeMicros = MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime);
    codegenStats.m_verifyTime = timeMicros;
    MOT_LOG_TRACE("Function '%s' verification time: %" PRIu64 " micros", functionName, timeMicros);

    startTime = GetSysClock();
    ctx->m_codeGen->FinalizeFunction(ctx->m_jittedFunction);
    endTime = GetSysClock();
    timeMicros = MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime);
    codegenStats.m_finalizeTime = timeMicros;
    MOT_LOG_TRACE("Function '%s' finalization time: %" PRIu64 " micros", functionName, timeMicros);

    // print if we are either configured to print or we are in debug mode
#ifdef MOT_JIT_DEBUG
    bool dumpFunction = true;
#else
    bool dumpFunction = false;
#endif

    if (dumpFunction || IsMotCodegenPrintEnabled()) {
#ifdef LLVM_ENABLE_DUMP
        ctx->m_jittedFunction->dump();
#else
        ctx->m_jittedFunction->print(llvm::errs());
#endif
        (void)fflush(stderr);
    }

    // that's it, we are ready
    JitFunctionContext* jitContext =
        (JitFunctionContext*)AllocJitContext(JIT_CONTEXT_GLOBAL, JitContextType::JIT_CONTEXT_TYPE_FUNCTION);
    if (jitContext == nullptr) {
        MOT_LOG_TRACE("Failed to allocate JIT context, aborting code generation");
        return nullptr;
    }

    MOT_LOG_DEBUG("Adding function to MCJit");
    ctx->m_codeGen->addFunctionToMCJit(ctx->m_jittedFunction, (void**)&jitContext->m_llvmSPFunction);

    MOT_LOG_DEBUG("Generating code...");
    startTime = GetSysClock();
    ctx->m_codeGen->enableOptimizations(true);
    ctx->m_codeGen->compileCurrentModule(false);
    endTime = GetSysClock();
    timeMicros = MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime);
    codegenStats.m_compileTime = timeMicros;
    MOT_LOG_TRACE("Function '%s' compilation time: %" PRIu64 " micros", functionName, timeMicros);

    // prepare global constant array
    if (ctx->m_constCount > 0) {
        if (!PrepareDatumArray(ctx->m_constValues, ctx->m_constCount, &jitContext->m_constDatums)) {
            MOT_LOG_ERROR("Failed to generate jitted code for function: Failed to prepare constant datum array");
            DestroyJitContext(jitContext);
            return nullptr;
        }
    }

    // setup execution details
    jitContext->m_SPArgCount = (uint64_t)ctx->_compiled_function->fn_nargs;
    jitContext->m_codeGen = ctx->m_codeGen;  // make sure module is not destroyed
    ctx->m_codeGen = nullptr;                // prevent destruction
    jitContext->m_validState = JIT_CONTEXT_VALID;
    jitContext->m_commandType = JIT_COMMAND_FUNCTION;
    jitContext->m_functionOid = plan->_function_id;
    jitContext->m_functionTxnId = plan->m_function->fn_xmin;
    jitContext->m_paramCount = plan->m_paramCount;
    if (jitContext->m_paramCount > 0) {
        size_t allocSize = sizeof(Oid) * jitContext->m_paramCount;
        jitContext->m_paramTypes = (Oid*)MOT::MemGlobalAlloc(allocSize);
        if (jitContext->m_paramTypes == nullptr) {
            MOT_LOG_TRACE("Failed to allocate %u bytes for %u parameter type items in JIT function context",
                (unsigned)allocSize,
                jitContext->m_paramCount);
            DestroyJitContext(jitContext);
            return nullptr;
        }
        errno_t erc = memcpy_s(jitContext->m_paramTypes, allocSize, plan->m_paramTypes, allocSize);
        securec_check(erc, "\0", "\0");
    } else {
        jitContext->m_paramTypes = nullptr;
    }
    jitContext->m_SPSubQueryCount = plan->_query_count;
    if (jitContext->m_SPSubQueryCount > 0) {
        size_t allocSize = sizeof(JitCallSite) * jitContext->m_SPSubQueryCount;
        jitContext->m_SPSubQueryList = (JitCallSite*)MOT::MemGlobalAlloc(allocSize);
        if (jitContext->m_SPSubQueryList == nullptr) {
            MOT_LOG_TRACE("Failed to allocate %u bytes for %u sub-query data items in JIT function context",
                (unsigned)allocSize,
                (unsigned)jitContext->m_SPSubQueryCount);
            DestroyJitContext(jitContext);
            return nullptr;
        }
        errno_t erc = memset_s(jitContext->m_SPSubQueryList, allocSize, 0, allocSize);
        securec_check(erc, "\0", "\0");

        // initialize the sub-query data, compile all sub-queries
        for (uint32_t i = 0; i < jitContext->m_SPSubQueryCount; ++i) {
            JitCallSitePlan* callSitePlan = &plan->m_callSitePlanList[i];
            JitCallSite* callSite = &jitContext->m_SPSubQueryList[i];
            if (!ProcessCallSitePlan(callSitePlan, callSite, ctx->_compiled_function)) {
                MOT_LOG_TRACE("Failed to process call site plan %d: %s", i, callSitePlan->m_queryString);
                DestroyJitContext(jitContext);
                return nullptr;
            }
            if (callSite->m_queryContext != nullptr) {
                callSite->m_queryContext->m_parentContext = jitContext;
            }
        }
    }
    jitContext->m_compositeResult = (ctx->m_typeFuncClass == TYPEFUNC_COMPOSITE) ? 1 : 0;
    jitContext->m_resultTupDesc = ctx->m_resultTupDesc;
    jitContext->m_rowTupDesc = ctx->m_rowTupDesc;
    ctx->m_resultTupDesc = nullptr;
    ctx->m_rowTupDesc = nullptr;

    return jitContext;
}

static bool ProcessStatement(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt* stmt)
{
    JIT_DEBUG_LOG_INT("Executing line %d", stmt->lineno);
    switch (stmt->cmd_type) {
        case PLPGSQL_STMT_BLOCK:
            return ProcessStatementBlock(ctx, (PLpgSQL_stmt_block*)stmt);
        case PLPGSQL_STMT_ASSIGN:
            return ProcessStatementAssign(ctx, (PLpgSQL_stmt_assign*)stmt);
        case PLPGSQL_STMT_IF:
            return ProcessStatementIf(ctx, (PLpgSQL_stmt_if*)stmt);
        case PLPGSQL_STMT_GOTO:
            return ProcessStatementGoto(ctx, (PLpgSQL_stmt_goto*)stmt);
        case PLPGSQL_STMT_CASE:
            return ProcessStatementCase(ctx, (PLpgSQL_stmt_case*)stmt);
        case PLPGSQL_STMT_LOOP:
            return ProcessStatementLoop(ctx, (PLpgSQL_stmt_loop*)stmt);
        case PLPGSQL_STMT_WHILE:
            return ProcessStatementWhile(ctx, (PLpgSQL_stmt_while*)stmt);
        case PLPGSQL_STMT_FORI:
            return ProcessStatementForI(ctx, (PLpgSQL_stmt_fori*)stmt);
        case PLPGSQL_STMT_FORS:
            return ProcessStatementForS(ctx, (PLpgSQL_stmt_fors*)stmt);
        case PLPGSQL_STMT_FORC:
            return ProcessStatementForC(ctx, (PLpgSQL_stmt_forc*)stmt);
        case PLPGSQL_STMT_FOREACH_A:
            return ProcessStatementForEach(ctx, (PLpgSQL_stmt_foreach_a*)stmt);
        case PLPGSQL_STMT_EXIT:
            return ProcessStatementExit(ctx, (PLpgSQL_stmt_exit*)stmt);
        case PLPGSQL_STMT_RETURN:
            return ProcessStatementReturn(ctx, (PLpgSQL_stmt_return*)stmt);
        case PLPGSQL_STMT_RETURN_NEXT:
            return ProcessStatementReturnNext(ctx, (PLpgSQL_stmt_return_next*)stmt);
        case PLPGSQL_STMT_RETURN_QUERY:
            return ProcessStatementReturnQuery(ctx, (PLpgSQL_stmt_return_query*)stmt);
        case PLPGSQL_STMT_RAISE:
            return ProcessStatementRaise(ctx, (PLpgSQL_stmt_raise*)stmt);
        case PLPGSQL_STMT_EXECSQL:
            return ProcessStatementExecSql(ctx, (PLpgSQL_stmt_execsql*)stmt);
        case PLPGSQL_STMT_DYNEXECUTE:
            return ProcessStatementDynExecute(ctx, (PLpgSQL_stmt_dynexecute*)stmt);
        case PLPGSQL_STMT_DYNFORS:
            return ProcessStatementDynForS(ctx, (PLpgSQL_stmt_dynfors*)stmt);
        case PLPGSQL_STMT_GETDIAG:
            return ProcessStatementGetDiag(ctx, (PLpgSQL_stmt_getdiag*)stmt);
        case PLPGSQL_STMT_OPEN:
            return ProcessStatementOpen(ctx, (PLpgSQL_stmt_open*)stmt);
        case PLPGSQL_STMT_FETCH:
            return ProcessStatementFetch(ctx, (PLpgSQL_stmt_fetch*)stmt);
        case PLPGSQL_STMT_CLOSE:
            return ProcessStatementClose(ctx, (PLpgSQL_stmt_close*)stmt);
        case PLPGSQL_STMT_PERFORM:
            return ProcessStatementPerform(ctx, (PLpgSQL_stmt_perform*)stmt);
        case PLPGSQL_STMT_NULL:
            return true;

        default:
            MOT_LOG_TRACE("Failed to process statement: invalid statement type %d", stmt->cmd_type);
            return false;
    }
}

static bool InitBlockVars(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_block* stmt)
{
    MOT_LOG_TRACE("Initializing block local variables");
    for (int i = 0; i < stmt->n_initvars; ++i) {
        if (!InitBlockLocalVar(ctx, stmt->initvarnos[i])) {
            MOT_LOG_TRACE("InitBlockVars(): Failed to initialize local variable %d (global index %d) in block %s",
                i,
                stmt->initvarnos[i],
                stmt->label);
            return false;
        }
    }
    return true;
}

static bool ProcessStatementList(JitLlvmFunctionCodeGenContext* ctx, List* actions)
{
    bool result = true;

    int stmtIndex = 0;  // for diagnostics
    ListCell* lc = nullptr;

    foreach (lc, actions) {
        PLpgSQL_stmt* stmt = (PLpgSQL_stmt*)lfirst(lc);
        if (!ProcessStatement(ctx, stmt)) {
            MOT_LOG_TRACE("processStatementList(): Failed to process statement %d", stmtIndex);
            result = false;
            break;
        }
        ++stmtIndex;
    }

    return result;
}

static bool ProcessStatementBlockBody(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_block* stmt)
{
    // each block has local variables, so we define them now
    // we also make sure to use a compile-time frame
    OpenCompileFrame(ctx);

    if (!InitBlockVars(ctx, stmt)) {
        MOT_LOG_TRACE("ProcessStatementBlockBody(): Failed to initialize block variables");
        return false;
    }

    MOT_LOG_TRACE("ProcessStatementBlockBody(): processing block body");
    if (!ProcessStatementList(ctx, stmt->body)) {
        MOT_LOG_TRACE("ProcessStatementBlockBody(): Failed to process block body");
        return false;
    }

    CloseCompileFrame(ctx);
    return true;
}

static bool MakeStringDatum(const char* message, Datum* target)
{
    size_t strSize = strlen(message);
    size_t allocSize = VARHDRSZ + strSize + 1;
    bytea* copy = (bytea*)MOT::MemSessionAlloc(allocSize);
    if (copy == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compile", "Failed to allocate %u bytes for datum string constant", (unsigned)allocSize);
        return false;
    }

    errno_t erc = memcpy_s(VARDATA(copy), strSize, (uint8_t*)message, strSize);
    securec_check(erc, "\0", "\0");

    VARDATA(copy)[strSize] = 0;
    SET_VARSIZE(copy, allocSize);

    *target = PointerGetDatum(copy);
    return true;
}

static void FreeStringDatum(Datum value)
{
    bytea* txt = DatumGetByteaP(value);
    if (txt != nullptr) {
        MOT::MemSessionFree(txt);
    }
}

static llvm::Value* GetStringConst(JitLlvmFunctionCodeGenContext* ctx, const char* str)
{
    Datum value = PointerGetDatum(nullptr);
    if (!MakeStringDatum(str, &value)) {
        MOT_LOG_TRACE("Failed to prepare string datum for string: %s", str);
        return nullptr;
    }
    bool pooled = false;
    int constId = AllocateConstId(ctx, VARCHAROID, value, false, true, &pooled);
    if (constId == -1) {
        MOT_LOG_TRACE("Failed to allocate constant identifier for string: %s", str);
        FreeStringDatum(value);
        return nullptr;
    }
    MOT_LOG_TRACE("Allocated const id %d for string: %s", constId, str);
    if (pooled) {
        FreeStringDatum(value);
    }
    llvm::Value* strValue = AddGetConstAt(ctx, constId);
    return strValue;
}

static bool AddSaveErrorInfoInContext(
    JitLlvmFunctionCodeGenContext* ctx, const char* errorMessage, int sqlState, char* sqlStateCode)
{
    llvm::Value* errorMessageValue = GetStringConst(ctx, errorMessage);
    llvm::Value* sqlStateValue = JIT_CONST_INT32(sqlState);
    llvm::Value* sqlStateStringValue = GetStringConst(ctx, sqlStateCode);
    AddSaveErrorInfo(ctx, errorMessageValue, sqlStateValue, sqlStateStringValue);
    return true;
}

static bool AddAssignErrorText(JitLlvmFunctionCodeGenContext* ctx, int varNo, const char* errorMessage)
{
    llvm::Value* errorValue = GetLocalOrParamByVarNo(ctx, varNo);
    llvm::Value* errorIsNull = GetIsNullLocalOrParamByVarNo(ctx, varNo);

    llvm::Value* errorMessageValue = GetStringConst(ctx, errorMessage);
    ctx->m_builder->CreateStore(errorMessageValue, errorValue, true);
    ctx->m_builder->CreateStore(JIT_CONST_BOOL(false), errorIsNull, true);
    JIT_DEBUG_LOG_INT("Assigned error text to varno: %d", varNo);
    JIT_DEBUG_LOG_VARNO(varNo);

#ifdef MOT_JIT_DEBUG
    llvm::Value* loadedValue = ctx->m_builder->CreateLoad(errorIsNull, true);
    JIT_DEBUG_LOG_INT_VAR("Error text is null var value: %d", loadedValue);
    loadedValue = ctx->m_builder->CreateLoad(errorValue, true);
    JIT_DEBUG_LOG_STRING_DATUM_VAR("Error text stored: ", loadedValue);
#endif
    return true;
}

static bool AddSetSqlStateAndErrorMessage(JitLlvmFunctionCodeGenContext* ctx, const char* errorMessage, int sqlState)
{
    // prepare sql state text consisting of 6 chars
    char sqlStateCode[6] = {};
    SqlStateToCode(sqlState, sqlStateCode);

    PLpgSQL_stmt_block* stmt = ctx->m_processedBlock;
    if ((stmt == nullptr) || (stmt->exceptions == nullptr)) {
        // this is a function which does not have an exception block, but still throws an exception. It is assumed
        // that there is a calling function which should catch this exception, so we save the exception information
        // in the backing context of the this function
        if (!AddSaveErrorInfoInContext(ctx, errorMessage, sqlState, sqlStateCode)) {
            MOT_LOG_TRACE("Failed to save error text in backing context");
            return false;
        }
        return true;
    }
    JIT_DEBUG_LOG_INT("Assigning SQLERRM message into varno: %d", ctx->_compiled_function->sqlerrm_varno);
    JIT_DEBUG_LOG_STRING("Assigning SQLERRM message: %s", errorMessage);
    if (!AddAssignErrorText(ctx, ctx->_compiled_function->sqlerrm_varno, errorMessage)) {
        MOT_LOG_TRACE("Failed to assign error text for sqlerrm");
        return false;
    }

    if (!AddAssignErrorText(ctx, ctx->_compiled_function->sqlstate_varno, sqlStateCode)) {
        MOT_LOG_TRACE("Failed to assign error text for sqlstate");
        return false;
    }
    return true;
}

inline void UpdateSqlErrorMessageVar(JitLlvmFunctionCodeGenContext* ctx)
{
    // access directly to avoid endless recursion
    llvm::Value* errorMessage = AddGetErrorMessage(ctx);
    llvm::Value* errorValue = ctx->m_datums[ctx->_compiled_function->sqlerrm_varno];
    llvm::Value* errorIsNull = ctx->m_datumNulls[ctx->_compiled_function->sqlerrm_varno];
    ctx->m_builder->CreateStore(errorMessage, errorValue, true);
    ctx->m_builder->CreateStore(JIT_CONST_BOOL(false), errorIsNull, true);
}

inline void UpdateSqlStateVar(JitLlvmFunctionCodeGenContext* ctx)
{
    // access directly to avoid endless recursion
    llvm::Value* sqlState = AddGetSqlStateString(ctx);
    llvm::Value* sqlStateValue = ctx->m_datums[ctx->_compiled_function->sqlstate_varno];
    llvm::Value* sqlStateIsNull = ctx->m_datumNulls[ctx->_compiled_function->sqlstate_varno];
    ctx->m_builder->CreateStore(sqlState, sqlStateValue, true);
    ctx->m_builder->CreateStore(JIT_CONST_BOOL(false), sqlStateIsNull, true);
}

inline void UpdateSqlCodeVar(JitLlvmFunctionCodeGenContext* ctx)
{
    // access directly to avoid endless recursion
    llvm::Value* sqlCode = nullptr;
    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        sqlCode = AddGetSqlStateString(ctx);
    } else {
        sqlCode = AddGetSqlState(ctx);
    }
    llvm::Value* sqlCodeValue = ctx->m_datums[ctx->_compiled_function->sqlcode_varno];
    llvm::Value* sqlCodeIsNull = ctx->m_datumNulls[ctx->_compiled_function->sqlcode_varno];
    ctx->m_builder->CreateStore(sqlCode, sqlCodeValue, true);
    ctx->m_builder->CreateStore(JIT_CONST_BOOL(false), sqlCodeIsNull, true);
}

static bool AddLoadErrorInfoFromContext(JitLlvmFunctionCodeGenContext* ctx)
{
    PLpgSQL_stmt_block* stmt = ctx->m_processedBlock;
    if ((stmt != nullptr) && (stmt->exceptions != nullptr)) {
        UpdateSqlErrorMessageVar(ctx);
        UpdateSqlStateVar(ctx);
        UpdateSqlCodeVar(ctx);
    }
    return true;
}

static bool ProcessStatementBlockWithExceptions(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_block* stmt)
{
    if (!InitBlockVars(ctx, stmt)) {
        MOT_LOG_TRACE("ProcessStatementBlockWithExceptions(): Failed to initialize block variables");
        return false;
    }

    // execute block entry (includes starting a new sub-transaction)
    AddBeginBlockWithExceptions(ctx);
    CheckThrownException(ctx);

    bool result = true;
    JIT_TRY_BEGIN(stmt_block_try)
    {
        LabelDesc labelDesc = {stmt->label, nullptr, JIT_TRY_POST_BLOCK(), false};
        ctx->m_labelStack.push_back(labelDesc);
        MOT_LOG_TRACE("processStatementBlockWithExceptions(): processing block body");
        if (!ProcessStatementList(ctx, stmt->body)) {
            MOT_LOG_TRACE("processStatementBlockWithExceptions(): Failed to process block body");
            result = false;
        } else {
            // Commit the inner transaction, return to outer xact context
            // NOTE: be careful, block might have ended already with terminator
            // ATTENTION: this is an open use case - we need to fix all return statements in all relevant blocks
            llvm::Instruction* termInst = ctx->m_builder->GetInsertBlock()->getTerminator();
            if (termInst == nullptr) {
                // commit and execute block exit
                AddEndBlockWithExceptions(ctx);
                CheckThrownException(ctx);
            } else {
                MOT_LOG_TRACE("processStatementBlockWithExceptions(): statement list ended with terminator");
            }

            // now process block exceptions
            bool catchAllIssued = false;
            ListCell* lc = nullptr;
            foreach (lc, stmt->exceptions->exc_list) {
                PLpgSQL_exception* exceptionBlock = (PLpgSQL_exception*)lfirst(lc);
                MOT_LOG_TRACE("processStatementBlockWithExceptions(): Adding catch block");
                PLpgSQL_condition* cond = exceptionBlock->conditions;
                // single SQL error state denotes "WHEN OTHERS THEN", so this is a catch-all block
                if (cond->sqlerrstate == 0) {
                    if (cond->next != nullptr) {
                        MOT_LOG_TRACE("processStatementBlockWithExceptions(): Invalid exception catch-all block with "
                                      "multiple conditions");
                        result = false;
                        break;
                    }
                    JIT_CATCH_ALL();
                    catchAllIssued = true;
                } else {
                    while (cond) {
                        MOT_LOG_TRACE(
                            "processStatementBlockWithExceptions(): Adding catch handler for SQL error state: %d",
                            cond->sqlerrstate);
                        JIT_BEGIN_CATCH_MANY(JIT_CONST_INT32(cond->sqlerrstate));
                        cond = cond->next;
                    }
                    JIT_CATCH_MANY_TERM();
                }
                MOT_LOG_TRACE("processStatementBlockWithExceptions(): Processing catch block statements");

                // in every case of caught exception we first rollback current transaction
                JIT_DEBUG_LOG("Executing SP catch handler statements");
                // ATTENTION: when the exception is thrown from within the jitted function we should not load the error
                // info into SQLERRM, but when it was thrown from within a helper we should. This should be done before
                // any cleanup, or else the original error might get lost
                JIT_IF_BEGIN(load_error_info);
                llvm::Value* exceptionOrigin = AddGetExceptionOrigin(ctx);
                JIT_IF_EVAL_CMP(exceptionOrigin, JIT_CONST_INT32(JIT_EXCEPTION_EXTERNAL), JitICmpOp::JIT_ICMP_EQ);
                {
                    JIT_DEBUG_LOG("Loading error info from context");
                    AddLoadErrorInfoFromContext(ctx);
                    // reset the exception origin
                    AddSetExceptionOrigin(ctx, JIT_EXCEPTION_INTERNAL);
                }
                JIT_IF_END();
                JIT_DEBUG_LOG_VARNO(ctx->_compiled_function->sqlerrm_varno);
                AddCleanupBlockAfterException(ctx);
                CheckThrownException(ctx);
                --ctx->m_exceptionBlockCount;  // one exception block handled

                // attention: sql error message and state are already set up when JIT exception is thrown

                // and process the exception statement list
                if (!ProcessStatementList(ctx, exceptionBlock->action)) {
                    MOT_LOG_TRACE("processStatementBlockWithExceptions(): Failed to process exception handler: Failed "
                                  "to process exception statement list");
                    result = false;
                    break;
                }
                JIT_DEBUG_LOG("Finished executing SP catch handler statements");
                JIT_END_CATCH();
                ++ctx->m_exceptionBlockCount;  // return to previous state
            }

            if (result && !catchAllIssued) {
                // we must issue a catch-all if none was issued so that rollback will be called in any case
                JIT_CATCH_ALL();
                {
                    JIT_DEBUG_LOG("Forced catch-all for sub-tx rollback");
                    // we do not save sqlerrm, just fold back properly
                    AddCleanupBlockAfterException(ctx);
                    CheckThrownException(ctx);
                    --ctx->m_exceptionBlockCount;  // one exception block handled
                    JIT_RETHROW();
                }
                JIT_END_CATCH();
            }
        }
    }
    JIT_TRY_END()
    ctx->m_labelStack.pop_back();

    return result;
}

static bool ProcessStatementLabel(JitLlvmFunctionCodeGenContext* ctx, const char* label, const char* defaultName)
{
    bool result = true;

    // if label exists, then create a named block and add to goto map
    // otherwise just create a block
    if (label && (ctx->m_gotoLabels.find(label) != ctx->m_gotoLabels.end())) {
        MOT_LOG_TRACE("Invalid block name %s: already exists", label);
        result = false;
    } else {
        llvm::BasicBlock* block =
            llvm::BasicBlock::Create(ctx->m_codeGen->context(), label ? label : defaultName, ctx->m_jittedFunction);
        if (label) {
            result = ctx->m_gotoLabels.insert(GotoLabelMap::value_type(label, block)).second;
            MOT_ASSERT(result);
        }
        if (result) {
            // terminate previous block and start a new one (so that goto would work properly)
            ctx->m_builder->CreateBr(block);
            ctx->m_builder->SetInsertPoint(block);
        }
    }

    return result;
}

static bool ProcessStatementBlock(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_block* stmt)
{
    MOT_LOG_TRACE("Processing statement: block");
    PLpgSQL_stmt_block* enclosingBlock = ctx->m_processedBlock;
    if (stmt->exceptions != nullptr) {
        ctx->m_processedBlock = stmt;
    }
    bool result = ProcessStatementLabel(ctx, stmt->label, "unnamed_stmt_block");
    if (result) {
        if (stmt->exceptions) {
            if (ctx->m_exceptionBlockCount >= MOT_JIT_MAX_BLOCK_DEPTH) {
                MOT_LOG_TRACE("Failed to process function: Reached maximum nesting level of blocks with exceptions");
                result = false;
            } else {
                ++ctx->m_exceptionBlockCount;
                result = ProcessStatementBlockWithExceptions(ctx, stmt);
                --ctx->m_exceptionBlockCount;
            }
        } else {
            // create post block to provide a branch point for EXIT statement
            llvm::BasicBlock* block = nullptr;
            if (stmt->label != nullptr) {
                block = llvm::BasicBlock::Create(ctx->m_codeGen->context(), stmt->label, ctx->m_jittedFunction);
                LabelDesc labelDesc = {stmt->label, nullptr, block, false};
                ctx->m_labelStack.push_back(labelDesc);
            }

            result = ProcessStatementBlockBody(ctx, stmt);

            // terminate previous block and start a new one (so that goto would work properly)
            if (stmt->label != nullptr) {
                ctx->m_labelStack.pop_back();
                ctx->m_builder->CreateBr(block);
                ctx->m_builder->SetInsertPoint(block);
            }
        }
    }
    ctx->m_processedBlock = enclosingBlock;

    return result;
}

static void AddValidateSubXid(JitLlvmFunctionCodeGenContext* ctx)
{
    llvm::Value* endSubXid = AddGetCurrentSubTransactionId(ctx);
    JIT_IF_BEGIN(validate_sub_xid)
    JIT_IF_EVAL_CMP(ctx->m_initSubXid, endSubXid, JitExec::JIT_ICMP_NE);
    {
        AddAbortFunction(ctx, JIT_FAULT_SUB_TX_NOT_CLOSED);
    }
    JIT_IF_END()
}

static bool ProcessFunctionAction(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_function* function)
{
    // although we do not have an exception block, we must have some enclosing try-catch block, because sometimes
    // we generate code for throwing exceptions
    bool result = true;
    ctx->m_initSubXid = AddGetCurrentSubTransactionId(ctx);
    JIT_TRY_BEGIN(top_level)
    {
        if (!ProcessStatementBlock(ctx, function->action)) {
            MOT_LOG_TRACE("Failed to process function action block");
            result = false;
        }
    }
    if (result) {
        JIT_CATCH_ALL();
        {
            // cleanup and report error to user
            JIT_DEBUG_LOG("Uncaught exception in top level block");
            AddLlvmClearExceptionStack(ctx);  // clean up run-time exception stack

            // inject profile data before exiting
            InjectProfileData(ctx, MOT_JIT_PROFILE_REGION_TOTAL, false);

            AddValidateSubXid(ctx);

            // since this might be a normal flow of events for sub-SP, we return a special return code to avoid error
            // reporting in such case
            JIT_RETURN(JIT_CONST_INT32(MOT::RC_JIT_SP_EXCEPTION));
        }
        JIT_END_CATCH();
    }
    JIT_TRY_END()

    // we want to avoid a post-try block without instructions at all, let alone terminators
    AddAbortFunction(ctx, JIT_FAULT_INTERNAL_ERROR);
    JIT_RETURN(JIT_CONST_INT32(MOT::RC_ERROR));

    return result;
}

static llvm::Value* GetLocalOrParamByVarNo(JitLlvmFunctionCodeGenContext* ctx, int varNo)
{
    llvm::Value* result = nullptr;
    if (varNo < (int)ctx->m_datums.size()) {
        result = ctx->m_datums[varNo];
        if (result == nullptr) {
            MOT_LOG_TRACE("Cannot retrieve parameter or local variable in index %d: entry is nullptr", varNo);
            MOT_ASSERT(false);
        }
    } else {
        MOT_LOG_TRACE("Cannot retrieve parameter or local variable: invalid index %d", varNo);
    }
    return result;
}

static llvm::Value* GetIsNullLocalOrParamByVarNo(JitLlvmFunctionCodeGenContext* ctx, int varNo)
{
    llvm::Value* result = nullptr;
    if (varNo < (int)ctx->m_datumNulls.size()) {
        result = ctx->m_datumNulls[varNo];
        if (!result) {
            MOT_LOG_TRACE("Cannot retrieve null for parameter or local variable in index %d: entry is nullptr", varNo);
            MOT_ASSERT(false);
        }
    } else {
        MOT_LOG_TRACE("Cannot retrieve null for parameter or local variable: invalid index %d", varNo);
    }
    return result;
}

static bool ExecSimpleCheckNode(Node* node)
{
    if (node == nullptr) {
        return true;
    }

    switch (nodeTag(node)) {
        case T_Const:
            return true;

        case T_Param:
            return true;

        case T_ArrayRef: {
            ArrayRef* expr = (ArrayRef*)node;
            if (!ExecSimpleCheckNode((Node*)expr->refupperindexpr)) {
                return false;
            }
            if (!ExecSimpleCheckNode((Node*)expr->reflowerindexpr)) {
                return false;
            }
            if (!ExecSimpleCheckNode((Node*)expr->refexpr)) {
                return false;
            }
            if (!ExecSimpleCheckNode((Node*)expr->refassgnexpr)) {
                return false;
            }
            return true;
        }

        case T_FuncExpr: {
            FuncExpr* expr = (FuncExpr*)node;
            if (expr->funcretset) {
                return false;
            }
            if (!ExecSimpleCheckNode((Node*)expr->args)) {
                return false;
            }
            if (!IsFunctionSupported(expr)) {
                return false;
            }
            return true;
        }

        case T_OpExpr: {
            OpExpr* expr = (OpExpr*)node;
            if (expr->opretset) {
                return false;
            }
            if (!ExecSimpleCheckNode((Node*)expr->args)) {
                return false;
            }
            return true;
        }

        case T_DistinctExpr: {
            DistinctExpr* expr = (DistinctExpr*)node;
            if (expr->opretset) {
                return false;
            }
            if (!ExecSimpleCheckNode((Node*)expr->args)) {
                return false;
            }
            return true;
        }

        case T_NullIfExpr: {
            NullIfExpr* expr = (NullIfExpr*)node;
            if (expr->opretset) {
                return false;
            }
            if (!ExecSimpleCheckNode((Node*)expr->args)) {
                return false;
            }
            return true;
        }

        case T_ScalarArrayOpExpr: {
            ScalarArrayOpExpr* expr = (ScalarArrayOpExpr*)node;
            if (!ExecSimpleCheckNode((Node*)expr->args)) {
                return false;
            }
            return true;
        }

        case T_BoolExpr: {
            BoolExpr* expr = (BoolExpr*)node;
            if (!ExecSimpleCheckNode((Node*)expr->args)) {
                return false;
            }
            return true;
        }

        case T_FieldSelect:
            return ExecSimpleCheckNode((Node*)((FieldSelect*)node)->arg);

        case T_FieldStore: {
            FieldStore* expr = (FieldStore*)node;
            if (!ExecSimpleCheckNode((Node*)expr->arg)) {
                return false;
            }
            if (!ExecSimpleCheckNode((Node*)expr->newvals)) {
                return false;
            }
            return true;
        }

        case T_RelabelType:
            return ExecSimpleCheckNode((Node*)((RelabelType*)node)->arg);

        case T_CoerceViaIO:
            return ExecSimpleCheckNode((Node*)((CoerceViaIO*)node)->arg);

        case T_ArrayCoerceExpr:
            return ExecSimpleCheckNode((Node*)((ArrayCoerceExpr*)node)->arg);

        case T_ConvertRowtypeExpr:
            return ExecSimpleCheckNode((Node*)((ConvertRowtypeExpr*)node)->arg);

        case T_CaseExpr: {
            CaseExpr* expr = (CaseExpr*)node;
            if (!ExecSimpleCheckNode((Node*)expr->arg)) {
                return false;
            }
            if (!ExecSimpleCheckNode((Node*)expr->args)) {
                return false;
            }
            if (!ExecSimpleCheckNode((Node*)expr->defresult)) {
                return false;
            }
            return true;
        }

        case T_CaseWhen: {
            CaseWhen* when = (CaseWhen*)node;
            if (!ExecSimpleCheckNode((Node*)when->expr)) {
                return false;
            }
            if (!ExecSimpleCheckNode((Node*)when->result)) {
                return false;
            }
            return true;
        }

        case T_CaseTestExpr:
            return true;

        case T_ArrayExpr: {
            ArrayExpr* expr = (ArrayExpr*)node;
            if (!ExecSimpleCheckNode((Node*)expr->elements)) {
                return false;
            }
            return true;
        }

        case T_RowExpr: {
            RowExpr* expr = (RowExpr*)node;
            if (!ExecSimpleCheckNode((Node*)expr->args)) {
                return false;
            }
            return true;
        }

        case T_RowCompareExpr: {
            RowCompareExpr* expr = (RowCompareExpr*)node;
            if (!ExecSimpleCheckNode((Node*)expr->largs)) {
                return false;
            }
            if (!ExecSimpleCheckNode((Node*)expr->rargs)) {
                return false;
            }
            return true;
        }

        case T_CoalesceExpr: {
            CoalesceExpr* expr = (CoalesceExpr*)node;
            if (!ExecSimpleCheckNode((Node*)expr->args)) {
                return false;
            }
            return true;
        }

        case T_MinMaxExpr: {
            MinMaxExpr* expr = (MinMaxExpr*)node;
            if (!ExecSimpleCheckNode((Node*)expr->args)) {
                return false;
            }
            return true;
        }

        case T_XmlExpr: {
            XmlExpr* expr = (XmlExpr*)node;
            if (!ExecSimpleCheckNode((Node*)expr->named_args)) {
                return false;
            }
            if (!ExecSimpleCheckNode((Node*)expr->args)) {
                return false;
            }
            return true;
        }

        case T_NullTest:
            return ExecSimpleCheckNode((Node*)((NullTest*)node)->arg);

        case T_HashFilter:
            return ExecSimpleCheckNode((Node*)((HashFilter*)node)->arg);

        case T_BooleanTest:
            return ExecSimpleCheckNode((Node*)((BooleanTest*)node)->arg);

        case T_CoerceToDomain:
            return ExecSimpleCheckNode((Node*)((CoerceToDomain*)node)->arg);

        case T_CoerceToDomainValue:
            return true;

        case T_List: {
            List* expr = (List*)node;
            ListCell* l = nullptr;
            foreach (l, expr) {
                if (!ExecSimpleCheckNode((Node*)lfirst(l))) {
                    return false;
                }
            }
            return true;
        }

        default:
            return false;
    }
}

static bool RecheckExprSimplePlan(PLpgSQL_expr* expr, CachedPlan* cplan)
{
    PlannedStmt* stmt = nullptr;
    Plan* plan = nullptr;
    TargetEntry* tle = nullptr;

    // Initialize to "not simple", and remember the plan generation number we last checked.
    expr->expr_simple_expr = nullptr;
    expr->expr_simple_generation = cplan->generation;
    expr->expr_simple_need_snapshot = true;

    // 1. There must be one single plan tree
    if (list_length(cplan->stmt_list) != 1) {
        return false;
    }
    stmt = (PlannedStmt*)linitial(cplan->stmt_list);
    // 2. It must be a RESULT plan --> no scan's required
    if (!IsA(stmt, PlannedStmt)) {
        return false;
    }
    if (stmt->commandType != CMD_SELECT) {
        return false;
    }
    plan = stmt->planTree;
    if (!IsA(plan, BaseResult)) {
        return false;
    }

    // 3. Can't have any subplan or qual clause, either
    if (plan->lefttree != nullptr || plan->righttree != nullptr || plan->initPlan != nullptr || plan->qual != nullptr ||
        ((BaseResult*)plan)->resconstantqual != nullptr) {
        return false;
    }

    // 4. The plan must have a single attribute as result
    if (list_length(plan->targetlist) != 1) {
        return false;
    }
    tle = (TargetEntry*)linitial(plan->targetlist);
    // 5. Check that all the nodes in the expression are non-scary.
    if (!ExecSimpleCheckNode((Node*)tle->expr)) {
        return false;
    }

    // Yes - this is a simple expression.  Mark it as such, and initialize state to "not valid in current transaction".
    expr->expr_simple_expr = tle->expr;
    expr->expr_simple_state = nullptr;
    expr->expr_simple_in_use = false;
    expr->expr_simple_lxid = InvalidLocalTransactionId;
    // Also stash away the expression result type
    expr->expr_simple_type = exprType((Node*)tle->expr);
    return true;
}

static bool CheckExprSimplePlan(PLpgSQL_expr* expr, ExprQueryAttrs* attrs)
{
    // Attention: expression has already been checked for having exactly one plan source containing one query tree

    /// Initialize to "not simple", and remember the plan generation number we last checked.  (If we don't get as far
    // as obtaining a plan to check, we just leave expr_simple_generation set to 0.)
    expr->expr_simple_expr = nullptr;
    expr->expr_simple_generation = 0;
    expr->expr_simple_need_snapshot = true;

    // Do some checking on the analyzed-and-rewritten form of the query. These checks are basically redundant with the
    // tests in exec_simple_recheck_plan, but the point is to avoid building a plan if possible.  Since this function
    // is only called immediately after creating the CachedPlanSource, we need not worry about the query being stale.

    Query* query = attrs->m_query;

    // 1. It must be a plain SELECT query without any input tables
    if (!IsA(query, Query)) {
        return false;
    }
    if (query->commandType != CMD_SELECT) {
        return false;
    }
    if (query->rtable != NIL) {
        return false;
    }

    // 2. Can't have any subplans, aggregates, qual clauses either
    if (query->hasAggs || query->hasWindowFuncs || query->hasSubLinks || query->hasForUpdate || query->cteList ||
        query->jointree->quals || query->groupClause || query->havingQual || query->windowClause ||
        query->distinctClause || query->sortClause || query->limitOffset || query->limitCount || query->setOperations) {
        return false;
    }

    // 3. The query must have a single attribute as result
    if (list_length(query->targetList) != 1) {
        return false;
    }

    // OK, it seems worth constructing a plan for more careful checking.

    // Get the generic plan for the query
    // Can't fail, because we have a single cached plan source
    CachedPlan* cplan = SPI_plan_get_cached_plan(expr->plan);
    MOT_ASSERT(cplan != nullptr);

    // Share the remaining work with recheck code path
    bool result = RecheckExprSimplePlan(expr, cplan);

    // Release our plan ref-count
    ReleaseCachedPlan(cplan, expr->plan->saved);

    return result;
}

static llvm::Value* ProcessConstExpr(JitLlvmFunctionCodeGenContext* ctx, Const* constExpr, Oid* resultType)
{
    *resultType = constExpr->consttype;
    llvm::Type* type = GetOidType(ctx, constExpr->consttype);
    if (type == nullptr) {
        MOT_LOG_TRACE("Unsupported constant type: %d", (int)constExpr->consttype);
        return nullptr;
    }

    llvm::Value* result = nullptr;
    if (IsTypeSupported(constExpr->consttype) || (constExpr->consttype == UNKNOWNOID)) {
        AddSetExprIsNull(ctx, JIT_CONST_INT32(constExpr->constisnull));
        AddSetExprCollation(ctx, JIT_CONST_INT32(constExpr->constcollid));
        if (IsPrimitiveType(constExpr->consttype)) {
            result = JIT_CONST_INT64(constExpr->constvalue);
        } else {
            int constId = AllocateConstId(ctx, constExpr->consttype, constExpr->constvalue, constExpr->constisnull);
            if (constId == -1) {
                MOT_LOG_TRACE("Failed to allocate constant identifier");
            } else {
                result = AddGetConstAt(ctx, constId);
            }
        }
    }
    return result;
}

static llvm::Value* ProcessParamExpr(JitLlvmFunctionCodeGenContext* ctx, Param* param, Oid* resultType)
{
    // we expect external parameter kind, with index from 1 to N
    llvm::Type* type = GetOidType(ctx, param->paramtype);
    if (type == nullptr) {
        MOT_LOG_TRACE("Unsupported parameter type: %d", (int)param->paramtype);
        return nullptr;
    }

    *resultType = param->paramtype;

    int varNo = param->paramid - 1;
    JIT_DEBUG_LOG_INT("Retrieving param from varno: %d", varNo);
    llvm::Value* var = GetIsNullLocalOrParamByVarNo(ctx, varNo);
    llvm::Value* isNull = ctx->m_builder->CreateLoad(var, true);
    JIT_DEBUG_LOG_INT_VAR("param is null value: %d", isNull);
    llvm::Value* isNullInt = ctx->m_builder->CreateCast(llvm::Instruction::SExt, isNull, ctx->INT32_T);
    AddSetExprIsNull(ctx, isNullInt);
    AddSetExprCollation(ctx, JIT_CONST_INT32(param->paramcollid));

    // this is a varno reference and not an input param reference
    llvm::Value* result = GetLocalOrParamByVarNo(ctx, varNo);
    return ctx->m_builder->CreateLoad(result, true);
}

static llvm::Value* ProcessArrayRefExpr(JitLlvmFunctionCodeGenContext* ctx, ArrayRef* arrayRef, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported array ref expression");
    return nullptr;
}

static llvm::Value* AddInvokePGFunction(JitLlvmFunctionCodeGenContext* ctx, Oid funcId, uint32_t argNum,
    Oid funcCollationId, Oid resultCollationId, llvm::Value** args, llvm::Value** argIsNull, Oid* argTypes)
{
    uint32_t argCount = 0;
    bool isStrict = false;
    PGFunction funcPtr = GetPGFunctionInfo(funcId, &argCount, &isStrict);
    if (funcPtr == nullptr) {
        MOT_LOG_TRACE("AddInvokePGFunction(): Cannot find function by id: %u", (unsigned)funcId);
        return nullptr;
    }
    // some functions (like text_concat) seem to be able to receive more arguments than stated, so we don't assert
    if (argCount != argNum) {
        MOT_LOG_TRACE("AddInvokePGFunction(): number of arguments passed %u does not equal function argument count %u",
            argNum,
            argCount);
    }

    llvm::Value* result = nullptr;
    if (argNum == 0) {
        result = AddInvokePGFunction0(ctx, funcPtr, funcCollationId);
    } else if (argNum == 1) {
        result = AddInvokePGFunction1(ctx, funcPtr, funcCollationId, isStrict, args[0], argIsNull[0], argTypes[0]);
    } else if (argNum == 2) {
        result = AddInvokePGFunction2(ctx,
            funcPtr,
            funcCollationId,
            isStrict,
            args[0],
            argIsNull[0],
            argTypes[0],
            args[1],
            argIsNull[1],
            argTypes[1]);
    } else if (argNum == 3) {
        result = AddInvokePGFunction3(ctx,
            funcPtr,
            funcCollationId,
            isStrict,
            args[0],
            argIsNull[0],
            argTypes[0],
            args[1],
            argIsNull[1],
            argTypes[1],
            args[2],
            argIsNull[2],
            argTypes[2]);
    } else {
        result = AddInvokePGFunctionN(ctx, funcPtr, funcCollationId, isStrict, args, argIsNull, argTypes, argCount);
    }
    CheckThrownException(ctx);
    AddSetExprCollation(ctx, JIT_CONST_INT32(resultCollationId));
    return result;
}

static llvm::Value* ProcessFuncCall(JitLlvmFunctionCodeGenContext* ctx, Oid funcId, List* args, Oid funcCollationId,
    Oid resultCollationId, const char* exprName)
{
    llvm::Value* result = nullptr;
    llvm::Value** argArray = nullptr;
    llvm::Value** argIsNull = nullptr;
    Oid* argTypes = nullptr;
    int argCount = list_length(args);
    if (argCount > 0) {
        size_t allocSize = sizeof(llvm::Value*) * argCount;
        argArray = (llvm::Value**)MOT::MemSessionAlloc(allocSize);
        if (argArray == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "JIT Compile",
                "Failed to allocate %u bytes for %d arguments in function expression",
                allocSize,
                argCount);
            return nullptr;
        }

        argIsNull = (llvm::Value**)MOT::MemSessionAlloc(allocSize);
        if (argIsNull == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "JIT Compile",
                "Failed to allocate %u bytes for %d argument nulls in function expression",
                allocSize,
                argCount);
            MOT::MemSessionFree(argArray);
            return nullptr;
        }

        allocSize = sizeof(Oid) * argCount;
        argTypes = (Oid*)MOT::MemSessionAlloc(allocSize);
        if (argTypes == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "JIT Compile",
                "Failed to allocate %u bytes for %d argument types in function expression",
                allocSize,
                argCount);
            MOT::MemSessionFree(argArray);
            MOT::MemSessionFree(argIsNull);
            return nullptr;
        }

        int argNum = 0;
        Oid argResultType = 0;
        ListCell* lc;
        foreach (lc, args) {
            Expr* subExpr = (Expr*)lfirst(lc);
            argArray[argNum] = ProcessExpr(ctx, subExpr, &argResultType);
            if (argArray[argNum] == nullptr) {
                MOT_LOG_TRACE("Failed to process function sub-expression %d", argNum);
                MOT::MemSessionFree(argArray);
                MOT::MemSessionFree(argIsNull);
                MOT::MemSessionFree(argTypes);
                return nullptr;
            }
            argIsNull[argNum] = AddGetExprIsNull(ctx);
            argTypes[argNum] = argResultType;
            ++argNum;
        }
        MOT_ASSERT(argNum == argCount);
    }

    result =
        AddInvokePGFunction(ctx, funcId, argCount, funcCollationId, resultCollationId, argArray, argIsNull, argTypes);

    if (argCount > 0) {
        MOT::MemSessionFree(argArray);
        MOT::MemSessionFree(argIsNull);
        MOT::MemSessionFree(argTypes);
    }

    return result;
}

static llvm::Value* ProcessFuncExpr(JitLlvmFunctionCodeGenContext* ctx, FuncExpr* funcExpr, Oid* resultType)
{
    *resultType = funcExpr->funcresulttype;
    return ProcessFuncCall(
        ctx, funcExpr->funcid, funcExpr->args, funcExpr->inputcollid, funcExpr->funccollid, "function");
}

static llvm::Value* ProcessOpExpr(JitLlvmFunctionCodeGenContext* ctx, OpExpr* opExpr, Oid* resultType)
{
    *resultType = opExpr->opresulttype;
    return ProcessFuncCall(ctx, opExpr->opfuncid, opExpr->args, opExpr->inputcollid, opExpr->opcollid, "operator");
}

static llvm::Value* ProcessDistinctExpr(JitLlvmFunctionCodeGenContext* ctx, DistinctExpr* distinctExpr, Oid* resultType)
{
    // we should implement "x IS DISTINCT FROM y"
    // the actual OpExpr provided implements an "equals" operator, and we should invert the result
    // in addition, we can avoid calling the operator expression if any of the arguments is null, since the result can
    // be computed directly

    // we expect exactly two parameters
    int argCount = list_length(distinctExpr->args);
    if (argCount != MOT_JIT_MAX_BOOL_EXPR_ARGS) {
        MOT_LOG_TRACE("Disqualifying SP: Distinct operator without two parameters, but rather %d", argCount);
        return nullptr;
    }

    *resultType = distinctExpr->opresulttype;

    // check if any of the arguments is null
    Oid lhsResultType = InvalidOid;
    Oid rhsResultType = InvalidOid;
    Expr* lhs = (Expr*)linitial(distinctExpr->args);
    Expr* rhs = (Expr*)lsecond(distinctExpr->args);
    llvm::Value* lhsArg = ProcessExpr(ctx, lhs, &lhsResultType);
    if (lhsArg == nullptr) {
        MOT_LOG_TRACE("Failed to process distinct LHS expression");
        return nullptr;
    }
    llvm::Value* lhsArgIsNull = AddGetExprIsNull(ctx);
    llvm::Value* rhsArg = ProcessExpr(ctx, rhs, &rhsResultType);
    if (rhsArg == nullptr) {
        MOT_LOG_TRACE("Failed to process distinct RHS expression");
        return nullptr;
    }
    llvm::Value* rhsArgIsNull = AddGetExprIsNull(ctx);

    // we must define a local variable for this to work properly
    llvm::Value* distinctResult = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "distinct_result");
    ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(false)), distinctResult, true);
    AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
    AddSetExprCollation(ctx, JIT_CONST_INT32(distinctExpr->opcollid));

    bool failed = false;
    JIT_IF_BEGIN(distinct_op_lhs_null);
    JIT_IF_EVAL(lhsArgIsNull);
    {
        // LHS arg is null: if rhs is also null then they are not distinct, so return 0, otherwise return 1
        llvm::Value* rhsArgIsNullBool = ctx->m_builder->CreateCast(llvm::Instruction::Trunc, rhsArgIsNull, ctx->INT1_T);
        llvm::Value* result = ctx->m_builder->CreateSelect(
            rhsArgIsNullBool, JIT_CONST_INT64(BoolGetDatum(false)), JIT_CONST_INT64(BoolGetDatum(true)));
        ctx->m_builder->CreateStore(result, distinctResult, true);
    }
    JIT_ELSE();
    {
        // at this point we know that lhs arg is not null, so if rhs is null then they are definitely distinct
        JIT_IF_BEGIN(distinct_op_rhs_null);
        JIT_IF_EVAL(rhsArgIsNull);
        {
            ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(true)), distinctResult, true);
        }
        JIT_ELSE();
        {
            // at this point both are not null, so we compute the equals operator and invert the result
            uint32_t funcArgCount = 0;
            bool isStrict = false;
            PGFunction funcPtr = GetPGFunctionInfo(distinctExpr->opfuncid, &funcArgCount, &isStrict);
            if (funcPtr == nullptr) {
                MOT_LOG_TRACE(
                    "ProcessDistinctExpr(): Cannot find operator function by id: %u", (unsigned)distinctExpr->opfuncid);
                failed = true;
            } else {
                MOT_ASSERT(funcArgCount == MOT_JIT_MAX_BOOL_EXPR_ARGS);
                if (funcArgCount != MOT_JIT_MAX_BOOL_EXPR_ARGS) {
                    MOT_LOG_TRACE(
                        "ProcessDistinctExpr(): Invalid argument count %u for function %u in distinct operator",
                        funcArgCount,
                        (unsigned)distinctExpr->opfuncid);
                    failed = true;
                } else {
                    // compute the equals operator and invert the result
                    llvm::Value* result = AddInvokePGFunction2(ctx,
                        funcPtr,
                        distinctExpr->inputcollid,
                        isStrict,
                        lhsArg,
                        lhsArgIsNull,
                        lhsResultType,
                        rhsArg,
                        rhsArgIsNull,
                        rhsResultType);
                    CheckThrownException(ctx);
                    llvm::Value* resultBool = ctx->m_builder->CreateCast(llvm::Instruction::Trunc, result, ctx->INT1_T);
                    llvm::Value* inverseResult = ctx->m_builder->CreateSelect(
                        resultBool, JIT_CONST_INT64(BoolGetDatum(false)), JIT_CONST_INT64(BoolGetDatum(true)));
                    ctx->m_builder->CreateStore(inverseResult, distinctResult, true);
                }
            }
        }
        JIT_IF_END();
    }
    JIT_IF_END();

    if (failed) {
        return nullptr;
    }
    return ctx->m_builder->CreateLoad(distinctResult, true);
}

static llvm::Value* ProcessNullIfExpr(JitLlvmFunctionCodeGenContext* ctx, NullIfExpr* nullIfExpr, Oid* resultType)
{
    // code adapted from ExecEvalNullIf() in execQual.cpp
    // we expect exactly two parameters
    int argCount = list_length(nullIfExpr->args);
    if (argCount != MOT_JIT_MAX_BOOL_EXPR_ARGS) {
        MOT_LOG_TRACE("Disqualifying SP: Null-if operator without two parameters, but rather %d", argCount);
        return nullptr;
    }
    *resultType = nullIfExpr->opresulttype;

    // evaluate arguments
    Oid lhsResultType = InvalidOid;
    Oid rhsResultType = InvalidOid;
    Expr* lhs = (Expr*)linitial(nullIfExpr->args);
    Expr* rhs = (Expr*)lsecond(nullIfExpr->args);
    llvm::Value* lhsArg = ProcessExpr(ctx, lhs, &lhsResultType);
    if (lhsArg == nullptr) {
        MOT_LOG_TRACE("Failed to process Null-if LHS expression");
        return nullptr;
    }
    llvm::Value* lhsArgIsNull = AddGetExprIsNull(ctx);
    llvm::Value* rhsArg = ProcessExpr(ctx, rhs, &rhsResultType);
    if (rhsArg == nullptr) {
        MOT_LOG_TRACE("Failed to process Null-if RHS expression");
        return nullptr;
    }
    llvm::Value* rhsArgIsNull = AddGetExprIsNull(ctx);

    // we must define a local variable for this to work properly
    llvm::Value* nullIfResult = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "nulif_result");
    ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(false)), nullIfResult, true);
    AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
    AddSetExprCollation(ctx, JIT_CONST_INT32(nullIfExpr->opcollid));

    bool failed = false;
    JIT_DEFINE_BLOCK(nullif_post_cmp);

    // if either argument is NULL they can't be equal
    JIT_IF_BEGIN(nullif_op_lhs_null);
    JIT_IF_EVAL_NOT(lhsArgIsNull);
    {
        JIT_IF_BEGIN(nullif_op_rhs_null);
        JIT_IF_EVAL_NOT(rhsArgIsNull);
        {
            // at this point both are not null, so we compute the equals operator and invert the result
            uint32_t funcArgCount = 0;
            bool isStrict = false;
            PGFunction funcPtr = GetPGFunctionInfo(nullIfExpr->opfuncid, &funcArgCount, &isStrict);
            if (funcPtr == nullptr) {
                MOT_LOG_TRACE(
                    "ProcessNullIfExpr(): Cannot find operator function by id: %u", (unsigned)nullIfExpr->opfuncid);
                failed = true;
            } else {
                MOT_ASSERT(funcArgCount == MOT_JIT_MAX_BOOL_EXPR_ARGS);
                if (funcArgCount != MOT_JIT_MAX_BOOL_EXPR_ARGS) {
                    MOT_LOG_TRACE("ProcessNullIfExpr(): Invalid argument count %u for function %u in distinct operator",
                        funcArgCount,
                        (unsigned)nullIfExpr->opfuncid);
                    failed = true;
                } else {
                    // compute the equals operator and see if the result is non-null true
                    llvm::Value* result = AddInvokePGFunction2(ctx,
                        funcPtr,
                        nullIfExpr->inputcollid,
                        isStrict,
                        lhsArg,
                        lhsArgIsNull,
                        lhsResultType,
                        rhsArg,
                        rhsArgIsNull,
                        rhsResultType);
                    CheckThrownException(ctx);
                    llvm::Value* resIsNull = AddGetExprIsNull(ctx);
                    JIT_IF_BEGIN(nullif_res_not_null);
                    JIT_IF_EVAL_NOT(resIsNull);
                    {
                        JIT_IF_BEGIN(nullif_res_value_true);
                        JIT_IF_EVAL(result);
                        {
                            // the arguments are equal, so return null
                            AddSetExprIsNull(ctx, JIT_CONST_INT32(1));
                            ctx->m_builder->CreateStore(JIT_CONST_INT64((Datum)0), nullIfResult, true);
                            JIT_GOTO(nullif_post_cmp);
                        }
                        JIT_IF_END();
                    }
                    JIT_IF_END();
                }
            }
        }
        JIT_IF_END();
    }
    JIT_IF_END();

    if (failed) {
        return nullptr;
    }

    // return the first argument
    AddSetExprIsNull(ctx, lhsArgIsNull);
    ctx->m_builder->CreateStore(lhsArg, nullIfResult, true);

    ctx->m_builder->SetInsertPoint(nullif_post_cmp);
    return nullIfResult;
}

static llvm::Value* ProcessScalarConstArrayOpExpr(JitLlvmFunctionCodeGenContext* ctx, ScalarArrayOpExpr* scalarExpr,
    llvm::Value* lhsValue, llvm::Value* lhsIsNull, Oid lhsArgType, Const* constArrayExpr)
{
    AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
    AddSetExprCollation(ctx, JIT_CONST_INT32(InvalidOid));
    Datum arrayDatum = constArrayExpr->constvalue;
    ArrayType* arrayType = DatumGetArrayTypeP(arrayDatum);
    int nitems = ArrayGetNItems(ARR_NDIM(arrayType), ARR_DIMS(arrayType));
    if (nitems <= 0) {
        return JIT_CONST_INT64(BoolGetDatum(!scalarExpr->useOr));
    }

    // get the function pointer
    uint32_t args = 0;
    bool isStrict = false;
    PGFunction funcPtr = GetPGFunctionInfo(scalarExpr->opfuncid, &args, &isStrict);
    if (funcPtr == nullptr) {
        MOT_LOG_TRACE(
            "ProcessScalarConstArrayOpExpr(): Cannot find function by id: %u", (unsigned)scalarExpr->opfuncid);
        return nullptr;
    }
    MOT_ASSERT(args == 2);

    // get array element type data once
    Oid elementType = ARR_ELEMTYPE(arrayType);
    int16 typeLen = 0;
    bool typeByVal = true;
    char typeAlign = 0;
    get_typlenbyvalalign(elementType, &typeLen, &typeByVal, &typeAlign);
    if (!IsTypeSupported(elementType)) {
        MOT_LOG_TRACE("ProcessScalarConstArrayOpExpr(): Constant array element type %u unsupported", elementType);
        return nullptr;
    }

    // we must define a local variable for this to work properly
    llvm::Value* scalarResult = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "scalar_result");
    ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(!scalarExpr->useOr)), scalarResult, true);

    // evaluate result
    bool failed = false;
    JIT_IF_BEGIN(scalar_lhs_is_null);
    // if the scalar is NULL, and the function is strict, return NULL
    // for simpler logic, we embed the isStrict constant in the generated code
    llvm::BasicBlock* postIfBlock = JIT_IF_POST_BLOCK();
    llvm::Value* cond = ctx->m_builder->CreateSelect(JIT_CONST_INT1(isStrict), lhsIsNull, JIT_CONST_INT32(0));
    JIT_IF_EVAL(cond);
    {
        // if the scalar is NULL, and the function is strict, return NULL
        AddSetExprIsNull(ctx, JIT_CONST_INT32(1));
    }
    JIT_ELSE();
    {
        char* arrayData = (char*)ARR_DATA_PTR(arrayType);
        bits8* bitmap = ARR_NULLBITMAP(arrayType);
        int bitmask = 1;

        // iterate over array elements, evaluate each one, and compare to LHS value
        for (int i = 0; i < nitems; i++) {
            Datum elementValue = PointerGetDatum(nullptr);
            bool isNull = true;

            // get array element, checking for NULL
            if (bitmap && (*bitmap & bitmask) == 0) {
                MOT_LOG_TRACE("Element %d is null", i);
            } else {
                elementValue = fetch_att(arrayData, typeByVal, typeLen);
                arrayData = att_addlength_pointer(arrayData, typeLen, arrayData);
                arrayData = (char*)att_align_nominal(arrayData, typeAlign);
                isNull = false;
            }

            // get the datum as a constant
            int constId = AllocateConstId(ctx, elementType, elementValue, isNull);
            if (constId == -1) {
                MOT_LOG_TRACE("Failed to allocate constant identifier");
                failed = true;
                break;
            }

            llvm::Value* element = AddGetConstAt(ctx, constId);
            llvm::Value* elementIsNull = JIT_CONST_INT32(isNull ? 1 : 0);

            // invoke function
            llvm::Value* cmpRes = AddInvokePGFunction2(ctx,
                funcPtr,
                scalarExpr->inputcollid,
                isStrict,
                lhsValue,
                lhsIsNull,
                lhsArgType,
                element,
                elementIsNull,
                elementType);
            CheckThrownException(ctx);
            llvm::Value* cmpResNull = AddGetExprIsNull(ctx);
            JIT_IF_BEGIN(cmp_res_null);
            JIT_IF_EVAL(cmpResNull);
            {
                // if comparison result is null, then entire result is null
                AddSetExprIsNull(ctx, JIT_CONST_INT32(1));
                JIT_GOTO(postIfBlock);
            }
            JIT_ELSE();
            {
                if (scalarExpr->useOr) {
                    JIT_IF_BEGIN(eval_cmp);
                    JIT_IF_EVAL(cmpRes);
                    {
                        ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(true)), scalarResult, true);
                        AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
                        JIT_GOTO(postIfBlock);
                    }
                    JIT_IF_END();
                } else {
                    JIT_IF_BEGIN(eval_cmp);
                    JIT_IF_EVAL_NOT(cmpRes);
                    {
                        ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(false)), scalarResult, true);
                        AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
                        JIT_GOTO(postIfBlock);
                    }
                    JIT_IF_END();
                }
            }
            JIT_IF_END();

            // advance bitmap pointer if any
            if (bitmap != nullptr) {
                bitmask <<= 1;
                if (bitmask == 0x100) {
                    bitmap++;
                    bitmask = 1;
                }
            }
        }
    }
    JIT_IF_END();

    if (failed) {
        return nullptr;
    }
    return ctx->m_builder->CreateLoad(scalarResult, true);
}

static llvm::Value* ProcessScalarArrayOpExpr(
    JitLlvmFunctionCodeGenContext* ctx, ScalarArrayOpExpr* scalarExpr, Oid* resultType)
{
    *resultType = BOOLOID;
    MOT_ASSERT(list_length(scalarExpr->args) == 2);
    Expr* lhsExpr = (Expr*)linitial(scalarExpr->args);
    Oid lhsArgType = 0;
    llvm::Value* lhsValue = ProcessExpr(ctx, lhsExpr, &lhsArgType);
    if (lhsValue == nullptr) {
        MOT_LOG_TRACE("Failed to parse LHS expression in Scalar Array Op");
        return nullptr;
    }
    llvm::Value* lhsIsNull = AddGetExprIsNull(ctx);

    // we might get array expression or constant text array expression
    Expr* expr = (Expr*)lsecond(scalarExpr->args);
    if (expr->type == T_Const) {
        Const* constArrayExpr = (Const*)expr;
        return ProcessScalarConstArrayOpExpr(ctx, scalarExpr, lhsValue, lhsIsNull, lhsArgType, constArrayExpr);
    } else if (expr->type != T_ArrayExpr) {
        MOT_LOG_TRACE("Failed to process scalar array operation: unsupported array node type %d", expr->type);
        return nullptr;
    }

    // do some simple checks first
    ArrayExpr* arrayExpr = (ArrayExpr*)expr;
    if (arrayExpr->multidims) {
        MOT_LOG_TRACE("Unsupported multi-dimensional array in Scalar Array Op");
        return nullptr;
    }
    if (!IsTypeSupported(arrayExpr->element_typeid)) {
        MOT_LOG_TRACE("Unsupported array element type %u in Scalar Array Op", (unsigned)arrayExpr->element_typeid);
        return nullptr;
    }
    AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
    AddSetExprCollation(ctx, JIT_CONST_INT32(InvalidOid));
    int nitems = list_length(arrayExpr->elements);
    if (nitems <= 0) {
        return JIT_CONST_INT64(BoolGetDatum(!scalarExpr->useOr));
    }

    // get the function pointer
    uint32_t args = 0;
    bool isStrict = false;
    PGFunction funcPtr = GetPGFunctionInfo(scalarExpr->opfuncid, &args, &isStrict);
    if (funcPtr == nullptr) {
        MOT_LOG_TRACE("ProcessScalarArrayOpExpr(): Cannot find function by id: %u", (unsigned)scalarExpr->opfuncid);
        return nullptr;
    }
    MOT_ASSERT(args == 2);

    // we must define a local variable for this to work properly
    llvm::Value* scalarResult = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "scalar_result");
    ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(!scalarExpr->useOr)), scalarResult, true);

    // evaluate result
    bool failed = false;
    JIT_IF_BEGIN(scalar_lhs_is_null);
    // if the scalar is NULL, and the function is strict, return NULL
    // for simpler logic, we embed the isStrict constant in the generated code
    llvm::BasicBlock* postIfBlock = JIT_IF_POST_BLOCK();
    llvm::Value* cond = ctx->m_builder->CreateSelect(JIT_CONST_INT1(isStrict), lhsIsNull, JIT_CONST_INT32(0));
    JIT_IF_EVAL(cond);
    {
        // if the scalar is NULL, and the function is strict, return NULL
        AddSetExprIsNull(ctx, JIT_CONST_INT32(1));
    }
    JIT_ELSE();
    {
        // iterate over array elements, evaluate each one, and compare to LHS value
        Oid elementType = InvalidOid;
        AddSetExprIsNull(ctx, JIT_CONST_INT32(0));  // initially result is not null
        ListCell* arg = nullptr;
        foreach (arg, arrayExpr->elements) {
            Expr* e = (Expr*)lfirst(arg);
            llvm::Value* element = ProcessExpr(ctx, e, &elementType);
            if (element == nullptr) {
                MOT_LOG_TRACE("Failed to process distinct array element expression");
                failed = true;
                break;
            }
            llvm::Value* elementIsNull = AddGetExprIsNull(ctx);

            // invoke function
            llvm::Value* cmpRes = AddInvokePGFunction2(ctx,
                funcPtr,
                scalarExpr->inputcollid,
                isStrict,
                lhsValue,
                lhsIsNull,
                lhsArgType,
                element,
                elementIsNull,
                elementType);
            CheckThrownException(ctx);
            llvm::Value* cmpResNull = AddGetExprIsNull(ctx);
            JIT_IF_BEGIN(cmp_res_null);
            JIT_IF_EVAL(cmpResNull);
            {
                // if comparison result is null, then entire result is null
                AddSetExprIsNull(ctx, JIT_CONST_INT32(1));
                JIT_GOTO(postIfBlock);
            }
            JIT_ELSE();
            {
                if (scalarExpr->useOr) {
                    JIT_IF_BEGIN(eval_cmp);
                    JIT_IF_EVAL(cmpRes);
                    {
                        ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(true)), scalarResult, true);
                        AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
                        JIT_GOTO(postIfBlock);
                    }
                    JIT_IF_END();
                } else {
                    JIT_IF_BEGIN(eval_cmp);
                    JIT_IF_EVAL_NOT(cmpRes);
                    {
                        ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(false)), scalarResult, true);
                        AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
                        JIT_GOTO(postIfBlock);
                    }
                    JIT_IF_END();
                }
            }
            JIT_IF_END();
        }
    }
    JIT_IF_END();

    if (failed) {
        return nullptr;
    }
    return ctx->m_builder->CreateLoad(scalarResult, true);
}

static llvm::Value* ProcessBoolAndExpr(JitLlvmFunctionCodeGenContext* ctx, BoolExpr* boolExpr)
{
    // if any of the arguments is false we stop evaluation
    // if there was a null then the result is null
    // otherwise the result is true
    // and expression with no arguments evaluates to true
    // we generate a label to implement goto instruction for one of the cases above
    llvm::BasicBlock* boolAndPostBlock =
        llvm::BasicBlock::Create(ctx->m_codeGen->context(), "eval_bool_post_bb", ctx->m_jittedFunction);

    // in order to process correctly nulls we need a local variable
    llvm::Value* boolResult = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "bool_result");
    ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(true)), boolResult, true);
    llvm::Value* anyNulls = ctx->m_builder->CreateAlloca(ctx->INT32_T, 0, nullptr, "any_nulls");
    ctx->m_builder->CreateStore(JIT_CONST_INT32(0), anyNulls, true);
    ListCell* lc = nullptr;
    Oid resultType = 0;
    bool failed = false;
    foreach (lc, boolExpr->args) {
        llvm::Value* arg = ProcessExpr(ctx, (Expr*)lfirst(lc), &resultType);
        if (arg == nullptr) {
            MOT_LOG_TRACE("Failed to process argument expression in Boolean AND expression");
            failed = true;
            break;
        }
        llvm::Value* isNull = AddGetExprIsNull(ctx);
        JIT_IF_BEGIN(arg_is_null);
        JIT_IF_EVAL(isNull);
        {
            JIT_DEBUG_LOG("Encountered null argument in Boolean AND expression");
            ctx->m_builder->CreateStore(JIT_CONST_INT32(1), anyNulls, true);
        }
        JIT_ELSE();
        {
            JIT_IF_BEGIN(arg_is_false);
            JIT_IF_EVAL_NOT(arg);
            {
                JIT_DEBUG_LOG("Encountered false argument in Boolean AND expression");
                ctx->m_builder->CreateStore(arg, boolResult, true);
                JIT_GOTO(boolAndPostBlock);
            }
            JIT_IF_END();
        }
        JIT_IF_END();
    }
    if (failed) {
        return nullptr;
    }
    JIT_GOTO(boolAndPostBlock);

    ctx->m_builder->SetInsertPoint(boolAndPostBlock);
    llvm::Value* ret = ctx->m_builder->CreateLoad(boolResult, true);
    llvm::Value* retIsNull = ctx->m_builder->CreateLoad(anyNulls, true);
    JIT_IF_BEGIN(result_is_true);
    JIT_IF_EVAL_NOT(ret);
    {
        JIT_DEBUG_LOG("Boolean AND expression evaluated to false");
        AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
    }
    JIT_ELSE();
    {
        JIT_IF_BEGIN(any_null_found);
        JIT_IF_EVAL(retIsNull);
        {
            JIT_DEBUG_LOG("Boolean AND expression evaluated to true having nulls, setting false");
            ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(false)), boolResult, true);
            AddSetExprIsNull(ctx, JIT_CONST_INT32(1));
        }
        JIT_ELSE();
        {
            JIT_DEBUG_LOG("Boolean AND expression evaluated to true");
            AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
        }
        JIT_IF_END();
    }
    JIT_IF_END();
    AddSetExprCollation(ctx, JIT_CONST_INT32(InvalidOid));
    return ctx->m_builder->CreateLoad(boolResult, true);
}

static llvm::Value* ProcessBoolOrExpr(JitLlvmFunctionCodeGenContext* ctx, BoolExpr* boolExpr)
{
    // if any of the arguments is true we stop evaluation
    // if there was a null then the result is null
    // otherwise the result is true
    // OR expression with no arguments evaluates to false
    // we generate a label to implement goto instruction for one of the cases above
    llvm::BasicBlock* boolOrPostBlock =
        llvm::BasicBlock::Create(ctx->m_codeGen->context(), "eval_bool_post_bb", ctx->m_jittedFunction);

    // in order to process correctly nulls we need a local variable
    llvm::Value* boolResult = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "bool_result");
    ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(false)), boolResult, true);
    llvm::Value* anyNulls = ctx->m_builder->CreateAlloca(ctx->INT32_T, 0, nullptr, "any_nulls");
    ctx->m_builder->CreateStore(JIT_CONST_INT32(0), anyNulls, true);
    ListCell* lc = nullptr;
    Oid resultType = 0;
    bool failed = false;
    foreach (lc, boolExpr->args) {
        llvm::Value* arg = ProcessExpr(ctx, (Expr*)lfirst(lc), &resultType);
        if (arg == nullptr) {
            MOT_LOG_TRACE("Failed to process argument expression in Boolean OR expression");
            failed = true;
            break;
        }
        llvm::Value* isNull = AddGetExprIsNull(ctx);
        JIT_IF_BEGIN(arg_is_null);
        JIT_IF_EVAL(isNull);
        {
            JIT_DEBUG_LOG("Encountered null argument in Boolean OR expression");
            ctx->m_builder->CreateStore(JIT_CONST_INT32(1), anyNulls, true);
        }
        JIT_ELSE();
        {
            JIT_IF_BEGIN(arg_is_true);
            JIT_IF_EVAL(arg);
            {
                JIT_DEBUG_LOG("Encountered true argument in Boolean OR expression");
                ctx->m_builder->CreateStore(arg, boolResult, true);
                JIT_GOTO(boolOrPostBlock);
            }
            JIT_IF_END();
        }
        JIT_IF_END();
    }
    if (failed) {
        return nullptr;
    }
    JIT_GOTO(boolOrPostBlock);

    ctx->m_builder->SetInsertPoint(boolOrPostBlock);
    llvm::Value* ret = ctx->m_builder->CreateLoad(boolResult, true);
    llvm::Value* retIsNull = ctx->m_builder->CreateLoad(anyNulls, true);
    JIT_IF_BEGIN(result_is_true);
    JIT_IF_EVAL(ret);
    {
        JIT_DEBUG_LOG("Boolean OR expression evaluated to true");
        AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
    }
    JIT_ELSE();
    {
        JIT_IF_BEGIN(any_null_found);
        JIT_IF_EVAL(retIsNull);
        {
            JIT_DEBUG_LOG("Boolean OR expression evaluated to false having nulls");
            AddSetExprIsNull(ctx, JIT_CONST_INT32(1));
        }
        JIT_ELSE();
        {
            JIT_DEBUG_LOG("Boolean OR expression evaluated to false");
            AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
        }
        JIT_IF_END();
    }
    JIT_IF_END();
    AddSetExprCollation(ctx, JIT_CONST_INT32(InvalidOid));
    return ret;
}

static llvm::Value* ProcessBoolNotExpr(JitLlvmFunctionCodeGenContext* ctx, BoolExpr* boolExpr)
{
    MOT_ASSERT(list_length(boolExpr->args) == 1);
    Oid resultType = 0;
    llvm::Value* arg = ProcessExpr(ctx, (Expr*)linitial(boolExpr->args), &resultType);
    if (arg == nullptr) {
        MOT_LOG_TRACE("Failed to process Boolean NOT operator argument");
        return nullptr;
    }
    llvm::Value* argIsNull = AddGetExprIsNull(ctx);

    // in order to process correctly nulls we need a local variable
    llvm::Value* boolResult = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "bool_result");
    ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(false)), boolResult, true);

    AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
    AddSetExprCollation(ctx, JIT_CONST_INT32(InvalidOid));

    JIT_IF_BEGIN(arg_is_null);
    JIT_IF_EVAL(argIsNull);
    {
        AddSetExprIsNull(ctx, JIT_CONST_INT32(1));
    }
    JIT_ELSE();
    {
        llvm::Value* cond = ctx->m_builder->CreateCast(llvm::Instruction::Trunc, arg, ctx->INT1_T);
        llvm::Value* result = ctx->m_builder->CreateSelect(
            cond, JIT_CONST_INT64(BoolGetDatum(false)), JIT_CONST_INT64(BoolGetDatum(true)));
        ctx->m_builder->CreateStore(result, boolResult);
    }
    JIT_IF_END();

    return ctx->m_builder->CreateLoad(boolResult, true);
}

static llvm::Value* ProcessBoolExpr(JitLlvmFunctionCodeGenContext* ctx, BoolExpr* boolExpr, Oid* resultType)
{
    *resultType = BOOLOID;
    switch (boolExpr->boolop) {
        case AND_EXPR:
            return ProcessBoolAndExpr(ctx, boolExpr);

        case OR_EXPR:
            return ProcessBoolOrExpr(ctx, boolExpr);

        case NOT_EXPR:
            return ProcessBoolNotExpr(ctx, boolExpr);

        default:
            MOT_LOG_TRACE("Invalid Boolean expression: %d", (int)boolExpr->boolop);
            return nullptr;
    }
}

static llvm::Value* ProcessFieldSelect(
    JitLlvmFunctionCodeGenContext* ctx, FieldSelect* field_select_expr, Oid* resultType)
{
    llvm::Value* row = ProcessExpr(ctx, field_select_expr->arg, resultType);
    if (row == nullptr) {
        MOT_LOG_TRACE("Failed to evaluate row argument of field-select expression");
        return nullptr;
    }

    MOT_LOG_TRACE("Unsupported field-select expression");
    return nullptr;
}

static llvm::Value* ProcessFieldStore(JitLlvmFunctionCodeGenContext* ctx, FieldStore* expr, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported field-store expression");
    return nullptr;
}

static llvm::Value* ProcessRelabelType(JitLlvmFunctionCodeGenContext* ctx, RelabelType* expr, Oid* resultType)
{
    // relabel type is a dummy no-op node used only for type coercion for binary compatible types, so we just need to
    // execute the underlying node, and report back the type and collation specified in the relabel type node
    llvm::Value* result = ProcessExpr(ctx, expr->arg, resultType);
    if (result != nullptr) {
        // override result type and collation id
        *resultType = expr->resulttype;
        AddSetExprCollation(ctx, JIT_CONST_INT32(expr->resultcollid));
    }
    return result;
}

static llvm::Value* ProcessCoerceViaIO(JitLlvmFunctionCodeGenContext* ctx, CoerceViaIO* expr, Oid* resultType)
{
    Oid argType = InvalidOid;
    llvm::Value* arg = ProcessExpr(ctx, expr->arg, &argType);
    if (arg == nullptr) {
        MOT_LOG_TRACE("Failed to process coerce via IO expression input argument");
        return nullptr;
    }

    // call conversion function
    llvm::Value* result =
        AddConvertViaString(ctx, arg, JIT_CONST_INT32(argType), JIT_CONST_INT32(expr->resulttype), JIT_CONST_INT32(-1));
    if (result != nullptr) {
        *resultType = expr->resulttype;
        AddSetExprCollation(ctx, JIT_CONST_INT32(expr->resultcollid));
    }
    return result;
}

static llvm::Value* ProcessArrayCoerceExpr(JitLlvmFunctionCodeGenContext* ctx, ArrayCoerceExpr* expr, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported array-coerce expression");
    return nullptr;
}

static llvm::Value* ProcessConvertRowtypeExpr(
    JitLlvmFunctionCodeGenContext* ctx, ConvertRowtypeExpr* expr, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported convert-row-type expression");
    return nullptr;
}

static llvm::Value* ProcessCaseExpr(JitLlvmFunctionCodeGenContext* ctx, CaseExpr* expr, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported case expression");
    return nullptr;
}

static llvm::Value* ProcessCaseWhenExpr(JitLlvmFunctionCodeGenContext* ctx, CaseWhen* expr, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported case-when expression");
    return nullptr;
}

static llvm::Value* ProcessCaseTestExpr(JitLlvmFunctionCodeGenContext* ctx, CaseTestExpr* expr, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported case-test expression");
    return nullptr;
}

static llvm::Value* ProcessArrayExpr(JitLlvmFunctionCodeGenContext* ctx, ArrayExpr* expr, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported array expression");
    return nullptr;
}

static llvm::Value* ProcessRowExpr(JitLlvmFunctionCodeGenContext* ctx, RowExpr* expr, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported row expression");
    return nullptr;
}

static llvm::Value* ProcessRowCompareExpr(JitLlvmFunctionCodeGenContext* ctx, RowCompareExpr* expr, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported row-compare expression");
    return nullptr;
}

static llvm::Value* ProcessCoalesceExpr(JitLlvmFunctionCodeGenContext* ctx, CoalesceExpr* expr, Oid* resultType)
{
    // evaluate expressions in list one by one, return the first non-null, otherwise return null
    // we must define a local variable for this to work properly
    llvm::Value* coalesceResult = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "coalesce_result");
    ctx->m_builder->CreateStore(JIT_CONST_INT64(0), coalesceResult, true);
    std::list<llvm_util::JitIf*> ifStack;
    ListCell* arg = nullptr;
    bool result = true;
    foreach (arg, expr->args) {
        Expr* e = (Expr*)lfirst(arg);
        llvm::Value* value = ProcessExpr(ctx, e, resultType);
        if (value == nullptr) {
            MOT_LOG_TRACE("Failed to parse COALESCE argument");
            result = false;
            break;
        }
        llvm::Value* isNull = AddGetExprIsNull(ctx);
        llvm_util::JitIf* jitIf = new (std::nothrow) llvm_util::JitIf(ctx, "coalesce");
        if (jitIf == nullptr) {
            MOT_LOG_TRACE("Failed to allocate JIT IF statement");
            result = false;
            break;
        }
        ifStack.push_back(jitIf);
        jitIf->JitIfNot(isNull);
        {
            ctx->m_builder->CreateStore(value, coalesceResult, true);
            AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
        }
        jitIf->JitElse();
    }
    // terminate last if expression by marking expression as evaluated to null
    if (result) {
        AddSetExprIsNull(ctx, JIT_CONST_INT32(1));
        AddSetExprCollation(ctx, JIT_CONST_INT32(expr->coalescecollid));
    }
    while (!ifStack.empty()) {
        ifStack.back()->JitEnd();
        llvm_util::JitIf* jitIf = ifStack.back();
        if (result) {
            jitIf->JitEnd();
        }
        delete jitIf;
        ifStack.pop_back();
    }
    llvm::Value* finalResult = nullptr;
    if (result) {
        finalResult = ctx->m_builder->CreateLoad(coalesceResult, true);
    }
    return finalResult;
}

static llvm::Value* ProcessMinMaxExpr(JitLlvmFunctionCodeGenContext* ctx, MinMaxExpr* expr, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported min-max expression");
    return nullptr;
}

static llvm::Value* ProcessXmlExpr(JitLlvmFunctionCodeGenContext* ctx, MinMaxExpr* expr, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported xml expression");
    return nullptr;
}

static llvm::Value* ProcessNullTest(JitLlvmFunctionCodeGenContext* ctx, NullTest* expr, Oid* resultType)
{
    // we do not support null test for row type
    if (expr->argisrow) {
        MOT_LOG_TRACE("Disqualifying SP: unsupported null test for row type");
        return nullptr;
    }

    // evaluate expression
    llvm::Value* arg = ProcessExpr(ctx, expr->arg, resultType);
    if (arg == nullptr) {
        MOT_LOG_TRACE("Failed to process null test operator argument");
        return nullptr;
    }

    // evaluate result
    llvm::Value* argIsNull = AddGetExprIsNull(ctx);
    llvm::Value* argIsNullBool = ctx->m_builder->CreateCast(llvm::Instruction::Trunc, argIsNull, ctx->INT1_T);

    // mark current expression as not null and return result
    AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
    AddSetExprCollation(ctx, JIT_CONST_INT32(InvalidOid));
    if (expr->nulltesttype == IS_NULL) {
        return ctx->m_builder->CreateSelect(
            argIsNullBool, JIT_CONST_INT64(BoolGetDatum(true)), JIT_CONST_INT64(BoolGetDatum(false)));
    } else if (expr->nulltesttype == IS_NOT_NULL) {
        return ctx->m_builder->CreateSelect(
            argIsNullBool, JIT_CONST_INT64(BoolGetDatum(false)), JIT_CONST_INT64(BoolGetDatum(true)));
    } else {
        MOT_LOG_TRACE("Unrecognized null-test type: %d", (int)expr->nulltesttype);
        return nullptr;
    }
}

static llvm::Value* ProcessHashFilter(JitLlvmFunctionCodeGenContext* ctx, HashFilter* expr, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported hash-filter expression");
    return nullptr;
}

static llvm::Value* ProcessBooleanTest(JitLlvmFunctionCodeGenContext* ctx, BooleanTest* expr, Oid* resultType)
{
    // evaluate expression
    llvm::Value* arg = ProcessExpr(ctx, expr->arg, resultType);
    if (arg == nullptr) {
        MOT_LOG_TRACE("Failed to process Boolean test operator argument");
        return nullptr;
    }

    // evaluate result
    llvm::Value* argIsNull = AddGetExprIsNull(ctx);

    // mark current expression as not null and return result
    AddSetExprIsNull(ctx, JIT_CONST_INT32(0));
    AddSetExprCollation(ctx, JIT_CONST_INT32(InvalidOid));

    // we must define a local variable for this to work properly
    llvm::Value* boolResult = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "bool_result");
    ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(false)), boolResult, true);

    // by default the result is false, we relate only to changes into true value
    switch (expr->booltesttype) {
        case IS_TRUE: {
            JIT_IF_BEGIN(bool_is_true);
            JIT_IF_EVAL_NOT(argIsNull);
            {
                JIT_IF_BEGIN(eval_arg);
                JIT_IF_EVAL(arg);
                {
                    ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(true)), boolResult, true);
                }
                JIT_IF_END();
            }
            JIT_IF_END();
            break;
        }

        case IS_NOT_TRUE: {
            JIT_IF_BEGIN(bool_is_not_true);
            JIT_IF_EVAL(argIsNull);
            {
                // null value is not true, so result is true
                ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(true)), boolResult, true);
            }
            JIT_ELSE();
            {
                JIT_IF_BEGIN(eval_arg);
                JIT_IF_EVAL_NOT(arg);
                {
                    ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(true)), boolResult, true);
                }
                JIT_IF_END();
            }
            JIT_IF_END();
            break;
        }

        case IS_FALSE: {
            // null value is not considered false
            JIT_IF_BEGIN(bool_is_false);
            JIT_IF_EVAL_NOT(argIsNull);
            {
                JIT_IF_BEGIN(eval_arg);
                JIT_IF_EVAL_NOT(arg);
                {
                    ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(true)), boolResult, true);
                }
                JIT_IF_END();
            }
            JIT_IF_END();
            break;
        }

        case IS_NOT_FALSE: {
            // null value is not considered false
            JIT_IF_BEGIN(bool_is_not_false);
            JIT_IF_EVAL(argIsNull);
            {
                // null value is not true, so result is true
                ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(true)), boolResult, true);
            }
            JIT_ELSE();
            {
                JIT_IF_BEGIN(eval_arg);
                JIT_IF_EVAL(arg);
                {
                    ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(true)), boolResult, true);
                }
                JIT_IF_END();
            }
            JIT_IF_END();
            break;
        }

        case IS_UNKNOWN: {
            // null is considered unknown
            JIT_IF_BEGIN(bool_is_not_false);
            JIT_IF_EVAL(argIsNull);
            {
                // null value is not true, so result is true
                ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(true)), boolResult, true);
            }
            JIT_IF_END();
            break;
        }

        case IS_NOT_UNKNOWN: {
            // null is considered unknown
            JIT_IF_BEGIN(bool_is_not_false);
            JIT_IF_EVAL_NOT(argIsNull);
            {
                // null value is not true, so result is true
                ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(true)), boolResult, true);
            }
            JIT_IF_END();
            break;
        }

        default:
            MOT_LOG_TRACE("Unrecognized booltesttype: %d", (int)expr->booltesttype);
            return nullptr;
    }

    return ctx->m_builder->CreateLoad(boolResult, true);
}

static llvm::Value* ProcessCoerceToDomain(JitLlvmFunctionCodeGenContext* ctx, CoerceToDomain* expr, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported coerce-to-domain expression");
    return nullptr;
}

static llvm::Value* ProcessCoerceToDomainValue(
    JitLlvmFunctionCodeGenContext* ctx, CoerceToDomainValue* expr, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported coerce-to-domain-value expression");
    return nullptr;
}

static llvm::Value* ProcessExpr(JitLlvmFunctionCodeGenContext* ctx, Expr* expr, Oid* resultType)
{
    switch (nodeTag(expr)) {
        case T_Const:
            return ProcessConstExpr(ctx, (Const*)expr, resultType);

        case T_Param:
            return ProcessParamExpr(ctx, (Param*)expr, resultType);

        case T_ArrayRef:
            return ProcessArrayRefExpr(ctx, (ArrayRef*)expr, resultType);

        case T_FuncExpr:
            return ProcessFuncExpr(ctx, (FuncExpr*)expr, resultType);

        case T_OpExpr:
            return ProcessOpExpr(ctx, (OpExpr*)expr, resultType);

        case T_DistinctExpr:
            return ProcessDistinctExpr(ctx, (DistinctExpr*)expr, resultType);

        case T_NullIfExpr:
            return ProcessNullIfExpr(ctx, (NullIfExpr*)expr, resultType);

        case T_ScalarArrayOpExpr:
            return ProcessScalarArrayOpExpr(ctx, (ScalarArrayOpExpr*)expr, resultType);

        case T_BoolExpr:
            return ProcessBoolExpr(ctx, (BoolExpr*)expr, resultType);

        case T_FieldSelect:
            return ProcessFieldSelect(ctx, (FieldSelect*)expr, resultType);

        case T_FieldStore:
            return ProcessFieldStore(ctx, (FieldStore*)expr, resultType);

        case T_RelabelType:
            return ProcessRelabelType(ctx, (RelabelType*)expr, resultType);

        case T_CoerceViaIO:
            return ProcessCoerceViaIO(ctx, (CoerceViaIO*)expr, resultType);

        case T_ArrayCoerceExpr:
            return ProcessArrayCoerceExpr(ctx, (ArrayCoerceExpr*)expr, resultType);

        case T_ConvertRowtypeExpr:
            return ProcessConvertRowtypeExpr(ctx, (ConvertRowtypeExpr*)expr, resultType);

        case T_CaseExpr:
            return ProcessCaseExpr(ctx, (CaseExpr*)expr, resultType);

        case T_CaseWhen:
            return ProcessCaseWhenExpr(ctx, (CaseWhen*)expr, resultType);

        case T_CaseTestExpr:
            return ProcessCaseTestExpr(ctx, (CaseTestExpr*)expr, resultType);

        case T_ArrayExpr:
            return ProcessArrayExpr(ctx, (ArrayExpr*)expr, resultType);

        case T_RowExpr:
            return ProcessRowExpr(ctx, (RowExpr*)expr, resultType);

        case T_RowCompareExpr:
            return ProcessRowCompareExpr(ctx, (RowCompareExpr*)expr, resultType);

        case T_CoalesceExpr:
            return ProcessCoalesceExpr(ctx, (CoalesceExpr*)expr, resultType);

        case T_MinMaxExpr:
            return ProcessMinMaxExpr(ctx, (MinMaxExpr*)expr, resultType);

        case T_XmlExpr:
            return ProcessXmlExpr(ctx, (MinMaxExpr*)expr, resultType);

        case T_NullTest:
            return ProcessNullTest(ctx, (NullTest*)expr, resultType);

        case T_HashFilter:
            return ProcessHashFilter(ctx, (HashFilter*)expr, resultType);

        case T_BooleanTest:
            return ProcessBooleanTest(ctx, (BooleanTest*)expr, resultType);

        case T_CoerceToDomain:
            return ProcessCoerceToDomain(ctx, (CoerceToDomain*)expr, resultType);

        case T_CoerceToDomainValue:
            return ProcessCoerceToDomainValue(ctx, (CoerceToDomainValue*)expr, resultType);

        case T_List:
            MOT_LOG_TRACE("Unexpected list expression");
            break;

        default:
            MOT_LOG_TRACE("Unexpected expression type %d", (int)expr->type);
            break;
    }

    return nullptr;
}

static llvm::Value* ProcessSimpleExpr(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_expr* expr, Oid* resultType)
{
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
        char* parsed_query = nodeToString(expr->expr_simple_expr);
        MOT_LOG_TRACE("Processing simple expression:\n%s", parsed_query);
        pfree(parsed_query);
    }
    return ProcessExpr(ctx, expr->expr_simple_expr, resultType);
}

static llvm::Value* ProcessComplexExpr(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_expr* expr, Oid* resultType)
{
    MOT_LOG_TRACE("Unsupported complex expression");
    return nullptr;
}

extern llvm::Value* ProcessExpr(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_expr* expr, Oid* resultType)
{
    llvm::Value* result = nullptr;

    // code adapted from pl_exec.cpp

    // work on copy
    PLpgSQL_expr exprCopy = *expr;
    ExprQueryAttrs attrs;
    if (!GetExprQueryAttrs(&exprCopy, ctx->_compiled_function, &attrs)) {
        MOT_LOG_TRACE("Failed to process expression: failed to get query attributes");
        return nullptr;
    }
    exprCopy.plan = attrs.m_spiPlan;
    if (!CheckExprSimplePlan(&exprCopy, &attrs)) {
        MOT_LOG_TRACE("Failed to process expression: expression is not simple");
        CleanupExprQueryAttrs(&attrs);
        return nullptr;
    }

    if (exprCopy.expr_simple_expr) {
        result = ProcessSimpleExpr(ctx, &exprCopy, resultType);
    } else {
        result = ProcessComplexExpr(ctx, &exprCopy, resultType);
    }
    CleanupExprQueryAttrs(&attrs);

    return result;
}

static bool ProcessStatementAssign(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_assign* stmt)
{
    MOT_LOG_TRACE("Processing statement: assign");
    return AssignValue(ctx, stmt->varno, stmt->expr);
}

static bool ProcessStatementIf(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_if* stmt)
{
    MOT_LOG_TRACE("Processing statement: if");
    Oid resultType = 0;
    llvm::Value* cond = ProcessExpr(ctx, stmt->cond, &resultType);
    if (cond == nullptr) {
        MOT_LOG_TRACE("Failed to process if statement: Failed to process condition expression");
        return false;
    }

    bool result = true;

    JIT_IF_BEGIN(if_stmt);
    JIT_IF_EVAL(cond);
    {
        if (!ProcessStatementList(ctx, stmt->then_body)) {
            MOT_LOG_TRACE("Failed to process if statement: Failed to process then-body statement list");
            result = false;
        }
    }
    JIT_ELSE();
    {
        std::list<llvm_util::JitIf*> ifStack;
        ListCell* lc = nullptr;
        foreach (lc, stmt->elsif_list) {
            PLpgSQL_if_elsif* elsifBlock = (PLpgSQL_if_elsif*)lfirst(lc);
            cond = ProcessExpr(ctx, elsifBlock->cond, &resultType);
            if (cond == nullptr) {
                MOT_LOG_TRACE("Failed to process if statement: Failed to process elsif condition expression");
                result = false;
                break;
            }
            llvm_util::JitIf* jitIf = new (std::nothrow) llvm_util::JitIf(ctx, "elsif_stmt");
            if (jitIf == nullptr) {
                MOT_LOG_TRACE("Failed to create JIT IF statement");
                result = false;
                break;
            }
            jitIf->JitIfEval(cond);
            if (!ProcessStatementList(ctx, elsifBlock->stmts)) {
                MOT_LOG_TRACE("Failed to process if statement: Failed to process elsif-body statement list");
                result = false;
                break;
            }
            jitIf->JitElse();
            ifStack.push_back(jitIf);
        }
        if (result && stmt->else_body) {
            if (!ProcessStatementList(ctx, stmt->else_body)) {
                MOT_LOG_TRACE("Failed to process if statement: Failed to process else-body statement list");
                result = false;
            }
        }
        while (!ifStack.empty()) {
            llvm_util::JitIf* jitIf = ifStack.back();
            if (result) {
                jitIf->JitEnd();
            }
            delete jitIf;
            ifStack.pop_back();
        }
    }
    JIT_IF_END();

    return result;
}

static bool ProcessStatementGoto(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_goto* stmt)
{
    bool result = false;

    MOT_LOG_TRACE("Processing statement: goto");
    GotoLabelMap::iterator itr = ctx->m_gotoLabels.find(stmt->label);
    if (itr == ctx->m_gotoLabels.end()) {
        MOT_LOG_TRACE("Cannot jump to label %s: block label not found", stmt->label);
    } else {
        llvm::BasicBlock* block = itr->second;
        ctx->m_builder->CreateBr(block);
        result = true;
    }

    return result;
}

static bool ProcessStatementCase(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_case* stmt)
{
    // since case labels are translated into SQL queries (sometimes simplified into expressions)
    // we actually need here an if-else JIT statement

    MOT_LOG_TRACE("Processing statement: switch-case");
    JIT_DEBUG_LOG("Processing statement: CASE-WHEN");
    llvm::Value* assignee = GetLocalOrParamByVarNo(ctx, stmt->t_varno);
    if (!assignee) {
        MOT_LOG_TRACE(
            "Failed to process switch-case statement: Failed to find local assignee by varno %d", stmt->t_varno);
        return false;
    }

    llvm::Value* assigneeIsNull = GetIsNullLocalOrParamByVarNo(ctx, stmt->t_varno);
    if (!assigneeIsNull) {
        MOT_LOG_TRACE(
            "Failed to process switch-case statement: Failed to find local assignee nulls by varno %d", stmt->t_varno);
        return false;
    }
    Oid resultType = 0;
    llvm::Value* switchValue = nullptr;
    if (stmt->t_expr) {  // non-searched CASE-WHEN syntax
        switchValue = ProcessExpr(ctx, stmt->t_expr, &resultType);
        if (switchValue == nullptr) {
            MOT_LOG_TRACE("Failed to process switch-case statement: Failed to process switch value expression");
            return false;
        }
        if (resultType == INT4OID) {
            JIT_DEBUG_LOG_INT_VAR("CASE-WHEN switch var value: %d", switchValue);
        }

        llvm::Value* switchValueIsNull = AddGetExprIsNull(ctx);
        llvm::Value* isNullBool = ctx->m_builder->CreateCast(llvm::Instruction::Trunc, switchValueIsNull, ctx->BOOL_T);
        llvm::StoreInst* storeInstNull = ctx->m_builder->CreateStore(isNullBool, assigneeIsNull, true);
        if (!storeInstNull) {
            MOT_LOG_TRACE("Failed to store switch is null value");
            return false;
        }
        // the switch value must be stored in the specified variable, since subsequent case expressions
        // are actually if statements which refer to this stored value
        llvm::StoreInst* storeInst = ctx->m_builder->CreateStore(switchValue, assignee, true);
        if (!storeInst) {
            MOT_LOG_TRACE("Failed to store switch value");
            return false;
        }
    } else {
        JIT_DEBUG_LOG("CASE-WHEN searched syntax");
    }

    bool result = true;

    // begin switch-case block
    std::list<llvm_util::JitIf*> ifStack;
    ListCell* lc = nullptr;
    foreach (lc, stmt->case_when_list) {
        PLpgSQL_case_when* caseWhenBlock = (PLpgSQL_case_when*)lfirst(lc);
        llvm::Value* caseCond = ProcessExpr(ctx, caseWhenBlock->expr, &resultType);
        if (caseCond == nullptr) {
            MOT_LOG_TRACE("Failed to process switch-case statement: Failed to process elsif condition expression");
            result = false;
            break;
        }

        if (resultType == INT4OID) {
            JIT_DEBUG_LOG_INT_VAR("CASE-WHEN condition value: %d", caseCond);
        }

        llvm_util::JitIf* jitIf = new (std::nothrow) llvm_util::JitIf(ctx, "stmt_case");
        if (jitIf == nullptr) {
            MOT_LOG_TRACE("Failed to create JIT IF statement");
            result = false;
            break;
        }
        ifStack.push_back(jitIf);
        jitIf->JitIfEval(caseCond);

        JIT_DEBUG_LOG("CASE-WHEN condition passed");

        if (!ProcessStatementList(ctx, caseWhenBlock->stmts)) {
            MOT_LOG_TRACE("Failed to process switch-case statement: Failed to process case-when-body statement list");
            result = false;
            break;
        }
        jitIf->JitElse();
        JIT_DEBUG_LOG("CASE-WHEN condition rejected");
    }
    if (stmt->have_else && (stmt->else_stmts != nullptr)) {
        JIT_DEBUG_LOG("CASE-WHEN else body");
        if (!ProcessStatementList(ctx, stmt->else_stmts)) {
            MOT_LOG_TRACE("Failed to process switch-case statement: Failed to process else-body statement list");
            result = false;
        }
    }
    while (!ifStack.empty()) {
        llvm_util::JitIf* jitIf = ifStack.back();
        if (result) {
            jitIf->JitEnd();
        }
        delete jitIf;
        ifStack.pop_back();
    }
    JIT_DEBUG_LOG("CASE-WHEN done");

    return result;
}

static bool ProcessStatementLoop(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_loop* stmt)
{
    MOT_LOG_TRACE("Processing statement: loop");
    bool result = ProcessStatementLabel(ctx, stmt->label, "unnamed_loop");
    if (result) {
        // unconditional loop
        JIT_DEBUG_LOG("LOOP begin");
        JIT_WHILE_BEGIN(stmt_loop)
        LabelDesc labelDesc = {stmt->label, JIT_WHILE_COND_BLOCK(), JIT_WHILE_POST_BLOCK(), true};
        ctx->m_labelStack.push_back(labelDesc);
        JIT_WHILE_EVAL(JIT_CONST_INT64(1));
        {
            if (!ProcessStatementList(ctx, stmt->body)) {
                MOT_LOG_TRACE("Failed to process loop statement: Failed to process loop-body statement list");
                result = false;
            }
        }
        JIT_WHILE_END()
        ctx->m_labelStack.pop_back();
        JIT_DEBUG_LOG("LOOP end");
    }

    return result;
}

static bool ProcessStatementWhile(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_while* stmt)
{
    MOT_LOG_TRACE("Processing statement: while");
    bool result = ProcessStatementLabel(ctx, stmt->label, "unnamed_while");
    if (result) {
        // conditional loop
        JIT_WHILE_BEGIN(stmt_while)
        LabelDesc labelDesc = {stmt->label, JIT_WHILE_COND_BLOCK(), JIT_WHILE_POST_BLOCK(), true};
        ctx->m_labelStack.push_back(labelDesc);
        // attention: the condition expression evaluation code must be emitted after JIT_WHILE_BEGIN()
        Oid resultType = 0;
        llvm::Value* condValue = ProcessExpr(ctx, stmt->cond, &resultType);
        if (condValue == nullptr) {
            MOT_LOG_TRACE("Failed to process while statement: Failed to process condition value expression");
            result = false;
        }

        if (result) {
            JIT_WHILE_EVAL(condValue);
            {
                if (!ProcessStatementList(ctx, stmt->body)) {
                    MOT_LOG_TRACE("Failed to process while statement: Failed to process while-body statement list");
                    result = false;
                }
            }
        }
        JIT_WHILE_END()
        ctx->m_labelStack.pop_back();
    }

    return result;
}

static bool ProcessStatementForI(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_fori* stmt)
{
    MOT_LOG_TRACE("Processing statement: fori");
    bool result = ProcessStatementLabel(ctx, stmt->label, "unnamed_fori");
    if (result) {
        // evaluate lower bound
        Oid resultType = 0;
        llvm::Value* lowerBound = ProcessExpr(ctx, stmt->lower, &resultType);
        if (lowerBound == nullptr) {
            MOT_LOG_TRACE("Failed to process for-integer statement: Failed to process lower bound expression");
            return false;
        }
        llvm::Value* lowerBoundIsNull = AddGetExprIsNull(ctx);

        // convert type if needed
        if (resultType != stmt->var->datatype->typoid) {
            // we need local variable in order to reuse AssignScalarValue
            llvm::Value* lowerBoundValueLocal = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "lower_bound");
            llvm::Value* lowerBoundIsNullLocal =
                ctx->m_builder->CreateAlloca(ctx->BOOL_T, 0, nullptr, "lower_bound_is_null");
            if (!AssignScalarValue(ctx,
                    stmt->var,
                    lowerBoundValueLocal,
                    lowerBoundIsNullLocal,
                    lowerBound,
                    lowerBoundIsNull,
                    resultType)) {
                MOT_LOG_TRACE("ProcessStatementForI(): For to convert loop value")
                return false;
            }
            lowerBound = ctx->m_builder->CreateLoad(lowerBoundValueLocal, true);
            lowerBoundIsNull = ctx->m_builder->CreateLoad(lowerBoundIsNullLocal, true);
        }

        // check in run time whether lower bound is null
        JIT_IF_BEGIN(lower_bound_is_null);
        JIT_IF_EVAL(lowerBoundIsNull);
        {
            AddSetSqlStateAndErrorMessage(
                ctx, "lower bound of FOR loop cannot be null", ERRCODE_NULL_VALUE_NOT_ALLOWED);
            JIT_DEBUG_LOG("JIT SP exception: lower bound of FOR loop cannot be null");
            JIT_THROW(JIT_CONST_UINT32(ERRCODE_NULL_VALUE_NOT_ALLOWED));
        }
        JIT_IF_END();
        llvm::Value* loopValue = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "loop_value");
        ctx->m_builder->CreateStore(lowerBound, loopValue, true);
        JIT_DEBUG_LOG_DATUM("Lower bound is", lowerBound, JIT_CONST_INT32(0), INT4OID);

        // evaluate upper bound
        llvm::Value* upperBound = ProcessExpr(ctx, stmt->upper, &resultType);
        if (upperBound == nullptr) {
            MOT_LOG_TRACE("Failed to process for-integer statement: Failed to process upper bound expression");
            return false;
        }
        llvm::Value* upperBoundIsNull = AddGetExprIsNull(ctx);

        // convert type if needed
        if (resultType != stmt->var->datatype->typoid) {
            // we need local variable in order to reuse AssignScalarValue
            llvm::Value* upperBoundLocal = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "upper_bound");
            llvm::Value* upperBoundIsNullLocal =
                ctx->m_builder->CreateAlloca(ctx->BOOL_T, 0, nullptr, "upper_bound_is_null");
            if (!AssignScalarValue(
                    ctx, stmt->var, upperBoundLocal, upperBoundIsNullLocal, upperBound, upperBoundIsNull, resultType)) {
                MOT_LOG_TRACE("ProcessStatementForI(): For to convert bound value")
                return false;
            }
            upperBound = ctx->m_builder->CreateLoad(upperBoundLocal, true);
            upperBoundIsNull = ctx->m_builder->CreateLoad(upperBoundIsNullLocal, true);
        }

        // check in run time whether upper bound is null
        JIT_IF_BEGIN(upper_bound_is_null);
        JIT_IF_EVAL(upperBoundIsNull);
        {
            AddSetSqlStateAndErrorMessage(
                ctx, "upper bound of FOR loop cannot be null", ERRCODE_NULL_VALUE_NOT_ALLOWED);
            JIT_DEBUG_LOG("JIT SP exception: upper bound of FOR loop cannot be null");
            JIT_THROW(JIT_CONST_UINT32(ERRCODE_NULL_VALUE_NOT_ALLOWED));
        }
        JIT_IF_END();
        JIT_DEBUG_LOG_DATUM("Upper bound is", upperBound, JIT_CONST_INT32(0), INT4OID);

        // evaluate step
        llvm::Value* step = nullptr;
        if (stmt->step != nullptr) {
            step = ProcessExpr(ctx, stmt->step, &resultType);
            if (step == nullptr) {
                MOT_LOG_TRACE("Failed to process for-integer statement: Failed to process step expression");
                return false;
            }
            llvm::Value* stepIsNull = AddGetExprIsNull(ctx);

            // convert type if needed
            if (resultType != stmt->var->datatype->typoid) {
                // we need local variable in order to reuse AssignScalarValue
                llvm::Value* stepLocal = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "step");
                llvm::Value* stepIsNullLocal = ctx->m_builder->CreateAlloca(ctx->BOOL_T, 0, nullptr, "step_is_null");
                if (!AssignScalarValue(ctx, stmt->var, stepLocal, stepIsNullLocal, step, stepIsNull, resultType)) {
                    MOT_LOG_TRACE("ProcessStatementForI(): For to convert bound value")
                    return false;
                }
                step = ctx->m_builder->CreateLoad(stepLocal, true);
                stepIsNull = ctx->m_builder->CreateLoad(stepIsNullLocal, true);
            }

            // check in run time whether upper bound is null
            JIT_IF_BEGIN(step_is_null);
            JIT_IF_EVAL(stepIsNull);
            {
                AddSetSqlStateAndErrorMessage(
                    ctx, "BY value of FOR loop cannot be null", ERRCODE_NULL_VALUE_NOT_ALLOWED);
                JIT_DEBUG_LOG("JIT SP exception: BY value of FOR loop cannot be null");
                JIT_THROW(JIT_CONST_UINT32(ERRCODE_NULL_VALUE_NOT_ALLOWED));
            }
            JIT_IF_END();
            JIT_IF_BEGIN(step_is_negative);
            JIT_IF_EVAL_CMP(step, JIT_CONST_INT64(0), JitICmpOp::JIT_ICMP_LE);
            {
                AddSetSqlStateAndErrorMessage(
                    ctx, "BY value of FOR loop must be greater than zero", ERRCODE_INVALID_PARAMETER_VALUE);
                JIT_DEBUG_LOG("JIT SP exception: BY value of FOR loop must be greater than zero");
                JIT_THROW(JIT_CONST_UINT32(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            JIT_IF_END();
        } else {
            // if none is specified then the step is 1, just make sure it has the same type as the loop counter/bounds
            step = llvm::ConstantInt::get(lowerBound->getType(), 1, true);
        }
        if (step == nullptr) {
            MOT_LOG_TRACE("Failed to process for-integer statement: Failed to process step expression");
            return false;
        }
        JIT_DEBUG_LOG_DATUM("Step is", step, JIT_CONST_INT32(0), INT4OID);

        // define local found variable and initialize to false
        llvm::Value* foundLocal = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "found");
        llvm::Value* falseDatum = llvm::ConstantInt::get(ctx->DATUM_T, BoolGetDatum(false), false);
        ctx->m_builder->CreateStore(falseDatum, foundLocal, true);

        // execute loop
        JIT_WHILE_BEGIN(stmt_fori);
        JIT_WHILE_EVAL(JIT_CONST_INT32(1));
        {
            // check for loop end condition
            llvm::Value* currLoopValue = ctx->m_builder->CreateLoad(loopValue, true);
            JIT_DEBUG_LOG_DATUM("Current loop value is", currLoopValue, JIT_CONST_INT32(0), INT4OID);
            JIT_IF_BEGIN(end_loop);
            JIT_IF_EVAL_CMP(currLoopValue, upperBound, stmt->reverse ? JitICmpOp::JIT_ICMP_LT : JitICmpOp::JIT_ICMP_GT);
            {
                JIT_DEBUG_LOG("Reached loop bound, breaking from loop");
                JIT_WHILE_BREAK();
            }
            JIT_IF_END();

            // remember loop executed at least once
            JIT_DEBUG_LOG("Storing true in FOUND");
            llvm::Value* trueDatum = llvm::ConstantInt::get(ctx->DATUM_T, BoolGetDatum(true), false);
            ctx->m_builder->CreateStore(trueDatum, foundLocal, true);

            // assign loop value (so it can be accessed by other statements in the loop)
            JIT_DEBUG_LOG("Storing current loop value in loop variable");
            llvm::Value* loopLocalValue = GetLocalOrParamByVarNo(ctx, stmt->var->dno);
            llvm::Value* loopIsNullLocal = GetIsNullLocalOrParamByVarNo(ctx, stmt->var->dno);
            ctx->m_builder->CreateStore(currLoopValue, loopLocalValue, true);
            ctx->m_builder->CreateStore(JIT_CONST_BOOL(false), loopIsNullLocal, true);

            // execute loop statements
            JIT_DEBUG_LOG("Processing loop statements");
            if (!ProcessStatementList(ctx, stmt->body)) {
                MOT_LOG_TRACE("Failed to process for-integer statement: Failed to process for-body statement list");
                result = false;
            }

            if (ctx->m_builder->GetInsertBlock()->getTerminator() == nullptr) {
                JIT_DEBUG_LOG("Incrementing loop counter");
                if (stmt->reverse) {
                    llvm::Value* nextLoopValue = ctx->m_builder->CreateSub(currLoopValue, step);
                    JIT_IF_BEGIN(loop_bound_overflow);
                    JIT_IF_EVAL_CMP(nextLoopValue, currLoopValue, JitICmpOp::JIT_ICMP_GT);
                    {
                        JIT_DEBUG_LOG("Loop value overflow");
                        JIT_WHILE_BREAK();
                    }
                    JIT_ELSE();
                    {
                        JIT_DEBUG_LOG_DATUM(
                            "Storing in loop value next value", nextLoopValue, JIT_CONST_INT32(0), INT4OID);
                        ctx->m_builder->CreateStore(nextLoopValue, loopValue, true);
                    }
                    JIT_IF_END();
                } else {
                    llvm::Value* nextLoopValue = ctx->m_builder->CreateAdd(currLoopValue, step);
                    JIT_IF_BEGIN(loop_bound_overflow);
                    JIT_IF_EVAL_CMP(nextLoopValue, currLoopValue, JitICmpOp::JIT_ICMP_LT);
                    {
                        JIT_DEBUG_LOG("Loop value underflow");
                        JIT_WHILE_BREAK();
                    }
                    JIT_ELSE();
                    {
                        JIT_DEBUG_LOG_DATUM(
                            "Storing in loop value next value", nextLoopValue, JIT_CONST_INT32(0), INT4OID);
                        ctx->m_builder->CreateStore(nextLoopValue, loopValue, true);
                    }
                    JIT_IF_END();
                }
            } else {
                MOT_LOG_TRACE(
                    "Skipping code generation for incrementing loop counter: current block ends with terminator");
            }
        }
        JIT_WHILE_END();

        // set found variable
        llvm::Value* finalFoundValue = ctx->m_builder->CreateLoad(foundLocal, true);
        llvm::Value* foundValue = GetLocalOrParamByVarNo(ctx, ctx->_compiled_function->found_varno);
        ctx->m_builder->CreateStore(finalFoundValue, foundValue, true);
    }

    return result;
}

static bool ProcessStatementForS(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_fors* stmt)
{
    MOT_LOG_TRACE("Processing statement: fors");
    return false;
}

static bool ProcessStatementForC(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_forc* stmt)
{
    // Not supported
    MOT_LOG_TRACE("Processing statement: forc");
    return false;
}

static bool ProcessStatementForEach(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_foreach_a* stmt)
{
    // Not supported
    MOT_LOG_TRACE("Processing statement: foreach");
    return false;
}

static bool ProcessStatementExitGoto(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_exit* stmt)
{
    if (!stmt->label) {
        if (JIT_LOOP_CURRENT() == nullptr) {
            MOT_LOG_WARN("CONTINUE/BREAK cannot be used outside of a loop");
            return false;
        }
        if (stmt->is_exit) {
            JIT_LOOP_BREAK();  // break innermost loop
        } else {
            JIT_LOOP_CONTINUE();  // continue to next iteration on innermost loop
        }
        return true;
    } else {
        if (stmt->is_exit) {
            // branch to labeled end-of outer loop or block
            // search in the label stack the named loop or block
            LabelStack::reverse_iterator itr = ctx->m_labelStack.rbegin();
            while (itr != ctx->m_labelStack.rend()) {
                LabelDesc& labelDesc = *itr;
                if (strcmp(labelDesc.m_name, stmt->label) == 0) {
                    JIT_GOTO(labelDesc.m_postBlock);
                    return true;
                }
                ++itr;
            }
        } else {
            // loop represents any outer loop name
            LabelStack::reverse_iterator itr = ctx->m_labelStack.rbegin();
            while (itr != ctx->m_labelStack.rend()) {
                LabelDesc& labelDesc = *itr;
                if (labelDesc.m_isLoop && labelDesc.m_name != nullptr && stmt->label != nullptr &&
                    (strcmp(labelDesc.m_name, stmt->label) == 0)) {
                    JIT_GOTO(labelDesc.m_postBlock);
                    return true;
                }
                ++itr;
            }
        }
        MOT_LOG_TRACE("Failed to find label %s in EXIT/CONTINUE statment", stmt->label);
        return false;
    }
}

static bool ProcessStatementExit(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_exit* stmt)
{
    MOT_LOG_TRACE("Processing statement: exit/continue");
    Oid resultType = 0;
    if (stmt->is_exit) {
        JIT_DEBUG_LOG(stmt->cond ? "EXIT conditional" : "EXIT unconditional");
    } else {
        JIT_DEBUG_LOG(stmt->cond ? "CONTINUE conditional" : "CONTINUE unconditional");
    }

    if (!stmt->cond) {
        return ProcessStatementExitGoto(ctx, stmt);
    }

    bool result = true;
    JIT_IF_BEGIN(stmt_exit);
    llvm::Value* condValue = ProcessExpr(ctx, stmt->cond, &resultType);
    if (condValue == nullptr) {
        MOT_LOG_TRACE("Failed to process exit/continue statement: Failed to process lower bound expression");
        result = false;
    }
    if (result) {
        JIT_IF_EVAL(condValue);
        {
            result = ProcessStatementExitGoto(ctx, stmt);
        }
    }
    JIT_IF_END();
    return result;
}

static bool ProcessRetVarNo(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_return* stmt)
{
    // if the variable is scalar then just put in slot 0
    bool result = false;
    int varNo = stmt->retvarno;
    PLpgSQL_datum* datum = ctx->_compiled_function->datums[varNo];
    if (datum->dtype == PLPGSQL_DTYPE_VAR) {
        llvm::Value* value = GetLocalOrParamByVarNo(ctx, varNo);
        llvm::Value* loadedValue = ctx->m_builder->CreateLoad(value, true);
        value = GetIsNullLocalOrParamByVarNo(ctx, varNo);
        llvm::Value* valueIsNull = ctx->m_builder->CreateLoad(value, true);
        llvm::Value* valueIsNullInt = ctx->m_builder->CreateCast(llvm::Instruction::SExt, valueIsNull, ctx->INT32_T);
#ifdef MOT_JIT_DEBUG
        PLpgSQL_var* varDatum = (PLpgSQL_var*)datum;
        JIT_DEBUG_LOG_DATUM("Setting slot value", loadedValue, valueIsNull, varDatum->datatype->typoid);
#endif
        AddSetSlotValue(ctx, 0, loadedValue, valueIsNullInt);
        result = true;
    } else if (datum->dtype == PLPGSQL_DTYPE_ROW) {
        PLpgSQL_row* row = (PLpgSQL_row*)datum;
        if (ctx->m_resultTupDesc == nullptr) {
            MemoryContext oldCtx = CurrentMemoryContext;
            CurrentMemoryContext = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR);
            result = GetFuncTypeClass(ctx->m_plan, &ctx->m_resultTupDesc, &ctx->m_typeFuncClass);
            if (result) {
                ctx->m_rowTupDesc = PrepareTupleDescFromRow(row, ctx->_compiled_function);
            }
            CurrentMemoryContext = oldCtx;
            if (!result) {
                return false;
            }
            // the result tuple desc is moved/cloned into the JitSource/Context
        }
        JIT_IF_BEGIN(composite_result);
        llvm::Value* isCompositeResult = AddIsCompositeResult(ctx);
        JIT_IF_EVAL(isCompositeResult);
        {
            llvm::Value* resultDatums = AddCreateResultDatums(ctx);
            llvm::Value* resultNulls = AddCreateResultNulls(ctx);
            int columnId = 0;
            for (int i = 0; i < row->nfields; ++i) {
                int fieldVarNo = row->varnos[i];
                if (fieldVarNo >= 0) {  // skip dropped columns
                    JIT_DEBUG_LOG_INT("Returning out parameter with varno: %d", fieldVarNo);
                    JIT_DEBUG_LOG_VARNO(fieldVarNo);
                    llvm::Value* value = GetLocalOrParamByVarNo(ctx, fieldVarNo);
                    llvm::Value* resultVar = ctx->m_builder->CreateLoad(value, true);
                    value = GetIsNullLocalOrParamByVarNo(ctx, fieldVarNo);
                    llvm::Value* resultIsNull = ctx->m_builder->CreateLoad(value, true);
                    llvm::Value* resultIsNullInt =
                        ctx->m_builder->CreateCast(llvm::Instruction::SExt, resultIsNull, ctx->INT32_T);
#ifdef MOT_JIT_DEBUG
                    PLpgSQL_datum* fieldDatum = ctx->_compiled_function->datums[fieldVarNo];
                    MOT_ASSERT(fieldDatum->dtype == PLPGSQL_DTYPE_VAR);
                    if (fieldDatum->dtype == PLPGSQL_DTYPE_VAR) {
                        PLpgSQL_var* varDatum = (PLpgSQL_var*)fieldDatum;
                        JIT_DEBUG_LOG_DATUM(
                            "Setting result value", resultVar, resultIsNull, varDatum->datatype->typoid);
                    } else {
                        JIT_DEBUG_LOG("Setting non-scalar result value");
                    }
#endif
                    AddSetResultValue(ctx, resultDatums, resultNulls, columnId++, resultVar, resultIsNullInt);
                }
            }
            llvm::Value* resultHeapTuple = AddCreateResultHeapTuple(ctx, resultDatums, resultNulls);
            AddSetSlotValue(ctx, 0, resultHeapTuple, JIT_CONST_INT32(0));
        }
        JIT_ELSE();
        {
            int columnId = 0;
            for (int i = 0; i < row->nfields; ++i) {
                int fieldVarNo = row->varnos[i];
                if (fieldVarNo >= 0) {  // skip dropped columns
                    JIT_DEBUG_LOG_INT("Returning out parameter with varno: %d", fieldVarNo);
                    JIT_DEBUG_LOG_VARNO(fieldVarNo);
                    llvm::Value* value = GetLocalOrParamByVarNo(ctx, fieldVarNo);
                    llvm::Value* resVar = ctx->m_builder->CreateLoad(value, true);
                    value = GetIsNullLocalOrParamByVarNo(ctx, fieldVarNo);
                    llvm::Value* resIsNull = ctx->m_builder->CreateLoad(value, true);
                    llvm::Value* resIsNullInt =
                        ctx->m_builder->CreateCast(llvm::Instruction::SExt, resIsNull, ctx->INT32_T);
#ifdef MOT_JIT_DEBUG
                    PLpgSQL_datum* fieldDatum = ctx->_compiled_function->datums[fieldVarNo];
                    MOT_ASSERT(fieldDatum->dtype == PLPGSQL_DTYPE_VAR);
                    if (fieldDatum->dtype == PLPGSQL_DTYPE_VAR) {
                        PLpgSQL_var* varDatum = (PLpgSQL_var*)fieldDatum;
                        JIT_DEBUG_LOG_DATUM("Setting slot value", resVar, resIsNull, varDatum->datatype->typoid);
                    } else {
                        JIT_DEBUG_LOG("Setting non-scalar slot value");
                    }
#endif
                    AddSetSlotValue(ctx, columnId++, resVar, resIsNullInt);
                }
            }
        }
        JIT_IF_END();
        result = true;
    } else {
        MOT_LOG_TRACE("Unsupported return type %d", datum->dtype);
    }

    if (result) {
        // store the tuple and communicate back one tuple was processed.
        AddExecStoreVirtualTuple(ctx);
        AddSetTpProcessed(ctx, JIT_CONST_INT64(1));
        AddSetScanEnded(ctx, JIT_CONST_INT32(1));

        // commit all open sub-transactions, and we are done
        AddCleanupBeforeReturn(ctx, ctx->m_initSubXid);
        CheckThrownException(ctx);
        AddLlvmClearExceptionStack(ctx);
        InjectProfileData(ctx, MOT_JIT_PROFILE_REGION_TOTAL, false);
        JIT_RETURN(JIT_CONST_INT32(MOT::RC_OK));  // success-done
    }
    return result;
}

static bool ProcessStatementReturn(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_return* stmt)
{
    // we disregard the retvarno member, as we return values through the tuple slot
    MOT_LOG_TRACE("Processing statement: return");

    // sometimes we return a variable by no and expression is null
    if (stmt->retvarno >= 0) {
        MOT_LOG_TRACE("Process return var no %d", stmt->retvarno);
        return ProcessRetVarNo(ctx, stmt);
    }

    // we always expect a simple expression here
    MOT_LOG_TRACE("Process return expression");
    Oid resultType = 0;
    if (stmt->expr != nullptr) {  // could return void
        llvm::Value* resultValue = ProcessExpr(ctx, stmt->expr, &resultType);
        if (resultValue == nullptr) {
            MOT_LOG_TRACE("Failed to process return statement: Failed to process result value expression");
            return false;
        }

        // check if expression evaluated to null (this is a top level expression at position 0)
        llvm::Value* isNull = AddGetExprIsNull(ctx);

        // store the result value in the result tuple slot (always tuple column 0 for single return value)
        AddSetSlotValue(ctx, 0, resultValue, isNull);
        AddExecStoreVirtualTuple(ctx);
    }

    // communicate back one tuple was processed. commit all open sub-transactions, and we are done
    AddSetTpProcessed(ctx, JIT_CONST_INT64(1));
    AddSetScanEnded(ctx, JIT_CONST_INT32(1));
    AddCleanupBeforeReturn(ctx, ctx->m_initSubXid);
    CheckThrownException(ctx);
    AddLlvmClearExceptionStack(ctx);
    InjectProfileData(ctx, MOT_JIT_PROFILE_REGION_TOTAL, false);
    JIT_RETURN(JIT_CONST_INT32(MOT::RC_OK));  // success-done

    return true;
}

static bool ProcessStatementReturnNext(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_return_next* stmt)
{
    // Not supported
    MOT_LOG_TRACE("Processing statement: return next");
    return false;
}

static bool ProcessStatementReturnQuery(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_return_query* stmt)
{
    // Not supported
    MOT_LOG_TRACE("Processing statement: return query");
    return false;
}

static bool IsSimpleExprListStatic(JitLlvmFunctionCodeGenContext* ctx, List* exprList)
{
    ListCell* lc = nullptr;
    foreach (lc, exprList) {
        Expr* expr = (Expr*)lfirst(lc);
        if (!IsSimpleExprStatic(ctx, expr)) {
            return false;
        }
    }
    return true;
}

static bool IsSimpleExprStatic(JitLlvmFunctionCodeGenContext* ctx, Expr* expr)
{
    // currently only direct constants are considered as static (i.e. can be evaluated during compile-time)
    // and functions over constants
    switch (expr->type) {
        case T_Const:
            return true;

        case T_RelabelType:
            return IsSimpleExprStatic(ctx, ((RelabelType*)expr)->arg);

        case T_FuncExpr:
            return IsSimpleExprListStatic(ctx, ((FuncExpr*)expr)->args);

        case T_OpExpr:
            return IsSimpleExprListStatic(ctx, ((OpExpr*)expr)->args);

        case T_BoolExpr:
            return IsSimpleExprListStatic(ctx, ((BoolExpr*)expr)->args);

        default:
            return false;
    }
}

static bool ProcessStatementRaise(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_raise* stmt)
{
    MOT_LOG_TRACE("Processing statement: raise");
    if (MOT::GetGlobalConfiguration().m_enableCodegenProfile) {
        if ((stmt->elog_level == DEBUG1) && ((strcmp(stmt->message, MOT_JIT_PROFILE_BEGIN_MESSAGE) == 0) ||
                                                (strcmp(stmt->message, MOT_JIT_PROFILE_END_MESSAGE) == 0))) {
            if (stmt->options != nullptr) {
                PLpgSQL_raise_option* option = (PLpgSQL_raise_option*)linitial(stmt->options);
                if ((option->opt_type == PLPGSQL_RAISEOPTION_HINT) && (option->expr->query != nullptr)) {
                    int beginRegion = (strcmp(stmt->message, MOT_JIT_PROFILE_BEGIN_MESSAGE) == 0) ? 1 : 0;
                    InjectProfileData(ctx, option->expr->query, beginRegion);
                    return true;
                }
            }
        }
    }
    // no other implementation yet
    return false;

    // special case: empty RAISE instruction emits a re-throw
    if (stmt->condname == nullptr && stmt->message == nullptr && stmt->options == NIL) {
        // make sure we are safe inside a try-catch block
        if (!JIT_TRY_CURRENT()) {
            MOT_LOG_TRACE("Cannot use re-throw RAISE not inside exception block");
            return false;
        }
        JIT_RETHROW();
        return true;
    }
}

inline void HandleTooManyRows(JitLlvmFunctionCodeGenContext* ctx, int tcount)
{
    MOT::mot_string errMsg;
    if (errMsg.format("query returned %d rows more than one row", tcount)) {
        AddSetSqlStateAndErrorMessage(ctx, errMsg.c_str(), ERRCODE_TOO_MANY_ROWS);
    } else {
        MOT_LOG_TRACE("Failed to format error message for handling too many rows on SP query return value processing");
        AddSetSqlStateAndErrorMessage(ctx, "query returned more than one row", ERRCODE_TOO_MANY_ROWS);
    }
    JIT_DEBUG_LOG("JIT SP exception: Too many rows found for INTO clause with STRICT execution");
    JIT_THROW(JIT_CONST_UINT32(ERRCODE_TOO_MANY_ROWS));
}

static void ProcessSubQueryResult(JitLlvmFunctionCodeGenContext* ctx, int subQueryId, llvm::Value* queryResult,
    llvm::Value* tuplesProcessed, bool strict, bool mod, int tcount, bool into, bool isPerform)
{
    // prepare FOUND magic variable
    if (ctx->m_usingMagicVars) {
        llvm::Value* foundValue = GetLocalOrParamByVarNo(ctx, ctx->_compiled_function->found_varno);
        llvm::Value* foundIsNull = GetIsNullLocalOrParamByVarNo(ctx, ctx->_compiled_function->found_varno);
        llvm::Value* sqlNotFoundValue = GetLocalOrParamByVarNo(ctx, ctx->_compiled_function->sql_notfound_varno);
        llvm::Value* sqlNotFoundIsNull = GetIsNullLocalOrParamByVarNo(ctx, ctx->_compiled_function->sql_notfound_varno);
        llvm::Value* sqlRowCountValue = GetLocalOrParamByVarNo(ctx, ctx->_compiled_function->sql_rowcount_varno);
        llvm::Value* sqlRowCountIsNull = GetIsNullLocalOrParamByVarNo(ctx, ctx->_compiled_function->sql_rowcount_varno);
        llvm::Value* trueDatum = llvm::ConstantInt::get(ctx->INT64_T, BoolGetDatum(true), false);
        llvm::Value* falseDatum = llvm::ConstantInt::get(ctx->INT64_T, BoolGetDatum(false), false);
        llvm::Value* falseBool = JIT_CONST_BOOL(false);

        // update magic variables according to sub-query result (make sure not null)
        ctx->m_builder->CreateStore(falseBool, foundIsNull, true);
        ctx->m_builder->CreateStore(falseBool, sqlNotFoundIsNull, true);
        ctx->m_builder->CreateStore(falseBool, sqlRowCountIsNull, true);

        JIT_DEBUG_LOG("Setting magic variables");
        llvm::Value* spiResult = AddGetSpiResult(ctx, subQueryId);
        JIT_SWITCH_BEGIN(spi_result, spiResult);
        {
            JIT_CASE_MANY(JIT_CONST_INT32(SPI_OK_SELECT));
            JIT_CASE_MANY(JIT_CONST_INT32(SPI_OK_INSERT));
            JIT_CASE_MANY(JIT_CONST_INT32(SPI_OK_UPDATE));
            JIT_CASE_MANY(JIT_CONST_INT32(SPI_OK_DELETE));
            JIT_CASE_MANY(JIT_CONST_INT32(SPI_OK_INSERT_RETURNING));
            JIT_CASE_MANY(JIT_CONST_INT32(SPI_OK_UPDATE_RETURNING));
            JIT_CASE_MANY(JIT_CONST_INT32(SPI_OK_DELETE_RETURNING));
            JIT_CASE_MANY(JIT_CONST_INT32(SPI_OK_MERGE));
            JIT_CASE_MANY_TERM();
            {
                JIT_IF_BEGIN(set_magic_vars);
                JIT_IF_EVAL(tuplesProcessed);
                {
                    JIT_DEBUG_LOG("Setting FOUND magic variables to TRUE");
                    ctx->m_builder->CreateStore(trueDatum, foundValue, true);
                    ctx->m_builder->CreateStore(falseDatum, sqlNotFoundValue, true);
                }
                JIT_ELSE();
                {
                    JIT_DEBUG_LOG("Setting FOUND magic variables to FALSE");
                    ctx->m_builder->CreateStore(falseDatum, foundValue, true);
                    ctx->m_builder->CreateStore(trueDatum, sqlNotFoundValue, true);
                }
                JIT_IF_END();

                // set the row count
                llvm::Value* tuplesProcessed64 =
                    ctx->m_builder->CreateCast(llvm::Instruction::SExt, tuplesProcessed, ctx->INT64_T);
                ctx->m_builder->CreateStore(tuplesProcessed64, sqlRowCountValue, true);
                JIT_CASE_BREAK();
            }
            JIT_CASE_END();

            JIT_CASE(JIT_CONST_INT32(SPI_OK_REWRITTEN));
            {
                JIT_DEBUG_LOG("Setting only FOUND magic variable to FALSE when return value is SPI_OK_REWRITTEN");
                ctx->m_builder->CreateStore(falseDatum, foundValue, true);
                JIT_CASE_BREAK();
            }
            JIT_CASE_END();

            JIT_CASE_DEFAULT();
            {
                // this is unexpected (should be handled in helpers), but we deal with it anyway
                AddSetSqlStateAndErrorMessage(
                    ctx, "SPI_execute_plan_with_paramlist failed executing query", ERRCODE_FEATURE_NOT_SUPPORTED);
                JIT_THROW(JIT_CONST_UINT32(ERRCODE_FEATURE_NOT_SUPPORTED));
                JIT_CASE_BREAK();
            }
            JIT_CASE_END();
        }
        JIT_SWITCH_END();
    }

    // following checks are NOT generated for PERFORM statements
    if (!isPerform) {
        if (into) {
            if (strict) {
                JIT_IF_BEGIN(strict_res_empty_check);
                JIT_IF_EVAL_NOT(tuplesProcessed);
                {
                    AddSetSqlStateAndErrorMessage(
                        ctx, "query returned no rows when process INTO", ERRCODE_NO_DATA_FOUND);
                    JIT_DEBUG_LOG("JIT SP exception: No rows found for INTO clause with STRICT execution");
                    JIT_THROW(JIT_CONST_UINT32(ERRCODE_NO_DATA_FOUND));
                }
                JIT_IF_END();
            }
            if (strict || mod) {
                JIT_IF_BEGIN(strict_res_too_many_check);
                JIT_IF_EVAL_CMP(tuplesProcessed, JIT_CONST_INT32(1), JitExec::JIT_ICMP_GT);
                {
                    HandleTooManyRows(ctx, tcount);
                }
                JIT_IF_END();
            }
        } else {
            // we check number of tuples only for select sub-queries (be careful with non-jittable query)
            JitCallSitePlan* callSitePlan = &ctx->m_plan->m_callSitePlanList[subQueryId];
            if ((callSitePlan->m_queryPlan && IsSelectCommand(callSitePlan->m_queryPlan->_command_type)) ||
                (!callSitePlan->m_queryPlan && (callSitePlan->m_queryCmdType == CMD_SELECT))) {
                JIT_IF_BEGIN(res_no_into_check);
                JIT_IF_EVAL_CMP(tuplesProcessed, JIT_CONST_INT32(0), JitExec::JIT_ICMP_GT);
                {
                    HandleTooManyRows(ctx, tcount);
                }
                JIT_IF_END();
            }
        }
    }
}

static void CheckThrownException(JitLlvmFunctionCodeGenContext* ctx)
{
    JIT_IF_BEGIN(check_exception);

    llvm::Value* exceptionStatus = JIT_EXCEPTION_STATUS();
    JIT_IF_EVAL(exceptionStatus);
    {
        // just re-throw - catch handler will load error info
        JIT_RETHROW();
    }
    JIT_IF_END();
}

static bool UpdateTCount(SPIPlanPtr spiPlan, int& tcount)
{
    volatile bool result = false;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    PG_TRY();
    {
        CachedPlan* cplan = SPI_plan_get_cached_plan(spiPlan);
        if (cplan == nullptr) {
            MOT_LOG_TRACE("Cannot get cached plan from SPI plan");
        } else {
            Node* stmt = (Node*)linitial(cplan->stmt_list);
            bool canSetTag = false;
            if (IsA(stmt, PlannedStmt)) {
                canSetTag = ((PlannedStmt*)stmt)->canSetTag;
            }
            if (!canSetTag) {
                tcount = 0;  // run to completion
            }
            ReleaseCachedPlan(cplan, spiPlan->saved);
            result = true;
        }
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while getting cached plan: %s", edata->message);
        FlushErrorState();
        FreeErrorData(edata);
    }
    PG_END_TRY();

    return (bool)result;
}

static llvm::Value* ExecSubQuery(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_expr* expr, int tcount, bool strict,
    bool mod, bool into, int* subQueryId, bool isPerform /* = false */)
{
    // find sub-query id by query string
    char* queryString = expr->query;
    *subQueryId = -1;
    for (int i = 0; i < ctx->m_plan->_query_count; ++i) {
        JitCallSitePlan* callSitePlan = &ctx->m_plan->m_callSitePlanList[i];
        if (strcmp(queryString, callSitePlan->m_queryString) == 0) {
            *subQueryId = i;
            break;
        }
    }
    if (*subQueryId == -1) {
        MOT_LOG_TRACE("execSubQuery(): Could not find sub-query: %s", queryString);
        return nullptr;
    }
    MOT_LOG_TRACE("Found sub-query %d: %s", *subQueryId, queryString);

    JitCallSitePlan* callSitePlan = &ctx->m_plan->m_callSitePlanList[*subQueryId];
    if (into && (callSitePlan->m_queryCmdType != CMD_SELECT)) {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
            "JIT Compile",
            "INTO used with a command that cannot return data, on sub-query %d: %s",
            *subQueryId,
            callSitePlan->m_queryString);
        return nullptr;
    } else if (!into && !isPerform && (callSitePlan->m_queryCmdType == CMD_SELECT)) {
        // unless this is invoke into procedure, this is unacceptable
        if (((callSitePlan->m_queryPlan != nullptr) && (callSitePlan->m_queryPlan->_plan_type != JIT_PLAN_INVOKE)) ||
            ((callSitePlan->m_queryPlan == nullptr) && (callSitePlan->m_isUnjittableInvoke))) {
            MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
                "JIT Compile",
                "query has no destination for result data, on sub-query %d: %s",
                *subQueryId,
                callSitePlan->m_queryString);
            return nullptr;
        }
    }
    JIT_DEBUG_LOG_INT("Passing %d parameters to sub query", callSitePlan->m_callParamCount);
    for (int i = 0; i < callSitePlan->m_callParamCount; ++i) {
        JitCallParamInfo* param = &callSitePlan->m_callParamInfo[i];
        if (param->m_paramKind == JitCallParamKind::JIT_CALL_PARAM_ARG) {
            // pass parameter by ref (already handled directly by ExecuteSubQuery)
            MOT_LOG_TRACE("Passing parameter %d to from argument %d to sub-query %d by reference (SP argument)",
                param->m_paramIndex,
                param->m_invokeArgIndex,
                *subQueryId);
            JIT_DEBUG_LOG_INT("Passing argument at pos %d to sub query by reference", param->m_invokeArgIndex);
        } else {
            // pass parameter by value
            int datumIndex = param->m_invokeDatumIndex;
            MOT_LOG_TRACE("Passing parameter %d from datum %d to sub-query %d by value (SP local variable)",
                param->m_paramIndex,
                datumIndex,
                *subQueryId);
            llvm::Value* value = GetLocalOrParamByVarNo(ctx, datumIndex);
            llvm::Value* loadedValue = ctx->m_builder->CreateLoad(ctx->DATUM_T, value, true);
            value = GetIsNullLocalOrParamByVarNo(ctx, datumIndex);
            llvm::Value* isNull = ctx->m_builder->CreateLoad(ctx->BOOL_T, value, true);
            llvm::Value* isNull32 = ctx->m_builder->CreateCast(llvm::Instruction::CastOps::SExt, isNull, ctx->INT32_T);
            AddSetSPSubQueryParamValue(
                ctx, *subQueryId, param->m_paramIndex, param->m_paramType, loadedValue, isNull32);
        }
    }

    // fix tcount - this can throw ereport, so be careful
    if (!UpdateTCount(callSitePlan->m_spiPlan, tcount)) {
        MOT_LOG_TRACE("Failed to update tcount");
        return nullptr;
    }

    // prepare call-site for query in the function context
    llvm::Value* queryResult = nullptr;
    InjectProfileData(ctx, MOT_JIT_PROFILE_REGION_CHILD_CALL, true);
    queryResult = AddExecuteSubQuery(ctx, *subQueryId, tcount);
    InjectProfileData(ctx, MOT_JIT_PROFILE_REGION_CHILD_CALL, false);
    JIT_DEBUG_LOG("Returned from sub-query");

    // check for invalid SQL state
    JIT_IF_BEGIN(check_subq_except);
    llvm::Value* sqlState = AddGetSqlState(ctx);
    JIT_IF_EVAL(sqlState);
    {
        // mark exception origin as external and let the catch handler to load error info
        AddSetExceptionOrigin(ctx, JIT_EXCEPTION_EXTERNAL);
        JIT_THROW(sqlState);
    }
    JIT_IF_END();

    // check for other exception (not due to some invalid SQL state)
    CheckThrownException(ctx);

    // collect result and process it
    llvm::Value* tuplesProcessed = AddGetTuplesProcessed(ctx, *subQueryId);
    ProcessSubQueryResult(ctx, *subQueryId, queryResult, tuplesProcessed, strict, mod, tcount, into, isPerform);
    return tuplesProcessed;
}

static bool AssignScalarValue(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_var* target, llvm::Value* assignee,
    llvm::Value* assigneeIsNull, llvm::Value* value, llvm::Value* isNull, Oid sourceType)
{
    // first check if null is allowed
    if (target->notnull) {
        JIT_IF_BEGIN(var_not_null);
        JIT_IF_EVAL(isNull);
        {
            AddSetSqlStateAndErrorMessage(
                ctx, "null value cannot be assigned to variable declared NOT NULL", ERRCODE_NULL_VALUE_NOT_ALLOWED);
            JIT_THROW(JIT_CONST_UINT32(ERRCODE_NULL_VALUE_NOT_ALLOWED));
        }
        JIT_IF_END();
    }

    // collect cast information
    Oid targetType = target->datatype->typoid;
    int32 typeMod = target->datatype->atttypmod;
    Oid funcId = InvalidOid;
    CoercionPathType coercePath = COERCION_PATH_NONE;
    Oid funcId2 = InvalidOid;
    CoercionPathType coercePath2 = COERCION_PATH_NONE;
    int nargs = 0;
    if ((targetType != sourceType) || (typeMod != -1)) {
        MOT_LOG_TRACE("AssignScalarValue(): converting from type %u to type %u", sourceType, targetType);
        coercePath = find_coercion_pathway(targetType, sourceType, COERCION_ASSIGNMENT, &funcId);
        if ((funcId != InvalidOid) &&
            !(coercePath == COERCION_PATH_COERCEVIAIO || coercePath == COERCION_PATH_ARRAYCOERCE)) {
            MOT_LOG_TRACE("AssignScalarValue(): Found coercion path via func id: %d", funcId);
            coercePath2 = find_typmod_coercion_function(targetType, &funcId2);
            if (coercePath2 == COERCION_PATH_FUNC && OidIsValid(funcId2)) {
                nargs = get_func_nargs(funcId2);
            }
        }
    }

    // call cast function
    JIT_IF_BEGIN(assign_null);
    JIT_IF_EVAL_NOT(isNull);
    {
        if ((targetType != sourceType) || (typeMod != -1) || !target->datatype->typbyval) {
            llvm::Value* castResult = AddCastValue(ctx,
                value,
                sourceType,
                targetType,
                typeMod,
                coercePath,
                funcId,
                coercePath2,
                funcId2,
                nargs,
                target->datatype->typbyval);
            CheckThrownException(ctx);
            ctx->m_builder->CreateStore(castResult, assignee, true);
        } else {
            ctx->m_builder->CreateStore(value, assignee, true);
        }
    }
    JIT_IF_END();

    // store null value
    llvm::Value* isNullBool = ctx->m_builder->CreateCast(llvm::Instruction::Trunc, isNull, ctx->BOOL_T);
    ctx->m_builder->CreateStore(isNullBool, assigneeIsNull, true);

#ifdef MOT_JIT_DEBUG
    llvm::Value* loadedValue = ctx->m_builder->CreateLoad(ctx->DATUM_T, assignee, true);
    llvm::Value* loadedIsNull = ctx->m_builder->CreateLoad(ctx->BOOL_T, assigneeIsNull, true);
    JIT_DEBUG_LOG_DATUM("Stored value", loadedValue, loadedIsNull, targetType);
#endif

    return true;
}

static bool AssignValue(JitLlvmFunctionCodeGenContext* ctx, int varNo, PLpgSQL_expr* expr)
{
    MOT_LOG_TRACE("Assign into varno %d", varNo);
    JIT_DEBUG_LOG_VARNO(varNo);
    llvm::Value* assignee = GetLocalOrParamByVarNo(ctx, varNo);
    llvm::Value* assigneeIsNull = GetIsNullLocalOrParamByVarNo(ctx, varNo);
    if (assignee == nullptr) {
        MOT_LOG_TRACE("Failed to assign value: Failed to find local assignee by varno %d", varNo);
        return false;
    }

    Oid valueType;
    llvm::Value* value = ProcessExpr(ctx, expr, &valueType);
    if (value == nullptr) {
        MOT_LOG_TRACE("Failed to assign value: Failed to process value expression");
        return false;
    }
    llvm::Value* isNull = AddGetExprIsNull(ctx);

    // code adapted from exec_assign_value() in pl_exec.cpp
    PLpgSQL_datum* target = ctx->_compiled_function->datums[varNo];
    if (target->dtype == PLPGSQL_DTYPE_VAR) {
        JIT_DEBUG_LOG_INT("Assigning scalar into varno: %d", varNo);
        bool result = AssignScalarValue(ctx, (PLpgSQL_var*)target, assignee, assigneeIsNull, value, isNull, valueType);
        JIT_DEBUG_LOG_VARNO(varNo);
#ifdef MOT_JIT_DEBUG
        llvm::Value* loadedValue = ctx->m_builder->CreateLoad(ctx->DATUM_T, assignee, true);
        llvm::Value* loadedIsNull = ctx->m_builder->CreateLoad(ctx->BOOL_T, assigneeIsNull, true);
        JIT_DEBUG_LOG_DATUM(
            "Stored value after assign 2", loadedValue, loadedIsNull, ((PLpgSQL_var*)target)->datatype->typoid);
#endif
        return result;
    } else {
        MOT_LOG_TRACE("Unsupported assignment");
        return false;
    }

    bool failed = false;
    JIT_IF_BEGIN(assign_null);
    JIT_IF_EVAL_NOT(isNull);
    {
        llvm::StoreInst* storeInst = ctx->m_builder->CreateStore(value, assignee, true);
        if (!storeInst) {
            MOT_LOG_TRACE("Failed to create store instruction");
            failed = true;
        }
    }
    JIT_IF_END();
    if (failed) {
        return false;
    }

    llvm::Value* isNullBool = ctx->m_builder->CreateCast(llvm::Instruction::Trunc, isNull, ctx->BOOL_T);
    llvm::StoreInst* storeInst = ctx->m_builder->CreateStore(isNullBool, assigneeIsNull, true);
    if (!storeInst) {
        MOT_LOG_TRACE("Failed to create store instruction");
        return false;
    }

    return true;
}

static bool IsSubQueryResultComposite(JitLlvmFunctionCodeGenContext* ctx, int subQueryId)
{
    bool result = false;
    if (subQueryId < ctx->m_plan->_query_count) {
        JitCallSitePlan* callSitePlan = &ctx->m_plan->m_callSitePlanList[subQueryId];
        if ((callSitePlan->m_tupDesc->natts == 1) && (callSitePlan->m_tupDesc->attrs[0].atttypid == RECORDOID)) {
            result = true;
        }
    }
    MOT_LOG_DEBUG("Sub-query %d is composite: %s", subQueryId, result ? "true" : "false");
    return result;
}

static void DetermineStrictFlagAndTCount(PLpgSQL_stmt_execsql* stmt, bool& strict, int& tcount)
{
    strict = stmt->strict;
    if (stmt->into) {
        if (!stmt->mod_stmt) {
            strict = true;
        }
        if (strict || stmt->mod_stmt) {
            tcount = 2;
        } else {
            tcount = 1;
        }
    }
}

static bool ProcessStatementExecSql(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_execsql* stmt)
{
    // determine the number of invocation of the query
    int tcount = 0;
    bool strict = stmt->strict;
    DetermineStrictFlagAndTCount(stmt, strict, tcount);

    MOT_LOG_TRACE("Processing statement: exec-sql");
    int subQueryId = -1;
    llvm::Value* queryResult =
        ExecSubQuery(ctx, stmt->sqlstmt, tcount, strict, stmt->mod_stmt, stmt->into, &subQueryId);
    if ((queryResult == nullptr) || (subQueryId < 0)) {
        MOT_LOG_TRACE("processStatementExecSql(): Failed to process sub-query");
        return false;
    }

    if (stmt->into) {
        // Determine if we assign to a record or a row
        PLpgSQL_rec* rec = nullptr;
        PLpgSQL_row* row = nullptr;
        if (stmt->rec != nullptr) {
            rec = (PLpgSQL_rec*)(ctx->_compiled_function->datums[stmt->rec->dno]);
        } else if (stmt->row != nullptr) {
            row = (PLpgSQL_row*)(ctx->_compiled_function->datums[stmt->row->dno]);
        } else {
            MOT_LOG_TRACE("unsupported target, use record and row instead.");
            return false;
        }

        bool failed = false;
        JIT_IF_BEGIN(into_null);
        JIT_IF_EVAL_NOT(queryResult);
        {
            // set the target to NULL(s)
            for (int i = 0; i < stmt->row->nfields; ++i) {
                int varNo = stmt->row->varnos[i];
                if (varNo >= 0) {  // skip dropped columns
                    llvm::Value* resultVar = GetLocalOrParamByVarNo(ctx, varNo);
                    llvm::Value* resultIsNullVar = GetIsNullLocalOrParamByVarNo(ctx, varNo);
                    llvm::StoreInst* storeInst = ctx->m_builder->CreateStore(JIT_CONST_UINT64(0), resultVar, true);
                    if (!storeInst) {
                        MOT_LOG_TRACE("Failed to store zero in datum value");
                        failed = true;
                        break;
                    }
                    storeInst = ctx->m_builder->CreateStore(JIT_CONST_BOOL(true), resultIsNullVar, true);
                    if (!storeInst) {
                        MOT_LOG_TRACE("Failed to store non-zero value in datum is-null property");
                        failed = true;
                        break;
                    }
                }
            }
        }
        JIT_ELSE();
        {
            // Put the result row into the target variables
            bool subQueryResultComposite = IsSubQueryResultComposite(ctx, subQueryId);
            llvm::Value* resultHeapTuple = nullptr;
            llvm::Value* isNullRef = nullptr;
            if (subQueryResultComposite) {
                resultHeapTuple = AddGetSubQueryResultHeapTuple(ctx, subQueryId);
                isNullRef = ctx->m_builder->CreateAlloca(ctx->INT32_T);
            }
            llvm::Value* slotValue = nullptr;
            llvm::Value* isNullValue = nullptr;
            for (int i = 0; i < stmt->row->nfields; ++i) {
                int varNo = stmt->row->varnos[i];
                if (varNo >= 0) {  // skip dropped columns
                    if (subQueryResultComposite) {
                        slotValue = AddGetHeapTupleValue(ctx, resultHeapTuple, subQueryId, i, isNullRef);
                        isNullValue = ctx->m_builder->CreateLoad(isNullRef);
                    } else {
                        slotValue = AddGetSubQuerySlotValue(ctx, subQueryId, i);
                        isNullValue = AddGetSubQuerySlotIsNull(ctx, subQueryId, i);
                    }
                    PLpgSQL_datum* target = ctx->_compiled_function->datums[varNo];

                    llvm::Value* resultVar = GetLocalOrParamByVarNo(ctx, varNo);
                    llvm::Value* resultIsNullVar = GetIsNullLocalOrParamByVarNo(ctx, varNo);

                    JIT_DEBUG_LOG_INT("Assigning scalar from sub-query into varno: %d", varNo);
                    PLpgSQL_datum* datum = ctx->_compiled_function->datums[varNo];
                    if (datum->dtype != PLPGSQL_DTYPE_VAR) {
                        MOT_LOG_TRACE("Cannot assign non-var row field at var-no: %d", varNo);
                        failed = true;
                        break;
                    }
                    JitCallSitePlan* callSitePlan = &ctx->m_plan->m_callSitePlanList[subQueryId];
                    Oid slotType = callSitePlan->m_tupDesc->attrs[i].atttypid;
                    if (!AssignScalarValue(
                            ctx, (PLpgSQL_var*)target, resultVar, resultIsNullVar, slotValue, isNullValue, slotType)) {
                        MOT_LOG_TRACE("Failed to assign scalar value form sub-query");
                        failed = true;
                        break;
                    }
                    JIT_DEBUG_LOG_VARNO(varNo);
#ifdef MOT_JIT_DEBUG
                    llvm::Value* loadedValue = ctx->m_builder->CreateLoad(ctx->DATUM_T, resultVar, true);
                    llvm::Value* loadedIsNull = ctx->m_builder->CreateLoad(ctx->BOOL_T, resultIsNullVar, true);
                    JIT_DEBUG_LOG_DATUM(
                        "Stored value 2", loadedValue, loadedIsNull, ((PLpgSQL_var*)target)->datatype->typoid);
#endif
                }
            }
        }
        JIT_IF_END();
        if (failed) {
            return false;
        }
    }

    // cleanup non-jittable sub-query resources (SPI tub-table can be released now after datum values copied to result)
    AddReleaseNonJitSubQueryResources(ctx, subQueryId);

    return true;
}

static bool ProcessStatementPerform(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_perform* stmt)
{
    MOT_LOG_TRACE("Processing statement: perform");
    int subQueryId = -1;
    llvm::Value* queryResult = ExecSubQuery(ctx, stmt->expr, 0, false, false, false, &subQueryId, true);
    if ((queryResult == nullptr) || (subQueryId < 0)) {
        MOT_LOG_TRACE("ProcessStatementPerform(): Failed to process sub-query");
        return false;
    }
    return true;
}

static bool ProcessStatementDynExecute(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_dynexecute* stmt)
{
    // Not supported
    MOT_LOG_TRACE("Processing statement: dyn-exec-sql");
    return false;
}

static bool ProcessStatementDynForS(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_dynfors* stmt)
{
    // Not supported
    MOT_LOG_TRACE("Processing statement: dyn-fors");
    return false;
}

static bool ProcessStatementGetDiag(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_getdiag* stmt)
{
    // Not supported
    MOT_LOG_TRACE("Processing statement: get-diag");
    return false;
}

static bool ProcessStatementOpen(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_open* stmt)
{
    // Not supported
    MOT_LOG_TRACE("Processing statement: open");
    return false;
}

static bool ProcessStatementFetch(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_fetch* stmt)
{
    // Not supported
    MOT_LOG_TRACE("Processing statement: fetch");
    return false;
}

static bool ProcessStatementClose(JitLlvmFunctionCodeGenContext* ctx, PLpgSQL_stmt_close* stmt)
{
    // Not supported
    MOT_LOG_TRACE("Processing statement: close");
    return false;
}

static bool DefineLocalVariables(JitLlvmFunctionCodeGenContext* ctx)
{
    for (int i = 0; i < ctx->_compiled_function->ndatums; ++i) {
        if (i == ctx->_compiled_function->sql_bulk_exceptions_varno) {
            // This datum is used only for SAVE EXCEPTIONS and SQL%BULK_EXCEPTION, which we don't support.
            continue;
        }

        if (ctx->m_datums[i] == nullptr) {  // skip already defined function arguments
            if (!DefineBlockLocalVar(ctx, i)) {
                MOT_LOG_TRACE("Failed to define local variable %d", i);
                return false;
            }
        }
    }

    return true;
}

static bool InitLocalVariables(JitLlvmFunctionCodeGenContext* ctx)
{
    for (int i = 0; i < ctx->_compiled_function->ndatums; ++i) {
        if (i == ctx->_compiled_function->sql_bulk_exceptions_varno) {
            // This datum is used only for SAVE EXCEPTIONS and SQL%BULK_EXCEPTION, which we don't support.
            continue;
        }

        if (ctx->m_datums[i] != nullptr) {
            if (!InitBlockLocalVar(ctx, i)) {
                MOT_LOG_TRACE("Failed to initialize local variable %d", i);
                return false;
            }
        }
    }

    return true;
}

extern MotJitContext* JitCodegenLlvmFunction(PLpgSQL_function* function, HeapTuple procTuple, Oid functionOid,
    ReturnSetInfo* returnSetInfo, JitPlan* plan, JitCodegenStats& codegenStats)
{
    JIT_ASSERT_LLVM_CODEGEN_UTIL_VALID();
    bool procNameIsNull = false;
    bool procSrcIsNull = false;
    bool procIsStrictIsNull = false;
    Datum procNameDatum = SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_proname, &procNameIsNull);
    Datum procSrcDatum = SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_prosrc, &procSrcIsNull);
    Datum procIsStrictDatum = SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_proisstrict, &procIsStrictIsNull);
    if (procNameIsNull || procSrcIsNull || procIsStrictIsNull) {
        MOT_LOG_TRACE("Failed to generate jitted code for stored procedure: catalog entry for stored procedure "
                      "contains null attributes");
        JIT_ASSERT_LLVM_CODEGEN_UTIL_VALID();
        return nullptr;
    }

    // NOTE: every variable used after catch needs to be volatile (see longjmp() man page)
    volatile char* functionName = NameStr(*DatumGetName(procNameDatum));
    MOT_LOG_TRACE("Generating LLVM-jitted code for stored procedure %s with %d args at thread %p",
        functionName,
        function->fn_nargs,
        (void*)pthread_self());

    // get required function attributes
    char* functionSource = TextDatumGetCString(procSrcDatum);
    if (!functionSource) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "Parse Stored Procedure", "Failed to allocate memory for stored procedure source code");
        JIT_ASSERT_LLVM_CODEGEN_UTIL_VALID();
        return nullptr;
    }
    bool isStrict = DatumGetBool(procIsStrictDatum);

    // connect to SPI for query parsing/analysis
    // note: usually we should already be connected to the SPI manager (since we are called from plpgsql_validator())

    // prepare compile context
    GsCodeGen* codeGen = SetupCodegenEnv();
    if (codeGen == nullptr) {
        JIT_ASSERT_LLVM_CODEGEN_UTIL_VALID();
        return nullptr;
    }
    GsCodeGen::LlvmBuilder builder(codeGen->context());
    volatile JitLlvmFunctionCodeGenContext cgCtx = {};
    if (!InitLlvmFunctionCodeGenContext(
            (JitLlvmFunctionCodeGenContext*)&cgCtx, codeGen, &builder, function, returnSetInfo)) {
        JIT_ASSERT_LLVM_CODEGEN_UTIL_VALID();
        FreeGsCodeGen(codeGen);
        return nullptr;
    }
    cgCtx.m_plan = (JitFunctionPlan*)plan;
    volatile JitLlvmFunctionCodeGenContext* ctx = &cgCtx;
    ctx->m_resultTupDesc = nullptr;
    ctx->m_rowTupDesc = nullptr;

    // we temporarily cause the function to be treated as triggered, so we don't need execution state for parsing
    volatile bool preParseTrig = function->pre_parse_trig;
    function->pre_parse_trig = true;
    volatile MotJitContext* jitContext = nullptr;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    PG_TRY();
    {
        do {  // instead of goto
            // prepare the jitted function (declare, get arguments into context and define locals)
            std::string jittedFunctionName = std::string("MotJittedSp") + "_" + (char*)functionName;
            if (!CreateJittedFunction(
                    (JitLlvmFunctionCodeGenContext*)ctx, jittedFunctionName.c_str(), functionSource, isStrict)) {
                break;
            }
            JIT_DEBUG_LOG("Starting execution of jitted stored procedure");

            InjectProfileData((JitLlvmFunctionCodeGenContext*)ctx, MOT_JIT_PROFILE_REGION_INIT_VARS, true);

            if (!DefineLocalVariables((JitLlvmFunctionCodeGenContext*)ctx)) {
                MOT_LOG_TRACE("Failed to generate jitted code for stored procedure: failed to define local variables");
                break;
            }

            bool failed = false;
            JIT_TRY_BEGIN(top_init_vars)
            {
                if (!InitLocalVariables((JitLlvmFunctionCodeGenContext*)ctx)) {
                    MOT_LOG_TRACE(
                        "Failed to generate jitted code for stored procedure: failed to initialize local variables");
                    failed = true;
                }
            }
            if (!failed) {
                JIT_CATCH_ALL();
                {
                    // cleanup run-time exception stack and report error to user
                    JIT_DEBUG_LOG("Uncaught exception in top init vars block");
                    AddLlvmClearExceptionStack((JitLlvmFunctionCodeGenContext*)ctx);

                    // inject profile data before exiting
                    InjectProfileData((JitLlvmFunctionCodeGenContext*)ctx, MOT_JIT_PROFILE_REGION_INIT_VARS, false);
                    InjectProfileData((JitLlvmFunctionCodeGenContext*)ctx, MOT_JIT_PROFILE_REGION_TOTAL, false);

                    // since this might be a normal flow of events for sub-SP, we return a special return code to avoid
                    // error reporting in such case
                    JIT_RETURN(JIT_CONST_INT32(MOT::RC_JIT_SP_EXCEPTION));
                }
                JIT_END_CATCH();
            }
            JIT_TRY_END()
            if (failed) {
                break;
            }

            InjectProfileData((JitLlvmFunctionCodeGenContext*)ctx, MOT_JIT_PROFILE_REGION_INIT_VARS, false);

            // clear result slot
            AddExecClearTuple((JitLlvmFunctionCodeGenContext*)ctx);

            // process main statement block of the function
            if (!ProcessFunctionAction((JitLlvmFunctionCodeGenContext*)ctx, function)) {
                MOT_LOG_TRACE(
                    "Failed to generate jitted code for stored procedure: failed to process main statement block");
                break;
            }

            // wrap up
            jitContext = FinalizeCodegen(
                (JitLlvmFunctionCodeGenContext*)ctx, (JitFunctionPlan*)plan, (const char*)functionName, codegenStats);
        } while (0);
    }
    PG_CATCH();
    {
        // cleanup
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN(
            "Caught exception while generating LLVM code for stored procedure '%s': %s", functionName, edata->message);
        FlushErrorState();
        FreeErrorData(edata);
    }
    PG_END_TRY();

    // cleanup
    function->pre_parse_trig = preParseTrig;
    DestroyLlvmFunctionCodeGenContext((JitLlvmFunctionCodeGenContext*)ctx);

    // reset compile state for robustness
    llvm_util::JitResetCompileState();
    JIT_ASSERT_LLVM_CODEGEN_UTIL_VALID();
    return (MotJitContext*)jitContext;
}

extern int JitExecLlvmFunction(JitFunctionContext* jitContext, ParamListInfo params, TupleTableSlot* slot,
    uint64_t* tuplesProcessed, int* scanEnded)
{
    MOT_ASSERT(jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION);
    JitFunctionExecState* execState = (JitFunctionExecState*)jitContext->m_execState;

    // NOTE: every variable used after catch needs to be volatile (see longjmp() man page)
    volatile int result = 0;
    SubTransactionId subXid = ::GetCurrentSubTransactionId();

    MOT_LOG_TRACE("Calling sigsetjmp on faultBuf %p, function: %s", execState->m_faultBuf, jitContext->m_queryString);

    // we setup a jump buffer in the execution state for fault handling
    if (sigsetjmp(execState->m_faultBuf, 1) == 0) {
        // execute the jitted-function
        result = jitContext->m_llvmSPFunction(params, slot, tuplesProcessed, scanEnded);
    } else {
        // invoke the global fault handler if there is any registered
        uint64_t faultCode = execState->m_exceptionValue;
        bytea* errorMessage = DatumGetByteaP(execState->m_errorMessage);
        bytea* sqlState = DatumGetByteaP(execState->m_sqlStateString);
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Execute JIT",
            "Encountered run-time fault %" PRIu64 " (%s), error: %.*s, SQL state: %.*s, while executing %s",
            faultCode,
            llvm_util::LlvmRuntimeFaultToString(faultCode),
            VARSIZE(errorMessage) - VARHDRSZ,
            (char*)VARDATA(errorMessage),
            VARSIZE(sqlState) - VARHDRSZ,
            (char*)VARDATA(sqlState),
            jitContext->m_queryString);

        JitReleaseAllSubTransactions(true, subXid);

        // report error to user only for top level context
        if (jitContext->m_parentContext && !IsJitSubContextInline(jitContext->m_parentContext)) {
            const char* errorDetail = nullptr;
            const char* errorHint = nullptr;
            bytea* txt = DatumGetByteaP(execState->m_errorMessage);
            const char* errorMessage = (const char*)VARDATA(txt);
            if (execState->m_errorDetail != 0) {
                bytea* txtd = DatumGetByteaP(execState->m_errorDetail);
                errorDetail = (const char*)VARDATA(txtd);
            }
            if (execState->m_errorHint != 0) {
                bytea* txth = DatumGetByteaP(execState->m_errorHint);
                errorHint = (const char*)VARDATA(txth);
            }
            PG_TRY();
            {
                RaiseEreport(jitContext->m_execState->m_sqlState, errorMessage, errorDetail, errorHint);
            }
            PG_CATCH();
            {
                EmitErrorReport();
                FlushErrorState();
            }
            PG_END_TRY();
        }
        result = MOT::RC_ERROR;
    }
    return result;
}
}  // namespace JitExec
