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
 * jit_llvm_sp.h
 *    JIT LLVM stored-procedure common definitions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_llvm_sp.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_LLVM_SP_H
#define JIT_LLVM_SP_H

/*
 * ATTENTION:
 * 1. Be sure to include gscodegen.h before anything else to avoid clash with PM definition in datetime.h.
 * 2. Be sure to include libintl.h before gscodegen.h to avoid problem with gettext.
 */
#include "libintl.h"
#include "codegen/gscodegen.h"
#include "storage/mot/jit_exec.h"
#include "funcapi.h"

#include "jit_common.h"
#include "jit_llvm_util.h"

#include <map>
#include <string>
#include <vector>
#include <set>

struct PLpgSQL_function;
struct PLpgSQL_expr;

namespace JitExec {
// forward declaration
struct JitFunctionPlan;

using GotoLabelMap = std::map<std::string, llvm::BasicBlock*>;

struct LabelDesc {
    const char* m_name;
    llvm::BasicBlock* m_loopStart;
    llvm::BasicBlock* m_postBlock;  // either loop or block
    bool m_isLoop;
};
using LabelStack = std::list<LabelDesc>;

/** @struct Context used for compiling llvm-jitted functions. */
struct JitLlvmFunctionCodeGenContext : public llvm_util::LlvmCodeGenContext {
    /** @var The plan used to create the jitted code. */
    JitFunctionPlan* m_plan;

    /** @var The PG compiled function. */
    PLpgSQL_function* _compiled_function;

    /** @var The function type class (for returning composite type). */
    TypeFuncClass m_typeFuncClass;

    /** @var The function result tuple descriptor (for returning composite type). */
    TupleDesc m_resultTupDesc;

    /** @var The function row tuple descriptor (for returning composite type). */
    TupleDesc m_rowTupDesc;

    // PG Types
    llvm::StructType* ParamExternDataType;
    llvm::StructType* ParamListInfoDataType;
    llvm::StructType* TupleTableSlotType;
    llvm::StructType* NumericDataType;
    llvm::StructType* VarCharType;
    llvm::StructType* BpCharType;

    // args
    llvm::Value* m_params;
    llvm::Value* m_slot;
    llvm::Value* m_tuplesProcessed;
    llvm::Value* m_scanEnded;

    // MOT Types
    llvm::StructType* RowType;

    // function types
    llvm::StructType* NullableDatumType;

    // arguments and local variables are identified by index, so we keep all of them in one bug array
    std::vector<llvm::Value*> m_datums;

    // arguments and local variables are identified by index, so we keep all of them in one bug array
    std::vector<llvm::Value*> m_datumNulls;

    /** @var The sub-transaction id with which the function started executing. */
    llvm::Value* m_initSubXid;

    // map of goto labels for each named block (label name and block name are identical, but block name might have id
    // suffix)
    GotoLabelMap m_gotoLabels;
    LabelStack m_labelStack;
    int m_exceptionBlockCount;

    bool m_usingMagicVars;

    // non-primitive default parameter constants
    uint32_t m_defaultValueCount;
    Const* m_defaultValues;

    // non-primitive constants in code
    uint32_t m_constCount;
    Const* m_constValues;
    bool* m_selfManaged;

    /** @var The PG processed block (for managing SQL state and error message. */
    PLpgSQL_stmt_block* m_processedBlock;

    // helper functions
#ifdef MOT_JIT_DEBUG
    llvm::FunctionCallee m_debugLogFunc;
    llvm::FunctionCallee m_debugLogIntFunc;
    llvm::FunctionCallee m_debugLogStringFunc;
    llvm::FunctionCallee m_debugLogStringDatumFunc;
    llvm::FunctionCallee m_debugLogDatumFunc;
#endif
    llvm::FunctionCallee m_getCurrentSubTransactionIdFunc;

    llvm::FunctionCallee m_beginBlockWithExceptionsFunc;
    llvm::FunctionCallee m_endBlockWithExceptionsFunc;
    llvm::FunctionCallee m_cleanupBlockAfterExceptionFunc;
    llvm::FunctionCallee m_getExceptionOriginFunc;
    llvm::FunctionCallee m_setExceptionOriginFunc;
    llvm::FunctionCallee m_cleanupBeforeReturnFunc;

    llvm::FunctionCallee m_convertViaStringFunc;
    llvm::FunctionCallee m_castValueFunc;
    llvm::FunctionCallee m_saveErrorInfoFunc;
    llvm::FunctionCallee m_getErrorMessageFunc;
    llvm::FunctionCallee m_getSqlStateFunc;
    llvm::FunctionCallee m_getSqlStateStringFunc;
    llvm::FunctionCallee m_getDatumIsNotNullFunc;
    llvm::FunctionCallee m_getExprIsNullFunc;
    llvm::FunctionCallee m_setExprIsNullFunc;
    llvm::FunctionCallee m_getExprCollationFunc;
    llvm::FunctionCallee m_setExprCollationFunc;
    llvm::FunctionCallee m_execClearTupleFunc;
    llvm::FunctionCallee m_execStoreVirtualTupleFunc;
    llvm::FunctionCallee m_getParamAtRefFunc;
    llvm::FunctionCallee m_isParamNullRefFunc;
    llvm::FunctionCallee m_isCompositeResultFunc;
    llvm::FunctionCallee m_createResultDatumsFunc;
    llvm::FunctionCallee m_createResultNullsFunc;
    llvm::FunctionCallee m_setResultValueFunc;
    llvm::FunctionCallee m_createResultHeapTupleFunc;
    llvm::FunctionCallee m_setSlotValueFunc;
    llvm::FunctionCallee m_setSPSubQueryParamValueFunc;
    llvm::FunctionCallee m_executeSubQueryFunc;
    llvm::FunctionCallee m_releaseNonJitSubQueryResourcesFunc;
    llvm::FunctionCallee m_getTuplesProcessedFunc;
    llvm::FunctionCallee m_getSubQuerySlotValueFunc;
    llvm::FunctionCallee m_getSubQuerySlotIsNullFunc;
    llvm::FunctionCallee m_getSubQueryResultHeapTupleFunc;
    llvm::FunctionCallee m_getHeapTupleValueFunc;
    llvm::FunctionCallee m_getSpiResultFunc;
    llvm::FunctionCallee m_setTpProcessedFunc;
    llvm::FunctionCallee m_setScanEndedFunc;
    llvm::FunctionCallee m_getConstAtFunc;
    llvm::FunctionCallee m_abortFunctionFunc;
    llvm::FunctionCallee m_llvmClearExceptionStackFunc;
    llvm::FunctionCallee m_emitProfileDataFunc;
};
}  // namespace JitExec

#endif /* JIT_LLVM_SP_H */
