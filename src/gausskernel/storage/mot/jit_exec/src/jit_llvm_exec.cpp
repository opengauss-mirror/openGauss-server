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
 * jit_llvm_exec.cpp
 *    LLVM JIT-compiled execution.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/src/jit_llvm_exec.cpp
 *
 * -------------------------------------------------------------------------
 */

// be careful to include gscodegen.h before anything else to avoid clash with PM definition in datetime.h
#include "global.h"
#include "codegen/gscodegen.h"
#include "codegen/builtinscodegen.h"
#include "catalog/pg_operator.h"
#include "codegen/datecodegen.h"
#include "codegen/timestampcodegen.h"
#include "utils/fmgroids.h"
#include "nodes/parsenodes.h"
#include "storage/ipc.h"
#include "nodes/pg_list.h"
#include "tcop/dest.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "catalog/pg_aggregate.h"

#include "mot_internal.h"
#include "storage/mot/jit_exec.h"
#include "jit_common.h"
#include "jit_llvm_exec.h"
#include "jit_plan.h"
#include "jit_llvm_util.h"
#include "jit_util.h"

#include "mot_engine.h"
#include "utilities.h"
#include "mot_internal.h"
#include "catalog_column_types.h"
#include "mot_error.h"
#include "utilities.h"

#include <vector>
#include <assert.h>

// for  checking if LLVM_ENABLE_DUMP is defined
#include "llvm/Config/llvm-config.h"

extern bool GlobalCodeGenEnvironmentSuccess;
extern bool isCPUFeatureSupportCodegen();

namespace JitExec {
DECLARE_LOGGER(JitLlvm, JitExec);

using namespace dorado;

// forward declarations
struct JitLlvmCodeGenContext;
static bool ProcessJoinOpExpr(
    JitLlvmCodeGenContext* ctx, const OpExpr* op_expr, int* column_count, int* column_array, int* max_arg);
static bool ProcessJoinBoolExpr(
    JitLlvmCodeGenContext* ctx, const BoolExpr* boolexpr, int* column_count, int* column_array, int* max_arg);
static llvm::Value* ProcessExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitExpr* expr, int* max_arg);
static llvm::Value* ProcessFilterExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitFilter* filter, int* max_arg);

/** @struct Holds instructions that evaluate in runtime to begin and end iterators of a cursor. */
struct JitLlvmRuntimeCursor {
    /** @var The iterator pointing to the beginning of the range. */
    llvm::Value* begin_itr;

    /** @var The iterator pointing to the end of the range. */
    llvm::Value* end_itr;
};

/** @struct Code generation context. */
struct JitLlvmCodeGenContext {
    // primitive types
    llvm::IntegerType* BOOL_T;
    llvm::IntegerType* INT8_T;
    llvm::IntegerType* INT16_T;
    llvm::IntegerType* INT32_T;
    llvm::IntegerType* INT64_T;
    llvm::Type* VOID_T;
    llvm::Type* FLOAT_T;
    llvm::Type* DOUBLE_T;
    llvm::PointerType* STR_T;
    llvm::IntegerType* DATUM_T;

    // PG Types
    llvm::StructType* ParamExternDataType;
    llvm::StructType* ParamListInfoDataType;
    llvm::StructType* TupleTableSlotType;
    llvm::StructType* NumericDataType;
    llvm::StructType* VarCharType;
    llvm::StructType* BpCharType;

    // MOT types
    llvm::StructType* MMEngineType;
    llvm::StructType* TableType;
    llvm::StructType* IndexType;
    llvm::StructType* KeyType;
    llvm::StructType* ColumnType;
    llvm::StructType* RowType;
    llvm::StructType* BitmapSetType;
    llvm::StructType* IndexIteratorType;

    // helper functions
    llvm::Constant* debugLogFunc;
    llvm::Constant* isSoftMemoryLimitReachedFunc;
    llvm::Constant* getPrimaryIndexFunc;
    llvm::Constant* getTableIndexFunc;
    llvm::Constant* initKeyFunc;
    llvm::Constant* getColumnAtFunc;

    llvm::Constant* setExprArgIsNullFunc;
    llvm::Constant* getExprArgIsNullFunc;
    llvm::Constant* getDatumParamFunc;
    llvm::Constant* readDatumColumnFunc;
    llvm::Constant* writeDatumColumnFunc;
    llvm::Constant* buildDatumKeyFunc;

    llvm::Constant* setBitFunc;
    llvm::Constant* resetBitmapSetFunc;
    llvm::Constant* getTableFieldCountFunc;
    llvm::Constant* writeRowFunc;
    llvm::Constant* searchRowFunc;
    llvm::Constant* createNewRowFunc;
    llvm::Constant* insertRowFunc;
    llvm::Constant* deleteRowFunc;
    llvm::Constant* setRowNullBitsFunc;
    llvm::Constant* setExprResultNullBitFunc;
    llvm::Constant* execClearTupleFunc;
    llvm::Constant* execStoreVirtualTupleFunc;
    llvm::Constant* selectColumnFunc;
    llvm::Constant* setTpProcessedFunc;
    llvm::Constant* setScanEndedFunc;

    llvm::Constant* copyKeyFunc;
    llvm::Constant* fillKeyPatternFunc;
    llvm::Constant* adjustKeyFunc;
    llvm::Constant* searchIteratorFunc;
    llvm::Constant* createEndIteratorFunc;
    llvm::Constant* isScanEndFunc;
    llvm::Constant* getRowFromIteratorFunc;
    llvm::Constant* destroyIteratorFunc;

    llvm::Constant* setStateIteratorFunc;
    llvm::Constant* getStateIteratorFunc;
    llvm::Constant* isStateIteratorNullFunc;
    llvm::Constant* isStateScanEndFunc;
    llvm::Constant* getRowFromStateIteratorFunc;
    llvm::Constant* destroyStateIteratorsFunc;
    llvm::Constant* setStateScanEndFlagFunc;
    llvm::Constant* getStateScanEndFlagFunc;

    llvm::Constant* resetStateRowFunc;
    llvm::Constant* setStateRowFunc;
    llvm::Constant* getStateRowFunc;
    llvm::Constant* copyOuterStateRowFunc;
    llvm::Constant* getOuterStateRowCopyFunc;
    llvm::Constant* isStateRowNullFunc;

    llvm::Constant* resetStateLimitCounterFunc;
    llvm::Constant* incrementStateLimitCounterFunc;
    llvm::Constant* getStateLimitCounterFunc;

    llvm::Constant* prepareAvgArrayFunc;
    llvm::Constant* loadAvgArrayFunc;
    llvm::Constant* saveAvgArrayFunc;
    llvm::Constant* computeAvgFromArrayFunc;

    llvm::Constant* resetAggValueFunc;
    llvm::Constant* getAggValueFunc;
    llvm::Constant* setAggValueFunc;

    llvm::Constant* resetAggMaxMinNullFunc;
    llvm::Constant* setAggMaxMinNotNullFunc;
    llvm::Constant* getAggMaxMinIsNullFunc;

    llvm::Constant* prepareDistinctSetFunc;
    llvm::Constant* insertDistinctItemFunc;
    llvm::Constant* destroyDistinctSetFunc;

    llvm::Constant* resetTupleDatumFunc;
    llvm::Constant* readTupleDatumFunc;
    llvm::Constant* writeTupleDatumFunc;

    // builtins
#define APPLY_UNARY_OPERATOR(funcid, name) llvm::Constant* _builtin_##name;
#define APPLY_BINARY_OPERATOR(funcid, name) llvm::Constant* _builtin_##name;
#define APPLY_TERNARY_OPERATOR(funcid, name) llvm::Constant* _builtin_##name;
#define APPLY_UNARY_CAST_OPERATOR(funcid, name) APPLY_UNARY_OPERATOR(funcid, name)
#define APPLY_BINARY_CAST_OPERATOR(funcid, name) APPLY_BINARY_OPERATOR(funcid, name)
#define APPLY_TERNARY_CAST_OPERATOR(funcid, name) APPLY_TERNARY_OPERATOR(funcid, name)

    APPLY_OPERATORS()

#undef APPLY_UNARY_OPERATOR
#undef APPLY_BINARY_OPERATOR
#undef APPLY_TERNARY_OPERATOR
#undef APPLY_UNARY_CAST_OPERATOR
#undef APPLY_BINARY_CAST_OPERATOR
#undef APPLY_TERNARY_CAST_OPERATOR

    // locals
    llvm::Value* rows_processed;

    // args
    llvm::Value* table_value;
    llvm::Value* index_value;
    llvm::Value* key_value;
    llvm::Value* end_iterator_key_value;
    llvm::Value* params_value;
    llvm::Value* slot_value;
    llvm::Value* tp_processed_value;
    llvm::Value* scan_ended_value;
    llvm::Value* bitmap_value;
    llvm::Value* inner_table_value;
    llvm::Value* inner_index_value;
    llvm::Value* inner_key_value;
    llvm::Value* inner_end_iterator_key_value;

    // compile context
    TableInfo _table_info;
    TableInfo _inner_table_info;

    GsCodeGen* _code_gen;
    GsCodeGen::LlvmBuilder* _builder;
    llvm::Function* m_jittedQuery;
};

/** @brief Gets a key from the execution context. */
static llvm::Value* getExecContextKey(
    JitLlvmCodeGenContext* ctx, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
{
    llvm::Value* key = nullptr;
    if (range_scan_type == JIT_RANGE_SCAN_INNER) {
        if (range_itr_type == JIT_RANGE_ITERATOR_END) {
            key = ctx->inner_end_iterator_key_value;
        } else {
            key = ctx->inner_key_value;
        }
    } else {
        if (range_itr_type == JIT_RANGE_ITERATOR_END) {
            key = ctx->end_iterator_key_value;
        } else {
            key = ctx->key_value;
        }
    }
    return key;
}

/*--------------------------- Define LLVM Helper Prototypes  ---------------------------*/

static llvm::Constant* defineFunction(llvm::Module* module, llvm::Type* ret_type, const char* name, ...)
{
    va_list vargs;
    va_start(vargs, name);
    std::vector<llvm::Type*> args;
    llvm::Type* arg_type = va_arg(vargs, llvm::Type*);
    while (arg_type != nullptr) {
        args.push_back(arg_type);
        arg_type = va_arg(vargs, llvm::Type*);
    }
    va_end(vargs);
    llvm::ArrayRef<llvm::Type*> argsRef(args);
    llvm::FunctionType* funcType = llvm::FunctionType::get(ret_type, argsRef, false);
    return module->getOrInsertFunction(name, funcType);
}

static void defineDebugLog(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->debugLogFunc = defineFunction(module, ctx->VOID_T, "debugLog", ctx->STR_T, ctx->STR_T, nullptr);
}

static void defineIsSoftMemoryLimitReached(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->isSoftMemoryLimitReachedFunc = defineFunction(module, ctx->INT32_T, "isSoftMemoryLimitReached", nullptr);
}

static void defineGetPrimaryIndex(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getPrimaryIndexFunc = defineFunction(
        module, ctx->IndexType->getPointerTo(), "getPrimaryIndex", ctx->TableType->getPointerTo(), nullptr);
}

static void defineGetTableIndex(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getTableIndexFunc = defineFunction(
        module, ctx->IndexType->getPointerTo(), "getTableIndex", ctx->TableType->getPointerTo(), ctx->INT32_T, nullptr);
}

static void defineInitKey(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->initKeyFunc = defineFunction(
        module, ctx->VOID_T, "InitKey", ctx->KeyType->getPointerTo(), ctx->IndexType->getPointerTo(), nullptr);
}

static void defineGetColumnAt(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getColumnAtFunc = defineFunction(
        module, ctx->ColumnType->getPointerTo(), "getColumnAt", ctx->TableType->getPointerTo(), ctx->INT32_T, nullptr);
}

static void defineSetExprArgIsNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setExprArgIsNullFunc =
        defineFunction(module, ctx->VOID_T, "setExprArgIsNull", ctx->INT32_T, ctx->INT32_T, nullptr);
}

static void defineGetExprArgIsNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getExprArgIsNullFunc = defineFunction(module, ctx->INT32_T, "getExprArgIsNull", ctx->INT32_T, nullptr);
}

static void defineGetDatumParam(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getDatumParamFunc = defineFunction(module,
        ctx->DATUM_T,
        "getDatumParam",
        ctx->ParamListInfoDataType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

static void defineReadDatumColumn(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->readDatumColumnFunc = defineFunction(module,
        ctx->DATUM_T,
        "readDatumColumn",
        ctx->TableType->getPointerTo(),
        ctx->RowType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

static void defineWriteDatumColumn(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->writeDatumColumnFunc = defineFunction(module,
        ctx->VOID_T,
        "writeDatumColumn",
        ctx->RowType->getPointerTo(),
        ctx->ColumnType->getPointerTo(),
        ctx->DATUM_T,
        nullptr);
}

static void defineBuildDatumKey(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->buildDatumKeyFunc = defineFunction(module,
        ctx->VOID_T,
        "buildDatumKey",
        ctx->ColumnType->getPointerTo(),
        ctx->KeyType->getPointerTo(),
        ctx->DATUM_T,
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

static void defineSetBit(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setBitFunc =
        defineFunction(module, ctx->VOID_T, "setBit", ctx->BitmapSetType->getPointerTo(), ctx->INT32_T, nullptr);
}

static void defineResetBitmapSet(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetBitmapSetFunc =
        defineFunction(module, ctx->VOID_T, "resetBitmapSet", ctx->BitmapSetType->getPointerTo(), nullptr);
}

static void defineGetTableFieldCount(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getTableFieldCountFunc =
        defineFunction(module, ctx->INT32_T, "getTableFieldCount", ctx->TableType->getPointerTo(), nullptr);
}

static void defineWriteRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->writeRowFunc = defineFunction(
        module, ctx->INT32_T, "writeRow", ctx->RowType->getPointerTo(), ctx->BitmapSetType->getPointerTo(), nullptr);
}

static void defineSearchRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->searchRowFunc = defineFunction(module,
        ctx->RowType->getPointerTo(),
        "searchRow",
        ctx->TableType->getPointerTo(),
        ctx->KeyType->getPointerTo(),
        ctx->INT32_T,
        nullptr);
}

static void defineCreateNewRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->createNewRowFunc =
        defineFunction(module, ctx->RowType->getPointerTo(), "createNewRow", ctx->TableType->getPointerTo(), nullptr);
}

static void defineInsertRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->insertRowFunc = defineFunction(
        module, ctx->INT32_T, "insertRow", ctx->TableType->getPointerTo(), ctx->RowType->getPointerTo(), nullptr);
}

static void defineDeleteRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->deleteRowFunc = defineFunction(module, ctx->INT32_T, "deleteRow", nullptr);
}

static void defineSetRowNullBits(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setRowNullBitsFunc = defineFunction(
        module, ctx->VOID_T, "setRowNullBits", ctx->TableType->getPointerTo(), ctx->RowType->getPointerTo(), nullptr);
}

static void defineSetExprResultNullBit(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setExprResultNullBitFunc = defineFunction(module,
        ctx->INT32_T,
        "setExprResultNullBit",
        ctx->TableType->getPointerTo(),
        ctx->RowType->getPointerTo(),
        ctx->INT32_T,
        nullptr);
}

static void defineExecClearTuple(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->execClearTupleFunc =
        defineFunction(module, ctx->VOID_T, "execClearTuple", ctx->TupleTableSlotType->getPointerTo(), nullptr);
}

static void defineExecStoreVirtualTuple(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->execStoreVirtualTupleFunc =
        defineFunction(module, ctx->VOID_T, "execStoreVirtualTuple", ctx->TupleTableSlotType->getPointerTo(), nullptr);
}

static void defineSelectColumn(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->selectColumnFunc = defineFunction(module,
        ctx->VOID_T,
        "selectColumn",
        ctx->TableType->getPointerTo(),
        ctx->RowType->getPointerTo(),
        ctx->TupleTableSlotType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

static void defineSetTpProcessed(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setTpProcessedFunc =
        defineFunction(module, ctx->VOID_T, "setTpProcessed", ctx->INT64_T->getPointerTo(), ctx->INT64_T, nullptr);
}

static void defineSetScanEnded(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setScanEndedFunc =
        defineFunction(module, ctx->VOID_T, "setScanEnded", ctx->INT32_T->getPointerTo(), ctx->INT32_T, nullptr);
}

static void defineCopyKey(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->copyKeyFunc = defineFunction(module,
        ctx->VOID_T,
        "copyKey",
        ctx->IndexType->getPointerTo(),
        ctx->KeyType->getPointerTo(),
        ctx->KeyType->getPointerTo(),
        nullptr);
}

static void defineFillKeyPattern(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->fillKeyPatternFunc = defineFunction(module,
        ctx->VOID_T,
        "FillKeyPattern",
        ctx->KeyType->getPointerTo(),
        ctx->INT8_T,
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

static void defineAdjustKey(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->adjustKeyFunc = defineFunction(module,
        ctx->VOID_T,
        "adjustKey",
        ctx->KeyType->getPointerTo(),
        ctx->IndexType->getPointerTo(),
        ctx->INT8_T,
        nullptr);
}

static void defineSearchIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->searchIteratorFunc = defineFunction(module,
        ctx->IndexIteratorType->getPointerTo(),
        "searchIterator",
        ctx->IndexType->getPointerTo(),
        ctx->KeyType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

static void defineCreateEndIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->createEndIteratorFunc = defineFunction(module,
        ctx->IndexIteratorType->getPointerTo(),
        "createEndIterator",
        ctx->IndexType->getPointerTo(),
        ctx->KeyType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

static void defineIsScanEnd(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->isScanEndFunc = defineFunction(module,
        ctx->INT32_T,
        "isScanEnd",
        ctx->IndexType->getPointerTo(),
        ctx->IndexIteratorType->getPointerTo(),
        ctx->IndexIteratorType->getPointerTo(),
        ctx->INT32_T,
        nullptr);
}

static void defineGetRowFromIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getRowFromIteratorFunc = defineFunction(module,
        ctx->RowType->getPointerTo(),
        "getRowFromIterator",
        ctx->IndexType->getPointerTo(),
        ctx->IndexIteratorType->getPointerTo(),
        ctx->IndexIteratorType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

static void defineDestroyIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->destroyIteratorFunc =
        defineFunction(module, ctx->VOID_T, "destroyIterator", ctx->IndexIteratorType->getPointerTo(), nullptr);
}

static void defineSetStateIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setStateIteratorFunc = defineFunction(module,
        ctx->VOID_T,
        "setStateIterator",
        ctx->IndexIteratorType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

static void defineGetStateIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getStateIteratorFunc = defineFunction(
        module, ctx->IndexIteratorType->getPointerTo(), "getStateIterator", ctx->INT32_T, ctx->INT32_T, nullptr);
}

static void defineIsStateIteratorNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->isStateIteratorNullFunc =
        defineFunction(module, ctx->INT32_T, "isStateIteratorNull", ctx->INT32_T, ctx->INT32_T, nullptr);
}

static void defineIsStateScanEnd(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->isStateScanEndFunc =
        defineFunction(module, ctx->INT32_T, "isStateScanEnd", ctx->INT32_T, ctx->INT32_T, nullptr);
}

static void defineGetRowFromStateIteratorFunc(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getRowFromStateIteratorFunc = defineFunction(module,
        ctx->RowType->getPointerTo(),
        "getRowFromStateIterator",
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

static void defineDestroyStateIterators(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->destroyStateIteratorsFunc =
        defineFunction(module, ctx->VOID_T, "destroyStateIterators", ctx->INT32_T, nullptr);
}

static void defineSetStateScanEndFlag(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setStateScanEndFlagFunc =
        defineFunction(module, ctx->VOID_T, "setStateScanEndFlag", ctx->INT32_T, ctx->INT32_T, nullptr);
}

static void defineGetStateScanEndFlag(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getStateScanEndFlagFunc = defineFunction(module, ctx->INT32_T, "getStateScanEndFlag", ctx->INT32_T, nullptr);
}

static void defineResetStateRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetStateRowFunc = defineFunction(module, ctx->VOID_T, "resetStateRow", ctx->INT32_T, nullptr);
}

static void defineSetStateRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setStateRowFunc =
        defineFunction(module, ctx->VOID_T, "setStateRow", ctx->RowType->getPointerTo(), ctx->INT32_T, nullptr);
}

static void defineGetStateRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getStateRowFunc = defineFunction(module, ctx->RowType->getPointerTo(), "getStateRow", ctx->INT32_T, nullptr);
}

static void defineCopyOuterStateRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->copyOuterStateRowFunc = defineFunction(module, ctx->VOID_T, "copyOuterStateRow", nullptr);
}

static void defineGetOuterStateRowCopy(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getOuterStateRowCopyFunc =
        defineFunction(module, ctx->RowType->getPointerTo(), "getOuterStateRowCopy", nullptr);
}

static void defineIsStateRowNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->isStateRowNullFunc = defineFunction(module, ctx->INT32_T, "isStateRowNull", ctx->INT32_T, nullptr);
}

static void defineResetStateLimitCounter(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetStateLimitCounterFunc = defineFunction(module, ctx->VOID_T, "resetStateLimitCounter", nullptr);
}

static void defineIncrementStateLimitCounter(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->incrementStateLimitCounterFunc = defineFunction(module, ctx->VOID_T, "incrementStateLimitCounter", nullptr);
}

static void defineGetStateLimitCounter(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getStateLimitCounterFunc = defineFunction(module, ctx->INT32_T, "getStateLimitCounter", nullptr);
}

static void definePrepareAvgArray(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->prepareAvgArrayFunc =
        defineFunction(module, ctx->VOID_T, "prepareAvgArray", ctx->INT32_T, ctx->INT32_T, nullptr);
}

static void defineLoadAvgArray(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->loadAvgArrayFunc = defineFunction(module, ctx->DATUM_T, "loadAvgArray", nullptr);
}

static void defineSaveAvgArray(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->saveAvgArrayFunc = defineFunction(module, ctx->VOID_T, "saveAvgArray", ctx->DATUM_T, nullptr);
}

static void defineComputeAvgFromArray(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->computeAvgFromArrayFunc = defineFunction(module, ctx->DATUM_T, "computeAvgFromArray", ctx->INT32_T, nullptr);
}

static void defineResetAggValue(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetAggValueFunc = defineFunction(module, ctx->VOID_T, "resetAggValue", ctx->INT32_T, nullptr);
}

static void defineGetAggValue(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getAggValueFunc = defineFunction(module, ctx->DATUM_T, "getAggValue", nullptr);
}

static void defineSetAggValue(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setAggValueFunc = defineFunction(module, ctx->VOID_T, "setAggValue", ctx->DATUM_T, nullptr);
}

static void defineResetAggMaxMinNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetAggMaxMinNullFunc = defineFunction(module, ctx->VOID_T, "resetAggMaxMinNull", nullptr);
}

static void defineSetAggMaxMinNotNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setAggMaxMinNotNullFunc = defineFunction(module, ctx->VOID_T, "setAggMaxMinNotNull", nullptr);
}

static void defineGetAggMaxMinIsNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getAggMaxMinIsNullFunc = defineFunction(module, ctx->INT32_T, "getAggMaxMinIsNull", nullptr);
}

static void definePrepareDistinctSet(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->prepareDistinctSetFunc = defineFunction(module, ctx->VOID_T, "prepareDistinctSet", ctx->INT32_T, nullptr);
}

static void defineInsertDistinctItem(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->insertDistinctItemFunc =
        defineFunction(module, ctx->INT32_T, "insertDistinctItem", ctx->INT32_T, ctx->DATUM_T, nullptr);
}

static void defineDestroyDistinctSet(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->destroyDistinctSetFunc = defineFunction(module, ctx->VOID_T, "destroyDistinctSet", ctx->INT32_T, nullptr);
}

static void defineResetTupleDatum(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetTupleDatumFunc = defineFunction(module,
        ctx->VOID_T,
        "resetTupleDatum",
        ctx->TupleTableSlotType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

static void defineReadTupleDatum(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->readTupleDatumFunc = defineFunction(module,
        ctx->DATUM_T,
        "readTupleDatum",
        ctx->TupleTableSlotType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

static void defineWriteTupleDatum(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->writeTupleDatumFunc = defineFunction(module,
        ctx->VOID_T,
        "writeTupleDatum",
        ctx->TupleTableSlotType->getPointerTo(),
        ctx->INT32_T,
        ctx->DATUM_T,
        nullptr);
}

/*--------------------------- End of LLVM Helper Prototypes ---------------------------*/
/** @brief Define all LLVM prototypes. */
static void InitCodeGenContextFuncs(JitLlvmCodeGenContext* ctx)
{
    llvm::Module* module = ctx->_code_gen->module();

    // define all function calls
    defineDebugLog(ctx, module);
    defineIsSoftMemoryLimitReached(ctx, module);
    defineGetPrimaryIndex(ctx, module);
    defineGetTableIndex(ctx, module);
    defineInitKey(ctx, module);
    defineGetColumnAt(ctx, module);
    defineSetExprArgIsNull(ctx, module);
    defineGetExprArgIsNull(ctx, module);
    defineGetDatumParam(ctx, module);
    defineReadDatumColumn(ctx, module);
    defineWriteDatumColumn(ctx, module);
    defineBuildDatumKey(ctx, module);

    defineSetBit(ctx, module);
    defineResetBitmapSet(ctx, module);
    defineGetTableFieldCount(ctx, module);
    defineWriteRow(ctx, module);
    defineSearchRow(ctx, module);
    defineCreateNewRow(ctx, module);
    defineInsertRow(ctx, module);
    defineDeleteRow(ctx, module);
    defineSetRowNullBits(ctx, module);
    defineSetExprResultNullBit(ctx, module);
    defineExecClearTuple(ctx, module);
    defineExecStoreVirtualTuple(ctx, module);
    defineSelectColumn(ctx, module);
    defineSetTpProcessed(ctx, module);
    defineSetScanEnded(ctx, module);

    defineCopyKey(ctx, module);
    defineFillKeyPattern(ctx, module);
    defineAdjustKey(ctx, module);
    defineSearchIterator(ctx, module);
    defineCreateEndIterator(ctx, module);
    defineIsScanEnd(ctx, module);
    defineGetRowFromIterator(ctx, module);
    defineDestroyIterator(ctx, module);

    defineSetStateIterator(ctx, module);
    defineGetStateIterator(ctx, module);
    defineIsStateIteratorNull(ctx, module);
    defineIsStateScanEnd(ctx, module);
    defineGetRowFromStateIteratorFunc(ctx, module);
    defineDestroyStateIterators(ctx, module);
    defineSetStateScanEndFlag(ctx, module);
    defineGetStateScanEndFlag(ctx, module);

    defineResetStateRow(ctx, module);
    defineSetStateRow(ctx, module);
    defineGetStateRow(ctx, module);
    defineCopyOuterStateRow(ctx, module);
    defineGetOuterStateRowCopy(ctx, module);
    defineIsStateRowNull(ctx, module);

    defineResetStateLimitCounter(ctx, module);
    defineIncrementStateLimitCounter(ctx, module);
    defineGetStateLimitCounter(ctx, module);

    definePrepareAvgArray(ctx, module);
    defineLoadAvgArray(ctx, module);
    defineSaveAvgArray(ctx, module);
    defineComputeAvgFromArray(ctx, module);

    defineResetAggValue(ctx, module);
    defineGetAggValue(ctx, module);
    defineSetAggValue(ctx, module);

    defineResetAggMaxMinNull(ctx, module);
    defineSetAggMaxMinNotNull(ctx, module);
    defineGetAggMaxMinIsNull(ctx, module);

    definePrepareDistinctSet(ctx, module);
    defineInsertDistinctItem(ctx, module);
    defineDestroyDistinctSet(ctx, module);

    defineResetTupleDatum(ctx, module);
    defineReadTupleDatum(ctx, module);
    defineWriteTupleDatum(ctx, module);
}

#define APPLY_UNARY_OPERATOR(funcid, name)                                                              \
    static void define_builtin_##name(JitLlvmCodeGenContext* ctx, llvm::Module* module)                 \
    {                                                                                                   \
        ctx->_builtin_##name =                                                                          \
            defineFunction(module, ctx->DATUM_T, "invoke_" #name, ctx->DATUM_T, ctx->INT32_T, nullptr); \
    }

#define APPLY_BINARY_OPERATOR(funcid, name)                                                                           \
    static void define_builtin_##name(JitLlvmCodeGenContext* ctx, llvm::Module* module)                               \
    {                                                                                                                 \
        ctx->_builtin_##name =                                                                                        \
            defineFunction(module, ctx->DATUM_T, "invoke_" #name, ctx->DATUM_T, ctx->DATUM_T, ctx->INT32_T, nullptr); \
    }

#define APPLY_TERNARY_OPERATOR(funcid, name)                                                                         \
    static void define_builtin_##name(JitLlvmCodeGenContext* ctx, llvm::Module* module)                              \
    {                                                                                                                \
        ctx->_builtin_##name = defineFunction(                                                                       \
            module, ctx->DATUM_T, "invoke_" #name, ctx->DATUM_T, ctx->DATUM_T, ctx->DATUM_T, ctx->INT32_T, nullptr); \
    }

#define APPLY_UNARY_CAST_OPERATOR(funcid, name) APPLY_UNARY_OPERATOR(funcid, name)
#define APPLY_BINARY_CAST_OPERATOR(funcid, name) APPLY_BINARY_OPERATOR(funcid, name)
#define APPLY_TERNARY_CAST_OPERATOR(funcid, name) APPLY_TERNARY_OPERATOR(funcid, name)

APPLY_OPERATORS()

#undef APPLY_UNARY_OPERATOR
#undef APPLY_BINARY_OPERATOR
#undef APPLY_TERNARY_OPERATOR
#undef APPLY_UNARY_CAST_OPERATOR
#undef APPLY_BINARY_CAST_OPERATOR
#undef APPLY_TERNARY_CAST_OPERATOR

/** @brief Define all operator invocation prototypes. */
static void InitCodeGenContextBuiltins(JitLlvmCodeGenContext* ctx)
{
    llvm::Module* module = ctx->_code_gen->module();

#define APPLY_UNARY_OPERATOR(funcid, name) define_builtin_##name(ctx, module);
#define APPLY_BINARY_OPERATOR(funcid, name) define_builtin_##name(ctx, module);
#define APPLY_TERNARY_OPERATOR(funcid, name) define_builtin_##name(ctx, module);
#define APPLY_UNARY_CAST_OPERATOR(funcid, name) APPLY_UNARY_OPERATOR(funcid, name)
#define APPLY_BINARY_CAST_OPERATOR(funcid, name) APPLY_BINARY_OPERATOR(funcid, name)
#define APPLY_TERNARY_CAST_OPERATOR(funcid, name) APPLY_TERNARY_OPERATOR(funcid, name)

    APPLY_OPERATORS()

#undef APPLY_UNARY_OPERATOR
#undef APPLY_BINARY_OPERATOR
#undef APPLY_TERNARY_OPERATOR
#undef APPLY_UNARY_CAST_OPERATOR
#undef APPLY_BINARY_CAST_OPERATOR
#undef APPLY_TERNARY_CAST_OPERATOR
}

/** @brief Define all LLVM used types synonyms. */
static void InitCodeGenContextTypes(JitLlvmCodeGenContext* ctx)
{
    llvm::LLVMContext& context = ctx->_code_gen->context();

    // primitive types
    ctx->INT8_T = llvm::Type::getInt8Ty(context);
    ctx->INT16_T = llvm::Type::getInt16Ty(context);
    ctx->INT32_T = llvm::Type::getInt32Ty(context);
    ctx->INT64_T = llvm::Type::getInt64Ty(context);
    ctx->VOID_T = llvm::Type::getVoidTy(context);
    ctx->STR_T = llvm::Type::getInt8Ty(context)->getPointerTo();
    ctx->FLOAT_T = llvm::Type::getFloatTy(context);
    ctx->DOUBLE_T = llvm::Type::getDoubleTy(context);
    ctx->DATUM_T = ctx->INT64_T;
    ctx->BOOL_T = ctx->INT8_T;

    // PG types
    ctx->ParamListInfoDataType = llvm::StructType::create(context, "ParamListInfoData");
    ctx->TupleTableSlotType = llvm::StructType::create(context, "TupleTableSlot");
    ctx->NumericDataType = llvm::StructType::create(context, "NumericData");
    ctx->VarCharType = llvm::StructType::create(context, "VarChar");
    ctx->BpCharType = llvm::StructType::create(context, "BpChar");

    // MOT types
    ctx->MMEngineType = llvm::StructType::create(context, "MMEngine");
    ctx->TableType = llvm::StructType::create(context, "Table");
    ctx->IndexType = llvm::StructType::create(context, "Index");
    ctx->KeyType = llvm::StructType::create(context, "Key");
    ctx->ColumnType = llvm::StructType::create(context, "Column");
    ctx->RowType = llvm::StructType::create(context, "Row");
    ctx->BitmapSetType = llvm::StructType::create(context, "BitmapSet");
    ctx->IndexIteratorType = llvm::StructType::create(context, "IndexIterator");
}

/** @brief Initializes a code generation context. */
static bool InitCodeGenContext(JitLlvmCodeGenContext* ctx, GsCodeGen* code_gen, GsCodeGen::LlvmBuilder* builder,
    MOT::Table* table, MOT::Index* index, MOT::Table* inner_table = nullptr, MOT::Index* inner_index = nullptr)
{
    // make sure all members are nullptr for proper cleanup in case of failure
    errno_t erc = memset_s(ctx, sizeof(JitLlvmCodeGenContext), 0, sizeof(JitLlvmCodeGenContext));
    securec_check(erc, "\0", "\0");

    ctx->_code_gen = code_gen;
    ctx->_builder = builder;
    if (!InitTableInfo(&ctx->_table_info, table, index)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compile", "Failed to initialize table information for code-generation context");
        return false;
    }
    if (inner_table && !InitTableInfo(&ctx->_inner_table_info, inner_table, inner_index)) {
        DestroyTableInfo(&ctx->_table_info);
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Compile",
            "Failed to initialize inner-scan table information for code-generation context");
        return false;
    }

    InitCodeGenContextTypes(ctx);
    InitCodeGenContextFuncs(ctx);
    InitCodeGenContextBuiltins(ctx);

    return true;
}

/** @brief Destroys a code generation context. */
static void DestroyCodeGenContext(JitLlvmCodeGenContext* ctx)
{
    DestroyTableInfo(&ctx->_table_info);
    DestroyTableInfo(&ctx->_inner_table_info);
    if (ctx->_code_gen != nullptr) {
        ctx->_code_gen->releaseResource();
        delete ctx->_code_gen;
    }
}

/*--------------------------- Helpers to generate calls to Helper function via LLVM ---------------------------*/
static llvm::Value* AddFunctionCall(JitLlvmCodeGenContext* ctx, llvm::Constant* func, ...)
{
    va_list vargs;
    va_start(vargs, func);
    std::vector<llvm::Value*> args;
    llvm::Value* arg_value = va_arg(vargs, llvm::Value*);
    while (arg_value != nullptr) {
        args.push_back(arg_value);
        arg_value = va_arg(vargs, llvm::Value*);
    }
    va_end(vargs);
    llvm::ArrayRef<llvm::Value*> argsRef(args);
    return ctx->_builder->CreateCall(func, argsRef);
}

/** @brief Adds a call to isSoftMemoryLimitReached(). */
static llvm::Value* AddIsSoftMemoryLimitReached(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->isSoftMemoryLimitReachedFunc, nullptr);
}

/** @brief Adds a call to InitKey(). */
static void AddInitKey(JitLlvmCodeGenContext* ctx, llvm::Value* key, llvm::Value* index)
{
    AddFunctionCall(ctx, ctx->initKeyFunc, key, index, nullptr);
}

/** @brief Adds a call to initSearchKey(key, index). */
static void AddInitSearchKey(JitLlvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    if (range_scan_type == JIT_RANGE_SCAN_INNER) {
        AddInitKey(ctx, ctx->inner_key_value, ctx->inner_index_value);
    } else {
        AddInitKey(ctx, ctx->key_value, ctx->index_value);
    }
}

/** @brief Adds a call to getColumnAt(table, colid). */
static llvm::Value* AddGetColumnAt(JitLlvmCodeGenContext* ctx, int colid, JitRangeScanType range_scan_type)
{
    llvm::ConstantInt* colid_value = llvm::ConstantInt::get(ctx->INT32_T, colid, true);
    llvm::Value* table = (range_scan_type == JIT_RANGE_SCAN_INNER) ? ctx->inner_table_value : ctx->table_value;
    return AddFunctionCall(ctx, ctx->getColumnAtFunc, table, colid_value, nullptr);
}

/** @brief Adds a call to setExprArgIsNull(arg_pos, isnull). */
static void AddSetExprArgIsNull(JitLlvmCodeGenContext* ctx, int arg_pos, int isnull)
{
    llvm::ConstantInt* arg_pos_value = llvm::ConstantInt::get(ctx->INT32_T, arg_pos, true);
    llvm::ConstantInt* isnull_value = llvm::ConstantInt::get(ctx->INT32_T, isnull, true);
    AddFunctionCall(ctx, ctx->setExprArgIsNullFunc, arg_pos_value, isnull_value, nullptr);
}

/** @brief Adds a call to getExprArgIsNull(arg_pos). */
static llvm::Value* AddGetExprArgIsNull(JitLlvmCodeGenContext* ctx, int arg_pos)
{
    llvm::ConstantInt* arg_pos_value = llvm::ConstantInt::get(ctx->INT32_T, arg_pos, true);
    return AddFunctionCall(ctx, ctx->getExprArgIsNullFunc, arg_pos_value, nullptr);
}

/** @brief Adds a call to getDatumParam(paramid, arg_pos). */
static llvm::Value* AddGetDatumParam(JitLlvmCodeGenContext* ctx, int paramid, int arg_pos)
{
    llvm::ConstantInt* paramid_value = llvm::ConstantInt::get(ctx->INT32_T, paramid, true);
    llvm::ConstantInt* arg_pos_value = llvm::ConstantInt::get(ctx->INT32_T, arg_pos, true);
    return AddFunctionCall(ctx, ctx->getDatumParamFunc, ctx->params_value, paramid_value, arg_pos_value, nullptr);
}

/** @brief Adds a call to readDatumColumn(table_colid, arg_pos). */
static llvm::Value* AddReadDatumColumn(
    JitLlvmCodeGenContext* ctx, llvm::Value* table, llvm::Value* row, int table_colid, int arg_pos)
{
    llvm::ConstantInt* table_colid_value = llvm::ConstantInt::get(ctx->INT32_T, table_colid, true);
    llvm::ConstantInt* arg_pos_value = llvm::ConstantInt::get(ctx->INT32_T, arg_pos, true);
    return AddFunctionCall(
        ctx, ctx->readDatumColumnFunc, ctx->table_value, row, table_colid_value, arg_pos_value, nullptr);
}

/** @brief Adds a call to writeDatumColumn(table_colid, value). */
static llvm::Value* AddWriteDatumColumn(
    JitLlvmCodeGenContext* ctx, int table_colid, llvm::Value* row, llvm::Value* value)
{
    // make sure we have a column before issuing the call
    // we always write to a main table row (whether UPDATE or range UPDATE, so inner_scan value below is false)
    llvm::Value* column = AddGetColumnAt(ctx, table_colid, JIT_RANGE_SCAN_MAIN);
    return AddFunctionCall(ctx, ctx->writeDatumColumnFunc, row, column, value, nullptr);
}

/** @brief Adds a call to buildDatumKey(column, key, value, index_colid, offset, value). */
static void AddBuildDatumKey(JitLlvmCodeGenContext* ctx, llvm::Value* column, int index_colid, llvm::Value* value,
    int value_type, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
{
    int offset = -1;
    int size = -1;
    int inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    if (inner_scan) {
        offset = ctx->_inner_table_info.m_indexColumnOffsets[index_colid];
        size = ctx->_inner_table_info.m_index->GetLengthKeyFields()[index_colid];
    } else {
        offset = ctx->_table_info.m_indexColumnOffsets[index_colid];
        size = ctx->_table_info.m_index->GetLengthKeyFields()[index_colid];
    }
    llvm::ConstantInt* colid_value = llvm::ConstantInt::get(ctx->INT32_T, index_colid, true);
    llvm::ConstantInt* offset_value = llvm::ConstantInt::get(ctx->INT32_T, offset, true);
    llvm::ConstantInt* value_type_value = llvm::ConstantInt::get(ctx->INT32_T, value_type, true);
    llvm::ConstantInt* size_value = llvm::ConstantInt::get(ctx->INT32_T, size, true);
    llvm::Value* key_value = getExecContextKey(ctx, range_itr_type, range_scan_type);
    AddFunctionCall(ctx,
        ctx->buildDatumKeyFunc,
        column,
        key_value,
        value,
        colid_value,
        offset_value,
        size_value,
        value_type_value,
        nullptr);
}

/** @brief Adds a call to setBit(bitmap, colid). */
static void AddSetBit(JitLlvmCodeGenContext* ctx, int colid)
{
    llvm::ConstantInt* bit_index = llvm::ConstantInt::get(ctx->INT32_T, colid, true);
    AddFunctionCall(ctx, ctx->setBitFunc, ctx->bitmap_value, bit_index, nullptr);
}

/** @brief Adds a call to resetBitmapSet(bitmap). */
static void AddResetBitmapSet(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->resetBitmapSetFunc, ctx->bitmap_value, nullptr);
}

/** @brief Adds a call to writeRow(row, bitmap). */
static llvm::Value* AddWriteRow(JitLlvmCodeGenContext* ctx, llvm::Value* row)
{
    return AddFunctionCall(ctx, ctx->writeRowFunc, row, ctx->bitmap_value, nullptr);
}

/** @brief Adds a call to searchRow(table, key, access_mode). */
static llvm::Value* AddSearchRow(
    JitLlvmCodeGenContext* ctx, MOT::AccessType access_mode, JitRangeScanType range_scan_type)
{
    llvm::Value* row = nullptr;
    llvm::ConstantInt* access_mode_value = llvm::ConstantInt::get(ctx->INT32_T, access_mode, true);
    if (range_scan_type == JIT_RANGE_SCAN_INNER) {
        row = AddFunctionCall(
            ctx, ctx->searchRowFunc, ctx->inner_table_value, ctx->inner_key_value, access_mode_value, nullptr);
    } else {
        row = AddFunctionCall(ctx, ctx->searchRowFunc, ctx->table_value, ctx->key_value, access_mode_value, nullptr);
    }
    return row;
}

/** @brief Adds a call to createNewRow(table). */
static llvm::Value* AddCreateNewRow(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->createNewRowFunc, ctx->table_value, nullptr);
}

static llvm::Value* AddInsertRow(JitLlvmCodeGenContext* ctx, llvm::Value* row)
{
    return AddFunctionCall(ctx, ctx->insertRowFunc, ctx->table_value, row, nullptr);
}

static llvm::Value* AddDeleteRow(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->deleteRowFunc, nullptr);
}

/** @brief Adds a call to setRowNullBits(table, row). */
static void AddSetRowNullBits(JitLlvmCodeGenContext* ctx, llvm::Value* row)
{
    AddFunctionCall(ctx, ctx->setRowNullBitsFunc, ctx->table_value, row, nullptr);
}

/** @brief Adds a call to setExprResultNullBit(table, row, colid). */
static llvm::Value* AddSetExprResultNullBit(JitLlvmCodeGenContext* ctx, llvm::Value* row, int colid)
{
    llvm::ConstantInt* colid_value = llvm::ConstantInt::get(ctx->INT32_T, colid, true);
    return AddFunctionCall(ctx, ctx->setExprResultNullBitFunc, ctx->table_value, row, colid_value, nullptr);
}

/** @brief Adds a call to execClearTuple(slot). */
static void AddExecClearTuple(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->execClearTupleFunc, ctx->slot_value, nullptr);
}

/** @brief Adds a call to execStoreVirtualTuple(slot). */
static void AddExecStoreVirtualTuple(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->execStoreVirtualTupleFunc, ctx->slot_value, nullptr);
}

/** @brief Adds a call to selectColumn(table, row, slot, colid, column_count). */
static bool AddSelectColumn(
    JitLlvmCodeGenContext* ctx, llvm::Value* row, int table_colid, int tuple_colid, JitRangeScanType range_scan_type)
{
    llvm::ConstantInt* table_colid_value = llvm::ConstantInt::get(ctx->INT32_T, table_colid, true);
    llvm::ConstantInt* tuple_colid_value = llvm::ConstantInt::get(ctx->INT32_T, tuple_colid, true);
    llvm::Value* value = nullptr;
    if (range_scan_type == JIT_RANGE_SCAN_INNER) {
        value = AddFunctionCall(ctx,
            ctx->selectColumnFunc,
            ctx->inner_table_value,
            row,
            ctx->slot_value,
            table_colid_value,
            tuple_colid_value,
            nullptr);
    } else {
        value = AddFunctionCall(ctx,
            ctx->selectColumnFunc,
            ctx->table_value,
            row,
            ctx->slot_value,
            table_colid_value,
            tuple_colid_value,
            nullptr);
    }

    if (value == nullptr) {
        return false;
    }

    return true;
}

/** @brief Adds a call to setTpProcessed(tp_processed, rows_processed). */
static void AddSetTpProcessed(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->setTpProcessedFunc, ctx->tp_processed_value, ctx->rows_processed, nullptr);
}

/** @brief Adds a call to setScanEnded(scan_ended, result). */
static void AddSetScanEnded(JitLlvmCodeGenContext* ctx, int result)
{
    llvm::ConstantInt* result_value = llvm::ConstantInt::get(ctx->INT32_T, result, true);
    AddFunctionCall(ctx, ctx->setScanEndedFunc, ctx->scan_ended_value, result_value, nullptr);
}

/** @brief Adds a call to copyKey(index, key, end_iterator_key). */
static void AddCopyKey(JitLlvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    if (range_scan_type == JIT_RANGE_SCAN_INNER) {
        AddFunctionCall(ctx,
            ctx->copyKeyFunc,
            ctx->inner_index_value,
            ctx->inner_key_value,
            ctx->inner_end_iterator_key_value,
            nullptr);
    } else {
        AddFunctionCall(ctx, ctx->copyKeyFunc, ctx->index_value, ctx->key_value, ctx->end_iterator_key_value, nullptr);
    }
}

/** @brief Adds a call to FillKeyPattern(key, pattern, offset, size) or FillKeyPattern(end_iterator_key, pattern,
 * offset, size). */
static void AddFillKeyPattern(JitLlvmCodeGenContext* ctx, unsigned char pattern, int offset, int size,
    JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
{
    llvm::ConstantInt* pattern_value = llvm::ConstantInt::get(ctx->INT8_T, pattern, true);
    llvm::ConstantInt* offset_value = llvm::ConstantInt::get(ctx->INT32_T, offset, true);
    llvm::ConstantInt* size_value = llvm::ConstantInt::get(ctx->INT32_T, size, true);
    llvm::Value* key_value = getExecContextKey(ctx, range_itr_type, range_scan_type);
    AddFunctionCall(ctx, ctx->fillKeyPatternFunc, key_value, pattern_value, offset_value, size_value, nullptr);
}

/** @brief Adds a call to adjustKey(key, index, pattern) or adjustKey(end_iterator_key, index, pattern). */
static void AddAdjustKey(JitLlvmCodeGenContext* ctx, unsigned char pattern, JitRangeIteratorType range_itr_type,
    JitRangeScanType range_scan_type)
{
    llvm::ConstantInt* pattern_value = llvm::ConstantInt::get(ctx->INT8_T, pattern, true);
    llvm::Value* key_value = getExecContextKey(ctx, range_itr_type, range_scan_type);
    llvm::Value* index_value = (range_scan_type == JIT_RANGE_SCAN_INNER) ? ctx->inner_index_value : ctx->index_value;
    AddFunctionCall(ctx, ctx->adjustKeyFunc, key_value, index_value, pattern_value, nullptr);
}

/** @brief Adds a call to searchIterator(index, key). */
static llvm::Value* AddSearchIterator(JitLlvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitRangeBoundMode range_bound_mode, JitRangeScanType range_scan_type)
{
    llvm::Value* itr = nullptr;
    uint64_t forward_scan = (index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    uint64_t include_bound = (range_bound_mode == JIT_RANGE_BOUND_INCLUDE) ? 1 : 0;
    llvm::ConstantInt* forward_iterator_value = llvm::ConstantInt::get(ctx->INT32_T, forward_scan, true);
    llvm::ConstantInt* include_bound_value = llvm::ConstantInt::get(ctx->INT32_T, include_bound, true);
    if (range_scan_type == JIT_RANGE_SCAN_INNER) {
        itr = AddFunctionCall(ctx,
            ctx->searchIteratorFunc,
            ctx->inner_index_value,
            ctx->inner_key_value,
            forward_iterator_value,
            include_bound_value,
            nullptr);
    } else {
        itr = AddFunctionCall(ctx,
            ctx->searchIteratorFunc,
            ctx->index_value,
            ctx->key_value,
            forward_iterator_value,
            include_bound_value,
            nullptr);
    }
    return itr;
}

/** @brief Adds a call to createEndIterator(index, end_iterator_key). */
static llvm::Value* AddCreateEndIterator(JitLlvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitRangeBoundMode range_bound_mode, JitRangeScanType range_scan_type)
{
    llvm::Value* itr = nullptr;
    uint64_t forward_scan = (index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    uint64_t include_bound = (range_bound_mode == JIT_RANGE_BOUND_INCLUDE) ? 1 : 0;
    llvm::ConstantInt* forward_scan_value = llvm::ConstantInt::get(ctx->INT32_T, forward_scan, true);
    llvm::ConstantInt* include_bound_value = llvm::ConstantInt::get(ctx->INT32_T, include_bound, true);
    if (range_scan_type == JIT_RANGE_SCAN_INNER) {
        itr = AddFunctionCall(ctx,
            ctx->createEndIteratorFunc,
            ctx->inner_index_value,
            ctx->inner_end_iterator_key_value,
            forward_scan_value,
            include_bound_value,
            nullptr);
    } else {
        itr = AddFunctionCall(ctx,
            ctx->createEndIteratorFunc,
            ctx->index_value,
            ctx->end_iterator_key_value,
            forward_scan_value,
            include_bound_value,
            nullptr);
    }
    return itr;
}

/** @brief Adds a call to isScanEnd(index, iterator, end_iterator). */
static llvm::Value* AddIsScanEnd(JitLlvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitLlvmRuntimeCursor* cursor, JitRangeScanType range_scan_type)
{
    uint64_t forward_scan = (index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    llvm::ConstantInt* forward_scan_value = llvm::ConstantInt::get(ctx->INT32_T, forward_scan, true);
    llvm::Value* index_value = (range_scan_type == JIT_RANGE_SCAN_INNER) ? ctx->inner_index_value : ctx->index_value;
    return AddFunctionCall(
        ctx, ctx->isScanEndFunc, index_value, cursor->begin_itr, cursor->end_itr, forward_scan_value, nullptr);
}

/** @brief Adds a cal to getRowFromIterator(index, iterator, end_iterator). */
static llvm::Value* AddGetRowFromIterator(JitLlvmCodeGenContext* ctx, MOT::AccessType access_mode,
    JitIndexScanDirection index_scan_direction, JitLlvmRuntimeCursor* cursor, JitRangeScanType range_scan_type)
{
    uint64_t forward_scan = (index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    llvm::ConstantInt* access_mode_value = llvm::ConstantInt::get(ctx->INT32_T, access_mode, true);
    llvm::ConstantInt* forward_scan_value = llvm::ConstantInt::get(ctx->INT32_T, forward_scan, true);
    llvm::Value* index_value = (range_scan_type == JIT_RANGE_SCAN_INNER) ? ctx->inner_index_value : ctx->index_value;
    return AddFunctionCall(ctx,
        ctx->getRowFromIteratorFunc,
        index_value,
        cursor->begin_itr,
        cursor->end_itr,
        access_mode_value,
        forward_scan_value,
        nullptr);
}

/** @brief Adds a call to destroyIterator(iterator). */
static void AddDestroyIterator(JitLlvmCodeGenContext* ctx, llvm::Value* itr)
{
    AddFunctionCall(ctx, ctx->destroyIteratorFunc, itr, nullptr);
}

static void AddDestroyCursor(JitLlvmCodeGenContext* ctx, JitLlvmRuntimeCursor* cursor)
{
    AddDestroyIterator(ctx, cursor->begin_itr);
    AddDestroyIterator(ctx, cursor->end_itr);
}

/** @brief Adds a call to setStateIterator(itr, begin_itr). */
static void AddSetStateIterator(
    JitLlvmCodeGenContext* ctx, llvm::Value* itr, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
{
    uint64_t begin_itr = (range_itr_type == JIT_RANGE_ITERATOR_START) ? 1 : 0;
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* begin_itr_value = llvm::ConstantInt::get(ctx->INT32_T, begin_itr, true);
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    AddFunctionCall(ctx, ctx->setStateIteratorFunc, itr, begin_itr_value, inner_scan_value, nullptr);
}

/** @brief Adds a call to isStateIteratorNull(begin_itr). */
static llvm::Value* AddIsStateIteratorNull(
    JitLlvmCodeGenContext* ctx, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
{
    uint64_t begin_itr = (range_itr_type == JIT_RANGE_ITERATOR_START) ? 1 : 0;
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* begin_itr_value = llvm::ConstantInt::get(ctx->INT32_T, begin_itr, true);
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    return AddFunctionCall(ctx, ctx->isStateIteratorNullFunc, begin_itr_value, inner_scan_value, nullptr);
}

/** @brief Adds a call to isStateScanEnd(index, forward_scan). */
static llvm::Value* AddIsStateScanEnd(
    JitLlvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction, JitRangeScanType range_scan_type)
{
    uint64_t forward_scan = (index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* forward_scan_value = llvm::ConstantInt::get(ctx->INT32_T, forward_scan, true);
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    return AddFunctionCall(ctx, ctx->isStateScanEndFunc, forward_scan_value, inner_scan_value, nullptr);
}

/** @brief Adds a cal to getRowFromStateIterator(index, access_mode, forward_scan). */
static llvm::Value* AddGetRowFromStateIterator(JitLlvmCodeGenContext* ctx, MOT::AccessType access_mode,
    JitIndexScanDirection index_scan_direction, JitRangeScanType range_scan_type)
{
    uint64_t forward_scan = (index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* access_mode_value = llvm::ConstantInt::get(ctx->INT32_T, access_mode, true);
    llvm::ConstantInt* forward_scan_value = llvm::ConstantInt::get(ctx->INT32_T, forward_scan, true);
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    return AddFunctionCall(
        ctx, ctx->getRowFromStateIteratorFunc, access_mode_value, forward_scan_value, inner_scan_value, nullptr);
}

/** @brief Adds a call to destroyStateIterators(). */
static void AddDestroyStateIterators(JitLlvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    AddFunctionCall(ctx, ctx->destroyStateIteratorsFunc, inner_scan_value, nullptr);
}

/** @brief Adds a call to setStateScanEndFlag(scan_ended). */
static void AddSetStateScanEndFlag(JitLlvmCodeGenContext* ctx, int scan_ended, JitRangeScanType range_scan_type)
{
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* scan_ended_value = llvm::ConstantInt::get(ctx->INT32_T, scan_ended, true);
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    AddFunctionCall(ctx, ctx->setStateScanEndFlagFunc, scan_ended_value, inner_scan_value, nullptr);
}

/** @brief Adds a call to getStateScanEndFlag(). */
static llvm::Value* AddGetStateScanEndFlag(JitLlvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    return AddFunctionCall(ctx, ctx->getStateScanEndFlagFunc, inner_scan_value, nullptr);
}

static void AddResetStateRow(JitLlvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    AddFunctionCall(ctx, ctx->resetStateRowFunc, inner_scan_value, nullptr);
}

static void AddSetStateRow(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitRangeScanType range_scan_type)
{
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    AddFunctionCall(ctx, ctx->setStateRowFunc, row, inner_scan_value, nullptr);
}

static llvm::Value* AddGetStateRow(JitLlvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    return AddFunctionCall(ctx, ctx->getStateRowFunc, inner_scan_value, nullptr);
}

static void AddCopyOuterStateRow(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->copyOuterStateRowFunc, nullptr);
}

static llvm::Value* AddGetOuterStateRowCopy(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->getOuterStateRowCopyFunc, nullptr);
}

static llvm::Value* AddIsStateRowNull(JitLlvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    return AddFunctionCall(ctx, ctx->isStateRowNullFunc, inner_scan_value, nullptr);
}

static void AddResetStateLimitCounter(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->resetStateLimitCounterFunc, nullptr);
}

static void AddIncrementStateLimitCounter(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->incrementStateLimitCounterFunc, nullptr);
}

static llvm::Value* AddGetStateLimitCounter(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->getStateLimitCounterFunc, nullptr);
}

static void AddPrepareAvgArray(JitLlvmCodeGenContext* ctx, int element_type, int element_count)
{
    llvm::ConstantInt* element_type_value = llvm::ConstantInt::get(ctx->INT32_T, element_type, true);
    llvm::ConstantInt* element_count_value = llvm::ConstantInt::get(ctx->INT32_T, element_count, true);
    AddFunctionCall(ctx, ctx->prepareAvgArrayFunc, element_type_value, element_count_value, nullptr);
}

static llvm::Value* AddLoadAvgArray(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->loadAvgArrayFunc, nullptr);
}

static void AddSaveAvgArray(JitLlvmCodeGenContext* ctx, llvm::Value* avg_array)
{
    AddFunctionCall(ctx, ctx->saveAvgArrayFunc, avg_array, nullptr);
}

static llvm::Value* AddComputeAvgFromArray(JitLlvmCodeGenContext* ctx, int element_type)
{
    llvm::ConstantInt* element_type_value = llvm::ConstantInt::get(ctx->INT32_T, element_type, true);
    return AddFunctionCall(ctx, ctx->computeAvgFromArrayFunc, element_type_value, nullptr);
}

static void AddResetAggValue(JitLlvmCodeGenContext* ctx, int element_type)
{
    llvm::ConstantInt* element_type_value = llvm::ConstantInt::get(ctx->INT32_T, element_type, true);
    AddFunctionCall(ctx, ctx->resetAggValueFunc, element_type_value, nullptr);
}

static void AddResetAggMaxMinNull(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->resetAggMaxMinNullFunc, nullptr);
}

static llvm::Value* AddGetAggValue(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->getAggValueFunc, nullptr);
}

static void AddSetAggValue(JitLlvmCodeGenContext* ctx, llvm::Value* value)
{
    AddFunctionCall(ctx, ctx->setAggValueFunc, value, nullptr);
}

static void AddSetAggMaxMinNotNull(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->setAggMaxMinNotNullFunc, nullptr);
}

static llvm::Value* AddGetAggMaxMinIsNull(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->getAggMaxMinIsNullFunc, nullptr);
}

static void AddPrepareDistinctSet(JitLlvmCodeGenContext* ctx, int element_type)
{
    llvm::ConstantInt* element_type_value = llvm::ConstantInt::get(ctx->INT32_T, element_type, true);
    AddFunctionCall(ctx, ctx->prepareDistinctSetFunc, element_type_value, nullptr);
}

static llvm::Value* AddInsertDistinctItem(JitLlvmCodeGenContext* ctx, int element_type, llvm::Value* value)
{
    llvm::ConstantInt* element_type_value = llvm::ConstantInt::get(ctx->INT32_T, element_type, true);
    return AddFunctionCall(ctx, ctx->insertDistinctItemFunc, element_type_value, value, nullptr);
}

static void AddDestroyDistinctSet(JitLlvmCodeGenContext* ctx, int element_type)
{
    llvm::ConstantInt* element_type_value = llvm::ConstantInt::get(ctx->INT32_T, element_type, true);
    AddFunctionCall(ctx, ctx->destroyDistinctSetFunc, element_type_value, nullptr);
}

/** @brief Adds a call to writeTupleDatum(slot, tuple_colid, value). */
static void AddWriteTupleDatum(JitLlvmCodeGenContext* ctx, int tuple_colid, llvm::Value* value)
{
    llvm::ConstantInt* tuple_colid_value = llvm::ConstantInt::get(ctx->INT32_T, tuple_colid, true);
    AddFunctionCall(ctx, ctx->writeTupleDatumFunc, ctx->slot_value, tuple_colid_value, value, nullptr);
}

/** @brief Adds a call to issueDebugLog(function, msg). */
#ifdef MOT_JIT_DEBUG
static void IssueDebugLogImpl(JitLlvmCodeGenContext* ctx, const char* function, const char* msg)
{
    llvm::ConstantInt* function_value = llvm::ConstantInt::get(ctx->INT64_T, (int64_t)function, true);
    llvm::ConstantInt* msg_value = llvm::ConstantInt::get(ctx->INT64_T, (int64_t)msg, true);
    llvm::Value* function_ptr = llvm::ConstantExpr::getIntToPtr(function_value, ctx->STR_T);
    llvm::Value* msg_ptr = llvm::ConstantExpr::getIntToPtr(msg_value, ctx->STR_T);
    AddFunctionCall(ctx, ctx->debugLogFunc, function_ptr, msg_ptr, nullptr);
}
#endif

#ifdef MOT_JIT_DEBUG
#define IssueDebugLog(msg) IssueDebugLogImpl(ctx, __func__, msg)
#else
#define IssueDebugLog(msg)
#endif

/*--------------------------- Helpers to generate compound LLVM code ---------------------------*/
/** @brief Builds a code segment for checking if soft memory limit has been reached. */
static void buildIsSoftMemoryLimitReached(JitLlvmCodeGenContext* ctx)
{
    JIT_IF_BEGIN(soft_limit_reached)
    llvm::Value* is_limit_reached_res = AddIsSoftMemoryLimitReached(ctx);
    JIT_IF_EVAL(is_limit_reached_res)
    IssueDebugLog("Soft memory limit reached");
    JIT_RETURN_CONST(MOT::RC_MEMORY_ALLOCATION_ERROR);
    JIT_ELSE()
    IssueDebugLog("Soft memory limit not reached");
    JIT_IF_END()
}

/** @brief Builds a code segment for writing datum value to a column. */
static void buildWriteDatumColumn(JitLlvmCodeGenContext* ctx, llvm::Value* row, int colid, llvm::Value* datum_value)
{
    llvm::Value* set_null_bit_res = AddSetExprResultNullBit(ctx, row, colid);
    IssueDebugLog("Set null bit");

    if (ctx->_table_info.m_table->GetField(colid)->m_isNotNull) {
        JIT_IF_BEGIN(check_null_violation)
        JIT_IF_EVAL_CMP(set_null_bit_res, JIT_CONST(MOT::RC_OK), JIT_ICMP_NE)
        IssueDebugLog("Null constraint violated");
        JIT_RETURN(set_null_bit_res);
        JIT_IF_END()
    }

    // now check if the result is not null, and if so write column datum
    llvm::Value* is_expr_null = AddGetExprArgIsNull(ctx, 0);
    JIT_IF_BEGIN(check_expr_null)
    JIT_IF_EVAL_NOT(is_expr_null)
    IssueDebugLog("Encountered non-null expression result, writing datum column");
    AddWriteDatumColumn(ctx, colid, row, datum_value);
    JIT_IF_END()
}

/** @brief Builds a code segment for writing a row. */
static void buildWriteRow(JitLlvmCodeGenContext* ctx, llvm::Value* row, bool isPKey, JitLlvmRuntimeCursor* cursor)
{
    IssueDebugLog("Writing row");
    llvm::Value* write_row_res = AddWriteRow(ctx, row);

    JIT_IF_BEGIN(check_row_written)
    JIT_IF_EVAL_CMP(write_row_res, JIT_CONST(MOT::RC_OK), JIT_ICMP_NE)
    IssueDebugLog("Row not written");
    // need to emit cleanup code
    if (!isPKey) {
        AddDestroyCursor(ctx, cursor);
    }
    JIT_RETURN(write_row_res);
    JIT_IF_END()
}

/** @brief Process a join expression (WHERE clause) and generate code to build a search key. */
static bool ProcessJoinExpr(JitLlvmCodeGenContext* ctx, Expr* expr, int* column_count, int* column_array, int* max_arg)
{
    bool result = false;
    if (expr->type == T_OpExpr) {
        result = ProcessJoinOpExpr(ctx, (OpExpr*)expr, column_count, column_array, max_arg);
    } else if (expr->type == T_BoolExpr) {
        result = ProcessJoinBoolExpr(ctx, (BoolExpr*)expr, column_count, column_array, max_arg);
    } else {
        MOT_LOG_TRACE("Unsupported expression type %d while processing Join Expr", (int)expr->type);
    }
    return result;
}

/** @brief Process a general expression and generate code to evaluate the expression in runtime. */
static llvm::Value* ProcessExpr(
    JitLlvmCodeGenContext* ctx, Expr* expr, int& result_type, int arg_pos, int depth, int* max_arg);

/** @brief Process an operator expression (process only "COLUMN equals EXPR" operators). */
static bool ProcessJoinOpExpr(
    JitLlvmCodeGenContext* ctx, const OpExpr* op_expr, int* column_count, int* column_array, int* max_arg)
{
    bool result = false;
    // process only point queries
    if (IsWhereOperatorSupported(op_expr->opno)) {
        llvm::Value* value = nullptr;
        ListCell* lc1 = nullptr;
        int colid = -1;
        int vartype = -1;
        int result_type = -1;

        foreach (lc1, op_expr->args) {
            Expr* expr = (Expr*)lfirst(lc1);
            // sometimes relabel expression hides the inner expression, so we peel it off
            if (expr->type == T_RelabelType) {
                expr = ((RelabelType*)expr)->arg;
            }
            if (expr->type == T_Var) {
                Var* var = (Var*)expr;
                colid = var->varattno;
                vartype = var->vartype;
                if (!IsTypeSupported(vartype)) {
                    MOT_LOG_TRACE("ProcessJoinOpExpr(): Unsupported type %d", vartype);
                    return false;
                }
                // no further processing
            } else {
                value = ProcessExpr(ctx, expr, result_type, 0, 0, max_arg);
                if (value == nullptr) {
                    MOT_LOG_TRACE("Unsupported operand type %d while processing Join OpExpr", (int)expr->type);
                }
                if (!IsTypeSupported(result_type)) {
                    MOT_LOG_TRACE("ProcessJoinOpExpr(): Unsupported result type %d", result_type);
                    return false;
                }
                break;
            }
        }

        if ((colid != -1) && (value != nullptr) && (vartype != -1) && (result_type != -1)) {
            if (result_type != vartype) {
                MOT_LOG_TRACE("ProcessJoinOpExpr(): vartype %d and result-type %d mismatch", vartype, result_type);
                return false;
            }
            // execute: column = getColumnAt(colid)
            llvm::Value* column = AddGetColumnAt(ctx,
                colid,
                JIT_RANGE_SCAN_MAIN);  // no need to translate to zero-based index (first column is null bits)
            int index_colid = ctx->_table_info.m_columnMap[colid];
            AddBuildDatumKey(ctx, column, index_colid, value, vartype, JIT_RANGE_ITERATOR_START, JIT_RANGE_SCAN_MAIN);

            MOT_LOG_DEBUG("Encountered table column %d, index column %d in where clause", colid, index_colid);
            ++(*column_count);
            column_array[index_colid] = 1;
            result = true;
        } else {
            MOT_LOG_TRACE("ProcessJoinOpExpr(): Invalid expression (colid=%d, value=%p, vartype=%d, result_type=%d)",
                colid,
                value,
                vartype,
                result_type);
        }
    } else {
        MOT_LOG_TRACE("ProcessJoinOpExpr(): Unsupported operator type %u", op_expr->opno);
    }

    return result;
}

/** @brief Process a boolean operator (process only AND operators, since we handle only point queries, or full-prefix
 * range update). */
static bool ProcessJoinBoolExpr(
    JitLlvmCodeGenContext* ctx, const BoolExpr* boolexpr, int* column_count, int* column_array, int* max_arg)
{
    bool result = false;
    if (boolexpr->boolop == AND_EXPR) {
        // now traverse args to get param index to build search key
        ListCell* lc = nullptr;
        foreach (lc, boolexpr->args) {
            // each element is Expr
            Expr* expr = (Expr*)lfirst(lc);
            result = ProcessJoinExpr(ctx, expr, column_count, column_array, max_arg);
            if (!result) {
                MOT_LOG_TRACE("Failed to process operand while processing Join BoolExpr");
                break;
            }
        }
    } else {
        MOT_LOG_TRACE("Unsupported bool operation %d while processing Join BoolExpr", (int)boolexpr->boolop);
    }
    return result;
}

/** @brief Creates a jitted function for code generation. Builds prototype and entry block. */
static void CreateJittedFunction(JitLlvmCodeGenContext* ctx, const char* function_name)
{
    llvm::Value* llvmargs[MOT_JIT_FUNC_ARG_COUNT];

    // define the function prototype
    GsCodeGen::FnPrototype fn_prototype(ctx->_code_gen, function_name, ctx->INT32_T);
    fn_prototype.addArgument(GsCodeGen::NamedVariable("table", ctx->TableType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("index", ctx->IndexType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("key", ctx->KeyType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("bitmap", ctx->BitmapSetType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("params", ctx->ParamListInfoDataType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("slot", ctx->TupleTableSlotType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("tp_processed", ctx->INT64_T->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("scan_ended", ctx->INT32_T->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("end_iterator_key", ctx->KeyType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("inner_table", ctx->TableType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("inner_index", ctx->IndexType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("inner_key", ctx->KeyType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("innerm_endIteratorKey", ctx->KeyType->getPointerTo()));

    ctx->m_jittedQuery = fn_prototype.generatePrototype(ctx->_builder, &llvmargs[0]);

    // get the arguments
    int arg_index = 0;
    ctx->table_value = llvmargs[arg_index++];
    ctx->index_value = llvmargs[arg_index++];
    ctx->key_value = llvmargs[arg_index++];
    ctx->bitmap_value = llvmargs[arg_index++];
    ctx->params_value = llvmargs[arg_index++];
    ctx->slot_value = llvmargs[arg_index++];
    ctx->tp_processed_value = llvmargs[arg_index++];
    ctx->scan_ended_value = llvmargs[arg_index++];
    ctx->end_iterator_key_value = llvmargs[arg_index++];
    ctx->inner_table_value = llvmargs[arg_index++];
    ctx->inner_index_value = llvmargs[arg_index++];
    ctx->inner_key_value = llvmargs[arg_index++];
    ctx->inner_end_iterator_key_value = llvmargs[arg_index++];

    IssueDebugLog("Starting execution of jitted function");
}

/** @brief Adds code to reset the number of rows processed. */
static void buildResetRowsProcessed(JitLlvmCodeGenContext* ctx)
{
    ctx->rows_processed = llvm::ConstantInt::get(ctx->INT64_T, 0, true);
}

/** @brief Adds code to increment the number of rows processed. */
static void buildIncrementRowsProcessed(JitLlvmCodeGenContext* ctx)
{
    llvm::ConstantInt* one_value = llvm::ConstantInt::get(ctx->INT64_T, 1, true);
    ctx->rows_processed = ctx->_builder->CreateAdd(ctx->rows_processed, one_value);
}

/** @brief Adds code to create a new row. */
static llvm::Value* buildCreateNewRow(JitLlvmCodeGenContext* ctx)
{
    llvm::Value* row = AddCreateNewRow(ctx);

    JIT_IF_BEGIN(check_row_created)
    JIT_IF_EVAL_NOT(row)
    IssueDebugLog("Failed to create row");
    JIT_RETURN_CONST(MOT::RC_MEMORY_ALLOCATION_ERROR);
    JIT_IF_END()

    return row;
}

/** @brief Adds code to search for a row by a key. */
static llvm::Value* buildSearchRow(
    JitLlvmCodeGenContext* ctx, MOT::AccessType access_type, JitRangeScanType range_scan_type)
{
    IssueDebugLog("Searching row");
    llvm::Value* row = AddSearchRow(ctx, access_type, range_scan_type);

    JIT_IF_BEGIN(check_row_found)
    JIT_IF_EVAL_NOT(row)
    IssueDebugLog("Row not found");
    JIT_RETURN_CONST(MOT::RC_LOCAL_ROW_NOT_FOUND);
    JIT_IF_END()

    IssueDebugLog("Row found");
    return row;
}

static llvm::Value* buildFilter(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitFilter* filter, int* max_arg)
{
    llvm::Value* result = nullptr;
    llvm::Value* lhs_expr = ProcessExpr(ctx, row, filter->_lhs_operand, max_arg);
    if (lhs_expr == nullptr) {
        MOT_LOG_TRACE(
            "buildFilter(): Failed to process LHS expression with type %d", (int)filter->_lhs_operand->_expr_type);
    } else {
        llvm::Value* rhs_expr = ProcessExpr(ctx, row, filter->_rhs_operand, max_arg);
        if (rhs_expr == nullptr) {
            MOT_LOG_TRACE(
                "buildFilter(): Failed to process RHS expression with type %d", (int)filter->_rhs_operand->_expr_type);
        } else {
            result = ProcessFilterExpr(ctx, row, filter, max_arg);
        }
    }
    return result;
}

static bool buildFilterRow(
    JitLlvmCodeGenContext* ctx, llvm::Value* row, JitFilterArray* filters, int* max_arg, llvm::BasicBlock* next_block)
{
    // 1. for each filter expression we generate the equivalent instructions which should evaluate to true or false
    // 2. We assume that all filters are applied with AND operator between them (imposed during query analysis/plan
    // phase)
    for (int i = 0; i < filters->_filter_count; ++i) {
        llvm::Value* filter_expr = buildFilter(ctx, row, &filters->_scan_filters[i], max_arg);
        if (filter_expr == nullptr) {
            MOT_LOG_TRACE("buildFilterRow(): Failed to process filter expression %d", i);
            return false;
        }

        JIT_IF_BEGIN(filter_row)
        JIT_IF_EVAL_NOT(filter_expr)
        IssueDebugLog("Row filter expression failed");
        if (next_block == nullptr) {
            JIT_RETURN_CONST(MOT::RC_LOCAL_ROW_NOT_FOUND);
        } else {
            ctx->_builder->CreateBr(next_block);
        }
        JIT_IF_END()
    }
    return true;
}

/** @brief Adds code to insert a new row. */
static void buildInsertRow(JitLlvmCodeGenContext* ctx, llvm::Value* row)
{
    IssueDebugLog("Inserting row");
    llvm::Value* insert_row_res = AddInsertRow(ctx, row);

    JIT_IF_BEGIN(check_row_inserted)
    JIT_IF_EVAL_CMP(insert_row_res, JIT_CONST(MOT::RC_OK), JIT_ICMP_NE)
    IssueDebugLog("Row not inserted");
    JIT_RETURN(insert_row_res);
    JIT_IF_END()

    IssueDebugLog("Row inserted");
}

/** @brief Adds code to delete a row. */
static void buildDeleteRow(JitLlvmCodeGenContext* ctx)
{
    IssueDebugLog("Deleting row");
    llvm::Value* delete_row_res = AddDeleteRow(ctx);

    JIT_IF_BEGIN(check_delete_row)
    JIT_IF_EVAL_CMP(delete_row_res, JIT_CONST(MOT::RC_OK), JIT_ICMP_NE)
    IssueDebugLog("Row not deleted");
    JIT_RETURN(delete_row_res);
    JIT_IF_END()

    IssueDebugLog("Row deleted");
}

/** @brief Adds code to search for an iterator. */
static llvm::Value* buildSearchIterator(JitLlvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitRangeBoundMode range_bound_mode, JitRangeScanType range_scan_type)
{
    // search the row, execute: IndexIterator* itr = searchIterator(table, key);
    IssueDebugLog("Searching range start");
    llvm::Value* itr = AddSearchIterator(ctx, index_scan_direction, range_bound_mode, range_scan_type);

    JIT_IF_BEGIN(check_itr_found)
    JIT_IF_EVAL_NOT(itr)
    IssueDebugLog("Range start not found");
    JIT_RETURN_CONST(MOT::RC_LOCAL_ROW_NOT_FOUND);
    JIT_IF_END()

    IssueDebugLog("Range start found");
    return itr;
}

/** @brief Adds code to get row from iterator. */
static llvm::Value* buildGetRowFromIterator(JitLlvmCodeGenContext* ctx, llvm::BasicBlock* endLoopBlock,
    MOT::AccessType access_mode, JitIndexScanDirection index_scan_direction, JitLlvmRuntimeCursor* cursor,
    JitRangeScanType range_scan_type)
{
    IssueDebugLog("Retrieving row from iterator");
    llvm::Value* row = AddGetRowFromIterator(ctx, access_mode, index_scan_direction, cursor, range_scan_type);

    JIT_IF_BEGIN(check_itr_row_found)
    JIT_IF_EVAL_NOT(row)
    IssueDebugLog("Iterator row not found");
    JIT_GOTO(endLoopBlock);
    // NOTE: we can actually do here a JIT_WHILE_BREAK() if we have an enclosing while loop
    JIT_IF_END()

    IssueDebugLog("Iterator row found");
    return row;
}

/** @brief Process constant expression. */
static llvm::Value* ProcessConstExpr(
    JitLlvmCodeGenContext* ctx, const Const* const_value, int& result_type, int arg_pos, int depth, int* max_arg)
{
    llvm::Value* result = nullptr;
    MOT_LOG_DEBUG("%*s --> Processing CONST expression", depth, "");
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    // verify type is supported and return compile-time constant (no need to generate code for runtime evaluation)
    if (IsTypeSupported(const_value->consttype)) {
        result_type = const_value->consttype;
        AddSetExprArgIsNull(ctx, arg_pos, const_value->constisnull);  // mark expression null status
        result = llvm::ConstantInt::get(ctx->INT64_T, const_value->constvalue, true);
        if (max_arg && (arg_pos > *max_arg)) {
            *max_arg = arg_pos;
        }
    }

    MOT_LOG_DEBUG("%*s <-- Processing CONST expression result: %p", depth, "", result);
    return result;
}

/** @brief Process Param expression. */
static llvm::Value* ProcessParamExpr(
    JitLlvmCodeGenContext* ctx, const Param* param, int& result_type, int arg_pos, int depth, int* max_arg)
{
    llvm::Value* result = nullptr;
    MOT_LOG_DEBUG("%*s --> Processing PARAM expression", depth, "");
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    // verify type is supported and generate code to extract the parameter in runtime
    if (IsTypeSupported(param->paramtype)) {
        result_type = param->paramtype;
        result = AddGetDatumParam(ctx, param->paramid - 1, arg_pos);
        if (max_arg && (arg_pos > *max_arg)) {
            *max_arg = arg_pos;
        }
    }

    MOT_LOG_DEBUG("%*s <-- Processing PARAM expression result: %p", depth, "", result);
    return result;
}

/** @brief Process Relabel expression as Param expression. */
static llvm::Value* ProcessRelabelExpr(
    JitLlvmCodeGenContext* ctx, RelabelType* relabel_type, int& result_type, int arg_pos, int depth, int* max_arg)
{
    llvm::Value* result = nullptr;
    MOT_LOG_DEBUG("Processing RELABEL expression");
    Expr* expr = (Expr*)relabel_type->arg;
    if (expr->type == T_Param) {
        Param* param = (Param*)expr;
        result = ProcessParamExpr(ctx, param, result_type, arg_pos, depth, max_arg);
    } else {
        MOT_LOG_TRACE("Unexpected relabel argument type: %d", (int)expr->type);
    }
    return result;
}

/** @brief Proess Var expression. */
static llvm::Value* ProcessVarExpr(
    JitLlvmCodeGenContext* ctx, const Var* var, int& result_type, int arg_pos, int depth, int* max_arg)
{
    llvm::Value* result = nullptr;
    MOT_LOG_DEBUG("%*s --> Processing VAR expression", depth, "");
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    // verify that type is supported and generate code to read column datum during runtime
    if (IsTypeSupported(var->vartype)) {
        result_type = var->vartype;
        int table_colid = var->varattno;
        result = AddReadDatumColumn(ctx, ctx->table_value, nullptr, table_colid, arg_pos);
        if (max_arg && (arg_pos > *max_arg)) {
            *max_arg = arg_pos;
        }
    }

    MOT_LOG_DEBUG("%*s <-- Processing VAR expression result: %p", depth, "", result);
    return result;
}

/** @brief Adds call to PG unary operator. */
static llvm::Value* AddExecUnaryOperator(
    JitLlvmCodeGenContext* ctx, llvm::Value* param, llvm::Constant* unary_operator, int arg_pos)
{
    llvm::Constant* arg_pos_value = llvm::ConstantInt::get(ctx->INT32_T, arg_pos, true);
    return AddFunctionCall(ctx, unary_operator, param, arg_pos_value, nullptr);
}

/** @brief Adds call to PG binary operator. */
static llvm::Value* AddExecBinaryOperator(JitLlvmCodeGenContext* ctx, llvm::Value* lhs_param, llvm::Value* rhs_param,
    llvm::Constant* binary_operator, int arg_pos)
{
    llvm::Constant* arg_pos_value = llvm::ConstantInt::get(ctx->INT32_T, arg_pos, true);
    return AddFunctionCall(ctx, binary_operator, lhs_param, rhs_param, arg_pos_value, nullptr);
}

/** @brief Adds call to PG ternary operator. */
static llvm::Value* AddExecTernaryOperator(JitLlvmCodeGenContext* ctx, llvm::Value* param1, llvm::Value* param2,
    llvm::Value* param3, llvm::Constant* ternary_operator, int arg_pos)
{
    llvm::Constant* arg_pos_value = llvm::ConstantInt::get(ctx->INT32_T, arg_pos, true);
    return AddFunctionCall(ctx, ternary_operator, param1, param2, param3, arg_pos_value, nullptr);
}

#define APPLY_UNARY_OPERATOR(funcid, name)                                          \
    case funcid:                                                                    \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                            \
        result = AddExecUnaryOperator(ctx, args[0], ctx->_builtin_##name, arg_pos); \
        break;

#define APPLY_BINARY_OPERATOR(funcid, name)                                                   \
    case funcid:                                                                              \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                                      \
        result = AddExecBinaryOperator(ctx, args[0], args[1], ctx->_builtin_##name, arg_pos); \
        break;

#define APPLY_TERNARY_OPERATOR(funcid, name)                                                            \
    case funcid:                                                                                        \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                                                \
        result = AddExecTernaryOperator(ctx, args[0], args[1], args[2], ctx->_builtin_##name, arg_pos); \
        break;

#define APPLY_UNARY_CAST_OPERATOR(funcid, name) APPLY_UNARY_OPERATOR(funcid, name)
#define APPLY_BINARY_CAST_OPERATOR(funcid, name) APPLY_BINARY_OPERATOR(funcid, name)
#define APPLY_TERNARY_CAST_OPERATOR(funcid, name) APPLY_TERNARY_OPERATOR(funcid, name)

/** @brief Process operator expression. */
static llvm::Value* ProcessOpExpr(
    JitLlvmCodeGenContext* ctx, const OpExpr* op_expr, int& result_type, int arg_pos, int depth, int* max_arg)
{
    llvm::Value* result = nullptr;
    MOT_LOG_DEBUG("%*s --> Processing OP %d expression", depth, "", (int)op_expr->opfuncid);
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (list_length(op_expr->args) > 3) {
        MOT_LOG_TRACE("Unsupported operator %d: too many arguments", op_expr->opno);
        return nullptr;
    }

    llvm::Value* args[3] = {nullptr, nullptr, nullptr};
    int arg_num = 0;
    int dummy = 0;

    // process operator arguments (each one is an expression by itself)
    ListCell* lc = nullptr;
    foreach (lc, op_expr->args) {
        Expr* sub_expr = (Expr*)lfirst(lc);
        args[arg_num] = ProcessExpr(ctx, sub_expr, dummy, arg_pos + arg_num, depth + 1, max_arg);
        if (args[arg_num] == nullptr) {
            MOT_LOG_TRACE("Failed to process operator sub-expression %d", arg_num);
            return nullptr;
        }
        if (++arg_num == 3) {
            break;
        }
    }

    // process the operator - generate code to call the operator in runtime
    result_type = op_expr->opresulttype;
    switch (op_expr->opfuncid) {
        APPLY_OPERATORS()

        default:
            MOT_LOG_TRACE("Unsupported operator function type: %d", op_expr->opfuncid);
            break;
    }

    MOT_LOG_DEBUG("%*s <-- Processing OP %d expression result: %p", depth, "", (int)op_expr->opfuncid, result);
    return result;
}

/** @brief Process function expression. */
static llvm::Value* ProcessFuncExpr(
    JitLlvmCodeGenContext* ctx, const FuncExpr* func_expr, int& result_type, int arg_pos, int depth, int* max_arg)
{
    llvm::Value* result = nullptr;
    MOT_LOG_DEBUG("%*s --> Processing FUNC %d expression", depth, "", (int)func_expr->funcid);
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (list_length(func_expr->args) > 3) {
        MOT_LOG_TRACE("Unsupported function %d: too many arguments", func_expr->funcid);
        return nullptr;
    }

    llvm::Value* args[3] = {nullptr, nullptr, nullptr};
    int arg_num = 0;
    int dummy = 0;

    // process function arguments (each one is an expression by itself)
    ListCell* lc = nullptr;
    foreach (lc, func_expr->args) {
        Expr* sub_expr = (Expr*)lfirst(lc);
        args[arg_num] = ProcessExpr(ctx, sub_expr, dummy, arg_pos + arg_num, depth + 1, max_arg);
        if (args[arg_num] == nullptr) {
            MOT_LOG_TRACE("Failed to process function sub-expression %d", arg_num);
            return nullptr;
        }
        if (++arg_num == 3) {
            break;
        }
    }

    // process the function - generate code to call the function in runtime
    result_type = func_expr->funcresulttype;
    switch (func_expr->funcid) {
        APPLY_OPERATORS()

        default:
            MOT_LOG_TRACE("Unsupported function type: %d", (int)func_expr->funcid);
            break;
    }

    MOT_LOG_DEBUG("%*s <-- Processing FUNC %d expression result: %p", depth, "", (int)func_expr->funcid, result);
    return result;
}

// we allow only binary operators for filters
#undef APPLY_UNARY_OPERATOR
#undef APPLY_TERNARY_OPERATOR
#undef APPLY_UNARY_CAST_OPERATOR
#undef APPLY_TERNARY_CAST_OPERATOR

#define APPLY_UNARY_OPERATOR(funcid, name)                                              \
    case funcid:                                                                        \
        MOT_LOG_TRACE("Unexpected call in filter expression to unary builtin: " #name); \
        break;

#define APPLY_TERNARY_OPERATOR(funcid, name)                                              \
    case funcid:                                                                          \
        MOT_LOG_TRACE("Unexpected call in filter expression to ternary builtin: " #name); \
        break;

#define APPLY_UNARY_CAST_OPERATOR(funcid, name)                                              \
    case funcid:                                                                             \
        MOT_LOG_TRACE("Unexpected call in filter expression to unary cast builtin: " #name); \
        break;

#define APPLY_TERNARY_CAST_OPERATOR(funcid, name)                                              \
    case funcid:                                                                               \
        MOT_LOG_TRACE("Unexpected call in filter expression to ternary cast builtin: " #name); \
        break;

static llvm::Value* ProcessFilterExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitFilter* filter, int* max_arg)
{
    llvm::Value* result = nullptr;

    llvm::Value* args[3] = {nullptr, nullptr, nullptr};

    args[0] = ProcessExpr(ctx, row, filter->_lhs_operand, max_arg);
    if (!args[0]) {
        MOT_LOG_TRACE("Failed to process filter LHS expression");
        return nullptr;
    }

    args[1] = ProcessExpr(ctx, row, filter->_rhs_operand, max_arg);
    if (!args[1]) {
        MOT_LOG_TRACE("Failed to process filter RHS expression");
        return nullptr;
    }

    int arg_pos = 0;  // always a top-level expression
    switch (filter->_filter_op_funcid) {
        APPLY_OPERATORS()

        default:
            MOT_LOG_TRACE("Unsupported filter function type: %d", filter->_filter_op_funcid);
            break;
    }

    return result;
}

#undef APPLY_UNARY_OPERATOR
#undef APPLY_BINARY_OPERATOR
#undef APPLY_TERNARY_OPERATOR
#undef APPLY_UNARY_CAST_OPERATOR
#undef APPLY_BINARY_CAST_OPERATOR
#undef APPLY_TERNARY_CAST_OPERATOR

/** @brief Process an expression. Generates code to evaluate the expression. */
static llvm::Value* ProcessExpr(
    JitLlvmCodeGenContext* ctx, Expr* expr, int& result_type, int arg_pos, int depth, int* max_arg)
{
    llvm::Value* result = nullptr;
    MOT_LOG_DEBUG("%*s --> Processing expression %d", depth, "", (int)expr->type);
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    // case 1: assign from parameter (cases like: s_quantity = $1)
    if (expr->type == T_Const) {
        result = ProcessConstExpr(ctx, (Const*)expr, result_type, arg_pos, depth + 1, max_arg);
    } else if (expr->type == T_Param) {
        result = ProcessParamExpr(ctx, (Param*)expr, result_type, arg_pos, depth + 1, max_arg);
    } else if (expr->type == T_RelabelType) {
        result = ProcessRelabelExpr(ctx, (RelabelType*)expr, result_type, arg_pos, depth + 1, max_arg);
    } else if (expr->type == T_Var) {
        result = ProcessVarExpr(ctx, (Var*)expr, result_type, arg_pos, depth + 1, max_arg);
    } else if (expr->type == T_OpExpr) {
        result = ProcessOpExpr(ctx, (OpExpr*)expr, result_type, arg_pos, depth + 1, max_arg);
    } else if (expr->type == T_FuncExpr) {
        result = ProcessFuncExpr(ctx, (FuncExpr*)expr, result_type, arg_pos, depth + 1, max_arg);
    } else {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for query: unsupported target expression type: %d", (int)expr->type);
    }

    MOT_LOG_DEBUG("%*s <-- Processing expression %d result: %p", depth, "", (int)expr->type, result);
    return result;
}

static llvm::Value* ProcessConstExpr(JitLlvmCodeGenContext* ctx, const JitConstExpr* expr, int* max_arg)
{
    AddSetExprArgIsNull(ctx, expr->_arg_pos, expr->_is_null);  // mark expression null status
    llvm::Value* result = llvm::ConstantInt::get(ctx->INT64_T, expr->_value, true);
    if (max_arg && (expr->_arg_pos > *max_arg)) {
        *max_arg = expr->_arg_pos;
    }
    return result;
}

static llvm::Value* ProcessParamExpr(JitLlvmCodeGenContext* ctx, const JitParamExpr* expr, int* max_arg)
{
    llvm::Value* result = AddGetDatumParam(ctx, expr->_param_id, expr->_arg_pos);
    if (max_arg && (expr->_arg_pos > *max_arg)) {
        *max_arg = expr->_arg_pos;
    }
    return result;
}

static llvm::Value* ProcessVarExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, const JitVarExpr* expr, int* max_arg)
{
    llvm::Value* result = nullptr;
    if (row == nullptr) {
        MOT_LOG_TRACE("ProcessVarExpr(): Unexpected VAR expression without a row");
    } else {
        // this is a bit awkward, but it works
        llvm::Value* table = (expr->_table == ctx->_table_info.m_table) ? ctx->table_value : ctx->inner_table_value;
        result = AddReadDatumColumn(ctx, table, row, expr->_column_id, expr->_arg_pos);
        if (max_arg && (expr->_arg_pos > *max_arg)) {
            *max_arg = expr->_arg_pos;
        }
    }
    return result;
}

#define APPLY_UNARY_OPERATOR(funcid, name)                                                 \
    case funcid:                                                                           \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                                   \
        result = AddExecUnaryOperator(ctx, args[0], ctx->_builtin_##name, expr->_arg_pos); \
        break;

#define APPLY_BINARY_OPERATOR(funcid, name)                                                          \
    case funcid:                                                                                     \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                                             \
        result = AddExecBinaryOperator(ctx, args[0], args[1], ctx->_builtin_##name, expr->_arg_pos); \
        break;

#define APPLY_TERNARY_OPERATOR(funcid, name)                                                                   \
    case funcid:                                                                                               \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                                                       \
        result = AddExecTernaryOperator(ctx, args[0], args[1], args[2], ctx->_builtin_##name, expr->_arg_pos); \
        break;

#define APPLY_UNARY_CAST_OPERATOR(funcid, name) APPLY_UNARY_OPERATOR(funcid, name)
#define APPLY_BINARY_CAST_OPERATOR(funcid, name) APPLY_BINARY_OPERATOR(funcid, name)
#define APPLY_TERNARY_CAST_OPERATOR(funcid, name) APPLY_TERNARY_OPERATOR(funcid, name)

static llvm::Value* ProcessOpExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitOpExpr* expr, int* max_arg)
{
    llvm::Value* result = nullptr;

    llvm::Value* args[3] = {nullptr, nullptr, nullptr};
    int arg_num = 0;

    for (int i = 0; i < expr->_arg_count; ++i) {
        args[i] = ProcessExpr(ctx, row, expr->_args[i], max_arg);
        if (args[i] == nullptr) {
            MOT_LOG_TRACE("Failed to process operator sub-expression %d", arg_num);
            return nullptr;
        }
    }

    switch (expr->_op_func_id) {
        APPLY_OPERATORS()

        default:
            MOT_LOG_TRACE("Unsupported operator function type: %d", expr->_op_func_id);
            break;
    }

    return result;
}

static llvm::Value* ProcessFuncExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitFuncExpr* expr, int* max_arg)
{
    llvm::Value* result = nullptr;

    llvm::Value* args[3] = {nullptr, nullptr, nullptr};
    int arg_num = 0;

    for (int i = 0; i < expr->_arg_count; ++i) {
        args[i] = ProcessExpr(ctx, row, expr->_args[i], max_arg);
        if (args[i] == nullptr) {
            MOT_LOG_TRACE("Failed to process function sub-expression %d", arg_num);
            return nullptr;
        }
    }

    switch (expr->_func_id) {
        APPLY_OPERATORS()

        default:
            MOT_LOG_TRACE("Unsupported function type: %d", expr->_func_id);
            break;
    }

    return result;
}

#undef APPLY_UNARY_OPERATOR
#undef APPLY_BINARY_OPERATOR
#undef APPLY_TERNARY_OPERATOR
#undef APPLY_UNARY_CAST_OPERATOR
#undef APPLY_BINARY_CAST_OPERATOR
#undef APPLY_TERNARY_CAST_OPERATOR

static llvm::Value* ProcessExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitExpr* expr, int* max_arg)
{
    llvm::Value* result = nullptr;

    if (expr->_expr_type == JIT_EXPR_TYPE_CONST) {
        result = ProcessConstExpr(ctx, (JitConstExpr*)expr, max_arg);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_PARAM) {
        result = ProcessParamExpr(ctx, (JitParamExpr*)expr, max_arg);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_VAR) {
        result = ProcessVarExpr(ctx, row, (JitVarExpr*)expr, max_arg);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_OP) {
        result = ProcessOpExpr(ctx, row, (JitOpExpr*)expr, max_arg);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_FUNC) {
        result = ProcessFuncExpr(ctx, row, (JitFuncExpr*)expr, max_arg);
    } else {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for query: unsupported target expression type: %d", (int)expr->_expr_type);
    }

    return result;
}

#ifdef __aarch64__
// on ARM platforms we experienced instability during concurrent JIT compilation, so we use a spin-lock
/** @var The global ARM compilation lock. */
static pthread_spinlock_t _arm_compile_lock;

/** @var Denotes whether the global ARM compilation lock initialization was already attempted. */
static int _arm_compile_lock_initialized = 0;

/** @var Denote the global ARM compilation lock is initialized and ready for use. */
static bool _arm_compile_lock_ready = false;

/** @brief Initializes the global ARM compilation lock. */
extern bool InitArmCompileLock()
{
    bool result = true;
    if (__sync_bool_compare_and_swap(&_arm_compile_lock_initialized, 0, 1)) {
        int res = pthread_spin_init(&_arm_compile_lock, 0);
        if (res != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(
                res, pthread_spin_init, "Initialize LLVM", "Failed to initialize compile spin-lock for ARM platform");
            // turn off JIT
            DisableMotCodegen();
            result = false;
        } else {
            _arm_compile_lock_ready = true;
        }
    }
    return result;
}

/** @brief Destroys the global ARM compilation lock. */
extern void DestroyArmCompileLock()
{
    if (_arm_compile_lock_ready) {
        int res = pthread_spin_destroy(&_arm_compile_lock);
        if (res != 0) {
            MOT_REPORT_SYSTEM_ERROR_CODE(
                res, pthread_spin_init, "Initialize LLVM", "Failed to destroy compile spin-lock for ARM platform");
        }
    }
}

/** @brief Locks the global ARM compilation lock. */
static bool AcquireArmCompileLock()
{
    bool result = false;
    int res = pthread_spin_lock(&_arm_compile_lock);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(
            res, pthread_spin_init, "Compile LLVM", "Failed to acquire compile spin-lock for ARM platform");
    } else {
        result = true;
    }
    return result;
}

/** @brief Unlocks the global ARM compilation lock. */
static bool ReleaseArmCompileLock()
{
    bool result = false;
    int res = pthread_spin_unlock(&_arm_compile_lock);
    if (res != 0) {
        MOT_REPORT_SYSTEM_ERROR_CODE(
            res, pthread_spin_init, "Compile LLVM", "Failed to release compile spin-lock for ARM platform");
    } else {
        result = true;
    }
    return result;
}
#endif

/** @brief Checks whether the current platforms natively supports LLVM. */
extern bool JitCanInitThreadCodeGen()
{
#ifdef __aarch64__
    return true;  // Using native llvm on ARM
#else
    bool canInit = false;
    if (GlobalCodeGenEnvironmentSuccess && IS_PGXC_DATANODE && IsMotCodegenEnabled()) {
        canInit = isCPUFeatureSupportCodegen();
        if (!canInit) {
            // turn off JIT
            DisableMotCodegen();
            MOT_LOG_WARN("SSE4.2 is not supported, disable codegen.");
        }
    }
    return canInit;
#endif
}

/** @brief Prepares for code generation. */
static GsCodeGen* SetupCodegenEnv()
{
    // verify that native LLVM is supported
    if (!t_thrd.mot_cxt.init_codegen_once) {
        t_thrd.mot_cxt.init_codegen_once = true;
        if (!JitCanInitThreadCodeGen()) {
            return nullptr;
        }
    }

    // on ARM platform we need to make sure that the compile lock was initialized properly
#ifdef __aarch64__
    if (!_arm_compile_lock_ready) {
        MOT_LOG_TRACE("Previous attempt to initialize compilation lock on ARM failed. Code generation is disabled.");
        // turn off JIT
        DisableMotCodegen();
        return nullptr;
    }
#endif

    // create GsCodeGen object for LLVM code generation
    GsCodeGen* code_gen = New(g_instance.instance_context) GsCodeGen();
    if (code_gen != nullptr) {
        code_gen->initialize();
        code_gen->createNewModule();
    }
    return code_gen;
}

/** @brief Wraps up an LLVM function (compiles it and prepares a funciton pointer). */
static JitContext* FinalizeCodegen(JitLlvmCodeGenContext* ctx, int max_arg, JitCommandType command_type)
{
    // do GsCodeGen stuff to wrap up
    if (!ctx->_code_gen->verifyFunction(ctx->m_jittedQuery)) {
        MOT_LOG_ERROR("Failed to generate jitted code for query: Failed to verify jit function");
#ifdef LLVM_ENABLE_DUMP
        ctx->m_jittedQuery->dump();
#else
        ctx->m_jittedQuery->print(llvm::errs(), nullptr, false, true);
#endif
        return nullptr;
    }
    ctx->_code_gen->FinalizeFunction(ctx->m_jittedQuery);

    if (IsMotCodegenPrintEnabled()) {
#ifdef LLVM_ENABLE_DUMP
        ctx->m_jittedQuery->dump();
#else
        ctx->m_jittedQuery->print(llvm::errs());
#endif
    }

    // that's it, we are ready
    JitContext* jit_context = AllocJitContext(JIT_CONTEXT_GLOBAL);
    if (jit_context == nullptr) {
        MOT_LOG_TRACE("Failed to allocate JIT context, aborting code generation");
        return nullptr;
    }

    MOT_LOG_DEBUG("Adding function to MCJit");
    ctx->_code_gen->addFunctionToMCJit(ctx->m_jittedQuery, (void**)&jit_context->m_llvmFunction);

    // on ARM platform we must ensure compilation is synchronized
#ifdef __aarch64__
    if (!AcquireArmCompileLock()) {
        MOT_LOG_TRACE("Failed to acquire compilation lock on ARM platform");
        FreeJitContext(jit_context);
        return nullptr;
    }
#endif

    MOT_LOG_DEBUG("Generating code...");
    ctx->_code_gen->enableOptimizations(true);
    ctx->_code_gen->compileCurrentModule(false);
    MOT_LOG_DEBUG(
        "MOT jitted query: code generated at %p --> %p", &jit_context->m_llvmFunction, jit_context->m_llvmFunction);

    // on ARM platform we must ensure compilation is synchronized
#ifdef __aarch64__
    if (!ReleaseArmCompileLock()) {
        MOT_LOG_TRACE("Failed to release compilation lock on ARM platform");
        FreeJitContext(jit_context);
        return nullptr;
    }
#endif

    // setup execution details
    jit_context->m_table = ctx->_table_info.m_table;
    jit_context->m_index = ctx->_table_info.m_index;
    jit_context->m_codeGen = ctx->_code_gen;  // steal the context
    ctx->_code_gen = nullptr;                 // prevent destruction
    jit_context->m_argCount = max_arg + 1;
    jit_context->m_innerTable = ctx->_inner_table_info.m_table;
    jit_context->m_innerIndex = ctx->_inner_table_info.m_index;
    jit_context->m_commandType = command_type;

    return jit_context;
}

static bool buildScanExpression(JitLlvmCodeGenContext* ctx, JitColumnExpr* expr, int* max_arg,
    JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type, llvm::Value* outer_row)
{
    llvm::Value* value = ProcessExpr(ctx, outer_row, expr->_expr, max_arg);
    if (value == nullptr) {
        MOT_LOG_TRACE("buildScanExpression(): Failed to process expression with type %d", (int)expr->_expr->_expr_type);
        return false;
    } else {
        llvm::Value* column = AddGetColumnAt(ctx,
            expr->_table_column_id,
            range_scan_type);  // no need to translate to zero-based index (first column is null bits)
        int index_colid = -1;
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            index_colid = ctx->_inner_table_info.m_columnMap[expr->_table_column_id];
        } else {
            index_colid = ctx->_table_info.m_columnMap[expr->_table_column_id];
        }
        AddBuildDatumKey(ctx, column, index_colid, value, expr->_column_type, range_itr_type, range_scan_type);
    }
    return true;
}

static bool buildPointScan(JitLlvmCodeGenContext* ctx, JitColumnExprArray* expr_array, int* max_arg,
    JitRangeScanType range_scan_type, llvm::Value* outer_row, int expr_count = 0)
{
    if (expr_count == 0) {
        expr_count = expr_array->_count;
    }
    AddInitSearchKey(ctx, range_scan_type);
    for (int i = 0; i < expr_count; ++i) {
        JitColumnExpr* expr = &expr_array->_exprs[i];

        // validate the expression refers to the right table (in search expressions array, all expressions refer to the
        // same table)
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            if (expr->_table != ctx->_inner_table_info.m_table) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Generate LLVM JIT Code",
                    "Invalid expression table (expected inner table %s, got %s)",
                    ctx->_inner_table_info.m_table->GetTableName().c_str(),
                    expr->_table->GetTableName().c_str());
                return false;
            }
        } else {
            if (expr->_table != ctx->_table_info.m_table) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Generate LLVM JIT Code",
                    "Invalid expression table (expected main/outer table %s, got %s)",
                    ctx->_inner_table_info.m_table->GetTableName().c_str(),
                    expr->_table->GetTableName().c_str());
                return false;
            }
        }

        // prepare the sub-expression
        if (!buildScanExpression(ctx, expr, max_arg, JIT_RANGE_ITERATOR_START, range_scan_type, outer_row)) {
            return false;
        }
    }
    return true;
}

static bool writeRowColumns(
    JitLlvmCodeGenContext* ctx, llvm::Value* row, JitColumnExprArray* expr_array, int* max_arg, bool is_update)
{
    for (int i = 0; i < expr_array->_count; ++i) {
        JitColumnExpr* column_expr = &expr_array->_exprs[i];

        llvm::Value* value = ProcessExpr(ctx, row, column_expr->_expr, max_arg);
        if (value == nullptr) {
            MOT_LOG_TRACE("ProcessExpr() returned nullptr");
            return false;
        }

        // set null bit or copy result data to column
        buildWriteDatumColumn(ctx, row, column_expr->_table_column_id, value);

        // execute (for incremental redo): setBit(bmp, bit_index);
        if (is_update) {
            AddSetBit(ctx, column_expr->_table_column_id - 1);
        }
    }

    return true;
}

static bool selectRowColumns(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitSelectExprArray* expr_array, int* max_arg,
    JitRangeScanType range_scan_type)
{
    bool result = true;
    for (int i = 0; i < expr_array->_count; ++i) {
        JitSelectExpr* expr = &expr_array->_exprs[i];
        // we skip expressions that select from other tables
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            if (expr->_column_expr->_table != ctx->_inner_table_info.m_table) {
                continue;
            }
        } else {
            if (expr->_column_expr->_table != ctx->_table_info.m_table) {
                continue;
            }
        }
        result = AddSelectColumn(ctx, row, expr->_column_expr->_column_id, expr->_tuple_column_id, range_scan_type);
        if (!result) {
            break;
        }
    }

    return result;
}

static bool buildClosedRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, llvm::Value* outer_row)
{
    // a closed range scan starts just like a point scan (with not enough search expressions) and then adds key patterns
    bool result = buildPointScan(ctx, &index_scan->_search_exprs, max_arg, range_scan_type, outer_row);
    if (result) {
        AddCopyKey(ctx, range_scan_type);

        // now fill each key with the right pattern for the missing pkey columns in the where clause
        bool ascending = (index_scan->_sort_order == JIT_QUERY_SORT_ASCENDING);

        int* index_column_offsets = nullptr;
        const uint16_t* key_length = nullptr;
        int index_column_count = 0;
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            index_column_offsets = ctx->_inner_table_info.m_indexColumnOffsets;
            key_length = ctx->_inner_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_inner_table_info.m_index->GetNumFields();
        } else {
            index_column_offsets = ctx->_table_info.m_indexColumnOffsets;
            key_length = ctx->_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_table_info.m_index->GetNumFields();
        }

        int first_zero_column = index_scan->_column_count;
        for (int i = first_zero_column; i < index_column_count; ++i) {
            int offset = index_column_offsets[i];
            int size = key_length[i];
            MOT_LOG_DEBUG(
                "Filling begin/end iterator pattern for missing pkey fields at offset %d, size %d", offset, size);
            AddFillKeyPattern(ctx, ascending ? 0x00 : 0xFF, offset, size, JIT_RANGE_ITERATOR_START, range_scan_type);
            AddFillKeyPattern(ctx, ascending ? 0xFF : 0x00, offset, size, JIT_RANGE_ITERATOR_END, range_scan_type);
        }

        AddAdjustKey(ctx,
            ascending ? 0x00 : 0xFF,
            JIT_RANGE_ITERATOR_START,
            range_scan_type);  // currently this is relevant only for secondary index searches
        AddAdjustKey(ctx,
            ascending ? 0xFF : 0x00,
            JIT_RANGE_ITERATOR_END,
            range_scan_type);  // currently this is relevant only for secondary index searches
    }
    return result;
}

static bool buildSemiOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitRangeBoundMode* begin_range_bound, JitRangeBoundMode* end_range_bound,
    llvm::Value* outer_row)
{
    // an open range scan starts just like a point scan (with not enough search expressions) and then adds key patterns
    // we do not use the last search expression
    bool result = buildPointScan(
        ctx, &index_scan->_search_exprs, max_arg, range_scan_type, outer_row, index_scan->_search_exprs._count - 1);
    if (result) {
        AddCopyKey(ctx, range_scan_type);

        // now fill each key with the right pattern for the missing pkey columns in the where clause
        bool ascending = (index_scan->_sort_order == JIT_QUERY_SORT_ASCENDING);

        int* index_column_offsets = nullptr;
        const uint16_t* key_length = nullptr;
        int index_column_count = 0;
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            index_column_offsets = ctx->_inner_table_info.m_indexColumnOffsets;
            key_length = ctx->_inner_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_inner_table_info.m_index->GetNumFields();
        } else {
            index_column_offsets = ctx->_table_info.m_indexColumnOffsets;
            key_length = ctx->_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_table_info.m_index->GetNumFields();
        }

        // prepare offset and size for last column in search
        int last_dim_column = index_scan->_column_count - 1;
        int offset = index_column_offsets[last_dim_column];
        int size = key_length[last_dim_column];

        // now we fill the last dimension (override extra work of point scan above)
        int last_expr_index = index_scan->_search_exprs._count - 1;
        JitColumnExpr* last_expr = &index_scan->_search_exprs._exprs[last_expr_index];
        if (ascending) {
            if ((index_scan->_last_dim_op1 == JIT_WOC_LESS_THAN) ||
                (index_scan->_last_dim_op1 == JIT_WOC_LESS_EQUALS)) {
                // this is an upper bound operator on an ascending semi-open scan so we fill the begin key with zeros,
                // and the end key with the value
                AddFillKeyPattern(ctx, 0x00, offset, size, JIT_RANGE_ITERATOR_START, range_scan_type);
                buildScanExpression(ctx, last_expr, max_arg, JIT_RANGE_ITERATOR_END, range_scan_type, outer_row);
                *begin_range_bound = JIT_RANGE_BOUND_INCLUDE;
                *end_range_bound = (index_scan->_last_dim_op1 == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE
                                                                                      : JIT_RANGE_BOUND_EXCLUDE;
            } else {
                // this is a lower bound operator on an ascending semi-open scan so we fill the begin key with the
                // value, and the end key with 0xFF
                buildScanExpression(ctx, last_expr, max_arg, JIT_RANGE_ITERATOR_START, range_scan_type, outer_row);
                AddFillKeyPattern(ctx, 0xFF, offset, size, JIT_RANGE_ITERATOR_END, range_scan_type);
                *begin_range_bound = (index_scan->_last_dim_op1 == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE
                                                                                           : JIT_RANGE_BOUND_EXCLUDE;
                *end_range_bound = JIT_RANGE_BOUND_INCLUDE;
            }
        } else {
            if ((index_scan->_last_dim_op1 == JIT_WOC_LESS_THAN) ||
                (index_scan->_last_dim_op1 == JIT_WOC_LESS_EQUALS)) {
                // this is an upper bound operator on a descending semi-open scan so we fill the begin key with value,
                // and the end key with zeroes
                buildScanExpression(ctx, last_expr, max_arg, JIT_RANGE_ITERATOR_START, range_scan_type, outer_row);
                AddFillKeyPattern(ctx, 0x00, offset, size, JIT_RANGE_ITERATOR_END, range_scan_type);
                *begin_range_bound = (index_scan->_last_dim_op1 == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE
                                                                                        : JIT_RANGE_BOUND_EXCLUDE;
                *end_range_bound = JIT_RANGE_BOUND_INCLUDE;
            } else {
                // this is a lower bound operator on a descending semi-open scan so we fill the begin key with 0xFF, and
                // the end key with the value
                AddFillKeyPattern(ctx, 0xFF, offset, size, JIT_RANGE_ITERATOR_START, range_scan_type);
                buildScanExpression(ctx, last_expr, max_arg, JIT_RANGE_ITERATOR_END, range_scan_type, outer_row);
                *begin_range_bound = JIT_RANGE_BOUND_INCLUDE;
                *end_range_bound = (index_scan->_last_dim_op1 == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE
                                                                                         : JIT_RANGE_BOUND_EXCLUDE;
            }
        }

        // now fill the rest as usual
        int first_zero_column = index_scan->_column_count;
        for (int i = first_zero_column; i < index_column_count; ++i) {
            int offset = index_column_offsets[i];
            int size = key_length[i];
            MOT_LOG_DEBUG(
                "Filling begin/end iterator pattern for missing pkey fields at offset %d, size %d", offset, size);
            AddFillKeyPattern(ctx, ascending ? 0x00 : 0xFF, offset, size, JIT_RANGE_ITERATOR_START, range_scan_type);
            AddFillKeyPattern(ctx, ascending ? 0xFF : 0x00, offset, size, JIT_RANGE_ITERATOR_END, range_scan_type);
        }

        AddAdjustKey(ctx,
            ascending ? 0x00 : 0xFF,
            JIT_RANGE_ITERATOR_START,
            range_scan_type);  // currently this is relevant only for secondary index searches
        AddAdjustKey(ctx,
            ascending ? 0xFF : 0x00,
            JIT_RANGE_ITERATOR_END,
            range_scan_type);  // currently this is relevant only for secondary index searches
    }
    return result;
}

static bool buildOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitRangeBoundMode* begin_range_bound, JitRangeBoundMode* end_range_bound,
    llvm::Value* outer_row)
{
    // an open range scan starts just like a point scan (with not enough search expressions) and then adds key patterns
    // we do not use the last two expressions
    bool result = buildPointScan(
        ctx, &index_scan->_search_exprs, max_arg, range_scan_type, outer_row, index_scan->_search_exprs._count - 2);
    if (result) {
        AddCopyKey(ctx, range_scan_type);

        // now fill each key with the right pattern for the missing pkey columns in the where clause
        bool ascending = (index_scan->_sort_order == JIT_QUERY_SORT_ASCENDING);

        int* index_column_offsets = nullptr;
        const uint16_t* key_length = nullptr;
        int index_column_count = 0;
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            index_column_offsets = ctx->_inner_table_info.m_indexColumnOffsets;
            key_length = ctx->_inner_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_inner_table_info.m_index->GetNumFields();
        } else {
            index_column_offsets = ctx->_table_info.m_indexColumnOffsets;
            key_length = ctx->_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_table_info.m_index->GetNumFields();
        }

        // now we fill the last dimension (override extra work of point scan above)
        JitWhereOperatorClass before_last_dim_op = index_scan->_last_dim_op1;  // avoid confusion, and give proper names
        JitWhereOperatorClass last_dim_op = index_scan->_last_dim_op2;         // avoid confusion, and give proper names
        int last_expr_index = index_scan->_search_exprs._count - 1;
        JitColumnExpr* last_expr = &index_scan->_search_exprs._exprs[last_expr_index];
        JitColumnExpr* before_last_expr = &index_scan->_search_exprs._exprs[last_expr_index - 1];
        if (ascending) {
            if ((before_last_dim_op == JIT_WOC_LESS_THAN) || (before_last_dim_op == JIT_WOC_LESS_EQUALS)) {
                MOT_ASSERT((last_dim_op == JIT_WOC_GREATER_THAN) || (last_dim_op == JIT_WOC_GREATER_EQUALS));
                // the before-last operator is an upper bound operator on an ascending open scan so we fill the begin
                // key with the last value, and the end key with the before-last value
                buildScanExpression(ctx,
                    last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_START,
                    range_scan_type,
                    outer_row);  // lower bound on begin iterator key
                buildScanExpression(ctx,
                    before_last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_END,
                    range_scan_type,
                    outer_row);  // upper bound on end iterator key
                *begin_range_bound =
                    (last_dim_op == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
                *end_range_bound =
                    (before_last_dim_op == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
            } else {
                MOT_ASSERT((last_dim_op == JIT_WOC_LESS_THAN) || (last_dim_op == JIT_WOC_LESS_EQUALS));
                // the before-last operator is a lower bound operator on an ascending open scan so we fill the begin key
                // with the before-last value, and the end key with the last value
                buildScanExpression(ctx,
                    before_last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_START,
                    range_scan_type,
                    outer_row);  // lower bound on begin iterator key
                buildScanExpression(ctx,
                    last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_END,
                    range_scan_type,
                    outer_row);  // upper bound on end iterator key
                *begin_range_bound =
                    (before_last_dim_op == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
                *end_range_bound =
                    (last_dim_op == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
            }
        } else {
            if ((before_last_dim_op == JIT_WOC_LESS_THAN) || (before_last_dim_op == JIT_WOC_LESS_EQUALS)) {
                MOT_ASSERT((last_dim_op == JIT_WOC_GREATER_THAN) || (last_dim_op == JIT_WOC_GREATER_EQUALS));
                // the before-last operator is an upper bound operator on an descending open scan so we fill the begin
                // key with the last value, and the end key with the before-last value
                buildScanExpression(ctx,
                    before_last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_START,
                    range_scan_type,
                    outer_row);  // upper bound on begin iterator key
                buildScanExpression(ctx,
                    last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_END,
                    range_scan_type,
                    outer_row);  // lower bound on end iterator key
                *begin_range_bound =
                    (before_last_dim_op == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
                *end_range_bound =
                    (last_dim_op == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
            } else {
                MOT_ASSERT((last_dim_op == JIT_WOC_LESS_THAN) || (last_dim_op == JIT_WOC_LESS_EQUALS));
                // the before-last operator is a lower bound operator on an descending open scan so we fill the begin
                // key with the last value, and the end key with the before-last value
                buildScanExpression(ctx,
                    last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_START,
                    range_scan_type,
                    outer_row);  // upper bound on begin iterator key
                buildScanExpression(ctx,
                    before_last_expr,
                    max_arg,
                    JIT_RANGE_ITERATOR_END,
                    range_scan_type,
                    outer_row);  // lower bound on end iterator key
                *begin_range_bound =
                    (last_dim_op == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
                *end_range_bound =
                    (before_last_dim_op == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
            }
        }

        // now fill the rest as usual
        int first_zero_column = index_scan->_column_count;
        for (int i = first_zero_column; i < index_column_count; ++i) {
            int offset = index_column_offsets[i];
            int size = key_length[i];
            MOT_LOG_DEBUG(
                "Filling begin/end iterator pattern for missing pkey fields at offset %d, size %d", offset, size);
            AddFillKeyPattern(ctx, ascending ? 0x00 : 0xFF, offset, size, JIT_RANGE_ITERATOR_START, range_scan_type);
            AddFillKeyPattern(ctx, ascending ? 0xFF : 0x00, offset, size, JIT_RANGE_ITERATOR_END, range_scan_type);
        }

        AddAdjustKey(ctx,
            ascending ? 0x00 : 0xFF,
            JIT_RANGE_ITERATOR_START,
            range_scan_type);  // currently this is relevant only for secondary index searches
        AddAdjustKey(ctx,
            ascending ? 0xFF : 0x00,
            JIT_RANGE_ITERATOR_END,
            range_scan_type);  // currently this is relevant only for secondary index searches
    }
    return result;
}

static bool buildRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitRangeBoundMode* begin_range_bound, JitRangeBoundMode* end_range_bound,
    llvm::Value* outer_row)
{
    bool result = false;

    // if this is a point scan we generate two identical keys for the iterators
    if (index_scan->_scan_type == JIT_INDEX_SCAN_POINT) {
        result = buildPointScan(ctx, &index_scan->_search_exprs, max_arg, range_scan_type, outer_row);
        if (result) {
            AddCopyKey(ctx, range_scan_type);
            *begin_range_bound = JIT_RANGE_BOUND_INCLUDE;
            *end_range_bound = JIT_RANGE_BOUND_INCLUDE;
        }
    } else if (index_scan->_scan_type == JIT_INDEX_SCAN_CLOSED) {
        result = buildClosedRangeScan(ctx, index_scan, max_arg, range_scan_type, outer_row);
        if (result) {
            *begin_range_bound = JIT_RANGE_BOUND_INCLUDE;
            *end_range_bound = JIT_RANGE_BOUND_INCLUDE;
        }
    } else if (index_scan->_scan_type == JIT_INDEX_SCAN_SEMI_OPEN) {
        result = buildSemiOpenRangeScan(
            ctx, index_scan, max_arg, range_scan_type, begin_range_bound, end_range_bound, outer_row);
    } else if (index_scan->_scan_type == JIT_INDEX_SCAN_OPEN) {
        result = buildOpenRangeScan(
            ctx, index_scan, max_arg, range_scan_type, begin_range_bound, end_range_bound, outer_row);
    }
    return result;
}

static bool buildPrepareStateScan(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, llvm::Value* outer_row)
{
    JitRangeBoundMode begin_range_bound = JIT_RANGE_BOUND_NONE;
    JitRangeBoundMode end_range_bound = JIT_RANGE_BOUND_NONE;

    // emit code to check if state iterators are null
    JIT_IF_BEGIN(state_iterators_exist)
    llvm::Value* is_begin_itr_null = AddIsStateIteratorNull(ctx, JIT_RANGE_ITERATOR_START, range_scan_type);
    JIT_IF_EVAL(is_begin_itr_null)
    // prepare search keys
    if (!buildRangeScan(ctx, index_scan, max_arg, range_scan_type, &begin_range_bound, &end_range_bound, outer_row)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range select query: unsupported WHERE clause type");
        return false;
    }

    // search begin iterator and save it in execution state
    IssueDebugLog("Building search iterator from search key, and saving in execution state");
    llvm::Value* itr = buildSearchIterator(ctx, index_scan->_scan_direction, begin_range_bound, range_scan_type);
    AddSetStateIterator(ctx, itr, JIT_RANGE_ITERATOR_START, range_scan_type);

    // create end iterator and save it in execution state
    IssueDebugLog("Creating end iterator from end search key, and saving in execution state");
    itr = AddCreateEndIterator(ctx, index_scan->_scan_direction, end_range_bound, range_scan_type);
    AddSetStateIterator(ctx, itr, JIT_RANGE_ITERATOR_END, range_scan_type);
    if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
        AddResetStateLimitCounter(ctx);              // in case there is a limit clause
        AddResetStateRow(ctx, JIT_RANGE_SCAN_MAIN);  // in case this is a join query
        AddSetStateScanEndFlag(
            ctx, 0, JIT_RANGE_SCAN_MAIN);  // reset state flag (only once, and not repeatedly if row filter failed)
    }
    JIT_IF_END()

    return true;
}

static bool buildPrepareStateRow(JitLlvmCodeGenContext* ctx, MOT::AccessType access_mode, JitIndexScan* index_scan,
    int* max_arg, JitRangeScanType range_scan_type, llvm::BasicBlock* next_block)
{
    llvm::LLVMContext& context = ctx->_code_gen->context();

    MOT_LOG_DEBUG("Generating select code for stateful range select");
    llvm::Value* row = nullptr;

    // we start a new block so current block must end with terminator
    DEFINE_BLOCK(prepare_state_row_bb, ctx->m_jittedQuery);
    MOT_LOG_TRACE("Adding unconditional branch into prepare_state_row_bb from branch %s",
        ctx->_builder->GetInsertBlock()->getName().data());
    ctx->_builder->CreateBr(prepare_state_row_bb);
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
        MOT_LOG_TRACE("Added unconditional branch result:");
        ctx->_builder->GetInsertBlock()->back().print(llvm::errs());
        fprintf(stderr, "\n");
    }
    ctx->_builder->SetInsertPoint(prepare_state_row_bb);

    // check if state row is nullptr
    JIT_IF_BEGIN(test_state_row)
    IssueDebugLog("Checking if state row is nullptr");
    llvm::Value* res = AddIsStateRowNull(ctx, range_scan_type);
    JIT_IF_EVAL(res)
    IssueDebugLog("State row is nullptr, fetching from state iterators");

    // check if state scan ended
    JIT_IF_BEGIN(test_scan)
    IssueDebugLog("Checking if state scan ended");
    llvm::Value* res = AddIsStateScanEnd(ctx, index_scan->_scan_direction, range_scan_type);
    JIT_IF_EVAL(res)
    // fail scan block
    IssueDebugLog("Scan ended, raising internal state scan end flag");
    AddSetStateScanEndFlag(ctx, 1, range_scan_type);
    JIT_ELSE()
    // now get row from iterator (remember current block if filter fails)
    llvm::BasicBlock* get_row_from_itr_block = JIT_CURRENT_BLOCK();
    IssueDebugLog("State scan not ended - Retrieving row from iterator");
    row = AddGetRowFromStateIterator(ctx, access_mode, index_scan->_scan_direction, range_scan_type);

    // check if row was found
    JIT_IF_BEGIN(test_row_found)
    JIT_IF_EVAL_NOT(row)
    // row not found branch
    IssueDebugLog("Could not retrieve row from state iterator, raising internal state scan end flag");
    AddSetStateScanEndFlag(ctx, 1, range_scan_type);
    JIT_ELSE()
    // row found, check for additional filters, if not passing filter then go back to execute getRowFromStateIterator()
    if (!buildFilterRow(ctx, row, &index_scan->_filters, max_arg, get_row_from_itr_block)) {
        MOT_LOG_TRACE("Failed to generate jitted code for query: failed to build filter expressions for row");
        return false;
    }
    // row passed all filters, so save it in state outer row
    AddSetStateRow(ctx, row, range_scan_type);
    if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
        AddSetStateScanEndFlag(ctx, 0, range_scan_type);  // reset inner scan flag
    }
    JIT_IF_END()
    JIT_IF_END()
    JIT_IF_END()

    // cleanup state iterators if needed and return/jump to next block
    JIT_IF_BEGIN(state_scan_ended)
    IssueDebugLog("Checking if state scan ended flag was raised");
    llvm::Value* state_scan_end_flag = AddGetStateScanEndFlag(ctx, range_scan_type);
    JIT_IF_EVAL(state_scan_end_flag)
    IssueDebugLog(" State scan ended flag was raised, cleaning up iterators and reporting to user");
    AddDestroyStateIterators(ctx, range_scan_type);  // cleanup
    if ((range_scan_type == JIT_RANGE_SCAN_MAIN) || (next_block == nullptr)) {
        // either a main scan ended (simple range or outer loop of join),
        // or an inner range ended in an outer point join (so next block is null)
        // in either case let caller know scan ended and return appropriate value
        AddSetScanEnded(ctx, 1);  // no outer row, we are definitely done
        JIT_RETURN_CONST(MOT::RC_LOCAL_ROW_NOT_FOUND);
    } else {
        AddResetStateRow(ctx, JIT_RANGE_SCAN_MAIN);  // make sure a new row is fetched in outer loop
        ctx->_builder->CreateBr(next_block);         // jump back to outer loop
    }
    JIT_IF_END()

    return true;
}

static llvm::Value* buildPrepareStateScanRow(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan,
    JitRangeScanType range_scan_type, MOT::AccessType access_mode, int* max_arg, llvm::Value* outer_row,
    llvm::BasicBlock* next_block, llvm::BasicBlock** loop_block)
{
    // prepare stateful scan if not done so already
    if (!buildPrepareStateScan(ctx, index_scan, max_arg, range_scan_type, outer_row)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range JOIN query: unsupported %s WHERE clause type",
            range_scan_type == JIT_RANGE_SCAN_MAIN ? "outer" : "inner");
        return nullptr;
    }

    // mark position for later jump
    if (loop_block != nullptr) {
        *loop_block = llvm::BasicBlock::Create(ctx->_code_gen->context(), "fetch_outer_row_bb", ctx->m_jittedQuery);
        ctx->_builder->CreateBr(*loop_block);        // end current block
        ctx->_builder->SetInsertPoint(*loop_block);  // start new block
    }

    // fetch row for read
    if (!buildPrepareStateRow(ctx, access_mode, index_scan, max_arg, range_scan_type, next_block)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range JOIN query: failed to build search outer row block");
        return nullptr;
    }
    llvm::Value* row = AddGetStateRow(ctx, range_scan_type);
    return row;
}

static JitLlvmRuntimeCursor buildRangeCursor(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitIndexScanDirection index_scan_direction, llvm::Value* outer_row)
{
    JitLlvmRuntimeCursor result = {nullptr, nullptr};
    JitRangeBoundMode begin_range_bound = JIT_RANGE_BOUND_NONE;
    JitRangeBoundMode end_range_bound = JIT_RANGE_BOUND_NONE;
    if (!buildRangeScan(ctx, index_scan, max_arg, range_scan_type, &begin_range_bound, &end_range_bound, outer_row)) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for aggregate range JOIN query: unsupported %s-loop WHERE clause type",
            outer_row ? "inner" : "outer");
        return result;
    }

    // build range iterators
    result.begin_itr = buildSearchIterator(ctx, index_scan_direction, begin_range_bound, range_scan_type);
    result.end_itr = AddCreateEndIterator(ctx, index_scan_direction, end_range_bound, range_scan_type);  // forward scan

    return result;
}

static bool prepareAggregateAvg(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate)
{
    // although we already have this information in the aggregate descriptor, we still check again
    switch (aggregate->_func_id) {
        case INT8AVGFUNCOID:
        case NUMERICAVGFUNCOID:
            // the current_aggregate is a 2 numeric array
            AddPrepareAvgArray(ctx, NUMERICOID, 2);
            break;

        case INT4AVGFUNCOID:
        case INT2AVGFUNCOID:
        case 5537:  // int1 avg
            // the current_aggregate is a 2 int8 array
            AddPrepareAvgArray(ctx, INT8OID, 2);
            break;

        case 2104:  // float4
        case 2105:  // float8
            // the current_aggregate is a 3 float8 array
            AddPrepareAvgArray(ctx, FLOAT8OID, 3);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate AVG() operator function type: %d", aggregate->_func_id);
            return false;
    }

    return true;
}

static bool prepareAggregateSum(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate)
{
    switch (aggregate->_func_id) {
        case INT8SUMFUNCOID:
            // current sum in numeric, and value is int8, both can be null
            AddResetAggValue(ctx, NUMERICOID);
            break;

        case INT4SUMFUNCOID:
        case INT2SUMFUNCOID:
            // current aggregate is a int8, and value is int4, both can be null
            AddResetAggValue(ctx, INT8OID);
            break;

        case 2110:  // float4
            // current aggregate is a float4, and value is float4, both can **NOT** be null
            AddResetAggValue(ctx, FLOAT4OID);
            break;

        case 2111:  // float8
            // current aggregate is a float8, and value is float8, both can **NOT** be null
            AddResetAggValue(ctx, FLOAT8OID);
            break;

        case NUMERICSUMFUNCOID:
            AddResetAggValue(ctx, NUMERICOID);
            // current aggregate is a numeric, and value is numeric, both can **NOT** be null
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate SUM() operator function type: %d", aggregate->_func_id);
            return false;
    }
    return true;
}

static bool prepareAggregateMaxMin(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate)
{
    AddResetAggMaxMinNull(ctx);
    return true;
}

static bool prepareAggregateCount(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate)
{
    switch (aggregate->_func_id) {
        case 2147:  // int8inc_any
        case 2803:  // int8inc
            // current aggregate is int8, and can **NOT** be null
            AddResetAggValue(ctx, INT8OID);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate COUNT() operator function type: %d", aggregate->_func_id);
            return false;
    }
    return true;
}

static bool prepareDistinctSet(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate)
{
    // we need a hash-set according to the aggregated type (preferably but not necessarily linear-probing hash)
    // we use an opaque datum type, with a tailor-made hash-function and equals function
    AddPrepareDistinctSet(ctx, aggregate->_element_type);
    return true;
}

static bool prepareAggregate(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate)
{
    bool result = false;

    switch (aggregate->_aggreaget_op) {
        case JIT_AGGREGATE_AVG:
            result = prepareAggregateAvg(ctx, aggregate);
            break;

        case JIT_AGGREGATE_SUM:
            result = prepareAggregateSum(ctx, aggregate);
            break;

        case JIT_AGGREGATE_MAX:
        case JIT_AGGREGATE_MIN:
            result = prepareAggregateMaxMin(ctx, aggregate);
            break;

        case JIT_AGGREGATE_COUNT:
            result = prepareAggregateCount(ctx, aggregate);
            break;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "JIT Compile",
                "Cannot prepare for aggregation: invalid aggregate operator %d",
                (int)aggregate->_aggreaget_op);
            break;
    }

    if (result) {
        if (aggregate->_distinct) {
            result = prepareDistinctSet(ctx, aggregate);
        }
    }

    return result;
}

static llvm::Value* buildAggregateAvg(
    JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, llvm::Value* current_aggregate, llvm::Value* var_expr)
{
    llvm::Value* aggregate_expr = nullptr;
    switch (aggregate->_func_id) {
        case INT8AVGFUNCOID:
            // the current_aggregate is a 2 numeric array, and the var expression should evaluate to int8
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_int8_avg_accum, 0);
            break;

        case INT4AVGFUNCOID:
            // the current_aggregate is a 2 int8 array, and the var expression should evaluate to int4
            // we can save a palloc by ensuring "AggCheckCallContext(fcinfo, nullptr)" return true
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_int4_avg_accum, 0);
            break;

        case INT2AVGFUNCOID:
            // the current_aggregate is a 2 int8 array, and the var expression should evaluate to int2
            // we can save a palloc by ensuring "AggCheckCallContext(fcinfo, nullptr)" return true
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_int2_avg_accum, 0);
            break;

        case 5537:
            // the current_aggregate is a 2 int8 array, and the var expression should evaluate to int1
            // we can save a palloc by ensuring "AggCheckCallContext(fcinfo, nullptr)" return true
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_int1_avg_accum, 0);
            break;

        case 2104:  // float4
            // the current_aggregate is a 3 float8 array, and the var expression should evaluate to float4
            // the function computes 3 values: count, sum, square sum
            // we can save a palloc by ensuring "AggCheckCallContext(fcinfo, nullptr)" return true
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_float4_accum, 0);
            break;

        case 2105:  // float8
            // the current_aggregate is a 3 float8 array, and the var expression should evaluate to float8
            // the function computes 3 values: count, sum, square sum
            // we can save a palloc by ensuring "AggCheckCallContext(fcinfo, nullptr)" return true
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_float8_accum, 0);
            break;

        case NUMERICAVGFUNCOID:
            // the current_aggregate is a 2 numeric array, and the var expression should evaluate to numeric
            aggregate_expr =
                AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_numeric_avg_accum, 0);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate AVG() operator function type: %d", aggregate->_func_id);
            break;
    }

    return aggregate_expr;
}

static llvm::Value* buildAggregateSum(
    JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, llvm::Value* current_aggregate, llvm::Value* var_expr)
{
    llvm::Value* aggregate_expr = nullptr;
    switch (aggregate->_func_id) {
        case INT8SUMFUNCOID:
            // current sum in numeric, and next value is int8, both can be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_int8_sum, 0);
            break;

        case INT4SUMFUNCOID:
            // current aggregate is a int8, and value is int4, both can be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_int4_sum, 0);
            break;

        case INT2SUMFUNCOID:
            // current aggregate is a int8, and value is int3, both can be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_int2_sum, 0);
            break;

        case 2110:  // float4
            // current aggregate is a float4, and value is float4, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_float4pl, 0);
            break;

        case 2111:  // float8
            // current aggregate is a float4, and value is float4, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_float8pl, 0);
            break;

        case NUMERICSUMFUNCOID:
            // current aggregate is a numeric, and value is numeric, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_numeric_add, 0);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate SUM() operator function type: %d", aggregate->_func_id);
            break;
    }

    return aggregate_expr;
}

static llvm::Value* buildAggregateMax(
    JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, llvm::Value* current_aggregate, llvm::Value* var_expr)
{
    llvm::Value* aggregate_expr = nullptr;
    switch (aggregate->_func_id) {
        case INT8LARGERFUNCOID:
            // current aggregate is a int8, and value is int8, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_int8larger, 0);
            break;

        case INT4LARGERFUNCOID:
            // current aggregate is a int4, and value is int4, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_int4larger, 0);
            break;

        case INT2LARGERFUNCOID:
            // current aggregate is a int2, and value is int2, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_int2larger, 0);
            break;

        case 5538:
            // current aggregate is a int1, and value is int1, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_int1larger, 0);
            break;

        case 2119:  // float4larger
            // current aggregate is a float4, and value is float4, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_float4larger, 0);
            break;

        case 2120:  // float8larger
            // current aggregate is a float8, and value is float8, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_float8larger, 0);
            break;

        case NUMERICLARGERFUNCOID:
            // current aggregate is a numeric, and value is numeric, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_numeric_larger, 0);
            break;

        case 2126:
            // current aggregate is a timestamp, and value is timestamp, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_timestamp_larger, 0);
            break;

        case 2122:
            // current aggregate is a date, and value is date, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_date_larger, 0);
            break;

        case 2244:
            // current aggregate is a bpchar, and value is bpchar, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_bpchar_larger, 0);
            break;

        case 2129:
            // current aggregate is a text, and value is text, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_text_larger, 0);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate MAX() operator function type: %d", aggregate->_func_id);
            break;
    }

    return aggregate_expr;
}

static llvm::Value* buildAggregateMin(
    JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, llvm::Value* current_aggregate, llvm::Value* var_expr)
{
    llvm::Value* aggregate_expr = nullptr;
    switch (aggregate->_func_id) {
        case INT8SMALLERFUNCOID:
            // current sum is a int8, and value is int8, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_int8smaller, 0);
            break;

        case INT4SMALLERFUNCOID:
            // current aggregate is a int4, and value is int4, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_int4smaller, 0);
            break;

        case INT2SMALLERFUNCOID:
            // current aggregate is a int2, and value is int2, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_int2smaller, 0);
            break;

        case 2135:  // float4smaller
            // current aggregate is a float4, and value is float4, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_float4smaller, 0);
            break;

        case 2120:  // float8smaller
            // current aggregate is a float8, and value is float8, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_float8smaller, 0);
            break;

        case NUMERICSMALLERFUNCOID:
            // current aggregate is a numeric, and value is numeric, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_numeric_smaller, 0);
            break;

        case 2142:
            // current aggregate is a timestamp, and value is timestamp, both can **NOT** be null
            aggregate_expr =
                AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_timestamp_smaller, 0);
            break;

        case 2138:
            // current aggregate is a date, and value is date, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_date_smaller, 0);
            break;

        case 2245:
            // current aggregate is a bpchar, and value is bpchar, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_bpchar_smaller, 0);
            break;

        case 2145:
            // current aggregate is a text, and value is text, both can **NOT** be null
            aggregate_expr = AddExecBinaryOperator(ctx, current_aggregate, var_expr, ctx->_builtin_text_smaller, 0);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate MIN() operator function type: %d", aggregate->_func_id);
            break;
    }

    return aggregate_expr;
}

static llvm::Value* buildAggregateCount(
    JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, llvm::Value* count_aggregate)
{
    llvm::Value* aggregate_expr = nullptr;
    switch (aggregate->_func_id) {
        case 2147:  // int8inc_any
            // current aggregate is int8, and can **NOT** be null
            aggregate_expr = AddExecUnaryOperator(ctx, count_aggregate, ctx->_builtin_int8inc, 0);
            break;

        case 2803:  // int8inc
            // current aggregate is int8, and can **NOT** be null
            aggregate_expr = AddExecUnaryOperator(ctx, count_aggregate, ctx->_builtin_int8inc, 0);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate COUNT() operator function type: %d", aggregate->_func_id);
            break;
    }

    return aggregate_expr;
}

static bool buildAggregateMaxMin(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate, llvm::Value* var_expr)
{
    bool result = true;

    // we first check if the min/max value is null and if so just store the column value
    JIT_IF_BEGIN(test_max_min_value_null)
    llvm::Value* res = AddGetAggMaxMinIsNull(ctx);
    JIT_IF_EVAL(res)
    AddSetAggValue(ctx, var_expr);
    AddSetAggMaxMinNotNull(ctx);
    JIT_ELSE()
    // get the aggregated value and call the operator
    llvm::Value* current_aggregate = AddGetAggValue(ctx);
    switch (aggregate->_aggreaget_op) {
        case JIT_AGGREGATE_MAX:
            result = buildAggregateMax(ctx, aggregate, current_aggregate, var_expr);
            break;

        case JIT_AGGREGATE_MIN:
            result = buildAggregateMin(ctx, aggregate, current_aggregate, var_expr);
            break;

        default:
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "JIT Compile", "Invalid aggregate operator %d", (int)aggregate->_aggreaget_op);
            result = false;
            break;
    }
    JIT_IF_END()

    return result;
}

static bool buildAggregateTuple(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate, llvm::Value* var_expr)
{
    bool result = false;

    if ((aggregate->_aggreaget_op == JIT_AGGREGATE_MAX) || (aggregate->_aggreaget_op == JIT_AGGREGATE_MIN)) {
        result = buildAggregateMaxMin(ctx, aggregate, var_expr);
    } else {
        // get the aggregated value
        llvm::Value* current_aggregate = nullptr;
        if (aggregate->_aggreaget_op == JIT_AGGREGATE_AVG) {
            current_aggregate = AddLoadAvgArray(ctx);
        } else {
            current_aggregate = AddGetAggValue(ctx);
        }

        // the operators below take care of null inputs
        llvm::Value* aggregate_expr = nullptr;
        switch (aggregate->_aggreaget_op) {
            case JIT_AGGREGATE_AVG:
                aggregate_expr = buildAggregateAvg(ctx, aggregate, current_aggregate, var_expr);
                break;

            case JIT_AGGREGATE_SUM:
                aggregate_expr = buildAggregateSum(ctx, aggregate, current_aggregate, var_expr);
                break;

            case JIT_AGGREGATE_COUNT:
                aggregate_expr = buildAggregateCount(ctx, aggregate, current_aggregate);
                break;

            default:
                MOT_REPORT_ERROR(
                    MOT_ERROR_INTERNAL, "JIT Compile", "Invalid aggregate operator %d", (int)aggregate->_aggreaget_op);
                break;
        }

        // write back the sum to the aggregated value/array
        if (aggregate_expr != nullptr) {
            if (aggregate->_aggreaget_op == JIT_AGGREGATE_AVG) {
                AddSaveAvgArray(ctx, aggregate_expr);
            } else {
                AddSetAggValue(ctx, aggregate_expr);
            }
            result = true;
        } else {
            MOT_LOG_TRACE("Failed to generate aggregate AVG/SUM/COUNT code");
        }
    }

    return result;
}

static bool buildAggregateRow(
    JitLlvmCodeGenContext* ctx, JitAggregate* aggregate, llvm::Value* row, llvm::BasicBlock* next_block)
{
    bool result = false;

    // extract the aggregated column
    llvm::Value* table = (aggregate->_table == ctx->_table_info.m_table) ? ctx->table_value : ctx->inner_table_value;
    llvm::Value* expr = AddReadDatumColumn(ctx, table, row, aggregate->_table_column_id, 0);

    // we first check if we have DISTINCT modifier
    if (aggregate->_distinct) {
        // check row is distinct in state set (managed on the current jit context)
        // emit code to convert column to primitive data type, add value to distinct set and verify
        // if value is not distinct then do not use this row in aggregation
        JIT_IF_BEGIN(test_distinct_value)
        llvm::Value* res = AddInsertDistinctItem(ctx, aggregate->_element_type, expr);
        JIT_IF_EVAL_NOT(res)
        IssueDebugLog("Value is not distinct, skipping aggregated row");
        ctx->_builder->CreateBr(next_block);
        JIT_IF_END()
    }

    // aggregate row
    IssueDebugLog("Aggregating row into inner value");
    result = buildAggregateTuple(ctx, aggregate, expr);
    if (result) {
        // update number of rows processes
        buildIncrementRowsProcessed(ctx);
    }

    return result;
}

static void buildAggregateResult(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate)
{
    // in case of average we compute it when aggregate loop is done
    if (aggregate->_aggreaget_op == JIT_AGGREGATE_AVG) {
        llvm::Value* avg_value = AddComputeAvgFromArray(
            ctx, aggregate->_avg_element_type);  // we infer this during agg op analysis, but don't save it...
        AddWriteTupleDatum(ctx, 0, avg_value);   // we alway aggregate to slot tuple 0
    } else {
        llvm::Value* count_value = AddGetAggValue(ctx);
        AddWriteTupleDatum(ctx, 0, count_value);  // we alway aggregate to slot tuple 0
    }

    // we take the opportunity to cleanup as well
    if (aggregate->_distinct) {
        AddDestroyDistinctSet(ctx, aggregate->_element_type);
    }
}

static void buildCheckLimit(JitLlvmCodeGenContext* ctx, int limit_count)
{
    // if a limit clause exists, then increment limit counter and check if reached limit
    if (limit_count > 0) {
        AddIncrementStateLimitCounter(ctx);
        JIT_IF_BEGIN(limit_count_reached)
        llvm::Value* current_limit_count = AddGetStateLimitCounter(ctx);
        llvm::Value* limit_count_inst = JIT_CONST(limit_count);
        JIT_IF_EVAL_CMP(current_limit_count, limit_count_inst, JIT_ICMP_EQ);
        IssueDebugLog("Reached limit specified in limit clause, raising internal state scan end flag");
        // cleanup and signal scan ended (it is safe to cleanup even if not initialized, so we cleanup everything)
        AddDestroyStateIterators(ctx, JIT_RANGE_SCAN_MAIN);
        AddDestroyStateIterators(ctx, JIT_RANGE_SCAN_INNER);
        AddSetScanEnded(ctx, 1);
        JIT_IF_END()
    }
}

static bool selectJoinRows(
    JitLlvmCodeGenContext* ctx, llvm::Value* outer_row_copy, llvm::Value* inner_row, JitJoinPlan* plan, int* max_arg)
{
    // select inner and outer row expressions into result tuple (no aggregate because aggregate is not stateful)
    IssueDebugLog("Retrieved row from state iterator, beginning to select columns into result tuple");
    if (!selectRowColumns(ctx, outer_row_copy, &plan->_select_exprs, max_arg, JIT_RANGE_SCAN_MAIN)) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for Inner Point JOIN query: failed to select outer row expressions");
        return false;
    }
    if (!selectRowColumns(ctx, inner_row, &plan->_select_exprs, max_arg, JIT_RANGE_SCAN_INNER)) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for Inner Point JOIN query: failed to select outer row expressions");
        return false;
    }
    return true;
}

static JitContext* JitUpdateCodegen(Query* query, const char* query_string, JitUpdatePlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT update at thread %p", (void*)pthread_self());

    GsCodeGen* code_gen = SetupCodegenEnv();
    if (code_gen == nullptr) {
        return nullptr;
    }
    GsCodeGen::LlvmBuilder builder(code_gen->context());

    JitLlvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_query._table;
    if (!InitCodeGenContext(&cg_ctx, code_gen, &builder, table, table->GetPrimaryIndex())) {
        return nullptr;
    }
    JitLlvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedUpdate");
    IssueDebugLog("Starting execution of jitted UPDATE");

    // update is not allowed if we reached soft memory limit
    buildIsSoftMemoryLimitReached(ctx);

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // begin the WHERE clause (this is a point query
    int max_arg = 0;
    if (!buildPointScan(ctx, &plan->_query._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for update query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // fetch row for writing
    MOT_LOG_DEBUG("Generating update code for point query");
    llvm::Value* row = buildSearchRow(ctx, MOT::AccessType::WR, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, &plan->_query._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for update query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // prepare a bitmap array
    IssueDebugLog("Resetting bitmap set for incremental redo");
    AddResetBitmapSet(ctx);

    // now begin updating columns
    IssueDebugLog("Updating row columns");
    if (!writeRowColumns(ctx, row, &plan->_update_exprs, &max_arg, true)) {
        MOT_LOG_TRACE("Failed to generate jitted code for update query: failed to process target entry");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // add code:
    // MOT::Rc rc = writeRow(row, bmp);
    // if (rc != MOT::RC_OK)
    //   return rc;
    IssueDebugLog("Writing row");
    buildWriteRow(ctx, row, true, nullptr);

    // the next call will be executed only if the previous call to writeRow succeeded
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(llvm::ConstantInt::get(ctx->INT32_T, (int)MOT::RC_OK, true));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_UPDATE);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitRangeUpdateCodegen(Query* query, const char* query_string, JitRangeUpdatePlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT range update at thread %p", (void*)pthread_self());

    GsCodeGen* code_gen = SetupCodegenEnv();
    if (code_gen == nullptr) {
        return nullptr;
    }
    GsCodeGen::LlvmBuilder builder(code_gen->context());

    JitLlvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_index_scan._table;
    if (!InitCodeGenContext(&cg_ctx, code_gen, &builder, table, table->GetPrimaryIndex())) {
        return nullptr;
    }
    JitLlvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedRangeUpdate");
    IssueDebugLog("Starting execution of jitted range UPDATE");

    // update is not allowed if we reached soft memory limit
    buildIsSoftMemoryLimitReached(ctx);

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // begin the WHERE clause
    int max_arg = 0;
    MOT_LOG_DEBUG("Generating range cursor for range UPDATE query");
    JitLlvmRuntimeCursor cursor =
        buildRangeCursor(ctx, &plan->_index_scan, &max_arg, JIT_RANGE_SCAN_MAIN, JIT_INDEX_SCAN_FORWARD, nullptr);
    if (cursor.begin_itr == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for range UPDATE query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddResetBitmapSet(ctx);

    JIT_WHILE_BEGIN(cursor_loop)
    llvm::Value* res = AddIsScanEnd(ctx, JIT_INDEX_SCAN_FORWARD, &cursor, JIT_RANGE_SCAN_MAIN);
    JIT_WHILE_EVAL_NOT(res)
    llvm::Value* row = buildGetRowFromIterator(
        ctx, JIT_WHILE_POST_BLOCK(), MOT::AccessType::WR, JIT_INDEX_SCAN_FORWARD, &cursor, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, &plan->_index_scan._filters, &max_arg, JIT_WHILE_COND_BLOCK())) {
        MOT_LOG_TRACE("Failed to generate jitted code for range UPDATE query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // now begin updating columns
    IssueDebugLog("Updating row columns");
    if (!writeRowColumns(ctx, row, &plan->_update_exprs, &max_arg, true)) {
        MOT_LOG_TRACE("Failed to generate jitted code for update query: failed to process target entry");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    IssueDebugLog("Writing row");
    buildWriteRow(ctx, row, false, &cursor);

    // the next call will be executed only if the previous call to writeRow succeeded
    buildIncrementRowsProcessed(ctx);

    // reset bitmap for next loop
    AddResetBitmapSet(ctx);
    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of range update loop");
    AddDestroyCursor(ctx, &cursor);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(llvm::ConstantInt::get(ctx->INT32_T, (int)MOT::RC_OK, true));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_RANGE_UPDATE);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitInsertCodegen(Query* query, const char* query_string, JitInsertPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT insert at thread %p", (void*)pthread_self());

    GsCodeGen* code_gen = SetupCodegenEnv();
    if (code_gen == nullptr) {
        return nullptr;
    }
    GsCodeGen::LlvmBuilder builder(code_gen->context());

    JitLlvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_table;
    if (!InitCodeGenContext(&cg_ctx, code_gen, &builder, table, table->GetPrimaryIndex())) {
        return nullptr;
    }
    JitLlvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedInsert");
    IssueDebugLog("Starting execution of jitted INSERT");

    // insert is not allowed if we reached soft memory limit
    buildIsSoftMemoryLimitReached(ctx);

    // create new row and bitmap set
    llvm::Value* row = buildCreateNewRow(ctx);

    // set row null bits
    IssueDebugLog("Setting row null bits before insert");
    AddSetRowNullBits(ctx, row);

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    IssueDebugLog("Setting row columns");
    int max_arg = 0;
    if (!writeRowColumns(ctx, row, &plan->_insert_exprs, &max_arg, false)) {
        MOT_LOG_TRACE("Failed to generate jitted code for insert query: failed to process target entry");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    IssueDebugLog("Inserting row");
    buildInsertRow(ctx, row);

    // the next call will be executed only if the previous call to writeRow succeeded
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(llvm::ConstantInt::get(ctx->INT32_T, (int)MOT::RC_OK, true));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_INSERT);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitDeleteCodegen(Query* query, const char* query_string, JitDeletePlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT delete at thread %p", (void*)pthread_self());

    GsCodeGen* code_gen = SetupCodegenEnv();
    if (code_gen == nullptr) {
        return nullptr;
    }
    GsCodeGen::LlvmBuilder builder(code_gen->context());

    JitLlvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_query._table;
    if (!InitCodeGenContext(&cg_ctx, code_gen, &builder, table, table->GetPrimaryIndex())) {
        return nullptr;
    }
    JitLlvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedDelete");
    IssueDebugLog("Starting execution of jitted DELETE");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // begin the WHERE clause
    int max_arg = 0;
    if (!buildPointScan(ctx, &plan->_query._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for DELETE query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // fetch row for delete
    llvm::Value* row = buildSearchRow(ctx, MOT::AccessType::DEL, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, &plan->_query._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for DELETE query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    IssueDebugLog("Deleting row");
    // row is already cached in concurrency control module, so we do not need to provide an argument
    buildDeleteRow(ctx);

    // the next call will be executed only if the previous call to deleteRow succeeded
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(llvm::ConstantInt::get(ctx->INT32_T, (int)MOT::RC_OK, true));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_DELETE);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitSelectCodegen(const Query* query, const char* query_string, JitSelectPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT select at thread %p", (void*)pthread_self());

    GsCodeGen* code_gen = SetupCodegenEnv();
    if (code_gen == nullptr) {
        return nullptr;
    }
    GsCodeGen::LlvmBuilder builder(code_gen->context());

    JitLlvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_query._table;
    if (!InitCodeGenContext(&cg_ctx, code_gen, &builder, table, table->GetPrimaryIndex())) {
        return nullptr;
    }
    JitLlvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedSelect");
    IssueDebugLog("Starting execution of jitted SELECT");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // begin the WHERE clause
    int max_arg = 0;
    if (!buildPointScan(ctx, &plan->_query._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for SELECT query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // fetch row for read
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, &plan->_query._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for SELECT query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // now begin selecting columns into result
    IssueDebugLog("Selecting columns into result");
    if (!selectRowColumns(ctx, row, &plan->_select_exprs, &max_arg, JIT_RANGE_SCAN_MAIN)) {
        MOT_LOG_TRACE("Failed to generate jitted code for insert query: failed to process target entry");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);

    // update number of rows processed
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended (this is a point query)
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(llvm::ConstantInt::get(ctx->INT32_T, (int)MOT::RC_OK, true));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_SELECT);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range SELECT query with a possible LIMIT clause. */
static JitContext* JitRangeSelectCodegen(const Query* query, const char* query_string, JitRangeSelectPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT select at thread %p", (void*)pthread_self());

    GsCodeGen* code_gen = SetupCodegenEnv();
    if (code_gen == nullptr) {
        return nullptr;
    }
    GsCodeGen::LlvmBuilder builder(code_gen->context());

    JitLlvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_index_scan._table;
    int index_id = plan->_index_scan._index_id;
    if (!InitCodeGenContext(&cg_ctx, code_gen, &builder, table, table->GetIndex(index_id))) {
        return nullptr;
    }
    JitLlvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedRangeSelect");
    IssueDebugLog("Starting execution of jitted range SELECT");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // prepare stateful scan if not done so already, if no row exists then emit code to return from function
    int max_arg = 0;
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* row = buildPrepareStateScanRow(
        ctx, &plan->_index_scan, JIT_RANGE_SCAN_MAIN, access_mode, &max_arg, nullptr, nullptr, nullptr);
    if (row == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for range select query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // select inner and outer row expressions into result tuple (no aggregate because aggregate is not stateful)
    IssueDebugLog("Retrieved row from state iterator, beginning to select columns into result tuple");
    if (!selectRowColumns(ctx, row, &plan->_select_exprs, &max_arg, JIT_RANGE_SCAN_MAIN)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range SELECT query: failed to select row expressions");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }
    AddExecStoreVirtualTuple(ctx);
    buildIncrementRowsProcessed(ctx);

    // make sure that next iteration find an empty state row, so scan makes a progress
    AddResetStateRow(ctx, JIT_RANGE_SCAN_MAIN);

    // if a limit clause exists, then increment limit counter and check if reached limit
    buildCheckLimit(ctx, plan->_limit_count);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // return success from calling function
    builder.CreateRet(llvm::ConstantInt::get(ctx->INT32_T, (int)MOT::RC_OK, true));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_RANGE_SELECT);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range SELECT query with aggregator. */
static JitContext* JitAggregateRangeSelectCodegen(
    const Query* query, const char* query_string, JitRangeSelectPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT aggregate range select at thread %p", (void*)pthread_self());

    GsCodeGen* code_gen = SetupCodegenEnv();
    if (code_gen == nullptr) {
        return nullptr;
    }
    GsCodeGen::LlvmBuilder builder(code_gen->context());

    MOT::Table* table = plan->_index_scan._table;
    int index_id = plan->_index_scan._index_id;
    JitLlvmCodeGenContext cg_ctx = {0};
    if (!InitCodeGenContext(&cg_ctx, code_gen, &builder, table, table->GetIndex(index_id))) {
        return nullptr;
    }
    JitLlvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedAggregateRangeSelect");
    IssueDebugLog("Starting execution of jitted aggregate range SELECT");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
    AddExecClearTuple(ctx);

    // prepare for aggregation
    prepareAggregate(ctx, &plan->_aggregate);
    AddResetStateLimitCounter(ctx);

    // pay attention: aggregated range scan is not stateful, since we scan all tuples in one call
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;

    // begin the WHERE clause
    int max_arg = 0;
    JitIndexScanDirection index_scan_direction = JIT_INDEX_SCAN_FORWARD;

    // build range iterators
    MOT_LOG_DEBUG("Generating range cursor for range SELECT query");
    JitLlvmRuntimeCursor cursor =
        buildRangeCursor(ctx, &plan->_index_scan, &max_arg, JIT_RANGE_SCAN_MAIN, index_scan_direction, nullptr);
    if (cursor.begin_itr == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range SELECT query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    JIT_WHILE_BEGIN(cursor_aggregate_loop)
    llvm::Value* res = AddIsScanEnd(ctx, index_scan_direction, &cursor, JIT_RANGE_SCAN_MAIN);
    JIT_WHILE_EVAL_NOT(res)
    llvm::Value* row = buildGetRowFromIterator(
        ctx, JIT_WHILE_POST_BLOCK(), access_mode, JIT_INDEX_SCAN_FORWARD, &cursor, JIT_RANGE_SCAN_MAIN);

    // check for additional filters, if not try to fetch next row
    if (!buildFilterRow(ctx, row, &plan->_index_scan._filters, &max_arg, JIT_WHILE_COND_BLOCK())) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range SELECT query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // aggregate into tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
    // if row disqualified due to DISTINCT operator then go back to loop test block
    buildAggregateRow(ctx, &plan->_aggregate, row, JIT_WHILE_COND_BLOCK());

    // if a limit clause exists, then increment limit counter and check if reached limit
    if (plan->_limit_count > 0) {
        AddIncrementStateLimitCounter(ctx);
        JIT_IF_BEGIN(limit_count_reached)
        llvm::Value* current_limit_count = AddGetStateLimitCounter(ctx);
        JIT_IF_EVAL_CMP(current_limit_count, JIT_CONST(plan->_limit_count), JIT_ICMP_EQ);
        IssueDebugLog("Reached limit specified in limit clause, raising internal state scan end flag");
        JIT_WHILE_BREAK()  // break from loop
        JIT_IF_END()
    }
    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of aggregate range select loop");
    AddDestroyCursor(ctx, &cursor);

    // wrap up aggregation and write to result tuple
    buildAggregateResult(ctx, &plan->_aggregate);

    // store the result tuple
    AddExecStoreVirtualTuple(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended (this is an aggregate loop)
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(llvm::ConstantInt::get(ctx->INT32_T, (int)MOT::RC_OK, true));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_AGGREGATE_RANGE_SELECT);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitPointJoinCodegen(const Query* query, const char* query_string, JitJoinPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT Point JOIN query at thread %p", (void*)pthread_self());

    GsCodeGen* code_gen = SetupCodegenEnv();
    if (code_gen == nullptr) {
        return nullptr;
    }
    GsCodeGen::LlvmBuilder builder(code_gen->context());

    JitLlvmCodeGenContext cg_ctx = {0};
    MOT::Table* outer_table = plan->_outer_scan._table;
    MOT::Index* outer_index = outer_table->GetIndex(plan->_outer_scan._index_id);
    MOT::Table* inner_table = plan->_inner_scan._table;
    MOT::Index* inner_index = inner_table->GetIndex(plan->_inner_scan._index_id);
    if (!InitCodeGenContext(&cg_ctx, code_gen, &builder, outer_table, outer_index, inner_table, inner_index)) {
        return nullptr;
    }
    JitLlvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedPointJoin");
    IssueDebugLog("Starting execution of jitted Point JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // search the outer row
    int max_arg = 0;
    if (!buildPointScan(ctx, &plan->_outer_scan._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Point JOIN query: unsupported outer WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // fetch row for read
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* outer_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, outer_row, &plan->_outer_scan._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Point JOIN query: unsupported outer scan filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // before we move on to inner point scan, we save the outer row in a safe copy (but for that purpose we need to save
    // row in outer scan state)
    AddSetStateRow(ctx, outer_row, JIT_RANGE_SCAN_MAIN);
    AddCopyOuterStateRow(ctx);

    // now search the inner row
    if (!buildPointScan(ctx, &plan->_inner_scan._search_exprs, &max_arg, JIT_RANGE_SCAN_INNER, outer_row)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Point JOIN query: unsupported inner WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }
    llvm::Value* inner_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_INNER);

    // check for additional filters
    if (!buildFilterRow(ctx, inner_row, &plan->_inner_scan._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Point JOIN query: unsupported inner scan filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    llvm::Value* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now begin selecting columns into result
    if (!selectJoinRows(ctx, outer_row_copy, inner_row, plan, &max_arg)) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for Point JOIN query: failed to select row columns into result tuple");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);

    // update number of rows processed
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended (this is a point query)
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(llvm::ConstantInt::get(ctx->INT32_T, (int)MOT::RC_OK, true));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_POINT_JOIN);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range JOIN query with outer point query and a possible LIMIT clause but without
 * aggregation. */
static JitContext* JitPointOuterJoinCodegen(const Query* query, const char* query_string, JitJoinPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT Outer Point JOIN query at thread %p", (void*)pthread_self());

    int max_arg = 0;
    GsCodeGen* code_gen = SetupCodegenEnv();
    if (code_gen == nullptr) {
        return nullptr;
    }
    GsCodeGen::LlvmBuilder builder(code_gen->context());

    JitLlvmCodeGenContext cg_ctx = {0};
    MOT::Table* outer_table = plan->_outer_scan._table;
    MOT::Index* outer_index = outer_table->GetIndex(plan->_outer_scan._index_id);
    MOT::Table* inner_table = plan->_inner_scan._table;
    MOT::Index* inner_index = inner_table->GetIndex(plan->_inner_scan._index_id);
    if (!InitCodeGenContext(&cg_ctx, code_gen, &builder, outer_table, outer_index, inner_table, inner_index)) {
        return nullptr;
    }
    JitLlvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedOuterPointJoin");
    IssueDebugLog("Starting execution of jitted Outer Point JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // we first check if outer state row was already searched
    llvm::Value* outer_row_copy = AddGetOuterStateRowCopy(ctx);
    JIT_IF_BEGIN(check_outer_row_ready)
    JIT_IF_EVAL_NOT(outer_row_copy)
    // search the outer row
    if (!buildPointScan(ctx, &plan->_outer_scan._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Outer Point JOIN query: unsupported outer WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // fetch row for read
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* outer_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, outer_row, &plan->_outer_scan._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Outer Point JOIN query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // before we move on to inner range scan, we save the outer row in a safe copy (but for that purpose we need to save
    // row in outer scan state)
    AddSetStateRow(ctx, outer_row, JIT_RANGE_SCAN_MAIN);
    AddCopyOuterStateRow(ctx);
    outer_row_copy = AddGetOuterStateRowCopy(ctx);  // must get copy again, otherwise it is null
    JIT_IF_END()

    // now prepare inner scan if needed, if no row was found then emit code to return from function (since outer scan is
    // a point query)
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* inner_row = buildPrepareStateScanRow(
        ctx, &plan->_inner_scan, JIT_RANGE_SCAN_INNER, access_mode, &max_arg, outer_row_copy, nullptr, nullptr);
    if (inner_row == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for Outer Point JOIN query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now begin selecting columns into result
    if (!selectJoinRows(ctx, outer_row_copy, inner_row, plan, &max_arg)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Outer Point JOIN query: failed to select row columns into "
                      "result tuple");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);

    // update number of rows processed
    buildIncrementRowsProcessed(ctx);

    // clear inner row for next iteration
    AddResetStateRow(ctx, JIT_RANGE_SCAN_INNER);

    // if a limit clause exists, then increment limit counter and check if reached limit
    buildCheckLimit(ctx, plan->_limit_count);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // return success from calling function
    builder.CreateRet(llvm::ConstantInt::get(ctx->INT32_T, (int)MOT::RC_OK, true));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_RANGE_JOIN);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range JOIN query with inner point query and a possible LIMIT clause but without
 * aggregation. */
static JitContext* JitPointInnerJoinCodegen(const Query* query, const char* query_string, JitJoinPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT Inner Point JOIN at thread %p", (void*)pthread_self());

    GsCodeGen* code_gen = SetupCodegenEnv();
    if (code_gen == nullptr) {
        return nullptr;
    }
    GsCodeGen::LlvmBuilder builder(code_gen->context());

    JitLlvmCodeGenContext cg_ctx = {0};
    MOT::Table* outer_table = plan->_outer_scan._table;
    MOT::Index* outer_index = outer_table->GetIndex(plan->_outer_scan._index_id);
    MOT::Table* inner_table = plan->_inner_scan._table;
    MOT::Index* inner_index = inner_table->GetIndex(plan->_inner_scan._index_id);
    if (!InitCodeGenContext(&cg_ctx, code_gen, &builder, outer_table, outer_index, inner_table, inner_index)) {
        return nullptr;
    }
    JitLlvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedInnerPointJoin");
    IssueDebugLog("Starting execution of jitted inner point JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // prepare stateful scan if not done so already, if row not found then emit code to return from function (since this
    // is an outer scan)
    int max_arg = 0;
    llvm::BasicBlock* fetch_outer_row_bb = nullptr;
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* outer_row = buildPrepareStateScanRow(
        ctx, &plan->_outer_scan, JIT_RANGE_SCAN_MAIN, access_mode, &max_arg, nullptr, nullptr, &fetch_outer_row_bb);
    if (outer_row == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for Inner Point JOIN query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // before we move on to inner scan, we save the outer row in a safe copy
    AddCopyOuterStateRow(ctx);

    // now search the inner row
    if (!buildPointScan(ctx, &plan->_inner_scan._search_exprs, &max_arg, JIT_RANGE_SCAN_INNER, outer_row)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Inner Point JOIN query: unsupported inner WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }
    llvm::Value* inner_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_INNER);

    // check for additional filters
    if (!buildFilterRow(ctx, inner_row, &plan->_inner_scan._filters, &max_arg, fetch_outer_row_bb)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Inner Point JOIN query: unsupported inner scan filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    llvm::Value* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now begin selecting columns into result
    if (!selectJoinRows(ctx, outer_row_copy, inner_row, plan, &max_arg)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Inner Point JOIN query: failed to select row columns into "
                      "result tuple");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);
    buildIncrementRowsProcessed(ctx);

    // if a limit clause exists, then increment limit counter and check if reached limit
    buildCheckLimit(ctx, plan->_limit_count);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // return success from calling function
    builder.CreateRet(llvm::ConstantInt::get(ctx->INT32_T, (int)MOT::RC_OK, true));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_RANGE_JOIN);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range JOIN query with a possible LIMIT clause but without aggregation. */
static JitContext* JitRangeJoinCodegen(const Query* query, const char* query_string, JitJoinPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT range JOIN at thread %p", (void*)pthread_self());

    GsCodeGen* code_gen = SetupCodegenEnv();
    if (code_gen == nullptr) {
        return nullptr;
    }
    GsCodeGen::LlvmBuilder builder(code_gen->context());

    JitLlvmCodeGenContext cg_ctx = {0};
    MOT::Table* outer_table = plan->_outer_scan._table;
    MOT::Index* outer_index = outer_table->GetIndex(plan->_outer_scan._index_id);
    MOT::Table* inner_table = plan->_inner_scan._table;
    MOT::Index* inner_index = inner_table->GetIndex(plan->_inner_scan._index_id);
    if (!InitCodeGenContext(&cg_ctx, code_gen, &builder, outer_table, outer_index, inner_table, inner_index)) {
        return nullptr;
    }
    JitLlvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedRangeJoin");
    IssueDebugLog("Starting execution of jitted range JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // prepare stateful scan if not done so already
    int max_arg = 0;
    llvm::BasicBlock* fetch_outer_row_bb = nullptr;
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* outer_row = buildPrepareStateScanRow(
        ctx, &plan->_outer_scan, JIT_RANGE_SCAN_MAIN, access_mode, &max_arg, nullptr, nullptr, &fetch_outer_row_bb);
    if (outer_row == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for Range JOIN query: unsupported outer WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // before we move on to inner scan, we save the outer row in a safe copy
    AddCopyOuterStateRow(ctx);

    // now prepare inner scan if needed
    llvm::Value* inner_row = buildPrepareStateScanRow(
        ctx, &plan->_inner_scan, JIT_RANGE_SCAN_INNER, access_mode, &max_arg, outer_row, fetch_outer_row_bb, nullptr);
    if (inner_row == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for Range JOIN query: unsupported inner WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    llvm::Value* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now begin selecting columns into result
    if (!selectJoinRows(ctx, outer_row_copy, inner_row, plan, &max_arg)) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for Range JOIN query: failed to select row columns into result tuple");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);
    buildIncrementRowsProcessed(ctx);

    // clear inner row for next iteration
    AddResetStateRow(ctx, JIT_RANGE_SCAN_INNER);

    // if a limit clause exists, then increment limit counter and check if reached limit
    buildCheckLimit(ctx, plan->_limit_count);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // return success from calling function
    builder.CreateRet(llvm::ConstantInt::get(ctx->INT32_T, (int)MOT::RC_OK, true));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_RANGE_JOIN);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range JOIN query with an aggregator. */
static JitContext* JitAggregateRangeJoinCodegen(Query* query, const char* query_string, JitJoinPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT aggregate range JOIN at thread %p", (void*)pthread_self());

    GsCodeGen* code_gen = SetupCodegenEnv();
    if (code_gen == nullptr) {
        return nullptr;
    }
    GsCodeGen::LlvmBuilder builder(code_gen->context());

    JitLlvmCodeGenContext cg_ctx = {0};
    MOT::Table* outer_table = plan->_outer_scan._table;
    MOT::Index* outer_index = outer_table->GetIndex(plan->_outer_scan._index_id);
    MOT::Table* inner_table = plan->_inner_scan._table;
    MOT::Index* inner_index = inner_table->GetIndex(plan->_inner_scan._index_id);
    if (!InitCodeGenContext(&cg_ctx, code_gen, &builder, outer_table, outer_index, inner_table, inner_index)) {
        return nullptr;
    }
    JitLlvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedAggregateRangeJoin");
    IssueDebugLog("Starting execution of jitted aggregate range JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
    AddExecClearTuple(ctx);

    // prepare for aggregation
    prepareAggregate(ctx, &plan->_aggregate);
    AddResetStateLimitCounter(ctx);

    // pay attention: aggregated range scan is not stateful, since we scan all tuples in one call
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;

    // begin the WHERE clause
    int max_arg = 0;

    // build range iterators
    MOT_LOG_DEBUG("Generating outer loop cursor for range JOIN query");
    JitLlvmRuntimeCursor outer_cursor =
        buildRangeCursor(ctx, &plan->_outer_scan, &max_arg, JIT_RANGE_SCAN_MAIN, JIT_INDEX_SCAN_FORWARD, nullptr);
    if (outer_cursor.begin_itr == nullptr) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for aggregate range JOIN query: unsupported outer-loop WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    JIT_WHILE_BEGIN(cursor_aggregate_outer_loop)
    llvm::BasicBlock* endOuterLoopBlock = JIT_WHILE_POST_BLOCK();
    llvm::Value* res = AddIsScanEnd(ctx, JIT_INDEX_SCAN_FORWARD, &outer_cursor, JIT_RANGE_SCAN_MAIN);
    JIT_WHILE_EVAL_NOT(res)
    llvm::Value* outer_row = buildGetRowFromIterator(
        ctx, JIT_WHILE_POST_BLOCK(), access_mode, JIT_INDEX_SCAN_FORWARD, &outer_cursor, JIT_RANGE_SCAN_MAIN);

    // check for additional filters, if not try to fetch next row
    if (!buildFilterRow(ctx, outer_row, &plan->_outer_scan._filters, &max_arg, JIT_WHILE_COND_BLOCK())) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range JOIN query: unsupported outer-loop filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // before we move on to inner scan, we save the outer row in a safe copy (but for that purpose we need to save row
    // in outer scan state)
    AddSetStateRow(ctx, outer_row, JIT_RANGE_SCAN_MAIN);
    AddCopyOuterStateRow(ctx);

    // now build the inner loop
    MOT_LOG_DEBUG("Generating inner loop cursor for range JOIN query");
    JitLlvmRuntimeCursor inner_cursor =
        buildRangeCursor(ctx, &plan->_inner_scan, &max_arg, JIT_RANGE_SCAN_INNER, JIT_INDEX_SCAN_FORWARD, outer_row);
    if (inner_cursor.begin_itr == nullptr) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for aggregate range JOIN query: unsupported inner-loop WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    JIT_WHILE_BEGIN(cursor_aggregate_inner_loop)
    llvm::Value* res = AddIsScanEnd(ctx, JIT_INDEX_SCAN_FORWARD, &inner_cursor, JIT_RANGE_SCAN_INNER);
    JIT_WHILE_EVAL_NOT(res)
    llvm::Value* inner_row = buildGetRowFromIterator(
        ctx, JIT_WHILE_POST_BLOCK(), access_mode, JIT_INDEX_SCAN_FORWARD, &inner_cursor, JIT_RANGE_SCAN_INNER);

    // check for additional filters, if not try to fetch next row
    if (!buildFilterRow(ctx, inner_row, &plan->_inner_scan._filters, &max_arg, JIT_WHILE_COND_BLOCK())) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range JOIN query: unsupported inner-loop filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // aggregate into tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
    // find out to which table the aggreate expression refers, and aggregate it
    // if row disqualified due to DISTINCT operator then go back to inner loop test block
    if (plan->_aggregate._table == ctx->_inner_table_info.m_table) {
        buildAggregateRow(ctx, &plan->_aggregate, inner_row, JIT_WHILE_COND_BLOCK());
    } else {
        // retrieve the safe copy of the outer row
        llvm::Value* outer_row_copy = AddGetOuterStateRowCopy(ctx);
        buildAggregateRow(ctx, &plan->_aggregate, outer_row_copy, JIT_WHILE_COND_BLOCK());
    }

    // if a limit clause exists, then increment limit counter and check if reached limit
    if (plan->_limit_count > 0) {
        AddIncrementStateLimitCounter(ctx);
        JIT_IF_BEGIN(limit_count_reached)
        llvm::Value* current_limit_count = AddGetStateLimitCounter(ctx);
        JIT_IF_EVAL_CMP(current_limit_count, JIT_CONST(plan->_limit_count), JIT_ICMP_EQ);
        IssueDebugLog("Reached limit specified in limit clause, raising internal state scan end flag");
        AddDestroyCursor(ctx, &outer_cursor);
        AddDestroyCursor(ctx, &inner_cursor);
        ctx->_builder->CreateBr(endOuterLoopBlock);  // break from inner outside of outer loop
        JIT_IF_END()
    }
    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of inner loop");
    AddDestroyCursor(ctx, &inner_cursor);

    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of outer loop");
    AddDestroyCursor(ctx, &outer_cursor);

    // wrap up aggregation and write to result tuple
    buildAggregateResult(ctx, &plan->_aggregate);

    // store the result tuple
    AddExecStoreVirtualTuple(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(llvm::ConstantInt::get(ctx->INT32_T, (int)MOT::RC_OK, true));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_AGGREGATE_JOIN);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitJoinCodegen(Query* query, const char* query_string, JitJoinPlan* plan)
{
    JitContext* jit_context = nullptr;

    if (plan->_aggregate._aggreaget_op == JIT_AGGREGATE_NONE) {
        switch (plan->_scan_type) {
            case JIT_JOIN_SCAN_POINT:
                // special case: this is really a point query
                jit_context = JitPointJoinCodegen(query, query_string, plan);
                break;

            case JIT_JOIN_SCAN_OUTER_POINT:
                // special case: outer scan is really a point query
                jit_context = JitPointOuterJoinCodegen(query, query_string, plan);
                break;

            case JIT_JOIN_SCAN_INNER_POINT:
                // special case: inner scan is really a point query
                jit_context = JitPointInnerJoinCodegen(query, query_string, plan);
                break;

            case JIT_JOIN_SCAN_RANGE:
                jit_context = JitRangeJoinCodegen(query, query_string, plan);
                break;

            default:
                MOT_LOG_TRACE(
                    "Cannot generate jitteed code for JOIN plan: Invalid JOIN scan type %d", (int)plan->_scan_type);
                break;
        }
    } else {
        jit_context = JitAggregateRangeJoinCodegen(query, query_string, plan);
    }

    return jit_context;
}

static JitContext* JitRangeScanCodegen(Query* query, const char* query_string, JitRangeScanPlan* plan)
{
    JitContext* jit_context = nullptr;

    switch (plan->_command_type) {
        case JIT_COMMAND_UPDATE:
            jit_context = JitRangeUpdateCodegen(query, query_string, (JitRangeUpdatePlan*)plan);
            break;

        case JIT_COMMAND_SELECT: {
            JitRangeSelectPlan* range_select_plan = (JitRangeSelectPlan*)plan;
            if (range_select_plan->_aggregate._aggreaget_op == JIT_AGGREGATE_NONE) {
                jit_context = JitRangeSelectCodegen(query, query_string, range_select_plan);
            } else {
                jit_context = JitAggregateRangeSelectCodegen(query, query_string, range_select_plan);
            }
        } break;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Generate JIT Code",
                "Invalid point query JIT plan command type %d",
                (int)plan->_command_type);
            break;
    }

    return jit_context;
}

static JitContext* JitPointQueryCodegen(Query* query, const char* query_string, JitPointQueryPlan* plan)
{
    JitContext* jit_context = nullptr;

    switch (plan->_command_type) {
        case JIT_COMMAND_UPDATE:
            jit_context = JitUpdateCodegen(query, query_string, (JitUpdatePlan*)plan);
            break;

        case JIT_COMMAND_DELETE:
            jit_context = JitDeleteCodegen(query, query_string, (JitDeletePlan*)plan);
            break;

        case JIT_COMMAND_SELECT:
            jit_context = JitSelectCodegen(query, query_string, (JitSelectPlan*)plan);
            break;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Generate JIT Code",
                "Invalid point query JIT plan command type %d",
                (int)plan->_command_type);
            break;
    }

    return jit_context;
}

extern void PrintNativeLlvmStartupInfo()
{
    MOT_LOG_INFO("Using native LLVM version " LLVM_VERSION_STRING);
}

extern JitContext* JitCodegenLlvmQuery(Query* query, const char* query_string, JitPlan* plan)
{
    JitContext* jit_context = nullptr;

    MOT_LOG_DEBUG("*** Attempting to generate planned LLVM-jitted code for query: %s", query_string);

    switch (plan->_plan_type) {
        case JIT_PLAN_INSERT_QUERY:
            jit_context = JitInsertCodegen(query, query_string, (JitInsertPlan*)plan);
            break;

        case JIT_PLAN_POINT_QUERY:
            jit_context = JitPointQueryCodegen(query, query_string, (JitPointQueryPlan*)plan);
            break;

        case JIT_PLAN_RANGE_SCAN:
            jit_context = JitRangeScanCodegen(query, query_string, (JitRangeScanPlan*)plan);
            break;

        case JIT_PLAN_JOIN:
            jit_context = JitJoinCodegen(query, query_string, (JitJoinPlan*)plan);
            break;

        default:
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Generate JIT Code", "Invalid JIT plan type %d", (int)plan->_plan_type);
            break;
    }

    if (jit_context == nullptr) {
        MOT_LOG_TRACE("Failed to generate LLVM-jitted code for query: %s", query_string);
    } else {
        MOT_LOG_DEBUG(
            "Got LLVM-jitted function %p after compile, for query: %s", jit_context->m_tvmFunction, query_string);
    }

    return jit_context;
}

extern void FreeGsCodeGen(dorado::GsCodeGen* code_gen)
{
    // object was allocate on the Process memory context (see SetupCodegenEnv() above, so we can
    // simply call delete operator (which invokes BaseObject::delete class operator defined in palloc.h)
    code_gen->releaseResource();
    delete code_gen;  // invokes parent class BaseObject::delete() operator defined in palloc.h)
}
}  // namespace JitExec
