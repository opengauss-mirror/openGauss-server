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
 * jit_llvm_funcs.h
 *    LLVM Helper Prototypes and Helpers to generate calls to Helper function via LLVM.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_llvm_funcs.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_LLVM_FUNCS_H
#define JIT_LLVM_FUNCS_H

/*
 * ATTENTION: Be sure to include jit_llvm_query.h before anything else because of gscodegen.h
 * See jit_llvm_query.h for more details.
 */
#include "jit_llvm_query.h"
#include "jit_plan.h"

#include <vector>

namespace JitExec {
/** @brief Gets a key from the execution context. */
inline llvm::Value* getExecContextKey(JitLlvmCodeGenContext* ctx, JitRangeIteratorType range_itr_type,
    JitRangeScanType range_scan_type, int subQueryIndex)
{
    llvm::Value* key = nullptr;
    if (range_scan_type == JIT_RANGE_SCAN_INNER) {
        if (range_itr_type == JIT_RANGE_ITERATOR_END) {
            key = ctx->inner_end_iterator_key_value;
        } else {
            key = ctx->inner_key_value;
        }
    } else if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
        if (range_itr_type == JIT_RANGE_ITERATOR_END) {
            key = ctx->end_iterator_key_value;
        } else {
            key = ctx->key_value;
        }
    } else if (range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
        if (range_itr_type == JIT_RANGE_ITERATOR_END) {
            key = ctx->m_subQueryData[subQueryIndex].m_endIteratorKey;
        } else {
            key = ctx->m_subQueryData[subQueryIndex].m_searchKey;
        }
    }
    return key;
}

inline llvm::Value* getExecContextIndex(JitLlvmCodeGenContext* ctx, JitRangeScanType rangeScanType, int subQueryIndex)
{
    llvm::Value* indexValue = nullptr;
    if (rangeScanType == JIT_RANGE_SCAN_INNER) {
        indexValue = ctx->inner_index_value;
    } else if (rangeScanType == JIT_RANGE_SCAN_MAIN) {
        indexValue = ctx->index_value;
    } else if (rangeScanType == JIT_RANGE_SCAN_SUB_QUERY) {
        indexValue = ctx->m_subQueryData[subQueryIndex].m_index;
    }
    return indexValue;
}

/*--------------------------- Define LLVM Helper Prototypes  ---------------------------*/
inline llvm::FunctionCallee defineFunction(llvm::Module* module, llvm::Type* ret_type, const char* name, ...)
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

inline void defineDebugLog(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->debugLogFunc = defineFunction(module, ctx->VOID_T, "debugLog", ctx->STR_T, ctx->STR_T, nullptr);
}

inline void defineIsSoftMemoryLimitReached(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->isSoftMemoryLimitReachedFunc = defineFunction(module, ctx->INT32_T, "isSoftMemoryLimitReached", nullptr);
}

inline void defineGetPrimaryIndex(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getPrimaryIndexFunc = defineFunction(
        module, ctx->IndexType->getPointerTo(), "getPrimaryIndex", ctx->TableType->getPointerTo(), nullptr);
}

inline void defineGetTableIndex(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getTableIndexFunc = defineFunction(
        module, ctx->IndexType->getPointerTo(), "getTableIndex", ctx->TableType->getPointerTo(), ctx->INT32_T, nullptr);
}

inline void defineInitKey(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->initKeyFunc = defineFunction(
        module, ctx->VOID_T, "InitKey", ctx->KeyType->getPointerTo(), ctx->IndexType->getPointerTo(), nullptr);
}

inline void defineGetColumnAt(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getColumnAtFunc = defineFunction(
        module, ctx->ColumnType->getPointerTo(), "getColumnAt", ctx->TableType->getPointerTo(), ctx->INT32_T, nullptr);
}

inline void defineSetExprArgIsNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setExprArgIsNullFunc =
        defineFunction(module, ctx->VOID_T, "setExprArgIsNull", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void defineGetExprArgIsNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getExprArgIsNullFunc = defineFunction(module, ctx->INT32_T, "getExprArgIsNull", ctx->INT32_T, nullptr);
}

inline void defineGetDatumParam(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getDatumParamFunc = defineFunction(module,
        ctx->DATUM_T,
        "getDatumParam",
        ctx->ParamListInfoDataType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

inline void defineReadDatumColumn(JitLlvmCodeGenContext* ctx, llvm::Module* module)
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

inline void defineWriteDatumColumn(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->writeDatumColumnFunc = defineFunction(module,
        ctx->VOID_T,
        "writeDatumColumn",
        ctx->RowType->getPointerTo(),
        ctx->ColumnType->getPointerTo(),
        ctx->DATUM_T,
        nullptr);
}

inline void defineBuildDatumKey(JitLlvmCodeGenContext* ctx, llvm::Module* module)
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

inline void defineSetBit(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setBitFunc =
        defineFunction(module, ctx->VOID_T, "setBit", ctx->BitmapSetType->getPointerTo(), ctx->INT32_T, nullptr);
}

inline void defineResetBitmapSet(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetBitmapSetFunc =
        defineFunction(module, ctx->VOID_T, "resetBitmapSet", ctx->BitmapSetType->getPointerTo(), nullptr);
}

inline void defineGetTableFieldCount(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getTableFieldCountFunc =
        defineFunction(module, ctx->INT32_T, "getTableFieldCount", ctx->TableType->getPointerTo(), nullptr);
}

inline void defineWriteRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->writeRowFunc = defineFunction(
        module, ctx->INT32_T, "writeRow", ctx->RowType->getPointerTo(), ctx->BitmapSetType->getPointerTo(), nullptr);
}

inline void defineSearchRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->searchRowFunc = defineFunction(module,
        ctx->RowType->getPointerTo(),
        "searchRow",
        ctx->TableType->getPointerTo(),
        ctx->KeyType->getPointerTo(),
        ctx->INT32_T,
        nullptr);
}

inline void defineCreateNewRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->createNewRowFunc =
        defineFunction(module, ctx->RowType->getPointerTo(), "createNewRow", ctx->TableType->getPointerTo(), nullptr);
}

inline void defineInsertRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->insertRowFunc = defineFunction(
        module, ctx->INT32_T, "insertRow", ctx->TableType->getPointerTo(), ctx->RowType->getPointerTo(), nullptr);
}

inline void defineDeleteRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->deleteRowFunc = defineFunction(module, ctx->INT32_T, "deleteRow", nullptr);
}

inline void defineSetRowNullBits(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setRowNullBitsFunc = defineFunction(
        module, ctx->VOID_T, "setRowNullBits", ctx->TableType->getPointerTo(), ctx->RowType->getPointerTo(), nullptr);
}

inline void defineSetExprResultNullBit(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setExprResultNullBitFunc = defineFunction(module,
        ctx->INT32_T,
        "setExprResultNullBit",
        ctx->TableType->getPointerTo(),
        ctx->RowType->getPointerTo(),
        ctx->INT32_T,
        nullptr);
}

inline void defineExecClearTuple(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->execClearTupleFunc =
        defineFunction(module, ctx->VOID_T, "execClearTuple", ctx->TupleTableSlotType->getPointerTo(), nullptr);
}

inline void defineExecStoreVirtualTuple(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->execStoreVirtualTupleFunc =
        defineFunction(module, ctx->VOID_T, "execStoreVirtualTuple", ctx->TupleTableSlotType->getPointerTo(), nullptr);
}

inline void defineSelectColumn(JitLlvmCodeGenContext* ctx, llvm::Module* module)
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

inline void defineSetTpProcessed(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setTpProcessedFunc =
        defineFunction(module, ctx->VOID_T, "setTpProcessed", ctx->INT64_T->getPointerTo(), ctx->INT64_T, nullptr);
}

inline void defineSetScanEnded(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setScanEndedFunc =
        defineFunction(module, ctx->VOID_T, "setScanEnded", ctx->INT32_T->getPointerTo(), ctx->INT32_T, nullptr);
}

inline void defineCopyKey(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->copyKeyFunc = defineFunction(module,
        ctx->VOID_T,
        "copyKey",
        ctx->IndexType->getPointerTo(),
        ctx->KeyType->getPointerTo(),
        ctx->KeyType->getPointerTo(),
        nullptr);
}

inline void defineFillKeyPattern(JitLlvmCodeGenContext* ctx, llvm::Module* module)
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

inline void defineAdjustKey(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->adjustKeyFunc = defineFunction(module,
        ctx->VOID_T,
        "adjustKey",
        ctx->KeyType->getPointerTo(),
        ctx->IndexType->getPointerTo(),
        ctx->INT8_T,
        nullptr);
}

inline void defineSearchIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
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

inline void defineBeginIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->beginIteratorFunc = defineFunction(
        module, ctx->IndexIteratorType->getPointerTo(), "beginIterator", ctx->IndexType->getPointerTo(), nullptr);
}

inline void defineCreateEndIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
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

inline void defineIsScanEnd(JitLlvmCodeGenContext* ctx, llvm::Module* module)
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

inline void defineGetRowFromIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
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

inline void defineDestroyIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->destroyIteratorFunc =
        defineFunction(module, ctx->VOID_T, "destroyIterator", ctx->IndexIteratorType->getPointerTo(), nullptr);
}

inline void defineSetStateIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setStateIteratorFunc = defineFunction(module,
        ctx->VOID_T,
        "setStateIterator",
        ctx->IndexIteratorType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

inline void defineGetStateIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getStateIteratorFunc = defineFunction(
        module, ctx->IndexIteratorType->getPointerTo(), "getStateIterator", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void defineIsStateIteratorNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->isStateIteratorNullFunc =
        defineFunction(module, ctx->INT32_T, "isStateIteratorNull", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void defineIsStateScanEnd(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->isStateScanEndFunc =
        defineFunction(module, ctx->INT32_T, "isStateScanEnd", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void defineGetRowFromStateIteratorFunc(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getRowFromStateIteratorFunc = defineFunction(module,
        ctx->RowType->getPointerTo(),
        "getRowFromStateIterator",
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

inline void defineDestroyStateIterators(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->destroyStateIteratorsFunc =
        defineFunction(module, ctx->VOID_T, "destroyStateIterators", ctx->INT32_T, nullptr);
}

inline void defineSetStateScanEndFlag(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setStateScanEndFlagFunc =
        defineFunction(module, ctx->VOID_T, "setStateScanEndFlag", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void defineGetStateScanEndFlag(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getStateScanEndFlagFunc = defineFunction(module, ctx->INT32_T, "getStateScanEndFlag", ctx->INT32_T, nullptr);
}

inline void defineResetStateRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetStateRowFunc = defineFunction(module, ctx->VOID_T, "resetStateRow", ctx->INT32_T, nullptr);
}

inline void defineSetStateRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setStateRowFunc =
        defineFunction(module, ctx->VOID_T, "setStateRow", ctx->RowType->getPointerTo(), ctx->INT32_T, nullptr);
}

inline void defineGetStateRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getStateRowFunc = defineFunction(module, ctx->RowType->getPointerTo(), "getStateRow", ctx->INT32_T, nullptr);
}

inline void defineCopyOuterStateRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->copyOuterStateRowFunc = defineFunction(module, ctx->VOID_T, "copyOuterStateRow", nullptr);
}

inline void defineGetOuterStateRowCopy(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getOuterStateRowCopyFunc =
        defineFunction(module, ctx->RowType->getPointerTo(), "getOuterStateRowCopy", nullptr);
}

inline void defineIsStateRowNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->isStateRowNullFunc = defineFunction(module, ctx->INT32_T, "isStateRowNull", ctx->INT32_T, nullptr);
}

inline void defineResetStateLimitCounter(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetStateLimitCounterFunc = defineFunction(module, ctx->VOID_T, "resetStateLimitCounter", nullptr);
}

inline void defineIncrementStateLimitCounter(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->incrementStateLimitCounterFunc = defineFunction(module, ctx->VOID_T, "incrementStateLimitCounter", nullptr);
}

inline void defineGetStateLimitCounter(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getStateLimitCounterFunc = defineFunction(module, ctx->INT32_T, "getStateLimitCounter", nullptr);
}

inline void definePrepareAvgArray(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->prepareAvgArrayFunc =
        defineFunction(module, ctx->VOID_T, "prepareAvgArray", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void defineLoadAvgArray(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->loadAvgArrayFunc = defineFunction(module, ctx->DATUM_T, "loadAvgArray", nullptr);
}

inline void defineSaveAvgArray(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->saveAvgArrayFunc = defineFunction(module, ctx->VOID_T, "saveAvgArray", ctx->DATUM_T, nullptr);
}

inline void defineComputeAvgFromArray(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->computeAvgFromArrayFunc = defineFunction(module, ctx->DATUM_T, "computeAvgFromArray", ctx->INT32_T, nullptr);
}

inline void defineResetAggValue(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetAggValueFunc = defineFunction(module, ctx->VOID_T, "resetAggValue", ctx->INT32_T, nullptr);
}

inline void defineGetAggValue(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getAggValueFunc = defineFunction(module, ctx->DATUM_T, "getAggValue", nullptr);
}

inline void defineSetAggValue(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setAggValueFunc = defineFunction(module, ctx->VOID_T, "setAggValue", ctx->DATUM_T, nullptr);
}

inline void defineResetAggMaxMinNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetAggMaxMinNullFunc = defineFunction(module, ctx->VOID_T, "resetAggMaxMinNull", nullptr);
}

inline void defineSetAggMaxMinNotNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setAggMaxMinNotNullFunc = defineFunction(module, ctx->VOID_T, "setAggMaxMinNotNull", nullptr);
}

inline void defineGetAggMaxMinIsNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getAggMaxMinIsNullFunc = defineFunction(module, ctx->INT32_T, "getAggMaxMinIsNull", nullptr);
}

inline void definePrepareDistinctSet(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->prepareDistinctSetFunc = defineFunction(module, ctx->VOID_T, "prepareDistinctSet", ctx->INT32_T, nullptr);
}

inline void defineInsertDistinctItem(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->insertDistinctItemFunc =
        defineFunction(module, ctx->INT32_T, "insertDistinctItem", ctx->INT32_T, ctx->DATUM_T, nullptr);
}

inline void defineDestroyDistinctSet(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->destroyDistinctSetFunc = defineFunction(module, ctx->VOID_T, "destroyDistinctSet", ctx->INT32_T, nullptr);
}

inline void defineResetTupleDatum(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetTupleDatumFunc = defineFunction(module,
        ctx->VOID_T,
        "resetTupleDatum",
        ctx->TupleTableSlotType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

inline void defineReadTupleDatum(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->readTupleDatumFunc = defineFunction(module,
        ctx->DATUM_T,
        "readTupleDatum",
        ctx->TupleTableSlotType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

inline void defineWriteTupleDatum(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->writeTupleDatumFunc = defineFunction(module,
        ctx->VOID_T,
        "writeTupleDatum",
        ctx->TupleTableSlotType->getPointerTo(),
        ctx->INT32_T,
        ctx->DATUM_T,
        nullptr);
}

inline void DefineSelectSubQueryResultFunc(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->selectSubQueryResultFunc = defineFunction(module, ctx->DATUM_T, "SelectSubQueryResult", ctx->INT32_T, nullptr);
}

inline void DefineCopyAggregateToSubQueryResultFunc(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->copyAggregateToSubQueryResultFunc =
        defineFunction(module, ctx->VOID_T, "CopyAggregateToSubQueryResult", ctx->INT32_T, nullptr);
}

inline void DefineGetSubQuerySlot(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->GetSubQuerySlotFunc =
        defineFunction(module, ctx->TupleTableSlotType->getPointerTo(), "GetSubQuerySlot", ctx->INT32_T, nullptr);
}

inline void DefineGetSubQueryTable(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->GetSubQueryTableFunc =
        defineFunction(module, ctx->TableType->getPointerTo(), "GetSubQueryTable", ctx->INT32_T, nullptr);
}

inline void DefineGetSubQueryIndex(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->GetSubQueryIndexFunc =
        defineFunction(module, ctx->IndexType->getPointerTo(), "GetSubQueryIndex", ctx->INT32_T, nullptr);
}

inline void DefineGetSubQuerySearchKey(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->GetSubQuerySearchKeyFunc =
        defineFunction(module, ctx->KeyType->getPointerTo(), "GetSubQuerySearchKey", ctx->INT32_T, nullptr);
}

inline void DefineGetSubQueryEndIteratorKey(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->GetSubQueryEndIteratorKeyFunc =
        defineFunction(module, ctx->KeyType->getPointerTo(), "GetSubQueryEndIteratorKey", ctx->INT32_T, nullptr);
}

inline void DefineGetConstAt(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->GetConstAtFunc = defineFunction(module, ctx->DATUM_T, "GetConstAt", ctx->INT32_T, ctx->INT32_T, nullptr);
}

/*--------------------------- End of LLVM Helper Prototypes ---------------------------*/

/*--------------------------- Helpers to generate calls to Helper function via LLVM ---------------------------*/
inline llvm::Value* AddFunctionCall(JitLlvmCodeGenContext* ctx, llvm::FunctionCallee func, ...)
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
inline llvm::Value* AddIsSoftMemoryLimitReached(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->isSoftMemoryLimitReachedFunc, nullptr);
}

/** @brief Adds a call to InitKey(). */
inline void AddInitKey(JitLlvmCodeGenContext* ctx, llvm::Value* key, llvm::Value* index)
{
    AddFunctionCall(ctx, ctx->initKeyFunc, key, index, nullptr);
}

/** @brief Adds a call to initSearchKey(key, index). */
inline void AddInitSearchKey(JitLlvmCodeGenContext* ctx, JitRangeScanType rangeScanType, int subQueryIndex)
{
    if (rangeScanType == JIT_RANGE_SCAN_INNER) {
        AddInitKey(ctx, ctx->inner_key_value, ctx->inner_index_value);
    } else if (rangeScanType == JIT_RANGE_SCAN_MAIN) {
        AddInitKey(ctx, ctx->key_value, ctx->index_value);
    } else if (rangeScanType == JIT_RANGE_SCAN_SUB_QUERY) {
        AddInitKey(ctx, ctx->m_subQueryData[subQueryIndex].m_searchKey, ctx->m_subQueryData[subQueryIndex].m_index);
    }
}

/** @brief Adds a call to getColumnAt(table, colid). */
inline llvm::Value* AddGetColumnAt(
    JitLlvmCodeGenContext* ctx, int colid, JitRangeScanType range_scan_type, int subQueryIndex = -1)
{
    llvm::ConstantInt* colid_value = llvm::ConstantInt::get(ctx->INT32_T, colid, true);
    llvm::Value* table = nullptr;
    if (range_scan_type == JIT_RANGE_SCAN_INNER) {
        table = ctx->inner_table_value;
    } else if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
        table = ctx->table_value;
    } else if (range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
        table = ctx->m_subQueryData[subQueryIndex].m_table;
    }
    return AddFunctionCall(ctx, ctx->getColumnAtFunc, table, colid_value, nullptr);
}

/** @brief Adds a call to setExprArgIsNull(arg_pos, isnull). */
inline void AddSetExprArgIsNull(JitLlvmCodeGenContext* ctx, int arg_pos, int isnull)
{
    llvm::ConstantInt* arg_pos_value = llvm::ConstantInt::get(ctx->INT32_T, arg_pos, true);
    llvm::ConstantInt* isnull_value = llvm::ConstantInt::get(ctx->INT32_T, isnull, true);
    AddFunctionCall(ctx, ctx->setExprArgIsNullFunc, arg_pos_value, isnull_value, nullptr);
}

/** @brief Adds a call to getExprArgIsNull(arg_pos). */
inline llvm::Value* AddGetExprArgIsNull(JitLlvmCodeGenContext* ctx, int arg_pos)
{
    llvm::ConstantInt* arg_pos_value = llvm::ConstantInt::get(ctx->INT32_T, arg_pos, true);
    return AddFunctionCall(ctx, ctx->getExprArgIsNullFunc, arg_pos_value, nullptr);
}

/** @brief Adds a call to getDatumParam(paramid, arg_pos). */
inline llvm::Value* AddGetDatumParam(JitLlvmCodeGenContext* ctx, int paramid, int arg_pos)
{
    llvm::ConstantInt* paramid_value = llvm::ConstantInt::get(ctx->INT32_T, paramid, true);
    llvm::ConstantInt* arg_pos_value = llvm::ConstantInt::get(ctx->INT32_T, arg_pos, true);
    return AddFunctionCall(ctx, ctx->getDatumParamFunc, ctx->params_value, paramid_value, arg_pos_value, nullptr);
}

/** @brief Adds a call to readDatumColumn(table_colid, arg_pos). */
inline llvm::Value* AddReadDatumColumn(
    JitLlvmCodeGenContext* ctx, llvm::Value* table, llvm::Value* row, int table_colid, int arg_pos)
{
    llvm::ConstantInt* table_colid_value = llvm::ConstantInt::get(ctx->INT32_T, table_colid, true);
    llvm::ConstantInt* arg_pos_value = llvm::ConstantInt::get(ctx->INT32_T, arg_pos, true);
    return AddFunctionCall(
        ctx, ctx->readDatumColumnFunc, ctx->table_value, row, table_colid_value, arg_pos_value, nullptr);
}

/** @brief Adds a call to writeDatumColumn(table_colid, value). */
inline void AddWriteDatumColumn(JitLlvmCodeGenContext* ctx, int table_colid, llvm::Value* row, llvm::Value* value)
{
    // make sure we have a column before issuing the call
    // we always write to a main table row (whether UPDATE or range UPDATE, so inner_scan value below is false)
    llvm::Value* column = AddGetColumnAt(ctx, table_colid, JIT_RANGE_SCAN_MAIN);
    AddFunctionCall(ctx, ctx->writeDatumColumnFunc, row, column, value, nullptr);
}

/** @brief Adds a call to buildDatumKey(column, key, value, index_colid, offset, value). */
inline void AddBuildDatumKey(JitLlvmCodeGenContext* ctx, llvm::Value* column, int index_colid, llvm::Value* value,
    int value_type, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type, int subQueryIndex = -1)
{
    int offset = -1;
    int size = -1;
    if (range_scan_type == JIT_RANGE_SCAN_INNER) {
        offset = ctx->_inner_table_info.m_indexColumnOffsets[index_colid];
        size = ctx->_inner_table_info.m_index->GetLengthKeyFields()[index_colid];
    } else if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
        offset = ctx->_table_info.m_indexColumnOffsets[index_colid];
        size = ctx->_table_info.m_index->GetLengthKeyFields()[index_colid];
    } else if (range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
        offset = ctx->m_subQueryTableInfo[subQueryIndex].m_indexColumnOffsets[index_colid];
        size = ctx->m_subQueryTableInfo[subQueryIndex].m_index->GetLengthKeyFields()[index_colid];
    }
    llvm::ConstantInt* colid_value = llvm::ConstantInt::get(ctx->INT32_T, index_colid, true);
    llvm::ConstantInt* offset_value = llvm::ConstantInt::get(ctx->INT32_T, offset, true);
    llvm::ConstantInt* value_type_value = llvm::ConstantInt::get(ctx->INT32_T, value_type, true);
    llvm::ConstantInt* size_value = llvm::ConstantInt::get(ctx->INT32_T, size, true);
    llvm::Value* key_value = getExecContextKey(ctx, range_itr_type, range_scan_type, subQueryIndex);
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
inline void AddSetBit(JitLlvmCodeGenContext* ctx, int colid)
{
    llvm::ConstantInt* bit_index = llvm::ConstantInt::get(ctx->INT32_T, colid, true);
    AddFunctionCall(ctx, ctx->setBitFunc, ctx->bitmap_value, bit_index, nullptr);
}

/** @brief Adds a call to resetBitmapSet(bitmap). */
inline void AddResetBitmapSet(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->resetBitmapSetFunc, ctx->bitmap_value, nullptr);
}

/** @brief Adds a call to writeRow(row, bitmap). */
inline llvm::Value* AddWriteRow(JitLlvmCodeGenContext* ctx, llvm::Value* row)
{
    return AddFunctionCall(ctx, ctx->writeRowFunc, row, ctx->bitmap_value, nullptr);
}

/** @brief Adds a call to searchRow(table, key, access_mode). */
inline llvm::Value* AddSearchRow(
    JitLlvmCodeGenContext* ctx, MOT::AccessType access_mode, JitRangeScanType range_scan_type, int subQueryIndex)
{
    llvm::Value* row = nullptr;
    llvm::ConstantInt* access_mode_value = llvm::ConstantInt::get(ctx->INT32_T, access_mode, true);
    if (range_scan_type == JIT_RANGE_SCAN_INNER) {
        row = AddFunctionCall(
            ctx, ctx->searchRowFunc, ctx->inner_table_value, ctx->inner_key_value, access_mode_value, nullptr);
    } else if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
        row = AddFunctionCall(ctx, ctx->searchRowFunc, ctx->table_value, ctx->key_value, access_mode_value, nullptr);
    } else if (range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
        JitLlvmCodeGenContext::SubQueryData* subQueryData = &ctx->m_subQueryData[subQueryIndex];
        row = AddFunctionCall(
            ctx, ctx->searchRowFunc, subQueryData->m_table, subQueryData->m_searchKey, access_mode_value, nullptr);
    }
    return row;
}

/** @brief Adds a call to createNewRow(table). */
inline llvm::Value* AddCreateNewRow(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->createNewRowFunc, ctx->table_value, nullptr);
}

inline llvm::Value* AddInsertRow(JitLlvmCodeGenContext* ctx, llvm::Value* row)
{
    return AddFunctionCall(ctx, ctx->insertRowFunc, ctx->table_value, row, nullptr);
}

inline llvm::Value* AddDeleteRow(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->deleteRowFunc, nullptr);
}

/** @brief Adds a call to setRowNullBits(table, row). */
inline void AddSetRowNullBits(JitLlvmCodeGenContext* ctx, llvm::Value* row)
{
    AddFunctionCall(ctx, ctx->setRowNullBitsFunc, ctx->table_value, row, nullptr);
}

/** @brief Adds a call to setExprResultNullBit(table, row, colid). */
inline llvm::Value* AddSetExprResultNullBit(JitLlvmCodeGenContext* ctx, llvm::Value* row, int colid)
{
    llvm::ConstantInt* colid_value = llvm::ConstantInt::get(ctx->INT32_T, colid, true);
    return AddFunctionCall(ctx, ctx->setExprResultNullBitFunc, ctx->table_value, row, colid_value, nullptr);
}

/** @brief Adds a call to execClearTuple(slot). */
inline void AddExecClearTuple(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->execClearTupleFunc, ctx->slot_value, nullptr);
}

/** @brief Adds a call to execStoreVirtualTuple(slot). */
inline void AddExecStoreVirtualTuple(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->execStoreVirtualTupleFunc, ctx->slot_value, nullptr);
}

/** @brief Adds a call to selectColumn(table, row, slot, colid, column_count). */
inline bool AddSelectColumn(JitLlvmCodeGenContext* ctx, llvm::Value* row, int tableColumnId, int tupleColumnId,
    JitRangeScanType rangeScanType, int subQueryIndex)
{
    llvm::ConstantInt* table_colid_value = llvm::ConstantInt::get(ctx->INT32_T, tableColumnId, true);
    llvm::ConstantInt* tuple_colid_value = llvm::ConstantInt::get(ctx->INT32_T, tupleColumnId, true);
    llvm::Value* value = nullptr;
    if (rangeScanType == JIT_RANGE_SCAN_INNER) {
        value = AddFunctionCall(ctx,
            ctx->selectColumnFunc,
            ctx->inner_table_value,
            row,
            ctx->slot_value,
            table_colid_value,
            tuple_colid_value,
            nullptr);
    } else if (rangeScanType == JIT_RANGE_SCAN_MAIN) {
        value = AddFunctionCall(ctx,
            ctx->selectColumnFunc,
            ctx->table_value,
            row,
            ctx->slot_value,
            table_colid_value,
            tuple_colid_value,
            nullptr);
    } else if (rangeScanType == JIT_RANGE_SCAN_SUB_QUERY) {
        JitLlvmCodeGenContext::SubQueryData* subQueryData = &ctx->m_subQueryData[subQueryIndex];
        value = AddFunctionCall(ctx,
            ctx->selectColumnFunc,
            subQueryData->m_table,
            row,
            subQueryData->m_slot,
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
inline void AddSetTpProcessed(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->setTpProcessedFunc, ctx->tp_processed_value, ctx->rows_processed, nullptr);
}

/** @brief Adds a call to setScanEnded(scan_ended, result). */
inline void AddSetScanEnded(JitLlvmCodeGenContext* ctx, int result)
{
    llvm::ConstantInt* result_value = llvm::ConstantInt::get(ctx->INT32_T, result, true);
    AddFunctionCall(ctx, ctx->setScanEndedFunc, ctx->scan_ended_value, result_value, nullptr);
}

/** @brief Adds a call to copyKey(index, key, end_iterator_key). */
inline void AddCopyKey(JitLlvmCodeGenContext* ctx, JitRangeScanType rangeScanType, int subQueryIndex)
{
    if (rangeScanType == JIT_RANGE_SCAN_INNER) {
        AddFunctionCall(ctx,
            ctx->copyKeyFunc,
            ctx->inner_index_value,
            ctx->inner_key_value,
            ctx->inner_end_iterator_key_value,
            nullptr);
    } else if (rangeScanType == JIT_RANGE_SCAN_MAIN) {
        AddFunctionCall(ctx, ctx->copyKeyFunc, ctx->index_value, ctx->key_value, ctx->end_iterator_key_value, nullptr);
    } else if (rangeScanType == JIT_RANGE_SCAN_SUB_QUERY) {
        JitLlvmCodeGenContext::SubQueryData* subQueryData = &ctx->m_subQueryData[subQueryIndex];
        AddFunctionCall(ctx,
            ctx->copyKeyFunc,
            subQueryData->m_index,
            subQueryData->m_searchKey,
            subQueryData->m_endIteratorKey,
            nullptr);
    }
}

/** @brief Adds a call to FillKeyPattern(key, pattern, offset, size) or FillKeyPattern(end_iterator_key, pattern,
 * offset, size). */
inline void AddFillKeyPattern(JitLlvmCodeGenContext* ctx, unsigned char pattern, int offset, int size,
    JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type, int subQueryIndex)
{
    llvm::ConstantInt* pattern_value = llvm::ConstantInt::get(ctx->INT8_T, pattern, true);
    llvm::ConstantInt* offset_value = llvm::ConstantInt::get(ctx->INT32_T, offset, true);
    llvm::ConstantInt* size_value = llvm::ConstantInt::get(ctx->INT32_T, size, true);
    llvm::Value* key_value = getExecContextKey(ctx, range_itr_type, range_scan_type, subQueryIndex);
    AddFunctionCall(ctx, ctx->fillKeyPatternFunc, key_value, pattern_value, offset_value, size_value, nullptr);
}

/** @brief Adds a call to adjustKey(key, index, pattern) or adjustKey(end_iterator_key, index, pattern). */
inline void AddAdjustKey(JitLlvmCodeGenContext* ctx, unsigned char pattern, JitRangeIteratorType range_itr_type,
    JitRangeScanType range_scan_type, int subQueryIndex)
{
    llvm::ConstantInt* pattern_value = llvm::ConstantInt::get(ctx->INT8_T, pattern, true);
    llvm::Value* key_value = getExecContextKey(ctx, range_itr_type, range_scan_type, subQueryIndex);
    llvm::Value* index_value = getExecContextIndex(ctx, range_scan_type, subQueryIndex);
    AddFunctionCall(ctx, ctx->adjustKeyFunc, key_value, index_value, pattern_value, nullptr);
}

/** @brief Adds a call to searchIterator(index, key). */
inline llvm::Value* AddSearchIterator(JitLlvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitRangeBoundMode range_bound_mode, JitRangeScanType range_scan_type, int subQueryIndex)
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
    } else if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
        itr = AddFunctionCall(ctx,
            ctx->searchIteratorFunc,
            ctx->index_value,
            ctx->key_value,
            forward_iterator_value,
            include_bound_value,
            nullptr);
    } else if (range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
        JitLlvmCodeGenContext::SubQueryData* subQueryData = &ctx->m_subQueryData[subQueryIndex];
        itr = AddFunctionCall(ctx,
            ctx->searchIteratorFunc,
            subQueryData->m_index,
            subQueryData->m_searchKey,
            forward_iterator_value,
            include_bound_value,
            nullptr);
    }
    return itr;
}

/** @brief Adds a call to beginIterator(index). */
inline llvm::Value* AddBeginIterator(JitLlvmCodeGenContext* ctx, JitRangeScanType rangeScanType, int subQueryIndex)
{
    llvm::Value* itr = nullptr;
    if (rangeScanType == JIT_RANGE_SCAN_INNER) {
        itr = AddFunctionCall(ctx, ctx->beginIteratorFunc, ctx->inner_index_value, nullptr);
    } else if (rangeScanType == JIT_RANGE_SCAN_MAIN) {
        itr = AddFunctionCall(ctx, ctx->beginIteratorFunc, ctx->index_value, nullptr);
    } else if (rangeScanType == JIT_RANGE_SCAN_SUB_QUERY) {
        JitLlvmCodeGenContext::SubQueryData* subQueryData = &ctx->m_subQueryData[subQueryIndex];
        itr = AddFunctionCall(ctx, ctx->beginIteratorFunc, subQueryData->m_index, nullptr);
    }
    return itr;
}

/** @brief Adds a call to createEndIterator(index, end_iterator_key). */
inline llvm::Value* AddCreateEndIterator(JitLlvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitRangeBoundMode range_bound_mode, JitRangeScanType range_scan_type, int subQueryIndex = -1)
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
    } else if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
        itr = AddFunctionCall(ctx,
            ctx->createEndIteratorFunc,
            ctx->index_value,
            ctx->end_iterator_key_value,
            forward_scan_value,
            include_bound_value,
            nullptr);
    } else if (range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
        JitLlvmCodeGenContext::SubQueryData* subQueryData = &ctx->m_subQueryData[subQueryIndex];
        itr = AddFunctionCall(ctx,
            ctx->createEndIteratorFunc,
            subQueryData->m_index,
            subQueryData->m_endIteratorKey,
            forward_scan_value,
            include_bound_value,
            nullptr);
    }
    return itr;
}

/** @brief Adds a call to isScanEnd(index, iterator, end_iterator). */
inline llvm::Value* AddIsScanEnd(JitLlvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitLlvmRuntimeCursor* cursor, JitRangeScanType range_scan_type, int subQueryIndex = -1)
{
    uint64_t forward_scan = (index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    llvm::ConstantInt* forward_scan_value = llvm::ConstantInt::get(ctx->INT32_T, forward_scan, true);
    llvm::Value* index_value = getExecContextIndex(ctx, range_scan_type, subQueryIndex);
    return AddFunctionCall(
        ctx, ctx->isScanEndFunc, index_value, cursor->begin_itr, cursor->end_itr, forward_scan_value, nullptr);
}

/** @brief Adds a cal to getRowFromIterator(index, iterator, end_iterator). */
inline llvm::Value* AddGetRowFromIterator(JitLlvmCodeGenContext* ctx, MOT::AccessType access_mode,
    JitIndexScanDirection index_scan_direction, JitLlvmRuntimeCursor* cursor, JitRangeScanType range_scan_type,
    int subQueryIndex)
{
    uint64_t forward_scan = (index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    llvm::ConstantInt* access_mode_value = llvm::ConstantInt::get(ctx->INT32_T, access_mode, true);
    llvm::ConstantInt* forward_scan_value = llvm::ConstantInt::get(ctx->INT32_T, forward_scan, true);
    llvm::Value* index_value = getExecContextIndex(ctx, range_scan_type, subQueryIndex);
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
inline void AddDestroyIterator(JitLlvmCodeGenContext* ctx, llvm::Value* itr)
{
    AddFunctionCall(ctx, ctx->destroyIteratorFunc, itr, nullptr);
}

inline void AddDestroyCursor(JitLlvmCodeGenContext* ctx, JitLlvmRuntimeCursor* cursor)
{
    if (cursor != nullptr) {
        AddDestroyIterator(ctx, cursor->begin_itr);
        AddDestroyIterator(ctx, cursor->end_itr);
    }
}

/** @brief Adds a call to setStateIterator(itr, begin_itr). */
inline void AddSetStateIterator(
    JitLlvmCodeGenContext* ctx, llvm::Value* itr, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
{
    uint64_t begin_itr = (range_itr_type == JIT_RANGE_ITERATOR_START) ? 1 : 0;
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* begin_itr_value = llvm::ConstantInt::get(ctx->INT32_T, begin_itr, true);
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    AddFunctionCall(ctx, ctx->setStateIteratorFunc, itr, begin_itr_value, inner_scan_value, nullptr);
}

/** @brief Adds a call to isStateIteratorNull(begin_itr). */
inline llvm::Value* AddIsStateIteratorNull(
    JitLlvmCodeGenContext* ctx, JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type)
{
    uint64_t begin_itr = (range_itr_type == JIT_RANGE_ITERATOR_START) ? 1 : 0;
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* begin_itr_value = llvm::ConstantInt::get(ctx->INT32_T, begin_itr, true);
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    return AddFunctionCall(ctx, ctx->isStateIteratorNullFunc, begin_itr_value, inner_scan_value, nullptr);
}

/** @brief Adds a call to isStateScanEnd(index, forward_scan). */
inline llvm::Value* AddIsStateScanEnd(
    JitLlvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction, JitRangeScanType range_scan_type)
{
    uint64_t forward_scan = (index_scan_direction == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* forward_scan_value = llvm::ConstantInt::get(ctx->INT32_T, forward_scan, true);
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    return AddFunctionCall(ctx, ctx->isStateScanEndFunc, forward_scan_value, inner_scan_value, nullptr);
}

/** @brief Adds a cal to getRowFromStateIterator(index, access_mode, forward_scan). */
inline llvm::Value* AddGetRowFromStateIterator(JitLlvmCodeGenContext* ctx, MOT::AccessType access_mode,
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
inline void AddDestroyStateIterators(JitLlvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    AddFunctionCall(ctx, ctx->destroyStateIteratorsFunc, inner_scan_value, nullptr);
}

/** @brief Adds a call to setStateScanEndFlag(scan_ended). */
inline void AddSetStateScanEndFlag(JitLlvmCodeGenContext* ctx, int scan_ended, JitRangeScanType range_scan_type)
{
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* scan_ended_value = llvm::ConstantInt::get(ctx->INT32_T, scan_ended, true);
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    AddFunctionCall(ctx, ctx->setStateScanEndFlagFunc, scan_ended_value, inner_scan_value, nullptr);
}

/** @brief Adds a call to getStateScanEndFlag(). */
inline llvm::Value* AddGetStateScanEndFlag(JitLlvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    return AddFunctionCall(ctx, ctx->getStateScanEndFlagFunc, inner_scan_value, nullptr);
}

inline void AddResetStateRow(JitLlvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    AddFunctionCall(ctx, ctx->resetStateRowFunc, inner_scan_value, nullptr);
}

inline void AddSetStateRow(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitRangeScanType range_scan_type)
{
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    AddFunctionCall(ctx, ctx->setStateRowFunc, row, inner_scan_value, nullptr);
}

inline llvm::Value* AddGetStateRow(JitLlvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    return AddFunctionCall(ctx, ctx->getStateRowFunc, inner_scan_value, nullptr);
}

inline void AddCopyOuterStateRow(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->copyOuterStateRowFunc, nullptr);
}

inline llvm::Value* AddGetOuterStateRowCopy(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->getOuterStateRowCopyFunc, nullptr);
}

inline llvm::Value* AddIsStateRowNull(JitLlvmCodeGenContext* ctx, JitRangeScanType range_scan_type)
{
    uint64_t inner_scan = (range_scan_type == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm::ConstantInt* inner_scan_value = llvm::ConstantInt::get(ctx->INT32_T, inner_scan, true);
    return AddFunctionCall(ctx, ctx->isStateRowNullFunc, inner_scan_value, nullptr);
}

inline void AddResetStateLimitCounter(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->resetStateLimitCounterFunc, nullptr);
}

inline void AddIncrementStateLimitCounter(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->incrementStateLimitCounterFunc, nullptr);
}

inline llvm::Value* AddGetStateLimitCounter(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->getStateLimitCounterFunc, nullptr);
}

inline void AddPrepareAvgArray(JitLlvmCodeGenContext* ctx, int element_type, int element_count)
{
    llvm::ConstantInt* element_type_value = llvm::ConstantInt::get(ctx->INT32_T, element_type, true);
    llvm::ConstantInt* element_count_value = llvm::ConstantInt::get(ctx->INT32_T, element_count, true);
    AddFunctionCall(ctx, ctx->prepareAvgArrayFunc, element_type_value, element_count_value, nullptr);
}

inline llvm::Value* AddLoadAvgArray(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->loadAvgArrayFunc, nullptr);
}

inline void AddSaveAvgArray(JitLlvmCodeGenContext* ctx, llvm::Value* avg_array)
{
    AddFunctionCall(ctx, ctx->saveAvgArrayFunc, avg_array, nullptr);
}

inline llvm::Value* AddComputeAvgFromArray(JitLlvmCodeGenContext* ctx, int element_type)
{
    llvm::ConstantInt* element_type_value = llvm::ConstantInt::get(ctx->INT32_T, element_type, true);
    return AddFunctionCall(ctx, ctx->computeAvgFromArrayFunc, element_type_value, nullptr);
}

inline void AddResetAggValue(JitLlvmCodeGenContext* ctx, int element_type)
{
    llvm::ConstantInt* element_type_value = llvm::ConstantInt::get(ctx->INT32_T, element_type, true);
    AddFunctionCall(ctx, ctx->resetAggValueFunc, element_type_value, nullptr);
}

inline void AddResetAggMaxMinNull(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->resetAggMaxMinNullFunc, nullptr);
}

inline llvm::Value* AddGetAggValue(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->getAggValueFunc, nullptr);
}

inline void AddSetAggValue(JitLlvmCodeGenContext* ctx, llvm::Value* value)
{
    AddFunctionCall(ctx, ctx->setAggValueFunc, value, nullptr);
}

inline void AddSetAggMaxMinNotNull(JitLlvmCodeGenContext* ctx)
{
    AddFunctionCall(ctx, ctx->setAggMaxMinNotNullFunc, nullptr);
}

inline llvm::Value* AddGetAggMaxMinIsNull(JitLlvmCodeGenContext* ctx)
{
    return AddFunctionCall(ctx, ctx->getAggMaxMinIsNullFunc, nullptr);
}

inline void AddPrepareDistinctSet(JitLlvmCodeGenContext* ctx, int element_type)
{
    llvm::ConstantInt* element_type_value = llvm::ConstantInt::get(ctx->INT32_T, element_type, true);
    AddFunctionCall(ctx, ctx->prepareDistinctSetFunc, element_type_value, nullptr);
}

inline llvm::Value* AddInsertDistinctItem(JitLlvmCodeGenContext* ctx, int element_type, llvm::Value* value)
{
    llvm::ConstantInt* element_type_value = llvm::ConstantInt::get(ctx->INT32_T, element_type, true);
    return AddFunctionCall(ctx, ctx->insertDistinctItemFunc, element_type_value, value, nullptr);
}

inline void AddDestroyDistinctSet(JitLlvmCodeGenContext* ctx, int element_type)
{
    llvm::ConstantInt* element_type_value = llvm::ConstantInt::get(ctx->INT32_T, element_type, true);
    AddFunctionCall(ctx, ctx->destroyDistinctSetFunc, element_type_value, nullptr);
}

/** @brief Adds a call to writeTupleDatum(slot, tuple_colid, value). */
inline void AddWriteTupleDatum(JitLlvmCodeGenContext* ctx, int tuple_colid, llvm::Value* value)
{
    llvm::ConstantInt* tuple_colid_value = llvm::ConstantInt::get(ctx->INT32_T, tuple_colid, true);
    AddFunctionCall(ctx, ctx->writeTupleDatumFunc, ctx->slot_value, tuple_colid_value, value, nullptr);
}

inline llvm::Value* AddSelectSubQueryResult(JitLlvmCodeGenContext* ctx, int subQueryIndex)
{
    llvm::ConstantInt* subQueryIndexValue = llvm::ConstantInt::get(ctx->INT32_T, subQueryIndex, true);
    return AddFunctionCall(ctx, ctx->selectSubQueryResultFunc, subQueryIndexValue, nullptr);
}

inline void AddCopyAggregateToSubQueryResult(JitLlvmCodeGenContext* ctx, int subQueryIndex)
{
    llvm::ConstantInt* subQueryIndexValue = llvm::ConstantInt::get(ctx->INT32_T, subQueryIndex, true);
    AddFunctionCall(ctx, ctx->copyAggregateToSubQueryResultFunc, subQueryIndexValue, nullptr);
}

inline llvm::Value* AddGetSubQuerySlot(JitLlvmCodeGenContext* ctx, int subQueryIndex)
{
    llvm::ConstantInt* subQueryIndexValue = llvm::ConstantInt::get(ctx->INT32_T, subQueryIndex, true);
    return AddFunctionCall(ctx, ctx->GetSubQuerySlotFunc, subQueryIndexValue, nullptr);
}

inline llvm::Value* AddGetSubQueryTable(JitLlvmCodeGenContext* ctx, int subQueryIndex)
{
    llvm::ConstantInt* subQueryIndexValue = llvm::ConstantInt::get(ctx->INT32_T, subQueryIndex, true);
    return AddFunctionCall(ctx, ctx->GetSubQueryTableFunc, subQueryIndexValue, nullptr);
}

inline llvm::Value* AddGetSubQueryIndex(JitLlvmCodeGenContext* ctx, int subQueryIndex)
{
    llvm::ConstantInt* subQueryIndexValue = llvm::ConstantInt::get(ctx->INT32_T, subQueryIndex, true);
    return AddFunctionCall(ctx, ctx->GetSubQueryIndexFunc, subQueryIndexValue, nullptr);
}

inline llvm::Value* AddGetSubQuerySearchKey(JitLlvmCodeGenContext* ctx, int subQueryIndex)
{
    llvm::ConstantInt* subQueryIndexValue = llvm::ConstantInt::get(ctx->INT32_T, subQueryIndex, true);
    return AddFunctionCall(ctx, ctx->GetSubQuerySearchKeyFunc, subQueryIndexValue, nullptr);
}

inline llvm::Value* AddGetSubQueryEndIteratorKey(JitLlvmCodeGenContext* ctx, int subQueryIndex)
{
    llvm::ConstantInt* subQueryIndexValue = llvm::ConstantInt::get(ctx->INT32_T, subQueryIndex, true);
    return AddFunctionCall(ctx, ctx->GetSubQueryEndIteratorKeyFunc, subQueryIndexValue, nullptr);
}

inline llvm::Value* AddGetConstAt(JitLlvmCodeGenContext* ctx, int constId, int argPos)
{
    llvm::ConstantInt* constIdValue = llvm::ConstantInt::get(ctx->INT32_T, constId, true);
    llvm::ConstantInt* argPosValue = llvm::ConstantInt::get(ctx->INT32_T, argPos, true);
    return AddFunctionCall(ctx, ctx->GetConstAtFunc, constIdValue, argPosValue, nullptr);
}

/** @brief Adds a call to issueDebugLog(function, msg). */
#ifdef MOT_JIT_DEBUG
inline void IssueDebugLogImpl(JitLlvmCodeGenContext* ctx, const char* function, const char* msg)
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
}  // namespace JitExec

#endif /* JIT_LLVM_FUNCS_H */
