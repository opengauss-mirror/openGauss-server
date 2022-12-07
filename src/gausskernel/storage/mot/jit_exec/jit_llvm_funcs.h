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
#include "jit_llvm_util.h"
#include "jit_profiler.h"

#include <vector>

namespace JitExec {
/** @brief Gets a key from the execution context. */
inline llvm::Value* getExecContextKey(
    JitLlvmCodeGenContext* ctx, JitRangeIteratorType rangeItrType, JitRangeScanType rangeScanType, int subQueryIndex)
{
    llvm::Value* key = nullptr;
    if (rangeScanType == JIT_RANGE_SCAN_INNER) {
        if (rangeItrType == JIT_RANGE_ITERATOR_END) {
            key = ctx->inner_end_iterator_key_value;
        } else {
            key = ctx->inner_key_value;
        }
    } else if (rangeScanType == JIT_RANGE_SCAN_MAIN) {
        if (rangeItrType == JIT_RANGE_ITERATOR_END) {
            key = ctx->end_iterator_key_value;
        } else {
            key = ctx->key_value;
        }
    } else if (rangeScanType == JIT_RANGE_SCAN_SUB_QUERY) {
        if (rangeItrType == JIT_RANGE_ITERATOR_END) {
            key = ctx->m_subQueryData[subQueryIndex].m_endIteratorKey;
        } else {
            key = ctx->m_subQueryData[subQueryIndex].m_searchKey;
        }
    }
    return key;
}

inline llvm::Value* getExecContextTable(JitLlvmCodeGenContext* ctx, JitRangeScanType rangeScanType, int subQueryIndex)
{
    llvm::Value* tableValue = nullptr;
    if (rangeScanType == JIT_RANGE_SCAN_INNER) {
        tableValue = ctx->inner_table_value;
    } else if (rangeScanType == JIT_RANGE_SCAN_MAIN) {
        tableValue = ctx->table_value;
    } else if (rangeScanType == JIT_RANGE_SCAN_SUB_QUERY) {
        tableValue = ctx->m_subQueryData[subQueryIndex].m_table;
    }
    return tableValue;
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
inline void defineDebugLog(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->debugLogFunc = llvm_util::DefineFunction(module, ctx->VOID_T, "debugLog", ctx->STR_T, ctx->STR_T, nullptr);
}

inline void defineIsSoftMemoryLimitReached(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->isSoftMemoryLimitReachedFunc =
        llvm_util::DefineFunction(module, ctx->INT32_T, "isSoftMemoryLimitReached", nullptr);
}

inline void defineGetPrimaryIndex(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getPrimaryIndexFunc = llvm_util::DefineFunction(
        module, ctx->IndexType->getPointerTo(), "getPrimaryIndex", ctx->TableType->getPointerTo(), nullptr);
}

inline void defineGetTableIndex(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getTableIndexFunc = llvm_util::DefineFunction(
        module, ctx->IndexType->getPointerTo(), "getTableIndex", ctx->TableType->getPointerTo(), ctx->INT32_T, nullptr);
}

inline void defineInitKey(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->initKeyFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "InitKey", ctx->KeyType->getPointerTo(), ctx->IndexType->getPointerTo(), nullptr);
}

inline void defineGetColumnAt(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getColumnAtFunc = llvm_util::DefineFunction(
        module, ctx->ColumnType->getPointerTo(), "getColumnAt", ctx->TableType->getPointerTo(), ctx->INT32_T, nullptr);
}

inline void DefineGetExprIsNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getExprIsNullFunc = llvm_util::DefineFunction(module, ctx->INT32_T, "GetExprIsNull", nullptr);
}

inline void DefineSetExprIsNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_setExprIsNullFunc = llvm_util::DefineFunction(module, ctx->VOID_T, "SetExprIsNull", ctx->INT32_T, nullptr);
}

inline void DefineGetExprCollation(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_getExprCollationFunc = llvm_util::DefineFunction(module, ctx->INT32_T, "GetExprCollation", nullptr);
}

inline void DefineSetExprCollation(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->m_setExprCollationFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "SetExprCollation", ctx->INT32_T, nullptr);
}

inline void defineGetDatumParam(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getDatumParamFunc = llvm_util::DefineFunction(
        module, ctx->DATUM_T, "getDatumParam", ctx->ParamListInfoDataType->getPointerTo(), ctx->INT32_T, nullptr);
}

inline void defineReadDatumColumn(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->readDatumColumnFunc = llvm_util::DefineFunction(module,
        ctx->DATUM_T,
        "readDatumColumn",
        ctx->TableType->getPointerTo(),
        ctx->RowType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

inline void defineWriteDatumColumn(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->writeDatumColumnFunc = llvm_util::DefineFunction(module,
        ctx->VOID_T,
        "writeDatumColumn",
        ctx->RowType->getPointerTo(),
        ctx->ColumnType->getPointerTo(),
        ctx->DATUM_T,
        nullptr);
}

inline void defineBuildDatumKey(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->buildDatumKeyFunc = llvm_util::DefineFunction(module,
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
    ctx->setBitFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "setBit", ctx->BitmapSetType->getPointerTo(), ctx->INT32_T, nullptr);
}

inline void defineResetBitmapSet(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetBitmapSetFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "resetBitmapSet", ctx->BitmapSetType->getPointerTo(), nullptr);
}

inline void defineGetTableFieldCount(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getTableFieldCountFunc =
        llvm_util::DefineFunction(module, ctx->INT32_T, "getTableFieldCount", ctx->TableType->getPointerTo(), nullptr);
}

inline void defineWriteRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->writeRowFunc = llvm_util::DefineFunction(
        module, ctx->INT32_T, "writeRow", ctx->RowType->getPointerTo(), ctx->BitmapSetType->getPointerTo(), nullptr);
}

inline void defineSearchRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->searchRowFunc = llvm_util::DefineFunction(module,
        ctx->RowType->getPointerTo(),
        "searchRow",
        ctx->TableType->getPointerTo(),
        ctx->KeyType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

inline void defineCreateNewRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->createNewRowFunc = llvm_util::DefineFunction(
        module, ctx->RowType->getPointerTo(), "createNewRow", ctx->TableType->getPointerTo(), nullptr);
}

inline void defineInsertRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->insertRowFunc = llvm_util::DefineFunction(
        module, ctx->INT32_T, "insertRow", ctx->TableType->getPointerTo(), ctx->RowType->getPointerTo(), nullptr);
}

inline void defineDeleteRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->deleteRowFunc = llvm_util::DefineFunction(module, ctx->INT32_T, "deleteRow", nullptr);
}

inline void defineSetRowNullBits(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setRowNullBitsFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "setRowNullBits", ctx->TableType->getPointerTo(), ctx->RowType->getPointerTo(), nullptr);
}

inline void defineSetExprResultNullBit(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setExprResultNullBitFunc = llvm_util::DefineFunction(module,
        ctx->INT32_T,
        "setExprResultNullBit",
        ctx->TableType->getPointerTo(),
        ctx->RowType->getPointerTo(),
        ctx->INT32_T,
        nullptr);
}

inline void defineExecClearTuple(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->execClearTupleFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "execClearTuple", ctx->TupleTableSlotType->getPointerTo(), nullptr);
}

inline void defineExecStoreVirtualTuple(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->execStoreVirtualTupleFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "execStoreVirtualTuple", ctx->TupleTableSlotType->getPointerTo(), nullptr);
}

inline void defineSelectColumn(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->selectColumnFunc = llvm_util::DefineFunction(module,
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
    ctx->setTpProcessedFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "setTpProcessed", ctx->INT64_T->getPointerTo(), ctx->INT64_T, nullptr);
}

inline void defineSetScanEnded(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setScanEndedFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "setScanEnded", ctx->INT32_T->getPointerTo(), ctx->INT32_T, nullptr);
}

inline void defineCopyKey(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->copyKeyFunc = llvm_util::DefineFunction(module,
        ctx->VOID_T,
        "copyKey",
        ctx->IndexType->getPointerTo(),
        ctx->KeyType->getPointerTo(),
        ctx->KeyType->getPointerTo(),
        nullptr);
}

inline void defineFillKeyPattern(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->fillKeyPatternFunc = llvm_util::DefineFunction(module,
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
    ctx->adjustKeyFunc = llvm_util::DefineFunction(module,
        ctx->VOID_T,
        "adjustKey",
        ctx->KeyType->getPointerTo(),
        ctx->IndexType->getPointerTo(),
        ctx->INT8_T,
        nullptr);
}

inline void defineSearchIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->searchIteratorFunc = llvm_util::DefineFunction(module,
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
    ctx->beginIteratorFunc = llvm_util::DefineFunction(
        module, ctx->IndexIteratorType->getPointerTo(), "beginIterator", ctx->IndexType->getPointerTo(), nullptr);
}

inline void defineCreateEndIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->createEndIteratorFunc = llvm_util::DefineFunction(module,
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
    ctx->isScanEndFunc = llvm_util::DefineFunction(module,
        ctx->INT32_T,
        "isScanEnd",
        ctx->IndexType->getPointerTo(),
        ctx->IndexIteratorType->getPointerTo(),
        ctx->IndexIteratorType->getPointerTo(),
        ctx->INT32_T,
        nullptr);
}

inline void DefineCheckRowExistsInIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->CheckRowExistsInIteratorFunc = llvm_util::DefineFunction(module,
        ctx->INT32_T,
        "CheckRowExistsInIterator",
        ctx->IndexType->getPointerTo(),
        ctx->IndexIteratorType->getPointerTo(),
        ctx->IndexIteratorType->getPointerTo(),
        ctx->INT32_T,
        nullptr);
}

inline void defineGetRowFromIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getRowFromIteratorFunc = llvm_util::DefineFunction(module,
        ctx->RowType->getPointerTo(),
        "getRowFromIterator",
        ctx->IndexType->getPointerTo(),
        ctx->IndexIteratorType->getPointerTo(),
        ctx->IndexIteratorType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

inline void defineDestroyIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->destroyIteratorFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "destroyIterator", ctx->IndexIteratorType->getPointerTo(), nullptr);
}

inline void defineSetStateIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setStateIteratorFunc = llvm_util::DefineFunction(module,
        ctx->VOID_T,
        "setStateIterator",
        ctx->IndexIteratorType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

inline void defineGetStateIterator(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getStateIteratorFunc = llvm_util::DefineFunction(
        module, ctx->IndexIteratorType->getPointerTo(), "getStateIterator", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void defineIsStateIteratorNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->isStateIteratorNullFunc =
        llvm_util::DefineFunction(module, ctx->INT32_T, "isStateIteratorNull", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void defineIsStateScanEnd(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->isStateScanEndFunc =
        llvm_util::DefineFunction(module, ctx->INT32_T, "isStateScanEnd", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void defineGetRowFromStateIteratorFunc(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getRowFromStateIteratorFunc = llvm_util::DefineFunction(module,
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
        llvm_util::DefineFunction(module, ctx->VOID_T, "destroyStateIterators", ctx->INT32_T, nullptr);
}

inline void defineSetStateScanEndFlag(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setStateScanEndFlagFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "setStateScanEndFlag", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void defineGetStateScanEndFlag(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getStateScanEndFlagFunc =
        llvm_util::DefineFunction(module, ctx->INT32_T, "getStateScanEndFlag", ctx->INT32_T, nullptr);
}

inline void defineResetStateRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetStateRowFunc = llvm_util::DefineFunction(module, ctx->VOID_T, "resetStateRow", ctx->INT32_T, nullptr);
}

inline void defineSetStateRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setStateRowFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "setStateRow", ctx->RowType->getPointerTo(), ctx->INT32_T, nullptr);
}

inline void defineGetStateRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getStateRowFunc =
        llvm_util::DefineFunction(module, ctx->RowType->getPointerTo(), "getStateRow", ctx->INT32_T, nullptr);
}

inline void defineCopyOuterStateRow(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->copyOuterStateRowFunc = llvm_util::DefineFunction(module, ctx->VOID_T, "copyOuterStateRow", nullptr);
}

inline void defineGetOuterStateRowCopy(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getOuterStateRowCopyFunc =
        llvm_util::DefineFunction(module, ctx->RowType->getPointerTo(), "getOuterStateRowCopy", nullptr);
}

inline void defineIsStateRowNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->isStateRowNullFunc = llvm_util::DefineFunction(module, ctx->INT32_T, "isStateRowNull", ctx->INT32_T, nullptr);
}

inline void defineResetStateLimitCounter(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetStateLimitCounterFunc = llvm_util::DefineFunction(module, ctx->VOID_T, "resetStateLimitCounter", nullptr);
}

inline void defineIncrementStateLimitCounter(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->incrementStateLimitCounterFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "incrementStateLimitCounter", nullptr);
}

inline void defineGetStateLimitCounter(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getStateLimitCounterFunc = llvm_util::DefineFunction(module, ctx->INT32_T, "getStateLimitCounter", nullptr);
}

inline void definePrepareAvgArray(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->prepareAvgArrayFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "prepareAvgArray", ctx->INT32_T, ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void defineLoadAvgArray(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->loadAvgArrayFunc = llvm_util::DefineFunction(module, ctx->DATUM_T, "loadAvgArray", ctx->INT32_T, nullptr);
}

inline void defineSaveAvgArray(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->saveAvgArrayFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "saveAvgArray", ctx->INT32_T, ctx->DATUM_T, nullptr);
}

inline void defineComputeAvgFromArray(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->computeAvgFromArrayFunc =
        llvm_util::DefineFunction(module, ctx->DATUM_T, "computeAvgFromArray", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void defineResetAggValue(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetAggValueFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "resetAggValue", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void defineGetAggValue(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getAggValueFunc = llvm_util::DefineFunction(module, ctx->DATUM_T, "getAggValue", ctx->INT32_T, nullptr);
}

inline void defineSetAggValue(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setAggValueFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "setAggValue", ctx->INT32_T, ctx->DATUM_T, nullptr);
}

inline void defineGetAggValueIsNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->getAggValueIsNullFunc =
        llvm_util::DefineFunction(module, ctx->INT32_T, "getAggValueIsNull", ctx->INT32_T, nullptr);
}

inline void defineSetAggValueIsNull(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->setAggValueIsNullFunc =
        llvm_util::DefineFunction(module, ctx->INT32_T, "setAggValueIsNull", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void definePrepareDistinctSet(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->prepareDistinctSetFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "prepareDistinctSet", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void defineInsertDistinctItem(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->insertDistinctItemFunc = llvm_util::DefineFunction(
        module, ctx->INT32_T, "insertDistinctItem", ctx->INT32_T, ctx->INT32_T, ctx->DATUM_T, nullptr);
}

inline void defineDestroyDistinctSet(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->destroyDistinctSetFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "destroyDistinctSet", ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void defineResetTupleDatum(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->resetTupleDatumFunc = llvm_util::DefineFunction(module,
        ctx->VOID_T,
        "resetTupleDatum",
        ctx->TupleTableSlotType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        nullptr);
}

inline void defineReadTupleDatum(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->readTupleDatumFunc = llvm_util::DefineFunction(
        module, ctx->DATUM_T, "readTupleDatum", ctx->TupleTableSlotType->getPointerTo(), ctx->INT32_T, nullptr);
}

inline void defineWriteTupleDatum(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->writeTupleDatumFunc = llvm_util::DefineFunction(module,
        ctx->VOID_T,
        "writeTupleDatum",
        ctx->TupleTableSlotType->getPointerTo(),
        ctx->INT32_T,
        ctx->DATUM_T,
        ctx->INT32_T,
        nullptr);
}

inline void DefineSelectSubQueryResultFunc(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->selectSubQueryResultFunc =
        llvm_util::DefineFunction(module, ctx->DATUM_T, "SelectSubQueryResult", ctx->INT32_T, nullptr);
}

inline void DefineCopyAggregateToSubQueryResultFunc(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->copyAggregateToSubQueryResultFunc =
        llvm_util::DefineFunction(module, ctx->VOID_T, "CopyAggregateToSubQueryResult", ctx->INT32_T, nullptr);
}

inline void DefineGetSubQuerySlot(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->GetSubQuerySlotFunc = llvm_util::DefineFunction(
        module, ctx->TupleTableSlotType->getPointerTo(), "GetSubQuerySlot", ctx->INT32_T, nullptr);
}

inline void DefineGetSubQueryTable(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->GetSubQueryTableFunc =
        llvm_util::DefineFunction(module, ctx->TableType->getPointerTo(), "GetSubQueryTable", ctx->INT32_T, nullptr);
}

inline void DefineGetSubQueryIndex(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->GetSubQueryIndexFunc =
        llvm_util::DefineFunction(module, ctx->IndexType->getPointerTo(), "GetSubQueryIndex", ctx->INT32_T, nullptr);
}

inline void DefineGetSubQuerySearchKey(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->GetSubQuerySearchKeyFunc =
        llvm_util::DefineFunction(module, ctx->KeyType->getPointerTo(), "GetSubQuerySearchKey", ctx->INT32_T, nullptr);
}

inline void DefineGetSubQueryEndIteratorKey(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->GetSubQueryEndIteratorKeyFunc = llvm_util::DefineFunction(
        module, ctx->KeyType->getPointerTo(), "GetSubQueryEndIteratorKey", ctx->INT32_T, nullptr);
}

inline void DefineGetConstAt(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->GetConstAtFunc = llvm_util::DefineFunction(module, ctx->DATUM_T, "GetConstAt", ctx->INT32_T, nullptr);
}

inline void DefineGetInvokeParamListInfo(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->GetInvokeParamListInfoFunc = llvm_util::DefineFunction(
        module, ctx->ParamListInfoDataType->getPointerTo(), "GetInvokeParamListInfo", nullptr);
}

inline void DefineSetParamValue(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->SetParamValueFunc = llvm_util::DefineFunction(module,
        ctx->VOID_T,
        "SetParamValue",
        ctx->ParamListInfoDataType->getPointerTo(),
        ctx->INT32_T,
        ctx->INT32_T,
        ctx->INT64_T,
        ctx->INT32_T,
        nullptr);
}

inline void DefineInvokeStoredProcedure(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->InvokeStoredProcedureFunc = llvm_util::DefineFunction(module, ctx->INT32_T, "InvokeStoredProcedure", nullptr);
}

inline void DefineConvertViaString(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->ConvertViaStringFunc = llvm_util::DefineFunction(
        module, ctx->DATUM_T, "JitConvertViaString", ctx->DATUM_T, ctx->INT32_T, ctx->INT32_T, ctx->INT32_T, nullptr);
}

inline void DefineEmitProfileData(JitLlvmCodeGenContext* ctx, llvm::Module* module)
{
    ctx->EmitProfileDataFunc = llvm_util::DefineFunction(
        module, ctx->VOID_T, "EmitProfileData", ctx->INT32_T, ctx->INT32_T, ctx->INT32_T, nullptr);
}

/*--------------------------- End of LLVM Helper Prototypes ---------------------------*/

/*--------------------------- Helpers to generate calls to Helper function via LLVM ---------------------------*/

/** @brief Adds a call to isSoftMemoryLimitReached(). */
inline llvm::Value* AddIsSoftMemoryLimitReached(JitLlvmCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->isSoftMemoryLimitReachedFunc, nullptr);
}

/** @brief Adds a call to InitKey(). */
inline void AddInitKey(JitLlvmCodeGenContext* ctx, llvm::Value* key, llvm::Value* index)
{
    llvm_util::AddFunctionCall(ctx, ctx->initKeyFunc, key, index, nullptr);
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

/** @brief Adds a call to getColumnAt(table, columnId). */
inline llvm::Value* AddGetColumnAt(
    JitLlvmCodeGenContext* ctx, int columnId, JitRangeScanType rangeScanType, int subQueryIndex = -1)
{
    llvm::Value* table = getExecContextTable(ctx, rangeScanType, subQueryIndex);
    return llvm_util::AddFunctionCall(ctx, ctx->getColumnAtFunc, table, JIT_CONST_INT32(columnId), nullptr);
}

inline void AddSetExprIsNull(JitLlvmCodeGenContext* ctx, int isnull)
{
    llvm_util::AddFunctionCall(ctx, ctx->m_setExprIsNullFunc, JIT_CONST_INT32(isnull), nullptr);
}

inline llvm::Value* AddGetExprIsNull(JitLlvmCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_getExprIsNullFunc, nullptr);
}

inline void AddSetExprCollation(JitLlvmCodeGenContext* ctx, int collation)
{
    llvm_util::AddFunctionCall(ctx, ctx->m_setExprCollationFunc, JIT_CONST_INT32(collation), nullptr);
}

inline llvm::Value* AddGetExprCollation(JitLlvmCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->m_getExprCollationFunc, nullptr);
}

/** @brief Adds a call to getDatumParam(paramid, argPos). */
inline llvm::Value* AddGetDatumParam(JitLlvmCodeGenContext* ctx, int paramid)
{
    return llvm_util::AddFunctionCall(
        ctx, ctx->getDatumParamFunc, ctx->params_value, JIT_CONST_INT32(paramid), nullptr);
}

/** @brief Adds a call to readDatumColumn(tableColumnId, argPos). */
inline llvm::Value* AddReadDatumColumn(JitLlvmCodeGenContext* ctx, llvm::Value* table, llvm::Value* row,
    int tableColumnId, int isInnerRow, int subQueryIndex)
{
    return AddFunctionCall(ctx,
        ctx->readDatumColumnFunc,
        table,
        row,
        JIT_CONST_INT32(tableColumnId),
        JIT_CONST_INT32(isInnerRow),
        JIT_CONST_INT32(subQueryIndex),
        nullptr);
}

/** @brief Adds a call to writeDatumColumn(tableColumnId, value). */
inline void AddWriteDatumColumn(JitLlvmCodeGenContext* ctx, int tableColumnId, llvm::Value* row, llvm::Value* value)
{
    // make sure we have a column before issuing the call
    // we always write to a main table row (whether UPDATE or range UPDATE, so inner_scan value below is false)
    llvm::Value* column = AddGetColumnAt(ctx, tableColumnId, JIT_RANGE_SCAN_MAIN);
    llvm_util::AddFunctionCall(ctx, ctx->writeDatumColumnFunc, row, column, value, nullptr);
}

/** @brief Adds a call to buildDatumKey(column, key, value, index_colid, offset, value). */
inline void AddBuildDatumKey(JitLlvmCodeGenContext* ctx, llvm::Value* column, int indexColumnId, llvm::Value* value,
    int valueType, JitRangeIteratorType rangeItrType, JitRangeScanType rangeScanType, int subQueryIndex = -1)
{
    int offset = -1;
    int size = -1;
    if (rangeScanType == JIT_RANGE_SCAN_INNER) {
        offset = ctx->_inner_table_info.m_indexColumnOffsets[indexColumnId];
        size = ctx->_inner_table_info.m_index->GetLengthKeyFields()[indexColumnId];
    } else if (rangeScanType == JIT_RANGE_SCAN_MAIN) {
        offset = ctx->_table_info.m_indexColumnOffsets[indexColumnId];
        size = ctx->_table_info.m_index->GetLengthKeyFields()[indexColumnId];
    } else if (rangeScanType == JIT_RANGE_SCAN_SUB_QUERY) {
        offset = ctx->m_subQueryTableInfo[subQueryIndex].m_indexColumnOffsets[indexColumnId];
        size = ctx->m_subQueryTableInfo[subQueryIndex].m_index->GetLengthKeyFields()[indexColumnId];
    }
    llvm::Value* key = getExecContextKey(ctx, rangeItrType, rangeScanType, subQueryIndex);
    llvm_util::AddFunctionCall(ctx,
        ctx->buildDatumKeyFunc,
        column,
        key,
        value,
        JIT_CONST_INT32(indexColumnId),
        JIT_CONST_INT32(offset),
        JIT_CONST_INT32(size),
        JIT_CONST_INT32(valueType),
        nullptr);
}

/** @brief Adds a call to setBit(bitmap, columnId). */
inline void AddSetBit(JitLlvmCodeGenContext* ctx, int columnId)
{
    llvm_util::AddFunctionCall(ctx, ctx->setBitFunc, ctx->bitmap_value, JIT_CONST_INT32(columnId), nullptr);
}

/** @brief Adds a call to resetBitmapSet(bitmap). */
inline void AddResetBitmapSet(JitLlvmCodeGenContext* ctx)
{
    llvm_util::AddFunctionCall(ctx, ctx->resetBitmapSetFunc, ctx->bitmap_value, nullptr);
}

/** @brief Adds a call to writeRow(row, bitmap). */
inline llvm::Value* AddWriteRow(JitLlvmCodeGenContext* ctx, llvm::Value* row)
{
    return llvm_util::AddFunctionCall(ctx, ctx->writeRowFunc, row, ctx->bitmap_value, nullptr);
}

/** @brief Adds a call to searchRow(table, key, accessMode). */
inline llvm::Value* AddSearchRow(
    JitLlvmCodeGenContext* ctx, MOT::AccessType accessMode, JitRangeScanType rangeScanType, int subQueryIndex = -1)
{
    llvm::Value* table = getExecContextTable(ctx, rangeScanType, subQueryIndex);
    llvm::Value* key = getExecContextKey(ctx, JIT_RANGE_ITERATOR_START, rangeScanType, subQueryIndex);
    int innerRow = (rangeScanType == JIT_RANGE_SCAN_INNER);
    return AddFunctionCall(ctx,
        ctx->searchRowFunc,
        table,
        key,
        JIT_CONST_INT32(accessMode),
        JIT_CONST_INT32(innerRow),
        JIT_CONST_INT32(subQueryIndex),
        nullptr);
}

/** @brief Adds a call to createNewRow(table). */
inline llvm::Value* AddCreateNewRow(JitLlvmCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->createNewRowFunc, ctx->table_value, nullptr);
}

inline llvm::Value* AddInsertRow(JitLlvmCodeGenContext* ctx, llvm::Value* row)
{
    return llvm_util::AddFunctionCall(ctx, ctx->insertRowFunc, ctx->table_value, row, nullptr);
}

inline llvm::Value* AddDeleteRow(JitLlvmCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->deleteRowFunc, nullptr);
}

/** @brief Adds a call to setRowNullBits(table, row). */
inline void AddSetRowNullBits(JitLlvmCodeGenContext* ctx, llvm::Value* row)
{
    llvm_util::AddFunctionCall(ctx, ctx->setRowNullBitsFunc, ctx->table_value, row, nullptr);
}

/** @brief Adds a call to setExprResultNullBit(table, row, columnId). */
inline llvm::Value* AddSetExprResultNullBit(JitLlvmCodeGenContext* ctx, llvm::Value* row, int columnId)
{
    return llvm_util::AddFunctionCall(
        ctx, ctx->setExprResultNullBitFunc, ctx->table_value, row, JIT_CONST_INT32(columnId), nullptr);
}

/** @brief Adds a call to execClearTuple(slot). */
inline void AddExecClearTuple(JitLlvmCodeGenContext* ctx)
{
    llvm_util::AddFunctionCall(ctx, ctx->execClearTupleFunc, ctx->slot_value, nullptr);
}

/** @brief Adds a call to execStoreVirtualTuple(slot). */
inline void AddExecStoreVirtualTuple(JitLlvmCodeGenContext* ctx)
{
    llvm_util::AddFunctionCall(ctx, ctx->execStoreVirtualTupleFunc, ctx->slot_value, nullptr);
}

/** @brief Adds a call to selectColumn(table, row, slot, columnId, column_count). */
inline bool AddSelectColumn(JitLlvmCodeGenContext* ctx, llvm::Value* row, int tableColumnId, int tupleColumnId,
    JitRangeScanType rangeScanType, int subQueryIndex)
{
    llvm::Value* table = getExecContextTable(ctx, rangeScanType, subQueryIndex);
    llvm::Value* value = llvm_util::AddFunctionCall(ctx,
        ctx->selectColumnFunc,
        table,
        row,
        ctx->slot_value,
        JIT_CONST_INT32(tableColumnId),
        JIT_CONST_INT32(tupleColumnId),
        nullptr);

    if (value == nullptr) {
        return false;
    }

    return true;
}

/** @brief Adds a call to setTpProcessed(tp_processed, rows_processed). */
inline void AddSetTpProcessed(JitLlvmCodeGenContext* ctx)
{
    llvm::Value* rowsProcessed = ctx->m_builder->CreateLoad(ctx->rows_processed, true);
    llvm_util::AddFunctionCall(ctx, ctx->setTpProcessedFunc, ctx->tp_processed_value, rowsProcessed, nullptr);
}

/** @brief Adds a call to setScanEnded(scan_ended, result). */
inline void AddSetScanEnded(JitLlvmCodeGenContext* ctx, int result)
{
    llvm::ConstantInt* result_value = llvm::ConstantInt::get(ctx->INT32_T, result, true);
    llvm_util::AddFunctionCall(ctx, ctx->setScanEndedFunc, ctx->scan_ended_value, result_value, nullptr);
}

/** @brief Adds a call to copyKey(index, key, end_iterator_key). */
inline void AddCopyKey(JitLlvmCodeGenContext* ctx, JitRangeScanType rangeScanType, int subQueryIndex)
{
    llvm::Value* index = getExecContextIndex(ctx, rangeScanType, subQueryIndex);
    llvm::Value* beginKey = getExecContextKey(ctx, JIT_RANGE_ITERATOR_START, rangeScanType, subQueryIndex);
    llvm::Value* endKey = getExecContextKey(ctx, JIT_RANGE_ITERATOR_END, rangeScanType, subQueryIndex);
    AddFunctionCall(ctx, ctx->copyKeyFunc, index, beginKey, endKey, nullptr);
}

/** @brief Adds a call to FillKeyPattern(key, pattern, offset, size) or FillKeyPattern(end_iterator_key, pattern,
 * offset, size). */
inline void AddFillKeyPattern(JitLlvmCodeGenContext* ctx, unsigned char pattern, int offset, int size,
    JitRangeIteratorType rangeItrType, JitRangeScanType rangeScanType, int subQueryIndex)
{
    llvm::Value* key = getExecContextKey(ctx, rangeItrType, rangeScanType, subQueryIndex);
    llvm_util::AddFunctionCall(ctx,
        ctx->fillKeyPatternFunc,
        key,
        JIT_CONST_INT8(pattern),
        JIT_CONST_INT32(offset),
        JIT_CONST_INT32(size),
        nullptr);
}

/** @brief Adds a call to adjustKey(key, index, pattern) or adjustKey(end_iterator_key, index, pattern). */
inline void AddAdjustKey(JitLlvmCodeGenContext* ctx, unsigned char pattern, JitRangeIteratorType rangeItrType,
    JitRangeScanType rangeScanType, int subQueryIndex)
{
    llvm::Value* key = getExecContextKey(ctx, rangeItrType, rangeScanType, subQueryIndex);
    llvm::Value* index = getExecContextIndex(ctx, rangeScanType, subQueryIndex);
    llvm_util::AddFunctionCall(ctx, ctx->adjustKeyFunc, key, index, JIT_CONST_INT8(pattern), nullptr);
}

/** @brief Adds a call to searchIterator(index, key). */
inline llvm::Value* AddSearchIterator(JitLlvmCodeGenContext* ctx, JitIndexScanDirection indexScanDirection,
    JitRangeBoundMode rangeBoundMode, JitRangeScanType rangeScanType, int subQueryIndex)
{
    int forwardScan = (indexScanDirection == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    int includeBound = (rangeBoundMode == JIT_RANGE_BOUND_INCLUDE) ? 1 : 0;
    llvm::Value* index = getExecContextIndex(ctx, rangeScanType, subQueryIndex);
    llvm::Value* key = getExecContextKey(ctx, JIT_RANGE_ITERATOR_START, rangeScanType, subQueryIndex);
    return llvm_util::AddFunctionCall(
        ctx, ctx->searchIteratorFunc, index, key, JIT_CONST_INT32(forwardScan), JIT_CONST_INT32(includeBound), nullptr);
}

/** @brief Adds a call to beginIterator(index). */
inline llvm::Value* AddBeginIterator(JitLlvmCodeGenContext* ctx, JitRangeScanType rangeScanType, int subQueryIndex)
{
    llvm::Value* index = getExecContextIndex(ctx, rangeScanType, subQueryIndex);
    return llvm_util::AddFunctionCall(ctx, ctx->beginIteratorFunc, index, nullptr);
}

/** @brief Adds a call to createEndIterator(index, end_iterator_key). */
inline llvm::Value* AddCreateEndIterator(JitLlvmCodeGenContext* ctx, JitIndexScanDirection indexScanDirection,
    JitRangeBoundMode rangeBoundMode, JitRangeScanType rangeScanType, int subQueryIndex = -1)
{
    int forwardScan = (indexScanDirection == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    int includeBound = (rangeBoundMode == JIT_RANGE_BOUND_INCLUDE) ? 1 : 0;
    llvm::Value* index = getExecContextIndex(ctx, rangeScanType, subQueryIndex);
    llvm::Value* key = getExecContextKey(ctx, JIT_RANGE_ITERATOR_END, rangeScanType, subQueryIndex);
    return llvm_util::AddFunctionCall(ctx,
        ctx->createEndIteratorFunc,
        index,
        key,
        JIT_CONST_INT32(forwardScan),
        JIT_CONST_INT32(includeBound),
        nullptr);
}

/** @brief Adds a call to isScanEnd(index, iterator, end_iterator). */
inline llvm::Value* AddIsScanEnd(JitLlvmCodeGenContext* ctx, JitIndexScanDirection indexScanDirection,
    JitLlvmRuntimeCursor* cursor, JitRangeScanType rangeScanType, int subQueryIndex = -1)
{
    int forwardScan = (indexScanDirection == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    llvm::Value* index = getExecContextIndex(ctx, rangeScanType, subQueryIndex);
    return llvm_util::AddFunctionCall(
        ctx, ctx->isScanEndFunc, index, cursor->begin_itr, cursor->end_itr, JIT_CONST_INT32(forwardScan), nullptr);
}

inline llvm::Value* AddCheckRowExistsInIterator(JitLlvmCodeGenContext* ctx, JitIndexScanDirection indexScanDirection,
    JitLlvmRuntimeCursor* cursor, JitRangeScanType rangeScanType, int subQueryIndex)
{
    int forwardScan = (indexScanDirection == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    llvm::Value* index = getExecContextIndex(ctx, rangeScanType, subQueryIndex);
    return llvm_util::AddFunctionCall(ctx,
        ctx->CheckRowExistsInIteratorFunc,
        index,
        cursor->begin_itr,
        cursor->end_itr,
        JIT_CONST_INT32(forwardScan),
        nullptr);
}

/** @brief Adds a cal to getRowFromIterator(index, iterator, end_iterator). */
inline llvm::Value* AddGetRowFromIterator(JitLlvmCodeGenContext* ctx, MOT::AccessType accessMode,
    JitIndexScanDirection indexScanDirection, JitLlvmRuntimeCursor* cursor, JitRangeScanType rangeScanType,
    int subQueryIndex)
{
    int innerRow = (rangeScanType == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    int forwardScan = (indexScanDirection == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    llvm::Value* index = getExecContextIndex(ctx, rangeScanType, subQueryIndex);
    return llvm_util::AddFunctionCall(ctx,
        ctx->getRowFromIteratorFunc,
        index,
        cursor->begin_itr,
        cursor->end_itr,
        JIT_CONST_INT32(accessMode),
        JIT_CONST_INT32(forwardScan),
        JIT_CONST_INT32(innerRow),
        JIT_CONST_INT32(subQueryIndex),
        nullptr);
}

/** @brief Adds a call to destroyIterator(iterator). */
inline void AddDestroyIterator(JitLlvmCodeGenContext* ctx, llvm::Value* itr)
{
    llvm_util::AddFunctionCall(ctx, ctx->destroyIteratorFunc, itr, nullptr);
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
    JitLlvmCodeGenContext* ctx, llvm::Value* itr, JitRangeIteratorType rangeItrType, JitRangeScanType rangeScanType)
{
    int beginItr = (rangeItrType == JIT_RANGE_ITERATOR_START) ? 1 : 0;
    int innerScan = (rangeScanType == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm_util::AddFunctionCall(
        ctx, ctx->setStateIteratorFunc, itr, JIT_CONST_INT32(beginItr), JIT_CONST_INT32(innerScan), nullptr);
}

/** @brief Adds a call to isStateIteratorNull(begin_itr). */
inline llvm::Value* AddIsStateIteratorNull(
    JitLlvmCodeGenContext* ctx, JitRangeIteratorType rangeItrType, JitRangeScanType rangeScanType)
{
    int beginItr = (rangeItrType == JIT_RANGE_ITERATOR_START) ? 1 : 0;
    int innerScan = (rangeScanType == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    return llvm_util::AddFunctionCall(
        ctx, ctx->isStateIteratorNullFunc, JIT_CONST_INT32(beginItr), JIT_CONST_INT32(innerScan), nullptr);
}

/** @brief Adds a call to isStateScanEnd(index, forward_scan). */
inline llvm::Value* AddIsStateScanEnd(
    JitLlvmCodeGenContext* ctx, JitIndexScanDirection indexScanDirection, JitRangeScanType rangeScanType)
{
    int forwardScan = (indexScanDirection == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    int innerScan = (rangeScanType == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    return llvm_util::AddFunctionCall(
        ctx, ctx->isStateScanEndFunc, JIT_CONST_INT32(forwardScan), JIT_CONST_INT32(innerScan), nullptr);
}

/** @brief Adds a cal to getRowFromStateIterator(index, accessMode, forward_scan). */
inline llvm::Value* AddGetRowFromStateIterator(JitLlvmCodeGenContext* ctx, MOT::AccessType accessMode,
    JitIndexScanDirection indexScanDirection, JitRangeScanType rangeScanType)
{
    int forwardScan = (indexScanDirection == JIT_INDEX_SCAN_FORWARD) ? 1 : 0;
    int innerScan = (rangeScanType == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    return llvm_util::AddFunctionCall(ctx,
        ctx->getRowFromStateIteratorFunc,
        JIT_CONST_INT32(accessMode),
        JIT_CONST_INT32(forwardScan),
        JIT_CONST_INT32(innerScan),
        nullptr);
}

/** @brief Adds a call to destroyStateIterators(). */
inline void AddDestroyStateIterators(JitLlvmCodeGenContext* ctx, JitRangeScanType rangeScanType)
{
    int innerScan = (rangeScanType == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm_util::AddFunctionCall(ctx, ctx->destroyStateIteratorsFunc, JIT_CONST_INT32(innerScan), nullptr);
}

/** @brief Adds a call to setStateScanEndFlag(scanEnded). */
inline void AddSetStateScanEndFlag(JitLlvmCodeGenContext* ctx, int scanEnded, JitRangeScanType rangeScanType)
{
    int innerScan = (rangeScanType == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm_util::AddFunctionCall(
        ctx, ctx->setStateScanEndFlagFunc, JIT_CONST_INT32(scanEnded), JIT_CONST_INT32(innerScan), nullptr);
}

/** @brief Adds a call to getStateScanEndFlag(). */
inline llvm::Value* AddGetStateScanEndFlag(JitLlvmCodeGenContext* ctx, JitRangeScanType rangeScanType)
{
    int innerScan = (rangeScanType == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    return llvm_util::AddFunctionCall(ctx, ctx->getStateScanEndFlagFunc, JIT_CONST_INT32(innerScan), nullptr);
}

inline void AddResetStateRow(JitLlvmCodeGenContext* ctx, JitRangeScanType rangeScanType)
{
    int innerScan = (rangeScanType == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm_util::AddFunctionCall(ctx, ctx->resetStateRowFunc, JIT_CONST_INT32(innerScan), nullptr);
}

inline void AddSetStateRow(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitRangeScanType rangeScanType)
{
    int innerScan = (rangeScanType == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    llvm_util::AddFunctionCall(ctx, ctx->setStateRowFunc, row, JIT_CONST_INT32(innerScan), nullptr);
}

inline llvm::Value* AddGetStateRow(JitLlvmCodeGenContext* ctx, JitRangeScanType rangeScanType)
{
    int innerScan = (rangeScanType == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    return llvm_util::AddFunctionCall(ctx, ctx->getStateRowFunc, JIT_CONST_INT32(innerScan), nullptr);
}

inline void AddCopyOuterStateRow(JitLlvmCodeGenContext* ctx)
{
    llvm_util::AddFunctionCall(ctx, ctx->copyOuterStateRowFunc, nullptr);
}

inline llvm::Value* AddGetOuterStateRowCopy(JitLlvmCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->getOuterStateRowCopyFunc, nullptr);
}

inline llvm::Value* AddIsStateRowNull(JitLlvmCodeGenContext* ctx, JitRangeScanType rangeScanType)
{
    int innerScan = (rangeScanType == JIT_RANGE_SCAN_INNER) ? 1 : 0;
    return llvm_util::AddFunctionCall(ctx, ctx->isStateRowNullFunc, JIT_CONST_INT32(innerScan), nullptr);
}

inline void AddResetStateLimitCounter(JitLlvmCodeGenContext* ctx)
{
    llvm_util::AddFunctionCall(ctx, ctx->resetStateLimitCounterFunc, nullptr);
}

inline void AddIncrementStateLimitCounter(JitLlvmCodeGenContext* ctx)
{
    llvm_util::AddFunctionCall(ctx, ctx->incrementStateLimitCounterFunc, nullptr);
}

inline llvm::Value* AddGetStateLimitCounter(JitLlvmCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->getStateLimitCounterFunc, nullptr);
}

inline void AddPrepareAvgArray(JitLlvmCodeGenContext* ctx, int aggIndex, int elementType, int elementCount)
{
    llvm_util::AddFunctionCall(ctx,
        ctx->prepareAvgArrayFunc,
        JIT_CONST_INT32(aggIndex),
        JIT_CONST_INT32(elementType),
        JIT_CONST_INT32(elementCount),
        nullptr);
}

inline llvm::Value* AddLoadAvgArray(JitLlvmCodeGenContext* ctx, int aggIndex)
{
    return llvm_util::AddFunctionCall(ctx, ctx->loadAvgArrayFunc, JIT_CONST_INT32(aggIndex), nullptr);
}

inline void AddSaveAvgArray(JitLlvmCodeGenContext* ctx, int aggIndex, llvm::Value* avgArray)
{
    llvm_util::AddFunctionCall(ctx, ctx->saveAvgArrayFunc, JIT_CONST_INT32(aggIndex), avgArray, nullptr);
}

inline llvm::Value* AddComputeAvgFromArray(JitLlvmCodeGenContext* ctx, int aggIndex, int elementType)
{
    return llvm_util::AddFunctionCall(
        ctx, ctx->computeAvgFromArrayFunc, JIT_CONST_INT32(aggIndex), JIT_CONST_INT32(elementType), nullptr);
}

inline void AddResetAggValue(JitLlvmCodeGenContext* ctx, int aggIndex, int elementType)
{
    llvm_util::AddFunctionCall(
        ctx, ctx->resetAggValueFunc, JIT_CONST_INT32(aggIndex), JIT_CONST_INT32(elementType), nullptr);
}

inline llvm::Value* AddGetAggValue(JitLlvmCodeGenContext* ctx, int aggIndex)
{
    return llvm_util::AddFunctionCall(ctx, ctx->getAggValueFunc, JIT_CONST_INT32(aggIndex), nullptr);
}

inline void AddSetAggValue(JitLlvmCodeGenContext* ctx, int aggIndex, llvm::Value* value)
{
    llvm_util::AddFunctionCall(ctx, ctx->setAggValueFunc, JIT_CONST_INT32(aggIndex), value, nullptr);
}

inline llvm::Value* AddGetAggValueIsNull(JitLlvmCodeGenContext* ctx, int aggIndex)
{
    return llvm_util::AddFunctionCall(ctx, ctx->getAggValueIsNullFunc, JIT_CONST_INT32(aggIndex), nullptr);
}

inline llvm::Value* AddSetAggValueIsNull(JitLlvmCodeGenContext* ctx, int aggIndex, llvm::Value* isNull)
{
    return llvm_util::AddFunctionCall(ctx, ctx->setAggValueIsNullFunc, JIT_CONST_INT32(aggIndex), isNull, nullptr);
}

inline void AddPrepareDistinctSet(JitLlvmCodeGenContext* ctx, int aggIndex, int elementType)
{
    llvm_util::AddFunctionCall(
        ctx, ctx->prepareDistinctSetFunc, JIT_CONST_INT32(aggIndex), JIT_CONST_INT32(elementType), nullptr);
}

inline llvm::Value* AddInsertDistinctItem(JitLlvmCodeGenContext* ctx, int aggIndex, int elementType, llvm::Value* value)
{
    return llvm_util::AddFunctionCall(
        ctx, ctx->insertDistinctItemFunc, JIT_CONST_INT32(aggIndex), JIT_CONST_INT32(elementType), value, nullptr);
}

inline void AddDestroyDistinctSet(JitLlvmCodeGenContext* ctx, int aggIndex, int elementType)
{
    llvm_util::AddFunctionCall(
        ctx, ctx->destroyDistinctSetFunc, JIT_CONST_INT32(aggIndex), JIT_CONST_INT32(elementType), nullptr);
}

/** @brief Adds a call to writeTupleDatum(slot, tuple_colid, value). */
inline void AddWriteTupleDatum(JitLlvmCodeGenContext* ctx, int tupleColumnId, llvm::Value* value, llvm::Value* isNull)
{
    llvm_util::AddFunctionCall(
        ctx, ctx->writeTupleDatumFunc, ctx->slot_value, JIT_CONST_INT32(tupleColumnId), value, isNull, nullptr);
}

inline llvm::Value* AddSelectSubQueryResult(JitLlvmCodeGenContext* ctx, int subQueryIndex)
{
    return llvm_util::AddFunctionCall(ctx, ctx->selectSubQueryResultFunc, JIT_CONST_INT32(subQueryIndex), nullptr);
}

inline void AddCopyAggregateToSubQueryResult(JitLlvmCodeGenContext* ctx, int subQueryIndex)
{
    llvm_util::AddFunctionCall(ctx, ctx->copyAggregateToSubQueryResultFunc, JIT_CONST_INT32(subQueryIndex), nullptr);
}

inline llvm::Value* AddGetSubQuerySlot(JitLlvmCodeGenContext* ctx, int subQueryIndex)
{
    return llvm_util::AddFunctionCall(ctx, ctx->GetSubQuerySlotFunc, JIT_CONST_INT32(subQueryIndex), nullptr);
}

inline llvm::Value* AddGetSubQueryTable(JitLlvmCodeGenContext* ctx, int subQueryIndex)
{
    return llvm_util::AddFunctionCall(ctx, ctx->GetSubQueryTableFunc, JIT_CONST_INT32(subQueryIndex), nullptr);
}

inline llvm::Value* AddGetSubQueryIndex(JitLlvmCodeGenContext* ctx, int subQueryIndex)
{
    return llvm_util::AddFunctionCall(ctx, ctx->GetSubQueryIndexFunc, JIT_CONST_INT32(subQueryIndex), nullptr);
}

inline llvm::Value* AddGetSubQuerySearchKey(JitLlvmCodeGenContext* ctx, int subQueryIndex)
{
    return llvm_util::AddFunctionCall(ctx, ctx->GetSubQuerySearchKeyFunc, JIT_CONST_INT32(subQueryIndex), nullptr);
}

inline llvm::Value* AddGetSubQueryEndIteratorKey(JitLlvmCodeGenContext* ctx, int subQueryIndex)
{
    return llvm_util::AddFunctionCall(ctx, ctx->GetSubQueryEndIteratorKeyFunc, JIT_CONST_INT32(subQueryIndex), nullptr);
}

inline llvm::Value* AddGetConstAt(JitLlvmCodeGenContext* ctx, int constId)
{
    return llvm_util::AddFunctionCall(ctx, ctx->GetConstAtFunc, JIT_CONST_INT32(constId), nullptr);
}

inline llvm::Value* AddGetInvokeParamListInfo(JitLlvmCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->GetInvokeParamListInfoFunc, nullptr);
}

inline void AddSetParamValue(JitLlvmCodeGenContext* ctx, llvm::Value* params, llvm::Value* paramId, int paramType,
    llvm::Value* value, llvm::Value* argPos)
{
    llvm_util::AddFunctionCall(
        ctx, ctx->SetParamValueFunc, params, paramId, JIT_CONST_INT32(paramType), value, argPos, nullptr);
}

inline llvm::Value* AddInvokeStoredProcedure(JitLlvmCodeGenContext* ctx)
{
    return llvm_util::AddFunctionCall(ctx, ctx->InvokeStoredProcedureFunc, nullptr);
}

inline llvm::Value* AddConvertViaString(JitLlvmCodeGenContext* ctx, llvm::Value* value, llvm::Value* resultType,
    llvm::Value* targetType, llvm::Value* typeMod)
{
    return llvm_util::AddFunctionCall(ctx, ctx->ConvertViaStringFunc, value, resultType, targetType, typeMod, nullptr);
}

inline void AddEmitProfileData(JitLlvmCodeGenContext* ctx, uint32_t functionId, uint32_t regionId, bool startRegion)
{
    llvm::ConstantInt* functionIdValue = llvm::ConstantInt::get(ctx->INT32_T, functionId, true);
    llvm::ConstantInt* regionIdValue = llvm::ConstantInt::get(ctx->INT32_T, regionId, true);
    llvm::ConstantInt* startRegionValue = llvm::ConstantInt::get(ctx->INT32_T, startRegion ? 1 : 0, true);
    llvm_util::AddFunctionCall(
        ctx, ctx->EmitProfileDataFunc, functionIdValue, regionIdValue, startRegionValue, nullptr);
}

inline void InjectProfileDataImpl(JitLlvmCodeGenContext* ctx, const char* nameSpace, const char* queryString,
    const char* regionName, bool beginRegion)
{
    JitProfiler* jitProfiler = JitProfiler::GetInstance();
    uint32_t profileFunctionId = jitProfiler->GetProfileQueryId(nameSpace, queryString);
    if (profileFunctionId == MOT_JIT_PROFILE_INVALID_FUNCTION_ID) {
        profileFunctionId = jitProfiler->GetProfileFunctionId(queryString, InvalidOid);
    }
    if (profileFunctionId != MOT_JIT_PROFILE_INVALID_FUNCTION_ID) {
        uint32_t profileRegionId = jitProfiler->GetQueryProfileRegionId(nameSpace, queryString, regionName);
        if (profileRegionId != MOT_JIT_PROFILE_INVALID_REGION_ID) {
            AddEmitProfileData(ctx, profileFunctionId, profileRegionId, beginRegion);
        }
    }
}

inline void InjectProfileData(
    JitLlvmCodeGenContext* ctx, const char* nameSpace, const char* queryString, bool beginRegion)
{
    if (MOT::GetGlobalConfiguration().m_enableCodegenProfile) {
        InjectProfileDataImpl(ctx, nameSpace, queryString, MOT_JIT_PROFILE_REGION_TOTAL, beginRegion);
    }
}

inline void InjectInvokeProfileData(
    JitLlvmCodeGenContext* ctx, const char* nameSpace, const char* queryString, bool beginRegion)
{
    if (MOT::GetGlobalConfiguration().m_enableCodegenProfile) {
        InjectProfileDataImpl(ctx, nameSpace, queryString, MOT_JIT_PROFILE_REGION_CHILD_CALL, beginRegion);
    }
}

/** @brief Adds a call to issueDebugLog(function, msg). */
#ifdef MOT_JIT_DEBUG
inline void IssueDebugLogImpl(JitLlvmCodeGenContext* ctx, const char* function, const char* msg)
{
    llvm::Value* functionValue = ctx->m_builder->CreateGlobalStringPtr(function);
    llvm::Value* msgValue = ctx->m_builder->CreateGlobalStringPtr(msg);
    llvm_util::AddFunctionCall(ctx, ctx->debugLogFunc, functionValue, msgValue, nullptr);
}
#endif

#ifdef MOT_JIT_DEBUG
#define IssueDebugLog(msg) IssueDebugLogImpl(ctx, __func__, msg)
#else
#define IssueDebugLog(msg)
#endif
}  // namespace JitExec

#endif /* JIT_LLVM_FUNCS_H */
