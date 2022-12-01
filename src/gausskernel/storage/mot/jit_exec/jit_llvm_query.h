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
 * jit_llvm_query.h
 *    LLVM JIT-compiled Query codegen common header.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_llvm_query.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_LLVM_QUERY_H
#define JIT_LLVM_QUERY_H

/*
 * ATTENTION:
 * 1. Be sure to include gscodegen.h before anything else to avoid clash with PM definition in datetime.h.
 * 2. Be sure to include libintl.h before gscodegen.h to avoid problem with gettext.
 */
#include "libintl.h"
#include "codegen/gscodegen.h"
#include "codegen/builtinscodegen.h"
#include "codegen/datecodegen.h"
#include "codegen/timestampcodegen.h"

#include "storage/mot/jit_exec.h"
#include "jit_common.h"
#include "jit_llvm_util.h"

namespace JitExec {
/** @struct Holds instructions that evaluate in runtime to begin and end iterators of a cursor. */
struct JitLlvmRuntimeCursor {
    /** @var The iterator pointing to the beginning of the range. */
    llvm::Value* begin_itr;

    /** @var The iterator pointing to the end of the range. */
    llvm::Value* end_itr;
};

/** @struct Code generation context. */
struct JitLlvmCodeGenContext : public llvm_util::LlvmCodeGenContext {
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
    llvm::FunctionCallee debugLogFunc;
    llvm::FunctionCallee isSoftMemoryLimitReachedFunc;
    llvm::FunctionCallee getPrimaryIndexFunc;
    llvm::FunctionCallee getTableIndexFunc;
    llvm::FunctionCallee initKeyFunc;
    llvm::FunctionCallee getColumnAtFunc;

    llvm::FunctionCallee m_getExprIsNullFunc;
    llvm::FunctionCallee m_setExprIsNullFunc;
    llvm::FunctionCallee m_getExprCollationFunc;
    llvm::FunctionCallee m_setExprCollationFunc;
    llvm::FunctionCallee getDatumParamFunc;
    llvm::FunctionCallee readDatumColumnFunc;
    llvm::FunctionCallee writeDatumColumnFunc;
    llvm::FunctionCallee buildDatumKeyFunc;

    llvm::FunctionCallee setBitFunc;
    llvm::FunctionCallee resetBitmapSetFunc;
    llvm::FunctionCallee getTableFieldCountFunc;
    llvm::FunctionCallee writeRowFunc;
    llvm::FunctionCallee searchRowFunc;
    llvm::FunctionCallee createNewRowFunc;
    llvm::FunctionCallee insertRowFunc;
    llvm::FunctionCallee deleteRowFunc;
    llvm::FunctionCallee setRowNullBitsFunc;
    llvm::FunctionCallee setExprResultNullBitFunc;
    llvm::FunctionCallee execClearTupleFunc;
    llvm::FunctionCallee execStoreVirtualTupleFunc;
    llvm::FunctionCallee selectColumnFunc;
    llvm::FunctionCallee setTpProcessedFunc;
    llvm::FunctionCallee setScanEndedFunc;

    llvm::FunctionCallee copyKeyFunc;
    llvm::FunctionCallee fillKeyPatternFunc;
    llvm::FunctionCallee adjustKeyFunc;
    llvm::FunctionCallee searchIteratorFunc;
    llvm::FunctionCallee beginIteratorFunc;
    llvm::FunctionCallee createEndIteratorFunc;
    llvm::FunctionCallee isScanEndFunc;
    llvm::FunctionCallee CheckRowExistsInIteratorFunc;
    llvm::FunctionCallee getRowFromIteratorFunc;
    llvm::FunctionCallee destroyIteratorFunc;

    llvm::FunctionCallee setStateIteratorFunc;
    llvm::FunctionCallee getStateIteratorFunc;
    llvm::FunctionCallee isStateIteratorNullFunc;
    llvm::FunctionCallee isStateScanEndFunc;
    llvm::FunctionCallee getRowFromStateIteratorFunc;
    llvm::FunctionCallee destroyStateIteratorsFunc;
    llvm::FunctionCallee setStateScanEndFlagFunc;
    llvm::FunctionCallee getStateScanEndFlagFunc;

    llvm::FunctionCallee resetStateRowFunc;
    llvm::FunctionCallee setStateRowFunc;
    llvm::FunctionCallee getStateRowFunc;
    llvm::FunctionCallee copyOuterStateRowFunc;
    llvm::FunctionCallee getOuterStateRowCopyFunc;
    llvm::FunctionCallee isStateRowNullFunc;

    llvm::FunctionCallee resetStateLimitCounterFunc;
    llvm::FunctionCallee incrementStateLimitCounterFunc;
    llvm::FunctionCallee getStateLimitCounterFunc;

    llvm::FunctionCallee prepareAvgArrayFunc;
    llvm::FunctionCallee loadAvgArrayFunc;
    llvm::FunctionCallee saveAvgArrayFunc;
    llvm::FunctionCallee computeAvgFromArrayFunc;

    llvm::FunctionCallee resetAggValueFunc;
    llvm::FunctionCallee getAggValueFunc;
    llvm::FunctionCallee setAggValueFunc;

    llvm::FunctionCallee getAggValueIsNullFunc;
    llvm::FunctionCallee setAggValueIsNullFunc;

    llvm::FunctionCallee prepareDistinctSetFunc;
    llvm::FunctionCallee insertDistinctItemFunc;
    llvm::FunctionCallee destroyDistinctSetFunc;

    llvm::FunctionCallee resetTupleDatumFunc;
    llvm::FunctionCallee readTupleDatumFunc;
    llvm::FunctionCallee writeTupleDatumFunc;

    llvm::FunctionCallee selectSubQueryResultFunc;
    llvm::FunctionCallee copyAggregateToSubQueryResultFunc;

    llvm::FunctionCallee GetSubQuerySlotFunc;
    llvm::FunctionCallee GetSubQueryTableFunc;
    llvm::FunctionCallee GetSubQueryIndexFunc;
    llvm::FunctionCallee GetSubQuerySearchKeyFunc;
    llvm::FunctionCallee GetSubQueryEndIteratorKeyFunc;
    llvm::FunctionCallee GetConstAtFunc;
    llvm::FunctionCallee GetInvokeParamListInfoFunc;
    llvm::FunctionCallee SetParamValueFunc;
    llvm::FunctionCallee InvokeStoredProcedureFunc;
    llvm::FunctionCallee ConvertViaStringFunc;

    llvm::FunctionCallee EmitProfileDataFunc;

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
    llvm::Value* isNewScanValue;
    llvm::Value* bitmap_value;
    llvm::Value* inner_table_value;
    llvm::Value* inner_index_value;
    llvm::Value* inner_key_value;
    llvm::Value* inner_end_iterator_key_value;

    // sub-query data access (see JitContext::SubQueryData)
    struct SubQueryData {
        llvm::Value* m_slot;
        llvm::Value* m_table;
        llvm::Value* m_index;
        llvm::Value* m_searchKey;
        llvm::Value* m_endIteratorKey;
    };
    uint64_t m_subQueryCount;
    SubQueryData* m_subQueryData;

    // compile context
    TableInfo _table_info;
    TableInfo _inner_table_info;
    TableInfo* m_subQueryTableInfo;

    // non-primitive constants
    uint32_t m_constCount;
    Const* m_constValues;

    JitParamInfo* m_paramInfo;
    uint32_t m_paramCount;

    const char* m_queryString;
};

extern int AllocateConstId(JitLlvmCodeGenContext* ctx, int type, Datum value, bool isNull);
}  // namespace JitExec

#endif /* JIT_LLVM_QUERY_H */
