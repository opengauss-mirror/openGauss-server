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
 *    src/gausskernel/storage/mot/jit_exec/src/jit_llvm_query.h
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

namespace JitExec {
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
    llvm::Constant* beginIteratorFunc;
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

    llvm::Constant* selectSubQueryResultFunc;
    llvm::Constant* copyAggregateToSubQueryResultFunc;

    llvm::Constant* GetSubQuerySlotFunc;
    llvm::Constant* GetSubQueryTableFunc;
    llvm::Constant* GetSubQueryIndexFunc;
    llvm::Constant* GetSubQuerySearchKeyFunc;
    llvm::Constant* GetSubQueryEndIteratorKeyFunc;

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

    dorado::GsCodeGen* _code_gen;
    dorado::GsCodeGen::LlvmBuilder* _builder;
    llvm::Function* m_jittedQuery;
};
}  // namespace JitExec

#endif /* JIT_LLVM_QUERY_H */
