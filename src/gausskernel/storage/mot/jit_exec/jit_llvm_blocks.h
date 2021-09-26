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
 * jit_llvm_blocks.h
 *    Helpers to generate compound LLVM code.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_llvm_blocks.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_LLVM_BLOCKS_H
#define JIT_LLVM_BLOCKS_H

/*
 * ATTENTION: Be sure to include jit_llvm_query.h before anything else because of gscodegen.h
 * See jit_llvm_query.h for more details.
 */
#include "jit_llvm_query.h"
#include "jit_plan.h"
#include "jit_llvm_util.h"

namespace JitExec {
/** @brief Creates a jitted function for code generation. Builds prototype and entry block. */
void CreateJittedFunction(JitLlvmCodeGenContext* ctx, const char* function_name);

/** @brief Builds a code segment for checking if soft memory limit has been reached. */
void buildIsSoftMemoryLimitReached(JitLlvmCodeGenContext* ctx);

/** @brief Builds a code segment for writing a row. */
void buildWriteRow(JitLlvmCodeGenContext* ctx, llvm::Value* row, bool isPKey, JitLlvmRuntimeCursor* cursor);

/** @brief Adds code to reset the number of rows processed. */
void buildResetRowsProcessed(JitLlvmCodeGenContext* ctx);

/** @brief Adds code to increment the number of rows processed. */
void buildIncrementRowsProcessed(JitLlvmCodeGenContext* ctx);

/** @brief Adds code to create a new row. */
llvm::Value* buildCreateNewRow(JitLlvmCodeGenContext* ctx);

/** @brief Adds code to search for a row by a key. */
llvm::Value* buildSearchRow(
    JitLlvmCodeGenContext* ctx, MOT::AccessType access_type, JitRangeScanType range_scan_type, int subQueryIndex = -1);

bool buildFilterRow(
    JitLlvmCodeGenContext* ctx, llvm::Value* row, JitFilterArray* filters, int* max_arg, llvm::BasicBlock* next_block);

/** @brief Adds code to insert a new row. */
void buildInsertRow(JitLlvmCodeGenContext* ctx, llvm::Value* row);

/** @brief Adds code to delete a row. */
void buildDeleteRow(JitLlvmCodeGenContext* ctx);

/** @brief Adds code to get row from iterator. */
llvm::Value* buildGetRowFromIterator(JitLlvmCodeGenContext* ctx, llvm::BasicBlock* endLoopBlock,
    MOT::AccessType access_mode, JitIndexScanDirection index_scan_direction, JitLlvmRuntimeCursor* cursor,
    JitRangeScanType range_scan_type, int subQueryIndex = -1);

bool buildPointScan(JitLlvmCodeGenContext* ctx, JitColumnExprArray* exprArray, int* maxArg,
    JitRangeScanType rangeScanType, llvm::Value* outerRow, int exprCount = -1, int subQueryIndex = -1);

bool writeRowColumns(
    JitLlvmCodeGenContext* ctx, llvm::Value* row, JitColumnExprArray* expr_array, int* max_arg, bool is_update);

bool selectRowColumns(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitSelectExprArray* expr_array, int* max_arg,
    JitRangeScanType range_scan_type, int subQueryIndex = -1);

llvm::Value* buildPrepareStateScanRow(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan,
    JitRangeScanType range_scan_type, MOT::AccessType access_mode, int* max_arg, llvm::Value* outer_row,
    llvm::BasicBlock* next_block, llvm::BasicBlock** loop_block);

JitLlvmRuntimeCursor buildRangeCursor(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitIndexScanDirection indexScanDirection, llvm::Value* outerRow,
    int subQueryIndex = -1);

bool prepareAggregate(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate);

bool buildAggregateRow(
    JitLlvmCodeGenContext* ctx, JitAggregate* aggregate, llvm::Value* row, llvm::BasicBlock* next_block);

void buildAggregateResult(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate);

void buildCheckLimit(JitLlvmCodeGenContext* ctx, int limit_count);

bool selectJoinRows(
    JitLlvmCodeGenContext* ctx, llvm::Value* outer_row_copy, llvm::Value* inner_row, JitJoinPlan* plan, int* max_arg);
}  // namespace JitExec

#endif /* JIT_LLVM_BLOCKS_H */
