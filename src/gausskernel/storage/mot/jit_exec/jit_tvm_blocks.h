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
 * jit_tvm_blocks.h
 *    Helpers to generate compound TVM-jitted code.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_tvm_blocks.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_TVM_BLOCKS_H
#define JIT_TVM_BLOCKS_H

/*
 * ATTENTION: Be sure to include jit_tvm_query.h before anything else because of libintl.h
 * See jit_tvm_query.h for more details.
 */
#include "jit_tvm_query.h"
#include "jit_plan.h"

namespace JitExec {
void CreateJittedFunction(JitTvmCodeGenContext* ctx, const char* function_name, const char* query_string);

void buildIsSoftMemoryLimitReached(JitTvmCodeGenContext* ctx);

void buildWriteRow(JitTvmCodeGenContext* ctx, tvm::Instruction* row, bool isPKey, JitTvmRuntimeCursor* cursor);

void buildResetRowsProcessed(JitTvmCodeGenContext* ctx);

void buildIncrementRowsProcessed(JitTvmCodeGenContext* ctx);

tvm::Instruction* buildCreateNewRow(JitTvmCodeGenContext* ctx);

tvm::Instruction* buildSearchRow(
    JitTvmCodeGenContext* ctx, MOT::AccessType access_type, JitRangeScanType range_scan_type, int subQueryIndex = -1);

bool buildFilterRow(JitTvmCodeGenContext* ctx, tvm::Instruction* row, JitFilterArray* filters, int* max_arg,
    tvm::BasicBlock* next_block);

void buildInsertRow(JitTvmCodeGenContext* ctx, tvm::Instruction* row);

void buildDeleteRow(JitTvmCodeGenContext* ctx);

tvm::Instruction* buildGetRowFromIterator(JitTvmCodeGenContext* ctx, tvm::BasicBlock* endLoopBlock,
    MOT::AccessType access_mode, JitIndexScanDirection index_scan_direction, JitTvmRuntimeCursor* cursor,
    JitRangeScanType range_scan_type, int subQueryIndex = -1);

bool buildPointScan(JitTvmCodeGenContext* ctx, JitColumnExprArray* expr_array, int* max_arg,
    JitRangeScanType range_scan_type, tvm::Instruction* outer_row, int expr_count = -1, int subQueryIndex = -1);

bool writeRowColumns(
    JitTvmCodeGenContext* ctx, tvm::Instruction* row, JitColumnExprArray* expr_array, int* max_arg, bool is_update);

bool selectRowColumns(JitTvmCodeGenContext* ctx, tvm::Instruction* row, JitSelectExprArray* expr_array, int* max_arg,
    JitRangeScanType range_scan_type, int subQueryIndex = -1);

tvm::Instruction* buildPrepareStateScanRow(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan,
    JitRangeScanType range_scan_type, MOT::AccessType access_mode, int* max_arg, tvm::Instruction* outer_row,
    tvm::BasicBlock* next_block, tvm::BasicBlock** loop_block);

JitTvmRuntimeCursor buildRangeCursor(JitTvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitIndexScanDirection indexScanDirection, tvm::Instruction* outerRow,
    int subQueryIndex = -1);

bool prepareAggregate(JitTvmCodeGenContext* ctx, JitAggregate* aggregate);

bool buildAggregateRow(
    JitTvmCodeGenContext* ctx, JitAggregate* aggregate, tvm::Instruction* row, tvm::BasicBlock* next_block);

void buildAggregateResult(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate);

void buildCheckLimit(JitTvmCodeGenContext* ctx, int limit_count);

bool selectJoinRows(JitTvmCodeGenContext* ctx, tvm::Instruction* outer_row_copy, tvm::Instruction* inner_row,
    JitJoinPlan* plan, int* max_arg);
}  // namespace JitExec

#endif /* JIT_TVM_BLOCKS_H */
