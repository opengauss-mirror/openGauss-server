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
 *    src/gausskernel/storage/mot/jit_exec/src/jit_tvm_blocks.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_TVM_BLOCKS_H
#define JIT_TVM_BLOCKS_H

// Be sure to include jit_tvm_query.h before anything else because of global.h.
// See jit_tvm_query.h for more details.
#include "jit_tvm_query.h"

namespace JitExec {
void CreateJittedFunction(JitTvmCodeGenContext* ctx, const char* function_name, const char* query_string);

bool ProcessJoinExpr(JitTvmCodeGenContext* ctx, Expr* expr, int* column_count, int* column_array, int* max_arg);

bool ProcessJoinOpExpr(
    JitTvmCodeGenContext* ctx, const OpExpr* op_expr, int* column_count, int* column_array, int* max_arg);

bool ProcessJoinBoolExpr(
    JitTvmCodeGenContext* ctx, const BoolExpr* boolexpr, int* column_count, int* column_array, int* max_arg);

void buildIsSoftMemoryLimitReached(JitTvmCodeGenContext* ctx);

tvm::Instruction* buildExpression(JitTvmCodeGenContext* ctx, tvm::Expression* expr);

void buildWriteDatumColumn(JitTvmCodeGenContext* ctx, tvm::Instruction* row, int colid, tvm::Instruction* datum_value);

void buildWriteRow(JitTvmCodeGenContext* ctx, tvm::Instruction* row, bool isPKey, JitTvmRuntimeCursor* cursor);

void buildResetRowsProcessed(JitTvmCodeGenContext* ctx);

void buildIncrementRowsProcessed(JitTvmCodeGenContext* ctx);

tvm::Instruction* buildCreateNewRow(JitTvmCodeGenContext* ctx);

tvm::Instruction* buildSearchRow(
    JitTvmCodeGenContext* ctx, MOT::AccessType access_type, JitRangeScanType range_scan_type, int subQueryIndex = -1);

tvm::Expression* buildFilter(JitTvmCodeGenContext* ctx, tvm::Instruction* row, JitFilter* filter, int* max_arg);

bool buildFilterRow(JitTvmCodeGenContext* ctx, tvm::Instruction* row, JitFilterArray* filters, int* max_arg,
    tvm::BasicBlock* next_block);

void buildInsertRow(JitTvmCodeGenContext* ctx, tvm::Instruction* row);

void buildDeleteRow(JitTvmCodeGenContext* ctx);

tvm::Instruction* buildSearchIterator(JitTvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitRangeBoundMode range_bound_mode, JitRangeScanType range_scan_type, int subQueryIndex = -1);

/** @brief Adds code to search for an iterator. */
tvm::Instruction* buildBeginIterator(JitTvmCodeGenContext* ctx, JitRangeScanType rangeScanType, int subQueryIndex = -1);

tvm::Instruction* buildGetRowFromIterator(JitTvmCodeGenContext* ctx, tvm::BasicBlock* endLoopBlock,
    MOT::AccessType access_mode, JitIndexScanDirection index_scan_direction, JitTvmRuntimeCursor* cursor,
    JitRangeScanType range_scan_type, int subQueryIndex = -1);

tvm::Expression* ProcessConstExpr(
    JitTvmCodeGenContext* ctx, const Const* const_value, int& result_type, int arg_pos, int depth, int* max_arg);

tvm::Expression* ProcessParamExpr(
    JitTvmCodeGenContext* ctx, const Param* param, int& result_type, int arg_pos, int depth, int* max_arg);

tvm::Expression* ProcessRelabelExpr(
    JitTvmCodeGenContext* ctx, RelabelType* relabel_type, int& result_type, int arg_pos, int depth, int* max_arg);

tvm::Expression* ProcessVarExpr(
    JitTvmCodeGenContext* ctx, const Var* var, int& result_type, int arg_pos, int depth, int* max_arg);

tvm::Expression* ProcessOpExpr(
    JitTvmCodeGenContext* ctx, const OpExpr* op_expr, int& result_type, int arg_pos, int depth, int* max_arg);

tvm::Expression* ProcessFuncExpr(
    JitTvmCodeGenContext* ctx, const FuncExpr* func_expr, int& result_type, int arg_pos, int depth, int* max_arg);

tvm::Expression* ProcessExpr(
    JitTvmCodeGenContext* ctx, Expr* expr, int& result_type, int arg_pos, int depth, int* max_arg);

tvm::Expression* ProcessConstExpr(JitTvmCodeGenContext* ctx, const JitConstExpr* expr, int* max_arg);

tvm::Expression* ProcessParamExpr(JitTvmCodeGenContext* ctx, const JitParamExpr* expr, int* max_arg);

tvm::Expression* ProcessVarExpr(JitTvmCodeGenContext* ctx, tvm::Instruction* row, JitVarExpr* expr, int* max_arg);

tvm::Expression* ProcessOpExpr(JitTvmCodeGenContext* ctx, tvm::Instruction* row, JitOpExpr* expr, int* max_arg);

tvm::Expression* ProcessFuncExpr(JitTvmCodeGenContext* ctx, tvm::Instruction* row, JitFuncExpr* expr, int* max_arg);

tvm::Expression* ProcessFilterExpr(JitTvmCodeGenContext* ctx, tvm::Instruction* row, JitFilter* filter, int* max_arg);

tvm::Expression* ProcessSubLinkExpr(
    JitTvmCodeGenContext* ctx, tvm::Instruction* row, JitSubLinkExpr* expr, int* max_arg);

tvm::Expression* ProcessBoolExpr(JitTvmCodeGenContext* ctx, tvm::Instruction* row, JitBoolExpr* expr, int* maxArg);

tvm::Expression* ProcessExpr(JitTvmCodeGenContext* ctx, tvm::Instruction* row, JitExpr* expr, int* max_arg);

bool buildScanExpression(JitTvmCodeGenContext* ctx, JitColumnExpr* expr, int* max_arg,
    JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type, tvm::Instruction* outer_row,
    int subQueryIndex);

bool buildPointScan(JitTvmCodeGenContext* ctx, JitColumnExprArray* expr_array, int* max_arg,
    JitRangeScanType range_scan_type, tvm::Instruction* outer_row, int expr_count = -1, int subQueryIndex = -1);

bool writeRowColumns(
    JitTvmCodeGenContext* ctx, tvm::Instruction* row, JitColumnExprArray* expr_array, int* max_arg, bool is_update);

bool selectRowColumns(JitTvmCodeGenContext* ctx, tvm::Instruction* row, JitSelectExprArray* expr_array, int* max_arg,
    JitRangeScanType range_scan_type, int subQueryIndex = -1);

bool buildClosedRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, tvm::Instruction* outerRow, int subQueryIndex);

bool buildSemiOpenRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitRangeBoundMode* begin_range_bound, JitRangeBoundMode* end_range_bound,
    tvm::Instruction* outer_row, int subQueryIndex);

bool buildOpenRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitRangeBoundMode* begin_range_bound, JitRangeBoundMode* end_range_bound,
    tvm::Instruction* outer_row, int subQueryIndex);

bool buildRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg, JitRangeScanType rangeScanType,
    JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound, tvm::Instruction* outerRow,
    int subQueryIndex = -1);

bool buildPrepareStateScan(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, tvm::Instruction* outer_row);

bool buildPrepareStateRow(JitTvmCodeGenContext* ctx, MOT::AccessType access_mode, JitIndexScan* index_scan,
    int* max_arg, JitRangeScanType range_scan_type, tvm::BasicBlock* next_block);

tvm::Instruction* buildPrepareStateScanRow(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan,
    JitRangeScanType range_scan_type, MOT::AccessType access_mode, int* max_arg, tvm::Instruction* outer_row,
    tvm::BasicBlock* next_block, tvm::BasicBlock** loop_block);

JitTvmRuntimeCursor buildRangeCursor(JitTvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitIndexScanDirection indexScanDirection, tvm::Instruction* outerRow,
    int subQueryIndex = -1);

bool prepareAggregateAvg(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate);

bool prepareAggregateSum(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate);

bool prepareAggregateMaxMin(JitTvmCodeGenContext* ctx, JitAggregate* aggregate);

bool prepareAggregateCount(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate);

bool prepareDistinctSet(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate);

bool prepareAggregate(JitTvmCodeGenContext* ctx, JitAggregate* aggregate);

tvm::Expression* buildAggregateAvg(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate,
    tvm::Expression* current_aggregate, tvm::Expression* var_expr);

tvm::Expression* buildAggregateSum(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate,
    tvm::Expression* current_aggregate, tvm::Expression* var_expr);

tvm::Expression* buildAggregateMax(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate,
    tvm::Expression* current_aggregate, tvm::Expression* var_expr);

tvm::Expression* buildAggregateMin(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate,
    tvm::Expression* current_aggregate, tvm::Expression* var_expr);

tvm::Expression* buildAggregateCount(
    JitTvmCodeGenContext* ctx, const JitAggregate* aggregate, tvm::Expression* count_aggregate);

bool buildAggregateMaxMin(JitTvmCodeGenContext* ctx, JitAggregate* aggregate, tvm::Expression* var_expr);

bool buildAggregateTuple(JitTvmCodeGenContext* ctx, JitAggregate* aggregate, tvm::Expression* var_expr);

bool buildAggregateRow(
    JitTvmCodeGenContext* ctx, JitAggregate* aggregate, tvm::Instruction* row, tvm::BasicBlock* next_block);

void buildAggregateResult(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate);

void buildCheckLimit(JitTvmCodeGenContext* ctx, int limit_count);

bool selectJoinRows(JitTvmCodeGenContext* ctx, tvm::Instruction* outer_row_copy, tvm::Instruction* inner_row,
    JitJoinPlan* plan, int* max_arg);
}  // namespace JitExec

#endif /* JIT_TVM_BLOCKS_H */
