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
 *    src/gausskernel/storage/mot/jit_exec/src/jit_llvm_blocks.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JIT_LLVM_BLOCKS_H
#define JIT_LLVM_BLOCKS_H

// Be sure to include jit_llvm_query.h before anything else because of global.h.
// See jit_llvm_query.h for more details.
#include "jit_llvm_query.h"

namespace JitExec {
/** @brief Builds a code segment for checking if soft memory limit has been reached. */
void buildIsSoftMemoryLimitReached(JitLlvmCodeGenContext* ctx);

/** @brief Builds a code segment for writing datum value to a column. */
void buildWriteDatumColumn(JitLlvmCodeGenContext* ctx, llvm::Value* row, int colid, llvm::Value* datum_value);

/** @brief Builds a code segment for writing a row. */
void buildWriteRow(JitLlvmCodeGenContext* ctx, llvm::Value* row, bool isPKey, JitLlvmRuntimeCursor* cursor);

/** @brief Process a join expression (WHERE clause) and generate code to build a search key. */
bool ProcessJoinExpr(JitLlvmCodeGenContext* ctx, Expr* expr, int* column_count, int* column_array, int* max_arg);

/** @brief Process an operator expression (process only "COLUMN equals EXPR" operators). */
bool ProcessJoinOpExpr(
    JitLlvmCodeGenContext* ctx, const OpExpr* op_expr, int* column_count, int* column_array, int* max_arg);

/** @brief Process a boolean operator (process only AND operators, since we handle only point queries, or full-prefix
 * range update). */
bool ProcessJoinBoolExpr(
    JitLlvmCodeGenContext* ctx, const BoolExpr* boolexpr, int* column_count, int* column_array, int* max_arg);

/** @brief Creates a jitted function for code generation. Builds prototype and entry block. */
void CreateJittedFunction(JitLlvmCodeGenContext* ctx, const char* function_name);

/** @brief Adds code to reset the number of rows processed. */
void buildResetRowsProcessed(JitLlvmCodeGenContext* ctx);

/** @brief Adds code to increment the number of rows processed. */
void buildIncrementRowsProcessed(JitLlvmCodeGenContext* ctx);

/** @brief Adds code to create a new row. */
llvm::Value* buildCreateNewRow(JitLlvmCodeGenContext* ctx);

/** @brief Adds code to search for a row by a key. */
llvm::Value* buildSearchRow(
    JitLlvmCodeGenContext* ctx, MOT::AccessType access_type, JitRangeScanType range_scan_type, int subQueryIndex = -1);

llvm::Value* buildFilter(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitFilter* filter, int* max_arg);

bool buildFilterRow(
    JitLlvmCodeGenContext* ctx, llvm::Value* row, JitFilterArray* filters, int* max_arg, llvm::BasicBlock* next_block);

/** @brief Adds code to insert a new row. */
void buildInsertRow(JitLlvmCodeGenContext* ctx, llvm::Value* row);

/** @brief Adds code to delete a row. */
void buildDeleteRow(JitLlvmCodeGenContext* ctx);

/** @brief Adds code to search for an iterator. */
llvm::Value* buildSearchIterator(JitLlvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitRangeBoundMode range_bound_mode, JitRangeScanType range_scan_type, int subQueryIndex = -1);

/** @brief Adds code to search for an iterator. */
llvm::Value* buildBeginIterator(JitLlvmCodeGenContext* ctx, JitRangeScanType rangeScanType, int subQueryIndex = -1);

/** @brief Adds code to get row from iterator. */
llvm::Value* buildGetRowFromIterator(JitLlvmCodeGenContext* ctx, llvm::BasicBlock* endLoopBlock,
    MOT::AccessType access_mode, JitIndexScanDirection index_scan_direction, JitLlvmRuntimeCursor* cursor,
    JitRangeScanType range_scan_type, int subQueryIndex = -1);

/** @brief Process constant expression. */
llvm::Value* ProcessConstExpr(
    JitLlvmCodeGenContext* ctx, const Const* const_value, int& result_type, int arg_pos, int depth, int* max_arg);

/** @brief Process Param expression. */
llvm::Value* ProcessParamExpr(
    JitLlvmCodeGenContext* ctx, const Param* param, int& result_type, int arg_pos, int depth, int* max_arg);

/** @brief Process Relabel expression as Param expression. */
llvm::Value* ProcessRelabelExpr(
    JitLlvmCodeGenContext* ctx, RelabelType* relabel_type, int& result_type, int arg_pos, int depth, int* max_arg);

/** @brief Proess Var expression. */
llvm::Value* ProcessVarExpr(
    JitLlvmCodeGenContext* ctx, const Var* var, int& result_type, int arg_pos, int depth, int* max_arg);

/** @brief Adds call to PG unary operator. */
llvm::Value* AddExecUnaryOperator(
    JitLlvmCodeGenContext* ctx, llvm::Value* param, llvm::Constant* unary_operator, int arg_pos);

/** @brief Adds call to PG binary operator. */
llvm::Value* AddExecBinaryOperator(JitLlvmCodeGenContext* ctx, llvm::Value* lhs_param, llvm::Value* rhs_param,
    llvm::Constant* binary_operator, int arg_pos);

/** @brief Adds call to PG ternary operator. */
llvm::Value* AddExecTernaryOperator(JitLlvmCodeGenContext* ctx, llvm::Value* param1, llvm::Value* param2,
    llvm::Value* param3, llvm::Constant* ternary_operator, int arg_pos);

/** @brief Process operator expression. */
llvm::Value* ProcessOpExpr(
    JitLlvmCodeGenContext* ctx, const OpExpr* op_expr, int& result_type, int arg_pos, int depth, int* max_arg);

/** @brief Process function expression. */
llvm::Value* ProcessFuncExpr(
    JitLlvmCodeGenContext* ctx, const FuncExpr* func_expr, int& result_type, int arg_pos, int depth, int* max_arg);

llvm::Value* ProcessFilterExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitFilter* filter, int* max_arg);

/** @brief Process an expression. Generates code to evaluate the expression. */
llvm::Value* ProcessExpr(
    JitLlvmCodeGenContext* ctx, Expr* expr, int& result_type, int arg_pos, int depth, int* max_arg);

llvm::Value* ProcessConstExpr(JitLlvmCodeGenContext* ctx, const JitConstExpr* expr, int* max_arg);

llvm::Value* ProcessParamExpr(JitLlvmCodeGenContext* ctx, const JitParamExpr* expr, int* max_arg);

llvm::Value* ProcessVarExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, const JitVarExpr* expr, int* max_arg);

llvm::Value* ProcessOpExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitOpExpr* expr, int* max_arg);

llvm::Value* ProcessFuncExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitFuncExpr* expr, int* max_arg);

llvm::Value* ProcessSubLinkExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitSubLinkExpr* expr, int* max_arg);

llvm::Value* ProcessBoolExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitBoolExpr* expr, int* maxArg);

llvm::Value* ProcessExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitExpr* expr, int* max_arg);

bool buildScanExpression(JitLlvmCodeGenContext* ctx, JitColumnExpr* expr, int* max_arg,
    JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type, llvm::Value* outer_row, int subQueryIndex);

bool buildPointScan(JitLlvmCodeGenContext* ctx, JitColumnExprArray* exprArray, int* maxArg,
    JitRangeScanType rangeScanType, llvm::Value* outerRow, int exprCount = -1, int subQueryIndex = -1);

bool writeRowColumns(
    JitLlvmCodeGenContext* ctx, llvm::Value* row, JitColumnExprArray* expr_array, int* max_arg, bool is_update);

bool selectRowColumns(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitSelectExprArray* expr_array, int* max_arg,
    JitRangeScanType range_scan_type, int subQueryIndex = -1);

bool buildClosedRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, llvm::Value* outer_row, int subQueryIndex);

bool buildSemiOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound,
    llvm::Value* outerRow, int subQueryIndex);

bool buildOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitRangeBoundMode* begin_range_bound, JitRangeBoundMode* end_range_bound,
    llvm::Value* outer_row, int subQueryIndex);

bool buildRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitRangeBoundMode* begin_range_bound, JitRangeBoundMode* end_range_bound,
    llvm::Value* outer_row, int subQueryIndex = -1);

bool buildPrepareStateScan(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, llvm::Value* outer_row);

bool buildPrepareStateRow(JitLlvmCodeGenContext* ctx, MOT::AccessType access_mode, JitIndexScan* index_scan,
    int* max_arg, JitRangeScanType range_scan_type, llvm::BasicBlock* next_block);

llvm::Value* buildPrepareStateScanRow(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan,
    JitRangeScanType range_scan_type, MOT::AccessType access_mode, int* max_arg, llvm::Value* outer_row,
    llvm::BasicBlock* next_block, llvm::BasicBlock** loop_block);

JitLlvmRuntimeCursor buildRangeCursor(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitIndexScanDirection indexScanDirection, llvm::Value* outerRow,
    int subQueryIndex = -1);

bool prepareAggregateAvg(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate);

bool prepareAggregateSum(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate);

bool prepareAggregateMaxMin(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate);

bool prepareAggregateCount(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate);

bool prepareDistinctSet(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate);

bool prepareAggregate(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate);

llvm::Value* buildAggregateAvg(
    JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, llvm::Value* current_aggregate, llvm::Value* var_expr);

llvm::Value* buildAggregateSum(
    JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, llvm::Value* current_aggregate, llvm::Value* var_expr);

llvm::Value* buildAggregateMax(
    JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, llvm::Value* current_aggregate, llvm::Value* var_expr);

llvm::Value* buildAggregateMin(
    JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, llvm::Value* current_aggregate, llvm::Value* var_expr);

llvm::Value* buildAggregateCount(
    JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, llvm::Value* count_aggregate);

bool buildAggregateMaxMin(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate, llvm::Value* var_expr);

bool buildAggregateTuple(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate, llvm::Value* var_expr);

bool buildAggregateRow(
    JitLlvmCodeGenContext* ctx, JitAggregate* aggregate, llvm::Value* row, llvm::BasicBlock* next_block);

void buildAggregateResult(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate);

void buildCheckLimit(JitLlvmCodeGenContext* ctx, int limit_count);

bool selectJoinRows(
    JitLlvmCodeGenContext* ctx, llvm::Value* outer_row_copy, llvm::Value* inner_row, JitJoinPlan* plan, int* max_arg);
} // namespace JitExec

#endif /* JIT_LLVM_BLOCKS_H */
