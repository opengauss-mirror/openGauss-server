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
 * jit_llvm_blocks.cpp
 *    Helpers to generate compound LLVM code.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_llvm_blocks.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * ATTENTION: Be sure to include jit_llvm_query.h before anything else because of gscodegen.h
 * (jit_llvm_blocks.h includes jit_llvm_query.h before anything else).
 * See jit_llvm_query.h for more details.
 */
#include "jit_llvm_blocks.h"
#include "jit_llvm_funcs.h"
#include "jit_util.h"
#include "mot_error.h"
#include "utilities.h"

#include "catalog/pg_aggregate.h"

using namespace dorado;

namespace JitExec {
DECLARE_LOGGER(JitLlvmBlocks, JitExec)

static bool ProcessJoinOpExpr(
    JitLlvmCodeGenContext* ctx, const OpExpr* op_expr, int* column_count, int* column_array, int* max_arg);
static bool ProcessJoinBoolExpr(
    JitLlvmCodeGenContext* ctx, const BoolExpr* boolexpr, int* column_count, int* column_array, int* max_arg);
static llvm::Value* ProcessFilterExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitFilter* filter, int* max_arg);
static llvm::Value* ProcessExpr(
    JitLlvmCodeGenContext* ctx, Expr* expr, int& result_type, int arg_pos, int depth, int* max_arg);
static llvm::Value* ProcessExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitExpr* expr, int* max_arg);

/*--------------------------- Helpers to generate compound LLVM code ---------------------------*/
/** @brief Creates a jitted function for code generation. Builds prototype and entry block. */
void CreateJittedFunction(JitLlvmCodeGenContext* ctx, const char* function_name)
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
    fn_prototype.addArgument(GsCodeGen::NamedVariable("isNewScan", ctx->INT32_T));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("end_iterator_key", ctx->KeyType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("inner_table", ctx->TableType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("inner_index", ctx->IndexType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("inner_key", ctx->KeyType->getPointerTo()));
    fn_prototype.addArgument(GsCodeGen::NamedVariable("inner_end_iterator_key", ctx->KeyType->getPointerTo()));

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
    ctx->isNewScanValue = llvmargs[arg_index++];
    ctx->end_iterator_key_value = llvmargs[arg_index++];
    ctx->inner_table_value = llvmargs[arg_index++];
    ctx->inner_index_value = llvmargs[arg_index++];
    ctx->inner_key_value = llvmargs[arg_index++];
    ctx->inner_end_iterator_key_value = llvmargs[arg_index++];

    for (uint32_t i = 0; i < ctx->m_subQueryCount; ++i) {
        ctx->m_subQueryData[i].m_slot = AddGetSubQuerySlot(ctx, i);
        ctx->m_subQueryData[i].m_table = AddGetSubQueryTable(ctx, i);
        ctx->m_subQueryData[i].m_index = AddGetSubQueryIndex(ctx, i);
        ctx->m_subQueryData[i].m_searchKey = AddGetSubQuerySearchKey(ctx, i);
        ctx->m_subQueryData[i].m_endIteratorKey = AddGetSubQueryEndIteratorKey(ctx, i);
    }

    IssueDebugLog("Starting execution of jitted function");
}

/** @brief Builds a code segment for checking if soft memory limit has been reached. */
void buildIsSoftMemoryLimitReached(JitLlvmCodeGenContext* ctx)
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
void buildWriteRow(JitLlvmCodeGenContext* ctx, llvm::Value* row, bool isPKey, JitLlvmRuntimeCursor* cursor)
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

/** @brief Adds code to reset the number of rows processed. */
void buildResetRowsProcessed(JitLlvmCodeGenContext* ctx)
{
    ctx->rows_processed = llvm::ConstantInt::get(ctx->INT64_T, 0, true);
}

/** @brief Adds code to increment the number of rows processed. */
void buildIncrementRowsProcessed(JitLlvmCodeGenContext* ctx)
{
    llvm::ConstantInt* one_value = llvm::ConstantInt::get(ctx->INT64_T, 1, true);
    ctx->rows_processed = ctx->_builder->CreateAdd(ctx->rows_processed, one_value);
}

/** @brief Adds code to create a new row. */
llvm::Value* buildCreateNewRow(JitLlvmCodeGenContext* ctx)
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
llvm::Value* buildSearchRow(JitLlvmCodeGenContext* ctx, MOT::AccessType access_type, JitRangeScanType range_scan_type,
    int subQueryIndex /* = -1 */)
{
    IssueDebugLog("Searching row");
    llvm::Value* row = AddSearchRow(ctx, access_type, range_scan_type, subQueryIndex);

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

bool buildFilterRow(
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
void buildInsertRow(JitLlvmCodeGenContext* ctx, llvm::Value* row)
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
void buildDeleteRow(JitLlvmCodeGenContext* ctx)
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
    JitRangeBoundMode range_bound_mode, JitRangeScanType range_scan_type, int subQueryIndex = -1)
{
    // search the row
    IssueDebugLog("Searching range start");
    llvm::Value* itr = AddSearchIterator(ctx, index_scan_direction, range_bound_mode, range_scan_type, subQueryIndex);

    JIT_IF_BEGIN(check_itr_found)
    JIT_IF_EVAL_NOT(itr)
    IssueDebugLog("Range start not found");
    JIT_RETURN_CONST(MOT::RC_LOCAL_ROW_NOT_FOUND);
    JIT_IF_END()

    IssueDebugLog("Range start found");
    return itr;
}

/** @brief Adds code to search for an iterator. */
static llvm::Value* buildBeginIterator(
    JitLlvmCodeGenContext* ctx, JitRangeScanType rangeScanType, int subQueryIndex = -1)
{
    // search the row
    IssueDebugLog("Getting begin iterator for full-scan");
    llvm::Value* itr = AddBeginIterator(ctx, rangeScanType, subQueryIndex);

    JIT_IF_BEGIN(check_itr_found)
    JIT_IF_EVAL_NOT(itr)
    IssueDebugLog("Begin iterator not found");
    JIT_RETURN_CONST(MOT::RC_LOCAL_ROW_NOT_FOUND);
    JIT_IF_END()

    IssueDebugLog("Range start found");
    return itr;
}

/** @brief Adds code to get row from iterator. */
llvm::Value* buildGetRowFromIterator(JitLlvmCodeGenContext* ctx, llvm::BasicBlock* endLoopBlock,
    MOT::AccessType access_mode, JitIndexScanDirection index_scan_direction, JitLlvmRuntimeCursor* cursor,
    JitRangeScanType range_scan_type, int subQueryIndex /* = -1 */)
{
    IssueDebugLog("Retrieving row from iterator");
    llvm::Value* row =
        AddGetRowFromIterator(ctx, access_mode, index_scan_direction, cursor, range_scan_type, subQueryIndex);

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
        if (IsPrimitiveType(result_type)) {
            result = llvm::ConstantInt::get(ctx->INT64_T, const_value->constvalue, true);
        } else {
            int constId = AllocateConstId(ctx, result_type, const_value->constvalue, const_value->constisnull);
            if (constId == -1) {
                MOT_LOG_TRACE("Failed to allocate constant identifier");
            } else {
                result = AddGetConstAt(ctx, constId, arg_pos);
            }
        }
        if (max_arg && (arg_pos > *max_arg)) {
            *max_arg = arg_pos;
        }
    } else {
        MOT_LOG_TRACE("Failed to process const expression: type %d unsupported", (int)result_type);
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
    JitLlvmCodeGenContext* ctx, llvm::Value* param, llvm::FunctionCallee unary_operator, int arg_pos)
{
    llvm::Constant* arg_pos_value = llvm::ConstantInt::get(ctx->INT32_T, arg_pos, true);
    return AddFunctionCall(ctx, unary_operator, param, arg_pos_value, nullptr);
}

/** @brief Adds call to PG binary operator. */
static llvm::Value* AddExecBinaryOperator(JitLlvmCodeGenContext* ctx, llvm::Value* lhs_param, llvm::Value* rhs_param,
    llvm::FunctionCallee binary_operator, int arg_pos)
{
    llvm::Constant* arg_pos_value = llvm::ConstantInt::get(ctx->INT32_T, arg_pos, true);
    return AddFunctionCall(ctx, binary_operator, lhs_param, rhs_param, arg_pos_value, nullptr);
}

/** @brief Adds call to PG ternary operator. */
static llvm::Value* AddExecTernaryOperator(JitLlvmCodeGenContext* ctx, llvm::Value* param1, llvm::Value* param2,
    llvm::Value* param3, llvm::FunctionCallee ternary_operator, int arg_pos)
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
    MOT_LOG_DEBUG("%*s --> Processing OP %u expression", depth, "", op_expr->opfuncid);
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (list_length(op_expr->args) > 3) {
        MOT_LOG_TRACE("Unsupported operator %u: too many arguments", op_expr->opno);
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
            MOT_LOG_TRACE("Unsupported operator function type: %u", op_expr->opfuncid);
            break;
    }

    MOT_LOG_DEBUG("%*s <-- Processing OP %u expression result: %p", depth, "", op_expr->opfuncid, result);
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
    llvm::Value* result = nullptr;
    AddSetExprArgIsNull(ctx, expr->_arg_pos, expr->_is_null);  // mark expression null status
    if (IsPrimitiveType(expr->_const_type)) {
        result = llvm::ConstantInt::get(ctx->INT64_T, expr->_value, true);
    } else {
        int constId = AllocateConstId(ctx, expr->_const_type, expr->_value, expr->_is_null);
        if (constId == -1) {
            MOT_LOG_TRACE("Failed to allocate constant identifier");
        } else {
            result = AddGetConstAt(ctx, constId, expr->_arg_pos);
        }
    }
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

    llvm::Value* args[MOT_JIT_MAX_FUNC_EXPR_ARGS] = {nullptr, nullptr, nullptr};
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

    llvm::Value* args[MOT_JIT_MAX_FUNC_EXPR_ARGS] = {nullptr, nullptr, nullptr};
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

static llvm::Value* ProcessSubLinkExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitSubLinkExpr* expr, int* max_arg)
{
    return AddSelectSubQueryResult(ctx, expr->_sub_query_index);
}

static llvm::Value* ProcessBoolExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitBoolExpr* expr, int* maxArg)
{
    llvm::Value* result = nullptr;

    llvm::Value* args[MOT_JIT_MAX_BOOL_EXPR_ARGS] = {nullptr, nullptr};
    int argNum = 0;

    for (int i = 0; i < expr->_arg_count; ++i) {
        args[i] = ProcessExpr(ctx, row, expr->_args[i], maxArg);
        if (args[i] == nullptr) {
            MOT_LOG_TRACE("Failed to process boolean sub-expression %d", argNum);
            return nullptr;
        }
    }

    llvm::Value* typedZero = llvm::ConstantInt::get(args[0]->getType(), 0, true);
    switch (expr->_bool_expr_type) {
        case NOT_EXPR: {
            llvm::Value* notResult = ctx->_builder->CreateICmpEQ(args[0], typedZero);  // equivalent to NOT
            result = ctx->_builder->CreateIntCast(notResult, args[0]->getType(), true);
            break;
        }

        case AND_EXPR:
            result = ctx->_builder->CreateSelect(args[0], args[1], typedZero);
            break;

        case OR_EXPR: {
            llvm::Value* typedOne = llvm::ConstantInt::get(args[0]->getType(), 1, true);
            result = ctx->_builder->CreateSelect(args[0], typedOne, args[1]);
            break;
        }

        default:
            MOT_LOG_TRACE("Unsupported boolean expression type: %d", (int)expr->_bool_expr_type);
            break;
    }

    return result;
}

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
    } else if (expr->_expr_type == JIT_EXPR_TYPE_SUBLINK) {
        result = ProcessSubLinkExpr(ctx, row, (JitSubLinkExpr*)expr, max_arg);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_BOOL) {
        result = ProcessBoolExpr(ctx, row, (JitBoolExpr*)expr, max_arg);
    } else {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for query: unsupported target expression type: %d", (int)expr->_expr_type);
    }

    return result;
}

bool buildScanExpression(JitLlvmCodeGenContext* ctx, JitColumnExpr* expr, int* max_arg,
    JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type, llvm::Value* outer_row, int subQueryIndex)
{
    llvm::Value* value = ProcessExpr(ctx, outer_row, expr->_expr, max_arg);
    if (value == nullptr) {
        MOT_LOG_TRACE("buildScanExpression(): Failed to process expression with type %d", (int)expr->_expr->_expr_type);
        return false;
    } else {
        llvm::Value* column = AddGetColumnAt(ctx,
            expr->_table_column_id,
            range_scan_type,  // no need to translate to zero-based index (first column is null bits)
            subQueryIndex);
        int index_colid = -1;
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            index_colid = ctx->_inner_table_info.m_columnMap[expr->_table_column_id];
        } else if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
            index_colid = ctx->_table_info.m_columnMap[expr->_table_column_id];
        } else if (range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            index_colid = ctx->m_subQueryTableInfo[subQueryIndex].m_columnMap[expr->_table_column_id];
        }
        AddBuildDatumKey(
            ctx, column, index_colid, value, expr->_column_type, range_itr_type, range_scan_type, subQueryIndex);
    }
    return true;
}

bool buildPointScan(JitLlvmCodeGenContext* ctx, JitColumnExprArray* exprArray, int* maxArg,
    JitRangeScanType rangeScanType, llvm::Value* outerRow, int exprCount /* = -1 */, int subQueryIndex /* = -1 */)
{
    if (exprCount == -1) {
        exprCount = exprArray->_count;
    }
    AddInitSearchKey(ctx, rangeScanType, subQueryIndex);
    for (int i = 0; i < exprCount; ++i) {
        JitColumnExpr* expr = &exprArray->_exprs[i];

        // validate the expression refers to the right table (in search expressions array, all expressions refer to the
        // same table)
        if (rangeScanType == JIT_RANGE_SCAN_INNER) {
            if (expr->_table != ctx->_inner_table_info.m_table) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Generate LLVM JIT Code",
                    "Invalid expression table (expected inner table %s, got %s)",
                    ctx->_inner_table_info.m_table->GetTableName().c_str(),
                    expr->_table->GetTableName().c_str());
                return false;
            }
        } else if (rangeScanType == JIT_RANGE_SCAN_MAIN) {
            if (expr->_table != ctx->_table_info.m_table) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Generate LLVM JIT Code",
                    "Invalid expression table (expected main/outer table %s, got %s)",
                    ctx->_table_info.m_table->GetTableName().c_str(),
                    expr->_table->GetTableName().c_str());
                return false;
            }
        } else if (rangeScanType == JIT_RANGE_SCAN_SUB_QUERY) {
            if (expr->_table != ctx->m_subQueryTableInfo[subQueryIndex].m_table) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Generate TVM JIT Code",
                    "Invalid expression table (expected sub-query table %s, got %s)",
                    ctx->m_subQueryTableInfo[subQueryIndex].m_table->GetTableName().c_str(),
                    expr->_table->GetTableName().c_str());
                return false;
            }
        }

        // prepare the sub-expression
        if (!buildScanExpression(ctx, expr, maxArg, JIT_RANGE_ITERATOR_START, rangeScanType, outerRow, subQueryIndex)) {
            return false;
        }
    }
    return true;
}

bool writeRowColumns(
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

        // set bit for incremental redo
        if (is_update) {
            AddSetBit(ctx, column_expr->_table_column_id - 1);
        }
    }

    return true;
}

bool selectRowColumns(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitSelectExprArray* expr_array, int* max_arg,
    JitRangeScanType range_scan_type, int subQueryIndex /* = -1 */)
{
    bool result = true;
    for (int i = 0; i < expr_array->_count; ++i) {
        JitSelectExpr* expr = &expr_array->_exprs[i];
        // we skip expressions that select from other tables
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            if (expr->_column_expr->_table != ctx->_inner_table_info.m_table) {
                continue;
            }
        } else if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
            if (expr->_column_expr->_table != ctx->_table_info.m_table) {
                continue;
            }
        } else if (range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            if (expr->_column_expr->_table != ctx->m_subQueryTableInfo[subQueryIndex].m_table) {
                continue;
            }
        }
        result = AddSelectColumn(
            ctx, row, expr->_column_expr->_column_id, expr->_tuple_column_id, range_scan_type, subQueryIndex);
        if (!result) {
            break;
        }
    }

    return result;
}

static bool buildClosedRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, llvm::Value* outer_row, int subQueryIndex)
{
    // a closed range scan starts just like a point scan (without enough search expressions) and then adds key patterns
    bool result =
        buildPointScan(ctx, &index_scan->_search_exprs, max_arg, range_scan_type, outer_row, -1, subQueryIndex);
    if (result) {
        AddCopyKey(ctx, range_scan_type, subQueryIndex);

        // now fill each key with the right pattern for the missing pkey columns in the where clause
        bool ascending = (index_scan->_sort_order == JIT_QUERY_SORT_ASCENDING);

        int* index_column_offsets = nullptr;
        const uint16_t* key_length = nullptr;
        int index_column_count = 0;
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            index_column_offsets = ctx->_inner_table_info.m_indexColumnOffsets;
            key_length = ctx->_inner_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_inner_table_info.m_index->GetNumFields();
        } else if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
            index_column_offsets = ctx->_table_info.m_indexColumnOffsets;
            key_length = ctx->_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_table_info.m_index->GetNumFields();
        } else if (range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            index_column_offsets = ctx->m_subQueryTableInfo[subQueryIndex].m_indexColumnOffsets;
            key_length = ctx->m_subQueryTableInfo[subQueryIndex].m_index->GetLengthKeyFields();
            index_column_count = ctx->m_subQueryTableInfo[subQueryIndex].m_index->GetNumFields();
        }

        int first_zero_column = index_scan->_column_count;
        for (int i = first_zero_column; i < index_column_count; ++i) {
            int offset = index_column_offsets[i];
            int size = key_length[i];
            MOT_LOG_DEBUG(
                "Filling begin/end iterator pattern for missing pkey fields at offset %d, size %d", offset, size);
            AddFillKeyPattern(
                ctx, ascending ? 0x00 : 0xFF, offset, size, JIT_RANGE_ITERATOR_START, range_scan_type, subQueryIndex);
            AddFillKeyPattern(
                ctx, ascending ? 0xFF : 0x00, offset, size, JIT_RANGE_ITERATOR_END, range_scan_type, subQueryIndex);
        }

        AddAdjustKey(ctx,
            ascending ? 0x00 : 0xFF,
            JIT_RANGE_ITERATOR_START,
            range_scan_type,  // currently this is relevant only for secondary index searches
            subQueryIndex);
        AddAdjustKey(ctx,
            ascending ? 0xFF : 0x00,
            JIT_RANGE_ITERATOR_END,
            range_scan_type,  // currently this is relevant only for secondary index searches
            subQueryIndex);
    }
    return result;
}

static void BuildAscendingSemiOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound,
    llvm::Value* outerRow, int subQueryIndex, int offset, int size, JitColumnExpr* lastExpr)
{
    if ((indexScan->_last_dim_op1 == JIT_WOC_LESS_THAN) || (indexScan->_last_dim_op1 == JIT_WOC_LESS_EQUALS)) {
        // this is an upper bound operator on an ascending semi-open scan so we fill the begin key with zeros,
        // and the end key with the value
        AddFillKeyPattern(ctx, 0x00, offset, size, JIT_RANGE_ITERATOR_START, rangeScanType, subQueryIndex);
        buildScanExpression(ctx, lastExpr, maxArg, JIT_RANGE_ITERATOR_END, rangeScanType, outerRow, subQueryIndex);
        *beginRangeBound = JIT_RANGE_BOUND_INCLUDE;
        *endRangeBound =
            (indexScan->_last_dim_op1 == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
    } else {
        // this is a lower bound operator on an ascending semi-open scan so we fill the begin key with the
        // value, and the end key with 0xFF
        buildScanExpression(ctx, lastExpr, maxArg, JIT_RANGE_ITERATOR_START, rangeScanType, outerRow, subQueryIndex);
        AddFillKeyPattern(ctx, 0xFF, offset, size, JIT_RANGE_ITERATOR_END, rangeScanType, subQueryIndex);
        *beginRangeBound =
            (indexScan->_last_dim_op1 == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
        *endRangeBound = JIT_RANGE_BOUND_INCLUDE;
    }
}

static void BuildDescendingSemiOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound,
    llvm::Value* outerRow, int subQueryIndex, int offset, int size, JitColumnExpr* lastExpr)
{
    if ((indexScan->_last_dim_op1 == JIT_WOC_LESS_THAN) || (indexScan->_last_dim_op1 == JIT_WOC_LESS_EQUALS)) {
        // this is an upper bound operator on a descending semi-open scan so we fill the begin key with value,
        // and the end key with zeroes
        buildScanExpression(ctx, lastExpr, maxArg, JIT_RANGE_ITERATOR_START, rangeScanType, outerRow, subQueryIndex);
        AddFillKeyPattern(ctx, 0x00, offset, size, JIT_RANGE_ITERATOR_END, rangeScanType, subQueryIndex);
        *beginRangeBound =
            (indexScan->_last_dim_op1 == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
        *endRangeBound = JIT_RANGE_BOUND_INCLUDE;
    } else {
        // this is a lower bound operator on a descending semi-open scan so we fill the begin key with 0xFF, and
        // the end key with the value
        AddFillKeyPattern(ctx, 0xFF, offset, size, JIT_RANGE_ITERATOR_START, rangeScanType, subQueryIndex);
        buildScanExpression(ctx, lastExpr, maxArg, JIT_RANGE_ITERATOR_END, rangeScanType, outerRow, subQueryIndex);
        *beginRangeBound = JIT_RANGE_BOUND_INCLUDE;
        *endRangeBound =
            (indexScan->_last_dim_op1 == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
    }
}

static bool buildSemiOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound,
    llvm::Value* outerRow, int subQueryIndex)
{
    // an open range scan starts just like a point scan (with not enough search expressions) and then adds key patterns
    // we do not use the last search expression
    bool result = buildPointScan(ctx,
        &indexScan->_search_exprs,
        maxArg,
        rangeScanType,
        outerRow,
        indexScan->_search_exprs._count - 1,
        subQueryIndex);
    if (result) {
        AddCopyKey(ctx, rangeScanType, subQueryIndex);

        // now fill each key with the right pattern for the missing pkey columns in the where clause
        bool ascending = (indexScan->_sort_order == JIT_QUERY_SORT_ASCENDING);

        int* index_column_offsets = nullptr;
        const uint16_t* key_length = nullptr;
        int index_column_count = 0;
        if (rangeScanType == JIT_RANGE_SCAN_INNER) {
            index_column_offsets = ctx->_inner_table_info.m_indexColumnOffsets;
            key_length = ctx->_inner_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_inner_table_info.m_index->GetNumFields();
        } else if (rangeScanType == JIT_RANGE_SCAN_MAIN) {
            index_column_offsets = ctx->_table_info.m_indexColumnOffsets;
            key_length = ctx->_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_table_info.m_index->GetNumFields();
        } else if (rangeScanType == JIT_RANGE_SCAN_SUB_QUERY) {
            index_column_offsets = ctx->m_subQueryTableInfo[subQueryIndex].m_indexColumnOffsets;
            key_length = ctx->m_subQueryTableInfo[subQueryIndex].m_index->GetLengthKeyFields();
            index_column_count = ctx->m_subQueryTableInfo[subQueryIndex].m_index->GetNumFields();
        }

        // prepare offset and size for last column in search
        int last_dim_column = indexScan->_column_count - 1;
        int offset = index_column_offsets[last_dim_column];
        int size = key_length[last_dim_column];

        // now we fill the last dimension (override extra work of point scan above)
        int last_expr_index = indexScan->_search_exprs._count - 1;
        JitColumnExpr* last_expr = &indexScan->_search_exprs._exprs[last_expr_index];
        if (ascending) {
            BuildAscendingSemiOpenRangeScan(ctx,
                indexScan,
                maxArg,
                rangeScanType,
                beginRangeBound,
                endRangeBound,
                outerRow,
                subQueryIndex,
                offset,
                size,
                last_expr);
        } else {
            BuildDescendingSemiOpenRangeScan(ctx,
                indexScan,
                maxArg,
                rangeScanType,
                beginRangeBound,
                endRangeBound,
                outerRow,
                subQueryIndex,
                offset,
                size,
                last_expr);
        }

        // now fill the rest as usual
        int first_zero_column = indexScan->_column_count;
        for (int i = first_zero_column; i < index_column_count; ++i) {
            int offset = index_column_offsets[i];
            int size = key_length[i];
            MOT_LOG_DEBUG(
                "Filling begin/end iterator pattern for missing pkey fields at offset %d, size %d", offset, size);
            AddFillKeyPattern(
                ctx, ascending ? 0x00 : 0xFF, offset, size, JIT_RANGE_ITERATOR_START, rangeScanType, subQueryIndex);
            AddFillKeyPattern(
                ctx, ascending ? 0xFF : 0x00, offset, size, JIT_RANGE_ITERATOR_END, rangeScanType, subQueryIndex);
        }

        AddAdjustKey(ctx,
            ascending ? 0x00 : 0xFF,
            JIT_RANGE_ITERATOR_START,
            rangeScanType,  // currently this is relevant only for secondary index searches
            subQueryIndex);
        AddAdjustKey(ctx,
            ascending ? 0xFF : 0x00,
            JIT_RANGE_ITERATOR_END,
            rangeScanType,  // currently this is relevant only for secondary index searches
            subQueryIndex);
    }
    return result;
}

static void BuildAscendingOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound,
    llvm::Value* outerRow, int subQueryIndex, JitWhereOperatorClass beforeLastDimOp, JitWhereOperatorClass lastDimOp,
    JitColumnExpr* beforeLastExpr, JitColumnExpr* lastExpr)
{
    if ((beforeLastDimOp == JIT_WOC_LESS_THAN) || (beforeLastDimOp == JIT_WOC_LESS_EQUALS)) {
        MOT_ASSERT((lastDimOp == JIT_WOC_GREATER_THAN) || (lastDimOp == JIT_WOC_GREATER_EQUALS));
        // the before-last operator is an upper bound operator on an ascending open scan so we fill the begin
        // key with the last value, and the end key with the before-last value
        buildScanExpression(ctx,
            lastExpr,
            maxArg,
            JIT_RANGE_ITERATOR_START,
            rangeScanType,
            outerRow,  // lower bound on begin iterator key
            subQueryIndex);
        buildScanExpression(ctx,
            beforeLastExpr,
            maxArg,
            JIT_RANGE_ITERATOR_END,
            rangeScanType,
            outerRow,  // upper bound on end iterator key
            subQueryIndex);
        *beginRangeBound = (lastDimOp == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
        *endRangeBound = (beforeLastDimOp == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
    } else {
        MOT_ASSERT((lastDimOp == JIT_WOC_LESS_THAN) || (lastDimOp == JIT_WOC_LESS_EQUALS));
        // the before-last operator is a lower bound operator on an ascending open scan so we fill the begin key
        // with the before-last value, and the end key with the last value
        buildScanExpression(ctx,
            beforeLastExpr,
            maxArg,
            JIT_RANGE_ITERATOR_START,
            rangeScanType,
            outerRow,  // lower bound on begin iterator key
            subQueryIndex);
        buildScanExpression(ctx,
            lastExpr,
            maxArg,
            JIT_RANGE_ITERATOR_END,
            rangeScanType,
            outerRow,  // upper bound on end iterator key
            subQueryIndex);
        *beginRangeBound =
            (beforeLastDimOp == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
        *endRangeBound = (lastDimOp == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
    }
}

static void BuildDescendingOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound,
    llvm::Value* outerRow, int subQueryIndex, JitWhereOperatorClass beforeLastDimOp, JitWhereOperatorClass lastDimOp,
    JitColumnExpr* beforeLastExpr, JitColumnExpr* lastExpr)
{
    if ((beforeLastDimOp == JIT_WOC_LESS_THAN) || (beforeLastDimOp == JIT_WOC_LESS_EQUALS)) {
        MOT_ASSERT((lastDimOp == JIT_WOC_GREATER_THAN) || (lastDimOp == JIT_WOC_GREATER_EQUALS));
        // the before-last operator is an upper bound operator on an descending open scan so we fill the begin
        // key with the last value, and the end key with the before-last value
        buildScanExpression(ctx,
            beforeLastExpr,
            maxArg,
            JIT_RANGE_ITERATOR_START,
            rangeScanType,
            outerRow,  // upper bound on begin iterator key
            subQueryIndex);
        buildScanExpression(ctx,
            lastExpr,
            maxArg,
            JIT_RANGE_ITERATOR_END,
            rangeScanType,
            outerRow,  // lower bound on end iterator key
            subQueryIndex);
        *beginRangeBound = (beforeLastDimOp == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
        *endRangeBound = (lastDimOp == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
    } else {
        MOT_ASSERT((lastDimOp == JIT_WOC_LESS_THAN) || (lastDimOp == JIT_WOC_LESS_EQUALS));
        // the before-last operator is a lower bound operator on an descending open scan so we fill the begin
        // key with the last value, and the end key with the before-last value
        buildScanExpression(ctx,
            lastExpr,
            maxArg,
            JIT_RANGE_ITERATOR_START,
            rangeScanType,
            outerRow,  // upper bound on begin iterator key
            subQueryIndex);
        buildScanExpression(ctx,
            beforeLastExpr,
            maxArg,
            JIT_RANGE_ITERATOR_END,
            rangeScanType,
            outerRow,  // lower bound on end iterator key
            subQueryIndex);
        *beginRangeBound = (lastDimOp == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
        *endRangeBound =
            (beforeLastDimOp == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
    }
}

static bool buildOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitRangeBoundMode* begin_range_bound, JitRangeBoundMode* end_range_bound,
    llvm::Value* outer_row, int subQueryIndex)
{
    // an open range scan starts just like a point scan (with not enough search expressions) and then adds key patterns
    // we do not use the last two expressions
    bool result = buildPointScan(ctx,
        &index_scan->_search_exprs,
        max_arg,
        range_scan_type,
        outer_row,
        index_scan->_search_exprs._count - 2,
        subQueryIndex);
    if (result) {
        AddCopyKey(ctx, range_scan_type, subQueryIndex);

        // now fill each key with the right pattern for the missing pkey columns in the where clause
        bool ascending = (index_scan->_sort_order == JIT_QUERY_SORT_ASCENDING);

        int* index_column_offsets = nullptr;
        const uint16_t* key_length = nullptr;
        int index_column_count = 0;
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            index_column_offsets = ctx->_inner_table_info.m_indexColumnOffsets;
            key_length = ctx->_inner_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_inner_table_info.m_index->GetNumFields();
        } else if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
            index_column_offsets = ctx->_table_info.m_indexColumnOffsets;
            key_length = ctx->_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_table_info.m_index->GetNumFields();
        } else if (range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            index_column_offsets = ctx->m_subQueryTableInfo[subQueryIndex].m_indexColumnOffsets;
            key_length = ctx->m_subQueryTableInfo[subQueryIndex].m_index->GetLengthKeyFields();
            index_column_count = ctx->m_subQueryTableInfo[subQueryIndex].m_index->GetNumFields();
        }

        // now we fill the last dimension (override extra work of point scan above)
        JitWhereOperatorClass before_last_dim_op = index_scan->_last_dim_op1;  // avoid confusion, and give proper names
        JitWhereOperatorClass last_dim_op = index_scan->_last_dim_op2;         // avoid confusion, and give proper names
        int last_expr_index = index_scan->_search_exprs._count - 1;
        JitColumnExpr* last_expr = &index_scan->_search_exprs._exprs[last_expr_index];
        JitColumnExpr* before_last_expr = &index_scan->_search_exprs._exprs[last_expr_index - 1];
        if (ascending) {
            BuildAscendingOpenRangeScan(ctx,
                index_scan,
                max_arg,
                range_scan_type,
                begin_range_bound,
                end_range_bound,
                outer_row,
                subQueryIndex,
                before_last_dim_op,
                last_dim_op,
                before_last_expr,
                last_expr);
        } else {
            BuildDescendingOpenRangeScan(ctx,
                index_scan,
                max_arg,
                range_scan_type,
                begin_range_bound,
                end_range_bound,
                outer_row,
                subQueryIndex,
                before_last_dim_op,
                last_dim_op,
                before_last_expr,
                last_expr);
        }

        // now fill the rest as usual
        int first_zero_column = index_scan->_column_count;
        for (int i = first_zero_column; i < index_column_count; ++i) {
            int offset = index_column_offsets[i];
            int size = key_length[i];
            MOT_LOG_DEBUG(
                "Filling begin/end iterator pattern for missing pkey fields at offset %d, size %d", offset, size);
            AddFillKeyPattern(
                ctx, ascending ? 0x00 : 0xFF, offset, size, JIT_RANGE_ITERATOR_START, range_scan_type, subQueryIndex);
            AddFillKeyPattern(
                ctx, ascending ? 0xFF : 0x00, offset, size, JIT_RANGE_ITERATOR_END, range_scan_type, subQueryIndex);
        }

        AddAdjustKey(ctx,
            ascending ? 0x00 : 0xFF,
            JIT_RANGE_ITERATOR_START,
            range_scan_type,  // currently this is relevant only for secondary index searches
            subQueryIndex);
        AddAdjustKey(ctx,
            ascending ? 0xFF : 0x00,
            JIT_RANGE_ITERATOR_END,
            range_scan_type,  // currently this is relevant only for secondary index searches
            subQueryIndex);
    }
    return result;
}

static bool buildRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitRangeBoundMode* begin_range_bound, JitRangeBoundMode* end_range_bound,
    llvm::Value* outer_row, int subQueryIndex = -1)
{
    bool result = false;

    // if this is a point scan we generate two identical keys for the iterators
    if (index_scan->_scan_type == JIT_INDEX_SCAN_POINT) {
        result =
            buildPointScan(ctx, &index_scan->_search_exprs, max_arg, range_scan_type, outer_row, -1, subQueryIndex);
        if (result) {
            AddCopyKey(ctx, range_scan_type, subQueryIndex);
            *begin_range_bound = JIT_RANGE_BOUND_INCLUDE;
            *end_range_bound = JIT_RANGE_BOUND_INCLUDE;
        }
    } else if (index_scan->_scan_type == JIT_INDEX_SCAN_CLOSED) {
        result = buildClosedRangeScan(ctx, index_scan, max_arg, range_scan_type, outer_row, subQueryIndex);
        if (result) {
            *begin_range_bound = JIT_RANGE_BOUND_INCLUDE;
            *end_range_bound = JIT_RANGE_BOUND_INCLUDE;
        }
    } else if (index_scan->_scan_type == JIT_INDEX_SCAN_SEMI_OPEN) {
        result = buildSemiOpenRangeScan(
            ctx, index_scan, max_arg, range_scan_type, begin_range_bound, end_range_bound, outer_row, subQueryIndex);
    } else if (index_scan->_scan_type == JIT_INDEX_SCAN_OPEN) {
        result = buildOpenRangeScan(
            ctx, index_scan, max_arg, range_scan_type, begin_range_bound, end_range_bound, outer_row, subQueryIndex);
    } else if (index_scan->_scan_type == JIT_INDEX_SCAN_FULL) {
        result = true;  // no keys used
        *begin_range_bound = JIT_RANGE_BOUND_INCLUDE;
        *end_range_bound = JIT_RANGE_BOUND_INCLUDE;
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
    if (index_scan->_scan_type == JIT_INDEX_SCAN_FULL) {
        llvm::Value* itr = buildBeginIterator(ctx, range_scan_type);
        AddSetStateIterator(ctx, itr, JIT_RANGE_ITERATOR_START, range_scan_type);
    } else {
        llvm::Value* itr = buildSearchIterator(ctx, index_scan->_scan_direction, begin_range_bound, range_scan_type);
        AddSetStateIterator(ctx, itr, JIT_RANGE_ITERATOR_START, range_scan_type);

        // create end iterator and save it in execution state
        IssueDebugLog("Creating end iterator from end search key, and saving in execution state");
        itr = AddCreateEndIterator(ctx, index_scan->_scan_direction, end_range_bound, range_scan_type);
        AddSetStateIterator(ctx, itr, JIT_RANGE_ITERATOR_END, range_scan_type);
    }

    // initialize state scan variables
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
        MOT_LOG_BEGIN(MOT::LogLevel::LL_TRACE, "Added unconditional branch result:");
        std::string s;
        llvm::raw_string_ostream os(s);
        ctx->_builder->GetInsertBlock()->back().print(os);
        MOT_LOG_APPEND(MOT::LogLevel::LL_TRACE, "%s", s.c_str());
        MOT_LOG_END(MOT::LogLevel::LL_TRACE);
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
    llvm::BasicBlock* isStateScanEndBlock = JIT_CURRENT_BLOCK();  // remember current block if filter fails
    llvm::Value* res = AddIsStateScanEnd(ctx, index_scan->_scan_direction, range_scan_type);
    JIT_IF_EVAL(res)
    // fail scan block
    IssueDebugLog("Scan ended, raising internal state scan end flag");
    AddSetStateScanEndFlag(ctx, 1, range_scan_type);
    JIT_ELSE()
    // now get row from iterator
    IssueDebugLog("State scan not ended - Retrieving row from iterator");
    row = AddGetRowFromStateIterator(ctx, access_mode, index_scan->_scan_direction, range_scan_type);

    // check if row was found
    JIT_IF_BEGIN(test_row_found)
    JIT_IF_EVAL_NOT(row)
    // row not found branch
    IssueDebugLog("Could not retrieve row from state iterator, raising internal state scan end flag");
    AddSetStateScanEndFlag(ctx, 1, range_scan_type);
    JIT_ELSE()
    // row found, check for additional filters, if not passing filter then go back to execute IsStateScanEnd()
    if (!buildFilterRow(ctx, row, &index_scan->_filters, max_arg, isStateScanEndBlock)) {
        MOT_LOG_TRACE("Failed to generate jitted code for query: failed to build filter expressions for row");
        return false;
    }
    // row passed all filters, so save it in state outer row
    AddSetStateRow(ctx, row, range_scan_type);
    if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
        AddSetStateScanEndFlag(ctx, 0, JIT_RANGE_SCAN_INNER);  // reset inner scan flag for JOIN queries
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

llvm::Value* buildPrepareStateScanRow(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan,
    JitRangeScanType range_scan_type, MOT::AccessType access_mode, int* max_arg, llvm::Value* outer_row,
    llvm::BasicBlock* next_block, llvm::BasicBlock** loop_block)
{
    // prepare stateful scan if not done so already
    if (!buildPrepareStateScan(ctx, index_scan, max_arg, range_scan_type, outer_row)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range JOIN query: unsupported %s WHERE clause type",
            (range_scan_type == JIT_RANGE_SCAN_MAIN) ? "outer" : "inner");
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

JitLlvmRuntimeCursor buildRangeCursor(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitIndexScanDirection indexScanDirection, llvm::Value* outerRow,
    int subQueryIndex /* = -1 */)
{
    JitLlvmRuntimeCursor result = {nullptr, nullptr};
    JitRangeBoundMode beginRangeBound = JIT_RANGE_BOUND_NONE;
    JitRangeBoundMode endRangeBound = JIT_RANGE_BOUND_NONE;
    if (!buildRangeScan(
            ctx, indexScan, maxArg, rangeScanType, &beginRangeBound, &endRangeBound, outerRow, subQueryIndex)) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for aggregate range JOIN query: unsupported %s-loop WHERE clause type",
            outerRow ? "inner" : "outer");
        return result;
    }

    // build range iterators
    result.begin_itr = buildSearchIterator(ctx, indexScanDirection, beginRangeBound, rangeScanType, subQueryIndex);
    result.end_itr =
        AddCreateEndIterator(ctx, indexScanDirection, endRangeBound, rangeScanType, subQueryIndex);  // forward scan

    return result;
}

bool prepareAggregateAvg(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate)
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

bool prepareAggregateSum(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate)
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

bool prepareAggregateMaxMin(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate)
{
    AddResetAggMaxMinNull(ctx);
    return true;
}

bool prepareAggregateCount(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate)
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

bool prepareAggregate(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate)
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
    bool result = false;
    llvm::Value* aggregateExpr = nullptr;

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
            aggregateExpr = buildAggregateMax(ctx, aggregate, current_aggregate, var_expr);
            break;

        case JIT_AGGREGATE_MIN:
            aggregateExpr = buildAggregateMin(ctx, aggregate, current_aggregate, var_expr);
            break;

        default:
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "JIT Compile", "Invalid aggregate operator %d", (int)aggregate->_aggreaget_op);
            break;
    }
    JIT_IF_END()

    if (aggregateExpr != nullptr) {
        AddSetAggValue(ctx, aggregateExpr);
        result = true;
    }

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

bool buildAggregateRow(
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

void buildAggregateResult(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate)
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

void buildCheckLimit(JitLlvmCodeGenContext* ctx, int limit_count)
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

bool selectJoinRows(
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
}  // namespace JitExec
