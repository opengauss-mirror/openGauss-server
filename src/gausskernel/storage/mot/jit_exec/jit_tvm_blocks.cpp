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
 * jit_tvm_blocks.cpp
 *    Helpers to generate compound TVM-jitted code.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_tvm_blocks.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * ATTENTION: Be sure to include jit_tvm_query.h before anything else because of libintl.h
 * (jit_tvm_blocks.h includes jit_tvm_query.h before anything else).
 * See jit_tvm_query.h for more details.
 */
#include "jit_tvm_blocks.h"
#include "jit_tvm_funcs.h"
#include "jit_tvm_util.h"
#include "jit_util.h"
#include "mot_error.h"
#include "utilities.h"

#include "catalog/pg_aggregate.h"

using namespace tvm;

namespace JitExec {
DECLARE_LOGGER(JitTvmBlocks, JitExec)

static bool ProcessJoinOpExpr(
    JitTvmCodeGenContext* ctx, const OpExpr* op_expr, int* column_count, int* column_array, int* max_arg);
static bool ProcessJoinBoolExpr(
    JitTvmCodeGenContext* ctx, const BoolExpr* boolexpr, int* column_count, int* column_array, int* max_arg);
static Instruction* buildExpression(JitTvmCodeGenContext* ctx, Expression* expr);
static Expression* ProcessExpr(
    JitTvmCodeGenContext* ctx, Expr* expr, int& result_type, int arg_pos, int depth, int* max_arg);
static Expression* ProcessFilterExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitFilter* filter, int* max_arg);
static Expression* ProcessExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitExpr* expr, int* max_arg);

void CreateJittedFunction(JitTvmCodeGenContext* ctx, const char* function_name, const char* query_string)
{
    ctx->m_jittedQuery = ctx->_builder->createFunction(function_name, query_string);
    IssueDebugLog("Starting execution of jitted function");
}

static bool ProcessJoinExpr(JitTvmCodeGenContext* ctx, Expr* expr, int* column_count, int* column_array, int* max_arg)
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

static bool ProcessJoinOpExpr(
    JitTvmCodeGenContext* ctx, const OpExpr* op_expr, int* column_count, int* column_array, int* max_arg)
{
    bool result = false;

    // process only point queries
    if (IsWhereOperatorSupported(op_expr->opno)) {
        Instruction* value = nullptr;
        Expression* value_expr = nullptr;
        ListCell* lc1 = nullptr;
        int colid = -1;
        int vartype = -1;
        int result_type = -1;

        foreach (lc1, op_expr->args) {
            Expr* expr = (Expr*)lfirst(lc1);
            if (expr->type ==
                T_RelabelType) {  // sometimes relabel expression hides the inner expression, so we peel it off
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
                value_expr = ProcessExpr(ctx, expr, result_type, 0, 0, max_arg);
                if (value_expr == nullptr) {
                    MOT_LOG_TRACE("Unsupported operand type %d while processing Join OpExpr", (int)expr->type);
                } else if (!IsTypeSupported(result_type)) {
                    MOT_LOG_TRACE("ProcessJoinOpExpr(): Unsupported result type %d", result_type);
                } else {
                    value = buildExpression(ctx, value_expr);
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
            Instruction* column = AddGetColumnAt(ctx,
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

static bool ProcessJoinBoolExpr(
    JitTvmCodeGenContext* ctx, const BoolExpr* boolexpr, int* column_count, int* column_array, int* max_arg)
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

#ifdef DEFINE_BLOCK
#undef DEFINE_BLOCK
#endif
#define DEFINE_BLOCK(block_name, unused) BasicBlock* block_name = ctx->_builder->CreateBlock(#block_name);

void buildIsSoftMemoryLimitReached(JitTvmCodeGenContext* ctx)
{
    JIT_IF_BEGIN(soft_limit_reached)
    JIT_IF_EVAL_CMP(AddIsSoftMemoryLimitReached(ctx), JIT_CONST(0), JIT_ICMP_NE)
    IssueDebugLog("Soft memory limit reached");
    JIT_RETURN_CONST(MOT::RC_MEMORY_ALLOCATION_ERROR);
    JIT_ELSE()
    IssueDebugLog("Soft memory limit not reached");
    JIT_IF_END()
}

static Instruction* buildExpression(JitTvmCodeGenContext* ctx, Expression* expr)
{
    Instruction* expr_value = ctx->_builder->addExpression(expr);

    // generate code for checking expression evaluation only if the expression is fallible
    if (expr->canFail()) {
        Instruction* expr_rc = ctx->_builder->addGetExpressionRC();

        JIT_IF_BEGIN(check_expression_failed)
        JIT_IF_EVAL_CMP(expr_rc, JIT_CONST(MOT::RC_OK), JIT_ICMP_NE)
        IssueDebugLog("Expression execution failed");
        JIT_RETURN(expr_rc);
        JIT_IF_END()

        IssueDebugLog("Expression execution succeeded");
    }

    return expr_value;
}

static void buildWriteDatumColumn(JitTvmCodeGenContext* ctx, Instruction* row, int colid, Instruction* datum_value)
{
    //   ATTENTION: The datum_value expression-instruction MUST be already evaluated before this code is executed
    //              That is the reason why the expression-instruction was added to the current block and now we
    //              use only a register-ref instruction
    Instruction* set_null_bit_res = AddSetExprResultNullBit(ctx, row, colid);
    IssueDebugLog("Set null bit");

    if (ctx->_table_info.m_table->GetField(colid)->m_isNotNull) {
        JIT_IF_BEGIN(check_null_violation)
        JIT_IF_EVAL_CMP(set_null_bit_res, JIT_CONST(MOT::RC_OK), JIT_ICMP_NE)
        IssueDebugLog("Null constraint violated");
        JIT_RETURN(set_null_bit_res);
        JIT_IF_END()
    }

    // now check if the result is not null, and if so write column datum
    Instruction* is_expr_null = AddGetExprArgIsNull(ctx, 0);
    JIT_IF_BEGIN(check_expr_null)
    JIT_IF_EVAL_NOT(is_expr_null)
    IssueDebugLog("Encountered non-null expression result, writing datum column");
    AddWriteDatumColumn(ctx, colid, row, datum_value);
    JIT_IF_END()
}

void buildWriteRow(JitTvmCodeGenContext* ctx, Instruction* row, bool isPKey, JitTvmRuntimeCursor* cursor)
{
    IssueDebugLog("Writing row");
    Instruction* write_row_res = AddWriteRow(ctx, row);

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

void buildResetRowsProcessed(JitTvmCodeGenContext* ctx)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) ResetRowsProcessedInstruction());
}

void buildIncrementRowsProcessed(JitTvmCodeGenContext* ctx)
{
    (void)ctx->_builder->addInstruction(new (std::nothrow) IncrementRowsProcessedInstruction());
}

Instruction* buildCreateNewRow(JitTvmCodeGenContext* ctx)
{
    Instruction* row = AddCreateNewRow(ctx);

    JIT_IF_BEGIN(check_row_created)
    JIT_IF_EVAL_NOT(row)
    IssueDebugLog("Failed to create row");
    JIT_RETURN_CONST(MOT::RC_MEMORY_ALLOCATION_ERROR);
    JIT_IF_END()

    return row;
}

Instruction* buildSearchRow(JitTvmCodeGenContext* ctx, MOT::AccessType access_type, JitRangeScanType range_scan_type,
    int subQueryIndex /* = -1 */)
{
    IssueDebugLog("Searching row");
    Instruction* row = AddSearchRow(ctx, access_type, range_scan_type, subQueryIndex);

    JIT_IF_BEGIN(check_row_found)
    JIT_IF_EVAL_NOT(row)
    IssueDebugLog("Row not found");
    JIT_RETURN_CONST(MOT::RC_LOCAL_ROW_NOT_FOUND);
    JIT_IF_END()

    IssueDebugLog("Row found");
    return row;
}

static Expression* buildFilter(JitTvmCodeGenContext* ctx, Instruction* row, JitFilter* filter, int* max_arg)
{
    Expression* result = nullptr;
    Expression* lhs_expr = ProcessExpr(ctx, row, filter->_lhs_operand, max_arg);
    if (lhs_expr == nullptr) {
        MOT_LOG_TRACE(
            "buildFilter(): Failed to process LHS expression with type %d", (int)filter->_lhs_operand->_expr_type);
    } else {
        Expression* rhs_expr = ProcessExpr(ctx, row, filter->_rhs_operand, max_arg);
        if (rhs_expr == nullptr) {
            MOT_LOG_TRACE(
                "buildFilter(): Failed to process RHS expression with type %d", (int)filter->_rhs_operand->_expr_type);
            delete lhs_expr;
        } else {
            result = ProcessFilterExpr(ctx, row, filter, max_arg);
        }
    }
    return result;
}

bool buildFilterRow(
    JitTvmCodeGenContext* ctx, Instruction* row, JitFilterArray* filters, int* max_arg, BasicBlock* next_block)
{
    // 1. for each filter expression we generate the equivalent instructions which should evaluate to true or false
    // 2. We assume that all filters are applied with AND operator between them (imposed during query analysis/plan
    // phase)
    for (int i = 0; i < filters->_filter_count; ++i) {
        Expression* filter_expr = buildFilter(ctx, row, &filters->_scan_filters[i], max_arg);
        if (filter_expr == nullptr) {
            MOT_LOG_TRACE("buildFilterRow(): Failed to process filter expression %d", i);
            return false;
        }

        JIT_IF_BEGIN(filter_expr)
        IssueDebugLog("Checking of row passes filter");
        Instruction* value = buildExpression(ctx, filter_expr);  // this is a boolean datum result
        JIT_IF_EVAL_NOT(value)
        IssueDebugLog("Row did not pass filter");
        if (next_block == nullptr) {
            JIT_RETURN_CONST(MOT::RC_LOCAL_ROW_NOT_FOUND);
        } else {
            ctx->_builder->CreateBr(next_block);
        }
        JIT_ELSE()
        IssueDebugLog("Row passed filter");
        JIT_IF_END()
    }
    return true;
}

void buildInsertRow(JitTvmCodeGenContext* ctx, Instruction* row)
{
    IssueDebugLog("Inserting row");
    Instruction* insert_row_res = AddInsertRow(ctx, row);

    JIT_IF_BEGIN(check_row_inserted)
    JIT_IF_EVAL_CMP(insert_row_res, JIT_CONST(MOT::RC_OK), JIT_ICMP_NE)
    IssueDebugLog("Row not inserted");
    JIT_RETURN(insert_row_res);
    JIT_IF_END()

    IssueDebugLog("Row inserted");
}

void buildDeleteRow(JitTvmCodeGenContext* ctx)
{
    IssueDebugLog("Deleting row");
    Instruction* delete_row_res = AddDeleteRow(ctx);

    JIT_IF_BEGIN(check_delete_row)
    JIT_IF_EVAL_CMP(delete_row_res, JIT_CONST(MOT::RC_OK), JIT_ICMP_NE)
    IssueDebugLog("Row not deleted");
    JIT_RETURN(delete_row_res);
    JIT_IF_END()

    IssueDebugLog("Row deleted");
}

static Instruction* buildSearchIterator(JitTvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitRangeBoundMode range_bound_mode, JitRangeScanType range_scan_type, int subQueryIndex = -1)
{
    // search the row
    IssueDebugLog("Searching range start");
    Instruction* itr = AddSearchIterator(ctx, index_scan_direction, range_bound_mode, range_scan_type, subQueryIndex);

    JIT_IF_BEGIN(check_itr_found)
    JIT_IF_EVAL_NOT(itr)
    IssueDebugLog("Range start not found");
    JIT_RETURN_CONST(MOT::RC_LOCAL_ROW_NOT_FOUND);
    JIT_IF_END()

    IssueDebugLog("Range start found");
    return itr;
}

/** @brief Adds code to search for an iterator. */
static Instruction* buildBeginIterator(
    JitTvmCodeGenContext* ctx, JitRangeScanType rangeScanType, int subQueryIndex = -1)
{
    // search the row
    IssueDebugLog("Getting begin iterator for full-scan");
    Instruction* itr = AddBeginIterator(ctx, rangeScanType, subQueryIndex);

    JIT_IF_BEGIN(check_itr_found)
    JIT_IF_EVAL_NOT(itr)
    IssueDebugLog("Begin iterator not found");
    JIT_RETURN_CONST(MOT::RC_LOCAL_ROW_NOT_FOUND);
    JIT_IF_END()

    IssueDebugLog("Range start found");
    return itr;
}

Instruction* buildGetRowFromIterator(JitTvmCodeGenContext* ctx, BasicBlock* endLoopBlock, MOT::AccessType access_mode,
    JitIndexScanDirection index_scan_direction, JitTvmRuntimeCursor* cursor, JitRangeScanType range_scan_type,
    int subQueryIndex /* = -1 */)
{
    IssueDebugLog("Retrieving row from iterator");
    Instruction* row =
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

static Expression* ProcessConstExpr(
    JitTvmCodeGenContext* ctx, const Const* const_value, int& result_type, int arg_pos, int depth, int* max_arg)
{
    Expression* result = nullptr;
    MOT_LOG_DEBUG("%*s --> Processing CONST expression", depth, "");
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (IsTypeSupported(const_value->consttype)) {
        result_type = const_value->consttype;
        if (IsPrimitiveType(result_type)) {
            result =
                new (std::nothrow) ConstExpression(const_value->constvalue, arg_pos, (int)(const_value->constisnull));
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
        MOT_LOG_TRACE("Failed to process const expression: type %d unsupported", (int)const_value->consttype);
    }

    MOT_LOG_DEBUG("%*s <-- Processing CONST expression result: %p", depth, "", result);
    return result;
}

static Expression* ProcessParamExpr(
    JitTvmCodeGenContext* ctx, const Param* param, int& result_type, int arg_pos, int depth, int* max_arg)
{
    Expression* result = nullptr;
    MOT_LOG_DEBUG("%*s --> Processing PARAM expression", depth, "");
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (IsTypeSupported(param->paramtype)) {
        result_type = param->paramtype;
        result = AddGetDatumParam(ctx, param->paramid - 1, arg_pos);
        if (max_arg && (arg_pos > *max_arg)) {
            *max_arg = arg_pos;
        }
    } else {
        MOT_LOG_TRACE("Failed to process param expression: type %d unsupported", (int)param->paramtype);
    }

    MOT_LOG_DEBUG("%*s <-- Processing PARAM expression result: %p", depth, "", result);
    return result;
}

static Expression* ProcessRelabelExpr(
    JitTvmCodeGenContext* ctx, RelabelType* relabel_type, int& result_type, int arg_pos, int depth, int* max_arg)
{
    MOT_LOG_DEBUG("Processing RELABEL expression");
    Param* param = (Param*)relabel_type->arg;
    return ProcessParamExpr(ctx, param, result_type, arg_pos, depth, max_arg);
}

static Expression* ProcessVarExpr(
    JitTvmCodeGenContext* ctx, const Var* var, int& result_type, int arg_pos, int depth, int* max_arg)
{
    Expression* result = nullptr;
    MOT_LOG_DEBUG("%*s --> Processing VAR expression", depth, "");
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (IsTypeSupported(var->vartype)) {
        result_type = var->vartype;
        int table_colid = var->varattno;
        result = AddReadDatumColumn(ctx, ctx->_table_info.m_table, nullptr, table_colid, arg_pos);
        if (max_arg && (arg_pos > *max_arg)) {
            *max_arg = arg_pos;
        }
    } else {
        MOT_LOG_TRACE("Failed to process var expression: type %d unsupported", (int)var->vartype);
    }

    MOT_LOG_DEBUG("%*s <-- Processing VAR expression result: %p", depth, "", result);
    return result;
}

#define APPLY_UNARY_OPERATOR(funcid, name)                   \
    case funcid:                                             \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);     \
        result = new (std::nothrow) name##Operator(args[0]); \
        break;

#define APPLY_BINARY_OPERATOR(funcid, name)                           \
    case funcid:                                                      \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);              \
        result = new (std::nothrow) name##Operator(args[0], args[1]); \
        break;

#define APPLY_TERNARY_OPERATOR(funcid, name)                                   \
    case funcid:                                                               \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                       \
        result = new (std::nothrow) name##Operator(args[0], args[1], args[2]); \
        break;

#define APPLY_UNARY_CAST_OPERATOR(funcid, name)                       \
    case funcid:                                                      \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);              \
        result = new (std::nothrow) name##Operator(args[0], arg_pos); \
        break;

#define APPLY_BINARY_CAST_OPERATOR(funcid, name)                               \
    case funcid:                                                               \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                       \
        result = new (std::nothrow) name##Operator(args[0], args[1], arg_pos); \
        break;

#define APPLY_TERNARY_CAST_OPERATOR(funcid, name)                                       \
    case funcid:                                                                        \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                                \
        result = new (std::nothrow) name##Operator(args[0], args[1], args[2], arg_pos); \
        break;

static Expression* ProcessOpExpr(
    JitTvmCodeGenContext* ctx, const OpExpr* op_expr, int& result_type, int arg_pos, int depth, int* max_arg)
{
    Expression* result = nullptr;
    const int op_args_num = 3;
    MOT_LOG_DEBUG("%*s --> Processing OP %u expression", depth, "", op_expr->opfuncid);
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (list_length(op_expr->args) > op_args_num) {
        MOT_LOG_TRACE("Unsupported operator %d: too many arguments", op_expr->opno);
        return nullptr;
    }

    Expression* args[op_args_num] = {nullptr, nullptr, nullptr};
    int arg_num = 0;
    int dummy = 0;

    ListCell* lc = nullptr;
    foreach (lc, op_expr->args) {
        Expr* sub_expr = (Expr*)lfirst(lc);
        args[arg_num] = ProcessExpr(ctx, sub_expr, dummy, arg_pos + arg_num, depth + 1, max_arg);
        if (args[arg_num] == nullptr) {
            MOT_LOG_TRACE("Failed to process operator sub-expression %d", arg_num);
            return nullptr;
        }
        if (++arg_num == op_args_num) {
            break;
        }
    }

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

static Expression* ProcessFuncExpr(
    JitTvmCodeGenContext* ctx, const FuncExpr* func_expr, int& result_type, int arg_pos, int depth, int* max_arg)
{
    Expression* result = nullptr;
    const int func_args_num = 3;
    MOT_LOG_DEBUG("%*s --> Processing FUNC %d expression", depth, "", (int)func_expr->funcid);
    if (depth > MOT_JIT_MAX_EXPR_DEPTH) {
        MOT_LOG_TRACE("Cannot process expression: Expression exceeds depth limit %d", (int)MOT_JIT_MAX_EXPR_DEPTH);
        return nullptr;
    }

    if (list_length(func_expr->args) > func_args_num) {
        MOT_LOG_TRACE("Unsupported function %d: too many arguments", func_expr->funcid);
        return nullptr;
    }

    Expression* args[func_args_num] = {nullptr, nullptr, nullptr};
    int arg_num = 0;
    int dummy = 0;

    ListCell* lc = nullptr;
    foreach (lc, func_expr->args) {
        Expr* sub_expr = (Expr*)lfirst(lc);
        args[arg_num] = ProcessExpr(ctx, sub_expr, dummy, arg_pos + arg_num, depth + 1, max_arg);
        if (args[arg_num] == nullptr) {
            MOT_LOG_TRACE("Failed to process function sub-expression %d", arg_num);
            return nullptr;
        }
        if (++arg_num == func_args_num) {
            break;
        }
    }

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

#undef APPLY_UNARY_OPERATOR
#undef APPLY_BINARY_OPERATOR
#undef APPLY_TERNARY_OPERATOR
#undef APPLY_UNARY_CAST_OPERATOR
#undef APPLY_BINARY_CAST_OPERATOR
#undef APPLY_TERNARY_CAST_OPERATOR

static Expression* ProcessExpr(
    JitTvmCodeGenContext* ctx, Expr* expr, int& result_type, int arg_pos, int depth, int* max_arg)
{
    Expression* result = nullptr;
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

static Expression* ProcessConstExpr(JitTvmCodeGenContext* ctx, const JitConstExpr* expr, int* max_arg)
{
    Expression* result = nullptr;
    AddSetExprArgIsNull(ctx, expr->_arg_pos, expr->_is_null);  // mark expression null status
    if (IsPrimitiveType(expr->_const_type)) {
        result = new (std::nothrow) ConstExpression(expr->_value, expr->_arg_pos, (int)(expr->_is_null));
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

static Expression* ProcessParamExpr(JitTvmCodeGenContext* ctx, const JitParamExpr* expr, int* max_arg)
{
    Expression* result = AddGetDatumParam(ctx, expr->_param_id, expr->_arg_pos);
    if (max_arg && (expr->_arg_pos > *max_arg)) {
        *max_arg = expr->_arg_pos;
    }
    return result;
}

static Expression* ProcessVarExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitVarExpr* expr, int* max_arg)
{
    Expression* result = nullptr;
    if (row == nullptr) {
        MOT_LOG_TRACE("ProcessVarExpr(): Unexpected VAR expression without a row");
    } else {
        result = AddReadDatumColumn(ctx, expr->_table, row, expr->_column_id, expr->_arg_pos);
        if (max_arg && (expr->_arg_pos > *max_arg)) {
            *max_arg = expr->_arg_pos;
        }
    }
    return result;
}

#define APPLY_UNARY_OPERATOR(funcid, name)                   \
    case funcid:                                             \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);     \
        result = new (std::nothrow) name##Operator(args[0]); \
        break;

#define APPLY_BINARY_OPERATOR(funcid, name)                           \
    case funcid:                                                      \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);              \
        result = new (std::nothrow) name##Operator(args[0], args[1]); \
        break;

#define APPLY_TERNARY_OPERATOR(funcid, name)                                   \
    case funcid:                                                               \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                       \
        result = new (std::nothrow) name##Operator(args[0], args[1], args[2]); \
        break;

#define APPLY_UNARY_CAST_OPERATOR(funcid, name)                       \
    case funcid:                                                      \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);              \
        result = new (std::nothrow) name##Operator(args[0], arg_pos); \
        break;

#define APPLY_BINARY_CAST_OPERATOR(funcid, name)                               \
    case funcid:                                                               \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                       \
        result = new (std::nothrow) name##Operator(args[0], args[1], arg_pos); \
        break;

#define APPLY_TERNARY_CAST_OPERATOR(funcid, name)                                       \
    case funcid:                                                                        \
        MOT_LOG_DEBUG("Adding call to builtin: " #name);                                \
        result = new (std::nothrow) name##Operator(args[0], args[1], args[2], arg_pos); \
        break;

static Expression* ProcessOpExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitOpExpr* expr, int* max_arg)
{
    Expression* result = nullptr;

    Expression* args[MOT_JIT_MAX_FUNC_EXPR_ARGS] = {nullptr, nullptr, nullptr};

    for (int i = 0; i < expr->_arg_count; ++i) {
        args[i] = ProcessExpr(ctx, row, expr->_args[i], max_arg);
        if (args[i] == nullptr) {
            MOT_LOG_TRACE("Failed to process operator sub-expression %d", i);
            for (int j = 0; j < i; ++j) {
                delete args[j];  // cleanup after error
            }
            return nullptr;
        }
    }

    int arg_pos = expr->_arg_pos;
    switch (expr->_op_func_id) {
        APPLY_OPERATORS()

        default:
            MOT_LOG_TRACE("Unsupported operator function type: %d", expr->_op_func_id);
            break;
    }

    return result;
}

static Expression* ProcessFuncExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitFuncExpr* expr, int* max_arg)
{
    Expression* result = nullptr;

    Expression* args[MOT_JIT_MAX_FUNC_EXPR_ARGS] = {nullptr, nullptr, nullptr};

    for (int i = 0; i < expr->_arg_count; ++i) {
        args[i] = ProcessExpr(ctx, row, expr->_args[i], max_arg);
        if (args[i] == nullptr) {
            MOT_LOG_TRACE("Failed to process function sub-expression %d", i);
            for (int j = 0; j < i; ++j) {
                delete args[j];  // cleanup after error
            }
            return nullptr;
        }
    }

    int arg_pos = expr->_arg_pos;
    switch (expr->_func_id) {
        APPLY_OPERATORS()

        default:
            MOT_LOG_TRACE("Unsupported function type: %d", expr->_func_id);
            break;
    }

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

static Expression* ProcessFilterExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitFilter* filter, int* max_arg)
{
    Expression* result = nullptr;

    Expression* args[MOT_JIT_MAX_FUNC_EXPR_ARGS] = {nullptr, nullptr, nullptr};

    args[0] = ProcessExpr(ctx, row, filter->_lhs_operand, max_arg);
    if (!args[0]) {
        MOT_LOG_TRACE("Failed to process filter LHS expression");
        return nullptr;
    }

    args[1] = ProcessExpr(ctx, row, filter->_rhs_operand, max_arg);
    if (!args[1]) {
        MOT_LOG_TRACE("Failed to process filter RHS expression");
        delete args[0];
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

static Expression* ProcessSubLinkExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitSubLinkExpr* expr, int* max_arg)
{
    return AddSelectSubQueryResult(ctx, expr->_sub_query_index);
}

static Expression* ProcessBoolExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitBoolExpr* expr, int* maxArg)
{
    Expression* result = nullptr;

    Expression* args[MOT_JIT_MAX_BOOL_EXPR_ARGS] = {nullptr, nullptr};
    int argNum = 0;

    for (int i = 0; i < expr->_arg_count; ++i) {
        args[i] = ProcessExpr(ctx, row, expr->_args[i], maxArg);
        if (args[i] == nullptr) {
            MOT_LOG_TRACE("Failed to process boolean sub-expression %d", argNum);
            return nullptr;
        }
    }

    switch (expr->_bool_expr_type) {
        case NOT_EXPR:
            result = new (std::nothrow) NotExpression(args[0]);
            break;

        case AND_EXPR:
            result = new (std::nothrow) AndExpression(args[0], args[1]);
            break;

        case OR_EXPR:
            result = new (std::nothrow) OrExpression(args[0], args[1]);
            break;

        default:
            MOT_LOG_TRACE("Unsupported boolean expression type: %d", (int)expr->_bool_expr_type);
            break;
    }

    return result;
}

static Expression* ProcessExpr(JitTvmCodeGenContext* ctx, Instruction* row, JitExpr* expr, int* max_arg)
{
    Expression* result = nullptr;

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

static bool buildScanExpression(JitTvmCodeGenContext* ctx, JitColumnExpr* expr, int* max_arg,
    JitRangeIteratorType range_itr_type, JitRangeScanType range_scan_type, Instruction* outer_row, int subQueryIndex)
{
    Expression* value_expr = ProcessExpr(ctx, outer_row, expr->_expr, max_arg);
    if (value_expr == nullptr) {
        MOT_LOG_TRACE("buildScanExpression(): Failed to process expression with type %d", (int)expr->_expr->_expr_type);
        return false;
    } else {
        Instruction* value = buildExpression(ctx, value_expr);
        Instruction* column = AddGetColumnAt(ctx,
            expr->_table_column_id,
            range_scan_type,  // no need to translate to zero-based index (first column is null bits)
            subQueryIndex);
        int index_colid = -1;
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            index_colid = ctx->m_innerTable_info.m_columnMap[expr->_table_column_id];
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

bool buildPointScan(JitTvmCodeGenContext* ctx, JitColumnExprArray* expr_array, int* max_arg,
    JitRangeScanType range_scan_type, Instruction* outer_row, int expr_count /* = -1 */, int subQueryIndex /* = -1 */)
{
    if (expr_count == -1) {
        expr_count = expr_array->_count;
    }
    AddInitSearchKey(ctx, range_scan_type, subQueryIndex);
    for (int i = 0; i < expr_count; ++i) {
        JitColumnExpr* expr = &expr_array->_exprs[i];

        // validate the expression refers to the right table (in search expressions array, all expressions refer to the
        // same table)
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            if (expr->_table != ctx->m_innerTable_info.m_table) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Generate TVM JIT Code",
                    "Invalid expression table (expected inner table %s, got %s)",
                    ctx->m_innerTable_info.m_table->GetTableName().c_str(),
                    expr->_table->GetTableName().c_str());
                return false;
            }
        } else if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
            if (expr->_table != ctx->_table_info.m_table) {
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "Generate TVM JIT Code",
                    "Invalid expression table (expected main/outer table %s, got %s)",
                    ctx->_table_info.m_table->GetTableName().c_str(),
                    expr->_table->GetTableName().c_str());
                return false;
            }
        } else if (range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
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
        if (!buildScanExpression(
                ctx, expr, max_arg, JIT_RANGE_ITERATOR_START, range_scan_type, outer_row, subQueryIndex)) {
            return false;
        }
    }
    return true;
}

bool writeRowColumns(
    JitTvmCodeGenContext* ctx, Instruction* row, JitColumnExprArray* expr_array, int* max_arg, bool is_update)
{
    for (int i = 0; i < expr_array->_count; ++i) {
        JitColumnExpr* column_expr = &expr_array->_exprs[i];

        Expression* expr = ProcessExpr(ctx, row, column_expr->_expr, max_arg);
        if (expr == nullptr) {
            MOT_LOG_TRACE("ProcessExpr() returned NULL");
            return false;
        }

        // set null bit or copy result data to column
        Instruction* value = buildExpression(ctx, expr);
        buildWriteDatumColumn(ctx, row, column_expr->_table_column_id, value);

        // set bit for incremental redo
        if (is_update) {
            AddSetBit(ctx, column_expr->_table_column_id - 1);
        }
    }

    return true;
}

bool selectRowColumns(JitTvmCodeGenContext* ctx, Instruction* row, JitSelectExprArray* expr_array, int* max_arg,
    JitRangeScanType range_scan_type, int subQueryIndex /* = -1 */)
{
    for (int i = 0; i < expr_array->_count; ++i) {
        JitSelectExpr* expr = &expr_array->_exprs[i];
        // we skip expressions that select from other tables
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            if (expr->_column_expr->_table != ctx->m_innerTable_info.m_table) {
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
        AddSelectColumn(
            ctx, row, expr->_column_expr->_column_id, expr->_tuple_column_id, range_scan_type, subQueryIndex);
    }

    return true;
}

static bool buildClosedRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, Instruction* outerRow, int subQueryIndex)
{
    // a closed range scan starts just like a point scan (with not enough search expressions) and then adds key patterns
    bool result = buildPointScan(ctx, &indexScan->_search_exprs, maxArg, rangeScanType, outerRow, -1, subQueryIndex);
    if (result) {
        AddCopyKey(ctx, rangeScanType, subQueryIndex);

        // now fill each key with the right pattern for the missing pkey columns in the where clause
        bool ascending = (indexScan->_sort_order == JIT_QUERY_SORT_ASCENDING);

        int* index_column_offsets = nullptr;
        const uint16_t* key_length = nullptr;
        int index_column_count = 0;
        if (rangeScanType == JIT_RANGE_SCAN_INNER) {
            index_column_offsets = ctx->m_innerTable_info.m_indexColumnOffsets;
            key_length = ctx->m_innerTable_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->m_innerTable_info.m_index->GetNumFields();
        } else if (rangeScanType == JIT_RANGE_SCAN_MAIN) {
            index_column_offsets = ctx->_table_info.m_indexColumnOffsets;
            key_length = ctx->_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_table_info.m_index->GetNumFields();
        } else if (rangeScanType == JIT_RANGE_SCAN_SUB_QUERY) {
            index_column_offsets = ctx->m_subQueryTableInfo[subQueryIndex].m_indexColumnOffsets;
            key_length = ctx->m_subQueryTableInfo[subQueryIndex].m_index->GetLengthKeyFields();
            index_column_count = ctx->m_subQueryTableInfo[subQueryIndex].m_index->GetNumFields();
        }

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

static void BuildAscendingSemiOpenRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound,
    Instruction* outerRow, int subQueryIndex, int offset, int size, JitColumnExpr* lastExpr)
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

static void BuildDescendingSemiOpenRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound,
    Instruction* outerRow, int subQueryIndex, int offset, int size, JitColumnExpr* lastExpr)
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

static bool buildSemiOpenRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitRangeBoundMode* begin_range_bound, JitRangeBoundMode* end_range_bound,
    Instruction* outer_row, int subQueryIndex)
{
    // an open range scan starts just like a point scan (with not enough search expressions) and then adds key patterns
    // we do not use the last search expression
    bool result = buildPointScan(ctx,
        &index_scan->_search_exprs,
        max_arg,
        range_scan_type,
        outer_row,
        index_scan->_search_exprs._count - 1,
        subQueryIndex);
    if (result) {
        AddCopyKey(ctx, range_scan_type, subQueryIndex);

        // now fill each key with the right pattern for the missing pkey columns in the where clause
        bool ascending = (index_scan->_sort_order == JIT_QUERY_SORT_ASCENDING);

        int* index_column_offsets = nullptr;
        const uint16_t* key_length = nullptr;
        int index_column_count = 0;
        if (range_scan_type == JIT_RANGE_SCAN_INNER) {
            index_column_offsets = ctx->m_innerTable_info.m_indexColumnOffsets;
            key_length = ctx->m_innerTable_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->m_innerTable_info.m_index->GetNumFields();
        } else if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
            index_column_offsets = ctx->_table_info.m_indexColumnOffsets;
            key_length = ctx->_table_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->_table_info.m_index->GetNumFields();
        } else if (range_scan_type == JIT_RANGE_SCAN_SUB_QUERY) {
            index_column_offsets = ctx->m_subQueryTableInfo[subQueryIndex].m_indexColumnOffsets;
            key_length = ctx->m_subQueryTableInfo[subQueryIndex].m_index->GetLengthKeyFields();
            index_column_count = ctx->m_subQueryTableInfo[subQueryIndex].m_index->GetNumFields();
        }

        // prepare offset and size for last column in search
        int last_dim_column = index_scan->_column_count - 1;
        int offset = index_column_offsets[last_dim_column];
        int size = key_length[last_dim_column];

        // now we fill the last dimension (override extra work of point scan above)
        int last_expr_index = index_scan->_search_exprs._count - 1;
        JitColumnExpr* last_expr = &index_scan->_search_exprs._exprs[last_expr_index];
        if (ascending) {
            BuildAscendingSemiOpenRangeScan(ctx,
                index_scan,
                max_arg,
                range_scan_type,
                begin_range_bound,
                end_range_bound,
                outer_row,
                subQueryIndex,
                offset,
                size,
                last_expr);
        } else {
            BuildDescendingSemiOpenRangeScan(ctx,
                index_scan,
                max_arg,
                range_scan_type,
                begin_range_bound,
                end_range_bound,
                outer_row,
                subQueryIndex,
                offset,
                size,
                last_expr);
        }

        // now fill the rest as usual
        int first_zero_column = index_scan->_column_count;
        for (int i = first_zero_column; i < index_column_count; ++i) {
            int col_offset = index_column_offsets[i];
            int key_len = key_length[i];
            MOT_LOG_DEBUG("Filling begin/end iterator pattern for missing pkey fields at offset %d, size %d",
                col_offset,
                key_len);
            AddFillKeyPattern(ctx,
                ascending ? 0x00 : 0xFF,
                col_offset,
                key_len,
                JIT_RANGE_ITERATOR_START,
                range_scan_type,
                subQueryIndex);
            AddFillKeyPattern(ctx,
                ascending ? 0xFF : 0x00,
                col_offset,
                key_len,
                JIT_RANGE_ITERATOR_END,
                range_scan_type,
                subQueryIndex);
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

static void BuildAscendingOpenRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound,
    Instruction* outerRow, int subQueryIndex, JitWhereOperatorClass beforeLastDimOp, JitWhereOperatorClass lastDimOp,
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

static void BuildDescendingOpenRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound,
    Instruction* outerRow, int subQueryIndex, JitWhereOperatorClass beforeLastDimOp, JitWhereOperatorClass lastDimOp,
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

static bool buildOpenRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, JitRangeBoundMode* begin_range_bound, JitRangeBoundMode* end_range_bound,
    Instruction* outer_row, int subQueryIndex)
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
            index_column_offsets = ctx->m_innerTable_info.m_indexColumnOffsets;
            key_length = ctx->m_innerTable_info.m_index->GetLengthKeyFields();
            index_column_count = ctx->m_innerTable_info.m_index->GetNumFields();
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

static bool buildRangeScan(JitTvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound,
    Instruction* outerRow, int subQueryIndex = -1)
{
    bool result = false;

    // if this is a point scan we generate two identical keys for the iterators
    if (indexScan->_scan_type == JIT_INDEX_SCAN_POINT) {
        result = buildPointScan(ctx, &indexScan->_search_exprs, maxArg, rangeScanType, outerRow, -1, subQueryIndex);
        if (result) {
            AddCopyKey(ctx, rangeScanType, subQueryIndex);
            *beginRangeBound = JIT_RANGE_BOUND_INCLUDE;
            *endRangeBound = JIT_RANGE_BOUND_INCLUDE;
        }
    } else if (indexScan->_scan_type == JIT_INDEX_SCAN_CLOSED) {
        result = buildClosedRangeScan(ctx, indexScan, maxArg, rangeScanType, outerRow, subQueryIndex);
        if (result) {
            *beginRangeBound = JIT_RANGE_BOUND_INCLUDE;
            *endRangeBound = JIT_RANGE_BOUND_INCLUDE;
        }
    } else if (indexScan->_scan_type == JIT_INDEX_SCAN_SEMI_OPEN) {
        result = buildSemiOpenRangeScan(
            ctx, indexScan, maxArg, rangeScanType, beginRangeBound, endRangeBound, outerRow, subQueryIndex);
    } else if (indexScan->_scan_type == JIT_INDEX_SCAN_OPEN) {
        result = buildOpenRangeScan(
            ctx, indexScan, maxArg, rangeScanType, beginRangeBound, endRangeBound, outerRow, subQueryIndex);
    } else if (indexScan->_scan_type == JIT_INDEX_SCAN_FULL) {
        result = true;  // no keys used
        *beginRangeBound = JIT_RANGE_BOUND_INCLUDE;
        *endRangeBound = JIT_RANGE_BOUND_INCLUDE;
    }
    return result;
}

static bool buildPrepareStateScan(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan, int* max_arg,
    JitRangeScanType range_scan_type, Instruction* outer_row)
{
    JitRangeBoundMode begin_range_bound = JIT_RANGE_BOUND_NONE;
    JitRangeBoundMode end_range_bound = JIT_RANGE_BOUND_NONE;

    // emit code to check if state iterators are null
    JIT_IF_BEGIN(state_iterators_exist)
    Instruction* is_begin_itr_null = AddIsStateIteratorNull(ctx, JIT_RANGE_ITERATOR_START, range_scan_type);
    JIT_IF_EVAL(is_begin_itr_null)
    // prepare search keys
    if (!buildRangeScan(ctx, index_scan, max_arg, range_scan_type, &begin_range_bound, &end_range_bound, outer_row)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range select query: unsupported WHERE clause type");
        return false;
    }

    // search begin iterator and save it in execution state
    IssueDebugLog("Building search iterator from search key, and saving in execution state");
    if (index_scan->_scan_type == JIT_INDEX_SCAN_FULL) {
        Instruction* itr = buildBeginIterator(ctx, range_scan_type);
        AddSetStateIterator(ctx, itr, JIT_RANGE_ITERATOR_START, range_scan_type);
    } else {
        Instruction* itr = buildSearchIterator(ctx, index_scan->_scan_direction, begin_range_bound, range_scan_type);
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

static bool buildPrepareStateRow(JitTvmCodeGenContext* ctx, MOT::AccessType access_mode, JitIndexScan* index_scan,
    int* max_arg, JitRangeScanType range_scan_type, BasicBlock* next_block)
{
    MOT_LOG_DEBUG("Generating select code for stateful range select");
    Instruction* row = nullptr;

    // we start a new block so current block must end with terminator
    DEFINE_BLOCK(prepare_state_row_bb, ctx->m_jittedQuery);
    ctx->_builder->CreateBr(prepare_state_row_bb);
    ctx->_builder->SetInsertPoint(prepare_state_row_bb);

    // check if state row is null
    JIT_IF_BEGIN(test_state_row)
    IssueDebugLog("Checking if state row is NULL");
    Instruction* res = AddIsStateRowNull(ctx, range_scan_type);
    JIT_IF_EVAL(res)
    IssueDebugLog("State row is NULL, fetching from state iterators");

    // check if state scan ended
    JIT_IF_BEGIN(test_scan)
    IssueDebugLog("Checking if state scan ended");
    BasicBlock* isStateScanEndBlock = JIT_CURRENT_BLOCK();  // remember current block if filter fails
    Instruction* res_scan_end = AddIsStateScanEnd(ctx, index_scan->_scan_direction, range_scan_type);
    JIT_IF_EVAL(res_scan_end)
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
    // row found, check for additional filters, if not passing filter then go back to execute getRowFromStateIterator()
    if (!buildFilterRow(ctx, row, &index_scan->_filters, max_arg, isStateScanEndBlock)) {
        MOT_LOG_TRACE("Failed to generate jitted code for query: failed to build filter expressions for row");
        return false;
    }
    // row passed all filters, so save it in state outer row
    AddSetStateRow(ctx, row, range_scan_type);
    if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
        AddSetStateScanEndFlag(ctx, 0, JIT_RANGE_SCAN_INNER);  // reset inner scan flag
    }
    JIT_IF_END()
    JIT_IF_END()
    JIT_IF_END()

    // cleanup state iterators if needed and return/jump to next block
    JIT_IF_BEGIN(state_scan_ended)
    IssueDebugLog("Checking if state scan ended flag was raised");
    Instruction* state_scan_end_flag = AddGetStateScanEndFlag(ctx, range_scan_type);
    JIT_IF_EVAL(state_scan_end_flag)
    IssueDebugLog(" State scan ended flag was raised, cleaning up iterators and reporting to user");
    AddDestroyStateIterators(ctx, range_scan_type);  // cleanup
    if ((range_scan_type == JIT_RANGE_SCAN_MAIN) || (next_block == nullptr)) {
        // either a main scan ended (simple range or outer loop of join),
        // or an inner range ended in an outer point join (so next block is null)
        // in either case let caller know scan ended and return appripriate value
        AddSetScanEnded(ctx, 1);  // no outer row, we are definitely done
        JIT_RETURN_CONST(MOT::RC_LOCAL_ROW_NOT_FOUND);
    } else {
        AddResetStateRow(ctx, JIT_RANGE_SCAN_MAIN);  // make sure a new row is fetched in outer loop
        ctx->_builder->CreateBr(next_block);         // jump back to outer loop
    }
    JIT_IF_END()

    return true;
}

Instruction* buildPrepareStateScanRow(JitTvmCodeGenContext* ctx, JitIndexScan* index_scan,
    JitRangeScanType range_scan_type, MOT::AccessType access_mode, int* max_arg, Instruction* outer_row,
    BasicBlock* next_block, BasicBlock** loop_block)
{
    // prepare stateful scan if not done so already
    if (!buildPrepareStateScan(ctx, index_scan, max_arg, range_scan_type, outer_row)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range JOIN query: unsupported %s WHERE clause type",
            (range_scan_type == JIT_RANGE_SCAN_MAIN) ? "outer" : "inner");
        return nullptr;
    }

    // mark position for later jump
    if (loop_block != nullptr) {
        *loop_block = ctx->_builder->CreateBlock("fetch_outer_row_bb");
        ctx->_builder->CreateBr(*loop_block);        // end current block
        ctx->_builder->SetInsertPoint(*loop_block);  // start new block
    }

    // fetch row for read
    if (!buildPrepareStateRow(ctx, access_mode, index_scan, max_arg, range_scan_type, next_block)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range JOIN query: failed to build search outer row block");
        return nullptr;
    }
    Instruction* row = AddGetStateRow(ctx, range_scan_type);
    return row;
}

JitTvmRuntimeCursor buildRangeCursor(JitTvmCodeGenContext* ctx, JitIndexScan* indexScan, int* maxArg,
    JitRangeScanType rangeScanType, JitIndexScanDirection indexScanDirection, Instruction* outerRow,
    int subQueryIndex /* = -1 */)
{
    JitTvmRuntimeCursor result = {nullptr, nullptr};
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

static bool prepareAggregateAvg(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate)
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

static bool prepareAggregateSum(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate)
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

static bool prepareAggregateMaxMin(JitTvmCodeGenContext* ctx, JitAggregate* aggregate)
{
    AddResetAggMaxMinNull(ctx);
    return true;
}

static bool prepareAggregateCount(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate)
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

static bool prepareDistinctSet(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate)
{
    // we need a hash-set according to the aggregated type (preferably but not necessarily linear-probing hash)
    // we use an opaque datum type, with a tailor-made hash-function and equals function
    AddPrepareDistinctSet(ctx, aggregate->_element_type);
    return true;
}

bool prepareAggregate(JitTvmCodeGenContext* ctx, JitAggregate* aggregate)
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

static Expression* buildAggregateAvg(
    JitTvmCodeGenContext* ctx, const JitAggregate* aggregate, Expression* current_aggregate, Expression* var_expr)
{
    Expression* aggregate_expr = nullptr;
    switch (aggregate->_func_id) {
        case INT8AVGFUNCOID:
            // the current_aggregate is a 2 numeric array, and the var expression should evaluate to int8
            aggregate_expr = new (std::nothrow) int8_avg_accumOperator(current_aggregate, var_expr);
            break;

        case INT4AVGFUNCOID:
            // the current_aggregate is a 2 int8 array, and the var expression should evaluate to int4
            // we can save a palloc by ensuring "AggCheckCallContext(fcinfo, NULL)" return true
            aggregate_expr = new (std::nothrow) int4_avg_accumOperator(current_aggregate, var_expr);
            break;

        case INT2AVGFUNCOID:
            // the current_aggregate is a 2 int8 array, and the var expression should evaluate to int2
            // we can save a palloc by ensuring "AggCheckCallContext(fcinfo, NULL)" return true
            aggregate_expr = new (std::nothrow) int2_avg_accumOperator(current_aggregate, var_expr);
            break;

        case 5537:
            // the current_aggregate is a 2 int8 array, and the var expression should evaluate to int1
            // we can save a palloc by ensuring "AggCheckCallContext(fcinfo, NULL)" return true
            aggregate_expr = new (std::nothrow) int1_avg_accumOperator(current_aggregate, var_expr);
            break;

        case 2104:  // float4
            // the current_aggregate is a 3 float8 array, and the var expression should evaluate to float4
            // the function computes 3 values: count, sum, square sum
            // we can save a palloc by ensuring "AggCheckCallContext(fcinfo, NULL)" return true
            aggregate_expr = new (std::nothrow) float4_accumOperator(current_aggregate, var_expr);
            break;

        case 2105:  // float8
            // the current_aggregate is a 3 float8 array, and the var expression should evaluate to float8
            // the function computes 3 values: count, sum, square sum
            // we can save a palloc by ensuring "AggCheckCallContext(fcinfo, NULL)" return true
            aggregate_expr = new (std::nothrow) float8_accumOperator(current_aggregate, var_expr);
            break;

        case NUMERICAVGFUNCOID:
            // the current_aggregate is a 2 numeric array, and the var expression should evaluate to numeric
            aggregate_expr = new (std::nothrow) numeric_avg_accumOperator(current_aggregate, var_expr);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate AVG() operator function type: %d", aggregate->_func_id);
            break;
    }

    return aggregate_expr;
}

static Expression* buildAggregateSum(
    JitTvmCodeGenContext* ctx, const JitAggregate* aggregate, Expression* current_aggregate, Expression* var_expr)
{
    Expression* aggregate_expr = nullptr;
    switch (aggregate->_func_id) {
        case INT8SUMFUNCOID:
            // current sum in numeric, and next value is int8, both can be null
            aggregate_expr = new (std::nothrow) int8_sumOperator(current_aggregate, var_expr);
            break;

        case INT4SUMFUNCOID:
            // current aggregate is a int8, and value is int4, both can be null
            aggregate_expr = new (std::nothrow) int4_sumOperator(current_aggregate, var_expr);
            break;

        case INT2SUMFUNCOID:
            // current aggregate is a int8, and value is int3, both can be null
            aggregate_expr = new (std::nothrow) int2_sumOperator(current_aggregate, var_expr);
            break;

        case 2110:  // float4
            // current aggregate is a float4, and value is float4, both can **NOT** be null
            aggregate_expr = new (std::nothrow) float4plOperator(current_aggregate, var_expr);
            break;

        case 2111:  // float8
            // current aggregate is a float4, and value is float4, both can **NOT** be null
            aggregate_expr = new (std::nothrow) float8plOperator(current_aggregate, var_expr);
            break;

        case NUMERICSUMFUNCOID:
            // current aggregate is a numeric, and value is numeric, both can **NOT** be null
            aggregate_expr = new (std::nothrow) numeric_addOperator(current_aggregate, var_expr);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate SUM() operator function type: %d", aggregate->_func_id);
            break;
    }

    return aggregate_expr;
}

static Expression* buildAggregateMax(
    JitTvmCodeGenContext* ctx, const JitAggregate* aggregate, Expression* current_aggregate, Expression* var_expr)
{
    Expression* aggregate_expr = nullptr;
    switch (aggregate->_func_id) {
        case INT8LARGERFUNCOID:
            // current aggregate is a int8, and value is int8, both can **NOT** be null
            aggregate_expr = new (std::nothrow) int8largerOperator(current_aggregate, var_expr);
            break;

        case INT4LARGERFUNCOID:
            // current aggregate is a int4, and value is int4, both can **NOT** be null
            aggregate_expr = new (std::nothrow) int4largerOperator(current_aggregate, var_expr);
            break;

        case INT2LARGERFUNCOID:
            // current aggregate is a int2, and value is int2, both can **NOT** be null
            aggregate_expr = new (std::nothrow) int2largerOperator(current_aggregate, var_expr);
            break;

        case 5538:
            // current aggregate is a int1, and value is int1, both can **NOT** be null
            aggregate_expr = new (std::nothrow) int1largerOperator(current_aggregate, var_expr);
            break;

        case 2119:  // float4larger
            // current aggregate is a float4, and value is float4, both can **NOT** be null
            aggregate_expr = new (std::nothrow) float4largerOperator(current_aggregate, var_expr);
            break;

        case 2120:  // float8larger
            // current aggregate is a float8, and value is float8, both can **NOT** be null
            aggregate_expr = new (std::nothrow) float8largerOperator(current_aggregate, var_expr);
            break;

        case NUMERICLARGERFUNCOID:
            // current aggregate is a numeric, and value is numeric, both can **NOT** be null
            aggregate_expr = new (std::nothrow) numeric_largerOperator(current_aggregate, var_expr);
            break;

        case 2126:
            // current aggregate is a timestamp, and value is timestamp, both can **NOT** be null
            aggregate_expr = new (std::nothrow) timestamp_largerOperator(current_aggregate, var_expr);
            break;

        case 2122:
            // current aggregate is a date, and value is date, both can **NOT** be null
            aggregate_expr = new (std::nothrow) date_largerOperator(current_aggregate, var_expr);
            break;

        case 2244:
            // current aggregate is a bpchar, and value is bpchar, both can **NOT** be null
            aggregate_expr = new (std::nothrow) bpchar_largerOperator(current_aggregate, var_expr);
            break;

        case 2129:
            // current aggregate is a text, and value is text, both can **NOT** be null
            aggregate_expr = new (std::nothrow) text_largerOperator(current_aggregate, var_expr);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate MAX() operator function type: %d", aggregate->_func_id);
            break;
    }

    return aggregate_expr;
}

static Expression* buildAggregateMin(
    JitTvmCodeGenContext* ctx, const JitAggregate* aggregate, Expression* current_aggregate, Expression* var_expr)
{
    Expression* aggregate_expr = nullptr;
    switch (aggregate->_func_id) {
        case INT8SMALLERFUNCOID:
            // current sum is a int8, and value is int8, both can **NOT** be null
            aggregate_expr = new (std::nothrow) int8smallerOperator(current_aggregate, var_expr);
            break;

        case INT4SMALLERFUNCOID:
            // current aggregate is a int4, and value is int4, both can **NOT** be null
            aggregate_expr = new (std::nothrow) int4smallerOperator(current_aggregate, var_expr);
            break;

        case INT2SMALLERFUNCOID:
            // current aggregate is a int2, and value is int2, both can **NOT** be null
            aggregate_expr = new (std::nothrow) int2smallerOperator(current_aggregate, var_expr);
            break;

        case 2135:  // float4smaller
            // current aggregate is a float4, and value is float4, both can **NOT** be null
            aggregate_expr = new (std::nothrow) float4smallerOperator(current_aggregate, var_expr);
            break;

        case 2120:  // float8smaller
            // current aggregate is a float8, and value is float8, both can **NOT** be null
            aggregate_expr = new (std::nothrow) float8smallerOperator(current_aggregate, var_expr);
            break;

        case NUMERICSMALLERFUNCOID:
            // current aggregate is a numeric, and value is numeric, both can **NOT** be null
            aggregate_expr = new (std::nothrow) numeric_smallerOperator(current_aggregate, var_expr);
            break;

        case 2142:
            // current aggregate is a timestamp, and value is timestamp, both can **NOT** be null
            aggregate_expr = new (std::nothrow) timestamp_smallerOperator(current_aggregate, var_expr);
            break;

        case 2138:
            // current aggregate is a date, and value is date, both can **NOT** be null
            aggregate_expr = new (std::nothrow) date_smallerOperator(current_aggregate, var_expr);
            break;

        case 2245:
            // current aggregate is a bpchar, and value is bpchar, both can **NOT** be null
            aggregate_expr = new (std::nothrow) bpchar_smallerOperator(current_aggregate, var_expr);
            break;

        case 2145:
            // current aggregate is a text, and value is text, both can **NOT** be null
            aggregate_expr = new (std::nothrow) text_smallerOperator(current_aggregate, var_expr);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate MIN() operator function type: %d", aggregate->_func_id);
            break;
    }

    return aggregate_expr;
}

static Expression* buildAggregateCount(
    JitTvmCodeGenContext* ctx, const JitAggregate* aggregate, Expression* count_aggregate)
{
    Expression* aggregate_expr = nullptr;
    switch (aggregate->_func_id) {
        case 2147:  // int8inc_any
            // current aggregate is int8, and can **NOT** be null
            aggregate_expr = new (std::nothrow) int8incOperator(count_aggregate);
            break;

        case 2803:  // int8inc
            // current aggregate is int8, and can **NOT** be null
            aggregate_expr = new (std::nothrow) int8incOperator(count_aggregate);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate COUNT() operator function type: %d", aggregate->_func_id);
            break;
    }

    return aggregate_expr;
}

static bool buildAggregateMaxMin(JitTvmCodeGenContext* ctx, JitAggregate* aggregate, Expression* var_expr)
{
    bool result = false;
    Expression* aggregateExpr = nullptr;

    // we first check if the min/max value is null and if so just store the column value
    JIT_IF_BEGIN(test_max_min_value_null)
    Instruction* res = AddGetAggMaxMinIsNull(ctx);
    JIT_IF_EVAL(res)
    AddSetAggValue(ctx, var_expr);
    AddSetAggMaxMinNotNull(ctx);
    JIT_ELSE()
    // get the aggregated value and call the operator
    Expression* current_aggregate = AddGetAggValue(ctx);
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

static bool buildAggregateTuple(JitTvmCodeGenContext* ctx, JitAggregate* aggregate, Expression* var_expr)
{
    bool result = false;

    if ((aggregate->_aggreaget_op == JIT_AGGREGATE_MAX) || (aggregate->_aggreaget_op == JIT_AGGREGATE_MIN)) {
        result = buildAggregateMaxMin(ctx, aggregate, var_expr);
    } else {
        // get the aggregated value
        Expression* current_aggregate = nullptr;
        if (aggregate->_aggreaget_op == JIT_AGGREGATE_AVG) {
            current_aggregate = AddLoadAvgArray(ctx);
        } else {
            current_aggregate = AddGetAggValue(ctx);
        }

        // the operators below take care of null inputs
        Expression* aggregate_expr = nullptr;
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
            // cleanup
            delete current_aggregate;
            delete var_expr;
            MOT_LOG_TRACE("Failed to generate aggregate AVG/SUM/COUNT code");
        }
    }

    return result;
}

bool buildAggregateRow(JitTvmCodeGenContext* ctx, JitAggregate* aggregate, Instruction* row, BasicBlock* next_block)
{
    bool result = false;

    // extract the aggregated column
    Expression* expr = AddReadDatumColumn(ctx, aggregate->_table, row, aggregate->_table_column_id, 0);

    // we first check if we have DISTINCT modifier
    if (aggregate->_distinct) {
        // check row is distinct in state set (managed on the current jit context)
        // emit code to convert column to primitive data type, add value to distinct set and verify
        // if value is not distinct then do not use this row in aggregation
        JIT_IF_BEGIN(test_distinct_value)
        Instruction* res = AddInsertDistinctItem(ctx, aggregate->_element_type, expr);
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

void buildAggregateResult(JitTvmCodeGenContext* ctx, const JitAggregate* aggregate)
{
    // in case of average we compute it when aggregate loop is done
    if (aggregate->_aggreaget_op == JIT_AGGREGATE_AVG) {
        Instruction* avg_value = AddComputeAvgFromArray(
            ctx, aggregate->_avg_element_type);  // we infer this during agg op analysis, but don't save it...
        AddWriteTupleDatum(ctx, 0, avg_value);   // we alway aggregate to slot tuple 0
    } else {
        Expression* count_expr = AddGetAggValue(ctx);
        Instruction* count_value = buildExpression(ctx, count_expr);
        AddWriteTupleDatum(ctx, 0, count_value);  // we alway aggregate to slot tuple 0
    }

    // we take the opportunity to cleanup as well
    if (aggregate->_distinct) {
        AddDestroyDistinctSet(ctx, aggregate->_element_type);
    }
}

void buildCheckLimit(JitTvmCodeGenContext* ctx, int limit_count)
{
    // if a limit clause exists, then increment limit counter and check if reached limit
    if (limit_count > 0) {
        AddIncrementStateLimitCounter(ctx);
        JIT_IF_BEGIN(limit_count_reached)
        Instruction* current_limit_count = AddGetStateLimitCounter(ctx);
        Instruction* limit_count_inst = JIT_CONST(limit_count);
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
    JitTvmCodeGenContext* ctx, Instruction* outer_row_copy, Instruction* inner_row, JitJoinPlan* plan, int* max_arg)
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
