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
#include "jit_source_map.h"
#include "storage/mot/jit_def.h"

#include "catalog/pg_aggregate.h"

#define JIT_PROFILE_CMD(ctx, begin) InjectProfileData(ctx, GetActiveNamespace(), (ctx)->m_queryString, begin)
#define JIT_PROFILE_BEGIN(ctx) JIT_PROFILE_CMD(ctx, true)
#define JIT_PROFILE_END(ctx) JIT_PROFILE_CMD(ctx, false)

using namespace dorado;

namespace JitExec {
DECLARE_LOGGER(JitLlvmBlocks, JitExec)

/*--------------------------- Helpers to generate compound LLVM code ---------------------------*/
/** @brief Creates a jitted function for code generation. Builds prototype and entry block. */
void CreateJittedFunction(JitLlvmCodeGenContext* ctx, const char* function_name)
{
    llvm::Value* llvmargs[MOT_JIT_QUERY_ARG_COUNT];

    // define the function prototype
    GsCodeGen::FnPrototype fn_prototype(ctx->m_codeGen, function_name, ctx->INT32_T);
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

    ctx->m_jittedFunction = fn_prototype.generatePrototype(ctx->m_builder, &llvmargs[0]);

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

    ctx->rows_processed = ctx->m_builder->CreateAlloca(ctx->INT64_T, nullptr, "rowsProcessed");

    for (uint32_t i = 0; i < ctx->m_subQueryCount; ++i) {
        ctx->m_subQueryData[i].m_slot = AddGetSubQuerySlot(ctx, i);
        ctx->m_subQueryData[i].m_table = AddGetSubQueryTable(ctx, i);
        ctx->m_subQueryData[i].m_index = AddGetSubQueryIndex(ctx, i);
        ctx->m_subQueryData[i].m_searchKey = AddGetSubQuerySearchKey(ctx, i);
        ctx->m_subQueryData[i].m_endIteratorKey = AddGetSubQueryEndIteratorKey(ctx, i);
    }

    IssueDebugLog("Starting execution of jitted function");

    JIT_PROFILE_BEGIN(ctx);
}

/** @brief Builds a code segment for checking if soft memory limit has been reached. */
void buildIsSoftMemoryLimitReached(JitLlvmCodeGenContext* ctx)
{
    JIT_IF_BEGIN(soft_limit_reached);
    llvm::Value* is_limit_reached_res = AddIsSoftMemoryLimitReached(ctx);
    JIT_IF_EVAL(is_limit_reached_res);
    {
        IssueDebugLog("Soft memory limit reached");
        JIT_PROFILE_END(ctx);
        JIT_RETURN(JIT_CONST_INT32(MOT::RC_MEMORY_ALLOCATION_ERROR));
    }
    JIT_ELSE();
    {
        IssueDebugLog("Soft memory limit not reached");
    }
    JIT_IF_END();
}

/** @brief Builds a code segment for writing datum value to a column. */
static void buildWriteDatumColumn(JitLlvmCodeGenContext* ctx, llvm::Value* row, int colid, llvm::Value* datum_value)
{
    llvm::Value* set_null_bit_res = AddSetExprResultNullBit(ctx, row, colid);
    IssueDebugLog("Set null bit");

    if (ctx->_table_info.m_table->GetField(colid)->m_isNotNull) {
        JIT_IF_BEGIN(check_null_violation);
        JIT_IF_EVAL_CMP(set_null_bit_res, JIT_CONST_INT32(MOT::RC_OK), JIT_ICMP_NE);
        {
            IssueDebugLog("Null constraint violated");
            JIT_PROFILE_END(ctx);
            JIT_RETURN(set_null_bit_res);
        }
        JIT_IF_END();
    }

    // now check if the result is not null, and if so write column datum
    llvm::Value* is_expr_null = AddGetExprIsNull(ctx);
    JIT_IF_BEGIN(check_expr_null);
    JIT_IF_EVAL_NOT(is_expr_null);
    {
        IssueDebugLog("Encountered non-null expression result, writing datum column");
        AddWriteDatumColumn(ctx, colid, row, datum_value);
    }
    JIT_IF_END();
}

/** @brief Builds a code segment for writing a row. */
void buildWriteRow(JitLlvmCodeGenContext* ctx, llvm::Value* row, bool isPKey, JitLlvmRuntimeCursor* cursor)
{
    IssueDebugLog("Writing row");
    llvm::Value* write_row_res = AddWriteRow(ctx, row);

    JIT_IF_BEGIN(check_row_written);
    JIT_IF_EVAL_CMP(write_row_res, JIT_CONST_INT32(MOT::RC_OK), JIT_ICMP_NE);
    {
        IssueDebugLog("Row not written");
        // need to emit cleanup code
        if (!isPKey) {
            AddDestroyCursor(ctx, cursor);
        }
        JIT_PROFILE_END(ctx);
        JIT_RETURN(write_row_res);
    }
    JIT_IF_END();
}

/** @brief Adds code to reset the number of rows processed. */
void buildResetRowsProcessed(JitLlvmCodeGenContext* ctx)
{
    ctx->m_builder->CreateStore(JIT_CONST_INT64(0), ctx->rows_processed, true);
}

/** @brief Adds code to increment the number of rows processed. */
void buildIncrementRowsProcessed(JitLlvmCodeGenContext* ctx)
{
    llvm::Value* rowsProcessed = ctx->m_builder->CreateLoad(ctx->rows_processed, true);
    llvm::Value* newValue = ctx->m_builder->CreateAdd(rowsProcessed, JIT_CONST_INT64(1));
    ctx->m_builder->CreateStore(newValue, ctx->rows_processed, true);
}

/** @brief Adds code to create a new row. */
llvm::Value* buildCreateNewRow(JitLlvmCodeGenContext* ctx)
{
    llvm::Value* row = AddCreateNewRow(ctx);

    JIT_IF_BEGIN(check_row_created);
    JIT_IF_EVAL_NOT(row);
    {
        IssueDebugLog("Failed to create row");
        JIT_PROFILE_END(ctx);
        JIT_RETURN(JIT_CONST_INT32(MOT::RC_MEMORY_ALLOCATION_ERROR));
    }
    JIT_IF_END();

    return row;
}

/** @brief Adds code to search for a row by a key. */
llvm::Value* buildSearchRow(JitLlvmCodeGenContext* ctx, MOT::AccessType access_type, JitRangeScanType range_scan_type,
    int subQueryIndex /* = -1 */)
{
    IssueDebugLog("Searching row");
    llvm::Value* row = AddSearchRow(ctx, access_type, range_scan_type, subQueryIndex);

    JIT_IF_BEGIN(check_row_found);
    JIT_IF_EVAL_NOT(row);
    {
        IssueDebugLog("Row not found");
        JIT_PROFILE_END(ctx);
        JIT_RETURN(JIT_CONST_INT32(MOT::RC_LOCAL_ROW_NOT_FOUND));
    }
    JIT_IF_END();

    IssueDebugLog("Row found");
    return row;
}

bool buildFilterRow(JitLlvmCodeGenContext* ctx, llvm::Value* row, llvm::Value* innerRow, JitFilterArray* filters,
    llvm::BasicBlock* next_block)
{
    // 1. for each filter expression we generate the equivalent instructions which should evaluate to true or false
    // 2. We assume that all filters are applied with AND operator between them (imposed during query analysis/plan
    // phase)
    for (int i = 0; i < filters->_filter_count; ++i) {
        llvm::Value* filterExpr = ProcessExpr(ctx, row, innerRow, filters->_scan_filters[i]);
        if (filterExpr == nullptr) {
            MOT_LOG_TRACE("buildFilterRow(): Failed to process filter expression %d", i);
            return false;
        }

        JIT_IF_BEGIN(filter_row);
        IssueDebugLog("Checking if row passes filter");
        JIT_IF_EVAL_NOT(filterExpr);
        {
            IssueDebugLog("Row did not pass filter");
            if (next_block == nullptr) {
                JIT_PROFILE_END(ctx);
                JIT_RETURN(JIT_CONST_INT32(MOT::RC_LOCAL_ROW_NOT_FOUND));
            } else {
                ctx->m_builder->CreateBr(next_block);
            }
        }
        JIT_ELSE();
        {
            IssueDebugLog("Row passed filter");
        }
        JIT_IF_END();
    }
    return true;
}

/** @brief Adds code to insert a new row. */
void buildInsertRow(JitLlvmCodeGenContext* ctx, llvm::Value* row)
{
    IssueDebugLog("Inserting row");
    llvm::Value* insert_row_res = AddInsertRow(ctx, row);

    JIT_IF_BEGIN(check_row_inserted);
    JIT_IF_EVAL_CMP(insert_row_res, JIT_CONST_INT32(MOT::RC_OK), JIT_ICMP_NE);
    {
        IssueDebugLog("Row not inserted");
        JIT_PROFILE_END(ctx);
        JIT_RETURN(insert_row_res);
    }
    JIT_IF_END();

    IssueDebugLog("Row inserted");
}

/** @brief Adds code to delete a row. */
void buildDeleteRow(JitLlvmCodeGenContext* ctx)
{
    IssueDebugLog("Deleting row");
    llvm::Value* delete_row_res = AddDeleteRow(ctx);

    JIT_IF_BEGIN(check_delete_row);
    JIT_IF_EVAL_CMP(delete_row_res, JIT_CONST_INT32(MOT::RC_OK), JIT_ICMP_NE);
    {
        IssueDebugLog("Row not deleted");
        JIT_PROFILE_END(ctx);
        JIT_RETURN(delete_row_res);
    }
    JIT_IF_END();

    IssueDebugLog("Row deleted");
}

/** @brief Adds code to search for an iterator. */
static llvm::Value* buildSearchIterator(JitLlvmCodeGenContext* ctx, JitIndexScanDirection index_scan_direction,
    JitRangeBoundMode range_bound_mode, JitRangeScanType range_scan_type, int subQueryIndex = -1)
{
    // search the row
    IssueDebugLog("Searching range start");
    llvm::Value* itr = AddSearchIterator(ctx, index_scan_direction, range_bound_mode, range_scan_type, subQueryIndex);

    JIT_IF_BEGIN(check_itr_found);
    JIT_IF_EVAL_NOT(itr);
    {
        IssueDebugLog("Range start not found");
        JIT_PROFILE_END(ctx);
        JIT_RETURN(JIT_CONST_INT32(MOT::RC_LOCAL_ROW_NOT_FOUND));
    }
    JIT_IF_END();

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

    JIT_IF_BEGIN(check_itr_found);
    JIT_IF_EVAL_NOT(itr);
    {
        IssueDebugLog("Begin iterator not found");
        JIT_PROFILE_END(ctx);
        JIT_RETURN(JIT_CONST_INT32(MOT::RC_LOCAL_ROW_NOT_FOUND));
    }
    JIT_IF_END();

    IssueDebugLog("Range start found");
    return itr;
}

/** @brief Adds code to get row from iterator. */
void BuildCheckRowExistsInIterator(JitLlvmCodeGenContext* ctx, llvm::BasicBlock* endLoopBlock,
    JitIndexScanDirection indexScanDirection, JitLlvmRuntimeCursor* cursor, JitRangeScanType rangeScanType,
    int subQueryIndex /* = -1 */)
{
    IssueDebugLog("Checking if row exists in iterator");
    llvm::Value* rowExists = AddCheckRowExistsInIterator(ctx, indexScanDirection, cursor, rangeScanType, subQueryIndex);

    JIT_IF_BEGIN(check_itr_row_found);
    JIT_IF_EVAL_NOT(rowExists);
    {
        IssueDebugLog("Row not found in in iterator");
        JIT_GOTO(endLoopBlock);
        // NOTE: we can actually do here a JIT_WHILE_BREAK() if we have an enclosing while loop
    }
    JIT_IF_END();

    IssueDebugLog("Row found in iterator");
}

/** @brief Adds code to get row from iterator. */
llvm::Value* buildGetRowFromIterator(JitLlvmCodeGenContext* ctx, llvm::BasicBlock* startLoopBlock,
    llvm::BasicBlock* endLoopBlock, MOT::AccessType access_mode, JitIndexScanDirection index_scan_direction,
    JitLlvmRuntimeCursor* cursor, JitRangeScanType range_scan_type, int subQueryIndex /* = -1 */)
{
    IssueDebugLog("Retrieving row from iterator");
    llvm::Value* row =
        AddGetRowFromIterator(ctx, access_mode, index_scan_direction, cursor, range_scan_type, subQueryIndex);

    JIT_IF_BEGIN(check_itr_row_found);
    JIT_IF_EVAL_NOT(row);
    {
        IssueDebugLog("Iterator row not found");
        JIT_GOTO(endLoopBlock);
        // NOTE: we can actually do here a JIT_WHILE_BREAK() if we have an enclosing while loop
    }
    JIT_IF_END();

    IssueDebugLog("Iterator row found");
    return row;
}

static llvm::Value* AddInvokePGFunction(JitLlvmCodeGenContext* ctx, Oid funcId, uint32_t argNum, Oid funcCollationId,
    Oid resultCollationId, llvm::Value** args, llvm::Value** argIsNull, Oid* argTypes)
{
    uint32_t argCount = 0;
    bool isStrict = false;
    PGFunction funcPtr = GetPGFunctionInfo(funcId, &argCount, &isStrict);
    if (funcPtr == nullptr) {
        MOT_LOG_TRACE("AddInvokePGFunction(): Cannot find function by id: %u", (unsigned)funcId);
        return nullptr;
    }
    MOT_ASSERT(argCount == argNum);

    llvm::Value* result = nullptr;
    if (argCount == 0) {
        result = AddInvokePGFunction0(ctx, funcPtr, funcCollationId);
    } else if (argCount == 1) {
        result = AddInvokePGFunction1(ctx, funcPtr, funcCollationId, isStrict, args[0], argIsNull[0], argTypes[0]);
    } else if (argCount == 2) {
        result = AddInvokePGFunction2(ctx,
            funcPtr,
            funcCollationId,
            isStrict,
            args[0],
            argIsNull[0],
            argTypes[0],
            args[1],
            argIsNull[1],
            argTypes[1]);
    } else if (argCount == 3) {
        result = AddInvokePGFunction3(ctx,
            funcPtr,
            funcCollationId,
            isStrict,
            args[0],
            argIsNull[0],
            argTypes[2],
            args[1],
            argIsNull[1],
            argTypes[1],
            args[2],
            argIsNull[2],
            argTypes[2]);
    } else {
        result = AddInvokePGFunctionN(ctx, funcPtr, funcCollationId, isStrict, args, argIsNull, argTypes, argCount);
    }
    AddSetExprCollation(ctx, resultCollationId);
    return result;
}

static llvm::Value* ProcessConstExpr(JitLlvmCodeGenContext* ctx, const JitConstExpr* expr)
{
    llvm::Value* result = nullptr;
    if (IsTypeSupported(expr->_const_type)) {
        AddSetExprIsNull(ctx, expr->_is_null);  // mark expression null status
        AddSetExprCollation(ctx, expr->m_collationId);
        if (IsPrimitiveType(expr->_const_type)) {
            result = JIT_CONST_INT64(expr->_value);
        } else {
            int constId = AllocateConstId(ctx, expr->_const_type, expr->_value, expr->_is_null);
            if (constId == -1) {
                MOT_LOG_TRACE("Failed to allocate constant identifier");
            } else {
                result = AddGetConstAt(ctx, constId);
            }
        }
    } else {
        MOT_LOG_TRACE("Failed to process const expression: type %d unsupported", (int)expr->_const_type);
    }

    return result;
}

static llvm::Value* ProcessParamExpr(JitLlvmCodeGenContext* ctx, const JitParamExpr* expr)
{
    llvm::Value* result = AddGetDatumParam(ctx, expr->_param_id);
    AddSetExprCollation(ctx, expr->m_collationId);
    return result;
}

static llvm::Value* ProcessVarExpr(
    JitLlvmCodeGenContext* ctx, llvm::Value* row, llvm::Value* innerRow, const JitVarExpr* expr)
{
    llvm::Value* result = nullptr;

    // this is a bit awkward, but it works
    MOT_LOG_TRACE("ProcessVarExpr(): Expression table %p [%s]", expr->_table, expr->_table->GetTableName().c_str());
    MOT_LOG_TRACE("ProcessVarExpr(): Outer table %p [%s]",
        ctx->_table_info.m_table,
        ctx->_table_info.m_table->GetTableName().c_str());
    MOT_LOG_TRACE("ProcessVarExpr(): row=%p, inner-row=%p", row, innerRow);
    llvm::Value* table = (expr->_table == ctx->_table_info.m_table) ? ctx->table_value : ctx->inner_table_value;
    llvm::Value* usedRow = (expr->_table == ctx->_table_info.m_table) ? row : innerRow;
    if (usedRow == nullptr) {
        MOT_LOG_TRACE("ProcessVarExpr(): Unexpected VAR expression without a row");
    } else {
        MOT_LOG_DEBUG("ProcessVarExpr(): Using row %p and table %p", usedRow, table);
        int isInnerRow = (usedRow == row) ? 0 : 1;
        int subQueryIndex = -1;
        result = AddReadDatumColumn(ctx, table, usedRow, expr->_column_id, isInnerRow, subQueryIndex);
        AddSetExprCollation(ctx, expr->m_collationId);
    }
    return result;
}

static llvm::Value* ProcessFuncCall(JitLlvmCodeGenContext* ctx, llvm::Value* row, llvm::Value* innerRow, Oid functionId,
    int argCount, JitExpr** args, Oid functionCollationId, Oid resultCollationId, const char* exprName)
{
    llvm::Value* result = nullptr;
    llvm::Value** argArray = nullptr;
    llvm::Value** argIsNull = nullptr;
    Oid* argTypes = nullptr;

    if (argCount > 0) {
        size_t allocSize = sizeof(llvm::Value*) * argCount;
        argArray = (llvm::Value**)MOT::MemSessionAlloc(allocSize);
        if (argArray == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "JIT Compile",
                "Failed to allocate %u bytes for %d arguments in %s expression",
                allocSize,
                argCount,
                exprName);
            return nullptr;
        }

        argIsNull = (llvm::Value**)MOT::MemSessionAlloc(allocSize);
        if (argIsNull == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "JIT Compile",
                "Failed to allocate %u bytes for %d argument nulls in %s expression",
                allocSize,
                argCount,
                exprName);
            MOT::MemSessionFree(argArray);
            return nullptr;
        }

        allocSize = sizeof(Oid) * argCount;
        argTypes = (Oid*)MOT::MemSessionAlloc(allocSize);
        if (argTypes == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "JIT Compile",
                "Failed to allocate %u bytes for %d argument types in %s expression",
                allocSize,
                argCount,
                exprName);
            MOT::MemSessionFree(argArray);
            MOT::MemSessionFree(argIsNull);
            return nullptr;
        }

        for (int i = 0; i < argCount; ++i) {
            argArray[i] = ProcessExpr(ctx, row, innerRow, args[i]);
            if (argArray[i] == nullptr) {
                MOT_LOG_TRACE("Failed to process %s sub-expression %d", exprName, i);
                MOT::MemSessionFree(argArray);
                MOT::MemSessionFree(argIsNull);
                MOT::MemSessionFree(argTypes);
                return nullptr;
            }
            argIsNull[i] = AddGetExprIsNull(ctx);
            argTypes[i] = args[i]->_result_type;
        }
    }

    result = AddInvokePGFunction(
        ctx, functionId, argCount, functionCollationId, resultCollationId, argArray, argIsNull, argTypes);

    if (argCount > 0) {
        MOT::MemSessionFree(argArray);
        MOT::MemSessionFree(argIsNull);
        MOT::MemSessionFree(argTypes);
    }

    return result;
}

static llvm::Value* ProcessOpExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, llvm::Value* innerRow, JitOpExpr* expr)
{
    return ProcessFuncCall(ctx,
        row,
        innerRow,
        expr->_op_func_id,
        expr->_arg_count,
        expr->_args,
        expr->m_opCollationId,
        expr->m_collationId,
        "operator");
}

static llvm::Value* ProcessFuncExpr(
    JitLlvmCodeGenContext* ctx, llvm::Value* row, llvm::Value* innerRow, JitFuncExpr* expr)
{
    return ProcessFuncCall(ctx,
        row,
        innerRow,
        expr->_func_id,
        expr->_arg_count,
        expr->_args,
        expr->m_funcCollationId,
        expr->m_collationId,
        "function");
}

static llvm::Value* ProcessSubLinkExpr(JitLlvmCodeGenContext* ctx, JitSubLinkExpr* expr)
{
    return AddSelectSubQueryResult(ctx, expr->_sub_query_index);
}

static llvm::Value* ProcessBoolNotExpr(
    JitLlvmCodeGenContext* ctx, llvm::Value* row, llvm::Value* innerRow, JitBoolExpr* expr)
{
    llvm::Value* result = nullptr;
    llvm::Value* arg = ProcessExpr(ctx, row, innerRow, expr->_args[0]);
    if (arg == nullptr) {
        MOT_LOG_TRACE("Failed to process Boolean NOT sub-expression");
        return nullptr;
    }
    llvm::Value* argIsNull = AddGetExprIsNull(ctx);

    // if any argument is null then result is null - so we need a local var
    llvm::Value* boolResult = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "bool_result");
    ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(false)), boolResult, true);
    JIT_IF_BEGIN(arg_is_null);
    JIT_IF_EVAL(argIsNull);
    {
        AddSetExprIsNull(ctx, 1);
    }
    JIT_ELSE();
    {
        AddSetExprIsNull(ctx, 0);
        AddSetExprCollation(ctx, InvalidOid);
        llvm::Value* typedZero = llvm::ConstantInt::get(arg->getType(), 0, true);
        llvm::Value* notResult = ctx->m_builder->CreateICmpEQ(arg, typedZero);  // equivalent to NOT
        result = ctx->m_builder->CreateIntCast(notResult, arg->getType(), true);
        ctx->m_builder->CreateStore(result, boolResult, true);
    }
    JIT_IF_END();

    return ctx->m_builder->CreateLoad(boolResult, true);
}

static llvm::Value* ProcessBoolExpr(
    JitLlvmCodeGenContext* ctx, llvm::Value* row, llvm::Value* innerRow, JitBoolExpr* expr)
{
    if (expr->_bool_expr_type == NOT_EXPR) {
        return ProcessBoolNotExpr(ctx, row, innerRow, expr);
    }

    llvm::Value* args[MOT_JIT_MAX_BOOL_EXPR_ARGS] = {nullptr, nullptr};
    llvm::Value* argIsNull[MOT_JIT_MAX_BOOL_EXPR_ARGS] = {nullptr, nullptr};
    int argNum = 0;

    for (int i = 0; i < expr->_arg_count; ++i) {
        args[i] = ProcessExpr(ctx, row, innerRow, expr->_args[i]);
        if (args[i] == nullptr) {
            MOT_LOG_TRACE("Failed to process Boolean sub-expression %d", argNum);
            return nullptr;
        }
        argIsNull[i] = AddGetExprIsNull(ctx);
    }

    // if any argument is null then result is null - so we need a local var
    llvm::Value* boolResult = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "bool_result");
    ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(false)), boolResult, true);
    JIT_IF_BEGIN(lhs_arg_is_null);
    JIT_IF_EVAL(argIsNull[0]);
    {
        AddSetExprIsNull(ctx, 1);
    }
    JIT_ELSE();
    {
        JIT_IF_BEGIN(rhs_arg_is_null);
        JIT_IF_EVAL(argIsNull[1]);
        {
            AddSetExprIsNull(ctx, 1);
        }
        JIT_ELSE();
        {
            AddSetExprIsNull(ctx, 0);
            AddSetExprCollation(ctx, InvalidOid);
            llvm::Value* typedZero = llvm::ConstantInt::get(args[0]->getType(), 0, true);
            switch (expr->_bool_expr_type) {
                case AND_EXPR: {
                    llvm::Value* result = ctx->m_builder->CreateSelect(args[0], args[1], typedZero);
                    ctx->m_builder->CreateStore(result, boolResult, true);
                    break;
                }

                case OR_EXPR: {
                    llvm::Value* typedOne = llvm::ConstantInt::get(args[0]->getType(), 1, true);
                    llvm::Value* result = ctx->m_builder->CreateSelect(args[0], typedOne, args[1]);
                    ctx->m_builder->CreateStore(result, boolResult, true);
                    break;
                }

                default:
                    MOT_LOG_TRACE("Unsupported boolean expression type: %d", (int)expr->_bool_expr_type);
                    boolResult = nullptr;
                    break;
            }
        }
        JIT_IF_END();
    }
    JIT_IF_END();

    return ctx->m_builder->CreateLoad(boolResult, true);
}

static llvm::Value* ProcessScalarArrayOpExpr(
    JitLlvmCodeGenContext* ctx, llvm::Value* row, llvm::Value* innerRow, JitScalarArrayOpExpr* expr)
{
    // if the array is empty, we do not need to process anything, just return true or false according to useOr
    if (expr->m_arraySize == 0) {
        return JIT_CONST_INT64(BoolGetDatum(expr->m_useOr));
    }

    // process the scalar value
    llvm::Value* scalarValue = ProcessExpr(ctx, row, innerRow, expr->m_scalar);
    if (scalarValue == nullptr) {
        MOT_LOG_TRACE("ProcessScalarArrayOpExpr(): Failed to process scalar value in array operation");
        return nullptr;
    }
    llvm::Value* scalarIsNull = AddGetExprIsNull(ctx);
    Oid scalarType = expr->m_scalar->_result_type;

    // get comparison function
    uint32_t argCount = 0;
    bool isStrict = false;
    PGFunction funcPtr = GetPGFunctionInfo(expr->_op_func_id, &argCount, &isStrict);
    if (funcPtr == nullptr) {
        MOT_LOG_TRACE("ProcessScalarArrayOpExpr(): Cannot find function by id: %u", (unsigned)expr->_op_func_id);
        return nullptr;
    }
    MOT_ASSERT(argCount == 2);
    if (argCount != 2) {
        MOT_LOG_TRACE("ProcessScalarArrayOpExpr(): Unexpected argument count %u in function %u",
            argCount,
            (unsigned)expr->_op_func_id);
        return nullptr;
    }

    // we must define a local variable for this to work properly
    llvm::Value* scalarResult = ctx->m_builder->CreateAlloca(ctx->DATUM_T, 0, nullptr, "scalar_result");
    ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(!expr->m_useOr)), scalarResult, true);

    // evaluate result
    JIT_IF_BEGIN(scalar_lhs_is_null);
    // if the scalar is NULL, and the function is strict, return NULL
    // for simpler logic, we embed the isStrict constant in the generated code
    llvm::BasicBlock* postIfBlock = JIT_IF_POST_BLOCK();
    llvm::Value* cond = ctx->m_builder->CreateSelect(JIT_CONST_INT1(isStrict), scalarIsNull, JIT_CONST_INT32(0));
    JIT_IF_EVAL(cond);
    {
        // if the scalar is NULL, and the function is strict, return NULL
        AddSetExprIsNull(ctx, 1);
    }
    JIT_ELSE();
    {
        // iterate over array elements, evaluate each one, and compare to LHS value
        AddSetExprIsNull(ctx, 0);  // initially result is not null
        AddSetExprCollation(ctx, expr->m_collationId);
        for (int i = 0; i < expr->m_arraySize; ++i) {
            JitExpr* e = expr->m_arrayElements[i];
            llvm::Value* element = ProcessExpr(ctx, row, innerRow, e);
            llvm::Value* elementIsNull = AddGetExprIsNull(ctx);

            // invoke function
            llvm::Value* cmpRes = AddInvokePGFunction2(ctx,
                funcPtr,
                expr->m_funcCollationId,
                isStrict,
                scalarValue,
                scalarIsNull,
                scalarType,
                element,
                elementIsNull,
                e->_result_type);
            llvm::Value* cmpResNull = AddGetExprIsNull(ctx);
            JIT_IF_BEGIN(cmp_res_null);
            JIT_IF_EVAL(cmpResNull);
            {
                // if comparison result is null, then entire result is null
                AddSetExprIsNull(ctx, 1);
                JIT_GOTO(postIfBlock);
            }
            JIT_ELSE();
            {
                if (expr->m_useOr) {
                    JIT_IF_BEGIN(eval_cmp);
                    JIT_IF_EVAL(cmpRes);
                    {
                        ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(true)), scalarResult);
                        JIT_GOTO(postIfBlock);
                    }
                    JIT_IF_END();
                } else {
                    JIT_IF_BEGIN(eval_cmp);
                    JIT_IF_EVAL_NOT(cmpRes);
                    {
                        ctx->m_builder->CreateStore(JIT_CONST_INT64(BoolGetDatum(false)), scalarResult);
                        JIT_GOTO(postIfBlock);
                    }
                    JIT_IF_END();
                }
            }
            JIT_IF_END();
        }
    }
    JIT_IF_END();
    return ctx->m_builder->CreateLoad(scalarResult, true);
}

static llvm::Value* ProcessCoerceViaIOExpr(
    JitLlvmCodeGenContext* ctx, llvm::Value* row, llvm::Value* innerRow, JitCoerceViaIOExpr* expr)
{
    llvm::Value* arg = ProcessExpr(ctx, row, innerRow, expr->m_arg);
    llvm::Value* result = AddConvertViaString(
        ctx, arg, JIT_CONST_INT32(expr->m_arg->_result_type), JIT_CONST_INT32(expr->_result_type), JIT_CONST_INT32(-1));
    if (result != nullptr) {
        AddSetExprCollation(ctx, expr->m_collationId);
    }
    return result;
}

llvm::Value* ProcessExpr(JitLlvmCodeGenContext* ctx, llvm::Value* row, llvm::Value* innerRow, JitExpr* expr)
{
    llvm::Value* result = nullptr;

    if (expr->_expr_type == JIT_EXPR_TYPE_CONST) {
        result = ProcessConstExpr(ctx, (JitConstExpr*)expr);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_PARAM) {
        result = ProcessParamExpr(ctx, (JitParamExpr*)expr);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_VAR) {
        result = ProcessVarExpr(ctx, row, innerRow, (JitVarExpr*)expr);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_OP) {
        result = ProcessOpExpr(ctx, row, innerRow, (JitOpExpr*)expr);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_FUNC) {
        result = ProcessFuncExpr(ctx, row, innerRow, (JitFuncExpr*)expr);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_SUBLINK) {
        result = ProcessSubLinkExpr(ctx, (JitSubLinkExpr*)expr);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_BOOL) {
        result = ProcessBoolExpr(ctx, row, innerRow, (JitBoolExpr*)expr);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_SCALAR_ARRAY_OP) {
        result = ProcessScalarArrayOpExpr(ctx, row, innerRow, (JitScalarArrayOpExpr*)expr);
    } else if (expr->_expr_type == JIT_EXPR_TYPE_COERCE_VIA_IO) {
        result = ProcessCoerceViaIOExpr(ctx, row, innerRow, (JitCoerceViaIOExpr*)expr);
    } else {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for query: unsupported target expression type: %d", (int)expr->_expr_type);
    }

    return result;
}

bool buildScanExpression(JitLlvmCodeGenContext* ctx, JitColumnExpr* expr, JitRangeIteratorType range_itr_type,
    JitRangeScanType range_scan_type, llvm::Value* outer_row, int subQueryIndex)
{
    llvm::Value* value = ProcessExpr(ctx, outer_row, nullptr, expr->_expr);
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

bool buildPointScan(JitLlvmCodeGenContext* ctx, JitColumnExprArray* exprArray, JitRangeScanType rangeScanType,
    llvm::Value* outerRow, int exprCount /* = -1 */, int subQueryIndex /* = -1 */)
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
                    "Generate LLVM JIT Code",
                    "Invalid expression table (expected sub-query table %s, got %s)",
                    ctx->m_subQueryTableInfo[subQueryIndex].m_table->GetTableName().c_str(),
                    expr->_table->GetTableName().c_str());
                return false;
            }
        }

        // prepare the sub-expression
        if (!buildScanExpression(ctx, expr, JIT_RANGE_ITERATOR_START, rangeScanType, outerRow, subQueryIndex)) {
            return false;
        }
    }
    return true;
}

bool writeRowColumns(JitLlvmCodeGenContext* ctx, llvm::Value* row, JitColumnExprArray* expr_array, bool is_update)
{
    for (int i = 0; i < expr_array->_count; ++i) {
        JitColumnExpr* column_expr = &expr_array->_exprs[i];

        llvm::Value* value = ProcessExpr(ctx, row, nullptr, column_expr->_expr);
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

bool selectRowColumns(
    JitLlvmCodeGenContext* ctx, llvm::Value* row, JitSelectExprArray* expr_array, llvm::Value* innerRow /* = nullptr */)
{
    for (int i = 0; i < expr_array->_count; ++i) {
        JitSelectExpr* expr = &expr_array->_exprs[i];
        llvm::Value* value = ProcessExpr(ctx, row, innerRow, expr->_expr);
        if (value == nullptr) {
            MOT_LOG_TRACE("selectRowColumns(): Failed to process expression %d", i);
            return false;
        }
        llvm::Value* valueIsNull = AddGetExprIsNull(ctx);
        AddWriteTupleDatum(ctx, expr->_tuple_column_id, value, valueIsNull);
    }

    return true;
}

static bool buildClosedRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, JitRangeScanType range_scan_type,
    llvm::Value* outer_row, int subQueryIndex)
{
    // a closed range scan starts just like a point scan (without enough search expressions) and then adds key patterns
    bool result = buildPointScan(ctx, &index_scan->_search_exprs, range_scan_type, outer_row, -1, subQueryIndex);
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

static void BuildAscendingSemiOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan,
    JitRangeScanType rangeScanType, JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound,
    llvm::Value* outerRow, int subQueryIndex, int offset, int size, JitColumnExpr* lastExpr)
{
    if ((indexScan->_last_dim_op1 == JIT_WOC_LESS_THAN) || (indexScan->_last_dim_op1 == JIT_WOC_LESS_EQUALS)) {
        // this is an upper bound operator on an ascending semi-open scan so we fill the begin key with zeros,
        // and the end key with the value
        AddFillKeyPattern(ctx, 0x00, offset, size, JIT_RANGE_ITERATOR_START, rangeScanType, subQueryIndex);
        buildScanExpression(ctx, lastExpr, JIT_RANGE_ITERATOR_END, rangeScanType, outerRow, subQueryIndex);
        *beginRangeBound = JIT_RANGE_BOUND_INCLUDE;
        *endRangeBound =
            (indexScan->_last_dim_op1 == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
    } else {
        // this is a lower bound operator on an ascending semi-open scan so we fill the begin key with the
        // value, and the end key with 0xFF
        buildScanExpression(ctx, lastExpr, JIT_RANGE_ITERATOR_START, rangeScanType, outerRow, subQueryIndex);
        AddFillKeyPattern(ctx, 0xFF, offset, size, JIT_RANGE_ITERATOR_END, rangeScanType, subQueryIndex);
        *beginRangeBound =
            (indexScan->_last_dim_op1 == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
        *endRangeBound = JIT_RANGE_BOUND_INCLUDE;
    }
}

static void BuildDescendingSemiOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan,
    JitRangeScanType rangeScanType, JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound,
    llvm::Value* outerRow, int subQueryIndex, int offset, int size, JitColumnExpr* lastExpr)
{
    if ((indexScan->_last_dim_op1 == JIT_WOC_LESS_THAN) || (indexScan->_last_dim_op1 == JIT_WOC_LESS_EQUALS)) {
        // this is an upper bound operator on a descending semi-open scan so we fill the begin key with value,
        // and the end key with zeroes
        buildScanExpression(ctx, lastExpr, JIT_RANGE_ITERATOR_START, rangeScanType, outerRow, subQueryIndex);
        AddFillKeyPattern(ctx, 0x00, offset, size, JIT_RANGE_ITERATOR_END, rangeScanType, subQueryIndex);
        *beginRangeBound =
            (indexScan->_last_dim_op1 == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
        *endRangeBound = JIT_RANGE_BOUND_INCLUDE;
    } else {
        // this is a lower bound operator on a descending semi-open scan so we fill the begin key with 0xFF, and
        // the end key with the value
        AddFillKeyPattern(ctx, 0xFF, offset, size, JIT_RANGE_ITERATOR_START, rangeScanType, subQueryIndex);
        buildScanExpression(ctx, lastExpr, JIT_RANGE_ITERATOR_END, rangeScanType, outerRow, subQueryIndex);
        *beginRangeBound = JIT_RANGE_BOUND_INCLUDE;
        *endRangeBound =
            (indexScan->_last_dim_op1 == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
    }
}

static bool buildSemiOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan, JitRangeScanType rangeScanType,
    JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound, llvm::Value* outerRow, int subQueryIndex)
{
    // an open range scan starts just like a point scan (with not enough search expressions) and then adds key patterns
    // we do not use the last search expression
    bool result = buildPointScan(
        ctx, &indexScan->_search_exprs, rangeScanType, outerRow, indexScan->_search_exprs._count - 1, subQueryIndex);
    if (result) {
        AddCopyKey(ctx, rangeScanType, subQueryIndex);
        unsigned char startPattern;
        unsigned char endPattern;
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
                rangeScanType,
                beginRangeBound,
                endRangeBound,
                outerRow,
                subQueryIndex,
                offset,
                size,
                last_expr);
        }

        if (*beginRangeBound == JIT_RANGE_BOUND_INCLUDE) {
            startPattern = (ascending ? 0x00 : 0xFF);
        } else {
            startPattern = (ascending ? 0xFF : 0x00);
        }
        if (*endRangeBound == JIT_RANGE_BOUND_INCLUDE) {
            endPattern = (ascending ? 0xFF : 0x00);
        } else {
            endPattern = (ascending ? 0x00 : 0xFF);
        }
        // now fill the rest as usual
        int first_zero_column = indexScan->_column_count;
        for (int i = first_zero_column; i < index_column_count; ++i) {
            int offset = index_column_offsets[i];
            int size = key_length[i];
            MOT_LOG_DEBUG(
                "Filling begin/end iterator pattern for missing pkey fields at offset %d, size %d", offset, size);
            AddFillKeyPattern(ctx, startPattern, offset, size, JIT_RANGE_ITERATOR_START, rangeScanType, subQueryIndex);
            AddFillKeyPattern(ctx, endPattern, offset, size, JIT_RANGE_ITERATOR_END, rangeScanType, subQueryIndex);
        }

        AddAdjustKey(ctx,
            startPattern,
            JIT_RANGE_ITERATOR_START,
            rangeScanType,  // currently this is relevant only for secondary index searches
            subQueryIndex);
        AddAdjustKey(ctx,
            endPattern,
            JIT_RANGE_ITERATOR_END,
            rangeScanType,  // currently this is relevant only for secondary index searches
            subQueryIndex);
    }
    return result;
}

static void BuildAscendingOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan,
    JitRangeScanType rangeScanType, JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound,
    llvm::Value* outerRow, int subQueryIndex, JitWhereOperatorClass beforeLastDimOp, JitWhereOperatorClass lastDimOp,
    JitColumnExpr* beforeLastExpr, JitColumnExpr* lastExpr)
{
    if ((beforeLastDimOp == JIT_WOC_LESS_THAN) || (beforeLastDimOp == JIT_WOC_LESS_EQUALS)) {
        MOT_ASSERT((lastDimOp == JIT_WOC_GREATER_THAN) || (lastDimOp == JIT_WOC_GREATER_EQUALS));
        // the before-last operator is an upper bound operator on an ascending open scan so we fill the begin
        // key with the last value, and the end key with the before-last value
        MOT_LOG_TRACE("Building inverted ascending open range scan");
        buildScanExpression(ctx,
            lastExpr,
            JIT_RANGE_ITERATOR_START,
            rangeScanType,
            outerRow,  // lower bound on begin iterator key
            subQueryIndex);
        buildScanExpression(ctx,
            beforeLastExpr,
            JIT_RANGE_ITERATOR_END,
            rangeScanType,
            outerRow,  // upper bound on end iterator key
            subQueryIndex);
        *beginRangeBound = (lastDimOp == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
        *endRangeBound = (beforeLastDimOp == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
    } else {
        MOT_LOG_TRACE("Building regular ascending open range scan");
        MOT_ASSERT((lastDimOp == JIT_WOC_LESS_THAN) || (lastDimOp == JIT_WOC_LESS_EQUALS));
        // the before-last operator is a lower bound operator on an ascending open scan so we fill the begin key
        // with the before-last value, and the end key with the last value
        buildScanExpression(ctx,
            beforeLastExpr,
            JIT_RANGE_ITERATOR_START,
            rangeScanType,
            outerRow,  // lower bound on begin iterator key
            subQueryIndex);
        buildScanExpression(ctx,
            lastExpr,
            JIT_RANGE_ITERATOR_END,
            rangeScanType,
            outerRow,  // upper bound on end iterator key
            subQueryIndex);
        *beginRangeBound =
            (beforeLastDimOp == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
        *endRangeBound = (lastDimOp == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
    }
}

static void BuildDescendingOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan,
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
            JIT_RANGE_ITERATOR_START,
            rangeScanType,
            outerRow,  // upper bound on begin iterator key
            subQueryIndex);
        buildScanExpression(ctx,
            lastExpr,
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
            JIT_RANGE_ITERATOR_START,
            rangeScanType,
            outerRow,  // upper bound on begin iterator key
            subQueryIndex);
        buildScanExpression(ctx,
            beforeLastExpr,
            JIT_RANGE_ITERATOR_END,
            rangeScanType,
            outerRow,  // lower bound on end iterator key
            subQueryIndex);
        *beginRangeBound = (lastDimOp == JIT_WOC_LESS_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
        *endRangeBound =
            (beforeLastDimOp == JIT_WOC_GREATER_EQUALS) ? JIT_RANGE_BOUND_INCLUDE : JIT_RANGE_BOUND_EXCLUDE;
    }
}

static bool buildOpenRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, JitRangeScanType range_scan_type,
    JitRangeBoundMode* begin_range_bound, JitRangeBoundMode* end_range_bound, llvm::Value* outer_row, int subQueryIndex)
{
    // an open range scan starts just like a point scan (with not enough search expressions) and then adds key patterns
    // we do not use the last two expressions
    MOT_LOG_TRACE("Starting with point scan");
    bool result = buildPointScan(ctx,
        &index_scan->_search_exprs,
        range_scan_type,
        outer_row,
        index_scan->_search_exprs._count - 2,
        subQueryIndex);
    if (result) {
        AddCopyKey(ctx, range_scan_type, subQueryIndex);
        unsigned char startPattern;
        unsigned char endPattern;
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
            MOT_LOG_TRACE("Building main table scan");
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
            MOT_LOG_TRACE("Building ascending open range scan");
            BuildAscendingOpenRangeScan(ctx,
                index_scan,
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
            MOT_LOG_TRACE("Building descending open range scan");
            BuildDescendingOpenRangeScan(ctx,
                index_scan,
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

        if (*begin_range_bound == JIT_RANGE_BOUND_INCLUDE) {
            startPattern = (ascending ? 0x00 : 0xFF);
        } else {
            startPattern = (ascending ? 0xFF : 0x00);
        }
        if (*end_range_bound == JIT_RANGE_BOUND_INCLUDE) {
            endPattern = (ascending ? 0xFF : 0x00);
        } else {
            endPattern = (ascending ? 0x00 : 0xFF);
        }
        // now fill the rest as usual
        int first_zero_column = index_scan->_column_count;
        for (int i = first_zero_column; i < index_column_count; ++i) {
            int offset = index_column_offsets[i];
            int size = key_length[i];
            MOT_LOG_DEBUG(
                "Filling begin/end iterator pattern for missing pkey fields at offset %d, size %d", offset, size);
            AddFillKeyPattern(
                ctx, startPattern, offset, size, JIT_RANGE_ITERATOR_START, range_scan_type, subQueryIndex);
            AddFillKeyPattern(ctx, endPattern, offset, size, JIT_RANGE_ITERATOR_END, range_scan_type, subQueryIndex);
        }

        AddAdjustKey(ctx,
            startPattern,
            JIT_RANGE_ITERATOR_START,
            range_scan_type,  // currently this is relevant only for secondary index searches
            subQueryIndex);
        AddAdjustKey(ctx,
            endPattern,
            JIT_RANGE_ITERATOR_END,
            range_scan_type,  // currently this is relevant only for secondary index searches
            subQueryIndex);
    }
    return result;
}

static bool buildRangeScan(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan, JitRangeScanType rangeScanType,
    JitRangeBoundMode* beginRangeBound, JitRangeBoundMode* endRangeBound, llvm::Value* outerRow, int subQueryIndex = -1)
{
    bool result = false;

    // if this is a point scan we generate two identical keys for the iterators
    if (indexScan->_scan_type == JIT_INDEX_SCAN_POINT) {
        MOT_LOG_TRACE("Building point scan");
        result = buildPointScan(ctx, &indexScan->_search_exprs, rangeScanType, outerRow, -1, subQueryIndex);
        if (result) {
            AddCopyKey(ctx, rangeScanType, subQueryIndex);
            *beginRangeBound = JIT_RANGE_BOUND_INCLUDE;
            *endRangeBound = JIT_RANGE_BOUND_INCLUDE;
        }
    } else if (indexScan->_scan_type == JIT_INDEX_SCAN_CLOSED) {
        MOT_LOG_TRACE("Building closed range scan");
        result = buildClosedRangeScan(ctx, indexScan, rangeScanType, outerRow, subQueryIndex);
        if (result) {
            *beginRangeBound = JIT_RANGE_BOUND_INCLUDE;
            *endRangeBound = JIT_RANGE_BOUND_INCLUDE;
        }
    } else if (indexScan->_scan_type == JIT_INDEX_SCAN_SEMI_OPEN) {
        MOT_LOG_TRACE("Building semi-open range scan");
        result = buildSemiOpenRangeScan(
            ctx, indexScan, rangeScanType, beginRangeBound, endRangeBound, outerRow, subQueryIndex);
    } else if (indexScan->_scan_type == JIT_INDEX_SCAN_OPEN) {
        MOT_LOG_TRACE("Building open range scan");
        result =
            buildOpenRangeScan(ctx, indexScan, rangeScanType, beginRangeBound, endRangeBound, outerRow, subQueryIndex);
    } else if (indexScan->_scan_type == JIT_INDEX_SCAN_FULL) {
        MOT_LOG_TRACE("Building full range scan");
        result = true;  // no keys used
        *beginRangeBound = JIT_RANGE_BOUND_INCLUDE;
        *endRangeBound = JIT_RANGE_BOUND_INCLUDE;
    }
    return result;
}

static bool buildPrepareStateScan(
    JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan, JitRangeScanType range_scan_type, llvm::Value* outer_row)
{
    JitRangeBoundMode begin_range_bound = JIT_RANGE_BOUND_NONE;
    JitRangeBoundMode end_range_bound = JIT_RANGE_BOUND_NONE;

    // emit code to check if state iterators are null
    JIT_IF_BEGIN(state_iterators_exist);
    llvm::Value* is_begin_itr_null = AddIsStateIteratorNull(ctx, JIT_RANGE_ITERATOR_START, range_scan_type);
    JIT_IF_EVAL(is_begin_itr_null);
    {
        // prepare search keys
        if (!buildRangeScan(ctx, index_scan, range_scan_type, &begin_range_bound, &end_range_bound, outer_row)) {
            MOT_LOG_TRACE("Failed to generate jitted code for range select query: unsupported WHERE clause type");
            return false;
        }

        // search begin iterator and save it in execution state
        IssueDebugLog("Building search iterator from search key, and saving in execution state");
        if (index_scan->_scan_type == JIT_INDEX_SCAN_FULL) {
            llvm::Value* itr = buildBeginIterator(ctx, range_scan_type);
            AddSetStateIterator(ctx, itr, JIT_RANGE_ITERATOR_START, range_scan_type);
        } else {
            llvm::Value* itr =
                buildSearchIterator(ctx, index_scan->_scan_direction, begin_range_bound, range_scan_type);
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
    }
    JIT_IF_END();

    return true;
}

static bool buildPrepareStateRow(JitLlvmCodeGenContext* ctx, MOT::AccessType access_mode, JitIndexScan* index_scan,
    JitRangeScanType range_scan_type, llvm::BasicBlock* next_block, llvm::Value* outerRow, bool emitReturnOnFail)
{
    llvm::LLVMContext& context = ctx->m_codeGen->context();

    MOT_LOG_DEBUG("Generating select code for stateful range select");
    llvm::Value* row = nullptr;

    // we start a new block so current block must end with terminator
    DEFINE_BLOCK(prepare_state_row_bb, ctx->m_jittedFunction);
    MOT_LOG_DEBUG("Adding unconditional branch into prepare_state_row_bb from branch %s",
        ctx->m_builder->GetInsertBlock()->getName().data());
    ctx->m_builder->CreateBr(prepare_state_row_bb);
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_DEBUG)) {
        MOT_LOG_BEGIN(MOT::LogLevel::LL_DEBUG, "Added unconditional branch result:");
        std::string s;
        llvm::raw_string_ostream os(s);
        ctx->m_builder->GetInsertBlock()->back().print(os);
        MOT_LOG_APPEND(MOT::LogLevel::LL_DEBUG, "%s", s.c_str());
        MOT_LOG_END(MOT::LogLevel::LL_DEBUG);
    }
    ctx->m_builder->SetInsertPoint(prepare_state_row_bb);

    // check if state row is nullptr
    JIT_IF_BEGIN(test_state_row);
    IssueDebugLog("Checking if state row is nullptr");
    llvm::Value* res = AddIsStateRowNull(ctx, range_scan_type);
    JIT_IF_EVAL(res);
    {
        IssueDebugLog("State row is nullptr, fetching from state iterators");

        // check if state scan ended
        JIT_IF_BEGIN(test_scan);
        IssueDebugLog("Checking if state scan ended");
        llvm::BasicBlock* isStateScanEndBlock = JIT_CURRENT_BLOCK();  // remember current block if filter fails
        llvm::Value* res = AddIsStateScanEnd(ctx, index_scan->_scan_direction, range_scan_type);
        JIT_IF_EVAL(res);
        {
            // fail scan block
            IssueDebugLog("Scan ended, raising internal state scan end flag");
            AddSetStateScanEndFlag(ctx, 1, range_scan_type);
        }
        JIT_ELSE();
        {
            // now get row from iterator
            IssueDebugLog("State scan not ended - Retrieving row from iterator");
            row = AddGetRowFromStateIterator(ctx, access_mode, index_scan->_scan_direction, range_scan_type);

            // check if row was found
            JIT_IF_BEGIN(test_row_found);
            JIT_IF_EVAL_NOT(row);
            {
                // row not found branch
                IssueDebugLog("Could not retrieve row from state iterator, raising internal state scan end flag");
                AddSetStateScanEndFlag(ctx, 1, range_scan_type);
            }
            JIT_ELSE();
            {
                // row found, check for additional filters, if not passing filter then go back to execute
                // IsStateScanEnd()
                llvm::Value* outerRowReal =
                    outerRow ? outerRow : ((range_scan_type != JIT_RANGE_SCAN_INNER) ? row : nullptr);
                llvm::Value* innerRow = (range_scan_type == JIT_RANGE_SCAN_INNER) ? row : nullptr;
                if (!buildFilterRow(ctx, outerRowReal, innerRow, &index_scan->_filters, isStateScanEndBlock)) {
                    MOT_LOG_TRACE(
                        "Failed to generate jitted code for query: failed to build filter expressions for row");
                    return false;
                }
                // row passed all filters, so save it in state outer row
                AddSetStateRow(ctx, row, range_scan_type);
                if (range_scan_type == JIT_RANGE_SCAN_MAIN) {
                    AddSetStateScanEndFlag(ctx, 0, JIT_RANGE_SCAN_INNER);  // reset inner scan flag for JOIN queries
                }
            }
            JIT_IF_END();
        }
        JIT_IF_END();
    }
    JIT_IF_END();

    // cleanup state iterators if needed and return/jump to next block
    JIT_IF_BEGIN(state_scan_ended);
    IssueDebugLog("Checking if state scan ended flag was raised");
    llvm::Value* state_scan_end_flag = AddGetStateScanEndFlag(ctx, range_scan_type);
    JIT_IF_EVAL(state_scan_end_flag);
    {
        IssueDebugLog(" State scan ended flag was raised, cleaning up iterators and reporting to user");
        AddDestroyStateIterators(ctx, range_scan_type);  // cleanup
        if ((range_scan_type == JIT_RANGE_SCAN_MAIN) || (next_block == nullptr)) {
            // either a main scan ended (simple range or outer loop of join),
            // or an inner range ended in an outer point join (so next block is null)
            // in either case let caller know scan ended and return appropriate value
            AddSetScanEnded(ctx, 1);  // no outer row, we are definitely done
            if (emitReturnOnFail) {
                JIT_PROFILE_END(ctx);
                JIT_RETURN(JIT_CONST_INT32(MOT::RC_LOCAL_ROW_NOT_FOUND));
            } else {
                AddResetStateRow(ctx, range_scan_type);  // put null in state row, whether during inner or outer loop
            }
        } else {
            AddResetStateRow(ctx, JIT_RANGE_SCAN_MAIN);  // make sure a new row is fetched in outer loop
            ctx->m_builder->CreateBr(next_block);        // jump back to outer loop
        }
    }
    JIT_IF_END();

    return true;
}

llvm::Value* buildPrepareStateScanRow(JitLlvmCodeGenContext* ctx, JitIndexScan* index_scan,
    JitRangeScanType range_scan_type, MOT::AccessType access_mode, llvm::Value* outer_row, llvm::BasicBlock* next_block,
    llvm::BasicBlock** loop_block, bool emitReturnOnFail /* = true */)
{
    // prepare stateful scan if not done so already
    if (!buildPrepareStateScan(ctx, index_scan, range_scan_type, outer_row)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range JOIN query: unsupported %s WHERE clause type",
            (range_scan_type == JIT_RANGE_SCAN_MAIN) ? "outer" : "inner");
        return nullptr;
    }

    // mark position for later jump
    if (loop_block != nullptr) {
        *loop_block = llvm::BasicBlock::Create(ctx->m_codeGen->context(), "fetch_outer_row_bb", ctx->m_jittedFunction);
        ctx->m_builder->CreateBr(*loop_block);        // end current block
        ctx->m_builder->SetInsertPoint(*loop_block);  // start new block
    }

    // fetch row for read
    if (!buildPrepareStateRow(ctx, access_mode, index_scan, range_scan_type, next_block, outer_row, emitReturnOnFail)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range JOIN query: failed to build search outer row block");
        return nullptr;
    }
    llvm::Value* row = AddGetStateRow(ctx, range_scan_type);
    return row;
}

JitLlvmRuntimeCursor buildRangeCursor(JitLlvmCodeGenContext* ctx, JitIndexScan* indexScan,
    JitRangeScanType rangeScanType, llvm::Value* outerRow, int subQueryIndex /* = -1 */)
{
    JitLlvmRuntimeCursor result = {nullptr, nullptr};
    JitRangeBoundMode beginRangeBound = JIT_RANGE_BOUND_NONE;
    JitRangeBoundMode endRangeBound = JIT_RANGE_BOUND_NONE;
    JitIndexScanDirection indexScanDirection = indexScan->_scan_direction;
    if (!buildRangeScan(ctx, indexScan, rangeScanType, &beginRangeBound, &endRangeBound, outerRow, subQueryIndex)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range query: unsupported %s-loop WHERE clause type",
            outerRow ? "inner" : "outer");
        return result;
    }

    // build range iterators
    if (indexScan->_scan_type == JIT_INDEX_SCAN_FULL) {
        result.begin_itr = buildBeginIterator(ctx, rangeScanType);
        result.end_itr = ctx->m_builder->CreateCast(
            llvm::Instruction::IntToPtr, JIT_CONST_INT64(0), ctx->IndexIteratorType->getPointerTo());
    } else {
        result.begin_itr = buildSearchIterator(ctx, indexScanDirection, beginRangeBound, rangeScanType, subQueryIndex);
        result.end_itr = AddCreateEndIterator(ctx, indexScanDirection, endRangeBound, rangeScanType, subQueryIndex);
    }

    return result;
}

bool prepareAggregateAvg(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, int aggIndex)
{
    // although we already have this information in the aggregate descriptor, we still check again
    switch (aggregate->_func_id) {
        case INT8AVGFUNCOID:
        case NUMERICAVGFUNCOID:
            // the current_aggregate is a 2 numeric array
            AddPrepareAvgArray(ctx, aggIndex, NUMERICOID, 2);
            break;

        case INT4AVGFUNCOID:
        case INT2AVGFUNCOID:
        case INT1AVGFUNCOID:
            // the current_aggregate is a 2 int8 array
            AddPrepareAvgArray(ctx, aggIndex, INT8OID, 2);
            break;

        case FLOAT4AVGFUNCOID:
        case FLOAT8AVGFUNCOID:
            // the current_aggregate is a 3 float8 array
            AddPrepareAvgArray(ctx, aggIndex, FLOAT8OID, 3);
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate AVG() operator function type: %d", aggregate->_func_id);
            return false;
    }

    return true;
}

bool prepareAggregateSum(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, int aggIndex)
{
    // NOTE: there is NO sum function for type int1
    switch (aggregate->_func_id) {
        case INT8SUMFUNCOID:
            // current sum in numeric, and value is int8, both can be null
            AddResetAggValue(ctx, aggIndex, NUMERICOID);
            break;

        case INT4SUMFUNCOID:
        case INT2SUMFUNCOID:
            // current aggregate is a int8, and value is int4, both can be null
            AddResetAggValue(ctx, aggIndex, INT8OID);
            break;

        case 2110:  // float4
            // current aggregate is a float4, and value is float4, both can **NOT** be null
            AddResetAggValue(ctx, aggIndex, FLOAT4OID);
            break;

        case 2111:  // float8
            // current aggregate is a float8, and value is float8, both can **NOT** be null
            AddResetAggValue(ctx, aggIndex, FLOAT8OID);
            break;

        case NUMERICSUMFUNCOID:
            AddResetAggValue(ctx, aggIndex, NUMERICOID);
            // current aggregate is a numeric, and value is numeric, both can **NOT** be null
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate SUM() operator function type: %d", aggregate->_func_id);
            return false;
    }
    return true;
}

bool prepareAggregateMaxMin(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate, int aggIndex)
{
    AddSetAggValueIsNull(ctx, aggIndex, JIT_CONST_INT32(1));
    return true;
}

bool prepareAggregateCount(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate, int aggIndex)
{
    switch (aggregate->_func_id) {
        case 2147:  // int8inc_any
        case 2803:  // int8inc
            // current aggregate is int8, and can **NOT** be null
            AddResetAggValue(ctx, aggIndex, INT8OID);
            AddSetAggValueIsNull(ctx, aggIndex, JIT_CONST_INT32(0));
            break;

        default:
            MOT_LOG_TRACE("Unsupported aggregate COUNT() operator function type: %d", aggregate->_func_id);
            return false;
    }
    return true;
}

static bool prepareDistinctSet(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, int aggIndex)
{
    // we need a hash-set according to the aggregated type (preferably but not necessarily linear-probing hash)
    // we use an opaque datum type, with a tailor-made hash-function and equals function
    AddPrepareDistinctSet(ctx, aggIndex, aggregate->_element_type);
    return true;
}

bool prepareAggregates(JitLlvmCodeGenContext* ctx, JitAggregate* aggregates, int aggCount)
{
    bool result = false;
    for (int i = 0; i < aggCount; ++i) {
        switch (aggregates[i]._aggreaget_op) {
            case JIT_AGGREGATE_AVG:
                result = prepareAggregateAvg(ctx, &aggregates[i], i);
                break;

            case JIT_AGGREGATE_SUM:
                result = prepareAggregateSum(ctx, &aggregates[i], i);
                break;

            case JIT_AGGREGATE_MAX:
            case JIT_AGGREGATE_MIN:
                result = prepareAggregateMaxMin(ctx, &aggregates[i], i);
                break;

            case JIT_AGGREGATE_COUNT:
                result = prepareAggregateCount(ctx, &aggregates[i], i);
                break;

            default:
                MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                    "JIT Compile",
                    "Cannot prepare for aggregation: invalid aggregate operator %d",
                    (int)aggregates[i]._aggreaget_op);
                result = false;
                break;
        }

        if (result) {
            if (aggregates[i]._distinct) {
                result = prepareDistinctSet(ctx, &aggregates[i], i);
            }
        }
    }

    return result;
}

static llvm::Value* BuildAggregateFunc(
    JitLlvmCodeGenContext* ctx, Oid funcId, Oid collationId, llvm::Value* currentAggregate, llvm::Value* varExpr)
{
    uint32_t argCount = 0;
    bool isStrict = false;
    PGFunction funcPtr = GetPGAggFunctionInfo(funcId, &argCount, &isStrict);
    if (funcPtr == nullptr) {
        MOT_LOG_TRACE("BuildAggregateFunc(): Cannot find function by id: %u", (unsigned)funcId);
        return nullptr;
    }
    MOT_ASSERT(argCount == 2);
    if (argCount != 2) {
        MOT_LOG_TRACE(
            "BuildAggregateFunc(): Invalid argument count %u in aggregate function %u", argCount, (unsigned)funcId);
        return nullptr;
    }
    llvm::Value* isNull = JIT_CONST_INT32(0);
    // aggregate functions do not require argument types
    llvm::Value* result = AddInvokePGFunction2(
        ctx, funcPtr, collationId, isStrict, currentAggregate, isNull, InvalidOid, varExpr, isNull, InvalidOid);
    return result;
}

static llvm::Value* buildAggregateAvg(
    JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, llvm::Value* current_aggregate, llvm::Value* var_expr)
{
    return BuildAggregateFunc(ctx, aggregate->_func_id, aggregate->m_funcCollationId, current_aggregate, var_expr);
}

static llvm::Value* buildAggregateSum(
    JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, llvm::Value* current_aggregate, llvm::Value* var_expr)
{
    return BuildAggregateFunc(ctx, aggregate->_func_id, aggregate->m_funcCollationId, current_aggregate, var_expr);
}

static llvm::Value* buildAggregateMax(
    JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, llvm::Value* current_aggregate, llvm::Value* var_expr)
{
    return BuildAggregateFunc(ctx, aggregate->_func_id, aggregate->m_funcCollationId, current_aggregate, var_expr);
}

static llvm::Value* buildAggregateMin(
    JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, llvm::Value* current_aggregate, llvm::Value* var_expr)
{
    return BuildAggregateFunc(ctx, aggregate->_func_id, aggregate->m_funcCollationId, current_aggregate, var_expr);
}

static llvm::Value* buildAggregateCount(
    JitLlvmCodeGenContext* ctx, const JitAggregate* aggregate, llvm::Value* count_aggregate)
{
    uint32_t argCount = 0;
    bool isStrict = false;
    PGFunction funcPtr = GetPGAggFunctionInfo(aggregate->_func_id, &argCount, &isStrict);
    if (funcPtr == nullptr) {
        MOT_LOG_TRACE("buildAggregateCount(): Cannot find function by id: %u", (unsigned)aggregate->_func_id);
        return nullptr;
    }

    // some aggregate functions use more than 1 argument, which are all dummies, so we just fill nulls.
    // see implementation of int8inc_any,
    // note: aggregate functions do not require argument types
    llvm::Value* result = nullptr;
    llvm::Value* isNull = JIT_CONST_INT32(0);
    llvm::Value* dummy = JIT_CONST_INT64(0);
    if (argCount == 1) {
        result = AddInvokePGFunction1(
            ctx, funcPtr, aggregate->m_funcCollationId, isStrict, count_aggregate, isNull, InvalidOid);
    } else if (argCount == 2) {
        result = AddInvokePGFunction2(ctx,
            funcPtr,
            aggregate->m_funcCollationId,
            isStrict,
            count_aggregate,
            isNull,
            InvalidOid,
            dummy,
            isNull,
            InvalidOid);
    } else {
        MOT_LOG_TRACE("buildAggregateCount(): Invalid argument count %u in aggregate function %u",
            argCount,
            (unsigned)aggregate->_func_id);
        return nullptr;
    }
    return result;
}

static bool buildAggregateMaxMin(
    JitLlvmCodeGenContext* ctx, int aggIndex, JitAggregate* aggregate, llvm::Value* var_expr)
{
    bool result = false;
    llvm::Value* aggregateExpr = nullptr;

    // we first check if the min/max value is null and if so just store the column value
    JIT_IF_BEGIN(test_max_min_value_null);
    llvm::Value* res = AddGetAggValueIsNull(ctx, aggIndex);
    JIT_IF_EVAL(res);
    {
        AddSetAggValue(ctx, aggIndex, var_expr);
    }
    JIT_ELSE();
    {
        // get the aggregated value and call the operator
        llvm::Value* current_aggregate = AddGetAggValue(ctx, aggIndex);
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

        if (aggregateExpr != nullptr) {
            AddSetAggValue(ctx, aggIndex, aggregateExpr);
            result = true;
        }
    }
    JIT_IF_END();

    return result;
}

static bool buildAggregateTuple(
    JitLlvmCodeGenContext* ctx, int aggIndex, JitAggregate* aggregate, llvm::Value* var_expr)
{
    bool result = false;

    if ((aggregate->_aggreaget_op == JIT_AGGREGATE_MAX) || (aggregate->_aggreaget_op == JIT_AGGREGATE_MIN)) {
        result = buildAggregateMaxMin(ctx, aggIndex, aggregate, var_expr);
    } else {
        // get the aggregated value
        llvm::Value* current_aggregate = nullptr;
        if (aggregate->_aggreaget_op == JIT_AGGREGATE_AVG) {
            current_aggregate = AddLoadAvgArray(ctx, aggIndex);
        } else {
            current_aggregate = AddGetAggValue(ctx, aggIndex);
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
                AddSaveAvgArray(ctx, aggIndex, aggregate_expr);
            } else {
                AddSetAggValue(ctx, aggIndex, aggregate_expr);
            }
            result = true;
        } else {
            MOT_LOG_TRACE("Failed to generate aggregate AVG/SUM/COUNT code");
        }
    }

    return result;
}

static inline bool BuildAggreagateValue(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate, int aggIndex,
    llvm::Value* value, llvm::Value* isNull, llvm::Value* aggCount)
{
    bool result = true;
    JIT_IF_BEGIN(agg_value_is_null);
    JIT_IF_EVAL(isNull);
    {
        IssueDebugLog("Ignoring null value in aggregate");
    }
    JIT_ELSE();
    {
        IssueDebugLog("Aggregating value");
        result = buildAggregateTuple(ctx, aggIndex, aggregate, value);
        if (result) {
            // update number of aggregates processed for this row
            llvm::Value* aggValue = ctx->m_builder->CreateLoad(aggCount, true);
            llvm::Value* newValue = ctx->m_builder->CreateAdd(aggValue, JIT_CONST_INT32(1));
            ctx->m_builder->CreateStore(newValue, aggCount, true);
        }
    }
    JIT_IF_END();
    return result;
}

bool buildAggregateRow(JitLlvmCodeGenContext* ctx, JitAggregate* aggregate, int aggIndex, llvm::Value* row,
    llvm::Value* innerRow, llvm::Value* aggCount)
{
    bool result = false;

    // extract the aggregated column
    llvm::Value* expr = nullptr;
    llvm::Value* exprIsNull = nullptr;
    if (aggregate->_table != nullptr) {
        llvm::Value* table =
            (aggregate->_table == ctx->_table_info.m_table) ? ctx->table_value : ctx->inner_table_value;
        llvm::Value* usedRow = (aggregate->_table == ctx->_table_info.m_table) ? row : innerRow;
        int isInnerRow = (usedRow == innerRow) ? 1 : 0;
        int subQueryIndex = -1;
        expr = AddReadDatumColumn(ctx, table, usedRow, aggregate->_table_column_id, isInnerRow, subQueryIndex);
        exprIsNull = AddGetExprIsNull(ctx);
    } else {
        expr = llvm::ConstantInt::get(ctx->DATUM_T, Int64GetDatum(1), true);
        exprIsNull = llvm::ConstantInt::get(ctx->DATUM_T, BoolGetDatum(false), true);
    }

    // we first check if we have DISTINCT modifier
    if (aggregate->_distinct) {
        // check row is distinct in state set (managed on the current jit context)
        // emit code to convert column to primitive data type, add value to distinct set and verify
        // if value is not distinct then do not use this row in aggregation
        JIT_IF_BEGIN(test_distinct_value);
        llvm::Value* res = AddInsertDistinctItem(ctx, aggIndex, aggregate->_element_type, expr);
        JIT_IF_EVAL_NOT(res);
        {
            IssueDebugLog("Value is not distinct, skipping aggregated row");
        }
        JIT_ELSE();
        {
            IssueDebugLog("Aggregating distinct value");
            result = BuildAggreagateValue(ctx, aggregate, aggIndex, expr, exprIsNull, aggCount);
        }
        JIT_IF_END();
    } else {
        result = BuildAggreagateValue(ctx, aggregate, aggIndex, expr, exprIsNull, aggCount);
    }

    return result;
}

void buildAggregateResult(JitLlvmCodeGenContext* ctx, const JitAggregate* aggregates, int aggCount)
{
    // in case of average we compute it when aggregate loop is done
    for (int aggIndex = 0; aggIndex < aggCount; ++aggIndex) {
        const JitAggregate* aggregate = &aggregates[aggIndex];

        llvm::Value* aggIsNull = AddGetAggValueIsNull(ctx, aggIndex);
        if (aggregate->_aggreaget_op == JIT_AGGREGATE_AVG) {
            llvm::Value* avg_value = AddComputeAvgFromArray(ctx, aggIndex, aggregate->_avg_element_type);
            AddWriteTupleDatum(ctx, aggIndex, avg_value, aggIsNull);
        } else {
            llvm::Value* count_value = AddGetAggValue(ctx, aggIndex);
            AddWriteTupleDatum(ctx, aggIndex, count_value, aggIsNull);
        }

        // we take the opportunity to cleanup as well
        if (aggregate->_distinct) {
            AddDestroyDistinctSet(ctx, aggIndex, aggregate->_element_type);
        }
    }
}

void buildCheckLimit(JitLlvmCodeGenContext* ctx, int limit_count)
{
    // if a limit clause exists, then increment limit counter and check if reached limit
    if (limit_count > 0) {
        AddIncrementStateLimitCounter(ctx);
        JIT_IF_BEGIN(limit_count_reached);
        llvm::Value* current_limit_count = AddGetStateLimitCounter(ctx);
        llvm::Value* limit_count_inst = JIT_CONST_INT32(limit_count);
        JIT_IF_EVAL_CMP(current_limit_count, limit_count_inst, JIT_ICMP_EQ);
        {
            IssueDebugLog("Reached limit specified in limit clause, raising internal state scan end flag");
            // cleanup and signal scan ended (it is safe to cleanup even if not initialized, so we cleanup everything)
            AddDestroyStateIterators(ctx, JIT_RANGE_SCAN_MAIN);
            AddDestroyStateIterators(ctx, JIT_RANGE_SCAN_INNER);
            AddSetScanEnded(ctx, 1);
        }
        JIT_IF_END();
    }
}

void BuildCheckLimitNoState(JitLlvmCodeGenContext* ctx, int limitCount)
{
    if (limitCount > 0) {
        JIT_IF_BEGIN(limit_count_reached);
        llvm::Value* rowCount = ctx->m_builder->CreateLoad(ctx->rows_processed, true);
        llvm::Value* limitCountValue = JIT_CONST_INT32(limitCount);
        JIT_IF_EVAL_CMP(rowCount, limitCountValue, JIT_ICMP_EQ);
        {
            IssueDebugLog("Reached limit specified in limit clause, range scan ended");
            JIT_WHILE_BREAK();
        }
        JIT_IF_END();
    }
}

bool BuildSelectLeftJoinRowColumns(JitLlvmCodeGenContext* ctx, llvm::Value* outerRowCopy, JitJoinPlan* plan,
    llvm::Value* innerRow, llvm::Value* filterPassed)
{
    JIT_IF_BEGIN(filters_passed);
    JIT_IF_EVAL_NOT(ctx->m_builder->CreateLoad(ctx->INT32_T, filterPassed));
    {
        IssueDebugLog("Selecting null column values for LEFT JOIN");
        llvm::Value* nullRow =
            ctx->m_builder->CreateCast(llvm::Instruction::IntToPtr, JIT_CONST_INT64(0), ctx->RowType->getPointerTo());
        if (!selectRowColumns(ctx, outerRowCopy, &plan->_select_exprs, nullRow)) {
            MOT_LOG_TRACE(
                "Failed to generate jitted code for Point JOIN query: failed to select row columns into result tuple");
            return false;
        }
    }
    JIT_ELSE();
    {
        // now begin selecting columns into result
        MOT_LOG_TRACE("Selecting row columns in the result tuple for LEFT JOIN");
        if (!selectRowColumns(ctx, outerRowCopy, &plan->_select_exprs, innerRow)) {
            MOT_LOG_TRACE(
                "Failed to generate jitted code for Point JOIN query: failed to select row columns into result tuple");
            return false;
        }
    }
    JIT_IF_END();

    return true;
}

static bool BuildOneTimeFilter(JitLlvmCodeGenContext* ctx, JitExpr* filter, llvm::BasicBlock* nextBlock)
{
    llvm::Value* filterExpr = ProcessExpr(ctx, nullptr, nullptr, filter);
    if (filterExpr == nullptr) {
        MOT_LOG_TRACE("BuildOneTimeFilter(): Failed to process filter expression");
        return false;
    }

    JIT_IF_BEGIN(filter_row);
    IssueDebugLog("Checking if one-time filter passes");
    JIT_IF_EVAL_NOT(filterExpr);
    {
        IssueDebugLog("One-time filter did not pass");
        if (nextBlock != nullptr) {
            JIT_GOTO(nextBlock);
        } else {
            JIT_PROFILE_END(ctx);
            JIT_RETURN(JIT_CONST_INT32(MOT::RC_LOCAL_ROW_NOT_FOUND));
        }
    }
    JIT_ELSE();
    {
        IssueDebugLog("One-time filter passed");
    }
    JIT_IF_END();
    return true;
}

bool BuildOneTimeFilters(
    JitLlvmCodeGenContext* ctx, JitFilterArray* oneTimeFilterArray, llvm::BasicBlock* nextBlock /* = nullptr */)
{
    MOT_LOG_TRACE("Building %d one-time filters", oneTimeFilterArray->_filter_count);
    for (int i = 0; i < oneTimeFilterArray->_filter_count; ++i) {
        if (!BuildOneTimeFilter(ctx, oneTimeFilterArray->_scan_filters[i], nextBlock)) {
            MOT_LOG_TRACE("Failed to build one-time filter %d", i);
            return false;
        }
    }
    return true;
}
}  // namespace JitExec
