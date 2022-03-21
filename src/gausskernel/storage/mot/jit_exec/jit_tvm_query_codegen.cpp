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
 * jit_tvm_query_codegen.cpp
 *    TVM-jitted code generation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_tvm_query_codegen.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * ATTENTION: Be sure to include jit_tvm_query.h before anything else because of libintl.h
 * See jit_tvm_query.h for more details.
 */
#include "jit_tvm_query.h"
#include "jit_tvm_query_codegen.h"
#include "jit_tvm_funcs.h"
#include "jit_tvm_blocks.h"
#include "storage/mot/jit_exec.h"
#include "jit_tvm_util.h"
#include "jit_util.h"

using namespace tvm;

namespace JitExec {
DECLARE_LOGGER(JitTvmQueryCodegen, JitExec)

static void DestroyCodeGenContext(JitTvmCodeGenContext* ctx);

/** @brief Initializes a context for compilation. */
static bool InitCodeGenContext(JitTvmCodeGenContext* ctx, Builder* builder, MOT::Table* table, MOT::Index* index,
    MOT::Table* inner_table = nullptr, MOT::Index* inner_index = nullptr)
{
    errno_t erc = memset_s(ctx, sizeof(JitTvmCodeGenContext), 0, sizeof(JitTvmCodeGenContext));
    securec_check(erc, "\0", "\0");
    ctx->_builder = builder;
    if (!InitTableInfo(&ctx->_table_info, table, index)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compile", "Failed to initialize table information for code-generation context");
        return false;
    }
    if (inner_table && !InitTableInfo(&ctx->m_innerTable_info, inner_table, inner_index)) {
        DestroyTableInfo(&ctx->_table_info);
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Compile",
            "Failed to initialize inner-scan table information for code-generation context");
        return false;
    }

    ctx->m_constCount = 0;
    size_t allocSize = sizeof(Const) * MOT_JIT_MAX_CONST;
    ctx->m_constValues = (Const*)MOT::MemSessionAlloc(allocSize);
    if (ctx->m_constValues == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Compile",
            "Failed to allocate %u bytes for constant array in code-generation context",
            allocSize);
        DestroyCodeGenContext(ctx);
        return false;
    }

    return true;
}

/** @brief Initializes a context for compilation. */
static bool InitCompoundCodeGenContext(
    JitTvmCodeGenContext* ctx, Builder* builder, MOT::Table* table, MOT::Index* index, JitCompoundPlan* plan)
{
    // initialize outer query table info
    errno_t erc = memset_s(ctx, sizeof(JitTvmCodeGenContext), 0, sizeof(JitTvmCodeGenContext));
    securec_check(erc, "\0", "\0");
    ctx->_builder = builder;
    if (!InitTableInfo(&ctx->_table_info, table, index)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compile", "Failed to initialize table information for code-generation context");
        return false;
    }
    ctx->m_subQueryCount = plan->_sub_query_count;
    ctx->m_subQueryTableInfo = (TableInfo*)MOT::MemSessionAlloc(sizeof(TableInfo) * ctx->m_subQueryCount);
    if (ctx->m_subQueryTableInfo == nullptr) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compile", "Failed to initialize table information for code-generation context");
        DestroyTableInfo(&ctx->_table_info);
        return false;
    }

    // initialize sub-query table info
    bool result = true;
    for (uint32_t i = 0; i < ctx->m_subQueryCount; ++i) {
        JitPlan* subPlan = plan->_sub_query_plans[i];
        MOT::Table* subTable = nullptr;
        MOT::Index* subIndex = nullptr;
        if (subPlan->_plan_type == JIT_PLAN_POINT_QUERY) {
            subTable = ((JitSelectPlan*)subPlan)->_query._table;
            subIndex = subTable->GetPrimaryIndex();
        } else if (subPlan->_plan_type == JIT_PLAN_RANGE_SCAN) {
            subTable = ((JitRangeSelectPlan*)subPlan)->_index_scan._table;
            subIndex = subTable->GetIndex(((JitRangeSelectPlan*)subPlan)->_index_scan._index_id);
        } else {
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "JIT Compile", "Invalid sub-plan %u type: %d", i, (int)subPlan->_plan_type);
            result = false;
        }
        if (result && !InitTableInfo(&ctx->m_subQueryTableInfo[i], subTable, subIndex)) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "JIT Compile",
                "Failed to initialize sub-query table information for code-generation context");
            result = false;
        }
        if (!result) {
            for (uint32_t j = 0; j < i; ++j) {
                DestroyTableInfo(&ctx->m_subQueryTableInfo[j]);
            }
            MOT::MemSessionFree(ctx->m_subQueryTableInfo);
            ctx->m_subQueryTableInfo = nullptr;
            DestroyTableInfo(&ctx->_table_info);
            return false;
        }
    }

    return true;
}

/** @brief Destroys a compilation context. */
static void DestroyCodeGenContext(JitTvmCodeGenContext* ctx)
{
    if (ctx != nullptr) {
        DestroyTableInfo(&ctx->_table_info);
        DestroyTableInfo(&ctx->m_innerTable_info);
        for (uint32_t i = 0; i < ctx->m_subQueryCount; ++i) {
            DestroyTableInfo(&ctx->m_subQueryTableInfo[i]);
        }
        if (ctx->m_constValues != nullptr) {
            MOT::MemSessionFree(ctx->m_constValues);
        }
    }
}

extern int AllocateConstId(JitTvmCodeGenContext* ctx, int type, Datum value, bool isNull)
{
    int res = -1;
    if (ctx->m_constCount == MOT_JIT_MAX_CONST) {
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_LIMIT,
            "JIT Compile",
            "Cannot allocate constant identifier, reached limit of %u",
            ctx->m_constCount);
    } else {
        res = ctx->m_constCount++;
        ctx->m_constValues[res].consttype = type;
        ctx->m_constValues[res].constvalue = value;
        ctx->m_constValues[res].constisnull = isNull;
    }
    return res;
}

static JitContext* FinalizeCodegen(JitTvmCodeGenContext* ctx, int max_arg, JitCommandType command_type)
{
    // do minimal verification and wrap up
    if (!ctx->m_jittedQuery->finalize()) {
        MOT_LOG_ERROR("Failed to generate jitted code for query: Failed to finalize jit function");
        ctx->m_jittedQuery->dump();
        delete ctx->m_jittedQuery;
        return nullptr;
    }

    // dump if requested
    if (IsMotCodegenPrintEnabled()) {
        ctx->m_jittedQuery->dump();
    }

    // verify function structure
    if (!ctx->m_jittedQuery->verify()) {
        MOT_LOG_TRACE("Failed to generate jitted code for query: Failed to verify jit function");
        delete ctx->m_jittedQuery;
        return nullptr;
    }

    // prepare global constant array
    JitDatumArray datumArray = {};
    if (ctx->m_constCount > 0) {
        if (!PrepareDatumArray(ctx->m_constValues, ctx->m_constCount, &datumArray)) {
            MOT_LOG_ERROR("Failed to generate jitted code for query: Failed to prepare constant datum array");
            delete ctx->m_jittedQuery;
            return nullptr;
        }
    }

    // that's it, we are ready
    JitContext* jit_context = AllocJitContext(JIT_CONTEXT_GLOBAL);
    if (jit_context == nullptr) {
        MOT_LOG_TRACE("Failed to allocate JIT context, aborting code generation");
        delete ctx->m_jittedQuery;
        return nullptr;
    }

    // setup execution details
    jit_context->m_table = ctx->_table_info.m_table;
    jit_context->m_index = ctx->_table_info.m_index;
    jit_context->m_indexId = jit_context->m_index->GetExtId();
    MOT_LOG_TRACE("Installed index id: %" PRIu64, jit_context->m_indexId);
    jit_context->m_tvmFunction = ctx->m_jittedQuery;
    jit_context->m_argCount = max_arg + 1;
    jit_context->m_innerTable = ctx->m_innerTable_info.m_table;
    jit_context->m_innerIndex = ctx->m_innerTable_info.m_index;
    if (jit_context->m_innerIndex != nullptr) {
        jit_context->m_innerIndexId = jit_context->m_innerIndex->GetExtId();
        MOT_LOG_TRACE("Installed inner index id: %" PRIu64, jit_context->m_innerIndexId);
    }
    jit_context->m_commandType = command_type;
    jit_context->m_subQueryCount = 0;
    jit_context->m_constDatums.m_datumCount = datumArray.m_datumCount;
    jit_context->m_constDatums.m_datums = datumArray.m_datums;

    return jit_context;
}

static JitContext* JitUpdateCodegen(const Query* query, const char* query_string, JitUpdatePlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT update at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_query._table;
    if (!InitCodeGenContext(&cg_ctx, &builder, table, table->GetPrimaryIndex())) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedUpdate", query_string);
    IssueDebugLog("Starting execution of jitted UPDATE");

    // update is not allowed if we reached soft memory limit
    buildIsSoftMemoryLimitReached(ctx);

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // begin the WHERE clause (this is a point query
    int max_arg = 0;
    if (!buildPointScan(ctx, &plan->_query._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for update query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // fetch row for writing
    MOT_LOG_DEBUG("Generating update code for point query");
    Instruction* row = buildSearchRow(ctx, MOT::AccessType::WR, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, &plan->_query._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for update query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // prepare a bitmap array
    IssueDebugLog("Resetting bitmap set for incremental redo");
    AddResetBitmapSet(ctx);

    // now begin updating columns
    IssueDebugLog("Updating row columns");
    if (!writeRowColumns(ctx, row, &plan->_update_exprs, &max_arg, true)) {
        MOT_LOG_TRACE("Failed to generate jitted code for update query: failed to process target entry");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // write row
    IssueDebugLog("Writing row");
    buildWriteRow(ctx, row, true, nullptr);

    // the next call will be executed only if the previous call to writeRow succeeded
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_UPDATE);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitRangeUpdateCodegen(const Query* query, const char* query_string, JitRangeUpdatePlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT range update at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_index_scan._table;
    if (!InitCodeGenContext(&cg_ctx, &builder, table, table->GetPrimaryIndex())) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedRangeUpdate", query_string);
    IssueDebugLog("Starting execution of jitted range UPDATE");

    // update is not allowed if we reached soft memory limit
    buildIsSoftMemoryLimitReached(ctx);

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // begin the WHERE clause
    int max_arg = 0;
    MOT_LOG_DEBUG("Generating range cursor for range UPDATE query");
    JitTvmRuntimeCursor cursor =
        buildRangeCursor(ctx, &plan->_index_scan, &max_arg, JIT_RANGE_SCAN_MAIN, JIT_INDEX_SCAN_FORWARD, nullptr);
    if (cursor.begin_itr == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for range UPDATE query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddResetBitmapSet(ctx);

    JIT_WHILE_BEGIN(cursor_loop)
    Instruction* res = AddIsScanEnd(ctx, JIT_INDEX_SCAN_FORWARD, &cursor, JIT_RANGE_SCAN_MAIN);
    JIT_WHILE_EVAL_NOT(res)
    Instruction* row = buildGetRowFromIterator(
        ctx, JIT_WHILE_POST_BLOCK(), MOT::AccessType::WR, JIT_INDEX_SCAN_FORWARD, &cursor, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, &plan->_index_scan._filters, &max_arg, JIT_WHILE_COND_BLOCK())) {
        MOT_LOG_TRACE("Failed to generate jitted code for range UPDATE query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // now begin updating columns
    IssueDebugLog("Updating row columns");
    if (!writeRowColumns(ctx, row, &plan->_update_exprs, &max_arg, true)) {
        MOT_LOG_TRACE("Failed to generate jitted code for update query: failed to process target entry");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // write row
    IssueDebugLog("Writing row");
    buildWriteRow(ctx, row, false, &cursor);

    // the next call will be executed only if the previous call to writeRow succeeded
    buildIncrementRowsProcessed(ctx);

    // reset bitmap for next loop
    AddResetBitmapSet(ctx);
    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of range update loop");
    AddDestroyCursor(ctx, &cursor);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_RANGE_UPDATE);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitInsertCodegen(const Query* query, const char* query_string, JitInsertPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT insert at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_table;
    if (!InitCodeGenContext(&cg_ctx, &builder, table, table->GetPrimaryIndex())) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedInsert", query_string);
    IssueDebugLog("Starting execution of jitted INSERT");

    // insert is not allowed if we reached soft memory limit
    buildIsSoftMemoryLimitReached(ctx);

    // create new row and bitmap set
    Instruction* row = buildCreateNewRow(ctx);

    // set row null bits
    IssueDebugLog("Setting row null bits before insert");
    AddSetRowNullBits(ctx, row);

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    IssueDebugLog("Setting row columns");
    int max_arg = 0;
    if (!writeRowColumns(ctx, row, &plan->_insert_exprs, &max_arg, false)) {
        MOT_LOG_TRACE("Failed to generate jitted code for insert query: failed to process target entry");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    IssueDebugLog("Inserting row");
    buildInsertRow(ctx, row);

    // the next call will be executed only if the previous call to writeRow succeeded
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_INSERT);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitDeleteCodegen(const Query* query, const char* query_string, JitDeletePlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT delete at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_query._table;
    if (!InitCodeGenContext(&cg_ctx, &builder, table, table->GetPrimaryIndex())) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedDelete", query_string);
    IssueDebugLog("Starting execution of jitted DELETE");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // begin the WHERE clause
    int max_arg = 0;
    if (!buildPointScan(ctx, &plan->_query._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for DELETE query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // fetch row for delete
    Instruction* row = buildSearchRow(ctx, MOT::AccessType::DEL, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, &plan->_query._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for DELETE query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // delete row
    IssueDebugLog("Deleting row");
    buildDeleteRow(
        ctx);  // row is already cached in concurrency control module, so we do not need to provide an argument

    // the next call will be executed only if the previous call to deleteRow succeeded
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_DELETE);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitSelectCodegen(const Query* query, const char* query_string, JitSelectPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT select at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_query._table;
    if (!InitCodeGenContext(&cg_ctx, &builder, table, table->GetPrimaryIndex())) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedSelect", query_string);
    IssueDebugLog("Starting execution of jitted SELECT");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // begin the WHERE clause
    int max_arg = 0;
    if (!buildPointScan(ctx, &plan->_query._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for SELECT query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // fetch row for read
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    Instruction* row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, &plan->_query._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for SELECT query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // now begin selecting columns into result
    IssueDebugLog("Selecting columns into result");
    if (!selectRowColumns(ctx, row, &plan->_select_exprs, &max_arg, JIT_RANGE_SCAN_MAIN)) {
        MOT_LOG_TRACE("Failed to generate jitted code for insert query: failed to process target entry");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);

    // update number of rows processed
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended (this is a point query)
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_SELECT);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static void AddCleanupOldScan(JitTvmCodeGenContext* ctx)
{
    // emit code to cleanup previous scan in case this is a new scan
    JIT_IF_BEGIN(cleanup_old_scan)
    Instruction* isNewScan = AddIsNewScan(ctx);
    JIT_IF_EVAL(isNewScan)
    AddDestroyStateIterators(ctx, JIT_RANGE_SCAN_MAIN);
    AddDestroyStateIterators(ctx, JIT_RANGE_SCAN_INNER);
    // sub-query does not have a stateful execution, so no need to cleanup
    JIT_IF_END()
}

/** @brief Generates code for range SELECT query with a possible LIMIT clause. */
static JitContext* JitRangeSelectCodegen(const Query* query, const char* query_string, JitRangeSelectPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT select at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_index_scan._table;
    int index_id = plan->_index_scan._index_id;
    if (!InitCodeGenContext(&cg_ctx, &builder, table, table->GetIndex(index_id))) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedRangeSelect", query_string);
    IssueDebugLog("Starting execution of jitted range SELECT");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // emit code to cleanup previous scan in case this is a new scan
    AddCleanupOldScan(ctx);

    // prepare stateful scan if not done so already, if no row exists then emit code to return from function
    int max_arg = 0;
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    Instruction* row = buildPrepareStateScanRow(
        ctx, &plan->_index_scan, JIT_RANGE_SCAN_MAIN, access_mode, &max_arg, nullptr, nullptr, nullptr);
    if (row == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for range select query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // select inner and outer row expressions into result tuple (no aggregate because aggregate is not stateful)
    IssueDebugLog("Retrieved row from state iterator, beginning to select columns into result tuple");
    if (!selectRowColumns(ctx, row, &plan->_select_exprs, &max_arg, JIT_RANGE_SCAN_MAIN)) {
        MOT_LOG_TRACE("Failed to generate jitted code for range SELECT query: failed to select row expressions");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }
    AddExecStoreVirtualTuple(ctx);
    buildIncrementRowsProcessed(ctx);

    // make sure that next iteration find an empty state row, so scan makes a progress
    AddResetStateRow(ctx, JIT_RANGE_SCAN_MAIN);

    // if a limit clause exists, then increment limit counter and check if reached limit
    buildCheckLimit(ctx, plan->_limit_count);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitCommandType cmdType =
        (plan->_index_scan._scan_type == JIT_INDEX_SCAN_FULL) ? JIT_COMMAND_FULL_SELECT : JIT_COMMAND_RANGE_SELECT;
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, cmdType);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range SELECT query with aggregator. */
static JitContext* JitAggregateRangeSelectCodegen(
    const Query* query, const char* query_string, JitRangeSelectPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT aggregate range select at thread %p", (void*)pthread_self());

    Builder builder;
    MOT::Table* table = plan->_index_scan._table;
    int index_id = plan->_index_scan._index_id;
    JitTvmCodeGenContext cg_ctx = {0};
    if (!InitCodeGenContext(&cg_ctx, &builder, table, table->GetIndex(index_id))) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedAggregateRangeSelect", query_string);
    IssueDebugLog("Starting execution of jitted aggregate range SELECT");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
    AddExecClearTuple(ctx);

    // prepare for aggregation
    if (!prepareAggregate(ctx, &plan->_aggregate)) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range SELECT query: failed to prepare aggregate");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddResetStateLimitCounter(ctx);

    // pay attention: aggregated range scan is not stateful, since we scan all tuples in one call
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;

    // begin the WHERE clause
    int max_arg = 0;
    JitIndexScanDirection index_scan_direction = JIT_INDEX_SCAN_FORWARD;

    // build range iterators
    MOT_LOG_DEBUG("Generating range cursor for range SELECT query");
    JitTvmRuntimeCursor cursor =
        buildRangeCursor(ctx, &plan->_index_scan, &max_arg, JIT_RANGE_SCAN_MAIN, index_scan_direction, nullptr);
    if (cursor.begin_itr == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range SELECT query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    JIT_WHILE_BEGIN(cursor_aggregate_loop)
    Instruction* res = AddIsScanEnd(ctx, index_scan_direction, &cursor, JIT_RANGE_SCAN_MAIN);
    JIT_WHILE_EVAL_NOT(res)
    Instruction* row = buildGetRowFromIterator(
        ctx, JIT_WHILE_POST_BLOCK(), access_mode, JIT_INDEX_SCAN_FORWARD, &cursor, JIT_RANGE_SCAN_MAIN);

    // check for additional filters, if not try to fetch next row
    if (!buildFilterRow(ctx, row, &plan->_index_scan._filters, &max_arg, JIT_WHILE_COND_BLOCK())) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range SELECT query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // aggregate into tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
    // if row disqualified due to DISTINCT operator then go back to loop test block
    if (!buildAggregateRow(ctx, &plan->_aggregate, row, JIT_WHILE_COND_BLOCK())) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range SELECT query: unsupported aggregate");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // if a limit clause exists, then increment limit counter and check if reached limit
    if (plan->_limit_count > 0) {
        AddIncrementStateLimitCounter(ctx);
        JIT_IF_BEGIN(limit_count_reached)
        Instruction* current_limit_count = AddGetStateLimitCounter(ctx);
        JIT_IF_EVAL_CMP(current_limit_count, JIT_CONST(plan->_limit_count), JIT_ICMP_EQ);
        IssueDebugLog("Reached limit specified in limit clause, raising internal state scan end flag");
        JIT_WHILE_BREAK()  // break from loop
        JIT_IF_END()
    }
    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of aggregate range select loop");
    AddDestroyCursor(ctx, &cursor);

    // wrap up aggregation and write to result tuple
    buildAggregateResult(ctx, &plan->_aggregate);

    // store the result tuple
    AddExecStoreVirtualTuple(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended (this is an aggregate loop)
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_AGGREGATE_RANGE_SELECT);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitPointJoinCodegen(const Query* query, const char* query_string, JitJoinPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT Point JOIN query at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* outer_table = plan->_outer_scan._table;
    MOT::Index* outer_index = outer_table->GetIndex(plan->_outer_scan._index_id);
    MOT::Table* inner_table = plan->_inner_scan._table;
    MOT::Index* inner_index = inner_table->GetIndex(plan->_inner_scan._index_id);
    if (!InitCodeGenContext(&cg_ctx, &builder, outer_table, outer_index, inner_table, inner_index)) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedPointJoin", query_string);
    IssueDebugLog("Starting execution of jitted Point JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // search the outer row
    int max_arg = 0;
    if (!buildPointScan(ctx, &plan->_outer_scan._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Point JOIN query: unsupported outer WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // fetch row for read
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    Instruction* outer_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, outer_row, &plan->_outer_scan._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Point JOIN query: unsupported outer scan filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // before we move on to inner point scan, we save the outer row in a safe copy (but for that purpose we need to save
    // row in outer scan state)
    AddSetStateRow(ctx, outer_row, JIT_RANGE_SCAN_MAIN);
    AddCopyOuterStateRow(ctx);

    // now search the inner row
    if (!buildPointScan(ctx, &plan->_inner_scan._search_exprs, &max_arg, JIT_RANGE_SCAN_INNER, outer_row)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Point JOIN query: unsupported inner WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }
    Instruction* inner_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_INNER);

    // check for additional filters
    if (!buildFilterRow(ctx, inner_row, &plan->_inner_scan._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Point JOIN query: unsupported inner scan filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    Instruction* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now begin selecting columns into result
    if (!selectJoinRows(ctx, outer_row_copy, inner_row, plan, &max_arg)) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for Point JOIN query: failed to select row columns into result tuple");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);

    // update number of rows processed
    buildIncrementRowsProcessed(ctx);

    // generate code for setting output parameter tp_processed value to rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended (this is a point query)
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_POINT_JOIN);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range JOIN query with outer point query and a possible LIMIT clause but without
 * aggregation. */
static JitContext* JitPointOuterJoinCodegen(const Query* query, const char* query_string, JitJoinPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT Outer Point JOIN query at thread %p", (void*)pthread_self());

    int max_arg = 0;
    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* outer_table = plan->_outer_scan._table;
    MOT::Index* outer_index = outer_table->GetIndex(plan->_outer_scan._index_id);
    MOT::Table* inner_table = plan->_inner_scan._table;
    MOT::Index* inner_index = inner_table->GetIndex(plan->_inner_scan._index_id);
    if (!InitCodeGenContext(&cg_ctx, &builder, outer_table, outer_index, inner_table, inner_index)) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedOuterPointJoin", query_string);
    IssueDebugLog("Starting execution of jitted Outer Point JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // emit code to cleanup previous scan in case this is a new scan
    AddCleanupOldScan(ctx);

    // we first check if outer state row was already searched
    Instruction* outer_row_copy = AddGetOuterStateRowCopy(ctx);
    JIT_IF_BEGIN(check_outer_row_ready)
    JIT_IF_EVAL_NOT(outer_row_copy)
    // search the outer row
    if (!buildPointScan(ctx, &plan->_outer_scan._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Outer Point JOIN query: unsupported outer WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // fetch row for read
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    Instruction* outer_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, outer_row, &plan->_outer_scan._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Outer Point JOIN query: unsupported filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // before we move on to inner range scan, we save the outer row in a safe copy (but for that purpose we need to save
    // row in outer scan state)
    AddSetStateRow(ctx, outer_row, JIT_RANGE_SCAN_MAIN);
    AddCopyOuterStateRow(ctx);
    outer_row_copy = AddGetOuterStateRowCopy(ctx);  // must get copy again, otherwise it is null
    JIT_IF_END()

    // now prepare inner scan if needed, if no row was found then emit code to return from function (since outer scan is
    // a point query)
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    Instruction* inner_row = buildPrepareStateScanRow(
        ctx, &plan->_inner_scan, JIT_RANGE_SCAN_INNER, access_mode, &max_arg, outer_row_copy, nullptr, nullptr);
    if (inner_row == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for Outer Point JOIN query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now begin selecting columns into result
    if (!selectJoinRows(ctx, outer_row_copy, inner_row, plan, &max_arg)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Outer Point JOIN query: failed to select row columns into "
                      "result tuple");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);

    // update number of rows processed
    buildIncrementRowsProcessed(ctx);

    // clear inner row for next iteration
    AddResetStateRow(ctx, JIT_RANGE_SCAN_INNER);

    // if a limit clause exists, then increment limit counter and check if reached limit
    buildCheckLimit(ctx, plan->_limit_count);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_RANGE_JOIN);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range JOIN query with inner point query and a possible LIMIT clause but without
 * aggregation. */
static JitContext* JitPointInnerJoinCodegen(const Query* query, const char* query_string, JitJoinPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT Inner Point JOIN at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* outer_table = plan->_outer_scan._table;
    MOT::Index* outer_index = outer_table->GetIndex(plan->_outer_scan._index_id);
    MOT::Table* inner_table = plan->_inner_scan._table;
    MOT::Index* inner_index = inner_table->GetIndex(plan->_inner_scan._index_id);
    if (!InitCodeGenContext(&cg_ctx, &builder, outer_table, outer_index, inner_table, inner_index)) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedInnerPointJoin", query_string);
    IssueDebugLog("Starting execution of jitted inner point JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // emit code to cleanup previous scan in case this is a new scan
    AddCleanupOldScan(ctx);

    // prepare stateful scan if not done so already, if row not found then emit code to return from function (since this
    // is an outer scan)
    int max_arg = 0;
    BasicBlock* fetch_outer_row_bb = nullptr;
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    Instruction* outer_row = buildPrepareStateScanRow(
        ctx, &plan->_outer_scan, JIT_RANGE_SCAN_MAIN, access_mode, &max_arg, nullptr, nullptr, &fetch_outer_row_bb);
    if (outer_row == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for Inner Point JOIN query: unsupported WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // before we move on to inner scan, we save the outer row in a safe copy
    AddCopyOuterStateRow(ctx);

    // now search the inner row
    if (!buildPointScan(ctx, &plan->_inner_scan._search_exprs, &max_arg, JIT_RANGE_SCAN_INNER, outer_row)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Inner Point JOIN query: unsupported inner WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }
    Instruction* inner_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_INNER);

    // check for additional filters
    if (!buildFilterRow(ctx, inner_row, &plan->_inner_scan._filters, &max_arg, fetch_outer_row_bb)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Inner Point JOIN query: unsupported inner scan filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    Instruction* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now begin selecting columns into result
    if (!selectJoinRows(ctx, outer_row_copy, inner_row, plan, &max_arg)) {
        MOT_LOG_TRACE("Failed to generate jitted code for Inner Point JOIN query: failed to select row columns into "
                      "result tuple");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);
    buildIncrementRowsProcessed(ctx);

    // make sure that next iteration find an empty state row, so scan makes a progress
    AddResetStateRow(ctx, JIT_RANGE_SCAN_MAIN);

    // if a limit clause exists, then increment limit counter and check if reached limit
    buildCheckLimit(ctx, plan->_limit_count);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_RANGE_JOIN);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range JOIN query with a possible LIMIT clause but without aggregation. */
static JitContext* JitRangeJoinCodegen(const Query* query, const char* query_string, JitJoinPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT range JOIN at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* outer_table = plan->_outer_scan._table;
    MOT::Index* outer_index = outer_table->GetIndex(plan->_outer_scan._index_id);
    MOT::Table* inner_table = plan->_inner_scan._table;
    MOT::Index* inner_index = inner_table->GetIndex(plan->_inner_scan._index_id);
    if (!InitCodeGenContext(&cg_ctx, &builder, outer_table, outer_index, inner_table, inner_index)) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedRangeJoin", query_string);
    IssueDebugLog("Starting execution of jitted range JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // emit code to cleanup previous scan in case this is a new scan
    AddCleanupOldScan(ctx);

    // prepare stateful scan if not done so already
    int max_arg = 0;
    BasicBlock* fetch_outer_row_bb = nullptr;
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    Instruction* outer_row = buildPrepareStateScanRow(
        ctx, &plan->_outer_scan, JIT_RANGE_SCAN_MAIN, access_mode, &max_arg, nullptr, nullptr, &fetch_outer_row_bb);
    if (outer_row == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for Range JOIN query: unsupported outer WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // before we move on to inner scan, we save the outer row in a safe copy
    AddCopyOuterStateRow(ctx);

    // now prepare inner scan if needed
    Instruction* inner_row = buildPrepareStateScanRow(
        ctx, &plan->_inner_scan, JIT_RANGE_SCAN_INNER, access_mode, &max_arg, outer_row, fetch_outer_row_bb, nullptr);
    if (inner_row == nullptr) {
        MOT_LOG_TRACE("Failed to generate jitted code for Range JOIN query: unsupported inner WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    Instruction* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now begin selecting columns into result
    if (!selectJoinRows(ctx, outer_row_copy, inner_row, plan, &max_arg)) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for Range JOIN query: failed to select row columns into result tuple");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);
    buildIncrementRowsProcessed(ctx);

    // clear inner row for next iteration
    AddResetStateRow(ctx, JIT_RANGE_SCAN_INNER);

    // if a limit clause exists, then increment limit counter and check if reached limit
    buildCheckLimit(ctx, plan->_limit_count);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, max_arg, JIT_COMMAND_RANGE_JOIN);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

/** @brief Generates code for range JOIN query with an aggregator. */
static JitContext* JitAggregateRangeJoinCodegen(const Query* query, const char* query_string, JitJoinPlan* plan)
{
    MOT_LOG_DEBUG("Generating code for MOT aggregate range JOIN at thread %p", (void*)pthread_self());

    Builder builder;

    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* outer_table = plan->_outer_scan._table;
    MOT::Index* outer_index = outer_table->GetIndex(plan->_outer_scan._index_id);
    MOT::Table* inner_table = plan->_inner_scan._table;
    MOT::Index* inner_index = inner_table->GetIndex(plan->_inner_scan._index_id);
    if (!InitCodeGenContext(&cg_ctx, &builder, outer_table, outer_index, inner_table, inner_index)) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedAggregateRangeJoin", query_string);
    IssueDebugLog("Starting execution of jitted aggregate range JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
    AddExecClearTuple(ctx);

    // prepare for aggregation
    if (!prepareAggregate(ctx, &plan->_aggregate)) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range JOIN query: failed to prepare aggregate");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    AddResetStateLimitCounter(ctx);

    // pay attention: aggregated range scan is not stateful, since we scan all tuples in one call
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;

    // begin the WHERE clause
    int maxArg = 0;

    // build range iterators
    MOT_LOG_DEBUG("Generating outer loop cursor for range JOIN query");
    JitTvmRuntimeCursor outer_cursor =
        buildRangeCursor(ctx, &plan->_outer_scan, &maxArg, JIT_RANGE_SCAN_MAIN, JIT_INDEX_SCAN_FORWARD, nullptr);
    if (outer_cursor.begin_itr == nullptr) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for aggregate range JOIN query: unsupported outer-loop WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    JIT_WHILE_BEGIN(cursor_aggregate_outer_loop)
    BasicBlock* endOuterLoopBlock = JIT_WHILE_POST_BLOCK();
    Instruction* res = AddIsScanEnd(ctx, JIT_INDEX_SCAN_FORWARD, &outer_cursor, JIT_RANGE_SCAN_MAIN);
    JIT_WHILE_EVAL_NOT(res)
    Instruction* outer_row = buildGetRowFromIterator(
        ctx, JIT_WHILE_POST_BLOCK(), access_mode, JIT_INDEX_SCAN_FORWARD, &outer_cursor, JIT_RANGE_SCAN_MAIN);

    // check for additional filters, if not try to fetch next row
    if (!buildFilterRow(ctx, outer_row, &plan->_outer_scan._filters, &maxArg, JIT_WHILE_COND_BLOCK())) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range JOIN query: unsupported outer-loop filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // before we move on to inner scan, we save the outer row in a safe copy (but for that purpose we need to save row
    // in outer scan state)
    AddSetStateRow(ctx, outer_row, JIT_RANGE_SCAN_MAIN);
    AddCopyOuterStateRow(ctx);

    // now build the inner loop
    MOT_LOG_DEBUG("Generating inner loop cursor for range JOIN query");
    JitTvmRuntimeCursor inner_cursor =
        buildRangeCursor(ctx, &plan->_inner_scan, &maxArg, JIT_RANGE_SCAN_INNER, JIT_INDEX_SCAN_FORWARD, outer_row);
    if (inner_cursor.begin_itr == nullptr) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for aggregate range JOIN query: unsupported inner-loop WHERE clause type");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    JIT_WHILE_BEGIN(cursor_aggregate_inner_loop)
    Instruction* res_inner = AddIsScanEnd(ctx, JIT_INDEX_SCAN_FORWARD, &inner_cursor, JIT_RANGE_SCAN_INNER);
    JIT_WHILE_EVAL_NOT(res_inner)
    Instruction* inner_row = buildGetRowFromIterator(
        ctx, JIT_WHILE_POST_BLOCK(), access_mode, JIT_INDEX_SCAN_FORWARD, &inner_cursor, JIT_RANGE_SCAN_INNER);

    // check for additional filters, if not try to fetch next row
    if (!buildFilterRow(ctx, inner_row, &plan->_inner_scan._filters, &maxArg, JIT_WHILE_COND_BLOCK())) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range JOIN query: unsupported inner-loop filter");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // aggregate into tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
    // find out to which table the aggreate expression refers, and aggregate it
    // if row disqualified due to DISTINCT operator then go back to inner loop test block
    bool aggRes = false;
    if (plan->_aggregate._table == ctx->m_innerTable_info.m_table) {
        aggRes = buildAggregateRow(ctx, &plan->_aggregate, inner_row, JIT_WHILE_COND_BLOCK());
    } else {
        // retrieve the safe copy of the outer row
        Instruction* outer_row_copy = AddGetOuterStateRowCopy(ctx);
        aggRes = buildAggregateRow(ctx, &plan->_aggregate, outer_row_copy, JIT_WHILE_COND_BLOCK());
    }

    if (!aggRes) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range JOIN query: unsupported aggregate");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // if a limit clause exists, then increment limit counter and check if reached limit
    if (plan->_limit_count > 0) {
        AddIncrementStateLimitCounter(ctx);
        JIT_IF_BEGIN(limit_count_reached)
        Instruction* currentLimitCount = AddGetStateLimitCounter(ctx);
        JIT_IF_EVAL_CMP(currentLimitCount, JIT_CONST(plan->_limit_count), JIT_ICMP_EQ);
        IssueDebugLog("Reached limit specified in limit clause, raising internal state scan end flag");
        AddDestroyCursor(ctx, &outer_cursor);
        AddDestroyCursor(ctx, &inner_cursor);
        ctx->_builder->CreateBr(endOuterLoopBlock);  // break from inner outside of outer loop
        JIT_IF_END()
    }
    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of inner loop");
    AddDestroyCursor(ctx, &inner_cursor);

    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of outer loop");
    AddDestroyCursor(ctx, &outer_cursor);

    // wrap up aggregation and write to result tuple
    buildAggregateResult(ctx, &plan->_aggregate);

    // store the result tuple
    AddExecStoreVirtualTuple(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    builder.CreateRet(builder.CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    JitContext* jit_context = FinalizeCodegen(ctx, maxArg, JIT_COMMAND_AGGREGATE_JOIN);

    // cleanup
    DestroyCodeGenContext(ctx);

    return jit_context;
}

static JitContext* JitJoinCodegen(Query* query, const char* query_string, JitJoinPlan* plan)
{
    JitContext* jit_context = nullptr;

    if (plan->_aggregate._aggreaget_op == JIT_AGGREGATE_NONE) {
        switch (plan->_scan_type) {
            case JIT_JOIN_SCAN_POINT:
                // special case: this is really a point query
                jit_context = JitPointJoinCodegen(query, query_string, plan);
                break;

            case JIT_JOIN_SCAN_OUTER_POINT:
                // special case: outer scan is really a point query
                jit_context = JitPointOuterJoinCodegen(query, query_string, plan);
                break;

            case JIT_JOIN_SCAN_INNER_POINT:
                // special case: inner scan is really a point query
                jit_context = JitPointInnerJoinCodegen(query, query_string, plan);
                break;

            case JIT_JOIN_SCAN_RANGE:
                jit_context = JitRangeJoinCodegen(query, query_string, plan);
                break;

            default:
                MOT_LOG_TRACE(
                    "Cannot generate jitteed code for JOIN plan: Invalid JOIN scan type %d", (int)plan->_scan_type);
                break;
        }
    } else {
        jit_context = JitAggregateRangeJoinCodegen(query, query_string, plan);
    }

    return jit_context;
}

static bool JitSubSelectCodegen(JitTvmCodeGenContext* ctx, JitCompoundPlan* plan, int subQueryIndex)
{
    MOT_LOG_DEBUG("Generating code for MOT sub-select at thread %p", (intptr_t)pthread_self());
    IssueDebugLog("Executing simple SELECT sub-query");

    // get the sub-query plan
    JitSelectPlan* subPlan = (JitSelectPlan*)plan->_sub_query_plans[subQueryIndex];

    // begin the WHERE clause
    int maxArg = 0;
    if (!buildPointScan(
            ctx, &subPlan->_query._search_exprs, &maxArg, JIT_RANGE_SCAN_SUB_QUERY, nullptr, -1, subQueryIndex)) {
        MOT_LOG_TRACE("Failed to generate jitted code for SELECT sub-query: unsupported WHERE clause type");
        return false;
    }

    // fetch row for read
    Instruction* row = buildSearchRow(ctx, MOT::AccessType::RD, JIT_RANGE_SCAN_SUB_QUERY, subQueryIndex);

    // check for additional filters
    if (!buildFilterRow(ctx, row, &subPlan->_query._filters, &maxArg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for SELECT sub-query: unsupported filter");
        return false;
    }

    // now begin selecting columns into result
    IssueDebugLog("Selecting column into result");
    if (!selectRowColumns(ctx, row, &subPlan->_select_exprs, &maxArg, JIT_RANGE_SCAN_SUB_QUERY, subQueryIndex)) {
        MOT_LOG_TRACE("Failed to generate jitted code for SELECT sub-query: failed to process target entry");
        return false;
    }

    return true;
}

/** @brief Generates code for range SELECT sub-query with aggregator. */
static bool JitSubAggregateRangeSelectCodegen(JitTvmCodeGenContext* ctx, JitCompoundPlan* plan, int subQueryIndex)
{
    MOT_LOG_DEBUG("Generating code for MOT aggregate range select sub-query at thread %p", (intptr_t)pthread_self());
    IssueDebugLog("Executing aggregated range select sub-query");

    // get the sub-query plan
    JitRangeSelectPlan* subPlan = (JitRangeSelectPlan*)plan->_sub_query_plans[subQueryIndex];

    // prepare for aggregation
    if (!prepareAggregate(ctx, &subPlan->_aggregate)) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for aggregate range SELECT sub-query: failed to prepare aggregate");
        return false;
    }

    AddResetStateLimitCounter(ctx);

    // pay attention: aggregated range scan is not stateful, since we scan all tuples in one call
    MOT::AccessType accessMode = MOT::AccessType::RD;

    // begin the WHERE clause
    int maxArg = 0;
    JitIndexScanDirection index_scan_direction = JIT_INDEX_SCAN_FORWARD;

    // build range iterators
    MOT_LOG_DEBUG("Generating range cursor for range SELECT sub-query");
    JitTvmRuntimeCursor cursor = buildRangeCursor(
        ctx, &subPlan->_index_scan, &maxArg, JIT_RANGE_SCAN_SUB_QUERY, index_scan_direction, nullptr, subQueryIndex);
    if (cursor.begin_itr == nullptr) {
        MOT_LOG_TRACE(
            "Failed to generate jitted code for aggregate range SELECT sub-query: unsupported WHERE clause type");
        return false;
    }

    JIT_WHILE_BEGIN(cursor_aggregate_loop)
    Instruction* res = AddIsScanEnd(ctx, index_scan_direction, &cursor, JIT_RANGE_SCAN_SUB_QUERY, subQueryIndex);
    JIT_WHILE_EVAL_NOT(res)
    Instruction* row = buildGetRowFromIterator(ctx,
        JIT_WHILE_POST_BLOCK(),
        accessMode,
        JIT_INDEX_SCAN_FORWARD,
        &cursor,
        JIT_RANGE_SCAN_SUB_QUERY,
        subQueryIndex);

    // check for additional filters, if not try to fetch next row
    if (!buildFilterRow(ctx, row, &subPlan->_index_scan._filters, &maxArg, JIT_WHILE_COND_BLOCK())) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range SELECT sub-query: unsupported filter");
        return false;
    }

    // aggregate into tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
    // if row disqualified due to DISTINCT operator then go back to loop test block
    if (!buildAggregateRow(ctx, &subPlan->_aggregate, row, JIT_WHILE_COND_BLOCK())) {
        MOT_LOG_TRACE("Failed to generate jitted code for aggregate range SELECT sub-query: unsupported aggregate");
        return false;
    }

    // if a limit clause exists, then increment limit counter and check if reached limit
    if (subPlan->_limit_count > 0) {
        AddIncrementStateLimitCounter(ctx);
        JIT_IF_BEGIN(limit_count_reached)
        Instruction* current_limit_count = AddGetStateLimitCounter(ctx);
        JIT_IF_EVAL_CMP(current_limit_count, JIT_CONST(subPlan->_limit_count), JIT_ICMP_EQ);
        IssueDebugLog("Reached limit specified in limit clause, raising internal state scan end flag");
        JIT_WHILE_BREAK()  // break from loop
        JIT_IF_END()
    }
    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of aggregate range select sub-query loop");
    AddDestroyCursor(ctx, &cursor);

    // wrap up aggregation and write to result tuple (even though this is unfitting to outer query tuple...)
    buildAggregateResult(ctx, &subPlan->_aggregate);

    // coy aggregate result from outer query result tuple into sub-query result tuple
    AddCopyAggregateToSubQueryResult(ctx, subQueryIndex);

    return true;
}

static bool JitSubQueryCodeGen(JitTvmCodeGenContext* ctx, JitCompoundPlan* plan, int subQueryIndex)
{
    bool result = false;
    JitPlan* subPlan = plan->_sub_query_plans[subQueryIndex];
    if (subPlan->_plan_type == JIT_PLAN_POINT_QUERY) {
        result = JitSubSelectCodegen(ctx, plan, subQueryIndex);
    } else if (subPlan->_plan_type == JIT_PLAN_RANGE_SCAN) {
        result = JitSubAggregateRangeSelectCodegen(ctx, plan, subQueryIndex);
    } else {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
            "Generate JIT Code",
            "Cannot generate JIT code for sub-query plan: Invalid plan type %d",
            (int)subPlan->_plan_type);
    }
    return result;
}

static JitContext* JitCompoundOuterSelectCodegen(
    JitTvmCodeGenContext* ctx, Query* query, const char* query_string, JitSelectPlan* plan)
{
    // begin the WHERE clause
    int max_arg = 0;
    if (!buildPointScan(ctx, &plan->_query._search_exprs, &max_arg, JIT_RANGE_SCAN_MAIN, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for COMPOUND SELECT query: unsupported WHERE clause type");
        return nullptr;
    }

    // fetch row for read
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    Instruction* row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, &plan->_query._filters, &max_arg, nullptr)) {
        MOT_LOG_TRACE("Failed to generate jitted code for COMPOUND SELECT query: unsupported filter");
        return nullptr;
    }

    // now begin selecting columns into result
    IssueDebugLog("Selecting columns into result");
    if (!selectRowColumns(ctx, row, &plan->_select_exprs, &max_arg, JIT_RANGE_SCAN_MAIN)) {
        MOT_LOG_TRACE("Failed to generate jitted code for COMPOUND SELECT query: failed to process target entry");
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);

    // update number of rows processed
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended (this is a point query)
    AddSetScanEnded(ctx, 1);

    // return success from calling function
    ctx->_builder->CreateRet(ctx->_builder->CreateConst((uint64_t)MOT::RC_OK));

    // wrap up
    return FinalizeCodegen(ctx, max_arg, JIT_COMMAND_COMPOUND_SELECT);
}

static JitContext* JitCompoundOuterCodegen(
    JitTvmCodeGenContext* ctx, Query* query, const char* queryString, JitCompoundPlan* plan)
{
    JitContext* jitContext = nullptr;
    if (plan->_command_type == JIT_COMMAND_SELECT) {
        jitContext = JitCompoundOuterSelectCodegen(ctx, query, queryString, (JitSelectPlan*)plan->_outer_query_plan);
    }
    // currently other outer query types are not supported
    return jitContext;
}

static JitContext* JitCompoundCodegen(Query* query, const char* query_string, JitCompoundPlan* plan)
{
    // a compound query plan contains one or more sub-queries that evaluate to a datum that next needs to be fed as a
    // parameter to the outer query. We are currently imposing the following limitations:
    // 1. one sub-query that can only be a MAX aggregate
    // 2. outer query must be a simple point select query.
    //
    // our main strategy is as follows (based on the fact that each sub-query evaluates into a single value)
    // 1. for each sub-query:
    //  1.1 execute sub-query and put datum result in sub-query result slot, according to sub-query index
    // 2. execute the outer query as a simple query
    // 3. whenever we encounter a sub-link expression, it is evaluated as an expression that reads the pre-computed
    //    sub-query result in step 1.1, according to sub-query index
    MOT_LOG_DEBUG("Generating code for MOT compound select at thread %p", (intptr_t)pthread_self());

    // prepare code generation context
    Builder builder;
    JitTvmCodeGenContext cg_ctx = {0};
    MOT::Table* table = plan->_outer_query_plan->_query._table;
    if (!InitCompoundCodeGenContext(&cg_ctx, &builder, table, table->GetPrimaryIndex(), plan)) {
        return nullptr;
    }
    JitTvmCodeGenContext* ctx = &cg_ctx;

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedCompoundSelect", query_string);
    IssueDebugLog("Starting execution of jitted COMPOUND SELECT");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // generate code for sub-query execution
    uint32_t subQueryCount = 0;
    for (int i = 0; i < plan->_outer_query_plan->_query._search_exprs._count; ++i) {
        if (plan->_outer_query_plan->_query._search_exprs._exprs[i]._expr->_expr_type == JIT_EXPR_TYPE_SUBLINK) {
            JitSubLinkExpr* subLinkExpr =
                (JitSubLinkExpr*)plan->_outer_query_plan->_query._search_exprs._exprs[i]._expr;
            if (!JitSubQueryCodeGen(ctx, plan, subLinkExpr->_sub_query_index)) {
                MOT_LOG_TRACE(
                    "Failed to generate jitted code for COMPOUND SELECT query: Failed to generate code for sub-query");
                DestroyCodeGenContext(ctx);
                return nullptr;
            }
            ++subQueryCount;
        }
    }

    // clear tuple early, so that we will have a null datum in case outer query finds nothing
    AddExecClearTuple(ctx);

    // generate code for the outer query
    JitContext* jitContext = JitCompoundOuterCodegen(ctx, query, query_string, plan);
    if (jitContext == nullptr) {
        MOT_LOG_TRACE("Failed to generate code for outer query in compound select");
        DestroyCodeGenContext(ctx);
        return nullptr;
    }

    // prepare sub-query data in resulting JIT context (for later execution)
    MOT_ASSERT(subQueryCount > 0);
    MOT_ASSERT(subQueryCount == plan->_sub_query_count);
    if ((subQueryCount > 0) && !PrepareSubQueryData(jitContext, plan)) {
        MOT_LOG_TRACE("Failed to prepare tuple table slot array for sub-queries in JIT context object");
        DestroyJitContext(jitContext);
        jitContext = nullptr;
    }

    // cleanup
    DestroyCodeGenContext(ctx);

    return jitContext;
}

static JitContext* JitRangeScanCodegen(const Query* query, const char* query_string, JitRangeScanPlan* plan)
{
    JitContext* jit_context = nullptr;

    switch (plan->_command_type) {
        case JIT_COMMAND_UPDATE:
            jit_context = JitRangeUpdateCodegen(query, query_string, (JitRangeUpdatePlan*)plan);
            break;

        case JIT_COMMAND_SELECT: {
            JitRangeSelectPlan* range_select_plan = (JitRangeSelectPlan*)plan;
            if (range_select_plan->_aggregate._aggreaget_op == JIT_AGGREGATE_NONE) {
                jit_context = JitRangeSelectCodegen(query, query_string, range_select_plan);
            } else {
                jit_context = JitAggregateRangeSelectCodegen(query, query_string, range_select_plan);
            }
        } break;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Generate JIT Code",
                "Invalid point query JIT plan command type %d",
                (int)plan->_command_type);
            break;
    }

    return jit_context;
}

static JitContext* JitPointQueryCodegen(const Query* query, const char* query_string, JitPointQueryPlan* plan)
{
    JitContext* jit_context = nullptr;

    switch (plan->_command_type) {
        case JIT_COMMAND_UPDATE:
            jit_context = JitUpdateCodegen(query, query_string, (JitUpdatePlan*)plan);
            break;

        case JIT_COMMAND_DELETE:
            jit_context = JitDeleteCodegen(query, query_string, (JitDeletePlan*)plan);
            break;

        case JIT_COMMAND_SELECT:
            jit_context = JitSelectCodegen(query, query_string, (JitSelectPlan*)plan);
            break;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Generate JIT Code",
                "Invalid point query JIT plan command type %d",
                (int)plan->_command_type);
            break;
    }

    return jit_context;
}

extern JitContext* JitCodegenTvmQuery(Query* query, const char* query_string, JitPlan* plan)
{
    JitContext* jit_context = nullptr;

    MOT_LOG_DEBUG("*** Attempting to generate planned TVM-jitted code for query: %s", query_string);

    switch (plan->_plan_type) {
        case JIT_PLAN_INSERT_QUERY:
            jit_context = JitInsertCodegen(query, query_string, (JitInsertPlan*)plan);
            break;

        case JIT_PLAN_POINT_QUERY:
            jit_context = JitPointQueryCodegen(query, query_string, (JitPointQueryPlan*)plan);
            break;

        case JIT_PLAN_RANGE_SCAN:
            jit_context = JitRangeScanCodegen(query, query_string, (JitRangeScanPlan*)plan);
            break;

        case JIT_PLAN_JOIN:
            jit_context = JitJoinCodegen(query, query_string, (JitJoinPlan*)plan);
            break;

        case JIT_PLAN_COMPOUND:
            jit_context = JitCompoundCodegen(query, query_string, (JitCompoundPlan*)plan);
            break;

        default:
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Generate JIT Code", "Invalid JIT plan type %d", (int)plan->_plan_type);
            break;
    }

    if (jit_context == nullptr) {
        MOT_LOG_TRACE("Failed to generate TVM-jitted code for query: %s", query_string);
    } else {
        MOT_LOG_DEBUG(
            "Got TVM-jitted function %p after compile, for query: %s", jit_context->m_tvmFunction, query_string);
    }

    return jit_context;
}

extern int JitExecTvmQuery(JitContext* jit_context, ParamListInfo params, TupleTableSlot* slot, uint64_t* tp_processed,
    int* scan_ended, int newScan)
{
    int result = 0;
    ExecContext* exec_context = jit_context->m_execContext;

    // allocate execution context on-demand
    if (exec_context == nullptr) {
        exec_context = allocExecContext(jit_context->m_tvmFunction->getRegisterCount());
        if (exec_context == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Execute JIT", "Failed to allocate execution context for TVM-jit function");
            result = MOT::RC_MEMORY_ALLOCATION_ERROR;
        } else {
            // save for later execution
            jit_context->m_execContext = exec_context;
        }
    }

    if (exec_context != nullptr) {
        exec_context->_jit_context = jit_context;
        exec_context->_params = params;
        exec_context->_slot = slot;
        exec_context->_tp_processed = tp_processed;
        exec_context->_scan_ended = scan_ended;
        exec_context->m_newScan = newScan;

        result = (int)jit_context->m_tvmFunction->exec(exec_context);
    }

    return result;
}
}  // namespace JitExec
