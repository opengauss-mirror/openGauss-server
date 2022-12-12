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
 * jit_llvm_query_codegen.cpp
 *    LLVM JIT-compiled code generation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_llvm_query_codegen.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * ATTENTION: Be sure to include jit_llvm_query.h before anything else because of gscodegen.h
 * See jit_llvm_query.h for more details.
 */
#include "jit_llvm_query.h"
#include "jit_llvm_query_codegen.h"
#include "jit_llvm.h"
#include "jit_llvm_funcs.h"
#include "jit_llvm_blocks.h"
#include "jit_util.h"
#include "mot_error.h"
#include "utilities.h"
#include "mm_global_api.h"
#include "jit_source_map.h"

#ifdef ENABLE_LLVM_COMPILE
// for checking if LLVM_ENABLE_DUMP is defined and for using LLVM_VERSION_STRING
#include "llvm/Config/llvm-config.h"
#endif

namespace JitExec {
DECLARE_LOGGER(JitLlvmQueryCodegen, JitExec)

using namespace dorado;

// forward declarations
static void DestroyCodeGenContext(JitLlvmCodeGenContext* ctx);

/** @brief Define all LLVM prototypes. */
void InitCodeGenContextFuncs(JitLlvmCodeGenContext* ctx)
{
    llvm::Module* module = ctx->m_codeGen->module();

    // define all function calls
    defineDebugLog(ctx, module);
    defineIsSoftMemoryLimitReached(ctx, module);
    defineGetPrimaryIndex(ctx, module);
    defineGetTableIndex(ctx, module);
    defineInitKey(ctx, module);
    defineGetColumnAt(ctx, module);
    DefineGetExprIsNull(ctx, module);
    DefineSetExprIsNull(ctx, module);
    DefineGetExprCollation(ctx, module);
    DefineSetExprCollation(ctx, module);
    defineGetDatumParam(ctx, module);
    defineReadDatumColumn(ctx, module);
    defineWriteDatumColumn(ctx, module);
    defineBuildDatumKey(ctx, module);

    defineSetBit(ctx, module);
    defineResetBitmapSet(ctx, module);
    defineGetTableFieldCount(ctx, module);
    defineWriteRow(ctx, module);
    defineSearchRow(ctx, module);
    defineCreateNewRow(ctx, module);
    defineInsertRow(ctx, module);
    defineDeleteRow(ctx, module);
    defineSetRowNullBits(ctx, module);
    defineSetExprResultNullBit(ctx, module);
    defineExecClearTuple(ctx, module);
    defineExecStoreVirtualTuple(ctx, module);
    defineSelectColumn(ctx, module);
    defineSetTpProcessed(ctx, module);
    defineSetScanEnded(ctx, module);

    defineCopyKey(ctx, module);
    defineFillKeyPattern(ctx, module);
    defineAdjustKey(ctx, module);
    defineSearchIterator(ctx, module);
    defineBeginIterator(ctx, module);
    defineCreateEndIterator(ctx, module);
    defineIsScanEnd(ctx, module);
    DefineCheckRowExistsInIterator(ctx, module);
    defineGetRowFromIterator(ctx, module);
    defineDestroyIterator(ctx, module);

    defineSetStateIterator(ctx, module);
    defineGetStateIterator(ctx, module);
    defineIsStateIteratorNull(ctx, module);
    defineIsStateScanEnd(ctx, module);
    defineGetRowFromStateIteratorFunc(ctx, module);
    defineDestroyStateIterators(ctx, module);
    defineSetStateScanEndFlag(ctx, module);
    defineGetStateScanEndFlag(ctx, module);

    defineResetStateRow(ctx, module);
    defineSetStateRow(ctx, module);
    defineGetStateRow(ctx, module);
    defineCopyOuterStateRow(ctx, module);
    defineGetOuterStateRowCopy(ctx, module);
    defineIsStateRowNull(ctx, module);

    defineResetStateLimitCounter(ctx, module);
    defineIncrementStateLimitCounter(ctx, module);
    defineGetStateLimitCounter(ctx, module);

    definePrepareAvgArray(ctx, module);
    defineLoadAvgArray(ctx, module);
    defineSaveAvgArray(ctx, module);
    defineComputeAvgFromArray(ctx, module);

    defineResetAggValue(ctx, module);
    defineGetAggValue(ctx, module);
    defineSetAggValue(ctx, module);
    defineGetAggValueIsNull(ctx, module);
    defineSetAggValueIsNull(ctx, module);

    definePrepareDistinctSet(ctx, module);
    defineInsertDistinctItem(ctx, module);
    defineDestroyDistinctSet(ctx, module);

    defineResetTupleDatum(ctx, module);
    defineReadTupleDatum(ctx, module);
    defineWriteTupleDatum(ctx, module);

    DefineSelectSubQueryResultFunc(ctx, module);
    DefineCopyAggregateToSubQueryResultFunc(ctx, module);

    DefineGetSubQuerySlot(ctx, module);
    DefineGetSubQueryTable(ctx, module);
    DefineGetSubQueryIndex(ctx, module);
    DefineGetSubQuerySearchKey(ctx, module);
    DefineGetSubQueryEndIteratorKey(ctx, module);
    DefineGetConstAt(ctx, module);
    DefineGetInvokeParamListInfo(ctx, module);
    DefineSetParamValue(ctx, module);
    DefineInvokeStoredProcedure(ctx, module);
    DefineConvertViaString(ctx, module);

    DefineEmitProfileData(ctx, module);
}

/** @brief Define all LLVM used types synonyms. */
void InitCodeGenContextTypes(JitLlvmCodeGenContext* ctx)
{
    llvm::LLVMContext& context = ctx->m_codeGen->context();

    // PG types
    ctx->ParamListInfoDataType = llvm::StructType::create(context, "ParamListInfoData");
    ctx->TupleTableSlotType = llvm::StructType::create(context, "TupleTableSlot");
    ctx->NumericDataType = llvm::StructType::create(context, "NumericData");
    ctx->VarCharType = llvm::StructType::create(context, "VarChar");
    ctx->BpCharType = llvm::StructType::create(context, "BpChar");

    // MOT types
    ctx->MMEngineType = llvm::StructType::create(context, "MMEngine");
    ctx->TableType = llvm::StructType::create(context, "Table");
    ctx->IndexType = llvm::StructType::create(context, "Index");
    ctx->KeyType = llvm::StructType::create(context, "Key");
    ctx->ColumnType = llvm::StructType::create(context, "Column");
    ctx->RowType = llvm::StructType::create(context, "Row");
    ctx->BitmapSetType = llvm::StructType::create(context, "BitmapSet");
    ctx->IndexIteratorType = llvm::StructType::create(context, "IndexIterator");
}

/** @brief Initializes a code generation context. */
static bool InitCodeGenContext(JitLlvmCodeGenContext* ctx, GsCodeGen* code_gen, GsCodeGen::LlvmBuilder* builder,
    MOT::Table* table, MOT::Index* index, MOT::Table* inner_table = nullptr, MOT::Index* inner_index = nullptr,
    int invokeParamCount = 0)
{
    // make sure all members are nullptr for proper cleanup in case of failure
    errno_t erc = memset_s(ctx, sizeof(JitLlvmCodeGenContext), 0, sizeof(JitLlvmCodeGenContext));
    securec_check(erc, "\0", "\0");

    llvm_util::InitLlvmCodeGenContext(ctx, code_gen, builder);

    if (table && !InitTableInfo(&ctx->_table_info, table, index)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compile", "Failed to initialize table information for code-generation context");
        DestroyCodeGenContext(ctx);
        return false;
    }
    if (inner_table && inner_index && !InitTableInfo(&ctx->_inner_table_info, inner_table, inner_index)) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Compile",
            "Failed to initialize inner-scan table information for code-generation context");
        DestroyCodeGenContext(ctx);
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

    // allocate parameter info on global memory, as it will be passed to the global JIT context
    if (invokeParamCount > 0) {
        allocSize = sizeof(JitParamInfo) * invokeParamCount;
        ctx->m_paramInfo = (JitParamInfo*)MOT::MemGlobalAlloc(allocSize);
        if (ctx->m_paramInfo == nullptr) {
            MOT_REPORT_ERROR(MOT_ERROR_OOM,
                "JIT Compile",
                "Failed to allocate %u bytes for parameter info in code-generation context",
                allocSize);
            DestroyCodeGenContext(ctx);
            return false;
        }
        erc = memset_s(ctx->m_paramInfo, allocSize, 0, allocSize);
        securec_check(erc, "\0", "\0");
        ctx->m_paramCount = invokeParamCount;
    }

    InitCodeGenContextTypes(ctx);
    InitCodeGenContextFuncs(ctx);

    return true;
}

/** @brief Initializes a context for compilation. */
static bool InitCompoundCodeGenContext(JitLlvmCodeGenContext* ctx, GsCodeGen* code_gen, GsCodeGen::LlvmBuilder* builder,
    MOT::Table* table, MOT::Index* index, JitCompoundPlan* plan)
{
    // execute normal initialization
    if (!InitCodeGenContext(ctx, code_gen, builder, table, index)) {
        MOT_REPORT_ERROR(
            MOT_ERROR_OOM, "JIT Compile", "Failed to initialize table information for code-generation context");
        return false;
    }

    // prepare sub-query table info
    ctx->m_subQueryCount = plan->_sub_query_count;
    uint32_t allocSize = sizeof(TableInfo) * ctx->m_subQueryCount;
    ctx->m_subQueryTableInfo = (TableInfo*)MOT::MemSessionAlloc(allocSize);
    if (ctx->m_subQueryTableInfo == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Compile",
            "Failed to allocate %u bytes for %u sub-query table information objects in code-generation context",
            allocSize,
            (unsigned)ctx->m_subQueryCount);
        DestroyCodeGenContext(ctx);
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
            subIndex = ((JitRangeSelectPlan*)subPlan)->_index_scan._index;
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
            DestroyCodeGenContext(ctx);
            return false;
        }
    }

    // prepare sub-query data array (for sub-query runtime context)
    allocSize = sizeof(JitLlvmCodeGenContext::SubQueryData) * ctx->m_subQueryCount;
    ctx->m_subQueryData = (JitLlvmCodeGenContext::SubQueryData*)MOT::MemSessionAlloc(allocSize);
    if (ctx->m_subQueryData == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_OOM,
            "JIT Compile",
            "Failed to allocate %u bytes for %u sub-query data items in code-generation context",
            allocSize,
            (unsigned)ctx->m_subQueryCount);
        DestroyCodeGenContext(ctx);
        return false;
    }
    errno_t erc = memset_s(ctx->m_subQueryData, allocSize, 0, allocSize);
    securec_check(erc, "\0", "\0");

    return true;
}

/** @brief Destroys a code generation context. */
static void DestroyCodeGenContext(JitLlvmCodeGenContext* ctx)
{
    DestroyTableInfo(&ctx->_table_info);
    DestroyTableInfo(&ctx->_inner_table_info);
    for (uint32_t i = 0; i < ctx->m_subQueryCount; ++i) {
        DestroyTableInfo(&ctx->m_subQueryTableInfo[i]);
    }
    if (ctx->m_subQueryData != nullptr) {
        MOT::MemSessionFree(ctx->m_subQueryData);
        ctx->m_subQueryData = nullptr;
    }
    if (ctx->m_subQueryTableInfo != nullptr) {
        MOT::MemSessionFree(ctx->m_subQueryTableInfo);
        ctx->m_subQueryTableInfo = nullptr;
    }
    if (ctx->m_constValues != nullptr) {
        MOT::MemSessionFree(ctx->m_constValues);
    }
    if (ctx->m_paramInfo != nullptr) {
        MOT::MemGlobalFree(ctx->m_paramInfo);
    }
    llvm_util::DestroyLlvmCodeGenContext(ctx);
}

extern int AllocateConstId(JitLlvmCodeGenContext* ctx, int type, Datum value, bool isNull)
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
        MOT_LOG_TRACE("Allocated constant id: %d", res);
        JIT_PRINT_DATUM(MOT::LogLevel::LL_TRACE, "Allocated constant", type, value, isNull);
    }
    return res;
}

/** @brief Wraps up an LLVM function (compiles it and prepares a function pointer). */
static MotJitContext* FinalizeCodegen(
    JitLlvmCodeGenContext* ctx, JitCommandType command_type, const char* queryString, JitCodegenStats& codegenStats)
{
    // do GsCodeGen stuff to wrap up
    uint64_t startTime = GetSysClock();
    if (!ctx->m_codeGen->verifyFunction(ctx->m_jittedFunction)) {
        MOT_LOG_ERROR("Failed to generate jitted code for query: Failed to verify jit function");
#ifdef LLVM_ENABLE_DUMP
        ctx->m_jittedFunction->dump();
#else
        ctx->m_jittedFunction->print(llvm::errs(), nullptr, false, true);
#endif
        return nullptr;
    }
    uint64_t endTime = GetSysClock();
    uint64_t timeMicros = MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime);
    codegenStats.m_verifyTime = timeMicros;
    MOT_LOG_TRACE("Query '%s' verification time: %" PRIu64 " micros", queryString, timeMicros);

    startTime = GetSysClock();
    ctx->m_codeGen->FinalizeFunction(ctx->m_jittedFunction);
    endTime = GetSysClock();
    timeMicros = MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime);
    codegenStats.m_finalizeTime = timeMicros;
    MOT_LOG_TRACE("Query '%s' finalization time: %" PRIu64 " micros", queryString, timeMicros);

    if (IsMotCodegenPrintEnabled()) {
#ifdef LLVM_ENABLE_DUMP
        ctx->m_jittedFunction->dump();
#else
        ctx->m_jittedFunction->print(llvm::errs());
#endif
    }

    // prepare global constant array
    JitDatumArray datumArray = {};
    if (ctx->m_constCount > 0) {
        if (!PrepareDatumArray(ctx->m_constValues, ctx->m_constCount, &datumArray)) {
            MOT_LOG_ERROR("Failed to generate jitted code for query: Failed to prepare constant datum array");
            return nullptr;
        }
    }

    // that's it, we are ready
    JitQueryContext* jit_context =
        (JitQueryContext*)AllocJitContext(JIT_CONTEXT_GLOBAL, JitContextType::JIT_CONTEXT_TYPE_QUERY);
    if (jit_context == nullptr) {
        MOT_LOG_TRACE("Failed to allocate JIT context, aborting code generation");
        return nullptr;
    }

    MOT_LOG_DEBUG("Adding function to MCJit");
    ctx->m_codeGen->addFunctionToMCJit(ctx->m_jittedFunction, (void**)&jit_context->m_llvmFunction);

    MOT_LOG_DEBUG("Generating code...");
    startTime = GetSysClock();
    ctx->m_codeGen->enableOptimizations(true);
    ctx->m_codeGen->compileCurrentModule(false);
    endTime = GetSysClock();
    timeMicros = MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime);
    codegenStats.m_compileTime = timeMicros;
    MOT_LOG_TRACE("Query '%s' compilation time: %" PRIu64 " micros", queryString, timeMicros);

    // setup execution details
    jit_context->m_table = ctx->_table_info.m_table;
    if (jit_context->m_table != nullptr) {
        jit_context->m_tableId = jit_context->m_table->GetTableExId();
        MOT_LOG_TRACE("Installed table id: %" PRIu64, jit_context->m_tableId);
    }
    jit_context->m_index = ctx->_table_info.m_index;
    if (jit_context->m_index != nullptr) {
        jit_context->m_indexId = jit_context->m_index->GetExtId();
        MOT_LOG_TRACE("Installed index id: %" PRIu64, jit_context->m_indexId);
    }
    jit_context->m_codeGen = ctx->m_codeGen;  // steal the context
    ctx->m_codeGen = nullptr;                 // prevent destruction
    jit_context->m_innerTable = ctx->_inner_table_info.m_table;
    if (jit_context->m_innerTable != nullptr) {
        jit_context->m_innerTableId = jit_context->m_innerTable->GetTableExId();
        MOT_LOG_TRACE("Installed inner table id: %" PRIu64, jit_context->m_innerTableId);
    }
    jit_context->m_innerIndex = ctx->_inner_table_info.m_index;
    if (jit_context->m_innerIndex != nullptr) {
        jit_context->m_innerIndexId = jit_context->m_innerIndex->GetExtId();
        MOT_LOG_TRACE("Installed inner index id: %" PRIu64, jit_context->m_innerIndexId);
    }
    jit_context->m_aggCount = 0;
    jit_context->m_commandType = command_type;
    jit_context->m_subQueryCount = 0;
    jit_context->m_invokeParamCount = 0;
    jit_context->m_invokeParamInfo = nullptr;
    jit_context->m_constDatums.m_datumCount = datumArray.m_datumCount;
    jit_context->m_constDatums.m_datums = datumArray.m_datums;
    jit_context->m_validState = JIT_CONTEXT_VALID;

    return jit_context;
}

static inline void PrintErrorInfo(const char* queryString)
{
    ErrorData* edata = CopyErrorData();
    MOT_LOG_WARN("Caught exception while generating JIT LLVM code for query '%s': %s", queryString, edata->message);
    FlushErrorState();
    FreeErrorData(edata);
}

#define JIT_TRACE_FAIL(queryType, reason) \
    MOT_LOG_TRACE("Failed to generate jitted code for %s query: %s", queryType, reason)

static MotJitContext* JitUpdateCodegen(JitLlvmCodeGenContext* ctx, Query* query, const char* query_string,
    JitUpdatePlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT update at thread %p for query: %s", (void*)pthread_self(), query_string);

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedUpdate");
    IssueDebugLog("Starting execution of jitted UPDATE");

    // update is not allowed if we reached soft memory limit
    buildIsSoftMemoryLimitReached(ctx);

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // build one time filters if required
    if (plan->_query.m_oneTimeFilters._filter_count > 0) {
        if (!BuildOneTimeFilters(ctx, &plan->_query.m_oneTimeFilters)) {
            JIT_TRACE_FAIL("UPDATE", "Failed to build one-time filters");
            return nullptr;
        }
    } else {
        MOT_LOG_TRACE("One-time filters not specified");
    }

    // begin the WHERE clause (this is a point query
    if (!buildPointScan(ctx, &plan->_query._search_exprs, JIT_RANGE_SCAN_MAIN, nullptr)) {
        JIT_TRACE_FAIL("UPDATE", "Unsupported WHERE clause type");
        return nullptr;
    }

    // fetch row for writing
    MOT_LOG_DEBUG("Generating update code for point query");
    llvm::Value* row = buildSearchRow(ctx, MOT::AccessType::WR, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, nullptr, &plan->_query._filters, nullptr)) {
        JIT_TRACE_FAIL("UPDATE", "Unsupported filter");
        return nullptr;
    }

    // prepare a bitmap array
    IssueDebugLog("Resetting bitmap set for incremental redo");
    AddResetBitmapSet(ctx);

    // now begin updating columns
    IssueDebugLog("Updating row columns");
    if (!writeRowColumns(ctx, row, &plan->_update_exprs, true)) {
        JIT_TRACE_FAIL("UPDATE", "Failed to process target entry");
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

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    return FinalizeCodegen(ctx, JIT_COMMAND_UPDATE, query_string, codegenStats);
}

static MotJitContext* JitRangeUpdateCodegen(JitLlvmCodeGenContext* ctx, Query* query, const char* query_string,
    JitRangeUpdatePlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT range update at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedRangeUpdate");
    IssueDebugLog("Starting execution of jitted range UPDATE");

    // update is not allowed if we reached soft memory limit
    buildIsSoftMemoryLimitReached(ctx);

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // begin the WHERE clause
    MOT_LOG_DEBUG("Generating range cursor for range UPDATE query");
    JitLlvmRuntimeCursor cursor = buildRangeCursor(ctx, &plan->_index_scan, JIT_RANGE_SCAN_MAIN, nullptr);
    if (cursor.begin_itr == nullptr) {
        JIT_TRACE_FAIL("Range UPDATE", "Unsupported WHERE clause type");
        return nullptr;
    }

    AddResetBitmapSet(ctx);

    JIT_WHILE_BEGIN(cursor_loop)
    llvm::Value* res = AddIsScanEnd(ctx, JIT_INDEX_SCAN_FORWARD, &cursor, JIT_RANGE_SCAN_MAIN);
    JIT_WHILE_EVAL_NOT(res);
    {
        llvm::Value* row = buildGetRowFromIterator(ctx,
            JIT_WHILE_COND_BLOCK(),
            JIT_WHILE_POST_BLOCK(),
            MOT::AccessType::WR,
            plan->_index_scan._scan_direction,
            &cursor,
            JIT_RANGE_SCAN_MAIN);

        // check for additional filters
        if (!buildFilterRow(ctx, row, nullptr, &plan->_index_scan._filters, JIT_WHILE_COND_BLOCK())) {
            JIT_TRACE_FAIL("Range UPDATE", "Unsupported filter");
            return nullptr;
        }

        // now begin updating columns
        IssueDebugLog("Updating row columns");
        if (!writeRowColumns(ctx, row, &plan->_update_exprs, true)) {
            JIT_TRACE_FAIL("Range UPDATE", "Failed to process target entry");
            return nullptr;
        }

        IssueDebugLog("Writing row");
        buildWriteRow(ctx, row, false, &cursor);

        // the next call will be executed only if the previous call to writeRow succeeded
        buildIncrementRowsProcessed(ctx);

        // reset bitmap for next loop
        AddResetBitmapSet(ctx);
    }
    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of range update loop");
    AddDestroyCursor(ctx, &cursor);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    return FinalizeCodegen(ctx, JIT_COMMAND_RANGE_UPDATE, query_string, codegenStats);
}

static MotJitContext* JitInsertCodegen(JitLlvmCodeGenContext* ctx, Query* query, const char* query_string,
    JitInsertPlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT insert at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedInsert");
    IssueDebugLog("Starting execution of jitted INSERT");

    // insert is not allowed if we reached soft memory limit
    buildIsSoftMemoryLimitReached(ctx);

    // create new row and bitmap set
    llvm::Value* row = buildCreateNewRow(ctx);

    // set row null bits
    IssueDebugLog("Setting row null bits before insert");
    AddSetRowNullBits(ctx, row);

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    IssueDebugLog("Setting row columns");
    if (!writeRowColumns(ctx, row, &plan->_insert_exprs, false)) {
        JIT_TRACE_FAIL("INSERT", "Failed to process target entry");
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

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    return FinalizeCodegen(ctx, JIT_COMMAND_INSERT, query_string, codegenStats);
}

static MotJitContext* JitDeleteCodegen(JitLlvmCodeGenContext* ctx, Query* query, const char* query_string,
    JitDeletePlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT delete at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedDelete");
    IssueDebugLog("Starting execution of jitted DELETE");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // begin the WHERE clause
    if (!buildPointScan(ctx, &plan->_query._search_exprs, JIT_RANGE_SCAN_MAIN, nullptr)) {
        JIT_TRACE_FAIL("DELETE", "Unsupported WHERE clause type");
        return nullptr;
    }

    // fetch row for delete
    llvm::Value* row = buildSearchRow(ctx, MOT::AccessType::DEL, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, nullptr, &plan->_query._filters, nullptr)) {
        JIT_TRACE_FAIL("DELETE", "Unsupported filter");
        return nullptr;
    }

    // row is already cached in concurrency control module, so we do not need to provide an argument
    IssueDebugLog("Deleting row");
    buildDeleteRow(ctx);

    // the next call will be executed only if the previous call to deleteRow succeeded
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    return FinalizeCodegen(ctx, JIT_COMMAND_DELETE, query_string, codegenStats);
}

static MotJitContext* JitRangeDeleteCodegen(JitLlvmCodeGenContext* ctx, Query* query, const char* query_string,
    JitRangeDeletePlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT range delete at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedRangeDelete");
    IssueDebugLog("Starting execution of jitted range DELETE");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // begin the WHERE clause
    MOT_LOG_DEBUG("Generating range cursor for range DELETE query");
    JitLlvmRuntimeCursor cursor = buildRangeCursor(ctx, &plan->_index_scan, JIT_RANGE_SCAN_MAIN, nullptr);
    if (cursor.begin_itr == nullptr) {
        JIT_TRACE_FAIL("Range DELETE", "Unsupported WHERE clause type");
        return nullptr;
    }

    JIT_WHILE_BEGIN(cursor_loop)
    llvm::Value* res = AddIsScanEnd(ctx, JIT_INDEX_SCAN_FORWARD, &cursor, JIT_RANGE_SCAN_MAIN);
    JIT_WHILE_EVAL_NOT(res);
    {
        llvm::Value* row = buildGetRowFromIterator(ctx,
            JIT_WHILE_COND_BLOCK(),
            JIT_WHILE_POST_BLOCK(),
            MOT::AccessType::DEL,
            plan->_index_scan._scan_direction,
            &cursor,
            JIT_RANGE_SCAN_MAIN);

        // check for additional filters
        if (!buildFilterRow(ctx, row, nullptr, &plan->_index_scan._filters, JIT_WHILE_COND_BLOCK())) {
            JIT_TRACE_FAIL("Range DELETE", "Unsupported filter");
            return nullptr;
        }

        IssueDebugLog("Deleting row");
        // row is already cached in concurrency control module, so we do not need to provide an argument
        buildDeleteRow(ctx);

        // the next call will be executed only if the previous call to deleteRow succeeded
        buildIncrementRowsProcessed(ctx);

        // impose limit clause if any
        BuildCheckLimitNoState(ctx, plan->_limit_count);
    }
    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of range delete loop");
    AddDestroyCursor(ctx, &cursor);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    return FinalizeCodegen(ctx, JIT_COMMAND_RANGE_DELETE, query_string, codegenStats);
}

static MotJitContext* JitSelectCodegen(JitLlvmCodeGenContext* ctx, const Query* query, const char* query_string,
    JitSelectPlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT select at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedSelect");
    IssueDebugLog("Starting execution of jitted SELECT");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // build one time filters if required
    if (plan->_query.m_oneTimeFilters._filter_count > 0) {
        if (!BuildOneTimeFilters(ctx, &plan->_query.m_oneTimeFilters)) {
            JIT_TRACE_FAIL("SELECT", "Failed to build one-time filters");
            return nullptr;
        }
    } else {
        MOT_LOG_TRACE("One-time filters not specified");
    }

    // begin the WHERE clause
    if (!buildPointScan(ctx, &plan->_query._search_exprs, JIT_RANGE_SCAN_MAIN, nullptr)) {
        JIT_TRACE_FAIL("SELECT", "Unsupported WHERE clause type");
        return nullptr;
    }

    // fetch row for read
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, nullptr, &plan->_query._filters, nullptr)) {
        JIT_TRACE_FAIL("SELECT", "Unsupported filter");
        return nullptr;
    }

    // now begin selecting columns into result
    IssueDebugLog("Selecting columns into result");
    if (!selectRowColumns(ctx, row, &plan->_select_exprs)) {
        JIT_TRACE_FAIL("SELECT", "Failed to process target entry");
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);

    // update number of rows processed
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended (this is a point query)
    AddSetScanEnded(ctx, 1);

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    MotJitContext* jitContext = FinalizeCodegen(ctx, JIT_COMMAND_SELECT, query_string, codegenStats);
    return jitContext;
}

static void AddCleanupOldScan(JitLlvmCodeGenContext* ctx)
{
    // emit code to cleanup previous scan in case this is a new scan
    JIT_IF_BEGIN(cleanup_old_scan)
    JIT_IF_EVAL(ctx->isNewScanValue);
    {
        IssueDebugLog("Destroying state iterators due to new scan");
        AddDestroyStateIterators(ctx, JIT_RANGE_SCAN_MAIN);
        AddDestroyStateIterators(ctx, JIT_RANGE_SCAN_INNER);
        AddResetStateRow(ctx, JIT_RANGE_SCAN_MAIN);
        AddResetStateRow(ctx, JIT_RANGE_SCAN_INNER);
        // sub-query does not have a stateful execution, so no need to cleanup
    }
    JIT_IF_END()
}

/** @brief Generates code for range SELECT query with a possible LIMIT clause. */
static MotJitContext* JitRangeSelectCodegen(JitLlvmCodeGenContext* ctx, const Query* query, const char* query_string,
    JitRangeSelectPlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT select at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedRangeSelect");
    IssueDebugLog("Starting execution of jitted range SELECT");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // emit code to cleanup previous scan in case this is a new scan
    AddCleanupOldScan(ctx);

    // build one time filters if required, but only if scan is new
    bool result = true;
    JIT_IF_BEGIN(cleanup_old_scan)
    JIT_IF_EVAL(ctx->isNewScanValue);
    {
        IssueDebugLog("Checking for one-time filters due to new scan");
        if (plan->_index_scan.m_oneTimeFilters._filter_count > 0) {
            if (!BuildOneTimeFilters(ctx, &plan->_index_scan.m_oneTimeFilters)) {
                JIT_TRACE_FAIL("Range SELECT", "Failed to build one-time filters");
                result = false;
            }
        } else {
            MOT_LOG_TRACE("One-time filters not specified");
        }
    }
    JIT_IF_END()
    if (!result) {
        return nullptr;
    }

    // prepare stateful scan if not done so already, if no row exists then emit code to return from function
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* row =
        buildPrepareStateScanRow(ctx, &plan->_index_scan, JIT_RANGE_SCAN_MAIN, access_mode, nullptr, nullptr, nullptr);
    if (row == nullptr) {
        JIT_TRACE_FAIL("Range SELECT", "Unsupported WHERE clause type");
        return nullptr;
    }

    // select inner and outer row expressions into result tuple (no aggregate because aggregate is not stateful)
    IssueDebugLog("Retrieved row from state iterator, beginning to select columns into result tuple");
    if (!selectRowColumns(ctx, row, &plan->_select_exprs)) {
        JIT_TRACE_FAIL("Range SELECT", "Failed to select row expressions");
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

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    JitCommandType cmdType =
        (plan->_index_scan._scan_type == JIT_INDEX_SCAN_FULL) ? JIT_COMMAND_FULL_SELECT : JIT_COMMAND_RANGE_SELECT;

    MotJitContext* jitContext = FinalizeCodegen(ctx, cmdType, query_string, codegenStats);

    if (jitContext != nullptr) {
        if (cmdType == JIT_COMMAND_RANGE_SELECT && plan->m_nonNativeSortParams) {
            // This query is using non-native sort. clone the relevant data to context
            JitQueryContext* jitQueryContext = (JitQueryContext*)jitContext;
            jitQueryContext->m_nonNativeSortParams =
                CloneJitNonNativeSortParams(plan->m_nonNativeSortParams, JIT_CONTEXT_GLOBAL);
            if (jitQueryContext->m_nonNativeSortParams == nullptr) {
                MOT_LOG_ERROR("Failed to clone non native sort data into context.");
                DestroyJitContext(jitContext);
                jitContext = nullptr;
            }
        }
    }

    return jitContext;
}

/** @brief Generates code for range SELECT query with aggregator. */
static MotJitContext* JitAggregateRangeSelectCodegen(JitLlvmCodeGenContext* ctx, const Query* query,
    const char* query_string, JitRangeSelectPlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT aggregate range select at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedAggregateRangeSelect");
    IssueDebugLog("Starting execution of jitted aggregate range SELECT");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
    AddExecClearTuple(ctx);

    // prepare for aggregation
    if (!prepareAggregates(ctx, plan->m_aggregates, plan->m_aggCount)) {
        JIT_TRACE_FAIL("Aggregate Range SELECT", "Failed to prepare aggregates");
        return nullptr;
    }

    // counter for number of aggregates operates on a single row
    llvm::Value* aggCount = ctx->m_builder->CreateAlloca(ctx->INT32_T, 0, nullptr, "agg_count");
    ctx->m_builder->CreateStore(JIT_CONST_INT32(0), aggCount, true);

    // pay attention: aggregated range scan is not stateful, since we scan all tuples in one call
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;

    // build one time filters if required
    llvm::LLVMContext& context = ctx->m_codeGen->context();
    DEFINE_BLOCK(build_agg_select_result, ctx->m_jittedFunction);
    if (plan->_index_scan.m_oneTimeFilters._filter_count > 0) {
        if (!BuildOneTimeFilters(ctx, &plan->_index_scan.m_oneTimeFilters, build_agg_select_result)) {
            JIT_TRACE_FAIL("Aggregate Range SELECT", "Failed to build one-time filters");
            return nullptr;
        }
    } else {
        MOT_LOG_TRACE("One-time filters not specified");
    }

    // begin the WHERE clause
    // build range iterators
    MOT_LOG_DEBUG("Generating range cursor for range SELECT query");
    JitLlvmRuntimeCursor cursor = buildRangeCursor(ctx, &plan->_index_scan, JIT_RANGE_SCAN_MAIN, nullptr);
    if (cursor.begin_itr == nullptr) {
        JIT_TRACE_FAIL("Aggregate Range SELECT", "Unsupported WHERE clause type");
        return nullptr;
    }

    // when we have select count(*) without any filters we can optimize and skip row copy
    bool checkRowExistOnly = false;
    if ((plan->m_aggCount == 1) && (plan->m_aggregates[0]._aggreaget_op == JIT_AGGREGATE_COUNT) &&
        (plan->m_aggregates[0]._table == nullptr) && (plan->_index_scan._filters._filter_count == 0)) {
        checkRowExistOnly = true;
    }

    llvm::Value* row = nullptr;
    JIT_WHILE_BEGIN(cursor_aggregate_loop)
    llvm::Value* res = AddIsScanEnd(ctx, plan->_index_scan._scan_direction, &cursor, JIT_RANGE_SCAN_MAIN);
    JIT_WHILE_EVAL_NOT(res);
    {
        if (checkRowExistOnly) {
            BuildCheckRowExistsInIterator(
                ctx, JIT_WHILE_POST_BLOCK(), plan->_index_scan._scan_direction, &cursor, JIT_RANGE_SCAN_MAIN);
        } else {
            row = buildGetRowFromIterator(ctx,
                JIT_WHILE_COND_BLOCK(),
                JIT_WHILE_POST_BLOCK(),
                access_mode,
                plan->_index_scan._scan_direction,
                &cursor,
                JIT_RANGE_SCAN_MAIN);
            // check for additional filters, if not try to fetch next row
            if (!buildFilterRow(ctx, row, nullptr, &plan->_index_scan._filters, JIT_WHILE_COND_BLOCK())) {
                JIT_TRACE_FAIL("Aggregate Range SELECT", "Unsupported filter");
                return nullptr;
            }
        }

        // aggregate into tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
        // if row disqualified due to DISTINCT operator then go back to loop test block
        ctx->m_builder->CreateStore(JIT_CONST_INT32(0), aggCount);
        for (int i = 0; i < plan->m_aggCount; ++i) {
            if (!buildAggregateRow(ctx, &plan->m_aggregates[i], i, row, nullptr, aggCount)) {
                JIT_TRACE_FAIL("Aggregate Range SELECT", "Unsupported aggregate");
                return nullptr;
            }
        }
    }
    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of aggregate range select loop");
    AddDestroyCursor(ctx, &cursor);

    // wrap up aggregation and write to result tuple
    JIT_GOTO(build_agg_select_result);
    ctx->m_builder->SetInsertPoint(build_agg_select_result);
    buildAggregateResult(ctx, plan->m_aggregates, plan->m_aggCount);

    // store the result tuple
    AddExecStoreVirtualTuple(ctx);

    // execute *tp_processed = rows_processed
    buildIncrementRowsProcessed(ctx);  // aggregate ALWAYS has at least one row processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended (this is an aggregate loop)
    AddSetScanEnded(ctx, 1);

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    JitQueryContext* jitContext =
        (JitQueryContext*)FinalizeCodegen(ctx, JIT_COMMAND_AGGREGATE_RANGE_SELECT, query_string, codegenStats);
    if (jitContext != nullptr) {
        jitContext->m_aggCount = plan->m_aggCount;
    }
    return jitContext;
}

static bool BuildOneTimeJoinFilters(
    JitLlvmCodeGenContext* ctx, JitJoinPlan* plan, const char* planType, llvm::BasicBlock* nextBlock = nullptr)
{
    if (plan->_outer_scan.m_oneTimeFilters._filter_count > 0) {
        if (!BuildOneTimeFilters(ctx, &plan->_outer_scan.m_oneTimeFilters, nextBlock)) {
            JIT_TRACE_FAIL(planType, "Failed to build outer-scan one-time filters");
            return false;
        }
    } else {
        MOT_LOG_TRACE("One-time filters not specified for outer-scan");
    }

    // build one time filters if required
    if (plan->_inner_scan.m_oneTimeFilters._filter_count > 0) {
        if (!BuildOneTimeFilters(ctx, &plan->_inner_scan.m_oneTimeFilters, nextBlock)) {
            JIT_TRACE_FAIL(planType, "Failed to build inner-scan one-time filters");
            return false;
        }
    } else {
        MOT_LOG_TRACE("One-time filters not specified for inner-scan");
    }
    return true;
}

static MotJitContext* JitPointJoinCodegen(JitLlvmCodeGenContext* ctx, const Query* query, const char* query_string,
    JitJoinPlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT Point JOIN query at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedPointJoin");
    IssueDebugLog("Starting execution of jitted Point JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // build one time filters if required
    if (!BuildOneTimeJoinFilters(ctx, plan, "Point JOIN")) {
        return nullptr;
    }

    // search the outer row
    if (!buildPointScan(ctx, &plan->_outer_scan._search_exprs, JIT_RANGE_SCAN_MAIN, nullptr)) {
        JIT_TRACE_FAIL("Point JOIN", "Unsupported outer WHERE clause type");
        return nullptr;
    }

    // fetch row for read
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* outer_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, outer_row, nullptr, &plan->_outer_scan._filters, nullptr)) {
        JIT_TRACE_FAIL("Point JOIN", "Unsupported outer scan filter");
        return nullptr;
    }

    // before we move on to inner point scan, we save the outer row in a safe copy (but for that purpose we need to save
    // row in outer scan state)
    AddSetStateRow(ctx, outer_row, JIT_RANGE_SCAN_MAIN);
    AddCopyOuterStateRow(ctx);

    // now search the inner row
    if (!buildPointScan(ctx, &plan->_inner_scan._search_exprs, JIT_RANGE_SCAN_INNER, outer_row)) {
        JIT_TRACE_FAIL("Point JOIN", "Unsupported inner WHERE clause type");
        return nullptr;
    }
    llvm::Value* inner_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_INNER);

    // check for additional filters
    if (!buildFilterRow(ctx, outer_row, inner_row, &plan->_inner_scan._filters, nullptr)) {
        JIT_TRACE_FAIL("Point JOIN", "Unsupported inner scan filter");
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    llvm::Value* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now begin selecting columns into result
    MOT_LOG_TRACE("Selecting row columns in the result tuple");
    if (!selectRowColumns(ctx, outer_row_copy, &plan->_select_exprs, inner_row)) {
        JIT_TRACE_FAIL("Point JOIN", "Failed to select row columns into result tuple");
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);

    // update number of rows processed
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended (this is a point query)
    AddSetScanEnded(ctx, 1);

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    MotJitContext* jitContext = FinalizeCodegen(ctx, JIT_COMMAND_POINT_JOIN, query_string, codegenStats);
    return jitContext;
}

static MotJitContext* JitPointLeftJoinCodegen(JitLlvmCodeGenContext* ctx, const Query* query, const char* query_string,
    JitJoinPlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT Point LEFT JOIN query at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedPointLeftJoin");
    IssueDebugLog("Starting execution of jitted Point LEFT JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // build one time filters if required
    if (!BuildOneTimeJoinFilters(ctx, plan, "Point Left-JOIN")) {
        return nullptr;
    }

    // search the outer row
    if (!buildPointScan(ctx, &plan->_outer_scan._search_exprs, JIT_RANGE_SCAN_MAIN, nullptr)) {
        JIT_TRACE_FAIL("Point Left-JOIN", "Unsupported outer WHERE clause type");
        return nullptr;
    }

    // fetch row for read
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* outer_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, outer_row, nullptr, &plan->_outer_scan._filters, nullptr)) {
        JIT_TRACE_FAIL("Point Left-JOIN", "Unsupported outer scan filter");
        return nullptr;
    }

    // before we move on to inner point scan, we save the outer row in a safe copy (but for that purpose we need to save
    // row in outer scan state)
    AddSetStateRow(ctx, outer_row, JIT_RANGE_SCAN_MAIN);
    AddCopyOuterStateRow(ctx);

    // now search the inner row
    if (!buildPointScan(ctx, &plan->_inner_scan._search_exprs, JIT_RANGE_SCAN_INNER, outer_row)) {
        JIT_TRACE_FAIL("Point Left-JOIN", "Unsupported inner WHERE clause type");
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    llvm::Value* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // attention: if the row is null we continue
    llvm::Value* filterPassed = ctx->m_builder->CreateAlloca(ctx->INT32_T);
    ctx->m_builder->CreateStore(JIT_CONST_INT32(0), filterPassed);
    llvm::Value* inner_row = AddSearchRow(ctx, access_mode, JIT_RANGE_SCAN_INNER);
    JIT_IF_BEGIN(check_inner_row_found)
    JIT_IF_EVAL(inner_row);
    {
        // check for additional filters, if failed we jump to post block
        IssueDebugLog("Inner row found");
        if (!buildFilterRow(
                ctx, outer_row_copy, inner_row, &plan->_inner_scan._filters, JIT_IF_CURRENT()->GetPostBlock())) {
            JIT_TRACE_FAIL("Point Left-JOIN", "Unsupported inner scan filter");
            return nullptr;
        }
        // raise flag that all filters passed
        IssueDebugLog("Inner row PASSED filter test for LEFT JOIN");
        ctx->m_builder->CreateStore(JIT_CONST_INT32(1), filterPassed);
    }
    JIT_IF_END()

    // select row columns (if row not found or filter failed, then select null values for inner row)
    if (!BuildSelectLeftJoinRowColumns(ctx, outer_row_copy, plan, inner_row, filterPassed)) {
        JIT_TRACE_FAIL("Point Left-JOIN", "Failed to select row columns into result tuple");
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);

    // update number of rows processed
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended (this is a point query)
    AddSetScanEnded(ctx, 1);

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    MotJitContext* jitContext = FinalizeCodegen(ctx, JIT_COMMAND_POINT_JOIN, query_string, codegenStats);
    return jitContext;
}

/** @brief Generates code for range JOIN query with outer point query and a possible LIMIT clause but without
 * aggregation. */
static MotJitContext* JitOuterPointJoinCodegen(JitLlvmCodeGenContext* ctx, const Query* query, const char* query_string,
    JitJoinPlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT Outer Point JOIN query at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedOuterPointJoin");
    IssueDebugLog("Starting execution of jitted Outer Point JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // emit code to cleanup previous scan in case this is a new scan
    AddCleanupOldScan(ctx);

    // build one time filters if required
    if (!BuildOneTimeJoinFilters(ctx, plan, "Outer Point JOIN")) {
        return nullptr;
    }

    // we first check if outer state row was already searched
    llvm::Value* outer_row = AddGetStateRow(ctx, JIT_RANGE_SCAN_MAIN);
    JIT_IF_BEGIN(check_outer_row_ready)
    JIT_IF_EVAL_NOT(outer_row);
    {
        // search the outer row
        if (!buildPointScan(ctx, &plan->_outer_scan._search_exprs, JIT_RANGE_SCAN_MAIN, nullptr)) {
            JIT_TRACE_FAIL("Outer Point JOIN", "Unsupported outer WHERE clause type");
            return nullptr;
        }

        // fetch row for read and check for additional filters
        MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
        outer_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);
        if (!buildFilterRow(ctx, outer_row, nullptr, &plan->_outer_scan._filters, nullptr)) {
            JIT_TRACE_FAIL("Outer Point JOIN", "Unsupported outer scan filter");
            return nullptr;
        }

        // before we move on to inner range scan, we save the outer row in a safe copy (but for that purpose we need to
        // save row in outer scan state)
        AddSetStateRow(ctx, outer_row, JIT_RANGE_SCAN_MAIN);
        AddCopyOuterStateRow(ctx);
    }
    JIT_IF_END()

    // retrieve the safe copy of the outer row
    llvm::Value* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now prepare inner scan if needed, if no row was found then emit code to return from function (since outer scan is
    // a point query)
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* inner_row = buildPrepareStateScanRow(
        ctx, &plan->_inner_scan, JIT_RANGE_SCAN_INNER, access_mode, outer_row_copy, nullptr, nullptr);
    if (inner_row == nullptr) {
        JIT_TRACE_FAIL("Outer Point JOIN", "Unsupported inner WHERE clause type");
        return nullptr;
    }

    // now begin selecting columns into result
    if (!selectRowColumns(ctx, outer_row_copy, &plan->_select_exprs, inner_row)) {
        JIT_TRACE_FAIL("Outer Point JOIN", "Failed to select row columns into result tuple");
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

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    MotJitContext* jitContext = FinalizeCodegen(ctx, JIT_COMMAND_RANGE_JOIN, query_string, codegenStats);
    return jitContext;
}

/** @brief Generates code for range JOIN query with outer point query and a possible LIMIT clause but without
 * aggregation. */
static MotJitContext* JitOuterPointLeftJoinCodegen(JitLlvmCodeGenContext* ctx, const Query* query,
    const char* query_string, JitJoinPlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT Outer Point LEFT JOIN query at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedOuterPointLeftJoin");
    IssueDebugLog("Starting execution of jitted Outer Point LEFT JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // emit code to cleanup previous scan in case this is a new scan
    AddCleanupOldScan(ctx);

    // build one time filters if required
    if (!BuildOneTimeJoinFilters(ctx, plan, "Outer Point Left-JOIN")) {
        return nullptr;
    }

    // we first check if outer state row was already searched
    llvm::Value* outer_row = AddGetStateRow(ctx, JIT_RANGE_SCAN_MAIN);
    JIT_IF_BEGIN(check_outer_row_ready)
    JIT_IF_EVAL_NOT(outer_row);
    {
        // search the outer row
        if (!buildPointScan(ctx, &plan->_outer_scan._search_exprs, JIT_RANGE_SCAN_MAIN, nullptr)) {
            JIT_TRACE_FAIL("Outer Point Left-JOIN", "Unsupported outer WHERE clause type");
            return nullptr;
        }

        // fetch row for read and check for additional filters
        MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
        outer_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);
        if (!buildFilterRow(ctx, outer_row, nullptr, &plan->_outer_scan._filters, nullptr)) {
            JIT_TRACE_FAIL("Outer Point Left-JOIN", "Unsupported outer scan filter");
            return nullptr;
        }

        // before we move on to inner range scan, we save the outer row in a safe copy (but for that purpose we need to
        // save row in outer scan state)
        AddSetStateRow(ctx, outer_row, JIT_RANGE_SCAN_MAIN);
        AddCopyOuterStateRow(ctx);
    }
    JIT_IF_END()

    // retrieve the safe copy of the outer row
    llvm::Value* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now prepare inner scan if needed, if no row was found or failed to pass filter, then we continue (suppress emit
    // return instruction), since in left join we report nulls
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* inner_row = buildPrepareStateScanRow(
        ctx, &plan->_inner_scan, JIT_RANGE_SCAN_INNER, access_mode, outer_row_copy, nullptr, nullptr, false);
    if (inner_row == nullptr) {
        JIT_TRACE_FAIL("Outer Point Left-JOIN", "Unsupported inner WHERE clause type");
        return nullptr;
    }

    // now begin selecting columns into result
    if (!selectRowColumns(ctx, outer_row_copy, &plan->_select_exprs, inner_row)) {
        JIT_TRACE_FAIL("Outer Point Left-JOIN", "Failed to select row columns into result tuple");
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

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    MotJitContext* jitContext = FinalizeCodegen(ctx, JIT_COMMAND_RANGE_JOIN, query_string, codegenStats);
    return jitContext;
}

/** @brief Generates code for range JOIN query with inner point query and a possible LIMIT clause but without
 * aggregation. */
static MotJitContext* JitInnerPointJoinCodegen(JitLlvmCodeGenContext* ctx, const Query* query, const char* query_string,
    JitJoinPlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT Inner Point JOIN at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedInnerPointJoin");
    IssueDebugLog("Starting execution of jitted inner point JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // emit code to cleanup previous scan in case this is a new scan
    AddCleanupOldScan(ctx);

    // build one time filters if required
    if (!BuildOneTimeJoinFilters(ctx, plan, "Inner Point JOIN")) {
        return nullptr;
    }

    // prepare stateful scan if not done so already, if row not found then emit code to return from function (since this
    // is an outer scan)
    llvm::BasicBlock* fetch_outer_row_bb = nullptr;
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* outer_row = buildPrepareStateScanRow(
        ctx, &plan->_outer_scan, JIT_RANGE_SCAN_MAIN, access_mode, nullptr, nullptr, &fetch_outer_row_bb);
    if (outer_row == nullptr) {
        JIT_TRACE_FAIL("Inner Point JOIN", "Unsupported outer WHERE clause type");
        return nullptr;
    }

    // before we move on to inner scan, we save the outer row in a safe copy
    AddCopyOuterStateRow(ctx);

    // now search the inner row
    if (!buildPointScan(ctx, &plan->_inner_scan._search_exprs, JIT_RANGE_SCAN_INNER, outer_row)) {
        JIT_TRACE_FAIL("Inner Point JOIN", "Unsupported inner WHERE clause type");
        return nullptr;
    }
    llvm::Value* inner_row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_INNER);

    // retrieve the safe copy of the outer row
    llvm::Value* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // check for additional filters
    if (!buildFilterRow(ctx, outer_row_copy, inner_row, &plan->_inner_scan._filters, fetch_outer_row_bb)) {
        JIT_TRACE_FAIL("Inner Point JOIN", "Unsupported inner scan filter");
        return nullptr;
    }

    // now begin selecting columns into result
    if (!selectRowColumns(ctx, outer_row_copy, &plan->_select_exprs, inner_row)) {
        JIT_TRACE_FAIL("Inner Point JOIN", "Failed to select row columns into result tuple");
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);
    buildIncrementRowsProcessed(ctx);

    // make sure that next iteration find an empty state row, so scan makes a progress
    AddResetStateRow(ctx, JIT_RANGE_SCAN_MAIN);

    // if a limit clause exists, then increment limit counter and check if reached limit
    buildCheckLimit(ctx, plan->_limit_count);

    // generate code for setting output parameter tp_processed value to rows_processed
    AddSetTpProcessed(ctx);

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    MotJitContext* jitContext = FinalizeCodegen(ctx, JIT_COMMAND_RANGE_JOIN, query_string, codegenStats);
    return jitContext;
}

/** @brief Generates code for range JOIN query with inner point query and a possible LIMIT clause but without
 * aggregation. */
static MotJitContext* JitInnerPointLeftJoinCodegen(JitLlvmCodeGenContext* ctx, const Query* query,
    const char* query_string, JitJoinPlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT Inner Point Left JOIN at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedInnerPointLeftJoin");
    IssueDebugLog("Starting execution of jitted inner point LEFT JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // emit code to cleanup previous scan in case this is a new scan
    AddCleanupOldScan(ctx);

    // build one time filters if required
    if (!BuildOneTimeJoinFilters(ctx, plan, "Inner Point Left-JOIN")) {
        return nullptr;
    }

    // prepare stateful scan if not done so already, if row not found then emit code to return from function (since this
    // is an outer scan)
    llvm::BasicBlock* fetch_outer_row_bb = nullptr;
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* outer_row = buildPrepareStateScanRow(
        ctx, &plan->_outer_scan, JIT_RANGE_SCAN_MAIN, access_mode, nullptr, nullptr, &fetch_outer_row_bb);
    if (outer_row == nullptr) {
        JIT_TRACE_FAIL("Inner Point Left-JOIN", "Unsupported outer WHERE clause type");
        return nullptr;
    }

    // before we move on to inner scan, we save the outer row in a safe copy
    AddCopyOuterStateRow(ctx);

    // now search the inner row
    if (!buildPointScan(ctx, &plan->_inner_scan._search_exprs, JIT_RANGE_SCAN_INNER, outer_row)) {
        JIT_TRACE_FAIL("Inner Point Left-JOIN", "Unsupported inner WHERE clause type");
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    llvm::Value* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // attention: if the row is null we continue
    llvm::Value* filterPassed = ctx->m_builder->CreateAlloca(ctx->INT32_T);
    ctx->m_builder->CreateStore(JIT_CONST_INT32(0), filterPassed);
    llvm::Value* inner_row = AddSearchRow(ctx, access_mode, JIT_RANGE_SCAN_INNER);
    JIT_IF_BEGIN(check_inner_row_found)
    JIT_IF_EVAL(inner_row);
    {
        // check for additional filters, if failed we jump to post block
        IssueDebugLog("Inner row found");
        if (!buildFilterRow(
                ctx, outer_row_copy, inner_row, &plan->_inner_scan._filters, JIT_IF_CURRENT()->GetPostBlock())) {
            JIT_TRACE_FAIL("Inner Point Left-JOIN", "Unsupported inner scan filter");
            return nullptr;
        }
        // raise flag that all filters passed
        IssueDebugLog("Inner row PASSED filter test for LEFT JOIN");
        ctx->m_builder->CreateStore(JIT_CONST_INT32(1), filterPassed);
    }
    JIT_IF_END()

    // select row columns (if row not found or filter failed, then select null values for inner row)
    if (!BuildSelectLeftJoinRowColumns(ctx, outer_row_copy, plan, inner_row, filterPassed)) {
        JIT_TRACE_FAIL("Inner Point Left-JOIN", "Failed to select row columns into result tuple");
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);
    buildIncrementRowsProcessed(ctx);

    // make sure that next iteration find an empty state row, so scan makes a progress
    AddResetStateRow(ctx, JIT_RANGE_SCAN_MAIN);

    // if a limit clause exists, then increment limit counter and check if reached limit
    buildCheckLimit(ctx, plan->_limit_count);

    // generate code for setting output parameter tp_processed value to rows_processed
    AddSetTpProcessed(ctx);

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    MotJitContext* jitContext = FinalizeCodegen(ctx, JIT_COMMAND_RANGE_JOIN, query_string, codegenStats);
    return jitContext;
}

/** @brief Generates code for range JOIN query with a possible LIMIT clause but without aggregation. */
static MotJitContext* JitRangeJoinCodegen(JitLlvmCodeGenContext* ctx, const Query* query, const char* query_string,
    JitJoinPlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT range JOIN at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedRangeJoin");
    IssueDebugLog("Starting execution of jitted range JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // emit code to cleanup previous scan in case this is a new scan
    AddCleanupOldScan(ctx);

    // build one time filters if required
    if (!BuildOneTimeJoinFilters(ctx, plan, "Range JOIN")) {
        return nullptr;
    }

    // prepare stateful scan if not done so already
    llvm::BasicBlock* fetch_outer_row_bb = nullptr;
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* outer_row = buildPrepareStateScanRow(
        ctx, &plan->_outer_scan, JIT_RANGE_SCAN_MAIN, access_mode, nullptr, nullptr, &fetch_outer_row_bb);
    if (outer_row == nullptr) {
        JIT_TRACE_FAIL("Range JOIN", "Unsupported outer WHERE clause type");
        return nullptr;
    }

    // before we move on to inner scan, we save the outer row in a safe copy
    AddCopyOuterStateRow(ctx);

    // now prepare inner scan if needed
    llvm::Value* inner_row = buildPrepareStateScanRow(
        ctx, &plan->_inner_scan, JIT_RANGE_SCAN_INNER, access_mode, outer_row, fetch_outer_row_bb, nullptr);
    if (inner_row == nullptr) {
        JIT_TRACE_FAIL("Range JOIN", "Unsupported inner WHERE clause type");
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    llvm::Value* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now begin selecting columns into result
    if (!selectRowColumns(ctx, outer_row_copy, &plan->_select_exprs, inner_row)) {
        JIT_TRACE_FAIL("Range JOIN", "Failed to select row columns into result tuple");
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

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    MotJitContext* jitContext = FinalizeCodegen(ctx, JIT_COMMAND_RANGE_JOIN, query_string, codegenStats);
    return jitContext;
}

/** @brief Generates code for range JOIN query with a possible LIMIT clause but without aggregation. */
static MotJitContext* JitRangeLeftJoinCodegen(JitLlvmCodeGenContext* ctx, const Query* query, const char* query_string,
    JitJoinPlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT range LEFT JOIN at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedRangeLeftJoin");
    IssueDebugLog("Starting execution of jitted range LEFT JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple even if row is not found later
    AddExecClearTuple(ctx);

    // emit code to cleanup previous scan in case this is a new scan
    AddCleanupOldScan(ctx);

    // build one time filters if required
    if (!BuildOneTimeJoinFilters(ctx, plan, "Range Left-JOIN")) {
        return nullptr;
    }

    // prepare stateful scan if not done so already
    llvm::BasicBlock* fetch_outer_row_bb = nullptr;
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* outer_row = buildPrepareStateScanRow(
        ctx, &plan->_outer_scan, JIT_RANGE_SCAN_MAIN, access_mode, nullptr, nullptr, &fetch_outer_row_bb);
    if (outer_row == nullptr) {
        JIT_TRACE_FAIL("Range Left-JOIN", "Unsupported outer WHERE clause type");
        return nullptr;
    }

    // before we move on to inner scan, we save the outer row in a safe copy
    AddCopyOuterStateRow(ctx);

    // now prepare inner scan
    // attention: if we fail (row not found, or did not pass filters) we still continue and report nulls to user
    llvm::Value* inner_row = buildPrepareStateScanRow(
        ctx, &plan->_inner_scan, JIT_RANGE_SCAN_INNER, access_mode, outer_row, nullptr, nullptr, false);
    if (inner_row == nullptr) {
        JIT_TRACE_FAIL("Range Left-JOIN", "Unsupported inner WHERE clause type");
        return nullptr;
    }

    // retrieve the safe copy of the outer row
    llvm::Value* outer_row_copy = AddGetOuterStateRowCopy(ctx);

    // now begin selecting columns into result
    if (!selectRowColumns(ctx, outer_row_copy, &plan->_select_exprs, inner_row)) {
        JIT_TRACE_FAIL("Range Left-JOIN", "Failed to select row columns into result tuple");
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

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    MotJitContext* jitContext = FinalizeCodegen(ctx, JIT_COMMAND_RANGE_JOIN, query_string, codegenStats);
    return jitContext;
}

/** @brief Generates code for range JOIN query with an aggregator. */
static MotJitContext* JitAggregateRangeJoinCodegen(JitLlvmCodeGenContext* ctx, Query* query, const char* query_string,
    JitJoinPlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for MOT aggregate range JOIN at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedAggregateRangeJoin");
    IssueDebugLog("Starting execution of jitted aggregate range JOIN");

    // initialize rows_processed local variable
    buildResetRowsProcessed(ctx);

    // clear tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
    AddExecClearTuple(ctx);

    // prepare for aggregation
    if (!prepareAggregates(ctx, plan->m_aggregates, plan->m_aggCount)) {
        JIT_TRACE_FAIL("Aggregate Range JOIN", "Failed to prepare aggregates");
        return nullptr;
    }

    // counter for number of aggregates operates on a single row
    llvm::Value* aggCount = ctx->m_builder->CreateAlloca(ctx->INT32_T, 0, nullptr, "agg_count");
    ctx->m_builder->CreateStore(JIT_CONST_INT32(0), aggCount, true);

    // build one time filters if required
    llvm::LLVMContext& context = ctx->m_codeGen->context();
    DEFINE_BLOCK(build_agg_join_result, ctx->m_jittedFunction);
    if (!BuildOneTimeJoinFilters(ctx, plan, "Aggregate Range JOIN", build_agg_join_result)) {
        return nullptr;
    }

    // pay attention: aggregated range scan is not stateful, since we scan all tuples in one call
    MOT::AccessType accessMode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;

    // begin the WHERE clause
    // build range iterators
    MOT_LOG_DEBUG("Generating outer loop cursor for range JOIN query");
    JitLlvmRuntimeCursor outer_cursor = buildRangeCursor(ctx, &plan->_outer_scan, JIT_RANGE_SCAN_MAIN, nullptr);
    if (outer_cursor.begin_itr == nullptr) {
        JIT_TRACE_FAIL("Aggregate Range JOIN", "Unsupported outer WHERE clause type");
        return nullptr;
    }

    JIT_WHILE_BEGIN(cursor_aggregate_outer_loop)
    llvm::Value* res = AddIsScanEnd(ctx, plan->_outer_scan._scan_direction, &outer_cursor, JIT_RANGE_SCAN_MAIN);
    JIT_WHILE_EVAL_NOT(res);
    {
        llvm::Value* outer_row = buildGetRowFromIterator(ctx,
            JIT_WHILE_COND_BLOCK(),
            JIT_WHILE_POST_BLOCK(),
            accessMode,
            plan->_outer_scan._scan_direction,
            &outer_cursor,
            JIT_RANGE_SCAN_MAIN);

        // check for additional filters, if not try to fetch next row
        if (!buildFilterRow(ctx, outer_row, nullptr, &plan->_outer_scan._filters, JIT_WHILE_COND_BLOCK())) {
            JIT_TRACE_FAIL("Aggregate Range JOIN", "Unsupported outer scan filter");
            return nullptr;
        }

        // before we move on to inner scan, we save the outer row in a safe copy (but for that purpose we need to save
        // row in outer scan state)
        AddSetStateRow(ctx, outer_row, JIT_RANGE_SCAN_MAIN);
        AddCopyOuterStateRow(ctx);

        // now build the inner loop
        MOT_LOG_DEBUG("Generating inner loop cursor for range JOIN query");
        JitLlvmRuntimeCursor inner_cursor = buildRangeCursor(ctx, &plan->_inner_scan, JIT_RANGE_SCAN_INNER, outer_row);
        if (inner_cursor.begin_itr == nullptr) {
            JIT_TRACE_FAIL("Aggregate Range JOIN", "Unsupported outer scan WHERE clause type");
            return nullptr;
        }

        JIT_WHILE_BEGIN(cursor_aggregate_inner_loop)
        llvm::Value* resInner =
            AddIsScanEnd(ctx, plan->_inner_scan._scan_direction, &inner_cursor, JIT_RANGE_SCAN_INNER);
        JIT_WHILE_EVAL_NOT(resInner);
        {
            llvm::Value* inner_row = buildGetRowFromIterator(ctx,
                JIT_WHILE_COND_BLOCK(),
                JIT_WHILE_POST_BLOCK(),
                accessMode,
                plan->_inner_scan._scan_direction,
                &inner_cursor,
                JIT_RANGE_SCAN_INNER);

            // check for additional filters, if not try to fetch next row
            if (!buildFilterRow(ctx, nullptr, inner_row, &plan->_inner_scan._filters, JIT_WHILE_COND_BLOCK())) {
                JIT_TRACE_FAIL("Aggregate Range JOIN", "Unsupported inner scan filter");
                return nullptr;
            }

            // aggregate into tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
            // find out to which table the aggregate expression refers, and aggregate it
            // if row disqualified due to DISTINCT operator then go back to inner loop test block
            ctx->m_builder->CreateStore(JIT_CONST_INT32(0), aggCount, true);
            for (int i = 0; i < plan->m_aggCount; ++i) {
                bool aggRes = false;
                if (plan->m_aggregates[i]._table == ctx->_inner_table_info.m_table) {
                    aggRes = buildAggregateRow(ctx, &plan->m_aggregates[i], i, nullptr, inner_row, aggCount);
                } else {
                    // retrieve the safe copy of the outer row
                    llvm::Value* outer_row_copy = AddGetOuterStateRowCopy(ctx);
                    aggRes = buildAggregateRow(ctx, &plan->m_aggregates[i], i, outer_row_copy, nullptr, aggCount);
                }

                if (!aggRes) {
                    JIT_TRACE_FAIL("Aggregate Range JOIN", "Unsupported aggregate");
                    return nullptr;
                }
            }
        }
        JIT_WHILE_END()  // inner loop

        // cleanup
        IssueDebugLog("Reached end of inner loop");
        AddDestroyCursor(ctx, &inner_cursor);
    }
    JIT_WHILE_END()  // outer loop

    // cleanup
    IssueDebugLog("Reached end of outer loop");
    AddDestroyCursor(ctx, &outer_cursor);

    // wrap up aggregation and write to result tuple
    JIT_GOTO(build_agg_join_result);
    ctx->m_builder->SetInsertPoint(build_agg_join_result);
    buildAggregateResult(ctx, plan->m_aggregates, plan->m_aggCount);

    // store the result tuple
    AddExecStoreVirtualTuple(ctx);

    // execute *tp_processed = rows_processed
    buildIncrementRowsProcessed(ctx);  // aggregate ALWAYS has at least one row processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended
    AddSetScanEnded(ctx, 1);

    InjectProfileData(ctx, GetActiveNamespace(), query_string, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    JitQueryContext* jitContext =
        (JitQueryContext*)FinalizeCodegen(ctx, JIT_COMMAND_AGGREGATE_JOIN, query_string, codegenStats);
    jitContext->m_aggCount = plan->m_aggCount;
    return jitContext;
}

static MotJitContext* JitJoinCodegen(JitLlvmCodeGenContext* ctx, Query* query, const char* query_string,
    JitJoinPlan* plan, JitCodegenStats& codegenStats)
{
    MotJitContext* jit_context = nullptr;

    if (plan->m_aggCount == 0) {
        switch (plan->_scan_type) {
            case JIT_JOIN_SCAN_POINT:
                // special case: this is really a point query
                if (plan->_join_type == JitJoinType::JIT_JOIN_INNER) {
                    jit_context = JitPointJoinCodegen(ctx, query, query_string, plan, codegenStats);
                } else if (plan->_join_type == JitJoinType::JIT_JOIN_LEFT) {
                    jit_context = JitPointLeftJoinCodegen(ctx, query, query_string, plan, codegenStats);
                }
                break;

            case JIT_JOIN_SCAN_OUTER_POINT:
                // special case: outer scan is really a point query
                if (plan->_join_type == JitJoinType::JIT_JOIN_INNER) {
                    jit_context = JitOuterPointJoinCodegen(ctx, query, query_string, plan, codegenStats);
                } else if (plan->_join_type == JitJoinType::JIT_JOIN_LEFT) {
                    jit_context = JitOuterPointLeftJoinCodegen(ctx, query, query_string, plan, codegenStats);
                }
                break;

            case JIT_JOIN_SCAN_INNER_POINT:
                // special case: inner scan is really a point query
                if (plan->_join_type == JitJoinType::JIT_JOIN_INNER) {
                    jit_context = JitInnerPointJoinCodegen(ctx, query, query_string, plan, codegenStats);
                } else if (plan->_join_type == JitJoinType::JIT_JOIN_LEFT) {
                    jit_context = JitInnerPointLeftJoinCodegen(ctx, query, query_string, plan, codegenStats);
                }
                break;

            case JIT_JOIN_SCAN_RANGE:
                if (plan->_join_type == JitJoinType::JIT_JOIN_INNER) {
                    jit_context = JitRangeJoinCodegen(ctx, query, query_string, plan, codegenStats);
                } else if (plan->_join_type == JitJoinType::JIT_JOIN_LEFT) {
                    jit_context = JitRangeLeftJoinCodegen(ctx, query, query_string, plan, codegenStats);
                }
                break;

            default:
                MOT_LOG_TRACE(
                    "Cannot generate jitteed code for JOIN plan: Invalid JOIN scan type %d", (int)plan->_scan_type);
                break;
        }
    } else {
        jit_context = JitAggregateRangeJoinCodegen(ctx, query, query_string, plan, codegenStats);
    }

    return jit_context;
}

static bool JitSubSelectCodegen(JitLlvmCodeGenContext* ctx, JitCompoundPlan* plan, int subQueryIndex)
{
    MOT_LOG_DEBUG("Generating code for MOT sub-select at thread %p", (intptr_t)pthread_self());
    IssueDebugLog("Executing simple SELECT sub-query");

    // get the sub-query plan
    JitSelectPlan* subPlan = (JitSelectPlan*)plan->_sub_query_plans[subQueryIndex];

    // begin the WHERE clause
    if (!buildPointScan(ctx, &subPlan->_query._search_exprs, JIT_RANGE_SCAN_SUB_QUERY, nullptr, -1, subQueryIndex)) {
        JIT_TRACE_FAIL("Compound sub-SELECT", "Unsupported WHERE clause type");
        return false;
    }

    // fetch row for read
    llvm::Value* row = buildSearchRow(ctx, MOT::AccessType::RD, JIT_RANGE_SCAN_SUB_QUERY, subQueryIndex);

    // check for additional filters
    if (!buildFilterRow(ctx, row, nullptr, &subPlan->_query._filters, nullptr)) {
        JIT_TRACE_FAIL("Compound sub-SELECT", "Unsupported filter");
        return false;
    }

    // now begin selecting columns into result
    IssueDebugLog("Selecting column into result");
    if (!selectRowColumns(ctx, row, &subPlan->_select_exprs)) {
        JIT_TRACE_FAIL("Compound sub-SELECT", "Failed to process target entry");
        return false;
    }

    return true;
}

/** @brief Generates code for range SELECT sub-query with aggregator. */
static bool JitSubAggregateRangeSelectCodegen(JitLlvmCodeGenContext* ctx, JitCompoundPlan* plan, int subQueryIndex)
{
    MOT_LOG_DEBUG("Generating code for MOT aggregate range sub-select at thread %p", (intptr_t)pthread_self());
    IssueDebugLog("Executing aggregated range select sub-query");

    // get the sub-query plan
    JitRangeSelectPlan* subPlan = (JitRangeSelectPlan*)plan->_sub_query_plans[subQueryIndex];

    // prepare for aggregation
    if (!prepareAggregates(ctx, subPlan->m_aggregates, subPlan->m_aggCount)) {
        JIT_TRACE_FAIL("Aggregate Range sub-SELECT", "Failed to prepare aggregates");
        return false;
    }

    // counter for number of aggregates operates on a single row
    llvm::Value* aggCount = ctx->m_builder->CreateAlloca(ctx->INT32_T, 0, nullptr, "agg_count");
    ctx->m_builder->CreateStore(JIT_CONST_INT32(0), aggCount, true);

    // build one time filters if required
    llvm::LLVMContext& context = ctx->m_codeGen->context();
    DEFINE_BLOCK(build_agg_result, ctx->m_jittedFunction);
    if (subPlan->_index_scan.m_oneTimeFilters._filter_count > 0) {
        if (!BuildOneTimeFilters(ctx, &subPlan->_index_scan.m_oneTimeFilters, build_agg_result)) {
            JIT_TRACE_FAIL("Aggregate Range SELECT", "Failed to build one-time filters");
            return false;
        }
    } else {
        MOT_LOG_TRACE("One-time filters not specified");
    }

    // pay attention: aggregated range scan is not stateful, since we scan all tuples in one call
    MOT::AccessType accessMode = MOT::AccessType::RD;

    // begin the WHERE clause
    JitIndexScanDirection index_scan_direction = JIT_INDEX_SCAN_FORWARD;

    // build range iterators
    MOT_LOG_DEBUG("Generating range cursor for range SELECT sub-query");
    JitLlvmRuntimeCursor cursor =
        buildRangeCursor(ctx, &subPlan->_index_scan, JIT_RANGE_SCAN_SUB_QUERY, nullptr, subQueryIndex);
    if (cursor.begin_itr == nullptr) {
        JIT_TRACE_FAIL("Aggregate Range sub-SELECT", "Unsupported WHERE clause type");
        return false;
    }

    JIT_WHILE_BEGIN(cursor_aggregate_loop)
    llvm::Value* res = AddIsScanEnd(ctx, index_scan_direction, &cursor, JIT_RANGE_SCAN_SUB_QUERY, subQueryIndex);
    JIT_WHILE_EVAL_NOT(res);
    {
        llvm::Value* row = buildGetRowFromIterator(ctx,
            JIT_WHILE_COND_BLOCK(),
            JIT_WHILE_POST_BLOCK(),
            accessMode,
            subPlan->_index_scan._scan_direction,
            &cursor,
            JIT_RANGE_SCAN_SUB_QUERY,
            subQueryIndex);

        // check for additional filters, if not try to fetch next row
        if (!buildFilterRow(ctx, row, nullptr, &subPlan->_index_scan._filters, JIT_WHILE_COND_BLOCK())) {
            JIT_TRACE_FAIL("Aggregate Range sub-SELECT", "Unsupported filter");
            return false;
        }

        // aggregate into tuple (we use tuple's resno column as aggregated sum instead of defining local variable)
        // if row disqualified due to DISTINCT operator then go back to loop test block
        MOT_ASSERT(subPlan->m_aggCount <= 1);
        ctx->m_builder->CreateStore(JIT_CONST_INT32(0), aggCount, true);
        for (int i = 0; i < subPlan->m_aggCount; ++i) {
            if (!buildAggregateRow(ctx, &subPlan->m_aggregates[i], i, row, nullptr, aggCount)) {
                JIT_TRACE_FAIL("Aggregate Range sub-SELECT", "Unsupported aggregate");
                return false;
            }
        }
    }
    JIT_WHILE_END()

    // cleanup
    IssueDebugLog("Reached end of aggregate range select sub-query loop");
    AddDestroyCursor(ctx, &cursor);

    // wrap up aggregation and write to result tuple (even though this is unfitting to outer query tuple...)
    JIT_GOTO(build_agg_result);
    ctx->m_builder->SetInsertPoint(build_agg_result);
    buildAggregateResult(ctx, subPlan->m_aggregates, subPlan->m_aggCount);

    // coy aggregate result from outer query result tuple into sub-query result tuple
    AddCopyAggregateToSubQueryResult(ctx, subQueryIndex);

    return true;
}

static bool JitSubQueryCodeGen(JitLlvmCodeGenContext* ctx, JitCompoundPlan* plan, int subQueryIndex)
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

static MotJitContext* JitCompoundOuterSelectCodegen(JitLlvmCodeGenContext* ctx, Query* query, const char* queryString,
    JitSelectPlan* plan, JitCodegenStats& codegenStats)
{
    // begin the WHERE clause
    if (!buildPointScan(ctx, &plan->_query._search_exprs, JIT_RANGE_SCAN_MAIN, nullptr)) {
        JIT_TRACE_FAIL("Compound Outer-SELECT", "Unsupported WHERE clause type");
        return nullptr;
    }

    // fetch row for read
    MOT::AccessType access_mode = query->hasForUpdate ? MOT::AccessType::RD_FOR_UPDATE : MOT::AccessType::RD;
    llvm::Value* row = buildSearchRow(ctx, access_mode, JIT_RANGE_SCAN_MAIN);

    // check for additional filters
    if (!buildFilterRow(ctx, row, nullptr, &plan->_query._filters, nullptr)) {
        JIT_TRACE_FAIL("Compound Outer-SELECT", "Unsupported filter");
        return nullptr;
    }

    // now begin selecting columns into result
    IssueDebugLog("Selecting columns into result");
    if (!selectRowColumns(ctx, row, &plan->_select_exprs)) {
        JIT_TRACE_FAIL("Compound Outer-SELECT", "Failed to process target entry");
        return nullptr;
    }

    AddExecStoreVirtualTuple(ctx);

    // update number of rows processed
    buildIncrementRowsProcessed(ctx);

    // execute *tp_processed = rows_processed
    AddSetTpProcessed(ctx);

    // signal to envelope executor scan ended (this is a point query)
    AddSetScanEnded(ctx, 1);

    InjectProfileData(ctx, GetActiveNamespace(), queryString, false);

    // return success from calling function
    JIT_RETURN(JIT_CONST_INT32((int)MOT::RC_OK));

    // wrap up
    return FinalizeCodegen(ctx, JIT_COMMAND_COMPOUND_SELECT, queryString, codegenStats);
}

static MotJitContext* JitCompoundOuterCodegen(JitLlvmCodeGenContext* ctx, Query* query, const char* queryString,
    JitCompoundPlan* plan, JitCodegenStats& codegenStats)
{
    MotJitContext* jitContext = nullptr;
    if (plan->_command_type == JIT_COMMAND_SELECT) {
        jitContext = JitCompoundOuterSelectCodegen(
            ctx, query, queryString, (JitSelectPlan*)plan->_outer_query_plan, codegenStats);
    }
    // currently other outer query types are not supported
    return jitContext;
}

static MotJitContext* JitCompoundCodegen(JitLlvmCodeGenContext* ctx, Query* query, const char* query_string,
    JitCompoundPlan* plan, JitCodegenStats& codegenStats)
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

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedCompoundSelect");
    IssueDebugLog("Starting execution of jitted COMPOUND SELECT");

    // initialize rows_processed local variable (required for sub-queries)
    buildResetRowsProcessed(ctx);

    // generate code for sub-query execution
    uint32_t subQueryCount = 0;
    for (int i = 0; i < plan->_outer_query_plan->_query._search_exprs._count; ++i) {
        if (plan->_outer_query_plan->_query._search_exprs._exprs[i]._expr->_expr_type == JIT_EXPR_TYPE_SUBLINK) {
            JitSubLinkExpr* subLinkExpr =
                (JitSubLinkExpr*)plan->_outer_query_plan->_query._search_exprs._exprs[i]._expr;
            if (!JitSubQueryCodeGen(ctx, plan, subLinkExpr->_sub_query_index)) {
                JIT_TRACE_FAIL("Compound SELECT", "Failed to generate code for sub-query");
                return nullptr;
            }
            ++subQueryCount;
        }
    }

    // clear tuple early, so that we will have a null datum in case outer query finds nothing
    AddExecClearTuple(ctx);

    // generate code for the outer query
    MotJitContext* jitContext = JitCompoundOuterCodegen(ctx, query, query_string, plan, codegenStats);
    if (jitContext == nullptr) {
        JIT_TRACE_FAIL("Compound SELECT", "Failed to generate code for outer query");
        return nullptr;
    }

    // prepare sub-query data in resulting JIT context (for later execution)
    MOT_ASSERT(subQueryCount > 0);
    MOT_ASSERT(subQueryCount == plan->_sub_query_count);
    if ((subQueryCount > 0) && !PrepareSubQueryData((JitQueryContext*)jitContext, plan)) {
        JIT_TRACE_FAIL(
            "Compound SELECT", "Failed to prepare tuple table slot array for sub-queries in JIT context object");
        DestroyJitContext(jitContext);
        jitContext = nullptr;
    }

    return jitContext;
}

static MotJitContext* JitRangeScanCodegen(JitLlvmCodeGenContext* ctx, Query* query, const char* query_string,
    JitRangeScanPlan* plan, JitCodegenStats& codegenStats)
{
    MotJitContext* jit_context = nullptr;

    switch (plan->_command_type) {
        case JIT_COMMAND_UPDATE:
            jit_context = JitRangeUpdateCodegen(ctx, query, query_string, (JitRangeUpdatePlan*)plan, codegenStats);
            break;

        case JIT_COMMAND_SELECT: {
            JitRangeSelectPlan* range_select_plan = (JitRangeSelectPlan*)plan;
            if (range_select_plan->m_aggCount == 0) {
                jit_context = JitRangeSelectCodegen(ctx, query, query_string, range_select_plan, codegenStats);
            } else {
                jit_context = JitAggregateRangeSelectCodegen(ctx, query, query_string, range_select_plan, codegenStats);
            }
        } break;

        case JIT_COMMAND_DELETE:
            jit_context = JitRangeDeleteCodegen(ctx, query, query_string, (JitRangeDeletePlan*)plan, codegenStats);
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

static MotJitContext* JitPointQueryCodegen(JitLlvmCodeGenContext* ctx, Query* query, const char* query_string,
    JitPointQueryPlan* plan, JitCodegenStats& codegenStats)
{
    MotJitContext* jitContext = nullptr;
    switch (plan->_command_type) {
        case JIT_COMMAND_UPDATE:
            jitContext = JitUpdateCodegen(ctx, query, query_string, (JitUpdatePlan*)plan, codegenStats);
            break;

        case JIT_COMMAND_DELETE:
            jitContext = JitDeleteCodegen(ctx, query, query_string, (JitDeletePlan*)plan, codegenStats);
            break;

        case JIT_COMMAND_SELECT:
            jitContext = JitSelectCodegen(ctx, query, query_string, (JitSelectPlan*)plan, codegenStats);
            break;

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "Generate JIT Code",
                "Invalid point query JIT plan command type %d",
                (int)plan->_command_type);
            break;
    }
    return jitContext;
}

static MotJitContext* JitInvokeCodegen(JitLlvmCodeGenContext* ctx, Query* query, const char* queryString,
    JitInvokePlan* plan, JitCodegenStats& codegenStats)
{
    MOT_LOG_DEBUG("Generating code for invoke at thread %p", (void*)pthread_self());

    // prepare the jitted function (declare, get arguments into context and define locals)
    CreateJittedFunction(ctx, "MotJittedInvoke");
    IssueDebugLog("Starting execution of jitted INVOKE");

    IssueDebugLog("Preparing function call arguments");
    MOT_LOG_TRACE("Preparing %d function call arguments", plan->_arg_count);

    // prepare parameter list info
    llvm::Value* paramListInfo = AddGetInvokeParamListInfo(ctx);
    MOT_LOG_TRACE("Preparing %d plan arguments", plan->_arg_count);
    for (int i = 0; i < plan->_arg_count; ++i) {
        if (plan->_args[i]->_expr_type == JIT_EXPR_TYPE_PARAM) {
            ctx->m_paramInfo[i].m_mode = JitParamMode::JIT_PARAM_DIRECT;
            ctx->m_paramInfo[i].m_index = ((JitParamExpr*)plan->_args[i])->_param_id;
            continue;
        }
        ctx->m_paramInfo[i].m_mode = JitParamMode::JIT_PARAM_COPY;
        ctx->m_paramInfo[i].m_index = -1;
        llvm::Value* expr = ProcessExpr(ctx, nullptr, nullptr, plan->_args[i]);
        if (expr == nullptr) {
            JIT_TRACE_FAIL("INVOKE", "Failed to process parameter expression");
            return nullptr;
        }
        llvm::Value* exprIsNull = AddGetExprIsNull(ctx);
        AddSetParamValue(ctx, paramListInfo, JIT_CONST_INT32(i), plan->_args[i]->_result_type, expr, exprIsNull);
    }

    // pass default parameters if needed
    uint64_t paramCount = plan->m_functionPlan ? plan->m_functionPlan->m_argCount : get_func_nargs(plan->_function_id);
    if (plan->m_defaultParamCount > 0) {
        for (int i = 0; i < plan->m_defaultParamCount; ++i) {
            llvm::Value* expr = ProcessExpr(ctx, nullptr, nullptr, plan->m_defaultParams[i]);
            if (expr == nullptr) {
                JIT_TRACE_FAIL("INVOKE", "Failed to process default value expression");
                return nullptr;
            }
            llvm::Value* exprIsNull = AddGetExprIsNull(ctx);
            int paramPos = plan->_arg_count + i;
            Oid resultType = plan->m_defaultParams[i]->_result_type;
            AddSetParamValue(ctx, paramListInfo, JIT_CONST_INT32(paramPos), resultType, expr, exprIsNull);
            ctx->m_paramInfo[paramPos].m_mode = JitParamMode::JIT_PARAM_COPY;
            ctx->m_paramInfo[paramPos].m_index = -1;
        }
    }

    InjectInvokeProfileData(ctx, GetActiveNamespace(), queryString, true);
    // invoke the stored procedure
    llvm::Value* retValue = AddInvokeStoredProcedure(ctx);
    InjectInvokeProfileData(ctx, GetActiveNamespace(), queryString, false);

    // NOTE: in case of SP1 calling SP2, where SP2 threw unhandled exception, then there is no need here to throw
    // exception, because the sql code is pulled up into JIT context of SP1, where it is handled there

    InjectProfileData(ctx, GetActiveNamespace(), queryString, false);

    // return success from calling function
    JIT_RETURN(retValue);

    // wrap up
    JitQueryContext* jitContext = (JitQueryContext*)FinalizeCodegen(ctx, JIT_COMMAND_INVOKE, queryString, codegenStats);
    if (jitContext != nullptr) {
        // setup parameter info (for later preparing) - as much as the called function needs
        MOT_LOG_TRACE("Installing %d parameters in source context (%d used)", paramCount, plan->_arg_count);
        MOT_ASSERT((paramCount == 0 && ctx->m_paramInfo == nullptr) || (paramCount > 0 && ctx->m_paramInfo != nullptr));
        jitContext->m_invokeParamCount = paramCount;
        jitContext->m_invokeParamInfo = ctx->m_paramInfo;
        ctx->m_paramInfo = nullptr;
        // compile the called stored procedure if there is such
        if (plan->m_functionPlan != nullptr) {
            jitContext->m_invokedQueryString = nullptr;
            jitContext->m_invokedFunctionOid = InvalidOid;
            jitContext->m_invokedFunctionTxnId = InvalidTransactionId;
            jitContext->m_invokeContext =
                (JitFunctionContext*)ProcessInvokedPlan(plan->m_functionPlan, JIT_CONTEXT_GLOBAL_SECONDARY);
            if (jitContext->m_invokeContext == nullptr) {
                MOT_LOG_TRACE("Failed to generate code for invoked stored procedure: %s", plan->_function_name);
                DestroyJitContext(jitContext);
                return nullptr;
            }
            jitContext->m_invokeContext->m_parentContext = jitContext;
        } else {
            jitContext->m_invokedQueryString = MakeInvokedQueryString(plan->_function_name, plan->_function_id);
            if (jitContext->m_invokedQueryString == nullptr) {
                MOT_LOG_TRACE("Failed to prepare invoked query string for stored procedure: %s", plan->_function_name);
                DestroyJitContext(jitContext);
                return nullptr;
            }
            jitContext->m_invokedFunctionOid = plan->_function_id;
            jitContext->m_invokedFunctionTxnId = plan->m_functionTxnId;
            jitContext->m_invokeContext = nullptr;
        }
    }

    return jitContext;
}

static bool InitCodeGenContextByPlan(
    JitLlvmCodeGenContext* ctx, GsCodeGen* codeGen, GsCodeGen::LlvmBuilder* builder, JitPlan* plan)
{
    // ATTENTION: in case of failure the code-gen object is destroyed, otherwise it is owned by the context
    bool result = false;
    switch (plan->_plan_type) {
        case JIT_PLAN_INSERT_QUERY: {
            JitInsertPlan* insertPlan = (JitInsertPlan*)plan;
            MOT::Table* table = insertPlan->_table;
            result = InitCodeGenContext(ctx, codeGen, builder, table, table->GetPrimaryIndex());
            break;
        }

        case JIT_PLAN_POINT_QUERY: {
            JitPointQueryPlan* pqueryPlan = (JitPointQueryPlan*)plan;
            MOT::Table* table = pqueryPlan->_query._table;
            result = InitCodeGenContext(ctx, codeGen, builder, table, table->GetPrimaryIndex());
            break;
        }

        case JIT_PLAN_RANGE_SCAN: {
            JitRangeScanPlan* rscanPlan = (JitRangeScanPlan*)plan;
            MOT::Table* table = rscanPlan->_index_scan._table;
            MOT::Index* index = rscanPlan->_index_scan._index;
            result = InitCodeGenContext(ctx, codeGen, builder, table, index);
            break;
        }

        case JIT_PLAN_JOIN: {
            JitJoinPlan* joinPlan = (JitJoinPlan*)plan;
            MOT::Table* outerTable = joinPlan->_outer_scan._table;
            MOT::Index* outerIndex = joinPlan->_outer_scan._index;
            MOT::Table* innerTable = joinPlan->_inner_scan._table;
            MOT::Index* innerIndex = joinPlan->_inner_scan._index;
            result = InitCodeGenContext(ctx, codeGen, builder, outerTable, outerIndex, innerTable, innerIndex);
            break;
        }

        case JIT_PLAN_COMPOUND: {
            JitCompoundPlan* compoundPlan = (JitCompoundPlan*)plan;
            MOT::Table* table = compoundPlan->_outer_query_plan->_query._table;
            result = InitCompoundCodeGenContext(ctx, codeGen, builder, table, table->GetPrimaryIndex(), compoundPlan);
            break;
        }

        case JIT_PLAN_INVOKE: {
            JitInvokePlan* invokePlan = (JitInvokePlan*)plan;
            uint64_t paramCount = invokePlan->m_functionPlan ? invokePlan->m_functionPlan->m_argCount
                                                             : get_func_nargs(invokePlan->_function_id);
            result = InitCodeGenContext(ctx, codeGen, builder, nullptr, nullptr, nullptr, nullptr, (int)paramCount);
            break;
        }

        default:
            MOT_REPORT_ERROR(
                MOT_ERROR_INTERNAL, "Generate JIT Code", "Invalid JIT plan type %d", (int)plan->_plan_type);
            FreeGsCodeGen(codeGen);
            break;
    }

    return result;
}

MotJitContext* JitCodegenLlvmQuery(Query* query, const char* query_string, JitPlan* plan, JitCodegenStats& codegenStats)
{
    JIT_ASSERT_LLVM_CODEGEN_UTIL_VALID();
    GsCodeGen* codeGen = SetupCodegenEnv();
    if (codeGen == nullptr) {
        return nullptr;
    }
    GsCodeGen::LlvmBuilder builder(codeGen->context());

    volatile JitLlvmCodeGenContext ctx = {};
    if (!InitCodeGenContextByPlan((JitLlvmCodeGenContext*)&ctx, codeGen, &builder, plan)) {
        // ATTENTION: in case of error code-gen object is destroyed already
        MOT_LOG_TRACE("Failed to initialize code-gen context by plan");
        return nullptr;
    }
    ctx.m_queryString = query_string;

    // NOTE: every variable used after catch needs to be volatile (see longjmp() man page)
    volatile MotJitContext* jitContext = nullptr;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    MOT_LOG_DEBUG("*** Attempting to generate planned LLVM-jitted code for query: %s", query_string);
    PG_TRY();
    {
        switch (plan->_plan_type) {
            case JIT_PLAN_INSERT_QUERY:
                jitContext = JitInsertCodegen(
                    (JitLlvmCodeGenContext*)&ctx, query, query_string, (JitInsertPlan*)plan, codegenStats);
                break;

            case JIT_PLAN_POINT_QUERY:
                jitContext = JitPointQueryCodegen(
                    (JitLlvmCodeGenContext*)&ctx, query, query_string, (JitPointQueryPlan*)plan, codegenStats);
                break;

            case JIT_PLAN_RANGE_SCAN:
                jitContext = JitRangeScanCodegen(
                    (JitLlvmCodeGenContext*)&ctx, query, query_string, (JitRangeScanPlan*)plan, codegenStats);
                break;

            case JIT_PLAN_JOIN:
                jitContext =
                    JitJoinCodegen((JitLlvmCodeGenContext*)&ctx, query, query_string, (JitJoinPlan*)plan, codegenStats);
                break;

            case JIT_PLAN_COMPOUND:
                jitContext = JitCompoundCodegen(
                    (JitLlvmCodeGenContext*)&ctx, query, query_string, (JitCompoundPlan*)plan, codegenStats);
                break;

            case JIT_PLAN_INVOKE:
                jitContext = JitInvokeCodegen(
                    (JitLlvmCodeGenContext*)&ctx, query, query_string, (JitInvokePlan*)plan, codegenStats);
                break;

            default:
                MOT_REPORT_ERROR(
                    MOT_ERROR_INTERNAL, "Generate JIT Code", "Invalid JIT plan type %d", (int)plan->_plan_type);
                break;
        }
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(origCxt);
        PrintErrorInfo(query_string);
    }
    PG_END_TRY();

    // cleanup
    DestroyCodeGenContext((JitLlvmCodeGenContext*)&ctx);

    if (jitContext == nullptr) {
        MOT_LOG_TRACE("Failed to generate LLVM-jitted code for query: %s", query_string);
    } else {
        MOT_LOG_DEBUG(
            "Got LLVM-jitted function %p after compile, for query: %s", jitContext->m_llvmFunction, query_string);
    }

    // reset compile state for robustness
    llvm_util::JitResetCompileState();
    JIT_ASSERT_LLVM_CODEGEN_UTIL_VALID();
    return (MotJitContext*)jitContext;
}

extern int JitExecLlvmQuery(JitQueryContext* jitContext, ParamListInfo params, TupleTableSlot* slot,
    uint64_t* tuplesProcessed, int* scanEnded, int newScan)
{
    MOT_ASSERT(jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY);
    JitQueryExecState* execState = (JitQueryExecState*)jitContext->m_execState;

    int result = 0;

    MOT_LOG_TRACE("Calling sigsetjmp on faultBuf %p, query: %s", execState->m_faultBuf, jitContext->m_queryString);

    // we setup a jump buffer in the execution state for fault handling
    if (sigsetjmp(execState->m_faultBuf, 1) == 0) {
        // execute the jitted-function
        result = jitContext->m_llvmFunction(jitContext->m_table,
            jitContext->m_index,
            execState->m_searchKey,
            execState->m_bitmapSet,
            params,
            slot,
            tuplesProcessed,
            scanEnded,
            newScan,
            execState->m_endIteratorKey,
            jitContext->m_innerTable,
            jitContext->m_innerIndex,
            execState->m_innerSearchKey,
            execState->m_innerEndIteratorKey);
    } else {
        uint64_t faultCode = execState->m_exceptionValue;
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Execute JIT",
            "Encountered run-time fault %" PRIu64 " (%s), exception status: %" PRIu64 ", exception value: %" PRIu64,
            faultCode,
            llvm_util::LlvmRuntimeFaultToString(faultCode),
            execState->m_exceptionStatus,
            execState->m_exceptionValue);
        result = MOT::RC_JIT_SP_EXCEPTION;
    }

    MOT_LOG_TRACE("JitExecLlvmQuery returned %d/%u for query: %s", result, jitContext->m_rc, jitContext->m_queryString);
    return result;
}
}  // namespace JitExec
