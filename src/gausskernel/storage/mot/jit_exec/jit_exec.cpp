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
 * jit_exec.cpp
 *    Interface for JIT execution.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/jit_exec/jit_exec.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * ATTENTION:
 * 1. Be sure to include gscodegen.h before anything else to avoid clash with PM definition in datetime.h.
 * 2. Be sure to include libintl.h before gscodegen.h to avoid problem with gettext.
 */
#include "libintl.h"
#include "codegen/gscodegen.h"
#include "codegen/builtinscodegen.h"
#include "catalog/pg_operator.h"
#include "codegen/datecodegen.h"
#include "codegen/timestampcodegen.h"
#include "utils/fmgroids.h"
#include "nodes/parsenodes.h"
#include "storage/ipc.h"
#include "nodes/pg_list.h"
#include "tcop/dest.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "catalog/pg_aggregate.h"
#include "executor/spi.h"
#include "executor/executor.h"

#include "mot_internal.h"
#include "storage/mot/jit_exec.h"
#include "jit_common.h"
#include "jit_llvm_query_codegen.h"
#include "jit_llvm_sp_codegen.h"
#include "jit_source_pool.h"
#include "jit_source_map.h"
#include "jit_context_pool.h"
#include "jit_plan.h"
#include "jit_plan_sp.h"
#include "jit_statistics.h"

#include "mot_engine.h"
#include "utilities.h"
#include "catalog_column_types.h"
#include "mot_error.h"
#include "utilities.h"
#include "cycles.h"
#include "mot_atomic_ops.h"
#include "jit_profiler.h"

#include <cassert>

namespace JitExec {
DECLARE_LOGGER(LiteExecutor, JitExec);

#ifdef MOT_JIT_TEST
uint64_t totalQueryExecCount = 0;
uint64_t totalFunctionExecCount = 0;
static void UpdateTestStats(MotJitContext* jitContext, int newScan);
#endif

static void JITXactCallback(XactEvent event, void* arg);
static MotJitContext* MakeDummyContext(JitSource* jitSource, JitContextUsage usage);

inline void PrepareSessionAccess()
{
    // when running under thread-pool, it is possible to be executed from a thread that hasn't yet executed any MOT code
    // and thus lacking a thread id and NUMA node id. Nevertheless, we can be sure that a proper session context is set
    // up.
    (void)EnsureSafeThreadAccess();

    // since we use session local allocations and a session context might have not been created yet, we make sure such
    // one exists now
    (void)GetSafeTxn(__FUNCTION__);

    // make sure we get commit/rollback notifications in this session
    if (!u_sess->mot_cxt.jit_xact_callback_registered) {
        MOT_LOG_TRACE("Registering transaction call back for current session");
        RegisterXactCallback(JITXactCallback, nullptr);
        u_sess->mot_cxt.jit_xact_callback_registered = true;
    }
}

static bool PrepareIsJittable(const char* queryName, bool isFunction)
{
    // since this might be called through initdb, we make a safety check here
    if (MOT::MOTEngine::GetInstance() == nullptr) {
        return false;
    }

    // ensure all MOT thread/session-local identifiers are in place
    PrepareSessionAccess();

    // check limit not breached
    if (u_sess->mot_cxt.jit_context_count >= GetMotCodegenLimit()) {
        MOT_LOG_TRACE("%s %s is not jittable: Reached the maximum of %u JIT contexts per session",
            isFunction ? "Stored procedure" : "query",
            queryName,
            u_sess->mot_cxt.jit_context_count);
        JitStatisticsProvider::GetInstance().AddUnjittableLimitQuery();
        return false;
    }
    return true;
}

extern JitPlan* IsJittableQuery(Query* query, const char* queryString, bool forcePlan /* = false */)
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile JitPlan* jitPlan = nullptr;
    volatile MemoryContext origCxt = CurrentMemoryContext;

    // push currently parsed query
    void* prevQuery = u_sess->mot_cxt.jit_pg_query;
    u_sess->mot_cxt.jit_pg_query = query;

    PG_TRY();
    {
        if (!PrepareIsJittable(queryString, false)) {
            MOT_LOG_TRACE("IsJittableQuery(): Failed to prepare for query parsing");
        } else {
            // silently ignore other commands
            bool isSupported = ((query->commandType == CMD_UPDATE) || (query->commandType == CMD_INSERT) ||
                                (query->commandType == CMD_SELECT) || (query->commandType == CMD_DELETE));
            if (isSupported) {
                // first check if already exists in global source
                if (!forcePlan && ContainsReadyCachedJitSource(queryString, false, true)) {
                    MOT_LOG_TRACE("Query JIT source already exists, reporting query is jittable");
                    jitPlan = MOT_READY_JIT_PLAN;
                } else {
                    // either query never parsed or is expired, so we generate a plan
                    // print query
                    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
                        char* parsedQuery = nodeToString(query);
                        MOT_LOG_TRACE(
                            "Checking if query string is jittable: %s\nParsed query:\n%s\n", queryString, parsedQuery);
                        pfree(parsedQuery);
                    }

                    // prepare a new plan
                    jitPlan = JitPreparePlan(query, queryString);
                    if (jitPlan != nullptr && jitPlan != MOT_READY_JIT_PLAN) {
                        if (JitPlanHasDistinct((JitPlan*)jitPlan)) {
                            MOT_LOG_TRACE("Enabling plan with DISTINCT aggregate");
                            // In future, if we decide to disallow distinct operator, we just have to call
                            // JitDestroyPlan here.
                        }

                        // plan generated so query is jittable
                        MOT_LOG_TRACE("Query is jittable by plan");
                        if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
                            JitExplainPlan(query, (JitPlan*)jitPlan);
                        }
                    }
                }
            }

            // update statistics
            if (jitPlan == nullptr) {
                MOT_LOG_TRACE("Query is not jittable: %s", queryString);
                JitStatisticsProvider::GetInstance().AddUnjittableDisqualifiedQuery();
            } else {
                // whether generating or cloning code, this is a jittable query
                MOT_LOG_TRACE("Query is jittable: %s", queryString);
                JitStatisticsProvider::GetInstance().AddJittableQuery();
            }
        }
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while preparing JIT plan for query '%s': %s", queryString, edata->message);
        ereport(WARNING,
            (errmsg("Failed to parse query '%s' for MOT jitted execution.", queryString),
                errdetail("%s", edata->detail)));
        FlushErrorState();
        FreeErrorData(edata);
        if (jitPlan != nullptr) {
            JitDestroyPlan((JitPlan*)jitPlan);
            jitPlan = nullptr;
        }
    }
    PG_END_TRY();

    // pop currently parsed query
    u_sess->mot_cxt.jit_pg_query = prevQuery;

    if (jitPlan == nullptr) {
        MOT_LOG_TRACE("Query is not jittable: %s", queryString);
    }
    return (JitPlan*)jitPlan;
}

extern JitPlan* IsJittableFunction(
    PLpgSQL_function* function, HeapTuple procTuple, Oid functionOid, bool forcePlan /* = false */)
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile JitPlan* jitPlan = nullptr;
    volatile bool nsPushed = false;
    MOT::mot_string qualifiedFunctionName;
    volatile const char* qualifiedFunctionNameStr = "";
    char* functionName = "";

    ++u_sess->mot_cxt.jit_compile_depth;  // raise flag to guard SPI calls
    u_sess->mot_cxt.jit_parse_error = 0;
    PG_TRY();
    {
        bool procNameIsNull = false;
        bool procSrcIsNull = false;
        bool procIsStrictIsNull = false;
        Datum procNameDatum = SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_proname, &procNameIsNull);
        Datum procSrcDatum = SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_prosrc, &procSrcIsNull);
        Datum procIsStrictDatum = SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_proisstrict, &procIsStrictIsNull);
        bool hasNullAttr = (procNameIsNull || procSrcIsNull || procIsStrictIsNull);
        functionName = NameStr(*DatumGetName(procNameDatum));
        if (!PrepareIsJittable(functionName, true)) {
            MOT_LOG_TRACE("IsJittableQuery(): Failed to prepare for query parsing");
        } else if (hasNullAttr) {
            MOT_LOG_TRACE(
                "Stored procedure is not jittable: catalog entry for stored procedure contains null attributes");
        } else {
            // first check if already exists in global source (all functions defined in global name space)
            if (!qualifiedFunctionName.format("%s.%u", functionName, (unsigned)functionOid)) {
                MOT_LOG_TRACE("Failed to format qualified function name");
            } else {
                qualifiedFunctionNameStr = qualifiedFunctionName.c_str();
                if (!forcePlan && ContainsReadyCachedJitSource((const char*)qualifiedFunctionNameStr, true, true)) {
                    MOT_LOG_TRACE("Stored procedure JIT source already exists, reporting stored procedure is jittable");
                    jitPlan = MOT_READY_JIT_PLAN;
                } else {
                    // either function never parsed or is expired, so we generate a plan
                    if (PushJitSourceNamespace(functionOid, (const char*)qualifiedFunctionNameStr)) {
                        nsPushed = true;

                        // get required function attributes
                        char* functionSource = TextDatumGetCString(procSrcDatum);
                        bool isStrict = DatumGetBool(procIsStrictDatum);

                        // print query
                        if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
                            MOT_LOG_TRACE("Checking if function '%s' is jittable:\n%s\n",
                                qualifiedFunctionNameStr,
                                functionSource);
                            plpgsql_dumptree(function);
                        }

                        // prepare a new plan
                        jitPlan = JitPrepareFunctionPlan(function, functionSource, functionName, functionOid, isStrict);
                        if (jitPlan != nullptr) {
                            MOT_LOG_TRACE("Function %s is jittable by plan", qualifiedFunctionNameStr);
                            if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
                                JitExplainPlan(function, (JitPlan*)jitPlan);
                            }
                        }
                        PopJitSourceNamespace();
                        nsPushed = false;
                    }
                }

                // update statistics
                if (jitPlan == nullptr) {
                    MOT_LOG_TRACE("Function is not jittable: %s", qualifiedFunctionNameStr);
                    JitStatisticsProvider::GetInstance().AddUnjittableDisqualifiedQuery();
                } else {
                    // whether generating or cloning code, this is a jittable query
                    MOT_LOG_TRACE("Function is jittable: %s", qualifiedFunctionNameStr);
                    JitStatisticsProvider::GetInstance().AddJittableQuery();
                }
            }
        }
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while preparing JIT plan for stored procedure '%s': %s",
            qualifiedFunctionNameStr,
            edata->message);
        ereport(WARNING,
            (errmsg("Failed to parse function '%s' for MOT jitted execution.", qualifiedFunctionNameStr),
                errdetail("%s", edata->detail)));
        FlushErrorState();
        FreeErrorData(edata);
        if (nsPushed) {
            PopJitSourceNamespace();
        }
        if (jitPlan != nullptr) {
            JitDestroyPlan((JitPlan*)jitPlan);
            jitPlan = nullptr;
        }
    }
    PG_END_TRY();

    // decrement SPI guard counter
    --u_sess->mot_cxt.jit_compile_depth;

    if (jitPlan == nullptr) {
        if (u_sess->mot_cxt.jit_parse_error != 0) {
            MOT_LOG_WARN("Stored procedure is not jittable: %s", qualifiedFunctionNameStr);
        } else {
            MOT_LOG_TRACE("Stored procedure is not jittable: %s", qualifiedFunctionNameStr);
        }
    }
    return (JitPlan*)jitPlan;
}

extern bool IsInvokeReadyFunction(Query* query)
{
    // if function code generation is altogether disabled, then we silently fail
    if (!IsMotSPCodegenEnabled()) {
        return false;
    }
    if (query->commandType != CMD_SELECT) {
        MOT_LOG_DEBUG("IsInvokeReadyFunction(): Disqualifying invoke query - not a SELECT command");
        return false;
    }
    if (!CheckQueryAttributes(query, false, false, false)) {
        MOT_LOG_DEBUG("IsInvokeReadyFunction(): Disqualifying invoke query - Invalid query attributes");
        return false;
    }
    if ((query->jointree) && (query->jointree->fromlist || query->jointree->quals)) {
        MOT_LOG_DEBUG("IsInvokeReadyFunction(): Disqualifying invoke query - FROM clause is not empty");
        return false;
    }
    if (list_length(query->targetList) != 1) {
        MOT_LOG_DEBUG(
            "IsInvokeReadyFunction(): Disqualifying invoke query - target list does not contain exactly one entry");
        return false;
    }

    TargetEntry* targetEntry = (TargetEntry*)linitial(query->targetList);
    if (targetEntry->expr->type != T_FuncExpr) {
        MOT_LOG_DEBUG(
            "IsInvokeReadyFunction(): Disqualifying invoke query - single target entry is not a function expression");
        return false;
    }

    // get function name first
    FuncExpr* funcExpr = (FuncExpr*)targetEntry->expr;
    char* funcName = get_func_name(funcExpr->funcid);
    if (funcName == nullptr) {
        MOT_LOG_TRACE("IsInvokeReadyFunction(): Disqualifying invoke query - Failed to find function name for function "
                      "id %u",
            (unsigned)funcExpr->funcid);
        return false;
    }
    MOT::mot_string qualifiedFunctionName;
    if (!qualifiedFunctionName.format("%s.%u", funcName, (unsigned)funcExpr->funcid)) {
        MOT_LOG_TRACE("IsInvokeReadyFunction(): Disqualifying invoke query - Failed to format qualified function name "
                      "for function %s",
            funcName);
        pfree(funcName);
        return false;
    }
    bool result = ContainsReadyCachedJitSource(qualifiedFunctionName.c_str(), true);
    if (!result) {
        MOT_LOG_TRACE("IsInvokeReadyFunction(): Disqualifying invoke query - could not find a ready context for "
                      "function %s",
            funcName);
    }
    pfree(funcName);
    return result;
}

static void ProcessErrorResult(MOT::RC result, MotJitContext* jitContext, int newScan)
{
    if (result == MOT::RC_JIT_SP_EXCEPTION) {
        if (IsJitSubContextInline(jitContext)) {
#ifdef MOT_JIT_TEST
            if (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY && newScan) {
                UpdateTestStats(jitContext, newScan);
            }
#endif
            return;
        }

        const char* errorDetail = nullptr;
        const char* errorHint = nullptr;
        bytea* txt = DatumGetByteaP(jitContext->m_execState->m_errorMessage);
        const char* errorMessage = (const char*)VARDATA(txt);
        if (jitContext->m_execState->m_errorDetail != 0) {
            bytea* txtd = DatumGetByteaP(jitContext->m_execState->m_errorDetail);
            errorDetail = (const char*)VARDATA(txtd);
        }
        if (jitContext->m_execState->m_errorHint != 0) {
            bytea* txth = DatumGetByteaP(jitContext->m_execState->m_errorHint);
            errorHint = (const char*)VARDATA(txth);
        }

        RaiseEreport(jitContext->m_execState->m_sqlState, errorMessage, errorDetail, errorHint);
    }

    if (result != MOT::RC_OK) {
        void* arg1 = nullptr;
        void* arg2 = nullptr;

        MOT::TxnManager* currTxn = u_sess->mot_cxt.jit_txn;

        // prepare message argument for error report
        ColumnDef stub;
        if (result == MOT::RC_UNIQUE_VIOLATION) {
            arg1 = (void*)(currTxn->m_errIx ? currTxn->m_errIx->GetName().c_str() : "");
            arg2 = (void*)(currTxn->m_errMsgBuf);
        } else if (result == MOT::RC_NULL_VIOLATION) {
            MOT::Table* table = jitContext->m_execState->m_nullViolationTable;
            stub.colname = table->GetField((uint64_t)jitContext->m_execState->m_nullColumnId)->m_name;
            arg1 = (void*)&stub;
            arg2 = (void*)table;
        }

        if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
            MOT_LOG_ERROR_STACK("Failed to execute jitted function with error code: %d (%s), query is: %s",
                (int)result,
                MOT::RcToString(result),
                jitContext->m_queryString);
        }
        if (result == MOT::RC_ABORT) {
            JitStatisticsProvider::GetInstance().AddAbortExecQuery();
        } else {
            JitStatisticsProvider::GetInstance().AddFailExecQuery();
        }
        report_pg_error(result, (void*)arg1, (void*)arg2);
    }
}

static void ProcessJitResult(MOT::RC result, MotJitContext* jitContext, int newScan)
{
    // the result is being returned from jitted query
    if (result == MOT::RC_LOCAL_ROW_NOT_FOUND && jitContext->m_rc != MOT::RC_OK) {
        switch (jitContext->m_rc) {
            case MOT::RC_SERIALIZATION_FAILURE:
            case MOT::RC_MEMORY_ALLOCATION_ERROR:
            case MOT::RC_ABORT:
                result = jitContext->m_rc;
                jitContext->m_rc = MOT::RC_OK;
                break;

            default:
                MOT_LOG_ERROR("ProcessJitResult(): Unsupported error (%s), changing result to ABORT",
                    RcToString(jitContext->m_rc));
                MOT_ASSERT(false);
                result = MOT::RC_ABORT;
                jitContext->m_rc = MOT::RC_OK;
        }
    }

    // we want to make sure that jitContext->m_rc was already handled. jitContext->m_rc != RC_OK is an unexpected
    // behavior.
    MOT_ASSERT(jitContext->m_rc == MOT::RC_OK);
    // NOTE: errors might be reported in a better way, so this part can be reviewed sometime
    // we ignore "local row not found" in SELECT and DELETE scenarios
    if (result == MOT::RC_LOCAL_ROW_NOT_FOUND) {
        switch (jitContext->m_commandType) {
            case JIT_COMMAND_DELETE:
            case JIT_COMMAND_SELECT:
            case JIT_COMMAND_RANGE_SELECT:
            case JIT_COMMAND_FULL_SELECT:
            case JIT_COMMAND_POINT_JOIN:
            case JIT_COMMAND_RANGE_JOIN:
            case JIT_COMMAND_COMPOUND_SELECT:
            case JIT_COMMAND_UPDATE:
            case JIT_COMMAND_RANGE_UPDATE:
            case JIT_COMMAND_RANGE_DELETE:
                // this is considered as successful execution
                JitStatisticsProvider::GetInstance().AddInvokeQuery();
                if (newScan) {
#ifdef MOT_JIT_TEST
                    UpdateTestStats(jitContext, newScan);
#endif
                    JitStatisticsProvider::GetInstance().AddExecQuery();
                }
                return;
            default:
                break;
        }
    }

    ProcessErrorResult(result, jitContext, newScan);
}

static MotJitContext* ProcessGeneratedJitContext(JitSource* jitSource, MotJitContext* sourceJitContext,
    const char* queryString, JitCodegenStats* codegenStats, JitContextUsage usage)
{
    JitCodegenState newState = JitCodegenState::JIT_CODEGEN_NONE;
    if (sourceJitContext == nullptr) {
        // notify error for all waiters - this query will never again be JITTed - cleanup only during database shutdown
        MOT_LOG_TRACE("Failed to generate code for query with JIT source %p, signaling error context for query: %s",
            jitSource,
            queryString);
        SetJitSourceError(jitSource, MOT::GetRootError(), &newState);
        if (newState == JitCodegenState::JIT_CODEGEN_DEPRECATE) {
            // ATTENTION: Do not use the jitSource after this point, as it might be removed from the deprecated list
            // and freed by other sessions.
            CleanupConcurrentlyDroppedSPSource(queryString);
        }
        JitStatisticsProvider::GetInstance().AddCodeGenErrorQuery();
        return nullptr;
    }

    MOT_LOG_TRACE("Generated JIT context %p for query: '%s', codegenTime: %" PRIu64 " micros",
        sourceJitContext,
        queryString,
        codegenStats->m_codegenTime);

    bool isSPGlobalSource = false;
    if ((sourceJitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) &&
        (jitSource->m_usage == JIT_CONTEXT_GLOBAL)) {
        isSPGlobalSource = true;
    }

    if (!SetJitSourceReady(jitSource, sourceJitContext, codegenStats, &newState)) {
        // Two possible causes:
        // 1. Stored procedure was dropped and deprecated when we were compiling. In this case, we just fail
        // JIT code generation to trigger re-validation.
        // 2. Illegal state transition error in JIT source (internal bug), there is already a JIT context present
        // in the JIT source (maybe generated by another session, although impossible).
        // So we just fail JIT for this session (other sessions may still benefit from existing JIT context).
        // Pay attention that we cannot replace the JIT context in the JIT source, since
        // the JIT function in it is probably still being used by other sessions
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_STATE,
            "JIT Compile",
            "Failed to set ready source context %p to JIT source %p (newState %s), disqualifying query: %s",
            sourceJitContext,
            jitSource,
            JitCodegenStateToString(newState),
            queryString);
        if (newState == JitCodegenState::JIT_CODEGEN_DEPRECATE) {
            // ATTENTION: Do not use the jitSource after this point, as it might be removed from the deprecated list
            // and freed by other sessions.
            CleanupConcurrentlyDroppedSPSource(queryString);
        }
        DestroyJitContext(sourceJitContext);
        JitStatisticsProvider::GetInstance().AddCodeGenErrorQuery();
        return nullptr;
    }

    MOT_LOG_TRACE("Installed ready JIT source context %p to JIT source %p (newState %s) for query: %s",
        sourceJitContext,
        jitSource,
        JitCodegenStateToString(newState),
        queryString);

    bool needPruning = false;
    MotJitContext* jitContext = nullptr;
    if (isSPGlobalSource) {
        // If this is a global SP source, lock the source map for pruning.
        LockJitSourceMap();
    }

    LockJitSource(jitSource);
    // attention: the source context has its JIT source set up properly, so clone can use it
    JitCodegenState codegenState = jitSource->m_codegenState;
    if (jitSource->m_sourceJitContext == sourceJitContext) {
        if (codegenState == JitCodegenState::JIT_CODEGEN_READY) {
            if ((jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) &&
                (jitSource->m_usage == JIT_CONTEXT_GLOBAL)) {
                needPruning = true;
                MOT_ASSERT(isSPGlobalSource);
            }
            jitContext = CloneJitContext(sourceJitContext, usage);
        } else if (codegenState == JitCodegenState::JIT_CODEGEN_UNAVAILABLE ||
                   codegenState == JitCodegenState::JIT_CODEGEN_PENDING) {
            jitContext = MakeDummyContext(jitSource, usage);
        } else {
            MOT_LOG_TRACE("Cannot clone context from JIT source %p after code-generation, unexpected state %s: %s",
                jitSource,
                JitCodegenStateToString(codegenState),
                jitSource->m_queryString);
        }
    } else {
        MOT_LOG_TRACE("JIT source %p (codegenState %s) context %p changed concurrently to %p after code-generation",
            jitSource,
            JitCodegenStateToString(codegenState),
            sourceJitContext,
            jitSource->m_sourceJitContext);
    }
    UnlockJitSource(jitSource);

    if (isSPGlobalSource) {
        if (needPruning) {
            MOT_LOG_TRACE("Pruning global ready function source %p after code-generation: %s",
                jitSource,
                jitSource->m_queryString);
            PruneNamespace(jitSource);
        }
        UnlockJitSourceMap();
    }

    if (jitContext == nullptr) {
        // we keep the source JIT context intact in the JIT source, maybe some time later we will have resources for
        // cloning.
        MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_UNAVAILABLE,
            "JIT Compile",
            "Failed to clone ready source context %p from JIT source %p (codegenState %s), disqualifying query: %s",
            sourceJitContext,
            jitSource,
            JitCodegenStateToString(codegenState),
            queryString);
        JitStatisticsProvider::GetInstance().AddCodeCloneErrorQuery();
        return nullptr;
    }

    MOT_LOG_TRACE("Registered JIT context %p in JIT source %p for cleanup", jitContext, jitSource);

    // update statistics
    JitStatisticsProvider& instance = JitStatisticsProvider::GetInstance();
    instance.AddCodeGenTime(codegenStats->m_codegenTime);
    instance.AddCodeGenQuery();
    instance.AddCodeCloneQuery();

    return jitContext;
}

static MotJitContext* GenerateQueryJitContext(
    Query* query, const char* queryString, JitPlan* jitPlan, JitSource* jitSource, JitContextUsage usage)
{
    MotJitContext* sourceJitContext = nullptr;
    JitCodegenStats codegenStats = {};

    uint64_t startTime = GetSysClock();
    MOT_LOG_TRACE("Generating LLVM JIT context for query: %s", queryString);
    sourceJitContext = JitCodegenLlvmQuery(query, queryString, jitPlan, codegenStats);
    uint64_t endTime = GetSysClock();
    uint64_t timeMicros = MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime);
    codegenStats.m_codegenTime = timeMicros;

    return ProcessGeneratedJitContext(jitSource, sourceJitContext, queryString, &codegenStats, usage);
}

static MotJitContext* GenerateFunctionJitContext(PLpgSQL_function* function, HeapTuple procTuple, Oid functionOid,
    ReturnSetInfo* returnSetInfo, const char* queryString, JitPlan* jitPlan, JitSource* jitSource,
    JitContextUsage usage)
{
    MotJitContext* sourceJitContext = nullptr;
    JitCodegenStats codegenStats = {};
    uint64_t startTime = GetSysClock();
    MOT_LOG_TRACE("Generating LLVM JIT context for function: %s", queryString);
    sourceJitContext = JitCodegenLlvmFunction(function, procTuple, functionOid, returnSetInfo, jitPlan, codegenStats);
    uint64_t endTime = GetSysClock();
    uint64_t timeMicros = MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime);
    codegenStats.m_codegenTime = timeMicros;

    return ProcessGeneratedJitContext(jitSource, sourceJitContext, queryString, &codegenStats, usage);
}

static MotJitContext* MakeDummyContext(JitSource* jitSource, JitContextUsage usage)
{
    JitQueryContext* jitContext = (JitQueryContext*)AllocJitContext(usage, jitSource->m_contextType);
    if (jitContext != nullptr) {
        jitContext->m_contextType = jitSource->m_contextType;
        jitContext->m_commandType = jitSource->m_commandType;
        jitContext->m_validState = JIT_CONTEXT_PENDING_COMPILE;
        AddJitSourceContext(jitSource, jitContext);
    }
    return jitContext;
}

static void SetupJitSourceForCodegen(JitSource* jitSource, JitContextType contextType, JitPlan* jitPlan)
{
    LockJitSource(jitSource);
    MOT_ASSERT(jitSource->m_codegenState == JitCodegenState::JIT_CODEGEN_UNAVAILABLE);
    MOT_ASSERT(jitPlan != MOT_READY_JIT_PLAN);  // we must have a ready plan
    jitSource->m_contextType = contextType;
    jitSource->m_commandType = jitPlan->_command_type;
    if ((jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) &&
        (jitSource->m_commandType == JIT_COMMAND_INVOKE)) {
        jitSource->m_functionOid = ((JitInvokePlan*)jitPlan)->_function_id;
    } else if (jitSource->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) {
        jitSource->m_functionOid = ((JitFunctionPlan*)jitPlan)->_function_id;
    }
    UnlockJitSource(jitSource);
}

template <typename Generator>
static MotJitContext* JitCodegen(
    const char* queryName, JitPlan* jitPlan, Generator& generator, bool globalNamespace, JitContextUsage usage)
{
    MotJitContext* jitContext = nullptr;

    MOT_LOG_TRACE("*** Generating cached code for query: %s", queryName);
    MOT_ASSERT(jitPlan);

    // search for a cached JIT source - either use an existing one or generate a new one
    JitSource* jitSource = nullptr;
    do {                     // instead of goto
        LockJitSourceMap();  // jit-source already exists, so wait for it to become ready
        jitSource = GetCachedJitSource(queryName, globalNamespace);
        if (jitSource != nullptr) {
            MOT_LOG_TRACE("Found a jit-source %p", jitSource);
            UnlockJitSourceMap();

            // ATTENTION: JIT source cannot get expired at this phase, since DDL statements (that might cause the
            // JIT Source to expire) cannot run in parallel with other statements
            // Note: cached context was found, but maybe it is still being compiled, so we wait for it to be ready
            MOT_LOG_TRACE("Waiting for context to be ready: %s", queryName);
            JitCodegenState codegenState = GetReadyJitContext(jitSource, &jitContext, usage, jitPlan);
            if (codegenState == JitCodegenState::JIT_CODEGEN_READY) {
                MOT_LOG_TRACE("Collected ready context: %s", queryName);
            } else if (codegenState == JitCodegenState::JIT_CODEGEN_EXPIRED) {
                // regenerate JIT context, install it and notify
                MOT_LOG_TRACE("Regenerating real JIT code for expired context: %s", queryName);
                // we must prepare the analysis variables again (because we did not call IsJittable())
                // in addition, table/index definition might have change so we must re-analyze
                if (jitPlan == MOT_READY_JIT_PLAN) {
                    jitPlan = generator.IsJittable();
                }
                if (jitPlan == nullptr) {
                    MOT_LOG_TRACE("Failed to re-analyze expired JIT source, notifying error status: %s", queryName);
                    SetJitSourceError(jitSource, MOT_ERROR_INTERNAL);
                } else {
                    JitFunctionPlan* functionPlan = nullptr;
                    if (JitPlanHasFunctionPlan(jitPlan, &functionPlan)) {
                        // fn_xmin equal to m_functionTxnId is possible, if the JIT source moved from ERROR to
                        // UNAVAILABLE/EXPIRED in GetReadyJitContext.
                        MOT_ASSERT((functionPlan->m_function->fn_xmin >= jitSource->m_functionTxnId) &&
                                   (functionPlan->m_function->fn_xmin >= jitSource->m_expireTxnId));
                    }
                    SetupJitSourceForCodegen(jitSource, generator.GetContextType(), jitPlan);
                    jitContext = generator.GenerateJitContext(jitSource, jitPlan, usage);
                }
            } else if ((codegenState == JitCodegenState::JIT_CODEGEN_UNAVAILABLE) ||
                       (codegenState == JitCodegenState::JIT_CODEGEN_PENDING)) {
                // prepare a dummy context
                jitContext = MakeDummyContext(jitSource, usage);
                MOT_LOG_TRACE("Generated dummy context %p for query: %s", jitContext, queryName);
            } else {  // code generation (by another session) failed, or source was deprecated
                MOT_LOG_TRACE("Cached context status is not ready: %s", queryName);
                break;  // goto cleanup
            }
        } else {
            // JIT source is not ready, so we need to generate code.
            MOT_LOG_TRACE("JIT-source not found, generating code for query: %s", queryName);

            if (generator.GetContextType() == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) {
                JitFunctionPlan* functionPlan = (JitFunctionPlan*)jitPlan;
                Oid functionOid = functionPlan->_function_id;
                if (IsDroppedSP(functionOid)) {
                    MOT_LOG_ERROR("Function is already deprecated, skipping premature re-validation: %s", queryName);
                    UnlockJitSourceMap();
                    break;  // goto cleanup
                }
            }

            // we allocate an empty cached entry and install it in the global map, other threads can wait until it
            // is ready (thus only 1 thread regenerates code)
            JitContextUsage sourceUsage =
                (u_sess->mot_cxt.jit_session_source_map != nullptr) ? JIT_CONTEXT_LOCAL : JIT_CONTEXT_GLOBAL;
            jitSource = AllocJitSource(queryName, sourceUsage);
            if (jitSource == nullptr) {
                MOT_LOG_TRACE("Skipping query code generation: Reached total maximum of JIT source objects %d",
                    GetMotCodegenLimit());
                UnlockJitSourceMap();
                break;  // goto cleanup
            }

            MOT_ASSERT(jitPlan != MOT_READY_JIT_PLAN);  // we must have a ready plan

            SetupJitSourceForCodegen(jitSource, generator.GetContextType(), jitPlan);

            MOT_LOG_TRACE(
                "Created jit-source object %p (contextType %s, commandType %s, functionOid %u) during code-gen: %s",
                jitSource,
                JitContextTypeToString(jitSource->m_contextType),
                CommandToString(jitSource->m_commandType),
                jitSource->m_functionOid,
                queryName);

            if (!AddCachedJitSource(jitSource, globalNamespace)) {
                // unexpected: entry already exists (this is a bug)
                MOT_LOG_TRACE("Failed to add jit-source object to map");
                UnlockJitSourceMap();
                DestroyJitSource(jitSource);
                jitSource = nullptr;
                break;  // goto cleanup
            }

            UnlockJitSourceMap();  // let other operations continue

            // generate JIT context, install it and notify
            MOT_LOG_TRACE("Generating JIT code");
            jitContext = generator.GenerateJitContext(jitSource, jitPlan, usage);
        }
    } while (0);

    return jitContext;
}

struct JitQueryGenerator {
public:
    JitQueryGenerator(Query* query, const char* queryString) : m_query(query), m_queryString(queryString)
    {}

    inline JitPlan* IsJittable()
    {
        return IsJittableQuery(m_query, m_queryString, true);
    }

    inline MotJitContext* GenerateJitContext(JitSource* jitSource, JitPlan* jitPlan, JitContextUsage usage)
    {
        return GenerateQueryJitContext(m_query, m_queryString, jitPlan, jitSource, usage);
    }

    inline JitContextType GetContextType() const
    {
        return JitContextType::JIT_CONTEXT_TYPE_QUERY;
    }

private:
    Query* m_query;
    const char* m_queryString;
};

extern MotJitContext* JitCodegenQuery(Query* query, const char* queryString, JitPlan* jitPlan, JitContextUsage usage)
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile MotJitContext* result = nullptr;

    // push currently parsed query
    void* prevQuery = u_sess->mot_cxt.jit_pg_query;
    u_sess->mot_cxt.jit_pg_query = query;
    u_sess->mot_cxt.jit_codegen_error = 0;

    PG_TRY();
    {
        MOT_LOG_TRACE("Generating code for query: %s", queryString);
        uint64_t startTime = GetSysClock();
        JitQueryGenerator generator(query, queryString);
        result = JitCodegen(queryString, jitPlan, generator, false, usage);
        if (result != nullptr) {
            uint64_t endTime = GetSysClock();
            uint64_t timeMicros = MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime);
            MOT_LOG_TRACE("Code generation time %" PRIu64 " micros for query: %s", timeMicros, queryString);
        }
    }
    PG_CATCH();
    {
        // cleanup
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while generating code for query '%s': %s", queryString, edata->message);
        ereport(WARNING,
            (errmsg("Failed to generate code for query '%s' for MOT jitted execution.", queryString),
                errdetail("%s", edata->detail)));
        u_sess->mot_cxt.jit_codegen_error = edata->sqlerrcode;
        FreeErrorData(edata);
        FlushErrorState();
    }
    PG_END_TRY();

    // pop currently parsed query
    u_sess->mot_cxt.jit_pg_query = prevQuery;

    if (result == nullptr) {
        MOT_LOG_WARN("Failed to JIT-compile query: %s", queryString);
    }
    return (MotJitContext*)result;
}

extern MotJitContext* TryJitCodegenQuery(Query* query, const char* queryString)
{
    MotJitContext* jitContext = nullptr;

    // make sure SPI is set-up on top level call
    SPIAutoConnect spiAutoConn;
    if (!spiAutoConn.IsConnected()) {
        int rc = spiAutoConn.GetErrorCode();
        MOT_LOG_TRACE("Failed to connect to SPI while generating code for query: %s (%u)",
            queryString,
            SPI_result_code_string(rc),
            rc);
        return nullptr;
    }

    JitPlan* jitPlan = IsJittableQuery(query, queryString);
    if (jitPlan != nullptr) {
        jitContext = JitCodegenQuery(query, queryString, jitPlan, JIT_CONTEXT_LOCAL);
        if (jitContext == nullptr) {
            MOT_LOG_TRACE("Failed to generate jitted MOT code for query: %s", queryString);
        }
        JitDestroyPlan(jitPlan);
    }
    return jitContext;
}

struct JitFunctionGenerator {
public:
    JitFunctionGenerator(PLpgSQL_function* function, HeapTuple procTuple, Oid functionOid, ReturnSetInfo* returnSetInfo,
        const char* queryString)
        : m_function(function),
          m_procTuple(procTuple),
          m_functionOid(functionOid),
          m_returnSetInfo(returnSetInfo),
          m_queryString(queryString)
    {}

    inline JitPlan* IsJittable()
    {
        return IsJittableFunction(m_function, m_procTuple, m_functionOid, true);
    }

    inline MotJitContext* GenerateJitContext(JitSource* jitSource, JitPlan* jitPlan, JitContextUsage usage)
    {
        return GenerateFunctionJitContext(
            m_function, m_procTuple, m_functionOid, m_returnSetInfo, m_queryString, jitPlan, jitSource, usage);
    }

    inline JitContextType GetContextType() const
    {
        return JitContextType::JIT_CONTEXT_TYPE_FUNCTION;
    }

private:
    PLpgSQL_function* m_function;
    HeapTuple m_procTuple;
    Oid m_functionOid;
    ReturnSetInfo* m_returnSetInfo;
    const char* m_queryString;
};

extern MotJitContext* JitCodegenFunction(PLpgSQL_function* function, HeapTuple procTuple, Oid functionOid,
    ReturnSetInfo* returnSetInfo, JitPlan* jitPlan, JitContextUsage usage)
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile MotJitContext* result = nullptr;
    volatile bool nsPushed = false;
    MOT::mot_string qualifiedFunctionName;
    volatile const char* qualifiedFunctionNameStr = "";
    char* functionName = "";

    u_sess->mot_cxt.jit_codegen_error = 0;

    PG_TRY();
    {
        bool procNameIsNull = false;
        Datum procNameDatum = SysCacheGetAttr(PROCOID, procTuple, Anum_pg_proc_proname, &procNameIsNull);
        if (procNameIsNull) {
            MOT_LOG_TRACE(
                "Stored procedure is not jittable: catalog entry for stored procedure contains null attributes");
        } else {
            // generate function context sub-queries under name-space
            functionName = NameStr(*DatumGetName(procNameDatum));
            if (!qualifiedFunctionName.format("%s.%u", functionName, (unsigned)functionOid)) {
                MOT_LOG_TRACE("Failed to format qualified function name");
            } else {
                qualifiedFunctionNameStr = qualifiedFunctionName.c_str();
                if (PushJitSourceNamespace(functionOid, (const char*)qualifiedFunctionNameStr)) {
                    nsPushed = true;
                    MOT_LOG_TRACE("Generating code for function: %s", qualifiedFunctionNameStr);
                    uint64_t startTime = GetSysClock();
                    JitFunctionGenerator generator(
                        function, procTuple, functionOid, returnSetInfo, (const char*)qualifiedFunctionNameStr);
                    result = JitCodegen((const char*)qualifiedFunctionNameStr, jitPlan, generator, true, usage);
                    if (result != nullptr) {
                        uint64_t endTime = GetSysClock();
                        uint64_t timeMicros = MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime);
                        MOT_LOG_TRACE("Code generation time %" PRIu64 " micros for function: %s",
                            timeMicros,
                            qualifiedFunctionNameStr);
                    }
                }
            }
        }
    }
    PG_CATCH();
    {
        // cleanup
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while generating code for stored procedure %s: %s",
            qualifiedFunctionNameStr,
            edata->message);
        ereport(WARNING,
            (errmsg("Failed to generate code for stored procedure '%s' for MOT jitted execution.",
                 qualifiedFunctionNameStr),
                errdetail("%s", edata->detail)));
        u_sess->mot_cxt.jit_codegen_error = edata->sqlerrcode;
        FreeErrorData(edata);
        FlushErrorState();
    }
    PG_END_TRY();

    if (nsPushed) {
        PopJitSourceNamespace();
    }

    if (result == nullptr) {
        MOT_LOG_WARN("Failed to JIT-compile stored procedure: %s", qualifiedFunctionNameStr);
    }
    return (MotJitContext*)result;
}

extern void JitResetScan(MotJitContext* jitContext)
{
    // ATTENTION: due to order of events in exec_bind_message() (JitResetScan is called before TryRevalidateJitContext)
    // it is possible that the current JIT context is in temporary compile state, but is about to be undergo successful
    // revalidation, so we do not want to miss out the reset-scan operation in this case.
    // Therefore, we allow reset-scan for any context with valid execution state.
    MOT_LOG_DEBUG("JitResetScan(): Resetting iteration count for context %p", jitContext);
    MOT_ASSERT(jitContext->m_contextType != JitContextType::JIT_CONTEXT_TYPE_INVALID);
    if (jitContext->m_execState != nullptr) {
        if (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
            JitQueryExecState* execState = (JitQueryExecState*)jitContext->m_execState;
            if (execState != nullptr) {
                execState->m_iterCount = 0;
            }
        } else if (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION) {
            JitFunctionExecState* execState = (JitFunctionExecState*)jitContext->m_execState;
            if (execState != nullptr) {
                if (execState->m_currentSubQueryId >= 0) {
                    JitFunctionContext* funcContext = (JitFunctionContext*)jitContext;
                    MotJitContext* activeQueryContext =
                        funcContext->m_SPSubQueryList[execState->m_currentSubQueryId].m_queryContext;
                    if (activeQueryContext != nullptr) {
                        JitResetScan(activeQueryContext);
                    }
                }
            }
        }
    }
}

static MOT::RC ValidatePrepare(MotJitContext* jitContext)
{
    // make sure we can execute (guard against crash due to logical error)
    bool missingFunction = false;
    if (!jitContext->m_llvmFunction && !jitContext->m_llvmSPFunction) {
        missingFunction = true;
    }
    if (missingFunction) {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
            "Execute JIT",
            "Cannot execute jitted function: function is null. Aborting transaction.");
        JitStatisticsProvider::GetInstance().AddFailExecQuery();
        if (IsJitSubContextInline(jitContext)) {
            return MOT::RC_ERROR;
        }
        report_pg_error(MOT::RC_ERROR, NULL);  // execution control ends, calls ereport(error,...)
    }

    if (!IsJitSubContextInline(jitContext)) {
        // Ensure that MOT FDW routine and Xact callbacks are registered.
        if (!u_sess->mot_cxt.callbacks_set) {
            ForeignDataWrapper* fdw = GetForeignDataWrapperByName(MOT_FDW, false);
            if (fdw != NULL) {
                (void)GetFdwRoutine(fdw->fdwhandler);
            }
        }

        // Ensure all MOT thread/session-local identifiers are in place.
        PrepareSessionAccess();
    }

    // make sure we have a valid transaction object (independent of MOT FDW)
    u_sess->mot_cxt.jit_txn = u_sess->mot_cxt.txn_manager;
    if (u_sess->mot_cxt.jit_txn == nullptr) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Execute JIT",
            "Cannot execute jitted code: Current transaction is undefined. Aborting transaction.");
        JitStatisticsProvider::GetInstance().AddFailExecQuery();
        if (IsJitSubContextInline(jitContext)) {
            return MOT::RC_MEMORY_ALLOCATION_ERROR;
        }
        report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR, NULL);  // execution control ends, calls ereport
    }

    if (u_sess->mot_cxt.jit_txn->IsTxnAborted()) {
        raiseAbortTxnError();
    }

    return MOT::RC_OK;
}

static int JitPrepareExec(MotJitContext* jitContext)
{
    if (!IsJitSubContextInline(jitContext)) {
        MOT_LOG_TRACE("Executing JIT context %p with query/function: %s", jitContext, jitContext->m_queryString);
    } else {
        MOT_LOG_DEBUG("Executing JIT context %p with query/function: %s", jitContext, jitContext->m_queryString);
    }

    MOT::RC result = ValidatePrepare(jitContext);
    if (result != MOT::RC_OK) {
        return result;
    }

    // during the very first invocation of the query we need to setup the reusable search keys
    // This is also true after TRUNCATE TABLE, in which case we also need to re-fetch all index objects
    bool needPrepare = ((jitContext->m_execState == nullptr) || MOT_ATOMIC_LOAD(jitContext->m_execState->m_purged));
    if (needPrepare) {
        u_sess->mot_cxt.jit_codegen_error = 0;
        if (!PrepareJitContext(jitContext)) {
            int errorCode = MOT_ERROR_OOM;
            if (u_sess->mot_cxt.jit_codegen_error == ERRCODE_QUERY_CANCELED) {
                errorCode = MOT_ERROR_STATEMENT_CANCELED;
            } else {
                int rootErrorCode = MOT::GetRootError();
                if (rootErrorCode != MOT_NO_ERROR) {
                    MOT::RC rootRC = MOT::ErrorToRC(rootErrorCode);
                    MOT_LOG_TRACE("Root error is %d %s, translated to %d %s",
                        rootErrorCode,
                        MOT::ErrorCodeToString(rootErrorCode),
                        rootRC,
                        MOT::RcToString(rootRC));
                    errorCode = rootErrorCode;
                }
            }
            result = MOT::ErrorToRC(errorCode);
            MOT_REPORT_ERROR(
                errorCode, "Execute JIT", "Failed to prepare for executing jitted code, aborting transaction");
            JitStatisticsProvider::GetInstance().AddFailExecQuery();
            if (IsJitSubContextInline(jitContext)) {
                return result;
            }
            report_pg_error(result, NULL);  // execution control ends, calls ereport
        }
    }

    if (!IsJitSubContextInline(jitContext)) {
        u_sess->mot_cxt.jit_txn->SetIsolationLevel(u_sess->utils_cxt.XactIsoLevel);
    }

    // each query should start with m_rc = OK to avoid reporting wrong query result due to previous run
    jitContext->m_rc = MOT::RC_OK;

    return MOT::RC_OK;
}

#ifdef MOT_JIT_DEBUG
#define MOT_JIT_DEBUG_EXEC_COUNT 2

static bool DebugPrintQueryExecStats(MotJitContext* jitContext, JitQueryExecState* execState)
{
    bool debugExec = false;
    if ((++execState->m_execCount <= MOT_JIT_DEBUG_EXEC_COUNT) && MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
        MOT_LOG_TRACE("Executing JIT query context %p (exec: %" PRIu64 ", query: %" PRIu64 ", iteration: %" PRIu64
                      "): %s",
            jitContext,
            execState->m_execCount,
            execState->m_queryCount,
            execState->m_iterCount,
            jitContext->m_queryString);
        debugExec = true;
    }
    return debugExec;
}

static bool DebugPrintFunctionExecStats(MotJitContext* jitContext, JitFunctionExecState* execState)
{
    bool debugExec = false;
    if ((++execState->m_execCount <= MOT_JIT_DEBUG_EXEC_COUNT) && MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
        MOT_LOG_TRACE("Executing JIT function context %p (exec: %" PRIu64 "): %s",
            jitContext,
            execState->m_execCount,
            jitContext->m_queryString);
        debugExec = true;
    }
    return debugExec;
}
#endif

#ifdef MOT_JIT_TEST
static void UpdateTestStats(MotJitContext* jitContext, int newScan)
{
    if (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY) {
        if (newScan) {
            MOT_ATOMIC_INC(totalQueryExecCount);
            MOT_LOG_DEBUG("Updated query count to %" PRIu64 ": %s", totalQueryExecCount, jitContext->m_queryString);
        } else {
            MOT_LOG_DEBUG("Skipped updated query count, not a new scan: %s", jitContext->m_queryString);
        }
    } else {
        MOT_ATOMIC_INC(totalFunctionExecCount);
        MOT_LOG_DEBUG("Updated function count to %" PRIu64 ": %s", totalFunctionExecCount, jitContext->m_queryString);
    }
}
#endif

#ifdef MOT_JIT_DEBUG
static void JitExecWrapUp(MotJitContext* jitContext, MotJitContext* prevContext, int result, int newScan,
    bool debugExec, MOT::LogLevel prevLevel)
#else
static void JitExecWrapUp(MotJitContext* jitContext, MotJitContext* prevContext, int result, int newScan)
#endif
{
#ifdef MOT_JIT_DEBUG
    if (debugExec && !IsJitSubContextInline(jitContext)) {
        (void)MOT::SetLogComponentLogLevel("JitExec", prevLevel);
    }
#endif

    // restore previous JIT context
    if (prevContext) {
        // pull up error information
        prevContext->m_execState->m_nullColumnId = u_sess->mot_cxt.jit_context->m_execState->m_nullColumnId;
        prevContext->m_execState->m_nullViolationTable = u_sess->mot_cxt.jit_context->m_execState->m_nullViolationTable;
    }
    u_sess->mot_cxt.jit_context = prevContext;

    if (result == 0 && jitContext->m_rc != MOT::RC_OK) {
        // in case query result is OK, we need to verify that no error was raised while processing it.
        MOT_LOG_TRACE(
            "JitExecWrapUp(): setting result to (%s), jitContext->m_rc = %u, result = %u, jitContext = %p, query: (%s)",
            RcToString(jitContext->m_rc),
            jitContext->m_rc,
            result,
            jitContext,
            jitContext->m_queryString);
        result = jitContext->m_rc;
        jitContext->m_rc = MOT::RC_OK;
    }

    if (result == 0) {
        // update statistics
        JitStatisticsProvider::GetInstance().AddInvokeQuery();
#ifdef MOT_JIT_TEST
        UpdateTestStats(jitContext, newScan);
#endif
        JitStatisticsProvider::GetInstance().AddExecQuery();
    } else {
        ProcessJitResult((MOT::RC)result, jitContext, newScan);
    }
}

#ifdef MOT_JIT_DEBUG
static void DebugPrintParams(ParamListInfo params, JitCommandType commandType)
{
    int paramCount = params ? params->numParams : 0;
    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_DEBUG)) {
        MOT_LOG_DEBUG("Executing %s command with %d parameters: ", CommandToString(commandType), paramCount);
        for (int i = 0; i < paramCount; ++i) {
            if (!params->params[i].isnull) {
                MOT_LOG_BEGIN(MOT::LogLevel::LL_DEBUG, "Param %d: ", i);
                PrintDatum(MOT::LogLevel::LL_DEBUG,
                    params->params[i].ptype,
                    params->params[i].value,
                    params->params[i].isnull);
                MOT_LOG_END(MOT::LogLevel::LL_DEBUG);
            }
        }
    }
}
#define DEBUG_PRINT_PARAMS(params, commandType) DebugPrintParams(params, commandType)
#else
#define DEBUG_PRINT_PARAMS(params, commandType)
#endif

static inline int JitExecGetNextTuple(JitQueryContext* jitContext, ParamListInfo params, TupleTableSlot* slot,
    uint64_t* tuplesProcessed, int* scanEnded, int newScan)
{
    int result = MOT::RC_OK;

    if (jitContext->m_llvmFunction != nullptr) {
#ifdef MOT_JIT_DEBUG
        MOT_LOG_DEBUG("JitExecGetNextTuple(): Executing LLVM-jitted function %p: %s",
            jitContext->m_llvmFunction,
            jitContext->m_queryString);
#endif
        result = JitExecLlvmQuery(jitContext, params, slot, tuplesProcessed, scanEnded, newScan);
    } else {
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("Cannot execute LLVM function, function is missing: %s", jitContext->m_queryString)));
    }

    return result;
}

static inline int JitExecCreateNonNativeTupleSort(MotJitContext* jitContext,
    JitNonNativeSortExecState* jitNonNativeSortExecState, ParamListInfo params, TupleTableSlot* slot)
{
    JitQueryContext* queryContext = (JitQueryContext*)jitContext;
    JitNonNativeSortParams* sortParams = queryContext->m_nonNativeSortParams;

    int64 sortMem = SET_NODEMEM(0, 0);
    int64 maxMem = 0;  // No limit

    // Iterator's direction is consistent over single query
    bool randomAccess = false;

    // Slot for my result tuples
    TupleDesc tupDesc = slot->tts_tupleDescriptor;

    if (!tupDesc) {
        MOT_LOG_TRACE("JitExecCreateNonNativeTupleSort(): Tuple descriptor (from slot) is NULL. slot: (%p)", slot);
        MOT_LOG_ERROR("Tuple descriptor is invalid");

        return MOT::RC_ERROR;
    }

    // The parallel num of plan. Not relvant for MOT as this stage
    int dop = SET_DOP(0);

    // Session memory context is used (instead of query context) as the allocated tupleSortState might be released only
    // when the jit context is destroyed (In case the user didnt pull all results. e.g. In batch mode, the user can stop
    // the query in the middle). In this case, when using query memory context, we will try to free\destroy a memory
    // that was already freed
    MemoryContext oldCxt = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
    Tuplesortstate* tupleSortState = tuplesort_begin_heap(tupDesc,
        sortParams->numCols,
        sortParams->sortColIdx,
        sortParams->sortOperators,
        sortParams->collations,
        sortParams->nullsFirst,
        sortMem,
        randomAccess,
        maxMem,
        sortParams->plan_node_id,
        dop);

    (void)MemoryContextSwitchTo(oldCxt);
    if (sortParams->bound) {
        tuplesort_set_bound(tupleSortState, sortParams->bound);
    }

    // local variables for retrieving all tuples
    int scanEnded = 0;
    uint64_t tuplesProcessed = 0;
    int newScan = 1;
    int result = MOT::RC_OK;
    uint64_t numRetrievedTuples = 0;  // For debug only

    // Scan the index and feed all the tuples to tuplesort
    while (true) {
        tuplesProcessed = 0;
        result = JitExecGetNextTuple(queryContext, params, slot, &tuplesProcessed, &scanEnded, newScan);

        if (scanEnded) {
            ExecClearTuple(slot);
            MOT_LOG_DEBUG(
                "JitExecCreateNonNativeTupleSort(): Scan has ended. Total tuples received: %lu", numRetrievedTuples);
            break;
        }

        if (result != MOT::RC_OK || tuplesProcessed == 0) {
            MOT_LOG_TRACE("JitExecCreateNonNativeTupleSort(): Failed to retrieve all tuples. Tuples retrieved: %lu, "
                          "l_tuplesProcessed: %lu, jitContext->m_llvmFunction: %p, query: %s",
                numRetrievedTuples,
                tuplesProcessed,
                jitContext->m_llvmFunction,
                jitContext->m_queryString);
            MOT_LOG_ERROR("Failed to retrieve all tuples. Tuples retrieved: %lu, query: %s",
                numRetrievedTuples,
                (jitContext->m_queryString ? jitContext->m_queryString : "null"));

            ExecClearTuple(slot);
            tuplesort_end(tupleSortState);
            tupleSortState = nullptr;
            // this is an error state. MOT::RC_LOCAL_ROW_NOT_FOUND might not interpreted as error, so we need to
            // replace it with MOT::RC_ERROR.
            return (result == MOT::RC_LOCAL_ROW_NOT_FOUND ? MOT::RC_ERROR : result);
        }

        // if PGXC is defined, tuplesort_puttupleslotontape is used in some case (instead of tuplesort_puttupleslot).
        // We will need to address this issue in the future.
        tuplesort_puttupleslot(tupleSortState, slot);
        numRetrievedTuples++;
        newScan = 0;
    }

    sort_count(tupleSortState);

    // Complete the sort
    tuplesort_performsort(tupleSortState);

    jitNonNativeSortExecState->m_tupleSort = tupleSortState;

    return MOT::RC_OK;
}

static inline bool JitExecOrderedQueryIsLimitReached(
    JitNonNativeSortParams* sortParams, JitQueryExecState* jitQueryExecState)
{
    if (sortParams->bound && jitQueryExecState->m_limitCounter >= (uint32_t)sortParams->bound) {
        return true;
    }

    return false;
}

static int JitExecOrderedQuery(MotJitContext* jitContext, JitQueryExecState* jitQueryExecState, ParamListInfo params,
    TupleTableSlot* slot, uint64_t* tuplesProcessed, int* scanEnded, int newScan)
{
    JitNonNativeSortExecState* jitNonNativeSortExecState = jitQueryExecState->m_nonNativeSortExecState;
    JitQueryContext* queryContext = (JitQueryContext*)jitContext;
    JitNonNativeSortParams* sortParams = queryContext->m_nonNativeSortParams;

    MOT_ASSERT(jitNonNativeSortExecState);

    if (newScan) {
        MOT_LOG_TRACE("JitExecOrderedQuery(): Initializing JitNonNativeSortExecState for new sort for query: (%s)",
            jitContext->m_queryString);

        // If tupleSortState already exists, we need to re-init the sort state
        if (jitNonNativeSortExecState->m_tupleSort) {
            tuplesort_end(jitNonNativeSortExecState->m_tupleSort);
            jitNonNativeSortExecState->m_tupleSort = nullptr;
        }

        int result = JitExecCreateNonNativeTupleSort(jitContext, jitNonNativeSortExecState, params, slot);
        if (result != MOT::RC_OK) {
            MOT_LOG_ERROR("Failed to create Tuple sort state for non native sort");
            return result;
        }

        jitQueryExecState->m_limitCounter = 0;
    }

    if (!jitNonNativeSortExecState->m_tupleSort) {
        // This is not a new scan (newScan == 0) and we dont have a tupleSortState probably because we already destroyed
        // it after setting *scanEnded = 1
        MOT_LOG_ERROR("Tuple was requested after scanEnded flag was raised. newScan %d", newScan);
        *scanEnded = 1;
        *tuplesProcessed = 0;
        return MOT::RC_LOCAL_ROW_NOT_FOUND;
    }

    // If limit clause, enforce number of returned tuples before get next tuple from tuplesort.
    // Update scanEnded if last tuple was already provided
    ExecClearTuple(slot);
    if (JitExecOrderedQueryIsLimitReached(sortParams, jitQueryExecState) ||
        !(tuplesort_gettupleslot(
            jitNonNativeSortExecState->m_tupleSort, ScanDirectionIsForward(sortParams->scanDir), slot, NULL))) {
        MOT_LOG_TRACE("JitExecOrderedQuery(): limit reached or tuple sort is empty");
        tuplesort_end(jitNonNativeSortExecState->m_tupleSort);
        jitNonNativeSortExecState->m_tupleSort = nullptr;
        *scanEnded = 1;
        *tuplesProcessed = 0;
        return MOT::RC_LOCAL_ROW_NOT_FOUND;
    }

    MOT_LOG_DEBUG("JitExecOrderedQuery(): returning next tuple from tuple sort");
    *tuplesProcessed = 1;
    *scanEnded = 0;
    if (sortParams->bound) {
        // Increase tuple counter to support limit clause
        jitQueryExecState->m_limitCounter++;
    }

    return MOT::RC_OK;
}

extern int JitExecQuery(
    MotJitContext* jitContext, ParamListInfo params, TupleTableSlot* slot, uint64_t* tuplesProcessed, int* scanEnded)
{
    MOT_ASSERT(jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY);

    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile JitQueryContext* queryContext = (JitQueryContext*)jitContext;

    volatile int result = JitPrepareExec(jitContext);
    if (result != MOT::RC_OK) {
        return result;
    }

    // setup current JIT context
    volatile MotJitContext* prevContext = u_sess->mot_cxt.jit_context;
    u_sess->mot_cxt.jit_context = jitContext;

    // we avoid weird stuff by putting null in case of internal error
    bool isQueryContext = (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_QUERY);
    JitQueryExecState* execState = isQueryContext ? (JitQueryExecState*)jitContext->m_execState : nullptr;
    MOT_ASSERT(execState);
#ifdef MOT_JIT_DEBUG
    // in trace log-level we raise the log level to DEBUG on first few executions only
    volatile MOT::LogLevel prevLevel = MOT::LogLevel::LL_INFO;
    volatile bool debugExec = DebugPrintQueryExecStats(jitContext, execState);
    if (debugExec) {
        prevLevel = MOT::SetLogComponentLogLevel("JitExec", MOT::LogLevel::LL_DEBUG);
    }
#endif

    // update iteration count and identify a new scan
    volatile int newScan = 0;
    if (execState->m_iterCount == 0) {
        ++execState->m_queryCount;
        newScan = 1;
#ifdef MOT_JIT_DEBUG
        MOT_LOG_TRACE("Starting a new scan (exec: %" PRIu64 ", query: %" PRIu64 ", iter: %" PRIu64 ") for query %s",
            execState->m_execCount,
            execState->m_queryCount,
            execState->m_iterCount,
            jitContext->m_queryString);
#endif
    }
    ++execState->m_iterCount;

    // prepare invoke query parameters (invoked stored procedure reports results directly into caller's variables)
    if (jitContext->m_commandType == JIT_COMMAND_INVOKE) {
        execState->m_directParams = params;
        execState->m_invokeSlot = slot;
        execState->m_invokeTuplesProcessed = tuplesProcessed;
        execState->m_invokeScanEnded = scanEnded;
    }
    DEBUG_PRINT_PARAMS(params, jitContext->m_commandType);

    // clear error stack before new execution
    if (!IsJitSubContextInline(jitContext)) {
        MOT::ClearErrorStack();
    }

    // reset error state before new execution
    ResetErrorState(execState);

    // invoke the jitted function
    volatile MemoryContext origCxt = CurrentMemoryContext;

    // init out params
    *scanEnded = 0;
    *tuplesProcessed = 0;

    PG_TRY();
    {
        if (queryContext->m_commandType != JIT_COMMAND_INVOKE) {
            MOT_ASSERT(queryContext->m_commandType != JIT_COMMAND_FUNCTION);
        }
        if (IsWriteCommand(queryContext->m_commandType)) {
            (void)::GetCurrentTransactionId();
        }
        if (queryContext->m_nonNativeSortParams) {
            result = JitExecOrderedQuery(jitContext, execState, params, slot, tuplesProcessed, scanEnded, newScan);
        } else {
            result =
                JitExecGetNextTuple((JitQueryContext*)queryContext, params, slot, tuplesProcessed, scanEnded, newScan);
        }
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN(
            "Caught exception while executing jitted query '%s': %s", queryContext->m_queryString, edata->message);
        ereport(WARNING,
            (errmsg("Failed to execute MOT-jitted query '%s'.", queryContext->m_queryString),
                errdetail("%s", edata->detail)));
        FlushErrorState();
        FreeErrorData(edata);
        result = (int)MOT::RC_JIT_SP_EXCEPTION;
    }
    PG_END_TRY();

#ifdef MOT_JIT_DEBUG
    JitExecWrapUp((MotJitContext*)jitContext, (MotJitContext*)prevContext, result, newScan, debugExec, prevLevel);
#else
    JitExecWrapUp((MotJitContext*)jitContext, (MotJitContext*)prevContext, result, newScan);
#endif
    return result;
}

static void CopyTupleTableSlot(TupleTableSlot* tuple)
{
    // switch to memory context of caller
    MemoryContext oldCtx = SwitchToSPICallerContext();

    // copy slot values to memory context of caller
    TupleDesc tupDesc = tuple->tts_tupleDescriptor;
    for (int i = 0; i < tupDesc->natts; ++i) {
        // skip dropped columns in destination
        if (tupDesc->attrs[i].attisdropped) {
            continue;
        }

        bool isNull = tuple->tts_isnull[i];
        Datum value = tuple->tts_values[i];
        Oid type = tuple->tts_tupleDescriptor->attrs[i].atttypid;

        // perform proper type conversion as in exec_assign_value() at pl_exec.cpp
        MOT_LOG_DEBUG("CopyTupleTableSlot(): Copying datum %d of type %u", i, type);
        Datum resValue = CopyDatum(value, type, isNull);
        tuple->tts_values[i] = resValue;
    }

    // restore current memory context
    if (oldCtx) {
        (void)MemoryContextSwitchTo(oldCtx);
    }
}

extern int JitExecFunction(
    MotJitContext* jitContext, ParamListInfo params, TupleTableSlot* slot, uint64_t* tuplesProcessed, int* scanEnded)
{
    // every variable used after catch needs to be volatile (see longjmp() man page)
    MOT_ASSERT(jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION);
    volatile JitFunctionContext* functionContext = (JitFunctionContext*)jitContext;

    volatile int result = JitPrepareExec(jitContext);
    if (result != MOT::RC_OK) {
        return result;
    }

    // we avoid weird stuff by putting null in case of internal error
    bool isFunctionContext = (jitContext->m_contextType == JitContextType::JIT_CONTEXT_TYPE_FUNCTION);
    volatile JitFunctionExecState* execState =
        isFunctionContext ? (JitFunctionExecState*)jitContext->m_execState : nullptr;
    MOT_ASSERT(execState);
    execState->m_spiBlockId = 0;  // reset each time before execution
#ifdef MOT_JIT_DEBUG
    // in trace log-level we raise the log level to DEBUG on first few executions only
    volatile MOT::LogLevel prevLevel = MOT::LogLevel::LL_INFO;
    volatile bool debugExec = DebugPrintFunctionExecStats(jitContext, (JitFunctionExecState*)execState);
    if (debugExec) {
        prevLevel = MOT::SetLogComponentLogLevel("JitExec", MOT::LogLevel::LL_DEBUG);
    }
#endif

    // save parameters with which the function was invoked so they can later be pushed down to sub-queries
    execState->m_functionParams = params;

    // reset error state
    ResetErrorState((JitExec::JitExecState*)execState);
    execState->m_exceptionOrigin = JIT_EXCEPTION_INTERNAL;

    // clear error stack before new execution
    if (!IsJitSubContextInline(jitContext)) {
        MOT::ClearErrorStack();
    }
    DEBUG_PRINT_PARAMS(params, jitContext->m_commandType);

    // connect to SPI
    SPIAutoConnect spiAutoConn(false, functionContext->m_functionOid);
    if (!spiAutoConn.IsConnected()) {
        int rc = spiAutoConn.GetErrorCode();
        MOT_LOG_ERROR("SPI_connect failed with error code %d: %s when executing MOT jitted function %s.",
            rc,
            SPI_result_code_string(rc),
            jitContext->m_queryString);
        ereport(ERROR,
            (errmodule(MOD_MOT),
                errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("SPI_connect failed with error code %d: %s when executing MOT jitted function %s.",
                    rc,
                    SPI_result_code_string(rc),
                    jitContext->m_queryString)));
    }

    // setup current JIT context
    volatile MotJitContext* prevContext = u_sess->mot_cxt.jit_context;
    u_sess->mot_cxt.jit_context = jitContext;

    // invoke the jitted function
    volatile MemoryContext origCxt = CurrentMemoryContext;
    // since we use parts of the PG function in query invocation, we need to make sure it does not get deleted
    ++execState->m_function->use_count;
    MOT_LOG_DEBUG("JitExecFunction(): Increased use count of function %p to %lu: %s",
        execState->m_function,
        execState->m_function->use_count,
        jitContext->m_queryString);
    PG_TRY();
    {
        if (jitContext->m_llvmSPFunction != nullptr) {
#ifdef MOT_JIT_DEBUG
            MOT_LOG_DEBUG(
                "Executing LLVM-jitted function %p: %s", jitContext->m_llvmSPFunction, jitContext->m_queryString);
#endif
            result =
                JitExecLlvmFunction((JitFunctionContext*)functionContext, params, slot, tuplesProcessed, scanEnded);
        } else {
            ereport(ERROR,
                (errmodule(MOD_MOT),
                    errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("Cannot execute LLVM function, function is missing: %s", jitContext->m_queryString)));
        }

        if (result == MOT::RC_OK) {
            // copy tuple to caller's memory context
            CopyTupleTableSlot(slot);
        }
    }
    PG_CATCH();
    {
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while executing jitted function '%s': %s",
            functionContext->m_queryString,
            edata->message);
        ereport(WARNING,
            (errmodule(MOD_MOT),
                errmsg("Failed to execute MOT-jitted stored procedure '%s'.", functionContext->m_queryString),
                errdetail("%s", edata->detail)));
        FlushErrorState();
        FreeErrorData(edata);

        // clean up: roll back all open sub-transactions, and open SPI connections
        JitReleaseAllSubTransactions(true, InvalidSubTransactionId);
        while (execState->m_spiBlockId > 0) {
            JitCleanupBlockAfterException();
        }
        result = (int)MOT::RC_ERROR;
    }
    PG_END_TRY();
    --execState->m_function->use_count;
    MOT_LOG_DEBUG("JitExecFunction(): Decreased use count of function %p to %lu: %s",
        execState->m_function,
        execState->m_function->use_count,
        jitContext->m_queryString);

    // disconnect from SPI - this includes handling any error and intermediate SPI state
    spiAutoConn.Disconnect();

#ifdef MOT_JIT_DEBUG
    JitExecWrapUp((MotJitContext*)jitContext, (MotJitContext*)prevContext, result, 1, debugExec, prevLevel);
#else
    JitExecWrapUp((MotJitContext*)jitContext, (MotJitContext*)prevContext, result, 1);
#endif
    return result;
}

extern void PurgeJitSourceCache(
    uint64_t objectId, JitPurgeScope purgeScope, JitPurgeAction purgeAction, const char* funcName)
{
    // since this might be called through initdb, we make a safety check here
    if (MOT::MOTEngine::GetInstance() == nullptr) {
        return;
    }

    // ensure all MOT thread/session-local identifiers are in place
    PrepareSessionAccess();

    MOT_LOG_TRACE("Purging JIT source map by object id %" PRIu64 " scope: %s, action: %s",
        objectId,
        JitPurgeScopeToString(purgeScope),
        JitPurgeActionToString(purgeAction));
    PurgeJitSourceMap(objectId, purgeScope, purgeAction, funcName);
}

extern MotJitDetail* MOTGetJitDetail(uint32_t* num)
{
    *num = 0;
    if (!IsMotCodegenEnabled()) {
        return nullptr;
    }

    // ensure all MOT thread/session-local identifiers are in place
    PrepareSessionAccess();

    MOT_LOG_DEBUG("Getting mot_jit_detail() information");
    return GetJitSourceDetail(num);
}

extern MotJitProfile* MOTGetJitProfile(uint32_t* num)
{
    *num = 0;
    if (!IsMotCodegenEnabled() || !MOT::GetGlobalConfiguration().m_enableCodegenProfile) {
        return nullptr;
    }

    // ensure all MOT thread/session-local identifiers are in place
    PrepareSessionAccess();

    MOT_LOG_DEBUG("Getting mot_jit_profile() information");
    return JitProfiler::GetInstance()->GetProfileReport(num);
}

class JitQueryRegenerator : public JitFunctionQueryVisitor {
public:
    JitQueryRegenerator(
        PLpgSQL_function* function, JitFunctionContext* functionContext, FuncParamInfoList* paramInfoList)
        : m_function(function), m_functionContext(functionContext), m_paramInfoList(paramInfoList)
    {}

    ~JitQueryRegenerator() final
    {}

    JitVisitResult OnQuery(PLpgSQL_expr* expr, PLpgSQL_row* row, int index, bool into) final
    {
        // 1. find corresponding sub-query context
        // 2. check if it is invalid
        // 3. regenerate code if required
        JitCallSite* callSite = nullptr;
        uint32_t subQueryIndex = (uint32_t)-1;
        for (uint32_t i = 0; i < m_functionContext->m_SPSubQueryCount; ++i) {
            if (m_functionContext->m_SPSubQueryList[i].m_exprIndex == index) {
                callSite = &m_functionContext->m_SPSubQueryList[i];
                subQueryIndex = i;
                MOT_LOG_TRACE(
                    "Found expression in sub-query %u/%u: %s", i, m_functionContext->m_SPSubQueryCount, expr->query);
                break;
            }
        }
        if (callSite == nullptr) {
            MOT_LOG_TRACE("Expression %d at %p to be regenerated not found: %s", index, expr, expr->query);
            return JitVisitResult::JIT_VISIT_CONTINUE;  // go on, that's fine
        }
        MOT_ASSERT(subQueryIndex != (uint32_t)-1);
        MotJitContext* subQueryContext = callSite->m_queryContext;
        if (subQueryContext == nullptr) {
            if (strcmp(expr->query, callSite->m_queryString) != 0) {
                MOT_LOG_TRACE("Mismatching non-jittable sub-query %u: %s", subQueryIndex, expr->query);
                return JitVisitResult::JIT_VISIT_ERROR;
            }
            return JitVisitResult::JIT_VISIT_CONTINUE;
        }
        if ((subQueryContext->m_queryString != nullptr) && (strcmp(expr->query, subQueryContext->m_queryString) != 0)) {
            MOT_LOG_TRACE("Mismatching jittable sub-query %u: %s", subQueryIndex, expr->query);
            return JitVisitResult::JIT_VISIT_ERROR;
        }
        uint8_t validState = MOT_ATOMIC_LOAD(subQueryContext->m_validState);
        if (validState == JIT_CONTEXT_VALID) {
            return JitVisitResult::JIT_VISIT_CONTINUE;
        } else if (validState == JIT_CONTEXT_PENDING_COMPILE) {
            MOT_LOG_TRACE("Encountered dummy sub-query %u pending for compilation: %s", subQueryIndex, expr->query);
            return JitVisitResult::JIT_VISIT_CONTINUE;
        }

        // in either one of these cases we regenerate code for query:
        // 1. if query itself was invalidated
        // 2. if this is an invoke query with directly invalid sub-SP
        bool regenCode = false;
        if (validState & JIT_CONTEXT_INVALID) {
            MOT_LOG_TRACE("Regenerating invalid sub-query: %s", subQueryContext->m_queryString);
            regenCode = true;
        } else if (validState & JIT_CONTEXT_DEPRECATE) {
            MOT_LOG_TRACE("Regenerating deprecate sub-query: %s", subQueryContext->m_queryString);
            regenCode = true;
        } else if (validState & JIT_CONTEXT_CHILD_SP_INVALID) {
            MOT_LOG_TRACE("Inspecting sub-query with invalid sub-SP: %s", subQueryContext->m_queryString);
            MOT_ASSERT(subQueryContext->m_commandType == JIT_COMMAND_INVOKE);
            MotJitContext* invokedContext = ((JitQueryContext*)subQueryContext)->m_invokeContext;
            if (invokedContext == nullptr) {
                MOT_LOG_TRACE("Regenerating sub-query with null sub-SP: %s", subQueryContext->m_queryString);
                regenCode = true;
            } else if (MOT_ATOMIC_LOAD(invokedContext->m_validState) & JIT_CONTEXT_INVALID) {
                MOT_LOG_TRACE("Regenerating sub-query with invalid sub-SP: %s", subQueryContext->m_queryString);
                regenCode = true;
            }
        }
        if (regenCode) {
            // we now generate code as usual
            MOT_LOG_TRACE("Regenerating code for invalid sub-query: %s", expr->query);
            if (!RegenerateStmtQuery(expr,
                    row,
                    m_paramInfoList,
                    m_function,
                    callSite,
                    m_functionContext->m_SPSubQueryCount,
                    subQueryIndex)) {
                MOT_LOG_TRACE("Failed to regenerate sub-query: %s", expr->query);
                return JitVisitResult::JIT_VISIT_ERROR;
            }
            // reinstate parent context (careful, this might be a non-jittable sub-query)
            if (callSite->m_queryContext != nullptr) {
                callSite->m_queryContext->m_parentContext = m_functionContext;
            }
            // NOTE: JIT Source will be updated upon first access (during PREPARE)
        } else {
            // this must be an invoke context and the invoked child SP has some sub-SP/query that is invalid,
            // so we just revalidate it
            MOT_LOG_TRACE("Re-validating code for sub-query %u with invalid sub-SP: %s", subQueryIndex, expr->query);
            if (!RevalidateJitContext(subQueryContext)) {
                MOT_LOG_TRACE("Failed to revalidate sub-query %u: %s", subQueryIndex, expr->query);
                return JitVisitResult::JIT_VISIT_ERROR;
            }
        }

        return JitVisitResult::JIT_VISIT_CONTINUE;
    }

    void OnError(const char* stmtType, int lineNo) final
    {
        MOT_LOG_TRACE("Failed to regenerate SP query plans, during statement '%s' at line %d", stmtType, lineNo);
    }

private:
    PLpgSQL_function* m_function;
    JitFunctionContext* m_functionContext;
    FuncParamInfoList* m_paramInfoList;
};

extern bool JitReCodegenFunctionQueries(MotJitContext* jitContext)
{
    // 1. get PG compiled function object
    // 2. traverse all sub-queries
    // 3. match the corresponding sub-query context
    // 4. regenerate code if marked as invalid (as in first time compile)
    // 5. invalid child SPs are wrapped with query INVOKE context, so they are handled transparently

    // connect to SPI outside try-catch block (since it uses longjmp - no destructors called)
    SPIAutoConnect spiAutoConnect(true);

    // every variable used after catch needs to be volatile (see longjmp() man page)
    volatile JitFunctionContext* functionContext = (JitFunctionContext*)jitContext;
    volatile FuncParamInfoList paramInfoList = {};
    enum class CodeRegenState { CRS_INIT, CRS_FUNC_PARAM, CRS_NS_PUSH, CRS_NS_POP };
    volatile CodeRegenState regenState = CodeRegenState::CRS_INIT;
    volatile bool result = false;
    volatile MemoryContext origCxt = CurrentMemoryContext;
    volatile PLpgSQL_function* function = nullptr;
    PG_TRY();
    {
        function = GetPGCompiledFunction(functionContext->m_functionOid);
        if (function == nullptr) {
            MOT_LOG_TRACE(
                "Cannot regenerate code for function: Failed to get function by id %u", functionContext->m_functionOid);
            result = false;
        } else {
            // prepare execution state
            ++function->use_count;
            MOT_LOG_TRACE("JitReCodegenFunctionQueries(): Increased use count of function %p to %lu: %s",
                function,
                function->use_count,
                jitContext->m_queryString);

            if (spiAutoConnect.Connect()) {
                // prepare function parameters for query parsing
                if (!PrepareFuncParamInfoList((PLpgSQL_function*)function, (FuncParamInfoList*)&paramInfoList)) {
                    MOT_LOG_TRACE("Cannot regenerate code for function: Failed to prepare function %u parameters",
                        functionContext->m_functionOid);
                    result = false;
                } else {
                    regenState = CodeRegenState::CRS_FUNC_PARAM;

                    // regenerate code for all sub-queries
                    JitQueryRegenerator codeRegen((PLpgSQL_function*)function,
                        (JitFunctionContext*)functionContext,
                        (FuncParamInfoList*)&paramInfoList);

                    if (PushJitSourceNamespace(functionContext->m_functionOid, functionContext->m_queryString)) {
                        regenState = CodeRegenState::CRS_NS_PUSH;
                        result = VisitFunctionQueries((PLpgSQL_function*)function, &codeRegen);
                        PopJitSourceNamespace();
                        regenState = CodeRegenState::CRS_NS_POP;
                    }
                }
            } else {
                int rc = spiAutoConnect.GetErrorCode();
                MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_UNAVAILABLE,
                    "JIT Compile",
                    "Cannot re-generate code for SP sub-queries: Failed to connect to SPI - %s (error code %d)",
                    SPI_result_code_string(rc),
                    rc);
                result = false;
            }
        }
    }
    PG_CATCH();
    {
        MOT_LOG_WARN("Caught exception while regenerating code for jitted function sub-queries");
        // switch back to original context before issuing an error report
        (void)MemoryContextSwitchTo(origCxt);
        ErrorData* edata = CopyErrorData();
        MOT_LOG_WARN("Caught exception while regenerating code for sub-queries of jitted function '%s': %s",
            functionContext->m_queryString,
            edata->message);
        ereport(WARNING,
            (errmsg("Failed to regenerate code for MOT-jitted stored procedure '%s'.", functionContext->m_queryString),
                errdetail("%s", edata->detail)));
        FlushErrorState();
        FreeErrorData(edata);
        result = false;
    }
    PG_END_TRY();

    switch (regenState) {
        case CodeRegenState::CRS_NS_PUSH:
            PopJitSourceNamespace();  // fall through
        case CodeRegenState::CRS_FUNC_PARAM:
            DestroyFuncParamInfoList((FuncParamInfoList*)&paramInfoList);
            break;
        case CodeRegenState::CRS_NS_POP:
        default:
            break;
    }
    if (function != nullptr) {
        --function->use_count;
        MOT_LOG_TRACE("JitReCodegenFunctionQueries(): Decreased use count of function %p to %lu: %s",
            function,
            function->use_count,
            jitContext->m_queryString);
    }

    if (!result) {
        MOT_LOG_TRACE("Cannot regenerate code for function: Failed to regenerate code for sub-query of function %u",
            functionContext->m_functionOid);
    }

    return result;
}

extern bool TryRevalidateJitContext(MotJitContext* jitContext, TransactionId functionTxnId /* = InvalidTransactionId */)
{
    // Ensure that MOT FDW routine and Xact callbacks are registered.
    if (!u_sess->mot_cxt.callbacks_set) {
        ForeignDataWrapper* fdw = GetForeignDataWrapperByName(MOT_FDW, false);
        if (fdw != NULL) {
            (void)GetFdwRoutine(fdw->fdwhandler);
        }
    }

    // Make sure session is ready for access
    PrepareSessionAccess();

    // make sure SPI is set-up on top level call when revalidate takes place with invoke context
    if (jitContext->m_commandType == JIT_COMMAND_INVOKE) {
        SPIAutoConnect spiAutoConn;
        if (!spiAutoConn.IsConnected()) {
            int rc = spiAutoConn.GetErrorCode();
            MOT_LOG_TRACE("Failed to connect to SPI while generating code for query: %s (%u)",
                jitContext->m_queryString,
                SPI_result_code_string(rc),
                rc);
            MarkJitContextErrorCompile(jitContext);
            return false;
        }

        return RevalidateJitContext(jitContext, functionTxnId);
    } else {
        return RevalidateJitContext(jitContext, functionTxnId);
    }
}

static void JitDDLCallback(uint64_t relationId, MOT::DDLAccessType event, MOT::TxnDDLPhase txnDdlPhase)
{
    switch (event) {
        case MOT::DDL_ACCESS_DROP_TABLE:
        case MOT::DDL_ACCESS_TRUNCATE_TABLE:
        case MOT::DDL_ACCESS_DROP_INDEX:
        case MOT::DDL_ACCESS_ADD_COLUMN:
        case MOT::DDL_ACCESS_DROP_COLUMN:
        case MOT::DDL_ACCESS_RENAME_COLUMN:
            PurgeJitSourceCache(relationId, JIT_PURGE_SCOPE_QUERY, JIT_PURGE_EXPIRE, nullptr);
            break;

        case MOT::DDL_ACCESS_CREATE_TABLE:
        case MOT::DDL_ACCESS_CREATE_INDEX:
            PurgeJitSourceCache(relationId, JIT_PURGE_SCOPE_QUERY, JIT_PURGE_EXPIRE, nullptr);
            break;

        case MOT::DDL_ACCESS_UNKNOWN:
            if (txnDdlPhase == MOT::TxnDDLPhase::TXN_DDL_PHASE_COMMIT) {
                ApplyLocalJitSourceChanges();
            } else if (txnDdlPhase == MOT::TxnDDLPhase::TXN_DDL_PHASE_ROLLBACK) {
                RevertLocalJitSourceChanges();
            } else if (txnDdlPhase == MOT::TxnDDLPhase::TXN_DDL_PHASE_POST_COMMIT_CLEANUP) {
                PostCommitCleanupJitSources();
            }
            break;

        default:
            MOT_LOG_TRACE("Invalid DDL event: %d", event);
            break;
    }
}

inline const char* XactEventToString(XactEvent event)
{
    switch (event) {
        case XACT_EVENT_START:
            return "START";
        case XACT_EVENT_COMMIT:
            return "COMMIT";
        case XACT_EVENT_END_TRANSACTION:
            return "END TXN";
        case XACT_EVENT_RECORD_COMMIT:
            return "RECORD COMMIT";
        case XACT_EVENT_ABORT:
            return "ABORT";
        case XACT_EVENT_PREPARE:
            return "PREPARE";
        case XACT_EVENT_COMMIT_PREPARED:
            return "COMMIT PREPARED";
        case XACT_EVENT_ROLLBACK_PREPARED:
            return "ROLLBACK PREPARED";
        case XACT_EVENT_PREROLLBACK_CLEANUP:
            return "PRE-ROLLBACK CLEANUP";
        case XACT_EVENT_POST_COMMIT_CLEANUP:
            return "POST-COMMIT CLEANUP";
        default:
            return "N/A";
    }
}

static void JITXactCallback(XactEvent event, void* arg)
{
    if ((MOT::MOTEngine::GetInstance() != nullptr) && IsMotCodegenEnabled()) {
        MOT_LOG_DEBUG("Received transaction call back: %u (%s)", (int)event, XactEventToString(event));
        if (event == XACT_EVENT_COMMIT) {
            JitDDLCallback(0, MOT::DDL_ACCESS_UNKNOWN, MOT::TxnDDLPhase::TXN_DDL_PHASE_COMMIT);
        } else if (event == XACT_EVENT_ABORT) {
            JitDDLCallback(0, MOT::DDL_ACCESS_UNKNOWN, MOT::TxnDDLPhase::TXN_DDL_PHASE_ROLLBACK);
        } else if (event == XACT_EVENT_POST_COMMIT_CLEANUP) {
            JitDDLCallback(0, MOT::DDL_ACCESS_UNKNOWN, MOT::TxnDDLPhase::TXN_DDL_PHASE_POST_COMMIT_CLEANUP);
        }
    }
}

extern bool JitInitialize()
{
    if (!IsMotCodegenEnabled()) {
        MOT_LOG_INFO("MOT JIT execution is disabled by user configuration");
        return true;
    }

    if (JitCanInitThreadCodeGen()) {
        PrintNativeLlvmStartupInfo();
    } else {
        // whatever happened, we are done and we allow db to continue
        return true;
    }

    if (MOT::GetGlobalConfiguration().m_enableCodegenProfile) {
        MOT_LOG_INFO("MOT JIT profiling is enabled");
        if (!JitProfiler::CreateInstance()) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "JIT Initialization", "Failed to create JIT profiler instance");
            return false;
        }
    }

    enum InitState { JIT_INIT, JIT_CTX_POOL_INIT, JIT_SRC_POOL_INIT, JIT_INIT_DONE } initState = JIT_INIT;
    bool result = true;

    // instead of goto
    do {
        // initialize global JIT context pool
        result = InitGlobalJitContextPool();
        if (!result) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "JIT Initialization",
                "Failed to initialize global JIT context pool (size %d)",
                GetMotCodegenLimit());
            break;
        }
        initState = JIT_CTX_POOL_INIT;

        // initialize global JIT source pool
        result = InitJitSourcePool(GetMotCodegenLimit());
        if (!result) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "JIT Initialization",
                "Failed to initialize global JIT source pool (size %d)",
                GetMotCodegenLimit());
            break;
        }
        initState = JIT_SRC_POOL_INIT;

        // initialize global JIT source map
        result = InitJitSourceMap();
        if (!result) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
                "JIT Initialization",
                "Failed to initialize global JIT source map (size %d)",
                GetMotCodegenLimit());
            break;
        }
        MOT::MOTEngine::GetInstance()->SetDDLCallback(JitDDLCallback);
        // when no DDL was issued, but only SP REPLACE/DROP, we are still missing commit/rollback notification, so we
        // need to register a callback for that. This takes place in each session (see PrepareSessionAccess() above).

        initState = JIT_INIT_DONE;
    } while (0);

    // cleanup in reverse order if failed
    switch (initState) {
        case JIT_SRC_POOL_INIT:
            DestroyJitSourcePool();
            // fall through
        case JIT_CTX_POOL_INIT:
            DestroyGlobalJitContextPool();
            // fall through
        case JIT_INIT:
            if (MOT::GetGlobalConfiguration().m_enableCodegenProfile) {
                JitProfiler::DestroyInstance();
            }
            // fall through
        default:
            break;
    }

    return result;
}

extern void JitDestroy()
{
    MOT::MOTEngine::GetInstance()->SetDDLCallback(nullptr);
    DestroyJitSourceMap();
    DestroyJitSourcePool();
    DestroyGlobalJitContextPool();
    if (MOT::GetGlobalConfiguration().m_enableCodegenProfile) {
        JitProfiler::DestroyInstance();
    }
}

extern bool IsMotCodegenEnabled()
{
    return MOT::GetGlobalConfiguration().m_enableCodegen;
}

extern bool IsMotQueryCodegenEnabled()
{
    return IsMotCodegenEnabled() && MOT::GetGlobalConfiguration().m_enableQueryCodegen;
}

extern bool IsMotSPCodegenEnabled()
{
    return IsMotCodegenEnabled() && MOT::GetGlobalConfiguration().m_enableSPCodegen;
}

extern bool IsMotCodegenPrintEnabled()
{
    return MOT::GetGlobalConfiguration().m_enableCodegenPrint;
}

extern uint32_t GetMotCodegenLimit()
{
    return MOT::GetGlobalConfiguration().m_codegenLimit;
}

extern void JitReportParseError(ErrorData* edata, const char* queryString)
{
    MOT_LOG_WARN("Encountered parse error: %s\n\tWhile parsing query: %s", edata->message, queryString);
    if (u_sess->mot_cxt.jit_parse_error == 0) {
        u_sess->mot_cxt.jit_parse_error = MOT_JIT_GENERIC_PARSE_ERROR;
    }
}

extern void CleanupJitSourceTxnState()
{
    if (MOT::MOTEngine::GetInstance() != nullptr) {
        CleanupLocalJitSourceChanges();
    }
    if (u_sess->mot_cxt.jit_xact_callback_registered) {
        MOT_LOG_DEBUG("Unregistering transaction callback for current session");
        UnregisterXactCallback(JITXactCallback, nullptr);
        u_sess->mot_cxt.jit_xact_callback_registered = false;
    }
}

extern void ForceJitContextInvalidation(MotJitContext* jitContext)
{
    // Here we don't have necessary locks, because the plan was just invalidated and about to be refreshed (See the
    // caller RevalidateCachedQuery). We should not purge here without locks (purge tries to free the keys, etc., but
    // another DDL can happen in parallel). All the purge should have happened already during the DDL operation.
    InvalidateJitContext(jitContext, 0, JIT_CONTEXT_INVALID);
}

extern bool IsInvokeQueryPlan(CachedPlanSource* planSource, Oid* functionOid, TransactionId* functionTxnId)
{
    if (planSource->query_list == nullptr) {
        return false;
    }

    if (list_length(planSource->query_list) != 1) {
        return false;
    }

    Query* query = (Query*)linitial(planSource->query_list);
    FuncExpr* funcExpr = GetFuncExpr(query);
    if (funcExpr == nullptr) {
        return false;
    }

    Oid funcId = funcExpr->funcid;
    TransactionId funcXmin = InvalidTransactionId;
    HeapTuple procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcId));
    if (HeapTupleIsValid(procTuple)) {
        funcXmin = HeapTupleGetRawXmin(procTuple);
    }
    ReleaseSysCache(procTuple);

    if (functionOid != nullptr) {
        *functionOid = funcId;
    }
    if (functionTxnId != nullptr) {
        *functionTxnId = funcXmin;
    }
    return true;
}
}  // namespace JitExec
