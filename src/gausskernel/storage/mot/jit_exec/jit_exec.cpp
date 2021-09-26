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

#include "mot_internal.h"
#include "storage/mot/jit_exec.h"
#include "jit_common.h"
#include "jit_llvm_query_codegen.h"
#include "jit_tvm_query_codegen.h"
#include "jit_source_pool.h"
#include "jit_source_map.h"
#include "jit_context_pool.h"
#include "jit_plan.h"
#include "jit_statistics.h"

#include "mot_engine.h"
#include "utilities.h"
#include "catalog_column_types.h"
#include "mot_error.h"
#include "utilities.h"
#include "cycles.h"

#include <assert.h>

namespace JitExec {
DECLARE_LOGGER(LiteExecutor, JitExec);

#ifdef MOT_JIT_TEST
static uint64_t totalExecCount = 0;
#endif

extern JitPlan* IsJittable(Query* query, const char* queryString)
{
    JitPlan* jitPlan = NULL;
    bool limitBreached = false;

    // when running under thread-pool, it is possible to be executed from a thread that hasn't yet executed any MOT code
    // and thus lacking a thread id and NUMA node id. Nevertheless, we can be sure that a proper session context is set
    // up.
    EnsureSafeThreadAccess();

    // since we use session local allocations and a session context might have not been created yet, we make sure such
    // one exists now
    GetSafeTxn(__FUNCTION__);

    // check limit not breached
    if (u_sess->mot_cxt.jit_context_count >= GetMotCodegenLimit()) {
        MOT_LOG_TRACE("Query is not jittable: Reached the maximum of %d JIT contexts per session",
            u_sess->mot_cxt.jit_context_count);
        limitBreached = true;
    } else if ((query->commandType == CMD_UPDATE) || (query->commandType == CMD_INSERT) ||
               (query->commandType == CMD_SELECT) ||
               (query->commandType == CMD_DELETE)) {  // silently ignore other commands
        // first check if already exists in global source
        if (ContainsReadyCachedJitSource(queryString)) {
            MOT_LOG_TRACE("Query JIT source already exists, reporting query is jittable");
            jitPlan = MOT_READY_JIT_PLAN;
        } else {
            // either query never parsed or is expired, so we generate a plan
            // print query
            if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
                char* parsedQuery = nodeToString(query);
                MOT_LOG_TRACE("Checking if query string is jittable: %s\nParsed query:\n%s", queryString, parsedQuery);
                pfree(parsedQuery);
            }

            // prepare a new plan
            jitPlan = JitPreparePlan(query, queryString);
            if (jitPlan != nullptr) {
                // plan generated so query is jittable, but...
                // we disqualify plan if we see distinct operator, until integrated with PG
                if (JitPlanHasDistinct(jitPlan)) {
                    MOT_LOG_TRACE("Disqualifying plan with DISTINCT aggregate");
                    JitDestroyPlan(jitPlan);
                    jitPlan = nullptr;
                } else {
                    MOT_LOG_TRACE("Query %s jittable by plan", jitPlan ? "is" : "is not");
                    if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
                        JitExplainPlan(query, jitPlan);
                    }
                }
            }
        }
    }

    MOT_LOG_TRACE("Query %s jittable: %s", (jitPlan != nullptr) ? "is" : "is not", queryString);

    // update statistics
    if (limitBreached) {
        MOT_ASSERT(jitPlan == nullptr);
        JitStatisticsProvider::GetInstance().AddUnjittableLimitQuery();
    } else if (jitPlan == nullptr) {
        JitStatisticsProvider::GetInstance().AddUnjittableDisqualifiedQuery();
    } else {
        // whether generating or cloning code, this is a jittable query
        JitStatisticsProvider::GetInstance().AddJittableQuery();
    }

    return jitPlan;
}

static void ProcessJitResult(MOT::RC result, JitContext* jitContext, int newScan)
{
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
                // this is considered as successful execution
                JitStatisticsProvider::GetInstance().AddInvokeQuery();
                if (newScan) {
#ifdef MOT_JIT_TEST
                    MOT_ATOMIC_INC(totalExecCount);
                    MOT_LOG_INFO("JIT total queries executed: %" PRIu64, MOT_ATOMIC_LOAD(totalExecCount));
#endif
                    JitStatisticsProvider::GetInstance().AddExecQuery();
                }
                return;
            default:
                break;
        }
    }

    const char* arg1 = "";
    const char* arg2 = "";

    MOT::TxnManager* currTxn = u_sess->mot_cxt.jit_txn;

    // prepare message argument for error report
    if (result == MOT::RC_UNIQUE_VIOLATION) {
        arg1 = currTxn->m_errIx ? currTxn->m_errIx->GetName().c_str() : "";
        arg2 = currTxn->m_errMsgBuf;
    } else if (result == MOT::RC_NULL_VIOLATION) {
        arg1 = jitContext->m_table->GetField((uint64_t)jitContext->m_nullColumnId)->m_name;
        arg2 = jitContext->m_table->GetLongTableName().c_str();
    }

    if (result != MOT::RC_OK) {
        if (MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
            MOT_LOG_ERROR_STACK(
                "Failed to execute jitted function with error code: %d (%s)", (int)result, MOT::RcToString(result));
        }
        if (result == MOT::RC_ABORT) {
            JitStatisticsProvider::GetInstance().AddAbortExecQuery();
        } else {
            JitStatisticsProvider::GetInstance().AddFailExecQuery();
        }
        report_pg_error(result, (void*)arg1, (void*)arg2);
    }
}

static JitContext* GenerateJitContext(Query* query, const char* queryString, JitPlan* jitPlan, JitSource* jitSource)
{
    JitContext* jitContext = nullptr;
    JitContext* sourceJitContext = nullptr;

    uint64_t startTime = GetSysClock();
    if (g_instance.mot_cxt.jitExecMode == JIT_EXEC_MODE_LLVM) {
        MOT_LOG_TRACE("Generating LLVM JIT context for query: %s", queryString);
        sourceJitContext = JitCodegenLlvmQuery(query, queryString, jitPlan);
    } else {
        MOT_ASSERT(g_instance.mot_cxt.jitExecMode == JIT_EXEC_MODE_TVM);
        MOT_LOG_TRACE("Generating TVM JIT context for query: %s", queryString);
        sourceJitContext = JitCodegenTvmQuery(query, queryString, jitPlan);
    }
    uint64_t endTime = GetSysClock();

    if (sourceJitContext == nullptr) {
        // notify error for all waiters - this query will never again be JITTed - cleanup only during database shutdown
        MOT_LOG_TRACE("Failed to generate code for query, signaling error context for query: %s", queryString);
        SetJitSourceError(jitSource, MOT::GetRootError());
        JitStatisticsProvider::GetInstance().AddCodeGenErrorQuery();
    } else {
        MOT_LOG_TRACE("Generated JIT context %p for query: %s", sourceJitContext, queryString);
        MOT_LOG_TRACE("Cloning ready source context %p", sourceJitContext);
        jitContext = CloneJitContext(sourceJitContext);
        if (jitContext != nullptr) {
            if (SetJitSourceReady(jitSource, sourceJitContext)) {
                MOT_LOG_TRACE("Installed ready JIT context %p for query: %s", sourceJitContext, queryString);
                ++u_sess->mot_cxt.jit_context_count;
                AddJitSourceContext(jitSource, jitContext);  // register for cleanup due to DDL
                jitContext->m_jitSource = jitSource;
                jitContext->m_queryString = jitSource->_query_string;
                // update statistics
                MOT_LOG_TRACE("Registered JIT context %p in JIT source %p for cleanup", jitContext, jitSource);
                JitStatisticsProvider& instance = JitStatisticsProvider::GetInstance();
                instance.AddCodeGenTime(MOT::CpuCyclesLevelTime::CyclesToMicroseconds(endTime - startTime));
                instance.AddCodeGenQuery();
                instance.AddCodeCloneQuery();
            } else {
                // this is illegal state transition error in JIT source (internal bug)
                // there is already a JIT context present in the JIT source (maybe generated by another session,
                // although impossible), so we just fail JIT for this session (other sessions may still benefit from
                // existing JIT context). Pay attention that we cannot replace the JIT context in the JIT source, since
                // the JIT function in it is probably still being used by other sessions
                MOT_REPORT_ERROR(MOT_ERROR_INVALID_STATE,
                    "JIT Compile",
                    "Failed to set ready source context %p, disqualifying query: %s",
                    sourceJitContext,
                    queryString);
                DestroyJitContext(sourceJitContext);
                DestroyJitContext(jitContext);
                jitContext = nullptr;
                JitStatisticsProvider::GetInstance().AddCodeGenErrorQuery();
            }
        } else {
            // this is not expected to happen (because the number of context objects per session equals number of JIT
            // source objects), but we still must set JIT source state to error, otherwise all waiting sessions will
            // never wakeup
            SetJitSourceError(jitSource, MOT::GetRootError());
            MOT_REPORT_ERROR(MOT_ERROR_RESOURCE_UNAVAILABLE,
                "JIT Compile",
                "Failed to clone ready source context %p, disqualifying query: %s",
                sourceJitContext,
                queryString);
            DestroyJitContext(sourceJitContext);
            JitStatisticsProvider::GetInstance().AddCodeCloneErrorQuery();
        }
    }

    return jitContext;
}

extern JitContext* JitCodegenQuery(Query* query, const char* queryString, JitPlan* jitPlan)
{
    JitContext* jitContext = nullptr;

    MOT_LOG_TRACE("*** Generating cached code for query: %s", queryString);
    MOT_ASSERT(jitPlan);

    // search for a cached JIT source - either use an existing one or generate a new one
    JitSource* jitSource = nullptr;
    do {                     // instead of goto
        LockJitSourceMap();  // jit-source already exists, so wait for it to become ready
        jitSource = GetCachedJitSource(queryString);
        if (jitSource != nullptr) {
            MOT_LOG_TRACE("Found a jit-source %p", jitSource);
            UnlockJitSourceMap();

            // ATTENTION: JIT source cannot get expired at this phase, since DDL statements (that might cause the
            // JIT Source to expire) cannot run in parallel with other statements
            // Note: cached context was found, but maybe it is still being compiled, so we wait for it to be ready
            MOT_LOG_TRACE("Waiting for context to be ready: %s", queryString);
            JitContextStatus ctxStatus = WaitJitContextReady(jitSource, &jitContext);
            if (ctxStatus == JIT_CONTEXT_READY) {
                MOT_LOG_TRACE("Context is ready: %s", queryString);
            } else if (ctxStatus == JIT_CONTEXT_EXPIRED) {  // need to regenerate context
                // generate JIT context, install it and notify
                MOT_LOG_TRACE("Regenerating real JIT code for expired context: %s", queryString);
                // we must prepare the analysis variables again (because we did not call IsJittable())
                // in addition, table/index definition might have change so we must re-analyze
                if (jitPlan != MOT_READY_JIT_PLAN) {
                    JitDestroyPlan(jitPlan);
                }
                jitPlan = IsJittable(query, queryString);
                if (jitPlan == nullptr) {
                    MOT_LOG_TRACE("Failed to re-analyze expired JIT source, notifying error status: %s", queryString);
                    SetJitSourceError(jitSource, MOT_ERROR_INTERNAL);
                } else {
                    jitContext = GenerateJitContext(query, queryString, jitPlan, jitSource);
                }
            } else {  // code generation (by another session) failed
                MOT_LOG_TRACE("Cached context status is not ready: %s", queryString);
                break;  // goto cleanup
            }
        } else {                                        // jit-source is not ready, so we need to generate code
            MOT_ASSERT(jitPlan != MOT_READY_JIT_PLAN);  // we must have a real plan, right?
            MOT_LOG_TRACE("JIT-source not found, generating code for query: %s", queryString);
            if (GetJitSourceMapSize() == GetMotCodegenLimit()) {
                MOT_LOG_DEBUG("Skipping query code generation: Reached total maximum of JIT source objects %d",
                    GetMotCodegenLimit());
                UnlockJitSourceMap();
                break;  // goto cleanup
            }
            // we allocate an empty cached entry and install it in the global map, other threads can wait until it
            // is ready (thus only 1 thread regenerates code)
            jitSource = AllocPooledJitSource(queryString);
            if (jitSource != nullptr) {
                MOT_LOG_TRACE("Created jit-source object %p", jitSource);
                if (!AddCachedJitSource(jitSource)) {  // unexpected: entry already exists (this is a bug)
                    MOT_LOG_TRACE("Failed to add jit-source object to map");
                    UnlockJitSourceMap();
                    FreePooledJitSource(jitSource);
                    break;  // goto cleanup
                }
                UnlockJitSourceMap();  // let other operations continue

                // generate JIT context, install it and notify
                MOT_LOG_TRACE("Generating JIT code");
                jitContext = GenerateJitContext(query, queryString, jitPlan, jitSource);
            }
        }
    } while (0);

    // cleanup
    if ((jitPlan != nullptr) && (jitPlan != MOT_READY_JIT_PLAN)) {
        JitDestroyPlan(jitPlan);
    }

    return jitContext;
}

extern void JitResetScan(JitContext* jitContext)
{
    MOT_LOG_DEBUG("JitResetScan(): Resetting iteration count for context %p", jitContext);
    jitContext->m_iterCount = 0;
}

extern int JitExecQuery(
    JitContext* jitContext, ParamListInfo params, TupleTableSlot* slot, uint64_t* tuplesProcessed, int* scanEnded)
{
    int result = 0;

    // make sure we can execute (guard against crash due to logical error)
#ifdef MOT_JIT_DEBUG
    MOT_LOG_DEBUG("Executing JIT context %p with query: %s", jitContext, jitContext->m_queryString);
#endif
    if (!jitContext->m_llvmFunction && !jitContext->m_tvmFunction) {
        MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG,
            "Execute JIT",
            "Cannot execute jitted function: function is null. Aborting transaction.");
        JitStatisticsProvider::GetInstance().AddFailExecQuery();
        report_pg_error(MOT::RC_ERROR);  // execution control ends, calls ereport(error,...)
    }

    // when running under thread-pool, it is possible to be executed from a thread that hasn't yet executed any MOT code
    // and thus lacking a thread id and numa node id. Nevertheless, we can be sure that a proper session context is set
    // up.
    EnsureSafeThreadAccess();

    // make sure we have a valid transaction object (independent of MOT FDW)
    u_sess->mot_cxt.jit_txn = u_sess->mot_cxt.txn_manager;
    if (u_sess->mot_cxt.jit_txn == NULL) {
        MOT_REPORT_ERROR(MOT_ERROR_INTERNAL,
            "Execute JIT",
            "Cannot execute jitted code: Current transaction is undefined. Aborting transaction.");
        JitStatisticsProvider::GetInstance().AddFailExecQuery();
        report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR);  // execution control ends, calls ereport(error,...)
    }

    // during the very first invocation of the query we need to setup the reusable search keys
    // This is also true after TRUNCATE TABLE, in which case we also need to re-fetch all index objects
    if ((jitContext->m_argIsNull == nullptr) ||
        ((jitContext->m_commandType != JIT_COMMAND_INSERT) && (jitContext->m_index == nullptr))) {
        if (!PrepareJitContext(jitContext)) {
            MOT_REPORT_ERROR(
                MOT_ERROR_OOM, "Execute JIT", "Failed to prepare for executing jitted code, aborting transaction");
            JitStatisticsProvider::GetInstance().AddFailExecQuery();
            report_pg_error(MOT::RC_MEMORY_ALLOCATION_ERROR);  // execution control ends, calls ereport(error,...)
        }
    }

    // setup current JIT context
    u_sess->mot_cxt.jit_context = jitContext;

#ifdef MOT_JIT_DEBUG
    // in trace log-level we raise the log level to DEBUG on first few executions only
    bool firstExec = false;
    if ((++jitContext->m_execCount <= 2) && MOT_CHECK_LOG_LEVEL(MOT::LogLevel::LL_TRACE)) {
        MOT_LOG_TRACE("Executing JIT context %p (exec: %" PRIu64 ", query: %" PRIu64 ", iteration: %" PRIu64 "): %s",
            jitContext,
            jitContext->m_execCount,
            jitContext->m_queryCount,
            jitContext->m_iterCount,
            jitContext->m_queryString);
        MOT::SetLogComponentLogLevel("JitExec", MOT::LogLevel::LL_DEBUG);
        firstExec = true;
    }
#endif

    // update iteration count and identify a new scan
    int newScan = 0;
    if (jitContext->m_iterCount == 0) {
        ++jitContext->m_queryCount;
        newScan = 1;
#ifdef MOT_JIT_DEBUG
        MOT_LOG_TRACE("Starting a new scan (exec: %" PRIu64 ", query: %" PRIu64 ",iteration: %" PRIu64 ") for query %s",
            jitContext->m_execCount,
            jitContext->m_queryCount,
            jitContext->m_iterCount,
            jitContext->m_queryString);
#endif
    }
    ++jitContext->m_iterCount;

    // invoke the jitted function
    if (jitContext->m_llvmFunction != nullptr) {
#ifdef MOT_JIT_DEBUG
        MOT_LOG_DEBUG("Executing LLVM-jitted function %p: %s", jitContext->m_llvmFunction, jitContext->m_queryString);
#endif
        result = ((JitFunc)jitContext->m_llvmFunction)(jitContext->m_table,
            jitContext->m_index,
            jitContext->m_searchKey,
            jitContext->m_bitmapSet,
            params,
            slot,
            tuplesProcessed,
            scanEnded,
            newScan,
            jitContext->m_endIteratorKey,
            jitContext->m_innerTable,
            jitContext->m_innerIndex,
            jitContext->m_innerSearchKey,
            jitContext->m_innerEndIteratorKey);
    } else {
#ifdef MOT_JIT_DEBUG
        MOT_LOG_DEBUG("Executing TVM-jitted function %p: %s", jitContext->m_tvmFunction, jitContext->m_queryString);
#endif
        result = JitExecTvmQuery(jitContext, params, slot, tuplesProcessed, scanEnded, newScan);
    }

#ifdef MOT_JIT_DEBUG
    if (firstExec) {
        MOT::SetLogComponentLogLevel("JitExec", MOT::LogLevel::LL_TRACE);
    }
#endif

    // reset current JIT context
    u_sess->mot_cxt.jit_context = NULL;

    if (result == 0) {
        JitStatisticsProvider::GetInstance().AddInvokeQuery();
        if (newScan) {
#ifdef MOT_JIT_TEST
            MOT_ATOMIC_INC(totalExecCount);
            MOT_LOG_INFO("JIT total queries executed: %" PRIu64, MOT_ATOMIC_LOAD(totalExecCount));
#endif
            JitStatisticsProvider::GetInstance().AddExecQuery();
        }
    } else {
        ProcessJitResult((MOT::RC)result, jitContext, newScan);
    }

    return result;
}

extern void PurgeJitSourceCache(uint64_t relationId, bool purgeOnly)
{
    MOT_LOG_TRACE("Purging JIT source map by relation id %" PRIu64, relationId);
    (void)PurgeJitSourceMap(relationId, purgeOnly);
}

extern bool JitInitialize()
{
    if (!IsMotCodegenEnabled()) {
        MOT_LOG_INFO("MOT JIT execution is disabled by user configuration");
        return true;
    }

    if (JitCanInitThreadCodeGen()) {
        if (IsMotPseudoCodegenForced()) {
            MOT_LOG_INFO("Forcing TVM on LLVM natively supported platform by user configuration");
            g_instance.mot_cxt.jitExecMode = JIT_EXEC_MODE_TVM;
        } else {
            PrintNativeLlvmStartupInfo();
            g_instance.mot_cxt.jitExecMode = JIT_EXEC_MODE_LLVM;
        }
    } else {
        if (IsMotPseudoCodegenForced()) {
            MOT_LOG_INFO("Forcing TVM on LLVM natively unsupported platform by user configuration");
        } else {
            MOT_LOG_WARN("Defaulting to TVM on LLVM natively unsupported platform");
        }
        g_instance.mot_cxt.jitExecMode = JIT_EXEC_MODE_TVM;
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
        default:
            break;
    }

    return result;
}

extern void JitDestroy()
{
    DestroyJitSourceMap();
    DestroyJitSourcePool();
    DestroyGlobalJitContextPool();
}

extern bool IsMotCodegenEnabled()
{
    return MOT::GetGlobalConfiguration().m_enableCodegen;
}

extern bool IsMotPseudoCodegenForced()
{
    return MOT::GetGlobalConfiguration().m_forcePseudoCodegen;
}

extern bool IsMotCodegenPrintEnabled()
{
    return MOT::GetGlobalConfiguration().m_enableCodegenPrint;
}

extern uint32_t GetMotCodegenLimit()
{
    return MOT::GetGlobalConfiguration().m_codegenLimit;
}
}  // namespace JitExec
