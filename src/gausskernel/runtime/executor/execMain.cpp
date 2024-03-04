/* -------------------------------------------------------------------------
 *
 * execMain.cpp
 * 	  top level executor interface routines
 *
 * INTERFACE ROUTINES
 * 	ExecutorStart()
 * 	ExecutorRun()
 * 	ExecutorFinish()
 * 	ExecutorEnd()
 *
 * 	These four procedures are the external interface to the executor.
 * 	In each case, the query descriptor is required as an argument.
 *
 * 	ExecutorStart must be called at the beginning of execution of any
 * 	query plan and ExecutorEnd must always be called at the end of
 * 	execution of a plan (unless it is aborted due to error).
 *
 * 	ExecutorRun accepts direction and count arguments that specify whether
 * 	the plan is to be executed forwards, backwards, and for how many tuples.
 * 	In some cases ExecutorRun may be called multiple times to process all
 * 	the tuples for a plan.	It is also acceptable to stop short of executing
 * 	the whole plan (but only if it is a SELECT).
 *
 * 	ExecutorFinish must be called after the final ExecutorRun call and
 * 	before ExecutorEnd.  This can be omitted only in case of EXPLAIN,
 * 	which should also omit ExecutorRun.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 * 	  src/gausskernel/runtime/executor/execMain.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "codegen/gscodegen.h"

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/htup.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/ustore/knl_uheap.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/namespace.h"
#include "commands/trigger.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeRecursiveunion.h"
#include "foreign/fdwapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "storage/buf/bufmgr.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "access/heapam.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "commands/copy.h"
#endif
#include "vecexecutor/vectorbatch.h"
#include "vecexecutor/vecexecutor.h"
#include "utils/anls_opt.h"
#include "utils/memprot.h"
#include "utils/memtrack.h"
#include "workload/workload.h"
#include "distributelayer/streamProducer.h"
#include "commands/explain.h"
#include "workload/workload.h"
#include "instruments/instr_unique_sql.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/executer_gstrace.h"
#include "instruments/instr_slow_query.h"
#include "instruments/instr_statement.h"
#ifdef ENABLE_MOT
#include "storage/mot/jit_exec.h"
#endif
#include "gs_ledger/ledger_utils.h"
#include "gs_policy/gs_policy_masking.h"
#include "optimizer/gplanmgr.h"

/* Hooks for plugins to get control in ExecutorStart/Run/Finish/End */
THR_LOCAL ExecutorStart_hook_type ExecutorStart_hook = NULL;
THR_LOCAL ExecutorRun_hook_type ExecutorRun_hook = NULL;
THR_LOCAL ExecutorFinish_hook_type ExecutorFinish_hook = NULL;
THR_LOCAL ExecutorEnd_hook_type ExecutorEnd_hook = NULL;

/* Hook for plugin to get control in ExecCheckRTPerms() */
THR_LOCAL ExecutorCheckPerms_hook_type ExecutorCheckPerms_hook = NULL;
#define THREAD_INTSERVAL_60S 60

/* Debug information to hold the string of top plan node's node tag */
THR_LOCAL char *producer_top_plannode_str = NULL;
THR_LOCAL bool is_syncup_producer = false;

/* decls for local routines only used within this module */
void InitPlan(QueryDesc *queryDesc, int eflags);
static void CheckValidRowMarkRel(Relation rel, RowMarkType markType);
static void ExecPostprocessPlan(EState *estate);
void ExecEndPlan(PlanState *planstate, EState *estate);
static void ExecCollectMaterialForSubplan(EState *estate);
#ifdef ENABLE_MOT
static void ExecutePlan(EState *estate, PlanState *planstate, CmdType operation, bool sendTuples, long numberTuples,
    ScanDirection direction, DestReceiver *dest, JitExec::MotJitContext* motJitContext);
#else
static void ExecutePlan(EState *estate, PlanState *planstate, CmdType operation, bool sendTuples, long numberTuples,
    ScanDirection direction, DestReceiver *dest);
#endif
static void ExecuteVectorizedPlan(EState *estate, PlanState *planstate, CmdType operation, bool sendTuples,
    long numberTuples, ScanDirection direction, DestReceiver *dest);
static bool ExecCheckRTEPerms(RangeTblEntry *rte);
static bool ExecCheckRTEPermsModified(Oid relOid, Oid userid, Bitmapset *modifiedCols, AclMode requiredPerms);
void ExecCheckXactReadOnly(PlannedStmt *plannedstmt);
static void EvalPlanQualStart(EPQState *epqstate, EState *parentestate, Plan *planTree, bool isUHeap = false);

extern char* ExecBuildSlotValueDescription(
    Oid reloid, TupleTableSlot *slot, TupleDesc tupdesc, Bitmapset *modifiedCols, int maxfieldlen);

extern void BuildStreamFlow(PlannedStmt *plan);
extern void StartUpStreamInParallel(PlannedStmt* pstmt, EState* estate);

extern void CodeGenThreadRuntimeSetup();
extern bool CodeGenThreadObjectReady();
extern void CodeGenThreadRuntimeCodeGenerate();
extern void CodeGenThreadTearDown();
extern bool anls_opt_is_on(AnalysisOpt dfx_opt);
#ifdef USE_SPQ
extern void build_backward_connection(PlannedStmt *planstmt);
#endif

/*
 * Note that GetUpdatedColumns() also exists in commands/trigger.c.  There does
 * not appear to be any good header to put it into, given the structures that
 * it uses, so we let them be duplicated.  Be sure to update both if one needs
 * to be changed, however.
 */
#define GetInsertedColumns(relinfo, estate) \
    (rt_fetch((relinfo)->ri_RangeTableIndex, (estate)->es_range_table)->insertedCols)
#define GetUpdatedColumns(relinfo, estate) \
    (rt_fetch((relinfo)->ri_RangeTableIndex, (estate)->es_range_table)->updatedCols)
#define GET_ALL_UPDATED_COLUMNS(relinfo, estate)                                     \
    (bms_union(exec_rt_fetch((relinfo)->ri_RangeTableIndex, estate)->updatedCols, \
        exec_rt_fetch((relinfo)->ri_RangeTableIndex, estate)->extraUpdatedCols))

/* ----------------------------------------------------------------
 * report_iud_time
 *
 * send the finish time of insert/update/delete operations to pgstat collector.
 * ----------------------------------------------------------------
 */
static void report_iud_time(QueryDesc *query)
{
    ListCell *lc = NULL;
    Oid rid;
    if (u_sess->attr.attr_sql.enable_save_datachanged_timestamp == false) {
        return;
    }

    PlannedStmt *plannedstmt = query->plannedstmt;
    if (plannedstmt->resultRelations) {
        foreach (lc, (List*)linitial(plannedstmt->resultRelations)) {
            Index idx = lfirst_int(lc);
            rid = getrelid(idx, plannedstmt->rtable);
            if (OidIsValid(rid) == false || rid < FirstNormalObjectId) {
                continue;
            }
            Relation rel = NULL;
            rel = heap_open(rid, AccessShareLock);
            if (rel->rd_rel->relkind == RELKIND_RELATION) {
                if (rel->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT ||
                    rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED) {
                    pgstat_report_data_changed(rid, STATFLG_RELATION, rel->rd_rel->relisshared);
                }
            }
            heap_close(rel, AccessShareLock);
        }
    }
}

/* ----------------------------------------------------------------
 * 		ExecutorStart
 *
 * 		This routine must be called at the beginning of any execution of any
 * 		query plan
 *
 * Takes a QueryDesc previously created by CreateQueryDesc (which is separate
 * only because some places use QueryDescs for utility commands).  The tupDesc
 * field of the QueryDesc is filled in to describe the tuples that will be
 * returned, and the internal fields (estate and planstate) are set up.
 *
 * eflags contains flag bits as described in executor.h.
 *
 * NB: the CurrentMemoryContext when this is called will become the parent
 * of the per-query context used for this Executor invocation.
 *
 * We provide a function hook variable that lets loadable plugins
 * get control when ExecutorStart is called.	Such a plugin would
 * normally call standard_ExecutorStart().
 * ----------------------------------------------------------------
 */
void ExecutorStart(QueryDesc* queryDesc, int eflags)
{
    gstrace_entry(GS_TRC_ID_ExecutorStart);

    /* it's unsafe to deal with plugins hooks as dynamic lib may be released */
    if (ExecutorStart_hook && !(g_instance.status > NoShutdown))
        (*ExecutorStart_hook)(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);

    gstrace_exit(GS_TRC_ID_ExecutorStart);
}

void standard_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    EState *estate = NULL;
    MemoryContext old_context;
    double totaltime = 0;

    /* sanity checks: queryDesc must not be started already */
    Assert(queryDesc != NULL);
    Assert(queryDesc->estate == NULL);

#ifdef MEMORY_CONTEXT_CHECKING
    /* Check all memory contexts when executor starts */
    MemoryContextCheck(t_thrd.top_mem_cxt, false);
#endif

    /*
     * If the transaction is read-only, we need to check if any writes are
     * planned to non-temporary tables.  EXPLAIN is considered read-only.
     */
    if (u_sess->attr.attr_common.XactReadOnly && !(eflags & EXEC_FLAG_EXPLAIN_ONLY)) {
        ExecCheckXactReadOnly(queryDesc->plannedstmt);
    }

    /* reset the sequent number of memory context */
    t_thrd.utils_cxt.mctx_sequent_count = 0;

    /* Initialize the memory tracking information */
    if (u_sess->attr.attr_memory.memory_tracking_mode > MEMORY_TRACKING_NONE) {
        MemoryTrackingInit();
    }

    /*
     * Build EState, switch into per-query memory context for startup.
     */
    estate = CreateExecutorState();
    queryDesc->estate = estate;

    /* record the init memory track of the executor engine */
    if (u_sess->attr.attr_memory.memory_tracking_mode > MEMORY_TRACKING_NONE &&
        t_thrd.utils_cxt.ExecutorMemoryTrack == NULL) {
#ifndef ENABLE_MEMORY_CHECK
        t_thrd.utils_cxt.ExecutorMemoryTrack = ((AllocSet)(estate->es_query_cxt))->track;
#else
        t_thrd.utils_cxt.ExecutorMemoryTrack = ((AsanSet)(estate->es_query_cxt))->track;
#endif
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (!IS_SPQ_COORDINATOR) {
        (void)InitStreamObject(queryDesc->plannedstmt);
    }
#endif

    if (StreamTopConsumerAmI() && queryDesc->instrument_options != 0 && IS_PGXC_DATANODE) {
        int dop = queryDesc->plannedstmt->query_dop;
        if (queryDesc->plannedstmt->in_compute_pool) {
            dop = 1;
        }
        AutoContextSwitch streamCxtGuard(u_sess->stream_cxt.stream_runtime_mem_cxt);
        u_sess->instr_cxt.global_instr = StreamInstrumentation::InitOnDn(queryDesc, dop);

        // u_sess->instr_cxt.thread_instr in DN
        u_sess->instr_cxt.thread_instr =
            u_sess->instr_cxt.global_instr->allocThreadInstrumentation(queryDesc->plannedstmt->planTree->plan_node_id);
    }

    /* CN of the compute pool. */
    if (IS_PGXC_COORDINATOR && StreamTopConsumerAmI() && queryDesc->instrument_options != 0 &&
        queryDesc->plannedstmt->in_compute_pool) {
        const int dop = 1;

        /* m_instrDataContext in CN of compute pool is under t_thrd.mem_cxt.stream_runtime_mem_cxt */
        AutoContextSwitch streamCxtGuard(u_sess->stream_cxt.stream_runtime_mem_cxt);
        u_sess->instr_cxt.global_instr = StreamInstrumentation::InitOnCP(queryDesc, dop);

        u_sess->instr_cxt.thread_instr =
            u_sess->instr_cxt.global_instr->allocThreadInstrumentation(queryDesc->plannedstmt->planTree->plan_node_id);
    }

    old_context = MemoryContextSwitchTo(estate->es_query_cxt);
#ifdef ENABLE_LLVM_COMPILE
    /* Initialize the actual CodeGenObj */
    CodeGenThreadRuntimeSetup();
#endif

    /*
     * Fill in external parameters, if any, from queryDesc; and allocate
     * workspace for internal parameters
     */
    estate->es_param_list_info = queryDesc->params;

    if (queryDesc->plannedstmt->nParamExec > 0) {
        estate->es_param_exec_vals =
            (ParamExecData *)palloc0(queryDesc->plannedstmt->nParamExec * sizeof(ParamExecData));
    }

#ifdef USE_SPQ
    estate->es_sharenode = nullptr;
    if (IS_SPQ_EXECUTOR && StreamTopConsumerAmI()) {
        build_backward_connection(queryDesc->plannedstmt);
    }
#endif

    /*
     * If non-read-only query, set the command ID to mark output tuples with
     */
    switch (queryDesc->operation) {
        case CMD_SELECT:
            /*
             * SELECT FOR [KEY] UPDATE/SHARE and modifying CTEs need to mark tuples
             */
            if (queryDesc->plannedstmt->rowMarks != NIL || queryDesc->plannedstmt->hasModifyingCTE) {
                estate->es_output_cid = GetCurrentCommandId(true);
            }

            /*
             * A SELECT without modifying CTEs can't possibly queue triggers,
             * so force skip-triggers mode. This is just a marginal efficiency
             * hack, since AfterTriggerBeginQuery/AfterTriggerEndQuery aren't
             * all that expensive, but we might as well do it.
             */
            if (!queryDesc->plannedstmt->hasModifyingCTE) {
                eflags |= EXEC_FLAG_SKIP_TRIGGERS;
            }
            break;

        case CMD_INSERT:
        case CMD_DELETE:
        case CMD_UPDATE:
        case CMD_MERGE:
            estate->es_output_cid = GetCurrentCommandId(true);
            break;

        default:
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized operation code: %d", (int)queryDesc->operation)));
            break;
    }

    /*
     * Copy other important information into the EState
     */
    estate->es_snapshot = RegisterSnapshot(queryDesc->snapshot);
    estate->es_crosscheck_snapshot = RegisterSnapshot(queryDesc->crosscheck_snapshot);
    estate->es_top_eflags = eflags;
    estate->es_instrument = queryDesc->instrument_options;

    /* Apply BloomFilter array space. */
    if (queryDesc->plannedstmt->MaxBloomFilterNum > 0) {
        int bloom_size = queryDesc->plannedstmt->MaxBloomFilterNum;
        estate->es_bloom_filter.array_size = bloom_size;
        estate->es_bloom_filter.bfarray = (filter::BloomFilter **)palloc0(bloom_size * sizeof(filter::BloomFilter *));
    }
#ifdef ENABLE_MULTIPLE_NODES
    /* statement always start from CN or dn connected by client directly. */
    if (IS_PGXC_COORDINATOR || IsConnFromApp()) {
#else
    /* statement always start in non-stream thread */
    if (!StreamThreadAmI()) {
#endif
        SetCurrentStmtTimestamp();
    } /* else stmtSystemTimestamp synchronize from CN */

    /*
     * Initialize the plan state tree
     */
#ifndef ENABLE_LITE_MODE
    instr_time starttime;
    (void)INSTR_TIME_SET_CURRENT(starttime);
#endif

    IPC_PERFORMANCE_LOG_OUTPUT("standard_ExecutorStart InitPlan start.");
    InitPlan(queryDesc, eflags);
    IPC_PERFORMANCE_LOG_OUTPUT("standard_ExecutorStart InitPlan end.");
#ifndef ENABLE_LITE_MODE
    totaltime += elapsed_time(&starttime);
#endif

    /*
     * if current plan is working for expression, no need to collect instrumentation.
     */
    if (estate->es_instrument != INSTRUMENT_NONE && StreamTopConsumerAmI() && u_sess->instr_cxt.global_instr &&
        u_sess->instr_cxt.thread_instr) {
        int node_id = queryDesc->plannedstmt->planTree->plan_node_id - 1;
        int *m_instrArrayMap = u_sess->instr_cxt.thread_instr->m_instrArrayMap;

        u_sess->instr_cxt.thread_instr->m_instrArray[m_instrArrayMap[node_id]].instr.instruPlanData.init_time =
            totaltime;
    }

    /*
     * Set up an AFTER-trigger statement context, unless told not to, or
     * unless it's EXPLAIN-only mode (when ExecutorFinish won't be called).
     */
    if (!(eflags & (EXEC_FLAG_SKIP_TRIGGERS | EXEC_FLAG_EXPLAIN_ONLY))) {
        AfterTriggerBeginQuery();
    }
    (void)MemoryContextSwitchTo(old_context);
}

/* ----------------------------------------------------------------
 * 		ExecutorRun
 *
 * 		This is the main routine of the executor module. It accepts
 * 		the query descriptor from the traffic cop and executes the
 * 		query plan.
 *
 * 		ExecutorStart must have been called already.
 *
 * 		If direction is NoMovementScanDirection then nothing is done
 * 		except to start up/shut down the destination.  Otherwise,
 * 		we retrieve up to 'count' tuples in the specified direction.
 *
 * 		Note: count = 0 is interpreted as no portal limit, i.e., run to
 * 		completion.  Also note that the count limit is only applied to
 * 		retrieved tuples, not for instance to those inserted/updated/deleted
 * 		by a ModifyTable plan node.
 *
 * 		There is no return value, but output tuples (if any) are sent to
 * 		the destination receiver specified in the QueryDesc; and the number
 * 		of tuples processed at the top level can be found in
 * 		estate->es_processed.
 *
 * 	 	We provide a function hook variable that lets loadable plugins
 * 	 	get control when ExecutorRun is called.  Such a plugin would
 * 	 	normally call standard_ExecutorRun().
 *
 * ----------------------------------------------------------------
 */
void ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
    /* sql active feature, opeartor history statistics */
    int instrument_option = 0;
    bool has_track_operator = false;
    char* old_stmt_name = u_sess->pcache_cxt.cur_stmt_name;
    u_sess->statement_cxt.executer_run_level++;
    if (u_sess->SPI_cxt._connected >= 0) {
        u_sess->pcache_cxt.cur_stmt_name = NULL;
    }
    instr_stmt_report_query_plan(queryDesc);
    exec_explain_plan(queryDesc);
    if (u_sess->attr.attr_resource.use_workload_manager &&
        u_sess->attr.attr_resource.resource_track_level == RESOURCE_TRACK_OPERATOR && 
        queryDesc != NULL && queryDesc->plannedstmt != NULL &&
        queryDesc->plannedstmt->is_stream_plan && u_sess->exec_cxt.need_track_resource) {
#ifdef STREAMPLAN
        if (queryDesc->instrument_options) {
            instrument_option = queryDesc->instrument_options;
        }

        if (IS_PGXC_COORDINATOR && instrument_option != 0 && u_sess->instr_cxt.global_instr == NULL &&
            queryDesc->plannedstmt->num_nodes != 0) {
            has_track_operator = true;
            queryDesc->plannedstmt->instrument_option = instrument_option;
            AutoContextSwitch streamCxtGuard(t_thrd.mem_cxt.msg_mem_cxt);
            int dop = queryDesc->plannedstmt->query_dop;

            u_sess->instr_cxt.global_instr = StreamInstrumentation::InitOnCn(queryDesc, dop);

            MemoryContext old_context = u_sess->instr_cxt.global_instr->getInstrDataContext();
            u_sess->instr_cxt.thread_instr = u_sess->instr_cxt.global_instr->allocThreadInstrumentation(
                queryDesc->plannedstmt->planTree->plan_node_id);
            (void)MemoryContextSwitchTo(old_context);
        }
#endif
    }

    bool can_operator_history_statistics = false;
    if (u_sess->exec_cxt.need_track_resource && queryDesc &&
        (has_track_operator || (IS_PGXC_DATANODE && queryDesc->instrument_options))) {
        can_operator_history_statistics = true;
    }

    if (can_operator_history_statistics) {
        ExplainNodeFinish(queryDesc->planstate, NULL, (TimestampTz)0.0, true);
    }

    if (ExecutorRun_hook) {
        (*ExecutorRun_hook)(queryDesc, direction, count);
    } else {
        standard_ExecutorRun(queryDesc, direction, count);
    }

    if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
        if (queryDesc->operation == CMD_INSERT || queryDesc->operation == CMD_DELETE ||
            queryDesc->operation == CMD_UPDATE || queryDesc->operation == CMD_MERGE) {
            report_iud_time(queryDesc);
        }
    }

    /* SQL Self-Tuning : Analyze query plan issues based on runtime info when query execution is finished */
    if (u_sess->exec_cxt.need_track_resource && queryDesc != NULL && has_track_operator &&
        (IS_PGXC_COORDINATOR || IS_SINGLE_NODE)) {
        List *issue_results = PlanAnalyzerOperator(queryDesc, queryDesc->planstate);

        /* If plan issue is found, store it in sysview gs_wlm_session_history */
        if (issue_results != NIL) {
            RecordQueryPlanIssues(issue_results);
        }
    }
    print_duration(queryDesc);
    instr_stmt_report_cause_type(queryDesc->plannedstmt->cause_type);

    /* sql active feature, opeartor history statistics */
    if (can_operator_history_statistics) {
        u_sess->instr_cxt.can_record_to_table = true;
        ExplainNodeFinish(queryDesc->planstate, queryDesc->plannedstmt, GetCurrentTimestamp(), false);

        if ((IS_PGXC_COORDINATOR) && u_sess->instr_cxt.global_instr != NULL) {
            delete u_sess->instr_cxt.global_instr;
            u_sess->instr_cxt.thread_instr = NULL;
            u_sess->instr_cxt.global_instr = NULL;
        }
    }

    /* 
     * Record the number of rows affected into the session, but only support 
     * DML statement now
     */
    if(queryDesc!=NULL && queryDesc->estate!=NULL){
        switch (queryDesc->operation) {
            case CMD_INSERT:
            case CMD_UPDATE:
            case CMD_DELETE:
            case CMD_MERGE:
                u_sess->statement_cxt.current_row_count = queryDesc->estate->es_processed;
                u_sess->statement_cxt.last_row_count = u_sess->statement_cxt.current_row_count;
                break;
            case CMD_SELECT:
                u_sess->statement_cxt.current_row_count = -1;
                u_sess->statement_cxt.last_row_count = u_sess->statement_cxt.current_row_count;
                break;
            default:
                /* default set queryDesc->estate->es_processed */
                u_sess->statement_cxt.current_row_count = queryDesc->estate->es_processed;
                u_sess->statement_cxt.last_row_count = u_sess->statement_cxt.current_row_count;
                break;
        }
    }

    u_sess->pcache_cxt.cur_stmt_name = old_stmt_name;
    u_sess->statement_cxt.executer_run_level--;
}

void standard_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
    EState *estate = NULL;
    CmdType operation;
    DestReceiver *dest = NULL;
    bool send_tuples = false;
    MemoryContext old_context;
    double totaltime = 0;

    /* sanity checks */
    Assert(queryDesc != NULL);
    estate = queryDesc->estate;
    Assert(estate != NULL);
    Assert(!(estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY));

    /*
     * Switch into per-query memory context
     */
    old_context = MemoryContextSwitchTo(estate->es_query_cxt);

#ifdef ENABLE_LLVM_COMPILE
    /*
     * Generate machine code for this query.
     */
    if (CodeGenThreadObjectReady()) {
        if (anls_opt_is_on(ANLS_LLVM_COMPILE) && estate->es_instrument > 0) {
            TRACK_START(queryDesc->planstate->plan->plan_node_id, LLVM_COMPILE_TIME);
            CodeGenThreadRuntimeCodeGenerate();
            TRACK_END(queryDesc->planstate->plan->plan_node_id, LLVM_COMPILE_TIME);
        } else {
            CodeGenThreadRuntimeCodeGenerate();
        }
    }
#endif

    /* Allow instrumentation of Executor overall runtime */
    if (queryDesc->totaltime) {
        queryDesc->totaltime->memoryinfo.nodeContext = estate->es_query_cxt;
        InstrStartNode(queryDesc->totaltime);
    }

    /*
     * extract information from the query descriptor and the query feature.
     */
    operation = queryDesc->operation;
    dest = queryDesc->dest;

    /*
     * startup tuple receiver, if we will be emitting tuples
     */
    estate->es_processed = 0;
    estate->es_last_processed = 0;
    estate->es_lastoid = InvalidOid;

    send_tuples = (operation == CMD_SELECT || queryDesc->plannedstmt->hasReturning);

    /*
     * In order to ensure the integrity of the message(T-C-Z), regardless of the value of
     * u_sess->exec_cxt.executor_stop_flag, the 'T' message should be sent.
     */
    if (send_tuples)
        (*dest->rStartup)(dest, operation, queryDesc->tupDesc);

    if (queryDesc->plannedstmt->bucketMap[0] != NULL) {
        u_sess->exec_cxt.global_bucket_map = queryDesc->plannedstmt->bucketMap[0];
        u_sess->exec_cxt.global_bucket_cnt = queryDesc->plannedstmt->bucketCnt[0];
    } else {
        u_sess->exec_cxt.global_bucket_map = NULL;
        u_sess->exec_cxt.global_bucket_cnt = 0;
    }

#ifndef ENABLE_LITE_MODE
    instr_time starttime;
    (void)INSTR_TIME_SET_CURRENT(starttime);
#endif
    /*
     * run plan
     */
    if (!ScanDirectionIsNoMovement(direction)) {
        if (queryDesc->planstate->vectorized) {
            ExecuteVectorizedPlan(estate, queryDesc->planstate, operation, send_tuples, count, direction, dest);
        } else {
#ifdef ENABLE_MOT
            ExecutePlan(estate, queryDesc->planstate, operation, send_tuples,
                count, direction, dest, queryDesc->mot_jit_context);
#else
            ExecutePlan(estate, queryDesc->planstate, operation, send_tuples, count, direction, dest);
#endif
        }
    }
#ifndef ENABLE_LITE_MODE
    totaltime += elapsed_time(&starttime);
#endif

    queryDesc->executed = true;

    /*
    *  if current plan is working for expression, no need to collect instrumentation.
    */
    if (
#ifndef ENABLE_MULTIPLE_NODES
        !u_sess->attr.attr_common.enable_seqscan_fusion &&
#endif
        estate->es_instrument != INSTRUMENT_NONE
        && StreamTopConsumerAmI() && u_sess->instr_cxt.global_instr && u_sess->instr_cxt.thread_instr) {
        int node_id = queryDesc->plannedstmt->planTree->plan_node_id - 1;
        int* m_instrArrayMap = u_sess->instr_cxt.thread_instr->m_instrArrayMap;

        u_sess->instr_cxt.thread_instr->m_instrArray[m_instrArrayMap[node_id]].instr.instruPlanData.run_time =
            totaltime;
    }

    /*
     * shutdown tuple receiver, if we started it
     */
    if (send_tuples) {
        (*dest->rShutdown)(dest);
    }
    if (queryDesc->totaltime) {
        InstrStopNode(queryDesc->totaltime, estate->es_processed);
    }

    (void)MemoryContextSwitchTo(old_context);
}

/* ----------------------------------------------------------------
 * 		ExecutorFinish
 *
 * 		This routine must be called after the last ExecutorRun call.
 * 		It performs cleanup such as firing AFTER triggers.	It is
 * 		separate from ExecutorEnd because EXPLAIN ANALYZE needs to
 * 		include these actions in the total runtime.
 *
 * 		We provide a function hook variable that lets loadable plugins
 * 		get control when ExecutorFinish is called.	Such a plugin would
 * 		normally call standard_ExecutorFinish().
 *
 * ----------------------------------------------------------------
 */
void ExecutorFinish(QueryDesc *queryDesc)
{
    if (ExecutorFinish_hook) {
        (*ExecutorFinish_hook)(queryDesc);
    } else {
        standard_ExecutorFinish(queryDesc);
    }
}

void standard_ExecutorFinish(QueryDesc *queryDesc)
{
    EState *estate = NULL;
    MemoryContext old_context;

    /* sanity checks */
    Assert(queryDesc != NULL);
    estate = queryDesc->estate;
    Assert(estate != NULL);
    Assert(!(estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY));

    /* This should be run once and only once per Executor instance */
    Assert(!estate->es_finished);

    /* Switch into per-query memory context */
    old_context = MemoryContextSwitchTo(estate->es_query_cxt);

    /* Allow instrumentation of Executor overall runtime */
    if (queryDesc->totaltime)
        InstrStartNode(queryDesc->totaltime);

    /* Run ModifyTable nodes to completion */
    ExecPostprocessPlan(estate);

    /* Execute queued AFTER triggers, unless told not to */
    if (!(estate->es_top_eflags & EXEC_FLAG_SKIP_TRIGGERS)) {
        AfterTriggerEndQuery(estate);
    }
    if (queryDesc->totaltime) {
        InstrStopNode(queryDesc->totaltime, 0);
    }
    (void)MemoryContextSwitchTo(old_context);
    estate->es_finished = true;
}

/* ----------------------------------------------------------------
 * 		ExecutorEnd
 *
 * 		This routine must be called at the end of execution of any
 * 		query plan
 *
 * 		We provide a function hook variable that lets loadable plugins
 * 		get control when ExecutorEnd is called.  Such a plugin would
 * 	 	normally call standard_ExecutorEnd().
 *
 * ----------------------------------------------------------------
 */
void ExecutorEnd(QueryDesc *queryDesc)
{
    if (ExecutorEnd_hook) {
        (*ExecutorEnd_hook)(queryDesc);
    } else {
        standard_ExecutorEnd(queryDesc);
    }
}

/*
 * description: get the plan node id of stream thread
 * return value: 0: in openGauss thread
 *             >=1: in stream thread
 */
int ExecGetPlanNodeid(void)
{
    int key = 0;
    if (StreamThreadAmI()) {
        key = u_sess->stream_cxt.producer_obj->getKey().planNodeId;
    }
    return key;
}

void standard_ExecutorEnd(QueryDesc *queryDesc)
{
    EState *estate = NULL;
    MemoryContext old_context;
    double totaltime = 0;

#ifndef ENABLE_LITE_MODE
    instr_time starttime;
    (void)INSTR_TIME_SET_CURRENT(starttime);
#endif

    /* sanity checks */
    Assert(queryDesc != NULL);
    estate = queryDesc->estate;
    Assert(estate != NULL);

#ifdef MEMORY_CONTEXT_CHECKING
    /* Check all memory contexts when executor starts */
    MemoryContextCheck(t_thrd.top_mem_cxt, false);
#endif

    /*
     * Check that ExecutorFinish was called, unless in EXPLAIN-only mode. This
     * Assert is needed because ExecutorFinish is new as of 9.1, and callers
     * might forget to call it.
     */
    Assert(estate->es_finished || (estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY));

    /*
     * Switch into per-query memory context to run ExecEndPlan
     */
    old_context = MemoryContextSwitchTo(estate->es_query_cxt);
    EARLY_FREE_LOG(elog(LOG, "Early Free: Start to end plan, memory used %d MB.", getSessionMemoryUsageMB()));
    ExecEndPlan(queryDesc->planstate, estate);

    /* do away with our snapshots */
    UnregisterSnapshot(estate->es_snapshot);
    UnregisterSnapshot(estate->es_crosscheck_snapshot);

#ifdef ENABLE_LLVM_COMPILE
   /* Do not release codegen in Fmgr and Procedure */
    if (!t_thrd.codegen_cxt.g_runningInFmgr && u_sess->SPI_cxt._connected == -1) {
        CodeGenThreadTearDown();
    }
#endif

    /*
     * Must switch out of context before destroying it
     */
    (void)MemoryContextSwitchTo(old_context);

#ifdef MEMORY_CONTEXT_CHECKING
    /* Check per-query memory context before FreeExecutorState */
    MemoryContextCheck(estate->es_query_cxt, (estate->es_query_cxt->session_id > 0));
#endif

    /*
     * Release EState and per-query memory context.  This should release
     * everything the executor has allocated.
     */
    FreeExecutorState(estate);

    /* Reset queryDesc fields that no longer point to anything */
    queryDesc->tupDesc = NULL;
    queryDesc->estate = NULL;
    queryDesc->planstate = NULL;
    queryDesc->totaltime = NULL;

    /* output the memory tracking information into file */
    MemoryTrackingOutputFile();
#ifndef ENABLE_LITE_MODE
    totaltime += elapsed_time(&starttime);
#endif

    /*
     * if current plan is working for expression, no need to collect instrumentation.
     */
    if (queryDesc->instrument_options != 0 && StreamTopConsumerAmI() && u_sess->instr_cxt.global_instr &&
        u_sess->instr_cxt.thread_instr) {
        int node_id = queryDesc->plannedstmt->planTree->plan_node_id - 1;
        int *m_instrArrayMap = u_sess->instr_cxt.thread_instr->m_instrArrayMap;

        u_sess->instr_cxt.thread_instr->m_instrArray[m_instrArrayMap[node_id]].instr.instruPlanData.end_time =
            totaltime;
    }

    /* reset global values of perm space */
    perm_space_value_reset();
}

/* ----------------------------------------------------------------
 * 		ExecutorRewind
 *
 * 		This routine may be called on an open queryDesc to rewind it
 * 		to the start.
 * ----------------------------------------------------------------
 */
void ExecutorRewind(QueryDesc *queryDesc)
{
    EState *estate = NULL;
    MemoryContext old_context;

    /* sanity checks */
    Assert(queryDesc != NULL);
    estate = queryDesc->estate;
    Assert(estate != NULL);
    /* It's probably not sensible to rescan updating queries */
    Assert(queryDesc->operation == CMD_SELECT);
    /*
     * Switch into per-query memory context
     */
    old_context = MemoryContextSwitchTo(estate->es_query_cxt);
    /*
     * rescan plan
     */
    ExecReScan(queryDesc->planstate);
    (void)MemoryContextSwitchTo(old_context);
}

/*
 * ExecCheckRTPerms
 * 		Check access permissions for all relations listed in a range table.
 *
 * Returns true if permissions are adequate.  Otherwise, throws an appropriate
 * error if ereport_on_violation is true, or simply returns false otherwise.
 */
bool ExecCheckRTPerms(List *rangeTable, bool ereport_on_violation)
{
    ListCell *l = NULL;
    bool result = true;
    gstrace_entry(GS_TRC_ID_ExecCheckRTPerms);
#ifdef ENABLE_MULTIPLE_NODES
    bool with_ts_rel = false;
    char* ts_relname = NULL;
#endif
    foreach (l, rangeTable) {
        RangeTblEntry *rte = (RangeTblEntry *)lfirst(l);
#ifdef ENABLE_MULTIPLE_NODES
        /* As the inner table of timeseries table that the tag rel can be skipped */
        if (with_ts_rel && rte->relname != NULL &&
            strncmp(rte->relname, TsConf::TAG_TABLE_NAME_PREFIX, strlen(TsConf::TAG_TABLE_NAME_PREFIX)) == 0) {
            /* check from the next position after ts# */
            if (strncmp(strchr(rte->relname + strlen(TsConf::TAG_TABLE_NAME_PREFIX), '#') + 1,
                        ts_relname, strlen(ts_relname)) == 0) {
                with_ts_rel = false;
                continue;
            }
        }
#endif
        result = ExecCheckRTEPerms(rte);
        if (!result) {
            Assert(rte->rtekind == RTE_RELATION);
            if (ereport_on_violation) {
                aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_CLASS, get_rel_name(rte->relid));
            }
            gstrace_exit(GS_TRC_ID_ExecCheckRTPerms);
            return false;
#ifdef ENABLE_MULTIPLE_NODES
        } else {
            /* check whether the timeseries table */
            if (rte->rtekind == RTE_RELATION && list_length(rangeTable) > 1 &&
                with_ts_rel == false && rte->orientation == REL_TIMESERIES_ORIENTED) {
                with_ts_rel = true;
                ts_relname = rte->relname;
                continue;
            }
#endif
        }
    }

    if (ExecutorCheckPerms_hook) {
        result = (*ExecutorCheckPerms_hook)(rangeTable, ereport_on_violation);
    }
    gstrace_exit(GS_TRC_ID_ExecCheckRTPerms);
    return result;
}

/*
 * ExecCheckRTEPerms
 * 		Check access permissions for a single RTE.
 */
static bool ExecCheckRTEPerms(RangeTblEntry *rte)
{
    AclMode requiredPerms;
    AclMode relPerms;
    AclMode remainingPerms;
    Oid rel_oid;
    Oid userid;
    Bitmapset *tmpset = NULL;
    int col;

    gstrace_entry(GS_TRC_ID_ExecCheckRTEPerms);
    /*
     * Only plain-relation RTEs need to be checked here.  Function RTEs are
     * checked by init_fcache when the function is prepared for execution.
     * Join, subquery, and special RTEs need no checks.
     */
    if (rte->rtekind != RTE_RELATION) {
        gstrace_exit(GS_TRC_ID_ExecCheckRTEPerms);
        return true;
    }

    /*
     * Deal with 'plan_table_data' permission checking here.
     * We do not allow ordinary user to select from 'plan_table_data'.
     */
    if (rte->relname != NULL && strcasecmp(rte->relname, T_PLAN_TABLE_DATA) == 0) {
        if (checkPermsForPlanTable(rte) == 0) {
            gstrace_exit(GS_TRC_ID_ExecCheckRTEPerms);
            return false;
        } else if (checkPermsForPlanTable(rte) == 1) {
            gstrace_exit(GS_TRC_ID_ExecCheckRTEPerms);
            return true;
        }
    }

#ifndef ENABLE_LITE_MODE
    /*
     * If relation is in ledger schema, avoid procedure or function on it.
     */
    if (u_sess->SPI_cxt._connected > -1 && is_ledger_usertable(rte->relid)) {
        gstrace_exit(GS_TRC_ID_ExecCheckRTEPerms);
        return false;
    }
#endif

    /*
     * No work if requiredPerms is empty.
     */
    requiredPerms = rte->requiredPerms;
    if (requiredPerms == 0) {
        gstrace_exit(GS_TRC_ID_ExecCheckRTEPerms);
        return true;
    }

    /*
     * when a non-superuser is doing analyze, a select pg_statistic/pg_statistic_ext query will be
     * sent from the other CN to current CN to synchronize statistics collectted
     * from DNs. Unfortunately, non-superuser is not allowed to select pg_statistic/pg_statistic_ext,
     * so we should do special handling: for query involving pg_statistic/pg_statistic_ext from
     * other CNs, ignore the authorization check.
     */
    if ((StatisticRelationId == rte->relid || StatisticExtRelationId == rte->relid) && IsConnFromCoord()) {
        gstrace_exit(GS_TRC_ID_ExecCheckRTEPerms);
        return true;
    }

    rel_oid = rte->relid;

    /*
     * userid to check as: current user unless we have a setuid indication.
     *
     * Note: GetUserId() is presently fast enough that there's no harm in
     * calling it separately for each RTE.	If that stops being true, we could
     * call it once in ExecCheckRTPerms and pass the userid down from there.
     * But for now, no need for the extra clutter.
     */
    userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

    /*
     * We must have *all* the requiredPerms bits, but some of the bits can be
     * satisfied from column-level rather than relation-level permissions.
     * First, remove any bits that are satisfied by relation permissions.
     */
    relPerms = pg_class_aclmask(rel_oid, userid, requiredPerms, ACLMASK_ALL);
    remainingPerms = requiredPerms & ~relPerms;
    if (remainingPerms != 0) {
        /*
         * If we lack any permissions that exist only as relation permissions,
         * we can fail straight away.
         */
        if (remainingPerms & ~(ACL_SELECT | ACL_INSERT | ACL_UPDATE)) {
            gstrace_exit(GS_TRC_ID_ExecCheckRTEPerms);
            return false;
        }

        /*
         * Check to see if we have the needed privileges at column level.
         *
         * Note: failures just report a table-level error; it would be nicer
         * to report a column-level error if we have some but not all of the
         * column privileges.
         */
        if (remainingPerms & ACL_SELECT) {
            /*
             * When the query doesn't explicitly reference any columns (for
             * example, SELECT COUNT(*) FROM table), allow the query if we
             * have SELECT on any column of the rel, as per SQL spec.
             */
            if (bms_is_empty(rte->selectedCols)) {
                if (pg_attribute_aclcheck_all(rel_oid, userid, ACL_SELECT, ACLMASK_ANY) != ACLCHECK_OK) {
                    gstrace_exit(GS_TRC_ID_ExecCheckRTEPerms);
                    return false;
                }
            }

            tmpset = bms_copy(rte->selectedCols);
            while ((col = bms_first_member(tmpset)) >= 0) {
                /* remove the column number offset */
                col += FirstLowInvalidHeapAttributeNumber;
                if (col == InvalidAttrNumber) {
                    /* Whole-row reference, must have priv on all cols */
                    if (pg_attribute_aclcheck_all(rel_oid, userid, ACL_SELECT, ACLMASK_ALL) != ACLCHECK_OK) {
                        gstrace_exit(GS_TRC_ID_ExecCheckRTEPerms);
                        return false;
                    }
                } else {
                    if (pg_attribute_aclcheck(rel_oid, col, userid, ACL_SELECT) != ACLCHECK_OK) {
                        gstrace_exit(GS_TRC_ID_ExecCheckRTEPerms);
                        return false;
                    }
                }
            }
            bms_free_ext(tmpset);
        }

        /*
         * Basically the same for the mod columns, with either INSERT or
         * UPDATE privilege as specified by remainingPerms.
         */
        if ((remainingPerms & ACL_INSERT) &&
            !ExecCheckRTEPermsModified(rel_oid, userid, rte->insertedCols, ACL_INSERT)) {
            gstrace_exit(GS_TRC_ID_ExecCheckRTEPerms);
            return false;
        }

        if ((remainingPerms & ACL_UPDATE) && !ExecCheckRTEPermsModified(rel_oid, userid, rte->updatedCols, ACL_UPDATE)) {
            gstrace_exit(GS_TRC_ID_ExecCheckRTEPerms);
            return false;
        }
    }
    gstrace_exit(GS_TRC_ID_ExecCheckRTEPerms);
    return true;
}

/*
 * ExecCheckRTEPermsModified
 * 		Check INSERT or UPDATE access permissions for a single RTE (these
 * 		are processed uniformly).
 */
static bool ExecCheckRTEPermsModified(Oid relOid, Oid userid, Bitmapset *modifiedCols, AclMode requiredPerms)
{
    int col = -1;

    /*
     * When the query doesn't explicitly update any columns, allow the query
     * if we have permission on any column of the rel.  This is to handle
     * SELECT FOR UPDATE as well as possible corner cases in UPDATE.
     */
    if (bms_is_empty(modifiedCols)) {
        if (pg_attribute_aclcheck_all(relOid, userid, requiredPerms, ACLMASK_ANY) != ACLCHECK_OK) {
            return false;
        }
    }

    while ((col = bms_next_member(modifiedCols, col)) >= 0) {
        /* bit #s are offset by FirstLowInvalidHeapAttributeNumber */
        AttrNumber attno = col + FirstLowInvalidHeapAttributeNumber;

        if (attno == InvalidAttrNumber) {
            /* whole-row reference can't happen here */
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("whole-row update is not implemented")));
        } else {
            if (pg_attribute_aclcheck(relOid, attno, userid, requiredPerms) != ACLCHECK_OK)
                return false;
        }
    }
    return true;
}

/*
 * Check that the query does not imply any writes to non-temp tables.
 *
 * Note: in a Hot Standby slave this would need to reject writes to temp
 * tables as well; but an HS slave can't have created any temp tables
 * in the first place, so no need to check that.
 */
void ExecCheckXactReadOnly(PlannedStmt *plannedstmt)
{
    ListCell *l = NULL;

    /* Fail if write permissions are requested on any non-temp table */
    foreach (l, plannedstmt->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *)lfirst(l);

        if (rte->rtekind != RTE_RELATION) {
            continue;
        }

        if ((rte->requiredPerms & (~ACL_SELECT)) == 0) {
            continue;
        }

        if (isTempNamespace(get_rel_namespace(rte->relid))) {
            continue;
        }

        if (get_rel_persistence(rte->relid) == RELPERSISTENCE_GLOBAL_TEMP) {
            continue;
        }

        if (rte->relid == PgxcNodeRelationId && g_instance.attr.attr_storage.IsRoachStandbyCluster &&
            u_sess->attr.attr_common.xc_maintenance_mode) {
            continue;
        }

        PreventCommandIfReadOnly(CreateCommandTag((Node *)plannedstmt));
    }
}

/* ----------------------------------------------------------------
 * 		InitPlan
 *
 * 		Initializes the query plan: open files, allocate storage
 * 		and start up the rule manager
 * ----------------------------------------------------------------
 */
void InitPlan(QueryDesc *queryDesc, int eflags)
{
    CmdType operation = queryDesc->operation;
    PlannedStmt *plannedstmt = queryDesc->plannedstmt;
    Plan *plan = plannedstmt->planTree;
    List *rangeTable = plannedstmt->rtable;
    EState *estate = queryDesc->estate;
    PlanState *planstate = NULL;
    TupleDesc tupType = NULL;
    ListCell *l = NULL;
    ListCell *lc = NULL;
    int i;
    bool check = false;

    gstrace_entry(GS_TRC_ID_InitPlan);

    /* We release the partition object lock in InitPlan, here the snapshow is already obtained, so instantaneous
     * inconsistency will never happend. See pg_partition_fn.h for more detail. Distribute mode doesn't support
     * partition DDL/DML parallel work, no need this action. */
#ifndef ENABLE_MULTIPLE_NODES
    ListCell *cell;
    foreach(cell, u_sess->storage_cxt.partition_dml_oids) {
        UnlockPartitionObject(lfirst_oid(cell), PARTITION_OBJECT_LOCK_SDEQUENCE, PARTITION_SHARE_LOCK);
    }
    list_free_ext(u_sess->storage_cxt.partition_dml_oids);
    u_sess->storage_cxt.partition_dml_oids = NIL;
#endif

    /*
     * Do permissions checks
     */
    if (!(IS_PGXC_DATANODE && (IsConnFromCoord() || IsConnFromDatanode()))) {
        check = true;
    }

    if (u_sess->exec_cxt.is_exec_trigger_func) {
        check = true;
    }

    if (plannedstmt->in_compute_pool) {
        check = true;
    }

    if (u_sess->pgxc_cxt.is_gc_fdw && u_sess->pgxc_cxt.is_gc_fdw_analyze) {
        check = true;
    }

    if (check) {
        (void)ExecCheckRTPerms(rangeTable, true);
    }

    /*
     * initialize the node's execution state
     */
    estate->es_range_table = rangeTable;
    estate->es_plannedstmt = plannedstmt;
    estate->es_is_flt_frame = plannedstmt->is_flt_frame;
#ifdef ENABLE_MOT
    estate->mot_jit_context = queryDesc->mot_jit_context;
#endif

    /*
     * initialize result relation stuff, and open/lock the result rels.
     *
     * We must do this before initializing the plan tree, else we might try to
     * do a lock upgrade if a result rel is also a source rel.
     *
     *
     * nodegroup:
     * Node: We may skip a case where target table is not on this datanode, such
     * case happens on a target table's node group not matching the nodes that we
     * are shipping plan to.
     */
#ifdef ENABLE_MULTIPLE_NODES
    if (plannedstmt->resultRelations && (!IS_PGXC_DATANODE || NeedExecute(plan))) {
#else
    if (plannedstmt->resultRelations && (!StreamThreadAmI() || NeedExecute(plan))) {
#endif
        List *resultRelations = plannedstmt->resultRelations;
        int numResultRelations = 0;
        ResultRelInfo *resultRelInfos = NULL;
        ResultRelInfo *resultRelInfo = NULL;

        foreach (l, plannedstmt->resultRelations) {
            numResultRelations += list_length((List*)lfirst(l));
        }
        resultRelInfos = (ResultRelInfo *)palloc(numResultRelations * sizeof(ResultRelInfo));
        resultRelInfo = resultRelInfos;
        foreach (lc, resultRelations) {
            List* resultRels = (List*)lfirst(lc);
            foreach (l, resultRels) {
                Index resultRelationIndex = lfirst_int(l);
                Oid resultRelationOid;
                Relation resultRelation;

                resultRelationOid = getrelid(resultRelationIndex, rangeTable);
                resultRelation = heap_open(resultRelationOid, RowExclusiveLock);
                /* check if modifytable's related temp table is valid */
                if (STMT_RETRY_ENABLED) {
                    // do noting for now, if query retry is on, just to skip validateTempRelation here
                } else
                    validateTempRelation(resultRelation);

                InitResultRelInfo(resultRelInfo, resultRelation, resultRelationIndex, estate->es_instrument);
                resultRelInfo++;
            }
        }
        estate->es_result_relations = resultRelInfos;
        estate->es_num_result_relations = numResultRelations;
        /* es_result_relation_info is NULL except when within ModifyTable */
        estate->es_result_relation_info = NULL;
#ifdef PGXC
        estate->es_result_remoterel = NULL;
#endif
    } else {
        /*
         * if no result relation, then set state appropriately
         */
        estate->es_result_relations = NULL;
        estate->es_num_result_relations = 0;
        estate->es_result_relation_info = NULL;
#ifdef PGXC
        estate->es_result_remoterel = NULL;
#endif
    }

    /*
     * Similarly, we have to lock relations selected FOR [KEY] UPDATE/SHARE
     * before we initialize the plan tree, else we'd be risking lock upgrades.
     * While we are at it, build the ExecRowMark list.
     */
    estate->es_rowMarks = NIL;
    if (plannedstmt->rowMarks) {
        uint64 plan_start_time = time(NULL);
        foreach (l, plannedstmt->rowMarks) {
            PlanRowMark *rc = (PlanRowMark *)lfirst(l);
            Oid relid;
            Relation relation = NULL;
            ExecRowMark *erm = NULL;

            /* ignore "parent" rowmarks; they are irrelevant at runtime */
            if (rc->isParent) {
                continue;
            }

            /*
            * If you change the conditions under which rel locks are acquired
            * here, be sure to adjust ExecOpenScanRelation to match.
            */
            switch (rc->markType) {
                case ROW_MARK_EXCLUSIVE:
                case ROW_MARK_NOKEYEXCLUSIVE:
                case ROW_MARK_SHARE:
                case ROW_MARK_KEYSHARE:
                    if (IS_PGXC_COORDINATOR || u_sess->pgxc_cxt.PGXCNodeId < 0 ||
                        bms_is_member(u_sess->pgxc_cxt.PGXCNodeId, rc->bms_nodeids)) {
                        relid = getrelid(rc->rti, rangeTable);
                        relation = heap_open(relid, RowShareLock);
                    }
                    break;
                case ROW_MARK_REFERENCE:
                    if (IS_PGXC_COORDINATOR || u_sess->pgxc_cxt.PGXCNodeId < 0 ||
                        bms_is_member(u_sess->pgxc_cxt.PGXCNodeId, rc->bms_nodeids)) {
                        relid = getrelid(rc->rti, rangeTable);
                        relation = heap_open(relid, AccessShareLock);
                    }
                    break;
                case ROW_MARK_COPY:
                case ROW_MARK_COPY_DATUM:
                    /* there's no real table here ... */
                    break;
                default:
                    ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized markType: %d when initializing query plan.", rc->markType)));
                    break;
            }

            /* Check that relation is a legal target for marking */
            if (relation != NULL) {
                CheckValidRowMarkRel(relation, rc->markType);
            }

            erm = (ExecRowMark *)palloc(sizeof(ExecRowMark));
            erm->relation = relation;
            erm->rti = rc->rti;
            erm->prti = rc->prti;
            erm->rowmarkId = rc->rowmarkId;
            erm->markType = rc->markType;
            erm->waitPolicy = rc->waitPolicy;
            erm->waitSec = rc->waitSec;
            erm->numAttrs = rc->numAttrs;
            ItemPointerSetInvalid(&(erm->curCtid));
            estate->es_rowMarks = lappend(estate->es_rowMarks, erm);
        }
        uint64 plan_end_time = time(NULL);
        if ((plan_end_time - plan_start_time) > THREAD_INTSERVAL_60S) {
            ereport(WARNING,
                (errmsg("InitPlan foreach plannedstmt->rowMarks takes %lus, plan_start_time:%lus, plan_end_time:%lus.",
                plan_end_time - plan_start_time, plan_start_time, plan_end_time)));
        }
    }
    /*
     * Initialize the executor's tuple table to empty.
     */
    estate->es_tupleTable = NIL;
    estate->es_epqTupleSlot = NULL;
    estate->es_trig_tuple_slot = NULL;
    estate->es_trig_oldtup_slot = NULL;
    estate->es_trig_newtup_slot = NULL;

    /* mark EvalPlanQual not active */
    estate->es_epqTuple = NULL;
    estate->es_epqTupleSet = NULL;
    estate->es_epqScanDone = NULL;

    /* data redistribution for DFS table. */
    if (u_sess->attr.attr_sql.enable_cluster_resize) {
        estate->dataDestRelIndex = plannedstmt->dataDestRelIndex;
    }

    /* deprecated: explain_info_hashtbl/collected_info_hashtbl
     * set false disable data insertion into the hash table.
     * deprecated function: 
     * - pg_stat_get_wlm_realtime_operator_info
     * - pg_stat_get_wlm_realtime_ec_operator_info
     * - pg_stat_get_wlm_ec_operator_info
     * - gs_stat_get_wlm_plan_operator_info
     * - pg_stat_get_wlm_operator_info
     * */
    estate->es_can_realtime_statistics = false;
    estate->es_can_history_statistics = false;

    if (u_sess->attr.attr_resource.use_workload_manager &&
        u_sess->attr.attr_resource.resource_track_level == RESOURCE_TRACK_OPERATOR && !IsInitdb) {
        int current_realtime_num = hash_get_num_entries(g_operator_table.explain_info_hashtbl);
        if (current_realtime_num != 0) {
            /* unreached branch */
            ereport(LOG, (errmsg("Too many realtime info in the memory, current realtime record num is %d.",
                                 current_realtime_num)));
        }

        int current_collectinfo_num = hash_get_num_entries(g_operator_table.collected_info_hashtbl);
        if (current_collectinfo_num != 0) {
            /* unreached branch */
            ereport(LOG, (errmsg("Too many history info in the memory, current history record num is %d.",
                current_collectinfo_num)));
        }
        u_sess->instr_cxt.operator_plan_number = plannedstmt->num_plannodes;
    }

    /*
     * Initialize private state information for each SubPlan.  We must do this
     * before running ExecInitNode on the main query tree, since
     * ExecInitSubPlan expects to be able to find these entries.
     */
    Assert(estate->es_subplanstates == NIL);

    /* Only generate one time when u_sess->debug_query_id = 0 in CN */
    if ((IS_SINGLE_NODE || IS_PGXC_COORDINATOR) && u_sess->debug_query_id == 0) {
        u_sess->debug_query_id = generate_unique_id64(&gt_queryId);
        pgstat_report_queryid(u_sess->debug_query_id);
    }
#ifndef ENABLE_MULTIPLE_NODES
    plannedstmt->queryId = u_sess->debug_query_id;
#endif

    if (StreamTopConsumerAmI()) {
        uint64 stream_start_time = time(NULL);
        BuildStreamFlow(plannedstmt);
        uint64 stream_end_time = time(NULL);
        if ((stream_end_time - stream_start_time) > THREAD_INTSERVAL_60S) {
            ereport(WARNING,
                (errmsg("BuildStreamFlow stream_start_time:%lu,stream_end_time:%lu, BuildStreamFlow takes %lus.",
                stream_start_time, stream_end_time, (stream_end_time - stream_start_time))));
        }
    }

    if (IS_PGXC_DATANODE) {
        u_sess->instr_cxt.gs_query_id->queryId = u_sess->debug_query_id;
    } else {
        u_sess->instr_cxt.gs_query_id->procId = t_thrd.proc_cxt.MyProcPid;
        u_sess->instr_cxt.gs_query_id->queryId = u_sess->debug_query_id;
    }

    i = 1; /* subplan indices count from 1 */
    foreach (l, plannedstmt->subplans) {
        Plan *subplan = (Plan *)lfirst(l);
        PlanState *subplanstate = NULL;
        int sp_eflags;

        /*
         * A subplan will never need to do BACKWARD scan nor MARK/RESTORE. If
         * it is a parameterless subplan (not initplan), we suggest that it be
         * prepared to handle REWIND efficiently; otherwise there is no need.
         */
        sp_eflags = eflags & EXEC_FLAG_EXPLAIN_ONLY;
        if (bms_is_member(i, plannedstmt->rewindPlanIDs)) {
            sp_eflags |= EXEC_FLAG_REWIND;
        }

        /*
         * We initialize non-cte subplan node on coordinator (for explain) or one dn thread
         * that executes the subplan
         */
        if (subplan && (plannedstmt->subplan_ids == NIL ||
#ifdef ENABLE_MULTIPLE_NODES
            (IS_PGXC_COORDINATOR && list_nth_int(plannedstmt->subplan_ids, i - 1) != 0) ||
#else
            (!IS_SPQ_RUNNING && StreamTopConsumerAmI() && list_nth_int(plannedstmt->subplan_ids, i - 1) != 0) ||
            (IS_SPQ_COORDINATOR && list_nth_int(plannedstmt->subplan_ids, i - 1) != 0) ||
#endif
            plannedstmt->planTree->plan_node_id == list_nth_int(plannedstmt->subplan_ids, i - 1))) {
            estate->es_under_subplan = true;
            subplanstate = ExecInitNode(subplan, estate, sp_eflags);

            /* Report subplan or recursive union is init */
            if (IS_PGXC_DATANODE && IsA(subplan, RecursiveUnion)) {
                elog(DEBUG1, "MPP with-recursive init subplan for RecursiveUnion[%d] under top_plannode:[%d]",
                    subplan->plan_node_id, plannedstmt->planTree->plan_node_id);
            }

            estate->es_under_subplan = false;
        }

        estate->es_subplanstates = lappend(estate->es_subplanstates, subplanstate);

        i++;
    }

    /*
     * Initialize the private state information for all the nodes in the query
     * tree.  This opens files, allocates storage and leaves us ready to start
     * processing tuples.
     */
#ifdef ENABLE_MULTIPLE_NODES
    if (!IS_PGXC_COORDINATOR && plannedstmt->initPlan != NIL) {
#else
    if (!StreamTopConsumerAmI() && plannedstmt->initPlan != NIL) {
#endif
        plan->initPlan = plannedstmt->initPlan;
        estate->es_subplan_ids = plannedstmt->subplan_ids;
    }
    planstate = ExecInitNode(plan, estate, eflags);

    if (estate->pruningResult) {
        destroyPruningResult(estate->pruningResult);
        estate->pruningResult = NULL;
    }

    if (planstate->ps_ProjInfo) {
        planstate->ps_ProjInfo->pi_topPlan = true;
    }

    /*
     * Get the tuple descriptor describing the type of tuples to return.
     */
    tupType = ExecGetResultType(planstate);

    /*
     * Initialize the junk filter if needed.  SELECT queries need a filter if
     * there are any junk attrs in the top-level tlist.
     */
    if (operation == CMD_SELECT) {
        bool junk_filter_needed = false;
        ListCell *tlist = NULL;

        foreach (tlist, plan->targetlist) {
            TargetEntry *tle = (TargetEntry *)lfirst(tlist);

            if (tle->resjunk) {
                junk_filter_needed = true;
                break;
            }
        }

#ifdef ENABLE_MULTIPLE_NODES
        if (StreamTopConsumerAmI() || StreamThreadAmI()) {
#else
        if (StreamThreadAmI()) {
#endif
            junk_filter_needed = false;
        }

        if (junk_filter_needed) {
            JunkFilter *j = NULL;

            j = ExecInitJunkFilter(planstate->plan->targetlist, tupType->tdhasoid, ExecInitExtraTupleSlot(estate), tupType->td_tam_ops);
            estate->es_junkFilter = j;

            /* Want to return the cleaned tuple type */
            tupType = j->jf_cleanTupType;
        }
    }

    queryDesc->tupDesc = tupType;
    queryDesc->planstate = planstate;

    if (plannedstmt->num_streams > 0 && !StreamThreadAmI() &&
        !(eflags & EXEC_FLAG_EXPLAIN_ONLY)) {
        /* init stream thread in parallel */
        StartUpStreamInParallel(queryDesc->plannedstmt, queryDesc->estate);
    }

    gstrace_exit(GS_TRC_ID_InitPlan);
}

/*
 * Check that a proposed result relation is a legal target for the operation
 *
 * Generally the parser and/or planner should have noticed any such mistake
 * already, but let's make sure.
 *
 * Note: when changing this function, you probably also need to look at
 * CheckValidRowMarkRel.
 */
void CheckValidResultRel(Relation resultRel, CmdType operation)
{
    TriggerDesc *trigDesc = resultRel->trigdesc;
    FdwRoutine *fdwroutine = NULL;

    switch (resultRel->rd_rel->relkind) {
        case RELKIND_RELATION:
            if (u_sess->exec_cxt.is_exec_trigger_func && is_ledger_related_rel(resultRel)) {
                ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("cannot change ledger relation \"%s\"", RelationGetRelationName(resultRel))));
            }
            CheckCmdReplicaIdentity(resultRel, operation);
            break;
        case RELKIND_SEQUENCE:
        case RELKIND_LARGE_SEQUENCE:
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot change (large) sequence \"%s\"", RelationGetRelationName(resultRel))));
            break;
        case RELKIND_TOASTVALUE:
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot change TOAST relation \"%s\"", RelationGetRelationName(resultRel))));
            break;
        case RELKIND_VIEW:
        case RELKIND_CONTQUERY:
            /*
             * Okay only if there's a suitable INSTEAD OF trigger.  Messages
             * here should match rewriteHandler.c's rewriteTargetView and
             * RewriteQuery, except that we omit errdetail because we haven't
             * got the information handy (and given that we really shouldn't
             * get here anyway, it's not worth great exertion to get).
             */
            switch (operation) {
                case CMD_INSERT:
                    if (trigDesc == NULL || !trigDesc->trig_insert_instead_row) {
                        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("cannot insert into view \"%s\"", RelationGetRelationName(resultRel)),
                            errhint("To enable inserting into the view, provide an INSTEAD OF INSERT trigger or "
                            "an unconditional ON INSERT DO INSTEAD rule.")));
                    }

                    break;
                case CMD_UPDATE:
                    if (trigDesc == NULL || !trigDesc->trig_update_instead_row) {
                        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("cannot update view \"%s\"", RelationGetRelationName(resultRel)),
                            errhint("To enable updating the view, provide an INSTEAD OF UPDATE trigger or "
                            "an unconditional ON UPDATE DO INSTEAD rule.")));
                    }
                    break;
                case CMD_DELETE:
                    if (trigDesc == NULL || !trigDesc->trig_delete_instead_row) {
                        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("cannot delete from view \"%s\"", RelationGetRelationName(resultRel)),
                            errhint("To enable deleting from the view, provide an INSTEAD OF DELETE trigger or "
                            "an unconditional ON DELETE DO INSTEAD rule.")));
                    }
                    break;
                default:
                    ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized CmdType: %d when perform operations on view.", (int)operation)));
                    break;
            }
            break;
        case RELKIND_MATVIEW:
            break;
        case RELKIND_STREAM:
        case RELKIND_FOREIGN_TABLE:
            /* Okay only if the FDW supports it */
            fdwroutine = GetFdwRoutineForRelation(resultRel, false);
            switch (operation) {
                case CMD_INSERT:
                    if (fdwroutine->ExecForeignInsert == NULL) {
                        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("cannot insert into foreign table \"%s\"", RelationGetRelationName(resultRel))));
                    }
                    if (fdwroutine->IsForeignRelUpdatable != NULL &&
                        (fdwroutine->IsForeignRelUpdatable(resultRel) & (1 << CMD_INSERT)) == 0) {
                        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("foreign table \"%s\" does not allow inserts", RelationGetRelationName(resultRel))));
                    }
                    break;
                case CMD_UPDATE:
                    if (fdwroutine->ExecForeignUpdate == NULL) {
                        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("cannot update foreign table \"%s\"", RelationGetRelationName(resultRel))));
                    }
                    if (fdwroutine->IsForeignRelUpdatable != NULL &&
                        (fdwroutine->IsForeignRelUpdatable(resultRel) & (1 << CMD_UPDATE)) == 0) {
                        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("foreign table \"%s\" does not allow updates", RelationGetRelationName(resultRel))));
                    }
                    break;
                case CMD_DELETE:
                    if (fdwroutine->ExecForeignDelete == NULL) {
                        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("cannot delete from foreign table \"%s\"", RelationGetRelationName(resultRel))));
                    }
                    if (fdwroutine->IsForeignRelUpdatable != NULL &&
                        (fdwroutine->IsForeignRelUpdatable(resultRel) & (1 << CMD_DELETE)) == 0) {
                        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("foreign table \"%s\" does not allow deletes", RelationGetRelationName(resultRel))));
                    }
                    break;
                default:
                    ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized CmdType: %d when perform operation on foreign table.", (int)operation)));
                    break;
            }
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot change relation \"%s\"", RelationGetRelationName(resultRel))));
            break;
    }
}

/*
 * Check that a proposed rowmark target relation is a legal target
 *
 * In most cases parser and/or planner should have noticed this already, but
 * they don't cover all cases.
 */
static void CheckValidRowMarkRel(Relation rel, RowMarkType markType)
{
    switch (rel->rd_rel->relkind) {
        case RELKIND_RELATION:
            /* OK */
            break;
        case RELKIND_SEQUENCE:
        case RELKIND_LARGE_SEQUENCE:
            /* Must disallow this because we don't vacuum sequences */
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot lock rows in (large) sequence \"%s\"", RelationGetRelationName(rel))));
            break;
        case RELKIND_TOASTVALUE:
            /* We could allow this, but there seems no good reason to */
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot lock rows in TOAST relation \"%s\"", RelationGetRelationName(rel))));
            break;
        case RELKIND_VIEW:
            /* Should not get here; planner should have expanded the view */
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot lock rows in view \"%s\"", RelationGetRelationName(rel))));
            break;
        case RELKIND_CONTQUERY:
            /* Should not get here */
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot lock rows in contview \"%s\"", RelationGetRelationName(rel))));
            break;
        case RELKIND_MATVIEW:
            /* Should not get here */
            ereport(ERROR,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                     errmsg("cannot lock rows in materialized view \"%s\"",
                            RelationGetRelationName(rel))));
            break;
        case RELKIND_FOREIGN_TABLE:
            /* Should not get here; planner should have used ROW_MARK_COPY */
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot lock rows in foreign table \"%s\"", RelationGetRelationName(rel))));
            break;
        case RELKIND_STREAM:
            /* Should not get here; planner should have used ROW_MARK_COPY */
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot lock rows in stream \"%s\"", RelationGetRelationName(rel))));
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot lock rows in relation \"%s\"", RelationGetRelationName(rel))));
            break;
    }
}

/*
 * Initialize ResultRelInfo data for one result relation
 *
 * Caution: before Postgres 9.1, this function included the relkind checking
 * that's now in CheckValidResultRel, and it also did ExecOpenIndices if
 * appropriate.  Be sure callers cover those needs.
 */
void InitResultRelInfo(ResultRelInfo *resultRelInfo, Relation resultRelationDesc, Index resultRelationIndex,
    int instrument_options)
{
    errno_t rc = memset_s(resultRelInfo, sizeof(ResultRelInfo), 0, sizeof(ResultRelInfo));
    securec_check(rc, "\0", "\0");
    resultRelInfo->type = T_ResultRelInfo;
    resultRelInfo->ri_RangeTableIndex = resultRelationIndex;
    resultRelInfo->ri_RelationDesc = resultRelationDesc;
    resultRelInfo->ri_NumIndices = 0;
    resultRelInfo->ri_ContainGPI = false;
    resultRelInfo->ri_IndexRelationDescs = NULL;
    resultRelInfo->ri_IndexRelationInfo = NULL;
    /* make a copy so as not to depend on relcache info not changing... */
    resultRelInfo->ri_TrigDesc = CopyTriggerDesc(resultRelationDesc->trigdesc);
    if (resultRelInfo->ri_TrigDesc) {
        int n = resultRelInfo->ri_TrigDesc->numtriggers;

        resultRelInfo->ri_TrigFunctions = (FmgrInfo *)palloc0(n * sizeof(FmgrInfo));
        resultRelInfo->ri_TrigWhenExprs = (List **)palloc0(n * sizeof(List *));
        if (instrument_options) {
            resultRelInfo->ri_TrigInstrument = InstrAlloc(n, instrument_options);
        }
    } else {
        resultRelInfo->ri_TrigFunctions = NULL;
        resultRelInfo->ri_TrigWhenExprs = NULL;
        resultRelInfo->ri_TrigInstrument = NULL;
    }
    if (resultRelationDesc->rd_rel->relkind == RELKIND_FOREIGN_TABLE
        || resultRelationDesc->rd_rel->relkind == RELKIND_STREAM) {
        resultRelInfo->ri_FdwRoutine = GetFdwRoutineForRelation(resultRelationDesc, true);
    } else {
        resultRelInfo->ri_FdwRoutine = NULL;
    }
    resultRelInfo->ri_FdwState = NULL;
    resultRelInfo->ri_ConstraintExprs = NULL;
    resultRelInfo->ri_GeneratedExprs = NULL;
    resultRelInfo->ri_junkFilter = NULL;
    resultRelInfo->ri_projectReturning = NULL;
    resultRelInfo->ri_mergeTargetRTI = 0;
    resultRelInfo->ri_mergeState = (MergeState *)palloc0(sizeof(MergeState));
#ifdef USE_SPQ
    resultRelInfo->ri_actionAttno = InvalidAttrNumber;
#endif
}

/*
 * 		ExecGetTriggerResultRel
 *
 * Get a ResultRelInfo for a trigger target relation.  Most of the time,
 * triggers are fired on one of the result relations of the query, and so
 * we can just return a member of the es_result_relations array.  (Note: in
 * self-join situations there might be multiple members with the same OID;
 * if so it doesn't matter which one we pick.)  However, it is sometimes
 * necessary to fire triggers on other relations; this happens mainly when an
 * RI update trigger queues additional triggers on other relations, which will
 * be processed in the context of the outer query.	For efficiency's sake,
 * we want to have a ResultRelInfo for those triggers too; that can avoid
 * repeated re-opening of the relation.  (It also provides a way for EXPLAIN
 * ANALYZE to report the runtimes of such triggers.)  So we make additional
 * ResultRelInfo's as needed, and save them in es_trig_target_relations.
 */
ResultRelInfo *ExecGetTriggerResultRel(EState *estate, Oid relid)
{
    ResultRelInfo *rInfo = NULL;
    int nr;
    ListCell *l = NULL;
    Relation rel;
    MemoryContext old_context;

    /* First, search through the query result relations */
    rInfo = estate->es_result_relations;
    nr = estate->es_num_result_relations;
    while (nr > 0) {
        if (RelationGetRelid(rInfo->ri_RelationDesc) == relid) {
            return rInfo;
        }
        rInfo++;
        nr--;
    }
    /* Nope, but maybe we already made an extra ResultRelInfo for it */
    foreach (l, estate->es_trig_target_relations) {
        rInfo = (ResultRelInfo *)lfirst(l);
        if (RelationGetRelid(rInfo->ri_RelationDesc) == relid) {
            return rInfo;
        }
    }
    /* Nope, so we need a new one */
    /*
     * Open the target relation's relcache entry.  We assume that an
     * appropriate lock is still held by the backend from whenever the trigger
     * event got queued, so we need take no new lock here.	Also, we need not
     * recheck the relkind, so no need for CheckValidResultRel.
     */
    rel = heap_open(relid, NoLock);

    /*
     * Make the new entry in the right context.
     */
    old_context = MemoryContextSwitchTo(estate->es_query_cxt);
    rInfo = makeNode(ResultRelInfo);
    InitResultRelInfo(rInfo, rel, 0, /* dummy rangetable index */
        estate->es_instrument);
    estate->es_trig_target_relations = lappend(estate->es_trig_target_relations, rInfo);
    (void)MemoryContextSwitchTo(old_context);

    /*
     * Currently, we don't need any index information in ResultRelInfos used
     * only for triggers, so no need to call ExecOpenIndices.
     */
    return rInfo;
}

/*
 * 		ExecContextForcesOids
 *
 * This is pretty grotty: when doing INSERT, UPDATE, or CREATE TABLE AS,
 * we need to ensure that result tuples have space for an OID iff they are
 * going to be stored into a relation that has OIDs.  In other contexts
 * we are free to choose whether to leave space for OIDs in result tuples
 * (we generally don't want to, but we do if a physical-tlist optimization
 * is possible).  This routine checks the plan context and returns TRUE if the
 * choice is forced, FALSE if the choice is not forced.  In the TRUE case,
 * *hasoids is set to the required value.
 *
 * One reason this is ugly is that all plan nodes in the plan tree will emit
 * tuples with space for an OID, though we really only need the topmost node
 * to do so.  However, node types like Sort don't project new tuples but just
 * return their inputs, and in those cases the requirement propagates down
 * to the input node.  Eventually we might make this code smart enough to
 * recognize how far down the requirement really goes, but for now we just
 * make all plan nodes do the same thing if the top level forces the choice.
 *
 * We assume that if we are generating tuples for INSERT or UPDATE,
 * estate->es_result_relation_info is already set up to describe the target
 * relation.  Note that in an UPDATE that spans an inheritance tree, some of
 * the target relations may have OIDs and some not.  We have to make the
 * decisions on a per-relation basis as we initialize each of the subplans of
 * the ModifyTable node, so ModifyTable has to set es_result_relation_info
 * while initializing each subplan.
 *
 * CREATE TABLE AS is even uglier, because we don't have the target relation's
 * descriptor available when this code runs; we have to look aside at the
 * flags passed to ExecutorStart().
 */
bool ExecContextForcesOids(PlanState *planstate, bool *hasoids)
{
    ResultRelInfo *ri = planstate->state->es_result_relation_info;

    if (ri != NULL) {
        Relation rel = ri->ri_RelationDesc;

        if (rel != NULL) {
            *hasoids = rel->rd_rel->relhasoids;
            return true;
        }
    }

    if (planstate->state->es_top_eflags & EXEC_FLAG_WITH_OIDS) {
        *hasoids = true;
        return true;
    }
    if (planstate->state->es_top_eflags & EXEC_FLAG_WITHOUT_OIDS) {
        *hasoids = false;
        return true;
    }

    return false;
}

/* ----------------------------------------------------------------
 * 		ExecPostprocessPlan
 *
 * 		Give plan nodes a final chance to execute before shutdown
 * ----------------------------------------------------------------
 */
static void ExecPostprocessPlan(EState *estate)
{
    ListCell *lc = NULL;

    /*
     * Make sure nodes run forward.
     */
    estate->es_direction = ForwardScanDirection;

    /*
     * Run any secondary ModifyTable nodes to completion, in case the main
     * query did not fetch all rows from them.	(We do this to ensure that
     * such nodes have predictable results.)
     */
    foreach (lc, estate->es_auxmodifytables) {
        PlanState *ps = (PlanState *)lfirst(lc);

        if (!ps->vectorized) {
            for (;;) {
                TupleTableSlot *slot = NULL;

                /* Reset the per-output-tuple exprcontext each time */
                ResetPerTupleExprContext(estate);

                slot = ExecProcNode(ps);

                if (TupIsNull(slot)) {
                    break;
                }
            }
        } else {
            for (;;) {
                VectorBatch *batch = NULL;

                /* Reset the per-output-tuple exprcontext */
                ResetPerTupleExprContext(estate);

                /*
                 * Execute the plan and obtain a batch
                 */
                batch = VectorEngine(ps);

                if (BatchIsNull(batch)) {
                    break;
                }
            }
        }
    }
}

/* ----------------------------------------------------------------
 * 		ExecEndPlan
 *
 * 		Cleans up the query plan -- closes files and frees up storage
 *
 * NOTE: we are no longer very worried about freeing storage per se
 * in this code; FreeExecutorState should be guaranteed to release all
 * memory that needs to be released.  What we are worried about doing
 * is closing relations and dropping buffer pins.  Thus, for example,
 * tuple tables must be cleared or dropped to ensure pins are released.
 * ----------------------------------------------------------------
 */
void ExecEndPlan(PlanState *planstate, EState *estate)
{
    ResultRelInfo *resultRelInfo = NULL;
    int i;
    ListCell *l = NULL;

    /*
     * shut down the node-type-specific query processing
     */
    ExecEndNode(planstate);

    /*
     * for subplans too
     */
    foreach (l, estate->es_subplanstates) {
        PlanState *subplanstate = (PlanState *)lfirst(l);

        ExecEndNode(subplanstate);
    }

    /*
     * destroy the executor's tuple table.  Actually we only care about
     * releasing buffer pins and tupdesc refcounts; there's no need to pfree
     * the TupleTableSlots, since the containing memory context is about to go
     * away anyway.
     */
    ExecResetTupleTable(estate->es_tupleTable, false);

    /*
     * close the result relation(s) if any, but hold locks until xact commit.
     */
    resultRelInfo = estate->es_result_relations;
    for (i = estate->es_num_result_relations; i > 0; i--) {
        /* Close indices and then the relation itself */
        ExecCloseIndices(resultRelInfo);
        heap_close(resultRelInfo->ri_RelationDesc, NoLock);
        resultRelInfo++;
    }

    /* free the fakeRelationCache */
    if (estate->esfRelations != NULL) {
        FakeRelationCacheDestroy(estate->esfRelations);
    }
    estate->esCurrentPartition = NULL;

    /*
     * likewise close any trigger target relations
     */
    foreach (l, estate->es_trig_target_relations) {
        resultRelInfo = (ResultRelInfo *)lfirst(l);
        /* Close indices and then the relation itself */
        ExecCloseIndices(resultRelInfo);
        heap_close(resultRelInfo->ri_RelationDesc, NoLock);
    }

    /*
     * close any relations selected FOR [KEY] UPDATE/SHARE, again keeping locks
     */
    foreach (l, estate->es_rowMarks) {
        ExecRowMark *erm = (ExecRowMark *)lfirst(l);

        if (erm->relation) {
            heap_close(erm->relation, NoLock);
        }
    }
}

/*
 * @Description: Collect Material for Subplan before running the main plan
 *
 * @param[IN] estate:  working state for an Executor invocation
 * @return: void
 */
static void ExecCollectMaterialForSubplan(EState *estate)
{
    ListCell *lc = NULL;

    foreach (lc, estate->es_material_of_subplan) {
        PlanState *node = (PlanState *)lfirst(lc);

        /*
         * If the current materliaze node is recursive-union and the right tree has stream
         * node, we are skip the pre-materliaze the subplan as at current point the SyncPoint
         * on consumer side is not start yet in ExecRecursiveUnion()
         */
        if (EXEC_IN_RECURSIVE_MODE(node->plan)) {
            continue;
        }

        if (IsA(node, MaterialState)) {
            for (;;) {
                TupleTableSlot *slot = NULL;

                /* Reset the per-output-tuple exprcontext each time */
                ResetPerTupleExprContext(estate);

                slot = ExecProcNode(node);

                if (TupIsNull(slot)) {
                    /* Reset Material so that its output can be re-scanned */
                    ExecReScan(node);
                    break;
                }
            }
        } else if (IsA(node, VecMaterialState)) {
            for (;;) {
                VectorBatch *batch = NULL;

                /*
                 * Execute the plan and obtain a batch
                 */
                batch = VectorEngine(node);

                if (BatchIsNull(batch)) {
                    /* Reset Material so that its output can be re-scanned */
                    VecExecReScan(node);
                    break;
                }
            }
        }
    }
}

/* ----------------------------------------------------------------
 * 		ExecutePlan
 *
 * 		Processes the query plan until we have retrieved 'numberTuples' tuples,
 * 		moving in the specified direction.
 *
 * 		Runs to completion if numberTuples is 0
 *
 * Note: the ctid attribute is a 'junk' attribute that is removed before the
 * user can see it
 * ----------------------------------------------------------------
 */
#ifdef ENABLE_MOT
static void ExecutePlan(EState *estate, PlanState *planstate, CmdType operation, bool sendTuples, long numberTuples,
    ScanDirection direction, DestReceiver *dest, JitExec::MotJitContext* motJitContext)
#else
static void ExecutePlan(EState *estate, PlanState *planstate, CmdType operation, bool sendTuples, long numberTuples,
    ScanDirection direction, DestReceiver *dest)
#endif
{
    TupleTableSlot *slot = NULL;
    long current_tuple_count = 0;
    bool stream_instrument = false;
    bool need_sync_step = false;
    bool recursive_early_stop = false;
#ifdef ENABLE_MOT
    bool motFinishedExecution = false;
#endif
    /* set the flag to false, prepare to record */
    u_sess->storage_cxt.is_in_pre_read = false;
    u_sess->storage_cxt.bulk_read_count = 0;

    /* Mark sync-up step is required */
    if (unlikely(NeedSyncUpProducerStep(planstate->plan))) {
        need_sync_step = true;
        /*
         * (G)Distributed With-Recursive Support
         *
         * If current producer thread is under a recursive cte plan node, we need do
         * step sync-up across the whole cluster
         */
        u_sess->exec_cxt.global_iteration = 0;
        ExecutePlanSyncProducer(planstate, WITH_RECURSIVE_SYNC_NONERQ, &recursive_early_stop, &current_tuple_count);
        u_sess->exec_cxt.global_iteration = 1;
    }

    /*
     * Set the direction.
     */
    estate->es_direction = direction;

    if (!IS_SPQ_COORDINATOR && IS_PGXC_DATANODE) {
        /* Collect Material for Subplan first */
        ExecCollectMaterialForSubplan(estate);

        /* Collect Executor run time including sending data time */
        if (estate->es_instrument != INSTRUMENT_NONE && u_sess->instr_cxt.global_instr &&
            u_sess->instr_cxt.thread_instr) {
            stream_instrument = true;
            int plan_id = planstate->plan->plan_node_id;
            u_sess->instr_cxt.global_instr->SetStreamSend(plan_id, true);
        }
    }

    /* Change DestReceiver's tmpContext to PerTupleMemoryContext to avoid memory leak. */
    dest->tmpContext = GetPerTupleMemoryContext(estate);

    // planstate->plan will be release if rollback excuted
    bool is_saved_recursive_union_plan_nodeid = EXEC_IN_RECURSIVE_MODE(planstate->plan);
    /*
     * Loop until we've processed the proper number of tuples from the plan.
     */
    for (;;) {
        /* Reset the per-output-tuple exprcontext */
        ResetPerTupleExprContext(estate);

        /*
         * Execute the plan and obtain a tuple
         */
#ifdef ENABLE_MOT
        if (unlikely(recursive_early_stop)) {
            slot = NULL;
        } else if (motJitContext && JitExec::IsJitContextValid(motJitContext) && !IS_PGXC_COORDINATOR &&
            JitExec::IsMotCodegenEnabled()) {
            // MOT LLVM
            int scanEnded = 0;
            if (!motFinishedExecution) {
                // previous iteration has not signaled end of scan
                slot = planstate->ps_ResultTupleSlot;
                uint64_t tuplesProcessed = 0;
                int rc = JitExec::JitExecQuery(
                    motJitContext, estate->es_param_list_info, slot, &tuplesProcessed, &scanEnded);
                if (scanEnded || (tuplesProcessed == 0) || (rc != 0)) {
                    // raise flag so that next round we will bail out (current tuple still must be reported to user)
                    motFinishedExecution = true;
                }
            } else {
                (void)ExecClearTuple(slot);
            }
        } else {
            slot = ExecProcNode(planstate);
        }
#else
        slot = unlikely(recursive_early_stop) ? NULL : ExecProcNode(planstate);
#endif

        /*
         * ------------------------------------------------------------------------------
         * (G)Distributed With-Recursive Support
         *
         * If under recursive cte, we need check sync step and do rescan properly
         */
        if (unlikely(need_sync_step) && TupIsNull(slot)) {
            if (!ExecutePlanSyncProducer(planstate, WITH_RECURSIVE_SYNC_RQSTEP, &recursive_early_stop,
                &current_tuple_count)) {
                /* current iteration step is not finish, continue to the next iteration */
                continue;
            }
        }

        /*
         * if the tuple is null, then we assume there is nothing more to
         * process so we just end the loop...
         */
        if (TupIsNull(slot)) {
            if(!is_saved_recursive_union_plan_nodeid) {
                break;
            }
            ExecEarlyFreeBody(planstate);
            break;
        }

        /*
         * If we have a junk filter, then project a new tuple with the junk
         * removed.
         *
         * Store this new "clean" tuple in the junkfilter's resultSlot.
         * (Formerly, we stored it back over the "dirty" tuple, which is WRONG
         * because that tuple slot has the wrong descriptor.)
         */
#ifdef ENABLE_MULTIPLE_NDOES
        if (estate->es_junkFilter != NULL && !StreamTopConsumerAmI() && !StreamThreadAmI()) {
#else
        if (estate->es_junkFilter != NULL && !StreamThreadAmI()) {
#endif
            /* If junkfilter->jf_resultSlot->tts_tupleDescriptor is different from slot->tts_tupleDescriptor,
             * and the datatype is not Compatible,
             * we  reset junkfilter->jf_resultSlot->tts_tupleDescriptor by slot->tts_tupleDescriptor.
             * This just do only once.
             */
            if (current_tuple_count == 0) {
                ExecSetjunkFilteDescriptor(estate->es_junkFilter, slot->tts_tupleDescriptor);
            }
            slot = ExecFilterJunk(estate->es_junkFilter, slot);
        }

#if defined(ENABLE_MULTIPLE_NDOES) || defined(USE_SPQ)
        if (t_thrd.spq_ctx.spq_role != ROLE_UTILITY && stream_instrument) {
            t_thrd.pgxc_cxt.GlobalNetInstr = planstate->instrument;
        }
#endif
        /*
         * If we are supposed to send the tuple somewhere, do so. (In
         * practice, this is probably always the case at this point.)
         */
#ifdef ENABLE_MULTIPLE_NDOES
        if (sendTuples && !u_sess->exec_cxt.executorStopFlag)
#else
        if (sendTuples)
#endif
        {
            (*dest->receiveSlot)(slot, dest);
        }

#ifdef ENABLE_MULTIPLE_NDOES
        t_thrd.pgxc_cxt.GlobalNetInstr = NULL;
#endif
        /*
         * Count tuples processed, if this is a SELECT.  (For other operation
         * types, the ModifyTable plan node must count the appropriate
         * events.)
         */
        if (operation == CMD_SELECT) {
            (estate->es_processed)++;
        }

        /*
         * check our tuple count.. if we've processed the proper number then
         * quit, else loop again and process more tuples.  Zero numberTuples
         * means no limit.
         */
        current_tuple_count++;
        if (numberTuples == current_tuple_count) {
            break;
        }
    }

    /* end of plan, we should flush the record for pre-read process */
    if (u_sess->storage_cxt.is_in_pre_read) {
        /* it's useless to record the record for last time, because it will be mod of the blocks */
        int minValue = u_sess->storage_cxt.bulk_read_max == 1 ? 1 : u_sess->storage_cxt.bulk_read_min;
        ereport(LOG, (errmsg("End of pre-Read, the max blocks batch is %d, the small blocks batch is %d.",
                u_sess->storage_cxt.bulk_read_max, minValue)));
    }

    /*
     * if current plan is working for expression, no need to collect instrumentation.
     */
    if (
#ifndef ENABLE_MULTIPLE_NODES
        !u_sess->attr.attr_common.enable_seqscan_fusion &&
#endif
        estate->es_instrument != INSTRUMENT_NONE &&
        u_sess->instr_cxt.global_instr && StreamTopConsumerAmI() && u_sess->instr_cxt.thread_instr) {
        int64 peak_memory = (uint64)(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->peakChunksQuery -
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->initMemInChunks)
            << (chunkSizeInBits - BITS_IN_MB);
        u_sess->instr_cxt.global_instr->SetPeakNodeMemory(planstate->plan->plan_node_id, peak_memory);
    }
}

/* ----------------------------------------------------------------
 * 		ExecutePlan
 *
 * 		Processes the query plan until we have retrieved 'numberTuples' tuples,
 * 		moving in the specified direction.
 *
 * 		Runs to completion if numberTuples is 0
 *
 * Note: the ctid attribute is a 'junk' attribute that is removed before the
 * user can see it
 * ----------------------------------------------------------------
 */
static void ExecuteVectorizedPlan(EState *estate, PlanState *planstate, CmdType operation, bool sendTuples,
    long numberTuples, ScanDirection direction, DestReceiver *dest)
{
    VectorBatch *batch = NULL;
    long current_tuple_count;
    bool stream_instrument = false;

    /*
     * initialize local variables
     */
    current_tuple_count = 0;

    /*
     * Set the direction.
     */
    estate->es_direction = direction;

    if (IS_PGXC_DATANODE) {
        /* Collect Executor run time including sending data time */
        if (estate->es_instrument != INSTRUMENT_NONE && u_sess->instr_cxt.global_instr) {
            stream_instrument = true;
            int plan_id = planstate->plan->plan_node_id;
            u_sess->instr_cxt.global_instr->SetStreamSend(plan_id, true);
        }

        /* Collect Material for Subplan first */
        ExecCollectMaterialForSubplan(estate);
    }

    /*
     * Loop until we've processed the proper number of tuples from the plan.
     */
    for (;;) {
        /* Reset the per-output-tuple exprcontext */
        ResetPerTupleExprContext(estate);

        /*
         * Execute the plan and obtain a tuple
         */
        batch = VectorEngine(planstate);

        /*
         * if the tuple is null, then we assume there is nothing more to
         * process so we just end the loop...
         */
        if (BatchIsNull(batch)) {
            ExecEarlyFree(planstate);
            break;
        }

        /*
         * If we have a junk filter, then project a new tuple with the junk
         * removed.
         *
         * Store this new "clean" tuple in the junkfilter's resultSlot.
         * (Formerly, we stored it back over the "dirty" tuple, which is WRONG
         * because that tuple slot has the wrong descriptor.)
         */
#ifdef ENABLE_MULTIPLE_NDOES
        if (estate->es_junkFilter != NULL && !StreamTopConsumerAmI() && !StreamThreadAmI()) {
#else
        if (estate->es_junkFilter != NULL && !StreamThreadAmI()) {
#endif
            BatchExecFilterJunk(estate->es_junkFilter, batch);
        }


        if (stream_instrument) {
            t_thrd.pgxc_cxt.GlobalNetInstr = planstate->instrument;
        }

        /*
         * If we are supposed to send the tuple somewhere, do so. (In
         * practice, this is probably always the case at this point.)
         */
        if (sendTuples && !u_sess->exec_cxt.executorStopFlag) {
            (*dest->sendBatch)(batch, dest);
        }

        t_thrd.pgxc_cxt.GlobalNetInstr = NULL;

        /*
         * Count tuples processed, if this is a SELECT.  (For other operation
         * types, the ModifyTable plan node must count the appropriate
         * events.)
         */
        if (operation == CMD_SELECT) {
            estate->es_processed += batch->m_rows;
        }

        /*
         * check our tuple count.. if we've processed the proper number then
         * quit, else loop again and process more tuples.  Zero numberTuples
         * means no limit.
         */
        current_tuple_count += batch->m_rows;
        if (numberTuples && numberTuples == current_tuple_count) {
            break;
        }
    }

    /*
     * if current plan is working for expression, no need to collect instrumentation.
     */
    if (estate->es_instrument != INSTRUMENT_NONE && u_sess->instr_cxt.global_instr && StreamTopConsumerAmI()) {
        int64 peak_memory = (uint64)(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->peakChunksQuery -
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->initMemInChunks)
            << (chunkSizeInBits - BITS_IN_MB);
        u_sess->instr_cxt.global_instr->SetPeakNodeMemory(planstate->plan->plan_node_id, peak_memory);
    }
}

/*
 * ExecRelCheck --- check that tuple meets constraints for result relation
 */
static const char *ExecRelCheck(ResultRelInfo *resultRelInfo, TupleTableSlot *slot, EState *estate)
{
    Relation rel = resultRelInfo->ri_RelationDesc;
    int ncheck = rel->rd_att->constr->num_check;
    ConstrCheck *check = rel->rd_att->constr->check;
    ExprContext *econtext = NULL;
    MemoryContext oldContext;
    List *qual = NIL;
    int i;

    /*
     * If first time through for this result relation, build expression
     * nodetrees for rel's constraint expressions.  Keep them in the per-query
     * memory context so they'll survive throughout the query.
     */
    if (resultRelInfo->ri_ConstraintExprs == NULL) {
        oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
        resultRelInfo->ri_ConstraintExprs = (List **)palloc(ncheck * sizeof(List *));
        for (i = 0; i < ncheck; i++) {

            if (estate->es_is_flt_frame){
                resultRelInfo->ri_ConstraintExprs[i] = (List*)ExecPrepareExpr((Expr*)stringToNode(check[i].ccbin), estate);
            } else {
                qual = make_ands_implicit((Expr*)stringToNode(check[i].ccbin));
                resultRelInfo->ri_ConstraintExprs[i] = (List*)ExecPrepareExpr((Expr *)qual,estate);
            }
        }
        (void)MemoryContextSwitchTo(oldContext);
    }

    /*
     * We will use the EState's per-tuple context for evaluating constraint
     * expressions (creating it if it's not already there).
     */
    econtext = GetPerTupleExprContext(estate);

    /* Arrange for econtext's scan tuple to be the tuple under test */
    econtext->ecxt_scantuple = slot;

    /* And evaluate the constraints */
    for (i = 0; i < ncheck; i++) {
        qual = resultRelInfo->ri_ConstraintExprs[i];

        /*
         * NOTE: SQL92 specifies that a NULL result from a constraint
         * expression is not to be treated as a failure.  Therefore, tell
         * ExecQual to return TRUE for NULL.
         */
        if (estate->es_is_flt_frame){
            if (!ExecCheckByFlatten((ExprState*)qual, econtext)){
                return check[i].ccname;
            }
        } else {
            if (!ExecQual(qual, econtext, true)){
                return check[i].ccname;
            }
        }
    }

    /* NULL result means no error */
    return NULL;
}

bool ExecConstraints(ResultRelInfo *resultRelInfo, TupleTableSlot *slot, EState *estate, bool skipAutoInc)
{
    Relation rel = resultRelInfo->ri_RelationDesc;
    TupleDesc tupdesc = RelationGetDescr(rel);
    TupleConstr *constr = tupdesc->constr;
    Bitmapset *modifiedCols = NULL;
    Bitmapset *insertedCols = NULL;
    Bitmapset *updatedCols = NULL;
    int maxfieldlen = 64;

    Assert(constr);

    /* Get the Table Accessor Method*/
    Assert(slot != NULL && slot->tts_tupleDescriptor != NULL);
    if (constr->has_not_null) {
        int natts = tupdesc->natts;
        int attrChk;

        for (attrChk = 1; attrChk <= natts; attrChk++) {
            if (tupdesc->attrs[attrChk - 1].attnotnull && tableam_tslot_attisnull(slot, attrChk)) {
                 /* Skip auto_increment attribute not null check, ExecAutoIncrement will deal with it. */
                if (skipAutoInc && constr->cons_autoinc && constr->cons_autoinc->attnum == attrChk) {
                    continue;
                }
                char *val_desc = NULL;
                bool rel_masked = u_sess->attr.attr_security.Enable_Security_Policy &&
                    is_masked_relation_enabled(RelationGetRelid(rel));

                insertedCols = GetInsertedColumns(resultRelInfo, estate);
                updatedCols = GetUpdatedColumns(resultRelInfo, estate);
                modifiedCols = bms_union(insertedCols, updatedCols);
                if (!rel_masked) {
                    val_desc =
                        ExecBuildSlotValueDescription(RelationGetRelid(rel), slot, tupdesc, modifiedCols, maxfieldlen);
                }

                bool can_ignore = estate->es_plannedstmt && estate->es_plannedstmt->hasIgnore;
                ereport(can_ignore ? WARNING : ERROR, (errcode(ERRCODE_NOT_NULL_VIOLATION),
                    errmsg("null value in column \"%s\" violates not-null constraint",
                    NameStr(tupdesc->attrs[attrChk - 1].attname)),
                    val_desc ? errdetail("Failing row contains %s.", val_desc) : 0));
                return false;
            }
        }
    }

    if (constr->num_check == 0) {
        return true;
    }

    const char *failed = ExecRelCheck(resultRelInfo, slot, estate);
    if (failed == NULL) {
        return true;
    }

    char *val_desc = NULL;
    bool rel_masked = u_sess->attr.attr_security.Enable_Security_Policy &&
        is_masked_relation_enabled(RelationGetRelid(rel));
    insertedCols = GetInsertedColumns(resultRelInfo, estate);
    updatedCols = GetUpdatedColumns(resultRelInfo, estate);
    modifiedCols = bms_union(insertedCols, updatedCols);
    if (!rel_masked) {
        val_desc = ExecBuildSlotValueDescription(RelationGetRelid(rel), slot, tupdesc, modifiedCols, maxfieldlen);
    }
    /* client_min_messages < NOTICE show error details. */
    if (client_min_messages < NOTICE) {
        ereport(ERROR, 
            (errmodule(MOD_EXECUTOR), errcode(ERRCODE_CHECK_VIOLATION),
                errmsg("new row for relation \"%s\" violates check constraint \"%s\"",
                    RelationGetRelationName(rel), failed),
                val_desc ? errdetail("Failing row contains %s.", val_desc) : 0,
                errcause("some rows copy failed"),
                erraction("check table defination")));
    } else {
        ereport(ERROR, 
            (errmodule(MOD_EXECUTOR), errcode(ERRCODE_CHECK_VIOLATION),
                errmsg("new row for relation \"%s\" violates check constraint \"%s\"",
                    RelationGetRelationName(rel), failed),
                errdetail("N/A"),
                errcause("some rows copy failed"),
                erraction("set client_min_messages = info for more details")));
    }
    return true;
}

/*
 * ExecWithCheckOptions -- check that tuple satisfies any WITH CHECK OPTIONs
 */
void ExecWithCheckOptions(ResultRelInfo *resultRelInfo, TupleTableSlot *slot, EState *estate)
{
    Relation rel = resultRelInfo->ri_RelationDesc;
    TupleDesc tupdesc = RelationGetDescr(rel);
    ExprContext* econtext = NULL;
    ListCell *l1 = NULL, *l2 = NULL;

    /*
     * We will use the EState's per-tuple context for evaluating constraint
     * expressions (creating it if it's not already there).
     */
    econtext = GetPerTupleExprContext(estate);

    /* Arrange for econtext's scan tuple to be the tuple under test */
    econtext->ecxt_scantuple = slot;

    /* Check each of the constraints */
    forboth (l1, resultRelInfo->ri_WithCheckOptions, l2, resultRelInfo->ri_WithCheckOptionExprs) {
        WithCheckOption* wco = (WithCheckOption*)lfirst(l1);
        ExprState* wcoExpr = (ExprState*)lfirst(l2);
        Bitmapset* modifiedCols = NULL;
        Bitmapset *insertedCols = NULL;
        Bitmapset *updatedCols = NULL;
        char* val_desc = NULL;

        insertedCols = GetInsertedColumns(resultRelInfo, estate);
        updatedCols = GetUpdatedColumns(resultRelInfo, estate);
        modifiedCols = bms_union(insertedCols, updatedCols);
        val_desc = ExecBuildSlotValueDescription(RelationGetRelid(rel),
                                                 slot,
                                                 tupdesc,
                                                 modifiedCols,
                                                 64);

        /*
         * WITH CHECK OPTION checks are intended to ensure that the new tuple
         * is visible in the view.  If the view's qual evaluates to NULL, then
         * the new tuple won't be included in the view.  Therefore we need to
         * tell ExecQual to return FALSE for NULL (the opposite of what we do
         * above for CHECK constraints).
         */
        if (!ExecQual((List*)wcoExpr, econtext, false))
            ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),
                    errmsg("new row violates WITH CHECK OPTION for view \"%s\"", wco->viewname),
                    val_desc ? errdetail("Failing row contains %s.", val_desc) : 0));
    }
}

/*
 * ExecBuildSlotValueDescription -- construct a string representing a tuple
 *
 * This is intentionally very similar to BuildIndexValueDescription, but
 * unlike that function, we truncate long field values (to at most maxfieldlen
 * bytes). That seems necessary here since heap field values could be very
 * long, whereas index entries typically aren't so wide.
 *
 * Also, unlike the case with index entries, we need to be prepared to ignore
 * dropped columns.  We used to use the slot's tuple descriptor to decode the
 * data, but the slot's descriptor doesn't identify dropped columns, so we
 * now need to be passed the relation's descriptor.
 *
 * Note that, like BuildIndexValueDescription, if the user does not have
 * permission to view any of the columns involved, a NULL is returned.  Unlike
 * BuildIndexValueDescription, if the user has access to view a subset of the
 * column involved, that subset will be returned with a key identifying which
 * columns they are.
 */
char *ExecBuildSlotValueDescription(Oid reloid, TupleTableSlot *slot, TupleDesc tupdesc, Bitmapset *modifiedCols,
    int maxfieldlen)
{
    StringInfoData buf;
    StringInfoData collist;
    bool write_comma = false;
    bool write_comma_collist = false;
    int i;

    AclResult aclresult;
    bool table_perm = false;
    bool any_perm = false;

    initStringInfo(&buf);

    appendStringInfoChar(&buf, '(');

    /*
     * Check if the user has permissions to see the row.  Table-level SELECT
     * allows access to all columns.  If the user does not have table-level
     * SELECT then we check each column and include those the user has SELECT
     * rights on.  Additionally, we always include columns the user provided
     * data for.
     */
    aclresult = pg_class_aclcheck(reloid, GetUserId(), ACL_SELECT);
    if (aclresult != ACLCHECK_OK) {
        /* Set up the buffer for the column list */
        initStringInfo(&collist);
        appendStringInfoChar(&collist, '(');
    } else {
        table_perm = any_perm = true;
    }

    /* Make sure the tuple is fully deconstructed */

    /* Get the Table Accessor Method*/
    Assert(slot != NULL && slot->tts_tupleDescriptor != NULL);
    tableam_tslot_getallattrs(slot);

    for (i = 0; i < tupdesc->natts; i++) {
        bool column_perm = false;
        char *val = NULL;
        int vallen;

        /* ignore dropped columns */
        if (tupdesc->attrs[i].attisdropped) {
            continue;
        }

        if (!table_perm) {
            /*
             * No table-level SELECT, so need to make sure they either have
             * SELECT rights on the column or that they have provided the
             * data for the column.  If not, omit this column from the error
             * message.
             */
            aclresult = pg_attribute_aclcheck(reloid, tupdesc->attrs[i].attnum, GetUserId(), ACL_SELECT);
            if (bms_is_member(tupdesc->attrs[i].attnum - FirstLowInvalidHeapAttributeNumber, modifiedCols) ||
                aclresult == ACLCHECK_OK) {
                column_perm = any_perm = true;

                if (write_comma_collist) {
                    appendStringInfoString(&collist, ", ");
                } else {
                    write_comma_collist = true;
                }

                appendStringInfoString(&collist, NameStr(tupdesc->attrs[i].attname));
            }
        }

        if (table_perm || column_perm) {
            if (slot->tts_isnull[i]) {
                val = "null";
            } else {
                Oid foutoid;
                bool typisvarlena = false;

                getTypeOutputInfo(tupdesc->attrs[i].atttypid, &foutoid, &typisvarlena);
                val = OidOutputFunctionCall(foutoid, slot->tts_values[i]);
            }

            if (write_comma) {
                appendStringInfoString(&buf, ", ");
            } else {
                write_comma = true;
            }

            /* truncate if needed */
            vallen = strlen(val);
            if (vallen <= maxfieldlen) {
                appendStringInfoString(&buf, val);
            } else {
                vallen = pg_mbcliplen(val, vallen, maxfieldlen);
                appendBinaryStringInfo(&buf, val, vallen);
                appendStringInfoString(&buf, "...");
            }
        }
    }

    /* If we end up with zero columns being returned, then return NULL. */
    if (!any_perm) {
        return NULL;
    }

    appendStringInfoChar(&buf, ')');

    if (!table_perm) {
        appendStringInfoString(&collist, ") = ");
        appendStringInfoString(&collist, buf.data);

        return collist.data;
    }

    return buf.data;
}

/*
 * ExecFindRowMark -- find the ExecRowMark struct for given rangetable index
 */
ExecRowMark *ExecFindRowMark(EState *estate, Index rti)
{
    ListCell *lc = NULL;

    foreach (lc, estate->es_rowMarks) {
        ExecRowMark *erm = (ExecRowMark *)lfirst(lc);

        if (erm->rti == rti) {
            return erm;
        }
    }
    ereport(ERROR,
        (errcode(ERRCODE_FETCH_DATA_FAILED), errmsg("failed to find ExecRowMark for rangetable index %u", rti)));
    return NULL; /* keep compiler quiet */
}

/*
 * ExecBuildAuxRowMark -- create an ExecAuxRowMark struct
 *
 * Inputs are the underlying ExecRowMark struct and the targetlist of the
 * input plan node (not planstate node!).  We need the latter to find out
 * the column numbers of the resjunk columns.
 */
ExecAuxRowMark *ExecBuildAuxRowMark(ExecRowMark *erm, List *targetlist)
{
    ExecAuxRowMark *aerm = (ExecAuxRowMark *)palloc0(sizeof(ExecAuxRowMark));
    char resname[32];
    errno_t rc = 0;

    aerm->rowmark = erm;

    /* Look up the resjunk columns associated with this rowmark */
    if (erm->relation) {
        Assert(erm->markType != ROW_MARK_COPY);

        /* if child rel, need tableoid */
        if (erm->rti != erm->prti || RelationIsPartitioned(erm->relation)) {
            rc = snprintf_s(resname, sizeof(resname), sizeof(resname) - 1, "tableoid%u", erm->rowmarkId);
            securec_check_ss(rc, "\0", "\0");

            aerm->toidAttNo = ExecFindJunkAttributeInTlist(targetlist, resname);
            if (!AttributeNumberIsValid(aerm->toidAttNo)) {
                ereport(ERROR, (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE),
                    errmsg("could not find tableoid junk %s column when build RowMark", resname)));
            }
        }

        if (RELATION_OWN_BUCKET(erm->relation)) {
            rc = snprintf_s(resname, sizeof(resname), sizeof(resname) - 1, "tablebucketid%u", erm->rowmarkId);
            securec_check_ss(rc, "\0", "\0");
            aerm->tbidAttNo = ExecFindJunkAttributeInTlist(targetlist, resname);
            if (!AttributeNumberIsValid(aerm->tbidAttNo)) {
                ereport(ERROR, (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE),
                    errmsg("could not find bucketid junk %s column when build RowMark", resname)));
            }
        }
        /* always need ctid for real relations */
        rc = snprintf_s(resname, sizeof(resname), sizeof(resname) - 1, "ctid%u", erm->rowmarkId);
        securec_check_ss(rc, "\0", "\0");

        aerm->ctidAttNo = ExecFindJunkAttributeInTlist(targetlist, resname);
        if (!AttributeNumberIsValid(aerm->ctidAttNo)) {
            ereport(ERROR, (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE),
                errmsg("could not find ctid junk %s column when build RowMark", resname)));
        }
    } else {
        Assert(erm->markType == ROW_MARK_COPY || erm->markType == ROW_MARK_COPY_DATUM);

        rc = snprintf_s(resname, sizeof(resname), sizeof(resname) - 1, "wholerow%u", erm->rowmarkId);
        securec_check_ss(rc, "\0", "\0");

        aerm->wholeAttNo = ExecFindJunkAttributeInTlist(targetlist, resname);
        if (!AttributeNumberIsValid(aerm->wholeAttNo)) {
            ereport(ERROR, (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE),
                errmsg("could not find whole-row junk %s column when build RowMark", resname)));
        }
    }

    return aerm;
}

TupleTableSlot *EvalPlanQualUHeap(EState *estate, EPQState *epqstate, Relation relation, Index rti, ItemPointer tid,
    TransactionId priorXmax)
{
    TupleTableSlot *slot     = NULL;
    UHeapTuple     copyTuple = NULL;

    Assert(rti > 0);

    copyTuple =
        UHeapLockUpdated(estate->es_output_cid, relation, LockTupleExclusive, tid, priorXmax, estate->es_snapshot);

    if (copyTuple == NULL) {
        return NULL;
    }

    Assert(copyTuple->tupTableType = UHEAP_TUPLE);

    *tid = copyTuple->ctid;

    EvalPlanQualBegin(epqstate, estate);

    EvalPlanQualSetTuple(epqstate, rti, copyTuple);

    EvalPlanQualFetchRowMarks(epqstate);

    slot = EvalPlanQualNext(epqstate);

    // materialize the slot
    if (!TupIsNull(slot)) {
        ExecGetUHeapTupleFromSlot(slot);
    }

    EvalPlanQualSetTuple(epqstate, rti, NULL);

    return slot;
}

/*
 * EvalPlanQual logic --- recheck modified tuple(s) to see if we want to
 * process the updated version under READ COMMITTED rules.
 *
 * See gausskernel/runtime/executor/README for some info about how this works.
 *
 * Check a modified tuple to see if we want to process its updated version
 * under READ COMMITTED rules.
 *
 * 	estate - outer executor state data
 * 	epqstate - state for EvalPlanQual rechecking
 * 	relation - table containing tuple
 * 	rti - rangetable index of table containing tuple
 *  lockmode - requested tuple lock mode
 * 	*tid - t_ctid from the outdated tuple (ie, next updated version)
 * 	priorXmax - t_xmax from the outdated tuple
 *
 * *tid is also an output parameter: it's modified to hold the TID of the
 * latest version of the tuple (note this may be changed even on failure)
 *
 * Returns a slot containing the new candidate update/delete tuple, or
 * NULL if we determine we shouldn't process the row.
 *
 * Note: properly, lockmode should be declared as enum LockTupleMode,
 * but we use "int" to avoid having to include heapam.h in executor.h.
 */
TupleTableSlot *EvalPlanQual(EState *estate, EPQState *epqstate, Relation relation, Index rti, int lockmode,
    ItemPointer tid, TransactionId priorXmax, bool partRowMoveUpdate)
{
    TupleTableSlot *slot = NULL;
    Tuple copyTuple;

    Assert(rti > 0);

    /*
     * Get and lock the updated version of the row; if fail, return NULL.
     */
    copyTuple = tableam_tuple_lock_updated(estate->es_output_cid, relation, lockmode, tid, priorXmax,
        estate->es_snapshot);

    if (copyTuple == NULL) {
        /*
         * The tuple has been deleted or update in row movement case.
         */
        if (partRowMoveUpdate) {
            /*
             * the may be a row movement update action which delete tuple from original
             * partition and insert tuple to new partition or we can add lock on the tuple
             * to be delete or updated to avoid throw exception.
             */
            ereport(ERROR, (errcode(ERRCODE_TRANSACTION_ROLLBACK),
                errmsg("partition table update conflict"),
                errdetail("disable row movement of table can avoid this conflict")));
        }
        return NULL;
    }

    /*
     * For UPDATE/DELETE we have to return tid of actual row we're executing
     * PQ for.
     */
    *tid = ((HeapTuple)copyTuple)->t_self;

    /*
     * Need to run a recheck subquery.	Initialize or reinitialize EPQ state.
     */
    EvalPlanQualBegin(epqstate, estate);

    /*
     * Free old test tuple, if any, and store new tuple where relation's scan
     * node will see it
     */
    EvalPlanQualSetTuple(epqstate, rti, copyTuple);

    /*
     * Fetch any non-locked source rows
     */
    EvalPlanQualFetchRowMarks(epqstate);

    /*
     * Run the EPQ query.  We assume it will return at most one tuple.
     */
    slot = EvalPlanQualNext(epqstate);

    /*
     * If we got a tuple, force the slot to materialize the tuple so that it
     * is not dependent on any local state in the EPQ query (in particular,
     * it's highly likely that the slot contains references to any pass-by-ref
     * datums that may be present in copyTuple).  As with the next step, this
     * is to guard against early re-use of the EPQ query.
     */
    if (!TupIsNull(slot)) {
        (void)tableam_tslot_get_tuple_from_slot(relation, slot);
    }

    /*
     * Clear out the test tuple.  This is needed in case the EPQ query is
     * re-used to test a tuple for a different relation.  (Not clear that can
     * really happen, but let's be safe.)
     */
    EvalPlanQualSetTuple(epqstate, rti, NULL);

    return slot;
}

/*
 * Fetch a copy of the newest version of an outdated tuple
 *
 *  cid - command ID
 * 	relation - table containing tuple
 * 	lockmode - requested tuple lock mode
 * 	*tid - t_ctid from the outdated tuple (ie, next updated version)
 * 	priorXmax - t_xmax from the outdated tuple
 *
 * Returns a palloc'd copy of the newest tuple version, or NULL if we find
 * that there is no newest version (ie, the row was deleted not updated).
 * If successful, we have locked the newest tuple version, so caller does not
 * need to worry about it changing anymore.
 *
 * Note: properly, lockmode should be declared as enum LockTupleMode,
 * but we use "int" to avoid having to include heapam.h in executor.h.
 */
HeapTuple heap_lock_updated(CommandId cid, Relation relation, int lockmode, ItemPointer tid, TransactionId priorXmax)
{
    HeapTuple copyTuple = NULL;
    HeapTupleData tuple;
    SnapshotData SnapshotDirty;
    union {
        HeapTupleHeaderData hdr;
        char data[MaxHeapTupleSize + sizeof(HeapTupleHeaderData)];
    } tbuf;
    errno_t errorno = EOK;
    errorno = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
    securec_check(errorno, "\0", "\0");

    /*
     * fetch target tuple
     *
     * Loop here to deal with updated or busy tuples
     */
    InitDirtySnapshot(SnapshotDirty);
    tuple.t_self = *tid;
    tuple.t_data = &(tbuf.hdr);
    for (;;) {
        Buffer buffer;
        bool fetched = tableam_tuple_fetch(relation, &SnapshotDirty, &tuple, &buffer, true, NULL);

        if (fetched) {
            TM_Result test;
            TM_FailureData tmfd;

            HeapTupleCopyBaseFromPage(&tuple, BufferGetPage(buffer));

            /*
             * If xmin isn't what we're expecting, the slot must have been
             * recycled and reused for an unrelated tuple.	This implies that
             * the latest version of the row was deleted, so we need do
             * nothing.  (Should be safe to examine xmin without getting
             * buffer's content lock, since xmin never changes in an existing
             * tuple.)
             */
            if (!TransactionIdEquals(HeapTupleGetRawXmin(&tuple), priorXmax)) {
                ReleaseBuffer(buffer);
                return NULL;
            }

            /* otherwise xmin should not be dirty... */
            if (TransactionIdIsValid(SnapshotDirty.xmin)) {
                /*
                 * for transaction's commit, it is first set csnlog, and then
                 * removed from procarray, once the csnlog is set, the parent xid
                 * ismissing from the sub xid's csnlog, so you may not wait the top
                 * parent xid until it is removed from procarray for this
                 * condition. So we add a check here and retune the xmin to
                 * invalid if it is already committed.
                 */
                if (TransactionIdDidCommit(SnapshotDirty.xmin)) {
                    elog(DEBUG2,
                        "t_xmin %lu is committed in clog, but still"
                        " in procarray, so set it back to invalid.",
                        SnapshotDirty.xmin);
                    SnapshotDirty.xmin = InvalidTransactionId;
                } else {
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                            errmsg("t_xmin is uncommitted in tuple to be updated")));
                }
            }

            /*
             * If tuple is being updated by other transaction then we have to
             * wait for its commit/abort.
             */
            if (TransactionIdIsValid(SnapshotDirty.xmax)) {
                ReleaseBuffer(buffer);

                if (!u_sess->attr.attr_common.allow_concurrent_tuple_update)
                    ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                        errmsg("abort transaction due to concurrent update"),
                        errhint("Try to turn on GUC allow_concurrent_tuple_update if concurrent update is expected.")));
                XactLockTableWait(SnapshotDirty.xmax);
                continue; /* loop back to repeat heap_fetch */
            }

            /*
             * If tuple was inserted by our own transaction, we have to check
             * cmin against es_output_cid: cmin >= current CID means our
             * command cannot see the tuple, so we should ignore it.  Without
             * this we are open to the "Halloween problem" of indefinitely
             * re-updating the same tuple. (We need not check cmax because
             * HeapTupleSatisfiesDirty will consider a tuple deleted by our
             * transaction dead, regardless of cmax.)  We just checked that
             * priorXmax == xmin, so we can test that variable instead of
             * doing HeapTupleHeaderGetXmin again.
             */
            if (TransactionIdIsCurrentTransactionId(priorXmax) &&
                HeapTupleHeaderGetCmin(tuple.t_data, BufferGetPage(buffer)) >= cid) {
                ReleaseBuffer(buffer);
                return NULL;
            }

            /*
             * This is a live tuple, so now try to lock it.
             */
            test = tableam_tuple_lock(relation, &tuple, &buffer, 
                                      cid, (LockTupleMode)lockmode, LockWaitBlock, &tmfd,
                                      false, false, false,InvalidSnapshot, NULL, false);
            /* We now have two pins on the buffer, get rid of one */
            ReleaseBuffer(buffer);

            switch (test) {
                case TM_SelfCreated:
                    ReleaseBuffer(buffer);
                    ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                        errmsg("attempted to lock invisible tuple")));
                    break;
                case TM_SelfModified:
                    /* treat it as deleted; do not process */
                    ReleaseBuffer(buffer);
                    return NULL;

                case TM_Ok:
                    /* successfully locked */
                    break;

                case TM_Updated:
                    ReleaseBuffer(buffer);
                    if (IsolationUsesXactSnapshot()) {
                        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                    }

                    Assert(!ItemPointerEquals(&tmfd.ctid, &tuple.t_self));
                    /* it was updated, so look at the updated version */
                    tuple.t_self = tmfd.ctid;
                    /* updated row should have xmin matching this xmax */
                    priorXmax = tmfd.xmax;
                    continue;
                    break; /* keep compiler quiet */

                case TM_Deleted:
                    ReleaseBuffer(buffer);
                    if (IsolationUsesXactSnapshot()) {
                        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                    }

                    Assert(ItemPointerEquals(&tmfd.ctid, &tuple.t_self));
                    /* tuple was deleted, so give up */
                    return NULL;

                default:
                    ReleaseBuffer(buffer);
                    ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                        errmsg("unrecognized heap_lock_tuple status: %u", test)));
                    return NULL; /* keep compiler quiet */
            }

            /*
             * We got tuple - now copy it for use by recheck query.
             */
            copyTuple = (HeapTuple)tableam_tops_copy_tuple(&tuple);
            ReleaseBuffer(buffer);
            break;
        }

        /*
         * If the referenced slot was actually empty, the latest version of
         * the row must have been deleted, so we need do nothing.
         */
        /*
         * As above, if xmin isn't what we're expecting, do nothing.
         */
        /*
         * If we get here, the tuple was found but failed SnapshotDirty.
         * Assuming the xmin is either a committed xact or our own xact (as it
         * certainly should be if we're trying to modify the tuple), this must
         * mean that the row was updated or deleted by either a committed xact
         * or our own xact.  If it was deleted, we can ignore it; if it was
         * updated then chain up to the next version and repeat the whole
         * process.
         *
         * As above, it should be safe to examine xmax and t_ctid without the
         * buffer content lock, because they can't be changing.
         */
        bool is_null = tuple.t_data == NULL ||
            !TransactionIdEquals(HeapTupleGetRawXmin(&tuple), priorXmax) ||
            ItemPointerEquals(&tuple.t_self, &tuple.t_data->t_ctid);
        if (is_null) {
            /* deleted, so forget about it */
            ReleaseBuffer(buffer);
            return NULL;
        }

        /* updated, so look at the updated row */
        tuple.t_self = tuple.t_data->t_ctid;
        /* updated row should have xmin matching this xmax */
        priorXmax = HeapTupleGetUpdateXid(&tuple);
        ReleaseBuffer(buffer);
        /* loop back to fetch next in chain */
    }

    /*
     * Return the copied tuple
     */
    return copyTuple;
}

/*
 * EvalPlanQualInit -- initialize during creation of a plan state node
 * that might need to invoke EPQ processing.
 *
 * Note: subplan/auxrowmarks can be NULL/NIL if they will be set later
 * with EvalPlanQualSetPlan. projInfos is used for Multiple-Modify to
 * fetch the slot corresponding to the target table.
 */
void EvalPlanQualInit(EPQState *epqstate, EState *estate, Plan *subplan, List *auxrowmarks, int epqParam,
    ProjectionInfo** projInfos)
{
    /* Mark the EPQ state inactive */
    epqstate->estate = NULL;
    epqstate->planstate = NULL;
    epqstate->origslot = NULL;
    /* ... and remember data that EvalPlanQualBegin will need */
    epqstate->plan = subplan;
    epqstate->arowMarks = auxrowmarks;
    epqstate->epqParam = epqParam;
    epqstate->parentestate = estate;
    epqstate->projInfos = projInfos;
}

/*
 * EvalPlanQualSetPlan -- set or change subplan of an EPQState.
 *
 * We need this so that ModifyTuple can deal with multiple subplans.
 */
void EvalPlanQualSetPlan(EPQState *epqstate, Plan *subplan, List *auxrowmarks)
{
    /* If we have a live EPQ query, shut it down */
    EvalPlanQualEnd(epqstate);
    /* And set/change the plan pointer */
    epqstate->plan = subplan;
    /* The rowmarks depend on the plan, too */
    epqstate->arowMarks = auxrowmarks;
}

/*
 * Install one test tuple into EPQ state, or clear test tuple if tuple == NULL
 *
 * NB: passed tuple must be palloc'd; it may get freed later
 */
void EvalPlanQualSetTuple(EPQState *epqstate, Index rti, Tuple tuple)
{
    EState *estate = epqstate->estate;
    Assert(rti > 0);

    /*
     * free old test tuple, if any, and store new tuple where relation's scan
     * node will see it
     */
    if (estate->es_epqTuple[rti - 1] != NULL) {
        tableam_tops_free_tuple(estate->es_epqTuple[rti - 1]);
    }
    estate->es_epqTuple[rti - 1] = tuple;
    estate->es_epqTupleSet[rti - 1] = true;
}

/*
 * Fetch back the current test tuple (if any) for the specified RTI
 */
Tuple EvalPlanQualGetTuple(EPQState *epqstate, Index rti)
{
    EState *estate = epqstate->estate;
    Assert(rti > 0);
    return estate->es_epqTuple[rti - 1];
}

/*
 * Return, and create if necessary, a slot for an EPQ test tuple.
 */
TupleTableSlot *EvalPlanQualUSlot(EPQState *epqstate, Relation relation, Index rti)
{
    TupleTableSlot **slot;

    // To adapt inplacetuple and tuple,we have to use slot instead of tuple here
    slot = &epqstate->estate->es_epqTupleSlot[rti - 1];

    if (*slot == NULL) {
        MemoryContext oldcontext = MemoryContextSwitchTo(epqstate->parentestate->es_query_cxt);

        *slot = ExecAllocTableSlot(&epqstate->estate->es_tupleTable, TableAmUstore);
        if (relation)
            ExecSetSlotDescriptor(*slot, RelationGetDescr(relation));
        else
            ExecSetSlotDescriptor(*slot, epqstate->origslot->tts_tupleDescriptor);

        MemoryContextSwitchTo(oldcontext);
    }

    (*slot)->tts_tam_ops = TableAmUstore;

    return *slot;
}

void EvalPlanQualFetchRowMarksUHeap(EPQState *epqstate)
{
    ListCell *l = NULL;
    UHeapTupleData utuple;

    union {
        UHeapDiskTupleData hdr;
        char data[MaxPossibleUHeapTupleSize];
    } tbuf;

    errno_t errorno = EOK;
    errorno = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
    securec_check(errorno, "\0", "\0");
    utuple.disk_tuple = &(tbuf.hdr);

    Assert(epqstate->origslot != NULL);

    foreach (l, epqstate->arowMarks) {
        ExecAuxRowMark *aerm = (ExecAuxRowMark *)lfirst(l);
        ExecRowMark *erm = aerm->rowmark;
        Datum datum;
        bool isNull;
        TupleTableSlot *slot = NULL;
        Oid tableoid = InvalidOid;

        if (RowMarkRequiresRowShareLock(erm->markType)) {
            elog(ERROR, "EvalPlanQual doesn't support locking rowmarks");
        }

        if (epqstate->estate->es_result_relation_info != NULL &&
            epqstate->estate->es_result_relation_info->ri_RangeTableIndex == erm->rti) {
            continue;
        }
        /* clear any leftover test tuple for this rel */
        slot = EvalPlanQualUSlot(epqstate, erm->relation, erm->rti);
        ExecClearTuple(slot);

        if (erm->rti != erm->prti) {
            datum = ExecGetJunkAttribute(epqstate->origslot, aerm->toidAttNo, &isNull);

            if (isNull) {
                continue;
            }

            tableoid = DatumGetObjectId(datum);
            if (tableoid != RelationGetRelid(erm->relation)) {
                continue;
            }
        }

        if (erm->markType == ROW_MARK_REFERENCE) {
            Assert(erm->relation != NULL);

            if (RELATION_IS_PARTITIONED(erm->relation)) {
                datum = ExecGetJunkAttribute(epqstate->origslot, aerm->toidAttNo, &isNull);

                /* non-locked rels could be on the inside of outer joins */
                if (isNull) {
                    continue;
                }
                tableoid = DatumGetObjectId(datum);
            }

            datum = ExecGetJunkAttribute(epqstate->origslot, aerm->ctidAttNo, &isNull);

            /* non-locked rels could be on the inside of outer joins */
            if (isNull) {
                continue;
            }

            /* fetch requests on foreign tables must be passed to their FDW */
            if (erm->relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE) {
            } else if (RELATION_IS_PARTITIONED(erm->relation)) {
            } else {
                if (!UHeapFetchRow(erm->relation, (ItemPointer)DatumGetPointer(datum), epqstate->estate->es_snapshot,
                    slot, &utuple)) {
                    elog(ERROR, "failed to fetch tuple for EvalPlanQual recheck");
                }
            }
        } else {
            if (erm->markType == ROW_MARK_COPY) {
                HeapTupleData tuple;
                HeapTupleHeader td;

                Assert(erm->markType == ROW_MARK_COPY);

                /* fetch the whole-row Var for the relation */
                datum = ExecGetJunkAttribute(epqstate->origslot, aerm->wholeAttNo, &isNull);

                /* non-locked rels could be on the inside of outer joins */
                if (isNull) {
                    continue;
                }

                td = DatumGetHeapTupleHeader(datum);
                tuple.t_len = HeapTupleHeaderGetDatumLength(td);
                tuple.t_self = td->t_ctid;
                tuple.t_data = td;

                ExecClearTuple(slot);

                tableam_tops_deform_tuple(&tuple, slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);
                ExecStoreVirtualTuple(slot);
            } else {
                Assert(erm->markType == ROW_MARK_COPY_DATUM);

                HeapTupleHeader td;
                HeapTupleData tuple;
                MemoryContext oldcxt;
                TupleDesc tupdesc;
                Form_pg_attribute attrs[erm->numAttrs];

                Datum *data = (Datum *)palloc0(sizeof(Datum) * erm->numAttrs);
                bool *null = (bool *)palloc0(sizeof(bool) * erm->numAttrs);

                for (int i = 0; i < erm->numAttrs; i++) {
                    data[i] = ExecGetJunkAttribute(epqstate->origslot, aerm->wholeAttNo + i, &null[i]);
                    attrs[i] = &epqstate->origslot->tts_tupleDescriptor->attrs[aerm->wholeAttNo - 1 + i];
                }

                oldcxt = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
                tupdesc = CreateTupleDesc(erm->numAttrs, false, attrs);
                MemoryContextSwitchTo(oldcxt);
                tupdesc->natts = erm->numAttrs;
                tupdesc->tdhasoid = false;
                ExecSetSlotDescriptor(slot, tupdesc);

                if (!slot->tts_per_tuple_mcxt) {
                    slot->tts_per_tuple_mcxt = AllocSetContextCreate(slot->tts_mcxt, "SlotPerTupleMcxt",
                        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
                }

                /*
                 * This memory is freed during ExecClearTuple().
                 * Note that we cannot free tmptuple right after deform_tuple because
                 * values in slot->tts_values would be pointing to it.
                 */
                oldcxt = MemoryContextSwitchTo(slot->tts_per_tuple_mcxt);
                HeapTuple tmptuple = heap_form_tuple(tupdesc, data, null);
                MemoryContextSwitchTo(oldcxt);

                td = (HeapTupleHeader)((char *)tmptuple + HEAPTUPLESIZE);
                tuple.t_len = HeapTupleHeaderGetDatumLength(td);
                tuple.t_self = td->t_ctid;
                tuple.t_data = td;
                pfree_ext(data);
                pfree_ext(null);

                tableam_tops_deform_tuple(&tuple, slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);
                ExecStoreVirtualTuple(slot);
            }
        }
    }
}

/*
 * Fetch the current row values for any non-locked relations that need
 * to be scanned by an EvalPlanQual operation.	origslot must have been set
 * to contain the current result row (top-level row) that we need to recheck.
 */
void EvalPlanQualFetchRowMarks(EPQState *epqstate)
{
    ListCell *l = NULL;
    union {
        HeapTupleHeaderData hdr;
        char data[MaxHeapTupleSize + sizeof(HeapTupleHeaderData)];
    } tbuf;
    errno_t errorno = EOK;
    errorno = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
    securec_check(errorno, "\0", "\0");

    Assert(epqstate->origslot != NULL);

    foreach (l, epqstate->arowMarks) {
        ExecAuxRowMark *aerm = (ExecAuxRowMark *)lfirst(l);
        ExecRowMark *erm = aerm->rowmark;
        Datum datum;
        bool isNull = false;
        HeapTupleData tuple;
        Oid tableoid = InvalidOid;
        int2 bucketid = InvalidBktId;

        if (RowMarkRequiresRowShareLock(erm->markType)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("EvalPlanQual doesn't support locking rowmarks")));
        }

        if (epqstate->estate->es_result_relation_info != NULL &&
            epqstate->estate->es_result_relation_info->ri_RangeTableIndex == erm->rti) {
            continue;
        }
        /* clear any leftover test tuple for this rel */
        EvalPlanQualSetTuple(epqstate, erm->rti, NULL);

        if (erm->relation) {
            Buffer buffer;

            Assert(erm->markType == ROW_MARK_REFERENCE);

            /* if child rel, must check whether it produced this row */
            if (erm->rti != erm->prti) {
                Oid tmp_tableoid;
                datum = ExecGetJunkAttribute(epqstate->origslot, aerm->toidAttNo, &isNull);
                /* non-locked rels could be on the inside of outer joins */
                if (isNull) {
                    continue;
                }

                tmp_tableoid = DatumGetObjectId(datum);

                if (tmp_tableoid != RelationGetRelid(erm->relation)) {
                    /* this child is inactive right now */
                    continue;
                }
            }

            if (RELATION_IS_PARTITIONED(erm->relation)) {
                datum = ExecGetJunkAttribute(epqstate->origslot, aerm->toidAttNo, &isNull);
                /* non-locked rels could be on the inside of outer joins */
                if (isNull) {
                    continue;
                }
                tableoid = DatumGetObjectId(datum);
            }

            if (RELATION_OWN_BUCKET(erm->relation)) {
                datum = ExecGetJunkAttribute(epqstate->origslot, aerm->tbidAttNo, &isNull);
                if (isNull) {
                    continue;
                }
                bucketid = DatumGetObjectId(datum);
            }
            /* fetch the tuple's ctid */
            datum = ExecGetJunkAttribute(epqstate->origslot, aerm->ctidAttNo, &isNull);
            /* non-locked rels could be on the inside of outer joins */
            if (isNull) {
                continue;
            }

            tuple.t_self = *((ItemPointer)DatumGetPointer(datum));
            /* Must set a private data buffer for heap_fetch */
            tuple.t_data = &tbuf.hdr;

            if (RELATION_IS_PARTITIONED(erm->relation)) {
                Partition p = partitionOpen(erm->relation, tableoid, NoLock, bucketid);
                Relation fakeRelation = partitionGetRelation(erm->relation, p);

                /* okay, fetch the tuple */
                if (!tableam_tuple_fetch(fakeRelation, SnapshotAny, &tuple, &buffer, false, NULL)) {
                    ereport(ERROR, (errcode(ERRCODE_FETCH_DATA_FAILED),
                        errmsg("failed to fetch tuple for EvalPlanQual recheck from partition relation.")));
                }

                releaseDummyRelation(&fakeRelation);
                partitionClose(erm->relation, p, NoLock);
            } else {
                Relation fakeRelation = erm->relation;
                if (RELATION_OWN_BUCKET(erm->relation)) {
                    Assert(bucketid != InvalidBktId);
                    fakeRelation = bucketGetRelation(erm->relation, NULL, bucketid);
                }
                if (!tableam_tuple_fetch(fakeRelation, SnapshotAny, &tuple, &buffer, false, NULL)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FETCH_DATA_FAILED), errmsg("failed to fetch tuple for EvalPlanQual recheck")));
                }

                if (RELATION_OWN_BUCKET(erm->relation)) {
                    bucketCloseRelation(fakeRelation);
                }
            }

            /* successful, copy and store tuple */
            EvalPlanQualSetTuple(epqstate, erm->rti, tableam_tops_copy_tuple(&tuple));
            ReleaseBuffer(buffer);
        } else {
            HeapTupleHeader td;

            if (erm->markType == ROW_MARK_COPY) {
                /* fetch the whole-row Var for the relation */
                datum = ExecGetJunkAttribute(epqstate->origslot, aerm->wholeAttNo, &isNull);
                /* non-locked rels could be on the inside of outer joins */
                if (isNull) {
                    continue;
                }

                td = DatumGetHeapTupleHeader(datum);
            } else {
                Assert(erm->markType == ROW_MARK_COPY_DATUM);
                Datum *data = (Datum *)palloc0(sizeof(Datum) * erm->numAttrs);
                bool *null = (bool *)palloc0(sizeof(bool) * erm->numAttrs);
                Form_pg_attribute attrs[erm->numAttrs];

                TupleDesc tupdesc = (TupleDesc)palloc0(sizeof(tupleDesc));
                for (int i = 0; i < erm->numAttrs; i++) {
                    data[i] = ExecGetJunkAttribute(epqstate->origslot, aerm->wholeAttNo + i, &null[i]);
                    attrs[i] = &epqstate->origslot->tts_tupleDescriptor->attrs[aerm->wholeAttNo - 1 + i];
                }
                
                tupdesc = CreateTupleDesc(erm->numAttrs, false, attrs);
                tupdesc->natts = erm->numAttrs;
                tupdesc->tdhasoid = false;
                tupdesc->tdisredistable = false;
                td = (HeapTupleHeader)((char *)heap_form_tuple(tupdesc, data, null) + HEAPTUPLESIZE);
                pfree_ext(data);
                pfree_ext(null);
                pfree_ext(tupdesc);
            }

            /* build a temporary HeapTuple control structure */
            tuple.t_len = HeapTupleHeaderGetDatumLength(td);
            ItemPointerSetInvalid(&(tuple.t_self));
            /* relation might be a foreign table, if so provide tableoid */
            tuple.t_tableOid = getrelid(erm->rti, epqstate->estate->es_range_table);
            tuple.t_bucketId = InvalidBktId;
            HeapTupleSetZeroBase(&tuple);
#ifdef PGXC
            tuple.t_xc_node_id = 0;
#endif
            tuple.t_data = td;

            /* copy and store tuple */
            EvalPlanQualSetTuple(epqstate, erm->rti, tableam_tops_copy_tuple(&tuple));
        }
    }
}

/*
 * Fetch the next row (if any) from EvalPlanQual testing
 *
 * (In practice, there should never be more than one row...)
 */
TupleTableSlot *EvalPlanQualNext(EPQState *epqstate)
{
    MemoryContext old_context = MemoryContextSwitchTo(epqstate->estate->es_query_cxt);
    int resultRelation = epqstate->estate->result_rel_index;
    ExprContext* origExprContext = NULL;

    TupleTableSlot *slot = ExecProcNode(epqstate->planstate);
    /* for multiple modify, fetch the current read slot corresponding to the result relation. */
    if (resultRelation > 0 && !epqstate->plan->isinherit) {
        origExprContext = epqstate->projInfos[resultRelation]->pi_exprContext;
        epqstate->projInfos[resultRelation]->pi_exprContext = epqstate->planstate->ps_ExprContext;

        slot = ExecProject(epqstate->projInfos[resultRelation], NULL);
        epqstate->projInfos[resultRelation]->pi_exprContext = origExprContext;
    }
    (void)MemoryContextSwitchTo(old_context);

    return slot;
}

/*
 * Initialize or reset an EvalPlanQual state tree
 */
void EvalPlanQualBegin(EPQState *epqstate, EState *parentestate, bool isUHeap)
{
    EState *estate = epqstate->estate;
    errno_t rc = 0;

    if (estate == NULL) {
        /* First time through, so create a child EState */
        EvalPlanQualStart(epqstate, parentestate, epqstate->plan, isUHeap);
    } else {
        /*
         * We already have a suitable child EPQ tree, so just reset it.
         */
        int rtsize = list_length(parentestate->es_range_table);
        PlanState *planstate = epqstate->planstate;

        rc = memset_s(estate->es_epqScanDone, rtsize * sizeof(bool), 0, rtsize * sizeof(bool));
        securec_check(rc, "\0", "\0");

        /* Recopy current values of parent parameters */
        if (parentestate->es_plannedstmt->nParamExec > 0) {
            int i = parentestate->es_plannedstmt->nParamExec;

            while (--i >= 0) {
                /* copy value if any, but not execPlan link */
                estate->es_param_exec_vals[i].value = parentestate->es_param_exec_vals[i].value;
                estate->es_param_exec_vals[i].isnull = parentestate->es_param_exec_vals[i].isnull;
            }
        }

        /*
         * Mark child plan tree as needing rescan at all scan nodes.  The
         * first ExecProcNode will take care of actually doing the rescan.
         */
        planstate->chgParam = bms_add_member(planstate->chgParam, epqstate->epqParam);
        estate->result_rel_index = parentestate->result_rel_index;
        estate->es_result_relation_info = parentestate->es_result_relation_info;
    }
}

/*
 * Start execution of an EvalPlanQual plan tree.
 *
 * This is a cut-down version of ExecutorStart(): we copy some state from
 * the top-level estate rather than initializing it fresh.
 */
void Setestate(EState *estate, EState *parentestate)
{
    estate->es_direction = ForwardScanDirection;
    estate->es_snapshot = parentestate->es_snapshot;
    estate->es_crosscheck_snapshot = parentestate->es_crosscheck_snapshot;
    estate->es_range_table = parentestate->es_range_table;
    estate->es_plannedstmt = parentestate->es_plannedstmt;
    estate->es_junkFilter = parentestate->es_junkFilter;
    estate->es_output_cid = parentestate->es_output_cid;
    estate->es_result_relations = parentestate->es_result_relations;
    estate->es_num_result_relations = parentestate->es_num_result_relations;
    estate->es_result_relation_info = parentestate->es_result_relation_info;
    estate->result_rel_index = parentestate->result_rel_index;
    if (estate->result_rel_index != 0) {
        estate->es_result_relation_info -= estate->result_rel_index;
    }
    estate->es_skip_early_free = parentestate->es_skip_early_free;
    estate->es_skip_early_deinit_consumer = parentestate->es_skip_early_deinit_consumer;

#ifdef PGXC
    /* XXX Check if this is OK */
    estate->es_result_remoterel = parentestate->es_result_remoterel;
#endif

    /* es_trig_target_relations must NOT be copied
     * es_auxmodifytables must NOT be copied
     */
    estate->es_rowMarks = parentestate->es_rowMarks;
    estate->es_top_eflags = parentestate->es_top_eflags;
    estate->es_instrument = parentestate->es_instrument;
    /*
     * The external param list is simply shared from parent.  The internal
     * param workspace has to be local state, but we copy the initial values
     * from the parent, so as to have access to any param values that were
     * already set from other parts of the parent's plan tree.
     */
    estate->es_param_list_info = parentestate->es_param_list_info;
}

static void EvalPlanQualStart(EPQState *epqstate, EState *parentestate, Plan *planTree, bool isUHeap)
{
    EState *estate = NULL;
    int rtsize;
    MemoryContext old_context;
    ListCell *l = NULL;

    rtsize = list_length(parentestate->es_range_table);

    epqstate->estate = estate = CreateExecutorState();

    old_context = MemoryContextSwitchTo(estate->es_query_cxt);
    Setestate(estate, parentestate);
    /*
     * Child EPQ EStates share the parent's copy of unchanging state such as
     * the snapshot, rangetable, result-rel info, and external Param info.
     * They need their own copies of local state, including a tuple table,
     * es_param_exec_vals, etc.
     */
    if (parentestate->es_plannedstmt->nParamExec > 0) {
        int i = parentestate->es_plannedstmt->nParamExec;

        estate->es_param_exec_vals = (ParamExecData *)palloc0(i * sizeof(ParamExecData));
        while (--i >= 0) {
            /* copy value if any, but not execPlan link */
            estate->es_param_exec_vals[i].value = parentestate->es_param_exec_vals[i].value;
            estate->es_param_exec_vals[i].isnull = parentestate->es_param_exec_vals[i].isnull;
        }
    }

    /*
     * Each EState must have its own es_epqScanDone state, but if we have
     * nested EPQ checks they should share es_epqTuple arrays.	This allows
     * sub-rechecks to inherit the values being examined by an outer recheck.
     */
    estate->es_epqScanDone = (bool *)palloc0(rtsize * sizeof(bool));
    if (parentestate->es_epqTuple != NULL) {
        estate->es_epqTuple = parentestate->es_epqTuple;
        estate->es_epqTupleSet = parentestate->es_epqTupleSet;
    } else {
        estate->es_epqTuple = (Tuple *)palloc0(rtsize * sizeof(HeapTuple));
        estate->es_epqTupleSet = (bool *)palloc0(rtsize * sizeof(bool));
    }

    /*
     * Each estate also has its own tuple table.
     */
    estate->es_tupleTable = NIL;

    if (isUHeap) {
        if (estate->es_epqTupleSlot == NULL) {
            estate->es_epqTupleSlot =
                (TupleTableSlot **)MemoryContextAllocZero(CurrentMemoryContext, rtsize * sizeof(TupleTableSlot *));
        }
    }

    /*
     * Initialize private state information for each SubPlan.  We must do this
     * before running ExecInitNode on the main query tree, since
     * ExecInitSubPlan expects to be able to find these entries. Some of the
     * SubPlans might not be used in the part of the plan tree we intend to
     * run, but since it's not easy to tell which, we just initialize them
     * all.
     */
    Assert(estate->es_subplanstates == NIL);
    foreach (l, parentestate->es_plannedstmt->subplans) {
        Plan *subplan = (Plan *)lfirst(l);
        PlanState *subplanstate = NULL;

        estate->es_under_subplan = true;
        subplanstate = ExecInitNode(subplan, estate, 0);
        estate->es_under_subplan = false;
        estate->es_subplanstates = lappend(estate->es_subplanstates, subplanstate);
    }

    /*
     * Initialize the private state information for all the nodes in the part
     * of the plan tree we need to run.  This opens files, allocates storage
     * and leaves us ready to start processing tuples.
     */
    epqstate->planstate = ExecInitNode(planTree, estate, 0);
    estate->es_result_relation_info = parentestate->es_result_relation_info;

    (void)MemoryContextSwitchTo(old_context);
}

/*
 * EvalPlanQualEnd -- shut down at termination of parent plan state node,
 * or if we are done with the current EPQ child.
 *
 * This is a cut-down version of ExecutorEnd(); basically we want to do most
 * of the normal cleanup, but *not* close result relations (which we are
 * just sharing from the outer query).	We do, however, have to close any
 * trigger target relations that got opened, since those are not shared.
 * (There probably shouldn't be any of the latter, but just in case...)
 */
void EvalPlanQualEnd(EPQState *epqstate)
{
    EState *estate = epqstate->estate;
    MemoryContext old_context;
    ListCell *l = NULL;

    if (estate == NULL) {
        return; /* idle, so nothing to do */
    }

    old_context = MemoryContextSwitchTo(estate->es_query_cxt);

    ExecEndNode(epqstate->planstate);

    foreach (l, estate->es_subplanstates) {
        PlanState *subplanstate = (PlanState *)lfirst(l);

        ExecEndNode(subplanstate);
    }

    /* throw away the per-estate tuple table */
    ExecResetTupleTable(estate->es_tupleTable, false);

    /* close any trigger target relations attached to this EState */
    foreach (l, estate->es_trig_target_relations) {
        ResultRelInfo *resultRelInfo = (ResultRelInfo *)lfirst(l);

        /* Close indices and then the relation itself */
        ExecCloseIndices(resultRelInfo);
        heap_close(resultRelInfo->ri_RelationDesc, NoLock);
    }

    (void)MemoryContextSwitchTo(old_context);

    FreeExecutorState(estate);

    /* Mark EPQState idle */
    epqstate->estate = NULL;
    epqstate->planstate = NULL;
    epqstate->origslot = NULL;
}

TupleTableSlot* FetchPlanSlot(PlanState* subPlanState, ProjectionInfo** projInfos, bool isinherit)
{
    int result_rel_index = subPlanState->state->result_rel_index;

    if (result_rel_index > 0 && !isinherit) {
        return ExecProject(projInfos[result_rel_index], NULL);
    } else {
        return ExecProcNode(subPlanState);
    }
}
