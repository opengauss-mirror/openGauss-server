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
 * streamMain.cpp
 *	  stream main Interface.
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/stream/streamMain.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/dfs/dfs_am.h"
#include "access/gtm.h"
#include "access/printtup.h"
#include "distributelayer/streamMain.h"
#include "distributelayer/streamProducer.h"
#include "executor/exec/execStream.h"
#include "executor/executor.h"
#include "knl/knl_variable.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"
#include "postmaster/postmaster.h"
#include "replication/dataqueue.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "threadpool/threadpool.h"
#include "utils/memtrack.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "instruments/instr_handle_mgr.h"

extern void CodeGenThreadInitialize();
extern void InitRecursiveCTEGlobalVariables(const PlannedStmt* planstmt);

static void InitStreamThread();
static void InitStreamPath();
static void InitStreamSignal();
static void InitStreamResource();
static void HandleStreamSigjmp();
static void execute_stream_plan(StreamProducer* producer);
static void execute_stream_end(StreamProducer* producer);
static void StreamQuitAndClean(int code, Datum arg);
static void ResetStreamWorkerInfo();

/* ----------------------------------------------------------------
 * StreamMain
 *	   stream thread main entrance
 * ----------------------------------------------------------------
 */
int StreamMain()
{
    sigjmp_buf local_sigjmp_buf;

    InitStreamThread();

    SetProcessingMode(NormalProcessing);

    on_proc_exit(StreamQuitAndClean, 0);

    /*
     * process any libraries that should be preloaded at backend start (this
     * likewise can't be done until GUC settings are complete)
     */
    process_local_preload_libraries();

    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        /* reset signal block flag for threadpool worker */
        ResetInterruptCxt();
        if (g_threadPoolControler) {
            g_threadPoolControler->GetSessionCtrl()->releaseLockIfNecessary();
        }

        gstrace_tryblock_exit(true, oldTryCounter);
        HandleStreamSigjmp();
        if (IS_THREAD_POOL_STREAM) {
            t_thrd.threadpool_cxt.stream->CleanUp();
        } else {
            return 0;
        }
    }

    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);
    
    MemoryContext oldMemory = MemoryContextSwitchTo(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
#ifdef ENABLE_LLVM_COMPILE
    CodeGenThreadInitialize();
#endif
    (void)MemoryContextSwitchTo(oldMemory);
    
    /* We can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;

    if (IS_THREAD_POOL_STREAM) {
        ResetStreamWorkerInfo();
    }

    while (true) {
        if (IS_THREAD_POOL_STREAM) {
            pgstat_report_activity(STATE_IDLE, NULL);
            pgstat_report_waitstatus(STATE_WAIT_COMM);
            t_thrd.threadpool_cxt.stream->WaitMission();
            Assert(CheckMyDatabaseMatch());
            pgstat_report_waitstatus(STATE_WAIT_UNDEFINED);
        }

        pgstat_report_queryid(u_sess->debug_query_id);
        pgstat_report_unique_sql_id(false);
        pgstat_report_global_session_id(u_sess->globalSessionId);
        pgstat_report_smpid(u_sess->stream_cxt.smp_id);
        timeInfoRecordStart();

        /* Wait thread ID ready */
        u_sess->stream_cxt.producer_obj->waitThreadIdReady();

        execute_stream_plan(u_sess->stream_cxt.producer_obj);
        execute_stream_end(u_sess->stream_cxt.producer_obj);
        WLMReleaseNodeFromHash();
        WLMReleaseIoInfoFromHash();
        /* Reset here so that we can get debug_query_string when Stream thread is in Sync point */
        t_thrd.postgres_cxt.debug_query_string = NULL;

        /* 
         * Note that parent thread will do commit or abort transaction.
         * Stream thread should not change clog file
         */
        ResetTransactionInfo();

        if (IS_THREAD_POOL_STREAM) {
            t_thrd.threadpool_cxt.stream->CleanUp();
        } else {
            break;
        }
    }

    CloseGTM();

    return 0;
}

static void InitStreamThread()
{
    initRandomState(0, GetCurrentTimestamp());

    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    u_sess->exec_cxt.under_stream_runtime = true;
    t_thrd.codegen_cxt.g_runningInFmgr = false;
    FrontendProtocol = PG_PROTOCOL_LATEST;
    u_sess->attr.attr_common.remoteConnType = REMOTE_CONN_DATANODE;

    InitStreamPath();

    InitStreamSignal();

    /* Early initialization */
    BaseInit();

    /* We need to allow SIGINT, etc during the initial transaction */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);

    /* Initialize the memory tracking information */
    MemoryTrackingInit();

    if (!IS_THREAD_POOL_STREAM) {
        ExtractProduerInfo();
        t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(
            u_sess->stream_cxt.producer_obj->getDbName(), InvalidOid, u_sess->stream_cxt.producer_obj->getUserName());
        repair_guc_variables();
    }
    t_thrd.proc_cxt.PostInit->InitStreamWorker();

    InitVecFuncMap();

    InitStreamResource();
}

static void InitStreamPath()
{
    /* Compute paths, if we didn't inherit them from postmaster */
    if (my_exec_path[0] == '\0') {
        if (find_my_exec("postgres", my_exec_path) < 0)
            ereport(FATAL, (errmsg("openGauss: could not locate my own executable path")));
    }

    if (t_thrd.proc_cxt.pkglib_path[0] == '\0')
        get_pkglib_path(my_exec_path, t_thrd.proc_cxt.pkglib_path);
}

static void InitStreamSignal()
{
    (void)gspqsignal(SIGINT, StatementCancelHandler);
    (void)gspqsignal(SIGTERM, die);
    (void)gspqsignal(SIGALRM, handle_sig_alarm); /* timeout conditions */
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gs_signal_unblock_sigusr2();

    if (IsUnderPostmaster) {
        /* We allow SIGQUIT (quickdie) at all times */
        (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);
    }
}

static void InitStreamResource()
{
    /*
     * Create the memory context we will use in the main loop.
     *
     * t_thrd.mem_cxt.msg_mem_cxt is reset once per iteration of the main loop, ie, upon
     * completion of processing of each command message from the client.
     */
    t_thrd.mem_cxt.msg_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "MessageContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    t_thrd.mem_cxt.mask_password_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "MaskPasswordCtx",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    t_thrd.utils_cxt.TopResourceOwner = ResourceOwnerCreate(NULL, "stream thread",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
    t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.TopResourceOwner;

    if (t_thrd.mem_cxt.postmaster_mem_cxt) {
        MemoryContextDelete(t_thrd.mem_cxt.postmaster_mem_cxt);
        t_thrd.mem_cxt.postmaster_mem_cxt = NULL;
    }
}

void ExtractProduerInfo()
{
    if (u_sess->stream_cxt.producer_obj == NULL) {
        return;
    }
    u_sess->wlm_cxt->wlm_params = u_sess->stream_cxt.producer_obj->getWlmParams();
    u_sess->instr_cxt.gs_query_id->procId = u_sess->stream_cxt.producer_obj->getExplainThreadid();
    u_sess->exec_cxt.need_track_resource = u_sess->stream_cxt.producer_obj->getExplainTrack();
    u_sess->stream_cxt.producer_obj->getUniqueSQLKey(&u_sess->unique_sql_cxt.unique_sql_id,
        &u_sess->unique_sql_cxt.unique_sql_user_id, &u_sess->unique_sql_cxt.unique_sql_cn_id);
    u_sess->stream_cxt.producer_obj->getGlobalSessionId(&u_sess->globalSessionId);

    WLMGeneralParam *g_wlm_params =  &u_sess->wlm_cxt->wlm_params;
    errno_t ret = sprintf_s(u_sess->wlm_cxt->control_group, 
                            sizeof(u_sess->wlm_cxt->control_group), "%s", g_wlm_params->cgroup); 
    securec_check_ss(ret, "\0", "\0");

    /* Get the node group information */
    t_thrd.wlm_cxt.thread_node_group = WLMGetNodeGroupFromHTAB(g_wlm_params->ngroup);
    t_thrd.wlm_cxt.thread_climgr = &t_thrd.wlm_cxt.thread_node_group->climgr;
    t_thrd.wlm_cxt.thread_srvmgr = &t_thrd.wlm_cxt.thread_node_group->srvmgr;

    /* Set the right pgxcnodeid */
    u_sess->pgxc_cxt.PGXCNodeId = u_sess->stream_cxt.producer_obj->getPgxcNodeId();
    u_sess->instr_cxt.global_instr = u_sess->stream_cxt.producer_obj->getStreamInstrumentation();
    u_sess->proc_cxt.MyProcPort->database_name = u_sess->stream_cxt.producer_obj->getDbName();
    u_sess->proc_cxt.MyProcPort->user_name = u_sess->stream_cxt.producer_obj->getUserName();

    /* runtimethreadinstr */
    if (u_sess->instr_cxt.global_instr) {
        Assert(u_sess->instr_cxt.thread_instr == NULL);
        int segmentId = u_sess->stream_cxt.producer_obj->getPlan()->planTree->plan_node_id;
        u_sess->instr_cxt.thread_instr = u_sess->instr_cxt.global_instr->allocThreadInstrumentation(segmentId);
    }

    u_sess->stream_cxt.producer_obj->netInit();
    u_sess->stream_cxt.producer_obj->setThreadInit(true);
#ifdef ENABLE_MULTIPLE_NODES
    u_sess->stream_cxt.producer_obj->initSkewState();
#endif

    if (u_sess->stream_cxt.producer_obj->isDummy()) {
        u_sess->exec_cxt.executorStopFlag = true;
        u_sess->stream_cxt.dummy_thread = true;
    } else {
        u_sess->exec_cxt.executorStopFlag = false;
        u_sess->stream_cxt.dummy_thread = false;
    }

    /* stream workers share the same session memory entry as their parents */
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry = u_sess->stream_cxt.producer_obj->getSessionMemory();
    if (t_thrd.proc != NULL) {
        t_thrd.proc->sessMemorySessionid = u_sess->stream_cxt.producer_obj->getParentSessionid();
        Assert(t_thrd.proc->sessMemorySessionid == t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->sessionid);
    }

    /* Initialize the global variables for recursive */
    InitRecursiveCTEGlobalVariables(u_sess->stream_cxt.producer_obj->getPlan());

    STREAM_LOG(DEBUG2, "enter StreamMain, StreamKey(%lu, %u, %u)",
               u_sess->stream_cxt.producer_obj->getKey().queryId,
               u_sess->stream_cxt.producer_obj->getKey().planNodeId,
               u_sess->stream_cxt.producer_obj->getKey().smpIdentifier);
}

static void HandleStreamSigjmp()
{
    pgstat_report_waitstatus(STATE_WAIT_UNDEFINED);
    t_thrd.pgxc_cxt.GlobalNetInstr = NULL;

    /* output the memory tracking information when error happened */
    MemoryTrackingOutputFile();

    /* Since not using PG_TRY, must reset error stack by hand */
    t_thrd.log_cxt.error_context_stack = NULL;

    t_thrd.log_cxt.call_stack = NULL;
    
    /* reset buffer strategy flag */
    t_thrd.storage_cxt.is_btree_split = false;
    
    /* Prevent interrupts while cleaning up */
    HOLD_INTERRUPTS();

    /*
     * Turn off these interrupts too.  This is only needed here and not in
     * other exception-catching places since these interrupts are only
     * enabled while we wait for client input.
     */
    t_thrd.postgres_cxt.DoingCommandRead = false;

    /*
     * Abort the current transaction in order to recover.
     */
    ereport(DEBUG1,
        (errmsg("stream thread %lu end transaction " XID_FMT " abnormally",
            t_thrd.proc_cxt.MyProcPid,
            GetCurrentTransactionIdIfAny())));

    /*
     * when clearing the BCM encounter ERROR, we should ResetBCMArray, or it
     * will enter ClearBCMArray infinite loop, then coredump.
     */
    ResetBCMArray();

    /* release operator-level hash table in memory */
    releaseExplainTable();

    /* Mark recursive vfd is invalid before aborting transaction. */
    StreamNodeGroup::MarkRecursiveVfdInvalid();

    AbortCurrentTransaction();

    /* release resource held by lsc */
    AtEOXact_SysDBCache(false);

    LWLockReleaseAll();

    if (u_sess->stream_cxt.producer_obj != NULL) {
        u_sess->stream_cxt.producer_obj->reportError();
    }

    MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
    FlushErrorState();

    if (ENABLE_WORKLOAD_CONTROL)
        WLMResetStatInfo4Exception();

    RESUME_INTERRUPTS();

    timeInfoRecordEnd();
    StreamNodeGroup::syncQuit(STREAM_ERROR);
}

static void execute_stream_plan(StreamProducer* producer)
{
    /*
     * Start up a transaction command.	All queries generated by the
     * query_string will be in this same command block, *unless* we find a
     * BEGIN/COMMIT/ABORT statement; we have to force a new xact command after
     * one of those, else bad things will happen in xact.c. (Note that this
     * will normally change current memory context.)
     */
    start_xact_command();

    producer->setUpStreamTxnEnvironment();

    PlannedStmt* planstmt = producer->getPlan();
    CommandDest dest = producer->getDest();
    bool save_log_statement_stats = u_sess->attr.attr_common.log_statement_stats;
    bool isTopLevel = false;
    const char* commandTag = NULL;
    char completionTag[COMPLETION_TAG_BUFSIZE];
    Portal portal = NULL;
    DestReceiver* receiver = NULL;
    int16 format;
    char msec_str[PRINTF_DST_MAX];

    t_thrd.postgres_cxt.debug_query_string = planstmt->query_string;
    pgstat_report_activity(STATE_RUNNING, t_thrd.postgres_cxt.debug_query_string);
    /* Use planNodeId as thread_level, same as the key which SCTP use for send/receive */
    pgstat_report_parent_sessionid(producer->getParentSessionid(), producer->getKey().planNodeId);

    if (u_sess->instr_cxt.global_instr &&
        u_sess->instr_cxt.perf_monitor_enable)  // Don't use perf util you set has_use_perf = true
        CPUMon::Initialize(CMON_GENERAL);

    /*
     * We use save_log_statement_stats so ShowUsage doesn't report incorrect
     * results because ResetUsage wasn't called.
     */
    if (save_log_statement_stats)
        ResetUsage();

    isTopLevel = true;

    // For now plan shipping is used only for SELECTs, in future
    // we should remove this hard coding and get the tag automatically
    commandTag = "SELECT";

    set_ps_display(commandTag, false);

    BeginCommand(commandTag, dest);

    /*
     * If we are in an aborted transaction, reject all commands except
     * COMMIT/ABORT.  It is important that this test occur before we try
     * to do parse analysis, rewrite, or planning, since all those phases
     * try to do database accesses, which may fail in abort state. (It
     * might be safe to allow some additional utility commands in this
     * state, but not many...)
     */
    if (IsAbortedTransactionBlockState())  // &&
        ereport(ERROR,
            (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                errmsg("current transaction is aborted, "
                    "commands ignored until end of transaction block, firstChar[%c]",
                    u_sess->proc_cxt.firstChar),
                errdetail_abort()));

    /* Make sure we are in a transaction command */
    start_xact_command();

    /* If we got a cancel signal in parsing or prior command, quit */
    CHECK_FOR_INTERRUPTS();

    /*
     * Create unnamed portal to run the query or queries in. If there
     * already is one, silently drop it.
     */
    portal = CreatePortal("", true, true);
    /* Don't display the portal in pg_cursors */
    portal->visible = false;

    u_sess->instr_cxt.global_instr = producer->getStreamInstrumentation();
    u_sess->instr_cxt.obs_instr = producer->getOBSInstrumentation();
    if (u_sess->instr_cxt.obs_instr)
        u_sess->instr_cxt.p_OBS_instr_valid = u_sess->instr_cxt.obs_instr->m_p_globalOBSInstrument_valid;

    PortalDefineQuery(portal, NULL, "DUMMY", commandTag, lappend(NULL, planstmt), NULL);

    /*
     * Start the portal.  No parameters here.
     */
    PortalStart(portal, producer->getParams(), 0, producer->getSnapShot());
    format = 0;
    PortalSetResultFormat(portal, 1, &format);

    receiver = CreateDestReceiver(dest);
    if (dest >= DestTupleBroadCast)
        SetStreamReceiverParams(receiver, producer, portal);

    /*
     * Run the portal to completion, and then drop it (and the receiver).
     */
    (void)PortalRun(portal, FETCH_ALL, isTopLevel, receiver, receiver, completionTag);

    (*receiver->rDestroy)(receiver);

    PortalDrop(portal, false);

    finish_xact_command();

    /*
     * Emit duration logging if appropriate.
     */
    switch (check_log_duration(msec_str, false)) {
        case 1:
            Assert(false);
            break;
        case 2:
            ereport(LOG,
                (errmsg("duration: %s ms  statement: %s", msec_str, "TODO: deparse plan"),  // vam query_string),
                    errhidestmt(true)
                    // vam errdetail_execute(parsetree_list)
                    ));
            break;
        default:
            break;
    }

    if (save_log_statement_stats)
        ShowUsage("QUERY STATISTICS");
}

static void execute_stream_end(StreamProducer* producer)
{
    int consumer_number;
    int i, res;

    consumer_number = producer->getConnNum();
    StreamTransport** transport = producer->getTransport();

    // prepare an end message to all the consumer backend thread.
    //
    // if is dummy, do not bother send
    if (producer->isDummy() == false) {
        for (i = 0; i < consumer_number; i++) {
            if (producer->netSwitchDest(i)) {
                if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3) {
                    StringInfoData buf;

                    pq_beginmessage(&buf, 'Z');
                    pq_sendbyte(&buf, TransactionBlockStatusCode());
                    pq_endmessage(&buf);
                } else if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 2)
                    pq_putemptymessage('Z');
                /* Flush output at end of cycle in any case. */
                res = pq_flush();
                if (res == EOF) {
                    transport[i]->release();
                }
                producer->netStatusSave(i);
            }
        }
    }
    producer->finalizeLocalStream();
    timeInfoRecordEnd();
    StreamNodeGroup::syncQuit(STREAM_COMPLETE);
    ForgetRegisterStreamSnapshots();
}

/*
 * Called when the Stream thread is ending.
 */
static void StreamQuitAndClean(int code, Datum arg)
{
    /* Close connection with GTM, if active */
    CloseGTM();

    /* Free remote xact state */
    free_RemoteXactState();
}

// reset some flag related to stream
void ResetStreamEnv()
{
    t_thrd.subrole = NO_SUBROLE;
    u_sess->stream_cxt.dummy_thread = false;
    u_sess->exec_cxt.executorStopFlag = false;
    u_sess->stream_cxt.global_obj = NULL;
    u_sess->stream_cxt.producer_obj = NULL;
    u_sess->instr_cxt.global_instr = NULL;
    u_sess->instr_cxt.thread_instr = NULL;
    u_sess->exec_cxt.under_stream_runtime = false;
    u_sess->stream_cxt.in_waiting_quit = false;
    u_sess->stream_cxt.enter_sync_point = false;
    t_thrd.pgxc_cxt.GlobalNetInstr = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    u_sess->opt_cxt.query_dop = u_sess->attr.attr_sql.query_dop_tmp;
#endif

    /*
     * When gaussdb backend running in Query or Operator level, we are going to use global
     * variable notplanshipping to mark current query is not plan shipping, so we do string
     * initialization here
     */
    u_sess->opt_cxt.not_shipping_info->need_log = true;
    errno_t errorno = memset_s(
        u_sess->opt_cxt.not_shipping_info->not_shipping_reason, NOTPLANSHIPPING_LENGTH, '\0', NOTPLANSHIPPING_LENGTH);
    securec_check_c(errorno, "\0", "\0");

    t_thrd.postgres_cxt.table_created_in_CTAS = false;

    if (IS_PGXC_COORDINATOR) {
        u_sess->exec_cxt.need_track_resource = false;
    }

    u_sess->instr_cxt.gs_query_id->queryId = 0;

    u_sess->wlm_cxt->local_foreign_respool = NULL;
    t_thrd.postmaster_cxt.forceNoSeparate = false;

    u_sess->pcache_cxt.gpc_remote_msg = false;

    t_thrd.postgres_cxt.gpc_fisrt_send_clean = true;

    if (IS_PGXC_COORDINATOR) {
        WLMStatusTag status = t_thrd.wlm_cxt.collect_info->status;
        if (!(status == WLM_STATUS_FINISHED || status == WLM_STATUS_ABORT))
            return;
    }

    if (IS_PGXC_DATANODE) {
        WLMStatusTag status = t_thrd.wlm_cxt.dn_cpu_detail->status;
        if (!(status == WLM_STATUS_FINISHED || status == WLM_STATUS_ABORT))
            return;
    }

    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->initMemInChunks = t_thrd.utils_cxt.trackedMemChunks;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->queryMemInChunks = t_thrd.utils_cxt.trackedMemChunks;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->peakChunksQuery = t_thrd.utils_cxt.trackedMemChunks;
    t_thrd.utils_cxt.peakedBytesInQueryLifeCycle = 0;
    t_thrd.utils_cxt.basedBytesInQueryLifeCycle = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->spillCount = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->spillSize = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->broadcastSize = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->estimate_time = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->estimate_memory = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->warning = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->iscomplex = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->dnStartTime = GetCurrentTimestamp();
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->dnEndTime = 0;

    pfree_ext(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan);
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->plan_size = 0;
    pfree_ext(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan_issue);

    t_thrd.wlm_cxt.collect_info->status = WLM_STATUS_RESERVE;
    t_thrd.wlm_cxt.dn_cpu_detail->status = WLM_STATUS_RESERVE;

    t_thrd.sig_cxt.gs_sigale_check_type = SIGNAL_CHECK_NONE;
    t_thrd.sig_cxt.session_id = 0;
}

void SetStreamWorkerInfo(StreamProducer* proObj)
{
    if (proObj == NULL) {
        return;
    }

    u_sess->stream_cxt.producer_obj = proObj;
    u_sess->stream_cxt.smp_id = proObj->getKey().smpIdentifier;
    u_sess->stream_cxt.producer_dop = proObj->getParallelDesc().producerDop;
    u_sess->debug_query_id = proObj->getKey().queryId;
    u_sess->utils_cxt.sync_guc_variables = u_sess->stream_cxt.producer_obj->get_sync_guc_variables();
    // set the stopFlag;
    u_sess->stream_cxt.global_obj = u_sess->stream_cxt.producer_obj->getNodeGroup();
    u_sess->stream_cxt.global_obj->setStopFlagPoint(
        u_sess->stream_cxt.producer_obj->getNodeGroupIdx(), &u_sess->exec_cxt.executorStopFlag);
}

static void ResetStreamWorkerInfo()
{
    u_sess->stream_cxt.producer_obj = NULL;
    u_sess->stream_cxt.global_obj = NULL;
}

static void StoreStreamSyncParam(StreamSyncParam *syncParam)
{
    syncParam->TempNamespace = u_sess->catalog_cxt.myTempNamespace;
    syncParam->TempToastNamespace = u_sess->catalog_cxt.myTempToastNamespace;
    syncParam->IsBinaryUpgrade = u_sess->proc_cxt.IsBinaryUpgrade;
    if (module_logging_is_on(MOD_COMM_IPC) && (t_thrd.proc && t_thrd.proc->workingVersionNum >= 92060)) {
        syncParam->CommIpcLog = true;
    } else {
        syncParam->CommIpcLog = false;
    }
}

void RestoreStreamSyncParam(StreamSyncParam *syncParam)
{
    u_sess->catalog_cxt.myTempNamespace = syncParam->TempNamespace;
    u_sess->catalog_cxt.myTempToastNamespace = syncParam->TempToastNamespace;
    u_sess->proc_cxt.IsBinaryUpgrade = syncParam->IsBinaryUpgrade;
    if (syncParam->CommIpcLog == true) {
        module_logging_enable_comm(MOD_COMM_IPC);
    }
}

ThreadId ApplyStreamThread(StreamProducer *producer)
{
    ThreadId tid = InvalidTid;

    producer->setParentSessionid(t_thrd.proc->sessMemorySessionid);
    StoreStreamSyncParam(&producer->m_syncParam);

    if (t_thrd.threadpool_cxt.group != NULL) {
        tid = t_thrd.threadpool_cxt.group->GetStreamFromPool(producer);
        STREAM_LOG(DEBUG2, "[StreamPool] Apply thread %lu query_id %lu, tlevel %u, smpid %u",
                   tid,
                   producer->getKey().queryId,
                   producer->getKey().planNodeId,
                   producer->getKey().smpIdentifier);
    } else {
        producer->setChildSlot(AssignPostmasterChildSlot());
        if (producer->getChildSlot() == -1) {
            return InvalidTid;
        }
        tid = initialize_util_thread(STREAM_WORKER, producer);
    }

    return tid;
}

void RestoreStream()
{
    /*
     * We should restoreStreamEnter after release the memory context.
     * Make sure top consumer thread exit after stream thread.
     */
    if (StreamThreadAmI() && u_sess->stream_cxt.global_obj) {
        /*
         * Set CurrentResourceOwner to NULL or will core dumped in ResourceOwnerEnlargePthreadMutex
         * case t_thrd.top_mem_cxt has been set NULL.
         */
        t_thrd.utils_cxt.CurrentResourceOwner = NULL;
        u_sess->stream_cxt.global_obj->restoreStreamEnter();
        u_sess->stream_cxt.global_obj = NULL;
    }
}

void StreamExit()
{
    if (u_sess == t_thrd.fake_session) {
        return;
    }

    /* Reset to Local vfd if we have attach it to global vfdcache */
    ResetToLocalVfdCache();

    CleanupDfsHandlers(true);

    closeAllVfds();

    AtProcExit_Buffers(0, 0);
    ShutdownPostgres(0, 0);
    if(!EnableLocalSysCache()) {
        AtProcExit_Files(0, 0);
    }
    StreamQuitAndClean(0, 0);

    RestoreStream();

    if (!EnableLocalSysCache()) {
        /* release memory context and reset flags. */
        MemoryContextReset(u_sess->syscache_cxt.SysCacheMemCxt);
        errno_t rc = EOK;
        rc = memset_s(u_sess->syscache_cxt.SysCache, sizeof(CatCache*) * SysCacheSize,
                    0, sizeof(CatCache*) * SysCacheSize);
        securec_check(rc, "\0", "\0");
        rc = memset_s(u_sess->syscache_cxt.SysCacheRelationOid, sizeof(Oid) * SysCacheSize,
                    0, sizeof(Oid) * SysCacheSize);
        securec_check(rc, "\0", "\0");
    }

    /* release statement_cxt */
    if (t_thrd.proc_cxt.MyBackendId != InvalidBackendId) {
        release_statement_context(t_thrd.shemem_ptr_cxt.MyBEEntry, __FUNCTION__, __LINE__);
    }

    free_session_context(u_sess);
}
