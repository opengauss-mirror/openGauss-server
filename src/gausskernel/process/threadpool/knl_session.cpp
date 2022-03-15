/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * knl_session.cpp
 *    Initial functions for session level global variables.
 *
 * IDENTIFICATION
 *    src/gausskernel/process/threadpool/knl_session.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <c.h>
#include <locale.h>

#include "access/reloptions.h"
#include "access/xlogdefs.h"
#include "access/ustore/knl_uundovec.h"
#include "commands/tablespace.h"
#include "executor/instrument.h"
#include "gssignal/gs_signal.h"
#include "mb/pg_wchar.h"
#include "knl/knl_session.h"
#include "optimizer/cost.h"
#include "optimizer/dynsmp.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/streamplan.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgFdwRemote.h"
#include "pgxc/poolmgr.h"
#include "regex/regex.h"
#include "storage/procarray.h"
#include "storage/sinval.h"
#include "utils/anls_opt.h"
#include "utils/elog.h"
#include "utils/formatting.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/pg_locale.h"
#include "utils/pg_lzcompress.h"
#include "utils/plog.h"
#include "utils/portal.h"
#include "utils/relmapper.h"
#include "access/heapam.h"
#include "workload/workload.h"
#include "parser/scanner.h"
#include "pgstat.h"

THR_LOCAL knl_session_context* u_sess;

extern pg_enc2name pg_enc2name_tbl[];
extern int SysCacheSize;

#define RAND48_SEED_0 0x330e
#define RAND48_SEED_1 0xabcd
#define RAND48_SEED_2 0x1234

#define DEFAULT_DBE_BUFFER_LIMIT 20000

static void
KnlUUstoreInit(knl_u_ustore_context *ustoreCxt)
{
    ustoreCxt->urecvec = New(CurrentMemoryContext) URecVector();
    ustoreCxt->urecvec->Initialize(MAX_UNDORECORDS_PER_OPERATION, true);

    for (int i = 0; i < MAX_UNDORECORDS_PER_OPERATION; i++) {
        ustoreCxt->undo_records[i] = New(CurrentMemoryContext)UndoRecord();
        ustoreCxt->urecvec->PushBack(ustoreCxt->undo_records[i]);
    }

    ustoreCxt->undo_buffer_idx = 0;
    ustoreCxt->undo_buffers = (UndoBuffer*) palloc0(MAX_UNDO_BUFFERS * sizeof(UndoBuffer));

    ustoreCxt->tdSlotWaitFinishTime = 0;
    ustoreCxt->tdSlotWaitActive = false;
}

static void KnlURepOriginInit(knl_u_rep_origin_context* repOriginCxt)
{
    repOriginCxt->curRepState = NULL;
    repOriginCxt->originId = InvalidRepOriginId;
    repOriginCxt->originLsn = InvalidXLogRecPtr;
    repOriginCxt->originTs = 0;
    repOriginCxt->repStatesShm = NULL;
    repOriginCxt->registeredCleanup = false;
}

static void knl_u_analyze_init(knl_u_analyze_context* anl_cxt)
{
    anl_cxt->is_under_analyze = false;
    anl_cxt->need_autoanalyze = false;
    anl_cxt->analyze_context = NULL;
    anl_cxt->autoanalyze_process = NULL;
    anl_cxt->autoanalyze_timeinfo = NULL;
    anl_cxt->vac_strategy = (BufferAccessStrategyData*)palloc0(sizeof(BufferAccessStrategyData));
}

static void knl_u_attr_init(knl_session_attr* attr)
{
    const int sessionDefaultTimeout = 600; /* 10 * 60 = 10 min */
    attr->attr_common.backtrace_min_messages = PANIC;
    attr->attr_common.log_min_messages = WARNING;
    attr->attr_common.client_min_messages = NOTICE;
    attr->attr_common.SessionTimeout = sessionDefaultTimeout;
    attr->attr_common.SessionTimeoutCount = 0;
    attr->attr_common.enable_full_encryption = false;
    attr->attr_storage.sync_method = DEFAULT_SYNC_METHOD;
    attr->attr_sql.under_explain = false;
    attr->attr_resource.enable_auto_explain = false;
    attr->attr_sql.enable_upsert_to_merge = false;
    attr->attr_common.extension_session_vars_array_size = 0;
    attr->attr_common.extension_session_vars_array = NULL;
}

void knl_u_executor_init(knl_u_executor_context* exec_cxt)
{
    exec_cxt->remotequery_list = NIL;
    exec_cxt->exec_result_checkqual_fail = false;
    exec_cxt->under_stream_runtime = false;
    exec_cxt->under_auto_explain =  false;
    exec_cxt->extension_is_valid = false;
    exec_cxt->global_bucket_map = NULL;
    exec_cxt->global_bucket_cnt = 0;
    exec_cxt->vec_func_hash = NULL;
    exec_cxt->route = (PartitionIdentifier*)palloc0(sizeof(PartitionIdentifier));
    exec_cxt->cur_tuple_hash_table = NULL;
    exec_cxt->cur_light_proxy_obj = NULL;

    exec_cxt->ActivePortal = NULL;
    exec_cxt->PortalHashTable = NULL;
    exec_cxt->unnamed_portal_count = 0;
    exec_cxt->need_track_resource = false;
    exec_cxt->RetryController = New(CurrentMemoryContext) StatementRetryController(CurrentMemoryContext);
    exec_cxt->hasTempObject = false;
    exec_cxt->DfsDDLIsTopLevelXact = false;
    exec_cxt->could_cancel_redistribution = false;

    exec_cxt->g_pgaudit_agent_attached = false;
    exec_cxt->pgaudit_track_sqlddl = true;

    exec_cxt->HashScans = NULL;
    exec_cxt->executorStopFlag = false;

    exec_cxt->CurrentOpFusionObj = NULL;
    exec_cxt->nesting_level = 0;

    exec_cxt->is_exec_trigger_func = false;
    exec_cxt->single_shard_stmt = false;

    exec_cxt->cast_owner = InvalidOid;

    exec_cxt->CurrentRouter = NULL;
    exec_cxt->is_dn_enable_router = false;

    exec_cxt->isExecTrunc = false;

    exec_cxt->isLockRows = false;
}

static void knl_u_index_init(knl_u_index_context* index_cxt)
{
    index_cxt->counter = 1;
}

static void knl_u_instrument_init(knl_u_instrument_context* instr_cxt)
{
    instr_cxt->perf_monitor_enable = false;
    instr_cxt->can_record_to_table = false;
    instr_cxt->operator_plan_number = 0;
    instr_cxt->OBS_instr_valid = false;
    instr_cxt->p_OBS_instr_valid = NULL;
    instr_cxt->global_instr = NULL;
    instr_cxt->thread_instr = NULL;
    instr_cxt->obs_instr = NULL;
    instr_cxt->gs_query_id = (Qpid*)palloc0(sizeof(Qpid));
    instr_cxt->pg_buffer_usage = (BufferUsage*)palloc0(sizeof(BufferUsage));
    instr_cxt->plan_size = 0;
}

static void knl_u_locale_init(knl_u_locale_context* lc_cxt)
{
    lc_cxt->cur_lc_conv_valid = false;
    lc_cxt->cur_lc_time_valid = false;
    lc_cxt->cur_lc_conv = (struct lconv*)palloc0(sizeof(struct lconv));
    lc_cxt->lc_ctype_result = -1;
    lc_cxt->lc_collate_result = -1;
    lc_cxt->collation_cache = NULL;
}

static void knl_u_log_init(knl_u_log_context* log_cxt)
{
    log_cxt->syslog_ident = NULL;
    log_cxt->module_logging_configure = (char*)palloc0(sizeof(char) * BEMD_BITMAP_SIZE);
    log_cxt->msgbuf = (StringInfoData*)palloc0(sizeof(StringInfoData));
}

static void knl_u_optimizer_init(knl_u_optimizer_context* opt_cxt)
{
    errno_t rc;

    opt_cxt->disable_dop_change = false;
    opt_cxt->enable_nodegroup_explain = false;
    opt_cxt->has_obsrel = false;
    opt_cxt->is_stream = true;
    opt_cxt->is_stream_support = true;
    opt_cxt->is_multiple_nodegroup_scenario = false;
    /* Installation nodegroup. */
    opt_cxt->different_nodegroup_count = 1;
    opt_cxt->is_randomfunc_shippable = true;
    opt_cxt->is_dngather_support = true;

    opt_cxt->srvtype = 0;
    opt_cxt->qrw_inlist2join_optmode = QRW_INLIST2JOIN_CBO;
    opt_cxt->plan_current_seed = 0;
    opt_cxt->plan_prev_seed = 0;
    rc = memset_s(opt_cxt->path_current_seed_factor, PATH_SEED_FACTOR_LEN, 0, PATH_SEED_FACTOR_LEN);
    securec_check(rc, "\0", "\0");

    opt_cxt->in_redistribution_group_distribution = NULL;
    opt_cxt->compute_permission_group_distribution = NULL;
    opt_cxt->query_union_set_group_distribution = NULL;
    opt_cxt->single_node_distribution = NULL;
    opt_cxt->opr_proof_cache_hash = NULL;

    opt_cxt->query_dop_store = 1;
    opt_cxt->query_dop = 1;
    opt_cxt->max_query_dop = -1;
    opt_cxt->parallel_debug_mode = 0;

    opt_cxt->skew_strategy_opt = SKEW_OPT_OFF;
    opt_cxt->op_work_mem = 1024;
    opt_cxt->ft_context = NULL;
    opt_cxt->is_under_append_plan = false;

    /* Palloc memory. */
    opt_cxt->dynamic_smp_info = (DynamicSmpInfo*)palloc0(sizeof(DynamicSmpInfo));
    opt_cxt->not_shipping_info = (ShippingInfo*)palloc0(sizeof(ShippingInfo));
    opt_cxt->bottom_seq = (PartitionIdentifier*)palloc0(sizeof(PartitionIdentifier));
    opt_cxt->top_seq = (PartitionIdentifier*)palloc0(sizeof(PartitionIdentifier));

    /* Init dynamic smp info. */
    opt_cxt->dynamic_smp_info->num_of_dn_in_one_machine = UNKNOWN_NUM_OF_DN_IN_ONE_MACHINE;
    opt_cxt->dynamic_smp_info->num_of_cpu_in_one_machine = UNKNOWN_NUM_OF_CPU_IN_ONE_MACHINE;
    opt_cxt->dynamic_smp_info->num_of_cpu_for_one_dn = UNKNOWN_NUM_OF_CPU_FOR_ONE_DN;
    opt_cxt->dynamic_smp_info->active_statement = UNKNOWN_ACTIVE_STATEMENT;
    opt_cxt->dynamic_smp_info->cpu_util = UNKNOWN_CPU_UTIL;
    opt_cxt->dynamic_smp_info->free_mem = UNKNOWN_FREE_MEM;
    opt_cxt->dynamic_smp_info->num_of_machine = UNKNOWN_NUM_OF_MACHINE;
}

static void knl_u_parser_init(knl_u_parser_context* parser_cxt)
{
    parser_cxt->eaten_begin = false;
    parser_cxt->eaten_declare = false;
    parser_cxt->has_dollar = false;
    parser_cxt->has_placeholder = false;
    parser_cxt->stmt_contains_operator_plus = false;
    parser_cxt->is_load_copy = false;
    parser_cxt->copy_fieldname = NULL;
    parser_cxt->col_list = NIL;
    parser_cxt->hint_list = NIL;
    parser_cxt->hint_warning = NIL;
    parser_cxt->opr_cache_hash = NULL;
    parser_cxt->param_info = NULL;
    parser_cxt->param_message = NULL;
    parser_cxt->ddl_pbe_context = NULL;
    parser_cxt->in_package_function_compile = false;
    parser_cxt->isCreateFuncOrProc = false;
    parser_cxt->isTimeCapsule = false;
}

static void knl_u_advisor_init(knl_u_advisor_context* adv_cxt) 
{
    adv_cxt->adviseMode = AM_NONE;
    adv_cxt->adviseType = AT_NONE;
    adv_cxt->adviseCostMode = ACM_NONE;
    adv_cxt->adviseCompressLevel = ACL_NONE;
    adv_cxt->isOnline = false;
    adv_cxt->isConstraintPrimaryKey = true;
    adv_cxt->isJumpRecompute = false;
    adv_cxt->isCostModeRunning = false;
    adv_cxt->currGroupIndex = 0;
    adv_cxt->joinWeight = 0;
    adv_cxt->groupbyWeight = 0;
    adv_cxt->qualWeight = 0;
    adv_cxt->maxTime = 0;
    adv_cxt->startTime = 0;
    adv_cxt->endTime = 0;
    adv_cxt->maxMemory = 0;
    adv_cxt->maxsqlCount = 0;

    adv_cxt->candicateTables = NULL;
    adv_cxt->candicateQueries = NIL;
    adv_cxt->candicateAdviseGroups = NIL;
    adv_cxt->result = NIL;

    adv_cxt->SQLAdvisorContext = NULL;

    adv_cxt->getPlanInfoFunc = NULL;
}

static void knl_u_stream_init(knl_u_stream_context* stream_cxt)
{
    stream_cxt->global_obj = NULL;
    stream_cxt->producer_obj = NULL;
    stream_cxt->producer_dop = 1;
    stream_cxt->smp_id = 0;
    stream_cxt->in_waiting_quit = false;
    stream_cxt->dummy_thread = false;
    stream_cxt->enter_sync_point = false;
    stream_cxt->stop_mythread = false;
    stream_cxt->stop_pid = (ThreadId)-1;
    stream_cxt->stop_query_id = 0;
    stream_cxt->stream_runtime_mem_cxt = NULL;
    stream_cxt->data_exchange_mem_cxt = NULL;
}

static void knl_u_sig_init(knl_u_sig_context* sig_cxt)
{
    sig_cxt->got_SIGHUP = 0;
    sig_cxt->got_pool_reload = 0;
    sig_cxt->cp_PoolReload = 0;
}

static void knl_u_SPI_init(knl_u_SPI_context* spi)
{
    spi->lastoid = InvalidOid;
    spi->_stack_depth = 0;
    spi->_connected = -1;
    spi->_curid = -1;
    spi->_stack = NULL;
    spi->_current = NULL;
    spi->is_allow_commit_rollback = false;
    spi->is_stp = true;
    spi->is_proconfig_set = false;
    spi->portal_stp_exception_counter = 0;
    spi->current_stp_with_exception = false;
    spi->autonomous_session = NULL;
    spi->spi_exec_cplan_stack = NULL;
    spi->cur_tableof_index = NULL;
    spi->has_stream_in_cursor_or_forloop_sql = false;
}

static void knl_u_trigger_init(knl_u_trigger_context* tri_cxt)
{
    tri_cxt->ri_compare_cache = NULL;
    tri_cxt->ri_query_cache = NULL;

    tri_cxt->exec_row_trigger_on_datanode = false;

    tri_cxt->MyTriggerDepth = 0;
    tri_cxt->info_list = NIL;
}

static void knl_u_wlm_init(knl_u_wlm_context* wlmctx)
{
    wlmctx->cgroup_state = CG_ORIGINAL;
    wlmctx->cgroup_stmt = GSCGROUP_NONE_STMT;
    wlmctx->cgroup_last_stmt = GSCGROUP_NONE_STMT;
    wlmctx->session_respool_switch = false;
    wlmctx->session_respool_initialize = false;
    wlmctx->respool_create_oid = InvalidOid;
    wlmctx->respool_alter_oid = InvalidOid;
    wlmctx->respool_old_oid = InvalidOid;
    wlmctx->respool_delete_list = NULL;
    wlmctx->wlmcatalog_update_user = false;
    wlmctx->respool_io_limit_update = -1;
    wlmctx->respool_is_foreign = false;
    wlmctx->respool_node_group = NULL;
    wlmctx->local_foreign_respool = NULL;
    wlmctx->parctl_state_control = 0;
    wlmctx->parctl_state_exit = 0;
    wlmctx->TmptableCacheHash = NULL;
    wlmctx->spill_limit_error = false;
}

static void knl_u_utils_init(knl_u_utils_context* utils_cxt)
{
    utils_cxt->suffix_char = 0;
    utils_cxt->suffix_collation = 0;
    utils_cxt->test_err_type = 0;
    utils_cxt->cur_last_tid = (ItemPointerData*)palloc0(sizeof(ItemPointerData));
    utils_cxt->distribute_test_param = NULL;
    utils_cxt->segment_test_param = NULL;
    utils_cxt->hist_start = (PGLZ_HistEntry**)palloc0(sizeof(PGLZ_HistEntry*) * PGLZ_HISTORY_LISTS);
    utils_cxt->hist_entries = (PGLZ_HistEntry*)palloc0(sizeof(PGLZ_HistEntry) * PGLZ_HISTORY_SIZE);
    utils_cxt->analysis_options_configure = (char*)palloc0(sizeof(char) * ANLS_BEMD_BITMAP_SIZE);
    utils_cxt->guc_new_value = NULL;
    utils_cxt->lastFailedLoginTime = 0;
    utils_cxt->input_set_message = (StringInfoData*)palloc0(sizeof(StringInfoData));
    utils_cxt->GUC_check_errmsg_string = NULL;
    utils_cxt->GUC_check_errdetail_string = NULL;
    utils_cxt->GUC_check_errhint_string = NULL;
    utils_cxt->set_params_htab = NULL;
    utils_cxt->sync_guc_variables = NULL;
    for (int strategy = 0; strategy < MAX_GUC_ATTR; strategy++) {
        utils_cxt->ConfigureNamesBool[strategy] = NULL;
        utils_cxt->ConfigureNamesInt[strategy] = NULL;
        utils_cxt->ConfigureNamesReal[strategy] = NULL;
        utils_cxt->ConfigureNamesInt64[strategy] = NULL;
        utils_cxt->ConfigureNamesString[strategy] = NULL;
        utils_cxt->ConfigureNamesEnum[strategy] = NULL;
    }
    utils_cxt->guc_dirty = false;
    utils_cxt->reporting_enabled = false;
    utils_cxt->GUCNestLevel = 0;
    utils_cxt->behavior_compat_flags = 0;

    utils_cxt->comboHash = NULL;
    utils_cxt->comboCids = NULL;
    utils_cxt->StreamParentComboCids = NULL;
    utils_cxt->usedComboCids = 0;
    utils_cxt->sizeComboCids = 0;
    utils_cxt->StreamParentsizeComboCids = 0;

    utils_cxt->CurrentSnapshotData = (SnapshotData*)palloc0(sizeof(SnapshotData));
    utils_cxt->CurrentSnapshotData->satisfies = SNAPSHOT_MVCC;
    utils_cxt->SecondarySnapshotData = (SnapshotData*)palloc0(sizeof(SnapshotData));
    utils_cxt->SecondarySnapshotData->satisfies = SNAPSHOT_MVCC;
    utils_cxt->CurrentSnapshot = NULL;
    utils_cxt->SecondarySnapshot = NULL;
    utils_cxt->CatalogSnapshot = NULL;
    utils_cxt->HistoricSnapshot = NULL;
    utils_cxt->CatalogSnapshotStale = true;

    utils_cxt->TransactionXmin = FirstNormalTransactionId;
    utils_cxt->RecentXmin = FirstNormalTransactionId;
    utils_cxt->RecentDataXmin = FirstNormalTransactionId;
    utils_cxt->RecentGlobalXmin = InvalidTransactionId;
    utils_cxt->RecentGlobalDataXmin = InvalidTransactionId;
    utils_cxt->RecentGlobalCatalogXmin = InvalidTransactionId;

    utils_cxt->cn_xc_maintain_mode = false;
    utils_cxt->snapshot_source = SNAPSHOT_UNDEFINED;
    utils_cxt->gxmin = InvalidTransactionId;
    utils_cxt->gxmax = InvalidTransactionId;
    utils_cxt->GtmTimeline = InvalidTransactionTimeline;
    utils_cxt->g_GTM_Snapshot = (GTM_SnapshotData*)palloc0(sizeof(GTM_SnapshotData));
    utils_cxt->g_snapshotcsn = 0;
    utils_cxt->snapshot_need_sync_wait_all = false;
    utils_cxt->is_autovacuum_snapshot = false;

    utils_cxt->tuplecid_data = NULL;
    utils_cxt->ActiveSnapshot = NULL;
    utils_cxt->RegisteredSnapshots = 0;
    utils_cxt->FirstSnapshotSet = false;
    utils_cxt->FirstXactSnapshot = NULL;
    utils_cxt->exportedSnapshots = NIL;
    utils_cxt->g_output_version = 1;
    utils_cxt->XactIsoLevel = 0;

    utils_cxt->memory_context_limited_white_list = NULL;
    utils_cxt->enable_memory_context_control = false;
    (void)syscalllockInit(&utils_cxt->deleMemContextMutex);
}

static void knl_u_security_init(knl_u_security_context* sec_cxt) {
    sec_cxt->last_roleid = InvalidOid;
    sec_cxt->last_roleid_is_super = false;
    sec_cxt->last_roleid_is_sysdba = false;
    sec_cxt->last_roleid_is_securityadmin = false;
    sec_cxt->last_roleid_is_auditadmin = false;
    sec_cxt->last_roleid_is_monitoradmin = false;
    sec_cxt->last_roleid_is_operatoradmin = false;
    sec_cxt->last_roleid_is_policyadmin = false;
    sec_cxt->roleid_callback_registered = false;
}

static void knl_u_streaming_init(knl_u_streaming_context* streaming_cxt) 
{
    streaming_cxt->gather_session = false;
    streaming_cxt->streaming_ddl_session = false;
}

static void knl_u_mb_init(knl_u_mb_context* mb_cxt)
{
    mb_cxt->insertValuesBind_compatible_illegal_chars = false;
    mb_cxt->ConvProcList = NIL; /* List of ConvProcInfo */
    mb_cxt->ToServerConvProc = NULL;
    mb_cxt->ToClientConvProc = NULL;
    mb_cxt->ClientEncoding = &pg_enc2name_tbl[PG_SQL_ASCII];
    mb_cxt->DatabaseEncoding = &pg_enc2name_tbl[PG_SQL_ASCII];
    mb_cxt->PlatformEncoding = NULL;
    mb_cxt->backend_startup_complete = false;
    mb_cxt->pending_client_encoding = PG_SQL_ASCII;
}

static void knl_u_plancache_init(knl_u_plancache_context* pcache_cxt)
{
    pcache_cxt->first_saved_plan = NULL;
    pcache_cxt->ungpc_saved_plan = NULL;
    pcache_cxt->query_has_params = false;
    pcache_cxt->prepared_queries = NULL;
    pcache_cxt->lightproxy_objs = NULL;
    pcache_cxt->stmt_lightproxy_htab = NULL;
    pcache_cxt->unnamed_gpc_lp = NULL;
    pcache_cxt->datanode_queries = NULL;
    pcache_cxt->unnamed_stmt_psrc = NULL;
    pcache_cxt->cur_stmt_psrc = NULL;
    pcache_cxt->private_refcount = 0;

    pcache_cxt->cur_stmt_name = NULL;
    pcache_cxt->gpc_in_ddl = false;
    pcache_cxt->gpc_remote_msg = false;
    pcache_cxt->gpc_first_send = true;
    pcache_cxt->gpc_in_try_store = false;
    pcache_cxt->gpc_in_batch = false;
}

static void knl_u_typecache_init(knl_u_typecache_context* tycache_cxt)
{
    tycache_cxt->TypeCacheHash = NULL;
    tycache_cxt->RecordCacheHash = NULL;
    tycache_cxt->RecordCacheArray = NULL;
    tycache_cxt->RecordCacheArrayLen = 0;
    tycache_cxt->NextRecordTypmod = 0;
}

static void knl_u_tscache_init(knl_u_tscache_context* tscache_cxt)
{
    tscache_cxt->TSParserCacheHash = NULL;
    tscache_cxt->lastUsedParser = NULL;
    tscache_cxt->TSDictionaryCacheHash = NULL;
    tscache_cxt->lastUsedDictionary = NULL;
    tscache_cxt->TSConfigCacheHash = NULL;
    tscache_cxt->lastUsedConfig = NULL;
    tscache_cxt->TSCurrentConfigCache = InvalidOid;
}

static void knl_u_misc_init(knl_misc_context* misc_cxt)
{
    misc_cxt->Mode = InitProcessing;
    misc_cxt->AuthenticatedUserId = InvalidOid;
    misc_cxt->SessionUserId = InvalidOid;
    misc_cxt->OuterUserId = InvalidOid;
    misc_cxt->CurrentUserId = InvalidOid;
    misc_cxt->CurrentUserName = NULL;
    misc_cxt->current_logic_cluster = InvalidOid;
    misc_cxt->current_nodegroup_mode = NG_UNKNOWN;
    misc_cxt->nodegroup_callback_registered = false;
    misc_cxt->Pseudo_CurrentUserId = NULL;
    misc_cxt->AuthenticatedUserIsSuperuser = false;
    misc_cxt->SessionUserIsSuperuser = false;
    misc_cxt->SecurityRestrictionContext = 0;
    misc_cxt->SetRoleIsActive = false;
    misc_cxt->process_shared_preload_libraries_in_progress = false;
    misc_cxt->authentication_finished = false;
}

static void knl_u_postgres_init(knl_u_postgres_context* postgres_cxt)
{
    postgres_cxt->doing_extended_query_message = false;
    postgres_cxt->ignore_till_sync = false;
}

static void knl_u_proc_init(knl_u_proc_context* proc_cxt)
{
    proc_cxt->MyProcPort = NULL;
    proc_cxt->MyRoleId = InvalidOid;
    proc_cxt->MyDatabaseId = InvalidOid;
    proc_cxt->MyDatabaseTableSpace = InvalidOid;
    proc_cxt->DatabasePath = NULL;
    proc_cxt->Isredisworker = false;
    proc_cxt->IsInnerMaintenanceTools = false;
    proc_cxt->clientIsGsrewind = false;
    proc_cxt->clientIsGsredis = false;
    proc_cxt->clientIsGsdump = false;
    proc_cxt->clientIsGsCtl = false;
    proc_cxt->clientIsGsroach = false;
    proc_cxt->IsBinaryUpgrade = false;
    proc_cxt->IsWLMWhiteList = false;
    proc_cxt->sessionBackupState = SESSION_BACKUP_NONE;
    proc_cxt->LabelFile = NULL;
    proc_cxt->TblspcMapFile = NULL;
    proc_cxt->registerAbortBackupHandlerdone = false;
    proc_cxt->gsRewindAddCount = false;
    proc_cxt->PassConnLimit = false;
    proc_cxt->sessionBackupState = SESSION_BACKUP_NONE;
    proc_cxt->registerExclusiveHandlerdone = false;
}

static void knl_u_time_init(knl_u_time_context* time_cxt)
{
    time_cxt->DateStyle = USE_ISO_DATES;
    time_cxt->DateOrder = DATEORDER_MDY;
    time_cxt->HasCTZSet = false;
    time_cxt->CTimeZone = 0;

    time_cxt->sz_timezone_tktbl = 0;
    time_cxt->timezone_tktbl = NULL;
    errno_t rc;
    rc = memset_s(time_cxt->datecache, sizeof(datetkn*) * MAXDATEFIELDS, 0, sizeof(datetkn*) * MAXDATEFIELDS);
    securec_check(rc, "\0", "\0");

    rc = memset_s(time_cxt->deltacache, sizeof(datetkn*) * MAXDATEFIELDS, 0, sizeof(datetkn*) * MAXDATEFIELDS);
    securec_check(rc, "\0", "\0");

    time_cxt->timezone_cache = NULL;
}

void knl_u_commands_init(knl_u_commands_context* cmd_cxt)
{
    cmd_cxt->TableSpaceUsageArray = NULL;
    cmd_cxt->isUnderCreateForeignTable = false;
    cmd_cxt->CurrentExtensionObject = InvalidOid;
    cmd_cxt->PendingLibraryDeletes = NIL;

    cmd_cxt->seqtab = NULL;
    cmd_cxt->last_used_seq = NULL;

    cmd_cxt->TypeCreateType = '\0';

    cmd_cxt->label_provider_list = NIL;
    cmd_cxt->bulkload_compatible_illegal_chars = false;
    cmd_cxt->bulkload_copy_state = NULL;
    cmd_cxt->dest_encoding_for_copytofile = -1;
    cmd_cxt->need_transcoding_for_copytofile = false;
    cmd_cxt->OBSParserContext = NULL;
    cmd_cxt->on_commits = NIL;
    cmd_cxt->topRelatationIsInMyTempSession = false;
    cmd_cxt->bogus_marker = {(NodeTag)0};
}

static void knl_u_contrib_init(knl_u_contrib_context* contrib_cxt)
{
    errno_t rc;

    contrib_cxt->cursor_number = 0;
    contrib_cxt->current_context_id = 0;
    contrib_cxt->file_format = DFS_INVALID;
    contrib_cxt->file_number = 0;

#define MAX_SLOTS 50 /* supports 50 files */
    contrib_cxt->slots = (FileSlot*)palloc0(sizeof(FileSlot) * MAX_SLOTS);
    contrib_cxt->slotid = 0;
    /* mark the default value as not revoke set_maxline_size */
    const int default_max_line_size = 1024;
    contrib_cxt->max_linesize = default_max_line_size;
    contrib_cxt->cur_directory = (char*)palloc0(sizeof(char) * MAXPGPATH);
    rc = memset_s(contrib_cxt->cur_directory, MAXPGPATH, 0, MAXPGPATH);
    securec_check(rc, "\0", "\0");
}

static void knl_u_upgrade_init(knl_u_upgrade_context* upg_cxt)
{
    upg_cxt->InplaceUpgradeSwitch = false;
    upg_cxt->binary_upgrade_next_etbl_pg_type_oid = 0;
    upg_cxt->binary_upgrade_next_etbl_array_pg_type_oid = 0;
    upg_cxt->binary_upgrade_next_etbl_toast_pg_type_oid = 0;
    upg_cxt->binary_upgrade_next_etbl_heap_pg_class_oid = 0;
    upg_cxt->binary_upgrade_next_etbl_index_pg_class_oid = 0;
    upg_cxt->binary_upgrade_next_etbl_toast_pg_class_oid = 0;
    upg_cxt->binary_upgrade_next_etbl_heap_pg_class_rfoid = 0;
    upg_cxt->binary_upgrade_next_etbl_index_pg_class_rfoid = 0;
    upg_cxt->binary_upgrade_next_etbl_toast_pg_class_rfoid = 0;

    upg_cxt->binary_upgrade_next_array_pg_type_oid = InvalidOid;
    upg_cxt->Inplace_upgrade_next_array_pg_type_oid = InvalidOid;

    upg_cxt->binary_upgrade_next_pg_authid_oid = InvalidOid;

    upg_cxt->binary_upgrade_next_toast_pg_type_oid = InvalidOid;
    upg_cxt->binary_upgrade_max_part_toast_pg_type_oid = 0;
    upg_cxt->binary_upgrade_cur_part_toast_pg_type_oid = 0;
    upg_cxt->binary_upgrade_next_part_toast_pg_type_oid = NULL;
    upg_cxt->binary_upgrade_next_heap_pg_class_oid = InvalidOid;
    upg_cxt->binary_upgrade_next_toast_pg_class_oid = InvalidOid;
    upg_cxt->binary_upgrade_next_heap_pg_class_rfoid = InvalidOid;
    upg_cxt->binary_upgrade_next_toast_pg_class_rfoid = InvalidOid;
    upg_cxt->binary_upgrade_max_part_pg_partition_oid = 0;
    upg_cxt->binary_upgrade_cur_part_pg_partition_oid = 0;
    upg_cxt->binary_upgrade_next_part_pg_partition_oid = NULL;
    upg_cxt->binary_upgrade_max_part_pg_partition_rfoid = 0;
    upg_cxt->binary_upgrade_cur_part_pg_partition_rfoid = 0;
    upg_cxt->binary_upgrade_next_part_pg_partition_rfoid = NULL;
    upg_cxt->binary_upgrade_max_part_toast_pg_class_oid = 0;
    upg_cxt->binary_upgrade_cur_part_toast_pg_class_oid = 0;
    upg_cxt->binary_upgrade_next_part_toast_pg_class_oid = NULL;
    upg_cxt->binary_upgrade_max_part_toast_pg_class_rfoid = 0;
    upg_cxt->binary_upgrade_cur_part_toast_pg_class_rfoid = 0;
    upg_cxt->binary_upgrade_next_part_toast_pg_class_rfoid = NULL;
    upg_cxt->binary_upgrade_next_partrel_pg_partition_oid = InvalidOid;
    upg_cxt->binary_upgrade_next_partrel_pg_partition_rfoid = InvalidOid;
    upg_cxt->Inplace_upgrade_next_heap_pg_class_oid = InvalidOid;
    upg_cxt->Inplace_upgrade_next_toast_pg_class_oid = InvalidOid;
    upg_cxt->binary_upgrade_next_index_pg_class_oid = InvalidOid;
    upg_cxt->binary_upgrade_next_index_pg_class_rfoid = InvalidOid;
    upg_cxt->binary_upgrade_max_part_index_pg_class_oid = 0;
    upg_cxt->binary_upgrade_cur_part_index_pg_class_oid = 0;
    upg_cxt->binary_upgrade_next_part_index_pg_class_oid = NULL;
    upg_cxt->binary_upgrade_max_part_index_pg_class_rfoid = 0;
    upg_cxt->binary_upgrade_cur_part_index_pg_class_rfoid = 0;
    upg_cxt->binary_upgrade_next_part_index_pg_class_rfoid = NULL;
    upg_cxt->bupgrade_max_psort_pg_class_oid = 0;
    upg_cxt->bupgrade_cur_psort_pg_class_oid = 0;
    upg_cxt->bupgrade_next_psort_pg_class_oid = NULL;
    upg_cxt->bupgrade_max_psort_pg_type_oid = 0;
    upg_cxt->bupgrade_cur_psort_pg_type_oid = 0;
    upg_cxt->bupgrade_next_psort_pg_type_oid = NULL;
    upg_cxt->bupgrade_max_psort_array_pg_type_oid = 0;
    upg_cxt->bupgrade_cur_psort_array_pg_type_oid = 0;
    upg_cxt->bupgrade_next_psort_array_pg_type_oid = NULL;
    upg_cxt->bupgrade_max_psort_pg_class_rfoid = 0;
    upg_cxt->bupgrade_cur_psort_pg_class_rfoid = 0;
    upg_cxt->bupgrade_next_psort_pg_class_rfoid = NULL;
    upg_cxt->Inplace_upgrade_next_index_pg_class_oid = InvalidOid;
    upg_cxt->binary_upgrade_next_pg_enum_oid = InvalidOid;
    upg_cxt->Inplace_upgrade_next_general_oid = InvalidOid;
    upg_cxt->bupgrade_max_cudesc_pg_class_oid = 0;
    upg_cxt->bupgrade_cur_cudesc_pg_class_oid = 0;
    upg_cxt->bupgrade_next_cudesc_pg_class_oid = NULL;
    upg_cxt->bupgrade_max_cudesc_pg_type_oid = 0;
    upg_cxt->bupgrade_cur_cudesc_pg_type_oid = 0;
    upg_cxt->bupgrade_next_cudesc_pg_type_oid = NULL;
    upg_cxt->bupgrade_max_cudesc_array_pg_type_oid = 0;
    upg_cxt->bupgrade_cur_cudesc_array_pg_type_oid = 0;
    upg_cxt->bupgrade_next_cudesc_array_pg_type_oid = NULL;
    upg_cxt->bupgrade_max_cudesc_pg_class_rfoid = 0;
    upg_cxt->bupgrade_cur_cudesc_pg_class_rfoid = 0;
    upg_cxt->bupgrade_next_cudesc_pg_class_rfoid = NULL;
    upg_cxt->bupgrade_max_cudesc_index_oid = 0;
    upg_cxt->bupgrade_cur_cudesc_index_oid = 0;
    upg_cxt->bupgrade_next_cudesc_index_oid = NULL;
    upg_cxt->bupgrade_max_cudesc_toast_pg_class_oid = 0;
    upg_cxt->bupgrade_cur_cudesc_toast_pg_class_oid = 0;
    upg_cxt->bupgrade_next_cudesc_toast_pg_class_oid = NULL;
    upg_cxt->bupgrade_max_cudesc_toast_pg_type_oid = 0;
    upg_cxt->bupgrade_cur_cudesc_toast_pg_type_oid = 0;
    upg_cxt->bupgrade_next_cudesc_toast_pg_type_oid = NULL;
    upg_cxt->bupgrade_max_cudesc_toast_index_oid = 0;
    upg_cxt->bupgrade_cur_cudesc_toast_index_oid = 0;
    upg_cxt->bupgrade_next_cudesc_toast_index_oid = NULL;
    upg_cxt->bupgrade_max_cudesc_index_rfoid = 0;
    upg_cxt->bupgrade_cur_cudesc_index_rfoid = 0;
    upg_cxt->bupgrade_next_cudesc_index_rfoid = NULL;
    upg_cxt->bupgrade_max_cudesc_toast_pg_class_rfoid = 0;
    upg_cxt->bupgrade_cur_cudesc_toast_pg_class_rfoid = 0;
    upg_cxt->bupgrade_next_cudesc_toast_pg_class_rfoid = NULL;
    upg_cxt->bupgrade_max_cudesc_toast_index_rfoid = 0;
    upg_cxt->bupgrade_cur_cudesc_toast_index_rfoid = 0;
    upg_cxt->bupgrade_next_cudesc_toast_index_rfoid = NULL;
    upg_cxt->bupgrade_max_delta_toast_pg_class_oid = 0;
    upg_cxt->bupgrade_cur_delta_toast_pg_class_oid = 0;
    upg_cxt->bupgrade_next_delta_toast_pg_class_oid = NULL;
    upg_cxt->bupgrade_max_delta_toast_pg_type_oid = 0;
    upg_cxt->bupgrade_cur_delta_toast_pg_type_oid = 0;
    upg_cxt->bupgrade_next_delta_toast_pg_type_oid = NULL;
    upg_cxt->bupgrade_max_delta_toast_index_oid = 0;
    upg_cxt->bupgrade_cur_delta_toast_index_oid = 0;
    upg_cxt->bupgrade_next_delta_toast_index_oid = NULL;
    upg_cxt->bupgrade_max_delta_toast_pg_class_rfoid = 0;
    upg_cxt->bupgrade_cur_delta_toast_pg_class_rfoid = 0;
    upg_cxt->bupgrade_next_delta_toast_pg_class_rfoid = NULL;
    upg_cxt->bupgrade_max_delta_toast_index_rfoid = 0;
    upg_cxt->bupgrade_cur_delta_toast_index_rfoid = 0;
    upg_cxt->bupgrade_next_delta_toast_index_rfoid = NULL;
    upg_cxt->bupgrade_max_delta_pg_class_oid = 0;
    upg_cxt->bupgrade_cur_delta_pg_class_oid = 0;
    upg_cxt->bupgrade_next_delta_pg_class_oid = NULL;
    upg_cxt->bupgrade_max_delta_pg_type_oid = 0;
    upg_cxt->bupgrade_cur_delta_pg_type_oid = 0;
    upg_cxt->bupgrade_next_delta_pg_type_oid = NULL;
    upg_cxt->bupgrade_max_delta_array_pg_type_oid = 0;
    upg_cxt->bupgrade_cur_delta_array_pg_type_oid = 0;
    upg_cxt->bupgrade_next_delta_array_pg_type_oid = NULL;
    upg_cxt->bupgrade_max_delta_pg_class_rfoid = 0;
    upg_cxt->bupgrade_cur_delta_pg_class_rfoid = 0;
    upg_cxt->bupgrade_next_delta_pg_class_rfoid = NULL;
    upg_cxt->Inplace_upgrade_next_pg_proc_oid = InvalidOid;
    upg_cxt->Inplace_upgrade_next_pg_namespace_oid = InvalidOid;
    upg_cxt->binary_upgrade_next_pg_type_oid = InvalidOid;
    upg_cxt->Inplace_upgrade_next_pg_type_oid = InvalidOid;
    upg_cxt->new_catalog_isshared = false;
    upg_cxt->new_catalog_need_storage = true;
    upg_cxt->new_shared_catalog_list = NIL;
}

static void knl_u_plpgsql_init(knl_u_plpgsql_context* plsql_cxt)
{
    plsql_cxt->inited = false;
    plsql_cxt->plpgsql_HashTable = NULL;
    plsql_cxt->plpgsql_pkg_HashTable = NULL;
    plsql_cxt->plpgsql_dlist_objects = NULL;
    plsql_cxt->plpgsqlpkg_dlist_objects = NULL;
    plsql_cxt->compile_status = NONE_STATUS;
    plsql_cxt->curr_compile_context = NULL;
    plsql_cxt->compile_context_list = NIL;
    plsql_cxt->plpgsql_IndexErrorVariable = 0;
    plsql_cxt->shared_simple_eval_resowner = NULL;
    plsql_cxt->simple_eval_estate = NULL;
    plsql_cxt->simple_econtext_stack = NULL;
    plsql_cxt->context_array = NIL;
    plsql_cxt->plugin_ptr = NULL;
    plsql_cxt->rendezvousHash = NULL;
    plsql_cxt->debug_proc_htbl = NULL;
    plsql_cxt->debug_client = NULL;
    plsql_cxt->has_step_into = false;
    plsql_cxt->cur_debug_server = NULL;
    plsql_cxt->dbe_output_buffer_limit = DEFAULT_DBE_BUFFER_LIMIT;
    plsql_cxt->is_delete_function = false;
    plsql_cxt->have_error = false;
    plsql_cxt->client_info = NULL;
    pthread_mutex_init(&plsql_cxt->client_info_lock, NULL);
    plsql_cxt->sess_cxt_htab = NULL;
    plsql_cxt->have_error = false;
    plsql_cxt->stp_savepoint_cnt = 0;
    plsql_cxt->nextStackEntryId = 0;
    plsql_cxt->spi_xact_context = NULL;
    plsql_cxt->package_as_line = 0;
    plsql_cxt->procedure_start_line = 0;
    plsql_cxt->insertError = false;
    plsql_cxt->errorList = NULL;
    plsql_cxt->plpgsql_yylloc = 0;
    plsql_cxt->rawParsePackageFunction = false;
    plsql_cxt->isCreateFunction = false;
    plsql_cxt->need_pkg_dependencies = false;
    plsql_cxt->pkg_dependencies = NIL;
    plsql_cxt->func_tableof_index = NIL;
    plsql_cxt->pass_func_tupdesc = NULL;
    plsql_cxt->portal_depth = 0;
    plsql_cxt->auto_parent_session_pkgs = NULL;
    plsql_cxt->not_found_parent_session_pkgs = false;
    plsql_cxt->storedPortals = NIL;
    plsql_cxt->portalContext = NIL;
    plsql_cxt->call_after_auto = false;
    plsql_cxt->parent_session_id = 0;
    plsql_cxt->parent_thread_id = 0;
    plsql_cxt->parent_context = NULL;
    plsql_cxt->is_package_instantiation = false;
    plsql_cxt->cur_exception_cxt = NULL;
    plsql_cxt->pragma_autonomous = false;
    plsql_cxt->ActiveLobToastOid = InvalidOid;
    plsql_cxt->is_insert_gs_source = false;
}

static void knl_u_stat_init(knl_u_stat_context* stat_cxt)
{
    Size size = 0;
    errno_t rc = EOK;

    stat_cxt->pgstat_stat_filename = NULL;
    stat_cxt->pgstat_stat_tmpname = NULL;
    stat_cxt->pgStatDBHash = NULL;
    stat_cxt->pgStatTabList = NULL;

    stat_cxt->BgWriterStats = (PgStat_MsgBgWriter*)palloc0(sizeof(PgStat_MsgBgWriter));
    stat_cxt->globalStats = (PgStat_GlobalStats*)palloc0(sizeof(PgStat_GlobalStats));
    stat_cxt->pgStatTabHash = NULL;
    stat_cxt->pgStatTabHashContext = NULL;
    stat_cxt->pgStatFunctions = NULL;
    stat_cxt->have_function_stats = false;
    stat_cxt->pgStatXactStack = NULL;
    stat_cxt->pgStatXactCommit = 0;
    stat_cxt->pgStatXactRollback = 0;
    stat_cxt->pgStatBlockReadTime = 0;
    stat_cxt->pgStatBlockWriteTime = 0;
    stat_cxt->localBackendStatusTable = NULL;
    stat_cxt->localNumBackends = 0;
    stat_cxt->analyzeCheckHash = NULL;
    stat_cxt->pgStatRunningInCollector = false;

    stat_cxt->last_report = 0;
    stat_cxt->isTopLevelPlSql = true;

    stat_cxt->pgStatCollectThdStatusContext = NULL;
    stat_cxt->pgStatLocalContext = NULL;

    size = sizeof(NumericValue) * TOTAL_OS_RUN_INFO_TYPES;
    stat_cxt->osStatDataArray = (NumericValue*)palloc0(size);

    size = sizeof(OSRunInfoDesc) * TOTAL_OS_RUN_INFO_TYPES;
    stat_cxt->osStatDescArray = (OSRunInfoDesc*)palloc0(size);
    rc = memcpy_s(stat_cxt->osStatDescArray, size, osStatDescArrayOrg, size);
    securec_check(rc, "\0", "\0");

    size = sizeof(int64) * TOTAL_TIME_INFO_TYPES;
    stat_cxt->localTimeInfoArray = (int64*)palloc0(size);
    stat_cxt->localNetInfo = (uint64*)palloc0(sizeof(uint64) * TOTAL_NET_INFO_TYPES);

    stat_cxt->trackedMemChunks = 0;
    stat_cxt->trackedBytes = 0;
    stat_cxt->hotkeySessContext = NULL;
    stat_cxt->hotkeyCandidates = NULL;
}

static void knl_u_storage_init(knl_u_storage_context* storage_cxt)
{
    storage_cxt->target_prefetch_pages = 0;
    storage_cxt->session_timeout_active = false;
    storage_cxt->session_fin_time = 0;

    /* var in fd.cpp */
    storage_cxt->nfile = 0;
    storage_cxt->have_xact_temporary_files = false;
    storage_cxt->temporary_files_size = 0;
    storage_cxt->numAllocatedDescs = 0;
    storage_cxt->maxAllocatedDescs = 0;
    storage_cxt->allocatedDescs = NULL;
    storage_cxt->tempFileCounter = 0;
    storage_cxt->tempTableSpaces = NULL;
    storage_cxt->numTempTableSpaces = -1;
    storage_cxt->nextTempTableSpace = 0;
    storage_cxt->AsyncSubmitIOCount = 0;
    storage_cxt->VfdCache = NULL;
    storage_cxt->SizeVfdCache = 0;

    /* var in smgr.cpp */
    storage_cxt->SMgrRelationHash = NULL;
    storage_cxt->unowned_reln = {{NULL, NULL}};

    /* var in md.cpp */
    storage_cxt->MdCxt = NULL;

    /* var in knl_uundofile.cpp */
    storage_cxt->UndoFileCxt = NULL;

    /* var in sync.cpp */
    storage_cxt->pendingUnlinks = NIL;
    storage_cxt->pendingOpsCxt = NULL;
    storage_cxt->sync_cycle_ctr = 0;
    storage_cxt->checkpoint_cycle_ctr = 0;
    storage_cxt->pendingOps = NULL;

    storage_cxt->nextLocalTransactionId = InvalidTransactionId;
    for (int i = 0; i < MAX_LOCKMETHOD; i++) {
        storage_cxt->holdSessionLock[i] = false;
    }
    storage_cxt->twoPhaseCommitInProgress = false;
    storage_cxt->dumpHashbucketIdNum = 0;
    storage_cxt->dumpHashbucketIds = NULL;

    /* session local buffer */
    storage_cxt->NLocBuffer = 0; /* until buffers are initialized */
    storage_cxt->LocalBufferDescriptors = NULL;
    storage_cxt->LocalBufferBlockPointers = NULL;
    storage_cxt->LocalRefCount = NULL;
    storage_cxt->nextFreeLocalBuf = 0;
    storage_cxt->LocalBufHash = NULL;
    storage_cxt->cur_block = NULL;
    storage_cxt->next_buf_in_block = 0;
    storage_cxt->num_bufs_in_block = 0;
    storage_cxt->total_bufs_allocated = 0;
    storage_cxt->LocalBufferContext = NULL;
}

static void knl_u_libpq_init(knl_u_libpq_context* libpq_cxt)
{
    Assert(libpq_cxt != NULL);
    libpq_cxt->cookies = NULL;
    libpq_cxt->cookies_size = 0;
    libpq_cxt->fscxt = NULL;
    libpq_cxt->server_key = (GS_UCHAR*)palloc0((CIPHER_LEN + 1) * sizeof(GS_UCHAR));
    libpq_cxt->ident_lines = NIL;
    libpq_cxt->ident_line_nums = NIL;
    libpq_cxt->ident_context = NULL;
    libpq_cxt->IsConnFromCmAgent = false;
    libpq_cxt->HasErrorAccurs = false;
#ifdef USE_SSL
    libpq_cxt->ssl_loaded_verify_locations = false;
    libpq_cxt->ssl_initialized = false;
    libpq_cxt->SSL_server_context = NULL;
#endif
}

#define BUCKET_MAP_SIZE 32

static void knl_u_relcache_init(knl_u_relcache_context* relcache_cxt)
{
    relcache_cxt->RelationIdCache = NULL;
    relcache_cxt->criticalRelcachesBuilt = false;
    relcache_cxt->criticalSharedRelcachesBuilt = false;
    relcache_cxt->relcacheInvalsReceived = 0L;
    relcache_cxt->initFileRelationIds = NIL;
    relcache_cxt->RelCacheNeedEOXActWork = false;
    relcache_cxt->OpClassCache = NULL;
    relcache_cxt->pgclassdesc = NULL;
    relcache_cxt->pgindexdesc = NULL;
    relcache_cxt->g_bucketmap_cache = NIL;
    relcache_cxt->max_bucket_map_size = BUCKET_MAP_SIZE;

    relcache_cxt->EOXactTupleDescArray = NULL;
    relcache_cxt->NextEOXactTupleDescNum = 0;
    relcache_cxt->EOXactTupleDescArrayLen = 0;
}

static void knl_u_unique_sql_init(knl_u_unique_sql_context* unique_sql_cxt)
{
    Assert(unique_sql_cxt != NULL);

    unique_sql_cxt->unique_sql_id = 0;
    unique_sql_cxt->unique_sql_user_id = InvalidOid;
    unique_sql_cxt->unique_sql_cn_id = InvalidOid;
    unique_sql_cxt->unique_sql_start_time = 0;
    unique_sql_cxt->unique_sql_returned_rows_counter = 0;
    unique_sql_cxt->unique_sql_soft_parse = 0;
    unique_sql_cxt->unique_sql_hard_parse = 0;
    unique_sql_cxt->last_stat_counter = (PgStat_TableCounts*)palloc0(sizeof(PgStat_TableCounts));
    unique_sql_cxt->current_table_counter = (PgStat_TableCounts*)palloc0(sizeof(PgStat_TableCounts));
    unique_sql_cxt->curr_single_unique_sql = NULL;
    unique_sql_cxt->is_multi_unique_sql = false;
    unique_sql_cxt->multi_sql_offset = 0;
    unique_sql_cxt->is_top_unique_sql = false;
    unique_sql_cxt->need_update_calls = true;
    unique_sql_cxt->skipUniqueSQLCount = 0;
    unique_sql_cxt->unique_sql_sort_instr = (unique_sql_sorthash_instr*)palloc0(sizeof(unique_sql_sorthash_instr));
    unique_sql_cxt->unique_sql_hash_instr = (unique_sql_sorthash_instr*)palloc0(sizeof(unique_sql_sorthash_instr));
    unique_sql_cxt->unique_sql_sort_instr->has_sorthash = false;
    unique_sql_cxt->unique_sql_hash_instr->has_sorthash = false;
    unique_sql_cxt->portal_nesting_level = 0;
#ifndef ENABLE_MULTIPLE_NODES
    unique_sql_cxt->unique_sql_text = NULL;
#endif
}

static void knl_u_percentile_init(knl_u_percentile_context* percentile_cxt)
{
    Assert(percentile_cxt != NULL);
    percentile_cxt->LocalsqlRT = NULL;
    percentile_cxt->LocalCounter = 0;
}

static void knl_u_slow_query_init(knl_u_slow_query_context* slow_query_cxt)
{
    Assert(slow_query_cxt != NULL);
    slow_query_cxt->slow_query.localTimeInfoArray = (int64*)palloc0(sizeof(int64) * TOTAL_TIME_INFO_TYPES);
    slow_query_cxt->slow_query.n_returned_rows = 0;
    slow_query_cxt->slow_query.current_table_counter = (PgStat_TableCounts*)palloc0(sizeof(PgStat_TableCounts));
    slow_query_cxt->slow_query.unique_sql_id = 0;
}

static void knl_u_trace_context_init(knl_u_trace_context* trace_cxt)
{
    Assert(trace_cxt != NULL);
    trace_cxt->trace_id[0] = '\0';
}

static void knl_u_ledger_init(knl_u_ledger_context *ledger_context)
{
    ledger_context->resp_tag = NULL;
}

static void knl_u_user_login_init(knl_u_user_login_context* user_login_cxt)
{
    Assert(user_login_cxt != NULL);
    user_login_cxt->CurrentInstrLoginUserOid = InvalidOid;
}

static void knl_u_statement_init(knl_u_statement_context* statement_cxt)
{
    Assert(statement_cxt != NULL);

    statement_cxt->db_name = NULL;
    statement_cxt->user_name = NULL;
    statement_cxt->client_addr = NULL;
    statement_cxt->client_port = INSTR_STMT_NULL_PORT;
    statement_cxt->session_id = 0;

    statement_cxt->curStatementMetrics = NULL;
    statement_cxt->allocatedCxtCnt = 0;

    statement_cxt->free_count = 0;
    statement_cxt->toFreeStatementList = NULL;
    statement_cxt->suspend_count = 0;
    statement_cxt->suspendStatementList = NULL;
    statement_cxt->executer_run_level = 0;

    (void)syscalllockInit(&statement_cxt->list_protect);
    statement_cxt->stmt_stat_cxt = NULL;
}

void knl_u_relmap_init(knl_u_relmap_context* relmap_cxt)
{
    relmap_cxt->shared_map = (RelMapFile*)palloc0(sizeof(RelMapFile));
    relmap_cxt->local_map = (RelMapFile*)palloc0(sizeof(RelMapFile));
    relmap_cxt->active_shared_updates = (RelMapFile*)palloc0(sizeof(RelMapFile));
    relmap_cxt->active_local_updates = (RelMapFile*)palloc0(sizeof(RelMapFile));
    relmap_cxt->pending_shared_updates = (RelMapFile*)palloc0(sizeof(RelMapFile));
    relmap_cxt->pending_local_updates = (RelMapFile*)palloc0(sizeof(RelMapFile));
    relmap_cxt->RelfilenodeMapHash = NULL;
    relmap_cxt->UHeapRelfilenodeMapHash = NULL;
}

void knl_u_inval_init(knl_u_inval_context* inval_cxt)
{
    inval_cxt->DeepthInAcceptInvalidationMessage = 0;
    inval_cxt->transInvalInfo = NULL;
    inval_cxt->SharedInvalidMessagesArray = NULL;
    inval_cxt->numSharedInvalidMessagesArray = 0;
    inval_cxt->maxSharedInvalidMessagesArray = 0;
    inval_cxt->syscache_callback_list = (SYSCACHECALLBACK*)palloc0(sizeof(SYSCACHECALLBACK) * MAX_SYSCACHE_CALLBACKS);
    inval_cxt->syscache_callback_count = 0;
    inval_cxt->relcache_callback_list = (RELCACHECALLBACK*)palloc0(sizeof(RELCACHECALLBACK) * MAX_RELCACHE_CALLBACKS);
    inval_cxt->relcache_callback_count = 0;
    inval_cxt->partcache_callback_list =
        (PARTCACHECALLBACK*)palloc0(sizeof(PARTCACHECALLBACK) * MAX_PARTCACHE_CALLBACKS);
    inval_cxt->partcache_callback_count = 0;
    inval_cxt->SIMCounter = 0;
    inval_cxt->catchupInterruptPending = 0;
    inval_cxt->messages = (SharedInvalidationMessage*)palloc0(MAXINVALMSGS * sizeof(SharedInvalidationMessage));
    inval_cxt->nextmsg = 0;
    inval_cxt->nummsgs = 0;
}

static void knl_u_catalog_init(knl_u_catalog_context* catalog_cxt)
{
    catalog_cxt->nulls[0] = false;
    catalog_cxt->nulls[1] = false;
    catalog_cxt->nulls[2] = false;
    catalog_cxt->nulls[3] = false;
    catalog_cxt->route = (PartitionIdentifier*)palloc0(sizeof(PartitionIdentifier));
    catalog_cxt->Parse_sql_language = false;
    catalog_cxt->pendingDeletes = NULL;
    catalog_cxt->ColMainFileNodes = NULL;
#define ColMainFileNodesDefNum 16
    catalog_cxt->ColMainFileNodesMaxNum = ColMainFileNodesDefNum;
    catalog_cxt->ColMainFileNodesCurNum = 0;
    catalog_cxt->pendingDfsDeletes = NIL;
    catalog_cxt->delete_conn = NULL;
    catalog_cxt->vf_store_root = NULL;
    catalog_cxt->currentlyReindexedHeap = InvalidOid;
    catalog_cxt->currentlyReindexedIndex = InvalidOid;
    catalog_cxt->pendingReindexedIndexes = NIL;
    catalog_cxt->activeSearchPath = NIL;
    catalog_cxt->activeCreationNamespace = InvalidOid;
    catalog_cxt->activeTempCreationPending = false;
    catalog_cxt->baseSearchPath = NIL;
    catalog_cxt->baseCreationNamespace = InvalidOid;
    catalog_cxt->baseTempCreationPending = false;
    catalog_cxt->namespaceUser = InvalidOid;
    catalog_cxt->baseSearchPathValid = true;
    catalog_cxt->overrideStack = NIL;
    catalog_cxt->overrideStackValid = true;
    catalog_cxt->setCurCreateSchema = false;
    catalog_cxt->curCreateSchema = NULL;
    catalog_cxt->myTempNamespaceOld = InvalidOid;
    catalog_cxt->myTempNamespace = InvalidOid;
    catalog_cxt->myTempToastNamespace = InvalidOid;
    catalog_cxt->deleteTempOnQuiting = false;
    catalog_cxt->myTempNamespaceSubID = InvalidSubTransactionId;
    catalog_cxt->redistribution_cancelable = false;
}

static void knl_u_cache_init(knl_u_cache_context* cache_cxt)
{
    cache_cxt->num_res = 0;
    cache_cxt->re_array = (cached_re_str*)palloc0(sizeof(cached_re_str) * MAX_CACHED_RES);

    cache_cxt->cached_privs_role = InvalidOid;
    cache_cxt->cached_privs_roles = NIL;
    cache_cxt->cached_member_role = InvalidOid;
    cache_cxt->cached_membership_roles = NIL;

    cache_cxt->plan_getrulebyoid = NULL;
    cache_cxt->plan_getviewrule = NULL;

    cache_cxt->att_opt_cache_hash = NULL;
    cache_cxt->cache_header = NULL;

    cache_cxt->TableSpaceCacheHash = NULL;

    cache_cxt->PartitionIdCache = NULL;
    cache_cxt->BucketIdCache = NULL;
    cache_cxt->PartCacheNeedEOXActWork = false;
    cache_cxt->bucket_cache_need_eoxact_work = false;
    cache_cxt->dn_hash_table = NULL;
}

static void knl_u_syscache_init(knl_u_syscache_context* syscache_cxt)
{
    syscache_cxt->SysCache = (CatCache**)palloc0(sizeof(CatCache*) * SysCacheSize);
    syscache_cxt->SysCacheRelationOid = (Oid*)palloc0(sizeof(Oid) * SysCacheSize);
    syscache_cxt->CacheInitialized = false;
}

static void knl_u_pgxc_init(knl_u_pgxc_context* pgxc_cxt)
{
#ifdef ENABLE_MULTIPLE_NODES
    pgxc_cxt->NumDataNodes = 0;
    pgxc_cxt->NumTotalDataNodes = 0;
#else
    pgxc_cxt->NumDataNodes = 1;
#endif
    pgxc_cxt->NumCoords = 0;
    pgxc_cxt->NumStandbyDataNodes = 0;
    pgxc_cxt->datanode_count = 0;
    pgxc_cxt->coord_count = 0;
    pgxc_cxt->dn_matrics = NULL;
    pgxc_cxt->dn_handles = NULL;
    pgxc_cxt->co_handles = NULL;
#ifdef ENABLE_MULTIPLE_NODES
    pgxc_cxt->PGXCNodeId = -1;
#else
    pgxc_cxt->PGXCNodeId = 0;
#endif
    pgxc_cxt->PGXCNodeIdentifier = 0;

    pgxc_cxt->remoteXactState = (RemoteXactState*)palloc0(sizeof(RemoteXactState));
    pgxc_cxt->XactWriteNodes = NIL;
    pgxc_cxt->XactReadNodes = NIL;
    pgxc_cxt->preparedNodes = NULL;

    pgxc_cxt->last_reported_send_errno = 0;
    pgxc_cxt->PoolerResendParams = false;
    pgxc_cxt->PoolerConnectionInfo = (PGXCNodeConnectionInfo*)palloc0(sizeof(PGXCNodeConnectionInfo));
    pgxc_cxt->poolHandle = NULL;
    pgxc_cxt->ConsistencyPointUpdating = false;
    pgxc_cxt->disasterReadArray = NULL;
    pgxc_cxt->DisasterReadArrayInit = false;

    pgxc_cxt->connection_cache = NIL;
    pgxc_cxt->connection_cache_handle = NIL;

    pgxc_cxt->is_gc_fdw = false;
    pgxc_cxt->is_gc_fdw_analyze = false;
    pgxc_cxt->gc_fdw_current_idx = 0;
    pgxc_cxt->gc_fdw_max_idx = 0;
    pgxc_cxt->gc_fdw_run_version = GCFDW_VERSION;
    pgxc_cxt->gc_fdw_snapshot = NULL;
    pgxc_cxt->primary_data_node = InvalidOid;
    pgxc_cxt->num_preferred_data_nodes = 0;
}

static void knl_u_erand_init(knl_u_erand_context* rand_cxt)
{
    uint seed = 0;
    do {
        struct timeval time;
        gettimeofday(&time, NULL);
        seed = t_thrd.postmaster_cxt.random_start_time.tv_usec ^
               ((time.tv_usec << 16) | ((time.tv_usec >> 16) & 0xffff));
    } while (seed == 0);
    
    rand_cxt->rand48_seed[0] = RAND48_SEED_0;
    rand_cxt->rand48_seed[1] = (unsigned short)seed;
    rand_cxt->rand48_seed[2] = (unsigned short)(seed >> 16);
}

static void knl_u_regex_init(knl_u_regex_context* regex_cxt)
{
    regex_cxt->pg_ctype_cache_list = NULL;
}

static void knl_u_xact_init(knl_u_xact_context* xact_cxt)
{
    xact_cxt->Xact_callbacks = NULL;
    xact_cxt->SubXact_callbacks = NULL;
#ifdef PGXC
    xact_cxt->dbcleanup_info = (abort_callback_type*)palloc0(sizeof(abort_callback_type));
    xact_cxt->dbcleanupInfoList = NIL;
#endif

    /* alloc in TopTransactionMemory Context, safe to set NULL */
    xact_cxt->prepareGID = NULL;
    /* alloc in TopMemory Context, initialization is NULL when create new session */
    xact_cxt->savePrepareGID = NULL;

    xact_cxt->pbe_execute_complete = true;
    xact_cxt->sendSeqDbName = NULL;
    xact_cxt->sendSeqSchmaName = NULL;
    xact_cxt->sendSeqName = NULL;
    xact_cxt->send_result = NULL;
    xact_cxt->ActiveLobRelid = InvalidOid;
}

static void knl_u_ps_init(knl_u_ps_context* ps_cxt)
{
#ifndef PS_USE_CLOBBER_ARGV
#define PS_BUFFER_SIZE 256
    ps_cxt->ps_buffer_size = PS_BUFFER_SIZE;
#endif
}

#ifdef ENABLE_MOT
static void knl_u_mot_init(knl_u_mot_context* mot_cxt)
{
    Assert(mot_cxt != NULL);
    mot_cxt->callbacks_set = false;
    mot_cxt->session_id = -1; // invalid session id
    mot_cxt->connection_id = -1; // invalid connection id
    mot_cxt->session_context = NULL;
    mot_cxt->txn_manager = NULL;
    mot_cxt->jit_session_context_pool = NULL;
    mot_cxt->jit_context_count = 0;
    mot_cxt->jit_llvm_if_stack = NULL;
    mot_cxt->jit_llvm_while_stack = NULL;
    mot_cxt->jit_llvm_do_while_stack = NULL;
    mot_cxt->jit_tvm_if_stack = NULL;
    mot_cxt->jit_tvm_while_stack = NULL;
    mot_cxt->jit_tvm_do_while_stack = NULL;
    mot_cxt->jit_context = NULL;
    mot_cxt->jit_txn = NULL;
}
#endif

static void knl_u_clientConnTime_init(knl_u_clientConnTime_context* clientConnTime_cxt)
{
    Assert(clientConnTime_cxt != NULL);

    /* Record start time while initializing session for client connection */
    INSTR_TIME_SET_CURRENT(clientConnTime_cxt->connStartTime);

    /* Set flag to "true", to indicate this session in initial process */
    clientConnTime_cxt->checkOnlyInConnProcess = true;
}

void knl_session_init(knl_session_context* sess_cxt)
{
    sess_cxt->status = KNL_SESS_UNINIT;
    DLInitElem(&sess_cxt->elem, sess_cxt);
    DLInitElem(&sess_cxt->elem2, sess_cxt);

    sess_cxt->attachPid = InvalidTid;
    sess_cxt->top_transaction_mem_cxt = NULL;
    sess_cxt->self_mem_cxt = NULL;
    sess_cxt->temp_mem_cxt = NULL;
    sess_cxt->dbesql_mem_cxt = NULL;
    sess_cxt->guc_variables = NULL;
    sess_cxt->num_guc_variables = 0;
    sess_cxt->session_id = 0;
    sess_cxt->globalSessionId.sessionId = 0;
    sess_cxt->globalSessionId.nodeId = 0;
    sess_cxt->globalSessionId.seq = 0;
    sess_cxt->debug_query_id = 0;
    sess_cxt->prog_name = NULL;
    sess_cxt->ClientAuthInProgress = false;
    sess_cxt->is_autonomous_session = false;
    sess_cxt->autonomous_parent_sessionid = 0;
    sess_cxt->need_report_top_xid = false;
    sess_cxt->on_sess_exit_index = 0;
    sess_cxt->cn_session_abort_count = 0;
    sess_cxt->sess_ident.cn_sessid = 0;
    sess_cxt->sess_ident.cn_nodeid = 0;
    sess_cxt->sess_ident.cn_timeline = 0;

    MemoryContextUnSeal(sess_cxt->top_mem_cxt);
    /* workload manager session context init */
    sess_cxt->wlm_cxt = (knl_u_wlm_context*)palloc0(sizeof(knl_u_wlm_context));

    knl_u_advisor_init(&sess_cxt->adv_cxt);
    knl_u_analyze_init(&sess_cxt->analyze_cxt);
    knl_u_attr_init(&sess_cxt->attr);
    knl_u_cache_init(&sess_cxt->cache_cxt);
    knl_u_catalog_init(&sess_cxt->catalog_cxt);
    knl_u_commands_init(&sess_cxt->cmd_cxt);
    knl_u_contrib_init(&sess_cxt->contrib_cxt);
    knl_u_erand_init(&sess_cxt->rand_cxt);
    knl_u_index_init(&sess_cxt->index_cxt);
    knl_u_instrument_init(&sess_cxt->instr_cxt);
    knl_u_inval_init(&sess_cxt->inval_cxt);
    knl_u_locale_init(&sess_cxt->lc_cxt);
    knl_u_log_init(&sess_cxt->log_cxt);
    knl_u_libpq_init(&sess_cxt->libpq_cxt);
    knl_u_mb_init(&sess_cxt->mb_cxt);
    knl_u_misc_init(&sess_cxt->misc_cxt);
    knl_u_optimizer_init(&sess_cxt->opt_cxt);
    knl_u_parser_init(&sess_cxt->parser_cxt);
    knl_u_pgxc_init(&sess_cxt->pgxc_cxt);
    knl_u_plancache_init(&sess_cxt->pcache_cxt);
    knl_u_plpgsql_init(&sess_cxt->plsql_cxt);
    knl_u_xact_init(&sess_cxt->xact_cxt);
    knl_u_postgres_init(&sess_cxt->postgres_cxt);
    knl_u_proc_init(&sess_cxt->proc_cxt);
    knl_u_ps_init(&sess_cxt->ps_cxt);
    knl_u_regex_init(&sess_cxt->regex_cxt);

    knl_u_relcache_init(&sess_cxt->relcache_cxt);
    knl_u_relmap_init(&sess_cxt->relmap_cxt);
    knl_u_sig_init(&sess_cxt->sig_cxt);
    knl_u_SPI_init(&sess_cxt->SPI_cxt);
    knl_u_stat_init(&sess_cxt->stat_cxt);
    knl_u_storage_init(&sess_cxt->storage_cxt);
    knl_u_stream_init(&sess_cxt->stream_cxt);
    knl_u_syscache_init(&sess_cxt->syscache_cxt);
    knl_u_time_init(&sess_cxt->time_cxt);
    knl_u_trigger_init(&sess_cxt->tri_cxt);
    knl_u_tscache_init(&sess_cxt->tscache_cxt);
    knl_u_typecache_init(&sess_cxt->tycache_cxt);
    knl_u_upgrade_init(&sess_cxt->upg_cxt);
    knl_u_utils_init(&sess_cxt->utils_cxt);
    knl_u_security_init(&sess_cxt->sec_cxt);
    knl_u_wlm_init(sess_cxt->wlm_cxt);
    knl_u_unique_sql_init(&sess_cxt->unique_sql_cxt);
    knl_u_user_login_init(&sess_cxt->user_login_cxt);
    knl_u_percentile_init(&sess_cxt->percentile_cxt);
    knl_u_slow_query_init(&sess_cxt->slow_query_cxt);
    knl_u_trace_context_init(&sess_cxt->trace_cxt);
    knl_u_statement_init(&sess_cxt->statement_cxt);
    knl_u_streaming_init(&sess_cxt->streaming_cxt);
    knl_u_ledger_init(&sess_cxt->ledger_cxt);
#ifdef ENABLE_MOT
    knl_u_mot_init(&sess_cxt->mot_cxt);
#endif
    KnlUUstoreInit(&sess_cxt->ustore_cxt);
    KnlURepOriginInit(&sess_cxt->reporigin_cxt);

    knl_u_clientConnTime_init(&sess_cxt->clientConnTime_cxt);

    MemoryContextSeal(sess_cxt->top_mem_cxt);
}

static void alloc_context_from_top(knl_session_context* sess, MemoryContext top_mem_cxt)
{
    sess->self_mem_cxt = AllocSetContextCreate(top_mem_cxt,
        "SessionSelfMemoryContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    sess->cache_mem_cxt = AllocSetContextCreate(top_mem_cxt,
        "SessionCacheMemoryContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        (1024 * 1024)); /* set max block size to 1MB */
    sess->temp_mem_cxt = AllocSetContextCreate(top_mem_cxt,
        "SessionTempMemoryContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    sess->syscache_cxt.SysCacheMemCxt = AllocSetContextCreate(top_mem_cxt,
        "SystemCacheMemoryContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    sess->stat_cxt.hotkeySessContext = AllocSetContextCreate(top_mem_cxt,
        "HotkeySessionMemoryContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
}

knl_session_context* create_session_context(MemoryContext parent, uint64 id)
{
    knl_session_context *sess, *old_sess;
    const int secondToMilliSecond = 1000;
    old_sess = u_sess;
    MemoryContextUnSeal(t_thrd.top_mem_cxt);
    sess = (knl_session_context*)MemoryContextAllocZero(parent, sizeof(knl_session_context));
    MemoryContextSeal(t_thrd.top_mem_cxt);

    MemoryContext top_mem_cxt;

    if (id == 0) {
        top_mem_cxt = t_thrd.top_mem_cxt;
        sess->mcxt_group = t_thrd.mcxt_group;
    } else {
        top_mem_cxt = AllocSetContextCreate(parent,
            "SessionTopMemoryContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            STANDARD_CONTEXT,
            DEFAULT_MEMORY_CONTEXT_MAX_SIZE,
            true);
        top_mem_cxt->session_id = id;
    }
    MemoryContext old_cxt = MemoryContextSwitchTo(top_mem_cxt);
    sess->top_mem_cxt = top_mem_cxt;
    knl_session_init(sess);
    sess->session_id = id;
#ifdef ENABLE_MULTIPLE_NODES
    sess->globalSessionId.sessionId = id;
#endif
    u_sess = sess;

    if (id != 0) {
        MemoryContextUnSeal(top_mem_cxt);
        sess->mcxt_group = New(top_mem_cxt) MemoryContextGroup();
        sess->mcxt_group->Init(top_mem_cxt);
        MemoryContextSeal(top_mem_cxt);
        /*
        * threadpool session, set init timeout to
        * avoid user only establish connections but do not send data or SSL shakehands hang
        * non-threadpool/fake session will direct call ReadCommand and enable alarm by sig timer
        * */
        (void)enable_session_sig_alarm(u_sess->attr.attr_common.SessionTimeout * secondToMilliSecond);
    }

    // Switch to context group, in case knl_u_executor_init will alloc memory on CurrentMemoryContext.
    MemoryContextSwitchTo(sess->mcxt_group->GetMemCxtGroup(MEMORY_CONTEXT_EXECUTOR));
    knl_u_executor_init(&sess->exec_cxt);

    alloc_context_from_top(sess, top_mem_cxt);

    /* Initialize portal manager */
    EnablePortalManager();

    u_sess = old_sess;
    MemoryContextSwitchTo(old_cxt);
    return sess;
}

void use_fake_session()
{
    u_sess = t_thrd.fake_session;
    SetThreadLocalGUC(u_sess);
}

void free_session_context(knl_session_context* session)
{
    Assert(u_sess == session);

    /* free the locale cache */
    freeLocaleCache(false);

    /* discard all the data in the channel. */
    t_thrd.libpq_cxt.PqSendPointer = 0;
    t_thrd.libpq_cxt.PqSendStart = 0;
    t_thrd.libpq_cxt.PqRecvPointer = 0;
    t_thrd.libpq_cxt.PqRecvLength = 0;
    t_thrd.libpq_cxt.PqCommBusy = false;
    t_thrd.libpq_cxt.DoingCopyOut = false;

    t_thrd.xact_cxt.next_xid = InvalidTransactionId;

    /* Release session related memory. */
    SelfMemoryContext = NULL;
    if (CurrentMemoryContext == u_sess->top_transaction_mem_cxt) {
        /* abnormal cleanup */
        MemoryContextSwitchTo(t_thrd.mem_cxt.msg_mem_cxt);
    }

    MemoryContextDeleteChildren(session->top_mem_cxt);
    MemoryContextDelete(session->top_mem_cxt);
    (void)syscalllockFree(&session->utils_cxt.deleMemContextMutex);
    pfree_ext(session);
    use_fake_session();
}

bool stp_set_commit_rollback_err_msg(stp_xact_err_type type)
{
    int rt;
    size_t maxMsgLen = sizeof(u_sess->SPI_cxt.forbidden_commit_rollback_err_msg);
    if (u_sess->SPI_cxt.forbidden_commit_rollback_err_msg[0] == '\0') {
        switch (type) {
            case STP_XACT_OPEN_FOR:
                rt = snprintf_s(u_sess->SPI_cxt.forbidden_commit_rollback_err_msg, maxMsgLen, maxMsgLen - 1, "%s",
                    "transaction statement in store procedure used as cursor is not supported");
                break;
            case STP_XACT_IN_TRIGGER:
                rt = snprintf_s(u_sess->SPI_cxt.forbidden_commit_rollback_err_msg, maxMsgLen, maxMsgLen - 1, "%s",
                    "transaction statement in store procedure used in trigger is not supported");
                break;
            case STP_XACT_USED_AS_EXPR:
                rt = snprintf_s(u_sess->SPI_cxt.forbidden_commit_rollback_err_msg, maxMsgLen, maxMsgLen - 1, "%s",
                    "transaction statement in store procedure used as a expression is not supported");
                break;
            case STP_XACT_GUC_IN_OPT_CLAUSE:
                rt = snprintf_s(u_sess->SPI_cxt.forbidden_commit_rollback_err_msg, maxMsgLen, maxMsgLen - 1, "%s",
                    "transaction statement in store procedure with GUC setting in option clause is not supported");
                break;
            case STP_XACT_OF_SECURE_DEFINER:
                rt = snprintf_s(u_sess->SPI_cxt.forbidden_commit_rollback_err_msg, maxMsgLen, maxMsgLen - 1, "%s",
                    "transaction statement in security-definer/proconfig/plugin-hooked functions is not supported");
                break;
            case STP_XACT_AFTER_TRIGGER_BEGIN:
                rt = snprintf_s(u_sess->SPI_cxt.forbidden_commit_rollback_err_msg, maxMsgLen, maxMsgLen - 1, "%s",
                    "transaction statement in store procedure used as sql to get value is not supported");
                break;
            case STP_XACT_PACKAGE_INSTANTIATION:
                rt = snprintf_s(u_sess->SPI_cxt.forbidden_commit_rollback_err_msg, maxMsgLen, maxMsgLen - 1, "%s",
                    "can not use commit/rollback/savepoint in package instantiation");
                break;
            case STP_XACT_COMPL_SQL:
                rt = snprintf_s(u_sess->SPI_cxt.forbidden_commit_rollback_err_msg, maxMsgLen, maxMsgLen - 1, "%s",
                    "can not use commit rollback in Complex SQL");
                break;
            case STP_XACT_IMMUTABLE:
                rt = snprintf_s(u_sess->SPI_cxt.forbidden_commit_rollback_err_msg, maxMsgLen, maxMsgLen - 1, "%s",
                    "commit/rollback/savepoint is not allowed in a non-volatile function");
                break;
            case STP_XACT_TOO_MANY_PORTAL:
                rt = snprintf_s(u_sess->SPI_cxt.forbidden_commit_rollback_err_msg, maxMsgLen, maxMsgLen - 1, "%s",
                    "transaction statement in store procedure is not supported used in multi-layer portal");
                break;
            default:
                rt = snprintf_s(u_sess->SPI_cxt.forbidden_commit_rollback_err_msg, maxMsgLen, maxMsgLen - 1, "%s",
                    "invalid transaction in store procedure");
                break;
        }
        if (rt < 0 || rt > ((int)maxMsgLen)) {
            ereport(ERROR, (errmsg("string of invalid transaction message would overflow buffer"))); 
        }
        return true;
    }
    return false;
}

__attribute__ ((__used__)) knl_session_context *GetCurrentSession()
{
    return u_sess;
}

bool enable_out_param_override()
{
    return u_sess->attr.attr_sql.sql_compatibility == A_FORMAT
                && PROC_OUTPARAM_OVERRIDE;
}
