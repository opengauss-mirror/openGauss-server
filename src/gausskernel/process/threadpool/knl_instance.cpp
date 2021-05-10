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
 * knl_instance.cpp
 *    Initial functions for instance level global variables.
 *
 * IDENTIFICATION
 *    src/gausskernel/process/threadpool/knl_instance.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <c.h>

#include "access/parallel_recovery/page_redo.h"
#include "access/reloptions.h"
#include "commands/prepare.h"
#include "executor/instrument.h"
#include "gssignal/gs_signal.h"
#include "instruments/instr_waitevent.h"
#include "knl/knl_instance.h"
#include "libcomm/libcomm.h"
#include "optimizer/cost.h"
#include "optimizer/dynsmp.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/streamplan.h"
#include "pgstat.h"
#include "regex/regex.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "workload/workload.h"
#include "instruments/instr_waitevent.h"
#include "pgstat.h"
#include "access/multi_redo_api.h"
#include "utils/hotkey.h"
#include "lib/lrucache.h"

const int SIZE_OF_TWO_UINT64 = 16;

knl_instance_context g_instance;

extern void InitGlobalSeq();
extern void InitGlobalVecFuncMap();

static void knl_g_cost_init(knl_g_cost_context* cost_cxt)
{
    cost_cxt->cpu_hash_cost = DEFAULT_CPU_HASH_COST;
    cost_cxt->send_kdata_cost = DEFAULT_SEND_KDATA_COST;
    cost_cxt->receive_kdata_cost = DEFAULT_RECEIVE_KDATA_COST;
    cost_cxt->disable_cost = 1.0e10;
    cost_cxt->disable_cost_enlarge_factor = 10;
}

static void knl_g_quota_init(knl_g_quota_context* quota_cxt)
{
    Assert(quota_cxt != NULL);
    quota_cxt->g_quota = 0;
    quota_cxt->g_quotanofify_ratio = 0;
    quota_cxt->g_having_quota = true;
    quota_cxt->g_quota_changing = NULL;
}

static void knl_g_localinfo_init(knl_g_localinfo_context* localinfo_cxt)
{
    Assert(localinfo_cxt != NULL);
    localinfo_cxt->g_local_host = NULL;
    localinfo_cxt->g_self_nodename = NULL;
    localinfo_cxt->g_local_ctrl_tcp_sock = -1;
    localinfo_cxt->sock_to_server_loop = -1;
    localinfo_cxt->gs_krb_keyfile = NULL;
}

static void knl_g_counters_init(knl_g_counters_context* counters_cxt)
{
    Assert(counters_cxt != NULL);
    counters_cxt->g_cur_node_num = 0;
    counters_cxt->g_expect_node_num = 0;
    counters_cxt->g_max_stream_num = 0;
    counters_cxt->g_recv_num = 0;
    counters_cxt->g_comm_send_timeout = 0;
}

static void knl_g_ckpt_init(knl_g_ckpt_context* ckpt_cxt)
{
    Assert(ckpt_cxt != NULL);
    errno_t rc = memset_s(ckpt_cxt, sizeof(knl_g_ckpt_context), 0, sizeof(knl_g_ckpt_context));
    securec_check(rc, "\0", "\0");
    SpinLockInit(&(ckpt_cxt->queue_lock));
}

static void knl_g_wal_init(knl_g_wal_context *const wal_cxt)
{
    wal_cxt->walInsertStatusTable = NULL;
    wal_cxt->walFlushWaitLock = NULL;
    wal_cxt->walBufferInitWaitLock = NULL;
    wal_cxt->walInitSegLock = NULL;
    wal_cxt->isWalWriterUp = false;
    wal_cxt->flushResult = InvalidXLogRecPtr;
    wal_cxt->sentResult = InvalidXLogRecPtr;
    wal_cxt->flushResultMutex = PTHREAD_MUTEX_INITIALIZER;
    wal_cxt->flushResultCV = (pthread_cond_t)PTHREAD_COND_INITIALIZER;
    wal_cxt->XLogFlusherCPU = 0;
    wal_cxt->isWalWriterSleeping = false;
    wal_cxt->criticalEntryMutex = PTHREAD_MUTEX_INITIALIZER;
    wal_cxt->criticalEntryCV = (pthread_cond_t)PTHREAD_COND_INITIALIZER;
    wal_cxt->globalEndPosSegNo = InvalidXLogSegPtr;
    wal_cxt->walWaitFlushCount = 0;
    wal_cxt->lastWalStatusEntryFlushed = -1;
    wal_cxt->lastLRCScanned = WAL_SCANNED_LRC_INIT;
    wal_cxt->lastLRCFlushed = WAL_SCANNED_LRC_INIT;
    wal_cxt->num_locks_in_group = 0;
}

static void knl_g_bgwriter_init(knl_g_bgwriter_context *bgwriter_cxt)
{
    Assert(bgwriter_cxt != NULL);
    bgwriter_cxt->bgwriter_procs = NULL;
    bgwriter_cxt->bgwriter_num = 0;
    bgwriter_cxt->curr_bgwriter_num = 0;
    bgwriter_cxt->candidate_buffers = NULL;
    bgwriter_cxt->candidate_free_map = NULL;
    bgwriter_cxt->bgwriter_actual_total_flush = 0;
    bgwriter_cxt->get_buf_num_candidate_list = 0;
    bgwriter_cxt->get_buf_num_clock_sweep = 0;
}

static void knl_g_tests_init(knl_g_tests_context* tests_cxt)
{
    Assert(tests_cxt != NULL);
    tests_cxt->libcomm_test_current_thread = 0;
    tests_cxt->libcomm_test_thread_arg = NULL;
    tests_cxt->libcomm_stop_flag = true;
    tests_cxt->libcomm_test_thread_num = 0;
    tests_cxt->libcomm_test_msg_len = 0;
    tests_cxt->libcomm_test_send_sleep = 0;
    tests_cxt->libcomm_test_send_once = 0;
    tests_cxt->libcomm_test_recv_sleep = 0;
    tests_cxt->libcomm_test_recv_once = 0;
}

static void knl_g_pollers_init(knl_g_pollers_context* pollers_cxt)
{
    Assert(pollers_cxt != NULL);
    pollers_cxt->g_libcomm_receiver_poller_list = NULL;
    pollers_cxt->g_r_libcomm_poller_list_lock = NULL;
    pollers_cxt->g_r_poller_list = NULL;
    pollers_cxt->g_r_poller_list_lock = NULL;
    pollers_cxt->g_s_poller_list = NULL;
    pollers_cxt->g_s_poller_list_lock = NULL;
}

static void knl_g_reqcheck_init(knl_g_reqcheck_context* reqcheck_cxt)
{
    Assert(reqcheck_cxt != NULL);
    reqcheck_cxt->g_shutdown_requested = false;
    reqcheck_cxt->g_cancel_requested = 0;
    reqcheck_cxt->g_close_poll_requested = false;
}

static void knl_g_mctcp_init(knl_g_mctcp_context* mctcp_cxt)
{
    Assert(mctcp_cxt != NULL);
    mctcp_cxt->mc_tcp_keepalive_idle = 0;
    mctcp_cxt->mc_tcp_keepalive_interval = 0;
    mctcp_cxt->mc_tcp_keepalive_count = 0;
    mctcp_cxt->mc_tcp_connect_timeout = 0;
    mctcp_cxt->mc_tcp_send_timeout = 0;
}

static void knl_g_commutil_init(knl_g_commutil_context* commutil_cxt)
{
    Assert(commutil_cxt != NULL);
    commutil_cxt->g_debug_mode = false;
    commutil_cxt->g_timer_mode = false;
    commutil_cxt->g_stat_mode = false;
    commutil_cxt->g_no_delay = false;
    commutil_cxt->g_total_usable_memory = 0;
    commutil_cxt->g_memory_pool_size = 0;
    commutil_cxt->ut_libcomm_test = false;
    commutil_cxt->g_comm_sender_buffer_size = 0;
}

static void knl_g_parallel_redo_init(knl_g_parallel_redo_context* predo_cxt)
{
    Assert(predo_cxt != NULL);
    predo_cxt->state = REDO_INIT;
    predo_cxt->parallelRedoCtx = NULL;
    for (int i = 0; i < MAX_RECOVERY_THREAD_NUM; ++i) {
        predo_cxt->pageRedoThreadStatusList[i].threadId = 0;
        predo_cxt->pageRedoThreadStatusList[i].threadState = PAGE_REDO_WORKER_INVALID;
    }
    predo_cxt->totalNum = 0;
    SpinLockInit(&(predo_cxt->rwlock));
    predo_cxt->redoPf.redo_start_ptr = 0;
    predo_cxt->redoPf.redo_start_time = 0;
    predo_cxt->redoPf.redo_done_time = 0;
    predo_cxt->redoPf.current_time = 0;
    predo_cxt->redoPf.min_recovery_point = 0;
    predo_cxt->redoPf.read_ptr = 0;
    predo_cxt->redoPf.last_replayed_end_ptr = 0;
    predo_cxt->redoPf.recovery_done_ptr = 0;
    predo_cxt->redoPf.speed_according_seg = 0;
    predo_cxt->redoPf.local_max_lsn = 0;
    predo_cxt->redoPf.oldest_segment = 0;
    knl_g_set_redo_finish_status(0);
    predo_cxt->redoType = DEFAULT_REDO;
    predo_cxt->pre_enable_switch = 0;
    SpinLockInit(&(predo_cxt->destroy_lock));
    for (int i = 0; i < NUM_MAX_PAGE_FLUSH_LSN_PARTITIONS; ++i) {
        pg_atomic_write_u64(&(predo_cxt->max_page_flush_lsn[i]), 0);
    }
    predo_cxt->permitFinishRedo = 0;
    predo_cxt->hotStdby = 0;
    predo_cxt->newestCheckpointLoc = InvalidXLogRecPtr;
    errno_t rc = memset_s(const_cast<CheckPoint *>(&predo_cxt->newestCheckpoint), sizeof(CheckPoint), 0, 
        sizeof(CheckPoint));
    securec_check(rc, "\0", "\0");
    predo_cxt->unali_buf = (char*)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
        NUM_MAX_PAGE_FLUSH_LSN_PARTITIONS * BLCKSZ + BLCKSZ);
    predo_cxt->ali_buf = (char*)TYPEALIGN(BLCKSZ, predo_cxt->unali_buf);
}

static void knl_g_cache_init(knl_g_cache_context* cache_cxt)
{
    cache_cxt->global_cache_mem = NULL;
    for (int i = 0; i < MAX_GLOBAL_CACHEMEM_NUM; ++i)
        cache_cxt->global_plancache_mem[i] = NULL;
}

void knl_g_cachemem_create()
{
    g_instance.cache_cxt.global_cache_mem = AllocSetContextCreate(g_instance.instance_context,
                                                                  "GlobalCacheMemory",
                                                                  ALLOCSET_DEFAULT_MINSIZE,
                                                                  ALLOCSET_DEFAULT_INITSIZE,
                                                                  ALLOCSET_DEFAULT_MAXSIZE,
                                                                  SHARED_CONTEXT,
                                                                  DEFAULT_MEMORY_CONTEXT_MAX_SIZE,
                                                                  false);

    for (int i = 0; i < MAX_GLOBAL_CACHEMEM_NUM; ++i) {  
        g_instance.cache_cxt.global_plancache_mem[i] = AllocSetContextCreate(g_instance.instance_context,
                                                                             "GlobalPlanCacheMemory",
                                                                             ALLOCSET_DEFAULT_MINSIZE,
                                                                             ALLOCSET_DEFAULT_INITSIZE,
                                                                             ALLOCSET_DEFAULT_MAXSIZE,
                                                                             SHARED_CONTEXT,
                                                                             DEFAULT_MEMORY_CONTEXT_MAX_SIZE,
                                                                             false);
    }
    g_instance.plan_cache = New(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR)) GlobalPlanCache();
}
static void knl_g_comm_init(knl_g_comm_context* comm_cxt)
{
    Assert(comm_cxt != NULL);
    comm_cxt->gs_wakeup_consumer = NULL;
    comm_cxt->g_receivers = NULL;
    comm_cxt->g_senders = NULL;
    comm_cxt->g_delay_survey_switch = false;
    comm_cxt->g_unix_path = NULL;
    comm_cxt->g_ha_shm_data = NULL;
    comm_cxt->g_usable_streamid = NULL;
    comm_cxt->g_r_node_sock = NULL;
    comm_cxt->g_s_node_sock = NULL;
    comm_cxt->g_delay_info = NULL;
    comm_cxt->g_c_mailbox = NULL;
    comm_cxt->g_p_mailbox = NULL;
    comm_cxt->libcomm_log_timezone = NULL;
    comm_cxt->force_cal_space_info = false;
    comm_cxt->cal_all_space_info_in_progress = false;
    comm_cxt->current_gsrewind_count = 0;
    comm_cxt->usedDnSpace = NULL;

    knl_g_quota_init(&g_instance.comm_cxt.quota_cxt);
    knl_g_localinfo_init(&g_instance.comm_cxt.localinfo_cxt);
    knl_g_counters_init(&g_instance.comm_cxt.counters_cxt);
    knl_g_tests_init(&g_instance.comm_cxt.tests_cxt);
    knl_g_pollers_init(&g_instance.comm_cxt.pollers_cxt);
    knl_g_reqcheck_init(&g_instance.comm_cxt.reqcheck_cxt);
    knl_g_mctcp_init(&g_instance.comm_cxt.mctcp_cxt);
    knl_g_commutil_init(&g_instance.comm_cxt.commutil_cxt);
    knl_g_parallel_redo_init(&g_instance.comm_cxt.predo_cxt);
}

static void knl_g_conn_init(knl_g_conn_context* conn_cxt)
{
    conn_cxt->CurConnCount = 0;
    conn_cxt->CurCMAConnCount = 0;
    SpinLockInit(&conn_cxt->ConnCountLock);
}

static void knl_g_executor_init(knl_g_executor_context* exec_cxt)
{
    exec_cxt->function_id_hashtbl = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    exec_cxt->nodeName = "Local Node";
#endif
}

static void knl_g_xlog_init(knl_g_xlog_context *xlog_cxt)
{
    xlog_cxt->num_locks_in_group = 0;
#ifdef ENABLE_MOT
    xlog_cxt->redoCommitCallback = NULL;
#endif
}

static void knl_g_libpq_init(knl_g_libpq_context* libpq_cxt)
{
    Assert(libpq_cxt != NULL);
    libpq_cxt->pam_passwd = NULL;
    libpq_cxt->pam_port_cludge = NULL;
    libpq_cxt->comm_parsed_hba_lines = NIL;
}

static void InitHotkeyResources(knl_g_stat_context* stat_cxt)
{
    if (stat_cxt->hotkeysCxt == NULL) {
        stat_cxt->hotkeysCxt = AllocSetContextCreate(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER),
            "HotkeysMemory",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            SHARED_CONTEXT);
    }
    stat_cxt->hotkeysCollectList = New(stat_cxt->hotkeysCxt) CircularQueue(HOTKEYS_QUEUE_LENGTH, stat_cxt->hotkeysCxt);
    stat_cxt->hotkeysCollectList->Init();
    stat_cxt->lru = New(stat_cxt->hotkeysCxt) LRUCache(LRU_QUEUE_LENGTH, stat_cxt->hotkeysCxt);
    stat_cxt->lru->Init();
    stat_cxt->fifo = NIL;
}

static void knl_g_stat_init(knl_g_stat_context* stat_cxt)
{
    stat_cxt->WaitCountHashTbl = NULL;
    stat_cxt->WaitCountStatusList = NULL;
    stat_cxt->pgStatSock = PGINVALID_SOCKET;
    stat_cxt->got_SIGHUP = false;

    stat_cxt->UniqueSqlContext = NULL;
    stat_cxt->UniqueSQLHashtbl = NULL;
    stat_cxt->InstrUserHTAB = NULL;
    stat_cxt->calculate_on_other_cn = false;
    stat_cxt->force_process = false;
    stat_cxt->RTPERCENTILE[0] = 0;
    stat_cxt->RTPERCENTILE[1] = 0;
    stat_cxt->NodeStatResetTime = 0;
    stat_cxt->sql_rt_info_array = NULL;

    stat_cxt->gInstanceTimeInfo = (int64*)MemoryContextAllocZero(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX), TOTAL_TIME_INFO_TYPES * sizeof(int64));
    errno_t rc;
    rc = memset_s(
        stat_cxt->gInstanceTimeInfo, TOTAL_TIME_INFO_TYPES * sizeof(int64), 0, TOTAL_TIME_INFO_TYPES * sizeof(int64));
    securec_check(rc, "\0", "\0");

    stat_cxt->snapshot_thread_counter = 0;

    stat_cxt->fileIOStat = (FileIOStat*)MemoryContextAllocZero(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX), sizeof(FileIOStat));
    rc = memset_s(stat_cxt->fileIOStat, sizeof(FileIOStat), 0, sizeof(FileIOStat));
    securec_check(rc, "\0", "\0");
    stat_cxt->active_sess_hist_arrary = NULL;
    stat_cxt->ash_appname = NULL;
    stat_cxt->instr_stmt_is_cleaning = false;
    stat_cxt->ash_clienthostname = NULL;
    stat_cxt->ASHUniqueSQLHashtbl = NULL;
    stat_cxt->track_context_hash = NULL;
    stat_cxt->track_memory_info_hash = NULL;
    pthread_rwlockattr_t attr;
    /* set write-first lock for rwlock to avoid hunger of wrlock */
    (void)pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    (void)pthread_rwlock_init(&(stat_cxt->track_memory_lock), &attr);
    
    InitHotkeyResources(stat_cxt);
}

static void knl_g_pid_init(knl_g_pid_context* pid_cxt)
{
    errno_t rc;
    rc = memset_s(pid_cxt, sizeof(knl_g_pid_context), 0, sizeof(knl_g_pid_context));
    pid_cxt->PageWriterPID = NULL;
    pid_cxt->CkptBgWriterPID = NULL;
    pid_cxt->CommReceiverPIDS = NULL;
    securec_check(rc, "\0", "\0");
}

static void knl_g_wlm_init(knl_g_wlm_context* wlm_cxt)
{
    wlm_cxt->parctl_process_memory = 8;  // initialize the per query memory as 8 MB

    wlm_cxt->cluster_state = NULL;
    wlm_cxt->dnnum_in_cluster_state = 0;
    wlm_cxt->is_ccn = false;
    wlm_cxt->ccn_idx = -1;
    wlm_cxt->rp_number_in_dn = 0;
}

static void knl_g_shmem_init(knl_g_shmem_context* shmem_cxt)
{
    shmem_cxt->MaxBackends = 100;
    shmem_cxt->MaxReserveBackendId = (AUXILIARY_BACKENDS + AV_LAUNCHER_PROCS);
    shmem_cxt->ThreadPoolGroupNum = 0;
    shmem_cxt->numaNodeNum = 1;
}

static void knl_g_heartbeat_init(knl_g_heartbeat_context* hb_cxt)
{
    Assert(hb_cxt != NULL);
    hb_cxt->heartbeat_running = false;
}

static void knl_g_compaction_init(knl_g_ts_compaction_context* tsc_cxt)
{
#ifdef ENABLE_MULTIPLE_NODES
    Assert(tsc_cxt != NULL);
    tsc_cxt->totalNum = 0;
    tsc_cxt->state = Compaction::COMPACTION_INIT;
    tsc_cxt->drop_db_count = 0;
    tsc_cxt->origin_id = (int*)palloc0(sizeof(int) * Compaction::MAX_TSCOMPATION_NUM);
    tsc_cxt->compaction_rest = false;
    tsc_cxt->dropdb_id = InvalidOid;
    for (int i = 0; i < Compaction::MAX_TSCOMPATION_NUM; i++) {
        tsc_cxt->compaction_worker_status_list[i].thread_id = 0;
        tsc_cxt->compaction_worker_status_list[i].thread_state = Compaction::COMPACTION_WORKER_INVALID;
    }
#endif
}

static void knl_g_csnminsync_init(knl_g_csnminsync_context* cms_cxt)
{
    Assert(cms_cxt != NULL);
    cms_cxt->stop = false;
    cms_cxt->is_fcn_ccn = true;
}

static void knl_g_dw_init(knl_g_dw_context *dw_cxt)
{
    Assert(dw_cxt != NULL);
    errno_t rc = memset_s(dw_cxt, sizeof(knl_g_dw_context), 0, sizeof(knl_g_dw_context));
    securec_check(rc, "\0", "\0");
    dw_cxt->closed = 1;
}

static void knl_g_numa_init(knl_g_numa_context* numa_cxt)
{
    numa_cxt->inheritThreadPool = false;
    numa_cxt->maxLength = INIT_NUMA_ALLOC_COUNT;
    numa_cxt->numaAllocInfos = (NumaMemAllocInfo*)MemoryContextAllocZero(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_NUMA), numa_cxt->maxLength * sizeof(NumaMemAllocInfo));
    numa_cxt->allocIndex = 0;
}

static void knl_g_oid_nodename_cache_init(knl_g_oid_nodename_mapping_cache *cache)
{
    Assert(cache != NULL);
    cache->s_mapping_hash = NULL;
    cache->s_valid = false;
}


static void knl_g_archive_obs_init(knl_g_archive_obs_context *archive_obs_cxt)
{
    errno_t rc = memset_s(archive_obs_cxt, sizeof(knl_g_archive_obs_context), 0, sizeof(knl_g_archive_obs_context));
    securec_check(rc, "\0", "\0");
    Assert(archive_obs_cxt != NULL);
    archive_obs_cxt->pitr_finish_result = false;
    archive_obs_cxt->pitr_task_status = PITR_TASK_NONE;
    archive_obs_cxt->archive_task.sub_term = -1;
    archive_obs_cxt->archive_task.term = 0;
    archive_obs_cxt->archive_task.targetLsn = 0;
    archive_obs_cxt->barrierLsn = 0;
    archive_obs_cxt->obs_slot_idx = -1;
    archive_obs_cxt->obs_slot_num = 0;
    archive_obs_cxt->sync_walsender_term = 0;
    archive_obs_cxt->archive_slot = (ReplicationSlot*)MemoryContextAllocZero(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(ReplicationSlot));
}

static void knl_g_archive_standby_init(knl_g_archive_standby_context* archive_standby_cxt)
{
    errno_t rc = memset_s(archive_standby_cxt, sizeof(knl_g_archive_standby_context), 0, sizeof(knl_g_archive_standby_context));
    securec_check(rc, "\0", "\0");
    Assert(archive_standby_cxt != NULL);
    archive_standby_cxt->arch_task_status = 0;
    archive_standby_cxt->arch_finish_result = false;
    archive_standby_cxt->need_to_send_archive_status = false;

    /* we don't need to use this parameter, but we should init it. */
    archive_standby_cxt->archive_task.sub_term = -1;
    archive_standby_cxt->archive_task.term = 0;
    archive_standby_cxt->archive_task.targetLsn = 0;

    archive_standby_cxt->archive_enabled = false;
    archive_standby_cxt->standby_archive_start_point = false;
    archive_standby_cxt->arch_latch = NULL;
}

#ifdef ENABLE_MOT
static void knl_g_mot_init(knl_g_mot_context* mot_cxt)
{
    mot_cxt->jitExecMode = JitExec::JIT_EXEC_MODE_INVALID;
}
#endif

static void knl_g_hypo_init(knl_g_hypo_context* hypo_cxt)
{
    hypo_cxt->HypopgContext = NULL;
    hypo_cxt->hypo_index_list = NULL;
}

void knl_instance_init()
{
    g_instance.binaryupgrade = false;
    g_instance.dummy_standby = false;
    g_instance.role = VUNKNOWN;
    g_instance.status = NoShutdown;
    g_instance.demotion = NoDemote;
    g_instance.fatal_error = false;
    g_instance.recover_error = false;
    g_instance.WalSegmentArchSucceed = true;
    g_instance.flush_buf_requested = 0;
    g_instance.codegen_IRload_process_count = 0;
    g_instance.t_thrd = &t_thrd;
    g_instance.stat_cxt.track_memory_inited = false;
    g_instance.proc_base = NULL;
    g_instance.proc_array_idx = NULL;

    /*
     * Set up the process wise memory context. The memory allocated from this
     * context will not be released untill it is called free. Meanwhile, the
     * memory context is visible to all threads but not thread safe, so only
     * postmaster thread shall use it or use it with lock protection.
     */
    g_instance.instance_context = AllocSetContextCreate((MemoryContext)NULL,
        "ProcessMemory",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    g_instance.mcxt_group = New(g_instance.instance_context) MemoryContextGroup();
    g_instance.mcxt_group->Init(g_instance.instance_context, true);
    MemoryContextSeal(g_instance.instance_context);
    MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));

    InitGlobalSeq();
    InitGlobalVecFuncMap();
    pthread_mutex_init(&g_instance.gpc_reset_lock, NULL);
    g_instance.signal_base = (struct GsSignalBase*)MemoryContextAllocZero(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), sizeof(GsSignalBase));
    g_instance.wlm_cxt = (struct knl_g_wlm_context*)MemoryContextAllocZero(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), sizeof(struct knl_g_wlm_context));
    knl_g_cache_init(&g_instance.cache_cxt);
    knl_g_comm_init(&g_instance.comm_cxt);
    knl_g_conn_init(&g_instance.conn_cxt);
    knl_g_cost_init(&g_instance.cost_cxt);
    knl_g_executor_init(&g_instance.exec_cxt);
    knl_g_libpq_init(&g_instance.libpq_cxt);
    knl_g_pid_init(&g_instance.pid_cxt);
    knl_g_stat_init(&g_instance.stat_cxt);
    knl_g_wlm_init(g_instance.wlm_cxt);
    knl_g_ckpt_init(&g_instance.ckpt_cxt);
    knl_g_bgwriter_init(&g_instance.bgwriter_cxt);
    knl_g_shmem_init(&g_instance.shmem_cxt);
    g_instance.ckpt_cxt_ctl = &g_instance.ckpt_cxt;
    g_instance.ckpt_cxt_ctl = (knl_g_ckpt_context*)TYPEALIGN(SIZE_OF_TWO_UINT64, g_instance.ckpt_cxt_ctl);
    knl_g_heartbeat_init(&g_instance.heartbeat_cxt);
    knl_g_csnminsync_init(&g_instance.csnminsync_cxt);
    knl_g_dw_init(&g_instance.dw_batch_cxt);
    knl_g_dw_init(&g_instance.dw_single_cxt);
    knl_g_xlog_init(&g_instance.xlog_cxt);
    knl_g_compaction_init(&g_instance.ts_compaction_cxt);
    knl_g_numa_init(&g_instance.numa_cxt);

#ifdef ENABLE_MOT
    knl_g_mot_init(&g_instance.mot_cxt);
#endif

    knl_g_wal_init(&g_instance.wal_cxt);
    knl_g_oid_nodename_cache_init(&g_instance.oid_nodename_cache);
    knl_g_archive_obs_init(&g_instance.archive_obs_cxt);
    knl_g_archive_standby_init(&g_instance.archive_standby_cxt);
    knl_g_hypo_init(&g_instance.hypo_cxt);
}

void add_numa_alloc_info(void* numaAddr, size_t length)
{
    if (g_instance.numa_cxt.allocIndex >= g_instance.numa_cxt.maxLength) {
        size_t newLength = g_instance.numa_cxt.maxLength * 2;
        g_instance.numa_cxt.numaAllocInfos =
            (NumaMemAllocInfo*)repalloc(g_instance.numa_cxt.numaAllocInfos, newLength * sizeof(NumaMemAllocInfo));
        g_instance.numa_cxt.maxLength = newLength;
    }
    g_instance.numa_cxt.numaAllocInfos[g_instance.numa_cxt.allocIndex].numaAddr = numaAddr;
    g_instance.numa_cxt.numaAllocInfos[g_instance.numa_cxt.allocIndex].length = length;
    ++g_instance.numa_cxt.allocIndex;
}

void knl_g_set_redo_finish_status(uint32 status)
{
#if defined(__arm__) || defined(__arm) || defined(__aarch64__) || defined(__aarch64)
    pg_memory_barrier();
#endif
    pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.isRedoFinish), status);
}

void knl_g_clear_local_redo_finish_status()
{
#if defined(__arm__) || defined(__arm) || defined(__aarch64__) || defined(__aarch64)
    pg_memory_barrier();
#endif
    (void)pg_atomic_fetch_and_u32(&(g_instance.comm_cxt.predo_cxt.isRedoFinish), ~(uint32)REDO_FINISH_STATUS_LOCAL);
}

bool knl_g_get_local_redo_finish_status()
{
#if defined(__arm__) || defined(__arm) || defined(__aarch64__) || defined(__aarch64)
    pg_memory_barrier();
#endif
    uint32 isRedoFinish = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.isRedoFinish));
    return (isRedoFinish & REDO_FINISH_STATUS_LOCAL) == REDO_FINISH_STATUS_LOCAL;
}

bool knl_g_get_redo_finish_status()
{
#if defined(__arm__) || defined(__arm) || defined(__aarch64__) || defined(__aarch64)
    pg_memory_barrier();
#endif
    uint32 isRedoFinish = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.isRedoFinish));
    return (isRedoFinish & REDO_FINISH_STATUS_CM) == REDO_FINISH_STATUS_CM;
}

