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
#include "access/xlog.h"
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
#include "utils/snapshot.h"
#include "workload/workload.h"
#include "instruments/instr_waitevent.h"
#include "access/multi_redo_api.h"
#include "utils/hotkey.h"
#include "lib/lrucache.h"
#ifdef ENABLE_WHITEBOX
#include "access/ustore/knl_whitebox_test.h"
#endif

const int SIZE_OF_TWO_UINT64 = 16;

knl_instance_context g_instance;

extern void InitGlobalVecFuncMap();

static void knl_g_cost_init(knl_g_cost_context* cost_cxt)
{
    cost_cxt->cpu_hash_cost = DEFAULT_CPU_HASH_COST;
    cost_cxt->send_kdata_cost = DEFAULT_SEND_KDATA_COST;
    cost_cxt->receive_kdata_cost = DEFAULT_RECEIVE_KDATA_COST;
    cost_cxt->disable_cost = 1.0e10;
    cost_cxt->disable_cost_enlarge_factor = 10;
    cost_cxt->sql_patch_sequence_id = 0;
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
    int ret = 0;
    ret = pthread_condattr_init(&wal_cxt->criticalEntryAtt);
    if (ret != 0) {
        elog(FATAL, "Fail to init conattr for walwrite");
    }
    ret = pthread_condattr_setclock(&wal_cxt->criticalEntryAtt, CLOCK_MONOTONIC);
    if (ret != 0) {
        elog(FATAL, "Fail to setclock walwrite");
    }
    ret = pthread_cond_init(&wal_cxt->criticalEntryCV, &wal_cxt->criticalEntryAtt);
    if (ret != 0) {
        elog(FATAL, "Fail to init cond for walwrite");
    }

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
    wal_cxt->globalEndPosSegNo = InvalidXLogSegPtr;
    wal_cxt->walWaitFlushCount = 0;
    wal_cxt->lastWalStatusEntryFlushed = -1;
    wal_cxt->lastLRCScanned = WAL_SCANNED_LRC_INIT;
    wal_cxt->lastLRCFlushed = WAL_SCANNED_LRC_INIT;
    wal_cxt->num_locks_in_group = 0;
    wal_cxt->upgradeSwitchMode = NoDemote;
    wal_cxt->totalXlogIterBytes = 0;
    wal_cxt->totalXlogIterTimes = 0;
    wal_cxt->xlogFlushStats = NULL;
    wal_cxt->walSenderStats = (WalSenderStats*)palloc0(sizeof(WalSenderStats));
    SpinLockInit(&wal_cxt->walSenderStats->mutex);
    wal_cxt->walReceiverStats = (WalReceiverStats*)palloc0(sizeof(WalReceiverStats));
    SpinLockInit(&wal_cxt->walReceiverStats->mutex);
    wal_cxt->walRecvWriterStats = (WalRecvWriterStats*)palloc0(sizeof(WalRecvWriterStats));
    SpinLockInit(&wal_cxt->walRecvWriterStats->mutex);
}

static void knl_g_bgwriter_init(knl_g_bgwriter_context *bgwriter_cxt)
{
    Assert(bgwriter_cxt != NULL);
    bgwriter_cxt->unlink_rel_hashtbl = NULL;
    bgwriter_cxt->rel_hashtbl_lock = NULL;
    bgwriter_cxt->invalid_buf_proc_latch = NULL;
    bgwriter_cxt->unlink_rel_fork_hashtbl = NULL;
    bgwriter_cxt->rel_one_fork_hashtbl_lock = NULL;
}

static void knl_g_repair_init(knl_g_repair_context *repair_cxt)
{
    Assert(repair_cxt != NULL);
    repair_cxt->page_repair_hashtbl = NULL;
    repair_cxt->page_repair_hashtbl_lock = NULL;
    repair_cxt->repair_proc_latch = NULL;
}

static void knl_g_startup_init(knl_g_startup_context *starup_cxt)
{
    Assert(starup_cxt != NULL);
    starup_cxt->remoteReadPageNum = 0;
    starup_cxt->badPageHashTbl = NULL;
    starup_cxt->current_record = NULL;
}

static void knl_g_dms_init(knl_g_dms_context *dms_cxt)
{
    Assert(dms_cxt != NULL);
    dms_cxt->dmsProcSid = 0;
    dms_cxt->SSReformerControl.list_stable = 0;
    dms_cxt->SSReformerControl.primaryInstId = -1;
    dms_cxt->SSReformInfo.in_reform = false;
    dms_cxt->SSReformInfo.dms_role = DMS_ROLE_UNKNOW;
    dms_cxt->SSReformInfo.old_bitmap = 0;
    dms_cxt->SSReformInfo.new_bitmap = 0;
    dms_cxt->SSReformInfo.redo_total_bytes = 0;
    dms_cxt->SSClusterState = NODESTATE_NORMAL;
    dms_cxt->SSRecoveryInfo.recovery_inst_id = INVALID_INSTANCEID;
    dms_cxt->SSRecoveryInfo.cluster_ondemand_status = CLUSTER_NORMAL;
    dms_cxt->SSRecoveryInfo.recovery_pause_flag = true;
    dms_cxt->SSRecoveryInfo.failover_ckpt_status = NOT_ACTIVE;
    dms_cxt->SSRecoveryInfo.new_primary_reset_walbuf_flag = false;
    dms_cxt->SSRecoveryInfo.ready_to_startup = false;
    dms_cxt->SSRecoveryInfo.startup_reform = true;
    dms_cxt->SSRecoveryInfo.restart_failover_flag = false;
    dms_cxt->SSRecoveryInfo.reform_ready = false;
    dms_cxt->SSRecoveryInfo.in_failover = false;
    dms_cxt->SSRecoveryInfo.in_flushcopy = false;
    dms_cxt->SSRecoveryInfo.no_backend_left = false;
    dms_cxt->SSRecoveryInfo.in_ondemand_recovery = false;
    dms_cxt->SSRecoveryInfo.ondemand_realtime_build_status = DISABLED;
    dms_cxt->SSRecoveryInfo.startup_need_exit_normally = false;
    dms_cxt->SSRecoveryInfo.recovery_trapped_in_page_request = false;
    dms_cxt->SSRecoveryInfo.disaster_cluster_promoting = false;
    dms_cxt->SSRecoveryInfo.dorado_sharestorage_inited = false;
    dms_cxt->SSRecoveryInfo.ondemand_recovery_pause_status = NOT_PAUSE;
    dms_cxt->log_timezone = NULL;
    pg_atomic_init_u32(&dms_cxt->inDmsThreShmemInitCnt, 0);
    pg_atomic_init_u32(&dms_cxt->inProcExitCnt, 0);
    dms_cxt->dmsInited = false;
    dms_cxt->ckptRedo = InvalidXLogRecPtr;
    dms_cxt->resetSyscache = false;
    dms_cxt->finishedRecoverOldPrimaryDWFile = false;
    dms_cxt->dw_init = false;
    {
        ss_xmin_info_t *xmin_info = &g_instance.dms_cxt.SSXminInfo;
        for (int i = 0; i < DMS_MAX_INSTANCES; i++) {
            ss_node_xmin_item_t *item = &xmin_info->node_table[i];
            item->active = false;
            SpinLockInit(&item->item_lock);
            item->notify_oldest_xmin = MaxTransactionId;
        }
        xmin_info->snap_cache = NULL;
        xmin_info->snap_oldest_xmin = MaxTransactionId;
        SpinLockInit(&xmin_info->global_oldest_xmin_lock);
        xmin_info->global_oldest_xmin = MaxTransactionId;
        xmin_info->prev_global_oldest_xmin = MaxTransactionId;
        xmin_info->global_oldest_xmin_active = false;
        SpinLockInit(&xmin_info->bitmap_active_nodes_lock);
        xmin_info->bitmap_active_nodes = 0;
        SpinLockInit(&xmin_info->snapshot_available_lock);
        xmin_info->snapshot_available = false;
    }
    dms_cxt->latest_snapshot_xmin = 0;
    dms_cxt->latest_snapshot_xmax = 0;
    dms_cxt->latest_snapshot_csn = 0;
    SpinLockInit(&dms_cxt->set_snapshot_mutex);
    {
        ss_fake_seesion_context_t *fs_cxt = &g_instance.dms_cxt.SSFakeSessionCxt;
        SpinLockInit(&fs_cxt->lock);
        fs_cxt->fake_sessions = NULL;
        fs_cxt->fake_session_cnt = 0;
        fs_cxt->quickFetchIndex = 0;
        fs_cxt->session_start = 0;
    }
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
    mctcp_cxt->mc_tcp_user_timeout = 0;
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
    predo_cxt->redoPf.oldest_segment = 1;
    knl_g_set_redo_finish_status(0);
    predo_cxt->redoType = DEFAULT_REDO;
    predo_cxt->pre_enable_switch = 0;
    SpinLockInit(&(predo_cxt->destroy_lock));
    for (int i = 0; i < NUM_MAX_PAGE_FLUSH_LSN_PARTITIONS; ++i) {
        pg_atomic_write_u64(&(predo_cxt->max_page_flush_lsn[i]), 0);
    }
    predo_cxt->permitFinishRedo = 0;
    predo_cxt->last_replayed_conflict_csn = 0;
    predo_cxt->hotStdby = 0;
    predo_cxt->newestCheckpointLoc = InvalidXLogRecPtr;
    errno_t rc = memset_s(const_cast<CheckPoint *>(&predo_cxt->newestCheckpoint), sizeof(CheckPoint), 0, 
        sizeof(CheckPoint));
    securec_check(rc, "\0", "\0");
    predo_cxt->unali_buf = (char*)MemoryContextAllocZero(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
        NUM_MAX_PAGE_FLUSH_LSN_PARTITIONS * BLCKSZ + BLCKSZ);
    predo_cxt->ali_buf = (char*)TYPEALIGN(BLCKSZ, predo_cxt->unali_buf);

    rc = memset_s(&predo_cxt->redoCpuBindcontrl, sizeof(RedoCpuBindControl), 0, sizeof(RedoCpuBindControl));
    securec_check(rc, "", "");
    predo_cxt->global_recycle_lsn = InvalidXLogRecPtr;
    predo_cxt->exrto_recyle_xmin = 0;
    predo_cxt->exrto_snapshot = (ExrtoSnapshot)MemoryContextAllocZero(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(ExrtoSnapshotData));
    predo_cxt->redoItemHashCtrl = NULL;

    predo_cxt->standby_read_delay_ddl_stat.delete_stat = 0;
    predo_cxt->standby_read_delay_ddl_stat.next_index_can_insert = 0;
    predo_cxt->standby_read_delay_ddl_stat.next_index_need_unlink = 0;
    predo_cxt->max_clog_pageno = 0;
    predo_cxt->buffer_pin_wait_buf_ids = NULL;
    predo_cxt->buffer_pin_wait_buf_len = 0;
    predo_cxt->exrto_send_lsn_forworder_time = 0;
}

static void knl_g_parallel_decode_init(knl_g_parallel_decode_context* pdecode_cxt)
{
    Assert(pdecode_cxt != NULL);
    pdecode_cxt->state = DECODE_INIT;
    pdecode_cxt->parallelDecodeCtx = NULL;
    pdecode_cxt->logicalLogCtx = NULL;
    pdecode_cxt->ParallelReaderWorkerStatus.threadId = 0;
    pdecode_cxt->ParallelReaderWorkerStatus.threadState = PARALLEL_DECODE_WORKER_INVALID;
    for (int i = 0; i < MAX_PARALLEL_DECODE_NUM; ++i) {
        pdecode_cxt->ParallelDecodeWorkerStatusList[i].threadId = 0;
        pdecode_cxt->ParallelDecodeWorkerStatusList[i].threadState = PARALLEL_DECODE_WORKER_INVALID;
    }
    pdecode_cxt->totalNum = 0;
    pdecode_cxt->edata = NULL;
    SpinLockInit(&(pdecode_cxt->rwlock));
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
    for (int i = 0; i < MAX_GLOBAL_PRC_NUM; ++i) {
        g_instance.cache_cxt.global_prc_mem[i] = AllocSetContextCreate(g_instance.instance_context,
                                                                       "GlobalPackageRuntimeCacheMemory",
                                                                       ALLOCSET_DEFAULT_MINSIZE,
                                                                       ALLOCSET_DEFAULT_INITSIZE,
                                                                       ALLOCSET_DEFAULT_MAXSIZE,
                                                                       SHARED_CONTEXT,
                                                                       DEFAULT_MEMORY_CONTEXT_MAX_SIZE,
                                                                       false);
    }
    g_instance.plan_cache = New(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR)) GlobalPlanCache();
    g_instance.global_session_pkg = PLGlobalPackageRuntimeCache::Instance();
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
    comm_cxt->isNeedChangeRole = false;
    comm_cxt->usedDnSpace = NULL;
    comm_cxt->request_disaster_cluster = true;
    comm_cxt->lastArchiveRcvTime = 0;
    comm_cxt->pLogCtl = NULL;
    comm_cxt->rejectRequest = false;

#ifdef USE_SSL
    comm_cxt->libcomm_data_port_list = NULL;
    comm_cxt->libcomm_ctrl_port_list = NULL;
#endif

    knl_g_quota_init(&g_instance.comm_cxt.quota_cxt);
    knl_g_localinfo_init(&g_instance.comm_cxt.localinfo_cxt);
    knl_g_counters_init(&g_instance.comm_cxt.counters_cxt);
    knl_g_tests_init(&g_instance.comm_cxt.tests_cxt);
    knl_g_pollers_init(&g_instance.comm_cxt.pollers_cxt);
    knl_g_reqcheck_init(&g_instance.comm_cxt.reqcheck_cxt);
    knl_g_mctcp_init(&g_instance.comm_cxt.mctcp_cxt);
    knl_g_commutil_init(&g_instance.comm_cxt.commutil_cxt);
    knl_g_parallel_redo_init(&g_instance.comm_cxt.predo_cxt);
    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; ++i) {
        knl_g_parallel_decode_init(&g_instance.comm_cxt.pdecode_cxt[i]);
    }
}

static void knl_g_conn_init(knl_g_conn_context* conn_cxt)
{
    conn_cxt->CurConnCount = 0;
    conn_cxt->CurCMAConnCount = 0;
    conn_cxt->CurCMAProcCount = 0;
    SpinLockInit(&conn_cxt->ConnCountLock);
}

static void knl_g_listen_sock_init(knl_g_listen_context* listen_sock_cxt)
{
    listen_sock_cxt->reload_fds = false;
    listen_sock_cxt->is_reloading_listen_socket = 0;
}

static void knl_g_executor_init(knl_g_executor_context* exec_cxt)
{
    exec_cxt->function_id_hashtbl = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    exec_cxt->nodeName = "Local Node";
    exec_cxt->global_application_name = "";
#endif
}

static void knl_g_rto_init(knl_g_rto_context *rto_cxt)
{
    rto_cxt->rto_standby_data = NULL;
}

static void knl_g_xlog_init(knl_g_xlog_context *xlog_cxt)
{
    xlog_cxt->num_locks_in_group = 0;
#ifdef ENABLE_MOT
    xlog_cxt->redoCommitCallback = NULL;
#endif
    xlog_cxt->shareStorageXLogCtl = NULL;
    xlog_cxt->shareStorageXLogCtlOrigin = NULL;
    errno_t rc = memset_s(&xlog_cxt->shareStorageopCtl, sizeof(ShareStorageOperateCtl), 0, 
        sizeof(ShareStorageOperateCtl));
    securec_check(rc, "\0", "\0");
    pthread_mutex_init(&xlog_cxt->remain_segs_lock, NULL);
    xlog_cxt->shareStorageLockFd = -1;
    xlog_cxt->ssReplicationXLogCtl = NULL;
}

static void KnlGUndoInit(knl_g_undo_context *undoCxt)
{
    using namespace undo;
    MemoryContext oldContext;
    g_instance.undo_cxt.undoContext = AllocSetContextCreate(g_instance.instance_context,
        "Undo", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_UNDO_MAXSIZE, SHARED_CONTEXT);
    oldContext = MemoryContextSwitchTo(g_instance.undo_cxt.undoContext);
    /* 
     * Create three bitmaps for undozone with three kinds of tables(permanent, unlogged and temp). 
     * Use -1 to initialize each bit of the bitmap as 1.
     */
    for (auto i = 0; i < UNDO_PERSISTENCE_LEVELS; i++) {
        g_instance.undo_cxt.uZoneBitmap[i] = bms_add_member(g_instance.undo_cxt.uZoneBitmap[i], PERSIST_ZONE_COUNT);
        memset_s((g_instance.undo_cxt.uZoneBitmap[i])->words,
            (g_instance.undo_cxt.uZoneBitmap[i])->nwords * sizeof(bitmapword),
            -1, (g_instance.undo_cxt.uZoneBitmap[i])->nwords * sizeof(bitmapword));
        Assert(bms_num_members(g_instance.undo_cxt.uZoneBitmap[i]) != 0);
    }
    MemoryContextSwitchTo(oldContext);
    undoCxt->undoTotalSize = 0;
    undoCxt->undoMetaSize = 0;
    undoCxt->uZoneCount = 0;
    undoCxt->maxChainSize = 0;
    undoCxt->undoChainTotalSize = 0;
    undoCxt->globalFrozenXid = InvalidTransactionId;
    undoCxt->globalRecycleXid = InvalidTransactionId;
    undoCxt->hotStandbyRecycleXid = InvalidTransactionId;
    undoCxt->is_exrto_residual_undo_file_recycled = false;
}

static void knl_g_flashback_init(knl_g_flashback_context *flashbackCxt)
{
    flashbackCxt->oldestXminInFlashback = InvalidTransactionId;
    flashbackCxt->globalOldestXminInFlashback = InvalidTransactionId;
}

static void knl_g_libpq_init(knl_g_libpq_context* libpq_cxt)
{
    Assert(libpq_cxt != NULL);
    libpq_cxt->pam_passwd = NULL;
    libpq_cxt->pam_port_cludge = NULL;
    libpq_cxt->comm_parsed_hba_lines = NIL;
    libpq_cxt->parsed_hba_context = NULL;
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
static void InitStbyStmtHist(knl_g_stat_context* stat_cxt)
{
    MemFileChainCreateParam param = {0};
    param.notInit = true;
    param.parent = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX);
    stat_cxt->stbyStmtHistFast = MemFileChainCreate(&param);
    stat_cxt->stbyStmtHistSlow = MemFileChainCreate(&param);
}

static void knl_g_stat_init(knl_g_stat_context* stat_cxt)
{
    stat_cxt->WaitCountHashTbl = NULL;
    stat_cxt->WaitCountStatusList = NULL;
    stat_cxt->pgStatSock = PGINVALID_SOCKET;
    stat_cxt->got_SIGHUP = false;

    stat_cxt->UniqueSqlContext = NULL;
    stat_cxt->UniqueSQLHashtbl = NULL;
    stat_cxt->stbyStmtHistFast = NULL;
    stat_cxt->stbyStmtHistSlow = NULL;
    stat_cxt->InstrUserHTAB = NULL;
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

    stat_cxt->tableStat = (UHeapPruneStat *) palloc0(sizeof(UHeapPruneStat));
    rc = memset_s(stat_cxt->tableStat, sizeof(UHeapPruneStat), 0, sizeof(UHeapPruneStat));
    securec_check(rc, "\0", "\0");

    stat_cxt->memory_log_directory = NULL;
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
    InitStbyStmtHist(stat_cxt);
}

static void knl_g_adv_init(knl_g_advisor_conntext* adv_cxt)
{
    adv_cxt->isOnlineRunning = 0;
    adv_cxt->isUsingGWC = 0;
    adv_cxt->maxMemory = 0;
    adv_cxt->maxsqlCount = 0;
    adv_cxt->currentUser = InvalidOid;
    adv_cxt->currentDB = InvalidOid;
    adv_cxt->GWCArray = NULL;

    adv_cxt->SQLAdvisorContext = NULL;
}

static void knl_g_pid_init(knl_g_pid_context* pid_cxt)
{
    errno_t rc;
    rc = memset_s(pid_cxt, sizeof(knl_g_pid_context), 0, sizeof(knl_g_pid_context));
    pid_cxt->PageWriterPID = NULL;
    pid_cxt->CommReceiverPIDS = NULL;
    pid_cxt->PgAuditPID = NULL;
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
	
    dw_cxt->old_batch_version = false;
    dw_cxt->recovery_dw_file_num = 0;
    dw_cxt->recovery_dw_file_size = 0;
}

static void knl_g_numa_init(knl_g_numa_context* numa_cxt)
{
    numa_cxt->inheritThreadPool = false;
    numa_cxt->maxLength = INIT_NUMA_ALLOC_COUNT;
    numa_cxt->numaAllocInfos = (NumaMemAllocInfo*)MemoryContextAllocZero(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_NUMA), numa_cxt->maxLength * sizeof(NumaMemAllocInfo));
    numa_cxt->allocIndex = 0;
}

static void knl_g_archive_obs_init(knl_g_archive_context *archive_cxt)
{
    errno_t rc = memset_s(archive_cxt, sizeof(knl_g_archive_context), 0, sizeof(knl_g_archive_context));
    securec_check(rc, "\0", "\0");
    Assert(archive_cxt != NULL);
    archive_cxt->barrierLsn = 0;
    archive_cxt->archive_slot_num = 0;
    archive_cxt->archive_recovery_slot_num = 0;
    archive_cxt->in_switchover = false;
    archive_cxt->in_service_truncate = false;
    archive_cxt->slot_tline = 0;
    archive_cxt->chosen_walsender_index = -1;
    SpinLockInit(&archive_cxt->barrier_lock);
}

static void knl_g_archive_thread_info_init(knl_g_archive_thread_info *archive_thread_info) 
{
    errno_t rc = memset_s(archive_thread_info, sizeof(knl_g_archive_thread_info), 0, 
        sizeof(knl_g_archive_thread_info));
    securec_check(rc, "\0", "\0");
    Assert(archive_thread_info != NULL);
}

#ifdef ENABLE_MOT
static void knl_g_mot_init(knl_g_mot_context* mot_cxt)
{
    mot_cxt->shmemVariableCache = NULL;
}
#endif

static void knl_g_hypo_init(knl_g_hypo_context* hypo_cxt)
{
    hypo_cxt->HypopgContext = NULL;
    hypo_cxt->hypo_index_list = NULL;
}

static void knl_g_segment_init(knl_g_segment_context *segment_cxt)
{
    segment_cxt->segment_drop_timeline = 0;
}

static void knl_g_pldebug_init(knl_g_pldebug_context* pldebug_cxt)
{
    pldebug_cxt->PldebuggerCxt = AllocSetContextCreate(g_instance.instance_context,
                                                       "PldebuggerSharedCxt",
                                                       ALLOCSET_DEFAULT_MINSIZE,
                                                       ALLOCSET_DEFAULT_INITSIZE,
                                                       ALLOCSET_DEFAULT_MAXSIZE,
                                                       SHARED_CONTEXT);

    errno_t rc = memset_s(pldebug_cxt->debug_comm, sizeof(PlDebuggerComm) * PG_MAX_DEBUG_CONN,
                          0, sizeof(pldebug_cxt->debug_comm));
    securec_check(rc, "\0", "\0");
    for (int i = 0; i < PG_MAX_DEBUG_CONN; i++) {
        PlDebuggerComm* debugcomm = &pldebug_cxt->debug_comm[i];
        pthread_mutex_init(&debugcomm->mutex, NULL);
        pthread_cond_init(&debugcomm->cond, NULL);
    }
}

static void knl_g_spi_plan_init(knl_g_spi_plan_context* spi_plan_cxt)
{
    spi_plan_cxt->global_spi_plan_context = NULL;
    spi_plan_cxt->FPlans = NULL;
    spi_plan_cxt->nFPlans = 0;
    spi_plan_cxt->PPlans = NULL;
    spi_plan_cxt->nPPlans = 0;
}

static void knl_g_roach_init(knl_g_roach_context* roach_cxt)
{
    roach_cxt->isRoachRestore = false;
    roach_cxt->targetTimeInPITR = NULL;
    roach_cxt->globalBarrierRecordForPITR = NULL;
    roach_cxt->isXLogForceRecycled = false;
    roach_cxt->forceAdvanceSlotTigger = false;
    roach_cxt->isGtmFreeCsn = false;
    roach_cxt->targetRestoreTimeFromMedia = NULL;
}

static void knl_g_dwsubdir_init(knl_g_dwsubdatadir_context* dw_subdir_cxt)
{
    Assert(dw_subdir_cxt != NULL);
    errno_t rc = memset_s(dw_subdir_cxt, sizeof(knl_g_dwsubdatadir_context), 0, sizeof(knl_g_dwsubdatadir_context));
    securec_check(rc, "\0", "\0");

    errno_t errorno = EOK;
    
    errorno = strcpy_s(dw_subdir_cxt->dwOldPath, MAXPGPATH, "global/pg_dw");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(dw_subdir_cxt->dwPathPrefix, MAXPGPATH, "global/pg_dw_");
    securec_check_c(errorno, "\0", "\0");
    
    errorno = strcpy_s(dw_subdir_cxt->dwSinglePath, MAXPGPATH, "global/pg_dw_single");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(dw_subdir_cxt->dwBuildPath, MAXPGPATH, "global/pg_dw.build");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(dw_subdir_cxt->dwUpgradePath, MAXPGPATH, "global/dw_upgrade");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(dw_subdir_cxt->dwBatchUpgradeMetaPath, MAXPGPATH, "global/dw_batch_upgrade_meta");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(dw_subdir_cxt->dwBatchUpgradeFilePath, MAXPGPATH, "global/dw_batch_upgrade_files");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(dw_subdir_cxt->dwMetaPath, MAXPGPATH, "global/pg_dw_meta");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(dw_subdir_cxt->dwExtChunkPath, MAXPGPATH, "global/pg_dw_ext_chunk");
    securec_check_c(errorno, "\0", "\0");

    dw_subdir_cxt->dwStorageType = 0;
}

static void knl_g_datadir_init(knl_g_datadir_context* datadir_init)
{
    errno_t errorno = EOK;
    
    errorno = strcpy_s(datadir_init->baseDir, MAXPGPATH, "base");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(datadir_init->globalDir, MAXPGPATH, "global");
    securec_check_c(errorno, "\0", "\0");
    
    errorno = strcpy_s(datadir_init->clogDir, MAXPGPATH, "pg_clog");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(datadir_init->csnlogDir, MAXPGPATH, "pg_csnlog");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(datadir_init->locationDir, MAXPGPATH, "pg_location");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(datadir_init->notifyDir, MAXPGPATH, "pg_notify");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(datadir_init->serialDir, MAXPGPATH, "pg_serial");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(datadir_init->snapshotsDir, MAXPGPATH, "pg_snapshots");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(datadir_init->tblspcDir, MAXPGPATH, "pg_tblspc");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(datadir_init->twophaseDir, MAXPGPATH, "pg_twophase");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(datadir_init->multixactDir, MAXPGPATH, "pg_multixact");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(datadir_init->xlogDir, MAXPGPATH, "pg_xlog");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(datadir_init->controlPath, MAXPGPATH, "global/pg_control");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(datadir_init->controlBakPath, MAXPGPATH, "global/pg_control.backup");
    securec_check_c(errorno, "\0", "\0");

    errorno = strcpy_s(datadir_init->controlInfoPath, MAXPGPATH, "pg_replication/pg_ss_ctl_info");
    securec_check_c(errorno, "\0", "\0");

    knl_g_dwsubdir_init(&datadir_init->dw_subdir_cxt);
}

static void knl_g_streaming_dr_init(knl_g_streaming_dr_context* streaming_dr_cxt)
{
    streaming_dr_cxt->isInSwitchover = false;
    streaming_dr_cxt->isInteractionCompleted = false;
    streaming_dr_cxt->hadrWalSndNum = 0;
    streaming_dr_cxt->interactionCompletedNum = 0;
    streaming_dr_cxt->switchoverBarrierLsn = InvalidXLogRecPtr;
    streaming_dr_cxt->rpoSleepTime = 0;
    streaming_dr_cxt->rpoBalanceSleepTime = 0;
    errno_t rc = memset_s(streaming_dr_cxt->currentBarrierId, MAX_BARRIER_ID_LENGTH, 0,
                          sizeof(streaming_dr_cxt->currentBarrierId));
    securec_check(rc, "\0", "\0");
    rc = memset_s(streaming_dr_cxt->targetBarrierId, MAX_BARRIER_ID_LENGTH, 0,
                  sizeof(streaming_dr_cxt->targetBarrierId));
    securec_check(rc, "\0", "\0");
    SpinLockInit(&streaming_dr_cxt->mutex);
}

static void knl_g_csn_barrier_init(knl_g_csn_barrier_context* csn_barrier_cxt)
{
    csn_barrier_cxt->barrier_hash_table = NULL;
    csn_barrier_cxt->barrier_hashtbl_lock = NULL;
    csn_barrier_cxt->barrier_context = NULL;
    errno_t rc = memset_s(csn_barrier_cxt->stopBarrierId, MAX_BARRIER_ID_LENGTH, 0,
                          sizeof(csn_barrier_cxt->stopBarrierId));
    securec_check(rc, "\0", "\0");
}

static void knl_g_audit_init(knl_g_audit_context *audit_cxt)
{
    g_instance.audit_cxt.global_audit_context = AllocSetContextCreate(g_instance.instance_context,
        "GlobalAuditMemory",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT,
        DEFAULT_MEMORY_CONTEXT_MAX_SIZE,
        false);

    g_instance.audit_cxt.sys_audit_pipes = NULL;
    g_instance.audit_cxt.audit_indextbl = NULL;
    g_instance.audit_cxt.audit_indextbl_old = NULL;
    g_instance.audit_cxt.current_audit_index = 0;
    g_instance.audit_cxt.thread_num = 1;
    g_instance.audit_cxt.audit_init_done = 0;

    for (int i = 0; i < MAX_AUDIT_NUM; ++i) {
        g_instance.audit_cxt.audit_coru_fnum[i] = UINT32_MAX;
    }
}

void knl_plugin_vec_func_init(knl_g_plugin_vec_func_context* func_cxt) {
    for (int i = 0; i < PLUGIN_VEC_FUNC_HATB_COUNT; i++) {
        func_cxt->vec_func_plugin[i] = NULL;
    }
}

#ifdef USE_SPQ
void knl_g_spq_context_init(knl_g_spq_context* spq_context)
{
    HASHCTL hash_ctl;
    errno_t rc = 0;

    spq_context->adp_connects_lock = PTHREAD_RWLOCK_INITIALIZER;

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");

    hash_ctl.keysize = sizeof(QCConnKey);
    hash_ctl.entrysize = sizeof(QCConnEntry);
    hash_ctl.hash = tag_hash;
    hash_ctl.hcxt = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR);
    spq_context->adp_connects = hash_create("QC_CONNECTS", 4096, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}
#endif

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
    g_instance.stat_cxt.switchover_timeout = false;
    g_instance.stat_cxt.print_stack_flag = false;
    g_instance.proc_base = NULL;
    g_instance.proc_array_idx = NULL;
    pg_atomic_init_u32(&g_instance.extensionNum, 0);

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

    InitGlobalVecFuncMap();
    pthread_mutex_init(&g_instance.gpc_reset_lock, NULL);
    g_instance.global_session_seq = 0;
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
    knl_g_repair_init(&g_instance.repair_cxt);
    knl_g_startup_init(&g_instance.startup_cxt);
    knl_g_dms_init(&g_instance.dms_cxt);
    knl_g_shmem_init(&g_instance.shmem_cxt);
    g_instance.ckpt_cxt_ctl = &g_instance.ckpt_cxt;
    g_instance.ckpt_cxt_ctl = (knl_g_ckpt_context*)TYPEALIGN(SIZE_OF_TWO_UINT64, g_instance.ckpt_cxt_ctl);
    knl_g_heartbeat_init(&g_instance.heartbeat_cxt);
    knl_g_csnminsync_init(&g_instance.csnminsync_cxt);
    knl_g_dw_init(&g_instance.dw_batch_cxt);
    knl_g_dw_init(&g_instance.dw_single_cxt);
    knl_g_rto_init(&g_instance.rto_cxt);
    knl_g_xlog_init(&g_instance.xlog_cxt);
    knl_g_compaction_init(&g_instance.ts_compaction_cxt);
    KnlGUndoInit(&g_instance.undo_cxt);
    knl_g_numa_init(&g_instance.numa_cxt);
    knl_g_adv_init(&g_instance.adv_cxt);
    knl_g_flashback_init(&g_instance.flashback_cxt);

#ifdef ENABLE_MOT
    knl_g_mot_init(&g_instance.mot_cxt);
#endif

    knl_g_wal_init(&g_instance.wal_cxt);
    knl_g_archive_obs_init(&g_instance.archive_obs_cxt);
    knl_g_archive_thread_info_init(&g_instance.archive_thread_info);
    knl_g_hypo_init(&g_instance.hypo_cxt);
    knl_g_segment_init(&g_instance.segment_cxt);

#ifdef ENABLE_WHITEBOX
    g_instance.whitebox_test_param_instance = (WhiteboxTestParam*) palloc(sizeof(WhiteboxTestParam) * (MAX_UNIT_TEST));
#endif

    knl_g_pldebug_init(&g_instance.pldebug_cxt);
    knl_g_spi_plan_init(&g_instance.spi_plan_cxt);
    knl_g_roach_init(&g_instance.roach_cxt);
    knl_g_streaming_dr_init(&g_instance.streaming_dr_cxt);
    knl_g_csn_barrier_init(&g_instance.csn_barrier_cxt);
    knl_g_audit_init(&g_instance.audit_cxt);
    knl_plugin_vec_func_init(&g_instance.plugin_vec_func_cxt);
    
#ifndef ENABLE_MULTIPLE_NODES
    for (int i = 0; i < DB_CMPT_MAX; i++) {
        pthread_mutex_init(&g_instance.loadPluginLock[i], NULL);
    }
    g_instance.needCheckConflictSubIds = NIL;
    pthread_mutex_init(&g_instance.subIdsLock, NULL);
#endif
    g_instance.noNeedWaitForCatchup = 0;

    knl_g_datadir_init(&g_instance.datadir_cxt);
    knl_g_listen_sock_init(&g_instance.listen_cxt);

#ifdef USE_SPQ
    knl_g_spq_context_init(&g_instance.spq_cxt);
#endif
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
