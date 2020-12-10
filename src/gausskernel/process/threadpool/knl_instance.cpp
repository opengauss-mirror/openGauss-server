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
	predo_cxt->endRecPtr = (XLogRecPtr)0xFFFFFFFFFFFFFFFF;
    SpinLockInit(&(predo_cxt->rwlock));
    predo_cxt->redoPf.redo_start_ptr = 0;
    predo_cxt->redoPf.redo_start_time = 0;
    predo_cxt->redoPf.redo_done_time = 0;
    predo_cxt->redoPf.current_time = 0;
    predo_cxt->redoPf.min_recovery_point = 0;
    predo_cxt->redoPf.read_ptr = 0;
    predo_cxt->redoPf.last_replayed_read_ptr = 0;
    predo_cxt->redoPf.recovery_done_ptr = 0;
    predo_cxt->redoPf.speed_according_seg = 0;
    predo_cxt->redoPf.local_max_lsn = 0;
    knl_g_set_is_local_redo_finish(false);
    predo_cxt->redoType = DEFAULT_REDO;
    SpinLockInit(&(predo_cxt->destroy_lock));
}

static void knl_g_cache_init(knl_g_cache_context* cache_cxt)
{
    cache_cxt->global_cache_mem = AllocSetContextCreate(g_instance.instance_context,
                                                        "GlobalCacheMemory",
                                                        ALLOCSET_DEFAULT_MINSIZE,
                                                        ALLOCSET_DEFAULT_INITSIZE,
                                                        ALLOCSET_DEFAULT_MAXSIZE,
                                                        SHARED_CONTEXT,
                                                        DEFAULT_MEMORY_CONTEXT_MAX_SIZE,
                                                        false);
}

static void knl_g_comm_init(knl_g_comm_context* comm_cxt)
{
    Assert(comm_cxt != NULL);
    comm_cxt->gs_wakeup_consumer = NULL;
    comm_cxt->g_comm_tcp_mode = true;
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

static void knl_g_executor_init(knl_g_executor_context* exec_cxt)
{
    exec_cxt->function_id_hashtbl = NULL;
}

static void knl_g_xlog_init(knl_g_xlog_context *xlog_cxt)
{
    xlog_cxt->num_locks_in_group = 0;
    xlog_cxt->walCallback = NULL;
    xlog_cxt->redoCommitCallback = NULL;
}

static void knl_g_libpq_init(knl_g_libpq_context* libpq_cxt)
{
    Assert(libpq_cxt != NULL);
    libpq_cxt->pam_passwd = NULL;
    libpq_cxt->pam_port_cludge = NULL;
    libpq_cxt->comm_parsed_hba_lines = NIL;
}

static void knl_g_stat_init(knl_g_stat_context* stat_cxt)
{
    stat_cxt->WaitCountHashTbl = NULL;
    stat_cxt->WaitCountStatusList = NULL;
    stat_cxt->pgStatSock = PGINVALID_SOCKET;
    stat_cxt->need_exit = false;
    stat_cxt->got_SIGHUP = false;

    stat_cxt->UniqueSQLHashtbl = NULL;
    stat_cxt->InstrUserHTAB = NULL;
    stat_cxt->calculate_on_other_cn = false;
    stat_cxt->force_process = false;
    stat_cxt->RTPERCENTILE[0] = 0;
    stat_cxt->RTPERCENTILE[1] = 0;
    stat_cxt->NodeStatResetTime = 0;
    stat_cxt->sql_rt_info_array = NULL;

    stat_cxt->gInstanceTimeInfo = (int64*)palloc0(TOTAL_TIME_INFO_TYPES * sizeof(int64));
    errno_t rc;
    rc = memset_s(
        stat_cxt->gInstanceTimeInfo, TOTAL_TIME_INFO_TYPES * sizeof(int64), 0, TOTAL_TIME_INFO_TYPES * sizeof(int64));
    securec_check(rc, "\0", "\0");

    stat_cxt->snapshot_thread_counter = 0;

    stat_cxt->fileIOStat = (FileIOStat*)palloc0(sizeof(FileIOStat));
    rc = memset_s(stat_cxt->fileIOStat, sizeof(FileIOStat), 0, sizeof(FileIOStat));
    securec_check(rc, "\0", "\0");
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
    shmem_cxt->max_parallel_workers = 8;
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

static void knl_g_dw_init(knl_g_dw_context *dw_cxt)
{
    Assert(dw_cxt != NULL);
    dw_cxt->flush_lock = NULL;
}

static void knl_g_numa_init(knl_g_numa_context* numa_cxt)
{
    numa_cxt->inheritThreadPool = false;
    numa_cxt->maxLength = INIT_NUMA_ALLOC_COUNT;
    numa_cxt->numaAllocInfos = (NumaMemAllocInfo*)palloc0(numa_cxt->maxLength * sizeof(NumaMemAllocInfo));
    numa_cxt->allocIndex = 0;
}

static void knl_g_bgworker_init(knl_g_bgworker_context* bgworker_cxt)
{
    bgworker_cxt->start_worker_needed = true;
    bgworker_cxt->have_crashed_worker = false;
}

static void knl_g_mot_init(knl_g_mot_context* mot_cxt)
{
    mot_cxt->jitExecMode = JitExec::JIT_EXEC_MODE_INVALID;
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
    MemoryContext old_cxt = MemoryContextSwitchTo(g_instance.instance_context);

    InitGlobalVecFuncMap();

    g_instance.signal_base = (struct GsSignalBase*)palloc0(sizeof(GsSignalBase));
    g_instance.wlm_cxt = (struct knl_g_wlm_context*)palloc0(sizeof(struct knl_g_wlm_context));
    knl_g_cache_init(&g_instance.cache_cxt);
    knl_g_comm_init(&g_instance.comm_cxt);
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
    knl_g_dw_init(&g_instance.dw_cxt);
    knl_g_xlog_init(&g_instance.xlog_cxt);
    knl_g_numa_init(&g_instance.numa_cxt);
    knl_g_mot_init(&g_instance.mot_cxt);
    knl_g_bgworker_init(&g_instance.bgworker_cxt);

    MemoryContextSwitchTo(old_cxt);

    GPC = New(g_instance.instance_context) GlobalPlanCache();
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

void knl_g_set_is_local_redo_finish(bool status)
{
#if defined(__arm__) || defined(__arm) || defined(__aarch64__) || defined(__aarch64)
    pg_memory_barrier();
#endif
    status ? pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.isLocalRedoFinish), 1) :
        pg_atomic_write_u32(&(g_instance.comm_cxt.predo_cxt.isLocalRedoFinish), 0);
}

bool knl_g_get_is_local_redo()
{
#if defined(__arm__) || defined(__arm) || defined(__aarch64__) || defined(__aarch64)
    pg_memory_barrier();
#endif
    uint32 isLocalRedo = pg_atomic_read_u32(&(g_instance.comm_cxt.predo_cxt.isLocalRedoFinish));
    return (isLocalRedo != 0);
}
