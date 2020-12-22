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
 * ---------------------------------------------------------------------------------------
 *
 * knl_instance.h
 *        Data stucture for instance level global variables.
 *
 *  Instance level variables can be accessed by all threads in gaussdb.
 *  Thread competition must be considered at the first place. If it can
 *  be changed by every thread in this process, then we must use lock or
 *  atomic operation to prevent competition.
 *
 * IDENTIFICATION
 *        src/include/knl/knl_instance.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_KNL_KNL_INSTANCE_H_
#define SRC_INCLUDE_KNL_KNL_INSTANCE_H_

#include <sched.h>
#include "c.h"
#include "datatype/timestamp.h"
#include "gs_thread.h"
#include "knl/knl_guc.h"
#include "nodes/pg_list.h"
#include "storage/s_lock.h"
#include "access/double_write_basic.h"
#include "utils/palloc.h"
#include "replication/replicainternal.h"
#include "storage/latch.h"
#include "libcomm/libcomm.h"
#include "knl/knl_session.h"
#include "hotpatch/hotpatch.h"
#include "hotpatch/hotpatch_backend.h"
#include "utils/atomic.h"
#include "postmaster/bgwriter.h"
#include "postmaster/pagewriter.h"
#include "replication/heartbeat.h"
#include "access/multi_redo_settings.h"
#include "access/redo_statistic_msg.h"
#include "portability/instr_time.h"
#include "replication/rto_statistic.h"
#include "storage/mot/jit_def.h"

const int NUM_PERCENTILE_COUNT = 2;
const int INIT_NUMA_ALLOC_COUNT = 32;

enum knl_virtual_role {
    VUNKNOWN = 0,
    VCOORDINATOR = 1,
    VDATANODE = 2,
    VSINGLENODE = 3,
};

/*
 * start process: set to REDO_INIT
 * parallel redo begin:set to REDO_STARTING_BEGIN, wait for redo worker thread to start
 * all redo workers are started or waiting timeout: set to REDO_STARTING_END,
 * if redo worker starts at this stage, the redo worker has to exit
 */
enum knl_parallel_redo_state {
    REDO_INIT = 0,
    REDO_STARTING_BEGIN,
    REDO_STARTING_END,
    REDO_IN_PROGRESS,
    REDO_DONE,
};

/* all process level attribute which expose to user */
typedef struct knl_instance_attr {
    knl_instance_attr_sql attr_sql;
    knl_instance_attr_storage attr_storage;
    knl_instance_attr_security attr_security;
    knl_instance_attr_network attr_network;
    knl_instance_attr_memory attr_memory;
    knl_instance_attr_resource attr_resource;
    knl_instance_attr_common attr_common;
} knl_instance_attr_t;

typedef struct knl_g_cache_context{
    MemoryContext global_cache_mem;
} knl_g_cache_context;

typedef struct knl_g_cost_context {
    double cpu_hash_cost;

    double send_kdata_cost;

    double receive_kdata_cost;

    Cost disable_cost;

    Cost disable_cost_enlarge_factor;
} knl_g_cost_context;

typedef struct knl_g_pid_context {
    ThreadId StartupPID;
    ThreadId TwoPhaseCleanerPID;
    ThreadId FaultMonitorPID;
    ThreadId BgWriterPID;
    ThreadId* CkptBgWriterPID;
    ThreadId* PageWriterPID;
    ThreadId CheckpointerPID;
    ThreadId WalWriterPID;
    ThreadId WalWriterAuxiliaryPID;
    ThreadId WalReceiverPID;
    ThreadId WalRcvWriterPID;
    ThreadId DataReceiverPID;
    ThreadId DataRcvWriterPID;
    ThreadId AutoVacPID;
    ThreadId PgJobSchdPID;
    ThreadId PgArchPID;
    ThreadId PgStatPID;
    ThreadId PercentilePID;
    ThreadId PgAuditPID;
    ThreadId SysLoggerPID;
    ThreadId CatchupPID;
    ThreadId WLMCollectPID;
    ThreadId WLMCalSpaceInfoPID;
    ThreadId WLMMonitorPID;
    ThreadId WLMArbiterPID;
    ThreadId CPMonitorPID;
    ThreadId AlarmCheckerPID;
    ThreadId CBMWriterPID;
    ThreadId RemoteServicePID;
    ThreadId AioCompleterStarted;
    ThreadId HeartbeatPID;
    volatile ThreadId ReaperBackendPID;
    volatile ThreadId SnapshotPID;
    ThreadId CommSenderFlowPID;
    ThreadId CommReceiverFlowPID;
    ThreadId CommAuxiliaryPID;
    ThreadId CommPoolerCleanPID;
    ThreadId* CommReceiverPIDS;
} knl_g_pid_context;

typedef struct knl_g_stat_context {
    /* Every  cell is a array size of 128 and the list stores all users` sql count results */
    struct List* WaitCountStatusList;

    /* Shared hashtable used to mapping user and g_instance.stat.WaitCountStatusList index for QPS */
    struct HTAB* WaitCountHashTbl;

    pgsocket pgStatSock;

    Latch pgStatLatch;

    time_t last_pgstat_start_time;

    /* Last time the collector successfully wrote the stats file */
    TimestampTz last_statwrite;

    /* Latest statistics request time from backends */
    TimestampTz last_statrequest;

    volatile bool need_exit;

    volatile bool got_SIGHUP;

    /* unique sql */
    MemoryContext UniqueSqlContext;
    HTAB* UniqueSQLHashtbl;

    /* hypothetical index */
    MemoryContext HypopgContext;

    /* user logon/logout stat */
    MemoryContext InstrUserContext;
    HTAB* InstrUserHTAB;

    /* workload trx stat */
    HTAB* workload_info_hashtbl;

    /* percentile stat */
    volatile bool calculate_on_other_cn;
    volatile bool force_process;
    int64 RTPERCENTILE[NUM_PERCENTILE_COUNT];
    struct SqlRTInfoArray* sql_rt_info_array;

    /* Set at the following cases:
     1. the cluster occures ha action
     2. user perform reset action
     3. the node startup
     4. ...
    */
    volatile TimestampTz NodeStatResetTime;
    int64* gInstanceTimeInfo;

    /* snapshot thread status counter, will +1 when "each" startup */
    volatile uint32 snapshot_thread_counter;
    /* Record the sum of file io stat */
    struct FileIOStat* fileIOStat;
} knl_g_stat_context;

/*
 * quota related
 */
typedef struct knl_g_quota_context {
    /* quota for each stream */
    unsigned long g_quota;
    /* quota ratio for dynamic quota changing, but no use now */
    unsigned long g_quotanofify_ratio;
    /* using quota or not */
    bool g_having_quota;
    /* for notifying bufCAP of stream is changed */
    struct binary_semaphore* g_quota_changing;
} knl_g_quota_context;

typedef enum {
    POLLING_CONNECTION = 0,

    SPECIFY_CONNECTION = 1,

    PROHIBIT_CONNECTION = 2
} CONN_MODE;

typedef struct knl_g_disconn_node_context_data {
    CONN_MODE conn_mode;
    char disable_conn_node_host[NAMEDATALEN];
    int disable_conn_node_port;
} knl_g_disconn_node_context_data;

typedef struct knl_g_disconn_node_context {
    knl_g_disconn_node_context_data disable_conn_node_data;
    slock_t info_lck; /* locks shared variables shown above */
} knl_g_disconn_node_context;

/*
 * local basic connection info
 */
typedef struct knl_g_localinfo_context {
    char* g_local_host;
    char* g_self_nodename;
    int g_local_ctrl_tcp_sock;
    int sock_to_server_loop;
    /* The start time of receiving the ready message */ 
    uint64 g_r_first_recv_time;
    /* record CPU utilization, gs_receivers_flow_controller ,gs_sender_flow_controller, gs_reveiver_loop, postmaster */
    int *g_libcomm_used_rate;
    /* GSS kerberos authentication */
    char* gs_krb_keyfile;
    volatile uint32 term = 1;
    bool set_term = true;
    knl_g_disconn_node_context disable_conn_node;
    volatile bool need_disable_connection_node = false;
} knl_g_localinfo_context;

typedef struct knl_g_counters_context {
    /* connection number, equal to real node number (dn + cn) */
    int g_cur_node_num;
    /*
     * expected node number, if g_expect_node_num != g_cur_node_num,
     * we will use online capacity change.
     */
    int g_expect_node_num;
    /* stream number of each logic connection */
    int g_max_stream_num;
    /* the number of receiver thread */
    int g_recv_num;
    int g_comm_send_timeout;
} knl_g_counters_context;

typedef struct knl_g_tests_context {
    int libcomm_test_current_thread;
    int* libcomm_test_thread_arg;
    bool libcomm_stop_flag;
    int libcomm_test_thread_num;
    int libcomm_test_msg_len;
    int libcomm_test_send_sleep;
    int libcomm_test_send_once;
    int libcomm_test_recv_sleep;
    int libcomm_test_recv_once;
} knl_g_tests_context;

/* the epoll cabinets for receiving control messages and data */
typedef struct knl_g_pollers_context {
    /* data receiver epoll cabinet */
    class mc_poller_hndl_list* g_libcomm_receiver_poller_list;
    pthread_mutex_t* g_r_libcomm_poller_list_lock;
    /* the epoll cabinet for receiving control messages at receiver */
    class mc_poller_hndl_list* g_r_poller_list;
    pthread_mutex_t* g_r_poller_list_lock;
    /* the epoll cabinet for receiving control messages at sender */
    class mc_poller_hndl_list* g_s_poller_list;
    pthread_mutex_t* g_s_poller_list_lock;
} knl_g_pollers_context;

/* request checking variable */
typedef struct knl_g_reqcheck_context {
    volatile bool g_shutdown_requested;
    volatile ThreadId g_cancel_requested;
    volatile bool g_close_poll_requested;
} knl_g_reqcheck_context;

typedef struct knl_g_mctcp_context {
    int mc_tcp_keepalive_idle;
    int mc_tcp_keepalive_interval;
    int mc_tcp_keepalive_count;
    int mc_tcp_connect_timeout;
    int mc_tcp_send_timeout;
} knl_g_mctcp_context;

typedef struct knl_g_commutil_context {
    volatile bool g_debug_mode;
    volatile bool g_timer_mode;
    volatile bool g_stat_mode;
    volatile bool g_no_delay;
    long g_total_usable_memory;
    long g_memory_pool_size;
    bool ut_libcomm_test;
} knl_g_commutil_context;

const int TWO_UINT64_SLOT = 2;
/* checkpoint */
typedef struct knl_g_ckpt_context {
    uint64 dirty_page_queue_reclsn;
    uint64 dirty_page_queue_tail;

    CkptSortItem* CkptBufferIds;

    /* dirty_page_queue store dirty buffer, buf_id + 1, 0 is invalid */
    DirtyPageQueueSlot* dirty_page_queue;
    uint64 dirty_page_queue_size;
    pg_atomic_uint64 dirty_page_queue_head;
    pg_atomic_uint32 actual_dirty_page_num;

    /* pagewriter thread */
    PageWriterProcs page_writer_procs;
    uint64 page_writer_actual_flush;
    volatile uint64 page_writer_last_flush;

    /* full checkpoint infomation */
    volatile bool flush_all_dirty_page;
    volatile uint64 full_ckpt_expected_flush_loc;
    volatile XLogRecPtr full_ckpt_redo_ptr;
    volatile uint32 current_page_writer_count;
    volatile XLogRecPtr page_writer_xlog_flush_loc;
    volatile LWLock *backend_wait_lock;

    /* Pagewriter thread need wait shutdown checkpoint finish */
    volatile bool page_writer_can_exit;
    volatile bool ckpt_need_fast_flush;

    /* checkpoint flush page num except buffers */
    int64 ckpt_clog_flush_num;
    int64 ckpt_csnlog_flush_num;
    int64 ckpt_multixact_flush_num;
    int64 ckpt_predicate_flush_num;
    int64 ckpt_twophase_flush_num;
    volatile XLogRecPtr ckpt_current_redo_point;

    struct CheckpointCallbackItem* ckptCallback;

    uint64 pad[TWO_UINT64_SLOT];
} knl_g_ckpt_context;

typedef struct knl_g_bgwriter_context {
    BgWriterProc *bgwriter_procs;
    int bgwriter_num;                   /* bgwriter thread num*/
    pg_atomic_uint32 curr_bgwriter_num; 
    int *candidate_buffers;
    bool *candidate_free_map;

    /* bgwriter view information*/
    volatile uint64 bgwriter_actual_total_flush;
    volatile uint64 get_buf_num_candidate_list;
    volatile uint64 get_buf_num_clock_sweep;
}knl_g_bgwriter_context;

typedef struct {
    ThreadId threadId;
    uint32 threadState;
} PageRedoWorkerStatus;

typedef struct {
    uint64 preEndPtr;
    instr_time preTime;
    XLogRecPtr redo_start_ptr;
    int64 redo_start_time;
    int64 redo_done_time;
    int64 current_time;
    XLogRecPtr min_recovery_point;
    XLogRecPtr read_ptr;
    XLogRecPtr last_replayed_read_ptr;
    XLogRecPtr recovery_done_ptr;
    XLogRecPtr primary_flush_ptr;
    RedoWaitInfo wait_info[WAIT_REDO_NUM];
    uint32 speed_according_seg;
    XLogRecPtr local_max_lsn;
} RedoPerf;


typedef enum{
    DEFAULT_REDO,
    PARALLEL_REDO,
    EXTREME_REDO,
}RedoType;


typedef struct knl_g_parallel_redo_context {
    RedoType redoType;
    volatile knl_parallel_redo_state state;
    slock_t rwlock; /* redo worker starting lock */
    MemoryContext parallelRedoCtx;
    uint32 totalNum;
    volatile PageRedoWorkerStatus pageRedoThreadStatusList[MAX_RECOVERY_THREAD_NUM];
    RedoPerf redoPf; /* redo Performance statistics */
    pg_atomic_uint32 isLocalRedoFinish;
    slock_t destroy_lock; /* redo worker destroy lock */
    pg_atomic_uint64 endRecPtr;
} knl_g_parallel_redo_context;

typedef struct knl_g_heartbeat_context {
    volatile bool heartbeat_running;
} knl_g_heartbeat_context;

typedef struct knl_g_comm_context {
    /* function point, for wake up consumer in executor */
    wakeup_hook_type gs_wakeup_consumer;
    bool g_comm_tcp_mode;
    /* connection handles at receivers, for listening and receiving data through logic connection */
    struct local_receivers* g_receivers;
    /* connection handles at senders, for sending data through logic connection */
    struct local_senders* g_senders;
    bool g_delay_survey_switch;
    /* the unix path to send startuppacket to postmaster */
    char* g_unix_path;
    HaShmemData* g_ha_shm_data;
    /*
     * filo queue, for keeping the usable stream indexes of each node
     * we pop up a stream index for communication and push back after
     * the stream is closed
     */
    struct mc_queue* g_usable_streamid;

    /*
     * sockets (control tcp & sctp sockets) of receiver and sender, for socket management and avaliable checking
     * the indexes of the items in the array is the node indexes for each datanode,
     * e.g. g_r_node_sock[1] is using to keep socket information of Datanode 1 at receiver in this datanode, maybe
     * Datanode 2 or others
     */
    struct node_sock* g_r_node_sock;
    struct node_sock* g_s_node_sock;
    struct libcomm_delay_info* g_delay_info;
    /*
     * two-dimension array to maintain the information of streams associated to Consumer and Producer, each element
     * represent a stream. the first dimension represent node and the second is stream. we call it mailbox, and the node
     * index and stream index is the address, e.g. we can get information of a stream at receiver by g_c_mailbox[node
     * id][stream id].
     */
    /* Consumer mailboxes at receiver */
    struct c_mailbox** g_c_mailbox;
    /* Producer mailboxes at sender */
    struct p_mailbox** g_p_mailbox;
    struct pg_tz* libcomm_log_timezone;

    bool g_use_workload_manager;

    knl_g_quota_context quota_cxt;
    knl_g_localinfo_context localinfo_cxt;
    knl_g_counters_context counters_cxt;
    knl_g_tests_context tests_cxt;
    knl_g_pollers_context pollers_cxt;
    knl_g_reqcheck_context reqcheck_cxt;
    knl_g_mctcp_context mctcp_cxt;
    knl_g_commutil_context commutil_cxt;
    knl_g_parallel_redo_context predo_cxt; /* parallel redo context */
    MemoryContext comm_global_mem_cxt;
    bool force_cal_space_info;
    bool cal_all_space_info_in_progress;

    HTAB* usedDnSpace;
    HTAB* account_table;
    LWLock* account_table_lock;
} knl_g_comm_context;

typedef struct knl_g_libpq_context {
    /* Workaround for Solaris 2.6 brokenness */
    char* pam_passwd;
    /* Workaround for passing "Port *port" into pam_passwd_conv_proc */
    struct Port* pam_port_cludge;
#ifdef KRB5
    /*
     * Various krb5 state which is not connection specific, and a flag to
     * indicate whether we have initialised it yet.
     */
    int pg_krb5_initialised;
    krb5_context pg_krb5_context;
    krb5_keytab pg_krb5_keytab;
    krb5_principal pg_krb5_server;
#endif
    /* check internal IP, to avoid port injection */
    List* comm_parsed_hba_lines;
} knl_g_libpq_context;

#define MIN_CONNECTIONS 1024

typedef struct knl_g_shmem_context {   
    int MaxConnections;
    int max_parallel_workers;
    int MaxBackends;
    int MaxReserveBackendId;
    int ThreadPoolGroupNum;
    int numaNodeNum;
} knl_g_shmem_context;

typedef struct knl_g_executor_context {
    HTAB* function_id_hashtbl;
} knl_g_executor_context;

typedef struct knl_g_xlog_context {
    int num_locks_in_group;
    struct RedoCommitCallbackItem* redoCommitCallback;
} knl_g_xlog_context;

struct NumaMemAllocInfo {
    void* numaAddr; /* Start address returned from numa_alloc_xxx */
    size_t length;
};

typedef struct knl_g_numa_context {
    bool inheritThreadPool; // Inherit NUMA node No. from thread pool module
    NumaMemAllocInfo* numaAllocInfos;
    size_t maxLength;
    size_t allocIndex;
} knl_g_numa_context;

typedef struct knl_g_bgworker_context {
    /* set when there's a worker that needs to be started up */
    volatile bool start_worker_needed;
    volatile bool have_crashed_worker;
} knl_g_bgworker_context;

typedef struct knl_g_mot_context {
    JitExec::JitExecMode jitExecMode;
} knl_g_mot_context;

typedef struct WalInsertStatusEntry WALInsertStatusEntry;
typedef struct WALFlushWaitLockPadded WALFlushWaitLockPadded;
typedef struct WALBufferInitWaitLockPadded WALBufferInitWaitLockPadded;
typedef struct WALInitSegLockPadded WALInitSegLockPadded;

typedef struct knl_g_wal_context {
    /* Start address of WAL insert status table that contains WAL_INSERT_STATUS_ENTRIES entries */
    WALInsertStatusEntry* walInsertStatusTable;
    WALFlushWaitLockPadded* walFlushWaitLock;
    WALBufferInitWaitLockPadded* walBufferInitWaitLock;
    WALInitSegLockPadded* walInitSegLock;
    volatile bool isWalWriterUp;
    XLogRecPtr  flushResult;
    XLogRecPtr  sentResult;
    pthread_mutex_t flushResultMutex;
    pthread_cond_t flushResultCV;
    int XLogFlusherCPU;
    volatile bool isWalWriterSleeping;
    pthread_mutex_t criticalEntryMutex;
    pthread_cond_t criticalEntryCV;
    volatile uint32 walWaitFlushCount; /* only for xlog statistics use */
    volatile XLogSegNo globalEndPosSegNo; /* Global variable for init xlog segment files. */
    int lastWalStatusEntryFlushed;
    volatile int lastLRCScanned;
} knl_g_wal_context;

typedef struct knl_instance_context {
    knl_virtual_role role;
    volatile int status;
    DemoteMode demotion;
    bool fatal_error;

    bool WalSegmentArchSucceed;

    /*
     * request to flush buffer log data
     * 1. syslogger thread will read this var. if it's true,
     *    flush all buffered log data and reset this flag.
     * 2. the other worker threads will set this flag be true if needed.
     * needn't use THR_LOCAL modifier.
     */
    volatile sig_atomic_t flush_buf_requested;

    /* True if WAL recovery failed */
    bool recover_error;

    struct Dllist* backend_list;
    struct Backend* backend_array;

    char* prog_name;
    int64 start_time;

    bool binaryupgrade;

    bool dummy_standby;

    struct PROC_HDR* proc_base;
    slock_t proc_base_lock;
    struct PGPROC** proc_base_all_procs;
    struct PGXACT* proc_base_all_xacts;
    struct PGPROC** proc_aux_base;
    struct PGPROC** proc_preparexact_base;
    struct ProcArrayStruct* proc_array_idx;
    struct GsSignalBase* signal_base;
    struct DistributeTestParam* distribute_test_param_instance;

    /* Set by the -o option */
    char ExtraOptions[MAXPGPATH];

    /* load ir file count for each session */
    long codegen_IRload_process_count;

    struct HTAB* vec_func_hash;

    MemoryContext instance_context;
    MemoryContext error_context;
    MemoryContext signal_context;
    MemoryContext increCheckPoint_context;
    MemoryContext wal_context;
    MemoryContext account_context;

    knl_instance_attr_t attr;
    knl_g_stat_context stat_cxt;
    knl_g_pid_context pid_cxt;
    knl_g_cache_context cache_cxt;
    knl_g_cost_context cost_cxt;
    knl_g_comm_context comm_cxt;
    knl_g_libpq_context libpq_cxt;
    struct knl_g_wlm_context* wlm_cxt;
    knl_g_ckpt_context ckpt_cxt;
    knl_g_ckpt_context* ckpt_cxt_ctl;
    knl_g_bgwriter_context bgwriter_cxt;
    struct knl_g_dw_context dw_cxt;
    knl_g_shmem_context shmem_cxt;
    knl_g_wal_context wal_cxt;
    knl_g_executor_context exec_cxt;
    knl_g_heartbeat_context heartbeat_cxt;
    knl_g_rto_context rto_cxt;
    knl_g_xlog_context xlog_cxt;
    knl_g_numa_context numa_cxt;
    knl_g_mot_context mot_cxt;
    knl_g_bgworker_context bgworker_cxt;
} knl_instance_context;

extern void knl_instance_init();
extern knl_instance_context g_instance;

extern void add_numa_alloc_info(void* numaAddr, size_t length);
extern void knl_g_set_is_local_redo_finish(bool status);
extern bool knl_g_get_is_local_redo();


#define GTM_FREE_MODE (g_instance.attr.attr_storage.enable_gtm_free || \
                        g_instance.attr.attr_storage.gtm_option == GTMOPTION_GTMFREE)
#define GTM_MODE (!g_instance.attr.attr_storage.enable_gtm_free && \
                    (g_instance.attr.attr_storage.gtm_option == GTMOPTION_GTM))
#define GTM_LITE_MODE (!g_instance.attr.attr_storage.enable_gtm_free && \
                        g_instance.attr.attr_storage.gtm_option == GTMOPTION_GTMLITE)

#endif /* SRC_INCLUDE_KNL_KNL_INSTANCE_H_ */

