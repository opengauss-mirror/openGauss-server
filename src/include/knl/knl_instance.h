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
#include "lib/circularqueue.h"
#include "nodes/pg_list.h"
#include "storage/lock/s_lock.h"
#include "access/double_write_basic.h"
#include "utils/knl_globalsysdbcache.h"
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
#include "streaming/init.h"
#include "replication/rto_statistic.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/utils/constant_def.h"
#endif
#include "streaming/init.h"
#include "storage/lock/lwlock.h"
#include "utils/memgroup.h"

#include "replication/walprotocol.h"
#ifdef ENABLE_MOT
#include "storage/mot/jit_def.h"
#endif
#include "postmaster/barrier_creator.h"
#include "pgxc/barrier.h"

const int NUM_PERCENTILE_COUNT = 2;
const int INIT_NUMA_ALLOC_COUNT = 32;
const int HOTKEY_ABANDON_LENGTH = 100;
const int MAX_GLOBAL_CACHEMEM_NUM = 128;
const int MAX_GLOBAL_PRC_NUM = 32;
const int MAX_AUDIT_NUM = 48;

const uint32 PARALLEL_DECODE_WORKER_INVALID = 0;
const uint32 PARALLEL_DECODE_WORKER_START = 1;
const uint32 PARALLEL_DECODE_WORKER_READY = 2;
const uint32 PARALLEL_DECODE_WORKER_EXIT = 3;
/* Maximum number of max parallel decode threads */
#define MAX_PARALLEL_DECODE_NUM 20

/* Maximum number of max replication slots */
#define MAX_REPLICATION_SLOT_NUM 100

#ifndef ENABLE_MULTIPLE_NODES
const int DB_CMPT_MAX = 4;
#endif

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

typedef struct knl_g_cache_context
{
    MemoryContext global_cache_mem;
    MemoryContext global_plancache_mem[MAX_GLOBAL_CACHEMEM_NUM];
    MemoryContext global_prc_mem[MAX_GLOBAL_PRC_NUM];
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
    ThreadId SpBgWriterPID;
    ThreadId* PageWriterPID;
    ThreadId PageRepairPID;
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
    ThreadId *PgAuditPID;
    ThreadId SysLoggerPID;
    ThreadId CatchupPID;
    ThreadId WLMCollectPID;
    ThreadId WLMCalSpaceInfoPID;
    ThreadId WLMMonitorPID;
    ThreadId WLMArbiterPID;
    ThreadId CPMonitorPID;
    ThreadId AlarmCheckerPID;
    ThreadId CBMWriterPID;
    ThreadId AioCompleterStarted;
    ThreadId HeartbeatPID;
    ThreadId CsnminSyncPID;
    ThreadId BarrierCreatorPID;
    volatile ThreadId ReaperBackendPID;
    ThreadId TxnSnapCapturerPID;
    volatile ThreadId RbCleanrPID;
    volatile ThreadId SnapshotPID;
    ThreadId AshPID;
    ThreadId StatementPID;
    ThreadId CommSenderFlowPID;
    ThreadId CommReceiverFlowPID;
    ThreadId CommAuxiliaryPID;
    ThreadId CommPoolerCleanPID;
    ThreadId UndoLauncherPID;
    ThreadId GlobalStatsPID;
    ThreadId* CommReceiverPIDS;
    ThreadId UndoRecyclerPID;
    ThreadId TsCompactionPID;
    ThreadId TsCompactionAuxiliaryPID;
    ThreadId sharedStorageXlogCopyThreadPID;
    ThreadId LogicalReadWorkerPID;
    ThreadId LogicalDecoderWorkerPID;
    ThreadId BarrierPreParsePID;
    ThreadId ApplyLauncerPID;
} knl_g_pid_context;

typedef struct {
    int lockId;
    MemoryContext context;
    HTAB* hashTbl;
} GWCHashCtl;

typedef struct knl_g_advisor_conntext {
    uint32 isOnlineRunning;
    /* This flag is used to avoid early clean. */
    uint32 isUsingGWC;
    int maxMemory;
    int maxsqlCount;
    Oid currentUser;
    Oid currentDB;
    GWCHashCtl* GWCArray;

    MemoryContext SQLAdvisorContext;
} knl_g_advisor_conntext;

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

    volatile bool got_SIGHUP;

    /* unique sql */
    MemoryContext UniqueSqlContext;
    HTAB* volatile UniqueSQLHashtbl;

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
    MemoryContext InstrPercentileContext;

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

    /* Active session history */
    MemoryContext AshContext;
    struct ActiveSessHistArrary *active_sess_hist_arrary;
    char *ash_appname;
    char *ash_clienthostname;
    bool instr_stmt_is_cleaning;
    char *ashRelname;
    HTAB* ASHUniqueSQLHashtbl;
    /* used for capture view to json file */
    HTAB* capture_view_file_hashtbl;
    /* dynamic function control manager */
    HTAB* stmt_track_control_hashtbl;

    /* used for track memory alloc info */
    MemoryContext track_context;
    HTAB* track_context_hash;
    HTAB* track_memory_info_hash;
    bool track_memory_inited;
    struct UHeapPruneStat* tableStat;
#ifndef WIN32
    pthread_rwlock_t track_memory_lock;

    /* A g_instance lock-free queue to collect keys */
    CircularQueue* hotkeysCollectList;

    /* memorycontext for managing hotkeys */
    MemoryContext hotkeysCxt;

    struct LRUCache* lru;
    List* fifo;
#endif
} knl_g_stat_context;

/*
 *quota related
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
 *local basic connection info
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
#ifndef WIN32
    volatile uint32 term_from_file = 1;
    volatile uint32 term_from_xlog = 1;
    bool set_term = true;
    knl_g_disconn_node_context disable_conn_node;
    volatile uint32 is_finish_redo = 0;
    volatile uint32 need_disable_connection_node = false;
#else
    volatile uint32 term_from_file;
    volatile uint32 term_from_xlog;
    bool set_term;
    knl_g_disconn_node_context disable_conn_node;
    volatile uint32 is_finish_redo;
    volatile uint32 need_disable_connection_node;
#endif
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

    int g_comm_sender_buffer_size;
    long g_total_usable_memory;
    long g_memory_pool_size;
    bool ut_libcomm_test;
} knl_g_commutil_context;

typedef struct ckpt_view_struct {
    int64 ckpt_clog_flush_num;
    int64 ckpt_csnlog_flush_num;
    int64 ckpt_multixact_flush_num;
    int64 ckpt_predicate_flush_num;
    int64 ckpt_twophase_flush_num;
    volatile XLogRecPtr ckpt_current_redo_point;
} ckpt_view_struct;

const int TWO_UINT64_SLOT = 2;

typedef struct knl_g_audit_context {
    MemoryContext global_audit_context;
    /* audit pipes */
    int *sys_audit_pipes;

    /* audit logs */
    char index_file_path[MAXPGPATH];
    LWLock *index_file_lock;
    struct AuditIndexTableNew* audit_indextbl;
    struct AuditIndexTable* audit_indextbl_old;
    uint64 pgaudit_totalspace;

    /* audit thread */
    volatile uint32 current_audit_index;
    volatile int thread_num;
    volatile uint32 audit_coru_fnum[MAX_AUDIT_NUM];
} knl_g_audit_context;

/* incremental checkpoint */
typedef struct knl_g_ckpt_context {
    uint64 dirty_page_queue_reclsn;
    uint64 dirty_page_queue_tail;

    /* dirty pages applied to flush */
    volatile uint32 prepared;
    pg_atomic_uint32 CkptBufferIdsTail;
    pg_atomic_uint32 CkptBufferIdsCompletedPages;
    volatile uint32 CkptBufferIdsFlushPages;
    CkptSortItem* CkptBufferIds;

    /* dirty_page_queue store dirty buffer, buf_id + 1, 0 is invalid */
    DirtyPageQueueSlot* dirty_page_queue;
    uint64 dirty_page_queue_size;
    pg_atomic_uint64 dirty_page_queue_head;
    pg_atomic_uint32 actual_dirty_page_num;
    slock_t queue_lock;
    struct LWLock* prune_queue_lock;

    /* pagewriter thread */
    volatile uint32 current_page_writer_count;
    PageWriterProcs pgwr_procs;
    volatile uint32 page_writer_last_flush;
    volatile uint32 page_writer_last_queue_flush;
    Buffer *candidate_buffers;
    bool *candidate_free_map;

    volatile LWLock *backend_wait_lock;

    /* full checkpoint infomation */
    volatile bool flush_all_dirty_page;
    volatile uint64 full_ckpt_expected_flush_loc;
    volatile uint64 full_ckpt_redo_ptr;

    /* Pagewriter thread need wait shutdown checkpoint finish */
    volatile bool page_writer_can_exit;

    /* pagewriter thread view information */
    uint64 page_writer_actual_flush;
    volatile uint64 get_buf_num_candidate_list;
    volatile uint64 seg_get_buf_num_candidate_list;
    volatile uint64 get_buf_num_clock_sweep;
    volatile uint64 seg_get_buf_num_clock_sweep;

    /* checkpoint view information */
    ckpt_view_struct ckpt_view;

    RecoveryQueueState ckpt_redo_state;

#ifdef ENABLE_MOT
    struct CheckpointCallbackItem* ckptCallback;
#endif

    struct IncreCkptSyncShmemStruct* incre_ckpt_sync_shmem;

    uint64 pad[TWO_UINT64_SLOT];
} knl_g_ckpt_context;

typedef struct recovery_dw_buf {
    char* unaligned_buf;
    char* buf;
    dw_file_head_t* file_head;
    volatile uint16 write_pos;
    bool *single_flush_state;
} recovery_dw_buf;

typedef struct dw_batch_file_context{
    int id;

    volatile int fd;
    uint16 flush_page;          /* total number of flushed pages before truncate or reset */

    char* unaligned_buf;
    char* buf;

    struct LWLock* flush_lock;
    dw_file_head_t* file_head;
    volatile uint32 write_pos;  /* the copied pages in buffer, updated when mark page */
    uint64 file_size;

    dw_stat_info_batch batch_stat_info;

    char file_name[PATH_MAX];
}dw_batch_file_context;

typedef struct knl_g_dw_context {
    volatile int fd;
    volatile uint32 closed;

    volatile bool old_batch_version;
    volatile int recovery_dw_file_num;
    volatile int recovery_dw_file_size;

    dw_batch_meta_file batch_meta_file;
    dw_batch_file_context *batch_file_cxts;

    MemoryContext mem_cxt;

    volatile uint32 dw_version;

    uint16 flush_page;

    char* unaligned_buf;
    char* buf;

    struct LWLock* flush_lock;
    dw_file_head_t* file_head;
    volatile uint32 write_pos;

    /* second version flush, only single flush */
    struct LWLock* second_flush_lock;  /* single flush: second version pos lock */
    struct LWLock* second_buftag_lock;  /* single flush: second version bufferTag page lock */
    dw_file_head_t* second_file_head;
    volatile uint32 second_write_pos;

    bool is_new_relfilenode; /* RelFileNode or RelFileNodeOld */

    /* single flush dw extras information */
    bool *single_flush_state; /* dw single flush slot state */

    /* dw recovery buf */
    recovery_dw_buf recovery_buf;     /* recovery the c20 version dw file */

    /* dw view information */
    dw_stat_info_single single_stat_info;
} knl_g_dw_context;

typedef struct knl_g_bgwriter_context {
    HTAB *unlink_rel_hashtbl;
    LWLock *rel_hashtbl_lock;
    HTAB *unlink_rel_fork_hashtbl;     /* delete one fork relation file */
    LWLock *rel_one_fork_hashtbl_lock;
    Latch *invalid_buf_proc_latch;
}knl_g_bgwriter_context;

typedef struct knl_g_repair_context {
    ThreadId startup_tid;
    Latch *repair_proc_latch;
    HTAB *page_repair_hashtbl;          /* standby store page repair info */
    LWLock *page_repair_hashtbl_lock;
    HTAB *file_repair_hashtbl;          /* standby store file repair info */
    LWLock *file_repair_hashtbl_lock;
    HTAB *global_repair_bad_block_stat; /* local_bad_block_info use this */
    bool support_repair;
} knl_g_repair_context;

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
    XLogRecPtr last_replayed_end_ptr;
    XLogRecPtr recovery_done_ptr;
    XLogRecPtr primary_flush_ptr;
    RedoWaitInfo wait_info[WAIT_REDO_NUM];
    uint32 speed_according_seg;
    XLogRecPtr local_max_lsn;
    uint64    oldest_segment;
} RedoPerf;


typedef enum{
    DEFAULT_REDO,
    PARALLEL_REDO,
    EXTREME_REDO,
}RedoType;

extern struct ReorderBufferChange* change;

enum knl_parallel_decode_state {
    DECODE_INIT = 0,
    DECODE_STARTING_BEGIN,
    DECODE_STARTING_END,
    DECODE_IN_PROGRESS,
    DECODE_DONE,
};

typedef struct {
    ThreadId threadId;
    uint32 threadState;
} ParallelDecodeWorkerStatus;

typedef struct {
    ThreadId threadId;
    uint32 threadState;
} ParallelReaderWorkerStatusTye;

typedef struct knl_g_parallel_decode_context {
    MemoryContext parallelDecodeCtx;
    int state;
    slock_t rwlock;
    slock_t destroy_lock; /* redo worker destroy lock */
    char* dbUser;
    char* dbName;
    int totalNum;
    volatile ParallelDecodeWorkerStatus ParallelDecodeWorkerStatusList[MAX_PARALLEL_DECODE_NUM];
    volatile ParallelReaderWorkerStatusTye ParallelReaderWorkerStatus;
    gs_thread_t tid[MAX_PARALLEL_DECODE_NUM];
    Oid relationOid;
} knl_g_parallel_decode_context;

typedef struct knl_g_parallel_redo_context {
    RedoType redoType;
    volatile knl_parallel_redo_state state;
    slock_t rwlock; /* redo worker starting lock */
    MemoryContext parallelRedoCtx;
    uint32 totalNum;
    volatile PageRedoWorkerStatus pageRedoThreadStatusList[MAX_RECOVERY_THREAD_NUM];
    RedoPerf redoPf; /* redo Performance statistics */
    pg_atomic_uint32 isRedoFinish;
    int pre_enable_switch;
    slock_t destroy_lock; /* redo worker destroy lock */
    pg_atomic_uint64 max_page_flush_lsn[NUM_MAX_PAGE_FLUSH_LSN_PARTITIONS];
    pg_atomic_uint64 last_replayed_conflict_csn;
    pg_atomic_uint32 permitFinishRedo;
    pg_atomic_uint32 hotStdby;
    volatile XLogRecPtr newestCheckpointLoc;
    volatile CheckPoint newestCheckpoint;
    char* unali_buf; /* unaligned_buf */
    char* ali_buf;
    XLogRedoNumStatics xlogStatics[RM_NEXT_ID][MAX_XLOG_INFO_NUM];
    RedoCpuBindControl redoCpuBindcontrl;
} knl_g_parallel_redo_context;

typedef struct knl_g_heartbeat_context {
    volatile bool heartbeat_running;
} knl_g_heartbeat_context;

typedef struct knl_g_csnminsync_context {
    bool stop;
    bool is_fcn_ccn; /* whether this instance is center cn or the first cn while without ccn */
} knl_g_csnminsync_context;

typedef struct knl_g_barrier_creator_context {
    bool stop;
    bool is_barrier_creator;
} knl_g_barrier_creator_context;

typedef struct knl_g_csn_barrier_context {
    struct HTAB* barrier_hash_table;
    LWLock* barrier_hashtbl_lock;
    char stopBarrierId[MAX_BARRIER_ID_LENGTH];
    MemoryContext barrier_context;
} knl_g_csn_barrier_context;

typedef struct knl_g_comm_context {
    /* function point, for wake up consumer in executor */
    wakeup_hook_type gs_wakeup_consumer;
    /* connection handles at receivers, for listening and receiving data through logic connection */
    struct local_receivers* g_receivers;
    /* connection handles at senders, for sending data through logic connection */
    struct local_senders* g_senders;
    bool g_delay_survey_switch;
    uint64 g_delay_survey_start_time;
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
    knl_g_parallel_decode_context pdecode_cxt[MAX_REPLICATION_SLOT_NUM];
    knl_g_reqcheck_context reqcheck_cxt;
    knl_g_mctcp_context mctcp_cxt;
    knl_g_commutil_context commutil_cxt;
    knl_g_parallel_redo_context predo_cxt; /* parallel redo context */
    MemoryContext comm_global_mem_cxt;
    bool force_cal_space_info;
    bool cal_all_space_info_in_progress;

    HTAB* usedDnSpace;
    uint32 current_gsrewind_count;
    bool request_disaster_cluster;
    bool isNeedChangeRole;
    long lastArchiveRcvTime;

#ifdef USE_SSL
    libcomm_sslinfo* libcomm_data_port_list;
    libcomm_sslinfo* libcomm_ctrl_port_list;
#endif
} knl_g_comm_context;

class FdCollection;
typedef struct knl_g_comm_logic_context {
    MemoryContext comm_logic_mem_cxt;
    FdCollection *comm_fd_collection;
} knl_g_comm_logic_context;

/* g_instance struct for "Pooler" */
typedef struct knl_g_pooler_context {
    /* Record global connection status for function 'comm_check_connection_status' */
    struct GlobalConnStatus* globalConnStatus;
} knl_g_pooler_context;

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
    /* check internal IP, to avoid port injection*/
    List* comm_parsed_hba_lines;
    MemoryContext parsed_hba_context;
} knl_g_libpq_context;

#define MIN_CONNECTIONS 1024

typedef struct knl_g_shmem_context {
    int MaxConnections;
    int MaxBackends;
    int MaxReserveBackendId;
    int ThreadPoolGroupNum;
    int numaNodeNum;
} knl_g_shmem_context;

typedef struct knl_g_executor_context {
    HTAB* function_id_hashtbl;
#ifndef ENABLE_MULTIPLE_NODES
    char* nodeName;
    char* global_application_name;
#endif
} knl_g_executor_context;

typedef struct knl_g_xlog_context {
    int num_locks_in_group;
#ifdef ENABLE_MOT
    struct RedoCommitCallbackItem* redoCommitCallback;
#endif
    pthread_mutex_t remain_segs_lock;
    ShareStorageXLogCtl *shareStorageXLogCtl;
    void *shareStorageXLogCtlOrigin;
    ShareStorageOperateCtl shareStorageopCtl;
    int shareStorageLockFd;
} knl_g_xlog_context;

typedef struct knl_g_undo_context {
    void                    **uZones;
    uint32                   undoTotalSize;
    uint32                   undoMetaSize;
    volatile uint32          uZoneCount;
    Bitmapset               *uZoneBitmap[UNDO_PERSISTENCE_LEVELS];
    MemoryContext            undoContext;
    int64                    undoChainTotalSize;
    int64                    maxChainSize;
    uint32                   undo_chain_visited_count;
    uint32                   undoCountThreshold;
    TransactionId            oldestFrozenXid;
    /* Oldest transaction id which is having undo. */
    pg_atomic_uint64         oldestXidInUndo;
} knl_g_undo_context;

typedef struct knl_g_flashback_context {
    TransactionId oldestXminInFlashback;
} knl_g_flashback_context;

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

typedef struct knl_g_ts_compaction_context {
#ifdef ENABLE_MULTIPLE_NODES
    volatile Compaction::knl_compaction_state state;
    volatile TsCompactionWorkerStatus compaction_worker_status_list[Compaction::MAX_TSCOMPATION_NUM];
#endif
    int totalNum;
    int* origin_id;
    volatile uint32 drop_db_count;
    volatile bool compaction_rest;
    Oid dropdb_id;
    MemoryContext compaction_instance_cxt;
} knl_g_ts_compaction_context;

typedef struct knl_g_security_policy_context {
    MemoryContext policy_instance_cxt;
    HTAB* account_table;
    LWLock* account_table_lock;
} knl_g_security_policy_context;

typedef uint64 XLogSegNo; /* XLogSegNo - physical log file sequence number */
typedef struct WalInsertStatusEntry WALInsertStatusEntry;
typedef struct WALFlushWaitLockPadded WALFlushWaitLockPadded;
typedef struct WALBufferInitWaitLockPadded WALBufferInitWaitLockPadded;
typedef struct WALInitSegLockPadded WALInitSegLockPadded;
typedef struct XlogFlushStats XlogFlushStatistics;
typedef struct knl_g_conn_context {
    volatile int CurConnCount;
    volatile int CurCMAConnCount;  /* Connection count of cm_agent after initialize, using for connection limit */
    volatile uint32 CurCMAProcCount;  /* Proc count of cm_agent connections, indicate proc unsing condition */
    slock_t ConnCountLock;
} knl_g_conn_context;

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
    pthread_condattr_t criticalEntryAtt;
    volatile uint32 walWaitFlushCount; /* only for xlog statistics use */
    volatile XLogSegNo globalEndPosSegNo; /* Global variable for init xlog segment files. */
    int lastWalStatusEntryFlushed;
    volatile int lastLRCScanned;
    volatile int lastLRCFlushed;
    int num_locks_in_group;
    DemoteMode upgradeSwitchMode;
    uint64 totalXlogIterBytes;
    uint64 totalXlogIterTimes;
    XlogFlushStatistics* xlogFlushStats;
} knl_g_wal_context;

typedef struct GlobalSeqInfoHashBucket {
    DList* shb_list;
    int lock_id;
} GlobalSeqInfoHashBucket;

typedef struct knl_g_archive_context {
    XLogRecPtr barrierLsn;
    char barrierName[MAX_BARRIER_ID_LENGTH];
    slock_t mutex;
    volatile int archive_slot_num;
    volatile int archive_recovery_slot_num;
    int slot_tline;
    struct ArchiveSlotConfig *archive_config;
    struct ArchiveTaskStatus *archive_status;
    struct ArchiveBarrierLsnInfo *barrier_lsn_info;
    slock_t barrier_lock;
    int max_node_cnt;
    int chosen_walsender_index;
    bool in_switchover;
    bool in_service_truncate;
} knl_g_archive_context;

#ifdef ENABLE_MOT
typedef struct knl_g_mot_context {
    JitExec::JitExecMode jitExecMode;
} knl_g_mot_context;
#endif

typedef struct knl_g_hypo_context {
    MemoryContext HypopgContext;
    List* hypo_index_list;
} knl_g_hypo_context;

typedef struct knl_g_segment_context {
    uint32 segment_drop_timeline;
} knl_g_segment_context;

typedef struct knl_g_roleid_context {
    struct HTAB* roleid_table;
} knl_g_roleid_context;

#define PG_MAX_DEBUG_CONN 100
/* PlDebuggerComm should be reseted by server session */
typedef struct PlDebuggerComm {
    volatile bool hasUsed; /* only change when get PldebugLock */
    pthread_cond_t cond; /* wake up when all valid msg saved into buffer */
    pthread_mutex_t mutex;
    /* check flag for cond wake up */
    volatile bool hasServerFlushed; /* true mean still has valid msg in buffer, false means no valid msg */
    volatile bool hasClientFlushed; /* true mean still has valid msg in buffer, false means no valid msg */
    /* is waiting for cond wake up in loop */
    volatile bool IsClientWaited;
    volatile bool IsServerWaited;
    /* set true when server ereport, wake up client to ereport
       reset when client complete handled error */
    volatile bool hasServerErrorOccured;
    /* set true when client ereport, wake up server to ereport
       reset when server complete handled error */
    volatile bool hasClientErrorOccured;
    /* true during debug procedure is running */
    volatile bool isProcdeureRunning;
    volatile uint32 lock; /* 1 means locked, 0 means unlock */
    volatile uint64 serverId;
    volatile uint64 clientId;
    int bufLen; /* length of valid buffer msg */
    int bufSize; /* memory size of buffer */
    char* buffer;
    inline bool Used()
    {
        pg_memory_barrier();
        return hasUsed;
    }
    inline bool hasClient()
    {
        pg_memory_barrier();
        return hasUsed && clientId != 0;
    }
    inline bool isRunning()
    {
        pg_memory_barrier();
        return isProcdeureRunning;
    }
} PlDebuggerComm;

typedef struct knl_g_pldebug_context {
    MemoryContext PldebuggerCxt; /* global memcxt for pldebugger */
    PlDebuggerComm debug_comm[PG_MAX_DEBUG_CONN]; /* length is PG_MAX_DEBUG_CONN, use with lwlock HypoIndexLock */
} knl_g_pldebug_context;

typedef struct _SPI_plan* SPIPlanPtr;

typedef struct {
    char* ident;
    int nplans;
    SPIPlanPtr* splan;
} EPlanRefInt;

typedef struct knl_g_spi_plan_context {
    MemoryContext global_spi_plan_context;
    EPlanRefInt* FPlans;
    int nFPlans;
    EPlanRefInt* PPlans;
    int nPPlans;
} knl_g_spi_plan_context;

typedef struct knl_g_archive_thread_info {
    ThreadId* obsArchPID;
    ThreadId* obsBarrierArchPID;
    char** slotName;
} knl_g_archive_thread_info;

typedef struct knl_g_roach_context {
    bool isRoachRestore;
    char* targetTimeInPITR;
    char* globalBarrierRecordForPITR;
    bool isXLogForceRecycled;
    bool forceAdvanceSlotTigger;
    bool isGtmFreeCsn;
    char* targetRestoreTimeFromMedia;
} knl_g_roach_context;

/* Added for streaming disaster recovery */
typedef struct knl_g_streaming_dr_context {
    bool isInStreaming_dr;
    bool isInSwitchover;
    bool isInteractionCompleted;
    XLogRecPtr switchoverBarrierLsn;
    int64 rpoSleepTime;
    int64 rpoBalanceSleepTime;
    char currentBarrierId[MAX_BARRIER_ID_LENGTH];
    char targetBarrierId[MAX_BARRIER_ID_LENGTH];
    slock_t mutex; /* locks shared variables shown above */
} knl_g_streaming_dr_context;

typedef struct knl_g_startup_context {
    uint32 remoteReadPageNum;
    HTAB *badPageHashTbl;
    char page[BLCKSZ];
    XLogReaderState *current_record;
    volatile uint32 BadFileNum;
    ThreadId startup_tid;
    XLogRecPtr suspend_lsn;
}knl_g_startup_context;

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
    pthread_mutex_t gpc_reset_lock;
    struct Dllist* backend_list;
    struct Backend* backend_array;

    void* t_thrd;
    char* prog_name;
    int64 start_time;

    bool binaryupgrade;

    bool dummy_standby;

    struct GlobalSeqInfoHashBucket global_seq[NUM_GS_PARTITIONS];

    struct GlobalPlanCache*   plan_cache;

    void* bgw_base;
    pthread_mutex_t bgw_base_lock;
    struct PROC_HDR* proc_base;
    pthread_mutex_t proc_base_lock;
    struct PGPROC** proc_base_all_procs;
    struct PGXACT* proc_base_all_xacts;
    struct PGPROC** proc_aux_base;
    struct PGPROC** proc_preparexact_base;
    struct ProcArrayStruct* proc_array_idx;
    struct HTAB*            ProcXactTable;
    struct GsSignalBase* signal_base;
    struct DistributeTestParam* distribute_test_param_instance;
    struct SegmentTestParam* segment_test_param_instance;
    struct WhiteboxTestParam* whitebox_test_param_instance;

    /* Set by the -o option */
    char ExtraOptions[MAXPGPATH];

    /* load ir file count for each session */
    long codegen_IRload_process_count;
    uint64 global_session_seq;

    struct HTAB* vec_func_hash;

    MemoryContext instance_context;
    MemoryContext error_context;
    MemoryContext signal_context;
    MemoryContext increCheckPoint_context;
    MemoryContext wal_context;
    MemoryContext account_context;
    MemoryContext builtin_proc_context;
    MemoryContextGroup* mcxt_group;

    knl_g_advisor_conntext adv_cxt;
    knl_instance_attr_t attr;
    knl_g_stat_context stat_cxt;
    knl_g_pid_context pid_cxt;
    knl_g_cache_context cache_cxt;
    knl_g_cost_context cost_cxt;
    knl_g_comm_context comm_cxt;
    knl_g_comm_logic_context comm_logic_cxt;
    knl_g_pooler_context pooler_cxt;
    knl_g_conn_context conn_cxt;
    knl_g_libpq_context libpq_cxt;
    struct knl_g_wlm_context* wlm_cxt;
    knl_g_ckpt_context ckpt_cxt;
    knl_g_ckpt_context* ckpt_cxt_ctl;
    knl_g_bgwriter_context bgwriter_cxt;
    knl_g_repair_context repair_cxt;
    struct knl_g_dw_context dw_batch_cxt;
    struct knl_g_dw_context dw_single_cxt;
    knl_g_shmem_context shmem_cxt;
    knl_g_wal_context wal_cxt;
    knl_g_executor_context exec_cxt;
    knl_g_heartbeat_context heartbeat_cxt;
    knl_g_rto_context rto_cxt;
    knl_g_xlog_context xlog_cxt;
    knl_g_numa_context numa_cxt;

    knl_g_undo_context undo_cxt;
    knl_g_flashback_context flashback_cxt;

#ifdef ENABLE_MOT
    knl_g_mot_context mot_cxt;
#endif

    knl_g_ts_compaction_context ts_compaction_cxt;
    knl_g_security_policy_context policy_cxt;
    knl_g_streaming_context streaming_cxt;
    knl_g_csnminsync_context csnminsync_cxt;
    knl_g_barrier_creator_context barrier_creator_cxt;
    knl_g_csn_barrier_context csn_barrier_cxt;
    GlobalSysDBCache global_sysdbcache;
    knl_g_archive_context archive_obs_cxt;
    knl_g_archive_thread_info archive_thread_info;
    struct HTAB* ngroup_hash_table;
    struct HTAB* mmapCache;
    knl_g_hypo_context hypo_cxt;

    knl_g_segment_context segment_cxt;
    knl_g_pldebug_context pldebug_cxt;
    knl_g_spi_plan_context spi_plan_cxt;
    knl_g_roleid_context roleid_cxt;
    knl_g_roach_context roach_cxt;
    knl_g_streaming_dr_context streaming_dr_cxt;
    struct PLGlobalPackageRuntimeCache* global_session_pkg;
    knl_g_startup_context startup_cxt;

#ifndef ENABLE_MULTIPLE_NODES
    void *raw_parser_hook[DB_CMPT_MAX];
    void *plsql_parser_hook[DB_CMPT_MAX];
#endif
    pg_atomic_uint32 extensionNum;
    knl_g_audit_context audit_cxt;
} knl_instance_context;

extern long random();
extern void knl_instance_init();
extern void knl_g_set_redo_finish_status(uint32 status);
extern void knl_g_clear_local_redo_finish_status();
extern bool knl_g_get_local_redo_finish_status();
extern bool knl_g_get_redo_finish_status();
extern void knl_g_cachemem_create();
extern knl_instance_context g_instance;

extern void add_numa_alloc_info(void* numaAddr, size_t length);

#define GTM_FREE_MODE (g_instance.attr.attr_storage.enable_gtm_free || \
                        g_instance.attr.attr_storage.gtm_option == GTMOPTION_GTMFREE)
#define GTM_MODE (false)
#define GTM_LITE_MODE (!(GTM_FREE_MODE))

#define REDO_FINISH_STATUS_LOCAL		0x00000001
#define REDO_FINISH_STATUS_CM	    	0x00000002

#define ATOMIC_TRUE                     1
#define ATOMIC_FALSE                    0

#define GLOBAL_PLANCACHE_MEMCONTEXT \
    (g_instance.cache_cxt.global_plancache_mem[u_sess->session_id % MAX_GLOBAL_CACHEMEM_NUM])

#define GLOBAL_PRC_MEMCONTEXT \
    (g_instance.cache_cxt.global_prc_mem[u_sess->session_id % MAX_GLOBAL_PRC_NUM])

#define DISABLE_MULTI_NODES_GPI (u_sess->attr.attr_storage.default_index_kind == DEFAULT_INDEX_KIND_NONE)
#define DEFAULT_CREATE_LOCAL_INDEX (u_sess->attr.attr_storage.default_index_kind == DEFAULT_INDEX_KIND_LOCAL)
#define DEFAULT_CREATE_GLOBAL_INDEX (u_sess->attr.attr_storage.default_index_kind == DEFAULT_INDEX_KIND_GLOBAL)

#endif /* SRC_INCLUDE_KNL_KNL_INSTANCE_H_ */

