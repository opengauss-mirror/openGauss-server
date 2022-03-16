/* -------------------------------------------------------------------------
 *
 * postmaster.cpp
 *	  This program acts as a clearing house for requests to the
 *	  openGauss system.	Frontend programs send a startup message
 *	  to the Postmaster and the postmaster uses the info in the
 *	  message to setup a backend process.
 *
 *	  The postmaster also manages system-wide operations such as
 *	  startup and shutdown. The postmaster itself doesn't do those
 *	  operations, mind you --- it just forks off a subprocess to do them
 *	  at the right times.  It also takes care of resetting the system
 *	  if a backend crashes.
 *
 *	  The postmaster process creates the shared memory and semaphore
 *	  pools during startup, but as a rule does not touch them itself.
 *	  In particular, it is not a member of the PGPROC array of backends
 *	  and so it cannot participate in lock-manager operations.	Keeping
 *	  the postmaster away from shared memory operations makes it simpler
 *	  and more reliable.  The postmaster is almost always able to recover
 *	  from crashes of individual backends by resetting shared memory;
 *	  if it did much with shared memory then it would be prone to crashing
 *	  along with the backends.
 *
 *	  When a request message is received, we now fork() immediately.
 *	  The child process performs authentication of the request, and
 *	  then becomes a backend if successful.  This allows the auth code
 *	  to be written in a simple single-threaded style (as opposed to the
 *	  crufty "poor man's multitasking" code that used to be needed).
 *	  More importantly, it ensures that blockages in non-multithreaded
 *	  libraries like SSL or PAM cannot cause denial of service to other
 *	  clients.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/postmaster.cpp
 *
 * NOTES
 *
 * Initialization:
 *		The Postmaster sets up shared memory data structures
 *		for the backends.
 *
 * Synchronization:
 *		The Postmaster shares memory with the backends but should avoid
 *		touching shared memory, so as not to become stuck if a crashing
 *		backend screws up locks or shared memory.  Likewise, the Postmaster
 *		should never block on messages from frontend clients.
 *
 * Garbage Collection:
 *		The Postmaster cleans up after backends if they have an emergency
 *		exit and/or core dump.
 *
 * Error Reporting:
 *		Use write_stderr() only for reporting "interactive" errors
 *		(essentially, bogus arguments on the command line).  Once the
 *		postmaster is launched, use ereport().
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "gs_bbox.h"
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <openssl/rand.h>
#include <sys/param.h>
#include <arpa/inet.h>
#include <sys/resource.h>

#ifdef HAVE_POLL_H
#include <poll.h>
#endif

#include "access/cbmparsexlog.h"
#include "access/obs/obs_am.h"
#include "access/transam.h"
#include "access/ustore/undo/knl_uundoapi.h"
#include "access/xlog.h"
#include "access/xact.h"
#include "bootstrap/bootstrap.h"
#include "commands/matview.h"
#include "commands/verify.h"
#include "catalog/pg_control.h"
#include "dbmind/hypopg_index.h"
#include "instruments/instr_unique_sql.h"
#include "instruments/instr_user.h"
#include "instruments/percentile.h"
#include "instruments/ash.h"
#include "instruments/capture_view.h"
#include "opfusion/opfusion_util.h"
#include "instruments/instr_slow_query.h"
#include "instruments/instr_statement.h"
#include "instruments/instr_func_control.h"

#include "lib/dllist.h"
#include "libpq/auth.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#ifdef PGXC
#include "pgxc/csnminsync.h"
#include "pgxc/pgxc.h"
/* COORD */
#include "pgxc/locator.h"
#include "nodes/nodes.h"
#include "nodes/memnodes.h"
#include "pgxc/poolmgr.h"
#include "access/gtm.h"
#endif
#include "pgstat.h"
#include "instruments/snapshot.h"
#include "pgaudit.h"
#include "job/job_scheduler.h"
#include "job/job_worker.h"
#include "postmaster/autovacuum.h"
#include "postmaster/pagewriter.h"
#include "postmaster/fork_process.h"
#include "postmaster/pgarch.h"
#include "postmaster/postmaster.h"
#include "postmaster/syslogger.h"
#include "postmaster/alarmchecker.h"
#include "postmaster/snapcapturer.h"
#include "postmaster/rbcleaner.h"
#include "postmaster/aiocompleter.h"
#include "postmaster/fencedudf.h"
#include "postmaster/barrier_creator.h"
#include "postmaster/bgworker.h"
#include "replication/heartbeat.h"
#include "replication/catchup.h"
#include "replication/dataqueue.h"
#include "replication/datasender.h"
#include "replication/datasender_private.h"
#include "replication/datareceiver.h"
#include "replication/replicainternal.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"
#include "replication/walreceiver.h"
#include "replication/dcf_replication.h"
#include "replication/logicallauncher.h"
#include "replication/logicalworker.h"
#include "postmaster/bgwriter.h"
#include "postmaster/cbmwriter.h"
#include "postmaster/startup.h"
#include "postmaster/twophasecleaner.h"
#include "postmaster/licensechecker.h"
#include "postmaster/walwriter.h"
#include "postmaster/walwriterauxiliary.h"
#include "postmaster/lwlockmonitor.h"
#include "postmaster/barrier_preparse.h"
#include "replication/walreceiver.h"
#include "replication/datareceiver.h"
#include "replication/logical.h"
#include "replication/slot.h"
#include "storage/smgr/fd.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/lock/pg_sema.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/remote_read.h"
#include "storage/procarray.h"
#include "storage/xlog_share_storage/xlog_share_storage.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/plog.h"
#include "utils/zfiles.h"
#include "utils/inval.h"
#ifdef PGXC
#include "utils/resowner.h"
#include "workload/cpwlm.h"
#include "workload/workload.h"
#endif

#ifdef EXEC_BACKEND
#include "storage/spin.h"
#endif

#include "access/dfs/dfs_insert.h"
#include "access/twophase.h"
#include "access/ustore/knl_undoworker.h"
#include "alarm/alarm.h"
#include "auditfuncs.h"
#include "catalog/pg_type.h"
#include "common/config/cm_config.h"
#include "distributelayer/streamMain.h"
#include "executor/exec/execStream.h"
#include "funcapi.h"
#include "gs_thread.h"
#include "gssignal/gs_signal.h"
#include "libcomm/libcomm.h"
#include "libpq/pqformat.h"
#include "postmaster/startup.h"
#include "storage/spin.h"
#include "threadpool/threadpool.h"
#include "utils/guc.h"
#include "utils/guc_storage.h"
#include "utils/guc_tables.h"
#include "utils/mmpool.h"
#include "libcomm/libcomm.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/memprot.h"
#include "pgstat.h"

#include "distributelayer/streamMain.h"
#include "distributelayer/streamProducer.h"
#ifndef ENABLE_LITE_MODE
#include "eSDKOBS.h"
#endif
#include "cjson/cJSON.h"

#include "tcop/stmt_retry.h"
#include "gaussdb_version.h"
#include "hotpatch/hotpatch.h"
#include "hotpatch/hotpatch_backend.h"
// Parallel recovery
#include "access/parallel_recovery/page_redo.h"
#include "access/multi_redo_api.h"
#include "postmaster/postmaster.h"
#include "access/parallel_recovery/dispatcher.h"
#include "utils/distribute_test.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/compaction/compaction_entry.h"
#include "tsdb/compaction/compaction_worker_entry.h"
#include "tsdb/compaction/session_control.h"
#include "tsdb/compaction/compaction_auxiliary_entry.h"
#include "tsdb/cache/tags_cachemgr.h"
#include "tsdb/cache/part_cachemgr.h"
#include "tsdb/cache/partid_cachemgr.h"
#include "tsdb/storage/part.h"
#endif   /* ENABLE_MULTIPLE_NODES */

#include "streaming/launcher.h"
#include "streaming/init.h"
#ifdef ENABLE_MOT
#include "storage/mot/mot_fdw.h"
#endif
#include "executor/node/nodeExtensible.h"
#include "tde_key_management/tde_key_storage.h"
#include "tde_key_management/ckms_message.h"

#include "gs_ledger/blockchain.h"
#include "communication/commproxy_interface.h"

#ifdef ENABLE_UT
#define static
#endif

extern void InitGlobalSeq();
extern void auto_explain_init(void);
extern int S3_init();
static const int RECOVERY_PARALLELISM_DEFAULT = 1;

/* flag to get logic cluster name for dn alarm */
static bool isNeedGetLCName = true;
/* logic cluster name list file name */
#define LOGIC_CLUSTER_LIST "logic_cluster_name.txt"
/* judge whether a char is digital */
#define isDigital(_ch) (((_ch) >= '0') && ((_ch) <= '9'))

#define IS_FD_TO_RECV_GSSOCK(fd) \
    ((fd) == t_thrd.postmaster_cxt.sock_for_libcomm || (fd) == t_thrd.libpq_cxt.listen_fd_for_recv_flow_ctrl)

/* These two are only here before of the SSL multithread initialization of OpenSSL component */
#include "ssl/gs_openssl_client.h"

#define MAXLISTEN 64

#define PM_BUSY_ALARM_USED_US 30000000L
#define PM_BUSY_ALARM_US 1000000L
#define PM_POLL_TIMEOUT_SECOND 20
#define PM_POLL_TIMEOUT_MINUTE 58*SECS_PER_MINUTE*60*1000000L
#define CHECK_TIMES 10

static char gaussdb_state_file[MAXPGPATH] = {0};
static char AddMemberFile[MAXPGPATH] = {0};
static char RemoveMemberFile[MAXPGPATH] = {0};
static char TimeoutFile[MAXPGPATH] = {0};
static char SwitchoverStatusFile[MAXPGPATH] = {0};
static char ChangeRoleFile[MAXPGPATH] = {0};
static char StartMinorityFile[MAXPGPATH] = {0};
static char SetRunmodeStatusFile[MAXPGPATH] = {0};
static char g_changeRoleStatusFile[MAXPGPATH] = {0};

uint32 noProcLogicTid = 0;

volatile int Shutdown = NoShutdown;

extern void gs_set_hs_shm_data(HaShmemData* ha_shm_data);
extern void ReaperBackendMain();
extern void AdjustThreadAffinity();

#define EXTERN_SLOTS_NUM 17
volatile PMState pmState = PM_INIT;
bool dummyStandbyMode = false;
volatile uint64 sync_system_identifier = 0;
bool FencedUDFMasterMode = false;
bool PythonFencedMasterModel = false;

extern char* optarg;
extern int optind, opterr;

#ifdef HAVE_INT_OPTRESET
extern int optreset; /* might not be declared by system headers */
#endif

#ifdef USE_BONJOUR
static DNSServiceRef bonjour_sdref = NULL;
#endif

/* for backtrace function */
pthread_mutex_t bt_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_rwlock_t hba_rwlock = PTHREAD_RWLOCK_INITIALIZER;

extern bool data_catchup;
extern bool wal_catchup;

char g_bbox_dump_path[1024] = {0};

#define CHECK_FOR_PROCDIEPENDING()                                                                          \
    do {                                                                                                    \
        if (t_thrd.int_cxt.ProcDiePending) {                                                                \
            if (t_thrd.storage_cxt.cancel_from_timeout) {                                                   \
                ereport(FATAL,                                                                              \
                    (errcode(ERRCODE_QUERY_CANCELED),                                                       \
                        errmsg("terminate because pooler connect timeout(%ds) when process startup packet", \
                            u_sess->attr.attr_network.PoolerConnectTimeout)));                              \
            } else {                                                                                        \
                proc_exit(1);                                                                               \
            }                                                                                               \
        }                                                                                                   \
    } while (0)

#define DataRcvIsOnline()                                                                                         \
    ((IS_DN_DUMMY_STANDYS_MODE() ? (g_instance.pid_cxt.DataReceiverPID != 0 && t_thrd.datareceiver_cxt.DataRcv && \
                                       t_thrd.datareceiver_cxt.DataRcv->isRuning)                                 \
                                 : true))

#define IsCascadeStandby()                                            \
    (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE && \
    t_thrd.postmaster_cxt.HaShmData->is_cascade_standby)

/*
 * postmaster.c - function prototypes
 */
static void CloseServerPorts(int status, Datum arg);
static void getInstallationPaths(const char* argv0);
static void checkDataDir(void);
static void CheckGUCConflicts(void);
static Port* ConnCreateToRecvGssock(pollfd* ufds, int idx, int* nSockets);
static Port* ConnCreate(int serverFd);
static void reset_shared(int port);
static void SIGHUP_handler(SIGNAL_ARGS);
static void pmdie(SIGNAL_ARGS);
static void startup_alarm(SIGNAL_ARGS);
static void SetWalsndsNodeState(ClusterNodeState requester, ClusterNodeState others);
static void ProcessDemoteRequest(void);
static void reaper(SIGNAL_ARGS);
static void sigusr1_handler(SIGNAL_ARGS);
static void dummy_handler(SIGNAL_ARGS);
static void CleanupBackend(ThreadId pid, int exitstatus);
static const char* GetProcName(ThreadId pid);
static void LogChildExit(int lev, const char* procname, ThreadId pid, int exitstatus);
static void PostmasterStateMachineReadOnly(void);
static void PostmasterStateMachine(void);
static void BackendInitialize(Port* port);
static int BackendRun(Port* port);
static int ServerLoop(void);
static int BackendStartup(Port* port, bool isConnectHaPort);
static void processCancelRequest(Port* port, void* pkt);
static void processStopRequest(Port* port, void* pkt);
#ifndef HAVE_POLL
static int initMasks(fd_set* rmask);
#endif
static int initPollfd(struct pollfd* ufds);
static void report_fork_failure_to_client(Port* port, int errnum, const char* specialErrorInfo = NULL);
void signal_child(ThreadId pid, int signal, int be_mode = -1);
static bool SignalSomeChildren(int signal, int targets);

static bool IsChannelAdapt(Port* port, ReplConnInfo* repl);
static bool IsLocalPort(Port* port);
static bool IsLocalIp(const char* address);
static bool IsInplicitIp(const char* address);
static int NeedPoolerPort(const char* hostName);
static void SetHaShmemData(void);
static bool IsAlreadyListen(const char* ip, int port);
static void IntArrayRegulation(int array[], int len, int def);
static void ListenSocketRegulation(void);
static bool ParseHaListenAddr(LISTEN_ADDRS* pListenList, ReplConnInfo** replConnArray);
static void CreateHaListenSocket(void);
static void RemoteHostInitilize(Port* port);
static int StartupPacketInitialize(Port* port);
static void PsDisplayInitialize(Port* port);
static void SetListenSocket(ReplConnInfo **replConnArray, bool *listen_addr_saved);
static void UpdateArchiveSlotStatus();

static ServerMode get_cur_mode(void);
static int get_cur_repl_num(void);

static void PMInitDBStateFile();
static void PMReadDBStateFile(GaussState* state);
static void PMSetDBStateFile(GaussState* state);
static void PMUpdateDBState(DbState db_state, ServerMode mode, int conn_num);
static void PMUpdateDBStateLSN(void);

static void PMUpdateDBStateHaRebuildReason(void);

#define SignalChildren(sig) SignalSomeChildren(sig, BACKEND_TYPE_ALL)
static void StartPgjobWorker(void);
static void StartPoolCleaner(void);
static void StartCleanStatement(void);

static void check_and_reset_ha_listen_port(void);
static void* cJSON_internal_malloc(size_t size);
static bool NeedHeartbeat();

bool PMstateIsRun(void);

/*
 * Possible types of a backend. These are OR-able request flag bits
 * for SignalSomeChildren() and CountChildren().
 */
#define BACKEND_TYPE_NORMAL 0x0001  /* normal backend */
#define BACKEND_TYPE_AUTOVAC 0x0002 /* autovacuum worker process */
#define BACKEND_TYPE_WALSND 0x0004  /* walsender process */
#define BACKEND_TYPE_DATASND 0x0008 /* datasender process */
#define BACKEND_TYPE_TEMPBACKEND                                        \
    0x0010                      /* temp thread processing cancel signal \
                                   or stream connection */
#define BACKEND_TYPE_ALL 0x001F /* OR of all the above */

#define GTM_LITE_CN (GTM_LITE_MODE && IS_PGXC_COORDINATOR)

#ifdef ENABLE_MULTIPLE_NODES
#define START_BARRIER_CREATOR (IS_PGXC_COORDINATOR && !IS_DISASTER_RECOVER_MODE)
#else
#define START_BARRIER_CREATOR IS_PGXC_DATANODE
#endif

static int CountChildren(int target);
static bool CreateOptsFile(int argc, const char* argv[], const char* fullprogname);
static void UpdateOptsFile(void);
static void StartRbWorker(void);
static void StartTxnSnapWorker(void);
static void StartAutovacuumWorker(void);
static void StartUndoWorker(void);
static void StartApplyWorker(void);
static ThreadId StartCatchupWorker(void);
static void InitPostmasterDeathWatchHandle(void);
static void NotifyShutdown(void);
static void NotifyProcessActive(void);
static int init_stream_comm(void);
#ifndef ENABLE_MULTIPLE_NODES
static void handle_change_role_signal(char *role);
static bool CheckSignalByFile(const char *filename, void *infoPtr, size_t infoSize);
#endif

int GaussDbThreadMain(knl_thread_arg* arg);
const char* GetThreadName(knl_thread_role role);

#ifdef EXEC_BACKEND

typedef int InheritableSocket;
typedef struct LWLock LWLock; /* ugly kluge */

/*
 * Structure contains all variables passed to exec:ed backends
 */
typedef struct {
    Port port;
    pgsocket portsocket;
    char DataDir[MAXPGPATH];
    pgsocket ListenSocket[MAXLISTEN];
    long MyCancelKey;
    int MyPMChildSlot;
#ifndef WIN32
    unsigned long UsedShmemSegID;
#else
    HANDLE UsedShmemSegID;
#endif
    void* UsedShmemSegAddr;
    slock_t* ShmemLock;
    VariableCache ShmemVariableCache;
    LWLock* mainLWLockArray;
    PMSignalData* PMSignalState;

    char LocalAddrList[MAXLISTEN][IP_LEN];
    int LocalIpNum;
    HaShmemData* HaShmData;

    TimestampTz PgStartTime;
    TimestampTz PgReloadTime;
    pg_time_t first_syslogger_file_time;
    bool redirection_done;
    bool IsBinaryUpgrade;
    int max_safe_fds;
    int max_files_per_process;
    int max_userdatafiles;
    int postmaster_alive_fds[2];
    int syslogPipe[2];
    char my_exec_path[MAXPGPATH];
    char pkglib_path[MAXPGPATH];
    Oid myTempNamespace;
    Oid myTempToastNamespace;
    bool comm_ipc_log;
} BackendParameters;

static BackendParameters backend_save_para;

BackendParameters *BackendVariablesGlobal = NULL;

static void read_backend_variables(char* id, Port* port);
static void restore_backend_variables(BackendParameters* param, Port* port);
#ifndef WIN32
static bool save_backend_variables(BackendParameters* param, Port* port);
#else
static bool save_backend_variables(BackendParameters* param, Port* port, HANDLE childProcess, pid_t childPid);
#endif
#endif /* EXEC_BACKEND */

static void BackendArrayAllocation(void);
static void BackendArrayRemove(Backend* bn);

PMStateInfo pmStateDescription[] = {{PM_INIT, "PM_INIT"},
    {PM_STARTUP, "PM_STARTUP"},
    {PM_RECOVERY, "PM_RECOVERY"},
    {PM_HOT_STANDBY, "PM_HOT_STANDBY"},
    {PM_RUN, "PM_RUN"},
    {PM_WAIT_BACKUP, "PM_WAIT_BACKUP"},
    {PM_WAIT_READONLY, "PM_WAIT_READONLY"},
    {PM_WAIT_BACKENDS, "PM_WAIT_BACKENDS"},
    {PM_SHUTDOWN, "PM_SHUTDOWN"},
    {PM_SHUTDOWN_2, "PM_SHUTDOWN_2"},
    {PM_WAIT_DEAD_END, "PM_WAIT_DEAD_END"},
    {PM_NO_CHILDREN, "PM_NO_CHILDREN"}};

/* convert PM state to string */
const char* GetPMState(const PMState pmStateCode)
{
    uint32_t count;
    for (count = 0; count < lengthof(pmStateDescription); count++) {
        if (pmStateCode == pmStateDescription[count].pmState) {
            return pmStateDescription[count].pmStateMsg;
        }
    }
    return "UNKOWN";
}

/*
 * While adding a new node to the cluster we need to restore the schema of
 * an existing database to the new node.
 * If the new node is a datanode and we connect directly to it,
 * it does not allow DDL, because it is in read only mode &
 * If the new node is a coordinator it will send DDLs to all the other
 * coordinators which we do not want it to do
 * To provide ability to restore on the new node a new command line
 * argument is provided called --restoremode
 * It is to be provided in place of --coordinator OR --datanode.
 * In restore mode both coordinator and datanode are internally
 * treated as a datanode.
 */
bool isRestoreMode = false;

/*
 *The securitymode is to satisfy the security request of database on
 *cloud environment. In securitymode, some operations are carried out
 *to satisfy the security request.
 */
bool isSecurityMode = false;

#define SECURITY_MODE_NAME "securitymode"

#define SECURITY_MODE_NAME_LEN 12

bool isSingleMode = false;

#ifdef ENABLE_MULTIPLE_NODES
#define StartPoolManager() PoolManagerInit()
#endif

/* Macros to check exit status of a child process */
#define EXIT_STATUS_0(st) ((st) == 0)
#define EXIT_STATUS_1(st) (1 == (st))

/*
 * @hdfs
 * deleteHdfsUser() function is used to clean Hdfs User List and
 * release alocated memory
 */
extern void deleteHdfsUser();

extern void CodeGenProcessInitialize();
extern void CodeGenProcessTearDown();

extern void CPmonitorMain(void);

extern void load_searchserver_library();

/*
 * Protocol versions we supported:
 * For x.y, if y < 50, it's from the postgres's front end.
 * And for now, we only support versions: 2.0, 3.0, 3.50
 * Attention:
 * 		For protocol version x.y, x and y are integers eachly, and x.y is not a float.
 * 		Which means that 3.50 != 3.5, 3.5 == 3.05
 *
 * Version desc:
 *		2.0 and 3.0 are from postgres.
 *		3.50: backend does not send signature to frontend anymore while authorization.
 *		3.51: backend send iteration to frontend in the authenication.
 */
const unsigned short protoVersionList[][2] = {{2, 0}, {3, 0}, {3, 50}, {3, 51}};
/* transparent encryption database encryption key. */
extern bool getAndCheckTranEncryptDEK();

void SetServerMode(ServerMode mode)
{
    t_thrd.xlog_cxt.server_mode = mode;
}

ServerMode GetServerMode()
{
    return (ServerMode)t_thrd.xlog_cxt.server_mode;
}

void ReportAlarmAbnormalDataHAInstListeningSocket()
{
    Alarm alarmItem[1];
    AlarmAdditionalParam tempAdditionalParam;

    // Initialize the alarm item
    AlarmItemInitialize(alarmItem, ALM_AI_AbnormalDataHAInstListeningSocket, ALM_AS_Reported, NULL);
    // fill the alarm message
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
        g_instance.attr.attr_common.PGXCNodeName,
        "",
        "",
        alarmItem,
        ALM_AT_Fault,
        g_instance.attr.attr_common.PGXCNodeName);
    // report the alarm
    AlarmReporter(alarmItem, ALM_AT_Fault, &tempAdditionalParam);
}

void ReportResumeAbnormalDataHAInstListeningSocket()
{
    Alarm alarmItem[1];
    AlarmAdditionalParam tempAdditionalParam;

    // Initialize the alarm item
    AlarmItemInitialize(alarmItem, ALM_AI_AbnormalDataHAInstListeningSocket, ALM_AS_Normal, NULL);
    // fill the alarm message
    WriteAlarmAdditionalInfo(
        &tempAdditionalParam, g_instance.attr.attr_common.PGXCNodeName, "", "", alarmItem, ALM_AT_Resume);
    // report the alarm
    AlarmReporter(alarmItem, ALM_AT_Resume, &tempAdditionalParam);
}

void SetFlagForGetLCName(bool falg)
{
    isNeedGetLCName = falg;
}

uint32 GetNodeId(const char* nodeIdStr)
{
    if (nodeIdStr == NULL) {
        return 0;
    }
    int index = 0;
    while (nodeIdStr[index] != '\0') {
        if (!isDigital(nodeIdStr[index])) {
            return 0;
        }
        index++;
    }
    uint32 nodeId = (uint32)atoi(nodeIdStr);
    return nodeId;
}

/*
 * "Safe" wrapper around strdup()
 */
char* pg_strdup(const char* string)
{
    char* tmp = NULL;

    if (NULL == string) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("pg_strdup: cannot duplicate null pointer (internal error)\n")));
    }
    tmp = strdup(string);
    if (NULL == tmp) {
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("out of memory\n")));
    }
    return tmp;
}

/* Init the hash table to save the debug query id and the used space */
static void InitDnHashTable(void)
{
    HASHCTL hctl;
    errno_t rc = 0;

    rc = memset_s(&hctl, sizeof(HASHCTL), 0, sizeof(HASHCTL));
    securec_check(rc, "", "");

    hctl.keysize = sizeof(uint64);
    hctl.entrysize = sizeof(DnUsedSpaceHashEntry);
    hctl.hash = tag_hash;
    hctl.hcxt = g_instance.instance_context;

    g_instance.comm_cxt.usedDnSpace = hash_create("SQL used space on datanode",
        g_instance.attr.attr_network.MaxConnections,
        &hctl,
        HASH_ELEM | HASH_FUNCTION | HASH_SHRCTX);
}

/*
 *Get master node id from node name
 */
uint32 GetMasterIdByNodeName()
{
    uint32 node_id = 0;
    char* nodeName = pg_strdup(g_instance.attr.attr_common.PGXCNodeName);
    const char delims[] = "_";
    char* ptr = NULL;
    char* outer_ptr = NULL;
    ptr = strtok_r(nodeName, delims, &outer_ptr);
    if (ptr != NULL) {
        ptr = strtok_r(NULL, delims, &outer_ptr);
    }

    if (ptr != NULL) {
        node_id = GetNodeId(ptr);
    }

    if (nodeName != NULL) {
        free(nodeName);
        nodeName = NULL;
    }
    return node_id;
}

/*
 *Get Logic Cluster From LC StaticConfig file for given node
 *
 *return true if success else return false
 */
bool GetLogicClusterFromConfig(const char* filepath, char* lcname)
{
    g_logicClusterName = lcname;
    g_datanodeid = GetMasterIdByNodeName();
    if (g_datanodeid == 0) {
        ereport(LOG, (errmsg("cannot get node id of datanode %s\n", g_instance.attr.attr_common.PGXCNodeName)));
        return false;
    }

    int status = 0;
    int err_no = 0;
    status = read_logic_cluster_config_files(filepath, &err_no);
    switch (status) {
        case OPEN_FILE_ERROR:
        case READ_FILE_ERROR:
            ereport(LOG, (errmsg("could not open or read logic cluster static config files: %s", gs_strerror(err_no))));
            return false;
        case OUT_OF_MEMORY:
            ereport(LOG, (errmsg("out of memory")));
            return false;
        default:
            break;
    }

    if (g_logicClusterName[0] == '\0') {
        int ret = snprintf_s(g_logicClusterName, CLUSTER_NAME_LEN, CLUSTER_NAME_LEN - 1, "elastic_group");
        securec_check_ss_c(ret, "\0", "\0");
    }
    return true;
}

/*
 *Get logic cluster name for datanode alarm
 *
 *if exist logic_cluster_name.txt get logic cluster from LC static config
 *else reset logic cluster name
 */
void GetLogicClusterForAlarm(char* lcname)
{
    if (!IS_PGXC_DATANODE || !isNeedGetLCName)
        return;

    int ret;
    char LCListfile[MAXPGPATH] = {'\0'};
    char* gausshome = getGaussHome();
    if (gausshome == NULL) {
        return;
    }
    if (*gausshome == '\0') {
        pfree(gausshome);
        return;
    }

    ret = snprintf_s(LCListfile, MAXPGPATH, MAXPGPATH - 1, "%s/bin/%s", gausshome, LOGIC_CLUSTER_LIST);
    securec_check_ss_c(ret, "\0", "\0");
    pfree(gausshome);
    gausshome = NULL;

    if (0 == access(LCListfile, F_OK)) {
        /* if false we need reset the logic cluster name */
        if (!GetLogicClusterFromConfig(LCListfile, lcname))
            lcname[0] = '\0';
        else
            SetFlagForGetLCName(false);
    } else
        lcname[0] = '\0';
}

/*
 * get_coredump_pattern_path - get the core dump path from the file "/proc/sys/kernel/core_pattern"
 */
void get_coredump_pattern_path(char* path, Size len)
{
    FILE* fp = NULL;
    char* p = NULL;
    struct stat stat_buf;

    if (NULL == (fp = fopen("/proc/sys/kernel/core_pattern", "r"))) {
        write_stderr("cannot open file: /proc/sys/kernel/core_pattern.\n");
        return;
    }

    if (NULL == fgets(path, len, fp)) {
        fclose(fp);
        write_stderr("failed to get the core pattern path.\n ");
        return;
    }
    fclose(fp);

    if ((p = strrchr(path, '/')) == NULL) {
        *path = '\0';
    } else {
        *(++p) = '\0';
    }

    if (0 != stat(path, &stat_buf) || !S_ISDIR(stat_buf.st_mode) || 0 != access(path, W_OK)) {
        write_stderr("The core dump path is an invalid directory\n");
        *path = '\0';
        return;
    }
}

/*
 * Only update gaussdb.state file's state field.
 *
 * PARAMETERS:
 *      state: INPUT new state
 * RETURN:
 *      true if success, otherwise false.
 *
 * NOTE: unsafe function is not expected here since it is referred in signal handler.
 */
bool SetDBStateFileState(DbState state, bool optional)
{
    /* do nothing while core dump be appeared so early. */
    if (strlen(gaussdb_state_file) > 0) {
        char temppath[MAXPGPATH] = {0};
        GaussState s;
        int len = 0;

        /* zero it in case gaussdb.state doesn't exist. */
        int rc = memset_s(&s, sizeof(GaussState), 0, sizeof(GaussState));
        securec_check_c(rc, "\0", "\0");

        rc = snprintf_s(temppath, MAXPGPATH, MAXPGPATH - 1, "%s.temp", gaussdb_state_file);
        securec_check_intval(rc, , false);

        /* Write the new content into a temp file and rename it at last. */
        int fd = open(gaussdb_state_file, O_RDONLY);
        if (fd == -1) {
            if (errno == ENOENT && optional) {
                write_stderr("gaussdb.state does not exist, and skipt setting since it is optional.");
                return true;
            } else {
                write_stderr("Failed to open gaussdb.state.temp: %d", errno);
                return false;
            }
        }

        /* Read old content from file. */
        len = read(fd, &s, sizeof(GaussState));
        /* sizeof(int) is for current_connect_idx of GaussState */
        if ((len != sizeof(GaussState)) && (len != sizeof(GaussState) - sizeof(int))) {
            write_stderr("Failed to read gaussdb.state: %d", errno);
            (void)close(fd);
            return false;
        }

        if (close(fd) != 0) {
            write_stderr("Failed to close gaussdb.state: %d", errno);
            return false;
        }

        /* replace state with the new value. */
        s.state = state;

        fd = open(temppath, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
        if (fd == -1) {
            write_stderr("Failed to open gaussdb.state.temp: %d", errno);
            return false;
        }

        len = write(fd, &s, sizeof(GaussState));
        if (len != sizeof(GaussState)) {
            write_stderr("Failed to write gaussdb.state.temp: %d", errno);
            (void)close(fd);
            return false;
        }

        if (close(fd) != 0) {
            write_stderr("Failed to close gaussdb.state.temp: %d", errno);
            return false;
        }

        if (rename(temppath, gaussdb_state_file) != 0) {
            write_stderr("Failed to rename gaussdb.state.temp: %s", gs_strerror(errno));
            return false;
        }
    }

    return true;
}

void gs_hotpatch_log_callback(int level, char* logstr)
{
    ereport(level, (errmsg("%s", logstr)));
}

void signal_sysloger_flush(void)
{
    if (t_thrd.postmaster_cxt.redirection_done == false) {
        fflush(stdout);
        fflush(stderr);
    } else if (g_instance.pid_cxt.SysLoggerPID != 0) {
        set_flag_to_flush_buffer();
        signal_child(g_instance.pid_cxt.SysLoggerPID, SIGUSR1);
        pg_usleep(100000);
    }
}

void SetShmemCxt(void)
{
    int thread_pool_worker_num = 0;
    int thread_pool_stream_proc_num = 0;

    if (g_threadPoolControler != NULL) {
        thread_pool_worker_num = g_threadPoolControler->GetThreadNum();
        g_instance.shmem_cxt.ThreadPoolGroupNum = g_threadPoolControler->GetGroupNum();
        thread_pool_stream_proc_num = GetThreadPoolStreamProcNum();
    } else {
        g_instance.shmem_cxt.ThreadPoolGroupNum = 0;
    }

    /* Keep enough slot for thread pool. */
    g_instance.shmem_cxt.MaxConnections = 
                Max((g_instance.attr.attr_network.MaxConnections + g_instance.attr.attr_network.maxInnerToolConnections), thread_pool_worker_num);

    g_instance.shmem_cxt.MaxBackends = g_instance.shmem_cxt.MaxConnections +
                                       g_instance.attr.attr_sql.job_queue_processes +
                                       g_instance.attr.attr_storage.autovacuum_max_workers +
                                       g_instance.attr.attr_storage.max_undo_workers + 1 +
                                       AUXILIARY_BACKENDS +
                                       AV_LAUNCHER_PROCS + 
                                       g_max_worker_processes +
                                       thread_pool_stream_proc_num;

#ifndef ENABLE_LITE_MODE
    g_instance.shmem_cxt.MaxReserveBackendId = g_instance.attr.attr_sql.job_queue_processes +
                                               g_instance.attr.attr_storage.autovacuum_max_workers +
                                               g_instance.attr.attr_storage.max_undo_workers + 1 +
                                               (thread_pool_worker_num * STREAM_RESERVE_PROC_TIMES) +
                                               AUXILIARY_BACKENDS +
                                               AV_LAUNCHER_PROCS +
                                               g_max_worker_processes;
#else
    g_instance.shmem_cxt.MaxReserveBackendId = g_instance.attr.attr_sql.job_queue_processes +
                                               g_instance.attr.attr_storage.autovacuum_max_workers +
                                               g_instance.attr.attr_storage.max_undo_workers + 1 +
                                               thread_pool_worker_num +
                                               AUXILIARY_BACKENDS +
                                               AV_LAUNCHER_PROCS +
                                               g_max_worker_processes;
#endif
    Assert(g_instance.shmem_cxt.MaxBackends <= MAX_BACKENDS);
}

static void print_port_info()
{
    FILE* fp = NULL;
    StringInfoData strinfo;
    initStringInfo(&strinfo);
    char buf[MAXPGPATH];

    appendStringInfo(&strinfo, "lsof -i:%d", g_instance.attr.attr_network.PostPortNumber);
    fp = popen(strinfo.data, "r");
    if (fp == NULL) {
        ereport(LOG, (errmsg("Unable to use 'lsof' to read port info.")));
    } else {
        ereport(LOG, (errmsg("exec cmd: %s", strinfo.data)));
        while (fgets(buf, sizeof(buf), fp) != NULL) {
            ereport(LOG, (errmsg("<lsof>:%s", buf)));
        }
        pclose(fp);
    }

    resetStringInfo(&strinfo);
    appendStringInfo(&strinfo, "netstat -anp | grep %d", g_instance.attr.attr_network.PostPortNumber);
    fp = popen(strinfo.data, "r");
    if (fp == NULL) {
        ereport(LOG, (errmsg("Unable to use 'netstat' to read port info.")));
    } else {
        ereport(LOG, (errmsg("exec cmd: %s", strinfo.data)));
        while (fgets(buf, sizeof(buf), fp) != NULL) {
            ereport(LOG, (errmsg("<netstat>:%s", buf)));
        }
        pclose(fp);
    }

    pfree_ext(strinfo.data);

    return;
}

static void SetListenSocket(ReplConnInfo **replConnArray, bool *listen_addr_saved)
{
    int i = 0;
    int success = 0;
    int status = STATUS_OK;

    for (i = 1; i < MAX_REPLNODE_NUM; i++) {
        if (replConnArray[i] != NULL) {
            if (!(*listen_addr_saved) &&
                !IsInplicitIp(replConnArray[i]->localhost)) {
                AddToDataDirLockFile(LOCK_FILE_LINE_LISTEN_ADDR, replConnArray[i]->localhost);
                *listen_addr_saved = true;
            }
            if (IsAlreadyListen(replConnArray[i]->localhost,
                replConnArray[i]->localport)) {
                success++;
                continue;
            }

            status = StreamServerPort(AF_UNSPEC,
                replConnArray[i]->localhost,
                (unsigned short)replConnArray[i]->localport,
                g_instance.attr.attr_network.UnixSocketDir,
                t_thrd.postmaster_cxt.ListenSocket,
                MAXLISTEN,
                false,
                false,
                false);
            if (status == STATUS_OK) {
                success++;
            } else {
                ReportAlarmAbnormalDataHAInstListeningSocket();
                ereport(FATAL,
                    (errmsg("could not create Ha listen socket for ReplConnInfoArr[%d]\"%s:%d\"",
                        i,
                        replConnArray[i]->localhost,
                        replConnArray[i]->localport)));
            }
        }
    }
                
    if (success == 0) {
        ReportAlarmAbnormalDataHAInstListeningSocket();
        ereport(WARNING, (errmsg("could not create any HA TCP/IP sockets")));
    }
}

bool isNotWildcard(void* val1, void* val2)
{
    ListCell* cell = (ListCell*)val1;
    char* nodename = (char*)val2;
    
    char* curhost = (char*)lfirst(cell);
    return (strcmp(curhost, nodename) == 0) ? false : true;
}

void initKnlRTOContext(void)
{
    if (g_instance.rto_cxt.rto_standby_data == NULL) {
        g_instance.rto_cxt.rto_standby_data = (RTOStandbyData*)MemoryContextAllocZero(
            INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(RTOStandbyData) *
            g_instance.attr.attr_storage.max_wal_senders);
    }
}

/*
 * Postmaster main entry point
 */
int PostmasterMain(int argc, char* argv[])
{
    int opt;
    int status = STATUS_OK;
    char* output_config_variable = NULL;
    char* userDoption = NULL;
    bool listen_addr_saved = false;
    int use_pooler_port = -1;
    int i;
    OptParseContext optCtxt;
    errno_t rc = 0;
    Port port;

    t_thrd.proc_cxt.MyProcPid = PostmasterPid = gs_thread_self();

    t_thrd.proc_cxt.MyStartTime = time(NULL);

    IsPostmasterEnvironment = true;

    t_thrd.proc_cxt.MyProgName = "gaussmaster";

    /*
     * for security, no dir or file created can be group or other accessible
     */
    umask(S_IRWXG | S_IRWXO);

    /*
     * Fire up essential subsystems: memory management
     */

    /*
     * By default, palloc() requests in the postmaster will be allocated in
     * the t_thrd.mem_cxt.postmaster_mem_cxt, which is space that can be recycled by backends.
     * Allocated data that needs to be available to backends should be
     * allocated in t_thrd.top_mem_cxt.
     */
    t_thrd.mem_cxt.postmaster_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "Postmaster",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContextSwitchTo(t_thrd.mem_cxt.postmaster_mem_cxt);

    /**
     * initialize version info.
     */
    initialize_feature_flags();

#ifndef ENABLE_LITE_MODE
    /*
     * @OBS
     * Create a global OBS CA object shared among threads
     */
    initOBSCacheObject();

    S3_init();
#endif

    /* set memory manager for minizip libs */
    pm_set_unzip_memfuncs();

    /* set memory manager for cJSON */
    cJSON_Hooks hooks = {cJSON_internal_malloc, cJSON_internal_free};
    cJSON_InitHooks(&hooks);

#ifdef ENABLE_LLVM_COMPILE
    /*
     * Prepare codegen enviroment.
     */
    CodeGenProcessInitialize();
#endif

    /* Initialize paths to installation files */
    getInstallationPaths(argv[0]);

    /*
     * Options setup
     */
    InitializeGUCOptions();

    /*
     *Initialize Callback function type of cb_for_getlc
     */
    SetcbForGetLCName(GetLogicClusterForAlarm);

    optCtxt.opterr = 1;

    /*
     * Parse command-line options.	CAUTION: keep this in sync with
     * tcop/postgres.c (the option sets should not conflict) and with the
     * common help() function in main/main.c.
     */
    initOptParseContext(&optCtxt);
    while ((opt = getopt_r(argc, argv, "A:B:bc:C:D:d:EeFf:h:ijk:lM:N:nOo:Pp:Rr:S:sTt:u:W:g:X:-:", &optCtxt)) != -1) {
        switch (opt) {
            case 'A':
                SetConfigOption("debug_assertions", optCtxt.optarg, PGC_POSTMASTER, PGC_S_ARGV);
                break;

            case 'B':
                SetConfigOption("shared_buffers", optCtxt.optarg, PGC_POSTMASTER, PGC_S_ARGV);
                break;

            case 'b':
                /* Undocumented flag used for binary upgrades */
                u_sess->proc_cxt.IsBinaryUpgrade = true;
                break;

            case 'C':
                output_config_variable = optCtxt.optarg;
                break;

            case 'D':
                userDoption = optCtxt.optarg;
                break;

            case 'd':
                set_debug_options(atoi(optCtxt.optarg), PGC_POSTMASTER, PGC_S_ARGV);
                break;

            case 'E':
                SetConfigOption("log_statement", "all", PGC_POSTMASTER, PGC_S_ARGV);
                break;

            case 'e':
                SetConfigOption("datestyle", "euro", PGC_POSTMASTER, PGC_S_ARGV);
                break;

            case 'F':
                SetConfigOption("fsync", "false", PGC_POSTMASTER, PGC_S_ARGV);
                break;

            case 'f':
                if (!set_plan_disabling_options(optCtxt.optarg, PGC_POSTMASTER, PGC_S_ARGV)) {
                    write_stderr("%s: invalid argument for option -f: \"%s\"\n", progname, optCtxt.optarg);
                    ExitPostmaster(1);
                }

                break;
            case 'g':
                SetConfigOption("xlog_file_path", optCtxt.optarg, PGC_POSTMASTER, PGC_S_ARGV);
                break;
            case 'h':
                SetConfigOption("listen_addresses", optCtxt.optarg, PGC_POSTMASTER, PGC_S_ARGV);
                break;

            case 'i':
                SetConfigOption("listen_addresses", "*", PGC_POSTMASTER, PGC_S_ARGV);
                break;

            case 'j':
                /* only used by interactive backend */
                break;

            case 'k':
                SetConfigOption("unix_socket_directory", optCtxt.optarg, PGC_POSTMASTER, PGC_S_ARGV);
                break;

            case 'l':
                SetConfigOption("ssl", "true", PGC_POSTMASTER, PGC_S_ARGV);
                break;

            case 'M':
                if (0 == strncmp(optCtxt.optarg, "primary", strlen("primary")) &&
                    '\0' == optCtxt.optarg[strlen("primary")]) {
                    t_thrd.xlog_cxt.server_mode = PRIMARY_MODE;
                } else if (0 == strncmp(optCtxt.optarg, "standby", strlen("standby")) &&
                           '\0' == optCtxt.optarg[strlen("standby")]) {
                    t_thrd.xlog_cxt.server_mode = STANDBY_MODE;
                } else if (0 == strncmp(optCtxt.optarg, "pending", strlen("pending")) &&
                           '\0' == optCtxt.optarg[strlen("pending")]) {
                    t_thrd.xlog_cxt.server_mode = PENDING_MODE;
                } else if (0 == strncmp(optCtxt.optarg, "normal", strlen("normal")) &&
                           '\0' == optCtxt.optarg[strlen("normal")]) {
                    t_thrd.xlog_cxt.server_mode = NORMAL_MODE;
                } else if (0 == strncmp(optCtxt.optarg, "cascade_standby", strlen("cascade_standby")) &&
                           '\0' == optCtxt.optarg[strlen("cascade_standby")]) {
                    t_thrd.xlog_cxt.server_mode = STANDBY_MODE;
                    t_thrd.xlog_cxt.is_cascade_standby = true;
                } else if (0 == strncmp(optCtxt.optarg, "hadr_main_standby", strlen("hadr_main_standby")) &&
                           '\0' == optCtxt.optarg[strlen("hadr_main_standby")]) {
                    t_thrd.xlog_cxt.server_mode = STANDBY_MODE;
                    t_thrd.xlog_cxt.is_hadr_main_standby = true;
                } else {
                    ereport(FATAL, (errmsg("the options of -M is not recognized")));
                }
                break;

            case 'N':
                SetConfigOption("max_connections", optCtxt.optarg, PGC_POSTMASTER, PGC_S_ARGV);
                break;

            case 'n':
                /* Don't reinit shared mem after abnormal exit */
                ereport(FATAL, (errmsg("the options of -n is deprecated")));
                break;

            case 'O':
                SetConfigOption("allow_system_table_mods", "true", PGC_POSTMASTER, PGC_S_ARGV);
                break;

            case 'o':
                /* Other options to pass to the backend on the command line */
                rc = snprintf_s(g_instance.ExtraOptions + strlen(g_instance.ExtraOptions),
                    sizeof(g_instance.ExtraOptions) - strlen(g_instance.ExtraOptions),
                    sizeof(g_instance.ExtraOptions) - strlen(g_instance.ExtraOptions) - 1,
                    " %s",
                    optCtxt.optarg);
                securec_check_ss(rc, "", "");
                break;

            case 'P':
                SetConfigOption("ignore_system_indexes", "true", PGC_POSTMASTER, PGC_S_ARGV);
                break;

            case 'p':
                SetConfigOption("port", optCtxt.optarg, PGC_POSTMASTER, PGC_S_ARGV);
                break;
#ifdef ENABLE_MULTIPLE_NODES
            case 'R':
                /* indicate run as xlogreiver.Only used with -M standby */
                dummyStandbyMode = true;
                break;
#endif
            case 'r':
                /* only used by single-user backend */
                break;

            case 'S':
                SetConfigOption("work_mem", optCtxt.optarg, PGC_POSTMASTER, PGC_S_ARGV);
                break;

            case 's':
                SetConfigOption("log_statement_stats", "true", PGC_POSTMASTER, PGC_S_ARGV);
                break;

            case 'T':

                /*
                 * In the event that some backend dumps core, send SIGSTOP,
                 * rather than SIGQUIT, to all its peers.  This lets the wily
                 * post_hacker collect core dumps from everyone.
                 */
                ereport(FATAL, (errmsg("the options of -T is deprecated")));
                break;

            case 't': {
                const char* tmp = get_stats_option_name(optCtxt.optarg);

                if (tmp != NULL) {
                    SetConfigOption(tmp, "true", PGC_POSTMASTER, PGC_S_ARGV);
                } else {
                    write_stderr("%s: invalid argument for option -t: \"%s\"\n", progname, optCtxt.optarg);
                    ExitPostmaster(1);
                }

                break;
            }

            case 'u':
                /* Undocumented version used for inplace or online upgrades */
                errno = 0;
                pg_atomic_write_u32(&WorkingGrandVersionNum, (uint32)strtoul(optCtxt.optarg, NULL, 10));
                if (errno != 0 || pg_atomic_read_u32(&WorkingGrandVersionNum) > GRAND_VERSION_NUM) {
                    write_stderr("%s: invalid argument for option -u: \"%s\", GRAND_VERSION_NUM is %u\n",
                        progname,
                        optCtxt.optarg,
                        (uint32)GRAND_VERSION_NUM);
                    ExitPostmaster(1);
                } else if (pg_atomic_read_u32(&WorkingGrandVersionNum) == INPLACE_UPGRADE_PRECOMMIT_VERSION) {
                    pg_atomic_write_u32(&WorkingGrandVersionNum, GRAND_VERSION_NUM);
                    g_instance.comm_cxt.force_cal_space_info = true;
                }
                break;

            case 'W':
                SetConfigOption("post_auth_delay", optCtxt.optarg, PGC_POSTMASTER, PGC_S_ARGV);
                break;
            case 'X':
                /* stop barrier */
                if ((optCtxt.optarg != NULL) && (strlen(optCtxt.optarg) > 0)) {
                    if (strlen(optCtxt.optarg) > MAX_BARRIER_ID_LENGTH) {
                        ereport(FATAL, (errmsg("the options of -X is too long")));
                    }
                    rc = strncpy_s(g_instance.csn_barrier_cxt.stopBarrierId, MAX_BARRIER_ID_LENGTH,
                                   (char *)optCtxt.optarg, strlen(optCtxt.optarg));
                    securec_check(rc, "\0", "\0");
                    ereport(LOG, (errmsg("Set stop barrierID %s", g_instance.csn_barrier_cxt.stopBarrierId)));
                }
                break;
            case 'c':
            case '-': {
                char* name = NULL;
                char* value = NULL;

                ParseLongOption(optCtxt.optarg, &name, &value);

#ifndef ENABLE_MULTIPLE_NODES
                if (opt == '-' && (strcmp(name, "coordinator") == 0 || strcmp(name, "datanode") == 0)) {
                    ereport(FATAL,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("Single node mode: must start as single node (--single_node)\n")));
                }
#endif
                /* A Coordinator is being activated */
                if (name != NULL && strcmp(name, "coordinator") == 0 && value == NULL)
                    g_instance.role = VCOORDINATOR;
                else if (name != NULL && strcmp(name, "datanode") == 0 && value == NULL)
                    g_instance.role = VDATANODE;
                /* A SingleDN mode is being activated */
                else if (name != NULL && strcmp(name, "single_node") == 0 && value == NULL) {
                    g_instance.role = VSINGLENODE;
                    useLocalXid = true;
                } else if (name != NULL && strcmp(name, "restoremode") == 0 && value == NULL) {
                    /*
                     * In restore mode both coordinator and datanode
                     * are internally treated as datanodes
                     */
                    isRestoreMode = true;
                    g_instance.role = VDATANODE;
                } else if (name != NULL && 0 == strcmp(name, "fenced") && value == NULL)
                    FencedUDFMasterMode = true;
                else if (name != NULL && strlen(name) == SECURITY_MODE_NAME_LEN &&
                         strncmp(name, SECURITY_MODE_NAME, SECURITY_MODE_NAME_LEN) == 0 && value == NULL) {
                    /* In securitymode, safety strategy is opened */
                    isSecurityMode = true;
                } else {
                    /* default case */
                    if (value == NULL) {
                        if (opt == '-')
                            ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("--%s requires a value", optCtxt.optarg)));
                        else
                            ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("-c %s requires a value", optCtxt.optarg)));
                    }

                    SetConfigOption(name, value, PGC_POSTMASTER, PGC_S_ARGV);
                }
                pfree(name);

                if (value != NULL) {
                    pfree(value);
                }

                break;
            }

            default:
                write_stderr("Try \"%s --help\" for more information.\n", progname);
                ExitPostmaster(1);
        }
    }

#ifndef ENABLE_MULTIPLE_NODES
    /* single_node mode default role is VSINGLENODE and must be it */
    if (g_instance.role == VUNKNOWN) {
        g_instance.role = VSINGLENODE;
        useLocalXid = true;
    } else if (g_instance.role != VSINGLENODE) {
        write_stderr("Single node mode: must start as single node.\n");
        ExitPostmaster(1);
    }
#else

    if (!IS_PGXC_COORDINATOR && !IS_PGXC_DATANODE && !FencedUDFMasterMode && !PythonFencedMasterModel) {
        write_stderr(
            "%s: openGauss: must start as either a Coordinator (--coordinator) or Datanode (--datanode)\n", progname);
        ExitPostmaster(1);
    }

#endif
    /*
     * Postmaster accepts no non-option switch arguments.
     */
    if (optCtxt.optind < argc) {
        write_stderr("%s: invalid argument: \"%s\"\n", progname, argv[optCtxt.optind]);
        write_stderr("Try \"%s --help\" for more information.\n", progname);
        ExitPostmaster(1);
    }
    
    /*
     * Locate the proper configuration files and data directory, and read
     * postgresql.conf for the first time.
     */
#if ((defined ENABLE_PYTHON2) || (defined ENABLE_PYTHON3))
    if (!SelectConfigFiles(userDoption, progname))
        ExitPostmaster(1);

    if (strlen(GetConfigOption(const_cast<char*>("unix_socket_directory"), true, false)) != 0) {
        PythonFencedMasterModel = true;
        
        /* disable bbox for fenced UDF process */
        SetConfigOption("enable_bbox_dump", "false", PGC_POSTMASTER, PGC_S_ARGV);
    }
#else
    if (FencedUDFMasterMode) {
        /* disable bbox for fenced UDF process */
        SetConfigOption("enable_bbox_dump", "false", PGC_POSTMASTER, PGC_S_ARGV);
    } else if (!SelectConfigFiles(userDoption, progname)) {
        ExitPostmaster(1);
    }
#endif

    if ((g_instance.attr.attr_security.transparent_encrypted_string != NULL &&
         g_instance.attr.attr_security.transparent_encrypted_string[0] != '\0') &&
        (g_instance.attr.attr_common.transparent_encrypt_kms_url != NULL &&
         g_instance.attr.attr_common.transparent_encrypt_kms_url[0] != '\0') &&
        (g_instance.attr.attr_security.transparent_encrypt_kms_region != NULL &&
         g_instance.attr.attr_security.transparent_encrypt_kms_region[0] != '\0')) {
        isSecurityMode = true;
    }

    /*
     * Initiaize Postmaster level GUC option.
     *
     * Note that some guc can't get right value because of some global var not init,
     * for example single_node mode,
     * so need this function to init postmaster level guc.
     */
    InitializePostmasterGUC();

    t_thrd.myLogicTid = noProcLogicTid + POSTMASTER_LID;
    if (output_config_variable != NULL) {
        /*
         * permission is handled because the user is reading inside the data
         * dir
         */
        puts(GetConfigOption(output_config_variable, false, false));
        ExitPostmaster(0);
    }

    InitializeNumLwLockPartitions();

    noProcLogicTid = GLOBAL_ALL_PROCS;

    if (FencedUDFMasterMode) {
        /* t_thrd.proc_cxt.DataDir must be set */
        if (userDoption != NULL && userDoption[0] == '/') {
            if (chdir(userDoption) == -1)
                ExitPostmaster(1);
            SetDataDir(userDoption);
        } else {
            ExitPostmaster(1);
        }
        /* init thread args pool for ever sub threads except signal moniter */
        gs_thread_args_pool_init(GLOBAL_ALL_PROCS + EXTERN_SLOTS_NUM, sizeof(BackendParameters));
        /* Init signal manage struct */
        gs_signal_slots_init(GLOBAL_ALL_PROCS + EXTERN_SLOTS_NUM);
        gs_signal_startup_siginfo("PostmasterMain");

        gs_signal_monitor_startup();
        g_instance.attr.attr_common.Logging_collector = true;
        g_instance.global_sysdbcache.Init(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
        CreateLocalSysDBCache();
        g_instance.pid_cxt.SysLoggerPID = SysLogger_Start();
        FencedUDFMasterMain(0, NULL);
        return 0;
    }

    /* Verify that t_thrd.proc_cxt.DataDir looks reasonable */
    checkDataDir();

    /* And switch working directory into it */
    ChangeToDataDir();
    /*
        * Check for invalid combinations of GUC settings.
        */
    CheckGUCConflicts();

    /* Set parallel recovery config */
    ConfigRecoveryParallelism();
    ProcessRedoCpuBindInfo();
    /*
        * Other one-time internal sanity checks can go here, if they are fast.
        * (Put any slow processing further down, after postmaster.pid creation.)
        */
    if (!CheckDateTokenTables()) {
        write_stderr("%s: invalid datetoken tables, please fix\n", progname);
        ExitPostmaster(1);
    }

    initKnlRTOContext();

    /*
        * Now that we are done processing the postmaster arguments, reset
        * getopt(3) library so that it will work correctly in subprocesses.
        */
    optCtxt.optind = 1;
#ifdef HAVE_INT_OPTRESET
    optreset = 1; /* some systems need this too */
#endif

    int rc1 = snprintf_s(
        gaussdb_state_file, sizeof(gaussdb_state_file), MAXPGPATH - 1, "%s/gaussdb.state", t_thrd.proc_cxt.DataDir);
    securec_check_intval(rc1, , -1);
    gaussdb_state_file[MAXPGPATH - 1] = '\0';
    if (!SetDBStateFileState(UNKNOWN_STATE, true)) {
        write_stderr("Failed to set gaussdb.state with UNKNOWN_STATE");
        ExitPostmaster(1);
    }

    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
        rc1 = snprintf_s(AddMemberFile, sizeof(AddMemberFile), MAXPGPATH - 1, "%s/addmember", t_thrd.proc_cxt.DataDir);
        securec_check_intval(rc1, , -1);
        AddMemberFile[MAXPGPATH - 1] = '\0';

        rc1 = snprintf_s(RemoveMemberFile, sizeof(RemoveMemberFile), MAXPGPATH - 1, "%s/removemember",
            t_thrd.proc_cxt.DataDir);
        securec_check_intval(rc1, , -1);
        RemoveMemberFile[MAXPGPATH - 1] = '\0';

        rc1 = snprintf_s(TimeoutFile, sizeof(TimeoutFile), MAXPGPATH - 1, "%s/timeout", t_thrd.proc_cxt.DataDir);
        securec_check_intval(rc1, , -1);
        TimeoutFile[MAXPGPATH - 1] = '\0';

        rc1 = snprintf_s(SwitchoverStatusFile, sizeof(SwitchoverStatusFile), MAXPGPATH - 1, "%s/switchoverstatus",
            t_thrd.proc_cxt.DataDir);
        securec_check_intval(rc1, , -1);
        SwitchoverStatusFile[MAXPGPATH - 1] = '\0';

        rc1 = snprintf_s(SetRunmodeStatusFile, sizeof(SetRunmodeStatusFile), MAXPGPATH - 1, "%s/setrunmodestatus",
            t_thrd.proc_cxt.DataDir);
        securec_check_intval(rc1, , -1);
        SetRunmodeStatusFile[MAXPGPATH - 1] = '\0';

        rc1 = snprintf_s(g_changeRoleStatusFile, sizeof(g_changeRoleStatusFile), MAXPGPATH - 1, "%s/changerolestatus",
            t_thrd.proc_cxt.DataDir);
        securec_check_intval(rc1, , -1);
        g_changeRoleStatusFile[MAXPGPATH - 1] = '\0';

        rc1 =
            snprintf_s(ChangeRoleFile, sizeof(ChangeRoleFile), MAXPGPATH - 1, "%s/changerole", t_thrd.proc_cxt.DataDir);
        securec_check_intval(rc1, , -1);
        ChangeRoleFile[MAXPGPATH - 1] = '\0';

        rc1 = snprintf_s(StartMinorityFile, sizeof(StartMinorityFile), MAXPGPATH - 1, "%s/startminority",
            t_thrd.proc_cxt.DataDir);
        securec_check_intval(rc1, , -1);
        StartMinorityFile[MAXPGPATH - 1] = '\0';
    }

    /* For debugging: display postmaster environment */
    {
        extern char** environ;
        char** p;

        ereport(DEBUG3, (errmsg_internal("%s: PostmasterMain: initial environment dump:", progname)));
        ereport(DEBUG3, (errmsg_internal("-----------------------------------------")));

        for (p = environ; *p; ++p)
            ereport(DEBUG3, (errmsg_internal("\t%s", *p)));

        ereport(DEBUG3, (errmsg_internal("-----------------------------------------")));
    }

    rc = memcpy_s(g_alarmComponentPath, MAXPGPATH - 1, Alarm_component, strlen(Alarm_component));
    securec_check_c(rc, "\0", "\0");
    g_alarmReportInterval = AlarmReportInterval;
    AlarmEnvInitialize();

    /*
        * Create lockfile for data directory.
        *
        * We want to do this before we try to grab the input sockets, because the
        * data directory interlock is more reliable than the socket-file
        * interlock (thanks to whoever decided to put socket files in /tmp :-().
        * For the same reason, it's best to grab the TCP socket(s) before the
        * Unix socket.
        */
    CreateDataDirLockFile(true);

    /* Module load callback */
    pgaudit_agent_init();
    auto_explain_init();
    ledger_hook_init();

    /*
        * process any libraries that should be preloaded at postmaster start
        */
    process_shared_preload_libraries();

    /*
        * Establish input sockets.
        */
    for (i = 0; i < MAXLISTEN; i++)
        t_thrd.postmaster_cxt.ListenSocket[i] = PGINVALID_SOCKET;

    if (g_instance.attr.attr_network.ListenAddresses && !dummyStandbyMode) {
        char* rawstring = NULL;
        List* elemlist = NULL;
        ListCell* l = NULL;
        int success = 0;

        /*
            * start commproxy if needed
            */
        if (CommProxyNeedSetup()) {
            CommProxyStartUp();
        }

        /* Need a modifiable copy of g_instance.attr.attr_network.ListenAddresses */
        rawstring = pstrdup(g_instance.attr.attr_network.ListenAddresses);

        /* Parse string into list of identifiers */
        if (!SplitIdentifierString(rawstring, ',', &elemlist)) {
            /* syntax error in list */
            ereport(FATAL,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid list syntax for \"listen_addresses\"")));
        }

        bool haswildcard = false;
        foreach (l, elemlist) {
            char* curhost = (char*)lfirst(l);
            if (strcmp(curhost, "*") == 0) {
                haswildcard = true;
                break;
            }
        }
        
        if (haswildcard == true) {
            char *wildcard = "*";
            elemlist = list_cell_clear(elemlist, (void *)wildcard, isNotWildcard);
        }

        foreach (l, elemlist) {
            char* curhost = (char*)lfirst(l);

            if (strcmp(curhost, "*") == 0)
                status = StreamServerPort(AF_UNSPEC,
                    NULL,
                    (unsigned short)g_instance.attr.attr_network.PostPortNumber,
                    g_instance.attr.attr_network.UnixSocketDir,
                    t_thrd.postmaster_cxt.ListenSocket,
                    MAXLISTEN,
                    true,
                    true,
                    false);
            else
                status = StreamServerPort(AF_UNSPEC,
                    curhost,
                    (unsigned short)g_instance.attr.attr_network.PostPortNumber,
                    g_instance.attr.attr_network.UnixSocketDir,
                    t_thrd.postmaster_cxt.ListenSocket,
                    MAXLISTEN,
                    true,
                    true,
                    false);

            if (status == STATUS_OK)
                success++;
            else {
                print_port_info();
                ereport(FATAL,
                    (errmsg("could not create listen socket for \"%s:%d\"",
                        curhost,
                        g_instance.attr.attr_network.PostPortNumber)));
            }

            /* At present, we do not listen replconn channels under NORMAL_MODE, so pooler port is needed */
            use_pooler_port = NeedPoolerPort(curhost);
            if (t_thrd.xlog_cxt.server_mode == NORMAL_MODE || use_pooler_port == -1) {
                /* In om and other maintenance tools, pooler port is hardwired to be gsql port plus one */
                if (g_instance.attr.attr_network.PoolerPort != (g_instance.attr.attr_network.PostPortNumber + 1)) {
                    ereport(FATAL, (errmsg("pooler_port must equal to gsql listen port plus one!")));
                }

                if (strcmp(curhost, "*") == 0) {
                    status = StreamServerPort(AF_UNSPEC,
                        NULL,
                        (unsigned short)g_instance.attr.attr_network.PoolerPort,
                        g_instance.attr.attr_network.UnixSocketDir,
                        t_thrd.postmaster_cxt.ListenSocket,
                        MAXLISTEN,
                        false,
                        false,
                        false);
                } else {
                    status = StreamServerPort(AF_UNSPEC,
                        curhost,
                        (unsigned short)g_instance.attr.attr_network.PoolerPort,
                        g_instance.attr.attr_network.UnixSocketDir,
                        t_thrd.postmaster_cxt.ListenSocket,
                        MAXLISTEN,
                        false,
                        false,
                        false);
                }

                if (status != STATUS_OK)
                    ereport(FATAL,
                        (errmsg("could not create ha listen socket for \"%s:%d\"",
                            curhost,
                            g_instance.attr.attr_network.PoolerPort)));

                /*
                    * Record the first successful host addr which does not mean 'localhost' in lockfile.
                    * Inner maintanence tools, such as cm_agent and gs_ctl, will use that host for connecting cn.
                    */
                if (!listen_addr_saved && !IsInplicitIp(curhost)) {
                    AddToDataDirLockFile(LOCK_FILE_LINE_LISTEN_ADDR, curhost);
                    listen_addr_saved = true;
                }
            }
        }

        if (!success && list_length(elemlist))
            ereport(FATAL, (errmsg("could not create any TCP/IP sockets")));

        list_free_ext(elemlist);
        pfree(rawstring);
    }

    if (t_thrd.xlog_cxt.server_mode != NORMAL_MODE) {
        SetListenSocket(t_thrd.postmaster_cxt.ReplConnArray, &listen_addr_saved);
        ReportResumeAbnormalDataHAInstListeningSocket();
    }
    SetListenSocket(t_thrd.postmaster_cxt.CrossClusterReplConnArray, &listen_addr_saved);
    ReportResumeAbnormalDataHAInstListeningSocket();
#ifdef USE_BONJOUR

    /* Register for Bonjour only if we opened TCP socket(s) */
    if (g_instance.attr.attr_common.enable_bonjour && t_thrd.postmaster_cxt.ListenSocket[0] != PGINVALID_SOCKET) {
        DNSServiceErrorType err;

        /*
            * We pass 0 for interface_index, which will result in registering on
            * all "applicable" interfaces.  It's not entirely clear from the
            * DNS-SD docs whether this would be appropriate if we have bound to
            * just a subset of the available network interfaces.
            */
        err = DNSServiceRegister(&bonjour_sdref,
            0,
            0,
            g_instance.attr.attr_common.bonjour_name,
            "_postgresql._tcp.",
            NULL,
            NULL,
            htons(g_instance.attr.attr_network.PostPortNumber),
            0,
            NULL,
            NULL,
            NULL);

        if (err != kDNSServiceErr_NoError)
            ereport(LOG, (errmsg("DNSServiceRegister() failed: error code %ld", (long)err)));

        /*
            * We don't bother to read the mDNS daemon's reply, and we expect that
            * it will automatically terminate our registration when the socket is
            * closed at postmaster termination.  So there's nothing more to be
            * done here.  However, the bonjour_sdref is kept around so that
            * forked children can close their copies of the socket.
            */
    }

#endif

#ifdef HAVE_UNIX_SOCKETS
    if (!dummyStandbyMode) {
        /* unix socket for gsql port */
        status = StreamServerPort(AF_UNIX,
            NULL,
            (unsigned short)g_instance.attr.attr_network.PostPortNumber,
            g_instance.attr.attr_network.UnixSocketDir,
            t_thrd.postmaster_cxt.ListenSocket,
            MAXLISTEN,
            false,
            true,
            false);

        if (status != STATUS_OK)
            ereport(FATAL,
                (errmsg("could not create Unix-domain socket for \"%s:%d\"",
                    g_instance.attr.attr_network.UnixSocketDir,
                    g_instance.attr.attr_network.PostPortNumber)));

        /* unix socket for ha port */
        status = StreamServerPort(AF_UNIX,
            NULL,
            (unsigned short)g_instance.attr.attr_network.PoolerPort,
            g_instance.attr.attr_network.UnixSocketDir,
            t_thrd.postmaster_cxt.ListenSocket,
            MAXLISTEN,
            false,
            false,
            false);

        if (status != STATUS_OK)
            ereport(FATAL,
                (errmsg("could not create Unix-domain socket for \"%s:%d\"",
                    g_instance.attr.attr_network.UnixSocketDir,
                    g_instance.attr.attr_network.PoolerPort)));

        /*
            * create listened unix domain socket to receive gs_sock
            * from receiver_loop thread.
            * NOTE: as we will use global variable sock_path to init libcomm later,
            * so this socket must be the last created unix domain socket.
            * otherwise, the sock_path will be replace by other path.
            */
        if (g_instance.attr.attr_storage.comm_cn_dn_logic_conn && !isRestoreMode && !IS_SINGLE_NODE) {
            status = StreamServerPort(AF_UNIX,
                NULL,
                (unsigned short)g_instance.attr.attr_network.comm_sctp_port,
                g_instance.attr.attr_network.UnixSocketDir,
                t_thrd.postmaster_cxt.ListenSocket,
                MAXLISTEN,
                false,
                true,
                true);

            if (status != STATUS_OK)
                ereport(WARNING, (errmsg("could not create Unix-domain for comm  socket")));
        }
    }
#endif

    /*
        * check that we have some socket to listen on
        */
    if (t_thrd.postmaster_cxt.ListenSocket[0] == PGINVALID_SOCKET) {
        ereport(FATAL, (errmsg("no socket created for listening")));
    }

    /*
        * Set up an on_proc_exit function that's charged with closing the sockets
        * again at postmaster shutdown.  You might think we should have done this
        * earlier, but we want it to run before not after the proc_exit callback
        * that will remove the Unix socket file.
        */
    on_proc_exit(CloseServerPorts, 0);

    /*
        * If no valid TCP ports, write an empty line for listen address,
        * indicating the Unix socket must be used.  Note that this line is not
        * added to the lock file until there is a socket backing it.
        */
    if (!listen_addr_saved) {
        AddToDataDirLockFile(LOCK_FILE_LINE_LISTEN_ADDR, "");
        ereport(WARNING, (errmsg("No explicit IP is configured for listen_addresses GUC.")));
    }

    if (g_instance.attr.attr_common.enable_thread_pool) {
        /* No need to start thread pool for dummy standby node. */
        if (!dummyStandbyMode) {
            g_threadPoolControler = (ThreadPoolControler*)
                New(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR)) ThreadPoolControler();
            g_threadPoolControler->SetThreadPoolInfo();
        } else {
            g_instance.attr.attr_common.enable_thread_pool = false;
            g_threadPoolControler = NULL;
            AdjustThreadAffinity();
        }
    }

    /* init gstrace context */
    int errcode = gstrace_init(g_instance.attr.attr_network.PostPortNumber);
    if (errcode != 0) {
        ereport(LOG, (errmsg("gstrace initializes with failure. errno = %d.", errcode)));
    }

    InitGlobalBcm();

    /*
        * Set up shared memory and semaphores.
        */
    reset_shared(g_instance.attr.attr_network.PostPortNumber);

    /* Alloc array for backend record. */
    BackendArrayAllocation();

    /* init thread args pool for ever sub threads except signal moniter */
    gs_thread_args_pool_init(GLOBAL_ALL_PROCS + EXTERN_SLOTS_NUM, sizeof(BackendParameters));
    // 1.init signal manage struct
    //
    gs_signal_slots_init(GLOBAL_ALL_PROCS + EXTERN_SLOTS_NUM);
    gs_signal_startup_siginfo("PostmasterMain");

    gs_signal_monitor_startup();

    /*
        * Estimate number of openable files.  This must happen after setting up
        * semaphores, because on some platforms semaphores count as open files.
        */
    SetHaShmemData();

    PMInitDBStateFile();

    set_max_safe_fds();

    /*
        * Set reference point for stack-depth checking.
        */
    set_stack_base();

    /*
        * Initialize the list of active backends.
        */
    g_instance.backend_list = DLNewList();

    /*
        * Initialize pipe (or process handle on Windows) that allows children to
        * wake up from sleep on postmaster death.
        */
    InitPostmasterDeathWatchHandle();

#ifdef WIN32

    /*
        * Initialize I/O completion port used to deliver list of dead children.
        */
    win32ChildQueue = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 1);

    if (win32ChildQueue == NULL)
        ereport(FATAL, (errmsg("could not create I/O completion port for child queue")));

#endif

    /*
        * Record postmaster options.  We delay this till now to avoid recording
        * bogus options (eg, NBuffers too high for available memory).
        */
    if (!CreateOptsFile(argc, (const char**)argv, (const char*)my_exec_path))
        ExitPostmaster(1);

#ifdef EXEC_BACKEND
    /* Write out nondefault GUC settings for child processes to use */
    write_nondefault_variables(PGC_POSTMASTER);
#endif

#ifndef ENABLE_LITE_MODE
#if defined (ENABLE_MULTIPLE_NODES) || defined (ENABLE_PRIVATEGAUSS)
    /* init hotpatch */
    if (hotpatch_remove_signal_file(t_thrd.proc_cxt.DataDir) == HP_OK) {
        int ret;
        ret = hotpatch_init(t_thrd.proc_cxt.DataDir, (HOTPATCH_LOG_FUNC)gs_hotpatch_log_callback);
        if (ret != HP_OK) {
            write_stderr("hotpatch init failed ret is %d!\n", ret);
        }
    }
#endif
#endif
    /*
     * Write the external PID file if requested
     */
    if (g_instance.attr.attr_common.external_pid_file) {
        FILE* fpidfile = fopen(g_instance.attr.attr_common.external_pid_file, "w");

        if (fpidfile != NULL) {
            fprintf(fpidfile, "%lu\n", t_thrd.proc_cxt.MyProcPid);
            fclose(fpidfile);
            /* Should we remove the pid file on postmaster exit? */

            /* Make PID file world readable */
            if (chmod(g_instance.attr.attr_common.external_pid_file, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH) != 0)
                write_stderr("%s: could not change permissions of external PID file \"%s\": %s\n",
                    progname,
                    g_instance.attr.attr_common.external_pid_file,
                    gs_strerror(errno));
        } else
            write_stderr("%s: could not write external PID file \"%s\": %s\n",
                progname,
                g_instance.attr.attr_common.external_pid_file,
                gs_strerror(errno));
    }

    /*
     * Set up signal handlers for the postmaster process.
     *
     * CAUTION: when changing this list, check for side-effects on the signal
     * handling setup of child processes.  See tcop/postgres.c,
     * bootstrap/bootstrap.c, postmaster/bgwriter.c, postmaster/walwriter.c,
     * postmaster/autovacuum.c, postmaster/pgarch.c, postmaster/pgstat.c,
     * postmaster/syslogger.c and postmaster/checkpointer.c.
     */

    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
    gs_signal_block_sigusr2();

    (void)gspqsignal(SIGHUP, SIGHUP_handler); /* reread config file and have
                                               * children do same */
    (void)gspqsignal(SIGINT, pmdie);          /* send SIGTERM and shut down */
    (void)gspqsignal(SIGQUIT, pmdie);         /* send SIGQUIT and die */
    (void)gspqsignal(SIGTERM, pmdie);         /* wait for children and shut down */

    pqsignal(SIGALRM, SIG_IGN); /* ignored */
    pqsignal(SIGPIPE, SIG_IGN); /* ignored */
    pqsignal(SIGFPE, FloatExceptionHandler);

    (void)gspqsignal(SIGUSR1, sigusr1_handler); /* message from child process */
    (void)gspqsignal(SIGUSR2, dummy_handler);   /* unused, reserve for children */
    (void)gspqsignal(SIGCHLD, reaper);          /* handle child termination */
    (void)gspqsignal(SIGTTIN, SIG_IGN);         /* ignored */
    (void)gspqsignal(SIGTTOU, SIG_IGN);         /* ignored */

    /* ignore SIGXFSZ, so that ulimit violations work like disk full */
#ifdef SIGXFSZ
    (void)gspqsignal(SIGXFSZ, SIG_IGN); /* ignored */
#endif

    /* core dump injection */
    bbox_initialize();

    /*
     * Initialize stats collection subsystem (this does NOT start the
     * collector process!)
     */
    pgstat_init();

    /* Initialize the global stats tracker */
    GlobalStatsTrackerInit();

    /* initialize workload manager */
    InitializeWorkloadManager();

    g_instance.global_sysdbcache.Init(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
    CreateLocalSysDBCache();

    /* Init proc's subxid cache context, parent is g_instance.instance_context */
    ProcSubXidCacheContext = AllocSetContextCreate(g_instance.instance_context,
        "ProcSubXidCacheContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    /*
     * Create StreamInfoContext for stream thread connection, parent is g_instance.instance_context.
     * All stream threads will share this context.
     */
    StreamInfoContext = AllocSetContextCreate(g_instance.instance_context,
        "StreamInfoContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);
        
    /* create global cache memory context */
    knl_g_cachemem_create();

    /* create node group cache hash table */
    ngroup_info_hash_create();
    /*init Role id hash table*/
    InitRoleIdHashTable();
    /* pcmap */
    RealInitialMMapLockArray();
    /* init unique sql */
    InitUniqueSQL();
    /* init hypo index */
    InitHypopg();
    InitAsp();
    /* init instr user */
    InitInstrUser();
    /* init Opfusion function id */
    InitOpfusionFunctionId();
    /* init capture view */
    init_capture_view();
    /* init percentile */
    InitPercentile();
    /* init dynamic statement track control */
    InitTrackStmtControl();
    /* init global sequence */
    InitGlobalSeq();
#ifdef ENABLE_MULTIPLE_NODES
    /* init compaction */
    CompactionProcess::init_instance();
    /*
     * Set up TsStoreTagsCache
     */
    if (g_instance.attr.attr_common.enable_tsdb) {
        TagsCacheMgr::GetInstance().init();
        PartIdMgr::GetInstance().init();
        Tsdb::PartCacheMgr::GetInstance().init();
        InitExtensiblePlanMethodsHashTable();
    }
#endif

    /*
     * If enabled, start up syslogger collection subprocess
     */
    g_instance.pid_cxt.SysLoggerPID = SysLogger_Start();

    if (IS_PGXC_DATANODE && !dummyStandbyMode && !isRestoreMode) {
        StreamObj::startUp();
        StreamNodeGroup::StartUp();
        pthread_mutex_init(&nodeDefCopyLock, NULL);
    }

    /* init the usedDnSpace hash table */
    InitDnHashTable();

    /*
     * Load configuration files for client authentication.
     * Load pg_hba.conf before communication thread.
     */
    int loadhbaCount = 0;
    while (!load_hba()) {
        check_old_hba(true);
        loadhbaCount++;
        if (loadhbaCount >= 3) {
            /*
             * It makes no sense to continue if we fail to load the HBA file,
             * since there is no way to connect to the database in this case.
             */
            ereport(FATAL, (errmsg("could not load pg_hba.conf")));
        }
        pg_usleep(200000L);  /* sleep 200ms for reload next time */
    }

    if (ENABLE_THREAD_POOL_DN_LOGICCONN) {
        InitCommLogicResource();
    }

    if ((!IS_SINGLE_NODE) &&
        ((IS_PGXC_DATANODE && !dummyStandbyMode && !isRestoreMode) ||
            (IS_PGXC_COORDINATOR && g_instance.attr.attr_storage.comm_cn_dn_logic_conn && !isRestoreMode))) {
        status = init_stream_comm();
        if (status != STATUS_OK)
            ereport(FATAL, (errmsg("Init libcomm for stream failed, maybe listen port already in use")));
    }
    if (g_instance.attr.attr_security.enable_tde) {
        /* init cloud KMS message instance */
        TDE::CKMSMessage::get_instance().init();
        TDE::CKMSMessage::get_instance().load_user_info();
        /* init TDE storage hash table */
        if (IS_PGXC_DATANODE) {
            TDE::TDEKeyStorage::get_instance().init();
            TDE::TDEBufferCache::get_instance().init();
        }
    }

#ifndef ENABLE_LITE_MODE
    if (g_instance.attr.attr_storage.enable_adio_function)
        AioResourceInitialize();
#endif

    /* start alarm checker thread. */
    if (!dummyStandbyMode)
        g_instance.pid_cxt.AlarmCheckerPID = startAlarmChecker();

    /* start reaper backend thread which is always alive. */
    g_instance.pid_cxt.ReaperBackendPID = initialize_util_thread(REAPER);

    /*
     * Reset whereToSendOutput from DestDebug (its starting state) to
     * DestNone. This stops ereport from sending log messages to stderr unless
     * t_thrd.aes_cxt.Log_destination permits.  We don't do this until the postmaster is
     * fully launched, since startup failures may as well be reported to
     * stderr.
     */
    t_thrd.postgres_cxt.whereToSendOutput = DestNone;

    /*
     * Initialize the autovacuum subsystem (again, no process start yet)
     */
    autovac_init();

    load_ident();

    /*
     * Remove old temporary files.	At this point there can be no other
     * openGauss processes running in this directory, so this should be safe.
     */
    RemovePgTempFiles();

    RemoveErrorCacheFiles();
    /*
     * Remember postmaster startup time
     */
    t_thrd.time_cxt.pg_start_time = GetCurrentTimestamp();

    /* node stat validation timestamp */
    gs_lock_test_and_set_64(&g_instance.stat_cxt.NodeStatResetTime, GetCurrentTimestamp());

    /* PostmasterRandom wants its own copy */
    gettimeofday(&t_thrd.postmaster_cxt.random_start_time, NULL);

    /*
     * We're ready to rock and roll...
     */
    ShareStorageInit();
    g_instance.pid_cxt.StartupPID = initialize_util_thread(STARTUP);
    Assert(g_instance.pid_cxt.StartupPID != 0);
    pmState = PM_STARTUP;

#ifdef ENABLE_MULTIPLE_NODES

    if (IS_PGXC_COORDINATOR) {
        MemoryContext oldcontext = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));

        /*
         * Initialize the Data Node connection pool.
         * pooler thread don't exist any more, StartPoolManager() is an alias of
         * PoolManagerInit().
         */
        StartPoolManager();

        MemoryContextSwitchTo(oldcontext);
    }

    load_searchserver_library();
#endif
    /* 
     * Save backend variables for DCF call back thread, 
     * the saved backend variables will be restored in
     * DCF call back thread share memory init function.
     */
    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
        int ss_rc = memset_s(&port, sizeof(port), 0, sizeof(port));
        securec_check(ss_rc, "\0", "\0");
        port.sock = PGINVALID_SOCKET;
        BackendVariablesGlobal = static_cast<BackendParameters *>(palloc(sizeof(BackendParameters)));
        save_backend_variables(BackendVariablesGlobal, &port);
    }
    /* If start with plpython fenced mode, we just startup as fenced mode */
    if (PythonFencedMasterModel) {
        /*
         * If enabled, start up syslogger collection subprocess
         */
        g_instance.attr.attr_common.Logging_collector = true;
        g_instance.pid_cxt.SysLoggerPID = SysLogger_Start();
        StartUDFMaster();
    }
    if (status == STATUS_OK)
        status = ServerLoop();

    /*
     * ServerLoop probably shouldn't ever return, but if it does, close down.
     */
    ExitPostmaster(status != STATUS_OK);

    return 0; /* not reached */
}

/*
 * Compute and check the directory paths to files that are part of the
 * installation (as deduced from the postgres executable's own location)
 */
static void getInstallationPaths(const char* argv0)
{
    DIR* pdir = NULL;

    /* Locate the openGauss executable itself */
    if (find_my_exec(argv0, my_exec_path) < 0) {
        ereport(FATAL, (errmsg("%s: could not locate my own executable path", argv0)));
    }

#ifdef EXEC_BACKEND

    /* Locate executable backend before we change working directory */
    if (find_other_exec(argv0, "gaussdb", PG_BACKEND_VERSIONSTR, t_thrd.proc_cxt.postgres_exec_path) < 0) {
        ereport(FATAL, (errmsg("%s: could not locate matching openGauss executable", argv0)));
    }

#endif

    /*
     * Locate the pkglib directory --- this has to be set early in case we try
     * to load any modules from it in response to postgresql.conf entries.
     */
    get_pkglib_path(my_exec_path, t_thrd.proc_cxt.pkglib_path);

    /*
     * Verify that there's a readable directory there; otherwise the openGauss
     * installation is incomplete or corrupt.  (A typical cause of this
     * failure is that the openGauss executable has been moved or hardlinked to
     * some directory that's not a sibling of the installation lib/
     * directory.)
     */
    pdir = AllocateDir(t_thrd.proc_cxt.pkglib_path);

    if (pdir == NULL) {
        ereport(ERROR,
            (errcode_for_file_access(),
                errmsg("could not open directory \"%s\": %m", t_thrd.proc_cxt.pkglib_path),
                errhint("This may indicate an incomplete PostgreSQL installation, or that the file \"%s\" has been "
                        "moved away from its proper location.",
                    my_exec_path)));
    }

    FreeDir(pdir);

    /*
     * XXX is it worth similarly checking the share/ directory?  If the lib/
     * directory is there, then share/ probably is too.
     */
}

/*
 * Validate the proposed data directory
 */
static void checkDataDir(void)
{
#define BUILD_TAG_START "build_completed.start"
    char path[MAXPGPATH];
    char identFile[MAXPGPATH] = {0};
    FILE* fp = NULL;
    struct stat stat_buf;

    Assert(t_thrd.proc_cxt.DataDir);

    int rc = snprintf_s(identFile, sizeof(identFile), MAXPGPATH - 1, "%s/%s", t_thrd.proc_cxt.DataDir, BUILD_TAG_START);
    securec_check_intval(rc, , );
    if (0 == stat(identFile, &stat_buf)) {
        write_stderr(
            "%s: Uncompleted build is detected, please build again and then start database after its success.\n",
            progname);
        ExitPostmaster(2);
    }

    if (stat(t_thrd.proc_cxt.DataDir, &stat_buf) != 0) {
        if (errno == ENOENT)
            ereport(FATAL,
                (errcode_for_file_access(), errmsg("data directory \"%s\" does not exist", t_thrd.proc_cxt.DataDir)));
        else
            ereport(FATAL,
                (errcode_for_file_access(),
                    errmsg("could not read permissions of directory \"%s\": %m", t_thrd.proc_cxt.DataDir)));
    }

    /* eventual chdir would fail anyway, but let's test ... */
    if (!S_ISDIR(stat_buf.st_mode))
        ereport(FATAL,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("specified data directory \"%s\" is not a directory", t_thrd.proc_cxt.DataDir)));

        /*
         * Check that the directory belongs to my userid; if not, reject.
         *
         * This check is an essential part of the interlock that prevents two
         * postmasters from starting in the same directory (see CreateLockFile()).
         * Do not remove or weaken it.
         *
         * XXX can we safely enable this check on Windows?
         */
#if !defined(WIN32) && !defined(__CYGWIN__)

    if (stat_buf.st_uid != geteuid())
        ereport(FATAL,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("data directory \"%s\" has wrong ownership", t_thrd.proc_cxt.DataDir),
                errhint("The server must be started by the user that owns the data directory.")));

#endif

        /*
         * Check if the directory has group or world access.  If so, reject.
         *
         * It would be possible to allow weaker constraints (for example, allow
         * group access) but we cannot make a general assumption that that is
         * okay; for example there are platforms where nearly all users
         * customarily belong to the same group.  Perhaps this test should be
         * configurable.
         *
         * XXX temporarily suppress check when on Windows, because there may not
         * be proper support for Unix-y file permissions.  Need to think of a
         * reasonable check to apply on Windows.
         */
#if !defined(WIN32) && !defined(__CYGWIN__)

    if (stat_buf.st_mode & (S_IRWXG | S_IRWXO))
        ereport(FATAL,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("data directory \"%s\" has group or world access", t_thrd.proc_cxt.DataDir),
                errdetail("Permissions should be u=rwx (0700).")));

#endif

    /* Look for PG_VERSION before looking for pg_control */
    ValidatePgVersion(t_thrd.proc_cxt.DataDir);

    int ret = snprintf_s(path, sizeof(path), MAXPGPATH - 1, "%s/global/pg_control", t_thrd.proc_cxt.DataDir);
    securec_check_intval(ret, , );
    fp = AllocateFile(path, PG_BINARY_R);

    if (fp == NULL) {
        write_stderr("%s: could not find the database system\n"
                     "Expected to find it in the directory \"%s\",\n"
                     "but could not open file \"%s\": %s\n",
            progname,
            t_thrd.proc_cxt.DataDir,
            path,
            gs_strerror(errno));
        ExitPostmaster(2);
    }

    FreeFile(fp);
}

static void CheckExtremeRtoGUCConflicts(void)
{
    const int minReceiverBufSize = 32 * 1024;
    if ((g_instance.attr.attr_storage.recovery_parse_workers > 1) && IS_DN_DUMMY_STANDYS_MODE()) {
        ereport(WARNING,
            (errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("when starting as dummy_standby mode, we couldn't support extreme rto."),
                errhint("so down extreme rto")));
        g_instance.attr.attr_storage.recovery_parse_workers = 1;
    }

    if ((g_instance.attr.attr_storage.recovery_parse_workers > 1) && 
        g_instance.attr.attr_storage.WalReceiverBufSize < minReceiverBufSize) {
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("when starting extreme rto, wal receiver buf should not smaller than %dMB", 
                    minReceiverBufSize / 1024),
                errhint("recommend config \"wal_receiver_buffer_size=64MB\"")));
    }

#ifndef ENABLE_MULTIPLE_NODES
    if ((g_instance.attr.attr_storage.recovery_parse_workers > 1) && g_instance.attr.attr_storage.EnableHotStandby) {
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("extreme rto could not support hot standby."),
                errhint("Either turn off extreme rto, or turn off hot_standby.")));
    }
#endif
}
static void CheckRecoveryParaConflict()
{
	if (g_instance.attr.attr_storage.max_recovery_parallelism > RECOVERY_PARALLELISM_DEFAULT 
	    && IS_DN_DUMMY_STANDYS_MODE()) {
		ereport(WARNING,
		    (errmsg("when starting as dummy_standby mode, we couldn't support parallel redo, down it")));
		g_instance.attr.attr_storage.max_recovery_parallelism = RECOVERY_PARALLELISM_DEFAULT;
	}
}

static void CheckGUCConflictsMaxConnections()
{
    if (g_instance.attr.attr_network.ReservedBackends >= g_instance.attr.attr_network.MaxConnections) {
        write_stderr("%s: sysadmin_reserved_connections must be less than max_connections\n", progname);
        ExitPostmaster(1);
    }

    if (g_instance.attr.attr_storage.max_wal_senders >= g_instance.attr.attr_network.MaxConnections) {
        write_stderr("%s: max_wal_senders must be less than max_connections\n", progname);
        ExitPostmaster(1);
    }
    return;
}

static void CheckShareStorageConfigConflicts(void)
{
    if (g_instance.attr.attr_storage.xlog_file_path != NULL) {
        if (g_instance.attr.attr_storage.xlog_file_size == 0) {
            ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("configured \"xlog_file_path\" but not configured  \"xlog_file_size\"")));
        }

        if (g_instance.attr.attr_storage.xlog_lock_file_path == NULL) {
            ereport(WARNING, (errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("configured \"xlog_file_path\" but not configured  \"xlog_lock_file_path\"")));
        }
    }
}

/*
 * Check for invalid combinations of GUC settings during starting up.
 */
static void CheckGUCConflicts(void)
{
    CheckGUCConflictsMaxConnections();
    if (dummyStandbyMode && STANDBY_MODE != t_thrd.xlog_cxt.server_mode) {
        write_stderr("%s: dummy standby should be running under standby mode\n", progname);
        ExitPostmaster(1);
    }

    if (u_sess->attr.attr_common.XLogArchiveMode && g_instance.attr.attr_storage.wal_level == WAL_LEVEL_MINIMAL)
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR),
                errmsg(
                    "WAL archival (archive_mode=on) requires wal_level \"archive\", \"hot_standby\" or \"logical\"")));

    if (g_instance.attr.attr_storage.max_wal_senders > 0 && g_instance.attr.attr_storage.wal_level == WAL_LEVEL_MINIMAL)
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("WAL streaming (max_wal_senders > 0) requires wal_level \"archive\", \"hot_standby\" or "
                       "\"logical\"")));

    if (g_instance.attr.attr_storage.EnableHotStandby == true &&
        g_instance.attr.attr_storage.wal_level < WAL_LEVEL_HOT_STANDBY)
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR),
            errmsg("hot standby is not possible because wal_level was not set to \"hot_standby\""),
            errhint("Either set wal_level to \"hot_standby\", or turn off hot_standby.")));

    if ((g_instance.attr.attr_storage.wal_level == WAL_LEVEL_MINIMAL ||
            g_instance.attr.attr_storage.max_wal_senders < 1) &&
        (t_thrd.xlog_cxt.server_mode == PRIMARY_MODE || t_thrd.xlog_cxt.server_mode == PENDING_MODE ||
            t_thrd.xlog_cxt.server_mode == STANDBY_MODE))
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("when starting as dual mode, we must ensure wal_level was not \"minimal\" and max_wal_senders "
                       "was set at least 1")));

    if (u_sess->attr.attr_storage.enable_data_replicate && IS_DN_MULTI_STANDYS_MODE()) {
        /* when init, we should force change the option */
        ereport(LOG, (errmsg("when starting as multi_standby mode, we couldn't support data replicaton.")));
        u_sess->attr.attr_storage.enable_data_replicate = false;
    }

    CheckRecoveryParaConflict();

    if (g_instance.attr.attr_storage.enable_mix_replication &&
        g_instance.attr.attr_storage.MaxSendSize >= g_instance.attr.attr_storage.DataQueueBufSize) {
        write_stderr("%s: the data queue buffer size must be larger than the wal sender max send size for the "
                     "replication data synchronized by the WAL streaming.\n",
            progname);
        ExitPostmaster(1);
    }
    CheckExtremeRtoGUCConflicts();
    CheckShareStorageConfigConflicts();
}

static bool save_backend_variables_for_callback_thread()
{
    Port port;

    /* This entry point passes dummy values for the Port variables */
    int ss_rc = memset_s(&port, sizeof(port), 0, sizeof(port));
    securec_check(ss_rc, "\0", "\0");

    /*
     * Socket 0 may be closed if we do not use it, so we
     * must set socket to invalid socket instead of 0.
     */
    port.sock = PGINVALID_SOCKET;

    return save_backend_variables(&backend_save_para, &port);
}

static bool ObsSlotThreadExist(const char* slotName) 
{
    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        if (g_instance.archive_thread_info.slotName[i] == NULL) {
            continue;
        }
        if (strcmp(g_instance.archive_thread_info.slotName[i], slotName) == 0 && 
            g_instance.archive_thread_info.obsArchPID[i] != 0) {
            return true;
        }
    }

    return false;
}

static char *GetObsSlotName(const List *archiveSlotNames) 
{
    foreach_cell(cell, archiveSlotNames) {
        char *slotName = (char*)lfirst(cell);
        if (slotName == NULL || strlen(slotName) == 0) {
            return NULL;
        }
        if (!ObsSlotThreadExist(slotName) && getArchiveReplicationSlotWithName(slotName) != NULL) {
            return slotName;
        }
    }

    return NULL;
}

static void ArchObsThreadStart(int threadIndex)
{
    List *archiveSlotNames = GetAllArchiveSlotsName();
    if (archiveSlotNames == NIL || archiveSlotNames->length == 0) {
        return;
    }
    char *slotName = GetObsSlotName(archiveSlotNames);
    if (slotName == NULL || strlen(slotName) == 0) {
        list_free_deep(archiveSlotNames);
        return;
    }
    ereport(LOG, (errmsg("pgarch thread need start, create slotName: %s, index: %d", slotName, threadIndex)));
    
    errno_t rc = EOK;
    rc = memcpy_s(g_instance.archive_thread_info.slotName[threadIndex], NAMEDATALEN, slotName, strlen(slotName));
    securec_check(rc, "\0", "\0");

    g_instance.archive_thread_info.obsArchPID[threadIndex] = initialize_util_thread(ARCH,
        g_instance.archive_thread_info.slotName[threadIndex]);
    if (START_BARRIER_CREATOR && pmState == PM_RUN) {
        if (g_instance.archive_thread_info.obsBarrierArchPID[threadIndex] != 0) {
            signal_child(g_instance.archive_thread_info.obsBarrierArchPID[threadIndex], SIGUSR2);
        }
        g_instance.archive_thread_info.obsBarrierArchPID[threadIndex] = 
            initialize_util_thread(BARRIER_ARCH, g_instance.archive_thread_info.slotName[threadIndex]);
    }
    list_free_deep(archiveSlotNames);
}

static void ArchObsThreadShutdown(int threadIndex) 
{
    char *slotName = g_instance.archive_thread_info.slotName[threadIndex];
    if (slotName == NULL || strlen(slotName) == 0 || getArchiveReplicationSlotWithName(slotName) != NULL) {
        return;
    }

    ereport(LOG, (errmsg("pgarch thread need shutdonw, delete slotName: %s, index: %d", slotName, threadIndex)));
    if (g_instance.archive_thread_info.obsArchPID[threadIndex] != 0) {
        signal_child(g_instance.archive_thread_info.obsArchPID[threadIndex], SIGUSR2);
    }
    if (g_instance.archive_thread_info.obsBarrierArchPID[threadIndex] != 0) {
        signal_child(g_instance.archive_thread_info.obsBarrierArchPID[threadIndex], SIGUSR2);
    }
    g_instance.archive_thread_info.obsArchPID[threadIndex] = 0;
    g_instance.archive_thread_info.obsBarrierArchPID[threadIndex] = 0;
    errno_t rc = EOK;
    rc = memset_s(g_instance.archive_thread_info.slotName[threadIndex], NAMEDATALEN, 0, 
        strlen(g_instance.archive_thread_info.slotName[threadIndex]));
    securec_check(rc, "\0", "\0");
}

void ArchObsThreadManage()
{
    volatile int *g_tline = &g_instance.archive_obs_cxt.slot_tline;
    volatile int *l_tline = &t_thrd.arch.slot_tline;
    if (likely(*g_tline == *l_tline)) {
        for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
            if (START_BARRIER_CREATOR && pmState == PM_RUN && g_instance.archive_thread_info.obsArchPID[i] != 0 && 
                g_instance.archive_thread_info.obsBarrierArchPID[i] == 0) {
                g_instance.archive_thread_info.obsBarrierArchPID[i] = 
                    initialize_util_thread(BARRIER_ARCH, g_instance.archive_thread_info.slotName[i]);
            }
        }
        return;
    }

    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        if (g_instance.archive_thread_info.obsArchPID[i] == 0) {
            ArchObsThreadStart(i);
        } else {
            ArchObsThreadShutdown(i);
        }
    }
    SpinLockAcquire(&g_instance.archive_obs_cxt.mutex);
    *l_tline = *g_tline;
    SpinLockRelease(&g_instance.archive_obs_cxt.mutex);
}

/*
 * Main idle loop of postmaster
 */
static int ServerLoop(void)
{
    fd_set readmask;
    int nSockets;
    uint64 this_start_poll_time, last_touch_time, last_start_loop_time, last_start_poll_time;
    uint64 current_poll_time = 0;
    uint64 last_poll_time = 0;
    /* Database Security: Support database audit */
    char details[PGAUDIT_MAXLENGTH] = {0};
    bool threadPoolActivated = g_instance.attr.attr_common.enable_thread_pool;

    /* make sure gaussdb can receive request */
    DISABLE_MEMORY_PROTECT();

#ifdef HAVE_POLL
    struct pollfd ufds[MAXLISTEN * 2 + 1];
#endif

    FD_ZERO(&readmask);
    last_start_loop_time = last_touch_time = last_start_poll_time = mc_timers_us();

#ifdef HAVE_POLL
    nSockets = initPollfd(ufds);
#else
    nSockets = initMasks(&readmask);
#endif

    /* for rpc function call */
    if (!save_backend_variables_for_callback_thread()) {
        ereport(LOG, (errmsg("save_backend_variables_for_callback_thread error")));
        return STATUS_ERROR;
    }
    ereport(LOG, (errmsg("start create thread!")));

    /* Init backend thread pool */
    if (threadPoolActivated) {
        bool enableNumaDistribute = (g_instance.shmem_cxt.numaNodeNum > 1);
        g_threadPoolControler->Init(enableNumaDistribute);
    }
    ereport(LOG, (errmsg("create thread end!")));

    for (;;) {
        fd_set rmask;
        int selres;

        if (t_thrd.postmaster_cxt.HaShmData->current_mode != NORMAL_MODE || IS_SHARED_STORAGE_MODE) {
            check_and_reset_ha_listen_port();

#ifdef HAVE_POLL
            nSockets = initPollfd(ufds);
#else
            nSockets = initMasks(&readmask);
#endif
        }

        /*
         * Wait for a connection request to arrive.
         *
         * We wait at most one minute, to ensure that the other background
         * tasks handled below get done even when no requests are arriving.
         *
         * If we are in PM_WAIT_DEAD_END state, then we don't want to accept
         * any new connections, so we don't call select() at all; just sleep
         * for a little bit with signals unblocked.
         */
        int ss_rc = memcpy_s((char*)&rmask, sizeof(fd_set), (char*)&readmask, sizeof(fd_set));
        securec_check(ss_rc, "\0", "\0");

        gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
        (void)gs_signal_unblock_sigusr2();

        this_start_poll_time = mc_timers_us();
        if ((this_start_poll_time - last_start_loop_time) != 0) {
            gs_set_libcomm_used_rate(
                (this_start_poll_time - last_start_poll_time) * 100 / (this_start_poll_time - last_start_loop_time));
        }

        /*
         * check how many seconds has took in this loop
         * Detail: If work time > PM_BUSY_ALARM_USED_US, we think the ServerLoop is busy.
         */
        if (this_start_poll_time - last_start_poll_time > PM_BUSY_ALARM_USED_US) {
            /* Prevent interrupts while cleaning up */
            HOLD_INTERRUPTS();
            ereport(WARNING, (errmsg("postmaster is busy, this cycle used %lu seconds.",
                (this_start_poll_time - last_start_loop_time) / PM_BUSY_ALARM_US)));
            /* Now we can allow interrupts again */
            RESUME_INTERRUPTS();
        }
        last_start_loop_time = this_start_poll_time;

        /*
         * Touch the socket and lock file every 58 minutes, to ensure that
         * they are not removed by overzealous /tmp-cleaning tasks.  We assume
         * no one runs cleaners with cutoff times of less than an hour ...
         */
        if (this_start_poll_time - last_touch_time >= PM_POLL_TIMEOUT_MINUTE) {
            TouchSocketFile();
            TouchSocketLockFile();
            last_touch_time = this_start_poll_time;
        }

        if (pmState == PM_WAIT_DEAD_END) {
            pg_usleep(100000L); /* 100 msec seems reasonable */
            selres = 0;
        } else {
            /* must set timeout each time; some OSes change it! */
            struct timeval timeout;

            timeout.tv_sec = PM_POLL_TIMEOUT_SECOND;
            timeout.tv_usec = 0;

            ereport(DEBUG4, (errmsg("postmaster poll start, fd nums:%d", nSockets)));
#ifdef HAVE_POLL
            selres = comm_poll(ufds, nSockets, timeout.tv_sec * 1000); /* CommProxy Support */
            last_start_poll_time = mc_timers_us();
#else
            int ss_rc = memcpy_s((char*)&rmask, sizeof(fd_set), (char*)&readmask, sizeof(fd_set));
            securec_check(ss_rc, "\0", "\0");
            selres = comm_select(nSockets, &rmask, NULL, NULL, &timeout);
            last_start_poll_time = mc_timers_us();
#endif
        }

        /*
         * Block all signals until we wait again.  (This makes it safe for our
         * signal handlers to do nontrivial work.)
         */
        gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
        gs_signal_block_sigusr2();

        /* Now check the select() result */
        if (selres < 0) {
            if (errno != EINTR && errno != EWOULDBLOCK) {
                ereport(LOG, (errcode_for_socket_access(), errmsg("select()/poll() failed in postmaster: %m")));
                return STATUS_ERROR;
            }
        }

        /*
         * New connection pending on any of our sockets? If so, fork a child
         * process to deal with it.
         */
        if (selres > 0) {
            int i;

            for (i = 0; i < (MAXLISTEN * 2); i++) {
                if (ufds[i].fd == PGINVALID_SOCKET)
                    break;

#ifdef HAVE_POLL

                if (ufds[i].revents & POLLIN) {
                    ufds[i].revents = 0;
#else

                if (FD_ISSET(t_thrd.postmaster_cxt.ListenSocket[i], &rmask)) {
#endif
                    Port* port = NULL;

                    ufds[i].revents = 0;

                    if (IS_FD_TO_RECV_GSSOCK(ufds[i].fd)) {
                        port = ConnCreateToRecvGssock(ufds, i, &nSockets);
                    } else {
                        port = ConnCreate(ufds[i].fd);
                    }

                    if (port != NULL) {
                        int result = STATUS_OK;
                        bool isConnectHaPort =
                            (i < MAXLISTEN) && (t_thrd.postmaster_cxt.listen_sock_type[i] == HA_LISTEN_SOCKET);
                        /*
                         * Since at present, HA only uses TCP sockets, we can directly compare
                         * the corresponding enty in t_thrd.postmaster_cxt.listen_sock_type, even
                         * though ufds are not one-to-one mapped to tcp and sctp socket array.
                         * If HA adopts STCP sockets later, we will need to maintain socket type
                         * array for ufds in initPollfd.
                         */
                        if (threadPoolActivated && !isConnectHaPort) {
                            result = g_threadPoolControler->DispatchSession(port);
                        } else {
                            result = BackendStartup(port, isConnectHaPort);
                        }

                        if (result != STATUS_OK) {
                            if (port->is_logic_conn) {
                                gs_close_gsocket(&port->gs_sock);
                            } else {
                                comm_closesocket(port->sock);  /* CommProxy Support */
                            }
                        }

                        /* do not free port for unix domain conn from receiver flow control */
                        if (!IS_FD_TO_RECV_GSSOCK(ufds[i].fd))
                            /*
                             * We no longer need the open socket or port structure
                             * in this process
                             */
                            ConnFree((void*)port);
                    }
                }
            }
        }

        ereport(DEBUG4, (errmsg("postmaster poll event process end.")));

        /*
         * If the AioCompleters have not been started start them.
         * These should remain run indefinitely.
         */
        ADIO_RUN()
        {
            if (!g_instance.pid_cxt.AioCompleterStarted && !dummyStandbyMode) {
                int aioStartErr = 0;
                if ((aioStartErr = AioCompltrStart()) == 0) {
                    g_instance.pid_cxt.AioCompleterStarted = 1;
                } else {
                    ereport(LOG, (errmsg_internal("Cannot start AIO completer threads error=%d", aioStartErr)));
                    /*
                     * If we failed to fork a aio process, just shut down.
                     * Any required cleanup will happen at next restart. We
                     * set g_instance.fatal_error so that an "abnormal shutdown" message
                     * gets logged when we exit.
                     */
                    g_instance.fatal_error = true;
                    HandleChildCrash(g_instance.pid_cxt.AioCompleterStarted, 1, "AIO process");
                }
            }
        }
        ADIO_END();

        if (threadPoolActivated && (pmState == PM_RUN || pmState == PM_HOT_STANDBY))
            g_threadPoolControler->AddWorkerIfNecessary();

        /* If we have lost the log collector, try to start a new one */
        if (g_instance.pid_cxt.SysLoggerPID == 0 && g_instance.attr.attr_common.Logging_collector)
            g_instance.pid_cxt.SysLoggerPID = SysLogger_Start();

        /*  start auditor process */
        /* If we have lost the audit collector, try to start a new one */

#ifndef ENABLE_MULTIPLE_NODES
        if (g_instance.pid_cxt.PgAuditPID != NULL && u_sess->attr.attr_security.Audit_enabled &&
            (pmState == PM_RUN || pmState == PM_HOT_STANDBY) && !dummyStandbyMode) {
            pgaudit_start_all();
        }

        if (g_instance.comm_cxt.isNeedChangeRole) {
            char role[MAXPGPATH] = {0};
            if (!CheckSignalByFile(ChangeRoleFile, role, MAXPGPATH)) {
                ereport(WARNING, (errmsg("Could not read changerole file")));
                g_instance.comm_cxt.isNeedChangeRole = false;
            }
            handle_change_role_signal(role);
            g_instance.comm_cxt.isNeedChangeRole = false;
        }
#else
        if (g_instance.pid_cxt.PgAuditPID != NULL && u_sess->attr.attr_security.Audit_enabled && pmState == PM_RUN &&
            !dummyStandbyMode) {
            pgaudit_start_all();
        }
#endif
        /* If u_sess->attr.attr_security.Audit_enabled is set to false, terminate auditor process. */
        if (g_instance.pid_cxt.PgAuditPID != NULL && !u_sess->attr.attr_security.Audit_enabled) {
            pgaudit_stop_all();
        }

        if (g_instance.pid_cxt.AlarmCheckerPID == 0 && !dummyStandbyMode)
            g_instance.pid_cxt.AlarmCheckerPID = startAlarmChecker();

        /* If we have lost the reaper backend thread, try to start a new one */
        if (g_instance.pid_cxt.ReaperBackendPID == 0)
            g_instance.pid_cxt.ReaperBackendPID = initialize_util_thread(REAPER);

        if ((pmState == PM_RUN || t_thrd.xlog_cxt.is_hadr_main_standby) &&
            g_instance.pid_cxt.sharedStorageXlogCopyThreadPID == 0 && !dummyStandbyMode &&
            g_instance.attr.attr_storage.xlog_file_path != NULL) {
            g_instance.pid_cxt.sharedStorageXlogCopyThreadPID = initialize_util_thread(SHARE_STORAGE_XLOG_COPYER);
        }

#ifdef ENABLE_MULTIPLE_NODES
        /* when execuating xlog redo in standby cluster,
          * pmState is PM_HOT_STANDBY, neither PM_RECOVERY nor PM_RUN
          */
        if (pmState == PM_HOT_STANDBY && g_instance.pid_cxt.BarrierPreParsePID == 0 &&
            !dummyStandbyMode && IS_DISASTER_RECOVER_MODE) {
            g_instance.pid_cxt.BarrierPreParsePID = initialize_util_thread(BARRIER_PREPARSE);
        }
#endif
        /*
         * If the startup thread is running, need check the page repair thread.
         */
        if (g_instance.pid_cxt.PageRepairPID == 0 && (pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY)) {
            g_instance.pid_cxt.PageRepairPID = initialize_util_thread(PAGEREPAIR_THREAD);
        }
        /*
         * If no background writer process is running, and we are not in a
         * state that prevents it, start one.  It doesn't matter if this
         * fails, we'll just try again later.  Likewise for the checkpointer.
         */
        if (pmState == PM_RUN || pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY) {
            if (g_instance.pid_cxt.CheckpointerPID == 0 && !dummyStandbyMode)
                g_instance.pid_cxt.CheckpointerPID = initialize_util_thread(CHECKPOINT_THREAD);

            if (g_instance.pid_cxt.BgWriterPID == 0 && !dummyStandbyMode && !ENABLE_INCRE_CKPT) {
                g_instance.pid_cxt.BgWriterPID = initialize_util_thread(BGWRITER);
            }

            if (g_instance.pid_cxt.SpBgWriterPID == 0 && !dummyStandbyMode) {
                g_instance.pid_cxt.SpBgWriterPID = initialize_util_thread(SPBGWRITER);
            }

            if (g_instance.pid_cxt.CBMWriterPID == 0 && !dummyStandbyMode &&
                u_sess->attr.attr_storage.enable_cbm_tracking)
                g_instance.pid_cxt.CBMWriterPID = initialize_util_thread(CBMWRITER);

            if (!dummyStandbyMode && ENABLE_INCRE_CKPT) {
                for (int i = 0; i < g_instance.ckpt_cxt_ctl->pgwr_procs.num; i++) {
                    if (g_instance.pid_cxt.PageWriterPID[i] == 0) {
                        g_instance.pid_cxt.PageWriterPID[i] = initialize_util_thread(PAGEWRITER_THREAD);
                    }
                }
            }
        }

        /*
         * Likewise, if we have lost the walwriter process, try to start a new
         * one.  But this is needed only in normal operation (else we cannot
         * be writing any new WAL).
         */
        if (g_instance.pid_cxt.WalWriterPID == 0 && pmState == PM_RUN) {
            g_instance.pid_cxt.WalWriterPID = initialize_util_thread(WALWRITER);
        }

        if (g_instance.pid_cxt.WalWriterAuxiliaryPID == 0 && (pmState == PM_RUN ||
            ((pmState == PM_HOT_STANDBY || pmState == PM_RECOVERY) && g_instance.pid_cxt.WalRcvWriterPID != 0 &&
            t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE))) {
            g_instance.pid_cxt.WalWriterAuxiliaryPID = initialize_util_thread(WALWRITERAUXILIARY);
            ereport(LOG,
                (errmsg("ServerLoop create WalWriterAuxiliary(%lu) for pmState:%u, ServerMode:%u.",
                    g_instance.pid_cxt.WalWriterAuxiliaryPID, pmState, t_thrd.postmaster_cxt.HaShmData->current_mode)));
        }

        /*
         * let cbm writer thread exit if enable_cbm_track gus is switched off
         */
        if (!u_sess->attr.attr_storage.enable_cbm_tracking && g_instance.pid_cxt.CBMWriterPID != 0 &&
            pmState == PM_RUN) {
            ereport(LOG,
                (errmsg("stop cbm writer thread because enable_cbm_tracking is switched off, "
                        "cbm writer thread pid=%lu",
                    g_instance.pid_cxt.CBMWriterPID)));
            signal_child(g_instance.pid_cxt.CBMWriterPID, SIGTERM);
        }

        /*
         * If we have lost the autovacuum launcher, try to start a new one. We
         * don't want autovacuum to run in binary upgrade mode because
         * autovacuum might update relfrozenxid64 for empty tables before the
         * physical files are put in place.
         */
        if (!u_sess->proc_cxt.IsBinaryUpgrade && g_instance.pid_cxt.AutoVacPID == 0 &&
            (AutoVacuumingActive() || t_thrd.postmaster_cxt.start_autovac_launcher) && pmState == PM_RUN &&
            !dummyStandbyMode && u_sess->attr.attr_common.upgrade_mode != 1) {
            g_instance.pid_cxt.AutoVacPID = initialize_util_thread(AUTOVACUUM_LAUNCHER);

            if (g_instance.pid_cxt.AutoVacPID != 0)
                t_thrd.postmaster_cxt.start_autovac_launcher = false; /* signal processed */
        }

        /*
        * Start the Undo launcher thread if we need to.
        */
        if (g_instance.attr.attr_storage.enable_ustore &&
            g_instance.pid_cxt.UndoLauncherPID == 0 &&
            pmState == PM_RUN && !dummyStandbyMode) {
            g_instance.pid_cxt.UndoLauncherPID = initialize_util_thread(UNDO_LAUNCHER);
        }

        if (g_instance.attr.attr_storage.enable_ustore &&
            g_instance.pid_cxt.GlobalStatsPID == 0 && 
            pmState == PM_RUN && !dummyStandbyMode) {
            g_instance.pid_cxt.GlobalStatsPID = initialize_util_thread(GLOBALSTATS_THREAD);
        }
#if !defined(ENABLE_MULTIPLE_NODES) && !defined(ENABLE_LITE_MODE)
        if (u_sess->attr.attr_common.upgrade_mode == 0 && g_instance.pid_cxt.ApplyLauncerPID == 0 &&
            pmState == PM_RUN && !dummyStandbyMode) {
            g_instance.pid_cxt.ApplyLauncerPID = initialize_util_thread(APPLY_LAUNCHER);
        }
#endif

        /*
         * If we have lost the job scheduler, try to start a new one.
         *
         * Before GRAND VERSION NUM 81000, we do not support scheduled job.
         */
        if (g_instance.pid_cxt.PgJobSchdPID == 0 && pmState == PM_RUN &&
            (g_instance.attr.attr_sql.job_queue_processes || t_thrd.postmaster_cxt.start_job_scheduler) &&
            u_sess->attr.attr_common.upgrade_mode != 1) {
            g_instance.pid_cxt.PgJobSchdPID = initialize_util_thread(JOB_SCHEDULER);

            if (g_instance.pid_cxt.PgJobSchdPID != 0) {
                t_thrd.postmaster_cxt.start_job_scheduler = false; /* signal processed */
                ereport(LOG, (errmsg("job scheduler started, pid=%lu", g_instance.pid_cxt.PgJobSchdPID)));
            }
        }
#ifdef ENABLE_MULTIPLE_NODES
        if ((IS_PGXC_COORDINATOR) && g_instance.pid_cxt.CommPoolerCleanPID == 0 && pmState == PM_RUN &&
            u_sess->attr.attr_common.upgrade_mode != 1) {
            StartPoolCleaner();
        }
#endif
        /* If g_instance.attr.attr_sql.job_queue_processes set to 0, terminate jobscheduler thread. */
        if (g_instance.pid_cxt.PgJobSchdPID != 0 &&
            !g_instance.attr.attr_sql.job_queue_processes) {
            signal_child(g_instance.pid_cxt.PgJobSchdPID, SIGTERM);
        }

#ifndef ENABLE_LITE_MODE
        /* If we have lost the barrier creator thread, try to start a new one */
        if (START_BARRIER_CREATOR && g_instance.pid_cxt.BarrierCreatorPID == 0 && pmState == PM_RUN &&
            (g_instance.archive_obs_cxt.archive_slot_num != 0 || START_AUTO_CSN_BARRIER)) {
            g_instance.pid_cxt.BarrierCreatorPID = initialize_util_thread(BARRIER_CREATOR);
        }
#endif

        /* If we have lost the archiver, try to start a new one */
        if (!dummyStandbyMode) {
            if (g_instance.pid_cxt.PgArchPID == 0 && pmState == PM_RUN && XLogArchivingActive() && 
                (XLogArchiveCommandSet() || XLogArchiveDestSet())) {
                g_instance.pid_cxt.PgArchPID = pgarch_start();
            } else if (g_instance.archive_thread_info.obsArchPID != NULL && 
                (pmState == PM_RUN || pmState == PM_HOT_STANDBY)) {
                ArchObsThreadManage();
            }
        }

        /* If we have lost the stats collector, try to start a new one */
        if (g_instance.pid_cxt.PgStatPID == 0 && (pmState == PM_RUN || pmState == PM_HOT_STANDBY) && !dummyStandbyMode)
            g_instance.pid_cxt.PgStatPID = pgstat_start();

        /* If we have lost the snapshot capturer, try to start a new one */
        if (ENABLE_TCAP_VERSION && (g_instance.role == VSINGLENODE) && pmState == PM_RUN && g_instance.pid_cxt.TxnSnapCapturerPID == 0 && !dummyStandbyMode)
            g_instance.pid_cxt.TxnSnapCapturerPID = StartTxnSnapCapturer();

        /* If we have lost the rbcleaner, try to start a new one */
        if (ENABLE_TCAP_RECYCLEBIN && (g_instance.role == VSINGLENODE) && pmState == PM_RUN && g_instance.pid_cxt.RbCleanrPID == 0 && !dummyStandbyMode)
            g_instance.pid_cxt.RbCleanrPID = StartRbCleaner();

        /* If we have lost the stats collector, try to start a new one */
        if ((IS_PGXC_COORDINATOR || (g_instance.role == VSINGLENODE)) && g_instance.pid_cxt.SnapshotPID == 0 &&
            u_sess->attr.attr_common.enable_wdr_snapshot && pmState == PM_RUN)
            g_instance.pid_cxt.SnapshotPID = snapshot_start();

        if (ENABLE_ASP && g_instance.pid_cxt.AshPID == 0 && pmState == PM_RUN && !dummyStandbyMode)
            g_instance.pid_cxt.AshPID = initialize_util_thread(ASH_WORKER);

        /* If we have lost the full sql flush thread, try to start a new one */
        if (ENABLE_STATEMENT_TRACK && g_instance.pid_cxt.StatementPID == 0 && pmState == PM_RUN)
            g_instance.pid_cxt.StatementPID = initialize_util_thread(TRACK_STMT_WORKER);

        if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && u_sess->attr.attr_common.enable_instr_rt_percentile &&
            g_instance.pid_cxt.PercentilePID == 0 &&
            pmState == PM_RUN)
            g_instance.pid_cxt.PercentilePID = initialize_util_thread(PERCENTILE_WORKER);

        /* if workload manager is off, we still use this thread to build user hash table */
        if ((ENABLE_WORKLOAD_CONTROL || !WLMIsInfoInit()) && g_instance.pid_cxt.WLMCollectPID == 0 &&
            pmState == PM_RUN && !dummyStandbyMode)
            g_instance.pid_cxt.WLMCollectPID = initialize_util_thread(WLM_WORKER);

        if (ENABLE_WORKLOAD_CONTROL && (g_instance.pid_cxt.WLMMonitorPID == 0) && (pmState == PM_RUN) &&
            !dummyStandbyMode)
            g_instance.pid_cxt.WLMMonitorPID = initialize_util_thread(WLM_MONITOR);

        if (ENABLE_WORKLOAD_CONTROL && (g_instance.pid_cxt.WLMArbiterPID == 0) && (pmState == PM_RUN) &&
            !dummyStandbyMode)
            g_instance.pid_cxt.WLMArbiterPID = initialize_util_thread(WLM_ARBITER);

        if (IS_PGXC_COORDINATOR && g_instance.attr.attr_sql.max_resource_package &&
            (g_instance.pid_cxt.CPMonitorPID == 0) && (pmState == PM_RUN) && !dummyStandbyMode)
            g_instance.pid_cxt.CPMonitorPID = initialize_util_thread(WLM_CPMONITOR);

#ifndef ENABLE_LITE_MODE
        /* If we have lost the twophase cleaner, try to start a new one */
        if (
#ifdef ENABLE_MULTIPLE_NODES
            IS_PGXC_COORDINATOR &&
#else
            (t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE ||
             t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE) &&
#endif
            u_sess->attr.attr_common.upgrade_mode != 1 &&
            g_instance.pid_cxt.TwoPhaseCleanerPID == 0 && pmState == PM_RUN)
            g_instance.pid_cxt.TwoPhaseCleanerPID = initialize_util_thread(TWOPASECLEANER);

        /* If we have lost the LWLock monitor, try to start a new one */
        if (g_instance.pid_cxt.FaultMonitorPID == 0 && pmState == PM_RUN)
            g_instance.pid_cxt.FaultMonitorPID = initialize_util_thread(FAULTMONITOR);
#endif
        /* If we have lost the heartbeat service, try to start a new one */
        if (NeedHeartbeat())
            g_instance.pid_cxt.HeartbeatPID = initialize_util_thread(HEARTBEAT);

        /* If we have lost the csnmin sync thread, try to start a new one */
        if (GTM_LITE_CN && g_instance.pid_cxt.CsnminSyncPID == 0 && pmState == PM_RUN) {
            g_instance.pid_cxt.CsnminSyncPID = initialize_util_thread(CSNMIN_SYNC);
        }
        if (IS_CN_DISASTER_RECOVER_MODE && g_instance.pid_cxt.CsnminSyncPID == 0 && pmState == PM_HOT_STANDBY) {
            g_instance.pid_cxt.CsnminSyncPID = initialize_util_thread(CSNMIN_SYNC);
        }

        if (g_instance.attr.attr_storage.enable_ustore &&
            g_instance.pid_cxt.UndoRecyclerPID == 0 &&
            pmState == PM_RUN) {
            g_instance.pid_cxt.UndoRecyclerPID = initialize_util_thread(UNDO_RECYCLER);
        }

        if (g_instance.attr.attr_storage.enable_ustore &&
            g_instance.pid_cxt.GlobalStatsPID == 0 &&
            pmState == PM_RUN) {
            g_instance.pid_cxt.GlobalStatsPID = initialize_util_thread(GLOBALSTATS_THREAD);
        }

        /* If we need to signal the autovacuum launcher, do so now */
        if (t_thrd.postmaster_cxt.avlauncher_needs_signal) {
            t_thrd.postmaster_cxt.avlauncher_needs_signal = false;

            if (g_instance.pid_cxt.AutoVacPID != 0)
                gs_signal_send(g_instance.pid_cxt.AutoVacPID, SIGUSR2);
        }

#ifdef ENABLE_MULTIPLE_NODES
        if (IS_PGXC_DATANODE && g_instance.attr.attr_common.enable_tsdb &&
            g_instance.pid_cxt.TsCompactionPID == 0 && pmState == PM_RUN &&
            u_sess->attr.attr_common.enable_ts_compaction) {
            g_instance.pid_cxt.TsCompactionPID = initialize_util_thread(TS_COMPACTION);
        }

        if (IS_PGXC_DATANODE && g_instance.attr.attr_common.enable_tsdb &&
            u_sess->attr.attr_common.enable_ts_compaction && pmState == PM_RUN &&
            g_instance.pid_cxt.TsCompactionAuxiliaryPID == 0) {
            g_instance.pid_cxt.TsCompactionAuxiliaryPID = initialize_util_thread(TS_COMPACTION_AUXILIAY);
        }
#endif   /* ENABLE_MULTIPLE_NODES */
        /* TDE cache timer checking */
        if (g_instance.attr.attr_security.enable_tde && IS_PGXC_DATANODE) {
            /* TDE cache watchdog */
            current_poll_time = mc_timers_us();
            if ((current_poll_time - last_poll_time) >= PM_POLL_TIMEOUT_MINUTE) {
                TDE::TDEKeyStorage::get_instance().cache_watch_dog();
                last_poll_time = current_poll_time;
            }
        }
        /* If job worker failed to run, postmaster need send signal SIGUSR2 to job scheduler thread. */
        if (t_thrd.postmaster_cxt.jobscheduler_needs_signal) {
            t_thrd.postmaster_cxt.jobscheduler_needs_signal = false;
            if (g_instance.pid_cxt.PgJobSchdPID != 0) {
                gs_signal_send(g_instance.pid_cxt.PgJobSchdPID, SIGUSR2);
            }
        }

        /* Database Security: Support database audit */
        if (pmState == PM_RUN) {
            if (t_thrd.postmaster_cxt.audit_primary_failover) {
                int rcs = snprintf_s(details,
                    sizeof(details),
                    sizeof(details) - 1,
                    "the standby do failover success,now it is primary!");
                securec_check_ss(rcs, "", "");
                pgaudit_system_switchover_ok(details);
                t_thrd.postmaster_cxt.audit_primary_failover = false;
            }
            if (t_thrd.postmaster_cxt.audit_standby_switchover) {
                int rcs = snprintf_s(details,
                    sizeof(details),
                    sizeof(details) - 1,
                    "the standby do switchover success,now it is primary!");
                securec_check_ss(rcs, "", "");
                pgaudit_system_switchover_ok(details);
                t_thrd.postmaster_cxt.audit_standby_switchover = false;
            }
        }
#ifdef ENABLE_MULTIPLE_NODES
        if (PMstateIsRun()) {
            (void)streaming_backend_manager(STREAMING_BACKEND_INIT);
        }
#endif   /* ENABLE_MULTIPLE_NODES */
    }
}

/*
 * Initialise the ufds for poll() for the ports we are listening on.
 * Return the number of sockets to listen on.
 */
#ifdef HAVE_POLL
static int initPollfd(struct pollfd* ufds)
{
    int i, cnt = 0;
    int fd;

    /* set default value for all pollfds */
    for (i = 0; i < MAXLISTEN * 2 + 1; i++) {
        ufds[i].fd = PGINVALID_SOCKET;
        ufds[cnt].events = 0;
    }

    for (i = 0; i < MAXLISTEN; i++) {
        fd = t_thrd.postmaster_cxt.ListenSocket[i];

        if (fd == PGINVALID_SOCKET)
            break;

        ufds[cnt].fd = fd;
        ufds[cnt].events = POLLIN | POLLPRI;
        cnt++;
    }
    if (t_thrd.postmaster_cxt.sock_for_libcomm != PGINVALID_SOCKET) {
        ufds[cnt].fd = t_thrd.postmaster_cxt.sock_for_libcomm;
        ufds[cnt].events = POLLIN | POLLPRI;
        cnt++;
    }
    return cnt;
}
#else
/*
 * Initialise the masks for select() for the ports we are listening on.
 * Return the number of sockets to listen on.
 */
static int initMasks(fd_set* rmask)
{
    int maxsock = -1;
    int i;

    FD_ZERO(rmask);

    for (i = 0; i < MAXLISTEN; i++) {
        int fd = t_thrd.postmaster_cxt.ListenSocket[i];

        if (fd == PGINVALID_SOCKET) {
            continue;
        }

        FD_SET(fd, rmask);

        if (fd > maxsock) {
            maxsock = fd;
        }
    }

    return maxsock + 1;
}
#endif  // end of HAVE_POLL

/*
 * Read a client's startup packet and do something according to it.
 *
 * Returns STATUS_OK or STATUS_ERROR, or might call ereport(FATAL) and
 * not return at all.
 *
 * (Note that ereport(FATAL) stuff is sent to the client, so only use it
 * if that's what you want.  Return STATUS_ERROR if you don't want to
 * send anything to the client, which would typically be appropriate
 * if we detect a communications failure.)
 */
int ProcessStartupPacket(Port* port, bool SSLdone)
{
    const int tvFactor = 5;
    int32 len;
    void* buf = NULL;
    ProtocolVersion proto;
    MemoryContext oldcontext;
    volatile HaShmemData* hashmdata = t_thrd.postmaster_cxt.HaShmData;
    bool isMaintenanceConnection = false;
    bool clientIsGsql = false;
    bool clientIsCmAgent = false;
    bool clientIsGsClean = false;
    bool clientIsOM = false;
    bool clientIsWDRXdb = false;
    bool clientIsRemoteRead = false;
    bool findProtoVer = false;
    int elevel = (IS_THREAD_POOL_WORKER ? ERROR : FATAL);
    struct timeval tv = {tvFactor * u_sess->attr.attr_network.PoolerConnectTimeout, 0};
    struct timeval oldTv = {0, 0};
    socklen_t oldTvLen = sizeof(oldTv);
    bool isTvSeted = false;
    bool clientIsAutonomousTransaction = false;
    bool clientIsLocalHadrWalrcv = false;
    CHECK_FOR_PROCDIEPENDING();

    /* Set recv timeout on coordinator in case of connected from external application */
    if (IS_PGXC_COORDINATOR && !is_cluster_internal_IP(*(struct sockaddr*)&port->raddr.addr)) {
        if (comm_getsockopt(port->sock, SOL_SOCKET, SO_RCVTIMEO, &oldTv, &oldTvLen)) {
            ereport(LOG, (errmsg("getsockopt(SO_RCVTIMEO) failed: %m")));
            return STATUS_ERROR;
        }
        
        if (comm_setsockopt(port->sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(struct timeval)) < 0) {
            ereport(LOG, (errmsg("setsockopt(SO_RCVTIMEO) failed: %m")));
            return STATUS_ERROR;
        }

        isTvSeted = true;
    }

    if (pq_getbytes((char*)&len, 4) == EOF) {
        /*
         * EOF after SSLdone probably means the client didn't like our
         * response to NEGOTIATE_SSL_CODE.	That's not an error condition, so
         * don't clutter the log with a complaint.
         */
        if (!SSLdone)
            ereport(DEBUG1, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("incomplete startup packet")));

        return STATUS_ERROR;
    }

    len = ntohl(len);
    len -= 4;

    if (len < (int32)sizeof(ProtocolVersion) || len > MAX_STARTUP_PACKET_LENGTH) {
        ereport(COMMERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid length of startup packet")));
        return STATUS_ERROR;
    }

    /*
     * Allocate at least the size of an old-style startup packet, plus one
     * extra byte, and make sure all are zeroes.  This ensures we will have
     * null termination of all strings, in both fixed- and variable-length
     * packet layouts.
     */
    if (len <= (int32)sizeof(StartupPacket))
        buf = palloc0(sizeof(StartupPacket) + 1);
    else
        buf = palloc0(len + 1);

    if (pq_getbytes((char*)buf, len) == EOF) {
        ereport(COMMERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                    errmsg("incomplete startup packet, remote_host[%s], remote_port[%s].",
                        u_sess->proc_cxt.MyProcPort->remote_host,
                        u_sess->proc_cxt.MyProcPort->remote_port)));
        return STATUS_ERROR;
    }

    /*
     * If we're going to reject the connection due to database state, say so
     * now instead of wasting cycles on an authentication exchange. (This also
     * allows a pg_ping utility to be written.)
     */
    switch (port->canAcceptConnections) {
        case CAC_STARTUP:
            ereport(elevel, (errcode(ERRCODE_CANNOT_CONNECT_NOW), errmsg("the database system is starting up")));
            break;

        case CAC_SHUTDOWN:
            ereport(elevel, (errcode(ERRCODE_CANNOT_CONNECT_NOW), errmsg("the database system is shutting down")));
            break;

        case CAC_RECOVERY:
            ereport(elevel, (errcode(ERRCODE_CANNOT_CONNECT_NOW), errmsg("the database system is in recovery mode")));
            break;

        case CAC_TOOMANY:
            ereport(elevel, (errcode(ERRCODE_TOO_MANY_CONNECTIONS), errmsg("sorry, too many clients already")));
            break;

        case CAC_WAITBACKUP:
            /* OK for now, will check in InitPostgres */
            break;

        case CAC_OK:
            break;

        default:
            break;
    }

    /*
     * The first field is either a protocol version number or a special
     * request code.
     */
    port->proto = proto = ntohl(*((ProtocolVersion*)buf));

    if (proto == CANCEL_REQUEST_CODE) {
        /*
         * Mark it is a temp thread which will exit quickly itself.
         * Then PM need not wait and retry to send SIGTERM signal to it.
         * We can not mark it as temp thread in thread pool mode, because
         * the thread will be reused later.
         */
        if (!ENABLE_THREAD_POOL)
            MarkPostmasterTempBackend();

        processCancelRequest(port, buf);
        /* Not really an error, but we don't want to proceed further */
        return STATUS_ERROR;
    } else if (proto == STOP_REQUEST_CODE) {
        processStopRequest(port, buf);

        return STATUS_ERROR;
    }

    if (proto == NEGOTIATE_SSL_CODE && !SSLdone) {
        char SSLok;

#ifdef USE_SSL

        /* No SSL when disabled or on Unix sockets */
        if (!g_instance.attr.attr_security.EnableSSL || IS_AF_UNIX(port->laddr.addr.ss_family))
            SSLok = 'N';
        else
            SSLok = 'S'; /* Support for SSL */

#else
        SSLok = 'N'; /* No support for SSL */
#endif

    retry1:
        errno = 0;
        if (comm_send(port->sock, &SSLok, 1, 0) <= 0) {
            if (errno == EINTR)
                goto retry1; /* if interrupted, just retry */

            ereport(COMMERROR, (errcode_for_socket_access(), errmsg("failed to send SSL negotiation response: %m")));
            return STATUS_ERROR; /* close the connection */
        }

#ifdef USE_SSL

        if (SSLok == 'S' && secure_open_server(port) == -1) {
            return STATUS_ERROR;
        }

#endif
        /* regular startup packet, cancel, etc packet should follow... */
        /* but not another SSL negotiation request */
        return ProcessStartupPacket(port, true);
    }

    /* Could add additional special packet types here */

    /*
     * Set FrontendProtocol now so that ereport() knows what format to send if
     * we fail during startup.
     */
    FrontendProtocol = proto;

    /* Check we can handle the protocol the frontend is using. */
    for (size_t i = 0; i < sizeof(protoVersionList) / sizeof(protoVersionList[0]); i++) {
        if (PG_PROTOCOL_MAJOR(proto) == protoVersionList[i][0] && PG_PROTOCOL_MINOR(proto) == protoVersionList[i][1]) {
            findProtoVer = true;
            break;
        }
    }

    if (!findProtoVer) {
        ereport(elevel,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("unsupported frontend protocol %u.%u.", PG_PROTOCOL_MAJOR(proto), PG_PROTOCOL_MINOR(proto))));
    }

    /*
     * Now fetch parameters out of startup packet and save them into the Port
     * structure.  All data structures attached to the Port struct must be
     * allocated in t_thrd.top_mem_cxt so that they will remain available in a
     * running backend (even after t_thrd.mem_cxt.postmaster_mem_cxt is destroyed).  We need
     * not worry about leaking this storage on failure, since we aren't in the
     * postmaster process anymore.
     */
    oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));

    if (PG_PROTOCOL_MAJOR(proto) >= 3) {
        int32 offset = sizeof(ProtocolVersion);

        /*
         * Scan packet body for name/option pairs.	We can assume any string
         * beginning within the packet body is null-terminated, thanks to
         * zeroing extra byte above.
         */
        port->guc_options = NIL;

        while (offset < len) {
            char* nameptr = ((char*)buf) + offset;
            int32 valoffset;
            char* valptr = NULL;

            if (*nameptr == '\0')
                break; /* found packet terminator */

            valoffset = offset + strlen(nameptr) + 1;

            if (valoffset >= len)
                break; /* missing value, will complain below */

            valptr = ((char*)buf) + valoffset;

            if (strcmp(nameptr, "database") == 0)
                port->database_name = pstrdup(valptr);
            else if (strcmp(nameptr, "user") == 0)
                port->user_name = pstrdup(valptr);
            else if (strcmp(nameptr, "options") == 0) {
                port->cmdline_options = pstrdup(valptr);

                if (strstr(port->cmdline_options, "xc_maintenance_mode=on") != NULL) {
                    isMaintenanceConnection = true;
#ifndef ENABLE_MULTIPLE_NODES
                } else if (strstr(port->cmdline_options, "remotetype=internaltool") != NULL) {
                    u_sess->proc_cxt.IsInnerMaintenanceTools = true;
#endif
                }
            } else if (strcmp(nameptr, "replication") == 0) {
                if (!IsHAPort(u_sess->proc_cxt.MyProcPort) && g_instance.attr.attr_common.enable_thread_pool) {
                        ereport(elevel,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("replication should connect HA port in thread_pool")));
                }
                /*
                 * Due to backward compatibility concerns the replication
                 * parameter is a hybrid beast which allows the value to be
                 * either boolean or the string 'database'. The latter
                 * connects to a specific database which is e.g. required for
                 * logical decoding while.
                 */
                /* Add data replication */
                if (strcmp(valptr, "data") == 0) {
                    /* mark the data sender as a wal sender for some common management */
                    t_thrd.role = WAL_NORMAL_SENDER;

                    if (!g_instance.attr.attr_storage.enable_mix_replication)
                        t_thrd.datasender_cxt.am_datasender = true;
                } else if (strcmp(valptr, "database") == 0) {
                    if (!IsHAPort(u_sess->proc_cxt.MyProcPort) && g_instance.attr.attr_common.enable_thread_pool) {
                        ereport(elevel,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("logical replication should connect HA port in thread_pool")));
                    }
                    t_thrd.role = WAL_DB_SENDER;
                } else if (strcmp(valptr, "hadr_main_standby") == 0) {
                    t_thrd.role = WAL_HADR_SENDER;
                } else if (strcmp(valptr, "hadr_standby_cn") == 0) {
                    t_thrd.role = WAL_HADR_CN_SENDER;
                } else if (strcmp(valptr, "standby_cluster") == 0) {
                    t_thrd.role = WAL_SHARE_STORE_SENDER;
                } else {
                    bool _am_normal_walsender = false;
                    if (!parse_bool(valptr, &_am_normal_walsender)) {
                        ereport(elevel,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("invalid value for parameter \"replication\""),
                                errhint("Valid values are: false, 0, true, 1, data, database.")));
                    } else if (_am_normal_walsender) {
                        t_thrd.role = WAL_NORMAL_SENDER;
                    }
                }

                /*
                 * CommProxy Support
                 *
                 * if i am wal/data sender, we bind cpu to die that fd be created
                 */
                if (g_comm_controller != NULL) {
                    g_comm_controller->BindThreadToGroupExcludeProxy(port->sock);
                    CommSockDesc *comm_sock = g_comm_controller->FdGetCommSockDesc(port->sock);
                    if (comm_sock != NULL) {
                        comm_sock->InitDataStatic();
                        CommRingBuffer* ring_buffer = (CommRingBuffer*)comm_sock->m_transport[ChannelTX];
                        if (ring_buffer->EnlargeBufferSize(MaxBuildAllocSize + 1) == false) {
                            ereport(LOG, (errmsg("EnlargeBufferSize faile. The m_status in ringBuffer is %d\n",
                                ring_buffer->m_status)));
                        }
                        gaussdb_read_barrier();
                        while (ring_buffer->m_status == TransportChange) {
                            gaussdb_read_barrier();
                        }
                    }
                }
            } else if (strcmp(nameptr, "backend_version") == 0) {
                errno = 0;
                port->SessionVersionNum = (uint32)strtoul(valptr, NULL, 10);
                if (errno != 0)
                    ereport(elevel,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("invalid value for parameter \"backend_version\""),
                            errhint("Valid values are of uint32 type.")));

                if (port->SessionVersionNum > GRAND_VERSION_NUM)
                    ereport(elevel,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("requested backend version is larger than grand version.")));
            } else if (strcmp(nameptr, "enable_full_encryption") == 0) {
                bool enable_ce = false;
                if (!parse_bool(valptr, &enable_ce)) {
                    ereport(elevel,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("invalid value for parameter \"enable_full_encryption\""),
                            errhint("Valid values are: 0, 1.")));
                }
                u_sess->attr.attr_common.enable_full_encryption = enable_ce;
            } else if (strcmp(nameptr, "connect_timeout") == 0) {
                errno = 0;
                const uint32 poolerConnectTimeoutMaxValue = 7200;  /* Max value of pooler_connect_timeout */
                uint32 poolerConnectTimeout = (uint32) strtoul(valptr, NULL, 10);
                if (errno != 0) {
                    ereport(FATAL,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                             errmsg("invalid value[%d] for parameter \"connect_timeout\"",
                                     u_sess->attr.attr_network.PoolerConnectTimeout),
                             errhint("Valid values are of uint32 type.")));
                }
                u_sess->attr.attr_network.PoolerConnectTimeout =
                    (poolerConnectTimeout < poolerConnectTimeoutMaxValue) ?
                        poolerConnectTimeout : poolerConnectTimeoutMaxValue;
            } else {
                if (strcmp(nameptr, "application_name") == 0) {
                    /* check if remote is dummystandby */
                    if (strcmp(valptr, "gs_ctl") == 0) {
                        /* mark remote as gs_ctl build */
                        t_thrd.postmaster_cxt.senderToBuildStandby = true;
                        u_sess->proc_cxt.clientIsGsCtl = true;
                        ereport(DEBUG5, (errmsg("gs_ctl connected")));
                    } else if (strcmp(valptr, "cm_agent") == 0) {
                        /* mark remote as cm_agent */
                        clientIsCmAgent = true;
                        u_sess->libpq_cxt.IsConnFromCmAgent = true;
                        ereport(DEBUG5, (errmsg("cm_agent connected")));
                    } else if (strcmp(valptr, "gs_clean") == 0) {
                        clientIsGsClean = true;
                        ereport(DEBUG5, (errmsg("gs_clean connected")));
#ifdef ENABLE_MULTIPLE_NODES
                    } else if (strcmp(valptr, "dummystandby") == 0) {
                        /* mark remote as dummystandby */
                        t_thrd.postmaster_cxt.senderToDummyStandby = true;
                        ereport(DEBUG5, (errmsg("secondary standby connected")));
                    } else if (strcmp(valptr, "gs_roach") == 0) {
                        u_sess->proc_cxt.clientIsGsroach = true;
                        ereport(DEBUG5, (errmsg("gs_roach connected")));
                    } else if (strcmp(valptr, "gs_redis") == 0) {
                        u_sess->proc_cxt.clientIsGsredis = true;
                        ereport(DEBUG5, (errmsg("gs_redis connected")));
#endif
                    } else if (strcmp(valptr, "WDRXdb") == 0) {
                        clientIsWDRXdb = true;
                        ereport(DEBUG5, (errmsg("WDRXdb connected")));
                    } else if (strcmp(valptr, "gs_rewind") == 0) {
                        /*
                         * mark remote as gs_rewind.
                         * in single-node mode, gs_ctl need gs_rewind to do inc-build.
                         */
                        u_sess->proc_cxt.clientIsGsrewind = true;
                        ereport(DEBUG5, (errmsg("gs_rewind connected")));
                    } else if (strcmp(valptr, "gsql") == 0) {
                        /* mark remote as gsql */
                        clientIsGsql = true;
                        ereport(DEBUG5, (errmsg("gsql connected")));
                    } else if (strcmp(valptr, "OM") == 0) {
                        clientIsOM = true;
                        ereport(DEBUG5, (errmsg("OM connected")));
                    } else if (strcmp(valptr, "gs_dump") == 0) {
                        u_sess->proc_cxt.clientIsGsdump = true;
                        ereport(DEBUG5, (errmsg("gs_dump connected")));
                    } else if (strcmp(valptr, "gs_basebackup") == 0) {
                        u_sess->proc_cxt.clientIsGsBasebackup = true;
                        ereport(LOG, (errmsg("gs_basebackup connected")));
                    } else if (strcmp(valptr, "gs_restore") == 0) {
                        u_sess->proc_cxt.clientIsGsRestore = true;
                        ereport(DEBUG5, (errmsg("gs_restore connected")));
                    } else if (strcmp(valptr, "remote_read") == 0) {
                        clientIsRemoteRead = true;
                        ereport(DEBUG5, (errmsg("remote_read connected")));
                    } else if (strcmp(valptr, "autonomoustransaction") == 0) {
                        clientIsAutonomousTransaction = true;
                        ereport(DEBUG5, (errmsg("autonomoustransaction connected")));
                    } else if (strcmp(valptr, "local_hadr_walrcv") == 0) {
                        clientIsLocalHadrWalrcv = true;
                        ereport(DEBUG5, (errmsg("local hadr walreceiver connected")));
                    } else if (strcmp(valptr, "subscription") == 0) {
                        u_sess->proc_cxt.clientIsSubscription = true;
                        ereport(DEBUG5, (errmsg("subscription connected")));
                    } else {
                        ereport(DEBUG5, (errmsg("application %s connected", valptr)));
                    }

                    errno_t ssrc = strncpy_s(u_sess->proc_cxt.applicationName, NAMEDATALEN, valptr, NAMEDATALEN - 1);
                    if (ssrc != EOK) {
                        ereport(WARNING, (errmsg("Save app name %s failed in receive startup packet", valptr)));
                    }
                }
                /* Assume it's a generic GUC option */
                port->guc_options = lappend(port->guc_options, pstrdup(nameptr));
                port->guc_options = lappend(port->guc_options, pstrdup(valptr));
            }

            offset = valoffset + strlen(valptr) + 1;
        }

        /*
         * If we didn't find a packet terminator exactly at the end of the
         * given packet length, complain.
         */
        if (offset != len - 1)
            ereport(elevel,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                    errmsg("invalid startup packet layout: expected terminator as last byte")));
    } else {
        /*
         * Get the parameters from the old-style, fixed-width-fields startup
         * packet as C strings.  The packet destination was cleared first so a
         * short packet has zeros silently added.  We have to be prepared to
         * truncate the pstrdup result for oversize fields, though.
         */
        StartupPacket* packet = (StartupPacket*)buf;

        port->database_name = pstrdup(packet->database);

        if (strlen(port->database_name) > sizeof(packet->database))
            port->database_name[sizeof(packet->database)] = '\0';

        port->user_name = pstrdup(packet->user);

        if (strlen(port->user_name) > sizeof(packet->user))
            port->user_name[sizeof(packet->user)] = '\0';

        port->cmdline_options = pstrdup(packet->options);

        if (strlen(port->cmdline_options) > sizeof(packet->options))
            port->cmdline_options[sizeof(packet->options)] = '\0';

        port->guc_options = NIL;
    }

    /* subscription is allowed to use HA port only, the auth check also done in HA port */
    if (u_sess->proc_cxt.clientIsSubscription && !IsHAPort(u_sess->proc_cxt.MyProcPort)) {
            ereport(elevel,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("subcription should connect HA port")));
    }

    /*  Inner tool with local sha256 will not be authenicated. */
    if (clientIsCmAgent || clientIsGsClean || clientIsOM || u_sess->proc_cxt.clientIsGsroach || clientIsWDRXdb ||
        clientIsRemoteRead || u_sess->proc_cxt.clientIsGsCtl || u_sess->proc_cxt.clientIsGsrewind ||
        u_sess->proc_cxt.clientIsGsredis || clientIsAutonomousTransaction || clientIsLocalHadrWalrcv) {
        u_sess->proc_cxt.IsInnerMaintenanceTools = true;
    }
    /* cm_agent and gs_clean should not be controlled by workload manager */
    if (clientIsCmAgent || clientIsGsClean || clientIsLocalHadrWalrcv) {
        u_sess->proc_cxt.IsWLMWhiteList = true;
    }
#ifdef ENABLE_MULTIPLE_NODES
    if (clientIsCmAgent) {
#ifdef ENABLE_DISTRIBUTE_TEST
        if (TEST_STUB(DN_CM_NEW_CONN, stub_sleep_emit)) {
            ereport(get_distribute_test_param()->elevel,
                (errmsg("sleep_emit happen during ProcessStartupPacket  time:%ds, stub_name:%s",
                    get_distribute_test_param()->sleep_time,
                    get_distribute_test_param()->test_stub_name)));
        }
#endif
    }
#endif

    /* Check a user name was given. */
    if (port->user_name == NULL || port->user_name[0] == '\0')
        ereport(elevel,
            (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                errmsg("no openGauss user name specified in startup packet")));

    /* The database defaults to the user name. */
    if (port->database_name == NULL || port->database_name[0] == '\0')
        port->database_name = pstrdup(port->user_name);

    /*
     * Truncate given database and user names to length of a Postgres name.
     * This avoids lookup failures when overlength names are given.
     */
    if (strlen(port->database_name) >= NAMEDATALEN)
        port->database_name[NAMEDATALEN - 1] = '\0';

    if (strlen(port->user_name) >= NAMEDATALEN)
        port->user_name[NAMEDATALEN - 1] = '\0';

    /*
     * Normal walsender backends, e.g. for streaming replication, are not
     * connected to a particular database. But walsenders used for logical
     * replication need to connect to a specific database. We allow streaming
     * replication commands to be issued even if connected to a database as it
     * can make sense to first make a basebackup and then stream changes
     * starting from that.
     */
    if (AM_WAL_SENDER && !AM_WAL_DB_SENDER && !AM_WAL_HADR_SENDER && !AM_WAL_HADR_CN_SENDER) {
        port->database_name[0] = '\0';
    }
    /* set special tcp keepalive parameters for build senders */
    if (AM_WAL_SENDER && t_thrd.postmaster_cxt.senderToBuildStandby) {
        if (!IS_AF_UNIX(port->laddr.addr.ss_family)) {
            pq_setkeepalivesidle(7200, port);
            pq_setkeepalivesinterval(75, port);
            pq_setkeepalivescount(9, port);
        }
    }

    /*
     * Done putting stuff in t_thrd.top_mem_cxt.
     */
    (void)MemoryContextSwitchTo(oldcontext);

    if (AM_WAL_SENDER) {
        int channel_adapt = 0, i = 0;

        if (!IS_PGXC_COORDINATOR) {
#ifdef ENABLE_MULTIPLE_NODES
            if (NORMAL_MODE == hashmdata->current_mode) {
                if (!u_sess->proc_cxt.clientIsGsBasebackup && !AM_WAL_DB_SENDER) {
                    ereport(elevel,
                        (errmsg("the current t_thrd.postmaster_cxt.server_mode is NORMAL, "
                                "could not accept HA connection.")));
                }
            }
#endif

            for (i = 1; i < DOUBLE_MAX_REPLNODE_NUM; i++) {
                ReplConnInfo *replConnInfo = NULL;
                if (i >= MAX_REPLNODE_NUM) {
                    replConnInfo = t_thrd.postmaster_cxt.CrossClusterReplConnArray[i - MAX_REPLNODE_NUM];
                } else {
                    replConnInfo = t_thrd.postmaster_cxt.ReplConnArray[i];
                }
                if (replConnInfo != NULL && IsChannelAdapt(port, replConnInfo)) {
                    channel_adapt++;
                }
            }

            if (0 == channel_adapt) {
                int elevel = comm_client_bind ? FATAL : WARNING;
                ereport(elevel,
                    (errcode(ERRCODE_CANNOT_CONNECT_NOW), errmsg("the ha connection is not in the channel list")));
            }
        }
    } else {
        if (dummyStandbyMode)
            ereport(
                elevel, (errcode(ERRCODE_CANNOT_CONNECT_NOW), errmsg("Secondary Standby does not accept connection")));

        /*
         * clients other than inner maintenance tools and remote coordinators are only
         * allowed to connect through gsql port, gsql port unix socket and ha port unix socket
         */
        if (!u_sess->proc_cxt.IsInnerMaintenanceTools && !IsConnPortFromCoord(port) &&
            (!IsLocalAddr(port) || !IsLocalPort(port))) {
            ereport(elevel,
                (errcode(ERRCODE_CANNOT_CONNECT_NOW),
                    errmsg("the local listen ip and port is not for the gsql client")));
        }
    }

    /*
     * We need to check whether the connection can be accepted, standby and pending
     * mode can not accept connection.
     * Do not accept gsql connection when promote primary node does not write empty log.
     * 1: when the client is walsender we do not check, except sender to dummy standby.
     * 2: when the client is gsql, and use -m(it will make xc_maintenance_mode to on)
     *    to connect, we do not check.
     */
    if ((!AM_WAL_SENDER && !(isMaintenanceConnection && (clientIsGsql || clientIsCmAgent ||
        t_thrd.postmaster_cxt.senderToBuildStandby || clientIsLocalHadrWalrcv)) && !clientIsRemoteRead) ||
        t_thrd.postmaster_cxt.senderToDummyStandby) {

        if (PENDING_MODE == hashmdata->current_mode && !IS_PGXC_COORDINATOR) {
            ereport(elevel, (errcode(ERRCODE_CANNOT_CONNECT_NOW),
                    errmsg("can not accept connection in pending mode.")));
        } else {
#ifdef ENABLE_MULTIPLE_NODES
            if (STANDBY_MODE == hashmdata->current_mode && (!IS_DISASTER_RECOVER_MODE || GTM_FREE_MODE ||
                                                            g_instance.attr.attr_storage.recovery_parse_workers > 1)) {
                ereport(ERROR, (errcode(ERRCODE_CANNOT_CONNECT_NOW),
                        errmsg("can not accept connection in standby mode.")));
            }
#else
            if (hashmdata->current_mode == STANDBY_MODE && !g_instance.attr.attr_storage.EnableHotStandby) {
                ereport(elevel, (errcode(ERRCODE_CANNOT_CONNECT_NOW),
                        errmsg("can not accept connection if hot standby off")));
            }
#endif
        }
    }

    if ((PM_RUN != pmState) && t_thrd.postmaster_cxt.senderToDummyStandby) {
        ereport(elevel,
            (errcode(ERRCODE_CANNOT_CONNECT_NOW), errmsg("can not accept dummy standby connection in standby mode.")));
        return STATUS_ERROR;
    }

    /*
     * We need to check whether the connection can be accepted in roach restore.
     * When the client is gsql or other external connection, and use -m(it will make xc_maintenance_mode to on)
     */
    if (g_instance.roach_cxt.isRoachRestore && !isMaintenanceConnection && !u_sess->proc_cxt.IsInnerMaintenanceTools &&
        IsLocalPort(port)) {
        ereport(elevel,
            (errcode(ERRCODE_CANNOT_CONNECT_NOW), errmsg("can not accept connection in roach restore mode.")));
        return STATUS_ERROR;
    }

#ifdef USE_SSL
    if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && g_instance.attr.attr_security.EnableSSL &&
        u_sess->attr.attr_security.RequireSSL && !IS_AF_UNIX(port->laddr.addr.ss_family) && !SSLdone) {
        /*
         * only deal with connections between client and server.
         * for connections between coordinators, we should not use SSL.
         */
        if (!u_sess->proc_cxt.IsInnerMaintenanceTools && !IsConnPortFromCoord(port)) {
            ereport(elevel,
                (errcode(ERRCODE_CANNOT_CONNECT_NOW), errmsg("SSL connection is required by the database system")));
        }
    }
#endif

#ifdef ENABLE_MULTIPLE_NODES
    if (port->cmdline_options != NULL && strstr(port->cmdline_options, "remotetype=coordinator") != NULL) {
        u_sess->attr.attr_common.remoteConnType = REMOTE_CONN_COORD;
    } else
#endif
    {
        u_sess->attr.attr_common.remoteConnType = REMOTE_CONN_APP;
    }

    /* We need to restore the socket settings to prevent unexpected errors. */
    if (isTvSeted && (comm_setsockopt(port->sock, SOL_SOCKET, SO_RCVTIMEO, &oldTv, oldTvLen) < 0)) {
        ereport(LOG, (errmsg("setsockopt(SO_RCVTIMEO) failed: %m")));
        return STATUS_ERROR;
    }
    
    return STATUS_OK;
}

/*
 * The client has sent a stop query request, not a normal
 * start-a-new-connection packet.
 */
static void processStopRequest(Port* port, void* pkt)
{
    StopRequestPacket* csp = (StopRequestPacket*)pkt;
    ThreadId backendPID;
    int logictid = 0;
    uint64 query_id = 0;
    // get thread id from logic thread id
    //
    logictid = (int)ntohl(csp->backendPID);
    query_id = (((uint64)ntohl(csp->query_id_first)) << 32) + (uint32)ntohl(csp->query_id_end);

    if (ENABLE_THREAD_POOL) {
        g_threadPoolControler->GetSessionCtrl()->SendProcSignal(logictid, PROCSIG_EXECUTOR_FLAG, query_id);
        return;
    }

    backendPID = getThreadIdFromLogicThreadId(logictid);

    /* If the mian thread already exit, no need to stop. */
    if (0 != backendPID)
        StreamNodeGroup::stopAllThreadInNodeGroup(backendPID, query_id);
}

/*
 * The client has sent a cancel request packet, not a normal
 * start-a-new-connection packet.  Perform the necessary processing.
 * Nothing is sent back to the client.
 */
static void processCancelRequest(Port* port, void* pkt)
{
    CancelRequestPacket* canc = (CancelRequestPacket*)pkt;
    long cancelAuthCode = (long)ntohl(canc->cancelAuthCode);

    if (((unsigned long)cancelAuthCode & 0x1) == 0) {
        if (ENABLE_THREAD_POOL) {
            int sess_ctrl_id = (int)ntohl(canc->backendPID);
            ThreadPoolSessControl *sess_ctrl = g_threadPoolControler->GetSessionCtrl();
            knl_session_context *sess = sess_ctrl->GetSessionByIdx(sess_ctrl_id);

            if (sess == NULL || sess->cancel_key != cancelAuthCode) {
                ereport(LOG,(errmsg("Don't found the match sess, the session slot(%d)", sess_ctrl_id)));
                return;
            }

            int err = sess_ctrl->SendSignal((int)sess_ctrl_id, SIGINT);
            if (err != 0) {
                ereport(WARNING,
                    (errmsg("kill(session %ld, signal %d) failed: \"%s\", pmState %d, Demotion %d, Shutdown %d",
                        (long)sess_ctrl_id, SIGINT, gs_strerror(err), pmState, g_instance.demotion, Shutdown)));
            }
        } else {
            ereport(WARNING, (errmsg("Receive invalid cancel key, which suppose to be thread pool mode.")));
        }
    } else {
        int backendSlot = 0;
        // get thread id from logic thread id
        backendSlot = (int)ntohl(canc->backendPID);
        Backend* bn = GetBackend(backendSlot);

        if (bn == NULL || bn->pid == 0 || bn->pid == InvalidTid) {
            ereport(LOG,
                (errmsg(
                    "Don't found the match process, the backend slot(%d), pid(%lu)", backendSlot, bn ? bn->pid : 0)));
            return;
        }

        if (bn->cancel_key == cancelAuthCode) {
            /* Found a match; signal that backend to cancel current op */
            ereport(DEBUG2, (errmsg_internal("processing cancel request: sending SIGINT to process %lu", bn->pid)));
            signal_child(bn->pid, SIGINT);
        } else
            /* Right PID, wrong key: no way, Jose */
            ereport(LOG, (errmsg("wrong key in cancel request for process %lu", bn->pid)));
    }
}

/*
 * canAcceptConnections --- check to see if database state allows connections.
 */
CAC_state canAcceptConnections(bool isSession)
{
    CAC_state result = CAC_OK;

    /*
     * Can't start backends when in startup/shutdown/inconsistent recovery
     * state.
     *
     * In state PM_WAIT_BACKUP only superusers can connect (this must be
     * allowed so that a superuser can end online backup mode); we return
     * CAC_WAITBACKUP code to indicate that this must be checked later. Note
     * that neither CAC_OK nor CAC_WAITBACKUP can safely be returned until we
     * have checked for too many children.
     */
    if (pmState != PM_RUN) {
        if (pmState == PM_WAIT_BACKUP)
            result = CAC_WAITBACKUP; /* allow superusers only */
        else if (g_instance.status > NoShutdown || g_instance.demotion > NoDemote)
            return CAC_SHUTDOWN; /* shutdown is pending */
        else if (!g_instance.fatal_error && (pmState == PM_STARTUP || pmState == PM_RECOVERY))
            return CAC_STARTUP; /* normal startup */
        else if (!g_instance.fatal_error && pmState == PM_HOT_STANDBY)
            result = CAC_OK; /* connection OK during hot standby */
        else
            return CAC_RECOVERY; /* else must be crash recovery */
    }

    if (isSession)
        return result;

    /*
     * Don't start too many children.
     *
     * We allow more connections than we can have backends here because some
     * might still be authenticating; they might fail auth, or some existing
     * backend might exit before the auth cycle is completed. The exact
     * g_instance.shmem_cxt.MaxBackends limit is enforced when a new backend tries to join the
     * shared-inval backend array.
     *
     * The limit here must match the sizes of the per-child-process arrays;
     * see comments for MaxLivePostmasterChildren().
     */
    if (CountChildren(BACKEND_TYPE_ALL) >= MaxLivePostmasterChildren())
        result = CAC_TOOMANY;

    return result;
}

/*
 * ConnCreateToRecvGssock -- For logic connection from CN, we generate gs_socket
 * for logic connection in receiver flow control thread, we use this function to generate
 * a port and receive gs_socket from receiver flow control thread by unix domain socket.
 *
 * Returns NULL on failure, Returns Port with gs_sock on succeed.
 */
static Port* ConnCreateToRecvGssock(pollfd* ufds, int idx, int* nSockets)
{
    Port* port = NULL;
    int error;
    /*
     * receiver flow ctrl receive logic connection request
     * and no unix domain socket between receiver flow ctrl and server loop
     * so receiver flow ctrl make connection and listening fd is polled up
     */
    if (ufds[idx].fd == t_thrd.libpq_cxt.listen_fd_for_recv_flow_ctrl) {
        port = ConnCreate(ufds[idx].fd);

        if (port == NULL)
            return port;

        /* clean old socket if exist */
        if (t_thrd.postmaster_cxt.sock_for_libcomm != PGINVALID_SOCKET) {
            ConnFree((void*)t_thrd.postmaster_cxt.port_for_libcomm);
            comm_close(t_thrd.postmaster_cxt.sock_for_libcomm); /* CommProxy Support */
            t_thrd.postmaster_cxt.sock_for_libcomm = PGINVALID_SOCKET;
            t_thrd.postmaster_cxt.port_for_libcomm = NULL;
            ufds[--(*nSockets)].fd = PGINVALID_SOCKET;
        }

        /* save the port and socket for connection between receiver flow ctrl and server loop */
        t_thrd.postmaster_cxt.port_for_libcomm = port;
        t_thrd.postmaster_cxt.sock_for_libcomm = port->sock;
        /* add in ufds and it will polled up when receive a new gs_sock */
        ufds[*nSockets].events = POLLIN | POLLPRI;
        ufds[*nSockets].revents = 0;
        ufds[(*nSockets)++].fd = t_thrd.postmaster_cxt.sock_for_libcomm;
    }
    /* sock_for_libcomm is polled up when recv flow ctrl send gs_sock by unix_domain sock */
    else {
        port = t_thrd.postmaster_cxt.port_for_libcomm;
        if (port == NULL) {
            return NULL;
        }
        port->sock = t_thrd.postmaster_cxt.sock_for_libcomm;
    }

    /*  receive gs_sock by unix domain sock */
    port->is_logic_conn = true;
    error = gs_recv_msg_by_unix_domain(port->sock, &port->gs_sock);

    /*
     * for logic connection gs_sock is used for communication
     * and port->sock is used to receive gs_sock, as we have already get gs_sock
     * port->sock is unneeded for current connection, so set to -1.
     * note: port->sock is an long_term socket, only one for each process
     * it will be closed when receive error.
     */
    if ((error > 0)) {
        port->sock = PGINVALID_SOCKET;
        return port;
    }

    ConnFree((void*)t_thrd.postmaster_cxt.port_for_libcomm);
    t_thrd.postmaster_cxt.sock_for_libcomm = PGINVALID_SOCKET;
    t_thrd.postmaster_cxt.port_for_libcomm = NULL;
    port = NULL;
    ufds[--(*nSockets)].fd = PGINVALID_SOCKET;

    return port;
}

/*
 * ConnCreate -- create a local connection data structure
 *
 * Returns NULL on failure, other than out-of-memory which is fatal.
 */
static Port* ConnCreate(int serverFd)
{
    Port* port = NULL;

    port =
        (Port*)MemoryContextAllocZero(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), 1 * sizeof(Port));

    if (port == NULL) {
        ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        ExitPostmaster(1);
    }

    port->sock = PGINVALID_SOCKET;
    port->gs_sock = GS_INVALID_GSOCK;

    if (StreamConnection(serverFd, port) != STATUS_OK) {
        if (port->sock >= 0)
            StreamClose(port->sock);

        ConnFree((void*)port);
        return NULL;
    }

    /*
     * Precompute password salt values to use for this connection. It's
     * slightly annoying to do this long in advance of knowing whether we'll
     * need 'em or not, but we must do the random() calls before we fork, not
     * after.  Else the postmaster's random sequence won't get advanced, and
     * all backends would end up using the same salt...
     * Use openssl RAND_priv_bytes interface to generate random salt, cast char to
     * unsigned char here.
     */
    int retval = RAND_priv_bytes((unsigned char*)port->md5Salt, sizeof(port->md5Salt));
    if (retval != 1) {
        ereport(ERROR, (errmsg("Failed to Generate the random number,errcode:%d", retval)));
    }

    /*
     * Allocate GSSAPI specific state struct
     */
#ifndef EXEC_BACKEND
#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)
    port->gss = (pg_gssinfo*)MemoryContextAllocZero(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), 1 * sizeof(pg_gssinfo));

    if (!port->gss) {
        ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        ExitPostmaster(1);
    }

#endif
#endif
    port->is_logic_conn = false;
    port->gs_sock.type = GSOCK_INVALID;

    return port;
}

/*
 * ConnFree -- free a local connection data structure
 */
void ConnFree(void* conn)
{
    int at_main_thread = (!IsPostmasterEnvironment || t_thrd.proc_cxt.MyProcPid == PostmasterPid) ? 1 : 0;

    Port* tmp_conn = (Port*)conn;
#ifdef USE_SSL
    secure_close(tmp_conn);
#endif

    if (tmp_conn != NULL && tmp_conn->gss != NULL) {
        if (0 != at_main_thread) {
            pfree(tmp_conn->gss);
        } else {
            free(tmp_conn->gss);
        }
    }

    if (0 != at_main_thread) {
        pfree(tmp_conn);
    } else {
        free(tmp_conn);
        tmp_conn = NULL;
    }
}

/*
 * ClosePostmasterPorts -- close all the postmaster's open sockets
 *
 * This is called during child process startup to release file descriptors
 * that are not needed by that child process.  The postmaster still has
 * them open, of course.
 *
 * Note: we pass am_syslogger as a boolean because we don't want to set
 * the global variable yet when this is called.
 */
void ClosePostmasterPorts(bool am_syslogger)
{
    int i;

#ifndef WIN32

    /*
     * Close the write end of postmaster death watch pipe. It's important to
     * do this as early as possible, so that if postmaster dies, others won't
     * think that it's still running because we're holding the pipe open.
     */
    if (comm_close(t_thrd.postmaster_cxt.postmaster_alive_fds[POSTMASTER_FD_OWN]))
        ereport(FATAL,
            (errcode_for_file_access(),
                errmsg_internal("could not close postmaster death monitoring pipe in child process: %m")));

    t_thrd.postmaster_cxt.postmaster_alive_fds[POSTMASTER_FD_OWN] = -1;
#endif

    /* Close the listen sockets */
    for (i = 0; i < MAXLISTEN; i++) {
        if (t_thrd.postmaster_cxt.ListenSocket[i] != PGINVALID_SOCKET) {
            StreamClose(t_thrd.postmaster_cxt.ListenSocket[i]);
            t_thrd.postmaster_cxt.ListenSocket[i] = PGINVALID_SOCKET;
        }
    }
    /* If using syslogger, close the read side of the pipe */
    if (!am_syslogger) {
#ifndef WIN32

        if (t_thrd.postmaster_cxt.syslogPipe[0] >= 0) {
            comm_close(t_thrd.postmaster_cxt.syslogPipe[0]);
        }

        t_thrd.postmaster_cxt.syslogPipe[0] = -1;
#else

        if (t_thrd.postmaster_cxt.syslogPipe[0])
            CloseHandle(t_thrd.postmaster_cxt.syslogPipe[0]);

        t_thrd.postmaster_cxt.syslogPipe[0] = 0;
#endif
    }

#ifdef USE_BONJOUR

    /* If using Bonjour, close the connection to the mDNS daemon */
    if (bonjour_sdref) {
        comm_close(DNSServiceRefSockFD(bonjour_sdref));
    }

#endif
}

/*
 * on_proc_exit callback to close server's listen sockets
 */
static void CloseServerPorts(int status, Datum arg)
{
    int i;

    /*
     * First, explicitly close all the socket FDs.  We used to just let this
     * happen implicitly at postmaster exit, but it's better to close them
     * before we remove the postmaster.pid lockfile; otherwise there's a race
     * condition if a new postmaster wants to re-use the TCP port number.
     */
    for (i = 0; i < MAXLISTEN; i++) {
        if (t_thrd.postmaster_cxt.ListenSocket[i] != PGINVALID_SOCKET) {
            StreamClose(t_thrd.postmaster_cxt.ListenSocket[i]);
            t_thrd.postmaster_cxt.ListenSocket[i] = PGINVALID_SOCKET;
        }
    }

    /*
     * Removal of the Unix socket file and socket lockfile will happen in
     * later on_proc_exit callbacks.
     */
}

void socket_close_on_exec(void)
{
    /* Close the listen sockets */
    for (int i = 0; i < MAXLISTEN; i++) {
        if (t_thrd.postmaster_cxt.ListenSocket[i] != PGINVALID_SOCKET) {
            int flags = comm_fcntl(t_thrd.postmaster_cxt.ListenSocket[i], F_GETFD);
            if (flags < 0)
                ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("fcntl F_GETFD failed!")));

            flags |= FD_CLOEXEC;
            if (comm_fcntl(t_thrd.postmaster_cxt.ListenSocket[i], F_SETFD, flags) < 0)
                ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("fcntl F_SETFD failed!")));
        }
    }
}

static void UpdateCrossRegionMode()
{
    volatile HaShmemData* hashmdata = t_thrd.postmaster_cxt.HaShmData;
    int i = 0, repl_list_num = 0;
    bool is_cross_region = false;

    for (i = 1; i < MAX_REPLNODE_NUM; i++) {
        if (t_thrd.postmaster_cxt.ReplConnArray[i] != NULL) {
            repl_list_num++;
            if (t_thrd.postmaster_cxt.ReplConnArray[i]->isCrossRegion &&
                !IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE) {
                is_cross_region = true;
            }
        }
        if (t_thrd.postmaster_cxt.CrossClusterReplConnArray[i] != NULL) {
            repl_list_num++;
        }
    }
    if (is_cross_region != hashmdata->is_cross_region) {
        SpinLockAcquire(&hashmdata->mutex);
        hashmdata->is_cross_region = is_cross_region;
        hashmdata->repl_list_num = repl_list_num;
        SpinLockRelease(&hashmdata->mutex);
        ereport(LOG, (errmsg("SIGHUP_handler update is_cross_region: %d", hashmdata->is_cross_region)));
    }
}

/*
 * reset_shared -- reset shared memory and semaphores
 */
static void reset_shared(int port)
{
    /*
     * Create or re-create shared memory and semaphores.
     *
     * Note: in each "cycle of life" we will normally assign the same IPC keys
     * (if using SysV shmem and/or semas), since the port number is used to
     * determine IPC keys.	This helps ensure that we will clean up dead IPC
     * objects if the postmaster crashes and is restarted.
     */
    CreateSharedMemoryAndSemaphores(false, port);
}

static void ObsArchSighupHandler()
{
    if (g_instance.archive_thread_info.obsArchPID != NULL) {
            for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
                if (g_instance.archive_thread_info.obsArchPID[i] != 0) {
                    signal_child(g_instance.archive_thread_info.obsArchPID[i], SIGHUP);
                }
            }
        }
    
    if (START_BARRIER_CREATOR && g_instance.archive_thread_info.obsBarrierArchPID != NULL) {
        for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
            if (g_instance.archive_thread_info.obsBarrierArchPID[i] != 0) {
                signal_child(g_instance.archive_thread_info.obsBarrierArchPID[i], SIGHUP);
            }
        }
    }
}

/*
 * SIGHUP -- reread config files, and tell children to do same
 */
static void SIGHUP_handler(SIGNAL_ARGS)
{
    int save_errno = errno;
    ConfFileLock filelock = {NULL, 0};

    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    if (g_instance.status <= SmartShutdown && g_instance.demotion <= SmartDemote) {

        char gucconf_lock_file[MAXPGPATH] = {0};
        ereport(LOG, (errmsg("received SIGHUP, reloading configuration files")));
        int rc = snprintf_s(gucconf_lock_file,
            sizeof(gucconf_lock_file),
            MAXPGPATH - 1,
            "%s/postgresql.conf.lock",
            t_thrd.proc_cxt.DataDir);
        securec_check_intval(rc, , );
        if (get_file_lock(gucconf_lock_file, &filelock) != CODE_OK) {
            ereport(WARNING, (errmsg("the last sigup signal is processing,get file lock failed.")));
            (void)PG_SETMASK(&t_thrd.libpq_cxt.UnBlockSig);
            errno = save_errno;
            return;
        }

        ProcessConfigFile(PGC_SIGHUP);
        release_file_lock(&filelock);

        (void)SignalChildren(SIGHUP);
        if (ENABLE_THREAD_POOL) {
            g_threadPoolControler->GetSessionCtrl()->SigHupHandler();
            g_threadPoolControler->GetScheduler()->SigHupHandler();
        }

        if (g_instance.pid_cxt.StartupPID != 0)
            signal_child(g_instance.pid_cxt.StartupPID, SIGHUP);

        if (g_instance.pid_cxt.PageRepairPID != 0) {
            signal_child(g_instance.pid_cxt.PageRepairPID, SIGHUP);
        }

#ifdef PGXC /* PGXC_COORD */
        if (
#ifdef ENABLE_MULTIPLE_NODES
            IS_PGXC_COORDINATOR &&
#endif
            g_instance.pid_cxt.TwoPhaseCleanerPID != 0)
            signal_child(g_instance.pid_cxt.TwoPhaseCleanerPID, SIGHUP);

        if (GTM_LITE_CN && g_instance.pid_cxt.CsnminSyncPID != 0) {
            signal_child(g_instance.pid_cxt.CsnminSyncPID, SIGHUP);
        }

        if (g_instance.pid_cxt.FaultMonitorPID != 0)
            signal_child(g_instance.pid_cxt.FaultMonitorPID, SIGHUP);
#endif

        if (START_BARRIER_CREATOR && g_instance.pid_cxt.BarrierCreatorPID != 0) {
            signal_child(g_instance.pid_cxt.BarrierCreatorPID, SIGHUP);
        }

        if (g_instance.pid_cxt.BgWriterPID != 0) {
            Assert(!dummyStandbyMode);
            signal_child(g_instance.pid_cxt.BgWriterPID, SIGHUP);
        }

        if (g_instance.pid_cxt.SpBgWriterPID != 0) {
            Assert(!dummyStandbyMode);
            signal_child(g_instance.pid_cxt.SpBgWriterPID, SIGHUP);
        }

        if (g_instance.pid_cxt.CheckpointerPID != 0) {
            Assert(!dummyStandbyMode);
            signal_child(g_instance.pid_cxt.CheckpointerPID, SIGHUP);
        }
        if (g_instance.pid_cxt.PageWriterPID != NULL) {
            int i;
            for (i = 0; i < g_instance.ckpt_cxt_ctl->pgwr_procs.num; i++) {
                if (g_instance.pid_cxt.PageWriterPID[i] != 0) {
                    Assert(!dummyStandbyMode);
                    signal_child(g_instance.pid_cxt.PageWriterPID[i], SIGHUP);
                }
            }
        }

#ifndef ENABLE_MULTIPLE_NODES
        SetDcfNeedSyncConfig();
#endif

        if (g_instance.pid_cxt.WalWriterPID != 0)
            signal_child(g_instance.pid_cxt.WalWriterPID, SIGHUP);

        if (g_instance.pid_cxt.WalWriterAuxiliaryPID != 0)
            signal_child(g_instance.pid_cxt.WalWriterAuxiliaryPID, SIGHUP);

        if (g_instance.pid_cxt.WalRcvWriterPID != 0)
            signal_child(g_instance.pid_cxt.WalRcvWriterPID, SIGHUP);

        if (g_instance.pid_cxt.WalReceiverPID != 0)
            signal_child(g_instance.pid_cxt.WalReceiverPID, SIGHUP);

        if (g_instance.pid_cxt.DataRcvWriterPID != 0)
            signal_child(g_instance.pid_cxt.DataRcvWriterPID, SIGHUP);

        if (g_instance.pid_cxt.DataReceiverPID != 0)
            signal_child(g_instance.pid_cxt.DataReceiverPID, SIGHUP);

        if (g_instance.pid_cxt.AutoVacPID != 0)
            signal_child(g_instance.pid_cxt.AutoVacPID, SIGHUP);

        if (g_instance.pid_cxt.PgJobSchdPID != 0)
            signal_child(g_instance.pid_cxt.PgJobSchdPID, SIGHUP);

        if (g_instance.pid_cxt.PgArchPID != 0)
            signal_child(g_instance.pid_cxt.PgArchPID, SIGHUP);

        ObsArchSighupHandler();

        if (g_instance.pid_cxt.SysLoggerPID != 0)
            signal_child(g_instance.pid_cxt.SysLoggerPID, SIGHUP);
        /* signal the auditor process */
        if (g_instance.pid_cxt.PgAuditPID != NULL) {
            for (int i = 0; i < g_instance.audit_cxt.thread_num; ++i) {
                if (g_instance.pid_cxt.PgAuditPID[i] != 0) {
                    Assert(!dummyStandbyMode);
                    signal_child(g_instance.pid_cxt.PgAuditPID[i], SIGHUP);
                }
            }
        }
        if (g_instance.pid_cxt.PgStatPID != 0)
            signal_child(g_instance.pid_cxt.PgStatPID, SIGHUP);

        if (g_instance.pid_cxt.TxnSnapCapturerPID != 0) {
            Assert(!dummyStandbyMode);
            signal_child(g_instance.pid_cxt.TxnSnapCapturerPID, SIGHUP);
        }

        if (g_instance.pid_cxt.RbCleanrPID != 0) {
            Assert(!dummyStandbyMode);
            signal_child(g_instance.pid_cxt.RbCleanrPID, SIGHUP);
        }

        if (g_instance.pid_cxt.SnapshotPID != 0)
            signal_child(g_instance.pid_cxt.SnapshotPID, SIGHUP);

        if (g_instance.pid_cxt.AshPID != 0)
            signal_child(g_instance.pid_cxt.AshPID, SIGHUP);

        if (g_instance.pid_cxt.StatementPID != 0)
            signal_child(g_instance.pid_cxt.StatementPID, SIGHUP);

        if (g_instance.pid_cxt.AlarmCheckerPID != 0) {
            Assert(!dummyStandbyMode);
            signal_child(g_instance.pid_cxt.AlarmCheckerPID, SIGHUP);
        }

        if (g_instance.pid_cxt.ReaperBackendPID != 0)
            signal_child(g_instance.pid_cxt.ReaperBackendPID, SIGHUP);

        if (g_instance.pid_cxt.WLMCollectPID != 0)
            signal_child(g_instance.pid_cxt.WLMCollectPID, SIGHUP);

        if (g_instance.pid_cxt.WLMMonitorPID != 0)
            signal_child(g_instance.pid_cxt.WLMMonitorPID, SIGHUP);

        if (g_instance.pid_cxt.WLMArbiterPID != 0)
            signal_child(g_instance.pid_cxt.WLMArbiterPID, SIGHUP);

        if (g_instance.pid_cxt.CBMWriterPID != 0) {
            Assert(!dummyStandbyMode);
            signal_child(g_instance.pid_cxt.CBMWriterPID, SIGHUP);
        }

        if (g_instance.pid_cxt.PercentilePID != 0) {
            Assert(!dummyStandbyMode);
            signal_child(g_instance.pid_cxt.PercentilePID, SIGHUP);
        }

        if (g_instance.pid_cxt.HeartbeatPID != 0)
            signal_child(g_instance.pid_cxt.HeartbeatPID, SIGHUP);

        if (g_instance.pid_cxt.CommSenderFlowPID != 0)
            signal_child(g_instance.pid_cxt.CommSenderFlowPID, SIGHUP);

        if (g_instance.pid_cxt.CommReceiverFlowPID != 0)
            signal_child(g_instance.pid_cxt.CommReceiverFlowPID, SIGHUP);

        if (g_instance.pid_cxt.CommAuxiliaryPID != 0) {
            signal_child(g_instance.pid_cxt.CommAuxiliaryPID, SIGHUP);
        }

        if (g_instance.pid_cxt.CommPoolerCleanPID != 0) {
            signal_child(g_instance.pid_cxt.CommPoolerCleanPID, SIGHUP);
        }

        if (g_instance.pid_cxt.UndoLauncherPID != 0) {
            signal_child(g_instance.pid_cxt.UndoLauncherPID, SIGHUP);
        }
#if !defined(ENABLE_MULTIPLE_NODES) && !defined(ENABLE_LITE_MODE)
        if (g_instance.pid_cxt.ApplyLauncerPID != 0) {
            signal_child(g_instance.pid_cxt.ApplyLauncerPID, SIGHUP);
        }
#endif
        if (g_instance.pid_cxt.UndoRecyclerPID != 0) {
            signal_child(g_instance.pid_cxt.UndoRecyclerPID, SIGHUP);
        }

        if (g_instance.pid_cxt.GlobalStatsPID != 0) {
            signal_child(g_instance.pid_cxt.GlobalStatsPID, SIGHUP);
        }

        if (g_instance.pid_cxt.CommReceiverPIDS != NULL) {
            int recv_loop = 0;
            for (recv_loop = 0; recv_loop < g_instance.attr.attr_network.comm_max_receiver; recv_loop++) {
                if (g_instance.pid_cxt.CommReceiverPIDS[recv_loop] != 0) {
                    signal_child(g_instance.pid_cxt.CommReceiverPIDS[recv_loop], SIGHUP);
                }
            }
        }

        if (g_instance.pid_cxt.sharedStorageXlogCopyThreadPID != 0) {
            signal_child(g_instance.pid_cxt.sharedStorageXlogCopyThreadPID, SIGHUP);
        }

#ifdef ENABLE_MULTIPLE_NODES
        if (g_instance.pid_cxt.BarrierPreParsePID != 0) {
            signal_child(g_instance.pid_cxt.BarrierPreParsePID, SIGHUP);
        }

        if (g_instance.pid_cxt.TsCompactionPID != 0) {
            signal_child(g_instance.pid_cxt.TsCompactionPID, SIGHUP);
        }
        if (g_instance.pid_cxt.TsCompactionAuxiliaryPID != 0) {
            signal_child(g_instance.pid_cxt.TsCompactionAuxiliaryPID, SIGHUP);
        }
        (void)streaming_backend_manager(STREAMING_BACKEND_SIGHUP);
#endif   /* ENABLE_MULTIPLE_NODES */

        /* Update is_cross_region for streaming dr */
        UpdateCrossRegionMode();

        /* Reload authentication config files too */
        int loadhbaCount = 0;
        while (!load_hba()) {
            loadhbaCount++;
            if (loadhbaCount >= 3) {
                /*
                 * It makes no sense to continue if we fail to load the HBA file,
                 * since there is no way to connect to the database in this case.
                 */
                ereport(WARNING, (errmsg("pg_hba.conf not reloaded")));
                break;
            }
            pg_usleep(200000L);  /* sleep 200ms for reload next time */
        }

        load_ident();

        /* Reload license file. */
        signalReloadLicenseHandler(SIGHUP);

#ifdef EXEC_BACKEND
        /* Update the starting-point file for future children */
        write_nondefault_variables(PGC_SIGHUP);
#endif
    }

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);

    errno = save_errno;
}

void KillGraceThreads(void)
{
    if (g_instance.pid_cxt.PgStatPID != 0) {
        signal_child(g_instance.pid_cxt.PgStatPID, SIGQUIT);
    }

    if (g_instance.pid_cxt.AlarmCheckerPID != 0) {
        Assert(!dummyStandbyMode);
        signal_child(g_instance.pid_cxt.AlarmCheckerPID, SIGQUIT);
    }
}

static void NotifyShutdown(void)
{
    will_shutdown = true;
    return;
}

static void NotifyProcessActive(void)
{
    will_shutdown = false;
    return;
}
/*
 * pmdie -- signal handler for processing various postmaster signals.
 */

static void pmdie(SIGNAL_ARGS)
{
    int save_errno = errno;

    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    ereport(DEBUG2, (errmsg_internal("postmaster received signal %d", postgres_signal_arg)));
    NotifyShutdown();

    switch (postgres_signal_arg) {
        case SIGTERM:
        case SIGINT:

            if (STANDBY_MODE == t_thrd.postmaster_cxt.HaShmData->current_mode && !dummyStandbyMode &&
                SIGTERM == postgres_signal_arg) {
                /*
                 * Smart g_instance.status:
                 *
                 * Wait for children to end their work, then shut down.
                 */
                if (g_instance.status >= SmartShutdown)
                    break;

                g_instance.status = SmartShutdown;
                ereport(LOG, (errmsg("received smart shutdown request")));

                /* Audit system stop */
                pgaudit_system_stop_ok(SmartShutdown);
            } else {
                /*
                 * Fast g_instance.status:
                 *
                 * Abort all children with SIGTERM (rollback active transactions
                 * and exit) and shut down when they are gone.
                 */
                if (g_instance.status >= FastShutdown)
                    break;

                g_instance.status = FastShutdown;
                ereport(LOG, (errmsg("received fast shutdown request")));

                /*  Audit system stop */
                pgaudit_system_stop_ok(FastShutdown);
            }

            if (pmState == PM_STARTUP || pmState == PM_INIT) {
                KillGraceThreads();
                WaitGraceThreadsExit();

                // threading: do not clean sema, maybe other thread is using it.
                cancelSemphoreRelease();
                cancelIpcMemoryDetach();

                ExitPostmaster(0);
            }

            if (g_instance.pid_cxt.StartupPID != 0) {
                ereport(LOG, (errmsg("send to startup shutdown request")));
                signal_child(g_instance.pid_cxt.StartupPID, SIGTERM);
            }

#ifdef ENABLE_MULTIPLE_NODES
            if (g_instance.pid_cxt.BarrierPreParsePID != 0) {
                signal_child(g_instance.pid_cxt.BarrierPreParsePID, SIGTERM);
            }
#endif

            if (g_instance.pid_cxt.PageRepairPID != 0) {
                signal_child(g_instance.pid_cxt.PageRepairPID, SIGTERM);
            }

            if (g_instance.pid_cxt.BgWriterPID != 0) {
                Assert(!dummyStandbyMode);
                signal_child(g_instance.pid_cxt.BgWriterPID, SIGTERM);
            }

            if (g_instance.pid_cxt.SpBgWriterPID != 0) {
                Assert(!dummyStandbyMode);
                signal_child(g_instance.pid_cxt.SpBgWriterPID, SIGTERM);
            }

            if (g_instance.pid_cxt.WalRcvWriterPID != 0)
                signal_child(g_instance.pid_cxt.WalRcvWriterPID, SIGTERM);

            if (g_instance.pid_cxt.WalReceiverPID != 0)
                signal_child(g_instance.pid_cxt.WalReceiverPID, SIGTERM);

            if (g_instance.pid_cxt.DataRcvWriterPID != 0)
                signal_child(g_instance.pid_cxt.DataRcvWriterPID, SIGTERM);

            if (g_instance.pid_cxt.DataReceiverPID != 0)
                signal_child(g_instance.pid_cxt.DataReceiverPID, SIGTERM);

            if (g_instance.pid_cxt.FaultMonitorPID != 0)
                signal_child(g_instance.pid_cxt.FaultMonitorPID, SIGTERM);

            if (g_instance.pid_cxt.TwoPhaseCleanerPID != 0)
                signal_child(g_instance.pid_cxt.TwoPhaseCleanerPID, SIGTERM);

            if (g_instance.pid_cxt.WLMCollectPID != 0) {
                WLMProcessThreadShutDown();
                signal_child(g_instance.pid_cxt.WLMCollectPID, SIGTERM);
            }

            if (g_instance.pid_cxt.TxnSnapCapturerPID != 0) {
                signal_child(g_instance.pid_cxt.TxnSnapCapturerPID, SIGTERM);
            }

            if (g_instance.pid_cxt.RbCleanrPID != 0) {
                signal_child(g_instance.pid_cxt.RbCleanrPID, SIGTERM);
            }

            if (g_instance.pid_cxt.SnapshotPID != 0) {
                WLMProcessThreadShutDown();
                signal_child(g_instance.pid_cxt.SnapshotPID, SIGTERM);
            }

            if (g_instance.pid_cxt.AshPID != 0) {
                WLMProcessThreadShutDown();
                signal_child(g_instance.pid_cxt.AshPID, SIGTERM);
            }

            if (g_instance.pid_cxt.StatementPID != 0) {
                WLMProcessThreadShutDown();
                signal_child(g_instance.pid_cxt.StatementPID, SIGTERM);
            }

            if (g_instance.pid_cxt.PercentilePID != 0) {
                WLMProcessThreadShutDown();
                signal_child(g_instance.pid_cxt.PercentilePID, SIGTERM);
            }
            if (g_instance.pid_cxt.WLMMonitorPID != 0)
                signal_child(g_instance.pid_cxt.WLMMonitorPID, SIGTERM);

            if (g_instance.pid_cxt.WLMArbiterPID != 0)
                signal_child(g_instance.pid_cxt.WLMArbiterPID, SIGTERM);

            if (g_instance.pid_cxt.CPMonitorPID != 0)
                signal_child(g_instance.pid_cxt.CPMonitorPID, SIGTERM);

            if (g_instance.pid_cxt.CBMWriterPID != 0) {
                Assert(!dummyStandbyMode);
                signal_child(g_instance.pid_cxt.CBMWriterPID, SIGTERM);
            }

            if (g_instance.pid_cxt.CommSenderFlowPID != 0) {
                signal_child(g_instance.pid_cxt.CommSenderFlowPID, SIGTERM);
            }

            if (g_instance.pid_cxt.CommReceiverFlowPID != 0) {
                signal_child(g_instance.pid_cxt.CommReceiverFlowPID, SIGTERM);
            }

            if (g_instance.pid_cxt.CommAuxiliaryPID != 0) {
                signal_child(g_instance.pid_cxt.CommAuxiliaryPID, SIGTERM);
            }

            if (g_instance.pid_cxt.CommPoolerCleanPID != 0) {
                signal_child(g_instance.pid_cxt.CommPoolerCleanPID, SIGTERM);
            }

            if (g_instance.pid_cxt.UndoLauncherPID != 0) {
                signal_child(g_instance.pid_cxt.UndoLauncherPID, SIGTERM);
            }
#if !defined(ENABLE_MULTIPLE_NODES) && !defined(ENABLE_LITE_MODE)
            if (g_instance.pid_cxt.ApplyLauncerPID != 0) {
                signal_child(g_instance.pid_cxt.ApplyLauncerPID, SIGTERM);
            }
#endif
            if (g_instance.pid_cxt.GlobalStatsPID != 0) {
                signal_child(g_instance.pid_cxt.GlobalStatsPID, SIGTERM);
            }

            if (g_instance.pid_cxt.CommReceiverPIDS != NULL) {
                int recv_loop = 0;
                for (recv_loop = 0; recv_loop < g_instance.attr.attr_network.comm_max_receiver; recv_loop++) {
                    if (g_instance.pid_cxt.CommReceiverPIDS[recv_loop] != 0) {
                        signal_child(g_instance.pid_cxt.CommReceiverPIDS[recv_loop], SIGTERM);
                    }
                }
            }

#ifndef ENABLE_LITE_MODE
            if (g_instance.pid_cxt.BarrierCreatorPID != 0) {
                barrier_creator_thread_shutdown();
                signal_child(g_instance.pid_cxt.BarrierCreatorPID, SIGTERM);
            }
            if (g_instance.archive_thread_info.obsBarrierArchPID != NULL) {
                int arch_loop = 0;
                for (arch_loop = 0; arch_loop < g_instance.attr.attr_storage.max_replication_slots; arch_loop++) {
                    if (g_instance.archive_thread_info.obsBarrierArchPID[arch_loop] != 0) {
                        signal_child(g_instance.archive_thread_info.obsBarrierArchPID[arch_loop], SIGTERM);
                    }
                }
            }
#endif

#ifdef ENABLE_MULTIPLE_NODES
            if (g_instance.pid_cxt.CsnminSyncPID != 0) {
                csnminsync_thread_shutdown();
                signal_child(g_instance.pid_cxt.CsnminSyncPID, SIGTERM);
            }

            if (g_instance.pid_cxt.TsCompactionPID != 0) {
                signal_child(g_instance.pid_cxt.TsCompactionPID, SIGTERM);
            }

            if (g_instance.pid_cxt.TsCompactionAuxiliaryPID != 0) {
                signal_child(g_instance.pid_cxt.TsCompactionAuxiliaryPID, SIGTERM);
            }

            (void)streaming_backend_manager(STREAMING_BACKEND_SIGTERM);
#endif   /* ENABLE_MULTIPLE_NODES */

            if (g_instance.pid_cxt.UndoRecyclerPID != 0) {
                signal_child(g_instance.pid_cxt.UndoRecyclerPID, SIGTERM);
            }
			
            if (g_instance.pid_cxt.WalWriterAuxiliaryPID != 0) {
                signal_child(g_instance.pid_cxt.WalWriterAuxiliaryPID, SIGTERM);
            }

            if (ENABLE_THREAD_POOL && (pmState == PM_RECOVERY || pmState == PM_STARTUP)) {
                /*
                 * Although there is not connections from client at PM_RECOVERY and PM_STARTUP
                 * state, we still have to close thread pool thread.
                 */
                g_threadPoolControler->ShutDownThreads();
            }

            if (pmState == PM_RECOVERY) {
                /*
                 * Only startup, bgwriter, walreceiver, and/or checkpointer
                 * should be active in this state; we just signaled the first
                 * three, and we don't want to kill checkpointer yet.
                 */
                pmState = PM_WAIT_BACKENDS;
            } else if (pmState == PM_RUN || pmState == PM_WAIT_BACKUP || pmState == PM_WAIT_READONLY ||
                       pmState == PM_WAIT_BACKENDS || pmState == PM_HOT_STANDBY) {
                ereport(LOG, (errmsg("aborting any active transactions")));

                if (ENABLE_THREAD_POOL) {
                    g_threadPoolControler->CloseAllSessions();
                    /* 
                     * before pmState set to wait backends, 
                     * threadpool cannot launch new thread by scheduler during demote.  
                     */
                    g_threadPoolControler->ShutDownScheduler(true, true);
                    g_threadPoolControler->ShutDownThreads();
                }
                /* shut down all backends and autovac workers */
                (void)SignalSomeChildren(SIGTERM, BACKEND_TYPE_NORMAL | BACKEND_TYPE_AUTOVAC);

                /* and the autovac launcher too */
                if (g_instance.pid_cxt.AutoVacPID != 0)
                    signal_child(g_instance.pid_cxt.AutoVacPID, SIGTERM);

                if (g_instance.pid_cxt.PgJobSchdPID != 0)
                    signal_child(g_instance.pid_cxt.PgJobSchdPID, SIGTERM);

                /* and the walwriter too */
                if (g_instance.pid_cxt.WalWriterPID != 0)
                    signal_child(g_instance.pid_cxt.WalWriterPID, SIGTERM);


                pmState = PM_WAIT_BACKENDS;
                if (ENABLE_THREAD_POOL) {
                    g_threadPoolControler->EnableAdjustPool();
                }
            }
            /*
             * Now wait for backends to exit.  If there are none,
             * PostmasterStateMachine will take the next step.
             */
            PostmasterStateMachine();
            break;

        case SIGQUIT:

            /*
             * Immediate g_instance.status:
             *
             * exit whole process, do not send SIGQUIT to child.
             */
            ereport(LOG, (errmsg("received immediate shutdown request")));
            /* Audit system stop */
            pgaudit_system_stop_ok(ImmediateShutdown);
            g_instance.status = ImmediateShutdown;

            KillGraceThreads();
            WaitGraceThreadsExit();

            // threading: do not clean sema, maybe other thread is using it.
            //
            cancelSemphoreRelease();
            cancelIpcMemoryDetach();

            ExitPostmaster(0);
            break;
        default:
            break;
    }

    NotifyProcessActive();

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);

    errno = save_errno;
}

/* set walsenders node state */
static void SetWalsndsNodeState(ClusterNodeState requester, ClusterNodeState others)
{
    int i;

    /* update the demote state */
    for (i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        volatile WalSnd* walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[i];

        if (0 == walsnd->pid)
            continue;

        if (walsnd->node_state >= NODESTATE_SMART_DEMOTE_REQUEST && walsnd->node_state <= NODESTATE_FAST_DEMOTE_REQUEST)
            walsnd->node_state = requester;
        else
            walsnd->node_state = others;
    }
}

static void UpdateArchiveSlotStatus()
{
    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
        ReplicationSlot *slot = NULL;
        slot = &t_thrd.slot_cxt.ReplicationSlotCtl->replication_slots[i];
        SpinLockAcquire(&slot->mutex);
        if (!slot->active && slot->in_use && slot->extra_content != NULL &&
            GET_SLOT_PERSISTENCY(slot->data) != RS_BACKUP) {
            slot->active = true;
        }
        SpinLockRelease(&slot->mutex);
        ereport(LOG, (errmsg("Archive slot change to active when DBstatus Promoting")));
    }
}

/* prepare to response to standby for switchover */
static void PrepareDemoteResponse(void)
{
    if (NoDemote == g_instance.demotion)
        return;

    SetWalsndsNodeState(NODESTATE_PROMOTE_APPROVE, NODESTATE_STANDBY_REDIRECT);

    /*
     * For standby demote to a cascade standby, it is safe
     * to change servermode here when promote has been approved.
     */
    if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
        volatile HaShmemData* hashmdata = t_thrd.postmaster_cxt.HaShmData;

        /* A standby instance will be demote to a cascade standby */
        SpinLockAcquire(&hashmdata->mutex);
        hashmdata->current_mode = STANDBY_MODE;
        hashmdata->is_cascade_standby = true;
        SpinLockRelease(&hashmdata->mutex);

        g_instance.global_sysdbcache.RefreshHotStandby();
        load_server_mode();
    }

    allow_immediate_pgstat_restart();
}

/* * process demote request from standby */
static void ProcessDemoteRequest(void)
{
    DemoteMode mode;

    /* The temperary solution is to exit Gauss when demoting happened in DCF mode */
    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf) {
        /* Don't free share memory */
        ereport(LOG, (errmsg("Exit postmaster when demoting.")));
        HandleChildCrash(t_thrd.proc_cxt.MyProcPid, 1, t_thrd.proc_cxt.MyProgName);
    }

    /* get demote request type */
    mode = t_thrd.walsender_cxt.WalSndCtl->demotion;

    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf || g_instance.wal_cxt.upgradeSwitchMode == ExtremelyFast)
        mode = FastDemote;

    if (NoDemote == mode)
        return;

    /* check the postmaster state */
    if (pmState != PM_RUN && pmState != PM_HOT_STANDBY && NoDemote == g_instance.demotion) {
        SetWalsndsNodeState(NODESTATE_NORMAL, NODESTATE_NORMAL);
        t_thrd.walsender_cxt.WalSndCtl->demotion = NoDemote;
        ereport(NOTICE, (errmsg("postmaster state not in PM_RUN or PM_HOT_STANDBY, it would not demote.")));
        return;
    }

    PMUpdateDBState(DEMOTING_STATE, get_cur_mode(), get_cur_repl_num());
    ereport(LOG,
        (errmsg("update gaussdb state file: db state(DEMOTING_STATE), server mode(%s)",
            wal_get_role_string(get_cur_mode()))));
    switch (mode) {
        case SmartDemote:
            /*
             * Smart Demote:
             *
             * Wait for children to end their work, then start up as standby.
             */
            if (g_instance.demotion >= SmartDemote)
                break;
            g_instance.demotion = SmartDemote;
            ereport(LOG, (errmsg("received smart demote request")));

            if (pmState == PM_RUN || pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY || pmState == PM_STARTUP) {
                /* autovacuum workers are told to shut down immediately */
                (void)SignalSomeChildren(SIGTERM, BACKEND_TYPE_AUTOVAC);
                /* and the autovac launcher too */
                if (g_instance.pid_cxt.AutoVacPID != 0)
                    signal_child(g_instance.pid_cxt.AutoVacPID, SIGTERM);

                if (g_instance.pid_cxt.PgJobSchdPID != 0)
                    signal_child(g_instance.pid_cxt.PgJobSchdPID, SIGTERM);

                if ((IS_PGXC_COORDINATOR) && g_instance.pid_cxt.CommPoolerCleanPID != 0)
                    signal_child(g_instance.pid_cxt.CommPoolerCleanPID, SIGTERM);

                /* and the bgwriter too */
                if (g_instance.pid_cxt.BgWriterPID != 0) {
                    Assert(!dummyStandbyMode);
                    signal_child(g_instance.pid_cxt.BgWriterPID, SIGTERM);
                }
                if (g_instance.pid_cxt.SpBgWriterPID != 0) {
                    Assert(!dummyStandbyMode);
                    signal_child(g_instance.pid_cxt.SpBgWriterPID, SIGTERM);
                }

                if (g_instance.pid_cxt.TwoPhaseCleanerPID != 0)
                    signal_child(g_instance.pid_cxt.TwoPhaseCleanerPID, SIGTERM);

                /* and the walwriter too */
                if (g_instance.pid_cxt.WalWriterPID != 0)
                    signal_child(g_instance.pid_cxt.WalWriterPID, SIGTERM);

                if (g_instance.pid_cxt.WalWriterAuxiliaryPID != 0)
                    signal_child(g_instance.pid_cxt.WalWriterAuxiliaryPID, SIGTERM);

                if (g_instance.pid_cxt.CBMWriterPID != 0) {
                    Assert(!dummyStandbyMode);
                    signal_child(g_instance.pid_cxt.CBMWriterPID, SIGTERM);
                }

                if (g_instance.pid_cxt.UndoLauncherPID != 0)
                    signal_child(g_instance.pid_cxt.UndoLauncherPID, SIGTERM);
#if !defined(ENABLE_MULTIPLE_NODES) && !defined(ENABLE_LITE_MODE)
                if (g_instance.pid_cxt.ApplyLauncerPID != 0)
                    signal_child(g_instance.pid_cxt.ApplyLauncerPID, SIGTERM);
#endif
                if (g_instance.pid_cxt.GlobalStatsPID != 0)
                    signal_child(g_instance.pid_cxt.GlobalStatsPID, SIGTERM);

                if (g_instance.pid_cxt.UndoRecyclerPID != 0)
                    signal_child(g_instance.pid_cxt.UndoRecyclerPID, SIGTERM);
#ifdef ENABLE_MULTIPLE_NODES
                (void)streaming_backend_manager(STREAMING_BACKEND_SIGTERM);
#endif   /* ENABLE_MULTIPLE_NODES */

                StopAliveBuildSender();
                /*
                 * If we're in recovery, we can't kill the startup process
                 * right away, because at present doing so does not release
                 * its locks.  We might want to change this in a future
                 * release.  For the time being, the PM_WAIT_READONLY state
                 * indicates that we're waiting for the regular (read only)
                 * backends to die off; once they do, we'll kill the startup
                 * and walreceiver processes.
                 */
                pmState = (pmState == PM_RUN) ? PM_WAIT_BACKUP : PM_WAIT_READONLY;
            }

            break;

        case FastDemote:
            /*
             * Fast Demote:
             *
             * Abort all children with SIGTERM (rollback active transactions
             * and exit) and start up when they are gone.
             */
            if (g_instance.demotion >= FastDemote)
                break;
            g_instance.demotion = FastDemote;
            ereport(LOG, (errmsg("received fast demote request")));

            if (g_instance.pid_cxt.StartupPID != 0)
                signal_child(g_instance.pid_cxt.StartupPID, SIGTERM);

            if (g_instance.pid_cxt.PageRepairPID != 0) {
                signal_child(g_instance.pid_cxt.PageRepairPID, SIGTERM);
            }

            if (g_instance.pid_cxt.BgWriterPID != 0) {
                Assert(!dummyStandbyMode);
                signal_child(g_instance.pid_cxt.BgWriterPID, SIGTERM);
            }

            if (g_instance.pid_cxt.SpBgWriterPID != 0) {
                Assert(!dummyStandbyMode);
                signal_child(g_instance.pid_cxt.SpBgWriterPID, SIGTERM);
            }

            if (g_instance.pid_cxt.WalRcvWriterPID != 0)
                signal_child(g_instance.pid_cxt.WalRcvWriterPID, SIGTERM);

            if (g_instance.pid_cxt.WalReceiverPID != 0)
                signal_child(g_instance.pid_cxt.WalReceiverPID, SIGTERM);

            if (g_instance.pid_cxt.DataRcvWriterPID != 0)
                signal_child(g_instance.pid_cxt.DataRcvWriterPID, SIGTERM);

            if (g_instance.pid_cxt.DataReceiverPID != 0)
                signal_child(g_instance.pid_cxt.DataReceiverPID, SIGTERM);

            if (g_instance.pid_cxt.TwoPhaseCleanerPID != 0)
                signal_child(g_instance.pid_cxt.TwoPhaseCleanerPID, SIGTERM);

            if (g_instance.pid_cxt.WLMCollectPID != 0) {
                WLMProcessThreadShutDown();
                signal_child(g_instance.pid_cxt.WLMCollectPID, SIGTERM);
            }

            if (g_instance.pid_cxt.WLMMonitorPID != 0)
                signal_child(g_instance.pid_cxt.WLMMonitorPID, SIGTERM);

            if (g_instance.pid_cxt.WLMArbiterPID != 0)
                signal_child(g_instance.pid_cxt.WLMArbiterPID, SIGTERM);

            if (g_instance.pid_cxt.CPMonitorPID != 0)
                signal_child(g_instance.pid_cxt.CPMonitorPID, SIGTERM);

            if (g_instance.pid_cxt.FaultMonitorPID != 0)
                signal_child(g_instance.pid_cxt.FaultMonitorPID, SIGTERM);

            if (g_instance.pid_cxt.CBMWriterPID != 0) {
                Assert(!dummyStandbyMode);
                signal_child(g_instance.pid_cxt.CBMWriterPID, SIGTERM);
            }

            /* as single_node model will start WLM & Snaphost, but dont't terminate it whiele switchover, now kill it */
            if (g_instance.pid_cxt.WLMCollectPID != 0) {
                Assert(!dummyStandbyMode);
                signal_child(g_instance.pid_cxt.WLMCollectPID, SIGTERM);
            }

            if (g_instance.pid_cxt.TxnSnapCapturerPID != 0) {
                Assert(!dummyStandbyMode);
                signal_child(g_instance.pid_cxt.TxnSnapCapturerPID, SIGTERM);
            }

            if (g_instance.pid_cxt.RbCleanrPID != 0) {
                Assert(!dummyStandbyMode);
                signal_child(g_instance.pid_cxt.RbCleanrPID, SIGTERM);
            }

            if (g_instance.pid_cxt.SnapshotPID != 0) {
                Assert(!dummyStandbyMode);
                signal_child(g_instance.pid_cxt.SnapshotPID, SIGTERM);
            }

            if (g_instance.pid_cxt.AshPID!= 0) {
                Assert(!dummyStandbyMode);
                signal_child(g_instance.pid_cxt.AshPID, SIGTERM);
            }

            if (g_instance.pid_cxt.StatementPID!= 0) {
                Assert(!dummyStandbyMode);
                signal_child(g_instance.pid_cxt.StatementPID, SIGTERM);
            }

            if (g_instance.pid_cxt.PercentilePID != 0) {
                Assert(!dummyStandbyMode);
                signal_child(g_instance.pid_cxt.PercentilePID, SIGTERM);
            }

#ifndef ENABLE_LITE_MODE
            if (g_instance.pid_cxt.BarrierCreatorPID != 0) {
                barrier_creator_thread_shutdown();
                signal_child(g_instance.pid_cxt.BarrierCreatorPID, SIGTERM);
            }
#endif

#ifdef ENABLE_MULTIPLE_NODES
            if (g_instance.pid_cxt.CsnminSyncPID != 0) {
                csnminsync_thread_shutdown();
                signal_child(g_instance.pid_cxt.CsnminSyncPID, SIGTERM);
            }

            if (g_instance.pid_cxt.TsCompactionPID != 0) {
                Assert(!dummyStandbyMode);
                signal_child(g_instance.pid_cxt.TsCompactionPID, SIGTERM);
            }

            if (g_instance.pid_cxt.TsCompactionAuxiliaryPID != 0) {
                Assert(!dummyStandbyMode);
                signal_child(g_instance.pid_cxt.TsCompactionAuxiliaryPID, SIGTERM);
            }

            (void)streaming_backend_manager(STREAMING_BACKEND_SIGTERM);

#endif   /* ENABLE_MULTIPLE_NODES */

            if (pmState == PM_RECOVERY) {
                /*
                 * Only startup, bgwriter, and checkpointer should be active
                 * in this state; we just signaled the first two, and we don't
                 * want to kill checkpointer yet.
                 */
                pmState = PM_WAIT_BACKENDS;
            } else if (pmState == PM_RUN || pmState == PM_WAIT_BACKUP || pmState == PM_WAIT_READONLY ||
                       pmState == PM_WAIT_BACKENDS || pmState == PM_HOT_STANDBY) {
                ereport(LOG, (errmsg("aborting any active transactions")));

                if (ENABLE_THREAD_POOL) {
                    g_threadPoolControler->CloseAllSessions();
                    g_threadPoolControler->ShutDownScheduler(true, true);
                    g_threadPoolControler->ShutDownThreads();
                }
                /* shut down all backends and autovac workers */
                (void)SignalSomeChildren(SIGTERM, BACKEND_TYPE_NORMAL | BACKEND_TYPE_AUTOVAC);

                /* and the autovac launcher too */
                if (g_instance.pid_cxt.AutoVacPID != 0)
                    signal_child(g_instance.pid_cxt.AutoVacPID, SIGTERM);

                if (g_instance.pid_cxt.UndoLauncherPID != 0)
                    signal_child(g_instance.pid_cxt.UndoLauncherPID, SIGTERM);
#if !defined(ENABLE_MULTIPLE_NODES) && !defined(ENABLE_LITE_MODE)
                if (g_instance.pid_cxt.ApplyLauncerPID != 0)
                    signal_child(g_instance.pid_cxt.ApplyLauncerPID, SIGTERM);
#endif
                if (g_instance.pid_cxt.GlobalStatsPID != 0)
                    signal_child(g_instance.pid_cxt.GlobalStatsPID, SIGTERM);

                if (g_instance.pid_cxt.UndoRecyclerPID != 0)
                    signal_child(g_instance.pid_cxt.UndoRecyclerPID, SIGTERM);

                if (g_instance.pid_cxt.PgJobSchdPID != 0)
                    signal_child(g_instance.pid_cxt.PgJobSchdPID, SIGTERM);

                if ((IS_PGXC_COORDINATOR) && g_instance.pid_cxt.CommPoolerCleanPID != 0)
                    signal_child(g_instance.pid_cxt.CommPoolerCleanPID, SIGTERM);

                /* and the walwriter too */
                if (g_instance.pid_cxt.WalWriterPID != 0)
                    signal_child(g_instance.pid_cxt.WalWriterPID, SIGTERM);
                StopAliveBuildSender();

                if (g_instance.pid_cxt.WalWriterAuxiliaryPID != 0)
                    signal_child(g_instance.pid_cxt.WalWriterAuxiliaryPID, SIGTERM);

                pmState = PM_WAIT_BACKENDS;
                if (ENABLE_THREAD_POOL) {
                    g_threadPoolControler->EnableAdjustPool();
                }
            }
            break;

        default:
            break;
    }

    /* Reset the seqno for matview */
    MatviewShmemSetInvalid();

    /*
     * Now wait for backends to exit. If there are none,
     * PostmasterStateMachine will take the next step.
     */
    PostmasterStateMachine();
}

/*
 * Reaper -- get current time.
 */
static void GetTimeNowForReaperLog(char* nowTime, int timeLen)
{
    time_t formatTime;
    struct timeval current = {0};
    const int tmpBufSize = 32;
    char tmpBuf[tmpBufSize] = {0};

    if (nowTime == NULL || timeLen == 0) {
        return;
    }

    (void)gettimeofday(&current, NULL);
    formatTime = current.tv_sec;
    struct tm* pTime = localtime(&formatTime);
    strftime(tmpBuf, sizeof(tmpBuf), "%Y-%m-%d %H:%M:%S", pTime);

    errno_t rc = sprintf_s(nowTime, timeLen - 1, "%s.%ld ", tmpBuf, current.tv_usec / 1000);
    securec_check_ss(rc, "\0", "\0");
}

/*
 * Reaper -- encap reaper prefix log.
 */
static char* GetReaperLogPrefix(char* buf, int bufLen)
{
    const int bufSize = 256;
    char timeBuf[bufSize] = {0};
    errno_t rc;

    GetTimeNowForReaperLog(timeBuf, bufSize);

    rc = memset_s(buf, bufLen, 0, bufLen);
    securec_check(rc, "\0", "\0");
    rc = sprintf_s(buf, bufLen - 1, "%s [postmaster][reaper][%lu]", timeBuf, PostmasterPid);
    securec_check_ss(rc, "\0", "\0");

    return buf;
}

/*
 * Reaper -- signal handler to cleanup after a child process dies.
 */
static void reaper(SIGNAL_ARGS)
{
    int save_errno = errno;
    ThreadId pid;    /* process id of dead child process */
    long exitstatus; /* its exit status */
    int* status = NULL;
    ThreadId oldpid = 0;
    char logBuf[ReaperLogBufSize] = {0};

#define LOOPTEST() (pid = gs_thread_id(t_thrd.postmaster_cxt.CurExitThread))
#define LOOPHEADER() (exitstatus = (long)(intptr_t)status)

    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    for (;;) {
        LOOPTEST();

        if (pid == oldpid) {
            break;
        }

        oldpid = pid;
        if (gs_thread_join(t_thrd.postmaster_cxt.CurExitThread, (void**)&status) != 0) {
            /*
             * If the thread does not exist, treat it as normal exit and we continue to
             * do our clean-up work. Otherwise, we treate it as crashed 'cause we do
             * not know the current status of the thread and it's better to quit directly
             * which sames more safely.
             */
            if (ESRCH == pthread_kill(pid, 0)) {
                exitstatus = 0;
                write_stderr("%s LOG: failed to join thread %lu, no such process\n",
                    GetReaperLogPrefix(logBuf, ReaperLogBufSize), pid);
            } else {
                exitstatus = 1;
                HandleChildCrash(pid, exitstatus, _(GetProcName(pid)));
            }
        } else {
            LOOPHEADER();
        }

        /*
         * Check if this child was a startup process.
         */
        if (pid == g_instance.pid_cxt.StartupPID) {
            g_instance.pid_cxt.StartupPID = 0;
            write_stderr("%s LOG: startupprocess exit\n", GetReaperLogPrefix(logBuf, ReaperLogBufSize));

            /*
             * Startup process exited in response to a shutdown request (or it
             * completed normally regardless of the shutdown request).
             */
            if (g_instance.status > NoShutdown && (EXIT_STATUS_0(exitstatus) || EXIT_STATUS_1(exitstatus))) {
                pmState = PM_WAIT_BACKENDS;
                /* PostmasterStateMachine logic does the rest */
                continue;
            }

            /*
             * Startup process exited in response to a standby demote request.
             */
            if (g_instance.demotion > NoDemote &&
                t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE &&
                (EXIT_STATUS_0(exitstatus) || EXIT_STATUS_1(exitstatus))) {
                pmState = PM_WAIT_BACKENDS;
                /* PostmasterStateMachine logic does the rest */
                continue;
            }

            /*
             * Unexpected exit of startup process (including FATAL exit)
             * during PM_STARTUP is treated as catastrophic. There are no
             * other processes running yet, so we can just exit.
             */
            if (pmState == PM_STARTUP && !EXIT_STATUS_0(exitstatus)) {
                LogChildExit(LOG, _("startup process"), pid, exitstatus);
                write_stderr("%s LOG: aborting startup due to startup process failure\n",
                    GetReaperLogPrefix(logBuf, ReaperLogBufSize));
                if (get_real_recovery_parallelism() > 1) {
                    HandleChildCrash(pid, exitstatus, _("startup process"));
                } else {
                    /* Shut down threadpool worker before exit. */
                    if (ENABLE_THREAD_POOL) {
                        g_threadPoolControler->ShutDownThreads(true);
                        g_threadPoolControler->ShutDownListeners(true);
                        g_threadPoolControler->ShutDownScheduler(true, false);
                    }
                    ExitPostmaster(1);
                }
            }

            /*
             * After PM_STARTUP, any unexpected exit (including FATAL exit) of
             * the startup process is catastrophic, so kill other children,
             * and set RecoveryError so we don't try to reinitialize after
             * they're gone.  Exception: if g_instance.fatal_error is already set, that
             * implies we previously sent the startup process a SIGQUIT, so
             * that's probably the reason it died, and we do want to try to
             * restart in that case.
             */
            if (!EXIT_STATUS_0(exitstatus)) {
                if (!g_instance.fatal_error)
                    g_instance.recover_error = true;

                HandleChildCrash(pid, exitstatus, _("startup process"));
                continue;
            }

            /* startup process exit, standby pagerepair thread can exit */
            if (g_instance.pid_cxt.PageRepairPID != 0) {
                signal_child(g_instance.pid_cxt.PageRepairPID, SIGTERM);
            }

            /*
             * Startup succeeded, commence normal operations
             */
            g_instance.fatal_error = false;
            g_instance.demotion = NoDemote;
            t_thrd.postmaster_cxt.ReachedNormalRunning = true;
            pmState = PM_RUN;

            if (t_thrd.postmaster_cxt.HaShmData && (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE ||
                                                       t_thrd.postmaster_cxt.HaShmData->current_mode == PENDING_MODE)) {
                t_thrd.postmaster_cxt.HaShmData->current_mode = PRIMARY_MODE;
                UpdateArchiveSlotStatus();
                g_instance.global_sysdbcache.RefreshHotStandby();
                if (g_instance.pid_cxt.HeartbeatPID != 0)
                    signal_child(g_instance.pid_cxt.HeartbeatPID, SIGTERM);
                UpdateOptsFile();
                if (t_thrd.walreceiverfuncs_cxt.WalRcv)
                    t_thrd.walreceiverfuncs_cxt.WalRcv->node_state = NODESTATE_NORMAL;
            }

            /*
             * Kill any walsenders to force the downstream standby(s) to
             * reread the timeline history file, adjust their timelines and
             * establish replication connections again. This is required
             * because the timeline of cascading standby is not consistent
             * with that of cascaded one just after failover. We LOG this
             * message since we need to leave a record to explain this
             * disconnection.
             *
             * XXX should avoid the need for disconnection. When we do,
             * am_cascading_walsender should be replaced with
             * RecoveryInProgress()
             */
            if (g_instance.attr.attr_storage.max_wal_senders > 0 && CountChildren(BACKEND_TYPE_WALSND) > 0) {
                write_stderr("%s LOG: terminating all walsender processes to force cascaded "
                        "standby(s) to update timeline and reconnect\n",
                        GetReaperLogPrefix(logBuf, ReaperLogBufSize));
                (void)SignalSomeChildren(SIGUSR2, BACKEND_TYPE_WALSND);
            }

            /*
             * Crank up the background tasks, if we didn't do that already
             * when we entered consistent recovery state.  It doesn't matter
             * if this fails, we'll just try again later.
             */
            if (g_instance.pid_cxt.CheckpointerPID == 0 && !dummyStandbyMode)
                g_instance.pid_cxt.CheckpointerPID = initialize_util_thread(CHECKPOINT_THREAD);

            if (g_instance.pid_cxt.BgWriterPID == 0 && !dummyStandbyMode && !ENABLE_INCRE_CKPT) {
                g_instance.pid_cxt.BgWriterPID = initialize_util_thread(BGWRITER);
            }
            if (g_instance.pid_cxt.SpBgWriterPID == 0 && !dummyStandbyMode) {
                g_instance.pid_cxt.SpBgWriterPID = initialize_util_thread(SPBGWRITER);
            }

            if (!dummyStandbyMode && ENABLE_INCRE_CKPT) {
                for (int i = 0; i < g_instance.ckpt_cxt_ctl->pgwr_procs.num; i++) {
                    if (g_instance.pid_cxt.PageWriterPID[i] == 0) {
                        g_instance.pid_cxt.PageWriterPID[i] = initialize_util_thread(PAGEWRITER_THREAD);
                    }
                }
            }

            if (g_instance.pid_cxt.WalWriterPID == 0)
                g_instance.pid_cxt.WalWriterPID = initialize_util_thread(WALWRITER);

            if (g_instance.pid_cxt.WalWriterAuxiliaryPID == 0)
                g_instance.pid_cxt.WalWriterAuxiliaryPID = initialize_util_thread(WALWRITERAUXILIARY);

            if (g_instance.pid_cxt.CBMWriterPID == 0 && !dummyStandbyMode &&
                u_sess->attr.attr_storage.enable_cbm_tracking)
                g_instance.pid_cxt.CBMWriterPID = initialize_util_thread(CBMWRITER);

            /*
             * Likewise, start other special children as needed.  In a restart
             * situation, some of them may be alive already.
             */
            if (!u_sess->proc_cxt.IsBinaryUpgrade && AutoVacuumingActive() && g_instance.pid_cxt.AutoVacPID == 0 &&
                !dummyStandbyMode && u_sess->attr.attr_common.upgrade_mode != 1)
                g_instance.pid_cxt.AutoVacPID = initialize_util_thread(AUTOVACUUM_LAUNCHER);

            /* Before GRAND VERSION NUM 81000, we do not support scheduled job. */
            if (g_instance.pid_cxt.PgJobSchdPID == 0 &&
                g_instance.attr.attr_sql.job_queue_processes && u_sess->attr.attr_common.upgrade_mode != 1)
                g_instance.pid_cxt.PgJobSchdPID = initialize_util_thread(JOB_SCHEDULER);

            if ((IS_PGXC_COORDINATOR) && g_instance.pid_cxt.CommPoolerCleanPID == 0 &&
                u_sess->attr.attr_common.upgrade_mode != 1) {
                StartPoolCleaner();
            }

            if (g_instance.pid_cxt.sharedStorageXlogCopyThreadPID == 0 && !dummyStandbyMode && 
                g_instance.attr.attr_storage.xlog_file_path != NULL) {
                g_instance.pid_cxt.sharedStorageXlogCopyThreadPID = initialize_util_thread(SHARE_STORAGE_XLOG_COPYER);
            }

            if (g_instance.attr.attr_storage.enable_ustore && g_instance.pid_cxt.UndoLauncherPID == 0 && !dummyStandbyMode)
                g_instance.pid_cxt.UndoLauncherPID = initialize_util_thread(UNDO_LAUNCHER);

            if (g_instance.attr.attr_storage.enable_ustore && g_instance.pid_cxt.GlobalStatsPID == 0 && !dummyStandbyMode)
                g_instance.pid_cxt.GlobalStatsPID = initialize_util_thread(GLOBALSTATS_THREAD);

#if !defined(ENABLE_MULTIPLE_NODES) && !defined(ENABLE_LITE_MODE)
            if (u_sess->attr.attr_common.upgrade_mode == 0 &&
                g_instance.pid_cxt.ApplyLauncerPID == 0 && !dummyStandbyMode) {
                g_instance.pid_cxt.ApplyLauncerPID = initialize_util_thread(APPLY_LAUNCHER);
            }
#endif

            if (XLogArchivingActive() && g_instance.pid_cxt.PgArchPID == 0 && !dummyStandbyMode && 
                    XLogArchiveCommandSet())
                g_instance.pid_cxt.PgArchPID = pgarch_start();
            
            if (!dummyStandbyMode && g_instance.archive_obs_cxt.archive_slot_num != 0 && 
                    g_instance.archive_thread_info.obsArchPID != NULL) {
                ArchObsThreadManage();
            }

            if (g_instance.pid_cxt.PgStatPID == 0 && !dummyStandbyMode)
                g_instance.pid_cxt.PgStatPID = pgstat_start();

            if (ENABLE_TCAP_VERSION && (g_instance.role == VSINGLENODE) && pmState == PM_RUN && g_instance.pid_cxt.TxnSnapCapturerPID == 0 && !dummyStandbyMode)
                g_instance.pid_cxt.TxnSnapCapturerPID = StartTxnSnapCapturer();

            if (ENABLE_TCAP_RECYCLEBIN && (g_instance.role == VSINGLENODE) && pmState == PM_RUN && g_instance.pid_cxt.RbCleanrPID== 0 && !dummyStandbyMode)
                g_instance.pid_cxt.RbCleanrPID = StartRbCleaner();

            if ((IS_PGXC_COORDINATOR || (g_instance.role == VSINGLENODE)) && u_sess->attr.attr_common.enable_wdr_snapshot &&
                g_instance.pid_cxt.SnapshotPID == 0 && !dummyStandbyMode)
                g_instance.pid_cxt.SnapshotPID = snapshot_start();
            if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && u_sess->attr.attr_common.enable_instr_rt_percentile &&
                g_instance.pid_cxt.PercentilePID == 0 && !dummyStandbyMode)
                g_instance.pid_cxt.PercentilePID = initialize_util_thread(PERCENTILE_WORKER);

            if (ENABLE_ASP && g_instance.pid_cxt.AshPID == 0 && !dummyStandbyMode)
                g_instance.pid_cxt.AshPID = initialize_util_thread(ASH_WORKER);

            if (ENABLE_STATEMENT_TRACK && g_instance.pid_cxt.StatementPID == 0)
                g_instance.pid_cxt.StatementPID = initialize_util_thread(TRACK_STMT_WORKER);

            /* Database Security: Support database audit */
            /*  start auditor process */
            /* start the audit collector as needed. */
            if (u_sess->attr.attr_security.Audit_enabled && !dummyStandbyMode) {
                pgaudit_start_all();
            }

            if (t_thrd.postmaster_cxt.audit_primary_start && !t_thrd.postmaster_cxt.audit_primary_failover &&
                !t_thrd.postmaster_cxt.audit_standby_switchover) {
                pgaudit_system_start_ok(g_instance.attr.attr_network.PostPortNumber);
                t_thrd.postmaster_cxt.audit_primary_start = false;
            }

#ifndef ENABLE_LITE_MODE
            if (
#ifdef ENABLE_MULTIPLE_NODES
                IS_PGXC_COORDINATOR &&
#else
                (t_thrd.postmaster_cxt.HaShmData->current_mode == NORMAL_MODE ||
                 t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE) &&
#endif
                u_sess->attr.attr_common.upgrade_mode != 1 &&
                g_instance.pid_cxt.TwoPhaseCleanerPID == 0)
                g_instance.pid_cxt.TwoPhaseCleanerPID = initialize_util_thread(TWOPASECLEANER);

            if (g_instance.pid_cxt.FaultMonitorPID == 0)
                g_instance.pid_cxt.FaultMonitorPID = initialize_util_thread(FAULTMONITOR);
#endif

            /* if workload manager is off, we still use this thread to build user hash table */
            if ((ENABLE_WORKLOAD_CONTROL || !WLMIsInfoInit()) && g_instance.pid_cxt.WLMCollectPID == 0 &&
                !dummyStandbyMode) {
                /* DN need rebuild hash when upgrade to primary */
                if (IS_PGXC_DATANODE)
                    g_instance.wlm_cxt->stat_manager.infoinit = 0;
                g_instance.pid_cxt.WLMCollectPID = initialize_util_thread(WLM_WORKER);
            }

            if (ENABLE_WORKLOAD_CONTROL && (g_instance.pid_cxt.WLMMonitorPID == 0) && !dummyStandbyMode)
                g_instance.pid_cxt.WLMMonitorPID = initialize_util_thread(WLM_MONITOR);

            if (ENABLE_WORKLOAD_CONTROL && (g_instance.pid_cxt.WLMArbiterPID == 0) && !dummyStandbyMode)
                g_instance.pid_cxt.WLMArbiterPID = initialize_util_thread(WLM_ARBITER);

            if (IS_PGXC_COORDINATOR && g_instance.attr.attr_sql.max_resource_package &&
                (g_instance.pid_cxt.CPMonitorPID == 0) && !dummyStandbyMode)
                g_instance.pid_cxt.CPMonitorPID = initialize_util_thread(WLM_CPMONITOR);

            if (NeedHeartbeat())
                g_instance.pid_cxt.HeartbeatPID = initialize_util_thread(HEARTBEAT);

#ifndef ENABLE_LITE_MODE
            if (START_BARRIER_CREATOR && g_instance.pid_cxt.BarrierCreatorPID == 0 &&
                (g_instance.archive_obs_cxt.archive_slot_num != 0 || START_AUTO_CSN_BARRIER)) {
                g_instance.pid_cxt.BarrierCreatorPID = initialize_util_thread(BARRIER_CREATOR);
            }
#endif

            if (GTM_LITE_CN && g_instance.pid_cxt.CsnminSyncPID == 0) {
                g_instance.pid_cxt.CsnminSyncPID = initialize_util_thread(CSNMIN_SYNC);
            }
            if (IS_CN_DISASTER_RECOVER_MODE && g_instance.pid_cxt.CsnminSyncPID == 0) {
                g_instance.pid_cxt.CsnminSyncPID = initialize_util_thread(CSNMIN_SYNC);
            }

            PMUpdateDBState(NORMAL_STATE, get_cur_mode(), get_cur_repl_num());
            write_stderr("%s LOG: update gaussdb state file: db state(NORMAL_STATE), server mode(%s)\n",
                GetReaperLogPrefix(logBuf, ReaperLogBufSize), wal_get_role_string(get_cur_mode()));

            /* at this point we are really open for business */
            write_stderr("%s LOG: database system is ready to accept connections\n",
                GetReaperLogPrefix(logBuf, ReaperLogBufSize));

            continue;
        }

        if (g_threadPoolControler != NULL && pmState == PM_RUN &&
            pid == g_threadPoolControler->GetScheduler()->GetThreadId() &&
            g_threadPoolControler->GetScheduler()->HasShutDown() == true) {
            g_threadPoolControler->GetScheduler()->StartUp();
            g_threadPoolControler->GetScheduler()->SetShutDown(false);
            continue;
        }

        if (pid == g_instance.pid_cxt.PageRepairPID) {
            g_instance.pid_cxt.PageRepairPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                HandleChildCrash(pid, exitstatus, _("page reapir process"));

            continue;
        }

        /*
         * Was it the bgwriter?  Normal exit can be ignored; we'll start a new
         * one at the next iteration of the postmaster's main loop, if
         * necessary.  Any other exit condition is treated as a crash.
         */
        if (pid == g_instance.pid_cxt.BgWriterPID) {
            Assert(!dummyStandbyMode);
            g_instance.pid_cxt.BgWriterPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                HandleChildCrash(pid, exitstatus, _("background writer process"));

            continue;
        }

        /*
         * Was it the special bgwriter?
         */

        if (pid == g_instance.pid_cxt.SpBgWriterPID) {
            Assert(!dummyStandbyMode);
            g_instance.pid_cxt.SpBgWriterPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                HandleChildCrash(pid, exitstatus, _("invalidate buffer background writer process"));
            continue;
        }

        /*
         * Was it the pagewriter?
         */
        if (ENABLE_INCRE_CKPT) {
            int i;
            for (i = 0; i < g_instance.ckpt_cxt_ctl->pgwr_procs.num; i++) {
                if (pid == g_instance.pid_cxt.PageWriterPID[i]) {
                    Assert(!dummyStandbyMode);
                    g_instance.pid_cxt.PageWriterPID[i] = 0;
                    if (!EXIT_STATUS_0(exitstatus)) {
                        HandleChildCrash(pid, exitstatus, _("page writer process"));
                    }
                    continue;
                }
            }
        }

        /*
         * Was it the checkpointer?
         */
        if (pid == g_instance.pid_cxt.CheckpointerPID|| 
            (pid == g_instance.pid_cxt.sharedStorageXlogCopyThreadPID)) {
            Assert(!dummyStandbyMode);

            if (EXIT_STATUS_0(exitstatus) && pmState == PM_SHUTDOWN) {
                /*
                 * OK, we saw normal exit of the checkpointer after it's been
                 * told to shut down.  We expect that it wrote a shutdown
                 * checkpoint.	(If for some reason it didn't, recovery will
                 * occur on next postmaster start.)
                 *
                 * At this point we should have no normal backend children
                 * left (else we'd not be in PM_SHUTDOWN state) but we might
                 * have dead_end children to wait for.
                 *
                 * If we have an archiver subprocess, tell it to do a last
                 * archive cycle and quit. Likewise, if we have walsender
                 * processes, tell them to send any remaining WAL and quit.
                 */
                Assert(g_instance.status > NoShutdown || g_instance.demotion > NoDemote);

                if (pid == g_instance.pid_cxt.CheckpointerPID) {
                    g_instance.pid_cxt.CheckpointerPID = 0;
                    if ((g_instance.attr.attr_storage.xlog_file_path != NULL) &&
                        (g_instance.pid_cxt.sharedStorageXlogCopyThreadPID != 0)) {
                        signal_child(g_instance.pid_cxt.sharedStorageXlogCopyThreadPID, SIGTERM);
                        write_stderr("%s LOG: checkpoint thread exit and wait for sharestorage\n",
                            GetReaperLogPrefix(logBuf, ReaperLogBufSize));
                        continue;
                    }
                    write_stderr("%s LOG: checkpoint thread exit and nowait for sharestorage\n",
                        GetReaperLogPrefix(logBuf, ReaperLogBufSize));
                }

                if (pid == g_instance.pid_cxt.sharedStorageXlogCopyThreadPID) {
                    g_instance.pid_cxt.sharedStorageXlogCopyThreadPID = 0;
                    write_stderr("%s LOG: sharestorage thread exit\n",
                        GetReaperLogPrefix(logBuf, ReaperLogBufSize));
                }

                /* Waken archiver for the last time */
                if (g_instance.pid_cxt.PgArchPID != 0)
                    signal_child(g_instance.pid_cxt.PgArchPID, SIGUSR2);

                if (g_instance.archive_thread_info.obsArchPID != NULL) {
                    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
                        if (g_instance.archive_thread_info.obsArchPID[i] != 0) {
                            signal_child(g_instance.archive_thread_info.obsArchPID[i], SIGUSR2);
                        }
                    }
                }
                
                if (g_instance.archive_thread_info.obsBarrierArchPID != NULL) {
                    for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
                        if (g_instance.archive_thread_info.obsBarrierArchPID[i] != 0) {
                            signal_child(g_instance.archive_thread_info.obsBarrierArchPID[i], SIGUSR2);
                        }
                    }
                }

                PrepareDemoteResponse();

                /*
                 * Waken all senders for the last time. No regular backends
                 * should be around anymore except catchup process.
                 * When all walsender exit, exit heartbeat thread.
                 */
                SignalChildren(SIGUSR2);
                if (g_instance.pid_cxt.HeartbeatPID != 0) {
                    signal_child(g_instance.pid_cxt.HeartbeatPID, SIGTERM);
                }
                pmState = PM_SHUTDOWN_2;

                /*
                 * We can also shut down the stats collector now; there's
                 * nothing left for it to do.
                 */
                if (g_instance.pid_cxt.PgStatPID != 0)
                    signal_child(g_instance.pid_cxt.PgStatPID, SIGQUIT);

                if (g_instance.pid_cxt.TxnSnapCapturerPID != 0)
                    signal_child(g_instance.pid_cxt.TxnSnapCapturerPID, SIGQUIT);

                if (g_instance.pid_cxt.RbCleanrPID != 0)
                    signal_child(g_instance.pid_cxt.RbCleanrPID, SIGQUIT);

                if (g_instance.pid_cxt.SnapshotPID != 0)
                    signal_child(g_instance.pid_cxt.SnapshotPID, SIGQUIT);

                if (g_instance.pid_cxt.AshPID != 0)
                    signal_child(g_instance.pid_cxt.AshPID, SIGQUIT);

                if (g_instance.pid_cxt.StatementPID != 0)
                    signal_child(g_instance.pid_cxt.StatementPID, SIGQUIT);

                if (g_instance.pid_cxt.PercentilePID != 0)
                    signal_child(g_instance.pid_cxt.PercentilePID, SIGQUIT);

                /*
                 * We can also shut down the audit collector now; there's
                 * nothing left for it to do.
                 */
                if (g_instance.pid_cxt.PgAuditPID != NULL) {
                    pgaudit_stop_all();
                }
            } else {
                /*
                 * Any unexpected exit of the checkpointer (including FATAL
                 * exit) is treated as a crash.
                 */
                HandleChildCrash(pid, exitstatus, _("checkpointer process"));
            }

            continue;
        }

        /*
         * Was it the wal writer?  Normal exit can be ignored; we'll start a
         * new one at the next iteration of the postmaster's main loop, if
         * necessary.  Any other exit condition is treated as a crash.
         */
        if (pid == g_instance.pid_cxt.WalWriterPID) {
            g_instance.pid_cxt.WalWriterPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                HandleChildCrash(pid, exitstatus, _("WAL writer process"));

            continue;
        }

        /*
         * Was it the wal file creator?  Normal exit can be ignored; we'll start a
         * new one at the next iteration of the postmaster's main loop, if
         * necessary.	Any other exit condition is treated as a crash.
         */
        if (pid == g_instance.pid_cxt.WalWriterAuxiliaryPID) {
            g_instance.pid_cxt.WalWriterAuxiliaryPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                HandleChildCrash(pid, exitstatus, _("WAL file creator process"));

            continue;
        }

        /*
         * Was it the wal receiver?  If exit status is zero (normal) or one
         * (FATAL exit), we assume everything is all right just like normal
         * backends.
         */
        if (pid == g_instance.pid_cxt.WalReceiverPID) {
            g_instance.pid_cxt.WalReceiverPID = 0;

            if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
                HandleChildCrash(pid, exitstatus, _("WAL receiver process"));

            continue;
        }

        /*
         * Was it the wal receive writer?  If exit status is zero (normal) or one
         * (FATAL exit), we assume everything is all right just like normal
         * backends.
         */
        if (pid == g_instance.pid_cxt.WalRcvWriterPID) {
            g_instance.pid_cxt.WalRcvWriterPID = 0;
            if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
                HandleChildCrash(pid, exitstatus, _("WAL receive writer process"));
            continue;
        }

        /*
         * Was it the data receiver?  If exit status is zero (normal) or one
         * (FATAL exit), we assume everything is all right just like normal
         * backends.
         */
        if (pid == g_instance.pid_cxt.DataReceiverPID) {
            g_instance.pid_cxt.DataReceiverPID = 0;
            if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
                HandleChildCrash(pid, exitstatus, _("DATA receiver process"));
            continue;
        }

        /*
         * Was it the data receive writer?  If exit status is zero (normal) or one
         * (FATAL exit), we assume everything is all right just like normal
         * backends.
         */
        if (pid == g_instance.pid_cxt.DataRcvWriterPID) {
            g_instance.pid_cxt.DataRcvWriterPID = 0;
            if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
                HandleChildCrash(pid, exitstatus, _("DATA receive writer process"));
            continue;
        }

        /*
         * Was it the data catchup?  If exit status is zero (normal) or one
         * (FATAL exit), we assume everything is all right just like normal
         * backends.
         */
        if (pid == g_instance.pid_cxt.CatchupPID) {
            g_instance.pid_cxt.CatchupPID = 0;
            catchup_online = false;

            /*
             * Catchup likes a normal backend, do standard backend child cleanup by
             * 'CleanupBackend', it will handle child crash, at here, we do not need
             * to handle it like other threads(for example: g_instance.pid_cxt.DataRcvWriterPID);
             */
        }

        /*
         * Was it the autovacuum launcher?	Normal exit can be ignored; we'll
         * start a new one at the next iteration of the postmaster's main
         * loop, if necessary.	Any other exit condition is treated as a
         * crash.
         */
        if (pid == g_instance.pid_cxt.AutoVacPID) {
            g_instance.pid_cxt.AutoVacPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                HandleChildCrash(pid, exitstatus, _("autovacuum launcher process"));

            continue;
        }

        if (pid == g_instance.pid_cxt.PgJobSchdPID) {
            g_instance.pid_cxt.PgJobSchdPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("job scheduler process"), pid, exitstatus);
            continue;
        }

        /*
         * Was it the archiver?  If so, just try to start a new one; no need
         * to force reset of the rest of the system.  (If fail, we'll try
         * again in future cycles of the main loop.).  Unless we were waiting
         * for it to shut down; don't restart it in that case, and
         * PostmasterStateMachine() will advance to the next shutdown step.
         */
        if (pid == g_instance.pid_cxt.PgArchPID) {
            g_instance.pid_cxt.PgArchPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("archiver process"), pid, exitstatus);

            if (XLogArchivingActive() && pmState == PM_RUN) {
                g_instance.pid_cxt.PgArchPID = pgarch_start();
            }
            continue;
        }
        
        if (g_instance.archive_thread_info.obsArchPID != NULL) {
            for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
                if (pid == g_instance.archive_thread_info.obsArchPID[i]) {
                    g_instance.archive_thread_info.obsArchPID[i] = 0;
                    
                    if (!EXIT_STATUS_0(exitstatus))
                        LogChildExit(LOG, _("archiver process"), pid, exitstatus);
                    if (getArchiveReplicationSlotWithName(g_instance.archive_thread_info.slotName[i]) != NULL && 
                        (pmState == PM_RUN || pmState == PM_HOT_STANDBY)) {
                        g_instance.archive_thread_info.obsArchPID[i] = 
                            initialize_util_thread(ARCH, g_instance.archive_thread_info.slotName[i]);
                    }
                    continue;
                }
            }
        }

        if (g_instance.archive_thread_info.obsBarrierArchPID != NULL) {
            for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
                if (pid == g_instance.archive_thread_info.obsBarrierArchPID[i]) {
                    g_instance.archive_thread_info.obsBarrierArchPID[i] = 0;

                    if (!EXIT_STATUS_0(exitstatus))
                        LogChildExit(LOG, _("barrier archiver process"), pid, exitstatus);
                    
                    if (getArchiveReplicationSlotWithName(g_instance.archive_thread_info.slotName[i]) != NULL &&
                        (pmState == PM_RUN || pmState == PM_HOT_STANDBY)) {
                        g_instance.archive_thread_info.obsBarrierArchPID[i] = 
						    initialize_util_thread(BARRIER_ARCH, g_instance.archive_thread_info.slotName[i]);
                    }
                    continue;
                }
            }
        }

        /*
         * Was it the statistics collector?  If so, just try to start a new
         * one; no need to force reset of the rest of the system.  (If fail,
         * we'll try again in future cycles of the main loop.)
         */
        if (pid == g_instance.pid_cxt.PgStatPID) {
            Assert(!dummyStandbyMode);
            g_instance.pid_cxt.PgStatPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("statistics collector process"), pid, exitstatus);

            if (pmState == PM_RUN || pmState == PM_HOT_STANDBY)
                g_instance.pid_cxt.PgStatPID = pgstat_start();
            continue;
        }

        if ((g_instance.role == VSINGLENODE) && pid == g_instance.pid_cxt.TxnSnapCapturerPID) {
            Assert(!dummyStandbyMode);
            g_instance.pid_cxt.TxnSnapCapturerPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("txnsnapcapturer process"), pid, exitstatus);

            if (ENABLE_TCAP_VERSION && pmState == PM_RUN)
                g_instance.pid_cxt.TxnSnapCapturerPID = StartTxnSnapCapturer();
            continue;
        }

        if ((g_instance.role == VSINGLENODE) && pid == g_instance.pid_cxt.RbCleanrPID) {
            Assert(!dummyStandbyMode);
            g_instance.pid_cxt.RbCleanrPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("snapshot capturer process"), pid, exitstatus);

            if (ENABLE_TCAP_RECYCLEBIN && pmState == PM_RUN)
                g_instance.pid_cxt.RbCleanrPID = StartRbCleaner();
            continue;
        }

        if ((IS_PGXC_COORDINATOR || (g_instance.role == VSINGLENODE)) && pid == g_instance.pid_cxt.SnapshotPID) {
            Assert(!dummyStandbyMode);
            g_instance.pid_cxt.SnapshotPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("snapshot collector process"), pid, exitstatus);

            if (pmState == PM_RUN)
                g_instance.pid_cxt.SnapshotPID = snapshot_start();
            continue;
        }

        if (pid == g_instance.pid_cxt.AshPID) {
            Assert(!dummyStandbyMode);
            g_instance.pid_cxt.AshPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("Active session history collector process"), pid, exitstatus);

            if (pmState == PM_RUN && ENABLE_ASP)
                g_instance.pid_cxt.AshPID = initialize_util_thread(ASH_WORKER);
            continue;
        }

        if (pid == g_instance.pid_cxt.StatementPID) {
            Assert(!dummyStandbyMode);
            g_instance.pid_cxt.StatementPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("full SQL statement flush process"), pid, exitstatus);

            if (pmState == PM_RUN && ENABLE_STATEMENT_TRACK)
                g_instance.pid_cxt.StatementPID = initialize_util_thread(TRACK_STMT_WORKER);
            continue;
        }

        if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && pid == g_instance.pid_cxt.PercentilePID) {
            Assert(!dummyStandbyMode);
            g_instance.pid_cxt.PercentilePID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("percentile collector process"), pid, exitstatus);

            if (pmState == PM_RUN)
                g_instance.pid_cxt.PercentilePID = initialize_util_thread(PERCENTILE_WORKER);
            continue;
        }

        /* Database Security: Support database audit */
        /*
         * Was it the system auditor?  If so, try to start a new one.
         */
        if (g_instance.pid_cxt.PgAuditPID != NULL) {
            bool is_audit_thread = false;
            for (int i = 0; i < g_instance.audit_cxt.thread_num; ++i) {
                if (pid == g_instance.pid_cxt.PgAuditPID[i]) {
                    Assert(!dummyStandbyMode);
                    g_instance.pid_cxt.PgAuditPID[i] = 0;
                    is_audit_thread = true; 
                    if (!EXIT_STATUS_0(exitstatus))
                        LogChildExit(LOG, _("system auditor process"), pid, exitstatus);
                    if (pmState == PM_RUN)
                        g_instance.pid_cxt.PgAuditPID[i] = pgaudit_start();
                }
            }

            if (is_audit_thread) {
                continue;
            }
        }

        /* Was it the system logger?  If so, try to start a new one */
        if (pid == g_instance.pid_cxt.SysLoggerPID) {
            g_instance.pid_cxt.SysLoggerPID = 0;
            /* for safety's sake, launch new logger *first* */
            g_instance.pid_cxt.SysLoggerPID = SysLogger_Start();

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("system logger process"), pid, exitstatus);

            continue;
        }

        /* Was it the reaper backend thead ?  If so, try to start a new one */
        if (pid == g_instance.pid_cxt.ReaperBackendPID) {
            g_instance.pid_cxt.ReaperBackendPID = 0;
            /* for safety's sake, launch new logger *first* */
            g_instance.pid_cxt.ReaperBackendPID = initialize_util_thread(REAPER);

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("reaper backend process"), pid, exitstatus);

            continue;
        }

        /* Was it the wlm collector?  If so, try to start a new one */
        if (pid == g_instance.pid_cxt.WLMCollectPID) {
            g_instance.pid_cxt.WLMCollectPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("wlm collector process"), pid, exitstatus);

            continue;
        }

        if (pid == g_instance.pid_cxt.WLMMonitorPID) {
            g_instance.pid_cxt.WLMMonitorPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("wlm monitor process"), pid, exitstatus);

            continue;
        }

        if (pid == g_instance.pid_cxt.WLMArbiterPID) {
            g_instance.pid_cxt.WLMArbiterPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("wlm arbiter process"), pid, exitstatus);

            continue;
        }

        if (pid == g_instance.pid_cxt.CPMonitorPID) {
            g_instance.pid_cxt.CPMonitorPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("compute pool monitor process"), pid, exitstatus);

            continue;
        }

        /* Was it the twophasecleaner? If so, try to handle */
        if (
#ifdef ENABLE_MULTIPLE_NODES
            IS_PGXC_COORDINATOR &&
#endif
            pid == g_instance.pid_cxt.TwoPhaseCleanerPID) {
            g_instance.pid_cxt.TwoPhaseCleanerPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                HandleChildCrash(pid, exitstatus, _("twophase cleaner process"));

            continue;
        }

        /* Was it the csnmin sync?  If so, try to start a new one */
        if (GTM_LITE_CN && pid == g_instance.pid_cxt.CsnminSyncPID) {
            g_instance.pid_cxt.CsnminSyncPID = 0;
            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("csnmin sync process"), pid, exitstatus);
            continue;
        }

#ifndef ENABLE_LITE_MODE
        /* Was it the barrier creator?  If so, try to start a new one */
        if (START_BARRIER_CREATOR && pid == g_instance.pid_cxt.BarrierCreatorPID) {
            g_instance.pid_cxt.BarrierCreatorPID = 0;
            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("barrier creator process"), pid, exitstatus);
            continue;
        }
#endif

        if (pid == g_instance.pid_cxt.FaultMonitorPID) {
            g_instance.pid_cxt.FaultMonitorPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                HandleChildCrash(pid, exitstatus, _("fault monitor process"));

            continue;
        }

        if (pid == g_instance.pid_cxt.CBMWriterPID) {
            g_instance.pid_cxt.CBMWriterPID = 0;

            if (!EXIT_STATUS_0(exitstatus)) {
                LogChildExit(LOG, _("CBM writer process"), pid, exitstatus);
            }
            t_thrd.cbm_cxt.XlogCbmSys->needReset = true;
            continue;
        }

        /*
        * Check if this child was a undo recycle process.
        */
        if (pid == g_instance.pid_cxt.UndoRecyclerPID) {
            g_instance.pid_cxt.UndoRecyclerPID = 0;

            if (!EXIT_STATUS_0(exitstatus)) {
                LogChildExit(LOG, _("Undo recycle process"), pid, exitstatus);
            }
            continue;
        }

        if (get_real_recovery_parallelism() > 1) {
            PageRedoExitStatus pageredoStatus = CheckExitPageWorkers(pid);
            if (pageredoStatus == PAGE_REDO_THREAD_EXIT_NORMAL) {
                continue;
            } else if (pageredoStatus == PAGE_REDO_THREAD_EXIT_ABNORMAL) {
                write_stderr("%s LOG: aborting  due to page redo process failure\n",
                        GetReaperLogPrefix(logBuf, ReaperLogBufSize));
                HandleChildCrash(pid, exitstatus, _("page redo process"));
                continue;
            }
        }

        if (pid == g_instance.pid_cxt.HeartbeatPID) {
            g_instance.pid_cxt.HeartbeatPID = 0;
            if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
                HandleChildCrash(pid, exitstatus, _("Heartbeat process"));

            continue;
        }

        if (pid == g_instance.pid_cxt.CommPoolerCleanPID) {
            g_instance.pid_cxt.CommPoolerCleanPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("pooler cleaner process"), pid, exitstatus);
            continue;
        }

#ifdef ENABLE_MULTIPLE_NODES
        if (pid == g_instance.pid_cxt.BarrierPreParsePID) {
            g_instance.pid_cxt.BarrierPreParsePID = 0;
            write_stderr("%s LOG: barrier pre parse thread exit\n", GetReaperLogPrefix(logBuf, ReaperLogBufSize));
            if (!EXIT_STATUS_0(exitstatus))
                HandleChildCrash(pid, exitstatus, _("barrier pre parse process"));
            continue;
        }
        
        if (pid == g_instance.pid_cxt.TsCompactionPID) {
            g_instance.pid_cxt.TsCompactionPID = 0;
            if (!EXIT_STATUS_0(exitstatus))
                HandleChildCrash(pid, exitstatus, _("timeseries compaction process"));
            continue;
        }

        if (pid == g_instance.pid_cxt.TsCompactionAuxiliaryPID) {
            g_instance.pid_cxt.TsCompactionAuxiliaryPID = 0;
            if (!EXIT_STATUS_0(exitstatus))
                HandleChildCrash(pid, exitstatus, _("timeseries compaction auxiliary process"));
            continue;
        }

        if (streaming_backend_manager(STREAMING_BACKEND_REAP, (void *)&pid)) {
            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("streaming backend process"), pid, exitstatus);
            continue;
        }
#endif   /* ENABLE_MULTIPLE_NODES */

        if (pid == g_instance.pid_cxt.UndoLauncherPID) {
            g_instance.pid_cxt.UndoLauncherPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("undo launcher process"), pid, exitstatus);
            continue;
        }
#if !defined(ENABLE_MULTIPLE_NODES) && !defined(ENABLE_LITE_MODE)
        if (pid == g_instance.pid_cxt.ApplyLauncerPID) {
            g_instance.pid_cxt.ApplyLauncerPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("apply launcher process"), pid, exitstatus);
            continue;
        }
#endif
        if (pid == g_instance.pid_cxt.GlobalStatsPID) {
            g_instance.pid_cxt.GlobalStatsPID = 0;

            if (!EXIT_STATUS_0(exitstatus))
                LogChildExit(LOG, _("global stats process"), pid, exitstatus);
            continue;
        }

        /*
         * Else do standard backend child cleanup.
         */
        CleanupBackend(pid, exitstatus);
    } /* loop over pending child-death reports */

    /*
     * After cleaning out the SIGCHLD queue, see if we have any state changes
     * or actions to make.
     */
    PostmasterStateMachine();

    /* Done with signal handler */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);

    errno = save_errno;
}

/*
 * Get proc name due to thread id.
 */
static const char* GetProcName(ThreadId pid)
{
    if (pid == 0)
        return "invalid process";
    else if (pid == g_instance.pid_cxt.StartupPID)
        return "startup process";
    else if (pid == g_instance.pid_cxt.BgWriterPID)
        return "background writer process";
    else if (pid == g_instance.pid_cxt.CheckpointerPID)
        return "checkpointer process";
    else if (pid == g_instance.pid_cxt.WalWriterPID)
        return "WAL writer process";
    else if (pid == g_instance.pid_cxt.WalWriterAuxiliaryPID)
        return "WAL file creator process";
    else if (pid == g_instance.pid_cxt.WalReceiverPID)
        return "WAL receiver process";
    else if (pid == g_instance.pid_cxt.WalRcvWriterPID)
        return "WAL receive writer process";
    else if (pid == g_instance.pid_cxt.DataReceiverPID)
        return "DATA receiver process";
    else if (pid == g_instance.pid_cxt.DataRcvWriterPID)
        return "DATA receive writer process";
    else if (pid == g_instance.pid_cxt.CatchupPID)
        return "catchup process";
    else if (pid == g_instance.pid_cxt.AutoVacPID)
        return "autovacuum launcher process";
    else if (pid == g_instance.pid_cxt.PgJobSchdPID)
        return "job scheduler process";
    else if (pid == g_instance.pid_cxt.PgArchPID)
        return "archiver process";
    else if (pid == g_instance.pid_cxt.PgStatPID)
        return "statistics collector process";
    else if (g_instance.pid_cxt.TxnSnapCapturerPID == pid)
        return "txnsnapcapturer process";
    else if (g_instance.pid_cxt.RbCleanrPID == pid)
        return "recyclebin cleaner process";
    else if (pid == g_instance.pid_cxt.SnapshotPID)
        return "snapshot collector process";
    else if (pid == g_instance.pid_cxt.AshPID)
        return "active session history collector process";
    else if (pid == g_instance.pid_cxt.StatementPID)
        return "full SQL statement flush process";
    else if (pid == g_instance.pid_cxt.PercentilePID)
        return "percentile collector process";
    else if (pg_auditor_thread(pid)) {
        return "system auditor process";
    }
    else if (pid == g_instance.pid_cxt.SysLoggerPID)
        return "system logger process";
    else if (pid == g_instance.pid_cxt.WLMCollectPID)
        return "wlm collector process";
    else if (pid == g_instance.pid_cxt.WLMMonitorPID)
        return "wlm monitor process";
    else if (pid == g_instance.pid_cxt.WLMArbiterPID)
        return "wlm arbiter process";
    else if (pid == g_instance.pid_cxt.CPMonitorPID)
        return "compute pool monitor process";
    else if (pid == g_instance.pid_cxt.TwoPhaseCleanerPID)
        return "twophase cleaner process";
    else if (pid == g_instance.pid_cxt.FaultMonitorPID)
        return "fault monitor process";
    else if (g_instance.pid_cxt.AlarmCheckerPID == pid)
        return "alarm checker process";
    else if (g_instance.pid_cxt.AioCompleterStarted == pid)
        return "aio completer process";
    else if (pid == g_instance.pid_cxt.CBMWriterPID)
        return "CBM writer process";
    else if (g_instance.pid_cxt.HeartbeatPID == pid)
        return "Heartbeat process";
#ifdef ENABLE_MULTIPLE_NODES
    else if (g_instance.pid_cxt.TsCompactionPID  == pid)
        return "TsCompaction process";
    else if (g_instance.pid_cxt.TsCompactionAuxiliaryPID  == pid)
        return "TsCompaction Auxiliary process";
#endif   /* ENABLE_MULTIPLE_NODES */
    else if (g_instance.pid_cxt.CsnminSyncPID == pid)
        return "csnmin sync process";
    else if (g_instance.pid_cxt.BarrierCreatorPID == pid)
        return "barrier creator process";
    else if (g_instance.pid_cxt.CommSenderFlowPID == pid)
        return "libcomm sender flow process";
    else if (g_instance.pid_cxt.CommReceiverFlowPID == pid)
        return "libcomm receiver flow process";
    else if (g_instance.pid_cxt.CommAuxiliaryPID == pid)
        return "libcomm auxiliary process";
    else if (g_instance.pid_cxt.CommPoolerCleanPID == pid)
        return "pool cleaner process";
    else if (g_instance.pid_cxt.sharedStorageXlogCopyThreadPID == pid)
        return "share xlog copy process";
    else if (g_instance.pid_cxt.BarrierPreParsePID == pid)
        return "barrier preparse process";
    else if (g_instance.pid_cxt.CommReceiverPIDS != NULL) {
        int recv_loop = 0;
        for (recv_loop = 0; recv_loop < g_instance.attr.attr_network.comm_max_receiver; recv_loop++) {
            if (g_instance.pid_cxt.CommReceiverPIDS[recv_loop] == pid) {
                return "libcomm receiver loop process";
            }
        }
        return "server process";
    } else {
        return "server process";
    }
}

/*
 * CleanupBackend -- cleanup after terminated backend.
 *
 * Remove all local state associated with backend.
 */
static void CleanupBackend(ThreadId pid, int exitstatus) /* child's exit status. */
{
    Dlelem* curr = NULL;
    char logBuf[ReaperLogBufSize] = {0};

    /*
     * If a backend dies in an ugly way then we must signal all other backends
     * to quickdie.  If exit status is zero (normal) or one (FATAL exit), we
     * assume everything is all right and proceed to remove the backend from
     * the active backend list.
     */
#ifdef WIN32

    /*
     * On win32, also treat ERROR_WAIT_NO_CHILDREN (128) as nonfatal case,
     * since that sometimes happens under load when the process fails to start
     * properly (long before it starts using shared memory). Microsoft reports
     * it is related to mutex failure:
     * http://archives.postgresql.org/pgsql-hackers/2010-09/msg00790.php
     */
    if (exitstatus == ERROR_WAIT_NO_CHILDREN) {
        LogChildExit(LOG, _("server process"), pid, exitstatus);
        exitstatus = 0;
    }

#endif

    if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus)) {
        HandleChildCrash(pid, exitstatus, _("server process"));
        return;
    }

    /*
     * We always add the backend_list from head. Think about a scenario, an exit
     * thread released its pid to system, which then reused by a new thread.
     * After that, PM received signal to clean up the exit thread, we traversed the
     * list from head and use pid to match thread Backend, we probably matched
     * the new thread rather than the exit thread. To avoid this problem, we can
     * traverse the backend_list from tail.
     */
    bool found = false;
    int cnt = 0;
    knl_thread_role role = (knl_thread_role)0;
    for (curr = DLGetTail(g_instance.backend_list); curr; curr = DLGetPred(curr)) {
        Backend* bp = (Backend*)DLE_VAL(curr);

        if (bp->pid == pid) {
            cnt++;
            role = bp->role;
            if (bp->dead_end) {
                {
                    if (!ReleasePostmasterChildSlot(bp->child_slot)) {
                        /*
                         * Uh-oh, the child failed to clean itself up.	Treat as a
                         * crash after all.
                         */
                        HandleChildCrash(pid, exitstatus, _("server process"));
                        return;
                    }

                    BackendArrayRemove(bp);
                }

                DLRemove(curr);
                found = true;
                break;
            }
        }
    }
    if (!found) {
        write_stderr("%s WARNING: Did not found reaper thread id %lu in backend list, with %d thread still alive,"
                    "last thread role %d, may has leak occurs\n",
                    GetReaperLogPrefix(logBuf, ReaperLogBufSize), pid, cnt, (int)role);
    }
}

/*
 * HandleChildCrash -- cleanup after failed backend, bgwriter, checkpointer,
 * walwriter or autovacuum.
 *
 * The objectives here are to clean up our local state about the child
 * process, and to signal all other remaining children to quickdie.
 */
void HandleChildCrash(ThreadId pid, int exitstatus, const char* procname)
{
    char logBuf[ReaperLogBufSize] = {0};

    /*
     * Make log entry unless there was a previous crash (if so, nonzero exit
     * status is to be expected in SIGQUIT response; don't clutter log)
     */
    if (!g_instance.fatal_error) {
        LogChildExit(LOG, procname, pid, exitstatus);
        write_stderr("%s LOG: terminating any other active server processes\n",
            GetReaperLogPrefix(logBuf, ReaperLogBufSize));
    }

    write_stderr("%s LOG: %s (ThreadId %lu) exited with exit code %d\n",
        GetReaperLogPrefix(logBuf, ReaperLogBufSize), procname, pid, WEXITSTATUS(exitstatus));

    // Threading: do not handle child crash,
    // &g_instance.proc_aux_base or autovacuum elog(FATAL) could reach here,
    // if we handle it, we will send SIGQUIT to backend process,
    // then backend may handle the signal when doing malloc, cause memory exception.
    // So exit directly.
    //

    write_stderr("%s LOG: the server process exits\n", GetReaperLogPrefix(logBuf, ReaperLogBufSize));

    cancelIpcMemoryDetach();

    fflush(stdout);
    fflush(stderr);
    _exit(1);
}

/*
 * Log the death of a child process.
 */
static void LogChildExit(int lev, const char* procname, ThreadId pid, int exitstatus)
{
    /*
     * size of activity_buffer is arbitrary, but set equal to default
     * track_activity_query_size
     */
    char activity_buffer[1024];
    const char* activity = NULL;
    char logBuf[ReaperLogBufSize] = {0};

    if (!EXIT_STATUS_0(exitstatus)) {
        activity = pgstat_get_crashed_backend_activity(pid, activity_buffer, sizeof(activity_buffer));
    }
    if (WIFEXITED(exitstatus)) {
        if (lev == LOG) {
            write_stderr("%s LOG: %s (ThreadId %lu) exited with exit code %d"
                "Failed process was running: %s\n",
                GetReaperLogPrefix(logBuf, ReaperLogBufSize), procname, pid, WEXITSTATUS(exitstatus),
                activity ? activity : 0);
        } else {
            ereport(lev,
                /* ------
                  translator: %s is a noun phrase describing a child process, such as
                  "server process" */
                (errmsg("%s (ThreadId %lu) exited with exit code %d", procname, pid, WEXITSTATUS(exitstatus)),
                    activity ? errdetail("Failed process was running: %s", activity) : 0));
        }
    } else if (WIFSIGNALED(exitstatus)) {
#if defined(WIN32)
        ereport(lev,

            /* ------
              translator: %s is a noun phrase describing a child process, such as
              "server process" */
            (errmsg("%s (ThreadId %lu) was terminated by exception 0x%X", procname, pid, WTERMSIG(exitstatus)),
                errhint("See C include file \"ntstatus.h\" for a description of the hexadecimal value."),
                activity ? errdetail("Failed process was running: %s", activity) : 0));

#elif defined(HAVE_DECL_SYS_SIGLIST) && HAVE_DECL_SYS_SIGLIST
        ereport(lev,

            /* ------
              translator: %s is a noun phrase describing a child process, such as
              "server process" */
            (errmsg("%s (ThreadId %lu) was terminated by signal %d: %s",
                 procname,
                 pid,
                 WTERMSIG(exitstatus),
                 WTERMSIG(exitstatus) < NSIG ? sys_siglist[WTERMSIG(exitstatus)] : "(unknown)"),
                activity ? errdetail("Failed process was running: %s", activity) : 0));
#else
        if (lev == LOG) {
            write_stderr("%s LOG: %s (ThreadId %lu) exited with exit code %d"
                "Failed process was running: %s\n",
                GetReaperLogPrefix(logBuf, ReaperLogBufSize), procname, pid, WEXITSTATUS(exitstatus),
                activity ? activity : 0);
        } else {
            ereport(lev,
                /* ------
                  translator: %s is a noun phrase describing a child process, such as
                  "server process" */
                (errmsg("%s (ThreadId %lu) exited with exit code %d", procname, pid, WEXITSTATUS(exitstatus)),
                    activity ? errdetail("Failed process was running: %s", activity) : 0));
        }
#endif
    } else {
        if (lev == LOG) {
            write_stderr("%s LOG: %s (ThreadId %lu) exited with unrecognized status %d"
                "Failed process was running: %s\n",
                GetReaperLogPrefix(logBuf, ReaperLogBufSize), procname, pid, WEXITSTATUS(exitstatus),
                activity ? activity : 0);
        } else {
            ereport(lev,
                /* ------
                  translator: %s is a noun phrase describing a child process, such as
                  "server process" */
                (errmsg("%s (ThreadId %lu) exited with unrecognized status %d", procname, pid, exitstatus),
                    activity ? errdetail("Failed process was running: %s", activity) : 0));
        }
    }
}

static bool ckpt_all_flush_buffer_thread_exit()
{
    if (g_instance.pid_cxt.PageWriterPID[0] == 0){
        return true;
    }else {
        return false;
    }
}

static void PostmasterStateMachineReadOnly(void)
{
    if (pmState == PM_WAIT_READONLY) {
        /*
         * PM_WAIT_READONLY state ends when we have no regular backends that
         * have been started during recovery.  We kill the startup and
         * walreceiver processes and transition to PM_WAIT_BACKENDS.  Ideally,
         * we might like to kill these processes first and then wait for
         * backends to die off, but that doesn't work at present because
         * killing the startup process doesn't release its locks.
         */
        if (CountChildren(BACKEND_TYPE_NORMAL) == 0) {
            if (g_instance.pid_cxt.StartupPID != 0)
                signal_child(g_instance.pid_cxt.StartupPID, SIGTERM);

            if (g_instance.pid_cxt.PageRepairPID != 0) {
                signal_child(g_instance.pid_cxt.PageRepairPID, SIGTERM);
            }

            if (g_instance.pid_cxt.WalReceiverPID != 0)
                signal_child(g_instance.pid_cxt.WalReceiverPID, SIGTERM);

            if (g_instance.pid_cxt.WalRcvWriterPID != 0)
                signal_child(g_instance.pid_cxt.WalRcvWriterPID, SIGTERM);

            if (g_instance.pid_cxt.DataReceiverPID != 0)
                signal_child(g_instance.pid_cxt.DataReceiverPID, SIGTERM);

            if (g_instance.pid_cxt.DataRcvWriterPID != 0)
                signal_child(g_instance.pid_cxt.DataRcvWriterPID, SIGTERM);

            if (g_instance.pid_cxt.WLMCollectPID != 0) {
                WLMProcessThreadShutDown();
                signal_child(g_instance.pid_cxt.WLMCollectPID, SIGTERM);
            }

            if (g_instance.pid_cxt.WLMMonitorPID != 0)
                signal_child(g_instance.pid_cxt.WLMMonitorPID, SIGTERM);

            if (g_instance.pid_cxt.WLMArbiterPID != 0)
                signal_child(g_instance.pid_cxt.WLMArbiterPID, SIGTERM);

            if (g_instance.pid_cxt.CPMonitorPID != 0)
                signal_child(g_instance.pid_cxt.CPMonitorPID, SIGTERM);

            if (g_instance.pid_cxt.HeartbeatPID != 0)
                signal_child(g_instance.pid_cxt.HeartbeatPID, SIGTERM);

#ifndef ENABLE_LITE_MODE
            if (g_instance.pid_cxt.BarrierCreatorPID != 0) {
                barrier_creator_thread_shutdown();
                signal_child(g_instance.pid_cxt.BarrierCreatorPID, SIGTERM);
            }
#endif
#ifdef ENABLE_MULTIPLE_NODES
            if (g_instance.pid_cxt.CsnminSyncPID != 0) {
                csnminsync_thread_shutdown();
                signal_child(g_instance.pid_cxt.CsnminSyncPID, SIGTERM);
            }

            if (g_instance.pid_cxt.BarrierPreParsePID != 0)
                signal_child(g_instance.pid_cxt.BarrierPreParsePID, SIGTERM);
#endif   /* ENABLE_MULTIPLE_NODES */

            if (g_instance.pid_cxt.UndoLauncherPID != 0)
                signal_child(g_instance.pid_cxt.UndoLauncherPID, SIGTERM);
#if !defined(ENABLE_MULTIPLE_NODES) && !defined(ENABLE_LITE_MODE)
            if (g_instance.pid_cxt.ApplyLauncerPID != 0)
                signal_child(g_instance.pid_cxt.ApplyLauncerPID, SIGTERM);
#endif
            if (g_instance.pid_cxt.UndoRecyclerPID != 0)
                signal_child(g_instance.pid_cxt.UndoRecyclerPID, SIGTERM);

            if (g_instance.pid_cxt.GlobalStatsPID != 0)
                signal_child(g_instance.pid_cxt.GlobalStatsPID, SIGTERM);

            pmState = PM_WAIT_BACKENDS;
        }
    }
}

static bool ObsArchAllShutDown()
{
    if (g_instance.archive_thread_info.obsArchPID != NULL) {
        for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
            if (g_instance.archive_thread_info.obsArchPID[i] != 0) {
                return false;
            }
        }
    }
    if (g_instance.archive_thread_info.obsBarrierArchPID != NULL) {
        for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
            if (g_instance.archive_thread_info.obsBarrierArchPID[i] != 0) {
                return false;
            }
        }
    }
    ereport(LOG, (errmsg("All obsArch and obsBarrier exit.")));
    return true;
}

static bool AuditAllShutDown()
{
    if (g_instance.pid_cxt.PgAuditPID != NULL) {
        for (int i = 0; i < g_instance.audit_cxt.thread_num; i++) {
            if (g_instance.pid_cxt.PgAuditPID[i] != 0) {
                return false;
            }
        }
    }
    ereport(LOG, (errmsg("All Audit threads exit.")));
    return true;
}

static void AsssertAllChildThreadExit()
{
    /* These other guys should be dead already */
    Assert(g_instance.pid_cxt.TwoPhaseCleanerPID == 0);
    Assert(g_instance.pid_cxt.FaultMonitorPID == 0);
    Assert(g_instance.pid_cxt.StartupPID == 0);
    Assert(g_instance.pid_cxt.PageRepairPID == 0);
    Assert(g_instance.pid_cxt.WalReceiverPID == 0);
    Assert(g_instance.pid_cxt.WalRcvWriterPID == 0);
    Assert(g_instance.pid_cxt.DataReceiverPID == 0);
    Assert(g_instance.pid_cxt.DataRcvWriterPID == 0);
    Assert(g_instance.pid_cxt.BgWriterPID == 0);
    Assert(g_instance.pid_cxt.CheckpointerPID == 0);
    Assert(g_instance.pid_cxt.WalWriterPID == 0);
    Assert(g_instance.pid_cxt.WalWriterAuxiliaryPID == 0);
    Assert(g_instance.pid_cxt.AutoVacPID == 0);
    Assert(g_instance.pid_cxt.PgJobSchdPID == 0);
    Assert(g_instance.pid_cxt.CBMWriterPID == 0);
    Assert(g_instance.pid_cxt.TxnSnapCapturerPID == 0);
    Assert(g_instance.pid_cxt.RbCleanrPID == 0);
    Assert(g_instance.pid_cxt.SnapshotPID == 0);
    Assert(g_instance.pid_cxt.AshPID == 0);
    Assert(g_instance.pid_cxt.StatementPID == 0);
    Assert(g_instance.pid_cxt.PercentilePID == 0);
    Assert(g_instance.pid_cxt.HeartbeatPID == 0);
    Assert(g_instance.pid_cxt.CsnminSyncPID == 0);
    Assert(g_instance.pid_cxt.BarrierCreatorPID == 0);
    Assert(g_instance.pid_cxt.BarrierPreParsePID == 0);
#ifdef ENABLE_MULTIPLE_NODES
    Assert(g_instance.pid_cxt.TsCompactionPID == 0);
#endif   /* ENABLE_MULTIPLE_NODES */
    Assert(g_instance.pid_cxt.CommPoolerCleanPID == 0);
    Assert(g_instance.pid_cxt.UndoLauncherPID == 0);
    Assert(g_instance.pid_cxt.UndoRecyclerPID == 0);
#if !defined(ENABLE_MULTIPLE_NODES) && !defined(ENABLE_LITE_MODE)
    Assert(g_instance.pid_cxt.ApplyLauncerPID == 0);
#endif
    Assert(g_instance.pid_cxt.GlobalStatsPID == 0);
    Assert(IsAllPageWorkerExit() == true);
    Assert(IsAllBuildSenderExit() == true);
    Assert(g_instance.pid_cxt.sharedStorageXlogCopyThreadPID == 0);
}

/*
 * Advance the postmaster's state machine and take actions as appropriate
 *
 * This is common code for pmdie(), reaper() and sigusr1_handler(), which
 * receive the signals that might mean we need to change state.
 */
static void PostmasterStateMachine(void)
{
    if (pmState == PM_WAIT_BACKUP) {
        /*
         * PM_WAIT_BACKUP state ends when online backup mode is not active.
         */
        if (!BackupInProgress())
            pmState = PM_WAIT_BACKENDS;
    }

    PostmasterStateMachineReadOnly();
    /*
     * If we are in a state-machine state that implies waiting for backends to
     * exit, see if they're all gone, and change state if so.
     */
    if (pmState == PM_WAIT_BACKENDS) {
        /*
         * PM_WAIT_BACKENDS state ends when we have no regular backends
         * (including autovac workers) and no walwriter, autovac launcher or
         * bgwriter.  If we are doing crash recovery then we expect the
         * checkpointer to exit as well, otherwise not. The archiver, stats,
         * and syslogger processes are disregarded since they are not
         * connected to shared memory; we also disregard dead_end children
         * here. Walsenders are also disregarded, they will be terminated
         * later after writing the checkpoint record, like the archiver
         * process.
         */
        if (CountChildren(BACKEND_TYPE_NORMAL | BACKEND_TYPE_AUTOVAC) == 0 && g_instance.pid_cxt.StartupPID == 0 &&
            g_instance.pid_cxt.TwoPhaseCleanerPID == 0 && g_instance.pid_cxt.FaultMonitorPID == 0 &&
            g_instance.pid_cxt.WalReceiverPID == 0 && g_instance.pid_cxt.WalRcvWriterPID == 0 &&
            g_instance.pid_cxt.DataReceiverPID == 0 && g_instance.pid_cxt.DataRcvWriterPID == 0 &&
            g_instance.pid_cxt.BgWriterPID == 0 && g_instance.pid_cxt.StatementPID == 0 &&
            (g_instance.pid_cxt.CheckpointerPID == 0 || !g_instance.fatal_error) &&
            g_instance.pid_cxt.WalWriterPID == 0 && g_instance.pid_cxt.WalWriterAuxiliaryPID == 0 &&
            g_instance.pid_cxt.AutoVacPID == 0 && g_instance.pid_cxt.SpBgWriterPID == 0 &&
            g_instance.pid_cxt.WLMCollectPID == 0 && g_instance.pid_cxt.WLMMonitorPID == 0 &&
            g_instance.pid_cxt.WLMArbiterPID == 0 && g_instance.pid_cxt.CPMonitorPID == 0 &&
            g_instance.pid_cxt.PgJobSchdPID == 0 && g_instance.pid_cxt.CBMWriterPID == 0 &&
            g_instance.pid_cxt.TxnSnapCapturerPID == 0 && 
            g_instance.pid_cxt.RbCleanrPID == 0 && 
            g_instance.pid_cxt.SnapshotPID == 0 && g_instance.pid_cxt.PercentilePID == 0 &&
            g_instance.pid_cxt.AshPID == 0 && g_instance.pid_cxt.CsnminSyncPID == 0 &&
            g_instance.pid_cxt.BarrierCreatorPID == 0 &&  g_instance.pid_cxt.PageRepairPID == 0 &&
#ifdef ENABLE_MULTIPLE_NODES
            g_instance.pid_cxt.BarrierPreParsePID == 0 &&
            g_instance.pid_cxt.CommPoolerCleanPID == 0 && streaming_backend_manager(STREAMING_BACKEND_SHUTDOWN) &&
            g_instance.pid_cxt.TsCompactionPID == 0 && g_instance.pid_cxt.TsCompactionAuxiliaryPID == 0
            && g_instance.pid_cxt.CommPoolerCleanPID == 0 &&
#endif   /* ENABLE_MULTIPLE_NODES */

            g_instance.pid_cxt.UndoLauncherPID == 0 && g_instance.pid_cxt.UndoRecyclerPID == 0 &&
            g_instance.pid_cxt.GlobalStatsPID == 0 &&
#if !defined(ENABLE_MULTIPLE_NODES) && !defined(ENABLE_LITE_MODE)
            g_instance.pid_cxt.ApplyLauncerPID == 0 &&
#endif
            IsAllPageWorkerExit() && IsAllBuildSenderExit()) {
            if (g_instance.fatal_error) {
                /*
                 * Start waiting for dead_end children to die.	This state
                 * change causes ServerLoop to stop creating new ones.
                 */
                pmState = PM_WAIT_DEAD_END;

                /*
                 * We already SIGQUIT'd the archiver and stats processes, if
                 * any, when we entered g_instance.fatal_error state.
                 */
            } else {
                /*
                 * If we get here, we are proceeding with normal shutdown. All
                 * the regular children are gone, and it's time to tell the
                 * checkpointer to do a shutdown checkpoint.
                 */
                Assert(g_instance.status > NoShutdown || g_instance.demotion > NoDemote);

                /* Start the checkpointer if not running */
                if (g_instance.pid_cxt.CheckpointerPID == 0 && !dummyStandbyMode)
                    g_instance.pid_cxt.CheckpointerPID = initialize_util_thread(CHECKPOINT_THREAD);

                if (g_instance.pid_cxt.PageWriterPID != NULL) {
                    g_instance.ckpt_cxt_ctl->page_writer_can_exit = false;

                    for (int i = 0; i < g_instance.ckpt_cxt_ctl->pgwr_procs.num; i++) {
                        if (g_instance.pid_cxt.PageWriterPID[i] != 0) {
                            signal_child(g_instance.pid_cxt.PageWriterPID[i], SIGTERM);
                        }
                    }
                }

                /* And tell it to shut down */
                if (g_instance.pid_cxt.CheckpointerPID != 0) {
                    Assert(!dummyStandbyMode);
                    signal_child(g_instance.pid_cxt.CheckpointerPID, SIGUSR2);
                    pmState = PM_SHUTDOWN;
                } else {
                    /*
                     * If we failed to fork a checkpointer, just shut down.
                     * Any required cleanup will happen at next restart. We
                     * set g_instance.fatal_error so that an "abnormal shutdown" message
                     * gets logged when we exit.
                     */
                    g_instance.ckpt_cxt_ctl->page_writer_can_exit = true;
                    g_instance.fatal_error = true;
                    pmState = PM_WAIT_DEAD_END;

                    /* Kill the senders, archiver and stats collector too */
                    SignalChildren(SIGQUIT);

                    if (g_instance.pid_cxt.PgArchPID != 0)
                        signal_child(g_instance.pid_cxt.PgArchPID, SIGQUIT);
                    
                    if (g_instance.archive_thread_info.obsArchPID != NULL) {
                        for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
                            if (g_instance.archive_thread_info.obsArchPID[i] != 0) {
                                signal_child(g_instance.archive_thread_info.obsArchPID[i], SIGQUIT);
                            }
                        }
                    }

                    if (g_instance.archive_thread_info.obsBarrierArchPID != NULL) {
                        for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
                            if (g_instance.archive_thread_info.obsBarrierArchPID[i] != 0) {
                                signal_child(g_instance.archive_thread_info.obsBarrierArchPID[i], SIGQUIT);
                            }
                        }
                    }

                    if (g_instance.pid_cxt.PgStatPID != 0)
                        signal_child(g_instance.pid_cxt.PgStatPID, SIGQUIT);

                    /*  signal the auditor process */
                    if (g_instance.pid_cxt.PgAuditPID != NULL) {
                        pgaudit_stop_all();
                    }

                    if (g_instance.pid_cxt.sharedStorageXlogCopyThreadPID != 0) {
                        Assert(!dummyStandbyMode);
                        signal_child(g_instance.pid_cxt.sharedStorageXlogCopyThreadPID, SIGTERM);
                        ereport(LOG, (errmsg("pmstat send exit to sharestorage thread")));
                    }
                }
            }
        }
    }

    if (pmState == PM_SHUTDOWN_2) {
        /*
         * PM_SHUTDOWN_2 state ends when there's no other children than
         * dead_end children left. There shouldn't be any regular backends
         * left by now anyway; what we're really waiting for is walsenders and
         * archiver.
         *
         * Walreceiver should normally be dead by now, but not when a fast
         * shutdown is performed during recovery.
         */
        if (g_instance.pid_cxt.PgArchPID == 0 && CountChildren(BACKEND_TYPE_ALL) == 0 &&
            g_instance.pid_cxt.WalReceiverPID == 0 && g_instance.pid_cxt.WalRcvWriterPID == 0 &&
            g_instance.pid_cxt.DataReceiverPID == 0 && g_instance.pid_cxt.DataRcvWriterPID == 0 && 
            ObsArchAllShutDown() && g_instance.pid_cxt.HeartbeatPID == 0 &&
            g_instance.pid_cxt.sharedStorageXlogCopyThreadPID == 0) {
            pmState = PM_WAIT_DEAD_END;
        }
    }

    if (pmState == PM_WAIT_DEAD_END) {
        /*
         * PM_WAIT_DEAD_END state ends when the g_instance.backend_list is entirely empty
         * (ie, no dead_end children remain), and the archiver and stats
         * collector are gone too.
         *
         * The reason we wait for those two is to protect them against a new
         * postmaster starting conflicting subprocesses; this isn't an
         * ironclad protection, but it at least helps in the
         * shutdown-and-immediately-restart scenario.  Note that they have
         * already been sent appropriate shutdown signals, either during a
         * normal state transition leading up to PM_WAIT_DEAD_END, or during
         * g_instance.fatal_error processing.
         */
        if (DLGetHead(g_instance.backend_list) == NULL && g_instance.pid_cxt.PgArchPID == 0 &&
            g_instance.pid_cxt.PgStatPID == 0 && AuditAllShutDown() &&
            ckpt_all_flush_buffer_thread_exit() && ObsArchAllShutDown()) {

            AsssertAllChildThreadExit();
            /* syslogger is not considered here */
            pmState = PM_NO_CHILDREN;
        }
    }

    /*
     * If we've been told to shut down, we exit as soon as there are no
     * remaining children.	If there was a crash, cleanup will occur at the
     * next startup.  (Before PostgreSQL 8.3, we tried to recover from the
     * crash before exiting, but that seems unwise if we are quitting because
     * we got SIGTERM from init --- there may well not be time for recovery
     * before init decides to SIGKILL us.)
     *
     * Note that the syslogger continues to run.  It will exit when it sees
     * EOF on its input pipe, which happens when there are no more upstream
     * processes.
     */
    if (g_instance.status > NoShutdown && pmState == PM_NO_CHILDREN) {
        if (g_instance.fatal_error) {
            ereport(LOG, (errmsg("abnormal database system shutdown")));
            ExitPostmaster(1);
        } else {
            /*
             * Terminate exclusive backup mode to avoid recovery after a clean
             * fast shutdown.  Since an exclusive backup can only be taken
             * during normal running (and not, for example, while running
             * under Hot Standby) it only makes sense to do this if we reached
             * normal running. If we're still in recovery, the backup file is
             * one we're recovering *from*, and we must keep it around so that
             * recovery restarts from the right place.
             */
            if (t_thrd.postmaster_cxt.ReachedNormalRunning)
                CancelBackup();

            /* Normal exit from the postmaster is here */
            ExitPostmaster(0);
        }
    }

    /*
     * If recovery failed, or the user does not want an automatic restart
     * after backend crashes, wait for all non-syslogger children to exit, and
     * then exit postmaster. We don't try to reinitialize when recovery fails,
     * because more than likely it will just fail again and we will keep
     * trying forever.
     */
    if (pmState == PM_NO_CHILDREN && (g_instance.recover_error || !u_sess->attr.attr_sql.restart_after_crash))
        ExitPostmaster(1);

    /*
     * If we need to recover from a crash, wait for all non-syslogger children
     * to exit, then reset shmem and StartupDataBase.
     */
    if (g_instance.fatal_error && pmState == PM_NO_CHILDREN) {
        volatile HaShmemData* hashmdata = NULL;
        ServerMode cur_mode = NORMAL_MODE;
        ereport(LOG, (errmsg("all server processes terminated; reinitializing")));
        hashmdata = t_thrd.postmaster_cxt.HaShmData;
        cur_mode = hashmdata->current_mode;
        /* cause gpc scheduler use lwlock, so before reset shared memory(still has lwlock), 
          get gpc_reset_lock and reset gpc */
        if (ENABLE_GPC) {
            GPCResetAll();
            if (g_threadPoolControler && g_threadPoolControler->GetScheduler()->HasShutDown() == false)
                g_threadPoolControler->ShutDownScheduler(true, false);
        }
        shmem_exit(1);
        reset_shared(g_instance.attr.attr_network.PostPortNumber);

        /* after reseting shared memory, we shall reset col-space cache.
         * all the data of this cache will be out of date after switchover.
         * clean up in order to corrupted data writing.
         */
        CStoreAllocator::ResetColSpaceCache();
        DfsInsert::ResetDfsSpaceCache();

        hashmdata = t_thrd.postmaster_cxt.HaShmData;
        hashmdata->current_mode = cur_mode;
        g_instance.global_sysdbcache.RefreshHotStandby();
        g_instance.pid_cxt.StartupPID = initialize_util_thread(STARTUP);
        Assert(g_instance.pid_cxt.StartupPID != 0);
        pmState = PM_STARTUP;
    }

    /*
     * If we need to recover from primary demote, wait for all non-syslogger children
     * to exit, then reset shmem and StartupDataBase.
     */
    if (g_instance.demotion > NoDemote && pmState == PM_NO_CHILDREN) {
        ereport(LOG, (errmsg("all server processes terminated; reinitializing")));
        /* cause gpc scheduler use lwlock, so before reset shared memory(still has lwlock), 
          get gpc_reset_lock and reset gpc */
        if (ENABLE_GPC) {
            GPCResetAll();
            if (g_threadPoolControler && g_threadPoolControler->GetScheduler()->HasShutDown() == false)
                g_threadPoolControler->ShutDownScheduler(true, false);
        }
        shmem_exit(1);
        reset_shared(g_instance.attr.attr_network.PostPortNumber);

        /* after reseting shared memory, we shall reset col-space cache.
         * all the data of this cache will be out of date after switchover.
         * clean up in order to corrupted data writing.
         */
        CStoreAllocator::ResetColSpaceCache();
        DfsInsert::ResetDfsSpaceCache();

        /* if failed to enter archive-recovery state, then reboot as primary. */
        {
            volatile HaShmemData* hashmdata = t_thrd.postmaster_cxt.HaShmData;
            hashmdata->current_mode = STANDBY_MODE;
            g_instance.global_sysdbcache.RefreshHotStandby();
            UpdateOptsFile();
            ereport(LOG, (errmsg("archive recovery started")));
        }

        g_instance.pid_cxt.StartupPID = initialize_util_thread(STARTUP);
        Assert(g_instance.pid_cxt.StartupPID != 0);
        pmState = PM_STARTUP;
    }

    if (pmState == PM_STARTUP &&
        t_thrd.xlog_cxt.server_mode == STANDBY_MODE &&
        t_thrd.xlog_cxt.is_cascade_standby) {
        SetHaShmemData();
    }
}

void signalBackend(Backend* bn, int signal, int be_mode)
{
    if ((uint32)bn->flag & THRD_EXIT)
        ereport(LOG, (errmsg("Thread pid(%lu), flag(%d) may be exited repeatedly", bn->pid, bn->flag)));

    int nowait = 0;
    if (signal == SIGTERM) {
        nowait = 1;
        bn->flag = ((uint32)(bn->flag)) | THRD_SIGTERM;
    }

    if (gs_signal_send(bn->pid, signal, nowait) != 0) {
        ereport(WARNING,
            (errmsg("kill(pid %ld, signal %d) failed,"
                    " thread name \"%s\", pmState %d, Demotion %d, Shutdown %d, backend type %d",
                (long)bn->pid,
                signal,
                GetProcName(bn->pid),
                pmState,
                g_instance.demotion,
                Shutdown,
                be_mode)));
    }
}

/*
 * Send a signal to a postmaster child process
 *
 * On systems that have setsid(), each child process sets itself up as a
 * process group leader.  For signals that are generally interpreted in the
 * appropriate fashion, we signal the entire process group not just the
 * direct child process.  This allows us to, for example, SIGQUIT a blocked
 * archive_recovery script, or SIGINT a script being run by a backend via
 * system().
 *
 * There is a race condition for recently-forked children: they might not
 * have executed setsid() yet.	So we signal the child directly as well as
 * the group.  We assume such a child will handle the signal before trying
 * to spawn any grandchild processes.  We also assume that signaling the
 * child twice will not cause any problems.
 *
 * *be_mode* is meaning fully when this is a backend pid.
 */
void signal_child(ThreadId pid, int signal, int be_mode)
{
    int err = 0;
    if (0 != (err = gs_signal_send(pid, signal))) {
        ereport(WARNING,
            (errmsg("kill(pid %ld, signal %d) failed: \"%s\","
                    " thread name \"%s\", pmState %d, Demotion %d, Shutdown %d, backend type %d",
                (long)pid,
                signal,
                gs_strerror(err),
                GetProcName(pid),
                pmState,
                g_instance.demotion,
                Shutdown,
                be_mode)));
    }
}

/*
 * Send a signal to the targeted children (but NOT special children;
 * dead_end children are never signaled, either).
 */
static bool SignalSomeChildren(int signal, int target)
{
    Dlelem* curr = NULL;
    bool signaled = false;

    for (curr = DLGetHead(g_instance.backend_list); curr; curr = DLGetSucc(curr)) {
        Backend* bp = (Backend*)DLE_VAL(curr);
        int child = BACKEND_TYPE_ALL;

        /*
         * we want to know the type of this backend thread, so
         * in debug mode IF judgement is skipped. but in release
         * mode, turn it on.
         */
#ifndef USE_ASSERT_CHECKING
        /*
         * Since target == BACKEND_TYPE_ALL is the most common case, we test
         * it first and avoid touching shared memory for every child.
         */
        if (target != BACKEND_TYPE_ALL)
#endif /* USE_ASSERT_CHECKING */
        {
            if (bp->is_autovacuum)
                child = BACKEND_TYPE_AUTOVAC;
            else if (IsPostmasterChildWalSender(bp->child_slot))
                child = BACKEND_TYPE_WALSND;
            else if (IsPostmasterChildDataSender(bp->child_slot))
                child = BACKEND_TYPE_DATASND;
            else if (IsPostmasterChildTempBackend(bp->child_slot))
                child = BACKEND_TYPE_TEMPBACKEND;
            else
                child = BACKEND_TYPE_NORMAL;

            if (!(target & child))
                continue;
        }

        ereport(DEBUG4, (errmsg_internal("sending signal %d to process %lu", signal, bp->pid)));

        signalBackend(bp, signal, child);
        signaled = true;
    }

    return signaled;
}

bool SignalCancelAllBackEnd()
{
    return SignalSomeChildren(SIGINT, BACKEND_TYPE_NORMAL);
}

static int IsHaWhiteListIp(const Port* port)
{
    const int maxIpAddressLen = 64;
    char ipstr[maxIpAddressLen] = {'\0'};
    sockaddr_in* pSin = (sockaddr_in*)&port->raddr.addr;

    inet_ntop(AF_INET, &pSin->sin_addr, ipstr, maxIpAddressLen - 1);
    if (IS_PGXC_COORDINATOR &&
        !is_cluster_internal_IP(*(struct sockaddr*)&port->raddr.addr) &&
        !(strcmp(ipstr, "0.0.0.0") == 0)) {
        ereport(WARNING, (errmsg("the ha listen ip and port is not for remote client from [%s].", ipstr)));
        return STATUS_ERROR;
    }

    return STATUS_OK;
}

/*
 * BackendStartup -- start backend process
 *
 * returns: STATUS_ERROR if the fork failed, STATUS_OK otherwise.
 *
 * Note: if you change this code, also consider StartAutovacuumWorker.
 */
static int BackendStartup(Port* port, bool isConnectHaPort)
{
    Backend* bn = NULL; /* for backend cleanup */
    ThreadId pid;

    if (isConnectHaPort && IsHaWhiteListIp(port) == STATUS_ERROR) {
        return STATUS_ERROR;
    }

    int childSlot = AssignPostmasterChildSlot();

    if (childSlot == -1)
        return STATUS_ERROR;

    bn = AssignFreeBackEnd(childSlot);

    GenerateCancelKey(false);
    /* Pass down canAcceptConnections state */
    port->canAcceptConnections = canAcceptConnections(false);
    if (port->canAcceptConnections != CAC_OK && port->canAcceptConnections != CAC_WAITBACKUP) {
        (void)ReleasePostmasterChildSlot(childSlot);
        if (port->canAcceptConnections == CAC_TOOMANY)
            ereport(WARNING, (errmsg("could not fork new process for connection due to too many connections")));
        else
            ereport(
                WARNING, (errmsg("could not fork new process for connection due to PMstate %s", GetPMState(pmState))));

        return STATUS_ERROR;
    }

    // We can't fork thread when PM is shutting down to avoid PM deal with SIGCHLD in busy
    // One scenario is switchover DN and many client connect to server
    //
    if (CAC_SHUTDOWN == port->canAcceptConnections) {
        (void)ReleasePostmasterChildSlot(childSlot);
        ereport(WARNING, (errmsg("could not fork new process for connection:postmaster is shutting down")));
        report_fork_failure_to_client(port, 0, "could not fork new process for connection:postmaster is shutting down");
        return STATUS_ERROR;
    }

    /*
     * Unless it's a dead_end child, assign it a child slot number
     */
    bn->child_slot = t_thrd.proc_cxt.MyPMChildSlot = childSlot;

    pid = initialize_worker_thread(WORKER, port);
    t_thrd.proc_cxt.MyPMChildSlot = 0;
    if (pid == (ThreadId)-1) {
        /* in parent, fork failed */
        int save_errno = errno;
        (void)ReleasePostmasterChildSlot(childSlot);

        errno = save_errno;
        ereport(WARNING, (errmsg("could not fork new process for connection socket %d : %m", (int)port->sock)));
        report_fork_failure_to_client(port, save_errno);
        return STATUS_ERROR;
    }

    /* in parent, successful fork */
    ereport(DEBUG2, (errmsg_internal("forked new backend, pid=%lu socket=%d", pid, (int)port->sock)));
    ereport(DEBUG2, (errmsg("forked new backend, pid=%lu socket=%d", pid, (int)port->sock)));

    /*
     * Everything's been successful, it's safe to add this backend to our list
     * of backends.
     */
    bn->pid = pid;
    bn->role = WORKER;
    bn->is_autovacuum = false;
    bn->cancel_key = t_thrd.proc_cxt.MyCancelKey;
    DLInitElem(&bn->elem, bn);
    DLAddHead(g_instance.backend_list, &bn->elem);

    return STATUS_OK;
}

/*
 * Try to report backend fork() failure to client before we close the
 * connection.	Since we do not care to risk blocking the postmaster on
 * this connection, we set the connection to non-blocking and try only once.
 *
 * This is grungy special-purpose code; we cannot use backend libpq since
 * it's not up and running.
 */
static void report_fork_failure_to_client(Port* port, int errnum, const char* specialErrorInfo)
{
    char buffer[1000] = {0};
    int rc;
    errno_t ret;

    if (specialErrorInfo == NULL) {
        /* Format the error message packet (always V2 protocol) */
        ret = sprintf_s(
            buffer, sizeof(buffer), "E%s%s\n", _("could not fork new process for connection: "), gs_strerror(errnum));
        securec_check_ss(ret, "\0", "\0");
    } else {
        size_t len = strlen(specialErrorInfo);
        if (len >= sizeof(buffer)) {
            rc = snprintf_truncated_s(buffer, sizeof(buffer), "%s", specialErrorInfo);
            securec_check_ss(rc, "\0", "\0");
        } else {
            rc = snprintf_truncated_s(buffer, sizeof(buffer), "%s", specialErrorInfo);
            securec_check_ss(rc, "\0", "\0");
            buffer[len] = 0;
        }
    }
    if (port->is_logic_conn) {
        rc = gs_send(&port->gs_sock, buffer, strlen(buffer) + 1, -1, TRUE);
    } else {
        /* Set port to non-blocking.  Don't do send() if this fails */
        if (!pg_set_noblock(port->sock))
            return;

        /* We'll retry after EINTR, but ignore all other failures */
        do {
            /* CommProxy Support */
            rc = comm_send(port->sock, buffer, strlen(buffer) + 1, 0);
        } while (rc < 0 && errno == EINTR);
    }
}

/*
 * BackendInitialize -- initialize an interactive (postmaster-child)
 *				backend process, and collect the client's startup packet.
 *
 * returns: nothing.  Will not return at all if there's any failure.
 *
 * Note: this code does not depend on having any access to shared memory.
 * In the EXEC_BACKEND case, we are physically attached to shared memory
 * but have not yet set up most of our local pointers to shmem structures.
 */
static void BackendInitialize(Port* port)
{
    PreClientAuthorize();

    /* save thread start time */
    t_thrd.proc_cxt.MyStartTime = timestamptz_to_time_t(GetCurrentTimestamp());

    /*
     * Initialize libpq to talk to client and enable reporting of ereport errors
     * to the client. Must do this now because authentication uses libpq to
     * send messages.
     */
    pq_init();

    /* now safe to ereport to client */
    t_thrd.postgres_cxt.whereToSendOutput = DestRemote;

    /*
     * We arrange for a simple exit(1) if we receive SIGTERM or SIGQUIT or
     * timeout while trying to collect the startup packet.	Otherwise the
     * postmaster cannot shutdown the database FAST or IMMED cleanly if a
     * buggy client fails to send the packet promptly.
     */
    (void)gspqsignal(SIGTERM, startup_die);
    (void)gspqsignal(SIGQUIT, startup_die);
    (void)gspqsignal(SIGALRM, startup_alarm);

    /* Do the next initialization when we get a real connetion. */
    if (IS_THREAD_POOL_WORKER)
        return;

    int status = ClientConnInitilize(port);

    if (status == STATUS_EOF)
        return;
    else if (status == STATUS_ERROR)
        proc_exit(0);
}

void PreClientAuthorize()
{
    /*
     * PreAuthDelay is a debugging aid for investigating problems in the
     * authentication cycle: it can be set in postgresql.conf to allow time to
     * attach to the newly-forked backend with a debugger.	(See also
     * PostAuthDelay, which we allow clients to pass through PGOPTIONS, but it
     * is not honored until after authentication.)
     */
    if (u_sess->attr.attr_security.PreAuthDelay > 0)
        pg_usleep(u_sess->attr.attr_security.PreAuthDelay * 1000000L);

    /* This flag will remain set until InitPostgres finishes authentication */
    u_sess->ClientAuthInProgress = true; /* limit visibility of log messages */
}

int ClientConnInitilize(Port* port)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.StartupBlockSig, NULL);

    /* Save session start time. */
    port->SessionStartTime = GetCurrentTimestamp();

    RemoteHostInitilize(port);

    /*
     * Ready to begin client interaction.  We will give up and exit(1) after a
     * time delay, so that a broken client can't hog a connection
     * indefinitely.  PreAuthDelay and any DNS interactions above don't count
     * against the time limit.
     */
#ifdef ENABLE_MULTIPLE_NODES
    if (!enable_sig_alarm((u_sess->attr.attr_network.PoolerConnectTimeout - 1) * 1000, false)) {
        ereport(FATAL, (errmsg("could not set timer for startup packet timeout")));
    }
#endif
    int status = StartupPacketInitialize(port);

    /*
     * Stop here if it was bad or a cancel packet.	ProcessStartupPacket
     * already did any appropriate error reporting.
     */
    if (u_sess->stream_cxt.stop_mythread) {
        gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
        return STATUS_EOF;
    }

    if (status != STATUS_OK)
        return status;

    PsDisplayInitialize(port);

    /*
     * Disable the timeout, and prevent SIGTERM/SIGQUIT again.
     */
    if (!disable_sig_alarm(false))
        ereport(FATAL, (errmsg("could not disable timer for startup packet timeout")));

    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    return STATUS_OK;
}

static void RemoteHostInitilize(Port* port)
{
    char remote_host[NI_MAXHOST];
    char remote_port[NI_MAXSERV];

    /* set these to empty in case they are needed before we set them up */
    port->remote_host = "";
    port->remote_port = "";

    /*
     * Get the remote host name and port for logging and status display.
     */
    remote_host[0] = '\0';
    remote_port[0] = '\0';

    if (pg_getnameinfo_all(&port->raddr.addr,
            port->raddr.salen,
            remote_host,
            sizeof(remote_host),
            remote_port,
            sizeof(remote_port),
            (u_sess->attr.attr_common.log_hostname ? 0 : NI_NUMERICHOST) | NI_NUMERICSERV) != 0) {
        int ret = pg_getnameinfo_all(&port->raddr.addr,
            port->raddr.salen,
            remote_host,
            sizeof(remote_host),
            remote_port,
            sizeof(remote_port),
            NI_NUMERICHOST | NI_NUMERICSERV);

        if (ret != 0)
            ereport(WARNING, (errmsg_internal("pg_getnameinfo_all() failed: %s", gai_strerror(ret))));
    }

    if (u_sess->attr.attr_storage.Log_connections) {
        if (remote_port[0])
            ereport(LOG, (errmsg("connection received: host=%s port=%s", remote_host, remote_port)));
        else
            ereport(LOG, (errmsg("connection received: host=%s", remote_host)));
    }

    /*
     * save remote_host and remote_port in port structure
     */
    port->remote_host = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), remote_host);
    port->remote_port = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), remote_port);

    if (u_sess->attr.attr_common.log_hostname)
        port->remote_hostname = port->remote_host;
}

static int StartupPacketInitialize(Port* port)
{
    int status;
    sigset_t old_sigset;
    bool connect_baseport = true;

    /*
     * Unblock SIGUSR2 so that SIGALRM can be triggered if ProcessStartupPacket encounter timeout.
     */
    old_sigset = gs_signal_unblock_sigusr2();

    /*
     * Receive the startup packet (which might turn out to be a cancel request
     * packet). for logic connection interruption is not allowed.
     */
    connect_baseport = IsConnectBasePort(port);
    if (IS_PGXC_DATANODE && g_instance.attr.attr_storage.comm_cn_dn_logic_conn && connect_baseport)
        t_thrd.postmaster_cxt.ProcessStartupPacketForLogicConn = true;
    status = ProcessStartupPacket(port, false);
    if (IS_PGXC_DATANODE && g_instance.attr.attr_storage.comm_cn_dn_logic_conn && connect_baseport)
        t_thrd.postmaster_cxt.ProcessStartupPacketForLogicConn = false;

    CHECK_FOR_PROCDIEPENDING();

    /* recover the signal mask */
    gs_signal_recover_mask(old_sigset);

    return status;
}

static void PsDisplayInitialize(Port* port)
{
#ifdef ENABLE_LITE_MODE
    char thr_name[16];
    int rcs = 0;

    if (t_thrd.role == WORKER) {
        rcs = snprintf_truncated_s(thr_name, sizeof(thr_name), "w:%s", port->user_name);
        securec_check_ss(rcs, "\0", "\0");
        (void)pthread_setname_np(gs_thread_self(), thr_name);
    } else if (t_thrd.role == THREADPOOL_WORKER) {
        rcs = snprintf_truncated_s(thr_name, sizeof(thr_name), "tw:%s", port->user_name);
        securec_check_ss(rcs, "\0", "\0");
        (void)pthread_setname_np(gs_thread_self(), thr_name);
    }

#else
    char remote_ps_data[NI_MAXHOST + NI_MAXSERV + 2];
    errno_t rc;

    if (port->remote_port[0] == '\0')
        rc = snprintf_s(remote_ps_data, sizeof(remote_ps_data), NI_MAXHOST, "%s", port->remote_host);
    else
        rc = snprintf_s(remote_ps_data,
            sizeof(remote_ps_data),
            sizeof(remote_ps_data) - 1,
            "%s(%s)",
            port->remote_host,
            port->remote_port);
    securec_check_ss_c(rc, "\0", "\0");

    /*
     * Now that we have the user and database name, we can set the process
     * title for ps.  It's good to do this as early as possible in startup.
     *
     * For a walsender, the ps display is set in the following form:
     *
     * postgres: wal sender process <user> <host> <activity>
     *
     * To achieve that, we pass "wal sender process" as username and username
     * as dbname to init_ps_display(). XXX: should add a new variant of
     * init_ps_display() to avoid abusing the parameters like this.
     */
    if (AM_WAL_SENDER)
        init_ps_display("wal sender process",
            port->user_name,
            remote_ps_data,
            u_sess->attr.attr_common.update_process_title ? "authentication" : "");
    else
        init_ps_display(port->user_name,
            port->database_name,
            remote_ps_data,
            u_sess->attr.attr_common.update_process_title ? "authentication" : "");
#endif
}

void PortInitialize(Port* port, knl_thread_arg* arg)
{
    /* When we do port init at GaussDBThreadMain, use knl_thread_arg to init */
    if (arg != NULL) {
        /* Read in the variables file */
        int ss_rc = memset_s(port, sizeof(Port), 0, sizeof(Port));
        securec_check(ss_rc, "\0", "\0");

        /*
         * Socket 0 may be closed if we do not use it, so we
         * must set socket to invalid socket instead of 0.
         */
        port->sock = PGINVALID_SOCKET;
        port->gs_sock = GS_INVALID_GSOCK;

        /* Save port etc. for ps status */
        u_sess->proc_cxt.MyProcPort = port;

        /* read variables from arg */
        read_backend_variables(arg->save_para, port);

        /* fix thread pool workers and some background threads creation_time
         * in pg_os_threads view not correct issue.
         */
        u_sess->proc_cxt.MyProcPort->SessionStartTime = GetCurrentTimestamp();
    }

    /*
     * Set up memory area for GSS information. Mirrors the code in ConnCreate
     * for the non-exec case.
     */
#if defined(ENABLE_GSS) || defined(ENABLE_SSPI)
    port->gss = (pg_gssinfo*)MemoryContextAllocZero(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), 1 * sizeof(pg_gssinfo));

    if (!port->gss)
        ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
#endif
}

void CheckClientIp(Port* port)
{
    /* Check whether the client ip is configured in pg_hba.conf */
    char ip[IP_LEN] = {'\0'};
    if (!check_ip_whitelist(port, ip, IP_LEN)) {
        pq_init();                                          /* initialize libpq to talk to client */
        t_thrd.postgres_cxt.whereToSendOutput = DestRemote; /* now safe to ereport to client */
        ereport(FATAL,
            (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                errmsg("no pg_hba.conf entry for host \"%s\".", ip)));
    }
}

void initRandomState(TimestampTz start_time, TimestampTz stop_time)
{
    long secs;
    int usecs;

    /*
     * Don't want backend to be able to see the postmaster random number
     * generator state.  We have to clobber the static random_seed *and* start
     * a new random sequence in the random() library function.
     */
    t_thrd.postmaster_cxt.random_seed = 0;
    t_thrd.postmaster_cxt.random_start_time.tv_usec = 0;
    /* slightly hacky way to get integer microseconds part of timestamptz */
    TimestampDifference(start_time, stop_time, &secs, &usecs);
    gs_srandom((unsigned int)(t_thrd.proc_cxt.MyProcPid ^ (unsigned int)usecs));
}

/*
 * BackendRun -- set up the backend's argument list and invoke PostgresMain()
 *
 * returns:
 *		Shouldn't return at all.
 *		If PostgresMain() fails, return status.
 */
static int BackendRun(Port* port)
{
    char** av;
    int maxac;
    int ac;
    long secs;
    int usecs;
    int i;

    /* add process definer mode */
    Reset_Pseudo_CurrentUserId();

    /*
     * Don't want backend to be able to see the postmaster random number
     * generator state.  We have to clobber the static random_seed *and* start
     * a new random sequence in the random() library function.
     */
    t_thrd.postmaster_cxt.random_seed = 0;
    t_thrd.postmaster_cxt.random_start_time.tv_usec = 0;
    /* slightly hacky way to get integer microseconds part of timestamptz */
    TimestampDifference(0, port->SessionStartTime, &secs, &usecs);
    gs_srandom((unsigned int)(t_thrd.proc_cxt.MyProcPid ^ (unsigned int)usecs));

    /*
     * Now, build the argv vector that will be given to PostgresMain.
     *
     * The maximum possible number of commandline arguments that could come
     * from ExtraOptions is (strlen(ExtraOptions) + 1) / 2; see
     * pg_split_opts().
     */
    maxac = 2; /* for fixed args supplied below */
    maxac += (strlen(g_instance.ExtraOptions) + 1) / 2;

    av = (char**)MemoryContextAlloc(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR),
                                    maxac * sizeof(char*));
    ac = 0;

    av[ac++] = "gaussdb";

    /*
     * Pass any backend switches specified with -o on the postmaster's own
     * command line.  We assume these are secure.  (It's OK to mangle
     * ExtraOptions now, since we're safely inside a subprocess.)
     */
    pg_split_opts(av, &ac, g_instance.ExtraOptions);

    av[ac] = NULL;

    Assert(ac < maxac);

    /*
     * Debug: print arguments being passed to backend
     */
    ereport(DEBUG3, (errmsg_internal("%s child[%d]: starting with (", progname, (int)gs_thread_self())));

    for (i = 0; i < ac; ++i)
        ereport(DEBUG3, (errmsg_internal("\t%s", av[i])));

    ereport(DEBUG3, (errmsg_internal(")")));

    /*
     * Make sure we aren't in t_thrd.mem_cxt.postmaster_mem_cxt anymore.  (We can't delete it
     * just yet, though, because InitPostgres will need the HBA data.)
     */
    MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));

    return PostgresMain(ac, av, port->database_name, port->user_name);
}

#ifdef ENABLE_LLT
extern "C" {
extern void HLLT_Coverage_SaveCoverageData();
}
#endif

/*
 * ExitPostmaster -- cleanup
 *
 * Do NOT call exit() directly --- always go through here!
 */
void ExitPostmaster(int status)
{
    /* should cleanup shared memory and kill all backends */

    /*
     * Not sure of the semantics here.	When the Postmaster dies, should the
     * backends all be killed? probably not.
     *
     * MUST		-- vadim 05-10-1999
     */

    CloseGaussPidDir();

#ifndef ENABLE_LITE_MODE
    obs_deinitialize();
#endif

    /* when exiting the postmaster process, destroy the hash table */
    if (g_instance.comm_cxt.usedDnSpace != NULL) {
        hash_destroy(g_instance.comm_cxt.usedDnSpace);
    }

    if (g_instance.policy_cxt.account_table != NULL) {
        hash_destroy(g_instance.policy_cxt.account_table);
    }

#ifdef ENABLE_MOT
    TermMOT();   /* shutdown memory engine before codegen is destroyed */
#endif

    if (ENABLE_THREAD_POOL_DN_LOGICCONN) {
        ProcessCommLogicTearDown();
    }

#ifdef ENABLE_LLVM_COMPILE
    CodeGenProcessTearDown();
#endif

    /* Save llt data to disk before postmaster exit */
#ifdef ENABLE_LLT
    HLLT_Coverage_SaveCoverageData();
#endif

    // flush stdout buffer before _exit
    //
    fflush(stdout);

    LogCtlLastFlushBeforePMExit();

    proc_exit(status);
}

static void handle_recovery_started()
{
    if (pmState == PM_STARTUP && g_instance.status == NoShutdown && !dummyStandbyMode) {
        /* WAL redo has started. We're out of reinitialization. */
        g_instance.fatal_error = false;
        g_instance.demotion = NoDemote;

        /*
         * Crank up the background tasks.  It doesn't matter if this fails,
         * we'll just try again later.
         */
        Assert(g_instance.pid_cxt.CheckpointerPID == 0);
        g_instance.pid_cxt.CheckpointerPID = initialize_util_thread(CHECKPOINT_THREAD);
        Assert(g_instance.pid_cxt.BgWriterPID == 0);
        if (!ENABLE_INCRE_CKPT) {
            g_instance.pid_cxt.BgWriterPID = initialize_util_thread(BGWRITER);
        }
        Assert(g_instance.pid_cxt.SpBgWriterPID == 0);
        g_instance.pid_cxt.SpBgWriterPID = initialize_util_thread(SPBGWRITER);

        Assert(g_instance.pid_cxt.PageRepairPID == 0);
        g_instance.pid_cxt.PageRepairPID = initialize_util_thread(PAGEREPAIR_THREAD);

        if (ENABLE_INCRE_CKPT) {
            for (int i = 0; i < g_instance.ckpt_cxt_ctl->pgwr_procs.num; i++) {
                Assert(g_instance.pid_cxt.PageWriterPID[i] == 0);
                g_instance.pid_cxt.PageWriterPID[i] = initialize_util_thread(PAGEWRITER_THREAD);
            }
        }
        Assert(g_instance.pid_cxt.CBMWriterPID == 0);
        if (u_sess->attr.attr_storage.enable_cbm_tracking) {
            g_instance.pid_cxt.CBMWriterPID = initialize_util_thread(CBMWRITER);
        }
        pmState = PM_RECOVERY;
    }
}

static void handle_begin_hot_standby()
{
    if ((dummyStandbyMode || pmState == PM_RECOVERY) && g_instance.status == NoShutdown) {
        /*
         * Likewise, start other special children as needed.
         */
        Assert(g_instance.pid_cxt.PgStatPID == 0);
        if (!dummyStandbyMode)
            g_instance.pid_cxt.PgStatPID = pgstat_start();
        PMUpdateDBState(NORMAL_STATE, get_cur_mode(), get_cur_repl_num());
        ereport(LOG,
            (errmsg("update gaussdb state file: db state(NORMAL_STATE), server mode(%s)",
                wal_get_role_string(get_cur_mode()))));

        ereport(LOG, (errmsg("database system is ready to accept read only connections")));
#ifdef ENABLE_MULTIPLE_NODES
        if (IS_DISASTER_RECOVER_MODE && g_instance.pid_cxt.BarrierPreParsePID == 0) {
            g_instance.pid_cxt.BarrierPreParsePID = initialize_util_thread(BARRIER_PREPARSE);
        }
#endif
        pmState = PM_HOT_STANDBY;
    }
}

static void handle_promote_signal()
{
    if (g_instance.pid_cxt.StartupPID != 0 &&
        (pmState == PM_STARTUP ||
         pmState == PM_RECOVERY ||
         pmState == PM_HOT_STANDBY ||
         pmState == PM_WAIT_READONLY)) {
        gs_lock_test_and_set_64(&g_instance.stat_cxt.NodeStatResetTime, GetCurrentTimestamp());
        if (GetHaShmemMode() != STANDBY_MODE) {
            ereport(LOG, (errmsg("Instance can't be promoted in none standby mode.")));
        } else if (IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE) {
            ereport(LOG, (errmsg("Instance can't be promoted in standby cluster")));
        } else if (t_thrd.postmaster_cxt.HaShmData->is_hadr_main_standby) {
            ereport(LOG, (errmsg("Instance can't be promoted in hadr_main_standby mode")));
        } else {
           /* Database Security: Support database audit */
           if (t_thrd.walreceiverfuncs_cxt.WalRcv &&
               NODESTATE_STANDBY_PROMOTING == t_thrd.walreceiverfuncs_cxt.WalRcv->node_state) {
               t_thrd.postmaster_cxt.audit_standby_switchover = true;
               /* Tell startup process to finish recovery */
               SendNotifySignal(NOTIFY_SWITCHOVER, g_instance.pid_cxt.StartupPID);
           } else {
                if (t_thrd.walreceiverfuncs_cxt.WalRcv)
                    t_thrd.walreceiverfuncs_cxt.WalRcv->node_state = NODESTATE_STANDBY_FAILOVER_PROMOTING;
                t_thrd.postmaster_cxt.audit_primary_failover = true;
                /* Tell startup process to finish recovery */
                ereport(LOG, (errmsg("Instance to do failover.")));
                SendNotifySignal(NOTIFY_FAILOVER, g_instance.pid_cxt.StartupPID);
           }
        }
    }
}

static void handle_primary_signal(volatile HaShmemData* hashmdata)
{
    if (g_instance.pid_cxt.StartupPID != 0 &&
        hashmdata->current_mode == PENDING_MODE &&
        (pmState == PM_STARTUP ||
         pmState == PM_RECOVERY ||
         pmState == PM_HOT_STANDBY ||
         pmState == PM_WAIT_READONLY)) {
        /* just notify the startup process, does not set HAshmemory here. */
        SendNotifySignal(NOTIFY_PRIMARY, g_instance.pid_cxt.StartupPID);
    }

    if (hashmdata->current_mode == NORMAL_MODE) {
        if (g_instance.attr.attr_storage.max_wal_senders < 2 ||
            g_instance.attr.attr_storage.wal_level != WAL_LEVEL_HOT_STANDBY ||
            g_instance.attr.attr_storage.EnableHotStandby == false)
            ereport(WARNING, (errmsg("when notifying normal mode to primary mode, \
                         wal_level requires \"hot_standby\", and hot_standby requires \"on\", \
                         and max_wal_senders requires at least 2.")));
        else {
            hashmdata->current_mode = PRIMARY_MODE;
            g_instance.global_sysdbcache.RefreshHotStandby();
            UpdateOptsFile();
        }
    }

    PMUpdateDBState(NORMAL_STATE, get_cur_mode(), get_cur_repl_num());
    ereport(LOG,
        (errmsg("update gaussdb state file: db state(NORMAL_STATE), server mode(%s)",
            wal_get_role_string(get_cur_mode()))));
}

static void handle_standby_signal(volatile HaShmemData* hashmdata)
{
    if (g_instance.pid_cxt.StartupPID != 0 &&
        (pmState == PM_STARTUP ||
         pmState == PM_RECOVERY ||
         pmState == PM_HOT_STANDBY ||
         pmState == PM_WAIT_READONLY)) {
        hashmdata->current_mode = STANDBY_MODE;
        g_instance.global_sysdbcache.RefreshHotStandby();
        PMUpdateDBState(NEEDREPAIR_STATE, get_cur_mode(), get_cur_repl_num());
        ereport(LOG,
           (errmsg("update gaussdb state file: db state(NEEDREPAIR_STATE), server mode(%s)",
               wal_get_role_string(get_cur_mode()))));
        /*
        * wakeup startup process from sleep by signal, cause we are
        * in standby mode, the signal has no specific affect.
        */
        SendNotifySignal(NOTIFY_STANDBY, g_instance.pid_cxt.StartupPID);
        UpdateOptsFile();
    }
}

static void handle_cascade_standby_signal(volatile HaShmemData* hashmdata)
{
    if (g_instance.pid_cxt.StartupPID != 0 &&
        (pmState == PM_STARTUP ||
         pmState == PM_RECOVERY ||
         pmState == PM_HOT_STANDBY ||
         pmState == PM_WAIT_READONLY)) {
        hashmdata->current_mode = STANDBY_MODE;
        g_instance.global_sysdbcache.RefreshHotStandby();
        hashmdata->is_cascade_standby = true;
        PMUpdateDBState(NEEDREPAIR_STATE, get_cur_mode(), get_cur_repl_num());
        ereport(LOG,
           (errmsg("update gaussdb state file: db state(NEEDREPAIR_STATE), server mode(%s)",
               wal_get_role_string(get_cur_mode()))));
        /*
        * wakeup startup process from sleep by signal, cause we are
        * in standby mode, the signal has no specific affect.
        */
        SendNotifySignal(NOTIFY_CASCADE_STANDBY, g_instance.pid_cxt.StartupPID);
        UpdateOptsFile();
    }
}

#ifndef ENABLE_MULTIPLE_NODES
static bool CheckChangeRoleFileExist(const char *filename)
{
    struct stat stat_buf;
    if (stat(filename, &stat_buf) != 0 || !S_ISREG(stat_buf.st_mode)) {
        return false;
    } else {
        return true;
    }
}

static bool CheckSignalByFile(const char *filename, void *infoPtr, size_t infoSize)
{
    FILE* sofile = nullptr;
    sofile = fopen(filename, "rb");
    if (sofile == nullptr) {
        /* File doesn't exist. */
        if (errno == ENOENT) {
            return false;
        }
        (void)unlink(filename);
        ereport(WARNING, (errmsg("Open file %s failed for %s!", filename, strerror(errno))));
        return false;
    }
    /* Read info from filename */
    if (fread(infoPtr, infoSize, 1, sofile) != 1) {
        fclose(sofile);
        sofile = nullptr;
        (void)unlink(filename);
        ereport(WARNING, (errmsg("Read file %s failed!", filename)));
        return false;
    }
    fclose(sofile);
    sofile = nullptr;
    (void)unlink(filename);
    return true;

}
bool CheckAddMemberSignal(NewNodeInfo *nodeinfoPtr)
{
    return CheckSignalByFile(AddMemberFile, nodeinfoPtr, sizeof(NewNodeInfo));
}

void handle_add_member_signal(NewNodeInfo nodeinfo)
{
    ereport(LOG, (errmsg("Begin to handle adding a member...")));
    /* Call dcf interface to add member */
    int ret = dcf_add_member(nodeinfo.stream_id,
                             nodeinfo.node_id,
                             nodeinfo.ip,
                             nodeinfo.port,
                             static_cast<dcf_role_t>(nodeinfo.role),
                             nodeinfo.wait_timeout_ms);
    if (ret != 0) {
        ereport(WARNING, (errmsg("DCF add member failed!")));
    } else {
        ereport(LOG, (errmsg("Add member with node id %u successfully.", nodeinfo.node_id)));
    }
}

/* If return 0, then no remove member triggered. The DCF node ID can't be 0. */
static bool CheckRemoveMemberSignal(uint32 *nodeID)
{
    return CheckSignalByFile(RemoveMemberFile, nodeID, sizeof(uint32));
}

static void handle_remove_member_signal(uint32 nodeID)
{
    ereport(LOG, (errmsg("Begin to remove member with node id %u.", nodeID)));
    int ret = dcf_remove_member(1, nodeID, 1000);
    if (ret != 0) {
        ereport(WARNING, (errmsg("Remove member with node ID %u from DCF failed!", nodeID)));
        return;
    }
    ereport(LOG, (errmsg("Remove member with node id %u successfully.", nodeID)));
    /* Remove node from nodesinfo */
    bool isResetNode = ResetDCFNodeInfoWithNodeID(nodeID);
    if (!isResetNode) {
        ereport(WARNING, (errmsg("Didn't find node with node id %d in DCF nodes info", nodeID)));
    }
}

static bool CheckChangeRoleSignal()
{
    return CheckChangeRoleFileExist(ChangeRoleFile);
}

static int changeRole(const char *role)
{
    FILE *sofile = nullptr;
    int timeout = 60; /* seconds */
    int ret = -1;
    char changeStr[MAXPGPATH] = {0};
    int roleStatus = -1;
    int groupStatus = -1;
    int priorityStatus = -1;
    errno_t rc;
    sofile = fopen(TimeoutFile, "rb");
    if (sofile == nullptr) {
        ereport(WARNING, (errmsg("Open timeout file %s failed!", TimeoutFile)));
        return -1;
    }
    if (fread(&timeout, sizeof(timeout), 1, sofile) == 0) {
        fclose(sofile);
        sofile = nullptr;
        (void)unlink(TimeoutFile);
        ereport(WARNING, (errmsg("Read timeout file %s failed!", TimeoutFile)));
        return -1;
    }
    fclose(sofile);
    sofile = nullptr;
    (void)unlink(TimeoutFile);

    const uint32 unit = 1000;
    uint32 mTimeout = (uint32)timeout * unit;
    ereport(LOG, (errmsg("The timeout of changing role is %d ms!", mTimeout)));
    if (sscanf_s(role, "%d_%d_%d", &roleStatus, &groupStatus, &priorityStatus) != 3) {
        ereport(WARNING, (errmsg("Content could not get all param for change role %s", role)));
        return -1;
    }
    if (roleStatus == 0) { /* fo denotes follower role */
        if (groupStatus == -1 && priorityStatus == -1) {
            ret = dcf_change_member_role(1, g_instance.attr.attr_storage.dcf_attr.dcf_node_id,
                static_cast<dcf_role_t>(2), mTimeout);
        } else if (groupStatus == -1 && priorityStatus != -1) {
            rc = snprintf_s(changeStr, MAXPGPATH, MAXPGPATH - 1,
                "[{\"stream_id\":1,\"node_id\":%d, \"priority\":%d,\"role\":\"FOLLOWER\"}]",
                g_instance.attr.attr_storage.dcf_attr.dcf_node_id, priorityStatus);
            securec_check_ss_c(rc, "\0", "\0");
            ret = dcf_change_member(changeStr, 60000);
        } else if (groupStatus != -1 && priorityStatus == -1) {
            rc = snprintf_s(changeStr, MAXPGPATH, MAXPGPATH - 1,
                "[{\"stream_id\":1,\"node_id\":%d, \"group\":%d,\"role\":\"FOLLOWER\"}]",
                g_instance.attr.attr_storage.dcf_attr.dcf_node_id, groupStatus);
            securec_check_ss_c(rc, "\0", "\0");
            ret = dcf_change_member(changeStr, 60000);
        } else {
            rc = snprintf_s(changeStr, MAXPGPATH, MAXPGPATH - 1,
                "[{\"stream_id\":1,\"node_id\":%d, \"group\":%d,\"priority\":%d,\"role\":\"FOLLOWER\"}]",
                g_instance.attr.attr_storage.dcf_attr.dcf_node_id, groupStatus, priorityStatus);
            securec_check_ss_c(rc, "\0", "\0");
            ret = dcf_change_member(changeStr, 60000);
        }
    } else if (roleStatus == 1) { /* pa denotes passive role */
        if (groupStatus == -1 && priorityStatus == -1) {
            ret = dcf_change_member_role(1, g_instance.attr.attr_storage.dcf_attr.dcf_node_id,
                static_cast<dcf_role_t>(4), mTimeout);
        } else if (groupStatus == -1 && priorityStatus != -1) {
            rc = snprintf_s(changeStr, MAXPGPATH, MAXPGPATH - 1,
                "[{\"stream_id\":1,\"node_id\":%d, \"priority\":%d,\"role\":\"PASSIVE\"}]",
                g_instance.attr.attr_storage.dcf_attr.dcf_node_id, priorityStatus);
            securec_check_ss_c(rc, "\0", "\0");
            ret = dcf_change_member(changeStr, 60000);
        } else if (groupStatus != -1 && priorityStatus == -1) {
            rc = snprintf_s(changeStr, MAXPGPATH, MAXPGPATH - 1,
                "[{\"stream_id\":1,\"node_id\":%d, \"group\":%d,\"role\":\"PASSIVE\"}]",
                g_instance.attr.attr_storage.dcf_attr.dcf_node_id, groupStatus);
            securec_check_ss_c(rc, "\0", "\0");
            ret = dcf_change_member(changeStr, 60000);
        } else {
            rc = snprintf_s(changeStr, MAXPGPATH, MAXPGPATH - 1,
                "[{\"stream_id\":1,\"node_id\":%d, \"group\":%d,\"priority\":%d,\"role\":\"PASSIVE\"}]",
                g_instance.attr.attr_storage.dcf_attr.dcf_node_id, groupStatus, priorityStatus);
            securec_check_ss_c(rc, "\0", "\0");
            ret = dcf_change_member(changeStr, 60000);
        }
    } else if (roleStatus == -1) {
        if (groupStatus == -1 && priorityStatus == -1) {
            ereport(WARNING, (errmsg("Nothing changed when change role been called %s", role)));
            return 0;
        } else if (groupStatus == -1 && priorityStatus != -1) {
            rc = snprintf_s(changeStr, MAXPGPATH, MAXPGPATH - 1,
                "[{\"stream_id\":1,\"node_id\":%d, \"priority\":%d}]",
                g_instance.attr.attr_storage.dcf_attr.dcf_node_id, priorityStatus);
            securec_check_ss_c(rc, "\0", "\0");
            ret = dcf_change_member(changeStr, 60000);
        } else if (groupStatus != -1 && priorityStatus == -1) {
            rc = snprintf_s(changeStr, MAXPGPATH, MAXPGPATH - 1,
                "[{\"stream_id\":1,\"node_id\":%d, \"group\":%d}]",
                g_instance.attr.attr_storage.dcf_attr.dcf_node_id, groupStatus);
            securec_check_ss_c(rc, "\0", "\0");
            ret = dcf_change_member(changeStr, 60000);
        } else {
            rc = snprintf_s(changeStr, MAXPGPATH, MAXPGPATH - 1,
                "[{\"stream_id\":1,\"node_id\":%d, \"group\":%d,\"priority\":%d}]",
                g_instance.attr.attr_storage.dcf_attr.dcf_node_id, groupStatus, priorityStatus);
            securec_check_ss_c(rc, "\0", "\0");
            ret = dcf_change_member(changeStr, 60000);
        }
    } else {
        ereport(WARNING, (errmsg("Content prefix read from role file can not be recognized %s", role)));
        return -1;
    }
    if (ret != 0) {
        ereport(WARNING, (errmsg("DCF failed to change role to %s with err code %d!", role, ret)));
    } else {
        ereport(LOG, (errmsg("DCF succeeded to change role to %s.", role)));
    }
    return ret;
}

static void handle_change_role_signal(char *role)
{
    ereport(LOG, (errmsg("Begin to change role.")));
    int ret = changeRole(role);
    FILE* sofile = nullptr;

    /* Write status into changlerole status file */
    sofile = fopen(g_changeRoleStatusFile, "wb");
    if (sofile == nullptr) {
        ereport(WARNING, (errmsg("Open %s failed!", g_changeRoleStatusFile)));
        return;
    }
    if (fwrite(&ret, sizeof(ret), 1, sofile) == 0) {
        fclose(sofile);
        sofile = nullptr;
        ereport(WARNING, (errmsg("Write change role status into %s failed!", g_changeRoleStatusFile)));
        return;
    }
    fclose(sofile);
}

static bool CheckSetRunModeSignal(RunModeParam *paramPtr)
{
    return CheckSignalByFile(StartMinorityFile, paramPtr, sizeof(RunModeParam));
}

static void handle_start_minority_signal(RunModeParam param)
{
    unsigned int vote_num = 0;
    unsigned int xmode = 0;
    FILE *sofile = nullptr;
    vote_num = param.voteNum;
    xmode = param.xMode;
    ereport(LOG, (errmsg("DCF going to set run-mode, vote num %d, xmode %d", vote_num, xmode)));
    Assert(xmode == 0 || xmode == 1);
    if (xmode == 0) {
        vote_num = 0;
    }
    int ret = dcf_set_work_mode(1, (dcf_work_mode_t)xmode, vote_num);
    if (ret != 0) {
        ereport(LOG, (errmsg("DCF failed to set run-mode with err code %d!", ret)));
    } else {
        ereport(LOG, (errmsg("DCF succeeded to set run-mode")));
    }
    /* Write status into setrunmode status file */
    sofile = fopen(SetRunmodeStatusFile, "wb");
    if (sofile == nullptr) {
        ereport(WARNING, (errmsg("Open %s failed!", SetRunmodeStatusFile)));
        return;
    }
    if (fwrite(&ret, sizeof(ret), 1, sofile) == 0) {
        fclose(sofile);
        sofile = nullptr;
        ereport(WARNING, (errmsg("Write set-run-mode status into %s failed!", SetRunmodeStatusFile)));
        return;
    }
    fclose(sofile);
}
#endif

static void PaxosPromoteLeader(void)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(t_thrd.dcf_cxt.dcfCtxInfo->isDcfStarted);
    ereport(LOG, (errmsg("The node with nodeID %d begin to promote leader in DCF mode.", 
                    g_instance.attr.attr_storage.dcf_attr.dcf_node_id)));
    int timeout = 60; /* seconds */
    /* Read timeout from TimeoutFile */
    FILE* fd = NULL;
    fd = fopen(TimeoutFile, "r");
    if (fd != NULL) {
        if ((fread(&timeout, sizeof(timeout), 1, fd)) == 0) {
            timeout = 60;
            ereport(WARNING,
                    (errmsg("Read timeout from %s failed and take 60s as default timeout!", TimeoutFile)));
        }
        fclose(fd);
        fd = NULL;
    } else {
        ereport(LOG, (errmsg("Open %s failed and take 60s as default timeout!", TimeoutFile)));
    }
    ereport(LOG, (errmsg("timeout is %d!", timeout)));
    int promoteRes = dcf_promote_leader(1, g_instance.attr.attr_storage.dcf_attr.dcf_node_id, timeout * 1000);
    if (promoteRes != 0) {
        ereport(WARNING, (errmsg("PromoteLeader ERROR: returns %d!", promoteRes)));
    }
    unlink(TimeoutFile);
    /* Write status into switchover file */
    fd = fopen(SwitchoverStatusFile, "w");
    if (fd == NULL) {
        ereport(WARNING, (errmsg("Open switchover status file, %s, failed!", SwitchoverStatusFile)));
        return;
    }
    if (fwrite(&promoteRes, sizeof(promoteRes), 1, fd) == 0) {
        fclose(fd);
        fd = NULL;
        ereport(WARNING, (errmsg("Write switchover status file, %s, failed!", SwitchoverStatusFile)));
        return;
    }
    fclose(fd);
    fd = NULL;
    ereport(LOG, (errmsg("Write dcf promote result %d into switchover status file, %s, successfully!",
        promoteRes, SwitchoverStatusFile)));
#endif
}

/*
 * sigusr1_handler - handle signal conditions from child processes
 */
static void sigusr1_handler(SIGNAL_ARGS)
{
    int mode = 0;
    int save_errno = errno;
    volatile HaShmemData* hashmdata = t_thrd.postmaster_cxt.HaShmData;

    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    /*
     * RECOVERY_STARTED and BEGIN_HOT_STANDBY signals are ignored in
     * unexpected states. If the startup process quickly starts up, completes
     * recovery, exits, we might process the death of the startup process
     * first. We don't want to go back to recovery in that case.
     */
    if (CheckPostmasterSignal(PMSIGNAL_RECOVERY_STARTED)) {
        handle_recovery_started();
    }

    if (CheckPostmasterSignal(PMSIGNAL_BEGIN_HOT_STANDBY)) {
        handle_begin_hot_standby();
    }

    if (CheckPostmasterSignal(PMSIGNAL_LOCAL_RECOVERY_DONE) &&
        (pmState == PM_STARTUP || pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY || pmState == PM_WAIT_READONLY) &&
        g_instance.status == NoShutdown) {
        PMUpdateDBStateLSN();
        ereport(LOG, (errmsg("set lsn after recovery done in gaussdb state file")));
    }

    if (g_instance.pid_cxt.WalWriterAuxiliaryPID == 0 && 
        t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE &&
        (pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY)) {
        g_instance.pid_cxt.WalWriterAuxiliaryPID = initialize_util_thread(WALWRITERAUXILIARY);
        ereport(LOG,
            (errmsg("sigusr1_handler create WalWriterAuxiliary(%lu) after local recovery is done. \
                pmState:%u, ServerMode:%u",
                g_instance.pid_cxt.WalWriterAuxiliaryPID, pmState, t_thrd.postmaster_cxt.HaShmData->current_mode)));
    }
        
    if (CheckPostmasterSignal(PMSIGNAL_UPDATE_WAITING)) {
        PMUpdateDBState(WAITING_STATE, get_cur_mode(), get_cur_repl_num());
        ereport(LOG,
            (errmsg("set gaussdb state file: db state(WAITING_STATE), server mode(%s)",
                wal_get_role_string(get_cur_mode()))));
    }
    if (CheckPostmasterSignal(PMSIGNAL_UPDATE_PROMOTING)) {
        PMUpdateDBState(PROMOTING_STATE, get_cur_mode(), get_cur_repl_num());
        ereport(LOG,
            (errmsg("set gaussdb state file: db state(PROMOTING_STATE), server mode(%s)",
                wal_get_role_string(get_cur_mode()))));

        /* promote cascade standby */
        if (IsCascadeStandby()) {
            t_thrd.xlog_cxt.is_cascade_standby = false;
            if (t_thrd.postmaster_cxt.HaShmData->is_cross_region) {
                t_thrd.xlog_cxt.is_hadr_main_standby = true;
            }
            SetHaShmemData();
            if (g_instance.pid_cxt.HeartbeatPID != 0) {
                signal_child(g_instance.pid_cxt.HeartbeatPID, SIGTERM);
            }
        }
    }
    if (CheckPostmasterSignal(PMSIGNAL_UPDATE_HAREBUILD_REASON) &&
        (pmState == PM_STARTUP || pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY || pmState == PM_WAIT_READONLY) &&
        g_instance.status == NoShutdown) {
        PMUpdateDBStateHaRebuildReason();
    }

    if (CheckPostmasterSignal(PMSIGNAL_WAKEN_ARCHIVER) && g_instance.pid_cxt.PgArchPID != 0) {
        /*
         * Send SIGUSR1 to archiver process, to wake it up and begin archiving
         * next transaction log file.
         */
        signal_child(g_instance.pid_cxt.PgArchPID, SIGUSR1);
    }

    if (CheckPostmasterSignal(PMSIGNAL_WAKEN_ARCHIVER) && g_instance.archive_thread_info.obsArchPID != NULL) {
        /*
         * Send SIGUSR1 to archiver process, to wake it up and begin archiving
         * next transaction log file.
         */
        for (int i = 0; i < g_instance.attr.attr_storage.max_replication_slots; i++) {
            if (g_instance.archive_thread_info.obsArchPID[i] != 0) {
                signal_child(g_instance.archive_thread_info.obsArchPID[i], SIGUSR1);
            }
        }
    }

    if (CheckPostmasterSignal(PMSIGNAL_ROTATE_LOGFILE) && g_instance.pid_cxt.SysLoggerPID != 0) {
        /* Tell syslogger to rotate logfile */
        signal_child(g_instance.pid_cxt.SysLoggerPID, SIGUSR1);
    }

    if (CheckPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER) && g_instance.status == NoShutdown) {
        /*
         * Start one iteration of the autovacuum daemon, even if autovacuuming
         * is nominally not enabled.  This is so we can have an active defense
         * excessive clog.  We set a flag for the main loop to do it rather than
         * trying to do it here --- this is because the autovac process itself
         * may send the signal, and we want to handle that by launching
         * another iteration as soon as the current one completes.
         */
        t_thrd.postmaster_cxt.start_autovac_launcher = true;
    }

    /* should not start a worker in shutdown or demotion procedure */
    if (CheckPostmasterSignal(PMSIGNAL_START_AUTOVAC_WORKER) && g_instance.status == NoShutdown &&
        g_instance.demotion == NoDemote) {
        /* The autovacuum launcher wants us to start a worker process. */
        StartAutovacuumWorker();
    }
    
    /* should not start a worker in shutdown or demotion procedure */
    if (CheckPostmasterSignal(PMSIGNAL_START_RB_WORKER) && g_instance.status == NoShutdown &&
        g_instance.demotion == NoDemote) {
        /* The rbcleaner wants us to start a worker process. */
        StartRbWorker();
    }

    /* should not start a worker in shutdown or demotion procedure */
    if (CheckPostmasterSignal(PMSIGNAL_START_TXNSNAPWORKER) && g_instance.status == NoShutdown &&
        g_instance.demotion == NoDemote) {
        /* The rbcleaner wants us to start a worker process. */
        StartTxnSnapWorker();
    }

    /* should not start a worker in shutdown or demotion procedure */
    if (CheckPostmasterSignal(PMSIGNAL_START_UNDO_WORKER) &&
        g_instance.status == NoShutdown &&
        g_instance.demotion == NoDemote) {
        StartUndoWorker();
    }

    /* should not start a worker in shutdown or demotion procedure */
    if (CheckPostmasterSignal(PMSIGNAL_START_APPLY_WORKER) &&
        g_instance.status == NoShutdown &&
        g_instance.demotion == NoDemote) {
        StartApplyWorker();
    }

    /* should not start a worker in shutdown or demotion procedure */
    if (CheckPostmasterSignal(PMSIGNAL_START_CLEAN_STATEMENT) && g_instance.status == NoShutdown &&
        g_instance.demotion == NoDemote) {
        /* The statement flush thread wants us to start a clean statement worker process. */
        StartCleanStatement();
    }

    if (CheckPostmasterSignal(PMSIGNAL_START_JOB_SCHEDULER)) {
        t_thrd.postmaster_cxt.start_job_scheduler = true;
    }

    if (CheckPostmasterSignal(PMSIGNAL_START_JOB_WORKER) &&
        g_instance.status == NoShutdown && g_instance.demotion == NoDemote) {
        /* the parent process, return 0 if the fork failed, return the PID if fork succeed. */
        StartPgjobWorker();
    }

    if (CheckPostmasterSignal(PMSIGNAL_START_WALRECEIVER) && g_instance.pid_cxt.WalReceiverPID == 0 &&
        (pmState == PM_STARTUP || pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY || pmState == PM_WAIT_READONLY) &&
        g_instance.status == NoShutdown) {
        if (g_instance.pid_cxt.WalRcvWriterPID == 0) {
            g_instance.pid_cxt.WalRcvWriterPID = initialize_util_thread(WALRECWRITE);
            SetWalRcvWriterPID(g_instance.pid_cxt.WalRcvWriterPID);
        }

        /* Startup Process wants us to start the walreceiver process. */
        g_instance.pid_cxt.WalReceiverPID = initialize_util_thread(WALRECEIVER);
    }

    if (CheckPostmasterSignal(PMSIGNAL_START_DATARECEIVER) && g_instance.pid_cxt.DataReceiverPID == 0 &&
        (pmState == PM_STARTUP || pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY || pmState == PM_WAIT_READONLY) &&
        g_instance.status == NoShutdown) {
        if (g_instance.pid_cxt.DataRcvWriterPID == 0) {
            g_instance.pid_cxt.DataRcvWriterPID = initialize_util_thread(DATARECWRITER);
            SetDataRcvWriterPID(g_instance.pid_cxt.DataRcvWriterPID);
        }

        /* Startup Process wants us to start the datareceiver process. */
        g_instance.pid_cxt.DataReceiverPID = initialize_util_thread(DATARECIVER);
    }

    if (CheckPostmasterSignal(PMSIGNAL_START_CATCHUP) && g_instance.pid_cxt.CatchupPID == 0 &&
        g_instance.status == NoShutdown) {
        /* The datasender wants us to start the catch-up process. */
        g_instance.pid_cxt.CatchupPID = StartCatchupWorker();
    }

    if (CheckPostmasterSignal(PMSIGNAL_ADVANCE_STATE_MACHINE) &&
        (pmState == PM_WAIT_BACKUP || pmState == PM_WAIT_BACKENDS)) {
        /* Advance postmaster's state machine */
        PostmasterStateMachine();
    }
    if ((mode = CheckSwitchoverSignal()) != 0 && WalRcvIsOnline() && DataRcvIsOnline() &&
        (pmState == PM_STARTUP || pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY || pmState == PM_WAIT_READONLY)) {
        if (!IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE) {
            ereport(LOG, (errmsg("to do switchover")));
            /* Label the standby to do switchover */
            t_thrd.walreceiverfuncs_cxt.WalRcv->node_state = (ClusterNodeState)mode;
            if (g_instance.attr.attr_storage.dcf_attr.enable_dcf && t_thrd.dcf_cxt.dcfCtxInfo->isDcfStarted) {
                PaxosPromoteLeader();
            } else {
                /* Tell walreceiver process to start switchover */
                signal_child(g_instance.pid_cxt.WalReceiverPID, SIGUSR1);
            }
        } else {
            ereport(LOG, (errmsg("standby cluster could not do switchover")));
        }
    }
    if (CheckPostmasterSignal(PMSIGNAL_DEMOTE_PRIMARY)) {
        gs_lock_test_and_set_64(&g_instance.stat_cxt.NodeStatResetTime, GetCurrentTimestamp());
        ProcessDemoteRequest();
    }

    if (CheckFinishRedoSignal() && g_instance.comm_cxt.localinfo_cxt.is_finish_redo != 1) {
        pg_atomic_write_u32(&(g_instance.comm_cxt.localinfo_cxt.is_finish_redo), 1);
    }
    if (CheckPromoteSignal()) {
        handle_promote_signal();
    }

    /* If it is primary signal, then set HaShmData and send sigusr2 to startup process */
    if (CheckPrimarySignal()) {
        handle_primary_signal(hashmdata);
    }

    if (CheckStandbySignal()) {
        handle_standby_signal(hashmdata);
    }

    /* If it is cascade standby signal, then set HaShmData and send sigusr2 to startup process */
    if (CheckCascadeStandbySignal()) {
        handle_cascade_standby_signal(hashmdata);
    }

    if (CheckPostmasterSignal(PMSIGNAL_UPDATE_NORMAL)) {
        PMUpdateDBState(NORMAL_STATE, get_cur_mode(), get_cur_repl_num());
        ereport(LOG,
            (errmsg("update gaussdb state file: db state(NORMAL_STATE), server mode(%s)",
                wal_get_role_string(get_cur_mode()))));
    }

    if (CheckPostmasterSignal(PMSIGNAL_ROLLBACK_STANDBY_PROMOTE)) {
        PMUpdateDBState(NORMAL_STATE, get_cur_mode(), get_cur_repl_num());
        ereport(LOG,
            (errmsg("update gaussdb state file:"
                    "db state(NORMAL_STATE), server mode(%s), reason(switchover failure)",
                wal_get_role_string(get_cur_mode()))));
    }

    if (CheckPostmasterSignal(PMSIGNAL_START_THREADPOOL_WORKER) &&
        g_instance.status == NoShutdown) {
        if (g_threadPoolControler != NULL) {
            g_threadPoolControler->AddWorkerIfNecessary();
        }
    }

#ifndef ENABLE_MULTIPLE_NODES
    uint32 nodeID = 0;
    NewNodeInfo nodeinfo;
    RunModeParam param;
    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf && CheckAddMemberSignal(&nodeinfo) &&
        t_thrd.dcf_cxt.dcfCtxInfo->isDcfStarted) {
        handle_add_member_signal(nodeinfo);
    }
    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf && CheckRemoveMemberSignal(&nodeID) &&
        t_thrd.dcf_cxt.dcfCtxInfo->isDcfStarted) {
        handle_remove_member_signal(nodeID);
    }

    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf &&
        CheckChangeRoleSignal() && t_thrd.dcf_cxt.dcfCtxInfo->isDcfStarted &&
        !g_instance.comm_cxt.isNeedChangeRole) {
        g_instance.comm_cxt.isNeedChangeRole = true;
    }

    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf &&
        CheckSetRunModeSignal(&param) && t_thrd.dcf_cxt.dcfCtxInfo->isDcfStarted) {
        handle_start_minority_signal(param);
    }
#endif

#ifndef ENABLE_LITE_MODE
#if defined (ENABLE_MULTIPLE_NODES) || defined (ENABLE_PRIVATEGAUSS)
    check_and_process_hotpatch();
#endif
#endif

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);

    errno = save_errno;
}

/*
 * Timeout or shutdown signal from postmaster while processing startup packet.
 * Cleanup and exit(1).
 *
 * XXX: possible future improvement: try to send a message indicating
 * why we are disconnecting.  Problem is to be sure we don't block while
 * doing so, nor mess up SSL initialization.  In practice, if the client
 * has wedged here, it probably couldn't do anything with the message anyway.
 */
void startup_die(SIGNAL_ARGS)
{
    /*
     * when process startup packet for logic conn
     * gs_wait_poll will hold lock, so proc_exit here
     * will occur dead lock. gs_r_cancel will signal gs_wait_poll
     * without lock and then proc_exit when
     * ProcessStartupPacketForLogicConn is false
     */
    if (t_thrd.postmaster_cxt.ProcessStartupPacketForLogicConn) {
        t_thrd.int_cxt.ProcDiePending = true;
        gs_r_cancel();
    } else {
        t_thrd.int_cxt.ProcDiePending = true;
    }
}

/* copy from startup_die, and set cancel_from_timeout flag */
static void
startup_alarm(SIGNAL_ARGS)
{
    /*
     * when process startup packet for logic conn
     * gs_wait_poll will hold lock, so proc_exit here
     * will occur dead lock. gs_r_cancel will signal gs_wait_poll
     * without lock and then proc_exit when
     * ProcessStartupPacketForLogicConn is false
     */
    t_thrd.storage_cxt.cancel_from_timeout = true;
    force_backtrace_messages = true;
    if (t_thrd.postmaster_cxt.ProcessStartupPacketForLogicConn) {
        t_thrd.int_cxt.ProcDiePending = true;
        gs_r_cancel();
    }
}

/*
 * Dummy signal handler
 *
 * We use this for signals that we don't actually use in the postmaster,
 * but we do use in backends.  If we were to SIG_IGN such signals in the
 * postmaster, then a newly started backend might drop a signal that arrives
 * before it's able to reconfigure its signal processing.  (See notes in
 * tcop/postgres.c.)
 */
static void dummy_handler(SIGNAL_ARGS)
{}

/*
 * PostmasterRandom
 */
long PostmasterRandom(void)
{
    /*
     * Select a random seed at the time of first receiving a request.
     */
    if (t_thrd.postmaster_cxt.random_seed == 0) {
        do {
            struct timeval random_stop_time;

            gettimeofday(&random_stop_time, NULL);

            /*
             * We are not sure how much precision is in tv_usec, so we swap
             * the high and low 16 bits of 'random_stop_time' and XOR them
             * with 'random_start_time'. On the off chance that the result is
             * 0, we loop until it isn't.
             */
            t_thrd.postmaster_cxt.random_seed =
                (unsigned long)t_thrd.postmaster_cxt.random_start_time.tv_usec ^
                (((unsigned long)random_stop_time.tv_usec << 16) |
                 (((unsigned long)random_stop_time.tv_usec >> 16) & 0xffff));
        } while (t_thrd.postmaster_cxt.random_seed == 0);

        gs_srandom(t_thrd.postmaster_cxt.random_seed);
    }

    return gs_random();
}

/*
 * Count up number of child processes of specified types (dead_end chidren
 * are always excluded).
 */
static int CountChildren(int target)
{
    Dlelem* curr = NULL;
    int cnt = 0;

    for (curr = DLGetHead(g_instance.backend_list); curr; curr = DLGetSucc(curr)) {
        Backend* bp = (Backend*)DLE_VAL(curr);

        /*
         * Since target == BACKEND_TYPE_ALL is the most common case, we test
         * it first and avoid touching shared memory for every child.
         */
        if (target != BACKEND_TYPE_ALL) {
            int child;

            if (bp->is_autovacuum)
                child = BACKEND_TYPE_AUTOVAC;
            else if (IsPostmasterChildWalSender(bp->child_slot))
                child = BACKEND_TYPE_WALSND;
            else if (IsPostmasterChildDataSender(bp->child_slot))
                child = BACKEND_TYPE_DATASND;
            else
                child = BACKEND_TYPE_NORMAL;

            if (!((unsigned int)target & (unsigned int)child))
                continue;
        }

        cnt++;
    }

    return cnt;
}

/*
 * StartRbCleanerWorker
 *		Start an rbcleaner worker process.
 *
 * This function is here because it enters the resulting PID into the
 * postmaster's private backends list.
 *
 * NB -- this code very roughly matches BackendStartup.
 */
static void StartRbWorker(void)
{
    Backend* bn = NULL;

    /*
     * If not in condition to run a process, don't try, but handle it like a
     * fork failure.  This does not normally happen, since the signal is only
     * supposed to be sent by autovacuum launcher when it's OK to do it, but
     * we have to check to avoid race-condition problems during DB state
     * changes.
     */
    if (canAcceptConnections(false) == CAC_OK) {
        int slot = AssignPostmasterChildSlot();
        if (slot == -1) {
            return;
        }

        bn = AssignFreeBackEnd(slot);

        if (bn != NULL) {
            /*
             * Compute the cancel key that will be assigned to this session.
             * We probably don't need cancel keys for autovac workers, but
             * we'd better have something random in the field to prevent
             * unfriendly people from sending cancels to them.
             */
            GenerateCancelKey(false);
            bn->cancel_key = t_thrd.proc_cxt.MyCancelKey;

            /* Autovac workers are not dead_end and need a child slot */
            bn->child_slot = t_thrd.proc_cxt.MyPMChildSlot = slot;
            bn->role = RBWORKER;
            bn->pid = initialize_util_thread(RBWORKER, bn);
            t_thrd.proc_cxt.MyPMChildSlot = 0;
            if (bn->pid > 0) {
                bn->is_autovacuum = false;
                DLInitElem(&bn->elem, bn);
                DLAddHead(g_instance.backend_list, &bn->elem);
                /* all OK */
                return;
            }

            /*
             * fork failed, fall through to report -- actual error message was
             * logged by initialize_util_thread
             */
            (void)ReleasePostmasterChildSlot(bn->child_slot);
            bn->pid = 0;
            bn->role = (knl_thread_role)0;
        } else {
            ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        }
    }
}


/*
 * StartSnapCapturerWorker
 *		Start an txnsnapworker process.
 *
 * This function is here because it enters the resulting PID into the
 * postmaster's private backends list.
 *
 * NB -- this code very roughly matches BackendStartup.
 */
static void StartTxnSnapWorker(void)
{
    Backend* bn = NULL;

    /*
     * If not in condition to run a process, don't try, but handle it like a
     * fork failure.  This does not normally happen, since the signal is only
     * supposed to be sent by autovacuum launcher when it's OK to do it, but
     * we have to check to avoid race-condition problems during DB state
     * changes.
     */
    if (canAcceptConnections(false) == CAC_OK) {
        int slot = AssignPostmasterChildSlot();
        if (slot == -1) {
            return;
        }

        bn = AssignFreeBackEnd(slot);

        if (bn != NULL) {
            /*
             * Compute the cancel key that will be assigned to this session.
             * We probably don't need cancel keys for autovac workers, but
             * we'd better have something random in the field to prevent
             * unfriendly people from sending cancels to them.
             */
            GenerateCancelKey(false);
            bn->cancel_key = t_thrd.proc_cxt.MyCancelKey;

            /* Autovac workers are not dead_end and need a child slot */
            bn->child_slot = t_thrd.proc_cxt.MyPMChildSlot = slot;
            bn->role = TXNSNAP_WORKER;
            bn->pid = initialize_util_thread(TXNSNAP_WORKER, bn);
            t_thrd.proc_cxt.MyPMChildSlot = 0;
            if (bn->pid > 0) {
                bn->is_autovacuum = false;
                DLInitElem(&bn->elem, bn);
                DLAddHead(g_instance.backend_list, &bn->elem);
                /* all OK */
                return;
            }

            /*
             * fork failed, fall through to report -- actual error message was
             * logged by initialize_util_thread
             */
            (void)ReleasePostmasterChildSlot(bn->child_slot);
            bn->pid = 0;
            bn->role = (knl_thread_role)0;
        } else {
            ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        }
    }
}

/*
 * StartAutovacuumWorker
 *		Start an autovac worker process.
 *
 * This function is here because it enters the resulting PID into the
 * postmaster's private backends list.
 *
 * NB -- this code very roughly matches BackendStartup.
 */
static void StartAutovacuumWorker(void)
{
    Backend* bn = NULL;

    /*
     * If not in condition to run a process, don't try, but handle it like a
     * fork failure.  This does not normally happen, since the signal is only
     * supposed to be sent by autovacuum launcher when it's OK to do it, but
     * we have to check to avoid race-condition problems during DB state
     * changes.
     */
    if (canAcceptConnections(false) == CAC_OK) {
        int slot = AssignPostmasterChildSlot();

        if (slot == -1) {
            return;
        }

        bn = AssignFreeBackEnd(slot);

        if (bn != NULL) {
            /*
             * Compute the cancel key that will be assigned to this session.
             * We probably don't need cancel keys for autovac workers, but
             * we'd better have something random in the field to prevent
             * unfriendly people from sending cancels to them.
             */
            GenerateCancelKey(false);
            bn->cancel_key = t_thrd.proc_cxt.MyCancelKey;

            /* Autovac workers are not dead_end and need a child slot */
            bn->child_slot = t_thrd.proc_cxt.MyPMChildSlot = slot;
            bn->role = AUTOVACUUM_WORKER;
            bn->pid = initialize_util_thread(AUTOVACUUM_WORKER, bn);
            t_thrd.proc_cxt.MyPMChildSlot = 0;
            if (bn->pid > 0) {
                bn->is_autovacuum = true;
                DLInitElem(&bn->elem, bn);
                DLAddHead(g_instance.backend_list, &bn->elem);
                /* all OK */
                return;
            }

            /*
             * fork failed, fall through to report -- actual error message was
             * logged by initialize_util_thread
             */
            (void)ReleasePostmasterChildSlot(bn->child_slot);
            bn->pid = 0;
            bn->role = (knl_thread_role)0;
        } else
            ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }

    /*
     * Report the failure to the launcher, if it's running.  (If it's not, we
     * might not even be connected to shared memory, so don't try to call
     * AutoVacWorkerFailed.)  Note that we also need to signal it so that it
     * responds to the condition, but we don't do that here, instead waiting
     * for ServerLoop to do it.  This way we avoid a ping-pong signalling in
     * quick succession between the autovac launcher and postmaster in case
     * things get ugly.
     */
    if (g_instance.pid_cxt.AutoVacPID != 0) {
        AutoVacWorkerFailed();
        t_thrd.postmaster_cxt.avlauncher_needs_signal = true;
    }
}

static void StartApplyWorker(void)
{
    Backend* bn = NULL;

    /*
     * If not in condition to run a process, don't try, but handle it like a
     * fork failure.  This does not normally happen, since the signal is only
     * supposed to be sent by apply launcher when it's OK to do it, but
     * we have to check to avoid race-condition problems during DB state
     * changes.
     */
    if (canAcceptConnections(false) == CAC_OK) {
        int slot = AssignPostmasterChildSlot();
        if (slot < 1) {
            ereport(ERROR, (errmsg("no free slots in PMChildFlags array")));
        }

        bn = AssignFreeBackEnd(slot);

        if (bn != NULL) {
            /*
             * Compute the cancel key that will be assigned to this session.
             * We probably don't need cancel keys for workers, but
             * we'd better have something random in the field to prevent
             * unfriendly people from sending cancels to them.
             */
            GenerateCancelKey(false);
            bn->cancel_key = t_thrd.proc_cxt.MyCancelKey;

            /* ApplyWorkers need a child slot */
            bn->child_slot = t_thrd.proc_cxt.MyPMChildSlot = slot;
            bn->pid = initialize_util_thread(APPLY_WORKER, bn);
            t_thrd.proc_cxt.MyPMChildSlot = 0;
            if (bn->pid > 0) {
                bn->is_autovacuum = false;
                DLInitElem(&bn->elem, bn);
                DLAddHead(g_instance.backend_list, &bn->elem);
                /* all OK */
                return;
            }

            /*
             * fork failed, fall through to report -- actual error message was
             * logged by initialize_util_thread
             */
            (void)ReleasePostmasterChildSlot(bn->child_slot);
            bn->pid = 0;
        } else {
            ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        }
    }
}

static void StartUndoWorker(void)
{
    Backend* bn = NULL;

    /*
     * If not in condition to run a process, don't try, but handle it like a
     * fork failure.  This does not normally happen, since the signal is only
     * supposed to be sent by undo launcher when it's OK to do it, but
     * we have to check to avoid race-condition problems during DB state
     * changes.
     */
    if (canAcceptConnections(false) == CAC_OK)
    {
        int slot = AssignPostmasterChildSlot();
        if (slot < 1) {
            ereport(ERROR, (errmsg("no free slots in PMChildFlags array")));
        }

        bn = AssignFreeBackEnd(slot);

        if (bn != NULL)
        {

            /*
             * Compute the cancel key that will be assigned to this session.
             * We probably don't need cancel keys for workers, but
             * we'd better have something random in the field to prevent
             * unfriendly people from sending cancels to them.
             */
            t_thrd.proc_cxt.MyCancelKey = PostmasterRandom();
            bn->cancel_key = t_thrd.proc_cxt.MyCancelKey;

            /* UndoWorkers need a child slot */
            bn->child_slot = t_thrd.proc_cxt.MyPMChildSlot = slot;
            bn->role = UNDO_WORKER;
            bn->pid = initialize_util_thread(UNDO_WORKER, bn);
            t_thrd.proc_cxt.MyPMChildSlot = 0;

            if (bn->pid > 0) {
                bn->is_autovacuum = true;
                DLInitElem(&bn->elem, bn);
                DLAddHead(g_instance.backend_list, &bn->elem);
                /* all OK */
                return;
            }

            /*
             * fork failed, fall through to report -- actual error message was
             * logged by initialize_util_thread
             */
            (void)ReleasePostmasterChildSlot(bn->child_slot);
            bn->pid = 0;
            bn->role = (knl_thread_role)0;
        } else
            ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }

}

/*
 * Reaper -- signal handler to cleanup after a child process dies.
 */
static void reaper_backend(SIGNAL_ARGS)
{
    int save_errno = errno;
    ThreadId pid;    /* process id of dead child process */
    long exitstatus; /* its exit status */
    int* status = NULL;
    ThreadId oldpid = 0;

#define LOOPTEST() (pid = gs_thread_id(t_thrd.postmaster_cxt.CurExitThread))
#define LOOPHEADER() (exitstatus = (long)(intptr_t)status)

    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    ereport(DEBUG4, (errmsg_internal("reaping dead processes")));

    for (;;) {
        LOOPTEST();

        if (pid == oldpid) {
            break;
        }

        oldpid = pid;
        if (gs_thread_join(t_thrd.postmaster_cxt.CurExitThread, (void**)&status) != 0) {
            /*
             * If the thread does not exist, treat it as normal exit and we continue to
             * do our clean-up work. Otherwise, we treate it as crashed 'cause we do
             * not know the current status of the thread and it's better to quit directly
             * which sames more safely.
             */
            if (ESRCH == pthread_kill(pid, 0)) {
                exitstatus = 0;
                ereport(LOG, (errmsg("failed to join thread %lu, no such process", pid)));
            } else {
                exitstatus = 1;
                HandleChildCrash(pid, exitstatus, "Backend process");
            }
        } else {
            LOOPHEADER();
            ereport(DEBUG1, (errmsg("have joined thread %lu, exitstatus=%ld.", pid, exitstatus)));
        }

        // copy from CleanupBackend(),but remove traversing g_instance.backend_list scope.
        LogChildExit(DEBUG2, _("server process"), pid, exitstatus);
#ifdef WIN32
        if (exitstatus == ERROR_WAIT_NO_CHILDREN) {
            LogChildExit(LOG, _("server process"), pid, exitstatus);
            exitstatus = 0;
        }

#endif
        if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus)) {
            HandleChildCrash(pid, exitstatus, _("server process"));
        }
    }

    /* Done with signal handler */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    errno = save_errno;
}

/*
 * @@GaussDB@@
 * Brief        : the implement of the reaper backend thread
 * Description  :
 * Notes        :
 */
void ReaperBackendMain()
{

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    /* reord my name */
    t_thrd.proc_cxt.MyProgName = "ReaperBackend";

    /* Identify myself via ps */
    init_ps_display("ReaperBackend", "", "", "");

    ereport(LOG, (errmsg("reaper backend started.")));

    InitializeLatchSupport(); /* needed for latch waits */

    /* Initialize private latch for use by signal handlers */
    InitLatch(&t_thrd.postmaster_cxt.ReaperBackendLatch);

    /*
     * Properly accept or ignore signals the postmaster might send us
     *
     * Note: we deliberately ignore SIGTERM, because during a standard Unix
     * system shutdown cycle, init will SIGTERM all processes at once.	We
     * want to wait for the backends to exit, whereupon the postmaster will
     * tell us it's okay to shut down (via SIGUSR2).
     */
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, SIG_IGN);
    (void)gspqsignal(SIGQUIT, SIG_IGN);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);

    (void)gspqsignal(SIGCHLD, reaper_backend);
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /* all is done info top memory context. */
    (void)MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));

    for (;;) {
        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.postmaster_cxt.ReaperBackendLatch);

        /*
         * Sleep until there's something to do
         */
        (void)WaitLatch(&t_thrd.postmaster_cxt.ReaperBackendLatch, WL_LATCH_SET | WL_TIMEOUT, 10 * 1000);
    }

    ereport(LOG, (errmsg("Reaper backend thread shutting down...")));

    proc_exit(0);
}

/*
 * StartPgjobWorker
 *		Start an job worker process.
 *
 * This function is here because it enters the resulting PID into the
 * postmaster's private backends list.
 *
 */
static void StartPgjobWorker(void)
{
    Backend* bn = NULL;

    /*
     * If not in condition to run a process, don't try, but handle it like a
     * fork failure.  This does not normally happen, since the signal is only
     * supposed to be sent by autovacuum launcher when it's OK to do it, but
     * we have to check to avoid race-condition problems during DB state
     * changes.
     */
    if (canAcceptConnections(false) == CAC_OK) {

        int slot = AssignPostmasterChildSlot();

        if (slot == -1) {
            return;
        }
        bn = AssignFreeBackEnd(slot);
        if (bn != NULL) {
            /*
             * Compute the cancel key that will be assigned to this session.
             * We probably don't need cancel keys for autovac workers, but
             * we'd better have something random in the field to prevent
             * unfriendly people from sending cancels to them.
             */
            GenerateCancelKey(false);
            bn->cancel_key = t_thrd.proc_cxt.MyCancelKey;
            bn->child_slot = t_thrd.proc_cxt.MyPMChildSlot = slot;
            bn->role = JOB_WORKER;
            bn->pid = initialize_util_thread(JOB_WORKER);
            t_thrd.proc_cxt.MyPMChildSlot = 0;
            if (bn->pid > 0) {
                bn->is_autovacuum = false;
                DLInitElem(&bn->elem, bn);
                DLAddHead(g_instance.backend_list, &bn->elem);
                /* all OK */
                return;
            }

            /*
             * fork failed, fall through to report -- actual error message was
             * logged by initialize_util_thread
             */
            (void)ReleasePostmasterChildSlot(bn->child_slot);
            bn->pid = 0;
            bn->role = (knl_thread_role)0;
            bn = NULL;
        } else
            ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }

    /*
     * Report the failure to the launcher, if it's running.  (If it's not, we
     * might not even be connected to shared memory, so don't try to call
     * RecordForkJobWorkerFailed.)  Note that we also need to signal it so that it
     * responds to the condition, but we don't do that here, instead waiting
     * for ServerLoop to do it.  This way we avoid a ping-pong signalling in
     * quick succession between the job scheduler and postmaster in case
     * things get ugly.
     */
    if (g_instance.pid_cxt.PgJobSchdPID != 0) {
        RecordForkJobWorkerFailed();
        t_thrd.postmaster_cxt.jobscheduler_needs_signal = true;
    }
}

static void StartPoolCleaner(void)
{
    Backend* bn = NULL;

    /*
     * If not in condition to run a process, don't try, but handle it like a
     * fork failure.  This does not normally happen, since the signal is only
     * supposed to be sent by autovacuum launcher when it's OK to do it, but
     * we have to check to avoid race-condition problems during DB state
     * changes.
     */
    if (canAcceptConnections(false) == CAC_OK) {

        int slot = AssignPostmasterChildSlot();
        if (slot == -1) {
            return;
        }
        bn = AssignFreeBackEnd(slot);
        if (bn != NULL) {
            /*
             * Compute the cancel key that will be assigned to this session.
             * We probably don't need cancel keys for autovac workers, but
             * we'd better have something random in the field to prevent
             * unfriendly people from sending cancels to them.
             */
            GenerateCancelKey(false);
            bn->cancel_key = t_thrd.proc_cxt.MyCancelKey;
            bn->child_slot = t_thrd.proc_cxt.MyPMChildSlot = slot;
            bn->role = COMM_POOLER_CLEAN;
            bn->pid = initialize_util_thread(COMM_POOLER_CLEAN);
            g_instance.pid_cxt.CommPoolerCleanPID = bn->pid;
            t_thrd.proc_cxt.MyPMChildSlot = 0;
            if (bn->pid > 0) {
                bn->is_autovacuum = false;
                DLInitElem(&bn->elem, bn);
                DLAddHead(g_instance.backend_list, &bn->elem);
                /* all OK */
                return;
            }

            /*
             * fork failed, fall through to report -- actual error message was
             * logged by initialize_util_thread
             */
            (void)ReleasePostmasterChildSlot(bn->child_slot);
            bn->pid = 0;
            bn->role = (knl_thread_role)0;
            bn = NULL;
        } else
            ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }
}

static void StartCleanStatement(void)
{
    Backend* bn = NULL;

    /*
     * If not in condition to run a process, don't try, but handle it like a
     * fork failure.  This does not normally happen, since the signal is only
     * supposed to be sent by autovacuum launcher when it's OK to do it, but
     * we have to check to avoid race-condition problems during DB state
     * changes.
     */
    if (canAcceptConnections(false) == CAC_OK) {
        int slot = AssignPostmasterChildSlot();

        if (slot == -1) {
            return;
        }

        bn = AssignFreeBackEnd(slot);

        if (bn != NULL) {
            GenerateCancelKey(false);
            bn->cancel_key = t_thrd.proc_cxt.MyCancelKey;

            /* Autovac workers are not dead_end and need a child slot */
            bn->child_slot = t_thrd.proc_cxt.MyPMChildSlot = slot;
            bn->role = TRACK_STMT_CLEANER;
            bn->pid = initialize_util_thread(TRACK_STMT_CLEANER, bn);
            t_thrd.proc_cxt.MyPMChildSlot = 0;
            if (bn->pid > 0) {
                bn->is_autovacuum = false;
                DLInitElem(&bn->elem, bn);
                DLAddHead(g_instance.backend_list, &bn->elem);
                /* all OK */
                return;
            }

            (void)ReleasePostmasterChildSlot(bn->child_slot);
            bn->pid = 0;
            bn->role = (knl_thread_role)0;
        } else {
            ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        }
    }
}

/*
 * Create the opts file
 */
static bool CreateOptsFile(int argc, const char* argv[], const char* fullprogname)
{
    FILE* fp = NULL;
    int i;

#define OPTS_FILE "postmaster.opts"

    if ((fp = fopen(OPTS_FILE, "w")) == NULL) {
        ereport(LOG, (errmsg("could not create file \"%s\": %m", OPTS_FILE)));
        return false;
    }

    fprintf(fp, "%s", fullprogname);

    for (i = 1; i < argc; i++)
        fprintf(fp, " \"%s\"", argv[i]);

    fputs("\n", fp);

    if (fclose(fp)) {
        ereport(LOG, (errmsg("could not close file \"%s\": %m", OPTS_FILE)));
        return false;
    }

    return true;
}

/* Update the opts file */
static void UpdateOptsFile(void)
{
    FILE* fp = NULL;
    char* dest = NULL;
    char buffer[MAXPGPATH] = {0};
    const char* modeopt = " \"-M\" \"";
    const char* modestr = NULL;

#define OPTS_FILE "postmaster.opts"
#define OPTS_TEMP_FILE "postmaster.opts.tmp"

    if (NULL == t_thrd.postmaster_cxt.HaShmData)
        return;

    switch (t_thrd.postmaster_cxt.HaShmData->current_mode) {
        case PRIMARY_MODE:
            modestr = "primary";
            break;
        case STANDBY_MODE:
            modestr = "standby";
            break;
        default:
            return;
    }

    if ((fp = fopen(OPTS_FILE, "r")) == NULL) {
        ereport(LOG, (errmsg("could not open file \"%s\": %m", OPTS_FILE)));
        return;
    }

    (void)fgets(buffer, MAXPGPATH, fp);
    if (fclose(fp)) {
        ereport(LOG, (errmsg("could not close file \"%s\": %m", OPTS_FILE)));
        return;
    }

    if ((fp = fopen(OPTS_TEMP_FILE, "w")) == NULL) {
        ereport(LOG, (errmsg("could not create file \"%s\": %m", OPTS_TEMP_FILE)));
        return;
    }

    if ((dest = strchr(buffer, '\n')) != NULL)
        *dest = 0;

    dest = strstr(buffer, modeopt);
    if (dest != NULL) {
        dest += strlen(modeopt);
        *dest = 0;
        (void)fprintf(fp, "%s%s\"", buffer, modestr);

        dest++;
        dest = strstr(dest, "\" \"");
        if (dest != NULL)
            (void)fprintf(fp, "%s\n", ++dest);
        else
            (void)fputs("\n", fp);
    } else {
        (void)fprintf(fp, "%s%s%s\"\n", buffer, modeopt, modestr);
    }

    if (fclose(fp)) {
        ereport(LOG, (errmsg("could not close file \"%s\": %m", OPTS_TEMP_FILE)));
        return;
    }

    (void)unlink(OPTS_FILE);
    if (rename(OPTS_TEMP_FILE, OPTS_FILE) != 0)
        ereport(LOG,
            (errcode_for_file_access(),
                errmsg("could not rename file \"%s\" to \"%s\": %m", OPTS_TEMP_FILE, OPTS_FILE)));
}

/*
 * StartCatchupWorker
 *		Start catch-up process.
 *
 * This function is here because it enters the resulting PID into the
 * postmaster's private backends list.
 *
 * NB -- this code very roughly matches BackendStartup.
 */
static ThreadId StartCatchupWorker(void)
{
    Backend* bn = NULL;

    /*
     * If not in condition to run a process, don't try, but handle it like a
     * fork failure.  This does not normally happen, since the signal is only
     * supposed to be sent by autovacuum launcher when it's OK to do it, but
     * we have to check to avoid race-condition problems during DB state
     * changes.
     */
    if (canAcceptConnections(false) == CAC_OK) {

        int slot = AssignPostmasterChildSlot();
        if (slot == -1) {
            return 0;
        }
        bn = AssignFreeBackEnd(slot);
        if (bn != NULL) {
            /*
             * Compute the cancel key that will be assigned to this session.
             * We probably don't need cancel keys for autovac workers, but
             * we'd better have something random in the field to prevent
             * unfriendly people from sending cancels to them.
             */
            GenerateCancelKey(false);
            bn->cancel_key = t_thrd.proc_cxt.MyCancelKey;

            /* Data catch-up are not dead_end and need a child slot */
            bn->child_slot = t_thrd.proc_cxt.MyPMChildSlot = slot;
            bn->role = CATCHUP;
            bn->pid = initialize_util_thread(CATCHUP);
            t_thrd.proc_cxt.MyPMChildSlot = 0;
            if (bn->pid > 0) {
                bn->is_autovacuum = false;
                DLInitElem(&bn->elem, bn);
                DLAddHead(g_instance.backend_list, &bn->elem);
                /* all OK */
                return bn->pid;
            }

            /*
             * fork failed, fall through to report -- actual error message was
             * logged by StartDataCatchup
             */
            (void)ReleasePostmasterChildSlot(bn->child_slot);
            bn->pid = 0;
            bn->role = (knl_thread_role)0;
            bn = NULL;
        } else
            ereport(LOG, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }
    return 0;
}

/*
 * MaxLivePostmasterChildren
 *
 * This reports the number of entries needed in per-child-process arrays
 * (the PMChildFlags array, and if EXEC_BACKEND the ShmemBackendArray).
 * These arrays include regular backends, autovac workers and walsenders,
 * but not special children nor dead_end children.	This allows the arrays
 * to have a fixed maximum size, to wit the same too-many-children limit
 * enforced by canAcceptConnections().	The exact value isn't too critical
 * as long as it's more than g_instance.shmem_cxt.MaxBackends.
 */
int MaxLivePostmasterChildren(void)
{
    return 6 * g_instance.shmem_cxt.MaxBackends;
}

#ifdef EXEC_BACKEND
#ifndef WIN32
#define write_inheritable_socket(dest, src) ((*(dest) = (src)))
#define read_inheritable_socket(dest, src) (*(dest) = *(src))
#else
static bool write_duplicated_handle(HANDLE* dest, HANDLE src, HANDLE child);
static bool write_inheritable_socket(InheritableSocket* dest, SOCKET src, pid_t childPid);
static void read_inheritable_socket(SOCKET* dest, InheritableSocket* src);
#endif

/* Save critical backend variables into the BackendParameters struct */
#ifndef WIN32
static bool save_backend_variables(BackendParameters* param, Port* port)
#else
static bool save_backend_variables(BackendParameters* param, Port* port, HANDLE childProcess, pid_t childPid)
#endif
{
    int ss_rc = memcpy_s(&param->port, sizeof(param->port), port, sizeof(Port));
    securec_check(ss_rc, "\0", "\0");
    write_inheritable_socket(&param->portsocket, port->sock);

    strlcpy(param->DataDir, t_thrd.proc_cxt.DataDir, MAXPGPATH);

    ss_rc = memcpy_s(&param->ListenSocket,
        sizeof(param->ListenSocket),
        &t_thrd.postmaster_cxt.ListenSocket,
        sizeof(t_thrd.postmaster_cxt.ListenSocket));
    securec_check(ss_rc, "\0", "\0");

    param->MyCancelKey = t_thrd.proc_cxt.MyCancelKey;
    param->MyPMChildSlot = t_thrd.proc_cxt.MyPMChildSlot;

    param->UsedShmemSegID = UsedShmemSegID;
    param->UsedShmemSegAddr = UsedShmemSegAddr;

    param->ShmemLock = t_thrd.shemem_ptr_cxt.ShmemLock;
    param->ShmemVariableCache = t_thrd.xact_cxt.ShmemVariableCache;

    param->mainLWLockArray = (LWLock*)t_thrd.shemem_ptr_cxt.mainLWLockArray;
    param->PMSignalState = t_thrd.shemem_ptr_cxt.PMSignalState;

    param->LocalIpNum = t_thrd.postmaster_cxt.LocalIpNum;
    int rc = memcpy_s(param->LocalAddrList, (MAXLISTEN * IP_LEN), t_thrd.postmaster_cxt.LocalAddrList, (MAXLISTEN * IP_LEN));
    securec_check(rc, "", "");
    param->HaShmData = t_thrd.postmaster_cxt.HaShmData;

    param->PgStartTime = t_thrd.time_cxt.pg_start_time;
    param->PgReloadTime = t_thrd.time_cxt.pg_reload_time;
    param->first_syslogger_file_time = t_thrd.logger.first_syslogger_file_time;

    param->redirection_done = t_thrd.postmaster_cxt.redirection_done;
    param->IsBinaryUpgrade = u_sess->proc_cxt.IsBinaryUpgrade;
    param->max_safe_fds = t_thrd.storage_cxt.max_safe_fds;
    param->max_files_per_process = g_instance.attr.attr_common.max_files_per_process;
    param->max_userdatafiles = t_thrd.storage_cxt.max_userdatafiles;

#ifdef WIN32
    param->PostmasterHandle = PostmasterHandle;

    if (!write_duplicated_handle(&param->initial_signal_pipe, pgwin32_create_signal_listener(childPid), childProcess))
        return false;

#else
    ss_rc = memcpy_s(&param->postmaster_alive_fds,
        sizeof(t_thrd.postmaster_cxt.postmaster_alive_fds),
        &t_thrd.postmaster_cxt.postmaster_alive_fds,
        sizeof(t_thrd.postmaster_cxt.postmaster_alive_fds));
    securec_check(ss_rc, "\0", "\0");
#endif

    ss_rc = memcpy_s(&param->syslogPipe,
        sizeof(param->syslogPipe),
        &t_thrd.postmaster_cxt.syslogPipe,
        sizeof(t_thrd.postmaster_cxt.syslogPipe));
    securec_check(ss_rc, "\0", "\0");
    strlcpy(param->my_exec_path, my_exec_path, MAXPGPATH);

    strlcpy(param->pkglib_path, t_thrd.proc_cxt.pkglib_path, MAXPGPATH);

    param->myTempNamespace = u_sess->catalog_cxt.myTempNamespace;
    param->myTempToastNamespace = u_sess->catalog_cxt.myTempToastNamespace;
    if (module_logging_is_on(MOD_COMM_IPC) && (t_thrd.proc && t_thrd.proc->workingVersionNum >= 92060)) {
        param->comm_ipc_log = true;
    } else {
        param->comm_ipc_log = false;
    }

    return true;
}

#ifdef WIN32
/*
 * Duplicate a handle for usage in a child process, and write the child
 * process instance of the handle to the parameter file.
 */
static bool write_duplicated_handle(HANDLE* dest, HANDLE src, HANDLE childProcess)
{
    HANDLE hChild = INVALID_HANDLE_VALUE;

    if (!DuplicateHandle(
            GetCurrentProcess(), src, childProcess, &hChild, 0, TRUE, DUPLICATE_CLOSE_SOURCE | DUPLICATE_SAME_ACCESS)) {
        ereport(LOG,
            (errmsg_internal(
                "could not duplicate handle to be written to backend parameter file: error code %lu", GetLastError())));
        return false;
    }

    *dest = hChild;
    return true;
}

/*
 * Duplicate a socket for usage in a child process, and write the resulting
 * structure to the parameter file.
 * This is required because a number of LSPs (Layered Service Providers) very
 * common on Windows (antivirus, firewalls, download managers etc) break
 * straight socket inheritance.
 */
static bool write_inheritable_socket(InheritableSocket* dest, SOCKET src, pid_t childpid)
{
    dest->origsocket = src;

    if (src != 0 && src != PGINVALID_SOCKET) {
        /* Actual socket */
        if (WSADuplicateSocket(src, childpid, &dest->wsainfo) != 0) {
            ereport(LOG,
                (errmsg(
                    "could not duplicate socket %d for use in backend: error code %d", (int)src, WSAGetLastError())));
            return false;
        }
    }

    return true;
}

/*
 * Read a duplicate socket structure back, and get the socket descriptor.
 */
static void read_inheritable_socket(SOCKET* dest, InheritableSocket* src)
{
    SOCKET s;

    if (src->origsocket == PGINVALID_SOCKET || src->origsocket == 0) {
        /* Not a real socket! */
        *dest = src->origsocket;
    } else {
        /* Actual socket, so create from structure */
        s = WSASocket(FROM_PROTOCOL_INFO, FROM_PROTOCOL_INFO, FROM_PROTOCOL_INFO, &src->wsainfo, 0, 0);

        if (s == INVALID_SOCKET) {
            write_stderr("could not create inherited socket: error code %d\n", WSAGetLastError());
            exit(1);
        }

        *dest = s;

        /*
         * To make sure we don't get two references to the same socket, close
         * the original one. (This would happen when inheritance actually
         * works..
         */
        comm_closesocket(src->origsocket); /* CommProxy Support */
    }
}
#endif

static void read_backend_variables(char* arg, Port* port)
{
    BackendParameters* pParam = (BackendParameters*)arg;

    restore_backend_variables(pParam, port);
}

/* Restore critical backend variables from the BackendParameters struct */
static void restore_backend_variables(BackendParameters* param, Port* port)
{
    int rc = memcpy_s(port, sizeof(Port), &param->port, sizeof(Port));
    securec_check(rc, "", "");

    read_inheritable_socket(&port->sock, &param->portsocket);

    SetDataDir(param->DataDir);

    int ss_rc = memcpy_s(&t_thrd.postmaster_cxt.ListenSocket,
        sizeof(t_thrd.postmaster_cxt.ListenSocket),
        &param->ListenSocket,
        sizeof(t_thrd.postmaster_cxt.ListenSocket));
    securec_check(ss_rc, "\0", "\0");

    t_thrd.proc_cxt.MyCancelKey = param->MyCancelKey;
    t_thrd.proc_cxt.MyPMChildSlot = param->MyPMChildSlot;

    UsedShmemSegID = param->UsedShmemSegID;
    UsedShmemSegAddr = param->UsedShmemSegAddr;

    t_thrd.shemem_ptr_cxt.ShmemLock = param->ShmemLock;
    t_thrd.xact_cxt.ShmemVariableCache = param->ShmemVariableCache;

    t_thrd.shemem_ptr_cxt.mainLWLockArray = (LWLockPadded*)param->mainLWLockArray;
    t_thrd.shemem_ptr_cxt.PMSignalState = param->PMSignalState;

    t_thrd.postmaster_cxt.LocalIpNum = param->LocalIpNum;
    rc = memcpy_s(t_thrd.postmaster_cxt.LocalAddrList, (MAXLISTEN * IP_LEN), param->LocalAddrList, (MAXLISTEN * IP_LEN));
    securec_check(rc, "", "");
    t_thrd.postmaster_cxt.HaShmData = param->HaShmData;
    t_thrd.time_cxt.pg_start_time = param->PgStartTime;
    t_thrd.time_cxt.pg_reload_time = param->PgReloadTime;
    t_thrd.logger.first_syslogger_file_time = param->first_syslogger_file_time;

    t_thrd.postmaster_cxt.redirection_done = param->redirection_done;
    u_sess->proc_cxt.IsBinaryUpgrade = param->IsBinaryUpgrade;
    t_thrd.storage_cxt.max_safe_fds = param->max_safe_fds;
    g_instance.attr.attr_common.max_files_per_process = param->max_files_per_process;
    t_thrd.storage_cxt.max_userdatafiles = param->max_userdatafiles;

#ifdef WIN32
    PostmasterHandle = param->PostmasterHandle;
    pgwin32_initial_signal_pipe = param->initial_signal_pipe;
#endif
    ss_rc = memcpy_s(&t_thrd.postmaster_cxt.syslogPipe,
        sizeof(t_thrd.postmaster_cxt.syslogPipe),
        &param->syslogPipe,
        sizeof(t_thrd.postmaster_cxt.syslogPipe));
    securec_check(ss_rc, "\0", "\0");
    strlcpy(my_exec_path, param->my_exec_path, MAXPGPATH);

    strlcpy(t_thrd.proc_cxt.pkglib_path, param->pkglib_path, MAXPGPATH);

    if (StreamThreadAmI()) {
        u_sess->catalog_cxt.myTempNamespace = param->myTempNamespace;
        u_sess->catalog_cxt.myTempToastNamespace = param->myTempToastNamespace;
    }

    if (param->comm_ipc_log == true) {
        module_logging_enable_comm(MOD_COMM_IPC);
    }
}


#endif /* EXEC_BACKEND */

static void BackendArrayAllocation(void)
{
    // should be the same size as PostmasterChildSlot.
    Size size = mul_size(MaxLivePostmasterChildren(), sizeof(Backend));
    g_instance.backend_array = (Backend*)palloc0(size);
}

Backend* GetBackend(int slot)
{
    if (slot > 0 && slot <= MaxLivePostmasterChildren()) {
#ifdef USE_ASSERT_CHECKING
        if (g_instance.backend_array[slot - 1].role != t_thrd.role &&
            t_thrd.role != STREAM_WORKER && t_thrd.role != THREADPOOL_STREAM) {
            elog(WARNING, "get thread backend role %d not match current thread rolr %d ",
                          g_instance.backend_array[slot - 1].role, t_thrd.role);
        }
#endif
        return &g_instance.backend_array[slot - 1];
    } else {
        return NULL;
    }
}

Backend* AssignFreeBackEnd(int slot)
{
    Assert(g_instance.backend_array[slot - 1].pid == 0);
    Backend* bn = &g_instance.backend_array[slot - 1];

    bn->flag = 0;
    bn->pid = 0;
    bn->role = t_thrd.role;
    bn->cancel_key = 0;
    bn->dead_end = false;
    return bn;
}

static void BackendArrayRemove(Backend* bn)
{
    int i = bn->child_slot - 1;

    Assert(g_instance.backend_array[i].pid == bn->pid);
    /* Mark the slot as empty */
    g_instance.backend_array[i].pid = 0;
    g_instance.backend_array[i].role = (knl_thread_role)0;
    g_instance.backend_array[i].flag = 0;
    g_instance.backend_array[i].cancel_key = 0;
    g_instance.backend_array[i].dead_end = false;
}

#ifdef WIN32
static ThreadId win32_waitpid(int* exitstatus)
{
    DWORD dwd;
    ULONG_PTR key;
    OVERLAPPED* ovl = NULL;

    /*
     * Check if there are any dead children. If there are, return the pid of
     * the first one that died.
     */
    if (GetQueuedCompletionStatus(win32ChildQueue, &dwd, &key, &ovl, 0)) {
        *exitstatus = (int)key;
        return dwd;
    }

    return -1;
}

#endif /* WIN32 */

/*
 * Initialize one and only handle for monitoring postmaster death.
 *
 * Called once in the postmaster, so that child processes can subsequently
 * monitor if their parent is dead.
 */
static void InitPostmasterDeathWatchHandle(void)
{
#ifndef WIN32

    /*
     * Create a pipe. Postmaster holds the write end of the pipe open
     * (POSTMASTER_FD_OWN), and children hold the read end. Children can pass
     * the read file descriptor to select() to wake up in case postmaster
     * dies, or check for postmaster death with a (read() == 0). Children must
     * close the write end as soon as possible after forking, because EOF
     * won't be signaled in the read end until all processes have closed the
     * write fd. That is taken care of in ClosePostmasterPorts().
     */
    Assert(t_thrd.proc_cxt.MyProcPid == PostmasterPid);

    if (pipe(t_thrd.postmaster_cxt.postmaster_alive_fds))
        ereport(FATAL,
            (errcode_for_file_access(), errmsg_internal("could not create pipe to monitor postmaster death: %m")));

    /*
     * Set O_NONBLOCK to allow testing for the fd's presence with a read()
     * call.
     */
    if (comm_fcntl(t_thrd.postmaster_cxt.postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFL, O_NONBLOCK))
        ereport(FATAL,
            (errcode_for_socket_access(),
                errmsg_internal("could not set postmaster death monitoring pipe to non-blocking mode: %m")));

#else

    /*
     * On Windows, we use a process handle for the same purpose.
     */
    if (DuplicateHandle(GetCurrentProcess(),
            GetCurrentProcess(),
            GetCurrentProcess(),
            &PostmasterHandle,
            0,
            TRUE,
            DUPLICATE_SAME_ACCESS) == 0)
        ereport(FATAL, (errmsg_internal("could not duplicate postmaster handle: error code %lu", GetLastError())));

#endif /* WIN32 */
}

Size CBMShmemSize(void)
{
    Size size = 0;

    size = add_size(size, sizeof(XlogBitmap));
    return size;
}

void CBMShmemInit(void)
{
    bool found = false;

    t_thrd.cbm_cxt.XlogCbmSys = (XlogBitmap*)ShmemInitStruct("CBM Shmem Data", CBMShmemSize(), &found);

    if (!found)
        InitXlogCbmSys();
}

Size HaShmemSize(void)
{
    Size size = 0;

    size = add_size(size, sizeof(HaShmemData));
    return size;
}

void HaShmemInit(void)
{
    bool found = false;

    t_thrd.postmaster_cxt.HaShmData = (HaShmemData*)ShmemInitStruct("HA Shmem Data ", HaShmemSize(), &found);

    if (!found) {
        int i = 0;

        t_thrd.postmaster_cxt.HaShmData->current_mode = NORMAL_MODE;
        g_instance.global_sysdbcache.RefreshHotStandby();
        for (i = 1; i < DOUBLE_MAX_REPLNODE_NUM; i++) {
            t_thrd.postmaster_cxt.HaShmData->disconnect_count[i] = 0;
            t_thrd.postmaster_cxt.HaShmData->repl_reason[i] = NONE_REBUILD;
        }
        SpinLockInit(&t_thrd.postmaster_cxt.HaShmData->mutex);
        t_thrd.postmaster_cxt.HaShmData->current_repl = 1;
        t_thrd.postmaster_cxt.HaShmData->prev_repl = 0;
        t_thrd.postmaster_cxt.HaShmData->is_cascade_standby = false;
        t_thrd.postmaster_cxt.HaShmData->is_cross_region = false;
        t_thrd.postmaster_cxt.HaShmData->is_hadr_main_standby = false;
    }
    if (!IsUnderPostmaster) {
        gs_set_hs_shm_data(t_thrd.postmaster_cxt.HaShmData);
    }
}

/* Check whether the connect info of the port is equal to the replconninfo */
static bool IsChannelAdapt(Port* port, ReplConnInfo* repl)
{
    struct sockaddr* laddr = (struct sockaddr*)&(port->laddr.addr);
    struct sockaddr* raddr = (struct sockaddr*)&(port->raddr.addr);
    char local_ip[IP_LEN] = {0};
    char remote_ip[IP_LEN] = {0};
    char* result = NULL;
    char* local_ipNoZone = NULL;
    char* remote_ipNoZone = NULL;
    char local_ipNoZoneData[IP_LEN] = {0};
    char remote_ipNoZoneData[IP_LEN] = {0};

    Assert(repl != NULL);

    if (AF_INET6 == laddr->sa_family) {
        result = inet_net_ntop(AF_INET6, &((struct sockaddr_in6*)laddr)->sin6_addr, 128, local_ip, IP_LEN);
        if (NULL == result) {
            ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
        }
    } else if (AF_INET == laddr->sa_family) {
        result = inet_net_ntop(AF_INET, &((struct sockaddr_in*)laddr)->sin_addr, 32, local_ip, IP_LEN);
        if (NULL == result) {
            ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
        }
    }

    if (AF_INET6 == raddr->sa_family) {
        result = inet_net_ntop(AF_INET6, &((struct sockaddr_in6*)raddr)->sin6_addr, 128, remote_ip, IP_LEN);
        if (NULL == result) {
            ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
        }
    } else if (AF_INET == raddr->sa_family) {
        result = inet_net_ntop(AF_INET, &((struct sockaddr_in*)raddr)->sin_addr, 32, remote_ip, IP_LEN);
        if (NULL == result) {
            ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
        }
    }

    /* remove any '%zone' part from an IPv6 address string */
    local_ipNoZone = remove_ipv6_zone(repl->localhost, local_ipNoZoneData, IP_LEN);
    remote_ipNoZone = remove_ipv6_zone(repl->remotehost, remote_ipNoZoneData, IP_LEN);

    if (0 == strcmp(local_ip, local_ipNoZone) && 0 == strcmp(remote_ip, remote_ipNoZone)) {
        return true;
    } else {
        ereport(DEBUG1, (errmsg("connect local ip %s, connect remote ip %s", local_ip, remote_ip)));
        return false;
    }
}

/*
 * check whether the login connection is from local machine.
 */
bool IsFromLocalAddr(Port* port)
{
    struct sockaddr* local_addr = (struct sockaddr*)&(port->laddr.addr);
    struct sockaddr* remote_addr = (struct sockaddr*)&(port->raddr.addr);
    char local_ip[IP_LEN] = {0};
    char remote_ip[IP_LEN] = {0};
    char* result = NULL;

    /* parse the local ip address */
    if (AF_INET6 == local_addr->sa_family) {
        result = inet_net_ntop(AF_INET6, &((struct sockaddr_in6*)local_addr)->sin6_addr, 128, local_ip, IP_LEN);
    } else if (AF_INET == local_addr->sa_family) {
        result = inet_net_ntop(AF_INET, &((struct sockaddr_in*)local_addr)->sin_addr, 32, local_ip, IP_LEN);
    }

    if (result == NULL) {
        ereport(WARNING, (errmsg("inet_net_ntop local failed, error: %d", EAFNOSUPPORT)));
    }

    /* parse the remote ip address */
    if (AF_INET6 == remote_addr->sa_family) {
        result = inet_net_ntop(AF_INET6, &((struct sockaddr_in6*)remote_addr)->sin6_addr, 128, remote_ip, IP_LEN);
    } else if (AF_INET == remote_addr->sa_family) {
        result = inet_net_ntop(AF_INET, &((struct sockaddr_in*)remote_addr)->sin_addr, 32, remote_ip, IP_LEN);
    }

    if (result == NULL) {
        ereport(WARNING, (errmsg("inet_net_ntop remote failed, error: %d", EAFNOSUPPORT)));
    }

    /* only the remote ip equal to local ip, return true */
    if (strlen(local_ip) != 0 && strlen(remote_ip) != 0 && strcmp(local_ip, remote_ip) == 0)
        return true;
    else
        return false;
}

/*
 * check whether the localaddr of the ip is in the LocalAddrList
 */
bool IsLocalAddr(Port* port)
{
    int i;
#ifndef WIN32
    struct sockaddr* laddr = (struct sockaddr*)&(port->laddr.addr);
    char local_ip[IP_LEN] = {0};
    char* result = NULL;

    if (AF_INET6 == laddr->sa_family) {
        result = inet_net_ntop(AF_INET6, &((struct sockaddr_in6*)laddr)->sin6_addr, 128, local_ip, IP_LEN);
    } else if (AF_INET == laddr->sa_family) {
        result = inet_net_ntop(AF_INET, &((struct sockaddr_in*)laddr)->sin_addr, 32, local_ip, IP_LEN);
    }

    if (AF_UNIX == laddr->sa_family) {
        return true;
    }
    for (i = 0; i != t_thrd.postmaster_cxt.LocalIpNum; ++i) {
        if (0 == strcmp(local_ip, t_thrd.postmaster_cxt.LocalAddrList[i]) ||
            (AF_INET == laddr->sa_family && 0 == strcmp("0.0.0.0", t_thrd.postmaster_cxt.LocalAddrList[i])) ||
            (AF_INET6 == laddr->sa_family && 0 == strcmp("::", t_thrd.postmaster_cxt.LocalAddrList[i]))) {

            return true;
        }
    }
    if (NULL == result && laddr->sa_family != AF_UNSPEC) {
        ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
    }
    return false;
#else
    for (i = 0; i != t_thrd.postmaster_cxt.LocalIpNum; ++i) {
        ereport(DEBUG1, (errmsg("LocalAddrIP %s\n", t_thrd.postmaster_cxt.LocalAddrList[i])));
    }
    return true;
#endif
}

/*
 * check whether the input address string is equivalent to local host
 */
static bool IsLocalIp(const char* address)
{
    return (
        strcmp(address, LOCAL_HOST) == 0 || strcmp(address, LOOP_IP_STRING) == 0 || strcmp(address, LOOP_IPV6_IP) == 0);
}

/*
 * check whether the input address string is inplicit, including local host and *
 */
static bool IsInplicitIp(const char* address)
{
    return (IsLocalIp(address) || strcmp(address, "*") == 0);
}

/*
 * if certain replconn channel is equivalent to pooler port,
 * we use that channel for internal tool connections for dn.
 * return value: equivalent replconn array index, i.e. no need
 * for pooler port, or -1 if none equivalent, i.e. pooler port needed.
 * At present, this is determined only at postmaster startup.
 * If we are to support dynamic modification of replconninfo,
 * we would also need to do the judge dynamically.
 */
static int NeedPoolerPort(const char* hostName)
{
    int index = -1;

    for (int i = 1; i < DOUBLE_MAX_REPLNODE_NUM; i++) {
        ReplConnInfo *replConnInfo = NULL;
        if (i >= MAX_REPLNODE_NUM) {
            replConnInfo = t_thrd.postmaster_cxt.CrossClusterReplConnArray[i - MAX_REPLNODE_NUM];
        } else {
            replConnInfo = t_thrd.postmaster_cxt.ReplConnArray[i];
        }
        if (replConnInfo != NULL &&
            replConnInfo->localport == g_instance.attr.attr_network.PoolerPort &&
            (strcmp(replConnInfo->localhost, "*") == 0 ||
            strcmp(replConnInfo->localhost, hostName) == 0)) {
            index = i;
            break;
        }
    }
    return index;
}

/*
 * check whether the port of the socket address is the compare_port
 */
bool IsMatchSocketAddr(const struct sockaddr* sock_addr, int target_port)
{
    int portNumber = 0;
    struct sockaddr_in* ipv4addr = NULL;
    struct sockaddr_in6* ipv6addr = NULL;
#ifdef HAVE_UNIX_SOCKETS
    struct sockaddr_un* unixaddr = NULL;
    char unixStr[MAX_UNIX_PATH_LEN] = {'\0'};
#endif
    bool result = false;

    if (sock_addr == NULL) {
        return false;
    }

    switch (sock_addr->sa_family) {
        case AF_INET:
            ipv4addr = (struct sockaddr_in*)sock_addr;
            portNumber = ntohs(ipv4addr->sin_port);
            result = (portNumber == target_port);
            break;

        case AF_INET6:
            ipv6addr = (struct sockaddr_in6*)sock_addr;
            portNumber = ntohs(ipv6addr->sin6_port);
            result = (portNumber == target_port);
            break;

#ifdef HAVE_UNIX_SOCKETS
        case AF_UNIX:
            unixaddr = (struct sockaddr_un*)sock_addr;
            UNIXSOCK_PATH(unixStr, target_port, g_instance.attr.attr_network.UnixSocketDir);

            if (0 == (strncmp(unixaddr->sun_path, unixStr, MAX_UNIX_PATH_LEN))) {
                result = true;
            }
            break;
#endif

        default:
            break;
    }

    return result;
}

/*
 * check whether the port of the session Port structure is the basePort
 */
bool IsConnectBasePort(const Port* port)
{
    if (port == NULL) {
        Assert(port);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("IsConnectBasePort: invalid params detail port[null]")));
    }
    struct sockaddr* laddr = (struct sockaddr*)&(port->laddr.addr);
    return IsMatchSocketAddr(laddr, g_instance.attr.attr_network.PostPortNumber);
}

static bool IsClusterHaPort(const struct sockaddr* laddr)
{
    for (int i = 1; i < MAX_REPLNODE_NUM; i++) {
        ReplConnInfo *replConnInfo = t_thrd.postmaster_cxt.CrossClusterReplConnArray[i];

        if (replConnInfo != NULL) {
            bool matched = IsMatchSocketAddr(laddr, replConnInfo->localport);
            if (matched) {
                return true;
            }
        }
    }
    return false;
}

/*
 * check whether the port of the session Port structure is the PoolerPort
 */
bool IsHAPort(Port* port)
{
    if (port == NULL) {
        return false;
    }

    struct sockaddr* laddr = (struct sockaddr*)&(port->laddr.addr);

    if (IsMatchSocketAddr(laddr, g_instance.attr.attr_network.PoolerPort)) {
        return true;
    }

    if (IsClusterHaPort(laddr)) {
        return true;
    }

    for (int i = 1; i < DOUBLE_MAX_REPLNODE_NUM + 1; i++) {
        ReplConnInfo *replConnInfo = t_thrd.postmaster_cxt.ReplConnArray[i];
        if (replConnInfo != NULL) {
            bool matched = IsMatchSocketAddr(laddr, replConnInfo->localport);
            if (matched) {
                return true;
            }
        }
    }
    
    return false;
}

/*
 * check whether the localaddr of the port is the PostPortNumber
 */
static bool IsLocalPort(Port* port)
{
    struct sockaddr* laddr = (struct sockaddr*)&(port->laddr.addr);
    int sockport = 0;

    if (AF_UNIX == laddr->sa_family) {
        return true;
    } else if (AF_INET == laddr->sa_family) {
        sockport = ntohs(((struct sockaddr_in*)laddr)->sin_port);
    } else if (AF_INET6 == laddr->sa_family) {
        sockport = ntohs(((struct sockaddr_in6*)laddr)->sin6_port);
    }

    if (sockport == g_instance.attr.attr_network.PostPortNumber) {
        return true;
    }
    return false;
}

static void SetHaShmemData()
{
    volatile HaShmemData* hashmdata = t_thrd.postmaster_cxt.HaShmData;
    int i = 0, repl_list_num = 0;

    switch (t_thrd.xlog_cxt.server_mode) {
        case STANDBY_MODE: {
            hashmdata->current_mode = STANDBY_MODE;
            hashmdata->is_cascade_standby = t_thrd.xlog_cxt.is_cascade_standby;
            hashmdata->is_hadr_main_standby = t_thrd.xlog_cxt.is_hadr_main_standby;
            break;
        }
        case PRIMARY_MODE: {
            hashmdata->current_mode = PRIMARY_MODE;
            break;
        }
        case PENDING_MODE: {
            hashmdata->current_mode = PENDING_MODE;
            break;
        }
        default:
            break;
    }
    g_instance.global_sysdbcache.RefreshHotStandby();

    for (i = 1; i < MAX_REPLNODE_NUM; i++) {
        if (t_thrd.postmaster_cxt.ReplConnArray[i] != NULL) {
            repl_list_num++;
            if (t_thrd.postmaster_cxt.ReplConnArray[i]->isCrossRegion &&
                !IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE) {
                hashmdata->is_cross_region = true;
            }
        }
        if (t_thrd.postmaster_cxt.CrossClusterReplConnArray[i] != NULL) {
            repl_list_num++;
        }
    }

    hashmdata->repl_list_num = repl_list_num;
}
/*
 * check whether the ip and port is already listened
 */
static bool IsAlreadyListen(const char* ip, int port)
{
    int listen_index = 0;
    char sock_ip[IP_LEN] = {0};
    errno_t rc = 0;

    if (ip == NULL || port <= 0) {
        return false;
    }

    for (listen_index = 0; listen_index != MAXLISTEN; ++listen_index) {
        if (t_thrd.postmaster_cxt.ListenSocket[listen_index] != PGINVALID_SOCKET) {
            struct sockaddr_storage saddr;
            socklen_t slen = 0;
            char* result = NULL;
            rc = memset_s(&saddr, sizeof(saddr), 0, sizeof(saddr));
            securec_check(rc, "\0", "\0");

            slen = sizeof(saddr);
            if (comm_getsockname(t_thrd.postmaster_cxt.ListenSocket[listen_index],
                    (struct sockaddr*)&saddr,
                    (socklen_t*)&slen) < 0) {
                ereport(LOG, (errmsg("Error in getsockname int IsAlreadyListen()")));
                continue;
            }

            if (AF_INET6 == ((struct sockaddr *) &saddr)->sa_family) {
                result = inet_net_ntop(AF_INET6, &((struct sockaddr_in6 *) &saddr)->sin6_addr, 128, sock_ip, IP_LEN);
                if (NULL == result) {
                    ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
                }
            } else if (AF_INET == ((struct sockaddr *) &saddr)->sa_family) {
                result = inet_net_ntop(AF_INET, &((struct sockaddr_in *) &saddr)->sin_addr, 32, sock_ip, IP_LEN);
                if (NULL == result) {
                    ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
                }

            } else if (AF_UNIX == ((struct sockaddr *) &saddr)->sa_family) {
                continue;
            }

            /* Check if all IP addresses of local host had been listened already,
             * which was set using '*' in postgresql.conf.
             * :: represents all IPs for IPv6, 0.0.0.0 represents all IPs for IPv4. */
            if (((struct sockaddr *) &saddr)->sa_family == AF_INET6) {
                char* ipNoZone = NULL;
                char ipNoZoneData[IP_LEN] = {0};

                /* remove any '%zone' part from an IPv6 address string */
                ipNoZone = remove_ipv6_zone((char *)ip, ipNoZoneData, IP_LEN);
                if ((0 == strcmp(ipNoZone, sock_ip)) && (port == ntohs(((struct sockaddr_in6 *) &saddr)->sin6_port))) {
                    return true;
                }

                if ((strcmp(sock_ip, "::") == 0) && (port == ntohs(((struct sockaddr_in6 *) &saddr)->sin6_port))) {
                    return true;
                }
            } else if (((struct sockaddr *) &saddr)->sa_family == AF_INET) {
                if ((0 == strcmp(ip, sock_ip)) && (port == ntohs(((struct sockaddr_in *) &saddr)->sin_port))) {
                    return true;
                }

                if ((strcmp(sock_ip, "0.0.0.0") == 0) && (port == ntohs(((struct sockaddr_in *) &saddr)->sin_port))) {
                    return true;
                }
            } else {
                ereport(WARNING,
                       (errmsg("Unknown network protocal family type: %d", ((struct sockaddr *) &saddr)->sa_family)));
            }
        }
    }

    return false;
}

/*
 * whether the sockect is corresponding with IP-Port pair
 */
bool CheckSockAddr(struct sockaddr* sock_addr, const char* szIP, int port)
{
    struct sockaddr_in* ipv4addr = NULL;
    struct sockaddr_in6* ipv6addr = NULL;
#ifdef HAVE_UNIX_SOCKETS
    struct sockaddr_un* unixaddr = NULL;
    char unixStr[MAX_UNIX_PATH_LEN] = {'\0'};
#endif
    char IPstr[MAX_IP_STR_LEN] = {'\0'};
    int portNumber = 0;
    bool cmpResult = false;
    char* result = NULL;
    char* ipNoZone = NULL;
    char ipNoZoneData[IP_LEN] = {0};

    if ((NULL == sock_addr) || (NULL == szIP) || (0 == port)) {
        ereport(LOG, (errmsg("invalid socket information or IP string or port")));
        return false;
    }

    switch (sock_addr->sa_family) {
        /* if AF_UNIX, just compare the the name of the socket file */
#ifdef HAVE_UNIX_SOCKETS
        case AF_UNIX:
            unixaddr = (struct sockaddr_un*)sock_addr;
            UNIXSOCK_PATH(unixStr, port, g_instance.attr.attr_network.UnixSocketDir);

            if (0 == (strncmp(unixaddr->sun_path, unixStr, MAX_UNIX_PATH_LEN))) {
                cmpResult = true;
            }
            break;
#endif

        case AF_INET:
            ipv4addr = (struct sockaddr_in*)sock_addr;
            result = inet_net_ntop(AF_INET, &(ipv4addr->sin_addr), 32, IPstr, MAX_IP_STR_LEN - 1);
            if (NULL == result) {
                ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
            }
            portNumber = ntohs(ipv4addr->sin_port);
            if ((((0 == strncmp(szIP, LOCAL_HOST, MAX_IP_STR_LEN)) &&
                     (0 == strncmp(IPstr, LOOP_IP_STRING, MAX_IP_STR_LEN))) ||
                    (0 == strncmp(IPstr, szIP, MAX_IP_STR_LEN))) &&
                (port == portNumber)) {
                cmpResult = true;
            }
            break;

        case AF_INET6:
            /* remove any '%zone' part from an IPv6 address string */
            ipNoZone = remove_ipv6_zone((char *)szIP, ipNoZoneData, IP_LEN);

            ipv6addr = (struct sockaddr_in6*)sock_addr;
            result = inet_net_ntop(AF_INET6, &(ipv6addr->sin6_addr), 128, IPstr, MAX_IP_STR_LEN - 1);
            if (NULL == result) {
                ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
            }
            portNumber = ntohs(ipv6addr->sin6_port);
            if ((((0 == strncmp(ipNoZone, LOCAL_HOST, MAX_IP_STR_LEN)) &&
                     (0 == strncmp(IPstr, LOOP_IPV6_IP, MAX_IP_STR_LEN))) ||
                    (0 == strncmp(IPstr, ipNoZone, MAX_IP_STR_LEN))) &&
                (port == portNumber)) {
                cmpResult = true;
            }
            break;

        default:
            ereport(LOG, (errmsg("unkown socket address family")));
            break;
    }

    return cmpResult;
}

/*
 * According to createmode, create the listen socket
 */
void CreateServerSocket(
    char* ipaddr, int portNumber, int enCreatemode, int* success, bool add_localaddr_flag, bool is_create_psql_sock)
{
    int status = 0;
    int successCount = 0;

    Assert(ipaddr != NULL);
    Assert(success != NULL);

    successCount = *success;

    /* if the createmode is NEED_CREATE_TCPIP or NEED_CREATE_UN_TCPIP and
        the socket has not been created, then create it */
    if (((unsigned int)enCreatemode & NEED_CREATE_TCPIP) && (!IsAlreadyListen(ipaddr, portNumber))) {
        ereport(LOG, (errmsg("Create TCP/IP socket [%s:%d]", ipaddr, portNumber)));

        if (strcmp(ipaddr, "*") == 0) {
            status = StreamServerPort(AF_UNSPEC,
                NULL,
                (unsigned short)portNumber,
                g_instance.attr.attr_network.UnixSocketDir,
                t_thrd.postmaster_cxt.ListenSocket,
                MAXLISTEN,
                add_localaddr_flag,
                is_create_psql_sock,
                false);
        } else {
            status = StreamServerPort(AF_UNSPEC,
                ipaddr,
                (unsigned short)portNumber,
                g_instance.attr.attr_network.UnixSocketDir,
                t_thrd.postmaster_cxt.ListenSocket,
                MAXLISTEN,
                add_localaddr_flag,
                is_create_psql_sock,
                false);
        }
        if (status == STATUS_OK) {
            successCount++;
        } else {
            ereport(WARNING,
                (errmsg("could not create listen socket for \"ipaddr:%s,portNumber:%d\"", ipaddr, portNumber)));
        }
    } else {
        successCount++;
        ereport(
            LOG, (errmsg("TCP/IP socket [%s:%d] has been created already, no need recreate it", ipaddr, portNumber)));
    }

    *success = successCount;
}

/*
 * parse ReplConnArray1 and ReplConnArray2 into ip-port pair and save it in pListenList
 */
bool ParseHaListenAddr(LISTEN_ADDRS* pListenList, ReplConnInfo** replConnArray)
{
    int count = 0, i = 0;
    errno_t rc = 0;

    Assert(NULL != pListenList);
    if (NULL == pListenList) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("parameter error in ParseHaListenAddr()")));
        return false;
    }
    count = pListenList->usedNum;

    for (i = 1; i < MAX_REPLNODE_NUM; i++) {
        if (replConnArray[i] != NULL) {
            pListenList->lsnArray[count].portnum = replConnArray[i]->localport;
            pListenList->lsnArray[count].createmodel = NEED_CREATE_TCPIP;
            rc = strncpy_s(pListenList->lsnArray[count].ipaddr,
                sizeof(pListenList->lsnArray[count].ipaddr),
                replConnArray[i]->localhost,
                MAX_IPADDR_LEN - 1);
            securec_check(rc, "\0", "\0");
            pListenList->lsnArray[count].ipaddr[MAX_IPADDR_LEN - 1] = '\0';
            count++;
        }
    }
    pListenList->usedNum = count;

    return ((count == 0) ? false : true);
}

/*
 * according to ReplConnArray, close the socket not in ReplConnArray and create new socket
 * if the socket in ReplConnArray is already created, just ignore and step over
 */
static void CreateHaListenSocket(void)
{
    LISTEN_ADDRS newListenAddrs;
    struct sockaddr_storage sock_addr;
    int i = 0, j = 0, success = 0;
    socklen_t s_len;

    int ss_rc = memset_s(&newListenAddrs, sizeof(newListenAddrs), 0, sizeof(newListenAddrs));
    securec_check(ss_rc, "\0", "\0");

    /* parse the ReplConnArray into IP-Port pair */
    if (!ParseHaListenAddr(&newListenAddrs, t_thrd.postmaster_cxt.ReplConnArray)) {
        ereport(WARNING, (errmsg("\"replconninfo\" has been set null or invalid ")));
    }
    if (!ParseHaListenAddr(&newListenAddrs, t_thrd.postmaster_cxt.CrossClusterReplConnArray)) {
        ereport(WARNING, (errmsg("\"cross_cluster_replconninfo\" has been set null or invalid ")));
    }

    /*
     * if the IP-Port in newListenAddrs has not been created, then create the socket
     * if already has been created, just set the createmodel and continue.
     */
    for (i = 0; i < MAXLISTEN; i++) {
        if (PGINVALID_SOCKET == t_thrd.postmaster_cxt.ListenSocket[i] ||
            t_thrd.postmaster_cxt.listen_sock_type[i] != HA_LISTEN_SOCKET) {
            continue;
        }

        s_len = sizeof(sock_addr);
        if (comm_getsockname(t_thrd.postmaster_cxt.ListenSocket[i], (struct sockaddr*)&sock_addr, (socklen_t*)&s_len) < 0) {
            ereport(LOG, (errmsg("Error in getsockname int IsAlreadyListen()")));
        }
        success = 0;

        /* if the socket in newListenAddrs is already created, just set the createmodel */
        for (j = 0; j < newListenAddrs.usedNum; j++) {
            if (CheckSockAddr((struct sockaddr*)&sock_addr,
                    newListenAddrs.lsnArray[j].ipaddr,
                    newListenAddrs.lsnArray[j].portnum)) {
                if (AF_UNIX == ((struct sockaddr*)&sock_addr)->sa_family) {
                    if (newListenAddrs.lsnArray[j].portnum != g_instance.attr.attr_network.PoolerPort)
                        ereport(WARNING,
                            (errmsg("replication socket could not be Unix Domain Socket, "
                                    "something must be wrong of the Ha listen socket")));
                } else {
                    newListenAddrs.lsnArray[j].createmodel = (uint32)newListenAddrs.lsnArray[j].createmodel
                                                             & ~NEED_CREATE_TCPIP;
                }
                success++;
            }
        }

        /* ha pooler port and ha pooler socket also belongs to HA_LISTEN_SOCKET, no need to close them */
        if (!success && IsMatchSocketAddr((struct sockaddr*)&sock_addr, g_instance.attr.attr_network.PoolerPort)) {
            success++;
        }

        /* if the socket is not match all the IP-Port pair in newListenAddrs, close it and set listen_sock_type */
        if (!success) {
            (void)comm_closesocket(t_thrd.postmaster_cxt.ListenSocket[i]);
            t_thrd.postmaster_cxt.ListenSocket[i] = PGINVALID_SOCKET;
            t_thrd.postmaster_cxt.listen_sock_type[i] = UNUSED_LISTEN_SOCKET;
        }
    }

    /* according to createmodel, create the socket of IP-Port in newListenAddrs */
    success = 0;
    for (i = 0; i < newListenAddrs.usedNum; i++) {
        CreateServerSocket(newListenAddrs.lsnArray[i].ipaddr,
            newListenAddrs.lsnArray[i].portnum,
            (int)newListenAddrs.lsnArray[i].createmodel,
            &success,
            false,
            false);
    }

    if (0 == success) {
        ereport(LOG, (errmsg("could not create any TCP/IP sockets for HA listen addresses")));
    } else {
        ereport(LOG, (errmsg("number of valid HA TCP/IP sockets is \"%d\"", success)));
    }

    return;
}

/*
 * move all the non-default value at the head of the array
 */
static void IntArrayRegulation(int array[], int len, int def)
{
    int i = 0, j = 0;
    int* tmp = NULL;
    if (NULL == array || len < 0) {
        ereport(WARNING, (errmsg("The parameter error in array regulation")));
        return;
    }

    tmp = (int*)palloc(sizeof(int) * len);
    if (NULL == tmp) {
        ereport(WARNING, (errmsg("Error palloc in array regulation")));
        return;
    }
    for (i = 0; i != len; ++i) {
        tmp[i] = array[i];
        array[i] = def;
    }

    for (i = 0; i != len; ++i) {
        if (tmp[i] != def) {
            array[j++] = tmp[i];
        }
    }
    pfree(tmp);
}

/*
 * adjust the array  ListenSocket and listen_sock_type, so that the readmask
 * in ServerLoop can be available
 */
static void ListenSocketRegulation(void)
{
    IntArrayRegulation((int*)t_thrd.postmaster_cxt.ListenSocket, MAXLISTEN, (int)PGINVALID_SOCKET);

    IntArrayRegulation((int*)t_thrd.postmaster_cxt.listen_sock_type, MAXLISTEN, UNUSED_LISTEN_SOCKET);
}

static DbState get_share_storage_node_dbstate_sub()
{
    if (IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE) {
        return NORMAL_STATE;
    } else if (WalRcvIsOnline()) {
        return NORMAL_STATE;
    } else if (pg_atomic_read_u32(&t_thrd.walreceiverfuncs_cxt.WalRcv->rcvDoneFromShareStorage)) {
        return NEEDREPAIR_STATE;
    } else {
        return STARTING_STATE;
    }
}

DbState get_local_dbstate_sub(WalRcvData* walrcv, ServerMode mode)
{
    bool has_build_reason = true;
    bool share_storage_has_no_build_reason = (IS_SHARED_STORAGE_MODE &&
        t_thrd.postmaster_cxt.HaShmData->repl_reason[t_thrd.postmaster_cxt.HaShmData->current_repl] == NONE_REBUILD) ||
        (IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE &&
        t_thrd.postmaster_cxt.HaShmData->repl_reason[t_thrd.postmaster_cxt.HaShmData->current_repl] == CONNECT_REBUILD);
    bool disater_recovery_has_no_build_reason = (IS_OBS_DISASTER_RECOVER_MODE &&
        t_thrd.postmaster_cxt.HaShmData->repl_reason[t_thrd.postmaster_cxt.HaShmData->current_repl] == NONE_REBUILD);
    if ((t_thrd.postmaster_cxt.HaShmData->repl_reason[t_thrd.postmaster_cxt.HaShmData->current_repl] ==
        NONE_REBUILD && walrcv != NULL && walrcv->isRuning && 
        (walrcv->conn_target == REPCONNTARGET_PRIMARY || IsCascadeStandby())) ||
        dummyStandbyMode || share_storage_has_no_build_reason || disater_recovery_has_no_build_reason) {
        has_build_reason = false;
    }
    switch (mode) {
        case NORMAL_MODE:
        case PRIMARY_MODE:
            return NORMAL_STATE;
        case PENDING_MODE:
        case STANDBY_MODE:
            if (has_build_reason)
                return NEEDREPAIR_STATE;
            else {
                /* use keeplive msg from walsender to check if db state is catchup or not.
                   Also use local info from walreceiver as a supplement. */
                if (wal_catchup || data_catchup) {
                    return CATCHUP_STATE;
                } else if (IS_SHARED_STORAGE_MODE) {
                    return get_share_storage_node_dbstate_sub();
                } else if (!WalRcvIsOnline() && !IS_OBS_DISASTER_RECOVER_MODE) {
                    return STARTING_STATE;
                } else {
                    return NORMAL_STATE;
                }
            }
            break;
        default:
            break;
    }

    return UNKNOWN_STATE;
}

DbState get_local_dbstate(void)
{
    volatile WalRcvData* walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    ServerMode mode = t_thrd.postmaster_cxt.HaShmData->current_mode;
    DbState db_state = UNKNOWN_STATE;

    if (t_thrd.walsender_cxt.WalSndCtl && t_thrd.walsender_cxt.WalSndCtl->demotion > NoDemote)
        db_state = DEMOTING_STATE;
    else if (walrcv && NODESTATE_STANDBY_WAITING == walrcv->node_state)
        db_state = WAITING_STATE;
    else if (walrcv && (NODESTATE_STANDBY_PROMOTING == walrcv->node_state ||
                           NODESTATE_STANDBY_FAILOVER_PROMOTING == walrcv->node_state))
        db_state = PROMOTING_STATE;
    else {
        db_state = get_local_dbstate_sub((WalRcvData*)walrcv, mode);
    }
    return db_state;
}

const char* wal_get_db_state_string(DbState db_state)
{
    switch (db_state) {
        case NORMAL_STATE:
            return "Normal";
        case UNKNOWN_STATE:
            return "Unknown";
        case NEEDREPAIR_STATE:
            return "Need repair";
        case STARTING_STATE:
            return "Starting";
        case WAITING_STATE:
            return "Wait promoting";
        case DEMOTING_STATE:
            return "Demoting";
        case PROMOTING_STATE:
            return "Promoting";
        case BUILDING_STATE:
            return "Building";
        case CATCHUP_STATE:
            return "Catchup";
        case COREDUMP_STATE:
            return "Coredump";
        default:
            return "Unknown";
    }
}

static ServerMode get_cur_mode(void)
{
    return t_thrd.postmaster_cxt.HaShmData->current_mode;
}

static int get_cur_repl_num(void)
{
    return t_thrd.postmaster_cxt.HaShmData->repl_list_num;
}

static void PMReadDBStateFile(GaussState* state)
{
    FILE* statef = NULL;

    if (NULL == state) {
        ereport(LOG, (errmsg("%s: parameter state is null in PMReadDBStateFile()", progname)));
        return;
    }
    int rc = memset_s(state, sizeof(GaussState), 0, sizeof(GaussState));
    securec_check(rc, "", "");
    statef = fopen(gaussdb_state_file, "r");
    if (NULL == statef) {
        ereport(LOG,
            (errmsg("%s: open gaussdb state file \"%s\" failed, could "
                    "not read the build infomation: %m",
                progname,
                gaussdb_state_file)));
        return;
    }

    if (0 == (fread(state, 1, sizeof(GaussState), statef))) {
        ereport(LOG,
            (errmsg(
                "%s: read gaussdb state infomation from the file \"%s\" failed: %m", progname, gaussdb_state_file)));
    }
    fclose(statef);
}

/*
 * update the hashmemdata infomation in gaussdb state file
 */
static void PMSetDBStateFile(GaussState* state)
{
    FILE* statef = NULL;
    char temppath[MAXPGPATH] = {0};

    /*
     * skip update gaussdb state file in bootstrap and dummy mode.
     */
    if (IsInitdb || dummyStandbyMode)
        return;

    Assert(t_thrd.proc_cxt.MyProcPid == PostmasterPid);

    if (NULL == state) {
        ereport(LOG, (errmsg("%s: parameter state is null in PMSetDBStateFile()", progname)));
        return;
    }

    int rc = snprintf_s(temppath, MAXPGPATH, MAXPGPATH - 1, "%s.temp", gaussdb_state_file);
    securec_check_intval(rc, , );

    statef = fopen(temppath, "w");
    if (NULL == statef) {
        ereport(LOG,
            (errmsg("%s: open gaussdb state file \"%s\" failed, could not "
                    "update the build infomation: %m",
                progname,
                temppath)));
        return;
    }
    if (0 == (fwrite(state, 1, sizeof(GaussState), statef))) {
        ereport(
            LOG, (errmsg("%s: update gaussdb state infomation from the file \"%s\" failed: %m", progname, temppath)));
    }

    if (fsync(fileno(statef)) == 0) {
        ereport(LOG, (errmsg("%s: fsync file \"%s\" success", progname, temppath)));
    } else {
        ereport(LOG, (errmsg("%s: fsync file \"%s\" fail", progname, temppath)));
    }
    fclose(statef);

    if (rename(temppath, gaussdb_state_file) != 0)
        ereport(LOG, (errmsg("can't rename \"%s\" to \"%s\": %m", temppath, gaussdb_state_file)));
}

static bool IsDBStateFileExist()
{
    return access(gaussdb_state_file, F_OK) != -1;
}

static void PMInitDBStateFile()
{
    GaussState state;
    int rc = memset_s(&state, sizeof(state), 0, sizeof(state));
    securec_check(rc, "", "");
    state.conn_num = t_thrd.postmaster_cxt.HaShmData->repl_list_num;
    state.mode = t_thrd.postmaster_cxt.HaShmData->current_mode;
    state.state = STARTING_STATE;
    state.lsn = 0;
    state.term = 0;
    state.sync_stat = false;
    state.ha_rebuild_reason = NONE_REBUILD;
    state.current_connect_idx = 1;
    if (IS_CN_DISASTER_RECOVER_MODE && IsDBStateFileExist()) {
        GaussState temp_state;
        PMReadDBStateFile(&temp_state);
        t_thrd.postmaster_cxt.HaShmData->current_repl = temp_state.current_connect_idx;
        t_thrd.postmaster_cxt.HaShmData->prev_repl = temp_state.current_connect_idx;
        state.current_connect_idx = temp_state.current_connect_idx;
    }

    PMSetDBStateFile(&state);
    ereport(LOG,
        (errmsg("create gaussdb state file success: db state(STARTING_STATE), server mode(%s), connection index(%d)",
        wal_get_role_string(t_thrd.postmaster_cxt.HaShmData->current_mode), state.current_connect_idx)));
}


/*
 * according to input parameters, update the gaussdb state file
 */
static void PMUpdateDBState(DbState db_state, ServerMode mode, int conn_num)
{
    GaussState state;

    PMReadDBStateFile(&state);
    state.state = db_state;
    if (mode == STANDBY_MODE && t_thrd.postmaster_cxt.HaShmData->is_cascade_standby) {
        state.mode = CASCADE_STANDBY_MODE;
    } else {
        state.mode = mode;
    }
    state.conn_num = conn_num;
    PMSetDBStateFile(&state);
}

static void PMUpdateDBStateLSN(void)
{
    GaussState state;
    XLogRecPtr recptr = GetXLogReplayRecPtrInPending();

    PMReadDBStateFile(&state);
    state.lsn = recptr;
    state.term = Max(g_instance.comm_cxt.localinfo_cxt.term_from_file,
        g_instance.comm_cxt.localinfo_cxt.term_from_xlog);
    PMSetDBStateFile(&state);
}

/*
 * Update ha build reason to gaussdb.state file, if gs_ctl can not query
 * Ha status(e.g: recovery thread hold a system table lock), we can read
 * the gaussdb.state file to get it.
 */
static void PMUpdateDBStateHaRebuildReason(void)
{
    GaussState state;
    volatile HaShmemData* hashmdata = t_thrd.postmaster_cxt.HaShmData;
    HaRebuildReason reason = NONE_REBUILD;

    SpinLockAcquire(&hashmdata->mutex);
    reason = hashmdata->repl_reason[hashmdata->current_repl];
    SpinLockRelease(&hashmdata->mutex);

    PMReadDBStateFile(&state);
    state.ha_rebuild_reason = reason;
    if (reason != NONE_REBUILD) {
        state.state = NEEDREPAIR_STATE;
    } else {
        state.state = NORMAL_STATE;
        state.current_connect_idx = hashmdata->current_repl;
    }
    PMSetDBStateFile(&state);
    ereport(LOG,
        (errmsg("update gaussdb state file: build reason(%s), "
                "db state(%s), server mode(%s), current connect index(%d)",
            wal_get_rebuild_reason_string(reason),
            wal_get_db_state_string(state.state),
            wal_get_role_string(get_cur_mode()),
            state.current_connect_idx)));
}

static int init_stream_comm()
{
    int error, rc;
    char local_ip[IP_LEN] = {0};

    char* rawstring = NULL;
    List* elemlist = NULL;
    ListCell* l = NULL;
    MemoryContext oldctx = NULL;

    oldctx = MemoryContextSwitchTo(g_instance.comm_cxt.comm_global_mem_cxt);

    /* Need a modifiable copy of local_bind_address */
    rawstring = pstrdup(tcp_link_addr);

    /* Parse string into list of identifiers */
    if (!SplitIdentifierString(rawstring, ',', &elemlist)) {
        /* syntax error in list */
        ereport(
            FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid list syntax for \"listen_addresses\"")));
    }

    foreach (l, elemlist) {
        char* curhost = (char*)lfirst(l);

        if (strcmp(curhost, "*") != 0 && strcmp(curhost, "localhost") != 0 && strcmp(curhost, "0.0.0.0") != 0) {
            /* Just use the first ip addr,no support multi-ip now! */
            rc = strncpy_s(local_ip, IP_LEN, curhost, strlen(curhost));
            securec_check(rc, "", "");
            break;
        }
    }

    if (*local_ip == '\0') {
        errno_t ss_rc = strncpy_s(local_ip, IP_LEN, "127.0.0.1", IP_LEN - 1);
        securec_check(ss_rc, "", "");
    }

    list_free(elemlist);
    pfree(rawstring);

    gs_init_hash_table();
    gs_set_hs_shm_data(t_thrd.postmaster_cxt.HaShmData);
    gs_connect_regist_callback(&StreamConsumer::wakeUpConsumerCallBack);

    error = gs_set_basic_info(local_ip,
        g_instance.attr.attr_common.PGXCNodeName,
        u_sess->attr.attr_network.comm_max_datanode + g_instance.attr.attr_network.MaxCoords,
        t_thrd.libpq_cxt.sock_path);

    if (error != 0) {
        ereport(FATAL, (errmsg("set basic info of stream failed!")));
    }

    MemoryContextSwitchTo(oldctx);

    return error;
}

// pg_logging_comm_status
// 		Logging status of internal structures of stream communication layer.
//
// This function is here so we don't have to export the
// GlobalTransactionData struct definition.
//
Datum pg_log_comm_status(PG_FUNCTION_ARGS)
{
    // If this is a datanode and in stream communication mode, do log status
    //
    if (IS_PGXC_DATANODE)
        gs_log_comm_status();
    PG_RETURN_DATUM(0);
}

/*
 * Finish inplace or online upgrade and enable new features of the new gaussdb
 * binary by switching the grand version num to the newer version num.
 *
 * Before calling this function, all system catalog changes and other upgrade
 * procedures should have been completed. After calling this function, the upgrade
 * process are nominally succeeded and usually can not be rollbacked.
 *
 * In effect, this function only changes the grand version of postmaster thread.
 * Any newly started backend will inherit this version. However, for pre-existing
 * backends, including those newly started backends caused by pre-existing backends,
 * e.g. newly started DN backends to process queries or planes passed down by
 * pre-exisitng CN backends, their backend versions are not changed and they still
 * adopt pre-upgrade behavior of the old-version gaussdb binary.
 */
Datum set_working_grand_version_num_manually(PG_FUNCTION_ARGS)
{
    uint32 tmp_version = 0;

    if (u_sess->upg_cxt.InplaceUpgradeSwitch)
        tmp_version = PG_GETARG_UINT32(0);
    else
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("could not set WorkingGrandVersionNum manually while not performing upgrade")));

    if (!tmp_version)
        tmp_version = GRAND_VERSION_NUM;

    pg_atomic_write_u32(&WorkingGrandVersionNum, tmp_version);

    PG_RETURN_VOID();
}

/*
 * @Description: check the value from environment variablethe to prevent command injection.
 * @in input_env_value : the input value need be checked.
 */
bool backend_env_valid(const char* input_env_value, const char* stamp)
{
#define MAXENVLEN 1024

    const char* danger_character_list[] = {";", "`", "\\", "'", "\"", ">", "<", "$", "&", "|", "!", "\n", "../", NULL};
    int i = 0;

    if (input_env_value == NULL || strlen(input_env_value) >= MAXENVLEN) {
        ereport(LOG, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("environment variable $%s is NULL or size is out of %d\n",
                      stamp, MAXENVLEN)));
        return false;
    }

    for (i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr((const char*)input_env_value, danger_character_list[i])) {
            ereport(LOG, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Error: environment variable $%s(%s) contain invaild symbol \"%s\".\n",
                           stamp, input_env_value, danger_character_list[i])));
            return false;
        }
    }

    return true;
}


/*
 * @Description: check the value from environment variablethe to prevent command injection.
 * @in input_env_value : the input value need be checked.
 */
void check_backend_env(const char* input_env_value)
{
#define MAXENVLEN 1024

    const char* danger_character_list[] = {";", "`", "\\", "'", "\"", ">", "<", "$", "&", "|", "!", "\n", NULL};
    int i = 0;

    if (input_env_value == NULL || strlen(input_env_value) >= MAXENVLEN) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("wrong environment variable \"%s\"", input_env_value)));
    }

    for (i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr((const char*)input_env_value, danger_character_list[i])) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Error: environment variable \"%s\" contain invaild symbol \"%s\".\n",
                        input_env_value,
                        danger_character_list[i])));
        }
    }
}

EnvCheckResult check_backend_env_sigsafe(const char* input_env_value)
{

    const char danger_character_list[] = {';', '`', '\\', '\'', '"', '>', '<', '$', '&', '|', '!', '\n'};
    int i, j;

    if (input_env_value == NULL) {
        return ENV_ERR_NULL;
    }

    int sizeof_danger = sizeof(danger_character_list)/sizeof(char);
    for (i = 0; i < MAXPGPATH; i++) {
        if (input_env_value[i] == '\0') {
            break;
        }
        for (j = 0; j < sizeof_danger; j++) {
            if (danger_character_list[j] == input_env_value[i]) {
                return ENV_ERR_DANGER_CHARACTER;
            }
        }
    }

    if (i == MAXPGPATH) {
        return ENV_ERR_LENGTH;
    }

    return ENV_OK;
}

void CleanSystemCaches(bool is_in_read_command)
{
    int64 usedSize = 0;

    usedSize = ((AllocSet)u_sess->cache_mem_cxt)->totalSpace - ((AllocSet)u_sess->cache_mem_cxt)->freeSpace;

    /* Over threshold, need to clean cache. */
    if (usedSize > g_instance.attr.attr_memory.local_syscache_threshold*1024) {
        ereport(DEBUG1,
            (errmsg("CleanSystemCaches due to "
                    "SystemCache(%ld) greater than (%d),in_read_command(%d).",
                usedSize,
                 g_instance.attr.attr_memory.local_syscache_threshold*1024,
                (is_in_read_command ? 1 : 0))));

        if (IsTransactionOrTransactionBlock() || !is_in_read_command) {
            InvalidateSessionSystemCaches();
        }
    }
}

static void check_and_reset_ha_listen_port(void)
{
    int j;
    int repl_list_num = 0;
    bool refreshReplList = false;
    bool needToRestart = false;
    bool refreshListenSocket = false;

    /*
     * when Ha replconninfo have changed and current_mode is not NORMAL,
     * dynamically modify the ha socket.
     */
    for (j = 1; j < MAX_REPLNODE_NUM; j++) {

        if (t_thrd.postmaster_cxt.ReplConnChangeType[j] == ADD_REPL_CONN_INFO_WITH_OLD_LOCAL_IP_PORT ||
            t_thrd.postmaster_cxt.ReplConnChangeType[j] == ADD_REPL_CONN_INFO_WITH_NEW_LOCAL_IP_PORT ||
            t_thrd.postmaster_cxt.CrossClusterReplConnChanged[j]) {
            refreshReplList = true;
        }

        if (t_thrd.postmaster_cxt.ReplConnChangeType[j] == OLD_REPL_CHANGE_IP_OR_PORT ||
            t_thrd.postmaster_cxt.CrossClusterReplConnChanged[j]) {
            needToRestart = true;
            refreshListenSocket = true;
        }

        if (t_thrd.postmaster_cxt.ReplConnChangeType[j] == ADD_REPL_CONN_INFO_WITH_NEW_LOCAL_IP_PORT) {
            refreshListenSocket = true;
        }

        t_thrd.postmaster_cxt.ReplConnChangeType[j] = NO_CHANGE;
        t_thrd.postmaster_cxt.CrossClusterReplConnChanged[j] = false;

        if (t_thrd.postmaster_cxt.ReplConnArray[j] != NULL)
            repl_list_num++;
        if (t_thrd.postmaster_cxt.CrossClusterReplConnArray[j] != NULL)
            repl_list_num++;
    }

    if (refreshReplList) {
        t_thrd.postmaster_cxt.HaShmData->repl_list_num = repl_list_num;
    }

    if (refreshListenSocket) {
        CreateHaListenSocket();
        ListenSocketRegulation();
    }

    if (needToRestart) {
        /* send SIGTERM to end process senders and receiver */
        (void)SignalSomeChildren(SIGTERM, BACKEND_TYPE_WALSND);
        (void)SignalSomeChildren(SIGTERM, BACKEND_TYPE_DATASND);
        if (g_instance.pid_cxt.WalRcvWriterPID != 0)
            signal_child(g_instance.pid_cxt.WalRcvWriterPID, SIGTERM);
        if (g_instance.pid_cxt.WalReceiverPID != 0)
            signal_child(g_instance.pid_cxt.WalReceiverPID, SIGTERM);
        if (g_instance.pid_cxt.DataRcvWriterPID != 0)
            signal_child(g_instance.pid_cxt.DataRcvWriterPID, SIGTERM);
        if (g_instance.pid_cxt.DataReceiverPID != 0)
            signal_child(g_instance.pid_cxt.DataReceiverPID, SIGTERM);
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.postmaster_cxt.HaShmData != NULL &&
        t_thrd.postmaster_cxt.HaShmData->repl_list_num == 0 &&
        t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE) {
        t_thrd.postmaster_cxt.HaShmData->current_mode = NORMAL_MODE;
        g_instance.global_sysdbcache.RefreshHotStandby();
        SetServerMode(NORMAL_MODE);
    }
#endif

    return;
}

bool IsCBMWriterRunning(void)
{
    if (g_instance.pid_cxt.CBMWriterPID != 0)
        return true;
    else
        return false;
}
bool PMstateIsRun(void)
{
    return PM_RUN == pmState;
}

/* malloc api of cJSON at backend side */
static void* cJSON_internal_malloc(size_t size)
{
    return palloc(size);
}

/* free api of cJSON at backend side */
void cJSON_internal_free(void* pointer)
{
    pfree(pointer);
}

/*
 * Begin shutdown of an auxiliary process.	This is approximately the equivalent
 * of ShutdownPostgres() in postinit.c.  We can't run transactions in an
 * auxiliary process, so most of the work of AbortTransaction() is not needed,
 * but we do need to make sure we've released any LWLocks we are holding.
 * (This is only critical during an error exit.)
 */
static void ShutdownAuxiliaryProcess(int code, Datum arg)
{
    LWLockReleaseAll();
    /* Clear wait information */
    pgstat_report_waitevent(WAIT_EVENT_END);
}

template <knl_thread_role thread_role>
static void SetAuxType()
{
    switch (thread_role) {
        case PARALLEL_DECODE:
            t_thrd.bootstrap_cxt.MyAuxProcType = ParallelDecodeProcess;
            break;
        case LOGICAL_READ_RECORD:
            t_thrd.bootstrap_cxt.MyAuxProcType = LogicalReadRecord;
            break;
        case PAGEREDO:
            t_thrd.bootstrap_cxt.MyAuxProcType = PageRedoProcess;
            break;
        case TWOPASECLEANER:
            t_thrd.bootstrap_cxt.MyAuxProcType = TwoPhaseCleanerProcess;
            break;
        case FAULTMONITOR:
            t_thrd.bootstrap_cxt.MyAuxProcType = FaultMonitorProcess;
            break;
        case BGWRITER:
            t_thrd.bootstrap_cxt.MyAuxProcType = BgWriterProcess;
            break;
        case SPBGWRITER:
            t_thrd.bootstrap_cxt.MyAuxProcType = SpBgWriterProcess;
            break;
        case CHECKPOINT_THREAD:
            t_thrd.bootstrap_cxt.MyAuxProcType = CheckpointerProcess;
            break;
        case WALWRITER:
            t_thrd.bootstrap_cxt.MyAuxProcType = WalWriterProcess;
            break;
        case WALWRITERAUXILIARY:
            t_thrd.bootstrap_cxt.MyAuxProcType = WalWriterAuxiliaryProcess;
            break;
        case WALRECEIVER:
            t_thrd.bootstrap_cxt.MyAuxProcType = WalReceiverProcess;
            break;
        case WALRECWRITE:
            t_thrd.bootstrap_cxt.MyAuxProcType = WalRcvWriterProcess;
            break;
        case DATARECIVER:
            t_thrd.bootstrap_cxt.MyAuxProcType = DataReceiverProcess;
            break;
        case DATARECWRITER:
            t_thrd.bootstrap_cxt.MyAuxProcType = DataRcvWriterProcess;
            break;
        case CBMWRITER:
            t_thrd.bootstrap_cxt.MyAuxProcType = CBMWriterProcess;
            break;
        case STARTUP:
            t_thrd.bootstrap_cxt.MyAuxProcType = StartupProcess;
            break;
        case PAGEWRITER_THREAD:
            t_thrd.bootstrap_cxt.MyAuxProcType = PageWriterProcess;
            break;
        case PAGEREPAIR_THREAD:
            t_thrd.bootstrap_cxt.MyAuxProcType = PageRepairProcess;
            break;
        case THREADPOOL_LISTENER:
            t_thrd.bootstrap_cxt.MyAuxProcType = TpoolListenerProcess;
            break;
        case THREADPOOL_SCHEDULER:
            t_thrd.bootstrap_cxt.MyAuxProcType = TpoolSchdulerProcess;
            break;
        case HEARTBEAT:
            t_thrd.bootstrap_cxt.MyAuxProcType = HeartbeatProcess;
            break;
        case UNDO_RECYCLER:
            t_thrd.bootstrap_cxt.MyAuxProcType = UndoRecyclerProcess;
            break;
        case SHARE_STORAGE_XLOG_COPYER:
            t_thrd.bootstrap_cxt.MyAuxProcType = XlogCopyBackendProcess;
            break;
#ifdef ENABLE_MULTIPLE_NODES
        case BARRIER_PREPARSE:
            t_thrd.bootstrap_cxt.MyAuxProcType = BarrierPreParseBackendProcess;
            break;
        case TS_COMPACTION:
            t_thrd.bootstrap_cxt.MyAuxProcType = TsCompactionProcess;
            break;
        case TS_COMPACTION_CONSUMER:
            t_thrd.bootstrap_cxt.MyAuxProcType = TsCompactionConsumerProcess;
            break;
        case TS_COMPACTION_AUXILIAY:
            t_thrd.bootstrap_cxt.MyAuxProcType = TsCompactionAuxiliaryProcess;
            break;
#endif   /* ENABLE_MULTIPLE_NODES */
        default:
            ereport(ERROR, (errmsg("unrecorgnized proc type %d", thread_role)));
    }
}

template <knl_thread_role role>
void SetExtraThreadInfo(knl_thread_arg* arg)
{
    if (arg->payload == NULL)
        return;

    switch (role) {
        case THREADPOOL_WORKER: {
            t_thrd.threadpool_cxt.worker = (ThreadPoolWorker*)arg->payload;
            t_thrd.threadpool_cxt.group  = t_thrd.threadpool_cxt.worker->GetGroup();
            break;
        }
        case STREAM_WORKER: {
            StreamProducer* proObj = (StreamProducer*)arg->payload;
            SetStreamWorkerInfo(proObj);
            break;
        }
        case THREADPOOL_STREAM: {
            t_thrd.threadpool_cxt.stream = (ThreadPoolStream*)arg->payload;
            t_thrd.threadpool_cxt.group  = t_thrd.threadpool_cxt.stream->GetGroup();
            StreamProducer* proObj = (StreamProducer*)t_thrd.threadpool_cxt.stream->GetProducer();
            SetStreamWorkerInfo(proObj);
            break;
        }
#ifdef ENABLE_MULTIPLE_NODES
        case TS_COMPACTION_CONSUMER: {
            CompactionWorkerProcess::SetConsumerThreadLocal(arg);
            break;
        }
#endif
        case THREADPOOL_LISTENER: {
            t_thrd.threadpool_cxt.listener = (ThreadPoolListener*)arg->payload;
            break;
        }
        case THREADPOOL_SCHEDULER: {
            t_thrd.threadpool_cxt.scheduler = (ThreadPoolScheduler*)arg->payload;
            break;
        }
        case PAGEREDO: {
            SetMyPageRedoWorker(arg);
            break;
        }
        case BGWORKER: {
            t_thrd.bgworker_cxt.bgwcontext = ((BackgroundWorkerArgs *)arg->payload)->bgwcontext;
            t_thrd.bgworker_cxt.bgworker   = ((BackgroundWorkerArgs *)arg->payload)->bgworker;
            t_thrd.bgworker_cxt.bgworkerId = ((BackgroundWorkerArgs *)arg->payload)->bgworkerId;
            pfree_ext(arg->payload);
        }
        default:
            break;
    }
}

void InitProcessAndShareMemory()
{
    /* Restore basic shared memory pointers */
    InitShmemAccess(UsedShmemSegAddr);

    /* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
    InitProcess();

    CHECK_FOR_PROCDIEPENDING();

    /*
     * Attach process to shared data structures.  If testing EXEC_BACKEND
     * on Linux, you must run this as root before starting the postmaster:
     *
     * echo 0 >/proc/sys/kernel/randomize_va_space
     *
     * This prevents a randomized stack base address that causes child
     * shared memory to be at a different address than the parent, making
     * it impossible to attached to shared memory.  Return the value to
     * '1' when finished.
     */
    CreateSharedMemoryAndSemaphores(false, 0);
}

template <knl_thread_role thread_role>
int GaussDbAuxiliaryThreadMain(knl_thread_arg* arg)
{
    t_thrd.role = arg->role;
    Assert(thread_role == t_thrd.role);
    Assert(thread_role == arg->role);

    /*
     * initialize globals
     */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    t_thrd.proc_cxt.MyStartTime = time(NULL);
    t_thrd.comm_sock_option = g_default_invalid_sock_opt;
    t_thrd.comm_epoll_option = g_default_invalid_epoll_opt;

    /*
     * Initialize random() for the first time, like PostmasterMain() would.
     * In a regular IsUnderPostmaster backend, BackendRun() computes a
     * high-entropy seed before any user query.  Fewer distinct initial seeds
     * can occur here.
     */
    srandom((unsigned int)(t_thrd.proc_cxt.MyProcPid ^ (unsigned int)t_thrd.proc_cxt.MyStartTime));
    t_thrd.proc_cxt.MyProgName = "Auxiliary";

    /* register thread information to PGPROC structure */
    init_ps_display("Auxiliary", "", "", "");

    /* Validate we have been given a reasonable-looking t_thrd.proc_cxt.DataDir */
    Assert(t_thrd.proc_cxt.DataDir);
    ValidatePgVersion(t_thrd.proc_cxt.DataDir);

    SetProcessingMode(BootstrapProcessing);
    u_sess->attr.attr_common.IgnoreSystemIndexes = true;
    BaseInit();
    BindRedoThreadToSpecifiedCpu(thread_role);
    /*
     * When we are an auxiliary process, we aren't going to do the full
     * InitPostgres pushups, but there are a couple of things that need to get
     * lit up even in an auxiliary process.
     */
    if (thread_role != LOGICAL_READ_RECORD && thread_role != PARALLEL_DECODE) {
        if (IsUnderPostmaster) {
            /*
             * Create a PGPROC so we can use LWLocks.  In the EXEC_BACKEND case,
             * this was already done by SubPostmasterMain().
             */
#ifndef EXEC_BACKEND
            InitAuxiliaryProcess();
#endif

            /*
             * Assign the ProcSignalSlot for an auxiliary process.	Since it
             * doesn't have a BackendId, the slot is statically allocated based on
             * the auxiliary process type (MyAuxProcType).  Backends use slots
             * indexed in the range from 1 to g_instance.shmem_cxt.MaxBackends (inclusive), so we use
             * g_instance.shmem_cxt.MaxBackends + 1 as the base index of the slot for an
             * auxiliary process.
             */
            int index = GetAuxProcEntryIndex(g_instance.shmem_cxt.MaxBackends + 1);

            ProcSignalInit(index);

            /* finish setting up bufmgr.c */
            InitBufferPoolBackend();

            /* register a shutdown callback for LWLock cleanup */
            on_shmem_exit(ShutdownAuxiliaryProcess, 0);
        }
        pgstat_initialize();
        pgstat_bestart();
    }
    /*
     * XLOG operations
     */
    SetProcessingMode(NormalProcessing);

    switch (thread_role) {
        case TWOPASECLEANER:
            TwoPhaseCleanerMain();
            proc_exit(1); /* should never return */
            break;

        case FAULTMONITOR:
            FaultMonitorMain();
            proc_exit(1);
            break;

        case STARTUP:
            /* don't set signals, startup process has its own agenda */
            StartupProcessMain();
            proc_exit(1); /* should never return */
            break;

        case PAGEREDO:
            /* don't set signals, pageredo process has its own agenda */
            MultiRedoMain();
            proc_exit(1); /* should never return */
            break;

        case BGWRITER:
                /* don't set signals, bgwriter has its own agenda */
                BackgroundWriterMain();
                proc_exit(1); /* should never return */
                break;

        case SPBGWRITER:
            invalid_buffer_bgwriter_main();
            proc_exit(1); /* should never return */
            break;

        case LOGICAL_READ_RECORD: {
            LogicalReadWorkerMain(arg->payload);
            proc_exit(1); /* should never return */
        } break;

        case PARALLEL_DECODE: {
            ParallelDecodeWorkerMain(arg->payload);
            proc_exit(1); /* should never return */
        } break;

        case CHECKPOINT_THREAD:
            /* don't set signals, checkpointer has its own agenda */
            CheckpointerMain();
            proc_exit(1); /* should never return */
            break;

        case WALWRITER:
            /* don't set signals, walwriter has its own agenda */
            InitXLOGAccess();
            WalWriterMain();
            proc_exit(1); /* should never return */
            break;

        case WALWRITERAUXILIARY:
            /* don't set signals, walwriterauxiliary has its own agenda */
            WalWriterAuxiliaryMain();
            proc_exit(1); /* should never return */
            break;

        case WALRECEIVER:
            /* don't set signals, walreceiver has its own agenda */
            WalReceiverMain();
            proc_exit(1); /* should never return */
            break;

        case WALRECWRITE:
            /* don't set signals, walrcvwriter has its own agenda */
            walrcvWriterMain(); /* should never return */
            proc_exit(1);
            break;

        case DATARECIVER:
            /* don't set signals, datareceiver has its own agenda */
            DataReceiverMain(); /* should never return */
            proc_exit(1);
            break;

        case DATARECWRITER:
            /* don't set signals, datarcvwriter has its own agenda */
            DataRcvWriterMain(); /* should never return */
            proc_exit(1);
            break;

        case CBMWRITER:
            /* don't set signals, cbmwriter has its own agenda */
            CBMWriterMain(); /* should never return */
            proc_exit(1);
            break;

        case PAGEWRITER_THREAD:
            ckpt_pagewriter_main();
            proc_exit(1);
            break;

        case PAGEREPAIR_THREAD:
            PageRepairMain();
            proc_exit(1);
            break;

        case THREADPOOL_LISTENER:
            TpoolListenerMain(t_thrd.threadpool_cxt.listener);
            proc_exit(1);
            break;

        case THREADPOOL_SCHEDULER:
            TpoolSchedulerMain(t_thrd.threadpool_cxt.scheduler);
            proc_exit(1);
            break;

        case HEARTBEAT:
            heartbeat_main();
            proc_exit(1);
            break;

        case UNDO_RECYCLER:
            undo::UndoRecycleMain();
            proc_exit(1);
            break;
        
        case SHARE_STORAGE_XLOG_COPYER:
            SharedStorageXlogCopyBackendMain();
            proc_exit(1);
            break;
#ifdef ENABLE_MULTIPLE_NODES
        case BARRIER_PREPARSE:
            BarrierPreParseMain();
            proc_exit(1);
            break;
        case TS_COMPACTION:
            CompactionProcess::compaction_main();
            proc_exit(1);
            break;
        case TS_COMPACTION_CONSUMER:
            CompactionWorkerProcess::compaction_consumer_main();
            proc_exit(1);
            break;
        case TS_COMPACTION_AUXILIAY:
            CompactionAuxiliaryProcess::compaction_auxiliary_main();
            proc_exit(1);
            break;
#endif   /* ENABLE_MULTIPLE_NODES */
        default:
            ereport(PANIC, (errmsg("unrecognized process type: %d", (int)t_thrd.bootstrap_cxt.MyAuxProcType)));
            proc_exit(1);
    }

    return 0;
}

static void is_memory_backend_reserved(const knl_thread_arg* arg)
{
    if (arg->role == WORKER) {
        Port port = ((BackendParameters*)(arg->save_para))->port;
        if (processMemInChunks >= maxChunksPerProcess * 0.8 &&
            IsHAPort(&port)) {
            t_thrd.utils_cxt.backend_reserved = true;
        } else {
            t_thrd.utils_cxt.backend_reserved = false;
        }
        return;
    }
    
    switch (arg->role) {
        case WALWRITER:
        case WALRECEIVER:
        case WALRECWRITE:
        case DATARECIVER:
        case DATARECWRITER:
            t_thrd.utils_cxt.backend_reserved = true;
            break;
        default:
            t_thrd.utils_cxt.backend_reserved = false;
            break;
    }
}

template <knl_thread_role thread_role>
int GaussDbThreadMain(knl_thread_arg* arg)
{
    Port port;
    char* p_name_thread = NULL;
    /* Do this sooner rather than later... */
    IsUnderPostmaster = true; /* we are a postmaster subprocess now */
    Assert(thread_role == arg->role);
    /* get child slot from backend_variables */
    t_thrd.child_slot = (arg != NULL) ? ((BackendParameters*)arg->save_para)->MyPMChildSlot : -1;
    /* Check this thread will use reserved memory or not */
    is_memory_backend_reserved(arg);
    /* Initialize the Memory Protection at the thread level */
    gs_memprot_thread_init();
    MemoryContextInit();
    knl_thread_init(thread_role);

    MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
    t_thrd.fake_session = create_session_context(t_thrd.top_mem_cxt, 0);
    t_thrd.fake_session->status = KNL_SESS_FAKE;
    u_sess = t_thrd.fake_session;
#ifdef ENABLE_MULTIPLE_NODES
    /* tsdb compaction need to switch database, so control the session lifecycle */
    if (thread_role == TS_COMPACTION || thread_role == TS_COMPACTION_CONSUMER
        || thread_role == TS_COMPACTION_AUXILIAY) {
        u_sess = SessionControl::create_compaction_session_context(t_thrd.top_mem_cxt);
    }
#endif   /* ENABLE_MULTIPLE_NODES */

    t_thrd.proc_cxt.MyProcPid = gs_thread_self(); /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyStartTime = time(NULL);
    t_thrd.role = arg->role;

    (void)ShowThreadName(GetThreadName(arg->role));
    init_ps_display(GetThreadName(arg->role), "", "", "");

    SetExtraThreadInfo<thread_role>(arg);

    /* Lose the postmaster's on-exit routines (really a no-op) */
    on_exit_reset();

    /* In EXEC_BACKEND case we will not have inherited these settings */
    IsPostmasterEnvironment = true;
    t_thrd.postgres_cxt.whereToSendOutput = DestNone;

    init_plog_global_mem();

    SelfMemoryContext = THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT);

    /* create timer with thread safe */
    if (gs_signal_createtimer() < 0) {
        ereport(FATAL, (errmsg("create timer fail at thread : %lu", t_thrd.proc_cxt.MyProcPid)));
    }

    InitializeGUCOptions();
    /*
     * Set reference point for stack-depth checking
     */
    set_stack_base();

    gs_signal_block_sigusr2();
    gs_signal_startup_siginfo(p_name_thread);
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    PortInitialize(&port, arg);

    if (port.sock != PGINVALID_SOCKET) {
        ereport(DEBUG2, (errmsg("start a new [%d] thread with fd:[%d]", thread_role, port.sock)));
    }

    t_thrd.bn = GetBackend(t_thrd.proc_cxt.MyPMChildSlot);
    /* thread get backend pointer, backend list can be tracked by current thread's t_thrd.bn */
    t_thrd.is_inited = true;

    /* We don't need read GUC variables */
    if (!FencedUDFMasterMode && !PythonFencedMasterModel) {
        /* Read in remaining GUC variables */
        read_nondefault_variables();
    }

    if ((thread_role != WORKER && thread_role != THREADPOOL_WORKER && thread_role != STREAM_WORKER) &&
        u_sess->attr.attr_resource.use_workload_manager && g_instance.attr.attr_resource.enable_backend_control &&
        g_instance.wlm_cxt->gscgroup_init_done) {
        if (thread_role == AUTOVACUUM_WORKER)
            (void)gscgroup_attach_backend_task(GSCGROUP_VACUUM, false);
        else
            (void)gscgroup_attach_backend_task(GSCGROUP_DEFAULT_BACKEND, false);
    }

    t_thrd.wlm_cxt.query_resource_track_mcxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "QueryResourceTrackContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * Reload any libraries that were preloaded by the postmaster.	Since we
     * exec'd this process, those libraries didn't come along with us; but we
     * should load them into all child processes to be consistent with the
     * non-EXEC_BACKEND behavior.
     */
    process_shared_preload_libraries();
    CreateLocalSysDBCache();

    switch (thread_role) {
        case STREAM_WORKER:
        case THREADPOOL_STREAM: {
            /* restore child slot */
            if (thread_role == STREAM_WORKER) {
                t_thrd.proc_cxt.MyPMChildSlot = u_sess->stream_cxt.producer_obj->getChildSlot();
            } else {
                t_thrd.proc_cxt.MyPMChildSlot = t_thrd.threadpool_cxt.stream->GetProducer()->getChildSlot();
            }

            InitProcessAndShareMemory();

            /* And run the backend */
            proc_exit(StreamMain());
        } break;
        case WORKER:
            CheckClientIp(&port); /* For THREADPOOL_WORKER check in InitPort */
            /* fall through */
        case THREADPOOL_WORKER: {
            /* Module load callback */
            pgaudit_agent_init();
            auto_explain_init();
            ledger_hook_init();

            /* unique sql hooks */
            instr_unique_sql_register_hook();

            /* hypopg index hooks */
            hypopg_register_hook();

            /*
             * Perform additional initialization and collect startup packet.
             *
             * We want to do this before InitProcess() for a couple of reasons: 1.
             * so that we aren't eating up a PGPROC slot while waiting on the
             * client. 2. so that if InitProcess() fails due to being out of
             * PGPROC slots, we have already initialized libpq and are able to
             * report the error to the client.
             */
            BackendInitialize(&port);

            InitProcessAndShareMemory();

            /* CN has sent a stop query request. For non-stream node,
             *  we can send sigusr1 to tell the backend  thread.  sigusr1 can not be used until ProcSignalSlots
             *  be initialized in CreateSharedMemoryAndSemaphores.
             */
            if (u_sess->stream_cxt.stop_mythread) {
                t_thrd.sig_cxt.gs_sigale_check_type = SIGNAL_CHECK_EXECUTOR_STOP;
                SendProcSignal(u_sess->stream_cxt.stop_pid, PROCSIG_EXECUTOR_FLAG, InvalidBackendId);
                u_sess->stream_cxt.stop_mythread = false;
                u_sess->stream_cxt.stop_pid = 0;
                u_sess->stream_cxt.stop_query_id = 0;
                proc_exit(0);
            }

            CHECK_FOR_PROCDIEPENDING();

            /* And run the backend */
            proc_exit(BackendRun(&port));
        } break;

        case PAGEREDO:
        case TWOPASECLEANER:
        case FAULTMONITOR:
        case BGWRITER:
        case SPBGWRITER:
        case CHECKPOINT_THREAD:
        case WALWRITER:
        case WALWRITERAUXILIARY:
        case WALRECEIVER:
        case WALRECWRITE:
        case DATARECIVER:
        case DATARECWRITER:
        case CBMWRITER:
        case STARTUP:
        case PAGEWRITER_THREAD:
        case PAGEREPAIR_THREAD:
        case HEARTBEAT:
        case SHARE_STORAGE_XLOG_COPYER:
#ifdef ENABLE_MULTIPLE_NODES
        case BARRIER_PREPARSE:
        case TS_COMPACTION:
        case TS_COMPACTION_CONSUMER:
        case TS_COMPACTION_AUXILIAY:
#endif   /* ENABLE_MULTIPLE_NODES */
        case THREADPOOL_LISTENER:
        case THREADPOOL_SCHEDULER:
        case LOGICAL_READ_RECORD:
        case PARALLEL_DECODE:
        case UNDO_RECYCLER: {
            SetAuxType<thread_role>();
            /* Restore basic shared memory pointers */
            InitShmemAccess(UsedShmemSegAddr);
            /* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
            InitAuxiliaryProcess();
            /* Attach process to shared data structures */
            CreateSharedMemoryAndSemaphores(false, 0);
            GaussDbAuxiliaryThreadMain<thread_role>(arg);
            proc_exit(0);
        } break;

        case AUTOVACUUM_LAUNCHER: {
            InitProcessAndShareMemory();
            AutoVacLauncherMain();
            proc_exit(0);
        } break;

        case AUTOVACUUM_WORKER: {
            InitProcessAndShareMemory();
            AutoVacWorkerMain();
            proc_exit(0);
        } break;

        case JOB_SCHEDULER: {
            InitProcessAndShareMemory();
            JobScheduleMain();
            proc_exit(0);
        } break;

        case JOB_WORKER: {
            InitProcessAndShareMemory();
            JobExecuteWorkerMain();
            proc_exit(0);
        } break;

        case TRACK_STMT_CLEANER: {
            InitProcessAndShareMemory();
            CleanStatementMain();
            proc_exit(0);
        } break;

        case CATCHUP: {
            InitProcessAndShareMemory();
            CatchupMain();
            proc_exit(0);
        } break;

        case WLM_WORKER: {
            t_thrd.role = WLM_WORKER;

            // restore child slot;
            t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
            if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                return STATUS_ERROR;
            }

            /* Restore basic shared memory pointers */
            InitShmemAccess(UsedShmemSegAddr);

            InitProcess();

            WLMWorkerInitialize(&port);
            /* Attach process to shared data structures */
            CreateSharedMemoryAndSemaphores(false, 0);
            WLMProcessThreadMain();
            proc_exit(0);
        } break;

        case WLM_MONITOR: {
            t_thrd.role = WLM_MONITOR;

            /* Restore basic shared memory pointers */
            InitShmemAccess(UsedShmemSegAddr);

            t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
            if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                return STATUS_ERROR;
            }

            /* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
            InitProcess();
            WLMWorkerInitialize(&port);
            /* Attach process to shared data structures */
            CreateSharedMemoryAndSemaphores(false, 0);
            WLMmonitorMain();
            proc_exit(0);
        } break;

        case WLM_ARBITER: {
            t_thrd.role = WLM_ARBITER;

            /* Restore basic shared memory pointers */
            InitShmemAccess(UsedShmemSegAddr);

            t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
            if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                return STATUS_ERROR;
            }

            /* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
            InitProcess();

            WLMWorkerInitialize(&port);

            /* Attach process to shared data structures */
            CreateSharedMemoryAndSemaphores(false, 0);

            WLMarbiterMain();

            proc_exit(0);

        } break;

        case WLM_CPMONITOR: {
            t_thrd.role = WLM_CPMONITOR;

            /* Restore basic shared memory pointers */
            InitShmemAccess(UsedShmemSegAddr);

            t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
            if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                return STATUS_ERROR;
            }

            /* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
            InitProcess();

            WLMWorkerInitialize(&port);

            /* Attach process to shared data structures */
            CreateSharedMemoryAndSemaphores(false, 0);

            CPmonitorMain();

            proc_exit(0);
        } break;

        case ARCH: {
            t_thrd.role = ARCH;

            /* Restore basic shared memory pointers */
            InitShmemAccess(UsedShmemSegAddr);

            t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
            if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                return STATUS_ERROR;
            }

            /* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
            InitProcess();

            /* Attach process to shared data structures */
            CreateSharedMemoryAndSemaphores(false, 0);

            PgArchiverMain(arg);
            proc_exit(0);
        } break;

        case PGSTAT: {
            /* Restore basic shared memory pointers */
            InitShmemAccess(UsedShmemSegAddr);

            /* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
            InitAuxiliaryProcess();

            /* Attach process to shared data structures */
            CreateSharedMemoryAndSemaphores(false, 0);

            /* Do not want to attach to shared memory */
            PgstatCollectorMain();
            proc_exit(0);

        } break;

        case SYSLOGGER: {
            /* Do not want to attach to shared memory */
            SysLoggerMain(arg->extra_payload.log_thread.syslog_handle);
            proc_exit(0);
        } break;

        case ALARMCHECK: {
            AlarmCheckerMain();
            proc_exit(0);
        } break;

        case REAPER: {
            /* Do not want to attach to shared memory */
            ReaperBackendMain();
            proc_exit(0);
        } break;

        case AUDITOR: {
            /* Restore basic shared memory pointers */
            InitShmemAccess(UsedShmemSegAddr);

            /* Need a PGPROC to run CreateSharedMemoryAndSemaphores */
            InitAuxiliaryProcess();

            /* Attach process to shared data structures */
            CreateSharedMemoryAndSemaphores(false, 0);

            PgAuditorMain();
            proc_exit(0);
        } break;

        case TXNSNAP_CAPTURER: {
            InitShmemAccess(UsedShmemSegAddr);

            t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
            if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                return STATUS_ERROR;
            }
            InitProcessAndShareMemory();
            TxnSnapCapturerMain();
            proc_exit(0);
        } break;

        case TXNSNAP_WORKER: {
            InitShmemAccess(UsedShmemSegAddr);
            InitProcessAndShareMemory();
            TxnSnapWorkerMain();
            proc_exit(0);
        } break;

        case RBCLEANER: {
            InitShmemAccess(UsedShmemSegAddr);

            t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
            if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                return STATUS_ERROR;
            }
            InitProcessAndShareMemory();
            RbCleanerMain();
            proc_exit(0);
        } break;

        case RBWORKER: {
            InitShmemAccess(UsedShmemSegAddr);
            InitProcessAndShareMemory();
            RbWorkerMain();
            proc_exit(0);
        } break;

        case SNAPSHOT_WORKER: {
            InitShmemAccess(UsedShmemSegAddr);

            t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
            if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                return STATUS_ERROR;
            }

            InitProcess();
            CreateSharedMemoryAndSemaphores(false, 0);
            SnapshotMain();
        } break;

        case ASH_WORKER: {
            InitShmemAccess(UsedShmemSegAddr);

            t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
            if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                return STATUS_ERROR;
            }

            InitProcess();
            CreateSharedMemoryAndSemaphores(false, 0);
            ActiveSessionCollectMain();
        } break;

        case TRACK_STMT_WORKER: {
            InitShmemAccess(UsedShmemSegAddr);

            t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
            if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                return STATUS_ERROR;
            }

            InitProcess();
            CreateSharedMemoryAndSemaphores(false, 0);
            StatementFlushMain();
        } break;

        case PERCENTILE_WORKER: {
            InitShmemAccess(UsedShmemSegAddr);

            t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
            if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                return STATUS_ERROR;
            }

            InitProcess();
            CreateSharedMemoryAndSemaphores(false, 0);
            PercentileMain();
        } break;

        case COMM_RECEIVER: {
            commReceiverMain(arg->payload);
            proc_exit(0);
        } break;

        case BGWORKER: {
            InitProcessAndShareMemory();
            BackgroundWorkerMain();
            proc_exit(0);
        }
        case COMM_SENDERFLOWER: {
            commSenderFlowMain();
            proc_exit(0);
        } break;

        case COMM_RECEIVERFLOWER: {
            commReceiverFlowMain();
            proc_exit(0);
        } break;

        case COMM_AUXILIARY: {
            commAuxiliaryMain();
            proc_exit(0);
        } break;

        case APPLY_LAUNCHER: {
            t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
            if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                return STATUS_ERROR;
            }
            InitProcessAndShareMemory();
            ApplyLauncherMain();
            proc_exit(0);
        } break;

        case APPLY_WORKER: {
            InitProcessAndShareMemory();
            ApplyWorkerMain();
            proc_exit(0);
        } break;

        case UNDO_LAUNCHER: {
            t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
            if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                return STATUS_ERROR;
            }
            InitProcessAndShareMemory();
            UndoLauncherMain();
            proc_exit(0);
        } break;

        case UNDO_WORKER: {
            InitProcessAndShareMemory();
            UndoWorkerMain();
            proc_exit(0);
        } break;

        case GLOBALSTATS_THREAD: {
            t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
            if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                return STATUS_ERROR;
            }
            InitProcessAndShareMemory();
            GlobalStatsTrackerMain();
            proc_exit(0);
        }

#ifndef ENABLE_LITE_MODE
        case BARRIER_CREATOR: {
            if (START_BARRIER_CREATOR) {
                t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
                if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                    return STATUS_ERROR;
                }
                InitProcessAndShareMemory();
                barrier_creator_main();
                proc_exit(0);
            }
       } break;

        case BARRIER_ARCH: {
            if (START_BARRIER_CREATOR) {
                t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
                if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                    return STATUS_ERROR;
                }
                InitProcessAndShareMemory();
                BarrierArchMain(arg);
                proc_exit(0);
            }
       } break;
#endif

#ifdef ENABLE_MULTIPLE_NODES
        case COMM_POOLER_CLEAN: {
            InitProcessAndShareMemory();
            commPoolCleanerMain();
            proc_exit(0);
        } break;

        case STREAMING_BACKEND: {
            t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
            if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                return STATUS_ERROR;
            }

            InitProcessAndShareMemory();
            streaming_backend_main(arg);
            proc_exit(0);
       } break;

       case CSNMIN_SYNC: {
            if (GTM_LITE_CN) {
                t_thrd.proc_cxt.MyPMChildSlot = AssignPostmasterChildSlot();
                if (t_thrd.proc_cxt.MyPMChildSlot == -1) {
                    return STATUS_ERROR;
                }

                InitProcessAndShareMemory();
                csnminsync_main();
                proc_exit(0);
            }
        } break;
#endif
        default:
            ereport(PANIC, (errmsg("unsupport thread role type %d", arg->role)));
            break;
    }

    return 1;
}

/* the order of role must be the same with enum knl_thread_role */
static ThreadMetaData GaussdbThreadGate[] = {
    { GaussDbThreadMain<MASTER_THREAD>, MASTER_THREAD, "main", "main thread" },
    { GaussDbThreadMain<WORKER>, WORKER, "worker", "woker thread" },
    { GaussDbThreadMain<THREADPOOL_WORKER>, THREADPOOL_WORKER, "TPLworker", "thread pool worker" },
    { GaussDbThreadMain<THREADPOOL_LISTENER>, THREADPOOL_LISTENER, "TPLlistener", "thread pool listner" },
    { GaussDbThreadMain<THREADPOOL_SCHEDULER>, THREADPOOL_SCHEDULER, "TPLscheduler", "thread pool scheduler" },
    { GaussDbThreadMain<THREADPOOL_STREAM>, THREADPOOL_STREAM, "TPLstream", "thread pool stream" },
    { GaussDbThreadMain<STREAM_WORKER>, STREAM_WORKER, "streamworker", "stream worker" },
    { GaussDbThreadMain<AUTOVACUUM_LAUNCHER>, AUTOVACUUM_LAUNCHER, "AVClauncher", "autovacuum launcher" },
    { GaussDbThreadMain<AUTOVACUUM_WORKER>, AUTOVACUUM_WORKER, "AVCworker", "autovacuum worker" },
    { GaussDbThreadMain<JOB_SCHEDULER>, JOB_SCHEDULER, "Jobscheduler", "job scheduler" },
    { GaussDbThreadMain<JOB_WORKER>, JOB_WORKER, "Jobworker", "job execute worker" },
    { GaussDbThreadMain<WLM_WORKER>, WLM_WORKER, "WLMworker", "wlm statistics collector" },
    { GaussDbThreadMain<WLM_MONITOR>, WLM_MONITOR, "WLMmonitor", "wlm monitor launcher" },
    { GaussDbThreadMain<WLM_ARBITER>, WLM_ARBITER, "WLMarbiter", "wlm arbiter launcher" },
    { GaussDbThreadMain<WLM_CPMONITOR>, WLM_CPMONITOR, "CPmonitor", "CPmonitor launcher" },
    { GaussDbThreadMain<AUDITOR>, AUDITOR, "auditor", "system auditor" },
    { GaussDbThreadMain<PGSTAT>, PGSTAT, "statscollector", "statistics collector" },
    { GaussDbThreadMain<SYSLOGGER>, SYSLOGGER, "syslogger", "system logger" },
    { GaussDbThreadMain<CATCHUP>, CATCHUP, "catchup", "catchup" },
    { GaussDbThreadMain<ARCH>, ARCH, "archiver", "archiver" },
    { GaussDbThreadMain<ALARMCHECK>, ALARMCHECK, "alarm", "alarm" },
    { GaussDbThreadMain<REAPER>, REAPER, "reaper", "reaper backend" },
    { GaussDbThreadMain<PAGEREDO>, PAGEREDO, "pageredo", "page redo" },
    { GaussDbThreadMain<TWOPASECLEANER>, TWOPASECLEANER, "2pccleaner", "twophase cleaner" },
    { GaussDbThreadMain<STARTUP>, STARTUP, "startup", "startup" },
    { GaussDbThreadMain<FAULTMONITOR>, FAULTMONITOR, "faultmonitor", "fault monitor" },
    { GaussDbThreadMain<BGWRITER>, BGWRITER, "bgwriter", "background writer" },
    { GaussDbThreadMain<SPBGWRITER>, SPBGWRITER, "Spbgwriter", "invalidate buffer bg writer" },
    { GaussDbThreadMain<PERCENTILE_WORKER>, PERCENTILE_WORKER, "percentworker", "statistics collector" },
    { GaussDbThreadMain<TXNSNAP_CAPTURER>, TXNSNAP_CAPTURER, "txnsnapcapturer", "transaction snapshot capturer" },
    { GaussDbThreadMain<TXNSNAP_WORKER>, TXNSNAP_WORKER, "txnsnapworker", "transaction snapshot worker" },
    { GaussDbThreadMain<RBCLEANER>, RBCLEANER, "rbcleaner", "rbcleaner" },
    { GaussDbThreadMain<RBWORKER>, RBWORKER, "rbworker", "rbworker" },
    { GaussDbThreadMain<SNAPSHOT_WORKER>, SNAPSHOT_WORKER, "snapshotworker", "snapshot" },
    { GaussDbThreadMain<ASH_WORKER>, ASH_WORKER, "ashworker", "ash worker" },
    { GaussDbThreadMain<TRACK_STMT_WORKER>, TRACK_STMT_WORKER, "TrackStmtWorker", "track stmt worker" },
    { GaussDbThreadMain<TRACK_STMT_CLEANER>, TRACK_STMT_CLEANER, "TrackStmtClean", "track stmt clean worker" },
    { GaussDbThreadMain<CHECKPOINT_THREAD>, CHECKPOINT_THREAD, "checkpointer", "checkpointer" },
    { GaussDbThreadMain<WALWRITER>, WALWRITER, "WALwriter", "WAL writer" },
    { GaussDbThreadMain<WALWRITERAUXILIARY>, WALWRITERAUXILIARY, "WALwriteraux", "WAL writer auxiliary" },
    { GaussDbThreadMain<WALRECEIVER>, WALRECEIVER, "WALreceiver", "WAL receiver" },
    { GaussDbThreadMain<WALRECWRITE>, WALRECWRITE, "WALrecwriter", "WAL receive writer" },
    { GaussDbThreadMain<DATARECIVER>, DATARECIVER, "datareceiver", "data receiver" },
    { GaussDbThreadMain<DATARECWRITER>, DATARECWRITER, "datarecwriter", "data receive writer" },
    { GaussDbThreadMain<CBMWRITER>, CBMWRITER, "CBMwriter", "CBM writer" },
    { GaussDbThreadMain<PAGEWRITER_THREAD>, PAGEWRITER_THREAD, "pagewriter", "page writer" },
    { GaussDbThreadMain<PAGEREPAIR_THREAD>, PAGEREPAIR_THREAD, "pagerepair", "page repair" },
    { GaussDbThreadMain<HEARTBEAT>, HEARTBEAT, "heartbeat", "heart beat" },
    { GaussDbThreadMain<COMM_SENDERFLOWER>, COMM_SENDERFLOWER, "COMMsendflow", "communicator sender flower" },
    { GaussDbThreadMain<COMM_RECEIVERFLOWER>, COMM_RECEIVERFLOWER, "COMMrecvflow", "communicator receiver flower" },
    { GaussDbThreadMain<COMM_RECEIVER>, COMM_RECEIVER, "COMMrecloop", "communicator receiver loop" },
    { GaussDbThreadMain<COMM_AUXILIARY>, COMM_AUXILIARY, "COMMaux", "communicator auxiliary" },
    { GaussDbThreadMain<COMM_POOLER_CLEAN>, COMM_POOLER_CLEAN, "COMMpoolcleaner", "communicator pooler auto cleaner" },
    { GaussDbThreadMain<LOGICAL_READ_RECORD>, LOGICAL_READ_RECORD, "LogicalRead", "LogicalRead pooler auto cleaner" },
    { GaussDbThreadMain<PARALLEL_DECODE>, PARALLEL_DECODE, "COMMpoolcleaner", "communicator pooler auto cleaner" },
    { GaussDbThreadMain<UNDO_RECYCLER>, UNDO_RECYCLER, "undorecycler", "undo recycler" },
    { GaussDbThreadMain<UNDO_LAUNCHER>, UNDO_LAUNCHER, "asyncundolaunch", "async undo launcher" },
    { GaussDbThreadMain<UNDO_WORKER>, UNDO_WORKER, "asyncundoworker", "async undo worker" },
    { GaussDbThreadMain<CSNMIN_SYNC>, CSNMIN_SYNC, "csnminsync", "csnmin sync" },
    { GaussDbThreadMain<GLOBALSTATS_THREAD>, GLOBALSTATS_THREAD, "globalstats", "global stats" },
    { GaussDbThreadMain<BARRIER_CREATOR>, BARRIER_CREATOR, "barriercreator", "barrier creator" },
    { GaussDbThreadMain<BGWORKER>, BGWORKER, "bgworker", "background worker" },
    { GaussDbThreadMain<BARRIER_ARCH>, BARRIER_ARCH, "barrierarch", "barrier arch" },
    { GaussDbThreadMain<SHARE_STORAGE_XLOG_COPYER>, SHARE_STORAGE_XLOG_COPYER, "xlogcopyer", "xlog copy backend" },
    { GaussDbThreadMain<APPLY_LAUNCHER>, APPLY_LAUNCHER, "applylauncher", "apply launcher" },
    { GaussDbThreadMain<APPLY_WORKER>, APPLY_WORKER, "applyworker", "apply worker" },

    /* Keep the block in the end if it may be absent !!! */
#ifdef ENABLE_MULTIPLE_NODES
    { GaussDbThreadMain<BARRIER_PREPARSE>, BARRIER_PREPARSE, "barrierpreparse", "barrier preparse backend" },
    { GaussDbThreadMain<TS_COMPACTION>, TS_COMPACTION, "TScompaction",
      "timeseries compaction" },
    { GaussDbThreadMain<TS_COMPACTION_CONSUMER>, TS_COMPACTION_CONSUMER, "TScompconsumer",
      "timeseries consumer compaction" },
    { GaussDbThreadMain<TS_COMPACTION_AUXILIAY>, TS_COMPACTION_AUXILIAY, "TScompaux",
      "compaction auxiliary" },
    { GaussDbThreadMain<STREAMING_BACKEND>, STREAMING_BACKEND, "streambackend",
      "streaming backend" },
    { GaussDbThreadMain<STREAMING_ROUTER_BACKEND>, STREAMING_ROUTER_BACKEND, "streamrouter",
      "streaming router backend" },
    { GaussDbThreadMain<STREAMING_WORKER_BACKEND>, STREAMING_WORKER_BACKEND, "streamworker",
      "streaming worker backend" },
    { GaussDbThreadMain<STREAMING_COLLECTOR_BACKEND>, STREAMING_COLLECTOR_BACKEND, "streamcollector",
      "streaming collector backend" },
    { GaussDbThreadMain<STREAMING_QUEUE_BACKEND>, STREAMING_QUEUE_BACKEND, "streamqueue",
      "streaming queue backend" },
    { GaussDbThreadMain<STREAMING_REAPER_BACKEND>, STREAMING_REAPER_BACKEND, "streamreaper",
      "streaming reaper backend" }
#endif   /* ENABLE_MULTIPLE_NODES */
};

GaussdbThreadEntry GetThreadEntry(knl_thread_role role)
{
    Assert(role > MASTER_THREAD && role < THREAD_ENTRY_BOUND);
    Assert(GaussdbThreadGate[static_cast<int>(role)].role == role);

    return GaussdbThreadGate[static_cast<int>(role)].func;
}

const char* GetThreadName(knl_thread_role role)
{
    Assert(role > MASTER_THREAD && role < THREAD_ENTRY_BOUND);
    Assert(GaussdbThreadGate[static_cast<int>(role)].role == role);
    /* pthread_setname_np requires thread name is no longer than 16 including the ending '\0' */
    Assert(strlen(GaussdbThreadGate[static_cast<int>(role)].thr_name) < 16);

    return GaussdbThreadGate[static_cast<int>(role)].thr_name;
}

static void* InternalThreadFunc(void* args)
{
    knl_thread_arg* thr_argv = (knl_thread_arg*)args;

    gs_thread_exit((GetThreadEntry(thr_argv->role))(thr_argv));
    return (void*)NULL;
}

ThreadId initialize_thread(ThreadArg* thr_argv)
{

    gs_thread_t thread;
    int error_code = gs_thread_create(&thread, InternalThreadFunc, 1, (void*)thr_argv);
    if (error_code != 0) {
        ereport(LOG,
            (errmsg("can not fork thread[%s], errcode:%d, %m",
             GetThreadName(thr_argv->m_thd_arg.role), error_code)));
        gs_thread_release_args_slot(thr_argv);
        return InvalidTid;
    }

    return gs_thread_id(thread);
}

ThreadId initialize_util_thread(knl_thread_role role, void* payload)
{
    ThreadArg* thr_argv = gs_thread_get_args_slot();
    if (thr_argv == NULL) {
        return 0;
    }
    thr_argv->m_thd_arg.role = role;
    thr_argv->m_thd_arg.payload = payload;
    Port port;
    ThreadId pid;
    errno_t rc;
    /* This entry point passes dummy values for the Port variables */
    rc = memset_s(&port, sizeof(port), 0, sizeof(port));
    securec_check(rc, "", "");
    port.sock = PGINVALID_SOCKET;
    port.SessionVersionNum = pg_atomic_read_u32(&WorkingGrandVersionNum);

    if (!save_backend_variables((BackendParameters*)(thr_argv->m_thd_arg.save_para), &port)) {
        gs_thread_release_args_slot(thr_argv);
        return 0; /* log made by save_backend_variables */
    }

    if (role == SYSLOGGER) {
        if (t_thrd.logger.syslogFile != NULL)
            thr_argv->m_thd_arg.extra_payload.log_thread.syslog_handle = fileno(t_thrd.logger.syslogFile);
        else
            thr_argv->m_thd_arg.extra_payload.log_thread.syslog_handle = -1;
    }

    pid = initialize_thread(thr_argv);

    if (pid == InvalidTid) {
        /*
         * fork failure is fatal during startup, but there's no need to choke
         * immediately if starting other child types fails.
         */
        if (role == STARTUP)
            ExitPostmaster(1);
        return 0;
    }
    return pid;
}

ThreadId initialize_worker_thread(knl_thread_role role, Port* port, void* payload)
{
    ThreadArg* thr_argv = gs_thread_get_args_slot();
    if (thr_argv == NULL) {
        return InvalidTid;
    }

    thr_argv->m_thd_arg.role = role;
    thr_argv->m_thd_arg.payload = payload;

    /*
     * We initialize the backend version to be the same as
     * postmaster, which should be the case at most of the time.
     */
    port->SessionVersionNum = pg_atomic_read_u32(&WorkingGrandVersionNum);

    if (!save_backend_variables((BackendParameters*)(thr_argv->m_thd_arg.save_para), port)) {
        gs_thread_release_args_slot(thr_argv);
        return InvalidTid; /* log made by save_backend_variables */
    }

    return initialize_thread(thr_argv);
}


static bool isVaildIpv6(const char* ip)
{
    struct sockaddr_storage addr;
    errno_t rc = 0;
    char* ipNoZone = NULL;
    char ipNoZoneData[IP_LEN] = {0};

    /* remove any '%zone' part from an IPv6 address string */
    ipNoZone = remove_ipv6_zone((char *)ip, ipNoZoneData, IP_LEN);

    rc = memset_s(&addr, sizeof(addr), 0, sizeof(addr));
    securec_check(rc, "", "");
    addr.ss_family = AF_INET6;
    rc = inet_pton(AF_INET6, ipNoZone, &(((struct sockaddr_in6 *) &addr)->sin6_addr));
    if (rc <= 0) {
        return false;
    }
    return true;
}

bool isVaildIpv4(const char* ip)
{
    int dots = 0;
    int setions = 0;

    if (NULL == ip || *ip == '.') {
        return false;
    }

    while (*ip) {
        if (*ip == '.') {
            dots++;
            if (setions >= 0 && setions <= 255) {
                setions = 0;
                ip++;
                continue;
            }
            return false;
        } else if (*ip >= '0' && *ip <= '9') {
            setions = setions * 10 + (*ip - '0');
        } else {
            return false;
        }
        ip++;
    }

    if (setions >= 0 && setions <= 255) {
        if (dots == 3) {
            return true;
        }
    }

    return false;
}

static bool isVaildIp(const char* ip)
{
    bool ret = false;
    if (NULL == ip) {
        return false;
    }

    if (strchr(ip, ':') != NULL) {
        return isVaildIpv6(ip);
    } else {
        return isVaildIpv4(ip);
    }

    return ret;
}

/*
 * set disable_conn_primary, deny connection to this node.
 */
Datum disable_conn(PG_FUNCTION_ARGS)
{
    knl_g_disconn_node_context_data disconn_node;
    text* arg0 = (text*)PG_GETARG_DATUM(0);
    text* arg1 = (text*)PG_GETARG_DATUM(1);
    bool redoDone = false;
    int checkTimes = CHECK_TIMES;
    if (arg0 == NULL) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_ATTRIBUTE), errmsg("Invalid null pointer attribute for disable_conn()")));
    }
    char* host = NULL;

    if (GetArchiveRecoverySlot() && IsServerModeStandby()) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("can not execute in obs recovery mode")));
    }

    const char* disconn_mode = TextDatumGetCString(arg0);
    if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("must be superuser/sysadmin account to perform disable_conn()")));
    }
    ValidateName(disconn_mode);

    if (0 == strcmp(disconn_mode, POLLING_CONNECTION_STR)) {
        disconn_node.conn_mode = POLLING_CONNECTION;
    } else if (0 == strcmp(disconn_mode, SPECIFY_CONNECTION_STR)) {
        disconn_node.conn_mode = SPECIFY_CONNECTION;
    } else if (0 == strcmp(disconn_mode, PROHIBIT_CONNECTION_STR)) {
        disconn_node.conn_mode = PROHIBIT_CONNECTION;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("Connection mode should be polling_connection or specify_connection or prohibit_connection")));
    }

    /*
     * Make sure that all xlog has been redo before locking.
     * Sleep 0.5s is an auxiliary way to check whether all xlog has been redone.
     */
    if (disconn_node.conn_mode == PROHIBIT_CONNECTION) {
        uint32 conn_mode = pg_atomic_read_u32(&g_instance.comm_cxt.localinfo_cxt.need_disable_connection_node);
        while (checkTimes--) {
            if (knl_g_get_redo_finish_status()) {
                redoDone = true;
                break;
            }
            ereport(LOG, (errmsg("%d redo_done", redoDone)));
            sleep(0.01);
        }
        ereport(LOG, (errmsg("%d redo_done", redoDone)));
        if (!redoDone) {
            if (!conn_mode) {
                pg_atomic_write_u32(&g_instance.comm_cxt.localinfo_cxt.need_disable_connection_node, true);
                // clean redo done
                pg_atomic_write_u32(&t_thrd.walreceiverfuncs_cxt.WalRcv->rcvDoneFromShareStorage, false);
            }
            ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not add lock when DN is not redo all xlog, redo done flag is false")));
        }

        XLogRecPtr replay1 = GetXLogReplayRecPtrInPending();
        sleep(0.5);
        XLogRecPtr replay2 = GetXLogReplayRecPtrInPending();
        if (replay1 != replay2) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not add lock when DN is not redo all xlog.")));
        }
    } else {
        pg_atomic_write_u32(&g_instance.comm_cxt.localinfo_cxt.need_disable_connection_node, false);
    }

    if (disconn_node.conn_mode != SPECIFY_CONNECTION) {
        disconn_node.disable_conn_node_host[0] = '\0';
    } else {
        if (arg1 == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_ATTRIBUTE), errmsg("Invalid null pointer attribute for disable_conn()")));
        }
        host = TextDatumGetCString(arg1);
        ValidateName(host);
        if (!isVaildIp(host)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("host is invalid")));
        }
        clean_ipv6_addr(AF_INET6, host);
        errno_t rc = memcpy_s(disconn_node.disable_conn_node_host, NAMEDATALEN, host, strlen(host) + 1);
        securec_check(rc, "\0", "\0");
    }
    if (disconn_node.conn_mode != SPECIFY_CONNECTION) {
        disconn_node.disable_conn_node_port = 0;
    } else {
        disconn_node.disable_conn_node_port = PG_GETARG_INT32(2);
    }
    int fd;
    fd = BasicOpenFile(disable_conn_file, O_CREAT | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", disable_conn_file)));
    }

    /*
     * save primary node info on disk.
     */
    pgstat_report_waitevent(WAIT_EVENT_DISABLE_CONNECT_FILE_WRITE);
    if (write(fd, &disconn_node, sizeof(disconn_node)) != sizeof(disconn_node)) {
        pgstat_report_waitevent(WAIT_EVENT_END);
        close(fd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", disable_conn_file)));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    pgstat_report_waitevent(WAIT_EVENT_DISABLE_CONNECT_FILE_SYNC);
    if (pg_fsync(fd) != 0) {
        pgstat_report_waitevent(WAIT_EVENT_END);
        close(fd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", disable_conn_file)));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);
    close(fd);

    SpinLockAcquire(&g_instance.comm_cxt.localinfo_cxt.disable_conn_node.info_lck);
    g_instance.comm_cxt.localinfo_cxt.disable_conn_node.disable_conn_node_data = disconn_node;
    SpinLockRelease(&g_instance.comm_cxt.localinfo_cxt.disable_conn_node.info_lck);
    ereport(LOG, (errcode(ERRCODE_LOG), errmsg("disable_conn set mode to %s and host is %s port is %d", disconn_mode,
        host, disconn_node.disable_conn_node_port)));
    PG_RETURN_VOID();
}

/*
 * return disable_conn_primary info.
 */
Datum read_disable_conn_file(PG_FUNCTION_ARGS)
{
    TupleDesc tupdesc;
    Datum values[6];
    bool nulls[6];
    HeapTuple tuple;
    errno_t rc;
    Datum result;
    char disconn_node_port_str[NAMEDATALEN];
    char* next_key = NULL;
    char* key_position = NULL;
    char local_host[NAMEDATALEN];
    char local_port[NAMEDATALEN];
    char local_info[DOUBLE_NAMEDATALEN];
    int max_local_address_length = DOUBLE_NAMEDATALEN - 1;
    rc = memset_s(local_info, DOUBLE_NAMEDATALEN, 0, DOUBLE_NAMEDATALEN);
    securec_check(rc, "\0", "\0");
    if (u_sess->attr.attr_storage.ReplConnInfoArr[1] == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("Can't get local connection address.")));
    }
    max_local_address_length = pg_mbcliplen(u_sess->attr.attr_storage.ReplConnInfoArr[1],
                                            strlen(u_sess->attr.attr_storage.ReplConnInfoArr[1]),
                                            max_local_address_length);
    rc = memcpy_s(local_info, DOUBLE_NAMEDATALEN  - 1,
                  u_sess->attr.attr_storage.ReplConnInfoArr[1], max_local_address_length);
    securec_check(rc, "\0", "\0");
    key_position = strtok_s(local_info, " ", &next_key);
    if (key_position == NULL || sscanf_s(key_position, "localhost=%s", local_host, NAMEDATALEN - 1) != 1) {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("get local host failed!")));
    }
    clean_ipv6_addr(AF_INET6, local_host);

    key_position = strtok_s(NULL, " ", &next_key);
    if (key_position == NULL || sscanf_s(key_position, "localport=%s", local_port, NAMEDATALEN - 1) != 1) {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("get local port failed!")));
    }

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("return type must be a row type")));

    SpinLockAcquire(&g_instance.comm_cxt.localinfo_cxt.disable_conn_node.info_lck);
    switch (g_instance.comm_cxt.localinfo_cxt.disable_conn_node.disable_conn_node_data.conn_mode) {
        case POLLING_CONNECTION:
            values[0] = CStringGetTextDatum(POLLING_CONNECTION_STR);
            break;
        case SPECIFY_CONNECTION:
            values[0] = CStringGetTextDatum(SPECIFY_CONNECTION_STR);
            break;
        case PROHIBIT_CONNECTION:
            values[0] = CStringGetTextDatum(PROHIBIT_CONNECTION_STR);
            break;
    }
    rc = snprintf_s(disconn_node_port_str,
        sizeof(disconn_node_port_str),
        sizeof(disconn_node_port_str) - 1,
        "%d",
        g_instance.comm_cxt.localinfo_cxt.disable_conn_node.disable_conn_node_data.disable_conn_node_port);
    securec_check_ss_c(rc, "\0", "\0");
    values[1] = CStringGetTextDatum(
        g_instance.comm_cxt.localinfo_cxt.disable_conn_node.disable_conn_node_data.disable_conn_node_host);
    values[2] = CStringGetTextDatum(disconn_node_port_str);
    values[3] = CStringGetTextDatum(local_host);
    values[4] = CStringGetTextDatum(local_port);
    if (knl_g_get_redo_finish_status()) {
        values[5] = CStringGetTextDatum("true");
    } else {
        values[5] = CStringGetTextDatum("false");
    }
    SpinLockRelease(&g_instance.comm_cxt.localinfo_cxt.disable_conn_node.info_lck);
    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);
    PG_RETURN_DATUM(result);
}

void set_disable_conn_mode()
{
    int fd = 0;
    size_t cnt = 0;
    knl_g_disconn_node_context_data* disconn_node =
        (knl_g_disconn_node_context_data*)palloc(sizeof(knl_g_disconn_node_context_data));

    fd = BasicOpenFile(disable_conn_file, O_RDONLY | PG_BINARY, 0);

    /*
     * We do not need to handle this as we are rename()ing the directory into
     * place only after we fsync()ed the state file.
     */
    if (fd >= 0) {
        pgstat_report_waitevent(WAIT_EVENT_DISABLE_CONNECT_FILE_SYNC);
        if (pg_fsync(fd) != 0) {
            pgstat_report_waitevent(WAIT_EVENT_END);
            close(fd);
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not fsync file \"%s\": %m", disable_conn_file)));
        }
        pgstat_report_waitevent(WAIT_EVENT_END);

        pgstat_report_waitevent(WAIT_EVENT_DISABLE_CONNECT_FILE_READ);

        cnt = read(fd, (void*)disconn_node, sizeof(knl_g_disconn_node_context_data));

        close(fd);
        fd = 0;
        if (cnt != sizeof(knl_g_disconn_node_context_data)) {
            ereport(ERROR, (0, errmsg("cannot read disable connection file: \"%s\" \n", disable_conn_file)));
        }

        pgstat_report_waitevent(WAIT_EVENT_END);

        SpinLockAcquire(&g_instance.comm_cxt.localinfo_cxt.disable_conn_node.info_lck);
        g_instance.comm_cxt.localinfo_cxt.disable_conn_node.disable_conn_node_data.conn_mode = disconn_node->conn_mode;
        g_instance.comm_cxt.localinfo_cxt.disable_conn_node.disable_conn_node_data.disable_conn_node_port =
            disconn_node->disable_conn_node_port;

        errno_t rc = memcpy_s(
            (void*)g_instance.comm_cxt.localinfo_cxt.disable_conn_node.disable_conn_node_data.disable_conn_node_host,
            NAMEDATALEN,
            (void*)disconn_node->disable_conn_node_host,
            NAMEDATALEN);
        securec_check(rc, "\0", "\0");
        SpinLockRelease(&g_instance.comm_cxt.localinfo_cxt.disable_conn_node.info_lck);
    } else {
        SpinLockAcquire(&g_instance.comm_cxt.localinfo_cxt.disable_conn_node.info_lck);
        g_instance.comm_cxt.localinfo_cxt.disable_conn_node.disable_conn_node_data.conn_mode = POLLING_CONNECTION;
        g_instance.comm_cxt.localinfo_cxt.disable_conn_node.disable_conn_node_data.disable_conn_node_host[0] = 0;
        g_instance.comm_cxt.localinfo_cxt.disable_conn_node.disable_conn_node_data.disable_conn_node_port = 0;
        SpinLockRelease(&g_instance.comm_cxt.localinfo_cxt.disable_conn_node.info_lck);
    }
    pfree_ext(disconn_node);
    return;
}

static bool NeedHeartbeat()
{
    /* heartbeat is no longer needed on DCF mode */
    if (g_instance.attr.attr_storage.dcf_attr.enable_dcf)
        return false;

    if (!(IS_PGXC_DATANODE && g_instance.pid_cxt.HeartbeatPID == 0 &&
            (pmState == PM_RUN || pmState == PM_HOT_STANDBY) &&
            (t_thrd.postmaster_cxt.HaShmData->current_mode == PRIMARY_MODE ||
                t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE)) ||
        dummyStandbyMode) {
        return false;
    }

    struct replconninfo* replconn = NULL;
    /* at least one replconninfo configures heartbeat port */
    for (int i = 1; i < MAX_REPLNODE_NUM; i++) {
        replconn = t_thrd.postmaster_cxt.ReplConnArray[i];
        if (replconn != NULL && replconn->localheartbeatport != 0 && replconn->remoteheartbeatport != 0) {
            return true;
        }
    }
    return false;
}

/* Current mode must be get in this function */
ServerMode GetHaShmemMode(void)
{
    volatile HaShmemData* hashmdata = t_thrd.postmaster_cxt.HaShmData;
    ServerMode tmpMode;
    Assert(t_thrd.postmaster_cxt.HaShmData != NULL);
    SpinLockAcquire(&hashmdata->mutex);
    tmpMode = hashmdata->current_mode;
    SpinLockRelease(&hashmdata->mutex);
    return tmpMode;
}

void GenerateCancelKey(bool isThreadPoolSession)
{
    if (isThreadPoolSession) {
        u_sess->cancel_key = gs_random();
        u_sess->cancel_key = (unsigned long)u_sess->cancel_key & ~0x1;
    } else {
        /*
         * Compute the cancel key that will be assigned to this backend. The
         * backend will have its own copy in the forked-off process' value of
         * t_thrd.proc_cxt.MyCancelKey, so that it can transmit the key to the frontend.
         */
        t_thrd.proc_cxt.MyCancelKey = PostmasterRandom();
        t_thrd.proc_cxt.MyCancelKey = (unsigned long)t_thrd.proc_cxt.MyCancelKey | 0x1;
    }
}
#ifndef ENABLE_MULTIPLE_NODES
void InitShmemForDcfCallBack()
{
    Port port;
    errno_t rc = 0;
    rc = memset_s(&port, sizeof(port), 0, sizeof(port));
    securec_check(rc, "\0", "\0");
    port.sock = PGINVALID_SOCKET;
    InitializeGUCOptions();
    restore_backend_variables(BackendVariablesGlobal, &port);
    read_nondefault_variables();
    InitProcessAndShareMemory();
}
#endif

