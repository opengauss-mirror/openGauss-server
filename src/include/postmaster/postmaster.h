/* -------------------------------------------------------------------------
 *
 * postmaster.h
 *	  Exports from postmaster/postmaster.c.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/postmaster/postmaster.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef _POSTMASTER_H
#define _POSTMASTER_H

extern THR_LOCAL bool comm_client_bind;

extern bool FencedUDFMasterMode;

/* the logicTid index for nonProc thread*/
#define POSTMASTER_LID 1
#define PGSTAT_LID 2
#define PGARCH_LID 3
#define SYSLOGGER_LID 4

/* Startup/shutdown state */
#define NoShutdown 0
#define SmartShutdown 1
#define FastShutdown 2
#define ImmediateShutdown 3

const int ReaperLogBufSize = 1024; /* reaper function log buffer size */

extern volatile int Shutdown;

extern uint32 noProcLogicTid;
extern THR_LOCAL bool stop_mythread;
extern THR_LOCAL ThreadId stop_threadid;

extern pthread_mutex_t bt_lock;
extern pthread_rwlock_t hba_rwlock;

typedef enum ReplicationType {
    RT_WITH_DUMMY_STANDBY,
    RT_WITH_MULTI_STANDBY,
    RT_WITHOUT_STANDBY,
    RT_NUM
} ReplicationType;

#define IS_DN_MULTI_STANDYS_MODE() (g_instance.attr.attr_storage.replication_type == RT_WITH_MULTI_STANDBY)
#define IS_DN_DUMMY_STANDYS_MODE() (g_instance.attr.attr_storage.replication_type == RT_WITH_DUMMY_STANDBY)
#define IS_DN_WITHOUT_STANDBYS_MODE() (g_instance.attr.attr_storage.replication_type == RT_WITHOUT_STANDBY)

/*
 * We use a simple state machine to control startup, shutdown, and
 * crash recovery (which is rather like shutdown followed by startup).
 *
 * After doing all the postmaster initialization work, we enter PM_STARTUP
 * state and the startup process is launched. The startup process begins by
 * reading the control file and other preliminary initialization steps.
 * In a normal startup, or after crash recovery, the startup process exits
 * with exit code 0 and we switch to PM_RUN state.	However, archive recovery
 * is handled specially since it takes much longer and we would like to support
 * hot standby during archive recovery.
 *
 * When the startup process is ready to start archive recovery, it signals the
 * postmaster, and we switch to PM_RECOVERY state. The background writer and
 * checkpointer are launched, while the startup process continues applying WAL.
 * If Hot Standby is enabled, then, after reaching a consistent point in WAL
 * redo, startup process signals us again, and we switch to PM_HOT_STANDBY
 * state and begin accepting connections to perform read-only queries.	When
 * archive recovery is finished, the startup process exits with exit code 0
 * and we switch to PM_RUN state.
 *
 * Normal child backends can only be launched when we are in PM_RUN or
 * PM_HOT_STANDBY state.  (We also allow launch of normal
 * child backends in PM_WAIT_BACKUP state, but only for superusers.)
 * In other states we handle connection requests by launching "dead_end"
 * child processes, which will simply send the client an error message and
 * quit.  (We track these in the g_instance.backend_list so that we can know when they
 * are all gone; this is important because they're still connected to shared
 * memory, and would interfere with an attempt to destroy the shmem segment,
 * possibly leading to SHMALL failure when we try to make a new one.)
 * In PM_WAIT_DEAD_END state we are waiting for all the dead_end children
 * to drain out of the system, and therefore stop accepting connection
 * requests at all until the last existing child has quit (which hopefully
 * will not be very long).
 *
 * Notice that this state variable does not distinguish *why* we entered
 * states later than PM_RUN --- g_instance.status and g_instance.fatal_error must be consulted
 * to find that out.  g_instance.fatal_error is never true in PM_RECOVERY_* or PM_RUN
 * states, nor in PM_SHUTDOWN states (because we don't enter those states
 * when trying to recover from a crash).  It can be true in PM_STARTUP state,
 * because we don't clear it until we've successfully started WAL redo.
 * Similarly, RecoveryError means that we have crashed during recovery, and
 * should not try to restart.
 */
typedef enum {
    PM_INIT,          /* postmaster starting */
    PM_STARTUP,       /* waiting for startup subprocess */
    PM_RECOVERY,      /* in archive recovery mode */
    PM_HOT_STANDBY,   /* in hot standby mode */
    PM_RUN,           /* normal "database is alive" state */
    PM_WAIT_BACKUP,   /* waiting for online backup mode to end */
    PM_WAIT_READONLY, /* waiting for read only backends to exit */
    PM_WAIT_BACKENDS, /* waiting for live backends to exit */
    PM_SHUTDOWN,      /* waiting for checkpointer to do shutdown
                       * ckpt */
    PM_SHUTDOWN_2,    /* waiting for archiver and walsenders to
                       * finish */
    PM_WAIT_DEAD_END, /* waiting for dead_end children to exit */
    PM_NO_CHILDREN    /* all important children have exited */
} PMState;

extern volatile PMState pmState;

#define MAX_PMSTATE_MSG_LEN 100
typedef struct PMStateInfo {
    PMState pmState;                      /* PM state */
    char pmStateMsg[MAX_PMSTATE_MSG_LEN]; /* PM state message */
} PMStateInfo;

extern const char* GetPMState(const PMState pmState);

/*
 * Constants that represent which of postmaster_alive_fds is held by
 * postmaster, and which is used in children to check for postmaster death.
 */
#define POSTMASTER_FD_WATCH                                  \
    0                       /* used in children to check for \
                             * postmaster death */
#define POSTMASTER_FD_OWN 1 /* kept open by postmaster only */

#define AmPostmasterProcess() (t_thrd.proc_cxt.MyProcPid == PostmasterPid)

extern const char* progname;

extern int PostmasterMain(int argc, char* argv[]);
extern void ClosePostmasterPorts(bool am_syslogger);
extern void ExitPostmaster(int status);

extern int MaxLivePostmasterChildren(void);

extern Size CBMShmemSize(void);
extern void CBMShmemInit(void);

extern Size HaShmemSize(void);
extern void HaShmemInit(void);
#ifdef EXEC_BACKEND
extern Size ShmemBackendArraySize(void);
extern void ShmemBackendArrayAllocation(void);
extern int SubPostmasterMain(int argc, char* argv[]);

#endif
#define MAX_BACKENDS 0x3FFFF
extern void KillGraceThreads(void);

#define MAX_IPADDR_LEN 64
#define MAX_PORT_LEN 6
#define MAX_LISTEN_ENTRY 64
#define MAX_IP_STR_LEN 64
#define MAX_UNIX_PATH_LEN 1024
#define LOCAL_HOST "localhost"
#define LOOP_IP_STRING "127.0.0.1"
#define LOOP_IPV6_IP "::1"

#define NO_NEED_UN_TCPIP (0x00)
#define NEED_CREATE_UN (0x0F)
#define NEED_CREATE_TCPIP (0xF0)
#define NEED_CREATE_UN_TCPIP (0xFF)

typedef struct LISTEN_ENTRY {
    char ipaddr[MAX_IPADDR_LEN];
    int portnum;
    int createmodel; /* type of the socket to be created */
} LISTEN_ENTRY;

typedef struct LISTEN_ADDRS {
    int usedNum;
    LISTEN_ENTRY lsnArray[MAX_LISTEN_ENTRY];
} LISTEN_ADDRS;

typedef struct DnUsedSpaceHashEntry {
    uint64 query_id;
    uint64 dnUsedSpace;
} DnUsedSpaceHashEntry;

extern void CreateServerSocket(
    char* ipaddr, int portNumber, int enCreatemode, int* success, bool add_localaddr_flag, bool is_create_psql_sock);
extern bool CheckSockAddr(struct sockaddr* sock_addr, const char* szIP, int port);

extern DbState get_local_dbstate(void);
extern const char* wal_get_db_state_string(DbState db_state);

extern void socket_close_on_exec(void);
extern void HandleChildCrash(ThreadId pid, int exitstatus, const char* procname);

/* for ReaperBackend thread */
extern volatile ThreadId ReaperBackendPID;

extern bool IsCBMWriterRunning(void);

void SetFlagForGetLCName(bool falg);

extern ServerMode GetServerMode();
/* set disable connection */
extern void set_disable_conn_mode(void);
/* Defines the position where the signal "disable_conn_primary" is saved. */
#define disable_conn_file "disable_conn_file"
#define POLLING_CONNECTION_STR "polling_connection"
#define SPECIFY_CONNECTION_STR "specify_connection"
#define PROHIBIT_CONNECTION_STR "prohibit_connection"

#ifdef ENABLE_MULTIPLE_NODES
#define IsConnPortFromCoord(port) \
    ((port)->cmdline_options != NULL && strstr((port)->cmdline_options, "remotetype=coordinator") != NULL)
#else
#define IsConnPortFromCoord(port) false
#endif

bool IsFromLocalAddr(Port* port);
extern bool IsMatchSocketAddr(const struct sockaddr* sock_addr, int compare_port);
extern bool IsHAPort(Port* port);
extern bool IsConnectBasePort(const Port* port);
extern ThreadId initialize_util_thread(knl_thread_role role, void* payload = NULL);
extern ThreadId initialize_worker_thread(knl_thread_role role, Port* port, void* payload = NULL);
extern void startup_die(SIGNAL_ARGS);
extern void PortInitialize(Port* port, knl_thread_arg* arg);
extern void PreClientAuthorize();
extern int ClientConnInitilize(Port* port);
extern void CheckClientIp(Port* port);
extern Backend* GetBackend(int slot);
extern Backend* AssignFreeBackEnd(int slot);
extern long PostmasterRandom(void);
extern void signal_child(ThreadId pid, int signal, int be_mode);
extern void GenerateCancelKey(bool isThreadPoolSession);
extern bool SignalCancelAllBackEnd();
extern bool IsLocalAddr(Port* port);
extern uint64_t mc_timers_us(void);
extern bool SetDBStateFileState(DbState state, bool optional);
extern void GPCResetAll();
extern void initRandomState(TimestampTz start_time, TimestampTz stop_time);
extern bool PMstateIsRun(void);
extern ServerMode GetHaShmemMode(void);
extern void InitProcessAndShareMemory();
extern void InitShmemForDcfCallBack();
#endif /* _POSTMASTER_H */
