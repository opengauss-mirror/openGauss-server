/* -------------------------------------------------------------------------
 *
 * poolmgr.h
 *
 *	  Definitions for the Datanode connection pool.
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/poolmgr.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef POOLMGR_H
#define POOLMGR_H

#include <sys/time.h>
#include "pool_comm.h"
#include "pgxcnode.h"
#include "storage/pmsignal.h"
#include "utils/hsearch.h"

#ifdef ENABLE_MULTIPLE_NODES
#include "postgres.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "catalog/pgxc_node.h"
#include "commands/dbcommands.h"
#include "nodes/nodes.h"
#include "threadpool/threadpool.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "pgxc/pgxc.h"
#include "pgxc/nodemgr.h"
#include "postmaster/postmaster.h" /* For UnixSocketDir */
#include "postmaster/autovacuum.h"
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include "gssignal/gs_signal.h"
#include "libcomm/libcomm.h"
#include "tcop/utility.h"
#include "postmaster/postmaster.h"
#include "portability/instr_time.h"
#endif

#define MAX_IDLE_TIME 60
#define ENABLE_STATELESS_REUSE g_instance.attr.attr_network.PoolerStatelessReuse
/* The handle status is normal, send back socket to the pool */
#define CONN_STATE_NORMAL 'n'
/* The handle status has an error, drop the socket */
#define CONN_STATE_ERROR 'e'
/* param number */
#define FOUR_ARGS (4)
#define FIVE_ARGS (5)
#define SIX_ARGS (6)

/*
 * List of flags related to pooler connection clean up when disconnecting
 * a session or relaeasing handles.
 * When Local SET commands (POOL_CMD_LOCAL_SET) are used, local parameter
 * string is cleaned by the node commit itself.
 * When global SET commands (POOL_CMD_GLOBAL_SET) are used, "RESET ALL"
 * command is sent down to activated nodes to at session end. At the end
 * of a transaction, connections using global SET commands are not sent
 * back to pool.
 * When temporary object commands are used (POOL_CMD_TEMP), "DISCARD ALL"
 * query is sent down to nodes whose connection is activated at the end of
 * a session.
 * At the end of a transaction, a session using either temporary objects
 * or global session parameters has its connections not sent back to pool.
 *
 * Local parameters are used to change within current transaction block.
 * They are sent to remote nodes invloved in the transaction after sending
 * BEGIN TRANSACTION using a special firing protocol.
 * They cannot be sent when connections are obtained, making them having no
 * effect as BEGIN is sent by backend after connections are obtained and
 * obtention confirmation has been sent back to backend.
 * SET CONSTRAINT, SET LOCAL commands are in this category.
 *
 * Global parmeters are used to change the behavior of current session.
 * They are sent to the nodes when the connections are obtained.
 * SET GLOBAL, general SET commands are in this category.
 */
typedef enum {
    POOL_CMD_TEMP,      /* Temporary object flag */
    POOL_CMD_LOCAL_SET, /* Local SET flag, current transaction block only */
    POOL_CMD_GLOBAL_SET /* Global SET flag */
} PoolCommandType;

typedef enum {
    POOL_NODE_CN, /* node type: coordinator */
    POOL_NODE_DN  /* node type: datanode */
} PoolNodeType;

typedef enum {
    POOL_NODE_NONE = 0, /* no node mode */
    POOL_NODE_PRIMARY,  /* node mode: primary */
    POOL_NODE_STANDBY   /* node mode: standby */
} PoolNodeMode;

/* Pool of connections to specified pgxc node */
typedef struct {
    Oid nodeoid; /* Node Oid related to this pool, Hash key (must be first!) */
    char* connstr;
    char* connstr1;
    int freeSize; /* available connections */
    int size;     /* total pool size */
    bool valid;   /* ensure atom initialization of node pool */
    pthread_mutex_t lock;
    PGXCNodePoolSlot** slot; /* use in not ENABLE_STATELESS_REUSE */
    HTAB* useridSlotsHash; /* hash table: key(userid), value(PGXCNodePoolSlot*), use in ENABLE_STATELESS_REUSE */
} PGXCNodePool;

typedef struct PoolerCleanParams {
    DatabasePool **dbPool;
    List *nodeList;
    Oid *nodeOids;
    int totalCleanNums;
    int needCleanNumsPerNode;
    int totalNodes;
    struct timeval *currVal;
} PoolerCleanParams;

typedef struct {
    ThreadId pid;                   /* Process ID of postmaster child process associated to pool agent */
    int num_invalid_node;           /* the num of nodes that connection is invalid */
    int num_connections;            /* num of the node connections */
    int result_offset;              /* offset of result */
    int* invalid_nodes;             /* the nodes that connection is invalid */
    Oid* conn_oids;                 /* connection id for each node */
    PGXCNodePoolSlot** connections; /* connection for each node */
    DatabasePool* dbpool;           /* database pool */
    char* session_params;           /* session params */
    char* temp_namespace;           /* @Temp table. temp namespace name of session related to this agent. */
    PoolNodeType node_type;         /* node type, dn or dn */
    PoolNodeMode* node_mode;        /* node mode: primary, standby or none */
    PoolConnDef* conndef;           /* pool conn def */
    PoolConnDef* conndef_for_validate;  /* record cn/dn connections while in connecting process */
    bool is_retry;                  /* retry to connect */
    PoolAgent* agent;               /* reference to local PoolAgent struct */
} PoolGeneralInfo;

/*
 * The number of conn slots is recorded based on the number of connSlotNums
 * So we bind the two values together to maintain integrity
 */
typedef struct PoolSlotGroup {
    PGXCNodePoolSlot** connSlots;
    Oid* connOids;
    uint32 connSlotNums;

    /* dn and cn slots often process together, so need offset to indicate real postion */
    uint32 offset;
} PoolSlotGroup;

/*
 * Invalid backend entry
 * Collect info from pooler and pgxc_node. An invalid backend
 * may hold several invalid connections to different node.
 */
typedef struct {
    ThreadId tid;
    char** node_name;
    bool is_thread_pool_session;

    /*
     * used to send signal.
     * if IS_THREAD_POOL_WORKER, set to session_ctr_index;
     * else is MyProcPid
     */
    ThreadId send_sig_pid;

    int num_nodes;
    int total_len;
    bool is_connecting;
} InvalidBackendEntry;

/*
 * PoolConnectionInfo entry for pg_pooler_status view
 */
typedef struct {
    char* database;
    char* user_name;
    ThreadId tid;
    char* pgoptions;
    bool in_use;
    Oid nodeOid;
    char *session_params;
    int fdsock;
    ThreadId remote_pid;
    uint32 used_count; /*counting the times of the slot be used*/
    int idx;
    int streamid;
} PoolConnectionInfo;

/*
 * Record global connection status for function 'comm_check_connection_status'
 * This struct in g_instance.pooler_cxt is defined to g_GlobalConnStatus!
 */
typedef struct GlobalConnStatus {
    /* Record connection status */
    struct ConnectionStatus **connEntries;

    /* Numbers of coordiantors and primary datanodes for connEntries */
    int totalEntriesCount;

    /* Lock of this struct */
    pthread_mutex_t connectionStatusLock;
} GlobalConnStatus;

/*
 * ConnectionStatus entry for pg_conn_status view
 */
typedef struct ConnectionStatus {
    Oid remote_nodeoid;  /* remode node oid */
    char *remote_name;
    char *remote_host;
    int remote_port;
    bool is_connected;  /* connection status flag, record by creating socket connection */
    bool no_error_occur;  /* pooler connect status flag, record by creating pooler connection */
    int sock;
} ConnectionStatus;

/* Connection information cached */
typedef struct PGXCNodeConnectionInfo {
    Oid nodeoid;
    char* host;
    int port;
    int64 cn_connect_time;
    int64 dn_connect_time;
} PGXCNodeConnectionInfo;

/* Connection statistics info */
typedef struct {
    int count;
    int free_count;
    int64 memory_size;
    int64 free_memory_size;
    int dbpool_count;
    int nodepool_count;
} PoolConnStat;

typedef struct DatanodeShardInfo
{
    Oid      s_nodeoid;
    NameData s_data_shardname;
    Oid      s_primary_nodeoid;
    NameData s_primary_nodename;
} DatanodeShardInfo;

/* Status inquiry functions */
extern void PGXCPoolerProcessIam(void);
extern bool IsPGXCPoolerProcess(void);

/* Initialize internal structures */
extern int PoolManagerInit(void);

/* Destroy internal structures */
extern int PoolManagerDestroy(void);

/*
 * Get handle to pool manager. This function should be called just before
 * forking off new session. It creates PoolHandle, PoolAgent and a pipe between
 * them. PoolAgent is stored within Postmaster's memory context and Session
 * closes it later. PoolHandle is returned and should be store in a local
 * variable. After forking off it can be stored in global memory, so it will
 * only be accessible by the process running the session.
 */
extern PoolHandle* GetPoolManagerHandle(void);

/*
 * Called from Postmaster(Coordinator) after fork. Close one end of the pipe and
 * free memory occupied by PoolHandler
 */
extern void PoolManagerCloseHandle(PoolHandle* handle);

/*
 * Gracefully close connection to the PoolManager
 */
extern void PoolManagerDisconnect(void);

extern char* session_options(void);

/*
 * Called from Session process after fork(). Associate handle with session
 * for subsequent calls. Associate session with specified database and
 * initialize respective connection pool
 */
extern void PoolManagerConnect(PoolHandle* handle, const char* database, const char* user_name, const char* pgoptions);

/*
 * Reconnect to pool manager
 * This simply does a disconnection followed by a reconnection.
 */
extern void PoolManagerReconnect(void);

/*
 * Save a SET command in Pooler.
 * This command is run on existent agent connections
 * and stored in pooler agent to be replayed when new connections
 * are requested.
 */
extern int PoolManagerSetCommand(PoolCommandType command_type, const char* set_command, const char* name = NULL);

/* Get pooled connection status */
extern bool PoolManagerConnectionStatus(List* datanodelist, List* coordlist);

/* Get pooled connections */
extern PoolConnDef* PoolManagerGetConnections(List* datanodelist, List* coordlist);

/* Clean pool connections */
extern bool PoolManagerCleanConnection(
    List* datanodelist, List* coordlist, const char* dbname, const char* username, bool is_missing = false);

/* Check consistency of connection information cached in pooler with catalogs */
extern bool PoolManagerCheckConnectionInfo(void);

/* Reload connection data in pooler and drop all the existing connections of pooler */
extern void PoolManagerReloadConnectionInfo(void);

/* Send Abort signal to transactions being run */
extern int PoolManagerAbortTransactions(
    const char* dbname, const char* username, ThreadId** proc_pids, bool** isthreadpool);

/* Return connections back to the pool, for both Coordinator and Datanode connections */
extern void PoolManagerReleaseConnections(const char* status_array, int array_size, bool has_error);

/* return (canceled) invalid backend entry */
extern InvalidBackendEntry* PoolManagerValidateConnection(bool clear, const char* co_node_name, uint32& count);

/* Cancel a running query on Datanodes as well as on other Coordinators */
extern int PoolManagerCancelQuery(int dn_count, const int* dn_list, int co_count, const int* co_list);

// stop a running query.
extern int PoolManagerStopQuery(int dn_count, const int* dn_list);

/* Check if pool has a handle */
extern bool IsPoolHandle(void);

extern bool test_conn(PGXCNodePoolSlot* slot, Oid nodeid);
extern PoolAgent* get_poolagent(void);

extern void release_connection(PoolAgent* agent, PGXCNodePoolSlot** ptrSlot, Oid node, bool force_destroy);

extern void wait_connection_ready(int finish_cnt, int total_cnt, int epfd, int timeout);
extern int fill_node_define(Oid *cn_oids, Oid *dn_oids, int cn_num, int dn_num,
    NodeDefinition **nodeDef_list, Oid &result);
extern ConnectionStatus *fill_conn_entry(int total_cnt, NodeDefinition **nodeDef_list, int epfd, int *finish_cnt);

/* Send commands to alter the behavior of current transaction */
extern int PoolManagerSendLocalCommand(int dn_count, const int* dn_list, int co_count, const int* co_list);
extern void PoolManagerInitPoolerAgent();
extern int get_pool_connection_info(PoolConnectionInfo** connectionEntry);
extern int check_connection_status(ConnectionStatus **connectionEntry);
extern void set_pooler_ping(bool mod);
extern void pooler_get_connection_statinfo(PoolConnStat* conn_stat);
extern int register_pooler_session_param(const char* name, const char* queryString, PoolCommandType command_type);
extern int delete_pooler_session_params(const char* name);
extern void agent_reset_session(PoolAgent* agent, bool need_reset);
extern const char* GetNodeNameByOid(Oid nodeOid);
extern void InitOidNodeNameMappingCache();
extern void CleanOidNodeNameMappingCache();
extern int* StreamConnectNodes(libcommaddrinfo** addrArray, int connNum);

#ifdef ENABLE_MULTIPLE_NODES
extern void free_agent(PoolAgent* agent);
void destroy_slots(List* slots);
extern int get_connection(DatabasePool* dbPool, Oid nodeid, PGXCNodePoolSlot** slot, PoolNodeType node_type);
extern void decrease_node_size(DatabasePool* dbpool, Oid nodeid);
extern PGXCNodePool* acquire_nodepool(DatabasePool* dbPool, Oid nodeid, PoolNodeType node_type);
extern int node_info_check(PoolAgent* agent);
extern int node_info_check_pooler_stateless(PoolAgent* agent);
extern void agent_init(PoolAgent* agent, const char* database, const char* user_name, const char* pgoptions);
extern void agent_destroy(PoolAgent* agent);
extern PoolAgent* agent_create(void);
extern int agent_session_command(PoolAgent* agent, const char* set_command,
    PoolCommandType command_type, bool append_sess_param);
extern int agent_set_command(PoolAgent* agent, const char* set_command,
    PoolCommandType command_type, bool append_sess_param);
extern int agent_temp_command(PoolAgent* agent, const char* namespace_name);
extern DatabasePool* create_database_pool(const char* database, const char* user_name, const char* pgoptions);
extern void reload_database_pools(PoolAgent* agent);
extern DatabasePool* find_database_pool(const char* database, const char* user_name, const char* pgoptions);
extern int send_local_commands(PoolAgent* agent, int dn_count, const int* dn_list, int co_count, const int* co_list);
extern int cancel_query_on_connections(
    PoolAgent* agent, int dn_count, const int* dn_list, int co_count, const int* co_list);
extern int stop_query_on_connections(PoolAgent* agent, int dn_count, const int* dn_list);
extern void agent_release_connections(PoolAgent* agent, bool force_destroy, const char* status_array = NULL);
void release_connection(PoolAgent* agent, PGXCNodePoolSlot** slot, Oid node, bool force_destroy);
extern void destroy_slot(PGXCNodePoolSlot* slot);
extern List* destroy_node_pool(PGXCNodePool* node_pool, List* slots);
extern int clean_connection(List* nodelist, const char* database, const char* user_name);
extern ThreadId* abort_pids(int* count, ThreadId pid, const char* database, const char* user_name, bool** isthreadpool);
extern char* build_node_conn_str(Oid nodeid, DatabasePool* dbPool, bool isNeedSlave);

/* acquire connections with parallel */
extern void agent_acquire_connections_parallel(
    PoolAgent* agent, PoolConnDef* result, List* datanodelist, List* coordlist);
extern void agent_acquire_connections_start(PoolAgent* agent, List* datanodelist, List* coordlist);
extern void agent_acquire_connections_end(PoolAgent* agent);
extern bool is_current_node_during_connecting(
    PoolAgent* agent, const NodeRelationInfo *needClearNodeArray, int waitClearCnt, char node_type);
extern void reset_params_htab(HTAB* htab, bool);
extern int PGXCNodeSendSetParallel(PoolAgent* agent, const char* set_command);
extern char* MakePoolerSessionParams(PoolCommandType commandType);
extern int get_nodeinfo_from_matric(
    char nodeType, int needCreateArrayLen, NodeRelationInfo *needCreateNodeArray);
extern void pooler_sleep_interval_time(bool *poolValidateCancel);
extern HTAB* create_user_slothash(const char* tabname, long nelem);
extern void agent_verify_node_size(PGXCNodePool* nodePool);
extern PGXCNodePoolSlot* alloc_slot_mem(DatabasePool* dbpool);
extern char* GenerateSqlCommand(int argCount, ...);
extern void reload_user_name_pgoptions(PoolGeneralInfo* info, PoolAgent* agent, PGXCNodePoolSlot* slot);
extern bool release_slot_to_nodepool(PGXCNodePool* nodePool, bool force_destroy, PGXCNodePoolSlot* slot);
extern void free_pool_conn(PoolConnDef* conndef_for_validate);

extern void FillNodeConnectionStatus(ConnectionStatus *connsEntry, int entryCnt);
extern void RecreateGlobalConnEntries();
extern void ResetPoolerConnectionStatus();
extern void FlushPoolerConnectionStatus(Oid nodeOid);

/* The root memory context */
extern MemoryContext PoolerMemoryContext;

/*
 * Allocations of core objects: Datanode connections, upper level structures,
 * connection strings, etc.
 */
extern MemoryContext PoolerCoreContext;
/*
 * Memory to store Agents
 */
extern MemoryContext PoolerAgentContext;

/* Pool to all the databases (linked list) */
extern DatabasePool* databasePools;

/* PoolAgents */
extern int agentCount;
extern PoolAgent** poolAgents;
extern pthread_mutex_t g_poolAgentsLock;
extern int MaxAgentCount;

/* GlobalConnStatus -- Record connection status in memory */
#define g_GlobalConnStatus (g_instance.pooler_cxt.globalConnStatus)

#define PROTO_TCP 1
#define get_agent(handle) ((PoolAgent*)(handle))
#endif

#ifdef ENABLE_UT
extern THR_LOCAL PoolHandle* poolHandle;
extern void free_user_name_pgoptions(PGXCNodePoolSlot* slot);
extern int agent_wait_send_connection_params_pooler_reuse(NODE_CONNECTION* conn);
#endif

#endif
