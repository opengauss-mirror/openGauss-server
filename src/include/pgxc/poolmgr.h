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
#include "nodes/nodes.h"
#include "pgxcnode.h"
#include "poolcomm.h"
#include "storage/pmsignal.h"
#include "utils/hsearch.h"
#include "executor/execStream.h"

#define MAX_IDLE_TIME 60

// The handle status is normal, send back socket to the pool
#define CONN_STATE_NORMAL 'n'
// The handle status has an error, drop the socket
#define CONN_STATE_ERROR 'e'

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

/* Connection pool entry */
typedef struct {
    /* In pooler stateless reuse mode, user_name and pgoptions save in slot to check whether to send params */
    char* user_name;
    char* pgoptions; /* Connection options */
    bool hava_session_params;
    bool need_validate;
    Oid userid;
    struct timeval released;
    NODE_CONNECTION* conn;
    NODE_CANCEL* xc_cancelConn;
} PGXCNodePoolSlot;

/* Pool of connections to specified pgxc node */
typedef struct {
    Oid nodeoid; /* Node Oid related to this pool, Hash key (must be first!) */
    char* connstr;
    char* connstr1;
    int freeSize; /* available connections */
    int size;     /* total pool size */
    bool valid;   /* ensure atom initialization of node pool */
    pthread_mutex_t lock;
    PGXCNodePoolSlot** slot;
} PGXCNodePool;

/* All pools for specified database */
typedef struct databasepool {
    char* database;
    char* user_name;
    Oid userId;
    char* pgoptions; /* Connection options */
    HTAB* nodePools; /* Hashtable of PGXCNodePool, one entry for each Coordinator or DataNode */
    MemoryContext mcxt;
    struct databasepool* next; /* Reference to next to organize linked list */
} DatabasePool;

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
    bool is_retry;                  /* retry to connect */
} PoolGeneralInfo;

/*
 * Agent of client session (Pool Manager side)
 * Acts as a session manager, grouping connections together
 * and managing session parameters
 */
typedef struct PoolAgent {
    /* Process ID of postmaster child process associated to pool agent */
    ThreadId pid;
    /* communication channel */
    PoolPort port;
    DatabasePool* pool;

    /* In pooler stateless reuse mode, user_name and pgoptions save in PoolAgent */
    char* user_name;
    char* pgoptions;              /* Connection options */
    bool has_check_params;        // Deal with the situations where SET GUC commands are run within a Transaction block
    bool is_thread_pool_session;  // use for thread pool
    uint64 session_id;            /* user session id(when thread pool disabled, equal to pid) */

    MemoryContext mcxt;
    int num_dn_connections;
    int num_coord_connections;
    int params_set;                       /* param is set */
    int reuse;                            /* connection reuse flag */
    Oid* dn_conn_oids;                    /* one for each Datanode */
    Oid* coord_conn_oids;                 /* one for each Coordinator */
    Oid* dn_connecting_oids;              /* current connection dn oids */
    int dn_connecting_cnt;                /* current connection dn count */
    Oid* coord_connecting_oids;           /* current connection cn oids */
    int coord_connecting_cnt;             /* current connection cn count */
    PGXCNodePoolSlot** dn_connections;    /* one for each Datanode */
    PGXCNodePoolSlot** coord_connections; /* one for each Coordinator */
    char* session_params;
    char* local_params;
    char* temp_namespace; /* @Temp table. temp namespace name of session related to this agent. */
    List* params_list;    /* session params list */
} PoolAgent;

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
} PoolConnectionInfo;

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

typedef PoolAgent PoolHandle;

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
extern int PoolManagerSetCommand(PoolCommandType command_type, const char* set_command);

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
extern void PoolManagerReleaseConnections(const char* status_array, bool has_error);

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
extern void release_connection(DatabasePool* dbPool, PGXCNodePoolSlot* slot, Oid node, bool force_destroy);
extern int agent_release_free_slots(PoolAgent* agent, Oid nodeoid);
/* Send commands to alter the behavior of current transaction */
extern int PoolManagerSendLocalCommand(int dn_count, const int* dn_list, int co_count, const int* co_list);

extern int* StreamConnectNodes(List* datanodelist, int consumerDop, int distrType, NodeDefinition* nodeDef,
    struct addrinfo** addrArray, StreamKey key, Oid nodeoid);
extern int* StreamConnectNodes(libcommaddrinfo** addrArray, int connNum);

extern void PoolManagerInitPoolerAgent();
extern int register_pooler_session_param(const char* name, const char* queryString);
extern void unregister_pooler_session_param(const char* name);

extern int get_pool_connection_info(PoolConnectionInfo** connectionEntry);
extern void set_pooler_ping(bool mod);
extern void pooler_get_connection_statinfo(PoolConnStat* conn_stat);
extern int register_pooler_session_param(const char* name, const char* queryString);
#endif

#ifdef ENABLE_UT
extern THR_LOCAL PoolHandle* poolHandle;
extern void free_user_name_pgoptions(PGXCNodePoolSlot* slot);
extern void reload_user_name_pgoptions(PoolGeneralInfo* info, PoolAgent* agent, PGXCNodePoolSlot* slot);
extern PGXCNodePoolSlot* alloc_slot_mem(DatabasePool* dbpool);
extern int agent_wait_send_connection_params_pooler_reuse(NODE_CONNECTION* conn);
#endif
