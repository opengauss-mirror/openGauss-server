/* -------------------------------------------------------------------------
 *
 * poolcomm.h
 *
 *	  Definitions for the Pooler-Seesion communications.
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/pool_comm.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef POOLCOMM_H
#define POOLCOMM_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "lib/stringinfo.h"
#include "libcomm/libcomm.h"

#define POOL_BUFFER_SIZE 1024
#define Socket(port) (port).fdsock

/* Connection to Datanode maintained by Pool Manager */
typedef struct pg_conn NODE_CONNECTION;
typedef struct pg_cancel NODE_CANCEL;

typedef struct {
    /* file descriptors */
    int fdsock;
    /* receive buffer */
    int RecvLength;
    int RecvPointer;
    char RecvBuffer[POOL_BUFFER_SIZE];
    /* send buffer */
    int SendPointer;
    char SendBuffer[POOL_BUFFER_SIZE];
} PoolPort;

/* Connection pool entry */
typedef struct PGXCNodePoolSlot {
    /* In pooler stateless reuse mode, user_name and pgoptions save in slot to check whether to send params */
    char* user_name;
    char* pgoptions; /* Connection options */
    char* session_params;
    bool need_validate;
    Oid userid;
    Oid nodeOid;
    struct timeval released;
    NODE_CONNECTION* conn;
    NODE_CANCEL* xc_cancelConn;
    pthread_mutex_t slot_lock;
    union {
        uint32 node_count; /* for list head: numbers of node */
        uint32 used_count; /* for list node: counting the times of the slot be used */
    };
    struct PGXCNodePoolSlot* next;
} PGXCNodePoolSlot;

typedef struct {
    int* fds;
    int fdsCnt;
    gsocket* gsock;
    PoolConnInfo* connInfos;
    NODE_CONNECTION** pgConn;
    int pgConnCnt;
    int waitCreateCount;
} PoolConnDef;

/* All pools for specified database */
typedef struct databasepool {
    char* database;
    char* user_name;
    Oid userId;
    char* pgoptions; /* Connection options */
    HTAB* nodePools; /* Hashtable of PGXCNodePool, one entry for each Coordinator or DataNode */
    /* lock for nodePools to add/find/remove */
    pthread_mutex_t nodePoolsLock;
    MemoryContext mcxt;
    struct databasepool* next; /* Reference to next to organize linked list */
} DatabasePool;

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
    int localParamsSet;                 /* local param is set */
    int reuse;                            /* connection reuse flag */
    Oid* dn_conn_oids;                    /* one for each Datanode */
    Oid* coord_conn_oids;                 /* one for each Coordinator */
    Oid* dn_connecting_oids;              /* current connection dn oids */
    int dn_connecting_cnt;                /* current connection dn count */
    Oid* coord_connecting_oids;           /* current connection cn oids */
    int coord_connecting_cnt;             /* current connection cn count */
    PGXCNodePoolSlot** dn_connections;    /* one for each Datanode */
    PGXCNodePoolSlot** coord_connections; /* one for each Coordinator */
    PoolConnDef* coor_conndef_for_validate;  /* record connections while in connecting process for cn */
    PoolConnDef* dn_conndef_for_validate;  /* record connections while in connecting process for dn */
    char* session_params;
    char* local_params;
    char* temp_namespace;                 /* @Temp table. temp namespace name of session related to this agent. */
    List* params_list;                    /* session params list */
    List* localParamsList;              /* local params list */
    pthread_mutex_t lock;                 /* protect agent in mutilthread */
} PoolAgent;

typedef PoolAgent PoolHandle;

extern int pool_listen(unsigned short port, const char* unixSocketName);
extern int pool_connect(unsigned short port, const char* unixSocketName);
extern int pool_getbyte(PoolPort* port);
extern int pool_pollbyte(PoolPort* port);
extern int pool_getmessage(PoolPort* port, StringInfo s, int maxlen);
extern int pool_getbytes(PoolPort* port, char* s, size_t len);
extern int pool_putmessage(PoolPort* port, char msgtype, const char* s, size_t len);
extern int pool_putbytes(PoolPort* port, const char* s, size_t len);
extern int pool_flush(PoolPort* port);
extern int pool_sendconnDefs(PoolPort* port, PoolConnDef* connDef, int count);
extern int pool_recvconnDefs(PoolPort* port, PoolConnDef* connDef, int count);
extern int pool_sendres(PoolPort* port, int res);
extern int pool_recvres(PoolPort* port);
extern int pool_sendpids(PoolPort* port, ThreadId* pids, int count);
extern int pool_recvpids(PoolPort* port, ThreadId** pids);

#endif /* POOLCOMM_H */
