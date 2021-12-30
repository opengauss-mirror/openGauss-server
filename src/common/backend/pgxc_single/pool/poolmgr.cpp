/* -------------------------------------------------------------------------
 *
 * poolmgr.c
 *
 *    Connection pool manager handles connections to Datanodes
 *
 * The pooler runs as a separate process and is forked off from a
 * Coordinator postmaster. If the Coordinator needs a connection from a
 * Datanode, it asks for one from the pooler, which maintains separate
 * pools for each Datanode. A group of connections can be requested in
 * a single request, and the pooler returns a list of file descriptors
 * to use for the connections.
 *
 * Note the current implementation does not yet shrink the pool over time
 * as connections are idle.  Also, it does not queue requests; if a
 * connection is unavailable, it will simply fail. This should be implemented
 * one day, although there is a chance for deadlocks. For now, limiting
 * connections should be done between the application and Coordinator.
 * Still, this is useful to avoid having to re-establish connections to the
 * Datanodes all the time for multiple Coordinator backend sessions.
 *
 * The term "agent" here refers to a session manager, one for each backend
 * Coordinator connection to the pooler. It will contain a list of connections
 * allocated to a session, at most one per Datanode.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *    $$
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pgxc_node.h"
#include "commands/dbcommands.h"
#include "commands/prepare.h"
#include "nodes/nodes.h"
#include "pgstat.h"
#include "pgxc/poolmgr.h"
#include "threadpool/threadpool.h"
#include "utils/biginteger.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "libpq/auth.h"
#include "libpq/ip.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolutils.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "postmaster/postmaster.h" /* For UnixSocketDir */
#include "postmaster/autovacuum.h"
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include "gssignal/gs_signal.h"
#include "libcomm/libcomm.h"
#include "executor/executor.h"
#include "tcop/utility.h"
#include "postmaster/postmaster.h"
#include "portability/instr_time.h"
#include "utils/elog.h"
#include "pgxc/execRemote.h"
#include "distributelayer/streamCore.h"

#pragma GCC diagnostic ignored "-Wunused-function"

#ifdef ENABLE_UT
#define static
#endif

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

extern GlobalNodeDefinition* global_node_definition;
static volatile bool PoolerPing = false;

#define PROTO_TCP 1
#define get_agent(handle) ((PoolAgent*)(handle))

#define PARAMS_LEN 4096

/*
 * whether the function name is saw by other .o file is on the basis of the
 * declaration of the function not the implement of it. For example:
 * -----------------------------------------------------------------------------
 * void foo();
 * void foo(){...}
 * the name "foo" can be saw by other .o file
 * -----------------------------------------------------------------------------
 * static void foo();
 * void foo(){...}
 * the name "foo" can NOT be saw by other .o file
 * -----------------------------------------------------------------------------
 */

/* The root memory context */
static MemoryContext PoolerMemoryContext = NULL;

/*
 * Allocations of core objects: Datanode connections, upper level structures,
 * connection strings, etc.
 */
static MemoryContext PoolerCoreContext = NULL;
/*
 * Memory to store Agents
 */
static MemoryContext PoolerAgentContext = NULL;

/* Pool to all the databases (linked list) */
static DatabasePool* databasePools = NULL;

/* PoolAgents */
static int agentCount = 0;
static PoolAgent** poolAgents = NULL;
int MaxAgentCount = 0;

/* current retry count for making connection in pooler */
static const int retry_cnt = 2;
static const int E_OK = 0;
static const int E_TO_CREATE = -1;
static const int E_NO_FREE_SLOT = -2;
static const int E_NO_NODE = -3;

static void free_agent(PoolAgent* agent);
void destroy_slots(List* slots);
static int get_connection(DatabasePool* dbPool, Oid nodeid, PGXCNodePoolSlot** slot, PoolNodeType node_type);
static void decrease_node_size(DatabasePool* dbpool, Oid nodeid);
static PGXCNodePool* acquire_nodepool(DatabasePool* dbPool, Oid nodeid, PoolNodeType node_type);
static int node_info_check(PoolAgent* agent);
static int node_info_check_pooler_stateless(PoolAgent* agent);
static void agent_init(PoolAgent* agent, const char* database, const char* user_name, const char* pgoptions);
static void agent_destroy(PoolAgent* agent);
static PoolAgent* agent_create(void);
static int agent_session_command(PoolAgent* agent, const char* set_command, PoolCommandType command_type);
static int agent_set_command(PoolAgent* agent, const char* set_command, PoolCommandType command_type);
static int agent_temp_command(PoolAgent* agent, const char* namespace_name);
static DatabasePool* create_database_pool(const char* database, const char* user_name, const char* pgoptions);
static void reload_database_pools(PoolAgent* agent);
static DatabasePool* find_database_pool(const char* database, const char* user_name, const char* pgoptions);
static int send_local_commands(PoolAgent* agent, int dn_count, const int* dn_list, int co_count, const int* co_list);
static int cancel_query_on_connections(
    PoolAgent* agent, int dn_count, const int* dn_list, int co_count, const int* co_list);
static int stop_query_on_connections(PoolAgent* agent, int dn_count, const int* dn_list);
static void agent_release_connections(PoolAgent* agent, bool force_destroy, const char* status_array = NULL);
void release_connection(PoolAgent* agent, PGXCNodePoolSlot** ptrSlot, Oid node, bool force_destroy);
static void destroy_slot(PGXCNodePoolSlot* slot);
static List* destroy_node_pool(PGXCNodePool* node_pool, List* slots);
static int clean_connection(List* nodelist, const char* database, const char* user_name);
static ThreadId* abort_pids(int* count, ThreadId pid, const char* database, const char* user_name, bool** isthreadpool);
static char* build_node_conn_str(Oid nodeid, DatabasePool* dbPool, bool isNeedSlave);

/* acquire connections with parallel */
static void agent_acquire_connections_parallel(
    PoolAgent* agent, PoolConnDef* result, List* datanodelist, List* coordlist);

static uint32 get_current_connection_nums(Oid* cnOids, Oid* dnOids, List* cn_list, List* dn_list);
static void agent_acquire_connections_start(PoolAgent* agent, List* datanodelist, List* coordlist);
static void agent_acquire_connections_end(PoolAgent* agent);
static bool is_current_node_during_connecting(
    PoolAgent* agent, const NodeRelationInfo *needClearNodeArray, int waitClearCnt, char node_type);

extern void reset_params_htab(HTAB* htab, bool);
extern PGresult* LibcommGetResult(NODE_CONNECTION* conn);
extern int PGXCNodeSendSetParallel(PoolAgent* agent, const char* set_command);

PoolHandle* GetPoolAgent()
{
    return u_sess->pgxc_cxt.poolHandle;
}

/*
 * init the pooler agent.
 */
void PoolManagerInitPoolerAgent()
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: PoolManagerInitPoolerAgent()")));

    if (IS_PGXC_COORDINATOR && !IsPoolHandle()) {
        PoolHandle* pool_handle = NULL;
        if (!IsConnFromGTMTool()) {
            pool_handle = GetPoolManagerHandle();
            char* session_options_ptr = session_options();

            /* Pooler initialization has to be made before ressource is released */
            PoolManagerConnect(pool_handle,
                u_sess->proc_cxt.MyProcPort->database_name,
                u_sess->proc_cxt.MyProcPort->user_name,
                session_options_ptr);
                if (session_options_ptr != NULL) {
                    pfree(session_options_ptr);
                }
        }
    }
}

/*
 * Initialize internal structures
 */
int PoolManagerInit()
{
    MemoryContext oldcontext;

    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: PoolManagerInit()")));

    /*
     * Set up memory contexts for the pooler objects
     */
    PoolerMemoryContext = AllocSetContextCreate(g_instance.instance_context,
        "PoolerMemoryContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);
    PoolerCoreContext = AllocSetContextCreate(PoolerMemoryContext,
        "PoolerCoreContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);
    PoolerAgentContext = AllocSetContextCreate(PoolerMemoryContext,
        "PoolerAgentContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    /* Allocate pooler structures in the Pooler context
     * NOTES: "MaxConnections" should be NEVER changed
     */
    oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);

    /* JobProcess thread and auxiliary thread maybe used pooler agent */
    MaxAgentCount = g_instance.shmem_cxt.MaxBackends;
    poolAgents = (PoolAgent**)palloc(MaxAgentCount * sizeof(PoolAgent*));

    MemoryContextSwitchTo(oldcontext);

    return 0;
}

void free_user_name_pgoptions(PGXCNodePoolSlot* slot)
{
    if (slot != NULL) {
        pfree_ext(slot->user_name);
        pfree_ext(slot->pgoptions);
    }
    pfree_ext(slot);
}

void reload_user_name_pgoptions(PoolGeneralInfo* info, PoolAgent* agent, PGXCNodePoolSlot* slot)
{
    MemoryContext oldcontext = MemoryContextSwitchTo(info->dbpool->mcxt);
    char* user_name = pstrdup(agent->user_name);
    char* pgoptions = pstrdup(agent->pgoptions);
    MemoryContextSwitchTo(oldcontext);

    pfree_ext(slot->user_name);
    pfree_ext(slot->pgoptions);

    slot->user_name = user_name;
    slot->pgoptions = pgoptions;
}

/*
 * Check connection info consistency with system catalogs
 */
int node_info_check(PoolAgent* agent)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: node_info_check()")));

    DatabasePool* dbPool = NULL;
    List* checked = NIL;
    int res = POOL_CHECK_SUCCESS;
    Oid* coOids = NULL;
    Oid* dnOids = NULL;
    int numCo;
    int numDn;

    /*
     * First check if agent's node information matches to current content of the
     * shared memory table.
     */
    PgxcNodeGetOids(&coOids, &dnOids, &numCo, &numDn, false);

    if (agent->num_coord_connections != numCo || agent->num_dn_connections != numDn) {
        res = POOL_CHECK_FAILED;
        ereport(WARNING,
            (errcode(ERRCODE_WARNING),
                errmsg("pooler: the number of cn/dn changes, old cn/dn: %d/%d, new cn/dn: %d/%d",
                    agent->num_coord_connections,
                    agent->num_dn_connections,
                    numCo,
                    numDn)));
    }

    if (POOL_CHECK_SUCCESS == res)
        if (memcmp(agent->coord_conn_oids, coOids, numCo * sizeof(Oid)) ||
            memcmp(agent->dn_conn_oids, dnOids, numDn * sizeof(Oid))) {
            res = POOL_CHECK_FAILED;
            ereport(LOG, (errcode(ERRCODE_WARNING), errmsg("pooler: oid of cn/dn changes!")));
        }

    /* Release palloc'ed memory */
    pfree_ext(coOids);
    pfree_ext(dnOids);

    /*
     * Iterate over all dbnode pools and check if connection strings
     * are matching node definitions.
     */
    LWLockAcquire(PoolerLock, LW_EXCLUSIVE);

    dbPool = databasePools;
    while ((res == POOL_CHECK_SUCCESS) && (dbPool != NULL)) {
        HASH_SEQ_STATUS hseq_status;
        PGXCNodePool* nodePool = NULL;

        hash_seq_init(&hseq_status, dbPool->nodePools);
        while ((nodePool = (PGXCNodePool*)hash_seq_search(&hseq_status))) {
            char* connstr_chk = NULL;

            /* part-initialized node pool, will be destroied somewhere else */
            if (false == nodePool->valid)
                continue;

            /* No need to check same Datanode twice */
            if (list_member_oid(checked, nodePool->nodeoid))
                continue;
            checked = lappend_oid(checked, nodePool->nodeoid);

            connstr_chk = build_node_conn_str(nodePool->nodeoid, dbPool, false);
            if (connstr_chk == NULL) {
                /* Problem of constructing connection string */
                hash_seq_term(&hseq_status);
                res = POOL_CHECK_FAILED;
                ereport(LOG,
                    (errcode(ERRCODE_WARNING),
                        errmsg("pooler: could not build connstr for node oid: %u", nodePool->nodeoid)));
                break;
            }
            /* return error if there is difference */
            if (strcmp(connstr_chk, nodePool->connstr)) {
                hash_seq_term(&hseq_status);
                res = POOL_CHECK_FAILED;
                ereport(LOG,
                    (errcode(ERRCODE_WARNING),
                        errmsg("pooler: connstr of node(oid:%u) changes, old: %s, new %s",
                            nodePool->nodeoid,
                            nodePool->connstr,
                            connstr_chk)));
                pfree_ext(connstr_chk);
                break;
            }
            pfree_ext(connstr_chk);

            connstr_chk = build_node_conn_str(nodePool->nodeoid, dbPool, true);
            if (connstr_chk == NULL) {
                /* Problem of constructing connection string */
                hash_seq_term(&hseq_status);
                res = POOL_CHECK_FAILED;
                ereport(LOG,
                    (errcode(ERRCODE_WARNING),
                        errmsg("pooler: could not build connstr1 for node oid: %u", nodePool->nodeoid)));
                break;
            }
            /* return error if there is difference */
            if (IS_DN_DUMMY_STANDYS_MODE() && strcmp(connstr_chk, nodePool->connstr1)) {
                hash_seq_term(&hseq_status);
                res = POOL_CHECK_FAILED;
                ereport(LOG,
                    (errcode(ERRCODE_WARNING),
                        errmsg("pooler: connstr1 of node(oid:%u) changes, old: %s, new %s",
                            nodePool->nodeoid,
                            nodePool->connstr1,
                            connstr_chk)));
                pfree_ext(connstr_chk);
                break;
            }
            pfree_ext(connstr_chk);
        }
        dbPool = dbPool->next;
    }

    LWLockRelease(PoolerLock);

    list_free(checked);
    return res;
}

/*
 * Check connection info function in pooler stateless reuse mode.
 */
int node_info_check_pooler_stateless(PoolAgent* agent)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: node_info_check_pooler_stateless()")));

    int res = POOL_CHECK_SUCCESS;
    Oid* coOids = NULL;
    Oid* dnOids = NULL;
    int numCo;
    int numDn;

    /*
     * First check if agent's node information matches to current content of the
     * shared memory table.
     */
    PgxcNodeGetOids(&coOids, &dnOids, &numCo, &numDn, false);

    if (agent->num_coord_connections != numCo || agent->num_dn_connections != numDn) {
        res = POOL_CHECK_FAILED;
        ereport(WARNING,
            (errcode(ERRCODE_WARNING),
                errmsg("pooler: the number of cn/dn changes, old cn/dn: %d/%d, new cn/dn: %d/%d",
                    agent->num_coord_connections,
                    agent->num_dn_connections,
                    numCo,
                    numDn)));
    }

    if (POOL_CHECK_SUCCESS == res)
        if (memcmp(agent->coord_conn_oids, coOids, numCo * sizeof(Oid)) ||
            memcmp(agent->dn_conn_oids, dnOids, numDn * sizeof(Oid))) {
            res = POOL_CHECK_FAILED;
            ereport(LOG, (errcode(ERRCODE_WARNING), errmsg("pooler: oid of cn/dn changes!")));
        }

    /* Release palloc'ed memory */
    pfree_ext(coOids);
    pfree_ext(dnOids);

    return res;
}

/*
 * Get handle to pool manager
 * Invoked from Postmaster's main loop just before forking off new session
 * Returned PoolHandle structure will be inherited by session process
 *
 * Before removing pooler thread, there are just 2 steps in the function:
 * first, connect to pooler; second, malloc PoolHandle.
 *
 * After pooler thread is removed, so the 2nd step is always OK almost.
 * So we do the 2nd step first, and then call agent_create() directly to
 * alloc a unique ID to handle->port.fdsock.
 */
PoolHandle* GetPoolManagerHandle(void)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: GetPoolManagerHandle()")));

    PoolHandle* handle = NULL;

    /* Allocate handle */
    /*
     * XXX we may change malloc here to palloc but first ensure
     * the CurrentMemoryContext is properly set.
     * The handle allocated just before new session is forked off and
     * inherited by the session process. It should remain valid for all
     * the session lifetime.
     */

    /* pooler thread do NOT exist any more, so create agent here
     * there is a potential problem:
     * if PG_RE_THROW is called in agent_create(), so memory of handle
     * is lost.
     */

    handle = agent_create();

    return handle;
}

/*
 * get agent by fdsock(agent id) of PoolHandle
 * get_agent() should be called out of lock.
 */

/*
 * Create agent
 */
PoolAgent* agent_create(void)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: agent_create()")));

    MemoryContext oldcontext;
    PoolAgent* agent = NULL;

    LWLockAcquire(PoolerLock, LW_EXCLUSIVE);

    if (agentCount >= MaxAgentCount - 1) {
        LWLockRelease(PoolerLock);

        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
                errmsg("pooler: Failed to create agent, number of agent reaches MaxAgentCount: %d", MaxAgentCount)));
    }

    oldcontext = MemoryContextSwitchTo(PoolerAgentContext);

    /* Allocate agent */
    agent = (PoolAgent*)palloc0_noexcept(sizeof(PoolAgent));
    if (agent == NULL) {
        LWLockRelease(PoolerLock);
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("agent cannot alloc memory.")));
    }

    agent->port.fdsock = -1;
    agent->mcxt = PoolerAgentContext;
    if (IS_THREAD_POOL_WORKER) {
        agent->pid = u_sess->session_ctr_index;
        agent->session_id = u_sess->session_id;
        agent->is_thread_pool_session = true;
    } else {
        agent->pid = t_thrd.proc_cxt.MyProcPid;
        agent->session_id = 0;
        agent->is_thread_pool_session = false;
    }

    agent->params_list = NIL;
    agent->session_params = NULL;
    agent->temp_namespace = NULL;

    /* Append new agent to the list */

    MemoryContextSwitchTo(oldcontext);
    LWLockRelease(PoolerLock);

    return agent;
}

/*
 * @Description: make pooler session params with hash table
 * @IN void
 * @Return: new session params
 * @See also:
 */
char* MakePoolerSessionParams(PoolCommandType command_type)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * @Description: unregister session params
 * @IN name: param name
 * @Return: void
 * @See also:
 */
void unregister_pooler_session_param(const char* name)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

/*
 * @Description: register session params
 * @IN name: param name
 * @IN queryString: query string
 * @Return: -1 param is not set 0 param is set
 * @See also:
 */
int register_pooler_session_param(const char* name, const char* queryString, PoolCommandType command_type)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return -1;
}

/*
 * @Description: delete session params
 * @IN name: param name
 * @Return: -1 param is invalid, 1 param is not found, 0 param is delete
 * @See also:
 */
int delete_pooler_session_params(const char* name)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return -1;
}

/*
 * @Description: check whether force reuse connections
 * @IN void
 * @Return: true: yes false: no
 * @See also:
 */
bool check_connection_reuse(void)
{
    if (!u_sess->attr.attr_network.PoolerForceReuse)
        return false;

    PoolAgent* agent = get_agent(u_sess->pgxc_cxt.poolHandle);

    if (agent == NULL)
        return false;

    return agent->reuse == 1;
}

/*
 * session_options
 * Returns the pgoptions string generated using a particular
 * list of parameters that are required to be propagated to Datanodes.
 * These parameters then become default values for the pooler sessions.
 * For e.g., a psql user sets PGDATESTYLE. This value should be set
 * as the default connection parameter in the pooler session that is
 * connected to the Datanodes. There are various parameters which need to
 * be analysed individually to determine whether these should be set on
 * Datanodes.
 *
 * Note: These parameters values are the default values of the particular
 * Coordinator backend session, and not the new values set by SET command.
 *
 */

char* session_options(void)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
#else

    int i;
    char* pgoptions[] = {"DateStyle", "timezone", "geqo", "intervalstyle", "lc_monetary"};
    StringInfoData options;
    List* value_list = NIL;
    ListCell* l = NULL;

    initStringInfo(&options);

    for (i = 0; i < sizeof(pgoptions) / sizeof(char*); i++) {
        const char* value = NULL;

        appendStringInfo(&options, " -c %s=", pgoptions[i]);

        value = GetConfigOptionResetString(pgoptions[i]);

        /* lc_monetary does not accept lower case values */
        if (strcmp(pgoptions[i], "lc_monetary") == 0) {
            appendStringInfoString(&options, value);
            continue;
        }

        char *rawString = strdup(value);
        SplitIdentifierString(rawString, ',', &value_list);
        free(rawString);
        foreach (l, value_list) {
            char* value = (char*)lfirst(l);
            appendStringInfoString(&options, value);
            if (lnext(l))
                appendStringInfoChar(&options, ',');
        }

        list_free_ext(value_list);
    }

    return options.data;

#endif
}

/*
 * Associate session with specified database and respective connection pool
 * Invoked from Session process
 */
void PoolManagerConnect(PoolHandle* handle, const char* database, const char* user_name, const char* pgoptions)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    int n32;
    char msgtype = 'c';

    if (handle == NULL || database == NULL || user_name == NULL) {
        ereport(PANIC, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("pooler: connect failed, invaild argument.")));
    }

    /* Save the handle */
    poolHandle = handle;

    /* Message type */
    pool_putbytes(&handle->port, &msgtype, 1);

    /* Message length */
    n32 = htonl(strlen(database) + strlen(user_name) + strlen(pgoptions) + 23);
    pool_putbytes(&handle->port, (char*)&n32, 4);

    /* PID number */
    n32 = htonl(MyProcPid);
    pool_putbytes(&handle->port, (char*)&n32, 4);

    /* Length of Database string */
    n32 = htonl(strlen(database) + 1);
    pool_putbytes(&handle->port, (char*)&n32, 4);

    /* Send database name followed by \0 terminator */
    pool_putbytes(&handle->port, database, strlen(database) + 1);
    pool_flush(&handle->port);

    /* Length of user name string */
    n32 = htonl(strlen(user_name) + 1);
    pool_putbytes(&handle->port, (char*)&n32, 4);

    /* Send user name followed by \0 terminator */
    pool_putbytes(&handle->port, user_name, strlen(user_name) + 1);
    pool_flush(&handle->port);

    /* Length of pgoptions string */
    n32 = htonl(strlen(pgoptions) + 1);
    pool_putbytes(&handle->port, (char*)&n32, 4);

    /* Send pgoptions followed by \0 terminator */
    pool_putbytes(&handle->port, pgoptions, strlen(pgoptions) + 1);
    pool_flush(&handle->port);
#endif
}

/*
 * Reconnect to pool manager
 * It simply does a disconnection and a reconnection.
 */
void PoolManagerReconnect(void)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    PoolHandle* handle = NULL;
    char* session_options_ptr = session_options();

    Assert(poolHandle);

    PoolManagerDisconnect();
    handle = GetPoolManagerHandle();
    PoolManagerConnect(handle, get_database_name(MyDatabaseId), GetUserNameFromId(GetUserId()), session_options_ptr);
    if (session_options_ptr != NULL) {
        pfree(session_options_ptr);
    }

#endif
}

int PoolManagerSetCommand(PoolCommandType command_type, const char* set_command, const char* name)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
#else
    int n32, res;
    char msgtype = 's';

    Assert(poolHandle);

    /*
     * If SET LOCAL is in use, flag current transaction as using
     * transaction-block related parameters with pooler agent.
     */
    if (command_type == POOL_CMD_LOCAL_SET)
        SetCurrentLocalParamStatus(true);

    /* Message type */
    pool_putbytes(&poolHandle->port, &msgtype, 1);

    /* Message length */
    if (set_command)
        n32 = htonl(strlen(set_command) + 13);
    else
        n32 = htonl(12);

    pool_putbytes(&poolHandle->port, (char*)&n32, 4);

    /* LOCAL or SESSION parameter ? */
    n32 = htonl(command_type);
    pool_putbytes(&poolHandle->port, (char*)&n32, 4);

    if (set_command) {
        /* Length of SET command string */
        n32 = htonl(strlen(set_command) + 1);
        pool_putbytes(&poolHandle->port, (char*)&n32, 4);

        /* Send command string followed by \0 terminator */
        pool_putbytes(&poolHandle->port, set_command, strlen(set_command) + 1);
    } else {
        /* Send empty command */
        n32 = htonl(0);
        pool_putbytes(&poolHandle->port, (char*)&n32, 4);
    }

    pool_flush(&poolHandle->port);

    /* Get result */
    res = pool_recvres(&poolHandle->port);

    return res;
#endif
}

/*
 * Send commands to alter the behavior of current transaction and update begin sent status
 */
int PoolManagerSendLocalCommand(int dn_count, const int* dn_list, int co_count, const int* co_list)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: PoolManagerSendLocalCommand()")));

    if (u_sess->pgxc_cxt.poolHandle == NULL)
        return EOF;

    if (dn_count == 0 && co_count == 0)
        return EOF;

    if (dn_count != 0 && dn_list == NULL)
        return EOF;

    if (co_count != 0 && co_list == NULL)
        return EOF;

    PoolAgent* agent = get_agent(u_sess->pgxc_cxt.poolHandle);
    if (NULL == agent->local_params)  // short cut
        return 0;

    int rs = send_local_commands(agent, dn_count, dn_list, co_count, co_list);
    return rs;
}

/*
 * Clean invalid connections for same username without different user oid
 *
 * We add userId in struct DatabasePool to identify the scenes that drop
 * user cascade and recreate a same username, we must clean cached connections
 * for old user.
 *
 * Assume have got PoolerLock in exclusive mode.
 */
static List* PoolCleanInvalidConnections(DatabasePool* databasePool)
{
    if (ENABLE_STATELESS_REUSE) {
        return NULL;
    }

    Assert(databasePool);

    /* Destroy database pool slot */
    HASH_SEQ_STATUS hseq_status;
    PGXCNodePool* nodePool = NULL;
    List* slots = NIL;

    databasePool->userId = GetAuthenticatedUserId();
    hash_seq_init(&hseq_status, databasePool->nodePools);
    while ((nodePool = (PGXCNodePool*)hash_seq_search(&hseq_status))) {
        if (nodePool->valid == false) {
            pfree_ext(nodePool->connstr);
            pfree_ext(nodePool->connstr1);
            pfree_ext(nodePool->slot);
            hash_search(databasePool->nodePools, &nodePool->nodeoid, HASH_REMOVE, NULL);
            continue;
        }

        slots = destroy_node_pool(nodePool, slots);
        hash_search(databasePool->nodePools, &nodePool->nodeoid, HASH_REMOVE, NULL);
    }

    return slots;
}

/*
 * Init PoolAgent
 */
void agent_init(PoolAgent* agent, const char* database, const char* user_name, const char* pgoptions)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: agent_init()")));

    MemoryContext oldcontext;
    List* destroySlot = NULL;

    Assert(agent);
    Assert(database);
    Assert(user_name);

    /* disconnect if we are still connected */
    if (agent->pool != NULL)
        agent_release_connections(agent, false);

    oldcontext = MemoryContextSwitchTo(agent->mcxt);

    /* Get needed info and allocate memory */
    PgxcNodeGetOids(&agent->coord_conn_oids,
        &agent->dn_conn_oids,
        &agent->num_coord_connections,
        &agent->num_dn_connections,
        false);

    agent->coord_connecting_oids = (Oid*)palloc(agent->num_coord_connections * sizeof(Oid));
    agent->dn_connecting_oids = (Oid*)palloc(agent->num_dn_connections * sizeof(Oid));
    agent->coord_connecting_cnt = 0;
    agent->dn_connecting_cnt = 0;

    agent->coord_connections = (PGXCNodePoolSlot**)palloc0(agent->num_coord_connections * sizeof(PGXCNodePoolSlot*));
    agent->dn_connections = (PGXCNodePoolSlot**)palloc0(agent->num_dn_connections * sizeof(PGXCNodePoolSlot*));

    /* user_name, pgoptions save in agent when pooler is stateless reuse mode. */
    if (ENABLE_STATELESS_REUSE) {
        /* Copy the user name */
        agent->user_name = pstrdup(user_name);

        /* Copy the pgoptions */
        agent->pgoptions = pstrdup(pgoptions);

        agent->has_check_params = false;
    }

    MemoryContextSwitchTo(oldcontext);

    LWLockAcquire(PoolerLock, LW_EXCLUSIVE);

    if (agentCount >= MaxAgentCount - 1) {
        LWLockRelease(PoolerLock);

        ereport(ERROR,
            (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
                errmsg("pooler: Failed to init agent, number of agent reaches MaxAgentCount: %d", MaxAgentCount)));
    }

    /* find database, otherwise create it */
    agent->pool = find_database_pool(database, user_name, pgoptions);
    if (agent->pool == NULL) {
        agent->pool = create_database_pool(database, user_name, pgoptions);
    } else {
        if (agent->pool->userId != GetAuthenticatedUserId()) {
            destroySlot = PoolCleanInvalidConnections(agent->pool);
        }
    }

    /* move here from agent_create(), add to poolAgents after others is done */
    poolAgents[agentCount++] = agent;

    LWLockRelease(PoolerLock);

    /*
     * Pool slots need to be destroyed are not protected in PoolerLock,
     * for these slots will not be reused agagin, and the destroy action
     * may take a long time for big database cluster.
     */
    if (destroySlot) {
        destroy_slots(destroySlot);
    }

    return;
}

/*
 * Destroy PoolAgent
 * remove agent from poolAgents first, so any error in agent_reset_session() and
 * agent_release_connections() can NOT make agent inconsistent.
 * memory may leak on fail of agent_reset_session() or agent_release_connections().
 */
void agent_destroy(PoolAgent* agent)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: agent_destroy()")));

    int i;

    Assert(agent);

    LWLockAcquire(PoolerLock, LW_EXCLUSIVE);

    /* find agent in the list */
    for (i = 0; i < agentCount; i++) {
        if (poolAgents[i] == agent) {
            /* shrink the list and move last agent into the freed slot */
            if (i < --agentCount)
                poolAgents[i] = poolAgents[agentCount];

            /* only one match is expected so exit */
            break;
        }
    }

    LWLockRelease(PoolerLock);

    /* Discard connections if any remaining */
    if (agent->pool != NULL) {
        /*
         * Agent is being destroyed, so reset session parameters
         * before putting back connections to pool.
         * Handle in two different methods: pooler stateless reuse mode or normal mode.
         */
        agent_reset_session(agent, (bool)!ENABLE_STATELESS_REUSE);

        /*
         * Release them all.
         * Force disconnection if there are temporary objects on agent.
         */
        agent_release_connections(agent, agent->temp_namespace != NULL);
    }

    free_agent(agent);
}

/*
 * Release handle to pool manager
 */
void PoolManagerDisconnect(void)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else

    Assert(poolHandle);

    pool_putmessage(&poolHandle->port, 'd', NULL, 0);
    pool_flush(&poolHandle->port);

    close(Socket(poolHandle->port));
    poolHandle = NULL;

#endif
}

// Get pooled connection status.
//
bool PoolManagerConnectionStatus(List* datanodelist, List* coordlist)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: PoolManagerConnectionStatus()")));

    PoolConnDef* pfds = NULL;
    int totlen = list_length(datanodelist) + list_length(coordlist);
    errno_t ss_rc = EOK;
    bool status = true;

    // should NOT happen.
    //
    if (0 == totlen)
        return true;

    // initialize pooler agent if needed.
    //
    PoolManagerInitPoolerAgent();

    // acquire connections from pool.
    //
    PoolAgent* agent = get_agent(u_sess->pgxc_cxt.poolHandle);

    // Release connections in PoolAgent.
    // We can get new connections and get right connection status each time.
    //
    agent_release_connections(agent, true);

    WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_POOLER_GETCONN);

    pfds = (PoolConnDef*)palloc0(sizeof(PoolConnDef));
    pfds->connInfos = (PoolConnInfo*)palloc0(sizeof(PoolConnInfo) * totlen);
    pfds->fds = (int*)palloc0(sizeof(int) * totlen);
    pfds->gsock = (gsocket*)palloc0(sizeof(gsocket) * totlen);
    pfds->pgConn = NULL;
    pfds->pgConnCnt = 0;
    ss_rc = memset_s(pfds->fds, sizeof(int) * totlen, -1, sizeof(int) * totlen);
    securec_check(ss_rc, "\0", "\0");

    PG_TRY();
    {
        /* pooler connect with parallel mode */
        agent_acquire_connections_parallel(agent, pfds, datanodelist, coordlist);
    }
    PG_CATCH();
    {
        if (LWLockHeldByMe(PoolerLock)) {
            HOLD_INTERRUPTS();
            LWLockRelease(PoolerLock);
        }

        ereport(LOG,
            (errcode(ERRCODE_WARNING),
                errmsg("pooler: Failed to get connections, close all fds which have been duplicated!")));

        // If catch an error, just empty the error stack and set the connection status to false.
        //
        FlushErrorState();
        status = false;
    }
    PG_END_TRY();

    // Close all received fds
    //
    for (int i = 0; i < totlen; i++) {
        if (pfds->fds[i] > 0) {
            close(pfds->fds[i]);
            pfds->fds[i] = -1;
        }
    }

    pgxc_node_free_def(pfds);
    pfds = NULL;

    (void)pgstat_report_waitstatus(oldStatus);
    return status;
}

/*
 * Get pooled connections
 */
PoolConnDef* PoolManagerGetConnections(List* datanodelist, List* coordlist)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
#else
    int i;
    ListCell* nodelist_item = NULL;
    int* fds = NULL;
    int totlen = list_length(datanodelist) + list_length(coordlist);
    int nodes[totlen + 2];

    Assert(poolHandle);

    /*
     * Prepare end send message to pool manager.
     * First with Datanode list.
     * This list can be NULL for a query that does not need
     * Datanode Connections (Sequence DDLs)
     */
    nodes[0] = htonl(list_length(datanodelist));
    i = 1;
    if (list_length(datanodelist) != 0) {
        foreach (nodelist_item, datanodelist) {
            nodes[i++] = htonl(lfirst_int(nodelist_item));
        }
    }
    /* Then with Coordinator list (can be nul) */
    nodes[i++] = htonl(list_length(coordlist));
    if (list_length(coordlist) != 0) {
        foreach (nodelist_item, coordlist) {
            nodes[i++] = htonl(lfirst_int(nodelist_item));
        }
    }

    pool_putmessage(&poolHandle->port, 'g', (char*)nodes, sizeof(int) * (totlen + 2));
    pool_flush(&poolHandle->port);

    /* Receive response */
    fds = (int*)palloc(sizeof(int) * totlen);
    if (fds == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }
    if (pool_recvfds(&poolHandle->port, fds, totlen)) {
        pfree(fds);
        return NULL;
    }

    return fds;
#endif
}

/*
 * Abort active transactions using pooler.
 * Take a lock forbidding access to Pooler for new transactions.
 */
int PoolManagerAbortTransactions(const char* dbname, const char* username, ThreadId** pids, bool** isthreadpool)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: PoolManagerAbortTransactions()")));

    PoolAgent* agent = NULL;
    int i, num_proc_ids;

    WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_POOLER_ABORTCONN);
    PoolManagerInitPoolerAgent();
    Assert(u_sess->pgxc_cxt.poolHandle);

    agent = get_agent(u_sess->pgxc_cxt.poolHandle);

    *pids = abort_pids(&num_proc_ids, agent->pid, dbname, username, isthreadpool);

    for (i = 0; i < num_proc_ids; i++) {
        if (gs_signal_send((*pids)[i], SIGTERM, (*isthreadpool)[i]) < 0) {
            ThreadId tid = (*pids)[i];
            pfree_ext(*pids);
            ereport(ERROR,
                (errcode(ERRCODE_SYSTEM_ERROR),
                    errmsg("pooler: Failed to send SIGTERM to openGauss thread:%lu in "
                           "PoolManagerAbortTransactions(), failed: %m",
                        tid)));
        }
    }

    (void)pgstat_report_waitstatus(oldStatus);
    return num_proc_ids;
}

/*
 * Clean up Pooled connections
 */
bool PoolManagerCleanConnection(
    List* datanodelist, List* coordlist, const char* dbname, const char* username, bool is_missing)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: PoolManagerCleanConnection()")));

    int loops = 0;

    List* nodelist = NIL;
    ListCell* item = NULL;
    PoolAgent* agent = NULL;

    PoolManagerInitPoolerAgent();
    Assert(u_sess->pgxc_cxt.poolHandle);

    WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_POOLER_CLEANCONN);
    agent = get_agent(u_sess->pgxc_cxt.poolHandle);

    foreach (item, datanodelist) {
        int index = lfirst_int(item);
        Oid oid = agent->dn_conn_oids[index];
        nodelist = lappend_oid(nodelist, oid);
    }

    foreach (item, coordlist) {
        int index = lfirst_int(item);
        Oid oid = agent->coord_conn_oids[index];
        nodelist = lappend_oid(nodelist, oid);
    }

retry:
    /* Receive result message */
    if (clean_connection(nodelist, dbname, username) != CLEAN_CONNECTION_COMPLETED) {
        /*
         * Wait 0.5 sec in order to void failure.
         */
        pg_usleep(500000);
        if (loops < 3) {
            loops++;
            goto retry;
        } else {
            ereport(WARNING, (errmsg("Clean connections not completed")));
        }
    }

    (void)pgstat_report_waitstatus(oldStatus);
    list_free(nodelist);

    return true;
}

/*
 * Check connection information consistency cached in pooler with catalog information
 */
bool PoolManagerCheckConnectionInfo(void)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: PoolManagerCheckConnectionInfo()")));

    PoolAgent* agent = NULL;
    int res;

    PoolManagerInitPoolerAgent();
    Assert(u_sess->pgxc_cxt.poolHandle);

    PgxcNodeListAndCount();

    agent = get_agent(u_sess->pgxc_cxt.poolHandle);

    /* check in two different methods: pooler stateless reuse mode or normal mode. */
    if (ENABLE_STATELESS_REUSE) {
        res = node_info_check_pooler_stateless(agent);
    } else {
        res = node_info_check(agent);
    }

    if (res == POOL_CHECK_SUCCESS)
        return true;

    return false;
}

/*
 * Reload connection data in pooler and drop all the existing connections of pooler
 */
void PoolManagerReloadConnectionInfo(void)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: PoolManagerReloadConnectionInfo()")));

    PoolManagerInitPoolerAgent();
    Assert(u_sess->pgxc_cxt.poolHandle);

    PgxcNodeListAndCount();

    PoolAgent* agent = get_agent(u_sess->pgxc_cxt.poolHandle);
    reload_database_pools(agent);
}

/*
 * Get all invalid free slot in pooler to destroy
 */
List* PoolCleanInvalidFreeSlot(List* invalid_slots, Oid node_oid, char* invalid_host)
{
    PGXCNodePoolSlot* slot = NULL;
    int i = 0;
    DatabasePool* databasePool = databasePools;

    while (databasePool != NULL) {
        PGXCNodePool* nodePool = NULL;

        nodePool = (PGXCNodePool*)hash_search(databasePool->nodePools, &node_oid, HASH_FIND, NULL);
        if (nodePool == NULL || nodePool->valid == false || nodePool->slot == NULL || nodePool->freeSize == 0) {
            databasePool = databasePool->next;
            continue;
        }

        /* check all free slot is it invlid */
        for (i = 0; i < nodePool->freeSize; i++) {
            slot = nodePool->slot[i];

            if ((slot != NULL) && (slot->conn != NULL) && (strcmp(PQhost(slot->conn), invalid_host) == 0)) {
                /* save invalid slot in list */
                invalid_slots = lappend(invalid_slots, nodePool->slot[i]);
                nodePool->slot[i] = nodePool->slot[nodePool->freeSize - 1];
                nodePool->size--;
                nodePool->freeSize--;
            }
        }

        databasePool = databasePool->next;
    }

    return invalid_slots;
}

/*
 * check whether current node is under connecting process
 */
bool is_current_node_during_connecting(
    PoolAgent* agent, NodeRelationInfo *needClearNodeArray, int waitClearCnt, char node_type)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

/*
 * Get or cancel invalid backends from pooler
 * In pgxc_node relation, the column hostis_primary points out which ip is now primary
 * node ip. if hostis_primary is true, it means the node_host is primary now, on the
 * contrary node_host1 is primary.
 * We scan every connection using in each pooler agent, compare it with node and ip
 * in pgxc_node, if a backend is holding an invalid connections which is connecting to
 * standby, show it or cancel the invalid backend during to user request.
 * if co_node_name specified, validate the pooler connections to it.
 *
 * In multi standby mode, the hostis_primary is not the flag means primary,
 *	We use node_type = 'S' for standby, 'D' for primary.
 */
InvalidBackendEntry* PoolManagerValidateConnection(bool clear, const char* co_node_name, uint32& count)
{
    Relation rel;
    TableScanDesc scan;
    HeapTuple tuple;
    InvalidBackendEntry* entry = NULL;
    int i = 0, j = 0;
    /* invalid slot list, it free in destroy_slots() */
    List* invalid_slots = NIL;
    pg_conn* invalid_conn = NULL;
    NodeRelationInfo needClearNodeArray = {0};

    /* Lock in exclusive mode so we can see a static pooler. */
    LWLockAcquire(PoolerLock, LW_EXCLUSIVE);
    WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_POOLER_CLEANCONN);

    entry = (InvalidBackendEntry*)palloc0(agentCount * sizeof(InvalidBackendEntry));
    for (i = 0; i < agentCount; i++)
        entry[i].node_name = (char**)palloc0(g_instance.attr.attr_network.MaxPoolSize * sizeof(char*));

    rel = heap_open(PgxcNodeRelationId, AccessShareLock);
    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);

    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        Form_pgxc_node node_form = (Form_pgxc_node)GETSTRUCT(tuple);
        Oid node_oid = InvalidOid;
        char* node_name = NULL;
        char* invalid_host = NULL;

        if (node_form->node_type == PGXC_NODE_COORDINATOR &&
            (co_node_name == NULL || strcmp(NameStr(node_form->node_name), co_node_name)) != 0)
            continue;

        node_oid = HeapTupleGetOid(tuple);
        node_name = NameStr(node_form->node_name);
        invalid_host = node_form->hostis_primary ? NameStr(node_form->node_host1) : NameStr(node_form->node_host);

        if (clear)
            invalid_slots = PoolCleanInvalidFreeSlot(invalid_slots, node_oid, invalid_host);

        /*
         * Check all the agents, find the node we are handling and it's slots.
         * Compare connections in slot if we find the invalid host in it, the backend
         * should be canceled later because it's connected to current dynamic standby.
         */
        for (i = 0; i < agentCount; i++) {
            for (j = 0; IS_DATA_NODE(node_form->node_type) && j < poolAgents[i]->num_dn_connections; j++) {
                if (poolAgents[i]->dn_conn_oids[j] == node_oid) {
                    if (poolAgents[i]->dn_connections[j] != NULL && poolAgents[i]->dn_connections[j]->conn != NULL &&
                        (!IS_PRIMARY_DATA_NODE(node_form->node_type) ||
                        (IS_DN_MULTI_STANDYS_MODE() &&
                        strcmp(PQhost(poolAgents[i]->dn_connections[j]->conn), invalid_host) != 0) ||
                        (IS_DN_DUMMY_STANDYS_MODE() &&
                        strcmp(PQhost(poolAgents[i]->dn_connections[j]->conn), invalid_host) == 0))) {
                        /* Shutdown the invalid socket */
                        invalid_conn = poolAgents[i]->dn_connections[j]->conn;
                        if (invalid_conn->is_logic_conn == true) {
                            /* Gsocket has version to check, can asynchronous close it */
                            gs_close_gsocket(&(invalid_conn->libcomm_addrinfo.gs_sock));
                            poolAgents[i]->dn_connections[j]->need_validate = true;
                        } else if (invalid_conn->sock >= 0) {
                            /* Must not close socket here, maybe owner thread is using it */
                            shutdown(invalid_conn->sock, SHUT_RDWR);
                            poolAgents[i]->dn_connections[j]->need_validate = true;
                        }

                        /* current agent is using an invalid host connected to standby */
                        if (entry[i].tid == 0) {
                            if (poolAgents[i]->is_thread_pool_session)
                                entry[i].tid = poolAgents[i]->session_id;
                            else
                                entry[i].tid = poolAgents[i]->pid;

                            entry[i].send_sig_pid = poolAgents[i]->pid;
                            entry[i].is_thread_pool_session = poolAgents[i]->is_thread_pool_session;
                            count++;
                        }
                        entry[i].node_name[entry[i].num_nodes] = pstrdup(node_name);
                        entry[i].total_len += strlen(node_name) + 1;
                        entry[i].is_connecting = false;
                        entry[i].num_nodes++;
                    } else if (is_current_node_during_connecting(
                        poolAgents[i], &needClearNodeArray, 0, node_form->node_type)) {
                        /* current agent is going to connect this invalid node_oid */
                        if (entry[i].tid == 0) {
                            entry[i].tid = poolAgents[i]->pid;
                            entry[i].send_sig_pid = poolAgents[i]->pid;
                            entry[i].is_thread_pool_session = poolAgents[i]->is_thread_pool_session;
                            count++;
                            entry[i].node_name[entry[i].num_nodes] = pstrdup(node_name);
                            entry[i].total_len += strlen(node_name) + 1;
                            entry[i].is_connecting = true;
                            entry[i].num_nodes++;
                        }
                    }
                }
            }
            for (j = 0; PGXC_NODE_COORDINATOR == node_form->node_type && j < poolAgents[i]->num_coord_connections;
                 j++) {
                if (poolAgents[i]->coord_conn_oids[j] == node_oid) {
                    if (poolAgents[i]->coord_connections[j] != NULL &&
                        poolAgents[i]->coord_connections[j]->conn != NULL &&
                        (strcmp(PQhost(poolAgents[i]->coord_connections[j]->conn), invalid_host) == 0)) {
                        /* Shutdown the invalid socket */
                        invalid_conn = poolAgents[i]->coord_connections[j]->conn;
                        if (invalid_conn->sock >= 0) {
                            /* Must not close socket here, maybe owner thread is using it */
                            shutdown(invalid_conn->sock, SHUT_RDWR);
                            poolAgents[i]->coord_connections[j]->need_validate = true;
                        }

                        /* current agent is using an invalid host connected to coordinator */
                        if (entry[i].tid == 0) {
                            if (poolAgents[i]->is_thread_pool_session)
                                entry[i].tid = poolAgents[i]->session_id;
                            else
                                entry[i].tid = poolAgents[i]->pid;

                            entry[i].send_sig_pid = poolAgents[i]->pid;
                            entry[i].is_thread_pool_session = poolAgents[i]->is_thread_pool_session;
                            count++;
                        }
                        entry[i].node_name[entry[i].num_nodes] = pstrdup(node_name);
                        entry[i].total_len += strlen(node_name) + 1;
                        entry[i].is_connecting = false;
                        entry[i].num_nodes++;
                    }
                } else if (is_current_node_during_connecting(
                    poolAgents[i], &needClearNodeArray, 0, node_form->node_type)) {
                    /* current agent is going to connect this invalid node_oid */
                    if (entry[i].tid == 0) {
                        entry[i].tid = poolAgents[i]->pid;
                        entry[i].send_sig_pid = poolAgents[i]->pid;
                        entry[i].is_thread_pool_session = poolAgents[i]->is_thread_pool_session;
                        count++;
                        entry[i].node_name[entry[i].num_nodes] = pstrdup(node_name);
                        entry[i].total_len += strlen(node_name) + 1;
                        entry[i].is_connecting = true;
                        entry[i].num_nodes++;
                    }
                }
            }
        }
    }
    tableam_scan_end(scan);
    heap_close(rel, AccessShareLock);

    /* move all valid entry to low pos */
    for (i = 0, j = 0; i < agentCount; i++) {
        if (entry[i].tid != 0) {
            entry[j].tid = entry[i].tid;
            entry[j].is_thread_pool_session = entry[i].is_thread_pool_session;
            entry[j].send_sig_pid = entry[i].send_sig_pid;
            entry[j].node_name = entry[i].node_name;
            entry[j].num_nodes = entry[i].num_nodes;
            entry[j].total_len = entry[i].total_len;
            j++;
        }
    }
    Assert((int)count == j);

    if (clear) {
        /* now we can cancel all the invalid backends */
        for (i = 0; i < (int)count; i++) {
            if (gs_signal_send(entry[i].send_sig_pid, SIGUSR2, entry[i].is_thread_pool_session) < 0) {
                ereport(WARNING,
                    (errmsg("pooler: Failed to send SIGUSR2 to openGauss thread:%lu in "
                            "PoolManagerValidateConnection(), failed: %m",
                        entry[i].tid)));
            } else {
                ereport(LOG,
                    (errmsg("pooler: Success to send SIGUSR2 to openGauss thread:%lu in "
                            "PoolManagerValidateConnection(), current state: %s.",
                        entry[i].tid,
                        entry[i].is_connecting ? "connecting" : "connected")));
            }
        }
    }
    (void)pgstat_report_waitstatus(oldStatus);
    LWLockRelease(PoolerLock);

    if (clear)
        destroy_slots(invalid_slots);

    return entry;
}

/*
 * Manage a session command for pooler
 */
int agent_session_command(PoolAgent* agent, const char* set_command, PoolCommandType command_type)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: agent_session_command()")));

    int res;

    switch (command_type) {
        case POOL_CMD_LOCAL_SET:
        case POOL_CMD_GLOBAL_SET:
            res = agent_set_command(agent, set_command, command_type);
            break;
        case POOL_CMD_TEMP:
            res = agent_temp_command(agent, set_command);
            break;
        default:
            res = -1;
            break;
    }

    return res;
}

/*
 * Set agent flag that a temporary object is in use.
 */
int agent_temp_command(PoolAgent* agent, const char* namespace_name)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: agent_temp_command()")));

    MemoryContext oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);
    agent->temp_namespace = namespace_name ? pstrdup(namespace_name) : NULL;
    MemoryContextSwitchTo(oldcontext);

    return 0;
}

/*
 * Save a SET command and distribute it to the agent connections
 * already in use.
 */
int agent_set_command(PoolAgent* agent, const char* set_command, PoolCommandType command_type)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: agent_set_command()")));

    char* params_string = NULL;
    int res = 0;
    int rc = -1;

    Assert(agent);
    Assert(set_command);
    Assert(command_type == POOL_CMD_LOCAL_SET || command_type == POOL_CMD_GLOBAL_SET);

    if (command_type == POOL_CMD_LOCAL_SET)
        params_string = agent->local_params;
    else if (command_type == POOL_CMD_GLOBAL_SET)
        params_string = agent->session_params;

    /*
     * Launch the new command to all the connections already hold by the agent
     * It does not matter if command is local or global as this has explicitely been sent
     * by client. openGauss backend also cannot send to its pooler agent SET LOCAL if current
     * transaction is not in a transaction block. This has also no effect on local Coordinator
     * session.
     */
    res = PGXCNodeSendSetParallel(agent, set_command);
    if (res != 0) {
        ereport(LOG,
            (errcode(ERRCODE_WARNING), errmsg("pooler: Failed to send set command: %s to %d nodes", set_command, res)));
        return -1;
    }

    MemoryContext oldcontext = MemoryContextSwitchTo(PoolerMemoryContext);

    /* First command recorded */
    if (params_string == NULL) {
        params_string = pstrdup(set_command);
    } else {
        /*
         * Second command or more recorded.
         * Commands are saved with format 'SET param1 TO value1;...;SET paramN TO valueN'
         */
        char* back_string = NULL;
        const int EXTRA_LEN = 2;

        back_string = pstrdup(params_string);
        pfree_ext(params_string);
        params_string = (char*)palloc0(strlen(back_string) + strlen(set_command) + EXTRA_LEN);
        rc = sprintf_s(
            params_string, strlen(back_string) + strlen(set_command) + EXTRA_LEN, "%s;%s", back_string, set_command);
        securec_check_ss(rc, "\0", "\0");
        pfree(back_string);
    }

    MemoryContextSwitchTo(oldcontext);

    /* Save the latest string */
    if (command_type == POOL_CMD_LOCAL_SET)
        agent->local_params = params_string;
    else if (command_type == POOL_CMD_GLOBAL_SET)
        agent->session_params = params_string;

    return res;
}

void decrease_node_size(DatabasePool* dbpool, Oid nodeid)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: decrease_node_size()")));

    Assert(dbpool);

    bool found = false;
    PGXCNodePool* nodePool = NULL;

    LWLockAcquire(PoolerLock, LW_SHARED);

    nodePool = (PGXCNodePool*)hash_search(dbpool->nodePools, &nodeid, HASH_FIND, &found);
     /* "found == false" means node may be dropped */
    if (found) {
        pthread_mutex_lock(&nodePool->lock);
        if (nodePool->valid && nodePool->size > 0) {
            nodePool->size--;
        }
        pthread_mutex_unlock(&nodePool->lock);
    }
    LWLockRelease(PoolerLock);
}

PGXCNodePoolSlot* alloc_slot_mem(DatabasePool* dbpool)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: alloc_slot_mem()")));

    Assert(dbpool);
    PGXCNodePoolSlot* slot = NULL;

    MemoryContext oldcontext = MemoryContextSwitchTo(dbpool->mcxt);
    PG_TRY();
    {
        slot = (PGXCNodePoolSlot*)palloc0(sizeof(PGXCNodePoolSlot));
        if (ENABLE_STATELESS_REUSE) {
            PoolAgent* agent = get_agent(u_sess->pgxc_cxt.poolHandle);
            slot->user_name = pstrdup(agent->user_name);
            slot->pgoptions = pstrdup(agent->pgoptions);
            slot->session_params = NULL;
        }
    }
    PG_CATCH();
    {
        free_user_name_pgoptions(slot);
        MemoryContextSwitchTo(oldcontext);
        PG_RE_THROW();
    }
    PG_END_TRY();
    MemoryContextSwitchTo(oldcontext);

    return slot;
}

/*
 * LibcommConnectdbParallel: make logic connection via libcomm interface parallel.
 * STEP1: build start up packet and pg_conn for each connection
 * STEP2: build logic connections parallel
 * STEP3: send start up packet to connected node
 * STEP4: wait ready for query from backend which we have sent start up packet.
 *
 * NOTE: conn list for failed connections will be an NULL pointer or the state is BAD_CONNECTION
 * and return no_error = 0 means not all connections succeed, then we will retry the slave for those failed nodes.
 */

int LibcommConnectdbParallel(
    char const* const* const conninfo, int count, PGconn* conn[], const Oid* needCreateNodeArray, PoolGeneralInfo* info)
{
    int i, rc;
    char** startpacket_list = NULL;
    int* packetlen_list = NULL;
    int no_error = 1;
    PGresult* result = NULL;
    Oid node_oid;

    if (conninfo == NULL || conn == NULL || info == NULL) {
        return 0;
    }

    int need_connect = 0;
    libcommaddrinfo** addr_list = (libcommaddrinfo**)palloc0(sizeof(libcommaddrinfo*) * count);
    NodeDefinition** nodesDef = (NodeDefinition**)palloc0(sizeof(NodeDefinition*) * count);
    startpacket_list = (char**)palloc0(count * sizeof(char*));
    packetlen_list = (int*)palloc0(count * sizeof(int));

    /* build pg_conn and startup packet info list */
    for (i = 0; i < count; i++) {
        startpacket_list[i] = PQbuildPGconn(conninfo[i], &conn[i], &packetlen_list[i]);
        node_oid = needCreateNodeArray[i];

        if (startpacket_list[i] == NULL || conn[i] == NULL) {
            ereport(WARNING,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                    errmsg("pooler: failed to get startup packet or pg_conn of datanode %u", node_oid)));

            /* close connection */
            PGXCNodeClose(conn[i]);
            conn[i] = NULL;
            no_error = 0;
            continue;
        }
        if (IS_DN_DUMMY_STANDYS_MODE()) {
            nodesDef[i] = PgxcNodeGetDefinition(node_oid);
        } else {
            Oid current_primary_oid = PgxcNodeGetPrimaryDNFromMatric(node_oid);
            nodesDef[i] = PgxcNodeGetDefinition(current_primary_oid);
        }

        if (nodesDef[i] == NULL) {
            /* No such definition, node is dropped? */
            ereport(WARNING,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                    errmsg("pooler: failed to get nodes def of datanode %u", node_oid)));

            /* close connection */
            PGXCNodeClose(conn[i]);
            conn[i] = NULL;
            no_error = 0;
            continue;
        }

        conn[i]->is_logic_conn = true;

        /*
         * node_mode is given by create_connections_parallel
         * for primary we connect master
         * for standby we connect slave
         */
        if (info->node_mode[i] != POOL_NODE_STANDBY || IS_DN_MULTI_STANDYS_MODE()) {
            conn[i]->libcomm_addrinfo.host = pstrdup(nodesDef[i]->nodehost.data);
            conn[i]->libcomm_addrinfo.ctrl_port = nodesDef[i]->nodectlport;
            conn[i]->libcomm_addrinfo.listen_port = nodesDef[i]->nodesctpport;
        } else {
            conn[i]->libcomm_addrinfo.host = pstrdup(nodesDef[i]->nodehost1.data);
            conn[i]->libcomm_addrinfo.ctrl_port = nodesDef[i]->nodectlport1;
            conn[i]->libcomm_addrinfo.listen_port = nodesDef[i]->nodesctpport1;
        }

        /* use for pg_stat_get_status */
        conn[i]->libcomm_addrinfo.nodeIdx = nodesDef[i]->nodeoid;
        rc = strncpy_s(conn[i]->libcomm_addrinfo.nodename,
            NAMEDATALEN,
            nodesDef[i]->nodename.data,
            strlen(nodesDef[i]->nodename.data));
        securec_check(rc, "\0", "\0");

        addr_list[need_connect++] = &(conn[i]->libcomm_addrinfo);
    }

    /*
     * make connection parallel
     * return -1 when all connections are failed.
     */
    rc = -1;
    if (need_connect != 0)
        rc = gs_connect(addr_list, need_connect, -1);
    if (rc != 0) {
        no_error = 0;
        goto return_result;
    }

    /* send start packet to connected node */
    for (i = 0; i < count; i++) {
        if (conn[i] != NULL && conn[i]->libcomm_addrinfo.gs_sock.type != GSOCK_INVALID) {
            rc = LibcommPacketSend((NODE_CONNECTION*)conn[i], 0, startpacket_list[i], packetlen_list[i]);
            if (rc != STATUS_OK) {
                ereport(WARNING,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
                        errmsg("pooler: failed to send startup packet to remote %s.",
                            conn[i]->libcomm_addrinfo.nodename)));
                conn[i]->status = CONNECTION_BAD;
                no_error = 0;
            } else {
                conn[i]->status = CONNECTION_AWAITING_RESPONSE;
            }
        } else {
            if (conn[i] != NULL)
                conn[i]->status = CONNECTION_BAD;
            no_error = 0;
        }
    }

    /*
     * for which we have sent start up packet
     * we check result
     * here we expect an ready for query reply from backend
     */
    for (i = 0; i < count; i++) {
        if (conn[i] != NULL && conn[i]->status == CONNECTION_AWAITING_RESPONSE) {
            conn[i]->asyncStatus = PGASYNC_BUSY;
            /* LibcommGetResult return null when get ready for query */
            result = LibcommGetResult((PGconn*)conn[i]);
            if (result != NULL || (conn[i]->status == CONNECTION_BAD)) {
                no_error = 0;
            } else {
                conn[i]->status = CONNECTION_OK;
            }
            PQclear(result);
        }
    }

return_result:

    for (i = 0; i < count; i++) {
        if (startpacket_list[i] != NULL) {
            free(startpacket_list[i]);
        }
        pfree_ext(nodesDef[i]);
    }

    pfree_ext(nodesDef);
    pfree_ext(startpacket_list);
    pfree_ext(packetlen_list);
    pfree_ext(addr_list);

    return no_error;
}

/*
 * @Description: create connections parallel.
 * @IN info: result relation info
 * @IN needCreateNodeArray: node to create
 * @IN count: count of the node to create
 * @OUT failedNode: nodes which create failed
 * @Return: 0: all connections success not 0: failed node count
 * @See also:
 */
int create_connections_parallel(
    PoolGeneralInfo *info, NodeRelationInfo *needCreateNodeArray, int needCreateArrayLen, int loopIndex, char* firstError)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                         errmsg("pooler: create_connections_parallel()")));

    PGXCNodePool *nodePool = NULL;
    DatabasePool *dbpool = info->dbpool;
    char **connectionStrs = NULL;
    char *err_msg = NULL;
    char versionStr[40] = {'\0'};
    PGconn **nodeCons = NULL;
    Oid *waitCreateNode = NULL;
    int failedCount = 0;
    int versionLen  = 0;
    errno_t rc = 0;
    char *node_connstr = NULL;
    bool isNeedSlave = false;
    int i = 0;
    int j = 0;
    int waitCreateCount = 0;
    int *indexMap = NULL;
    bool isPrimary = true;
    int node = 0;
    Oid nodeid = 0;
    int connStrSize = 0;

    Assert(needCreateArrayLen > 0);
    waitCreateNode = (Oid*)palloc0(sizeof(Oid) * needCreateArrayLen);
    indexMap = (int*)palloc0(sizeof(int) * needCreateArrayLen);
    connectionStrs = (char**)palloc0(sizeof(char*) * needCreateArrayLen);
    nodeCons = (PGconn**)palloc0(sizeof(PGconn*) * needCreateArrayLen);
    info->conndef->pgConn = nodeCons;
    info->conndef->pgConnCnt = needCreateArrayLen;
    
    rc = snprintf_s(versionStr, sizeof(versionStr), sizeof(versionStr) - 1,
        " backend_version=%u",
        t_thrd.proc ? t_thrd.proc->workingVersionNum : pg_atomic_read_u32(&WorkingGrandVersionNum));
    securec_check_ss(rc, "\0", "\0");

    versionLen = strlen(versionStr);

    /*
     * Design intent:
     * A complete loop attempt is performed. The needCreateNode[i]->isSucceed status is always maintained. 
     * If a node fails to be created and the value of the gs_guc parameter pooler_connect_max_loops is not 0, 
     * a new round of link establishment attempts will be made after the primary node fails to establish a link. 
     * In this case, the j is used to record the node subscript of the, which facilitates the maintenance of the 
     * node status in the needCreateNode. The indexMap stores the mapping between the j and the i, so we could 
     * get the value of i by j easily.
     *
     * for example:
     * int node = needCreateNode[indexMap[j]].nodeIndex;
     * Oid nodeid = needCreateNode[indexMap[j]].nodeList[loopIndex];
     */
    for (i = 0, j = 0; i < needCreateArrayLen; i++) {
        if (needCreateNodeArray[i].isSucceed == true) {
            continue;
        }
        
        node = needCreateNodeArray[i].nodeIndex;
        nodeid = needCreateNodeArray[i].nodeList[loopIndex];
        if (IS_DN_DUMMY_STANDYS_MODE()) {
            isPrimary = is_pgxc_hostprimary(nodeid);
        } else {
            if (info->node_type == POOL_NODE_DN) {
                isPrimary = IS_PRIMARY_DATA_NODE(get_pgxc_nodetype(nodeid));
            }
        }

        isNeedSlave = (loopIndex > 0) ? true : false;
        node_connstr = build_node_conn_str(nodeid, dbpool, isNeedSlave);
        if (node_connstr == NULL) {
            pfree_ext(node_connstr);
            needCreateNodeArray[i].isSucceed = false;
            ++j;
            
            /* Node has been removed or altered */
            ereport(LOG, (errcode(ERRCODE_WARNING),
                errmsg("pooler: could not build connection string 1 for node[%d], oid[%u], "
                    "needCreateArrayLen[%d], loopIndex[%d], i[%d], j[%d]", 
                    node, nodeid, needCreateArrayLen, loopIndex, i, j),
                    errdetail("node(oid:%u) maybe dropped or changed.", nodeid)));
            continue;
        }

        connStrSize  = strlen(node_connstr) + versionLen + 1;
        connectionStrs[j] = (char*)palloc0(connStrSize);
        rc = snprintf_s(connectionStrs[j], connStrSize, connStrSize - 1, "%s%s", node_connstr, versionStr);
        securec_check_ss(rc, "\0", "\0");
        pfree_ext(node_connstr);

        /* LIBCOMMconnectdbParallel need this flag to choose primary or standby node */
        info->node_mode[j] = (isPrimary == true) ? POOL_NODE_PRIMARY : POOL_NODE_STANDBY;
        waitCreateNode[j] = nodeid;
        indexMap[j] = i;
        ++j;
    }
    waitCreateCount = j;
    info->conndef->waitCreateCount = waitCreateCount;

    /* could not find node in the dbpool, return failed. */
    if (failedCount > 0) {
        pfree_ext(connectionStrs);
        pfree_ext(nodeCons);
        info->conndef->pgConn = NULL;
        info->conndef->pgConnCnt = 0;
        info->conndef->waitCreateCount = 0;
        return failedCount;
    }

    int no_error = 0;
    WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_UNDEFINED, true);

    if (info->node_type == POOL_NODE_DN && g_instance.attr.attr_storage.comm_cn_dn_logic_conn) {
        /* build logic connection use libcomm */
        no_error = LibcommConnectdbParallel(connectionStrs, waitCreateCount, nodeCons, waitCreateNode, info);
    } else {
        /* build physic connection use libpq */
        (void)pgstat_report_waitstatus(STATE_POOLER_CREATE_CONN);
        PQconnectdbParallel(connectionStrs, waitCreateCount, nodeCons, waitCreateNode);
    }
    (void)pgstat_report_waitstatus(oldStatus);

    for (j = 0; j < waitCreateCount; j++) {
        node = needCreateNodeArray[indexMap[j]].nodeIndex;
        nodeid = needCreateNodeArray[indexMap[j]].nodeList[loopIndex];

        if (nodeCons[j] && (CONNECTION_OK == nodeCons[j]->status)) {
            PGXCNodePoolSlot *slot = NULL;
            LWLockAcquire(PoolerLock, LW_SHARED);

            /*
             * We may find the nodePool is null if DN switchover appears.
             * So we need to create a new node pool for the primary DN node.
             */
            nodePool = acquire_nodepool(dbpool, nodeid, info->node_type);
            if (nodePool == NULL || !nodePool->valid) {
                LWLockRelease(PoolerLock);
                needCreateNodeArray[indexMap[j]].isSucceed = false;
                pfree_ext(connectionStrs[j]);
                ++failedCount;
                ++j;
                ereport(ERROR, (errmsg("pooler: could not find node of node[%d], oid[%u], needCreateArrayLen[%d], "
                    "loopIndex[%d], i[%d], j[%d], isNull[%d]",
                    node, nodeid, needCreateArrayLen, loopIndex, i, j, ((nodePool == NULL) ? true : nodePool->valid))));
            }

            /*
             * Allocate a new slot for establishing a new connection to data nodes.
             * We first try with the primary connection using the information in
             * system cache if fails to connect, then retry with the other ip.
             */
            slot = alloc_slot_mem(dbpool);
            slot->xc_cancelConn = NULL;
            slot->userid = InvalidOid;

            if (ENABLE_STATELESS_REUSE) {
                slot->userid = GetCurrentUserId();
            }

            slot->conn = nodeCons[j];
            info->conndef->pgConn[j] = NULL;
            slot->xc_cancelConn = (NODE_CANCEL*)PQgetCancel((PGconn*)slot->conn);
            needCreateNodeArray[indexMap[j]].isSucceed = true;
            info->connections[node] = slot;
            LWLockRelease(PoolerLock);
        } else {
            if (firstError[0] == '\0') {
                if (nodeCons[j] == NULL) {
                    err_msg = "out of memory";
                } else if (nodeCons[j]->errorMessage.data != NULL) {
                    err_msg = nodeCons[j]->errorMessage.data;
                } else {
                    err_msg = "";
                }
                
                /* if firstError space is not enough, only copy the front part */
                if (nodeCons[j] && strlen(nodeCons[j]->errorMessage.data) >= INITIAL_EXPBUFFER_SIZE) {
                    nodeCons[j]->errorMessage.data[INITIAL_EXPBUFFER_SIZE - 1] = '\0';
                }

                errno_t ss_rc = EOK;
                ss_rc = strcpy_s(firstError, INITIAL_EXPBUFFER_SIZE, err_msg);
                securec_check(ss_rc, "\0", "\0");
                firstError[INITIAL_EXPBUFFER_SIZE - 1] = '\0';
            }

            /* close connection */
            PGXCNodeClose(nodeCons[j]);
            nodeCons[j] = NULL;

            Assert(no_error == 0);
            needCreateNodeArray[indexMap[j]].isSucceed = false;
            ++failedCount;
        }
        
        pfree_ext(connectionStrs[j]);
    }

    pfree_ext(connectionStrs);
    pfree_ext(nodeCons);
    info->conndef->pgConn = NULL;
    info->conndef->pgConnCnt = 0;
    info->conndef->waitCreateCount = 0;
    pfree_ext(waitCreateNode);
    pfree_ext(indexMap);
    return failedCount;
}


/*
 * @Description: add the active/standby relationship table for retry connections when connect primary node failed.
 * @IN info: pool general info.
 * @IN needCreateArrayLen: nodes count.
 * @INOUT needCreateNodeArray: used for the active/standby relationship table.
 * @Return: slaveNodeNums
 * @See also:
 */
int get_nodeinfo_from_matric(char nodeType, int needCreateArrayLen, NodeRelationInfo *needCreateNodeArray)
{
    int i = 0;
    int slaveNodeNums = 0;
    bool isFind = false;
    Oid *tmpNodeArray = NULL;
    bool *isMatricVisited = NULL;
    
    if (needCreateNodeArray == NULL) {
        ereport(ERROR, (errcode(ERRCODE_WARNING), errmsg("pooler: Failed to "
            "get_nodeInfo_from_matric: needCreateNodeArray is null.")));
    }
    
    if (IS_DN_DUMMY_STANDYS_MODE() || nodeType == PGXC_NODE_COORDINATOR) {
        slaveNodeNums = 1;
        /* total primary dn num is slaveNodeNums, but we will try the dummy node again when the failure occurs */
        tmpNodeArray = (Oid*)palloc0(sizeof(Oid) * needCreateArrayLen * (slaveNodeNums + 1));

        for (i = 0; i < needCreateArrayLen; i++) {
            needCreateNodeArray[i].nodeList = &tmpNodeArray[i * (slaveNodeNums + 1)];
            needCreateNodeArray[i].nodeList[0] = needCreateNodeArray[i].primaryNodeId;
            needCreateNodeArray[i].nodeList[1] = needCreateNodeArray[i].primaryNodeId;
        }        
    } else {
        /* prune to record access nodes and avoid repeated traversal. */
        isMatricVisited = (bool*)palloc0(sizeof(bool) * u_sess->pgxc_cxt.NumDataNodes);
        slaveNodeNums = u_sess->pgxc_cxt.NumStandbyDataNodes / u_sess->pgxc_cxt.NumDataNodes;
        /* total dn num is (slaveNodeNums + 1), we will try every node when the failure occurs. */
        tmpNodeArray = (Oid*)palloc0(sizeof(Oid) * needCreateArrayLen * (slaveNodeNums + 1));
        
        for (i = 0; i < needCreateArrayLen; i++) {
            needCreateNodeArray[i].nodeList = &tmpNodeArray[i * (slaveNodeNums + 1)];
            needCreateNodeArray[i].nodeList[0] = needCreateNodeArray[i].primaryNodeId;

            /* add nodeid about the standby nodes */
            isFind = set_dnoid_info_from_matric(&needCreateNodeArray[i], isMatricVisited);
            if (isFind != true) {
                pfree_ext(tmpNodeArray);
                pfree_ext(isMatricVisited);
                ereport(ERROR, (errcode(ERRCODE_WARNING), errmsg("pooler: Failed to "
                        "get_slave_datanode_oid: can't find slave nodes.")));
            }
        }
        pfree_ext(isMatricVisited);
    }

    return slaveNodeNums;
}


/*
 * @Description: acquire connections parallel.
 * @IN info: pool general info
 * @IN nodelist: node list
 * @OUT needCreateNodeArray: node which need create connection
 * @OUT count: num of the node to create connection
 * @Return: void
 * @See also:
 */
void acquire_connection_parallel(PoolGeneralInfo* info, List* nodelist, NodeRelationInfo* needCreateNodeArray, int* count)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: acquire_connection_parallel()")));

    int i = 0;
    int needCreateArrayLen = 0;
    ListCell* nodelist_item = NULL;
    bool ret = false;
    bool is_dn = (info->node_type == POOL_NODE_DN) ? true : false;

    /* Save in PoolConnDef of data nodes first */
    foreach (nodelist_item, nodelist) {
        int node = lfirst_int(nodelist_item);
        if ((node < 0) || (is_dn && node >= u_sess->pgxc_cxt.NumDataNodes) ||
            (!is_dn && node >= u_sess->pgxc_cxt.NumCoords)) {
            ereport(ERROR,
                (errcode(ERRCODE_NODE_ID_MISSMATCH),
                    errmsg("invalid %s node number: %d, max_value is: %d.",
                        is_dn ? "datanode" : "coordinator",
                        node,
                        is_dn ? u_sess->pgxc_cxt.NumDataNodes : u_sess->pgxc_cxt.NumCoords)));
        }

    retry:
        if (info->connections[node] != NULL && info->connections[node]->conn != NULL &&
            info->connections[node]->need_validate == true) {
            /* Need test connection, if test fail, close it and go to acquire new one */
            if (info->connections[node]->conn->is_logic_conn)
                ret = gs_test_libcomm_conn(&(info->connections[node]->conn->libcomm_addrinfo.gs_sock));
            else
                ret = test_conn(info->connections[node], info->conn_oids[node]);
            if (ret == false) {
                /* close connection, CONNECTION_BAD means no need to send 'X' message */
                info->connections[node]->conn->status = CONNECTION_BAD;
                LWLockAcquire(PoolerLock, LW_SHARED);
                release_connection(info->agent, &(info->connections[node]), info->conn_oids[node], true);
                LWLockRelease(PoolerLock);
            } else {
                /* Expect not to go this branch */
                LWLockAcquire(PoolerLock, LW_SHARED);
                info->connections[node]->need_validate = false;
                LWLockRelease(PoolerLock);
            }
        }

        /* Acquire from the pool if none */
        if (info->connections[node] == NULL) {
            PGXCNodePoolSlot* slot = NULL;
            DatabasePool* dbpool = info->dbpool;
            Oid nodeid = info->conn_oids[node];

            /* get slot from node of node id */
            int rs = get_connection(dbpool, nodeid, &slot, info->node_type);

            switch (rs) {
                case E_OK:
                    if ((slot != NULL) && slot->conn->is_logic_conn) {
                        LIBCOMM_DEBUG_LOG("get_connection to node[nid:%d,sid:%d,ver:%d].",
                            slot->conn->libcomm_addrinfo.gs_sock.idx,
                            slot->conn->libcomm_addrinfo.gs_sock.sid,
                            slot->conn->libcomm_addrinfo.gs_sock.ver);
                        ret = gs_test_libcomm_conn(&(slot->conn->libcomm_addrinfo.gs_sock));
                    } else {
                        ret = test_conn(slot, nodeid);
                    }
                    if (!ret) {
                        /*
                         * If temp obj exists, need_clean is true,
                         * and conn is invalid, destroy it, decrease
                         * counter of pool & retry
                         */
                        ereport(LOG,
                            (errcode(ERRCODE_WARNING),
                                errmsg("pooler: Failed to "
                                       "test conn, retry to get one conn more for node oid: %u.",
                                    nodeid)));

                        release_connection(info->agent, &slot, nodeid, true);

                        goto retry;
                    } else {
                    LWLockAcquire(PoolerLock, LW_SHARED);
                        /* Store in the descriptor */
                        info->connections[node] = slot;
                        info->connections[node]->need_validate = false;
                        LWLockRelease(PoolerLock);
                    }
                    break;
                case E_TO_CREATE:
                    needCreateNodeArray[needCreateArrayLen].nodeIndex = node;
                    needCreateNodeArray[needCreateArrayLen].primaryNodeId = info->conn_oids[node];  
                    needCreateArrayLen++;
                    break;
                case E_NO_FREE_SLOT:
                    pfree_ext(needCreateNodeArray);
                    pfree_ext(info->invalid_nodes);
                    ereport(ERROR,
                        (errcode(ERRCODE_TOO_MANY_CONNECTIONS),
                            errmsg("pooler: The node(oid:%u) has no available slot, "
                                   "the number of slot in use reaches upper limit!",
                                nodeid)));
                    break;
                case E_NO_NODE:
                    /*
                     * run here rarely, cluster changes but
                     * info of agent is not updated until now.
                     */
                    pfree_ext(needCreateNodeArray);
                    pfree_ext(info->invalid_nodes);
                    ereport(ERROR,
                        (errcode(ERRCODE_WARNING), errmsg("pooler: Node(oid:%u) has been removed or altered", nodeid)));
                    break;
                default:
                    /* Never to here */
                    Assert(false);
                    break;
            }

            /*
             * Maybe the connection is OK, they need not create
             * connection, but we must send session params to
             * these nodes.
             */
            if (!info->is_retry)
                info->invalid_nodes[i++] = node;
        }
    }

    if (!info->is_retry)
        info->num_invalid_node = i;

    *count = needCreateArrayLen;
}

/*
 * @Description: send node connection parameters.
 * @IN info: pool general info
 * @IN node: node id
 * @Return: void
 * @See also:
 */
int agent_send_connection_params(PoolGeneralInfo* info, int node, bool is_noexcept)
{
    int rs = 0;

    /*
     * Update newly-acquired slot with session parameters.
     * Local parameters are fired only once BEGIN has been
     * launched on remote nodes.
     */
    PGXCNodePoolSlot* slot = info->connections[node];

    if (info->session_params != NULL) {
        rs = PGXCNodeSendSetQuery(slot->conn, info->session_params);
        if (-1 == rs) {
            LWLockAcquire(PoolerLock, LW_SHARED);
            /* can NOT update session params, destroy the slot */
            release_connection(info->agent, &(info->connections[node]), info->conn_oids[node], true);
            LWLockRelease(PoolerLock);

            if (!is_noexcept) {
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("pooler: Communication failure, failed to send "
                               "session commands or invalid incoming data.")));
            } else {
                ereport(LOG, (errmsg("send session params to node %u failed.", info->conn_oids[node])));
            }
        }
    }

    if (rs != 0)
        return rs;

    /*
     * @Temp Table. When we are acquiring new connection, just like session params,
     * we should set myTempNamespace in new connection, to let temp object's
     * operation be right. We use "set search_path to pg_temp_XXX" to tell new
     * connected DN openGauss set proper temp namespace. see set_config_option
     * in guc.cpp.
     */
    if (info->node_type == POOL_NODE_DN && (info->temp_namespace != NULL)) {
        char sql[NAMEDATALEN + 64] = {0};
        int rc = -1;

        rc = sprintf_s(sql, sizeof(sql), "SET SEARCH_PATH to %s", info->temp_namespace);
        securec_check_ss(rc, "\0", "\0");

        WaitState oldStatus = pgstat_report_waitstatus_comm(STATE_POOLER_WAIT_SETCMD, info->conn_oids[node]);
        rs = PGXCNodeSendSetQuery(slot->conn, sql);
        (void)pgstat_report_waitstatus(oldStatus);

        if (-1 == rs) {
            LWLockAcquire(PoolerLock, LW_SHARED);
            /* can NOT update session params, destroy the slot */
            release_connection(info->agent, &(info->connections[node]), info->conn_oids[node], true);
            LWLockRelease(PoolerLock);

            if (!is_noexcept) {
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("pooler: Communication failure, failed to send "
                               "session commands or invalid incoming data.")));
            } else {
                ereport(LOG, (errmsg("send temp namespace to node %u failed.", info->conn_oids[node])));
            }
        }
    }

    return rs;
}

/*
 * @Description: through one network interaction to send node connection parameters in pooler stateless reuse mode.
 * @IN info: pool general info
 * @IN node: node id
 * @Return: int
 * @See also:
 */
int agent_send_connection_params_pooler_reuse(PoolGeneralInfo* info, int node, bool is_noexcept)
{
    ereport(DEBUG5,
        (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: agent_send_connection_params_pooler_reuse()")));

    int rs = 0;

    /*
     * Update newly-acquired slot with connection parameters.
     */
    if (info->session_params != NULL || ENABLE_STATELESS_REUSE) {
        /*
         * In pooler stateless resue mode to send three kind connection parameters:
         * 1.reset slot connection session for remote node;
         * 2.sent user_name, pgoptions to reset remote node;
         * 3.send session params.
         */
        char sql[PARAMS_LEN] = {0};
        int rc = -1;

        PoolAgent* agent = get_agent(u_sess->pgxc_cxt.poolHandle);
        char* remote_type_option = (char*)(IS_PGXC_COORDINATOR ? "coordinator" : "datanode");
        if (info->session_params != NULL) {
            if (info->node_type == POOL_NODE_DN && (info->temp_namespace != NULL)) {
                rc = sprintf_s(sql,
                    sizeof(sql),
                    "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;@%s@-c remotetype=%s %s@SET SEARCH_PATH to %s@%s",
                    agent->user_name,
                    remote_type_option,
                    agent->pgoptions,
                    info->temp_namespace,
                    info->session_params);
            } else {
                rc = sprintf_s(sql,
                    sizeof(sql),
                    "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;@%s@-c remotetype=%s %s@null;@%s",
                    agent->user_name,
                    remote_type_option,
                    agent->pgoptions,
                    info->session_params);
            }
        } else {
            if (info->node_type == POOL_NODE_DN && (info->temp_namespace != NULL)) {
                rc = sprintf_s(sql,
                    sizeof(sql),
                    "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;@%s@-c remotetype=%s %s@SET SEARCH_PATH to %s@null;",
                    agent->user_name,
                    remote_type_option,
                    agent->pgoptions,
                    info->temp_namespace);
            } else {
                rc = sprintf_s(sql,
                    sizeof(sql),
                    "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;@%s@-c remotetype=%s %s@null;@null;",
                    agent->user_name,
                    remote_type_option,
                    agent->pgoptions);
            }
        }

        securec_check_ss(rc, "\0", "\0");

        rs = PGXCNodeSendSetQueryPoolerStatelessReuse(info->connections[node]->conn, sql);
        if (-1 == rs) {
            LWLockAcquire(PoolerLock, LW_SHARED);
            /* can NOT update connection params, destroy the slot */
            release_connection(info->agent, &(info->connections[node]), info->conn_oids[node], true);
            LWLockRelease(PoolerLock);

            if (!is_noexcept) {
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("pooler: Communication failure, failed to send "
                               "session commands or invalid incoming data.")));
            } else {
                ereport(LOG, (errmsg("send session params to node %u failed.", info->conn_oids[node])));
            }
        }
    }

    return rs;
}

/*
 * @Description: check agent whether need to send node connection parameters for every slot.
 * @IN slot: connection slot
 * @Return: true need, false not need
 * @See also:
 */
bool agent_check_need_send_connection_params_pooler_reuse(PGXCNodePoolSlot* slot)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

/*
 * @Description: wait for send node connection parameters.
 * @IN conn: node conn
 * @Return: 0 OK, others ERROR
 * @See also:
 */
int agent_wait_send_connection_params_pooler_reuse(NODE_CONNECTION* conn)
{
    PGresult* result = NULL;

    /*
     * Consume results from SET commands.
     * If the connection was lost, stop getting result and break the loop.
     */
    if (conn->is_logic_conn) {
        while ((result = LibcommGetResult((NODE_CONNECTION*)conn)) != NULL && conn->status != CONNECTION_BAD) {
            /* If the resultStatus of result is FATAL_ERROR or BAD_RESPONSE, we must return error */
            if (!u_sess->attr.attr_network.comm_stat_mode &&
                (result->resultStatus == PGRES_FATAL_ERROR || result->resultStatus == PGRES_BAD_RESPONSE)) {
                ereport(WARNING,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Get result failed during wait DN for responsing message of set commands: %s.",
                            PQerrorMessage(conn))));

                PQclear(result);
                return -1;
            }
            if (result != NULL) {
                PQclear(result);
                result = NULL;
            }
        }
    } else {
        while ((result = PQgetResult((PGconn*)conn)) != NULL && PQsocket(conn) != -1) {
            /* If the resultStatus of result is FATAL_ERROR or BAD_RESPONSE, we must return error */
            if (!u_sess->attr.attr_network.comm_stat_mode &&
                (result->resultStatus == PGRES_FATAL_ERROR || result->resultStatus == PGRES_BAD_RESPONSE)) {
                ereport(WARNING,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Get result failed during wait DN for responsing message of set commands: %s.",
                            PQerrorMessage(conn))));

                PQclear(result);
                return -1;
            }
            if (result != NULL) {
                PQclear(result);
                result = NULL;
            }
        }
    }

    if (result != NULL)
        PQclear(result);

    if ((PQsocket(conn) == -1 && !conn->is_logic_conn) || PQstatus(conn) == CONNECTION_BAD) {
        ereport(
            WARNING, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Connection error: %s.", PQerrorMessage(conn))));
        return -1;
    }

    return 0;
}

static int agent_send_setquery_parallel(const int* nodelist, int count, PoolGeneralInfo* info, char* query)
{
    int i, ret, node;
    PGconn* conn = NULL;
    int err_count = 0;

    for (i = 0; i < count; ++i) {
        node = nodelist[i];
        if (info->connections[node] == NULL)
            continue;

        conn = (PGconn*)info->connections[node]->conn;

        if (conn->is_logic_conn)
            ret = LibcommSendQuery(conn, query);
        else
            ret = PQsendQuery(conn, query);

        if (ret == 0) {
            ereport(LOG,
                (errmsg("Send query(%s) to node %u [host:%s, port:%s] failed %s.",
                    query,
                    info->conn_oids[node],
                    (conn->pghost != NULL) ? conn->pghost : "unknown",
                    (conn->pgport != NULL) ? conn->pgport : "unknown",
                    PQerrorMessage(conn))));

            LWLockAcquire(PoolerLock, LW_SHARED);
            release_connection(info->agent, &(info->connections[node]), info->conn_oids[node], true);
            LWLockRelease(PoolerLock);
            ++err_count;
        }
    }

    for (i = 0; i < count; ++i) {
        node = nodelist[i];
        if (info->connections[node] == NULL)
            continue;

        conn = (PGconn*)info->connections[node]->conn;

        if (PGXCNodeSetQueryGetResult(conn) == -1) {
            ereport(LOG,
                (errmsg("Wait result for query(%s) from node %u [host:%s, port:%s] failed %s.",
                    query,
                    info->conn_oids[node],
                    (conn->pghost != NULL) ? conn->pghost : "unknown",
                    (conn->pgport != NULL) ? conn->pgport : "unknown",
                    PQerrorMessage(conn))));

            LWLockAcquire(PoolerLock, LW_SHARED);
            release_connection(info->agent, &(info->connections[node]), info->conn_oids[node], true);
            LWLockRelease(PoolerLock);
            ++err_count;
        }
    }

    return err_count;
}

/*
 * @Description: send connection params for parallel.
 * @IN info: pool general info
 * @IN node_list: node list
 * @Return: void
 * @See also:
 */
void agent_send_connection_params_parallel(PoolGeneralInfo* info, List* node_list)
{
    errno_t rc;
    int i;
    ListCell* nodelist_item = NULL;

    PoolConnDef* result = info->conndef;
    int err_count = 0;
    int offset = info->result_offset;
    int* nodes = NULL;
    int nodecount = 0;
    int* fds = result->fds + offset;

    /* Send info to DN if we have session params or temp table namespace. */
    if (info->session_params || info->temp_namespace) {
        if (!u_sess->pgxc_cxt.PoolerResendParams) {
            nodes = info->invalid_nodes;
            nodecount = info->num_invalid_node;
        } else {
            nodes = (int*)palloc(sizeof(int) * info->num_connections);
            for (i = 0; i < info->num_connections; ++i) {
                if (info->connections[i] != NULL)
                    nodes[nodecount++] = i;
            }
        }

        if (info->session_params != NULL) {
            err_count = agent_send_setquery_parallel(nodes, nodecount, info, info->session_params);
        }

        if (err_count == 0 && info->node_type == POOL_NODE_DN && info->temp_namespace) {
            WaitState oldStatus;
            char sql[NAMEDATALEN + 64] = {0};

            rc = sprintf_s(sql, sizeof(sql), "SET SEARCH_PATH to %s", info->temp_namespace);
            securec_check_ss(rc, "\0", "\0");

            oldStatus = pgstat_report_waitstatus_comm(STATE_POOLER_WAIT_SETCMD);

            err_count = agent_send_setquery_parallel(nodes, nodecount, info, sql);

            (void)pgstat_report_waitstatus(oldStatus);
        }

        if (!u_sess->pgxc_cxt.PoolerResendParams)
            nodes = NULL;
    }

    FREE_POINTER(info->invalid_nodes);
    FREE_POINTER(nodes);

    i = 0;

    /* Save in PoolConnDef of data nodes first */
    foreach (nodelist_item, node_list) {
        int node = lfirst_int(nodelist_item);
        if (info->connections[node] == NULL)
            continue;

        result->gsock[offset + i] = GS_INVALID_GSOCK;

        if (info->node_type == POOL_NODE_CN || !g_instance.attr.attr_storage.comm_cn_dn_logic_conn) {
            fds[i] = dup(PQsocket((PGconn*)info->connections[node]->conn));

            /* failed to get a fd */
            if (-1 == fds[i]) {
                ereport(ERROR,
                    (errcode(ERRCODE_OUT_OF_MEMORY),
                        errmsg("pooler: Failed to duplicate fd, error: %s", strerror(errno))));
            }
        }

        if (info->node_type == POOL_NODE_DN) {
            rc = strcpy_s(
                NameStr(result->connInfos[offset + i].host), NAMEDATALEN, PQhost(info->connections[node]->conn));
            securec_check(rc, "\0", "\0");

            char* pqPort = PQport(info->connections[node]->conn);

            if (pqPort != NULL)
                result->connInfos[offset + i].port = strtoul(pqPort, NULL, 10);
            else
                result->connInfos[offset + i].port = 0; /* invalid port */
            if (g_instance.attr.attr_storage.comm_cn_dn_logic_conn) {
                result->gsock[offset + i] = info->connections[node]->conn->libcomm_addrinfo.gs_sock;
            }
        }
        ++i;
    }

    /* error occurred, report error */
    if (err_count > 0)
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("pooler: Communication failure, failed to send "
                       "session commands or invalid incoming data, "
                       "error count: %d.",
                    err_count)));
}

/*
 * @Description: send connection params for parallel in pooler stateless reuse mode.
 * @IN info: pool general info
 * @IN node_list: node list
 * @Return: void
 * @See also:
 */
void agent_send_connection_params_parallel_pooler_reuse(PoolGeneralInfo* info, List* node_list)
{
    ereport(DEBUG5,
        (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
            errmsg("pooler: agent_send_connection_params_parallel_pooler_reuse()")));

    PoolConnDef* result = info->conndef;

    int i = 0;
    int err_count = 0;
    int offset = info->result_offset;

    ListCell* nodelist_item = NULL;
    bool* release_nodes = (bool*)palloc0(sizeof(bool) * info->num_connections);

    errno_t rc;

    int* fds = result->fds + offset;

    PG_TRY();
    {
        if (!u_sess->pgxc_cxt.PoolerResendParams) {
            for (i = 0; i < info->num_invalid_node; ++i) {
                int node = info->invalid_nodes[i];
                PGXCNodePoolSlot* slot = info->connections[node];

                if (!agent_check_need_send_connection_params_pooler_reuse(slot)) {
                    continue;
                }

                release_nodes[node] = true;
                if (agent_send_connection_params_pooler_reuse(info, node, true) != 0)
                    ++err_count;
            }

            WaitState oldStatus = pgstat_report_waitstatus(STATE_POOLER_WAIT_SETCMD);
            for (i = 0; i < info->num_invalid_node; ++i) {
                int node = info->invalid_nodes[i];
                PGXCNodePoolSlot* slot = info->connections[node];

                if (slot == NULL) {
                    continue;
                }

                if (!agent_check_need_send_connection_params_pooler_reuse(slot)) {
                    continue;
                }

                PoolAgent* agent = get_agent(u_sess->pgxc_cxt.poolHandle);

                if (strcmp(slot->user_name, agent->user_name) != 0 || strcmp(slot->pgoptions, agent->pgoptions) != 0) {
                    reload_user_name_pgoptions(info, agent, slot);
                }

                slot->userid = GetCurrentUserId();

                if (agent_wait_send_connection_params_pooler_reuse(slot->conn) != 0) {
                    LWLockAcquire(PoolerLock, LW_SHARED);
                    /* can NOT update connection params, destroy the slot */
                    release_connection(info->agent, &(info->connections[node]), info->conn_oids[node], true);
                    LWLockRelease(PoolerLock);

                    ereport(LOG, (errmsg("send connection params to node %u failed.", info->conn_oids[node])));

                    ++err_count;
                }
                release_nodes[node] = false;
            }
            (void)pgstat_report_waitstatus(oldStatus);
        } else {
            for (i = 0; i < info->num_connections; ++i) {
                if (info->connections[i]) {
                    release_nodes[i] = true;
                    if (agent_send_connection_params_pooler_reuse(info, i, true) != 0)
                        ++err_count;
                }
            }

            WaitState oldStatus = pgstat_report_waitstatus(STATE_POOLER_WAIT_SETCMD);
            for (i = 0; i < info->num_connections; ++i) {
                PGXCNodePoolSlot* slot = info->connections[i];

                if (slot == NULL) {
                    continue;
                }

                PoolAgent* agent = get_agent(u_sess->pgxc_cxt.poolHandle);

                if (strcmp(slot->user_name, agent->user_name) != 0 || strcmp(slot->pgoptions, agent->pgoptions) != 0) {
                    reload_user_name_pgoptions(info, agent, slot);
                }

                slot->userid = GetCurrentUserId();

                if (agent_wait_send_connection_params_pooler_reuse(slot->conn) != 0) {
                    LWLockAcquire(PoolerLock, LW_SHARED);
                    /* can NOT update connection params, destroy the slot */
                    release_connection(info->agent, &(info->connections[i]), info->conn_oids[i], true);
                    LWLockRelease(PoolerLock);

                    ereport(LOG, (errmsg("send connection params to node %u failed.", info->conn_oids[i])));

                    ++err_count;
                }
                release_nodes[i] = false;
            }
            (void)pgstat_report_waitstatus(oldStatus);
        }
    }
    PG_CATCH();
    {
        LWLockAcquire(PoolerLock, LW_EXCLUSIVE);
        for (i = 0; i < info->num_connections; ++i) {
            if (!release_nodes[i])
                continue;
            if (info->connections[i] != NULL) {
                release_connection(info->agent, &(info->connections[i]), info->conn_oids[i], true);
            }
        }
        LWLockRelease(PoolerLock);

        FREE_POINTER(release_nodes);
        FREE_POINTER(info->invalid_nodes);
        PG_RE_THROW();
    }
    PG_END_TRY();

    FREE_POINTER(release_nodes);
    FREE_POINTER(info->invalid_nodes);

    i = 0;

    /* Save in PoolConnDef of data nodes first */
    foreach (nodelist_item, node_list) {
        int node = lfirst_int(nodelist_item);
        if (info->connections[node] == NULL)
            continue;

        result->gsock[offset + i] = GS_INVALID_GSOCK;

        if (info->node_type == POOL_NODE_CN || !g_instance.attr.attr_storage.comm_cn_dn_logic_conn) {
            fds[i] = dup(PQsocket((PGconn*)info->connections[node]->conn));

            /* failed to get a fd */
            if (-1 == fds[i]) {
                ereport(ERROR,
                    (errcode(ERRCODE_OUT_OF_MEMORY),
                        errmsg("pooler: Failed to duplicate fd, error: %s", strerror(errno))));
            }
        }

        if (info->node_type == POOL_NODE_DN) {
            rc = strcpy_s(
                NameStr(result->connInfos[offset + i].host), NAMEDATALEN, PQhost(info->connections[node]->conn));
            securec_check(rc, "\0", "\0");

            char* pqPort = PQport(info->connections[node]->conn);

            if (pqPort != NULL)
                result->connInfos[offset + i].port = strtoul(pqPort, NULL, 10);
            else
                result->connInfos[offset + i].port = 0; /* invalid port */
            if (g_instance.attr.attr_storage.comm_cn_dn_logic_conn) {
                result->gsock[offset + i] = info->connections[node]->conn->libcomm_addrinfo.gs_sock;
            }
        }

        ++i;
    }

    /* error occurred, report error */
    if (err_count > 0) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("pooler: Communication failure, failed to send "
                       "session commands or invalid incoming data, "
                       "error count: %d.",
                    err_count)));
    }
}

int one_round_create_connection(PoolGeneralInfo* info, NodeRelationInfo *needCreateNodeArray, int needCreateArrayLen,
    char* firstError, bool *poolValidateCancel)
{
    int slaveNodeNums = 0;
    int circleLoopNums = 0;
    int failedCount = 0;
    char nodeType;
    
    /* add the active/standby relationship table for retry connections when connect primary node failed. */
    if (needCreateArrayLen > 0) {
        /* only the primary node needs to be tried when PoolerConnectMaxRetries is 0. */ 
        nodeType = (info->node_type == POOL_NODE_CN) ? PGXC_NODE_COORDINATOR : PGXC_NODE_DATANODE;
        slaveNodeNums = get_nodeinfo_from_matric(nodeType, needCreateArrayLen, needCreateNodeArray);
        circleLoopNums = (u_sess->attr.attr_network.PoolerConnectMaxLoops == 0 ||
            nodeType == PGXC_NODE_COORDINATOR) ? 1 : (slaveNodeNums + 1);
    }

    /* Create data node connections parallel */
    for (int i = 0; i < circleLoopNums; i++) {
        failedCount = create_connections_parallel(info, needCreateNodeArray, needCreateArrayLen, i, firstError);
        if (failedCount == 0) {
            break;
        }

        if (g_pq_interrupt_happened == true) {
            *poolValidateCancel = true;
            break;
        }
    }

    return failedCount;
}

void pooler_sleep_interval_time(bool *poolValidateCancel)
{
    for (int i = 0; i < u_sess->attr.attr_network.PoolerConnectIntervalTime; i++) {
        if (g_pq_interrupt_happened == true) {
            *poolValidateCancel = true;
            break;
        }
        /* sleep 1s */
        pg_usleep(1000000);
    }
    return;
}

/*
 * @Description: acquire all nodes connections parallel.
 * @IN info: pool general info
 * @IN nodelist: node list
 * @Return: void
 * @See also:
 */
void acquire_connections_parallel(PoolGeneralInfo* info, List* node_list)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: acquire_connections_parallel()")));

    int loop = 1;
    int i, failedCount;
    char firstError[INITIAL_EXPBUFFER_SIZE] = {0};
    NodeRelationInfo *needCreateNodeArray = NULL;

    info->invalid_nodes = (int*)palloc0(sizeof(int) * info->num_connections);
    info->num_invalid_node = 0;
    info->is_retry = false;
    
retry:
    int needCreateArrayLen = 0;
    bool poolValidateCancel = false;

    needCreateNodeArray = (NodeRelationInfo*)palloc0(sizeof(NodeRelationInfo) * info->num_connections);
    info->node_mode = (PoolNodeMode*)palloc0(sizeof(PoolNodeMode) * info->num_connections);

    if (node_list == NULL) {
        return;
    }

    /*
     * If the connection retry operation occurs, the node
     * that needs to send the parameter is only the result
     * of the first time, or else the retry will reset the
     * node record and the node parameter is inconsistent
     */
    if (loop > 1) {
        info->is_retry = true;
        ereport(LOG, (errmsg("reconnect flag, some connection may be not set params: %s.", info->session_params)));
    }

    /* acquire connection for the nodes in the list, get the nodes to create */
    acquire_connection_parallel(info, node_list, needCreateNodeArray, &needCreateArrayLen);

    failedCount = one_round_create_connection(info, needCreateNodeArray, needCreateArrayLen, firstError, &poolValidateCancel);
    if (failedCount > 0) {
        failedCount = 0;

        /* release node size */
        for (i = 0; i < needCreateArrayLen; ++i) {
            HOLD_INTERRUPTS();
            int node = needCreateNodeArray[i].nodeIndex;

            if (info->connections[node] == NULL) {
                decrease_node_size(info->dbpool, info->conn_oids[node]);
                ++failedCount;
            }
            RESUME_INTERRUPTS();
        }

        if (loop >= u_sess->attr.attr_network.PoolerConnectMaxLoops ||
            info->node_type == POOL_NODE_CN ||
            poolValidateCancel == true) {
            for (i = 0; i < needCreateArrayLen; ++i) {
                if (needCreateNodeArray[i].isSucceed != true) {
                    ereport(WARNING, (errcode(ERRCODE_CONNECTION_FAILURE),
                        errmsg("pooler: failed to make connection to datanode[%u] for thread[%lu]."
                            "Detail: thisLoop[%d], needConnectNums[%d], nodeIdx[%d], fragNodeIdx[%d]",
                            info->conn_oids[needCreateNodeArray[i].nodeIndex], 
                            info->pid, 
                            loop,
                            needCreateArrayLen,
                            needCreateNodeArray[i].nodeIndex,
                            i)));
                }
            }
        } else {
            ereport(LOG, (errmsg("pooler connect failed. Detail: maxloop[%d], thisLoop[%d], needConnectNums[%d], failedNums[%d].", 
                u_sess->attr.attr_network.PoolerConnectMaxLoops, loop, needCreateArrayLen, failedCount)));
            
            pooler_sleep_interval_time(&poolValidateCancel);

            if (poolValidateCancel != true) {
                loop++;
                goto retry;
            }
        }
        
        /* We applay all memory for nodeLists in function get_nodeinfo_from_matric at the first time, the memory is continuous, 
         * so it only needs to be released once. 
         */
        pfree_ext(info->invalid_nodes);
        pfree_ext(needCreateNodeArray[0].nodeList);
        pfree_ext(needCreateNodeArray);
        
        if (poolValidateCancel == true) {
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                    errmsg("pooler: failed to create connections in parallel mode, due to failover, pending")));
        } else {
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                errmsg("pooler: failed to create connections in parallel mode, Error Message: %s", firstError)));
        }
    }
    
    pfree_ext(needCreateNodeArray[0].nodeList);
    pfree_ext(needCreateNodeArray);
    pfree_ext(info->node_mode);

    if (ENABLE_STATELESS_REUSE) {
        /* 
         * In pooler stateless resue mode to send three kind connection parameters:
         * 1.reset slot connetction session for remote node;
         * 2.sent user_name, pgoptions to reset remote node;
         * 3.send session params.
         */
        agent_send_connection_params_parallel_pooler_reuse(info, node_list);
    } else {
        /* send connection params for parallel */
        agent_send_connection_params_parallel(info, node_list);
    }
    return;
}


/*
 * regist connecting nodes on agent
 */
void agent_acquire_connections_start(PoolAgent* agent, List* datanodelist, List* coordlist)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: agent_acquire_connections_start()")));
    Assert(agent);
    Assert(agent->dn_connecting_cnt == 0);
    Assert(agent->coord_connecting_cnt == 0);
    ListCell* nodelist_item = NULL;
    int node;
    LWLockAcquire(PoolerLock, LW_SHARED);

    if (datanodelist != NULL) {
        foreach (nodelist_item, datanodelist) {
            node = lfirst_int(nodelist_item);
            agent->dn_connecting_oids[agent->dn_connecting_cnt++] = agent->dn_conn_oids[node];
        }
    }

    if (coordlist != NULL) {
        foreach (nodelist_item, coordlist) {
            node = lfirst_int(nodelist_item);
            agent->coord_connecting_oids[agent->coord_connecting_cnt++] = agent->coord_conn_oids[node];
        }
    }

    LWLockRelease(PoolerLock);
}

void agent_acquire_connections_end(PoolAgent* agent)
{
    Assert(agent);
    LWLockAcquire(PoolerLock, LW_SHARED);
    agent->dn_connecting_cnt = 0;
    agent->coord_connecting_cnt = 0;
    g_pq_interrupt_happened = false;
    LWLockRelease(PoolerLock);
}

/*
 * @Description: acquire connections parallel.
 * @IN agent: pool agent info
 * @OUT result: pool connect result
 * @IN datanodelist: data nodes list
 * @IN coordlist: coordinator nodes list
 * @Return: void
 * @See also:
 */
void agent_acquire_connections_parallel(PoolAgent* agent, PoolConnDef* result, List* datanodelist, List* coordlist)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: agent_acquire_connections_parallel()")));

    Assert(agent);
    Assert(result);

    PoolGeneralInfo info;

    /* init connect info */
    info.pid = agent->pid;
    info.dbpool = agent->pool;
    info.session_params = MakePoolerSessionParams(POOL_CMD_GLOBAL_SET); /* make new session params */
    info.temp_namespace = agent->temp_namespace;
    info.conndef = result;

    TimestampTz start_dn; /* connect data nodes start time */
    TimestampTz start_cn; /* connect coordinators start time */

    double consume_dn; /* connect dn elapsed time */
    double consume_cn; /* connect cn elapsed time */

    /* Check if pooler can accept those requests */
    if (list_length(datanodelist) > agent->num_dn_connections || list_length(coordlist) > agent->num_coord_connections)
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_FAILURE),
                errmsg("pooler: invalid cn/dn node number, input cn: %d, dn: %d; "
                       "current cn: %d, dn: %d",
                    list_length(coordlist),
                    list_length(datanodelist),
                    agent->num_coord_connections,
                    agent->num_dn_connections)));

    start_dn = GetCurrentTimestamp();

    /* DN connect info */
    info.num_connections = agent->num_dn_connections;
    info.conn_oids = agent->dn_conn_oids;
    info.connections = agent->dn_connections;
    info.result_offset = 0;
    info.node_type = POOL_NODE_DN;

    agent_acquire_connections_start(agent, datanodelist, coordlist);

    /* acquire datanodes connections parallel */
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("Begin DN -> acquire_connections_parallel()")));
    acquire_connections_parallel(&info, datanodelist);
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("End   DN -> acquire_connections_parallel()")));

    /* save connect data nodes elapsed time */
    u_sess->pgxc_cxt.PoolerConnectionInfo->dn_connect_time = (int64)(GetCurrentTimestamp() - start_dn);

    consume_dn = (double)(u_sess->pgxc_cxt.PoolerConnectionInfo->dn_connect_time * 1.0) / MSECS_PER_SEC;
    /* write the total elapsed time into log */
    ereport(DEBUG1, (errmsg("Parallel Connect DataNodes End, elapsed time: %.3fms", consume_dn)));

    start_cn = GetCurrentTimestamp();

    /* CN connect info */
    info.num_connections = agent->num_coord_connections;
    info.conn_oids = agent->coord_conn_oids;
    info.connections = agent->coord_connections;
    info.result_offset = list_length(datanodelist);
    info.node_type = POOL_NODE_CN;

    /* acquire coordinators connections parallel */
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("Begin CN -> acquire_connections_parallel()")));
    acquire_connections_parallel(&info, coordlist);
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("End   CN -> acquire_connections_parallel()")));

    agent_acquire_connections_end(agent);
    /* save connect coordinators elapsed time */
    u_sess->pgxc_cxt.PoolerConnectionInfo->cn_connect_time = (int64)(GetCurrentTimestamp() - start_cn);

    consume_cn = (double)(u_sess->pgxc_cxt.PoolerConnectionInfo->cn_connect_time * 1.0) / MSECS_PER_SEC;

    /* write the total elapsed time into log */
    ereport(DEBUG1,
        (errmsg(
            "Connect Coordinator End, elapsed time: %.3fms, total time: %.3fms", consume_cn, consume_dn + consume_cn)));

    return;
}

/*
 * send transaction local commands if any, set the begin sent status in any case
 */
int send_local_commands(PoolAgent* agent, int dn_count, const int* dn_list, int co_count, const int* co_list)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: send_local_commands()")));

    int tmp;
    int res;
    PGXCNodePoolSlot* slot = NULL;
    WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_UNDEFINED, true);

    Assert(agent);

    res = 0;

    if (dn_list != NULL) {
        res = dn_count;
        if (res > 0 && agent->dn_connections == NULL)
            return 0;

        for (int i = 0; i < dn_count; i++) {
            int node = dn_list[i];

            if (node < 0 || node >= agent->num_dn_connections)
                continue;

            slot = agent->dn_connections[node];

            if (slot == NULL)
                continue;

            if (agent->local_params != NULL) {
                pgstat_report_waitstatus_comm(STATE_POOLER_WAIT_SETCMD, agent->dn_conn_oids[node]);
                tmp = PGXCNodeSendSetQuery(slot->conn, agent->local_params);
                if (tmp == -1) {
                    ereport(LOG,
                        (errcode(ERRCODE_WARNING),
                            errmsg("pooler: Failed to send local command, dn oid: %u, local command: %s",
                                agent->dn_conn_oids[node],
                                agent->local_params)));
                    LWLockAcquire(PoolerLock, LW_SHARED);
                    release_connection(agent, &(agent->dn_connections[node]), agent->dn_conn_oids[node], true);
                    LWLockRelease(PoolerLock);
                    (void)pgstat_report_waitstatus(oldStatus);
                    return -1;
                }
                res = res + tmp;
            }
        }
    }

    if (co_list != NULL) {
        res = co_count;
        if (res > 0 && agent->coord_connections == NULL)
            return 0;

        for (int i = 0; i < co_count; i++) {
            int node = co_list[i];

            if (node < 0 || node >= agent->num_coord_connections)
                continue;

            slot = agent->coord_connections[node];

            if (slot == NULL)
                continue;

            if (agent->local_params != NULL) {
                pgstat_report_waitstatus_comm(STATE_POOLER_WAIT_SETCMD, agent->coord_conn_oids[node]);
                tmp = PGXCNodeSendSetQuery(slot->conn, agent->local_params);
                if (tmp == -1) {
                    ereport(LOG,
                        (errcode(ERRCODE_WARNING),
                            errmsg("pooler: Failed to send local command, dn oid: %u, local command: %s",
                                agent->coord_conn_oids[node],
                                agent->local_params)));
                    LWLockAcquire(PoolerLock, LW_SHARED);
                    release_connection(agent, &(agent->coord_connections[node]), agent->coord_conn_oids[node], true);
                    LWLockRelease(PoolerLock);
                    (void)pgstat_report_waitstatus(oldStatus);
                    return -1;
                }
                res = res + tmp;
            }
        }
    }
    (void)pgstat_report_waitstatus(oldStatus);

    if (res < 0)
        return -res;
    return 0;
}

// stop query on connection
int stop_query_on_connections(PoolAgent* agent, int dn_count, const int* dn_list)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: stop_query_on_connections()")));

    char errbuf[256];
    bool bRet = false;
    int nCount;
    int nRealCount;
    errno_t serrno = EOK;

    nCount = 0;
    nRealCount = 0;

    Assert(agent);
    WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_UNDEFINED, true);

    /* Send stop on Datanodes */
    for (int i = 0; i < dn_count; i++) {
        int node = dn_list[i];

        if (node < 0 || node >= agent->num_dn_connections)
            continue;

        if (agent->dn_connections == NULL)
            break;

        if (agent->dn_connections[node] == NULL)
            continue;

        nRealCount++;
        pgstat_report_waitstatus_comm(STATE_POOLER_WAIT_STOP, agent->dn_conn_oids[node]);
        serrno = memset_s(errbuf, sizeof(errbuf), '\0', sizeof(errbuf));
        securec_check(serrno, "\0", "\0");
        if (agent->dn_connections[node]->conn->is_logic_conn)
            bRet = LibcommStopQuery((NODE_CONNECTION*)agent->dn_connections[node]->conn);
        else
            bRet = PQstop_timeout((PGcancel*)agent->dn_connections[node]->xc_cancelConn,
                errbuf,
                sizeof(errbuf),
                u_sess->attr.attr_network.PoolerCancelTimeout,
                u_sess->debug_query_id);
        if (!bRet) {
            ereport(LOG,
                (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                    errmsg("pooler stop connection timeout, nodeoid %u: %s", agent->dn_conn_oids[node], errbuf)));
        } else {
            nCount++;
        }
    }
    (void)pgstat_report_waitstatus(oldStatus);

    return nCount != nRealCount;
}

/*
 * Cancel query
 */
int cancel_query_on_connections(PoolAgent* agent, int dn_count, const int* dn_list, int co_count, const int* co_list)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: cancel_query_on_connections()")));

    char errbuf[256];
    bool bRet = false;
    int nCount;
    int nRealCount;
    errno_t serrno = EOK;

    nCount = 0;
    nRealCount = 0;

    Assert(agent);
    WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_UNDEFINED, true);
    /* Send cancel on Datanodes first */
    for (int i = 0; i < dn_count; i++) {
        int node = dn_list[i];

        if (node < 0 || node >= agent->num_dn_connections)
            continue;

        if (agent->dn_connections == NULL)
            break;

        if (agent->dn_connections[node] == NULL)
            continue;

        nRealCount++;
        pgstat_report_waitstatus_comm(STATE_POOLER_WAIT_CANCEL, agent->dn_conn_oids[node]);
        serrno = memset_s(errbuf, sizeof(errbuf), '\0', sizeof(errbuf));
        securec_check(serrno, "\0", "\0");

        if (agent->dn_connections[node]->conn->is_logic_conn)
            bRet = LibcommCancelOrStop((NODE_CONNECTION*)agent->dn_connections[node]->conn,
                errbuf,
                sizeof(errbuf),
                u_sess->attr.attr_network.PoolerCancelTimeout,
                CANCEL_REQUEST_CODE);
        else
            bRet = PQcancel_timeout((PGcancel*)agent->dn_connections[node]->xc_cancelConn,
                errbuf,
                sizeof(errbuf),
                u_sess->attr.attr_network.PoolerCancelTimeout);

        if (!bRet) {
            ereport(LOG,
                (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                    errmsg("pooler cancel connection timeout, nodeoid %d: %s", agent->dn_conn_oids[node], errbuf)));
        } else {
            nCount++;
        }
    }

    /* Send cancel to Coordinators too, e.g. if DDL was in progress */
    for (int i = 0; i < co_count; i++) {
        int node = co_list[i];

        if (node < 0 || node >= agent->num_coord_connections)
            continue;

        if (agent->coord_connections == NULL)
            break;

        if (agent->coord_connections[node] == NULL)
            continue;

        nRealCount++;
        pgstat_report_waitstatus_comm(STATE_POOLER_WAIT_CANCEL, agent->coord_conn_oids[node]);
        serrno = memset_s(errbuf, sizeof(errbuf), '\0', sizeof(errbuf));
        securec_check(serrno, "\0", "\0");
        bRet = PQcancel_timeout((PGcancel*)agent->coord_connections[node]->xc_cancelConn,
            errbuf,
            sizeof(errbuf),
            u_sess->attr.attr_network.PoolerCancelTimeout);
        if (!bRet) {
            ereport(LOG,
                (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                    errmsg("pooler cancel connection timeout, nodeoid %d: %s", agent->coord_conn_oids[node], errbuf)));
        } else {
            nCount++;
        }
    }
    (void)pgstat_report_waitstatus(oldStatus);

    return nCount != nRealCount;
}

/*
 * @Description: Connect consumer with TCP
 *
 * @param[IN] datanodelist:  datanode list of consumer
 * @param[IN] consumerDop:  consumer DOP
 * @param[IN] distriType:  data distribute type
 * @param[IN] nodeDef:  all datanode definition
 * @param[IN] addrArray:  addr info array
 * @param[IN] key:  stream key
 * @param[IN] nodeoid:  node oid of current datanode
 * @return: int*, fds array
 */
int* StreamConnectNodes(List* datanodelist, int consumerDop, int distriType, NodeDefinition* nodeDef,
    struct addrinfo** addrArray, StreamKey key, Oid nodeoid)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * @Description: Connect consumer with SCTP
 *
 * @param[IN] addrArray:  addr info array
 * @param[IN] connNum:  connection number
 * @return: int*, stream ids array
 */
int* StreamConnectNodes(libcommaddrinfo** addrArray, int connNum)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * Return connections back to the pool
 */
void PoolManagerReleaseConnections(const char* status_array, int array_size, bool has_error)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: PoolManagerReleaseConnections()")));

#ifdef USE_ASSERT_CHECKING
    instr_time t_start, t_end;
    INSTR_TIME_SET_CURRENT(t_start);
#endif

    PoolManagerInitPoolerAgent();
    Assert(u_sess->pgxc_cxt.poolHandle);

    PoolAgent* agent = get_agent(u_sess->pgxc_cxt.poolHandle);

    bool force_destroy = false;

    if (agent && agent->pool && agent->pool->userId != GetAuthenticatedUserId()) {
        force_destroy = true;
    }

    // Deal with the situations where SET GUC commands are run within a Transaction block
    if (ENABLE_STATELESS_REUSE && !agent->has_check_params) {
        if (agent->session_params != NULL) {
            agent->has_check_params = true;
        }
    }

    if (!has_error) {
        /*
         * we do not have to cache the database template1 connection,
         * the reason is that its cache thread will lead to other nodes
         * in the creation of the database failed
         */
        if (strcmp(agent->pool->database, "template1") == 0)
            force_destroy = true;

        agent_release_connections(agent, force_destroy);
    } else {
        if (NULL == status_array) {
            force_destroy = true;
        } else if ((int)strlen(status_array) != agent->num_dn_connections + agent->num_coord_connections) {
            status_array = NULL;
            force_destroy = true;
        }

        agent_release_connections(agent, force_destroy, status_array);
    }

#ifdef USE_ASSERT_CHECKING
    INSTR_TIME_SET_CURRENT(t_end);
    INSTR_TIME_SUBTRACT(t_end, t_start);
    elog(DEBUG1, "pooler: time of PoolManagerReleaseConnections(): %ums", (uint32)INSTR_TIME_GET_MILLISEC(t_end));
#endif
}

/*
 * Stop Query
 * Return 0 means all nodes got stop, otherwise not all.
 */
int PoolManagerStopQuery(int dn_count, const int* dn_list)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: PoolManagerStopQuery()")));

    int rs;
    PoolAgent* agent = NULL;

    if (u_sess->pgxc_cxt.poolHandle == NULL)
        return 0;

    if (dn_count == 0)
        return 0;

    if (dn_list == NULL) {
        ereport(LOG,
            (errcode(ERRCODE_WARNING), errmsg("poolmgr stop query failed, dn_count=%d but dn_list is NULL", dn_count)));
        return 0;
    }

    agent = get_agent(u_sess->pgxc_cxt.poolHandle);
    rs = stop_query_on_connections(agent, dn_count, dn_list);
    return rs;
}

/*
 * Cancel Query
 * Return 0 means all nodes got cancel, otherwise not all.
 */
int PoolManagerCancelQuery(int dn_count, const int* dn_list, int co_count, const int* co_list)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: PoolManagerCancelQuery()")));

    int rs;
    PoolAgent* agent = NULL;

    if (u_sess->pgxc_cxt.poolHandle == NULL)
        return 0;

    if (dn_count == 0 && co_count == 0)
        return 0;

    if (dn_count != 0 && dn_list == NULL) {
        ereport(LOG,
            (errcode(ERRCODE_WARNING),
                errmsg("poolmgr cancel query failed, dn_count=%d but dn_list is NULL", dn_count)));
        return 0;
    }

    if (co_count != 0 && co_list == NULL) {
        ereport(LOG,
            (errcode(ERRCODE_WARNING),
                errmsg("poolmgr cancel query failed, cn_count=%d but cn_list is NULL", co_count)));
        return 0;
    }

    agent = get_agent(u_sess->pgxc_cxt.poolHandle);
    rs = cancel_query_on_connections(agent, dn_count, dn_list, co_count, co_list);
    return rs;
}

/*
 * Release connections for Datanodes and Coordinators
 * If the 'status_array' is not NULL, 'force_destroy' is useless.
 */
void agent_release_connections(PoolAgent* agent, bool force_destroy, const char* status_array)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: agent_release_connections()")));

    Assert(agent);

    //	MemoryContext oldcontext;
    int i;

    if (agent == NULL) {
        ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("pooler: agent_release_connections: agent is null")));
    }

    if ((agent->dn_connections == NULL) && (agent->coord_connections == NULL))
        return;

    /*
     * During AbortTransaction, this function is called before LWLockReleaseAll, We need
     * consider this function after LWLockReleaseAll later. But here, for safety, just do
     * little change to resolve this deadlock in hurry. If error occur after Hold PoolerLock,
     * We need release it here. But we must refactor pooler code later.
     */
    if (LWLockHeldByMe(PoolerLock)) {
        /* match the upcoming RESUME_INTERRUPTS */
        HOLD_INTERRUPTS();
        LWLockRelease(PoolerLock);
    }

    /*
     * If there are some session parameters or temporary objects,
     * do not put back connections to pool.
     * Disconnection will be made when session is cut for this user.
     * Local parameters are reset when transaction block is finished,
     * so don't do anything for them, but just reset their list.
     */
    if (agent->local_params != NULL) {
        LWLockAcquire(PoolerLock, LW_SHARED);
        pfree_ext(agent->local_params);
        LWLockRelease(PoolerLock);
    }

    u_sess->pgxc_cxt.PoolerResendParams = false;

    /*
     * Add PoolerStatelessReuse parameter if pooler is set in stateless reuse mode.
     */
    if (!ENABLE_STATELESS_REUSE) {
        if (((!check_connection_reuse() && (agent->session_params != NULL)) || (agent->temp_namespace != NULL)) &&
            !force_destroy && (status_array == NULL))
            return;
    } else {
        if ((agent->temp_namespace != NULL) && !force_destroy && (status_array == NULL))
            return;
    }

    /*
     * There are possible memory allocations in the core pooler, we want
     * these allocations in the contect of the database pool
     */
    /* grow_pool is not called in release_connection() any more, so memory
     * context switch is NOT necessary */

    // If status_array do not define, use 'force_destroy' for all the connections.
    if (NULL == status_array) {
        /*
         * Remaining connections are assumed to be clean.
         * First clean up for Datanodes
         */
        for (i = 0; i < agent->num_dn_connections; i++) {
            /*
             * Release connection.
             * If connection has temporary objects on it, destroy connection slot.
             */
            LWLockAcquire(PoolerLock, LW_SHARED);
            if (agent->dn_connections[i] != NULL)
                release_connection(agent, &(agent->dn_connections[i]), agent->dn_conn_oids[i], force_destroy);
            LWLockRelease(PoolerLock);
        }
        /* Then clean up for Coordinator connections */
        for (i = 0; i < agent->num_coord_connections; i++) {
            /*
             * Release connection.
             * If connection has temporary objects on it, destroy connection slot.
             */
            LWLockAcquire(PoolerLock, LW_SHARED);
            if (agent->coord_connections[i] != NULL)
                release_connection(agent, &(agent->coord_connections[i]), agent->coord_conn_oids[i], force_destroy);
            LWLockRelease(PoolerLock);
        }

        if (check_connection_reuse())
            u_sess->pgxc_cxt.PoolerResendParams = true;
    } else {
        /* Release each connection by it's status */
        bool is_force = false;

        /*
         * Remaining connections are assumed to be clean.
         * First clean up for Datanodes
         */
        for (i = 0; i < agent->num_dn_connections; i++, status_array++) {
            is_force = (*status_array == CONN_STATE_NORMAL) ? false : true;

            /*
             * Check if pooler is set in stateless reuse mode.
             */
            if (!ENABLE_STATELESS_REUSE) {
                if (!is_force && (agent->session_params != NULL))
                    continue;
            }

            /*
             * Release connection.
             * If connection status is error, destroy connection slot.
             */
            LWLockAcquire(PoolerLock, LW_SHARED);
            if (agent->dn_connections[i] != NULL)
                release_connection(agent, &(agent->dn_connections[i]), agent->dn_conn_oids[i], is_force);
            LWLockRelease(PoolerLock);
        }

        /* Then clean up for Coordinator connections */
        for (i = 0; i < agent->num_coord_connections; i++, status_array++) {
            is_force = (*status_array == CONN_STATE_NORMAL) ? false : true;

            /*
             * Check if pooler is set in stateless reuse mode.
             */
            if (!ENABLE_STATELESS_REUSE) {
                if (!is_force && (agent->session_params != NULL))
                    continue;
            }

            /*
             * Release connection.
             * If connection status is error, destroy connection slot.
             */
            LWLockAcquire(PoolerLock, LW_SHARED);
            if (agent->coord_connections[i] != NULL) {
                release_connection(agent, &(agent->coord_connections[i]), agent->coord_conn_oids[i], is_force);
            }
            LWLockRelease(PoolerLock);
        }
    }
}

/*
 * Reset session parameters for given connections in the agent.
 * This is done before putting back to pool connections that have been
 * modified by session parameters.
 */
void agent_reset_session(PoolAgent* agent, bool need_reset)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

/*
 * Create new empty pool for a database.
 * By default Database Pools have a size null so as to avoid interactions
 * between PGXC nodes in the cluster (Co/Co, Dn/Dn and Co/Dn).
 * Pool is increased at the first GET_CONNECTION message received.
 * Returns POOL_OK if operation succeed POOL_FAIL in case of OutOfMemory
 * error and POOL_WEXIST if poll for this database already exist.
 */
DatabasePool* create_database_pool(const char* database, const char* user_name, const char* pgoptions)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: create_database_pool()")));

    MemoryContext oldcontext;
    MemoryContext dbcontext;
    DatabasePool* databasePool = NULL;
    HASHCTL hinfo;
    int hflags;

    dbcontext = PoolerCoreContext;
    oldcontext = MemoryContextSwitchTo(dbcontext);
    /* Allocate memory */
    databasePool = (DatabasePool*)palloc0(sizeof(DatabasePool));
    databasePool->mcxt = dbcontext;
    /* Copy the database name */
    databasePool->database = pstrdup(database);

    /* In pooler stateless reuse mode, databasePool is stateless: user_name and pgoptions are NULL */
    if (ENABLE_STATELESS_REUSE) {
        databasePool->user_name = NULL;
        databasePool->pgoptions = NULL;
    } else {
        /* Copy the user name */
        databasePool->user_name = pstrdup(user_name);
        /* Copy the user oid */
        databasePool->userId = GetAuthenticatedUserId();
        /* Copy the pgoptions */
        databasePool->pgoptions = pstrdup(pgoptions);
    }

    /* Init next reference */
    databasePool->next = NULL;

    /* Init node hashtable */
    check_memset_s(memset_s(&hinfo, sizeof(hinfo), 0, sizeof(hinfo)));
    hflags = 0;

    hinfo.hash = oid_hash;
    hflags = (unsigned int)hflags | HASH_FUNCTION;

    hinfo.keysize = sizeof(Oid);
    hinfo.entrysize = sizeof(PGXCNodePool);
    hflags = (unsigned int)hflags | HASH_ELEM;

    hinfo.hcxt = dbcontext;
    hflags = (unsigned int)hflags | HASH_SHRCTX;

    databasePool->nodePools = hash_create(
        "Node Pool", g_instance.attr.attr_common.MaxDataNodes + g_instance.attr.attr_network.MaxCoords, &hinfo, hflags);

    MemoryContextSwitchTo(oldcontext);

    /* Insert into the list */
    if (databasePools != NULL)
        databasePool->next = databasePools;
    else
        databasePool->next = NULL;

    /* Update head pointer */
    databasePools = databasePool;

    return databasePool;
}

/*
 * Rebuild information of database pools
 */
void reload_database_pools(PoolAgent* agent)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: reload_database_pools()")));

    Assert(agent);

    List* slots = NIL;
    DatabasePool* databasePool = NULL;

    /*
     * Release node connections if any held. It is not guaranteed client session
     * does the same so don't ever try to return them to pool and reuse
     */
    agent_release_connections(agent, true);

    LWLockAcquire(PoolerLock, LW_EXCLUSIVE);

    /* Forget previously allocated node info */
    FREE_POINTER(agent->coord_connections);
    FREE_POINTER(agent->coord_conn_oids);
    FREE_POINTER(agent->dn_connections);
    FREE_POINTER(agent->dn_conn_oids);
    FREE_POINTER(agent->dn_connecting_oids);
    FREE_POINTER(agent->coord_connecting_oids);

    PG_TRY();
    {
        /* and allocate new */
        MemoryContext oldcontext = MemoryContextSwitchTo(agent->mcxt);
        PgxcNodeGetOids(&agent->coord_conn_oids,
            &agent->dn_conn_oids,
            &agent->num_coord_connections,
            &agent->num_dn_connections,
            false);

        agent->coord_connecting_oids = (Oid*)palloc(agent->num_coord_connections * sizeof(Oid));
        agent->dn_connecting_oids = (Oid*)palloc(agent->num_dn_connections * sizeof(Oid));
        agent->coord_connecting_cnt = 0;
        agent->dn_connecting_cnt = 0;

        agent->coord_connections =
            (PGXCNodePoolSlot**)palloc0(agent->num_coord_connections * sizeof(PGXCNodePoolSlot*));
        agent->dn_connections = (PGXCNodePoolSlot**)palloc0(agent->num_dn_connections * sizeof(PGXCNodePoolSlot*));
        MemoryContextSwitchTo(oldcontext);
    }
    PG_CATCH();
    {
        HOLD_INTERRUPTS();
        LWLockRelease(PoolerLock);
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("pooler: Failed to reset agent!")));
    }
    PG_END_TRY();

    /*
     * Scan the list and destroy any altered pool. They will be recreated
     * upon subsequent connection acquisition.
     */
    databasePool = databasePools;
    while (databasePool != NULL) {
        /* Update each database pool slot with new connection information */
        HASH_SEQ_STATUS hseq_status;
        PGXCNodePool* nodePool = NULL;

        hash_seq_init(&hseq_status, databasePool->nodePools);
        while ((nodePool = (PGXCNodePool*)hash_seq_search(&hseq_status))) {
            if (false == nodePool->valid) {
                pfree_ext(nodePool->connstr);
                pfree_ext(nodePool->connstr1);
                pfree_ext(nodePool->slot);
                hash_search(databasePool->nodePools, &nodePool->nodeoid, HASH_REMOVE, NULL);
                continue;
            }

            char* connstr_chk = build_node_conn_str(nodePool->nodeoid, databasePool, false);
            char* connstr_ch1k = NULL;

            if (IS_DN_DUMMY_STANDYS_MODE()) {
                connstr_ch1k = build_node_conn_str(nodePool->nodeoid, databasePool, true);
            }

            /* Pooler not in stateless reuse mode */
            if (!ENABLE_STATELESS_REUSE) {
                if (connstr_chk == NULL || strcmp(connstr_chk, nodePool->connstr) ||
                    (IS_DN_DUMMY_STANDYS_MODE() && connstr_ch1k != NULL && strcmp(connstr_ch1k, nodePool->connstr1))) {
                    /* Node has been removed or altered */
                    ereport(DEBUG1,
                        (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                            errmsg("pooler: node(oid:%u) would be destroy. old connstr: %s, "
                                   "old connstr1: %s, new connstr: %s, new connstr1: %s",
                                nodePool->nodeoid,
                                nodePool->connstr,
                                nodePool->connstr1,
                                (connstr_chk == NULL) ? "" : connstr_chk,
                                (connstr_ch1k == NULL) ? "" : connstr_ch1k)));

                    slots = destroy_node_pool(nodePool, slots);
                    hash_search(databasePool->nodePools, &nodePool->nodeoid, HASH_REMOVE, NULL);
                }
            }

            pfree_ext(connstr_chk);
            pfree_ext(connstr_ch1k);
        }

        databasePool = databasePool->next;
    }
    LWLockRelease(PoolerLock);

    destroy_slots(slots);
}

/*
 * Find pool for specified database and username in the list
 */
DatabasePool* find_database_pool(const char* database, const char* user_name, const char* pgoptions)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: find_database_pool()")));

    DatabasePool* databasePool = NULL;

    /* In pooler stateless reuse mode, every databasePool can reuse for all agent */
    if (ENABLE_STATELESS_REUSE) {
        /* Scan the list */
        databasePool = databasePools;
        while (databasePool != NULL) {
            if (strcmp(database, databasePool->database) == 0)
                break;

            databasePool = databasePool->next;
        }
    } else {
        /* Scan the list */
        databasePool = databasePools;
        while (databasePool != NULL) {
            if (strcmp(database, databasePool->database) == 0 && strcmp(user_name, databasePool->user_name) == 0 &&
                strcmp(pgoptions, databasePool->pgoptions) == 0)
                break;

            databasePool = databasePool->next;
        }
    }

    return databasePool;
}

/*
 * find node pool by node oid, if not, create a new node pool.
 */
PGXCNodePool* acquire_nodepool(DatabasePool* dbPool, Oid nodeid, PoolNodeType node_type)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: acquire_nodepool()")));

    Assert(dbPool);

    bool found = false;
    PGXCNodePool* nodePool = NULL;
    MemoryContext oldcontext;
    errno_t ss_rc = EOK;
    Oid node_oid = nodeid;

    nodePool = (PGXCNodePool*)hash_search(dbPool->nodePools, &node_oid, HASH_FIND, &found);

    if (found) {
        return nodePool;
    }

    LWLockRelease(PoolerLock);
    oldcontext = MemoryContextSwitchTo(dbPool->mcxt);

    LWLockAcquire(PoolerLock, LW_EXCLUSIVE);

    nodePool = (PGXCNodePool*)hash_search(dbPool->nodePools, &node_oid, HASH_FIND, &found);
    if (found == true) {
        MemoryContextSwitchTo(oldcontext);
        return nodePool;
    }

    nodePool = (PGXCNodePool*)hash_search(dbPool->nodePools, &node_oid, HASH_ENTER, &found);

    /* memset_s(nodePool, 0, ) means nodePool->valid = false */
    size_t len = sizeof(PGXCNodePool) - sizeof(Oid);
    char* dest = (char*)nodePool;
    ss_rc = memset_s(dest + sizeof(Oid), len, 0, len);
    securec_check(ss_rc, "\0", "\0");

    if (!found) {
        /* nodePool is stateless in pooler stateless reuse mode */
        if (ENABLE_STATELESS_REUSE) {
            nodePool->connstr = NULL;
            nodePool->connstr1 = NULL;
        } else {
            nodePool->connstr = build_node_conn_str(node_oid, dbPool, false);
            if (nodePool->connstr == NULL) {
                /* Node has been removed or altered */
                hash_search(dbPool->nodePools, &node_oid, HASH_REMOVE, NULL);
                ereport(LOG,
                    (errcode(ERRCODE_WARNING),
                        errmsg("pooler: could not build connection string for node oid: %u", node_oid),
                        errdetail("node(oid:%u) maybe dropped or changed.", node_oid)));
                goto failure;
            }
            nodePool->connstr1 = build_node_conn_str(node_oid, dbPool, true);
            if (nodePool->connstr1 == NULL) {
                pfree_ext(nodePool->connstr);
                /* Node has been removed or altered */
                hash_search(dbPool->nodePools, &node_oid, HASH_REMOVE, NULL);
                ereport(LOG,
                    (errcode(ERRCODE_WARNING),
                        errmsg("pooler: could not build connection string 1 for node oid: %u", node_oid),
                        errdetail("node(oid:%u) maybe dropped or changed.", node_oid)));
                goto failure;
            }
        }

        nodePool->slot =
            (PGXCNodePoolSlot**)palloc0(g_instance.attr.attr_network.MaxPoolSize * sizeof(PGXCNodePoolSlot*));
        nodePool->freeSize = 0;
        nodePool->size = 0;
        nodePool->valid = true;  // It must be the last step of initialization ot nodepool.
        pthread_mutex_init(&nodePool->lock, 0);
    }

    MemoryContextSwitchTo(oldcontext);
    return nodePool;

failure:
    MemoryContextSwitchTo(oldcontext);
    return NULL;
}

/*
 * @Description: verify node pool size
 * @IN node pool
 * @Return: void
 * @See also:
 */
void agent_verify_node_size(PGXCNodePool* nodePool)
{
    int count = 0;

    /* get all using connections of the node from poolAgents */
    for (int i = 0; i < agentCount; ++i) {
        int j = 0;

        for (j = 0; j < poolAgents[i]->num_dn_connections; ++j) {
            if (poolAgents[i]->dn_conn_oids[j] != nodePool->nodeoid)
                continue;

            /* increase using count */
            if (poolAgents[i]->dn_connections[j])
                ++count;
        }

        for (j = 0; j < poolAgents[i]->num_coord_connections; j++) {
            if (poolAgents[i]->coord_conn_oids[j] != nodePool->nodeoid)
                continue;

            if (poolAgents[i]->coord_connections[j])
                ++count;
        }
    }

    nodePool->size = count + nodePool->freeSize;
}

/*
 * Acquire connection
 * return: E_POOL_FULL, no free slot && nodepool.size == maxconn,
 *         E_TO_CREATE,    no free slot && nodepool.size < maxconn,
 *         (E_OK, slot),return free slot for test
 *         E_NO_NODE,    can NOT create nodepool for nodeid(oid)
 */
int get_connection(DatabasePool* dbpool, Oid nodeid, PGXCNodePoolSlot** slot, PoolNodeType node_type)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: get_connection()")));

    Assert(dbpool);

    int rs;
    PGXCNodePool* nodePool = NULL;

    LWLockAcquire(PoolerLock, LW_SHARED);

    /*
     * When a Coordinator pool is initialized by a Coordinator Postmaster,
     * it has a NULL size and is below minimum size that is 1
     * This is to avoid problems of connections between Coordinators
     * when creating or dropping Databases.
     */
    nodePool = acquire_nodepool(dbpool, nodeid, node_type);
    if (NULL == nodePool) {
        /* Node has been removed or altered */
        rs = E_NO_NODE;
        LWLockRelease(PoolerLock);
        return rs;
    }

    pthread_mutex_lock(&nodePool->lock);
    /*
     * It's NOT necessary to check whether nodePool is partly initialized here,
     * because nodePool from acquire_nodepool() is NULL or initialized completely.
     *
     * During inplace or online upgrade, for compatibility consideration, we must
     * ensure that the local backend and the remote backend have the same version,
     * i.e. t_thrd.proc->workingVersionNum. To achieve this, we stop picking up connections
     * from pooler and always create new connections. The working version of local
     * backend is passed within the connection string. Later at transaction commit
     * or abort, we just destroy the connections instead of putting them back to pooler.
     *
     * Under thread pool mode, for inner maintenance tools or other maintenance queries
     * connected through ha port, such as gs_clean, we do not want to go through pooler rountine,
     * to avoid thread pool queueing.
     * Instead, we create new connections with cn/dn maintenance port.
     */
    if (!isInLargeUpgrade() && !(g_instance.attr.attr_common.enable_thread_pool &&
        (u_sess->proc_cxt.IsInnerMaintenanceTools || IsHAPort(u_sess->proc_cxt.MyProcPort))) &&
        nodePool->freeSize > 0) {
        *slot = nodePool->slot[nodePool->freeSize - 1];
        nodePool->slot[nodePool->freeSize - 1] = NULL;
        (nodePool->freeSize)--;

        rs = E_OK;
        pthread_mutex_unlock(&nodePool->lock);
        LWLockRelease(PoolerLock);
        return rs;
    }

    /* node pool size is out of max pool size, we need verify current node size */
    if (nodePool->size >= g_instance.attr.attr_network.MaxPoolSize)
        agent_verify_node_size(nodePool);

    if (nodePool->size >= g_instance.attr.attr_network.MaxPoolSize) {
        /* no available slot and number of slot in use reaches upper limit */
        rs = E_NO_FREE_SLOT;
        ereport(LOG,
            (errmsg("node %d has no free slot, used/max: %d/%d",
                nodePool->nodeoid,
                nodePool->size,
                g_instance.attr.attr_network.MaxPoolSize)));
    } else {
        if (ENABLE_STATELESS_REUSE) {
            /* increase counter of node pool first, and create slot later */
            ereport(DEBUG2,
                (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                    errmsg("pooler: No free connection in node pool, node oid: %u, the number of connection in use: %d",
                        nodePool->nodeoid,
                        nodePool->size)));
        } else {
            /* increase counter of node pool first, and create slot later */
            ereport(DEBUG2,
                (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                    errmsg("pooler: No free connection in node pool: %s, node oid: %u, the number of connection in "
                           "use: %d",
                        nodePool->connstr,
                        nodePool->nodeoid,
                        nodePool->size)));
        }

        nodePool->size++;
        rs = E_TO_CREATE;
    }

    pthread_mutex_unlock(&nodePool->lock);
    LWLockRelease(PoolerLock);
    return rs;
}

void set_pooler_ping(bool mod)
{
    PoolerPing = mod;
}

static int pingNode(const char* host, const char* port, const char* dbname)
{
#define MAXPATH (512 - 1)
#define MAXLINE (8192 - 1)

    PGPing status;
    char conninfo[MAXLINE + 1];
    char editBuf[MAXPATH + 1];
    errno_t rc = EOK;
    int nRet = 0;

    if (!PoolerPing)
        return 0;

    conninfo[0] = 0;
    if (host != NULL) {
        nRet = snprintf_s(editBuf, MAXPATH + 1, MAXPATH, "host = '%s' ", host);
        securec_check_ss(nRet, "\0", "\0");
        rc = strncat_s(conninfo, MAXLINE + 1, editBuf, strlen(editBuf));
        securec_check(rc, "\0", "\0");
    }
    if (port != NULL) {
        nRet = snprintf_s(editBuf, MAXPATH + 1, MAXPATH, "port = %d ", atoi(port));
        securec_check_ss(nRet, "\0", "\0");
        rc = strncat_s(conninfo, MAXLINE + 1, editBuf, strlen(editBuf));
        securec_check(rc, "\0", "\0");
    }

    nRet = snprintf_s(editBuf,
        MAXPATH + 1,
        MAXPATH,
        "dbname = %s connect_timeout = %d ",
        dbname ? dbname : "postgres",
        u_sess->attr.attr_network.PoolerCancelTimeout);
    securec_check_ss(nRet, "\0", "\0");
    rc = strncat_s(conninfo, MAXLINE + 1, editBuf, strlen(editBuf));
    securec_check(rc, "\0", "\0");

    char* remoteType = (char*)(IS_PGXC_COORDINATOR ? "coordinator" : "datanode");
    nRet = snprintf_s(editBuf, MAXPATH + 1, MAXPATH, "options='-c remotetype=%s' ", remoteType);
    securec_check_ss(nRet, "\0", "\0");
    rc = strncat_s(conninfo, MAXLINE + 1, editBuf, strlen(editBuf));
    securec_check(rc, "\0", "\0");

    if (conninfo[0]) {
        status = PQping(conninfo);
        if (status == PQPING_OK)
            return 0;
        else
            return 1;
    } else
        return -1;
}

/*
 * test if slot is available
 */
bool test_conn(PGXCNodePoolSlot* slot, Oid nodeid)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: test_conn()")));

    if (slot == NULL || slot->conn == NULL)
        return false;

    /* Check host to quick determin if it is an Old conn to Old Primary */
    if (!node_check_host(PQhost((PGconn*)slot->conn), nodeid)) {
        ereport(LOG,
            (errcode(ERRCODE_WARNING),
                errmsg("pooler: node %u connection to host %s is not primary", nodeid, PQhost((PGconn*)slot->conn))));

        return false;
    }

retry:
    /*
     * Make sure connection is ok, destroy connection slot if there is a
     * problem.
     */
    int result = pqReadReady((PGconn*)slot->conn);
    if (result < 0) {
        if (errno == EAGAIN || errno == EINTR)
            goto retry;

        ereport(LOG,
            (errcode(ERRCODE_WARNING),
                errmsg("pooler: Error in checking connection to node %u, error: %s", nodeid, strerror(errno))));
        return false;
    } else if (result > 0) { /* result > 0, there is some data */
        ereport(LOG,
            (errcode(ERRCODE_WARNING), errmsg("pooler: Unexpected data on connection to node %u, cleaning.", nodeid)));
        return false;
    }

    /* ping not ok */
    if (pingNode(PQhost((PGconn*)slot->conn), PQport((PGconn*)slot->conn), PQdb((PGconn*)slot->conn)) != 0)
        return false;

    return true;
}

/*
 * release connection from specified pool and slot
 */
void release_connection(PoolAgent* agent, PGXCNodePoolSlot** ptrSlot, Oid node, bool force_destroy)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else

    PGXCNodePool* nodePool = NULL;
    DatabasePool* dbPool = agent->pool;
    PGXCNodePoolSlot* slot = *ptrSlot;

    Assert(dbPool);
    Assert(slot);

    AutoMutexLock agentLock(&agent->lock);
    agentLock.lock();
    *ptrSlot = NULL;
    agentLock.unLock();

    nodePool = (PGXCNodePool*)hash_search(dbPool->nodePools, &node, HASH_FIND, NULL);
    if (nodePool == NULL) {
        /*
         * The node may be altered or dropped.
         * In any case the slot is no longer valid.
         */
        destroy_slot(slot);
        return;
    }

    pthread_mutex_lock(&nodePool->lock);
    /* return or discard */
    if (!force_destroy) {
        /* Insert the slot into the array and increase pool size */
        nodePool->slot[(nodePool->freeSize)++] = slot;
    } else {
        elog(DEBUG1, "Cleaning up connection from pool %s, closing", nodePool->connstr);
        destroy_slot(slot);
        /* Decrement pool size */
        (nodePool->size)--;
        /* Ensure we are not below minimum size */
        grow_pool(dbPool, node);
    }

    pthread_mutex_lock(&nodePool->lock);

    destroy_slot(slot);
#endif
}

/*
 * Destroy pool slot, but free slot memory
 */
void destroy_slot(PGXCNodePoolSlot* slot)
{
    char rw_timeout[MAXINT32DIGIT + 1] = {'\0'};
    int ret = 0;

    if (slot == NULL) {
        return;
    }

    if (u_sess->attr.attr_network.PoolerCancelTimeout && (slot->conn != NULL)) {
        ret = snprintf_s(
            rw_timeout, MAXINT32DIGIT + 1, MAXINT32DIGIT, "%d", u_sess->attr.attr_network.PoolerCancelTimeout);
        securec_check_ss(ret, "\0", "\0");

        if (slot->conn->rw_timeout != NULL)
            free(slot->conn->rw_timeout);
        slot->conn->rw_timeout = strdup(rw_timeout);
    }

    pfree_ext(slot->user_name);
    pfree_ext(slot->pgoptions);
    pfree_ext(slot->session_params);
    slot->userid = InvalidOid;

    PQfreeCancel((PGcancel*)slot->xc_cancelConn);
    slot->xc_cancelConn = NULL;

    PGXCNodeClose(slot->conn);
    slot->conn = NULL;

    pfree_ext(slot);
}

/*
 * Destroy node pool
 */
List* destroy_node_pool(PGXCNodePool* node_pool, List* slots)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: destroy_node_pool()")));

    int i;

    Assert(node_pool);

    /* node pool may be part initialized */
    if (false == node_pool->valid)
        return slots;

    /*
     * At this point all agents using connections from this pool should be already closed
     * If this not the connections to the Datanodes assigned to them remain open, this will
     * consume Datanode resources.
     */
    if (ENABLE_STATELESS_REUSE) {
        ereport(DEBUG1,
            (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                errmsg("pooler: About to destroy node pool, available size is %d, %d connections are in use",
                    node_pool->freeSize,
                    node_pool->size - node_pool->freeSize)));
    } else {
        ereport(DEBUG1,
            (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                errmsg("pooler: About to destroy node pool %s, available size is %d, %d connections are in use",
                    node_pool->connstr,
                    node_pool->freeSize,
                    node_pool->size - node_pool->freeSize)));
    }

    pfree_ext(node_pool->connstr);

    if (node_pool->slot != NULL) {
        for (i = 0; i < node_pool->freeSize; i++)
            slots = lappend(slots, node_pool->slot[i]);
        pfree_ext(node_pool->slot);
    }
    node_pool->size -= node_pool->freeSize;
    node_pool->freeSize = 0;

    return slots;
}

/*
 * Clean Connection in all Database Pools for given Datanode and Coordinator list
 */
int clean_connection(List* nodelist, const char* database, const char* user_name)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: clean_connection()")));

    List* slots = NIL;

    LWLockAcquire(PoolerLock, LW_EXCLUSIVE);

    DatabasePool* databasePool = NULL;
    int res = CLEAN_CONNECTION_COMPLETED;

    databasePool = databasePools;

    while (databasePool != NULL) {
        ListCell* lc = NULL;

        /* Clean connection by databasePool in pooler stateless reuse mode */
        if (ENABLE_STATELESS_REUSE) {
            if (((database != NULL) && strcmp(database, databasePool->database))) {
                /* The pool does not match to request, skip */
                databasePool = databasePool->next;
                continue;
            }
        } else {
            if (((database != NULL) && strcmp(database, databasePool->database)) ||
                ((user_name != NULL) && strcmp(user_name, databasePool->user_name))) {
                /* The pool does not match to request, skip */
                databasePool = databasePool->next;
                continue;
            }
        }

        /*
         * Clean each requested node pool
         */
        foreach (lc, nodelist) {
            PGXCNodePool* nodePool = NULL;
            Oid node = lfirst_oid(lc);

            nodePool = (PGXCNodePool*)hash_search(databasePool->nodePools, &node, HASH_FIND, NULL);
            if (NULL == nodePool)
                continue;
            if (nodePool->valid) {
                /* Check if connections are in use */
                if (nodePool->freeSize < nodePool->size) {
                    ereport(LOG,
                        (errcode(ERRCODE_WARNING),
                            errmsg("Pool of Database %s is using Datanode %u connections, freeSize: %d, size: %d",
                                databasePool->database,
                                node,
                                nodePool->freeSize,
                                nodePool->size)));
                    res = CLEAN_CONNECTION_NOT_COMPLETED;
                }

                /* Destroy connections currently in Node Pool */
                if (nodePool->slot != NULL) {
                    int i;
                    for (i = 0; i < nodePool->freeSize; i++)
                        slots = lappend(slots, nodePool->slot[i]);
                }
                nodePool->size -= nodePool->freeSize;
                nodePool->freeSize = 0;
            }
        }

        databasePool = databasePool->next;
    }

    /*
     * here is ugly code, PoolManagerCleanConnections() & CleanConnection() have
     * retry, if conns are NOT clean clearly first time, others agent can access
     * pooler.
     */
    LWLockRelease(PoolerLock);
    destroy_slots(slots);
    return res;
}

/*
 * Take a Lock on Pooler.
 * Abort PIDs registered with the agents for the given database.
 * Send back to client list of PIDs signaled to watch them.
 */
/* pooler threading change pid to ThreadId */
ThreadId* abort_pids(int* len, ThreadId pid, const char* database, const char* user_name, bool** isthreadpool)
{
    ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: abort_pids()")));

    ThreadId* pids = NULL;
    int i = 0;
    int count;

    Assert(agentCount > 0);

    /*
     * use MaxAgentCount instead of agentCount to avoid PoolerLock.
     */
    pids = (ThreadId*)palloc(MaxAgentCount * sizeof(ThreadId));
    *isthreadpool = (bool*)palloc(MaxAgentCount * sizeof(bool));
    ereport(DEBUG1, (errcode(ERRCODE_SUCCESSFUL_COMPLETION), errmsg("pooler: the number of session: %d", agentCount)));

    LWLockAcquire(PoolerLock, LW_EXCLUSIVE);

    /* find all Pooler agents except this one */
    for (count = 0; count < agentCount; count++) {
        AutoMutexLock agentLock(&poolAgents[count]->lock);
        agentLock.lock();
        if (poolAgents[count]->pid == pid) {
            agentLock.unLock();
            continue;
        }

        if ((database != NULL) && strcmp(poolAgents[count]->pool->database, database) != 0) {
            agentLock.unLock();
            continue;
        }

        if (ENABLE_STATELESS_REUSE) {
            if ((user_name != NULL) && strcmp(poolAgents[count]->user_name, user_name) != 0) {
                agentLock.unLock();
                continue;
            }
        } else {
            if ((user_name != NULL) && strcmp(poolAgents[count]->pool->user_name, user_name) != 0) {
                agentLock.unLock();
                continue;
            }
        }

        (*isthreadpool)[i] = poolAgents[count]->is_thread_pool_session;
        pids[i++] = poolAgents[count]->pid;
        agentLock.unLock();
    }

    LWLockRelease(PoolerLock);

    ereport(DEBUG1,
        (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
            errmsg("pooler: the number of the sessions with the same database and user name: %d", i)));
    *len = i;

    return pids;
}

bool IsPoolHandle(void)
{
    if (u_sess->pgxc_cxt.poolHandle == NULL)
        return false;
    return true;
}

/*
 * Given node identifier, dbname and user name build connection string.
 * Get node connection details from the shared memory node table
 */
char* build_node_conn_str(Oid nodeid, DatabasePool *dbPool, bool isNeedSlave)
{
    NodeDefinition *nodeDef = NULL;
    char *user = NULL;
    char *pgoptions = NULL;
    char *connstr = NULL;
    int port;
    PoolAgent *agent = NULL;
    NameData *host = NULL;
    NameData nodename = {{0}};

    nodeDef = PgxcNodeGetDefinition(nodeid);
    if (nodeDef == NULL) {
        /* No such definition, node is dropped? */
        return NULL;
    }
    
    if (IS_DN_DUMMY_STANDYS_MODE() && isNeedSlave == true) {
        host = &(nodeDef->nodehost1);
        port = nodeDef->nodeport1;
    } else {
        host = &(nodeDef->nodehost);
        port = nodeDef->nodeport;
    }    

    if (ENABLE_STATELESS_REUSE) {
        agent = get_agent(u_sess->pgxc_cxt.poolHandle);
        user = agent->user_name;
        pgoptions = agent->pgoptions;
    } else {
        user = dbPool->user_name;
        pgoptions = dbPool->pgoptions;
    }

    connstr = PGXCNodeConnStr(NameStr(*host),
        port,
        dbPool->database,
        (const char *)user,
        (const char *)pgoptions,
        (char *)(IS_PGXC_COORDINATOR ? "coordinator" : "datanode"),
        PROTO_TCP,
        get_pgxc_nodename(nodeid, &nodename));

    ereport(DEBUG1, (errcode(ERRCODE_WARNING),
        errmsg("pooler: build_node_conn_str host = %s, port = %d, database = %s, user = %s",
            host->data, port, dbPool->database, user),
            errdetail("node.....")));
                    
    pfree_ext(nodeDef);
    return connstr;
}

void destroy_slots(List* slots)
{
    if (NULL == slots)
        return;

    /* destroy slot and free memory */
    ListCell* item = NULL;
    foreach (item, slots) {
        PGXCNodePoolSlot* slot = (PGXCNodePoolSlot*)lfirst(item);
        destroy_slot(slot);
    }
    list_free(slots);
}

void free_agent(PoolAgent* agent)
{
    if (NULL == agent)
        return;

    pfree_ext(agent->user_name);
    pfree_ext(agent->pgoptions);
    pfree_ext(agent->coord_connections);
    pfree_ext(agent->coord_conn_oids);
    pfree_ext(agent->coord_connecting_oids);
    pfree_ext(agent->dn_connections);
    pfree_ext(agent->dn_conn_oids);
    pfree_ext(agent->dn_connecting_oids);
    pfree_ext(agent->local_params);
    pfree_ext(agent->session_params);
    pfree_ext(agent->temp_namespace);

    /* free agent params_list */
    if (agent->params_list != NIL) {
        ListCell* item = NULL;
        foreach (item, agent->params_list) {
            char* params_query = (char*)lfirst(item);
            pfree_ext(params_query);
        }
        list_free(agent->params_list);
    }
    agent->params_list = NIL;

    (void)pthread_mutex_destroy(&agent->lock);

    pfree_ext(agent);
}

static uint32 get_current_connection_nums(Oid* cnOids, Oid* dnOids, List* cn_list, List* dn_list)
{
    int i, nodeIndex;
    uint32 connCount = 0;
    DatabasePool* dbPool = databasePools;
    ListCell* nodelist_item = NULL;
    PGXCNodePool* nodePool = NULL;

    /* get connections info from poolAgents */
    for (i = 0; i < agentCount; i++) {
        connCount += poolAgents[i]->num_dn_connections;
        connCount += poolAgents[i]->num_coord_connections;
    }

    /* get free connections info from nodepool */
    while (dbPool != NULL) {
        foreach (nodelist_item, dn_list) {
            nodeIndex = lfirst_int(nodelist_item);
            nodePool = (PGXCNodePool*)hash_search(dbPool->nodePools, &dnOids[nodeIndex], HASH_FIND, NULL);
            if (nodePool != NULL) {
                connCount += nodePool->freeSize;
            }
        }

        foreach (nodelist_item, cn_list) {
            nodeIndex = lfirst_int(nodelist_item);
            nodePool = (PGXCNodePool*)hash_search(dbPool->nodePools, &cnOids[nodeIndex], HASH_FIND, NULL);
            if (nodePool != NULL) {
                connCount += nodePool->freeSize;
            }
        }

        /* Current dbPool contains all nodes connection information for a database. */
        dbPool = dbPool->next;
    }

    return connCount;
}

int get_pool_connection_info(PoolConnectionInfo** connectionEntry)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

int check_connection_status(ConnectionStatus **connectionEntry)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

PoolAgent* get_poolagent(void)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * Get all connections memory size and connection count, include all databases, all PoolerAgent.
 */
void pooler_get_connection_statinfo(PoolConnStat* conn_stat)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}
