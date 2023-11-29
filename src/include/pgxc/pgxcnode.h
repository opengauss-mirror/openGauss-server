/* -------------------------------------------------------------------------
 *
 * pgxcnode.h
 *
 *	  Utility functions to communicate to Datanodes and Coordinators
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group ?
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/pgxcnode.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PGXCNODE_H
#define PGXCNODE_H
#include "postgres.h"
#include "knl/knl_variable.h"
#include "gtm/gtm_c.h"
#include "utils/timestamp.h"
#include "nodes/pg_list.h"
#include "utils/globalplancache.h"
#include "utils/snapshot.h"
#include "libcomm/libcomm.h"
#include "pgxc/poolmgr.h"
#include "getaddrinfo.h"
#include <unistd.h>
#include "knl/knl_variable.h"
#include <sys/time.h>
#include <sys/ioctl.h>
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "nodes/nodes.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pgxc_node.h"
#include "catalog/pg_collation.h"
#include "tcop/dest.h"
#include "utils/distribute_test.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/formatting.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "postmaster/twophasecleaner.h"
#include "libcomm/libcomm.h"

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#define NO_SOCKET -1
#define CMD_ID_MSG_LEN 8
#define PGXC_CANCEL_DELAY 15

#define ERROR_OCCURED true
#define NO_ERROR_OCCURED false
#define CN_SESSION_ID_MASK 0xffffffffffff

/* Message type code of bucket map and dn node index */
#define MSG_TYPE_PGXC_BUCKET_MAP 'G'

/* for logic connection sock is useless, type of gstock gives whether current logic connection is ok */
#define IS_VALID_CONNECTION(handle) \
    ((handle->is_logic_conn) ? (handle->gsock.type != GSOCK_INVALID) : (handle->sock != NO_SOCKET))

#define DDL_PBE_VERSION_NUM 92067    /* Version control for DDL PBE */

struct StreamNetCtl;

/* Helper structure to access Datanode from Session */
typedef enum {
    DN_CONNECTION_STATE_IDLE,        /* idle, ready for query */
    DN_CONNECTION_STATE_QUERY,       /* query is sent, response expected */
    DN_CONNECTION_STATE_ERROR_FATAL, /* fatal error */
    DN_CONNECTION_STATE_COPY_IN,
    DN_CONNECTION_STATE_COPY_OUT
} DNConnectionState;

typedef enum { HANDLE_IDLE, HANDLE_ERROR, HANDLE_DEFAULT } PGXCNode_HandleRequested;

/*
 * Enumeration for two purposes
 * 1. To indicate to the HandleCommandComplete function whether response checking is required or not
 * 2. To enable HandleCommandComplete function to indicate whether the response was a ROLLBACK or not
 * Response checking is required in case of PREPARE TRANSACTION and should not be done for the rest
 * of the cases for performance reasons, hence we have an option to ignore response checking.
 * The problem with PREPARE TRANSACTION is that it can result in a ROLLBACK response
 * yet Coordinator would think it got done on all nodes.
 * If we ignore ROLLBACK response then we would try to COMMIT a transaction that
 * never got prepared, which in an incorrect behavior.
 */
typedef enum {
    RESP_ROLLBACK_IGNORE,      /* Ignore response checking */
    RESP_ROLLBACK_CHECK,       /* Check whether response was ROLLBACK */
    RESP_ROLLBACK_RECEIVED,    /* Response is ROLLBACK */
    RESP_ROLLBACK_NOT_RECEIVED /* Response is NOT ROLLBACK */
} RESP_ROLLBACK;

/* To identify the different uses of CSN */
typedef enum {
    FETCH_CSN,        /* A flag to fecth csn from other nodes */
    GLOBAL_CSN_MIN    /* The global min csn between all nodes */
} CsnType;

#define DN_CONNECTION_STATE_ERROR(dnconn) \
    ((dnconn)->state == DN_CONNECTION_STATE_ERROR_FATAL || (dnconn)->transaction_status == 'E')

#define HAS_MESSAGE_BUFFERED(conn)           \
    ((conn)->inCursor + 4 < (conn)->inEnd && \
        (conn)->inCursor + ntohl(*((uint32_t*)((conn)->inBuffer + (conn)->inCursor + 1))) < (conn)->inEnd)

/*
 * Represents a DataRow message received from a remote node.
 * Contains originating node number and message body in DataRow format without
 * message code and length. Length is separate field
 */
typedef struct RemoteDataRowData {
    char* msg;   /* last data row message */
    int msglen;  /* length of the data row message */
    int msgnode; /* node number of the data row message */
} RemoteDataRowData;
typedef RemoteDataRowData* RemoteDataRow;

struct pgxc_node_handle {
    Oid nodeoid;

    /* fd of the connection */
    int sock;

    /* tcp connection */
    int tcpCtlPort;
    int listenPort;
    /* logic connection socket between cn and dn */
    gsocket gsock;

    char transaction_status;
    DNConnectionState state;
    struct RemoteQueryState* combiner;
    struct StreamState* stream;
#ifdef DN_CONNECTION_DEBUG
    bool have_row_desc;
#endif
    char* error;
    char* hint;
    /* Output buffer */
    char* outBuffer;
    size_t outSize;
    size_t outEnd;
    uint64 outNum;
    /* Input buffer */
    char* inBuffer;
    size_t inSize;
    size_t inStart;
    size_t inEnd;
    size_t inCursor;

    /*
     * Have a variable to enable/disable response checking and
     * if enable then read the result of response checking
     *
     * For details see comments of RESP_ROLLBACK
     */
    RESP_ROLLBACK ck_resp_rollback;

    char* remoteNodeName;
    int nodeIdx; /* datanode index, like t_thrd.postmaster_cxt.PGXCNodeId */
    knl_virtual_role remote_node_type;
    PoolConnInfo connInfo;
    /* flag to identify logic connection between cn and dn */
    bool is_logic_conn;
    NODE_CONNECTION* pg_conn;
    MessageCommLog *msgLog;
    TransactionId  remote_top_txid;
};
typedef struct pgxc_node_handle PGXCNodeHandle;

/*
 * Because the number of node handles is identical to the number of nodes,
 * so here we combine both CN and DN handles together for better maitanences
 */
typedef struct PGXCNodeHandleGroup {
    PGXCNodeHandle* nodeHandles;
    uint32 nodeHandleNums;

    /* dn and cn slots often process together, so need offset to indicate real postion */
    uint32 offset;
} PGXCNodeHandleGroup;

typedef struct PGXCNodeNetCtlLayer {
    int conn_num;        /* size of array */
    struct pollfd* ufds; /* physic poll fds. */
    int* datamarks;      /* producer number triggers poll. */
    int* poll2conn;      /* poll idx to connection idx. */
    gsocket* gs_sock;    /* logic poll gs_sock. */
} PGXCNodeNetCtlLayer;

/* Structure used to get all the handles involved in a transaction */
typedef struct PGXCNodeAllHandles {
    PGXCNodeHandle* primary_handle;    /* Primary connection to PGXC node */
    int dn_conn_count;                 /* number of Datanode Handles including primary handle */
    PGXCNodeHandle** datanode_handles; /* an array of Datanode handles */
    List* dummy_datanode_handles;      /* only valid for replicate table dml */
    int co_conn_count;                 /* number of Coordinator handles */
    PGXCNodeHandle** coord_handles;    /* an array of Coordinator handles */
} PGXCNodeAllHandles;

extern unsigned long LIBCOMM_BUFFER_SIZE;
extern void InitMultinodeExecutor(bool is_force);

/* Open/close connection routines (invoked from Pool Manager) */
extern char *PGXCNodeConnStr(const char *host, int port, const char *dbname, const char *user,
    const char *pgoptions, const char *remote_type, int proto_type, const char *remote_nodename);
extern NODE_CONNECTION* PGXCNodeConnect(char* connstr);
extern int PGXCNodeSendSetQuery(NODE_CONNECTION* conn, const char* sql_command);
extern int PGXCNodeSetQueryGetResult(NODE_CONNECTION* conn);
extern int PGXCNodeSendSetQueryPoolerStatelessReuse(NODE_CONNECTION* conn, const char* sql_command);
extern void PGXCNodeClose(NODE_CONNECTION* conn);
extern int PGXCNodeConnected(NODE_CONNECTION* conn);
extern int PGXCNodeConnClean(NODE_CONNECTION* conn);
extern void PGXCNodeCleanAndRelease(int code, Datum arg);
extern void PGXCConnClean(int code, Datum arg);

/* Look at information cached in node handles */
extern int PGXCNodeGetNodeId(Oid nodeoid, char node_type);
extern int PGXCNodeDRGetNodeId(Oid nodeoid);
extern Oid PGXCNodeGetNodeOid(int nodeid, char node_type);
extern int PGXCNodeGetNodeIdFromName(const char* node_name, char node_type);
extern char* PGXCNodeGetNodeNameFromId(int nodeid, char node_type);
extern int LibcommPacketSend(NODE_CONNECTION* conn, char pack_type, const void* buf, size_t buf_len);
extern bool LibcommCancelOrStop(NODE_CONNECTION* conn, char* errbuf, int errbufsize, int timeout, uint32 request_code);
extern bool LibcommStopQuery(NODE_CONNECTION* conn);
extern int LibcommSendSome(NODE_CONNECTION* conn, int len);
extern int LibcommPutMsgEnd(NODE_CONNECTION* conn);
extern int LibcommFlush(NODE_CONNECTION* conn);
extern int LibcommSendQuery(NODE_CONNECTION* conn, const char* query);
extern int LibcommReadData(NODE_CONNECTION* conn);
extern PGXCNodeAllHandles* get_handles(
    List* datanodelist, List* coordlist, bool is_query_coord_only, List* dummydatanodelist = NIL);
extern void pfree_pgxc_all_handles(PGXCNodeAllHandles* handles);
extern void release_pgxc_handles(PGXCNodeAllHandles* pgxc_handles);
extern void release_handles(void);
extern void destroy_handles(void);
extern void reset_handles_at_abort(void);
extern void cancel_query(void);
extern void cancel_query_without_read(void);
extern void stop_query(void);
extern void clear_all_data(void);
extern int get_transaction_nodes(
    PGXCNodeHandle** connections, char client_conn_type, PGXCNode_HandleRequested type_requested);
extern char* collect_pgxcnode_names(
    char* nodestring, int conn_count, PGXCNodeHandle** connections, char client_conn_type);
extern char* collect_localnode_name(char* nodestring);
extern int get_active_nodes(PGXCNodeHandle** connections);
extern void ensure_in_buffer_capacity(size_t bytes_needed, PGXCNodeHandle* handle);
extern void ensure_out_buffer_capacity(size_t bytes_needed, PGXCNodeHandle* handle);
extern int pgxc_node_send_threadid(PGXCNodeHandle* handle, uint32 threadid);
extern int pgxc_node_send_queryid(PGXCNodeHandle* handle, uint64 queryid);
extern int pgxc_node_send_unique_sql_id(PGXCNodeHandle* handle);
extern int pgxc_node_send_global_session_id(PGXCNodeHandle *handle);
extern int pgxc_node_send_query(PGXCNodeHandle* handle, const char* query, bool isPush = false,
    bool isCreateSchemaPush = false, bool trigger_ship = false, bool check_gtm_mode = false,
    const char *compressPlan = NULL, int cLen = 0);
extern void pgxc_node_send_gtm_mode(PGXCNodeHandle* handle);
extern int pgxc_node_send_plan_with_params(PGXCNodeHandle* handle, const char* query, short num_params,
    Oid* param_types, int paramlen, const char* params, int fetch_size);
extern int pgxc_node_send_describe(PGXCNodeHandle* handle, bool is_statement, const char* name);
#ifndef ENABLE_MULTIPLE_NODES
extern int pgxc_node_send_execute(PGXCNodeHandle* handle, const char* portal, int fetch);
#endif
extern int pgxc_node_send_close(PGXCNodeHandle* handle, bool is_statement, const char* name);
extern int pgxc_node_send_sync(PGXCNodeHandle* handle);
extern int pgxc_node_send_bind(
    PGXCNodeHandle* handle, const char* portal, const char* statement, int paramlen, const char* params);
extern int pgxc_node_send_parse(
    PGXCNodeHandle* handle, const char* statement, const char* query, short num_params, Oid* param_types);
#ifndef ENABLE_MULTIPLE_NODES
extern int pgxc_node_send_query_extended(PGXCNodeHandle* handle, const char* query, const char* statement,
    const char* portal, int num_params, Oid* param_types, int paramlen, const char* params, bool send_describe,
    int fetch_size);
#endif
extern int pgxc_node_send_gxid(PGXCNodeHandle* handle, GlobalTransactionId gxid, bool isforcheck);
extern int pgxc_node_send_commit_csn(PGXCNodeHandle* handle, uint64 commit_csn);
extern int pgxc_node_send_csn(PGXCNodeHandle* handle, uint64 commit_csn, CsnType csn_type);
extern int pgxc_node_notify_commit(PGXCNodeHandle* handle);
extern int pgxc_node_send_cmd_id(PGXCNodeHandle* handle, CommandId cid);
extern int pgxc_node_send_wlm_cgroup(PGXCNodeHandle* handle);
extern int pgxc_node_dywlm_send_params_for_jobs(PGXCNodeHandle* handle, int tag, const char* keystr);
extern int pgxc_node_dywlm_send_params(PGXCNodeHandle* handle, const void* info);
extern int pgxc_node_dywlm_send_record(PGXCNodeHandle* handle, int tag, const char* infostr);
extern int pgxc_node_send_pgfdw(PGXCNodeHandle* handle, int tag, const char* keystr, int len);
extern int pgxc_node_send_snapshot(PGXCNodeHandle* handle, Snapshot snapshot, int max_push_sqls = 1);
extern int pgxc_node_send_ddl_params(PGXCNodeHandle* handle);
extern int pgxc_node_send_timestamp(
    PGXCNodeHandle* handle, TimestampTz gtmstart_timestamp, TimestampTz stmtsys_timestamp);
extern int pgxc_node_send_pushschema(PGXCNodeHandle* handle, bool* isPush, bool *isCreateSchemaPush);
extern int pgxc_node_send_popschema(PGXCNodeHandle* handle, bool isPush);
extern int pgxc_node_is_data_enqueued(PGXCNodeHandle* conn);
extern int send_some(PGXCNodeHandle* handle, int len);
extern int pgxc_node_flush(PGXCNodeHandle* handle);
extern bool pgxc_node_flush_read(PGXCNodeHandle* handle);
extern int pgxc_all_handles_send_gxid(PGXCNodeAllHandles* pgxc_handles, GlobalTransactionId gxid, bool stop_at_error);
extern int pgxc_all_handles_send_query(PGXCNodeAllHandles* pgxc_handles, const char* buffer, bool stop_at_error);
extern void add_error_message(PGXCNodeHandle* handle, const char *message, ...);
extern char get_message(PGXCNodeHandle *conn, int *len, char **msg);
extern Datum pgxc_execute_on_nodes(int numnodes, Oid* nodelist, char* query);
extern bool pgxc_node_receive(const int conn_count, PGXCNodeHandle** connections,
                              struct timeval* timeout, bool ignoreTimeoutWarning = false);
extern bool datanode_receive_from_physic_conn(
const int conn_count, PGXCNodeHandle** connections, struct timeval* timeout);
extern bool datanode_receive_from_logic_conn(
const int conn_count, PGXCNodeHandle** connections, StreamNetCtl* ctl, int time_out);
extern bool pgxc_node_validate(PGXCNodeHandle *conn);
extern int pgxc_node_read_data(PGXCNodeHandle* conn, bool close_if_error, bool StreamConnection = false);
extern int pgxc_node_read_data_from_logic_conn(PGXCNodeHandle* conn, bool close_if_error);
extern void ResetHandleOutBuffer(PGXCNodeHandle* handle);
extern void connect_server(
    const char* conn_str, PGXCNodeHandle** handle, const char* host, int port, const char* log_conn_str);
extern void release_conn_to_compute_pool();
extern void release_pgfdw_conn();
extern int pgxc_node_send_userpl(PGXCNodeHandle* handle, int userpl);
extern int pgxc_node_send_userpl(PGXCNodeHandle* handle, int64 userpl);
extern int pgxc_node_send_cpconf(PGXCNodeHandle* handle, char* data);
extern int  pgxc_send_bucket_map(PGXCNodeHandle* handle, uint2* bucketMap, int dnNodeIdx, int bucketCnt);
extern bool light_node_receive(PGXCNodeHandle* handle);
extern bool light_node_receive_from_logic_conn(PGXCNodeHandle* handle);
extern int pgxc_node_pgstat_send(PGXCNodeHandle* handle, char tag);
extern void pgxc_node_free(PGXCNodeHandle* handle);
extern void pgxc_node_init(PGXCNodeHandle* handle, int sock);
extern void pgxc_node_free_def(PoolConnDef* result);
extern void pgxc_palloc_net_ctl(int conn_num);
extern int ts_get_tag_colnum(Oid relid, bool need_index = false, List** idx_list = NULL);
extern void init_pgxc_handle(PGXCNodeHandle* pgxc_handle);

#ifdef ENABLE_MULTIPLE_NODES
extern GlobalNodeDefinition *global_node_definition;
extern bool IsAutoVacuumWorkerProcess();
extern char *format_type_with_nspname(Oid type_oid);
extern void pgxc_node_all_free(void);
extern int pgxc_node_send_execute(PGXCNodeHandle* handle, const char* portal, int fetch, bool trigger_ship = false);
extern int pgxc_node_send_query_extended(PGXCNodeHandle* handle, const char* query, const char* statement,
    const char* portal, int num_params, Oid* param_types, int paramlen, const char* params, bool send_describe,
    int fetch_size, bool trigger_ship = false);
extern void FreePgxcNodehandles(PGXCNodeHandleGroup* handleGroup);
extern void FreePgxcNodehandlesWithStatus(PGXCNodeHandleGroup* handleGroup, char *statusArray, bool *hasError);
extern void LibcommFinish(NODE_CONNECTION *conn);
extern int LibcommWaitpoll(NODE_CONNECTION *conn);
extern int get_int(PGXCNodeHandle * conn, size_t len, int *out);
extern int get_char(PGXCNodeHandle * conn, char *out);
extern PoolHandle * GetPoolAgent();
extern void destroy_slots(List *slots);
extern void init_pgxc_handle(PGXCNodeHandle *pgxc_handle);
extern char* get_pgxc_nodename(Oid nodeid, NameData* namedata);
extern List* get_dn_handles(List *datanodelist, List *dummynodelist, PGXCNodeAllHandles* result);
extern List* get_cn_handles(List *coordlist, PGXCNodeAllHandles* result);
extern void init_handles(List* dn_allocate, List* co_allocate, PoolConnDef* pfds);
extern int PollInterrupts(struct pollfd *ufds, int nfds, int timeout);
extern int find_name_by_params(Oid *param_types, char	**paramTypes, short num_params);
extern void add_params_name_to_send(PGXCNodeHandle *handle, char	**paramTypes, short num_params);
extern int LibcommSendQueryPoolerStatelessReuse(NODE_CONNECTION *conn, const char *query);
#endif

#ifdef ENABLE_UT
extern char* getNodenameByIndex(int index);
extern int light_node_read_data(PGXCNodeHandle* conn);
#endif /* USE_UT */

#endif /* PGXCNODE_H */
