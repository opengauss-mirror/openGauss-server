/* -------------------------------------------------------------------------
 *
 * execRemote.h
 *
 *	  Functions to execute commands on multiple Datanodes
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/execRemote.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef EXECREMOTE_H
#define EXECREMOTE_H
#include "locator.h"
#include "nodes/nodes.h"
#include "pgxcnode.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "optimizer/pgxcplan.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/snapshot.h"
#include "utils/rowstore.h"

#define LPROXY_CONTINUE 0
#define LPROXY_FINISH 1
#define LPROXY_ERROR 2

/* Outputs of handle_response() */
#define RESPONSE_EOF EOF
#define RESPONSE_COMPLETE 0
#define RESPONSE_SUSPENDED 1
#define RESPONSE_TUPDESC 2
#define RESPONSE_DATAROW 3
#define RESPONSE_COPY 4
#define RESPONSE_BARRIER_OK 5
#define RESPONSE_PLANID_OK 6
#define RESPONSE_ANALYZE_ROWCNT 7

#define REMOTE_CHECKMSG_LEN 8  /* it equals to the count of bytes added in AddCheckMessage when is_stream is false */
#define STREAM_CHECKMSG_LEN 20 /* it equals to the count of bytes added in AddCheckMessage when is_stream is true */

/* with-recursive added messages */
#define RESPONSE_RECURSIVE_SYNC_R 98
#define RESPONSE_RECURSIVE_SYNC_F 99

#ifdef WIN32
#define SOCK_ERRNO (WSAGetLastError())
#define SOCK_STRERROR winsock_strerror
#define SOCK_ERRNO_SET(e) WSASetLastError(e)
#else
#define SOCK_ERRNO errno
#define SOCK_STRERROR pqStrerror
#define SOCK_ERRNO_SET(e) (errno = (e))
#endif

#define GCFDW_VERSION GCFDW_VERSION_V1R8C10_1

#define ERROR_CHECK_TIMEOUT 3 /* time out for checking some normal communication error(s) */

typedef enum {
    REQUEST_TYPE_NOT_DEFINED, /* not determined yet */
    REQUEST_TYPE_COMMAND,     /* OK or row count response */
    REQUEST_TYPE_QUERY,       /* Row description response */
    REQUEST_TYPE_COPY_IN,     /* Copy In response */
    REQUEST_TYPE_COPY_OUT,    /* Copy Out response */
    REQUEST_TYPE_COMMITING    /* Commiting response */
} RequestType;

/*
 * Type of requests associated to a remote COPY OUT
 */
typedef enum {
    REMOTE_COPY_NONE,      /* Not defined yet */
    REMOTE_COPY_STDOUT,    /* Send back to client */
    REMOTE_COPY_FILE,      /* Write in file */
    REMOTE_COPY_TUPLESTORE /* Store data in tuplestore */
} RemoteCopyType;

/* Combines results of INSERT statements using multiple values */
typedef struct CombineTag {
    CmdType cmdType;                   /* DML command type */
    char data[COMPLETION_TAG_BUFSIZE]; /* execution result combination data */
} CombineTag;

typedef struct NodeIdxInfo {
    Oid nodeoid;
    ;
    int nodeidx;
} NodeIdxInfo;

typedef enum {
    PBE_NONE = 0,      /* no pbe or for initialization */
    PBE_ON_ONE_NODE,   /* pbe running on one datanode for now */
    PBE_ON_MULTI_NODES /* pbe running on more than one datanode */
} PBERunStatus;

typedef enum PgFdwMessageTag {
    PGFDW_GET_TABLE_INFO = 0,
    PGFDW_ANALYZE_TABLE,
    PGFDW_QUERY_PARAM,
    PGFDW_GET_VERSION,
    PGFDW_GET_ENCODE
} PgFdwMessageTag;

typedef enum PgFdwCheckResult {
    PGFDW_CHECK_OK = 0,
    PGFDW_CHECK_TABLE_FAIL,
    PGFDW_CHECK_RELKIND_FAIL,
    PGFDW_CHECK_COLUNM_FAIL
} PgFdwCheckResult;

/*
 * the cooperation analysis version control
 * if modify the message which is sended and received, u need do such step
 * 1.enmu GcFdwVersion, need add version,
 *		e.g. GCFDW_VERSION_V1R8C10_1 = 101		GCFDW_VERSION_V1R9C00 = 200
 * 2.modify GCFDW_VERSION, the value need be set the lastest version.
 * 3.new logic, need use if conditon with new version
 *		e.g. if (gc_fdw_run_version >= GCFDW_VERSION_V1R8C10_1)
 *			 { pq_sendint64(&retbuf, u_sess->debug_query_id); }
 */
typedef enum GcFdwVersion {
    GCFDW_VERSION_V1R8C10 = 100,   /* the first version */
    GCFDW_VERSION_V1R8C10_1 = 101, /* add foreign table option : encode type */

    GCFDW_VERSION_V1R9C00 = 200
} GcFdwVersion;

typedef struct PgFdwRemoteInfo {
    NodeTag type;
    char reltype;      /* relation type */
    int datanodenum;   /* datanode num  */
    Size snapsize;     /* the really size of snapshot */
    Snapshot snapshot; /* snapshot */
} PgFdwRemoteInfo;

typedef struct ParallelFunctionState {
    char* sql_statement;
    ExecNodes* exec_nodes;
    TupleDesc tupdesc;
    Tuplestorestate* tupstore;
    int64 result;
    void* resultset;
    MemoryContext slotmxct;
    bool read_only;
} ParallelFunctionState;

typedef bool (*fetchTupleFun)(
    RemoteQueryState* combiner, TupleTableSlot* slot, ParallelFunctionState* parallelfunctionstate);

typedef struct RemoteQueryState {
    ScanState ss;                 /* its first field is NodeTag */
    int node_count;               /* total count of participating nodes */
    PGXCNodeHandle** connections; /* Datanode connections being combined */
    int conn_count;               /* count of active connections */
    int current_conn;             /* used to balance load when reading from connections */
    CombineType combine_type;     /* see CombineType enum */
    int command_complete_count;   /* count of received CommandComplete messages */
    RequestType request_type;     /* see RequestType enum */
    TupleDesc tuple_desc;         /* tuple descriptor to be referenced by emitted tuples */
    int description_count;        /* count of received RowDescription messages */
    int copy_in_count;            /* count of received CopyIn messages */
    int copy_out_count;           /* count of received CopyOut messages */
    int errorCode;                /* error code to send back to client */
    char* errorMessage;           /* error message to send back to client */
    char* errorDetail;            /* error detail to send back to client */
    char* errorContext;           /* error context to send back to client */
    char* hint;                   /* hint message to send back to client */
    char* query;                  /* query message to send back to client */
    int cursorpos;                /* cursor index into query string */
    bool is_fatal_error;          /* mark if it is a FATAL error */
    bool query_Done;              /* query has been sent down to Datanodes */
    RemoteDataRowData currentRow; /* next data ro to be wrapped into a tuple */
    RowStoreManager row_store;    /* buffer where rows are stored when connection 
    should be cleaned for reuse by other RemoteQuery */
    
    /*
     * To handle special case - if this RemoteQuery is feeding sorted data to
     * Sort plan and if the connection fetching data from the Datanode
     * is buffered. If EOF is reached on a connection it should be removed from
     * the array, but we need to know node number of the connection to find
     * messages in the buffer. So we store nodenum to that array if reach EOF
     * when buffering
     */
    int* tapenodes;
    RemoteCopyType remoteCopyType; /* Type of remote COPY operation */
    FILE* copy_file;               /* used if remoteCopyType == REMOTE_COPY_FILE */
    uint64 processed;              /* count of data rows when running CopyOut */
    /* cursor support */
    char* cursor;                        /* cursor name */
    char* update_cursor;                 /* throw this cursor current tuple can be updated */
    int cursor_count;                    /* total count of participating nodes */
    PGXCNodeHandle** cursor_connections; /* Datanode connections being combined */
    /* Support for parameters */
    char* paramval_data;  /* parameter data, format is like in BIND */
    int paramval_len;     /* length of parameter values data */
    Oid* rqs_param_types; /* Types of the remote params */
    int rqs_num_params;

    int eflags;          /* capability flags to pass to tuplestore */
    bool eof_underlying; /* reached end of underlying plan? */
    Tuplestorestate* tuplestorestate;
    CommandId rqs_cmd_id;     /* Cmd id to use in some special cases */
    uint64 rqs_processed;     /* Number of rows processed (only for DMLs) */
    uint64 rqs_cur_processed; /* Number of rows processed for currently processed DN */

    void* tuplesortstate; /* for merge tuplesort */
    void* batchsortstate; /* for merge batchsort */
    fetchTupleFun fetchTuple;
    bool need_more_data;
    RemoteErrorData remoteErrData;           /* error data from remote */
    bool need_error_check;                   /* if need check other errors? */
    int64 analyze_totalrowcnt[ANALYZEDELTA]; /* save total row count received from dn under analyzing. */
    int64 analyze_memsize[ANALYZEDELTA];     /* save processed row count from dn under analyzing. */
    int valid_command_complete_count;        /* count of valid complete count */

    RemoteQueryType position; /* the position where the RemoteQuery node will run. */
    bool* switch_connection;  /* mark if connection reused by other RemoteQuery */
    bool refresh_handles;
    NodeIdxInfo* nodeidxinfo;
    PBERunStatus pbe_run_status; /* record running status of a pbe remote query */

    bool resource_error;    /* true if error from the compute pool */
    char* previousNodeName; /* previous DataNode that rowcount is different from current DataNode */
    char* serializedPlan;   /* the serialized plan tree */
} RemoteQueryState;

typedef void (*xact_callback)(bool isCommit, const void* args);
typedef void (*strategy_func)(ParallelFunctionState*);

typedef enum RemoteXactNodeStatus {
    RXACT_NODE_NONE,           /* Initial state */
    RXACT_NODE_PREPARE_SENT,   /* PREPARE request sent */
    RXACT_NODE_PREPARE_FAILED, /* PREPARE failed on the node */
    RXACT_NODE_PREPARED,       /* PREPARED successfully on the node */
    RXACT_NODE_COMMIT_SENT,    /* COMMIT sent successfully */
    RXACT_NODE_COMMIT_FAILED,  /* failed to COMMIT on the node */
    RXACT_NODE_COMMITTED,      /* COMMITTed successfully on the node */
    RXACT_NODE_ABORT_SENT,     /* ABORT sent successfully */
    RXACT_NODE_ABORT_FAILED,   /* failed to ABORT on the node */
    RXACT_NODE_ABORTED         /* ABORTed successfully on the node */
} RemoteXactNodeStatus;

typedef enum RemoteXactStatus {
    RXACT_NONE,           /* Initial state */
    RXACT_PREPARE_FAILED, /* PREPARE failed */
    RXACT_PREPARED,       /* PREPARED succeeded on all nodes */
    RXACT_COMMIT_FAILED,  /* COMMIT failed on all the nodes */
    RXACT_PART_COMMITTED, /* COMMIT failed on some and succeeded on other nodes */
    RXACT_COMMITTED,      /* COMMIT succeeded on all the nodes */
    RXACT_ABORT_FAILED,   /* ABORT failed on all the nodes */
    RXACT_PART_ABORTED,   /* ABORT failed on some and succeeded on other nodes */
    RXACT_ABORTED         /* ABORT succeeded on all the nodes */
} RemoteXactStatus;

typedef struct RemoteXactState {
    /* Current status of the remote 2PC */
    RemoteXactStatus status;

    /*
     * Information about all the nodes involved in the transaction. We track
     * the number of writers and readers. The first numWriteRemoteNodes entries
     * in the remoteNodeHandles and remoteNodeStatus correspond to the writer
     * connections and rest correspond to the reader connections.
     */
    int numWriteRemoteNodes;
    int numReadRemoteNodes;
    int maxRemoteNodes;
    PGXCNodeHandle** remoteNodeHandles;
    RemoteXactNodeStatus* remoteNodeStatus;

    GlobalTransactionId commitXid;

    bool preparedLocalNode;

    char prepareGID[256]; /* GID used for internal 2PC and set the length 256 */
} RemoteXactState;

#ifdef PGXC
typedef struct abort_callback_type {
    xact_callback function;
    void* fparams;
} abort_callback_type;
#endif

/* Multinode Executor */
extern void PGXCNodeBegin(void);
extern void PGXCNodeSetBeginQuery(char* query_string);
extern void PGXCNodeCommit(bool bReleaseHandles);
extern int PGXCNodeRollback(void);
extern bool PGXCNodePrepare(char* gid);
extern bool PGXCNodeRollbackPrepared(char* gid);
extern void PGXCNodeCommitPrepared(char* gid);

/* Copy command just involves Datanodes */
extern PGXCNodeHandle** DataNodeCopyBegin(const char* query, List* nodelist, Snapshot snapshot);
extern int DataNodeCopyIn(const char* data_row, int len, const char* eol, ExecNodes* exec_nodes,
    PGXCNodeHandle** copy_connections, bool is_binary = false);
extern uint64 DataNodeCopyOut(ExecNodes* exec_nodes, PGXCNodeHandle** copy_connections, TupleDesc tupleDesc,
    FILE* copy_file, Tuplestorestate* store, RemoteCopyType remoteCopyType);
extern void DataNodeCopyFinish(PGXCNodeHandle** copy_connections, int n_copy_connections, int primary_dn_index,
    CombineType combine_type, Relation rel);
extern bool DataNodeCopyEnd(PGXCNodeHandle* handle, bool is_error);
extern int DataNodeCopyInBinaryForAll(const char* msg_buf, int len, PGXCNodeHandle** copy_connections);

extern int ExecCountSlotsRemoteQuery(RemoteQuery* node);
extern RemoteQueryState* ExecInitRemoteQuery(RemoteQuery* node, EState* estate, int eflags, bool row_plan = true);
extern TupleTableSlot* ExecRemoteQuery(RemoteQueryState* step);
extern void ExecEndRemoteQuery(RemoteQueryState* step, bool pre_end = false);
extern void FreeParallelFunctionState(ParallelFunctionState* state);
extern void StrategyFuncSum(ParallelFunctionState* state);
extern ParallelFunctionState* RemoteFunctionResultHandler(char* sql_statement, ExecNodes* exec_nodes,
    strategy_func function, bool read_only = true, RemoteQueryExecType exec_type = EXEC_ON_DATANODES,
    bool non_check_count = false);
extern void ExecRemoteUtility(RemoteQuery* node);

extern void ExecRemoteUtility_ParallelDDLMode(RemoteQuery* node, const char* FirstExecNode);
extern RemoteQueryState* CreateResponseCombinerForBarrier(int nodeCount, CombineType combineType);
extern void CloseCombinerForBarrier(RemoteQueryState* combiner);

extern HeapTuple* ExecRemoteUtilityWithResults(
    VacuumStmt* stmt, RemoteQuery* node, ANALYZE_RQTYPE arq_type, AnalyzeMode eAnalyzeMode = ANALYZENORMAL);

extern HeapTuple* RecvRemoteSampleMessage(
    VacuumStmt* stmt, RemoteQuery* node, ANALYZE_RQTYPE arq_type, AnalyzeMode eAnalyzeMode = ANALYZENORMAL);
extern int handle_response(PGXCNodeHandle* conn, RemoteQueryState* combiner, bool isdummy = false);
extern bool is_data_node_ready(PGXCNodeHandle* conn);
extern void HandleCmdComplete(CmdType commandType, CombineTag* combine, const char* msg_body, size_t len);
extern bool FetchTuple(
    RemoteQueryState* combiner, TupleTableSlot* slot, ParallelFunctionState* parallelfunctionstate = NULL);
extern bool FetchTupleSimple(RemoteQueryState* combiner, TupleTableSlot* slot);
template <bool BatchFormat, bool ForParallelFunction>
extern bool FetchTupleByMultiChannel(
    RemoteQueryState* combiner, TupleTableSlot* slot, ParallelFunctionState* parallelfunctionstate = NULL);
extern bool FetchBatch(RemoteQueryState* combiner, VectorBatch* batch);

extern void BufferConnection(PGXCNodeHandle* conn);

extern void ExecRemoteQueryReScan(RemoteQueryState* node, ExprContext* exprCtxt);

extern void SetDataRowForExtParams(ParamListInfo params, RemoteQueryState* rq_state);

extern void ExecCloseRemoteStatement(const char* stmt_name, List* nodelist);
extern void PreCommit_Remote(char* prepareGID, bool barrierLockHeld);
extern void SubXactCancel_Remote(void);
extern char* PrePrepare_Remote(const char* prepareGID, bool implicit, bool WriteCnLocalNode);
extern void PostPrepare_Remote(char* prepareGID, char* nodestring, bool implicit);
extern bool PreAbort_Remote(bool PerfectRollback);
extern void AtEOXact_Remote(void);
extern bool IsTwoPhaseCommitRequired(bool localWrite);
extern void reset_remote_handle_xid(void);

/* Flags related to temporary objects included in query */
extern void ExecSetTempObjectIncluded(void);
extern bool ExecIsTempObjectIncluded(void);
extern TupleTableSlot* ExecProcNodeDMLInXC(EState* estate, TupleTableSlot* sourceDataSlot, TupleTableSlot* newDataSlot);

extern void pgxc_all_success_nodes(ExecNodes** d_nodes, ExecNodes** c_nodes, char** failednodes_msg);
extern void AtEOXact_DBCleanup(bool isCommit);

extern void set_dbcleanup_callback(xact_callback function, const void* paraminfo, int paraminfo_size);

void do_query(RemoteQueryState* node, bool vectorized);
bool do_query_for_planrouter(RemoteQueryState* node, bool vectorized);
void do_query_for_scangather(RemoteQueryState* node, bool vectorized);

extern void free_RemoteXactState(void);
extern void FetchGlobalStatistics(VacuumStmt* stmt, Oid relid, RangeVar* parentRel, bool isReplication = false);
extern char* repairObjectName(const char* relname);
extern char* repairTempNamespaceName(char* schemaname);

extern void pgxc_node_report_error(RemoteQueryState* combiner, int elevel = 0);
extern void pgfdw_node_report_error(RemoteQueryState* combiner);
extern void setSocketError(const char*, const char*);
extern char* getSocketError(int* errcode);
extern int getSctpSocketError(const char* str);
extern uint64 get_datasize(Plan* plan, int srvtype, int* filenum);

extern int FetchStatistics4WLM(const char* sql, void* info, Size size, strategy_func func);

extern bool IsInheritor(Oid relid);
extern Tuplesortstate* tuplesort_begin_merge(TupleDesc tupDesc, int nkeys, AttrNumber* attNums, Oid* sortOperators,
    Oid* sortCollations, const bool* nullsFirstFlags, void* combiner, int workMem);

extern void pgxc_node_remote_savepoint(
    const char* cmdString, RemoteQueryExecType exec_type, bool bNeedXid, bool bNeedBegin);
extern PGXCNodeAllHandles* connect_compute_pool(int srvtype);
extern char* generate_begin_command(void);

extern void FetchGlobalPgfdwStatistics(VacuumStmt* stmt, bool has_var, PGFDWTableAnalyze* info);
extern bool PgfdwGetTuples(
    int cn_conn_count, PGXCNodeHandle** pgxc_connections, RemoteQueryState* remotestate, TupleTableSlot* scanSlot);

extern void pgfdw_send_query(PGXCNodeAllHandles* pgxc_handles, char* query, RemoteQueryState** remotestate);
extern void PgFdwReportError(PgFdwCheckResult check_result);
extern void PgFdwSendSnapshot(StringInfo buf, Snapshot snapshot);
extern void PgFdwSendSnapshot(StringInfo buf, Snapshot snapshot, Size snap_size);
extern Snapshot PgFdwRecvSnapshot(StringInfo buf);
extern void PgFdwRemoteSender(PGXCNodeAllHandles* pgxc_handles, const char* keystr, int len, PgFdwMessageTag tag);
extern void PgFdwRemoteReply(StringInfo msg);
extern void PgFdwRemoteReceiver(PGXCNodeAllHandles* pgxc_handles, void* info, int size);

extern void SendPGXCNodeCommitCsn(uint64 commit_csn);
extern void NotifyDNSetCSN2CommitInProgress();
extern List* GetWriterHandles();
extern List* GetReaderHandles();
extern void AssembleDataRow(StreamState* node);

StringInfo* SendExplainToDNs(ExplainState*, RemoteQuery*, int*, const char*);
bool CheckPrepared(RemoteQuery* rq, Oid nodeoid);
void FindExecNodesInPBE(RemoteQueryState* planstate, ExecNodes* exec_nodes, RemoteQueryExecType exec_type);
#endif

#ifdef ENABLE_UT
#include "workload/cpwlm.h"
extern THR_LOCAL List* XactWriteNodes;
extern THR_LOCAL List* XactReadNodes;
extern PGXCNodeAllHandles* connect_compute_pool_for_HDFS();
extern PGXCNodeAllHandles* make_cp_conn(ComputePoolConfig** configs, int cnum, int srvtype, const char* dbname);
extern List* get_dnlist_for_hdfs(int fnum);
extern void ReloadTransactionNodes(void);
#endif
