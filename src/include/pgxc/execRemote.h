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
#include "pgxc/remoteCombiner.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/lsyscache.h"
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
#define RESPONSE_SEQUENCE_OK 8
#define RESPONSE_MAXCSN_RECEIVED 9

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

#define ERROR_CHECK_TIMEOUT 3 /* time out for checking some normal communication error(s) */

/* Add Skewness alarm retry count  in case something fails. default 2*/
#define ALARM_RETRY_COUNT 2

/* Combines results of INSERT statements using multiple values */
typedef struct CombineTag {
    CmdType cmdType;                   /* DML command type */
    char data[COMPLETION_TAG_BUFSIZE]; /* execution result combination data */
} CombineTag;

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

    bool preparedLocalNode;
    bool need_primary_dn_commit;
    char prepareGID[256]; /* GID used for internal 2PC */
} RemoteXactState;

#ifdef PGXC
typedef struct abort_callback_type {
    xact_callback function;
    void* fparams;
} abort_callback_type;
#endif

static inline char* GetIndexNameForStat(Oid indid, char* relname)
{
    char* indname = get_rel_name(indid);
    if (indname == NULL) {
        ereport(LOG,
                (errmsg("Analyze can not get index name by index id %u on table %s and will skip this index.",
                indid, relname)));
    }
    return indname;
}

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
    bool non_check_count = false, bool need_tran_block = false, bool need_transform_anyarray = false,
    bool active_nodes_only = false);
extern void ExecRemoteUtility(RemoteQuery* node);

extern void ExecRemoteUtility_ParallelDDLMode(RemoteQuery* node, const char* FirstExecNode);
extern void ExecRemoteUtilityParallelBarrier(const RemoteQuery* node, const char* firstExecNode);

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

extern void BufferConnection(PGXCNodeHandle* conn, bool cachedata = true);

extern void ExecRemoteQueryReScan(RemoteQueryState* node, ExprContext* exprCtxt);

extern void SetDataRowForExtParams(ParamListInfo params, RemoteQueryState* rq_state);

extern void ExecCloseRemoteStatement(const char* stmt_name, List* nodelist);

/* Flags related to temporary objects included in query */
extern void ExecSetTempObjectIncluded(void);
extern bool ExecIsTempObjectIncluded(void);
extern TupleTableSlot* ExecProcNodeDMLInXC(EState* estate, TupleTableSlot* sourceDataSlot, TupleTableSlot* newDataSlot);

extern void pgxc_all_success_nodes(ExecNodes** d_nodes, ExecNodes** c_nodes, char** failednodes_msg);
extern int PackConnections(RemoteQueryState* node);
extern void AtEOXact_DBCleanup(bool isCommit);

extern void set_dbcleanup_callback(xact_callback function, const void* paraminfo, int paraminfo_size);

extern void do_query(RemoteQueryState* node, bool vectorized = false);
extern bool do_query_for_planrouter(RemoteQueryState* node, bool vectorized = false);
extern void do_query_for_scangather(RemoteQueryState* node, bool vectorized = false);
extern void do_query_for_first_tuple(RemoteQueryState* node, bool vectorized, int regular_conn_count,
    PGXCNodeHandle** connections, PGXCNodeHandle* primaryconnection, List* dummy_connections);

extern void free_RemoteXactState(void);
extern char* repairObjectName(const char* relname);
extern char* repairTempNamespaceName(char* schemaname);

extern void pgxc_node_report_error(RemoteQueryState* combiner, int elevel = 0);
extern void setSocketError(const char*, const char*);
extern char* getSocketError(int* errcode);
extern int getStreamSocketError(const char* str);

extern int FetchStatistics4WLM(const char* sql, void* info, Size size, strategy_func func);
extern void FetchGlobalStatistics(VacuumStmt* stmt, Oid relid, RangeVar* parentRel, bool isReplication = false);

extern bool IsInheritor(Oid relid);
extern Tuplesortstate* tuplesort_begin_merge(TupleDesc tupDesc, int nkeys, AttrNumber* attNums, Oid* sortOperators,
    Oid* sortCollations, const bool* nullsFirstFlags, void* combiner, int workMem);

extern void pgxc_node_remote_savepoint(
    const char* cmdString, RemoteQueryExecType exec_type, bool bNeedXid, bool bNeedBegin,
    GlobalTransactionId transactionId = InvalidTransactionId);
extern bool pgxc_start_command_on_connection(PGXCNodeHandle* connection, RemoteQueryState* remotestate,
    Snapshot snapshot, const char* compressPlan = NULL, int cLen = 0);
extern PGXCNodeAllHandles* connect_compute_pool(int srvtype);
extern char* generate_begin_command(void);
extern PGXCNodeAllHandles* get_exec_connections(
    RemoteQueryState* planstate, ExecNodes* exec_nodes, RemoteQueryExecType exec_type);
extern void send_local_csn_min_to_ccn();
extern void csnminsync_get_global_csn_min(int conn_count, PGXCNodeHandle** connections);
extern void SendPGXCNodeCommitCsn(uint64 commit_csn);
extern void NotifyDNSetCSN2CommitInProgress();
extern void AssembleDataRow(StreamState* node);
extern bool isInLargeUpgrade();
extern uint64 get_datasize(Plan* plan, int srvtype, int* filenum);
extern void report_table_skewness_alarm(AlarmType alarmType, const char* tableName);
extern bool InternalDoQueryForPlanrouter(RemoteQueryState* node, bool vectorized);
extern List* TryGetNeededDNNum(uint64 dnneeded);
extern List* GetDnlistForHdfs(int fnum);
extern void MakeNewSpiltmap(Plan* plan, SplitMap* map);
extern List* ReassignSplitmap(Plan* plan, int dn_num);
extern int ComputeNodeBegin(int conn_count, PGXCNodeHandle** connections, GlobalTransactionId gxid);
extern void sendQuery(const char* sql, const PGXCNodeAllHandles* pgxc_handles,
                      int conn_count, bool isCoordinator,
                      RemoteQueryState* remotestate, const Snapshot snapshot);

StringInfo* SendExplainToDNs(ExplainState*, RemoteQuery*, int*, const char*);
bool CheckPrepared(RemoteQuery* rq, Oid nodeoid);
void FindExecNodesInPBE(RemoteQueryState* planstate, ExecNodes* exec_nodes, RemoteQueryExecType exec_type);
extern PGXCNodeHandle* GetRegisteredTransactionNodes(bool write);
#endif

#ifdef ENABLE_UT
#include "workload/cpwlm.h"
extern THR_LOCAL List* XactWriteNodes;
extern THR_LOCAL List* XactReadNodes;
extern PGXCNodeAllHandles* connect_compute_pool_for_HDFS();
extern PGXCNodeAllHandles* make_cp_conn(ComputePoolConfig** configs, int cnum, int srvtype, const char* dbname);
extern List* get_dnlist_for_hdfs(int fnum);
extern void ReloadTransactionNodes(void);
extern void PgFdwRemoteReply(StringInfo msg);

#endif
