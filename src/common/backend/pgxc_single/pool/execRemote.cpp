/* -------------------------------------------------------------------------
 *
 * execRemote.c
 *
 *	  Functions to execute commands on remote Datanodes
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/backend/pgxc/pool/execRemote.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <arpa/inet.h>
#include "access/twophase.h"
#include "access/gtm.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/relscan.h"
#include "access/multixact.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pgxc_node.h"
#include "commands/tablespace.h"
#include "commands/prepare.h"
#include "commands/tablecmds.h"
#include "postmaster/autovacuum.h"
#ifdef PGXC
#include "commands/trigger.h"
#endif
#include "executor/executor.h"
#include "executor/lightProxy.h"
#include "foreign/dummyserver.h"
#include "gtm/gtm_c.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgFdwRemote.h"
#include "pgxc/pgxcXact.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/var.h"
#include "pgxc/copyops.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/poolmgr.h"
#include "storage/ipc.h"
#include "storage/procarray.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "utils/datum.h"
#include "utils/extended_statistics.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/tuplesort.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "parser/parse_relation.h"
#include "pgstat.h"
#include "optimizer/streamplan.h"
#include "tcop/utility.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "access/heapam.h"
#include "utils/fmgroids.h"
#include "catalog/catalog.h"
#include "catalog/pg_statistic.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/indexing.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_inherits_fn.h"
#include "funcapi.h"
#include "commands/vacuum.h"
#include "utils/distribute_test.h"
#include "utils/batchsort.h"
#include "vecexecutor/vectorbatch.h"
#include "access/hash.h"
#include "mb/pg_wchar.h"
#include "workload/cpwlm.h"
#include "instruments/instr_unique_sql.h"
#include "utils/elog.h"
#include "utils/globalplancore.h"
#include "executor/node/nodeModifyTable.h"
#ifdef USE_SPQ
#include "libpq/libpq-int.h"
#include "optimizer/planmem_walker.h"
#endif

#ifndef MIN
#define MIN(A, B) (((B) < (A)) ? (B) : (A))
#endif

#ifdef ENABLE_UT
#define static
#endif

#pragma GCC diagnostic ignored "-Wunused-function"

extern bool IsAnalyzeTempRel(VacuumStmt* stmt);

#define ROLLBACK_RESP_LEN 9

#define DFS_PRIVATE_ITEM "DfsPrivateItem"

/*
 * Buffer size does not affect performance significantly, just do not allow
 * connection buffer grows infinitely
 */
#define COPY_BUFFER_SIZE 8192
#define PRIMARY_NODE_WRITEAHEAD (1024 * 1024)

#define PROTO_TCP 1

/* refer to ESTIMATE_BLOCK_FACTOR in src/backend/commands/analyze.cpp */
#define ESTIMATE_BLOCK_FACTOR 0.65

static int compute_node_begin(int conn_count, PGXCNodeHandle** connections, GlobalTransactionId gxid);

static void close_node_cursors(PGXCNodeHandle** connections, int conn_count, const char* cursor);
static void ExecRemoteFunctionInParallel(
    ParallelFunctionState* state, RemoteQueryExecType exec_remote_type, bool non_check_count = false);

static int pgxc_get_transaction_nodes(PGXCNodeHandle* connections[], int size, bool writeOnly);
static int GetNodeIdFromNodesDef(NodeDefinition* node_def, Oid nodeoid);
static int pgxc_get_connections(PGXCNodeHandle* connections[], int size, List* connlist);
static void pgxc_node_send_queryid_with_sync(PGXCNodeHandle** connections, int conn_count, uint64 queryId);
static TupleTableSlot* RemoteQueryNext(ScanState* node);
static bool RemoteQueryRecheck(RemoteQueryState* node, TupleTableSlot* slot);

static char* pgxc_node_get_nodelist(bool localNode);

static void ExecClearTempObjectIncluded(void);
static void init_RemoteXactState(bool preparedLocalNode);
static void clear_RemoteXactState(void);
static bool IsReturningDMLOnReplicatedTable(RemoteQuery* rq);
static void SetDataRowForIntParams(
    JunkFilter* junkfilter, TupleTableSlot* sourceSlot, TupleTableSlot* newSlot, RemoteQueryState* rq_state);
static void pgxc_append_param_val(StringInfo buf, Datum val, Oid valtype);
static void pgxc_append_param_junkval(
    TupleTableSlot* slot, AttrNumber attno, Oid valtype, StringInfo buf, bool allow_dummy_junkfilter);
static void pgxc_rq_fire_bstriggers(RemoteQueryState* node);
static void pgxc_rq_fire_astriggers(RemoteQueryState* node);
static void pgxc_check_and_update_nodedef(PlannedStmt* planstmt, PGXCNodeHandle** connections, int regular_conn_count);

bool FetchTupleSimple(RemoteQueryState* combiner, TupleTableSlot* slot);

typedef enum StatisticKind {
    StatisticNone,
    StatisticPageAndTuple,
    StatisticHistogram,
    StatisticMultiHistogram,
    StatisticPartitionPageAndTuple
} StatisticKind;

// parentRel is only valid for dfs delta table.
static void FetchStatisticsInternal(const char* schemaname, const char* relname, List* va_cols, StatisticKind kind,
    RangeVar* parentRel, VacuumStmt* stmt = NULL, bool isReplication = false);
static void FetchGlobalRelationStatistics(VacuumStmt* stmt, Oid relid, RangeVar* parentRel, bool isReplication = false);
static void FetchGlobalStatisticsInternal(const char* schemaname, const char* relname, List* va_cols,
    StatisticKind kind, RangeVar* parentRel, VacuumStmt* stmt = NULL);
static void ReceiveHistogram(
    Oid relid, TupleTableSlot* slot, bool isReplication = false, PGFDWTableAnalyze* info = NULL);
static void ReceiveHistogramMultiColStats(
    Oid relid, TupleTableSlot* slot, bool isReplication = false, PGFDWTableAnalyze* info = NULL);
static void ReceivePageAndTuple(Oid relid, TupleTableSlot* slot, VacuumStmt* stmt = NULL);
static void ReceivePartitionPageAndTuple(Oid relid, TupleTableSlot* slot);

static bool clean_splitmap(Plan* plan);
static List* reassign_splitmap(Plan* plan, int dn_num);
static void resetRelidOfRTE(PlannedStmt* ps);
static PGXCNodeAllHandles* connect_compute_pool_for_OBS();
static PGXCNodeAllHandles* connect_compute_pool_for_HDFS();
static PGXCNodeAllHandles* make_cp_conn(ComputePoolConfig** config, int num, int srvtype, const char* dbname = NULL);
static PGXCNodeAllHandles* try_make_cp_conn(
    const char* cpip, ComputePoolConfig* config, int srvtype, const char* dbname = NULL);

extern void cancel_query_without_read();
extern void destroy_handles();
extern void pgxc_node_init(PGXCNodeHandle* handle, int sock);
extern void CheckGetServerIpAndPort(const char* Address, List** AddrList, bool IsCheck, int real_addr_max);
extern int32 get_relation_data_width(Oid relid, Oid partitionid, int32* attr_widths, bool vectorized = false);

void CheckRemoteUtiltyConn(PGXCNodeHandle* conn, Snapshot snapshot);
extern void ReorganizeSqlStatement(
    ExplainState* es, RemoteQuery* rq, const char* queryString, const char* explainsql, Oid nodeoid);

void setSocketError(const char* msg, const char* node_name)
{
    StringInfoData str;
    errno_t rc = EOK;

    if (msg != NULL) {
        rc = strncpy_s(t_thrd.pgxc_cxt.socket_buffer,
            sizeof(t_thrd.pgxc_cxt.socket_buffer),
            msg,
            sizeof(t_thrd.pgxc_cxt.socket_buffer) - 1);
        securec_check(rc, "", "");
    } else {
        (void)SOCK_STRERROR(SOCK_ERRNO, t_thrd.pgxc_cxt.socket_buffer, sizeof(t_thrd.pgxc_cxt.socket_buffer));
    }

    if (IS_PGXC_DATANODE && node_name) {
        initStringInfo(&str);
        appendStringInfo(&str,
            "%s. Local: %s Remote: %s.",
            t_thrd.pgxc_cxt.socket_buffer,
            g_instance.attr.attr_common.PGXCNodeName,
            node_name);
        rc = strncpy_s(t_thrd.pgxc_cxt.socket_buffer, sizeof(t_thrd.pgxc_cxt.socket_buffer), str.data, str.len);
        securec_check(rc, "", "");
        resetStringInfo(&str);
    }
}

#define CONN_RESET_BY_PEER "Connection reset by peer"
#define CONN_TIMED_OUT "Connection timed out"
#define CONN_REMOTE_CLOSE "Remote close socket unexpectedly"
#define CONN_SCTP_ERR_1 "1002   Memeory allocate error"
#define CONN_SCTP_ERR_2 "1041   No data in buffer"
#define CONN_SCTP_ERR_3 "1046   Close because release memory"
#define CONN_SCTP_ERR_4 "1047   TCP disconnect"
#define CONN_SCTP_ERR_5 "1048   SCTP disconnect"
#define CONN_SCTP_ERR_6 "1049   Stream closed by remote"
#define CONN_SCTP_ERR_7 "1059   Wait poll unknow error"

#ifdef USE_SPQ

typedef struct SpqConnectWalkerContext {
    MethodPlanWalkerContext cxt;
    uint64 queryId;
    spq_qc_ctx *qc_ctx;
} SpqConnectWalkerContext;

void spq_finishQcThread(void);
void CopyDataRowTupleToSlot(RemoteQueryState* combiner, TupleTableSlot* slot);
static void HandleCopyOutComplete(RemoteQueryState* combiner);
static void HandleCommandComplete(
    RemoteQueryState* combiner, const char* msg_body, size_t len, PGXCNodeHandle* conn, bool isdummy);
static bool HandleRowDescription(RemoteQueryState* combiner, char* msg_body);
static void HandleDataRow(
    RemoteQueryState* combiner, char* msg_body, size_t len, Oid nodeoid, const char* remoteNodeName);
static void HandleAnalyzeTotalRow(RemoteQueryState* combiner, const char* msg_body, size_t len);
static void HandleCopyIn(RemoteQueryState* combiner);
static void HandleCopyOut(RemoteQueryState* combiner);
static void HandleCopyDataRow(RemoteQueryState* combiner, char* msg_body, size_t len);
static void HandleError(RemoteQueryState* combiner, char* msg_body, size_t len);
static void HandleNotice(RemoteQueryState* combiner, char* msg_body, size_t len);
static void HandleDatanodeCommandId(RemoteQueryState* combiner, const char* msg_body, size_t len);
static TupleTableSlot* ExecSPQRemoteQuery(PlanState* state);

inline static uint64 GetSPQQueryidFromRemoteQuery(RemoteQueryState* node)
{
    if (node->ss.ps.state != NULL && node->ss.ps.state->es_plannedstmt != NULL) {
        return node->ss.ps.state->es_plannedstmt->queryId;
    } else {
        return 0;
    }
}

static TupleTableSlot* ExecSPQRemoteQuery(PlanState* state)
{
    RemoteQueryState* node = castNode(RemoteQueryState, state);
    return ExecScan(&(node->ss), (ExecScanAccessMtd)RemoteQueryNext, (ExecScanRecheckMtd)RemoteQueryRecheck);
}
int getStreamSocketError(const char* str)
{
    if (pg_strncasecmp(str, CONN_SCTP_ERR_1, strlen(CONN_SCTP_ERR_1)) == 0)
        return ERRCODE_SCTP_MEMORY_ALLOC;
    else if (pg_strncasecmp(str, CONN_SCTP_ERR_2, strlen(CONN_SCTP_ERR_2)) == 0)
        return ERRCODE_SCTP_NO_DATA_IN_BUFFER;
    else if (pg_strncasecmp(str, CONN_SCTP_ERR_3, strlen(CONN_SCTP_ERR_3)) == 0)
        return ERRCODE_SCTP_RELEASE_MEMORY_CLOSE;
    else if (pg_strncasecmp(str, CONN_SCTP_ERR_4, strlen(CONN_SCTP_ERR_4)) == 0)
        return ERRCODE_SCTP_TCP_DISCONNECT;
    else if (pg_strncasecmp(str, CONN_SCTP_ERR_5, strlen(CONN_SCTP_ERR_5)) == 0)
        return ERRCODE_SCTP_DISCONNECT;
    else if (pg_strncasecmp(str, CONN_SCTP_ERR_6, strlen(CONN_SCTP_ERR_6)) == 0)
        return ERRCODE_SCTP_REMOTE_CLOSE;
    else if (pg_strncasecmp(str, CONN_SCTP_ERR_7, strlen(CONN_SCTP_ERR_7)) == 0)
        return ERRCODE_SCTP_WAIT_POLL_UNKNOW;
    else
        return ERRCODE_CONNECTION_FAILURE;
}
 
char* getSocketError(int* error_code)
{
    Assert(error_code != NULL);
 
    /* Set error code by checking socket error message. */
    if (pg_strncasecmp(t_thrd.pgxc_cxt.socket_buffer, CONN_RESET_BY_PEER, strlen(CONN_RESET_BY_PEER)) == 0) {
        *error_code = IS_PGXC_DATANODE ? ERRCODE_STREAM_CONNECTION_RESET_BY_PEER : ERRCODE_CONNECTION_RESET_BY_PEER;
    } else if (pg_strncasecmp(t_thrd.pgxc_cxt.socket_buffer, CONN_TIMED_OUT, strlen(CONN_TIMED_OUT)) == 0) {
        *error_code = ERRCODE_CONNECTION_TIMED_OUT;
    } else if (pg_strncasecmp(t_thrd.pgxc_cxt.socket_buffer, CONN_REMOTE_CLOSE, strlen(CONN_REMOTE_CLOSE)) == 0) {
        *error_code = IS_PGXC_DATANODE ? ERRCODE_STREAM_REMOTE_CLOSE_SOCKET : ERRCODE_CONNECTION_FAILURE;
    } else {
        *error_code = ERRCODE_CONNECTION_FAILURE;
    }
 
    return t_thrd.pgxc_cxt.socket_buffer;
}
bool SpqFetchTuple(RemoteQueryState* combiner, TupleTableSlot* slot, ParallelFunctionState* parallelfunctionstate)
{
    bool have_tuple = false;
 
    /* If we have message in the buffer, consume it */
    if (combiner->currentRow.msg) {
        CopyDataRowTupleToSlot(combiner, slot);
        have_tuple = true;
    }
 
    /*
     * If this is ordered fetch we can not know what is the node
     * to handle next, so sorter will choose next itself and set it as
     * currentRow to have it consumed on the next call to FetchTuple.
     * Otherwise allow to prefetch next tuple.
     */
    if (((RemoteQuery*)combiner->ss.ps.plan) != NULL && ((RemoteQuery*)combiner->ss.ps.plan)->sort) {
        return have_tuple;
    }
    /*
     * If we are fetching no sorted results we can not have both
     * currentRow and buffered rows. When connection is buffered currentRow
     * is moved to buffer, and then it is cleaned after buffering is completed.
     * Afterwards rows will be taken from the buffer bypassing
     * currentRow until buffer is empty, and only after that data are read
     * from a connection. The message should be allocated in the same memory context as
     * that of the slot. We are not sure of that in the call to
     * ExecStoreDataRowTuple below. If one fixes this memory issue, please
     * consider using CopyDataRowTupleToSlot() for the same.
     */
    if (RowStoreLen(combiner->row_store) > 0) {
        RemoteDataRowData dataRow;
        RowStoreFetch(combiner->row_store, &dataRow);
        NetWorkTimeDeserializeStart(t_thrd.pgxc_cxt.GlobalNetInstr);
        ExecStoreDataRowTuple(dataRow.msg, dataRow.msglen, dataRow.msgnode, slot, true);
        NetWorkTimeDeserializeEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
        return true;
    }
 
    while (combiner->conn_count > 0) {
        int res;
        PGXCNodeHandle* conn = combiner->connections[combiner->current_conn];
 
        /* Going to use a connection, buffer it if needed */
        if (conn->state == DN_CONNECTION_STATE_QUERY && conn->combiner != NULL && conn->combiner != combiner) {
            BufferConnection(conn);
        }
 
        /*
         * If current connection is idle it means portal on the Datanode is suspended.
         * If we have a tuple do not hurry to request more rows,
         * leave connection clean for other RemoteQueries.
         * If we do not have, request more and try to get it.
         */
        if (conn->state == DN_CONNECTION_STATE_IDLE) {
            /*
             * Keep connection clean.
             */
            if (have_tuple) {
                return true;
            } else {
                if (pgxc_node_send_execute(conn, combiner->cursor, 1) != 0)
                    ereport(ERROR,
                            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                             errmsg("Failed to fetch from %s[%u]", conn->remoteNodeName, conn->nodeoid)));
                if (pgxc_node_send_sync(conn) != 0)
                    ereport(ERROR,
                            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                             errmsg("Failed to fetch from %s[%u]", conn->remoteNodeName, conn->nodeoid)));
                if (pgxc_node_receive(1, &conn, NULL))
                    ereport(ERROR,
                            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                             errmsg("Failed to fetch from %s[%u]", conn->remoteNodeName, conn->nodeoid)));
                conn->combiner = combiner;
            }
        }
 
        /* read messages */
        res = handle_response(conn, combiner);
        if (res == RESPONSE_EOF) {
            /* incomplete message, read more */
            if (pgxc_node_receive(1, &conn, NULL)) {
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Failed to fetch from Datanode %u", conn->nodeoid)));
            }
            continue;
        } else if (res == RESPONSE_COMPLETE) {
            /* Make last connection current */
            combiner->conn_count = combiner->conn_count - 1;
            if (combiner->current_conn >= combiner->conn_count) {
                combiner->current_conn = 0;
            } else {
                combiner->connections[combiner->current_conn] = combiner->connections[combiner->conn_count];
            }
        } else if (res == RESPONSE_SUSPENDED) {
            /* Make next connection current */
            combiner->current_conn = combiner->conn_count + 1;
            if (combiner->current_conn >= combiner->conn_count) {
                combiner->current_conn = 0;
            }
        } else if (res == RESPONSE_DATAROW && have_tuple) {
            /* We already have a tuple and received another one, leave it tillnext fetch. */
            return true;
        }
 
        if (combiner->currentRow.msg) {
            CopyDataRowTupleToSlot(combiner, slot);
            have_tuple = true;
        }
    }
 
    /* report end of data to the caller */
    if (!have_tuple) {
        ExecClearTuple(slot);
    }
 
    return have_tuple;
}
static void update_spq_port(PGXCNodeHandle **conn, PlannedStmt *planstmt, int regular_conn_count)
{
    for (int i = 0; i < regular_conn_count; i++) {
        if (strcmp(planstmt->nodesDefinition[i].nodename.data, conn[i]->remoteNodeName) != 0) {
            elog(WARNING, "update_spq_port index[%d] not same[%s][%s]", i, planstmt->nodesDefinition[i].nodename.data,
                  conn[i]->remoteNodeName);
        }
        planstmt->nodesDefinition[i].nodectlport = conn[i]->tcpCtlPort;
        planstmt->nodesDefinition[i].nodesctpport = conn[i]->listenPort;
    }
}
static void
HandleDatanodeGxid(PGXCNodeHandle* conn, const char* msg_body, size_t len)
{
    uint32 n32;
    TransactionId gxid = InvalidTransactionId;
 
    Assert(msg_body != NULL);
    Assert(len >= 2);
 
    /* Get High half part */
    errno_t rc = 0;
    rc = memcpy_s(&n32, sizeof(uint32), &msg_body[0], sizeof(uint32));
    securec_check(rc, "\0", "\0");
    gxid += ((uint64)ntohl(n32)) << 32;
    /* Get low half part */
    rc = memcpy_s(&n32, sizeof(uint32), &msg_body[0] + sizeof(uint32), sizeof(uint32));
    securec_check(rc, "\0", "\0");
    gxid += ntohl(n32);
    conn->remote_top_txid = gxid;
}
static void HandleLibcommPort(PGXCNodeHandle *conn, const char *msg_body, size_t len)
{
    Assert(msg_body != NULL);
    errno_t rc = 0;
    uint16 n16;
    rc = memcpy_s(&n16, sizeof(uint16), msg_body, sizeof(uint16));
    securec_check(rc, "\0", "\0");
    conn->tcpCtlPort = ntohs(n16);
    msg_body += sizeof(uint16);
    rc = memcpy_s(&n16, sizeof(uint16), msg_body, sizeof(uint16));
    securec_check(rc, "\0", "\0");
    conn->listenPort = ntohs(n16);
    msg_body += sizeof(uint16);
    elog(DEBUG1, "spq HandleLibcommPort get [%d:%d]", conn->tcpCtlPort, conn->listenPort);
    rc = memcpy_s(&n16, sizeof(uint16), msg_body, sizeof(uint16));
    securec_check(rc, "\0", "\0");
    uint16 nodenamelen = ntohs(n16);
    msg_body += sizeof(uint16);
    char nodename[NAMEDATALEN];
    rc = memcpy_s(nodename, NAMEDATALEN * sizeof(char), msg_body, nodenamelen * sizeof(char));
    securec_check(rc, "\0", "\0");
    if (strcmp(conn->remoteNodeName, nodename) != 0) {
        elog(ERROR, "remote name [%s] not match cluster map: [%s].", nodename, conn->remoteNodeName);
    }
}
static void HandleDirectRead(PGXCNodeHandle *conn, const char *msg_body, size_t len)
{
    Assert(msg_body != NULL);
    Assert(len >= 4);
    errno_t rc = 0;
    uint32 n32;
    int readlen = 0;
    rc = memcpy_s(&n32, sizeof(uint32), msg_body, sizeof(uint32));
    securec_check(rc, "", "");
    int oidcount = ntohl(n32);
    readlen += sizeof(uint32);
    int checkLen = sizeof(uint32) + (sizeof(uint32) + sizeof(uint32)) * oidcount;
    if (len != checkLen) {
        elog(LOG, "HandleDirectRead len not match [%d:%d]", len, checkLen);
        return;
    }
    ListCell *cell;
    foreach (cell, u_sess->spq_cxt.direct_read_map) {
        rc = memcpy_s(&n32, sizeof(uint32), msg_body + readlen, sizeof(uint32));
        securec_check(rc, "", "");
        Oid oid = ntohl(n32);
        readlen += sizeof(uint32);

        rc = memcpy_s(&n32, sizeof(uint32), msg_body + readlen, sizeof(uint32));
        securec_check(rc, "", "");
        uint32 ReadBlkNum = ntohl(n32);
        readlen += sizeof(uint32);
        SpqDirectReadEntry *entry = (SpqDirectReadEntry *)lfirst(cell);
        if (entry->rel_id == oid) {
            entry->nums = ReadBlkNum;
        }
    }
}
static void HandleLocalCsnMin(PGXCNodeHandle* conn, const char* msg_body, size_t len)
{
    Assert(msg_body != NULL);
    Assert(len >= 2);
 
    uint32 n32;
    errno_t rc;
    CommitSeqNo csn_min = 0;
    /* Get High half part */
    rc = memcpy_s(&n32, sizeof(uint32), &msg_body[0], sizeof(uint32));
    securec_check(rc, "", "");
    csn_min += ((uint64)ntohl(n32)) << 32;
    /* Get low half part */
    rc = memcpy_s(&n32, sizeof(uint32), &msg_body[0] + sizeof(uint32), sizeof(uint32));
    securec_check(rc, "", "");
    csn_min += ntohl(n32);
    if (module_logging_is_on(MOD_TRANS_SNAPSHOT)) {
        ereport(LOG, (errmodule(MOD_TRANS_SNAPSHOT),
            errmsg("[CsnMinSync] local csn min : %lu from node %u", csn_min, conn->nodeoid)));
    }
    if (csn_min < t_thrd.xact_cxt.ShmemVariableCache->local_csn_min) {
        t_thrd.xact_cxt.ShmemVariableCache->local_csn_min = csn_min;
    }
}
 
static void HandleMaxCSN(RemoteQueryState* combiner, const char* msg, int msg_len)
{
    Assert(msg_len == sizeof(int64) + sizeof(int8));
    combiner->maxCSN = ntohl64(*((CommitSeqNo *)msg));
    combiner->hadrMainStandby = *(bool*)(msg + sizeof(int64));
}

static SpqAdpScanReqState *make_adps_state(SpqAdpScanPagesReq *req)
{
    SpqAdpScanReqState *paging_state = (SpqAdpScanReqState *)palloc(sizeof(SpqAdpScanReqState));
    if (!paging_state)
        return NULL;
    paging_state->plan_node_id = req->plan_node_id;
    paging_state->direction = req->direction;
    paging_state->nblocks = req->nblocks;
    paging_state->cur_scan_iter_no = req->cur_scan_iter_no;
    paging_state->node_num = t_thrd.spq_ctx.qc_ctx->num_nodes;
    paging_state->node_states = (NodePagingState *)palloc(sizeof(NodePagingState) * paging_state->node_num);
    return paging_state;
}

static void init_adps_state_per_worker(SpqAdpScanReqState *p_state)
{
    for (int i = 0; i < p_state->node_num; ++i) {
        NodePagingState *n_state = &p_state->node_states[i];
        n_state->finish = false;
        n_state->batch_size = 512;

        int64 cached_unit_pages = n_state->batch_size * p_state->node_num;

        int64 start, end;
        int64 header_unit_page;
        int64 tail_unit_page;
        if (p_state->nblocks == InvalidBlockNumber) {
            /* Scan all the blocks */
            start = 0;
            end = p_state->nblocks - 1;
        } else {
            /* Forward scan part of blocks */
            if (p_state->direction == ForwardScanDirection) {
                start = 0;
                end = p_state->nblocks - 1;
            } else {
                start = p_state->nblocks - 1;
                end = 0;
                if (start < 0)
                    start = 0;
            }
        }
        header_unit_page = cached_unit_pages * (int64_t)(start / cached_unit_pages);
        tail_unit_page = cached_unit_pages * (int64_t)(end / cached_unit_pages);

        n_state->tail_unit_begin = tail_unit_page + n_state->batch_size * i;
        n_state->tail_unit_end = tail_unit_page + n_state->batch_size * (i + 1) - 1;
        if (n_state->tail_unit_begin > end) {
            n_state->tail_unit_begin -= cached_unit_pages;
            n_state->tail_unit_end -= cached_unit_pages;
        } else if (n_state->tail_unit_end > end) {
            n_state->tail_unit_end = end;
        }
        if (n_state->tail_unit_end < start) {
            n_state->finish = true;
        } else if (n_state->tail_unit_begin < start) {
            n_state->tail_unit_begin = start;
        }

        n_state->header_unit_begin = header_unit_page + n_state->batch_size * i;
        n_state->header_unit_end = header_unit_page + n_state->batch_size * (i + 1) - 1;
        if (n_state->header_unit_end < start) {
            n_state->header_unit_begin += cached_unit_pages;
            n_state->header_unit_end += cached_unit_pages;
        } else if (n_state->header_unit_begin < start) {
            n_state->header_unit_begin = start;
        }
        if (n_state->header_unit_begin > end) {
            n_state->finish = true;
        } else if (n_state->header_unit_end > end) {
            n_state->header_unit_end = end;
        }
        if (p_state->direction == ForwardScanDirection) {
            n_state->current_page = n_state->header_unit_begin;
        } else {
            n_state->current_page = n_state->tail_unit_end;
        }
    }
}

static bool adps_get_next_unit(SpqAdpScanReqState *p_state, int idx, int64_t *start, int64_t *end, bool current_is_last)
{
    NodePagingState *n_state = &p_state->node_states[idx];
    int cached_unit_pages = n_state->batch_size * p_state->node_num;

    if (n_state->finish) {
        return false;
    }

    /* Forward scan */
    if (p_state->direction == ForwardScanDirection) {
        if (n_state->current_page == n_state->header_unit_begin) {
            *start = n_state->current_page;
            *end = n_state->header_unit_end;
            n_state->current_page = n_state->header_unit_end + cached_unit_pages - n_state->batch_size + 1;
        } else if (n_state->current_page >= n_state->tail_unit_begin) {
            if (current_is_last) {
                /* When this is the last only slice, make batch_size small */
                int small_batch = n_state->batch_size / 16;
                /* But at least one page */
                if (small_batch < 1)
                    small_batch = 1;
                *start = n_state->current_page;
                *end = n_state->current_page + small_batch - 1;
                /* And not large than tail */
                if (*end > n_state->tail_unit_end)
                    *end = n_state->tail_unit_end;
                n_state->current_page += small_batch;
            } else {
                *start = n_state->current_page;
                *end = n_state->tail_unit_end;
                n_state->finish = true;
            }
        } else {
            *start = n_state->current_page;
            *end = n_state->current_page + n_state->batch_size - 1;
            n_state->current_page += cached_unit_pages;
        }
        if (n_state->current_page > n_state->tail_unit_end)
            n_state->finish = true;
    } else {
        if (n_state->current_page == n_state->header_unit_end) {
            *start = n_state->current_page;
            *end = n_state->header_unit_begin;
            n_state->finish = true;
        } else if (n_state->current_page == n_state->tail_unit_end) {
            *start = n_state->current_page;
            *end = n_state->tail_unit_begin;
            n_state->current_page = n_state->tail_unit_begin - cached_unit_pages + n_state->batch_size - 1;
        } else {
            *start = n_state->current_page;
            *end = n_state->current_page - n_state->batch_size + 1;
            n_state->current_page -= cached_unit_pages;
        }
        if (n_state->current_page < n_state->header_unit_begin)
            n_state->finish = true;
    }
    return true;
}

static bool adps_get_next_scan_unit(SpqAdpScanReqState *p_state, SpqAdpScanPagesRes *pRes, int node_idx)
{
    int i;
    bool found_next_unit;
    int last_only_idx = -1, unfinished = 0;
    int64_t start = -1, end = -1;

    for (i = 0; i < p_state->node_num; ++i) {
        if (!p_state->node_states[i].finish) {
            ++unfinished;
            last_only_idx = i;
        }
    }
    last_only_idx = (unfinished == 1) ? i : (-1);

    found_next_unit = adps_get_next_unit(p_state, node_idx, &start, &end, last_only_idx == node_idx);
    if (!found_next_unit) {
        /*
         * May be first task is empty, scan the other tasks.
         */
        for (i = 0; i < p_state->node_num; i++) {
            /* Skip the worker in the first task */
            if (i == node_idx)
                continue;

            /* Scan other tasks on each px workers */
            found_next_unit = adps_get_next_unit(p_state, i, &start, &end, last_only_idx == i);
            if (found_next_unit)
                break;
        }
    }

    /* Get the scan unit, update the result's page_start and page_end */
    pRes->page_start = (BlockNumber)start;
    pRes->page_end = (BlockNumber)end;
    return found_next_unit;
}

static bool check_match_and_update_state(SpqAdpScanReqState *p_state, SpqAdpScanPagesReq *seqReq, bool *has_finished)
{
    if (p_state->plan_node_id != seqReq->plan_node_id)
        return false;
    /* this round has finished */
    if (p_state->cur_scan_iter_no > seqReq->cur_scan_iter_no) {
        *has_finished = true;
        return true;
    }
    *has_finished = false;
    /* upgrade to next round */
    if (p_state->cur_scan_iter_no < seqReq->cur_scan_iter_no) {
        if (!p_state->this_round_finish) {
            /* maybe error occur in paging */
            elog(ERROR, "block_iter: error: round %ld has unfinished page", p_state->cur_scan_iter_no);
        }
        p_state->cur_scan_iter_no++;
        /* must be one round ahead */
        assert(p_state->cur_scan_iter_no == seqReq->cur_scan_iter_no);
        /* reinit the paging state */
        init_adps_state_per_worker(p_state);
    }
    return true;
}

static void adps_array_append(SpqScanAdpReqs *array, SpqAdpScanReqState *state)
{
    array->size += 1;
    if (array->size > array->max) {
        SpqAdpScanReqState **temp = array->req_states;
        int size = array->max * sizeof(SpqAdpScanReqState *);
        array->req_states = (SpqAdpScanReqState **)palloc(size * 2);
        errno_t rc = memcpy_s(array->req_states, size * 2, temp, size);
        securec_check(rc, "\0", "\0");
        array->max *= 2;
        pfree(temp);
    }
    array->req_states[array->size - 1] = state;
}

SpqAdpScanPagesRes adps_get_response_block(SpqAdpScanPagesReq* seqReq, int node_idx)
{
    SpqAdpScanPagesRes seqRes;
    SpqAdpScanReqState *p_state;
    seqRes.page_start = InvalidBlockNumber;
    seqRes.page_end = InvalidBlockNumber;
    seqRes.success = 0;
    if (node_idx < 0 || seqReq->plan_node_id < 0) {
        elog(ERROR, "adps_get_response_block: unrecognized node_id");
        return seqRes;
    }
    bool found = false;
    /*
     * Init seq_paging_array is 0, so at the beginning of searching,
     * it will miss.
     */
    for (int i = 0; i < t_thrd.spq_ctx.qc_ctx->seq_paging_array.size; i++) {
        bool has_finished = false;
        p_state = t_thrd.spq_ctx.qc_ctx->seq_paging_array.req_states[i];
        if (check_match_and_update_state(p_state, seqReq, &has_finished)) {
            /* This round has consumed by other workers */
            if (has_finished)
                return seqRes;

            found = true;

            /* Search all the nodes to find the next unit(or page) to read */
            if (adps_get_next_scan_unit(p_state, &seqRes, node_idx)) {
                BlockNumber page_count;
                seqRes.success = 1;
                page_count = Abs((int64_t)seqRes.page_end - (int64_t)seqRes.page_start) + 1;
            }
            break;
        }
    }
    /* Can not find a task matches the request task, init a new task and record it */
    if (!found) {
        p_state = make_adps_state(seqReq);
        if (!p_state) {
            elog(ERROR, "not enough memory when adps_get_response_block");
            return seqRes;
        }
        /* init node state */
        adps_array_append(&t_thrd.spq_ctx.qc_ctx->seq_paging_array, p_state);
        init_adps_state_per_worker(p_state);
        if (adps_get_next_scan_unit(p_state, &seqRes, node_idx)) {
            BlockNumber page_count;
            seqRes.success = 1;
            page_count = Abs((int64_t)seqRes.page_end - (int64_t)seqRes.page_start) + 1;
        }
    }
    if (p_state == nullptr) {
        elog(ERROR, "not enough memory when adps_get_response_block");
        return seqRes;
    }

    /* Fix the result, rs_nblocks may be different between workers */
    if (seqRes.success) {
        if (ForwardScanDirection == seqReq->direction) {
            if (seqRes.page_start >= p_state->nblocks)
                seqRes.success = false;
            if (seqRes.page_end >= p_state->nblocks)
                seqRes.page_end = p_state->scan_end - 1;
        } else {
            /* Move to the tail page */
            if (seqRes.page_start >= p_state->nblocks)
                seqRes.page_start = p_state->scan_end - 1;

            /* Move to the head page */
            if (seqRes.page_end < 0) {
                seqRes.page_end = p_state->scan_start;
            }
        }
    }
    return seqRes;
}

int spq_handle_response(PGXCNodeHandle* conn, RemoteQueryState* combiner, bool isdummy)
{
    char* msg = NULL;
    int msg_len;
    char msg_type;
    bool suspended = false;
    bool error_flag = false;
    int node_idx;
    int32 cur_smp_id;

    node_idx = conn->nodeIdx;
 
    for (;;) {
        Assert(conn->state != DN_CONNECTION_STATE_IDLE);
 
        /*
         * If we are in the process of shutting down, we
         * may be rolling back, and the buffer may contain other messages.
         * We want to avoid a procarray exception
         * as well as an error stack overflow.
         *
         * If not in GPC mode, should receive datanode messages but not interrupt immediately in loop while.
         */
        if (t_thrd.proc_cxt.proc_exit_inprogress && ENABLE_CN_GPC) {
            conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
            ereport(DEBUG2,
                (errmsg("DN_CONNECTION_STATE_ERROR_FATAL0 is set for connection to node %s[%u] when proc_exit_inprogress",
                    conn->remoteNodeName, conn->nodeoid)));
        }
 
        /* don't read from from the connection if there is a fatal error */
        if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL) {
            ereport(DEBUG2,
                (errmsg("handle_response0 returned with DN_CONNECTION_STATE_ERROR_FATAL for connection to node %s[%u] ",
                    conn->remoteNodeName, conn->nodeoid)));
            return RESPONSE_COMPLETE;
        }
 
        /* No data available, read one more time or exit */
        if (!HAS_MESSAGE_BUFFERED(conn)) {
            /*
             * For FATAL error, no need to read once more, because openGauss thread(DN) will exit
             * immediately after sending error message without sending 'Z'(ready for query).
             */
            if (combiner != NULL && combiner->is_fatal_error) {
                conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
                conn->combiner = NULL;
 
                return RESPONSE_COMPLETE;
            }
 
            if (error_flag) {
                /* incomplete message, if last message type is ERROR,read once more */
                if (pgxc_node_receive(1, &conn, NULL))
                    ereport(ERROR,
                        (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Failed to receive message from %s[%u]",
                        conn->remoteNodeName, conn->nodeoid)));
                error_flag = false;
                continue;
            } else {
                return RESPONSE_EOF;
            }
        }
        /* no need to check conn's combiner when abort transaction */
        Assert(t_thrd.xact_cxt.bInAbortTransaction || conn->combiner == combiner || conn->combiner == NULL);
 
        msg_type = get_message(conn, &msg_len, &msg);
        LIBCOMM_DEBUG_LOG("handle_response to node:%s[nid:%d,sid:%d] with msg:%c",
            conn->remoteNodeName,
            conn->gsock.idx,
            conn->gsock.sid,
            msg_type);
 
        switch (msg_type) {
            case '\0': /* Not enough data in the buffer */
                return RESPONSE_EOF;
            case 'c': /* CopyToCommandComplete */
                HandleCopyOutComplete(combiner);
                break;
            case 'C': /* CommandComplete */
                HandleCommandComplete(combiner, msg, msg_len, conn, isdummy);
                break;
            case 'T': /* RowDescription */
#ifdef DN_CONNECTION_DEBUG
                Assert(!conn->have_row_desc);
                conn->have_row_desc = true;
#endif
                if (HandleRowDescription(combiner, msg))
                    return RESPONSE_TUPDESC;
                break;
            case 'D': /* DataRow */
            case 'B': /* DataBatch */
#ifdef DN_CONNECTION_DEBUG
                Assert(conn->have_row_desc);
#endif
                HandleDataRow(combiner, msg, msg_len, conn->nodeoid, conn->remoteNodeName);
                return RESPONSE_DATAROW;
            case 'P': /* AnalyzeTotalRow */
                HandleAnalyzeTotalRow(combiner, msg, msg_len);
                return RESPONSE_ANALYZE_ROWCNT;
                break;
            case 'U': /* Stream instrumentation data */
                /* receive data from the CN of the compute pool in first thread
                 * of the smp.
                 */
                cur_smp_id = -1;
                if (u_sess->instr_cxt.global_instr)
                    u_sess->instr_cxt.global_instr->deserialize(node_idx, msg, msg_len, false, cur_smp_id);
                break;
            case 'u': /* OBS runtime instrumentation data */
                if (u_sess->instr_cxt.obs_instr)
                    u_sess->instr_cxt.obs_instr->deserialize(msg, msg_len);
                break;
            case 'V': /* Track data for developer-define profiling */
                /* receive data from the CN of the compute pool in first thread
                 * of the smp.
                 */
                if (0 != u_sess->stream_cxt.smp_id && IS_PGXC_DATANODE)
                    break;
 
                if (u_sess->instr_cxt.global_instr)
                    u_sess->instr_cxt.global_instr->deserializeTrack(node_idx, msg, msg_len);
                break;
            case 's': /* PortalSuspended */
                suspended = true;
                break;
            case '1': /* ParseComplete */
            case '2': /* BindComplete */
            case '3': /* CloseComplete */
            case 'n': /* NoData */
                /* simple notifications, continue reading */
                break;
            case 'G': /* CopyInResponse */
                conn->state = DN_CONNECTION_STATE_COPY_IN;
                HandleCopyIn(combiner);
                /* Done, return to caller to let it know the data can be passed in */
                return RESPONSE_COPY;
            case 'H': /* CopyOutResponse */
                conn->state = DN_CONNECTION_STATE_COPY_OUT;
                HandleCopyOut(combiner);
                return RESPONSE_COPY;
            case 'd': /* CopyOutDataRow */
                conn->state = DN_CONNECTION_STATE_COPY_OUT;
                HandleCopyDataRow(combiner, msg, msg_len);
                break;
            case 'E': /* ErrorResponse */
                HandleError(combiner, msg, msg_len);
                add_error_message(conn, "%s", combiner->errorMessage);
                error_flag = true;
                /*
                 * Do not return with an error, we still need to consume Z,
                 * ready-for-query
                 */
                break;
            case 'N': /* NoticeResponse */
                HandleNotice(combiner, msg, msg_len);
                break;
            case 'A': /* NotificationResponse */
            case 'S': /* SetCommandComplete */
                /*
                 * Ignore these to prevent multiple messages, one from each
                 * node. Coordinator will send one for DDL anyway
                 */
                break;
            case 'Z': /* ReadyForQuery */
            {
                /*
                 * Return result depends on previous connection state.
                 * If it was PORTAL_SUSPENDED Coordinator want to send down
                 * another EXECUTE to fetch more rows, otherwise it is done
                 * with the connection
                 */
                int result = suspended ? RESPONSE_SUSPENDED : RESPONSE_COMPLETE;
                conn->transaction_status = msg[0];
                conn->state = DN_CONNECTION_STATE_IDLE;
                conn->combiner = NULL;
#ifdef DN_CONNECTION_DEBUG
                conn->have_row_desc = false;
#endif
                return result;
            }
            case 'M': /* Command Id */
                HandleDatanodeCommandId(combiner, msg, msg_len);
                break;
            case 'm': /* Commiting */
                conn->state = DN_CONNECTION_STATE_IDLE;
                combiner->request_type = REQUEST_TYPE_COMMITING;
                return RESPONSE_COMPLETE;
            case 'b':
                conn->state = DN_CONNECTION_STATE_IDLE;
                return RESPONSE_BARRIER_OK;
            case 'y':
                conn->state = DN_CONNECTION_STATE_IDLE;
                return RESPONSE_SEQUENCE_OK;
            case 'O': /* PlanIdComplete */
                conn->state = DN_CONNECTION_STATE_IDLE;
                return RESPONSE_PLANID_OK;
            case 'l': /* libcomm port infomation */
                HandleLibcommPort(conn, msg, msg_len);
                break;
            case 'i': /* Direct Read */
                HandleDirectRead(conn, msg, msg_len);
                break;
            case 'g': /* DN top xid */
                HandleDatanodeGxid(conn, msg, msg_len);
                break;
            case 'L': /* DN local csn min */
                HandleLocalCsnMin(conn, msg, msg_len);
                return RESPONSE_COMPLETE;
            case 'z': /* pbe for ddl */
                break;
            case 'J':
                conn->state = DN_CONNECTION_STATE_IDLE;
                HandleMaxCSN(combiner, msg, msg_len);
                return RESPONSE_MAXCSN_RECEIVED;
            case 'I': /* EmptyQuery */
            default:
                /* sync lost? */
                elog(WARNING, "Received unsupported message type: %c", msg_type);
                conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
                /* stop reading */
                return RESPONSE_COMPLETE;
        }
    }
    /* never happen, but keep compiler quiet */
    return RESPONSE_EOF;
}
static void ExecInitPlanState(PlanState* plan_state, EState* estate, RemoteQuery* node, RemoteQueryState* remotestate)
{
    plan_state->state = estate;
    plan_state->plan = (Plan*)node;
    plan_state->qual = (List*)ExecInitExpr((Expr*)node->scan.plan.qual, (PlanState*)remotestate);
    plan_state->targetlist = (List*)ExecInitExpr((Expr*)node->scan.plan.targetlist, (PlanState*)remotestate);
    ExecAssignExprContext(estate, plan_state);
    ExecInitResultTupleSlot(estate, &remotestate->ss.ps);
    plan_state->ps_vec_TupFromTlist = false;
    ExecAssignResultTypeFromTL(&remotestate->ss.ps);
}

bool backward_connection_walker(Node *plan, void *cxt)
{
    if (plan == nullptr) return false;
    if (IsA(plan, SpqSeqScan)) {
        QCConnKey key = {
            .query_id = u_sess->debug_query_id,
            .plan_node_id = ((Plan*)plan)->plan_node_id,
            .node_id = 0,
            .type = SPQ_QC_CONNECTION,
        };
        bool found = false;
        QCConnEntry *entry;
        pthread_rwlock_wrlock(&g_instance.spq_cxt.adp_connects_lock);
        entry = (QCConnEntry *)hash_search(g_instance.spq_cxt.adp_connects, (void *)&key, HASH_FIND, &found);
        if (!found) {
            pthread_rwlock_unlock(&g_instance.spq_cxt.adp_connects_lock);
            ereport(ERROR, ((errmsg("spq forward direction not found"))));
        }
        // connection from qc already here, try build backward connection
        if (entry->forward.idx == 0) {
            BackConnInfo fcmsg;
            fcmsg.node_idx = entry->backward.idx;
            fcmsg.version = entry->backward.ver;
            fcmsg.streamcap = entry->streamcap;
            fcmsg.query_id = u_sess->debug_query_id;
            fcmsg.stream_key = {
                .queryId = entry->key.query_id,
                .planNodeId = entry->key.plan_node_id,
                .producerSmpId = 0,
                .consumerSmpId = 0,
            };
            fcmsg.backward = &entry->backward;
            int error = gs_r_build_reply_connection(&fcmsg, entry->backward.ver, &entry->forward.sid);
            if (error != 0) {
                gs_close_gsocket(&entry->forward);
                pthread_rwlock_unlock(&g_instance.spq_cxt.adp_connects_lock);
                ereport(ERROR, ((errmsg("spq try build dual channel backward direction failed"))));
            }
            entry->forward.idx = entry->backward.idx;
            entry->forward.ver = entry->backward.ver;
            entry->forward.type = GSOCK_PRODUCER;
        }
        pthread_rwlock_unlock(&g_instance.spq_cxt.adp_connects_lock);
        if (entry->backward.idx == 0) {
            gs_close_gsocket(&entry->backward);
            ereport(ERROR, ((errmsg("spq try build dual channel forward direction failed"))));
        }
        u_sess->spq_cxt.adp_connections = lappend(u_sess->spq_cxt.adp_connections, entry);
    }
    return plan_tree_walker(plan, (MethodWalker)backward_connection_walker, cxt);
}

void build_backward_connection(PlannedStmt *planstmt)
{
    if (!planstmt || !planstmt->enable_adaptive_scan) return;
    if (!u_sess->stream_cxt.stream_runtime_mem_cxt) return;
    AutoContextSwitch switcher(u_sess->stream_cxt.stream_runtime_mem_cxt);
    MethodPlanWalkerContext cxt;
    cxt.base.init_plans = NIL;
    cxt.base.traverse_flag = NULL;
    exec_init_plan_tree_base(&cxt.base, planstmt);
    backward_connection_walker((Node *)planstmt->planTree, &cxt);
}

void disconnect_qc_conn()
{
    if (u_sess->spq_cxt.adp_connections == NIL) return;
    if (!StreamTopConsumerAmI()) return;
    ListCell *cell;
    foreach (cell, u_sess->spq_cxt.adp_connections) {
        QCConnEntry *entry = (QCConnEntry *)lfirst(cell);
        bool found;
        pthread_rwlock_wrlock(&g_instance.spq_cxt.adp_connects_lock);
        gs_close_gsocket(&entry->backward);
        hash_search(g_instance.spq_cxt.adp_connects, (void *)entry, HASH_REMOVE, &found);
        pthread_rwlock_unlock(&g_instance.spq_cxt.adp_connects_lock);
    }
    list_free(u_sess->spq_cxt.adp_connections);
    u_sess->spq_cxt.adp_connections = NIL;
}

bool build_connections(Node* plan, void* cxt)
{
    if (plan == nullptr) return false;
    if (IsA(plan, SpqSeqScan)) {
        int error;
        errno_t rc = EOK;

        SpqConnectWalkerContext* walkerCxt = (SpqConnectWalkerContext*)cxt;
        PlannedStmt* planstmt = (PlannedStmt*)walkerCxt->cxt.base.node;
        int num_nodes = planstmt->num_nodes;
        NodeDefinition *nodesDef = planstmt->nodesDefinition;
        libcommaddrinfo **addressArray = (libcommaddrinfo **)palloc(sizeof(libcommaddrinfo *) * num_nodes);

        for (int i = 0; i < num_nodes; ++i) {
            int nodeNameLen = strlen(nodesDef[i].nodename.data);
            int nodehostLen = strlen(nodesDef[i].nodehost.data);
            addressArray[i] = (libcomm_addrinfo *)palloc0(sizeof(libcomm_addrinfo));
            addressArray[i]->host = (char *)palloc0(NAMEDATALEN);
            addressArray[i]->ctrl_port = nodesDef[i].nodectlport;
            addressArray[i]->listen_port = nodesDef[i].nodesctpport;
            addressArray[i]->nodeIdx = nodesDef[i].nodeid;
            rc = strncpy_s(addressArray[i]->host, NAMEDATALEN, nodesDef[i].nodehost.data, nodehostLen + 1);
            securec_check(rc, "\0", "\0");
            rc = strncpy_s(addressArray[i]->nodename, NAMEDATALEN, nodesDef[i].nodename.data, nodeNameLen + 1);
            securec_check(rc, "\0", "\0");
            /* set flag for parallel send mode */
            addressArray[i]->parallel_send_mode = false;

            addressArray[i]->streamKey.queryId = walkerCxt->queryId;
            addressArray[i]->streamKey.planNodeId = ((Plan*)plan)->plan_node_id;
            addressArray[i]->streamKey.producerSmpId = -1;
            addressArray[i]->streamKey.consumerSmpId = -1;
        }

        error = gs_connect(addressArray, num_nodes, -1);

        if (error != 0) {
            ereport(ERROR, (errmsg("connect failed, code : %d", error)));
        }

        for (int i = 0; i < num_nodes; ++i) {
            QCConnEntry* entry = (QCConnEntry*)palloc(sizeof(QCConnEntry));
            entry->key = {
                .query_id = walkerCxt->queryId,
                .plan_node_id = ((Plan*)plan)->plan_node_id,
                .node_id = addressArray[i]->gs_sock.idx,
                .type = SPQ_QE_CONNECTION,
            };
            entry->scannedPageNum = 0;
            entry->forward = addressArray[i]->gs_sock;
            entry->backward.idx = 0;
            entry->internal_node_id = i;
            walkerCxt->qc_ctx->connects = lappend(walkerCxt->qc_ctx->connects, (void*)entry);
            pfree(addressArray[i]);
        }
        pfree(addressArray);
    }
    return plan_tree_walker(plan, (MethodWalker)build_connections, cxt);
}

void spq_adps_initconns(RemoteQueryState *node)
{
    SpqConnectWalkerContext cxt;
    cxt.cxt.base.init_plans = NIL;
    cxt.cxt.base.traverse_flag = NULL;
    exec_init_plan_tree_base(&cxt.cxt.base, node->ss.ps.state->es_plannedstmt);
    cxt.queryId = node->queryId;
    cxt.qc_ctx = node->qc_ctx;
    build_connections((Node*)node->ss.ps.plan->lefttree, (void*)&cxt);
}

void spq_adps_consumer()
{
    if (t_thrd.spq_ctx.qc_ctx->connects == nullptr) {
        return;
    }

    SpqAdpScanPagesReq req;
    SpqAdpScanPagesRes res;

    ListCell *cell;
    foreach(cell, t_thrd.spq_ctx.qc_ctx->connects) {
        QCConnEntry* entry = (QCConnEntry*)lfirst(cell);
        /* it means backward connection not build yet */
        if (entry->backward.idx == 0) {
            bool found = false;
            QCConnEntry* entry_in_hash;
            pthread_rwlock_rdlock(&g_instance.spq_cxt.adp_connects_lock);
            entry_in_hash = (QCConnEntry*)hash_search(g_instance.spq_cxt.adp_connects, (void*)(&(entry->key)), HASH_FIND, &found);
            if (found && entry_in_hash->backward.idx != 0) {
                entry->backward = entry_in_hash->backward;
            }
            pthread_rwlock_unlock(&g_instance.spq_cxt.adp_connects_lock);
            if (entry->backward.idx != 0) {
                pthread_rwlock_wrlock(&g_instance.spq_cxt.adp_connects_lock);
                hash_search(g_instance.spq_cxt.adp_connects, (void*)(&(entry->key)), HASH_REMOVE, &found);
                pthread_rwlock_unlock(&g_instance.spq_cxt.adp_connects_lock);
            }
        } else {
            int rc = gs_recv(&entry->backward, (char *)&req, sizeof(SpqAdpScanPagesReq));

            if (rc == 0 || errno == ECOMMTCPNODATA) {
                continue;
            }

            if (rc < 0) {
                ereport(ERROR, (errmsg("spq adps thread recv data failed")));
                return;
            }

            res = adps_get_response_block(&req, entry->internal_node_id);

            rc = gs_send(&entry->forward, (char *)&res, sizeof(SpqAdpScanPagesRes), -1, true);
            if (rc <= 0) {
                ereport(ERROR, (errmsg("spq adps thread send data failed")));
                return;
            }
            if (res.success) {
                entry->scannedPageNum += res.page_end - res.page_start + 1;
            }
        }
    }
}

void spq_adps_coordinator_thread_main()
{
    t_thrd.spq_ctx.spq_role = ROLE_QUERY_COORDINTOR;
    ereport(LOG, (errmsg("spq thread started")));

    while (!t_thrd.spq_ctx.qc_ctx->is_done) {
        pthread_mutex_lock(&t_thrd.spq_ctx.qc_ctx->spq_pq_mutex);
        if (t_thrd.spq_ctx.qc_ctx->scanState == NULL) {
            pthread_cond_wait(&t_thrd.spq_ctx.qc_ctx->pq_wait_cv, &t_thrd.spq_ctx.qc_ctx->spq_pq_mutex);
            pthread_mutex_unlock(&t_thrd.spq_ctx.qc_ctx->spq_pq_mutex);
        } else {
            spq_adps_consumer();
            pthread_mutex_unlock(&t_thrd.spq_ctx.qc_ctx->spq_pq_mutex);
        }
    }
    spq_finishQcThread();
    ereport(LOG, (errmsg("spq thread destroyed")));
    t_thrd.spq_ctx.qc_ctx->is_exited = true;
}

spq_qc_ctx* spq_createAdaptiveThread()
{
    if (!u_sess->attr.attr_spq.spq_enable_adaptive_scan) {
        return nullptr;
    }

    spq_qc_ctx* qc_ctx = (spq_qc_ctx*)palloc(sizeof(spq_qc_ctx));
    qc_ctx->scanState = NULL;
    qc_ctx->connects = NULL;
    qc_ctx->is_done = false;
    qc_ctx->is_exited = false;
    qc_ctx->pq_wait_cv = PTHREAD_COND_INITIALIZER;
    if (pthread_mutex_init(&qc_ctx->spq_pq_mutex, NULL)) {
        elog(ERROR, "spq_pq_mutex init failed");
    }

    ThreadId threadId = initialize_util_thread(SPQ_COORDINATOR, (void *)qc_ctx);
    if (threadId == 0) {
        pthread_cond_destroy(&qc_ctx->pq_wait_cv);
        pthread_mutex_destroy(&qc_ctx->spq_pq_mutex);
        ereport(FATAL, (errmsg("Cannot create coordinating thread.")));
    } else {
        ereport(LOG, (errmsg("Create Adaptive thread successfully, threadId:%lu.", threadId)));
    }
    return qc_ctx;
}

void spq_startQcThread(RemoteQueryState *node)
{
    /* If guc flag is closed */
    if (!u_sess->attr.attr_spq.spq_enable_adaptive_scan) {
        return;
    }
    spq_qc_ctx* qc_ctx = node->qc_ctx;
    if (qc_ctx == NULL) {
        return;
    }
    pthread_mutex_lock(&qc_ctx->spq_pq_mutex);

    qc_ctx->num_nodes = node->ss.ps.state->es_plannedstmt->num_nodes;
    qc_ctx->scanState = node;
    qc_ctx->seq_paging_array.size = 0;
    qc_ctx->seq_paging_array.max = PREALLOC_PAGE_ARRAY_SIZE;
    qc_ctx->seq_paging_array.req_states =
        (SpqAdpScanReqState **)palloc(PREALLOC_PAGE_ARRAY_SIZE * sizeof(SpqAdpScanReqState *));
    spq_adps_initconns(node);

    pthread_cond_signal(&qc_ctx->pq_wait_cv);
    pthread_mutex_unlock(&qc_ctx->spq_pq_mutex);
    ereport(DEBUG1, (errmsg("starting adaptive scan thread.")));
}

void spq_finishQcThread(void)
{
    pthread_mutex_lock(&t_thrd.spq_ctx.qc_ctx->spq_pq_mutex);
    t_thrd.spq_ctx.qc_ctx->scanState = nullptr;
    /* free connections */
    if (t_thrd.spq_ctx.qc_ctx->connects != nullptr) {
        ListCell *cell;
        foreach (cell, t_thrd.spq_ctx.qc_ctx->connects) {
            QCConnEntry *entry = (QCConnEntry *)lfirst(cell);
            gs_close_gsocket(&entry->forward);
            gs_close_gsocket(&entry->backward);
            elog(DEBUG1, "adaptive scan end, query_id: %lu, plan_node_id: %u, node_id: %u, scanned page: %d",
                 entry->key.query_id, entry->key.plan_node_id, entry->key.node_id, entry->scannedPageNum);
        }
        list_free(t_thrd.spq_ctx.qc_ctx->connects);
        t_thrd.spq_ctx.qc_ctx->connects = nullptr;
    }
    pthread_mutex_unlock(&t_thrd.spq_ctx.qc_ctx->spq_pq_mutex);
    elog(DEBUG5, "pq_thread: stopping the background adaptive thread");
}

void spq_destroyQcThread(RemoteQueryState* node)
{
    if (!u_sess->attr.attr_spq.spq_enable_adaptive_scan) {
        return;
    }
    spq_qc_ctx* qc_ctx = node->qc_ctx;
    if (qc_ctx == NULL) {
        return;
    }

    pthread_mutex_lock(&qc_ctx->spq_pq_mutex);
    qc_ctx->is_done = true;
    pthread_cond_signal(&qc_ctx->pq_wait_cv);
    pthread_mutex_unlock(&qc_ctx->spq_pq_mutex);

    while (!qc_ctx->is_exited) {
        pthread_mutex_lock(&qc_ctx->spq_pq_mutex);
        pthread_cond_signal(&qc_ctx->pq_wait_cv);
        pthread_mutex_unlock(&qc_ctx->spq_pq_mutex);
        pg_usleep(1);
    }
    pthread_cond_destroy(&qc_ctx->pq_wait_cv);
    pthread_mutex_destroy(&qc_ctx->spq_pq_mutex);
    ereport(DEBUG3, (errmsg("destory adaptive scan thread")));
}

RemoteQueryState* ExecInitSpqRemoteQuery(RemoteQuery* node, EState* estate, int eflags, bool row_plan)
{
    RemoteQueryState* spqRemoteState = NULL;
 
    /* RemoteQuery node is the leaf node in the plan tree, just like seqscan */
    Assert(innerPlan(node) == NULL);
    //Assert(node->is_simple == false);

    spqRemoteState = CreateResponseCombiner(0, node->combine_type);
    spqRemoteState->position = node->position;
    spqRemoteState->qc_ctx = spq_createAdaptiveThread();
 
    ExecInitPlanState(&spqRemoteState->ss.ps, estate, node, spqRemoteState);
 
    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_MARK)));
 
    /* Extract the eflags bits that are relevant for tuplestorestate */
    spqRemoteState->eflags = (eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD));
    /* We anyways have to support REWIND for ReScan */
    spqRemoteState->eflags |= EXEC_FLAG_REWIND;
    spqRemoteState->ss.ps.ExecProcNode = ExecSPQRemoteQuery;
 
    spqRemoteState->eof_underlying = false;
    spqRemoteState->tuplestorestate = NULL;
    spqRemoteState->switch_connection = NULL;
    spqRemoteState->refresh_handles = false;
    spqRemoteState->nodeidxinfo = NULL;
    spqRemoteState->serializedPlan = NULL;
 
    ExecInitScanTupleSlot(estate, &spqRemoteState->ss);
    ExecAssignScanType(&spqRemoteState->ss, ExecTypeFromTL(node->base_tlist, false));
 
    /*
     * If there are parameters supplied, get them into a form to be sent to the
     * Datanodes with bind message. We should not have had done this before.
     */
    SetDataRowForExtParams(estate->es_param_list_info, spqRemoteState);
    //if (false == node->is_simple || true == node->rq_need_proj)
        //ExecAssignScanProjectionInfo(&spqRemoteState->ss);
 
    //if (node->rq_save_command_id) {
        /* Save command id to be used in some special cases */
        //remotestate->rqs_cmd_id = GetCurrentCommandId(false);
    //} 
    // todo  only pgxc_FQS_create_remote_plan, orca may diff
    if (node->is_simple /* || PLAN_ROUTER == node->position */) {
        /* u_sess->instr_cxt.thread_instr in CN do not init nodes which exec on DN */
        ThreadInstrumentation* oldInstr = u_sess->instr_cxt.thread_instr;
        u_sess->instr_cxt.thread_instr = NULL;
 
        u_sess->exec_cxt.under_stream_runtime = true;
        /* For explain command and sessionId generation of import or export execution. */
        if (outerPlan(node))
            outerPlanState(spqRemoteState) = ExecInitNode(outerPlan(node), estate, eflags);
 
        u_sess->instr_cxt.thread_instr = oldInstr;
    }
    /* Add relations ref count for FQS Query. */
    //RelationIncrementReferenceCountForFQS(node);
 
    if (node->is_simple /* || node->poll_multi_channel */) {
        /* receive logic different from pgxc way */
        spqRemoteState->fetchTuple = FetchTupleByMultiChannel<false, false>;
    } else {
        spqRemoteState->fetchTuple = FetchTuple;
    }
 
    if (row_plan) {
        estate->es_remotequerystates = lcons(spqRemoteState, estate->es_remotequerystates);
    }
 
    spqRemoteState->parallel_function_state = NULL;
 
    return spqRemoteState;
}
// copy void InitMultinodeExecutor(bool is_force)
PGXCNodeHandle* InitSPQMultinodeExecutor(Oid nodeoid, char* nodename) 
{
    PGXCNodeHandle *result = (PGXCNodeHandle *)palloc0(sizeof(PGXCNodeHandle));
    result->sock = NO_SOCKET;
    init_pgxc_handle(result);
    result->nodeoid = nodeoid;
    result->remoteNodeName = nodename;
    result->remote_node_type = VDATANODE;
    return result;
}
void spq_release_conn(RemoteQueryState* planstate)
{
    if (planstate == NULL) {
        return;
    }
    for (int i = 0; i < planstate->node_count; i++) {
        if (planstate->nodeCons != NULL && planstate->nodeCons[i] != NULL) {
            PGXCNodeClose(planstate->nodeCons[i]);
            planstate->nodeCons[i] = NULL;
        }
        if (planstate->spq_connections_info != NULL && planstate->spq_connections_info[i] != NULL) {
            PGXCNodeHandle *handle = planstate->spq_connections_info[i];
            pfree_ext(handle->inBuffer);
            pfree_ext(handle->outBuffer);
            pfree_ext(handle->error);
            pfree_ext(planstate->spq_connections_info[i]);
        }
    }
    pfree_ext(planstate->spq_connections_info);
    pfree_ext(planstate->nodeCons);
    planstate->connections = NULL;
    planstate->conn_count = 0;
    u_sess->spq_cxt.remoteQuerys = list_delete_ptr(u_sess->spq_cxt.remoteQuerys, planstate);
}
PGXCNodeHandle** spq_get_exec_connections(
    RemoteQueryState* planstate, ExecNodes* exec_nodes, RemoteQueryExecType exec_type)
{
    int dn_conn_count;
    PlannedStmt* planstmt = planstate->ss.ps.state->es_plannedstmt;

    /* Set datanode list and DN number */
    /* Set Coordinator list and Coordinator number */
    // QD count
    dn_conn_count = planstate->node_count;
    PGXCNodeHandle** connections = (PGXCNodeHandle **)palloc(dn_conn_count * sizeof(PGXCNodeHandle *));
    planstate->spq_connections_info = (PGXCNodeHandle **)palloc(dn_conn_count * sizeof(PGXCNodeHandle *));
    planstate->nodeCons = (PGconn **)palloc0(sizeof(PGconn *) * dn_conn_count);

    Oid *dnNode = (Oid *)palloc0(sizeof(Oid) * dn_conn_count);
    PGconn **nodeCons = planstate->nodeCons;
    char **connectionStrs = (char **)palloc0(sizeof(char *) * dn_conn_count);

    auto spq_release = [&](char* err_msg) {
        for (int i = 0; i < dn_conn_count; i++) {
            pfree_ext(connectionStrs[i]); 
        }
        pfree_ext(dnNode); 
        pfree_ext(connectionStrs);
        if (err_msg != NULL) {
            pfree_ext(connections);
            connections = NULL;
            spq_release_conn(planstate);
            spq_destroyQcThread(planstate);
            ereport(ERROR, (errmsg("PQconnectdbParallel error: %s", err_msg)));
        }
        return;  
    };
    for (int j = 0; j < dn_conn_count; ++j) {
        connectionStrs[j] = (char *)palloc0(INITIAL_EXPBUFFER_SIZE * 4);
        NodeDefinition* node = &planstmt->nodesDefinition[j];
        sprintf_s(connectionStrs[j], INITIAL_EXPBUFFER_SIZE * 4,
        "host=%s port=%d dbname=%s user=%s application_name=coordinator1 connect_timeout=600 rw_timeout=600 \
        options='-c remotetype=coordinator  -c DateStyle=iso,mdy -c timezone=prc -c geqo=on -c intervalstyle=postgres \
        -c lc_monetary=en_US.UTF-8 -c lc_numeric=en_US.UTF-8	-c lc_time=en_US.UTF-8 -c omit_encoding_error=off' \
        prototype=1 keepalives_idle=600 keepalives_interval=30 keepalives_count=20 \
        remote_nodename=%s backend_version=%u enable_ce=1", 
        node->nodehost.data, node->nodeport, u_sess->proc_cxt.MyProcPort->database_name,
        u_sess->proc_cxt.MyProcPort->user_name, node->nodename.data, GRAND_VERSION_NUM);
        dnNode[j] = node->nodeoid;
        connections[j] = InitSPQMultinodeExecutor(node->nodeoid, node->nodename.data);
        planstate->spq_connections_info[j] = connections[j];
        connections[j]->nodeIdx = j;
    }
 
    PQconnectdbParallel(connectionStrs, dn_conn_count, nodeCons, dnNode);

    //ListCell *node_list_item = NULL;
    for (int i = 0; i < dn_conn_count; i++) {
        if (nodeCons[i] && (CONNECTION_OK == nodeCons[i]->status)) {
            pgxc_node_init(connections[i], nodeCons[i]->sock);
        } else {
            char firstError[INITIAL_EXPBUFFER_SIZE] = {0};
            errno_t ss_rc = EOK;
            if (nodeCons[i] ==  NULL) {
                ss_rc = strcpy_s(firstError, INITIAL_EXPBUFFER_SIZE, "out of memory");
            } else if (nodeCons[i]->errorMessage.data != NULL) {
                if (strlen(nodeCons[i]->errorMessage.data) >= INITIAL_EXPBUFFER_SIZE) {
                    nodeCons[i]->errorMessage.data[INITIAL_EXPBUFFER_SIZE - 1] = '\0';
                }
                ss_rc = strcpy_s(firstError, INITIAL_EXPBUFFER_SIZE, nodeCons[i]->errorMessage.data);
            } else {
                ss_rc = strcpy_s(firstError, INITIAL_EXPBUFFER_SIZE, "unknown error");
            }
            spq_release(firstError);
        } 
    }
    spq_release(NULL);
    return connections;
}

void spq_cancel_query(void)
{
    if (u_sess->spq_cxt.remoteQuerys == NULL)
        return;
    char errbuf[256];
    ListCell *cell;
    foreach (cell, u_sess->spq_cxt.remoteQuerys) {
        RemoteQueryState* combiner = (RemoteQueryState *)lfirst(cell);
        for (int i = 0; i < combiner->node_count; ++i) {
            PGconn *pgconn = combiner->nodeCons[i];
            PGcancel *cancel = PQgetCancel(pgconn);
            bool bRet = PQcancel_timeout(cancel, errbuf, sizeof(errbuf), u_sess->attr.attr_network.PoolerCancelTimeout);
            if (!bRet) {
                ereport(LOG, (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                             errmsg("spq cancel connection timeout, nodeid %d: %s", i, errbuf)));
            }
            PQfreeCancel(cancel);
        }
    }
    while (list_length(u_sess->spq_cxt.remoteQuerys) > 0) {
        RemoteQueryState *combiner = (RemoteQueryState *)lfirst(list_head(u_sess->spq_cxt.remoteQuerys));
        spq_destroyQcThread(combiner);
        spq_release_conn(combiner);
    }
}

static void spq_do_direct_read()
{
    if (u_sess->spq_cxt.direct_read_map != NIL) {
        ListCell *cell;
        foreach (cell, u_sess->spq_cxt.direct_read_map) {
            SpqDirectReadEntry *entry = (SpqDirectReadEntry *)lfirst(cell);
            if (entry->nums != InvalidBlockNumber) {
                ListCell *nodecell;
                foreach (nodecell, entry->spq_seq_scan_node_list) {
                    SpqSeqScan *node = (SpqSeqScan *)lfirst(nodecell);
                    node->isDirectRead = true;
                    node->DirectReadBlkNum = entry->nums;
                }
                list_free(entry->spq_seq_scan_node_list);
            }
        }
        list_free(u_sess->spq_cxt.direct_read_map);
        u_sess->spq_cxt.direct_read_map = NIL;
    }
}

void spq_do_query(RemoteQueryState* node)
{
    RemoteQuery* step = (RemoteQuery*)node->ss.ps.plan;
    bool is_read_only = step->read_only;
    bool need_stream_sync = false;
 
    Snapshot snapshot = GetActiveSnapshot();
    PGXCNodeHandle** connections = NULL;
    int i;
    int regular_conn_count = 0;
    bool need_tran_block = false;
    PlannedStmt* planstmt = node->ss.ps.state->es_plannedstmt;
    NameData nodename = {{0}};
 
    /* RecoveryInProgress */
 
    if (node->conn_count == 0)
        node->connections = NULL;
 
    planstmt->queryId = u_sess->debug_query_id;
    planstmt->spq_session_id = u_sess->debug_query_id;
    planstmt->current_id = step->streamID;
    node->queryId = generate_unique_id64(&gt_queryId);
    node->node_count = step->nodeCount;

    connections = spq_get_exec_connections(node, step->exec_nodes, step->exec_type);
    u_sess->spq_cxt.remoteQuerys = lappend(u_sess->spq_cxt.remoteQuerys, node);
 
    Assert(node->spq_connections_info != NULL);
    Assert(connections != NULL);
    Assert(step->exec_type == EXEC_ON_DATANODES);

    regular_conn_count = node->node_count;
    /*
     *	Send begin statement to all datanodes for RW transaction parallel.
     *  Current it should be RO transaction
     */
    pgxc_node_send_queryid_with_sync(connections, regular_conn_count, node->queryId);
    if (u_sess->attr.attr_spq.spq_enable_direct_read) {
        spq_do_direct_read();
    }
    update_spq_port(connections, planstmt, regular_conn_count);
    spq_startQcThread(node);

    pfree_ext(node->switch_connection);
    pfree_ext(node->nodeidxinfo);
    node->switch_connection = (bool*)palloc0(regular_conn_count * sizeof(bool));
    node->nodeidxinfo = (NodeIdxInfo*)palloc0(regular_conn_count * sizeof(NodeIdxInfo));
    for (int k = 0; k < regular_conn_count; k++) {
        node->nodeidxinfo[k].nodeidx = connections[k]->nodeIdx;
        node->nodeidxinfo[k].nodeoid = connections[k]->nodeoid;
    }
 
    Assert(is_read_only);
    if (is_read_only)
        need_tran_block = false;
 
    elog(DEBUG1,
        "regular_conn_count = %d, need_tran_block = %s", regular_conn_count, need_tran_block ? "true" : "false");
 
    // Do not generate gxid for read only query.
    //
    //if (is_read_only) {
    //    gxid = GetCurrentTransactionIdIfAny();
    //}
 
#ifdef STREAMPLAN
    char *compressedPlan = NULL;
    int cLen = 0;
 
    if (step->is_simple) {
 
        StringInfoData str_remoteplan;
        initStringInfo(&str_remoteplan);
        elog(DEBUG5,
            "Node Id %d, Thread ID:%lu, queryId: %lu, query: %s",
            u_sess->pgxc_cxt.PGXCNodeId,
            gs_thread_self(),
            planstmt->queryId,
            t_thrd.postgres_cxt.debug_query_string ? t_thrd.postgres_cxt.debug_query_string : "");
 
        planstmt->query_string =
            const_cast<char*>(t_thrd.postgres_cxt.debug_query_string ? t_thrd.postgres_cxt.debug_query_string : "");
        /* Flag 'Z' to indicate it's serialized plan */
        /* todo: SerializePlan DISTRIBUTED_FEATURE_NOT_SUPPORTED */
        SpqSerializePlan(step->scan.plan.lefttree, planstmt, &str_remoteplan, step, true, node->queryId);
        node->serializedPlan = str_remoteplan.data;
 
		/* Compress the 'Z' plan here. */
		char *tmpQuery = node->serializedPlan;
		/* Skip the msgType 'Z'. */
		tmpQuery++;
        /* todo: CompressSerializedPlan DISTRIBUTED_FEATURE_NOT_SUPPORTED */
		compressedPlan = CompressSerializedPlan(tmpQuery, &cLen);
        u_sess->instr_cxt.plan_size = cLen;
 
		need_stream_sync = step->num_stream > 0 ? true : false;
	}
#endif
    if (node->rqs_num_params) {
        step->sql_statement = node->serializedPlan;
    }
    for (i = 0; i < regular_conn_count; i++) {
        if (!pgxc_start_command_on_connection(connections[i], node, snapshot, compressedPlan, cLen)) {
            Oid nodeid = connections[i]->nodeoid;
            pfree_ext(connections);
            spq_release_conn(node);
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                    errmsg("Failed to send command to  Datanodes %s[%u]",
                        get_pgxc_nodename_noexcept(nodeid, &nodename), nodeid)));
        }
        connections[i]->combiner = node;
 
        // If send command to Datanodes successfully, outEnd must be 0.
        // So the outBuffer can be freed here and reset to default buffer size 16K.
        //
        Assert(connections[i]->outEnd == 0);
        ResetHandleOutBuffer(connections[i]);
    }
 
    do_query_for_first_tuple(node, false, regular_conn_count, connections, NULL, NIL);
 
    /* reset */
    if (step->is_simple) {
        pfree_ext(node->serializedPlan);
        pfree_ext(compressedPlan);
    }
}
static TupleTableSlot* SpqRemoteQueryNext(ScanState* scan_node)
{
    TupleTableSlot* scanslot = scan_node->ss_ScanTupleSlot;
    RemoteQueryState* node = (RemoteQueryState*)scan_node;
 
    /*
     * Initialize tuples processed to 0, to make sure we don't re-use the
     * values from the earlier iteration of RemoteQueryNext().
     */
    node->rqs_processed = 0;
    if (!node->query_Done) {
        spq_do_query(node);
        node->query_Done = true;
    }
 
    //Assert(rq->spool_no_data == true);
    //if (rq->spool_no_data == true) {
        /* for simple remotequery, we just pass the data, no need to spool */
       
    //}
    node->fetchTuple(node, scanslot, NULL);
 
    /* When finish remote query already, should better reset the flag. */
    if (TupIsNull(scanslot))
        node->need_error_check = false;
 
    /* report error if any */
    pgxc_node_report_error(node);
 
    return scanslot;
}
static void ReleaseTupStore(RemoteQueryState* node)
{
    if (node->tuplestorestate != NULL) {
        tuplestore_end(node->tuplestorestate);
    }
}
static void CloseNodeCursors(RemoteQueryState* node, PGXCNodeHandle** cur_handles, int nCount)
{
    if (node->cursor) {
        close_node_cursors(cur_handles, nCount, node->cursor);
        /*
         * node->cursor now points to the string array attached in hash table of Portals.
         * it can't be freed here.
         */
        node->cursor = NULL;
    }
 
    if (node->update_cursor) {
        close_node_cursors(cur_handles, nCount, node->update_cursor);
        /*
         * different from cursor, it can be freed here.
         */
        pfree_ext(node->update_cursor);
    }
}
/* close active cursors during end remote query */
static void CloseActiveCursors(RemoteQueryState* node)
{
    bool noFree = true;
    PGXCNodeAllHandles* all_handles = NULL;
 
    if (node->cursor || node->update_cursor) {
        PGXCNodeHandle** cur_handles = node->cursor_connections;
        int nCount = node->cursor_count;
        int i;
        for (i = 0; i < nCount; i++) {
            if (node->cursor_connections == NULL || (!IS_VALID_CONNECTION(node->cursor_connections[i]))) {
                all_handles = get_exec_connections(node, NULL, EXEC_ON_DATANODES);
                noFree = false;
                break;
            }
        }
 
        if (all_handles != NULL) {
            cur_handles = all_handles->datanode_handles;
            nCount = all_handles->dn_conn_count;
        }
 
        /* not clear ?? */
        CloseNodeCursors(node, cur_handles, nCount);
 
        if (!noFree) {
            pfree_pgxc_all_handles(all_handles);
        }
    }
}
 
void RelationDecrementReferenceCountForFQS(const RemoteQuery* node)
{
    if (!node->isFQS || node->relationOids == NULL) {
        return;
    }
 
    ListCell *lc = NULL;
    foreach(lc, node->relationOids) {
        Oid oid = lfirst_oid(lc);
        RelationDecrementReferenceCount(oid);
    }
}
 
void ExecEndSpqRemoteQuery(RemoteQueryState* node, bool pre_end)
{
    RemoteQuery* remote_query = (RemoteQuery*)node->ss.ps.plan;
 
    if (pre_end == false) {
        RowStoreReset(node->row_store);
    }

    spq_destroyQcThread(node);

    /* Pack all un-completed connections together and recorrect node->conn_count */
    if (node->conn_count > 0 && remote_query->sort != NULL) {
        node->conn_count = PackConnections(node);
    }
 
    node->current_conn = 0;
    while (node->current_conn < node->conn_count) {
        int res;
        PGXCNodeHandle* conn = node->connections[node->current_conn];
 
        /* throw away message */
        pfree_ext(node->currentRow.msg);
 
        if (conn == NULL) {
            node->conn_count--;
            if (node->current_conn < node->conn_count) {
                node->connections[node->current_conn] = node->connections[node->conn_count];
            }
            continue;
        }
 
        /* no data is expected */
        if (conn->state == DN_CONNECTION_STATE_IDLE || conn->state == DN_CONNECTION_STATE_ERROR_FATAL) {
            if (node->current_conn < --node->conn_count) {
                node->connections[node->current_conn] = node->connections[node->conn_count];
            }
            continue;
        }
 
        /* incomlete messages */
        res = handle_response(conn, node);
        if (res == RESPONSE_EOF) {
            node->current_conn++;
        }
    }
 
    /*
     * Send stop signal to DNs when we already get the tuples
     * we need but the DNs are still running.
     * Especially for query with limit or likewise.
     */
    if (node->conn_count > 0) {
        if (u_sess->debug_query_id == 0) {
            /*
             * when cn send stop signal to dn,
             * need to check queryid preventing signal wrong query.
             * so if queryid is 0, get query from RemoteQuery Node.
             */
            u_sess->debug_query_id = GetSPQQueryidFromRemoteQuery(node);
        }
        stop_query();
    }
 
    while (node->conn_count > 0) {
        int i = 0;
        if (pgxc_node_receive(node->conn_count, node->connections, NULL))
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                    errmsg("Failed to read response from Datanodes when ending query")));
 
        while (i < node->conn_count) {
            /* throw away message */
            pfree_ext(node->currentRow.msg);
            int result = handle_response(node->connections[i], node);
            switch (result) {
                case RESPONSE_EOF: /* have something to read, keep receiving */
                    i++;
                    break;
                default:
                    if (node->connections[i]->state == DN_CONNECTION_STATE_IDLE ||
                        node->connections[i]->state == DN_CONNECTION_STATE_ERROR_FATAL) {
                        node->conn_count--;
                        if (i < node->conn_count)
                            node->connections[i] = node->connections[node->conn_count];
                    }
                    break;
            }
        }
    }
 
    /* pre_end true for explain performance receiving data before print plan. */
    if (pre_end) {
        return;
    }
    if (node->tuplestorestate != NULL) {
        ExecClearTuple(node->ss.ss_ScanTupleSlot);
    }
    /* Release tuplestore resources */
    ReleaseTupStore(node);
    /* If there are active cursors close them */
    CloseActiveCursors(node);
    /* Clean up parameters if they were set */
    if (node->paramval_data) {
        pfree_ext(node->paramval_data);
        node->paramval_data = NULL;
        node->paramval_len = 0;
    }
 
    /* Free the param types if they are newly allocated */
    if (node->rqs_param_types && node->rqs_param_types != ((RemoteQuery*)node->ss.ps.plan)->rq_param_types) {
        pfree_ext(node->rqs_param_types);
        node->rqs_param_types = NULL;
        node->rqs_num_params = 0;
    }
 
    if (node->ss.ss_currentRelation)
        ExecCloseScanRelation(node->ss.ss_currentRelation);
 
    if (node->parallel_function_state != NULL) {
        FreeParallelFunctionState(node->parallel_function_state);
        node->parallel_function_state = NULL;
    }
 
 
#ifdef STREAMPLAN
    PlanState* outer_planstate = outerPlanState(node);
#endif
    CloseCombiner(node);
    node = NULL;
 
#ifdef STREAMPLAN
    if ((IS_PGXC_COORDINATOR && remote_query->is_simple) ||
        (IS_PGXC_DATANODE && remote_query->is_simple && remote_query->rte_ref) ||
        IS_SPQ_COORDINATOR ||
        (IS_PGXC_DATANODE && remote_query->is_simple &&
        (remote_query->position == PLAN_ROUTER || remote_query->position == SCAN_GATHER)))
        ExecEndNode(outer_planstate);
#endif
 
    /* Add relations's ref count for FQS Query. */
    RelationDecrementReferenceCountForFQS(remote_query);
 
    /*
     * Free nodelist if there is en_expr:
     * If there is en_expr, nodelist is useless now, for it is generated by en_expr in get_exec_connnection.
     * Else, nodelist contains datanodes generated during planning, which is keeped in the plansource
     * and should not be set NIL.
     */
    ExecNodes* exec_nodes = remote_query->exec_nodes;
    if (exec_nodes != NULL && exec_nodes->en_expr && exec_nodes->nodelist_is_nil) {
        exec_nodes->primarynodelist = NIL;
        exec_nodes->nodeList = NIL;
    }
 
}
#else
int getStreamSocketError(const char* str)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

char* getSocketError(int* err_code)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}
#endif

/*
 * @Description: Check if need check the other error message(s) when receiving
 * 				some error caused by closing socket normally in Stream thread.
 *
 * @param[IN] error_code:  error code
 * @return: bool, true if need check the other error message(s)
 */
static bool IsErrorNeedCheck(int error_code)
{
    if (error_code == ERRCODE_SCTP_REMOTE_CLOSE || error_code == ERRCODE_STREAM_REMOTE_CLOSE_SOCKET ||
        error_code == ERRCODE_STREAM_CONNECTION_RESET_BY_PEER || error_code == ERRCODE_RU_STOP_QUERY ||
        error_code == ERRCODE_QUERY_INTERNAL_CANCEL)
        return true;

    return false;
}

/*
 * Create a structure to store parameters needed to combine responses from
 * multiple connections as well as state information
 */
RemoteQueryState* CreateResponseCombiner(int node_count, CombineType combine_type)
{
    RemoteQueryState* combiner = NULL;

    /* ResponseComber is a typedef for pointer to ResponseCombinerData */
    combiner = makeNode(RemoteQueryState);
    combiner->node_count = node_count;
    combiner->connections = NULL;
    combiner->conn_count = 0;
    combiner->combine_type = combine_type;
    combiner->command_complete_count = 0;
    combiner->request_type = REQUEST_TYPE_NOT_DEFINED;
    combiner->tuple_desc = NULL;
    combiner->description_count = 0;
    combiner->copy_in_count = 0;
    combiner->copy_out_count = 0;
    combiner->errorCode = 0;
    combiner->errorMessage = NULL;
    combiner->errorDetail = NULL;
    combiner->errorContext = NULL;
    combiner->hint = NULL;
    combiner->query = NULL;
    combiner->cursorpos = 0;
    combiner->query_Done = false;
    combiner->is_fatal_error = false;
    combiner->currentRow.msg = NULL;
    combiner->currentRow.msglen = 0;
    combiner->currentRow.msgnode = 0;
    combiner->row_store = RowStoreAlloc(CurrentMemoryContext, ROW_STORE_MAX_MEM,
                                        t_thrd.utils_cxt.CurrentResourceOwner);
    combiner->maxCSN = InvalidCommitSeqNo;
    combiner->hadrMainStandby = false;
    combiner->tapenodes = NULL;
    combiner->remoteCopyType = REMOTE_COPY_NONE;
    combiner->copy_file = NULL;
    combiner->rqs_cmd_id = FirstCommandId;
    combiner->rqs_processed = 0;
    combiner->rqs_cur_processed = 0;
    combiner->need_error_check = false;
    combiner->valid_command_complete_count = 0;
    combiner->pbe_run_status = PBE_NONE;

    return combiner;
}

RemoteQueryState* CreateResponseCombinerForBarrier(int nodeCount, CombineType combineType)
{
    return CreateResponseCombiner(nodeCount, combineType);
}

/*
 * Parse out row count from the command status response and convert it to integer
 */
static int parse_row_count(const char* message, size_t len, uint64* rowcount)
{
    int digits = 0;
    size_t pos;

    *rowcount = 0;
    /* skip \0 string terminator */
    for (pos = 0; pos < len - 1; pos++) {
        if (message[pos] >= '0' && message[pos] <= '9') {
            int num = message[pos] - '0';
            if (*rowcount <= (PG_UINT64_MAX - num) / 10) {
                *rowcount = *rowcount * 10 + num;
                digits++;
            } else {
                ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("message is out of range")));
            }
        } else {
            *rowcount = 0;
            digits = 0;
        }
    }
    return digits;
}

/*
 * Convert RowDescription message to a TupleDesc
 */
static TupleDesc create_tuple_desc(char* msg_body)
{
    TupleDesc result;
    int i, nattr;
    uint16 n16;

    /* get number of attributes */
    errno_t rc = 0;
    rc = memcpy_s(&n16, sizeof(uint16), msg_body, sizeof(uint16));
    securec_check(rc, "\0", "\0");
    nattr = ntohs(n16);
    msg_body += 2;

    result = CreateTemplateTupleDesc(nattr, false);

    /* decode attributes */
    for (i = 1; i <= nattr; i++) {
        AttrNumber attnum;
        char* attname = NULL;
        char* typname = NULL;
        Oid oidtypeid;
        int32 typemode, typmod, oidtypeidint;

        attnum = (AttrNumber)i;

        /* attribute name */
        attname = msg_body;
        msg_body += strlen(attname) + 1;

        /* table OID, ignored */
        msg_body += 4;

        /* column no, ignored */
        msg_body += 2;

        /* data type OID, ignored */
        rc = memcpy_s(&oidtypeidint, sizeof(int32), msg_body, sizeof(int32));
        securec_check(rc, "\0", "\0");
        oidtypeid = ntohl(oidtypeidint);
        msg_body += 4;

        /* type len, ignored */
        msg_body += 2;

        /* type mod */
        rc = memcpy_s(&typemode, sizeof(int32), msg_body, sizeof(int32));
        securec_check(rc, "\0", "\0");
        typmod = ntohl(typemode);
        msg_body += 4;

        /* Get the OID type and mode type from typename */
        if (oidtypeid >= FirstBootstrapObjectId) {
            /* type name */
            typname = msg_body;
            msg_body += strlen(typname) + 1;

            oidtypeid = get_typeoid_with_namespace(typname);
        } else
            typname = "";

        msg_body += 2;

        TupleDescInitEntry(result, attnum, attname, oidtypeid, typmod, 0);
    }

    return result;
}

/*
 * Handle CopyOutCommandComplete ('c') message from a Datanode connection
 */
static void HandleCopyOutComplete(RemoteQueryState* combiner)
{
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_COPY_OUT;
    if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
        /* Inconsistent responses */
        ereport(ERROR,
            (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("Unexpected response from the Datanodes for 'c' message, current request type %d",
                    combiner->request_type)));
    /* Just do nothing, close message is managed by the Coordinator */
    combiner->copy_out_count++;
}

/*
 * Handle CommandComplete ('C') message from a Datanode connection
 */
static void HandleCommandComplete(
    RemoteQueryState* combiner, const char* msg_body, size_t len, PGXCNodeHandle* conn, bool isdummy)
{
    int digits = 0;
    bool non_fqs_dml = false;

    /* Is this a DML query that is not FQSed ? */
    non_fqs_dml = (combiner->ss.ps.plan && ((RemoteQuery*)combiner->ss.ps.plan)->rq_params_internal);
    /*
     * If we did not receive description we are having rowcount or OK response
     */
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_COMMAND;
    /* Extract rowcount */
    if (combiner->combine_type != COMBINE_TYPE_NONE) {
        uint64 rowcount;
        digits = parse_row_count(msg_body, len, &rowcount);
        if (digits > 0) {
            /*
             * Need to completely remove the dependency on whether
             * it's an FQS or non-FQS DML query in future. For this, command_complete_count
             * needs to be better handled. Currently this field is being updated
             * for each iteration of FetchTuple by re-using the same combiner
             * for each iteration, whereas it seems it should be updated only
             * for each node execution, not for each tuple fetched.
             */

            /* Replicated write, make sure they are the same */
            if (combiner->combine_type == COMBINE_TYPE_SAME) {
                if (combiner->valid_command_complete_count) {
                    /* For FQS, check if there is a consistency issue with replicated table. */
                    if (rowcount != combiner->rqs_processed && !isdummy && !non_fqs_dml) {
                        ereport(ERROR,
                            (errcode(ERRCODE_DATA_CORRUPTED),
                                errmsg("Write to replicated table returned "
                                       "different results from the Datanodes on current DN:%s and previous DN:%s.",
                                    conn->remoteNodeName,
                                    combiner->previousNodeName)));
                    }
                }
                /* Always update the row count. We have initialized it to 0 */
                if (!isdummy)
                    combiner->rqs_processed = rowcount;
            } else {
                combiner->rqs_processed += rowcount;
                combiner->rqs_cur_processed = rowcount;
            }
            combiner->previousNodeName = conn->remoteNodeName;

            /*
             * This rowcount will be used to increment estate->es_processed
             * either in ExecInsert/Update/Delete for non-FQS query, or will
             * used in RemoteQueryNext() for FQS query.
             */
        } else {
            combiner->combine_type = COMBINE_TYPE_NONE;
        }
    }

    /* If response checking is enable only then do further processing */

    if (conn->ck_resp_rollback == RESP_ROLLBACK_CHECK) {
        conn->ck_resp_rollback = RESP_ROLLBACK_NOT_RECEIVED;
        if (len == ROLLBACK_RESP_LEN) { /* No need to do string comparison otherwise */
            if (strcmp(msg_body, "ROLLBACK") == 0)
                conn->ck_resp_rollback = RESP_ROLLBACK_RECEIVED;
        }
    }

    if (!isdummy)
        combiner->valid_command_complete_count++;
    combiner->command_complete_count++;
}

/*
 * Handle RowDescription ('T') message from a Datanode connection
 */
static bool HandleRowDescription(RemoteQueryState* combiner, char* msg_body)
{
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_QUERY;
    if (combiner->request_type != REQUEST_TYPE_QUERY) {
        /* Inconsistent responses */
        ereport(ERROR,
            (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("Unexpected response from the Datanodes for 'T' message, current request type %d",
                    combiner->request_type)));
    }
    /* Increment counter and check if it was first */
    if (combiner->description_count++ == 0) {
        combiner->tuple_desc = create_tuple_desc(msg_body);
        return true;
    }
    return false;
}

#ifdef NOT_USED
/*
 * Handle ParameterStatus ('S') message from a Datanode connection (SET command)
 */
static void HandleParameterStatus(RemoteQueryState* combiner, char* msg_body, size_t len)
{
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_QUERY;
    if (combiner->request_type != REQUEST_TYPE_QUERY) {
        /* Inconsistent responses */
        ereport(ERROR,
            (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("Unexpected response from the Datanodes for 'S' message, current request type %d",
                    combiner->request_type)));
    }
    /* Proxy last */
    if (++combiner->description_count == combiner->node_count) {
        pq_putmessage('S', msg_body, len);
    }
}
#endif

/*
 * Handle CopyInResponse ('G') message from a Datanode connection
 */
static void HandleCopyIn(RemoteQueryState* combiner)
{
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_COPY_IN;
    if (combiner->request_type != REQUEST_TYPE_COPY_IN) {
        /* Inconsistent responses */
        ereport(ERROR,
            (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("Unexpected response from the Datanodes for 'G' message, current request type %d",
                    combiner->request_type)));
    }
    /*
     * The normal PG code will output an G message when it runs in the
     * Coordinator, so do not proxy message here, just count it.
     */
    combiner->copy_in_count++;
}

/*
 * Handle CopyOutResponse ('H') message from a Datanode connection
 */
static void HandleCopyOut(RemoteQueryState* combiner)
{
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_COPY_OUT;
    if (combiner->request_type != REQUEST_TYPE_COPY_OUT) {
        /* Inconsistent responses */
        ereport(ERROR,
            (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("Unexpected response from the Datanodes for 'H' message, current request type %d",
                    combiner->request_type)));
    }
    /*
     * The normal PG code will output an H message when it runs in the
     * Coordinator, so do not proxy message here, just count it.
     */
    combiner->copy_out_count++;
}

/*
 * Handle CopyOutDataRow ('d') message from a Datanode connection
 */
static void HandleCopyDataRow(RemoteQueryState* combiner, char* msg_body, size_t len)
{
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        combiner->request_type = REQUEST_TYPE_COPY_OUT;

    /* Inconsistent responses */
    if (combiner->request_type != REQUEST_TYPE_COPY_OUT)
        ereport(ERROR,
            (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("Unexpected response from the Datanodes for 'd' message, current request type %d",
                    combiner->request_type)));

    /* count the row */
    combiner->processed++;

    /* Output remote COPY operation to correct location */
    switch (combiner->remoteCopyType) {
        case REMOTE_COPY_FILE: {
            /* Write data directly to file */

            /*
             * This is supposed to be a temperary patch for our file_encoding/client_encoding issue for
             * COPY and FDWs with gsmpp_server. Dedicated "minimal modification" was asked to be
             * done here, so this chunk of code only fix COPY TO FILE (but not the others) in a very
             * superficial way.
             */
            char* transcoding_for_file = NULL;
            if (u_sess->cmd_cxt.need_transcoding_for_copytofile &&
                WillTranscodingBePerformed(u_sess->cmd_cxt.dest_encoding_for_copytofile)) {
                transcoding_for_file = pg_server_to_any(msg_body, len, u_sess->cmd_cxt.dest_encoding_for_copytofile);
                Assert(transcoding_for_file != msg_body);
                fwrite(transcoding_for_file, 1, strlen(transcoding_for_file), combiner->copy_file);
                pfree_ext(transcoding_for_file);
            } else
                fwrite(msg_body, 1, len, combiner->copy_file);
            break;
        }
        case REMOTE_COPY_STDOUT:
            /* Send back data to client */
            pq_putmessage('d', msg_body, len);
            break;
        case REMOTE_COPY_TUPLESTORE: {
            Datum* values = NULL;
            bool* nulls = NULL;
            TupleDesc tupdesc = combiner->tuple_desc;
            int i, dropped;
            FormData_pg_attribute* attr = tupdesc->attrs;
            FmgrInfo* in_functions = NULL;
            Oid* typioparams = NULL;
            char** fields = NULL;

            values = (Datum*)palloc(tupdesc->natts * sizeof(Datum));
            nulls = (bool*)palloc(tupdesc->natts * sizeof(bool));
            in_functions = (FmgrInfo*)palloc(tupdesc->natts * sizeof(FmgrInfo));
            typioparams = (Oid*)palloc(tupdesc->natts * sizeof(Oid));

            /* Calculate the Oids of input functions */
            for (i = 0; i < tupdesc->natts; i++) {
                Oid in_func_oid;

                /* Do not need any information for dropped attributes */
                if (attr[i].attisdropped)
                    continue;

                getTypeInputInfo(attr[i].atttypid, &in_func_oid, &typioparams[i]);
                fmgr_info(in_func_oid, &in_functions[i]);
            }

            /*
             * Convert message into an array of fields.
             * Last \n is not included in converted message.
             */
            fields = CopyOps_RawDataToArrayField(tupdesc, msg_body, len - 1);

            /* Fill in the array values */
            dropped = 0;
            for (i = 0; i < tupdesc->natts; i++) {
                char* string = fields[i - dropped];
                /* Do not need any information for dropped attributes */
                if (attr[i].attisdropped) {
                    dropped++;
                    nulls[i] = true; /* Consider dropped parameter as NULL */
                    continue;
                }

                /* Find value */
                values[i] = InputFunctionCall(&in_functions[i], string, typioparams[i], attr[i].atttypmod);
                /* Setup value with NULL flag if necessary */
                if (string == NULL)
                    nulls[i] = true;
                else
                    nulls[i] = false;
            }

            /* Then insert the values into tuplestore */
            tuplestore_putvalues(combiner->tuplestorestate, combiner->tuple_desc, values, nulls);

            /* Clean up everything */
            if (*fields)
                pfree_ext(*fields);
            pfree_ext(fields);
            pfree_ext(values);
            pfree_ext(nulls);
            pfree_ext(in_functions);
            pfree_ext(typioparams);
        } break;
        case REMOTE_COPY_NONE:
        default:
            Assert(0); /* Should not happen */
            break;
    }
}

/*
 * Handle DataRow ('D') message from a Datanode connection
 * The function returns true if buffer can accept more data rows.
 * Caller must stop reading if function returns false
 */
static void HandleDataRow(
    RemoteQueryState* combiner, char* msg_body, size_t len, Oid nodeoid, const char* remoteNodeName)
{
    /* We expect previous message is consumed */
    Assert(combiner->currentRow.msg == NULL);

    if (combiner->request_type != REQUEST_TYPE_QUERY) {
        /* Inconsistent responses */
        ereport(ERROR,
            (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg("Unexpected response from the Datanodes for 'D' message, current request type %d",
                    combiner->request_type)));
    }

    /*
     * If we got an error already ignore incoming data rows from other nodes
     * Still we want to continue reading until get CommandComplete
     */
    if (combiner->errorMessage)
        return;

    /* Check messages from DN. */
    if (IS_SPQ_COORDINATOR || IS_PGXC_COORDINATOR) {
#ifdef USE_ASSERT_CHECKING
        if (IS_SPQ_COORDINATOR || strcmp(remoteNodeName, g_instance.attr.attr_common.PGXCNodeName) != 0) {
            CheckMessages(0, 0, msg_body, len, false);
            msg_body += REMOTE_CHECKMSG_LEN;
            len -= REMOTE_CHECKMSG_LEN;
        }
#else

        if (unlikely(anls_opt_is_on(ANLS_STREAM_DATA_CHECK) &&
                     (IS_SPQ_COORDINATOR || strcmp(remoteNodeName, g_instance.attr.attr_common.PGXCNodeName) != 0))) {
            CheckMessages(0, 0, msg_body, len, false);
            msg_body += REMOTE_CHECKMSG_LEN;
            len -= REMOTE_CHECKMSG_LEN;
        }
#endif
    }

    /*
     * We are copying message because it points into connection buffer, and
     * will be overwritten on next socket read
     */
    combiner->currentRow.msg = (char*)palloc(len);
    errno_t rc = 0;
    rc = memcpy_s(combiner->currentRow.msg, len, msg_body, len);
    securec_check(rc, "\0", "\0");
    combiner->currentRow.msglen = len;
    combiner->currentRow.msgnode = nodeoid;
}

/*
 * Handle ErrorResponse ('E') message from a Datanode connection
 */
static void HandleError(RemoteQueryState* combiner, char* msg_body, size_t len)
{
    /* parse error message */
    char* code = NULL;
    char* message = NULL;
    char* detail = NULL;
    char* context = NULL;
    size_t offset = 0;
    char* realerrcode = NULL;
    char* funcname = NULL;
    char* filename = NULL;
    char* lineno = NULL;
    int error_code = 0;
    char* mod_id = NULL;
    char* hint = NULL;
    char* query = NULL;
    char* cursorpos = NULL;

    /*
     * Scan until point to terminating \0
     */
    while (offset + 1 < len) {
        /* pointer to the field message */
        char* str = msg_body + offset + 1;

        switch (msg_body[offset]) {
            case 'c':
                realerrcode = str;
                break;
            case 'C': /* code */
                code = str;

                /* Error Code is exactly 5 significant bytes */
                if (code != NULL)
                    error_code = MAKE_SQLSTATE((unsigned char)code[0],
                        (unsigned char)code[1],
                        (unsigned char)code[2],
                        (unsigned char)code[3],
                        (unsigned char)code[4]);
                break;
            case 'M': /* message */
                message = str;
                break;
            case 'D': /* details */
                detail = str;
                break;
            case 'd': /* mod_id */
                mod_id = str;
                break;
            case 'W': /* where */
                context = str;
                break;
            case 'S': /* severity */
                if (pg_strncasecmp(str, "FATAL", 5) == 0)
                    combiner->is_fatal_error = true;
                break;
            case 'R': /* routine */
                funcname = str;
                break;
            case 'F': /* file */
                filename = str;
                break;
            case 'L': /* line */
                lineno = str;
                break;

            case 'H': /* hint */
                hint = str;
                break;

            case 'q': /* int query */
                query = str;
                break;

            case 'p': /* position int */
                cursorpos = str;
                break;

            /* Fields not yet in use */
            case 'P': /* position string */
            default:
                break;
        }

        /* code, message and \0 */
        offset += strlen(str) + 2;
    }

    if (!IsErrorNeedCheck(error_code)) {
        combiner->need_error_check = false;

        /*
         * If the error comes after some communication error(s), we should free the
         * former cached one.
         */
        combiner->errorCode = 0;

        if (combiner->errorMessage) {
            pfree_ext(combiner->errorMessage);
            combiner->errorMessage = NULL;
        }

        if (combiner->errorDetail) {
            pfree_ext(combiner->errorDetail);
            combiner->errorDetail = NULL;
        }

        if (combiner->errorContext) {
            pfree_ext(combiner->errorContext);
            combiner->errorContext = NULL;
        }

        if (combiner->hint) {
            pfree_ext(combiner->hint);
            combiner->hint = NULL;
        }

        if (combiner->query) {
            pfree_ext(combiner->query);
            combiner->query = NULL;
        }

        combiner->cursorpos = 0;
        combiner->remoteErrData.internalerrcode = 0;
        combiner->remoteErrData.lineno = 0;

        if (combiner->remoteErrData.filename) {
            pfree_ext(combiner->remoteErrData.filename);
            combiner->remoteErrData.filename = NULL;
        }

        if (combiner->remoteErrData.errorfuncname) {
            pfree_ext(combiner->remoteErrData.errorfuncname);
            combiner->remoteErrData.errorfuncname = NULL;
        }
    } else {
        if (!combiner->need_error_check) {
            /*
             * If it's the first time meeting a communication error which may be caused by
             * normal connection close, cache it in Combiner(RemoteQueryState) and set
             * flag 'need_error_check' to true. The other errors comes from Datanodes need
             * to be check to figure out the real one.
             */
            combiner->need_error_check = true;
        } else {
            /* If still getting a communication error, just increment the counter and return. */
            combiner->command_complete_count++;
            return;
        }
    }

    /*
     * We may have special handling for some errors, default handling is to
     * throw out error with the same message. We can not ereport immediately
     * because we should read from this and other connections until
     * ReadyForQuery is received, so we just store the error message.
     * If multiple connections return errors only first one is reported.
     */
    if (combiner->errorMessage == NULL) {
        if (message != NULL) {
            combiner->errorMessage = pstrdup(message);

            if (code != NULL)
                combiner->errorCode = error_code;

            if (realerrcode != NULL)
                combiner->remoteErrData.internalerrcode = pg_strtoint32(realerrcode);
        }

        if (hint != NULL)
            combiner->hint = pstrdup(hint);
        else
            combiner->hint = NULL;

        if (query != NULL)
            combiner->query = pstrdup(query);
        else
            combiner->query = NULL;

        if (detail != NULL)
            combiner->errorDetail = pstrdup(detail);
        else
            combiner->errorDetail = NULL;

        if (context != NULL)
            combiner->errorContext = pstrdup(context);
        else
            combiner->errorContext = NULL;

        if (filename != NULL)
            combiner->remoteErrData.filename = pstrdup(filename);
        else
            combiner->remoteErrData.filename = NULL;

        if (funcname != NULL)
            combiner->remoteErrData.errorfuncname = pstrdup(funcname);
        else
            combiner->remoteErrData.errorfuncname = NULL;

        if (lineno != NULL)
            combiner->remoteErrData.lineno = pg_strtoint32(lineno);
        else
            combiner->remoteErrData.lineno = 0;

        if (cursorpos != NULL)
            combiner->cursorpos = pg_strtoint32(cursorpos);
        else
            combiner->cursorpos = 0;

        if (mod_id != NULL)
            combiner->remoteErrData.mod_id = get_module_id(mod_id);
    }

    /*
     * If Datanode have sent ErrorResponse it will never send CommandComplete.
     * Increment the counter to prevent endless waiting for it.
     */
    combiner->command_complete_count++;
}

/*
 * Handle NoticeResponse ('N') message from a Datanode connection
 */
static void HandleNotice(RemoteQueryState* combiner, char* msg_body, size_t len)
{
    /* parse error message */
    char* message = NULL;
    char* detail = NULL;
    char* hint = NULL;
    size_t offset = 0;
    int elevel = NOTICE;

    /*
     * Scan until point to terminating \0
     */
    while (offset + 1 < len) {
        /* pointer to the field message */
        char* str = msg_body + offset + 1;

        switch (msg_body[offset]) {
            case 'M': /* message */
                message = str;
                break;
            case 'D': /* details */
                detail = str;
                break;

            /* Fields not yet in use */
            case 'S': /* severity */
                if (pg_strncasecmp(str, "WARNING", strlen("WARNING")) == 0) {
                    elevel = WARNING;
                }
                break;

            case 'H': /* hint */
                hint = str;
                break;

            case 'C': /* code */
            case 'R': /* routine */
            case 'P': /* position string */
            case 'p': /* position int */
            case 'q': /* int query */
            case 'W': /* where */
            case 'F': /* file */
            case 'L': /* line */
            default:
                break;
        }

        /* code, message and \0 */
        offset += strlen(str) + 2;
    }

    if (message != NULL) {
        if ((detail != NULL) && (hint != NULL))
            ereport(
                elevel, (errmsg("%s", message), errdetail("%s", detail), errhint("%s", hint), handle_in_client(true)));
        else if (detail != NULL)
            ereport(elevel, (errmsg("%s", message), errdetail("%s", detail), handle_in_client(true)));
        else if (hint != NULL)
            ereport(elevel, (errmsg("%s", message), errhint("%s", hint), handle_in_client(true)));
        else
            ereport(elevel, (errmsg("%s", message), handle_in_client(true)));
    }
}

void HandleCmdComplete(CmdType commandType, CombineTag* combine, const char* msg_body, size_t len)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    int digits = 0;
    uint64 originrowcount = 0;
    uint64 rowcount = 0;
    uint64 total = 0;
    errno_t rc = EOK;

    if (msg_body == NULL)
        return;

    /* if there's nothing in combine, just copy the msg_body */
    if (strlen(combine->data) == 0) {
        errno_t ret = strcpy_s(combine->data, COMPLETION_TAG_BUFSIZE, msg_body);
        securec_check(ret, "\0", "\0");
        combine->cmdType = commandType;
        return;
    } else {
        /* commandType is conflict */
        if (combine->cmdType != commandType)
            return;

        /* get the processed row number from msg_body */
        digits = parse_row_count(msg_body, len + 1, &rowcount);
        elog(DEBUG1, "digits is %d\n", digits);
        Assert(digits >= 0);

        /* no need to combine */
        if (digits == 0)
            return;

        /* combine the processed row number */
        parse_row_count(combine->data, strlen(combine->data) + 1, &originrowcount);
        elog(DEBUG1, "originrowcount is %lu, rowcount is %lu\n", originrowcount, rowcount);
        total = originrowcount + rowcount;
    }

    /* output command completion tag */
    switch (commandType) {
        case CMD_SELECT:
            rc = strcpy_s(combine->data, COMPLETION_TAG_BUFSIZE, "SELECT");
            securec_check(rc, "", "");
            break;
        case CMD_INSERT:
            rc = snprintf_s(
                combine->data, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "INSERT %u %lu", 0, total);
            securec_check_ss(rc, "", "");
            break;
        case CMD_UPDATE:
            rc = snprintf_s(combine->data, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "UPDATE %lu", total);
            securec_check_ss(rc, "", "");
            break;
        case CMD_DELETE:
            rc = snprintf_s(combine->data, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "DELETE %lu", total);
            securec_check_ss(rc, "", "");
            break;
        default:
            rc = strcpy_s(combine->data, COMPLETION_TAG_BUFSIZE, "");
            securec_check(rc, "", "");
            break;
    }
#endif
}

/*
 * HandleDatanodeCommandId ('M') message from a Datanode connection
 */
static void HandleDatanodeCommandId(RemoteQueryState* combiner, const char* msg_body, size_t len)
{
    uint32 n32;
    CommandId cid;

    Assert(msg_body != NULL);
    Assert(len >= 2);

    /* Get the command Id */
    errno_t rc = 0;
    rc = memcpy_s(&n32, sizeof(uint32), &msg_body[0], sizeof(uint32));
    securec_check(rc, "\0", "\0");
    cid = ntohl(n32);

    /* If received command Id is higher than current one, set it to a new value */
    if (cid > GetReceivedCommandId())
        SetReceivedCommandId(cid);
}

/*
 * Description: message type is 'P' identify received total row count from a datanode under analyzing.
 *
 * Parameters:
 *	@in combiner: Connection info for a remote node.
 *	@in msg_body: Represents a TotalRowCount message received from a remote node.
 *	@in len: The message length.
 * Returns: void
 */
static void HandleAnalyzeTotalRow(RemoteQueryState* combiner, const char* msg_body, size_t len)
{
    StringInfoData buf;

    Assert(msg_body != NULL);

    initStringInfo(&buf);
    appendBinaryStringInfo(&buf, &msg_body[0], len);

    if (2 * sizeof(int64) == len) {
        combiner->analyze_totalrowcnt[ANALYZENORMAL] = pq_getmsgint64(&buf);
        combiner->analyze_memsize[ANALYZENORMAL] = pq_getmsgint64(&buf);
    } else {
        Assert(len == sizeof(int64) * 3);
        combiner->analyze_totalrowcnt[ANALYZEDELTA - 1] = pq_getmsgint64(&buf);
        combiner->analyze_memsize[ANALYZEDELTA - 1] = 0;
    }

    pfree_ext(buf.data);
}

/*
 * Examine the specified combiner state and determine if command was completed
 * successfully
 */
bool validate_combiner(RemoteQueryState* combiner)
{
    /* There was error message while combining */
    if (combiner->errorMessage)
        return false;
    /* Check if state is defined */
    if (combiner->request_type == REQUEST_TYPE_NOT_DEFINED)
        return false;

    /* Check all nodes completed */
    if ((combiner->request_type == REQUEST_TYPE_COMMAND || combiner->request_type == REQUEST_TYPE_QUERY) &&
        combiner->command_complete_count != combiner->node_count)
        return false;

    /* Check count of description responses */
    if (combiner->request_type == REQUEST_TYPE_QUERY && combiner->description_count != combiner->node_count)
        return false;

    /* Check count of copy-in responses */
    if (combiner->request_type == REQUEST_TYPE_COPY_IN && combiner->copy_in_count != combiner->node_count)
        return false;

    /* Check count of copy-out responses */
    if (combiner->request_type == REQUEST_TYPE_COPY_OUT && combiner->copy_out_count != combiner->node_count)
        return false;

    /* Add other checks here as needed */

    /* All is good if we are here */
    return true;
}

/*
 * Close combiner and free allocated memory, if it is not needed
 */
void CloseCombiner(RemoteQueryState* combiner)
{
    if (combiner != NULL) {
#ifdef USE_SPQ
        spq_release_conn(combiner);
#endif
        if (combiner->connections)
            pfree_ext(combiner->connections);
        if (combiner->tuple_desc) {
            /*
             * In the case of a remote COPY with tuplestore, combiner is not
             * responsible from freeing the tuple store. This is done at an upper
             * level once data redistribution is completed.
             */
            if (combiner->remoteCopyType != REMOTE_COPY_TUPLESTORE)
                FreeTupleDesc(combiner->tuple_desc);
        }
        if (combiner->errorMessage)
            pfree_ext(combiner->errorMessage);
        if (combiner->errorDetail)
            pfree_ext(combiner->errorDetail);
        if (combiner->cursor_connections)
            pfree_ext(combiner->cursor_connections);
        if (combiner->tapenodes)
            pfree_ext(combiner->tapenodes);
        if (combiner->errorContext)
            pfree_ext(combiner->errorContext);
        if (combiner->switch_connection)
            pfree_ext(combiner->switch_connection);
        if (combiner->nodeidxinfo)
            pfree_ext(combiner->nodeidxinfo);
        if (combiner->row_store) {
            RowStoreDestory(combiner->row_store);
        }
        pfree_ext(combiner);
    }
}

void CloseCombinerForBarrier(RemoteQueryState* combiner)
{
    CloseCombiner(combiner);
}

/*
 * Validate combiner and release storage freeing allocated memory
 */
bool ValidateAndCloseCombiner(RemoteQueryState* combiner)
{
    bool valid = validate_combiner(combiner);

    CloseCombiner(combiner);

    return valid;
}

/*
 * @Description: get actual connection idx from combiner;
 * @ in conn -- current connection info
 * @return - connection idx
 */
static int GetConnIdx(PGXCNodeHandle* conn)
{
    int idx = 0;
    RemoteQueryState* combiner = conn->combiner;
    for (int j = 0; j < combiner->node_count; j++) {
        if (conn->nodeoid == combiner->nodeidxinfo[j].nodeoid) {
            idx = combiner->nodeidxinfo[j].nodeidx;
            break;
        }
    }

    return idx;
}

void BufferConnection(PGXCNodeHandle* conn, bool cachedata)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    RemoteQueryState* combiner = conn->combiner;
    MemoryContext oldcontext;

    if (combiner == NULL || conn->state != DN_CONNECTION_STATE_QUERY)
        return;

    /*
     * When BufferConnection is invoked CurrentContext is related to other
     * portal, which is trying to control the connection.
     */
    oldcontext = MemoryContextSwitchTo(combiner->ss.ss_ScanTupleSlot->tts_mcxt);

    /* Verify the connection is in use by the combiner */
    combiner->current_conn = 0;
    while (combiner->current_conn < combiner->conn_count) {
        if (combiner->connections[combiner->current_conn] == conn)
            break;
        combiner->current_conn++;
    }
    Assert(combiner->current_conn < combiner->conn_count);

    /*
     * Buffer data rows until Datanode return number of rows specified by the
     * fetch_size parameter of last Execute message (PortalSuspended message)
     * or end of result set is reached (CommandComplete message)
     */
    while (conn->state == DN_CONNECTION_STATE_QUERY) {
        int res;

        /* Move to buffer currentRow (received from the Datanode) */
        if (combiner->currentRow.msg) {
            RemoteDataRow dataRow = (RemoteDataRow)palloc(sizeof(RemoteDataRowData));
            *dataRow = combiner->currentRow;
            combiner->currentRow.msg = NULL;
            combiner->currentRow.msglen = 0;
            combiner->currentRow.msgnode = 0;
            combiner->rowBuffer = lappend(combiner->rowBuffer, dataRow);
        }

            res = handle_response(conn, combiner);
            /*
             * If response message is a DataRow it will be handled on the next
             * iteration.
             * PortalSuspended will cause connection state change and break the loop
             * The same is for CommandComplete, but we need additional handling -
             * remove connection from the list of active connections.
             * We may need to add handling error response
             */
            if (res == RESPONSE_EOF) {
                /* incomplete message, read more */
                if (pgxc_node_receive(1, &conn, NULL)) {
                    conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
                    add_error_message(conn, "%s", "Failed to fetch from Datanode");
                }
            } else if (res == RESPONSE_COMPLETE) {
                /* Remove current connection, move last in-place, adjust current_conn */
                if (combiner->current_conn < --combiner->conn_count)
                    combiner->connections[combiner->current_conn] = combiner->connections[combiner->conn_count];
                else
                    combiner->current_conn = 0;
            }
            /*
             * Before output RESPONSE_COMPLETE or PORTAL_SUSPENDED handle_response()
             * changes connection state to DN_CONNECTION_STATE_IDLE, breaking the
             * loop. We do not need to do anything specific in case of
             * PORTAL_SUSPENDED so skiping "else if" block for that case
             */
        }
        MemoryContextSwitchTo(oldcontext);
        conn->combiner = NULL;
#endif
}

void CopyDataRowToBatch(RemoteQueryState* node, VectorBatch* batch)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

/*
 * copy the datarow from combiner to the given slot, in the slot's memory
 * context
 */
void CopyDataRowTupleToSlot(RemoteQueryState* combiner, TupleTableSlot* slot)
{
    char* msg = NULL;
    MemoryContext oldcontext;
    oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);
    msg = (char*)palloc(combiner->currentRow.msglen);
    errno_t rc = 0;
    rc = memcpy_s(msg, combiner->currentRow.msglen, combiner->currentRow.msg, combiner->currentRow.msglen);
    securec_check(rc, "\0", "\0");
    if (IS_SPQ_RUNNING) {
        ExecStoreMinimalTuple((MinimalTuple)msg, slot, true);
    } else {
        ExecStoreDataRowTuple(msg, combiner->currentRow.msglen, combiner->currentRow.msgnode, slot, true);
    }
    pfree_ext(combiner->currentRow.msg);
    combiner->currentRow.msg = NULL;
    combiner->currentRow.msglen = 0;
    combiner->currentRow.msgnode = 0;
    MemoryContextSwitchTo(oldcontext);
}

bool FetchTuple(RemoteQueryState* combiner, TupleTableSlot* slot, ParallelFunctionState* parallelfunctionstate)
{
#ifdef USE_SPQ
    return SpqFetchTuple(combiner, slot, parallelfunctionstate);
#endif

#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
#else
        bool have_tuple = false;

        /* If we have message in the buffer, consume it */
        if (combiner->currentRow.msg) {
            CopyDataRowTupleToSlot(combiner, slot);
            have_tuple = true;
        }

        /*
         * Note: If we are fetching not sorted results we can not have both
         * currentRow and buffered rows. When connection is buffered currentRow
         * is moved to buffer, and then it is cleaned after buffering is
         * completed. Afterwards rows will be taken from the buffer bypassing
         * currentRow until buffer is empty, and only after that data are read
         * from a connection.
         * PGXCTODO: the message should be allocated in the same memory context as
         * that of the slot. Are we sure of that in the call to
         * ExecStoreDataRowTuple below? If one fixes this memory issue, please
         * consider using CopyDataRowTupleToSlot() for the same.
         */
        if (list_length(combiner->rowBuffer) > 0) {
            RemoteDataRow dataRow = (RemoteDataRow)linitial(combiner->rowBuffer);
            combiner->rowBuffer = list_delete_first(combiner->rowBuffer);
            ExecStoreDataRowTuple(dataRow->msg, dataRow->msglen, dataRow->msgnode, slot, true);
            pfree(dataRow);
            return true;
        }

        while (combiner->conn_count > 0) {
            int res;
            PGXCNodeHandle* conn = combiner->connections[combiner->current_conn];

            /* Going to use a connection, buffer it if needed */
            if (conn->state == DN_CONNECTION_STATE_QUERY && conn->combiner != NULL && conn->combiner != combiner)
                BufferConnection(conn);

            /*
             * If current connection is idle it means portal on the Datanode is
             * suspended. If we have a tuple do not hurry to request more rows,
             * leave connection clean for other RemoteQueries.
             * If we do not have, request more and try to get it
             */
            if (conn->state == DN_CONNECTION_STATE_IDLE) {
                /*
                 * If we have tuple to return do not hurry to request more, keep
                 * connection clean
                 */
                if (have_tuple)
                    return true;
                else {
                    if (pgxc_node_send_execute(conn, combiner->cursor, 1) != 0)
                        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to fetch from Datanode")));
                    if (pgxc_node_send_sync(conn) != 0)
                        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to fetch from Datanode")));
                    if (pgxc_node_receive(1, &conn, NULL))
                        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to fetch from Datanode")));
                    conn->combiner = combiner;
                }
            }

            /* read messages */
            res = handle_response(conn, combiner);
            if (res == RESPONSE_EOF) {
                /* incomplete message, read more */
                if (pgxc_node_receive(1, &conn, NULL))
                    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to fetch from Datanode")));
                continue;
            } else if (res == RESPONSE_SUSPENDED) {
                /* Make next connection current */
                if (++combiner->current_conn >= combiner->conn_count)
                    combiner->current_conn = 0;
            } else if (res == RESPONSE_COMPLETE) {
                /* Remove current connection, move last in-place, adjust current_conn */
                if (combiner->current_conn < --combiner->conn_count)
                    combiner->connections[combiner->current_conn] = combiner->connections[combiner->conn_count];
                else
                    combiner->current_conn = 0;
            } else if (res == RESPONSE_DATAROW && have_tuple) {
                /*
                 * We already have a tuple and received another one, leave it till
                 * next fetch
                 */
                return true;
            }

            /* If we have message in the buffer, consume it */
            if (combiner->currentRow.msg) {
                CopyDataRowTupleToSlot(combiner, slot);
                have_tuple = true;
            }
        }

        /* report end of data to the caller */
        if (!have_tuple)
            (void)ExecClearTuple(slot);

        return have_tuple;
#endif
}

/*
 * Get next data row from the combiner's buffer into provided slot
 * Just clear slot and return false if buffer is empty, that means end of result
 * set is reached
 */

template <bool BatchFormat>
static bool GetTupleFromConn(RemoteQueryState* node, void* slot, ParallelFunctionState* parallelfunctionstate)
{
    int connIdx = 0;
    PGXCNodeHandle** connections = NULL;

    connIdx = node->current_conn;
    connections = node->connections;

    // handle data from all connection.
    while (connIdx < node->conn_count) {
        int res = handle_response(connections[connIdx], node);
        Assert(connections[connIdx]->combiner == NULL || connections[connIdx]->combiner == node);
        switch (res) {
            case RESPONSE_EOF:  // try next run.
            {
                connIdx++;
            } break;
            case RESPONSE_COMPLETE:  // finish one connection.
            {
                node->conn_count = node->conn_count - 1;

                // all finished
                if (node->conn_count == 0) {
                    if (BatchFormat == false)
                        (void)ExecClearTuple((TupleTableSlot*)slot);

                    node->need_more_data = false;
                    return false;
                }

                if (connIdx < node->conn_count) {
                    connections[connIdx] =
                        connections[node->conn_count];  // shrink for one size as one connection has finished
                }
            } break;
            case RESPONSE_TUPDESC: {
                /* when used in ParallelFunction we should get tupledesc by ourself. */
                if (parallelfunctionstate != NULL) {
                    ExecSetSlotDescriptor((TupleTableSlot*)slot, node->tuple_desc);
                    parallelfunctionstate->tupdesc = node->tuple_desc;
                } else {
                    ereport(ERROR,
                        (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Unexpected TUPDESC response from Datanode")));
                }
            } break;
            case RESPONSE_DATAROW: {
                /* If we have message in the buffer, consume it */
                if (node->currentRow.msg != 0) {
                    if (BatchFormat)
                        CopyDataRowToBatch(node, (VectorBatch*)slot);
                    else
                        CopyDataRowTupleToSlot(node, (TupleTableSlot*)slot);
                    node->need_more_data = false;
                    node->current_conn = connIdx;  // remember the current connection index.
                }

                if (parallelfunctionstate != NULL) {
                    if (parallelfunctionstate->tupstore != NULL && !TupIsNull((TupleTableSlot*)slot)) {
                        /*
                         * Store the tuple received from each node into the tuplestore in
                         * ParallelFunction and we won't return for each tuple recivied here.
                         */
                        tuplestore_puttupleslot(parallelfunctionstate->tupstore, (TupleTableSlot*)slot);
                        (void)ExecClearTuple((TupleTableSlot*)slot);
                    }
                } else {
                    return true;
                }
            } break;
            default:
                ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Unexpected response from Datanode")));
                break;
        }
    }

    Assert(connIdx == node->conn_count);
    node->current_conn = 0;
    node->need_more_data = true;
    return false;
}

/*
 * Get next data row from the combiner's buffer into provided slot
 * Just clear slot and return false if buffer is empty, that means end of result
 * set is reached
 */
template <bool BatchFormat, bool ForParallelFunction>
bool FetchTupleByMultiChannel(
    RemoteQueryState* combiner, TupleTableSlot* slot, ParallelFunctionState* parallelfunctionstate)
{
    if (!ForParallelFunction) {
        /* If we have message in the buffer, consume it */
        if (combiner->currentRow.msg) {
            if (BatchFormat)
                CopyDataRowToBatch(combiner, (VectorBatch*)slot);
            else
                CopyDataRowTupleToSlot(combiner, slot);

            combiner->current_conn = 0;
            return true;
        }

        if (((RemoteQuery*)combiner->ss.ps.plan) != NULL && ((RemoteQuery*)combiner->ss.ps.plan)->sort)
            return false;

        if (RowStoreLen(combiner->row_store) > 0) {
            RemoteDataRowData dataRow;

            RowStoreFetch(combiner->row_store, &dataRow);

            if (BatchFormat) {
                ((VectorBatch*)slot)->DeserializeWithLZ4Decompress(dataRow.msg, dataRow.msglen);
            } else {
                NetWorkTimeDeserializeStart(t_thrd.pgxc_cxt.GlobalNetInstr);
                ExecStoreDataRowTuple(dataRow.msg, dataRow.msglen, dataRow.msgnode, slot, true);
                NetWorkTimeDeserializeEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
            }

            return true;
        }
    }

    while (combiner->conn_count > 0) {
        if (combiner->need_more_data) {
            struct timeval timeout;
            timeout.tv_sec = ERROR_CHECK_TIMEOUT;
            timeout.tv_usec = 0;

            /*
             * If need check the other errors after getting a normal communcation error,
             * set timeout first when coming to receive data again. If then get any poll
             * error, report the former cached error in combiner(RemoteQueryState).
             */
            if (pgxc_node_receive(
                    combiner->conn_count, combiner->connections, combiner->need_error_check ? &timeout : NULL)) {
                if (!combiner->need_error_check) {
                    int error_code;
                    char* error_msg = getSocketError(&error_code);

                    ereport(ERROR,
                        (errcode(error_code),
                            errmsg("Failed to read response from Datanodes Detail: %s\n", error_msg)));
                } else {
                    combiner->need_error_check = false;
                    pgxc_node_report_error(combiner);
                }
            }
        }
        if (!ForParallelFunction) {
            if (GetTupleFromConn<BatchFormat>(combiner, slot, NULL))
                return true;
            else {
                if (combiner->need_more_data == false) {
                    if (BatchFormat == false)
                        (void)ExecClearTuple(slot);
                    return false;
                }
            }
        } else {
            /* don't need check function's return value as it will be always false in ParallelFunction. */
            (void)GetTupleFromConn<false>(combiner, slot, parallelfunctionstate);

            /* report error if any. */
            pgxc_node_report_error(combiner);

            if (combiner->need_more_data == false)
                return true;
        }
    }

    if (BatchFormat == false && !ForParallelFunction)
        (void)ExecClearTuple(slot);

    return false;
}

bool FetchBatch(RemoteQueryState* combiner, VectorBatch* batch)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

/*
 * Handle responses from the Datanode connections
 */
int pgxc_node_receive_responses(const int conn_count, PGXCNodeHandle** connections, struct timeval* timeout,
    RemoteQueryState* combiner, bool checkerror)
{
    int count = conn_count;
    errno_t rc = EOK;
    PGXCNodeHandle** to_receive = NULL;

    if (conn_count > 0) {
        to_receive = (PGXCNodeHandle**)palloc(conn_count * sizeof(PGXCNodeHandle*));

        /* make a copy of the pointers to the connections */
        rc = memcpy_s(
            to_receive, conn_count * sizeof(PGXCNodeHandle*), connections, conn_count * sizeof(PGXCNodeHandle*));
        securec_check(rc, "", "");
    }

    /*
     * Read results.
     * Note we try and read from Datanode connections even if there is an error on one,
     * so as to avoid reading incorrect results on the next statement.
     * Other safegaurds exist to avoid this, however.
     */
    while (count > 0) {
        int i = 0;

        if (pgxc_node_receive(count, to_receive, timeout)) {
            pfree_ext(to_receive);
            return EOF;
        }
        while (i < count) {
            int result = handle_response(to_receive[i], combiner);
            switch (result) {
                case RESPONSE_EOF: /* have something to read, keep receiving */
                    i++;
                    break;
                case RESPONSE_COMPLETE:
                case RESPONSE_COPY:
                    /* Handling is done, do not track this connection */
                    count--;
                    /* Move last connection in place */
                    if (i < count)
                        to_receive[i] = to_receive[count];
                    break;
                default:
                    /* Inconsistent responses */
                    add_error_message(to_receive[i], "%s", "Unexpected response from the Datanodes");
                    ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
                            errmsg("Unexpected response from the Datanodes, result = %d, request type %d",
                                result, combiner->request_type)));
                    /* Stop tracking and move last connection in place */
                    count--;
                    if (i < count)
                        to_receive[i] = to_receive[count];
                    break;
            }
        }
    }
    /*
     *  For pgxc_node_remote_prepare, pgxc_node_remote_commit and pgxc_node_remote_abort
     *  we don't report error here, because we have not set remoteXactState.status by now.
     */
    if (checkerror)
        pgxc_node_report_error(combiner);

    if (conn_count > 0)
        pfree_ext(to_receive);
    return 0;
}

void light_node_report_error(lightProxyErrData* combiner)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

/*
 * For Light proxy: like HandleError
 */
void light_handle_error(lightProxyErrData* combiner, char* msg_body, size_t len)
{
    /* parse error message */
    char* code = NULL;
    char* message = NULL;
    char* detail = NULL;
    char* context = NULL;
    size_t offset = 0;
    char* realerrcode = NULL;
    char* funcname = NULL;
    char* filename = NULL;
    char* lineno = NULL;
    int error_code = 0;

    /*
     * Scan until point to terminating \0
     */
    while (offset + 1 < len) {
        /* pointer to the field message */
        char* str = msg_body + offset + 1;

        switch (msg_body[offset]) {
            case 'c':
                realerrcode = str;
                break;
            case 'C': /* code */
                code = str;
                /* Error Code is exactly 5 significant bytes */
                if (code != NULL)
                    error_code = MAKE_SQLSTATE((unsigned char)code[0],
                        (unsigned char)code[1],
                        (unsigned char)code[2],
                        (unsigned char)code[3],
                        (unsigned char)code[4]);
                break;
            case 'M': /* message */
                message = str;
                break;
            case 'D': /* details */
                detail = str;
                break;
            case 'W': /* where */
                context = str;
                break;
            case 'S': /* severity */
                if (pg_strncasecmp(str, "FATAL", 5) == 0)
                    combiner->is_fatal_error = true;
                break;
            case 'R': /* routine */
                funcname = str;
                break;
            case 'F': /* file */
                filename = str;
                break;
            case 'L': /* line */
                lineno = str;
                break;
            /* Fields not yet in use */
            case 'H': /* hint */
            case 'P': /* position string */
            case 'p': /* position int */
            case 'q': /* int query */
            default:
                break;
        }
        /* code, message and \0 */
        offset += strlen(str) + 2;
    }

    if (message != NULL) {
        combiner->errorMessage = pstrdup(message);

        if (code != NULL)
            combiner->errorCode = error_code;

        if (realerrcode != NULL)
            combiner->remoteErrData.internalerrcode = pg_strtoint32(realerrcode);
    }

    if (detail != NULL)
        combiner->errorDetail = pstrdup(detail);

    if (context != NULL)
        combiner->errorContext = pstrdup(context);

    if (filename != NULL)
        combiner->remoteErrData.filename = pstrdup(filename);

    if (funcname != NULL)
        combiner->remoteErrData.errorfuncname = pstrdup(funcname);

    if (lineno != NULL)
        combiner->remoteErrData.lineno = pg_strtoint32(lineno);
}

int light_handle_response(PGXCNodeHandle* conn, lightProxyMsgCtl* msgctl, lightProxy* lp)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

int handle_response(PGXCNodeHandle* conn, RemoteQueryState* combiner, bool isdummy)
{
#ifdef USE_SPQ
    return spq_handle_response(conn, combiner, isdummy);
#endif

#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
#else
        char* msg;
        int msg_len;
        char msg_type;
        bool suspended = false;

        for (;;) {
            Assert(conn->state != DN_CONNECTION_STATE_IDLE);

            /*
             * If we are in the process of shutting down, we
             * may be rolling back, and the buffer may contain other messages.
             * We want to avoid a procarray exception
             * as well as an error stack overflow.
             */
            if (proc_exit_inprogress)
                conn->state = DN_CONNECTION_STATE_ERROR_FATAL;

            /* don't read from from the connection if there is a fatal error */
            if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL)
                return RESPONSE_COMPLETE;

            /* No data available, exit */
            if (!HAS_MESSAGE_BUFFERED(conn))
                return RESPONSE_EOF;

            Assert(conn->combiner == combiner || conn->combiner == NULL);

            msg_type = get_message(conn, &msg_len, &msg);
            switch (msg_type) {
                case '\0': /* Not enough data in the buffer */
                    return RESPONSE_EOF;
                case 'c': /* CopyToCommandComplete */
                    HandleCopyOutComplete(combiner);
                    break;
                case 'C': /* CommandComplete */
                    HandleCommandComplete(combiner, msg, msg_len, conn);
                    break;
                case 'T': /* RowDescription */
#ifdef DN_CONNECTION_DEBUG
                    Assert(!conn->have_row_desc);
                    conn->have_row_desc = true;
#endif
                    if (HandleRowDescription(combiner, msg))
                        return RESPONSE_TUPDESC;
                    break;
                case 'D': /* DataRow */
#ifdef DN_CONNECTION_DEBUG
                    Assert(conn->have_row_desc);
#endif
                    HandleDataRow(combiner, msg, msg_len, conn->nodeoid);
                    return RESPONSE_DATAROW;
                case 's': /* PortalSuspended */
                    suspended = true;
                    break;
                case '1': /* ParseComplete */
                case '2': /* BindComplete */
                case '3': /* CloseComplete */
                case 'n': /* NoData */
                    /* simple notifications, continue reading */
                    break;
                case 'G': /* CopyInResponse */
                    conn->state = DN_CONNECTION_STATE_COPY_IN;
                    HandleCopyIn(combiner);
                    /* Done, return to caller to let it know the data can be passed in */
                    return RESPONSE_COPY;
                case 'H': /* CopyOutResponse */
                    conn->state = DN_CONNECTION_STATE_COPY_OUT;
                    HandleCopyOut(combiner);
                    return RESPONSE_COPY;
                case 'd': /* CopyOutDataRow */
                    conn->state = DN_CONNECTION_STATE_COPY_OUT;
                    HandleCopyDataRow(combiner, msg, msg_len);
                    break;
                case 'E': /* ErrorResponse */
                    HandleError(combiner, msg, msg_len);
                    add_error_message(conn, "%s", combiner->errorMessage);
                    /*
                     * Do not return with an error, we still need to consume Z,
                     * ready-for-query
                     */
                    break;
                case 'A': /* NotificationResponse */
                case 'N': /* NoticeResponse */
                case 'S': /* SetCommandComplete */
                    /*
                     * Ignore these to prevent multiple messages, one from each
                     * node. Coordinator will send one for DDL anyway
                     */
                    break;
                case 'Z': /* ReadyForQuery */
                {
                    /*
                     * Return result depends on previous connection state.
                     * If it was PORTAL_SUSPENDED Coordinator want to send down
                     * another EXECUTE to fetch more rows, otherwise it is done
                     * with the connection
                     */
                    int result = suspended ? RESPONSE_SUSPENDED : RESPONSE_COMPLETE;
                    conn->transaction_status = msg[0];
                    conn->state = DN_CONNECTION_STATE_IDLE;
                    conn->combiner = NULL;
#ifdef DN_CONNECTION_DEBUG
                    conn->have_row_desc = false;
#endif
                    return result;
                }
                case 'M': /* Command Id */
                    HandleDatanodeCommandId(combiner, msg, msg_len);
                    break;
                case 'b':
                    conn->state = DN_CONNECTION_STATE_IDLE;
                    return RESPONSE_BARRIER_OK;
                case 'I': /* EmptyQuery */
                default:
                    /* sync lost? */
                    elog(WARNING, "Received unsupported message type: %c", msg_type);
                    conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
                    /* stop reading */
                    return RESPONSE_COMPLETE;
            }
        }
        /* never happen, but keep compiler quiet */
        return RESPONSE_EOF;
#endif
}

/*
 * Has the Datanode sent Ready For Query
 */

bool is_data_node_ready(PGXCNodeHandle* conn)
{
    char* msg = NULL;
    int msg_len;
    char msg_type;

    for (;;) {
        /*
         * If we are in the process of shutting down, we
         * may be rolling back, and the buffer may contain other messages.
         * We want to avoid a procarray exception
         * as well as an error stack overflow.
         */
        if (t_thrd.proc_cxt.proc_exit_inprogress) {
            conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
            ereport(DEBUG2,
                (errmsg("DN_CONNECTION_STATE_ERROR_FATAL1 is set for connection to node %u when proc_exit_inprogress",
                    conn->nodeoid)));
        }

        /* don't read from from the connection if there is a fatal error */
        if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL) {
            ereport(DEBUG2,
                (errmsg("is_data_node_ready returned with DN_CONNECTION_STATE_ERROR_FATAL for connection to node %u",
                    conn->nodeoid)));
            return true;
        }

        /* No data available, exit */
        if (!HAS_MESSAGE_BUFFERED(conn))
            return false;

        msg_type = get_message(conn, &msg_len, &msg);
        switch (msg_type) {
            case 's': /* PortalSuspended */
                break;

            case 'Z': /* ReadyForQuery */
                /*
                 * Return result depends on previous connection state.
                 * If it was PORTAL_SUSPENDED Coordinator want to send down
                 * another EXECUTE to fetch more rows, otherwise it is done
                 * with the connection
                 */
                conn->transaction_status = msg[0];
                conn->state = DN_CONNECTION_STATE_IDLE;
                conn->combiner = NULL;
                return true;
            default:
                break;
        }
    }
    /* never happen, but keep compiler quiet */
    return false;
}

/*
 * Construct a BEGIN TRANSACTION command after taking into account the
 * current options. The returned string is not palloced and is valid only until
 * the next call to the function.
 */
char* generate_begin_command(void)
{
    const char* read_only = NULL;
    const char* isolation_level = NULL;
    int rcs = 0;

    /*
     * First get the READ ONLY status because the next call to GetConfigOption
     * will overwrite the return buffer
     */
    if (strcmp(GetConfigOption("transaction_read_only", false, false), "on") == 0)
        read_only = "READ ONLY";
    else
        read_only = "READ WRITE";

    /* Now get the isolation_level for the transaction */
    isolation_level = GetConfigOption("transaction_isolation", false, false);
    if (strcmp(isolation_level, "default") == 0)
        isolation_level = GetConfigOption("default_transaction_isolation", false, false);

    /* Finally build a START TRANSACTION command */
    rcs = sprintf_s(t_thrd.pgxc_cxt.begin_cmd,
        BEGIN_CMD_BUFF_SIZE,
        "START TRANSACTION ISOLATION LEVEL %s %s",
        isolation_level,
        read_only);
    securec_check_ss(rcs, "\0", "\0");

    return t_thrd.pgxc_cxt.begin_cmd;
}

/*
 * Send BEGIN command to the compute Datanodes.
 */
static int compute_node_begin(int conn_count, PGXCNodeHandle** connections, GlobalTransactionId gxid)
{
    int i;
    TimestampTz gtmstart_timestamp = GetCurrentGTMStartTimestamp();
    TimestampTz stmtsys_timestamp = GetCurrentStmtsysTimestamp();
    /*
     * If no remote connections, we don't have anything to do
     */
    if (conn_count == 0) {
        return 0;
    }

    for (i = 0; i < conn_count; i++) {
        if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
            BufferConnection(connections[i]);

        /*
         * Send GXID and check for errors every time when gxid is valid
         * For subTransaction if datanode has no next_xid for assignning, bogus occurs.
         */
        if (GlobalTransactionIdIsValid(gxid) && pgxc_node_send_gxid(connections[i], gxid, false))
            return EOF;

        /*
         * If the node is already a participant in the transaction, skip it
         */
        if (list_member(u_sess->pgxc_cxt.XactReadNodes, connections[i]) ||
            list_member(u_sess->pgxc_cxt.XactWriteNodes, connections[i])) {
            continue;
        }

        /* Send timestamp and check for errors */
        if (GlobalTimestampIsValid(gtmstart_timestamp) && GlobalTimestampIsValid(stmtsys_timestamp) &&
            pgxc_node_send_timestamp(connections[i], gtmstart_timestamp, stmtsys_timestamp))
            return EOF;
    }

    /* No problem, let's get going */
    return 0;
}

bool light_xactnodes_member(bool write, const void* datum)
{
    if (write)
        return list_member(u_sess->pgxc_cxt.XactWriteNodes, datum);
    else
        return list_member(u_sess->pgxc_cxt.XactReadNodes, datum);
}

/*
 * Send BEGIN command to the Datanodes or Coordinators and receive responses.
 * Also send the GXID for the transaction.
 */
int pgxc_node_begin(int conn_count, PGXCNodeHandle** connections, GlobalTransactionId gxid, bool need_tran_block,
    bool readOnly, char node_type, bool need_send_queryid)
{
    int i;
    struct timeval* timeout = NULL;
    RemoteQueryState* combiner = NULL;
    TimestampTz gtmstart_timestamp = GetCurrentGTMStartTimestamp();
    TimestampTz stmtsys_timestamp = GetCurrentStmtsysTimestamp();
    int new_count = 0;
    int j = 0;

    /*
     * If no remote connections, we don't have anything to do
     */
    if (conn_count == 0) {
        return 0;
    }

    PGXCNodeHandle** new_connections = (PGXCNodeHandle**)palloc(conn_count * sizeof(PGXCNodeHandle*));
    int* con = (int*)palloc(conn_count * sizeof(int));

    for (i = 0; i < conn_count; i++) {
        if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
            BufferConnection(connections[i]);

        if (need_send_queryid) {
            if (pgxc_node_send_queryid(connections[i], u_sess->debug_query_id))
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Failed to send queryid to %s", connections[i]->remoteNodeName)));
        }

        if (connections[i]->state != DN_CONNECTION_STATE_IDLE)
            LIBCOMM_DEBUG_LOG("pgxc_node_begin to node:%s[nid:%d,sid:%d] with abnormal state:%d",
                connections[i]->remoteNodeName,
                connections[i]->gsock.idx,
                connections[i]->gsock.sid,
                connections[i]->state);

        /*

        * Send GXID and check for errors every time when gxid is valid
        * For subTransaction if datanode has no next_xid for assignning, bogus occurs.
        */

        if (GlobalTransactionIdIsValid(gxid) && pgxc_node_send_gxid(connections[i], gxid, false))
            return EOF;

        /*
         * If the node is already a participant in the transaction, skip it
         */
        if (list_member(u_sess->pgxc_cxt.XactReadNodes, connections[i]) ||
            list_member(u_sess->pgxc_cxt.XactWriteNodes, connections[i])) {
            /*
             * If we are doing a write operation, we may need to shift the node
             * to the write-list. RegisterTransactionNodes does that for us
             */
            if (!readOnly)
                RegisterTransactionNodes(1, (void**)&connections[i], true);
            continue;
        }

        /* Send timestamp and check for errors */
        if (GlobalTimestampIsValid(gtmstart_timestamp) && GlobalTimestampIsValid(stmtsys_timestamp) &&
            pgxc_node_send_timestamp(connections[i], gtmstart_timestamp, stmtsys_timestamp))
            return EOF;

        /* Send BEGIN */
        if (need_tran_block) {
            WaitStatePhase oldPhase = pgstat_report_waitstatus_phase(PHASE_BEGIN);
            /* Send the BEGIN TRANSACTION command and check for errors */
            if (pgxc_node_send_query(connections[i],
                    generate_begin_command(),
                    false,
                    false,
                    false,
                    g_instance.attr.attr_storage.enable_gtm_free)) {
                pgstat_report_waitstatus_phase(oldPhase);
                return EOF;
            }
            pgstat_report_waitstatus_phase(oldPhase);

            con[j++] = PGXCNodeGetNodeId(connections[i]->nodeoid, node_type);
            /*

            * Register the node as a participant in the transaction. The
            * caller should tell us if the node may do any write activitiy
            *
            * XXX This is a bit tricky since it would be difficult to know if
            * statement has any side effect on the Datanode. So a SELECT
            * statement may invoke a function on the Datanode which may end up
            * modifying the data at the Datanode. We can possibly rely on the
            * function qualification to decide if a statement is a read-only or a
            * read-write statement.
            */
            RegisterTransactionNodes(1, (void**)&connections[i], !readOnly);
            new_connections[new_count++] = connections[i];
        }
    }

    /* list the read nodes and write nodes */
    PrintRegisteredTransactionNodes();

    /*
     * If we did not send a BEGIN command to any node, we are done. Otherwise,
     * we need to check for any errors and report them
     */
    if (new_count == 0) {
        pfree_ext(new_connections);
        pfree_ext(con);
        return 0;
    }

    combiner = CreateResponseCombiner(new_count, COMBINE_TYPE_NONE);

    /* Receive responses */
    if (pgxc_node_receive_responses(new_count, new_connections, timeout, combiner)) {
        pfree_ext(new_connections);
        pfree_ext(con);
        return EOF;
    }
    /* Verify status */
    if (!ValidateAndCloseCombiner(combiner)) {
        pfree_ext(new_connections);
        pfree_ext(con);
        return EOF;
    }

    /*
     * Ask pooler to send commands (if any) to nodes involved in transaction to alter the
     * behavior of current transaction. This fires all transaction level commands before
     * issuing any DDL, DML or SELECT within the current transaction block.
     */
    if (GetCurrentLocalParamStatus()) {
        int res;
        if (node_type == PGXC_NODE_DATANODE)
            res = PoolManagerSendLocalCommand(j, con, 0, NULL);
        else
            res = PoolManagerSendLocalCommand(0, NULL, j, con);

        if (res != 0) {
            pfree_ext(new_connections);
            pfree_ext(con);
            return EOF;
        }
    }

    /* No problem, let's get going */
    pfree_ext(new_connections);
    pfree_ext(con);
    return 0;
}

/*
 * RemoteXactNodeStatusAsString
 * 		for pgxc prepare comm status info support
 */
static const char* RemoteXactNodeStatusAsString(RemoteXactNodeStatus status)
{
    switch (status) {
        case RXACT_NODE_NONE:
            return "RXACT_NODE_NONE";
        case RXACT_NODE_PREPARE_SENT:
            return "RXACT_NODE_PREPARE_SENT";
        case RXACT_NODE_PREPARE_FAILED:
            return "RXACT_NODE_PREPARE_FAILED";
        case RXACT_NODE_PREPARED:
            return "RXACT_NODE_PREPARED";
        case RXACT_NODE_COMMIT_SENT:
            return "RXACT_NODE_COMMIT_SENT";
        case RXACT_NODE_COMMIT_FAILED:
            return "RXACT_NODE_COMMIT_FAILED";
        case RXACT_NODE_COMMITTED:
            return "RXACT_NODE_COMMITTED";
        case RXACT_NODE_ABORT_SENT:
            return "RXACT_NODE_ABORT_SENT";
        case RXACT_NODE_ABORT_FAILED:
            return "RXACT_NODE_ABORT_FAILED";
        case RXACT_NODE_ABORTED:
            return "RXACT_NODE_ABORTED";
        default:
            break;
    }
    return "UNRECOGNIZED RXACT_NODE_STATUS";
}

/*
 * RemoteXactStatusAsString
 *		for readable error msg support
 */
static const char* RemoteXactStatusAsString(RemoteXactStatus status)
{
    switch (status) {
        case RXACT_NONE:
            return "RXACT_NONE:Initial state";
        case RXACT_PREPARE_FAILED:
            return "RXACT_PREPARE_FAILED:PREPARE failed";
        case RXACT_PREPARED:
            return "RXACT_PREPARED:PREPARED succeeded on all nodes";
        case RXACT_COMMIT_FAILED:
            return "RXACT_COMMIT_FAILED:COMMIT failed on all the nodes";
        case RXACT_PART_COMMITTED:
            return "RXACT_PART_COMMITTED:COMMIT failed on some and succeeded on other nodes";
        case RXACT_COMMITTED:
            return "RXACT_COMMITTED:COMMIT succeeded on all the nodes";
        case RXACT_ABORT_FAILED:
            return "RXACT_ABORT_FAILED:ABORT failed on all the nodes";
        case RXACT_PART_ABORTED:
            return "RXACT_PART_ABORTED:ABORT failed on some and succeeded on other nodes";
        case RXACT_ABORTED:
            return "RXACT_ABORTED:ABORT succeeded on all the nodes";
        default:
            break;
    }
    return "UNRECOGNIZED";
}

/*
 * Prepare all remote nodes involved in this transaction. The local node is
 * handled separately and prepared first in xact.c. If there is any error
 * during this phase, it will be reported via ereport() and the transaction
 * will be aborted on the local as well as remote nodes
 *
 * prepareGID is created and passed from xact.c
 */
bool pgxc_node_remote_prepare(const char* prepareGID, bool WriteCnLocalNode)
{
    int result = 0;
    int write_conn_count = u_sess->pgxc_cxt.remoteXactState->numWriteRemoteNodes;
    char prepare_cmd[256];
    int i;
    PGXCNodeHandle** connections = u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles;
    RemoteQueryState* combiner = NULL;
    errno_t errorno = EOK;

    t_thrd.xact_cxt.XactPrepareSent = false;

    /*
     * If there is NO write activity or the caller does not want us to run a
     * 2PC protocol, we don't need to do anything special
     */
    if ((write_conn_count == 0) || (prepareGID == NULL)) {
        /* Involes only one node, 2pc is not needed, notify GTM firstly when xact end */
        if (!g_instance.attr.attr_storage.enable_gtm_free) {
            /* Notify DN to set csn at commit in progress */
            if (TransactionIdIsValid(GetTopTransactionIdIfAny())) {
                NotifyDNSetCSN2CommitInProgress();
            }

            if (!AtEOXact_GlobalTxn(true)) {
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Failed to receive GTM commit transaction response.")));
            }
        }
        return false;
    }

    t_thrd.pgxact->prepare_xid = GetCurrentTransactionIdIfAny();

    SetSendCommandId(false);

    /* Save the prepareGID in the global state information */
    const int slen = 256;
    errorno = snprintf_s(u_sess->pgxc_cxt.remoteXactState->prepareGID, slen, slen - 1, "%s", prepareGID);
    securec_check_ss(errorno, "\0", "\0");

    /* Generate the PREPARE TRANSACTION command */
    errorno = snprintf_s(
        prepare_cmd, slen, slen - 1, "PREPARE TRANSACTION '%s'", u_sess->pgxc_cxt.remoteXactState->prepareGID);
    securec_check_ss(errorno, "\0", "\0");

    for (i = 0; i < write_conn_count; i++) {
        /*
         * We should actually make sure that the connection state is
         * IDLE when we reach here. The executor should have guaranteed that
         * before the transaction gets to the commit point. For now, consume
         * the pending data on the connection
         */
        if (connections[i]->state != DN_CONNECTION_STATE_IDLE)
            BufferConnection(connections[i]);

        /* Clean the previous errors, if any */
        pfree_ext(connections[i]->error);

#ifdef ENABLE_DISTRIBUTE_TEST
        if (TEST_STUB(CN_PREPARED_SEND_ALL_FAILED, twophase_default_error_emit)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                (errmsg("GTM_TEST  %s: send %s failed, all failed",
                    g_instance.attr.attr_common.PGXCNodeName,
                    prepare_cmd)));
        }

        /* white box test start */
        if (execute_whitebox(WHITEBOX_LOC, u_sess->pgxc_cxt.remoteXactState->prepareGID, WHITEBOX_CORE, 0.1)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                (errmsg("WHITE_BOX TEST  %s: prepare send all to remote failed",
                    g_instance.attr.attr_common.PGXCNodeName)));
        }
        /* white box test end */

        if (i == write_conn_count - 1) {
            if (TEST_STUB(CN_PREPARED_SEND_PART_FAILED, twophase_default_error_emit)) {
                ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("GTM_TEST  %s: send %s failed, part failed",
                        g_instance.attr.attr_common.PGXCNodeName,
                        prepare_cmd)));
            }

            /* white box test start */
            if (execute_whitebox(WHITEBOX_LOC, u_sess->pgxc_cxt.remoteXactState->prepareGID, WHITEBOX_CORE, 0.1)) {
                ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("WHITE_BOX TEST  %s: prepare send part to remote failed",
                        g_instance.attr.attr_common.PGXCNodeName)));
            }
            /* white box test end */
        }
#endif
        /*
         * Send queryid to all the participants
         */
        if (pgxc_node_send_queryid(connections[i], u_sess->debug_query_id))
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                    errmsg("Failed to send queryid to %s before PREPARE command", connections[i]->remoteNodeName)));

        /*
         * Now we are ready to PREPARE the transaction. Any error at this point
         * can be safely ereport-ed and the transaction will be aborted.
         */
        if (pgxc_node_send_query(connections[i], prepare_cmd)) {
            u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_PREPARE_FAILED;
            u_sess->pgxc_cxt.remoteXactState->status = RXACT_PREPARE_FAILED;
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                    errmsg("failed to send PREPARE TRANSACTION command to "
                           "the node %u",
                        connections[i]->nodeoid)));
        } else {
            u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_PREPARE_SENT;
            /* Let the HandleCommandComplete know response checking is enable */
            connections[i]->ck_resp_rollback = RESP_ROLLBACK_CHECK;

            t_thrd.xact_cxt.XactPrepareSent = true;
        }

#ifdef ENABLE_DISTRIBUTE_TEST
        if (TEST_STUB(CN_PREPARED_MESSAGE_REPEAT, twophase_default_error_emit)) {
            if (pgxc_node_send_query(connections[i], prepare_cmd))
                ereport(LOG, (errmsg("Failed to send query in 2pc TEST : CN_PREPARED_MESSAGE_REPEAT.")));
            ereport(g_instance.distribute_test_param_instance->elevel,
                (errmsg("GTM_TEST  %s: repeate message %s", g_instance.attr.attr_common.PGXCNodeName, prepare_cmd)));
        }

        /* white box test start */
        if (execute_whitebox(WHITEBOX_LOC, NULL, WHITEBOX_REPEAT, 0.1)) {
            (void)pgxc_node_send_query(connections[i], prepare_cmd);
        }
        /* white box test end */
#endif
    }

#ifdef ENABLE_DISTRIBUTE_TEST
    if (TEST_STUB(CN_PREPARED_RESPONSE_FAILED, twophase_default_error_emit)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
            (errmsg(
                "GTM_TEST  %s: wait response of %s failed", g_instance.attr.attr_common.PGXCNodeName, prepare_cmd)));
    }

    /* white box test start */
    if (execute_whitebox(WHITEBOX_LOC, NULL, WHITEBOX_CORE, 0.1)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
            (errmsg(
                "WHITE_BOX TEST  %s: wait prepare remote response failed", g_instance.attr.attr_common.PGXCNodeName)));
    }
    /* white box test end */
#endif

    /*
     * Receive and check for any errors. In case of errors, we don't bail out
     * just yet. We first go through the list of connections and look for
     * errors on each connection. This is important to ensure that we run
     * an appropriate ROLLBACK command later on (prepared transactions must be
     * rolled back with ROLLBACK PREPARED commands).
     *
     * There doesn't seem to be a solid mechanism to track errors on
     * individual connections. The transaction_status field doesn't get set
     * every time there is an error on the connection. The combiner mechanism is
     * good for parallel proessing, but I think we should have a leak-proof
     * mechanism to track connection status
     */
    if (write_conn_count) {
        combiner = CreateResponseCombiner(write_conn_count, COMBINE_TYPE_NONE);
        /* Receive responses */
        result = pgxc_node_receive_responses(write_conn_count, connections, NULL, combiner, false);
        if (result || !validate_combiner(combiner))
            result = EOF;
        else {
            CloseCombiner(combiner);
            combiner = NULL;
        }

        for (i = 0; i < write_conn_count; i++) {
            if (u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] == RXACT_NODE_PREPARE_SENT) {
                if (connections[i]->error) {
                    u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_PREPARE_FAILED;
                    u_sess->pgxc_cxt.remoteXactState->status = RXACT_PREPARE_FAILED;
                    ereport(LOG,
                        (errcode(ERRCODE_CONNECTION_EXCEPTION),

                            errmsg("Failed to PREPARE the transaction on node: %u, state: %s, result: %d, %s",
                                connections[i]->nodeoid,
                                RemoteXactNodeStatusAsString(u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i]),
                                result,
                                connections[i]->error)));
                } else {
                    /* Did we receive ROLLBACK in response to PREPARE TRANSCATION? */
                    if (connections[i]->ck_resp_rollback == RESP_ROLLBACK_RECEIVED) {
                        /* If yes, it means PREPARE TRANSACTION failed */
                        u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_PREPARE_FAILED;
                        u_sess->pgxc_cxt.remoteXactState->status = RXACT_PREPARE_FAILED;
                        ereport(LOG,
                            (errcode(ERRCODE_CONNECTION_EXCEPTION),

                                errmsg("Failed to PREPARE the transaction on node: %u, state : %s, result: %d",
                                    connections[i]->nodeoid,
                                    RemoteXactNodeStatusAsString(u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i]),
                                    result)));
                        result = 0;
                    } else {
                        u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_PREPARED;
                    }
                }
            }
        }
    }

    /*
     * If we failed to PREPARE on one or more nodes, report an error and let
     * the normal abort processing take charge of aborting the transaction
     */
    if (result) {
        u_sess->pgxc_cxt.remoteXactState->status = RXACT_PREPARE_FAILED;
        if (combiner != NULL)
            pgxc_node_report_error(combiner);

        elog(LOG, "failed to PREPARE transaction on one or more nodes - result %d", result);
    }

    if (u_sess->pgxc_cxt.remoteXactState->status == RXACT_PREPARE_FAILED)
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Failed to PREPARE the transaction on one or more nodes")));

    /* Everything went OK. */
    u_sess->pgxc_cxt.remoteXactState->status = RXACT_PREPARED;
    TwoPhaseCommit = true;

    /* Set csn to commit in progress on CN if CN don't write. */
    if (!WriteCnLocalNode)
        SetXact2CommitInProgress(GetTopTransactionIdIfAny(), 0);

    if (!g_instance.attr.attr_storage.enable_gtm_free) {
        /*
         * Notify GTM firstly when xact end when LocalCnNode isn't write node
         * If LocalCnNode need write(e.g DDL), notify GTM after local node prepare finish
         */
        if (!WriteCnLocalNode && !AtEOXact_GlobalTxn(true)) {
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                    errmsg("Failed to receive GTM commit transaction response after %s.", prepare_cmd)));
        }
    }

    return result;
}

/*
 * Commit a running or a previously PREPARED transaction on the remote nodes.
 * The local transaction is handled separately in xact.c
 *
 * Once a COMMIT command is sent to any node, the transaction must be finally
 * be committed. But we still report errors via ereport and let
 * AbortTransaction take care of handling partly committed transactions.
 *
 * For 2PC transactions: If local node is involved in the transaction, its
 * already prepared locally and we are in a context of a different transaction
 * (we call it auxulliary transaction) already. So AbortTransaction will
 * actually abort the auxilliary transaction, which is OK. OTOH if the local
 * node is not involved in the main transaction, then we don't care much if its
 * rolled back on the local node as part of abort processing.
 *
 * When 2PC is not used for reasons such as because transaction has accessed
 * some temporary objects, we are already exposed to the risk of committing it
 * one node and aborting on some other node. So such cases need not get more
 * attentions.
 */
void pgxc_node_remote_commit(bool barrierLockHeld)
{
    int result = 0;
    int rc = 0;
    char commitPrepCmd[256];
    char commitCmd[256];
    char errMsg[256];
    int write_conn_count = u_sess->pgxc_cxt.remoteXactState->numWriteRemoteNodes;
    int read_conn_count = u_sess->pgxc_cxt.remoteXactState->numReadRemoteNodes;
    PGXCNodeHandle** connections = u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles;
    PGXCNodeHandle* new_connections[write_conn_count + read_conn_count];
    int new_conn_count = 0;
    int i;
    RemoteQueryState* combiner = NULL;
    int rcs = 0;
    StringInfoData str;

    /*
     * We must handle reader and writer connections both since the transaction
     * must be closed even on a read-only node
     */
    if (read_conn_count + write_conn_count == 0) {
        return;
    }

    SetSendCommandId(false);

    /*
     * Barrier:
     *
     * We should acquire the BarrierLock in SHARE mode here to ensure that
     * there are no in-progress barrier at this point. This mechanism would
     * work as long as LWLock mechanism does not starve a EXCLUSIVE lock
     * requester
     *
     * When local cn is involved in 2PC xacts, we get barrier lock at previous stage,
     * just before cn local commit, to avoid backup inconsistency.
     */
    if (!barrierLockHeld)
        LWLockAcquire(BarrierLock, LW_SHARED);

    /*
     * The readers can be committed with a simple COMMIT command. We still need
     * this to close the transaction block
     */
    rcs = sprintf_s(commitCmd, sizeof(commitCmd), "COMMIT TRANSACTION");
    securec_check_ss(rcs, "\0", "\0");

    /*
     * If we are running 2PC, construct a COMMIT command to commit the prepared
     * transactions
     */
    if (u_sess->pgxc_cxt.remoteXactState->status == RXACT_PREPARED) {
        rcs = sprintf_s(
            commitPrepCmd, sizeof(commitPrepCmd), "COMMIT PREPARED '%s'", u_sess->pgxc_cxt.remoteXactState->prepareGID);
        securec_check_ss(rcs, "\0", "\0");
    }
    /*
     * Now send the COMMIT command to all the participants
     */
    WaitStatePhase oldPhase = pgstat_report_waitstatus_phase(PHASE_COMMIT);
    for (i = 0; i < write_conn_count + read_conn_count; i++) {
        const char* command = NULL;

        Assert(u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] == RXACT_NODE_PREPARED ||
               u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] == RXACT_NODE_NONE);

        if (u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] == RXACT_NODE_PREPARED)
            command = commitPrepCmd;
        else
            command = commitCmd;

        /* Clean the previous errors, if any */
        pfree_ext(connections[i]->error);

#ifdef ENABLE_DISTRIBUTE_TEST
        if (TEST_STUB(CN_COMMIT_PREPARED_SEND_ALL_FAILED, twophase_default_error_emit)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                (errmsg(
                    "GTM_TEST  %s: send %s failed, all failed", g_instance.attr.attr_common.PGXCNodeName, command)));
        }

        /* white box test start */
        if (execute_whitebox(WHITEBOX_LOC, u_sess->pgxc_cxt.remoteXactState->prepareGID, WHITEBOX_CORE, 0.1)) {
            ereport(g_instance.distribute_test_param_instance->elevel,
                (errmsg("WHITE_BOX TEST  %s: send remote commit msg %s all failed",
                    g_instance.attr.attr_common.PGXCNodeName,
                    command)));
        }
        /* white box test end */

        if (i == write_conn_count - 1) {
            if (TEST_STUB(CN_COMMIT_PREPARED_SEND_PART_FAILED, twophase_default_error_emit)) {
                ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("GTM_TEST  %s: send %s failed, part failed",
                        g_instance.attr.attr_common.PGXCNodeName,
                        command)));
            }

            /* white-box test inject start */
            if (execute_whitebox(WHITEBOX_LOC, u_sess->pgxc_cxt.remoteXactState->prepareGID, WHITEBOX_CORE, 0.1)) {
                ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("WHITE_BOX TEST  %s: send remote commit msg %s part failed",
                        g_instance.attr.attr_common.PGXCNodeName,
                        command)));
            }
            /* white-box test inject end */
        }
#endif
        if (pgxc_node_send_queryid(connections[i], u_sess->debug_query_id) != 0) {
            const int dest_max = 256;
            rc = sprintf_s(errMsg,
                dest_max,
                "failed to send queryid "
                "to node %s before COMMIT command(2PC)",
                connections[i]->remoteNodeName);
            securec_check_ss(rc, "\0", "\0");
            add_error_message(connections[i], "%s", errMsg);
        }

        if (pgxc_node_send_query(connections[i], command)) {
            u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_COMMIT_FAILED;
            u_sess->pgxc_cxt.remoteXactState->status = RXACT_COMMIT_FAILED;

            /*
             * If error occurred on two phase commit, gs_clean will clean the node later,
             * just note warning message, else note error message.
             */
            if (TwoPhaseCommit) {
                ereport(WARNING,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("failed to send %s command to node %u", command, connections[i]->nodeoid)));

                const int dest_max = 256;
                rc = sprintf_s(errMsg,
                    dest_max,
                    "failed to send COMMIT PREPARED "
                    "command to node %s",
                    connections[i]->remoteNodeName);
                securec_check_ss(rc, "\0", "\0");
                add_error_message(connections[i], "%s", errMsg);
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("failed to send %s command to node %u", command, connections[i]->nodeoid)));
            }
        } else {
            u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_COMMIT_SENT;
            new_connections[new_conn_count++] = connections[i];
        }

#ifdef ENABLE_DISTRIBUTE_TEST
        if (TEST_STUB(CN_COMMIT_PREPARED_MESSAGE_REPEAT, twophase_default_error_emit)) {
            (void)pgxc_node_send_queryid(connections[i], u_sess->debug_query_id);
            (void)pgxc_node_send_query(connections[i], command);
            ereport(g_instance.distribute_test_param_instance->elevel,
                (errmsg("GTM_TEST  %s: repeate send %s", g_instance.attr.attr_common.PGXCNodeName, command)));
        }

        /* white-box test inject start */
        if (execute_whitebox(WHITEBOX_LOC, u_sess->pgxc_cxt.remoteXactState->prepareGID, WHITEBOX_REPEAT, 0.1)) {
            (void)pgxc_node_send_queryid(connections[i], u_sess->debug_query_id);
            (void)pgxc_node_send_query(connections[i], command);
        }
        /* white-box test inject end */
#endif
    }
    pgstat_report_waitstatus_phase(oldPhase);

    /*
     * Release the BarrierLock.
     */
    LWLockRelease(BarrierLock);

#ifdef ENABLE_DISTRIBUTE_TEST
    if (TEST_STUB(CN_COMMIT_PREPARED_RESPONSE_FAILED, twophase_default_error_emit)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
            (errmsg(
                "GTM_TEST  %s: wait response of %s failed", g_instance.attr.attr_common.PGXCNodeName, commitPrepCmd)));
    }

    /* white-box test inject start */
    if (execute_whitebox(WHITEBOX_LOC, NULL, WHITEBOX_CORE, 0.1)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
            (errmsg(
                "WHITE_BOX TEST  %s: wait remote commit response failed", g_instance.attr.attr_common.PGXCNodeName)));
    }
    /* white-box test inject end */

#endif

    if (new_conn_count) {
        initStringInfo(&str);
        combiner = CreateResponseCombiner(new_conn_count, COMBINE_TYPE_NONE);
        /* Receive responses */
        result = pgxc_node_receive_responses(new_conn_count, new_connections, NULL, combiner, false);
        if (result || !validate_combiner(combiner))
            result = EOF;
        else {
            CloseCombiner(combiner);
            combiner = NULL;
        }
        /*
         * Even if the command failed on some node, don't throw an error just
         * yet. That gives a chance to look for individual connection status
         * and record appropriate information for later recovery
         *
         * XXX A node once prepared must be able to either COMMIT or ABORT. So a
         * COMMIT can fail only because of either communication error or because
         * the node went down. Even if one node commits, the transaction must be
         * eventually committed on all the nodes.
         */

        /* At this point, we must be in one the following state */
        Assert(u_sess->pgxc_cxt.remoteXactState->status == RXACT_COMMIT_FAILED ||
               u_sess->pgxc_cxt.remoteXactState->status == RXACT_PREPARED ||
               u_sess->pgxc_cxt.remoteXactState->status == RXACT_NONE);

        /*
         * Go through every connection and check if COMMIT succeeded or failed on
         * that connection. If the COMMIT has failed on one node, but succeeded on
         * some other, such transactions need special attention (by the
         * administrator for now)
         */
        for (i = 0; i < write_conn_count + read_conn_count; i++) {
            if (u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] == RXACT_NODE_COMMIT_SENT) {
                if (connections[i]->error) {
                    u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_COMMIT_FAILED;
                    if (u_sess->pgxc_cxt.remoteXactState->status != RXACT_PART_COMMITTED)
                        u_sess->pgxc_cxt.remoteXactState->status = RXACT_COMMIT_FAILED;
                } else {
                    u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_COMMITTED;
                    if (u_sess->pgxc_cxt.remoteXactState->status == RXACT_COMMIT_FAILED)
                        u_sess->pgxc_cxt.remoteXactState->status = RXACT_PART_COMMITTED;
                }
            }

            /* acquire and print the commit involved handles status for each failed node */
            if (RXACT_NODE_COMMITTED != u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i]) {
                if (i > 0)
                    appendStringInfoChar(&str, ',');
                appendStringInfo(&str, "%u", connections[i]->nodeoid);
            }
        }
    }

    if (result) {
        if (NULL != combiner) {
            // for cm_agent sql, return error
            if (strcmp(u_sess->attr.attr_common.application_name, "cm_agent") == 0) {
                pgxc_node_report_error(combiner, ERROR);
            } else {
                pgxc_node_report_error(combiner, TwoPhaseCommit ? WARNING : ERROR);
            }
        } else {
            if (strcmp(u_sess->attr.attr_common.application_name, "cm_agent") == 0) {
                ereport(
                    ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                     errmsg(
                         "Connection error with Datanode, so failed to COMMIT the transaction on one or more nodes")));
            } else {
                ereport(
                    TwoPhaseCommit ? WARNING : ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                     errmsg(
                         "Connection error with Datanode, so failed to COMMIT the transaction on one or more nodes")));
            }
        }
    }

    if (u_sess->pgxc_cxt.remoteXactState->status == RXACT_COMMIT_FAILED ||
        u_sess->pgxc_cxt.remoteXactState->status == RXACT_PART_COMMITTED) {
        if (strcmp(u_sess->attr.attr_common.application_name, "cm_agent") == 0) {
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
                            errmsg("Failed to COMMIT the transaction on nodes: %s.", str.data)));
        } else {
            ereport(TwoPhaseCommit ? WARNING : ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                     errmsg("Failed to COMMIT the transaction on nodes: %s.", str.data)));
        }
    } else {
        u_sess->pgxc_cxt.remoteXactState->status = RXACT_COMMITTED;
    }
}

/*
 * Abort the current transaction on the local and remote nodes. If the
 * transaction is prepared on the remote node, we send a ROLLBACK PREPARED
 * command, otherwise a ROLLBACK command is sent.
 *
 * Note that if the local node was involved and prepared successfully, we are
 * running in a separate transaction context right now
 */
int pgxc_node_remote_abort(void)
{
#define ERRMSG_BUFF_SIZE 256
    int rc = 0;
    int result = 0;
    const char* rollbackCmd = "ROLLBACK TRANSACTION";
    char rollbackPrepCmd[256];
    char errMsg[ERRMSG_BUFF_SIZE];
    int write_conn_count = u_sess->pgxc_cxt.remoteXactState->numWriteRemoteNodes;
    int read_conn_count = u_sess->pgxc_cxt.remoteXactState->numReadRemoteNodes;
    int i;
    PGXCNodeHandle** connections = u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles;
    PGXCNodeHandle* new_connections[u_sess->pgxc_cxt.remoteXactState->numWriteRemoteNodes +
                                    u_sess->pgxc_cxt.remoteXactState->numReadRemoteNodes];
    int new_conn_count = 0;
    RemoteQueryState* combiner = NULL;

    SetSendCommandId(false);

    /* Send COMMIT/ROLLBACK PREPARED TRANSACTION to the remote nodes */
    WaitStatePhase oldPhase = pgstat_report_waitstatus_phase(PHASE_ROLLBACK);
    for (i = 0; i < write_conn_count + read_conn_count; i++) {
        RemoteXactNodeStatus status = u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i];

        /* We should buffer all messages before reusing the connections to send TRANSACTION commands */
        if (connections[i]->state == DN_CONNECTION_STATE_QUERY) {
            BufferConnection(connections[i]);
        }

        /* Clean the previous errors, if any */
        pfree_ext(connections[i]->error);

        if ((status == RXACT_NODE_PREPARED) || (status == RXACT_NODE_PREPARE_SENT)) {
            rc = sprintf_s(rollbackPrepCmd,
                sizeof(rollbackPrepCmd),
                "ROLLBACK PREPARED '%s'",
                u_sess->pgxc_cxt.remoteXactState->prepareGID);
            securec_check_ss(rc, "\0", "\0");

#ifdef ENABLE_DISTRIBUTE_TEST
            if (TEST_STUB(CN_ABORT_PREPARED_SEND_ALL_FAILED, twophase_default_error_emit)) {
                ereport(g_instance.distribute_test_param_instance->elevel,
                    (errmsg("GTM_TEST  %s: send %s failed, all failed",
                        g_instance.attr.attr_common.PGXCNodeName,
                        rollbackPrepCmd)));
            }

            /* white box test start */
            if (execute_whitebox(WHITEBOX_LOC, u_sess->pgxc_cxt.remoteXactState->prepareGID, WHITEBOX_CORE, 0.1)) {
                ereport(LOG,
                    (errmsg("WHITE_BOX TEST  %s: send abort prepared msg to remote all failed",
                        g_instance.attr.attr_common.PGXCNodeName)));
            }
            /* white box test end */

            if (i == write_conn_count - 1) {
                if (TEST_STUB(CN_ABORT_PREPARED_SEND_PART_FAILED, twophase_default_error_emit)) {
                    ereport(g_instance.distribute_test_param_instance->elevel,
                        (errmsg("GTM_TEST  %s: send %s failed, part failed",
                            g_instance.attr.attr_common.PGXCNodeName,
                            rollbackPrepCmd)));
                }
                /* white box test start */
                if (execute_whitebox(WHITEBOX_LOC, u_sess->pgxc_cxt.remoteXactState->prepareGID, WHITEBOX_CORE, 0.1)) {
                    ereport(LOG,
                        (errmsg("WHITE_BOX TEST  %s: send abort prepared msg to remote part failed",
                            g_instance.attr.attr_common.PGXCNodeName)));
                }
                /* white box test end */
            }
#endif
            if (pgxc_node_send_query(connections[i], rollbackPrepCmd)) {
                rc = sprintf_s(errMsg,
                    ERRMSG_BUFF_SIZE,
                    "failed to send ROLLBACK PREPARED "
                    "TRANSACTION command to node %s",
                    connections[i]->remoteNodeName);
                securec_check_ss(rc, "\0", "\0");
                add_error_message(connections[i], "%s", errMsg);
                u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_ABORT_FAILED;
                u_sess->pgxc_cxt.remoteXactState->status = RXACT_ABORT_FAILED;
            } else {
                u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_ABORT_SENT;
                new_connections[new_conn_count++] = connections[i];
            }
        } else {
            if (pgxc_node_send_query(connections[i], rollbackCmd)) {
                rc = sprintf_s(errMsg,
                    ERRMSG_BUFF_SIZE,
                    "failed to send ROLLBACK "
                    "TRANSACTION command to node %s",
                    connections[i]->remoteNodeName);
                securec_check_ss(rc, "\0", "\0");
                add_error_message(connections[i], "%s", errMsg);
                u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_ABORT_FAILED;
                u_sess->pgxc_cxt.remoteXactState->status = RXACT_ABORT_FAILED;
            } else {
                u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_ABORT_SENT;
                new_connections[new_conn_count++] = connections[i];
            }
        }
    }
    pgstat_report_waitstatus_phase(oldPhase);

#ifdef ENABLE_DISTRIBUTE_TEST
    if (TEST_STUB(CN_ABORT_PREPARED_RESPONSE_FAILED, twophase_default_error_emit)) {
        ereport(g_instance.distribute_test_param_instance->elevel,
            (errmsg("GTM_TEST  %s: wait response of %s failed",
                g_instance.attr.attr_common.PGXCNodeName,
                rollbackPrepCmd)));
    }

    /* white box test start */
    if (execute_whitebox(WHITEBOX_LOC, NULL, WHITEBOX_CORE, 0.1)) {
        ereport(LOG,
            (errmsg("WHITE_BOX TEST  %s: wait remote abort prepared response failed",
                g_instance.attr.attr_common.PGXCNodeName)));
    }
    /* white box test end */
#endif

    if (new_conn_count) {
        struct timeval abort_timeout = {0};

        abort_timeout.tv_sec = u_sess->attr.attr_network.PoolerCancelTimeout; /* seconds */
        abort_timeout.tv_usec = 0;                                            /* microseconds */

        combiner = CreateResponseCombiner(new_conn_count, COMBINE_TYPE_NONE);

        /*
         * Receive responses
         * We use pooler cancel timeout here 'cause when we abort transaction in
         * proc die process, we should not wait forever.
         */
        result = pgxc_node_receive_responses(new_conn_count,
            new_connections,
            u_sess->attr.attr_network.PoolerCancelTimeout ? &abort_timeout : NULL,
            combiner,
            false);
        if (result || !validate_combiner(combiner))
            result = EOF;
        else {
            CloseCombiner(combiner);
            combiner = NULL;
        }

        for (i = 0; i < write_conn_count + read_conn_count; i++) {
            if (u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] == RXACT_NODE_ABORT_SENT) {
                if (connections[i]->error) {
                    u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_ABORT_FAILED;
                    if (u_sess->pgxc_cxt.remoteXactState->status != RXACT_PART_ABORTED)
                        u_sess->pgxc_cxt.remoteXactState->status = RXACT_ABORT_FAILED;
                    elog(LOG, "Failed to ABORT at node %u\nDetail: %s", connections[i]->nodeoid, connections[i]->error);
                } else {
                    u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_ABORTED;
                    if (u_sess->pgxc_cxt.remoteXactState->status == RXACT_ABORT_FAILED)
                        u_sess->pgxc_cxt.remoteXactState->status = RXACT_PART_ABORTED;
                }
            }
        }
    }

    if (result) {
        if (combiner != NULL)
            pgxc_node_report_error(combiner, WARNING);
        else {
            elog(LOG,
                "Failed to ABORT an implicitly PREPARED "
                "transaction - result %d",
                result);
        }
    }

    /*
     * Don't ereport because we might already been abort processing and any
     * error at this point can lead to infinite recursion
     *
     * XXX How do we handle errors reported by internal functions used to
     * communicate with remote nodes ?
     */
    if (u_sess->pgxc_cxt.remoteXactState->status == RXACT_ABORT_FAILED ||
        u_sess->pgxc_cxt.remoteXactState->status == RXACT_PART_ABORTED) {
        result = EOF;
        elog(LOG,
            "Failed to ABORT an implicitly PREPARED transaction "
            "status - %s.",
            RemoteXactStatusAsString(u_sess->pgxc_cxt.remoteXactState->status));
    } else
        u_sess->pgxc_cxt.remoteXactState->status = RXACT_ABORTED;

    return result;
}

void pgxc_node_remote_savepoint(const char* cmdString, RemoteQueryExecType exec_type, bool bNeedXid, bool bNeedBegin,
    GlobalTransactionId transactionId)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

PGXCNodeHandle** DataNodeCopyBegin(const char* query, List* nodelist, Snapshot snapshot)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
#else
    int i;
    int conn_count = list_length(nodelist) == 0 ? NumDataNodes : list_length(nodelist);
    struct timeval* timeout = NULL;
    PGXCNodeAllHandles* pgxc_handles = NULL;
    PGXCNodeHandle** connections = NULL;
    PGXCNodeHandle** copy_connections = NULL;
    ListCell* nodeitem = NULL;
    bool need_tran_block = false;
    GlobalTransactionId gxid;
    RemoteQueryState* combiner = NULL;

    if (conn_count == 0)
        return NULL;

    /* Get needed Datanode connections */
    pgxc_handles = get_handles(nodelist, NULL, false);
    connections = pgxc_handles->datanode_handles;

    if (!connections)
        return NULL;

    /*
     * If more than one nodes are involved or if we are already in a
     * transaction block, we must the remote statements in a transaction block
     */
    need_tran_block = (conn_count > 1) || (TransactionBlockStatusCode() == 'T');

    elog(DEBUG1, "conn_count = %d, need_tran_block = %s", conn_count, need_tran_block ? "true" : "false");

    /*
     * We need to be able quickly find a connection handle for specified node number,
     * So store connections in an array where index is node-1.
     * Unused items in the array should be NULL
     */
    copy_connections = (PGXCNodeHandle**)palloc0(NumDataNodes * sizeof(PGXCNodeHandle*));
    i = 0;
    foreach (nodeitem, nodelist)
        copy_connections[lfirst_int(nodeitem)] = connections[i++];

    gxid = GetCurrentTransactionId();

    if (!GlobalTransactionIdIsValid(gxid)) {
        pfree_pgxc_all_handles(pgxc_handles);
        pfree(copy_connections);
        return NULL;
    }

    /* Start transaction on connections where it is not started */
    if (pgxc_node_begin(conn_count, connections, gxid, need_tran_block, false, PGXC_NODE_DATANODE)) {
        pfree_pgxc_all_handles(pgxc_handles);
        pfree(copy_connections);
        return NULL;
    }

    /* Send query to nodes */
    for (i = 0; i < conn_count; i++) {
        if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
            BufferConnection(connections[i]);

        if (snapshot && pgxc_node_send_snapshot(connections[i], snapshot)) {
            add_error_message(connections[i], "%s", "Can not send request");
            pfree_pgxc_all_handles(pgxc_handles);
            pfree(copy_connections);
            return NULL;
        }
        if (pgxc_node_send_query(connections[i], query) != 0) {
            add_error_message(connections[i], "%s", "Can not send request");
            pfree_pgxc_all_handles(pgxc_handles);
            pfree(copy_connections);
            return NULL;
        }
    }

    /*
     * We are expecting CopyIn response, but do not want to send it to client,
     * caller should take care about this, because here we do not know if
     * client runs console or file copy
     */
    combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

    /* Receive responses */
    if (pgxc_node_receive_responses(conn_count, connections, timeout, combiner) ||
        !ValidateAndCloseCombiner(combiner)) {
        DataNodeCopyFinish(connections, -1, COMBINE_TYPE_NONE);
        pfree(connections);
        pfree(copy_connections);
        return NULL;
    }
    pfree(connections);
    return copy_connections;
#endif
}

/*
 * Send a data row to the specified nodes
 */

template <bool is_binary>
static int DataNodeCopyInT(
    const char* data_row, int len, const char* eol, ExecNodes* exec_nodes, PGXCNodeHandle** copy_connections)
{
    PGXCNodeHandle* primary_handle = NULL;
    ListCell* nodeitem = NULL;
    errno_t rc = EOK;

    /* size + data row + \n */
    int msgLen = 0;
    if (is_binary) {
        msgLen = 4 + len;
    } else {
        msgLen = 4 + len + ((eol == NULL) ? 1 : strlen(eol));
    }
    int nLen = htonl(msgLen);

    if (exec_nodes->primarynodelist != NIL) {
        primary_handle = copy_connections[lfirst_int(list_head(exec_nodes->primarynodelist))];
    }

    if (primary_handle != NULL) {
        if (primary_handle->state == DN_CONNECTION_STATE_COPY_IN) {
            /* precalculate to speed up access */
            const int bytes_needed = 1 + msgLen;

            /* flush buffer if it is almost full */
            if (bytes_needed + primary_handle->outEnd > COPY_BUFFER_SIZE) {
                int read_status = -1;
                /* First look if Datanode has sent a error message */
                if (primary_handle->is_logic_conn)
                    /* for logic connection between cn & dn */
                    read_status = pgxc_node_read_data_from_logic_conn(primary_handle, true);
                else
                    read_status = pgxc_node_read_data(primary_handle, true);
                if (read_status == EOF || read_status < 0) {
                    add_error_message(primary_handle, "%s", "failed to read data from Datanode");
                    return EOF;
                }

                if (primary_handle->inStart < primary_handle->inEnd) {
                    RemoteQueryState* combiner = CreateResponseCombiner(1, COMBINE_TYPE_NONE);
                    (void)handle_response(primary_handle, combiner);
                    if (!ValidateAndCloseCombiner(combiner))
                        return EOF;
                }

                if (DN_CONNECTION_STATE_ERROR(primary_handle))
                    return EOF;

                if (pgxc_node_flush(primary_handle) < 0) {
                    add_error_message(primary_handle, "%s", "failed to send data to Datanode");
                    return EOF;
                }
            }

            ensure_out_buffer_capacity(bytes_needed, primary_handle);
            Assert(primary_handle->outBuffer != NULL);
            primary_handle->outBuffer[primary_handle->outEnd++] = 'd';
            const int memcpy_len = 4;
            rc = memcpy_s(primary_handle->outBuffer + primary_handle->outEnd, memcpy_len, &nLen, memcpy_len);
            securec_check(rc, "", "");
            primary_handle->outEnd += 4;
            if (len != 0) {
                rc = memcpy_s(primary_handle->outBuffer + primary_handle->outEnd, len, data_row, len);
                securec_check(rc, "", "");
            }
            primary_handle->outEnd += len;
            if (!is_binary) {
                if (eol == NULL) {
                    primary_handle->outBuffer[primary_handle->outEnd++] = '\n';
                } else {
                    rc = memcpy_s(primary_handle->outBuffer + primary_handle->outEnd, strlen(eol), eol, strlen(eol));
                    securec_check(rc, "", "");
                    primary_handle->outEnd += strlen(eol);
                }
            }
        } else {
            add_error_message(primary_handle, "%s", "Invalid Datanode connection");
            return EOF;
        }
    }

    foreach (nodeitem, exec_nodes->nodeList) {
        PGXCNodeHandle* handle = copy_connections[lfirst_int(nodeitem)];
        if (handle != NULL && handle->state == DN_CONNECTION_STATE_COPY_IN) {
            /* precalculate to speed up access */
            const int bytes_needed = 1 + msgLen;

            /* flush buffer if it is almost full */
            if ((primary_handle != NULL && bytes_needed + handle->outEnd > PRIMARY_NODE_WRITEAHEAD) ||
                (primary_handle == NULL && bytes_needed + handle->outEnd > COPY_BUFFER_SIZE)) {
                int to_send = handle->outEnd;
                int read_status = -1;
                /* First look if Datanode has sent a error message */
                if (handle->is_logic_conn)
                    /* for logic connection between cn & dn */
                    read_status = pgxc_node_read_data_from_logic_conn(handle, true);
                else
                    read_status = pgxc_node_read_data(handle, true);
                if (read_status == EOF || read_status < 0) {
                    add_error_message(handle, "%s", "failed to read data from Datanode");
                    return EOF;
                }

                if (handle->inStart < handle->inEnd) {
                    RemoteQueryState* combiner = CreateResponseCombiner(1, COMBINE_TYPE_NONE);
                    (void)handle_response(handle, combiner);
                    if (combiner->errorMessage)
                        pgxc_node_report_error(combiner);
                    if (!ValidateAndCloseCombiner(combiner))
                        return EOF;
                }

                if (DN_CONNECTION_STATE_ERROR(handle))
                    return EOF;

                /*
                 * Allow primary node to write out data before others.
                 * If primary node was blocked it would not accept copy data.
                 * So buffer at least PRIMARY_NODE_WRITEAHEAD at the other nodes.
                 * If primary node is blocked and is buffering, other buffers will
                 * grow accordingly.
                 */
                if (primary_handle != NULL) {
                    if (primary_handle->outEnd + PRIMARY_NODE_WRITEAHEAD < handle->outEnd)
                        to_send = handle->outEnd - primary_handle->outEnd - PRIMARY_NODE_WRITEAHEAD;
                    else
                        to_send = 0;
                }

                /*
                 * Try to send down buffered data if we have
                 */
                if (to_send && pgxc_node_flush(handle) < 0) {
                    add_error_message(handle, "%s", "failed to send data to Datanode");
                    return EOF;
                }
            }

            ensure_out_buffer_capacity(bytes_needed, handle);
            Assert(handle->outBuffer != NULL);
            handle->outBuffer[handle->outEnd++] = 'd';
            const int mem_len = 4;
            rc = memcpy_s(handle->outBuffer + handle->outEnd, mem_len, &nLen, mem_len);
            securec_check(rc, "", "");
            handle->outEnd += 4;
            if (len != 0) {
                rc = memcpy_s(handle->outBuffer + handle->outEnd, len, data_row, len);
                securec_check(rc, "", "");
            }
            handle->outEnd += len;
            handle->outNum += 1;
            if (!is_binary) {
                if (eol == NULL) {
                    handle->outBuffer[handle->outEnd++] = '\n';
                } else {
                    rc = memcpy_s(handle->outBuffer + handle->outEnd, strlen(eol), eol, strlen(eol));
                    securec_check(rc, "", "");
                    handle->outEnd += strlen(eol);
                }
            }
        } else {
            if (handle != NULL)
                add_error_message(handle, "%s", "Invalid Datanode connection");
            return EOF;
        }
    }
    return 0;
}

int DataNodeCopyIn(const char* data_row, int len, const char* eol, ExecNodes* exec_nodes,
    PGXCNodeHandle** copy_connections, bool is_binary)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
#else
    PGXCNodeHandle* primary_handle = NULL;
    ListCell* nodeitem = NULL;
    /* size + data row + \n */
    int msgLen = 4 + len + 1;
    int nLen = htonl(msgLen);

    if (exec_nodes->primarynodelist) {
        primary_handle = copy_connections[lfirst_int(list_head(exec_nodes->primarynodelist))];
    }

    if (primary_handle) {
        if (primary_handle->state == DN_CONNECTION_STATE_COPY_IN) {
            /* precalculate to speed up access */
            int bytes_needed = primary_handle->outEnd + 1 + msgLen;

            /* flush buffer if it is almost full */
            if (bytes_needed > COPY_BUFFER_SIZE) {
                /* First look if Datanode has sent a error message */
                int read_status = pgxc_node_read_data(primary_handle, true);
                if (read_status == EOF || read_status < 0) {
                    add_error_message(primary_handle, "%s", "failed to read data from Datanode");
                    return EOF;
                }

                if (primary_handle->inStart < primary_handle->inEnd) {
                    RemoteQueryState* combiner = CreateResponseCombiner(1, COMBINE_TYPE_NONE);
                    handle_response(primary_handle, combiner);
                    if (!ValidateAndCloseCombiner(combiner))
                        return EOF;
                }

                if (DN_CONNECTION_STATE_ERROR(primary_handle))
                    return EOF;

                if (send_some(primary_handle, primary_handle->outEnd) < 0) {
                    add_error_message(primary_handle, "%s", "failed to send data to Datanode");
                    return EOF;
                }
            }

            if (ensure_out_buffer_capacity(bytes_needed, primary_handle) != 0) {
                ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
            }

            primary_handle->outBuffer[primary_handle->outEnd++] = 'd';
            rc = memcpy_s(primary_handle->outBuffer + primary_handle->outEnd, 4, &nLen, 4);
            securec_check(rc, "", "");
            primary_handle->outEnd += 4;
            rc = memcpy_s(primary_handle->outBuffer + primary_handle->outEnd, len, data_row, len);
            securec_check(rc, "", "");
            primary_handle->outEnd += len;
            primary_handle->outBuffer[primary_handle->outEnd++] = '\n';
        } else {
            add_error_message(primary_handle, "%s", "Invalid Datanode connection");
            return EOF;
        }
    }

    foreach (nodeitem, exec_nodes->nodeList) {
        PGXCNodeHandle* handle = copy_connections[lfirst_int(nodeitem)];
        if (handle && handle->state == DN_CONNECTION_STATE_COPY_IN) {
            /* precalculate to speed up access */
            int bytes_needed = handle->outEnd + 1 + msgLen;

            /* flush buffer if it is almost full */
            if ((primary_handle && bytes_needed > PRIMARY_NODE_WRITEAHEAD) ||
                (!primary_handle && bytes_needed > COPY_BUFFER_SIZE)) {
                int to_send = handle->outEnd;

                /* First look if Datanode has sent a error message */
                int read_status = pgxc_node_read_data(handle, true);
                if (read_status == EOF || read_status < 0) {
                    add_error_message(handle, "%s", "failed to read data from Datanode");
                    return EOF;
                }

                if (handle->inStart < handle->inEnd) {
                    RemoteQueryState* combiner = CreateResponseCombiner(1, COMBINE_TYPE_NONE);
                    handle_response(handle, combiner);
                    if (!ValidateAndCloseCombiner(combiner))
                        return EOF;
                }

                if (DN_CONNECTION_STATE_ERROR(handle))
                    return EOF;

                /*
                 * Allow primary node to write out data before others.
                 * If primary node was blocked it would not accept copy data.
                 * So buffer at least PRIMARY_NODE_WRITEAHEAD at the other nodes.
                 * If primary node is blocked and is buffering, other buffers will
                 * grow accordingly.
                 */
                if (primary_handle) {
                    if (primary_handle->outEnd + PRIMARY_NODE_WRITEAHEAD < handle->outEnd)
                        to_send = handle->outEnd - primary_handle->outEnd - PRIMARY_NODE_WRITEAHEAD;
                    else
                        to_send = 0;
                }

                /*
                 * Try to send down buffered data if we have
                 */
                if (to_send && send_some(handle, to_send) < 0) {
                    add_error_message(handle, "%s", "failed to send data to Datanode");
                    return EOF;
                }
            }

            if (ensure_out_buffer_capacity(bytes_needed, handle) != 0) {
                ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
            }

            handle->outBuffer[handle->outEnd++] = 'd';
            rc = memcpy_s(handle->outBuffer + handle->outEnd, 4, &nLen, 4);
            securec_check(rc, "", "");
            handle->outEnd += 4;
            rc = memcpy_s(handle->outBuffer + handle->outEnd, len, data_row, len);
            securec_check(rc, "", "");
            handle->outEnd += len;
            handle->outBuffer[handle->outEnd++] = '\n';
        } else {
            add_error_message(handle, "%s", "Invalid Datanode connection");
            return EOF;
        }
    }
    return 0;
#endif
}

uint64 DataNodeCopyOut(ExecNodes* exec_nodes, PGXCNodeHandle** copy_connections, TupleDesc tupleDesc, FILE* copy_file,
    Tuplestorestate* store, RemoteCopyType remoteCopyType)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
#else
    RemoteQueryState* combiner;
    int conn_count = (list_length(exec_nodes->nodeList) == 0) ? NumDataNodes : list_length(exec_nodes->nodeList);
    ListCell* nodeitem = NULL;
    uint64 processed;

    combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_SUM);
    combiner->processed = 0;
    combiner->remoteCopyType = remoteCopyType;

    /*
     * If there is an existing file where to copy data,
     * pass it to combiner when remote COPY output is sent back to file.
     */
    if (copy_file && remoteCopyType == REMOTE_COPY_FILE)
        combiner->copy_file = copy_file;
    if (store && remoteCopyType == REMOTE_COPY_TUPLESTORE) {
        combiner->tuplestorestate = store;
        combiner->tuple_desc = tupleDesc;
    }

    foreach (nodeitem, exec_nodes->nodeList) {
        PGXCNodeHandle* handle = copy_connections[lfirst_int(nodeitem)];
        int read_status = 0;

        Assert(handle && handle->state == DN_CONNECTION_STATE_COPY_OUT);

        /*
         * H message has been consumed, continue to manage data row messages.
         * Continue to read as long as there is data.
         */
        while (read_status >= 0 && handle->state == DN_CONNECTION_STATE_COPY_OUT) {
            if (handle_response(handle, combiner) == RESPONSE_EOF) {
                /* read some extra-data */
                read_status = pgxc_node_read_data(handle, true);
                if (read_status < 0)
                	ereport(ERROR,
                    	(errcode(ERRCODE_CONNECTION_FAILURE), errmsg("unexpected EOF on datanode connection")));
                else
                    /*
                     * Set proper connection status - handle_response
                     * has changed it to DN_CONNECTION_STATE_QUERY
                     */
                    handle->state = DN_CONNECTION_STATE_COPY_OUT;
            }
            /* There is no more data that can be read from connection */
        }
    }

    processed = combiner->processed;

    if (!ValidateAndCloseCombiner(combiner)) {
        if (!PersistentConnections)
            release_handles();
        pfree(copy_connections);
        ereport(ERROR,
            (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg(
                    "Unexpected response from the Datanodes when combining, request type %d", combiner->request_type)));
    }

    return processed;
#endif
}

void report_table_skewness_alarm(AlarmType alarmType, const char* tableName)
{
    Alarm AlarmTableSkewness[1];
    AlarmItemInitialize(&(AlarmTableSkewness[0]), ALM_AI_AbnormalTableSkewness, ALM_AS_Normal, NULL);
    AlarmAdditionalParam tempAdditionalParam;
    // fill the alarm message
    WriteAlarmAdditionalInfo(
        &tempAdditionalParam, "", const_cast<char*>(tableName), "", AlarmTableSkewness, alarmType, tableName);
    // report the alarm
    AlarmReporter(AlarmTableSkewness, alarmType, &tempAdditionalParam);
}

void DataNodeCopyFinish(PGXCNodeHandle** copy_connections, int n_copy_connections, int primary_dn_index,
    CombineType combine_type, Relation rel)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    int i;
    RemoteQueryState* combiner = NULL;
    bool error = false;
    struct timeval* timeout = NULL; /* wait forever */
    PGXCNodeHandle* connections[NumDataNodes];
    PGXCNodeHandle* primary_handle = NULL;
    int conn_count = 0;

    for (i = 0; i < NumDataNodes; i++) {
        PGXCNodeHandle* handle = copy_connections[i];

        if (!handle)
            continue;

        if (i == primary_dn_index)
            primary_handle = handle;
        else
            connections[conn_count++] = handle;
    }

    if (primary_handle) {
        error = true;
        if (primary_handle->state == DN_CONNECTION_STATE_COPY_IN ||
            primary_handle->state == DN_CONNECTION_STATE_COPY_OUT)
            error = DataNodeCopyEnd(primary_handle, false);

        combiner = CreateResponseCombiner(conn_count + 1, combine_type);
        error = (pgxc_node_receive_responses(1, &primary_handle, timeout, combiner) != 0) || error;
    }

    for (i = 0; i < conn_count; i++) {
        PGXCNodeHandle* handle = connections[i];

        error = true;
        if (handle->state == DN_CONNECTION_STATE_COPY_IN || handle->state == DN_CONNECTION_STATE_COPY_OUT)
            error = DataNodeCopyEnd(handle, false);
    }

    if (!combiner)
        combiner = CreateResponseCombiner(conn_count, combine_type);
    error = (pgxc_node_receive_responses(conn_count, connections, timeout, combiner) != 0) || error;

    if (!ValidateAndCloseCombiner(combiner) || error)
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Error while running COPY")));
#endif
}

/*
 * End copy process on a connection
 */
bool DataNodeCopyEnd(PGXCNodeHandle* handle, bool is_error)
{
    int nLen = htonl(4);
    errno_t rc = EOK;

    if (handle == NULL)
        return true;

    /* msgType + msgLen */
    ensure_out_buffer_capacity(1 + 4, handle);
    Assert(handle->outBuffer != NULL);
    if (is_error)
        handle->outBuffer[handle->outEnd++] = 'f';
    else
        handle->outBuffer[handle->outEnd++] = 'c';

    rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd - 1, &nLen, sizeof(int));
    securec_check(rc, "", "");
    handle->outEnd += 4;

    /* We need response right away, so send immediately */
    if (pgxc_node_flush(handle) < 0)
        return true;

    return false;
}

RemoteQueryState* ExecInitRemoteQuery(RemoteQuery* node, EState* estate, int eflags, bool row_plan)
{
#ifdef USE_SPQ
    return ExecInitSpqRemoteQuery(node, estate, eflags, row_plan);
#endif
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
#else
    RemoteQueryState* remotestate = NULL;
    TupleDesc scan_type;

    /* RemoteQuery node is the leaf node in the plan tree, just like seqscan */
    Assert(innerPlan(node) == NULL);
    Assert(outerPlan(node) == NULL);

    remotestate = CreateResponseCombiner(0, node->combine_type);
    remotestate->ss.ps.plan = (Plan*)node;
    remotestate->ss.ps.state = estate;

    /*
     * Miscellaneous initialisation
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &remotestate->ss.ps);

    /* Initialise child expressions */
    if (estate->es_is_flt_frame) {
        remotestate->ss.ps.qual = (List*)ExecInitQualByFlatten(node->scan.plan.qual, (PlanState*)remotestate);
    } else {
        remotestate->ss.ps.targetlist = (List*)ExecInitExprByRecursion((Expr*)node->scan.plan.targetlist, (PlanState*)remotestate);
        remotestate->ss.ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->scan.plan.qual, (PlanState*)remotestate);
    }

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_MARK)));

    /* Extract the eflags bits that are relevant for tuplestorestate */
    remotestate->eflags = (eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD));

    /* We anyways have to support REWIND for ReScan */
    remotestate->eflags |= EXEC_FLAG_REWIND;

    remotestate->eof_underlying = false;
    remotestate->tuplestorestate = NULL;

    ExecInitResultTupleSlot(estate, &remotestate->ss.ps);
    ExecInitScanTupleSlot(estate, &remotestate->ss);
    scan_type = ExecTypeFromTL(node->base_tlist, false);
    ExecAssignScanType(&remotestate->ss, scan_type);
    remotestate->ss.ps.ps_vec_TupFromTlist = false;
    /*
     * If there are parameters supplied, get them into a form to be sent to the
     * Datanodes with bind message. We should not have had done this before.
     */
    SetDataRowForExtParams(estate->es_param_list_info, remotestate);

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(&remotestate->ss.ps);
    ExecAssignScanProjectionInfo(&remotestate->ss);

    if (node->rq_save_command_id) {
        /* Save command id to be used in some special cases */
        remotestate->rqs_cmd_id = GetCurrentCommandId(false);
    }

    return remotestate;
#endif
}
/*
 * GetNodeIdFromNodesDef
 */
static int GetNodeIdFromNodesDef(NodeDefinition* node_def, Oid nodeoid)
{
    int i;
    int res = -1;

    /* Look into the handles and return correct position in array */
    for (i = 0; i < u_sess->pgxc_cxt.NumDataNodes; i++) {
        if (node_def[i].nodeoid == nodeoid) {
            res = i;
            break;
        }
    }
    return res;
}

/*
 * Get Node connections depending on the connection type:
 * Datanodes Only, Coordinators only or both types
 */
PGXCNodeAllHandles* get_exec_connections(
    RemoteQueryState* planstate, ExecNodes* exec_nodes, RemoteQueryExecType exec_type)
{
    List* nodelist = NIL;
    List* primarynode = NIL;
    List* coordlist = NIL;
    PGXCNodeHandle* primaryconnection = NULL;
    int co_conn_count, dn_conn_count;
    bool is_query_coord_only = false;
    PGXCNodeAllHandles* pgxc_handles = NULL;
    RelationLocInfo* rel_loc_info = NULL;
    bool isFreeNodeList = true;
    List* dummynodelist = NIL;

    /*
     * If query is launched only on Coordinators, we have to inform get_handles
     * not to ask for Datanode connections even if list of Datanodes is NIL.
     */
    if (exec_type == EXEC_ON_COORDS)
        is_query_coord_only = true;

    if (exec_nodes != NULL) {
        if (exec_nodes->en_expr) {
            /* execution time determining of target Datanodes */
            bool isnull = false;
            MemoryContext oldContext;

            rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);
            if (unlikely(rel_loc_info == NULL)) {
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("Can not find location info for relation oid: %u", exec_nodes->en_relid)));
            }

            int len = list_length(rel_loc_info->partAttrNum);
            /* It should switch memctx to ExprContext for makenode in ExecInitExpr */
            Datum* values = (Datum*)palloc(len * sizeof(Datum));
            bool* null = (bool*)palloc(len * sizeof(bool));
            Oid* typOid = (Oid*)palloc(len * sizeof(Oid));
            List* dist_col = NULL;
            int i = 0;

            ListCell* cell = NULL;
            foreach (cell, exec_nodes->en_expr) {
                Expr* expr = (Expr*)lfirst(cell);
                oldContext = MemoryContextSwitchTo(planstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

                ExprState* estate = ExecInitExpr(expr, (PlanState*)planstate);

                Datum partvalue = ExecEvalExpr(estate, planstate->ss.ps.ps_ExprContext, &isnull, NULL);
                MemoryContextSwitchTo(oldContext);

                values[i] = partvalue;
                null[i] = isnull;
                typOid[i] = exprType((Node*)expr);
                dist_col = lappend_int(dist_col, i);
                i++;
            }
            ExecNodes* nodes = GetRelationNodes(rel_loc_info, values, null, typOid, dist_col, exec_nodes->accesstype);

            if (nodes != NULL) {
                nodelist = nodes->nodeList;
                primarynode = nodes->primarynodelist;

                /* for explain analyze to show the datanode which really runs */
                if (t_thrd.postgres_cxt.mark_explain_analyze) {
                    if (planstate->pbe_run_status == PBE_NONE) {
                        /* first run */
                        planstate->pbe_run_status = PBE_ON_ONE_NODE;
                        /* keep original nodeList if needed */
                        if (!exec_nodes->nodelist_is_nil && !exec_nodes->original_nodeList)
                            exec_nodes->original_nodeList = exec_nodes->nodeList;

                        exec_nodes->nodeList = nodelist;
                        isFreeNodeList = false;
                    } else if (planstate->pbe_run_status == PBE_ON_ONE_NODE) {
                        /* second run or always same nodelist before */
                        if (list_difference_int(exec_nodes->nodeList, nodelist)) {
                            planstate->pbe_run_status = PBE_ON_MULTI_NODES;
                            if (exec_nodes->nodeList)
                                list_free_ext(exec_nodes->nodeList);

                            exec_nodes->nodeList = exec_nodes->original_nodeList;
                        }
                    }
                }

                bms_free_ext(nodes->distribution.bms_data_nodeids);
                pfree_ext(nodes);
            }
            /*
             * en_expr is set by pgxc_set_en_expr only for distributed
             * relations while planning DMLs, hence a select for update
             * on a replicated table here is an assertion
             */
            Assert(!(exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE && IsRelationReplicated(rel_loc_info)));
            pfree_ext(values);
            pfree_ext(null);
            pfree_ext(typOid);
        } else if (OidIsValid(exec_nodes->en_relid)) {
            rel_loc_info = GetRelationLocInfo(exec_nodes->en_relid);
            if (unlikely(rel_loc_info == NULL)) {
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("Can not find location info for relation oid: %u", exec_nodes->en_relid)));
            }

            ExecNodes* nodes = GetRelationNodes(rel_loc_info, NULL, NULL, NULL, NULL, exec_nodes->accesstype);

            /*
             * en_relid is set only for DMLs, hence a select for update on a
             * replicated table here is an assertion
             */
            Assert(!(exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE && IsRelationReplicated(rel_loc_info)));

            /* Use the obtained list for given table */
            if (nodes != NULL) {
                bms_free_ext(nodes->distribution.bms_data_nodeids);
                nodelist = nodes->nodeList;
            }

            /*
             * Special handling for ROUND ROBIN distributed tables. The target
             * node must be determined at the execution time
             */
            if (rel_loc_info->locatorType == LOCATOR_TYPE_RROBIN && nodes) {
                nodelist = nodes->nodeList;
                primarynode = nodes->primarynodelist;
            } else if (nodes != NULL) {
                if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES) {
                    isFreeNodeList = true;
                    if (exec_nodes->nodeList || exec_nodes->primarynodelist) {
                        nodelist = exec_nodes->nodeList;
                        primarynode = exec_nodes->primarynodelist;
                        isFreeNodeList = false;
                    }
                }
            }

            /* Must free nodelist if nodelist is not belone to exec_nodes */
            if (nodes && !isFreeNodeList) {
                if (nodes->nodeList)
                    list_free_ext(nodes->nodeList);
                if (nodes->primarynodelist)
                    list_free_ext(nodes->primarynodelist);

                pfree_ext(nodes);
                nodes = NULL;
            }
        } else {
            if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES)
                nodelist = exec_nodes->nodeList;
            else if (exec_type == EXEC_ON_COORDS)
                coordlist = exec_nodes->nodeList;

            primarynode = exec_nodes->primarynodelist;
            isFreeNodeList = false;
        }
    }

    /* Set datanode list and DN number */
    if (list_length(nodelist) == 0 && (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_DATANODES)) {
        /* Primary connection is included in this number of connections if it exists */
        dn_conn_count = u_sess->pgxc_cxt.NumDataNodes;
    } else {
        if (exec_type == EXEC_ON_DATANODES || exec_type == EXEC_ON_ALL_NODES) {
            if (primarynode != NULL)
                dn_conn_count = list_length(nodelist) + 1;
            else
                dn_conn_count = list_length(nodelist);
        } else
            dn_conn_count = 0;
    }

    /* Set Coordinator list and Coordinator number */
    if (exec_type == EXEC_ON_ALL_NODES || (list_length(coordlist) == 0 && exec_type == EXEC_ON_COORDS)) {
        coordlist = GetAllCoordNodes();
        co_conn_count = list_length(coordlist);
    } else {
        if (exec_type == EXEC_ON_COORDS)
            co_conn_count = list_length(coordlist);
        else
            co_conn_count = 0;
    }

    /*
     * For multi-node group, the exec_nodes of replicate table dml may be different from max datanode list.
     * Therefore, we should get valid datanode from subplan, and check if the results are same for them
     */
    if (planstate != NULL && planstate->combine_type == COMBINE_TYPE_SAME && exec_type == EXEC_ON_DATANODES) {
        if (planstate->ss.ps.plan->lefttree != NULL && planstate->ss.ps.plan->lefttree->exec_nodes != NULL)
            dummynodelist = list_difference_int(nodelist, planstate->ss.ps.plan->lefttree->exec_nodes->nodeList);
    }

    /* Get other connections (non-primary) */
    pgxc_handles = get_handles(nodelist, coordlist, is_query_coord_only, dummynodelist);

    /* Get connection for primary node, if used */
    if (primarynode != NULL) {
        /* Let's assume primary connection is always a Datanode connection for the moment */
        PGXCNodeAllHandles* pgxc_conn_res = NULL;
        pgxc_conn_res = get_handles(primarynode, NULL, false);

        /* primary connection is unique */
        primaryconnection = pgxc_conn_res->datanode_handles[0];

        pfree_ext(pgxc_conn_res);

        if (primaryconnection == NULL)
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("could not obtain connection from pool")));
        pgxc_handles->primary_handle = primaryconnection;
    }

    /* Depending on the execution type, we still need to save the initial node counts */
    pgxc_handles->dn_conn_count = dn_conn_count;
    pgxc_handles->co_conn_count = co_conn_count;

    if (rel_loc_info != NULL)
        FreeRelationLocInfo(rel_loc_info);

    /* Must free nodelist if nodelist is not belone to exec_nodes */
    if (isFreeNodeList) {
        if (nodelist != NULL)
            list_free_ext(nodelist);
        if (primarynode != NULL)
            list_free_ext(primarynode);
    }

    return pgxc_handles;
}

/*
 * @Description: Send the queryId down to the PGXC node with sync
 *
 * @param[IN] connections:  all connection handle with Datanode
 * @param[IN] conn_count:  number of connections
 * @param[IN] queryId:  query ID of current simple query
 * @return: void
 */
static void pgxc_node_send_queryid_with_sync(PGXCNodeHandle** connections, int conn_count, uint64 queryId)
{
    PGXCNodeHandle** temp_connections = NULL;
    RemoteQueryState* combiner = NULL;
    int i = 0;
    errno_t ss_rc = 0;

    /* use temp connections instead */
    temp_connections = (PGXCNodeHandle**)palloc(conn_count * sizeof(PGXCNodeHandle*));
    for (i = 0; i < conn_count; i++)
        temp_connections[i] = connections[i];

    Assert(queryId != 0);

    int countOID = list_length(u_sess->spq_cxt.direct_read_map);
    for (i = 0; i < conn_count; i++) {
        int msglen = sizeof(int) + sizeof(uint64) + sizeof(int) + sizeof(Oid) * countOID;

        if (temp_connections[i]->state == DN_CONNECTION_STATE_QUERY)
            BufferConnection(temp_connections[i]);

        if (connections[i]->state != DN_CONNECTION_STATE_IDLE)
            LIBCOMM_DEBUG_LOG("send_queryid to node:%s[nid:%hu,sid:%hu] with abnormal state:%d",
                temp_connections[i]->remoteNodeName,
                temp_connections[i]->gsock.idx,
                temp_connections[i]->gsock.sid,
                temp_connections[i]->state);

        /* msgType + msgLen */
        ensure_out_buffer_capacity(1 + msglen, temp_connections[i]);
        Assert(temp_connections[i]->outBuffer != NULL);
        temp_connections[i]->outBuffer[temp_connections[i]->outEnd++] = 'r';
        msglen = htonl(msglen);
        ss_rc = memcpy_s(temp_connections[i]->outBuffer + temp_connections[i]->outEnd,
            temp_connections[i]->outSize - temp_connections[i]->outEnd - 1,
            &msglen,
            sizeof(int));
        securec_check(ss_rc, "\0", "\0");
        temp_connections[i]->outEnd += 4;
        ss_rc = memcpy_s(temp_connections[i]->outBuffer + temp_connections[i]->outEnd,
            temp_connections[i]->outSize - temp_connections[i]->outEnd,
            &queryId,
            sizeof(uint64));
        securec_check(ss_rc, "\0", "\0");
        temp_connections[i]->outEnd += sizeof(uint64);

        ss_rc = memcpy_s(temp_connections[i]->outBuffer + temp_connections[i]->outEnd,
                         temp_connections[i]->outSize - temp_connections[i]->outEnd,
                         &countOID,
                         sizeof(int));
        securec_check(ss_rc, "\0", "\0");
        temp_connections[i]->outEnd += sizeof(int);
        ListCell *cell;
        foreach (cell, u_sess->spq_cxt.direct_read_map) {
            SpqDirectReadEntry *entry = (SpqDirectReadEntry *)lfirst(cell);
            ss_rc = memcpy_s(temp_connections[i]->outBuffer + temp_connections[i]->outEnd,
                             temp_connections[i]->outSize - temp_connections[i]->outEnd,
                             &(entry->rel_id), sizeof(Oid));
            securec_check(ss_rc, "\0", "\0");
            temp_connections[i]->outEnd += sizeof(Oid);
        }

        if (pgxc_node_flush(temp_connections[i]) != 0) {
            temp_connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                    errmsg("Failed to send query ID to %s while sending query ID with sync",
                        temp_connections[i]->remoteNodeName)));
        }

        temp_connections[i]->state = DN_CONNECTION_STATE_QUERY;
    }

    combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

    while (conn_count > 0) {
        if (pgxc_node_receive(conn_count, temp_connections, NULL)) {
            int error_code;
            char* error_msg = getSocketError(&error_code);

            ereport(ERROR,
                (errcode(error_code),
                    errmsg("Failed to read response from Datanodes while sending query ID with sync. Detail: %s\n",
                        error_msg)));
        }
        i = 0;
        while (i < conn_count) {
            int res = handle_response(temp_connections[i], combiner);
            if (res == RESPONSE_EOF) {
                i++;
            } else if (res == RESPONSE_PLANID_OK) {
                if (--conn_count > i)
                    temp_connections[i] = temp_connections[conn_count];
            } else {
                temp_connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Unexpected response from %s while sending query ID with sync",
                            temp_connections[i]->remoteNodeName),
                        errdetail("%s", (combiner->errorMessage == NULL) ? "none" : combiner->errorMessage)));
            }
        }
        /* report error if any */
        pgxc_node_report_error(combiner);
    }

    ValidateAndCloseCombiner(combiner);
    pfree_ext(temp_connections);
}

bool pgxc_start_command_on_connection(
    PGXCNodeHandle* connection, RemoteQueryState* remotestate, Snapshot snapshot,
    const char* compressedPlan, int cLen)
{
    CommandId cid;
    RemoteQuery* step = (RemoteQuery*)remotestate->ss.ps.plan;
    bool trigger_ship = false;

    /*
     * When enable_stream_operator = off;
     * The current mechanism has such a problem that a CN will split complex SQL into multiple simple SQL and send it to
     * the DN for execution. In order to ensure data consistency, the DN needs to use the same Snapshot for visibility
     * judgment of such SQL. Therefore, the CN side sends such SQL identifier to the DN. Use
     * PlannedStmt->Max_push_sql_num records the maximum number of SQL statements split by a SQL statement.
     */
    int max_push_sqls = remotestate->ss.ps.state->es_plannedstmt->max_push_sql_num; /* max SQLs may send one DN */

    if (connection->state == DN_CONNECTION_STATE_QUERY)
        BufferConnection(connection);

    /*
     * Scan descriptor would be valid and would contain a valid snapshot
     * in cases when we need to send out of order command id to data node
     * e.g. in case of a fetch
     */
    TableScanDesc scanDesc = GetTableScanDesc(remotestate->ss.ss_currentScanDesc, remotestate->ss.ss_currentRelation);

    if (remotestate->cursor != NULL && remotestate->cursor[0] != '\0' && scanDesc != NULL &&
        scanDesc->rs_snapshot != NULL)
        cid = scanDesc->rs_snapshot->curcid;
    else {
        /*
         * An insert into a child by selecting form its parent gets translated
         * into a multi-statement transaction in which first we select from parent
         * and then insert into child, then select form child and insert into child.
         * The select from child should not see the just inserted rows.
         * The command id of the select from child is therefore set to
         * the command id of the insert-select query saved earlier.
         * Similarly a WITH query that updates a table in main query
         * and inserts a row in the same table in the WITH query
         * needs to make sure that the row inserted by the WITH query does
         * not get updated by the main query.
         */
        /* step->exec_nodes will be null in plan router/scan gather nodes */
        if (step->exec_nodes && step->exec_nodes->accesstype == RELATION_ACCESS_READ && step->rq_save_command_id)
            cid = remotestate->rqs_cmd_id;
        else
            // This "false" as passed-in parameter remains questionable, as it might affect the
            // future updates of CommandId in someway we are not aware of yet.
            cid = GetCurrentCommandId(false);
    }

    if (pgxc_node_send_cmd_id(connection, cid) < 0)
        return false;

    /* snapshot is not necessary to be sent to the compute pool */
    if (snapshot && pgxc_node_send_snapshot(connection, snapshot, max_push_sqls) && IS_PGXC_COORDINATOR)
        return false;

    /* wlm_cgroup is not necessary to be sent to the compute pool */
    if (ENABLE_WORKLOAD_CONTROL && *u_sess->wlm_cxt->control_group && IS_PGXC_COORDINATOR &&
        pgxc_node_send_wlm_cgroup(connection))
        return false;

    if (ENABLE_WORKLOAD_CONTROL && u_sess->attr.attr_resource.resource_track_level == RESOURCE_TRACK_OPERATOR &&
        pgxc_node_send_threadid(connection, t_thrd.proc_cxt.MyProcPid))
        return false;
#ifdef USE_SPQ
    if (pgxc_node_send_queryid(connection, remotestate->queryId))
        return false;
#else
    if (pgxc_node_send_queryid(connection, u_sess->debug_query_id))
        return false;
#endif

    // Instrumentation/Unique SQL: send unique sql id to DN node
    if (is_unique_sql_enabled() && pgxc_node_send_unique_sql_id(connection))
        return false;

    if (step->remote_query && step->remote_query->isRowTriggerShippable)
        trigger_ship = true;

    if ((step->statement && step->statement[0] != '\0') || step->cursor || remotestate->rqs_num_params) {
        /* need to use Extended Query Protocol */
        int fetch = 0;
        bool prepared = false;
        bool send_desc = false;

        if (step->base_tlist != NULL || step->has_row_marks ||
            (step->exec_nodes && step->exec_nodes->accesstype == RELATION_ACCESS_READ)) {
            send_desc = true;
        }

        /* if prepared statement is referenced see if it is already exist */
        if (step->statement && step->statement[0] != '\0') {
            if (step->is_simple) {
                prepared = HaveActiveCoordinatorPreparedStatement(step->statement);

                if (!prepared)
                    ereport(ERROR,
                        (errcode(ERRCODE_UNDEFINED_PSTATEMENT),
                            errmsg("prepared statement \"%s\" does not exist", step->statement)));
            } else
                prepared = ActivateDatanodeStatementOnNode(
                    step->statement, PGXCNodeGetNodeId(connection->nodeoid, PGXC_NODE_DATANODE));
        }

        /*
         * execute and fetch rows only if they will be consumed
         * immediately by the sorter
         */
        if (step->cursor)
            fetch = 1;

        if (step->is_simple) {
            if (pgxc_node_send_plan_with_params(connection,
                    step->sql_statement,
                    remotestate->rqs_num_params,
                    remotestate->rqs_param_types,
                    remotestate->paramval_len,
                    remotestate->paramval_data,
                    fetch) != 0)
                return false;
        } else {
            if (pgxc_node_send_query_extended(connection,
                    prepared ? NULL : step->sql_statement,
                    step->statement,
                    step->cursor,
                    remotestate->rqs_num_params,
                    remotestate->rqs_param_types,
                    remotestate->paramval_len,
                    remotestate->paramval_data,
                    send_desc,
                    fetch) != 0)
                return false;
        }
    } else {
        char* query = step->sql_statement;
        if (step->is_simple) {
            query = remotestate->serializedPlan;
        }
        if (pgxc_node_send_query(connection, query, false, false, trigger_ship, false, compressedPlan, cLen) != 0)
            return false;
    }
    return true;
}

/*
 * IsReturningDMLOnReplicatedTable
 *
 * This function returns true if the passed RemoteQuery
 * 1. Operates on a table that is replicated
 * 2. Represents a DML
 * 3. Has a RETURNING clause in it
 *
 * If the passed RemoteQuery has a non null base_tlist
 * means that DML has a RETURNING clause.
 */

static bool IsReturningDMLOnReplicatedTable(RemoteQuery* rq)
{
    if (IsExecNodesReplicated(rq->exec_nodes) && rq->base_tlist != NULL && /* Means DML has RETURNING */
        (rq->exec_nodes->accesstype == RELATION_ACCESS_UPDATE || rq->exec_nodes->accesstype == RELATION_ACCESS_INSERT))
        return true;

    return false;
}

static uint64 get_datasize_for_hdfsfdw(Plan* plan, int* filenum)
{
    int fnum = 0;

    Plan* fsplan = get_foreign_scan(plan);

    /* get private data from foreign scan node */
    ForeignScan* foreignScan = (ForeignScan*)fsplan;

    List* foreignPrivateList = (List*)foreignScan->fdw_private;

    if (0 == list_length(foreignPrivateList)) {
        return 0;
    }

    DfsPrivateItem* item = NULL;
    ListCell* lc = NULL;
    foreach (lc, foreignPrivateList) {
        DefElem* def = (DefElem*)lfirst(lc);

        if (0 == pg_strcasecmp(def->defname, DFS_PRIVATE_ITEM)) {
            item = (DfsPrivateItem*)def->arg;
        }
    }

    if (item == NULL) {
        return 0;
    } else if (!item->dnTask) {
        return 0;
    }

    uint64 totalSize = 0;

    foreach (lc, item->dnTask) {
        SplitMap* tmp_map = (SplitMap*)lfirst(lc);
        totalSize += tmp_map->totalSize;
        fnum += tmp_map->fileNums;
    }

    if (filenum != NULL)
        *filenum = fnum;

    return totalSize;
}

static uint64 get_datasize_for_gdsfdw(Plan* plan, int* filenum)
{
    uint64 totalSize = 0;
    int fnum = 0;

    Plan* fsplan = get_foreign_scan(plan);

    /* get private data from foreign scan node */
    ForeignScan* foreignScan = (ForeignScan*)fsplan;

    List* foreignPrivateList = (List*)foreignScan->fdw_private;

    if (0 == list_length(foreignPrivateList)) {
        return 0;
    }

    ListCell *lc = NULL, *lc1 = NULL, *lc2 = NULL;
    foreach (lc, foreignPrivateList) {
        DefElem* def = (DefElem*)lfirst(lc);

        if (0 == pg_strcasecmp(def->defname, optTaskList)) {
            List* oldDnTask = (List*)def->arg;
            foreach (lc1, oldDnTask) {
                DistFdwDataNodeTask* task = (DistFdwDataNodeTask*)lfirst(lc1);
                foreach (lc2, task->task) {
                    DistFdwFileSegment* segment = (DistFdwFileSegment*)lfirst(lc2);
                    if (segment->ObjectSize) {
                        totalSize += segment->ObjectSize;
                        fnum++;
                    }
                }
            }
        }
    }

    if (filenum != NULL)
        *filenum = fnum;

    return totalSize;
}

uint64 get_datasize(Plan* plan, int srvtype, int* filenum)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

/**
 * @Description: delete the other datanode task for current data node.
 * @in item, the given item, it repreants all node tasks.
 * @return if we find the current data node task, return true, otherwise
 * retuen false.
 */
bool handleDfsPrivateItemForComputPool(DfsPrivateItem* item)
{
    ListCell* lc = NULL;
    SplitMap* self_map = NULL;

    /* reset splitmap for this data ndoe */
    List* dnTask = item->dnTask;
    item->dnTask = NIL;

    foreach (lc, dnTask) {
        SplitMap* tmp_map = (SplitMap*)lfirst(lc);

        if ((u_sess->pgxc_cxt.PGXCNodeId == tmp_map->nodeId || LOCATOR_TYPE_REPLICATED == tmp_map->locatorType) &&
            NIL != tmp_map->splits) {
            self_map = tmp_map;

            break;
        }
    }

    /*
     * In some scenarioes fileList would be NIL. Take a example:
     * In a cluster, we have M DNs, the table file count we read
     * in OBS/HDFS is N. When M < N, some DNs will be not assign
     * files to scan. Add this if statement to bypass the following
     * codes. The programer who changes this should take care about
     * this condition.
     */
    if (self_map == NULL) {
        return false;
    }

    /*
     * When we don't set producerDOP, it should be 1. In this condition
     * fileList should not be splitted into pieces shared between producer
     * threads.
     */
    if (u_sess->stream_cxt.producer_dop == 1) {
        item->dnTask = lappend(item->dnTask, self_map);
    } else {
        SplitMap* new_map = (SplitMap*)makeNode(SplitMap);

        // query on obs, more comments here to explain why set the flag.
        new_map->locatorType = LOCATOR_TYPE_REPLICATED;

        List* splits = list_copy(self_map->splits);
        int fileCnt = 0;
        foreach (lc, splits) {
            SplitInfo* si = (SplitInfo*)lfirst(lc);
            if ((fileCnt % u_sess->stream_cxt.producer_dop) != u_sess->stream_cxt.smp_id) {
                self_map->splits = list_delete(self_map->splits, si);
            }

            fileCnt++;
        }

        new_map->splits = list_copy(self_map->splits);
        new_map->fileNums = list_length(new_map->splits);

        if (new_map->splits == NIL)
            return false;

        item->dnTask = lappend(item->dnTask, new_map);
    }

    return true;
}

/**
 * @Description: delete the other datanode task for current data node.
 * @in dnTask, the given dnTask, it repreants all node tasks.
 * @return if we find the current data node task, return true, otherwise
 * retuen false.
 */
List* handleDistFdwDataNodeTaskForComputePool(List* dnTask)
{
    DistFdwDataNodeTask* task = NULL;
    ListCell* lc = NULL;
    List* taskList = NIL;
    foreach (lc, dnTask) {
        task = (DistFdwDataNodeTask*)lfirst(lc);
        if (0 == pg_strcasecmp(task->dnName, g_instance.attr.attr_common.PGXCNodeName)) {
            taskList = lappend(taskList, copyObject(task));
            break;
        }
    }

    return taskList;
}

/*
 * @Description: Just keep the splitmap which belongs to the data node, and
 *               remove all others. This function should be called in DataNode.
 *
 * @param[IN] plan :  the foreign scan node.
 * @return: false if this data node does nothing.
 */
static bool clean_splitmap(Plan* plan)
{
    Assert(plan);

    Assert(T_ForeignScan == nodeTag(plan) || T_VecForeignScan == nodeTag(plan));

    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));

    /* get private data from foreign scan node */
    ForeignScan* foreignScan = (ForeignScan*)plan;
    List* foreignPrivateList = (List*)foreignScan->fdw_private;

    if (0 == list_length(foreignPrivateList)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmodule(MOD_ACCELERATE), errmsg("foreignPrivateList is NULL")));
    }

    /*
     * find the splitmap which belongs to the data node, and reset
     * DfsPrivateItem for DFS fdw.
     * There is three listcell fdw_private for dist_fdw, it includes
     * convert_selectively, optTaskList, gt_sessionId. This function only
     * excute on datanode. Only the current datanode dntask is need to pushdow
     * to coodinator node of computing pool.
     */
    bool has_splits = false;

    ListCell* lc = NULL;
    foreach (lc, foreignPrivateList) {
        DefElem* def = (DefElem*)lfirst(lc);

        if (0 == pg_strcasecmp(def->defname, DFS_PRIVATE_ITEM)) {
            DfsPrivateItem* item = (DfsPrivateItem*)def->arg;
            has_splits = handleDfsPrivateItemForComputPool(item);
        } else if (0 == pg_strcasecmp(def->defname, optTaskList)) {
            List* dnTask = (List*)def->arg;
            def->arg = (Node*)handleDistFdwDataNodeTaskForComputePool(dnTask);
            list_free_ext(dnTask);
            has_splits = list_length(((List*)def->arg)) ? true : false;
        }
    }

    return has_splits;
}

/**
 * @Description: need to assign dnTask on coordinator node of computing pool
 * for dfs_fdw foreign table.
 * @in item, the given item, that includes the all files, which is form main
 * cluster.
 * @in dn_num, current data node number.
 * @return return new dn task.
 */
List* computePoolAssignDnTaskForFt(DfsPrivateItem* item, int dn_num)
{
    List* dnTask = item->dnTask;
    item->dnTask = NIL;

    /* make ONE empty SplitMap for each DN */
    List* new_maps = NIL;
    for (int i = 0; i < dn_num; i++) {
        SplitMap* new_map = (SplitMap*)makeNode(SplitMap);

        new_map->locatorType = LOCATOR_TYPE_REPLICATED;

        new_maps = lappend(new_maps, new_map);
    }

    /*
     * I'm sure that just ONE splitmap in dnTask, because dnTask has been
     * cleaned up in DWS DN by calling clean_splitmap()
     */
    SplitMap* old_map = (SplitMap*)linitial(dnTask);

    /* assign file(SplitInfo) in old_map to all DNs(SplitMap in new_maps */
    int index = 0;
    ListCell* lc = NULL;
    foreach (lc, old_map->splits) {
        SplitInfo* si = (SplitInfo*)lfirst(lc);

        if (index >= dn_num)
            index = 0;

        Assert(new_maps);

        SplitMap* map = (SplitMap*)list_nth(new_maps, index);

        map->splits = lappend(map->splits, si);

        index++;
    }

    return new_maps;
}

/**
 * @Description: need to assign dnTask on coordinator node of computing pool
 * for dfs_fdw foreign table.
 * @in oldDnTask, the given oldDnTask, that includes the all files, which is form main
 * cluster.
 * @in dn_num, current data node number.
 * @return return new dn task.
 */
List* computePoolAssignDnTaskForImportFt(List* oldDnTask)
{
    Assert(list_length(oldDnTask) == 1);

    List* dnNameList = PgxcNodeGetAllDataNodeNames();
    DistFdwDataNodeTask* oldDataNodetask = (DistFdwDataNodeTask*)linitial(oldDnTask);
    int nTasks = 0;
    DistFdwDataNodeTask* task = NULL;
    List* totalTask = NULL;
    ListCell* lc = NULL;
    int dnNum = list_length(dnNameList);

    if (list_length(oldDataNodetask->task) > dnNum) {
        nTasks = dnNum;
    } else {
        nTasks = list_length(oldDataNodetask->task);
    }

    for (int i = 0; i < nTasks; i++) {
        task = makeNode(DistFdwDataNodeTask);
        task->dnName = pstrdup((char*)list_nth(dnNameList, (i % dnNum)));
        totalTask = lappend(totalTask, task);
    }

    int num_processed = 0;
    foreach (lc, oldDataNodetask->task) {
        DistFdwFileSegment* segment = (DistFdwFileSegment*)lfirst(lc);

        task = (DistFdwDataNodeTask*)list_nth(totalTask, (num_processed % nTasks));
        task->task = lappend(task->task, segment);

        num_processed++;
    }

    return totalTask;
}

List* try_get_needed_dnnum(uint64 dnneeded)
{
    return get_dnlist(dnneeded);
}

/*
 * @Description:
Reassign the file in filelist to all DNs in the compute pool.
 *               This function should be called in the CN of the comptue pool.
 *
 * @param[IN] plan   :  the foreign scan node;
 * @param[IN] dn_num :  the number of the DN in the compute pool;
 * @return: the list of the SplitMap, the number of SplitMap == the number of DN
 */
static List* reassign_splitmap(Plan* plan, int dn_num)
{
    ListCell* lc = NULL;
    List* new_maps = NULL;
    Assert(plan && dn_num > 0);

    Assert(T_ForeignScan == nodeTag(plan) || T_VecForeignScan == nodeTag(plan));

    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));

    /* get private data from foreign scan node */
    ForeignScan* foreignScan = (ForeignScan*)plan;
    List* foreignPrivateList = (List*)foreignScan->fdw_private;

    if (0 == list_length(foreignPrivateList)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmodule(MOD_ACCELERATE), errmsg("foreignPrivateList is NULL")));
    }

    foreach (lc, foreignPrivateList) {
        DefElem* def = (DefElem*)lfirst(lc);

        if (0 == pg_strcasecmp(def->defname, DFS_PRIVATE_ITEM)) {
            DfsPrivateItem* item = (DfsPrivateItem*)def->arg;
            new_maps = computePoolAssignDnTaskForFt(item, dn_num);
        } else if (0 == pg_strcasecmp(def->defname, optTaskList)) {
            List* dnTask = (List*)def->arg;
            new_maps = computePoolAssignDnTaskForImportFt(dnTask);
            def->arg = (Node*)new_maps;
        }
    }

    return new_maps;
}

static void make_new_spiltmap(Plan* plan, SplitMap* map)
{
    Assert(T_ForeignScan == nodeTag(plan) || T_VecForeignScan == nodeTag(plan));

    ForeignScan* foreignScan = (ForeignScan*)plan;

    if (foreignScan->options->stype == T_TXT_CSV_OBS_SERVER) {
        return;
    }

    List* foreignPrivateList = (List*)foreignScan->fdw_private;
    if (0 == list_length(foreignPrivateList)) {
        ereport(
            ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmodule(MOD_HDFS), errmsg("foreignPrivateList is NULL")));
    }

    DfsPrivateItem* item = (DfsPrivateItem*)((DefElem*)linitial(foreignPrivateList))->arg;

    item->dnTask = NIL;
    item->dnTask = lappend(item->dnTask, map);
}

/*
 * @Description: set relid of RET in PlannedStmt to InvalidOid.
 *
 * @param[IN] ps : PlannedStmt*
 * @return: void
 */
static void resetRelidOfRTE(PlannedStmt* ps)
{
    ListCell* lc = NULL;

    foreach (lc, ps->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);

        if (rte->rtekind == RTE_RELATION) {
            rte->relid = InvalidOid;
            rte->ignoreResetRelid = true;
        }
    }
}

List* get_dnlist_for_hdfs(int fnum)
{
    static int origin_number = 0;

    List* dnlist = NIL;

    int dnnum = MIN(fnum, u_sess->pgxc_cxt.NumDataNodes);

    if (dnnum == u_sess->pgxc_cxt.NumDataNodes)
        return NIL;

    int start_dn_number = origin_number;

    if (start_dn_number >= u_sess->pgxc_cxt.NumDataNodes)
        start_dn_number %= u_sess->pgxc_cxt.NumDataNodes;

    int dnindex = 0;
    for (int i = 1; i <= dnnum; i++) {
        dnindex = start_dn_number + i;

        if (dnindex >= u_sess->pgxc_cxt.NumDataNodes)
            dnindex -= u_sess->pgxc_cxt.NumDataNodes;

        dnlist = lappend_int(dnlist, dnindex);
    }

    origin_number = dnindex;

    return dnlist;
}

void do_query_for_scangather(RemoteQueryState* node, bool vectorized)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

/**
 * @Description: fill the BloomFilter information to foreignscan node.
 * we need to Serialize it, and store it in foreignscan.
 * @in node, the RemoteQueryState.
 * @return none.
 */
void addBloomFilterSet(RemoteQueryState* node)
{
    EState* estate = node->ss.ps.state;
    RemoteQuery* step = (RemoteQuery*)node->ss.ps.plan;
    ForeignScan* fScan = NULL;
    List* bfIndex = NULL;
    int bfCount = 0;

    Plan* plan = step->scan.plan.lefttree;
    while (plan->lefttree) {
        plan = plan->lefttree;
    }

    Assert(IsA(plan, ForeignScan) || IsA(plan, VecForeignScan));

    fScan = (ForeignScan*)plan;

    bfIndex = ((Plan*)fScan)->filterIndexList;
    bfCount = list_length(((Plan*)fScan)->var_list);

    fScan->bfNum = bfCount;
    fScan->bloomFilterSet = (BloomFilterSet**)palloc0(sizeof(BloomFilterSet*) * fScan->bfNum);

    for (int bfNum = 0; bfNum < fScan->bfNum; bfNum++) {
        int idx = list_nth_int(bfIndex, bfNum);
        if (NULL != estate->es_bloom_filter.bfarray[idx]) {
            fScan->bloomFilterSet[bfNum] = estate->es_bloom_filter.bfarray[idx]->makeBloomFilterSet();
        }
    }
}

/* return true if nothing to to */
static bool internal_do_query_for_planrouter(RemoteQueryState* node, bool vectorized)
{
    Assert(IS_PGXC_DATANODE);

    ereport(DEBUG5, (errmodule(MOD_ACCELERATE), errmsg("in %s", __FUNCTION__)));

    RemoteQuery* step = (RemoteQuery*)node->ss.ps.plan;

    Assert(step->position == PLAN_ROUTER);

    PlannedStmt* planstmt = node->ss.ps.state->es_plannedstmt;
    GlobalTransactionId gxid = InvalidGlobalTransactionId;

    PGXCNodeAllHandles* pgxc_connections = NULL;
    PGXCNodeHandle** connections = NULL;
    int regular_conn_count = 0;

    /*
     * A openGauss node cannot run transactions while in recovery as
     * this operation needs transaction IDs. This is more a safety guard than anything else.
     */
    if (RecoveryInProgress())
        ereport(ERROR,
            (errcode(ERRCODE_RUN_TRANSACTION_DURING_RECOVERY),
                errmsg("cannot run transaction to remote nodes during recovery")));

    /*
     * Consider a test case
     *
     * create table rf(a int, b int) distributed by replication;
     * insert into rf values(1,2),(3,4) returning ctid;
     *
     * While inserting the first row do_query works fine, receives the returned
     * row from the first connection and returns it. In this iteration the other
     * datanodes also had returned rows but they have not yet been read from the
     * network buffers. On Next Iteration do_query does not enter the data
     * receiving loop because it finds that node->connections is not null.
     * It is therefore required to set node->connections to null here.
     */
    if (node->conn_count == 0)
        node->connections = NULL;

    /* just keep the files for the node itself */
    Plan* fsplan = get_foreign_scan(step->scan.plan.lefttree);
    if (!clean_splitmap(fsplan)) {
        return true;
    }

    addBloomFilterSet(node);

    ForeignOptions* options = ((ForeignScan*)fsplan)->options;
    ServerTypeOption srvtype = options->stype;

    /* collect coordinator get connection time */
    TRACK_START(node->ss.ps.plan->plan_node_id, GET_COMPUTE_POOL_CONNECTION);
    pgxc_connections = connect_compute_pool(srvtype);
    TRACK_END(node->ss.ps.plan->plan_node_id, GET_COMPUTE_POOL_CONNECTION);

    pgxc_palloc_net_ctl(1);

    Assert(pgxc_connections);
    connections = pgxc_connections->datanode_handles;
    regular_conn_count = 1;

    pfree_ext(pgxc_connections);

    /*
     * We save only regular connections, at the time we exit the function
     * we finish with the primary connection and deal only with regular
     * connections on subsequent invocations
     */
    node->node_count = regular_conn_count;

    /* assign gxid */
    gxid = GetCurrentTransactionIdIfAny();

#ifdef STREAMPLAN
    if (step->is_simple) {
        StringInfoData str_remoteplan;
        initStringInfo(&str_remoteplan);
        elog(DEBUG5,
            "DN Node Id %d, Thread ID:%lu, queryId: %lu, query: %s",
            u_sess->pgxc_cxt.PGXCNodeId,
            gs_thread_self(),
            planstmt->queryId,
            t_thrd.postgres_cxt.debug_query_string ? t_thrd.postgres_cxt.debug_query_string : "");

        planstmt->query_string =
            const_cast<char*>(t_thrd.postgres_cxt.debug_query_string ? t_thrd.postgres_cxt.debug_query_string : "");

        PlannedStmt* ps = (PlannedStmt*)copyObject(planstmt);
        resetRelidOfRTE(ps);

        ps->in_compute_pool = true;

        Plan* pushdown_plan = (Plan*)copyObject(step->scan.plan.lefttree);
        ForeignScan* scan_plan = (ForeignScan*)get_foreign_scan(pushdown_plan);
        scan_plan->in_compute_pool = true;
        scan_plan->errCache = NULL;

        SerializePlan(pushdown_plan, ps, &str_remoteplan, step->num_stream, step->num_gather, false);
        step->sql_statement = str_remoteplan.data;
    }
#endif

    Assert(u_sess->debug_query_id != 0);
    pgstat_report_queryid(u_sess->debug_query_id);

    for (int i = 0; i < regular_conn_count; i++) {
        if (srvtype == T_OBS_SERVER || srvtype == T_TXT_CSV_OBS_SERVER) {
            ComputePoolConfig** conf = get_cp_conninfo();

            char* file_format = getFTOptionValue(options->fOptions, OPTION_NAME_FORMAT);
            Assert(file_format);

            uint64 pl_size = conf[0]->pl;
            pl_size *= 1024;

            Index index = ((ForeignScan*)fsplan)->scan.scanrelid;
            RangeTblEntry* rte = (RangeTblEntry*)list_nth(planstmt->rtable, index - 1);

            if (!pg_strcasecmp(file_format, "orc"))
                pl_size = adjust_plsize(rte->relid, (uint64)fsplan->plan_width, pl_size, NULL);

            if (pgxc_node_send_userpl(connections[i], int64(pl_size)))
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Could not send user pl to CN of the compute pool: %s.",
                            connections[i]->connInfo.host.data)));
        }

        if (compute_node_begin(1, &connections[i], gxid))
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                    errmsg("Could not begin transaction on Datanodes %u.", connections[i]->nodeoid)));

        if (!pgxc_start_command_on_connection(connections[i], node, NULL)) {
            pfree_ext(connections);
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Failed to send command to Datanodes")));
        }
        connections[i]->combiner = node;

        // If send command to Datanodes successfully, outEnd must be 0.
        // So the outBuffer can be freed here and reset to default buffer size 16K.
        //
        Assert(connections[i]->outEnd == 0);
        ResetHandleOutBuffer(connections[i]);
    }

    do_query_for_first_tuple(node, vectorized, regular_conn_count, connections, NULL, NIL);

    /* reset */
    if (step->is_simple)
        step->sql_statement = NULL;

    return false;
}

bool do_query_for_planrouter(RemoteQueryState* node, bool vectorized)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

void do_query(RemoteQueryState* node, bool vectorized)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    RemoteQuery* step = (RemoteQuery*)node->ss.ps.plan;
    TupleTableSlot* scanslot = node->ss.ss_ScanTupleSlot;
    bool force_autocommit = step->force_autocommit;
    bool is_read_only = step->read_only;
    GlobalTransactionId gxid = InvalidGlobalTransactionId;
    Snapshot snapshot = GetActiveSnapshot();
    PGXCNodeHandle** connections = NULL;
    PGXCNodeHandle* primaryconnection = NULL;
    int i;
    int regular_conn_count = 0;
    bool need_tran_block = false;
    PGXCNodeAllHandles* pgxc_connections = NULL;

    /*
     * A openGauss node cannot run transactions while in recovery as
     * this operation needs transaction IDs. This is more a safety guard than anything else.
     */
    if (RecoveryInProgress())
        elog(ERROR, "cannot run transaction to remote nodes during recovery");

    /*
     * Remember if the remote query is accessing a temp object
     *
     * !! PGXC TODO Check if the is_temp flag is propogated correctly when a
     * remote join is reduced
     */
    if (step->is_temp)
        ExecSetTempObjectIncluded();

    /*
     * Consider a test case
     *
     * create table rf(a int, b int) distributed by replication;
     * insert into rf values(1,2),(3,4) returning ctid;
     *
     * While inserting the first row do_query works fine, receives the returned
     * row from the first connection and returns it. In this iteration the other
     * datanodes also had returned rows but they have not yet been read from the
     * network buffers. On Next Iteration do_query does not enter the data
     * receiving loop because it finds that node->connections is not null.
     * It is therefore required to set node->connections to null here.
     */
    if (node->conn_count == 0)
        node->connections = NULL;

    /*
     * Get connections for Datanodes only, utilities and DDLs
     * are launched in ExecRemoteUtility
     */
    pgxc_connections = get_exec_connections(node, step->exec_nodes, step->exec_type);

    if (step->exec_type == EXEC_ON_DATANODES) {
        connections = pgxc_connections->datanode_handles;
        regular_conn_count = pgxc_connections->dn_conn_count;
    } else if (step->exec_type == EXEC_ON_COORDS) {
        connections = pgxc_connections->coord_handles;
        regular_conn_count = pgxc_connections->co_conn_count;
    }

    primaryconnection = pgxc_connections->primary_handle;

    /* Primary connection is counted separately */
    if (primaryconnection)
        regular_conn_count--;

    pfree(pgxc_connections);

    /*
     * We save only regular connections, at the time we exit the function
     * we finish with the primary connection and deal only with regular
     * connections on subsequent invocations
     */
    node->node_count = regular_conn_count;

    if (force_autocommit || is_read_only)
        need_tran_block = false;
    else
        need_tran_block = true;
    /*
     * XXX We are forcing a transaction block for non-read-only every remote query. We can
     * get smarter here and avoid a transaction block if all of the following
     * conditions are true:
     *
     * 	- there is only one writer node involved in the transaction (including
     * 	the local node)
     * 	- the statement being executed on the remote writer node is a single
     * 	step statement. IOW, Coordinator must not send multiple queries to the
     * 	remote node.
     *
     * 	Once we have leak-proof mechanism to enforce these constraints, we
     * 	should relax the transaction block requirement.
     *
       need_tran_block = (!is_read_only && total_conn_count > 1) ||
                         (TransactionBlockStatusCode() == 'T');
     */

    elog(DEBUG1,
        "has primary = %s, regular_conn_count = %d, "
        "need_tran_block = %s",
        primaryconnection ? "true" : "false",
        regular_conn_count,
        need_tran_block ? "true" : "false");

    gxid = GetCurrentTransactionId();

    if (!GlobalTransactionIdIsValid(gxid)) {
        if (primaryconnection)
            pfree(primaryconnection);
        pfree(connections);
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to get next transaction ID")));
    }

    /* See if we have a primary node, execute on it first before the others */
    if (primaryconnection) {
        if (pgxc_node_begin(1, &primaryconnection, gxid, need_tran_block, is_read_only, PGXC_NODE_DATANODE))
            ereport(
                ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Could not begin transaction on primary Datanode.")));

        if (!pgxc_start_command_on_connection(primaryconnection, node, snapshot)) {
            pfree(connections);
            pfree(primaryconnection);
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to send command to Datanodes")));
        }
        Assert(node->combine_type == COMBINE_TYPE_SAME);

        /* Make sure the command is completed on the primary node */
        while (true) {
            int res;
            if (pgxc_node_receive(1, &primaryconnection, NULL))
                break;

            res = handle_response(primaryconnection, node);
            if (res == RESPONSE_COMPLETE)
                break;
            else if (res == RESPONSE_TUPDESC) {
                ExecSetSlotDescriptor(scanslot, node->tuple_desc);
                /*
                 * Now tuple table slot is responsible for freeing the
                 * descriptor
                 */
                node->tuple_desc = NULL;
                /*
                 * RemoteQuery node doesn't support backward scan, so
                 * randomAccess is false, neither we want this tuple store
                 * persist across transactions.
                 */
                node->tuplestorestate = tuplestore_begin_heap(false, false, work_mem);
                tuplestore_set_eflags(node->tuplestorestate, node->eflags);
            } else if (res == RESPONSE_DATAROW) {
                pfree(node->currentRow.msg);
                node->currentRow.msg = NULL;
                node->currentRow.msglen = 0;
                node->currentRow.msgnode = 0;
                continue;
            } else if (res == RESPONSE_EOF)
                continue;
            else
                ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Unexpected response from Datanode")));
        }
        /* report error if any */
        pgxc_node_report_error(node);
    }

    for (i = 0; i < regular_conn_count; i++) {
        if (pgxc_node_begin(1, &connections[i], gxid, need_tran_block, is_read_only, PGXC_NODE_DATANODE))
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Could not begin transaction on Datanodes.")));

        if (!pgxc_start_command_on_connection(connections[i], node, snapshot)) {
            pfree(connections);
            if (primaryconnection)
                pfree(primaryconnection);
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to send command to Datanodes")));
        }
        connections[i]->combiner = node;
    }

    if (step->cursor) {
        node->cursor_count = regular_conn_count;
        node->cursor_connections = (PGXCNodeHandle**)palloc(regular_conn_count * sizeof(PGXCNodeHandle*));
        memcpy(node->cursor_connections, connections, regular_conn_count * sizeof(PGXCNodeHandle*));
    }

    /*
     * Stop if all commands are completed or we got a data row and
     * initialized state node for subsequent invocations
     */
    while (regular_conn_count > 0 && node->connections == NULL) {
        int i = 0;

        if (pgxc_node_receive(regular_conn_count, connections, NULL)) {
            pfree(connections);
            if (primaryconnection)
                pfree(primaryconnection);
            if (node->cursor_connections)
                pfree(node->cursor_connections);

            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to read response from Datanodes")));
        }
        /*
         * Handle input from the Datanodes.
         * If we got a RESPONSE_DATAROW we can break handling to wrap
         * it into a tuple and return. Handling will be continued upon
         * subsequent invocations.
         * If we got 0, we exclude connection from the list. We do not
         * expect more input from it. In case of non-SELECT query we quit
         * the loop when all nodes finish their work and send ReadyForQuery
         * with empty connections array.
         * If we got EOF, move to the next connection, will receive more
         * data on the next iteration.
         */
        while (i < regular_conn_count) {
            int res = handle_response(connections[i], node);
            if (res == RESPONSE_EOF) {
                i++;
            } else if (res == RESPONSE_COMPLETE) {
                if (i < --regular_conn_count)
                    connections[i] = connections[regular_conn_count];
            } else if (res == RESPONSE_TUPDESC) {
                ExecSetSlotDescriptor(scanslot, node->tuple_desc);
                /*
                 * Now tuple table slot is responsible for freeing the
                 * descriptor
                 */
                node->tuple_desc = NULL;
                /*
                 * RemoteQuery node doesn't support backward scan, so
                 * randomAccess is false, neither we want this tuple store
                 * persist across transactions.
                 */
                node->tuplestorestate = tuplestore_begin_heap(false, false, work_mem);
                tuplestore_set_eflags(node->tuplestorestate, node->eflags);
            } else if (res == RESPONSE_DATAROW) {
                /*
                 * Got first data row, quit the loop
                 */
                node->connections = connections;
                node->conn_count = regular_conn_count;
                node->current_conn = i;
                break;
            } else
                ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Unexpected response from Datanode")));
        }
        /* report error if any */
        pgxc_node_report_error(node);
    }

    if (node->cursor_count) {
        node->conn_count = node->cursor_count;
        memcpy(connections, node->cursor_connections, node->cursor_count * sizeof(PGXCNodeHandle*));
        node->connections = connections;
    }
#endif
}

/*
 * the code in this function belongs to do_query() previously.
 * Now, split old do_query() into new do_query() and do_query_for_first_tuple()
 * for lower complexity.
 */
void do_query_for_first_tuple(RemoteQueryState* node, bool vectorized, int regular_conn_count,
    PGXCNodeHandle** connections, PGXCNodeHandle* primaryconnection, List* dummy_connections)
{
    RemoteQuery* step = (RemoteQuery*)node->ss.ps.plan;
    TupleTableSlot* scanslot = node->ss.ss_ScanTupleSlot;
    PlannedStmt* planstmt = node->ss.ps.state->es_plannedstmt;
    uint64 max_processed = 0;
    uint64 min_processed = PG_UINT64_MAX;
    Oid relid = (planstmt->resultRelations == NULL)
                    ? InvalidOid
                    : getrelid(list_nth_int((List*)linitial2(planstmt->resultRelations), 0), planstmt->rtable);
    bool compute_dn_oids = true;
    List* oids = NULL;
    errno_t rc = 0;

    /*
     * Stop if all commands are completed or we got a data row and
     * initialized state node for subsequent invocations
     */
    bool isFreeConn = true;
    while (regular_conn_count > 0 && node->connections == NULL) {
        int i = 0;
        struct timeval timeout;
        timeout.tv_sec = ERROR_CHECK_TIMEOUT;
        timeout.tv_usec = 0;

        /*
         * If need check the other errors after getting a normal communcation error,
         * set timeout first when coming to receive data again. If then get any poll
         * error, report the former cached error in combiner(RemoteQueryState).
         */
        if (pgxc_node_receive(regular_conn_count, connections, node->need_error_check ? &timeout : NULL)) {
            pfree_ext(connections);
            if (primaryconnection != NULL)
                pfree_ext(primaryconnection);
            if (node->cursor_connections)
                pfree_ext(node->cursor_connections);

            if (!node->need_error_check) {
                int error_code = 0;
                char* error_msg = getSocketError(&error_code);

                ereport(ERROR,
                    (errcode(error_code), errmsg("Failed to read response from Datanodes Detail: %s\n", error_msg == NULL ? "null" : error_msg)));
            } else {
                node->need_error_check = false;
                pgxc_node_report_error(node);
            }
        }

        /*
         * Handle input from the Datanodes.
         * If we got a RESPONSE_DATAROW we can break handling to wrap
         * it into a tuple and return. Handling will be continued upon
         * subsequent invocations.
         * If we got 0, we exclude connection from the list. We do not
         * expect more input from it. In case of non-SELECT query we quit
         * the loop when all nodes finish their work and send ReadyForQuery
         * with empty connections array.
         * If we got EOF, move to the next connection, will receive more
         * data on the next iteration.
         */
        while (i < regular_conn_count) {
            int res = handle_response(connections[i],
                node,
                (dummy_connections != NIL) ? list_member_ptr(dummy_connections, connections[i]) : false);
            if (res == RESPONSE_EOF) {
                i++;
            } else if (res == RESPONSE_COMPLETE) {
                /* the response is complete but the connection state is fatal */
                if (unlikely(connections[i]->state == DN_CONNECTION_STATE_ERROR_FATAL))
                    ereport(ERROR,
                        (errcode(ERRCODE_CONNECTION_EXCEPTION),
                            errmsg("FATAL state of connection to datanode %u", connections[i]->nodeoid)));

                if (u_sess->attr.attr_sql.table_skewness_warning_threshold < 1 && planstmt->commandType == CMD_INSERT &&
                    node->combine_type != COMBINE_TYPE_SAME) {
                    if (compute_dn_oids) {
                        RelationLocInfo* rlc = (relid == InvalidOid) ? NULL : GetRelationLocInfo(relid);
                        oids = (rlc == NULL) ? NULL : PgxcNodeGetDataNodeOids(rlc->nodeList);
                        compute_dn_oids = false;
                    }

                    if (node->rqs_cur_processed > max_processed)
                        max_processed = node->rqs_cur_processed;
                    if (node->rqs_cur_processed < min_processed) {
                        if (node->rqs_cur_processed > 0)
                            min_processed = node->rqs_cur_processed;
                        else if (list_member_int(oids, connections[i]->nodeoid))
                            min_processed = 0;
                    }
                }

                if (i < --regular_conn_count)
                    connections[i] = connections[regular_conn_count];
            } else if (res == RESPONSE_TUPDESC) {
                ExecSetSlotDescriptor(scanslot, node->tuple_desc);
                /*
                 * Now tuple table slot is responsible for freeing the
                 * descriptor
                 */
                node->tuple_desc = NULL;
                /*
                 * RemoteQuery node doesn't support backward scan, so
                 * randomAccess is false, neither we want this tuple store
                 * persist across transactions.
                 */
                node->tuplestorestate = tuplestore_begin_heap(false, false, u_sess->attr.attr_memory.work_mem);
                tuplestore_set_eflags(node->tuplestorestate, node->eflags);

                if (step->sort) {
                    SimpleSort* sort = step->sort;
                    node->connections = connections;
                    isFreeConn = false;
                    node->conn_count = regular_conn_count;

                    if (!vectorized)
                        node->tuplesortstate = tuplesort_begin_merge(scanslot->tts_tupleDescriptor,
                            sort->numCols,
                            sort->sortColIdx,
                            sort->sortOperators,
                            sort->sortCollations,
                            sort->nullsFirst,
                            node,
                            u_sess->attr.attr_memory.work_mem);
                    else
                        node->batchsortstate = batchsort_begin_merge(scanslot->tts_tupleDescriptor,
                            sort->numCols,
                            sort->sortColIdx,
                            sort->sortOperators,
                            sort->sortCollations,
                            sort->nullsFirst,
                            node,
                            u_sess->attr.attr_memory.work_mem);

                    /*
                     * Break the loop, do not wait for first row.
                     * Tuplesort module want to control node it is
                     * fetching rows from, while in this loop first
                     * row would be got from random node
                     */
                    break;
                }
            } else if (res == RESPONSE_DATAROW) {
                /*
                 * Got first data row, quit the loop
                 */
                node->connections = connections;
                isFreeConn = false;
                node->conn_count = regular_conn_count;
                node->current_conn = i;
                break;
            } else
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Unexpected response from Datanode %u", connections[i]->nodeoid)));
        }

        /* report error if any */
        pgxc_node_report_error(node);
    }

    /* Alarm table skewness warning if occurs. */
    if (u_sess->attr.attr_sql.table_skewness_warning_threshold < 1 && planstmt->commandType == CMD_INSERT &&
        node->combine_type != COMBINE_TYPE_SAME &&
        node->rqs_processed > (uint64)(u_sess->attr.attr_sql.table_skewness_warning_rows * planstmt->num_nodes) &&
        (max_processed - min_processed) >
            node->rqs_processed * u_sess->attr.attr_sql.table_skewness_warning_threshold) {
        char tableInfo[256] = {'\0'};
        char* dbName = NULL;
        errno_t ret = EOK;
        if (u_sess->proc_cxt.MyProcPort)
            dbName = u_sess->proc_cxt.MyProcPort->database_name;
        else
            dbName = "[unknown]";
        ret = sprintf_s(tableInfo, sizeof(tableInfo), "%s.%s", dbName, get_nsp_relname(relid));
        securec_check_ss_c(ret, "\0", "\0");
        if (get_rel_relkind(relid) != RELKIND_FOREIGN_TABLE && get_rel_relkind(relid) != RELKIND_STREAM)
            ereport(WARNING,
                (errmsg("Skewness occurs, table name: %s, min value: %lu, max value: %lu, sum value: %lu, avg value: "
                        "%lu, skew ratio: %.3lf",
                     tableInfo,
                     min_processed,
                     max_processed,
                     node->rqs_processed,
                     (list_length(oids) != 0) ? (node->rqs_processed / list_length(oids)) : 0,
                     (double)(max_processed - min_processed) / (double)(node->rqs_processed)),
                    errhint("Please check data distribution or modify warning threshold")));
        for (int j = 0; j < ALARM_RETRY_COUNT; j++) {
            report_table_skewness_alarm(ALM_AT_Fault, tableInfo);
        }
    }
    if (node->cursor_count && node->cursor_connections) {
        node->conn_count = node->cursor_count;
        rc = memcpy_s(connections,
            node->cursor_count * sizeof(PGXCNodeHandle*),
            node->cursor_connections,
            node->cursor_count * sizeof(PGXCNodeHandle*));
        securec_check(rc, "\0", "\0");
        node->connections = connections;
    }

    /* Must free connections if it does not used */
    if (isFreeConn) {
        pfree_ext(connections);
        connections = NULL;
    }
    if (dummy_connections != NULL)
        list_free_ext(dummy_connections);
}

TupleTableSlot* ExecRemoteQuery(RemoteQueryState* step)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
#else
        return ExecScan(&(node->ss), (ExecScanAccessMtd)RemoteQueryNext, (ExecScanRecheckMtd)RemoteQueryRecheck);
#endif
}

/*
 * RemoteQueryRecheck -- remote query routine to recheck a tuple in EvalPlanQual
 */
static bool RemoteQueryRecheck(RemoteQueryState* node, TupleTableSlot* slot)
{
    /*
     * Note that unlike IndexScan, RemoteQueryScan never use keys in tableam_scan_begin
     * (and this is very bad) - so, here we do not check are keys ok or not.
     */
    return true;
}
/*
 * Execute step of PGXC plan.
 * The step specifies a command to be executed on specified nodes.
 * On first invocation connections to the Datanodes are initialized and
 * command is executed. Further, as well as within subsequent invocations,
 * responses are received until step is completed or there is a tuple to emit.
 * If there is a tuple it is returned, otherwise returned NULL. The NULL result
 * from the function indicates completed step.
 * The function returns at most one tuple per invocation.
 */
static TupleTableSlot* RemoteQueryNext(ScanState* scan_node)
{
#ifdef USE_SPQ
    return SpqRemoteQueryNext(scan_node);
#endif
    PlanState* outerNode = NULL;

    RemoteQueryState* node = (RemoteQueryState*)scan_node;
    TupleTableSlot* scanslot = scan_node->ss_ScanTupleSlot;
    RemoteQuery* rq = (RemoteQuery*)node->ss.ps.plan;
    EState* estate = node->ss.ps.state;

    /*
     * Initialize tuples processed to 0, to make sure we don't re-use the
     * values from the earlier iteration of RemoteQueryNext(). For an FQS'ed
     * DML returning query, it may not get updated for subsequent calls.
     * because there won't be a HandleCommandComplete() call to update this
     * field.
     */
    node->rqs_processed = 0;

    if (!node->query_Done) {
        /* Fire BEFORE STATEMENT triggers just before the query execution */
        if (rq->remote_query)
            pgxc_rq_fire_bstriggers(node);

        if (rq->position == PLAN_ROUTER) {
            if (do_query_for_planrouter(node))
                return NULL;
        } else if (rq->position == SCAN_GATHER) {
            do_query_for_scangather(node);
        } else {
            do_query(node);
        }

        node->query_Done = true;
    }

    if (rq->position == PLAN_ROUTER && node->resource_error == true) {
        outerNode = outerPlanState(outerPlanState(node)); /* skip scangather node */
        scanslot = ExecProcNode(outerNode);
    } else if (node->update_cursor) {
        PGXCNodeAllHandles* all_dn_handles = get_exec_connections(node, NULL, EXEC_ON_DATANODES);
        close_node_cursors(all_dn_handles->datanode_handles, all_dn_handles->dn_conn_count, node->update_cursor);
        pfree_ext(node->update_cursor);
        node->update_cursor = NULL;
        pfree_pgxc_all_handles(all_dn_handles);
    } else if (rq->is_simple && node->tuplesortstate) {
        if (rq->sort->sortToStore) {
            Assert(node->tuplestorestate);
            Tuplestorestate* tuplestorestate = node->tuplestorestate;
            bool eof_tuplestore = tuplestore_ateof(tuplestorestate);

            /*
             * If we can fetch another tuple from the tuplestore, return it.
             */
            if (!eof_tuplestore) {
                /* RemoteQuery node doesn't support backward scans */
                if (!tuplestore_gettupleslot(tuplestorestate, true, false, scanslot))
                    eof_tuplestore = true;
            }

            /*
             * If there is no tuple in tuplestore, but there are tuples not be processed, we should sort them
             * and put it into tuplestore too.
             */
            if (eof_tuplestore && (!node->eof_underlying || (node->currentRow.msg != NULL))) {
                if (!tuplesort_gettupleslot_into_tuplestore(
                        (Tuplesortstate*)node->tuplesortstate, true, scanslot, NULL, node->tuplestorestate)) {
                    (void)ExecClearTuple(scanslot);
                    node->eof_underlying = true;
                }
            }

            if (eof_tuplestore && node->eof_underlying)
                (void)ExecClearTuple(scanslot);
        } else
            (void)tuplesort_gettupleslot((Tuplesortstate*)node->tuplesortstate, true, scanslot, NULL);
    } else if (rq->spool_no_data == true) {  // for simple remotequery, we just pass the data, no need to spool
        node->fetchTuple(node, scanslot, NULL);
    } else if (node->tuplestorestate) {
        /*
         * If we are not at the end of the tuplestore, try
         * to fetch a tuple from tuplestore.
         */
        Tuplestorestate* tuplestorestate = node->tuplestorestate;
        bool eof_tuplestore = tuplestore_ateof(tuplestorestate);

        /*
         * If we can fetch another tuple from the tuplestore, return it.
         */
        if (!eof_tuplestore) {
            /* RemoteQuery node doesn't support backward scans */
            if (!tuplestore_gettupleslot(tuplestorestate, true, false, scanslot))
                eof_tuplestore = true;
        }

        /*
         * Consider a test case
         *
         * create table ta1 (v1 int, v2 int);
         * insert into ta1 values(1,2),(2,3),(3,4);
         *
         * create table ta2 (v1 int, v2 int);
         * insert into ta2 values(1,2),(2,3),(3,4);
         *
         * select t1.ctid, t2.ctid,* from ta1 t1, ta2 t2
         * where t2.v2<=3 order by t1.v1;
         *          ctid  | ctid  | v1 | v2 | v1 | v2
         *         -------+-------+----+----+----+----
         * Row_1    (0,1) | (0,1) |  1 |  2 |  1 |  2
         * Row_2    (0,1) | (0,2) |  1 |  2 |  2 |  3
         * Row_3    (0,2) | (0,1) |  2 |  3 |  1 |  2
         * Row_4    (0,2) | (0,2) |  2 |  3 |  2 |  3
         * Row_5    (0,1) | (0,1) |  3 |  4 |  1 |  2
         * Row_6    (0,1) | (0,2) |  3 |  4 |  2 |  3
         *         (6 rows)
         *
         * Note that in the resulting join, we are getting one row of ta1 twice,
         * as shown by the ctid's in the results. Now consider this update
         *
         * update ta1 t1 set v2=t1.v2+10 from ta2 t2
         * where t2.v2<=3 returning t1.ctid,t1.v1 t1_v1, t1.v2 t1_v2;
         *
         * The first iteration of the update runs for Row_1, succeeds and
         * updates its ctid to say (0,3). In the second iteration for Row_2,
         * since the ctid of the row has already changed, fails to update any
         * row and hence do_query does not return any tuple. The FetchTuple
         * call in RemoteQueryNext hence fails and eof_underlying is set to true.
         * However in the third iteration for Row_3, the update succeeds and
         * returns a row, but since the eof_underlying is already set to true,
         * the RemoteQueryNext does not bother calling FetchTuple, we therefore
         * do not get more than one row returned as a result of the update
         * returning query. It is therefore required in RemoteQueryNext to call
         * FetchTuple in case do_query has copied a row in node->currentRow.msg.
         * Also we have to reset the eof_underlying flag every time
         * FetchTuple succeeds to clear any previously set status.
         */
        if (eof_tuplestore && (!node->eof_underlying || (node->currentRow.msg != NULL))) {
            /*
             * If tuplestore has reached its end but the underlying RemoteQueryNext() hasn't
             * finished yet, try to fetch another row.
             */
            if (node->fetchTuple(node, scanslot, NULL)) {
                /* See comments a couple of lines above */
                node->eof_underlying = false;
                /*
                 * Append a copy of the returned tuple to tuplestore.  NOTE: because
                 * the tuplestore is certainly in EOF state, its read position will
                 * move forward over the added tuple.  This is what we want.
                 */
                if (tuplestorestate && !TupIsNull(scanslot))
                    tuplestore_puttupleslot(tuplestorestate, scanslot);
            } else
                node->eof_underlying = true;
        }

        if (eof_tuplestore && node->eof_underlying)
            (void)ExecClearTuple(scanslot);
    } else
        (void)ExecClearTuple(scanslot);

    /* When finish remote query already, should better reset the flag. */
    if (TupIsNull(scanslot))
        node->need_error_check = false;

    /* report error if any */
    pgxc_node_report_error(node);

    /*
     * Now we know the query is successful. Fire AFTER STATEMENT triggers. Make
     * sure this is the last iteration of the query. If an FQS query has
     * RETURNING clause, this function can be called multiple times until we
     * return NULL.
     */
    if (TupIsNull(scanslot) && rq->remote_query)
        pgxc_rq_fire_astriggers(node);

    /*
     * If it's an FQSed DML query for which command tag is to be set,
     * then update estate->es_processed. For other queries, the standard
     * executer takes care of it; namely, in ExecModifyTable for DML queries
     * and ExecutePlan for SELECT queries.
     */
    if (rq->remote_query != NULL && rq->remote_query->canSetTag && !rq->rq_params_internal &&
        (rq->remote_query->commandType == CMD_INSERT || rq->remote_query->commandType == CMD_UPDATE ||
            rq->remote_query->commandType == CMD_DELETE || rq->remote_query->commandType == CMD_MERGE))
        estate->es_processed += node->rqs_processed;

    /*
     * We only handle stream plan && RemoteQuery as root && DML's tag here.
     * Other are handled by ExecModifyTable and ExecutePlan
     */
    if (rq->is_simple && (Plan*)rq == estate->es_plannedstmt->planTree &&
        (estate->es_plannedstmt->commandType == CMD_INSERT || estate->es_plannedstmt->commandType == CMD_UPDATE ||
            estate->es_plannedstmt->commandType == CMD_DELETE || estate->es_plannedstmt->commandType == CMD_MERGE))
        estate->es_processed += node->rqs_processed;

    /* early free the connection to the compute pool */
    if (TupIsNull(scanslot) && PLAN_ROUTER == rq->position) {
        release_conn_to_compute_pool();
    }

    return scanslot;
}

/*
 * @Description: Pack all un-completed connections together
 * @in node - remotequerystate info
 *
 * @out - int, active count
 */
int PackConnections(RemoteQueryState* node)
{
    int active_count = 0;
    RemoteQuery* step = (RemoteQuery*)node->ss.ps.plan;
    PGXCNodeAllHandles* pgxc_handles = NULL;

    /* refresh the connections due to connections switch */
    pgxc_handles = get_handles(step->exec_nodes->nodeList, NULL, false);
    node->connections = pgxc_handles->datanode_handles;

    for (int i = 0; i < pgxc_handles->dn_conn_count; i++) {
        if (node->connections[i] == NULL)
            continue;

        if (node->connections[i]->state == DN_CONNECTION_STATE_QUERY) {
            /* Buffer the connection first if it's combiner diffs from the current one */
            if (node->connections[i]->combiner != node) {
                BufferConnection(node->connections[i]);
                continue;
            } else {
                node->connections[active_count] = node->connections[i];
                active_count++;
            }
        }
    }

    return active_count;
}

inline static uint64 GetQueryidFromRemoteQuery(RemoteQueryState* node)
{
    if (node->ss.ps.state != NULL && node->ss.ps.state->es_plannedstmt != NULL) {
        return node->ss.ps.state->es_plannedstmt->queryId;
    } else {
        return 0;
    }
}

void ExecEndRemoteQuery(RemoteQueryState* step, bool pre_end)
{

#ifdef USE_SPQ
    return ExecEndSpqRemoteQuery(step, pre_end);
#endif
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    ListCell* lc = NULL;

    /* clean up the buffer */
    foreach (lc, node->rowBuffer) {
        RemoteDataRow dataRow = (RemoteDataRow)lfirst(lc);
        pfree(dataRow->msg);
    }
    list_free_deep(node->rowBuffer);

    node->current_conn = 0;
    while (node->conn_count > 0) {
        int res;
        PGXCNodeHandle* conn = node->connections[node->current_conn];

        /* throw away message */
        if (node->currentRow.msg) {
            pfree(node->currentRow.msg);
            node->currentRow.msg = NULL;
        }

        if (conn == NULL) {
            node->conn_count--;
            continue;
        }

        /* no data is expected */
        if (conn->state == DN_CONNECTION_STATE_IDLE || conn->state == DN_CONNECTION_STATE_ERROR_FATAL) {
            if (node->current_conn < --node->conn_count)
                node->connections[node->current_conn] = node->connections[node->conn_count];
            continue;
        }
        res = handle_response(conn, node);
        if (res == RESPONSE_EOF) {
            struct timeval timeout;
            timeout.tv_sec = END_QUERY_TIMEOUT;
            timeout.tv_usec = 0;

            if (pgxc_node_receive(1, &conn, &timeout))
                ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("Failed to read response from Datanodes when ending query")));
        }
    }

    if (node->tuplestorestate != NULL)
    	(void)ExecClearTuple(node->ss.ss_ScanTupleSlot);
    /*
     * Release tuplestore resources
     */
    if (node->tuplestorestate != NULL)
        tuplestore_end(node->tuplestorestate);
    node->tuplestorestate = NULL;

    /*
     * If there are active cursors close them
     */
    if (node->cursor || node->update_cursor) {
        PGXCNodeAllHandles* all_handles = NULL;
        PGXCNodeHandle** cur_handles;
        bool bFree = false;
        int nCount;
        int i;

        cur_handles = node->cursor_connections;
        nCount = node->cursor_count;

        for (i = 0; i < node->cursor_count; i++) {
            if (node->cursor_connections == NULL || node->cursor_connections[i]->sock == -1) {
                bFree = true;
                all_handles = get_exec_connections(node, NULL, EXEC_ON_DATANODES);
                cur_handles = all_handles->datanode_handles;
                nCount = all_handles->dn_conn_count;
                break;
            }
        }

        if (node->cursor) {
            close_node_cursors(cur_handles, nCount, node->cursor);
            pfree(node->cursor);
            node->cursor = NULL;
        }

        if (node->update_cursor) {
            close_node_cursors(cur_handles, nCount, node->update_cursor);
            pfree(node->update_cursor);
            node->update_cursor = NULL;
        }

        if (bFree)
            pfree_pgxc_all_handles(all_handles);
    }

    /*
     * Clean up parameters if they were set
     */
    if (node->paramval_data) {
        pfree(node->paramval_data);
        node->paramval_data = NULL;
        node->paramval_len = 0;
    }

    /* Free the param types if they are newly allocated */
    if (node->rqs_param_types && node->rqs_param_types != ((RemoteQuery*)node->ss.ps.plan)->rq_param_types) {
        pfree(node->rqs_param_types);
        node->rqs_param_types = NULL;
        node->rqs_num_params = 0;
    }

    if (node->ss.ss_currentRelation)
        ExecCloseScanRelation(node->ss.ss_currentRelation);

    CloseCombiner(node);
#endif
}

static void close_node_cursors(PGXCNodeHandle** connections, int conn_count, const char* cursor)
{
    int i;
    RemoteQueryState* combiner = NULL;

    for (i = 0; i < conn_count; i++) {
        if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
            BufferConnection(connections[i]);
        if (pgxc_node_send_close(connections[i], false, cursor) != 0)
            ereport(WARNING,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                    errmsg("Failed to close Datanode %u cursor", connections[i]->nodeoid)));
        if (pgxc_node_send_sync(connections[i]) != 0)
            ereport(WARNING,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                    errmsg("Failed to close Datanode %u cursor", connections[i]->nodeoid)));
    }

    combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

    while (conn_count > 0) {
        if (pgxc_node_receive(conn_count, connections, NULL))
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Failed to close Datanode cursor")));
        i = 0;
        while (i < conn_count) {
            int res = handle_response(connections[i], combiner);
            if (res == RESPONSE_EOF) {
                i++;
            } else if (res == RESPONSE_COMPLETE) {
                if (--conn_count > i)
                    connections[i] = connections[conn_count];
            } else {
                // Unexpected response, ignore?
            }
        }
    }

    (void)ValidateAndCloseCombiner(combiner);
}

/*
 * Encode parameter values to format of DataRow message (the same format is
 * used in Bind) to prepare for sending down to Datanodes.
 * The data row is copied to RemoteQueryState.paramval_data.
 */
void SetDataRowForExtParams(ParamListInfo paraminfo, RemoteQueryState* rq_state)
{
    StringInfoData buf;
    uint16 n16;
    int i;
    int real_num_params = 0;
    RemoteQuery* node = (RemoteQuery*)rq_state->ss.ps.plan;

    /* If there are no parameters, there is no data to BIND. */
    if (!paraminfo)
        return;

    /*
     * If this query has been generated internally as a part of two-step DML
     * statement, it uses only the internal parameters for input values taken
     * from the source data, and it never uses external parameters. So even if
     * parameters were being set externally, they won't be present in this
     * statement (they might be present in the source data query). In such
     * case where parameters refer to the values returned by SELECT query, the
     * parameter data and parameter types would be set in SetDataRowForIntParams().
     */
    if (node->rq_params_internal)
        return;

    Assert(!rq_state->paramval_data);

    /*
     * It is necessary to fetch parameters
     * before looking at the output value.
     */
    for (i = 0; i < paraminfo->numParams; i++) {
        ParamExternData* param = NULL;

        param = &paraminfo->params[i];

        if (!OidIsValid(param->ptype) && paraminfo->paramFetch != NULL)
            (*paraminfo->paramFetch)(paraminfo, i + 1);

        /*
         * This is the last parameter found as useful, so we need
         * to include all the previous ones to keep silent the remote
         * nodes. All the parameters prior to the last usable having no
         * type available will be considered as NULL entries.
         */
        if (OidIsValid(param->ptype))
            real_num_params = i + 1;
    }

    /*
     * If there are no parameters available, simply leave.
     * This is possible in the case of a query called through SPI
     * and using no parameters.
     */
    if (real_num_params == 0) {
        rq_state->paramval_data = NULL;
        rq_state->paramval_len = 0;
        return;
    }

    initStringInfo(&buf);

    /* Number of parameter values */
    n16 = htons(real_num_params);
    appendBinaryStringInfo(&buf, (char*)&n16, 2);

    /* Parameter values */
    for (i = 0; i < real_num_params; i++) {
        ParamExternData* param = &paraminfo->params[i];
        uint32 n32;

        /*
         * Parameters with no types are considered as NULL and treated as integer
         * The same trick is used for dropped columns for remote DML generation.
         */
        if (param->isnull || !OidIsValid(param->ptype)) {
            n32 = htonl(~0);
            appendBinaryStringInfo(&buf, (char*)&n32, 4);
        } else {
            Oid typOutput;
            bool typIsVarlena = false;
            Datum pval;
            char* pstring = NULL;
            int len;

            /* Get info needed to output the value */
            getTypeOutputInfo(param->ptype, &typOutput, &typIsVarlena);

            /*
             * If we have a toasted datum, forcibly detoast it here to avoid
             * memory leakage inside the type's output routine.
             */
            if (typIsVarlena)
                pval = PointerGetDatum(PG_DETOAST_DATUM(param->value));
            else
                pval = param->value;

            /* Convert Datum to string */
            pstring = OidOutputFunctionCall(typOutput, pval);

            /* copy data to the buffer */
            len = strlen(pstring);
            n32 = htonl(len);
            appendBinaryStringInfo(&buf, (char*)&n32, 4);
            appendBinaryStringInfo(&buf, pstring, len);
        }
    }

    /*
     * If parameter types are not already set, infer them from
     * the paraminfo.
     */
    if (node->rq_num_params > 0) {
        /*
         * Use the already known param types for BIND. Parameter types
         * can be already known when the same plan is executed multiple
         * times.
         */
        if (node->rq_num_params != real_num_params)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Number of user-supplied parameters do not match the number of remote parameters")));
        rq_state->rqs_num_params = node->rq_num_params;
        rq_state->rqs_param_types = node->rq_param_types;
    } else {
        rq_state->rqs_num_params = real_num_params;
        rq_state->rqs_param_types = (Oid*)palloc(sizeof(Oid) * real_num_params);
        for (i = 0; i < real_num_params; i++)
            rq_state->rqs_param_types[i] = paraminfo->params[i].ptype;
    }

    /* Assign the newly allocated data row to paramval */
    rq_state->paramval_data = buf.data;
    rq_state->paramval_len = buf.len;
}

/* ----------------------------------------------------------------
 *		ExecRemoteQueryReScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void ExecRemoteQueryReScan(RemoteQueryState* node, ExprContext* exprCtxt)
{
#if !defined(ENABLE_MULTIPLE_NODES) && !defined(USE_SPQ)
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    /*
     * If the materialized store is not empty, just rewind the stored output.
     */
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

    if (!node->tuplestorestate)
        return;

    tuplestore_rescan(node->tuplestorestate);
#endif
}

void FreeParallelFunctionState(ParallelFunctionState* state)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

void StrategyFuncSum(ParallelFunctionState* state)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

ParallelFunctionState* RemoteFunctionResultHandler(char* sql_statement, ExecNodes* exec_nodes, strategy_func function,
    bool read_only, RemoteQueryExecType exec_type, bool non_check_count,
    bool need_tran_block, bool need_transform_anyarray, bool active_nodes_only)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * ExecRemoteFunctionInParallel, it executes function statement on all DN/CN
 * nodes in parallel and fetch results from all of them.
 *
 * We call this function in relfilenode swap at the end of resizing stage on one local table
 */
static void ExecRemoteFunctionInParallel(
    ParallelFunctionState* state, RemoteQueryExecType exec_remote_type, bool non_check_count)
{
    GlobalTransactionId gxid = InvalidGlobalTransactionId;
    Snapshot snapshot = GetActiveSnapshot();
    RemoteQueryState* remotestate = NULL;
    PGXCNodeAllHandles* pgxc_connections = NULL;
    PGXCNodeHandle** dn_connections = NULL;
    PGXCNodeHandle** cn_connections = NULL;
    int dn_conn_count = 0;
    int cn_conn_count = 0;
    int i = 0;

#define exec_on_datanode(type) (EXEC_ON_ALL_NODES == (type) || EXEC_ON_DATANODES == (type))
#define exec_on_coordinator(type) (EXEC_ON_ALL_NODES == (type) || EXEC_ON_COORDS == (type))

    if (!state->read_only)
        gxid = GetCurrentTransactionId();

    /* If no Datanodes defined, the query cannot be launched */
    if (u_sess->pgxc_cxt.NumDataNodes == 0 && exec_on_datanode(exec_remote_type) && !IS_SINGLE_NODE) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("No Datanode defined in cluster"),
                errhint("Need to define at least 1 Datanode with CREATE NODE.")));
    }

    if (u_sess->pgxc_cxt.NumCoords == 0 && exec_on_coordinator(exec_remote_type)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("No coordinator nodes defined in cluster"),
                errhint("Need to define at least 1 Coordinator with CREATE NODE.")));
    }

    remotestate = CreateResponseCombiner(0, non_check_count ? COMBINE_TYPE_NONE : COMBINE_TYPE_SAME);
    pgxc_connections = get_exec_connections(NULL, state->exec_nodes, exec_remote_type);
    dn_connections = pgxc_connections->datanode_handles;
    dn_conn_count = pgxc_connections->dn_conn_count;
    /* cn_connections maybe NULL, because we may have no other cn. */
    cn_connections = pgxc_connections->coord_handles;
    cn_conn_count = pgxc_connections->co_conn_count;

    state->tupstore = tuplestore_begin_heap(false, false, u_sess->attr.attr_memory.work_mem);

    if (exec_on_datanode(exec_remote_type)) {
        /* send execute info to datanodes */
        /* The query_id parameter is added to facilitate DFX fault locating. */
        if (pgxc_node_begin(dn_conn_count, dn_connections, gxid, false, state->read_only, PGXC_NODE_DATANODE, true)) {
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Could not begin transaction on Datanodes")));
        }

        for (i = 0; i < dn_conn_count; i++) {
            if (dn_connections[i]->state == DN_CONNECTION_STATE_QUERY)
                BufferConnection(dn_connections[i]);

            if (snapshot && pgxc_node_send_snapshot(dn_connections[i], snapshot)) {
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Failed to send snapshot to %s", dn_connections[i]->remoteNodeName)));
            }

            if (pgxc_node_send_query(dn_connections[i], state->sql_statement) != 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Failed to send command to %s", dn_connections[i]->remoteNodeName)));
            }
        }
    }

    if (exec_on_coordinator(exec_remote_type)) {
        /* send execute info to coordinators */
        if (pgxc_node_begin(cn_conn_count, cn_connections, gxid, false, state->read_only, PGXC_NODE_COORDINATOR)) {
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Could not begin transaction on Coordinator nodes")));
        }

        for (i = 0; i < cn_conn_count; i++) {
            if (cn_connections[i]->state == DN_CONNECTION_STATE_QUERY)
                BufferConnection(cn_connections[i]);

            if (snapshot && pgxc_node_send_snapshot(cn_connections[i], snapshot)) {
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Failed to send snapshot to %s", cn_connections[i]->remoteNodeName)));
            }

            if (pgxc_node_send_query(cn_connections[i], state->sql_statement) != 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Failed to send command to %s", cn_connections[i]->remoteNodeName)));
            }
        }
    }

    /* Parallel collect data from datanode through fetch tuple by multi channel. */
    TupleTableSlot* slot = MakeTupleTableSlot();
    remotestate->conn_count = dn_conn_count;
    remotestate->connections = dn_connections;
    (void)FetchTupleByMultiChannel<false, true>(remotestate, slot, state);

    /* Parallel collect data from coordinator through fetch tuple by multi channel. */
    remotestate->conn_count = cn_conn_count;
    remotestate->connections = cn_connections;
    (void)FetchTupleByMultiChannel<false, true>(remotestate, slot, state);
}

/*
 * Execute utility statement on multiple Datanodes
 * It does approximately the same as
 *
 * RemoteQueryState *state = ExecInitRemoteQuery(plan, estate, flags);
 * Assert(TupIsNull(ExecRemoteQuery(state));
 * ExecEndRemoteQuery(state)
 *
 * But does not need an Estate instance and does not do some unnecessary work,
 * like allocating tuple slots.
 */
void ExecRemoteUtility(RemoteQuery* node)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else

    RemoteQueryState* remotestate = NULL;
    bool force_autocommit = node->force_autocommit;
    RemoteQueryExecType exec_type = node->exec_type;
    GlobalTransactionId gxid = InvalidGlobalTransactionId;
    Snapshot snapshot = GetActiveSnapshot();
    PGXCNodeAllHandles* pgxc_connections;
    int co_conn_count;
    int dn_conn_count;
    bool need_tran_block = false;
    ExecDirectType exec_direct_type = node->exec_direct_type;
    int i;

    if (!force_autocommit)
        RegisterTransactionLocalNode(true);

    /*
     * It is possible to invoke create table with inheritance on
     * temporary objects. Remember that we might have accessed a temp object
     */
    if (node->is_temp)
        ExecSetTempObjectIncluded();

    remotestate = CreateResponseCombiner(0, node->combine_type);

    pgxc_connections = get_exec_connections(NULL, node->exec_nodes, exec_type);

    dn_conn_count = pgxc_connections->dn_conn_count;
    co_conn_count = pgxc_connections->co_conn_count;

    if (force_autocommit)
        need_tran_block = false;
    else
        need_tran_block = true;

    /* Commands launched through EXECUTE DIRECT do not need start a transaction */
    if (exec_direct_type == EXEC_DIRECT_UTILITY) {
        need_tran_block = false;

        /* This check is not done when analyzing to limit dependencies */
        if (IsTransactionBlock())
            ereport(ERROR,
                (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
                    errmsg("cannot run EXECUTE DIRECT with utility inside a transaction block")));
    }

    gxid = GetCurrentTransactionId();
    if (!GlobalTransactionIdIsValid(gxid))
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to get next transaction ID")));

    if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_DATANODES) {
        if (pgxc_node_begin(
                dn_conn_count, pgxc_connections->datanode_handles, gxid, need_tran_block, false, PGXC_NODE_DATANODE))
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Could not begin transaction on Datanodes")));
        for (i = 0; i < dn_conn_count; i++) {
            PGXCNodeHandle* conn = pgxc_connections->datanode_handles[i];

            if (conn->state == DN_CONNECTION_STATE_QUERY)
                BufferConnection(conn);
            if (snapshot && pgxc_node_send_snapshot(conn, snapshot)) {
                ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to send command to Datanodes")));
            }
            if (pgxc_node_send_query(conn, node->sql_statement) != 0) {
                ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to send command to Datanodes")));
            }
        }
    }

    if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_COORDS) {
        if (pgxc_node_begin(
                co_conn_count, pgxc_connections->coord_handles, gxid, need_tran_block, false, PGXC_NODE_COORDINATOR))
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Could not begin transaction on coordinators")));
        /* Now send it to Coordinators if necessary */
        for (i = 0; i < co_conn_count; i++) {
            if (snapshot && pgxc_node_send_snapshot(pgxc_connections->coord_handles[i], snapshot)) {
                ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to send command to coordinators")));
            }
            if (pgxc_node_send_query(pgxc_connections->coord_handles[i], node->sql_statement) != 0) {
                ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to send command to coordinators")));
            }
        }
    }

    /*
     * Stop if all commands are completed or we got a data row and
     * initialized state node for subsequent invocations
     */
    if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_DATANODES) {
        while (dn_conn_count > 0) {
            int i = 0;

            if (pgxc_node_receive(dn_conn_count, pgxc_connections->datanode_handles, NULL))
                break;
            /*
             * Handle input from the Datanodes.
             * We do not expect Datanodes returning tuples when running utility
             * command.
             * If we got EOF, move to the next connection, will receive more
             * data on the next iteration.
             */
            while (i < dn_conn_count) {
                PGXCNodeHandle* conn = pgxc_connections->datanode_handles[i];
                int res = handle_response(conn, remotestate);
                if (res == RESPONSE_EOF) {
                    i++;
                } else if (res == RESPONSE_COMPLETE) {
                    if (i < --dn_conn_count)
                        pgxc_connections->datanode_handles[i] = pgxc_connections->datanode_handles[dn_conn_count];
                } else if (res == RESPONSE_TUPDESC) {
                    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Unexpected response from Datanode")));
                } else if (res == RESPONSE_DATAROW) {
                    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Unexpected response from Datanode")));
                }
            }
        }
    }

    /* Make the same for Coordinators */
    if (exec_type == EXEC_ON_ALL_NODES || exec_type == EXEC_ON_COORDS) {
        while (co_conn_count > 0) {
            int i = 0;

            if (pgxc_node_receive(co_conn_count, pgxc_connections->coord_handles, NULL))
                break;

            while (i < co_conn_count) {
                int res = handle_response(pgxc_connections->coord_handles[i], remotestate);
                if (res == RESPONSE_EOF) {
                    i++;
                } else if (res == RESPONSE_COMPLETE) {
                    if (i < --co_conn_count)
                        pgxc_connections->coord_handles[i] = pgxc_connections->coord_handles[co_conn_count];
                } else if (res == RESPONSE_TUPDESC) {
                    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Unexpected response from coordinator")));
                } else if (res == RESPONSE_DATAROW) {
                    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Unexpected response from coordinator")));
                }
            }
        }
    }
    /*
     * We have processed all responses from nodes and if we have
     * error message pending we can report it. All connections should be in
     * consistent state now and so they can be released to the pool after ROLLBACK.
     */
    pgxc_node_report_error(remotestate);
#endif
}
void ExecRemoteUtility_ParallelDDLMode(RemoteQuery* node, const char* FirstExecNode)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

void ExecRemoteUtilityParallelBarrier(const RemoteQuery* node, const char* firstExecNode)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}


void CheckRemoteUtiltyConn(PGXCNodeHandle* conn, Snapshot snapshot)
{
    if (pgxc_node_send_cmd_id(conn, GetCurrentCommandId(true)) < 0) {
        ereport(
            ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Failed to send cid to Datanode %u", conn->nodeoid)));
    }

    if (snapshot && pgxc_node_send_snapshot(conn, snapshot)) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Failed to send snapshot to Datanode %u", conn->nodeoid)));
    }

    if (ENABLE_WORKLOAD_CONTROL && *u_sess->wlm_cxt->control_group && pgxc_node_send_wlm_cgroup(conn)) {
        ereport(WARNING,
            (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Failed to send cgroup to Datanode %u", conn->nodeoid)));
    }

    if (pgxc_node_send_queryid(conn, u_sess->debug_query_id) != 0) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Failed to send queryid to Datanode %u", conn->nodeoid)));
    }
}

/**
 * @global stats
 * @Description: receive estimate or real total row count from dn.
 *
 * @in dn_conn_count - count of target datanodes connection
 * @in pgxc_connections - target datanodes connection information
 * @in remotestate - state of remote query node
 * @out totalRowCnts - receive estimate total row count, save in it and return.
 * @return: void
 *
 */
static void recv_totalrowcnt_from_dn(int dn_conn_count, PGXCNodeAllHandles* pgxc_connections,
    RemoteQueryState* remotestate, ANALYZE_RQTYPE arq_type, VacuumStmt* stmt, AnalyzeMode eAnalyzeMode)
{
    /*
     * Stop if all commands are completed or we got a data row and
     * initialized state node for subsequent invocations
     */
    int i;
    int deadblock_dn_num = 0;

    /* Reset totalRowCnts */
    if (eAnalyzeMode == ANALYZENORMAL) {
        stmt->pstGlobalStatEx[ANALYZENORMAL].totalRowCnts = 0;
        stmt->pstGlobalStatEx[ANALYZENORMAL].topRowCnts = 0;
        stmt->pstGlobalStatEx[ANALYZENORMAL].topMemSize = 0;
    }

    if (remotestate->request_type == REQUEST_TYPE_NOT_DEFINED)
        remotestate->request_type = REQUEST_TYPE_QUERY;

    while (dn_conn_count > 0) {
        i = 0;

        if (pgxc_node_receive(dn_conn_count, pgxc_connections->datanode_handles, NULL)) {
            int error_code;
            char* error_msg = getSocketError(&error_code);
            pfree_ext(pgxc_connections->datanode_handles);

            ereport(
                ERROR, (errcode(error_code), errmsg("Failed to read response from Datanodes Detail: %s\n", error_msg)));
        }

        while (i < dn_conn_count) {
            PGXCNodeHandle* conn = pgxc_connections->datanode_handles[i];
            int res = handle_response(conn, remotestate);
            if (res == RESPONSE_ANALYZE_ROWCNT) {
                NameData nodename = {{0}};
                if (eAnalyzeMode == ANALYZENORMAL) {
                    if ((double)remotestate->analyze_totalrowcnt[ANALYZENORMAL] >= 0) {
                        stmt->pstGlobalStatEx[ANALYZENORMAL].totalRowCnts +=
                            (double)remotestate->analyze_totalrowcnt[ANALYZENORMAL];
                        stmt->pstGlobalStatEx[ANALYZENORMAL].topRowCnts =
                            Max(stmt->pstGlobalStatEx[ANALYZENORMAL].topRowCnts,
                                remotestate->analyze_totalrowcnt[ANALYZENORMAL]);
                        stmt->pstGlobalStatEx[ANALYZENORMAL].topMemSize =
                            Max(stmt->pstGlobalStatEx[ANALYZENORMAL].topMemSize,
                                remotestate->analyze_memsize[ANALYZENORMAL]);
                    } else if (arq_type == ARQ_TYPE_TOTALROWCNTS) {
                        deadblock_dn_num++;
                    }

                    elog(DEBUG1,
                        "%s total row count[%lf] for %s.",
                        (arq_type == ARQ_TYPE_SAMPLE) ? "Step 4-1: Get real" : "Step 1-1: Get estimate",
                        (double)remotestate->analyze_totalrowcnt[ANALYZENORMAL],
                        get_pgxc_nodename(conn->nodeoid, &nodename));
                }
            } else if (res == RESPONSE_COMPLETE) {
                if (i < --dn_conn_count) {
                    pgxc_connections->datanode_handles[i] = pgxc_connections->datanode_handles[dn_conn_count];
                }
            } else if (res == RESPONSE_EOF) {
                i++;
            } else
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Unexpected response from Datanode %u", conn->nodeoid)));
        }
    }

    /*
     * If it exists dead block(as estimate totalrows invalid) in some or all datanodes,
     * and there is no valid totalrows, may be we haven't got the valid block for sampling,
     * then we should set invalid totalrows as flag for setting sample rate of 2% as default.
     */
    if (arq_type == ARQ_TYPE_TOTALROWCNTS && eAnalyzeMode == ANALYZENORMAL && 0 < deadblock_dn_num &&
        stmt->pstGlobalStatEx[ANALYZENORMAL].totalRowCnts == 0) {
        stmt->pstGlobalStatEx[ANALYZENORMAL].totalRowCnts = INVALID_ESTTOTALROWS;
    }
}

/**
 * @global stats
 * @Description: receive sample rows from dn.
 *
 * @in dn_conn_count - count of target datanodes connection
 * @in pgxc_connections - target datanodes connection information
 * @in remotestate - state of remote query node
 * @in stmt - analyze or vacuum statement, we will receive sample rows and save in it
 * @in eAnalyzeMode - identify which type the table, normal table/dfs table/delta table
 * @return: HeapTuple*
 *
 */
static HeapTuple* recv_samplerows_from_dn(int dn_conn_count, PGXCNodeAllHandles* pgxc_connections,
    RemoteQueryState* remotestate, VacuumStmt* stmt, AnalyzeMode eAnalyzeMode)
{
#define RECV_DFSTUPLE_TUPDESCNO 1
#define RECV_DELTATUPLE_TUPDESCNO 2

    int i, numRows = 0;
    int* tupdescno = NULL;
    TupleTableSlot** scanslot = NULL;
    HeapTuple* results = NULL;
    int estrows = 0;
    double rstate = 0;

    if (dn_conn_count > 0) {
        tupdescno = (int*)palloc0(dn_conn_count * sizeof(int));
        scanslot = (TupleTableSlot**)palloc0(dn_conn_count * sizeof(TupleTableSlot*));
    }

    /* identify it will get sample rows from dn, so we should get enough memory to save sample rows. */
    if (eAnalyzeMode == ANALYZENORMAL) {
        int targetrows =
            (int)(stmt->pstGlobalStatEx[ANALYZENORMAL].totalRowCnts * stmt->pstGlobalStatEx[ANALYZENORMAL].sampleRate);

        stmt->tupleDesc = NULL;

        /* get estimate total sample rows for normal table. */
        estrows = dn_conn_count * Max(DEFAULT_SAMPLE_ROWCNT, targetrows);
        if (SUPPORT_PRETTY_ANALYZE) {
            estrows = Max(DEFAULT_SAMPLE_ROWCNT, targetrows);
            rstate = anl_init_selection_state(estrows);
        }

        results = (HeapTuple*)palloc0(estrows * sizeof(HeapTuple));
        stmt->pstGlobalStatEx[ANALYZENORMAL].totalRowCnts = 0;
    }

    if (remotestate->request_type == REQUEST_TYPE_NOT_DEFINED)
        remotestate->request_type = REQUEST_TYPE_QUERY;

    while (dn_conn_count > 0) {
        i = 0;

        if (pgxc_node_receive(dn_conn_count, pgxc_connections->datanode_handles, NULL)) {
            int error_code;
            char* error_msg = getSocketError(&error_code);
            pfree_ext(pgxc_connections->datanode_handles);

            ereport(
                ERROR, (errcode(error_code), errmsg("Failed to read response from Datanodes Detail: %s\n", error_msg)));
        }

        while (i < dn_conn_count) {
            PGXCNodeHandle* conn = pgxc_connections->datanode_handles[i];
            int res = handle_response(conn, remotestate);
            if (res == RESPONSE_TUPDESC) {
                scanslot[i] = MakeSingleTupleTableSlot(remotestate->tuple_desc);

                /* receive next tupdesc for hdfs table. */
                tupdescno[i]++;
                Assert(tupdescno[i] < ANALYZE_MODE_MAX_NUM);
                remotestate->description_count = 0;
            } else if (res == RESPONSE_DATAROW) {
                /*
                 * If scanslot[i] is NULL, it may be cause some error result in confusion
                 * between TUPDESC and DATAROW. So we should make sure process samplerows well.
                 */
                if (scanslot[i] == NULL) {
                    if (remotestate->tuple_desc == NULL) {
                        ereport(ERROR,
                            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                                errmsg("TUPDESC message has not been received before DATAROW message from %s",
                                    conn->remoteNodeName),
                                errdetail("Maybe datanode cause some error result in confusion between TUPDESC and "
                                          "DATAROW.")));
                    } else {
                        scanslot[i] = MakeSingleTupleTableSlot(remotestate->tuple_desc);
                    }
                }

                FetchTuple(remotestate, scanslot[i]);
                /* report error message come from datanode if there is error message. */
                if (remotestate->errorMessage)
                    pgxc_node_report_error(remotestate);

                /* Make sure the tuple is fully deconstructed */
                tableam_tslot_getallattrs(scanslot[i]);

                /* receive and save sample rows for normal table. */
                if (eAnalyzeMode == ANALYZENORMAL) {
                    /* save tuple descriptor for the sample rows if it is NULL. */
                    if (stmt->tupleDesc == NULL)
                        stmt->tupleDesc = CreateTupleDescCopy(scanslot[i]->tts_tupleDescriptor);

                    /* don't save the received sample rows if the num rows we have received more than total rows. */
                    if (numRows < estrows) {
                        results[numRows++] = heap_form_tuple(
                            scanslot[i]->tts_tupleDescriptor, scanslot[i]->tts_values, scanslot[i]->tts_isnull);
                    } else if (SUPPORT_PRETTY_ANALYZE) {
                        if (0 >= anl_get_next_S(numRows, estrows, &rstate)) {
                            /* Found a suitable tuple, so save it, replacing one old tuple at random */
                            int64 k = (int64)(estrows * anl_random_fract());

                            Assert(k >= 0 && k < estrows);
                            heap_freetuple_ext(results[k]);
                            results[k] = heap_form_tuple(
                                scanslot[i]->tts_tupleDescriptor, scanslot[i]->tts_values, scanslot[i]->tts_isnull);
                        }
                        numRows++;
                    }
                }

                (void)ExecClearTuple(scanslot[i]);
            } else if (res == RESPONSE_ANALYZE_ROWCNT) {
                NameData nodename = {{0}};
                if (eAnalyzeMode == ANALYZENORMAL) {
                    elog(DEBUG1,
                        "Step 4-1: Get real total row count[%lf] for %s.",
                        (double)remotestate->analyze_totalrowcnt[ANALYZENORMAL],
                        get_pgxc_nodename(conn->nodeoid, &nodename));

                    stmt->pstGlobalStatEx[ANALYZENORMAL].totalRowCnts +=
                        (double)remotestate->analyze_totalrowcnt[ANALYZENORMAL];
                }
            } else if (res == RESPONSE_COMPLETE) {
                if (i < --dn_conn_count) {
                    pgxc_connections->datanode_handles[i] = pgxc_connections->datanode_handles[dn_conn_count];

                    /*
                     * end of receive desc and datarows for current connection,
                     * it should clear for next connection.
                     */
                    tupdescno[i] = tupdescno[dn_conn_count];
                    scanslot[i] = scanslot[dn_conn_count];
                }
            } else if (res == RESPONSE_EOF) {
                i++;
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Unexpected response from Datanode %u", conn->nodeoid)));
            }
        }
    }

    /* save num of sample rows for normal table. */
    if (eAnalyzeMode == ANALYZENORMAL) {
        stmt->num_samples = (estrows > numRows) ? numRows : estrows;
        elog(DEBUG1,
            "Step 3: Get sample rows from DNs finished, receive [%d] tuples, sample [%d] tuples",
            numRows,
            stmt->num_samples);
    }

    return results;
}

HeapTuple* RecvRemoteSampleMessage(
    VacuumStmt* stmt, RemoteQuery* node, ANALYZE_RQTYPE arq_type, AnalyzeMode eAnalyzeMode)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * Called when the backend is ending.
 */
void PGXCNodeCleanAndRelease(int code, Datum arg)
{
    /* Clean up prepared transactions before releasing connections */
    DropAllPreparedStatements();

    /* clean saved plan but not save into gpc */
    GPCCleanUpSessionSavedPlan();

    /* Release Datanode connections */
    release_handles();

    /* Disconnect from Pooler */
    if (IsPoolHandle())
        PoolManagerDisconnect();

    /* Close connection with GTM */
    CloseGTM();

    /* Free remote xact state */
    free_RemoteXactState();

    /* Free gxip */
    UnsetGlobalSnapshotData();
}

/*
 * Called when the session is ending.
 */
void PGXCConnClean(int code, Datum arg)
{
    /* Clean up prepared transactions before releasing connections */
    DropAllPreparedStatements();

    /* Release Datanode connections */
    release_handles();

    /* Disconnect from Pooler */
    if (IsPoolHandle())
        PoolManagerDisconnect();

    /* Free remote xact state */
    free_RemoteXactState();
}

static int pgxc_get_connections(PGXCNodeHandle* connections[], int size, List* connlist)
{
    ListCell* lc = NULL;
    int count = 0;

    foreach (lc, connlist) {
        PGXCNodeHandle* conn = (PGXCNodeHandle*)lfirst(lc);
        Assert(count < size);
        connections[count++] = conn;
    }
    return count;
}
/*
 * Get all connections for which we have an open transaction,
 * for both Datanodes and Coordinators
 */
static int pgxc_get_transaction_nodes(PGXCNodeHandle* connections[], int size, bool write)
{
    return pgxc_get_connections(
        connections, size, write ? u_sess->pgxc_cxt.XactWriteNodes : u_sess->pgxc_cxt.XactReadNodes);
}

void ExecCloseRemoteStatement(const char* stmt_name, List* nodelist)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    PGXCNodeAllHandles* all_handles = NULL;
    PGXCNodeHandle** connections = NULL;
    RemoteQueryState* combiner = NULL;
    int conn_count;
    int i;

    /* Exit if nodelist is empty */
    if (list_length(nodelist) == 0)
        return;

    /* get needed Datanode connections */
    all_handles = get_handles(nodelist, NIL, false);
    conn_count = all_handles->dn_conn_count;
    connections = all_handles->datanode_handles;

    for (i = 0; i < conn_count; i++) {
        if (connections[i]->state == DN_CONNECTION_STATE_QUERY)
            BufferConnection(connections[i]);

        if (pgxc_node_send_close(connections[i], true, stmt_name) != 0) {
            /*
             * statements are not affected by statement end, so consider
             * unclosed statement on the Datanode as a fatal issue and
             * force connection is discarded
             */
            connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
            ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to close Datanode statemrnt")));
        }
        if (pgxc_node_send_sync(connections[i]) != 0) {
            connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
            ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to close Datanode statement")));
        }
    }

    combiner = CreateResponseCombiner(conn_count, COMBINE_TYPE_NONE);

    while (conn_count > 0) {
        if (pgxc_node_receive(conn_count, connections, NULL)) {
            for (i = 0; i <= conn_count; i++)
                connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;

            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to close Datanode statement")));
        }
        i = 0;
        while (i < conn_count) {
            int res = handle_response(connections[i], combiner);
            if (res == RESPONSE_EOF) {
                i++;
            } else if (res == RESPONSE_COMPLETE) {
                if (--conn_count > i)
                    connections[i] = connections[conn_count];
            } else {
                connections[i]->state = DN_CONNECTION_STATE_ERROR_FATAL;
            }
        }
    }

    ValidateAndCloseCombiner(combiner);
    pfree_pgxc_all_handles(all_handles);
#endif
}

int DataNodeCopyInBinaryForAll(const char* msg_buf, int len, PGXCNodeHandle** copy_connections)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
#else
    int i;
    int conn_count = 0;
    PGXCNodeHandle* connections[NumDataNodes];
    int msgLen = 4 + len + 1;
    int nLen = htonl(msgLen);
    errno_t rc = EOK;

    for (i = 0; i < NumDataNodes; i++) {
        PGXCNodeHandle* handle = copy_connections[i];

        if (!handle)
            continue;

        connections[conn_count++] = handle;
    }

    for (i = 0; i < conn_count; i++) {
        PGXCNodeHandle* handle = connections[i];
        if (handle->state == DN_CONNECTION_STATE_COPY_IN) {
            /* msgType + msgLen */
            if (ensure_out_buffer_capacity(handle->outEnd + 1 + msgLen, handle) != 0) {
                ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
            }

            handle->outBuffer[handle->outEnd++] = 'd';
            rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd - 1, &nLen, sizeof(int));
            securec_check(rc, "", "");
            handle->outEnd += 4;
            rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd - 1, msg_buf, len);
            securec_check(rc, "", "");
            handle->outEnd += len;
            handle->outBuffer[handle->outEnd++] = '\n';
        } else {
            add_error_message(handle, "%s", "Invalid Datanode connection");
            return EOF;
        }
    }

    return 0;
#endif
}

/*
 * ExecSetTempObjectIncluded
 *
 * Remember that we have accessed a temporary object.
 */
void ExecSetTempObjectIncluded(void)
{
    t_thrd.pgxc_cxt.temp_object_included = true;
}

/*
 * ExecClearTempObjectIncluded
 *
 * Forget about temporary objects
 */
static void ExecClearTempObjectIncluded(void)
{
    t_thrd.pgxc_cxt.temp_object_included = false;
}

/* ExecIsTempObjectIncluded
 *
 * Check if a temporary object has been accessed
 */
bool ExecIsTempObjectIncluded(void)
{
    return t_thrd.pgxc_cxt.temp_object_included;
}

/*
 * ExecProcNodeDMLInXC
 *
 * This function is used by ExecInsert/Update/Delete to execute the
 * Insert/Update/Delete on the datanode using RemoteQuery plan.
 *
 * In XC, a non-FQSed UPDATE/DELETE is planned as a two step process
 * The first step selects the ctid & node id of the row to be modified and the
 * second step creates a parameterized query that is supposed to take the data
 * row returned by the lower plan node as the parameters to modify the affected
 * row. In case of an INSERT however the first step is used to get the new
 * column values to be inserted in the target table and the second step uses
 * those values as parameters of the INSERT query.
 *
 * We use extended query protocol to avoid repeated planning of the query and
 * pass the column values(in case of an INSERT) and ctid & xc_node_id
 * (in case of UPDATE/DELETE) as parameters while executing the query.
 *
 * Parameters:
 * resultRemoteRel:  The RemoteQueryState containing DML statement to be
 *					 executed
 * sourceDataSlot: The tuple returned by the first step (described above)
 *					 to be used as parameters in the second step.
 * newDataSlot: This has all the junk attributes stripped off from
 *				sourceDataSlot, plus BEFORE triggers may have modified the
 *				originally fetched data values. In other words, this has
 *				the final values that are to be sent to datanode through BIND.
 *
 * Returns the result of RETURNING clause if any
 */
TupleTableSlot* ExecProcNodeDMLInXC(EState* estate, TupleTableSlot* sourceDataSlot, TupleTableSlot* newDataSlot)
{
    ResultRelInfo* resultRelInfo = estate->es_result_relation_info;
    RemoteQueryState* resultRemoteRel = (RemoteQueryState*)estate->es_result_remoterel;
    ExprContext* econtext = resultRemoteRel->ss.ps.ps_ExprContext;
    TupleTableSlot* returningResultSlot = NULL; /* RETURNING clause result */
    TupleTableSlot* temp_slot = NULL;
    bool dml_returning_on_replicated = false;
    RemoteQuery* step = (RemoteQuery*)resultRemoteRel->ss.ps.plan;
    uint64 saved_rqs_processed = 0;

    /*
     * If the tuple returned by the previous step was null,
     * simply return null tuple, no need to execute the DML
     */
    if (TupIsNull(sourceDataSlot))
        return NULL;

    /*
     * The current implementation of DMLs with RETURNING when run on replicated
     * tables returns row from one of the datanodes. In order to achieve this
     * ExecProcNode is repeatedly called saving one tuple and rejecting the rest.
     * Do we have a DML on replicated table with RETURNING?
     */
    dml_returning_on_replicated = IsReturningDMLOnReplicatedTable(step);

    /*
     * Use data row returned by the previous step as parameter for
     * the DML to be executed in this step.
     */
    SetDataRowForIntParams(resultRelInfo->ri_junkFilter, sourceDataSlot, newDataSlot, resultRemoteRel);

    /*
     * do_query calls get_exec_connections to determine target nodes
     * at execution time. The function get_exec_connections can decide
     * to evaluate en_expr to determine the target nodes. To evaluate en_expr,
     * ExecEvalVar is called which picks up values from ecxt_scantuple if Var
     * does not refer either OUTER or INNER varno. Hence we should copy the
     * tuple returned by previous step in ecxt_scantuple if econtext is set.
     * The econtext is set only when en_expr is set for execution time
     * determination of the target nodes.
     */

    if (econtext != NULL)
        econtext->ecxt_scantuple = newDataSlot;

    /*
     * This loop would be required to reject tuples received from datanodes
     * when a DML with RETURNING is run on a replicated table otherwise it
     * would run once.
     * PGXC need to: This approach is error prone if the DML statement constructed
     * by the planner is such that it updates more than one row (even in case of
     * non-replicated data). Fix it.
     */
    do {
        temp_slot = ExecProcNode((PlanState*)resultRemoteRel);
        if (!TupIsNull(temp_slot)) {
            /* Have we already copied the returned tuple? */
            if (returningResultSlot == NULL) {
                /* Copy the received tuple to be returned later */
                returningResultSlot = MakeSingleTupleTableSlot(temp_slot->tts_tupleDescriptor);
                returningResultSlot = ExecCopySlot(returningResultSlot, temp_slot);
                saved_rqs_processed = resultRemoteRel->rqs_processed;
            }

            /* we should never get a failure here */
            Assert(ExecIsTempObjectIncluded() || (saved_rqs_processed == resultRemoteRel->rqs_processed));

            /* Clear the received tuple, the copy required has already been saved */
            (void)ExecClearTuple(temp_slot);
        } else {
            if (dml_returning_on_replicated) {
                resultRemoteRel->rqs_processed = saved_rqs_processed;
            }
            /* Null tuple received, so break the loop */
            (void)ExecClearTuple(temp_slot);
            break;
        }
    } while (dml_returning_on_replicated);

    /*
     * A DML can impact more than one row, e.g. an update without any where
     * clause on a table with more than one row. We need to make sure that
     * RemoteQueryNext calls do_query for each affected row, hence we reset
     * the flag here and finish the DML being executed only when we return
     * NULL from ExecModifyTable
     */
    resultRemoteRel->query_Done = false;

    return returningResultSlot;
}

void RegisterTransactionNodes(int count, void** connections, bool write)
{
    int i;
    MemoryContext oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));

    for (i = 0; i < count; i++) {
        /*
         * Add the node to either read or write participants. If a node is
         * already in the write participant's list, don't add it to the read
         * participant's list. OTOH if a node is currently in the read
         * participant's list, but we are now initiating a write operation on
         * the node, move it to the write participant's list
         */
        if (write) {
            u_sess->pgxc_cxt.XactWriteNodes = list_append_unique(u_sess->pgxc_cxt.XactWriteNodes, connections[i]);
            u_sess->pgxc_cxt.XactReadNodes = list_delete(u_sess->pgxc_cxt.XactReadNodes, connections[i]);
        } else {
            if (!list_member(u_sess->pgxc_cxt.XactWriteNodes, connections[i]))
                u_sess->pgxc_cxt.XactReadNodes = list_append_unique(u_sess->pgxc_cxt.XactReadNodes, connections[i]);
        }
    }

    MemoryContextSwitchTo(oldcontext);
}

void PrintRegisteredTransactionNodes(void)
{
    ListCell* cell = NULL;

    if (module_logging_is_on(MOD_TRANS_HANDLE)) {
        foreach (cell, u_sess->pgxc_cxt.XactReadNodes) {
            PGXCNodeHandle* handle = (PGXCNodeHandle*)lfirst(cell);
            ereport(LOG,
                (errmodule(MOD_TRANS_HANDLE),
                    errmsg("u_sess->pgxc_cxt.XactReadNodes list : nodeoid = %u,  nodeIdx = %d",
                        handle->nodeoid,
                        handle->nodeIdx)));
        }

        foreach (cell, u_sess->pgxc_cxt.XactWriteNodes) {
            PGXCNodeHandle* handle = (PGXCNodeHandle*)lfirst(cell);
            ereport(LOG,
                (errmodule(MOD_TRANS_HANDLE),
                    errmsg("u_sess->pgxc_cxt.XactWriteNodes list : nodeoid = %u,  nodeIdx = %d",
                        handle->nodeoid,
                        handle->nodeIdx)));
        }
    }
}

void ForgetTransactionNodes(void)
{
    list_free_ext(u_sess->pgxc_cxt.XactReadNodes);
    u_sess->pgxc_cxt.XactReadNodes = NIL;

    list_free_ext(u_sess->pgxc_cxt.XactWriteNodes);
    u_sess->pgxc_cxt.XactWriteNodes = NIL;
}

/*
 * Clear per transaction remote information
 */
void AtEOXact_Remote(void)
{
    ExecClearTempObjectIncluded();
    ForgetTransactionNodes();
    clear_RemoteXactState();

#ifdef PGXC
    /* for "analyze table"
     *
     * cn1         |   cn2
     * create db;  |   drop database db;
     * create t1;  |   WARNING: Clean connections not completed
     * analyze;    |
     *
     * analyze create a openGauss on cn2, the openGauss do NOT give conn back
     * to pooler, so code here do this.
     */
    if (IS_PGXC_COORDINATOR && IsConnFromCoord())
        destroy_handles();

#endif
}

/*
 * For 2PC, we do the following steps:
 *
 *  1. PREPARE the transaction locally if the local node is involved in the
 *     transaction. If local node is not involved, skip this step and go to the
 *     next step
 *  2. PREPARE the transaction on all the remote nodes. If any node fails to
 *     PREPARE, directly go to step 6
 *  3. Now that all the involved nodes are PREPAREd, we can commit the
 *     transaction. We first inform the GTM that the transaction is fully
 *     PREPARED and also supply the list of the nodes involved in the
 *     transaction
 *  4. Start a new transaction so that normal commit processing
 *     works unchanged. COMMIT PREPARED the transaction on local node firstly if its involved in the
 *     transaction . And then commit prepared all the remotes nodes.
 *     and . Go to step 5.
 *  5. Return and let the normal commit processing resume
 *  6. Abort by ereporting the error and let normal abort-processing take
 *     charge.
 */

/*
 * Do commit prepared processiong for remote nodes includes Datanodes and other Coordinators.
 */
void PreCommit_Remote(char* prepareGID, bool barrierLockHeld)
{
    MemoryContext current_context = NULL;
    /* send the commit csn before send the commit command to remote. */
    if (!(useLocalXid || !IsPostmasterEnvironment || g_instance.attr.attr_storage.enable_gtm_free))
        SendPGXCNodeCommitCsn(GetCommitCsn());

    /*
     * OK, everything went fine. At least one remote node is in PREPARED state
     * and the transaction is successfully prepared on all the involved nodes.
     * Now we are ready to commit the transaction. We need a new GXID to send
     * down the remote nodes to execute the forthcoming COMMIT PREPARED
     * command. So grab one from the GTM and track it. It will be closed along
     * with the main transaction at the end.
     */
    if (TwoPhaseCommit) {
        /*
         * At two phase transaction, after CN has committed,
         * pgxc_node_remote_commit can not ereport ERROR when receive signal.
         */
        current_context = CurrentMemoryContext;
        PG_TRY();
        {
            pgxc_node_remote_commit(barrierLockHeld);
        }
        PG_CATCH();
        {
            (void)MemoryContextSwitchTo(current_context);
            ErrorData* edata = CopyErrorData();
            FlushErrorState();

            if (!barrierLockHeld && LWLockHeldByMe(BarrierLock)) {
                /* match the upcoming RESUME_INTERRUPTS */
                HOLD_INTERRUPTS();
                LWLockRelease(BarrierLock);
            }

            /*
             * remoteXactState.status maybe RXACT_COMMIT_FAILED or
             * RXACT_PART_COMMITTED, we set it to RXACT_COMMIT_FAILED at here,
             * it doesn't matter.
             */
            u_sess->pgxc_cxt.remoteXactState->status = RXACT_COMMIT_FAILED;
            ereport(WARNING, (errmsg("Failed during commit prepared transaction: %s", edata->message)));
        }
        PG_END_TRY();
    } else
        pgxc_node_remote_commit(barrierLockHeld);

    Assert(u_sess->pgxc_cxt.remoteXactState->status == RXACT_COMMITTED ||
           u_sess->pgxc_cxt.remoteXactState->status == RXACT_COMMIT_FAILED ||
           u_sess->pgxc_cxt.remoteXactState->status == RXACT_PART_COMMITTED ||
           u_sess->pgxc_cxt.remoteXactState->status == RXACT_NONE);
    /*
     * we can not ereport ERROR after CN has committed when two phase transaction
     */
    if (TwoPhaseCommit)
        START_CRIT_SECTION();

    clear_RemoteXactState();

    if (TwoPhaseCommit) {
        END_CRIT_SECTION();
    }

    /*
     * The transaction is now successfully committed on all the remote nodes.
     * (XXX How about the local node ?). It can now be cleaned up from the GTM
     * as well.
     *
     * During inplace or online upgrade, we should never put the connection
     * back to the pooler, since they may be reused to communicate with CNs
     * having different t_thrd.proc->workingVersionNum values, which may lead to
     * incompatibility.
     */
    if (!u_sess->attr.attr_common.pooler_cache_connection || isInLargeUpgrade() ||
        (g_instance.attr.attr_common.enable_thread_pool && !u_sess->attr.attr_common.PersistentConnections &&
        (u_sess->proc_cxt.IsInnerMaintenanceTools || IsHAPort(u_sess->proc_cxt.MyProcPort))))
        destroy_handles();
    else if (!u_sess->attr.attr_common.PersistentConnections)
        release_handles();
}

void SubXactCancel_Remote(void)
{
    cancel_query();
    clear_RemoteXactState();
}

/*
 * Do abort processing for the transaction. We must abort the transaction on
 * all the involved nodes. If a node has already prepared a transaction, we run
 * ROLLBACK PREPARED command on the node. Otherwise, a simple ROLLBACK command
 * is sufficient.
 *
 * We must guard against the case when a transaction is prepared succefully on
 * all the nodes and some error occurs after we send a COMMIT PREPARED message
 * to at lease one node. Such a transaction must not be aborted to preserve
 * global consistency. We handle this case by recording the nodes involved in
 * the transaction at the GTM and keep the transaction open at the GTM so that
 * its reported as "in-progress" on all the nodes until resolved
 */
bool PreAbort_Remote(bool PerfectRollback)
{
    int has_error = 0;
    // If has any communacation failure when cancel, we just drop the connection.
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() && !PerfectRollback)
        cancel_query_without_read();
    else if (IS_SPQ_COORDINATOR && !IsConnFromCoord() && !PerfectRollback)
        spq_cancel_query();

    if (!t_thrd.xact_cxt.XactLocalNodeCanAbort)
        return false;

    if (u_sess->pgxc_cxt.remoteXactState->status == RXACT_COMMITTED)
        return false;

    if (u_sess->pgxc_cxt.remoteXactState->status == RXACT_NONE && !IsNormalProcessingMode())
        return true;

    if (u_sess->pgxc_cxt.remoteXactState->status == RXACT_PART_COMMITTED)
        return false;
    else {
        /*
         * The transaction is neither part or fully committed. We can safely
         * abort such transaction
         */
        if (u_sess->pgxc_cxt.remoteXactState->status == RXACT_NONE)
            init_RemoteXactState(false);

        has_error = pgxc_node_remote_abort();
    }

    clear_RemoteXactState();

    // Here we destroy the connection unconditionally, to avoid possible message
    // disorder, which may cause the message of this query be received by the next query.
    if (!PerfectRollback || has_error || !u_sess->attr.attr_common.pooler_cache_connection ||
        isInLargeUpgrade() || (g_instance.attr.attr_common.enable_thread_pool &&
        (u_sess->proc_cxt.IsInnerMaintenanceTools || IsHAPort(u_sess->proc_cxt.MyProcPort))))
        destroy_handles();
    else if (!u_sess->attr.attr_common.PersistentConnections)
        release_handles();

    return has_error;
}

/* Indicates the version containing the backend_version parameter,
 * This parameter is used to determine whether the handle is used to update catalog.
 * For a version that does not contain the backend_version parameter,
 * check whether the version is a handle in the upgrade process.. */
bool isInLargeUpgrade()
{
    if (contain_backend_version(t_thrd.proc->workingVersionNum)) {
        return u_sess->attr.attr_common.IsInplaceUpgrade;
    } else {
        return (t_thrd.proc->workingVersionNum != GRAND_VERSION_NUM || u_sess->attr.attr_common.IsInplaceUpgrade);
    }
}

char* PrePrepare_Remote(const char* prepareGID, bool implicit, bool WriteCnLocalNode)
{
    init_RemoteXactState(false);

    /*
     * PREPARE the transaction on all nodes including remote nodes as well as
     * local node. Any errors will be reported via ereport and the transaction
     * will be aborted accordingly.
     */
    (void)pgxc_node_remote_prepare(prepareGID, WriteCnLocalNode);

    if (u_sess->pgxc_cxt.preparedNodes)
        pfree_ext(u_sess->pgxc_cxt.preparedNodes);
    u_sess->pgxc_cxt.preparedNodes = NULL;

    if (!implicit)
        u_sess->pgxc_cxt.preparedNodes = pgxc_node_get_nodelist(true);

    return u_sess->pgxc_cxt.preparedNodes;
}

void PostPrepare_Remote(char* prepareGID, char* nodestring, bool implicit)
{
    u_sess->pgxc_cxt.remoteXactState->preparedLocalNode = true;

    /*
     * If this is an explicit PREPARE request by the client, we must also save
     * the list of nodes involved in this transaction on the GTM for later use
     */

    /* Now forget the transaction nodes */
    ForgetTransactionNodes();
}

/*
 * Return the list of nodes where the prepared transaction is not yet committed
 */
static char* pgxc_node_get_nodelist(bool localNode)
{
    int i;
    char* nodestring = NULL;
    errno_t rc = EOK;

    for (i = 0; i < u_sess->pgxc_cxt.remoteXactState->numWriteRemoteNodes; i++) {
        RemoteXactNodeStatus status = u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i];
        PGXCNodeHandle* conn = u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles[i];

        if (status != RXACT_NODE_COMMITTED) {
            NameData nodename = {{0}};
            (void)get_pgxc_nodename(conn->nodeoid, &nodename);
            if (nodestring == NULL) {
                nodestring = (char*)MemoryContextAlloc(
                    SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), strlen(nodename.data) + 1);
                rc = sprintf_s(nodestring, strlen(nodename.data) + 1, "%s", nodename.data);
                securec_check_ss(rc, "", "");
            } else {
                nodestring = (char*)repalloc(nodestring, strlen(nodename.data) + strlen(nodestring) + 2);
                rc = sprintf_s(
                    nodestring, strlen(nodename.data) + strlen(nodestring) + 2, "%s,%s", nodestring, nodename.data);
                securec_check_ss(rc, "", "");
            }
        }
    }

    /* Case of a single Coordinator */
    if (localNode && u_sess->pgxc_cxt.PGXCNodeId >= 0) {
        if (nodestring == NULL) {
            nodestring = (char*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR),
                strlen(g_instance.attr.attr_common.PGXCNodeName) + 1);
            rc = sprintf_s(nodestring,
                strlen(g_instance.attr.attr_common.PGXCNodeName) + 1,
                "%s",
                g_instance.attr.attr_common.PGXCNodeName);
            securec_check_ss(rc, "", "");
        } else {
            nodestring =
                (char*)repalloc(nodestring, strlen(g_instance.attr.attr_common.PGXCNodeName) + strlen(nodestring) + 2);
            rc = sprintf_s(nodestring,
                strlen(g_instance.attr.attr_common.PGXCNodeName) + strlen(nodestring) + 2,
                "%s,%s",
                nodestring,
                g_instance.attr.attr_common.PGXCNodeName);
            securec_check_ss(rc, "", "");
        }
    }

    return nodestring;
}

bool IsTwoPhaseCommitRequired(bool localWrite)
{
    /*
     * under gtm free mode, disable 2pc DML when  enable_twophase_commit is off.
     * always enable 2pc commit when upgrade,expand or deal with replicated table.
     */
    if (g_instance.attr.attr_storage.enable_gtm_free && (u_sess->attr.attr_common.upgrade_mode == 0) &&
        !t_thrd.xact_cxt.MyXactAccessedRepRel && !u_sess->attr.attr_storage.enable_twophase_commit &&
        !u_sess->attr.attr_sql.enable_cluster_resize && !localWrite &&
        (list_length(u_sess->pgxc_cxt.XactWriteNodes) > 1)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_TRANS_XACT),
                errmsg("Unsupport DML two phase commit under gtm free mode."),
                errhint("Set enable_twophase_commit to on if need to use DML two phase commit.")));
    }
    if ((list_length(u_sess->pgxc_cxt.XactWriteNodes) > 1) ||
        ((list_length(u_sess->pgxc_cxt.XactWriteNodes) == 1) && localWrite)) {
        // temp table can use 2PC, so just return true here.
        return true;
    } else
        return false;
}

static void clear_RemoteXactState(void)
{
    /* Clear the previous state */
    u_sess->pgxc_cxt.remoteXactState->numWriteRemoteNodes = 0;
    u_sess->pgxc_cxt.remoteXactState->numReadRemoteNodes = 0;
    u_sess->pgxc_cxt.remoteXactState->status = RXACT_NONE;
    u_sess->pgxc_cxt.remoteXactState->prepareGID[0] = '\0';
    errno_t rc = EOK;

    if ((u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles == NULL) ||
        (u_sess->pgxc_cxt.remoteXactState->maxRemoteNodes <
            (u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords))) {
        if (u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles != NULL)
            free(u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles);

        u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles = (PGXCNodeHandle**)malloc(
            sizeof(PGXCNodeHandle*) *
            (g_instance.attr.attr_common.MaxDataNodes + g_instance.attr.attr_network.MaxCoords));

        if (u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus != NULL)
            free(u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus);

        u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus = (RemoteXactNodeStatus*)malloc(
            sizeof(RemoteXactNodeStatus) *
            (g_instance.attr.attr_common.MaxDataNodes + g_instance.attr.attr_network.MaxCoords));

        u_sess->pgxc_cxt.remoteXactState->maxRemoteNodes = u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords;
    }

    if (u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles != NULL) {
        rc = memset_s(u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles,
            sizeof(PGXCNodeHandle*) * (u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords),
            0,
            sizeof(PGXCNodeHandle*) * (u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords));
        securec_check(rc, "\0", "\0");
    } else
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

    if (u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords <= 0)
        return;

    if (u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus != NULL) {
        rc = memset_s(u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus,
            sizeof(RemoteXactNodeStatus) * (u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords),
            0,
            sizeof(RemoteXactNodeStatus) * (u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords));
        securec_check(rc, "\0", "\0");
    } else
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
}

static void init_RemoteXactState(bool preparedLocalNode)
{
    int write_conn_count, read_conn_count;
    PGXCNodeHandle** connections = NULL;

    clear_RemoteXactState();

    u_sess->pgxc_cxt.remoteXactState->preparedLocalNode = preparedLocalNode;
    connections = u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles;

    Assert(connections);

    /*
     * First get information about all the nodes involved in this transaction
     */
    write_conn_count =
        pgxc_get_transaction_nodes(connections, u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords, true);
    u_sess->pgxc_cxt.remoteXactState->numWriteRemoteNodes = write_conn_count;

    read_conn_count = pgxc_get_transaction_nodes(connections + write_conn_count,
        u_sess->pgxc_cxt.NumDataNodes + u_sess->pgxc_cxt.NumCoords - write_conn_count,
        false);
    u_sess->pgxc_cxt.remoteXactState->numReadRemoteNodes = read_conn_count;
}

void free_RemoteXactState(void)
{
    if (u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles != NULL)
        free(u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles);
    u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles = NULL;

    if (u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus != NULL)
        free(u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus);
    u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus = NULL;
}

/*
 * pgxc_node_report_error
 * Throw error if any.
 */
void pgxc_node_report_error(RemoteQueryState* combiner, int elevel)
{
#define REPORT_ERROR ((elevel > 0) ? elevel : ERROR)

    /* If no combiner, nothing to do */
    if (combiner == NULL)
        return;

    if (combiner->need_error_check)
        return;

    if (combiner->errorMessage) {
        // For internal cancel error message, do not bother reporting it.
        //
        if (combiner->errorCode == ERRCODE_QUERY_INTERNAL_CANCEL && t_thrd.xact_cxt.bInAbortTransaction)
            return;

        char* errMsg = combiner->errorMessage;

        if (combiner->position == PLAN_ROUTER) {
            StringInfo si = makeStringInfo();
            appendStringInfo(si, "from the Compute Pool: \"%s\"", errMsg);
            errMsg = si->data;
        }

        if (combiner->errorDetail != NULL && combiner->query != NULL && combiner->errorContext != NULL &&
            combiner->hint != NULL)
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos),
                    errdetail("%s", combiner->errorDetail),
                    errquery("%s", combiner->query),
                    errcontext("%s", combiner->errorContext),
                    errhint("%s", combiner->hint)));
        else if (combiner->errorDetail != NULL && combiner->query != NULL && combiner->errorContext != NULL)
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos),
                    errdetail("%s", combiner->errorDetail),
                    errquery("%s", combiner->query),
                    errcontext("%s", combiner->errorContext)));
        else if (combiner->errorDetail != NULL && combiner->errorContext != NULL && combiner->hint != NULL)
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos),
                    errdetail("%s", combiner->errorDetail),
                    errcontext("%s", combiner->errorContext),
                    errhint("%s", combiner->hint)));
        else if (combiner->errorDetail != NULL && combiner->query != NULL && combiner->hint != NULL)
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos),
                    errdetail("%s", combiner->errorDetail),
                    errquery("%s", combiner->query),
                    errhint("%s", combiner->hint)));
        else if (combiner->query && combiner->errorContext != NULL && combiner->hint != NULL)
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos),
                    errquery("%s", combiner->query),
                    errcontext("%s", combiner->errorContext),
                    errhint("%s", combiner->hint)));
        else if (combiner->errorDetail != NULL && combiner->query != NULL)
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos),
                    errdetail("%s", combiner->errorDetail),
                    errquery("%s", combiner->query)));
        else if (combiner->errorDetail != NULL && combiner->errorContext != NULL)
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos),
                    errdetail("%s", combiner->errorDetail),
                    errcontext("%s", combiner->errorContext)));
        else if (combiner->errorDetail != NULL && combiner->hint != NULL)
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos),
                    errdetail("%s", combiner->errorDetail),
                    errhint("%s", combiner->hint)));
        else if (combiner->query != NULL && combiner->errorContext != NULL)
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos),
                    errquery("%s", combiner->query),
                    errcontext("%s", combiner->errorContext)));
        else if (combiner->query != NULL && combiner->hint != NULL)
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos),
                    errquery("%s", combiner->query),
                    errhint("%s", combiner->hint)));
        else if (combiner->errorContext != NULL && combiner->hint != NULL)
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos),
                    errcontext("%s", combiner->errorContext),
                    errhint("%s", combiner->hint)));
        else if (combiner->errorDetail != NULL)
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos),
                    errdetail("%s", combiner->errorDetail)));
        else if (combiner->errorContext != NULL)
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos),
                    errcontext("%s", combiner->errorContext)));
        else if (combiner->hint != NULL)
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos),
                    errhint("%s", combiner->hint)));
        else if (combiner->query != NULL)
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos),
                    errquery("%s", combiner->query)));
        else
            ereport(REPORT_ERROR,
                (errcode(combiner->errorCode),
                    combiner_errdata(&combiner->remoteErrData),
                    errmsg("%s", errMsg),
                    internalerrposition(combiner->cursorpos)));
    }
}

/*
 * get_success_nodes:
 * Currently called to print a user-friendly message about
 * which nodes the query failed.
 * Gets all the nodes where no 'E' (error) messages were received; i.e. where the
 * query ran successfully.
 */
static ExecNodes* get_success_nodes(int node_count, PGXCNodeHandle** handles, char node_type, StringInfo failednodes)
{
    ExecNodes* success_nodes = NULL;
    int i;

    for (i = 0; i < node_count; i++) {
        PGXCNodeHandle* handle = handles[i];
        int nodenum = PGXCNodeGetNodeId(handle->nodeoid, node_type);

        if (!handle->error) {
            if (success_nodes == NULL)
                success_nodes = makeNode(ExecNodes);
            success_nodes->nodeList = lappend_int(success_nodes->nodeList, nodenum);
        } else {
            if (failednodes->len == 0)
                appendStringInfo(failednodes, "Error message received from nodes:");
            appendStringInfo(failednodes, " %s", handle->remoteNodeName);
        }
    }
    return success_nodes;
}

void pgxc_all_success_nodes(ExecNodes** d_nodes, ExecNodes** c_nodes, char** failednodes_msg)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    PGXCNodeAllHandles* connections = get_exec_connections(NULL, NULL, EXEC_ON_ALL_NODES);
    StringInfoData failednodes;
    initStringInfo(&failednodes);

    *d_nodes =
        get_success_nodes(connections->dn_conn_count, connections->datanode_handles, PGXC_NODE_DATANODE, &failednodes);

    *c_nodes =
        get_success_nodes(connections->co_conn_count, connections->coord_handles, PGXC_NODE_COORDINATOR, &failednodes);

    if (failednodes.len == 0)
        *failednodes_msg = NULL;
    else
        *failednodes_msg = failednodes.data;
#endif
}

/*
 * set_dbcleanup_callback:
 * Register a callback function which does some non-critical cleanup tasks
 * on xact success or abort, such as tablespace/database directory cleanup.
 */
void set_dbcleanup_callback(xact_callback function, const void* paraminfo, int paraminfo_size)
{
    AutoContextSwitch dbcleanupCxt(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
    
    void* fparams = NULL;
    errno_t rc = EOK;

    abort_callback_type* dbcleanupInfo = (abort_callback_type*)palloc(sizeof(abort_callback_type));
    fparams = palloc(paraminfo_size);
    rc = memcpy_s(fparams, paraminfo_size, paraminfo, paraminfo_size);
    securec_check(rc, "", "");

    dbcleanupInfo->fparams = fparams;
    dbcleanupInfo->function = function;

    u_sess->xact_cxt.dbcleanupInfoList = lappend(u_sess->xact_cxt.dbcleanupInfoList, dbcleanupInfo);
}

/*
 * AtEOXact_DBCleanup: To be called at post-commit or pre-abort.
 * Calls the cleanup function registered during this transaction, if any.
 */
void AtEOXact_DBCleanup(bool isCommit)
{
    bool doFunc = isCommit || t_thrd.xact_cxt.XactLocalNodeCanAbort;
    ListCell *lc = NULL;
    if (u_sess->xact_cxt.dbcleanupInfoList && doFunc) {
        foreach (lc, u_sess->xact_cxt.dbcleanupInfoList) {
            abort_callback_type *dbcleanupInfo = (abort_callback_type*)lfirst(lc);
            if (dbcleanupInfo->function) {
                (*dbcleanupInfo->function)(isCommit, dbcleanupInfo->fparams);
            }
        }
    }

    /*
     * Just reset the callbackinfo. We anyway don't want this to be called again,
     * until explicitly set.
     */
    foreach (lc, u_sess->xact_cxt.dbcleanupInfoList) {
        abort_callback_type *dbcleanupInfo = (abort_callback_type*)lfirst(lc);
        pfree_ext(dbcleanupInfo->fparams);
        dbcleanupInfo->function = NULL;
    }
    list_free_deep(u_sess->xact_cxt.dbcleanupInfoList);
    u_sess->xact_cxt.dbcleanupInfoList = NIL;
}

/*
 * SetDataRowForIntParams: Form a BIND data row for internal parameters.
 * This function is called when the data for the parameters of remote
 * statement resides in some plan slot of an internally generated remote
 * statement rather than from some extern params supplied by the caller of the
 * query. Currently DML is the only case where we generate a query with
 * internal parameters.
 * The parameter data is constructed from the slot data, and stored in
 * RemoteQueryState.paramval_data.
 * At the same time, remote parameter types are inferred from the slot
 * tuple descriptor, and stored in RemoteQueryState.rqs_param_types.
 * On subsequent calls, these param types are re-used.
 * The data to be BOUND consists of table column data to be inserted/updated
 * and the ctid/nodeid values to be supplied for the WHERE clause of the
 * query. The data values are present in dataSlot whereas the ctid/nodeid
 * are available in sourceSlot as junk attributes.
 * For DELETEs, the dataSlot is NULL.
 * sourceSlot is used only to retrieve ctid/nodeid, so it does not get
 * used for INSERTs, although it will never be NULL.
 * The slots themselves are undisturbed.
 */
static void SetDataRowForIntParams(
    JunkFilter* junkfilter, TupleTableSlot* sourceSlot, TupleTableSlot* dataSlot, RemoteQueryState* rq_state)
{
    StringInfoData buf;
    uint16 numparams = 0;
    RemoteQuery* step = (RemoteQuery*)rq_state->ss.ps.plan;
    errno_t rc = EOK;

    Assert(sourceSlot);

    /* Calculate the total number of parameters */
    if (dataSlot != NULL)
        numparams = dataSlot->tts_tupleDescriptor->natts;
    /* Add number of junk attributes */
    if (junkfilter != NULL) {
        if (junkfilter->jf_primary_keys != NIL)
            numparams += list_length(junkfilter->jf_primary_keys);
        if (junkfilter->jf_junkAttNo)
            numparams++;
        if (junkfilter->jf_xc_node_id)
            numparams++;
        if (junkfilter->jf_xc_part_id)
            numparams++;
    }

    /*
     * Infer param types from the slot tupledesc and junk attributes. But we
     * have to do it only the first time: the interal parameters remain the same
     * while processing all the source data rows because the data slot tupdesc
     * never changes. Even though we can determine the internal param types
     * during planning, we want to do it here: we don't want to set the param
     * types and param data at two different places. Doing them together here
     * helps us to make sure that the param types are in sync with the param
     * data.
     */

    /*
     * We know the numparams, now initialize the param types if not already
     * done. Once set, this will be re-used for each source data row.
     */
    if (rq_state->rqs_num_params == 0) {
        int attindex = 0;
        TupleDesc tdesc;

        rq_state->rqs_num_params = numparams;
        rq_state->rqs_param_types = (Oid*)palloc(sizeof(Oid) * rq_state->rqs_num_params);

        if (dataSlot != NULL) /* We have table attributes to bind */
        {
            tdesc = dataSlot->tts_tupleDescriptor;
            int numatts = tdesc->natts;
            for (attindex = 0; attindex < numatts; attindex++) {
                rq_state->rqs_param_types[attindex] = tdesc->attrs[attindex].atttypid;

                /* For unknown param type(maybe a const), we need to convert it to text */
                if (tdesc->attrs[attindex].atttypid == UNKNOWNOID) {
                    rq_state->rqs_param_types[attindex] = TEXTOID;
                }
            }
        }

        if (junkfilter != NULL) /* Param types for specific junk attributes if present */
        {
            ListCell* lc = NULL;

            foreach (lc, junkfilter->jf_primary_keys) {
                Var* var = (Var*)lfirst(lc);
                Assert(IsA(var, Var));

                rq_state->rqs_param_types[attindex++] = var->vartype;
            }

            /* jf_junkAttNo always contains ctid */
            if (AttributeNumberIsValid(junkfilter->jf_junkAttNo))
                rq_state->rqs_param_types[attindex++] = TIDOID;

            if (AttributeNumberIsValid(junkfilter->jf_xc_node_id))
                rq_state->rqs_param_types[attindex++] = INT4OID;

            if (AttributeNumberIsValid(junkfilter->jf_xc_part_id))
                rq_state->rqs_param_types[attindex++] = OIDOID;
        }
    } else {
        Assert(rq_state->rqs_num_params == numparams);
    }

    /*
     * If we already have the data row, just copy that, and we are done. One
     * scenario where we can have the data row is for INSERT ... SELECT.
     * Effectively, in this case, we just re-use the data row from SELECT as-is
     * for BIND row of INSERT. But just make sure all of the data required to
     * bind is available in the slot. If there are junk attributes to be added
     * in the BIND row, we cannot re-use the data row as-is.
     */
    if (junkfilter == NULL && dataSlot != NULL && dataSlot->tts_dataRow != NULL) {
        if (rq_state->paramval_data != NULL) {
            pfree_ext(rq_state->paramval_data);
            rq_state->paramval_data = NULL;
        }
        rq_state->paramval_data = (char*)palloc(dataSlot->tts_dataLen);
        rc = memcpy_s(rq_state->paramval_data, dataSlot->tts_dataLen, dataSlot->tts_dataRow, dataSlot->tts_dataLen);
        securec_check(rc, "", "");
        rq_state->paramval_len = dataSlot->tts_dataLen;
        return;
    }

    initStringInfo(&buf);

    {
        uint16 params_nbo = htons(numparams); /* Network byte order */
        appendBinaryStringInfo(&buf, (char*)&params_nbo, sizeof(params_nbo));
    }

    /*
     * The data attributes would not be present for DELETE. In such case,
     * dataSlot will be NULL.
     */
    if (dataSlot != NULL) {
        TupleDesc tdesc = dataSlot->tts_tupleDescriptor;
        int attindex;

        /* Append the data attributes */

        /* ensure we have all values */
        tableam_tslot_getallattrs(dataSlot);
        for (attindex = 0; attindex < tdesc->natts; attindex++) {
            uint32 n32;
            Assert(attindex < numparams);

            if (dataSlot->tts_isnull[attindex]) {
                n32 = htonl(~0);
                appendBinaryStringInfo(&buf, (char*)&n32, 4);
            } else
                /* It should switch memctx to ExprContext for makenode in ExecInitExpr */
                pgxc_append_param_val(&buf, dataSlot->tts_values[attindex], tdesc->attrs[attindex].atttypid);
        }
    }

    /*
     * From the source data, fetch the junk attribute values to be appended in
     * the end of the data buffer. The junk attribute vals like ctid and
     * xc_node_id are used in the WHERE clause parameters.
     * These attributes would not be present for INSERT.
     *
     * For MergeInto query, there are junkfilters, but we do not need it for INSERT
     */
    if (junkfilter != NULL) {
        ListCell* lc = NULL;
        int attindex = junkfilter->jf_junkAttNo - list_length(junkfilter->jf_primary_keys);
        bool allow_dummy_junkfilter = step->remote_query->commandType == CMD_INSERT;

        foreach (lc, junkfilter->jf_primary_keys) {
            Var* var = (Var*)lfirst(lc);

            Assert(IsA(var, Var));
            pgxc_append_param_junkval(sourceSlot, attindex++, var->vartype, &buf, allow_dummy_junkfilter);
        }

        /* First one - jf_junkAttNo - always reprsents ctid */
        pgxc_append_param_junkval(sourceSlot, junkfilter->jf_junkAttNo, TIDOID, &buf, allow_dummy_junkfilter);
        pgxc_append_param_junkval(sourceSlot, junkfilter->jf_xc_node_id, INT4OID, &buf, allow_dummy_junkfilter);
        pgxc_append_param_junkval(sourceSlot, junkfilter->jf_xc_part_id, OIDOID, &buf, allow_dummy_junkfilter);
    }

    /* Assign the newly allocated data row to paramval */
    if (rq_state->paramval_data != NULL) {
        pfree_ext(rq_state->paramval_data);
        rq_state->paramval_data = NULL;
    }
    rq_state->paramval_data = buf.data;
    rq_state->paramval_len = buf.len;
}

/*
 * pgxc_append_param_junkval:
 * Append into the data row the parameter whose value cooresponds to the junk
 * attributes in the source slot, namely ctid or node_id.
 */
static void pgxc_append_param_junkval(
    TupleTableSlot* slot, AttrNumber attno, Oid valtype, StringInfo buf, bool allow_dummy_junkfilter)
{
    bool isNull = false;

    if (slot != NULL && attno != InvalidAttrNumber) {
        /* Junk attribute positions are saved by ExecFindJunkAttribute() */
        Datum val = ExecGetJunkAttribute(slot, attno, &isNull);

        /* shouldn't ever get a null result... */
        if (isNull && !allow_dummy_junkfilter)
            ereport(ERROR, (errcode(ERRCODE_NULL_JUNK_ATTRIBUTE), errmsg("NULL junk attribute")));

        /* for MERGEINTO query ,we allow null junkfilter */
        if (isNull && allow_dummy_junkfilter) {
            char* pstring = NULL;

            if (valtype == TIDOID) {
                pstring = "(0,0)";
                int len = strlen(pstring);
                uint32 n32 = htonl(len);

                appendBinaryStringInfo(buf, (char*)&n32, 4);
                appendBinaryStringInfo(buf, pstring, len);
            } else {
                pstring = "0";
                int len = strlen(pstring);
                uint32 n32 = htonl(len);

                appendBinaryStringInfo(buf, (char*)&n32, 4);
                appendBinaryStringInfo(buf, pstring, len);
            }
        }

        if (!isNull)
            pgxc_append_param_val(buf, val, valtype);
    }
}

/*
 * pgxc_append_param_val:
 * Append the parameter value for the SET clauses of the UPDATE statement.
 * These values are the table attribute values from the dataSlot.
 */
static void pgxc_append_param_val(StringInfo buf, Datum val, Oid valtype)
{
    /* Convert Datum to string */
    char* pstring = NULL;
    int len;
    uint32 n32;
    Oid typOutput;
    bool typIsVarlena = false;
    Datum newval = 0;

    /* Get info needed to output the value */
    getTypeOutputInfo(valtype, &typOutput, &typIsVarlena);
    /*
     * If we have a toasted datum, forcibly detoast it here to avoid
     * memory leakage inside the type's output routine.
     */
    if (typIsVarlena)
        newval = PointerGetDatum(PG_DETOAST_DATUM(val));

    pstring = OidOutputFunctionCall(typOutput, ((typIsVarlena == true) ? newval : val));

    /* copy data to the buffer */
    len = strlen(pstring);
    n32 = htonl(len);
    appendBinaryStringInfo(buf, (char*)&n32, 4);
    appendBinaryStringInfo(buf, pstring, len);

    /*
     * pstring should never be null according to input param 'val'.
     * This function is only invoked when val is not null.
     */
    pfree_ext(pstring);
    if (typIsVarlena && (val != newval))
        pfree(DatumGetPointer(newval));
}

/*
 * pgxc_rq_fire_bstriggers:
 * BEFORE STATEMENT triggers to be fired for a user-supplied DML query.
 * For non-FQS query, we internally generate remote DML query to be executed
 * for each row to be processed. But we do not want to explicitly fire triggers
 * for such a query; ExecModifyTable does that for us. It is the FQS DML query
 * where we need to explicitly fire statement triggers on coordinator. We
 * cannot run stmt triggers on datanode. While we can fire stmt trigger on
 * datanode versus coordinator based on the function shippability, we cannot
 * do the same for FQS query. The datanode has no knowledge that the trigger
 * being fired is due to a non-FQS query or an FQS query. Even though it can
 * find that all the triggers are shippable, it won't know whether the stmt
 * itself has been FQSed. Even though all triggers were shippable, the stmt
 * might have been planned on coordinator due to some other non-shippable
 * clauses. So the idea here is to *always* fire stmt triggers on coordinator.
 * Note that this does not prevent the query itself from being FQSed. This is
 * because we separately fire stmt triggers on coordinator.
 */
static void pgxc_rq_fire_bstriggers(RemoteQueryState* node)
{
    RemoteQuery* rq = (RemoteQuery*)node->ss.ps.plan;
    EState* estate = node->ss.ps.state;

    /* If it's not an internally generated query, fire BS triggers */
    if (!rq->rq_params_internal && estate->es_result_relations) {
        Assert(rq->remote_query);
        switch (rq->remote_query->commandType) {
            case CMD_INSERT:
                ExecBSInsertTriggers(estate, estate->es_result_relations);
                break;
            case CMD_UPDATE:
                ExecBSUpdateTriggers(estate, estate->es_result_relations);
                break;
            case CMD_DELETE:
                ExecBSDeleteTriggers(estate, estate->es_result_relations);
                break;
            default:
                break;
        }
    }
}

/*
 * pgxc_rq_fire_astriggers:
 * AFTER STATEMENT triggers to be fired for a user-supplied DML query.
 * See comments in pgxc_rq_fire_astriggers()
 */
static void pgxc_rq_fire_astriggers(RemoteQueryState* node)
{
    RemoteQuery* rq = (RemoteQuery*)node->ss.ps.plan;
    EState* estate = node->ss.ps.state;

    /* If it's not an internally generated query, fire AS triggers */
    if (!rq->rq_params_internal && estate->es_result_relations) {
        Assert(rq->remote_query);
        switch (rq->remote_query->commandType) {
            case CMD_INSERT:
                ExecASInsertTriggers(estate, estate->es_result_relations);
                break;
            case CMD_UPDATE:
                ExecASUpdateTriggers(estate, estate->es_result_relations);
                break;
            case CMD_DELETE:
                ExecASDeleteTriggers(estate, estate->es_result_relations);
                break;
            default:
                break;
        }
    }
}

bool IsInheritor(Oid relid)
{
    Relation pginherits;
    SysScanDesc scan;
    ScanKeyData key[1];
    bool ret = false;

    if (InvalidOid == relid)
        return ret;

    pginherits = heap_open(InheritsRelationId, AccessShareLock);
    ScanKeyInit(&key[0], Anum_pg_inherits_inhrelid, BTEqualStrategyNumber, F_OIDEQ, relid);
    scan = systable_beginscan(pginherits, InheritsRelidSeqnoIndexId, true, NULL, 1, key);
    if (HeapTupleIsValid(systable_getnext(scan))) {
        ret = true;
    }
    systable_endscan(scan);
    heap_close(pginherits, AccessShareLock);

    return ret;
}

/*
 * Description: Construct query string for fetch statistics from system table of data nodes.
 *
 * Parameters:
 *	@in stmt: the statment for analyze or vacuum command
 *	@in schemaname: the schema name of the relation for analyze or vacuum
 *	@in relname: the relname for analyze or vacuum command
 *	@in va_cols: the columns of the relation for analyze or vacuum
 *	@in kind: which type of statistic we will get from DN, pg_class/pg_statistic/pg_partition
 *	@in relid: relation oid for analyze command
 *	@in parentRel: the parent relation's stmt for delta table, this is NULL for non delta table
 *
 * @return: void
 */
static char* construct_fetch_statistics_query(const char* schemaname, const char* relname, List* va_cols,
    StatisticKind kind, VacuumStmt* stmt, Oid relid, RangeVar* parentRel)
{
    /* there is no tuples in pg_class for complex table, but it has pg_statistic. */
    if (stmt && IS_PGXC_COORDINATOR && !IsConnFromCoord() && (StatisticPageAndTuple == kind)) {
        return NULL;
    }

#define relLiteral(a) repairObjectName(a)
#define nspLiteral(a) (strcasecmp(a, "pg_temp") ? repairObjectName(a) : repairTempNamespaceName(a))

    char* tablename = (char*)relname;
    char* nspname = (char*)schemaname;
    Oid namespaceId = LookupNamespaceNoError(schemaname);

    StringInfo query = makeStringInfo();

    /* set guc paramter to force all table do indexscan */
    appendStringInfo(query, "set enable_seqscan = off;set enable_index_nestloop = on;set enable_indexscan = on;");

    switch (kind) {
        case StatisticPageAndTuple:
            /* judge the delta table. If it is a delta table, you need to find delta table in pg_class to append query.
             */
            if (parentRel && IsCStoreNamespace(namespaceId) &&
                pg_strncasecmp(relname, "pg_delta", strlen("pg_delta")) == 0) {
                appendStringInfo(query,
                    "select /*+  nestloop(p g) nestloop(p c) nestloop(c n) indexscan(g pg_namespace_nspname_index) "
                    "indexscan(n pg_namespace_nspname_index) indexscan(c pg_class_oid_index) indexscan(p "
                    "pg_class_relname_nsp_index)*/ c.relpages,c.reltuples,c.relallvisible,c.relhasindex from pg_class "
                    "c "
                    "join pg_namespace n on n.oid=c.relnamespace and n.nspname='%s' "
                    "inner join pg_class p on c.oid=p.reldeltarelid and p.relname ='%s' "
                    "inner join pg_namespace g on p.relnamespace= g.oid and g.nspname = '%s';",
                    nspLiteral(nspname),
                    relLiteral(parentRel->relname),
                    nspLiteral(parentRel->schemaname));
            } else {
                appendStringInfo(query,
                    "select /*+ nestloop(c n) indexscan(n pg_namespace_nspname_index) indexscan(c "
                    "pg_class_relname_nsp_index)*/ relpages,reltuples,relallvisible,relhasindex from pg_class c "
                    "join pg_namespace n on n.oid=c.relnamespace where c.relname='%s' and n.nspname='%s';",
                    relLiteral(tablename),
                    nspLiteral(nspname));
            }
            break;

        case StatisticHistogram:
        case StatisticMultiHistogram: {
            const char* pgstat_name = (kind == StatisticHistogram) ? "pg_statistic" : "pg_statistic_ext";

            if (parentRel && IsCStoreNamespace(namespaceId) &&
                pg_strncasecmp(relname, "pg_delta", strlen("pg_delta")) == 0) {
                appendStringInfo(query,
                    "select /*+  nestloop(p g) nestloop(p c) nestloop(n c) nestloop(s c) indexscan(p "
                    "pg_class_relname_nsp_index) "
                    "indexscan(c pg_class_oid_index) indexscan(s %s_relid_kind_att_inh_index) indexscan(n "
                    "pg_namespace_oid_index) indexscan(g pg_namespace_nspname_index) */ "
                    "s.* from %s s join pg_class c on s.starelid=c.oid "
                    "join pg_namespace n on n.oid=c.relnamespace and n.nspname='%s' and s.stainherit=false "
                    "inner join pg_class p on c.oid = p.reldeltarelid and p.relname ='%s' "
                    "inner join pg_namespace g on g.oid=p.relnamespace and g.nspname = '%s' ",
                    pgstat_name,
                    pgstat_name,
                    nspLiteral(nspname),
                    relLiteral(parentRel->relname),
                    nspLiteral(parentRel->schemaname));
            } else {
                appendStringInfo(query,
                    "select /*+ nestloop(s c) nestloop(n c) indexscan(c pg_class_relname_nsp_index) indexscan(n "
                    "pg_namespace_nspname_index) indexscan(s %s_relid_kind_att_inh_index)*/ "
                    "s.* from %s s join pg_class c on s.starelid=c.oid "
                    "join pg_namespace n on n.oid=c.relnamespace where relname='%s' and "
                    "n.nspname='%s' ",
                    pgstat_name,
                    pgstat_name,
                    relLiteral(tablename),
                    nspLiteral(nspname));
            }

            /* stainherit is false when select pg_statistic from dfs or delta table, and is true for complex. */
            if (stmt && IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
                (stmt->pstGlobalStatEx[stmt->tableidx].eAnalyzeMode != ANALYZENORMAL)) {
                switch (stmt->pstGlobalStatEx[stmt->tableidx].eAnalyzeMode) {
                    case ANALYZEDELTA:
                        appendStringInfoString(query, "and s.stainherit=false ");
                        break;
                    default:
                        return NULL;
                }
            }

            if (list_length(va_cols) > 0) {
                Relation rel = relation_open(relid, ShareUpdateExclusiveLock);
                if (kind == StatisticHistogram) {
                    Bitmapset* bms_single_cols = NULL;
                    StringInfo single_cols = makeStringInfo();

                    ListCell* col = NULL;
                    foreach (col, va_cols) {
                        Node* col_node = (Node*)lfirst(col);

                        Assert(IsA(col_node, String));

                        int attnum = attnameAttNum(rel, strVal(col_node), false);

                        if (attnum == InvalidAttrNumber || bms_is_member(attnum, bms_single_cols))
                            continue;

                        bms_single_cols = bms_add_member(bms_single_cols, attnum);

                        if (0 < single_cols->len)
                            appendStringInfoString(single_cols, ",");

                        appendStringInfo(single_cols, "%d", attnum);
                    }
                    if (single_cols->len > 0)
                        appendStringInfo(query, " and s.staattnum in (%s) order by staattnum", single_cols->data);
                    bms_free(bms_single_cols);
                    pfree(single_cols->data);
                } else if (kind == StatisticMultiHistogram) {
                    List* bmslist_multi_cols = NIL;
                    StringInfo ext_info = makeStringInfo();
                    Bitmapset* bms_multi_attnum = NULL;

                    ListCell* col = NULL;
                    foreach (col, va_cols) {
                        Node* col_node = (Node*)lfirst(col);

                        Assert(IsA(col_node, List));

                        if (!RelationIsRelation(rel))
                            continue;

                        ListCell* lc = NULL;
                        foreach (lc, (List*)col_node) {
                            Node* m_attname = (Node*)lfirst(lc);
                            int attnum = -1;

                            Assert(IsA(m_attname, String));
                            attnum = attnameAttNum(rel, strVal(m_attname), false);
                            bms_multi_attnum = bms_add_member(bms_multi_attnum, attnum);
                        }

                        int ori_size = list_length(bmslist_multi_cols);
                        bmslist_multi_cols = es_attnum_bmslist_add_unique_item(bmslist_multi_cols, bms_multi_attnum);

                        if (ori_size == list_length(bmslist_multi_cols))
                            continue;

                        int2 attnum = -1;
                        StringInfoData ext_info_str;
                        {
                            initStringInfo(&ext_info_str);
                            appendStringInfo(&ext_info_str, "'");

                            for (int i = 0; (attnum = bms_next_member(bms_multi_attnum, attnum)) > 0; ++i) {
                                if (i != 0) {
                                    appendStringInfo(&ext_info_str, " ");
                                }
                                appendStringInfo(&ext_info_str, " %d", attnum);
                            }

                            appendStringInfo(&ext_info_str, "'::int2vector");
                        }

                        if (ext_info->len > 0)
                            appendStringInfoString(ext_info, ",");

                        appendStringInfo(ext_info, "%s", ext_info_str.data);
                    }
                    if (ext_info->len > 0)
                        appendStringInfo(query, " and s.stakey in (%s) order by stakey", ext_info->data);
                    list_free_deep(bmslist_multi_cols);
                    pfree(ext_info->data);
                }
                relation_close(rel, ShareUpdateExclusiveLock);
            }
            appendStringInfoString(query, ";");
        } break;

        case StatisticPartitionPageAndTuple:
            appendStringInfo(query,
                "select /*+ nestloop(c p) nestloop(n c) indexscan(c pg_class_relname_nsp_index) indexscan(p "
                "pg_partition_partoid_index) indexscan(n pg_namespace_nspname_index)*/ "
                "p.relname,p.parttype,p.relpages,p.reltuples,p.relallvisible from "
                "pg_partition p join pg_class c on c.oid=p.parentid join pg_namespace n "
                "on n.oid=c.relnamespace where (p.parttype='p' or p.parttype='x') and "
                "c.relname='%s' and n.nspname='%s';",
                relLiteral(tablename),
                nspLiteral(nspname));
            break;

        default:
            return NULL;
    }

    elog(DEBUG1, "Fetch statistics from 1st datanode with query:%s", query->data);

    return query->data;
}

/*
 * These statistics come from system table of data nodes.
 * We cann't simply "select" by normal, because optimizer will intercept it.
 * So we send query directly here, meanwhile omit a long process on cooridinator,
 * including parse,analyze,rewrite,optimize,execute.
 * @in stmt - the statment for analyze or vacuum command
 * @in schemaname - the schema name of the relation for analyze or vacuum
 * @in relname - the relname for analyze or vacuum command
 * @in va_cols - the columns of the relation for analyze or vacuum
 * @in kind - which type of statistic we will get from DN, pg_class/pg_statistic/pg_partition
 * @in reltuples - it will get estimate reltuples for hdfs forign table of global stats
 * @in isReplication - if the relation is replication, we will identify if we get dirty data from datanode1
 * @in parentRel: the parent relation's stmt for delta table, this is NULL for non delta table
 * @return: void
 */
static void FetchStatisticsInternal(const char* schemaname, const char* relname, List* va_cols, StatisticKind kind,
    RangeVar* parentRel, VacuumStmt* stmt, bool isReplication)
{
    List* nodeList = NIL;
    int dn_conn_count, i;
    PGXCNodeAllHandles* pgxc_handles = NULL;
    PGXCNodeHandle** pgxc_connections = NULL;
    RemoteQueryState* remotestate = NULL;
    TupleTableSlot* scanslot = NULL;
    char* query_string = NULL;
    GlobalTransactionId gxid = GetCurrentTransactionId();
    Snapshot snapshot = GetActiveSnapshot();
    Oid namespaceId = LookupNamespaceNoError(schemaname);
    Oid relid = get_relname_relid(relname, namespaceId);

    /* Construct query string for fetch statistics from system table of data nodes. */
    query_string = construct_fetch_statistics_query(schemaname, relname, va_cols, kind, stmt, relid, parentRel);
    if (query_string == NULL) {
        return;
    }

    if (!stmt->isForeignTables) /* get global stats from dn1 for replication table. */
    {
        ExecNodes* nodes = RelidGetExecNodes(relid, false);
        nodeList = lappend_int(NIL, linitial_int(nodes->nodeList));
    } else /* @hfds foreign table Fetch from the nodeNo Data Node */
    {
        /*
         * we sholud use nodeNo identify the dn node if local cn get global stats for replication,
         * otherwise, we sholud use orgCnNodeNo identify the original cn node
         * if other cn get global stats from original cn.
         */
        nodeList = !IsConnFromCoord() ? lappend_int(nodeList, stmt->nodeNo) : lappend_int(nodeList, stmt->orgCnNodeNo);
    }

    pgxc_handles = get_handles(nodeList, NULL, false);
    pgxc_connections = pgxc_handles->datanode_handles;
    dn_conn_count = pgxc_handles->dn_conn_count;
    if (pgxc_connections == NULL)
        return;

    if (pgxc_node_begin(dn_conn_count, pgxc_connections, gxid, false, false, PGXC_NODE_DATANODE))
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Could not begin transaction on Datanodes")));
    for (i = 0; i < dn_conn_count; i++) {
        if (pgxc_connections[i]->state == DN_CONNECTION_STATE_QUERY)
            BufferConnection(pgxc_connections[i]);

        if (snapshot && pgxc_node_send_snapshot(pgxc_connections[i], snapshot)) {
            if (i < --dn_conn_count) {
                pgxc_connections[i] = pgxc_connections[dn_conn_count];
                i--;
            }
            continue;
        }
        if (pgxc_node_send_queryid(pgxc_connections[i], u_sess->debug_query_id) != 0) {
            add_error_message(pgxc_connections[i], "%s", "Can not send query ID");
            pfree_pgxc_all_handles(pgxc_handles);
            pfree_ext(query_string);
            return;
        }
        if (pgxc_node_send_query(pgxc_connections[i], query_string) != 0) {
            add_error_message(pgxc_connections[i], "%s", "Can not send request");
            pfree_pgxc_all_handles(pgxc_handles);
            pfree_ext(query_string);
            return;
        }
    }
    pfree_ext(query_string);

    if (dn_conn_count == 0) {
        return;
    }

    if (pgxc_node_receive(dn_conn_count, pgxc_connections, NULL)) {
        int error_code;
        char* error_msg = getSocketError(&error_code);

        pfree_ext(pgxc_connections);

        ereport(ERROR, (errcode(error_code), errmsg("Failed to read response from Datanodes Detail: %s\n", error_msg)));
    }

    // Consider statistics are same on any datanode by now.
    //
    remotestate = CreateResponseCombiner(0, COMBINE_TYPE_SAME);
    remotestate->request_type = REQUEST_TYPE_QUERY;
    scanslot = remotestate->ss.ss_ScanTupleSlot;

    while (dn_conn_count > 0) {
        i = 0;

        while (i < dn_conn_count) {
            int res = handle_response(pgxc_connections[i], remotestate);

            if (res == RESPONSE_TUPDESC) {
                if (scanslot == NULL)
                    scanslot = MakeSingleTupleTableSlot(remotestate->tuple_desc);
                else
                    ExecSetSlotDescriptor(scanslot, remotestate->tuple_desc);
            } else if (res == RESPONSE_DATAROW) {
                FetchTuple(remotestate, scanslot);
                tableam_tslot_getallattrs(scanslot);
                switch (kind) {
                    case StatisticPageAndTuple:
                        ReceivePageAndTuple(relid, scanslot, stmt);
                        break;
                    case StatisticHistogram:
                        ReceiveHistogram(relid, scanslot, isReplication);
                        break;
                    case StatisticMultiHistogram:
                        ReceiveHistogramMultiColStats(relid, scanslot, isReplication);
                        break;
                    case StatisticPartitionPageAndTuple:
                        ReceivePartitionPageAndTuple(relid, scanslot);
                        break;
                    default:
                        return;
                }
            } else if (res == RESPONSE_COMPLETE) {
                if (i < --dn_conn_count)
                    pgxc_handles->datanode_handles[i] = pgxc_handles->datanode_handles[dn_conn_count];
            } else if (res == RESPONSE_EOF) {
                if (pgxc_node_receive(1, &pgxc_handles->datanode_handles[i], NULL))
                    ereport(ERROR,
                        (errcode(ERRCODE_CONNECTION_EXCEPTION),
                            errmsg("Failed to read response from Datanode %u when ending query",
                                (pgxc_handles->datanode_handles[i])->nodeoid)));
            } else
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Unexpected response from Datanode %u", pgxc_connections[i]->nodeoid)));
        }
    }

    pgxc_node_report_error(remotestate);
}

void FetchGlobalStatistics(VacuumStmt* stmt, Oid relid, RangeVar* parentRel, bool isReplication)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

/*
 * @global stats
 * Get stadndistinct from DN1 if local is CN and receive analyze command from client.
 * Fetch global statistics information, if other CN has been told to fetch local table statistics information.
 * @in parentRel: the parent relation's stmt for delta table, this is NULL for non delta table
 */
static void FetchGlobalRelationStatistics(VacuumStmt* stmt, Oid relid, RangeVar* parentRel, bool isReplication)
{
    Relation rel = NULL;
    Oid indid;
    List* indexList = NIL;
    ListCell* indexId = NULL;
    char* indname = NULL;
    bool shouldfree = false;

    /* get schemaname and relation name */
    char* schemaname = NULL;
    char* relname = NULL;

    if (!OidIsValid(relid)) {
        Assert(stmt->relation != NULL);
        relname = stmt->relation->relname;
        if (stmt->relation->schemaname) {
            Oid namespaceId;

            schemaname = stmt->relation->schemaname;
            namespaceId = LookupNamespaceNoError(schemaname);
            relid = get_relname_relid(relname, namespaceId);
        } else {
            relid = RelnameGetRelid(stmt->relation->relname);
        }
    }

    rel = relation_open(relid, ShareUpdateExclusiveLock);

    if (IsSystemRelation(rel) || !check_analyze_permission(relid) || RelationIsContquery(rel) || 
        RelationIsView(rel) || RelationIsIndex(rel)) {
        /*
         * Don't fetch statistics if
         * 1) it is a system table since it do local analyze
         * 2) it is not permisision
         * 3) it is a view since it doesnot have stat info
         * 4) it is a sequence since it doesnot have stat info
         */
        relation_close(rel, ShareUpdateExclusiveLock);
        return;
    }

    if (relname == NULL) {
        relname = RelationGetRelationName(rel);
    }

    if (schemaname == NULL) {
        if (stmt->relation != NULL && stmt->relation->schemaname != NULL) {
            schemaname = stmt->relation->schemaname;
        } else {
            schemaname = get_namespace_name(RelationGetNamespace(rel));
            if (schemaname == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for namespace %u", RelationGetNamespace(rel))));
            }
            shouldfree = true;
        }
    }

    List *va_cols = NIL, *va_cols_multi = NIL;
    es_split_multi_column_stats(stmt->va_cols, &va_cols, &va_cols_multi);

#define FETCH_STATS_REPLICATION(relname, single_cols, multi_cols, has_cols)                                           \
    do {                                                                                                              \
        FetchStatisticsInternal(schemaname, relname, NIL, StatisticPageAndTuple, parentRel, stmt, true);              \
        if (!(has_cols) || (single_cols) != NIL)                                                                      \
            FetchStatisticsInternal(schemaname, relname, single_cols, StatisticHistogram, parentRel, stmt, true);     \
        if (!(has_cols) || (multi_cols) != NIL)                                                                       \
            FetchStatisticsInternal(schemaname, relname, multi_cols, StatisticMultiHistogram, parentRel, stmt, true); \
    } while (0)

#define FETCH_GLOBAL_STATS(relname, single_cols, multi_cols, has_cols)                                                \
    do {                                                                                                              \
        FetchGlobalStatisticsInternal(schemaname, relname, NIL, StatisticPageAndTuple, parentRel, stmt);              \
        if (!(has_cols) || (single_cols) != NIL)                                                                      \
            FetchGlobalStatisticsInternal(schemaname, relname, single_cols, StatisticHistogram, parentRel, stmt);     \
        if (!(has_cols) || (multi_cols) != NIL)                                                                       \
            FetchGlobalStatisticsInternal(schemaname, relname, multi_cols, StatisticMultiHistogram, parentRel, stmt); \
    } while (0)

    /* get info from DN1 for global stats. */
    if (!IsConnFromCoord()) {
        if (!isReplication) {
            FetchGlobalStatisticsInternal(schemaname, relname, stmt->va_cols, StatisticPageAndTuple, parentRel, stmt);

            /*
             * We don't need get dndistinct for foreign table because scheduler
             * assign different filelist for different datanode, so the dndistinct is no sense.
             * It should estimate dndistinct with poisson.
             */
            if (!stmt->isForeignTables) {
                if (stmt->va_cols == NIL || va_cols != NIL)
                    FetchGlobalStatisticsInternal(schemaname, relname, va_cols, StatisticHistogram, parentRel, stmt);
                if (stmt->va_cols == NIL || va_cols_multi != NIL)
                    FetchGlobalStatisticsInternal(
                        schemaname, relname, va_cols_multi, StatisticMultiHistogram, parentRel, stmt);
            }
        } else /* for replicate table, get stats from DN1, we have set stmt->totalRowCnts = 0 before here. */
        {
            FETCH_STATS_REPLICATION(relname, va_cols, va_cols_multi, (stmt->va_cols != NIL));

            indexList = RelationGetIndexList(rel);
            foreach (indexId, indexList) {
                indid = lfirst_oid(indexId);
                indname = GetIndexNameForStat(indid, relname);
                if (indname == NULL) {
                    continue;
                }
                FETCH_STATS_REPLICATION(indname, va_cols, va_cols_multi, (stmt->va_cols != NIL));
                pfree_ext(indname);
            }
        }

        if (RelationIsPartitioned(rel))
            FetchStatisticsInternal(
                schemaname, relname, stmt->va_cols, StatisticPartitionPageAndTuple, parentRel, stmt, isReplication);
    } else /* other CNs get stats */
    {
        stmt->tableidx = ANALYZENORMAL;
        stmt->pstGlobalStatEx[stmt->tableidx].eAnalyzeMode = ANALYZENORMAL;

        /* for global stats, other CNs get stats from the CN which do analyze. */
        FETCH_GLOBAL_STATS(relname, va_cols, va_cols_multi, (stmt->va_cols != NIL));

        if (RelationIsPartitioned(rel))
            FetchGlobalStatisticsInternal(
                schemaname, relname, stmt->va_cols, StatisticPartitionPageAndTuple, parentRel, stmt);

        indexList = RelationGetIndexList(rel);
        foreach (indexId, indexList) {
            indid = lfirst_oid(indexId);
            indname = GetIndexNameForStat(indid, relname);
            if (indname == NULL) {
                continue;
            }
            FETCH_GLOBAL_STATS(indname, va_cols, va_cols_multi, (stmt->va_cols != NIL));
            pfree_ext(indname);
        }
    }

    if (shouldfree) {
        pfree_ext(schemaname);
    }
    list_free_ext(va_cols);
    list_free_ext(va_cols_multi);

    relation_close(rel, NoLock);
}

void FetchGlobalPgfdwStatistics(VacuumStmt* stmt, bool has_var, PGFDWTableAnalyze* info)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

/**
 * @global stats
 * @Description: send query string to target CN or DN for get Page/Tuple/Histogram
 * @in conn_count - count of target connection
 * @in pgxc_connections - target connection information
 * @in pgxc_handles - all the handles involved in a transaction
 * @in gxid - global transaction id
 * @in snapshot - snapshot for transaction
 * @in query - query string for get Page/Tuple/Histogram
 * @in exec_type - execute on datanodes or coordinator
 * @return: int - 0: success; -1: fail
 *
 * Build connections with execute nodes(datanodes or coordinator) through send begin and query string.
 */
static int SendQueryToExecNode(int conn_count, PGXCNodeHandle** pgxc_connections, PGXCNodeAllHandles* pgxc_handles,
    GlobalTransactionId gxid, Snapshot snapshot, char* query, RemoteQueryExecType exec_type)
{
    if (pgxc_node_begin(conn_count, pgxc_connections, gxid, false, false, PGXC_NODE_DATANODE)) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("Could not begin transaction on %s",
                    (exec_type == EXEC_ON_DATANODES) ? "Datanodes" : "Coordinator")));
    }

    for (int i = 0; i < conn_count; i++) {
        if (pgxc_connections[i]->state == DN_CONNECTION_STATE_QUERY)
            BufferConnection(pgxc_connections[i]);

        if (snapshot && pgxc_node_send_snapshot(pgxc_connections[i], snapshot)) {
            if (i < --conn_count) {
                pgxc_connections[i] = pgxc_connections[conn_count];
                i--;
            }
            continue;
        }
        if (pgxc_node_send_queryid(pgxc_connections[i], u_sess->debug_query_id) != 0) {
            add_error_message(pgxc_connections[i], "%s", "Can not send query ID");
            pfree_pgxc_all_handles(pgxc_handles);
            pfree_ext(query);
            return -1;
        }
        if (pgxc_node_send_query(pgxc_connections[i], query) != 0) {
            add_error_message(pgxc_connections[i], "%s", "Can not send request");
            pfree_pgxc_all_handles(pgxc_handles);
            pfree_ext(query);
            return -1;
        }
    }
    pfree_ext(query);
    query = NULL;

    if (conn_count == 0) {
        return -1;
    }

    if (pgxc_node_receive(conn_count, pgxc_connections, NULL)) {
        int error_code;
        char* error_msg = getSocketError(&error_code);

        pfree_ext(pgxc_connections);

        ereport(ERROR, (errcode(error_code), errmsg("Failed to read response from Datanodes Detail: %s\n", error_msg)));
    }

    return 0;
}

/**
 * @global stats
 * @Description: other CNs will get statistic info(Page/Tuple/Histogram) from the original CN which
 *                      receive analyze command.
 * @in cn_conn_count - count of target coordinator connection
 * @in pgxc_connections - target coordinator connection information
 * @in remotestate - state of remote query node
 * @in kind - identify which statistic info we should get, relpage/reltuple/histogram
 * @in relid - relation oid which table we should get statistic
 * @return: void
 *
 */
static void FetchGlobalStatisticsFromCN(int cn_conn_count, PGXCNodeHandle** pgxc_connections,
    RemoteQueryState* remotestate, StatisticKind kind, VacuumStmt* stmt, Oid relid, PGFDWTableAnalyze* info)
{
    int i;
    TupleTableSlot* scanslot = NULL;

    while (cn_conn_count > 0) {
        i = 0;

        if (pgxc_node_receive(cn_conn_count, pgxc_connections, NULL))
            break;

        while (i < cn_conn_count) {
            /* read messages */
            int res = handle_response(pgxc_connections[i], remotestate);

            if (res == RESPONSE_TUPDESC) {
                /*
                 * Now tuple table slot is responsible for freeing the descriptor
                 */
                if (scanslot == NULL)
                    scanslot = MakeSingleTupleTableSlot(remotestate->tuple_desc);
                else
                    ExecSetSlotDescriptor(scanslot, remotestate->tuple_desc);
            } else if (res == RESPONSE_DATAROW) {
                /*
                 * We already have a tuple and received another one.
                 */
                FetchTuple(remotestate, scanslot);
                tableam_tslot_getallattrs(scanslot);
                /* We already get statistic info(Page/Tuple/Histogram) and update in local. */
                switch (kind) {
                    case StatisticPageAndTuple:
                        ReceivePageAndTuple(relid, scanslot, stmt);
                        break;
                    case StatisticHistogram:
                        ReceiveHistogram(relid, scanslot, false, info);
                        break;
                    case StatisticMultiHistogram:
                        ReceiveHistogramMultiColStats(relid, scanslot, false, info);
                        break;
                    case StatisticPartitionPageAndTuple:
                        ReceivePartitionPageAndTuple(relid, scanslot);
                        break;
                    default:
                        return;
                }
            } else if (res == RESPONSE_COMPLETE) {
                if (i < --cn_conn_count)
                    pgxc_connections[i] = pgxc_connections[cn_conn_count];
            } else if (res == RESPONSE_EOF) {
                if (pgxc_node_receive(1, &pgxc_connections[i], NULL))
                    ereport(ERROR,
                        (errcode(ERRCODE_CONNECTION_EXCEPTION),
                            errmsg("Failed to read response from CN %u when ending query",
                                (pgxc_connections[i])->nodeoid)));
            } else
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Unexpected response from CN %u", pgxc_connections[i]->nodeoid)));
        }
    }
}

/**
 * @global stats
 * @Description: Fetch relpage and reltuple from pg_class in DN1 using to estimate global relpages.
 *				Fetch stadndistinct from pg_statistic in DN1 and update local.
 * @in dn_conn_count - count of target datanodes connection
 * @in pgxc_connections - target datanodes connection information
 * @in remotestate - state of remote query node
 * @in kind - identify which statistic info we should get, relpage/reltuple/histogram
 * @in stmt - analyze or vacuum statement, we will use totalRowCnts to estimate global relpages
 * @in relid - relation oid which table we should get statistic
 * @return: void
 *
 */
static void FetchGlobalStatisticsFromDN(int dn_conn_count, PGXCNodeHandle** pgxc_connections,
    RemoteQueryState* remotestate, StatisticKind kind, VacuumStmt* stmt, Oid relid)
{
    int i, colno;
    TupleTableSlot* scanslot = NULL;
    bool* bRecvedAttnum = (bool*)palloc0(stmt->pstGlobalStatEx[stmt->tableidx].attnum * sizeof(bool));

    while (dn_conn_count > 0) {
        i = 0;
        if (pgxc_node_receive(dn_conn_count, pgxc_connections, NULL))
            break;

        while (i < dn_conn_count) {
            /* read messages */
            int res = handle_response(pgxc_connections[i], remotestate);

            if (res == RESPONSE_TUPDESC) {
                /*
                 * Now tuple table slot is responsible for freeing the descriptor
                 */
                if (scanslot == NULL)
                    scanslot = MakeSingleTupleTableSlot(remotestate->tuple_desc);
                else
                    ExecSetSlotDescriptor(scanslot, remotestate->tuple_desc);
            } else if (res == RESPONSE_DATAROW) {
                /*
                 * We already have a tuple and received another one.
                 */
                FetchTuple(remotestate, scanslot);
                tableam_tslot_getallattrs(scanslot);
                switch (kind) {
                    case StatisticPageAndTuple: {
                        double reltuples = 0;
                        double relpages = 0;
                        /* Received relpage and reltuple from pg_class in DN1 */
                        RelPageType dn_pages = (RelPageType)DatumGetFloat8(scanslot->tts_values[0]);
                        double dn_tuples = (double)DatumGetFloat8(scanslot->tts_values[1]);
                        stmt->pstGlobalStatEx[stmt->tableidx].dn1totalRowCnts = dn_tuples;
                        reltuples = stmt->pstGlobalStatEx[stmt->tableidx].totalRowCnts;

                        if (0 < dn_pages && 0 < dn_tuples) {
                            /* Estimate global relpages. */
                            relpages = ceil(stmt->pstGlobalStatEx[stmt->tableidx].totalRowCnts / dn_tuples * dn_pages);
                        } else {
                            elog(LOG,
                                "fetch stats info from first datanode: reltuples = %lf relpages = %lf",
                                dn_tuples,
                                dn_pages);
                            if (dn_pages > 0) {
                                relpages = dn_pages * *t_thrd.pgxc_cxt.shmemNumDataNodes;
                            } else if (reltuples > 0) {
                                double est_cu_num = 0;
                                double est_cu_pages = 0;
                                Relation rel = heap_open(relid, NoLock);
                                int32 relwidth = get_relation_data_width(relid, InvalidOid, NULL);

                                if (RelationIsColStore(rel)) {
                                    /* refer to estimate_cstore_blocks */
                                    est_cu_num = ceil(reltuples / RelDefaultFullCuSize);
                                    est_cu_pages = (double)(RelDefaultFullCuSize * relwidth / BLCKSZ);
                                    relpages = ceil(est_cu_num * est_cu_pages);
                                } else if (RelationIsPAXFormat(rel)) {
                                    /* refer to estimate_cstore_blocks */
                                    est_cu_num = (reltuples * relwidth) / BLCKSZ;
                                    est_cu_pages = ESTIMATE_BLOCK_FACTOR;
                                    relpages = ceil(est_cu_num * est_cu_pages);
                                } else {
                                    /* Estimate global pages as normal. */
                                    relpages = ceil(reltuples * relwidth / BLCKSZ);
                                }
                                heap_close(rel, NoLock);
                            }
                        }
                        BlockNumber relallvisible = (BlockNumber)DatumGetInt32(scanslot->tts_values[2]);
                        relallvisible = (BlockNumber)floor(stmt->DnCnt * relallvisible + 0.5);
                        if (relallvisible > relpages) {
                            relallvisible = (BlockNumber)relpages;
                        }
                        scanslot->tts_values[0] = Float8GetDatum(relpages);
                        scanslot->tts_values[1] = Float8GetDatum(reltuples);
                        scanslot->tts_values[2] = UInt32GetDatum(relallvisible);
                        /* Update pg_class in local. */
                        ReceivePageAndTuple(relid, scanslot, stmt);
                        break;
                    }
                    case StatisticHistogram: {
                        colno = scanslot->tts_values[Anum_pg_statistic_staattnum - 1];

                        /*
                         * If received current attno from pg_statistic in DN1 more than the attnum
                         * of analyzing table, It identify have concurrency with delete/insert column
                         * under analyzing. We should realloc memory of dndistinct and bRecvedAttnum.
                         */
                        if (colno > stmt->pstGlobalStatEx[stmt->tableidx].attnum) {
                            bRecvedAttnum = (bool*)repalloc(bRecvedAttnum, colno * sizeof(bool));
                            bRecvedAttnum[colno - 1] = false;
                            stmt->pstGlobalStatEx[stmt->tableidx].dndistinct = (double*)repalloc(
                                stmt->pstGlobalStatEx[stmt->tableidx].dndistinct, colno * sizeof(double));
                            stmt->pstGlobalStatEx[stmt->tableidx].correlations = (double*)repalloc(
                                stmt->pstGlobalStatEx[stmt->tableidx].correlations, colno * sizeof(double));
                            stmt->pstGlobalStatEx[stmt->tableidx].dndistinct[colno - 1] = 1;
                            stmt->pstGlobalStatEx[stmt->tableidx].correlations[colno - 1] = 1;
                            stmt->pstGlobalStatEx[stmt->tableidx].attnum = colno;
                        }

                        if (!bRecvedAttnum[colno - 1]) {
                            if (!scanslot->tts_isnull[Anum_pg_statistic_stadistinct - 1]) {
                                stmt->pstGlobalStatEx[stmt->tableidx].dndistinct[colno - 1] =
                                    DatumGetFloat4(scanslot->tts_values[Anum_pg_statistic_stadistinct - 1]);
                                if (stmt->pstGlobalStatEx[stmt->tableidx].dndistinct[colno - 1] == 0) {
                                    stmt->pstGlobalStatEx[stmt->tableidx].dndistinct[colno - 1] = 1;
                                    elog(LOG, "the collumn[%d] have no distinct on DN1", colno);
                                }
                            }

                            /*
                             * Get correlations of each column from dn1, because the value of
                             * correlations can't accurately if analyze with sample table.
                             */
                            if (stmt->sampleTableRequired) {
                                for (int stakindno = 0; stakindno < STATISTIC_NUM_SLOTS; stakindno++) {
                                    int j = Anum_pg_statistic_stakind1 - 1 + stakindno;
                                    int k = Anum_pg_statistic_stanumbers1 - 1 + stakindno;

                                    if (!scanslot->tts_isnull[j] &&
                                        STATISTIC_KIND_CORRELATION == DatumGetInt16(scanslot->tts_values[j])) {
                                        Oid foutoid;
                                        bool typisvarlena = false;
                                        char *corrs = NULL, *tmp = NULL;
                                        getTypeOutputInfo(
                                            scanslot->tts_tupleDescriptor->attrs[k].atttypid, &foutoid, &typisvarlena);
                                        corrs = OidOutputFunctionCall(foutoid, scanslot->tts_values[k]);
                                        while (corrs != NULL) {
                                            if (*corrs == '{')
                                                tmp = corrs + 1;
                                            else if (*corrs == '}') {
                                                *corrs = '\0';
                                                break;
                                            }

                                            corrs++;
                                        }

                                        stmt->pstGlobalStatEx[stmt->tableidx].correlations[colno - 1] = atof(tmp);
                                        break;
                                    }
                                }
                            }

                            bRecvedAttnum[colno - 1] = true;
                        }

                        break;
                    }
                    case StatisticMultiHistogram:
                        break;
                    case StatisticPartitionPageAndTuple:
                        ReceivePartitionPageAndTuple(relid, scanslot);
                        /* fall through */
                    default:
                        return;
                }
            } else if (res == RESPONSE_COMPLETE) {
                if (i < --dn_conn_count)
                    pgxc_connections[i] = pgxc_connections[dn_conn_count];
            } else if (res == RESPONSE_EOF) {
                /* incomplete message, read more */
                if (pgxc_node_receive(1, &pgxc_connections[i], NULL))
                    ereport(ERROR,
                        (errcode(ERRCODE_CONNECTION_EXCEPTION),
                            errmsg("Failed to read response from DN %u when ending query",
                                (pgxc_connections[i])->nodeoid)));
            } else
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("Unexpected response from DN %u", pgxc_connections[i]->nodeoid)));
        }
    }

    pfree_ext(bRecvedAttnum);
    bRecvedAttnum = NULL;
}

typedef struct {
    Oid nodeoid;
    StringInfo explain;
} DnExplain;

/*
 * check whether prepared statement is ready in DN exec node.
 */
bool CheckPrepared(RemoteQuery* rq, Oid nodeoid)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

void FindExecNodesInPBE(RemoteQueryState* planstate, ExecNodes* exec_nodes, RemoteQueryExecType exec_type)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

/**
 * @Description: send query string to DN.
 * @in pgxc_connections - target connection information
 * @in pgxc_handles - all the handles involved in a transaction
 * @in query - explain/explain verbose query string
 * @return: int - 0: success; -1: fail
 *
 * Build connections with execute datanodes through send query string.
 */
static int SendQueryToExecDN(PGXCNodeHandle** pgxc_connections, PGXCNodeAllHandles* pgxc_handles, const char* query)
{
    GlobalTransactionId gxid = GetCurrentTransactionId();
    Snapshot snapshot = GetActiveSnapshot();

    /* begin transaction for all datanodes */
    if (pgxc_node_begin(1, pgxc_connections, gxid, false, false, PGXC_NODE_DATANODE)) {
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("Could not begin transaction on Datanode.")));
    }

    if (pgxc_connections[0]->state == DN_CONNECTION_STATE_QUERY)
        BufferConnection(pgxc_connections[0]);

    if (snapshot && pgxc_node_send_snapshot(pgxc_connections[0], snapshot)) {
        pfree_pgxc_all_handles(pgxc_handles);
        return -1;
    }

    /*
     * Send queryid and query, but should not free query,
     * because query need to be used multiple times.
     */
    if (pgxc_node_send_queryid(pgxc_connections[0], u_sess->debug_query_id) != 0) {
        add_error_message(pgxc_connections[0], "%s", "Can not send query ID");
        pfree_pgxc_all_handles(pgxc_handles);
        return -1;
    }
    if (pgxc_node_send_query(pgxc_connections[0], query) != 0) {
        add_error_message(pgxc_connections[0], "%s", "Can not send request");
        pfree_pgxc_all_handles(pgxc_handles);
        return -1;
    }

    if (pgxc_node_receive(1, pgxc_connections, NULL)) {
        int error_code;
        char* error_msg = getSocketError(&error_code);

        pfree_ext(pgxc_connections);

        ereport(ERROR, (errcode(error_code), errmsg("Failed to read response from Datanodes Detail: %s\n", error_msg)));
    }

    return 0;
}

StringInfo* SendExplainToDNs(ExplainState*, RemoteQuery*, int*, const char*)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * @global stats
 * Get stadndistinct from DN1 if local is CN and receive analyze command from client.
 * Fetch global statistics information, if other CN has been told to fetch local table statistics information.
 *.@in parentRel: the parent relation's stmt for delta table, this is NULL for non delta table
 */
static void FetchGlobalStatisticsInternal(const char* schemaname, const char* relname, List* va_cols,
    StatisticKind kind, RangeVar* parentRel, VacuumStmt* stmt)
{
    List* nodeList = NIL;
    int co_conn_count = 0, dn_conn_count = 0;
    PGXCNodeAllHandles* pgxc_handles = NULL;
    PGXCNodeHandle** pgxc_connections = NULL;
    RemoteQueryState* remotestate = NULL;
    RemoteQueryExecType exec_type;
    char* query_string = NULL;
    GlobalTransactionId gxid = GetCurrentTransactionId();
    Snapshot snapshot = GetActiveSnapshot();
    Oid namespaceId = LookupNamespaceNoError(schemaname);
    Oid relid = get_relname_relid(relname, namespaceId);
    if (!OidIsValid(relid)) {
        ereport(ERROR, (errmsg("%s.%s not found when analyze fetching global statistics.", schemaname, relname)));
        return;
    }

    /* Construct query string for fetch statistics from system table of data nodes. */
    query_string = construct_fetch_statistics_query(schemaname, relname, va_cols, kind, stmt, relid, parentRel);
    if (query_string == NULL) {
        return;
    }

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        /*
         * for other table: fetch stats in pg_class and pg_statistic from DN1 if non-hdfs foreign table;
         * otherwise, fetch stats from the determined DN of scheduler.
         */
        ExecNodes* exec_nodes = getRelationExecNodes(relid);
        nodeList = stmt->isForeignTables ? lappend_int(nodeList, stmt->nodeNo)
                                         : lappend_int(nodeList, linitial_int(exec_nodes->nodeList));
        pgxc_handles = get_handles(nodeList, NULL, false);
        pgxc_connections = pgxc_handles->datanode_handles;
        dn_conn_count = pgxc_handles->dn_conn_count;
        exec_type = EXEC_ON_DATANODES;
    } else {
        /*
         * for foreign tables: fetch stats from nodeNo Data Node
         * global stats: other coordinators will get statistics from coordinator node identified by nodeNo.
         */
        Assert(stmt->orgCnNodeNo >= 0);

        nodeList = lappend_int(nodeList, stmt->orgCnNodeNo);
        pgxc_handles = get_handles(NULL, nodeList, true);
        pgxc_connections = pgxc_handles->coord_handles;
        co_conn_count = pgxc_handles->co_conn_count;
        exec_type = EXEC_ON_COORDS;

        if (strcmp(pgxc_connections[0]->remoteNodeName, g_instance.attr.attr_common.PGXCNodeName) == 0) {
            ereport(ERROR, (errmsg("Fetch statistics from myself is unexpected. Maybe switchover happened.")));
            return;
        }
    }
    if (pgxc_connections == NULL)
        return;

    /* Send query string to target CN or DN for get Page/Tuple/Histogram. */
    int ret = SendQueryToExecNode((exec_type == EXEC_ON_DATANODES) ? dn_conn_count : co_conn_count,
        pgxc_connections,
        pgxc_handles,
        gxid,
        snapshot,
        query_string,
        exec_type);
    /* It should return if encounter some error. */
    if (ret != 0) {
        return;
    }

    remotestate = CreateResponseCombiner(0, COMBINE_TYPE_SAME);
    remotestate->request_type = REQUEST_TYPE_QUERY;

    if (exec_type == EXEC_ON_DATANODES) {
        /*
         * Fetch relpage and reltuple from pg_class in DN1 using to estimate global relpages.
         * Fetch stadndistinct from pg_statistic in DN1.
         */
        FetchGlobalStatisticsFromDN(dn_conn_count, pgxc_connections, remotestate, kind, stmt, relid);
    }

    else /* Make the same for Coordinators */
    {
        /* for global stats, other CNs will get statistic info from the original CN which do analyze. */
        FetchGlobalStatisticsFromCN(co_conn_count, pgxc_connections, remotestate, kind, stmt, relid, NULL);
    }

    pgxc_node_report_error(remotestate);
}

/*
 * @cooperation analysis
 * get really attnum
 */
bool PgfdwGetRelAttnum(int2vector* keys, PGFDWTableAnalyze* info)
{
    int attnum = 0;
    char** att_name = (char**)palloc0(keys->dim1 * sizeof(char*));

    for (int i = 0; i < info->natts; i++) {
        for (int j = 0; j < keys->dim1; j++) {
            if (info->attnum[i] == keys->values[j]) {
                att_name[attnum] = info->attname[i];
                attnum++;
                break;
            }
        }
    }

    if (keys->dim1 != attnum) {
        return false;
    }

    Relation rel = relation_open(info->relid, AccessShareLock);
    TupleDesc tupdesc = RelationGetDescr(rel);
    char* tup_attname = NULL;
    int* real_attnum = (int*)palloc0(attnum * sizeof(int));
    int total = 0;

    for (int i = 0; i < tupdesc->natts; i++) {
        for (int j = 0; j < attnum; j++) {
            tup_attname = tupdesc->attrs[i].attname.data;
            if (tup_attname && strcmp(tup_attname, att_name[j]) == 0) {
                real_attnum[total] = tupdesc->attrs[i].attnum;
                total++;
                break;
            }
        }
    }

    relation_close(rel, AccessShareLock);

    if (total != attnum) {
        return false;
    }

    for (int i = 0; i < keys->dim1; i++) {
        keys->values[i] = (int2)real_attnum[i];
    }
    return true;
}

/*
 * @cooperation analysis
 * get really attnum
 */
bool PgfdwGetRelAttnum(TupleTableSlot* slot, PGFDWTableAnalyze* info)
{
    int attnum = slot->tts_values[Anum_pg_statistic_staattnum - 1];
    char* tup_attname = NULL;
    char* att_name = NULL;
    int real_attnum = -1;

    for (int i = 0; i < info->natts; i++) {
        if (info->attnum[i] == attnum) {
            att_name = info->attname[i];
            break;
        }
    }

    if (att_name == NULL) {
        return false;
    }

    Relation rel = relation_open(info->relid, AccessShareLock);
    TupleDesc tupdesc = RelationGetDescr(rel);

    for (int i = 0; i < tupdesc->natts; i++) {
        tup_attname = tupdesc->attrs[i].attname.data;
        if (tup_attname && strcmp(tup_attname, att_name) == 0) {
            real_attnum = tupdesc->attrs[i].attnum;
            break;
        }
    }

    relation_close(rel, AccessShareLock);

    if (real_attnum == -1) {
        return false;
    }

    slot->tts_values[Anum_pg_statistic_staattnum - 1] = real_attnum;

    return true;
}

/*update pages, tuples, etc in pg_class */
static void ReceivePageAndTuple(Oid relid, TupleTableSlot* slot, VacuumStmt* stmt)
{
    Relation rel;
    Relation classRel;
    RelPageType relpages;
    double reltuples;
    BlockNumber relallvisible;
    bool hasindex = false;

    relpages = (RelPageType)DatumGetFloat8(slot->tts_values[0]);
    reltuples = (double)DatumGetFloat8(slot->tts_values[1]);

    relallvisible = (BlockNumber)DatumGetInt32(slot->tts_values[2]);
    hasindex = DatumGetBool(slot->tts_values[3]);
    rel = relation_open(relid, ShareUpdateExclusiveLock);
    classRel = heap_open(RelationRelationId, RowExclusiveLock);

    vac_update_relstats(rel, classRel, relpages, reltuples, relallvisible,
        hasindex, BootstrapTransactionId, InvalidMultiXactId);

    /* Save the flag identify is there dirty data in the relation. */
    if (stmt != NULL) {
        stmt->pstGlobalStatEx[stmt->tableidx].totalRowCnts = reltuples;
    }

    /*
     * we does not fetch dead tuples info from remote DN/CN, just set  deadtuples to 0. it does
     * not matter because we should fetch all deadtuples info from all datanodes to calculate a
     * user-definedtable's deadtuples info
     */
    if (!IS_PGXC_COORDINATOR || IsConnFromCoord())
        pgstat_report_analyze(rel, (PgStat_Counter)reltuples, (PgStat_Counter)0);

    heap_close(classRel, NoLock);
    relation_close(rel, NoLock);
}

/*
 * Handle statistics fetched from datanode.
 * in isReplication -identify the relation is replication table if it is true.
 */
static void ReceiveHistogram(Oid relid, TupleTableSlot* slot, bool isReplication, PGFDWTableAnalyze* info)
{
    MemoryContext oldcontext;
    Relation sd;
    Form_pg_attribute attForm;
    HeapTuple attTup, stup, oldtup;
    Oid atttypid;
    int attnum, atttypmod, i, j, k;
    bool replaces[Natts_pg_statistic];

    for (i = 0; i < Natts_pg_statistic; i++) {
        replaces[i] = true;
    }
    replaces[Anum_pg_statistic_starelid - 1] = false;
    replaces[Anum_pg_statistic_staattnum - 1] = false;

    /*
     * Step : Get true type of attribute
     */
    if (info != NULL) {
        Assert(relid == info->relid);

        if (Natts_pg_statistic != info->natts_pg_statistic) {
            ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                    errmsg("cooperation analysis: please update to the same version")));
        }

        if (!PgfdwGetRelAttnum(slot, info)) {
            return;
        }

        info->has_analyze = true;
    }
    attnum = slot->tts_values[Anum_pg_statistic_staattnum - 1];

    attTup = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int32GetDatum(attnum));
    if (!HeapTupleIsValid(attTup)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for attribute %d of relation %u", attnum, relid)));
    }

    attForm = (Form_pg_attribute)GETSTRUCT(attTup);

    /*
     * If a drop column operation happened between fetching sample rows and updating
     * pg_statistic, we don't have to further processing the dropped-column's stats
     * update, so just skip.
     */
    if (attForm->attisdropped) {
        elog(WARNING,
            "relation:%s's attnum:%d is droppred during ANALYZE, so skipped.",
            get_rel_name(attForm->attrelid),
            attForm->attnum);

        ReleaseSysCache(attTup);
        return;
    }

    atttypid = attForm->atttypid;
    atttypmod = attForm->atttypmod;

    ReleaseSysCache(attTup);

    /*
     * Step : Reconstruct staValues
     */
    oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);

    slot->tts_values[Anum_pg_statistic_starelid - 1] = relid;
    if (slot->tts_attinmeta == NULL)
        slot->tts_attinmeta = TupleDescGetAttInMetadata(slot->tts_tupleDescriptor);

    /* if the relation is replication for global stats, we should set stadndistinct the same as stadistinct from dn1. */
    if (u_sess->attr.attr_sql.enable_global_stats && isReplication && IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        /*
         * we should set stadndistinct as 1 if there is no global distinct.
         * because we consider stadndistinct is 0 as single stats.
         */
        if (slot->tts_values[Anum_pg_statistic_stadistinct - 1] == 0)
            slot->tts_values[Anum_pg_statistic_stadndistinct - 1] = Float4GetDatum(1);
        else
            slot->tts_values[Anum_pg_statistic_stadndistinct - 1] = slot->tts_values[Anum_pg_statistic_stadistinct - 1];
    }

    for (i = 0; i < STATISTIC_NUM_SLOTS; i++) {
        j = Anum_pg_statistic_stakind1 - 1 + i;
        k = Anum_pg_statistic_stavalues1 - 1 + i;

        if (!slot->tts_isnull[j]) {
            int t = DatumGetInt16(slot->tts_values[j]);

            if (0 != t && STATISTIC_KIND_HISTOGRAM != t) {
                Assert(!slot->tts_isnull[Anum_pg_statistic_stanumbers1 - 1 + i]);
            }

            if (STATISTIC_KIND_MCV == t || STATISTIC_KIND_HISTOGRAM == t || STATISTIC_KIND_MCELEM == t) {
                Assert(!slot->tts_isnull[k]);

                /* When stakindN = STATISTIC_KIND_MCELEM, element type of staValuesN is text by now */
                if (STATISTIC_KIND_MCELEM == t) {
                    atttypid = TEXTOID;
                    atttypmod = -1;
                }

                slot->tts_values[k] = FunctionCall3Coll(slot->tts_attinmeta->attinfuncs + k,
                    InvalidOid,
                    (Datum)slot->tts_values[k],
                    UInt32GetDatum(atttypid),
                    Int32GetDatum(atttypmod));
            }
        }
    }

    // Is there already a pg_statistic tuple for this attribute?
    //
    sd = heap_open(StatisticRelationId, RowExclusiveLock);
    MemoryContext current_context = MemoryContextSwitchTo(oldcontext);
    ResourceOwner asOwner, oldOwner1;
    /*
     * Create a resource owner to keep track of resources
     * in order to release resources when catch the exception.
     */
    asOwner = ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner, "update_stats",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    oldOwner1 = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = asOwner;

    PG_TRY();
    {
        oldtup = SearchSysCache4(STATRELKINDATTINH,
            ObjectIdGetDatum(slot->tts_values[Anum_pg_statistic_starelid - 1]),
            CharGetDatum(slot->tts_values[Anum_pg_statistic_starelkind - 1]),
            Int16GetDatum(slot->tts_values[Anum_pg_statistic_staattnum - 1]),
            BoolGetDatum(slot->tts_values[Anum_pg_statistic_stainherit - 1]));

        if (HeapTupleIsValid(oldtup)) {
            // Yes, replace it
            //
            stup = heap_modify_tuple(oldtup, RelationGetDescr(sd), slot->tts_values, slot->tts_isnull, replaces);
            ReleaseSysCache(oldtup);
            (void)simple_heap_update(sd, &stup->t_self, stup);
        } else {
            // No, insert new tuple
            //
            stup = heap_form_tuple(RelationGetDescr(sd), slot->tts_values, slot->tts_isnull);
            simple_heap_insert(sd, stup);
        }

        // update indexes too
        //
        CatalogUpdateIndexes(sd, stup);
    }
    PG_CATCH();
    {
        analyze_concurrency_process(slot->tts_values[Anum_pg_statistic_starelid - 1],
            slot->tts_values[Anum_pg_statistic_staattnum - 1],
            current_context,
            PG_FUNCNAME_MACRO);
    }
    PG_END_TRY();

    /* Release everything */
    ResourceOwnerRelease(asOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
    ResourceOwnerRelease(asOwner, RESOURCE_RELEASE_LOCKS, false, false);
    ResourceOwnerRelease(asOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, false);
    t_thrd.utils_cxt.CurrentResourceOwner = oldOwner1;
    ResourceOwnerDelete(asOwner);
    heap_close(sd, RowExclusiveLock);
}

/*
 * Handle Extended Statistics fetched from datanode.
 * in isReplication -identify the relation is replication table if it is true.
 */
static void ReceiveHistogramMultiColStats(Oid relid, TupleTableSlot* slot, bool isReplication, PGFDWTableAnalyze* info)
{
    MemoryContext oldcontext;
    HeapTuple stup, oldtup;
    int i, j, k;
    bool replaces[Natts_pg_statistic_ext];

    for (i = 0; i < Natts_pg_statistic_ext; i++) {
        replaces[i] = true;
    }
    replaces[Anum_pg_statistic_ext_starelid - 1] = false;
    replaces[Anum_pg_statistic_ext_stakey - 1] = false;

    /*
     * Step : Get true type of attribute
     */
    Oid atttypid;
    int atttypmod;
    Oid* atttypid_array = NULL;
    int* atttypmod_array = NULL;
    unsigned int num_column = 1;

    /* Mark as anyarray */
    atttypid = ANYARRAYOID;
    atttypmod = -1;

    int2vector* keys = (int2vector*)DatumGetPointer(slot->tts_values[Anum_pg_statistic_ext_stakey - 1]);
    if (info != NULL) {
        Assert(relid == info->relid);

        if (Natts_pg_statistic_ext != info->natts_pg_statistic_ext) {
            ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                    errmsg("cooperation analysis: please update to the same version")));
        }

        if (!PgfdwGetRelAttnum(keys, info)) {
            return;
        }
    }
    es_get_columns_typid_typmod(relid, keys, &atttypid_array, &atttypmod_array, &num_column);

    /*
     * Step : Reconstruct staValues
     */
    oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);

    slot->tts_values[Anum_pg_statistic_ext_starelid - 1] = relid;
    if (slot->tts_attinmeta == NULL)
        slot->tts_attinmeta = TupleDescGetAttInMetadata(slot->tts_tupleDescriptor);

    /* if the relation is replication for global stats, we should set stadndistinct the same as stadistinct from dn1. */
    if (u_sess->attr.attr_sql.enable_global_stats && isReplication && IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        /*
         * we should set stadndistinct as 1 if there is no global distinct.
         * because we consider stadndistinct is 0 as single stats.
         */
        if (0 == slot->tts_values[Anum_pg_statistic_ext_stadistinct - 1])
            slot->tts_values[Anum_pg_statistic_ext_stadndistinct - 1] = Float4GetDatum(1);
        else
            slot->tts_values[Anum_pg_statistic_ext_stadndistinct - 1] =
                slot->tts_values[Anum_pg_statistic_ext_stadistinct - 1];
    }

    for (i = 0; i < STATISTIC_NUM_SLOTS; i++) {
        j = Anum_pg_statistic_ext_stakind1 - 1 + i;
        k = Anum_pg_statistic_ext_stavalues1 - 1 + i;

        if (!slot->tts_isnull[j]) {
            int t = DatumGetInt16(slot->tts_values[j]);

            if (0 != t && STATISTIC_KIND_HISTOGRAM != t) {
                Assert(!slot->tts_isnull[Anum_pg_statistic_ext_stanumbers1 - 1 + i]);
            }

            if (STATISTIC_KIND_MCV == t || STATISTIC_KIND_NULL_MCV == t || STATISTIC_KIND_HISTOGRAM == t ||
                STATISTIC_KIND_MCELEM == t) {
                Assert(!slot->tts_isnull[k]);
                Oid atttypid_temp = atttypid;
                int atttypmod_temp = atttypmod;

                /* When stakindN = STATISTIC_KIND_MCELEM, element type of staValuesN is text by now */
                if (STATISTIC_KIND_MCELEM == t) {
                    atttypid_temp = TEXTOID;
                    atttypmod_temp = -1;
                }

                if (STATISTIC_KIND_NULL_MCV == t || STATISTIC_KIND_MCV == t) {
                    atttypid_temp = CSTRINGOID;
                    atttypmod_temp = -1;
                }

                slot->tts_values[k] = FunctionCall3Coll(slot->tts_attinmeta->attinfuncs + k,
                    InvalidOid,
                    (Datum)slot->tts_values[k],
                    UInt32GetDatum(atttypid_temp),
                    Int32GetDatum(atttypmod_temp));

                if (STATISTIC_KIND_NULL_MCV == t || STATISTIC_KIND_MCV == t) {
                    slot->tts_values[k] = es_mcv_slot_cstring_array_to_array_array(
                        (Datum)slot->tts_values[k], num_column, atttypid_array, atttypmod_array);
                }
            }
        }
    }

    /* In multi-column statistic, we are going to check & free the array of typid/typmode */
    if (atttypid_array != NULL) {
        pfree_ext(atttypid_array);
        atttypid_array = NULL;
    }

    if (atttypmod_array != NULL) {
        pfree_ext(atttypmod_array);
        atttypmod_array = NULL;
    }

    // Is there already a pg_statistic tuple for this attribute?
    //
    Relation sd = heap_open(StatisticExtRelationId, RowExclusiveLock);
    MemoryContext current_context = MemoryContextSwitchTo(oldcontext);
    ResourceOwner asOwner, oldOwner1;
    /*
     * Create a resource owner to keep track of resources
     * in order to release resources when catch the exception.
     */
    asOwner = ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner, "update_stats",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
    oldOwner1 = t_thrd.utils_cxt.CurrentResourceOwner;
    t_thrd.utils_cxt.CurrentResourceOwner = asOwner;

    PG_TRY();
    {
        oldtup = SearchSysCache4(STATRELKINDKEYINH,
            ObjectIdGetDatum(slot->tts_values[Anum_pg_statistic_ext_starelid - 1]),
            CharGetDatum(slot->tts_values[Anum_pg_statistic_ext_starelkind - 1]),
            BoolGetDatum(slot->tts_values[Anum_pg_statistic_ext_stainherit - 1]),
            slot->tts_values[Anum_pg_statistic_ext_stakey - 1]);

        if (HeapTupleIsValid(oldtup)) {
            // Yes, replace it
            //
            stup = heap_modify_tuple(oldtup, RelationGetDescr(sd), slot->tts_values, slot->tts_isnull, replaces);
            ReleaseSysCache(oldtup);
            (void)simple_heap_update(sd, &stup->t_self, stup);
        } else {
            // No, insert new tuple
            //
            stup = heap_form_tuple(RelationGetDescr(sd), slot->tts_values, slot->tts_isnull);
            simple_heap_insert(sd, stup);
        }

        // update indexes too
        //
        CatalogUpdateIndexes(sd, stup);
    }
    PG_CATCH();
    {
        analyze_concurrency_process(slot->tts_values[Anum_pg_statistic_ext_starelid - 1],
            ES_MULTI_COLUMN_STATS_ATTNUM,
            current_context,
            PG_FUNCNAME_MACRO);
    }
    PG_END_TRY();

    /* Release everything */
    ResourceOwnerRelease(asOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
    ResourceOwnerRelease(asOwner, RESOURCE_RELEASE_LOCKS, false, false);
    ResourceOwnerRelease(asOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, false);
    t_thrd.utils_cxt.CurrentResourceOwner = oldOwner1;
    ResourceOwnerDelete(asOwner);
    heap_close(sd, RowExclusiveLock);
}

/* Handle partition pages&tuples fetched from datanode. */
static void ReceivePartitionPageAndTuple(Oid relid, TupleTableSlot* slot)
{
    Relation rel;
    Relation fakerel;
    Partition partrel;
    Name partname;
    char parttype;
    Oid partitionid = InvalidOid;
    LOCKMODE part_lock;
    RelPageType relpages;
    double reltuples;
    BlockNumber relallvisible;

    partname = DatumGetName(slot->tts_values[0]);
    parttype = DatumGetChar(slot->tts_values[1]);
    relpages = (RelPageType)DatumGetFloat8(slot->tts_values[2]);
    reltuples = (double)DatumGetFloat8(slot->tts_values[3]);
    relallvisible = (BlockNumber)DatumGetInt32(slot->tts_values[4]);

    if (parttype == PART_OBJ_TYPE_TABLE_PARTITION) {
        part_lock = ShareUpdateExclusiveLock;
    } else if (parttype == PART_OBJ_TYPE_INDEX_PARTITION) {
        part_lock = RowExclusiveLock;
    } else {
        /* should not happen */
        Assert(0);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unrecognize LOCKMODE type.")));
        part_lock = 0; /* make complier slience */
    }

    rel = relation_open(relid, ShareUpdateExclusiveLock);
    partitionid = PartitionNameGetPartitionOid(
        relid, (const char*)partname->data, parttype, part_lock, true, false, NULL, NULL, NoLock);

    if (!OidIsValid(partitionid)) {
        relation_close(rel, NoLock);
        return;
    }
    partrel = partitionOpen(rel, partitionid, NoLock);

    vac_update_partstats(partrel, (BlockNumber)relpages, reltuples, relallvisible,
                         BootstrapTransactionId, RelationIsColStore(rel) ? InvalidMultiXactId : FirstMultiXactId);

    /*
     * we does not fetch dead tuples info from remote DN/CN, just set  deadtuples to 0. it does
     * not matter because we should fetch all deadtuples info from all datanodes to calculate a
     * user-defined table's deadtuples info
     *
     * now we just do analyze on table, and there is no stats info on partition, we deal with partition
     * just because we will support to analyze a partition oneday
     */
    fakerel = partitionGetRelation(rel, partrel);
    pgstat_report_analyze(fakerel, (PgStat_Counter)reltuples, (PgStat_Counter)0);
    releaseDummyRelation(&fakerel);

    partitionClose(rel, partrel, NoLock);
    relation_close(rel, NoLock);
}

char* repairObjectName(const char* relname)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

//@Temp Table. when use pg_temp.table_name, we should change the schema name
// to actual temp schema name when generate SQL use the name.
char* repairTempNamespaceName(char* name)
{
    Assert(name != NULL);
    Assert(strcasecmp(name, "pg_temp") == 0);

    return pstrdup(get_namespace_name(u_sess->catalog_cxt.myTempNamespace));
}

int FetchStatistics4WLM(const char* sql, void* info, Size size, strategy_func func)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

PGXCNodeAllHandles* connect_compute_pool(int srvtype)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * @Description: connect to the compute pool for OBS foreign table.
 *                       conninfo is from the cp_client.conf which is in the
 *                       data directory of the DWS CN.
 *
 * @return: conn handle to the compute pool.
 */
static PGXCNodeAllHandles* connect_compute_pool_for_OBS()
{
    int cnum = 0;
    ComputePoolConfig** confs = get_cp_conninfo(&cnum);

    return make_cp_conn(confs, cnum, T_OBS_SERVER);
}

/*
 * @Description: connect to the compute pool for HDFS foreign table.
 *                       conninfo is from dummy server.
 *
 * @return: conn handle to the compute pool.
 */
static PGXCNodeAllHandles* connect_compute_pool_for_HDFS()
{
    errno_t rt;
    int ret;

    char address[NAMEDATALEN] = {0};

    struct addrinfo* gai_result = NULL;

    /* get all connection info from the options of "dummy server" */
    DummyServerOptions* options = getDummyServerOption();

    Assert(options);

    rt = memcpy_s(address, NAMEDATALEN, options->address, strlen(options->address));
    securec_check(rt, "\0", "\0");

    /* parse "ip:port" into the binary format of IP and Port */
    char* port = strchr(address, ':');
    if (port == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmodule(MOD_ACCELERATE),
                errmsg("invalid address for the compute pool: %s", address)));

    *port = '\0';
    port++;

    ret = getaddrinfo(address, NULL, NULL, &gai_result);
    if (ret != 0)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmodule(MOD_ACCELERATE),
                errmsg("could not translate host name \"%s\" to address: %s", address, gai_strerror(ret))));

    struct sockaddr_in* h = (struct sockaddr_in*)gai_result->ai_addr;
    char ip[16] = {0};
    rt = strcpy_s(ip, sizeof(ip), inet_ntoa(h->sin_addr));
    securec_check(rt, "\0", "\0");

    freeaddrinfo(gai_result);

    elog(DEBUG1, "compute pool ip: %s, port: %s", ip, port);

    ComputePoolConfig config;
    config.cpip = ip;
    config.cpport = port;
    config.username = options->userName;
    config.password = options->passWord;

    ComputePoolConfig** configs = (ComputePoolConfig**)palloc0(sizeof(ComputePoolConfig*));
    configs[0] = &config;

    return make_cp_conn(configs, 1, T_HDFS_SERVER, options->dbname);
}

/*
 * @Description: connect to the compute pool.
 *
 * @param[IN] : config, necessary conn info such as ip, port, username and password.
 *
 * @return: conn handle to the compute pool.
 */
static PGXCNodeAllHandles* make_cp_conn(ComputePoolConfig** configs, int cnum, int srvtype, const char* dbname)
{
    PGXCNodeAllHandles* handles = NULL;

    for (int i = 0; i < cnum - 1; i++) {
        MemoryContext current_ctx = CurrentMemoryContext;

        PG_TRY();
        {
            handles = try_make_cp_conn(configs[i]->cpip, configs[i], srvtype, dbname);

            PG_TRY_RETURN(handles);
        }
        PG_CATCH();
        {
            /*
             * the compute pool is unavailable, so reset memory contex and clear
             * error stack.
             */
            MemoryContextSwitchTo(current_ctx);

            /* Save error info */
            ErrorData* edata = CopyErrorData();

            ereport(LOG,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                    errmodule(MOD_ACCELERATE),
                    errmsg("Fail to connect to the compute pool: %s, cause: %s", configs[i]->cpip, edata->message)));

            FlushErrorState();

            FreeErrorData(edata);
        }
        PG_END_TRY();
    }

    handles = try_make_cp_conn(configs[cnum - 1]->cpip, configs[cnum - 1], srvtype, dbname);

    return handles;
}

/*
 * @Description: do real job to connect to the compute pool.
 *
 * @param[IN] : config, necessary conn info such as ip, port, username and password.
 *
 * @return: conn handle to the compute pool.
 */
static PGXCNodeAllHandles* try_make_cp_conn(
    const char* cpip, ComputePoolConfig* config, int srvtype, const char* dbname)
{
#define PASSWORD_LEN 128

    /* make connection string */
    PGXCNodeHandle** handles = NULL;
    PGXCNodeAllHandles* pgxc_handles = NULL;

    if (dbname == NULL) {
        dbname = "postgres";
    }

    char* pgoptions = session_options();
    char* tmp_str =
        PGXCNodeConnStr(cpip, atoi(config->cpport), dbname, config->username, pgoptions, "application", PROTO_TCP, "");

    StringInfo conn_str = makeStringInfo();

    if (srvtype == T_OBS_SERVER) {
        char password[PASSWORD_LEN] = {0};
        errno_t rc = 0;
        decryptOBS(config->password, password, PASSWORD_LEN);
        appendStringInfo(conn_str, "%s, password='%s'", tmp_str, password);
        rc = memset_s(password, PASSWORD_LEN, 0, PASSWORD_LEN);
        securec_check(rc, "\0", "\0");
    } else
        appendStringInfo(conn_str, "%s, password='%s'", tmp_str, config->password);

    /* prepare result */
    pgxc_handles = (PGXCNodeAllHandles*)palloc0(sizeof(PGXCNodeAllHandles));
    pgxc_handles->dn_conn_count = 1;

    handles = (PGXCNodeHandle**)palloc0(sizeof(PGXCNodeHandle*));

    pgxc_handles->datanode_handles = handles;

    /* connect to the compute pool */
    connect_server(conn_str->data, &handles[0], cpip, atoi(config->cpport), tmp_str);

    /* clear password */
    int passWordLength = strlen(conn_str->data);
    errno_t rc = memset_s(conn_str->data, passWordLength, 0, passWordLength);
    securec_check(rc, "\0", "\0");

    return pgxc_handles;
}

void PgFdwSendSnapshot(StringInfo buf, Snapshot snapshot)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

void PgFdwSendSnapshot(StringInfo buf, Snapshot snapshot, Size snap_size)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

void PgFdwRemoteReply(StringInfo msg)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

/* PG_VERSION_STR */

/**
 * @Description: CN will commit on GTM, first notify DN to set csn to commit_in_progress
 * @return -  no return
 */
void NotifyDNSetCSN2CommitInProgress()
{
    PGXCNodeHandle** connections = u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles;

    /* Two Phase trx don't need to notify */
    if (u_sess->pgxc_cxt.remoteXactState->numWriteRemoteNodes != 1)
        return;

    /*
     * Send queryid to all the participants
     */
    if (pgxc_node_send_queryid(connections[0], u_sess->debug_query_id))
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("Failed to send queryid to %s before COMMIT command(1PC)", connections[0]->remoteNodeName)));
    /*
     * Now send the COMMIT command to all the participants
     */
    if (pgxc_node_notify_commit(connections[0])) {
        u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[0] = RXACT_NODE_COMMIT_FAILED;
        u_sess->pgxc_cxt.remoteXactState->status = RXACT_COMMIT_FAILED;

        /*
         * If the error occurred, can't abort because local committed firstly. Just note error message
         */
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("failed to notify node %u to commit", connections[0]->nodeoid)));
    }

    /* wait for response */
    {
        RemoteQueryState* combiner = CreateResponseCombiner(1, COMBINE_TYPE_NONE);
        /* Receive responses */
        int result = pgxc_node_receive_responses(1, connections, NULL, combiner, false);
        if (result || !validate_combiner(combiner)) {
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                    errmsg("failed to receice response from node %u after notify commit", connections[0]->nodeoid)));
        } else {
            CloseCombiner(combiner);
            combiner = NULL;
        }
    }
}

/**
 * @Description: send the commit csn to other pgxc node.
 * @in commit_csn -  the csn to be sent
 * @return -  no return
 */
void SendPGXCNodeCommitCsn(uint64 commit_csn)
{
#define ERRMSG_BUFF_SIZE 256
    int rc = 0;
    char errMsg[ERRMSG_BUFF_SIZE];
    int write_conn_count = u_sess->pgxc_cxt.remoteXactState->numWriteRemoteNodes;
    PGXCNodeHandle** connections = u_sess->pgxc_cxt.remoteXactState->remoteNodeHandles;

    /*
     * We must handle reader and writer connections both since the transaction
     * must be closed even on a read-only node
     */
    if (IS_PGXC_DATANODE || write_conn_count == 0)
        return;

    /*
     * Now send the COMMIT command to all the participants
     */
    for (int i = 0; i < write_conn_count; i++) {
        if (pgxc_node_send_commit_csn(connections[i], commit_csn)) {
            u_sess->pgxc_cxt.remoteXactState->remoteNodeStatus[i] = RXACT_NODE_COMMIT_FAILED;
            u_sess->pgxc_cxt.remoteXactState->status = RXACT_COMMIT_FAILED;

            /*
             * If the error occurred, can't abort because local committed firstly. Just note error message
             */
            if (i == 0) {
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("failed to send commit csn to node %u", connections[i]->nodeoid)));
            } else {
                rc = sprintf_s(errMsg,
                    ERRMSG_BUFF_SIZE,
                    "failed to send commit csn "
                    "command to node %s",
                    connections[i]->remoteNodeName);
                securec_check_ss(rc, "\0", "\0");
                add_error_message(connections[i], "%s", errMsg);
            }
        }
    }
}

/**
 * @Description: get List of PGXCNodeHandle to track writers involved in the
 * current transaction.
 * @return - List *u_sess->pgxc_cxt.XactWriteNodes
 */
List* GetWriterHandles()
{
    return u_sess->pgxc_cxt.XactWriteNodes;
}

/**
 * @Description: get List of PGXCNodeHandle to track readers involved in the
 * current transaction.
 * @return - List *u_sess->pgxc_cxt.XactReadNodes
 */
List* GetReaderHandles()
{
    return u_sess->pgxc_cxt.XactReadNodes;
}

/**
 * @Description: checking the host of connections are the same as the node definitions of the current plan.
 * If not, then switchover happened, the address and port information of current plan needs to be updated.
 * @planstmt - current plan statment
 * @connections - connections info from pooler, which are current primary node info
 * @regular_conn_count - number of nodes in plan
 */
void pgxc_check_and_update_nodedef(PlannedStmt* planstmt, PGXCNodeHandle** connections, int regular_conn_count)
{
    int i, node_id, rc;
    NameData temp_nodehost;
    int temp_nodeport;
    int temp_nodectlport;
    int temp_nodesctpport;
    char node_type;

    if (unlikely(planstmt == NULL || connections == NULL)) {
        return;
    }

    for (i = 0; i < regular_conn_count; i++) {
        /*
         * For primary/standby/dummmy mode. nodedef of primary and standby are in same row in pgxc_node.
         * just switch the upper part and the lower part.
         */
        if (IS_DN_DUMMY_STANDYS_MODE()) {
            node_id = GetNodeIdFromNodesDef(planstmt->nodesDefinition, connections[i]->nodeoid);
            if (unlikely(node_id < 0)) {
                ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Failed to get valid node id from node definitions")));
            }

            if (unlikely(strcmp(NameStr(connections[i]->connInfo.host),
                             NameStr(planstmt->nodesDefinition[node_id].nodehost)) != 0)) {
                temp_nodehost = planstmt->nodesDefinition[node_id].nodehost;
                temp_nodeport = planstmt->nodesDefinition[node_id].nodeport;

                planstmt->nodesDefinition[node_id].nodehost = planstmt->nodesDefinition[node_id].nodehost1;
                planstmt->nodesDefinition[node_id].nodeport = planstmt->nodesDefinition[node_id].nodeport1;
                planstmt->nodesDefinition[node_id].nodehost1 = temp_nodehost;
                planstmt->nodesDefinition[node_id].nodeport1 = temp_nodeport;

                /* sctp mode information of standby datanode */
                temp_nodectlport = planstmt->nodesDefinition[node_id].nodectlport;
                temp_nodesctpport = planstmt->nodesDefinition[node_id].nodesctpport;

                planstmt->nodesDefinition[node_id].nodectlport = planstmt->nodesDefinition[node_id].nodectlport1;
                planstmt->nodesDefinition[node_id].nodesctpport = planstmt->nodesDefinition[node_id].nodesctpport1;
                planstmt->nodesDefinition[node_id].nodectlport1 = temp_nodectlport;
                planstmt->nodesDefinition[node_id].nodesctpport1 = temp_nodesctpport;
            }
        }
        /*
         *for multi-standby mode, nodedef of primary and standby are different rows,
         *get primary node oid from pgxc_node and update the nodedef to current primary node.
         */
        else {
            if (get_pgxc_nodetype(connections[i]->nodeoid) == PGXC_NODE_COORDINATOR) {
                node_type = PGXC_NODE_COORDINATOR;
            } else {
                node_type = PGXC_NODE_DATANODE;
            }

            node_id = PGXCNodeGetNodeId(connections[i]->nodeoid, node_type);
            if (unlikely(strcmp(NameStr(connections[i]->connInfo.host),
                             NameStr(planstmt->nodesDefinition[node_id].nodehost)) != 0)) {
                Oid current_primary_oid = PgxcNodeGetPrimaryDNFromMatric(planstmt->nodesDefinition[node_id].nodeoid);
                NodeDefinition* res = PgxcNodeGetDefinition(current_primary_oid);

                elog(WARNING,
                    "nodesDefinition of planstmt is wrong, [node:%s,oid:%u] planstmt host:%s, expected host:%s, has "
                    "fixed to:%s.",
                    connections[i]->remoteNodeName,
                    connections[i]->nodeoid,
                    NameStr(planstmt->nodesDefinition[node_id].nodehost),
                    NameStr(connections[i]->connInfo.host),
                    (res == NULL) ? "" : NameStr(res->nodehost));
                if (res != NULL) {
                    rc = memcpy_s(
                        &planstmt->nodesDefinition[node_id], sizeof(NodeDefinition), res, sizeof(NodeDefinition));
                    securec_check(rc, "\0", "\0");
                    pfree(res);
                }
            }
        }
    }
}

static void parse_nodes_name(char* node_list, char** target_nodes, int* node_num)
{
    char* p = node_list;
    int num = *node_num;
    int node_list_len = strlen(node_list);
    while (((p - node_list) <= node_list_len) && *p != '\0') {
        char* node_name = p;
        int node_name_lenth = 0;
        while (*p != '\0' && (*p != MOD_DELIMITER)) {
            p++;
            node_name_lenth++;
        }
        if (num >= (u_sess->pgxc_cxt.NumCoords + u_sess->pgxc_cxt.NumDataNodes))
            ereport(ERROR,
                (errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
                    errmsg("invalid node_list, coor_num:%d, datanode_num:%d",
                        u_sess->pgxc_cxt.NumCoords,
                        u_sess->pgxc_cxt.NumDataNodes)));
        /* skip ',' */
        target_nodes[num] = node_name;
        num++;
        if (*p == MOD_DELIMITER) {
            /* mark the end */
            *p = '\0';
            p++;
        }
    }
    *node_num = num;
}

static int get_target_conn(
    int node_num, char** target_nodes, PGXCNodeHandle** connections, PGXCNodeAllHandles* pgxc_connections)
{
    PGXCNodeHandle** dn_connections = NULL;
    PGXCNodeHandle** cn_connections = NULL;
    int dn_conn_count = 0;
    int cn_conn_count = 0;
    int conn_idx = 0;
    int i = 0;
    int j = 0;
    bool find_node = false;

    pgxc_connections = get_exec_connections(NULL, NULL, EXEC_ON_ALL_NODES);
    dn_connections = pgxc_connections->datanode_handles;
    dn_conn_count = pgxc_connections->dn_conn_count;
    cn_connections = pgxc_connections->coord_handles;
    cn_conn_count = pgxc_connections->co_conn_count;

    for (i = 0; i < node_num; i++) {
        for (j = 0; j < dn_conn_count; j++) {
            if (strcmp(dn_connections[j]->remoteNodeName, target_nodes[i]) == 0) {
                connections[conn_idx] = dn_connections[j];
                conn_idx++;
                find_node = true;
            }
        }
        for (j = 0; j < cn_conn_count; j++) {
            Oid node_oid = get_pgxc_nodeoid(target_nodes[i]);
            bool nodeis_active = true;
            nodeis_active = is_pgxc_nodeactive(node_oid);
            if ((strcmp(cn_connections[j]->remoteNodeName, target_nodes[i]) == 0) && OidIsValid(node_oid) &&
                nodeis_active) {
                connections[conn_idx] = cn_connections[j];
                conn_idx++;
                find_node = true;
            }
        }
        if (!find_node) {
            ereport(WARNING, (errmsg("target node %s is not in cluster nodes.", target_nodes[i])));
        }
    }
    return conn_idx;
}

static void send_remote_node_query(int conn_num, PGXCNodeHandle** connections, const char* sql)
{
    int new_conn_count = 0;
    int i = 0;
    int result = 0;
    RemoteQueryState* combiner = NULL;
    StringInfoData send_fail_node;
    StringInfoData exec_fail_node;
    bool clean_success = true;
    PGXCNodeHandle** new_connections = (PGXCNodeHandle**)palloc0(sizeof(PGXCNodeHandle*) * conn_num);
    for (i = 0; i < conn_num; i++) {
        initStringInfo(&send_fail_node);
        /* Clean the previous errors, if any */
        connections[i]->error = NULL;
        if (pgxc_node_send_query(connections[i], sql, false, false, false,
            g_instance.attr.attr_storage.enable_gtm_free)) {
            /* do something record send failed node_name */
            clean_success = false;
            appendStringInfo(&send_fail_node, "%s ", connections[i]->remoteNodeName);
        } else {
            new_connections[new_conn_count++] = connections[i];
        }
    }

    /* get response */
    if (new_conn_count) {
        initStringInfo(&exec_fail_node);
        combiner = CreateResponseCombiner(new_conn_count, COMBINE_TYPE_NONE);
        /* Receive responses */
        result = pgxc_node_receive_responses(new_conn_count, new_connections, NULL, combiner, false);
        if (result || !validate_combiner(combiner)) {
            result = EOF;
        } else {
            CloseCombiner(combiner);
            combiner = NULL;
        }

        for (i = 0; i < new_conn_count; i++) {
            if (new_connections[i]->error) {
                clean_success = false;
                appendStringInfo(&exec_fail_node, "%s ", new_connections[i]->remoteNodeName);
            }
        }
    }
    pfree(new_connections);

    /* ereport detail message */
    if (clean_success == false)
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("Failed to send COMMIT/ROLLBACK on nodes: %s.Failed to COMMIT/ROLLBACK the transaction on "
                       "nodes: %s.",
                    send_fail_node.data,
                    exec_fail_node.data)));

    if (result) {
        if (combiner != NULL)
            pgxc_node_report_error(combiner, TwoPhaseCommit ? WARNING : ERROR);
        else
            ereport(TwoPhaseCommit ? WARNING : ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                    errmsg(
                        "Connection error with Datanode, so failed to COMMIT the transaction on one or more nodes")));
    }
}

Datum global_clean_prepared_xacts(PG_FUNCTION_ARGS)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported proc in single node mode.")));

    PG_RETURN_NULL();
}

bool check_receive_buffer(RemoteQueryState* combiner, int tapenum, bool* has_checked, int* has_err_idx)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

