/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * lightProxy.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/lightProxy.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/transam.h"
#include "access/xact.h"
#include "utils/dynahash.h"
#include "utils/hotkey.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "executor/executor.h"
#include "executor/lightProxy.h"
#include "mb/pg_wchar.h"
#include "optimizer/pgxcship.h"
#include "pgxc/poolmgr.h"
#include "pgxc/pgxc.h"
#include "utils/snapmgr.h"
#include "pgstat.h"
#include "pgaudit.h"
#include "pgxc/route.h"
#include "libpq/pqformat.h"
#include "gs_policy/policy_common.h"
#include "instruments/instr_unique_sql.h"
#include "instruments/instr_slow_query.h"
#include "tcop/tcopprot.h"
#include "optimizer/streamplan.h"
#include "gs_ledger/blockchain.h"
#include "parser/parse_hint.h"
#include "replication/walreceiver.h"

const int MAX_COMMAND = 51;
typedef struct commandType {
    CmdType type;
    const char* commandTag;
} CommandType;

static CommandType g_command_type_array[MAX_COMMAND] = {{CMD_DML, "SELECT"},
    {CMD_DML, "UPDATE"},
    {CMD_DML, "INSERT"},
    {CMD_DML, "DELETE"},
    {CMD_DML, "MERGE"},
    {CMD_TCL, "BEGIN"},
    {CMD_TCL, "COMMIT"},
    {CMD_TCL, "ROLLBACK"},
    {CMD_TCL, "START"},
    {CMD_TCL, "SAVEPOINT"},
    {CMD_TCL, "RELEASE"},
    {CMD_TCL, "PREPARE TRANSACTION"},
    {CMD_DDL, "PREPARE"},
    {CMD_DML, "CHECKPOINT"},
    {CMD_DML, "TRUNCATE"},
    {CMD_DML, "EXPLAIN"},
    {CMD_DML, "SHOW"},
    {CMD_DML, "LOCK"},
    {CMD_DML, "COPY"},
    {CMD_DML, "CLUSTER"},
    {CMD_DML, "ANONYMOUS"},
    {CMD_DML, "VACUUM"},
    {CMD_DML, "DELTA"},
    {CMD_DML, "ANALYZE"},
    {CMD_DML, "EXECUTE"},
    {CMD_DDL, "MOVE"},
    {CMD_DDL, "FETCH"},
    {CMD_DCL, "CREATE ROLE"},
    {CMD_DCL, "CREATE USER"},
    {CMD_DDL, "CREATE"},
    {CMD_DCL, "DROP ROLE"},
    {CMD_DCL, "DROP USER"},
    {CMD_DDL, "DROP"},
    {CMD_DCL, "ALTER ROLE"},
    {CMD_DCL, "ALTER USER"},
    {CMD_DCL, "ALTER DEFAULT PRIVILEGES"},
    {CMD_DDL, "ALTER"},
    {CMD_DDL, "CLOSE"},
    {CMD_DDL, "DEALLOCATE"},
    {CMD_DDL, "DECLARE"},
    {CMD_DDL, "REINDEX"},
    {CMD_DDL, "COMMENT"},
    {CMD_DDL, "BARRIER"},
    {CMD_DCL, "GRANT"},
    {CMD_DCL, "REVOKE"},
    {CMD_DCL, "NOTIFY"},
    {CMD_DCL, "LISTEN"},
    {CMD_DCL, "LOAD"},
    {CMD_DCL, "DISCARD"},
    {CMD_DCL, "REASSIGN"},
    {CMD_DCL, "CONSTRAINTS"}};

extern void pgxc_node_init(PGXCNodeHandle* handle, int sock);
extern void pgxc_handle_unsupported_stmts(Query* query);
extern Oid exprType(const Node* expr);

extern int light_node_send_begin(PGXCNodeHandle* handle, bool check_gtm_mode);
extern int light_handle_response(PGXCNodeHandle* conn, lightProxyMsgCtl* msgctl, lightProxy* lp);
extern void light_node_report_error(lightProxyErrData* combiner);
extern bool light_node_receive(PGXCNodeHandle* handle);
extern bool light_node_receive_from_logic_conn(PGXCNodeHandle* handle);
extern void light_pgaudit_ExecutorEnd(Query* query);

void report_qps_type(CmdType commandType);
CmdType set_cmd_type(const char* commandTag);

static void report_iud_time_for_lightproxy(const Query* query)
{
    if (u_sess->attr.attr_sql.enable_save_datachanged_timestamp == false)
        return;

    if (!IS_PGXC_COORDINATOR && !IS_SINGLE_NODE)
        return;

    if (query->commandType != CMD_INSERT && query->commandType != CMD_DELETE && query->commandType != CMD_UPDATE &&
        query->commandType != CMD_MERGE) {
        return;
    }

    if (query->rtable == NULL)
        return;

    if (query->resultRelation <= list_length(query->rtable)) {
        RangeTblEntry* rte = (RangeTblEntry*)list_nth(query->rtable, query->resultRelation - 1);
        if (RTE_RELATION != rte->rtekind)
            return;

        MemoryContext current_ctx = CurrentMemoryContext;
        Relation rel = NULL;

        PG_TRY();
        {
            rel = heap_open(rte->relid, AccessShareLock);
            if (rel->rd_rel->relkind == RELKIND_RELATION) {
                if (rel->rd_rel->relpersistence == RELPERSISTENCE_PERMANENT ||
                    rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED) {
                    pgstat_report_data_changed(rte->relid, STATFLG_RELATION, rel->rd_rel->relisshared);
                }
            }

            heap_close(rel, AccessShareLock);
        }
        PG_CATCH();
        {
            (void)MemoryContextSwitchTo(current_ctx);

            ErrorData* edata = CopyErrorData();

            ereport(DEBUG1, (errmsg("Failed to send data changed time, cause: %s", edata->message)));

            FlushErrorState();

            FreeErrorData(edata);

            if (rel != NULL)
                heap_close(rel, AccessShareLock);
        }
        PG_END_TRY();
    }
}

static void report_unsupport_light(LightUnSupportType type)
{
    if (type == CTRL_DISABLE) {
        return;
    }

    char* unsupport_msg[MAX_UNSUPPORT_TYPE] = {"guc ctrl disable",
        "not support client encoding different from database encoding",
        "not support cursor",
        "not support execute direct on",
        "not support others cmd type except I/D/U/S",
        "not support table entry relkind is foreign",
        "not support query has a statement trigger",
        "not support user-defined type",
        "not support query with node_name hint"};

    ereport(DEBUG2, (errmodule(MOD_LIGHTPROXY),
                    errmsg("[LIGHT PROXY]  check failed with type: %s.", unsupport_msg[type])));

    return;
}

static bool isSupportLightQuery(Query* query)
{
    ListCell* item = NULL;

    if (!u_sess->attr.attr_sql.enable_fast_query_shipping ||
        (!u_sess->attr.attr_sql.enable_light_proxy && !GTM_LITE_MODE)) {
        report_unsupport_light(CTRL_DISABLE);

        return false;
    }

    if (pg_get_client_encoding() != GetDatabaseEncoding()) {
        report_unsupport_light(ENCODE_UNSUPPORT);

        return false;
    }

    /* not support cursor */
    if (query->utilityStmt && IsA(query->utilityStmt, DeclareCursorStmt)) {
        report_unsupport_light(CURSOR_UNSUPPORT);

        return false;
    }

    /* not support execute direct on */
    if (query->utilityStmt && IsA(query->utilityStmt, RemoteQuery)) {
        report_unsupport_light(REMOTE_UNSUPPORT);

        return false;
    }

    /* not support others */
    if (query->commandType != CMD_SELECT && query->commandType != CMD_UPDATE && query->commandType != CMD_INSERT &&
        query->commandType != CMD_DELETE && !(HAS_ROUTER && query->commandType == CMD_MERGE)) {
        report_unsupport_light(CMD_UNSUPPORT);

        return false;
    }

    /* do not support node_name hint due to agg function's different behavior */
    if (CheckNodeNameHint(query->hintState)) {
        report_unsupport_light(NODE_NAME_UNSUPPORT);
        return false;
    }

    foreach (item, query->rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(item);
#ifdef ENABLE_MOT
        if (rte->relkind == RELKIND_STREAM || (rte->relkind == RELKIND_FOREIGN_TABLE && !isMOTFromTblOid(rte->relid))) {
#else
        if (rte->relkind == RELKIND_STREAM || rte->relkind == RELKIND_FOREIGN_TABLE) {
#endif
            report_unsupport_light(FOREIGN_UNSUPPORT);

            return false;
        }

        /*
         * Essentially, lightProxy is a fast path for FQSed when length
         * of exec_nodes is 1, which is not supported when query has a
         * statement trigger.
         */
        if (pgxc_find_statement_trigger(rte->relid, query->commandType)) {
            report_unsupport_light(STATEMENT_UNSUPPORT);

            return false;
        }
    }

    /* check the target list for T message */
    foreach (item, query->targetList) {
        TargetEntry* tle = (TargetEntry*)lfirst(item);
        if (tle->resjunk)
            continue;

        /* not support user-defined type */
        if (exprType((Node*)tle->expr) >= FirstBootstrapObjectId) {
            report_unsupport_light(USERTYPE_UNSUPPORT);

            return false;
        }
    }

    return true;
}

lightProxy::lightProxy(Query *query)
    : m_cplan(NULL),
      m_nodeIdx(-1),
      m_context(NULL),
      m_stmtName(NULL),
      m_formats(NULL),
      m_entry(NULL),
      m_query(query),
      m_handle(NULL)
{
    m_cmdType = query ? query->commandType : CMD_UNKNOWN;
    queryType = CMD_DML;
    m_portalName = NULL;
    m_msgctl = (lightProxyMsgCtl*)palloc0(sizeof(lightProxyMsgCtl));
    m_msgctl->errData = (lightProxyErrData*)palloc0(sizeof(lightProxyErrData));
    m_msgctl->relhash = 0;
    m_msgctl->has_relhash = false;
    m_isRowTriggerShippable = query->isRowTriggerShippable;
    initStringInfo(&m_bindMessage);
    initStringInfo(&m_describeMessage);
#ifdef LPROXY_DEBUG
    m_msgctl->stmt_name = NULL;
    m_msgctl->query_string = query->sql_statement;
#endif
}

lightProxy::lightProxy(MemoryContext context, CachedPlanSource *psrc, const char *portalname, const char *stmtname)
    : m_cplan(psrc),
      m_nodeIdx(-1),
      m_context(context),
      m_stmtName(NULL),
      m_portalName(NULL),
      m_formats(NULL),
      m_entry(NULL),
      m_cmdType(CMD_UNKNOWN),
      m_query(NULL),
      m_handle(NULL)
{
    MemoryContext old_context = MemoryContextSwitchTo(context);

    if (psrc) {
        m_cmdType = set_cmd_type(psrc->commandTag);
        queryType = set_command_type_by_commandTag(psrc->commandTag);
    }
    initStringInfo(&m_bindMessage);
    initStringInfo(&m_describeMessage);

    m_msgctl = (lightProxyMsgCtl *)palloc0(sizeof(lightProxyMsgCtl));
    m_msgctl->errData = (lightProxyErrData *)palloc0(sizeof(lightProxyErrData));
    m_msgctl->relhash = 0;
    m_msgctl->has_relhash = false;

    if (psrc != NULL && list_length(psrc->query_list) == 1 && linitial(psrc->query_list) != NULL) {
        m_isRowTriggerShippable = ((Query*)linitial(psrc->query_list))->isRowTriggerShippable;
    } else {
        m_isRowTriggerShippable = false;
    }

    if (portalname != NULL && portalname[0] != '\0') {
        storeLightProxy(portalname);
    } else {
        m_portalName = NULL;
    }
    m_stmtName = (stmtname != NULL && stmtname[0] != '\0') ? pstrdup(stmtname) : NULL;

#ifdef LPROXY_DEBUG
    m_msgctl->stmt_name = pstrdup(m_stmtName);
    m_msgctl->query_string = pstrdup(m_cplan->query_string);
#endif
    MemoryContextSwitchTo(old_context);
}


lightProxy::~lightProxy()
{
    m_cplan = NULL;
    m_context = NULL;
    m_query = NULL;
    m_handle = NULL;
    m_msgctl = NULL;
    m_entry = NULL;
    pfree_ext(m_formats);
}

void lightProxy::getResultFormat(StringInfo message)
{
    pfree_ext(m_formats);
    if (m_cplan->resultDesc == NULL)
        return;
    /* Get the result format codes */
    int numRFormats = pq_getmsgint(message, 2);
    int i = 0;
    int natts = m_cplan->resultDesc->natts;
    int16* formats = NULL;
    m_formats = (int16*)palloc(natts * sizeof(int16));

    /* Get the result format codes */
    if (numRFormats > 0) {
        formats = (int16*)palloc(numRFormats * sizeof(int16));
        for (i = 0; i < numRFormats; i++)
            formats[i] = pq_getmsgint(message, 2);
    }
    pq_getmsgend(message);

    if (numRFormats > 1) {
        if (numRFormats != natts) {
            pfree_ext(formats);
            ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                    errmsg("bind message has %d result formats but query has %d columns", numRFormats, natts)));
        }
        int rc = memcpy_s(m_formats, natts * sizeof(int16), formats, natts * sizeof(int16));
        securec_check(rc, "\0", "\0");
    } else if (numRFormats > 0) {
        for (i = 0; i < natts; i++)
            m_formats[i] = formats[0];
    } else {
        for (i = 0; i < natts; i++)
            m_formats[i] = 0;
    }
    pfree_ext(formats);
}

void lightProxy::saveMsg(int msgType, StringInfo message)
{
    // to do , save memory consumption
    AutoContextSwitch contexts(m_context);

    if (msgType == BIND_MESSAGE) {
        /* clean previous message if exists */
        if (m_bindMessage.len > 0)
            resetStringInfo(&m_bindMessage);
        if (m_describeMessage.len > 0)
            resetStringInfo(&m_describeMessage);

        getResultFormat(message);
        appendBinaryStringInfo(&m_bindMessage, message->data, message->len);
    } else if (msgType == DESC_MESSAGE) {
        /* clean previous message if exists */
        if (m_describeMessage.len > 0)
            resetStringInfo(&m_describeMessage);
        appendBinaryStringInfo(&m_describeMessage, message->data, message->len);
    }
}

void lightProxy::connect()
{
    List* dn_allocate = NULL;
    errno_t ss_rc = 0;
    int dnNum = u_sess->pgxc_cxt.NumDataNodes;
    if (IS_CN_DISASTER_RECOVER_MODE) {
        dnNum = u_sess->pgxc_cxt.NumTotalDataNodes;
        if (!u_sess->pgxc_cxt.DisasterReadArrayInit) {
            disaster_read_array_init();
        }
        Assert(m_nodeIdx < u_sess->pgxc_cxt.NumDataNodes);
        if (u_sess->pgxc_cxt.disasterReadArray[m_nodeIdx] != -1) {
            m_nodeIdx = u_sess->pgxc_cxt.disasterReadArray[m_nodeIdx];
        }
    }

    m_handle = &u_sess->pgxc_cxt.dn_handles[m_nodeIdx];
    if (!IS_VALID_CONNECTION(m_handle)) {
        Assert(m_nodeIdx < dnNum);
        if (m_nodeIdx >= dnNum) {
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                errmsg("[LIGHT PROXY] m_nodeIdx error, m_nodeIdx:%d, numDataNodes:%d",
                    m_nodeIdx, dnNum)));
        }

        dn_allocate = lappend_int(dn_allocate, m_nodeIdx);
        PoolConnDef* pfds = PoolManagerGetConnections(dn_allocate, NULL);
        if (pfds == NULL)
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                    errmsg("[LIGHT PROXY] Failed to get pooled connections from %s[%u]",
                        m_handle->remoteNodeName,
                        m_handle->nodeoid)));

        int fdsock = pfds->fds[0];
        PoolConnInfo* conn_info = &pfds->connInfos[0];

        pgxc_node_init(m_handle, fdsock);
        ss_rc = memcpy_s(&m_handle->connInfo, sizeof(PoolConnInfo), conn_info, sizeof(PoolConnInfo));

        securec_check(ss_rc, "\0", "\0");
#ifdef ENABLE_MULTIPLE_NODES
        pgxc_node_send_global_session_id((PGXCNodeHandle*)m_handle);
#endif
        u_sess->pgxc_cxt.dn_handles[m_nodeIdx] = *m_handle;
        u_sess->pgxc_cxt.datanode_count++;

        if (pfds->gsock[0].type != GSOCK_INVALID) {
            m_handle->gsock = pfds->gsock[0];
            m_handle->is_logic_conn = true;
        }
        pgxc_node_free_def(pfds);
        pfds = NULL;
    } else if (m_handle->state == DN_CONNECTION_STATE_QUERY) {
        BufferConnection(m_handle);
    }
}

void lightProxy::sendParseIfNecessary()
{
    /* if no stmt_name, we need to send parse every time */
    if (m_stmtName == NULL || m_stmtName[0] == '\0') {
        if (pgxc_node_send_parse(
                m_handle, m_stmtName, m_cplan->query_string, m_cplan->num_params, m_cplan->param_types)) {
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                    errmsg("[LIGHT PROXY] Failed to send parse to %s[%u]",
                        m_handle->remoteNodeName,
                        m_handle->nodeoid)));
        }
        return;
    }

    if (unlikely(m_entry == NULL)) {
        /*
         * If we have reloaded pooler, we need to add it into datanode_queries again,
         * as we do in parse phrase previously.
         */
        m_entry = light_set_datanode_queries(m_stmtName);
        Assert(m_entry != NULL);
    }
    Assert(m_nodeIdx != -1);
    bool need_send_again = false;
    /* see if statement already active on the node */
    for (int i = 0; i < m_entry->current_nodes_number; i++) {
        if (m_entry->dns_node_indices[i] == m_nodeIdx) {
            if (ENABLE_CN_GPC || IN_GPC_GRAYRELEASE_CHANGE) {
                need_send_again = true;
            } else {
                return;
            }
        }
    }

    if (pgxc_node_send_parse(
            m_handle, m_stmtName, m_cplan->query_string, m_cplan->num_params, m_cplan->param_types)) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("[LIGHT PROXY] Failed to send parse to %s[%u]",
                    m_handle->remoteNodeName,
                    m_handle->nodeoid)));
    }
    if (need_send_again) {
        return;
    }
    /* After cluster expansion, must expand entry->dns_node_indices array too */
    if (unlikely(m_entry->current_nodes_number == m_entry->max_nodes_number)) {
        int* new_dns_node_indices = (int*)MemoryContextAllocZero(
            u_sess->pcache_cxt.datanode_queries->hcxt, m_entry->max_nodes_number * 2 * sizeof(int));
        errno_t error_no = EOK;
        error_no = memcpy_s(new_dns_node_indices,
            m_entry->max_nodes_number * 2 * sizeof(int),
            m_entry->dns_node_indices,
            m_entry->max_nodes_number * sizeof(int));
        securec_check(error_no, "\0", "\0");
        pfree_ext(m_entry->dns_node_indices);
        m_entry->dns_node_indices = new_dns_node_indices;
        m_entry->max_nodes_number = m_entry->max_nodes_number * 2;
        elog(LOG,
            "expand node ids array for active datanode statements "
            "after cluster expansion, now array size is %d",
            m_entry->max_nodes_number);
    }

    /* statement is not active on the specified node append item to the list */
    m_entry->dns_node_indices[m_entry->current_nodes_number++] = m_nodeIdx;
}

/*
 * @Description: Send BEGIN command to the DataNode. Also send the GXID for the transaction.
 *    See pgxc_node_begin for more details.
 */
void lightProxy::proxyNodeBegin(bool is_read_only)
{
    bool need_tran_block = false;
    GlobalTransactionId gxid = InvalidTransactionId;
    if (HAS_ROUTER) {
        // push down function to router dn, need transaction
        is_read_only = false;
    }
    if (IsTransactionBlock()) {
        need_tran_block = true;
    } else if (is_read_only) {
        need_tran_block = false;
    } else {
        need_tran_block = true;
    }

    if (is_read_only) {
        gxid = GetCurrentTransactionIdIfAny();
    }

    /*
     * If the node is already a participant in the transaction, skip it
     */
    if (list_member(u_sess->pgxc_cxt.XactReadNodes, m_handle) ||
        list_member(u_sess->pgxc_cxt.XactWriteNodes, m_handle)) {
        if (!is_read_only) {
            RegisterTransactionNodes(1, (void**)&m_handle, true);
        }
    } else {
        SetCurrentStmtTimestamp();

        TimestampTz gtmstart_timestamp = GetCurrentGTMStartTimestamp();
        TimestampTz stmtsys_timestamp = GetCurrentStmtsysTimestamp();
        if (GlobalTimestampIsValid(gtmstart_timestamp) &&
            GlobalTimestampIsValid(stmtsys_timestamp) &&
            pgxc_node_send_timestamp(m_handle, gtmstart_timestamp, stmtsys_timestamp)) {
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                    errmsg("[LIGHT PROXY] Failed to send timestamp to %s[%u]",
                        m_handle->remoteNodeName,
                        m_handle->nodeoid)));
        }

        if (need_tran_block) {
            if (light_node_send_begin(m_handle, g_instance.attr.attr_storage.enable_gtm_free)) {
                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("[LIGHT PROXY] Failed to send internal begin to %s[%u]",
                            m_handle->remoteNodeName,
                            m_handle->nodeoid)));
            }
            LPROXY_DEBUG(ereport(DEBUG2,(errmodule(MOD_LIGHTPROXY),
                errmsg("[LIGHT PROXY] Send internal begin to DataNode %u: query %s",
                m_handle->nodeoid,
                m_msgctl->query_string))));

            /* recieve message */
            m_msgctl->cnMsg = true;
            handleResponse();
            RegisterTransactionNodes(1, (void**)&m_handle, !is_read_only);
        }
    }

    CommandId cid = GetCurrentCommandId(!is_read_only);
    if (pgxc_node_send_cmd_id(m_handle, cid) < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("[LIGHT PROXY] Failed to send cid to %s[%u]",
                    m_handle->remoteNodeName,
                    m_handle->nodeoid)));
    }
    /* print the XactWriteNodes and XactReadNodes list info */
    PrintRegisteredTransactionNodes();

    Snapshot snapshot = GetActiveSnapshot();
    if (!GTM_FREE_MODE && snapshot != NULL &&
        pgxc_node_send_snapshot(m_handle, snapshot)) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("[LIGHT PROXY] Failed to send snapshot to %s[%u]",
                    m_handle->remoteNodeName,
                    m_handle->nodeoid)));
    }

    if (u_sess->attr.attr_resource.use_workload_manager && *u_sess->wlm_cxt->control_group &&
        pgxc_node_send_wlm_cgroup(m_handle)) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("[LIGHT PROXY] Failed to send cgroup to %s[%u]",
                    m_handle->remoteNodeName,
                    m_handle->nodeoid)));
    }

    /* Only generate one time when debug_query_id = 0 in CN */
    if (unlikely(u_sess->debug_query_id == 0)) {
        u_sess->debug_query_id = generate_unique_id64(&gt_queryId);
        pgstat_report_queryid(u_sess->debug_query_id);
    }
    if (pgxc_node_send_queryid(m_handle, u_sess->debug_query_id)) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("[LIGHT PROXY] Failed to send query id to %s[%u]",
                    m_handle->remoteNodeName,
                    m_handle->nodeoid)));
    }

    /* Instrumentation/Unique SQL: send unique sql id to DN node */
    if (is_unique_sql_enabled() && pgxc_node_send_unique_sql_id(m_handle)) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("[LIGHT PROXY] Failed to send unique sql id to %s[%u]",
                    m_handle->remoteNodeName,
                    m_handle->nodeoid)));
    }
}

void lightProxy::setCurrentProxy(lightProxy* proxy)
{
    /* set process_count = NULL for common PBE */
    if (proxy != NULL && proxy->m_msgctl != NULL && proxy->m_msgctl->process_count != NULL)
        proxy->m_msgctl->process_count = NULL;

    u_sess->exec_cxt.cur_light_proxy_obj = proxy;
}

ExecNodes* lightProxy::checkRouterQuery(Query* query)
{
    ExecNodes* exec_nodes = NULL;
    if (!isSupportLightQuery(query) || !HAS_ROUTER) {
        return NULL;
    }
    exec_nodes = makeNode(ExecNodes);
    exec_nodes->nodeList = lappend_int(exec_nodes->nodeList, u_sess->exec_cxt.CurrentRouter->GetRouterNodeId());
    return exec_nodes;
}

/*
 * Constraints specially defined for Light CN are checked here
 */
ExecNodes* lightProxy::checkLightQuery(Query* query)
{
    ExecNodes* exec_nodes = NULL;

    /* for UPSERT, use the insert part to check Light CN */
    if (query->upsertQuery != NULL) {
        /* only allow UPSERT transformed MERGE statement have an upsertQuery */
        if (unlikely(query->commandType != CMD_MERGE)) {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("INSERT ON DUPLICATE KEY UPDATE must have an transformed InsertStmt query."))));
        }
        query = query->upsertQuery;
    }

    if (!isSupportLightQuery(query)) {
        return NULL;
    }

    /* handle the un-supported statements, obvious errors etc. */
    pgxc_handle_unsupported_stmts(query);

    /* Do permissions checks */
    Assert(IS_PGXC_COORDINATOR && !IsConnFromCoord());
    (void)ExecCheckRTPerms(query->rtable, true);

    exec_nodes = pgxc_is_query_shippable(query, 0, true);

    return exec_nodes;
}

void lightProxy::tearDown(lightProxy *proxy)
{
    MemoryContext context = proxy->m_context;

    proxy->removeLpByStmtName(proxy->m_stmtName);

    removeLightProxy(proxy->m_portalName);

    MemoryContextDelete(context);

    lightProxy::setCurrentProxy(NULL);
}

void lightProxy::initStmtHtab()
{
    HASHCTL hash_ctl;
    errno_t rc = 0;
    int htab_size = 64;
    
    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");

    hash_ctl.keysize = NAMEDATALEN;
    hash_ctl.entrysize = sizeof(stmtLpObj);
    hash_ctl.hcxt = u_sess->cache_mem_cxt;
    u_sess->pcache_cxt.stmt_lightproxy_htab = hash_create("lightProxy Named Object for GPC",
                                                          htab_size,
                                                          &hash_ctl,
                                                          HASH_ELEM | HASH_CONTEXT);
}


void lightProxy::initlightProxyTable()
{
    HASHCTL hash_ctl;
    errno_t rc = 0;
	rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
	securec_check(rc, "\0", "\0");

	hash_ctl.keysize = NAMEDATALEN;
	hash_ctl.entrysize = sizeof(lightProxyNamedObj);
	hash_ctl.hcxt = u_sess->cache_mem_cxt;
	u_sess->pcache_cxt.lightproxy_objs = hash_create("lightProxy Named Object", 64, &hash_ctl, HASH_ELEM | HASH_CONTEXT);
}

void lightProxy::removeLpByStmtName(const char *stmtname)
{
    if (u_sess->pcache_cxt.stmt_lightproxy_htab && stmtname != NULL && stmtname[0] != '\0') {
        (void)hash_search(u_sess->pcache_cxt.stmt_lightproxy_htab, stmtname, HASH_REMOVE, NULL);
    }
}

void lightProxy::removeLightProxy(const char* portalname)
{
	if(u_sess->pcache_cxt.lightproxy_objs && portalname != NULL) {
		 hash_search(u_sess->pcache_cxt.lightproxy_objs, portalname, HASH_REMOVE, NULL);
	}
}

void lightProxy::storeLpByStmtName(const char *stmtname)
{
    stmtLpObj *entry = NULL;
    if (!u_sess->pcache_cxt.stmt_lightproxy_htab)
        initStmtHtab();

    entry = (stmtLpObj *)hash_search(u_sess->pcache_cxt.stmt_lightproxy_htab, stmtname, HASH_ENTER, NULL);
    entry->proxy = this;
}

void lightProxy::storeLightProxy(const char* portalname)
{
    lightProxyNamedObj* entry = NULL;
    if(!u_sess->pcache_cxt.lightproxy_objs)
        initlightProxyTable();

    entry = (lightProxyNamedObj*)hash_search(u_sess->pcache_cxt.lightproxy_objs, portalname, HASH_ENTER, NULL);
    entry->proxy = this;
    pfree_ext(m_portalName);
    MemoryContext old_context = MemoryContextSwitchTo(m_context);
    m_portalName = pstrdup(portalname);
    (void)MemoryContextSwitchTo(old_context);
}

lightProxy *lightProxy::locateLpByStmtName(const char *stmtname)
{
    stmtLpObj *entry = NULL;
    if (u_sess->pcache_cxt.stmt_lightproxy_htab && stmtname && stmtname[0] != '\0') {
        entry = (stmtLpObj *)hash_search(u_sess->pcache_cxt.stmt_lightproxy_htab, stmtname, HASH_FIND, NULL);
    }

    if (entry) {
        return entry->proxy;
    } else {
        return NULL;
    }
}

lightProxy* lightProxy::locateLightProxy(const char* portalname)
{
	lightProxyNamedObj* entry = NULL;
	if(u_sess->pcache_cxt.lightproxy_objs) {
		entry = (lightProxyNamedObj*)hash_search(u_sess->pcache_cxt.lightproxy_objs, portalname, HASH_FIND, NULL);
	}

	if(entry) {
		return entry->proxy;
	} else {
		return NULL;
	}
}

bool lightProxy::processMsg(int msgType, StringInfo msg)
{
    lightProxy* lp = u_sess->exec_cxt.cur_light_proxy_obj;

    if (msgType == EXEC_MESSAGE) {
        lp = lightProxy::tryLocateLightProxy(msg);
    }
    bool res = false;
    bool old_status = u_sess->exec_cxt.need_track_resource;
    if (lp != NULL) {
        switch (msgType) {
            case BIND_MESSAGE:
            case DESC_MESSAGE:
                lp->saveMsg(msgType, msg);
                break;

            case EXEC_MESSAGE:
                if (u_sess->attr.attr_resource.resource_track_cost == 0 &&
                    u_sess->attr.attr_resource.enable_resource_track &&
                    u_sess->attr.attr_resource.resource_track_level != RESOURCE_TRACK_NONE) {
                    u_sess->exec_cxt.need_track_resource = true;
                    WLMSetCollectInfoStatus(WLM_STATUS_RUNNING);
                }
                lp->runMsg(msg);
                u_sess->exec_cxt.need_track_resource = old_status;
                break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_CASE_NOT_FOUND), errmsg("invalid msgType %d for process message \n", msgType)));
        }
        res = true;;
    }

    /*
    * Emit duration logging if appropriate.
    */
    char msec_str[PRINTF_DST_MAX];
    switch (check_log_duration(msec_str, false)) {
        case 1:
            Assert(false);
            break;
        case 2: {
            ereport(LOG, (errmsg("duration: %s ms queryid %ld unique id %ld", msec_str,
                u_sess->debug_query_id, u_sess->slow_query_cxt.slow_query.unique_sql_id), errhidestmt(true)));
            break;
        }
        default:
            break;
    }
    return res;
}

void lightProxy::assemableMsg(char msgtype, StringInfo msgBuf, bool trigger_ship)
{
    int msg_len = 4 + msgBuf->len;
    errno_t ss_rc;

    /* If trigger is being shipped to DN. */
    if (trigger_ship) {
        ensure_out_buffer_capacity(msg_len + 2, m_handle);
        m_handle->outBuffer[m_handle->outEnd++] = 'a';
    } else {
        ensure_out_buffer_capacity(msg_len + 1, m_handle);
    }
    m_handle->outBuffer[m_handle->outEnd++] = msgtype;

    msg_len = htonl(msg_len);

    ss_rc = memcpy_s(m_handle->outBuffer + m_handle->outEnd, m_handle->outSize - m_handle->outEnd,
                    &msg_len, sizeof(uint32));
    securec_check(ss_rc, "\0", "\0");
    m_handle->outEnd += 4;
    ss_rc = memcpy_s(m_handle->outBuffer + m_handle->outEnd, m_handle->outSize - m_handle->outEnd,
                    msgBuf->data, (size_t)msgBuf->len);
    securec_check(ss_rc, "\0", "\0");
    m_handle->outEnd += msgBuf->len;
}

void lightProxy::handleResponse()
{
    int res = 0;

    /*
     * Reset lightProxyErrData. This only happens for PBE.
     * Memory of lightProxyErrData itself is in m_context, no need to free here.
     * Memory of char* inside is in t_thrd.mem_cxt.msg_mem_cxt, no need to free too.
     */
    if (m_msgctl->errData->hasError) {
        errno_t rc = 0;
        rc = memset_s(m_msgctl->errData, sizeof(lightProxyErrData), 0, sizeof(lightProxyErrData));
        securec_check_c(rc, "\0", "\0");
    }

    m_handle->state = DN_CONNECTION_STATE_QUERY;
    // process all messages.
    while (true) {
        if (m_handle->is_logic_conn)
            res = light_node_receive_from_logic_conn(m_handle);
        else
            res = light_node_receive(m_handle);

        if (res) {
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                    errmsg("[LIGHT PROXY] Failed to fetch from Datanode %s[%u]",
                        m_handle->remoteNodeName,
                        m_handle->nodeoid)));
        }

        res = light_handle_response(m_handle, m_msgctl, this);

        if (res == LPROXY_FINISH) {
            break;
        } else if (res == LPROXY_ERROR) {
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                    errmsg("[LIGHT PROXY] Unexpected response from %s[%u]",
                        m_handle->remoteNodeName,
                        m_handle->nodeoid)));
        }
    }

    /* report error if any */
    if (m_msgctl->errData->hasError) {
        setCurrentProxy(NULL);
        light_node_report_error(m_msgctl->errData);
    }
}

lightProxy* lightProxy::tryLocateLightProxy(StringInfo msg)
{
	lightProxy* lp = u_sess->exec_cxt.cur_light_proxy_obj;

	int oldCursor = msg->cursor;
	const char* portal_name = pq_getmsgstring(msg);
	if(portal_name[0] != '\0') {
	   lp = lightProxy::locateLightProxy(portal_name);
	   lightProxy::setCurrentProxy(lp);
	}
	msg->cursor = oldCursor;

	return lp;
}

void lightProxy::runSimpleQuery(StringInfo exec_message)
{
    connect();

    LPROXY_DEBUG(ereport(DEBUG2, (errmodule(MOD_LIGHTPROXY), errmsg(
        "[LIGHT PROXY] Got exec_simple_query slim to Datanode %u: query %s",
        m_handle->nodeoid,
        m_query->sql_statement))));

    bool is_read_only = (m_query->commandType == CMD_SELECT && !m_query->hasForUpdate);

    proxyNodeBegin(is_read_only);

    bool trigger_ship = false;
    if (u_sess->attr.attr_sql.enable_trigger_shipping && m_isRowTriggerShippable)
        trigger_ship = true;
    assemableMsg('Q', exec_message, trigger_ship);

    if (pgxc_node_flush(m_handle)) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("[LIGHT PROXY] Unexpected response from %s[%u]",
                    m_handle->remoteNodeName,
                    m_handle->nodeoid)));
    }

    m_msgctl->sendDMsg = true;
    m_msgctl->cnMsg = false;
    m_msgctl->hasResult = (m_query->commandType == CMD_SELECT || m_query->returningList != NIL);

    handleResponse();

    /* pgaudit */
    if ((u_sess->attr.attr_security.Audit_DML_SELECT != 0 || u_sess->attr.attr_security.Audit_DML != 0) &&
        u_sess->attr.attr_security.Audit_enabled && IsPostmasterEnvironment) {
        light_pgaudit_ExecutorEnd(m_query);
    }
    /* unified auditing policy */
    if (!(g_instance.status > NoShutdown) && light_unified_audit_executor_hook) {
        light_unified_audit_executor_hook(m_query);
    }
    /* global chain record */
    if (IS_PGXC_COORDINATOR && m_msgctl->has_relhash) {
        light_ledger_ExecutorEnd(m_query, m_msgctl->relhash);
    }
    /* doing sql count accordiong to cmdType */
    if (u_sess->attr.attr_common.pgstat_track_activities && u_sess->attr.attr_common.pgstat_track_sql_count &&
        !u_sess->attr.attr_sql.enable_cluster_resize) {
        report_qps_type(m_cmdType);
        report_qps_type(queryType);
    }

    /* update unique sql stat */
    if (is_unique_sql_enabled() && is_local_unique_sql()) {
        UpdateUniqueSQLStat(NULL, NULL, GetCurrentStatementLocalStartTimestamp());
    }
    pgstate_update_percentile_responsetime();
    // no more proxy
    setCurrentProxy(NULL);

    report_iud_time_for_lightproxy(m_query);
}

int lightProxy::runBatchMsg(StringInfo batch_message, bool sendDMsg, int batch_count)
{
    int process_count = 0;

    SetUniqueSQLIdFromCachedPlanSource(this->m_cplan);

    connect();

    LPROXY_DEBUG(ereport(DEBUG2,(errmodule(MOD_LIGHTPROXY),
        errmsg("[LIGHT PROXY] Got Batch slim to DataNode %u: name %s, query %s",
        m_handle->nodeoid,
        m_stmtName,
        m_cplan->query_string))));

    /* Must set snapshot before starting executor. */
    PushActiveSnapshot(GetTransactionSnapshot(GTM_LITE_MODE));

    proxyNodeBegin(m_cplan->is_read_only);

    /* check if we need to send parse or not */
    sendParseIfNecessary();

    assemableMsg('U', batch_message);

    // send sync message  and flush
    if (pgxc_node_send_sync(m_handle)) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("[LIGHT PROXY] Failed to send sync to %s[%u]",
                    m_handle->remoteNodeName,
                    m_handle->nodeoid)));
    }

    m_msgctl->sendDMsg = sendDMsg;
    m_msgctl->process_count = &process_count;
    m_msgctl->cnMsg = false;
    m_msgctl->hasResult = (m_cplan->resultDesc != NULL) ? true : false;
    handleResponse();

    PopActiveSnapshot();

    /*
     * We need a CommandCounterIncrement after every query, except
     * those that start or end a transaction block.
     */
    CommandCounterIncrement();

    /* pgaudit */
    if ((u_sess->attr.attr_security.Audit_DML_SELECT != 0 || u_sess->attr.attr_security.Audit_DML != 0) &&
        u_sess->attr.attr_security.Audit_enabled && IsPostmasterEnvironment) {
        for (int i = 0; i < batch_count; i++)
            light_pgaudit_ExecutorEnd((Query*)linitial(m_cplan->query_list));
    }
    /* unified auditing policy */
    if (!(g_instance.status > NoShutdown) && light_unified_audit_executor_hook) {
        light_unified_audit_executor_hook((Query*)linitial(m_cplan->query_list));
    }
    /* global chain record */
    if (IS_PGXC_COORDINATOR && m_msgctl->has_relhash) {
        light_ledger_ExecutorEnd((Query*)linitial(m_cplan->query_list), m_msgctl->relhash);
    }

    /*
     * track_sql_count is on, counting WaitEventSQL for per user
     */
    if (u_sess->attr.attr_common.pgstat_track_activities && u_sess->attr.attr_common.pgstat_track_sql_count &&
        !u_sess->attr.attr_sql.enable_cluster_resize) {
        for (int i = 0; i < batch_count; i++) {
            report_qps_type(m_cmdType);
            report_qps_type(queryType);
        }
    }

    // finish with this proxy
    setCurrentProxy(NULL);
    return process_count;
}

void lightProxy::runMsg(StringInfo exec_message)
{
    /*
     * If we are in aborted transaction state, the only portals we can
     * actually run are those containing COMMIT or ROLLBACK commands.
     */
    if (IsAbortedTransactionBlockState())
        ereport(ERROR,
            (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
                errmsg("current transaction is aborted, "
                    "commands ignored until end of transaction block, firstChar[%c]",
                    u_sess->proc_cxt.firstChar), 0));

    connect();

    LPROXY_DEBUG(ereport(DEBUG2,(errmodule(MOD_LIGHTPROXY),
        errmsg("[LIGHT PROXY] Got Execute slim to DataNode %u: name %s, query %s",
        m_handle->nodeoid,
        m_stmtName,
        m_cplan->query_string))));

    bool trigger_ship = false;
    if (u_sess->attr.attr_sql.enable_trigger_shipping && m_isRowTriggerShippable)
        trigger_ship = true;

    /*
     * Ensure we are in a transaction command (this should normally be the
     * case already due to prior BIND).
     */
    start_xact_command();

    /* Set after start transaction in case there is no CurrentResourceOwner */
    SetUniqueSQLIdFromCachedPlanSource(this->m_cplan);

    /* Must set snapshot before starting executor, unless it is a MOT tables transaction. */
#ifdef ENABLE_MOT
    if (!IsMOTEngineUsed()) {
#endif
        PushActiveSnapshot(GetTransactionSnapshot(GTM_LITE_MODE));
#ifdef ENABLE_MOT
    }
#endif

    proxyNodeBegin(m_cplan->is_read_only);
    /* check if we need to send parse or not */
    sendParseIfNecessary();

    if (m_bindMessage.len > 0) {
        assemableMsg('B', &m_bindMessage);
        resetStringInfo(&m_bindMessage);
    }

    if (m_describeMessage.len > 0) {
        assemableMsg('D', &m_describeMessage);
        resetStringInfo(&m_describeMessage);
        m_msgctl->sendDMsg = true;
    } else {
        m_msgctl->sendDMsg = false;
    }

    assemableMsg('E', exec_message, trigger_ship);

    /* send sync message and flush */
    if (pgxc_node_send_sync(m_handle)) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("[LIGHT PROXY] Failed to send sync to %s[%u]",
                    m_handle->remoteNodeName, m_handle->nodeoid)));
    }

    m_msgctl->cnMsg = false;
    m_msgctl->hasResult = (m_cplan->resultDesc != NULL) ? true : false;
    handleResponse();

#ifdef ENABLE_MOT
    if (!IsMOTEngineUsed()) {
#endif
        PopActiveSnapshot();
#ifdef ENABLE_MOT
    }
#endif

    /*
     * We need a CommandCounterIncrement after every query, except
     * those that start or end a transaction block.
     */
    CommandCounterIncrement();
    t_thrd.wlm_cxt.parctl_state.except = 0;

    if (ENABLE_WORKLOAD_CONTROL) {
        if (g_instance.wlm_cxt->dynamic_workload_inited) {
            dywlm_client_max_release(&t_thrd.wlm_cxt.parctl_state);
        } else {
            WLMParctlRelease(&t_thrd.wlm_cxt.parctl_state);
        }
    }

    /* pgaudit */
    if ((u_sess->attr.attr_security.Audit_DML_SELECT != 0 || u_sess->attr.attr_security.Audit_DML != 0) &&
        u_sess->attr.attr_security.Audit_enabled && IsPostmasterEnvironment) {
        light_pgaudit_ExecutorEnd((Query*)linitial(m_cplan->query_list));
    }
    /* unified auditing policy */
    if (!(g_instance.status > NoShutdown) && light_unified_audit_executor_hook) {
        light_unified_audit_executor_hook((Query*)linitial(m_cplan->query_list));
    }
    /* global chain record */
    if (IS_PGXC_COORDINATOR && m_msgctl->has_relhash) {
        light_ledger_ExecutorEnd((Query*)linitial(m_cplan->query_list), m_msgctl->relhash);
    }

    /*
     * track_sql_count is on, counting WaitEventSQL for per user
     */
    if (u_sess->attr.attr_common.pgstat_track_activities && u_sess->attr.attr_common.pgstat_track_sql_count &&
        !u_sess->attr.attr_sql.enable_cluster_resize) {
        report_qps_type(m_cmdType);
        report_qps_type(queryType);
    }

    /* update unique sql stat */
    if (is_unique_sql_enabled() && is_local_unique_sql()) {
        UpdateUniqueSQLStat(NULL, NULL, GetCurrentStatementLocalStartTimestamp());
    }
    pgstate_update_percentile_responsetime();
    setCurrentProxy(NULL);
}

bool lightProxy::isDeleteLimit(const Query* query)
{
    if (query == NULL || query->commandType != CMD_DELETE)
        return false;
    if (query->limitCount != NULL)
        return true;
    return false;
}

/*
 * @Description:  according to commandType get corresponsile  WaitEventSQL,
 *    and call function 'pgstat_report_wait_count' to increase sql count
 */
void report_qps_type(CmdType commandType)
{
    switch (commandType) {
        case CMD_SELECT:
            pgstat_report_wait_count(WAIT_EVENT_SQL_SELECT);
            break;
        case CMD_UPDATE:
            pgstat_report_wait_count(WAIT_EVENT_SQL_UPDATE);
            break;
        case CMD_INSERT:
            pgstat_report_wait_count(WAIT_EVENT_SQL_INSERT);
            break;
        case CMD_DELETE:
            pgstat_report_wait_count(WAIT_EVENT_SQL_DELETE);
            break;
        case CMD_MERGE:
            pgstat_report_wait_count(WAIT_EVENT_SQL_MERGEINTO);
            break;
        case CMD_DML:
            pgstat_report_wait_count(WAIT_EVENT_SQL_DML);
            break;
        case CMD_DDL:
            pgstat_report_wait_count(WAIT_EVENT_SQL_DDL);
            break;
        case CMD_DCL:
            pgstat_report_wait_count(WAIT_EVENT_SQL_DCL);
            break;
        case CMD_TCL:
            pgstat_report_wait_count(WAIT_EVENT_SQL_TCL);
            break;
        default:
            /* do not map any commandType */
            break;
    }
}

/*
 * @Description:  according to sql query get corresponsile cmdType
 * @in  - const char *commandTag
 * @out - static CmdType
 */
CmdType set_cmd_type(const char* commandTag)
{
    CmdType cmd_type = CMD_UNKNOWN;
    if (strcmp(commandTag, "SELECT") == 0)
        cmd_type = CMD_SELECT;
    else if (strcmp(commandTag, "UPDATE") == 0)
        cmd_type = CMD_UPDATE;
    else if (strcmp(commandTag, "INSERT") == 0)
        cmd_type = CMD_INSERT;
    else if (strcmp(commandTag, "DELETE") == 0)
        cmd_type = CMD_DELETE;
    else if (strcmp(commandTag, "MERGE") == 0)
        cmd_type = CMD_MERGE;
    return cmd_type;
}

CmdType set_command_type_by_commandTag(const char* commandTag)
{
    CmdType cmd_type = CMD_UNKNOWN;
    int i;
    for (i = 0; i < MAX_COMMAND; i++) {
        if (strstr(commandTag, g_command_type_array[i].commandTag))
            return g_command_type_array[i].type;
    }
    return cmd_type;
}

bool IsLightProxyOn(void)
{
    return (u_sess->exec_cxt.cur_light_proxy_obj != NULL);
}

bool exec_query_through_light_proxy(List* querytree_list, Node* parsetree, bool snapshot_set, StringInfo msg, 
                                    MemoryContext OptimizerContext)
{
    if ((list_length(querytree_list) == 1) && !IsA(parsetree, CreateTableAsStmt) &&
        !IsA(parsetree, RefreshMatViewStmt)) {
        ExecNodes* single_exec_node = NULL;
        lightProxy* proxy = NULL;
        Query* query = (Query*)linitial(querytree_list);
        
        if (ENABLE_ROUTER(query->commandType)) {
            single_exec_node = lightProxy::checkRouterQuery(query);
        } else {
            single_exec_node = lightProxy::checkLightQuery(query);
        }
        /* only deal with single node */
        if (single_exec_node && list_length(single_exec_node->nodeList) +
            list_length(single_exec_node->primarynodelist) == 1) {
            /* GTMLite: need to mark that this is single shard statement */
            u_sess->exec_cxt.single_shard_stmt = true;
            if (CmdtypeSupportsHotkey(query->commandType))
                SendHotkeyToPgstat();

            proxy = New(OptimizerContext) lightProxy(query);
            proxy->m_nodeIdx = linitial_int(single_exec_node->nodeList);
            bool old_status = u_sess->exec_cxt.need_track_resource;
            if (u_sess->attr.attr_resource.resource_track_cost == 0 &&
                u_sess->attr.attr_resource.enable_resource_track &&
                u_sess->attr.attr_resource.resource_track_level != RESOURCE_TRACK_NONE) {
                u_sess->exec_cxt.need_track_resource = true;
                WLMSetCollectInfoStatus(WLM_STATUS_RUNNING);
            }
            proxy->runSimpleQuery(msg);

            /* Done with the snapshot used for parsing/planning */
            if (snapshot_set) {
                PopActiveSnapshot();
            }

            FreeExecNodes(&single_exec_node);
            u_sess->exec_cxt.need_track_resource = old_status;
            t_thrd.wlm_cxt.parctl_state.except = 0;
            return true;
        }
        FreeExecNodes(&single_exec_node);
        CleanHotkeyCandidates(true);
        return false;
    }
    return false;
}
void GPCDropLPIfNecessary(const char *stmt_name, bool need_drop_dnstmt, bool need_del, CachedPlanSource *reset_plan) {
    if (stmt_name == NULL || stmt_name[0] == '\0' || !IS_PGXC_COORDINATOR)
        return;
    lightProxy *lp = lightProxy::locateLpByStmtName(stmt_name);
    if (lp != NULL) {
        if (reset_plan) {
            lp->m_cplan = reset_plan;
        }
        if (stmt_name && need_drop_dnstmt) {
            lp->m_entry = NULL;
            DropDatanodeStatement(stmt_name);
        }
        if (need_del) {
            lightProxy::tearDown(lp);
        }
        return;
    }
    return;
}

void GPCFillMsgForLp(CachedPlanSource* psrc)
{
    Assert(psrc != NULL);
    if (psrc->gpc.status.InSavePlanList(GPC_SHARED)) {
        pfree_ext(psrc->gpc.key);
        MemoryContext oldcxt = MemoryContextSwitchTo(psrc->context);
        psrc->gpc.key = (GPCKey*)palloc0(sizeof(GPCKey));
        psrc->gpc.key->query_string = psrc->query_string;
        psrc->gpc.key->query_length = (uint32)strlen(psrc->query_string);
        psrc->gpc.key->spi_signature = psrc->spi_signature;
        GlobalPlanCache::EnvFill(&psrc->gpc.key->env, psrc->dependsOnRole);
        psrc->gpc.key->env.search_path = psrc->search_path;
        psrc->gpc.key->env.num_params = psrc->num_params;
        psrc->gpc.key->env.param_types = psrc->param_types;
        (void)MemoryContextSwitchTo(oldcxt);
    }
}
