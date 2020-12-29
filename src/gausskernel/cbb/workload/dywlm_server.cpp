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
 * dywlm_server.cpp
 *     functions for workload management
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/workload/dywlm_server.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/poolmgr.h"
#include "utils/atomic.h"
#include "utils/lsyscache.h"
#include "utils/memprot.h"
#include "utils/syscache.h"
#include "access/heapam.h"
#include "storage/lmgr.h"
#include "optimizer/nodegroups.h"
#include "workload/commgr.h"
#include "workload/workload.h"

#define MEM_UPPER_LIMIT 80       /* COMPUTATION ACCELERATION cluster only */
#define MEM_LOWER_LIMIT 40       /* COMPUTATION ACCELERATION cluster only */
#define MAX_STATEMENT_QUOTA 100; /* COMPUTATION ACCELERATION cluster only */
#define MID_STATEMENT_QUOTA 50;  /* COMPUTATION ACCELERATION cluster only */
#define MIN_STATEMENT_QUOTA 0;   /* COMPUTATION ACCELERATION cluster only */

/*  Freesize adjust value. When freesize_inc is greater than 20% of DN totalsize,
 *   we should adjust the freesize.
 */
#define FREESIZE_ADJUST_VALUE 20

/* dn is idle and cn is pending times */
THR_LOCAL int cn_dn_idle_times = 0;

static void dywlm_server_remove_record(
    ServerDynamicManager* srvmgr, const DynamicMessageInfo* msginfo, ParctlType qtype);

static void dywlm_server_release_phase2(ServerDynamicManager* srvmgr);

static List* dywlm_server_pull_records(ServerDynamicManager* srvmgr);

/*
 * @Description: server parse message
 * @IN input_message: input message string info
 * @IN info: message detail info
 * @Return: void
 * @See also:
 */
void dywlm_server_parse_message(StringInfo input_message, DynamicMessageInfo* info)
{
    /* get qid */
    info->qid.procId = (Oid)pq_getmsgint(input_message, 4);
    info->qid.queryId = (uint64)pq_getmsgint64(input_message);
    info->qid.stamp = (TimestampTz)pq_getmsgint64(input_message);

    /* get params */
    info->actpts = (int)pq_getmsgint(input_message, 4);
    info->max_actpts = (int)pq_getmsgint(input_message, 4);
    info->min_actpts = (int)pq_getmsgint(input_message, 4);
    info->maxpts = (int)pq_getmsgint(input_message, 4);
    info->memsize = (int)pq_getmsgint(input_message, 4);
    info->max_memsize = (int)pq_getmsgint(input_message, 4);
    info->min_memsize = (int)pq_getmsgint(input_message, 4);
    info->priority = (int)pq_getmsgint(input_message, 4);
    info->subquery = (int)pq_getmsgint(input_message, 4);

    /* get parallel control type */
    info->qtype = (ParctlType)pq_getmsgint(input_message, 4);

    /* get resource pool name */
    errno_t errval =
        strncpy_s(info->rpname, sizeof(info->rpname), pq_getmsgstring(input_message), sizeof(info->rpname) - 1);
    securec_check_errval(errval, , LOG);

    /* get node name */
    errval =
        strncpy_s(info->nodename, sizeof(info->nodename), pq_getmsgstring(input_message), sizeof(info->nodename) - 1);
    securec_check_errval(errval, , LOG);

    /* get node group */
    errval = strncpy_s(
        info->groupname, sizeof(info->groupname), pq_getmsgstring(input_message), sizeof(info->groupname) - 1);
    securec_check_errval(errval, , LOG);
    pq_getmsgend(input_message);
}

/*
 * @Description: check is the same node
 * @IN val1: queue node
 * @IN val2: node name
 * @Return: same or not same
 * @See also: it's used by function list_cell_clear
 */
bool is_same_node(void* val1, void* val2)
{
    ListCell* cell = (ListCell*)val1;
    char* nodename = (char*)val2;

    DynamicWorkloadRecord* qnode = (DynamicWorkloadRecord*)lfirst(cell);

    bool is_same = (strcmp(qnode->nodename, nodename) == 0) ? true : false;

    if (is_same) {
        /*
         * while trying to wake up the query, do not free memory,
         * just mark it will be removed. when wake up failed,
         * the thread will free the memory.
         */
        if (qnode->try_wakeup) {
            is_same = false;
            qnode->removed = true;
        } else {
            pfree(qnode);
        }
    }

    return is_same;
}

/*
 * @Description: check connection is invalid
 * @IN val1: node index
 * @IN val2: null pointer
 * @Return: valid or invalid
 * @See also: it's used by function list_cell_clear
 */
bool is_conn_invalid(void* val1, const void* val2)
{
    ListCell* cell = (ListCell*)val1;

    PoolAgent* agent = get_poolagent();

    int nodeidx = lfirst_int(cell);

    return agent->coord_connections[nodeidx] == val2;
}

/*
 * @Description: update current query info to hash table reserved in server
 * @Return: void
 * @See also:
 */
void update_resource_info(ServerDynamicManager* srvmgr, const Qid* qid, int memsize, int actpts)
{
    USE_AUTO_LWLOCK(WorkloadRecordLock, LW_SHARED);
    DynamicWorkloadRecord* record =
        (DynamicWorkloadRecord*)hash_search(srvmgr->global_info_hashtbl, qid, HASH_FIND, NULL);
    if (record != NULL) {
        record->memsize = memsize;
        record->actpts = actpts;
    }
}

/*
 * @Description: server reply message to client
 * @IN qid: query id
 * @IN qtype: parctl type
 * @IN etype: enqueue type
 * @Return: void
 * @See also:
 */
void dywlm_server_reply(DynamicMessageInfo* msginfo, ParctlType qtype, EnqueueType etype)
{
    Assert(msginfo != NULL);

    Qid* qid = &msginfo->qid;
    /*
     * If it is in the release state, it may wake up
     * the next job of the node, depending on whether
     * the qid is consistent with the node, which needs
     * to be determined by the caller
     */
    StringInfoData retbuf;

    pq_beginmessage(&retbuf, 'w');
    pq_sendint(&retbuf, (int)qid->procId, sizeof(int));
    pq_sendint64(&retbuf, (uint64)qid->queryId);
    pq_sendint64(&retbuf, (int64)qid->stamp);
    pq_sendint(&retbuf, (int)qtype, sizeof(int));
    pq_sendint(&retbuf, (int)etype, sizeof(int));
    pq_sendint(&retbuf, msginfo->memsize, sizeof(int));
    pq_sendint(&retbuf, msginfo->actpts, sizeof(int));
    pq_sendstring(&retbuf, ""); /* we need not know node name while replying message */
    pq_sendstring(&retbuf, msginfo->groupname);

    pq_endmessage(&retbuf);

    pq_flush();
}

/*
 * @Description: server reply message to client for synchronizing
 * @IN msginfo: detail message
 * @Return: void
 * @See also:
 */
void dywlm_server_sync_reply(ServerDynamicManager* srvmgr, DynamicMessageInfo* msginfo)
{
    Assert(msginfo != NULL);

    WLMAutoLWLock htab_lock(WorkloadRecordLock, LW_SHARED);

    htab_lock.AutoLWLockAcquire();

    DynamicWorkloadRecord* qnode =
        (DynamicWorkloadRecord*)hash_search(srvmgr->global_info_hashtbl, &msginfo->qid, HASH_FIND, NULL);

    htab_lock.AutoLWLockRelease();

    /*
     * the query request may received last time, but due to connection
     * error, client will send message again. find the allocated memory
     */
    if (qnode != NULL && qnode->qtype != PARCTL_NONE) {
        msginfo->qtype = (msginfo->qtype == PARCTL_TRANSACT) ? msginfo->qtype : qnode->qtype;
        msginfo->etype = (qnode->qtype == PARCTL_ACTIVE) ? ENQUEUE_NONE : ENQUEUE_BLOCK;
        msginfo->memsize = qnode->memsize;
        msginfo->actpts = qnode->actpts;
    }

    StringInfoData retbuf;

    pq_beginmessage(&retbuf, 'w');
    pq_sendint(&retbuf, (int)msginfo->qid.procId, sizeof(int));
    pq_sendint64(&retbuf, (uint64)msginfo->qid.queryId);
    pq_sendint64(&retbuf, (int64)msginfo->qid.stamp);
    pq_sendint(&retbuf, (int)msginfo->qtype, sizeof(int));
    pq_sendint(&retbuf, (int)msginfo->etype, sizeof(int));
    pq_sendint(&retbuf, msginfo->memsize, sizeof(int));
    pq_sendint(&retbuf, msginfo->actpts, sizeof(int));
    pq_sendstring(&retbuf, "");
    pq_sendstring(&retbuf, msginfo->groupname);

    pq_endmessage(&retbuf);
    pq_flush();
}

/*
 * @Description: server update global amount, including memory, resource pool active points
 * @IN srvmgr: the server management of one node group
 * @IN memsize: memory size to add
 * @IN maxpts: max active points of resource pool
 * @IN actpts: active points to add
 * @IN rpname: resource pool name
 * @Return: void
 * @See also:
 */
void dywlm_server_amount_update(ServerDynamicManager* srvmgr, int memsize, int maxpts, int actpts, const char* rpname)
{
    /* it's running, we have to compute the free memory size with lock */
    USE_CONTEXT_LOCK(&srvmgr->global_list_mutex);

    srvmgr->freesize += memsize;

    if (srvmgr->freesize > srvmgr->freesize_limit) {
        srvmgr->freesize = srvmgr->freesize_limit;
    }

    RELEASE_CONTEXT_LOCK();

    if (maxpts > 0 && StringIsValid(rpname)) {
        Oid rpoid = get_resource_pool_oid(rpname);

        if (!OidIsValid(rpoid)) {
            return;
        }

        USE_AUTO_LWLOCK(ResourcePoolHashLock, LW_SHARED);

        /* get resource pool from hash table */
        ResourcePool* respool = GetRespoolFromHTab(rpoid);

        if (respool == NULL) {
            return;
        }

        /* compute active points with lock */
        USE_LOCK_TO_ADD(respool->mutex, respool->active_points, actpts);
    }
}

/*
 * @Description: server send message to client
 * @IN msginfo: message detail info
 * @Return: 0: send ok
 *          not 0: send failed
 * @See also:
 */
int dywlm_server_send(ServerDynamicManager* srvmgr, const DynamicMessageInfo* msginfo)
{
    PGXCNodeHandle* cn_handle = NULL;
    PGXCNodeAllHandles* pgxc_handles = NULL;
    List* coordlist = NIL;

    int ret = DYWLM_SEND_OK;
    int retry_count = 0;

    if (IsQidInvalid(&msginfo->qid)) {
        return ret;
    }

    /* If node name is the same node as CCN, the message is forwarded directly */
    if (strcmp(msginfo->nodename, g_instance.attr.attr_common.PGXCNodeName) == 0) {
        return dywlm_client_post(srvmgr->climgr, msginfo);
    }

    MemoryContext curr_mcxt = CurrentMemoryContext;

    PoolAgent* agent = NULL;

    while (retry_count < 3) {
        bool hasError = false;
        bool isFinished = false;

        /* get node index from the qid to send message */
        int nodeidx = dywlm_get_node_idx(msginfo->nodename);

        if (nodeidx == -1) {
            return DYWLM_NO_NODE;
        }

        Assert(coordlist == NULL);

        coordlist = lappend_int(coordlist, nodeidx);

        PG_TRY();
        {
            pgxc_handles = get_handles(NULL, coordlist, true);

            agent = get_poolagent();
        }
        PG_CATCH();
        {
            pgxc_handles = NULL;
            FlushErrorState();
        }
        PG_END_TRY();

        if (pgxc_handles == NULL) {
            MemoryContextSwitchTo(curr_mcxt);

            ++retry_count;

            list_free_ext(coordlist);

            if (retry_count >= 3) {
                return DYWLM_SEND_FAILED;
            }

            continue;
        }

        cn_handle = pgxc_handles->coord_handles[0];

        if (agent == NULL || agent->coord_connections[nodeidx] == NULL) {
            return DYWLM_SEND_FAILED;
        }

        if (cn_handle != NULL) {
            /* send dynamic workload message to coordinator from central node */
            if (pgxc_node_dywlm_send_params(cn_handle, msginfo) != 0) {
                /*
                 * Before reconnecting, we must first clean up the infomation of
                 * this connection, including:
                 *
                 * 1. the slot to central node of the agent need clear up
                 * 2. the original connection socket release
                 * 3. pool cache may be cleaned up in order to reconnect successfully
                 */
                release_connection(agent, &(agent->coord_connections[nodeidx]), agent->coord_conn_oids[nodeidx], true);

                ereport(DEBUG1,
                    (errmsg("pooler: Failed to send dynamic workload params, node idx: %d, node oid: %u",
                        nodeidx,
                        agent->coord_conn_oids[nodeidx])));

                ++retry_count;

                /* the original connection socket reset */
                pgxc_node_free(cn_handle);
                pgxc_node_init(cn_handle, NO_SOCKET);

                list_free_ext(coordlist);
                release_pgxc_handles(pgxc_handles);
                pgxc_handles = NULL;

                /* the number of retries reaches the limit, we have to return an error */
                if (retry_count >= 3) {
                    return DYWLM_SEND_FAILED;
                }

                continue;
            }

            struct timeval timeout = {120, 0};

            while (true) {
                if (!pgxc_node_receive(1, &cn_handle, &timeout)) {
                    char* msg = NULL;
                    int len, retcode;
                    char msg_type;

                    msg_type = get_message(cn_handle, &len, &msg);

                    switch (msg_type) {
                        case 'w': {
                            errno_t errval = memcpy_s(&retcode, sizeof(int), msg, 4);
                            securec_check_errval(errval, , LOG);
                            retcode = (int)ntohl(retcode);

                            if (retcode >= 0) {
                                ret = retcode;
                            }

                            ereport(DEBUG2, (errmsg("receive code for sending: %d", retcode)));
                            if (retcode == -1) {
                                ereport(ERROR, (errmsg("invalid retcode [%d]", retcode)));
                            }
                            break;
                        }
                        case 'E':
                            hasError = true;
                            break;
                        case 'Z':
                            if (hasError) {
                                list_free(coordlist);
                                release_pgxc_handles(pgxc_handles);
                                pgxc_handles = NULL;
                                ereport(LOG,
                                    (errmsg("receive message error, "
                                            "error occurred on node %u, please check node log",
                                        agent->coord_conn_oids[nodeidx])));
                                return DYWLM_SEND_FAILED;
                            }
                            break;
                        default:
                            break;
                    }

                    if (msg_type == 'w' || msg_type == 'Z') {
                        isFinished = true;
                        break;
                    }
                } else {
                    /* release the slot of the agent */
                    release_connection(agent,
                        &(agent->coord_connections[nodeidx]), agent->coord_conn_oids[nodeidx], true);

                    ereport(LOG,
                        (errmsg("pooler: Failed to send dynamic workload params, node idx: %d, node oid: %u",
                            nodeidx,
                            agent->coord_conn_oids[nodeidx])));

                    ++retry_count;

                    /* reset the socket */
                    pgxc_node_free(cn_handle);
                    pgxc_node_init(cn_handle, NO_SOCKET);

                    list_free_ext(coordlist);
                    release_pgxc_handles(pgxc_handles);
                    pgxc_handles = NULL;

                    if (retry_count >= 3) {
                        return DYWLM_SEND_FAILED;
                    }

                    break;
                }
            }

            cn_handle->state = DN_CONNECTION_STATE_IDLE;

            if (isFinished) {
                break;
            }
        }
    }

    list_free(coordlist);
    release_pgxc_handles(pgxc_handles);

    return ret;
}

/*
 * @Description: workload record whether in the hash cache
 * @IN qid: message query id
 * @Return: query state now
 * @See also:
 */
ParctlType dywlm_record_in_cache(ServerDynamicManager* srvmgr, const Qid* qid)
{
    ParctlType qtype = PARCTL_NONE;

    USE_AUTO_LWLOCK(WorkloadRecordLock, LW_SHARED);

    DynamicWorkloadRecord* qnode =
        (DynamicWorkloadRecord*)hash_search(srvmgr->global_info_hashtbl, qid, HASH_FIND, NULL);

    /* get queue type of the dynamic workload record */
    if (qnode != NULL) {
        qtype = qnode->qtype;
    }

    return qtype;
}

/*
 * @Description: reset hash cache
 * @IN void
 * @Return: void
 * @See also:
 */
void dywlm_server_reset_cache(ServerDynamicManager* srvmgr)
{
    USE_AUTO_LWLOCK(WorkloadRecordLock, LW_EXCLUSIVE);

    DynamicWorkloadRecord* record = NULL;

    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, srvmgr->global_info_hashtbl);

    /* remove all records from the hash table */
    while ((record = (DynamicWorkloadRecord*)hash_seq_search(&hash_seq)) != NULL) {
        hash_search(srvmgr->global_info_hashtbl, &record->qid, HASH_REMOVE, NULL);
    }
}

/*
 * @Description: create workload record in the hash table
 * @IN msginfo: message detail info
 * @IN qtype: record queue type
 * @Return: -1 create failed
 *          0 create success
 * @See also:
 */
int dywlm_server_create_record(
    ServerDynamicManager* srvmgr, const DynamicMessageInfo* msginfo, ParctlType qtype, bool maybe_deleted = false)
{
    /* qid is invalid, create failed */
    if (IsQidInvalid(&msginfo->qid)) {
        return -1;
    }

    bool found = false;

    USE_AUTO_LWLOCK(WorkloadRecordLock, LW_EXCLUSIVE);

    DynamicWorkloadRecord* record =
        (DynamicWorkloadRecord*)hash_search(srvmgr->global_info_hashtbl, &msginfo->qid, HASH_ENTER_NULL, &found);

    int ret = -1;

    /* init dynamic workload record */
    if (record != NULL && !found) {
        record->qid = msginfo->qid;
        record->memsize = msginfo->memsize;
        record->max_memsize = msginfo->max_memsize;
        record->min_memsize = msginfo->min_memsize;
        record->actpts = msginfo->actpts;
        record->max_actpts = msginfo->max_actpts;
        record->min_actpts = msginfo->min_actpts;
        record->maxpts = msginfo->maxpts;
        record->priority = msginfo->priority;
        record->qtype = qtype;
        record->pnode = NULL;

        if (!(COMP_ACC_CLUSTER || (record->max_memsize > 0 && record->min_actpts > 0))) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid paras.")));
        }

        errno_t errval = strncpy_s(record->rpname, sizeof(record->rpname), msginfo->rpname, sizeof(record->rpname) - 1);
        securec_check_errval(errval, , LOG);
        errval = strncpy_s(record->nodename, sizeof(record->nodename), msginfo->nodename, sizeof(record->nodename) - 1);
        securec_check_errval(errval, , LOG);
        errval =
            strncpy_s(record->groupname, sizeof(record->groupname), msginfo->groupname, sizeof(record->groupname) - 1);
        securec_check_errval(errval, , LOG);

        record->try_wakeup = false;
        record->removed = false;
        record->is_dirty = false;
        record->maybe_deleted = maybe_deleted;

        ret = 0;

        ereport(DEBUG3,
            (errmsg("add job info into server records, qid:[%u, %lu, %ld]",
                record->qid.procId,
                record->qid.queryId,
                record->qid.stamp)));
    }

    return ret;
}

/*
 * @Description: server write cache record
 * @IN qid: message detail query id
 * @IN qtype: parctl type
 * @Return: void
 * @See also:
 */
void dywlm_server_write_cache(ServerDynamicManager* srvmgr, const Qid* qid, ParctlType qtype)
{
    if (IsQidInvalid(qid)) {
        return;
    }

    USE_AUTO_LWLOCK(WorkloadRecordLock, LW_EXCLUSIVE);

    /* if queue type is release, we will remove it from the cache */
    if (qtype == PARCTL_RELEASE) {
        ereport(DEBUG3,
            (errmsg("remove job info from server records, qid:[%u, %lu, %ld]", qid->procId, qid->queryId, qid->stamp)));
        hash_search(srvmgr->global_info_hashtbl, qid, HASH_REMOVE, NULL);
        return;
    }

    DynamicWorkloadRecord* record =
        (DynamicWorkloadRecord*)hash_search(srvmgr->global_info_hashtbl, qid, HASH_FIND, NULL);
    /* set queue type to dynamic workload record */
    if (record != NULL) {
        record->maybe_deleted = false;
        record->try_wakeup = false;
        if (qtype == PARCTL_RESERVE) {
            record->is_dirty = false;
        } else {
            record->qtype = qtype;
        }
    }
}

/*
 * @Description: find the node waiting in list by qid
 * @IN  srvmgr: the server management for one node group
 * @IN  qid: the qid of node
 * @Return: the node if find
 * @See also:
 */
DynamicWorkloadRecord* dywlm_search_node(ServerDynamicManager* srvmgr, const Qid* qid)
{
    /* search priority list */
    foreach_cell(cell, srvmgr->global_waiting_list)
    {
        WLMListNode* pnode = (WLMListNode*)lfirst(cell);

        /* search request list */
        DynamicWorkloadRecord* node = search_node<DynamicWorkloadRecord, Qid>(pnode->request_list, qid);

        if (node != NULL) {
            Assert(node->pnode != NULL);
            return node;
        }
    }

    return NULL;
}

/*
 * @Description: check active count on server
 * @IN  srvmgr: the server management for one node group
 * @Return: has active count
 * @See also:
 */
bool dywlm_server_check_active(ServerDynamicManager* srvmgr)
{
    Assert(srvmgr != NULL);
    Assert(StringIsValid(srvmgr->group_name));

    USE_AUTO_LWLOCK(WorkloadRecordLock, LW_SHARED);

    if (hash_get_num_entries(srvmgr->global_info_hashtbl) <= 0) {
        return false;
    }

    DynamicWorkloadRecord* record = NULL;

    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, srvmgr->global_info_hashtbl);

    /* scan all active records from the server cache for current node */
    while ((record = (DynamicWorkloadRecord*)hash_seq_search(&hash_seq)) != NULL) {
        /* append it into the records list while qtype is ACTIVE */
        if (!record->is_dirty && record->qtype == PARCTL_ACTIVE) {
            hash_seq_term(&hash_seq);
            return true;
        }
    }

    return false;
}

/*
 * @Description: global parallel control release
 * @IN  srvmgr: the server management for one node group
 * @OUT msginfo: message detail info
 * @Return: enqueue type: ERROR or NONE
 * @See also: Global Workload Manager.
 *            Release resource after execution complete,
 *            and wake up the statements with max priority in
 *            the statements waiting list.
 */
EnqueueType dywlm_global_release(ServerDynamicManager* srvmgr, DynamicMessageInfo* msginfo)
{
    Assert(msginfo != NULL && srvmgr != NULL);

    ListCell* cell = list_head(srvmgr->global_waiting_list);
    ListCell* prev = NULL;

    while (cell != NULL) {
        WLMListNode* node = (WLMListNode*)lfirst(cell);
        ListCell* qcell = list_head(node->request_list);
        ListCell* qprev = NULL;

        while (qcell != NULL) {
            DynamicWorkloadRecord* qnode = (DynamicWorkloadRecord*)lfirst(qcell);

            if (qnode && !qnode->try_wakeup) {
                Assert(COMP_ACC_CLUSTER || (qnode->memsize > 0 && qnode->max_memsize > 0 && qnode->min_actpts > 0));

                /*
                 * We have to re-check the node index in the memory, maybe
                 * node index is changed bacuase some nodes crash down
                 */
                int nodeidx = dywlm_get_node_idx(qnode->nodename);

                /* node is removed from the cache, we need not release any request on this node */
                if (nodeidx < 0) {
                    node->request_list = list_delete_cell(node->request_list, qcell, qprev);

                    dywlm_server_write_cache(srvmgr, &qnode->qid, PARCTL_RELEASE);

                    pfree(qnode);

                    if (qprev == NULL) {
                        qcell = list_head(node->request_list);
                    } else {
                        qcell = lnext(qprev);
                    }

                    continue;
                }

                int global_freesize =
                    ((srvmgr->freesize_inc > 0) ? srvmgr->freesize : (srvmgr->freesize + srvmgr->freesize_inc));
                bool is_running = true;

                if (COMP_ACC_CLUSTER) { /* acceleration cluster */
                    if (u_sess->attr.attr_resource.max_active_statements > 0) { /* no control for -1 or 0 */
                        if (srvmgr->active_count >= u_sess->attr.attr_resource.max_active_statements) {
                            ereport(DEBUG1,
                                (errmsg(
                                    "No more queued statement can be released as max_active_statements is reached")));
                            return ENQUEUE_BLOCK;
                        }
                    }

                    if (srvmgr->statement_quota <= 0) { /* no quota, wait for next chance */
                        ereport(DEBUG1, (errmsg("No more queued statement can be released as quota is 0")));
                        return ENQUEUE_BLOCK;
                    }

                    srvmgr->active_count++;
                    srvmgr->statement_quota--;
                } else { /* customer cluster */
                    /*
                     * free memory size is not enough even for the mininum statement,
                     * it has to wait for the next chance
                     */
                    if (global_freesize < qnode->min_memsize) {
                        /*
                         * If there is no job executing, but the memory
                         * still does not satisfy the first memory
                         * request of the queue, grant the request
                         * privilege to allow it to execute
                         */
                        is_running = dywlm_server_check_active(srvmgr);
                        if (is_running) {
                            return ENQUEUE_BLOCK;
                        }

                        ereport(DEBUG3,
                            (errmsg("no job executing, but the memory still does not satisfy the first memory in "
                                    "dywlm_global_release,"
                                    "ID[%u,%lu,%ld] current, max and min memsize is [%d,%d,%d], and freesize is %d now",
                                qnode->qid.procId,
                                qnode->qid.queryId,
                                qnode->qid.stamp,
                                qnode->memsize,
                                qnode->max_memsize,
                                qnode->min_memsize,
                                srvmgr->freesize)));
                    /*
                     * we should try waking up the statements by different plans
                     */
                    } else {
                        if (global_freesize >= qnode->max_memsize) {
                            qnode->memsize = qnode->max_memsize;
                            qnode->actpts = qnode->max_actpts;
                            msginfo->use_max_mem = true;
                        } else {
                            qnode->memsize = global_freesize;
                            qnode->actpts = (qnode->max_actpts == FULL_PERCENT && qnode->min_actpts == FULL_PERCENT)
                                                ? FULL_PERCENT
                                                : (long)qnode->memsize * qnode->max_actpts / qnode->max_memsize;
                            msginfo->use_max_mem = false;
                        }

                        ereport(DEBUG3,
                            (errmsg("ID[%u,%lu,%ld] WAKEUP through dywlm_global_release,"
                                    "current, max and min memsize is [%d,%d,%d], and freesize is %d now",
                                qnode->qid.procId,
                                qnode->qid.queryId,
                                qnode->qid.stamp,
                                qnode->memsize,
                                qnode->max_memsize,
                                qnode->min_memsize,
                                srvmgr->freesize)));
                    }

                    srvmgr->freesize -= qnode->memsize;
                }

                msginfo->qid = qnode->qid;
                msginfo->memsize = qnode->memsize;
                msginfo->max_memsize = qnode->max_memsize;
                msginfo->min_memsize = qnode->min_memsize;
                msginfo->actpts = qnode->actpts;
                msginfo->max_actpts = qnode->max_actpts;
                msginfo->min_actpts = qnode->min_actpts;
                msginfo->maxpts = qnode->maxpts;
                msginfo->priority = qnode->pnode->data;

                errno_t errval =
                    strncpy_s(msginfo->rpname, sizeof(qnode->rpname), qnode->rpname, sizeof(qnode->rpname) - 1);
                securec_check_errval(errval, , LOG);
                errval =
                    strncpy_s(msginfo->nodename, sizeof(qnode->nodename), qnode->nodename, sizeof(qnode->nodename) - 1);
                securec_check_errval(errval, , LOG);
                errval = strncpy_s(
                    msginfo->groupname, sizeof(qnode->groupname), qnode->groupname, sizeof(qnode->groupname) - 1);
                securec_check_errval(errval, , LOG);

                if (msginfo->memsize > msginfo->max_memsize || msginfo->memsize < msginfo->min_memsize) {
                    ereport(LOG,
                        (errmsg("ID[%u,%lu,%ld] through dywlm_global_reserve,"
                                "current, max and min memsize is [%d,%d,%d], and freesize is %d now"
                                "it is due to the current active query list being empty : %d",
                            msginfo->qid.procId,
                            msginfo->qid.queryId,
                            msginfo->qid.stamp,
                            msginfo->memsize,
                            msginfo->max_memsize,
                            msginfo->min_memsize,
                            srvmgr->freesize,
                            !is_running)));
                }

                if (is_running) {
                    Assert(msginfo->memsize <= msginfo->max_memsize && msginfo->memsize >= msginfo->min_memsize);
                }

                return ENQUEUE_NONE;
            }

            qprev = qcell;
            qcell = lnext(qprev);
        }

        /* remove invalid priority from the global priority list */
        if (list_length(node->request_list) == 0) {
            srvmgr->global_waiting_list = list_delete_cell(srvmgr->global_waiting_list, cell, prev);

            pfree(node);

            if (prev == NULL) {
                cell = list_head(srvmgr->global_waiting_list);
            } else {
                cell = lnext(prev);
            }

            continue;
        }

        prev = cell;
        cell = lnext(prev);
    }

    return ENQUEUE_NONE;
}

/*
 * @Description: set DynamicWorkloadRecord detail from DynamicMessageInfo
 */
void dywlm_set_queue_nodes(DynamicWorkloadRecord* qnode, const DynamicMessageInfo* msginfo)
{
    if (qnode == NULL || msginfo == NULL) {
        return;
    }

    qnode->memsize = msginfo->max_memsize;
    qnode->max_memsize = msginfo->max_memsize;
    qnode->min_memsize = msginfo->min_memsize;
    qnode->actpts = msginfo->max_actpts;
    qnode->max_actpts = msginfo->max_actpts;
    qnode->min_actpts = msginfo->min_actpts;

    qnode->qid = msginfo->qid;
    qnode->maxpts = msginfo->maxpts;

    qnode->is_dirty = false;
    qnode->try_wakeup = false;
    qnode->removed = false;
}

/*
 * @Description: append queue node to global pending list
 * @IN  srvmgr: the server management for one node group
 * @IN msginfo: message detail info
 * @Return: enqueue type
 * @See also:
 */
EnqueueType dywlm_append_global_list(ServerDynamicManager* srvmgr, const DynamicMessageInfo* msginfo)
{
    WLMListNode* pnode = NULL;
    ListCell* cell = NULL;
    DynamicWorkloadRecord* qnode = NULL;
    errno_t errval;

    USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

    /* search the node with the priority, if do not find it, it will be created */
    ListCell* lcnode = append_to_list<int, WLMListNode, true>(&srvmgr->global_waiting_list, &msginfo->priority);

    if (lcnode == NULL) {
        goto alloc_failed;
    }

    pnode = (WLMListNode*)lfirst(lcnode);
    pnode->data = msginfo->priority;

    qnode = (DynamicWorkloadRecord*)palloc0_noexcept(sizeof(DynamicWorkloadRecord));

    if (qnode == NULL) {
        goto alloc_failed;
    }

    /* set queue node */
    dywlm_set_queue_nodes(qnode, msginfo);
    qnode->pnode = pnode;

    Assert(COMP_ACC_CLUSTER || (qnode->max_memsize > 0 && qnode->min_actpts > 0));

    errval = strncpy_s(qnode->rpname, sizeof(qnode->rpname), msginfo->rpname, sizeof(qnode->rpname) - 1);
    securec_check_errval(errval, , LOG);
    errval = strncpy_s(qnode->nodename, sizeof(qnode->nodename), msginfo->nodename, sizeof(qnode->nodename) - 1);
    securec_check_errval(errval, , LOG);
    errval = strncpy_s(qnode->groupname, sizeof(qnode->groupname), msginfo->groupname, sizeof(qnode->groupname) - 1);
    securec_check_errval(errval, , LOG);

    cell = (ListCell*)palloc0_noexcept(sizeof(ListCell));

    if (cell == NULL) {
        pfree(qnode);
        goto alloc_failed;
    }

    lfirst(cell) = qnode;

    qnode->pnode->request_list = lappend2(qnode->pnode->request_list, cell);

    /* append failed */
    if (qnode->pnode->request_list == NULL) {
        pfree(qnode);
        pfree(cell);
        goto alloc_failed;
    }

    return ENQUEUE_BLOCK;

alloc_failed:
    ereport(DEBUG1,
        (errmsg("ID[%u,%lu,%ld] reserve global statements failed, out of memory.",
            msginfo->qid.procId,
            msginfo->qid.queryId,
            msginfo->qid.stamp)));
    return ENQUEUE_MEMERR;
}

/*
 * @Description: decide if a statement can start executing; this function
         is only used for Computation Acceleration cluster
 * @IN msginfo: message detail info
 * @Return: enqueue type
 * @See also:
 */
EnqueueType dywlm_global_reserve_comp_acc(ServerDynamicManager* srvmgr, DynamicMessageInfo* msginfo)
{
    EnqueueType ret = ENQUEUE_BLOCK;

    Assert(msginfo != NULL);

    if ((-1 == u_sess->attr.attr_resource.max_active_statements) ||
        (0 == u_sess->attr.attr_resource.max_active_statements) ||
        (srvmgr->active_count < u_sess->attr.attr_resource.max_active_statements)) {
        /*
         * in a monitoring interval, if statement quota > 0
         * a newly arriving statement can start executing
         */
        if (srvmgr->statement_quota > 0) {
            srvmgr->statement_quota--;
            srvmgr->active_count++;

            ereport(DEBUG2,
                (errmsg("The newly arriving statement starts executing in Acceleration Cluster, quota: %d, count: %d",
                    srvmgr->statement_quota,
                    srvmgr->active_count)));
            return ENQUEUE_NONE;
        }
    }

    ret = dywlm_append_global_list(srvmgr, msginfo);
    ereport(DEBUG1, (errmsg("The newly arriving statement is queued in Acceleration Cluster.")));

    return ret;
}

/*
 * @Description: reserve resource to execute statement
 * @IN  srvmgr: the server management for one node group
 * @IN msginfo: message detail info
 * @Return: enqueue type
 * @See also:
 */
EnqueueType dywlm_global_reserve(ServerDynamicManager* srvmgr, DynamicMessageInfo* msginfo)
{
    Assert(msginfo != NULL);

    /* while free memory of DNs is less than booking, use it as global freesize */
    int global_freesize = ((srvmgr->freesize_inc > 0) ? srvmgr->freesize : (srvmgr->freesize + srvmgr->freesize_inc));

    bool found = false;

    /* find if there is statement with same priority waiting in the waiting list.
     * Expect this is a subquery of a query and not the first of it */
    if (!msginfo->subquery || !msginfo->insubquery) {
        search_list<int, WLMListNode>(srvmgr->global_waiting_list, &msginfo->priority, &found);
    }

    bool is_running = dywlm_server_check_active(srvmgr);

    /*
     * If a job is in the same priority queue, that we think current
     * job must be queued, otherwise, if the memory resources are
     * sufficient, we directly deducted after the implementation.
     * If a job can be runned by its maximum evaluated memory size, then let it go.
     * If not, try the minimum evaluated memory. If even minimum cannot, push it into the quene.
     */
    if (!found && (global_freesize >= msginfo->min_memsize || !is_running)) {
        if (global_freesize >= msginfo->max_memsize) {
            srvmgr->freesize -= msginfo->max_memsize;
            msginfo->memsize = msginfo->max_memsize;
            msginfo->actpts = msginfo->max_actpts;
            msginfo->use_max_mem = true;
        } else {
            msginfo->memsize = global_freesize;
            srvmgr->freesize -= msginfo->memsize;
            msginfo->actpts = (msginfo->max_actpts == FULL_PERCENT && msginfo->min_actpts == FULL_PERCENT)
                                  ? FULL_PERCENT
                                  : (long)msginfo->memsize * msginfo->max_actpts / msginfo->max_memsize;
            msginfo->use_max_mem = false;
        }

        if (msginfo->memsize > msginfo->max_memsize || msginfo->memsize < msginfo->min_memsize) {
            ereport(LOG,
                (errmsg("ID[%u,%lu,%ld] through dywlm_global_reserve,"
                        "current, max and min memsize is [%d,%d,%d], and freesize is %d now"
                        "it is due to the current active query list being empty : %d",
                    msginfo->qid.procId,
                    msginfo->qid.queryId,
                    msginfo->qid.stamp,
                    msginfo->memsize,
                    msginfo->max_memsize,
                    msginfo->min_memsize,
                    srvmgr->freesize,
                    !is_running)));
        }

        if (is_running) {
            if (!(msginfo->memsize <= msginfo->max_memsize && msginfo->memsize >= msginfo->min_memsize)) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid paras.")));
            }
        }

        return ENQUEUE_NONE;
    }

    return dywlm_append_global_list(srvmgr, msginfo);
}

/*
 * @Description: group internal release
 * @IN  srvmgr: the server management for one node group
 * @IN respool: resource pool
 * @IN msginfo: message detail info
 * @Return: void
 * @See also:
 */
void dywlm_group_release_internal(ServerDynamicManager* srvmgr, ResourcePool* respool, int actpts)
{
    WLMContextLock list_lock(&srvmgr->global_list_mutex);
    WLMContextLock rp_lock(&respool->mutex);

    list_lock.Lock();
    rp_lock.Lock();

    if (respool->active_points > 0 && actpts > 0) {
        respool->active_points -= actpts;

        if (respool->active_points <= 0) {
            respool->active_points = 0;
        }
    }

    DynamicMessageInfo tmpinfo;

    /* get the qid to wake up */
    if (list_length(respool->waiters) > 0) {
        ListCell* cell = list_head(respool->waiters);
        int send_result = 0;

        while (cell != NULL) {
            ListCell* next = lnext(cell);

            DynamicWorkloadRecord* qnode = (DynamicWorkloadRecord*)lfirst(cell);

            if (qnode == NULL || qnode->try_wakeup) {
                cell = next;
                continue;
            }

            int nodeidx = -1;

            /*
             * We have to re-check the node index in the memory, maybe
             * node index is changed because some nodes crash down
             */
            if ((nodeidx = dywlm_get_node_idx(qnode->nodename)) < 0) {
                /* node does not exist in the cache, ignore the request in the queue */
                respool->waiters = list_delete_cell2(respool->waiters, cell);

                dywlm_server_write_cache(srvmgr, &qnode->qid, PARCTL_RELEASE);

                pfree(qnode);

                cell = next;
                continue;
            }

            int global_freesize =
                ((srvmgr->freesize_inc > 0) ? srvmgr->freesize : (srvmgr->freesize + srvmgr->freesize_inc));

            /* we should try waking up the statements in diffrent ways */
            if (qnode->min_memsize > global_freesize || respool->active_points + qnode->min_actpts > qnode->maxpts) {
                break;
            } else {
                if (qnode->max_memsize <= global_freesize &&
                    respool->active_points + qnode->max_actpts <= qnode->maxpts) {
                    qnode->memsize = qnode->max_memsize;
                    qnode->actpts = qnode->max_actpts;
                    tmpinfo.use_max_mem = true;
                } else {
                    /*
                     * statement can not run with max_memsize
                     * run statement with remaining memory(min(respool_freesize, global_freesize))
                     * but remaining memory calculated may less than min_memsize due to rouding
                     * we should ensure qnode->memsize >= qnode->min_memsize
                     */
                    int remainMem = Min(global_freesize,
                        ((long)(qnode->maxpts - respool->active_points) * qnode->max_memsize / qnode->max_actpts));
                    qnode->memsize = Max(remainMem, qnode->min_memsize);
                    int remainActpts = (long)(qnode->memsize * qnode->max_actpts / qnode->max_memsize);
                    qnode->actpts = Max(remainActpts, qnode->min_actpts);
                    tmpinfo.use_max_mem = false;
                }

                update_resource_info(srvmgr, &qnode->qid, qnode->memsize, qnode->actpts);

                ereport(DEBUG3,
                    (errmsg("ID[%u,%lu,%ld] WAKEUP in dywlm_group_release_internal,"
                            "Memsize[current:%d, max:%d, min:%d], [free_memsize :%d, free_rpact:%d] now",
                        qnode->qid.procId,
                        qnode->qid.queryId,
                        qnode->qid.stamp,
                        qnode->memsize,
                        qnode->max_memsize,
                        qnode->min_memsize,
                        (srvmgr->freesize - qnode->memsize),
                        (qnode->maxpts - respool->active_points - qnode->actpts))));
            }

            /* set info to write cache */
            tmpinfo.qid = qnode->qid;
            tmpinfo.memsize = qnode->memsize;
            tmpinfo.max_memsize = qnode->max_memsize;
            tmpinfo.min_memsize = qnode->min_memsize;
            tmpinfo.actpts = qnode->actpts;
            tmpinfo.max_actpts = qnode->max_actpts;
            tmpinfo.min_actpts = qnode->min_actpts;
            tmpinfo.maxpts = qnode->maxpts;
            tmpinfo.qtype = PARCTL_RELEASE;
            tmpinfo.etype = ENQUEUE_NONE;

            errno_t errval =
                strncpy_s(tmpinfo.nodename, sizeof(qnode->nodename), qnode->nodename, sizeof(qnode->nodename) - 1);
            securec_check_errval(errval, , LOG);
            errval = strncpy_s(tmpinfo.rpname, sizeof(qnode->rpname), qnode->rpname, sizeof(qnode->rpname) - 1);
            securec_check_errval(errval, , LOG);
            errval =
                strncpy_s(tmpinfo.groupname, sizeof(qnode->groupname), qnode->groupname, sizeof(qnode->groupname) - 1);
            securec_check_errval(errval, , LOG);

            srvmgr->freesize -= tmpinfo.memsize;
            respool->active_points += tmpinfo.actpts;

            qnode->try_wakeup = true;

            tmpinfo.isserver = true;

            Assert(tmpinfo.memsize <= tmpinfo.max_memsize && tmpinfo.memsize >= tmpinfo.min_memsize);

            srvmgr->try_wakeup = true;

            /*
             * release the lock while sending message, because the target node
             * may be down, and the server should remove the query of that node.
             */
            rp_lock.UnLock();
            list_lock.UnLock();

            PG_TRY();
            {
                send_result = dywlm_server_send(srvmgr, &tmpinfo);
            }
            PG_CATCH();
            {
                list_lock.Lock();
                rp_lock.Lock();

                srvmgr->try_wakeup = false;
                qnode->try_wakeup = false;
                srvmgr->freesize += tmpinfo.memsize;
                respool->active_points -= tmpinfo.actpts;

                if (respool->active_points <= 0) {
                    respool->active_points = 0;
                }

                if (srvmgr->freesize >= srvmgr->freesize_limit) {
                    srvmgr->freesize = srvmgr->freesize_limit;
                }

                rp_lock.UnLock();
                list_lock.UnLock();
                PG_RE_THROW();
            }
            PG_END_TRY();

            list_lock.Lock();
            rp_lock.Lock();

            next = lnext(cell);

            /* qnode is dirty, only remove it */
            if (qnode->is_dirty) {
                send_result = DYWLM_NO_RECORD;
            }

            if (send_result == DYWLM_SEND_OK) {
                srvmgr->try_wakeup = false;

                respool->waiters = list_delete_cell2(respool->waiters, cell);

                pfree(qnode);

                dywlm_server_write_cache(srvmgr, &tmpinfo.qid, PARCTL_ACTIVE);

                ereport(DEBUG2,
                    (errmsg("send failed for qid [%u, %lu, %ld], update the freesize: %d, send result: %d",
                        tmpinfo.qid.procId,
                        tmpinfo.qid.queryId,
                        tmpinfo.qid.stamp,
                        srvmgr->freesize,
                        send_result)));
            } else {
                Assert(tmpinfo.memsize > 0);

                /* update server amount */
                srvmgr->freesize += tmpinfo.memsize;

                if (srvmgr->freesize >= srvmgr->freesize_limit) {
                    srvmgr->freesize = srvmgr->freesize_limit;
                }

                respool->active_points -= tmpinfo.actpts;

                if (respool->active_points <= 0) {
                    respool->active_points = 0;
                }

                srvmgr->try_wakeup = false;

                ereport(DEBUG2,
                    (errmsg("send failed for qid [%u, %lu, %ld], update the freesize: %d, send result: %d",
                        tmpinfo.qid.procId,
                        tmpinfo.qid.queryId,
                        tmpinfo.qid.stamp,
                        srvmgr->freesize,
                        send_result)));

                /* target node has crashed down and been removed from pgxc_node */
                if (send_result == DYWLM_NO_NODE || send_result == DYWLM_NO_RECORD) {
                    respool->waiters = list_delete_cell2(respool->waiters, cell);

                    pfree(qnode);

                    dywlm_server_write_cache(srvmgr, &tmpinfo.qid, PARCTL_RELEASE);
                } else {
                    /*
                     * query node may be removed by clean command, if not be removed,
                     * reset the tag and move it to the last of list, so it can be
                     * wake up in next time. Else, free memory.
                     */
                    if (!qnode->removed) {
                        qnode->try_wakeup = false;

                        /* move query to the last of list */
                        if (list_length(respool->waiters) != 1) {
                            USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

                            ListCell* newcell = (ListCell*)palloc0_noexcept(sizeof(ListCell));

                            if (newcell == NULL) {
                                rp_lock.UnLock();
                                list_lock.UnLock();
                                ereport(
                                    ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory of current node.")));
                            }

                            lfirst(newcell) = qnode;

                            respool->waiters = list_delete_cell2(respool->waiters, cell);

                            respool->waiters = lappend2(respool->waiters, newcell);
                        }
                    } else {
                        respool->waiters = list_delete_cell2(respool->waiters, cell);

                        pfree(qnode);
                    }
                }
            }

            cell = next;
        }
    }

    rp_lock.UnLock();
    list_lock.UnLock();
}

/*
 * @Description: release resource after execution complete,
 *               and wake up the statements in the resource
 *               pool waiter.
 * @IN rpname: resource name
 * @Return: void
 * @See also:
 */
void dywlm_group_release(ServerDynamicManager* srvmgr, const char* rpname)
{
    /* computation acceleration cluster */
    if (COMP_ACC_CLUSTER) {
        return;
    }

    Assert(StringIsValid(rpname));

    Oid rpoid = get_resource_pool_oid(rpname);

    if (!OidIsValid(rpoid)) {
        return;
    }

    WLMAutoLWLock htab_lock(ResourcePoolHashLock, LW_SHARED);

    htab_lock.AutoLWLockAcquire();

    /* it will alloc a new resource pool memory while not found in htab */
    ResourcePool* respool = GetRespoolFromHTab(rpoid);

    if (respool == NULL) {
        return;
    }

    htab_lock.AutoLWLockRelease();

    dywlm_group_release_internal(srvmgr, respool, 0);

    return;
}

/*
 * @Description: get the sum of all active query memory.
 * @IN void
 * @Return: sum of all active query memory.
 * @See also:
 */
int dywlm_server_get_used_memory(ServerDynamicManager* srvmgr)
{
    int total = 0;

    DynamicWorkloadRecord* record = NULL;

    HASH_SEQ_STATUS hash_seq;

    /* keep lock on global_list_mutex avoiding execute dywlm_server_release_phase2 */
    USE_CONTEXT_LOCK(&srvmgr->global_list_mutex);

    if (srvmgr->try_wakeup) {
        return -1;
    }

    ListCell* cell = list_head(srvmgr->global_waiting_list);
    if (cell != NULL) {
        WLMListNode* node = (WLMListNode*)lfirst(cell);
        if (node != NULL) {
            ListCell* qcell = list_head(node->request_list);
            if (qcell != NULL) {
                record = (DynamicWorkloadRecord*)lfirst(qcell);
            }

            if (record != NULL && record->try_wakeup) {
                return -1;
            }
        }
    }

    USE_AUTO_LWLOCK(WorkloadRecordLock, LW_SHARED);

    hash_seq_init(&hash_seq, srvmgr->global_info_hashtbl);

    while ((record = (DynamicWorkloadRecord*)hash_seq_search(&hash_seq)) != NULL) {
        if (record->is_dirty || record->qtype != PARCTL_ACTIVE) {
            continue;
        }

        total += record->memsize;
    }

    return total;
}

/*
 * @Description: adjust active_points of one resource pool
 * @IN void
 * @Return: void
 * @See also:
 */
static void dywlm_server_adjust_actpts(ServerDynamicManager* srvmgr, ResourcePool* rp)
{
    int count = 0;
    char* rpname = NULL;
    int actpts = 0;
    DynamicWorkloadRecord* record = NULL;
    HASH_SEQ_STATUS hash_seq;

    rpname = get_resource_pool_name(rp->rpoid);
    if (rpname == NULL) {
        return;
    }

    USE_CONTEXT_LOCK(&srvmgr->global_list_mutex);

    if (srvmgr->try_wakeup) {
        return;
    }

    LWLockAcquire(WorkloadRecordLock, LW_SHARED);

    hash_seq_init(&hash_seq, srvmgr->global_info_hashtbl);

    while ((record = (DynamicWorkloadRecord*)hash_seq_search(&hash_seq)) != NULL) {
        ereport(DEBUG3,
            (errmsg("RESERVE RP [%u, %lu, %ld] queue_type: %d",
                record->qid.procId,
                record->qid.queryId,
                record->qid.stamp,
                record->qtype)));

        if (record->is_dirty || record->maxpts <= 0 || record->qtype != PARCTL_ACTIVE) {
            continue;
        }

        if (strcmp(record->rpname, rpname) == 0) {
            count++;
            actpts += record->actpts;
        }
    }

    LWLockRelease(WorkloadRecordLock);

    if (rp->active_points != actpts) {
        ereport(DEBUG3,
            (errmsg("WORKLOAD: adjust resource pool %s active_points "
                    "from %d to %d, total record is %d",
                rpname,
                rp->active_points,
                actpts,
                count)));

        rp->active_points = actpts;
    }

    pfree(rpname);
}

/*
 * @Description: check all resource pool to release
 * @IN void
 * @Return: void
 * @See also:
 */
void dywlm_server_check_resource_pool(void)
{
    ResourcePool* rp = NULL;

    HASH_SEQ_STATUS hseq;

    /* search the resource pool hash table to find statement */
    hash_seq_init(&hseq, g_instance.wlm_cxt->resource_pool_hashtbl);

    WLMAutoLWLock htab_lock(ResourcePoolHashLock, LW_SHARED);

    htab_lock.AutoLWLockAcquire();

    /* search hash to find thread id */
    while ((rp = (ResourcePool*)hash_seq_search(&hseq)) != NULL) {
        WLMNodeGroupInfo* ng = WLMGetNodeGroupFromHTAB(rp->ngroup);

        if (NULL == ng) {
            ereport(LOG,
                (errmsg("WORKLOAD: %s logic cluster doesn't exist in hash table. "
                        "The resource pool oid %u should be deleted!",
                    rp->ngroup,
                    rp->rpoid)));
            continue;
        }

        if (u_sess->attr.attr_resource.enable_dywlm_adjust) {
            dywlm_server_adjust_actpts(&ng->srvmgr, rp);
        }

        if (list_length(rp->waiters) == 0) {
            continue;
        }

        dywlm_group_release_internal(&ng->srvmgr, rp, 0);
    }

    htab_lock.AutoLWLockRelease();
}

/*
 * @Description: get resource pool running count and waiting count
 * @IN rpname: resource pool name
 * @OUT running_count: running count in the resource pool
 * @OUT waiting_count: waiting count in the resource pool
 * @Return: void
 * @See also:
 */
void dywlm_server_get_respool_params(const char* rpname, int* running_count, int* waiting_count)
{
    Assert(running_count != NULL && waiting_count != NULL);

    /* find the node group info based on resource pool */
    int rcount = 0;
    int wcount = 0;

    DynamicWorkloadRecord* record = NULL;

    HASH_SEQ_STATUS hash_seq;

    Oid rpoid = get_resource_pool_oid(rpname);
    if (!OidIsValid(rpoid)) {
        return;
    }

    WLMAutoLWLock htab_lock(ResourcePoolHashLock, LW_SHARED);

    htab_lock.AutoLWLockAcquire();

    /* it will alloc a new resource pool memory while not found in htab */
    ResourcePool* respool = GetRespoolFromHTab(rpoid);

    if (respool == NULL) {
        return;
    }

    htab_lock.AutoLWLockRelease();

    WLMNodeGroupInfo* ng = WLMGetNodeGroupFromHTAB(respool->ngroup);

    if (NULL == ng) {
        return;
    }

    ServerDynamicManager* srvmgr = &ng->srvmgr;

    USE_AUTO_LWLOCK(WorkloadRecordLock, LW_SHARED);

    hash_seq_init(&hash_seq, srvmgr->global_info_hashtbl);

    /* scan all active records from the server cache for current node */
    while ((record = (DynamicWorkloadRecord*)hash_seq_search(&hash_seq)) != NULL) {
        if (strcmp(record->rpname, rpname) != 0) {
            continue;
        }

        /* append it into the records list while qtype is ACTIVE */
        if (record->qtype == PARCTL_ACTIVE) {
            ++rcount;
        } else if (record->qtype == PARCTL_RESPOOL) {
            ++wcount;
        }
    }

    *running_count = rcount;
    *waiting_count = wcount;
}

/*
 * @Description: append queue node to resource pool waiting list
 * @IN respool: resource pool
 * @IN msginfo: message detail info
 * @Return: enqueue type
 * @See also:
 */
EnqueueType dywlm_append_group_list(ResourcePool* respool, const DynamicMessageInfo* msginfo)
{
    ListCell* cell = NULL;
    errno_t errval;

    USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

    DynamicWorkloadRecord* qnode = (DynamicWorkloadRecord*)palloc0_noexcept(sizeof(DynamicWorkloadRecord));

    if (qnode == NULL) {
        goto alloc_failed;
    }

    /* qid of the request, this is the id to wake up the statement */
    dywlm_set_queue_nodes(qnode, msginfo);

    Assert(COMP_ACC_CLUSTER || (qnode->max_memsize > 0 && qnode->min_actpts > 0));

    /* we must save the node name of the request, we must know where the request is from */
    errval = strncpy_s(qnode->nodename, sizeof(qnode->nodename), msginfo->nodename, sizeof(qnode->nodename) - 1);
    securec_check_errval(errval, , LOG);
    errval = strncpy_s(qnode->groupname, sizeof(qnode->groupname), msginfo->groupname, sizeof(qnode->groupname) - 1);
    securec_check_errval(errval, , LOG);

    cell = (ListCell*)palloc0_noexcept(sizeof(ListCell));

    if (cell == NULL) {
        pfree(qnode);
        goto alloc_failed;
    }

    lfirst(cell) = qnode;

    respool->waiters = lappend2(respool->waiters, cell);

    /* append failed */
    if (respool->waiters == NULL) {
        pfree(qnode);
        pfree(cell);
        goto alloc_failed;
    }

    return ENQUEUE_BLOCK;

alloc_failed:
    ereport(DEBUG1,
        (errmsg("ID[%u,%lu,%ld] reserve group statements failed, out of memory.",
            msginfo->qid.procId,
            msginfo->qid.queryId,
            msginfo->qid.stamp)));
    return ENQUEUE_MEMERR;
}

/*
 * @Description: reserve resource in resource pool to execute statement.
 * @IN  srvmgr: the server management for one node group
 * @IN msginfo: message detail info
 * @Return: enqueue type
 * @See also:
 */
EnqueueType dywlm_group_reserve(ServerDynamicManager* srvmgr, DynamicMessageInfo* msginfo)
{
    Assert(msginfo != NULL);

    /* Computation Acceleration cluster */
    if (COMP_ACC_CLUSTER) {
        return ENQUEUE_NONE;
    }

    /* no active statements set, nothing to do */
    if (msginfo->maxpts <= 0) {
        return ENQUEUE_NONE;
    }

    Oid rpoid = get_resource_pool_oid(msginfo->rpname);

    if (!OidIsValid(rpoid)) {
        srvmgr->freesize += msginfo->memsize;

        if (srvmgr->freesize > srvmgr->freesize_limit) {
            srvmgr->freesize = srvmgr->freesize_limit;
        }

        return ENQUEUE_NORESPOOL;
    }

    WLMAutoLWLock htab_lock(ResourcePoolHashLock, LW_SHARED);

    /* grab exclusive lock */
    htab_lock.AutoLWLockAcquire();

    /* it will alloc memory when not found */
    ResourcePool* respool = GetRespoolFromHTab(rpoid);

    if (respool == NULL) {
        ereport(DEBUG2,
            (errmsg("ID[%u,%lu,%ld] cannot get resource pool %u",
                msginfo->qid.procId,
                msginfo->qid.queryId,
                msginfo->qid.stamp,
                rpoid)));
        srvmgr->freesize += msginfo->memsize;
        return ENQUEUE_NORESPOOL;
    }

    htab_lock.AutoLWLockRelease();

    USE_CONTEXT_LOCK(&respool->mutex);

    /* Make sure no statement waiting in waiting list and
     * active points is not out of max points in the resource pool
     * Especially, when the statement came with deduction of memory by maximun plan
     * but finally executes by the minimum plan according to the limit of resource pool,
     * we should make sure it should give the additional part back to freesize
     */
    if (((msginfo->subquery && msginfo->insubquery) || list_length(respool->waiters) == 0) &&
        respool->active_points + msginfo->min_actpts <= msginfo->maxpts) {
        int tmp_mem = msginfo->memsize;

        if (respool->active_points + msginfo->actpts <= msginfo->maxpts) {
            respool->active_points += msginfo->actpts;
        /* regenerate plan with max available resource in rp */
        } else {
            msginfo->actpts = msginfo->maxpts - respool->active_points;
            msginfo->memsize = (long)msginfo->actpts * msginfo->max_memsize / msginfo->max_actpts;
            respool->active_points += msginfo->actpts;
            msginfo->use_max_mem = false;

            /* we need to give the additional part back to freesize */
            srvmgr->freesize += (tmp_mem - msginfo->memsize);
        }

        update_resource_info(srvmgr, &msginfo->qid, msginfo->memsize, msginfo->actpts);

        if (msginfo->memsize > msginfo->max_memsize || msginfo->memsize < msginfo->min_memsize) {
            ereport(DEBUG3,
                (errmsg("ID[%u,%lu,%ld] through dywlm_group_reserve,"
                        "current, max and min memsize is [%d,%d,%d], and freesize is %d now",
                    msginfo->qid.procId,
                    msginfo->qid.queryId,
                    msginfo->qid.stamp,
                    msginfo->memsize,
                    msginfo->max_memsize,
                    msginfo->min_memsize,
                    srvmgr->freesize)));
        }

        return ENQUEUE_NONE;
    }

    /* release the global memory while statement blocking in resource pool */
    srvmgr->freesize += msginfo->memsize;

    if (srvmgr->freesize > srvmgr->freesize_limit) {
        srvmgr->freesize = srvmgr->freesize_limit;
    }

    EnqueueType etype = dywlm_append_group_list(respool, msginfo);

    return etype;
}

/*
 * @Description: server info reset
 * @IN  srvmgr: the server management for one node group
 * @Return: void
 * @See also:
 */
void dywlm_server_reset(ServerDynamicManager* srvmgr)
{
    USE_CONTEXT_LOCK(&srvmgr->global_list_mutex);

    /* reset free size */
    srvmgr->freesize = srvmgr->freesize_limit;

    /* reset global waiting list */
    foreach_cell(cell, srvmgr->global_waiting_list)
    {
        WLMListNode* pnode = (WLMListNode*)lfirst(cell);

        list_free_deep(pnode->request_list);
    }

    list_free_deep(srvmgr->global_waiting_list);

    srvmgr->global_waiting_list = NULL;

    RELEASE_CONTEXT_LOCK();

    USE_AUTO_LWLOCK(ResourcePoolHashLock, LW_SHARED);

    ResourcePool* rp = NULL;

    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, g_instance.wlm_cxt->resource_pool_hashtbl);

    /* reset resource pool waiting list and resource pool info */
    while ((rp = (ResourcePool*)hash_seq_search(&hash_seq)) != NULL) {
        USE_CONTEXT_LOCK(&rp->mutex);

        list_free_deep(rp->waiters);

        rp->waiters = NULL;
        rp->active_points = 0;
        rp->ref_count = 0;
        rp->running_count = 0;
        rp->running_count_simple = 0;
        rp->waiting_count = 0;
    }

    RELEASE_AUTO_LWLOCK();

    /* reset server hash cache */
    dywlm_server_reset_cache(srvmgr);
}

/*
 * @Description: server reserve resource
 * @IN msginfo: message detail info
 * @Return: enqueue type
 * @See also:
 */
bool dywlm_subquery_update(ServerDynamicManager* srvmgr, DynamicMessageInfo* msginfo)
{
    if (!msginfo->subquery) {
        return false;
    }

    USE_AUTO_LWLOCK(WorkloadRecordLock, LW_SHARED);

    DynamicWorkloadRecord* record =
        (DynamicWorkloadRecord*)hash_search(srvmgr->global_info_hashtbl, &msginfo->qid, HASH_FIND, NULL);

    /* release information of last subquery and then update it using new subquery */
    if (record != NULL) {
        /* Save relative data and release lwlock avoiding deadlock */
        int memsize = record->memsize;
        int maxpts = record->maxpts;
        int actpts = record->actpts;
        ParctlType qtype = record->qtype;

        record->qid = msginfo->qid;
        record->memsize = msginfo->memsize;
        record->max_memsize = msginfo->max_memsize;
        record->min_memsize = msginfo->min_memsize;
        record->actpts = msginfo->actpts;
        record->max_actpts = msginfo->max_actpts;
        record->min_actpts = msginfo->min_actpts;
        record->maxpts = msginfo->maxpts;
        record->priority = msginfo->priority;
        record->qtype = PARCTL_NONE;
        record->pnode = NULL;

        if (!(COMP_ACC_CLUSTER || (record->max_memsize > 0 && record->min_actpts > 0))) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid paras.")));
        }

        errno_t errval = strncpy_s(record->rpname, sizeof(record->rpname), msginfo->rpname, sizeof(record->rpname) - 1);
        securec_check_errval(errval, , LOG);
        errval = strncpy_s(record->nodename, sizeof(record->nodename), msginfo->nodename, sizeof(record->nodename) - 1);
        securec_check_errval(errval, , LOG);
        errval =
            strncpy_s(record->groupname, sizeof(record->groupname), msginfo->groupname, sizeof(record->groupname) - 1);
        securec_check_errval(errval, , LOG);

        record->try_wakeup = false;
        record->removed = false;
        record->is_dirty = false;
        record->maybe_deleted = false;

        if (qtype == PARCTL_ACTIVE) {
            dywlm_server_amount_update(srvmgr, memsize, maxpts, -actpts, msginfo->rpname);
        }
        return true;
    }

    return false;
}

/*
 * @Description: server reserve resource
 * @IN msginfo: message detail info
 * @Return: enqueue type
 * @See also:
 */
EnqueueType dywlm_server_reserve(ServerDynamicManager* srvmgr, DynamicMessageInfo* msginfo)
{
    ParctlType qtype = PARCTL_NONE;
    EnqueueType etype = ENQUEUE_NONE;
    WLMContextLock list_lock(&srvmgr->global_list_mutex);

    /* for the query with subqeury in it, we should release the resource of last subqeury */
    msginfo->insubquery = dywlm_subquery_update(srvmgr, msginfo);

    /*
     * We are in the reserve stage to check whether the statement
     * is still in the hash cache, the purpose of doing so is in
     * the reply message stage if a connection error occurs, the
     * client will perform a message retransmission, because the
     * job has been reserved, the server will reserve memory again,
     * we need to check the cache again, to ensure that the amount
     * of memory to release will not be doubled
     */
    if ((qtype = dywlm_record_in_cache(srvmgr, &msginfo->qid)) != PARCTL_NONE) {
        etype = (qtype == PARCTL_ACTIVE) ? ENQUEUE_NONE : ENQUEUE_BLOCK;

        if (!msginfo->isserver) {
            dywlm_server_sync_reply(srvmgr, msginfo);
        }

        return etype;
    }

    /* create workload in cache */
    if (!msginfo->insubquery && dywlm_server_create_record(srvmgr, msginfo, qtype) == -1) {
        if (!msginfo->isserver) {
            dywlm_server_reply(msginfo, qtype, ENQUEUE_MEMERR);
        }

        return ENQUEUE_MEMERR;
    }

    qtype = PARCTL_GLOBAL;

    list_lock.Lock();

    qtype = PARCTL_GLOBAL;

    if (COMP_ACC_CLUSTER) { /* Computation Acceleration cluster */
        etype = dywlm_global_reserve_comp_acc(srvmgr, msginfo);
    } else { /* Customer cluster */
        etype = dywlm_global_reserve(srvmgr, msginfo);

        if (etype == ENQUEUE_NONE) {
            qtype = PARCTL_RESPOOL;

            /* reserve resource in resource pool */
            etype = dywlm_group_reserve(srvmgr, msginfo);
        }
    }

    list_lock.UnLock();

    ereport(DEBUG2,
        (errmsg("reserve memory qid[%u, %lu, %ld], memory size: %d, freesize: %d",
            msginfo->qid.procId,
            msginfo->qid.queryId,
            msginfo->qid.stamp,
            msginfo->memsize,
            srvmgr->freesize)));

    if (etype == ENQUEUE_NONE) {
        qtype = PARCTL_ACTIVE;
    }

    /* write to catalog except error */
    if (!(etype == ENQUEUE_NORESPOOL || etype == ENQUEUE_MEMERR)) {
        dywlm_server_write_cache(srvmgr, &msginfo->qid, qtype);
    }

    /* this is not central node, reply the enqueue type */
    if (!msginfo->isserver) {
        dywlm_server_reply(msginfo, PARCTL_RESERVE, etype);
    }

    return etype;
}

/*
 * @Description: move node to end of the queue
 * @IN  srvmgr: the server management for one node group
 * @IN qid: the qid of node
 * @Return: 0 is success and 1 is failed
 * @See also:
 */
int dywlm_server_append_end(ServerDynamicManager* srvmgr, const Qid* qid)
{
    USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

    ListCell* prev = NULL;

    /* search priority list */
    foreach_cell(cell, srvmgr->global_waiting_list)
    {
        WLMListNode* pnode = (WLMListNode*)lfirst(cell);
        ListCell* qprev = NULL;

        /* search request list */
        foreach_cell(qcell, pnode->request_list)
        {
            DynamicWorkloadRecord* record = (DynamicWorkloadRecord*)lfirst(qcell);

            if (record->equals(qid)) {
                if (!record->removed) {
                    record->try_wakeup = false;

                    /* only one query in the last of list, do not move the query */
                    if (list_length(pnode->request_list) == 1 && lnext(cell) == NULL) {
                        return 0;
                    }

                    /* alloc memory faild, do not move the query */
                    ListCell* newcell = (ListCell*)palloc0_noexcept(sizeof(ListCell));

                    if (newcell == NULL) {
                        return 1;
                    }

                    lfirst(newcell) = record;

                    pnode->request_list = list_delete_cell(pnode->request_list, qcell, qprev);

                    if (list_length(pnode->request_list) != 0) {
                        pnode->request_list = lappend2(pnode->request_list, newcell);
                        return 0;
                    } else {
                        ListCell* tmpcell = lnext(cell);
                        srvmgr->global_waiting_list = list_delete_cell(srvmgr->global_waiting_list, cell, prev);
                        pfree(pnode);

                        pnode = (WLMListNode*)lfirst(tmpcell);
                        pnode->request_list = lappend2(pnode->request_list, newcell);

                        record->pnode = pnode;
                        record->priority = pnode->data;

                        return 0;
                    }
                } else {
                    pnode->request_list = list_delete_ptr(pnode->request_list, record);
                    ereport(DEBUG3,
                        (errmsg(
                            "remove job from server global waiting list as the flag removed is true,qid:[%u, %lu, %ld]",
                            record->qid.procId,
                            record->qid.queryId,
                            record->qid.stamp)));
                    pfree(record);
                    return 0;
                }
            }

            qprev = qcell;
        }

        prev = cell;
    }

    return 0;
}

/*
 * @Description: server release resource phase 2
 * @IN  srvmgr: the server management for one node group
 * @Return: void
 * @See also:
 */
void dywlm_server_release_phase2(ServerDynamicManager* srvmgr)
{
    DynamicMessageInfo tmpinfo;
    EnqueueType etype = ENQUEUE_BLOCK;

    if (COMP_ACC_CLUSTER) {
        srvmgr = &g_instance.wlm_cxt->MyDefaultNodeGroup.srvmgr;
    }

    WLMContextLock list_lock(&srvmgr->global_list_mutex);

    list_lock.Lock();

    /*
     * If there is enough resources, we will always
     * wake up the new job until the lack of resources
     */
    while (true) {
        /* Wake up process: global wake up -> resource pool resource reserve -> job execute */
        errno_t errval = memset_s(&tmpinfo, sizeof(tmpinfo), 0, sizeof(tmpinfo));
        securec_check_errval(errval, , LOG);

        etype = dywlm_global_release(srvmgr, &tmpinfo);

        if (etype == ENQUEUE_BLOCK || IsQidInvalid(&tmpinfo.qid)) {
            return;
        }

        Assert(COMP_ACC_CLUSTER || (tmpinfo.max_memsize > 0 && tmpinfo.min_actpts > 0));

        /* Computation Acceleration cluster does not have to consult ResourcePool */
        if (!COMP_ACC_CLUSTER && StringIsValid(tmpinfo.rpname)) {
            etype = dywlm_group_reserve(srvmgr, &tmpinfo);
        }

        ParctlType qtype = PARCTL_RESPOOL;

        DynamicWorkloadRecord* tmpnode = dywlm_search_node(srvmgr, &tmpinfo.qid);

        if (etype == ENQUEUE_NONE) {
            /* Customer Cluster, memsize != 0 */
            Assert(COMP_ACC_CLUSTER || tmpinfo.memsize != 0);

            if (tmpnode != NULL) {
                tmpnode->try_wakeup = true;
            }

            int send_result = 0;

            tmpinfo.isserver = true;
            tmpinfo.etype = etype;
            tmpinfo.qtype = PARCTL_RELEASE;

            srvmgr->try_wakeup = true;

            /* Customer Cluster */
            if (!COMP_ACC_CLUSTER) {
                ereport(DEBUG1,
                    (errmsg("send qid[%u, %lu, %ld] to wake up, memsize: %d, free_size: %d",
                        tmpinfo.qid.procId,
                        tmpinfo.qid.queryId,
                        tmpinfo.qid.stamp,
                        tmpinfo.memsize,
                        srvmgr->freesize)));
            } else { /* Computation Cluster */
                ereport(DEBUG1,
                    (errmsg("send qid[%u, %lu, %ld] to wake up, active_count: %d, statement_quota: %d",
                        tmpinfo.qid.procId,
                        tmpinfo.qid.queryId,
                        tmpinfo.qid.stamp,
                        srvmgr->active_count,
                        srvmgr->statement_quota)));
            }

            qtype = PARCTL_ACTIVE;

            list_lock.UnLock();

            PG_TRY();
            {
                send_result = dywlm_server_send(srvmgr, &tmpinfo);
            }
            PG_CATCH();
            {
                srvmgr->try_wakeup = false;
                if (tmpnode != NULL) {
                    tmpnode->try_wakeup = false;
                }
                if (COMP_ACC_CLUSTER) {
                    list_lock.Lock();
                    srvmgr->active_count--;
                    srvmgr->statement_quota++;
                    list_lock.UnLock();
                } else {
                    dywlm_server_amount_update(
                        srvmgr, tmpinfo.memsize, tmpinfo.maxpts, -tmpinfo.actpts, tmpinfo.rpname);
                }

                PG_RE_THROW();
            }
            PG_END_TRY();

            list_lock.Lock();

            if (tmpnode != NULL && tmpnode->is_dirty != false) {
                send_result = DYWLM_NO_RECORD;
            }

            list_lock.UnLock();

            /* In the case of failed delivery, we need to recover the resource information of the central node */
            if (send_result != DYWLM_SEND_OK) {
                qtype = PARCTL_RELEASE;

                if (COMP_ACC_CLUSTER) {
                    list_lock.Lock();
                    srvmgr->active_count--;
                    srvmgr->statement_quota++;
                    ereport(DEBUG2,
                        (errmsg("send failed for qid [%u, %lu, %ld], update active_count: %d, statement quota: %d",
                            tmpinfo.qid.procId,
                            tmpinfo.qid.queryId,
                            tmpinfo.qid.stamp,
                            srvmgr->active_count,
                            srvmgr->statement_quota)));
                    list_lock.UnLock();
                } else {
                    dywlm_server_amount_update(
                        srvmgr, tmpinfo.memsize, tmpinfo.maxpts, -tmpinfo.actpts, tmpinfo.rpname);

                    ereport(DEBUG1,
                        (errmsg("send failed for qid [%u, %lu, %ld], update the freesize: %d",
                            tmpinfo.qid.procId,
                            tmpinfo.qid.queryId,
                            tmpinfo.qid.stamp,
                            srvmgr->freesize)));
                }

                list_lock.Lock();

                if (send_result == DYWLM_SEND_FAILED) {
                    if (dywlm_server_append_end(srvmgr, &tmpinfo.qid)) {
                        ereport(DEBUG2,
                            (errmsg("move query[%u, %lu, %ld] to the last of list failed, "
                                    "caused by out of memory.",
                                tmpinfo.qid.procId,
                                tmpinfo.qid.queryId,
                                tmpinfo.qid.stamp)));
                    }
                } else {
                    /* target node has crashed down and been removed */
                    DynamicWorkloadRecord* qnode = dywlm_search_node(srvmgr, &tmpinfo.qid);
                    if (qnode != NULL) {
                        WLMListNode* pnode = qnode->pnode;
                        Assert(pnode != NULL);

                        ereport(DEBUG3,
                            (errmsg("move query[%u, %lu, %ld] to client %s failed, "
                                    "caused by client crash. qtype=%d",
                                tmpinfo.qid.procId,
                                tmpinfo.qid.queryId,
                                tmpinfo.qid.stamp,
                                tmpinfo.nodename,
                                qtype)));

                        pnode->request_list = list_delete_ptr(pnode->request_list, qnode);

                        pfree(qnode);

                        if (list_length(pnode->request_list) == 0) {
                            srvmgr->global_waiting_list = list_delete_ptr(srvmgr->global_waiting_list, pnode);
                            pfree(pnode);
                        }

                        dywlm_server_write_cache(srvmgr, &tmpinfo.qid, qtype);
                    }
                }
            } else {
                /* send success, remove the node from waiting list */
                list_lock.Lock();

                DynamicWorkloadRecord* qnode = dywlm_search_node(srvmgr, &tmpinfo.qid);

                if (qnode != NULL) {
                    WLMListNode* pnode = qnode->pnode;

                    pnode->request_list = list_delete_ptr(pnode->request_list, qnode);

                    pfree(qnode);

                    if (list_length(pnode->request_list) == 0) {
                        srvmgr->global_waiting_list = list_delete_ptr(srvmgr->global_waiting_list, pnode);
                        pfree(pnode);
                    }
                }

                dywlm_server_write_cache(srvmgr, &tmpinfo.qid, qtype);
            }
        } else {
            Assert(tmpnode != NULL);
            WLMListNode* pnode = tmpnode->pnode;

            pnode->request_list = list_delete_ptr(pnode->request_list, tmpnode);

            pfree(tmpnode);

            if (list_length(pnode->request_list) == 0) {
                srvmgr->global_waiting_list = list_delete_ptr(srvmgr->global_waiting_list, pnode);
                pfree(pnode);
            }

            dywlm_server_write_cache(srvmgr, &tmpinfo.qid, qtype);
        }

        srvmgr->try_wakeup = false;
    }

    list_lock.UnLock();
}

/*
 * @Description: server release resource for one node group
 * @IN  srvmgr: the server management for one node group
 * @IN msginfo: message detail info
 * @Return: enqueue type
 * @See also:
 */
EnqueueType dywlm_server_release(ServerDynamicManager* srvmgr, const DynamicMessageInfo* msginfo)
{
    ParctlType qtype;

    /*
     * We are in the reserve stage to check whether the statement
     * is still in the hash cache, the purpose of doing so is in
     * the reply message stage if a connection error occurs, the
     * client will perform a message retransmission, because the
     * job has been reserved, the server will reserve memory again,
     * we need to check the cache again, to ensure that the amount
     * of memory to release will not be doubled
     */
    if ((qtype = dywlm_record_in_cache(srvmgr, &msginfo->qid)) == PARCTL_NONE) {
        return ENQUEUE_NONE;
    }

    ereport(DEBUG3,
        (errmsg("release qid[%u,%lu,%ld], qtype: %d",
            msginfo->qid.procId,
            msginfo->qid.queryId,
            msginfo->qid.stamp,
            qtype)));

    dywlm_server_remove_record(srvmgr, msginfo, qtype);

    if (qtype != PARCTL_ACTIVE) {
        return ENQUEUE_NONE;
    }

    /* wake up next request in respool */
    dywlm_group_release(srvmgr, msginfo->rpname);

    /* wake up next request in global waiting requests. */
    dywlm_server_release_phase2(srvmgr);

    return ENQUEUE_NONE;
}

/*
 * @Description: search queue node in a list
 * @IN list: list pointer
 * @IN msginfo: message detail info
 * @Return: queue node
 * @See also:
 */
DynamicWorkloadRecord* dywlm_search_queue_node(const List* list, const DynamicMessageInfo* msginfo)
{
    /* search priority node with priority */
    WLMListNode* keynode = search_node<WLMListNode, int>(list, &msginfo->priority);

    /* search queue node with qid */
    if (keynode != NULL) {
        return search_node<DynamicWorkloadRecord, Qid>(keynode->request_list, &msginfo->qid);
    }

    return NULL;
}

/*
 * @Description: remove record from global or resource pool list
 * @IN  srvmgr: the server management for one node group
 * @IN msginfo: message info
 * @IN qtype: parallel contraol type
 * @Return: void
 * @See also:
 */
void dywlm_server_remove_record(ServerDynamicManager* srvmgr, const DynamicMessageInfo* msginfo, ParctlType qtype)
{
    switch (qtype) {
        case PARCTL_GLOBAL: {
            /* it's waiting in the global waiting list, find and remove it */
            USE_CONTEXT_LOCK(&srvmgr->global_list_mutex);

            DynamicWorkloadRecord* qnode = dywlm_search_queue_node(srvmgr->global_waiting_list, msginfo);

            if (qnode != NULL) {
                qnode->is_dirty = true;

                if (!qnode->try_wakeup) {
                    WLMListNode* pnode = qnode->pnode;

                    pnode->request_list = list_delete_ptr(pnode->request_list, qnode);

                    /* release memory of the queue node */
                    pfree(qnode);

                    /* delete node from the priority list */
                    if (list_length(pnode->request_list) == 0) {
                        srvmgr->global_waiting_list = list_delete_ptr(srvmgr->global_waiting_list, pnode);

                        pfree(pnode);
                    }
                }
            }

            break;
        }
        case PARCTL_RESPOOL: {
            /* it's waiting in the resource pool waiting list, find and remove it */
            if (msginfo->maxpts > 0 && StringIsValid(msginfo->rpname)) {
                Oid rpoid = get_resource_pool_oid(msginfo->rpname);

                if (!OidIsValid(rpoid)) {
                    break;
                }

                USE_AUTO_LWLOCK(ResourcePoolHashLock, LW_SHARED);

                ResourcePool* respool = GetRespoolFromHTab(rpoid);

                if (respool != NULL) {
                    USE_CONTEXT_LOCK(&respool->mutex);

                    DynamicWorkloadRecord* qnode =
                        search_node<DynamicWorkloadRecord, Qid>(respool->waiters, &msginfo->qid);

                    if (qnode != NULL) {
                        qnode->is_dirty = true;

                        if (!qnode->try_wakeup) {
                            respool->waiters = list_delete_ptr(respool->waiters, qnode);
                            pfree(qnode);
                        }
                    }
                }
            }

            break;
        }
        case PARCTL_ACTIVE: {
            USE_AUTO_LWLOCK(WorkloadRecordLock, LW_EXCLUSIVE);

            DynamicWorkloadRecord* record =
                (DynamicWorkloadRecord*)hash_search(srvmgr->global_info_hashtbl, &msginfo->qid, HASH_FIND, NULL);
            /* it's running, we must compute new free size and acitve points */
            if (record != NULL) {
                /* Save relative data and release lwlock avoiding deadlock */
                int memsize = record->memsize;
                int maxpts = record->maxpts;
                int actpts = record->actpts;

                ereport(DEBUG3,
                    (errmsg("release information: {memsize[%d,%d,%d] actpts[%d,%d,%d]},"
                            "msginfo information:[%d,%d,%d]",
                        memsize,
                        record->max_memsize,
                        record->min_memsize,
                        actpts,
                        record->max_actpts,
                        record->min_actpts,
                        msginfo->memsize,
                        msginfo->maxpts,
                        msginfo->actpts)));

                /* Remove the record before update g_server_dywlm.freesize */
                hash_search(srvmgr->global_info_hashtbl, &msginfo->qid, HASH_REMOVE, NULL);

                RELEASE_AUTO_LWLOCK();

                if (COMP_ACC_CLUSTER) { /* computation cluster */
                    USE_CONTEXT_LOCK(&srvmgr->global_list_mutex);

                    srvmgr->active_count--;
                    srvmgr->statement_quota++;
                } else {
                    /* customer cluster */
                    dywlm_server_amount_update(srvmgr, memsize, maxpts, -actpts, msginfo->rpname);
                }

                ereport(DEBUG3,
                    (errmsg("cancel qid[%u,%lu,%ld], total freesize: %d, memsize: %d",
                        msginfo->qid.procId,
                        msginfo->qid.queryId,
                        msginfo->qid.stamp,
                        srvmgr->freesize,
                        record->memsize)));
            }

            /* Don't need to call dywlm_server_write_cache */
            return;
        }
        default:
            break;
    }

    /* remove the record of the message from the cache */
    dywlm_server_write_cache(srvmgr, &msginfo->qid, PARCTL_RELEASE);
}

/*
 * @Description: server handle cancel request for one node group
 * @IN  srvmgr: the server management for one node group
 * @IN msginfo: message detail info
 * @Return: enqueue type
 * @See also:
 */
EnqueueType dywlm_server_cancel(ServerDynamicManager* srvmgr, DynamicMessageInfo* msginfo)
{
    /* get queue type from the cache */
    ParctlType qtype = dywlm_record_in_cache(srvmgr, &msginfo->qid);

    if (qtype != PARCTL_NONE) {
        dywlm_server_remove_record(srvmgr, msginfo, qtype);
    }
    /* reply the request if not the server */
    if (!msginfo->isserver) {
        dywlm_server_reply(msginfo, PARCTL_CANCEL, ENQUEUE_NONE);
    }

    return ENQUEUE_NONE;
}

/*
 * @Description: handle server record for clean operation
 * @IN  srvmgr: the server management for one node group
 * @IN record: catalog record
 * @Return: void
 * @See also:
 */
void dywlm_server_record_handle(ServerDynamicManager* srvmgr, const DynamicWorkloadRecord* record)
{
    Assert(record != NULL);
    Assert(srvmgr != NULL);

    /* if the record of query is running state, we must compute new memory free size and active points */
    if (record->qtype == PARCTL_ACTIVE) {
        dywlm_server_amount_update(srvmgr, record->memsize, record->maxpts, -record->actpts, record->rpname);
    }

    /* update cache */
    hash_search(srvmgr->global_info_hashtbl, &record->qid, HASH_REMOVE, NULL);
}

/*
 * @Description: server handle clean operation phase 2
 * @IN nodename: node name of the message
 * @Return: void
 * @See also:
 */
void dywlm_server_clean_phase2(ServerDynamicManager* srvmgr, const char* nodename)
{
    USE_AUTO_LWLOCK(WorkloadRecordLock, LW_EXCLUSIVE);

    DynamicWorkloadRecord* record = NULL;
    List* records_list = NULL;
    DynamicWorkloadRecord* tmp_record = NULL;

    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, srvmgr->global_info_hashtbl);

    while ((record = (DynamicWorkloadRecord*)hash_seq_search(&hash_seq)) != NULL) {
        if (strcmp(record->nodename, nodename) != 0) {
            continue;
        }

        if (record->qtype == PARCTL_ACTIVE) {
            /* Save active records to list avoiding deadlock. */
            tmp_record = (DynamicWorkloadRecord*)palloc0_noexcept(sizeof(DynamicWorkloadRecord));

            if (tmp_record != NULL) {
                tmp_record->qid = record->qid;
                tmp_record->memsize = record->memsize;
                tmp_record->max_memsize = record->max_memsize;
                tmp_record->min_memsize = record->min_memsize;
                tmp_record->actpts = record->actpts;
                tmp_record->max_actpts = record->max_actpts;
                tmp_record->min_actpts = record->min_actpts;
                tmp_record->maxpts = record->maxpts;
                tmp_record->priority = record->priority;
                tmp_record->qtype = PARCTL_ACTIVE;
                errno_t errval = strncpy_s(
                    tmp_record->rpname, sizeof(tmp_record->rpname), record->rpname, sizeof(tmp_record->rpname) - 1);
                securec_check_errval(errval, , LOG);
                records_list = lappend(records_list, tmp_record);
            }
        }

        /* update cache */
        hash_search(srvmgr->global_info_hashtbl, &record->qid, HASH_REMOVE, NULL);
    }

    RELEASE_AUTO_LWLOCK();

    /* release memsize and active points. */
    while (list_length(records_list) > 0) {
        record = (DynamicWorkloadRecord*)linitial(records_list);
        dywlm_server_amount_update(srvmgr, record->memsize, record->maxpts, -record->actpts, record->rpname);
        records_list = list_delete_first(records_list);
        pfree(record);
    }

    /* enqueue state is updated, maybe new job can execute, so release the jobs */
    dywlm_server_release_phase2(srvmgr);
}

/*
 * @Description: server handle clean operation for one node group
 * @IN srvmgr: the node group info
 * @IN nodename: node name of the message
 * @Return: void
 * @See also:
 */
EnqueueType dywlm_server_clean_internal(ServerDynamicManager* srvmgr, const char* nodename)
{
    USE_CONTEXT_LOCK(&srvmgr->global_list_mutex);

    ListCell* curr = list_head(srvmgr->global_waiting_list);
    ListCell* next = NULL;
    ListCell* prev = NULL;

    ereport(DEBUG3, (errmsg("clean node name: %s", nodename)));

    while (curr != NULL) {
        next = lnext(curr);

        WLMListNode* pnode = (WLMListNode*)lfirst(curr);

        /* remove the queue node from the global waiting list */
        pnode->request_list = list_cell_clear(pnode->request_list, (char*)nodename, is_same_node);

        if (list_length(pnode->request_list) == 0) {
            srvmgr->global_waiting_list = list_delete_cell(srvmgr->global_waiting_list, curr, prev);
            pfree(pnode);
            curr = next;
            continue;
        }

        prev = curr;
        curr = next;
    }

    RELEASE_CONTEXT_LOCK();

    USE_AUTO_LWLOCK(ResourcePoolHashLock, LW_SHARED);

    ResourcePool* rp = NULL;

    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, g_instance.wlm_cxt->resource_pool_hashtbl);

    /* Get all real time session statistics from the register info hash table. */
    while ((rp = (ResourcePool*)hash_seq_search(&hash_seq)) != NULL) {
        USE_CONTEXT_LOCK(&rp->mutex);

        /* remove the queue node from the resource pool waiting list */
        rp->waiters = list_cell_clear(rp->waiters, (char*)nodename, is_same_node);
    }

    RELEASE_AUTO_LWLOCK();

    /* handle clean operation next phase */
    dywlm_server_clean_phase2(srvmgr, nodename);

    return ENQUEUE_NONE;
}

/*
 * @Description: server handle clean operation for one node group
 * @IN srvmgr: the node group info
 * @IN nodename: node name of the message
 * @Return: void
 * @See also:
 */
void dywlm_server_clean(const char* nodename)
{
    ServerDynamicManager* srvmgr = &g_instance.wlm_cxt->MyDefaultNodeGroup.srvmgr;

    (void)dywlm_server_clean_internal(srvmgr, nodename);

    USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_SHARED);

    WLMNodeGroupInfo* hdata = NULL;

    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.node_group_hashtbl);

    while ((hdata = (WLMNodeGroupInfo*)hash_seq_search(&hash_seq)) != NULL) {
        /* skip unused node group and elastic group */
        if (!hdata->used || pg_strcasecmp(VNG_OPTION_ELASTIC_GROUP, hdata->group_name) == 0) {
            continue;
        }

        srvmgr = &hdata->srvmgr;

        (void)dywlm_server_clean_internal(srvmgr, nodename);
    }
}

/*
 * @Description: update the priority
 * @IN qid: message detail query id
 * @IN priority: new priority of record
 * @Return: void
 * @See also:
 */
void dywlm_server_update_priority(ServerDynamicManager* srvmgr, const Qid* qid, int priority)
{
    if (IsQidInvalid(qid)) {
        return;
    }

    USE_AUTO_LWLOCK(WorkloadRecordLock, LW_EXCLUSIVE);

    DynamicWorkloadRecord* record =
        (DynamicWorkloadRecord*)hash_search(srvmgr->global_info_hashtbl, qid, HASH_FIND, NULL);

    /* update the priority of dynamic workload record */
    if (record != NULL) {
        record->priority = priority;
    }
}

/*
 * @Description: move the qnode to the new priority node list
 * @IN   msginfo: message of the node need be moved
 *      priority: new priority
 * @Return: enqueue type
 * @See also:
 */
EnqueueType dywlm_server_move_node_to_list(ServerDynamicManager* srvmgr, const DynamicMessageInfo* msginfo)
{
    int priority = msginfo->priority;

    USE_CONTEXT_LOCK(&srvmgr->global_list_mutex);

    /* search record from the workload list with qid */
    DynamicWorkloadRecord* dnode = dywlm_search_node(srvmgr, &msginfo->qid);

    if (dnode != NULL) {
        int old_priority = dnode->pnode->data;

        /* prioriy is not changed, nothing to do */
        if (old_priority == priority) {
            return ENQUEUE_NONE;
        }

        USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

        ListCell* lcnode = append_to_list<int, WLMListNode, true>(&srvmgr->global_waiting_list, &priority);

        if (lcnode == NULL) {
            return ENQUEUE_NONE;
        }

        /* remove the node from old list */
        WLMListNode* pnode = dnode->pnode;

        pnode->request_list = list_delete_ptr(pnode->request_list, dnode);

        if (list_length(pnode->request_list) == 0) {
            srvmgr->global_waiting_list = list_delete_ptr(srvmgr->global_waiting_list, pnode);
            pfree(pnode);
        }

        /* move the node to new list */
        pnode = (WLMListNode*)lfirst(lcnode);

        pnode->data = priority;
        pnode->request_list = lappend(pnode->request_list, dnode);

        dnode->pnode = pnode;
        dnode->priority = pnode->data;

        dywlm_server_update_priority(srvmgr, &dnode->qid, priority);
    }

    return ENQUEUE_NONE;
}

/*
 * @Description: make the statements waiting in the global queue jump the priority
 * @IN  qid: the qid of statement which will jump the priority
 * @Return: enqueue type
 * @See also:
 */
EnqueueType dywlm_server_jump_queue(ServerDynamicManager* srvmgr, const DynamicMessageInfo* msginfo)
{
    USE_CONTEXT_LOCK(&srvmgr->global_list_mutex);

    DynamicWorkloadRecord* dnode = dywlm_search_node(srvmgr, &msginfo->qid);

    if (dnode != NULL) {
        int old_priority = dnode->pnode->data;

        ListCell* lcnode = list_head(srvmgr->global_waiting_list);
        WLMListNode* pnode = (WLMListNode*)lfirst(lcnode);

        /*
         * If the statement to change has the highest priority and
         * it's only one in the list, we need nod to anything.
         */
        if (old_priority == pnode->data && list_length(pnode->request_list) == 1) {
            return ENQUEUE_NONE;
        }

        USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

        /* remove the node from old list */
        pnode = dnode->pnode;

        pnode->request_list = list_delete_ptr(pnode->request_list, dnode);

        if (list_length(pnode->request_list) == 0) {
            srvmgr->global_waiting_list = list_delete_ptr(srvmgr->global_waiting_list, pnode);
            pfree(pnode);
        }

        /* move the node to highest priority */
        pnode = (WLMListNode*)lfirst(lcnode);
        pnode->request_list = lcons(dnode, pnode->request_list);

        dnode->pnode = pnode;
        dnode->priority = pnode->data;

        dywlm_server_update_priority(srvmgr, &dnode->qid, pnode->data);
    }

    return ENQUEUE_NONE;
}

/*
 * @Description: get all records from the hash table
 * @OUT num: entry count of the hash table
 * @Return: records list
 * @See also:
 */
DynamicWorkloadRecord* dywlm_server_get_records(ServerDynamicManager* srvmgr, const char* nodename, int* num)
{
    if (!StringIsValid(nodename) || !g_instance.wlm_cxt->dynamic_workload_inited) {
        return NULL;
    }

    USE_AUTO_LWLOCK(WorkloadRecordLock, LW_SHARED);

    /* no records, nothing to do. */
    if ((*num = hash_get_num_entries(srvmgr->global_info_hashtbl)) == 0) {
        return NULL;
    }

    DynamicWorkloadRecord* record = NULL;
    DynamicWorkloadRecord* records = (DynamicWorkloadRecord*)palloc0_noexcept(*num * sizeof(DynamicWorkloadRecord));

    if (records == NULL) {
        *num = 0;
        return records;
    }

    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, srvmgr->global_info_hashtbl);

    int idx = 0;

    /* get all workload records */
    while ((record = (DynamicWorkloadRecord*)hash_seq_search(&hash_seq)) != NULL) {
        if (strcmp(record->nodename, nodename) != 0) {
            continue;
        }

        if (record->is_dirty || record->maybe_deleted) {
            continue;
        }

        errno_t errval = memcpy_s(records + idx, sizeof(DynamicWorkloadRecord), record, sizeof(DynamicWorkloadRecord));
        securec_check_errval(errval, , LOG);

        ++idx;
    }

    *num = idx;

    return records;
}

/*
 * @Description: reply workload records on the server
 * @IN void
 * @Return: void
 * @See also:
 */
void dywlm_server_reply_records(ServerDynamicManager* srvmgr, const char* nodename)
{
    int count = 0;
    StringInfoData retbuf;

    DynamicWorkloadRecord* record_list = dywlm_server_get_records(srvmgr, nodename, &count);

    /* send record count first */
    pq_beginmessage(&retbuf, 'n');
    pq_sendint(&retbuf, count, sizeof(int));
    pq_endmessage(&retbuf);

    for (int i = 0; i < count; ++i) {
        DynamicWorkloadRecord* record = record_list + i;

        /* append workload record into the message */
        pq_beginmessage(&retbuf, 'r');
        pq_sendint(&retbuf, (int)record->qid.procId, sizeof(int));
        pq_sendint64(&retbuf, (uint64)record->qid.queryId);
        pq_sendint64(&retbuf, (int64)record->qid.stamp);
        pq_sendint(&retbuf, (int)record->memsize, sizeof(int));
        pq_sendint(&retbuf, (int)record->max_memsize, sizeof(int));
        pq_sendint(&retbuf, (int)record->min_memsize, sizeof(int));
        pq_sendint(&retbuf, (int)record->actpts, sizeof(int));
        pq_sendint(&retbuf, (int)record->max_actpts, sizeof(int));
        pq_sendint(&retbuf, (int)record->min_actpts, sizeof(int));
        pq_sendint(&retbuf, (int)record->maxpts, sizeof(int));
        pq_sendint(&retbuf, (int)record->priority, sizeof(int));
        pq_sendint(&retbuf, (int)record->qtype, sizeof(int));
        pq_sendstring(&retbuf, record->rpname);
        pq_sendstring(&retbuf, record->nodename);
        pq_sendstring(&retbuf, record->groupname);
        pq_endmessage(&retbuf);

        ereport(DEBUG1,
            (errmsg("SERVER REPLY RECORDS qid [%u, %lu, %ld], qtype: %d",
                record->qid.procId,
                record->qid.queryId,
                record->qid.stamp,
                record->qtype)));
    }

    /* message finished */
    pq_beginmessage(&retbuf, 'f');
    pq_sendint(&retbuf, count, 4);
    pq_endmessage(&retbuf);

    pq_flush();

    if (record_list != NULL) {
        pfree(record_list);
    }

    return;
}

/*
 * @Description: server receive message from the client
 * @IN msg: message string info
 * @Return: void
 * @See also:
 */
void dywlm_server_receive(StringInfo msg)
{
    EnqueueType etype = ENQUEUE_NONE;
    DynamicMessageInfo msginfo;

    errno_t errval = memset_s(&msginfo, sizeof(msginfo), 0, sizeof(msginfo));
    securec_check_errval(errval, , LOG);

    int count = 0;

    /* parse message string to message detail info firstly */
    dywlm_server_parse_message(msg, &msginfo);

    msginfo.use_max_mem = false;

    /* dynamic workload disable, reply non-enqueue */
    if ((!g_instance.wlm_cxt->dynamic_workload_inited ||
            strcmp(msginfo.nodename, g_instance.attr.attr_common.PGXCNodeName) == 0) &&
        msginfo.qtype != PARCTL_SYNC) {
        dywlm_server_reply(&msginfo, msginfo.qtype, ENQUEUE_NONE);
        return;
    }

    /* Get the server manager information */
    ServerDynamicManager* srvmgr = &g_instance.wlm_cxt->MyDefaultNodeGroup.srvmgr;

    if (!COMP_ACC_CLUSTER) {
        WLMNodeGroupInfo* ng = WLMGetNodeGroupFromHTAB(msginfo.groupname);

        if (ng == NULL) {
            etype = ENQUEUE_GROUPERR;
        } else {
            srvmgr = &ng->srvmgr;
        }
    }

    if (etype == ENQUEUE_GROUPERR && (msginfo.qtype != PARCTL_SYNC && msginfo.qtype != PARCTL_RELEASE)) {
        dywlm_server_reply(&msginfo, msginfo.qtype, etype);
        return;
    }

    switch (msginfo.qtype) {
        case PARCTL_RESERVE:
        case PARCTL_TRANSACT:
            /* if server is in recovery state, reply error */
            while (srvmgr->recover || (!COMP_ACC_CLUSTER && srvmgr->freesize_update == 0)) {
                pg_usleep(USECS_PER_SEC);
                ++count;

                /* wait for 5 seconds */
                if (count >= 5) {
                    dywlm_server_reply(&msginfo, msginfo.qtype, ENQUEUE_RECOVERY);
                    return;
                }
            }

            /* reserve resource */
            etype = dywlm_server_reserve(srvmgr, &msginfo);
            break;
        case PARCTL_RELEASE:
            /* if server is in recovery state, wait for recovery */
            while (srvmgr->recover) {
                pg_usleep(USECS_PER_SEC);
                ++count;

                /* wait for 5 seconds */
                if (count >= 5) {
                    return;
                }
            }

            /* release resource */
            etype = dywlm_server_release(srvmgr, &msginfo);
            break;
        case PARCTL_CANCEL:
            /* if server is in recovery state, wait for recovery */
            while (srvmgr->recover) {
                pg_usleep(USECS_PER_SEC);
                ++count;

                /* wait for 5 seconds */
                if (count >= 5) {
                    dywlm_server_reply(&msginfo, msginfo.qtype, ENQUEUE_RECOVERY);
                    return;
                }
            }

            /* cancel job */
            etype = dywlm_server_cancel(srvmgr, &msginfo);
            break;
        case PARCTL_CLEAN:
            /* if server is in recovery state, nothing to clean */
            if (srvmgr->recover) {
                dywlm_server_reply(&msginfo, msginfo.qtype, ENQUEUE_NONE);
                return;
            }

            /* client recover and server clean */
            etype = dywlm_server_clean_internal(srvmgr, msginfo.nodename);

            /* reply result of clean job */
            dywlm_server_reply(&msginfo, msginfo.qtype, etype);
            break;
        case PARCTL_MVNODE:
            if (srvmgr->recover) {
                dywlm_server_reply(&msginfo, msginfo.qtype, ENQUEUE_RECOVERY);
                return;
            }

            etype = dywlm_server_move_node_to_list(srvmgr, &msginfo);

            dywlm_server_reply(&msginfo, PARCTL_JPQUEUE, etype);
            break;
        case PARCTL_JPQUEUE:
            if (srvmgr->recover) {
                dywlm_server_reply(&msginfo, msginfo.qtype, ENQUEUE_RECOVERY);
                return;
            }

            etype = dywlm_server_jump_queue(srvmgr, &msginfo);

            dywlm_server_reply(&msginfo, msginfo.qtype, etype);
            break;
        case PARCTL_SYNC:
            /* reply records to do sync */
            dywlm_server_reply_records(srvmgr, msginfo.nodename);
            break;
        default:
            break;
    }

    ereport(DEBUG1,
        (errmsg("server receive qid [%u, %lu, %ld], qtype[%d], reply etype[%d]",
            msginfo.qid.procId,
            msginfo.qid.queryId,
            msginfo.qid.stamp,
            msginfo.qtype,
            etype)));
}

/*
 * @Description: recover server enqueue list
 * @IN void
 * @Return: void
 * @See also:
 */
void dywlm_server_recover(ServerDynamicManager* srvmgr)
{
    ereport(LOG, (errmsg("server recover start")));

    /* reset resource info on the server */
    dywlm_server_reset(srvmgr);

    /* pull workload records from each node */
    List* records = dywlm_server_pull_records(srvmgr);

    /* We are based on the records in records list to restore */
    while (list_length(records) > 0) {
        DynamicWorkloadRecord* record = (DynamicWorkloadRecord*)linitial(records);

        records = list_delete_first(records);

        DynamicMessageInfo msginfo;

        errno_t errval = memset_s(&msginfo, sizeof(msginfo), 0, sizeof(msginfo));
        securec_check_errval(errval, , LOG);

        /* get message detail info */
        msginfo.qid = record->qid;
        msginfo.memsize = record->memsize;
        msginfo.max_memsize = record->max_memsize;
        msginfo.min_memsize = record->min_memsize;
        msginfo.priority = record->priority;
        msginfo.actpts = record->actpts;
        msginfo.maxpts = record->maxpts;
        msginfo.max_actpts = record->max_actpts;
        msginfo.min_actpts = record->min_actpts;
        msginfo.qtype = record->qtype;

        errval = strncpy_s(msginfo.rpname, sizeof(msginfo.rpname), record->rpname, sizeof(msginfo.rpname) - 1);
        securec_check_errval(errval, , LOG);
        errval = strncpy_s(msginfo.nodename, sizeof(msginfo.nodename), record->nodename, sizeof(msginfo.nodename) - 1);
        securec_check_errval(errval, , LOG);
        errval =
            strncpy_s(msginfo.groupname, sizeof(msginfo.groupname), record->groupname, sizeof(msginfo.groupname) - 1);
        securec_check_errval(errval, , LOG);

        ereport(DEBUG2,
            (errmsg("RECORD [%u, %lu, %ld] queue_type: %d",
                record->qid.procId,
                record->qid.queryId,
                record->qid.stamp,
                record->qtype)));

        pfree(record);

        /* create workload record in the hash cache */
        if (dywlm_server_create_record(srvmgr, &msginfo, msginfo.qtype) != 0) {
            continue;
        }

        switch (msginfo.qtype) {
            case PARCTL_GLOBAL: {
                /* waiting in the global list */
                USE_CONTEXT_LOCK(&srvmgr->global_list_mutex);

                /* append it to the global list */
                msginfo.etype = dywlm_append_global_list(srvmgr, &msginfo);

                break;
            }
            case PARCTL_RESPOOL: {
                Oid rpoid = get_resource_pool_oid(msginfo.rpname);

                if (!OidIsValid(rpoid)) {
                    msginfo.etype = ENQUEUE_ERROR;
                    break;
                }

                WLMAutoLWLock htab_lock(ResourcePoolHashLock, LW_SHARED);
                htab_lock.AutoLWLockAcquire();

                ResourcePool* respool = GetRespoolFromHTab(rpoid);

                htab_lock.AutoLWLockRelease();

                USE_CONTEXT_LOCK(&respool->mutex);

                /* append it to the respool list */
                msginfo.etype = dywlm_append_group_list(respool, &msginfo);

                break;
            }
            case PARCTL_ACTIVE: {
                /* it's running, we have to compute the free memory size */
                dywlm_server_amount_update(srvmgr, -msginfo.memsize, msginfo.actpts, msginfo.actpts, msginfo.rpname);

                break;
            }
            default:
                break;
        }

        /* restore failed, send error to the client */
        if (msginfo.etype == ENQUEUE_ERROR) {
            /* message is from server */
            msginfo.isserver = true;

            ereport(DEBUG2,
                (errmsg("send qid[%u, %lu, %ld] to wake up, free_size: %d",
                    msginfo.qid.procId,
                    msginfo.qid.queryId,
                    msginfo.qid.stamp,
                    srvmgr->freesize)));

            /* send message */
            (void)dywlm_server_send(srvmgr, &msginfo);
        }
    }

    /* enqueue state is updated, maybe new job can execute, so release the jobs
     * dywlm_server_check_resource_pool will wake up respool waiting jobs
     */
    dywlm_server_release_phase2(srvmgr);
    ereport(LOG, (errmsg("server recover end")));
}

/*
 * @Description: recover node state
 * @IN isForce: force to recover node
 * @Return: void
 * @See also:
 */
void dywlm_node_recover_internal(WLMNodeGroupInfo* ng, bool isForce)
{
    if (!IS_PGXC_COORDINATOR || !g_instance.wlm_cxt->dynamic_workload_inited) {
        return;
    }

    WLMNodeGroupInfo* tmpng = ng;
    ServerDynamicManager* srvmgr = &tmpng->srvmgr;
    ClientDynamicManager* climgr = &tmpng->climgr;

    /* set current thread 's node id */
    u_sess->wlm_cxt->wlm_params.qid.procId = u_sess->pgxc_cxt.PGXCNodeId;

    if (isForce) {
        srvmgr->recover = false;
    }

    if (is_pgxc_central_nodename(g_instance.attr.attr_common.PGXCNodeName)) {
        /*
         * For the central node, it needs to do server-level
         * recovery, because the central node will have
         * operations in the implementation, it also need
         * to do a client-level recovery
         */
        if (!srvmgr->recover) {
            srvmgr->recover = true;

            PG_TRY();
            {
                dywlm_server_recover(srvmgr);
            }
            PG_CATCH();
            {
                srvmgr->recover = false;

                PG_RE_THROW();
            }
            PG_END_TRY();
        }

        srvmgr->recover = false;
    } else {
        /* For the non-central node, it only needs to do client-level recovery */
        dywlm_client_recover(climgr);
    }
}

/*
 * @Description: recover node state
 * @IN isForce: force to recover node
 * @Return: void
 * @See also:
 */
void dywlm_node_recover(bool isForce)
{
    if (!IS_PGXC_COORDINATOR || !g_instance.wlm_cxt->dynamic_workload_inited) {
        return;
    }

    dywlm_node_recover_internal(&g_instance.wlm_cxt->MyDefaultNodeGroup, isForce);

    USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_SHARED);

    WLMNodeGroupInfo* hdata = NULL;

    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.node_group_hashtbl);

    while ((hdata = (WLMNodeGroupInfo*)hash_seq_search(&hash_seq)) != NULL) {
        /* skip unused node group and elastic group */
        if (!hdata->used || pg_strcasecmp(VNG_OPTION_ELASTIC_GROUP, hdata->group_name) == 0) {
            continue;
        }

        dywlm_node_recover_internal(hdata, isForce);
    }
}

/*
 * @Description: computation acceleration cluster periodically update quota,
                 and then try to release queued requests
 * @IN Mem_Current_Util: cluster current memory utilization received
 * @Return: void
 * @See also: dywlm_parse_node_info
 */
void dywlm_server_update_statement_quota(ServerDynamicManager* srvmgr, int Mem_Current_Util)
{
    static int quota_update_counter = 0;
    bool release_queued_reqt = true;

    /*
     * check if memory utilization should be updated
     * if not, wait for next update
     */
    if (++quota_update_counter < UPDATE_FREQUENCY) {
        return;
    }

    /* reset memory-utilization-update counter */
    quota_update_counter = 0;

    ereport(DEBUG2, (errmsg("Acceleration Cluster current memory utilization is: %d Percent", Mem_Current_Util)));

    /* lock g_server_dywlm as other threads may be updating it */
    WLMContextLock list_lock(&srvmgr->global_list_mutex);

    list_lock.Lock();

    if (Mem_Current_Util <= MEM_LOWER_LIMIT) { // less than Lower_Limit threshold
        srvmgr->statement_quota = MAX_STATEMENT_QUOTA;
    } else if (Mem_Current_Util >= MEM_UPPER_LIMIT) { // greater than Upper_Limit threshold
        srvmgr->statement_quota = MIN_STATEMENT_QUOTA;
        release_queued_reqt = false;
    } else {  // in the between of Lower_Limit and Upper_limit
        srvmgr->statement_quota = MID_STATEMENT_QUOTA;
    }

    list_lock.UnLock();

    ereport(DEBUG1, (errmsg("Acceleration Cluster current Statement_Quota is: %d", srvmgr->statement_quota)));

    /* try to release queued requests after geting more quota */
    if (release_queued_reqt) {
        dywlm_server_release_phase2(srvmgr);
    }

    return;
}


void dywlm_encap_node_info(DynamicNodeData* nodedata, DynamicNodeData *tmpdata)
{
    /* get total memory size */
    nodedata->total_memory = tmpdata->total_memory;

    /* get the foreign resource pool size */
    nodedata->fp_memsize = tmpdata->fp_memsize;

    /* get the foreign resource pool size */
    nodedata->fp_estmsize = tmpdata->fp_estmsize;

    /* get the foreign resource pool memory percent */
    nodedata->fp_mempct = tmpdata->fp_mempct;

    /*
     * If the used memory exceeds 90% of the total memory,
     * adjust the value of the used memory so that the new
     * query can not be resumed
     */
    int freesize = (tmpdata->total_memory > tmpdata->used_memory) ? (tmpdata->total_memory - tmpdata->used_memory) : 0;

    /*
     * We use the min free memory available on all DNs
     * to book the free memory on the server
     */
    if (nodedata->min_freesize > freesize) {
        nodedata->min_freesize = (freesize >= 0) ? freesize : 0;
    }

    int memutil = 0;

    if (tmpdata->total_memory > 0) {
        memutil = tmpdata->used_memory * FULL_PERCENT / tmpdata->total_memory;
    }

    /* we get max memory util */
    if (nodedata->max_memutil < memutil) {
        nodedata->max_memutil = memutil;
    }

    /* we get min memory util */
    if (nodedata->min_memutil > memutil) {
        nodedata->min_memutil = memutil;
    }

    /* we get max CPU util */
    if (nodedata->cpu_util < tmpdata->cpu_util) {
        nodedata->cpu_util = tmpdata->cpu_util;
    }

    /* we get max CPU count */
    if (nodedata->cpu_count < tmpdata->cpu_count) {
        nodedata->cpu_count = tmpdata->cpu_count;
    }

    /* we get max IO util */
    if (nodedata->io_util < tmpdata->io_util) {
        nodedata->io_util = tmpdata->io_util;
    }

    /* get the max used size of foreign resource pool */
    if (nodedata->fp_usedsize < tmpdata->fp_usedsize) {
        nodedata->fp_usedsize = tmpdata->fp_usedsize;
    }

    /* we get max CPU util */
    if (nodedata->max_cpuutil < tmpdata->cpu_util) {
        nodedata->max_cpuutil = tmpdata->cpu_util;
    }

    /* we get min CPU util */
    if (nodedata->min_cpuutil > tmpdata->cpu_util) {
        nodedata->min_cpuutil = tmpdata->cpu_util;
    }

    /* we get min IO util */
    if (nodedata->max_ioutil < tmpdata->io_util) {
        nodedata->max_ioutil = tmpdata->io_util;
    }

    /* we get min CPU util */
    if (nodedata->min_ioutil > tmpdata->io_util) {
        nodedata->min_ioutil = tmpdata->io_util;
    }
}

/*
 * @Description: parse node info
 * @IN msg: message received
 * @IN suminfo: summary infomation
 * @IN size: not used
 * @Return: void
 * @See also: this is used by remote collector function
 */
void dywlm_parse_node_info(StringInfo msg, void* suminfo, int size)
{
    DynamicNodeData* nodedata = (DynamicNodeData*)suminfo;

    Assert(nodedata != NULL);
    Assert(nodedata->nodedata_htab != NULL);

    DynamicNodeData tmpdata;

    tmpdata.total_memory = (int)pq_getmsgint(msg, 4);
    tmpdata.used_memory = (int)pq_getmsgint(msg, 4);
    tmpdata.phy_totalmem = (int)pq_getmsgint(msg, 4);
    tmpdata.phy_freemem = (int)pq_getmsgint(msg, 4);
    tmpdata.cpu_util = (int)pq_getmsgint(msg, 4);
    tmpdata.cpu_count = (int)pq_getmsgint(msg, 4);
    tmpdata.io_util = (int)pq_getmsgint(msg, 4);
    tmpdata.fp_memsize = (int)pq_getmsgint(msg, 4);
    tmpdata.fp_usedsize = (int)pq_getmsgint(msg, 4);
    tmpdata.fp_estmsize = (int)pq_getmsgint(msg, 4);
    tmpdata.fp_mempct = (int)pq_getmsgint(msg, 4);

    errno_t errval = strncpy_s(tmpdata.host, sizeof(tmpdata.host), pq_getmsgstring(msg), sizeof(tmpdata.host) - 1);
    securec_check_errval(errval, , LOG);

    if (tmpdata.host[0] == '\0') {
        return;
    }

    bool found = false;

    DynamicNodeData* data =
        (DynamicNodeData*)hash_search(nodedata->nodedata_htab, tmpdata.host, HASH_ENTER_NULL, &found);

    /* Get a list of the physical information for each host */
    if (data != NULL) {
        if (!found) {
            errval = strncpy_s(data->host, sizeof(data->host), tmpdata.host, sizeof(data->host) - 1);
            securec_check_errval(errval, , LOG);
            /* save physical memory info */
            data->phy_totalmem = tmpdata.phy_totalmem;
            data->phy_freemem = tmpdata.phy_freemem;

            data->cpu_util = tmpdata.cpu_util;
            data->cpu_count = tmpdata.cpu_count;
            data->io_util = tmpdata.io_util;

            data->fp_usedsize = tmpdata.fp_usedsize;

            /* get physical memory usage rate for each node */
            if (data->phy_freemem > 0 && data->phy_totalmem >= data->phy_freemem) {
                data->phy_usemem_rate = (data->phy_totalmem - data->phy_freemem) * FULL_PERCENT / data->phy_totalmem;
            }

            /* we will get minimum memory usage rate */
            if (nodedata->phy_usemem_rate < data->phy_usemem_rate) {
                nodedata->phy_usemem_rate = data->phy_usemem_rate;
            }
        }

        /* get total used memory */
        data->used_memory += tmpdata.used_memory;
    } else {
        return;
    }

    dywlm_encap_node_info(nodedata, &tmpdata);
}

/*
 * @Description: self wake up CN jobs
 * @IN DIN: the info of the node
 * @Return int: success or failure
 * @See also:
 */
static int dywlm_client_self_wakeup_jobs(DynamicInfoNode* info)
{
    ereport(DEBUG3, (errmsg("------MLW CN SYNC jobs------ CN is trying to self_wakeup jobs...")));

    /* Make sure qid is valid */
    if (info == NULL || IsQidInvalid(&info->qid) || !g_instance.wlm_cxt->dynamic_workload_inited) {
        return 0;
    }

    /* client may receive wake up message twice due to network
     * avoid wake up dirty stmt
     */
    if (info->wakeup || info->is_dirty) {
        return 0;
    }

    WLMContextLock mtx_lock(&info->mutex, false);

    mtx_lock.Lock();

    /* wake up the corresponding thread */
    mtx_lock.ConditionWakeUp(&info->condition);

    info->wakeup = true;

    mtx_lock.UnLock();

    ereport(DEBUG3, (errmsg("record with qid: %lu, wakeup: %d has been woken up.", info->qid.queryId, info->wakeup)));

    return 1;
}

/*
 * @Description: CN sync jobs after collecting
 * @IN CDM: climgr
 * @IN List: jobs_list
 * @IN SDM: srvmgr
 * @Return: void
 * @See also:
 */
static void dywlm_server_sync_jobs(ClientDynamicManager* climgr, List* jobs_list, ServerDynamicManager* srvmgr)
{
    ereport(DEBUG3, (errmsg("------MLW CN SYNC jobs------")));

    /* current client manager */
    ClientDynamicManager* currmgr = climgr;

    bool dn_is_idle = true;
    bool cn_is_pending = true;

    ereport(DEBUG3,
        (errmsg("WLMarbiterMain: dywlm_server_collector_internal: "
                "dywlm_server_sync_jobs: length of jobs_list: %d from NG: %s",
            list_length(jobs_list),
            currmgr->group_name)));

    if (list_length(jobs_list) > 0) {
        ereport(DEBUG3, (errmsg("------MLW CN SYNC jobs------ jobs count %d.", list_length(jobs_list))));

        /* manipulate jobs on CN collected form DN */
        foreach_cell(curr, jobs_list)
        {
            DynamicWorkloadRecord* job = (DynamicWorkloadRecord*)lfirst(curr);

            ereport(DEBUG3,
                (errmsg("collected active job with qid: %lu, qtype(status): %d.", job->qid.queryId, job->qtype)));

            if (job->qtype == PARCTL_ACTIVE) {
                dn_is_idle = false;
                list_free_deep(jobs_list);
                return;
            }
        }
        list_free_deep(jobs_list);
    } else {
        ereport(DEBUG3, (errmsg("------MLW CN SYNC jobs------ no jobs collected.")));
    }

    /* check whether is ccn */
    if (is_pgxc_central_nodename(g_instance.attr.attr_common.PGXCNodeName)) {
        currmgr = srvmgr->climgr;
        ereport(DEBUG3, (errmsg("------MLW CN SYNC jobs------ current node is CCN.")));
    }

    /* manipulate existing records on CN */
    if (hash_get_num_entries(currmgr->dynamic_info_hashtbl) > 0) {
        ereport(DEBUG3,
            (errmsg("------MLW CN SYNC jobs------ "
                    "records count %ld.",
                hash_get_num_entries(currmgr->dynamic_info_hashtbl))));

        USE_AUTO_LWLOCK(WorkloadStatHashLock, LW_SHARED);

        DynamicInfoNode* info = NULL;
        HASH_SEQ_STATUS hash_seq;

        hash_seq_init(&hash_seq, currmgr->dynamic_info_hashtbl);

        /* flag for check all records are invalid */
        bool all_invalid = true;

        while ((info = (DynamicInfoNode*)hash_seq_search(&hash_seq)) != NULL) {
            all_invalid = false;

            ereport(DEBUG3, (errmsg("existing record with qid: %lu, wakeup: %d.", info->qid.queryId, info->wakeup)));

            if (info->wakeup == true) {
                cn_is_pending = false;
                hash_seq_term(&hash_seq);
                return;
            }
        }

        if (all_invalid) {
            cn_is_pending = false;
            ereport(DEBUG3, (errmsg("------MLW CN SYNC jobs------ all records are invalid! NO wakeup.")));
            return;
        }
    } else {
        ereport(DEBUG3, (errmsg("------MLW CN SYNC jobs------ no existing record.")));
        cn_is_pending = false;
        return;
    }

    /* check sync condition */
    if (dn_is_idle && cn_is_pending) {
        if (g_instance.wlm_cxt->dynamic_workload_inited) {
            DynamicInfoNode* ready_for_wakeup = NULL;

            HASH_SEQ_STATUS hash_seq;
            bool has_job_wait = false;

            hash_seq_init(&hash_seq, currmgr->dynamic_info_hashtbl);

            while ((ready_for_wakeup = (DynamicInfoNode*)hash_seq_search(&hash_seq)) != NULL) {
                /* only get the first one */
                if (ready_for_wakeup->wakeup == false) {
                    has_job_wait = true;
                    /* more than 3 times, wakeup jobs */
                    cn_dn_idle_times++;
                    if (cn_dn_idle_times <= 3) {
                        hash_seq_term(&hash_seq);
                        break;
                    }

                    ereport(DEBUG3, (errmsg("MLW CN SYNC jobs: CN needs to wake up jobs on DNs, WAKING...")));

                    int ret = dywlm_client_self_wakeup_jobs(ready_for_wakeup);
                    ereport(LOG, (errmsg("job has been woken up num: %d.", ret)));

                    hash_seq_term(&hash_seq);
                    break;
                }
            }
            /* no job waiting to wake up
             * or have tried to wake up job,reset cn_dn_idle_times
             */
            if (!has_job_wait || cn_dn_idle_times > 3) {
                cn_dn_idle_times = 0;
            }
        }
    } else {
        cn_dn_idle_times = 0;
        ereport(DEBUG3,
            (errmsg("------MLW CN SYNC jobs------ CN NO needs to wake up jobs on DNs: "
                    "dn_is_idle: %d and cn_is_pending: %d.\n",
                dn_is_idle,
                cn_is_pending)));
    }
}

/*
 * @Description: adjust CCN freesize
 * @IN int: min freesize in datanodes
 * @Return: void
 * @See also:
 */
static void dywlm_server_adjust_freesize(ServerDynamicManager* srvmgr, int min_freesize)
{
    int freesize_inc;
    int adjust_value;
    int used_memory;
    int freesize;

    ereport(DEBUG3,
        (errmsg("length of srvmgr->global_info_hashtbl: %ld.", hash_get_num_entries(srvmgr->global_info_hashtbl))));

    if (u_sess->attr.attr_resource.enable_dywlm_adjust && hash_get_num_entries(srvmgr->global_info_hashtbl) > 0) {
        freesize_inc = min_freesize - srvmgr->freesize;
        adjust_value = srvmgr->totalsize * FREESIZE_ADJUST_VALUE / 100;
        used_memory = dywlm_server_get_used_memory(srvmgr);

        ereport(DEBUG3, (errmsg("USED MEM: %d.", used_memory)));

        if (used_memory >= 0) {
            freesize = srvmgr->freesize_limit - used_memory;

            if (freesize > 0 && freesize_inc > adjust_value && srvmgr->freesize != freesize) {
                ereport(DEBUG3,
                    (errmsg("WORKLOAD: adjust global freesize from %d to %d,"
                            "DN min_freesize is %d.",
                        srvmgr->freesize,
                        freesize,
                        min_freesize)));

                gs_lock_test_and_set(&srvmgr->freesize, freesize);
            }

            if (freesize < 0 && srvmgr->freesize != freesize) {
                ereport(DEBUG3,
                    (errmsg("WORKLOAD: adjust global freesize from %d to %d because "
                            "active used memory is %d, DN min_freesize is %d.",
                        srvmgr->freesize,
                        freesize,
                        used_memory,
                        min_freesize)));

                gs_lock_test_and_set(&srvmgr->freesize, freesize);
            }
        }
    }
}

/*
 * @Description: server collector
 * @IN void
 * @Return: void
 * @See also:
 */
void dywlm_server_collector_internal(ServerDynamicManager* srvmgr)
{
    {
        ClientDynamicManager* climgr = srvmgr->climgr;
        ereport(DEBUG3,
            (errmsg("------NodeGroup_VC------"
                    " NG name: %s, srvmgr->global_info_hashtbl size: %ld, climgr->dynamic_info_hashtbl size: %ld;\t"
                    "totalsize: %d, freesize: %d, freesize_limit: %d, rp_memsize: %d, climgr freesize: %d.",
                srvmgr->group_name,
                hash_get_num_entries(srvmgr->global_info_hashtbl),
                hash_get_num_entries(climgr->dynamic_info_hashtbl),
                srvmgr->totalsize,
                srvmgr->freesize,
                srvmgr->freesize_limit,
                srvmgr->rp_memsize,
                climgr->freesize)));
    }

    DynamicNodeData nodedata;
    errno_t errval = memset_s(&nodedata, sizeof(nodedata), 0, sizeof(nodedata));
    securec_check_errval(errval, , LOG);

    nodedata.min_freesize = INT_MAX;
    nodedata.min_memutil = INT_MAX;

    HASHCTL hash_ctl;

    errval = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check_errval(errval, , LOG);

    hash_ctl.keysize = NAMEDATALEN;
    hash_ctl.entrysize = sizeof(DynamicNodeData);
    hash_ctl.alloc = WLMAlloc0NoExcept4Hash;

    /* init a node infomatino list */
    nodedata.nodedata_htab =
        hash_create("node data hash table", WORKLOAD_STAT_HASH_SIZE, &hash_ctl, HASH_ELEM | HASH_ALLOC);

    Oid groupoid = ng_get_group_groupoid(srvmgr->group_name);
    if (groupoid == InvalidOid) {
        nodedata.group_count = get_pgxc_groupmembers(ng_get_installation_group_oid(), &nodedata.group_members);
    } else {
        nodedata.group_count = get_pgxc_groupmembers(groupoid, &nodedata.group_members);
    }

    /* get the client manager based on node group name */
    WLMNodeGroupInfo* ng = WLMGetNodeGroupFromHTAB(srvmgr->group_name);
    if (ng == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("%s logical cluster doesn't exist!", srvmgr->group_name)));
    }

    if (0 == nodedata.group_count && 0 == strcmp(srvmgr->group_name, VNG_OPTION_ELASTIC_GROUP)) {
        ng->total_memory = 0;
        ng->used_memory = 0;
        ng->estimate_memory = 0;
        return;
    }

    /* collect job infomation from each datanode */
    List* jobs_list =
        WLMRemoteJobInfoCollector(g_instance.attr.attr_common.PGXCNodeName, &nodedata, WLM_COLLECT_JOBINFO);

    /* sync jobs based on collected jobs info and check if need wake up a job */
    dywlm_server_sync_jobs(&ng->climgr, jobs_list, srvmgr);

    /* collect memory infomation from each datanode */
    WLMRemoteInfoCollector(g_instance.attr.attr_common.PGXCNodeName,
        &nodedata,
        WLM_COLLECT_PROCINFO,
        sizeof(nodedata),
        dywlm_parse_node_info);

    ereport(DEBUG3,
        (errmsg("------MLW CN collect mem info from DNs------ "
                "datanode count: %d, total memory: %d, used memory: %d, "
                "min freesize: %d, phy totalmem: %d, phy freemem: %d.",
            u_sess->pgxc_cxt.NumDataNodes,
            nodedata.total_memory,
            nodedata.used_memory,
            nodedata.min_freesize,
            nodedata.phy_totalmem,
            nodedata.phy_freemem)));

    ereport(DEBUG3,
        (errmsg("------MLW current CN mem info------ "
                "totalsize: %d, freesize: %d, freesize_inc: %d, "
                "freesize_limit: %d, freesize_update: %d, rp memsize: %d, active count: %d.",
            srvmgr->totalsize,
            srvmgr->freesize,
            srvmgr->freesize_inc,
            srvmgr->freesize_limit,
            srvmgr->freesize_update,
            srvmgr->rp_memsize,
            srvmgr->active_count)));

    hash_remove(nodedata.nodedata_htab);
    pfree(nodedata.group_members);

    if (nodedata.cpu_count > 0) {
        USE_AUTO_LWLOCK(WorkloadIOUtilLock, LW_EXCLUSIVE);

        g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.cpu_util = (double)nodedata.cpu_util;
        g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.cpu_count = nodedata.cpu_count;
    }

    /* Computation Acceleration Cluster */
    if (COMP_ACC_CLUSTER && g_instance.wlm_cxt->dynamic_workload_inited) {
        dywlm_server_update_statement_quota(srvmgr, nodedata.used_memory * 100 / nodedata.total_memory);
        return;
    }

    if (nodedata.total_memory != 0 && srvmgr->freesize_update == 0) {
        gs_lock_test_and_set(&srvmgr->totalsize, nodedata.total_memory);
        gs_lock_test_and_set(&srvmgr->freesize_limit,
            srvmgr->totalsize * u_sess->attr.attr_resource.dynamic_memory_quota / FULL_PERCENT);
        gs_lock_test_and_set(&srvmgr->freesize, srvmgr->freesize_limit);
        gs_lock_test_and_set(&srvmgr->freesize_update, 1);
    }

    /* reset total memory when sighup signal is sent */
    if (nodedata.total_memory != 0) {
        gs_lock_test_and_set(&srvmgr->freesize_limit,
            srvmgr->totalsize * u_sess->attr.attr_resource.dynamic_memory_quota / FULL_PERCENT);
        gs_lock_test_and_set(&srvmgr->freesize_update, 1);
    }

    if (nodedata.min_freesize == INT_MAX) {
        nodedata.min_freesize = 0;
    }

    /* update the node group info */
    gs_lock_test_and_set(&ng->total_memory, nodedata.total_memory);
    gs_lock_test_and_set(&ng->used_memory, nodedata.total_memory - nodedata.min_freesize);
    gs_lock_test_and_set(&ng->estimate_memory, nodedata.estimate_memory);

    /* update the foreign resoruce pool */
    if (ng->foreignrp != NULL) {
        gs_lock_test_and_set(&ng->foreignrp->memsize, nodedata.fp_memsize);
        gs_lock_test_and_set(&ng->foreignrp->memused, nodedata.fp_usedsize);
        gs_lock_test_and_set(&ng->foreignrp->estmsize, nodedata.fp_estmsize);
        gs_lock_test_and_set(&ng->foreignrp->actpct, nodedata.fp_mempct);

        /* update the peak size */
        if (nodedata.fp_usedsize > ng->foreignrp->peakmem) {
            gs_lock_test_and_set(&ng->foreignrp->peakmem, nodedata.fp_usedsize);
        }

        /* update the freesize_limit again */
        int freesize_limit = (nodedata.total_memory - nodedata.fp_memsize) *
                             u_sess->attr.attr_resource.dynamic_memory_quota / FULL_PERCENT;
        int minfreesize_limit =
            (100 - u_sess->attr.attr_resource.dynamic_memory_quota) * nodedata.total_memory / FULL_PERCENT;

        if (freesize_limit < minfreesize_limit) {
            gs_lock_test_and_set(&srvmgr->freesize_limit, minfreesize_limit);
        } else {
            gs_lock_test_and_set(&srvmgr->freesize_limit, freesize_limit);
        }
    }

    ereport(DEBUG3,
        (errmsg("totalsize: %d, freesize_limit: %d, freesize: %d, update: %d",
            srvmgr->totalsize,
            srvmgr->freesize_limit,
            srvmgr->freesize,
            srvmgr->freesize_update)));

    if (nodedata.min_freesize == INT_MAX) {
        return;
    }

    /* reset free memory increment between booking and collector */
    if (nodedata.min_freesize != 0) {
        dywlm_server_adjust_freesize(srvmgr, nodedata.min_freesize);

        gs_lock_test_and_set(&srvmgr->freesize_inc, nodedata.min_freesize - srvmgr->freesize);
        gs_lock_test_and_set(&srvmgr->climgr->freesize, nodedata.min_freesize);
    }

    ereport(DEBUG3,
        (errmsg("------MLW post-adjust CN mem info------ "
                "totalsize: %d, freesize: %d, freesize_inc: %d, freesize_limit: %d, "
                "freesize_update: %d, rp memsize: %d, active count: %d.\n",
            srvmgr->totalsize,
            srvmgr->freesize,
            srvmgr->freesize_inc,
            srvmgr->freesize_limit,
            srvmgr->freesize_update,
            srvmgr->rp_memsize,
            srvmgr->active_count)));

    if (g_instance.wlm_cxt->dynamic_workload_inited) {
        ereport(DEBUG3, (errmsg("------MLW post-adjust CN mem to auto release------\n")));
        /* only work on CCN */
        dywlm_server_release_phase2(srvmgr);
    }
}

/*
 * @Description: server collector, both CNs and CCN, not only CCN
 * @IN void
 * @Return: void
 * @See also:
 */
void dywlm_server_collector(void)
{
    if (IS_PGXC_DATANODE) {
        return;
    }

    ServerDynamicManager* srvmgr = &g_instance.wlm_cxt->MyDefaultNodeGroup.srvmgr;

    dywlm_server_collector_internal(srvmgr);

    USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_SHARED);

    WLMNodeGroupInfo* hdata = NULL;

    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.node_group_hashtbl);

    while ((hdata = (WLMNodeGroupInfo*)hash_seq_search(&hash_seq)) != NULL) {
        if (!hdata->used) {
            continue;
        }

        srvmgr = &hdata->srvmgr;

        if (hdata->foreignrp != NULL) {
            ereport(DEBUG3,
                (errmsg("------NodeGroup_VC------ foreignrp: "
                        "ng_used_memory: %d, ng_total_memory: %d, memsize: %d, memused: %d, mempct: %d, estmsize: %d.",
                    hdata->used_memory,
                    hdata->total_memory,
                    hdata->foreignrp->memsize,
                    hdata->foreignrp->memused,
                    hdata->foreignrp->mempct,
                    hdata->foreignrp->estmsize)));
        }

        dywlm_server_collector_internal(srvmgr);
    }
    g_instance.wlm_cxt->dynamic_memory_collected = true;
}

/*
 * @Description: collect datanode resource info
 * @IN void
 * @Return: node process info data
 * @See also:
 */
DynamicNodeData* dywlm_get_resource_info(ServerDynamicManager* srvmgr)
{
    if (!g_instance.wlm_cxt->dynamic_workload_inited) {
        return NULL;
    }

    if (IS_PGXC_DATANODE) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("This function could not execute on datanode.")));
    }

    DynamicNodeData* nodedata = (DynamicNodeData*)palloc0_noexcept(sizeof(DynamicNodeData));

    if (nodedata == NULL) {
        ereport(NOTICE, (errmsg("alloc memory failed")));
        return NULL;
    }

    nodedata->min_freesize = INT_MAX;
    nodedata->min_memutil = INT_MAX;
    nodedata->min_cpuutil = INT_MAX;
    nodedata->min_ioutil = INT_MAX;

    HASHCTL hash_ctl;

    errno_t errval = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check_errval(errval, , LOG);

    hash_ctl.keysize = NAMEDATALEN;
    hash_ctl.entrysize = sizeof(DynamicNodeData);
    hash_ctl.alloc = WLMAlloc0NoExcept4Hash;

    /* init a node infomatino list */
    nodedata->nodedata_htab =
        hash_create("node data hash table", WORKLOAD_STAT_HASH_SIZE, &hash_ctl, HASH_ELEM | HASH_ALLOC);

    Oid groupoid = ng_get_group_groupoid(srvmgr->group_name);
    if (groupoid == InvalidOid) {
        nodedata->group_count = get_pgxc_groupmembers(ng_get_installation_group_oid(), &nodedata->group_members);
    } else {
        nodedata->group_count = get_pgxc_groupmembers(groupoid, &nodedata->group_members);
    }

    /* collect memory infomation from each datanode */
    WLMRemoteInfoCollector(g_instance.attr.attr_common.PGXCNodeName,
        nodedata,
        WLM_COLLECT_PROCINFO,
        sizeof(DynamicNodeData),
        dywlm_parse_node_info);

    hash_remove(nodedata->nodedata_htab);
    pfree(nodedata->group_members);

    if (nodedata->min_freesize == INT_MAX) {
        nodedata->min_freesize = -1;
    }

    if (nodedata->min_memutil == INT_MAX) {
        nodedata->min_memutil = -1;
    }

    if (nodedata->min_cpuutil == INT_MAX) {
        nodedata->min_cpuutil = -1;
    }

    if (nodedata->min_ioutil == INT_MAX) {
        nodedata->min_ioutil = -1;
    }

    return nodedata;
}

/*
 * @Description: server initialize
 * @IN msg: void
 * @Return: void
 * @See also:
 */
void dywlm_server_init(WLMNodeGroupInfo* info)
{
    ServerDynamicManager* srvmgr = &info->srvmgr;

    srvmgr->totalsize = 0;

    srvmgr->freesize = 0;

    srvmgr->freesize_inc = 0;

    srvmgr->freesize_update = 0;

    srvmgr->freesize_limit = 0;

    srvmgr->rp_memsize = 0;

    srvmgr->active_count = 0;

    srvmgr->statement_quota = MID_STATEMENT_QUOTA;

    srvmgr->recover = false;

    srvmgr->group_name = info->group_name;

    srvmgr->climgr = &info->climgr;

    srvmgr->global_waiting_list = NULL;

    srvmgr->try_wakeup = false;

    HASHCTL hash_ctl;

    errno_t errval = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check_errval(errval, , LOG);

    hash_ctl.keysize = sizeof(Qid);
    hash_ctl.hcxt = g_instance.wlm_cxt->workload_manager_mcxt; /* use workload manager memory context */
    hash_ctl.entrysize = sizeof(DynamicWorkloadRecord);
    hash_ctl.hash = WLMHashCode;
    hash_ctl.match = WLMHashMatch;
    hash_ctl.alloc = WLMAlloc0NoExcept4Hash;
    hash_ctl.dealloc = pfree;

    srvmgr->global_info_hashtbl = hash_create("global info hash table",
        WORKLOAD_STAT_HASH_SIZE,
        &hash_ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_COMPARE | HASH_ALLOC | HASH_DEALLOC);

    (void)pthread_mutex_init(&srvmgr->global_list_mutex, NULL);
}

/*
 * @Description: get active workload records from all node except itself
 * @IN void
 * @Return: records list
 * @See also:
 */
List* dywlm_server_pull_client_active_records(ServerDynamicManager* srvmgr)
{
    List* coordlist = NULL;
    List* connlist = NULL;
    List* records_list = NULL;

    PGXCNodeAllHandles* pgxc_handles = NULL;

    DynamicMessageInfo sendinfo;

    errno_t errval = memset_s(&sendinfo, sizeof(sendinfo), 0, sizeof(sendinfo));
    securec_check_errval(errval, , LOG);

    sendinfo.qtype = PARCTL_SYNC;
    sendinfo.isserver = true;
    errval =
        strncpy_s(sendinfo.groupname, sizeof(sendinfo.groupname), srvmgr->group_name, sizeof(sendinfo.groupname) - 1);
    securec_check_errval(errval, , LOG);
    struct timeval timeout = {120, 0};

    /* get all coordinator index except current node */
    coordlist = PgxcGetCoordlist(true);

    if (coordlist == NULL) {
        return NULL;
    }

    PG_TRY();
    {
        pgxc_handles = get_handles(NULL, coordlist, true);
    }
    PG_CATCH();
    {
        release_pgxc_handles(pgxc_handles);
        pgxc_handles = NULL;
        list_free_ext(coordlist);

        PG_RE_THROW();
    }
    PG_END_TRY();

    int connidx = 0;

    /* pull records from each coordinator */
    foreach_cell(item, coordlist)
    {
        int nodeid = lfirst_int(item);

        PGXCNodeHandle* cn_handle = pgxc_handles->coord_handles[connidx];

        if (cn_handle == NULL) {
            continue;
        }

        /* get node name for remote checking */
        PgxcGetNodeName(nodeid, sendinfo.nodename, sizeof(sendinfo.nodename));

        /* If a node has a connection error, we have to temporarily ignore the node */
        if (pgxc_node_dywlm_send_params(cn_handle, &sendinfo) == 0) {
            connlist = lappend_int(connlist, connidx);
        }

        ++connidx;
    }

    while (list_length(connlist) > 0) {
        connidx = linitial_int(connlist);

        connlist = list_delete_first(connlist);

        PGXCNodeHandle* cn_handle = pgxc_handles->coord_handles[connidx];

        Assert(cn_handle != NULL);

        bool hasError = false;
        bool isFinished = false;

        /* receive workload records from each node */
        for (;;) {
            if (pgxc_node_receive(1, &cn_handle, &timeout)) {
                ereport(LOG,
                    (errmsg("%s:%d recv fail", __FUNCTION__, __LINE__)));
                break;
            }

            char* msg = NULL;
            int len, retcode;
            char msg_type;

            msg_type = get_message(cn_handle, &len, &msg);

            /* check message type */
            switch (msg_type) {
                case '\0': /* message is not completed */
                case 'E':  /* cn is down or crashed */
                    hasError = true;
                    break;
                case 'Z':
                    if (hasError) {
                        isFinished = true;
                        ereport(LOG, (errmsg("pull records failed from node %d", connidx)));
                    }
                    break;
                case 'r': /* get workload record */
                {
                    DynamicWorkloadRecord* record =
                        (DynamicWorkloadRecord*)palloc0_noexcept(sizeof(DynamicWorkloadRecord));

                    if (record == NULL) {
                        list_free_ext(coordlist);
                        release_pgxc_handles(pgxc_handles);
                        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory of current node.")));
                    }

                    StringInfoData input_msg;
                    initStringInfo(&input_msg);

                    appendBinaryStringInfo(&input_msg, msg, len);

                    /* parse message to get record */
                    record->qid.procId = (Oid)pq_getmsgint(&input_msg, 4);
                    record->qid.queryId = (uint64)pq_getmsgint64(&input_msg);
                    record->qid.stamp = (TimestampTz)pq_getmsgint64(&input_msg);
                    record->memsize = (int)pq_getmsgint(&input_msg, 4);
                    record->max_memsize = (int)pq_getmsgint(&input_msg, 4);
                    record->min_memsize = (int)pq_getmsgint(&input_msg, 4);
                    record->actpts = (int)pq_getmsgint(&input_msg, 4);
                    record->max_actpts = (int)pq_getmsgint(&input_msg, 4);
                    record->min_actpts = (int)pq_getmsgint(&input_msg, 4);
                    record->maxpts = (int)pq_getmsgint(&input_msg, 4);
                    record->priority = (int)pq_getmsgint(&input_msg, 4);
                    record->qtype = (ParctlType)pq_getmsgint(&input_msg, 4);

                    errval = strncpy_s(record->rpname,
                        sizeof(record->rpname),
                        pq_getmsgstring(&input_msg),
                        sizeof(record->rpname) - 1);
                    securec_check_errval(errval, , LOG);
                    errval = strncpy_s(record->nodename,
                        sizeof(record->nodename),
                        pq_getmsgstring(&input_msg),
                        sizeof(record->nodename) - 1);
                    securec_check_errval(errval, , LOG);
                    errval = strncpy_s(record->groupname,
                        sizeof(record->groupname),
                        pq_getmsgstring(&input_msg),
                        sizeof(record->groupname) - 1);
                    securec_check_errval(errval, , LOG);

                    pq_getmsgend(&input_msg);

                    pfree(input_msg.data);

                    /* append it into the records list while qtype is ACTIVE */
                    records_list = lappend(records_list, record);

                    break;
                }
                case 'f': /* message is completed */
                {
                    errno_t errval = memcpy_s(&retcode, sizeof(int), msg, 4);
                    securec_check_errval(errval, , LOG);
                    retcode = (int)ntohl(retcode);

                    isFinished = true;

                    break;
                }
                case 'A': /* NotificationResponse */
                case 'S': /* SetCommandComplete */
                {
                    /*
                     * Ignore these to prevent multiple messages, one from
                     * each node. Coordinator will send on for DDL anyway
                     */
                    break;
                }
                default: {
                    ereport(ERROR, (errmsg("Unknow message type '%c' for client records.", msg_type)));;
                }
            }

            /* error occurred or finished, try next node */
            if (isFinished)
                break;
        }

        cn_handle->state = DN_CONNECTION_STATE_IDLE;
        if (!isFinished) {
            ereport(ERROR,
                (errcode(ERRCODE_FETCH_DATA_FAILED),
                    errmsg("%s", (cn_handle->error == NULL) ? "Receive Data Failed." : cn_handle->error)));
        }
    }

    list_free_ext(coordlist);
    release_pgxc_handles(pgxc_handles);

    return records_list;
}

/*
 * @Description: get active workload record from current node
 * @IN void
 * @Return: records list
 * @See also:
 */
List* dywlm_server_pull_server_active_records(ServerDynamicManager* srvmgr, List* record_list)
{
    ClientDynamicManager* climgr = srvmgr->climgr;

    USE_AUTO_LWLOCK(WorkloadStatHashLock, LW_SHARED);

    DynamicInfoNode* info = NULL;

    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, climgr->dynamic_info_hashtbl);

    /* scan all active records from the server cache for current node */
    while ((info = (DynamicInfoNode*)hash_seq_search(&hash_seq)) != NULL) {
        if (info->is_dirty) {
            continue;
        }

        DynamicWorkloadRecord* record = (DynamicWorkloadRecord*)palloc0_noexcept(sizeof(DynamicWorkloadRecord));

        if (record == NULL) {
            hash_seq_term(&hash_seq);
            RELEASE_AUTO_LWLOCK();
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory of current node.")));
        }

        /* get record */
        record->qid = info->qid;
        record->memsize = info->memsize;
        record->max_memsize = info->max_memsize;
        record->min_memsize = info->min_memsize;
        record->actpts = info->actpts;
        record->max_actpts = info->max_actpts;
        record->min_actpts = info->min_actpts;
        record->maxpts = info->maxpts;
        record->priority = info->priority;
        record->qtype = info->wakeup ? PARCTL_ACTIVE : PARCTL_GLOBAL;

        errno_t errval = strncpy_s(record->rpname, sizeof(record->rpname), info->rpname, sizeof(record->rpname) - 1);
        securec_check_errval(errval, , LOG);
        errval = strncpy_s(record->nodename,
            sizeof(record->nodename),
            g_instance.attr.attr_common.PGXCNodeName,
            sizeof(record->nodename) - 1);
        securec_check_errval(errval, , LOG);
        errval = strncpy_s(record->groupname, sizeof(record->groupname), info->ngroup, sizeof(record->groupname) - 1);
        securec_check_errval(errval, , LOG);

        /* append it into the records list while qtype is ACTIVE */
        record_list = lappend(record_list, record);
    }

    return record_list;
}

/*
 * @Description: get dynamic workload record from all node include itself
 * @IN server manager
 * @Return: records list
 * @See also:
 */
List* dywlm_server_pull_records(ServerDynamicManager* srvmgr)
{
    List* records_list = dywlm_server_pull_client_active_records(srvmgr);

    records_list = dywlm_server_pull_server_active_records(srvmgr, records_list);

    return records_list;
}

/*
 * @Description: find record in the list and remove it
 * @IN list: list of records
 *     qid : qid of record
 * @OUT found: 0 - record is not in list
 *             1 - find record and remove record
 * @Return: list of result
 * @See also:
 */
List* list_find_and_remove(List* list, Qid* qid, bool* found)
{
    ListCell* cell = NULL;
    ListCell* prev = NULL;

    foreach (cell, list) {
        DynamicWorkloadRecord* record = (DynamicWorkloadRecord*)lfirst(cell);

        if (record->equals(qid)) {
            *found = true;
            return list_delete_cell(list, cell, prev);
        }

        prev = cell;
    }

    *found = false;
    return list;
}

/*
 * @Description: get all dirty records in cache
 * @IN void
 * @Return: list of result
 * @See also:
 */
List* dywlm_server_get_dirty_records(ServerDynamicManager* srvmgr)
{
    List* list = NULL;

    DynamicWorkloadRecord* record = NULL;

    HASH_SEQ_STATUS hash_seq;

    USE_AUTO_LWLOCK(WorkloadRecordLock, LW_EXCLUSIVE);

    hash_seq_init(&hash_seq, srvmgr->global_info_hashtbl);

    while ((record = (DynamicWorkloadRecord*)hash_seq_search(&hash_seq)) != NULL) {
        if (!record->is_dirty) {
            continue;
        }

        DynamicMessageInfo* info = (DynamicMessageInfo*)palloc0_noexcept(sizeof(DynamicMessageInfo));

        if (info == NULL) {
            continue;
        }

        info->qid = record->qid;
        info->memsize = record->memsize;
        info->max_memsize = record->max_memsize;
        info->min_memsize = record->min_memsize;
        info->actpts = record->actpts;
        info->max_actpts = record->max_actpts;
        info->min_actpts = record->min_actpts;
        info->maxpts = record->maxpts;
        info->priority = record->priority;
        info->qtype = record->qtype;

        errno_t errval = strncpy_s(info->rpname, sizeof(record->rpname), record->rpname, sizeof(record->rpname) - 1);
        securec_check_errval(errval, , LOG);
        errval = strncpy_s(info->nodename, sizeof(record->nodename), record->nodename, sizeof(record->nodename) - 1);
        securec_check_errval(errval, , LOG);
        errval = strncpy_s(info->groupname, sizeof(info->groupname), record->groupname, sizeof(info->groupname) - 1);
        securec_check_errval(errval, , LOG);

        /* append it into the records list while qtype is ACTIVE */
        list = lappend(list, info);
    }

    return list;
}

/*
 * @Description: set DynamicMessageInfo detail from DynamicWorkloadRecord
 */
void dywlm_set_message_detail_info(DynamicMessageInfo* msginfo, DynamicWorkloadRecord* record)
{
    msginfo->qid = record->qid;
    msginfo->memsize = record->memsize;
    msginfo->max_memsize = record->max_memsize;
    msginfo->min_memsize = record->min_memsize;
    msginfo->priority = record->priority;
    msginfo->actpts = record->actpts;
    msginfo->max_actpts = record->max_actpts;
    msginfo->min_actpts = record->min_actpts;
    msginfo->maxpts = record->maxpts;
    msginfo->qtype = record->qtype;
}

/*
 * @Description: Synchronize workload records between client and server
 * @IN: void
 * @Return: void
 * @See also:
 */
void dywlm_server_sync_records_internal(ServerDynamicManager* srvmgr)
{
    List* records_list = dywlm_server_pull_records(srvmgr);

    AssignHTabInfoDirtyWithLock<DynamicWorkloadRecord>(srvmgr->global_info_hashtbl, WorkloadRecordLock);

    ereport(DEBUG3,
        (errmsg("------MLW current CCN record------ num of records: %ld.",
            hash_get_num_entries(srvmgr->global_info_hashtbl))));
    ereport(DEBUG3, (errmsg("------MLW collect job info from CNs------ num of jobs: %d.", list_length(records_list))));

    foreach_cell(curr, records_list)
    {
        DynamicWorkloadRecord* record = (DynamicWorkloadRecord*)lfirst(curr);

        ereport(DEBUG3,
            (errmsg("------MLW collect job info from CNs ------ "
                    "qid: %lu, pid: %u, qtype: %d, memsize: %d, actpts: %d, is dirty: %d.",
                record->qid.queryId,
                record->qid.procId,
                record->qtype,
                record->memsize,
                record->actpts,
                record->is_dirty)));

        if (IsQidInvalid(&record->qid)) {
            continue;
        }

        ParctlType qtype = dywlm_record_in_cache(srvmgr, &record->qid);
        if (qtype == PARCTL_NONE) {
            DynamicMessageInfo msginfo;

            errno_t errval = memset_s(&msginfo, sizeof(msginfo), 0, sizeof(msginfo));
            securec_check_errval(errval, , LOG);

            /* get message detail info */
            dywlm_set_message_detail_info(&msginfo, record);

            errval = strncpy_s(msginfo.rpname, sizeof(msginfo.rpname), record->rpname, sizeof(msginfo.rpname) - 1);
            securec_check_errval(errval, , LOG);
            errval =
                strncpy_s(msginfo.nodename, sizeof(msginfo.nodename), record->nodename, sizeof(msginfo.nodename) - 1);
            securec_check_errval(errval, , LOG);
            errval = strncpy_s(
                msginfo.groupname, sizeof(msginfo.groupname), record->groupname, sizeof(msginfo.groupname) - 1);
            securec_check_errval(errval, , LOG);

            ereport(DEBUG3,
                (errmsg("add job info into server records, qid:[%u, %lu, %ld] queue_type: %d",
                    record->qid.procId,
                    record->qid.queryId,
                    record->qid.stamp,
                    record->qtype)));

            if (record->qtype == PARCTL_ACTIVE) {
                /* create workload record in the hash cache */
                if (dywlm_server_create_record(srvmgr, &msginfo, msginfo.qtype) != 0) {
                    continue;
                }

                dywlm_server_amount_update(srvmgr, -record->memsize, record->maxpts, record->actpts, record->rpname);
            } else {
                /* waiting in the global list */
                USE_CONTEXT_LOCK(&srvmgr->global_list_mutex);

                /* append it to the global list */
                if (dywlm_append_global_list(srvmgr, &msginfo) == ENQUEUE_BLOCK) {
                    RELEASE_CONTEXT_LOCK();

                    /* create workload record in the hash cache */
                    (void)dywlm_server_create_record(srvmgr, &msginfo, msginfo.qtype);
                }
            }
        } else if (qtype == PARCTL_ACTIVE) {
            if (u_sess->attr.attr_resource.enable_dywlm_adjust &&
                (record->qtype == PARCTL_GLOBAL || record->qtype == PARCTL_RESPOOL)) {
                /* The wakeup status (RESERVE) in CN (dywlm_client) is different from
                 * status (ACTIVE) in CCN(dywlm_server). We need to wake up the CN again.
                 */
                DynamicMessageInfo msginfo;

                errno_t errval = memset_s(&msginfo, sizeof(msginfo), 0, sizeof(msginfo));
                securec_check_errval(errval, , LOG);

                /* get message detail info */
                dywlm_set_message_detail_info(&msginfo, record);
                msginfo.isserver = true;

                errval = strncpy_s(msginfo.rpname, sizeof(msginfo.rpname), record->rpname, sizeof(msginfo.rpname) - 1);
                securec_check_errval(errval, , LOG);
                errval = strncpy_s(
                    msginfo.nodename, sizeof(msginfo.nodename), record->nodename, sizeof(msginfo.nodename) - 1);
                securec_check_errval(errval, , LOG);
                errval = strncpy_s(
                    msginfo.groupname, sizeof(msginfo.groupname), record->groupname, sizeof(msginfo.groupname) - 1);
                securec_check_errval(errval, , LOG);

                ereport(DEBUG3,
                    (errmsg("wakeup the job again as status in CN is different from CCN: [%u,%lu,%ld]",
                        msginfo.qid.procId,
                        msginfo.qid.queryId,
                        msginfo.qid.stamp)));

                (void)dywlm_server_send(srvmgr, &msginfo);
            }
            dywlm_server_write_cache(srvmgr, &record->qid, PARCTL_RESERVE);
        } else if (record->qtype == PARCTL_ACTIVE) {
            /* The wakeup status (ACTIVE) in CN (dywlm_client) is different from
             * status (RESERVE) in CCN(dywlm_server). We need to release query on CCN .
             */
            ereport(DEBUG3,
                (errmsg("set server job status active as job is wakeup by CN, qid:[%u, %lu, %ld]",
                    record->qid.procId,
                    record->qid.queryId,
                    record->qid.stamp)));

            WLMAutoLWLock record_lock(WorkloadRecordLock, LW_EXCLUSIVE);
            record_lock.AutoLWLockAcquire();

            DynamicWorkloadRecord* record_srv =
                (DynamicWorkloadRecord*)hash_search(srvmgr->global_info_hashtbl, &record->qid, HASH_FIND, NULL);

            record_srv->qtype = record->qtype;
            record_srv->try_wakeup = true;
            record_srv->is_dirty = false;

            record_lock.AutoLWLockRelease();

            Assert(StringIsValid(record->rpname));

            dywlm_server_amount_update(srvmgr, -record->memsize, record->maxpts, record->actpts, record->rpname);

            /* reomve record from respool and global list */
            DynamicMessageInfo msginfo;

            errno_t errval = memset_s(&msginfo, sizeof(msginfo), 0, sizeof(msginfo));
            securec_check_errval(errval, , LOG);

            msginfo.qid = record->qid;
            msginfo.priority = record->priority;
            msginfo.maxpts = record->maxpts;
            errval = strncpy_s(msginfo.rpname, sizeof(msginfo.rpname), record->rpname, sizeof(msginfo.rpname) - 1);
            securec_check_errval(errval, , LOG);

            dywlm_server_remove_record(srvmgr, &msginfo, PARCTL_RESPOOL);
            dywlm_server_remove_record(srvmgr, &msginfo, PARCTL_GLOBAL);
        } else {
            ereport(DEBUG3,
                (errmsg("sync reserve job, qid:[%u, %lu, %ld] queue_type: %d",
                    record->qid.procId,
                    record->qid.queryId,
                    record->qid.stamp,
                    record->qtype)));

            dywlm_server_write_cache(srvmgr, &record->qid, PARCTL_RESERVE);
        }
    }

    list_free_deep(records_list);

    records_list = dywlm_server_get_dirty_records(srvmgr);

    if (list_length(records_list) > 0) {
        ereport(LOG, (errmsg("dirty record count %d.", list_length(records_list))));
    }

    /*
     * remove all dirty records that exist in ccn,
     * but do not exist on cn, to avoid the problem
     * of excess resource consumption
     */
    while (list_length(records_list) > 0) {
        DynamicMessageInfo* info = (DynamicMessageInfo*)linitial(records_list);

        ereport(DEBUG3, (errmsg("dirty record with qid: %lu, qtype: %d", info->qid.queryId, info->qtype)));

        dywlm_server_remove_record(srvmgr, info, info->qtype);

        pfree(info);

        records_list = list_delete_first(records_list);
    }

    /* verify the free size, not active statement, free size should be total size */
    if (!dywlm_server_check_active(srvmgr) && srvmgr->freesize_update) {
        USE_CONTEXT_LOCK(&srvmgr->global_list_mutex);

        if (srvmgr->freesize != srvmgr->freesize_limit) {
            srvmgr->freesize = srvmgr->freesize_limit;
        }
    }
}

/*
 * @Description: Synchronize workload records between client and server
 * @IN: void
 * @Return: void
 * @See also:
 */
void dywlm_server_sync_records(void)
{
    ServerDynamicManager* srvmgr = &g_instance.wlm_cxt->MyDefaultNodeGroup.srvmgr;

    dywlm_server_sync_records_internal(srvmgr);

    USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_SHARED);

    WLMNodeGroupInfo* hdata = NULL;

    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.node_group_hashtbl);

    while ((hdata = (WLMNodeGroupInfo*)hash_seq_search(&hash_seq)) != NULL) {
        /* skip unused node group and elastic group */
        if (!hdata->used || pg_strcasecmp(VNG_OPTION_ELASTIC_GROUP, hdata->group_name) == 0) {
            continue;
        }

        srvmgr = &hdata->srvmgr;

        dywlm_server_sync_records_internal(srvmgr);
    }
}

/*
 * @Description: display the server structure for one node group
 * @IN: void
 * @Return: void
 * @See also:
 */
void dywlm_server_display_srvmgr_info_internal(ServerDynamicManager* srvmgr, StringInfo strinfo)
{
    appendStringInfo(strinfo,
        _("totalsize[%d], freesize_limit[%d], freesize[%d], freesize_inc[%d], "
          "freesize_update[%d], recover[%d], try_wakeup[%d], group_name[%s].\n"),
        srvmgr->totalsize,
        srvmgr->freesize_limit,
        srvmgr->freesize,
        srvmgr->freesize_inc,
        srvmgr->freesize_update,
        srvmgr->recover,
        srvmgr->try_wakeup,
        srvmgr->group_name);

    WLMContextLock list_lock(&srvmgr->global_list_mutex);

    list_lock.Lock();

    int i = 0;

    DynamicWorkloadRecord* info = NULL;

    /* search priority list */
    foreach_cell(cell, srvmgr->global_waiting_list)
    {
        WLMListNode* pnode = (WLMListNode*)lfirst(cell);

        /* search request list */
        foreach_cell(qcell, pnode->request_list)
        {
            info = (DynamicWorkloadRecord*)lfirst(qcell);

            if (info != NULL) {
                appendStringInfo(strinfo,
                    _("Waiting Global Central number[%d]: qid.procId[%u], qid.queryId[%lu], qid.stamp[%ld], "
                      "memsize[%d], max_memsize[%d], min_memsize[%d], priority[%d], is_dirty[%d], "
                      "actpts[%d], max_actpts[%d], min_actpts[%d], maxpts[%d], "
                      "rpname[%s], nodename[%s], groupname[%s], "
                      "qtype[%d], try_wakeup[%d], removed[%d], is_dirty[%d], maybe_deleted[%d].\n"),
                    i++,
                    info->qid.procId,
                    info->qid.queryId,
                    info->qid.stamp,
                    info->memsize,
                    info->max_memsize,
                    info->min_memsize,
                    info->priority,
                    info->is_dirty,
                    info->actpts,
                    info->max_actpts,
                    info->min_actpts,
                    info->maxpts,
                    info->rpname,
                    info->nodename,
                    info->groupname,
                    info->qtype,
                    info->try_wakeup,
                    info->removed,
                    info->is_dirty,
                    info->maybe_deleted);
            }
        }
    }

    list_lock.UnLock();

    /* print of the hashtable */
    USE_AUTO_LWLOCK(WorkloadRecordLock, LW_SHARED);

    HASH_SEQ_STATUS hash_seq;

    int runcnt = 0;
    int waitcnt = 0;

    hash_seq_init(&hash_seq, srvmgr->global_info_hashtbl);

    while ((info = (DynamicWorkloadRecord*)hash_seq_search(&hash_seq)) != NULL) {

        if (info->qtype == PARCTL_ACTIVE) {
            appendStringInfo(strinfo,
                _("Central Running number[%d]: qid.procId[%u], qid.queryId[%lu], qid.stamp[%ld], "
                  "memsize[%d], max_memsize[%d], min_memsize[%d], priority[%d], "
                  "actpts[%d], max_actpts[%d], min_actpts[%d], maxpts[%d], "
                  "rpname[%s], nodename[%s], groupname[%s], "
                  "qtype[%d], try_wakeup[%d], removed[%d], is_dirty[%d], maybe_deleted[%d].\n"),
                runcnt++,
                info->qid.procId,
                info->qid.queryId,
                info->qid.stamp,
                info->memsize,
                info->max_memsize,
                info->min_memsize,
                info->priority,
                info->actpts,
                info->max_actpts,
                info->min_actpts,
                info->maxpts,
                info->rpname,
                info->nodename,
                info->groupname,
                info->qtype,
                info->try_wakeup,
                info->removed,
                info->is_dirty,
                info->maybe_deleted);
        } else {
            appendStringInfo(strinfo,
                _("Central Waiting number[%d]: qid.procId[%u], qid.queryId[%lu], qid.stamp[%ld], "
                  "memsize[%d], max_memsize[%d], min_memsize[%d], priority[%d], "
                  "actpts[%d], max_actpts[%d], min_actpts[%d], maxpts[%d], "
                  "rpname[%s], nodename[%s], groupname[%s], "
                  "qtype[%d], try_wakeup[%d], removed[%d], is_dirty[%d], maybe_deleted[%d].\n"),
                waitcnt++,
                info->qid.procId,
                info->qid.queryId,
                info->qid.stamp,
                info->memsize,
                info->max_memsize,
                info->min_memsize,
                info->priority,
                info->actpts,
                info->max_actpts,
                info->min_actpts,
                info->maxpts,
                info->rpname,
                info->nodename,
                info->groupname,
                info->qtype,
                info->try_wakeup,
                info->removed,
                info->is_dirty,
                info->maybe_deleted);
        }
    }
}

/*
 * @Description: display the server structure for all node group
 * @IN: void
 * @Return: void
 * @See also:
 */
void dywlm_server_display_srvmgr_info(StringInfo strinfo)
{
    appendStringInfo(
        strinfo, _("\nNodeGroup [%s] dynamic server info:\n"), g_instance.wlm_cxt->MyDefaultNodeGroup.group_name);

    ServerDynamicManager* srvmgr = &g_instance.wlm_cxt->MyDefaultNodeGroup.srvmgr;

    dywlm_server_display_srvmgr_info_internal(srvmgr, strinfo);

    USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_SHARED);

    WLMNodeGroupInfo* hdata = NULL;

    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.node_group_hashtbl);

    while ((hdata = (WLMNodeGroupInfo*)hash_seq_search(&hash_seq)) != NULL) {
        /* skip unused node group and elastic group */
        if (!hdata->used || pg_strcasecmp(VNG_OPTION_ELASTIC_GROUP, hdata->group_name) == 0) {
            continue;
        }

        appendStringInfo(strinfo, _("\nNodeGroup [%s] dynamic server info:\n"), hdata->group_name);

        srvmgr = &hdata->srvmgr;

        dywlm_server_display_srvmgr_info_internal(srvmgr, strinfo);
    }
}

/*
 * @Description: display the resource pool structure for all node group
 * @IN: void
 * @Return: void
 * @See also:
 */
void dywlm_server_display_respool_info(StringInfo strinfo)
{
    appendStringInfo(strinfo, _("\nResource Pool info:\n"));

    ResourcePool* rp = NULL;

    HASH_SEQ_STATUS hseq;
    int i = 0;
    DynamicWorkloadRecord* info = NULL;

    /* search the resource pool hash table to find statement */
    hash_seq_init(&hseq, g_instance.wlm_cxt->resource_pool_hashtbl);

    WLMAutoLWLock htab_lock(ResourcePoolHashLock, LW_SHARED);

    htab_lock.AutoLWLockAcquire();

    /* search hash to find thread id */
    while ((rp = (ResourcePool*)hash_seq_search(&hseq)) != NULL) {
        if (rp->is_dirty) {
            continue;
        }

        appendStringInfo(strinfo,
            _("rpoid[%u], parentoid[%u], active_points[%d], mempct[%d], actpct[%d], is_foreign[%d], "
              "cgroup[%s], ngroup[%s]\n"),
            rp->rpoid,
            rp->parentoid,
            rp->active_points,
            rp->mempct,
            rp->actpct,
            rp->is_foreign,
            rp->cgroup,
            rp->ngroup);

        i = 0;
        USE_CONTEXT_LOCK(&rp->mutex);
        foreach_cell(cell, rp->waiters)
        {
            info = (DynamicWorkloadRecord*)lfirst(cell);

            if (info != NULL) {
                appendStringInfo(strinfo,
                    _("Waiting resource pool number[%d]: qid.procId[%u], qid.queryId[%lu], qid.stamp[%ld], "
                      "memsize[%d], max_memsize[%d], min_memsize[%d], priority[%d], "
                      "actpts[%d], max_actpts[%d], min_actpts[%d], maxpts[%d], "
                      "rpname[%s], nodename[%s], groupname[%s], "
                      "qtype[%d], try_wakeup[%d], removed[%d], is_dirty[%d], maybe_deleted[%d].\n"),
                    i++,
                    info->qid.procId,
                    info->qid.queryId,
                    info->qid.stamp,
                    info->memsize,
                    info->max_memsize,
                    info->min_memsize,
                    info->priority,
                    info->actpts,
                    info->max_actpts,
                    info->min_actpts,
                    info->maxpts,
                    info->rpname,
                    info->nodename,
                    info->groupname,
                    info->qtype,
                    info->try_wakeup,
                    info->removed,
                    info->is_dirty,
                    info->maybe_deleted);
            }
        }
    }
    htab_lock.AutoLWLockRelease();
}
