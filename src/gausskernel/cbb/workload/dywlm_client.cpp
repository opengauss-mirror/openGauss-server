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
 * dywlm_client.cpp
 *     functions for workload management
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/workload/dywlm_client.cpp
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "gssignal/gs_signal.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "tcop/tcopprot.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/poolmgr.h"
#include "utils/atomic.h"
#include "utils/lsyscache.h"
#include "utils/memprot.h"
#include "access/heapam.h"
#include "workload/memctl.h"
#include "workload/workload.h"
#include "optimizer/nodegroups.h"

#define CPU_UTIL_THRESHOLD 95
#define IO_UTIL_THRESHOLD 90

THR_LOCAL bool WLMProcessExiting = false;

THR_LOCAL ServerDynamicManager* g_srvmgr = NULL;

extern unsigned char is_transcation_start(const char* str);

THR_LOCAL int reserved_in_central_waiting = 0;

static THR_LOCAL bool acce_not_enough_resource = false;

/*
 * @Description: register query dynamic information to hash table
 * @IN void
 * @Return: dynamic info node
 * @See also:
 */
DynamicInfoNode* dywlm_info_register(const DynamicMessageInfo* reginfo)
{
    ClientDynamicManager* g_climgr = t_thrd.wlm_cxt.thread_climgr;

    USE_AUTO_LWLOCK(WorkloadStatHashLock, LW_EXCLUSIVE);

    /* too many dynamic info now, ignore this time. */
    if (hash_get_num_entries(g_climgr->dynamic_info_hashtbl) > g_climgr->max_info_count) {
        return NULL;
    }

    if (g_climgr->max_support_statements > 0 &&
        hash_get_num_entries(g_climgr->dynamic_info_hashtbl) >= g_climgr->max_support_statements) {
        return NULL;
    }

    DynamicInfoNode* info =
        (DynamicInfoNode*)hash_search(g_climgr->dynamic_info_hashtbl, &reginfo->qid, HASH_ENTER_NULL, NULL);
    /* init dynamic node info */
    if (info != NULL) {
        info->qid = reginfo->qid;
        info->memsize = reginfo->memsize;
        info->max_memsize = reginfo->max_memsize;
        info->min_memsize = reginfo->min_memsize;
        info->actpts = reginfo->actpts;
        info->max_actpts = reginfo->max_actpts;
        info->min_actpts = reginfo->min_actpts;
        info->maxpts = reginfo->maxpts;
        info->priority = reginfo->priority;

        info->is_dirty = false;
        info->wakeup = false;
        info->condition = (pthread_cond_t)PTHREAD_COND_INITIALIZER;

        (void)pthread_mutex_init(&info->mutex, NULL);

        errno_t errval = strncpy_s(info->rpname, sizeof(info->rpname), reginfo->rpname, sizeof(info->rpname) - 1);
        securec_check_errval(errval, , LOG);
        errval = strncpy_s(info->ngroup, sizeof(info->ngroup), reginfo->groupname, sizeof(info->ngroup) - 1);
        securec_check_errval(errval, , LOG);

        ereport(DEBUG1,
            (errmsg("register dynamic info into dynamic_info_hashtbl, qid:[%u, %lu, %ld]",
                info->qid.procId,
                info->qid.queryId,
                info->qid.stamp)));

        info->threadid = t_thrd.proc_cxt.MyProcPid;
    }

    return info;
}

/*
 * @Description: unregister query dynamic information from hash table
 * @IN void
 * @Return: void
 * @See also:
 */
void dywlm_info_unregister(void)
{
    DynamicInfoNode* info = NULL;
    USE_AUTO_LWLOCK(WorkloadStatHashLock, LW_EXCLUSIVE);

    info = (DynamicInfoNode*)hash_search(
        t_thrd.wlm_cxt.thread_climgr->dynamic_info_hashtbl, &u_sess->wlm_cxt->wlm_params.qid, HASH_FIND, NULL);
    if (info != NULL) {
        ereport(DEBUG1,
            (errmsg("unregister dynamic info from dynamic_info_hashtbl, qid:[%u, %lu, %ld]",
                info->qid.procId,
                info->qid.queryId,
                info->qid.stamp)));
        (void)pthread_mutex_destroy(&info->mutex);
        (void)hash_search(
            t_thrd.wlm_cxt.thread_climgr->dynamic_info_hashtbl, &u_sess->wlm_cxt->wlm_params.qid, HASH_REMOVE, NULL);
    }
}

/*
 * @Description: parallel control ready for client
 * @IN sqltext: sql text
 * @Return: void
 * @See also:
 */
void dywlm_parallel_ready(const char* sqltext)
{
    errno_t errval = memset_s(
        &t_thrd.wlm_cxt.parctl_state, sizeof(t_thrd.wlm_cxt.parctl_state), 0, sizeof(t_thrd.wlm_cxt.parctl_state));
    securec_check_errval(errval, , LOG);

    /* we always handle exception. */
    u_sess->wlm_cxt->parctl_state_control = 1;
    t_thrd.wlm_cxt.parctl_state.except = 1;
    t_thrd.wlm_cxt.parctl_state.simple = 1;
    t_thrd.wlm_cxt.parctl_state.special = WLMIsSpecialQuery(sqltext) ? 1 : 0;
    /* Is the query in a transaction block now? */
    t_thrd.wlm_cxt.parctl_state.transact = IsTransactionBlock() ? 1 : 0;
    t_thrd.wlm_cxt.parctl_state.transact_begin = 0;
    t_thrd.wlm_cxt.parctl_state.subquery = 0;

    /*
     * If we are in a transaction block, we will make it
     * has reserved global and resource pool active statements,
     * so that we can release active statements while transaction
     * block is end.
     */
    if (t_thrd.wlm_cxt.parctl_state.transact && !t_thrd.wlm_cxt.parctl_state.transact_begin &&
        u_sess->wlm_cxt->is_reserved_in_transaction) {
        t_thrd.wlm_cxt.parctl_state.reserve = 1;
    }

    if (IsAbortedTransactionBlockState()) {
        return;
    }

    /* set current user info */
    WLMSetUserInfo();

    /*
     * If the user is super user or it's a special
     * query it will not do the global parallel control.
     */
    if (t_thrd.wlm_cxt.parctl_state.special == 0 && !u_sess->wlm_cxt->wlm_params.rpdata.superuser) {
        t_thrd.wlm_cxt.parctl_state.enqueue = 1;
    }

    /* set wlm stat info */
    WLMSetStatInfo(sqltext);

    /* set wlm debug info */
    u_sess->wlm_cxt->wlm_debug_info.climgr = &t_thrd.wlm_cxt.thread_node_group->climgr;
    u_sess->wlm_cxt->wlm_debug_info.srvmgr = &t_thrd.wlm_cxt.thread_node_group->srvmgr;
    u_sess->wlm_cxt->wlm_debug_info.pstate = &t_thrd.wlm_cxt.parctl_state;
}

/*
 * @Description: parse message from server for client
 * @IN input_message: message from the server
 * @OUT info: message detail info
 * @Return: void
 * @See also:
 */
void dywlm_client_parse_message(StringInfo input_message, DynamicMessageInfo* info)
{
    /* query is finished, maybe we need wake up next query in pending mode */
    info->qid.procId = (Oid)pq_getmsgint(input_message, 4);
    info->qid.queryId = (uint64)pq_getmsgint64(input_message);
    info->qid.stamp = (TimestampTz)pq_getmsgint64(input_message);
    info->qtype = (ParctlType)pq_getmsgint(input_message, 4);
    info->etype = (EnqueueType)pq_getmsgint(input_message, 4);

    info->memsize = pq_getmsgint(input_message, 4);
    info->actpts = pq_getmsgint(input_message, 4);

    errno_t errval =
        strncpy_s(info->nodename, sizeof(info->nodename), pq_getmsgstring(input_message), sizeof(info->nodename) - 1);
    securec_check_errval(errval, , LOG);
    errval = strncpy_s(
        info->groupname, sizeof(info->groupname), pq_getmsgstring(input_message), sizeof(info->groupname) - 1);
    securec_check_errval(errval, , LOG);

    pq_getmsgend(input_message);
}

/*
 * @Description: reply message to server from client
 * @IN errcode: error code
 * @IN mtype: message head character
 * @Return: void
 * @See also:
 */
void dywlm_client_reply(int errcode)
{
    StringInfoData retbuf;

    pq_beginmessage(&retbuf, 'w');
    pq_sendint(&retbuf, errcode, 4);
    pq_endmessage(&retbuf);
    pq_flush();
}

/*
 * @Description: reply workload records on the client
 * @IN void
 * @Return: void
 * @See also:
 */
void dywlm_client_reply_records(ClientDynamicManager* climgr)
{
    int count = 0;
    StringInfoData retbuf;

    if (g_instance.wlm_cxt->dynamic_workload_inited) {
        USE_AUTO_LWLOCK(WorkloadStatHashLock, LW_SHARED);

        DynamicInfoNode* info = NULL;

        HASH_SEQ_STATUS hash_seq;

        hash_seq_init(&hash_seq, climgr->dynamic_info_hashtbl);

        while ((info = (DynamicInfoNode*)hash_seq_search(&hash_seq)) != NULL) {
            /*
             * we can only judge whether the job is queued by whether the job
             * wakes up or not. If we are queuing, we can only think of queuing
             * in the global queue.
             */
            ParctlType qtype = info->wakeup ? PARCTL_ACTIVE : PARCTL_GLOBAL;

            pq_beginmessage(&retbuf, 'r');
            pq_sendint(&retbuf, (int)info->qid.procId, sizeof(int));
            pq_sendint64(&retbuf, (uint64)info->qid.queryId);
            pq_sendint64(&retbuf, (int64)info->qid.stamp);
            pq_sendint(&retbuf, info->memsize, sizeof(int));
            pq_sendint(&retbuf, info->max_memsize, sizeof(int));
            pq_sendint(&retbuf, info->min_memsize, sizeof(int));
            pq_sendint(&retbuf, info->actpts, sizeof(int));
            pq_sendint(&retbuf, info->max_actpts, sizeof(int));
            pq_sendint(&retbuf, info->min_actpts, sizeof(int));
            pq_sendint(&retbuf, info->maxpts, sizeof(int));
            pq_sendint(&retbuf, info->priority, sizeof(int));
            pq_sendint(&retbuf, (int)qtype, sizeof(int));
            pq_sendstring(&retbuf, info->rpname);
            pq_sendstring(&retbuf, g_instance.attr.attr_common.PGXCNodeName);
            pq_sendstring(&retbuf, info->ngroup);
            pq_endmessage(&retbuf);

            ereport(DEBUG1,
                (errmsg("CLIENT REPLY RECORDS qid [%u, %lu, %ld], qtype: %d",
                    info->qid.procId,
                    info->qid.queryId,
                    info->qid.stamp,
                    qtype)));

            ++count;
        }
    }

    /* completed tag */
    pq_beginmessage(&retbuf, 'f');
    pq_sendint(&retbuf, count, 4);
    pq_endmessage(&retbuf);

    pq_flush();
}

/*
 * @Description: forward the message received from the server
 * @IN msginfo: message detail info
 * @Return: void
 * @See also:
 */
int dywlm_client_post(ClientDynamicManager* climgr, const DynamicMessageInfo* msginfo)
{
    /* Make sure qid is valid */
    if (IsQidInvalid(&msginfo->qid) || !g_instance.wlm_cxt->dynamic_workload_inited) {
        return DYWLM_SEND_OK;
    }

    Assert(
        !StringIsValid(msginfo->nodename) || strcmp(msginfo->nodename, g_instance.attr.attr_common.PGXCNodeName) == 0);

    USE_AUTO_LWLOCK(WorkloadStatHashLock, LW_SHARED);

    /* search query dynamic info in the hash table */
    DynamicInfoNode* info = (DynamicInfoNode*)hash_search(climgr->dynamic_info_hashtbl, &msginfo->qid, HASH_FIND, NULL);

    if (info == NULL) {
        return DYWLM_NO_RECORD;
    }

    /* if server concurrency control has error, the corresponding thread will be cancel off */
    if (msginfo->etype == ENQUEUE_ERROR) {
        (void)gs_signal_send(info->threadid, SIGINT);
        return 0;
    }

    RELEASE_AUTO_LWLOCK();

    /* client may receive wake up message twice due to network
     * avoid wake up dirty stmt
     */
    if (info->wakeup || info->is_dirty) {
        return DYWLM_SEND_OK;
    }

    WLMContextLock mtx_lock(&info->mutex, false);

    mtx_lock.Lock();

    /* wake up the corresponding thread and update the current information */
    info->memsize = msginfo->memsize;
    info->actpts = msginfo->actpts;

    mtx_lock.ConditionWakeUp(&info->condition);
    info->wakeup = true;

    mtx_lock.UnLock();

    return DYWLM_SEND_OK;
}

/*
 * @Description: get node index
 * @IN nodename: node name
 * @Return: index, if not found ,return -1
 * @See also:
 */
int dywlm_get_node_idx(const char* nodename)
{
    return PgxcGetNodeIndex(nodename);
}

/*
 * @Description: directly use the interface of server on client
 * @IN msginfo: message detail info
 * @Return: enqueue type
 * @See also:
 */
EnqueueType dywlm_client_handle_for_server(ClientDynamicManager* climgr, DynamicMessageInfo* msginfo)
{
    int count = 0;

    ServerDynamicManager* srvmgr = (ServerDynamicManager*)climgr->srvmgr;

    while (srvmgr->recover || (!COMP_ACC_CLUSTER && srvmgr->freesize_update == 0)) {
        pg_usleep(USECS_PER_SEC);
        ++count;

        /* wait for 5 seconds */
        if (count >= 5) {
            return ENQUEUE_RECOVERY;
        }
    }

    switch (msginfo->qtype) {
        case PARCTL_RESERVE:
        case PARCTL_TRANSACT:
            return dywlm_server_reserve(srvmgr, msginfo); /* reserve memory */
        case PARCTL_RELEASE:
            return dywlm_server_release(srvmgr, msginfo); /* release memory */
        case PARCTL_CANCEL:
            return dywlm_server_cancel(srvmgr, msginfo); /* cancel request */
        case PARCTL_CLEAN:
            return dywlm_server_clean_internal(srvmgr, msginfo->nodename); /* clean for recovery */
        case PARCTL_MVNODE:
            return dywlm_server_move_node_to_list(srvmgr, msginfo); /* move node */
        case PARCTL_JPQUEUE:
            return dywlm_server_jump_queue(srvmgr, msginfo); /* jump priority */
        default:
            break;
    }

    return ENQUEUE_UNKNOWN;
}

/*
 * @Description: update memory usage while query can not be satisfied
 * @IN msginfo: message detail info
 * @Return: void
 * @See also:
 */
void dywlm_client_update_mem(ClientDynamicManager* climgr, DynamicMessageInfo* msginfo)
{
    USE_AUTO_LWLOCK(WorkloadStatHashLock, LW_SHARED);

    DynamicInfoNode* info = (DynamicInfoNode*)hash_search(climgr->dynamic_info_hashtbl, &msginfo->qid, HASH_FIND, NULL);

    if (info != NULL) {
        /* update the information judged from server. */
        info->memsize = msginfo->memsize;
        info->actpts = msginfo->actpts;
    }
}

/* @Description: get the connection to server and send message
 * @IN pgxc_handles: handle of connection
 * @IN ccn_handle: handle of connection to server
 * @IN retry_count: retry count
 * @OUT nodeidx: node index
 * @OUT msginfo: message detail info
 * @Return: true success; false failed
 * @See also:
 */
bool dywlm_client_connect(
    PGXCNodeAllHandles** pgxc_handles, PGXCNodeHandle** ccn_handle, int* nodeidx, DynamicMessageInfo* msginfo)
{
    List* coordlist = NIL;

    MemoryContext curr_mcxt = CurrentMemoryContext;

    PoolAgent* agent = NULL;
    *ccn_handle = NULL;
    *pgxc_handles = NULL;

    /* search central node index */
    *nodeidx = PgxcGetCentralNodeIndex();

    /* create connection to the central node */
    if (*nodeidx >= 0) {
        /* I am central node now, handle the message as server directly */
        if (PgxcIsCentralCoordinator(g_instance.attr.attr_common.PGXCNodeName)) {
            msginfo->isserver = true;
            return true;
        }

        if (coordlist == NIL) {
            USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

            coordlist = lappend_int(coordlist, *nodeidx);
        }

        Assert(list_length(coordlist) == 1);

        PG_TRY();
        {
            *pgxc_handles = get_handles(NULL, coordlist, true);

            agent = get_poolagent();
        }
        PG_CATCH();
        {
            *pgxc_handles = NULL;
            FlushErrorState();
        }
        PG_END_TRY();

        if (*pgxc_handles == NULL) {
            MemoryContextSwitchTo(curr_mcxt);

            list_free_ext(coordlist);

            pg_usleep(3 * USECS_PER_SEC);

            return false;
        }
        *ccn_handle = (*pgxc_handles)->coord_handles[0];
    }

    if (agent == NULL || agent->coord_connections[*nodeidx] == NULL) {
        return false;
    }

    if (*ccn_handle) {
        /* send message to the central node to reserve or release memory */
        if (pgxc_node_dywlm_send_params(*ccn_handle, msginfo) != 0) {
            list_free_ext(coordlist);
            release_pgxc_handles(*pgxc_handles);
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                    errmsg("[DYWLM] Failed to send dynamic workload params to CCN!")));
        }

        list_free_ext(coordlist);

        return true;
    }

    list_free_ext(coordlist);
    release_pgxc_handles(*pgxc_handles);
    *pgxc_handles = NULL;

    return false;
}

/*
 * @Description: send message to server from the client
 * @IN msginfo: message detail info
 * @Return: enqueue type
 * @See also:
 */
EnqueueType dywlm_client_send(ClientDynamicManager* climgr, DynamicMessageInfo* msginfo)
{
    PGXCNodeHandle* ccn_handle = NULL;
    PGXCNodeAllHandles* pgxc_handles = NULL;

    EnqueueType etype = ENQUEUE_CONNERR;

    int nodeidx = 0;

    struct timeval timeout = {300, 0};

    ereport(DEBUG2,
        (errmsg("client send qid[%u, %lu, %ld], queue_type: %d",
            msginfo->qid.procId,
            msginfo->qid.queryId,
            msginfo->qid.stamp,
            msginfo->qtype)));

    if (!dywlm_client_connect(&pgxc_handles, &ccn_handle, &nodeidx, msginfo)) {
        release_pgxc_handles(pgxc_handles);
        return etype;
    }

    /* The application node is the server itself, then directly use the interface on the server */
    if (msginfo->isserver) {
        return dywlm_client_handle_for_server(climgr, msginfo);
    }

    /* release message do not need reply */
    if (msginfo->qtype == PARCTL_RELEASE) {
        release_pgxc_handles(pgxc_handles);
        return ENQUEUE_NONE;
    }

    while (true) {
        bool hasError = false;
        bool isFinished = false;
        if (!pgxc_node_receive(1, &ccn_handle, &timeout)) {
            char* msg = NULL;
            int len;
            char msg_type;

            msg_type = get_message(ccn_handle, &len, &msg);

            switch (msg_type) {
                case 'w': {
                    DynamicMessageInfo info;
                    errno_t errval = memset_s(&info, sizeof(info), 0, sizeof(info));
                    securec_check_errval(errval, , LOG);
                    info.qtype = msginfo->qtype;

                    StringInfoData input_msg;
                    initStringInfo(&input_msg);

                    appendBinaryStringInfo(&input_msg, msg, len);

                    /* parse message string info */
                    dywlm_client_parse_message(&input_msg, &info);

                    etype = info.etype;
                    ereport(DEBUG3,
                        (errmsg("ID[%u,%lu,%ld] -----dywlm_client_Send-----[current:%d, max:%d, min:%d, qtype:%d, "
                                "etype:%d]",
                            info.qid.procId,
                            info.qid.queryId,
                            info.qid.stamp,
                            info.memsize,
                            msginfo->max_memsize,
                            msginfo->min_memsize,
                            info.qtype,
                            info.etype)));

                    /*
                     * If query is in a transaction or reserved from server, and the memory request
                     * is not satisfied, update the request memory.
                     */
                    if (((info.memsize != msginfo->memsize) || (info.actpts != msginfo->actpts)) &&
                        ((msginfo->qtype == PARCTL_TRANSACT) || (msginfo->qtype == PARCTL_RESERVE))) {
                        dywlm_client_update_mem(climgr, &info);
                    }

                    isFinished = true;
                    break;
                }
                case 'E':
                    hasError = true;
                    break;
                case 'Z':
                    if (hasError) {
                        isFinished = true;
                        ereport(LOG,
                            (errmsg("receive message error, "
                                    "error occurred on central node, %s",
                                (ccn_handle->error == NULL) ? "receive data failed" : ccn_handle->error)));
                        release_pgxc_handles(pgxc_handles);
                        pgxc_handles = NULL;
                    }
                    break;
                default: {
                    ereport(ERROR, (errmsg("Unknow message type '%c' for dywlm client send.", msg_type)));;
                    break;
                }
            }

            if (isFinished) {
                break;
            }
        } else {
            release_pgxc_handles(pgxc_handles);
            ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                    errmsg("[DYWLM] Failed to receive queue info from CCN, error info: %s",
                        (ccn_handle->error == NULL) ? "receive data failed" : ccn_handle->error)));
        }
    }

    ccn_handle->state = DN_CONNECTION_STATE_IDLE;
    release_pgxc_handles(pgxc_handles);

    return etype;
}

/*
 * @Description: client get memory size
 * @IN void
 * @Return: memory size to use
 * @See also:
 */
int64 dywlm_client_get_memory(void)
{
    if (!IS_PGXC_DATANODE || !g_instance.wlm_cxt->dynamic_workload_inited) {
        return 0L;
    }

    uint64 totalMemSize = (uint64)maxChunksPerProcess << (chunkSizeInBits - BITS_IN_MB);
    uint64 usedMemSize = (uint64)processMemInChunks << (chunkSizeInBits - BITS_IN_MB);
    int64 freeSize = totalMemSize - usedMemSize;
    int64 extsize = 0L;

    int active_count = g_instance.wlm_cxt->stat_manager.comp_count;

    if (usedMemSize >= totalMemSize * DYWLM_HIGH_QUOTA / FULL_PERCENT) {
        ereport(DEBUG1,
            (errmsg("used size %lu exceeds 90%% total size %lu, get size %ld",
                usedMemSize * KBYTES,
                totalMemSize * KBYTES,
                extsize)));
        return extsize;
    }

    if (active_count == 0) {
        extsize = freeSize * KBYTES * DYWLM_MEDIUM_QUOTA / FULL_PERCENT;
        ereport(DEBUG1, (errmsg("no acitve count, get size %ld", extsize)));
        return extsize;
    }

    if (active_count > 6) {
        if (usedMemSize >= totalMemSize * 70 / FULL_PERCENT) {
            ereport(DEBUG1,
                (errmsg("active count is %d, used size %lu exceeds 70%% total size %lu, get size %ld",
                    active_count,
                    usedMemSize * KBYTES,
                    totalMemSize * KBYTES,
                    extsize)));
            return extsize;
        }

        active_count = 6;
    }

    extsize = freeSize * KBYTES * (DYWLM_MEDIUM_QUOTA - 10 * (active_count - 1)) / FULL_PERCENT;

    ereport(DEBUG1,
        (errmsg("active count is %d, get size %ld is %d%% of free size %ld ",
            active_count,
            extsize,
            DYWLM_MEDIUM_QUOTA - 10 * (active_count - 1),
            freeSize * KBYTES)));

    return extsize;
}

/*
 * @Description: client set resource pool used memory size
 * @IN size: memory size to use
 * @Return: void
 * @See also:
 */
void dywlm_client_set_respool_memory(int size, WLMStatusTag status)
{
    if (!(IS_PGXC_COORDINATOR || IS_SINGLE_NODE) || !g_instance.wlm_cxt->dynamic_workload_inited) {
        return;
    }

    if (!OidIsValid(u_sess->wlm_cxt->wlm_params.rpdata.rpoid) ||
        u_sess->wlm_cxt->wlm_params.rpdata.rpoid == DEFAULT_POOL_OID || size == 0) {
        return;
    }

    ereport(DEBUG1, (errmsg("set resource pool memory: %d", size)));

    if (u_sess->wlm_cxt->wlm_params.rp == NULL) {
        if (status == WLM_STATUS_RELEASE) {
            return;
        }

        USE_AUTO_LWLOCK(ResourcePoolHashLock, LW_SHARED);

        /* get used memory size of resource pool */
        u_sess->wlm_cxt->wlm_params.rp = GetRespoolFromHTab(u_sess->wlm_cxt->wlm_params.rpdata.rpoid, true);

        if (u_sess->wlm_cxt->wlm_params.rp == NULL) {
            ereport(WARNING, (errmsg("cannot get resource pool %u", u_sess->wlm_cxt->wlm_params.rpdata.rpoid)));
            return;
        }
    }

    gs_atomic_add_32(&((ResourcePool*)u_sess->wlm_cxt->wlm_params.rp)->memsize, size);

    /* calculate the total amount of memory requests used by the current node */
    gs_atomic_add_32(&t_thrd.wlm_cxt.thread_climgr->usedsize, size);
}

/*
 * @Description: client get max memory size from resource pool
 * @IN void
 * @Return: max memory size to use
 * @See also:
 */
int64 dywlm_client_get_max_memory_internal(bool* use_tenant)
{
#define RESPOOL_MIN_SIZE (2 * 1024 * 1024)  // 2GB

    /* no memory limit or not coordinator, nothing to do */
    if (!(IS_PGXC_COORDINATOR || IS_SINGLE_NODE)) {
        return 0L;
    }

    int multiple = 1;

    if (g_instance.wlm_cxt->dynamic_workload_inited) {
        /* We allow at least two queries concurrently */
        multiple = (t_thrd.wlm_cxt.thread_climgr->active_statements > 1) ? 2 : 1;

        /* the default resource pool uses the global maximum memory */
        if (!OidIsValid(u_sess->wlm_cxt->wlm_params.rpdata.rpoid) ||
            u_sess->wlm_cxt->wlm_params.rpdata.rpoid == DEFAULT_POOL_OID) {
            return (int64)t_thrd.wlm_cxt.thread_srvmgr->totalsize * DYWLM_HIGH_QUOTA * KBYTES / FULL_PERCENT / multiple;
        }

        if (u_sess->wlm_cxt->wlm_params.rpdata.mem_size > RESPOOL_MIN_SIZE &&
            u_sess->wlm_cxt->wlm_params.rpdata.max_pts > FULL_PERCENT) {
            return (int64)u_sess->wlm_cxt->wlm_params.rpdata.mem_size * DYWLM_HIGH_QUOTA / FULL_PERCENT / multiple;
        } else {
            return (int64)u_sess->wlm_cxt->wlm_params.rpdata.mem_size * DYWLM_HIGH_QUOTA / FULL_PERCENT;
        }
    } else {
        if (u_sess->wlm_cxt->wlm_params.rpdata.rpoid != DEFAULT_POOL_OID) {
            *use_tenant = true;

            if (u_sess->wlm_cxt->wlm_params.rpdata.mem_size > RESPOOL_MIN_SIZE &&
                u_sess->wlm_cxt->wlm_params.rpdata.max_pts > FULL_PERCENT) {
                return (int64)u_sess->wlm_cxt->wlm_params.rpdata.mem_size * DYWLM_HIGH_QUOTA / FULL_PERCENT / multiple;
            } else {
                return (int64)u_sess->wlm_cxt->wlm_params.rpdata.mem_size * DYWLM_HIGH_QUOTA / FULL_PERCENT;
            }
        }
    }

    return 0;
}

/*
 * @Description: client get max memory size from resource pool
 * @IN void
 * @Return: max memory size to use
 * @See also:
 */
int64 dywlm_client_get_max_memory(bool* use_tenant)
{
    /* no memory limit or not coordinator, nothing to do */
    t_thrd.wlm_cxt.collect_info->max_mem = dywlm_client_get_max_memory_internal(use_tenant);

    return t_thrd.wlm_cxt.collect_info->max_mem;
}

/*
 * @Description: client get free memory size from resource pool
 * @IN void
 * @Return: free memory size to use
 * @See also:
 */
int64 dywlm_client_get_free_memory_internal(void)
{
    /* no memory limit or not coordinator, nothing to do */
    if (!(IS_PGXC_COORDINATOR || IS_SINGLE_NODE) || !g_instance.wlm_cxt->dynamic_workload_inited) {
        return 0L;
    }

    /* the default resource pool uses the global free memory */
    if (!OidIsValid(u_sess->wlm_cxt->wlm_params.rpdata.rpoid) ||
        u_sess->wlm_cxt->wlm_params.rpdata.rpoid == DEFAULT_POOL_OID) {
        /* only one active statements, use max memory */
        if (t_thrd.wlm_cxt.thread_climgr->active_statements == 1) {
            return (int64)t_thrd.wlm_cxt.thread_srvmgr->totalsize * DYWLM_HIGH_QUOTA * KBYTES / FULL_PERCENT;
        } else {
            return (int64)t_thrd.wlm_cxt.thread_climgr->freesize * DYWLM_HIGH_QUOTA * KBYTES / FULL_PERCENT;
        }
    }

    USE_AUTO_LWLOCK(ResourcePoolHashLock, LW_SHARED);

    /* get used memory size of resource pool */
    ResourcePool* rp = GetRespoolFromHTab(u_sess->wlm_cxt->wlm_params.rpdata.rpoid, true);

    if (rp == NULL) {
        ereport(WARNING, (errmsg("cannot get resource pool %u", u_sess->wlm_cxt->wlm_params.rpdata.rpoid)));
        return 0L;
    }

    if (rp->memsize <= 0) {
        rp->memsize = 0;
    }

    int64 max_size = (int64)u_sess->wlm_cxt->wlm_params.rpdata.mem_size * DYWLM_HIGH_QUOTA / FULL_PERCENT;
    int64 real_used_size =
        (int64)(t_thrd.wlm_cxt.thread_srvmgr->totalsize - t_thrd.wlm_cxt.thread_climgr->freesize) * KBYTES;
    int64 used_size = (int64)t_thrd.wlm_cxt.thread_climgr->usedsize * KBYTES;

    /* only one active statements, use max memory */
    if (t_thrd.wlm_cxt.thread_climgr->active_statements == 1) {
        return max_size;
    }

    if (used_size < real_used_size) {
        max_size -= real_used_size - used_size;

        if (max_size <= 0) {
            max_size = 0;
        }
    }

    return (int64)rp->memsize * KBYTES > max_size ? 0L : (max_size - (int64)rp->memsize * KBYTES);
}

/*
 * @Description: client get free memory size from resource pool
 * @IN void
 * @Return: free memory size to use
 * @See also:
 */
int64 dywlm_client_get_free_memory(void)
{
    t_thrd.wlm_cxt.collect_info->avail_mem = dywlm_client_get_free_memory_internal();

    return t_thrd.wlm_cxt.collect_info->avail_mem;
}

/*
 * @Description: client get max and free memory size from resource pool or GUC variable
 * @OUT total_mem: max mem info
 * @OUT free_mem: free mem info
 * @OUT use_tenant: if tenant is used
 * @Return: void
 * @See also:
 */
void dywlm_client_get_memory_info(int* total_mem, int* free_mem, bool* use_tenant)
{
    Assert(total_mem != NULL && free_mem != NULL && use_tenant != NULL);

    int query_mem = ASSIGNED_QUERY_MEM(u_sess->attr.attr_sql.statement_mem, u_sess->attr.attr_sql.statement_max_mem);
    int max_mem = (int)dywlm_client_get_max_memory(use_tenant);
    int available_mem = 0;
    const int kb_per_mb = 1024;

    if (g_instance.wlm_cxt->dynamic_workload_inited && g_instance.wlm_cxt->dynamic_memory_collected && max_mem == 0) {
        (elog(LOG, "[DYWLM] System max process memory unexpected to be 0."));
    }

    /* We use query_mem first if set */
    if (query_mem != 0) {
        /* query_mem set can't exceeds system max mem */
        if (max_mem != 0) {
            max_mem = Min(query_mem, max_mem);
        } else {
            max_mem = query_mem;
        }
        available_mem =
            ((u_sess->attr.attr_memory.work_mem >= (STATEMENT_MIN_MEM * kb_per_mb)) ?
                                                   u_sess->attr.attr_memory.work_mem : max_mem);
        available_mem = Min(available_mem, max_mem);

    /* We use dynamic workload if query_mem is not set */
    } else if (max_mem != 0) {
        int dywlm_mem = (int)dywlm_client_get_free_memory();
        max_mem = ASSIGNED_QUERY_MEM(max_mem, u_sess->attr.attr_sql.statement_max_mem);
        if (*use_tenant) {
            available_mem = u_sess->attr.attr_memory.work_mem;
        } else {
            available_mem = Min(Max(dywlm_mem, STATEMENT_MIN_MEM * kb_per_mb), max_mem);
        }
    }

    *total_mem = max_mem;
    *free_mem = available_mem;

    /* before dynamic memory collected,set free memory STATEMENT_MIN_MEM */
    if (g_instance.wlm_cxt->dynamic_workload_inited && !g_instance.wlm_cxt->dynamic_memory_collected) {
        *total_mem = STATEMENT_MIN_MEM * 1024;
        *free_mem = STATEMENT_MIN_MEM * 1024;
        ereport(DEBUG3, (errmsg("[DYWLM] Memory info has not been collected,use STATEMENT_MIN_MEM for query")));
    }
}

/*
 * @Description: initalize message detail info
 * @IN msginfo: message detail info
 * @IN qtype: parctl type
 * @Return: message detail info
 * @See also:
 */
DynamicMessageInfo* dywlm_client_initmsg(ClientDynamicManager* climgr, DynamicMessageInfo* msginfo, ParctlType qtype)
{
    errno_t errval = memset_s(msginfo, sizeof(DynamicMessageInfo), 0, sizeof(DynamicMessageInfo));
    securec_check_errval(errval, , LOG);

    msginfo->qid = u_sess->wlm_cxt->wlm_params.qid;
    msginfo->maxpts = u_sess->wlm_cxt->wlm_params.rpdata.max_pts;
    msginfo->priority = t_thrd.wlm_cxt.qnode.priority;

    /* different qtype, so that the message has different function */
    msginfo->qtype = qtype;
    msginfo->etype = ENQUEUE_NONE;
    msginfo->isserver = is_pgxc_central_nodename(g_instance.attr.attr_common.PGXCNodeName); /* Am I CCN? */

    errval = strncpy_s(msginfo->rpname,
        sizeof(msginfo->rpname),
        u_sess->wlm_cxt->wlm_params.rpdata.rpname,
        sizeof(msginfo->rpname) - 1);
    securec_check_errval(errval, , LOG);
    errval = strncpy_s(msginfo->nodename,
        sizeof(msginfo->nodename),
        g_instance.attr.attr_common.PGXCNodeName,
        sizeof(msginfo->nodename) - 1);
    securec_check_errval(errval, , LOG);
    errval = strncpy_s(msginfo->groupname,
        sizeof(msginfo->groupname),
        u_sess->wlm_cxt->wlm_params.ngroup,
        sizeof(msginfo->groupname) - 1);
    securec_check_errval(errval, , LOG);

    msginfo->actpts = u_sess->wlm_cxt->wlm_params.rpdata.act_pts;
    msginfo->memsize = u_sess->wlm_cxt->wlm_params.memsize;
    msginfo->subquery = (t_thrd.wlm_cxt.parctl_state.subquery == 1) ? true : false;

    /* reserve info */
    if (qtype == PARCTL_RESERVE || qtype == PARCTL_TRANSACT) {
        msginfo->max_actpts = u_sess->wlm_cxt->wlm_params.rpdata.max_act_pts;
        msginfo->min_actpts = u_sess->wlm_cxt->wlm_params.rpdata.min_act_pts;
        msginfo->max_memsize = u_sess->wlm_cxt->wlm_params.max_memsize;
        msginfo->min_memsize = u_sess->wlm_cxt->wlm_params.min_memsize;

        ereport(DEBUG3,
            (errmsg("----DYWLM---- dywlm_client_initmsg FULLinfo [min_mem: %d, min_act: %d, max_mem:%d, max_act:%d]",
                msginfo->min_memsize,
                msginfo->min_actpts,
                msginfo->max_memsize,
                msginfo->max_actpts)));

    /* release resource should get the resource used from records hash table */
    } else if (qtype == PARCTL_RELEASE) {
        USE_AUTO_LWLOCK(WorkloadStatHashLock, LW_SHARED);

        DynamicInfoNode* info =
            (DynamicInfoNode*)hash_search(climgr->dynamic_info_hashtbl, &msginfo->qid, HASH_FIND, NULL);

        /* get message info from register hash table */
        if (info != NULL) {
            msginfo->actpts = info->actpts;
            msginfo->max_actpts = info->max_actpts;
            msginfo->min_actpts = info->min_actpts;
            // Acceleration cluster
            if (COMP_ACC_CLUSTER) {
                msginfo->memsize = 0;
            } else { // customer cluster
                msginfo->memsize = info->memsize;
            }
            msginfo->max_memsize = info->max_memsize;
            msginfo->min_memsize = info->min_memsize;
        }
    }

    return msginfo;
}

/*
 * @Description: clean handler for the client
 * @IN ptr: parctl state pointer
 * @Return: void
 * @See also:
 */
void dywlm_client_clean(void* ptr)
{
    if (ptr == NULL) {
        return;
    }

    ParctlState* state = (ParctlState*)ptr;

    /* it's released, nothing to do */
    if (state->rp_release || t_thrd.proc_cxt.proc_exit_inprogress) {
        return;
    }

    /* record the wait count in central queue */
    if (t_thrd.wlm_cxt.parctl_state.reserve && !t_thrd.wlm_cxt.parctl_state.transact) {
        USE_CONTEXT_LOCK(&t_thrd.wlm_cxt.thread_climgr->statement_list_mutex);

        if (t_thrd.wlm_cxt.thread_climgr->central_waiting_count) {
            t_thrd.wlm_cxt.thread_climgr->central_waiting_count--;
            reserved_in_central_waiting--;
        }
    }

    DynamicMessageInfo msginfo;

    /* init message info to send to server, it's will release resource on the server */
    (void)dywlm_client_initmsg(t_thrd.wlm_cxt.thread_climgr, &msginfo, PARCTL_CANCEL);

    (void)dywlm_client_send(t_thrd.wlm_cxt.thread_climgr, &msginfo);

    pgstat_report_waiting_on_resource(STATE_NO_ENQUEUE);

    dywlm_info_unregister();

    state->central_waiting = 0;

    /* handle exception */
    if (state->except) {
        WLMResetStatInfo4Exception();
        state->except = 0;
    }

    state->rp_release = 1;
}

/*
 * @Description: wait for reserving resource on the server
 * @IN msginfo: message detail info
 * @Return: 1 - global reserved
 *          0 - global reserved failed
 * @See also:
 */
int dywlm_client_wait(DynamicMessageInfo* msginfo)
{
    ClientDynamicManager* g_climgr = t_thrd.wlm_cxt.thread_climgr;

    /* no memory to reserve, nothing to do */
    if (!COMP_ACC_CLUSTER && msginfo->memsize <= 0) {
        ereport(DEBUG1,
            (errmsg("query[%u:%lu:%ld] request memory is less than 0",
                msginfo->qid.procId, msginfo->qid.queryId, msginfo->qid.stamp)));

        return 0;
    }

    int count = 0;
    bool timeout = false;

    /* register the info node in the hash table */
    DynamicInfoNode* info = dywlm_info_register(msginfo);

    if (info == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory of current node, please check statement count")));
    }

    /*
     * While the current node is in recovery state, query operation
     * cannot be performed, the reason is, node maybe still has the
     * residual enqueue information of the last process, we must take
     * these information clearing off, Otherwise, the resource information
     * leakage will occur on the server.
     * If current node do not finish recovery after 5 seconds, the
     * query can run anyway.
     */
    while (g_climgr->recover) {
        pg_usleep(USECS_PER_SEC);
        count++;

        if (count > 5) {
            info->wakeup = true;
            return 0;
        }
    }

    EnqueueType etype = dywlm_client_send(g_climgr, msginfo);

    /* If current CN is CCN, the information passes without using hashtable.
     * Structure msginfo has changed as a parameter.
     * So we need to update client hashtable according to msginfo.
     */
    if (!msginfo->isserver) {
        USE_AUTO_LWLOCK(WorkloadStatHashLock, LW_SHARED);
        DynamicInfoNode* crtinfo =
            (DynamicInfoNode*)hash_search(g_climgr->dynamic_info_hashtbl, &msginfo->qid, HASH_FIND, NULL);
        if (crtinfo != NULL) {
            msginfo->memsize = crtinfo->memsize;
            msginfo->actpts = crtinfo->actpts;
        }
    /* If current CN is not CCN, the changes have been written into hashtable on bosth side,
     * Structure msginfo should be updated according to the client-side hash table.
     */
    } else {
        USE_AUTO_LWLOCK(WorkloadStatHashLock, LW_SHARED);
        DynamicInfoNode* crtinfo =
            (DynamicInfoNode*)hash_search(g_climgr->dynamic_info_hashtbl, &msginfo->qid, HASH_FIND, NULL);
        if (crtinfo != NULL) {
            crtinfo->memsize = msginfo->memsize;
            crtinfo->actpts = msginfo->actpts;
        }
    }

    switch (etype) {
        case ENQUEUE_CONNERR:
            /* failed to connect to central node, query will run */
            ereport(LOG, (errmsg("failed to connect to central node")));
            return 0;
        case ENQUEUE_ERROR:
            ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("failed to enqueue in central node")));
            break;
        case ENQUEUE_NORESPOOL:
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("resource pool: %s does not exist", msginfo->rpname)));
            break;
        case ENQUEUE_MEMERR:
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory of central node")));
            break;
        case ENQUEUE_GROUPERR:
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("group name %s does not exist on central node", msginfo->groupname)));
            break;
        case ENQUEUE_RECOVERY:
            /* central node is in recovery state, query will run */
            ereport(LOG, (errmsg("central node is recovering")));
            return 0;
        case ENQUEUE_NONE:
            /* cluster has enough memory, need not enqueue on server. */
            info->wakeup = true;
            return 1;
        case ENQUEUE_UNKNOWN:
            ereport(WARNING, (errmsg("parctl type %d is not valid for server handler", msginfo->qtype)));
            return 0;
        default:
            break;
    }

    /* query which used planA in logical-cluster should not be blocked */
    if (u_sess->wlm_cxt->wlm_params.use_planA) {
        msginfo->qtype = PARCTL_RELEASE;
        msginfo->isserver = is_pgxc_central_nodename(g_instance.attr.attr_common.PGXCNodeName);
        etype = dywlm_client_send(g_climgr, msginfo);

        ereport(DEBUG3,
            (errmsg("force run qid [%u, %lu, %ld] since it uses plan A, etype: %d",
                msginfo->qid.procId, msginfo->qid.queryId,
                msginfo->qid.stamp, etype)));

        t_thrd.wlm_cxt.parctl_state.enqueue = 0;
        t_thrd.wlm_cxt.parctl_state.rp_release = 1;
        msginfo->memsize = msginfo->min_memsize;
        msginfo->actpts = msginfo->min_actpts;

        return 0;
    }

    pgstat_report_waiting_on_resource(STATE_ACTIVE_STATEMENTS);

    reserved_in_central_waiting = 0;

    /* record the wait count in central queue */
    if (t_thrd.wlm_cxt.parctl_state.reserve && !t_thrd.wlm_cxt.parctl_state.transact &&
        !t_thrd.wlm_cxt.parctl_state.subquery) {
        WLMContextLock list_lock(&g_climgr->statement_list_mutex);

        list_lock.Lock();

        g_climgr->central_waiting_count++;

        reserved_in_central_waiting++;

        ereport(DEBUG3,
            (errmsg("---OVERRUN dywlm_client_wait() reserve --- "
                    "active_statements: %d, central_waiting_count: %d, max_active_statements: %d.\n",
                g_climgr->active_statements, g_climgr->central_waiting_count, g_climgr->max_active_statements)));

        /*
         * It will wake up a statement if there are
         * statements waiting in the statements waiting
         * list due to the number of active statements
         * is less than max_active_statements.
         */
        if (list_length(g_climgr->statements_waiting_list) > 0 && g_climgr->max_support_statements > 0 &&
            g_climgr->current_support_statements < g_climgr->max_support_statements) {
            DynamicInfoNode* dyinfo = (DynamicInfoNode*)linitial(g_climgr->statements_waiting_list);

            if (dyinfo != NULL) {
                list_lock.ConditionWakeUp(&dyinfo->condition);
            }
        }

        list_lock.UnLock();
    }

    WLMContextLock mtx_lock(&info->mutex, false);

    mtx_lock.Lock();

    mtx_lock.set(dywlm_client_clean, &t_thrd.wlm_cxt.parctl_state);

    t_thrd.wlm_cxt.parctl_state.central_waiting = 1;

    /* report workload status */
    pgstat_report_statement_wlm_status();

    while (!info->wakeup && !timeout) {
        bool ImmediateInterruptOK_Old = t_thrd.int_cxt.ImmediateInterruptOK;

        PG_TRY();
        {
            /* check for pending interrupts before waiting. */
            CHECK_FOR_INTERRUPTS();

            t_thrd.int_cxt.ImmediateInterruptOK = true;

            /* wait for the condition and timeout is set by user */
            if (u_sess->attr.attr_resource.transaction_pending_time <= 0) {
                Assert(&info->condition != NULL);
                mtx_lock.ConditionTimedWait(&info->condition, SECS_PER_MINUTE);
            } else {
                int sec_timeout;
                if (COMP_ACC_CLUSTER) {
                    sec_timeout = SECS_PER_MINUTE / 3; /* 20s is enough */
                } else {
                    sec_timeout = u_sess->attr.attr_resource.transaction_pending_time;
                }

                Assert(&info->condition != NULL);
                mtx_lock.ConditionTimedWait(&info->condition, sec_timeout);
                if ((t_thrd.wlm_cxt.parctl_state.transact || t_thrd.wlm_cxt.parctl_state.subquery ||
                        COMP_ACC_CLUSTER) &&
                    !info->wakeup) {
                    /* wait timeout, transaction block will not continue to wait avoiding deadlock. */
                    timeout = true;
                }
            }
            t_thrd.int_cxt.ImmediateInterruptOK = ImmediateInterruptOK_Old;
        }
        PG_CATCH();
        {
            t_thrd.int_cxt.ImmediateInterruptOK = ImmediateInterruptOK_Old;

            /* UnLock the waiting mutex. */
            mtx_lock.UnLock();

            PG_RE_THROW();
        }
        PG_END_TRY();
    }

    mtx_lock.reset();

    mtx_lock.UnLock();

    /* We should refresh from hashtable to get the current information when a query be waked up. */
    WLMAutoLWLock htab_lock(WorkloadStatHashLock, LW_SHARED);
    htab_lock.AutoLWLockAcquire();

    DynamicInfoNode* wkupinfo =
        (DynamicInfoNode*)hash_search(g_climgr->dynamic_info_hashtbl, &msginfo->qid, HASH_FIND, NULL);
    if (wkupinfo != NULL) {
        msginfo->memsize = wkupinfo->memsize;
        msginfo->actpts = wkupinfo->actpts;
    }

    htab_lock.AutoLWLockRelease();

    t_thrd.wlm_cxt.parctl_state.central_waiting = 0;

    /* record the wait count in central queue */
    if (t_thrd.wlm_cxt.parctl_state.reserve && !t_thrd.wlm_cxt.parctl_state.transact &&
        !t_thrd.wlm_cxt.parctl_state.subquery) {
        USE_CONTEXT_LOCK(&g_climgr->statement_list_mutex);

        if (g_climgr->central_waiting_count) {
            g_climgr->central_waiting_count--;
            reserved_in_central_waiting--;
            ereport(DEBUG3,
                (errmsg("---OVERRUN dywlm_client_wait() release --- "
                        "active_statements: %d, central_waiting_count: %d, max_active_statements: %d.\n",
                    g_climgr->active_statements,
                    g_climgr->central_waiting_count,
                    g_climgr->max_active_statements)));
        }
    }

    pgstat_report_waiting_on_resource(STATE_NO_ENQUEUE);

    pgstat_report_statement_wlm_status();

    if ((t_thrd.wlm_cxt.parctl_state.transact || t_thrd.wlm_cxt.parctl_state.subquery || COMP_ACC_CLUSTER) &&
        !info->wakeup) {
        msginfo->qtype = PARCTL_RELEASE;
        msginfo->isserver = is_pgxc_central_nodename(g_instance.attr.attr_common.PGXCNodeName);
        etype = dywlm_client_send(g_climgr, msginfo);

        ereport(DEBUG2,
            (errmsg("send qid [%u, %lu, %ld] to release transaction block, etype: %d",
                msginfo->qid.procId,
                msginfo->qid.queryId,
                msginfo->qid.stamp,
                etype)));

        t_thrd.wlm_cxt.parctl_state.enqueue = 0;
        t_thrd.wlm_cxt.parctl_state.rp_release = 1;
        u_sess->wlm_cxt->forced_running = true;
        if (COMP_ACC_CLUSTER) {
            acce_not_enough_resource = true;
        }

        return 0;
    }

    return 1;
}

/*
 * @Description: remove the node from waiting list
 * @IN ptr: the node will be removed
 * @Return: void
 * @See also:
 */
void dywlm_client_max_clean(void* ptr)
{
    Assert(ptr != NULL);
    Assert(t_thrd.wlm_cxt.thread_climgr != NULL);

    t_thrd.wlm_cxt.thread_climgr->statements_waiting_list =
        list_delete_ptr(t_thrd.wlm_cxt.thread_climgr->statements_waiting_list, ptr);

    pfree(ptr);

    t_thrd.wlm_cxt.parctl_state.preglobal_waiting = 0;

    return;
}

/*
 * @Description: if resource is limited
 * @IN: void
 * @Return: bool
 * @See also: dywlm_client_max_reserve
 */
bool dywlm_is_resource_limited(void)
{
    ClientDynamicManager* g_climgr = t_thrd.wlm_cxt.thread_climgr;

    ereport(DEBUG3,
        (errmsg("---OVERRUN dywlm_is_resource_limited()--- "
                "active_statements: %d, central_waiting_count: %d, max_active_statements: %d.\n",
            g_climgr->active_statements,
            g_climgr->central_waiting_count,
            g_climgr->max_active_statements)));

    if ((g_climgr->active_statements >= g_climgr->max_active_statements &&
            g_climgr->active_statements >= g_climgr->central_waiting_count &&
            (g_climgr->active_statements - g_climgr->central_waiting_count) >= g_climgr->max_active_statements) ||
        (u_sess->wlm_cxt->query_count_record == false && g_climgr->max_support_statements > 0 &&
            g_climgr->current_support_statements >= g_climgr->max_support_statements)) {

        /* example: WLMmonitorDeviceStat.maxCpuUtil >= CPU_UTIL_THRESHOLD ||
         *          WLMmonitorDeviceStat.maxIOUtil >= IO_UTIL_THRESHOLD
         */
        return true;
    }

    return false;
}

/*
 * @Description: CN reserve max resource
 * @IN: void
 * @Return: void
 * @See alsO:
 */
void dywlm_client_max_reserve(void)
{
    ClientDynamicManager* g_climgr = t_thrd.wlm_cxt.thread_climgr;

    Assert(g_climgr != NULL);

    if (!t_thrd.wlm_cxt.parctl_state.enqueue) {
        return;
    }

    if (IsTransactionBlock()) {
        if (!u_sess->wlm_cxt->is_reserved_in_transaction) {
            u_sess->wlm_cxt->is_reserved_in_transaction = true;
            t_thrd.wlm_cxt.parctl_state.reserve = 1;
        }

        return;
    }

    /* while max_active_statements less than 0, do not control */
    if (g_climgr->max_active_statements <= 0) {
        return;
    }

    WLMContextLock list_lock(&g_climgr->statement_list_mutex);

    list_lock.Lock();

    if (u_sess->wlm_cxt->reserved_in_active_statements > 0) {
        ereport(DEBUG2,
            (errmsg("When new query is arriving, thread is reserved %d statement and "
                    "the reserved debug query is %s.",
                u_sess->wlm_cxt->reserved_in_active_statements,
                u_sess->wlm_cxt->reserved_debug_query)));

        g_climgr->active_statements =
            (g_climgr->active_statements > u_sess->wlm_cxt->reserved_in_active_statements)
                ? (g_climgr->active_statements - u_sess->wlm_cxt->reserved_in_active_statements)
                : 0;
        u_sess->wlm_cxt->reserved_in_active_statements = 0;
    }

    /* Check if the central waiting count is changed */
    if (reserved_in_central_waiting > 0) {
        ereport(DEBUG2,
            (errmsg("When new query is waiting in central, thread is reserved %d statement and "
                    "the reserved debug query is %s.",
                reserved_in_central_waiting,
                u_sess->wlm_cxt->reserved_debug_query)));

        g_climgr->central_waiting_count = (g_climgr->central_waiting_count > reserved_in_central_waiting)
                                              ? (g_climgr->central_waiting_count - reserved_in_central_waiting)
                                              : 0;
        reserved_in_central_waiting = 0;
    }

    if (g_climgr->max_statements > 0 && (list_length(g_climgr->statements_waiting_list) >= g_climgr->max_statements)) {
        list_lock.UnLock();
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("the counts of waiting statements have reached the limitation.")));
    }

    /*
     * statement will insert waiting list when active statements up
     * to max_active_statements. And if cpu or io utility is up to
     * threshold, statement will insert waiting list too.
     */
    if (dywlm_is_resource_limited()) {
        pgstat_report_waiting_on_resource(STATE_ACTIVE_STATEMENTS);

        MemoryContext oldContext = MemoryContextSwitchTo(g_instance.wlm_cxt->workload_manager_mcxt);
        DynamicInfoNode* info = (DynamicInfoNode*)palloc0_noexcept(sizeof(DynamicInfoNode));

        if (info == NULL) {
            list_lock.UnLock();
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory of current memory.")));
        }

        info->condition = (pthread_cond_t)PTHREAD_COND_INITIALIZER;

        g_climgr->statements_waiting_list = lappend(g_climgr->statements_waiting_list, info);

        (void)MemoryContextSwitchTo(oldContext);

        list_lock.set(dywlm_client_max_clean, info);

        t_thrd.wlm_cxt.parctl_state.preglobal_waiting = 1;

        pgstat_report_statement_wlm_status();

        do {
            bool ImmediateInterruptOK_Old = t_thrd.int_cxt.ImmediateInterruptOK;

            PG_TRY();
            {
                CHECK_FOR_INTERRUPTS();
                t_thrd.int_cxt.ImmediateInterruptOK = true;
                list_lock.ConditionWait(&info->condition);
                t_thrd.int_cxt.ImmediateInterruptOK = ImmediateInterruptOK_Old;
            }
            PG_CATCH();
            {
                t_thrd.int_cxt.ImmediateInterruptOK = ImmediateInterruptOK_Old;

                /* we must make sure we have the lock now */
                if (!list_lock.IsOwner()) {
                    list_lock.Lock(true);
                }

                /* clean the info in the list */
                list_lock.UnLock();

                PG_RE_THROW();
            }
            PG_END_TRY();
        } while (dywlm_is_resource_limited());

        list_lock.clean();

        t_thrd.wlm_cxt.parctl_state.preglobal_waiting = 0;
    }

    g_climgr->active_statements++;
    u_sess->wlm_cxt->reserved_in_active_statements++;

    ereport(DEBUG3,
        (errmsg("---OVERRUN dywlm_client_max_reserve() --- "
                "active_statements: %d, central_waiting_count: %d, max_active_statements: %d.\n",
            g_climgr->active_statements,
            g_climgr->central_waiting_count,
            g_climgr->max_active_statements)));

    if (u_sess->wlm_cxt->query_count_record == false) {
        u_sess->wlm_cxt->query_count_record = true;
        g_climgr->current_support_statements++;
    }

    /* reserved the string for later checking */
    if (t_thrd.postgres_cxt.debug_query_string) {
        int rcs = snprintf_truncated_s(u_sess->wlm_cxt->reserved_debug_query,
            sizeof(u_sess->wlm_cxt->reserved_debug_query),
            "%s",
            t_thrd.postgres_cxt.debug_query_string);
        securec_check_ss(rcs, "\0", "\0");
    }

    u_sess->wlm_cxt->wlm_debug_info.active_statement = g_climgr->active_statements;

    pgstat_report_waiting_on_resource(STATE_NO_ENQUEUE);

    t_thrd.wlm_cxt.parctl_state.reserve = 1;

    pgstat_report_statement_wlm_status();

    list_lock.UnLock();
}

void dywlm_client_release_statement()
{
    if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) &&
        t_thrd.wlm_cxt.collect_info->sdetail.statement) {
        pfree_ext(
            t_thrd.wlm_cxt.collect_info->sdetail.statement);
    }

    return;
}

bool dywlm_is_local_node()
{
    if ((IS_PGXC_COORDINATOR && !IsConnFromCoord()) || IS_SINGLE_NODE) {
        return true;
    }

    return false;
}

/*
 * @Description: CN release resource when statements finish
 * @IN: state: parallel control state
 * @Return: void
 * @See also:
 */
void dywlm_client_max_release(ParctlState* state)
{
    ClientDynamicManager* g_climgr = t_thrd.wlm_cxt.thread_climgr;

    /* handle exception */
    if (state->except) {
        WLMResetStatInfo4Exception();
        state->except = 0;
    }

    if (state->release || !state->reserve) {
        dywlm_client_release_statement();
        return;
    }

    if (dywlm_is_local_node() && (u_sess->wlm_cxt->parctl_state_exit || !IsTransactionBlock())) {
        Assert(g_climgr != NULL);

        WLMContextLock list_lock(&g_climgr->statement_list_mutex);

        list_lock.Lock();

        /*
         * It will wake up a statement if there are
         * statements waiting in the statements waiting
         * list due to the number of active statements
         * is more than max_active_statements.
         */
        if (list_length(g_climgr->statements_waiting_list) > 0 && g_climgr->max_support_statements > 0 &&
            g_climgr->current_support_statements < g_climgr->max_support_statements) {
        /* &&
        WLMmonitorDeviceStat.maxCpuUtil < CPU_UTIL_THRESHOLD &&
        WLMmonitorDeviceStat.maxIOUtil < IO_UTIL_THRESHOLD */
            DynamicInfoNode* info = (DynamicInfoNode*)linitial(g_climgr->statements_waiting_list);

            if (info != NULL) {
                list_lock.ConditionWakeUp(&info->condition);
            }
        }

        if (g_climgr->active_statements > 0) {
            g_climgr->active_statements--;
            u_sess->wlm_cxt->reserved_in_active_statements--;
            ereport(DEBUG3,
                (errmsg("---OVERRUN dywlm_client_max_release() --- "
                        "active_statements: %d, central_waiting_count: %d, max_active_statements: %d.\n",
                    g_climgr->active_statements,
                    g_climgr->central_waiting_count,
                    g_climgr->max_active_statements)));
        }

        state->release = 1;

        if (u_sess->wlm_cxt->is_reserved_in_transaction) {
            u_sess->wlm_cxt->is_reserved_in_transaction = false;
        }

        list_lock.UnLock();

        if (u_sess->wlm_cxt->parctl_state_exit == 0) {
            pgstat_report_statement_wlm_status();
        }
    }

    dywlm_client_release_statement();
}

/* clean the active statements counts when thread is exiting */
void dywlm_client_proc_release(void)
{
    ClientDynamicManager* g_climgr = t_thrd.wlm_cxt.thread_climgr;

    WLMContextLock list_lock(&g_climgr->statement_list_mutex);

    list_lock.Lock();

    if (u_sess->wlm_cxt->query_count_record) {
        u_sess->wlm_cxt->query_count_record = false;
        g_climgr->current_support_statements--;

        /* try to wake if it is suspend by max support statements */
        if (g_climgr->max_support_statements > 0 &&
            (g_climgr->max_support_statements - g_climgr->current_support_statements == 1) &&
            list_length(g_climgr->statements_waiting_list) > 0) {
            DynamicInfoNode* info = (DynamicInfoNode*)linitial(g_climgr->statements_waiting_list);

            if (info != NULL) {
                list_lock.ConditionWakeUp(&info->condition);
            }
        }
    }

    if (u_sess->wlm_cxt->reserved_in_active_statements > 0) {
        ereport(LOG,
            (errmsg("When thread is exited, thread is reserved %d global statement and "
                    "the reserved debug query is %s.",
                u_sess->wlm_cxt->reserved_in_active_statements,
                u_sess->wlm_cxt->reserved_debug_query)));

        g_climgr->active_statements =
            (g_climgr->active_statements > u_sess->wlm_cxt->reserved_in_active_statements)
                ? (g_climgr->active_statements - u_sess->wlm_cxt->reserved_in_active_statements)
                : 0;
        u_sess->wlm_cxt->reserved_in_active_statements = 0;
    }

    if (reserved_in_central_waiting > 0) {
        ereport(LOG,
            (errmsg("When new query is exited and waiting in central, thread is reserved %d statement and "
                    "the reserved debug query is %s.",
                reserved_in_central_waiting,
                u_sess->wlm_cxt->reserved_debug_query)));

        g_climgr->central_waiting_count = (g_climgr->central_waiting_count > reserved_in_central_waiting)
                                              ? (g_climgr->central_waiting_count - reserved_in_central_waiting)
                                              : 0;
        reserved_in_central_waiting = 0;
    }

    list_lock.UnLock();
}

/*
 * @Description: update max_active_statements for one node group
 * @IN climgr: the information of one node group
 * @IN active_statements: new value of max_active_statements
 * @Return: void
 * @See also:
 */
void dywlm_update_max_statements_internal(ClientDynamicManager* climgr, int active_statements)
{
    WLMContextLock list_lock(&climgr->statement_list_mutex);

    list_lock.Lock();

    int old_active_statements = climgr->max_active_statements;

    int num_wake_up = active_statements - old_active_statements;

    climgr->max_active_statements = active_statements;

    climgr->max_statements = t_thrd.utils_cxt.gs_mp_inited
                                 ? (MAX_PARCTL_MEMORY * DYWLM_HIGH_QUOTA / FULL_PERCENT -
                                       active_statements * g_instance.wlm_cxt->parctl_process_memory) /
                                       PARCTL_MEMORY_UNIT
                                 : 0;

    /* If the new value of max_active_statements is larger than previous,
     * and there are statements waiting in waiting list, try to wake it up,
     * until active statements is up to new max_active_statements.
     */
    while (num_wake_up-- > 0 && list_length(climgr->statements_waiting_list)) {
        DynamicInfoNode* info = (DynamicInfoNode*)linitial(climgr->statements_waiting_list);

        if (info != NULL) {
            list_lock.ConditionWakeUp(&info->condition);
        }
    }

    list_lock.UnLock();
}

/*
 * @Description: update max_active_statements
 * @IN active_statements: new value of max_active_statements
 * @Return: void
 * @See also:
 */
void dywlm_update_max_statements(int active_statements)
{
    ClientDynamicManager* climgr = &g_instance.wlm_cxt->MyDefaultNodeGroup.climgr;

    if (AmPostmasterProcess()) {
        dywlm_update_max_statements_internal(climgr, active_statements);
    } else if (AmWLMWorkerProcess()) {
        USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_SHARED);

        WLMNodeGroupInfo* hdata = NULL;

        HASH_SEQ_STATUS hash_seq;
        hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.node_group_hashtbl);

        while ((hdata = (WLMNodeGroupInfo*)hash_seq_search(&hash_seq)) != NULL) {
            if (!hdata->used) {
                continue;
            }

            climgr = &hdata->climgr;

            dywlm_update_max_statements_internal(climgr, active_statements);
        }
    }
}

/*
 * @Description: client reserve resource
 * @IN void
 * @Return: 1 - global reserved
 *          0 - global reserved failed
 * @See also:
 */
void dywlm_client_reserve(QueryDesc* queryDesc, bool isQueryDesc)
{
    /* need not do enqueue */
    bool isNeedEnqueue = !t_thrd.wlm_cxt.parctl_state.enqueue || t_thrd.wlm_cxt.parctl_state.simple || 
                            !IsQueuedSubquery();
    if (isNeedEnqueue) {
        return;
    }

    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    if (g_wlm_params->rpdata.max_pts > 0 && g_wlm_params->rpdata.mem_size > 0) {
        Assert((g_wlm_params->memsize * KBYTES) <= g_wlm_params->rpdata.mem_size &&
               (g_wlm_params->max_memsize * KBYTES) <= g_wlm_params->rpdata.mem_size &&
               g_wlm_params->memsize <= g_wlm_params->max_memsize &&
               (g_wlm_params->min_memsize * KBYTES) <= g_wlm_params->rpdata.mem_size &&
               g_wlm_params->rpdata.act_pts <= g_wlm_params->rpdata.max_pts &&
               g_wlm_params->rpdata.max_act_pts <= g_wlm_params->rpdata.max_pts &&
               g_wlm_params->rpdata.min_act_pts <= g_wlm_params->rpdata.max_pts);
    }

    DynamicMessageInfo msginfo;
    ClientDynamicManager* g_climgr = t_thrd.wlm_cxt.thread_climgr;

    if (t_thrd.wlm_cxt.parctl_state.transact) {
        (void)dywlm_client_initmsg(g_climgr, &msginfo, PARCTL_TRANSACT);
    } else {
        (void)dywlm_client_initmsg(g_climgr, &msginfo, PARCTL_RESERVE);
    }

    if (IsQidInvalid(&msginfo.qid)) {
        return;
    }

    int ret = dywlm_client_wait(&msginfo);

    ereport(DEBUG3,
        (errmsg("ID[%u,%lu,%ld] -----DYWLM_client_reserve-----"
                "Memsize[current:%d, max:%d, min:%d]",
            msginfo.qid.procId,
            msginfo.qid.queryId,
            msginfo.qid.stamp,
            msginfo.memsize,
            msginfo.max_memsize,
            msginfo.min_memsize)));

    /* If return value does not match with the optimizer, we should report the current value. */
    if (msginfo.memsize > msginfo.max_memsize || msginfo.memsize < msginfo.min_memsize) {
        ereport(LOG,
            (errmsg("WARNING: ID[%u,%lu,%ld] return value not match in dywlm_client_reserve,"
                    "Memsize[current:%d, max:%d, min:%d]",
                msginfo.qid.procId,
                msginfo.qid.queryId,
                msginfo.qid.stamp,
                msginfo.memsize,
                msginfo.max_memsize,
                msginfo.min_memsize)));
    }

    g_wlm_params->memsize = msginfo.memsize;
    g_wlm_params->rpdata.act_pts = msginfo.actpts;

    if (ret == 0) {
        dywlm_info_unregister();
    } else {
        t_thrd.wlm_cxt.parctl_state.rp_reserve = 1;

        USE_CONTEXT_LOCK(&g_climgr->statement_list_mutex);

        g_climgr->running_count++;
    }

    /* Now the estimated memsize has been determined, the plan should be generated
     * For CalculateQueryMemMain() called by dywlm, we excute all 3 phases.
     */
    bool use_tenant = (g_wlm_params->rpdata.rpoid != DEFAULT_POOL_OID);

    if (queryDesc && (WLMGetQueryMem(queryDesc, isQueryDesc) > MEM_THRESHOLD * 1024)) {
        g_wlm_params->memsize = Max(Min(g_wlm_params->max_memsize, g_wlm_params->memsize), g_wlm_params->min_memsize);

        if (g_wlm_params->memsize != g_wlm_params->max_memsize) {
            if (isQueryDesc && queryDesc->plannedstmt != NULL) {
                queryDesc->plannedstmt->assigned_query_mem[1] = g_wlm_params->memsize * 1024;
            } else {
                ((UtilityDesc*)queryDesc)->assigned_mem = g_wlm_params->memsize * 1024;
            }
        }

        ereport(DEBUG3,
            (errmsg("REGENERATE PLAN[%d,%d,%d]",
                g_wlm_params->memsize,
                g_wlm_params->max_memsize,
                g_wlm_params->min_memsize)));
        if (isQueryDesc != false && queryDesc != NULL && queryDesc->plannedstmt) {
            CalculateQueryMemMain(queryDesc->plannedstmt, use_tenant, true);
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->estimate_memory =
                (unsigned int)WLMGetQueryMem(queryDesc, isQueryDesc) >> BITS_IN_KB;
        }
    }
}

/*
 * @Description: client release resource
 * @IN state: parctl state
 * @Return: void
 * @See also:
 */
void dywlm_client_release(ParctlState* state)
{
    /* If it has been released, ignore this time. */
    if (state == NULL || state->rp_release) {
        return;
    }

    ClientDynamicManager* g_climgr = t_thrd.wlm_cxt.thread_climgr;

    /*
     * It will release active statement if it has done
     * dynamic workload parallel control.
     */
    if (state->enqueue && state->rp_reserve &&
        ((IS_PGXC_COORDINATOR && !IsConnFromCoord()) || IS_SINGLE_NODE)) {
        dywlm_info_unregister();

        if (!state->errjmp) {
            DynamicMessageInfo msginfo;

            (void)dywlm_client_initmsg(g_climgr, &msginfo, PARCTL_RELEASE);

            EnqueueType etype = dywlm_client_send(g_climgr, &msginfo);

            ereport(DEBUG2,
                (errmsg("send qid [%u, %lu, %ld] to release, etype: %d",
                    msginfo.qid.procId,
                    msginfo.qid.queryId,
                    msginfo.qid.stamp,
                    etype)));
        }

        state->rp_release = 1;

        // remove the counting when query is terminated
        if (g_climgr->running_count > 0) {
            USE_CONTEXT_LOCK(&g_climgr->statement_list_mutex);
            g_climgr->running_count--;
        }
    }

    /* handle exception */
    if (state->except) {
        WLMResetStatInfo4Exception();
        state->except = 0;
    }
}

/*
 * @Description: client reserve message from the server
 * @IN msg: message string info
 * @Return: void
 * @See also:
 */
void dywlm_client_receive(StringInfo msg)
{
    int retcode = 0;

    DynamicMessageInfo recvinfo;

    errno_t errval = memset_s(&recvinfo, sizeof(recvinfo), 0, sizeof(recvinfo));
    securec_check_errval(errval, , LOG);

    /* parse message to message detail info */
    dywlm_client_parse_message(msg, &recvinfo);

    /* get the client manager based on node group name */
    WLMNodeGroupInfo* ng = WLMGetNodeGroupFromHTAB(recvinfo.groupname);

    if (NULL == ng) {
        ereport(LOG, (errmsg("failed to get the nodegroup %s from htab.", recvinfo.groupname)));
        dywlm_client_reply(DYWLM_NO_NGROUP);
        return;
    }

    if (recvinfo.qtype == PARCTL_SYNC) {
        /* reply all workload records to server */
        dywlm_client_reply_records(&ng->climgr);

        return;
    } else {
        if (strcmp(recvinfo.nodename, g_instance.attr.attr_common.PGXCNodeName) != 0) {
            ereport(LOG,
                (errmsg("node name %s received is not matched current node %s",
                    recvinfo.nodename,
                    g_instance.attr.attr_common.PGXCNodeName)));

            dywlm_client_reply(DYWLM_SEND_FAILED);
            return;
        }

        /* wake up the query to execute on the client */
        retcode = dywlm_client_post(&ng->climgr, &recvinfo);
    }

    ereport(DEBUG2, (errmsg("reply retcode: %d", retcode)));

    /* reply 'w' message to server */
    dywlm_client_reply(retcode);
}

/*
 * @Description: find statement by ThreadId
 * @IN tid: the ThreadId of statement
 * @Return: info node in the hash
 * @See also:
 */
DynamicInfoNode* dywlm_client_search_node(ClientDynamicManager* climgr, ThreadId tid)
{
    DynamicInfoNode* node = NULL;

    HASH_SEQ_STATUS hstat;
    /* search the statment hash table to find statement */
    hash_seq_init(&hstat, climgr->dynamic_info_hashtbl);

    USE_AUTO_LWLOCK(WorkloadStatHashLock, LW_SHARED);

    /* search hash to find thread id */
    while ((node = (DynamicInfoNode*)hash_seq_search(&hstat)) != NULL) {
        if (node->threadid == tid) {
            hash_seq_term(&hstat);
            break;
        }
    }

    return node;
}

/*
 * @Description: client handle move node to list
 * @IN  ng: the node group info
 *      qid: the qid of statement will be moved
 *      cgroup: new cgroup
 * @Return:
 * @See also:
 */
void dywlm_client_move_node_to_list(void* ng, uint64 tid, const char* cgroup)
{
    DynamicMessageInfo msginfo;
    WLMNodeGroupInfo* tmpng = (WLMNodeGroupInfo*)ng;

    errno_t errval = memset_s(&msginfo, sizeof(DynamicMessageInfo), 0, sizeof(DynamicMessageInfo));
    securec_check_errval(errval, , LOG);

    DynamicInfoNode* node = dywlm_client_search_node(&tmpng->climgr, tid);

    /* cannot find the statement in the hash */
    if (node == NULL) {
        ereport(LOG, (errmsg("statement %lu is not in waiting list", tid)));
        return;
    }

    msginfo.qid = node->qid;
    msginfo.priority = gscgroup_get_percent(tmpng, cgroup);
    msginfo.qtype = PARCTL_MVNODE;

    errval = strncpy_s(msginfo.groupname, sizeof(msginfo.groupname), node->ngroup, sizeof(msginfo.groupname) - 1);
    securec_check_errval(errval, , LOG);

    EnqueueType etype = dywlm_client_send(&tmpng->climgr, &msginfo);

    /* send message info the server */
    if (etype == ENQUEUE_RECOVERY) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("Central node is recovering now, please retry later.")));
    }

    if (etype == ENQUEUE_GROUPERR) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Central Node cannot get node group %s", msginfo.groupname)));
    }

    return;
}

/*
 * @Description: client handle jump priority
 * @IN  qid: the qid of statement will jump to highest priority
 * @Return:
 * @See also:
 */
int dywlm_client_jump_queue(ClientDynamicManager* climgr, ThreadId tid)
{
    if (!superuser()) {
        ereport(NOTICE, (errmsg("Only super user can change the queue.")));
        return 0;
    }

    DynamicMessageInfo msginfo;

    errno_t errval = memset_s(&msginfo, sizeof(DynamicMessageInfo), 0, sizeof(DynamicMessageInfo));
    securec_check_errval(errval, , LOG);

    DynamicInfoNode* node = dywlm_client_search_node(climgr, tid);

    /* cannot find the statement in the hash */
    if (node == NULL) {
        ereport(LOG, (errmsg("statement %lu in not in waiting list", tid)));
        return -1;
    }

    msginfo.qid = node->qid;
    msginfo.qtype = PARCTL_JPQUEUE;

    errval = strncpy_s(msginfo.groupname, sizeof(msginfo.groupname), node->ngroup, sizeof(msginfo.groupname) - 1);
    securec_check_errval(errval, , LOG);

    EnqueueType etype = dywlm_client_send(climgr, &msginfo);

    /* send message to server */
    if (etype == ENQUEUE_RECOVERY) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("Central node is recovering now, please retry later.")));
    }

    if (etype == ENQUEUE_GROUPERR) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Central node cannot get node group %s", msginfo.groupname)));
    }

    return 0;
}

/*
 * @Description: recover client enqueue list
 * @IN void
 * @Return: void
 * @See also:
 */
void dywlm_client_recover(ClientDynamicManager* climgr)
{
    if (climgr->recover) {
        return;
    }

    ereport(LOG, (errmsg("client recover start")));

    climgr->recover = true;

    /* restore the records */
    DynamicMessageInfo msginfo;

    (void)dywlm_client_initmsg(climgr, &msginfo, PARCTL_CLEAN);

    if (dywlm_client_send(climgr, &msginfo) == ENQUEUE_ERROR) {
        climgr->recover = false;
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("client recover failed")));
    }

    ereport(LOG, (errmsg("client recover end")));

    climgr->recover = false;
}

/*
 * @Description: get client physical info
 * @OUT total_mem: total physical memory size
 * @OUT free_mem: free physical memory size
 * @return: 0 success -1 failed
 * @See also:
 */
int dywlm_client_physical_info(int* total_mem, int* free_mem)
{
    FILE* fp = NULL;

    char buf[KBYTES];
    const char* file = "/proc/meminfo";

    /* open system file to read memory infomation */
    if ((fp = fopen(file, "rb")) == NULL) {
        return -1;
    }

    int count = 0;

    /* read each line */
    while (NULL != fgets(buf, KBYTES, fp)) {
        /* read total memory */
        if (strstr(buf, "MemTotal") != NULL) {
            char* p = strchr(buf, ':');

            if (p == NULL) {
                fclose(fp);
                return -1;
            }

            *total_mem = (int)strtol(p + 1, NULL, 10) / KBYTES;

            ++count;
        } else if (strstr(buf, "MemFree") != NULL) { /* read free memory */
            char* p = strchr(buf, ':');

            if (p == NULL) {
                fclose(fp);
                return -1;
            }

            *free_mem = (int)strtol(p + 1, NULL, 10) / KBYTES;

            ++count;
        }

        /* total memory and free memory get OK, finish reading */
        if (count == 2) {
            break;
        }
    }

    fclose(fp);

    return count - 2;
}

/*
 * @Description: get all records from server node
 * @OUT num: number of the records
 * @Return: records list
 * @See also:
 */
DynamicWorkloadRecord* dywlm_client_get_records(ClientDynamicManager* climgr, int* num)
{
    DynamicWorkloadRecord* records = NULL;

    PGXCNodeHandle* ccn_handle = NULL;
    PGXCNodeAllHandles* pgxc_handles = NULL;

    ServerDynamicManager* srvmgr = (ServerDynamicManager*)climgr->srvmgr;

    DynamicMessageInfo sendinfo;

    errno_t errval = memset_s(&sendinfo, sizeof(sendinfo), 0, sizeof(sendinfo));
    securec_check_errval(errval, , LOG);

    sendinfo.qtype = PARCTL_SYNC;

    errval = strncpy_s(sendinfo.nodename,
        sizeof(sendinfo.nodename),
        g_instance.attr.attr_common.PGXCNodeName,
        sizeof(sendinfo.nodename) - 1);
    securec_check_errval(errval, , LOG);
    errval =
        strncpy_s(sendinfo.groupname, sizeof(sendinfo.groupname), srvmgr->group_name, sizeof(sendinfo.groupname) - 1);
    securec_check_errval(errval, , LOG);

    struct timeval timeout = {300, 0};

    int codeidx = 0;

    /* connect to server with message info */
    if (!dywlm_client_connect(&pgxc_handles, &ccn_handle, &codeidx, &sendinfo)) {
        release_pgxc_handles(pgxc_handles);
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("[DYWLM] Failed to send dynamic workload params to CCN!")));
    }

    /* current node is server, get records directly */
    if (sendinfo.isserver) {
        return dywlm_server_get_records(srvmgr, sendinfo.nodename, num);
    }

    int retcode = 0;
    int offset = 0;

    bool hasError = false;
    bool isFinished = false;

    /* receive message */
    for (;;) {
        if (pgxc_node_receive(1, &ccn_handle, &timeout)) {
            ereport(LOG,
                (errmsg("%s:%d recv fail", __FUNCTION__, __LINE__)));
            break;
        }

        char* msg = NULL;
        int len;
        char msg_type;

        msg_type = get_message(ccn_handle, &len, &msg);

        switch (msg_type) {
            case '\0': /* message is not completed */
            case 'E':  /* message is error */
                hasError = true;
                break;
            case 'n': /* the number of records */
            {
                errno_t errval = memcpy_s(&retcode, sizeof(int), msg, 4);
                securec_check_errval(errval, , LOG);
                retcode = (int)ntohl(retcode);

                if (retcode != 0) {
                    records = (DynamicWorkloadRecord*)palloc0_noexcept(retcode * sizeof(DynamicWorkloadRecord));

                    if (records == NULL) {
                        ccn_handle->state = DN_CONNECTION_STATE_IDLE;
                        release_pgxc_handles(pgxc_handles);
                        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory of current node.")));
                    }
                }

                break;
            }
            case 'r': /* get workload record */
            {
                /* in a while loop, first it will be case n, and then it will enter case r, so the records will not be null */
                if (records == NULL) {
                    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("records is NULL.")));
                }
                if (offset >= retcode) {
                    ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory of records.")));
                }
                DynamicWorkloadRecord* record = records + offset;

                StringInfoData input_msg;
                initStringInfo(&input_msg);

                appendBinaryStringInfo(&input_msg, msg, len);

                /* parse message to get record */
                record->qid.procId = (Oid)pq_getmsgint(&input_msg, 4);
                record->qid.queryId = (uint64)pq_getmsgint64(&input_msg);
                record->qid.stamp = (TimestampTz)pq_getmsgint64(&input_msg);
                record->memsize = pq_getmsgint(&input_msg, 4);
                record->max_memsize = pq_getmsgint(&input_msg, 4);
                record->min_memsize = pq_getmsgint(&input_msg, 4);
                record->actpts = pq_getmsgint(&input_msg, 4);
                record->max_actpts = pq_getmsgint(&input_msg, 4);
                record->min_actpts = pq_getmsgint(&input_msg, 4);
                record->maxpts = pq_getmsgint(&input_msg, 4);
                record->priority = pq_getmsgint(&input_msg, 4);
                record->qtype = (ParctlType)pq_getmsgint(&input_msg, 4);

                errno_t errval = strncpy_s(
                    record->rpname, sizeof(record->rpname), pq_getmsgstring(&input_msg), sizeof(record->rpname) - 1);
                securec_check_errval(errval, , LOG);
                errval = strncpy_s(
                    record->nodename, sizeof(record->rpname), pq_getmsgstring(&input_msg), sizeof(record->rpname) - 1);
                securec_check_errval(errval, , LOG);
                errval = strncpy_s(record->groupname,
                    sizeof(record->groupname),
                    pq_getmsgstring(&input_msg),
                    sizeof(record->groupname) - 1);
                securec_check_errval(errval, , LOG);

                pq_getmsgend(&input_msg);

                pfree(input_msg.data);

                ++offset;

                break;
            }
            case 'f': /* message is completed */
            {
                errno_t errval = memcpy_s(num, sizeof(int), msg, 4);
                securec_check_errval(errval, , LOG);
                *num = (int)ntohl(*num);
                ereport(DEBUG2, (errmsg("finish receive record count: %d", *num)));
                if (retcode != *num) {
                    ereport(ERROR,
                        (errcode(ERRCODE_INTERNAL_ERROR), errmsg("dynamic workload record number is not correct!")));
                }

                isFinished = true;
                break;
            }
            case 'Z':
                if (hasError) {
                    isFinished = true;
                }
                break;
            case 'A': /* NotificationResponse */
            case 'S': /* SetCommandComplete */
            {
                /*
                 * Ignore these to prevent multiple messages, one from
                 * each node. Coordinator will send on for DDL anyway
                 */
                break;
            }
            default:
                break;
        }

        /* error occurred or receive finished, stop looping */
        if (isFinished) {
            break;
        }
    }

    ccn_handle->state = DN_CONNECTION_STATE_IDLE;
    release_pgxc_handles(pgxc_handles);

    return records;
}

bool dywlm_is_server_or_single_node()
{
    if (PgxcIsCentralCoordinator(g_instance.attr.attr_common.PGXCNodeName) ||
        IS_SINGLE_NODE) {
        return true;
    }

    return false;
}

/*
 * @Description: handle of getting all records
 * @IN num: entry count of the hash table
 * @Return: records list
 * @See also:
 */
DynamicWorkloadRecord* dywlm_get_records(int* num)
{
    *num = 0;
    if (!g_instance.wlm_cxt->dynamic_workload_inited) {
        return NULL;
    }

    int count = 100;  // default count
    int tmpnum = 0;
    errno_t rc = 0;
    bool flag = false;
    DynamicWorkloadRecord* tmprecord = NULL;
    DynamicWorkloadRecord* array = (DynamicWorkloadRecord*)palloc0(count * sizeof(DynamicWorkloadRecord));

    /* check whether current node is server */
    if (dywlm_is_server_or_single_node()) {
        tmprecord = dywlm_server_get_records(
            &g_instance.wlm_cxt->MyDefaultNodeGroup.srvmgr, g_instance.attr.attr_common.PGXCNodeName, num);
    } else {
        tmprecord = dywlm_client_get_records(&g_instance.wlm_cxt->MyDefaultNodeGroup.climgr, num);
    }

    if (*num > 0 && tmprecord != NULL) {
        while (*num >= count) {
            count *= 2;
            flag = true;
        }

        if (flag) {
            array = (DynamicWorkloadRecord*)repalloc(array, count * sizeof(DynamicWorkloadRecord));
        }

        rc = memcpy_s(array, count * sizeof(DynamicWorkloadRecord), tmprecord, *num * sizeof(DynamicWorkloadRecord));
        securec_check(rc, "\0", "\0");
    }

    if (tmprecord != NULL) {
        pfree_ext(tmprecord);
    }

    USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_SHARED);

    WLMNodeGroupInfo* hdata = NULL;

    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.node_group_hashtbl);

    while ((hdata = (WLMNodeGroupInfo*)hash_seq_search(&hash_seq)) != NULL) {
        /* skip unused node group and elastic group */
        if (!hdata->used || pg_strcasecmp(VNG_OPTION_ELASTIC_GROUP, hdata->group_name) == 0) {
            continue;
        }

        /* check whether current node is server */
        if (dywlm_is_server_or_single_node()) {
            tmprecord = dywlm_server_get_records(&hdata->srvmgr, g_instance.attr.attr_common.PGXCNodeName, &tmpnum);
        } else {
            tmprecord = dywlm_client_get_records(&hdata->climgr, &tmpnum);
        }

        if (tmpnum > 0 && tmprecord != NULL) {
            while ((*num + tmpnum) >= count) {
                count *= 2;
                flag = true;
            }

            if (flag) {
                array = (DynamicWorkloadRecord*)repalloc(array, count * sizeof(DynamicWorkloadRecord));
            }

            rc = memcpy_s(array + *num * sizeof(DynamicWorkloadRecord), (count - *num) * sizeof(DynamicWorkloadRecord),
                          tmprecord, tmpnum * sizeof(DynamicWorkloadRecord));
            securec_check(rc, "\0", "\0");

            *num += tmpnum;
        }

        if (tmprecord != NULL) {
            pfree_ext(tmprecord);
        }
    }

    return array;
}

/*
 * @Description: client dynamic workload manager
 * @IN void
 * @Return: void
 * @See also:
 */
void dywlm_client_manager(QueryDesc* queryDesc, bool isQueryDesc)
{
    bool force_control = false;

    u_sess->wlm_cxt->forced_running = false;

    if (t_thrd.wlm_cxt.parctl_state.subquery == 1) {
        t_thrd.wlm_cxt.parctl_state.rp_reserve = 0;
        t_thrd.wlm_cxt.parctl_state.rp_release = 0;

        t_thrd.wlm_cxt.parctl_state.release = 0;

        WLMSetCollectInfoStatus(WLM_STATUS_PENDING);
    }

    if (t_thrd.wlm_cxt.collect_info->status == WLM_STATUS_RUNNING) {
        return;
    }

    if (u_sess->wlm_cxt->is_active_statements_reset) {
        t_thrd.wlm_cxt.parctl_state.rp_reserve = 0;
        t_thrd.wlm_cxt.parctl_state.rp_release = 0;

        t_thrd.wlm_cxt.parctl_state.release = 0;

        WLMSetCollectInfoStatus(WLM_STATUS_PENDING);
    }

    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    /* get the user info */
    if (u_sess->attr.attr_resource.enable_force_memory_control && OidIsValid(g_wlm_params->rpdata.rpoid) &&
        (g_wlm_params->rpdata.rpoid != DEFAULT_POOL_OID) && (g_wlm_params->rpdata.mem_size > SIMPLE_THRESHOLD)) {
        UserData* userdata = GetUserDataFromHTab(GetUserId(), false);

        int memsize = (unsigned int)g_wlm_params->rpdata.mem_size >> BITS_IN_KB;

        if (userdata && (userdata->memsize > memsize)) {
            ereport(LOG,
                (errmsg("user used memory is %d MB, resource pool memory is %d MB. "
                        "So to control the simple query now.",
                    userdata->memsize,
                    memsize)));
            force_control = true;
        }
    }

    t_thrd.wlm_cxt.parctl_state.simple = WLMIsSimpleQuery(queryDesc, force_control, isQueryDesc) ? 1 : 0;
    /* update transact state for begin...end query */
    t_thrd.wlm_cxt.parctl_state.transact = IsTransactionBlock() ? 1 : 0;
    t_thrd.wlm_cxt.parctl_state.except = 1;
    t_thrd.wlm_cxt.parctl_state.enqueue =
        (IsQueuedSubquery() && t_thrd.wlm_cxt.parctl_state.special == 0 && !g_wlm_params->rpdata.superuser)
            ? 1
            : t_thrd.wlm_cxt.parctl_state.enqueue;

    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        g_wlm_params->qid.procId = u_sess->pgxc_cxt.PGXCNodeId;
    }

    if (g_instance.wlm_cxt->gscgroup_init_done && !IsAbortedTransactionBlockState()) {
        WLMSwitchCGroup();
    }

    if (COMP_ACC_CLUSTER && WLMGetInComputePool(queryDesc, isQueryDesc)) {
        t_thrd.wlm_cxt.parctl_state.simple = 0;
        t_thrd.wlm_cxt.parctl_state.enqueue = 1;
        u_sess->wlm_cxt->parctl_state_control = 1;

        if (IS_PGXC_COORDINATOR) {
            WLMSetCollectInfoStatus(WLM_STATUS_PENDING);
        }
    }

    if (IS_PGXC_DATANODE) {
        WLMSetCollectInfoStatus(WLM_STATUS_PENDING);
    }

    /* "in_compute_pool == true" means we are in the compute pool. */
    if (IS_PGXC_COORDINATOR && ((COMP_ACC_CLUSTER && WLMGetInComputePool(queryDesc, isQueryDesc)) ||
                                   (!COMP_ACC_CLUSTER && !IsConnFromCoord()))) {
        if (g_instance.wlm_cxt->dynamic_workload_inited && (t_thrd.wlm_cxt.parctl_state.simple == 0)) {
            WLMHandleDywlmSimpleExcept(false);
            dywlm_client_reserve(queryDesc, isQueryDesc);
        } else {
            WLMParctlReserve(PARCTL_RESPOOL);
        }
    }

    if (acce_not_enough_resource) {
        acce_not_enough_resource = false;
        WLMSetCollectInfoStatus(WLM_STATUS_ABORT);

        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("Can NOT get enough resource for this request.")));
    }

    WLMSetCollectInfoStatus(WLM_STATUS_RUNNING);
}

/*
 * @Description: client dynamic workload init all fields
 * @IN void
 * @Return: void
 * @See also:
 */
void dywlm_client_init(WLMNodeGroupInfo* info)
{
    ClientDynamicManager* climgr = &info->climgr;

    HASHCTL hash_ctl;

    /* memory size we will use */
    const int size = 1; /* MB */

    errno_t errval = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check_errval(errval, , LOG);

    hash_ctl.keysize = sizeof(Qid);
    hash_ctl.hcxt = g_instance.wlm_cxt->workload_manager_mcxt; /* use workload manager memory context */
    hash_ctl.entrysize = sizeof(DynamicInfoNode);
    hash_ctl.hash = WLMHashCode;
    hash_ctl.match = WLMHashMatch;
    hash_ctl.alloc = WLMAlloc0NoExcept4Hash;
    hash_ctl.dealloc = pfree;

    climgr->dynamic_info_hashtbl = hash_create("dywlm register hash table",
        WORKLOAD_STAT_HASH_SIZE,
        &hash_ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_COMPARE | HASH_ALLOC | HASH_DEALLOC);

    climgr->active_statements = 0;

    climgr->max_info_count = size * MBYTES / sizeof(DynamicInfoNode);

    climgr->max_active_statements = u_sess->attr.attr_resource.max_active_statements;

    climgr->max_support_statements =
        t_thrd.utils_cxt.gs_mp_inited
            ? (MAX_PARCTL_MEMORY * PARCTL_ACTIVE_PERCENT) / (FULL_PERCENT * g_instance.wlm_cxt->parctl_process_memory)
            : 0;

    climgr->max_statements = t_thrd.utils_cxt.gs_mp_inited
                                 ? (MAX_PARCTL_MEMORY * DYWLM_HIGH_QUOTA / FULL_PERCENT -
                                       climgr->max_active_statements * g_instance.wlm_cxt->parctl_process_memory) /
                                       PARCTL_MEMORY_UNIT
                                 : 0;

    climgr->current_support_statements = 0;

    climgr->central_waiting_count = 0;

    climgr->usedsize = 0;

    climgr->cluster = -1;  // unused

    climgr->freesize = 0;

    climgr->recover = false;

    climgr->group_name = info->group_name;

    climgr->srvmgr = (void*)(&info->srvmgr);

    climgr->statements_waiting_list = NULL;

    (void)pthread_mutex_init(&climgr->statement_list_mutex, NULL);
}

/*
 * @Description: client dynamic verfify resgister info
 * @IN void
 * @Return: void
 * @See also:
 */
void dywlm_client_verify_register_internal(ClientDynamicManager* climgr)
{
    USE_AUTO_LWLOCK(WorkloadStatHashLock, LW_SHARED);

    int i = 0;
    int count = hash_get_num_entries(climgr->dynamic_info_hashtbl);

    ereport(DEBUG3, (errmsg("WLMmonitorMain: dywlm_client_verify_register_internal: "
            "length of climgr->dynamic_info_hashtbl: %d from NG: %s", count, climgr->group_name)));

    if (count == 0) {
        return;
    }

    DynamicInfoNode* nodes = (DynamicInfoNode*)palloc0(count * sizeof(DynamicInfoNode));

    DynamicInfoNode* info = NULL;

    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, climgr->dynamic_info_hashtbl);

    while ((info = (DynamicInfoNode*)hash_seq_search(&hash_seq)) != NULL) {
        nodes[i++] = *info;
    }

    count = i;

    RELEASE_AUTO_LWLOCK();

    uint32 num_backends = 0;
    PgBackendStatusNode* node = gs_stat_read_current_status(&num_backends);

    for (i = 0; i < count; ++i) {
        uint32 j = 0;
        PgBackendStatusNode* tmpNode = node;
        if (nodes[i].threadid > 0) {
            while (tmpNode != NULL) {
                PgBackendStatus* beentry = tmpNode->data;
                tmpNode = tmpNode->next;
                /*
                 * If the backend thread is valid and the state
                 * is not idle or undefined, we treat it as a active thread.
                 */
                if (beentry != NULL && beentry->st_sessionid == nodes[i].qid.queryId &&
                    beentry->st_block_start_time == nodes[i].qid.stamp) {
                    break;
                }

                j++;
            }
        }

        if (nodes[i].threadid == 0 || j >= num_backends) {
            USE_AUTO_LWLOCK(WorkloadStatHashLock, LW_EXCLUSIVE);
            ereport(DEBUG1, (errmsg("VERIFY: remove job info from client record, qid:[%u, %lu, %ld]",
                                    nodes[i].qid.procId, nodes[i].qid.queryId, nodes[i].qid.stamp)));
            hash_search(climgr->dynamic_info_hashtbl, &nodes[i].qid, HASH_REMOVE, NULL);
        }
    }

    pfree(nodes);
}

/*
 * @Description: client dynamic verfify resgister info
 * @IN void
 * @Return: void
 * @See also:
 */
void dywlm_client_verify_register(void)
{

    dywlm_client_verify_register_internal(&g_instance.wlm_cxt->MyDefaultNodeGroup.climgr);

    USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_SHARED);

    WLMNodeGroupInfo* hdata = NULL;

    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.node_group_hashtbl);

    while ((hdata = (WLMNodeGroupInfo*)hash_seq_search(&hash_seq)) != NULL) {
        /* skip unused node group and elastic group */
        if (!hdata->used || pg_strcasecmp(VNG_OPTION_ELASTIC_GROUP, hdata->group_name) == 0) {
            continue;
        }

        dywlm_client_verify_register_internal(&hdata->climgr);
    }

    pgstat_reset_current_status();

}

/*
 * @Description: get cpu count
 * @IN void
 * @Return: min cpu count
 * @See also:
 */
int dywlm_get_cpu_count(void)
{
    USE_AUTO_LWLOCK(WorkloadIOUtilLock, LW_SHARED);

    return g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.cpu_count;
}

/*
 * @Description: get cpu util
 * @IN void
 * @Return: max cpu count
 * @See also:
 */
int dywlm_get_cpu_util(void)
{
    USE_AUTO_LWLOCK(WorkloadIOUtilLock, LW_SHARED);

    return (int)g_instance.wlm_cxt->io_context.WLMmonitorDeviceStat.cpu_util;
}

/*
 * @Description: get active statement count of complicated query
 * @IN void
 * @Return: active statement count
 * @See also:
 */
int dywlm_get_active_statement_count(void)
{
    return t_thrd.wlm_cxt.thread_climgr->running_count;
}

/*
 * @Description: display the climgr structure for one node group
 * @IN void
 * @Return: Void
 * @See also:
 */
void dywlm_client_display_climgr_info_internal(ClientDynamicManager* climgr, StringInfo strinfo)
{
    WLMContextLock list_lock(&climgr->statement_list_mutex);

    list_lock.Lock();

    /* print client dynamic workload state debug info */
    appendStringInfo(strinfo,
        _("active_statements[%d], central_waiting_count[%d], max_active_statements[%d], running_count[%d], "
          "max_info_count[%d], max_support_statements[%d], current_support_statements[%d], max_statements[%d], "
          "freesize of DN [%d], usedsize in respool [%d], in recover state[%d], group_name[%s].\n"),
        climgr->active_statements,
        climgr->central_waiting_count,
        climgr->max_active_statements,
        climgr->running_count,
        climgr->max_info_count,
        climgr->max_support_statements,
        climgr->current_support_statements,
        climgr->max_statements,
        climgr->freesize,
        climgr->usedsize,
        climgr->recover,
        climgr->group_name);

    DynamicInfoNode* info = NULL;

    int i = 0;

    foreach_cell(cell, climgr->statements_waiting_list)
    {
        info = (DynamicInfoNode*)lfirst(cell);

        if (info != NULL) {
            appendStringInfo(strinfo,
                _("Waiting Global number[%d]: qid.procId[%u], qid.queryId[%lu], qid.stamp[%ld], threadid[%lu], "
                  "memsize[%d], max_memsize[%d], min_memsize[%d], priority[%d], "
                  "actpts[%d], max_actpts[%d], min_actpts[%d], maxpts[%d], rpname[%s], ngroup[%s]\n"),
                i++,
                info->qid.procId,
                info->qid.queryId,
                info->qid.stamp,
                info->threadid,
                info->memsize,
                info->max_memsize,
                info->min_memsize,
                info->priority,
                info->actpts,
                info->max_actpts,
                info->min_actpts,
                info->maxpts,
                info->rpname,
                info->ngroup);
        }
    }

    list_lock.UnLock();

    USE_AUTO_LWLOCK(WorkloadStatHashLock, LW_SHARED);

    int waitcnt = 0;
    int runcnt = 0;

    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, climgr->dynamic_info_hashtbl);

    /* scan all records from the client cache for current node */
    while ((info = (DynamicInfoNode*)hash_seq_search(&hash_seq)) != NULL) {

        if (info->wakeup) {
            appendStringInfo(strinfo,
                _("Running number[%d]: qid.procId[%u], qid.queryId[%lu], qid.stamp[%ld], threadid[%lu], "
                  "memsize[%d], max_memsize[%d], min_memsize[%d], priority[%d], is_dirty[%d], "
                  "actpts[%d], max_actpts[%d], min_actpts[%d], maxpts[%d], rpname[%s], ngroup[%s]\n"),
                runcnt++,
                info->qid.procId,
                info->qid.queryId,
                info->qid.stamp,
                info->threadid,
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
                info->ngroup);
        } else {
            appendStringInfo(strinfo,
                _("Waiting Central number[%d]: qid.procId[%u], qid.queryId[%lu], qid.stamp[%ld], threadid[%lu], "
                  "memsize[%d], max_memsize[%d], min_memsize[%d], priority[%d], is_dirty[%d], "
                  "actpts[%d], max_actpts[%d], min_actpts[%d], maxpts[%d], rpname[%s], ngroup[%s]\n"),
                waitcnt++,
                info->qid.procId,
                info->qid.queryId,
                info->qid.stamp,
                info->threadid,
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
                info->ngroup);
        }
    }
}

/*
 * @Description: display the climgr structure for all node groups
 * @IN void
 * @Return: Void
 * @See also:
 */
void dywlm_client_display_climgr_info(StringInfo strinfo)
{
    appendStringInfo(
        strinfo, _("NodeGroup [%s] dynamic client info:\n"), g_instance.wlm_cxt->MyDefaultNodeGroup.group_name);

    dywlm_client_display_climgr_info_internal(&g_instance.wlm_cxt->MyDefaultNodeGroup.climgr, strinfo);

    USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_SHARED);

    WLMNodeGroupInfo* hdata = NULL;

    HASH_SEQ_STATUS hash_seq;
    hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.node_group_hashtbl);

    while ((hdata = (WLMNodeGroupInfo*)hash_seq_search(&hash_seq)) != NULL) {
        /* skip unused node group and elastic group */
        if (!hdata->used || pg_strcasecmp(VNG_OPTION_ELASTIC_GROUP, hdata->group_name) == 0) {
            continue;
        }

        appendStringInfo(strinfo, _("NodeGroup [%s] dynamic client info:\n"), hdata->group_name);

        dywlm_client_display_climgr_info_internal(&hdata->climgr, strinfo);
    }
}
