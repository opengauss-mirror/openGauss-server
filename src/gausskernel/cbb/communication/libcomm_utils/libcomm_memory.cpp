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
 * libcomm_memory.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/libcomm_utils/libcomm_memory.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <libcgroup.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <net/if.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <sys/param.h>
#include <sys/time.h>
#include <unistd.h>

#include "../libcomm_core/mc_tcp.h"
#include "../libcomm_core/mc_poller.h"
#include "../libcomm_utils/libcomm_thread.h"
#include "../libcomm_common.h"
#include "libcomm_lqueue.h"
#include "libcomm_queue.h"
#include "libcomm_lock_free_queue.h"
#include "distributelayer/streamCore.h"
#include "distributelayer/streamProducer.h"
#include "pgxc/poolmgr.h"
#include "libpq/auth.h"
#include "libpq/pqsignal.h"
#include "storage/ipc.h"
#include "utils/ps_status.h"
#include "utils/dynahash.h"

#include "vecexecutor/vectorbatch.h"
#include "vecexecutor/vecnodes.h"
#include "executor/exec/execStream.h"
#include "miscadmin.h"
#include "gssignal/gs_signal.h"
#include "pgxc/pgxc.h"

#ifdef ENABLE_UT
#define static
#endif


#define STREAM_SCAN_FINISH 'F'
#define STREAM_SCAN_WAIT 'W'
#define STREAM_SCAN_DATA 'D'

extern bool executorEarlyStop();

/* release memory of communication layer, just for LLT */
int gs_release_comm_memory()
{
    AutoContextSwitch commContext(g_instance.comm_cxt.comm_global_mem_cxt);
    gs_r_release_comm_memory();
    return 0;
}

/*
 * @Description: Add stream key to g_r_htab_nodeid_skey_to_memory_poll
 *
 * @param[IN] key_s: stream key
 */
void gs_memory_init_entry(StreamSharedContext* sharedContext, int consumerNum, int producerNum)
{
    struct hash_entry* entry = NULL;
    struct hash_entry** poll_entrys = NULL;
    struct hash_entry*** quota_entrys = NULL;

    poll_entrys = (struct hash_entry**)palloc(sizeof(struct hash_entry*) * consumerNum);
    quota_entrys = (struct hash_entry***)palloc(sizeof(struct hash_entry**) * consumerNum);

    for (int i = 0; i < consumerNum; i++) {
        entry = (struct hash_entry*)palloc(sizeof(struct hash_entry));
        (void)entry->_init();
        poll_entrys[i] = entry;
        quota_entrys[i] = (struct hash_entry**)palloc(sizeof(struct hash_entry*) * producerNum);
        for (int j = 0; j < producerNum; j++) {
            entry = (struct hash_entry*)palloc(sizeof(struct hash_entry));
            (void)entry->_init();
            quota_entrys[i][j] = entry;
        }
    }

    sharedContext->poll_entrys = poll_entrys;
    sharedContext->quota_entrys = quota_entrys;
}

/*
 * @Description: Send Error/Notice through memory
 *
 * @param[IN] buf: Error/Notice string info
 * @param[IN] sharedContext: context for shared memory stream
 * @param[IN] nthChannel: destination consumer
 */
void gs_message_by_memory(StringInfo buf, StreamSharedContext* sharedContext, int nthChannel)
{
    StringInfo buf_dst = NULL;
    struct hash_entry* entry = NULL;

    /* Copy Error/Notice messages to shared context. */
    buf_dst = sharedContext->messages[nthChannel][u_sess->stream_cxt.smp_id];

    /*
     * If producer is waked up and shared buffer has been consumed while waiting,
     * it can continue to append data to its messages of sharedContext.
     */
    entry = sharedContext->quota_entrys[nthChannel][u_sess->stream_cxt.smp_id];
    while (buf_dst->len > 0) {
        (void)entry->_timewait(SINGLE_WAITQUOTA);
    }
    appendBinaryStringInfo(buf_dst, buf->data, buf->len);
    buf_dst->cursor = buf->cursor;

    /* Send signal to dest consumer. */
    entry = sharedContext->poll_entrys[nthChannel];
    entry->_signal();

    pfree(buf->data);
    buf->data = NULL;
}

void gs_memory_disconnect(StreamSharedContext* sharedContext, int nthChannel)
{
    struct hash_entry* entry = NULL;
    sharedContext->dataStatus[nthChannel][u_sess->stream_cxt.smp_id] = CONN_ERR;
    entry = sharedContext->poll_entrys[nthChannel];
    entry->_signal();
}

#ifdef __aarch64__
/*
 * @Description: Judge whether the databuff is empty
 *
 * @param[IN] sharedContext: context for shared memory stream
 * @param[IN] nthChannel: destination consumer
 */
bool gs_is_databuff_empty(StreamSharedContext* sharedContext, int nthChannel)
{
    if (sharedContext->vectorized) {
        VectorBatch* batch = sharedContext->sharedBatches[nthChannel][u_sess->stream_cxt.smp_id];
        if (batch->m_rows == 0) {
            return true;
        }
    } else {
        TupleVector* tupleVec = sharedContext->sharedTuples[nthChannel][u_sess->stream_cxt.smp_id];
        if (tupleVec->tuplePointer == 0) {
            return true;
        }
    }
    return false;
}
#endif

/*
 * @Description: Send data to local consumer through shared memory
 *
 * @param[IN] tuple: tuple to be sent
 * @param[IN] batchsrc: batch to be send
 * @param[IN] sharedContext: context for shared memory stream
 * @param[IN] nthChannel: destination consumer
 * @param[IN] nthRow: the Nth row to be sent in batch
 */
void gs_memory_send(
    TupleTableSlot* tuple, VectorBatch* batchsrc, StreamSharedContext* sharedContext, int nthChannel, int nthRow)
{
    VectorBatch* batch = NULL;
    TupleVector* tupleVec = NULL;
    bool ready_to_send = false;
    DataStatus dataStatus;
    struct hash_entry* entry = NULL;

    WaitState oldStatus = pgstat_report_waitstatus_comm(STATE_WAIT_FLUSH_DATA,
        u_sess->pgxc_cxt.PGXCNodeId,
        -1,
        u_sess->stream_cxt.producer_obj->getParentPlanNodeId(),
        global_node_definition ? global_node_definition->num_nodes : -1);

    StreamTimeSendStart(t_thrd.pgxc_cxt.GlobalNetInstr);
    entry = sharedContext->quota_entrys[nthChannel][u_sess->stream_cxt.smp_id];
    for (;;) {
        /* Check for interrupt at the beginning of the loop. */
        CHECK_FOR_INTERRUPTS();

        /* Check if we should early stop. */
        /* Quit if the connection close, especially in a early close case. */
        if (executorEarlyStop() || sharedContext->is_connect_end[nthChannel][u_sess->stream_cxt.smp_id]) {
            (void)pgstat_report_waitstatus(oldStatus);
            return;
        }

        dataStatus = sharedContext->dataStatus[nthChannel][u_sess->stream_cxt.smp_id];
        /* Break the loop if we find quota. */
        if ((dataStatus == DATA_EMPTY
#ifdef __aarch64__
             && gs_is_databuff_empty(sharedContext, nthChannel)
#endif
             ) ||
            dataStatus == DATA_PREPARE) {
            break;
        }

        StreamTimeWaitQuotaStart(t_thrd.pgxc_cxt.GlobalNetInstr);
        (void)entry->_timewait(SINGLE_WAITQUOTA);
        StreamTimeWaitQuotaEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
    }

    StreamTimeCopyStart(t_thrd.pgxc_cxt.GlobalNetInstr);
    /* Copy data to shared context. */
    if (sharedContext->vectorized) {
        Assert(sharedContext->sharedBatches != NULL);
        batch = sharedContext->sharedBatches[nthChannel][u_sess->stream_cxt.smp_id];
        /* data copy */
        if (-1 == nthRow) {
            /* Do deep copy of all rows, for local roundrobin & local broadcast. */
            Assert(batch->m_rows == 0);
            batch->Copy<true, false>(batchsrc);
            ready_to_send = true;
        } else {
            batch->CopyNth(batchsrc, nthRow);
            if (BatchMaxSize == batch->m_rows) {
                ready_to_send = true;
            }
        }
    } else {
        Assert(sharedContext->sharedTuples != NULL);
        tupleVec = sharedContext->sharedTuples[nthChannel][u_sess->stream_cxt.smp_id];
        int n = tupleVec->tuplePointer;
        ExecCopySlot(tupleVec->tupleVector[n], tuple);
        tupleVec->tuplePointer++;
        if (TupleVectorMaxSize == tupleVec->tuplePointer) {
            ready_to_send = true;
        }
    }
    StreamTimeCopyEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

    /* send the signal if copy finished */
    if (ready_to_send) {
#ifdef __aarch64__
        pg_memory_barrier();
#endif
        /* set flag */
        sharedContext->dataStatus[nthChannel][u_sess->stream_cxt.smp_id] = DATA_READY;
        /* send signal */
        entry = sharedContext->poll_entrys[nthChannel];
        entry->_signal();
    } else {
        sharedContext->dataStatus[nthChannel][u_sess->stream_cxt.smp_id] = DATA_PREPARE;
    }
    StreamTimeSendEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

    (void)pgstat_report_waitstatus(oldStatus);
}

/*
 * @Description: catch a tuple from stream's buffer.
 *
 * @param[IN] node: stream state
 * @return bool: true -- found data
 */
FORCE_INLINE
bool gs_return_tuple(StreamState* node)
{
    TupleVector* tupleVec = node->tempTupleVec;

    if (tupleVec->tuplePointer == tupleVec->tupleCount) {
        return false;
    }

    int n = tupleVec->tuplePointer;
    node->ss.ps.ps_ResultTupleSlot = tupleVec->tupleVector[n];
    tupleVec->tuplePointer++;

    return true;
}

/*
 * @Description: Consume the data in shared memory from local producers.
 *
 * @param[IN] node: stream state
 * @param[IN] loc: data location
 * @return bool: true -- found data
 */
bool gs_consume_memory_data(StreamState* node, int loc)
{
    StreamSharedContext* sharedContext = node->sharedContext;

    NetWorkTimeCopyStart(t_thrd.pgxc_cxt.GlobalNetInstr);
    /* Take data from the shared context. */
    if (sharedContext->vectorized) {
        VectorBatch* batchsrc = sharedContext->sharedBatches[u_sess->stream_cxt.smp_id][loc];
        VectorBatch* batchdst = ((VecStreamState*)node)->m_CurrentBatch;

        if (batchsrc->m_rows == 0) {
            return false;
        }

        batchdst->Copy<true, false>(batchsrc);

        batchsrc->Reset();
    } else {
        TupleVector* tuplesrc = sharedContext->sharedTuples[u_sess->stream_cxt.smp_id][loc];
        TupleVector* tupledst = node->tempTupleVec;

        if (tuplesrc->tuplePointer == 0) {
            return false;
        }

        for (int i = 0; i < tuplesrc->tuplePointer; i++) {
            (void)ExecCopySlot(tupledst->tupleVector[i], tuplesrc->tupleVector[i]);
        }

        tupledst->tupleCount = tuplesrc->tuplePointer;
        tupledst->tuplePointer = 0;
        tuplesrc->tuplePointer = 0;
        (void)gs_return_tuple(node);
    }
    NetWorkTimeCopyEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

    struct hash_entry* entry = NULL;
    entry = sharedContext->quota_entrys[u_sess->stream_cxt.smp_id][loc];

#ifdef __aarch64__
    pg_memory_barrier();
#endif
    /* Reset flag */
    sharedContext->dataStatus[u_sess->stream_cxt.smp_id][loc] = DATA_EMPTY;

    /* send signal */
    entry->_signal();

    node->sharedContext->scanLoc[u_sess->stream_cxt.smp_id] = loc;
    return true;
}

/*
 * @Description: Scan the producer status to find the data.
 *
 * @param[IN] node: stream state
 * @return char: STREAM_SCAN_DATA -- successfully find data from producer.
 *                  STREAM_SCAN_WAIT -- still need to poll to wait for data.
 *                 STREAM_SCAN_FINISH -- stream scan finished.
 */
char gs_find_memory_data(StreamState* node, int* waitnode_count)
{
    DataStatus dataStatus;
    StringInfo buf = NULL;
    int scanLoc = node->sharedContext->scanLoc[u_sess->stream_cxt.smp_id];
    int i = scanLoc;
    bool finished = true;
    bool is_conn_end = false;
    int waitnodeCount = 0;
    struct hash_entry* entry = NULL;

    /* Check if there is available data, and scan from last time location. */
    do {
        i++;
        if (i == node->conn_count) {
            i = 0;
        }

        /* Update scan location. */
        node->sharedContext->scanLoc[u_sess->stream_cxt.smp_id] = i;
        dataStatus = node->sharedContext->dataStatus[u_sess->stream_cxt.smp_id][i];
        is_conn_end = node->sharedContext->is_connect_end[u_sess->stream_cxt.smp_id][i];

        if (!is_conn_end) {
            finished = false;
            waitnodeCount++;
        }

        /*
         * Firstly, we handle error or notice messages.
         * If an error occured, we should stop scan now.
         * If an notice occured, we can still receive data.
         */
        buf = node->sharedContext->messages[u_sess->stream_cxt.smp_id][i];
        if (buf->len > 0) {
            if (buf->cursor == 'E') {
                HandleStreamError(node, buf->data, buf->len);
                return STREAM_SCAN_FINISH;
            } else if (buf->cursor == 'N') {
                HandleStreamNotice(node, buf->data, buf->len);
                resetStringInfo(buf);

                /* After one notice message has handled, send signal and wake up the dest producer. */
                entry = node->sharedContext->quota_entrys[u_sess->stream_cxt.smp_id][i];
                entry->_signal();

                return STREAM_SCAN_WAIT;
            }
        }

        switch (dataStatus) {
            case DATA_EMPTY:
                break;

            case DATA_PREPARE:
                /* Take the rest data away when the connection is end. */
                if (is_conn_end) {
                    /* Return data if any. */
                    if (gs_consume_memory_data(node, i)) {
                        return STREAM_SCAN_DATA;
                    }
                }
                break;

            case DATA_READY:
                if (gs_consume_memory_data(node, i)) {
                    return STREAM_SCAN_DATA;
                } else {
                    break;
                }

            case CONN_ERR:
                ereport(ERROR,
                    (errcode(ERRCODE_STREAM_REMOTE_CLOSE_SOCKET),
                        errmsg("Failed to read response from Local Stream Node,"
                               " Detail: Node %s, Plan Node ID %u, SMP ID %d",
                            g_instance.attr.attr_common.PGXCNodeName,
                            node->sharedContext->key_s.planNodeId,
                            i)));
                break;
            // dataStatus is enum,
            default:
                break;
        }
    } while (i != scanLoc);

    *waitnode_count = waitnodeCount;

    if (finished) {
        return STREAM_SCAN_FINISH;
    } else {
        return STREAM_SCAN_WAIT;
    }
}

/*
 * @Description: Receive data from shared memory for local stream.
 *
 * @param[IN] node: stream state
 * @return bool: true -- successed to find data and need more data.
 *                  false -- all connection finished or recerive error.
 */
bool gs_memory_recv(StreamState* node)
{
    char result;
    struct hash_entry* entry = NULL;
    entry = node->sharedContext->poll_entrys[u_sess->stream_cxt.smp_id];
    bool re = true;
    int waitnode_count = 0;

    /* If there is already tuple in buffer, return the data at once. */
    if (!node->sharedContext->vectorized && gs_return_tuple(node)) {
        return true;
    }

    for (;;) {
        /* Check for interrupt at the beginning of the loop. */
        CHECK_FOR_INTERRUPTS();

        /* Check if we can early stop now. */
        if (executorEarlyStop()) {
            re = false;
            break;
        }

        /* Search all producers to find data. */
        result = gs_find_memory_data(node, &waitnode_count);
        if (result == STREAM_SCAN_DATA) {
            re = true;
            break;
        } else if (result == STREAM_SCAN_FINISH) {
            re = false;
            break;
        }

        WaitStatePhase oldPhase = pgstat_report_waitstatus_phase(PHASE_NONE, true);
        WaitState oldStatus = pgstat_report_waitstatus_comm(STATE_WAIT_NODE,
            u_sess->pgxc_cxt.PGXCNodeId,
            waitnode_count,
            node->sharedContext->key_s.planNodeId,
            global_node_definition ? global_node_definition->num_nodes : -1);

        /* Poll to wait data from producers. */
        NetWorkTimePollStart(t_thrd.pgxc_cxt.GlobalNetInstr);
        (void)entry->_timewait(SINGLE_WAITQUOTA);
        NetWorkTimePollEnd(t_thrd.pgxc_cxt.GlobalNetInstr);

        pgstat_reset_waitStatePhase(oldStatus, oldPhase);
    }

    return re;
}

/*
 * @Description: Inform all related consuemrs that there is no more data to send.
 *
 * @param[IN] sharedContext: context for shared memory stream
 * @param[IN] connNum: producer connection number
 */
void gs_memory_send_finish(StreamSharedContext* sharedContext, int connNum)
{
    struct hash_entry* entry = NULL;

    for (int i = 0; i < connNum; i++) {
        /* Set flags. */
        sharedContext->is_connect_end[i][u_sess->stream_cxt.smp_id] = true;

        /* send signal */
        entry = sharedContext->poll_entrys[i];
        entry->_signal();
    }
}

/*
 * @Description: Set all connections with this producer to close.
 *
 * @param[IN] sharedContext: context for shared memory stream
 * @param[IN] connNum: producer connection number
 * @param[IN] smpId: producer smp id
 */
void gs_memory_close_conn(StreamSharedContext* sharedContext, int connNum, int consumerId)
{
    struct hash_entry* entry = NULL;

    for (int i = 0; i < connNum; i++) {
        /* Set flags. */
        sharedContext->is_connect_end[consumerId][i] = true;

        /*
         * Send signal to the producers which may be still waiting quota,
         * in a query like "limit XXX", when consumer don't need data anymore,
         * but the producers haven't send all data yet.
         */
        entry = sharedContext->quota_entrys[consumerId][i];
        entry->_signal();
    }
}

