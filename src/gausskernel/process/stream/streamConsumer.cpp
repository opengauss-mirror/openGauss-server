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
 * streamConsumer.cpp
 *	  Support methods for class StreamConsumer.
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/stream/streamConsumer.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/printtup.h"
#include "distributelayer/streamConsumer.h"
#include "distributelayer/streamTransportComm.h"
#include "executor/executor.h"
#include "executor/exec/execStream.h"
#include "libpq/libpq.h"
#include "libcomm/libcomm.h"
#include "miscadmin.h"
#include "pgxc/pgxc.h"

extern GlobalNodeDefinition* global_node_definition;

StreamConsumer::StreamConsumer(MemoryContext context) : StreamObj(context, STREAM_CONSUMER)
{
    m_sharedContext = NULL;
    m_originProducerNodeList = NULL;
    m_ready = false;
    m_expectProducer = NULL;
    m_currentProducerNum = 0;
}

StreamConsumer::~StreamConsumer()
{
    m_originProducerNodeList = NULL;
    m_expectProducer = NULL;
    m_sharedContext = NULL;
}

/*
 * @Description: Init the consumer object
 *
 * @param[IN] key:  stream key
 * @param[IN] execProducerNodes:  producer node list
 * @param[IN] desc:  parallel description
 * @param[IN] transType:  transport type
 * @return: void
 */
void StreamConsumer::init(StreamKey key, List* execProducerNodes, ParallelDesc desc, StreamTransType transType,
    StreamSharedContext* sharedContext)
{
    int i = 0;
    bool found = false;
    
    int producerNum = 0;
    AutoMutexLock streamLock(&m_streamInfoLock);
    AutoMutexLock copyLock(&nodeDefCopyLock);
    errno_t rc = EOK;
    bool localNodeOnly = STREAM_IS_LOCAL_NODE(desc.distriType);
    if (localNodeOnly)
        producerNum = desc.producerDop;
    else
        producerNum = desc.producerDop * list_length(execProducerNodes);

    Assert(producerNum > 0);

    m_parallel_desc = desc;
    m_currentProducerNum = 0;
    m_connNum = producerNum;
    m_key = key;
    m_ready = false;
    m_transtype = transType;
    m_sharedContext = sharedContext;
    m_transport = (StreamTransport**)MemoryContextAllocZero(m_memoryCxt, producerNum * sizeof(StreamTransport*));
    m_expectProducer = (StreamConnInfo*)MemoryContextAllocZero(m_memoryCxt, producerNum * sizeof(StreamConnInfo));

    /* Initialize the origin nodelist */
    m_originProducerNodeList = NIL;
#ifdef ENABLE_MULTIPLE_NODES
    copyLock.lock();
    ListCell* nodelistCell = NULL;
    int nodeIdx;
    for (int j = 0; j < desc.producerDop; j++) {
        foreach (nodelistCell, execProducerNodes) {
            nodeIdx = lfirst_int(nodelistCell);
            if (localNodeOnly && nodeIdx != u_sess->pgxc_cxt.PGXCNodeId)
                continue;

            rc = strncpy_s(&m_expectProducer[i].nodeName[0],
                NAMEDATALEN,
                global_node_definition->nodesDefinition[nodeIdx].nodename.data,
                strlen(global_node_definition->nodesDefinition[nodeIdx].nodename.data) + 1);
            securec_check(rc, "\0", "\0");
            m_expectProducer[i].nodeIdx = nodeIdx;
            i++;
        }
    }
    copyLock.unLock();
#endif
    for (i = 0; i < producerNum; i++) {
        int nodeNameLen = 0;
        libcommaddrinfo* libcommaddr = NULL;

        libcommaddr = (libcommaddrinfo*)palloc0(sizeof(libcommaddrinfo));
        libcommaddr->ctrl_port = -1;
        libcommaddr->listen_port = g_instance.attr.attr_network.comm_sctp_port + i;
        nodeNameLen = strlen(&m_expectProducer[i].nodeName[0]);
        rc = strncpy_s(libcommaddr->nodename, NAMEDATALEN, &m_expectProducer[i].nodeName[0], nodeNameLen + 1);
        securec_check(rc, "\0", "\0");
        m_transport[i] = New(m_memoryCxt) StreamCOMM(libcommaddr, false);

        StreamCOMM* scomm = (StreamCOMM*)m_transport[i];
        scomm->m_addr->streamKey.queryId = m_key.queryId;
        scomm->m_addr->streamKey.planNodeId = m_key.planNodeId;
        scomm->m_addr->streamKey.producerSmpId = 0;
        scomm->m_addr->streamKey.consumerSmpId = m_key.smpIdentifier;
    }

    HOLD_INTERRUPTS(); /* Add this macro for double safety. */

    streamLock.lock();
    AutoContextSwitch streamInfoCxtGuard(StreamInfoContext);

    StreamElement* element = (StreamElement*)hash_search(m_streamInfoTbl, &m_key, HASH_ENTER, &found);
    if (element == NULL) {
        streamLock.unLock();
        ereport(
            ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("Failed to create stream element due to out of memory")));
    }

    if (found == false) {
        STREAM_LOG(DEBUG2,
            "init consumer(%lu, %u, %u), key not found, build it",
            m_key.queryId,
            m_key.planNodeId,
            m_key.smpIdentifier);
        element->value = (StreamValue*)palloc0_noexcept(sizeof(StreamValue));

        if (NULL == element->value) {
            hash_search(m_streamInfoTbl, &key, HASH_REMOVE, NULL);
            streamLock.unLock();
            ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("Failed to generate stream element due to out of memory")));
        }

        element->value->connNum = 0;
        element->value->connInfoSize = 0;
        /* No need to allocate the array size. */
        element->value->connInfo = NULL;
    } else {
        /* WTF? find a duplicate element in the hash table. */
        if (element->value == NULL || element->value->consumer) {
            streamLock.unLock();
            ereport(ERROR,
                (errmodule(MOD_STREAM),
                    errcode(ERRCODE_STREAM_DUPLICATE_QUERY_ID),
                    errmsg("Distribute query fail due to duplicate plan id")));
        }
    }

    /* Found true means some information has not been updated, protected by streamLock. */
    if (found == true) {
        Assert(element->value->connInfo != NULL);
        updateTransportInfo(element->value);

        if (element->value->connInfo) {
            pfree_ext(element->value->connInfo);
            element->value->connInfo = NULL;
            element->value->connInfoSize = 0;
        }
    }

    if (m_threadSyncObjInit == false) {
        pthread_mutex_init(&m_mutex, NULL);
        pthread_cond_init(&m_cond, NULL);
        m_threadSyncObjInit = true;
    }

    element->value->consumer = this;

    m_init = true;

    STREAM_LOG(DEBUG2,
        "consumer inited, StreamKey(%lu, %u, %u), "
        "local: %s, producer nodelist length: %d",
        key.queryId,
        key.planNodeId,
        key.smpIdentifier,
        PRINTTRUE(localNodeOnly),
        list_length(execProducerNodes));

    streamLock.unLock();
    RESUME_INTERRUPTS();
}

/*
 * @Description: Deinit the consumer object
 *
 * @return: void
 */
void StreamConsumer::deInit()
{
    AutoMutexLock streamHashLock(&m_streamInfoLock);
    StreamElement* delinfo = NULL;

    HOLD_INTERRUPTS();
    streamHashLock.lock();

    /* Do not need de init. */
    if (m_init == false) {
        if (m_threadSyncObjInit == true) {
            pthread_mutex_destroy(&m_mutex);
            pthread_cond_destroy(&m_cond);
            m_threadSyncObjInit = false;
        }

        streamHashLock.unLock();
        RESUME_INTERRUPTS();
        return;
    }

    releaseCommStream();

    /* Close local stream connection. */
    if (NULL != m_sharedContext) {
        gs_memory_close_conn(m_sharedContext, m_connNum, u_sess->stream_cxt.smp_id);
    }

    if (m_ready || u_sess->stream_cxt.dummy_thread == true) {
        delinfo = (StreamElement*)hash_search(m_streamInfoTbl, &m_key, HASH_REMOVE, NULL);
        if (delinfo != NULL) {
            if (delinfo->value->connInfo != NULL) {
                pfree_ext(delinfo->value->connInfo);
                delinfo->value->connInfo = NULL;
            }

            pfree_ext(delinfo->value);
            delinfo->value = NULL;
        }
    } else {
        /*
         * set flag to release net port in case some producer not ready and element
         * in stream info hash table can not get removed in wakeUpConsumer,
         * or consumer get canceled when waiting producer ready.
         */
        u_sess->stream_cxt.global_obj->setNeedClean(true);
    }

    if (m_threadSyncObjInit == true) {
        pthread_mutex_destroy(&m_mutex);
        pthread_cond_destroy(&m_cond);
        m_threadSyncObjInit = false;
    }

    m_init = false;
    streamHashLock.unLock();

    /* Cleanup the original producer list */
    m_originProducerNodeList = NIL;

    RESUME_INTERRUPTS();
}

/*
 * @Description: Release sctp mode stream
 *
 * @return: void
 */
void StreamConsumer::releaseCommStream()
{
    if (m_transport != NULL) {
        for (int i = 0; i < m_connNum; i++) {
            Assert(t_thrd.int_cxt.ImmediateInterruptOK == false);
            StreamCOMM* scomm = (StreamCOMM*)m_transport[i];
            if (scomm != NULL)
                scomm->release();
        }
    }
}

/*
 * @Description: Get nodeIdx of producer by nodename
 *
 * @param[IN] nodename:  node name
 * @return: node index
 */
int StreamConsumer::getNodeIdx(const char* nodename)
{
    for (int i = 0; i < m_connNum; i++) {
        if (pg_strncasecmp(m_expectProducer[i].nodeName, nodename, strlen(nodename)) == 0)
            return m_expectProducer[i].nodeIdx;
    }
    return -1;
}

/*
 * @Description: Find un connect producer
 *
 * @param[OUT] str:  string to store unconnected producer(s)
 * @return: void
 */
void StreamConsumer::findUnconnectProducer(StringInfo str)
{
    bool found = false;

    for (int j = 0; j < m_connNum; j++) {
        found = false;
        for (int i = 0; i < m_currentProducerNum; i++) {
            if (strcmp(m_expectProducer[j].nodeName, m_transport[i]->m_nodeName) == 0) {
                found = true;
                break;
            }
        }

        if (!found)
            appendStringInfo(str, " %s", m_expectProducer[j].nodeName);
    }
}

/*
 * @Description: Get nodeIdx of the first un-connected producer.
 *
 * @return: int: nodeid
 */
int StreamConsumer::getFirstUnconnectedProducerNodeIdx()
{
    bool found = false;
    int nodeIdx = -1;

    for (int j = 0; j < m_connNum; j++) {
        found = false;
        for (int i = 0; i < m_currentProducerNum; i++) {
            if (strcmp(m_expectProducer[j].nodeName, m_transport[i]->m_nodeName) == 0) {
                found = true;
                break;
            }
        }

        if (!found) {
            nodeIdx = m_expectProducer[j].nodeIdx;
            break;
        }
    }
    return nodeIdx;
}

/*
 * @Description: Waiting for the producer is ready
 *
 * @return: void
 */
void StreamConsumer::waitProducerReady()
{
    /* For local stream, producer will never connect to consumer. Consumer is ready, just return. */
    if (STREAM_IS_LOCAL_NODE(m_parallel_desc.distriType)) {
        m_ready = true;
        return;
    }

    AutoMutexLock streamLock(&m_mutex);
    if (m_ready == false) {
        STREAM_LOG(DEBUG2,
            "consumer(%lu, %u, %u) begin sleep until producer ready",
            m_key.queryId,
            m_key.planNodeId,
            m_key.smpIdentifier);
        WaitState oldStatus = pgstat_report_waitstatus(STATE_WAIT_UNDEFINED, true);

        Assert(t_thrd.int_cxt.ImmediateInterruptOK == false);
        streamLock.lock();

        /* 900s timeout. */
        struct timespec timer;
        int ret;
        int ntimes = 1;
        StringInfoData str;
        initStringInfo(&str);

        while (m_ready == false) {
            clock_gettime(CLOCK_REALTIME, &timer);
            timer.tv_sec += 3;
            timer.tv_nsec = 0;

            ret = pthread_cond_timedwait(&m_cond, &m_mutex, &timer);
            if (ret == ETIMEDOUT || m_ready == false) {
                streamLock.unLock();
                ntimes++;

                pgstat_report_waitstatus_comm(STATE_STREAM_WAIT_PRODUCER_READY,
                    getFirstUnconnectedProducerNodeIdx(),
                    m_connNum - m_currentProducerNum,
                    m_key.planNodeId,
                    global_node_definition ? global_node_definition->num_nodes : -1);

                if (ntimes == 300) {
                    if (t_thrd.int_cxt.QueryCancelPending) {
                        ereport(WARNING, (errmodule(MOD_STREAM),
                                      errcode(ERRCODE_CONNECTION_TIMED_OUT),
                                      errmsg("Did not handle interrupt signal, with InterruptHoldoffCount(%u),",
                                             t_thrd.int_cxt.InterruptHoldoffCount)));
                    }
                    resetStringInfo(&str);
                    findUnconnectProducer(&str);
                    ereport(ERROR,
                        (errmodule(MOD_STREAM),
                            errcode(ERRCODE_CONNECTION_TIMED_OUT),
                            errmsg("Distribute query initializing network connection timeout. un-connected nodes: %s",
                                str.data)));
                }

                /* Check for interrupts.(cancel signal?). */
                CHECK_FOR_INTERRUPTS();
                streamLock.lock();
            }
        }

        streamLock.unLock();

        pgstat_report_waitstatus(oldStatus);
        STREAM_LOG(DEBUG2,
            "consumer(%lu, %u, %u) wake up as producer ready",
            m_key.queryId,
            m_key.planNodeId,
            m_key.smpIdentifier);
    }

    return;
}

/*
 * @Description: Wake up consumer and let it work
 *
 * @param[IN] commKey:  stream key
 * @param[IN] connInfo:  connection info
 * @return: true if wake up consumer successfully
 */
bool StreamConsumer::wakeUpConsumerCallBack(CommStreamKey commKey, StreamConnInfo connInfo)
{
    bool found = false;
    bool res = true;
    AutoMutexLock streamLock(&m_streamInfoLock);
    int nodeNameLen = strlen(connInfo.nodeName);
    errno_t rc = EOK;
    StreamKey key = {0};

    key.queryId = commKey.queryId;
    key.planNodeId = commKey.planNodeId;
    key.smpIdentifier = commKey.consumerSmpId;
    connInfo.producerSmpId = commKey.producerSmpId;

    streamLock.lock();

    /* Check if can accept connection now */
    if (StreamNodeGroup::checkStreamConnectPermission(key.queryId) == false) {
        streamLock.unLock();
        return false;
    }

    AutoContextSwitch streamInfoCxtGuard(StreamInfoContext);

    /*
     * Register me in the global consumer table if the consumer thread has not been started.
     * Libcomm r_flow_ctrl thread call this, must not use palloc and elog.
     * so use isLibcommThread flag, return false where malloc failed.
     */
    StreamElement* element = (StreamElement*)hash_search(m_streamInfoTbl, &key, HASH_ENTER, &found);
    if (element == NULL) {
        errno = ENOMEM;
        streamLock.unLock();
        return false;
    }

    /* If StreamElement not register by the consumer thread, we save the information. */
    if (found == false) {
        element->value = (StreamValue*)palloc0_noexcept(sizeof(StreamValue));
        if (NULL == element->value) {
            hash_search(m_streamInfoTbl, &key, HASH_REMOVE, NULL);

            errno = ENOMEM;
            streamLock.unLock();
            return false;
        }

        element->value->connInfo = (StreamConnInfo*)palloc0_noexcept(sizeof(StreamConnInfo) * INIT_STREAMINFO_SIZE);
        if (NULL == element->value->connInfo) {
            pfree_ext(element->value);
            element->value = NULL;
            hash_search(m_streamInfoTbl, &key, HASH_REMOVE, NULL);

            errno = ENOMEM;
            streamLock.unLock();
            return false;
        }
        element->value->connInfoSize = INIT_STREAMINFO_SIZE;

        element->value->connInfo[0].port.libcomm_layer.gsock = connInfo.port.libcomm_layer.gsock;
        rc = strncpy_s(element->value->connInfo[0].nodeName, NAMEDATALEN, connInfo.nodeName, nodeNameLen + 1);
        securec_check(rc, "\0", "\0");
        element->value->connInfo[0].producerSmpId = connInfo.producerSmpId;
        element->value->connNum = 1;
        element->value->consumer = NULL;
    } else {
        if (element->value) {
            /* Consumer thread has not been registered yet. */
            if (element->value->consumer == NULL) {
                element->value->connInfo[element->value->connNum].port.libcomm_layer.gsock =
                    connInfo.port.libcomm_layer.gsock;
                rc = strncpy_s(element->value->connInfo[element->value->connNum].nodeName,
                    NAMEDATALEN,
                    connInfo.nodeName,
                    nodeNameLen + 1);
                securec_check(rc, "\0", "\0");
                element->value->connInfo[element->value->connNum].producerSmpId = connInfo.producerSmpId;
                element->value->connNum++;

                /* Check if need realloc to remember more connection info. */
                if (element->value->connNum == element->value->connInfoSize) {
                    StreamConnInfo* new_connInfo = NULL;
                    int new_connInfoSize = 2 * element->value->connInfoSize;

                    new_connInfo = (StreamConnInfo*)palloc0_noexcept(new_connInfoSize * sizeof(StreamConnInfo));
                    if (NULL == new_connInfo) {
                        pfree_ext(element->value->connInfo);
                        element->value->connInfo = NULL;
                        pfree_ext(element->value);
                        element->value = NULL;
                        hash_search(m_streamInfoTbl, &key, HASH_REMOVE, NULL);

                        errno = ENOMEM;
                        streamLock.unLock();
                        return false;
                    }

                    rc = memcpy_s(new_connInfo,
                        new_connInfoSize * sizeof(StreamConnInfo),
                        element->value->connInfo,
                        element->value->connInfoSize * sizeof(StreamConnInfo));
                    securec_check(rc, "\0", "\0");

                    pfree_ext(element->value->connInfo);
                    element->value->connInfo = new_connInfo;
                    element->value->connInfoSize = new_connInfoSize;
                }
            } else {
                /* Have registered, so we update the stream info. */
                res = element->value->consumer->updateStreamCommInfo(&connInfo);

                /* We can free the memory, no longer need it. */
                if (element->value->connInfo != NULL) {
                    pfree_ext(element->value->connInfo);
                    element->value->connInfo = NULL;
                }
            }
        } else {
            streamLock.unLock();
            return false;
        }
    }

    streamLock.unLock();
    return res;
}

/*
 * @Description: Update the stream info
 *
 * @param[IN] connInfo:  connection info
 * @return: true if update successfully
 */
bool StreamConsumer::updateStreamCommInfo(StreamConnInfo* connInfo)
{
    AutoMutexLock streamLock(&m_mutex);

    streamLock.lock();

    /* WTF, duplicate plan id encounter or plan error. */
    if (m_currentProducerNum >= m_connNum) {
        streamLock.unLock();
        return false;
    }

    StreamCOMM* scomm = (StreamCOMM*)m_transport[m_currentProducerNum];
    scomm->updateInfo(connInfo);

    m_currentProducerNum++;
    if (m_currentProducerNum == m_connNum) {
        m_ready = true;
        (void)pthread_cond_signal(&m_cond);
    }
    streamLock.unLock();

    return true;
}

/*
 * @Description: Update the transport info
 *
 * @param[IN] val:  stream value
 * @return: void
 */
void StreamConsumer::updateTransportInfo(StreamValue* val)
{
    if (val->connNum > m_connNum)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Distribute query fail due to duplicate plan id when register consumer socket, "
                       "current_connNum: %d, expected_connNum: %d",
                    val->connNum,
                    m_connNum)));

    for (int i = 0; i < val->connNum; i++) {
        m_transport[i]->updateInfo(&val->connInfo[i]);

        val->connInfo[i].port.tcpLayer.fd = NO_SOCKET;
        m_currentProducerNum++;

        if (m_currentProducerNum == m_connNum)
            m_ready = true;
    }
}
