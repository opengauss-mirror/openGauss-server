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
 * streamCore.cpp
 *	  Support methods for class streamObj and StreamNodeGroup.
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/stream/streamCore.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#ifdef PGXC
#include "commands/trigger.h"
#endif
#include "executor/executor.h"
#include "pgxc/execRemote.h"
#include "nodes/nodes.h"
#include "access/printtup.h"
#include "pgxc/copyops.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolmgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "utils/memutils.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libcomm/libcomm.h"
#include <sys/poll.h>
#include "executor/exec/execStream.h"
#include "executor/node/nodeRecursiveunion.h"
#include "postmaster/postmaster.h"
#include "access/transam.h"
#include "gssignal/gs_signal.h"
#include "utils/distribute_test.h"
#include "utils/guc_tables.h"
#include "utils/snapmgr.h"
#include "utils/combocid.h"
#include "vecexecutor/vecstream.h"
#include "access/hash.h"
#include "pgstat.h"
#include "tcop/tcopprot.h"
#include "distributelayer/streamCore.h"
#include "distributelayer/streamMain.h"
#include "distributelayer/streamProducer.h"
#include "distributelayer/streamConsumer.h"
#include "storage/procsignal.h"

/* Process-wise variables. */
MemoryContext StreamObj::m_memoryGlobalCxt = NULL;
pthread_mutex_t StreamObj::m_streamInfoLock;
HTAB* StreamObj::m_streamInfoTbl = NULL;

MemoryContext StreamNodeGroup::m_memoryGlobalCxt = NULL;
pthread_mutex_t StreamNodeGroup::m_streamNodeGroupLock;
HTAB* StreamNodeGroup::m_streamNodeGroupTbl = NULL;
HTAB* StreamNodeGroup::m_streamConnectSyncTbl = NULL;
pthread_mutex_t StreamNodeGroup::m_streamConnectSyncLock;

static void ConsumerNodeSyncUpMessage(RecursiveUnionController* controller, int step, StreamState* node);

StreamObj::StreamObj(MemoryContext context, StreamObjType type)
    : m_nodeGroup(0),
      m_transport(0),
      m_pair(NULL),
      m_type(type),
      m_memoryCxt(context),
      m_connNum(0),
      m_nodeGroupIdx(0),
      m_threadId(InvalidTid)
{
    m_nodeId = u_sess->pgxc_cxt.PGXCNodeId;
    m_streamNode = NULL;
    m_init = false;
    m_threadSyncObjInit = false;
    m_status = STREAM_UNDEFINED;
#ifdef ENABLE_MULTIPLE_NODES
    /* should not be the initial value, check the logic that coordinator send node id. */
    Assert(m_nodeId != -1);
#endif
}

StreamObj::~StreamObj()
{}

/*
 * @Description: Set stream thread id
 *
 * @param[IN] pid:  thread id
 * @return: void
 */
void StreamObj::setThreadId(ThreadId pid)
{
    m_threadId = pid;
}

/*
 * @Description: Get the pgxc data node id
 *
 * @return: node id
 */
int StreamObj::getPgxcNodeId()
{
    return m_nodeId;
}

/*
 * @Description: Get stream thread id
 *
 * @return: thread id
 */
ThreadId StreamObj::getThreadId()
{
    return m_threadId;
}

/*
 * @Description: Get the index itself in the group
 *
 * @return: node group index
 */
int StreamObj::getNodeGroupIdx()
{
    return m_nodeGroupIdx;
}

/*
 * @Description: Release net port
 *
 * @return: void
 */
void StreamObj::releaseNetPort()
{
    for (int i = 0; i < m_connNum; i++) {
        if (m_transport != NULL && m_transport[i] != NULL)
            m_transport[i]->release();
    }
}

/*
 * @Description: Release net port in hash table(for stream information, mainly for stream connect).
 *
 * @return: void
 */
void StreamObj::releaseNetPortInHashTable()
{
    StreamElement* element = NULL;
    AutoMutexLock streamLock(&m_streamInfoLock);
    HASH_SEQ_STATUS hash_seq;

    /* Exit if hash table is not yet created */
    if (NULL == m_streamInfoTbl)
        return;

    HOLD_INTERRUPTS(); /* add this macro for double safety */
    streamLock.lock();
    hash_seq_init(&hash_seq, m_streamInfoTbl);

    /* Sequentially search through hash table and release net port identified by u_sess->debug_query_id. */
    while ((element = (StreamElement*)hash_seq_search(&hash_seq)) != NULL) {
        if (element->key.queryId == (uint64)u_sess->debug_query_id) {
            StreamValue* sVal = element->value;
            StreamKey key = element->key;

            if (sVal != NULL && sVal->connInfo != NULL) {
                StreamConnInfo* connInfo = sVal->connInfo;
                int connNum = sVal->connNum;

                for (int i = 0; i < connNum; i++) {
                    gs_close_gsocket(&connInfo[i].port.libcomm_layer.gsock);
                }
            }

            /* Free memory. */
            if (element->value != NULL) {
                if (element->value->connInfo != NULL) {
                    pfree_ext(element->value->connInfo);
                    element->value->connInfo = NULL;
                }
                pfree_ext(element->value);
                element->value = NULL;
            }

            /* Remove the element left in hash table. */
            (void)hash_search(m_streamInfoTbl, &key, HASH_REMOVE, NULL);
        }
    }

    streamLock.unLock();
    RESUME_INTERRUPTS();
}

/*
 * @Description: Get connection number
 *
 * @return: connection number
 */
int StreamObj::getConnNum()
{
    return m_connNum;
}

/*
 * @Description: Get stream transport
 *
 * @return: stream transport array
 */
StreamTransport** StreamObj::getTransport()
{
    return m_transport;
}

/*
 * @Description: Set stream pair
 *
 * @param[IN] pair:  stream pair
 * @return: void
 */
void StreamObj::setPair(StreamPair* pair)
{
    m_pair = pair;
}

/*
 * @Description: Get stream pair
 *
 * @return: stream pair
 */
StreamPair* StreamObj::getPair()
{
    return m_pair;
}

/*
 * @Description: Get the stream plan node.
 *
 * @return: stream node
 */
const Stream* StreamObj::getStream()
{
    return m_streamNode;
}

/*
 * @Description: Initiliaze the stream environment
 *
 * @return: void
 */
void StreamObj::startUp()
{
    HASHCTL ctl;
    errno_t rc = EOK;

    m_memoryGlobalCxt = AllocSetContextCreate(g_instance.instance_context,
        "StreamGlobalContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    /* init the stream info table ctl. */
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(StreamKey);
    ctl.entrysize = sizeof(StreamElement);
    ctl.hash = tag_hash;
    ctl.hcxt = m_memoryGlobalCxt;

    /*
     *  m_streamInfoTbl is created with HASH_NOEXCEPT,
     *  so hash_search with HASH_ENTER don't throw when
     *  palloc failed. We must check whether the result is NULL.
     */
    m_streamInfoTbl =
        hash_create("stream info lookup hash", 256, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_SHRCTX | HASH_NOEXCEPT);
}

/*
 * @Description: Get the node group
 *
 * @return: node group
 */
StreamNodeGroup* StreamObj::getNodeGroup()
{
    return m_nodeGroup;
}

/*
 * @Description: Get stream key
 *
 * @return: stream key
 */
StreamKey StreamObj::getKey()
{
    return m_key;
}

/*
 * @Description: Get the stream type
 *
 * @return: stream type
 */
StreamObjType StreamObj::getType()
{
    return m_type;
}

/*
 * @Description: Get parallel description
 *
 * @return: parallel description
 */
ParallelDesc StreamObj::getParallelDesc()
{
    return m_parallel_desc;
}

StreamNodeGroup::StreamNodeGroup()
    : m_size(0),
      m_streamNum(1),
      m_createThreadNum(0),
      m_streamEnter(0),
      m_streamEnterCount(0),
      m_canceled(false),
      m_needClean(false),
      m_errorStop(false),
      m_recursiveVfdInvalid(false)
{
    pthread_mutex_init(&m_mutex, NULL);
    pthread_mutex_init(&m_recursiveMutex, NULL);
    pthread_cond_init(&m_cond, NULL);
    m_pid = gs_thread_self();
    m_streamPairList = NULL;
    m_streamConsumerList = NULL;
    m_streamProducerList = NULL;
    m_syncControllers = NIL;
    m_streamRuntimeContext = NULL;
    m_streamArray = NULL;
    m_quitWaitCond = 0;
    m_edataWriteProtect = EDATA_WRITE_ENABLE;
    m_producerEdata = NULL;
#ifndef ENABLE_MULTIPLE_NODES
    m_portal = NULL;
#endif
}

StreamNodeGroup::~StreamNodeGroup()
{}

/*
 * @Description: Init stream node group
 *
 * @param[IN] threadNum:  thread number
 * @return: void
 */
void StreamNodeGroup::Init(int threadNum)
{
    m_size = threadNum + 1; /* all stream thead + top consumer thread */
    m_streamArray = (StreamNode*)palloc0(sizeof(StreamNode) * m_size);
    for (int i = 0; i < m_size; i++)
        m_streamArray[i].status = STREAM_UNDEFINED;

    /* all stream thead + top consumer thread. */
    m_quitWaitCond = m_size;
#ifdef ENABLE_MULTIPLE_NODES
    bool found = false;
    AutoMutexLock streamLock(&m_streamNodeGroupLock);
    /* register the node group now */
    streamLock.lock();
    StreamNodeElement* element = (StreamNodeElement*)hash_search(m_streamNodeGroupTbl, &m_pid, HASH_ENTER, &found);
    if (found != false) {
        streamLock.unLock();
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("pid of stream nodegroup id is duplicated")));
    }
    element->value = this;
    element->key = gs_thread_self();
    streamLock.unLock();
#endif
}

/*
 * @Description: Init some static member
 *
 * @return: void
 */
void StreamNodeGroup::StartUp()
{
    HASHCTL nodectl;
    errno_t rc = EOK;

    m_memoryGlobalCxt = AllocSetContextCreate(g_instance.instance_context,
        "StreamNodeGlobalContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    /* init the stream node group table ctl. */
    rc = memset_s(&nodectl, sizeof(nodectl), 0, sizeof(nodectl));
    securec_check(rc, "\0", "\0");
    nodectl.keysize = sizeof(ThreadId);
    nodectl.entrysize = sizeof(StreamNodeElement);
    nodectl.hash = tag_hash;
    nodectl.hcxt = m_memoryGlobalCxt;
#ifdef ENABLE_MULTIPLE_NODES
    m_streamNodeGroupTbl =
        hash_create("stream node group lookup hash", 256, &nodectl, HASH_ELEM | HASH_FUNCTION | HASH_SHRCTX);
#endif
    pthread_mutex_init(&m_streamNodeGroupLock, NULL);

    /* init the stream connect sync table ctl */
    rc = memset_s(&nodectl, sizeof(nodectl), 0, sizeof(nodectl));
    securec_check(rc, "\0", "\0");
    nodectl.keysize = sizeof(uint64);
    nodectl.entrysize = sizeof(StreamConnectSyncElement);
    nodectl.hash = tag_hash;
    nodectl.hcxt = m_memoryGlobalCxt;

    m_streamConnectSyncTbl =
        hash_create("stream connect sync hash", 256, &nodectl, HASH_ELEM | HASH_FUNCTION | HASH_SHRCTX);

    pthread_mutex_init(&m_streamConnectSyncLock, NULL);
}

/*
 * @Description: Register the object in stream node group
 *
 * @param[IN] obj:  stream object
 * @return: index in node group
 */
int StreamNodeGroup::registerStream(StreamObj* obj)
{
    if (obj->getType() == STREAM_PRODUCER) {
        int streamIdx;
        AutoMutexLock streamLock(&m_mutex);

        streamLock.lock();
        m_streamArray[m_streamNum].streamObj = obj;
        setNodeStatus(m_streamNum, STREAM_INPROGRESS);
        streamIdx = m_streamNum;
        m_streamNum++;
        streamLock.unLock();
        Assert(m_streamNum <= m_size);
        return streamIdx;
    } else {
        AutoContextSwitch streamContext(u_sess->stream_cxt.stream_runtime_mem_cxt);
        m_streamArray[0].stopFlag = &u_sess->exec_cxt.executorStopFlag;
        m_streamArray[0].consumerList = lcons(obj, m_streamArray[0].consumerList);
        m_streamArray[0].status = STREAM_INPROGRESS;
        return 0;
    }
}

/*
 * @Description: Unregister the stream object represented in group idx with status
 *
 * @param[IN] groupIdx:  index in node group
 * @param[IN] status:  object status
 * @return: void
 */
void StreamNodeGroup::unregisterStream(int groupIdx, StreamObjStatus status)
{
    if (groupIdx == 0) {
        ListCell* consumerCell = NULL;
        StreamConsumer* consumerObj = NULL;

        if (m_streamArray && m_streamArray[0].consumerList != NULL) {
            if (m_streamArray[0].stopFlag != NULL)
                *(m_streamArray[0].stopFlag) = false;

            foreach (consumerCell, m_streamArray[0].consumerList) {
                consumerObj = (StreamConsumer*)lfirst(consumerCell);
                if (consumerObj == NULL) {
                    continue;
                }
                consumerObj->deInit();
            }

            m_streamArray[0].consumerList = NULL;
        }
    } else {
        Assert(groupIdx < m_streamNum);
        if (m_streamArray && m_streamArray[groupIdx].streamObj != NULL) {
            m_streamArray[groupIdx].streamObj->setThreadId(InvalidTid);
        }
        setNodeStatus(groupIdx, status);
    }
    /* Avoid other threads checking empty and assignmenting with stopFlag at the same time */
    AutoMutexLock streamLock(&m_streamNodeGroupLock);
    streamLock.lock();
    if (m_streamArray)
        m_streamArray[groupIdx].stopFlag = NULL;
    streamLock.unLock();
}

/*
 * @Description: Send signal to all stream thread registered in node group
 *
 * @param[IN] signo:  signal number
 * @return: void
 */
void StreamNodeGroup::signalStreamThreadInNodeGroup(int signo)
{
    if (StreamTopConsumerAmI() && m_streamArray) {
        for (int i = 0; i < m_streamNum; i++) {
            if (m_streamArray[i].streamObj != NULL && m_streamArray[i].streamObj->getThreadId() != InvalidTid) {
                int ntimes = 1;
                StreamProducer* producer = (StreamProducer*)m_streamArray[i].streamObj;

                /*
                 * Signal slot must be already registered if stream thread already inited.
                 * If not, wait 1ms once and then recheck. Sets the maximum total wait
                 * time as 30s.
                 */
                while (producer->getThreadInit() == false) {
                    /* sleep 1ms */
                    pg_usleep(1000);

                    ntimes++;
                    if (ntimes == 30000) {
                        /* wait 30s, just break here */
                        break;
                    }
                }

                /*
                 * mark before send signal,
                 * used for signal handle to check signal whether signal is vaild.
                 */
                t_thrd.sig_cxt.gs_sigale_check_type = SIGNAL_CHECK_STREAM_STOP;
                gs_signal_send(producer->getThreadId(), signo);
            }
        }
    }
}
#ifndef ENABLE_MULTIPLE_NODES
void StreamNodeGroup::SigStreamThreadClose()
{
    if (StreamTopConsumerAmI() && m_streamArray) {
        for (int i = 0; i < m_streamNum; i++) {
            if (m_streamArray[i].streamObj != NULL && m_streamArray[i].streamObj->getThreadId() != InvalidTid) {
                int ntimes = 1;
                StreamProducer* producer = (StreamProducer*)m_streamArray[i].streamObj;
                /*
                 * Signal slot must be already registered if stream thread already inited.
                 * If not, wait 1ms once and then recheck. Sets the maximum total wait
                 * time as 30s.
                 */
                while (producer->getThreadInit() == false) {
                    /* sleep 1ms */
                    pg_usleep(1000);
                    ntimes++;
                    if (ntimes == 30000) {
                        /* wait 30s, just break here */
                        break;
                    }
                }
                /*
                 * mark before send signal,
                 * used for signal handle to check signal whether signal is vaild.
                 */
                (void)SendProcSignal(producer->getThreadId(), PROCSIG_STREAM_STOP_CHECK, InvalidBackendId);
            }
        }
    }
}
#endif
/*
 * @Description: Cancel all stream thread registered in node group
 *
 * @return: void
 */
void StreamNodeGroup::cancelStreamThread()
{
    if (u_sess->stream_cxt.global_obj != NULL && !u_sess->stream_cxt.global_obj->m_canceled) {
        u_sess->stream_cxt.global_obj->m_canceled = true;
        u_sess->stream_cxt.global_obj->signalStreamThreadInNodeGroup(SIGINT);
    }
}

/*
 * @Description: Wait all thread in the node group to quit
 *
 * @return: void
 */
void StreamNodeGroup::quitSyncPoint()
{
    int timeout = 30;

    if (StreamThreadAmI() == true) {
        StreamPair* pair = NULL;
        AutoMutexLock streamLock(&m_mutex);

        /* signal the top consumer if i am the last stream thread. */
        streamLock.lock();
        m_streamEnter++;
        m_streamEnterCount++;
        Assert(u_sess->stream_cxt.producer_obj != NULL);
        pair = (u_sess->stream_cxt.producer_obj)->getPair();

        /* pair->subThreadNum - pair->startSubThreadNum is the supposed fail to launch thread. */
        if (u_sess->stream_cxt.smp_id == 0)
            m_quitWaitCond = m_quitWaitCond - 1 - (pair->expectThreadNum - pair->createThreadNum);
        else
            m_quitWaitCond = m_quitWaitCond - 1; /* other smp thread. */

        Assert(m_quitWaitCond >= 0);
        Assert(pair->expectThreadNum >= pair->createThreadNum);

        if (m_quitWaitCond < 0) {
            ereport(WARNING, (errmsg("Stream sub thread m_quitWaitCond invalid: %d. "
                    "To get backtrace detail, set backtrace_min_messages=warning.", m_quitWaitCond)));
            ereport(LOG, (errmsg("Stream info, smp id: %u, m_streamEnter: %d, m_streamEnterCount: %d, "
                "ThreadId: %u, m_createThreadNum: %d, m_size: %d", 
                u_sess->stream_cxt.smp_id, m_streamEnter, m_streamEnterCount,
                (u_sess->stream_cxt.producer_obj)->getThreadId(), m_createThreadNum, m_size)));
        }
        if (m_quitWaitCond <= 0)
            pthread_cond_broadcast(&m_cond);
        else {
            struct timespec ts;
            while (m_quitWaitCond > 0) {
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_sec += timeout;
                ts.tv_nsec = 0;
                pthread_cond_timedwait(&m_cond, &m_mutex, &ts);
            }
        }
        streamLock.unLock();
    } else if (StreamTopConsumerAmI() == true) {
        /* if none thread is created ,no bother to wait on condition. */
        if (m_createThreadNum != 0) {
            AutoMutexLock streamLock(&m_mutex);
            streamLock.lock();

            /* m_size - 1 - m_createNum means the failed producer thread. */
            m_quitWaitCond = m_quitWaitCond - 1 - (m_size - 1 - m_createThreadNum);

            Assert(m_quitWaitCond >= 0);
            Assert(m_size >= m_createThreadNum);

            if (m_quitWaitCond < 0) {
                ereport(WARNING, (errmsg("Stream top consumer thread m_quitWaitCond invalid: %d. "
                    "To get backtrace detail, set backtrace_min_messages=warning.", m_quitWaitCond)));
                ereport(LOG, (errmsg("Stream info, m_streamEnter: %d, m_streamEnterCount: %d, m_createThreadNum: %d, m_size: %d", 
                    m_streamEnter, m_streamEnterCount, m_createThreadNum, m_size)));
            }
            if (m_quitWaitCond <= 0)
                pthread_cond_broadcast(&m_cond);
            else {
                /*
                 * If inWaitingQuit is true, when we are interupted by a SIGINT signal,
                 * we just send SIGINT to sub-thread to stop them, and we do not elog,
                 * otherwise the waiting quit status of related threads will be broken.
                 * Notes that inWaitQuit should be set before t_thrd.int_cxt.ImmediateInterruptOK be set,
                 * and reset after t_thrd.int_cxt.ImmediateInterruptOK is reset, or there is an opportunity
                 * that we meet a signal but inWaitingQuit has not been set.
                 */
                u_sess->stream_cxt.in_waiting_quit = true;

                /*
                 * Set it to true to enable signal handling when waiting sub-thread quit,
                 * otherwise the cancel request will not be handled in this situation.
                 */
                t_thrd.int_cxt.ImmediateInterruptOK = true;

                /*
                 * If got a signal before t_thrd.int_cxt.ImmediateInterruptOK set to true, the signal can not be
                 * processed immediately util top consumer goes back to ReadCommand again.
                 * It maybe take long time to wait all stream threads come to quitSyncPoint
                 * and then it's not necessary to send signal to stream threads any more.
                 * Hence, signal should be handled soon here with CHECK_FOR_INTERRUPTS().
                 */
                CHECK_FOR_INTERRUPTS();

                struct timespec ts;
                while (m_quitWaitCond > 0) {
                    clock_gettime(CLOCK_REALTIME, &ts);
                    ts.tv_sec += timeout;
                    ts.tv_nsec = 0;
                    pthread_cond_timedwait(&m_cond, &m_mutex, &ts);
                }

                t_thrd.int_cxt.ImmediateInterruptOK = false;
                u_sess->stream_cxt.in_waiting_quit = false;
            }

            if (m_streamEnterCount < m_createThreadNum) {
                ereport(WARNING, (errmsg("Stream top consumer thread m_streamEnterCount invalid: %d", m_streamEnterCount)));
                ereport(LOG, (errmsg("Stream info, m_streamEnter: %d, m_streamEnterCount: %d, m_createThreadNum: %d, m_size: %d", 
                    m_streamEnter, m_streamEnterCount, m_createThreadNum, m_size)));
            }

            streamLock.unLock();
        }
    } else
        return;
}

/*
 * @Description: Push a stream pair
 *
 * @param[IN] key:  stream key
 * @param[IN] producerList:  producer list
 * @param[IN] consumerList:  consumer list
 * @return: stream pair
 */
StreamPair* StreamNodeGroup::pushStreamPair(StreamKey key, List* producerList, List* consumerList)
{
    StreamPair* pair = (StreamPair*)palloc0(sizeof(StreamPair));

    pair->key = key;
    pair->producerList = producerList;
    pair->consumerList = consumerList;
    pair->expectThreadNum = 0;
    pair->createThreadNum = 0;

    m_streamPairList = lcons(pair, m_streamPairList);

    return pair;
}

/*
 * @Description: Pop a stream pair according to key
 *
 * @param[IN] key:  stream key
 * @return: stream pair
 */
StreamPair* StreamNodeGroup::popStreamPair(StreamKey key)
{
    ListCell* cell = NULL;
    StreamPair* pair = NULL;

    foreach (cell, m_streamPairList) {
        pair = (StreamPair*)lfirst(cell);
        if (pair->key.queryId == key.queryId && pair->key.planNodeId == key.planNodeId)
            return pair;
    }

    return NULL;
}

/*
 * @Description: Get stream pair list
 * @return: stream pair list
 */
const List* StreamNodeGroup::getStreamPairList()
{
    return m_streamPairList;
}

/*
 * @Description: Start a stream thread
 *
 * @param[IN] producer:  producer object
 * @param[IN] smpIdentifier:  smp id
 * @param[IN] pair:  stream pair
 * @return: void
 */
void StreamNodeGroup::initStreamThread(StreamProducer* producer, uint8 smpIdentifier, StreamPair* pair)
{
    ThreadId producerThreadId = ApplyStreamThread(producer);
    if (producerThreadId != 0 && producerThreadId != InvalidTid) {
#ifdef __aarch64__
        pg_memory_barrier();
#endif
        AutoMutexLock streamLock(&m_mutex);
        streamLock.lock();
        producer->setThreadId(producerThreadId);
        /* Set create thread num for sync quit. */
        m_createThreadNum++;
        /* Assume all stream threads build successfully for sync quit process. */
        StreamPair* tmpPair = producer->getPair();
        tmpPair->createThreadNum = tmpPair->expectThreadNum;
        streamLock.unLock();

        Assert(m_createThreadNum <= m_size);
        STREAM_LOG(DEBUG2,
            "startup stream thread for "
            "producer(%lu, %u, %u), thread id %lu, %d/%d in top consumer",
            producer->getKey().queryId,
            producer->getKey().planNodeId,
            producer->getKey().smpIdentifier,
            producerThreadId,
            m_createThreadNum,
            m_size);
    } else {
        producer->releaseNetPort();
        int childslots = ((StreamProducer*)producer)->getChildSlot();
        // release only if StreamProducer has called setChildSlot(AssignPostmasterChildSlot()) before
        if (childslots > 0)
            ReleasePostmasterChildSlot(childslots);
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("failed to startup stream thread, NodeName: %s, key(%lu, %u): %m",
                    g_instance.attr.attr_common.PGXCNodeName ? g_instance.attr.attr_common.PGXCNodeName : "",
                    producer->getKey().queryId,
                    producer->getKey().planNodeId)));
    }
}

/*
 * @Description: In the same node group?
 *
 * @param[IN] pid1:  thread id1
 * @param[IN] pid2:  thread id2
 * @return: true if in same node group
 */
bool StreamNodeGroup::inNodeGroup(ThreadId pid1, ThreadId pid2)
{
    if (pid1 == pid2)
        return false;

    bool flag = false;

    if (pid1 == m_pid || pid2 == m_pid) {
        for (int i = 1; i < m_streamNum; i++) {
            if (m_streamArray[i].streamObj != NULL &&
                (m_streamArray[i].streamObj->getThreadId() == pid2 ||
                m_streamArray[i].streamObj->getThreadId() == pid1)) {
                flag = true;
                break;
            }
        }
    }

    return flag;
}

/*
 * @Description: Restore stream enter
 *
 * @return: void
 */
void StreamNodeGroup::restoreStreamEnter()
{
    AutoMutexLock streamLock(&m_mutex);

    streamLock.lock();
    m_streamEnter--;
    streamLock.unLock();
}

/*
 * @Description: Set the stop flag
 *
 * @param[IN] groupIdx:  group index
 * @param[IN] stopFlag:  stop flag
 * @return: void
 */
void StreamNodeGroup::setStopFlagPoint(int groupIdx, bool* stopFlag)
{
    m_streamArray[groupIdx].stopFlag = stopFlag;
}

/*
 * @Description: Set the node status.
 *
 * @param[IN] groupIdx:  group index
 * @param[IN] status:  object status
 * @return: void
 */
void StreamNodeGroup::setNodeStatus(int groupIdx, StreamObjStatus status)
{
    m_streamArray[groupIdx].status = status;
}

/*
 * @Description: Set need clean flag
 *
 * @param[IN] flag:  need clean flag
 * @return: void
 */
void StreamNodeGroup::setNeedClean(bool flag)
{
    m_needClean = flag;
}

/*
 * @Description: save the first error data of producer
 *
 * @return: void
 */
void StreamNodeGroup::saveProducerEdata()
{
    if (gs_compare_and_swap_32(&m_edataWriteProtect, EDATA_WRITE_ENABLE, EDATA_WRITE_START) == false)
        return;

    Assert(m_streamRuntimeContext != NULL);
    AutoContextSwitch streamCxtGuard(m_streamRuntimeContext);
    m_producerEdata = CopyErrorData();

    (void)gs_compare_and_swap_32(&m_edataWriteProtect, EDATA_WRITE_START, EDATA_WRITE_FINISH);
}

/*
 * @Description: get the saved error data of producer
 *
 * @return: the saved error data
 */
ErrorData* StreamNodeGroup::getProducerEdata()
{
    if (gs_compare_and_swap_32(&m_edataWriteProtect, EDATA_WRITE_FINISH, ERROR_READ_FINISH) == false)
        return NULL;

    return m_producerEdata;
}

/*
 * @Description: Destory a stream Node group, clean some status
 *
 * @param[IN] status:  object status
 * @return: void
 */
void StreamNodeGroup::destroy(StreamObjStatus status)
{
    /* Only top consumer responsible for destroying stream node group */
    if (StreamTopConsumerAmI() == false)
        return;

#ifdef USE_SPQ
    disconnect_qc_conn();
#endif
    /* We must relase all pthread mutex by my thread, Or it will dead lock. But it is not a good solution. */
    // lock the same thread mutex can't be conflict in one thread.
    ResourceOwnerReleaseAllXactPthreadMutex();

    WaitState oldStatus = pgstat_report_waitstatus(STATE_STREAM_WAIT_NODEGROUP_DESTROY);

    /*
     * If top consumer is canceled before BuildStreamFlow, streamNodeGroup is not initialized.
     * At this time, we should release net port left in hash table here.
     */
    if (STREAM_ERROR == status)
        StreamObj::releaseNetPortInHashTable();

    /* Destroy the stream node group. */
    if (u_sess->stream_cxt.global_obj != NULL) {
#ifndef ENABLE_MULTIPLE_NODES
        if (u_sess->stream_cxt.global_obj->m_portal != NULL) {
            u_sess->stream_cxt.global_obj->m_portal->streamInfo.Reset();
        }
#endif
        u_sess->stream_cxt.global_obj->deInit(status);
        delete u_sess->stream_cxt.global_obj;
        u_sess->stream_cxt.global_obj = NULL;
    }

    /* Destroy the global instrumentation. */
    if (u_sess->instr_cxt.global_instr != NULL) {
        delete u_sess->instr_cxt.global_instr;
        u_sess->instr_cxt.thread_instr = NULL;
        u_sess->instr_cxt.global_instr = NULL;
    }

    /* Release stream context. */
    DeinitStreamContext();
    pgstat_report_waitstatus(oldStatus);
}

/*
 * @Description: Synchronize quit
 *
 * @param[IN] status:  object status
 * @return: void
 */
void StreamNodeGroup::syncQuit(StreamObjStatus status)
{
    /* Only stream thread or top consumer need sync quit */
    if (IS_PGXC_COORDINATOR || (StreamTopConsumerAmI() == false && StreamThreadAmI() == false) ||
        u_sess->stream_cxt.enter_sync_point == true)
        return;

    /* add trace info while smp not correct. */
    if (t_thrd.log_cxt.errordata_stack_depth == (ERRORDATA_STACK_SIZE - 1) && StreamTopConsumerAmI()) {
        if (u_sess->stream_cxt.global_obj != NULL) {
            ereport(LOG, (errmsg("[StreamSyncQuit] global_obj: %lu, runtime_mem_cxt: %lu",
                (uint64)u_sess->stream_cxt.global_obj, (uint64)u_sess->stream_cxt.stream_runtime_mem_cxt)));
            return;
        }
    }

    /* We must relase all pthread mutex by my thread, Or it will dead lock. But it is not a good solution. */
    // lock the same thread mutex can't be conflict in one thread.
    ResourceOwnerReleaseAllXactPthreadMutex();

    WaitState oldStatus = pgstat_report_waitstatus(STATE_STREAM_WAIT_THREAD_SYNC_QUIT);

    if (IS_THREAD_POOL_STREAM) {
        volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
        if (beentry != NULL) {
            STREAM_LOG(DEBUG2, "[StreamPool] SyncQuit query_id %lu, tlevel %u, smpid %u",
                beentry->st_queryid, beentry->st_thread_level, beentry->st_smpid);
        }
    }

    if (StreamTopConsumerAmI()) {
        StreamNodeGroup::revokeStreamConnectPermission();
    }

    if (u_sess->stream_cxt.global_obj != NULL) {
#ifdef ENABLE_MULTIPLE_NODES
        if (status == STREAM_ERROR) {
#else
        if (status == STREAM_ERROR ||
            unlikely(u_sess->stream_cxt.global_obj->GetStreamQuitStatus() == STREAM_ERROR)) {
#endif
            u_sess->stream_cxt.global_obj->MarkSyncControllerStopFlagAll();
        }

        if (StreamTopConsumerAmI()) {
            /*
             * If an error is encountered during receiveing data, top consumer will
             * come to sync point and wait all stream threads. At this time, error message
             * may not be sent to Coordinator immediately(release net port and report
             * error are behind sync point). If some producers need long time to finish
             * sending data to other normal consumers, it will also take long time to
             * report error. So we should signal all stream thread to cancel query in this
             * case and then the whole query can be canceled quickly.
             */
            if (STREAM_ERROR == status)
                StreamNodeGroup::cancelStreamThread();

            /* before top consumer enter the sync point, it must unregister to make sure the fd is closed. */
            u_sess->stream_cxt.global_obj->unregisterStream(0, status);
        } else if (StreamThreadAmI()) {
            Assert(u_sess->stream_cxt.producer_obj != NULL);
            u_sess->stream_cxt.producer_obj->deInit(status);
        }

        u_sess->stream_cxt.global_obj->quitSyncPoint();

        /* After syncpoint, all these objects may have been freed by topConsumer. */
        if (StreamThreadAmI()) {
            u_sess->stream_cxt.producer_obj = NULL;
        }
    }

    u_sess->stream_cxt.enter_sync_point = true;
    pgstat_report_waitstatus(oldStatus);
}

void StreamNodeGroup::ReleaseStreamGroup(bool resetSession, StreamObjStatus status)
{
    if (u_sess->stream_cxt.global_obj != NULL) {
        StreamTopConsumerIam();
        /* Set sync point for waiting all stream threads complete. */
        StreamNodeGroup::syncQuit(status);
        UnRegisterStreamSnapshots();
        StreamNodeGroup::destroy(status);
        if (!resetSession) {
            /* reset some flag related to stream */
            ResetStreamEnv();
        }
    }
    if (resetSession) {
        ResetSessionEnv();
    }
}

/*
 * @Description: Clear the stream node group
 *
 * @param[IN] status:  object status
 * @return: void
 */
void StreamNodeGroup::deInit(StreamObjStatus status)
{
    ListCell* cell = NULL;
    StreamConsumer* consumer = NULL;
    StreamProducer* producer = NULL;
    bool saveQuit = false;
    AutoMutexLock streamLock1(&m_mutex);

    do {
        streamLock1.lock();
        if (m_streamEnter <= 0 && m_streamEnterCount >= m_createThreadNum)
            saveQuit = true;
        streamLock1.unLock();

        pg_usleep(1000);
    } while (saveQuit == false);

    /* release the socket in case not release. */
    foreach (cell, m_streamProducerList) {
        producer = (StreamProducer*)lfirst(cell);

        /*
         * 1. For TCP, close fd of producer.
         * 2. Consumer can accept connection after handling cancel signal.
         *  For SCTP, if top consumer get an error, close sctp stream of producer
         *  in case not release(producer complete execution), so that receiver
         *  stream can be closed by gs_receivers_flow_controller.
         */
        if (STREAM_ERROR == status)
            producer->releaseNetPort(); /* producer release socket will be enough. */
    }

    /* release the socket in case not release. */
    foreach (cell, m_streamConsumerList) {
        consumer = (StreamConsumer*)lfirst(cell);
        consumer->deInit();
    }

    /* Free the synccontroller list */
    if (m_syncControllers != NIL) {
        ListCell* lc = NULL;
        foreach (lc, m_syncControllers) {
            SyncController* sc = (SyncController*)lfirst(lc);
            ExecSyncControllerDelete(sc);
            pfree_ext(sc);
        }

        list_free(m_syncControllers);
        m_syncControllers = NIL;
    }

    m_streamRuntimeContext = NULL;

    /*
     * 1. If length of m_streamPairList is not the same as m_size(number of stream exists in plan tree),
     * it means that stream connection and stream thread initialization may not finish yet due to
     * cancel signal.
     * 2. m_needClean is true if some producer not ready and element in stream info hash table can not
     * get removed in wakeUpConsumer, or consumer get canceled when waiting producer ready.
     * In these two case, we should release net port cached in hash table instead of just releasing
     * consumer in m_streamPairList.
     */
    if ((list_length(m_streamPairList) != m_size) || m_needClean)
        StreamObj::releaseNetPortInHashTable();
#ifdef ENABLE_MULTIPLE_NODES
    ThreadId pid = gs_thread_self();
    AutoMutexLock streamLock2(&m_streamNodeGroupLock);

    streamLock2.lock();
    (StreamNodeElement*)hash_search(m_streamNodeGroupTbl, &pid, HASH_REMOVE, NULL);
    streamLock2.unLock();
#endif
    pthread_cond_destroy(&m_cond);
    pthread_mutex_destroy(&m_mutex);
    pthread_mutex_destroy(&m_recursiveMutex);
}

/*
 * @Description: Stop all register stream thread in the node group
 *
 * @param[IN] pid:  thread id
 * @return: void
 */
void StreamNodeGroup::stopAllThreadInNodeGroup(ThreadId pid, uint64 query_id)
{
    bool found = false;
    StreamNodeElement* element = NULL;
    AutoMutexLock streamLock(&m_streamNodeGroupLock);

    streamLock.lock();
    /* Search in the stream node group. */
    element = (StreamNodeElement*)hash_search(m_streamNodeGroupTbl, &pid, HASH_FIND, &found);
    if (found == true) {
        if (query_id == element->value->GetQueryId()) {
            element->value->stopThread();
        } else {
            ereport(DEBUG2,
                (errmodule(MOD_STREAM),
                    errcode(ERRCODE_LOG),
                    errmsg("receive stop signal from queryid %lu, but current queryid is %lu.",
                        query_id,
                        element->value->GetQueryId())));
        }
    } else if (t_thrd.comm_cxt.LibcommThreadType == LIBCOMM_SEND_CTRL) {
        u_sess->stream_cxt.stop_query_id = query_id;
        t_thrd.sig_cxt.gs_sigale_check_type = SIGNAL_CHECK_EXECUTOR_STOP;
        (void)SendProcSignalForLibcomm(pid, PROCSIG_EXECUTOR_FLAG, InvalidBackendId);
    } else {
        u_sess->stream_cxt.stop_mythread = true;
        u_sess->stream_cxt.stop_pid = pid;
        u_sess->stream_cxt.stop_query_id = query_id;
    }

    streamLock.unLock();
}

/*
 * @Description: Set the executor stop flag to true
 *
 * @return: void
 */
void StreamNodeGroup::stopThread()
{
    for (int i = 0; i < m_streamNum; i++) {
        if (m_streamArray[i].stopFlag != NULL)
            *(m_streamArray[i].stopFlag) = true;
    }
}

/*
 * @Description: Grant stream connect permission
 *
 * @return: void
 */
void StreamNodeGroup::grantStreamConnectPermission()
{
    bool found = false;
    AutoMutexLock streamLock(&m_streamConnectSyncLock);

    HOLD_INTERRUPTS(); /* add this macro for double safety */
    streamLock.lock();
    StreamConnectSyncElement* element =
        (StreamConnectSyncElement*)hash_search(m_streamConnectSyncTbl, &u_sess->debug_query_id, HASH_ENTER, &found);

    if (!found) {
        element->key = u_sess->debug_query_id;
    } else {
        streamLock.unLock();
        ereport(ERROR,
            (errmodule(MOD_STREAM),
                errcode(ERRCODE_STREAM_DUPLICATE_QUERY_ID),
                errmsg("Distribute query failed due to duplicate query id")));
    }
    streamLock.unLock();
    RESUME_INTERRUPTS();
}

/*
 * @Description: Revoke stream connect permission
 *
 * @return: void
 */
void StreamNodeGroup::revokeStreamConnectPermission()
{
    AutoMutexLock streamLock(&m_streamConnectSyncLock);

    HOLD_INTERRUPTS(); /* add this macro for double safety */
    streamLock.lock();
    (void)hash_search(m_streamConnectSyncTbl, &u_sess->debug_query_id, HASH_REMOVE, NULL);
    streamLock.unLock();
    RESUME_INTERRUPTS();
}

/*
 * @Description: Check if can accept connection
 *
 * @param[IN] query_id:  debug query id
 * @return: true if can accept connection
 */
bool StreamNodeGroup::checkStreamConnectPermission(uint64 query_id)
{
    AutoMutexLock streamLock(&m_streamConnectSyncLock);
    bool found = false;

    streamLock.lock();
    (void)hash_search(m_streamConnectSyncTbl, &query_id, HASH_FIND, &found);
    if (!found) {
        streamLock.unLock();
        return false;
    }

    streamLock.unLock();
    return true;
}

/*
 * --------------------------------------------------------------------------------------
 * MPPDB Recurieve Query Support
 * --------------------------------------------------------------------------------------
 */

void StreamNodeGroup::SyncConsumerNextPlanStep(int controller_plannodeid, int stepno)
{
    /* We don't have todo any-thing */
    if (!IS_PGXC_DATANODE) {
        return;
    }

    /* We don't have todo step syncup when there is no stream operator in execution plan */
    if (u_sess->stream_cxt.global_obj == NULL) {
        return;
    }

    StreamNodeGroup* stream_nodegroup = u_sess->stream_cxt.global_obj;
    SyncController* controller = stream_nodegroup->GetSyncController(controller_plannodeid);

    /* Report Error when the controller is not found */
    if (controller == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("MPP With-Recursive sync controller for PlanNode[%d] is not found", controller_plannodeid)));
    }

    switch (controller->controller_type) {
        case T_RecursiveUnion: {
            ExecSyncRecursiveUnionConsumer((RecursiveUnionController*)controller, stepno);
        } break;
        case T_Stream:
        case T_VecStream: {
            ExecSyncStreamConsumer((StreamController*)controller);
        } break;

        default:
            elog(ERROR, "Unsupported SyncProducerType %d", controller->controller_type);
    }

    return;
}

/*
 * Function: SyncProducerNextPlanStep()
 *
 * @Description: Global Synchronizer's interface for Prodeucer side
 *
 * @param[IN] p1: add parameter description
 * @param[IN] p1: add parameter description
 *
 * @return: no return
 */
void StreamNodeGroup::SyncProducerNextPlanStep(int controller_plannodeid, int producer_plannodeid, int step,
    int tuple_count, bool* need_rescan, int target_iteration)
{
    /* We don't have todo any-thing */
    if (!IS_PGXC_DATANODE) {
        return;
    }

    /* We don't have todo step syncup when there is no stream operator in execution plan */
    if (u_sess->stream_cxt.global_obj == NULL) {
        return;
    }

    StreamNodeGroup* stream_nodegroup = u_sess->stream_cxt.global_obj;
    SyncController* controller = NULL;
    while (true) {
        controller = stream_nodegroup->GetSyncController(controller_plannodeid);
        if (controller != NULL && controller->controller_planstate != NULL) {
            break;
        }

        pgstat_report_waitstatus(STATE_WAIT_SYNC_PRODUCER_NEXT_STEP);
        WITH_RECURSIVE_SYNCPOINT_WAIT_INTERVAL;
    }

    /*
     * Report Error when the controller for its belonging RecursiveUnion node is
     * not found.
     */
    if (controller == NULL) {
        elog(ERROR,
            "MPP with-recursive. controller is not found in SyncProducerNextPlanStep top:%s",
            producer_top_plannode_str);
    }

    /*
     * Normally, we should do sync-up on producer side at the end of execution, however
     * the corresponding consumer may encounter a "short-circuit" case where it does not
     * have the opportunity to invoke ExecStream to reach the end of current thread (send
     * 'Z' or 'R'), so in this case we need send 'R' forcely. to tell other datanodes we
     * have done.
     */
    if (controller->executor_stop) {
        u_sess->exec_cxt.executorStopFlag = true;

        /* Send 'Z' node */
        stream_nodegroup->ProducerFinishRUIteration(step);

        /* Send 'R' node */
        stream_nodegroup->ProducerSendSyncMessage(
            (RecursiveUnionController*)controller, producer_plannodeid, step, 0, RUSYNC_MSGTYPE_NODE_FINISH);
    } else {
        switch (controller->controller_type) {
            case T_RecursiveUnion: {
                ExecSyncRecursiveUnionProducer((RecursiveUnionController*)controller,
                    producer_plannodeid,
                    step,
                    tuple_count,
                    need_rescan,
                    target_iteration);
            } break;
            case T_Stream: {
                ExecSyncStreamProducer((StreamController*)controller, need_rescan, target_iteration);
            } break;
            default:
                elog(ERROR, "Unsupported SyncProducerType %d", controller->controller_type);
        }
    }

    /* Other thread failed, we need return error immediately */
    if (u_sess->stream_cxt.global_obj->m_errorStop) {
        ereport(ERROR, (errcode(ERRCODE_RU_STOP_QUERY), errmsg("error happened during execute query")));
    }

    pgstat_report_waitstatus(STATE_WAIT_UNDEFINED);
    return;
}

void StreamNodeGroup::ConsumerGetSyncUpMessage(
    RecursiveUnionController* controller, int step, StreamState* node, char msg_type)
{
    if (unlikely(node->isReady == false))
        StreamPrepareRequestForRecursive(node, true);

    if (msg_type == RUSYNC_MSGTYPE_NODE_FINISH) {
        /* For message 'R', we only recieve it from worker-node */
        ConsumerNodeSyncUpMessage(controller, step, node);
    } else {
        Assert(false);
    }

    return;
}

void StreamNodeGroup::ConsumerMarkRUStepStatus(RecursiveUnionController* controller, int step, int iteration,
    StreamState* node, int pgxc_nodeid, int producer_plannodeid, int tuple_processed)
{
    switch (step) {
        case WITH_RECURSIVE_SYNC_NONERQ: {
            if (pgxc_nodeid == u_sess->pgxc_cxt.PGXCNodeId) {
                /* Receive message from self DN, we need verify it has been updated */
                if (controller->none_recursive_tuples[pgxc_nodeid] == -1) {
                    elog(ERROR,
                        "MPP with-recursive step1 (C) is not set on datanode %s",
                        g_instance.attr.attr_common.PGXCNodeName);
                }
            }

            controller->none_recursive_tuples[pgxc_nodeid] = tuple_processed;
        } break;
        case WITH_RECURSIVE_SYNC_RQSTEP: {
            if (pgxc_nodeid == u_sess->pgxc_cxt.PGXCNodeId) {
                /* Receive message from self DN, we need verify it has been updated */
                if (controller->recursive_tuples[pgxc_nodeid] == -1) {
                    elog(ERROR,
                        "MPP with-recursive step2 (C) is not set on datanode %s",
                        g_instance.attr.attr_common.PGXCNodeName);
                }
            }

            /* Coordinator's step2.iteration is updated by itself, not from message */
            controller->recursive_tuples[pgxc_nodeid] = tuple_processed;
        } break;
        case WITH_RECURSIVE_SYNC_DONE: {
            controller->recursive_union_finish = true;
            controller->recursive_finished = true;
            RECURSIVE_LOG(LOG,
                "MPP with-recursive step3 (C) RECEIVE recursive-end messages on DN [%s]",
                g_instance.attr.attr_common.PGXCNodeName);
        } break;
        default:
            elog(ERROR, "unsupported steps");
    }

    return;
}

/*
 * Function: ProducerFinishRUIteration
 *
 * @Description: In recursive CTE execution, the plan-tree under the stream node could
 * be re-scaned, at the end of each iteration we need do similar as we did in function
 * execute_stream_end() to let the consumer to "know" that current execution is done so
 * the consumer side can do cluster-syncup
 *
 * @param[IN] step: indicate the current stepno normally for debug print
 *
 * @return: no rturn value
 */
void StreamNodeGroup::ProducerFinishRUIteration(int step)
{
    int i, res;
    StreamProducer* producer = u_sess->stream_cxt.producer_obj;
    int consumer_number = producer->getConnNum();
    StreamTransport** transport = producer->getTransport();

    elog(DEBUG1, "MPP with-recursive step%d send 'Z' to consumer start >>> %s", step, producer_top_plannode_str);

    /*
     * prepare an end message to all the consumer backend thread.
     * if is dummy, do not bother send
     */
    if (producer->isDummy() == false) {
        for (i = 0; i < consumer_number; i++) {
            if (producer->m_originConsumerNodeList != NIL && !list_member_int(producer->m_originConsumerNodeList, i)) {
                continue;
            }

            if (producer->netSwitchDest(i)) {
                if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3) {
                    StringInfoData buf;

                    pq_beginmessage(&buf, 'Z');
                    pq_sendbyte(&buf, TransactionBlockStatusCode());
                    pq_endmessage(&buf);
                } else if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 2)
                    pq_putemptymessage('Z');
                /* Flush output at end of cycle in any case. */
                res = pq_flush();
                if (res == EOF) {
                    transport[i]->release();
                }
                producer->netStatusSave(i);
            }
        }
    }

    /* Send a final signal to consumer for local stream, and producer can be triggered next one run. */
    producer->finalizeLocalStream();

    elog(DEBUG1, "MPP with-recursive step%d send 'Z' to consumer done <<< %s", step, producer_top_plannode_str);

    return;
}

/*
 * Function ProducerSendSyncMessage()
 *
 * @Description: syncup message sender API for producer thread
 *
 * @param[IN] @controller: the recursive-union controller's pointer
 * @param[IN] @producer_plan_nodeid: the producer's plan nodeid
 * @param[IN] @step. recursive_union processing step
 * @param[IN] @tuple_count: numer of tuple that will send with the message
 *
 * @return: none
 */
void StreamNodeGroup::ProducerSendSyncMessage(
    RecursiveUnionController* controller, int producer_plannodeid, int step, int tuple_count, char msg_type)
{
    Assert(u_sess->stream_cxt.global_obj != NULL);

    StreamProducer* producer = u_sess->stream_cxt.producer_obj;
    int consumer_number = producer->getConnNum();
    StreamTransport** transport = producer->getTransport();
    int iteration = 0;

    /* Only syncup producer thread need send message */
    if (!IsSyncUpProducerThread()) {
        return;
    }

    /* Only Sync-Producer need send sync-up message to control-node */
    if (!StreamNodeGroup::IsRUSyncProducer()) {
        return;
    }

    /*
     * Specify the iteration step in 'F' & 'R' to let the consumer side todo step
     * verification.
     */
    if (step == WITH_RECURSIVE_SYNC_DONE) {
        iteration = u_sess->attr.attr_sql.max_recursive_times;
    } else {
        iteration = controller->iteration;
    }

    /* Producer send sync-up message to each DN's consumer */
    if (producer->isDummy() == false) {
        for (int connIdx = 0; connIdx < consumer_number; connIdx++) {
            if (producer->netSwitchDest(connIdx)) {
                StringInfoData buf;
                initStringInfo(&buf);

                /* Prepare 'R' message */
                pq_beginmessage(&buf, msg_type);
                appendStringInfo(&buf, "nodename:%s,", g_instance.attr.attr_common.PGXCNodeName);
                appendStringInfo(&buf, "rustep:%d,", step);
                appendStringInfo(&buf, "iteration:%d,", iteration); /* iteration that already finished */
                appendStringInfo(&buf, "xcnodeid:%d,", u_sess->pgxc_cxt.PGXCNodeId);
                appendStringInfo(&buf, "controller_plannodeid:%d,", controller->controller.controller_plannodeid);
                appendStringInfo(&buf, "producer_plannodeid:%d,", producer_plannodeid);
                appendStringInfo(&buf, "tuple_processed:%d,", tuple_count);

                pq_endmessage(&buf);

                int iter = -1;
                if (msg_type == RUSYNC_MSGTYPE_NODE_FINISH) {
                    iter = controller->iteration;
                } else {
                    /* Can not get here */
                    Assert(false);
                }

                elog(DEBUG1,
                    "MPP with-recursive step%d (P) send step-sync message '%c' iteration[%d] to %s(nodeid:%u) %s",
                    step,
                    msg_type,
                    iter,
                    transport[connIdx]->m_nodeName,
                    transport[connIdx]->m_nodeoid,
                    producer_top_plannode_str);

                /* Flush output at end of cycle in any case. */
                int res = pq_flush();
                if (res == EOF) {
                    transport[connIdx]->release();
                }

                producer->netStatusSave(connIdx);
            }
        }
    }

    return;
}

/*
 * @Function: IsRecursiveUnionDone()
 *
 * @Description: verity if recursive union is finished across the entire cluster
 * normally, we check the global controller's recursive_union_finish flag
 *
 * @param[IN] state: the recursive-union planstate objects
 *
 * @return: return true means the current recursive-union is done
 */
bool StreamNodeGroup::IsRecursiveUnionDone(RecursiveUnionState* state)
{
    /* If no sync-up step required, just return TRUE */
    if (state != NULL && !NeedSyncUpRecursiveUnionStep((Plan*)state->ps.plan)) {
        return true;
    }

    /* We don't have todo step syncup when there is no stream operator in execution plan */
    if (u_sess->stream_cxt.global_obj == NULL) {
        return true;
    }

    Assert(state != NULL && state->rucontroller != NULL);

    return state->rucontroller->recursive_union_finish;
}

/*
 * @Function: AddSyncController()
 *
 * @Description: static function that used to register given controller to stream
 * nodegroup
 *
 * @param[IN] controller: the controller pointer that need add into global controller
 * list
 *
 * @return: no return value
 */
void StreamNodeGroup::AddSyncController(SyncController* controller)
{
    AutoMutexLock streamLock(&m_recursiveMutex);
    streamLock.lock();
    {
        u_sess->stream_cxt.global_obj->m_syncControllers =
            lappend(u_sess->stream_cxt.global_obj->m_syncControllers, (void*)controller);
    }
    streamLock.unLock();

    return;
}

/*
 * @Function: GetSyncController()
 *
 * @Description: static function that used to fetch controller with given controller id
 * nodegroup
 *
 * @param[IN] controller_plannodeid: the id of controller that need to fetch
 *
 * @return: no return value
 */
SyncController* StreamNodeGroup::GetSyncController(int controller_plannodeid)
{
    Assert(u_sess->stream_cxt.global_obj != NULL && controller_plannodeid > 0);

    SyncController* result = NULL;
    AutoMutexLock streamLock(&m_recursiveMutex);

    streamLock.lock();
    {
        ListCell* lc = NULL;
        foreach (lc, u_sess->stream_cxt.global_obj->m_syncControllers) {
            SyncController* controller = (SyncController*)lfirst(lc);

            if (controller->controller_plannodeid == controller_plannodeid) {
                result = controller;
            }
        }

        /* Other thread failed, we need return error immediately */
        if (u_sess->stream_cxt.global_obj->m_errorStop) {
            streamLock.unLock();
            ereport(ERROR, (errcode(ERRCODE_RU_STOP_QUERY), errmsg("error happened during execute query")));
        }
    }
    streamLock.unLock();

    return result;
}

/*
 * Mark executor stop flag for all sync controller
 */
void StreamNodeGroup::MarkSyncControllerStopFlagAll()
{
    if (u_sess->stream_cxt.global_obj != NULL) {
        AutoMutexLock streamLock(&m_recursiveMutex);
        streamLock.lock();

        ListCell* lc = NULL;
        foreach (lc, u_sess->stream_cxt.global_obj->m_syncControllers) {
            SyncController* controller = (SyncController*)lfirst(lc);
            controller->executor_stop = true;
        }
        u_sess->stream_cxt.global_obj->m_errorStop = true;
        streamLock.unLock();
    }
}

uint64 StreamNodeGroup::GetQueryId()
{
    if (m_streamArray != NULL && m_streamArray[0].streamObj != NULL) {
        return m_streamArray[0].streamObj->getKey().queryId;
    } else {
        return 0;
    }
}

/*
 * @Function: IsRUCoordNode()
 *
 * @Description: function to indicate if current datanode is the control node in
 * recursive-union sync-up process
 *
 * @param[IN] no
 *
 * @return: bool
 */
bool StreamNodeGroup::IsRUCoordNode()
{
    return (u_sess->pgxc_cxt.PGXCNodeId == 0);
}

bool StreamNodeGroup::IsRUSyncProducer()
{
    return true;
}

/*
 * @Function: ConsumerControllerNodeSyncUpMessage()
 *
 * @Description: Read from StreamState's consumer list to receive sync-up messages 'R'
 *
 * input param @controller: recursive controller
 * input param @step: the step number we want to sync-up currently
 * input param @streamstate: the planstate for sync-up stream node
 *
 * @return: no-return
 */
static void ConsumerNodeSyncUpMessage(RecursiveUnionController* controller, int step, StreamState* node)
{
    int connIdx = 0;
    int connCount = 0;
    PGXCNodeHandle** connections = NULL;

    connCount = node->conn_count;
    connections = node->connections;

    /* Handle data from connections until the number of conections without return data becomes 0 */
    while (connCount) {
        if (node->need_fresh_data) {
            if (datanode_receive_from_logic_conn(node->conn_count, connections, &node->netctl, -1)) {
                int error_code = getStreamSocketError(gs_comm_strerror());
                ereport(ERROR,
                    (errcode(error_code),
                        errmsg("MPP with-recursive step%d, %s failed [code:%d] to read response 'R' from Datanodes. "
                               "Detail: %s\n",
                            step,
                            g_instance.attr.attr_common.PGXCNodeName,
                            error_code,
                            gs_comm_strerror())));
            }
        }

        for (connIdx = 0; connIdx < node->conn_count; connIdx++) {
            /* conection has return data */
            if (connections[connIdx]->state == DN_CONNECTION_STATE_IDLE) {
                continue;
            }

            /* Handle data from connections */
            int res = HandleStreamResponse(connections[connIdx], node);
            if (res == RESPONSE_RECURSIVE_SYNC_R) {
                /* receive 'R' message, change conection's state, number of conections without return data -1 */
                connections[connIdx]->state = DN_CONNECTION_STATE_IDLE;
                connCount--;
                elog(DEBUG1,
                    "MPP with-recursive step%d (C) receive@ ru-sync message 'R' from node:%d",
                    step,
                    connections[connIdx]->nodeIdx);
            } else if (res == RESPONSE_COMPLETE) {
                /* receive unused 'Z' message, in case the unused message blocking connections */
                connections[connIdx]->state = DN_CONNECTION_STATE_QUERY;
                ereport(DEBUG1,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("MPP with-recursive step%d (C) receive unused message 'Z' from node:%d",
                            step,
                            connections[connIdx]->nodeIdx)));
            } else {
                elog(DEBUG1, "MPP with-recursive [error] receive unexpected message msg_type %d", res);
            }
        }
    }
}

/*
 * @Function: ConsumerNodeStreamMessage()
 *
 * @Description: Read from StreamState's consumer list to receive end messages 'Z'
 *
 * input param @streamstate: the planstate for sync-up stream node
 *
 * @return: no-return
 */
void StreamNodeGroup::ConsumerNodeStreamMessage(StreamState* node)
{
    int connIdx = 0;
    int connCount = 0;
    PGXCNodeHandle** connections = NULL;

    if (unlikely(node->isReady == false))
        StreamPrepareRequestForRecursive(node, true);

    connCount = node->conn_count;
    connections = node->connections;

    /* Handle data from connections until the number of conections without return data becomes 0 */
    while (connCount) {
        if (node->need_fresh_data) {
            if (datanode_receive_from_logic_conn(node->conn_count, connections, &node->netctl, -1)) {
                int error_code = getStreamSocketError(gs_comm_strerror());
                ereport(ERROR,
                    (errcode(error_code),
                        errmsg(
                            "MPP with-recursive, %s failed [code:%d] to read response 'Z' from Datanodes. Detail: %s\n",
                            g_instance.attr.attr_common.PGXCNodeName,
                            error_code,
                            gs_comm_strerror())));
            }
        }

        for (connIdx = 0; connIdx < node->conn_count; connIdx++) {
            /* conection has return data */
            if (connections[connIdx]->state == DN_CONNECTION_STATE_IDLE) {
                continue;
            }

            /* Handle data from connections */
            int res = HandleStreamResponse(connections[connIdx], node);
            if (res == RESPONSE_COMPLETE) {
                /* receive 'Z' message, conection's state has changed, number of conections without return data -1 */
                connCount--;
                elog(DEBUG1, "MPP with-recursive(C) receive message 'Z' from node:%d", connections[connIdx]->nodeIdx);
            } else if (res == RESPONSE_RECURSIVE_SYNC_R) {
                /* receive unexpect 'R' message */
                elog(ERROR,
                    "MPP with-recursive(C) receive message 'R' from stream node:%d",
                    connections[connIdx]->nodeIdx);
            } else {
                elog(DEBUG1, "MPP with-recursive [error] receive other message msg_type %d", res);
            }
        }
    }
}

/* Mark recursive vfd is invalid before aborting transaction. */
void StreamNodeGroup::MarkRecursiveVfdInvalid()
{
    /* Only stream thread or top consumer need invalidate recursive vfd */
    if (IS_PGXC_COORDINATOR || (StreamTopConsumerAmI() == false && StreamThreadAmI() == false)) {
        return;
    }

    bool processRecursive = IsThreadProcessStreamRecursive();
    if (unlikely(processRecursive == true)) {
        AutoMutexLock recursiveLock(u_sess->stream_cxt.global_obj->GetRecursiveMutex());
        recursiveLock.lock();
        u_sess->stream_cxt.global_obj->SetRecursiveVfdInvalid();
        recursiveLock.unLock();
    }
}

#ifndef ENABLE_MULTIPLE_NODES
bool InitStreamObject(PlannedStmt* planStmt)
{
    /* if plan contains stream node, shoule initial some object */
    if (planStmt->num_streams > 0 && !StreamTopConsumerAmI() && !StreamThreadAmI()) {
        /* Set top consumer at the very beginning. */
        StreamTopConsumerIam();
        /* Build stream context for stream plan. */
        InitStreamContext();

        u_sess->exec_cxt.under_stream_runtime = true;
        return true;
    }
    return false;
}

void StreamMarkStop()
{
    u_sess->exec_cxt.executorStopFlag = true;
}
#endif
