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
 * threadpool_group.cpp
 *        Thread pool group controls listener and worker threads.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/process/threadpool/threadpool_group.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "threadpool/threadpool.h"

#include "access/xact.h"
#include "catalog/pg_collation.h"
#include "distributelayer/streamProducer.h"
#include "gssignal/gs_signal.h"
#include "lib/dllist.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "storage/pmsignal.h"
#include "tcop/dest.h"
#include "utils/atomic.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "executor/executor.h"

#define WORKER_MAY_HANG(status) (status == STATE_WAIT_COMM || \
                                status == STATE_STREAM_WAIT_CONNECT_NODES || \
                                status == STATE_STREAM_WAIT_PRODUCER_READY || \
                                status == STATE_WAIT_XACTSYNC)

ThreadPoolGroup::ThreadPoolGroup(int maxWorkerNum, int expectWorkerNum, int maxStreamNum,
                                 int groupId, int numaId, int cpuNum, int* cpuArr, bool enableBindCpuNuma)
    : m_listener(NULL),
      m_maxWorkerNum(maxWorkerNum),
      m_maxStreamNum(maxStreamNum),
      m_defaultWorkerNum(expectWorkerNum),
      m_workerNum(0),
      m_listenerNum(0),
      m_expectWorkerNum(expectWorkerNum),
      m_idleWorkerNum(0),
      m_pendingWorkerNum(0),
      m_streamNum(0),
      m_idleStreamNum(0),
      m_sessionCount(0),
      m_waitServeSessionCount(0),
      m_processTaskCount(0),
      m_hasHanged(0),
      m_groupId(groupId),
      m_numaId(numaId),
      m_groupCpuNum(cpuNum),
      m_groupCpuArr(cpuArr),
      m_enableNumaDistribute(false),
      m_enableBindCpuNuma(enableBindCpuNuma),
      m_workers(NULL),
      m_context(NULL)
{
    pthread_mutex_init(&m_mutex, NULL);
    CPU_ZERO(&m_nodeCpuSet);
    CPU_ZERO(&m_CpuNumaSet);

    m_streams = NULL;
    m_freeStreamList = NULL;
}

ThreadPoolGroup::~ThreadPoolGroup()
{
    delete m_listener;
    m_listener = NULL;
    m_groupCpuArr = NULL;
    m_workers = NULL;
    m_context = NULL;

    m_freeStreamList = NULL;
    m_streams = NULL;
}

void ThreadPoolGroup::Init(bool enableNumaDistribute)
{
    m_context = AllocSetContextCreate(g_instance.instance_context,
        "ThreadPoolGroupContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    AutoContextSwitch acontext(m_context);

    m_listener = New(CurrentMemoryContext) ThreadPoolListener(this);
    m_listener->StartUp();

    if (m_enableBindCpuNuma) {
        for (int i = 0; i < m_groupCpuNum; i++) {
            CPU_SET(m_groupCpuArr[i], &m_CpuNumaSet);
        }
    }

    InitWorkerSentry();

    InitStreamSentry();

    /* Prepare the CPU_SET including all of available cpus in this node */
    m_enableNumaDistribute = enableNumaDistribute;
    for (int i = 0; i < m_groupCpuNum; ++i) {
        CPU_SET(m_groupCpuArr[i], &m_nodeCpuSet);
    }
}

void ThreadPoolGroup::InitWorkerSentry()
{
    /* Prepare slots in case we need to enlarge this thread group. */
    m_workers = (ThreadWorkerSentry*)palloc0_noexcept(sizeof(ThreadWorkerSentry) * m_maxWorkerNum);
    if (m_workers == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }

    /* Init lock for each slot. */
    for (int i = 0; i < m_maxWorkerNum; i++) {
        pthread_mutex_init(&m_workers[i].mutex, NULL);
        pthread_cond_init(&m_workers[i].cond, NULL);
    }

    /* Start up workers. */
    for (int i = 0; i < m_expectWorkerNum; i++) {
        AddWorker(i);
    }
}

void ThreadPoolGroup::AddWorker(int i)
{
    m_workers[i].worker = New(m_context) ThreadPoolWorker(i, this, &m_workers[i].mutex, &m_workers[i].cond);

    int ret = m_workers[i].worker->StartUp();
    if (ret == STATUS_OK) {
        m_workers[i].stat.slotStatus = THREAD_SLOT_INUSE;
        m_workers[i].stat.spawntick++;
        m_workers[i].stat.lastSpawnTime = GetCurrentTimestamp();
        pg_atomic_fetch_add_u32((volatile uint32*)&m_workerNum, 1);
        if (m_groupCpuArr) {
            if (m_enableNumaDistribute) {
                AttachThreadToNodeLevel(m_workers[i].worker->GetThreadId());
            } else if (m_enableBindCpuNuma) {
                AttachThreadToCpuNuma(m_workers[i].worker->GetThreadId());
            } else {
                AttachThreadToCPU(m_workers[i].worker->GetThreadId(), m_groupCpuArr[i % m_groupCpuNum]);
            }
        }
    } else {
        delete m_workers[i].worker;
        m_workers[i].worker = NULL;
        ereport(LOG, (errmsg("Faid to start up thread pool worker: %m")));
    }
}

void ThreadPoolGroup::ReleaseWorkerSlot(int i)
{
    Assert(m_workers[i].worker != NULL);

    pthread_mutex_lock(&m_mutex);
    pg_atomic_fetch_sub_u32((volatile uint32*)&m_workerNum, 1);
    Assert(m_workerNum >= 0);
    m_workers[i].stat.slotStatus = THREAD_SLOT_UNUSE;
    pthread_mutex_unlock(&m_mutex);
}

void ThreadPoolGroup::WaitReady()
{
    while (true) {
        if (m_listenerNum == 1) {
            break;
        }
        pg_usleep(500);
    }
}

float4 ThreadPoolGroup::GetSessionPerThread()
{
    return (float4)m_sessionCount / (float4)m_workerNum;
}

void ThreadPoolGroup::GetThreadPoolGroupStat(ThreadPoolStat* stat)
{
    stat->groupId = m_groupId;
    stat->numaId = m_numaId;
    stat->bindCpuNum = m_groupCpuNum;
    stat->listenerNum = m_listenerNum;

    int rc = sprintf_s(stat->workerInfo, STATUS_INFO_SIZE,
            "default: %d new: %d expect: %d actual: %d idle: %d pending: %d",
            m_defaultWorkerNum, m_expectWorkerNum - m_defaultWorkerNum, m_expectWorkerNum,
            m_workerNum, m_idleWorkerNum, m_pendingWorkerNum);
    securec_check_ss(rc, "", "");

    int runSessionNum = m_workerNum - m_idleWorkerNum;
    int idleSessionNum = m_sessionCount - m_waitServeSessionCount - runSessionNum;
    idleSessionNum = (idleSessionNum < 0) ? 0 : idleSessionNum;
    rc = sprintf_s(stat->sessionInfo, STATUS_INFO_SIZE,
            "total: %d waiting: %d running:%d idle: %d",
            m_sessionCount, m_waitServeSessionCount,
            runSessionNum, idleSessionNum);
    securec_check_ss(rc, "", "");

    if (IS_PGXC_DATANODE) {
        rc = sprintf_s(stat->streamInfo, STATUS_INFO_SIZE,
            "total: %d running: %d idle: %d", m_streamNum, m_streamNum - m_idleStreamNum, m_idleStreamNum);
        securec_check_ss(rc, "", "");
    } else {
        stat->streamInfo[0] = '\0';
    }
}

void ThreadPoolGroup::AddWorkerIfNecessary()
{
    AutoMutexLock alock(&m_mutex);
    alock.lock();
    m_workerNum = (m_workerNum >= 0) ? m_workerNum : 0;

    if (m_workerNum < m_expectWorkerNum) {
        for (int i = 0; i < m_expectWorkerNum; i++) {
            if (m_workers[i].stat.slotStatus == THREAD_SLOT_UNUSE) {
                if (m_workers[i].worker != NULL) {
                    pfree_ext(m_workers[i].worker);
                }
                AddWorker(i);
            }
        }
    }
    alock.unLock();
}

bool ThreadPoolGroup::EnlargeWorkers(int enlargeNum)
{
    AutoMutexLock alock(&m_mutex);
    alock.lock();

    if (m_expectWorkerNum == m_maxWorkerNum) {
        alock.unLock();
        return false;
    }

    int num = m_expectWorkerNum;
    m_expectWorkerNum += enlargeNum;
    m_expectWorkerNum = Min(m_expectWorkerNum, m_maxWorkerNum);
    elog(LOG, "[SCHEDULER] Group %d enlarge worker. Old worker num %d, new worker num %d",
                m_groupId, num, m_expectWorkerNum);
    int diff = m_expectWorkerNum - num;

    /* Turn pending workers into running workers if we have. */
    if (m_pendingWorkerNum != 0) {
        ThreadPoolWorker* worker = NULL;
        int wakeUpNum = Min(diff, m_pendingWorkerNum);
        m_pendingWorkerNum -= wakeUpNum;
        for (int i = num; i < num + wakeUpNum; i++) {
            if (m_workers[i].stat.slotStatus == THREAD_SLOT_INUSE) {
                worker = m_workers[i].worker;
                if (worker->GetthreadStatus() == THREAD_PENDING) {
                    worker->WakeUpToUpdate(THREAD_RUN);
                    elog(LOG, "[SCHEDULER] Group %d enlarge: wakeup pending worker %lu",
                               m_groupId, m_workers[i].worker->GetThreadId());
                }
            }
        }
    }
    alock.unLock();

    /* Start up worker if pending worker is not enough. */
    SendPostmasterSignal(PMSIGNAL_START_THREADPOOL_WORKER);
    return true;
}

void ThreadPoolGroup::ReduceWorkers(int reduceNum)
{
    AutoMutexLock alock(&m_mutex);
    alock.lock();

    int num = m_expectWorkerNum;
    m_expectWorkerNum -= reduceNum;
    m_expectWorkerNum = Max(m_expectWorkerNum, m_defaultWorkerNum);
    if (num - m_expectWorkerNum == 0) {
        alock.unLock();
        return;
    }

    m_pendingWorkerNum += (num - m_expectWorkerNum);
    elog(LOG, "[SCHEDULER] Group %d reduce worker. Old worker num %d, new worker num %d",
              m_groupId, num, m_expectWorkerNum);
    /* only wake up free thread to pending, if we meet working thread, just skip it. */
    for (int i = m_expectWorkerNum; i < num; i++) {
        if (m_workers[i].stat.slotStatus == THREAD_SLOT_INUSE) {
            Assert(m_workers[i].worker != NULL);
            if (m_workers[i].worker->WakeUpToPendingIfFree()) {
                elog(LOG, "[SCHEDULER] Group %d reduce: pending worker %lu",
                          m_groupId, m_workers[i].worker->GetThreadId());
            }
        }
    }

    elog(LOG, "[SCHEDULER] Group %d reduce worker end. Old worker num %d, new worker num %d",
              m_groupId, num, m_expectWorkerNum);
    alock.unLock();
}
void ThreadPoolGroup::ShutDownPendingWorkers()
{
    if (m_pendingWorkerNum == 0) {
        return;
    }

    elog(LOG, "[SCHEDULER] Group %d shut down pending workers start. pending worker num %d, current worker num %d",
              m_groupId, m_pendingWorkerNum, m_expectWorkerNum);

    AutoMutexLock alock(&m_mutex);
    ThreadPoolWorker* worker = NULL;
    alock.lock();
    for (int i = m_expectWorkerNum; i < m_expectWorkerNum + m_pendingWorkerNum; i++) {
        if (m_workers[i].stat.slotStatus == THREAD_SLOT_INUSE) {
            worker = m_workers[i].worker;
            if (worker->GetthreadStatus() == THREAD_PENDING) {
                worker->WakeUpToUpdate(THREAD_EXIT);
            }
        }
    }
    m_pendingWorkerNum = 0;
    elog(LOG, "[SCHEDULER] Group %d shut down pending workers end. pending worker num %d, current worker num %d",
              m_groupId, m_pendingWorkerNum, m_expectWorkerNum);
    alock.unLock();
}

void ThreadPoolGroup::ShutDownThreads()
{
    AutoMutexLock alock(&m_mutex);
    alock.lock();
    for (int i = 0; i < m_maxWorkerNum; i++) {
        if (m_workers[i].stat.slotStatus != THREAD_SLOT_UNUSE) {
            m_workers[i].worker->WakeUpToUpdate(THREAD_EXIT);
        }
    }
    m_pendingWorkerNum = 0;

    for (int i = 0; i < m_maxStreamNum; i++) {
        if (m_streams[i].stat.slotStatus != THREAD_SLOT_UNUSE) {
            m_streams[i].stream->WakeUpToUpdate(THREAD_EXIT);
        }
    }
    alock.unLock();
}

bool ThreadPoolGroup::IsGroupHang()
{
    if (pg_atomic_exchange_u32((volatile uint32*)&m_processTaskCount, 0) != 0 ||
        m_idleWorkerNum != 0)
        return false;

    bool ishang = m_listener->GetSessIshang(&m_current_time, &m_sessionId);
    return ishang;
}

void ThreadPoolGroup::SetGroupHanged(bool isHang)
{
    pg_atomic_exchange_u32((volatile uint32*)&m_hasHanged, (uint32)isHang);
}

bool ThreadPoolGroup::IsGroupHanged()
{
    pg_memory_barrier();
    return m_hasHanged != 0;
}

void ThreadPoolGroup::AttachThreadToCPU(ThreadId thread, int cpu)
{
    cpu_set_t cpuset;
    int ret = 0;

    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    ret = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
    if (ret != 0) {
        ereport(WARNING, (errmsg("Fail to attach thread %lu to CPU %d", thread, cpu)));
    }
}

void ThreadPoolGroup::AttachThreadToNodeLevel(ThreadId thread) const
{
    int ret = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &m_nodeCpuSet);
    if (ret != 0)
        ereport(WARNING, (errmsg("Fail to attach thread %lu to numa node %d", thread, m_numaId)));
}

void ThreadPoolGroup::AttachThreadToCpuNuma(ThreadId thread)
{
    int ret = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &m_CpuNumaSet);
    if (ret != 0) {
        ereport(WARNING, (errmsg("Fail to attach thread %lu to CPU NUMA", thread)));
    }
}

void ThreadPoolGroup::InitStreamSentry()
{
    m_streams = (ThreadStreamSentry*)palloc0_noexcept(sizeof(ThreadStreamSentry) * m_maxStreamNum);
    if (m_streams == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }

    for (int i = 0; i < m_maxStreamNum; i++) {
        pthread_mutex_init(&m_streams[i].mutex, NULL);
        pthread_cond_init(&m_streams[i].cond, NULL);
    }

    m_freeStreamList = New(CurrentMemoryContext)DllistWithLock();    
}

ThreadId ThreadPoolGroup::GetStreamFromPool(StreamProducer* producer)
{
    ThreadId tid = InvalidTid;
    ThreadPoolStream* stream = NULL;
    AutoMutexLock alock(&m_mutex);
    alock.lock();

    if (m_freeStreamList->IsEmpty()) {
        if (m_streamNum == m_maxStreamNum) {
            alock.unLock();
            ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("Exceed stream thread pool limitation %d in group %d", m_maxStreamNum, m_groupId)));
        }

        producer->setChildSlot(AssignPostmasterChildSlot());
        if (producer->getChildSlot() == -1) {
            return InvalidTid;
        }
        tid = AddStream(producer);
    } else {
        Dlelem* elem = m_freeStreamList->RemoveHead();
        pg_atomic_fetch_sub_u32((volatile uint32*)&m_idleStreamNum, 1);
        stream = (ThreadPoolStream*)DLE_VAL(elem);
        tid = stream->GetThreadId();
        stream->WakeUpToWork(producer);
    }
    return tid;
}

ThreadId ThreadPoolGroup::AddStream(StreamProducer* producer)
{
    ThreadId tid = InvalidTid;
    ThreadStreamSentry* streamSentry = NULL;
    int idx = 0;
    for (idx = 0; idx < m_maxStreamNum; idx++) {
        if (m_streams[idx].stat.slotStatus == THREAD_SLOT_UNUSE) {
            streamSentry = &m_streams[idx];
            if (streamSentry->stream != NULL) {
                pfree_ext(streamSentry->stream);
            }
            break;
        }
    }
    if (streamSentry == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                        errmsg("Fail to find a free slot for stream")));
    }

    ThreadPoolStream *stream = New(m_context) ThreadPoolStream();
    tid = stream->StartUp(idx, producer, this, &streamSentry->mutex, &streamSentry->cond);

    if (tid != 0) {
        streamSentry->stat.slotStatus = THREAD_SLOT_INUSE;
        streamSentry->stat.spawntick++;
        streamSentry->stat.lastSpawnTime = GetCurrentTimestamp();
        streamSentry->stream = stream;
        tid = stream->GetThreadId();
        if (m_groupCpuArr) {
            AttachThreadToNodeLevel(tid);
        }
        m_streamNum++;
    } else {
        delete stream;
        tid = 0;
        ereport(LOG, (errmsg("Faid to start up thread pool stream: %m")));
    }

    return tid;
}

void ThreadPoolGroup::ReturnStreamToPool(Dlelem* elem)
{
    m_freeStreamList->AddTail(elem);
    pg_atomic_fetch_add_u32((volatile uint32*)&m_idleStreamNum, 1);
}

void ThreadPoolGroup::RemoveStreamFromPool(Dlelem* elem, int idx)
{
    m_freeStreamList->Remove(elem);
    pg_atomic_fetch_sub_u32((volatile uint32*)&m_idleStreamNum, 1);
    pthread_mutex_lock(&m_mutex);
    m_streams[idx].stat.slotStatus = THREAD_SLOT_UNUSE;
    m_streamNum--;
    pthread_mutex_unlock(&m_mutex);
}

void ThreadPoolGroup::ReduceStreams()
{
    pthread_mutex_lock(&m_mutex);
    int max_reduce_num = m_streamNum - m_defaultWorkerNum;
    if (max_reduce_num > 0) {
        elog(LOG, "Reduce %d stream thread", max_reduce_num);
        for (int i = 0; i < max_reduce_num; i++) {
            Dlelem* elem = m_freeStreamList->RemoveHead();
            if (elem == NULL) {
                break;
            }
            ThreadPoolStream* stream = (ThreadPoolStream*)DLE_VAL(elem);
            stream->WakeUpToUpdate(THREAD_EXIT);
        }
    }
    pthread_mutex_unlock(&m_mutex);
}
