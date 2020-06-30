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

ThreadPoolGroup::ThreadPoolGroup(int maxWorkerNum, int expectWorkerNum,
                                 int groupId, int numaId, int cpuNum, int* cpuArr)
    : m_listener(NULL),
      m_maxWorkerNum(maxWorkerNum),
      m_defaultWorkerNum(expectWorkerNum),
      m_workerNum(0),
      m_listenerNum(0),
      m_expectWorkerNum(expectWorkerNum),
      m_idleWorkerNum(0),
      m_pendingWorkerNum(0),
      m_sessionCount(0),
      m_waitServeSessionCount(0),
      m_processTaskCount(0),
      m_groupId(groupId),
      m_numaId(numaId),
      m_groupCpuNum(cpuNum),
      m_groupCpuArr(cpuArr),
      m_workers(NULL),
      m_enableNumaDistribute(false)
{
    m_context = AllocSetContextCreate(g_instance.instance_context,
        "ThreadPoolGroupContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);
    pthread_mutex_init(&m_mutex, NULL);
    CPU_ZERO(&m_nodeCpuSet);
}

ThreadPoolGroup::~ThreadPoolGroup()
{
    delete m_listener;
    m_listener = NULL;
    m_groupCpuArr = NULL;
    m_workers = NULL;
}

void ThreadPoolGroup::init(bool enableNumaDistribute)
{
    AutoContextSwitch acontext(m_context);

    m_listener = New(CurrentMemoryContext) ThreadPoolListener(this);
    m_listener->StartUp();

    /* Prepare slots in case we need to enlarge this thread group. */
    m_workers = (ThreadWorkerSentry*)palloc0_noexcept(sizeof(ThreadWorkerSentry) * m_maxWorkerNum);
    if (m_workers == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }

    /* Prepare the CPU_SET including all of available cpus in this node */
    if (enableNumaDistribute) {
        Assert(m_groupCpuArr);
        m_enableNumaDistribute = enableNumaDistribute;
        for (int i = 0; i < m_groupCpuNum; ++i) {
            CPU_SET(m_groupCpuArr[i], &m_nodeCpuSet);
        }
    }

    for (int i = 0; i < m_expectWorkerNum; i++) {
        pthread_mutex_init(&m_workers[i].m_mutex, NULL);
        pthread_cond_init(&m_workers[i].m_cond, NULL);
        AddWorker(i);
    }
}

void ThreadPoolGroup::AddWorker(int i)
{
    m_workers[i].worker = New(m_context) ThreadPoolWorker(i, this, &m_workers[i].m_mutex, &m_workers[i].m_cond);

    int ret = m_workers[i].worker->StartUp();
    if (ret == STATUS_OK) {
        m_workers[i].stat.slotStatus = WORKER_SLOT_INUSE;
        m_workers[i].stat.spawntick++;
        m_workers[i].stat.lastSpawnTime = GetCurrentTimestamp();
        m_workerNum++;
        if (m_groupCpuArr) {
            if (m_enableNumaDistribute) {
                AttachThreadToNodeLevel(m_workers[i].worker->GetThreadId());
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
    m_workerNum--;
    Assert(m_workerNum >= 0);
    m_workers[i].stat.slotStatus = WORKER_SLOT_UNUSE;
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
    securec_check_ss(rc, "\0", "\0");

    int run_session_num = m_workerNum - m_idleWorkerNum;
    int idle_session_num = m_sessionCount - m_waitServeSessionCount - run_session_num;
    idle_session_num = (idle_session_num < 0) ? 0 : idle_session_num;
    rc = sprintf_s(stat->sessionInfo, STATUS_INFO_SIZE,
        "total: %d waiting: %d running:%d idle: %d",
        m_sessionCount, m_waitServeSessionCount,
        run_session_num, idle_session_num);
    securec_check_ss(rc, "\0", "\0");
}

void ThreadPoolGroup::AddWorkerIfNecessary()
{
    AutoMutexLock alock(&m_mutex);
    alock.lock();
    m_workerNum = m_workerNum >= 0 ? m_workerNum : 0;

    if (m_workerNum < m_expectWorkerNum) {
        for (int i = 0; i < m_expectWorkerNum; i++) {
            if (m_workers[i].stat.slotStatus == WORKER_SLOT_UNUSE) {
                if (m_workers[i].worker != NULL) {
                    pfree(m_workers[i].worker);
                }
                AddWorker(i);
            }
        }
    }
    alock.unLock();
}

bool ThreadPoolGroup::EnlargeWorkers(int enlargeNum)
{
    if (m_expectWorkerNum == m_maxWorkerNum) {
        return false;
    }

    int num = m_expectWorkerNum;
    m_expectWorkerNum += enlargeNum;
    m_expectWorkerNum = Min(m_expectWorkerNum, m_maxWorkerNum);
    elog(LOG, "[SCHEDULER] Group %d enlarge worker. Old worker num %d, new worker num %d",
	    m_groupId, num, m_expectWorkerNum);
    int diff = m_expectWorkerNum - num;

    AutoMutexLock alock(&m_mutex);
    alock.lock();
    /* Turn pending workers into running workers if we have. */
    if (m_pendingWorkerNum != 0) {
        elog(LOG, "[SCHEDULER] Group %d now have %d pending workers.", m_groupId, m_pendingWorkerNum);

        ThreadPoolWorker* worker = NULL;
        int wake_up_num = Min(diff, m_pendingWorkerNum);
        m_pendingWorkerNum -= wake_up_num;
        for (int i = num; i < num + wake_up_num; i++) {
            if (m_workers[i].stat.slotStatus == WORKER_SLOT_INUSE) {
                worker = m_workers[i].worker;
                worker->WakeUpToUpdate(THREAD_RUN);
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
    int num = m_expectWorkerNum;
    m_expectWorkerNum -= reduceNum;
    m_expectWorkerNum = Max(m_expectWorkerNum, m_defaultWorkerNum);
    m_pendingWorkerNum += (num - m_expectWorkerNum);

    if (m_expectWorkerNum - num == 0) {
        return;
    }
    
    elog(LOG, "[SCHEDULER] Group %d reduce worker. Old worker num %d, new worker num %d",
        m_groupId, num, m_expectWorkerNum);

    AutoMutexLock alock(&m_mutex);
    alock.lock();
    for (int i = m_expectWorkerNum; i < num; i++) {
        if (m_workers[i].stat.slotStatus == WORKER_SLOT_INUSE) {
            Assert(m_workers[i].worker != NULL);
            m_workers[i].worker->WakeUpToUpdate(THREAD_PENDING);
        }
    }
    alock.unLock();
}

void ThreadPoolGroup::ShutDownPendingWorkers()
{
    if (m_pendingWorkerNum == 0) {
        return;
    }

    elog(LOG, "[SCHEDULER] Group %d shut down pending workers.", m_groupId);

    AutoMutexLock alock(&m_mutex);
    ThreadPoolWorker* worker = NULL;
    alock.lock();
    for (int i = m_expectWorkerNum; i < m_expectWorkerNum + m_pendingWorkerNum; i++) {
        if (m_workers[i].stat.slotStatus == WORKER_SLOT_INUSE) {
            worker = m_workers[i].worker;
            worker->WakeUpToUpdate(THREAD_EXIT);
        }
    }
    m_pendingWorkerNum = 0;
    alock.unLock();
}

void ThreadPoolGroup::ShutdownWorker()
{
    AutoMutexLock alock(&m_mutex);
    alock.lock();
    for (int i = 0; i < m_expectWorkerNum + m_pendingWorkerNum; i++) {
        if (m_workers[i].stat.slotStatus != WORKER_SLOT_UNUSE) {
            m_workers[i].worker->WakeUpToUpdate(THREAD_EXIT);
        }
    }
    m_pendingWorkerNum = 0;
    alock.unLock();
}

bool ThreadPoolGroup::IsGroupHang()
{
    if (pg_atomic_exchange_u32((volatile uint32*)&m_processTaskCount, 0) != 0 ||
        m_idleWorkerNum != 0)
        return false;

    bool is_hang = true;
    AutoMutexLock alock(&m_mutex);
    alock.lock();
    for (int i = 0; i < m_expectWorkerNum + m_pendingWorkerNum; i++) {
        if (m_workers[i].stat.slotStatus != WORKER_SLOT_UNUSE) {
            is_hang = is_hang && WORKER_MAY_HANG(m_workers[i].worker->m_waitState);
        }
    }
    m_pendingWorkerNum = 0;
    alock.unLock();
    return is_hang;
}

void ThreadPoolGroup::AttachThreadToCPU(ThreadId thread, int cpu)
{
    cpu_set_t cpu_set;
    int ret = 0;

    CPU_ZERO(&cpu_set);
    CPU_SET(cpu, &cpu_set);
    ret = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpu_set);
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
