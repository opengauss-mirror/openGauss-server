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
 * ---------------------------------------------------------------------------------------
 * 
 * threadpool_group.h
 *     Thread pool group controls listener and worker threads.
 * 
 * IDENTIFICATION
 *        src/include/threadpool/threadpool_group.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef THREAD_POOL_GROUP_H
#define THREAD_POOL_GROUP_H

#include "c.h"
#include "utils/memutils.h"
#include "knl/knl_variable.h"

#define NUM_THREADPOOL_STATUS_ELEM 8
#define STATUS_INFO_SIZE 256

typedef enum { THREAD_SLOT_UNUSE = 0, THREAD_SLOT_INUSE } ThreadSlotStatus;

struct ThreadSentryStatus {
    int spawntick;
    TimestampTz lastSpawnTime;
    ThreadSlotStatus slotStatus;
};

struct ThreadWorkerSentry {
    ThreadSentryStatus stat;
    ThreadPoolWorker* worker;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
};

struct ThreadStreamSentry {
    ThreadSentryStatus stat;
    ThreadPoolStream* stream;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
};

typedef struct ThreadPoolStat {
    int groupId;
    int numaId;
    int bindCpuNum;
    int listenerNum;
    char workerInfo[STATUS_INFO_SIZE];
    char sessionInfo[STATUS_INFO_SIZE];
    char streamInfo[STATUS_INFO_SIZE];
} ThreadPoolStat;

class ThreadPoolGroup : public BaseObject {
public:
    ThreadPoolListener* m_listener;

    ThreadPoolGroup(int maxWorkerNum, int expectWorkerNum, int maxStreamNum,
                    int groupId, int numaId, int cpuNum, int* cpuArr, bool enableBindCpuNuma);
    ~ThreadPoolGroup();
    void Init(bool enableNumaDistribute);
    void InitWorkerSentry();
    void ReleaseWorkerSlot(int i);
    void AddWorker(int i);
    ThreadId AddStream(StreamProducer* producer);
    void ShutDownThreads();
    void AddWorkerIfNecessary();
    bool EnlargeWorkers(int enlargeNum);
    void ReduceWorkers(int reduceNum);
    void ShutDownPendingWorkers();
    void ReduceStreams();
    void WaitReady();
    float4 GetSessionPerThread();
    void GetThreadPoolGroupStat(ThreadPoolStat* stat);
    /* get ready session list check for hang */
    bool IsGroupHang();
    /* check for hang flag */
    void SetGroupHanged(bool isHang);
    bool IsGroupHanged();

    inline ThreadPoolListener* GetListener()
    {
        return m_listener;
    }

    inline int GetGroupId()
    {
        return m_groupId;
    }

    inline int GetNumaId()
    {
        return m_numaId;
    }

    inline bool AllSessionClosed()
    {
        return (m_sessionCount <= 0);
    }

    inline bool AllThreadShutDown()
    {
        return (m_workerNum <= 0);
    }

    ThreadId GetStreamFromPool(StreamProducer* producer);
    void ReturnStreamToPool(Dlelem* elem);
    void RemoveStreamFromPool(Dlelem* elem, int idx);
    void InitStreamSentry();

    inline bool HasFreeStream()
    {
        return (m_idleStreamNum > 0);
    }

    friend class ThreadPoolWorker;
    friend class ThreadPoolListener;
    friend class ThreadPoolScheduler;

private:
    void AttachThreadToCPU(ThreadId thread, int cpu);
    void AttachThreadToNodeLevel(ThreadId thread) const;
    void AttachThreadToCpuNuma(ThreadId thread);

private:
    int m_maxWorkerNum;
    int m_maxStreamNum;
    int m_defaultWorkerNum;
    volatile int m_workerNum;
    volatile int m_listenerNum;
    volatile int m_expectWorkerNum;
    volatile int m_idleWorkerNum;
    volatile int m_pendingWorkerNum;
    volatile int m_streamNum;
    volatile int m_idleStreamNum;
    volatile int m_sessionCount;           // all session count;
    volatile int m_waitServeSessionCount;  // wait for worker to server
    volatile int m_processTaskCount;
    volatile int m_hasHanged;

    int m_groupId;
    int m_numaId;
    int m_groupCpuNum;
    int* m_groupCpuArr;
    bool m_enableNumaDistribute;
    bool m_enableBindCpuNuma;
    cpu_set_t m_nodeCpuSet; /* for numa node distribution only */
    cpu_set_t m_CpuNumaSet; /* for numa node distribution only */

    ThreadWorkerSentry* m_workers;
    MemoryContext m_context;
    pthread_mutex_t m_mutex;

    ThreadStreamSentry* m_streams;
    DllistWithLock* m_freeStreamList;

    instr_time m_current_time;
    uint64 m_sessionId;
};

#endif /* THREAD_POOL_GROUP_H */
