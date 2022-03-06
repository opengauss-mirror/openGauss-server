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
 *---------------------------------------------------------------------------------------
 *
 * threadpool_scheduler.h
 *
 *
 * IDENTIFICATION
 *        /src/include/threadpool/threadpool_scheduler.h
 *
 *---------------------------------------------------------------------------------------
 */

#ifndef THREAD_POOL_SCHEDULER_CONTROL_H
#define THREAD_POOL_SCHEDULER_CONTROL_H

class ThreadPoolScheduler : public BaseObject{
public:
    ThreadPoolScheduler(int groupNum, ThreadPoolGroup** groups);
    ~ThreadPoolScheduler();
    int StartUp();
    void DynamicAdjustThreadPool();
    void GPCScheduleCleaner(int* gpc_count);
    void ShutDown() const;
    void SigHupHandler();
    inline ThreadId GetThreadId()
    {
        return m_tid;
    }
    inline void SetShutDown(bool has_shutdown)
    {
        m_has_shutdown = has_shutdown;
    }
    inline bool HasShutDown()
    {
        return m_has_shutdown;
    }
    MemoryContext m_gpcContext;
    bool m_getSIGHUP;
    volatile bool m_canAdjustPool;
    bool m_getKilled;
private:
    void AdjustWorkerPool(int idx);
    void AdjustStreamPool(int idx);
    void ReduceWorkerIfNecessary(int groupIdx);
    void EnlargeWorkerIfNecessage(int groupIdx);
private:
    ThreadId m_tid;
    int m_groupNum;
    ThreadPoolGroup** m_groups;
    uint* m_hangTestCount;
    uint* m_freeTestCount;
    uint* m_freeStreamCount;
    volatile bool  m_has_shutdown;
};

#define THREAD_SCHEDULER_STEP 8

extern void TpoolSchedulerMain(ThreadPoolScheduler* scheduler);

#endif
