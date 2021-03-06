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
 * threadpool_controler.h
 *     Controler for thread pool. Class ThreadPoolControler is defined to
 *     initilize thread pool's worker and listener threads, and dispatch
 *     new session from postmaster thread to suitable thread group.
 * 
 * IDENTIFICATION
 *        src/include/threadpool/threadpool_controler.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef THREAD_POOL_CONTROLER_H
#define THREAD_POOL_CONTROLER_H

#include "utils/memutils.h"
#include "threadpool_group.h"
#include "knl/knl_variable.h"

class ThreadPoolSessControl;

enum CPUBindType { NO_CPU_BIND, ALL_CPU_BIND, NODE_BIND, CPU_BIND };

typedef struct CPUInfo {
    int totalCpuNum;
    int activeCpuNum;
    int totalNumaNum;
    int activeNumaNum;
    int* cpuArrSize;
    int** cpuArr;

    CPUBindType bindType;
    bool* isBindCpuArr;
    bool* isBindNumaArr;
    bool* isMcsCpuArr;
} CPUInfo;

typedef struct ThreadPoolAttr {
    int threadNum;
    int groupNum;
    char* bindCpu;
} ThreadPoolAttr;

class ThreadPoolControler : public BaseObject {
public:
    ThreadPoolSessControl* m_sessCtrl;

    ThreadPoolControler();
    ~ThreadPoolControler();

    void Init(bool enableNumaDistribute);
    void ShutDownThreads(bool forceWait = false);
    int DispatchSession(Port* port);
    void AddWorkerIfNecessary();
    void SetThreadPoolInfo();
    int GetThreadNum();
    ThreadPoolStat* GetThreadPoolStat(uint32* num);
    bool StayInAttachMode();
    void CloseAllSessions();
    bool CheckNumaDistribute(int numaNodeNum) const;
    CPUBindType GetCpuBindType() const;
    void ShutDownListeners(bool forceWait);
    void ShutDownScheduler(bool forceWait);
    inline ThreadPoolSessControl* GetSessionCtrl()
    {
        return m_sessCtrl;
    }
    inline ThreadPoolScheduler* GetScheduler()
    {
        return m_scheduler;
    }
    inline int GetGroupNum()
    {
        return m_groupNum;
    }

    inline MemoryContext GetMemCxt()
    {
        return m_threadPoolContext;
    }
	
	void BindThreadToAllAvailCpu(ThreadId thread) const;

private:
    ThreadPoolGroup* FindThreadGroupWithLeastSession();
    void ParseAttr();
    void ParseBindCpu();
    int ParseRangeStr(char* attr, bool* arr, int totalNum, char* bindtype);
    void GetMcsCpuInfo();
    void GetSysCpuInfo();
    void InitCpuInfo();
    void GetCpuAndNumaNum();
    bool IsActiveCpu(int cpuid, int numaid);
    void SetGroupAndThreadNum();
    void ConstrainThreadNum();
    void GetInstanceBind();
    bool CheckCpuBind() const;

private:
    MemoryContext m_threadPoolContext;
    ThreadPoolGroup** m_groups;
    ThreadPoolScheduler* m_scheduler;
    CPUInfo m_cpuInfo;
    ThreadPoolAttr m_attr;
    cpu_set_t m_cpuset;
    int m_groupNum;
    int m_threadNum;
    int m_maxPoolSize;
};

#endif /* THREAD_POOL_CONTROLER_H */
