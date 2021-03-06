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
 * threadpool_controler.cpp
 *        Controler for thread pool. Class ThreadPoolControler is defined to
 *        initilize thread pool's worker and listener threads, and dispatch
 *        new session from postmaster thread to suitable thread group.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/process/threadpool/threadpool_controler.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#ifdef __USE_NUMA
#include <numa.h>
#endif
#include "postgres.h"
#include "knl/knl_variable.h"

#include "threadpool/threadpool.h"

#include "access/xact.h"
#include "catalog/pg_collation.h"
#include "commands/copy.h"
#include "gssignal/gs_signal.h"
#include "lib/dllist.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "pgxc/pgxc.h"
#include "storage/pmsignal.h"
#include "tcop/dest.h"
#include "utils/atomic.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/ps_status.h"
#include "executor/executor.h"

#ifdef HAVE_POLL_H
#include <poll.h>
#endif

ThreadPoolControler* g_threadPoolControler = NULL;

#define BUFSIZE 128

#define IS_NULL_STR(str) ((str) == NULL || (str)[0] == '\0')
#define INVALID_ATTR_ERROR(detail) \
    ereport(FATAL, (errcode(ERRCODE_OPERATE_INVALID_PARAM), errmsg("Invalid attribute for thread pool."), detail))

ThreadPoolControler::ThreadPoolControler()
{
    m_threadPoolContext = NULL;
    m_sessCtrl = NULL;
    m_groups = NULL;
    m_scheduler = NULL;
    m_groupNum = 1;
    m_threadNum = 0;
    m_maxPoolSize = 0;
}

ThreadPoolControler::~ThreadPoolControler()
{
    delete m_scheduler;
    delete m_sessCtrl;
    MemoryContextDelete(m_threadPoolContext);
    m_threadPoolContext = NULL;
    m_groups = NULL;
    m_sessCtrl = NULL;
}

void ThreadPoolControler::Init(bool enableNumaDistribute)
{

    m_threadPoolContext = AllocSetContextCreate(g_instance.instance_context,
        "ThreadPoolContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    AutoContextSwitch memSwitch(m_threadPoolContext);

    m_groups = (ThreadPoolGroup**)palloc(sizeof(ThreadPoolGroup*) * m_groupNum);
    m_sessCtrl = New(CurrentMemoryContext) ThreadPoolSessControl(CurrentMemoryContext);

    bool bindCpu = CheckCpuBind();
    int maxThreadNum = 0;
    int expectThreadNum = 0;
    int maxStreamNum = 0;
    int numaId = 0;
    int cpuNum = 0;
    int *cpuArr = NULL;

    for (int i = 0; i < m_groupNum; i++) {
        if (bindCpu) {
            while (m_cpuInfo.cpuArrSize[numaId] == 0)
                numaId++;

            /*
             * Invoke numa_set_preferred before starting worker thread to make
             * more memory allocation local to worker thread.
             */
#ifdef __USE_NUMA
            if (enableNumaDistribute) {
                numa_set_preferred(numaId);
            }
#endif

            Assert(numaId < m_cpuInfo.totalNumaNum);
            expectThreadNum = (int)round(
                (double)m_threadNum * ((double)m_cpuInfo.cpuArrSize[numaId] / (double)m_cpuInfo.activeCpuNum));
            maxThreadNum = (int)round(
                (double)m_maxPoolSize * ((double)m_cpuInfo.cpuArrSize[numaId] / (double)m_cpuInfo.activeCpuNum));
            maxStreamNum = (int)round(
                (double)m_maxPoolSize * ((double)m_cpuInfo.cpuArrSize[numaId] / (double)m_cpuInfo.activeCpuNum));
            cpuNum = m_cpuInfo.cpuArrSize[numaId];
            cpuArr = m_cpuInfo.cpuArr[numaId];

            numaId++;
        } else {
            expectThreadNum = m_threadNum / m_groupNum;
            maxThreadNum = m_maxPoolSize / m_groupNum;
            maxStreamNum = m_maxPoolSize / m_groupNum;
            numaId = -1;
        }

        m_groups[i] = New(CurrentMemoryContext)ThreadPoolGroup(maxThreadNum, expectThreadNum,
                                                    maxStreamNum, i, numaId, cpuNum, cpuArr);
        m_groups[i]->Init(enableNumaDistribute);
    }

    for (int i = 0; i < m_groupNum; i++) {
        m_groups[i]->WaitReady();
    }

#ifdef __USE_NUMA
    if (enableNumaDistribute) {
        /* Set to interleave mode for other than worker thread */
        numa_set_interleave_mask(numa_all_nodes_ptr);
    }
#endif

    m_scheduler = New(CurrentMemoryContext) ThreadPoolScheduler(m_groupNum, m_groups);
    m_scheduler->StartUp();
}

void ThreadPoolControler::SetThreadPoolInfo()
{
    InitCpuInfo();

    GetInstanceBind();

    GetCpuAndNumaNum();

    ParseAttr();

    GetSysCpuInfo();

    SetGroupAndThreadNum();
}

void ThreadPoolControler::GetInstanceBind()
{
    /* Check if the instance has been attch to some specific CPUs. */
    int ret = pthread_getaffinity_np(PostmasterPid, sizeof(cpu_set_t), &m_cpuset);
    if (ret == 0) {
        return;
    } else {
        errno_t rc = memset_s(&m_cpuset, sizeof(m_cpuset), 0, sizeof(m_cpuset));
        securec_check(rc, "\0", "\0");
    }
}

void ThreadPoolControler::ParseAttr()
{
    m_attr.threadNum = DEFAULT_THREAD_POOL_SIZE;
    m_attr.groupNum = DEFAULT_THREAD_POOL_GROUPS;
    m_attr.bindCpu = NULL;

    /* Do str copy and remove space. */
    char* attr = TrimStr(g_instance.attr.attr_common.thread_pool_attr);
    if (IS_NULL_STR(attr))
        return;

    char* ptoken = NULL;
    char* psave = NULL;
    const char* pdelimiter = ",";

    /* Get thread num */
    ptoken = TrimStr(strtok_r(attr, pdelimiter, &psave));
    if (!IS_NULL_STR(ptoken))
        m_attr.threadNum = pg_strtoint32(ptoken);

    /* Ger group num */
    ptoken = TrimStr(strtok_r(NULL, pdelimiter, &psave));
    if (!IS_NULL_STR(ptoken))
        m_attr.groupNum = pg_strtoint32(ptoken);

    if (m_attr.threadNum < 0 || m_attr.threadNum > MAX_THREAD_POOL_SIZE)
        INVALID_ATTR_ERROR(
            errdetail("Current thread num %d is out of range [%d, %d].", m_attr.threadNum, 0, MAX_THREAD_POOL_SIZE));
    if (m_attr.groupNum < 0 || m_attr.groupNum > MAX_THREAD_POOL_GROUPS)
        INVALID_ATTR_ERROR(
            errdetail("Current group num %d is out of range [%d, %d].", m_attr.groupNum, 0, MAX_THREAD_POOL_GROUPS));

    /* Get attach cpu */
    m_attr.bindCpu = TrimStr(psave);
    ParseBindCpu();
}

void ThreadPoolControler::ParseBindCpu()
{
    if (IS_NULL_STR(m_attr.bindCpu)) {
        m_cpuInfo.bindType = NO_CPU_BIND;
        return;
    }

    char* scpu = pstrdup(m_attr.bindCpu);
    char* ptoken = NULL;
    char* psave = NULL;
    const char* pdelimiter = ":";
    int bindNum = 0;

    if (scpu[0] != '(' || scpu[strlen(scpu) - 1] != ')')
        INVALID_ATTR_ERROR("Use '(' ')' to indicate cpu bind info.");

    scpu++;
    scpu[strlen(scpu) - 1] = '\0';

    ptoken = TrimStr(strtok_r(scpu, pdelimiter, &psave));
    ptoken = pg_strtolower(ptoken);

    if (strncmp("nobind", ptoken, strlen("nobind")) == 0) {
        m_cpuInfo.bindType = NO_CPU_BIND;
        return;
    } else if (strncmp("allbind", ptoken, strlen("allbind")) == 0) {
        m_cpuInfo.bindType = ALL_CPU_BIND;
        return;
    } else if (strncmp("cpubind", ptoken, strlen("cpubind")) == 0) {
        m_cpuInfo.bindType = CPU_BIND;
        m_cpuInfo.isBindCpuArr = (bool*)palloc0(sizeof(bool) * m_cpuInfo.totalCpuNum);
        bindNum = ParseRangeStr(psave, m_cpuInfo.isBindCpuArr, m_cpuInfo.totalCpuNum, "cpubind");
    } else if (strncmp("nodebind", ptoken, strlen("nodebind")) == 0) {
        m_cpuInfo.bindType = NODE_BIND;
        m_cpuInfo.isBindNumaArr = (bool*)palloc0(sizeof(bool) * m_cpuInfo.totalCpuNum);
        bindNum = ParseRangeStr(psave, m_cpuInfo.isBindNumaArr, m_cpuInfo.totalNumaNum, "nodebind");
    } else {
        INVALID_ATTR_ERROR(errdetail("Only 'nobind', 'allbind', 'cpubind', and 'nodebind' are valid attribute."));
    }

    if (bindNum == 0)
        INVALID_ATTR_ERROR(
            errdetail("Can not find valid CPU for thread binding, there are two possible reasons:\n"
                         "1. These CPUs are not active, use lscpu to check On-line CPU(s) list.\n"
                         "2. The process has been bind to other CPUs and there is no intersection,"
                         "use taskset -pc to check process CPU bind info.\n"));
}

int ThreadPoolControler::ParseRangeStr(char* attr, bool* arr, int totalNum, char* bindtype)
{
    char* ptoken = NULL;
    char* psave = NULL;
    const char* pdelimiter = ",";
    int retNum = 0;

    ptoken = TrimStr(strtok_r(attr, pdelimiter, &psave));

    while (!IS_NULL_STR(ptoken)) {
        char* pt = NULL;
        char* ps = NULL;
        const char* pd = "-";
        int startid = -1;
        int endid = -1;

        pt = TrimStr(strtok_r(ptoken, pd, &ps));
        if (!IS_NULL_STR(pt))
            startid = pg_strtoint32(pt);
        if (!IS_NULL_STR(ps))
            endid = pg_strtoint32(ps);

        if (startid < 0 && endid < 0)
            INVALID_ATTR_ERROR(errdetail("Can not parse attribute %s", pt));
        if (startid >= totalNum)
            INVALID_ATTR_ERROR(
                errdetail("The %s attribute %d is out of valid range [%d, %d]", bindtype, startid, 0, totalNum - 1));
        if (endid >= totalNum)
            INVALID_ATTR_ERROR(
                errdetail("The %s attribute %d is out of valid range [%d, %d]", bindtype, endid, 0, totalNum - 1));

        if (endid == -1) {
            retNum += arr[startid] ? 0 : 1;
            arr[startid] = true;
        } else {
            if (startid > endid) {
                int tmpid = startid;
                startid = endid;
                endid = tmpid;
            }

            for (int i = startid; i <= endid; i++) {
                retNum += arr[startid] ? 0 : 1;
                arr[i] = true;
            }
        }

        ptoken = TrimStr(strtok_r(NULL, pdelimiter, &psave));
    }

    return retNum;
}

void ThreadPoolControler::GetMcsCpuInfo()
{
    FILE* fp = NULL;
    char buf[BUFSIZE];

    m_cpuInfo.isMcsCpuArr = (bool*)palloc0(sizeof(bool) * m_cpuInfo.totalCpuNum);

    /*
     * When the database is deplyed on MCS, we need to read cpuset.cpus to find
     * available CPUs in this MCS. If we can read this file, then we think all
     * CPUs are available.
     */
    fp = fopen("/sys/fs/cgroup/cpuset/cpuset.cpus", "r");
    if (fp == NULL) {
        ereport(WARNING, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("Failed to open file /sys/fs/cgroup/cpuset/cpuset.cpus")));
        errno_t rc = memset_s(m_cpuInfo.isMcsCpuArr, m_cpuInfo.totalCpuNum, 1, m_cpuInfo.totalCpuNum);
        securec_check(rc, "", "");
        return;
    }

    if (fgets(buf, BUFSIZE, fp) == NULL) {
        ereport(WARNING, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("Failed to read file /sys/fs/cgroup/cpuset/cpuset.cpus")));
        errno_t rc = memset_s(m_cpuInfo.isMcsCpuArr, m_cpuInfo.totalCpuNum, 1, m_cpuInfo.totalCpuNum);
        securec_check(rc, "", "");
        fclose(fp);
        return;
    }

    int mcsNum = ParseRangeStr(buf, m_cpuInfo.isMcsCpuArr, m_cpuInfo.totalCpuNum, "Mcs Cpu set");
    if (mcsNum == 0) {
        ereport(WARNING, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("No available CPUs in /sys/fs/cgroup/cpuset/cpuset.cpus")));
        errno_t rc = memset_s(m_cpuInfo.isMcsCpuArr, m_cpuInfo.totalCpuNum, 1, m_cpuInfo.totalCpuNum);
        securec_check(rc, "", "");
    }
    fclose(fp);
}

void ThreadPoolControler::GetSysCpuInfo()
{
    FILE* fp = NULL;
    char buf[BUFSIZE];

    if (m_cpuInfo.totalNumaNum == 0 || m_cpuInfo.totalCpuNum == 0) {
        ereport(WARNING, (errmsg("Fail to read cpu num or numa num.")));
        return;
    }

    GetMcsCpuInfo();

    m_cpuInfo.cpuArr = (int**)palloc0(sizeof(int*) * m_cpuInfo.totalNumaNum);
    m_cpuInfo.cpuArrSize = (int*)palloc0(sizeof(int) * m_cpuInfo.totalNumaNum);
    int cpu_per_numa = m_cpuInfo.totalCpuNum / m_cpuInfo.totalNumaNum;
    for (int i = 0; i < m_cpuInfo.totalNumaNum; i++)
        m_cpuInfo.cpuArr[i] = (int*)palloc0(sizeof(int) * cpu_per_numa);

    /* use lscpu to get active cpu info */
    fp = popen("lscpu -b -e=cpu,node", "r");

    if (fp == NULL) {
        ereport(WARNING, (errmsg("Unable to use 'lscpu' to read CPU info.")));
        return;
    }

    char* ptoken = NULL;
    char* psave = NULL;
    const char* pdelimiter = " ";
    int cpuid = 0;
    int numaid = 0;
    /* try to read the header. */
    if (fgets(buf, sizeof(buf), fp) != NULL) {
        m_cpuInfo.activeCpuNum = 0;
        while (fgets(buf, sizeof(buf), fp) != NULL) {
            ptoken = strtok_r(buf, pdelimiter, &psave);
            if (!IS_NULL_STR(ptoken))
                cpuid = pg_strtoint32(ptoken);

            ptoken = strtok_r(NULL, pdelimiter, &psave);
            if (!IS_NULL_STR(ptoken))
                numaid = pg_strtoint32(ptoken);

            if (IsActiveCpu(cpuid, numaid)) {
                m_cpuInfo.cpuArr[numaid][m_cpuInfo.cpuArrSize[numaid]] = cpuid;
                m_cpuInfo.cpuArrSize[numaid]++;
                m_cpuInfo.activeCpuNum++;
            }
        }
    }

    for (int i = 0; i < m_cpuInfo.totalNumaNum; i++) {
        if (m_cpuInfo.cpuArrSize[i] > 0)
            m_cpuInfo.activeNumaNum++;
    }

    pclose(fp);
}

void ThreadPoolControler::InitCpuInfo()
{
    m_cpuInfo.totalCpuNum = 0;
    m_cpuInfo.activeCpuNum = 0;
    m_cpuInfo.totalNumaNum = 0;
    m_cpuInfo.activeNumaNum = 0;
    m_cpuInfo.cpuArrSize = NULL;
    m_cpuInfo.cpuArr = NULL;

    m_cpuInfo.bindType = NO_CPU_BIND;
    m_cpuInfo.isBindCpuArr = NULL;
    m_cpuInfo.isBindNumaArr = NULL;
    m_cpuInfo.isMcsCpuArr = NULL;
}

void ThreadPoolControler::GetCpuAndNumaNum()
{
    char buf[BUFSIZE];

    FILE* fp = NULL;

    if ((fp = popen("lscpu", "r")) != NULL) {
        while (fgets(buf, sizeof(buf), fp) != NULL) {
            if (strncmp("CPU(s)", buf, strlen("CPU(s)")) == 0 &&
                strncmp("On-line CPU(s) list", buf, strlen("On-line CPU(s) list")) != 0 &&
                strncmp("NUMA node", buf, strlen("NUMA node")) != 0) {
                char* loc = strchr(buf, ':');
                m_cpuInfo.totalCpuNum = pg_strtoint32(loc + 1);
            } else if (strncmp("NUMA node(s)", buf, strlen("NUMA node(s)")) == 0) {
                char* loc = strchr(buf, ':');
                m_cpuInfo.totalNumaNum = pg_strtoint32(loc + 1);
            }
        }
        pclose(fp);
    }
}

bool ThreadPoolControler::IsActiveCpu(int cpuid, int numaid)
{
    switch (m_cpuInfo.bindType) {
        case NO_CPU_BIND:
        case ALL_CPU_BIND:
            return (m_cpuInfo.isMcsCpuArr[cpuid] && CPU_ISSET(cpuid, &m_cpuset));
        case NODE_BIND:
            return (m_cpuInfo.isBindNumaArr[numaid] && m_cpuInfo.isMcsCpuArr[cpuid] && CPU_ISSET(cpuid, &m_cpuset));
        case CPU_BIND:
            return (m_cpuInfo.isBindCpuArr[cpuid] && m_cpuInfo.isMcsCpuArr[cpuid] && CPU_ISSET(cpuid, &m_cpuset));
    }
    return false;
}

bool ThreadPoolControler::CheckCpuBind() const
{
    if (m_cpuInfo.bindType == NO_CPU_BIND)
        return false;
    if (m_groupNum != m_cpuInfo.activeNumaNum) {
        ereport(WARNING,
            (errmsg("Can not bind worker thread to CPU because the "
                    "thread group num must equal to active NUMA num.")));
        return false;
    }
    if (m_cpuInfo.activeCpuNum == 0 || m_cpuInfo.cpuArr == NULL) {
        ereport(WARNING, (errmsg("Can not bind worker thread to CPU because no valid CPUs.")));
        return false;
    }

    return true;
}

bool ThreadPoolControler::CheckNumaDistribute(int numaNodeNum) const
{
    if (m_cpuInfo.bindType == NO_CPU_BIND) {
        ereport(WARNING,
            (errmsg("allbind should be used to replace nobind in thread_pool_attr when NUMA is activated.")));
        return false;
    }
    
    if (!CheckCpuBind()) {
        return false;
    }

    if (m_cpuInfo.totalNumaNum != numaNodeNum  || !m_cpuInfo.cpuArrSize) {
        ereport(WARNING,
            (errmsg("Can not activate NUMA distribute because no multiple NUMA nodes or CPUs are available.")));
        return false;
    }

    for (int i = 0; i < m_cpuInfo.totalNumaNum; ++i) {
        if (m_cpuInfo.cpuArrSize[i] <= 0) {
            ereport(WARNING,
                (errmsg("Can not activate NUMA distribute because no available cpu in node %d.", i)));
            return false;
        }
    }

    return true;
}

CPUBindType ThreadPoolControler::GetCpuBindType() const
{
    return m_cpuInfo.bindType;
}

void ThreadPoolControler::SetGroupAndThreadNum()
{
    if (m_attr.groupNum == 0) {
        if (m_cpuInfo.totalNumaNum > 0)
            m_groupNum = m_cpuInfo.activeNumaNum;
        else
            m_groupNum = DEFAULT_THREAD_POOL_GROUPS;
    } else {
        m_groupNum = m_attr.groupNum;
    }

    if (m_attr.threadNum == 0) {
        if (m_cpuInfo.activeCpuNum > 0)
            m_threadNum = m_cpuInfo.activeCpuNum * THREAD_CORE_RATIO;
        else
            m_threadNum = DEFAULT_THREAD_POOL_SIZE;
    } else {
        m_threadNum = m_attr.threadNum;
    }

    ConstrainThreadNum();
}

void ThreadPoolControler::ConstrainThreadNum()
{
    /* Thread pool size should not be larger than max_connections. */
    if (MAX_THREAD_POOL_SIZE > g_instance.attr.attr_network.MaxConnections) {
        m_maxPoolSize = g_instance.attr.attr_network.MaxConnections;
        ereport(LOG, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                      errmsg("Thread pool size %d should not be larger than max_connections %d, "
                             "so reduce thread pool size to max_connections",
                      m_threadNum, g_instance.attr.attr_network.MaxConnections)));
    }
    
    m_maxPoolSize = Min(MAX_THREAD_POOL_SIZE, g_instance.attr.attr_network.MaxConnections);
    m_threadNum = Min(m_threadNum, m_maxPoolSize);
}

int ThreadPoolControler::GetThreadNum()
{
    return m_maxPoolSize;
}

ThreadPoolStat* ThreadPoolControler::GetThreadPoolStat(uint32* num)
{
    ThreadPoolStat* result = (ThreadPoolStat*)palloc(m_groupNum * sizeof(ThreadPoolStat));
    int i;

    for (i = 0; i < m_groupNum; i++) {
        m_groups[i]->GetThreadPoolGroupStat(&result[i]);
    }

    *num = m_groupNum;
    return result;
}

void ThreadPoolControler::CloseAllSessions()
{
    m_sessCtrl->MarkAllSessionClose();
    (void)SignalCancelAllBackEnd();

    for (int i = 0; i < m_groupNum; i++) {
        m_groups[i]->GetListener()->SendShutDown();
    }

    /* Check until all groups have closed their sessions. */
    bool allclose = false;
    while (!allclose) {
        if (m_sessCtrl->IsActiveListEmpty()) {
            break;
        }

        allclose = true;
        for (int i = 0; i < m_groupNum; i++) {
            allclose = (m_groups[i]->AllSessionClosed() && allclose);
        }
        pg_usleep(100);
    }
}

void ThreadPoolControler::ShutDownThreads(bool forceWait)
{
    for (int i = 0; i < m_groupNum; i++) {
        m_groups[i]->ShutDownThreads();
    }

    if (forceWait) {
        /* Check until all groups have shut down their workers. */
        bool allshut = false;
        while (!allshut) {
            allshut = true;
            for (int i = 0; i < m_groupNum; i++) {
                allshut = (m_groups[i]->AllThreadShutDown() && allshut);
            }
            pg_usleep(100);
        }
    }
}

void ThreadPoolControler::ShutDownListeners(bool forceWait)
{
    for (int i = 0; i < m_groupNum; i++) {
        m_groups[i]->GetListener()->ShutDown();
    }
    if (forceWait) {
        bool allshut = false;
        while (!allshut) {
            allshut = true;
            for (int i = 0; i < m_groupNum; i++) {
                allshut = (m_groups[i]->GetListener()->GetThreadId() == 0) && allshut;
            }
            pg_usleep(100);
        }
    }
}

void ThreadPoolControler::ShutDownScheduler(bool forceWait)
{
    m_scheduler->ShutDown();
    if (forceWait) {
        bool allshut = false;
        while (!allshut) {
            allshut = m_scheduler->HasShutDown();
            pg_usleep(100);
        }
    }
}

void ThreadPoolControler::AddWorkerIfNecessary()
{
    for (int i = 0; i < m_groupNum; i++)
        m_groups[i]->AddWorkerIfNecessary();
    if (m_scheduler->HasShutDown()) {
        m_scheduler->StartUp();
        m_scheduler->SetShutDown(false);
    }
}

ThreadPoolGroup* ThreadPoolControler::FindThreadGroupWithLeastSession()
{
    int idx = 0;
    float4 least_session = 0.0;
    float4 session_per_thread = 0.0;

    least_session = m_groups[0]->GetSessionPerThread();
    for (int i = 1; i < m_groupNum; i++) {
        session_per_thread = m_groups[i]->GetSessionPerThread();
        if (session_per_thread < least_session) {
            least_session = session_per_thread;
            idx = i;
        }
    }

    return m_groups[idx];
}

bool ThreadPoolControler::StayInAttachMode()
{
    return m_sessCtrl->GetActiveSessionCount() < m_threadNum;
}

int ThreadPoolControler::DispatchSession(Port* port)
{
    ThreadPoolGroup* grp = NULL;
    knl_session_context* sc = NULL;

    grp = FindThreadGroupWithLeastSession();
    if (grp == NULL) {
        Assert(false);
        return STATUS_ERROR;
    }

    sc = m_sessCtrl->CreateSession(port);
    if (sc == NULL)
        return STATUS_ERROR;

    grp->GetListener()->AddNewSession(sc);
    return STATUS_OK;
}

/*
 * Bind the specified thread to all the available CPUs.
 * This is invoked by auxiliary thread, such as WALSender.
 */
void ThreadPoolControler::BindThreadToAllAvailCpu(ThreadId thread) const
{
    if (!CheckCpuBind()) {
        return;
    }

    if (m_cpuInfo.bindType == ALL_CPU_BIND) {
        return;
    }

    cpu_set_t availCpuSet;
    CPU_ZERO(&availCpuSet);
    for (int numaNo = 0; numaNo < m_cpuInfo.totalNumaNum; ++numaNo) {
        int cpuNumber = m_cpuInfo.cpuArrSize[numaNo];
        for (int i = 0; i < cpuNumber; ++i) {
            CPU_SET(m_cpuInfo.cpuArr[numaNo][i], &availCpuSet);
        }
    }
    int ret = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &availCpuSet);
    if (ret != 0)
        ereport(WARNING, (errmsg("BindThreadToAllAvailCpu fail to bind thread %lu, errno: %d", thread, ret)));
}

