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
    m_groupNum = 1;
    m_threadNum = 0;
    m_maxPoolSize = 0;
}

ThreadPoolControler::~ThreadPoolControler()
{
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

    bool bind_cpu = CheckCpuBind();
    int group_thread_num = 0;
    int max_thread_num = 0;
    int numa_id = 0;

    m_groups = (ThreadPoolGroup**)palloc(sizeof(ThreadPoolGroup*) * m_groupNum);
    m_sessCtrl = New(CurrentMemoryContext) ThreadPoolSessControl(CurrentMemoryContext);

    for (int i = 0; i < m_groupNum; i++) {
        if (bind_cpu) {
            while (m_cpuInfo.cpuArrSize[numa_id] == 0)
                numa_id++;

            /*
             * Invoke numa_set_preferred before starting worker thread to make 
             * more memory allocation local to worker thread.
             */
#ifdef __USE_NUMA
            if (enableNumaDistribute) {
                numa_set_preferred(numa_id);
            }
#endif
            Assert(numa_id < m_cpuInfo.totalNumaNum);
            group_thread_num = (int)round(
                (double)m_threadNum * ((double)m_cpuInfo.cpuArrSize[numa_id] / (double)m_cpuInfo.activeCpuNum));
            max_thread_num = (int)round(
                (double)m_maxPoolSize * ((double)m_cpuInfo.cpuArrSize[numa_id] / (double)m_cpuInfo.activeCpuNum));
            m_groups[i] = New(CurrentMemoryContext)
                        ThreadPoolGroup(max_thread_num, group_thread_num, i, numa_id,
                        m_cpuInfo.cpuArrSize[numa_id], m_cpuInfo.cpuArr[numa_id]);
            numa_id++;
        } else {
            group_thread_num = m_threadNum / m_groupNum;
            max_thread_num = m_maxPoolSize / m_groupNum;
            m_groups[i] = New(CurrentMemoryContext)
                        ThreadPoolGroup(max_thread_num, group_thread_num, i, -1, 0, NULL);
        }
        m_groups[i]->init(enableNumaDistribute);
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

    char* p_token = NULL;
    char* p_save = NULL;
    const char* p_delimiter = ",";

    /* Get thread num */
    p_token = TrimStr(strtok_r(attr, p_delimiter, &p_save));
    if (!IS_NULL_STR(p_token))
        m_attr.threadNum = pg_strtoint32(p_token);

    /* Ger group num */
    p_token = TrimStr(strtok_r(NULL, p_delimiter, &p_save));
    if (!IS_NULL_STR(p_token))
        m_attr.groupNum = pg_strtoint32(p_token);

    if (m_attr.threadNum < 0 || m_attr.threadNum > MAX_THREAD_POOL_SIZE)
        INVALID_ATTR_ERROR(
            errdetail("Current thread num %d is out of range [%d, %d].", m_attr.threadNum, 0, MAX_THREAD_POOL_SIZE));
    if (m_attr.groupNum < 0 || m_attr.groupNum > MAX_THREAD_POOL_GROUPS)
        INVALID_ATTR_ERROR(
            errdetail("Current group num %d is out of range [%d, %d].", m_attr.groupNum, 0, MAX_THREAD_POOL_GROUPS));

    /* Get attach cpu */
    m_attr.bindCpu = TrimStr(p_save);
    ParseBindCpu();
}

void ThreadPoolControler::ParseBindCpu()
{
    if (IS_NULL_STR(m_attr.bindCpu)) {
        m_cpuInfo.bindType = NO_CPU_BIND;
        return;
    }

    char* s_cpu = pstrdup(m_attr.bindCpu);
    char* p_token = NULL;
    char* p_save = NULL;
    const char* p_delimiter = ":";
    int bind_Num = 0;

    if (s_cpu[0] != '(' || s_cpu[strlen(s_cpu) - 1] != ')')
        INVALID_ATTR_ERROR("Use '(' ')' to indicate cpu bind info.");

    s_cpu++;
    s_cpu[strlen(s_cpu) - 1] = '\0';

    p_token = TrimStr(strtok_r(s_cpu, p_delimiter, &p_save));
    p_token = pg_strtolower(p_token);
    if (strncmp("nobind", p_token, strlen("nobind")) == 0) {
        m_cpuInfo.bindType = NO_CPU_BIND;
        return;
    } else if (strncmp("allbind", p_token, strlen("allbind")) == 0) {
        m_cpuInfo.bindType = ALL_CPU_BIND;
        return;
    } else if (strncmp("cpubind", p_token, strlen("cpubind")) == 0) {
        m_cpuInfo.bindType = CPU_BIND;
        m_cpuInfo.isBindCpuArr = (bool*)palloc0(sizeof(bool) * m_cpuInfo.totalCpuNum);
        bind_Num = ParseRangeStr(p_save, m_cpuInfo.isBindCpuArr, m_cpuInfo.totalCpuNum, "cpubind");
    } else if (strncmp("nodebind", p_token, strlen("nodebind")) == 0) {
        m_cpuInfo.bindType = NODE_BIND;
        m_cpuInfo.isBindNumaArr = (bool*)palloc0(sizeof(bool) * m_cpuInfo.totalCpuNum);
        bind_Num = ParseRangeStr(p_save, m_cpuInfo.isBindNumaArr, m_cpuInfo.totalNumaNum, "nodebind");
    } else {
        INVALID_ATTR_ERROR(errdetail("Only 'nobind', 'allbind', 'cpubind', and 'nodebind' are valid attribute."));
    }

    if (bind_Num == 0)
        INVALID_ATTR_ERROR(
            errdetail("Can not find valid CPU for thread binding, there are two possible reasons:\n"
                         "1. These CPUs are not active, use lscpu to check On-line CPU(s) list.\n"
                         "2. The process has been bind to other CPUs and there is no intersection,"
                         "use taskset -pc to check process CPU bind info.\n"));
}

int ThreadPoolControler::ParseRangeStr(char* attr, bool* arr, int total_num, char* bind_type)
{
    char* p_token = NULL;
    char* p_save = NULL;
    const char* p_delimiter = ",";
    int ret_Num = 0;

    p_token = TrimStr(strtok_r(attr, p_delimiter, &p_save));

    while (!IS_NULL_STR(p_token)) {
        char* pt = NULL;
        char* ps = NULL;
        const char* pd = "-";
        int start_id = -1;
        int end_id = -1;

        pt = TrimStr(strtok_r(p_token, pd, &ps));
        if (!IS_NULL_STR(pt))
            start_id = pg_strtoint32(pt);
        if (!IS_NULL_STR(ps))
            end_id = pg_strtoint32(ps);

        if (start_id < 0 && end_id < 0)
            INVALID_ATTR_ERROR(errdetail("Can not parse attribute %s", pt));
        if (start_id >= total_num)
            INVALID_ATTR_ERROR(
                errdetail("The %s attribute %d is out of valid range [%d, %d]", bind_type, start_id, 0, total_num - 1));
        if (end_id >= total_num)
            INVALID_ATTR_ERROR(
                errdetail("The %s attribute %d is out of valid range [%d, %d]", bind_type, end_id, 0, total_num - 1));

        if (end_id == -1) {
            ret_Num += arr[start_id] ? 0 : 1;
            arr[start_id] = true;
        } else {
            if (start_id > end_id) {
                int tmpid = start_id;
                start_id = end_id;
                end_id = tmpid;
            }

            for (int i = start_id; i <= end_id; i++) {
                ret_Num += arr[start_id] ? 0 : 1;
                arr[i] = true;
            }
        }

        p_token = TrimStr(strtok_r(NULL, p_delimiter, &p_save));
    }

    return ret_Num;
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

    char* p_token = NULL;
    char* p_save = NULL;
    const char* p_delimiter = " ";
    int cpu_id = 0;
    int numa_id = 0;
    /* try to read the header. */
    if (fgets(buf, sizeof(buf), fp) != NULL) {
        m_cpuInfo.activeCpuNum = 0;
        while (fgets(buf, sizeof(buf), fp) != NULL) {
            p_token = strtok_r(buf, p_delimiter, &p_save);
            if (!IS_NULL_STR(p_token))
                cpu_id = pg_strtoint32(p_token);

            p_token = strtok_r(NULL, p_delimiter, &p_save);
            if (!IS_NULL_STR(p_token))
                numa_id = pg_strtoint32(p_token);

            if (IsActiveCpu(cpu_id, numa_id)) {
                m_cpuInfo.cpuArr[numa_id][m_cpuInfo.cpuArrSize[numa_id]] = cpu_id;
                m_cpuInfo.cpuArrSize[numa_id]++;
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

bool ThreadPoolControler::IsActiveCpu(int cpu_id, int numa_id)
{
    switch (m_cpuInfo.bindType) {
        case NO_CPU_BIND:
        case ALL_CPU_BIND:
            return (m_cpuInfo.isMcsCpuArr[cpu_id] && CPU_ISSET(cpu_id, &m_cpuset));
        case NODE_BIND:
            return (m_cpuInfo.isBindNumaArr[numa_id] && m_cpuInfo.isMcsCpuArr[cpu_id] && CPU_ISSET(cpu_id, &m_cpuset));
        case CPU_BIND:
            return (m_cpuInfo.isBindCpuArr[cpu_id] && m_cpuInfo.isMcsCpuArr[cpu_id] && CPU_ISSET(cpu_id, &m_cpuset));
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

void ThreadPoolControler::ReBindStreamThread(ThreadId tid) const
{
    int ret = pthread_setaffinity_np(tid, sizeof(cpu_set_t), &m_cpuset);
    if (ret != 0)
        ereport(WARNING, (errmsg("Fail to bind stream thread. Error No: %d", ret)));
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
    bool all_close = false;
    while (!all_close) {
        all_close = true;
        for (int i = 0; i < m_groupNum; i++) {
            all_close = (m_groups[i]->AllSessionClosed() && all_close);
        }
        pg_usleep(100);
    }
}

void ThreadPoolControler::ShutDownWorker(bool forceWait)
{
    for (int i = 0; i < m_groupNum; i++) {
        m_groups[i]->ShutdownWorker();
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

void ThreadPoolControler::AddWorkerIfNecessary()
{
    for (int i = 0; i < m_groupNum; i++)
        m_groups[i]->AddWorkerIfNecessary();
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

