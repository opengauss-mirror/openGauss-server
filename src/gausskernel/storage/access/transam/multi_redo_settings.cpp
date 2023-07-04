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
 * settings.cpp
 *         Defines GUC options for parallel recovery.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/transam/parallel_recovery/settings.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stdio.h>
#include <unistd.h>
#ifdef __USE_NUMA
    #include <numa.h>
#endif
#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/guc.h"

#include "access/multi_redo_settings.h"
#include "access/multi_redo_api.h"
#include "threadpool/threadpool_controler.h"

static uint32 ComputeRecoveryParallelism(int);
static uint32 GetCPUCount();
static const int bufSize = 1024;

void ConfigRecoveryParallelism()
{
    char buf[16]; /* 16 is enough */

    if (g_instance.attr.attr_storage.recovery_parse_workers > 1) {
        g_instance.comm_cxt.predo_cxt.redoType = EXTREME_REDO;
        g_instance.attr.attr_storage.batch_redo_num = g_instance.attr.attr_storage.recovery_parse_workers;
        uint32 total_recovery_parallelism = g_instance.attr.attr_storage.batch_redo_num * 2 +
                                            g_instance.attr.attr_storage.recovery_redo_workers_per_paser_worker *
                                                g_instance.attr.attr_storage.batch_redo_num +
                                            TRXN_REDO_MANAGER_NUM + TRXN_REDO_WORKER_NUM + XLOG_READER_NUM;
        sprintf_s(buf, sizeof(buf), "%u", total_recovery_parallelism);

        ereport(LOG, (errmsg("ConfigRecoveryParallelism, parse workers:%d, "
                             "redo workers per parse worker:%d, total workernums is %u",
                             g_instance.attr.attr_storage.recovery_parse_workers,
                             g_instance.attr.attr_storage.recovery_redo_workers_per_paser_worker,
                             total_recovery_parallelism)));
        g_supportHotStandby = false;
        SetConfigOption("recovery_parallelism", buf, PGC_POSTMASTER, PGC_S_OVERRIDE);
    } else if (g_instance.attr.attr_storage.max_recovery_parallelism > 1) {
        g_instance.comm_cxt.predo_cxt.redoType = PARALLEL_REDO;
        uint32 true_max_recovery_parallelism = g_instance.attr.attr_storage.max_recovery_parallelism;
        if (true_max_recovery_parallelism > MOST_FAST_RECOVERY_LIMIT) {
            true_max_recovery_parallelism = MOST_FAST_RECOVERY_LIMIT;
        }
        sprintf_s(buf, sizeof(buf), "%u", ComputeRecoveryParallelism(true_max_recovery_parallelism));
        ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                      errmsg("ConfigRecoveryParallelism, true_max_recovery_parallelism:%u, "
                             "max_recovery_parallelism:%d",
                             true_max_recovery_parallelism, g_instance.attr.attr_storage.max_recovery_parallelism)));
        SetConfigOption("recovery_parallelism", buf, PGC_POSTMASTER, PGC_S_OVERRIDE);
    }
}

static uint32 ComputeRecoveryParallelism(int hint)
{
    /*
     * Reciprocal of the shares of CPU used for recovery.  The idea is that
     * using the default value the standby is able to keep up with the master
     * (assuming the standby and the master use the same hardware), while on
     * machines with fewer CPUs, the user is able to boost up recovery
     * performance by using more CPUs.  A capped maximum is used to protect
     * users from setting hugh values.
     *
     * The default is to use 1/32 of all CPUs.  On beefy machines, the capped
     * maximum is 1/4 of all CPUs.  On smaller machines, the capped maximum
     * is the number of CPUs or 8, whichever is smaller.
     */
    static const uint32 DEFAULT_CPU_SHARE = 32;
    static const uint32 MAX_CPU_SHARE = 4;
    static const uint32 MIN_ALLOWED_MAX_PARALLELISM = 8;
    uint32 g_cpu_count = 0;

    if (g_cpu_count == 0)
        g_cpu_count = GetCPUCount();
    uint32 default_parallelism = g_cpu_count / DEFAULT_CPU_SHARE;
    uint32 max_parallelism;
    if (g_cpu_count < MIN_ALLOWED_MAX_PARALLELISM)
        max_parallelism = g_cpu_count;
    else if (g_cpu_count / MAX_CPU_SHARE < MIN_ALLOWED_MAX_PARALLELISM)
        max_parallelism = MIN_ALLOWED_MAX_PARALLELISM;
    else
        max_parallelism = g_cpu_count / MAX_CPU_SHARE;

    uint32 actual_parallelism;
    if (hint <= 0)
        actual_parallelism = default_parallelism;
    else if (((uint32)hint) < max_parallelism)
        actual_parallelism = hint;
    else
        actual_parallelism = max_parallelism;

    /* We need to have at least one recovery thread. */
    if (actual_parallelism < 1)
        actual_parallelism = 1;

    ereport(LOG, (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                  errmsg("Recovery parallelism, cpu count = %u, max = %d, actual = %u", g_cpu_count, hint,
                         actual_parallelism)));
    return actual_parallelism;
}

static uint32 GetCPUCount()
{
#ifdef _SC_NPROCESSORS_ONLN
    return (uint32)sysconf(_SC_NPROCESSORS_ONLN);
#else
    static const uint32 DEFAULT_CPU_COUNT = 64;
    return DEFAULT_CPU_COUNT;
#endif
}

void ParseBindCpuInfo(RedoCpuBindControl *control)
{
    char* attr = TrimStr(g_instance.attr.attr_storage.redo_bind_cpu_attr);
    if (attr == NULL) {
        return;
    }

    char* ptoken = NULL;
    char* psave = NULL;
    const char* pdelimiter = ":";

    ptoken = TrimStr(strtok_r(attr, pdelimiter, &psave));
    ptoken = pg_strtolower(ptoken);

    int bindNum = 0;
    if (strncmp("nobind", ptoken, strlen("nobind")) == 0) {
        control->bindType = REDO_NO_CPU_BIND;
        return;
    } else if (strncmp("cpubind", ptoken, strlen("cpubind")) == 0) {
        control->bindType = REDO_CPU_BIND;
        control->isBindCpuArr = (bool*)palloc0(sizeof(bool) * control->totalCpuNum);
        bindNum = ThreadPoolControler::ParseRangeStr(psave, control->isBindCpuArr, control->totalCpuNum, "cpubind");
    } else if (strncmp("nodebind", ptoken, strlen("nodebind")) == 0) {
        control->bindType = REDO_NODE_BIND;
        control->isBindNumaArr = (bool*)palloc0(sizeof(bool) * control->totalCpuNum);
        bindNum = ThreadPoolControler::ParseRangeStr(psave, control->isBindNumaArr, control->totalNumaNum, "nodebind");
    } else {
        ereport(FATAL, (errcode(ERRCODE_OPERATE_INVALID_PARAM), errmsg("Invalid attribute for multi redo."),
            errdetail("redo bind config Only support 'nobind', 'cpubind', and 'nodebind'.")));
    }

    if (bindNum == 0) {
        ereport(FATAL, (errcode(ERRCODE_OPERATE_INVALID_PARAM), errmsg("Invalid attribute for multi redo."),
            errdetail("Can not find valid CPU for thread binding, there are two possible reasons:\n"
                      "1. These CPUs are not active, use lscpu to check On-line CPU(s) list.\n"
                      "2. The process has been bind to other CPUs and there is no intersection,"
                      "use taskset -pc to check process CPU bind info.\n")));
    }
}

bool CpuCanConfiged(RedoCpuBindControl *contrl, int cpuid, int numaid)
{
    switch (contrl->bindType) {
        case REDO_NODE_BIND:
            return (contrl->isBindNumaArr[numaid] && contrl->isMcsCpuArr[cpuid] && CPU_ISSET(cpuid, &contrl->cpuSet));
        case REDO_CPU_BIND:
            return (contrl->isBindCpuArr[cpuid] && contrl->isMcsCpuArr[cpuid] && CPU_ISSET(cpuid, &contrl->cpuSet));
        default:
            return false;
    }
    return false;
}

void ConfigBindCpuInfo(RedoCpuBindControl *contrl)
{
    contrl->isMcsCpuArr = ThreadPoolControler::GetMcsCpuInfo(contrl->totalCpuNum);

    NumaCpuId *sysNumaCpuIdList = (NumaCpuId*)palloc0(sizeof(NumaCpuId) * contrl->totalCpuNum);
    int sysNumaCpuIdNum = 0;
    ThreadPoolControler::GetActiveCpu(sysNumaCpuIdList, &sysNumaCpuIdNum);

    if (sysNumaCpuIdNum == 0) {
        return;
    }

    contrl->cpuArr = (int*)palloc0(sizeof(int) * contrl->totalCpuNum);

    for (int i = 0; i < sysNumaCpuIdNum; ++i) {
        int cpuid = sysNumaCpuIdList[i].cpuId;
        int numaid = sysNumaCpuIdList[i].numaId;
        if (CpuCanConfiged(contrl, cpuid, numaid)) {
            contrl->cpuArr[contrl->activeCpuNum++] = cpuid;
        }
    }

    pfree_ext(sysNumaCpuIdList);
    ereport(LOG, (errmsg("ConfigBindCpuInfo redo bind cpu num: %d.", contrl->activeCpuNum)));

    CPU_ZERO(&contrl->configCpuSet);
    for (int i = 0; i < contrl->activeCpuNum; ++i) {
        CPU_SET(contrl->cpuArr[i], &contrl->configCpuSet);
    }
}

bool CheckRedoBindConfigValid(RedoCpuBindControl *contrl)
{
    if (contrl->bindType == REDO_NO_CPU_BIND) {
        return false;
    }

    if (contrl->totalNumaNum == 0 || contrl->totalCpuNum == 0) {
        ereport(WARNING, (errmsg("CheckRedoBindConfigValid: Fail to read cpu num(%d) or numa num(%d).",
            contrl->totalNumaNum, contrl->totalCpuNum)));
        return false;
    }
#ifdef __USE_NUMA
    int numaNodeNum = numa_max_node() + 1;
    if (numaNodeNum <= 1) {
        ereport(WARNING, (errmsg("CheckRedoBindConfigValid No multiple NUMA nodes available: %d.", numaNodeNum)));
        return false;
    } else if (contrl->totalNumaNum != numaNodeNum) {
        ereport(WARNING, (errmsg("Cannot activate NUMA distribute because NUMA nodes are unavailable.")));
        return false;
    } else {
        return true;
    }
#endif
    return false;
}

void ProcessRedoCpuBindInfo()
{
    RedoCpuBindControl *contrl = &g_instance.comm_cxt.predo_cxt.redoCpuBindcontrl;
    ThreadPoolControler::GetInstanceBind(&contrl->cpuSet);
    ThreadPoolControler::GetCpuAndNumaNum(&contrl->totalCpuNum, &contrl->totalNumaNum);
    ParseBindCpuInfo(contrl);
    if (CheckRedoBindConfigValid(contrl)) {
        ConfigBindCpuInfo(contrl);
    }
}

void BindRedoThreadToSpecifiedCpu(knl_thread_role thread_role)
{
#ifdef __USE_NUMA
    if (thread_role == STARTUP || thread_role == PAGEREDO) {
        RedoCpuBindControl *contrl = &g_instance.comm_cxt.predo_cxt.redoCpuBindcontrl;
        if (contrl->activeCpuNum > 0) {
            int ret = pthread_setaffinity_np(gs_thread_self(), sizeof(cpu_set_t), &contrl->configCpuSet);
            if (ret != 0) {
                ereport(WARNING, (errmsg("Fail to attach %d thread %lu active cpu num %d, %d", (int)thread_role,
                    gs_thread_self(), contrl->activeCpuNum, ret)));
            }
        }
    }
#endif
}

