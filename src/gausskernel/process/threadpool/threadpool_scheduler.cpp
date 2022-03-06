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
 * threadpool_scheduler.cpp
 *    Scheduler thread is used to manage thread num in thread pool.
 *
 * IDENTIFICATION
 *    src/gausskernel/process/threadpool/threadpool_scheduler.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "threadpool/threadpool.h"

#include "access/xact.h"
#include "gssignal/gs_signal.h"
#include "libpq/libpq.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/atomic.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/guc.h"
#include "replication/syncrep.h"

#define SCHEDULER_TIME_UNIT 1000000  //us
#define ENLARGE_THREAD_TIME 5
#define MAX_HANG_TIME 100
#define REDUCE_THREAD_TIME 100
#define SHUTDOWN_THREAD_TIME 1000
#define GPC_CLEAN_TIME 300

static void SchedulerSIGKILLHandler(SIGNAL_ARGS)
{
    t_thrd.threadpool_cxt.scheduler->m_getKilled = true;
}

void ThreadPoolScheduler::SigHupHandler()
{
    m_getSIGHUP = true;
}

static void reloadConfigFileIfNecessary()
{
    if (unlikely(t_thrd.threadpool_cxt.scheduler->m_getSIGHUP)) {
        t_thrd.threadpool_cxt.scheduler->m_getSIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
        /* Update most_available_sync if it's modified dynamically. */
        most_available_sync = (volatile bool) u_sess->attr.attr_storage.guc_most_available_sync;
        SyncRepUpdateSyncStandbysDefined();
    }
}

void TpoolSchedulerMain(ThreadPoolScheduler *scheduler)
{
    int gpc_count = 0;

    (void)gspqsignal(SIGKILL, SchedulerSIGKILLHandler);
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
    if (ENABLE_GPC) {
        scheduler->m_gpcContext = AllocSetContextCreate(CurrentMemoryContext,
                                                        "GPCScheduler",
                                                        ALLOCSET_SMALL_MINSIZE,
                                                        ALLOCSET_SMALL_INITSIZE,
                                                        ALLOCSET_DEFAULT_MAXSIZE);
    }

    while (true) {
        if (unlikely(scheduler->m_getKilled)) {
            scheduler->m_getKilled = false;
            proc_exit(0);
        }
        pg_usleep(SCHEDULER_TIME_UNIT);
        reloadConfigFileIfNecessary();
        scheduler->DynamicAdjustThreadPool();
        scheduler->GPCScheduleCleaner(&gpc_count);
        g_threadPoolControler->GetSessionCtrl()->CheckSessionTimeout();
    }
    proc_exit(0);
}

ThreadPoolScheduler::ThreadPoolScheduler(int groupNum, ThreadPoolGroup** groups)
:m_groupNum(groupNum), m_groups(groups), m_has_shutdown(false)
{
    m_tid = 0;
    m_hangTestCount = (uint *)palloc0(sizeof(uint) * groupNum);
    m_freeTestCount = (uint *)palloc0(sizeof(uint) * groupNum);
    m_freeStreamCount = (uint *)palloc0(sizeof(uint) * groupNum);
    m_gpcContext = NULL;
    m_getSIGHUP = false;
    m_canAdjustPool = true;
}

ThreadPoolScheduler::~ThreadPoolScheduler()
{
    m_groups = NULL;
    m_hangTestCount = NULL;
    m_freeStreamCount = NULL;
    m_freeTestCount = NULL;
}

int ThreadPoolScheduler::StartUp()
{
    m_tid = initialize_util_thread(THREADPOOL_SCHEDULER, (void*)this);
    return ((m_tid == 0) ? STATUS_ERROR : STATUS_OK);
}

void ThreadPoolScheduler::DynamicAdjustThreadPool()
{
    for (int i = 0; i < m_groupNum; i++) {
        if (pmState == PM_RUN && m_canAdjustPool) {
            AdjustWorkerPool(i);
            AdjustStreamPool(i);
        }
    }
}

void ThreadPoolScheduler::GPCScheduleCleaner(int* gpc_count)
{
    if (ENABLE_GPC && *gpc_count == GPC_CLEAN_TIME) {
        if (pmState == PM_RUN || pmState == PM_HOT_STANDBY) {
            MemoryContext oldCxt = MemoryContextSwitchTo(m_gpcContext);
            pthread_mutex_lock(&g_instance.gpc_reset_lock);
            g_instance.plan_cache->DropInvalid();
            g_instance.plan_cache->CleanUpByTime();
            pthread_mutex_unlock(&g_instance.gpc_reset_lock);
            (void)MemoryContextSwitchTo(oldCxt);
            MemoryContextReset(m_gpcContext);
        }
        *gpc_count = 0;
    }
    (*gpc_count)++;
}

void ThreadPoolScheduler::ShutDown() const
{
    if (m_tid != 0)
        gs_signal_send(m_tid, SIGKILL);
}

void ThreadPoolScheduler::AdjustWorkerPool(int idx)
{
    ThreadPoolGroup* group = m_groups[idx];
    /* When no idle worker and no task has been processed, the system may hang. */
    if (group->IsGroupHang()) {
        m_hangTestCount[idx]++;
        m_freeTestCount[idx] = 0;
        EnlargeWorkerIfNecessage(idx);
    } else {
        group->SetGroupHanged(false);
        m_hangTestCount[idx] = 0;
        m_freeTestCount[idx]++;
        ReduceWorkerIfNecessary(idx);
    }
}

void ThreadPoolScheduler::AdjustStreamPool(int idx)
{
#ifdef ENABLE_MULTIPLE_NODES
    ThreadPoolGroup* group = m_groups[idx];

    if (group->HasFreeStream()) {
        m_freeStreamCount[idx]++;
        if (m_freeStreamCount[idx] == SHUTDOWN_THREAD_TIME) {
            group->ReduceStreams();
            m_freeStreamCount[idx] = 0;
        }
    } else {
        m_freeStreamCount[idx] = 0;
    }
#endif
}

void ThreadPoolScheduler::EnlargeWorkerIfNecessage(int groupIdx)
{
    ThreadPoolGroup *group = m_groups[groupIdx];
    if (m_hangTestCount[groupIdx] >= ENLARGE_THREAD_TIME && m_hangTestCount[groupIdx] < MAX_HANG_TIME) {
        if (group->EnlargeWorkers(THREAD_SCHEDULER_STEP)) {
            m_hangTestCount[groupIdx] = 0;
        }
    } else if (m_hangTestCount[groupIdx] == MAX_HANG_TIME) {
        elog(WARNING, "[SCHEDULER] Detect the system has hang %d seconds, "
            "and the thread num in pool exceed maximum, "
            "so we need to close all new sessions.", MAX_HANG_TIME);
        /* set flag for don't accept new session */
        group->SetGroupHanged(true);
    }
}

void ThreadPoolScheduler::ReduceWorkerIfNecessary(int groupIdx)
{
    ThreadPoolGroup *group = m_groups[groupIdx];

    if (group->m_expectWorkerNum == group->m_defaultWorkerNum &&
        group->m_pendingWorkerNum == 0) {
        m_freeTestCount[groupIdx] = 0;
        return;
    }

    if (m_freeTestCount[groupIdx] %  REDUCE_THREAD_TIME == 0) {
        group->ReduceWorkers(THREAD_SCHEDULER_STEP);
    }

    if (m_freeTestCount[groupIdx] == SHUTDOWN_THREAD_TIME) {
        group->ShutDownPendingWorkers();
        m_freeTestCount[groupIdx] = 0;
    }
}

