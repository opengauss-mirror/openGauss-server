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

#define SCHEDULER_TIME_UNIT 1000000  // us
#define ENLARGE_THREAD_TIME 5
#define MAX_HANG_TIME 100
#define REDUCE_THREAD_TIME 100
#define SHUTDOWN_THREAD_TIME 1000

void TpoolSchedulerMain(ThreadPoolScheduler *scheduler)
{
    while (true) {
        pg_usleep(SCHEDULER_TIME_UNIT);
        scheduler->DynamicAdjustThreadPool();
    }
    proc_exit(0);
}

ThreadPoolScheduler::ThreadPoolScheduler(int groupNum, ThreadPoolGroup** groups)
    :m_groupNum(groupNum)
    , m_groups(groups)
{
    m_tid = 0;
    m_hangTestCount = (uint *)palloc0(sizeof(uint) * groupNum);
    m_freeTestCount = (uint *)palloc0(sizeof(uint) * groupNum);
}

int ThreadPoolScheduler::StartUp()
{
    m_tid = initialize_util_thread(THREADPOOL_SCHEDULER, (void*)this);
    return (m_tid == 0 ? STATUS_ERROR : STATUS_OK);
}

void ThreadPoolScheduler::DynamicAdjustThreadPool()
{
    ThreadPoolGroup* group = NULL;

    for (int i = 0; i < m_groupNum; i++) {
        group = m_groups[i];

        if (pmState == PM_RUN) {
            /* When no idle worker and no task has been processed, the system may hang. */
            if (group->IsGroupHang()) {
                m_hangTestCount[i]++;
                m_freeTestCount[i] = 0;
                EnlargeWorkerIfNecessage(i);
            } else {
                m_hangTestCount[i] = 0;
                m_freeTestCount[i]++;
                ReduceWorkerIfNecessary(i);
            }
        }
    }
}

void ThreadPoolScheduler::EnlargeWorkerIfNecessage(int groupIdx)
{
    ThreadPoolGroup *group = m_groups[groupIdx];
    if (m_hangTestCount[groupIdx] == ENLARGE_THREAD_TIME) {
        if (group->EnlargeWorkers(THREAD_SCHEDULER_STEP)) {
            m_hangTestCount[groupIdx] = 0;
        }
    } else if (m_hangTestCount[groupIdx] == MAX_HANG_TIME) {
        elog(LOG, "[SCHEDULER] Detect the system has hang %d seconds, "
            "and the thread num in pool exceed maximum, "
            "so we need to cancel all current transactions.", MAX_HANG_TIME);
        (void)SignalCancelAllBackEnd();
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

