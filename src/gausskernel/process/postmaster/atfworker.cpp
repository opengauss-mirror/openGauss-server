/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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
 * atfworker.cpp
 *     Active completion of the application transparent failover stage.
 *
 * IDENTIFICATION
 *     src/gausskernel/process/postmaster/atfworker.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"

#include "ddes/dms/ss_common_attr.h"
#include "libpq/pqsignal.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/lock/lwlock.h"
#include "storage/procsignal.h"
#include "tcop/tcopprot.h"
#include "utils/timestamp.h"
#include "postmaster/atfworker.h"

static inline int GetAtfTimeDiffSec(TimestampTz start, TimestampTz stop)
{
    long secs = 0;
    int microsecs = 0;
    TimestampDifference(start, stop, &secs, &microsecs);
    return (int)secs;
}

bool MarkAtfRecoveryDone()
{
    knl_g_atf_context* atf = &g_instance.atf_cxt;
    bool changed = false;

    LWLockAcquire(atf->global_task_lock, LW_EXCLUSIVE);
    if (!atf->all_task_done) {
        atf->all_task_done = true;
        changed = true;
    }
    LWLockRelease(atf->global_task_lock);

    return changed;
}

void WaitForAtfTaskDone()
{
    if (!ENABLE_ATF_TIMEOUT) {
        return;
    }

    knl_g_atf_context* atf = &g_instance.atf_cxt;
    bool done = false;
    bool timeout = false;
    while (!done) {
        LWLockAcquire(atf->global_task_lock, LW_EXCLUSIVE);
        done = atf->all_task_done;
        if (!done) {
            TimestampTz now = GetCurrentTimestamp();
            int elapsedSec = GetAtfTimeDiffSec(atf->last_counter_update_ts, now);
            if (elapsedSec >= g_instance.attr.attr_common.atf_task_counter_timeout_sec &&
                pg_atomic_read_u64(&atf->global_task_counter) == 0) {
                atf->all_task_done = true;
                done = true;
                timeout = true;
            }
        }
        LWLockRelease(atf->global_task_lock);

        if (!done) {
            CHECK_FOR_INTERRUPTS();
            pg_usleep(ATF_TASK_CHECK_INTERVAL_USEC);
        }
    }

    if (timeout) {
        ereport(LOG, (errmsg("[ATF] recovery stage completed (reason: timeout)")));
    }
}

bool IsAtfRecoveryDone()
{
    if (!ENABLE_ATF_TIMEOUT) {
        return true;
    }

    knl_g_atf_context* atf = &g_instance.atf_cxt;
    LWLockAcquire(atf->global_task_lock, LW_SHARED);
    bool done = atf->all_task_done;
    LWLockRelease(atf->global_task_lock);
    return done;
}

bool PrepareAtfRecoveryStage()
{
    knl_g_atf_context* atf = &g_instance.atf_cxt;
    if (!ENABLE_ATF_TIMEOUT) {
        (void)MarkAtfRecoveryDone();
        return true;
    }

    LWLockAcquire(atf->global_task_lock, LW_EXCLUSIVE);
    pg_atomic_write_u64(&atf->global_task_counter, 0);
    atf->last_counter_update_ts = GetCurrentTimestamp();
    atf->all_task_done = false;
    LWLockRelease(atf->global_task_lock);

    ereport(LOG, (errmsg("[ATF] recovery stage started (timeout: %d seconds)",
        g_instance.attr.attr_common.atf_task_counter_timeout_sec)));
    g_instance.pid_cxt.AtfWorkerPID = initialize_util_thread(ATF_WORKER);
    if (g_instance.pid_cxt.AtfWorkerPID == 0) {
        (void)MarkAtfRecoveryDone();
        ereport(WARNING, (errmsg("[ATF] worker creation failed; recovery stage bypassed")));
        return false;
    }
    return true;
}

void AtfWorkerMain()
{
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, die);
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);

    WaitForAtfTaskDone();
    proc_exit(0);
}
