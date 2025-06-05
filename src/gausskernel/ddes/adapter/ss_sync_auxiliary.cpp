/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * ss_sync_auxiliary.cpp
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_sync_auxiliary.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "ddes/dms/ss_sync_auxiliary.h"
#include "ddes/dms/ss_transaction.h"
#include "postgres.h"
#include "storage/procarray.h"
#include "storage/ipc.h"

#define SYNC_AUXILIARY_SLEEP_TIME (60*60*1000) //  1h 60*60*1000ms

static void sync_sighup_handler(SIGNAL_ARGS)
{
    if (SS_PRIMARY_MODE) {
        SSBroadcastSyncGUC();
    }
}

static void sync_auxiliary_request_shutdown_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.sync_auxiliary_cxt.shutdown_requested = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }
    errno = save_errno;
}

static void sync_auxiliary_siguser1_handler(SIGNAL_ARGS)
{
    int save_errno = errno;
    latch_sigusr1_handler();
    errno = save_errno;
}

static void SetupSyncAuxiliarySignalHook(void)
{
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGHUP, sync_sighup_handler);
    (void)gspqsignal(SIGINT, sync_auxiliary_request_shutdown_handler);
    (void)gspqsignal(SIGTERM, sync_auxiliary_request_shutdown_handler);
    (void)gspqsignal(SIGQUIT, sync_auxiliary_request_shutdown_handler); /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, sync_auxiliary_siguser1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGURG, print_stack);
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);
}

void sync_auxiliary_handle_exception()
{
    /* Since not using PG_TRY, must reset error stack by hand */
    t_thrd.log_cxt.error_context_stack = NULL;
    t_thrd.log_cxt.call_stack = NULL;

    /* Prevent interrupts while cleaning up */
    HOLD_INTERRUPTS();

    if (hash_get_seq_num() > 0) {
        release_all_seq_scan();
    }

    LWLockReleaseAll();
 
    /* Report the error to the server log */
    EmitErrorReport();

    /* Now we can allow interrupts again */
    RESUME_INTERRUPTS();

    /*
     * Sleep at least 1 second after any error.  A write error is likely
     * to be repeated, and we don't want to be filling the error logs as
     * fast as we can.
     */
    pg_usleep(1000000L);
    return;
}

void SyncAuxiliaryMain(void)
{
    sigjmp_buf localSigjmpBuf;
    t_thrd.role = SYNCINFO_THREAD;
    SetupSyncAuxiliarySignalHook();
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        ereport(WARNING, (errmodule(MOD_DMS), errmsg("[SS] sync auxiliary thread exception occured.")));
        sync_auxiliary_handle_exception();
    }

    /* we can now handle ereport(ERROR) */
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    if (SS_STANDBY_MODE) {
        /* the standby node restarts or expands capacity */
        if (gs_signal_send(PostmasterPid, SIGHUP) != 0) {
            ereport(WARNING, (errmsg("[SS] failed to send SIGHUP during startup")));
        }
    }

    for (;;) {
        if (t_thrd.sync_auxiliary_cxt.shutdown_requested) {
            u_sess->attr.attr_common.ExitOnAnyError = true;
            proc_exit(0);
        }

        if (g_instance.dms_cxt.dms_status < DMS_STATUS_IN || SS_IN_REFORM) {
            pg_usleep(SS_REFORM_WAIT_TIME);
            continue;
        }

        int rc = WaitLatch(&t_thrd.proc->procLatch, WL_TIMEOUT | WL_POSTMASTER_DEATH, SYNC_AUXILIARY_SLEEP_TIME);
        if (rc & WL_POSTMASTER_DEATH) {
            gs_thread_exit(1);
        }

        /* the primary node notify PM to synchronize */
        if (SS_PRIMARY_MODE) {
            if (gs_signal_send(PostmasterPid, SIGHUP) != 0) {
                ereport(WARNING, (errmsg("[SS] send SIGHUP to PM failed when time is up")));
            }
        }
    }
}