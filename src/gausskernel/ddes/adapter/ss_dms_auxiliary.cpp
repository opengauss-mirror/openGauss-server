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
 * ss_dms_auxiliary.cpp
 *  dms auxiliary related
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_dms_auxiliary.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "ddes/dms/ss_dms_auxiliary.h"
#include "ddes/dms/ss_xmin.h"
#include "postgres.h"
#include "storage/procarray.h"
#include "storage/ipc.h"

#define DMS_AUXILIARY_PRIMARY_SLEEP_TIME (5000) // 5s 5000ms
#define DMS_AUXILIARY_STANDBY_SLEEP_TIME (1000) // 1s 1000ms

static void dms_auxiliary_request_shutdown_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.dms_aux_cxt.shutdown_requested = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }
    errno = save_errno;
}

static void dms_auxiliary_siguser1_handler(SIGNAL_ARGS)
{
    int save_errno = errno;
    latch_sigusr1_handler();
    errno = save_errno;
}

static void SetupDmsAuxiliarySignalHook(void)
{
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGINT, dms_auxiliary_request_shutdown_handler);
    (void)gspqsignal(SIGTERM, dms_auxiliary_request_shutdown_handler);
    (void)gspqsignal(SIGQUIT, dms_auxiliary_request_shutdown_handler); /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, dms_auxiliary_siguser1_handler);
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

void dms_auxiliary_handle_exception()
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

void SSInitXminInfo()
{
    if (!ENABLE_DMS) {
        return;
    }

    ss_xmin_info_t *xmin_info = &g_instance.dms_cxt.SSXminInfo;
    if (xmin_info->snap_cache != NULL) {
        return;
    }

    HASHCTL ctl;
    errno_t rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(ss_snap_xmin_key_t);
    ctl.entrysize = sizeof(ss_snap_xmin_item_t);
    ctl.hash = tag_hash;
    ctl.num_partitions = NUM_SS_SNAPSHOT_XMIN_CACHE_PARTITIONS;
    xmin_info->snap_cache = HeapMemInitHash("DMS snapshot xmin cache", 60, 30000, &ctl,
        HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);
    if (xmin_info->snap_cache == NULL) {
        ereport(FATAL, (errmodule(MOD_DMS), errmsg("could not initialize shared xmin_info hash table")));
    }
}

void DmsAuxiliaryMain(void)
{
    sigjmp_buf localSigjmpBuf;
    t_thrd.role = DMS_AUXILIARY_THREAD;
    SetupDmsAuxiliarySignalHook();
    (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    if (sigsetjmp(localSigjmpBuf, 1) != 0) {
        ereport(WARNING, (errmodule(MOD_DMS), errmsg("dms auxiliary thread exception occured.")));
        dms_auxiliary_handle_exception();
    }

    /* We can now handle ereport(ERROR)*/
    t_thrd.log_cxt.PG_exception_stack = &localSigjmpBuf;

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    for (;;) {
        if (t_thrd.dms_aux_cxt.shutdown_requested) {
            u_sess->attr.attr_common.ExitOnAnyError = true;
            proc_exit(0);
        }

        if (g_instance.dms_cxt.dms_status < DMS_STATUS_IN || SS_IN_REFORM) {
            pg_usleep(SS_REFORM_WAIT_TIME);
            continue;
        }

        if (SS_NORMAL_PRIMARY) {
            MaintXminInPrimary();
            int rc = WaitLatch(&t_thrd.proc->procLatch, WL_TIMEOUT | WL_POSTMASTER_DEATH, DMS_AUXILIARY_PRIMARY_SLEEP_TIME);
            if (rc & WL_POSTMASTER_DEATH) {
                gs_thread_exit(1);
            }
        } else if (SS_NORMAL_STANDBY) {
            MaintXminInStandby();
            int rc = WaitLatch(&t_thrd.proc->procLatch, WL_TIMEOUT | WL_POSTMASTER_DEATH, DMS_AUXILIARY_STANDBY_SLEEP_TIME);
            if (rc & WL_POSTMASTER_DEATH) {
                gs_thread_exit(1);
            }
        }
    }
}

void SSWaitDmsAuxiliaryExit()
{
    while (g_instance.pid_cxt.DmsAuxiliaryPID != 0) {
        pg_usleep(1);
    }
    ereport(LOG, (errmsg("[SS] dms auxiliary thread exit")));
}