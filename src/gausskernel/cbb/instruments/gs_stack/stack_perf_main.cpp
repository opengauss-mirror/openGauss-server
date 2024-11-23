/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd. All rights reserved.
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
 * stack_perf_main.cpp
 *
 * IDENTIFICATION
 *        src/gaussdbkernel/cbb/instruments/stack_perf/stack_perf_main.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "c.h"
#include "funcapi.h"
#include "pgstat.h"
#include "catalog/pg_database.h"
#include "storage/ipc.h"
#include "utils/ps_status.h"
#include "utils/postinit.h"

void init_gspqsignal()
{
    /*
     * Ignore all signals usually bound to some action in the postmaster,
     * except SIGHUP and SIGQUIT.
     */
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, SIG_IGN);
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
}

NON_EXEC_STATIC void stack_perf_main()
{
    char username[NAMEDATALEN];
    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;
    t_thrd.role = STACK_PERF_WORKER;

    /* reset MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);
    t_thrd.proc_cxt.MyProgName = "stack_perf_main";
    u_sess->attr.attr_common.application_name = pstrdup("stack_perf_main");
    /* Identify myself via ps */
    init_ps_display("stack perf process", "", "", "");
    SetProcessingMode(InitProcessing);
    init_gspqsignal();
    /* Early initialization */
    BaseInit();
#ifndef EXEC_BACKEND
    InitProcess();
#endif
    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser((char*)pstrdup(DEFAULT_DATABASE), InvalidOid, username);
    t_thrd.proc_cxt.PostInit->InitStackPerfWorker();

    SetProcessingMode(NormalProcessing);
    on_shmem_exit(PGXCNodeCleanAndRelease, 0);
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "stack_perf",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX));
    elog(LOG, "stack perf started");

    /* report this backend in the PgBackendStatus array */
    u_sess->proc_cxt.MyProcPort->SessionStartTime = GetCurrentTimestamp();
    pgstat_report_appname("stack perf");
    pgstat_report_activity(STATE_RUNNING, NULL);
    if (g_instance.stat_cxt.print_stack_flag) {
        g_instance.stat_cxt.print_stack_flag = false;
        print_all_stack();
    } else {
        get_stack_and_write_result();
    }
    pgstat_report_activity(STATE_IDLE, NULL);
    proc_exit(0);
}
