/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * exrto_recycle.cpp
 *
 * clean thread for standby read on block level page redo
 *
 * IDENTIFICATION
 *   src/gausskernel/storage/access/transam/extreme_rto/exrto_recycle.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/extreme_rto/page_redo.h"
#include "access/extreme_rto/dispatcher.h"
#include "access/extreme_rto/standby_read/lsn_info_meta.h"
#include "access/extreme_rto/standby_read.h"
#include "access/extreme_rto/standby_read/standby_read_delay_ddl.h"
#include "access/multi_redo_api.h"
#include "storage/ipc.h"
#include "storage/smgr/smgr.h"
#include "utils/memutils.h"

namespace extreme_rto {
static void exrto_recycle_sighup_handler(SIGNAL_ARGS)
{
    int save_errno = errno;
    t_thrd.exrto_recycle_cxt.got_SIGHUP = true;
    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);
    errno = save_errno;
}

static void exrto_recycle_shutdown_handler(SIGNAL_ARGS)
{
    int save_errno = errno;
    t_thrd.exrto_recycle_cxt.shutdown_requested = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }
    errno = save_errno;
}

static void exrto_recycle_quick_die(SIGNAL_ARGS)
{
    int status = 2;
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
    on_exit_reset();
    proc_exit(status);
}

static void exrto_recycle_setup_signal_handlers()
{
    (void)gspqsignal(SIGHUP, exrto_recycle_sighup_handler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, exrto_recycle_shutdown_handler);
    (void)gspqsignal(SIGQUIT, exrto_recycle_quick_die);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_IGN);
    (void)gspqsignal(SIGTTIN, SIG_IGN);
    (void)gspqsignal(SIGTTOU, SIG_IGN);
    (void)gspqsignal(SIGCONT, SIG_IGN);
    (void)gspqsignal(SIGWINCH, SIG_IGN);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
}

static void handle_exrto_recycle_shutdown()
{
    ereport(LOG, (errmsg("exrto recycle exit for request")));
    ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
    proc_exit(0);
}

static void exrto_recycle_wait()
{
    int rc = 0;
    rc = WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 1000L); /* 1s */
    /* Clear any already-pending wakeups */
    ResetLatch(&t_thrd.proc->procLatch);
    if (((unsigned int)rc) & WL_POSTMASTER_DEATH) {
        gs_thread_exit(1);
    }
}

bool check_if_need_force_recycle()
{
    uint32 worker_nums = g_dispatcher->allWorkersCnt;
    PageRedoWorker** workers = g_dispatcher->allWorkers;
    uint64 total_base_page_size = 0;
    uint64 total_lsn_info_size = 0;
    double ratio = g_instance.attr.attr_storage.standby_force_recycle_ratio;

    // if standby_force_recyle_ratio is 0, the system does not recyle file.
    if (ratio == 0 || worker_nums == 0) {
        return false;
    }

    BasePagePosition min_base_page_recyle_position = extreme_rto_standby_read::LSN_INFO_LIST_HEAD;
    BasePagePosition min_lsn_info_recyle_position = extreme_rto_standby_read::LSN_INFO_LIST_HEAD;
    BasePagePosition last_base_page_position = pg_atomic_read_u64(&extreme_rto::g_dispatcher->next_base_page);
    BasePagePosition last_lsn_info_position = pg_atomic_read_u64(&extreme_rto::g_dispatcher->next_lsn_info_page);

    for (uint32 i = 0; i < worker_nums; ++i) {
        PageRedoWorker* page_redo_worker = workers[i];
        StandbyReadMetaInfo meta_info = page_redo_worker->standby_read_meta_info;
        if (page_redo_worker->role != REDO_PAGE_WORKER || (page_redo_worker->isUndoSpaceWorker)) {
            continue;
        }
        min_base_page_recyle_position = rtl::min(min_base_page_recyle_position, meta_info.base_page_recyle_position);
        min_lsn_info_recyle_position = rtl::min(min_lsn_info_recyle_position, meta_info.lsn_table_recycle_position);
    }

    if (last_base_page_position > min_base_page_recyle_position) {
        total_base_page_size = last_base_page_position - min_base_page_recyle_position;
    }
    if (last_lsn_info_position > min_lsn_info_recyle_position) {
        total_lsn_info_size = last_lsn_info_position - min_lsn_info_recyle_position;
    }

    /* the unit of max_standby_base_page_size and max_standby_lsn_info_size is KB */
    uint64 max_standby_base_page_size = ((uint64)u_sess->attr.attr_storage.max_standby_base_page_size << 10);
    uint64 max_standby_lsn_info_size = ((uint64)u_sess->attr.attr_storage.max_standby_lsn_info_size << 10);
    if (total_base_page_size > max_standby_base_page_size * ratio ||
        total_lsn_info_size > max_standby_lsn_info_size * ratio) {
        return true;
    }

    return false;
}

void do_standby_read_recyle(XLogRecPtr recycle_lsn)
{
    uint32 worker_nums = g_dispatcher->allWorkersCnt;
    PageRedoWorker** workers = g_dispatcher->allWorkers;
    XLogRecPtr min_recycle_lsn = InvalidXLogRecPtr;
    XLogRecPtr before_min_lsn_position = UINT64_MAX;
    XLogRecPtr before_min_base_page_position = UINT64_MAX;
    XLogRecPtr after_min_lsn_position = UINT64_MAX;
    XLogRecPtr after_min_base_page_position = UINT64_MAX;
    for (uint32 i = 0; i < worker_nums; ++i) {
        PageRedoWorker* page_redo_worker = workers[i];
        if (page_redo_worker->role != REDO_PAGE_WORKER || (page_redo_worker->isUndoSpaceWorker)) {
            continue;
        }

        before_min_lsn_position = rtl::min(before_min_lsn_position,
                                           page_redo_worker->standby_read_meta_info.lsn_table_recycle_position);
        before_min_base_page_position = rtl::min(before_min_base_page_position,
                                                 page_redo_worker->standby_read_meta_info.base_page_recyle_position);

        extreme_rto_standby_read::standby_read_recyle_per_workers(&page_redo_worker->standby_read_meta_info, recycle_lsn);

        after_min_lsn_position = rtl::min(after_min_lsn_position,
                                          page_redo_worker->standby_read_meta_info.lsn_table_recycle_position);
        after_min_base_page_position = rtl::min(after_min_base_page_position,
                                                page_redo_worker->standby_read_meta_info.base_page_recyle_position);

        if (XLogRecPtrIsInvalid(min_recycle_lsn) ||
            XLByteLT(page_redo_worker->standby_read_meta_info.recycle_lsn_per_worker, min_recycle_lsn)) {
            min_recycle_lsn = page_redo_worker->standby_read_meta_info.recycle_lsn_per_worker;
        }
        pg_usleep(1000); // sleep 1ms
    }
    if (after_min_lsn_position / EXRTO_LSN_INFO_FILE_MAXSIZE >
        before_min_lsn_position / EXRTO_LSN_INFO_FILE_MAXSIZE) {
        g_dispatcher->global_recycle_lsn_info_page = after_min_lsn_position;
        extreme_rto_standby_read::recycle_lsn_info_file(after_min_lsn_position);
    }
    if (after_min_base_page_position / EXRTO_BASE_PAGE_FILE_MAXSIZE >
        before_min_base_page_position / EXRTO_BASE_PAGE_FILE_MAXSIZE) {
        extreme_rto_standby_read::recycle_base_page_file(after_min_base_page_position);
    }

    if (XLogRecPtrIsInvalid(min_recycle_lsn) && min_recycle_lsn > MAX_LSN_SIZE_PER_FORWARDER &&
        XLByteLT(g_instance.comm_cxt.predo_cxt.global_recycle_lsn, min_recycle_lsn - MAX_LSN_SIZE_PER_FORWARDER)) {
        pg_atomic_write_u64(&g_instance.comm_cxt.predo_cxt.global_recycle_lsn,
                            min_recycle_lsn - MAX_LSN_SIZE_PER_FORWARDER);
        ereport(LOG,
            (errmsg(EXRTOFORMAT("[exrto_recycle] update global recycle lsn: %08X/%08X"),
                    (uint32)(min_recycle_lsn >> UINT64_HALF), (uint32)(min_recycle_lsn - MAX_LSN_SIZE_PER_FORWARDER))));
    }
    delete_by_lsn(recycle_lsn);
}

void exrto_recycle_interrupt()
{
    if (t_thrd.exrto_recycle_cxt.got_SIGHUP) {
        t_thrd.exrto_recycle_cxt.got_SIGHUP = false;
        ProcessConfigFile(PGC_SIGHUP);
    }

    if (t_thrd.exrto_recycle_cxt.shutdown_requested) {
        handle_exrto_recycle_shutdown();
    }
}

void exrto_recycle_main()
{
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "exrto recycler",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    MemoryContext exrto_recycle_context = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "Exrto Recycler",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(exrto_recycle_context);

    ereport(LOG, (errmsg("exrto recycle started")));
    exrto_recycle_setup_signal_handlers();

    /*
     * Unblock signals (they were blocked when the postmaster forked us)
     */
    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    pgstat_report_appname("exrto recycler");
    pgstat_report_activity(STATE_IDLE, NULL);

    bool need_force_recyle = false;
    int sleep_count = 0;
    RegisterRedoInterruptCallBack(exrto_recycle_interrupt);

    if (pmState == PM_RUN && isDirExist(EXRTO_FILE_DIR)) {
        buffer_drop_exrto_standby_read_buffers();
        exrto_clean_dir();
    }
    if (isDirExist(EXRTO_OLD_FILE_DIR)) {
        exrto_recycle_old_dir();
        ereport(LOG, (errmsg("exrto recycle: clear standby_read_old dir success")));
    } else {
        ereport(LOG, (errmsg("exrto recycle: standby_read_old dir not exist")));
    }

    do_all_old_delay_ddl();
    if (!IS_EXRTO_READ || !RecoveryInProgress()) {
        ereport(LOG,
            (errmsg("exrto recycle is available only when exrto standby read is supported")));
        handle_exrto_recycle_shutdown();
    }
    while (true) {
        RedoInterruptCallBack();
        exrto_recycle_wait();
        ++sleep_count;

        /*
         * standby_recycle_interval = 0 means do not recyle
         */
        if (g_instance.attr.attr_storage.standby_recycle_interval == 0) {
            continue;
        }

        need_force_recyle = check_if_need_force_recycle();
        if (!need_force_recyle && sleep_count < g_instance.attr.attr_storage.standby_recycle_interval) {
            continue;
        }

        sleep_count = 0;

        XLogRecPtr recycle_lsn = exrto_calculate_recycle_position(need_force_recyle);
        if (XLogRecPtrIsInvalid(recycle_lsn)) {
            continue;
        }

        do_standby_read_recyle(recycle_lsn);
        smgrcloseall();
        MemoryContextResetAndDeleteChildren(exrto_recycle_context);
    }
    handle_exrto_recycle_shutdown();
}
}  /* namespace extreme_rto */