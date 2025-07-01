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
 * rack_mem_cleaner.cpp
 *
 * openGauss recyclebin cleaner thread Implementation
 *
 * IDENTIFICATION
 * src/gausskernel/process/postmaster/rack_mem_cleaner.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "utils/ps_status.h"
#include "storage/ipc.h"
#include "storage/rack_mem.h"
#include "utils/memutils.h"
#include "postmaster/rack_mem_cleaner.h"

static void RackMemShutdownHandler(SIGNAL_ARGS);
static bool RackMemServiceEnabled();
static bool CustomFree(void *ptr);
static void RackMemCleanSigSetup();
static void RackMemCleanerShutdown();
static void AppendFreeeBlocks(RackMemControlBlock *pending, size_t processed);
static RackMemControlBlock *GetBlockFreeQueue();

static void RackMemShutdownHandler(SIGNAL_ARGS)
{
    ereport(LOG, (errmsg("rack mem receive shutdown cmd")));
    g_instance.rackMemCleanerCxt.cleanupActive = false;
    pthread_cond_signal(&g_instance.rackMemCleanerCxt.cond);
}

static bool RackMemServiceEnabled(int *availBorrowMemSize)
{
    int availableMem;
    int ret = RackMemAvailable(&availableMem);
    if (ret != 0 || availableMem <= 0) {
        *availBorrowMemSize = 0;
        if (g_instance.attr.attr_memory.enable_borrow_memory) {
            ereport(
                WARNING,
                (errmsg(
                    "RackManager is not available, you may fix up RackManager or set enable_borrow_memory to off.")));
        }
        return false;
    }
    *availBorrowMemSize = availableMem;
    return true;
}

void RegisterFailedFreeMemory(void *ptr)
{
    if (!ptr) {
        return;
    }

    if (!g_instance.attr.attr_memory.enable_rack_memory_cleaner || !g_instance.rackMemCleanerCxt.cleanupActive) {
        return;
    }

    MemoryContext old = MemoryContextSwitchTo(g_instance.rackMemCleanerCxt.memoryContext);
    RackMemControlBlock *pblock = (RackMemControlBlock *)palloc0(sizeof(RackMemControlBlock));

    pblock->ptr = ptr;
    pblock->tryCount = 0;
    pblock->next = nullptr;

    uint64_t queueSizePrint = 0;
    pthread_mutex_lock(&g_instance.rackMemCleanerCxt.mutex);

    /* add to head */
    pblock->next = g_instance.rackMemCleanerCxt.queueHead;
    g_instance.rackMemCleanerCxt.queueHead = pblock;

    queueSizePrint = ++g_instance.rackMemCleanerCxt.queueSize + g_instance.rackMemCleanerCxt.countToProcess;
    g_instance.rackMemCleanerCxt.total++;

    pthread_cond_signal(&g_instance.rackMemCleanerCxt.cond);
    pthread_mutex_unlock(&g_instance.rackMemCleanerCxt.mutex);
    MemoryContextSwitchTo(old);

    ereport(LOG, (errmsg("add rack mem to free_queue, current free queue size: %lu", queueSizePrint)));
}

static bool CustomFree(void *ptr)
{
    int ret = RackMemFree(ptr);
    if (ret != 0) {
        return false;
    }

    return true;
}

void RackMemCleanerMain()
{
    ereport(LOG, (errmsg("rack mem cleaner thread started")));

    int availableMem;
    uint64 totalUsed = pg_atomic_read_u64(&rackUsedSize);
    uint64 availableMem_uint64;
    t_thrd.proc_cxt.MyProgName = "rackMemCleaner";
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    IsUnderPostmaster = true;
    t_thrd.role = RACK_MEM_FREE_THREAD;
    t_thrd.proc_cxt.MyStartTime = time(nullptr);
    init_ps_display("rack mem cleaner", "", "", "");

    g_instance.rackMemCleanerCxt.memoryContext =
        AllocSetContextCreate(t_thrd.top_mem_cxt, "RackMemCleaner", ALLOCSET_DEFAULT_INITSIZE,
                              ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(g_instance.rackMemCleanerCxt.memoryContext);
    g_instance.rackMemCleanerCxt.cleanupActive = true;

    RackMemCleanSigSetup();
    while (g_instance.rackMemCleanerCxt.cleanupActive || g_instance.rackMemCleanerCxt.queueSize > 0) {
        if (!RackMemServiceEnabled(&availableMem)) {
            pg_atomic_write_u32(&g_instance.rackMemCleanerCxt.rack_available, 0);
            pg_usleep(1000000L);
            continue;
        }

        if (g_instance.rackMemCleanerCxt.rack_available == 0) {
            pg_atomic_write_u32(&g_instance.rackMemCleanerCxt.rack_available, 1);
            ereport(WARNING, (errmsg("RackManager is available")));
        }

        totalUsed = pg_atomic_read_u64(&rackUsedSize);
        availableMem_uint64 = (uint64)availableMem;
#ifdef ENABLE_HTAP
        double percent = g_instance.attr.attr_memory.htap_borrow_mem_percent / 100;
        double preOccupy = (double)g_instance.attr.attr_memory.max_imcs_cache * percent;
#else
        double preOccupy = 0;
#endif
        if ((totalUsed / kilobytes + availableMem_uint64 * HIGH_PROCMEM_MARK * kilobytes / 100 + preOccupy) <
            MAX_RACK_MEMORY_LIMIT) {
            if ((totalUsed / kilobytes + availableMem_uint64 * HIGH_PROCMEM_MARK * kilobytes / 100 + preOccupy) <
                static_cast<uint64>(g_instance.attr.attr_memory.max_borrow_memory)) {
                g_instance.attr.attr_memory.max_borrow_memory = static_cast<int>(
                    totalUsed / kilobytes + availableMem_uint64 * HIGH_PROCMEM_MARK * kilobytes / 100 + preOccupy);
                elog(WARNING, "rack available memory is %d MB, adjust max_borrow_memory to %d kB", availableMem,
                     g_instance.attr.attr_memory.max_borrow_memory);
            }
        }

        RackMemControlBlock *current = GetBlockFreeQueue();
        RackMemControlBlock *pending = nullptr;
        size_t processed = 0;

        while (current) {
            if (!g_instance.rackMemCleanerCxt.cleanupActive) {
                break;
            }

            RackMemControlBlock *next = current->next;
            if (CustomFree(current->ptr)) {
                pfree(current);
                g_instance.rackMemCleanerCxt.freeCount++;
                g_instance.rackMemCleanerCxt.countToProcess--;
            } else {
                current->tryCount++;
                pending = current;
                break;
            }
            current = next;
        }
        
        if (pending) {
            AppendFreeeBlocks(pending, processed);
        }
    }
    RackMemCleanerShutdown();
}

static RackMemControlBlock *GetBlockFreeQueue()
{
    pthread_mutex_lock(&g_instance.rackMemCleanerCxt.mutex);
    if (!g_instance.rackMemCleanerCxt.queueHead && g_instance.rackMemCleanerCxt.cleanupActive) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        pthread_cond_timedwait(&g_instance.rackMemCleanerCxt.cond, &g_instance.rackMemCleanerCxt.mutex, &ts);
    }
    if (!g_instance.rackMemCleanerCxt.queueHead) {
        pthread_mutex_unlock(&g_instance.rackMemCleanerCxt.mutex);
        return nullptr;
    }
    RackMemControlBlock *current = g_instance.rackMemCleanerCxt.queueHead;
    g_instance.rackMemCleanerCxt.countToProcess = g_instance.rackMemCleanerCxt.queueSize;
    g_instance.rackMemCleanerCxt.queueHead = nullptr;
    g_instance.rackMemCleanerCxt.queueSize = 0;

    pthread_mutex_unlock(&g_instance.rackMemCleanerCxt.mutex);
    return current;
}

static void AppendFreeeBlocks(RackMemControlBlock *pending, size_t processed)
{
    pthread_mutex_lock(&g_instance.rackMemCleanerCxt.mutex);

    RackMemControlBlock **tail = &g_instance.rackMemCleanerCxt.queueHead;

    while (*tail) {
        tail = &(*tail)->next;
    }
    *tail = pending;
    g_instance.rackMemCleanerCxt.queueSize += (g_instance.rackMemCleanerCxt.countToProcess - processed);
    g_instance.rackMemCleanerCxt.countToProcess = 0;
    pthread_mutex_unlock(&g_instance.rackMemCleanerCxt.mutex);
}

static void RackMemCleanerShutdown()
{
    g_instance.pid_cxt.rackMemCleanerPID = 0;
    MemoryContextDelete(g_instance.rackMemCleanerCxt.memoryContext);
    ereport(LOG, (errmsg("rack mem thread end")));
    proc_exit(0);
}

static void RackMemCleanSigSetup()
{
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGINT, RackMemShutdownHandler);
    (void)gspqsignal(SIGTERM, RackMemShutdownHandler);
    (void)gspqsignal(SIGQUIT, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_IGN);
    (void)gspqsignal(SIGTTIN, SIG_IGN);
    (void)gspqsignal(SIGTTOU, SIG_IGN);
    (void)gspqsignal(SIGCONT, SIG_IGN);
    (void)gspqsignal(SIGWINCH, SIG_IGN);
    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGALRM, SIG_IGN);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, nullptr);
    (void)gs_signal_unblock_sigusr2();
}