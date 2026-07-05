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
 * aiocompleter.cpp
 *
 * The AIO completer threads complete Prefetch and BackWrite I/O so they
 * may only be stopped after all the worker threads or bgwrite threads have
 * been stopped.
 *
 * The aiocompleter threads complete  AIO requests using Linux Native AIO.
 * A single AIO completer thread serves on AIO queue associated with a
 * specific AIO context and I/O priority.
 *
 * The AIO completer threads are started by the postmaster as soon as the
 * startup subprocess finishes, or as soon as recovery begins if we are
 * doing archive recovery.  They remain alive until the postmaster commands
 * them to terminate.  Normal termination is by SIGTERM, which instructs the
 * threads to wait for any pending AIO and be prepared to exit.
 * via exit(0).  Emergency termination is by SIGQUIT.
 *
 * If completer thread exits unexpectedly, the postmaster treats that the same
 * as a backend crash: shared memory may be corrupted, so remaining backends
 * should be killed by SIGQUIT and then a recovery cycle started.
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/aiocompleter.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "gssignal/gs_signal.h"
#include "libpq/pqsignal.h"
#include "postmaster/aiocompleter.h"
#include "postmaster/postmaster.h"
#include "storage/smgr/fd.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include <pthread.h>

int CompltrReadReq(void* aioDesc, long res);
int CompltrWriteReq(void* aioDesc, long res);
int CompltrReadCUReq(void* aioDesc, long res);
int CompltrWriteCUReq(void* aioDesc, long res);

ThreadId Compltrfork_exec(int compltrIdx);

#define MAX_AIOCOMPLTR_THREADS 30

const int AioCompltrShutdownTimeout = 1;

AioCompltrDescT compltrDescArray[] = {
    {PREFETCH_TYPE, 2, -1, CompltrReadReq, 16384, 1, 16384, 60, HighPri},
    {FLUSH_TYPE, 2, -1, CompltrWriteReq, 16384, 1, 16384, 60, HighPri},
};

AioCompltrThreadT compltrArray[MAX_AIOCOMPLTR_THREADS];

bool volatile g_aioCompltrReady = false;

bool AioCompltrIsReady(void)
{
    return g_aioCompltrReady;
}

AioCallback ComptrCallback(AioCompltrType reqType)
{
    return compltrDescArray[reqType].callback;
}

short CompltrPriority(AioCompltrType reqType)
{
    return compltrDescArray[reqType].reqPrio;
}

io_context_t CompltrContext(AioCompltrType reqType, int fd)
{
    int idx = compltrDescArray[reqType].threadStartIdx + fd % compltrDescArray[reqType].threadNum;
    return compltrArray[idx].context;
}

/* Signal handlers */
static void CompltrConfig(SIGNAL_ARGS);
static void CompltrQuickDie(SIGNAL_ARGS);
static void CompltrShutdown(SIGNAL_ARGS);

ThreadId Compltrfork_exec(int compltrIdx)
{
    if (g_instance.pid_cxt.AioCompleterPID[compltrIdx] == 0) {
        g_instance.pid_cxt.AioCompleterPID[compltrIdx] =
            initialize_util_thread(AIO_COMPLETER_THREAD, (void *)(uintptr_t)(long)compltrIdx);
    }
    return g_instance.pid_cxt.AioCompleterPID[compltrIdx];
}

void AdioThreadNumInit()
{
    compltrDescArray[PREFETCH_TYPE].threadNum = g_instance.attr.attr_storage.adioReaderThreadNum;
    compltrDescArray[FLUSH_TYPE].threadNum = g_instance.attr.attr_storage.adioWriterThreadNum;
}

int AioCompltrNum()
{
    int num = 0;
    for (int i = 0; i < NUM_AIOCOMPLTR_TYPES; i++) {
        num += compltrDescArray[i].threadNum;
    }
    return num;
}

#define RETRY_TIMES (5)

/* AioCmpltrStart() failure stage codes reported via AioCompltrStartError() */
#define AIO_COMPLTR_ERR_THREAD_LIMIT (1)
#define AIO_COMPLTR_ERR_EVENT_MALLOC (2)
#define AIO_COMPLTR_ERR_FORK (3)

void AioCompltrStartError(int error)
{
    AioCompltrStop(SIGTERM);
    ereport(FATAL, (errmsg("AIO Startup Failed, error=%d", error)));
}

int AioCompltrIoSetup(int compltrIdx)
{
    int tryTimes = 0;
    int error = 0;
    do {
        error = io_setup(compltrArray[compltrIdx].compltrDescp->maxEvents, &compltrArray[compltrIdx].context);
        if (error == 0 || error != -EAGAIN) {
            break;
        }
        tryTimes++;
        pg_usleep(100000L);
    } while (tryTimes < RETRY_TIMES);
    return error;
}

void AioCmpltrStart(void)
{
    int error = 0;
    AdioThreadNumInit();
    int compltrNum = AioCompltrNum();
    int compltrTypeNum = sizeof(compltrDescArray) / sizeof(AioCompltrDescT);

    if (compltrNum > MAX_AIOCOMPLTR_THREADS) {
        AioCompltrStartError(AIO_COMPLTR_ERR_THREAD_LIMIT);
    }

    error = memset_s(&compltrArray, sizeof(AioCompltrThreadT) * MAX_AIOCOMPLTR_THREADS,
                          0, sizeof(AioCompltrThreadT) * MAX_AIOCOMPLTR_THREADS);
    securec_check(error, "\0", "\0");

    for (int compltrIdx = 0, i = 0; i < compltrTypeNum; i++) {
        compltrDescArray[i].threadStartIdx = compltrIdx;
        for (; compltrIdx < (compltrDescArray[i].threadStartIdx + compltrDescArray[i].threadNum); compltrIdx++) {
            compltrArray[compltrIdx].compltrDescp = &compltrDescArray[i];

            error = AioCompltrIoSetup(compltrIdx);
            if (error != 0) {
                ereport(WARNING, (errmsg("AIO io_setup failed: error %d", error)));
                AioCompltrStartError(error);
            }

            compltrArray[compltrIdx].eventsp =
                (io_event*)malloc(compltrArray[compltrIdx].compltrDescp->maxNr * sizeof(struct io_event));

            if (compltrArray[compltrIdx].eventsp == (struct io_event*)NULL) {
                ereport(WARNING,
                    (errmsg("AIO Startup malloc io_event failed: maxNr(%d), error(%d)",
                        compltrArray[compltrIdx].compltrDescp->maxNr, AIO_COMPLTR_ERR_EVENT_MALLOC)));
                AioCompltrStartError(AIO_COMPLTR_ERR_EVENT_MALLOC);
            }

            compltrArray[compltrIdx].tid = Compltrfork_exec(compltrIdx);
            if (compltrArray[compltrIdx].tid == ((ThreadId)0)) {
                ereport(WARNING, (errmsg("Start AIO Completer thread failed: error(%d)", AIO_COMPLTR_ERR_FORK)));
                AioCompltrStartError(AIO_COMPLTR_ERR_FORK);
            }
        };
    }
    g_aioCompltrReady = true;
}

void AioCompltrStop(int signal)
{
    gs_thread_t thread;
    g_aioCompltrReady = false;
    int aioCompltrNum = AioCompltrNum();

    for (int i = 0; i < aioCompltrNum; i++) {
        if (compltrArray[i].tid != 0) {
            if (gs_signal_send(compltrArray[i].tid, signal) < 0) {
                ereport(LOG, (errmsg("kill(%ld,%d) failed: %m", (long)(compltrArray[i].tid), signal)));
            }
        }
    }

    if (signal == SIGQUIT) {
        return;
    }

    for (int i = 0; i < aioCompltrNum; i++) {
        if (compltrArray[i].tid != 0) {
            thread.thid = compltrArray[i].tid;
            if (gs_thread_join(thread, NULL) != 0) {
                if (ESRCH == pthread_kill(thread.thid, 0))
                    ereport(LOG, (errmsg("failed to join thread %lu, no such process", thread.thid)));
                else
                    HandleChildCrash(thread.thid, 1, "AIO process");
            }
            compltrArray[i].tid = (pid_t)0;
        }
    }

    for (int i = 0; i < aioCompltrNum; i++) {
        compltrArray[i].compltrDescp = (AioCompltrDescT*)NULL;

        if (compltrArray[i].context) {
            io_destroy(compltrArray[i].context);
            compltrArray[i].context = (io_context_t)NULL;
        }

        if (compltrArray[i].eventsp) {
            free(compltrArray[i].eventsp);
            compltrArray[i].eventsp = (struct io_event*)NULL;
        }
    }

    return;
}

void AioCompltrMain(int compltrIdx)
{
    t_thrd.aio_cxt.compltrIdx = compltrIdx;
    Assert(compltrIdx >= 0 && compltrIdx < MAX_AIOCOMPLTR_THREADS);

    io_context_t context = compltrArray[compltrIdx].context;
    io_event* eventsp = compltrArray[compltrIdx].eventsp;
    AioCompltrDescT* compltrDescp = compltrArray[compltrIdx].compltrDescp;
    struct timespec timeout = {compltrDescp->timeout, 0};
    struct timespec shutdownTmeout = {AioCompltrShutdownTimeout, 0};
    AioCallback callback = compltrDescp->callback;

    (void)gspqsignal(SIGURG, print_stack);
    (void)gspqsignal(SIGHUP, CompltrConfig);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, CompltrShutdown);
    (void)gspqsignal(SIGQUIT, CompltrQuickDie);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);

    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    ereport(LOG, (errmsg("AIO Completer %d STARTED.", compltrIdx)));

    for (int eventsReceived;;) {
        if (t_thrd.aio_cxt.config_requested) {
            t_thrd.aio_cxt.config_requested = false;
        }

        if (t_thrd.aio_cxt.shutdown_requested) {
            timeout = shutdownTmeout;

            ereport(LOG, (errmsg("AIO Completer %d EXITED.", compltrIdx)));
            proc_exit(0);
        }

        eventsReceived = io_getevents(context, compltrDescp->minNr, compltrDescp->maxNr, eventsp, &timeout);
        if (eventsReceived == -EINTR) {
            continue;
        }

        if (eventsReceived < 0) {
            ereport(PANIC, (errmsg("AIO Completer io_getevents() failed: error %d .", eventsReceived)));
        }

        for (struct io_event* eventp = eventsp; eventsReceived--; eventp++) {
            callback((void*)eventp->obj, eventp->res);
        }
    }

    ereport(LOG, (errmsg("AIO Completer %d EXITED.", compltrIdx)));
    exit(0);
}

static void CompltrConfig(SIGNAL_ARGS)
{
    t_thrd.aio_cxt.config_requested = true;
}

static void CompltrQuickDie(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
    on_exit_reset();

    /*
     * Note we do exit(2) not exit(0).  This is to force the postmaster into a
     * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
     * backend.  This is necessary precisely because we don't clean up our
     * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
     * should ensure the postmaster sees this as a crash, too, but no harm in
     * being doubly sure.)
     */
    exit(2);
}

static void CompltrShutdown(SIGNAL_ARGS)
{
    t_thrd.aio_cxt.shutdown_requested = true;
}

void AioResourceInitialize(void)
{
    AdioSharedContext = AllocSetContextCreate((MemoryContext)g_instance.instance_context,
        "AdioSharedMemory",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);
}
