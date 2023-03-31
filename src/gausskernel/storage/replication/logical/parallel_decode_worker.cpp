/* ---------------------------------------------------------------------------------------
 *
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
 * ---------------------------------------------------------------------------------------
 *
 * parallel_decode_worker.cpp
 *        This module is used for creating a reader, and several decoders
 *        in parallel decoding.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/logical/parallel_decode_worker.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <unistd.h>
#include <sys/stat.h>


#include "access/rewriteheap.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/xact.h"

#include "miscadmin.h"
#include "pgstat.h"

#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/slot.h"
#include "replication/snapbuild.h" /* just for SnapBuildSnapDecRefcount */
#include "replication/parallel_decode_worker.h"
#include "replication/parallel_decode.h"
#include "replication/logical_parse.h"

#include "replication/parallel_reorderbuffer.h"
#include "access/xlog_internal.h"

#include "storage/smgr/fd.h"
#include "storage/sinval.h"

#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/combocid.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/relfilenodemap.h"
#include "knl/knl_thread.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "storage/ipc.h"

#include "catalog/catalog.h"
#include "catalog/pg_namespace.h"
#include "lib/binaryheap.h"

static const uint32 OUTPUT_WAIT_COUNT = 0xF;
static const uint32 PRINT_ALL_WAIT_COUNT = 0x7FF;

/*
 * Parallel decoding kill a decoder.
 */
static void ParallelDecodeKill(int code, Datum arg)
{
    ParallelDecodeWorker *worker = (ParallelDecodeWorker*) arg;

    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    SpinLockAcquire(&(gDecodeCxt[worker->slotId].rwlock));
    gDecodeCxt[worker->slotId].ParallelDecodeWorkerStatusList[worker->id].threadState = PARALLEL_DECODE_WORKER_EXIT;
    g_Logicaldispatcher[worker->slotId].abnormal = true;
    SpinLockRelease(&(gDecodeCxt[worker->slotId].rwlock));
    LWLockReleaseAll();
}

/* Run from the worker thread. */
static void LogicalWorkerSigHupHandler(SIGNAL_ARGS)
{
    t_thrd.parallel_decode_cxt.got_SIGHUP = true;
}

static void LogicalWorkerShutdownHandler(SIGNAL_ARGS)
{
    if (t_thrd.logical_cxt.dispatchSlotId != -1) {
        g_Logicaldispatcher[t_thrd.logical_cxt.dispatchSlotId].abnormal = true;
    }
}

static void LogicalWorkerQuickDie(SIGNAL_ARGS)
{
    int status = 2;
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
    on_exit_reset();
    proc_exit(status);
}

/*
 * Run from the worker thread, set up signal handlers for logical reader/decoder.
 */
static void SetupLogicalWorkerSignalHandlers()
{
    (void)gspqsignal(SIGHUP, LogicalWorkerSigHupHandler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGQUIT, LogicalWorkerQuickDie);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_IGN);
    (void)gspqsignal(SIGTTIN, SIG_IGN);
    (void)gspqsignal(SIGTTOU, SIG_IGN);
    (void)gspqsignal(SIGCONT, SIG_IGN);
    (void)gspqsignal(SIGWINCH, SIG_IGN);

    (void)gspqsignal(SIGTERM, LogicalWorkerShutdownHandler);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
}

bool IsLogicalWorkerShutdownRequested()
{
    return (t_thrd.logical_cxt.dispatchSlotId != -1 &&
        g_Logicaldispatcher[t_thrd.logical_cxt.dispatchSlotId].abnormal);
}

/*
 * Parallel decoding kill a reader.
 */
static void LogicalReadKill(int code, Datum arg)
{
    ereport(LOG, (errmsg("LogicalReader process shutdown.")));

    /* Make sure active replication slots are released */
    CleanMyReplicationSlot();

    int slotId = DatumGetInt32(arg);
    SpinLockAcquire(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    gDecodeCxt[slotId].ParallelReaderWorkerStatus.threadState = PARALLEL_DECODE_WORKER_EXIT;
    g_Logicaldispatcher[slotId].abnormal = true;
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
}

static void SetDecodeReaderThreadId(int slotId, ThreadId threadId)
{
    knl_g_parallel_decode_context *pdecode_cxt = &g_instance.comm_cxt.pdecode_cxt[slotId];

    SpinLockAcquire(&pdecode_cxt->rwlock);
    pdecode_cxt->ParallelReaderWorkerStatus.threadId = threadId;
    SpinLockRelease(&pdecode_cxt->rwlock);
}

static void SetDecodeWorkerThreadId(int slotId,  int workId, ThreadId threadId)
{
    knl_g_parallel_decode_context *pdecode_cxt = &g_instance.comm_cxt.pdecode_cxt[slotId];

    SpinLockAcquire(&pdecode_cxt->rwlock);
    pdecode_cxt->ParallelDecodeWorkerStatusList[workId].threadId = threadId;
    SpinLockRelease(&pdecode_cxt->rwlock);
}

static void SetDecodeReaderThreadState(int slotId, int state)
{
    knl_g_parallel_decode_context *pdecode_cxt = &g_instance.comm_cxt.pdecode_cxt[slotId];

    SpinLockAcquire(&pdecode_cxt->rwlock);
    pdecode_cxt->ParallelReaderWorkerStatus.threadState = state;
    SpinLockRelease(&pdecode_cxt->rwlock);
}

static void SetDecodeWorkerThreadState(int slotId, int workId, int state)
{
    knl_g_parallel_decode_context *pdecode_cxt = &g_instance.comm_cxt.pdecode_cxt[slotId];

    SpinLockAcquire(&pdecode_cxt->rwlock);
    pdecode_cxt->ParallelDecodeWorkerStatusList[workId].threadState = state;
    SpinLockRelease(&pdecode_cxt->rwlock);
}


/*
 * Parallel decoding release resource.
 */
void ReleaseParallelDecodeResource(int slotId)
{
    knl_g_parallel_decode_context *pDecodeCxt = &g_instance.comm_cxt.pdecode_cxt[slotId];
    MemoryContext decode_cxt = pDecodeCxt->parallelDecodeCtx;
    MemoryContext llog_cxt = pDecodeCxt->logicalLogCtx;

    SpinLockAcquire(&pDecodeCxt->rwlock);
    pDecodeCxt->parallelDecodeCtx = NULL;
    pDecodeCxt->logicalLogCtx = NULL;
    g_Logicaldispatcher[slotId].active = false;
    g_Logicaldispatcher[slotId].abnormal = false;
    SpinLockRelease(&pDecodeCxt->rwlock);

    if (decode_cxt != NULL) {
        MemoryContextDelete(decode_cxt);
    }
    if (llog_cxt != NULL) {
        MemoryContextDelete(llog_cxt);
    }

    ereport(LOG, (errmsg("g_Logicaldispatcher[%d].active = false", slotId)));
}

/*
 * Start a decoder thread.
 */
ThreadId StartDecodeWorkerThread(ParallelDecodeWorker *worker)
{
    worker->tid.thid = initialize_util_thread(PARALLEL_DECODE, worker);
    return worker->tid.thid;
}

/*
 * Start the reader thread.
 */
ThreadId StartLogicalReadWorkerThread(ParallelDecodeReaderWorker *worker)
{
    worker->tid = initialize_util_thread(LOGICAL_READ_RECORD, worker);
    return worker->tid;
}

/*
 * Parallel decoding start a decode worker.
 */
ParallelDecodeWorker* StartLogicalDecodeWorker(int id, int slotId, char* dbUser, char* dbName, char* slotname)
{
    ParallelDecodeWorker *worker = CreateLogicalDecodeWorker(id, dbUser, dbName, slotname, slotId);
    worker->slotId = slotId;

    ThreadId threadId = StartDecodeWorkerThread(worker);
    if (threadId == 0) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
            errmsg("Create parallel logical decoder thread failed"), errdetail("id = %u, slotId = %u, %m", id, slotId),
            errcause("System error."), erraction("Retry it in a few minutes.")));
        return NULL;
    }

    SetDecodeWorkerThreadId(slotId, id, threadId);
    ereport(LOG, (errmsg("StartParallelDecodeWorker successfully create logical decoder id: %d, threadId:%lu.",
        id, worker->tid.thid)));

    return worker;
}

/*
 * Parallel decoding start all decode workers.
 */
void StartLogicalDecodeWorkers(int parallelism, int slotId, char* dbUser, char* dbName, char* slotname)
{
    MemoryContext oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
    g_Logicaldispatcher[slotId].decodeWorkers = (ParallelDecodeWorker **)palloc0(sizeof(ParallelDecodeWorker *)
        * parallelism);
    MemoryContextSwitchTo(oldCtx);

    /*
     * This is necessary to avoid the cache coherence problem.
     * Because we are using atomic operation to do the synchronization.
     */
    int started = 0;
    for (; started < parallelism; started++) {
        g_Logicaldispatcher[slotId].decodeWorkers[started] =
            StartLogicalDecodeWorker(started, slotId, dbUser, dbName, slotname);
        if (g_Logicaldispatcher[slotId].decodeWorkers[started] == NULL) {
            break;
        }
    }

    if (started == 0) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
            errmsg("Start logical decode workers failed."),
            errdetail("We need at least one worker thread, slotId = %u", slotId),
            errcause("System error."), erraction("Retry it in a few minutes.")));
    }
    g_Logicaldispatcher[slotId].totalWorkerCount = started;
    g_instance.comm_cxt.pdecode_cxt[slotId].totalNum = started;

    g_Logicaldispatcher[slotId].curChangeNum = 0;
    g_Logicaldispatcher[slotId].curTupleNum = 0;
    g_Logicaldispatcher[slotId].curLogNum = 0;
    oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
    g_Logicaldispatcher[slotId].chosedWorkerIds = (uint32 *)palloc0(sizeof(uint32) * started);
    MemoryContextSwitchTo(oldCtx);

    g_Logicaldispatcher[slotId].chosedWorkerCount = 0;
}

/*
 * Parallel decoding create the read worker.
 */
ParallelDecodeReaderWorker *CreateLogicalReadWorker(int slotId, char* dbUser, char* dbName, char* slotname,
    List *options)
{
    errno_t rc;
    MemoryContext oldCtx = NULL;
    oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
    ParallelDecodeReaderWorker *worker = (ParallelDecodeReaderWorker *)palloc0(sizeof(ParallelDecodeReaderWorker));
    MemoryContextSwitchTo(oldCtx);

    worker->slotId = slotId;
    worker->tid = InvalidTid;

    worker->queue = LogicalQueueCreate(slotId);
    rc = memcpy_s(worker->slotname, NAMEDATALEN, slotname, strlen(slotname));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(worker->dbUser, NAMEDATALEN, dbUser, strlen(dbUser));
    securec_check(rc, "\0", "\0");
    
    rc = memcpy_s(worker->dbName, NAMEDATALEN, dbName, strlen(dbName));
    securec_check(rc, "\0", "\0");

    SetDecodeReaderThreadState(slotId, PARALLEL_DECODE_WORKER_INIT);
    SpinLockInit(&(worker->rwlock));
    return worker;
}

/*
 * Parallel decoding start the read worker.
 */
ThreadId StartDecodeReadWorker(ParallelDecodeReaderWorker *worker)
{
    ThreadId threadId = StartLogicalReadWorkerThread(worker);
    int slotId = worker->slotId;
    if (threadId == 0) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG), errmsg("Cannot create readworker thread"),
            errdetail("N/A"), errcause("System error."), erraction("Retry it in a few minutes.")));

        return threadId;
    }

    SetDecodeReaderThreadId(slotId, threadId);
    ereport(LOG, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
        errmsg("StartDecodeReadWorker successfully create decodeReaderWorker id:%u,threadId:%lu",
            worker->id, threadId)));

    return threadId;
}

/*
 * Check whether there are decoding threads that have not exited.
 */
void CheckAliveDecodeWorkers(uint32 slotId)
{
    int state = -1;

    /*
     * Check reader thread state.
     */
    SpinLockAcquire(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
    state = g_instance.comm_cxt.pdecode_cxt[slotId].ParallelReaderWorkerStatus.threadState;
    SpinLockRelease(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
    if (state != PARALLEL_DECODE_WORKER_INVALID && state != PARALLEL_DECODE_WORKER_EXIT) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
            errmsg("Check alive reader worker failed, state is %d", state),
            errdetail("Logical reader thread %lu is still alive",
            g_instance.comm_cxt.pdecode_cxt[slotId].ParallelReaderWorkerStatus.threadId),
            errcause("Previous reader thread exits too slow"),
            erraction("Make sure all previous worker threads have exited.")));
    }

    g_instance.comm_cxt.pdecode_cxt[slotId].ParallelReaderWorkerStatus.threadId = 0;

    /*
     * Check decoder thread state.
     */
    for (uint32 i = 0; i < MAX_PARALLEL_DECODE_NUM; ++i) {
        SpinLockAcquire(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
        state = g_instance.comm_cxt.pdecode_cxt[slotId].ParallelDecodeWorkerStatusList[i].threadState;
        SpinLockRelease(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
        if (state != PARALLEL_DECODE_WORKER_INVALID && state != PARALLEL_DECODE_WORKER_EXIT) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
                errmsg("Check alive decoder workers failed, state is %d", state),
                errdetail("Logical decoder thread %lu is still alive",
                    g_instance.comm_cxt.pdecode_cxt[slotId].ParallelDecodeWorkerStatusList[i].threadId),
                errcause("Previous decoder thread exits too slow"),
                erraction("Make sure all previous worker threads have exited.")));
        }
        g_instance.comm_cxt.pdecode_cxt[slotId].ParallelDecodeWorkerStatusList[i].threadId = 0;
    }
    g_instance.comm_cxt.pdecode_cxt[slotId].totalNum = 0;
}

/*
 * Parallel decoding get paralle decode num.
 */
int GetDecodeParallelism(int slotId)
{
    return g_Logicaldispatcher[slotId].pOptions.parallel_decode_num;
}

int GetParallelQueueSize(int slotId)
{
    return g_Logicaldispatcher[slotId].pOptions.parallel_queue_size;
}

/*
 * Parallel decoding init logical dispatcher, which is used for recording all worker states.
 */
void InitLogicalDispatcher(LogicalDispatcher *dispatcher)
{
    dispatcher->totalCostTime = 0;
    dispatcher->txnCostTime = 0;
    dispatcher->pprCostTime = 0;
    dispatcher->active = false;
    dispatcher->decodeWorkerId = 0;
    dispatcher->num = 0;
    dispatcher->firstLoop = true;
    dispatcher->freeGetTupleHead = NULL;
    dispatcher->freeTupleHead = NULL;
    dispatcher->freeChangeHead = NULL;
    dispatcher->freeLogicalLogHead = NULL;
}

/*
 * Get the number of ready decoders.
 */
int GetReadyDecodeWorker(int slotId)
{
    int readyWorkerCnt = 0;
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
    for (int i = 0; i < gDecodeCxt[slotId].totalNum; i++) {
        int state = gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[i].threadState;
        if (state == PARALLEL_DECODE_WORKER_RUN) {
            ++readyWorkerCnt;
        }
    }
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
    return readyWorkerCnt;
}

/*
 * Get the number of ready reader.
 */
int GetReadyDecodeReader(int slotId)
{
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
    int state = gDecodeCxt[slotId].ParallelReaderWorkerStatus.threadState;
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
    if (state == PARALLEL_DECODE_WORKER_RUN) {
        return 1;
    }
    return 0;
}

/*
 * Handle emitted error when checking decode worker status.
 */
static void DecodeWorkerErrorEmitter(int slotId)
{
    knl_g_parallel_decode_context *gDecodeCxt = &g_instance.comm_cxt.pdecode_cxt[slotId];
    if (g_Logicaldispatcher[slotId].abnormal) {
        if (gDecodeCxt->edata != NULL) {
            ReThrowError(gDecodeCxt->edata);
        } else {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
                errmsg("Wait for worker ready failed. Maybe resource is unavaliable now."),
                errdetail("At least one decode worker thread is abnormal to startup."),
                errcause("System error."), erraction("Retry it in a few minutes.")));
        }
    }
}

/*
 * Wait until all decoders are ready.
 */
void WaitDecoderReady(int slotId)
{
    uint32 loop = 0;
    int readyDecoder = 0;
    const uint32 maxCheckLoops = 6000;
    knl_g_parallel_decode_context *gDecodeCxt = &g_instance.comm_cxt.pdecode_cxt[slotId];

    for (; loop < maxCheckLoops; ++loop) {
        /* This connection exit if logical thread abnormal */
        DecodeWorkerErrorEmitter(slotId);

        readyDecoder = GetReadyDecodeWorker(slotId);
        if (readyDecoder == gDecodeCxt->totalNum) {
            ereport(LOG, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
                errmsg("WaitDecoderReady total decoder count:%d", readyDecoder)));
            break;
        }
        const long sleepTime = 1000;
        pg_usleep(sleepTime);
    }

    readyDecoder = GetReadyDecodeWorker(slotId);
    int totalDecoders = gDecodeCxt->totalNum;
    if (loop == maxCheckLoops && readyDecoder != totalDecoders) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG), errmsg("Wait for worker ready failed"),
            errdetail("Not all decoders are ready for work, totalDecoderCount: %d",
                readyDecoder), errcause("System error."), erraction("Retry it in a few minutes.")));
    }
}

/*
 * Wait until all workers are ready.
 */
void WaitWorkerReady(int slotId)
{
    uint32 loop = 0;
    int readyDecoder = 0;
    int readyReader = 0;
    const uint32 maxCheckLoops = 6000;
    knl_g_parallel_decode_context *gDecodeCxt = &g_instance.comm_cxt.pdecode_cxt[slotId];

    for (; loop < maxCheckLoops; ++loop) {
        /* This connection exit if logical thread abnormal */
        DecodeWorkerErrorEmitter(slotId);

        readyDecoder = GetReadyDecodeWorker(slotId);
        readyReader = GetReadyDecodeReader(slotId);
        if ((readyDecoder == gDecodeCxt->totalNum) && (readyReader == 1)) {
            ereport(LOG, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
                errmsg("WaitWorkerReady total worker count:%d, readyWorkerCnt:%d",
                    g_Logicaldispatcher[slotId].totalWorkerCount, readyDecoder)));
            break;
        }
        const long sleepTime = 1000;
        pg_usleep(sleepTime);
    }

    SpinLockAcquire(&gDecodeCxt->rwlock);
    gDecodeCxt->state = DECODE_STARTING_END;
    SpinLockRelease(&gDecodeCxt->rwlock);

    readyDecoder = GetReadyDecodeWorker(slotId);
    readyReader = GetReadyDecodeReader(slotId);
    int totalDecoders = gDecodeCxt->totalNum;
    if (loop == maxCheckLoops &&
        (readyDecoder != totalDecoders || readyReader != 1)) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG), errmsg("Wait for worker ready failed"),
            errdetail("Not all workers or no reader are ready for work, totalWorkerCount: %d",
                g_Logicaldispatcher[slotId].totalWorkerCount),
            errcause("System error."), erraction("Retry it in a few minutes.")));
    }

    ereport(LOG, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
        errmsg("WaitWorkerReady total worker count:%u, readyWorkerCnt:%d",
            g_Logicaldispatcher[slotId].totalWorkerCount, readyDecoder)));
}

/* Check if logicalReader and all logicalDecoder threads is PARALLEL_DECODE_WORKER_EXIT. */
bool logicalWorkerCouldExit(int slotId, ThreadId* tid, int* stateptr, bool* isReader)
{
    int state = -1;
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
    state = gDecodeCxt[slotId].ParallelReaderWorkerStatus.threadState;
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
    if (state != PARALLEL_DECODE_WORKER_EXIT && state != PARALLEL_DECODE_WORKER_INVALID) {
        *tid = gDecodeCxt[slotId].ParallelReaderWorkerStatus.threadId;
        *stateptr = state;
        *isReader = true;
        return false;
    }

    for (int i = 0; i < gDecodeCxt[slotId].totalNum; ++i) {
        SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
        state = gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[i].threadState;
        SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
        if (state != PARALLEL_DECODE_WORKER_EXIT && state != PARALLEL_DECODE_WORKER_INVALID) {
            *tid = gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[i].threadId;
            *stateptr = state;
            *isReader = false;
            return false;
        }
    }

    return true;
}

/*
 * Stop all decode workers.
 */
void StopParallelDecodeWorkers(int code, Datum arg)
{
    int slotId = DatumGetInt32(arg);
    knl_g_parallel_decode_context *gDecodeCxt = &g_instance.comm_cxt.pdecode_cxt[slotId];

    /* Notify all logical parallel worker (reader, decoder) to shutdown */
    SpinLockAcquire(&gDecodeCxt->rwlock);
    g_Logicaldispatcher[slotId].abnormal = true;
    SpinLockRelease(&gDecodeCxt->rwlock);

    uint32 count = 0;
    ThreadId tid = 0;
    int state = 0;
    bool isReader = false;
    while (!logicalWorkerCouldExit(slotId, &tid, &state, &isReader)) {
        ++count;
        if ((count & OUTPUT_WAIT_COUNT) == OUTPUT_WAIT_COUNT) {
            ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
                errmsg("StopDecodeWorkers wait reader and decoder exit, tid = %lu, state = %d, isReader = %d",
                tid, state, isReader)));
            if ((count & PRINT_ALL_WAIT_COUNT) == PRINT_ALL_WAIT_COUNT) {
                ereport(PANIC, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
                    errmsg("StopDecodeWorkers wait too long!!!"), errdetail("N/A"), errcause("System error."),
                    erraction("Retry it in a few minutes.")));
            }
        }
        const long sleepCheckTime = 1000000L;
        pg_usleep(sleepCheckTime);
    }

    SpinLockAcquire(&gDecodeCxt->rwlock);
    gDecodeCxt->ParallelReaderWorkerStatus.threadState = PARALLEL_DECODE_WORKER_INVALID;
    for (int i = 0; i < gDecodeCxt->totalNum; ++i) {
        gDecodeCxt->ParallelDecodeWorkerStatusList[i].threadState = PARALLEL_DECODE_WORKER_INVALID;
    }
    gDecodeCxt->state = DECODE_DONE;
    if (gDecodeCxt->edata != NULL) {
        FreeErrorData(gDecodeCxt->edata);
        gDecodeCxt->edata = NULL;
    }
    SpinLockRelease(&gDecodeCxt->rwlock);

    /* When parallel decoding is stopped, detach the logical slot */
    t_thrd.slot_cxt.MyReplicationSlot = NULL;
    ReleaseParallelDecodeResource(slotId);

    ereport(LOG, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
        errmsg("parallel decode thread exit in startup")));
}

/*
 * Get init a logicalDispatcher slot.
 * The return value of the function is logicalDispatcher slot ID.
 */
int GetLogicalDispatcher()
{
    int slotId = -1;
    const int maxReaderNum = 20;
    int maxDispatcherNum = Min(g_instance.attr.attr_storage.max_replication_slots, maxReaderNum);
    LWLockAcquire(ParallelDecodeLock, LW_EXCLUSIVE);
    for (int i = 0; i < maxDispatcherNum; i++) {
        if (g_Logicaldispatcher[i].active == false) {
            slotId = i;
            errno_t rc =
                memset_s(&g_Logicaldispatcher[slotId], sizeof(LogicalDispatcher), 0, sizeof(LogicalDispatcher));
            securec_check(rc, "", "");
            InitLogicalDispatcher(&g_Logicaldispatcher[slotId]);
            g_Logicaldispatcher[i].active = true;
            break;
        }
    }
    LWLockRelease(ParallelDecodeLock);
    if(slotId == -1) {
        return slotId;
    }
    ereport(LOG, (errmsg("g_Logicaldispatcher[%d].active = true", slotId)));

    MemoryContext ctx = AllocSetContextCreate(g_instance.instance_context, "ParallelDecodeDispatcher",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    MemoryContext logctx = AllocSetContextCreate(g_instance.instance_context, "ParallelDecodeLog",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;

    g_Logicaldispatcher[slotId].abnormal = false;
    gDecodeCxt[slotId].parallelDecodeCtx = ctx;
    gDecodeCxt[slotId].logicalLogCtx = logctx;

    SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
    int state = gDecodeCxt[slotId].state;
    if (state == DECODE_IN_PROGRESS) {
        SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG), errmsg("Get logical dispatcher failed"),
            errdetail("walsender reconnected thread exit."), errcause("System error."),
            erraction("Retry it in a few minutes.")));
    }
    gDecodeCxt[slotId].state = DECODE_STARTING_BEGIN;
    gDecodeCxt[slotId].edata = NULL;
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
    return slotId;
}

/*
 * Check whether a table is in the table white list.
 */
bool CheckWhiteList(const List *whiteList, const char *schema, const char *table)
{
    /* If the white list is empty, we do not need to filter tables. */
    if (list_length(whiteList) <= 0) {
        return true;
    }
    ListCell *lc = NULL;
    foreach(lc, whiteList) {
        chosenTable *cTable = (chosenTable *)lfirst(lc);

        if ((cTable->schema == NULL || strcmp(cTable->schema, schema) == 0) &&
            (cTable->table == NULL || strcmp(cTable->table, table) == 0)) {
            return true;
        }
    }
    ereport(DEBUG1, (errmodule(MOD_LOGICAL_DECODE),
        errmsg("logical change record of table %s.%s is filtered by white-table-list", schema, table)));
    return false;
}

/*
 * Parse a list of raw table names into a list of schema and table names.
 */
static bool ParseSchemaAndTableName(List *tableList, List **tableWhiteList)
{
    ListCell *table_cell = NULL;
    char *curPos = NULL;
    chosenTable *cTable = NULL;

    foreach(table_cell, tableList) {
        bool anySchema = false;
        bool anyTable = false;
        char *head = (char*)lfirst(table_cell);
        cTable = (chosenTable *)palloc0(sizeof(chosenTable));

        if (*head == '*' && *(head + 1) == '.') {
            cTable->schema = NULL;
            anySchema = true;
        }
        curPos = head;
        while (*curPos != '\0' && *curPos != '.') {
            curPos++;
        }
        size_t schema_len = (size_t)(curPos - head);

        if (*curPos == '\0') {
            pfree(cTable);
            return false;
        } else {
            if (!anySchema) {
                cTable->schema = (char *)palloc0((schema_len + 1) * sizeof(char));
                errno_t rc = strncpy_s(cTable->schema, schema_len + 1, head, schema_len);
                securec_check(rc, "", "");
            }

            curPos++;
            head = curPos;

            if (*head == '*' && *(head + 1) == '\0') {
                cTable->table = NULL;
                anyTable = true;
            }
            while (*curPos != '\0') {
                curPos++;
            }
            size_t table_len = (size_t)(curPos - head);

            if (!anyTable) {
                cTable->table = (char *)palloc0((table_len + 1) * sizeof(char));
                errno_t rc = strncpy_s(cTable->table, table_len + 1, head, table_len);
                securec_check(rc, "", "");
            }
        }
        *tableWhiteList = lappend(*tableWhiteList, cTable);
    }
    return true;
}

/*
 * Skip leading spaces.
 */
inline void SkipSpaceForString(char **str)
{
    while (isspace(**str)) {
        (*str)++;
    }
}

/*
 * Parse a raw string to a list of table names.
 */
bool ParseStringToWhiteList(char *tableString, List **tableWhiteList)
{
    char *curPos = tableString;
    SkipSpaceForString(&curPos);
    if (*curPos == '\0') {
        return true;
    }

    bool finished = false;
    List *tableList = NIL;
    do {
        char* tmpName = curPos;
        while (*curPos != '\0' && *curPos != ',' && !isspace(*curPos)) {
            curPos++;
        }
        if (tmpName == curPos) {
            list_free_deep(tableList);
            return false;
        }
        char *tmpEnd = curPos;
        SkipSpaceForString(&curPos);
        if (*curPos == '\0') {
            finished = true;
        } else if (*curPos == ',') {
            curPos++;
            SkipSpaceForString(&curPos);
        } else {
            list_free_deep(tableList);
            return false;
        }
        *tmpEnd = '\0';
        char *tableName = pstrdup(tmpName);
        tableList = lappend(tableList, tableName);
    } while (!finished);

    bool parseSuccess = ParseSchemaAndTableName(tableList, tableWhiteList);
    list_free_deep(tableList);
    return parseSuccess;
}

/*
 * Check decode_style, 't' for text and 'j' for json.
 */
static inline void CheckDecodeStyle(ParallelDecodeOption *data, DefElem* elem)
{
    if (elem->arg != NULL) {
        data->decode_style = *strVal(elem->arg);
        if (data->decode_style != 'j' && data->decode_style != 't' && data->decode_style != 'b') {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("could not parse value \"%s\" for parameter \"%s\"", strVal(elem->arg), elem->defname),
                errdetail("N/A"),  errcause("Wrong input value"), erraction("Input \'j\' or \'t\'")));
        }
        if (data->decode_style == 'j') {
            data->decode_change = parallel_decode_change_to_json;
        } else if (data->decode_style == 't') {
            data->decode_change = parallel_decode_change_to_text;
        } else {
            data->decode_change = parallel_decode_change_to_bin;
        }
    }
}

/*
 * Parse white list for logical decoding.
 */
void ParseWhiteList(List **whiteList, DefElem* elem)
{
    list_free_deep(*whiteList);
    *whiteList = NIL;
    if (elem->arg != NULL) {
        char *tableString = pstrdup(strVal(elem->arg));
        if (!ParseStringToWhiteList(tableString, whiteList)) {
            pfree(tableString);
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("could not parse value \"%s\" for parameter \"%s\"", strVal(elem->arg), elem->defname),
                errdetail("N/A"),  errcause("Wrong input value"), erraction("Use \' to seperate table names.")));
        }
        pfree(tableString);
    }
}

/*
 * Parse parallel decode num for logical decoding
 */
void ParseParallelDecodeNum(const DefElem *elem, int * const parallelDecodeNum)
{
    if (strncmp(elem->defname, "parallel-decode-num", sizeof("parallel-decode-num")) == 0) {
        if (elem->arg != NULL && (!parse_int(strVal(elem->arg), parallelDecodeNum, 0, NULL) ||
            *parallelDecodeNum <= 0 || *parallelDecodeNum > MAX_PARALLEL_DECODE_NUM)) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("could not parse value \"%s\" for parameter \"%s\"", strVal(elem->arg), elem->defname),
                errdetail("N/A"),  errcause("Wrong input value"), erraction("Input a number between 1 and 20.")));
        }
    }
}

int ParseParallelDecodeNumOnly(List *options)
{
    ListCell *option = NULL;
    int parallelDecodeNum = 1;

    DecodeOptionsDefault *defaultOption = LogicalDecodeGetOptionsDefault();
    if (defaultOption != NULL) {
        parallelDecodeNum = defaultOption->parallel_decode_num;
    }

    foreach (option, options) {
        DefElem* elem = (DefElem*)lfirst(option);
        ParseParallelDecodeNum(elem, &parallelDecodeNum);
    }
    return parallelDecodeNum;
}

/*
 * Parse a single logical decoding option.
 */
static void ParseDecodingOption(ParallelDecodeOption *data, ListCell *option)
{
    DefElem* elem = (DefElem*)lfirst(option);
    const int maxTxn = 100; /* max transaction in memory limit is between 0 and 100 in MB */
    const int maxReorderBuffer = 100; /* max reorderbuffer in memory is between 0 and 100 in GB */

    if (strncmp(elem->defname, "include-xids", sizeof("include-xids")) == 0) {
        CheckBooleanOption(elem, &data->include_xids, true);
    } else if (strncmp(elem->defname, "include-timestamp", sizeof("include-timestamp")) == 0) {
        CheckBooleanOption(elem, &data->include_timestamp, false);
    } else if (strncmp(elem->defname, "skip-empty-xacts", sizeof("skip-empty-xacts")) == 0) {
        CheckBooleanOption(elem, &data->skip_empty_xacts, false);
    } else if (strncmp(elem->defname, "only-local", sizeof("only-local")) == 0) {
        CheckBooleanOption(elem, &data->only_local, true);
    } else if (strncmp(elem->defname, "standby-connection", sizeof("standby-connection")) == 0) {
        CheckBooleanOption(elem, &t_thrd.walsender_cxt.standbyConnection, false);
    } else if (strncmp(elem->defname, "sending-batch", sizeof("sending-batch")) == 0) {
        CheckIntOption(elem, &data->sending_batch, 0, 0, 1);
    } else if (strncmp(elem->defname, "decode-style", sizeof("decode-style")) == 0) {
        CheckDecodeStyle(data, elem);
    } else if (strncmp(elem->defname, "max-txn-in-memory", sizeof("max-txn-in-memory")) == 0) {
        CheckIntOption(elem, &data->max_txn_in_memory, 0, 0, maxTxn);
    } else if (strncmp(elem->defname, "max-reorderbuffer-in-memory", sizeof("max-reorderbuffer-in-memory")) == 0) {
        CheckIntOption(elem, &data->max_reorderbuffer_in_memory, 0, 0, maxReorderBuffer);
    } else if (strncmp(elem->defname, "white-table-list", sizeof("white-table-list")) == 0) {
        ParseWhiteList(&data->tableWhiteList, elem);
    }  else if (strncmp(elem->defname, "parallel-queue-size", sizeof("parallel-queue-size")) == 0) {
        CheckIntOption(elem, &data->parallel_queue_size, DEFAULT_PARALLEL_QUEUE_SIZE,
            MIN_PARALLEL_QUEUE_SIZE, MAX_PARALLEL_QUEUE_SIZE);
        if (!POWER_OF_TWO(data->parallel_queue_size)) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("option parallel-queue-size should be a power of two"),
            errdetail("N/A"),  errcause("Wrong input option"), erraction("Please check documents for help")));
        }
    } else if (strncmp(elem->defname, "sender-timeout", sizeof("sender-timeout")) == 0 && elem->arg != NULL) {
        SetConfigOption("logical_sender_timeout", strVal(elem->arg), PGC_USERSET, PGC_S_OVERRIDE);
    } else if (strncmp(elem->defname, "include-originid", sizeof("include-originid")) == 0) {
        CheckBooleanOption(elem, &data->include_originid, false);
    } else if (strncmp(elem->defname, "parallel-decode-num", sizeof("parallel-decode-num")) != 0) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("option \"%s\" = \"%s\" is unknown", elem->defname, elem->arg ? strVal(elem->arg) : "(null)"),
            errdetail("N/A"),  errcause("Wrong input option"), erraction("Please check documents for help")));
    }
}

/*
 * Parse logical decoding options.
 */
static void ParseDecodingOptions(ParallelDecodeOption *data, List *options)
{
    ListCell *option = NULL;
    foreach (option, options) {
        ParseDecodingOption(data, option);
    }
}

static void initParallelDecodeOption(ParallelDecodeOption *pOptions, int parallelDecodeNum)
{
    pOptions->include_xids = true;
    pOptions->include_timestamp = false;
    pOptions->skip_empty_xacts = false;
    pOptions->only_local = true;
    pOptions->decode_style = 'b';
    pOptions->parallel_decode_num = 0;
    pOptions->sending_batch = 0;
    pOptions->decode_change = parallel_decode_change_to_bin;
    pOptions->parallel_queue_size = DEFAULT_PARALLEL_QUEUE_SIZE;
    pOptions->include_originid = false;

    /* GUC */
    DecodeOptionsDefault *defaultOption = LogicalDecodeGetOptionsDefault();
    if (defaultOption != NULL) {
        pOptions->parallel_decode_num = defaultOption->parallel_decode_num;
        pOptions->parallel_queue_size = defaultOption->parallel_queue_size;
        pOptions->max_txn_in_memory = defaultOption->max_txn_in_memory;
        pOptions->max_reorderbuffer_in_memory = defaultOption->max_reorderbuffer_in_memory;
    }

    /* what sepcified by startup has  a higher priority */
    pOptions->parallel_decode_num = parallelDecodeNum;
}

/*
 * Start a reader thread and N decoder threads.
 * When the client initiates a streaming decoding connection, if it is in parallel decoding mode,
 * we need to start a group of parallel decoding threads, including reader, decoder and sender.
 * Each group of decoding threads will occupy a logicaldispatcher slot.
 * The return value of the function is logicaldispatcher slot ID.
 */
void StartLogicalLogWorkers(char* dbUser, char* dbName, char* slotname, List *options, int parallelDecodeNum)
{
    int slotId = GetLogicalDispatcher();
    t_thrd.walsender_cxt.LogicalSlot = slotId;
    if (slotId == -1) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
            errmsg("can't create logical decode dispatcher"), errdetail("N/A"), errcause("System error."),
            erraction("Retry it in a few minutes.")));
    }

    on_shmem_exit(StopParallelDecodeWorkers, slotId);

    CheckAliveDecodeWorkers(slotId);
    initParallelDecodeOption(&g_Logicaldispatcher[slotId].pOptions, parallelDecodeNum);
    ParseDecodingOptions(&g_Logicaldispatcher[slotId].pOptions, options);
    errno_t rc = memcpy_s(g_Logicaldispatcher[slotId].slotName, NAMEDATALEN, slotname, strlen(slotname));
    securec_check(rc, "", "");

    StartLogicalDecodeWorkers(parallelDecodeNum, slotId, dbUser, dbName, slotname);
    WaitDecoderReady(slotId);
    g_Logicaldispatcher[slotId].readWorker = CreateLogicalReadWorker(slotId, dbUser, dbName, slotname, options);
    g_Logicaldispatcher[slotId].readWorker->tid = StartDecodeReadWorker(g_Logicaldispatcher[slotId].readWorker);

    WaitWorkerReady(slotId);
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
    gDecodeCxt[slotId].state = DECODE_IN_PROGRESS;
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
}

static void ParallelWorkerHandleError(int slotId)
{
    knl_g_parallel_decode_context *pDecodeCxt = &g_instance.comm_cxt.pdecode_cxt[slotId];

    /* Prevent interrupts while cleaning up */
    HOLD_INTERRUPTS();

    MemoryContext oldCxt = MemoryContextSwitchTo(pDecodeCxt->parallelDecodeCtx);
    ErrorData *edata = CopyErrorData();
    MemoryContextSwitchTo(oldCxt);

    SpinLockAcquire(&pDecodeCxt->rwlock);
    /* capture the first ErrorData in parallel workers */
    if (pDecodeCxt->edata == NULL) {
        pDecodeCxt->edata = edata;
    } else {
        FreeErrorData(edata);
    }
    SpinLockRelease(&pDecodeCxt->rwlock);

    /* Report the error to the server log */
    EmitErrorReport();

    FlushErrorState();

    /* Now we can allow interrupts again */
    RESUME_INTERRUPTS();
}

static void ParallelDecodeCleanup()
{
    /* release resource held by lsc */
    AtEOXact_SysDBCache(false);

    LWLockReleaseAll();
    AbortBufferIO();
    UnlockBuffers();

    /* buffer pins are released here */
    if (t_thrd.utils_cxt.CurrentResourceOwner != NULL) {
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_LOCKS, false, true);
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, true);
    }

    if (t_thrd.logicalreadworker_cxt.ReadWorkerCxt != NULL) {
        MemoryContextResetAndDeleteChildren(t_thrd.logicalreadworker_cxt.ReadWorkerCxt);
    }
}

void ParallelDecodeWorkerMain(void* point)
{
    ParallelDecodeWorker *worker = (ParallelDecodeWorker*)point;

    SetDecodeWorkerThreadState(worker->slotId, worker->id, PARALLEL_DECODE_WORKER_START);

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;

    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);
    t_thrd.proc_cxt.MyProgName = "LogicalDecodeWorker";

    t_thrd.role = PARALLEL_DECODE;

    /* Identify myself via ps */
    init_ps_display("Logical decoding worker process", "", "", "");

    ereport(LOG, (errmsg("Logical Decoding process started")));

    SetProcessingMode(InitProcessing);

    on_shmem_exit(ParallelDecodeKill, PointerGetDatum(worker));
    BaseInit();

    /* setup signal handler and unblock signal before initialization xact */
    SetupLogicalWorkerSignalHandlers();

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(worker->dbName,
        InvalidOid, worker->dbUser);
    t_thrd.proc_cxt.PostInit->InitParallelDecode();
    SetProcessingMode(NormalProcessing);
    pgstat_report_appname("LogicalDecodeWorker");
    pgstat_report_activity(STATE_IDLE, NULL);

    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "parallel decoder resource owner",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    t_thrd.logicalreadworker_cxt.ReadWorkerCxt = AllocSetContextCreate(t_thrd.top_mem_cxt, "Read Worker",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(t_thrd.logicalreadworker_cxt.ReadWorkerCxt);

    struct ParallelLogicalDecodingContext* ctx = ParallelCreateDecodingContext(
        0, NULL, false, logical_read_xlog_page, worker->slotId);
    ParallelDecodingData *data = (ParallelDecodingData *)palloc0(sizeof(ParallelDecodingData));
    data->context = (MemoryContext)AllocSetContextCreate(ctx->context, "text conversion context",
        ALLOCSET_DEFAULT_SIZES);
    errno_t rc = memcpy_s(&data->pOptions, sizeof(ParallelDecodeOption), &g_Logicaldispatcher[worker->slotId].pOptions,
        sizeof(ParallelDecodeOption));
    securec_check(rc, "", "");
    ctx->output_plugin_private = data;

    ereport(LOG, (errmsg("Parallel Decode Worker started")));

    /* do assginment ahead since some signal handler depends it. */
    t_thrd.logical_cxt.dispatchSlotId = worker->slotId;
    t_thrd.parallel_decode_cxt.parallelDecodeId = worker->id;

    PG_TRY();
    {
        ParallelReorderBufferChange *LogicalChangeHead = NULL;

        SetDecodeWorkerThreadState(worker->slotId, worker->id, PARALLEL_DECODE_WORKER_RUN);

        while (true) {
            if (IsLogicalWorkerShutdownRequested()) {
                ereport(LOG, (errmsg("Parallel Decode Worker stop")));
                break;
            }

            if (t_thrd.parallel_decode_cxt.got_SIGHUP) {
                t_thrd.parallel_decode_cxt.got_SIGHUP = false;
                ProcessConfigFile(PGC_SIGHUP);
            }

            LogicalChangeHead = (ParallelReorderBufferChange *)LogicalQueueTop(worker->changeQueue);
            if (LogicalChangeHead == NULL) {
                continue;
            }

            logicalLog* logChange = ParallelDecodeChange(LogicalChangeHead, ctx, worker);
            if (logChange != NULL) {
                logChange->toast_hash = LogicalChangeHead->toast_hash;
                LogicalChangeHead->toast_hash = NULL;
                LogicalQueuePut(worker->LogicalLogQueue, logChange);
            }
            LogicalQueuePop(worker->changeQueue);
            ParallelFreeChange(LogicalChangeHead, worker->slotId);
        }
    }
    PG_CATCH();
    {
        /* save edata into decode context and inform the sender what's wrong. */
        ParallelWorkerHandleError(worker->slotId);
    }
    PG_END_TRY();

    ParallelDecodeCleanup();
}

ParallelDecodeWorker *CreateLogicalDecodeWorker(int id, char* dbUser, char* dbName, char* slotname, int slotId)
{
    MemoryContext oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
    ParallelDecodeWorker *worker = (ParallelDecodeWorker *)palloc0(sizeof(ParallelDecodeWorker));
    MemoryContextSwitchTo(oldCtx);

    worker->id = id;
    worker->tid.thid = InvalidTid;
    worker->changeQueue = LogicalQueueCreate(slotId);
    worker->LogicalLogQueue = LogicalQueueCreate(slotId);
    errno_t rc = memcpy_s(worker->slotname, NAMEDATALEN, slotname, strlen(slotname));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(worker->dbUser, NAMEDATALEN, dbUser, strlen(dbUser));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(worker->dbName, NAMEDATALEN, dbName, strlen(dbName));
    securec_check(rc, "\0", "\0");
    SetDecodeWorkerThreadState(slotId, id, PARALLEL_DECODE_WORKER_INIT);
    return worker;
}

/*
 * The main functions of read and parse logs in parallel decoding.
 */
void LogicalReadWorkerMain(void* point)
{
    ParallelDecodeReaderWorker *reader = (ParallelDecodeReaderWorker*)point;

    SetDecodeReaderThreadState(reader->slotId, PARALLEL_DECODE_WORKER_START);

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;

    t_thrd.proc_cxt.MyProcPid = gs_thread_self();
    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);
    t_thrd.proc_cxt.MyProgName = "LogicalReadWorker";

    t_thrd.role = LOGICAL_READ_RECORD;
    /* Identify myself via ps */
    init_ps_display("Logical readworker process", "", "", "");
    t_thrd.xlog_cxt.ThisTimeLineID = GetRecoveryTargetTLI();
    ereport(LOG, (errmsg("LogicalReader process started, TimeLineId = %u", t_thrd.xlog_cxt.ThisTimeLineID)));

    SetProcessingMode(InitProcessing);

    on_shmem_exit(LogicalReadKill, Int32GetDatum(reader->slotId));

    BaseInit();

    /* setup signal handler and unblock signal before initialization xact */
    SetupLogicalWorkerSignalHandlers();

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(reader->dbName,
        InvalidOid, reader->dbUser);
    t_thrd.proc_cxt.PostInit->InitParallelDecode();
    SetProcessingMode(NormalProcessing);
    pgstat_report_appname("LogicalReadWorker");
    pgstat_report_activity(STATE_IDLE, NULL);
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "logical reader resource owner",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    t_thrd.logicalreadworker_cxt.ReadWorkerCxt = AllocSetContextCreate(t_thrd.top_mem_cxt, "Read Worker",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(t_thrd.logicalreadworker_cxt.ReadWorkerCxt);

    /* do assginment ahead since some signal handler depends it. */
    t_thrd.logical_cxt.dispatchSlotId = reader->slotId;

    LogicalReadRecordMain(reader);
}

void LogicalReadRecordMain(ParallelDecodeReaderWorker *worker)
{
    struct ParallelLogicalDecodingContext* ctx;

    SetDecodeReaderThreadState(worker->slotId, PARALLEL_DECODE_WORKER_RUN);

    PG_TRY();
    {
        int retries = 0;
        /* make sure that our requirements are still fulfilled */
        CheckLogicalDecodingRequirements(u_sess->proc_cxt.MyDatabaseId);

        XLogRecPtr startptr = InvalidXLogRecPtr;
        Assert(!t_thrd.slot_cxt.MyReplicationSlot);
        ReplicationSlotAcquire(worker->slotname, false);
        LogicalCleanSnapDirectory(true);

        /*
         * Initialize position to the last ack'ed one, then the xlog records begin
         * to be shipped from that position.
         */
        int slotId = worker->slotId;
        g_Logicaldispatcher[slotId].MyReplicationSlot = t_thrd.slot_cxt.MyReplicationSlot;

        ctx = ParallelCreateDecodingContext(0, NULL, false, logical_read_xlog_page, worker->slotId);

        ParallelDecodingData *data = (ParallelDecodingData *)MemoryContextAllocZero(
            g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx, sizeof(ParallelDecodingData));
        data->pOptions.only_local = g_Logicaldispatcher[slotId].pOptions.only_local;
        ctx->output_plugin_private = data;

        /* Start reading WAL from the oldest required WAL. */
        t_thrd.walsender_cxt.logical_startptr = t_thrd.slot_cxt.MyReplicationSlot->data.restart_lsn;

        /*
         * Report the location after which we'll send out further commits as the
         * current sentPtr.
         */
        t_thrd.walsender_cxt.sentPtr = t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush;
        startptr = t_thrd.slot_cxt.MyReplicationSlot->data.restart_lsn;

        ereport(LOG, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOGICAL_DECODE_ERROR),
            errmsg("init decode parallel")));

        ParallelReorderBufferChange *change = ParallelReorderBufferGetChange(ctx->reorder, slotId);
        change->action = PARALLEL_REORDER_BUFFER_CHANGE_CONFIRM_FLUSH;
        change->lsn = t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush;
        PutChangeQueue(slotId, change);

        while (true) {
            if (IsLogicalWorkerShutdownRequested()) {
                ereport(LOG, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOGICAL_DECODE_ERROR),
                    errmsg("Reader worker exit due to request")));
                break;
            }

            if (t_thrd.parallel_decode_cxt.got_SIGHUP) {
                t_thrd.parallel_decode_cxt.got_SIGHUP = false;
                ProcessConfigFile(PGC_SIGHUP);
            }
            char *errm = NULL;
            const uint32 upperLen = 32;
            XLogRecord *record = XLogReadRecord(ctx->reader, startptr, &errm);
            if (errm != NULL) {
                retries++;
                if (retries >= XLOG_STREAM_READREC_MAXTRY) {
                    ereport(ERROR, (errmsg("Stop parsing any XLog Record at %X/%X after %d attempts: %s.",
                        (uint32)(ctx->reader->EndRecPtr >> upperLen), (uint32)ctx->reader->EndRecPtr, retries, errm)));
                }
                const long sleepTime = 1000000L;
                pg_usleep(sleepTime);
                continue;
            } else if (retries != 0) {
                ereport(LOG, (errmsg("Reread XLog Record after %d retries at %X/%X.", retries,
                    (uint32)(ctx->reader->EndRecPtr >> upperLen), (uint32)ctx->reader->EndRecPtr)));
                retries = 0;
            }
            startptr = InvalidXLogRecPtr;

            if (record != NULL) {
                ParseProcessRecord(ctx, ctx->reader, worker);
            }

            pg_atomic_write_u64(&g_Logicaldispatcher[slotId].sentPtr, ctx->reader->EndRecPtr);
        }
    }
    PG_CATCH();
    {
        /* save edata into decode context and inform the sender what's wrong. */
        ParallelWorkerHandleError(worker->slotId);
    }
    PG_END_TRY();

    /* release slot and do cleanup. */
    CleanMyReplicationSlot();
    ParallelDecodeCleanup();
}

