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
void SendSignalToDecodeWorker(int signal, int slotId);
void SendSignalToReaderWorker(int signal, int slotId);

static const uint64 OUTPUT_WAIT_COUNT = 0x7FFFFFF;
static const uint64 PRINT_ALL_WAIT_COUNT = 0x7FFFFFFFF;
static const uint32 maxQueueLen = 1024;

/*
 * Parallel decoding kill a decoder.
 */
static void ParallelDecodeKill(int code, Datum arg)
{
    knl_t_parallel_decode_worker_context tDecodeCxt = t_thrd.parallel_decode_cxt;
    int slotId = tDecodeCxt.slotId;
    int id = tDecodeCxt.parallelDecodeId;
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
    gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[id].threadState = PARALLEL_DECODE_WORKER_EXIT;
    g_Logicaldispatcher[slotId].abnormal = true;
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
    SendSignalToReaderWorker(SIGTERM, DatumGetInt32(arg));
    SendSignalToDecodeWorker(SIGTERM, DatumGetInt32(arg));
}

/*
 * Send signal to a decoder thread.
 */
void SendSignalToDecodeWorker(int signal, int slotId)
{
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    for (int i = 0; i < gDecodeCxt[slotId].totalNum; ++i) {
        SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
        uint32 state = gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[i].threadState;
        SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
        if (state != PARALLEL_DECODE_WORKER_INVALID) {
            int err = gs_signal_send(gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[i].threadId, signal);
            if (err != 0) {
                ereport(WARNING, (errmsg("Kill logicalDecoder(pid %lu, signal %d) failed: \"%s\",",
                    gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[i].threadId, signal, gs_strerror(err))));
            } else {
                ereport(LOG, (errmsg("Kill logicalDecoder(pid %lu, signal %d) successfully: \"%s\",",
                    gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[i].threadId, signal, gs_strerror(err))));
            }
        }
    }
}

/*
 * Send signal to the reader thread.
 */
void SendSignalToReaderWorker(int signal, int slotId)
{
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
    uint32 state = gDecodeCxt[slotId].ParallelReaderWorkerStatus.threadState;
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));

    if (state != PARALLEL_DECODE_WORKER_INVALID) {
        int err = gs_signal_send(gDecodeCxt[slotId].ParallelReaderWorkerStatus.threadId, signal);
        if (err != 0) {
            ereport(WARNING, (errmsg("Kill logicalReader(pid %lu, signal %d) failed: \"%s\",",
                gDecodeCxt[slotId].ParallelReaderWorkerStatus.threadId, signal, gs_strerror(err))));
        } else {
            ereport(LOG, (errmsg("Kill logicalReader(pid %lu, signal %d) Successfully: \"%s\",",
                gDecodeCxt[slotId].ParallelReaderWorkerStatus.threadId, signal, gs_strerror(err))));
        }
    }
}

/* Run from the worker thread. */
static void ParallelDecodeSigHupHandler(SIGNAL_ARGS)
{
    t_thrd.parallel_decode_cxt.got_SIGHUP = true;
}

static void ParallelDecodeShutdownHandler(SIGNAL_ARGS)
{
    t_thrd.parallel_decode_cxt.shutdown_requested = true;
    ereport(LOG, (errmsg("ParallelDecodeShutdownHandler process shutdown.")));
}

static void ParallelReaderShutdownHandler(SIGNAL_ARGS)
{
    t_thrd.logicalreadworker_cxt.shutdown_requested = true;
    ereport(LOG, (errmsg("ParallelReaderShutdownHandler process shutdown.")));
}

static void ParallelDecodeQuickDie(SIGNAL_ARGS)
{
    ereport(LOG, (errmsg("ParallelDecodeQuickDie process shutdown.")));

    int status = 2;
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
    on_exit_reset();
    proc_exit(status);
}

/*
 * Run from the worker thread, set up the common part of signal handlers
 * for logical decoding workers.
 */
static void SetupLogicalWorkerCommonHandlers()
{
    (void)gspqsignal(SIGHUP, ParallelDecodeSigHupHandler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGQUIT, ParallelDecodeQuickDie);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_IGN);
    (void)gspqsignal(SIGTTIN, SIG_IGN);
    (void)gspqsignal(SIGTTOU, SIG_IGN);
    (void)gspqsignal(SIGCONT, SIG_IGN);
    (void)gspqsignal(SIGWINCH, SIG_IGN);
}

/*
 * Run from the worker thread, set up signal handlers for a decoder.
 */
static void SetupDecoderSignalHandlers()
{
    SetupLogicalWorkerCommonHandlers();
    (void)gspqsignal(SIGTERM, ParallelDecodeShutdownHandler);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
}

/*
 * Run from the worker thread, set up signal handlers for the reader.
 */
static void SetupLogicalReaderSignalHandlers()
{
    SetupLogicalWorkerCommonHandlers();
    (void)gspqsignal(SIGTERM, ParallelReaderShutdownHandler);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
}

/*
 * Parallel decoding kill a reader.
 */
static void LogicalReadKill(int code, Datum arg)
{
    ereport(LOG, (errmsg("LogicalReader process shutdown.")));

    /* Make sure active replication slots are released */
    if (t_thrd.slot_cxt.MyReplicationSlot != NULL) {
        ReplicationSlotRelease();
    }
    int slotId = DatumGetInt32(arg);
    SpinLockAcquire(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    gDecodeCxt[slotId].ParallelReaderWorkerStatus.threadState = PARALLEL_DECODE_WORKER_EXIT;
    g_Logicaldispatcher[slotId].abnormal = true;
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
    SendSignalToReaderWorker(SIGTERM, DatumGetInt32(arg));
    SendSignalToDecodeWorker(SIGTERM, DatumGetInt32(arg));
}

/*
 * Parallel decoding release resource.
 */
void ReleaseParallelDecodeResource(int slotId)
{
    SpinLockAcquire(&(g_instance.comm_cxt.pdecode_cxt[slotId].destroy_lock));
    MemoryContextDelete(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
    g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx = NULL;
    SpinLockRelease(&(g_instance.comm_cxt.pdecode_cxt[slotId].destroy_lock));
    g_Logicaldispatcher[slotId].active = false;
    ereport(LOG, (errmsg("g_Logicaldispatcher[%d].active = false", slotId)));
    g_Logicaldispatcher[slotId].abnormal = false;
    return;
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
ParallelDecodeWorker* StartLogicalDecodeWorker(uint32 id, uint32 slotId, char* dbUser, char* dbName, char* slotname)
{
    ParallelDecodeWorker *worker = CreateLogicalDecodeWorker(id, dbUser, dbName, slotname, slotId);
    worker->slotId = slotId;
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    ThreadId threadId = StartDecodeWorkerThread(worker);
    if (threadId == 0) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
            errmsg("Create parallel logical decoder thread failed"), errdetail("id = %u, slotId = %u, %m", id, slotId),
            errcause("System error."), erraction("Retry it in a few minutes.")));
        SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
        gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[id].threadState = PARALLEL_DECODE_WORKER_EXIT;
        SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
        ReleaseParallelDecodeResource(slotId);
        return NULL;
    } else {
        ereport(LOG, (errmsg("StartParallelDecodeWorker successfully create logical decoder id: %u, threadId:%lu.", id,
            worker->tid.thid)));
    }

    SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
    gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[id].threadId = threadId;
    uint32 state = pg_atomic_read_u32(&(gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[id].threadState));
    if (state != PARALLEL_DECODE_WORKER_READY) {
        gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[id].threadState = PARALLEL_DECODE_WORKER_START;
    }
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
    return worker;
}

/*
 * Parallel decoding start all decode workers.
 */
void StartLogicalDecodeWorkers(int parallelism, uint32 slotId, char* dbUser, char* dbName, char* slotname)
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
ParallelDecodeReaderWorker *CreateLogicalReadWorker(uint32 slotId, char* dbUser, char* dbName, char* slotname,
    List *options)
{
    errno_t rc;
    MemoryContext oldCtx = NULL;
    oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
    ParallelDecodeReaderWorker *worker = (ParallelDecodeReaderWorker *)palloc0(sizeof(ParallelDecodeReaderWorker));
    MemoryContextSwitchTo(oldCtx);

    worker->slotId = slotId;
    worker->tid = InvalidTid;

    worker->queue = LogicalQueueCreate(maxQueueLen, slotId);
    rc = memcpy_s(worker->slotname, NAMEDATALEN, slotname, strlen(slotname));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(worker->dbUser, NAMEDATALEN, dbUser, strlen(dbUser));
    securec_check(rc, "\0", "\0");
    
    rc = memcpy_s(worker->dbName, NAMEDATALEN, dbName, strlen(dbName));
    securec_check(rc, "\0", "\0");

    SpinLockAcquire(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
    g_instance.comm_cxt.pdecode_cxt[slotId].ParallelReaderWorkerStatus.threadState = PARALLEL_DECODE_WORKER_INVALID;
    SpinLockRelease(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
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
        SpinLockAcquire(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
        g_instance.comm_cxt.pdecode_cxt[slotId].ParallelReaderWorkerStatus.threadState = PARALLEL_DECODE_WORKER_EXIT;
        SpinLockRelease(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
        return threadId;
    } else {
        ereport(LOG, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
            errmsg("StartDecodeReadWorker successfully create decodeReaderWorker id:%u,threadId:%lu",
                worker->id, threadId)));
    }
    SpinLockAcquire(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
    g_instance.comm_cxt.pdecode_cxt[worker->slotId].ParallelReaderWorkerStatus.threadId = threadId;
    uint32 state = g_instance.comm_cxt.pdecode_cxt[worker->slotId].ParallelReaderWorkerStatus.threadState;
    SpinLockRelease(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
    if (state != PARALLEL_DECODE_WORKER_READY) {
        SpinLockAcquire(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
        g_instance.comm_cxt.pdecode_cxt[worker->slotId].ParallelReaderWorkerStatus.threadState =
            PARALLEL_DECODE_WORKER_START;
        SpinLockRelease(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
    }

    return threadId;
}

/*
 * Check whether there are decoding threads that have not exited.
 */
void CheckAliveDecodeWorkers(uint32 slotId)
{
    uint32 state = -1;

    /*
     * Check reader thread state.
     */
    SpinLockAcquire(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
    state = g_instance.comm_cxt.pdecode_cxt[slotId].ParallelReaderWorkerStatus.threadState;
    SpinLockRelease(&(g_instance.comm_cxt.pdecode_cxt[slotId].rwlock));
    if (state != PARALLEL_DECODE_WORKER_INVALID) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
            errmsg("Check alive reader worker failed"),
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
        if (state != PARALLEL_DECODE_WORKER_INVALID) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
                errmsg("Check alive decoder workers failed"),
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
int GetReadyDecodeWorker(uint32 slotId)
{
    int readyWorkerCnt = 0;
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
    for (int i = 0; i < gDecodeCxt[slotId].totalNum; i++) {
        uint32 state = gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[i].threadState;
        if (state == PARALLEL_DECODE_WORKER_READY) {
            ++readyWorkerCnt;
        }
    }
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
    return readyWorkerCnt;
}

/*
 * Get the number of ready reader.
 */
uint32 GetReadyDecodeReader(uint32 slotId)
{
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
    uint32 state = gDecodeCxt[slotId].ParallelReaderWorkerStatus.threadState;
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
    if (state == PARALLEL_DECODE_WORKER_READY) {
        return 1;
    }
    return 0;
}

/*
 * Wait until all workers are ready.
 */
void WaitWorkerReady(uint32 slotId)
{
    uint32 loop = 0;
    int readyDecoder = 0;
    uint32 readyReader = 0;
    const uint32 maxWaitSecondsForDecoder = 6000;
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;

    /* MAX wait 2min */
    for (loop = 0; loop < maxWaitSecondsForDecoder; ++loop) {
        /* This connection exit if logical thread abnormal */
        if (g_Logicaldispatcher[slotId].abnormal) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
                errmsg("Wait for worker ready failed."), errdetail("At least one thread is abnormal."),
                errcause("System error."), erraction("Retry it in a few minutes.")));
        }
        
        readyDecoder = GetReadyDecodeWorker(slotId);
        readyReader = GetReadyDecodeReader(slotId);
        if ((readyDecoder == gDecodeCxt[slotId].totalNum) && (readyReader == 1)) {
            ereport(LOG, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
                errmsg("WaitWorkerReady total worker count:%d, readyWorkerCnt:%d",
                    g_Logicaldispatcher[slotId].totalWorkerCount, readyDecoder)));
            break;
        }
        const long sleepTime = 1000;
        pg_usleep(sleepTime);
    }
    SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
    gDecodeCxt[slotId].state = DECODE_STARTING_END;
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
    readyDecoder = GetReadyDecodeWorker(slotId);
    readyReader = GetReadyDecodeReader(slotId);
    int totalDecoders = gDecodeCxt[slotId].totalNum;
    if (loop == maxWaitSecondsForDecoder &&
        (readyDecoder != totalDecoders || readyReader != 1)) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG), errmsg("Wait for worker ready failed"),
            errdetail("Not all workers or no reader are ready for work. totalWorkerCount: %d",
                g_Logicaldispatcher[slotId].totalWorkerCount),
            errcause("System error."), erraction("Retry it in a few minutes.")));
    }

    ereport(LOG, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
        errmsg("WaitWorkerReady total worker count:%u, readyWorkerCnt:%d",
            g_Logicaldispatcher[slotId].totalWorkerCount, readyDecoder)));
}

/*
 * Send signal to the reader and all decoders.
 */
void SendSingalToReaderAndDecoder(uint32 slotId, int signal)
{
    ereport(LOG, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG), errmsg("Send signal to reader and decoders.")));
    uint32 state = -1;
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    /* kill reader */
    SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
    state = gDecodeCxt[slotId].ParallelReaderWorkerStatus.threadState;
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
    if (state != PARALLEL_DECODE_WORKER_EXIT) {
        int err = gs_signal_send(gDecodeCxt[slotId].ParallelReaderWorkerStatus.threadId, signal);
        if (0 != err) {
            ereport(WARNING, (errmsg("Walsender kill reader(pid %lu, signal %d) failed: \"%s\",",
                gDecodeCxt[slotId].ParallelReaderWorkerStatus.threadId, signal, gs_strerror(err))));
        }
    }

    /* kill decoders */
    for (int i = 0; i < gDecodeCxt[slotId].totalNum; ++i) {
        SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
        state = gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[i].threadState;
        SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
        if (state != PARALLEL_DECODE_WORKER_EXIT) {
            int err = gs_signal_send(gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[i].threadId,
                signal);
            if (0 != err) {
                ereport(WARNING, (errmsg("Walsender kill decoder(pid %lu, signal %d) failed: \"%s\",",
                    gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[i].threadId, signal, gs_strerror(err))));
            }
        }
    }
}

/* Check if logicalReader and all logicalDecoder threads is PARALLEL_DECODE_WORKER_EXIT. */
bool logicalWorkerCouldExit(int slotId)
{
    uint32 state = -1;
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
    state = gDecodeCxt[slotId].ParallelReaderWorkerStatus.threadState;
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
    if (state != PARALLEL_DECODE_WORKER_EXIT) {
        return false;
    }

    for (int i = 0; i < gDecodeCxt[slotId].totalNum; ++i) {
        SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
        state = gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[i].threadState;
        SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
        if (state != PARALLEL_DECODE_WORKER_EXIT) {
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
    uint32 slotId = DatumGetUInt32(arg);
    SendSingalToReaderAndDecoder(slotId, SIGTERM);
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;

    uint64 count = 0;
    while (!logicalWorkerCouldExit(slotId)) {
        ++count;
        if ((count & OUTPUT_WAIT_COUNT) == OUTPUT_WAIT_COUNT) {
            ereport(WARNING, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
                errmsg("StopDecodeWorkers wait reader and decoder exit")));
            if ((count & PRINT_ALL_WAIT_COUNT) == PRINT_ALL_WAIT_COUNT) {
                ereport(PANIC, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
                    errmsg("StopDecodeWorkers wait too long!!!"), errdetail("N/A"), errcause("System error."),
                    erraction("Retry it in a few minutes.")));
            }
            const long sleepCheckTime = 100;
            pg_usleep(sleepCheckTime);
        }
    }

    SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
    gDecodeCxt[slotId].ParallelReaderWorkerStatus.threadState = PARALLEL_DECODE_WORKER_INVALID;
    for (int i = 0; i < gDecodeCxt[slotId].totalNum; ++i) {
        gDecodeCxt[slotId].ParallelDecodeWorkerStatusList[i].threadState = PARALLEL_DECODE_WORKER_INVALID;
    }

    gDecodeCxt[slotId].state = DECODE_DONE;
    SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
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
    int max_replication_slots = g_instance.attr.attr_storage.max_replication_slots;
    LWLockAcquire(ParallelDecodeLock, LW_EXCLUSIVE);
    MemoryContext ctx = AllocSetContextCreate(g_instance.instance_context, "ParallelDecodeDispatcher",
                                              ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                              ALLOCSET_DEFAULT_MAXSIZE, SHARED_CONTEXT);
    knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
    static int i = 0;

    for (; i < max_replication_slots; i++) {
        if (g_Logicaldispatcher[i].active == false) {
            slotId = i;
            errno_t rc = memset_s(&g_Logicaldispatcher[slotId], sizeof(LogicalDispatcher), 0,
                sizeof(LogicalDispatcher));
            securec_check(rc, "", "");
            InitLogicalDispatcher(&g_Logicaldispatcher[slotId]);
            g_Logicaldispatcher[i].active = true;

            ereport(LOG, (errmsg("g_Logicaldispatcher[%d].active = true", slotId)));
            g_Logicaldispatcher[i].abnormal = false;
            gDecodeCxt[i].parallelDecodeCtx = ctx;
            break;
        }
    }
    LWLockRelease(ParallelDecodeLock);
    if(slotId == -1) {
        return slotId;
    }

    SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
    int state = gDecodeCxt[slotId].state;
    if (state == DECODE_IN_PROGRESS) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG), errmsg("Get logical dispatcher failed"),
            errdetail("walsender reconnected thread exit."), errcause("System error."),
            erraction("Retry it in a few minutes.")));
    }
    gDecodeCxt[slotId].state = DECODE_STARTING_BEGIN;
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

        if ((cTable->schema == NULL || strncmp(cTable->schema, schema, strlen(schema)) == 0) &&
            (cTable->table == NULL || strncmp(cTable->table, table, strlen(table)) == 0)) {
                return true;
        }
    }
    return false;
}

/*
 * Parse a list of raw table names into a list of schema and table names.
 */
static bool ParseSchemaAndTableName(List *tableList, List **tableWhiteList)
{
    ListCell *lc = NULL;
    char *str = NULL;
    char *startPos = NULL;
    char *curPos = NULL;
    size_t len = 0;
    chosenTable *cTable = NULL;
    bool anySchema = false;
    bool anyTable = false;
    errno_t rc = 0;

    foreach(lc, tableList) {
        str = (char*)lfirst(lc);
        cTable = (chosenTable *)palloc(sizeof(chosenTable));

        if (*str == '*' && *(str + 1) == '.') {
            cTable->schema = NULL;
            anySchema = true;
        }
        startPos = str;
        curPos = str;
        while (*curPos != '\0' && *curPos != '.') {
            curPos++;
        }
        len = (size_t)(curPos - startPos);

        if (*curPos == '\0') {
            pfree(cTable);
            return false;
        } else {
            if (!anySchema) {
                cTable->schema = (char *)palloc0((len + 1) * sizeof(char));
                errno_t rc = strncpy_s(cTable->schema, len + 1, startPos, len);
                securec_check(rc, "", "");
            }

            curPos++;
            startPos = curPos;

            if (*startPos == '*' && *(startPos + 1) == '\0') {
                cTable->table = NULL;
                anyTable = true;
            }
            while (*curPos != '\0') {
                curPos++;
            }
            len = (size_t)(curPos - startPos);

            if (!anyTable) {
                cTable->table = (char *)palloc((len + 1) * sizeof(char));
                rc = strncpy_s(cTable->table, len + 1, startPos, len);
                securec_check(rc, "", "");
            }
        }
        *tableWhiteList = lappend(*tableWhiteList, cTable);
    }
    return true;
}

/*
 * Parse a rawstring to a list of table names.
 */
bool ParseStringToWhiteList(char *tableString, List **tableWhiteList)
{
    char *curPos = tableString;
    bool finished = false;
    List *tableList = NIL;
    while (isspace(*curPos)) {
        curPos++;
    }
    if (*curPos == '\0') {
        return true;
    }

    do {
        char* tmpName = curPos;
        while (*curPos != '\0' && *curPos != ',' && !isspace(*curPos)) {
            curPos++;
        }
        char *tmpEnd = curPos;
        if (tmpName == curPos) {
            list_free_deep(tableList);
            return false;
        }
        while (isspace(*curPos)) {
            curPos++;
        }
        if (*curPos == '\0') {
            finished = true;
        } else if (*curPos == ',') {
            curPos++;
            while (isspace(*curPos)) {
                curPos++;
            }
        } else {
            list_free_deep(tableList);
            return false;
        }
        *tmpEnd = '\0';
        char *tableName = pstrdup(tmpName);
        tableList = lappend(tableList, tableName);
    } while (!finished);

    if (!ParseSchemaAndTableName(tableList, tableWhiteList)) {
        list_free_deep(tableList);
        return false;
    }

    list_free_deep(tableList);
    return true;
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
    foreach (option, options) {
        DefElem* elem = (DefElem*)lfirst(option);
        ParseParallelDecodeNum(elem, &parallelDecodeNum);
    }
    return parallelDecodeNum;
}

static void CheckBatchSendingOption(const DefElem* elem, int *sendingBatch)
{
    if (elem->arg != NULL && (!parse_int(strVal(elem->arg), sendingBatch, 0, NULL) ||
        *sendingBatch < 0 || *sendingBatch > 1)) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("could not parse value \"%s\" for parameter \"%s\"", strVal(elem->arg), elem->defname),
                errdetail("N/A"),  errcause("Wrong input value"), erraction("Input 0 or 1.")));
    }
}

/*
 * Parse a single logical decoding option.
 */
static void ParseDecodingOption(ParallelDecodeOption *data, ListCell *option)
{
    DefElem* elem = (DefElem*)lfirst(option);

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
        CheckBatchSendingOption(elem, &data->sending_batch);
    } else if (strncmp(elem->defname, "decode-style", sizeof("decode-style")) == 0) {
        CheckDecodeStyle(data, elem);
    } else if (strncmp(elem->defname, "white-table-list", sizeof("white-table-list")) == 0) {
        ParseWhiteList(&data->tableWhiteList, elem);
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

/*
 * Start a reader thread and N decoder threads.
 * When the client initiates a streaming decoding connection, if it is in parallel decoding mode,
 * we need to start a group of parallel decoding threads, including reader, decoder and sender.
 * Each group of decoding threads will occupy a logicaldispatcher slot.
 * The return value of the function is logicaldispatcher slot ID.
 */
int StartLogicalLogWorkers(char* dbUser, char* dbName, char* slotname, List *options, int parallelDecodeNum)
{
    int slotId = GetLogicalDispatcher();
    if (slotId == -1) {
        ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOG),
            errmsg("can't create logical decode dispatcher"), errdetail("N/A"), errcause("System error."),
            erraction("Retry it in a few minutes.")));
    }
    on_shmem_exit(StopParallelDecodeWorkers, slotId);
    if (parallelDecodeNum > 1) {
        CheckAliveDecodeWorkers(slotId);
        ParallelDecodeOption *pOptions = &g_Logicaldispatcher[slotId].pOptions;
        pOptions->include_xids = true;
        pOptions->include_timestamp = false;
        pOptions->skip_empty_xacts = false;
        pOptions->only_local = true;
        pOptions->decode_style = 'b';
        pOptions->parallel_decode_num = parallelDecodeNum;
        pOptions->sending_batch = 0;
        pOptions->decode_change = parallel_decode_change_to_text;
        ParseDecodingOptions(&g_Logicaldispatcher[slotId].pOptions, options);
        errno_t rc = memcpy_s(g_Logicaldispatcher[slotId].slotName, NAMEDATALEN, slotname, strlen(slotname));
        securec_check(rc, "", "");

        StartLogicalDecodeWorkers(parallelDecodeNum, slotId, dbUser, dbName, slotname);
        g_Logicaldispatcher[slotId].readWorker = CreateLogicalReadWorker(slotId, dbUser, dbName, slotname, options);
        g_Logicaldispatcher[slotId].readWorker->tid = StartDecodeReadWorker(g_Logicaldispatcher[slotId].readWorker);

        WaitWorkerReady(slotId);
        knl_g_parallel_decode_context *gDecodeCxt = g_instance.comm_cxt.pdecode_cxt;
        SpinLockAcquire(&(gDecodeCxt[slotId].rwlock));
        gDecodeCxt[slotId].state = DECODE_IN_PROGRESS;
        SpinLockRelease(&(gDecodeCxt[slotId].rwlock));
    }
    return slotId;
}

void ParallelDecodeWorkerMain(void* point)
{
    ParallelDecodeWorker *worker = (ParallelDecodeWorker*)point;
    ParallelReorderBufferChange* LogicalChangeHead;

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

    on_shmem_exit(ParallelDecodeKill, Int32GetDatum(worker->slotId));

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(worker->dbName,
        InvalidOid, worker->dbUser);
    t_thrd.proc_cxt.PostInit->InitParallelDecode();
    SetProcessingMode(NormalProcessing);

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

    t_thrd.role = PARALLEL_DECODE;

    ereport(LOG, (errmsg("Parallel Decode Worker started")));

    SetupDecoderSignalHandlers();
    t_thrd.parallel_decode_cxt.slotId = worker->slotId;
    t_thrd.parallel_decode_cxt.parallelDecodeId = worker->id;

    knl_g_parallel_decode_context* pdecode_cxt = g_instance.comm_cxt.pdecode_cxt;
    int id = worker->id;
    SpinLockAcquire(&(g_instance.comm_cxt.pdecode_cxt[worker->slotId].rwlock));
    pdecode_cxt[worker->slotId].ParallelDecodeWorkerStatusList[id].threadState = PARALLEL_DECODE_WORKER_READY;
    SpinLockRelease(&(g_instance.comm_cxt.pdecode_cxt[worker->slotId].rwlock));

    while (true) {
        if (t_thrd.parallel_decode_cxt.shutdown_requested) {
            ereport(LOG, (errmsg("Parallel Decode Worker stop")));
            proc_exit(0);
        }

        LogicalChangeHead = (ParallelReorderBufferChange *)LogicalQueueTop(worker->changeQueue);
        if (LogicalChangeHead == NULL) {
            continue;
        }

        ParallelDecodeChange(LogicalChangeHead, ctx, worker);
        LogicalQueuePop(worker->changeQueue);
        ParallelFreeChange(LogicalChangeHead, worker->slotId);
    }
}

ParallelDecodeWorker *CreateLogicalDecodeWorker(uint32 id, char* dbUser, char* dbName, char* slotname, uint32 slotId)
{
    MemoryContext oldCtx = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
    ParallelDecodeWorker *worker = (ParallelDecodeWorker *)palloc0(sizeof(ParallelDecodeWorker));
    MemoryContextSwitchTo(oldCtx);

    worker->id = id;
    worker->tid.thid = InvalidTid;
    worker->changeQueue = LogicalQueueCreate(maxQueueLen, slotId);
    worker->LogicalLogQueue = LogicalQueueCreate(maxQueueLen, slotId);
    errno_t rc = memcpy_s(worker->slotname, NAMEDATALEN, slotname, strlen(slotname));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(worker->dbUser, NAMEDATALEN, dbUser, strlen(dbUser));
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(worker->dbName, NAMEDATALEN, dbName, strlen(dbName));
    securec_check(rc, "\0", "\0");
    return worker;
}

/*
 * The main functions of read and parse logs in parallel decoding.
 */
void LogicalReadWorkerMain(void* point)
{
    ParallelDecodeReaderWorker *reader = (ParallelDecodeReaderWorker*)point;
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

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(reader->dbName,
        InvalidOid, reader->dbUser);
    t_thrd.proc_cxt.PostInit->InitParallelDecode();
    SetProcessingMode(NormalProcessing);

    t_thrd.logicalreadworker_cxt.ReadWorkerCxt = AllocSetContextCreate(t_thrd.top_mem_cxt, "Read Worker",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    (void)MemoryContextSwitchTo(t_thrd.logicalreadworker_cxt.ReadWorkerCxt);

    SetupLogicalReaderSignalHandlers();
    SpinLockAcquire(&(g_instance.comm_cxt.pdecode_cxt[reader->slotId].rwlock));
    g_instance.comm_cxt.pdecode_cxt[reader->slotId].ParallelReaderWorkerStatus.threadState =
        PARALLEL_DECODE_WORKER_READY;
    SpinLockRelease(&(g_instance.comm_cxt.pdecode_cxt[reader->slotId].rwlock));

    LogicalReadRecordMain(reader);
}

void LogicalReadRecordMain(ParallelDecodeReaderWorker *worker)
{
    struct ParallelLogicalDecodingContext* ctx;

    /* make sure that our requirements are still fulfilled */
    CheckLogicalDecodingRequirements(u_sess->proc_cxt.MyDatabaseId);
    
    XLogRecPtr startptr = InvalidXLogRecPtr;
    Assert(!t_thrd.slot_cxt.MyReplicationSlot);
    ReplicationSlotAcquire(worker->slotname, false);
    {
        /*
         * Rebuild snap dir
         */
        char snappath[MAXPGPATH];
        struct stat st;
        int rc = 0;

        rc = snprintf_s(snappath,
            MAXPGPATH,
            MAXPGPATH - 1,
            "pg_replslot/%s/snap",
            NameStr(t_thrd.slot_cxt.MyReplicationSlot->data.name));
        securec_check_ss(rc, "\0", "\0");

        if (stat(snappath, &st) == 0 && S_ISDIR(st.st_mode)) {
            if (!rmtree(snappath, true)) {
                ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode_for_file_access(),
                    errmsg("could not remove directory \"%s\": %m", snappath), errdetail("N/A"),
                    errcause("System error."), erraction("Retry it in a few minutes.")));
            }
        }
        if (mkdir(snappath, S_IRWXU) < 0) {
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode_for_file_access(),
                errmsg("could not create directory \"%s\": %m", snappath), errdetail("N/A"), errcause("System error."),
                erraction("Retry it in a few minutes.")));
        }
    }
    /*
     * Initialize position to the last ack'ed one, then the xlog records begin
     * to be shipped from that position.
     */
    int slotId = worker->slotId;
    g_Logicaldispatcher[slotId].MyReplicationSlot = t_thrd.slot_cxt.MyReplicationSlot;
    ctx = ParallelCreateDecodingContext(
        0, NULL, false, logical_read_xlog_page, worker->slotId);
    MemoryContext oldContext = MemoryContextSwitchTo(g_instance.comm_cxt.pdecode_cxt[slotId].parallelDecodeCtx);
    ParallelDecodingData *data = (ParallelDecodingData *)palloc0(sizeof(ParallelDecodingData));
    data->pOptions.only_local = g_Logicaldispatcher[slotId].pOptions.only_local;
    ctx->output_plugin_private = data;
    MemoryContextSwitchTo(oldContext);

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

    ParallelReorderBufferChange *change = NULL;

    change = ParallelReorderBufferGetChange(ctx->reorder, slotId);

    change->action = PARALLEL_REORDER_BUFFER_CHANGE_CONFIRM_FLUSH;
    change->lsn = t_thrd.slot_cxt.MyReplicationSlot->data.confirmed_flush;
    PutChangeQueue(slotId, change);

    while (true) {
        if (t_thrd.logicalreadworker_cxt.shutdown_requested) {
            ereport(LOG, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOGICAL_DECODE_ERROR),
                errmsg("Reader worker exit due to request")));
            proc_exit(1);
        }
        XLogRecord *record = NULL;
        char *errm = NULL;

        record = XLogReadRecord(ctx->reader, startptr, &errm);
        if (errm != NULL) {
            const uint32 upperLen = 32;
            ereport(ERROR, (errmodule(MOD_LOGICAL_DECODE), errcode(ERRCODE_LOGICAL_DECODE_ERROR),
                errmsg("Stopped to parse any valid XLog Record at %X/%X: %s.",
                       (uint32)(ctx->reader->EndRecPtr >> upperLen), (uint32)ctx->reader->EndRecPtr, errm),
                errdetail("N/A"), errcause("Xlog damaged or removed."),
                erraction("Contact engineer to recover xlog files.")));
        }
        startptr = InvalidXLogRecPtr;

        if (record != NULL) {
            ParseProcessRecord(ctx, ctx->reader, worker);
        }

        pg_atomic_write_u64(&g_Logicaldispatcher[slotId].sentPtr, ctx->reader->EndRecPtr);
    };
    ReplicationSlotRelease();
}

