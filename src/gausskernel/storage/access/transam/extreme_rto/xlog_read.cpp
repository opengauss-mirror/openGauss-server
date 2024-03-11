/*
 * Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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
 * xlog_read.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/transam/extreme_rto/xlog_read.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "access/extreme_rto/spsc_blocking_queue.h"
#include "access/extreme_rto/dispatcher.h"
#include "access/multi_redo_api.h"
#include "access/xlog.h"
#include "ddes/dms/ss_reform_common.h"
#include "replication/walreceiver.h"
#include "replication/dcf_replication.h"
#include "replication/ss_disaster_cluster.h"
#include "replication/shared_storage_walreceiver.h"
#include "storage/ipc.h"

namespace extreme_rto {
static bool DoEarlyExit()
{
    if (g_dispatcher == NULL) {
        return false;
    }
    return g_dispatcher->recoveryStop;
}

inline static XLogReaderState *ReadNextRecordFromQueue(int emode)
{
    char *errormsg = NULL;
    SPSCBlockingQueue *linequeue = g_dispatcher->readLine.readPageThd->queue;
    XLogReaderState *xlogreader = NULL;
    do {
        xlogreader = (XLogReaderState *)SPSCBlockingQueueTake(linequeue);
        if (!xlogreader->isDecode) {
            XLogRecord *record = (XLogRecord *)xlogreader->readRecordBuf;
            GetRedoStartTime(t_thrd.xlog_cxt.timeCost[TIME_COST_STEP_5]);
            if (!DecodeXLogRecord(xlogreader, record, &errormsg)) {
                ereport(emode,
                        (errmsg("ReadNextRecordFromQueue %X/%X decode error, %s", (uint32)(xlogreader->EndRecPtr >> 32),
                                (uint32)(xlogreader->EndRecPtr), errormsg)));

                RedoItem *item = GetRedoItemPtr(xlogreader);

                FreeRedoItem(item);

                xlogreader = NULL;
            }
            CountRedoTime(t_thrd.xlog_cxt.timeCost[TIME_COST_STEP_5]);
        }

        if ((void *)xlogreader == (void *)&(g_GlobalLsnForwarder.record) ||
            (void *)xlogreader == (void *)&(g_cleanupMark.record)) {
            StartupSendFowarder(GetRedoItemPtr(xlogreader));
            xlogreader = NULL;
        }

        RedoInterruptCallBack();
    } while (xlogreader == NULL);

    return xlogreader;
}

XLogRecord *ReadNextXLogRecord(XLogReaderState **xlogreaderptr, int emode)
{
    XLogRecord *record = NULL;
    XLogReaderState *xlogreader = ReadNextRecordFromQueue(emode);

    if ((void *)xlogreader != (void *)&(g_redoEndMark.record)) {
        *xlogreaderptr = xlogreader;
        t_thrd.xlog_cxt.ReadRecPtr = xlogreader->ReadRecPtr;
        t_thrd.xlog_cxt.EndRecPtr = xlogreader->EndRecPtr;
        record = (XLogRecord *)xlogreader->readRecordBuf;
    } else {
        *xlogreaderptr = &g_redoEndMark.record;
        if (t_thrd.startup_cxt.shutdown_requested) {
            proc_exit(0);
        }
    }
    return record;
}

void SwitchToReadXlogFromFile(XLogRecPtr pageptr)
{
    pg_atomic_write_u32(&g_dispatcher->rtoXlogBufState.readSource, XLOG_FROM_PG_XLOG);
    pg_atomic_write_u64(&g_dispatcher->rtoXlogBufState.expectLsn, InvalidXLogRecPtr);
    pg_atomic_write_u32(&(g_recordbuffer->readWorkerState), WORKER_STATE_STOPPING);
    uint32 workerState = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
    while (workerState != WORKER_STATE_EXIT && workerState != WORKER_STATE_STOP) {
        RedoInterruptCallBack();
        workerState = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
    }
}

bool HasReceivedTrigger()
{
    uint32 trigger = pg_atomic_read_u32(&g_readManagerTriggerFlag);
    if (trigger > 0) {
        pg_atomic_write_u32(&g_dispatcher->rtoXlogBufState.readSource, XLOG_FROM_PG_XLOG);
        pg_atomic_write_u32(&(g_recordbuffer->readWorkerState), WORKER_STATE_STOPPING);
        return true;
    }
    return false;
}

// receivedUpto indicate received new datas, but can not read,we should check
bool IsReceivingStatusOk()
{
    WalRcvCtlBlock *walrcb = getCurrentWalRcvCtlBlock();
    uint32 startreadworker = pg_atomic_read_u32(&(g_recordbuffer->readWorkerState));
    if (startreadworker == WORKER_STATE_STOP && walrcb == NULL) {
        return false;
    }
    return true;
}

inline XLogRecPtr CalcExpectLsn(XLogRecPtr recPtr)
{
    XLogRecPtr expectedRecPtr = recPtr;
    if (recPtr % XLogSegSize == 0) {
        XLByteAdvance(expectedRecPtr, SizeOfXLogLongPHD);
    } else if (recPtr % XLOG_BLCKSZ == 0) {
        XLByteAdvance(expectedRecPtr, SizeOfXLogShortPHD);
    }
    return expectedRecPtr;
}

int ParallelXLogReadWorkBufRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen,
                                XLogRecPtr targetRecPtr, TimeLineID *readTLI)
{
    XLogRecPtr RecPtr = targetPagePtr;
    uint32 targetPageOff = targetPagePtr % XLogSegSize;

    XLByteToSeg(targetPagePtr, t_thrd.xlog_cxt.readSegNo);
    XLByteAdvance(RecPtr, reqLen);

    XLogRecPtr expectedRecPtr = CalcExpectLsn(RecPtr);
    uint64 waitXLogCount = 0;
    const uint64 pushLsnCount = 2;

    pg_atomic_write_u64(&g_dispatcher->rtoXlogBufState.expectLsn, expectedRecPtr);
    for (;;) {
        // Check to see if the trigger file exists. If so, update the gaussdb state file.
        if (CheckForStandbyTrigger()
#ifndef ENABLE_MULTIPLE_NODES
            && IsDCFReadyOrDisabled()
#endif
            ) {
            SendPostmasterSignal(PMSIGNAL_UPDATE_NORMAL);
        }

        /*
         * If we find an invalid record in the WAL streamed from
         * master, something is seriously wrong. There's little
         * chance that the problem will just go away, but PANIC is
         * not good for availability either, especially in hot
         * standby mode. Disconnect, and retry from
         * archive/pg_xlog again. The WAL in the archive should be
         * identical to what was streamed, so it's unlikely that
         * it helps, but one can hope...
         */
        if (t_thrd.xlog_cxt.failedSources & XLOG_FROM_STREAM) {
            pg_atomic_write_u32(&g_dispatcher->rtoXlogBufState.failSource, XLOG_FROM_STREAM);
            SwitchToReadXlogFromFile(targetPagePtr);
            return -1;
        }

        ResetRtoXlogReadBuf(targetPagePtr);
        /*
         * Walreceiver is active, so see if new data has arrived.
         *
         * We only advance XLogReceiptTime when we obtain fresh
         * WAL from walreceiver and observe that we had already
         * processed everything before the most recent "chunk"
         * that it flushed to disk.  In steady state where we are
         * keeping up with the incoming data, XLogReceiptTime will
         * be updated on each cycle.  When we are behind,
         * XLogReceiptTime will not advance, so the grace time
         * alloted to conflicting queries will decrease.
         */
        bool havedata = NewDataIsInBuf(expectedRecPtr);
        if (havedata) {
            /* just make sure source info is correct... */
            t_thrd.xlog_cxt.readSource = XLOG_FROM_STREAM;
            t_thrd.xlog_cxt.XLogReceiptSource = XLOG_FROM_STREAM;
            waitXLogCount = 0;
            if ((targetPagePtr / XLOG_BLCKSZ) != (t_thrd.xlog_cxt.receivedUpto / XLOG_BLCKSZ)) {
                t_thrd.xlog_cxt.readLen = XLOG_BLCKSZ;
            } else {
                t_thrd.xlog_cxt.readLen = t_thrd.xlog_cxt.receivedUpto % XLogSegSize - targetPageOff;
            }

            /*  read from wal writer buffer */
            bool readflag = XLogPageReadForExtRto(xlogreader, targetPagePtr, t_thrd.xlog_cxt.readLen);
            if (readflag) {
                *readTLI = t_thrd.xlog_cxt.curFileTLI;
                return t_thrd.xlog_cxt.readLen;
            } else {
                if (!IsReceivingStatusOk()) {
                    SwitchToReadXlogFromFile(targetPagePtr);
                    return -1;
                }
            }
        } else {
            if (HasReceivedTrigger()) {
                return -1;
            }

            uint32 waitRedoDone = pg_atomic_read_u32(&g_dispatcher->rtoXlogBufState.waitRedoDone);
            if (waitRedoDone == 1 || DoEarlyExit()) {
                SwitchToReadXlogFromFile(targetPagePtr);
                return -1;
            }
            /*
             * Wait for more WAL to arrive, or timeout to be reached
             */
            WaitLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupLatch, WL_LATCH_SET | WL_TIMEOUT, 1000L);
            ResetLatch(&t_thrd.shemem_ptr_cxt.XLogCtl->recoveryWakeupLatch);
            PushToWorkerLsn(waitXLogCount == pushLsnCount);
            ++waitXLogCount;
        }

        RedoInterruptCallBack();
    }

    return -1;
}

void WaitReplayFinishAfterReadXlogFileComplete(XLogRecPtr lastValidRecordLsn)
{
    Assert(t_thrd.xlog_cxt.EndRecPtr == lastValidRecordLsn);
    XLogRecPtr lastReplayedLsn = GetXLogReplayRecPtr(NULL);

    while (XLByteLT(lastReplayedLsn, lastValidRecordLsn) && !DoEarlyExit()) {
        RedoInterruptCallBack();
        const long sleepTime = 100;
        pg_usleep(sleepTime);
        lastReplayedLsn = GetXLogReplayRecPtr(NULL);
    }
}

int ParallelXLogPageReadFile(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr,
                             TimeLineID *readTLI)
{
    bool randAccess = false;
    uint32 targetPageOff;
    volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
    XLogRecPtr RecPtr = targetPagePtr;
    uint32 ret;
#ifdef USE_ASSERT_CHECKING
    XLogSegNo targetSegNo;

    XLByteToSeg(targetPagePtr, targetSegNo);
#endif
    targetPageOff = targetPagePtr % XLogSegSize;

    /*
     * See if we need to switch to a new segment because the requested record
     * is not in the currently open one.
     */
    if (t_thrd.xlog_cxt.readFile >= 0 && !XLByteInSeg(targetPagePtr, t_thrd.xlog_cxt.readSegNo)) {
        close(t_thrd.xlog_cxt.readFile);
        t_thrd.xlog_cxt.readFile = -1;
        t_thrd.xlog_cxt.readSource = 0;
    }

    XLByteToSeg(targetPagePtr, t_thrd.xlog_cxt.readSegNo);
    XLByteAdvance(RecPtr, reqLen);

retry:
    /* See if we need to retrieve more data */
    if (t_thrd.xlog_cxt.readFile < 0) {
        if (t_thrd.xlog_cxt.StandbyMode) {
            /*
             * In standby mode, wait for the requested record to become
             * available, either via restore_command succeeding to restore the
             * segment, or via walreceiver having streamed the record.
             */
            for (;;) {
                RedoInterruptCallBack();
                if (t_thrd.xlog_cxt.readFile >= 0) {
                    close(t_thrd.xlog_cxt.readFile);
                    t_thrd.xlog_cxt.readFile = -1;
                }
                /* Reset curFileTLI if random fetch. */
                if (randAccess) {
                    t_thrd.xlog_cxt.curFileTLI = 0;
                }

                /*
                 * Try to restore the file from archive, or read an
                 * existing file from pg_xlog.
                 */
                uint32 sources = XLOG_FROM_ARCHIVE | XLOG_FROM_PG_XLOG;
                if (!(sources & ~t_thrd.xlog_cxt.failedSources)) {
                    /*
                     * We've exhausted all options for retrieving the
                     * file. Retry.
                     */
                    t_thrd.xlog_cxt.failedSources = 0;

                    /*
                     * Before we sleep, re-scan for possible new timelines
                     * if we were requested to recover to the latest
                     * timeline.
                     */
                    if (t_thrd.xlog_cxt.recoveryTargetIsLatest) {
                        if (rescanLatestTimeLine()) {
                            continue;
                        }
                    }

                    PushToWorkerLsn(true);
                    WaitReplayFinishAfterReadXlogFileComplete(t_thrd.xlog_cxt.EndRecPtr);

                    if (!xlogctl->IsRecoveryDone) {
                        g_instance.comm_cxt.predo_cxt.redoPf.redo_done_time = GetCurrentTimestamp();
                        g_instance.comm_cxt.predo_cxt.redoPf.recovery_done_ptr = t_thrd.xlog_cxt.ReadRecPtr;
                    }

                    XLogRecPtr lastReplayedLsn = GetXLogReplayRecPtr(NULL);
                    ereport(LOG,
                            (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                             errmsg("ParallelXLogPageReadFile IsRecoveryDone is %s set true,"
                                    "ReadRecPtr:%X/%X, EndRecPtr:%X/%X, lastreplayed:%X/%X",
                                    xlogctl->IsRecoveryDone ? "next" : "first",
                                    (uint32)(t_thrd.xlog_cxt.ReadRecPtr >> 32), (uint32)(t_thrd.xlog_cxt.ReadRecPtr),
                                    (uint32)(t_thrd.xlog_cxt.EndRecPtr >> 32), (uint32)(t_thrd.xlog_cxt.EndRecPtr),
                                    (uint32)(lastReplayedLsn >> 32), (uint32)(lastReplayedLsn))));

                    /*
                     * signal postmaster to update local redo end
                     * point to gaussdb state file.
                     */
                    if (!xlogctl->IsRecoveryDone) {
                        SendPostmasterSignal(PMSIGNAL_LOCAL_RECOVERY_DONE);
                    }

                    SpinLockAcquire(&xlogctl->info_lck);
                    xlogctl->IsRecoveryDone = true;
                    SpinLockRelease(&xlogctl->info_lck);
                    if (!(IS_SHARED_STORAGE_MODE) ||
                        pg_atomic_read_u32(&t_thrd.walreceiverfuncs_cxt.WalRcv->rcvDoneFromShareStorage)) {
                        knl_g_set_redo_finish_status(REDO_FINISH_STATUS_LOCAL | REDO_FINISH_STATUS_CM);
                        ereport(LOG,
                                (errmodule(MOD_REDO), errcode(ERRCODE_LOG),
                                 errmsg("ParallelXLogPageReadFile set redo finish status,"
                                        "ReadRecPtr:%X/%X, EndRecPtr:%X/%X",
                                        (uint32)(t_thrd.xlog_cxt.ReadRecPtr >> 32),
                                        (uint32)(t_thrd.xlog_cxt.ReadRecPtr), (uint32)(t_thrd.xlog_cxt.EndRecPtr >> 32),
                                        (uint32)(t_thrd.xlog_cxt.EndRecPtr))));

                        /*
                         * If it hasn't been long since last attempt, sleep 1s to
                         * avoid busy-waiting.
                         */
                        pg_usleep(150000L);
                    }
                    /*
                     * If primary_conninfo is set, launch walreceiver to
                     * try to stream the missing WAL, before retrying to
                     * restore from archive/pg_xlog.
                     *
                     * If fetching_ckpt is TRUE, RecPtr points to the
                     * initial checkpoint location. In that case, we use
                     * RedoStartLSN as the streaming start position
                     * instead of RecPtr, so that when we later jump
                     * backwards to start redo at RedoStartLSN, we will
                     * have the logs streamed already.
                     */

                    uint32 trigger = pg_atomic_read_u32(&g_readManagerTriggerFlag);
                    if (trigger > 0) {
                        pg_atomic_write_u32(&g_readManagerTriggerFlag, TRIGGER_NORMAL);
                        goto triggered;
                    }

                    load_server_mode();
                    if (t_thrd.xlog_cxt.PrimaryConnInfo || t_thrd.xlog_cxt.server_mode == STANDBY_MODE) {
                        t_thrd.xlog_cxt.receivedUpto = 0;
                        uint32 failSouce = pg_atomic_read_u32(&g_dispatcher->rtoXlogBufState.failSource);

                        if (!(failSouce & XLOG_FROM_STREAM)) {
                            volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
                            SpinLockAcquire(&walrcv->mutex);
                            walrcv->receivedUpto = 0;
                            SpinLockRelease(&walrcv->mutex);
                            t_thrd.xlog_cxt.readSource = XLOG_FROM_STREAM;
                            t_thrd.xlog_cxt.XLogReceiptSource = XLOG_FROM_STREAM;
                            pg_atomic_write_u32(&g_dispatcher->rtoXlogBufState.readSource,
                                                XLOG_FROM_STREAM);
                            pg_atomic_write_u32(&g_dispatcher->rtoXlogBufState.waitRedoDone, 0);
                            return -1;
                        }
                    }
                }
                /* Don't try to read from a source that just failed */
                sources &= ~t_thrd.xlog_cxt.failedSources;
                t_thrd.xlog_cxt.readFile = XLogFileReadAnyTLI(t_thrd.xlog_cxt.readSegNo, DEBUG2, sources);
                if (t_thrd.xlog_cxt.readFile >= 0) {
                    break;
                }
                /*
                 * Nope, not found in archive and/or pg_xlog.:
                 */
                t_thrd.xlog_cxt.failedSources |= sources;

                /*
                 * Check to see if the trigger file exists. Note that we
                 * do this only after failure, so when you create the
                 * trigger file, we still finish replaying as much as we
                 * can from archive and pg_xlog before failover.
                 */
                uint32 trigger = pg_atomic_read_u32(&g_readManagerTriggerFlag);
                if (trigger > 0) {
                    pg_atomic_write_u32(&g_readManagerTriggerFlag, TRIGGER_NORMAL);
                    goto triggered;
                }
            }
        } else {
            /* In archive or crash recovery. */
            if (t_thrd.xlog_cxt.readFile < 0) {
                uint32 sources;

                /* Reset curFileTLI if random fetch. */
                if (randAccess) {
                    t_thrd.xlog_cxt.curFileTLI = 0;
                }

                sources = XLOG_FROM_PG_XLOG;
                if (t_thrd.xlog_cxt.InArchiveRecovery) {
                    sources |= XLOG_FROM_ARCHIVE;
                }

                t_thrd.xlog_cxt.readFile = XLogFileReadAnyTLI(t_thrd.xlog_cxt.readSegNo, LOG, sources);

                if (t_thrd.xlog_cxt.readFile < 0) {
                    return -1;
                }
            }
        }
    }

    /*
     * At this point, we have the right segment open and if we're streaming we
     * know the requested record is in it.
     */
    Assert(t_thrd.xlog_cxt.readFile != -1);

    /*
     * If the current segment is being streamed from master, calculate how
     * much of the current page we have received already. We know the
     * requested record has been received, but this is for the benefit of
     * future calls, to allow quick exit at the top of this function.
     */
    t_thrd.xlog_cxt.readLen = XLOG_BLCKSZ;

    /* Read the requested page */
    t_thrd.xlog_cxt.readOff = targetPageOff;

try_again:
    if (lseek(t_thrd.xlog_cxt.readFile, (off_t)t_thrd.xlog_cxt.readOff, SEEK_SET) < 0) {
        ereport(emode_for_corrupt_record(LOG, RecPtr),
                (errcode_for_file_access(),
                 errmsg("could not seek in log file %s to offset %u: %m",
                        XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, t_thrd.xlog_cxt.readSegNo),
                        t_thrd.xlog_cxt.readOff)));
        if (errno == EINTR) {
            errno = 0;
            pg_usleep(1000);
            goto try_again;
        }
        goto next_record_is_invalid;
    }
    pgstat_report_waitevent(WAIT_EVENT_WAL_READ);
    ret = read(t_thrd.xlog_cxt.readFile, xlogreader->readBuf, XLOG_BLCKSZ);
    pgstat_report_waitevent(WAIT_EVENT_END);
    if (ret != XLOG_BLCKSZ) {
        ereport(emode_for_corrupt_record(LOG, RecPtr),
                (errcode_for_file_access(),
                 errmsg("could not read from log file %s to offset %u: %m",
                        XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, t_thrd.xlog_cxt.readSegNo),
                        t_thrd.xlog_cxt.readOff)));
        if (errno == EINTR) {
            errno = 0;
            pg_usleep(1000);
            goto try_again;
        }
        goto next_record_is_invalid;
    }
    Assert(targetSegNo == t_thrd.xlog_cxt.readSegNo);
    Assert(targetPageOff == t_thrd.xlog_cxt.readOff);
    Assert((uint32)reqLen <= t_thrd.xlog_cxt.readLen);

    *readTLI = t_thrd.xlog_cxt.curFileTLI;

    return t_thrd.xlog_cxt.readLen;

next_record_is_invalid:
    t_thrd.xlog_cxt.failedSources |= t_thrd.xlog_cxt.readSource;

    if (t_thrd.xlog_cxt.readFile >= 0) {
        close(t_thrd.xlog_cxt.readFile);
    }
    t_thrd.xlog_cxt.readFile = -1;
    t_thrd.xlog_cxt.readLen = 0;
    t_thrd.xlog_cxt.readSource = 0;

    /* In standby-mode, keep trying */
    if (t_thrd.xlog_cxt.StandbyMode) {
        goto retry;
    } else {
        return -1;
    }

triggered:
    if (t_thrd.xlog_cxt.readFile >= 0) {
        close(t_thrd.xlog_cxt.readFile);
    }
    t_thrd.xlog_cxt.readFile = -1;
    t_thrd.xlog_cxt.readLen = 0;
    t_thrd.xlog_cxt.readSource = 0;
    t_thrd.xlog_cxt.recoveryTriggered = true;

    return -1;
}

int ParallelXLogPageRead(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr,
                         TimeLineID *readTLI, char* xlogPath)
{
    int readLen = -1;
    pg_atomic_write_u64(&g_dispatcher->rtoXlogBufState.targetRecPtr, targetRecPtr);
    xlogreader->readBuf = g_dispatcher->rtoXlogBufState.readBuf;

    for (;;) {
        uint32 readSource = pg_atomic_read_u32(&(g_recordbuffer->readSource));
        if (readSource & XLOG_FROM_STREAM && !SS_DISASTER_STANDBY_CLUSTER) {
            readLen = ParallelXLogReadWorkBufRead(xlogreader, targetPagePtr, reqLen, targetRecPtr, readTLI);
        } else {
            if (ENABLE_DMS && ENABLE_DSS) {
                if (SS_DORADO_CLUSTER) {
                    readLen = SSXLogPageRead(xlogreader, targetPagePtr, reqLen, targetRecPtr,
                        xlogreader->readBuf, readTLI, xlogPath);
                } else {
                    readLen = SSXLogPageRead(xlogreader, targetPagePtr, reqLen, targetRecPtr,
                        xlogreader->readBuf, readTLI, NULL);
                }
            } else {
                readLen = ParallelXLogPageReadFile(xlogreader, targetPagePtr, reqLen, targetRecPtr, readTLI);
            }
        }
        
        /* current path haven't xlog file for this xlog */
        if (SS_DORADO_CLUSTER && readLen < 0) {
            return -1;
        }

        if (readLen > 0 || t_thrd.xlog_cxt.recoveryTriggered || !t_thrd.xlog_cxt.StandbyMode || DoEarlyExit()) {
            return readLen;
        }

        RedoInterruptCallBack();
        ADD_ABNORMAL_POSITION(10);
    }

    return readLen;
}

int ParallelReadPageInternal(XLogReaderState *state, XLogRecPtr pageptr, int reqLen, char* xlogPath)
{
    int readLen;
    uint32 targetPageOff;
    XLogSegNo targetSegNo;
    XLogPageHeader hdr;

    Assert((pageptr % XLOG_BLCKSZ) == 0);

    XLByteToSeg(pageptr, targetSegNo);
    targetPageOff = (pageptr % XLogSegSize);

    /* check whether we have all the requested data already */
    if (targetSegNo == state->readSegNo && targetPageOff == state->readOff && reqLen < (int)state->readLen) {
        return state->readLen;
    }

    /*
     * First, read the requested data length, but at least a short page header
     * so that we can validate it.
     */
    readLen = ParallelXLogPageRead(state, pageptr, Max(reqLen, (int)SizeOfXLogShortPHD), state->currRecPtr,
                                   &state->readPageTLI, xlogPath);
    if (readLen < 0) {
        goto err;
    }

    Assert(readLen <= XLOG_BLCKSZ);

    /* Do we have enough data to check the header length? */
    if (readLen <= (int)SizeOfXLogShortPHD) {
        goto err;
    }

    Assert(readLen >= reqLen);

    hdr = (XLogPageHeader)state->readBuf;

    /* still not enough */
    if (readLen < (int)XLogPageHeaderSize(hdr)) {
        readLen = ParallelXLogPageRead(state, pageptr, XLogPageHeaderSize(hdr), state->currRecPtr,
                                       &state->readPageTLI, xlogPath);
        if (readLen < 0) {
            goto err;
        }
    }

    /*
     * Now that we know we have the full header, validate it.
     */
    if (!ValidXLogPageHeader(state, pageptr, hdr)) {
        goto err;
    }

    /* update read state information */
    state->readSegNo = targetSegNo;
    state->readOff = targetPageOff;
    state->readLen = readLen;

    return readLen;

err:
    XLogReaderInvalReadState(state);
    return -1;
}

XLogRecord *ParallelReadRecord(XLogReaderState *state, XLogRecPtr RecPtr, char **errormsg, char* xlogPath)
{
    XLogRecord *record = NULL;
    XLogRecPtr targetPagePtr;
    bool randAccess = false;
    uint32 len, total_len;
    uint32 targetRecOff;
    uint32 pageHeaderSize;
    bool gotheader = false;
    int readOff;
    errno_t errorno = EOK;

    /*
     * randAccess indicates whether to verify the previous-record pointer of
     * the record we're reading.  We only do this if we're reading
     * sequentially, which is what we initially assume.
     */
    randAccess = false;

    /* reset error state */
    *errormsg = NULL;
    state->errormsg_buf[0] = '\0';

    if (XLByteEQ(RecPtr, InvalidXLogRecPtr)) {
        /* No explicit start point; read the record after the one we just read */
        RecPtr = state->EndRecPtr;

        if (XLByteEQ(state->ReadRecPtr, InvalidXLogRecPtr))
            randAccess = true;

        /*
         * If at page start, we must skip over the page header using xrecoff check.
         */
        if (0 == RecPtr % XLogSegSize) {
            XLByteAdvance(RecPtr, SizeOfXLogLongPHD);
        } else if (0 == RecPtr % XLOG_BLCKSZ) {
            XLByteAdvance(RecPtr, SizeOfXLogShortPHD);
        }
    } else {
        /*
         * Caller supplied a position to start at.
         *
         * In this case, the passed-in record pointer should already be
         * pointing to a valid record starting position.
         */
        Assert(XRecOffIsValid(RecPtr));
        randAccess = true;
    }

    state->currRecPtr = RecPtr;

    targetPagePtr = RecPtr - RecPtr % XLOG_BLCKSZ;
    targetRecOff = RecPtr % XLOG_BLCKSZ;

    /*
     * Read the page containing the record into state->readBuf. Request
     * enough byte to cover the whole record header, or at least the part of
     * it that fits on the same page.
     */
    readOff = ParallelReadPageInternal(state, targetPagePtr, Min(targetRecOff + SizeOfXLogRecord, XLOG_BLCKSZ),
                                       xlogPath);
    if (readOff < 0) {
        report_invalid_record(state, "read xlog page failed at %X/%X", (uint32)(RecPtr >> 32), (uint32)RecPtr);
        goto err;
    }

    /*
     * ReadPageInternal always returns at least the page header, so we can
     * examine it now.
     */
    pageHeaderSize = XLogPageHeaderSize((XLogPageHeader)state->readBuf);
    if (targetRecOff == 0) {
        /*
         * At page start, so skip over page header.
         */
        RecPtr += pageHeaderSize;
        targetRecOff = pageHeaderSize;
    } else if (targetRecOff < pageHeaderSize) {
        report_invalid_record(state, "invalid record offset at %X/%X", (uint32)(RecPtr >> 32), (uint32)RecPtr);
        goto err;
    }

    if ((((XLogPageHeader)state->readBuf)->xlp_info & XLP_FIRST_IS_CONTRECORD) && targetRecOff == pageHeaderSize) {
        report_invalid_record(state, "contrecord is requested by %X/%X", (uint32)(RecPtr >> 32), (uint32)RecPtr);
        goto err;
    }

    /* ReadPageInternal has verified the page header */
    Assert((int)pageHeaderSize <= readOff);

    /*
     * Read the record length.
     *
     * NB: Even though we use an XLogRecord pointer here, the whole record
     * header might not fit on this page. xl_tot_len is the first field of the
     * struct, so it must be on this page (the records are MAXALIGNed), but we
     * cannot access any other fields until we've verified that we got the
     * whole header.
     */
    record = (XLogRecord *)(state->readBuf + RecPtr % XLOG_BLCKSZ);
    total_len = record->xl_tot_len;

    /*
     * If the whole record header is on this page, validate it immediately.
     * Otherwise do just a basic sanity check on xl_tot_len, and validate the
     * rest of the header after reading it from the next page.  The xl_tot_len
     * check is necessary here to ensure that we enter the "Need to reassemble
     * record" code path below; otherwise we might fail to apply
     * ValidXLogRecordHeader at all.
     */
    if (targetRecOff <= XLOG_BLCKSZ - SizeOfXLogRecord) {
        if (!ValidXLogRecordHeader(state, RecPtr, state->ReadRecPtr, record, randAccess))
            goto err;
        gotheader = true;
    } else {
        /* more validation should be done here */
        if (total_len < SizeOfXLogRecord || total_len >= XLogRecordMaxSize) {
            report_invalid_record(state, "invalid record length at %X/%X: wanted %u, got %u", (uint32)(RecPtr >> 32),
                                  (uint32)RecPtr, (uint32)(SizeOfXLogRecord),
                                  total_len);
            goto err;
        }
        gotheader = false;
    }

    /*
     * Enlarge readRecordBuf as needed.
     */
    if (total_len > state->readRecordBufSize && !allocate_recordbuf(state, total_len)) {
        /* We treat this as a "bogus data" condition */
        report_invalid_record(state, "record length %u at %X/%X too long", total_len, (uint32)(RecPtr >> 32),
                              (uint32)RecPtr);
        goto err;
    }

    len = XLOG_BLCKSZ - RecPtr % XLOG_BLCKSZ;
    if (total_len > len) {
        /* Need to reassemble record */
        char *contdata = NULL;
        XLogPageHeader pageHeader;
        char *buffer = NULL;
        uint32 gotlen;
        errno_t errorno = EOK;

        readOff = ParallelReadPageInternal(state, targetPagePtr, XLOG_BLCKSZ, xlogPath);
        if (readOff < 0) {
            goto err;
        }

        /* Copy the first fragment of the record from the first page. */
        errorno = memcpy_s(state->readRecordBuf, len, state->readBuf + RecPtr % XLOG_BLCKSZ, len);
        securec_check_c(errorno, "\0", "\0");
        buffer = state->readRecordBuf + len;
        gotlen = len;

        do {
            /* Calculate pointer to beginning of next page */
            XLByteAdvance(targetPagePtr, XLOG_BLCKSZ);

            /* Wait for the next page to become available */
            readOff = ParallelReadPageInternal(state, targetPagePtr,
                                               Min(total_len - gotlen + SizeOfXLogShortPHD, XLOG_BLCKSZ), xlogPath);
            if (readOff < 0)
                goto err;

            Assert((int)SizeOfXLogShortPHD <= readOff);

            /* Check that the continuation on next page looks valid */
            pageHeader = (XLogPageHeader)state->readBuf;
            if (!(pageHeader->xlp_info & XLP_FIRST_IS_CONTRECORD)) {
                report_invalid_record(state, "there is no contrecord flag at %X/%X", (uint32)(RecPtr >> 32),
                                      (uint32)RecPtr);
                goto err;
            }

            /*
             * Cross-check that xlp_rem_len agrees with how much of the record
             * we expect there to be left.
             */
            if (pageHeader->xlp_rem_len == 0 || total_len != (pageHeader->xlp_rem_len + gotlen)) {
                report_invalid_record(state, "invalid contrecord length %u at %X/%X", pageHeader->xlp_rem_len,
                                      (uint32)(RecPtr >> 32), (uint32)RecPtr);
                goto err;
            }

            /* Append the continuation from this page to the buffer */
            pageHeaderSize = XLogPageHeaderSize(pageHeader);
            if (readOff < (int)pageHeaderSize)
                readOff = ParallelReadPageInternal(state, targetPagePtr, pageHeaderSize, xlogPath);

            Assert((int)pageHeaderSize <= readOff);

            contdata = (char *)state->readBuf + pageHeaderSize;
            len = XLOG_BLCKSZ - pageHeaderSize;
            if (pageHeader->xlp_rem_len < len)
                len = pageHeader->xlp_rem_len;

            if (readOff < (int)(pageHeaderSize + len))
                readOff = ParallelReadPageInternal(state, targetPagePtr, pageHeaderSize + len, xlogPath);

            errorno = memcpy_s(buffer, total_len - gotlen, (char *)contdata, len);
            securec_check_c(errorno, "", "");
            buffer += len;
            gotlen += len;

            /* If we just reassembled the record header, validate it. */
            if (!gotheader) {
                record = (XLogRecord *)state->readRecordBuf;
                if (!ValidXLogRecordHeader(state, RecPtr, state->ReadRecPtr, record, randAccess))
                    goto err;
                gotheader = true;
            }
        } while (gotlen < total_len);

        Assert(gotheader);

        record = (XLogRecord *)state->readRecordBuf;
        if (!ValidXLogRecord(state, record, RecPtr))
            goto err;

        pageHeaderSize = XLogPageHeaderSize((XLogPageHeader)state->readBuf);
        state->ReadRecPtr = RecPtr;
        state->EndRecPtr = targetPagePtr;
        XLByteAdvance(state->EndRecPtr, (pageHeaderSize + MAXALIGN(pageHeader->xlp_rem_len)));
    } else {
        /* Wait for the record data to become available */
        readOff = ParallelReadPageInternal(state, targetPagePtr, Min(targetRecOff + total_len, XLOG_BLCKSZ), xlogPath);
        if (readOff < 0) {
            goto err;
        }

        /* Record does not cross a page boundary */
        if (!ValidXLogRecord(state, record, RecPtr))
            goto err;

        state->EndRecPtr = RecPtr;
        XLByteAdvance(state->EndRecPtr, MAXALIGN(total_len));

        state->ReadRecPtr = RecPtr;
        errorno = memcpy_s(state->readRecordBuf, total_len, record, total_len);
        securec_check_c(errorno, "\0", "\0");
        record = (XLogRecord *)state->readRecordBuf;
    }

    /*
     * Special processing if it's an XLOG SWITCH record
     */
    if (record->xl_rmid == RM_XLOG_ID && record->xl_info == XLOG_SWITCH) {
        /* Pretend it extends to end of segment */
        state->EndRecPtr += XLogSegSize - 1;
        state->EndRecPtr -= state->EndRecPtr % XLogSegSize;
    }

    return record;
err:

    /*
     * Invalidate the read state. We might read from a different source after
     * failure.
     */
    XLogReaderInvalReadState(state);

    if (state->errormsg_buf[0] != '\0')
        *errormsg = state->errormsg_buf;

    return NULL;
}

/*
* in ss dorado double cluster, we need read xlogpath ergodic，
* we will read xlog in path where last read success
*/
XLogRecord *SSExtremeXLogReadRecordFromAllNodes(XLogReaderState *state, XLogRecPtr RecPtr, char **errormsg)
{
    XLogRecord *record = NULL;
    errno_t errorno = 0;

    for (int i = 0; i < DMS_MAX_INSTANCE; i++) {
        if (g_instance.dms_cxt.SSRecoveryInfo.xlog_list[i][0] == '\0') {
            break;
        }
        char *curPath = g_instance.dms_cxt.SSRecoveryInfo.xlog_list[i];
        record = ParallelReadRecord(state, InvalidXLogRecPtr, errormsg, curPath);
        if (record != NULL) {
            /* read success, exchange index */
            if (i != 0) {
                /* read success, exchange index */
                char exPath[MAXPGPATH];
                errorno = snprintf_s(exPath, MAXPGPATH, MAXPGPATH - 1, curPath);
                securec_check_ss(errorno, "", "");
                errorno = snprintf_s(g_instance.dms_cxt.SSRecoveryInfo.xlog_list[i], MAXPGPATH, MAXPGPATH - 1,
                    g_instance.dms_cxt.SSRecoveryInfo.xlog_list[0]);
                securec_check_ss(errorno, "", "");
                errorno = snprintf_s(g_instance.dms_cxt.SSRecoveryInfo.xlog_list[0], MAXPGPATH, MAXPGPATH - 1, exPath);
                securec_check_ss(errorno, "", "");
            }
            break;
        } else {
            if (t_thrd.xlog_cxt.readFile >= 0) {
                close(t_thrd.xlog_cxt.readFile);
                t_thrd.xlog_cxt.readFile = -1;
            }

            /* If record which is read from file is NULL, when preReadStartPtr is not set InvalidXlogPreReadStartPtr
             * then exhchanging file, due to preread 64M now RecPtr < preReadStartPtr, so record still is got from
             * preReadBuf and record still is bad. Therefore, preReadStartPtr need to set InvalidXlogPreReadStartPtr
             * so that record is read from next file on disk instead of preReadBuf.
             */
            state->preReadStartPtr = InvalidXlogPreReadStartPtr;
        }
    }
    return record;
}

XLogRecord *XLogParallelReadNextRecord(XLogReaderState *xlogreader)
{
    XLogRecord *record = NULL;

    /* This is the first try to read this page. */
    t_thrd.xlog_cxt.failedSources = 0;
    for (;;) {
        char *errormsg = NULL;
        
        if (SS_DORADO_CLUSTER) {
            record = SSExtremeXLogReadRecordFromAllNodes(xlogreader, InvalidXLogRecPtr, &errormsg);
        } else {
            record = ParallelReadRecord(xlogreader, InvalidXLogRecPtr, &errormsg, NULL);
        }

        t_thrd.xlog_cxt.ReadRecPtr = xlogreader->ReadRecPtr;
        t_thrd.xlog_cxt.EndRecPtr = xlogreader->EndRecPtr;
        g_instance.comm_cxt.predo_cxt.redoPf.read_ptr = t_thrd.xlog_cxt.ReadRecPtr;

        if (record == NULL) {
            /*
             * We only end up here without a message when XLogPageRead() failed
             * - in that case we already logged something.
             * In StandbyMode that only happens if we have been triggered, so
             * we shouldn't loop anymore in that case.
             */
            if (errormsg != NULL)
                ereport(emode_for_corrupt_record(LOG, t_thrd.xlog_cxt.EndRecPtr),
                        (errmsg_internal("%s", errormsg) /* already translated */));
        }

        /*
         * Check page TLI is one of the expected values.
         */
        else if ((!timeLineInHistory(xlogreader->latestPageTLI, t_thrd.xlog_cxt.expectedTLIs)) &&
                 (!(g_instance.attr.attr_storage.IsRoachStandbyCluster && dummyStandbyMode))) {
            char fname[MAXFNAMELEN];
            XLogSegNo targetSegNo;
            int32 offset;
            errno_t errorno = EOK;

            XLByteToSeg(xlogreader->latestPagePtr, targetSegNo);
            offset = xlogreader->latestPagePtr % XLogSegSize;

            errorno = snprintf_s(fname, MAXFNAMELEN, MAXFNAMELEN - 1, "%08X%08X%08X", xlogreader->readPageTLI,
                                 (uint32)((targetSegNo) / XLogSegmentsPerXLogId),
                                 (uint32)((targetSegNo) % XLogSegmentsPerXLogId));
            securec_check_ss(errorno, "", "");

            ereport(emode_for_corrupt_record(LOG, t_thrd.xlog_cxt.EndRecPtr),
                    (errmsg("unexpected timeline ID %u in log segment %s, offset %u", xlogreader->latestPageTLI, fname,
                            offset)));
            record = NULL;
        }

        if (record != NULL) {
            /* Set up lastest valid record */
            latestValidRecord = t_thrd.xlog_cxt.ReadRecPtr;
            latestRecordCrc = record->xl_crc;
            latestRecordLen = record->xl_tot_len;
            ADD_ABNORMAL_POSITION(9);
            if (SS_DORADO_CLUSTER) {
                t_thrd.xlog_cxt.ssXlogReadFailedTimes = 0;
            }
            /* Great, got a record */
            return record;
        } else {
            if (SS_DORADO_CLUSTER) {
                t_thrd.xlog_cxt.ssXlogReadFailedTimes++;

                /* In SS_DISASTER_STANDBY_CLUSTER mode, loop back to retry. */
                xlogreader->preReadStartPtr = InvalidXlogPreReadStartPtr;
            }
           
            /* No valid record available from this source */
            t_thrd.xlog_cxt.failedSources |= t_thrd.xlog_cxt.readSource;

            if (t_thrd.xlog_cxt.readFile >= 0) {
                close(t_thrd.xlog_cxt.readFile);
                t_thrd.xlog_cxt.readFile = -1;
            }

            /*
             * If archive recovery was requested, but we were still doing
             * crash recovery, switch to archive recovery and retry using the
             * offline archive. We have now replayed all the valid WAL in
             * pg_xlog, so we are presumably now consistent.
             *
             * We require that there's at least some valid WAL present in
             * pg_xlog, however (!fetch_ckpt). We could recover using the WAL
             * from the archive, even if pg_xlog is completely empty, but we'd
             * have no idea how far we'd have to replay to reach consistency.
             * So err on the safe side and give up.
             */
            if (!t_thrd.xlog_cxt.InArchiveRecovery && t_thrd.xlog_cxt.ArchiveRecoveryRequested) {
                t_thrd.xlog_cxt.InArchiveRecovery = true;
                if (t_thrd.xlog_cxt.StandbyModeRequested)
                    t_thrd.xlog_cxt.StandbyMode = true;
                /* construct a minrecoverypoint, update LSN */
                UpdateMinrecoveryInAchive();
                /*
                 * Before we retry, reset lastSourceFailed and currentSource
                 * so that we will check the archive next.
                 */
                t_thrd.xlog_cxt.failedSources = 0;
                continue;
            }

            /* In standby mode, loop back to retry. Otherwise, give up. */
            if (t_thrd.xlog_cxt.StandbyMode && !t_thrd.xlog_cxt.recoveryTriggered && !DoEarlyExit())
                continue;
            else
                return NULL;
        }
    }
}

}  // namespace extreme_rto