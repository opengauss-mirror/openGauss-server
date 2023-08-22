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
 * xlog_share_storage.cpp
 *      do copy xlog from pg_xlog to share storage
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/xlog_share_storage/xlog_share_storage.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "gs_thread.h"
#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "replication/syncrep_gramparse.h"
#include "replication/walsender_private.h"
#include "storage/ipc.h"
#include "storage/file/fio_device.h"
#include "storage/dorado_operation/dorado_fd.h"
#include "storage/xlog_share_storage/xlog_share_storage.h"
#include "replication/shared_storage_walreceiver.h"
#include "replication/walreceiver.h"

static int copyFile = -1;
static TimeLineID copyFileTLI = 0;
static XLogSegNo copySegNo = 0;
static uint32 copyOff = 0;
const static short LOCK_LEN = 1024;
const static uint32 CHECK_LOCK_INTERVAL = 0xFF;
const static uint32 MAX_SIZE_CAN_COPY_TO_SHARE = 128 * 1024 * 1024;
const static uint32 PTR_PRINT_SHIFT_SIZE = 32;


static void SharedStorageXlogCopyBackendSigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.sharestoragexlogcopyer_cxt.got_SIGHUP = true;
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }
    errno = save_errno;
}

static void SharedStorageXlogCopyBackendShutdownHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.sharestoragexlogcopyer_cxt.shutdown_requested = true;

    if (t_thrd.proc)
        SetLatch(&t_thrd.proc->procLatch);

    errno = save_errno;
}

static void SharedStorageXlogCopyBackendQuickDie(SIGNAL_ARGS)
{
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);

    /*
     * We DO NOT want to run proc_exit() callbacks -- we're here because
     * shared memory may be corrupted, so we don't want to try to clean up our
     * transaction.  Just nail the windows shut and get out of town.  Now that
     * there's an atexit callback to prevent third-party code from breaking
     * things by calling exit() directly, we have to reset the callbacks
     * explicitly to make this work as intended.
     */
    on_exit_reset();

    /*
     * Note we do exit(2) not exit(0).	This is to force the postmaster into a
     * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
     * backend.  This is necessary precisely because we don't clean up our
     * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
     * should ensure the postmaster sees this as a crash, too, but no harm in
     * being doubly sure.)
     */
    exit(2);
}

static void SharedStorageXlogCopyBackendSigUsr1Handler(SIGNAL_ARGS)
{
    int saveErrno = errno;

    latch_sigusr1_handler();

    errno = saveErrno;
}

void LocalXLogRead(char *buf, XLogRecPtr startptr, Size count)
{
    char *p = NULL;
    XLogRecPtr recptr;
    Size nbytes;

    p = buf;
    recptr = startptr;
    nbytes = count;

    while (nbytes > 0) {
        uint32 startoff;
        int segbytes;
        int readbytes;

        startoff = recptr % XLogSegSize;

        /* Do we need to switch to a different xlog segment? */
        if (t_thrd.sharestoragexlogcopyer_cxt.readFile < 0 ||
            !XLByteInSeg(recptr, t_thrd.sharestoragexlogcopyer_cxt.readSegNo)) {
            char path[MAXPGPATH];

            if (t_thrd.sharestoragexlogcopyer_cxt.readFile >= 0) {
                (void)close(t_thrd.sharestoragexlogcopyer_cxt.readFile);
            }

            XLByteToSeg(recptr, t_thrd.sharestoragexlogcopyer_cxt.readSegNo);
            XLogFilePath(path, MAXPGPATH, t_thrd.xlog_cxt.ThisTimeLineID, t_thrd.sharestoragexlogcopyer_cxt.readSegNo);

            t_thrd.sharestoragexlogcopyer_cxt.readFile = BasicOpenFile(path, O_RDONLY | PG_BINARY, 0);
            if (t_thrd.sharestoragexlogcopyer_cxt.readFile < 0) {
                /*
                 * If the file is not found, assume it's because the standby
                 * asked for a too old WAL segment that has already been
                 * removed or recycled.
                 */
                if (errno == ENOENT) {
                    ereport(ERROR, (errcode_for_file_access(),
                                    errmsg("requested WAL segment %s has already been removed",
                                           XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID,
                                                         t_thrd.sharestoragexlogcopyer_cxt.readSegNo))));
                } else {
                    ereport(ERROR, (errcode_for_file_access(),
                                    errmsg("could not open file \"%s\" (log segment %s): %m", path,
                                           XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID,
                                                         t_thrd.sharestoragexlogcopyer_cxt.readSegNo))));
                }
            }
            t_thrd.sharestoragexlogcopyer_cxt.readOff = 0;
        }

        /* Need to seek in the file? */
        if (t_thrd.sharestoragexlogcopyer_cxt.readOff != startoff) {
            if (lseek(t_thrd.sharestoragexlogcopyer_cxt.readFile, (off_t)startoff, SEEK_SET) < 0) {
                (void)close(t_thrd.sharestoragexlogcopyer_cxt.readFile);
                t_thrd.sharestoragexlogcopyer_cxt.readFile = -1;
                ereport(ERROR,
                        (errcode_for_file_access(), errmsg("could not seek in log segment %s to offset %u: %m",
                                                           XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID,
                                                                         t_thrd.sharestoragexlogcopyer_cxt.readSegNo),
                                                           startoff)));
            }
            t_thrd.sharestoragexlogcopyer_cxt.readOff = startoff;
        }

        /* How many bytes are within this segment? */
        if (nbytes > (XLogSegSize - startoff)) {
            segbytes = XLogSegSize - startoff;
        } else {
            segbytes = nbytes;
        }

        pgstat_report_waitevent(WAIT_EVENT_WAL_READ);
        readbytes = read(t_thrd.sharestoragexlogcopyer_cxt.readFile, p, segbytes);
        pgstat_report_waitevent(WAIT_EVENT_END);
        if (readbytes <= 0) {
            (void)close(t_thrd.sharestoragexlogcopyer_cxt.readFile);
            t_thrd.sharestoragexlogcopyer_cxt.readFile = -1;
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("could not read from log segment %s, offset %u, length %lu: %m",
                            XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, t_thrd.sharestoragexlogcopyer_cxt.readSegNo),
                            t_thrd.sharestoragexlogcopyer_cxt.readOff, INT2ULONG(segbytes))));
        }

        /* Update state for read */
        XLByteAdvance(recptr, readbytes);

        t_thrd.sharestoragexlogcopyer_cxt.readOff += readbytes;
        nbytes -= readbytes;
        p += readbytes;
    }
}

void NotifySyncWaiters(XLogRecPtr newPos)
{
    volatile WalSndCtlData *walsndctl = t_thrd.walsender_cxt.WalSndCtl;

    (void)LWLockAcquire(SyncRepLock, LW_EXCLUSIVE);
    /*
     * Set the lsn first so that when we wake backends they will release up to
     * this location.
     */
    if (XLByteLT(walsndctl->lsn[SYNC_REP_WAIT_RECEIVE], newPos)) {
        walsndctl->lsn[SYNC_REP_WAIT_RECEIVE] = newPos;
        (void)SyncRepWakeQueue(false, SYNC_REP_WAIT_RECEIVE);
    }
    if (XLByteLT(walsndctl->lsn[SYNC_REP_WAIT_WRITE], newPos)) {
        walsndctl->lsn[SYNC_REP_WAIT_WRITE] = newPos;
        (void)SyncRepWakeQueue(false, SYNC_REP_WAIT_WRITE);
    }
    if (XLByteLT(walsndctl->lsn[SYNC_REP_WAIT_FLUSH], newPos)) {
        walsndctl->lsn[SYNC_REP_WAIT_FLUSH] = newPos;
        (void)SyncRepWakeQueue(false, SYNC_REP_WAIT_FLUSH);
    }

    LWLockRelease(SyncRepLock);
}

void PushCtlLsn(XLogRecPtr flushPtr)
{
    ShareStorageXLogCtl *sharestorageCtl = g_instance.xlog_cxt.shareStorageXLogCtl;
    sharestorageCtl->insertHead = flushPtr;
    if ((sharestorageCtl->insertHead - sharestorageCtl->insertTail) > sharestorageCtl->xlogFileSize) {
        sharestorageCtl->insertTail = sharestorageCtl->insertHead - sharestorageCtl->xlogFileSize;
    }
    sharestorageCtl->crc = CalShareStorageCtlInfoCrc(sharestorageCtl);
    UpdateShareStorageCtlInfo(sharestorageCtl);
    FsyncXlogToShareStorage();
    int mode = u_sess->attr.attr_storage.HaModuleDebug ? LOG : DEBUG2;
    const uint32 shftSz = 32;
    ereport(mode, (errmsg("[PushCtlLsn]insertHead lsn %08X/%08X\n", (uint32)(flushPtr >> shftSz), (uint32)flushPtr)));
    NotifySyncWaiters(flushPtr);
}

static inline int CalcWriteLen(XLogRecPtr startWrite, XLogRecPtr endPtr)
{
    Assert((startWrite % XLOG_BLCKSZ) == 0);
    XLogRecPtr alignWriteEnd = startWrite - startWrite % ShareStorageBufSize + ShareStorageBufSize;
    if (alignWriteEnd > endPtr) {
        XLogRecPtr ActualCopyEnd = TYPEALIGN(XLOG_BLCKSZ, endPtr);
        return static_cast<int>(ActualCopyEnd - startWrite);
    } else {
        return static_cast<int>(alignWriteEnd - startWrite);
    }
}

XLogRecPtr GetMaxPosCanOverWrite()
{
    if (!IsUnderPostmaster) {
        return MAX_XLOG_REC_PTR;
    }

    XLogRecPtr maxFlush = InvalidXLogRecPtr;
    for (int i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        volatile WalSnd *walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[i];
        SpinLockAcquire(&walsnd->mutex);
        if (walsnd->pid != 0 && walsnd->peer_role == STANDBY_CLUSTER_MODE) {
            if (XLByteLT(maxFlush, walsnd->flush)) {
                maxFlush = walsnd->flush;
            }
        }
        SpinLockRelease(&walsnd->mutex);
    }

    if (maxFlush == InvalidXLogRecPtr) {
        return MAX_XLOG_REC_PTR;
    }

    return g_instance.attr.attr_storage.xlog_file_size + maxFlush;
}

void AddXLogPageHeader(char* buf, XLogRecPtr startWrite, int writeLen, XLogRecPtr endPtr)
{
    int offset = 0;
    while (offset <  writeLen) {
        XLogPageHeader xlogPageHeader = (XLogPageHeader)(buf + offset);
        xlogPageHeader->xlp_total_len = XLOG_BLCKSZ;
        offset += XLOG_BLCKSZ;
    }

    if (XLByteLT(endPtr, startWrite + writeLen)) {
        XLogPageHeader xlogPageHeader = (XLogPageHeader)(buf + writeLen - XLOG_BLCKSZ);
        xlogPageHeader->xlp_total_len = endPtr % XLOG_BLCKSZ;
    }
}
void ShareStorageSleep()
{
    long maxSleepTime = 0;
    for (int i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        /* use volatile pointer to prevent code rearrangement */
        volatile WalSnd *walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[i];
        if (walsnd->pid != 0) {
            if (maxSleepTime < g_instance.rto_cxt.rto_standby_data[i].current_sleep_time)
                maxSleepTime = g_instance.rto_cxt.rto_standby_data[i].current_sleep_time;
        }
    }
    if (maxSleepTime > 0) {
        pgstat_report_waitevent(WAIT_EVENT_LOGCTRL_SLEEP);
        pg_usleep_retry(maxSleepTime, 0);
        pgstat_report_waitevent(WAIT_EVENT_END);
        ereport(DEBUG4, (errmodule(MOD_RTO_RPO), errmsg("ShareStorageSleep : %ld", maxSleepTime)));
    }
}

void DoXlogCopy(XLogRecPtr targetPtr)
{
    uint64 writeLength = 0;
    ShareStorageXLogCtl *sharestorageCtl = g_instance.xlog_cxt.shareStorageXLogCtl;
    Assert(targetPtr != InvalidXLogRecPtr);
    if (XLByteLE(targetPtr, sharestorageCtl->insertHead)) {
        return;
    }
    ShareStorageSleep();

    XLogRecPtr startWrite = sharestorageCtl->insertHead - (sharestorageCtl->insertHead % XLOG_BLCKSZ);
    while (XLByteLT(startWrite, targetPtr)) {
        int writeLen = CalcWriteLen(startWrite, targetPtr);
        LocalXLogRead(t_thrd.sharestoragexlogcopyer_cxt.buf, startWrite, static_cast<Size>(writeLen));
        AddXLogPageHeader(t_thrd.sharestoragexlogcopyer_cxt.buf, startWrite, writeLen, targetPtr);
        (void)WriteXlogToShareStorage(startWrite, t_thrd.sharestoragexlogcopyer_cxt.buf, writeLen);
        startWrite += writeLen;
        writeLength += writeLen;
        if ((writeLength >= XLogSegSize) || (XLByteLT(targetPtr, startWrite))) {
            PushCtlLsn(XLByteLE(startWrite, targetPtr) ? startWrite : targetPtr);
            writeLength = 0;
        }
    }

    if (writeLength > 0) {
        PushCtlLsn(targetPtr);
    }
}

static bool GetLock()
{
    if (g_instance.attr.attr_storage.xlog_lock_file_path == NULL) {
        return true;
    }

    if (LockNasWriteFile(g_instance.xlog_cxt.shareStorageLockFd)) {
        return true;
    }
    ereport(FATAL, (errmsg("could not lock lock file(%d) %s", g_instance.xlog_cxt.shareStorageLockFd,
        g_instance.attr.attr_storage.xlog_lock_file_path)));
    return false;
}

static bool ReleaseLock()
{
    if (g_instance.attr.attr_storage.xlog_lock_file_path == NULL) {
        return true;
    }

    if (UnlockNasWriteFile(g_instance.xlog_cxt.shareStorageLockFd)) {
        return true;
    }

    ereport(FATAL, (errmsg("could not unlock lock file(%d) %s", g_instance.xlog_cxt.shareStorageLockFd,
        g_instance.attr.attr_storage.xlog_lock_file_path)));
    return false;
}

bool CheckAndCopyXLog(bool forceCopy)
{
    ShareStorageXLogCtl *sharestorageCtl = g_instance.xlog_cxt.shareStorageXLogCtl;

    do {
        XLogRecPtr localFlush = InvalidXLogRecPtr;
        if (IS_SHARED_STORAGE_MAIN_STANDBY_MODE) {
            localFlush = GetFlushMainStandby();
        } else {
            localFlush = GetFlushRecPtr();
        }
        if (XLByteLE(localFlush, sharestorageCtl->insertHead)) {
            return true;
        }

        XLogRecPtr maxPosCanWrite = GetMaxPosCanOverWrite();
        if (XLByteLT(sharestorageCtl->insertHead, maxPosCanWrite)) {
            XLogRecPtr expectPos = XLByteLT(localFlush, maxPosCanWrite) ? localFlush : maxPosCanWrite;
            if ((expectPos - sharestorageCtl->insertHead) > MAX_SIZE_CAN_COPY_TO_SHARE) {
                expectPos = sharestorageCtl->insertHead + MAX_SIZE_CAN_COPY_TO_SHARE;
            }
            DoXlogCopy(expectPos);
        }
    } while (forceCopy && GetLock());

    return false;
}

void ShutdownShareStorageXLogCopy()
{
    if (g_instance.attr.attr_storage.xlog_file_path == NULL) {
        return;
    }

    if (t_thrd.sharestoragexlogcopyer_cxt.originBuf == NULL) {
        t_thrd.sharestoragexlogcopyer_cxt.originBuf =
            (char *)palloc(ShareStorageBufSize + g_instance.xlog_cxt.shareStorageopCtl.blkSize);
        t_thrd.sharestoragexlogcopyer_cxt.buf = (char *)TYPEALIGN(g_instance.xlog_cxt.shareStorageopCtl.blkSize,
                                                                  t_thrd.sharestoragexlogcopyer_cxt.originBuf);
    }

    ReadShareStorageCtlInfo(g_instance.xlog_cxt.shareStorageXLogCtl);
    CheckAndCopyXLog(true);
    if (t_thrd.sharestoragexlogcopyer_cxt.readFile >= 0) {
        (void)close(t_thrd.sharestoragexlogcopyer_cxt.readFile);
    }

    if (t_thrd.sharestoragexlogcopyer_cxt.originBuf != NULL) {
        pfree(t_thrd.sharestoragexlogcopyer_cxt.originBuf);
        t_thrd.sharestoragexlogcopyer_cxt.originBuf = NULL;
        t_thrd.sharestoragexlogcopyer_cxt.buf = NULL;
    }

    g_instance.proc_base->ShareStoragexlogCopyerLatch = NULL;
    ereport(LOG, (errmsg("stopped xlog copy at %X/%X",
        (uint32)(g_instance.xlog_cxt.shareStorageXLogCtl->insertHead >> PTR_PRINT_SHIFT_SIZE),
        (uint32)g_instance.xlog_cxt.shareStorageXLogCtl->insertHead)));
}

bool FileSizeCanUpdate()
{
    XLogRecPtr endCheckpointPtr = sizeof(CheckPointPlus) + t_thrd.shemem_ptr_cxt.ControlFile->checkPoint;
    ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.shareStorageXLogCtl;
    uint64 checkpointPos = endCheckpointPtr % ctlInfo->xlogFileSize;
    uint64 headPos = ctlInfo->insertHead % ctlInfo->xlogFileSize;
    if ((XLByteLE(endCheckpointPtr, ctlInfo->insertHead) && checkpointPos <= headPos) ||
        XLByteLT(ctlInfo->insertHead, endCheckpointPtr)) {
        return true;
    }

    return false;
}

void CheckShareStorageCtlInfo(XLogRecPtr localEnd)
{
    if (!IS_SHARED_STORAGE_MODE) {
        return;
    }

    ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.shareStorageXLogCtl;
    ReadShareStorageCtlInfo(ctlInfo);

    if (ctlInfo->systemIdentifier != GetSystemIdentifier()) {
        ereport(FATAL, (errmsg("database system version is different between shared storage %lu and local %lu",
                               ctlInfo->systemIdentifier, GetSystemIdentifier())));
    }

    uint32 shiftSize = 32;
    XLogRecPtr shareStorageLatestRecordStart = InvalidXLogRecPtr;
    int shareStorageLatestRecordLen;
    pg_crc32 shareStorageLatestRecordCrc;
    FindLastRecordCheckInfoOnShareStorage(&shareStorageLatestRecordStart, &shareStorageLatestRecordCrc,
                                          &shareStorageLatestRecordLen);

    Assert(XLByteLE(shareStorageLatestRecordStart + shareStorageLatestRecordLen, ctlInfo->insertHead));

    if (XLByteLT(localEnd, ctlInfo->insertHead)) {
        if (XLByteEQ(shareStorageLatestRecordStart, InvalidXLogRecPtr) ||
            !XLByteEQ(localEnd, shareStorageLatestRecordStart + MAXALIGN(shareStorageLatestRecordLen))) {
            ereport(FATAL, (errmsg("the local's head is smaller than The shared storage's head"),
                            errdetail("The shared storage's head %X/%X, the local's head is %X/%X. "
                                      "lastrecord on share storage: startlsn:%X/%X crc %u, len %d",
                                      static_cast<uint32>(ctlInfo->insertHead >> shiftSize),
                                      static_cast<uint32>(ctlInfo->insertHead),
                                      static_cast<uint32>(localEnd >> shiftSize), static_cast<uint32>(localEnd),
                                      static_cast<uint32>(shareStorageLatestRecordStart >> shiftSize),
                                      static_cast<uint32>(shareStorageLatestRecordStart), shareStorageLatestRecordCrc,
                                      shareStorageLatestRecordLen)));
        }

        ereport(LOG,
                (errmsg("modify share storage head from %X/%X to %X/%X",
                        static_cast<uint32>(ctlInfo->insertHead >> shiftSize), static_cast<uint32>(ctlInfo->insertHead),
                        static_cast<uint32>(localEnd >> shiftSize), static_cast<uint32>(localEnd))));
        ctlInfo->insertHead = localEnd;
        ctlInfo->crc = CalShareStorageCtlInfoCrc(ctlInfo);
        UpdateShareStorageCtlInfo(ctlInfo);
        FsyncXlogToShareStorage();
    } else {
        XLogRecPtr shareEnd = ctlInfo->insertHead;
        if (0 == shareEnd % XLogSegSize) {
            XLByteAdvance(shareEnd, SizeOfXLogLongPHD);
        } else if (0 == shareEnd % XLOG_BLCKSZ) {
            XLByteAdvance(shareEnd, SizeOfXLogShortPHD);
        }
        if (XLByteLT(shareEnd, localEnd)) {
            char path[MAXPGPATH];
            XLogSegNo sendSegNo;
            XLByteToSeg(ctlInfo->insertHead, sendSegNo);
            XLogFilePath(path, MAXPGPATH, t_thrd.xlog_cxt.ThisTimeLineID, sendSegNo);
            struct stat stat_buf;

            if (stat(path, &stat_buf) != 0) {
                ereport(FATAL, (errmsg("the local's tail is bigger than The shared storage's head %X/%X, path %s",
                                       static_cast<uint32>(ctlInfo->insertHead >> shiftSize),
                                       static_cast<uint32>(ctlInfo->insertHead), path)));
            }
        }
        bool crcValid = false;

        pg_crc32 localCheckCrc = GetXlogRecordCrc(shareStorageLatestRecordStart, crcValid, XLogPageRead, 0);
        if (shareStorageLatestRecordCrc != localCheckCrc) {
            ereport(FATAL, (errmsg("shared storage request lsn[%X/%X]'s crc mismatched (share, local):[%u,%u].",
                                   static_cast<uint32>(shareStorageLatestRecordStart >> shiftSize),
                                   static_cast<uint32>(shareStorageLatestRecordStart), shareStorageLatestRecordCrc,
                                   localCheckCrc)));
        }
    }

    uint32 localTerm = Max(g_instance.comm_cxt.localinfo_cxt.term_from_file,
                           g_instance.comm_cxt.localinfo_cxt.term_from_xlog);
    if (localTerm > ctlInfo->term) {
        ctlInfo->term = localTerm;
        ctlInfo->crc = CalShareStorageCtlInfoCrc(ctlInfo);
        UpdateShareStorageCtlInfo(ctlInfo);
        FsyncXlogToShareStorage();
    }
}

void UpdateShareStorageCtlInfo()
{
    bool changed = false;
    ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.shareStorageXLogCtl;
    ReadShareStorageCtlInfo(ctlInfo);

    if (ctlInfo->version != CURRENT_SHARE_STORAGE_CTL_VERSION) {
        ctlInfo->version = CURRENT_SHARE_STORAGE_CTL_VERSION;
        changed = true;
    }

    if (ctlInfo->length != SizeOfShareStorageXLogCtl) {
        ctlInfo->length = SizeOfShareStorageXLogCtl;
        changed = true;
    }

    if (ctlInfo->xlogFileSize != (uint64)g_instance.attr.attr_storage.xlog_file_size) {
        Assert(g_instance.xlog_cxt.shareStorageopCtl.xlogFileSize == ctlInfo->xlogFileSize);
        if (!FileSizeCanUpdate()) {
            ereport(FATAL, (errmsg("could not update share storage size."),
                            errdetail("current size:%lu, new size:%lu", ctlInfo->xlogFileSize,
                                      g_instance.attr.attr_storage.xlog_file_size)));
        }
        g_instance.xlog_cxt.shareStorageopCtl.xlogFileSize = g_instance.attr.attr_storage.xlog_file_size;
        ctlInfo->xlogFileSize = g_instance.xlog_cxt.shareStorageopCtl.xlogFileSize;
        changed = true;
    }

    if (changed) {
        ctlInfo->crc = CalShareStorageCtlInfoCrc(ctlInfo);
        UpdateShareStorageCtlInfo(ctlInfo);
    }
}

static void SharedStorageXlogCopyBackendQuitAndClean(int code, Datum arg)
{
    ereport(LOG, (errmsg("SharedStorageXlogCopyBackendMain unlock file(%d) %s", g_instance.xlog_cxt.shareStorageLockFd,
        g_instance.attr.attr_storage.xlog_lock_file_path)));
    ReleaseLock();
}

static void InitThreadSignal()
{
    (void)gspqsignal(SIGHUP, SharedStorageXlogCopyBackendSigHupHandler);    /* reload config file */
    (void)gspqsignal(SIGINT, SIG_IGN);                                      /* ignore query cancel */
    (void)gspqsignal(SIGTERM, SharedStorageXlogCopyBackendShutdownHandler); /* request shutdown */
    (void)gspqsignal(SIGQUIT, SharedStorageXlogCopyBackendQuickDie);        /* hard crash time */
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SharedStorageXlogCopyBackendSigUsr1Handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    /*
     * Reset some signals that are accepted by postmaster but not here
     */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);
    (void)gspqsignal(SIGURG, print_stack);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
}

void SharedStorageXlogCopyBackendMain(void)
{
    InitThreadSignal();
    if (IS_SHARED_STORAGE_MAIN_STANDBY_MODE) {
        GetWritePermissionSharedStorage();
    }
    if (RecoveryInProgress() && !IS_SHARED_STORAGE_MAIN_STANDBY_MODE) {
        ereport(LOG, (errmsg("stopped xlog copy in recovery")));
        proc_exit(0); /* done */
    }

    on_proc_exit(SharedStorageXlogCopyBackendQuitAndClean, 0);

    g_instance.proc_base->ShareStoragexlogCopyerLatch = &t_thrd.proc->procLatch;

    t_thrd.sharestoragexlogcopyer_cxt.originBuf =
        (char *)palloc(ShareStorageBufSize + g_instance.xlog_cxt.shareStorageopCtl.blkSize);
    t_thrd.sharestoragexlogcopyer_cxt.buf = (char *)TYPEALIGN(g_instance.xlog_cxt.shareStorageopCtl.blkSize,
                                                              t_thrd.sharestoragexlogcopyer_cxt.originBuf);
    UpdateShareStorageCtlInfo();
    ereport(LOG, (errmsg("start xlog copy at %X/%X",
        (uint32)(g_instance.xlog_cxt.shareStorageXLogCtl->insertHead >> PTR_PRINT_SHIFT_SIZE),
        (uint32)g_instance.xlog_cxt.shareStorageXLogCtl->insertHead)));

    if (IS_SHARED_STORAGE_MAIN_STANDBY_MODE) {
        t_thrd.xlog_cxt.ThisTimeLineID = GetRecoveryTargetTLI();
    } else {
        t_thrd.xlog_cxt.ThisTimeLineID = GetThisTargetTLI();
    }
    Assert(t_thrd.xlog_cxt.ThisTimeLineID != 0);

    uint64 checkLockCount = 0;
    pgstat_report_appname("xlog copy");
    pgstat_report_activity(STATE_IDLE, NULL);
    for (;;) {
        ResetLatch(&t_thrd.proc->procLatch);
        pgstat_report_activity(STATE_RUNNING, NULL);

        ++checkLockCount;
        if (checkLockCount & CHECK_LOCK_INTERVAL) {
            GetLock();
        }

        if (t_thrd.sharestoragexlogcopyer_cxt.shutdown_requested) {
            t_thrd.sharestoragexlogcopyer_cxt.shutdown_requested = false;
            g_instance.proc_base->ShareStoragexlogCopyerLatch = NULL;
            ShutdownShareStorageXLogCopy();
            proc_exit(0); /* done */
        }

        if (t_thrd.sharestoragexlogcopyer_cxt.got_SIGHUP) {
            t_thrd.sharestoragexlogcopyer_cxt.got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        bool noNewData = CheckAndCopyXLog(false);
        if (!noNewData) {
            // we have more data to copy, so not to sleep
            continue;
        }

        pgstat_report_activity(STATE_IDLE, NULL);
        const long sleepTime = 1000L;
        (void)WaitLatch(&t_thrd.proc->procLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, sleepTime);
    }

    g_instance.proc_base->ShareStoragexlogCopyerLatch = NULL;
    ereport(LOG, (errmsg("stopped xlog copy at %X/%X",
        (uint32)(g_instance.xlog_cxt.shareStorageXLogCtl->insertHead >> PTR_PRINT_SHIFT_SIZE),
        (uint32)g_instance.xlog_cxt.shareStorageXLogCtl->insertHead)));
    proc_exit(0);
}

void WakeUpXLogCopyerBackend()
{
    if (g_instance.proc_base->ShareStoragexlogCopyerLatch != NULL) {
        SetLatch(g_instance.proc_base->ShareStoragexlogCopyerLatch);
    }
}

void SSDoXLogCopyFromLocal(XLogRecPtr copyEnd)
{
    uint32 shiftSize = 32;
    ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.shareStorageXLogCtl;

    GetWritePermissionSharedStorage();
    ReadShareStorageCtlInfo(ctlInfo);
    if (XLByteLE(copyEnd, ctlInfo->insertHead)){
        return;
    }

    if (t_thrd.sharestoragexlogcopyer_cxt.originBuf == NULL) {
        t_thrd.sharestoragexlogcopyer_cxt.originBuf =
            (char *)palloc(ShareStorageBufSize + g_instance.xlog_cxt.shareStorageopCtl.blkSize);
        t_thrd.sharestoragexlogcopyer_cxt.buf = (char *)TYPEALIGN(g_instance.xlog_cxt.shareStorageopCtl.blkSize,
                                                                  t_thrd.sharestoragexlogcopyer_cxt.originBuf);
    }

    ereport(LOG,
            (errmsg("start to copy xlog from local to shared storage, start: %X/%X, end: %X/%X.",
                    static_cast<uint32>(ctlInfo->insertHead >> shiftSize), static_cast<uint32>(ctlInfo->insertHead),
                    static_cast<uint32>(copyEnd >> shiftSize), static_cast<uint32>(copyEnd))));

    DoXlogCopy(copyEnd);
    if (t_thrd.sharestoragexlogcopyer_cxt.readFile >= 0) {
        (void)close(t_thrd.sharestoragexlogcopyer_cxt.readFile);
    }
    if (t_thrd.sharestoragexlogcopyer_cxt.originBuf != NULL) {
        pfree(t_thrd.sharestoragexlogcopyer_cxt.originBuf);
        t_thrd.sharestoragexlogcopyer_cxt.originBuf = NULL;
        t_thrd.sharestoragexlogcopyer_cxt.buf = NULL;
    }

    ereport(LOG,
            (errmsg("successfully overwrite xlog from local to shared storage, shared storage's head is %X/%X",
                    static_cast<uint32>(ctlInfo->insertHead >> shiftSize), static_cast<uint32>(ctlInfo->insertHead))));
}

static void DoXLogCopyFromLocal(XLogRecPtr copyStart, XLogRecPtr copyEnd)
{
    uint32 shiftSize = 32;
    ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.shareStorageXLogCtl;
    uint64 xlogFileSize = static_cast<uint64>(g_instance.attr.attr_storage.xlog_file_size);

    GetWritePermissionSharedStorage();
    InitShareStorageCtlInfo(ctlInfo, ctlInfo->systemIdentifier);
    if (copyEnd - copyStart > xlogFileSize) {
        ctlInfo->insertHead = copyEnd - xlogFileSize;
    } else {
        ctlInfo->insertHead = copyStart;
    }
    ctlInfo->insertHead = ctlInfo->insertHead - ctlInfo->insertHead % XLogSegSize;
    ctlInfo->crc = CalShareStorageCtlInfoCrc(ctlInfo);
    UpdateShareStorageCtlInfo(ctlInfo);
    FsyncXlogToShareStorage();

    if (t_thrd.sharestoragexlogcopyer_cxt.originBuf == NULL) {
        t_thrd.sharestoragexlogcopyer_cxt.originBuf =
            (char *)palloc(ShareStorageBufSize + g_instance.xlog_cxt.shareStorageopCtl.blkSize);
        t_thrd.sharestoragexlogcopyer_cxt.buf = (char *)TYPEALIGN(g_instance.xlog_cxt.shareStorageopCtl.blkSize,
                                                                  t_thrd.sharestoragexlogcopyer_cxt.originBuf);
    }

    ereport(LOG,
            (errmsg("start to overwrite xlog from local to shared storage, start: %X/%X, end: %X/%X.",
                    static_cast<uint32>(ctlInfo->insertHead >> shiftSize), static_cast<uint32>(ctlInfo->insertHead),
                    static_cast<uint32>(copyEnd >> shiftSize), static_cast<uint32>(copyEnd))));

    DoXlogCopy(copyEnd);
    if (t_thrd.sharestoragexlogcopyer_cxt.readFile >= 0) {
        (void)close(t_thrd.sharestoragexlogcopyer_cxt.readFile);
    }
    if (t_thrd.sharestoragexlogcopyer_cxt.originBuf != NULL) {
        pfree(t_thrd.sharestoragexlogcopyer_cxt.originBuf);
        t_thrd.sharestoragexlogcopyer_cxt.originBuf = NULL;
        t_thrd.sharestoragexlogcopyer_cxt.buf = NULL;
    }

    ereport(LOG,
            (errmsg("successfully overwrite xlog from local to shared storage, shared storage's head is %X/%X",
                    static_cast<uint32>(ctlInfo->insertHead >> shiftSize), static_cast<uint32>(ctlInfo->insertHead))));
}

static XLogRecPtr GetMaxStartRecPtr(XLogRecPtr setStart, XLogRecPtr localStart, XLogRecPtr localEnd)
{
    XLogRecPtr res = InvalidXLogRecPtr;

    if (XLogRecPtrIsInvalid(setStart) || XLByteLT(localEnd, setStart)) {
        res = localStart;
    } else {
        res = XLByteLT(localStart, setStart) ? setStart : localStart;
    }

    return res;
}

bool XLogOverwriteFromLocal(bool force, XLogRecPtr setStart)
{
    ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.shareStorageXLogCtl;
    XLogRecPtr localMaxLSN = InvalidXLogRecPtr;
    XLogRecPtr localMinLSN = InvalidXLogRecPtr;
    XLogRecPtr localMaxEnd = InvalidXLogRecPtr;
    XLogRecPtr copyStart = InvalidXLogRecPtr;
    pg_crc32 localMaxLsnCrc = 0;
    pg_crc32 localMinLsnCrc = 0;
    uint32 localMaxLsnLen = 0;
    pg_crc32 sharedStorageRecCrc = 0;
    char maxLsnMsg[XLOG_READER_MAX_MSGLENTH] = {0};
    char minLsnMsg[XLOG_READER_MAX_MSGLENTH] = {0};
    TimeLineID tli = 0;
    bool crcValid = false;
    uint32 shiftSize = 32;
    log_min_messages = LOG;

    ReadShareStorageCtlInfo(ctlInfo);
    if (ctlInfo->systemIdentifier != GetSystemIdentifier()) {
        ereport(WARNING, (errmsg("database system version is different between shared storage and local"),
                         errdetail("The shared storage's system version is %lu, the local's system version is %lu.",
                                   ctlInfo->systemIdentifier, GetSystemIdentifier())));
        if (!force) {
            return false;
        }
    }

    localMaxLSN = FindMaxLSN(t_thrd.proc_cxt.DataDir, maxLsnMsg, XLOG_READER_MAX_MSGLENTH, &localMaxLsnCrc,
                             &localMaxLsnLen, &tli);
    if (XLogRecPtrIsInvalid(localMaxLSN)) {
        ereport(WARNING, (errmsg("find local max lsn fail: %s", maxLsnMsg)));
        return false;
    } else {
        ereport(LOG, (errmsg("find local max lsn success: %X/%X", static_cast<uint32>(localMaxLSN >> shiftSize),
                             static_cast<uint32>(localMaxLSN))));
    }
    localMaxEnd = MAXALIGN(localMaxLSN + localMaxLsnLen);

    localMinLSN = FindMinLSN(t_thrd.proc_cxt.DataDir, minLsnMsg, XLOG_READER_MAX_MSGLENTH, &localMinLsnCrc);
    if (XLogRecPtrIsInvalid(localMinLSN)) {
        ereport(WARNING, (errmsg("find local min lsn fail: %s", minLsnMsg)));
        return false;
    } else {
        ereport(LOG, (errmsg("find local min lsn success: %X/%X", static_cast<uint32>(localMinLSN >> shiftSize),
                             static_cast<uint32>(localMinLSN))));
    }

    t_thrd.xlog_cxt.ThisTimeLineID = tli;

    if (force == true && ctlInfo->systemIdentifier != GetSystemIdentifier()) {
        ctlInfo->systemIdentifier = GetSystemIdentifier();
        copyStart = GetMaxStartRecPtr(setStart, localMinLSN, localMaxEnd);
        DoXLogCopyFromLocal(copyStart, localMaxEnd);
        return true;
    }

    /* ctl info lsn may not be accurate */
    XLogRecPtr shareStorageLatestRecordStart = InvalidXLogRecPtr;
    XLogRecPtr shareStorageLatestRecordEnd = InvalidXLogRecPtr;
    int shareStorageLatestRecordLen = 0;
    pg_crc32 shareStorageLatestRecordCrc;
    FindLastRecordCheckInfoOnShareStorage(&shareStorageLatestRecordStart, &shareStorageLatestRecordCrc,
                                          &shareStorageLatestRecordLen);
    Assert(shareStorageLatestRecordStart != InvalidXLogRecPtr);

    shareStorageLatestRecordEnd = MAXALIGN(shareStorageLatestRecordStart + shareStorageLatestRecordLen);
    // check local maxLSN
    if (XLByteLE(localMaxEnd, shareStorageLatestRecordEnd)) {
        sharedStorageRecCrc = GetXlogRecordCrc(localMaxLSN, crcValid, SharedStorageXLogPageRead,
                                               g_instance.xlog_cxt.shareStorageopCtl.blkSize);
        ereport(
            LOG,
            (errmsg(
                "the shared storage's latestRecordEnd(%X/%X) is greater than or equal to the local maxEnd(%X/%X,"
                "crc[local, share]:[%u,%u]).",
                static_cast<uint32>(shareStorageLatestRecordEnd >> shiftSize),
                static_cast<uint32>(shareStorageLatestRecordEnd), static_cast<uint32>(localMaxEnd >> shiftSize),
                static_cast<uint32>(localMaxEnd), localMaxLsnCrc, sharedStorageRecCrc)));
        if (sharedStorageRecCrc == localMaxLsnCrc) {
            ctlInfo->insertHead = localMaxEnd;
            if (ctlInfo->insertHead > (ctlInfo->xlogFileSize + XLogSegSize)) {
                ctlInfo->insertTail = ctlInfo->insertHead - ctlInfo->xlogFileSize;
            } else {
                ctlInfo->insertTail = XLogSegSize;
            }
            ctlInfo->crc = CalShareStorageCtlInfoCrc(ctlInfo);
            UpdateShareStorageCtlInfo(ctlInfo);
            FsyncXlogToShareStorage();
            return true;
        }
    } else {
        pg_crc32 localCheckCrc = GetXlogRecordCrc(shareStorageLatestRecordStart, crcValid, XLogPageRead, 0);
        ereport(
            LOG,
            (errmsg("the shared storage's latestRecordEnd(%X/%X, crc[local, share]:[%u,%u]) is less than the local "
                    "maxEnd(%X/%X).",
                    static_cast<uint32>(shareStorageLatestRecordEnd >> shiftSize),
                    static_cast<uint32>(shareStorageLatestRecordEnd), localCheckCrc, shareStorageLatestRecordCrc,
                    static_cast<uint32>(localMaxEnd >> shiftSize), static_cast<uint32>(localMaxEnd))));
        if (shareStorageLatestRecordCrc == localCheckCrc) {
            copyStart = GetMaxStartRecPtr(setStart, shareStorageLatestRecordEnd, localMaxEnd);
            DoXLogCopyFromLocal(copyStart, localMaxEnd);
            return true;
        }
    }
    // check local minLSN
    sharedStorageRecCrc = GetXlogRecordCrc(localMinLSN, crcValid, SharedStorageXLogPageRead,
                                           g_instance.xlog_cxt.shareStorageopCtl.blkSize);
    if (sharedStorageRecCrc != localMinLsnCrc) {
        ereport(WARNING,
                (errmsg("the local minLsn(%X/%X) 's crc mismatched with shared storage, crc[local, share]:[%u,%u].",
                        (uint32)(localMinLSN >> shiftSize), (uint32)localMinLSN, localMinLsnCrc, sharedStorageRecCrc)));
    }
    copyStart = GetMaxStartRecPtr(setStart, localMinLSN, localMaxEnd);
    DoXLogCopyFromLocal(copyStart, localMaxEnd);

    return true;
}

static bool XLogCopyWrite(char *buf, int nbytes, XLogRecPtr recptr)
{
    uint64 startoff;
    int byteswritten;
    t_thrd.xlog_cxt.ThisTimeLineID = copyFileTLI;

    while (nbytes > 0) {
        int segbytes;

        if (copyFile < 0 || !XLByteInSeg(recptr, copySegNo)) {
            bool use_existent = false;

            if (copyFile >= 0) {
                if (close(copyFile) != 0) {
                    ereport(WARNING, (errcode_for_file_access(),
                        errmsg("could not close log file %s: %m", XLogFileNameP(copyFileTLI, copySegNo))));
                    return false;
                }
            }
            copyFile = -1;

            /* Create/use new log file */
            XLByteToSeg(recptr, copySegNo);
            use_existent = true;
            copyFile = XLogFileInit(copySegNo, &use_existent, true);
            copyOff = 0;
            ereport(LOG, (errmsg("create or open log file %s.", XLogFileNameP(copyFileTLI, copySegNo))));
        }
        /* Calculate the start offset of the logs */
        startoff = recptr % XLogSegSize;

        if (startoff + nbytes > XLogSegSize)
            segbytes = XLogSegSize - startoff;
        else
            segbytes = nbytes;

        /* Need to seek in the file? */
        if (copyOff != (uint32)startoff) {
            if (lseek(copyFile, (off_t)startoff, SEEK_SET) < 0) {
                ereport(WARNING, (errcode_for_file_access(), errmsg("could not seek in log file %s to offset %lu: %m",
                    XLogFileNameP(copyFileTLI, copySegNo), startoff)));
                return false;
            }
            copyOff = startoff;
        }

        /* OK to write the logs */
        errno = 0;

        byteswritten = write(copyFile, buf, segbytes);
        if (byteswritten <= 0) {
            /* if write didn't set errno, assume no disk space */
            if (errno == 0) {
                errno = ENOSPC;
            }
            ereport(WARNING, (errcode_for_file_access(),
                            errmsg("could not write to log file %s at offset %u, length %lu: %m",
                                   XLogFileNameP(copyFileTLI, copySegNo), copyOff, INT2ULONG(segbytes))));
            return false;
        }
        if (copyOff == (uint32)0 && segbytes >= (int)sizeof(XLogPageHeaderData)) {
            if (((XLogPageHeader)buf)->xlp_magic == XLOG_PAGE_MAGIC &&
                (copySegNo * XLogSegSize) != ((XLogPageHeader)buf)->xlp_pageaddr) {
                ereport(WARNING, (errcode_for_file_access(),
                                errmsg("unexpected page addr %lu of log file %s", ((XLogPageHeader)buf)->xlp_pageaddr,
                                       XLogFileNameP(copyFileTLI, copySegNo))));
                return false;
            }
        }
        /* Update state for write */
        XLByteAdvance(recptr, byteswritten);

        copyOff += byteswritten;
        nbytes -= byteswritten;
        buf += byteswritten;

        issue_xlog_fsync(copyFile, copySegNo);
    }

    return true;
}

static bool DoXLogCopyFromShare(XLogRecPtr copyStart)
{
    ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.shareStorageXLogCtl;
    char *bufOrigin = (char *)palloc(ShareStorageBufSize + g_instance.xlog_cxt.shareStorageopCtl.blkSize);
    char *buf = (char *)TYPEALIGN(g_instance.xlog_cxt.shareStorageopCtl.blkSize, bufOrigin);
    XLogReaderState *xlogreader = NULL;
    XLogRecPtr readPtr = copyStart;
    int read_len = 0;
    uint32 shiftSize = 32;

    ereport(LOG,
            (errmsg("start to overwrite xlog from shared storage to local, start: %X/%X, end: %X/%X.",
                    static_cast<uint32>(copyStart >> shiftSize), static_cast<uint32>(copyStart),
                    static_cast<uint32>(ctlInfo->insertHead >> shiftSize), static_cast<uint32>(ctlInfo->insertHead))));

    xlogreader = XLogReaderAllocate(SharedStorageXLogPageRead, 0, g_instance.xlog_cxt.shareStorageopCtl.blkSize);
    if (xlogreader == NULL) {
        ereport(WARNING, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"),
                        errdetail("Failed while allocating an XLog reading processor")));
        return false;
    }
    xlogreader->system_identifier = GetSystemIdentifier();

    while (XLByteLT(readPtr, ctlInfo->insertHead)) {
        // read xlog from shared storage to buf
        readPtr = readPtr - readPtr % XLOG_BLCKSZ;
        bool readResult = SharedStorageXlogReadCheck(xlogreader, ctlInfo->insertHead, readPtr, buf, &read_len);

        // write xlog from buf to local pg_xlog
        bool writeResult = XLogCopyWrite(buf, read_len, readPtr);
        if (!writeResult) {
            return false;
        }

        XLByteAdvance(readPtr, read_len);
        if (!readResult) {
            break;
        }
    }

    if (copyFile >= 0) {
        if (close(copyFile) != 0) {
            ereport(WARNING, (errcode_for_file_access(),
                            errmsg("could not close log file %s: %m", XLogFileNameP(copyFileTLI, copySegNo))));
            return false;
        }
    }
    copyFile = -1;
    pfree(bufOrigin);
    XLogReaderFree(xlogreader);

    ereport(LOG, (errmsg("successfully overwrite xlog from local to shared storage, readPtr is %X/%X",
                         static_cast<uint32>(readPtr >> shiftSize), static_cast<uint32>(readPtr))));
    return true;
}

bool XLogOverwriteFromShare()
{
    ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.shareStorageXLogCtl;
    XLogRecPtr localMaxLSN = InvalidXLogRecPtr;
    XLogRecPtr localMaxEnd = InvalidXLogRecPtr;
    pg_crc32 localMaxLsnCrc = 0;
    uint32 localMaxLsnLen = 0;
    pg_crc32 sharedStorageRecCrc = 0;
    char maxLsnMsg[XLOG_READER_MAX_MSGLENTH] = {0};
    bool crcValid = false;
    uint32 shiftSize = 32;
    log_min_messages = LOG;

    ReadShareStorageCtlInfo(ctlInfo);

    if (ctlInfo->systemIdentifier != GetSystemIdentifier()) {
        ereport(WARNING, (errmsg("database system version is different between shared storage and local"),
                        errdetail("The shared storage's system version is %lu, the local's system version is %lu.",
                                  ctlInfo->systemIdentifier, GetSystemIdentifier())));
        return false;
    }
    /* The local log is consistent with the local data and cannot be simply overwritten from the shared storage.
       If the verification fails, the build repair should be triggered */
    localMaxLSN = FindMaxLSN(t_thrd.proc_cxt.DataDir, maxLsnMsg, XLOG_READER_MAX_MSGLENTH, &localMaxLsnCrc,
                             &localMaxLsnLen, &copyFileTLI);
    if (XLogRecPtrIsInvalid(localMaxLSN)) {
        ereport(WARNING, (errmsg("find local max lsn fail: %s", maxLsnMsg)));
        return false;
    } else {
        ereport(LOG, (errmsg("find local max lsn success: %X/%X", static_cast<uint32>(localMaxLSN >> shiftSize),
                             static_cast<uint32>(localMaxLSN))));
    }
    localMaxEnd = MAXALIGN(localMaxLSN + localMaxLsnLen);
    if (XLByteLE(localMaxEnd, ctlInfo->insertHead)) {
        ereport(LOG,
                (errmsg("the shared storage's head(%X/%X) is greater than or equal to the local maxEnd(%X/%X).",
                        static_cast<uint32>(ctlInfo->insertHead >> shiftSize), static_cast<uint32>(ctlInfo->insertHead),
                        static_cast<uint32>(localMaxEnd >> shiftSize), static_cast<uint32>(localMaxEnd))));
        sharedStorageRecCrc = GetXlogRecordCrc(localMaxLSN, crcValid, SharedStorageXLogPageRead,
                                               g_instance.xlog_cxt.shareStorageopCtl.blkSize);
        if (sharedStorageRecCrc != localMaxLsnCrc) {
            ereport(WARNING, (errmsg("crc[local,share]:[%u,%u] mismatch.", localMaxLsnCrc, sharedStorageRecCrc)));
            return false;
        }
        return DoXLogCopyFromShare(localMaxEnd);
    } else {
        ereport(WARNING,
                (errmsg("the shared storage's head(%X/%X) is less than the local maxEnd(%X/%X).",
                        static_cast<uint32>(ctlInfo->insertHead >> shiftSize), static_cast<uint32>(ctlInfo->insertHead),
                        static_cast<uint32>(localMaxEnd >> shiftSize), static_cast<uint32>(localMaxEnd))));
        return false;
    }
}

Size CalShareStorageCtlSize()
{
    Size size = 0;
    if (g_instance.attr.attr_storage.xlog_file_path != NULL) {
        size = sizeof(ShareStorageXLogCtl) + MEMORY_ALIGNED_SIZE;
    }
    return size;
}

ShareStorageXLogCtl *AlignAllocShareStorageCtl()
{
    Assert(g_instance.attr.attr_storage.xlog_file_path != NULL);
    Assert(g_instance.xlog_cxt.shareStorageopCtl.blkSize != 0);
    uint32 blkSize = g_instance.xlog_cxt.shareStorageopCtl.blkSize;
    void *tmpBuf = palloc(sizeof(ShareStorageXLogCtlAllocBlock) + blkSize);
    ShareStorageXLogCtlAllocBlock *ctlInfo = (ShareStorageXLogCtlAllocBlock *)TYPEALIGN(blkSize, tmpBuf);
    ctlInfo->originPointer = tmpBuf;
    return &(ctlInfo->ctlInfo);
}

void AlignFreeShareStorageCtl(ShareStorageXLogCtl *ctlInfo)
{
    ShareStorageXLogCtlAllocBlock *ctlInfoBlock = reinterpret_cast<ShareStorageXLogCtlAllocBlock *>(ctlInfo);
    void *tmpBuf = ctlInfoBlock->originPointer;
    pfree(tmpBuf);
}

bool LockNasWriteFile(int fd)
{
    struct flock lock;
    lock.l_type = F_WRLCK;
    lock.l_start = 0;
    lock.l_whence = SEEK_SET;
    lock.l_len = LOCK_LEN;
    lock.l_pid = getpid();

    if (fcntl(fd, F_SETLK, &lock) == 0) {
        return true;
    }
    ereport(WARNING, (errcode_for_file_access(), errmsg("could not lock lock file : %m")));
    return false;
}

bool UnlockNasWriteFile(int fd)
{
    struct flock lock;
    lock.l_type = F_UNLCK;
    lock.l_start = 0;
    lock.l_whence = SEEK_SET;
    lock.l_len = LOCK_LEN;
    lock.l_pid = getpid();

    if (fcntl(fd, F_SETLKW, &lock) == 0) {
        return true;
    }
    ereport(WARNING, (errcode_for_file_access(), errmsg("could not unlock lock file : %m")));
    return false;
}

