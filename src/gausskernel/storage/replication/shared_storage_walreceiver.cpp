/*
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * shared_storage_walreceiver.cpp
 *
 * Description: This file contains the shared_storage-specific parts of walreceiver. It's
 * loaded as a dynamic module to avoid linking the main server binary with
 * obs.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/replication/shared_storage_walreceiver.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/time.h>

#include "replication/archive_walreceiver.h"
#include "libpq/libpq-int.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "nodes/pg_list.h"
#include "access/obs/obs_am.h"
#include "utils/timestamp.h"
#include "miscadmin.h"
#include "replication/walreceiver.h"
#include "replication/shared_storage_walreceiver.h"
#include "replication/slot.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/spin.h"
#include "utils/guc.h"
#include "pgxc/pgxc.h"

static const int READ_MSG_RATIO = 5;

bool SimpleCheckBlockheader(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, char *buff, int* size)
{
    int checkSize = 0;

    do {
        Size validSize = SimpleValidatePage(xlogreader, targetPagePtr, buff);
        if (validSize < XLOG_BLCKSZ) {
            checkSize += validSize;
            break;
        }
        targetPagePtr += XLOG_BLCKSZ;
        buff += XLOG_BLCKSZ;
        checkSize += XLOG_BLCKSZ;
    } while (checkSize < *size);

    if (checkSize < *size) {
        *size = checkSize;
    }

    return (checkSize > 0);
}

bool SharedStorageXlogReadCheck(XLogReaderState *xlogreader, XLogRecPtr readEnd, XLogRecPtr readPageStart,
    char *localBuff, int *readLen)
{
    int diffLen;
    Assert((readPageStart % XLOG_BLCKSZ) == 0);
    XLogRecPtr alignReadEnd = readPageStart - readPageStart % ShareStorageBufSize + ShareStorageBufSize;
    if (alignReadEnd > readEnd) {
        XLogRecPtr ActualCopyEnd = TYPEALIGN(XLOG_BLCKSZ, readEnd);
        diffLen = static_cast<int>(ActualCopyEnd - readPageStart);
    } else {
        diffLen = static_cast<int>(alignReadEnd - readPageStart);
    }
    bool readResult = false;
    int readBytes = ReadXlogFromShareStorage(readPageStart, localBuff,
                                             (int)TYPEALIGN(g_instance.xlog_cxt.shareStorageopCtl.blkSize, diffLen));
    if (readBytes > 0) {
        *readLen = (int)((XLogRecPtr)readBytes > (readEnd - readPageStart)) ? (readEnd - readPageStart) : readBytes;
        readResult = SimpleCheckBlockheader(xlogreader, readPageStart, localBuff, readLen);
    } else {
        *readLen = 0;
        ereport(FATAL,
                (errcode(ERRCODE_INVALID_STATUS),
                 errmsg("read zero length of xlog from shared storage startlsn : %lx, readlen :%d, inserthead :%lx",
                        readPageStart, diffLen, readEnd)));
    }
    return readResult;
}

bool shared_storage_xlog_read(int timeout, unsigned char *type, char **buffer, int *len, bool isStopping)
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    TimestampTz start_time = GetCurrentTimestamp();
    XLogRecPtr start_lsn = 0;

    uint32 disable_connection_node =
        pg_atomic_read_u32(&g_instance.comm_cxt.localinfo_cxt.need_disable_connection_node);
    uint32 shared_storage_recover_done =
        pg_atomic_read_u32(&t_thrd.walreceiverfuncs_cxt.WalRcv->rcvDoneFromShareStorage);
    if (disable_connection_node && shared_storage_recover_done) {
        return false;
    }

    SpinLockAcquire(&walrcv->mutex);
    if (walrcv->isFirstTimeAccessStorage) {
        start_lsn = walrcv->receiveStart;
        walrcv->isFirstTimeAccessStorage = false;
        SpinLockRelease(&walrcv->mutex);
    } else {
        SpinLockRelease(&walrcv->mutex);
        SpinLockAcquire(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
        // t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr will been updated in XLogWalRcvReceive()
        start_lsn = t_thrd.walreceiver_cxt.walRcvCtlBlock->receivePtr;
        SpinLockRelease(&t_thrd.walreceiver_cxt.walRcvCtlBlock->mutex);
    }

    /* check and adjust  */
    ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.shareStorageXLogCtl;
    ReadShareStorageCtlInfo(ctlInfo);

    if (XLByteLE(ctlInfo->insertHead, start_lsn)) {
        return false;
    }
    int read_len = 0;
    char *local_buff = t_thrd.libwalreceiver_cxt.shared_storage_read_buf;
    XLogRecPtr page_off = start_lsn % XLOG_BLCKSZ;
    XLogRecPtr page_lsn = start_lsn - page_off;
    XLogReaderState *xlogreader = t_thrd.libwalreceiver_cxt.xlogreader;

    do {
        read_len = 0;
        if (XLByteLT(page_lsn, ctlInfo->insertTail)) { /* maybe recyle by primary master */
            ha_set_rebuild_connerror(WALSEGMENT_REBUILD, REPL_INFO_ERROR);
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_STATUS), errmsg("xlog in shared storage is overwriten by primary node")));
        }

        ReadShareStorageCtlInfo(ctlInfo);
        uint32 lastTerm = pg_atomic_read_u32(&t_thrd.walreceiverfuncs_cxt.WalRcv->shareStorageTerm);
        if (ctlInfo->term > lastTerm || ctlInfo->xlogFileSize != (uint64)g_instance.attr.attr_storage.xlog_file_size) {
            t_thrd.walreceiver_cxt.termChanged = true;
            if (isStopping)
                return false;
            else {
                ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                    errmsg("the term(%u:%u) or xlog file size(%lu:%lu) on shared storage is changed", ctlInfo->term,
                        lastTerm, ctlInfo->xlogFileSize, (uint64)g_instance.attr.attr_storage.xlog_file_size)));
            }
        }

        if (SharedStorageXlogReadCheck(xlogreader, ctlInfo->insertHead, page_lsn, local_buff, &read_len)) {
            break;
        }

        uint32 disable_connection_node =
            pg_atomic_read_u32(&g_instance.comm_cxt.localinfo_cxt.need_disable_connection_node);
        if (!disable_connection_node) {
            const long sleeptime = 1000;
            pg_usleep(sleeptime);  // 10ms
        }
    } while (ComputeTimeStamp(start_time) < timeout);
    if (read_len <= 0 || read_len <= static_cast<int>(page_off)) {
        uint32 disable_connection_node =
            pg_atomic_read_u32(&g_instance.comm_cxt.localinfo_cxt.need_disable_connection_node);
        if (disable_connection_node)
            pg_atomic_write_u32(&t_thrd.walreceiverfuncs_cxt.WalRcv->rcvDoneFromShareStorage, true);
        return false;
    }

    pg_atomic_write_u32(&t_thrd.walreceiverfuncs_cxt.WalRcv->rcvDoneFromShareStorage, false);

    int dataLen = read_len - page_off;
    WalDataMessageHeader *msgHdr = (WalDataMessageHeader *)(local_buff + page_off - sizeof(WalDataMessageHeader));

    msgHdr->dataStart = start_lsn;
    msgHdr->walEnd = InvalidXLogRecPtr;
    msgHdr->sendTime = GetCurrentTimestamp();
    msgHdr->catchup = false;

    *len = sizeof(WalDataMessageHeader) + dataLen;
    *type = 'w';
    *buffer = (char *)msgHdr;
    const uint32 shiftSize = 32;
    ereport(u_sess->attr.attr_storage.HaModuleDebug ? LOG : DEBUG2,
            (errmsg("[shared_storage_xlog_read]get xlog startlsn %08X/%08X, len %X\n", (uint32)(start_lsn >> shiftSize),
                    (uint32)start_lsn, (uint32)dataLen)));
    return true;
}

void shared_storage_xlog_check_consistency()
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    XLogRecPtr localRec;
    pg_crc32 localRecCrc = 0;
    uint32 recordLen;
    SpinLockAcquire(&walrcv->mutex);
    localRecCrc = walrcv->latestRecordCrc;
    localRec = walrcv->latestValidRecord;
    recordLen = walrcv->latestRecordLen;
    SpinLockRelease(&walrcv->mutex);

    ShareStorageXLogCtl *sharedStorageCtl = g_instance.xlog_cxt.shareStorageXLogCtl;

    ReadShareStorageCtlInfo(sharedStorageCtl);

    if (sharedStorageCtl->systemIdentifier != GetSystemIdentifier()) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_STATUS),
                          errmsg("database system version is different between dorado and local"),
                          errdetail("The dorado's system version is %lu, the local's system version is %lu.",
                                    sharedStorageCtl->systemIdentifier, GetSystemIdentifier())));
        ShareStorageSetBuildErrorAndExit(SYSTEMID_REBUILD, false);
        return;
    }

    if (XLByteLT(localRec, sharedStorageCtl->insertTail)) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_STATUS),
                          errmsg("standby's local request lsn[%lx], shared storage insert Tail[%lx]", localRec,
                                 sharedStorageCtl->insertTail)));
        ShareStorageSetBuildErrorAndExit(WALSEGMENT_REBUILD, false);
        return;
    }

    XLogReaderState *xlogReader = XLogReaderAllocate(&SharedStorageXLogPageRead, NULL,
                                                     g_instance.xlog_cxt.shareStorageopCtl.blkSize);
    if (xlogReader == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"),
                        errdetail("shared_storage_xlog_check_consistency failed allocate an XLog reader")));
    }
    char *errormsg = NULL;

    if (XLByteLT(localRec + recordLen, sharedStorageCtl->insertHead)) {
        XLogRecord *record = XLogReadRecord(xlogReader, localRec, &errormsg, false);  // don't need decode
        if (record == NULL) {
            ereport(WARNING, (errcode(ERRCODE_INVALID_STATUS),
                              errmsg("standby's local request lsn[%lx] is not valid record", localRec)));
            ShareStorageSetBuildErrorAndExit(WALSEGMENT_REBUILD, false);
            if (xlogReader != NULL) {
                XLogReaderFree(xlogReader);
                xlogReader = NULL;
            }
            return;
        }

        if (localRecCrc != record->xl_crc || recordLen != record->xl_tot_len) {
            ereport(WARNING, (errcode(ERRCODE_INVALID_STATUS),
                              errmsg("standby's local request lsn[%lx] 's crc mismatched with shared storage"
                                     "crc(local, share):[%u,%u].",
                                     localRec, localRecCrc, record->xl_crc)));
            ShareStorageSetBuildErrorAndExit(WALSEGMENT_REBUILD, false);
            if (xlogReader != NULL) {
                XLogReaderFree(xlogReader);
                xlogReader = NULL;
            }
            return;
        }
        t_thrd.walreceiver_cxt.checkConsistencyOK = true;
    } else {
        ereport(WARNING, (errcode(ERRCODE_INVALID_STATUS),
                          errmsg("standby's local request lsn[%lx], shared storage inserthead[%lx], recordlen[%x]",
                                 localRec, sharedStorageCtl->insertHead, recordLen)));

        uint32 shiftSize = 32;
        XLogRecPtr shareStorageLatestRecordStart = InvalidXLogRecPtr;
        int shareStorageLatestRecordLen;
        pg_crc32 shareStorageLatestRecordCrc;
        FindLastRecordCheckInfoOnShareStorage(&shareStorageLatestRecordStart, &shareStorageLatestRecordCrc,
            &shareStorageLatestRecordLen);
        if (shareStorageLatestRecordStart == InvalidXLogRecPtr ||
            XLByteLT(sharedStorageCtl->insertHead, shareStorageLatestRecordStart + shareStorageLatestRecordLen)) {
            ShareStorageSetBuildErrorAndExit(NONE_REBUILD);
            if (xlogReader != NULL) {
                XLogReaderFree(xlogReader);
                xlogReader = NULL;
            }
            return;
        }

        bool crcValid = false;
        pg_crc32 localCheckCrc = GetXlogRecordCrc(shareStorageLatestRecordStart, crcValid, XLogPageRead, 0);
        if (shareStorageLatestRecordCrc != localCheckCrc) {
            ereport(WARNING, (errmsg("shared storage request lsn[%X/%X]'s crc mismatched (share, local):[%u,%u].",
                static_cast<uint32>(shareStorageLatestRecordStart >> shiftSize),
                static_cast<uint32>(shareStorageLatestRecordStart), shareStorageLatestRecordCrc, localCheckCrc)));
            ShareStorageSetBuildErrorAndExit(WALSEGMENT_REBUILD, false);
            if (xlogReader != NULL) {
                XLogReaderFree(xlogReader);
                xlogReader = NULL;
            }
            return;
        }

        ShareStorageSetBuildErrorAndExit(NONE_REBUILD);
    }
    pg_atomic_write_u32(&t_thrd.walreceiverfuncs_cxt.WalRcv->shareStorageTerm, sharedStorageCtl->term);

    if (xlogReader != NULL) {
        XLogReaderFree(xlogReader);
        xlogReader = NULL;
    }

    return;
}

bool try_connect_libpq(LibpqrcvConnectParam *param, bool printError)
{
    bool libpgConnected = false;
    MemoryContext current_ctx = CurrentMemoryContext;
    static TimestampTz first_fail_time = 0;
    bool timeout = false;
    PG_TRY();
    {
        libpgConnected = libpqrcv_connect(param->conninfo, &param->startpoint, param->slotname,
                                          param->channel_identifier);
    }
    PG_CATCH();
    {
        (void)MemoryContextSwitchTo(current_ctx);

        ErrorData *edata = CopyErrorData();
        if (printError)
            ereport(LOG, (errmsg("Failed to  call try_connect_libpq: %s", edata->message)));

        FlushErrorState();

        FreeErrorData(edata);
        libpgConnected = false;
        libpqrcv_disconnect();
        t_thrd.libwalreceiver_cxt.streamConn = 0;
        if (first_fail_time != 0) {
            const int maxWaitTime = 5000;
            if (ComputeTimeStamp(first_fail_time) > maxWaitTime)
                timeout = true;
        } else {
            first_fail_time = GetCurrentTimestamp();
        }
    }
    PG_END_TRY();

    ereport(LOG, (errmsg("try_connect_libpq try to connect to %s, status %u, timeout %u.", param->conninfo,
                         libpgConnected, timeout)));
    if (timeout) {
        first_fail_time = 0;
        ereport(FATAL,
                (errcode(ERRCODE_CONNECTION_TIMED_OUT), errmsg("try_connect_libpq timeout restart walreceiver")));
    }

    if (libpgConnected)
        first_fail_time = 0;
    return libpgConnected;
}

bool shared_storage_connect(char *conninfo, XLogRecPtr *startpoint, char *slotname, int channel_identifier)
{
    bool libpgConnected = true;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    walrcv->peer_role = PRIMARY_MODE;
    walrcv->peer_state = NORMAL_STATE;
    walrcv->isFirstTimeAccessStorage = true;
    uint32 totalLen = sizeof(WalDataMessageHeader) + ShareStorageBufSize +
                      g_instance.xlog_cxt.shareStorageopCtl.blkSize;
    t_thrd.libwalreceiver_cxt.shared_storage_buf = (char *)palloc0(totalLen);
    t_thrd.libwalreceiver_cxt.shared_storage_read_buf =
        (char *)TYPEALIGN(g_instance.xlog_cxt.shareStorageopCtl.blkSize,
                          t_thrd.libwalreceiver_cxt.shared_storage_buf + sizeof(WalDataMessageHeader));
    t_thrd.libwalreceiver_cxt.connect_param.conninfo = conninfo;
    t_thrd.libwalreceiver_cxt.connect_param.startpoint = *startpoint;
    t_thrd.libwalreceiver_cxt.connect_param.slotname = slotname;
    t_thrd.libwalreceiver_cxt.connect_param.channel_identifier = channel_identifier;
    XLogReaderState *xlogreader = XLogReaderAllocate(SharedStorageXLogPageRead, 0,
                                                     g_instance.xlog_cxt.shareStorageopCtl.blkSize);
    if (xlogreader == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory"),
                        errdetail("Failed while allocating an XLog reading processor")));
    }
    xlogreader->system_identifier = GetSystemIdentifier();
    t_thrd.libwalreceiver_cxt.xlogreader = xlogreader;

    shared_storage_xlog_check_consistency();

    libpgConnected = try_connect_libpq(&t_thrd.libwalreceiver_cxt.connect_param, true);

    return libpgConnected;
}

bool shared_storage_receive(int timeout, unsigned char *type, char **buffer, int *len)
{
    const uint32 timeRatio = 2;
    static uint64 receiveNum = 0;
    static TimestampTz prev_time = 0;
    bool hasReadData = false;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    bool isStopping = (walrcv->walRcvState == WALRCV_STOPPING);
    SpinLockRelease(&walrcv->mutex);

    if (!isStopping) {
        if (t_thrd.libwalreceiver_cxt.streamConn) {
            hasReadData = libpqrcv_receive(0, type, buffer, len);
            if (hasReadData) {
                return true;
            }
        }

        if ((receiveNum++) % READ_MSG_RATIO == 0) {
            uint32 disable_connection_node =
                pg_atomic_read_u32(&g_instance.comm_cxt.localinfo_cxt.need_disable_connection_node);
            if ((!t_thrd.libwalreceiver_cxt.streamConn) && (!disable_connection_node)) {
                const int waitTime = 5000;
                if (prev_time == 0 || ComputeTimeStamp(prev_time) > waitTime) {
                    try_connect_libpq(&t_thrd.libwalreceiver_cxt.connect_param, true);
                    prev_time = GetCurrentTimestamp();
                }
            }
        }
    }

    return shared_storage_xlog_read(timeout / timeRatio, type, buffer, len, isStopping);
}

void shared_storage_send(const char *buffer, int nbytes)
{
    if (IS_SHARED_STORAGE_STANBY_MODE) {
        if (t_thrd.libwalreceiver_cxt.streamConn)
            libpqrcv_send(buffer, nbytes);
    }
}

void shared_storage_disconnect(void)
{
    libpqrcv_disconnect();
    if (t_thrd.libwalreceiver_cxt.shared_storage_buf != NULL) {
        pfree(t_thrd.libwalreceiver_cxt.shared_storage_buf);
        t_thrd.libwalreceiver_cxt.shared_storage_buf = NULL;
        t_thrd.libwalreceiver_cxt.shared_storage_read_buf = NULL;
    }

    if (t_thrd.libwalreceiver_cxt.xlogreader) {
        XLogReaderFree(t_thrd.libwalreceiver_cxt.xlogreader);
        t_thrd.libwalreceiver_cxt.xlogreader = 0;
    }
}
