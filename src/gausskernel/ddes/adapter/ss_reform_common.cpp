/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
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
 * ss_reform_common.cpp
 *  common methods for crash recovery, switchover and failover.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_init.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/xlog.h"
#include "access/multi_redo_api.h"
#include "postmaster/postmaster.h"
#include "storage/smgr/fd.h"
#include "storage/dss/fio_dss.h"
#include "ddes/dms/ss_dms.h"
#include "ddes/dms/ss_common_attr.h"
#include "ddes/dms/ss_dms_bufmgr.h"
#include "ddes/dms/ss_reform_common.h"
#include "storage/file/fio_device.h"
#include "storage/smgr/segment_internal.h"
#include "replication/walreceiver.h"

/*
 * Add xlog reader private structure for page read.
 */
typedef struct XLogPageReadPrivate {
    int emode;
    bool fetching_ckpt; /* are we fetching a checkpoint record? */
    bool randAccess;
} XLogPageReadPrivate;

int SSXLogFileReadAnyTLI(XLogSegNo segno, int emode, uint32 sources, char* xlog_path)
{
    char path[MAXPGPATH];
    ListCell *cell = NULL;
    int fd = -1;
    errno_t errorno = EOK;
    
    foreach (cell, t_thrd.xlog_cxt.expectedTLIs) {
        TimeLineID tli = (TimeLineID)lfirst_int(cell);
        if (tli < t_thrd.xlog_cxt.curFileTLI) {
            break; /* don't bother looking at too-old TLIs */
        }

        errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%08X%08X%08X", xlog_path, tli,
                             (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
        securec_check_ss(errorno, "", "");
        t_thrd.xlog_cxt.restoredFromArchive = false;

        fd = BasicOpenFile(path, O_RDONLY | PG_BINARY, 0);
        if (fd >= 0) {
            /* Success! */
            t_thrd.xlog_cxt.curFileTLI = tli;

            /* Track source of data in assorted state variables */
            t_thrd.xlog_cxt.readSource = sources;
            t_thrd.xlog_cxt.XLogReceiptSource = (int)sources;

            /* In FROM_STREAM case, caller tracks receipt time, not me */
            if (sources != XLOG_FROM_STREAM) {
                t_thrd.xlog_cxt.XLogReceiptTime = GetCurrentTimestamp();
            }

            return fd;
        }
        if (!FILE_POSSIBLY_DELETED(errno)) { 
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\" (log segment %s): %m", path,
                                                              XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno))));
        }
    }

    /* Couldn't find it.  For simplicity, complain about front timeline */
    errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%08X%08X%08X", SS_XLOGDIR,
                         t_thrd.xlog_cxt.recoveryTargetTLI, (uint32)((segno) / XLogSegmentsPerXLogId),
                         (uint32)((segno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    errno = ENOENT;
    ereport(emode, (errcode_for_file_access(), errmsg("could not open file \"%s\" (log segment %s): %m", path,
                                                      XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno)))); 

    return -1;
}

static int emode_for_corrupt_record(int emode, XLogRecPtr RecPtr)
{
    if (t_thrd.xlog_cxt.readSource == XLOG_FROM_PG_XLOG && emode == LOG) {
        if (XLByteEQ(RecPtr, t_thrd.xlog_cxt.lastComplaint)) {
            emode = DEBUG1;
        } else {
            t_thrd.xlog_cxt.lastComplaint = RecPtr;
        }
    }
    return emode;
}

bool SSReadXlogInternal(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, XLogRecPtr targetRecPtr, char *buf)
{
    uint32 preReadOff;
    XLogRecPtr xlogFlushPtrForPerRead = xlogreader->xlogFlushPtrForPerRead;
    bool isReadFile = true;

    do {
        /* 
         * That source is XLOG_FROM_STREAM indicate that walreceiver receive xlog and walrecwriter have wrriten xlog
         * into pg_xlog segment file in dss. There exists a condition which preReadBuf possibly is zero for some xlog
         * record just writing into pg_xlog file when source is XLOG_FROM_STREAM and dms and dss are enabled. So we
         * need to reread xlog from dss to preReadBuf.
         */
        if (SS_STANDBY_CLUSTER_MAIN_STANDBY) {
            volatile XLogCtlData *xlogctl = t_thrd.shemem_ptr_cxt.XLogCtl;
            if (XLByteInPreReadBuf(targetPagePtr, xlogreader->preReadStartPtr) && 
               ((targetRecPtr < xlogFlushPtrForPerRead && t_thrd.xlog_cxt.readSource == XLOG_FROM_STREAM) || 
               (!xlogctl->IsRecoveryDone) || (t_thrd.xlog_cxt.readSource != XLOG_FROM_STREAM))) {
                   isReadFile = false;
               }
        }

        if ((XLByteInPreReadBuf(targetPagePtr, xlogreader->preReadStartPtr) &&
             !SS_STANDBY_CLUSTER_MAIN_STANDBY) || (!isReadFile)) {
            preReadOff = targetPagePtr % XLogPreReadSize;
            int err = memcpy_s(buf, XLOG_BLCKSZ, xlogreader->preReadBuf + preReadOff, XLOG_BLCKSZ);
            securec_check(err, "\0", "\0");
            break;
        } else {
            if (SS_STANDBY_CLUSTER_MAIN_STANDBY) {
                xlogreader->xlogFlushPtrForPerRead = GetWalRcvWriteRecPtr(NULL);
                xlogFlushPtrForPerRead = xlogreader->xlogFlushPtrForPerRead;
            }
            // pre-reading for dss
            uint32 targetPageOff = targetPagePtr % XLogSegSize;
            preReadOff = targetPageOff - targetPageOff % XLogPreReadSize;
            ssize_t actualBytes = pread(t_thrd.xlog_cxt.readFile, xlogreader->preReadBuf, XLogPreReadSize, preReadOff);
            if (actualBytes != XLogPreReadSize) {
                return false;
            }
            xlogreader->preReadStartPtr = targetPagePtr + preReadOff - targetPageOff;
        }
    } while (true);

    return true;
}

XLogReaderState *SSXLogReaderAllocate(XLogPageReadCB pagereadfunc, void *private_data, Size alignedSize)
{
    XLogReaderState *state = XLogReaderAllocate(pagereadfunc, private_data, alignedSize);
    if (state != NULL) {
        state->preReadStartPtr = InvalidXlogPreReadStartPtr;
        state->preReadBufOrigin = (char *)palloc_extended(XLogPreReadSize + alignedSize,
            MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
        if (state->preReadBufOrigin == NULL) {
            pfree(state->errormsg_buf);
            state->errormsg_buf = NULL;
            pfree(state->readBufOrigin);
            state->readBufOrigin = NULL;
            state->readBuf = NULL;
            pfree(state->readRecordBuf);
            state->readRecordBuf = NULL;
            pfree(state);
            state = NULL;
            return NULL;
        }

        if (alignedSize == 0) {
            state->preReadBuf = state->preReadBufOrigin;
        } else {
            state->preReadBuf = (char *)TYPEALIGN(alignedSize, state->preReadBufOrigin);
        }

        state->xlogFlushPtrForPerRead = InvalidXLogRecPtr;
    }

    return state;
}

void SSGetXlogPath()
{
    int primaryId = -1;
    errno_t rc = EOK;
    char *dssdir = g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name;

    /* get primary inst id */
    primaryId = SSGetPrimaryInstId();

    rc = snprintf_s(g_instance.dms_cxt.SSRecoveryInfo.recovery_xlogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog%d",
        dssdir, primaryId);
    securec_check_ss(rc, "", "");
}

void SSSaveReformerCtrl()
{
    int fd = -1;
    int len;
    errno_t err = EOK;
    char *fname[2];
    len = sizeof(ss_reformer_ctrl_t);

    int write_size = (int)BUFFERALIGN(len);
    char buffer[write_size] __attribute__((__aligned__(ALIGNOF_BUFFER))) = { 0 };

    err = memcpy_s(&buffer, write_size, &g_instance.dms_cxt.SSReformerControl, len);
    securec_check(err, "\0", "\0");

    INIT_CRC32C(((ss_reformer_ctrl_t *)buffer)->crc);
    COMP_CRC32C(((ss_reformer_ctrl_t *)buffer)->crc, (char *)buffer, offsetof(ss_reformer_ctrl_t, crc));
    FIN_CRC32C(((ss_reformer_ctrl_t *)buffer)->crc);

    fname[0] = XLOG_CONTROL_FILE_BAK;
    fname[1] = XLOG_CONTROL_FILE;

    for (int i = 0; i < BAK_CTRL_FILE_NUM; i++) {
        if (i == 0) {
            fd = BasicOpenFile(fname[i], O_CREAT | O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
        } else {
            fd = BasicOpenFile(fname[i], O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
        }

        if (fd < 0) {
            ereport(FATAL, (errcode_for_file_access(), errmsg("could not open control file \"%s\": %m", fname[i])));
        }

        SSWriteInstanceControlFile(fd, buffer, REFORM_CTRL_PAGE, write_size);
        if (close(fd)) {
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not close control file: %m")));
        }
    }
}

void SSClearSegCache()
{
    (void)LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);
    HeapMemResetHash(t_thrd.storage_cxt.SegSpcCache, "Shared Seg Spc hash by request");
    LWLockRelease(ShmemIndexLock);
}
