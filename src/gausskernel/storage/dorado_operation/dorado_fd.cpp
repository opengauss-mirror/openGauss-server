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
 * dorado_fd.cpp
 *      read/write dorado starage operation
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/dorado_operation/dorado_fd.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/xlog.h"
#include "storage/dorado_operation/dorado_fd.h"
#include "miscadmin.h"

const uint32 DORADO_XLOG_START_POS = XLogSegSize;

void DoradoWriteCtlInfo(const ShareStorageXLogCtl *ctlInfo);
void DoradoReadCtlInfo(ShareStorageXLogCtl *ctlInfo);
int DoradoWriteXLog(XLogRecPtr startLsn, char *buf, int writeLen);
int DoradoReadXLog(XLogRecPtr startLsn, char *buf, int expectReadLen);
void DoradoFsync();

static const ShareStorageOperateIf doradoOperateIf = {
    DoradoReadCtlInfo, DoradoWriteCtlInfo, DoradoReadXLog, DoradoWriteXLog, DoradoFsync,
};

static inline uint64 GetXlogPos(uint64 expect)
{
    return (expect + DORADO_XLOG_START_POS);
}

void InitDoradoStorage(char *filePath, uint64 fileSize)
{
    Assert(!g_instance.xlog_cxt.shareStorageopCtl.isInit);
    g_instance.xlog_cxt.shareStorageopCtl.xlogFilePath = filePath;
    
    g_instance.xlog_cxt.shareStorageopCtl.blkSize = MEMORY_ALIGNED_SIZE;
    g_instance.xlog_cxt.shareStorageopCtl.opereateIf = &doradoOperateIf;

    canonicalize_path(filePath);
    g_instance.xlog_cxt.shareStorageopCtl.fd = open(filePath, O_RDWR | PG_BINARY | O_DIRECT, S_IRUSR | S_IWUSR);
    if (g_instance.xlog_cxt.shareStorageopCtl.fd < 0) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not open xlog file \"%s\" : %m", filePath)));
    }

    g_instance.xlog_cxt.shareStorageopCtl.isInit = true;
    if (IsInitdb) {
        g_instance.xlog_cxt.shareStorageopCtl.xlogFileSize = fileSize;
    } else {
        Assert(g_instance.xlog_cxt.shareStorageXLogCtl != NULL);
        DoradoReadCtlInfo(g_instance.xlog_cxt.shareStorageXLogCtl);
        g_instance.xlog_cxt.shareStorageopCtl.xlogFileSize = g_instance.xlog_cxt.shareStorageXLogCtl->xlogFileSize;
    }
}

void DoradoWriteCtlInfo(const ShareStorageXLogCtl *ctlInfo)
{
    Assert(g_instance.xlog_cxt.shareStorageopCtl.fd > 0);

    if (!IS_TYPE_ALIGINED(g_instance.xlog_cxt.shareStorageopCtl.blkSize, ctlInfo)) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("dorado write control info ptr(%p) is not match,mask is %X",
                                                          ctlInfo, g_instance.xlog_cxt.shareStorageopCtl.blkSize)));
    }
    if (ctlInfo->magic != SHARE_STORAGE_CTL_MAGIC || ctlInfo->checkNumber != SHARE_STORAGE_CTL_CHCK_NUMBER) {
        ereport(FATAL, (errmsg("ShareStorageXLogCtl info in memory maybe damaged")));
    }
    pg_crc32c crc = CalShareStorageCtlInfoCrc(ctlInfo);
    if (!EQ_CRC32C(crc, ctlInfo->crc)) {
        ereport(FATAL, (errmsg("crc check fail for ShareStorageXLogCtl in DoradoWriteCtlInfo")));
    }

    // write 512bytes
    ssize_t actualBytes = pwrite(g_instance.xlog_cxt.shareStorageopCtl.fd, ctlInfo, DORADO_CTL_WRITE_SIZE, 0);
    if (actualBytes != (ssize_t)DORADO_CTL_WRITE_SIZE) {
        /* if write didn't set errno, assume no disk space */
        if (errno == 0) {
            errno = ENOSPC;
        }

        ereport(PANIC,
                (errcode_for_file_access(), errmsg("could not write dorado ctl info length %lu, expect length %lu: %m",
                                                   (unsigned long)actualBytes, (unsigned long)DORADO_CTL_WRITE_SIZE)));
    }
}

void DoradoReadCtlInfo(ShareStorageXLogCtl *ctlInfo)
{
    Assert(g_instance.xlog_cxt.shareStorageopCtl.fd > 0);
    if (!IS_TYPE_ALIGINED(g_instance.xlog_cxt.shareStorageopCtl.blkSize, ctlInfo)) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("dorado read control info ptr(%p) is not match,mask is %X",
                                                          ctlInfo, g_instance.xlog_cxt.shareStorageopCtl.blkSize)));
    }

    ssize_t actualBytes = pread(g_instance.xlog_cxt.shareStorageopCtl.fd, ctlInfo, DORADO_CTL_WRITE_SIZE, 0);
    if (actualBytes != (ssize_t)DORADO_CTL_WRITE_SIZE) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not read dorado ctl info: %m")));
    }

    if (ctlInfo->magic != SHARE_STORAGE_CTL_MAGIC || ctlInfo->checkNumber != SHARE_STORAGE_CTL_CHCK_NUMBER) {
        ereport(FATAL, (errmsg("dorado ctl info maybe damaged, cltInfo->magic = %u, ctlInfo->checkNumber = %lu",
                            ctlInfo->magic, ctlInfo->checkNumber)));
    }

    pg_crc32c crc = CalShareStorageCtlInfoCrc(ctlInfo);
    if (!EQ_CRC32C(crc, ctlInfo->crc)) {
        ereport(FATAL, (errmsg("crc check fail for ShareStorageXLogCtl in DoradoReadCtlInfo")));
    }
}

int DoradoReadXLog(XLogRecPtr startLsn, char *buf, int expectReadLen)
{
    Assert(g_instance.xlog_cxt.shareStorageopCtl.fd > 0);
    if (!IS_TYPE_ALIGINED(g_instance.xlog_cxt.shareStorageopCtl.blkSize, buf)) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("dorado read xlog ptr(%p) is not match,mask is %X", buf,
                                                          g_instance.xlog_cxt.shareStorageopCtl.blkSize)));
    }

    uint64 startPos = startLsn % g_instance.xlog_cxt.shareStorageopCtl.xlogFileSize;
    if ((startPos + expectReadLen) <= g_instance.xlog_cxt.shareStorageopCtl.xlogFileSize) {
        ssize_t actualBytes = pread(g_instance.xlog_cxt.shareStorageopCtl.fd, buf, expectReadLen, GetXlogPos(startPos));
        if (actualBytes < 0) {
            uint32 shiftSize = 32;
            ereport(PANIC, (errcode_for_file_access(), errmsg("read xlog(start:%X/%X, pos:%lu len:%d) failed : %m",
                                                              static_cast<uint32>(startLsn >> shiftSize),
                                                              static_cast<uint32>(startLsn), startPos, expectReadLen)));
        }

        return static_cast<int>(actualBytes);
    } else {
        int firstReadSize = g_instance.xlog_cxt.shareStorageopCtl.xlogFileSize - startPos;
        int secondReadSize = expectReadLen - firstReadSize;
        ssize_t actualBytes = pread(g_instance.xlog_cxt.shareStorageopCtl.fd, buf, firstReadSize, GetXlogPos(startPos));
        if (actualBytes < 0) {
            uint32 shiftSize = 32;
            ereport(PANIC, (errcode_for_file_access(), errmsg("first read xlog(start:%X/%X, pos:%lu len:%d) failed:%m",
                                                              static_cast<uint32>(startLsn >> shiftSize),
                                                              static_cast<uint32>(startLsn), startPos, firstReadSize)));
        }

        if (actualBytes < firstReadSize) {
            return static_cast<int>(actualBytes);
        }

        actualBytes = (ssize_t)pread(g_instance.xlog_cxt.shareStorageopCtl.fd, buf + firstReadSize, secondReadSize,
            (off_t)GetXlogPos(0));
        if (actualBytes < 0) {
            uint32 shiftSize = 32;
            XLogRecPtr nextStartLsn = startLsn + firstReadSize;
            ereport(PANIC, (errcode_for_file_access(), errmsg("second read xlog(start:%X/%X, pos:0 len:%d)failed :%m",
                                                              static_cast<uint32>(nextStartLsn >> shiftSize),
                                                              static_cast<uint32>(nextStartLsn), secondReadSize)));
        }

        return static_cast<int>(actualBytes + firstReadSize);
    }
}

int DoradoWriteXLog(XLogRecPtr startLsn, char *buf, int writeLen)
{
    Assert(g_instance.xlog_cxt.shareStorageopCtl.fd > 0);
    uint64 startPos = startLsn % g_instance.xlog_cxt.shareStorageopCtl.xlogFileSize;

    if (!IS_TYPE_ALIGINED(g_instance.xlog_cxt.shareStorageopCtl.blkSize, buf)) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("dorado write xlog ptr(%p) is not match,mask is %X", buf,
                                                          g_instance.xlog_cxt.shareStorageopCtl.blkSize)));
    }

    if ((startPos + writeLen) <= g_instance.xlog_cxt.shareStorageopCtl.xlogFileSize) {
        ssize_t actualBytes = pwrite(g_instance.xlog_cxt.shareStorageopCtl.fd, buf, writeLen, GetXlogPos(startPos));
        if (actualBytes != writeLen) {
            if (errno == 0) {
                errno = ENOSPC;
            }
            uint32 shiftSize = 32;
            ereport(PANIC,
                    (errcode_for_file_access(), errmsg("could not write xlog at start:%X/%X offset %lu, length %d: %m",
                                                       static_cast<uint32>(startLsn >> shiftSize),
                                                       static_cast<uint32>(startLsn), startPos, writeLen)));
        }
    } else {
        int firstWriteSize = g_instance.xlog_cxt.shareStorageopCtl.xlogFileSize - startPos;
        int secondWriteSize = writeLen - firstWriteSize;

        ssize_t actualBytes = pwrite(g_instance.xlog_cxt.shareStorageopCtl.fd, buf, firstWriteSize,
                                     GetXlogPos(startPos));
        if (actualBytes != firstWriteSize) {
            if (errno == 0) {
                errno = ENOSPC;
            }
            uint32 shiftSize = 32;
            ereport(PANIC,
                    (errcode_for_file_access(), errmsg("could not write xlog at start:%X/%X offset %lu, length %d: %m",
                                                       static_cast<uint32>(startLsn >> shiftSize),
                                                       static_cast<uint32>(startLsn), startPos, firstWriteSize)));
        }

        actualBytes = pwrite(g_instance.xlog_cxt.shareStorageopCtl.fd, buf + firstWriteSize, secondWriteSize,
                             GetXlogPos(0));
        if (actualBytes != secondWriteSize) {
            if (errno == 0) {
                errno = ENOSPC;
            }
            uint32 shiftSize = 32;
            XLogRecPtr nextStartLsn = startLsn + firstWriteSize;
            ereport(PANIC,
                    (errcode_for_file_access(), errmsg("could not write xlog at start:%X/%X offset 0, length %d: %m",
                                                       static_cast<uint32>(nextStartLsn >> shiftSize),
                                                       static_cast<uint32>(nextStartLsn), secondWriteSize)));
        }
    }

    return writeLen;
}

void DoradoFsync()
{
    Assert(g_instance.xlog_cxt.shareStorageopCtl.fd > 0);
    if (fsync(g_instance.xlog_cxt.shareStorageopCtl.fd) != 0) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not fsync dorado file %s: %m",
                                                          g_instance.xlog_cxt.shareStorageopCtl.xlogFilePath)));
    }
}
