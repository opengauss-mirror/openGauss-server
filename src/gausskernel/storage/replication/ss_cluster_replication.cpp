/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * Description: openGauss is licensed under Mulan PSL v2.
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
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/ss_cluster_replication.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "replication/ss_cluster_replication.h"
#include "access/xlog_internal.h"
#include "storage/file/fio_device.h"
#include "storage/smgr/fd.h"


void WriteSSDoradoCtlInfoFile()
{
    struct stat st;
    Assert(stat(SS_DORADO_CTRL_FILE, &st) == 0 && S_ISREG(st.st_mode));
    Assert(g_instance.xlog_cxt.ssReplicationXLogCtl != NULL);
    ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.ssReplicationXLogCtl;
    errno_t errorno = EOK;

    int fd = BasicOpenFile(SS_DORADO_CTRL_FILE, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("[WriteSSDoradoCtlInfoFile]could not open SS dorado control file \"%s\".",
                SS_DORADO_CTRL_FILE)));
    }

    pg_crc32c crc = CalShareStorageCtlInfoCrc(ctlInfo);
    ctlInfo->crc = crc;

    char buffer[SS_DORADO_CTL_INFO_SIZE] __attribute__((__aligned__(ALIGNOF_BUFFER)));

    errorno = memcpy_s(buffer, SS_DORADO_CTL_INFO_SIZE, ctlInfo, SS_DORADO_CTL_INFO_SIZE);
    securec_check_c(errorno, "\0", "\0");

    if (write(fd, buffer, SS_DORADO_CTL_INFO_SIZE) != SS_DORADO_CTL_INFO_SIZE) {
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("[WriteSSDoradoCtlInfoFile]could not write SS dorado control file \"%s\".",
                SS_DORADO_CTRL_FILE)));
    }

    if (close(fd)) {
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("[WriteSSDoradoCtlInfoFile]could not close SS dorado control file \"%s\".",
                SS_DORADO_CTRL_FILE)));
    }
}

void ReadSSDoradoCtlInfoFile()
{
    struct stat st;
    Assert(stat(SS_DORADO_CTRL_FILE, &st) == 0 && S_ISREG(st.st_mode));
    Assert(g_instance.xlog_cxt.ssReplicationXLogCtl != NULL);
    ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.ssReplicationXLogCtl;
    errno_t errorno = EOK;
    int fd = -1;
    fd = BasicOpenFile(SS_DORADO_CTRL_FILE, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        /* TODO: need consider that dorado is briefly unreadable during the synchronization process */
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("[ReadSSDoradoCtlInfo]could not create SS dorado control file \"%s\".",
                SS_DORADO_CTRL_FILE)));
    }

    char buffer[SS_DORADO_CTL_INFO_SIZE] __attribute__((__aligned__(ALIGNOF_BUFFER)));
    if (read(fd, buffer, SS_DORADO_CTL_INFO_SIZE) != SS_DORADO_CTL_INFO_SIZE) {
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("[ReadSSDoradoCtlInfo]could not read SS dorado control file \"%s\".",
                SS_DORADO_CTRL_FILE)));
    }

    errorno = memcpy_s(ctlInfo, SS_DORADO_CTL_INFO_SIZE, buffer, SS_DORADO_CTL_INFO_SIZE);
    securec_check_c(errorno, "\0", "\0");
    if (close(fd)) {
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("[ReadSSDoradoCtlInfo]could not close SS dorado control file \"%s\".",
                SS_DORADO_CTRL_FILE)));
    }

    if ((ctlInfo->magic != SHARE_STORAGE_CTL_MAGIC) || (ctlInfo->checkNumber != SHARE_STORAGE_CTL_CHCK_NUMBER)) {
        ereport(FATAL, (errmsg("[ReadSSDoradoCtlInfo]SS replication ctl_info maybe damaged.")));
    }

    pg_crc32c crc = CalShareStorageCtlInfoCrc(ctlInfo);
    if (!EQ_CRC32C(crc, ctlInfo->crc)) {
        ereport(FATAL, (errmsg("[ReadSSDoradoCtlInfo]SS replication ctl_info crc check failed.")));
    }
}

void InitSSDoradoCtlInfoFile()
{
    struct stat st;
    if (stat(SS_DORADO_CTRL_FILE, &st) == 0 && S_ISREG(st.st_mode)) {
        ReadSSDoradoCtlInfoFile();
        ereport(LOG, (errcode_for_file_access(), errmsg("[InitSSDoradoCtlInfoFile] Dorado ctl info file already exists.")));
        return;
    }

    ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.ssReplicationXLogCtl;
    Assert(ctlInfo != NULL);

    int fd = -1;
    char buffer[SS_DORADO_CTL_INFO_SIZE] __attribute__((__aligned__(ALIGNOF_BUFFER))); /* need to be aligned */
    errno_t errorno = EOK;
    Assert(stat(SS_DORADO_CTRL_FILE, &st) !=0 || !S_ISREG(st.st_mode));

    /* create SS_DORADO_CTRL_FILE first time */
    fd = BasicOpenFile(SS_DORADO_CTRL_FILE, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("could not create SS dorado control file \"%s\".", SS_DORADO_CTRL_FILE)));
    }

    ereport(LOG, (errcode_for_file_access(), errmsg("[InitSSDoradoCtlInfoFile] Create SS dorado ctl info succ.")));

    InitSSDoradoCtlInfo(ctlInfo, 0);
    ctlInfo->crc = CalShareStorageCtlInfoCrc(ctlInfo);

    Assert(sizeof(ShareStorageXLogCtl) > SS_DORADO_CTL_INFO_SIZE);
    errorno = memcpy_s(buffer, SS_DORADO_CTL_INFO_SIZE, ctlInfo, SS_DORADO_CTL_INFO_SIZE);
    securec_check_c(errorno, "\0", "\0");
    if (write(fd, buffer, SS_DORADO_CTL_INFO_SIZE) != SS_DORADO_CTL_INFO_SIZE) {
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("could not write SS dorado control file \"%s\".", SS_DORADO_CTRL_FILE)));
    }

    if (pg_fsync(fd) != 0) {
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("could not fsync SS dorado control file \"%s\".", SS_DORADO_CTRL_FILE)));
    }
    if (close(fd)) {
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("could not close SS dorado control file \"%s\".", SS_DORADO_CTRL_FILE)));
    }
}

void SSClusterDoradoStorageInit()
{
    bool found = false;
    g_instance.xlog_cxt.ssReplicationXLogCtl = (ShareStorageXLogCtl*)ShmemInitStruct("SS Replication Xlog Ctl", sizeof(ShareStorageXLogCtl), &found);
    InitSSDoradoCtlInfoFile();
    g_instance.dms_cxt.SSRecoveryInfo.dorado_sharestorage_inited = true;
}

void UpdateSSDoradoCtlInfoAndSync()
{
    if (!SS_CLUSTER_DORADO_REPLICATION) {
        return;
    }

    ReadSSDoradoCtlInfoFile();
    ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.ssReplicationXLogCtl;
    ctlInfo->insertHead = t_thrd.xlog_cxt.LogwrtResult->Write;
    WriteSSDoradoCtlInfoFile();
}

bool CheckSSCtlInfoConsistency(XLogRecPtr localEnd)
{
    if (!SS_CLUSTER_DORADO_REPLICATION) {
        return true;
    }

    ReadSSDoradoCtlInfoFile();
    ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.ssReplicationXLogCtl;

    if (ctlInfo->systemIdentifier != GetSystemIdentifier()) {
        ereport(FATAL, (errmsg("database system version is different between shared storage %lu and local %lu",
                            ctlInfo->systemIdentifier, GetSystemIdentifier())));
    }

    return true;
}

void InitSSDoradoCtlInfo(ShareStorageXLogCtl *ctlInfo, uint64 sysidentifier)
{
    ctlInfo->magic = SHARE_STORAGE_CTL_MAGIC;
    ctlInfo->length = SizeOfShareStorageXLogCtl;
    ctlInfo->version = CURRENT_SHARE_STORAGE_CTL_VERSION;
    ctlInfo->systemIdentifier = sysidentifier;
    ctlInfo->insertHead = 0;
    ctlInfo->xlogFileSize = 0;
    ctlInfo->checkNumber = SHARE_STORAGE_CTL_CHCK_NUMBER;
    ctlInfo->insertTail = 0;
    ctlInfo->term = 1;
    ctlInfo->pad1 = 0;
    ctlInfo->pad2 = 0;
    ctlInfo->pad3 = 0;
    ctlInfo->pad4 = 0;
}

void CheckSSDoradoCtlInfo(XLogRecPtr localEnd)
{
    if (!SS_CLUSTER_DORADO_REPLICATION) {
        return;
    }

    ReadSSDoradoCtlInfoFile();
    ShareStorageXLogCtl *ctlInfo = g_instance.xlog_cxt.ssReplicationXLogCtl;

    if (ctlInfo->systemIdentifier != GetSystemIdentifier()) {
        ereport(FATAL, (errmsg("database system version is different between shared storage %lu and local %lu",
                            ctlInfo->systemIdentifier, GetSystemIdentifier())));
    }

    uint32 shiftSize = 32;
    if (localEnd < ctlInfo->insertHead) {
        ereport(LOG,
                (errmsg("modify insertHead from %X/%X to %X/%X",
                        static_cast<uint32>(ctlInfo->insertHead >> shiftSize), static_cast<uint32>(ctlInfo->insertHead),
                        static_cast<uint32>(localEnd >> shiftSize), static_cast<uint32>(localEnd))));
        ctlInfo->insertHead = localEnd;
        WriteSSDoradoCtlInfoFile();
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
            /* Need to consider other xlog path?? */
            XLogFilePath(path, MAXPGPATH, t_thrd.xlog_cxt.ThisTimeLineID, sendSegNo);
            struct stat stat_buf;

            if (stat(path, &stat_buf) != 0) {
                ereport(FATAL, (errmsg("the local's tail is bigger than ctlInfo insertHead %X/%X, path %s",
                                       static_cast<uint32>(ctlInfo->insertHead >> shiftSize),
                                       static_cast<uint32>(ctlInfo->insertHead), path)));
            }
        }
    }

    uint32 localTerm = Max(g_instance.comm_cxt.localinfo_cxt.term_from_file,
                           g_instance.comm_cxt.localinfo_cxt.term_from_xlog);
    if (localTerm > ctlInfo->term) {
        ctlInfo->term = localTerm;
        WriteSSDoradoCtlInfoFile();
    }
}