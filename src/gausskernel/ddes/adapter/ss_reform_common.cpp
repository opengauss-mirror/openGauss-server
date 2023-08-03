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
#include "replication/ss_cluster_replication.h"

/*
 * Add xlog reader private structure for page read.
 */
typedef struct XLogPageReadPrivate {
    int emode;
    bool fetching_ckpt; /* are we fetching a checkpoint record? */
    bool randAccess;
} XLogPageReadPrivate;

std::vector<int> SSGetAllStableNodeId()
{
    std::vector<int> posList;
    int pos = 0;
    uint64 stableInstId = g_instance.dms_cxt.SSReformerControl.list_stable;
    while (stableInstId) {
        uint64 res = stableInstId & 0x01;
        if (res) {
            posList.emplace_back(pos);
        }
        pos++;
        stableInstId = stableInstId >> 1;
    }

    return posList;
}

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
retry:
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

        /* 
        * When SS_CLUSTER_DORADO_REPLICATION enabled, current xlog dictionary may be not the correct dictionary,
        * because all xlog dictionaries are in the same LUN, we need loop over other dictionaries.
        * Do we need source == XLOG_FROM_STREAM?
        */
        if (SS_CLUSTER_DORADO_REPLICATION) {
            std::vector<int> nodeList = SSGetAllStableNodeId(); // stable node list,
            Assert(!nodeList.empty());
            char xlogPath[MAXPGPATH];
            char *dssdir = g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name;
            for (auto elem : nodeList) {
                if (elem == g_instance.dms_cxt.SSReformerControl.recoveryInstId) {
                    continue;
                }

                errorno = memset_s(xlogPath, sizeof(xlogPath), 0, sizeof(xlogPath));
                securec_check_ss(errorno, "", "");
                /* try to read from other xlog dictionary */
                errorno = snprintf_s(xlogPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog%d", dssdir, elem);
                securec_check_ss(errorno, "", "");

                errorno = memset_s(path, sizeof(path), 0, sizeof(path));
                securec_check_ss(errorno, "", "");

                errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%08X%08X%08X", xlogPath, tli,
                                    (uint32)((segno) / XLogSegmentsPerXLogId), (uint32)((segno) % XLogSegmentsPerXLogId));
                securec_check_ss(errorno, "", "");

                fd = BasicOpenFile(path, O_RDONLY | PG_BINARY, 0);
                if (fd < 0) {
                    continue;
                }
                ereport(LOG, (errmsg("find xlog file in path : \"%s\"", path)));
                goto retry;
            }
        }

        if (!FILE_POSSIBLY_DELETED(errno)) { 
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not open file \"%s\" (log segment %s): %m", path,
                                                              XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno))));
        }
    }

    /* Couldn't find it.  For simplicity, complain about front timeline */
    errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%08X%08X%08X", xlog_path,
                         t_thrd.xlog_cxt.recoveryTargetTLI, (uint32)((segno) / XLogSegmentsPerXLogId),
                         (uint32)((segno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    errno = ENOENT;
    ereport(emode, (errcode_for_file_access(), errmsg("could not open file \"%s\" (log segment %s): %m", path,
                                                      XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno)))); 

    return -1;
}

int SSReadXlogInternal(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, XLogRecPtr targetRecPtr, char *buf,
    int readLen)
{
    uint32 preReadOff;
    XLogRecPtr xlogFlushPtrForPerRead = xlogreader->xlogFlushPtrForPerRead;
    bool isReadFile = true;

    Assert(readLen > 0);
    Assert(readLen <= XLogPreReadSize);

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
            int err = memcpy_s(buf, readLen, xlogreader->preReadBuf + preReadOff, readLen);
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

    return readLen;
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

void SSGetRecoveryXlogPath()
{
    errno_t rc = EOK;
    char *dssdir = g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name;

    rc = snprintf_s(g_instance.dms_cxt.SSRecoveryInfo.recovery_xlog_dir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog%d",
        dssdir, g_instance.dms_cxt.SSRecoveryInfo.recovery_inst_id);
    securec_check_ss(rc, "", "");
}

static void SSSaveOldReformerCtrl()
{
    ss_reformer_ctrl_t new_ctrl = g_instance.dms_cxt.SSReformerControl;
    ss_old_reformer_ctrl_t old_ctrl = {new_ctrl.list_stable, new_ctrl.primaryInstId, new_ctrl.crc};

    int len = sizeof(ss_old_reformer_ctrl_t);
    int write_size = (int)BUFFERALIGN(len);
    char buffer[write_size] __attribute__((__aligned__(ALIGNOF_BUFFER))) = { 0 };
    char *fname[2];
    int fd = -1;

    errno_t err = memcpy_s(&buffer, write_size, &old_ctrl, len);
    securec_check(err, "\0", "\0");

    INIT_CRC32C(((ss_old_reformer_ctrl_t *)buffer)->crc);
    COMP_CRC32C(((ss_old_reformer_ctrl_t *)buffer)->crc, (char *)buffer, offsetof(ss_old_reformer_ctrl_t, crc));
    FIN_CRC32C(((ss_old_reformer_ctrl_t *)buffer)->crc);

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

static bool SSReadOldReformerCtrl()
{
    ss_reformer_ctrl_t *new_ctrl = &g_instance.dms_cxt.SSReformerControl;
    ss_old_reformer_ctrl_t old_ctrl;
    pg_crc32c crc;
    int fd = -1;
    bool retry = false;
    char *fname = XLOG_CONTROL_FILE;

loop:
    fd = BasicOpenFile(fname, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(FATAL, (errcode_for_file_access(), errmsg("could not open control file \"%s\": %m", fname)));
    }

    off_t seekpos = (off_t)BLCKSZ * REFORM_CTRL_PAGE;
    int len = sizeof(ss_old_reformer_ctrl_t);

    int read_size = (int)BUFFERALIGN(len);
    char buffer[read_size] __attribute__((__aligned__(ALIGNOF_BUFFER)));
    if (pread(fd, buffer, read_size, seekpos) != read_size) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not read from control file: %m")));
    }

    errno_t rc = memcpy_s(&old_ctrl, len, buffer, len);
    securec_check(rc, "", "");
    if (close(fd) < 0) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not close control file: %m")));
    }

    /* Now check the CRC. */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (char *)&old_ctrl, offsetof(ss_old_reformer_ctrl_t, crc));
    FIN_CRC32C(crc);

    if (!EQ_CRC32C(crc, old_ctrl.crc)) {
        if (retry == false) {
            ereport(WARNING,
                (errmsg("control file \"%s\" contains incorrect checksum in upgrade mode, try backup file", fname)));
            fname = XLOG_CONTROL_FILE_BAK;
            retry = true;
            goto loop;
        } else {
            ereport(WARNING,
                (errmsg("backup control file \"%s\" contains incorrect checksum in upgrade mode, "
                "try again in post-upgrade mode", fname)));
            return false;
        }
    }

    // new params set to initial value
    new_ctrl->version = REFORM_CTRL_VERSION;
    new_ctrl->recoveryInstId = INVALID_INSTANCEID;
    new_ctrl->clusterStatus = CLUSTER_NORMAL;

    // exist param inherit
    new_ctrl->primaryInstId = old_ctrl.primaryInstId;
    new_ctrl->list_stable = old_ctrl.list_stable;
    new_ctrl->crc = old_ctrl.crc;

    return true;
}

void SSSaveReformerCtrl(bool force)
{
    int fd = -1;
    int len;
    errno_t err = EOK;
    char *fname[2];

    if ((pg_atomic_read_u32(&WorkingGrandVersionNum) < ONDEMAND_REDO_VERSION_NUM) && !force) {
        SSSaveOldReformerCtrl();
        return;
    }

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

void SSReadControlFile(int id, bool updateDmsCtx)
{
    pg_crc32c crc;
    errno_t rc = EOK;
    int fd = -1;
    char *fname = NULL;
    bool retry = false;
    int read_size = 0;
    int len = 0;
    fname = XLOG_CONTROL_FILE;

    if ((pg_atomic_read_u32(&WorkingGrandVersionNum) < ONDEMAND_REDO_VERSION_NUM) && (id == REFORM_CTRL_PAGE)) {
        if (SSReadOldReformerCtrl()) {
            return;
        }

        // maybe primary node already upgrade pg_control file, sleep and try read in lastest mode again
        if (SS_STANDBY_MODE) {
            pg_usleep(5000000);  /* 5 sec */
            goto loop;
        } else {
            ereport(PANIC, (errmsg("incorrect checksum in control file")));
        }
    }

loop:
    fd = BasicOpenFile(fname, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(FATAL, (errcode_for_file_access(), errmsg("could not open control file \"%s\": %m", fname)));
    }

    off_t seekpos = (off_t)BLCKSZ * id;

    if (id == REFORM_CTRL_PAGE) {
        len = sizeof(ss_reformer_ctrl_t);
    } else {
        len = sizeof(ControlFileData);
    }

    read_size = (int)BUFFERALIGN(len);
    char buffer[read_size] __attribute__((__aligned__(ALIGNOF_BUFFER)));
    if (pread(fd, buffer, read_size, seekpos) != read_size) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not read from control file: %m")));
    }

    if (id == REFORM_CTRL_PAGE) {
        rc = memcpy_s(&g_instance.dms_cxt.SSReformerControl, len, buffer, len);
        securec_check(rc, "", "");
        if (close(fd) < 0) {
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not close control file: %m")));
        }

        /* Now check the CRC. */
        INIT_CRC32C(crc);
        COMP_CRC32C(crc, (char *)&g_instance.dms_cxt.SSReformerControl, offsetof(ss_reformer_ctrl_t, crc));
        FIN_CRC32C(crc);

        if (!EQ_CRC32C(crc, g_instance.dms_cxt.SSReformerControl.crc)) {
            if (retry == false) {
                ereport(WARNING, (errmsg("control file \"%s\" contains incorrect checksum, try backup file", fname)));
                fname = XLOG_CONTROL_FILE_BAK;
                retry = true;
                goto loop;
            } else {
                ereport(FATAL, (errmsg("incorrect checksum in control file")));
            }
        }
    } else {
        ControlFileData* controlFile = NULL;
        ControlFileData tempControlFile;
        if (updateDmsCtx) {
            controlFile = &tempControlFile;
        } else {
            controlFile = t_thrd.shemem_ptr_cxt.ControlFile;
        }

        rc = memcpy_s(controlFile, (size_t)len, buffer, (size_t)len);
        securec_check(rc, "", "");
        if (close(fd) < 0) {
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not close control file: %m")));
        }

        /* Now check the CRC. */
        INIT_CRC32C(crc);
        COMP_CRC32C(crc, (char *)controlFile, offsetof(ControlFileData, crc));
        FIN_CRC32C(crc);

        if (!EQ_CRC32C(crc, controlFile->crc)) {
            if (retry == false) {
                ereport(WARNING, (errmsg("control file \"%s\" contains incorrect checksum, try backup file", fname)));
                fname = XLOG_CONTROL_FILE_BAK;
                retry = true;
                goto loop;
            } else {
                ereport(FATAL, (errmsg("incorrect checksum in control file")));
            }
        }

        if (XLByteLE(g_instance.dms_cxt.ckptRedo, controlFile->checkPointCopy.redo)) {
            g_instance.dms_cxt.ckptRedo = controlFile->checkPointCopy.redo;
        }
    }
}

void SSClearSegCache()
{
    (void)LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);
    HeapMemResetHash(t_thrd.storage_cxt.SegSpcCache, "Shared Seg Spc hash by request");
    LWLockRelease(ShmemIndexLock);
}

void SSStandbySetLibpqswConninfo()
{
    if (strlen(g_instance.dms_cxt.dmsInstAddr[g_instance.dms_cxt.SSReformerControl.primaryInstId]) == 0) {
        ereport(WARNING, (errmsg("Failed to get ip of primary node!")));
        return;
    }

    int replIdx = -1;
    ReplConnInfo *replconninfo = NULL;

    for (int i = 0; i < MAX_REPLNODE_NUM; ++i) {
        replconninfo = t_thrd.postmaster_cxt.ReplConnArray[i];
        if (replconninfo == NULL) {
            continue;
        }

        if (strcmp(replconninfo->remotehost,
            g_instance.dms_cxt.dmsInstAddr[g_instance.dms_cxt.SSReformerControl.primaryInstId]) == 0) {
            replIdx = i;
            break;
        }
    }

    if (replIdx == -1) {
        ereport(WARNING, (errmsg("Failed to get replconninfo of primary node, check the replconninfo config!")));
        return;
    }

    replconninfo = t_thrd.postmaster_cxt.ReplConnArray[replIdx];
    errno_t rc = EOK;
    rc = snprintf_s(g_instance.dms_cxt.conninfo, MAXCONNINFO, MAXCONNINFO - 1,
        "host=%s port=%d localhost=%s localport=%d", replconninfo->remotehost, replconninfo->remoteport,
        replconninfo->localhost, replconninfo->localport);
    securec_check_ss(rc, "\0", "\0");
    g_instance.dms_cxt.conninfo[MAXCONNINFO - 1] = '\0';

    return;
}
