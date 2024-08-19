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
#include "replication/walsender_private.h"
#include "replication/ss_disaster_cluster.h"

/*
 * Add xlog reader private structure for page read.
 */
typedef struct XLogPageReadPrivate {
    int emode;
    bool fetching_ckpt; /* are we fetching a checkpoint record? */
    bool randAccess;
} XLogPageReadPrivate;

int SSXLogFileOpenAnyTLI(XLogSegNo segno, int emode, uint32 sources, char* xlog_path)
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

        /* 
        * When SS_DORADO_CLUSTER enabled, current xlog dictionary may be not the correct dictionary,
        * because all xlog dictionaries are in the same LUN, we need loop over other dictionaries.
        */
        if (fd < 0 && SS_DORADO_CLUSTER) {
            return -1;
        }

        if (!FILE_POSSIBLY_DELETED(errno)) { 
            ereport(PANIC, (errcode_for_file_access(), errmsg("[SS] could not open file \"%s\" (log segment %s): %m,"
                    " %s", path, XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno), TRANSLATE_ERRNO)));
        }
    }

    /* Couldn't find it.  For simplicity, complain about front timeline */
    errorno = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%08X%08X%08X", xlog_path,
                         t_thrd.xlog_cxt.recoveryTargetTLI, (uint32)((segno) / XLogSegmentsPerXLogId),
                         (uint32)((segno) % XLogSegmentsPerXLogId));
    securec_check_ss(errorno, "", "");

    errno = ENOENT;
    if (!SS_ONDEMAND_REALTIME_BUILD_NORMAL) {
        ereport(emode, (errcode_for_file_access(), errmsg("[SS] could not open file \"%s\" (log segment %s): %m", path,
                                                          XLogFileNameP(t_thrd.xlog_cxt.ThisTimeLineID, segno))));
    }

    return -1;
}

int SSReadXlogInternal(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, XLogRecPtr targetRecPtr, char *buf,
    int readLen)
{
    uint32 preReadOff;

    Assert(readLen > 0);
    Assert(readLen <= XLogPreReadSize);

    do {
        if ((XLByteInPreReadBuf(targetPagePtr, xlogreader->preReadStartPtr))) {
            preReadOff = targetPagePtr % XLogPreReadSize;
            int err = memcpy_s(buf, readLen, xlogreader->preReadBuf + preReadOff, readLen);
            securec_check(err, "\0", "\0");
            break;
        } else {
            /*
             * That preReadStartPtr is InvalidXlogPreReadStartPtr has three kinds of occasions.
             */
            if (xlogreader->preReadStartPtr == InvalidXlogPreReadStartPtr && SS_DISASTER_CLUSTER) {
                ereport(LOG, (errmsg("[SS] In ss disaster cluster mode, preReadStartPtr is 0.")));
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
    char *dssdir = g_instance.attr.attr_storage.dss_attr.ss_dss_xlog_vg_name;

    rc = snprintf_s(g_instance.dms_cxt.SSRecoveryInfo.recovery_xlog_dir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog%d",
        dssdir, g_instance.dms_cxt.SSRecoveryInfo.recovery_inst_id);
    securec_check_ss(rc, "", "");
}

char* SSGetNextXLogPath(TimeLineID tli, XLogRecPtr startptr)
{
    static int index = 0;
    char path[MAXPGPATH];
    char fileName[MAXPGPATH];
    errno_t rc = EOK;
    XLogSegNo segno;
    XLByteToSeg(startptr, segno);
    rc = snprintf_s(fileName, MAXPGPATH, MAXPGPATH - 1, "%08X%08X%08X", tli,
                    (uint32)((segno) / XLogSegmentsPerXLogId),
                    (uint32)((segno) % XLogSegmentsPerXLogId));
    securec_check_ss_c(rc, "\0", "\0");

    while (true) { 
        index++;
        if (index >= DMS_MAX_INSTANCE || (g_instance.dms_cxt.SSRecoveryInfo.xlog_list[index][0] == '\0')) {
            index = 0;
            break;
        }

        rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", g_instance.dms_cxt.SSRecoveryInfo.xlog_list[index], fileName);
        securec_check_ss_c(rc, "\0", "\0");

        struct stat buffer;
        if (stat(path, &buffer) == 0) {
            break;
        }
    }

    return g_instance.dms_cxt.SSRecoveryInfo.xlog_list[index];
}

void SSDisasterGetXlogPathList()
{
    errno_t rc = EOK;
    for (int i = 0; i < DMS_MAX_INSTANCE; i++) {
        rc = memset_s(g_instance.dms_cxt.SSRecoveryInfo.xlog_list[i], MAXPGPATH, '\0', MAXPGPATH);
        securec_check_c(rc, "\0", "\0");
    }
    struct dirent *entry;
    
    DIR* dssdir = opendir(g_instance.attr.attr_storage.dss_attr.ss_dss_xlog_vg_name);
    if (dssdir == NULL) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("[SS] Error opening dssdir %s", 
                        g_instance.attr.attr_storage.dss_attr.ss_dss_xlog_vg_name)));                                                  
    }

    uint8_t len = strlen("pg_xlog");
    uint8_t index = 0;
    while ((entry = readdir(dssdir)) != NULL) {
        if (strncmp(entry->d_name, "pg_xlog", len) == 0) {
            if (strlen(entry->d_name) > len) {
                rc = memmove_s(entry->d_name, MAX_PATH, entry->d_name + len, strlen(entry->d_name) - len + 1);
                securec_check_c(rc, "\0", "\0");
                rc = snprintf_s(g_instance.dms_cxt.SSRecoveryInfo.xlog_list[index++], MAXPGPATH, MAXPGPATH - 1,
                    "%s/%s%d", g_instance.attr.attr_storage.dss_attr.ss_dss_xlog_vg_name, "pg_xlog", atoi(entry->d_name));
                securec_check_ss(rc, "", "");
            }
        } else {
            continue;
        }
    }
    closedir(dssdir);
}

void SSUpdateReformerCtrl()
{
    Assert(SS_PRIMARY_MODE || SS_DISASTER_CLUSTER);
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
            ereport(FATAL, (errcode_for_file_access(), errmsg("[SS] could not open control file \"%s\": %s",
                    fname[i], TRANSLATE_ERRNO)));
        }

        SSWriteInstanceControlFile(fd, buffer, REFORM_CTRL_PAGE, write_size);
        if (close(fd)) {
            ereport(PANIC, (errcode_for_file_access(), errmsg("[SS] could not close control file: %s",
                    TRANSLATE_ERRNO)));
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
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);

loop:
    fd = BasicOpenFile(fname, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        LWLockRelease(ControlFileLock);
        ereport(FATAL, (errcode_for_file_access(), errmsg("[SS] could not open control file \"%s\": %m", fname)));
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
        LWLockRelease(ControlFileLock);
        ereport(PANIC, (errcode_for_file_access(), errmsg("[SS] could not read from control file: %m")));
    }

    if (id == REFORM_CTRL_PAGE) {
        rc = memcpy_s(&g_instance.dms_cxt.SSReformerControl, len, buffer, len);
        securec_check(rc, "", "");
        if (close(fd) < 0) {
            LWLockRelease(ControlFileLock);
            ereport(PANIC, (errcode_for_file_access(), errmsg("[SS] could not close control file: %m")));
        }

        /* Now check the CRC. */
        INIT_CRC32C(crc);
        COMP_CRC32C(crc, (char *)&g_instance.dms_cxt.SSReformerControl, offsetof(ss_reformer_ctrl_t, crc));
        FIN_CRC32C(crc);

        if (!EQ_CRC32C(crc, g_instance.dms_cxt.SSReformerControl.crc)) {
            if (retry == false) {
                ereport(WARNING, (errmsg("[SS] control file \"%s\" contains incorrect checksum, try backup file", fname)));
                fname = XLOG_CONTROL_FILE_BAK;
                retry = true;
                goto loop;
            } else {
                LWLockRelease(ControlFileLock);
                ereport(FATAL, (errmsg("[SS] incorrect checksum in control file")));
            }
        }
        g_instance.dms_cxt.SSRecoveryInfo.cluster_ondemand_status = g_instance.dms_cxt.SSReformerControl.clusterStatus;
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
            LWLockRelease(ControlFileLock);
            ereport(PANIC, (errcode_for_file_access(), errmsg("[SS] could not close control file: %m")));
        }

        /* Now check the CRC. */
        INIT_CRC32C(crc);
        COMP_CRC32C(crc, (char *)controlFile, offsetof(ControlFileData, crc));
        FIN_CRC32C(crc);

        if (!EQ_CRC32C(crc, controlFile->crc)) {
            if (retry == false) {
                ereport(WARNING, (errmsg("[SS] control file \"%s\" contains incorrect checksum, try backup file", fname)));
                fname = XLOG_CONTROL_FILE_BAK;
                retry = true;
                goto loop;
            } else {
                LWLockRelease(ControlFileLock);
                ereport(FATAL, (errmsg("[SS] incorrect checksum in control file")));
            }
        }

        if (XLByteLE(g_instance.dms_cxt.ckptRedo, controlFile->checkPointCopy.redo)) {
            g_instance.dms_cxt.ckptRedo = controlFile->checkPointCopy.redo;
        }
    }
    LWLockRelease(ControlFileLock);
}

void SSClearSegCache()
{
    (void)LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);
    HeapMemResetHash(t_thrd.storage_cxt.SegSpcCache, "[SS] Shared Seg Spc hash by request");
    LWLockRelease(ShmemIndexLock);
}

void SSStandbySetLibpqswConninfo()
{
    if (strlen(g_instance.dms_cxt.dmsInstAddr[g_instance.dms_cxt.SSReformerControl.primaryInstId]) == 0) {
        ereport(WARNING, (errmsg("[SS] Failed to get ip of primary node!")));
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
        ereport(WARNING, (errmsg("[SS] Failed to get replconninfo of primary node, check the replconninfo config!")));
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

static void SSReadClusterRunMode()
{
    int fd = -1;
    char *fname = NULL;
    int read_size = 0;
    int len = sizeof(ss_reformer_ctrl_t);
    fname = XLOG_CONTROL_FILE;
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);

    fd = BasicOpenFile(fname, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        LWLockRelease(ControlFileLock);
        ereport(FATAL, (errcode_for_file_access(), errmsg("[SS] could not open control file \"%s\": %m", fname)));
    }

    off_t seekpos = (off_t)BLCKSZ * REFORM_CTRL_PAGE;

    read_size = (int)BUFFERALIGN(len);
    char buffer[read_size] __attribute__((__aligned__(ALIGNOF_BUFFER)));
    if (pread(fd, buffer, read_size, seekpos) != read_size) {
        (void)close(fd);
        LWLockRelease(ControlFileLock);
        ereport(PANIC, (errcode_for_file_access(), errmsg("[SS] could not read from control file: %m")));
    }
    
    g_instance.dms_cxt.SSReformerControl.clusterRunMode = ((ss_reformer_ctrl_t*)buffer)->clusterRunMode;

    if (close(fd) < 0) {
        LWLockRelease(ControlFileLock);
        ereport(PANIC, (errcode_for_file_access(), errmsg("[SS] could not close control file: %m")));
    }
    LWLockRelease(ControlFileLock);
}

void SSDisasterRefreshMode()
{
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);

    SSUpdateReformerCtrl();
    LWLockRelease(ControlFileLock);
    ereport(LOG, (errmsg("[SS] SSDisasterRefreshMode change control file cluster run mode to: %d",
        g_instance.dms_cxt.SSReformerControl.clusterRunMode)));
}

void SSDisasterUpdateHAmode() 
{
    SSReadClusterRunMode();
    if (SS_REFORM_REFORMER) {
        if (SS_DISASTER_PRIMARY_CLUSTER) {
            t_thrd.postmaster_cxt.HaShmData->current_mode = PRIMARY_MODE;
        } else if (SS_DISASTER_STANDBY_CLUSTER) {
            t_thrd.postmaster_cxt.HaShmData->current_mode = STANDBY_MODE;
        }
        ereport(LOG, (errmsg("[SS] SSDisasterUpdateHAmode change Ha current mode to: %d",
            t_thrd.postmaster_cxt.HaShmData->current_mode)));
    }
}

bool SSPerformingStandbyScenario()
{
    if (SS_IN_REFORM) {
        if (g_instance.dms_cxt.SSReformInfo.reform_type == DMS_REFORM_TYPE_FOR_NORMAL_OPENGAUSS &&
            ((uint64)(0x1 << SS_PRIMARY_ID) & g_instance.dms_cxt.SSReformInfo.bitmap_reconnect) == 0) {
            return true;
        } else if (g_instance.dms_cxt.SSReformInfo.reform_type == DMS_REFORM_TYPE_FOR_FULL_CLEAN) {
            return true;
        }
    }
    return false;
}

void SSGrantDSSWritePermission(void)
{
    /*
     * Whether dss_set_server_status_wrapper leads to dead loop. There exists occasion that database maybe block
     * here but fail and success log have no print. So it is suspected greatly that dss_set_server_status_wrapper
     * cause database stuck. 
     */
    ereport(LOG, (errmodule(MOD_DMS), (errmsg("[SS reform] set dss server status as primary start."))));
    while (dss_set_server_status_wrapper() != GS_SUCCESS) {
        pg_usleep(REFORM_WAIT_LONG);
        ereport(WARNING, (errmodule(MOD_DMS),
            errmsg("[SS reform] Failed to set DSS as primary, vgname: \"%s\", socketpath: \"%s\"",
                g_instance.attr.attr_storage.dss_attr.ss_dss_data_vg_name,
                g_instance.attr.attr_storage.dss_attr.ss_dss_conn_path),
                errhint("Check vgname and socketpath and restart later.")));
    }
    ereport(LOG, (errmodule(MOD_DMS), (errmsg("[SS reform] set dss server status as primary: success"))));
}

bool SSPrimaryRestartScenario()
{
    if (SS_IN_REFORM) {
        if (g_instance.dms_cxt.SSReformInfo.reform_type == DMS_REFORM_TYPE_FOR_NORMAL_OPENGAUSS &&
            ((uint64)(0x1 << SS_PRIMARY_ID) & g_instance.dms_cxt.SSReformInfo.bitmap_reconnect) != 0) {
            return true;
        }
    }
    return false;
}

/* 
 * PRIMARY_CLUSTER and single cluster
 *      1)standby scenario 
 *      2)primary restart -- no need exit
 *      3)switchover 
 *      4)failover -- need exit
 * STANDBY_CLUSTER
 *      all scenario  -- need exit
 */
bool SSBackendNeedExitScenario()
{
    if (!SS_IN_REFORM) {
        return false;
    }

    if (SS_DISASTER_STANDBY_CLUSTER) {
        return true;
    }

    if (g_instance.attr.attr_sql.enableRemoteExcute) {
        ereport(LOG, (errmsg("[SS] remote execute is enabled, all backends need exit to ensure"
                "complete transaction!")));
        return true;
    }

    if (SSPerformingStandbyScenario() || SSPrimaryRestartScenario()) {
        return false;
    }

    return true;
}

void SSProcessForceExit()
{
    int log_level = WARNING;
#ifdef USE_ASSERT_CHECKING
    log_level = PANIC;
#endif 
    ereport(log_level, (errmodule(MOD_DMS),
        errmsg("[SS reform] db exit directly by force now")));
    _exit(0);
}

void SSWaitStartupExit(bool send_signal)
{
    if (g_instance.pid_cxt.StartupPID == 0) {
        return;
    }

    if (send_signal) {
        if ((SS_STANDBY_FAILOVER && !g_instance.dms_cxt.SSRecoveryInfo.restart_failover_flag) ||
            SS_STANDBY_PROMOTING) {
            g_instance.dms_cxt.SSRecoveryInfo.startup_need_exit_normally = true;
            pg_memory_barrier();
        }
        SendPostmasterSignal(PMSIGNAL_DMS_TERM_STARTUP);
        ereport(LOG, (errmodule(MOD_DMS),
                errmsg("[SS reform] send terminate startup thread signal to PM.")));
    }

    
    if (SS_IN_REFORM && dms_reform_failed()) {
        ereport(WARNING, (errmodule(MOD_DMS),
            errmsg("[SS reform] reform failed.")));
    }
#ifdef USE_ASSERT_CHECKING
    ereport(LOG, (errmodule(MOD_DMS),
            errmsg("[SS reform] wait for the startup thread to exit")));
#else
    long rto_limit = SS_RTO_LIMIT;
    ereport(LOG, (errmodule(MOD_DMS),
        errmsg("[SS reform] wait startup thread exit until RTO limit time:%ld sec",
        rto_limit / (1000 * 1000))));
    
    long wait_time = 0;
#endif
    while (true) {
        if (g_instance.pid_cxt.StartupPID == 0) {
            break;
        }

        if (g_instance.dms_cxt.SSRecoveryInfo.recovery_trapped_in_page_request) {
            ereport(WARNING, (errmodule(MOD_DMS),
                    errmsg("[SS reform] recovery_trapped_in_page_request: Thread pageredo or startup are trapped"
                    "in page request during recovery phase, db exit directly by force now.")));
            _exit(0);
        }
#ifndef USE_ASSERT_CHECKING
        if (wait_time > rto_limit) {
            SSProcessForceExit();
        }
        wait_time += REFORM_WAIT_TIME;
#endif
        pg_usleep(REFORM_WAIT_TIME);
    }
}

void SSHandleStartupWhenReformStart(dms_reform_start_context_t *rs_cxt)
{
    if (g_instance.pid_cxt.StartupPID == 0) {
        return;
    }

    if (ENABLE_ONDEMAND_RECOVERY) {
        if (rs_cxt->reform_type == DMS_REFORM_TYPE_FOR_SWITCHOVER_OPENGAUSS &&
            rs_cxt->role != DMS_ROLE_REFORMER && ENABLE_ONDEMAND_REALTIME_BUILD) {
            g_instance.dms_cxt.SSRecoveryInfo.realtime_build_in_reform = true;
            ereport(LOG, (errmodule(MOD_DMS),
                errmsg("[SS reform][On-demand] reform start phase: stop ondemand realtime build before switchover.")));
            SSWaitStartupExit(true);
            return;
        /*
         * Ondemand recovery feature need to make sure startup-process alive in follow scenarios:
         * 1. Standby node does not need to stop startup process if ondemand realtime build is enable before normal reform.
         * 2. Primary node does not need to stop startup process in ondemand-redo pharse, because startup process is performing recovery.
         */
        } else if (SS_IN_ONDEMAND_RECOVERY || !SS_ONDEMAND_REALTIME_BUILD_DISABLED) {
            ereport(LOG, (errmodule(MOD_DMS),
                errmsg("[SS reform][On-demand] reform start phase: ondemand realtime build is enable, no need wait startup thread exit.")));
            return;
        }
    }

    if (SS_DISASTER_MAIN_STANDBY_NODE) {
        ereport(LOG, (errmodule(MOD_DMS),
            errmsg("[SS reform][Dual cluster] reform start phase: standby cluster main node "
            "no need wait startup thread exit.")));
        return;
    }

    ss_reform_info_t *reform_info = &g_instance.dms_cxt.SSReformInfo;
    if (reform_info->reform_ver == reform_info->reform_ver_startup_wait &&
        reform_info->reform_ver_startup_wait != 0) {
        SSWaitStartupExit(false);
    } else {
        ereport(WARNING, (errmodule(MOD_DMS),
            errmsg("[SS reform] reform start phase, last round reform version:%ld, startup wait version:%ld",
            reform_info->reform_ver, reform_info->reform_ver_startup_wait)));
        SSProcessForceExit();
    }
}

char* SSGetLogHeaderTypeStr()
{   
    if (!SS_IN_REFORM) {
        return "[SS]";
    }

    switch (g_instance.dms_cxt.SSReformInfo.reform_type)
    {
        case (DMS_REFORM_TYPE_FOR_NORMAL_OPENGAUSS):
            return "[SS reform]";
            break;
        case (DMS_REFORM_TYPE_FOR_FAILOVER_OPENGAUSS):
            return "[SS reform][SS failover]";
            break;
        case (DMS_REFORM_TYPE_FOR_SWITCHOVER_OPENGAUSS):
            return "[SS reform][SS swithover]";
            break;
        default:
            return "[SS]";
    }
}

void ProcessNoCleanBackendsScenario() {
    /* 
     * PM_WAIT_REFORM indicates that this node don't do actually thing during reform phase.
     * what only will be done currently is waiting this round of reform to finish.
     * 1. primary restart, standby need set PM_WAIT_REFORM
     * 2. standby restart, primary need set PM_WAIT_REFORM
     * 3. SS_PERFORMING_SWITCHOVER, primary and standby all return. Because primary need PM_RUN to 
     *    do ProcessDemoteRequest. 
     */
    if (SS_PERFORMING_SWITCHOVER) {
        return;    
    }

    if (((uint64)(0x1 << SS_MY_INST_ID) & g_instance.dms_cxt.SSReformInfo.bitmap_reconnect) == 0 &&
        !g_instance.dms_cxt.SSRecoveryInfo.startup_reform &&
        g_instance.dms_cxt.SSReformInfo.reform_type != DMS_REFORM_TYPE_FOR_FULL_CLEAN) {
        pmState = PM_WAIT_REFORM;
        ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS reform] no clean backends, pmState=%d, SSClusterState=%d, "
                      "demotion=%d-%d, rec=%d", pmState, g_instance.dms_cxt.SSClusterState, g_instance.demotion,
                      t_thrd.walsender_cxt.WalSndCtl->demotion, t_thrd.xlog_cxt.InRecovery))); 
    }
} 