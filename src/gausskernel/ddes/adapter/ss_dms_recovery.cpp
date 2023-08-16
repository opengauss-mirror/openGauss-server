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
 * ---------------------------------------------------------------------------------------
 *
 * ss_dms_recovery.cpp
 *        Provide common interface for recovery within DMS reform process.
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_dms_recovery.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xlog.h"
#include "access/xact.h"
#include "access/multi_redo_api.h"
#include "storage/standby.h"
#include "storage/pmsignal.h"
#include "storage/buf/bufmgr.h"
#include "storage/dss/fio_dss.h"
#include "storage/smgr/fd.h"
#include "storage/smgr/segment.h"
#include "postmaster/postmaster.h"
#include "storage/file/fio_device.h"
#include "replication/ss_cluster_replication.h"
#include "ddes/dms/ss_dms_bufmgr.h"
#include "ddes/dms/ss_dms_recovery.h"
#include "ddes/dms/ss_reform_common.h"
#include "access/double_write.h"
#include <sys/types.h>
#include <dirent.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

int SSGetPrimaryInstId()
{
    return g_instance.dms_cxt.SSReformerControl.primaryInstId;
}

void SSSavePrimaryInstId(int id)
{
    g_instance.dms_cxt.SSReformerControl.primaryInstId = id;
    SSSaveReformerCtrl();
}

void SSWaitStartupExit()
{
    if (g_instance.pid_cxt.StartupPID == 0) {
        return;
    }

    if (SS_STANDBY_FAILOVER && !g_instance.dms_cxt.SSRecoveryInfo.restart_failover_flag) {
        g_instance.dms_cxt.SSRecoveryInfo.startup_need_exit_normally = true;
    }
    SendPostmasterSignal(PMSIGNAL_DMS_TERM_STARTUP);
    int err_level = g_instance.dms_cxt.SSRecoveryInfo.startup_need_exit_normally ? LOG : WARNING;
    ereport(err_level, (errmodule(MOD_DMS), errmsg("[SS reform] reform failed, startup thread need exit")));

    while (true) {
        if (g_instance.pid_cxt.StartupPID == 0) {
            break;
        }

        if (g_instance.dms_cxt.SSRecoveryInfo.recovery_trapped_in_page_request) {
            ereport(WARNING, (errmodule(MOD_DMS), errmsg("[SS reform] pageredo or startup thread are trapped "
                "in page request during recovery phase, need exit")));
            _exit(0);
        }
        pg_usleep(5000L);
    }
}

/**
 * find reform failed in recovery phase, maybe other node restart
 * pageredo or startup thread may trapped in LockBuffer for page request
 * to solve this: reform_proc thread need exit process
 * 
 * reform failed during recovery phase has three situation
 * 1) primary restart 2) restart failover 3) alive failover
 *  
 * 1) primary restart 2) restart failover:
 *      gaussdb will restart
 * 3) alive failover:
 *      try to exit startup thread, 
 *      if success, gaussdb still alive and prepare next reform
 *      if not, gaussdb need exit cause pageredo or startup may trapped in LockBuffer
*/
bool SSRecoveryNodes()
{
    bool result = false;
    while (true) {
        /** why use lock:
         * time1 startup thread: update IsRecoveryDone, not finish UpdateControlFile
         * time2 reform_proc: finish reform, think ControlFile is ok
         * time3 DB crash
         * time4 read the checkpoint which created before failover. oops, it is wrong
         */
        LWLockAcquire(ControlFileLock, LW_SHARED);
        if (t_thrd.shemem_ptr_cxt.XLogCtl->IsRecoveryDone &&
            t_thrd.shemem_ptr_cxt.ControlFile->state == DB_IN_PRODUCTION) {
            LWLockRelease(ControlFileLock);
            result = true;
            break;
        }
        LWLockRelease(ControlFileLock);

        /* do not wait when on-demand HashMap build done */
        if (SS_ONDEMAND_BUILD_DONE) {
            result = true;
            break;
        }

        /* If main standby is set hot standby to on, when it reach consistency or recovery all xlogs in disk,
         * recovery phase could be regarded successful in hot_standby thus set pmState = PM_HOT_STANDBY, which
         * indicate database systerm is ready to accept read only connections.
         */
        if ((SS_STANDBY_CLUSTER_MAIN_STANDBY || IS_SS_REPLICATION_MAIN_STANBY_NODE) && pmState == PM_HOT_STANDBY) {
            result = true;
            break;
        }

        if (dms_reform_failed()) {
            SSWaitStartupExit();
            result = false;
            break;
        }
        pg_usleep(REFORM_WAIT_TIME);
    }
    return result;
}

bool SSRecoveryApplyDelay()
{
    if (!ENABLE_REFORM) {
        return false;
    }
    
    if (DORADO_STANDBY_CLUSTER || SS_REPLICATION_STANDBY_CLUSTER) {
        return true;
    }

    while (g_instance.dms_cxt.SSRecoveryInfo.recovery_pause_flag) {
        /* might change the trigger file's location */
        RedoInterruptCallBack();

        pg_usleep(REFORM_WAIT_TIME);
    }

    return true;
}

/* initialize reformer ctrl parameter when initdb */
void SSInitReformerControlPages(void)
{
    /*
     * If already exists control file, reformer page must have been initialized
     */
    struct stat st;
    if (stat(XLOG_CONTROL_FILE, &st) == 0 && S_ISREG(st.st_mode)) {
        SSReadControlFile(REFORM_CTRL_PAGE);
        if (g_instance.dms_cxt.SSReformerControl.list_stable != 0 ||
            g_instance.dms_cxt.SSReformerControl.primaryInstId == SS_MY_INST_ID) {
            (void)printf("[SS] ERROR: files from last install must be cleared.\n");
            ereport(ERROR, (errmsg("Files from last initdb not cleared")));
        }
        (void)printf("[SS] Current node:%d acknowledges cluster PRIMARY node:%d.\n",
            SS_MY_INST_ID, g_instance.dms_cxt.SSReformerControl.primaryInstId);
        return;
    }

    int fd = -1;
    char buffer[PG_CONTROL_SIZE] __attribute__((__aligned__(ALIGNOF_BUFFER))); /* need to be aligned */
    errno_t errorno = EOK;

    /*
     * Initialize list_stable and primaryInstId
     * First node to initdb is chosen as primary for now, and for first-time cluster startup.
     */
    Assert(stat(XLOG_CONTROL_FILE, &st) != 0 || !S_ISREG(st.st_mode));
    g_instance.dms_cxt.SSReformerControl.list_stable = 0;
    g_instance.dms_cxt.SSReformerControl.primaryInstId = SS_MY_INST_ID;
    g_instance.dms_cxt.SSReformerControl.recoveryInstId = INVALID_INSTANCEID;
    g_instance.dms_cxt.SSReformerControl.version = REFORM_CTRL_VERSION;
    g_instance.dms_cxt.SSReformerControl.clusterStatus = CLUSTER_NORMAL;
    g_instance.dms_cxt.SSReformerControl.clusterRunMode = RUN_MODE_PRIMARY;
    (void)printf("[SS] Current node:%d initdb first, will become PRIMARY for first-time SS cluster startup.\n",
        SS_MY_INST_ID);

    /* Contents are protected with a CRC */
    INIT_CRC32C(g_instance.dms_cxt.SSReformerControl.crc);
    COMP_CRC32C(g_instance.dms_cxt.SSReformerControl.crc, (char *)&g_instance.dms_cxt.SSReformerControl,
                offsetof(ss_reformer_ctrl_t, crc));
    FIN_CRC32C(g_instance.dms_cxt.SSReformerControl.crc);

    if (sizeof(ss_reformer_ctrl_t) > PG_CONTROL_SIZE) {
        ereport(PANIC, (errmsg("sizeof(ControlFileData) is larger than PG_CONTROL_SIZE; fix either one")));
    }

    errorno = memset_s(buffer, PG_CONTROL_SIZE, 0, PG_CONTROL_SIZE);
    securec_check(errorno, "", "");

    errorno = memcpy_s(buffer, PG_CONTROL_SIZE, &g_instance.dms_cxt.SSReformerControl, sizeof(ss_reformer_ctrl_t));
    securec_check(errorno, "", "");

    fd = BasicOpenFile(XLOG_CONTROL_FILE, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("could not create control file \"%s\": %m", XLOG_CONTROL_FILE)));
    }

    SSWriteInstanceControlFile(fd, buffer, REFORM_CTRL_PAGE, PG_CONTROL_SIZE);
    if (close(fd)) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not close control file: %m")));
    }
}

void SShandle_promote_signal()
{
    if (pmState == PM_WAIT_BACKENDS) {
        g_instance.pid_cxt.StartupPID = initialize_util_thread(STARTUP);
        Assert(g_instance.pid_cxt.StartupPID != 0);
        pmState = PM_STARTUP;
    }

    ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS failover] begin startup.")));
}


void ss_failover_dw_init_internal()
{
    /*
     * step 1: remove self dw file dw_exit close self dw
     * step 2: load old primary dw ,and finish dw recovery, exit
     * step 3: rebuild dw file and init self dw
     */

    char *dssdir = g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name;
    int old_primary_id = g_instance.dms_cxt.SSReformerControl.primaryInstId;
    int self_id = g_instance.attr.attr_storage.dms_attr.instance_id;
    if (!g_instance.dms_cxt.SSRecoveryInfo.startup_reform) {
        dw_exit(true);
        dw_exit(false);
    }

    dw_exit(true);
    dw_exit(false);
    ss_initdwsubdir(dssdir, old_primary_id);
    dw_ext_init();
    dw_init();
    g_instance.dms_cxt.finishedRecoverOldPrimaryDWFile = true;
    dw_exit(true);
    dw_exit(false);
    ss_initdwsubdir(dssdir, self_id);
    dw_ext_init();
    dw_init();
    g_instance.dms_cxt.finishedRecoverOldPrimaryDWFile = false;
    ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS failover] dw init finish")));
}

void ss_failover_dw_init()
{
    for (int i = 0; i < g_instance.ckpt_cxt_ctl->pgwr_procs.num; i++) {
        if (g_instance.pid_cxt.PageWriterPID[i] != 0) {
            signal_child(g_instance.pid_cxt.PageWriterPID[i], SIGTERM, -1);
        }
    }
    ckpt_shutdown_pagewriter();
    ss_failover_dw_init_internal();
    g_instance.dms_cxt.dw_init = true;
}

/* In serial switchover scenario, prevent this promoting node from reinitializing dw. */
void ss_switchover_promoting_dw_init()
{
    for (int i = 0; i < g_instance.ckpt_cxt_ctl->pgwr_procs.num; i++) {
        if (g_instance.pid_cxt.PageWriterPID[i] != 0) {
            signal_child(g_instance.pid_cxt.PageWriterPID[i], SIGTERM, -1);
        }
    }
    ckpt_shutdown_pagewriter();
    
    dw_exit(true);
    dw_exit(false);
    dw_ext_init();
    dw_init();
    g_instance.dms_cxt.dw_init = true;
    ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS switchover] dw init finished")));
}
