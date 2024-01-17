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
 * ss_switchover.cpp
 *  Shared storage switchover routines.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_switchover.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "ddes/dms/ss_switchover.h"

#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include "miscadmin.h"

#include "port/pg_crc32c.h"
#include "utils/elog.h"
#include "utils/atomic.h"
#include "access/xlog.h"
#include "knl/knl_instance.h"
#include "securec.h"
#include "storage/procarray.h"
#include "replication/replicainternal.h"
#include "storage/smgr/fd.h"
#include "access/csnlog.h"
#include "access/twophase.h"
#include "access/htup.h"
#include "access/multixact.h"
#include "access/multi_redo_api.h"
#include "catalog/pg_database.h"
#include "access/xlog_internal.h"
#include "postmaster/startup.h"

#include "ddes/dms/ss_dms_callback.h"
#include "ddes/dms/ss_dms_bufmgr.h"
#include "ddes/dms/ss_transaction.h"
#include "ddes/dms/ss_reform_common.h"
#include "storage/file/fio_device.h"

void SSDoSwitchover()
{
    /* SSClusterState and in_reform should be set atomically for role judgement */
    Assert(g_instance.dms_cxt.SSClusterState == NODESTATE_NORMAL);
    ereport(LOG, (errmsg("[SS switchover] Starting switchover, "
        "current inst:%d will be promoted.", SS_MY_INST_ID)));
    (void)dms_switchover((unsigned int)t_thrd.myLogicTid);
}

void SSNotifySwitchoverPromote()
{
    SendPostmasterSignal(PMSIGNAL_DMS_SWITCHOVER_PROMOTE);
}

void SSHandleSwitchoverPromote()
{
    ereport(LOG, (errmsg("[SS switchover] Standby promote: begin StartupThread.")));
    Assert(g_instance.dms_cxt.SSReformerControl.primaryInstId != SS_MY_INST_ID);

    /* let StartupXLOG do the rest of switchover standby promotion */
    if (pmState == PM_WAIT_BACKENDS) {
        g_instance.pid_cxt.StartupPID = initialize_util_thread(STARTUP);
        Assert(g_instance.pid_cxt.StartupPID != 0);
        pmState = PM_STARTUP;
    }
    return;
}