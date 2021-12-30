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
 * xlog_share_storage.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/storage/xlog_share_storage/xlog_share_storage.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef XLOG_SHARE_STORAGE_H
#define XLOG_SHARE_STORAGE_H

#include "postgres.h"
#include "knl/knl_variable.h"

void SharedStorageXlogCopyBackendMain(void);
void WakeUpXLogCopyerBackend();
void CheckShareStorageCtlInfo(XLogRecPtr localEnd);
bool XLogOverwriteFromLocal(bool force = false);
bool XLogOverwriteFromShare();
Size CalShareStorageCtlSize();
ShareStorageXLogCtl *AlignAllocShareStorageCtl();
void AlignFreeShareStorageCtl(ShareStorageXLogCtl *ctlInfo);
bool LockNasWriteFile(int fd);
bool UnlockNasWriteFile(int fd);

static inline void AddShareStorageXLopCopyBackendWakeupRequest()
{
    t_thrd.sharestoragexlogcopyer_cxt.wakeUp = true;
}

static inline void SendShareStorageXLogCopyBackendWakeupRequest()
{
    if (t_thrd.sharestoragexlogcopyer_cxt.wakeUp) {
        WakeUpXLogCopyerBackend();
    }
}

#endif
