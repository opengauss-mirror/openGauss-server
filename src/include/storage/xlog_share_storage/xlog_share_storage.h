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

/* For shared storage mode */
typedef enum {
    XLOG_COPY_NOT = 0,
    XLOG_COPY_FROM_LOCAL,
    XLOG_FORCE_COPY_FROM_LOCAL,
    XLOG_COPY_FROM_SHARE
} XLogCopyMode;

void SharedStorageXlogCopyBackendMain(void);
void WakeUpXLogCopyerBackend();
void CheckShareStorageCtlInfo(XLogRecPtr localEnd);
bool XLogOverwriteFromLocal(bool force = false, XLogRecPtr setStart = InvalidXLogRecPtr);
bool XLogOverwriteFromShare();
ShareStorageXLogCtl *AlignAllocShareStorageCtl();
void AlignFreeShareStorageCtl(ShareStorageXLogCtl *ctlInfo);
bool LockNasWriteFile(int fd);
bool UnlockNasWriteFile(int fd);
void DoShareStorageXLogCopy(XLogCopyMode mode, XLogRecPtr start);

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
