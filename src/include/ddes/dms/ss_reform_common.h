/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * ss_reform_common.h
 *  include header file for ss reform common.
 * 
 * 
 * IDENTIFICATION
 *        src/include/ddes/dms/ss_reform_common.h
 *
 * ---------------------------------------------------------------------------------------
 */
#include "access/xlog_basic.h"
#include "access/xlogdefs.h"

#define BAK_CTRL_FILE_NUM 2
#define BIT_NUM_INT32 32
#define REFORM_WAIT_TIME 10000 /* 0.01 sec */
#define REFORM_WAIT_LONG 100000 /* 0.1 sec */
#define WAIT_REFORM_CTRL_REFRESH_TRIES 1000

#define REFORM_CTRL_VERSION 1

typedef struct SSBroadcastCancelTrx {
    SSBroadcastOp type; // must be first
} SSBroadcastCancelTrx;

int SSReadXlogInternal(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, XLogRecPtr targetRecPtr, char *buf,
    int readLen);
XLogReaderState *SSXLogReaderAllocate(XLogPageReadCB pagereadfunc, void *private_data, Size alignedSize);
void SSGetRecoveryXlogPath();
void SSSaveReformerCtrl(bool force = false);
void SSReadControlFile(int id, bool updateDmsCtx = false);
void SSClearSegCache();
int SSCancelTransactionOfAllStandby(SSBroadcastOp type);
int SSProcessCancelTransaction(SSBroadcastOp type);
int SSXLogFileReadAnyTLI(XLogSegNo segno, int emode, uint32 sources, char* xlog_path);
void SSStandbySetLibpqswConninfo();
