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
#define BACKEND_TYPE_NORMAL 0x0001  /* normal backend */
#define BACKEND_TYPE_AUTOVAC 0x0002 /* autovacuum worker process */

#define WAIT_PMSTATE_UPDATE_TRIES 100

#define REFORM_CTRL_VERSION 1

#define SS_RTO_LIMIT (10 * 1000 * 1000) /* 10 sec */

typedef struct SSBroadcastCancelTrx {
    SSBroadcastOp type; // must be first
} SSBroadcastCancelTrx;

int SSReadXlogInternal(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, XLogRecPtr targetRecPtr, char *buf,
    int readLen);
XLogReaderState *SSXLogReaderAllocate(XLogPageReadCB pagereadfunc, void *private_data, Size alignedSize);
void SSGetRecoveryXlogPath();
char* SSGetNextXLogPath(TimeLineID tli, XLogRecPtr startptr);
void SSDisasterGetXlogPathList();
void SSUpdateReformerCtrl();
void SSReadControlFile(int id, bool updateDmsCtx = false);
void SSClearSegCache();
int SSCancelTransactionOfAllStandby(SSBroadcastOp type);
int SSProcessCancelTransaction(SSBroadcastOp type);
int SSXLogFileOpenAnyTLI(XLogSegNo segno, int emode, uint32 sources, char* xlog_path);
void SSStandbySetLibpqswConninfo();
void SSDisasterRefreshMode();
void SSDisasterUpdateHAmode();
bool SSPerformingStandbyScenario();
void SSGrantDSSWritePermission(void);
bool SSPrimaryRestartScenario();
bool SSBackendNeedExitScenario();
void SSWaitStartupExit(bool send_signal = true);
void SSHandleStartupWhenReformStart(dms_reform_start_context_t *rs_cxt);
char* SSGetLogHeaderTypeStr();
void ProcessNoCleanBackendsScenario();
void SSProcessForceExit();