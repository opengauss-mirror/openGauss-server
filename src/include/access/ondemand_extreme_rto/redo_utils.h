/*
 * Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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
 * redo_utils.h
 *
 * IDENTIFICATION
 *        src/include/access/ondemand_extreme_rto/redo_utils.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef ONDEMAND_EXTREME_RTO_REDO_UTILS_H
#define ONDEMAND_EXTREME_RTO_REDO_UTILS_H

#include "access/xlogproc.h"

typedef enum {
    PARSE_TYPE_DATA = 0,
    PARSE_TYPE_DDL,
    PARSE_TYPE_SEG,
} XLogRecParseType;

Size OndemandRecoveryShmemSize(void);
void OndemandRecoveryShmemInit(void);
void OndemandXlogFileIdCacheInit(void);
void OndemandXLogParseBufferInit(RedoParseManager *parsemanager, int buffernum, RefOperate *refOperate,
    InterruptFunc interruptOperte);
void OndemandXLogParseBufferDestory(RedoParseManager *parsemanager);
XLogRecParseState *OndemandXLogParseBufferAllocList(RedoParseManager *parsemanager, XLogRecParseState *blkstatehead,
    void *record);
void OndemandXLogParseBufferRelease(XLogRecParseState *recordstate);
XLogRecParseState *OndemandRedoReloadXLogRecord(XLogRecParseState *redoblockstate);
void OndemandRedoReleaseXLogRecord(XLogRecParseState *reloadBlockState);
void OnDemandSendRecoveryEndMarkToWorkersAndWaitForReach(int code);
void OnDemandWaitRedoFinish();
void OnDemandWaitRealtimeBuildShutDownInSwitchoverPromoting();
void OnDemandWaitRealtimeBuildShutDownInPartnerFailover();
void OnDemandWaitRealtimeBuildShutDown();
void OnDemandBackupControlFile(ControlFileData* controlFile);
XLogRecPtr GetRedoLocInCheckpointRecord(XLogReaderState *record);
void OnDemandUpdateRealtimeBuildPrunePtr();
XLogRecParseType GetCurrentXLogRecParseType(XLogRecParseState *preState);
void WaitUntilRealtimeBuildStatusToFailoverAndUpdatePrunePtr();

#endif /* ONDEMAND_EXTREME_RTO_REDO_UTILS_H */