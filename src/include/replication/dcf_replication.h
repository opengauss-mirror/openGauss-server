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
 * rto_statistic.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/dcf_replication.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef _DCF_REPLICATION_H
#define _DCF_REPLICATION_H

#ifndef ENABLE_MULTIPLE_NODES
#include "dcf_interface.h"
#include "cjson/cJSON.h"
#include "replication/dcf_flowcontrol.h"

#define DCF_QUERY_IDLE 30000 /* 30ms RTT */

#define DCF_CHECK_CONF_IDLE 3600000; /* 1 hour */

#define DCF_FOLLOWER_STR "FOLLOWER"
#define DCF_UNIT_S 1000 /* 1000ms */

typedef enum DcfCallBackType {
    NON_CALLBACK = 0,
    CONSENSUS_CALLBACK = 1,
    RECVLOG_CALLBACK,
    PROMOTE_DEMOTE_CALLBACK,
    DCF_EXCEPTION_CALLBACK,
    ELECTION_CALLBACK,
    PROCESSMSG_CALLBACK
} DcfCallBackType;

typedef enum {
    DCF_LOG_FILE_PERMISSION_600,
    DCF_LOG_FILE_PERMISSION_640,
    DCF_LOG_FILE_PERMISSION_UNKNOW
} DcfLogFilePerm;

typedef enum {
    DCF_LOG_PATH_PERMISSION_700,
    DCF_LOG_PATH_PERMISSION_750,
    DCF_LOG_PATH_PERMISSION_UNKNOW
} DcfLogPathPerm;
typedef enum {
    DCF_RUN_MODE_AUTO = 0,
    DCF_RUN_MODE_MANUAL = 1,
    DCF_RUN_MODE_DISABLE = 2
} DcfRunModePerm;

extern Size DcfContextShmemSize(void);
extern void DcfContextShmemInit(void);
extern bool InitPaxosModule();
extern bool SaveAppliedIndex(void);
extern int ConsensusLogCbFunc(unsigned int stream_id, unsigned long long paxosIdx, 
    const char* buf, unsigned int len, unsigned long long lsn, int error_no);
extern int ReceiveLogCbFunc(unsigned int stream_id, unsigned long long index, 
                     const char* buf, unsigned int len, unsigned long long lsn);
extern int PromoteOrDemote(unsigned int stream_id, dcf_role_t new_role);
extern int DCFExceptionCbFunc(unsigned int stream_id, dcf_exception_t dcfException);
extern int ElectionCbFunc(unsigned int stream_id, unsigned int new_leader);
extern int ProcessMsgCbFunc(unsigned int stream_id, unsigned int src_node_id, 
                            const char* msg, unsigned int msg_size);
extern void DcfThreadShmemInit(void);
extern bool SetDcfParams(void); 
extern void UpdateRecordIdxState(void);
extern void LaunchPaxos(void);
extern void StopPaxosModule(void);
extern void DcfLogTruncate(void);
extern bool QueryLeaderNodeInfo(uint32* leaderID, char* leaderIP = NULL, uint32 ipLen = 0, uint32 *leaderPort = NULL);
extern void CheckConfigFile(bool after_build = false);
extern bool DCFSendMsg(uint32 streamID, uint32 destNodeID, const char* msg, uint32 msgSize);
extern bool DCFSendXLogLocation(void);
extern void SetDcfNeedSyncConfig(void);
extern bool DcfArchiveRoachForPitrMaster(XLogRecPtr targetLsn);
extern void DcfSendArchiveXlogResponse(ArchiveTaskStatus *archive_task_status);
extern bool IsDCFReadyOrDisabled(void);
#endif
#endif 
