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
 *  dcf_flowcontrol.h
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/dcf/dcf_flowcontrol.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _DCF_FLOWCONTROL_H
#define _DCF_FLOWCONTROL_H

#include "c.h"
#include "cjson/cJSON.h"

#define DCF_MAX_STREAM_INFO_LEN 2048
#ifndef ENABLE_MULTIPLE_NODES

/*
 * Reply dcf message from standby (message type 'r').
 */
#define DCF_STANDBY_NAME_SIZE 1024
#define DCF_MAX_IP_LEN 64

typedef struct DCFStandbyReplyMessage {
    char id[DCF_STANDBY_NAME_SIZE];
    /*
     * The xlog locations that have been received, written, flushed, and applied by
     * standby-side. These may be invalid if the standby-side is unable to or
     * chooses not to report these.
     */
    XLogRecPtr receive;
    XLogRecPtr write;
    XLogRecPtr flush;
    XLogRecPtr apply;
    XLogRecPtr applyRead;

    /* local role on walreceiver, they will be sent to walsender */
    ServerMode peer_role;
    DbState peer_state;

    /* Sender's system clock at the time of transmission */
    TimestampTz sendTime;

    /*
     * If replyRequested is set, the server should reply immediately to this
     * message, to avoid a timeout disconnect.
     */
    bool replyRequested;
} DCFStandbyReplyMessage;
extern void ResetDCFNodesInfo(void);
extern void DCFProcessStandbyReplyMessage(uint32 srcNodeID, const char* msg, uint32 msgSize);
extern bool ResetDCFNodeInfoWithNodeID(uint32 nodeID);
extern bool SetNodeInfoByNodeID(uint32 nodeID, DCFStandbyReplyMessage reply, int *nodeIndex);
extern bool GetNodeInfos(cJSON **nodeInfos);
extern bool GetDCFNodeInfo(const cJSON *nodeJsons, int nodeID, char *role, int roleLen, char *ip, int ipLen, int *port);
extern bool QueryLeaderNodeInfo(uint32* leaderID, char* leaderIP, uint32 ipLen, uint32 *leaderPort);
#endif
#endif
