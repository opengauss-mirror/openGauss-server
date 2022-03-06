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
 *  dcf_flowcontrol.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/dcf/dcf_flowcontrol.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "c.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"
#include "cjson/cJSON.h"
#include "dcf_interface.h"
#include "replication/dcf_flowcontrol.h"
#include "pgstat.h"

#ifndef ENABLE_MULTIPLE_NODES

#ifdef ENABLE_UT
#define static
#endif

/* Statistics for log control */
static const int MICROSECONDS_PER_SECONDS = 1000000;
static const int MILLISECONDS_PER_SECONDS = 1000;
static const int MILLISECONDS_PER_MICROSECONDS = 1000;
static const int INIT_CONTROL_REPLY = 3;
static const int MAX_CONTROL_REPLY = 1000;
static const int SLEEP_MORE = 200;
static const int SLEEP_LESS = 400;
static const int SHIFT_SPEED = 3;

inline static void SetNodeInfo(int nodeIndex, uint32 nodeID, DCFStandbyReplyMessage reply)
{
    DCFStandbyInfo *standbyNodeInfo = &(t_thrd.dcf_cxt.dcfCtxInfo->nodes_info[nodeIndex]);
    standbyNodeInfo->nodeID = nodeID;
    standbyNodeInfo->isMember = true;
    standbyNodeInfo->isActive = true;
    standbyNodeInfo->receive = reply.receive;
    standbyNodeInfo->write = reply.write;
    standbyNodeInfo->flush = reply.flush;
    standbyNodeInfo->apply = reply.apply;
    standbyNodeInfo->peer_role = reply.peer_role;
    standbyNodeInfo->peer_state = reply.peer_state;
    standbyNodeInfo->sendTime = reply.sendTime;
}

static void ResetDCFNodeInfo(int index)
{
    DCFStandbyInfo *standbyNodeInfo = &(t_thrd.dcf_cxt.dcfCtxInfo->nodes_info[index]);
    standbyNodeInfo->isMember = false;
    standbyNodeInfo->isActive = false;
    standbyNodeInfo->nodeID = 0;
    standbyNodeInfo->receive = InvalidXLogRecPtr;
    standbyNodeInfo->write = InvalidXLogRecPtr;
    standbyNodeInfo->flush = InvalidXLogRecPtr;
    standbyNodeInfo->apply = InvalidXLogRecPtr;
    standbyNodeInfo->applyRead = InvalidXLogRecPtr;
    standbyNodeInfo->peer_role = UNKNOWN_MODE;
    standbyNodeInfo->peer_state = UNKNOWN_STATE;
    standbyNodeInfo->sendTime = 0;
}

static void ResetDCFNodeLogCtl(int index)
{
    DCFLogCtrlData *logCtrl = &(t_thrd.dcf_cxt.dcfCtxInfo->log_ctrl[index]);
    logCtrl->prev_sleep_time = -1;
    logCtrl->sleep_time = 0;
    logCtrl->balance_sleep_time = 0;
    logCtrl->prev_RTO = -1;
    logCtrl->current_RTO = -1;
    logCtrl->sleep_count = 0;
    logCtrl->sleep_count_limit = MAX_CONTROL_REPLY;
    logCtrl->prev_flush = 0;
    logCtrl->prev_apply = 0;
    logCtrl->prev_reply_time = 0;
    logCtrl->pre_rate1 = 0;
    logCtrl->pre_rate2 = 0;
    logCtrl->prev_RPO = -1;
    logCtrl->current_RPO = -1;
}

bool ResetDCFNodeInfoWithNodeID(uint32 nodeID)
{
    for (int i = 0; i < DCF_MAX_NODES; i++) {
        if (t_thrd.dcf_cxt.dcfCtxInfo->nodes_info[i].nodeID == nodeID) {
            ResetDCFNodeInfo(i);
            ResetDCFNodeLogCtl(i);
            return true;
        }
    }
    return false;
}

void ResetDCFNodesInfo(void)
{
    for (int i = 0; i < DCF_MAX_NODES; i++) {
        ResetDCFNodeInfo(i);
        ResetDCFNodeLogCtl(i);
    }
}

/* It is necessary to free nodeinfos outside when call this function */
bool GetNodeInfos(cJSON **nodeInfos)
{
    char replicInfo[DCF_MAX_STREAM_INFO_LEN] = {0};
    int len = dcf_query_stream_info(1, replicInfo, DCF_MAX_STREAM_INFO_LEN * sizeof(char));
    if (len == 0) {
        ereport(WARNING, (errmodule(MOD_DCF), errmsg("Failed to query dcf config!")));
        return false;
    }
    *nodeInfos = cJSON_Parse(replicInfo);
    if (*nodeInfos == nullptr) {
        const char* errorPtr = cJSON_GetErrorPtr();
        if (errorPtr != nullptr) {
            ereport(WARNING, (errmodule(MOD_DCF), errmsg("Failed to parse dcf config: %s!", errorPtr)));
        } else {
            ereport(WARNING, (errmodule(MOD_DCF), errmsg("Failed to parse dcf config: unkonwn error.")));
        }
        return false;
    }
    return true;
}

/* Get dcf role, ip and port from cJSON corresponding to its nodeID */
bool GetDCFNodeInfo(const cJSON *nodeJsons, int nodeID, char *role, int roleLen, char *ip, int ipLen, int *port)
{
    if (!cJSON_IsArray(nodeJsons)) {
        ereport(WARNING, (errmodule(MOD_DCF), errmsg("Must exist array format in the json file.")));
        return false;
    }
    const cJSON* nodeJson = nullptr;
    errno_t rc = EOK;
    cJSON_ArrayForEach(nodeJson, nodeJsons)
    {
        cJSON *idJson = cJSON_GetObjectItem(nodeJson, "node_id");
        if (idJson == nullptr) {
            ereport(WARNING, (errmodule(MOD_DCF), errmsg("No items with node id %d!", nodeID)));
            return false;
        }
        if (idJson->valueint == nodeID) {
            cJSON *roleJson = cJSON_GetObjectItem(nodeJson, "role");
            if (roleJson == nullptr) {
                ereport(WARNING, (errmodule(MOD_DCF), errmsg("No role item with node id %d!", nodeID)));
                return false;
            }
            rc = strcpy_s(role, roleLen, roleJson->valuestring);
            securec_check(rc, "\0", "\0");
            cJSON *ipJson = cJSON_GetObjectItem(nodeJson, "ip");
            if (ipJson == nullptr) {
                ereport(WARNING, (errmodule(MOD_DCF), errmsg("No ip item with node id %d!", nodeID)));
                return false;
            }
            rc = strcpy_s(ip, ipLen, ipJson->valuestring);
            securec_check(rc, "\0", "\0");
            cJSON *portJson = cJSON_GetObjectItem(nodeJson, "port");
            if (portJson == nullptr) {
                ereport(WARNING, (errmodule(MOD_DCF), errmsg("No port item with node id %d!", nodeID)));
                return false;
            }
            *port = portJson->valueint;
            return true;
        }
    }
    ereport(WARNING, (errmodule(MOD_DCF), errmsg("No node item with node id %d found!", nodeID)));
    return false;
}

bool GetNodeInfo(const uint32 nodeID, char *nodeIP, uint32 nodeIPLen, uint32 *nodePort)
{
    cJSON *nodeInfos = nullptr;
    /* nodeInfos is null when GetNodeInfos returned false */
    if (!GetNodeInfos(&nodeInfos)) {
        return false;
    }
    const cJSON *nodeJsons = cJSON_GetObjectItem(nodeInfos, "nodes");
    if (nodeJsons == nullptr) {
        cJSON_Delete(nodeInfos);
        ereport(ERROR, (errmodule(MOD_DCF), errmsg("Get nodes info failed from DCF!")));
        return false;
    }
    const int DCF_ROLE_LEN = 64;
    char localDCFRole[DCF_ROLE_LEN] = {0};
    if (!GetDCFNodeInfo(nodeJsons, nodeID, localDCFRole, DCF_ROLE_LEN, nodeIP, nodeIPLen, (int*)nodePort)) {
        cJSON_Delete(nodeInfos);
        return false;
    }
    cJSON_Delete(nodeInfos);
    return true;
}

bool SetNodeInfoByNodeID(uint32 nodeID, DCFStandbyReplyMessage reply, int *nodeIndex)
{
    *nodeIndex = -1;
    DCFStandbyInfo nodeInfo;
    /* Find if the src_node_id has added to nodes info */
    for (int i = 0; i < DCF_MAX_NODES; i++) {
        nodeInfo = t_thrd.dcf_cxt.dcfCtxInfo->nodes_info[i];
        if (nodeInfo.nodeID == nodeID) {
            *nodeIndex = i;
            break;
        }
    }
    if (*nodeIndex == -1) {
        for (int i = 0; i < DCF_MAX_NODES; i++) {
            nodeInfo = t_thrd.dcf_cxt.dcfCtxInfo->nodes_info[i];
            if (!nodeInfo.isMember) {
                *nodeIndex = i;
                break;
            }
        }
    }
    if (*nodeIndex == -1) {
        ereport(WARNING,
            (errmsg("Can't add the node info with node id %u for no space in dcf nodes info array", nodeID)));
        return false;
    }
    SetNodeInfo(*nodeIndex, nodeID, reply);
    return true;
}

bool IsForceUpdate(TimestampTz preSendTime, TimestampTz curSendTime)
{
    /* It is required to update rto when the time period of updating rto exceeds 2 second */
    long secToTime;
    int microsecToTime;
    long millisecTimeDiff = 0;
    TimestampDifference(preSendTime, curSendTime, &secToTime, &microsecToTime);
    millisecTimeDiff = secToTime * MILLISECONDS_PER_SECONDS
        + microsecToTime / MILLISECONDS_PER_MICROSECONDS;
    ereport(DEBUG1, (errmsg("The millisec_time_diff is %ld", millisecTimeDiff)));
    /* Update rto forcefully when the time interval exceeding 2s. */
    int secondsNum = 2;
    return (millisecTimeDiff > secondsNum * MILLISECONDS_PER_SECONDS);
}

static bool IsUpdateRto(int nodeIndex, TimestampTz sendTime)
{
    bool forceUpdate = false;
    uint64 curSleepCount = t_thrd.dcf_cxt.dcfCtxInfo->log_ctrl[nodeIndex].sleep_count;
    uint64 sleepCountLimit = t_thrd.dcf_cxt.dcfCtxInfo->log_ctrl[nodeIndex].sleep_count_limit;
    if (t_thrd.dcf_cxt.dcfCtxInfo->log_ctrl[nodeIndex].prev_reply_time > 0) {
        forceUpdate = IsForceUpdate(t_thrd.dcf_cxt.dcfCtxInfo->log_ctrl[nodeIndex].prev_reply_time, sendTime);
    }
    /* It is required to update rto every sleepCountLimit or when updating time period exceeds 1s */
    return ((sleepCountLimit > 0 && curSleepCount % sleepCountLimit == 0) || forceUpdate);
}

/* If it's called in call back func, ASSERT DcfCallBackThreadShmemInit true */
bool QueryLeaderNodeInfo(uint32* leaderID, char* leaderIP, uint32 ipLen, uint32 *leaderPort)
{
    Assert((t_thrd.dcf_cxt.is_dcf_thread && t_thrd.dcf_cxt.isDcfShmemInited) ||
           !t_thrd.dcf_cxt.is_dcf_thread);
    /* dcf_node_id > 0 */
    *leaderID = 0;
    uint32 tmpPort = 0;
    uint32 *port = &tmpPort;
    char tmpIP[DCF_MAX_IP_LEN] = {0};
    char *ip = tmpIP;
    uint32 leaderIPLen = DCF_MAX_IP_LEN;
    if (leaderPort != NULL) {
        port = leaderPort;
    }
    if (leaderIP != NULL) {
        ip= leaderIP;
    }
    if (ipLen != 0) {
        leaderIPLen = ipLen;
    }
    bool success = (dcf_query_leader_info(1, ip, leaderIPLen, port, leaderID) == 0);
    if (!success) {
        ereport(WARNING, (errmsg("DCF failed to query leader info.")));
        return false;
    }
    if (*leaderID == 0) {
        ereport(WARNING, (errmsg("DCF leader does not exist.")));
        return false;
    }
    return true;
}

static void SetGlobalRtoData(int nodeIndex, int srcNodeID, char *nodename)
{
    /* Update global rto info to show */
    RTOStandbyData *standbyData = &(g_instance.rto_cxt.dcf_rto_standby_data[nodeIndex]);
    int rc = strncpy_s(standbyData->id, DCF_STANDBY_NAME_SIZE, nodename, strlen(nodename));
    securec_check(rc, "\0", "\0");
    standbyData->current_rto = t_thrd.dcf_cxt.dcfCtxInfo->log_ctrl[nodeIndex].current_RTO;

    standbyData->current_sleep_time = t_thrd.dcf_cxt.dcfCtxInfo->log_ctrl[nodeIndex].sleep_time;

    standbyData->target_rto = t_thrd.dcf_cxt.dcfCtxInfo->targetRTO;

    char *remoteIP = standbyData->dest_ip;
    uint32 remotePort = 0;
    char *localIP = standbyData->source_ip;
    uint32 localPort = 0;
    uint32 leaderID = -1;
    if (GetNodeInfo(srcNodeID, remoteIP, IP_LEN, &remotePort)) {
        standbyData->dest_port = (int)remotePort;
    } else {
        ereport(WARNING, (errmsg("Get ip and port of node with nodeID %u failed", srcNodeID)));
    }
    if (QueryLeaderNodeInfo(&leaderID, localIP, IP_LEN, &localPort)) {
        standbyData->source_port = (int)localPort;
    } else {
        ereport(WARNING, (errmsg("Get ip and port of leader failed")));
    }
}

static inline uint64 LogCtrlCountBigSpeed(uint64 originSpeed, uint64 curSpeed)
{
    uint64 updateSpeed = (((originSpeed << SHIFT_SPEED) - originSpeed) >> SHIFT_SPEED) + curSpeed;
    return updateSpeed;
}

/*
 * Estimate the time standby need to flush and apply log.
 */
static void DCFLogCtrlCalculateCurrentRTO(const DCFStandbyReplyMessage *reply, const int nodeIndex)
{
    volatile DCFLogCtrlData *logCtrl = &(t_thrd.dcf_cxt.dcfCtxInfo->log_ctrl[nodeIndex]);
    long secToTime;
    int microsecToTime;
    if (XLByteLT(reply->receive, reply->flush) || XLByteLT(reply->flush, reply->apply) ||
        XLByteLT(reply->flush, logCtrl->prev_flush) || XLByteLT(reply->apply, logCtrl->prev_apply)) {
        return;
    }
    if (XLByteEQ(reply->receive, reply->apply)) {
        logCtrl->current_RTO = 0;
        return;
    }
    if (logCtrl->prev_reply_time == 0) {
        return;
    }
    uint64 part1 = reply->receive - reply->flush;
    uint64 part2 = reply->flush - reply->apply;
    uint64 part1Diff = reply->flush - logCtrl->prev_flush;
    uint64 part2Diff = reply->apply - logCtrl->prev_apply;

    TimestampDifference(logCtrl->prev_reply_time, reply->sendTime, &secToTime, &microsecToTime);
    long millisecTimeDiff = secToTime * MILLISECONDS_PER_SECONDS + (microsecToTime / MILLISECONDS_PER_MICROSECONDS);
    long timeThreshold = 10;
    if (millisecTimeDiff <= timeThreshold) {
        return;
    }

    /*
     * consumeRatePart1 and consumeRatePart2 is based on 7/8 previous_speed(walsnd->log_ctrl.pre_rate1) and 1/8
     * speed_now(part1_diff / millisec_time_diff). To be more precise and keep more decimal point, we expand speed_now
     * by multiply first then divide, which is (8 * previous_speed * 7/8 + speed_now) / 8.
     */
    if (logCtrl->pre_rate1 != 0) {
        logCtrl->pre_rate1 = LogCtrlCountBigSpeed(logCtrl->pre_rate1, (uint64)(part1Diff / millisecTimeDiff));
    } else {
        logCtrl->pre_rate1 = ((part1Diff / (uint64)millisecTimeDiff) << SHIFT_SPEED);
    }
    if (logCtrl->pre_rate2 != 0) {
        logCtrl->pre_rate2 = LogCtrlCountBigSpeed(logCtrl->pre_rate2, (uint64)(part2Diff / millisecTimeDiff));
    } else {
        logCtrl->pre_rate2 = ((uint64)(part2Diff / millisecTimeDiff) << SHIFT_SPEED);
    }

    uint64 consumeRatePart1 = (logCtrl->pre_rate1 >> SHIFT_SPEED);
    uint64 consumeRatePart2 = (logCtrl->pre_rate2 >> SHIFT_SPEED);
    if (consumeRatePart1 == 0)
        consumeRatePart1 = 1;

    if (consumeRatePart2 == 0)
        consumeRatePart2 = 1;

    uint64 secRTOPart1 = (part1 / consumeRatePart1) / MILLISECONDS_PER_SECONDS;
    uint64 secRTOPart2 = ((part1 + part2) / consumeRatePart2) / MILLISECONDS_PER_SECONDS;
    uint64 secRTO = (secRTOPart1 > secRTOPart2) ? secRTOPart1 : secRTOPart2;
    ereport(DEBUG4, (errmodule(MOD_RTO_RPO),
                     errmsg("The RTO estimated is = : %lu seconds. reply->reveive is %lu, reply->flush is %lu, "
                            "reply->apply is %lu, pre_flush is %lu, pre_apply is %lu, TimestampDifference is %ld, "
                            "consumeRatePart1 is %lu, consumeRatePart2 is %lu",
                            secRTO, reply->receive, reply->flush, reply->apply, logCtrl->prev_flush,
                            logCtrl->prev_apply, millisecTimeDiff, consumeRatePart1, consumeRatePart2)));
    t_thrd.dcf_cxt.dcfCtxInfo->log_ctrl[nodeIndex].current_RTO = secRTO;
}

/* Calculate the RTO and RPO changes and control the changes as long as one changes. */
static void DCFLogCtrlCalculateIndicatorChange(DCFLogCtrlData *logCtrl, int64 *gapDiff, int64 *gap)
{
    int64 rtoPrevGap = 0;
    int64 rtoGapDiff = 0;
    int64 rtoGap = 0;
    int64 rpoPrevGap = 0;
    int64 rpoGapDiff = 0;
    int64 rpoGap = 0;

    if (t_thrd.dcf_cxt.dcfCtxInfo->targetRTO > 0) {
        if (logCtrl->prev_RTO < 0) {
            logCtrl->prev_RTO = logCtrl->current_RTO;
        }
        /* The base number compared is targetRTO/2 */
        int balanceFactor = 2;
        int targetRTO = t_thrd.dcf_cxt.dcfCtxInfo->targetRTO / balanceFactor;
        int64 currentRTO = logCtrl->current_RTO;
        rtoGap = currentRTO - targetRTO;
        rtoPrevGap = logCtrl->prev_RTO - targetRTO;
        rtoGapDiff = rtoGap - rtoPrevGap;
    }

    if (t_thrd.dcf_cxt.dcfCtxInfo->targetRPO > 0) {
        if (logCtrl->prev_RPO < 0) {
            logCtrl->prev_RPO = logCtrl->current_RPO;
        }

        int targetRPO = t_thrd.dcf_cxt.dcfCtxInfo->targetRPO;
        int64 currentRPO = logCtrl->current_RPO;
        rpoGap = currentRPO - targetRPO;
        rpoPrevGap = logCtrl->prev_RPO - targetRPO;
        rpoGapDiff = rpoGap - rpoPrevGap;
    }

    if (abs(rpoGapDiff) > abs(rtoGapDiff)) {
        *gapDiff = rpoGapDiff;
        *gap = rpoGap;
    } else {
        *gapDiff = rtoGapDiff;
        *gap = rtoGap;
    }
    ereport(DEBUG4, (errmodule(MOD_RTO_RPO),
                     errmsg("[LogCtrlCalculateIndicatorChange] rto_gap=%d, rto_gap_diff=%d,"
                            "rpo_gap=%d, rpo_gap_diff=%d, gap=%d, gap_diff=%d",
                            (int)rtoGap, (int)rtoGapDiff, (int)rpoGap,
                            (int)rpoGapDiff, (int)*gap, (int)*gapDiff)));
}

/*
 * If current RTO/RPO is less than target_rto/time_to_target_rpo, primary need less sleep.
 * If current RTO/RPO is more than target_rto/time_to_target_rpo, primary need more sleep.
 * If current RTO/RPO equals to target_rto/time_to_target_rpo, primary will sleep.
 * according to balance_sleep to maintain rto.
 */
static void DCFLogCtrlCalculateSleepTime(int nodeIndex)
{
    DCFLogCtrlData *logCtrl = &(t_thrd.dcf_cxt.dcfCtxInfo->log_ctrl[nodeIndex]);
    if ((t_thrd.dcf_cxt.dcfCtxInfo->targetRTO == 0 && t_thrd.dcf_cxt.dcfCtxInfo->targetRPO == 0) ||
        logCtrl->current_RTO == 0) {
        logCtrl->sleep_time = 0;
        return;
    }
    int64 gapDiff;
    int64 gap;
    DCFLogCtrlCalculateIndicatorChange(logCtrl, &gapDiff, &gap);

    int64 sleepTime = logCtrl->sleep_time;
    /* use for rto log */
    int64 preTime = logCtrl->sleep_time;
    int balanceRange = 1;

    /* mark balance sleep time */
    if (abs(gapDiff) <= balanceRange) {
        if (logCtrl->balance_sleep_time == 0) {
            logCtrl->balance_sleep_time = sleepTime;
        } else {
            int64 balanceFactor = 2;
            logCtrl->balance_sleep_time = (logCtrl->balance_sleep_time + sleepTime) / balanceFactor;
        }
        ereport(DEBUG4, (errmodule(MOD_RTO_RPO), errmsg("The balance time for log control is : %ld microseconds",
            logCtrl->balance_sleep_time)));
    }

    if (abs(gap) <= balanceRange) { /* rto balance, currentRTO close to targetRTO */
        if (logCtrl->balance_sleep_time != 0) {
            sleepTime = logCtrl->balance_sleep_time;
        } else {
            sleepTime -= SLEEP_LESS;
        }
    } else if (gap > balanceRange) { /* need more sleep, currentRTO larger than targetRTO */
        sleepTime += SLEEP_MORE;
    } else if (gap < -balanceRange) { /* need less sleep, currentRTO less than targetRTO */
        sleepTime -= SLEEP_LESS;
    }
    sleepTime = (sleepTime >= 0) ? sleepTime : 0;
    sleepTime = (sleepTime < MICROSECONDS_PER_SECONDS) ? sleepTime : MICROSECONDS_PER_SECONDS;
    logCtrl->sleep_time = sleepTime;
    /* Report when sleep time exceeds 500ms */
    int threshold = 500000;
    if (logCtrl->sleep_time >= threshold) {
        volatile DCFStandbyInfo *nodeInfo = &(t_thrd.dcf_cxt.dcfCtxInfo->nodes_info[nodeIndex]);
        ereport(WARNING,
                (errmodule(MOD_RTO_RPO),
                 errmsg("Flow control report for node %d: current sleep time is %ld micronseconds, "
                        "current rto is %ld, target rto is %d, standby receive lsn is %X/%X, "
                        "previous flush lsn is %X/%X, flush lsn is %X/%X, previous apply lsn is %X/%X, "
                        "apply lsn is %X/%X, previous send time is %ld, current send time is %ld, "
                        "flush speed is %ld B/ms, replay speed is %ld B/ms",
                        nodeInfo->nodeID, logCtrl->sleep_time,
                        logCtrl->current_RTO, t_thrd.dcf_cxt.dcfCtxInfo->targetRTO,
                        (uint32)(nodeInfo->receive >> 32), (uint32)(nodeInfo->receive),
                        (uint32)(logCtrl->prev_flush >> 32), (uint32)(logCtrl->prev_flush),
                        (uint32)(nodeInfo->flush >> 32), (uint32)(nodeInfo->flush),
                        (uint32)(logCtrl->prev_apply >> 32), (uint32)(logCtrl->prev_apply),
                        (uint32)(nodeInfo->apply >> 32), (uint32)(nodeInfo->apply),
                        logCtrl->prev_reply_time, nodeInfo->sendTime,
                        logCtrl->pre_rate1 >> SHIFT_SPEED, logCtrl->pre_rate2 >> SHIFT_SPEED)));
    }
    /* log control take effect */
    if (preTime == 0 && logCtrl->sleep_time != 0) {
        ereport(LOG,
                (errmodule(MOD_RTO_RPO),
                 errmsg("Log control take effect, target_rto is %d, "
                        "current_rto is %ld, current the sleep time is %ld microseconds",
                        t_thrd.dcf_cxt.dcfCtxInfo->targetRTO, logCtrl->current_RTO,
                        logCtrl->sleep_time)));
    }
    /* log control take does not effect */
    if (preTime != 0 && logCtrl->sleep_time == 0) {
        ereport(LOG,
                (errmodule(MOD_RTO_RPO),
                 errmsg("Log control does not take effect, target_rto is %d, "
                        "current_rto is %ld, current the sleep time is %ld microseconds",
                        t_thrd.dcf_cxt.dcfCtxInfo->targetRTO,
                        logCtrl->current_RTO, logCtrl->sleep_time)));
    }
    ereport(DEBUG4,
            (errmodule(MOD_RTO_RPO),
             errmsg("The sleep time for log control is : %ld microseconds",
                    logCtrl->sleep_time)));
}

/*
 * Count the limit for sleep_count, it is based on sleep time.
 */
static void DCFLogCtrlCountSleepLimit(const int nodeIndex)
{
    volatile DCFLogCtrlData *logCtrl = &(t_thrd.dcf_cxt.dcfCtxInfo->log_ctrl[nodeIndex]);
    int64 sleepCountLimitCount = 0;
    if (logCtrl->sleep_time == 0) {
        sleepCountLimitCount = MAX_CONTROL_REPLY;
    } else {
        sleepCountLimitCount = INIT_CONTROL_REPLY * MICROSECONDS_PER_SECONDS / logCtrl->sleep_time;
        sleepCountLimitCount = (sleepCountLimitCount > MAX_CONTROL_REPLY) ?
            MAX_CONTROL_REPLY : sleepCountLimitCount;
    }
    if (sleepCountLimitCount <= 0) {
        sleepCountLimitCount = INIT_CONTROL_REPLY;
    }
    logCtrl->sleep_count_limit = sleepCountLimitCount;
    ereport(DEBUG1, (errmsg("Sleep count limit is %ld.", logCtrl->sleep_count_limit)));
}
static void DoActualSleep(int nodeID, int nodeIndex)
{
    /* try to control log sent rate so that standby can flush and apply log under RTO seconds */
    volatile DCFLogCtrlData *logCtrl = &(t_thrd.dcf_cxt.dcfCtxInfo->log_ctrl[nodeIndex]);
    if (logCtrl->sleep_time >= 0 && logCtrl->sleep_time != logCtrl->prev_sleep_time) {
        /* The logical replication sleep isn't considered now */
        dcf_pause_rep(1, nodeID, logCtrl->sleep_time);
    }
    logCtrl->sleep_count++;
}

static bool GetFlowControlParam(void)
{
    /* Get current rto and rpo */
    const int rtoLen = 10; /* 10 is enough for rto is between 0 and 3600 */
    char tempRTO[rtoLen] = {0};
    const int rpoLen = 10; /* 10 is enough for rpo is between 0 and 3600 */
    char tempRPO[rtoLen] = {0};
    if (dcf_get_param("DN_FLOW_CONTROL_RTO", tempRTO, rtoLen) != 0) {
        ereport(WARNING, (errmodule(MOD_RTO_RPO), errmsg("Get rto from dcf failed!")));
        return false;
    }
    t_thrd.dcf_cxt.dcfCtxInfo->targetRTO = atoi(tempRTO);
    ereport(DEBUG1, (errmodule(MOD_RTO_RPO),
                     errmsg("target rto got from dcf is %d",
                            t_thrd.dcf_cxt.dcfCtxInfo->targetRTO)));
    if (dcf_get_param("DN_FLOW_CONTROL_RPO", tempRPO, rpoLen) != 0) {
        ereport(WARNING, (errmodule(MOD_RTO_RPO), errmsg("Get rpo from dcf failed!")));
        return false;
    }
    t_thrd.dcf_cxt.dcfCtxInfo->targetRPO = atoi(tempRPO);
    ereport(DEBUG1, (errmodule(MOD_RTO_RPO),
                     errmsg("target rpo got from dcf is %d",
                            t_thrd.dcf_cxt.dcfCtxInfo->targetRPO)));
    return true;
}

static void ResetPreviousValue(int nodeIndex, DCFStandbyReplyMessage reply)
{
    DCFLogCtrlData *logCtrl = &(t_thrd.dcf_cxt.dcfCtxInfo->log_ctrl[nodeIndex]);
    logCtrl->prev_reply_time = reply.sendTime;
    logCtrl->prev_flush = reply.flush;
    logCtrl->prev_apply = reply.apply;
    logCtrl->prev_RTO = logCtrl->current_RTO;
    logCtrl->prev_sleep_time = logCtrl->sleep_time;
}

/*
 * Regular reply from standby advising of WAL positions on standby server.
 */
void DCFProcessStandbyReplyMessage(uint32 srcNodeID, const char* msg, uint32 msgSize)
{
    DCFStandbyReplyMessage reply;
    errno_t errorno = EOK;
    char *buf = NULL;
    int nodeIndex = -1;
    if (msgSize < (sizeof(DCFStandbyReplyMessage) + 1)) {
        ereport(WARNING, (errmsg("The size of msg didn't meet reply message and the size is %u\n", msgSize)));
        return;
    }

    /* skip first char */
    buf = const_cast<char*>(msg) + 1;
    errorno = memcpy_s(&reply,
                       sizeof(DCFStandbyReplyMessage),
                       buf,
                       sizeof(DCFStandbyReplyMessage));
    securec_check(errorno, "\0", "\0");

    ereport(DEBUG1, (errmsg("The src node id is %u, standby node name is %s "
                            "and receive %X/%X write %X/%X flush %X/%X apply %X/%X",
                            srcNodeID, reply.id,
                            (uint32)(reply.receive >> 32), (uint32)reply.receive,
                            (uint32)(reply.write >> 32), (uint32)reply.write,
                            (uint32)(reply.flush >> 32), (uint32)reply.flush,
                            (uint32)(reply.apply >> 32), (uint32)reply.apply)));
    /* Get which log_ctrl index stores the info of node id with srcNodeID */
    bool isSet = SetNodeInfoByNodeID(srcNodeID, reply, &nodeIndex);
    if (!isSet) {
        ereport(WARNING, (errmsg("Set node info with node ID %u failed!", srcNodeID)));
        return;
    }

    /* Get the current target rto and rpo */
    if (!GetFlowControlParam()) {
        ereport(WARNING, (errmsg("Get flow control parameters from dcf failed!")));
        return;
    }
    if (IsUpdateRto(nodeIndex, reply.sendTime)) {
        /* Current RPO calculation is not required now. */
        DCFLogCtrlCalculateCurrentRTO(&reply, nodeIndex);
        DCFLogCtrlCalculateSleepTime(nodeIndex);
        DCFLogCtrlCountSleepLimit(nodeIndex);
        DoActualSleep(srcNodeID, nodeIndex);
        ResetPreviousValue(nodeIndex, reply);
    }
    SetGlobalRtoData(nodeIndex, srcNodeID, reply.id);
}
#endif
