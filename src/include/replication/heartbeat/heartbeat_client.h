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
 * heartbeat_client.h
 *         Heartbeat client.
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/heartbeat/heartbeat_client.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _HEARTBEAT_CLIENT_H
#define _HEARTBEAT_CLIENT_H
#include "replication/heartbeat/heartbeat_conn.h"

class HeartbeatClient {
public:
    HeartbeatClient(int epollfd);
    ~HeartbeatClient();
    bool Connect();
    void DisConnect();
    bool IsConnect() const;
    bool SendBeatHeartPacket();

private:
    bool InitConnection(HeartbeatConnection* con, int remotePort);
    bool SendStartupPacket(const HeartbeatConnection* con) const;

private:
    int epollfd_;
    bool isConnect_;
    HeartbeatConnection* hbConn_;
};

#endif /* _HEARTBEAT_CLIENT_H */
