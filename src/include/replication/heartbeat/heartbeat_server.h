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
 * heartbeat_server.h
 *         Heartbeat server.
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/heartbeat/heartbeat_server.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _HEARTBEAT_SERVER_H
#define _HEARTBEAT_SERVER_H
#include "replication/heartbeat/heartbeat_conn.h"

class HeartbeatServer {
public:
    HeartbeatServer(int epollfd);
    ~HeartbeatServer();
    bool Start();
    void Stop();
    bool Restart();
    bool AddConnection(HeartbeatConnection* conn, HeartbeatConnection** releasedConnPtr);
    void RemoveConnection(HeartbeatConnection* conn);
    void AddUnidentifiedConnection(HeartbeatConnection* conn, HeartbeatConnection** releasedConnPtr);
    void RemoveUnidentifiedConnection(HeartbeatConnection* conn);

private:
    bool InitListenConnections();
    bool IsAlreadyListen(const char* ip, int port) const;
    void ClearListenConnections();
    int ReleaseOldestUnidentifiedConnection(HeartbeatConnection** releasedConnPtr);
    void CloseListenSockets();

private:
    int epollfd_;
    bool started_;
    /* serverListenSocket_ is redundant, keep it for adapt ugly libpq interface */
    int serverListenSocket_[MAX_REPLNODE_NUM];
    HeartbeatConnection* listenConns_[MAX_REPLNODE_NUM];
    /* holds accepted and identified connections  */
    HeartbeatConnection* identifiedConns_[MAX_REPLNODE_NUM];
    /* holds accepted but unindentified connections */
    HeartbeatConnection* unidentifiedConns_[MAX_REPLNODE_NUM];
};

#endif /* _HEARTBEAT_SERVER_H */
