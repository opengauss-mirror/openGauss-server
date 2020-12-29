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
 * heartbeat_conn.h
 *        Data struct to store heartbeat connection.
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/heartbeat/heartbeat_conn.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _HEARTBEAT_CONN_H
#define _HEARTBEAT_CONN_H
#include "utils/timestamp.h"
#include "replication/heartbeat/libpq/libpq-be.h"

const int INVALID_CHANNEL_ID = -1;
const int AF_INET6_MAX_BITS = 128;
const int AF_INET_MAX_BITS = 32;
const int START_REPLNODE_NUM = 1;

typedef PureLibpq::Port PurePort;

#ifndef FREE_AND_RESET
#define FREE_AND_RESET(ptr)  \
    do {                     \
        if (NULL != (ptr)) { \
            pfree(ptr);      \
            (ptr) = NULL;    \
        }                    \
    } while (0)
#endif

typedef struct HeartbeatStartupPacket {
    int channelIdentifier; /* Use local heart beat listening port for channel identifier */
    TimestampTz sendTime;
} HeartbeatStartupPacket;

typedef struct HeartbeatPacket {
    TimestampTz sendTime;
} HeartbeatPacket;

/* releasedConnPtr keeps the connection ptr freed by callback */
typedef void (*PCallback)(int fd, int events, void* arg, void** releasedConnPtr);

typedef struct HeartbeatConnection {
    int fd;
    int epHandle;
    int events;
    int channelIdentifier; /* use remote heartbeat port for channel identifier. */
    PurePort* port;
    char* remoteHost;
    TimestampTz lastActiveTime;
    PCallback callback;
    void* arg;
} HeartbeatConnection;

HeartbeatConnection* MakeConnection(int fd, PurePort* port);
int EventAdd(int epoll_handle, int events, HeartbeatConnection* conn);
void EventDel(int epollFd, HeartbeatConnection* conn);
bool SendHeartbeatPacket(const HeartbeatConnection* conn);
bool SendPacket(const HeartbeatConnection* con, const char* buf, size_t len);
PurePort* ConnectCreate(int serverFd);
void ConnCloseAndFree(HeartbeatConnection* conn);
void RemoveConn(HeartbeatConnection* con);
void UpdateLastHeartbeatTime(const char* remoteHost, int remotePort, TimestampTz timestamp);

#endif /* _HEARTBEAT_CONN_H */
