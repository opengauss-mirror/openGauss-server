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
 *  heartbeat_server.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/heartbeat/heartbeat_server.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/timestamp.h"
#include "replication/heartbeat/libpq/libpq.h"
#include "replication/heartbeat/heartbeat_conn.h"
#include "replication/walreceiver.h"
#include "replication/heartbeat/heartbeat_server.h"

using namespace PureLibpq;

static void AcceptConn(int epollFd, int events, void *arg, void **releasedConnPtr);

HeartbeatServer::HeartbeatServer(int epollfd)
{
    epollfd_ = epollfd;
    started_ = false;

    for (int i = 0; i < MAX_REPLNODE_NUM; ++i) {
        serverListenSocket_[i] = PGINVALID_SOCKET;
        listenConns_[i] = NULL;
        identifiedConns_[i] = NULL;
        unidentifiedConns_[i] = NULL;
    }
}

HeartbeatServer::~HeartbeatServer()
{
    Stop();
}

bool HeartbeatServer::Start()
{
    if (started_) {
        ereport(LOG, (errmsg("The heartbeat server has been started.")));
        return true;
    }

    int status = 0;
    /*  To adapt ReplConnArray, i start from 1. */
    for (int i = START_REPLNODE_NUM; i < MAX_REPLNODE_NUM; i++) {
        if (t_thrd.postmaster_cxt.ReplConnArray[i] != NULL) {
            if (IsAlreadyListen(t_thrd.postmaster_cxt.ReplConnArray[i]->localhost,
                                t_thrd.postmaster_cxt.ReplConnArray[i]->localheartbeatport)) {
                continue;
            }

            status = StreamServerPort(AF_UNSPEC, t_thrd.postmaster_cxt.ReplConnArray[i]->localhost,
                                      (unsigned short)t_thrd.postmaster_cxt.ReplConnArray[i]->localheartbeatport,
                                      serverListenSocket_, MAX_REPLNODE_NUM);
            if (status != STATUS_OK) {
                ereport(COMMERROR, (errmsg("could not create Ha listen socket for ReplConnInfoArr[%d]\"%s:%d\"", i,
                                           t_thrd.postmaster_cxt.ReplConnArray[i]->localhost,
                                           t_thrd.postmaster_cxt.ReplConnArray[i]->localheartbeatport)));
                CloseListenSockets();
                return false;
            }
        }
    }

    if (InitListenConnections()) {
        started_ = true;
    } else {
        /* If initing one connection failed, clear all inited connections */
        ClearListenConnections();
    }
    return started_;
}

void HeartbeatServer::Stop()
{
    if (!started_) {
        ereport(LOG, (errmsg("The heartbeat server has not been started.")));
        return;
    }

    ereport(LOG, (errmsg("Stop the heartbeat server.")));
    for (int i = 0; i < MAX_REPLNODE_NUM; ++i) {
        if (listenConns_[i] != NULL) {
            RemoveConn(listenConns_[i]);
            listenConns_[i] = NULL;
        }

        if (identifiedConns_[i] != NULL) {
            RemoveConn(identifiedConns_[i]);
            identifiedConns_[i] = NULL;
        }

        if (unidentifiedConns_[i] != NULL) {
            RemoveConn(unidentifiedConns_[i]);
            unidentifiedConns_[i] = NULL;
        }

        serverListenSocket_[i] = PGINVALID_SOCKET;
    }
    started_ = false;
}

bool HeartbeatServer::Restart()
{
    Stop();
    return Start();
}

bool HeartbeatServer::InitListenConnections()
{
    int i;
    int ret;

    for (i = 0; i < MAX_REPLNODE_NUM; i++) {
        int listenFd = serverListenSocket_[i];

        if (listenFd == PGINVALID_SOCKET) {
            break;
        }

        ret = SetSocketNoBlock(listenFd);
        if (ret != STATUS_OK) {
            ereport(COMMERROR, (errmsg("SetSocketNoBlock failed.")));
            return false;
        }

        HeartbeatConnection *listenCon = MakeConnection(listenFd, NULL);
        if (listenCon == NULL) {
            ereport(COMMERROR,
                    (errmsg("makeConnection failed, listenCon is NULL,epollFd=%d, listenFd=%d.", epollfd_, listenFd)));
            return false;
        }

        listenCon->callback = AcceptConn;
        listenCon->epHandle = epollfd_;
        listenCon->arg = (void *)this;

        if (EventAdd(epollfd_, EPOLLIN, listenCon)) {
            ereport(COMMERROR, (errmsg("Add listen socket failed[fd=%d].", listenFd)));
            ConnCloseAndFree(listenCon);
            serverListenSocket_[i] = PGINVALID_SOCKET;
            return false;
        }
        listenConns_[i] = listenCon;
        ereport(DEBUG2, (errmsg("Add listen socket [fd=%d] OK , evnets[%X].", listenCon->fd, EPOLLIN)));
    }

    return true;
}

void HeartbeatServer::ClearListenConnections()
{
    for (int i = 0; i < MAX_REPLNODE_NUM; ++i) {
        if (listenConns_[i] != NULL) {
            RemoveConn(listenConns_[i]);
            listenConns_[i] = NULL;
        }
        serverListenSocket_[i] = PGINVALID_SOCKET;
    }
}

void HeartbeatServer::CloseListenSockets()
{
    for (int i = 0; i < MAX_REPLNODE_NUM; ++i) {
        if (serverListenSocket_[i] != PGINVALID_SOCKET) {
            (void)close(serverListenSocket_[i]);
            serverListenSocket_[i] = PGINVALID_SOCKET;
        }
    }
}

bool HeartbeatServer::IsAlreadyListen(const char *ip, int port) const
{
    int listen_index = 0;
    char sock_ip[IP_LEN] = {0};
    errno_t rc = 0;

    if (ip == NULL || port <= 0) {
        return false;
    }

    for (listen_index = 0; listen_index < MAX_REPLNODE_NUM; ++listen_index) {
        if (serverListenSocket_[listen_index] != PGINVALID_SOCKET) {
            struct sockaddr_storage saddr;
            socklen_t slen;
            char *result = NULL;
            rc = memset_s(&saddr, sizeof(saddr), 0, sizeof(saddr));
            securec_check(rc, "", "");

            slen = sizeof(saddr);
            if (getsockname(serverListenSocket_[listen_index], (struct sockaddr *)&saddr, (socklen_t *)&slen) < 0) {
                ereport(WARNING, (errmsg("Get socket name failed")));
                continue;
            }

            if (((struct sockaddr *) &saddr)->sa_family == AF_INET6) {
                char* ipNoZone = NULL;
                char ipNoZoneData[IP_LEN] = {0};

                /* remove any '%zone' part from an IPv6 address string */
                ipNoZone = remove_ipv6_zone((char *)ip, ipNoZoneData, IP_LEN);

                result = inet_net_ntop(AF_INET6, &((struct sockaddr_in6 *) &saddr)->sin6_addr,
                                       AF_INET6_MAX_BITS, sock_ip, IP_LEN);
                if (result == NULL) {
                    ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
                }

                if ((strcmp(ipNoZone, sock_ip) == 0) && (ntohs(((struct sockaddr_in6 *) &saddr)->sin6_port)) == port) {
                    return true;
                }
            } else if (((struct sockaddr *) &saddr)->sa_family == AF_INET) {
                result = inet_net_ntop(AF_INET, &((struct sockaddr_in *) &saddr)->sin_addr,
                                       AF_INET_MAX_BITS, sock_ip, IP_LEN);
                if (result == NULL) {
                    ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
                }

                if ((strcmp(ip, sock_ip) == 0) && (ntohs(((struct sockaddr_in *) &saddr)->sin_port)) == port) {
                    return true;
                }
            } else if (((struct sockaddr *) &saddr)->sa_family == AF_UNIX) {
                continue;
            }
        }
    }

    return false;
}

/*
 * If the channelIdentifier from client startup packet equals to
 * the remoteheartbeatport in the server's configure file, add conn to identifiedConns_.
 * else the conn is illegal and remmove it from unidentifiedConns_.
 *
 */
bool HeartbeatServer::AddConnection(HeartbeatConnection *con, HeartbeatConnection **releasedConnPtr)
{
    *releasedConnPtr = NULL;
    char* ipNoZone = NULL;
    char ipNoZoneData[IP_LEN] = {0};

    for (int i = START_REPLNODE_NUM; i < MAX_REPLNODE_NUM; i++) {
        ReplConnInfo *replconninfo = t_thrd.postmaster_cxt.ReplConnArray[i];
        if (replconninfo == NULL) {
            continue;
        }

        /* remove any '%zone' part from an IPv6 address string */
        ipNoZone = remove_ipv6_zone(replconninfo->remotehost, ipNoZoneData, IP_LEN);

        if (strncmp((char *)ipNoZone, con->remoteHost, IP_LEN) == 0 &&
            replconninfo->remoteheartbeatport == con->channelIdentifier) {
            if (identifiedConns_[i] != NULL) {
                /* remove old connection if has duplicated connections. */
                ereport(COMMERROR, (errmsg("The connection has existed and remove the old connection, "
                                           "remote ip: %s, remote heartbeat port:%d, old fd:%d, new fd:%d.",
                                           con->remoteHost, con->channelIdentifier, identifiedConns_[i]->fd, con->fd)));
                RemoveConn(identifiedConns_[i]);
                *releasedConnPtr = identifiedConns_[i];
            }
            identifiedConns_[i] = con;
            ereport(LOG, (errmsg("Adding connection successed, remote ip: %s, remote heartbeat port:%d.",
                                 con->remoteHost, con->channelIdentifier)));
            RemoveUnidentifiedConnection(con);
            return true;
        }
    }

    ereport(COMMERROR, (errmsg("The connection is illegal, remote ip: %s, remote heartbeat port:%d.", con->remoteHost,
                               con->channelIdentifier)));
    RemoveUnidentifiedConnection(con);
    return false;
}

/*
 * Remove an indentified connection.
 */
void HeartbeatServer::RemoveConnection(HeartbeatConnection *con)
{
    char* ipNoZone = NULL;
    char ipNoZoneData[IP_LEN] = {0};

    for (int i = START_REPLNODE_NUM; i < MAX_REPLNODE_NUM; i++) {
        ReplConnInfo *replconninfo = t_thrd.postmaster_cxt.ReplConnArray[i];
        if (replconninfo == NULL) {
            continue;
        }

        /* remove any '%zone' part from an IPv6 address string */
        ipNoZone = remove_ipv6_zone(replconninfo->remotehost, ipNoZoneData, IP_LEN);

        if (strncmp((char *)ipNoZone, con->remoteHost, IP_LEN) == 0 &&
            replconninfo->remoteheartbeatport == con->channelIdentifier) {
            if (identifiedConns_[i] == NULL) {
                ereport(COMMERROR, (errmsg("The connection is not existed, remote ip: %s, remote heartbeat port:%d.",
                                           con->remoteHost, con->channelIdentifier)));
            } else {
                ereport(LOG, (errmsg("Removing connection successed, remote ip: %s, remote heartbeat port:%d.",
                                     con->remoteHost, con->channelIdentifier)));
                identifiedConns_[i] = NULL;
            }
            return;
        }
    }
    ereport(COMMERROR, (errmsg("Remove an unexisted connection, remote ip: %s, remote heartbeat port:%d.",
                               con->remoteHost, con->channelIdentifier)));
}

void HeartbeatServer::AddUnidentifiedConnection(HeartbeatConnection *con, HeartbeatConnection **releasedConnPtr)
{
    for (int i = START_REPLNODE_NUM; i < MAX_REPLNODE_NUM; i++) {
        if (unidentifiedConns_[i] == NULL) {
            unidentifiedConns_[i] = con;
            return;
        }
    }

    /* Too many unidentified connections, release the oldest. */
    ereport(COMMERROR, (errmsg("Too many unidentified connections.")));
    int pos = ReleaseOldestUnidentifiedConnection(releasedConnPtr);
    unidentifiedConns_[pos] = con;
}

/* used for move con from unidentifiedConns to identifiedConns_, so can't free con */
void HeartbeatServer::RemoveUnidentifiedConnection(HeartbeatConnection *con)
{
    for (int i = START_REPLNODE_NUM; i < MAX_REPLNODE_NUM; i++) {
        if (unidentifiedConns_[i] == con) {
            unidentifiedConns_[i] = NULL;
            return;
        }
    }
    /* should never happen. */
    ereport(COMMERROR, (errmsg("Remove an not existed and unidentified connection, remote ip: %s.", con->remoteHost)));
}

/* release the oldest unidentified connection and return the position */
int HeartbeatServer::ReleaseOldestUnidentifiedConnection(HeartbeatConnection **releasedConnPtr)
{
    int oldestPos = -1;
    TimestampTz minTime = -1;
    for (int i = START_REPLNODE_NUM; i < MAX_REPLNODE_NUM; i++) {
        if (unidentifiedConns_[i] == NULL) {
            continue;
        }
        if (minTime == -1 || unidentifiedConns_[i]->lastActiveTime < minTime) {
            oldestPos = i;
            minTime = unidentifiedConns_[i]->lastActiveTime;
        }
    }

    if (oldestPos == -1) {
        /* something bad happen, exit. */
        ereport(FATAL, (errmsg("Can't find the oldest unidentified connection.")));
    }

    RemoveConn(unidentifiedConns_[oldestPos]);
    *releasedConnPtr = unidentifiedConns_[oldestPos];
    unidentifiedConns_[oldestPos] = NULL;
    return oldestPos;
}

/* The server process the heartbeat packet and reply. */
static void ProcessHeartbeatPacketServer(int epollFd, int events, void *arg, void **releasedConnPtr)
{
    ereport(DEBUG2, (errmsg("[server] process heartbeat")));

    *releasedConnPtr = NULL;
    HeartbeatConnection *con = (HeartbeatConnection *)arg;
    HeartbeatServer *server = (HeartbeatServer *)con->arg;

    HeartbeatPacket inPacket;
    if (pq_getbytes(con->port, (char *)(&inPacket), sizeof(HeartbeatPacket)) != 0) {
        ereport(LOG, (errmsg("connection closed by peer.")));
        server->RemoveConnection(con);
        RemoveConn(con);
        *releasedConnPtr = con;
        return;
    }

    con->lastActiveTime = GetCurrentTimestamp();
    UpdateLastHeartbeatTime(con->remoteHost, con->channelIdentifier, con->lastActiveTime);

    if (!SendHeartbeatPacket(con)) {
        server->RemoveConnection(con);
        RemoveConn(con);
        *releasedConnPtr = con;
    }
}

static void ProcessStartupPacket(int epollFd, int events, void *arg, void **releasedConnPtr)
{
    *releasedConnPtr = NULL;
    HeartbeatConnection *con = (HeartbeatConnection *)arg;
    HeartbeatServer *server = (HeartbeatServer *)con->arg;

    HeartbeatStartupPacket inPacket;
    inPacket.channelIdentifier = INVALID_CHANNEL_ID;
    if (pq_getbytes(con->port, (char *)(&inPacket), sizeof(HeartbeatStartupPacket)) != 0) {
        ereport(LOG, (errmsg("connection closed by peer.")));
        server->RemoveUnidentifiedConnection(con);
        RemoveConn(con);
        *releasedConnPtr = con;
        return;
    }

    if (inPacket.channelIdentifier == INVALID_CHANNEL_ID) {
        ereport(COMMERROR, (errmsg("Invalid channel id.")));
        server->RemoveUnidentifiedConnection(con);
        RemoveConn(con);
        *releasedConnPtr = con;
        return;
    }

    con->channelIdentifier = inPacket.channelIdentifier;
    con->lastActiveTime = GetCurrentTimestamp();
    con->callback = ProcessHeartbeatPacketServer;

    if (!server->AddConnection(con, (HeartbeatConnection **)releasedConnPtr)) {
        RemoveConn(con);
        *releasedConnPtr = con;
        return;
    }

    UpdateLastHeartbeatTime(con->remoteHost, con->channelIdentifier, con->lastActiveTime);
    if (!SendHeartbeatPacket(con)) {
        server->RemoveConnection(con);
        RemoveConn(con);
        *releasedConnPtr = con;
    }
}

/* Accept new connections from clients */
static void AcceptConn(int epollFd, int events, void *arg, void **releasedConnPtr)
{
    *releasedConnPtr = NULL;
    PurePort *port = NULL;
    HeartbeatConnection *listenCon = (HeartbeatConnection *)arg;
    if (listenCon == NULL) {
        ereport(COMMERROR, (errmsg("AcceptConn arg is NULL.")));
        return;
    }

    port = ConnectCreate(listenCon->fd);
    if (port != NULL) {
        HeartbeatConnection *newCon = MakeConnection(port->sock, port);
        if (newCon == NULL) {
            ereport(DEBUG1, (errmsg("makeConnection failed.")));
            CloseAndFreePort(port);
            port = NULL;
            return;
        }

        newCon->callback = ProcessStartupPacket;
        newCon->epHandle = epollFd;
        newCon->arg = listenCon->arg;

        /*
         * The connection has not send startup packet,
         * and store it in undentified connections first.
         */
        HeartbeatServer *server = (HeartbeatServer *)listenCon->arg;
        server->AddUnidentifiedConnection(newCon, (HeartbeatConnection **)releasedConnPtr);

        /* add new connection fd to main thread to process startup packet */
        if (EventAdd(epollFd, EPOLLIN, newCon)) {
            ereport(COMMERROR, (errmsg("Add new connection socket failed[fd=%d], evnets[%X].", port->sock, EPOLLIN)));
            server->RemoveUnidentifiedConnection(newCon);
            ConnCloseAndFree(newCon);
            newCon = NULL;
            return;
        }
        ereport(LOG, (errmsg("Accept new connection, socket [fd=%d], evnets[%X].", port->sock, EPOLLIN)));
    }
}
