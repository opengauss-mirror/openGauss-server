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
 *  heartbeat_conn.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/heartbeat/heartbeat_conn.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <sys/epoll.h>
#include <arpa/inet.h>
#include "postgres.h"
#include "knl/knl_variable.h"
#include "postgres_ext.h"
#include "utils/timestamp.h"
#include "utils/elog.h"
#include "replication/heartbeat/libpq/libpq.h"
#include "replication/heartbeat/libpq/libpq-be.h"
#include "replication/walreceiver.h"
#include "replication/heartbeat/heartbeat_conn.h"

static char *GetIp(struct sockaddr *addr)
{
    char *ip = (char *)palloc0(IP_LEN);
    struct sockaddr_in *saddr = (sockaddr_in *)addr;
    char *result = NULL;

    if (AF_INET6 == saddr->sin_family) {
        struct sockaddr_in6 *saddr6 = (sockaddr_in6 *)addr;
        result = inet_net_ntop(AF_INET6, &saddr6->sin6_addr, AF_INET6_MAX_BITS, ip, IP_LEN);
        if (result == NULL) {
            ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
        }
    } else if (AF_INET == saddr->sin_family) {
        result = inet_net_ntop(AF_INET, &saddr->sin_addr, AF_INET_MAX_BITS, ip, IP_LEN);
        if (result == NULL) {
            ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
        }
    } else {
        ereport(WARNING, (errmsg("Unsupported sin_family: %d", saddr->sin_family)));
    }

    if (result == NULL) {
        pfree(ip);
        ip = NULL;
    }

    return ip;
}

static bool IsIpInWhiteList(const char *ip)
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

        if (strncmp((char *)ipNoZone, ip, IP_LEN) == 0) {
            return true;
        }
    }
    return false;
}

HeartbeatConnection *MakeConnection(int fd, PurePort *port)
{
    HeartbeatConnection *con = (HeartbeatConnection *)palloc0(sizeof(HeartbeatConnection));

    /* The port is NULL for listen fd */
    if (port != NULL) {
        con->remoteHost = GetIp((struct sockaddr *)&port->raddr.addr);
        if (con->remoteHost == NULL) {
            FREE_AND_RESET(con);
            ereport(COMMERROR, (errmsg("Get the remote host ip failed.\n")));
            return NULL;
        }

        if (!IsIpInWhiteList(con->remoteHost)) {
            ereport(COMMERROR, (errmsg("Illegal remote ip: %s.\n", con->remoteHost)));
            FREE_AND_RESET(con);
            return NULL;
        }
    }

    con->fd = fd;
    con->port = port;
    con->channelIdentifier = INVALID_CHANNEL_ID;
    con->lastActiveTime = GetCurrentTimestamp();

    return con;
}

void ConnCloseAndFree(HeartbeatConnection *con)
{
    Assert(con != NULL);
    if (con == NULL) {
        ereport(COMMERROR, (errmsg("The heartbeat connection is empty.\n")));
        return;
    }

    if (con->port != NULL) {
        CloseAndFreePort(con->port);
        con->port = NULL;
        /*
         * If con->port is not NULL, the connection is not a listening connection.
         * It means con->port->sock is identical with con->fd.
         */
        con->fd = -1;
    }

    if (con->fd >= 0) {
        (void)close(con->fd);
    }

    FREE_AND_RESET(con->remoteHost);
    con->fd = -1;
    FREE_AND_RESET(con);
}

void RemoveConn(HeartbeatConnection *con)
{
    if (con != NULL) {
        EventDel(con->epHandle, con);
        ConnCloseAndFree(con);
    }
}

/* Add an event to epoll */
int EventAdd(int epoll_handle, int events, HeartbeatConnection *con)
{
    struct epoll_event epv = { 0, {0}};
    epv.data.ptr = con;
    epv.events = con->events = events;

    if (epoll_ctl(epoll_handle, EPOLL_CTL_ADD, con->fd, &epv) < 0) {
        ereport(COMMERROR, (errmsg("Event Add failed [fd=%d], evnets[%X]: %m", con->fd, (unsigned int)events)));
        return -1;
    }

    return 0;
}

/* Delete an event from epoll */
void EventDel(int epollFd, HeartbeatConnection *con)
{
    struct epoll_event epv = { 0, {0}};
    epv.events = 0;
    epv.data.ptr = con;

    if (epoll_ctl(epollFd, EPOLL_CTL_DEL, con->fd, &epv) < 0) {
        ereport(COMMERROR, (errmsg("EPOLL_CTL_DEL failed [fd=%d]: %m", con->fd)));
    }
}

void UpdateLastHeartbeatTime(const char *remoteHost, int remotePort, TimestampTz timestamp)
{
    volatile heartbeat_state *stat = t_thrd.heartbeat_cxt.state;
    char* ipNoZone = NULL;
    char ipNoZoneData[IP_LEN] = {0};

    ReplConnInfo* replconninfo = NULL;
    SpinLockAcquire(&stat->mutex);
    for (int i = 1; i < DOUBLE_MAX_REPLNODE_NUM; i++) {        
        if (i >= MAX_REPLNODE_NUM)
            replconninfo = t_thrd.postmaster_cxt.CrossClusterReplConnArray[i - MAX_REPLNODE_NUM];
        else
            replconninfo = t_thrd.postmaster_cxt.ReplConnArray[i]; 
        if (replconninfo == NULL) {
            continue;
        }

        /* remove any '%zone' part from an IPv6 address string */
        ipNoZone = remove_ipv6_zone(replconninfo->remotehost, ipNoZoneData, IP_LEN);

        if (strncmp((char *)ipNoZone, (char *)remoteHost, IP_LEN) == 0 &&
            replconninfo->remoteheartbeatport == remotePort) {
            stat->channel_array[i].last_reply_timestamp = timestamp;
            ereport(DEBUG2, (errmsg("Update last heartbeat  time： remotehost:%s, port:%d, time:%ld", remoteHost,
                                    remotePort, timestamp)));
            SpinLockRelease(&stat->mutex);
            return;
        }
    }
    SpinLockRelease(&stat->mutex);
    ereport(COMMERROR, (errmsg("Can't find channel, remote host:%s, remote port:%d", remoteHost, remotePort)));
}

bool SendPacket(const HeartbeatConnection *con, const char *buf, size_t len)
{
    int ret = 0;

    if ((ret = pq_putbytes(con->port, buf, len)) != 0) {
        ereport(COMMERROR, (errmsg("pq_putbytes failed, return ret=%d", ret)));
        return false;
    }

    if ((ret = pq_flush(con->port)) != 0) {
        ereport(COMMERROR, (errmsg("pq_flush failed, return ret=%d", ret)));
        return false;
    }
    return true;
}

bool SendHeartbeatPacket(const HeartbeatConnection *con)
{
    HeartbeatPacket packet;
    packet.sendTime = GetCurrentTimestamp();

    return SendPacket(con, (char *)(&packet), sizeof(HeartbeatPacket));
}

/*
 * ConnectCreate -- create a local connection data structure
 */
PurePort *ConnectCreate(int serverFd)
{
    PurePort *port = (PurePort *)palloc0(sizeof(PurePort));
    port->sock = -1;

    if (StreamConnection(serverFd, port) != STATUS_OK) {
        CloseAndFreePort(port);
        port = NULL;
    }

    return port;
}
