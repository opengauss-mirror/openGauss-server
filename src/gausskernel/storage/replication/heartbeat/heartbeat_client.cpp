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
 *  heartbeat_client.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/replication/heartbeat/heartbeat_client.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <sys/epoll.h>
#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/timestamp.h"
#include "replication/heartbeat/libpq/libpq.h"
#include "replication/heartbeat/libpq/libpq-fe.h"
#include "replication/heartbeat/heartbeat_conn.h"
#include "replication/heartbeat/heartbeat_client.h"
#include "replication/walreceiver.h"

using namespace PureLibpq;

const int MAX_CONN_INFO = 1024;

HeartbeatClient::HeartbeatClient(int epollfd) : epollfd_(epollfd), isConnect_(false), hbConn_(NULL)
{
}

HeartbeatClient::~HeartbeatClient()
{
    DisConnect();
}


struct replconninfo* GetHeartBeatServerConnInfo(void)
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    char* ipNoZone = NULL;
    char ipNoZoneData[IP_LEN] = {0};

    if (walrcv->pid <= 0) {
        return NULL;
    }

    /* conn_channel will be set after walreceiver connected to upstream */
    if (*walrcv->conn_channel.localhost == '\0' ||
        *walrcv->conn_channel.remotehost == '\0') {
        return NULL;
    }

    struct replconninfo *conninfo = NULL;

    for (int i = 0; i < MAX_REPLNODE_NUM; i++) {
        conninfo = t_thrd.postmaster_cxt.ReplConnArray[i];
        if (conninfo == NULL) {
            continue;
        }

        /* remove any '%zone' part from an IPv6 address string */
        ipNoZone = remove_ipv6_zone(conninfo->remotehost, ipNoZoneData, IP_LEN);

        /* find target conninfo by conn_channel */
        if (strcmp(ipNoZone, (char*)walrcv->conn_channel.remotehost) == 0 &&
            conninfo->remoteport == walrcv->conn_channel.remoteport) {
            return conninfo;
        }
    }

    return NULL;
}

const int SLEEP_TIME = 1;

bool HeartbeatClient::Connect()
{
    if (isConnect_) {
        return true;
    }

    char connstr[MAX_CONN_INFO];
    struct replconninfo *conninfo = NULL;
    PurePort *port = NULL;
    int rcs = 0;
    int remotePort = -1;
    conninfo = GetHeartBeatServerConnInfo();
    if (conninfo == NULL) {
        return false;
    }

    rcs = snprintf_s(connstr, MAX_CONN_INFO, MAX_CONN_INFO - 1, "host=%s port=%d localhost=%s localport=%d connect_timeout=2",
                     conninfo->remotehost, conninfo->remoteheartbeatport, conninfo->localhost,
                     conninfo->localheartbeatport);
    securec_check_ss(rcs, "", "");
    port = PQconnect(connstr);
    if (port != NULL) {
        remotePort = conninfo->remoteheartbeatport;
        ereport(LOG, (errmsg("Connected to heartbeat primary :%s success.", connstr)));
    }

    if (port != NULL) {
        /* newCon takeover port */
        HeartbeatConnection *newCon = MakeConnection(port->sock, port);
        if (newCon == NULL) {
            ereport(COMMERROR, (errmsg("makeConnection failed.")));
            CloseAndFreePort(port);
            port = NULL;
            return false;
        }

        if (!InitConnection(newCon, remotePort)) {
            ConnCloseAndFree(newCon);
        }
    } else {
        ereport(COMMERROR, (errmsg("Failed to connect to heartbeat primary.")));
    }
    return isConnect_;
}

static void ProcessHeartbeatPacketClient(int epollFd, int events, void *arg, void **releasedConnPtr)
{
    ereport(DEBUG2, (errmsg("[client] process heartbeat.")));

    *releasedConnPtr = NULL;
    HeartbeatConnection *con = (HeartbeatConnection *)arg;
    HeartbeatPacket inPacket;
    if (pq_getbytes(con->port, (char *)(&inPacket), sizeof(HeartbeatPacket)) != 0) {
        ereport(LOG, (errmsg("connection closed by peer, disconnect.")));
        HeartbeatClient *client = (HeartbeatClient *)con->arg;
        client->DisConnect();
        *releasedConnPtr = con;
        return;
    }

    con->lastActiveTime = GetCurrentTimestamp();
    UpdateLastHeartbeatTime(con->remoteHost, con->channelIdentifier, con->lastActiveTime);
}

bool HeartbeatClient::InitConnection(HeartbeatConnection *con, int remotePort)
{
    int ret = SetSocketNoBlock(con->fd);
    if (ret != STATUS_OK) {
        ereport(COMMERROR, (errmsg("SetSocketNoBlock failed.")));
        return false;
    }

    con->callback = ProcessHeartbeatPacketClient;
    con->epHandle = epollfd_;
    con->channelIdentifier = remotePort;
    con->arg = (void *)this;

    if (EventAdd(epollfd_, EPOLLIN, con)) {
        ereport(COMMERROR, (errmsg("Add listen socket failed[fd=%d].", con->fd)));
        return false;
    }
    ereport(LOG, (errmsg("Add listen socket [fd=%d] OK , evnets[%X].", con->fd, EPOLLIN)));
    if (!SendStartupPacket(con)) {
        EventDel(con->epHandle, con);
        return false;
    }

    hbConn_ = con;
    isConnect_ = true;
    return true;
}

static int GetChannelId(char remotehost[IP_LEN], int remoteheartbeatport)
{
    struct replconninfo *replconninfo = NULL;
    char* ipNoZone = NULL;
    char ipNoZoneData[IP_LEN] = {0};

    for (int i = 0; i < MAX_REPLNODE_NUM; i++) {
        replconninfo = t_thrd.postmaster_cxt.ReplConnArray[i];
        if (replconninfo == NULL) {
            continue;
        }

        /* remove any '%zone' part from an IPv6 address string */
        ipNoZone = remove_ipv6_zone(replconninfo->remotehost, ipNoZoneData, IP_LEN);

        if (strncmp((char *)ipNoZone, (char *)remotehost, IP_LEN) == 0 &&
            replconninfo->remoteheartbeatport == remoteheartbeatport) {
            return replconninfo->localheartbeatport;
        }
    }
    ereport(COMMERROR,
            (errmsg("Failed to get channel id, remote host: %s, remote port:%d", remotehost, remoteheartbeatport)));
    return -1;
}

bool HeartbeatClient::SendStartupPacket(const HeartbeatConnection *con) const
{
    ereport(DEBUG2, (errmsg("[client] send statup packet")));
    HeartbeatStartupPacket packet;
    packet.channelIdentifier = GetChannelId(con->remoteHost, con->channelIdentifier);
    packet.sendTime = GetCurrentTimestamp();

    return SendPacket(con, (char *)(&packet), sizeof(HeartbeatStartupPacket));
}

bool HeartbeatClient::SendBeatHeartPacket()
{
    if (isConnect_) {
        if (SendHeartbeatPacket(hbConn_)) {
            ereport(DEBUG2, (errmsg("[client] send heartbeat")));
            return true;
        } else {
            ereport(COMMERROR, (errmsg("[client] Failed to send heartbeat, disconnect.")));
            DisConnect();
            return false;
        }
    }
    return false;
}

bool HeartbeatClient::IsConnect() const
{
    return isConnect_;
}

void HeartbeatClient::DisConnect()
{
    if (hbConn_ != NULL) {
        ereport(LOG, (errmsg("Disconnect the heartbeat client.")));
        RemoveConn(hbConn_);
    }
    hbConn_ = NULL;
    isConnect_ = false;
}
