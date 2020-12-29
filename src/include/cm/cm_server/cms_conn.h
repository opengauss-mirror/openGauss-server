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
 * -------------------------------------------------------------------------
 *
 * cms_conn.h
 *
 * IDENTIFICATION
 *    src/include/cm/cm_server/cms_conn.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CMS_CONN_H
#define CMS_CONN_H

#include "cm/libpq-fe.h"
#include "cm_server.h"


#define CM_SERVER_PACKET_ERROR_MSG 128
#define MSG_COUNT_FOR_LOG 300

extern int ServerListenSocket[MAXLISTEN];

void* CM_ThreadMain(void* argp);
void RemoveCMAgentConnection(CM_Connection* con);
void ConnCloseAndFree(CM_Connection* con);
void CloseHAConnection(CM_Connection* con);
void set_socket_timeout(Port* my_port, int timeout);

Port* ConnCreate(int serverFd);
void ConnFree(Port* conn);
int initMasks(const int* ListenSocket, fd_set* rmask);
int CMHandleCheckAuth(CM_Connection* con);
int cm_server_flush_msg(CM_Connection* conn);

int EventAdd(int epoll_handle, int events, CM_Connection* con);
void EventDel(int epollFd, CM_Connection* con);
void CMPerformAuthentication(CM_Connection* con);
int ReadCommand(Port* myport, CM_StringInfo inBuf);

#endif