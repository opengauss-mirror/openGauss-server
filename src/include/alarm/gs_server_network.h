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
 * gs_server_network.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/alarm/gs_server_network.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef __GS_SERVER_NETWORK_H__
#define __GS_SERVER_NETWORK_H__

#include "alarm/gs_warn_common.h"
#include "alarm/gs_server.h"

WARNERRCODE initNetworkLayer();

WARNERRCODE acceptOnSocket(GT_SOCKET listernSock, GT_SOCKET* iClientSock, char* ipAddr, uint16* psPort);

WARNERRCODE recvData(GT_SOCKET Sock, char* buf, int32 bufSize, int32* dataRecvSize);

WARNERRCODE sendPacketToClient(GT_SOCKET sock, char* pucBuf, int32 iSize);

WARNERRCODE sendAck(GT_SOCKET sock, char* sendBuf, int32 errCode);

void disconnectClient(GT_SOCKET sock);

WARNERRCODE retrySendAck(LP_GS_CLIENT_SESSION session, int32 errCode);

WARNERRCODE validateClientPacket(LPGT_PACKET_HEAD header);

#endif  //__GS_SERVER_NETWORK_H__
