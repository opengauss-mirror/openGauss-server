/**
 * @file cma_connect.h
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-03
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */

#ifndef CMA_CONNECT_H
#define CMA_CONNECT_H

#include "cm/libpq-fe.h"
#include "cm/libpq-int.h"

#define MAX_PRE_CONN_CMS 2
#define MAX_CONN_TIMEOUT 3

extern CM_Conn* agent_cm_server_connect;
extern CM_Conn* GetConnToCmserver(uint32 nodeid);

void CloseConnToCmserver(void);

int cm_client_flush_msg(CM_Conn* conn);
int cm_client_send_msg(CM_Conn* conn, char msgtype, const char* s, size_t lenmsg);

#endif