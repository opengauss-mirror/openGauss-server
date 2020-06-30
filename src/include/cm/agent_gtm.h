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
 * agent_gtm.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/cm/agent_gtm.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef AGENT_GTM_H
#define AGENT_GTM_H

#include "gtm/gtm_c.h"
#include "gtm/libpq-fe.h"
#include "cm/cm_c.h"

extern GTM_Conn* GetConnectionToGTM(char gtm_pid_file[MAXPGPATH]);
extern void CloseGTMConnection(GTM_Conn* conn);

/*this is used for cm_agent to query gtm status.*/
extern int query_gtm_status(GTM_Conn* conn, int* server_mode, int* connection_status, GlobalTransactionId* pxid,
    long* send_count, long* receive_count, int* sync_mode);

#endif /*AGENT_GTM_H*/