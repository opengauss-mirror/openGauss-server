/**
 * @file cma_gtm.h
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-01
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */

#ifndef CMA_GTM_H
#define CMA_GTM_H

#include "gtm/gtm_c.h"
#include "gtm/utils/libpq-fe.h"
#include "cm/cm_c.h"

extern void* GtmModeMain(void* arg);
extern GTM_Conn* GetConnectionToGTM(char gtm_pid_file[MAXPGPATH], int connectTimeOut = 5);
extern void CloseGTMConnection(GTM_Conn* conn);

/* this is used for cm_agent to query gtm status. */
extern int query_gtm_status(GTM_Conn* conn, int* server_mode, int* connection_status, GlobalTransactionId* pxid,
    long* send_count, long* receive_count, int* sync_mode);

#endif