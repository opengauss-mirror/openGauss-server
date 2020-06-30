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
 * gs_server.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/alarm/gs_server.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef __GS_WARN_SERVER_H__
#define __GS_WARN_SERVER_H__

#include "alarm/gs_warn_common.h"
#include "libpq-fe.h"
#include "alarm/gt_threads.h"
#include "alarm/gs_config.h"

#define MAX_THREAD_COUNT 128

typedef enum tagGaussConnState {
    GAUSS_CONN_INIT,
    GAUSS_CONN_CONNECTED,
    GAUSS_CONN_CONNECT_FAILED,
    GAUSS_CONN_EXECUTING,
    GAUSS_CONN_ERROR,

    GAUSS_CONN_BUTT = 0xffffffff
} GAUSS_CONN_STATE;

typedef struct tagGaussConn {
    PGconn* connHandle;
    THREAD_LOCK globalsLock; /* Global lock */
    GAUSS_CONN_STATE state;
} GAUSS_CONN, *LP_GAUSS_CONN;

typedef enum tagServerStates {
    SERVER_STATE_INIT,
    SERVER_STATE_CONNECTED,
    SERVER_STATE_LISTENING,
    SERVER_STATE_EXITING,

    SERVER_STATE_BUTT = 0xffffffff
} GAUSS_SERVER_STATES;

typedef struct tagWmpGlobals {
    GT_SOCKET sock;   /* Server socket */
    GS_CONFIG config; /* Configuration parameters */
    GAUSS_CONN gaussConn;
} WARN_GLOBALS, *LP_WARN_GLOBALS;

typedef struct tagClientSession {
    GT_SOCKET sock;
    char* sendBuf;
    char* recvBuf;
} GS_CLIENT_SESSION, *LP_GS_CLIENT_SESSION;

/* Mock structure of Alarm.  If one exits in Gauss then use that. */
typedef struct tagGsAlarm {
    uint64 alarmId;
    uint32 moduleId;
    uint32 submoduleId;
    bool isResolved;
    char alarmParam[MAX_ALARM_PARAM_LEN];
} GS_ALARM, *LP_GS_ALARM;

extern LP_WARN_GLOBALS globals;
extern bool gIsExit;

void collectClientWarnings(LPGT_THREAD_S pstThread);
WARNERRCODE getNewThreadCtx(LPGT_THREAD_S* newthreadCtx);

#endif  //__GS_WARN_SERVER_H__
