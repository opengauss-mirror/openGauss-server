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
 * server_main.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/cm/server_main.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SERVER_MAIN_H
#define SERVER_MAIN_H

#include "common/config/cm_config.h"
#include "cm/libpq-be.h"
#include "cm/stringinfo.h"

#define CM_MAX_CONNECTIONS 1024
#define CM_MAX_THREADS 256

#define CM_MONITOR_THREAD_NUM 1
#define CM_HA_THREAD_NUM 1

#define MAXLISTEN 64

#define MAX_EVENTS 512

#define DEFAULT_THREAD_NUM 5

#define INVALIDFD (-1)

typedef void (*PCallback)(int fd, int events, void* arg);

typedef struct CM_Connection {
    int fd;
    int epHandle;
    int events;
    PCallback callback;
    void* arg;
    Port* port;
    CM_StringInfo inBuffer;
    long last_active;
    bool gss_check;
    gss_ctx_id_t gss_ctx;           /* GSS context */
    gss_cred_id_t gss_cred;         /* GSS credential */
    gss_name_t gss_name;            /* GSS target name */
    gss_buffer_desc gss_outbuf;     /* GSS output token */
} CM_Connection;

typedef struct CM_Connections {
    uint32 count;
    CM_Connection* connections[CM_MAX_CONNECTIONS + MAXLISTEN];
    pthread_rwlock_t lock;
} CM_Connections;

typedef struct CM_Thread {
    pthread_t tid;
    int type;
    int epHandle;

} CM_Thread;

typedef struct CM_Threads {
    uint32 count;
    CM_Thread threads[CM_MAX_THREADS];
} CM_Threads;

typedef struct CM_HAThread {
    CM_Thread thread;
    int heartbeat;
    CM_Connection conn;
} CM_HAThread;

typedef struct CM_HAThreads {
    uint32 count;
    CM_HAThread threads[CM_HA_THREAD_NUM];
} CM_HAThreads;

typedef struct CM_MonitorThread {
    CM_Thread thread;
} CM_MonitorThread;

typedef struct CM_MonitorNodeStopThread{
    CM_Thread thread;
} CM_MonitorNodeStopThread;

typedef enum CM_ThreadStatus {
    CM_THREAD_STARTING,
    CM_THREAD_RUNNING,
    CM_THREAD_EXITING,
    CM_THREAD_INVALID
} CM_ThreadStatus;

typedef struct CM_Server_HA_Status {
    int local_role;
    int peer_role;
    int status;
    bool is_all_group_mode_pending;
    pthread_rwlock_t ha_lock;
} CM_Server_HA_Status;

typedef struct CM_ConnectionInfo {
    /* Port contains all the vital information about this connection */
    Port* con_port;

} CM_ConnectionInfo;

#define THREAD_TYPE_HA 1
#define THREAD_TYPE_MONITOR 2
#define THREAD_TYPE_CTL_SERVER 3
#define THREAD_TYPE_AGENT_SERVER 4
#define THREAD_TYPE_INIT 5
#define THREAD_TYPE_ALARM_CHECKER 6

#define MONITOR_CYCLE_TIMER 1000000
#define MONITOR_CYCLE_TIMER_OUT 6000000
#define MONITOR_CYCLE_MAX_COUNT (MONITOR_CYCLE_TIMER_OUT / MONITOR_CYCLE_TIMER)

// ARBITRATE_DELAY_CYCLE 10s
#define MONITOR_INSTANCE_ARBITRATE_DELAY_CYCLE_MAX_COUNT (10)

#define MONITOR_INSTANCE_ARBITRATE_DELAY_CYCLE_MAX_COUNT2 (MONITOR_INSTANCE_ARBITRATE_DELAY_CYCLE_MAX_COUNT * 2)
#define BUILD_TIMER_OUT (60 * 60 * 2)
#define PROMOTING_TIME_OUT   (30)

#define CM_INSTANCE_GROUP_SIZE 128

#define INSTANCE_NONE_COMMAND 0
#define INSTANCE_COMMAND_WAIT_SEND_SERVER 1
#define INSTANCE_COMMAND_WAIT_SERVER_ACK 2
#define INSTANCE_COMMAND_WAIT_EXEC 3
#define INSTANCE_COMMAND_WAIT_EXEC_ACK 4

#define INSTANCE_COMMAND_SEND_STATUS_NONE 0
#define INSTANCE_COMMAND_SEND_STATUS_SENDING 1
#define INSTANCE_COMMAND_SEND_STATUS_OK 2
#define INSTANCE_COMMAND_SEND_STATUS_FAIL 3

#define INSTANCE_ROLE_NO_CHANGE 0
#define INSTANCE_ROLE_CHANGED 1

#define INSTANCE_ARBITRATE_DELAY_NO_SET 0
#define INSTANCE_ARBITRATE_DELAY_HAVE_SET 1
constexpr int NO_NEED_TO_SET_PARAM = -1;

typedef struct cm_instance_report_status {
    cm_instance_command_status command_member[CM_PRIMARY_STANDBY_NUM];
    cm_instance_datanode_report_status data_node_member[CM_PRIMARY_STANDBY_NUM];
    cm_instance_gtm_report_status gtm_member[CM_PRIMARY_STANDBY_NUM];
    cm_instance_coordinate_report_status coordinatemember;
    cm_instance_arbitrate_status arbitrate_status_member[CM_PRIMARY_STANDBY_NUM];
    uint32 time;
    uint32 term;
    int sync_with_etcd;
    int cma_kill_instance_timeout;
} cm_instance_report_status;

typedef struct cm_instance_group_report_status {
    pthread_rwlock_t lk_lock;
    cm_instance_report_status instance_status;
} cm_instance_group_report_status;

typedef struct cm_fenced_UDF_report_status {
    pthread_rwlock_t lk_lock;
    int heart_beat;
    int status;
} cm_fenced_UDF_report_status;

typedef enum ProcessingMode {
    BootstrapProcessing,  /* bootstrap creation of template database */
    InitProcessing,       /* initializing system */
    NormalProcessing,     /* normal processing */
    PostUpgradeProcessing /* Post upgrade to run script */
} ProcessingMode;

#define IsBootstrapProcessingMode() (Mode == BootstrapProcessing)
#define IsInitProcessingMode() (Mode == InitProcessing)
#define IsNormalProcessingMode() (Mode == NormalProcessing)
#define IsPostUpgradeProcessingMode() (Mode == PostUpgradeProcessing)

#define GetProcessingMode() Mode

#define SetProcessingMode(mode)                                                                        \
    do {                                                                                               \
        if ((mode) == BootstrapProcessing || (mode) == InitProcessing || (mode) == NormalProcessing || \
            (mode) == PostUpgradeProcessing)                                                           \
            Mode = (mode);                                                                             \
    } while (0)

extern void ProcessStartupPacket(int epollFd, int events, void* arg);
extern int cm_server_process_ha_startuppacket(CM_Connection* con, CM_StringInfo msg);
extern void get_config_param(const char* config_file, const char* srcParam, char* destParam, int destLen);
void set_pending_command(
    const uint32 &group_index,
    const int &member_index,
    const CM_MessageType &pending_command,
    const int &time_out = NO_NEED_TO_SET_PARAM,
    const int &full_build = NO_NEED_TO_SET_PARAM);

#endif
