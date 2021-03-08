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
 * cms_global_params.h
 *
 * IDENTIFICATION
 *    src/include/cm/cm_server/cms_global_params.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CMS_GLOBAL_PARAMS_H
#define CMS_GLOBAL_PARAMS_H

#include <vector>
#include <string>
#include <set>
#include "common/config/cm_config.h"
#include "alarm/alarm.h"
#include "cm_server.h"
#include "cm/cm_c.h"
#include "cm/etcdapi.h"

using std::set;
using std::string;
using std::vector;


typedef struct azInfo {
    char azName[CM_AZ_NAME];
    uint32 nodes[CM_NODE_MAXNUM];
} azInfo;

typedef enum CMS_OPERATION_ {
    CMS_SWITCHOVER_DN = 0,
    CMS_FAILOVER_DN,
    CMS_BUILD_DN,
    CMS_DROP_CN,
    CMS_PHONY_DEAD_CHECK,
} cms_operation ;

typedef enum MAINTENANCE_MODE_ {
    MAINTENANCE_MODE_NONE = 0,
    MAINTENANCE_MODE_UPGRADE,
    MAINTENANCE_MODE_UPGRADE_OBSERVATION,
    MAINTENANCE_MODE_DILATATION,
    MAINTENANCE_NODE_UPGRADED_GRAYSCALE
} maintenance_mode;

/* data structures to record instances that are in switchover procedure */
typedef struct switchover_instance {
    uint32 node;
    uint32 instanceId;
    int instanceType;
} switchover_instance;
typedef struct DataNodeReadOnlyInfo {
    char dataNodePath[CM_PATH_LENGTH];
    uint32 instanceId;
    uint32 currReadOnly;
    uint32 lastReadOnly;
    uint32 currPreAlarm;
    uint32 lastPreAlarm;
    uint32 dataDiskUsage;
} DataNodeReadOnlyInfo;

typedef struct InstanceStatusKeyValue {
    char key[ETCD_KEY_LENGTH];
    char value[ETCD_VLAUE_LENGTH];
} InstanceStatusKeyValue;

typedef struct DynamicNodeReadOnlyInfo {
    char nodeName[CM_NODE_NAME];
    uint32 dataNodeCount;
    DataNodeReadOnlyInfo dataNode[CM_MAX_DATANODE_PER_NODE];

    DataNodeReadOnlyInfo coordinateNode;
    uint32 currLogDiskPreAlarm;
    uint32 lastLogDiskPreAlarm;
    uint32 logDiskUsage;
} DynamicNodeReadOnlyInfo;

typedef struct DatanodeDynamicStatus {
    int count;
    uint32 dnStatus[CM_PRIMARY_STANDBY_NUM];
} DatanodeDynamicStatus;

#define ELASTICGROUP "elastic_group"

#define PROCESS_NOT_EXIST 0
#define PROCESS_CORPSE 1
#define PROCESS_RUNNING 2

#define CMS_CURRENT_VERSION 1
#define LOGIC_CLUSTER_LIST "logic_cluster_name.txt"
#define CM_STATIC_CONFIG_FILE "cluster_static_config"
#define CM_CLUSTER_MANUAL_START "cluster_manual_start"
#define CM_INSTANCE_MANUAL_START "instance_manual_start"
#define MINORITY_AZ_START "minority_az_start"
#define MINORITY_AZ_ARBITRATE "minority_az_arbitrate_hist"
#define INSTANCE_MAINTANCE "instance_maintance"
#define CM_PID_FILE "cm_server.pid"

#define PRIMARY "PRIMARY"
#define STANDBY "STANDBY"
#define DELETED "DELETED"
#define DELETING "DELETING"
#define UNKNOWN "UNKNOWN"
#define NORMAL "NORMAL"
#define DATANODE_ALL 0
#define READONLY_OFF 0
#define READONLY_ON 1
#define DN_RESTART_COUNTS 3         /* DN restarts frequently due to core down */
#define DN_RESTART_COUNTS_IN_HOUR 8 /* DN restarts in hour */
#define TRY_TIME_GET_STATUSONLINE_FROM_ETCD 1
#define CMA_KILL_INSTANCE_BALANCE_TIME 10
/*
 * the sum of cm_server heartbeat timeout and arbitrate delay time must be less than
 * cm_agent disconnected timeout. or else, cm_agent may self-kill before cm_server
 * standby failover.
 */
#define AZ_MEMBER_MAX_COUNT (3)
#define INIT_CLUSTER_MODE_INSTANCE_DEAL_TIME 180
#define CM_SERVER_ARBITRATE_DELAY_CYCLE_MAX_COUNT 3
#define MAX_CN_COUNT 100
#define MAX_PATH_LEN 1024
#define THREAHOLD_LEN 10
#define BYTENUM 4
#define DEFAULT_THREAD_NUM 5
#define SWITCHOVER_SEND_CHECK_NUM 3
#define MAX_VALUE_OF_CM_PRIMARY_HEARTBEAT 86400
#define MAX_COUNT_OF_NOTIFY_CN 86400
#define MAX_VALUE_OF_PRINT 86400
#define CM_MAX_AUTH_TOKEN_LENGTH 65535
#define INSTANCE_ID_LEN 5
#define CM_GS_GUC_SEND_INTERVAL 3

#define CN_DELETE_DELAY_SECONDS 10
#define MAX_QUERY_DOWN_COUNTS 30

#define INSTANCE_DATA_NO_REDUCED 0 //  no reduce shard instanceId
#define INSTANCE_DATA_REDUCED 1   // reduce shard instanceId

#define SUCCESS_GET_VALUE 1
#define CAN_NOT_FIND_THE_KEY 2
#define FAILED_GET_VALUE 3

#define AZ1_INDEX 0                              // for the index flag, az1
#define AZ2_INDEX 1                              // for the index flag, az2
#define AZ3_INDEX 2                              // for the index flag, az3
#define AZ_ALL_INDEX (-1)                        // for the index flag, az1 and az2,az3


#define GS_GUC_SYNCHRONOUS_STANDBY_MODE 1
/*
 * ssh connect does not exit automatically when the network is fault,
 * this will cause cm_ctl hang for several hours,
 * so we should add the following timeout options for ssh.
 */
#define SSH_CONNECT_TIMEOUT "5"
#define SSH_CONNECT_ATTEMPTS "3"
#define SSH_SERVER_ALIVE_INTERVAL "15"
#define SSH_SERVER_ALIVE_COUNT_MAX "3"
#define PSSH_TIMEOUT_OPTION                                                                        \
    " -t 60 -O ConnectTimeout=" SSH_CONNECT_TIMEOUT " -O ConnectionAttempts=" SSH_CONNECT_ATTEMPTS \
    " -O ServerAliveInterval=" SSH_SERVER_ALIVE_INTERVAL " -O ServerAliveCountMax=" SSH_SERVER_ALIVE_COUNT_MAX " "
#define PSSH_TIMEOUT " -t 30 "


/*
 * the sum of cm_server heartbeat timeout and arbitrate delay time must be less than
 * cm_agent disconnected timeout. or else, cm_agent may self-kill before cm_server
 * standby failover.
 */
#define AZ_MEMBER_MAX_COUNT (3)
#define SWITCHOVER_DEFAULT_WAIT 1200  /* It needs an integer multiple of 3, because of sleep(3) */

#define AUTHENTICATION_TIMEOUT 60

#define CAN_NOT_SEND_SYNC_lIST 1
#define NOT_NEED_TO_SEND_SYNC_LIST 2
#define NEED_TO_SEND_SYNC_LIST 3

#define IS_CN_INSTANCEID(instanceId) \
    ((instanceId) > 5000 && (instanceId) < 6000)
#define IS_DN_INSTANCEID(instanceId) \
    ((instanceId) > 6000 && (instanceId) < 7000)
#define IS_GTM_INSTANCEID(instanceId) \
    ((instanceId) > 1000 && (instanceId) < 2000)

extern set<int> g_stopNodes;
extern set<int>::iterator g_stopNodeIter;
extern vector<switchover_instance> switchOverInstances;
extern vector<uint32> vecSortCmId;

extern const int HALF_HOUR;
extern const int MINUS_ONE;
extern const uint32 ZERO;
extern const uint32 FIVE;
extern bool backup_open;
extern global_barrier g_global_barrier_data;
extern global_barrier* g_global_barrier;

extern volatile arbitration_mode cm_arbitration_mode;
extern uint32 ctl_stop_cluster_server_halt_arbitration_timeout;
extern struct passwd* pw;
/* extern CM_Server_HA_Status g_HA_status_data; */
extern CM_Server_HA_Status* g_HA_status;
extern dynamicConfigHeader* g_dynamic_header;
extern cm_instance_role_group* g_instance_role_group_ptr;
extern CM_ConnEtcdInfo* g_sess;
extern EtcdTlsAuthPath g_tlsPath;
extern dynamicConfigHeader* g_dynamic_header;
extern cm_instance_group_report_status* g_instance_group_report_status_ptr;
extern CM_Connections gConns;
extern cm_instance_central_node g_central_node;
extern pthread_rwlock_t switchover_az_rwlock;
extern pthread_rwlock_t etcd_access_lock;
extern pthread_rwlock_t gsguc_feedback_rwlock;
extern pthread_rwlock_t g_finish_redo_rwlock;
extern syscalllock cmserver_env_lock;
extern synchronous_standby_mode current_cluster_az_status;
extern CM_Threads gThreads;
extern CM_HAThreads gHAThreads;
extern CM_MonitorThread gMonitorThread;
extern CM_MonitorNodeStopThread gMonitorNodeStopThread;
extern cm_fenced_UDF_report_status* g_fenced_UDF_report_status_ptr;
extern pthread_rwlock_t instance_status_rwlock;
extern pthread_rwlock_t dynamic_file_rwlock;
extern pthread_rwlock_t term_update_rwlock;
extern pthread_rwlock_t g_minorityArbitrateFileRwlock;
extern pthread_rwlock_t switchover_full_rwlock;
extern kerberos_group_report_status g_kerberos_group_report_status;
extern dynamic_cms_timeline* g_timeline;
extern DynamicNodeReadOnlyInfo *g_dynamicNodeReadOnlyInfo;
extern Alarm UnbalanceAlarmItem[1];
extern Alarm ServerSwitchAlarmItem[1];
extern Alarm* AbnormalEtcdAlarmList;
extern volatile logic_cluster_restart_mode cm_logic_cluster_restart_mode;
extern volatile cm_start_mode cm_server_start_mode;
extern volatile switchover_az_mode cm_switchover_az_mode;

extern THR_LOCAL ProcessingMode Mode;

extern const int majority_reelection_timeout_init;
#ifdef ENABLE_MULTIPLE_NODES
extern const int coordinator_deletion_timeout_init;
#endif
extern const int ctl_stop_cluster_server_halt_arbitration_timeout_init;
extern const int cn_dn_disconnect_to_delete_time;
extern int AbnormalEtcdAlarmListSize;
extern int switch_rto;
extern int force_promote;
extern int cm_auth_method;
extern int g_cms_ha_heartbeat;
extern int cm_server_arbitrate_delay_set;
extern int g_init_cluster_delay_time;
extern int g_sever_agent_conn_check_interval;
extern int datastorage_threshold_check_interval;
extern int max_datastorage_threshold_check;
extern int az_switchover_threshold;
extern int az_check_and_arbitrate_interval;
extern int az1_and_az2_connect_check_interval;
extern int az1_and_az2_connect_check_delay_time;
extern int phony_dead_effective_time;
extern int instance_phony_dead_restart_interval;
extern int enable_az_auto_switchover;
extern int cmserver_demote_delay_on_etcd_fault;
extern int cmserver_demote_delay_on_conn_less;
extern int cmserver_promote_delay_count;
extern int g_cmserver_promote_delay_count;
extern int g_cmserver_demote_delay_on_etcd_fault;
extern int g_monitor_thread_check_invalid_times;
extern int cm_server_current_role;
extern int ccn_change_delay_time;
extern int* cn_dn_disconnect_times;
extern int* g_lastCnDnDisconnectTimes;
extern int g_cms_ha_heartbeat_timeout[CM_PRIMARY_STANDBY_NUM];
extern int g_cms_ha_heartbeat_from_etcd[CM_PRIMARY_STANDBY_NUM];
extern int HAListenSocket[MAXLISTEN];


extern const uint32 min_normal_cn_number;
extern uint32 g_instance_manual_start_file_exist;
extern uint32 g_gaussdb_restart_counts;
extern uint32 g_cm_to_cm_report_sync_cycle_count;
extern uint32 g_current_node_index;
extern uint32 cm_server_arbitrate_delay_time_out;
extern uint32 arbitration_majority_reelection_timeout;
#ifdef ENABLE_MULTIPLE_NODES
extern uint32 g_cnDeleteDelayTimeForClusterStarting;
extern uint32 g_cnDeleteDelayTimeForDnWithoutPrimary;
extern uint32 g_cmd_disable_coordinatorId;
#endif
extern uint32 g_instance_failover_delay_time_from_set;
extern uint32 cmserver_gs_guc_reload_timeout;
extern uint32 serverHATimeout;
extern uint32 cmserver_switchover_timeout;
extern uint32 g_datanode_instance_count;
extern uint32 g_health_etcd_count_for_pre_conn;
extern uint32 cn_delete_default_time;
extern uint32 instance_heartbeat_timeout;
extern uint32 instance_failover_delay_timeout;
extern uint32 cmserver_ha_connect_timeout;
extern uint32 cmserver_ha_heartbeat_timeout;
extern uint32 cmserver_ha_status_interval;
extern uint32 cmserver_self_vote_timeout;
extern uint32 instance_keep_heartbeat_timeout;
extern uint32 cm_server_arbitrate_delay_base_time_out;
extern uint32 cm_server_arbitrate_delay_incrememtal_time_out;
#ifdef ENABLE_MULTIPLE_NODES
extern uint32 coordinator_heartbeat_timeout;
extern int32 g_cmAgentDeleteCn;
#endif
extern uint32 g_pre_agent_conn_count;
extern uint32 g_cm_agent_kill_instance_time;
extern uint32 cmserver_switchover_timeout;
extern uint32 cmserver_and_etcd_instance_status_for_timeout;
extern uint32 g_dropped_cn[MAX_CN_NUM];
extern uint32 g_instance_status_for_etcd[CM_PRIMARY_STANDBY_NUM];
extern uint32 g_instance_status_for_etcd_timeout[CM_PRIMARY_STANDBY_NUM];
extern uint32 g_node_index_for_cm_server[CM_PRIMARY_STANDBY_NUM];
extern uint32 g_instance_status_for_cm_server_timeout[CM_PRIMARY_STANDBY_NUM];
extern uint32 g_instance_status_for_cm_server[CM_PRIMARY_STANDBY_NUM];

extern azInfo g_azArray[CM_NODE_MAXNUM];
extern uint32 g_azNum;
extern bool do_finish_redo;
extern bool isNeedCancel;
extern bool g_init_cluster_mode;
extern bool g_gtm_free_mode;
extern bool g_open_new_logical;
extern bool isStart;
extern bool switchoverAZInProgress;
extern bool agentVotePrimary;
extern bool g_dnWithoutPrimaryFlag;
extern bool g_syncDNReadOnlyStatusFromEtcd;
extern bool switchoverFullInProgress;
extern bool g_elastic_exist_node;
extern bool g_kerberos_check_cms_primary_standby;
extern bool g_sync_dn_finish_redo_flag_from_etcd;
extern bool g_getHistoryDnStatusFromEtcd;
extern bool g_getHistoryCnStatusFromEtcd;
extern volatile uint32 g_refreshDynamicCfgNum;
extern bool g_needIncTermToEtcdAgain;
extern bool g_instance_status_for_cm_server_pending[CM_PRIMARY_STANDBY_NUM];
/* thread count of thread pool */
extern unsigned int cm_thread_count;

extern FILE* syslogFile;
extern char* cm_server_dataDir;
extern char sys_log_path[MAX_PATH_LEN];
extern char curLogFileName[MAXPGPATH];
extern char system_alarm_log[MAXPGPATH];
extern char cm_krb_server_keyfile[MAX_PATH_LEN];
extern char configDir[MAX_PATH_LEN];
extern char minority_az_start_file[MAX_PATH_LEN];
extern char g_minorityAzArbitrateFile[MAX_PATH_LEN];
extern char cm_instance_manual_start_path[MAX_PATH_LEN];

extern char g_enableSetReadOnly[10];
extern char g_enableSetReadOnlyThreshold[THREAHOLD_LEN];
extern char g_storageReadOnlyCheckCmd[MAX_PATH_LEN];
extern char cm_static_configure_path[MAX_PATH_LEN];
extern char cm_dynamic_configure_path[MAX_PATH_LEN];
extern char g_pre_agent_conn_flag[CM_MAX_CONNECTIONS];
extern char logic_cluster_list_path[MAX_PATH_LEN];
extern char instance_maintance_path[MAX_PATH_LEN];
extern char cm_manual_start_path[MAX_PATH_LEN];
extern char cm_force_start_file_path[MAX_PATH_LEN];

extern volatile int log_min_messages;
extern volatile int maxLogFileSize;
extern volatile int curLogFileNum;
extern volatile bool logInitFlag;
extern volatile bool cm_server_pending;

extern void clean_init_cluster_state();
extern void instance_delay_arbitrate_time_out_direct_clean(uint32 group_index, int member_index, uint32 delay_max_count);
extern void instance_delay_arbitrate_time_out_clean(
    int local_dynamic_role, int peerl_dynamic_role, uint32 group_index, int member_index, int delay_max_count);

extern int find_node_in_dynamic_configure(
    uint32 node, uint32 instanceId, int instanceType, uint32* group_index, int* member_index);

extern void change_primary_member_index(uint32 group_index, int primary_member_index);
extern void ChangeDnPrimaryMemberIndex(uint32 group_index, int primary_member_index);
extern void deal_dbstate_normal_primary_down(uint32 group_index, int instanceType);
extern int find_other_member_index(uint32 group_index, int member_index, int role, bool* majority);
extern int instance_delay_arbitrate_time_out(
    int local_dynamic_role, int peerl_dynamic_role, uint32 group_index, int member_index, int delay_max_count);
extern void CleanCommand(uint32 group_index, int member_index);
extern bool isLoneNode(int timeout);
extern int SetNotifyPrimaryInfoToEtcd(uint32 groupIndex, int memberIndex);
extern void deal_phony_dead_status(
    CM_Connection *con, int instanceRole, int group_index, int member_index, maintenance_mode mode);
extern void kill_instance_for_agent_fault(uint32 node, uint32 instanceId, int insType);
extern int find_other_member_index_for_DN_psd(uint32 group_index, int member_index);
extern int find_other_member_index_for_DN(uint32 group_index, int member_index, bool* majority);
extern int find_other_member_index_for_Gtm(uint32 group_index, int member_index);
extern int cmserver_getenv(const char* env_var, char* output_env_value, uint32 env_value_len, int elevel);
extern bool IsMaintenanceModeDisableOperation(const cms_operation &op, maintenance_mode mode);
extern void cm_pending_notify_broadcast_msg(uint32 group_index, uint32 instanceId);
extern inline bool isDisableSwitchoverDN(const maintenance_mode &mode);
extern inline bool isDisableFailoverDN(const maintenance_mode &mode);
extern inline bool isDisableDropCN(const maintenance_mode &mode);
extern inline bool isDisablePhonyDeadCheck(const maintenance_mode &mode);
extern inline bool isDisableBuildDN(const maintenance_mode &mode);
extern maintenance_mode getMaintenanceMode(const uint32 &group_index);
extern uint32 GetTermForMinorityStart(void);
extern cm_start_mode get_cm_start_mode(const char* path);

extern void initazArray(char azArray[][CM_AZ_NAME]);
bool isLargerNode();
void setStorageCheckCmd();

#endif
