/**
 * @file cma_global_params.h
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-01
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */

#ifndef CMA_GLOBAL_PARAMS_H
#define CMA_GLOBAL_PARAMS_H

#include "cm/cm_c.h"
#include "cm/cm_misc.h"
#include "cm/etcdapi.h"
#include "cma_main.h"

typedef enum MAINTENANCE_MODE_ {
    MAINTENANCE_MODE_NONE = 0,
    MAINTENANCE_MODE_UPGRADE,
    MAINTENANCE_MODE_UPGRADE_OBSERVATION,
    MAINTENANCE_MODE_DILATATION,
} maintenance_mode;

#define CM_SERVER_DATA_DIR "cm_server"
#define CM_INSTANCE_REPLACE "instance_replace"
#define PROC_NET_TCP "/proc/net/tcp"

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

/* UDF_DEFAULT_MEMORY is 200*1024 kB */
/* Must be same as UDF_DEFAULT_MEMORY in memprot.h and udf_memory_limit in cluster_guc.conf */
#define UDF_DEFAULT_MEMORY (200 * 1024)
#define AGENT_RECV_CYCLE (200 * 1000)
#define AGENT_REPORT_ETCD_CYCLE 600
#define INSTANCE_ID_LEN 8
#define SINGLE_INSTANCE 0
#define SINGLE_NODE 1
#define ALL_NODES 2
#define COMM_PORT_TYPE_DATA 1
#define COMM_PORT_TYPE_CTRL 2
#define MAX_RETRY_TIME 10
#define MAX_INSTANCE_BUILD 3
#define COMM_DATA_DFLT_PORT 7000
#define COMM_CTRL_DFLT_PORT 7001

#define INSTANCE_START_CYCLE 20
#define INSTANCE_BUILD_CYCLE 10

#define STARTUP_DN_CHECK_TIMES 3
#define STARTUP_CN_CHECK_TIMES 3
#define STARTUP_GTM_CHECK_TIMES 3
#define STARTUP_CMS_CHECK_TIMES 3

#define LISTEN 10

extern struct passwd* pw;
extern FILE* lockfile;
extern EtcdSession g_sess;
extern cm_instance_central_node g_central_node;
extern gtm_status_info gtm_report_msg;
extern coordinate_status_info cn_report_msg;
extern datanode_status_info dn_report_msg[CM_MAX_DATANODE_PER_NODE];
extern etcd_status_info etcd_report_msg;
extern kerberos_status_info g_kerberos_report_msg;
/* Enable the datanode incremental build mode */
extern volatile bool incremental_build;
extern volatile bool security_mode;
extern bool enable_xc_maintenance_mode;
extern char sys_log_path[MAX_PATH_LEN];
extern FILE* syslogFile;
extern LocalMaxLsnMng g_LMLsn[CM_MAX_DATANODE_PER_NODE];
extern pthread_t g_repair_cn_thread;
extern struct timeval gServerHeartbeatTime;
extern struct timeval gDisconnectTime;

extern EtcdTlsAuthPath g_tlsPath;
extern pthread_t g_thread_id[CM_MAX_DATANODE_PER_NODE + 5];
extern syscalllock cmagent_env_lock;

extern const char* progname;
extern char configDir[MAX_PATH_LEN];
extern char bin_path[MAX_PATH_LEN];
extern char cmagent_lockfile[MAX_PATH_LEN];
extern char run_long_command[MAX_LOGIC_DATANODE * CM_NODE_NAME + 64];
extern char cn_name_buf[MAXPGPATH];
extern char logic_cluster_list_path[MAXPGPATH];
extern char system_call_log_name[MAXPGPATH];
extern char result_path[MAX_PATH_LEN];
extern char cm_agent_log_path[MAX_PATH_LEN];
extern char cm_static_configure_path[MAX_PATH_LEN];
extern char cm_manual_start_path[MAX_PATH_LEN];
extern char cm_instance_manual_start_path[MAX_PATH_LEN];
extern char cm_resuming_cn_stop_path[MAX_PATH_LEN];
extern int datanode_num;
extern char datanode_names[MAX_LOGIC_DATANODE][CM_NODE_NAME];
extern char datanode_name_oids[MAX_LOGIC_DATANODE][CM_NODE_NAME];
extern char system_alarm_log[MAXPGPATH];
extern char dn_name_buf[MAX_LOGIC_DATANODE * CM_NODE_NAME];
extern char node_group_members1[MAX_NODE_GROUP_MEMBERS_LEN];
extern char node_group_members2[MAX_NODE_GROUP_MEMBERS_LEN];
extern char cm_cluster_resize_path[MAX_PATH_LEN];
extern char cm_cluster_replace_path[MAX_PATH_LEN];
extern char system_call_log[MAXPGPATH];
extern char g_unix_socket_directory[MAXPGPATH];
extern char log_base_path[MAXPGPATH];
extern char g_enable_cn_auto_repair[10];
extern char g_enableOnlineOrOffline[10];
extern char g_enableLogCompress[10];
extern char instance_maintance_path[MAX_PATH_LEN];

extern volatile bool g_repair_cn;
extern bool shutdown_request;
extern bool g_sync_dropped_coordinator;
extern bool exit_flag;
extern bool cm_do_force;          /*  stop by force */
extern bool g_pgxc_node_consist;
extern bool cm_agent_first_start;
extern bool cm_static_config_need_verify_to_coordinator;
extern bool cm_server_need_reconnect;
extern bool cm_agent_need_alter_pgxc_node;
extern bool cm_agent_need_check_libcomm_port;
extern bool is_cma_building_dn[CM_MAX_DATANODE_PER_NODE];
extern bool needUpdateSecboxConf;
extern bool gtm_disk_damage;
extern bool cn_disk_damage;
extern bool cms_disk_damage;
extern bool fenced_UDF_stopped;
extern bool cn_nic_down;
extern bool cms_nic_down;
extern bool gtm_nic_down;
extern bool dn_disk_damage[CM_MAX_DATANODE_PER_NODE];
extern bool dn_build[CM_MAX_DATANODE_PER_NODE];
extern bool nic_down[CM_MAX_DATANODE_PER_NODE];
extern bool g_cm_server_instance_pending;
extern bool cm_agent_first_start;
extern bool isStart;
extern bool suppressAlarm;
extern bool g_need_reload_active;

extern const uint32 AGENT_WATCH_DOG_THRESHOLD;
/* cm_agent config parameters */
extern uint32 agent_report_interval;
extern uint32 agent_heartbeat_timeout;
extern uint32 agent_connect_timeout;
extern uint32 agent_backup_open;
extern uint32 agent_connect_retries;
extern uint32 agent_check_interval;
extern uint32 agent_kill_instance_timeout;
extern uint32 agent_kerberos_status_check_interval;
extern uint32 agentDiskUsageStatusCheckTimes;
extern uint32 cn_auto_repair_delay;
extern uint32 dilatation_shard_count_for_disk_capacity_alarm;
extern uint32 g_check_disc_instance_now;
extern uint32 g_nodeId;
extern uint32 g_health_cn_for_repair;
extern uint32 agent_phony_dead_check_interval;
extern uint32 g_agentCheckTStatusInterval;
extern uint32 g_agentToDb;
extern uint32 thread_dead_effective_time;
extern uint32 enable_gtm_phony_dead_check;
extern uint32 g_cmaConnectCmsInOtherNodeCount;
extern uint32 g_cmaConnectCmsPrimaryInLocalNodeCount;
extern uint32 g_cmaConnectCmsInOtherAzCount;
extern uint32 g_cmaConnectCmsPrimaryInLocalAzCount;
extern uint32 dn_build_check_times[CM_MAX_DATANODE_PER_NODE];
extern uint32 g_node_index_for_cm_server[CM_PRIMARY_STANDBY_NUM];

extern const int cn_repair_build_full;
extern const int cn_repair_build_incre;
extern int cm_shutdown_level; /* cm_ctl stop single instance, single node or all nodes */
extern int cm_shutdown_mode;  /* fast shutdown */
extern int cn_frequent_restart_counts;
extern int g_CnDnPairsCount;
extern int is_catalog_changed;
extern int g_currenPgxcNodeNum;
extern int normalStopTryTimes;
extern int gtm_start_counts;
extern int co_start_counts;
extern int dn_start_counts[CM_MAX_DATANODE_PER_NODE];
extern int primary_dn_restart_counts[CM_MAX_DATANODE_PER_NODE];
extern int primary_dn_restart_counts_in_hour[CM_MAX_DATANODE_PER_NODE];

extern int startCMSCount;
extern int startCNCount;
extern int startDNCount[CM_MAX_DATANODE_PER_NODE];
extern int startGTMCount;
extern int g_clean_drop_cn_flag;
extern int g_cm_server_instance_status;
extern int g_dn_role_for_phony_dead[CM_MAX_DATANODE_PER_NODE];
extern int g_gtm_role_for_phony_dead;
extern int g_localCnStatus;
extern int g_dn_phony_dead_times[CM_MAX_DATANODE_PER_NODE];
extern int g_gtm_phony_dead_times;
extern int g_cn_phony_dead_times;
extern volatile uint32 g_cn_dn_disconnect_times;

extern int64 log_max_size;
extern uint32 log_max_count;
extern uint32 log_saved_days;
extern uint32 log_threshold_check_interval;
extern long g_check_disc_state;
extern long g_thread_state[CM_MAX_DATANODE_PER_NODE + 5];
extern uint32 g_serverNodeId;
#define FENCE_TIMEOUT (agent_connect_retries * (agent_connect_timeout + agent_report_interval))

#ifdef __aarch64__
extern uint32 agent_process_cpu_affinity;
extern uint32 g_datanode_primary_and_standby_num;
extern uint32 g_datanode_primary_num;
extern uint32 total_cpu_core_num;
extern bool g_dn_report_msg_ok;
/* cm_agent start_command parameters */
#define PHYSICAL_CPU_NUM (1 << agent_process_cpu_affinity)
#define CPU_AFFINITY_MAX 2
#endif

bool find_instance_by_id(
    uint32 instanceId, int* instance_type, uint32* node, uint32* node_index, uint32* instance_index);

#endif
