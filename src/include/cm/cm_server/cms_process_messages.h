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
 * cms_process_messages.h
 *
 * IDENTIFICATION
 *    src/include/cm/cm_server/cms_process_messages.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CMS_PROCESS_MESSAGES_H
#define CMS_PROCESS_MESSAGES_H

#ifndef CM_AZ_NAME
#define CM_AZ_NAME 65
#endif

// ETCD TIME THRESHOLD
#define ETCD_CLOCK_THRESHOLD 3
#define SWITCHOVER_SEND_CHECK_RATE 30

extern int cmserver_getenv(const char* env_var, char* output_env_value, uint32 env_value_len, int elevel);
extern int check_if_candidate_is_in_faulty_az(uint32 group_index, int candidate_member_index);
extern int findAzIndex(char azArray[][CM_AZ_NAME], const char* azName);
extern int ReadCommand(Port* myport, CM_StringInfo inBuf);
extern int isNodeBalanced(uint32* switchedInstance);
extern int get_logicClusterId_by_dynamic_dataNodeId(uint32 dataNodeId);

extern bool process_auto_switchover_full_check();
extern bool existMaintenanceInstanceInGroup(uint32 group_index,int *init_primary_member_index);
extern bool isMaintenanceInstance(const char *file_path,uint32 notify_instance_id);

extern uint32 GetClusterUpgradeMode();

extern void cm_to_cm_report_sync_setting(cm_to_cm_report_sync* cm_to_cm_report_sync_content, uint32 sync_cycle_count);
extern void cm_server_process_msg(CM_Connection* con, CM_StringInfo inBuffer);
extern void set_command_status_due_send_status(int send_status);
extern void SwitchOverSetting(int time_out, int instanceType, int ptrIndex, int memberIndex);

extern void getAZDyanmicStatus(
    int azCount, int* statusOnline, int* statusPrimary, int* statusFail, int* statusDnFail, char azArray[][CM_AZ_NAME]);
extern void process_cm_to_cm_report_sync_msg(cm_to_cm_report_sync* cm_to_cm_report_sync_ptr);
extern void process_cm_to_cm_role_change_msg(cm_to_cm_role_change_notify* cm_to_cm_role_change_notify_content_ptr);
extern void process_cm_to_cm_broadcast_msg(cm_to_cm_broadcast* cm_to_cm_broadcast_msg_content_ptr);
extern void process_cm_to_cm_vote_msg(cm_to_cm_vote* cm_to_cm_vote_msg_content_ptr);
extern void check_cluster_status();
int switchoverFullDone(void);
void set_cluster_status(void);

void process_ctl_to_cm_balance_result_msg(CM_Connection* con);
void process_ctl_to_cm_get_msg(CM_Connection* con);
void process_ctl_to_cm_build_msg(CM_Connection* con, ctl_to_cm_build* ctl_to_cm_build_ptr);
void process_ctl_to_cm_query_msg(CM_Connection* con, ctl_to_cm_query* ctl_to_cm_query_ptr);
void process_ctl_to_cm_query_kerberos_status_msg(CM_Connection* con);
void process_ctl_to_cm_query_cmserver_msg(CM_Connection* con);
void process_ctl_to_cm_switchover_msg(CM_Connection* con, ctl_to_cm_switchover* ctl_to_cm_swithover_ptr);
void process_ctl_to_cm_switchover_all_msg(CM_Connection* con, ctl_to_cm_switchover* ctl_to_cm_swithover_ptr);
void process_ctl_to_cm_switchover_full_msg(CM_Connection* con, ctl_to_cm_switchover* ctl_to_cm_swithover_ptr);
void process_ctl_to_cm_switchover_full_check_msg(CM_Connection* con);
void process_ctl_to_cm_switchover_full_timeout_msg(CM_Connection* con);
void process_ctl_to_cm_switchover_az_msg(CM_Connection* con, ctl_to_cm_switchover* ctl_to_cm_swithover_ptr);
void process_ctl_to_cm_switchover_az_check_msg(CM_Connection* con);
void process_ctl_to_cm_switchover_az_timeout_msg(CM_Connection* con);
void process_ctl_to_cm_setmode(CM_Connection* con);
void process_ctl_to_cm_set_msg(CM_Connection* con, ctl_to_cm_set* ctl_to_cm_set_ptr);
void process_ctl_to_cm_balance_check_msg(CM_Connection* con);
void process_ctl_to_cm_get_datanode_relation_msg(
    CM_Connection* con, ctl_to_cm_datanode_relation_info* ctl_to_cm_datanode_relation_info_ptr);

void process_cm_to_cm_timeline_msg(cm_to_cm_timeline* cm_to_cm_timeline_msg_content_ptr);

void process_gs_guc_feedback_msg(agent_to_cm_gs_guc_feedback* feedback_ptr);
void process_notify_cn_feedback_msg(CM_Connection* con, agent_to_cm_notify_cn_feedback* feedback_ptr);
void process_agent_to_cm_heartbeat_msg(CM_Connection* con, agent_to_cm_heartbeat* agent_to_cm_heartbeat_ptr);
void process_agent_to_cm_disk_usage_msg(CM_Connection* con, AgentToCMS_DiskUsageStatusReport* agent_to_cm_disk_usage_ptr);
void process_agent_to_cm_current_time_msg(agent_to_cm_current_time_report* etcd_time_ptr);
void process_agent_to_cm_kerberos_status_report_msg(
    CM_Connection* con, agent_to_cm_kerberos_status_report* agent_to_cm_kerberos_status_ptr);
void process_agent_to_cm_fenced_UDF_status_report_msg(
    agent_to_cm_fenced_UDF_status_report* agent_to_cm_fenced_UDF_status_ptr);
void process_ctl_to_cm_query_global_barrier_msg(CM_Connection* con);
void process_ctl_to_cm_query_barrier_msg(CM_Connection* con,
    ctl_to_cm_global_barrier_query* ctl_to_cm_global_barrier_query_ptr);
void process_ctl_to_cm_one_instance_barrier_query_msg(CM_Connection* con, uint32 node, uint32 instanceId, int instanceType);
void ProcessGetDnSyncListMsg(CM_Connection *con, AgentToCmserverDnSyncList *agentToCmserverDnSyncList);

#endif
