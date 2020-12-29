/**
 * @file cma_coordinator.h
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-01
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */

#ifndef CMA_COORDINATOR_H
#define CMA_COORDINATOR_H

#include "cm/cm_msg.h"
extern uint64 g_obs_drop_cn_xlog;

void* auto_repair_cn(void* arg);

int set_pgxc_node_isactive_to_false();
int cmagent_to_coordinator_disconnect(void);
int cm_check_to_coordinate_node_group(char* pid_path, char* node_group_name1, char* node_group_members1,
    char* node_group_name2, char* node_group_members2);
int cm_static_config_check_to_coordinate_wrapper(const char* pid_path, int* coordinate_node_check,
    char data_node_check[][CM_MAX_DATANODE_PER_NODE], char* name_record[CM_NODE_MAXNUM * CM_MAX_INSTANCE_PER_NODE],
    uint32* name_record_count, int* currentNodeNum);
int coordinate_status_check_and_report_wrapper(
    const char* pid_path, agent_to_cm_coordinate_status_report* report_msg, uint32 lc_count, agent_to_cm_coordinate_barrier_status_report* barrier_msg);
int drop_fault_coordinator(const char* pid_path, agent_to_cm_coordinate_status_report* report_msg);
void drop_cn_obs_xlog();

int agent_notify_coordinator_cancel_session(const char* pid_path);
int process_notify_ccn_command(const cm_to_agent_notify_cn_central_node* notify);
int coordinate_status_check_phony_dead(const char* pid_path);
int process_notify_ccn_command_internal(const char* pid_path);
int update_ha_relation_to_coordinator(const char* pid_path);

bool create_connection_to_healthcn(uint32 healthCn);
bool IsCnDnDisconnected();
bool lock_coordinator();
bool unlock_coordinator();
bool update_pgxc_node(uint32 healthCn);
bool start_delay_xlog_recycle();
bool stop_delay_xlog_recycle();
void CnDnDisconnectCheck();
void close_reset_conn();
#endif
