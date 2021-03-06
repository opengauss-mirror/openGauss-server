/**
 * @file cma_datanode.h
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-01
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */

#ifndef CMA_DATANODE_H
#define CMA_DATANODE_H

#include "cma_main.h"
#ifdef ENABLE_MULTIPLE_NODES
void* DNStorageScalingCheckMain(void * const arg);
#endif
bool is_process_alive(pgpid_t pid);

pgpid_t get_pgpid(char* pid_path);

uint32 GetDatanodeNumSort(staticNodeConfig* p_node_config, uint32 sort);

int cm_check_install_nodegroup_is_exist();
int cm_select_datanode_names(
    char* pid_path, char datanode_names[][CM_NODE_NAME], char datanode_name_oids[][CM_NODE_NAME], int* datanode_count);
int ReadDBStateFile(GaussState* state, char* state_path);
int process_unlock_command(uint32 instance_id);
/* Agent to DN connection */
int process_lock_no_primary_command(uint32 instance_id);
int process_lock_chosen_primary_command(cm_to_agent_lock2* msg_type_lock2_ptr);
int check_datanode_status(const char* pid_path, int* role);

int process_obs_delete_xlog_command(uint32 instance_id, uint64 lsn);


#endif
