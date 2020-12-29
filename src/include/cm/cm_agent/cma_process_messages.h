/**
 * @file cma_process_messages.h
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-03
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */

#ifndef CMA_PROCESS_MESSAGES_H
#define CMA_PROCESS_MESSAGES_H

#ifndef CM_IP_LENGTH
#define CM_IP_LENGTH 128
#endif

void* SendCmsMsgMain(void* arg);
void immediate_stop_one_instance(const char* instance_data_path, InstanceTypes instance_type);
void kill_instance_force(const char* data_path, InstanceTypes ins_type);
int search_HA_node(uint32 loal_port, uint32 LocalHAListenCount, char LocalHAIP[][CM_IP_LENGTH], uint32 Peer_port,
    uint32 PeerHAListenCount, char PeerHAIP[][CM_IP_LENGTH], uint32* node_index, uint32* instance_index,
    uint32 loal_role);
char* get_logicClusterName_by_dnInstanceId(uint32 dnInstanceId);

#endif