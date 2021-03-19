/**
 * @file cma_instance_management.h
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-01
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */

#ifndef CMA_INSTANCE_MANAGEMENT_H
#define CMA_INSTANCE_MANAGEMENT_H

#ifndef CM_IP_LENGTH
#define CM_IP_LENGTH 128
#endif

#define MAX_BUF_LEN 10
#define CHECK_DN_BUILD_TIME 25

void start_coordinator();
void kill_instance_force(const char* data_path, InstanceTypes ins_type);
void immediate_stop_one_instance(const char* instance_data_path, InstanceTypes instance_type);
void immediate_shutdown_nodes(bool kill_cmserver, bool kill_cn);
void* agentStartAndStopMain(void* arg);
bool ExecuteCmdWithResult(char* cmd, char* result);
bool getnicstatus(uint32 listen_ip_count, char ips[][CM_IP_LENGTH]);
bool Is_cluster_replacing(void);
bool Is_cluster_resizing(void);
int agentCheckPort(uint32 port);
uint32 GetLibcommPort(const char* file_path, uint32 base_port, int port_type);
extern bool UpdateLibcommConfig();
#endif