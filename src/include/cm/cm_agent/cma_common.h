/**
 * @file cma_common.h
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-01
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */

#ifndef CMA_COMMON_H
#define CMA_COMMON_H

#ifndef CM_IP_LENGTH
#define CM_IP_LENGTH 128
#endif

void set_thread_state(pthread_t thrId);
void immediate_stop_one_instance(const char* instance_data_path, InstanceTypes instance_type);

char* type_int_to_str_binname(InstanceTypes ins_type);
char* type_int_to_str_name(InstanceTypes ins_type);

int ExecuteCmd(const char* command, struct timeval timeout);
int cmagent_getenv(const char* env_var, char* output_env_value, uint32 env_value_len);

void listen_ip_merge(uint32 ip_count, char ip_listen[][CM_IP_LENGTH], char* ret_ip_merge, uint32 ipMergeLength);
void reload_parameters_from_configfile();

extern uint64 g_obs_drop_cn_xlog;

#endif