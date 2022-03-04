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
 * knl_instance_attr_common.h
 *        Data struct to store all knl_instance_attr_common GUC variables.
 *
 *   When anyone try to added variable in this file, which means add a guc
 *   variable, there are several rules needed to obey:
 *
 *   add variable to struct 'knl_@level@_attr_@group@'
 *
 *   @level@:
 *   1. instance: the level of guc variable is PGC_POSTMASTER.
 *   2. session: the other level of guc variable.
 *
 *   @group@: sql, storage, security, network, memory, resource, common
 *   select the group according to the type of guc variable.
 * 
 * 
 * IDENTIFICATION
 *        src/include/knl/knl_guc/knl_instance_attr_common.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_KNL_KNL_INSTANCE_ATTR_COMMON_H_
#define SRC_INCLUDE_KNL_KNL_INSTANCE_ATTR_COMMON_H_

#include "knl/knl_guc/knl_guc_common.h"

typedef struct knl_instance_attr_common {
    bool support_extended_features;
    bool lastval_supported;
#ifdef USE_BONJOUR
    bool enable_bonjour;
#endif
    bool Logging_collector;
    bool allowSystemTableMods;
    bool enable_thread_pool;
    bool enable_ffic_log;
    bool enable_global_plancache;
    bool enable_global_syscache;
    int max_files_per_process;
    int pgstat_track_activity_query_size;
    int GtmHostPortArray[MAX_GTM_HOST_NUM];
    int MaxDataNodes;
    int max_changes_in_memory;
    int max_cached_tuplebufs;
#ifdef USE_BONJOUR
    char* bonjour_name;
#endif
    char* shared_preload_libraries_string;
    char* event_source;
    char* data_directory;
    char* ConfigFileName;
    char* HbaFileName;
    char* IdentFileName;
    char* external_pid_file;
    char* PGXCNodeName;
    char* transparent_encrypt_kms_url;
    char* thread_pool_attr;
    char* thread_pool_stream_attr;
    char* comm_proxy_attr;
    char* numa_distribute_mode;

    bool data_sync_retry;

    bool enable_alarm;
    char* Alarm_component;
    bool enable_tsdb;
    char* Perf_directory;
    char* asp_log_directory;
    char* query_log_directory;
    int asp_sample_num;

    /*
     * guc - bbox_blacklist_items
     *      char* : original value
     *      uint64 : parsed bits mask
     */
    char* bbox_blacklist_items;
    uint64 bbox_blacklist_mask;
#ifdef ENABLE_MOT
    char* MOTConfigFileName;
#endif
#ifndef ENABLE_MULTIPLE_NODES
    int sync_config_strategy;
    bool enable_auto_clean_unique_sql;
#endif
    int cluster_run_mode;
    int stream_cluster_run_mode;
} knl_instance_attr_common;

#endif /* SRC_INCLUDE_KNL_KNL_INSTANCE_ATTR_COMMON_H_ */
