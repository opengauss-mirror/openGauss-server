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
 * knl_session_attr_resource.h
 *        Data struct to store all knl_session_attr_resource GUC variables.
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
 * IDENTIFICATION
 *        src/include/knl/knl_guc/knl_session_attr_resource.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_KNL_KNL_SESSION_ATTR_RESOURCE_H_
#define SRC_INCLUDE_KNL_KNL_SESSION_ATTR_RESOURCE_H_

#include "knl/knl_guc/knl_guc_common.h"

typedef struct knl_session_attr_resource {
#ifdef BTREE_BUILD_STATS
    bool log_btree_build_stats;
#endif
#ifdef TRACE_SYNCSCAN
    bool trace_syncscan;
#endif
    bool enable_cgroup_switch;
    bool enable_resource_track;
    bool enable_hotkeys_collection;
    bool enable_verify_statements;
    bool enable_force_memory_control;
    bool enable_dywlm_adjust;
    bool enable_resource_record;
    bool enable_user_metric_persistent;
    bool enable_instance_metric_persistent;
    bool enable_logical_io_statistics;
    bool enable_reaper_backend;
    bool enable_transaction_parctl;
    int max_active_statements;
    int dynamic_memory_quota;
    int memory_fault_percent;
    int parctl_min_cost;
    int resource_track_cost;
    int resource_track_duration;
    int user_metric_retention_time;
    int topsql_retention_time;
    int unique_sql_retention_time;
    int instance_metric_retention_time;
    int cpu_collect_timer;
    int iops_limits;
    int autovac_iops_limits;
    int io_control_unit;
    int transaction_pending_time;
    char* cgroup_name;
    char* query_band;
    char* session_resource_pool;
    int resource_track_level;
    int io_priority;
    bool use_workload_manager;
    bool enable_control_group;

    /* GUC variable for session statistics memory */
    int session_statistics_memory;
    /* GUC variable for session history memory */
    int session_history_memory;
    bool bypass_workload_manager;
    /* sql_use_spacelimit */
    int sqlUseSpaceLimit;
    bool enable_auto_explain;
    int  auto_explain_level;
} knl_session_attr_resource;

#endif /* SRC_INCLUDE_KNL_KNL_SESSION_ATTR_RESOURCE_H_ */
