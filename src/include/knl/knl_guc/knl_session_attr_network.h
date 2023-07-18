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
 * knl_session_attr_network.h
 *        Data struct to store all knl_session_attr_network GUC variables.
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
 *        src/include/knl/knl_guc/knl_session_attr_network.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_KNL_KNL_SESSION_ATTR_NETWORK_H_
#define SRC_INCLUDE_KNL_KNL_SESSION_ATTR_NETWORK_H_

#include "knl/knl_guc/knl_guc_common.h"

typedef struct knl_session_attr_network {
    bool PoolerForceReuse;
    bool comm_debug_mode;
    bool comm_stat_mode;
    bool comm_timer_mode;
    bool comm_no_delay;
    int PoolerTimeout;
    int MinPoolSize;
    int PoolerMaxIdleTime;
    int PoolerConnectMaxLoops;
    int PoolerConnectIntervalTime;
    int PoolerConnectTimeout;
    int PoolerCancelTimeout;
    int comm_max_datanode;
#ifndef ENABLE_MULTIPLE_NODES
    char* ListenAddresses;
#endif
#ifdef LIBCOMM_SPEED_TEST_ENABLE
    int comm_test_thread_num;
    int comm_test_msg_len;
    int comm_test_send_sleep;
    int comm_test_send_once;
    int comm_test_recv_sleep;
    int comm_test_recv_once;
#endif
#ifdef LIBCOMM_FAULT_INJECTION_ENABLE
    int comm_fault_injection;
#endif
    bool comm_client_bind;
    int comm_ackchk_time;
} knl_session_attr_network;

#endif /* SRC_INCLUDE_KNL_KNL_SESSION_ATTR_NETWORK_H_ */
