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
 * knl_instance_attr_network.h
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
 *        src/include/knl/knl_guc/knl_instance_attr_network.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_KNL_KNL_INSTANCE_ATTR_NETWORK_H_
#define SRC_INCLUDE_KNL_KNL_INSTANCE_ATTR_NETWORK_H_

#include "knl/knl_guc/knl_guc_common.h"
#include "libcomm/libcomm.h"

typedef struct knl_instance_attr_network {
    bool PoolerStatelessReuse;
    bool comm_tcp_mode;
    int MaxConnections;
    int maxInnerToolConnections;
    int ReservedBackends;
    int PostPortNumber;
    int Unix_socket_permissions;
    int MaxPoolSize;
    int PoolerPort;
    int MaxCoords;
    int comm_sctp_port;
    int comm_control_port;
    int comm_quota_size;
    int comm_usable_memory;
    int comm_memory_pool;
    int comm_memory_pool_percent;
    int comm_max_stream;
    int comm_max_receiver;
    int cn_send_buffer_size;
    char* Unix_socket_group;
    char* UnixSocketDir;
    char* ListenAddresses;
    char* tcp_link_addr;
    bool comm_enable_SSL;
    LibCommConn ** comm_ctrl_channel_conn;
    LibCommConn ** comm_data_channel_conn;
#ifdef USE_SSL
    bool ssl_initialized;
    SSL_CTX* SSL_server_context;
    GS_UCHAR* server_key;
#endif
} knl_instance_attr_network;

#endif /* SRC_INCLUDE_KNL_KNL_INSTANCE_ATTR_NETWORK_H_ */
