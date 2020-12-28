/**
 * @file cms_arbitrate_datanode.h
 * @author your name (you@domain.com)
 * @brief 
 * @version 0.1
 * @date 2020-07-31
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */
#ifndef CMS_ARBITRATE_DATANODE_H
#define CMS_ARBITRATE_DATANODE_H

#include "cm/elog.h"
#include "cm/cm_msg.h"
#include "cm_server.h"
#include "cms_global_params.h"

extern uint32 find_primary_term(uint32 group_index);
extern uint32 read_term_from_etcd(uint32 group_index);
bool check_datanode_arbitrate_status(uint32 group_index, int member_index);

cm_instance_datanode_report_status &GetDataNodeMember(const uint32 &group, const int &member);

int find_candiate_primary_node_in_instance_role_group(uint32 group_index, int member_index);
int find_auto_switchover_primary_node(uint32 group_index, int member_index);

void datanode_instance_arbitrate(
    CM_Connection *con, agent_to_cm_datanode_status_report *agent_to_cm_datanode_status_ptr);
void set_DN_barrier_info(agent_to_cm_datanode_barrier_status_report* barrier_info);
void datanode_instance_arbitrate_for_psd(
    CM_Connection *con, agent_to_cm_datanode_status_report *agent_to_cm_datanode_status_ptr);
void datanode_instance_arbitrate_new(
    CM_Connection* con, agent_to_cm_datanode_status_report* agent_to_cm_datanode_status_ptr,
    uint32 group_index, int member_index, maintenance_mode mode);
void datanode_instance_arbitrate_single(
    CM_Connection* con, agent_to_cm_datanode_status_report* agent_to_cm_datanode_status_ptr);

void DealDataNodeDBStateChange(const uint32 &group, const int &member, const int &dbStatePrev);
void NotifyDatanodeDynamicPrimary(CM_Connection *con, const uint32 &node, const uint32 &instanceId, const uint32 &group,
    const int &member);

#endif
