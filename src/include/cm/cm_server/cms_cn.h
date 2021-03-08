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
 * -------------------------------------------------------------------------
 *
 * cms_cn.h
 *
 * IDENTIFICATION
 *    src/include/cm/cm_server/cms_cn.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef CMS_CN_H
#define CMS_CN_H

#include "cm_server.h"

#ifdef ENABLE_MULTIPLE_NODES
void process_cm_to_agent_repair_ack(CM_Connection* con, uint32 group_index);
void process_agent_to_cm_coordinate_status_report_msg(
    CM_Connection* con, agent_to_cm_coordinate_status_report* agent_to_cm_coordinate_status_ptr);
void process_ctl_to_cm_disable_cn(CM_Connection* con, ctl_to_cm_disable_cn* ctl_to_cm_disable_cn_ptr);
void process_agent_to_central_coordinate_status(
    CM_Connection* con, const agent_to_cm_coordinate_status_report* report_msg, int group_index, uint32 count);
#endif
void SetBarrierInfo(agent_to_cm_coordinate_barrier_status_report* barrier_info);

#endif
