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
 * cms_common.h
 *
 * IDENTIFICATION
 *    src/include/cm/cm_server/cms_common.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CMS_COMMON_H
#define CMS_COMMON_H


uint32 findMinCmServerInstanceIdIndex();
uint32 findCmServerOrderIndex(uint32 nodeIndex);
void setNotifyCnFlagByNodeId(uint32 nodeId);
bool is_valid_host(CM_Connection* con, int remote_type);
bool findMinCmServerId(int primaryId);
void get_parameters_from_configfile();
void FreeNotifyMsg();
#ifdef ENABLE_MULTIPLE_NODES
void get_paramter_coordinator_heartbeat_timeout();
#endif
void clean_init_cluster_state();
void get_config_param(const char* config_file, const char* srcParam, char* destParam, int destLen);
int StopCheckNode(uint32 nodeIdCheck);
void SendSignalToAgentThreads();
extern int GetCtlThreadNum();
#endif
