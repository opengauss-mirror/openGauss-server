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
 * cms_etcd.h
 *
 * IDENTIFICATION
 *    src/include/cm/cm_server/cms_etcd.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CMS_ETCD_H
#define CMS_ETCD_H

#include "cm_server.h"
#include "cms_global_params.h"

extern volatile bool g_arbitration_changed_from_minority;
uint32 read_term_from_etcd(uint32 group_index);
void clear_sync_with_etcd_flag();
void etcd_ip_port_info_balance(EtcdServerSocket* server);
void CmsGetKerberosInfoFromEtcd();

void get_all_cmserver_heartbeat_from_etcd();
bool set_heartbeat_to_etcd(char* key);
bool get_finish_redo_flag_from_etcd(uint32 group_index);
bool GetFinishRedoFlagFromEtcdNew();
bool getDnFailStatusFromEtcd(int* statusOnline);
bool setDnFailStatusToEtcd(int* statusOnline);

bool getOnlineStatusFromEtcd(int* statusOnline);
bool setOnlineStatusToEtcd(int* statusOnline);

void set_dynamic_config_change_to_etcd(uint32 group_index, int member_index);
void get_coordinator_dynamic_config_change_from_etcd(uint32 group_index);
void GetCoordinatorDynamicConfigChangeFromEtcdNew(uint32 group_index);
void get_datanode_dynamic_config_change_from_etcd(uint32 group_index);
void GetDatanodeDynamicConfigChangeFromEtcdNew(uint32 group_index);
void get_gtm_dynamic_config_change_from_etcd(uint32 group_index);
void SetStaticPrimaryRole(uint32 group_index, int static_primary__index);
int SetReplaceCnStatusToEtcd();
void GetNodeReadOnlyStatusFromEtcd();
errno_t SetNodeReadOnlyStatusToEtcd(const char* bitsString);

int try_etcd_get(char* key, char* value, int max_size, int tryTimes);
uint64 GetTimeMinus(struct timeval checkEnd, struct timeval checkBegin);
int GetInstanceKeyValueFromEtcd(const char *key, InstanceStatusKeyValue *keyValue, const int length);

void server_etcd_init();
int GetHistoryClusterCurSyncListFromEtcd();
int GetHistoryClusterExceptSyncListFromEtcd();
bool SetGroupExpectSyncList(uint32 index, const DatanodeDynamicStatus *statusDnOnline);

void CloseAllEtcdSession();
EtcdSession GetNextEtcdSession();
int SetTermToEtcd(uint32 term);
int IncrementTermToEtcd();
#endif