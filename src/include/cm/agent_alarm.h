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
 * agent_alarm.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/cm/agent_alarm.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef AGENT_ALARM_H
#define AGENT_ALARM_H

extern void StartupAlarmItemInitialize(staticNodeConfig* g_currentNode);
extern void AbnormalAlarmItemInitialize(staticNodeConfig* g_currentNode);
extern void AbnormalCmaConnAlarmItemInitialize(staticNodeConfig* g_currentNode);
extern void DatanodeAbnormalAlarmItemInitialize(staticNodeConfig* g_currentNode);

extern Alarm* StartupAlarmList;
extern int StartupAlarmListSize;

extern Alarm* AbnormalAlarmList;
extern int AbnormalAlarmListSize;

extern Alarm* AbnormalCmaConnAlarmList;
extern int AbnormalCmaConnAlarmListSize;

extern Alarm* AbnormalBuildAlarmList;
extern Alarm* AbnormalDataInstDiskAlarmList;
extern int DatanodeAbnormalAlarmListSize;

extern Alarm* AbnormalEtcdAlarmList;
extern int AbnormalEtcdAlarmListSize;

extern void report_build_fail_alarm(AlarmType alarmType, char* instanceName, int alarmIndex);
extern void report_dn_disk_alarm(AlarmType alarmType, char* instanceName, int alarmIndex, char* data_path);
extern void report_etcd_fail_alarm(AlarmType alarmType, char* instanceName, int alarmIndex);
extern void InitializeAlarmItem(staticNodeConfig* g_currentNode);
#endif
