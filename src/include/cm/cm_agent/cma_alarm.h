/**
 * @file cma_alarm.h
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-01
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */
#ifndef CMA_ALARM_H
#define CMA_ALARM_H

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

extern void StorageScalingAlarmItemInitialize(void);
extern void ReportStorageScalingAlarm(AlarmType alarmType, const char* instanceName, int alarmIndex);
extern void StartupAlarmItemInitialize(staticNodeConfig* g_currentNode);
extern void AbnormalAlarmItemInitialize(staticNodeConfig* g_currentNode);
extern void AbnormalCmaConnAlarmItemInitialize(staticNodeConfig* g_currentNode);
extern void DatanodeAbnormalAlarmItemInitialize(staticNodeConfig* g_currentNode);

extern void report_build_fail_alarm(AlarmType alarmType, char* instanceName, int alarmIndex);
extern void report_dn_disk_alarm(AlarmType alarmType, char* instanceName, int alarmIndex, char* data_path);
extern void report_etcd_fail_alarm(AlarmType alarmType, char* instanceName, int alarmIndex);
extern void InitializeAlarmItem(staticNodeConfig* g_currentNode);

#endif